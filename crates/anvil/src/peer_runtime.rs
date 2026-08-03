//! Production wiring for Anvil's private Raft listener.
//!
//! This module owns no membership policy. It binds the already-approved peer
//! address, loads one local identity, and connects the Tonic transport to the
//! latest locally applied committed node descriptors.

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anvil_consensus::{
    ApplyError, ApplyResult, CLUSTER_CONTROL_COMMAND_VERSION, CapabilityRange, ClusterId, Command,
    CommittedPeerPinProvider, CommittedPeerPins, DecisionRaft, DecisionRaftError,
    JoinCapabilityHash, MAX_PEER_ADDRESS_BYTES, MembershipTransitionKind, NodeDescriptor, NodeId,
    NodeState, PeerAddress, PeerNode, PeerRpcKind, PeerSpkiSha256, PeerTlsAcceptor, PeerTlsConfig,
    PeerTlsConnector, PeerTlsError, PeerTlsIdentity, ServingLeaseIssuer, TonicPeerTransport,
    TonicRaftPeerService,
};
use anvil_store::{ErasureProfile, Store};
use anyhow::{Context, Result, bail};
use tonic::codegen::tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use uuid::Uuid;

use crate::cluster_peer::{
    ClusterPeerService, LateBoundDistributedControl, LateBoundFreshAuthorization,
    RoutedAuthzHandlers, RoutedIndexQueryHandlers, RoutedPublicHandlers,
};
use crate::data_peer::{DataPeerService, DataPeerTransport};
use crate::distributed_list::LateBoundListAuthorizer;
use crate::index_runtime::publication::LateBoundIndexArtifactPublication;
use crate::join_peer::{
    JoinActivationGate, JoinBootstrapPins, JoinPeerService, JoinPeerTransport, TypedAddHandoff,
};
use crate::logical_name_resolution::LateBoundLogicalNameResolution;
use crate::node_identity::{self, LocalNodeIdentity};
use crate::payload_distribution::PayloadPeerService;
use crate::programs::LateBoundProgramQuiescence;

const GENESIS_STORAGE_WEIGHT_MILLIONTHS: u32 = 1_000_000;
const PEER_PROTOCOL_VERSION: u16 = 1;
const STORAGE_FORMAT_VERSION: u16 = 1;

pub(crate) struct OpenPeerConfig<'a> {
    pub(crate) data_dir: &'a Path,
    pub(crate) node_id: NodeId,
    pub(crate) peer_address: PeerAddress,
    pub(crate) join_bundle: Option<&'a Path>,
    pub(crate) run_system_bootstrap: bool,
    pub(crate) max_commit_entries: u32,
    pub(crate) max_commit_bytes: u64,
    pub(crate) leader_timeout: Duration,
}

pub(crate) struct PeerRuntime {
    node_id: NodeId,
    identity: Arc<PeerTlsIdentity>,
    pins: Arc<RaftCommittedPeerPins>,
    transport: TonicPeerTransport,
    #[allow(
        dead_code,
        reason = "the distributed coordinators consume this transport in the immediately following integration slice"
    )]
    data_transport: DataPeerTransport,
    routed_public_handlers: RoutedPublicHandlers,
    routed_authz_handlers: RoutedAuthzHandlers,
    list_authorizer: LateBoundListAuthorizer,
    fresh_authorization: LateBoundFreshAuthorization,
    distributed_control: LateBoundDistributedControl,
    name_resolution: LateBoundLogicalNameResolution,
    program_quiescence: LateBoundProgramQuiescence,
    index_artifacts: LateBoundIndexArtifactPublication,
    routed_index_queries: RoutedIndexQueryHandlers,
    join_transport: Option<JoinPeerTransport>,
    bootstrap_pins: Option<Arc<JoinBootstrapPins>>,
    clear_pins_on_drop: bool,
}

pub(crate) struct PeerServerHandle {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
    pins: Arc<RaftCommittedPeerPins>,
}

impl PeerServerHandle {
    pub(crate) fn task_mut(&mut self) -> &mut tokio::task::JoinHandle<Result<()>> {
        self.task
            .as_mut()
            .expect("peer server task is present until completion is recorded")
    }

    pub(crate) fn record_completed(&mut self) {
        self.shutdown.take();
        self.task.take();
        self.pins.clear();
    }

    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let Some(task) = self.task.take() else {
            self.pins.clear();
            return Ok(());
        };
        let joined = task.await;
        self.pins.clear();
        joined.context("join private peer server task")?
    }
}

impl Drop for PeerServerHandle {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.pins.clear();
    }
}

impl Drop for PeerRuntime {
    fn drop(&mut self) {
        if self.clear_pins_on_drop {
            self.pins.clear();
        }
    }
}

/// Resolve the address committed for this node's peer connections.
pub(crate) fn peer_address(
    peer_listen: SocketAddr,
    configured: Option<&str>,
) -> Result<PeerAddress> {
    let advertised = match configured {
        Some(configured) => configured.to_owned(),
        None if peer_listen.ip().is_unspecified() => {
            bail!("--peer-advertise is required when --peer-listen uses a wildcard IP")
        }
        None => peer_listen.to_string(),
    };
    validate_peer_address(&advertised)?;
    Ok(PeerAddress(advertised))
}

fn validate_peer_address(address: &str) -> Result<()> {
    anyhow::ensure!(
        !address.is_empty() && address.len() <= MAX_PEER_ADDRESS_BYTES,
        "peer advertise address must contain 1 to {MAX_PEER_ADDRESS_BYTES} bytes"
    );
    anyhow::ensure!(
        !address
            .chars()
            .any(|character| character.is_control() || character.is_whitespace()),
        "peer advertise address must not contain whitespace or control characters"
    );
    let mut resolved = address
        .to_socket_addrs()
        .with_context(|| format!("peer advertise address {address:?} is not a host:port"))?;
    let first = resolved
        .next()
        .with_context(|| format!("peer advertise address {address:?} resolved to no addresses"))?;
    anyhow::ensure!(
        first.port() != 0,
        "peer advertise address must use a non-zero port"
    );
    Ok(())
}

pub(crate) async fn open(config: OpenPeerConfig<'_>) -> Result<(DecisionRaft, PeerRuntime)> {
    let identity_exists = node_identity::identity_path(config.data_dir)
        .try_exists()
        .context("inspect local node identity path")?;
    let decision_state_exists = legacy_decision_state_exists(config.data_dir)?;
    if !identity_exists && decision_state_exists && config.join_bundle.is_some() {
        bail!("a join bundle cannot be applied to an existing decision store");
    }
    let migrated_identity = if !identity_exists && decision_state_exists {
        migrate_released_identity(&config).await?
    } else {
        None
    };

    let mut created_genesis_identity = false;
    let identity = if identity_exists {
        match config.join_bundle {
            Some(path) => crate::join_bundle::consume(config.data_dir, path)
                .context("finish consuming copied join bundle")?
                .local_identity()
                .context("load consumed join identity")?,
            None => node_identity::load_for_node(config.data_dir, config.node_id)
                .context("load local node identity")?,
        }
    } else if let Some(identity) = migrated_identity {
        identity
    } else if let Some(path) = config.join_bundle {
        if config.run_system_bootstrap {
            tracing::warn!("--run-system-bootstrap is ignored when --join-bundle is supplied");
        }
        crate::join_bundle::consume(config.data_dir, path)
            .context("consume copied mode-0600 join bundle")?
            .local_identity()
            .context("load consumed join identity")?
    } else {
        anyhow::ensure!(
            config.run_system_bootstrap,
            "an empty node requires --run-system-bootstrap or --join-bundle"
        );
        let cluster_id = ClusterId(*Uuid::new_v4().as_bytes());
        let identity = node_identity::generate(cluster_id, config.node_id)
            .context("generate local peer identity")?;
        node_identity::create(config.data_dir, &identity).context("persist local peer identity")?;
        created_genesis_identity = true;
        tracing::info!(
            path = %node_identity::identity_path(config.data_dir).display(),
            "generated mode-0600 cluster node identity"
        );
        identity
    };
    anyhow::ensure!(
        identity.node_id() == config.node_id,
        "copied join bundle node ID does not match --node-id"
    );
    if let Some(pending) = identity.pending_join() {
        anyhow::ensure!(
            pending.peer_address() == &config.peer_address,
            "copied join bundle peer address does not match --peer-advertise"
        );
    }

    let (decisions, mut runtime) = open_with_identity(&config, &identity).await?;
    if !decisions.is_initialized().await? {
        if identity.pending_join().is_some() {
            return Ok((decisions, runtime));
        }
        anyhow::ensure!(
            created_genesis_identity,
            "an existing node identity cannot initialize a new Raft cluster"
        );
        decisions
            .initialize_genesis(std::collections::BTreeMap::from([(
                config.node_id.0,
                PeerNode::new(config.peer_address.0.clone()),
            )]))
            .await
            .context("initialize one-voter genesis Raft group")?;
    }

    if identity.pending_join().is_some() {
        let state = decisions.state()?;
        if state.cluster_id().is_none() {
            return Ok((decisions, runtime));
        }
        validate_joining_restart_state(
            &decisions,
            &identity,
            &config.peer_address,
            runtime.identity.spki_sha256(),
        )?;
        let descriptor = &state.cluster_control().nodes()[&config.node_id];
        if descriptor.state == NodeState::Joining {
            return Ok((decisions, runtime));
        }
        node_identity::clear_pending_join(config.data_dir, identity.cluster_id(), config.node_id)
            .context("clear consumed join capability after ACTIVE membership")?;
        runtime.clear_join_bootstrap();
    }

    let state = decisions.state()?;
    if state.cluster_control().nodes().is_empty() {
        decisions
            .wait_for_leader(config.leader_timeout)
            .await
            .context("elect genesis leader before node admission")?;
        ensure_cluster_identity(&decisions, identity.cluster_id()).await?;
        admit_genesis_descriptor(
            &decisions,
            config.node_id,
            &config.peer_address,
            runtime.identity.spki_sha256(),
        )
        .await?;
    }
    validate_restart_state(
        &decisions,
        &identity,
        &config.peer_address,
        runtime.identity.spki_sha256(),
    )?;
    Ok((decisions, runtime))
}

/// Catch up one consumed join identity, request activation through the
/// server-side typed-handoff gate, and clear the one-time capability only once
/// ACTIVE is locally applied.
pub(crate) async fn complete_pending_join(
    transport: &JoinPeerTransport,
    decisions: &DecisionRaft,
    data_dir: &Path,
    timeout: Duration,
) -> Result<()> {
    let transition = transport
        .catch_up(timeout)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let apply_deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .context("join timeout exceeds the process clock")?;
    loop {
        let state = decisions.state()?;
        let exact = state
            .cluster_control()
            .nodes()
            .get(&transition.node_id)
            .is_some_and(|descriptor| descriptor.state == NodeState::Joining)
            && state.cluster_control().transition() == Some(&transition);
        if exact {
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < apply_deadline,
            "timed out waiting for the caught-up ADD transition to apply locally"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let identity = node_identity::load_for_node(data_dir, transition.node_id)
        .context("reload caught-up JOINING identity")?;
    let state = decisions.state()?;
    let descriptor = state
        .cluster_control()
        .nodes()
        .get(&transition.node_id)
        .context("caught-up Raft state omitted the joining descriptor")?;
    anyhow::ensure!(
        descriptor.state == NodeState::Joining
            && state.cluster_control().transition() == Some(&transition),
        "caught-up Raft state does not contain the exact pending ADD transition"
    );
    validate_joining_restart_state(
        decisions,
        &identity,
        pending_peer_address(&identity)?,
        PeerTlsIdentity::from_pem(
            identity
                .presented_peer_identity()
                .certificate_pem()
                .as_bytes(),
            identity
                .presented_peer_identity()
                .private_key_pem()
                .as_bytes(),
        )
        .context("parse caught-up JOINING peer identity")?
        .spki_sha256(),
    )?;
    transport.clear_bootstrap_pins();
    transport
        .activate(timeout)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .context("join timeout exceeds the process clock")?;
    loop {
        let state = decisions.state()?;
        if state
            .cluster_control()
            .nodes()
            .get(&transition.node_id)
            .is_some_and(|descriptor| descriptor.state == NodeState::Active)
            && state.cluster_control().transition().is_none()
        {
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for locally applied ACTIVE membership"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    node_identity::clear_pending_join(data_dir, identity.cluster_id(), transition.node_id)
        .context("clear one-time join capability after ACTIVE membership")?;
    Ok(())
}

fn pending_peer_address(identity: &LocalNodeIdentity) -> Result<&PeerAddress> {
    identity
        .pending_join()
        .map(|pending| pending.peer_address())
        .context("caught-up node identity has no pending join material")
}

impl PeerRuntime {
    pub(crate) fn serving_transport(&self) -> TonicPeerTransport {
        self.transport.clone()
    }

    #[allow(
        dead_code,
        reason = "the distributed coordinators consume this transport in the immediately following integration slice"
    )]
    pub(crate) fn data_transport(&self) -> DataPeerTransport {
        self.data_transport.clone()
    }

    pub(crate) fn routed_public_handlers(&self) -> RoutedPublicHandlers {
        self.routed_public_handlers.clone()
    }

    pub(crate) fn routed_authz_handlers(&self) -> RoutedAuthzHandlers {
        self.routed_authz_handlers.clone()
    }

    pub(crate) fn list_authorizer(&self) -> LateBoundListAuthorizer {
        self.list_authorizer.clone()
    }

    pub(crate) fn fresh_authorization(&self) -> LateBoundFreshAuthorization {
        self.fresh_authorization.clone()
    }

    pub(crate) fn distributed_control(&self) -> LateBoundDistributedControl {
        self.distributed_control.clone()
    }

    pub(crate) fn name_resolution(&self) -> LateBoundLogicalNameResolution {
        self.name_resolution.clone()
    }

    pub(crate) fn program_quiescence(&self) -> LateBoundProgramQuiescence {
        self.program_quiescence.clone()
    }

    pub(crate) fn index_artifacts(&self) -> LateBoundIndexArtifactPublication {
        self.index_artifacts.clone()
    }

    pub(crate) fn routed_index_query_handlers(&self) -> RoutedIndexQueryHandlers {
        self.routed_index_queries.clone()
    }

    pub(crate) fn join_transport(&self) -> Option<JoinPeerTransport> {
        self.join_transport.clone()
    }

    fn clear_join_bootstrap(&mut self) {
        if let Some(pins) = self.bootstrap_pins.take() {
            pins.clear();
        }
        self.join_transport = None;
    }

    pub(crate) async fn start(
        self,
        peer_listen: SocketAddr,
        decisions: DecisionRaft,
        store: Store,
        erasure_profile: ErasureProfile,
        maximum_unary_time: Duration,
        max_blob_bytes: u64,
    ) -> Result<PeerServerHandle> {
        let leases = ServingLeaseIssuer::new();
        let activation_gate = Arc::new(TypedAddHandoff::new(
            self.node_id,
            decisions.clone(),
            store.clone(),
            self.data_transport.clone(),
            leases.clone(),
            self.program_quiescence.clone(),
            erasure_profile,
        ));
        self.start_with_activation_gate(
            peer_listen,
            decisions,
            store,
            erasure_profile,
            maximum_unary_time,
            max_blob_bytes,
            leases,
            activation_gate,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_with_activation_gate(
        mut self,
        peer_listen: SocketAddr,
        decisions: DecisionRaft,
        store: Store,
        erasure_profile: ErasureProfile,
        maximum_unary_time: Duration,
        max_blob_bytes: u64,
        leases: ServingLeaseIssuer,
        activation_gate: Arc<dyn JoinActivationGate>,
    ) -> Result<PeerServerHandle> {
        anyhow::ensure!(
            peer_listen.port() != 0,
            "peer listener must use a non-zero port"
        );
        let listener = tokio::net::TcpListener::bind(peer_listen)
            .await
            .with_context(|| format!("bind private peer listener at {peer_listen}"))?;
        let acceptor = PeerTlsAcceptor::new(&self.identity, PeerTlsConfig::default())
            .context("configure mandatory peer mTLS listener")?;
        let incoming = TcpIncoming::from(listener)
            .then(move |stream| {
                let acceptor = acceptor.clone();
                async move {
                    let stream = stream.map_err(PeerTlsError::Io)?;
                    acceptor.accept(stream).await
                }
            })
            .filter_map(|result| match result {
                Ok(stream) => Some(Ok::<_, std::io::Error>(stream)),
                Err(error) => {
                    tracing::warn!(%error, "rejected private peer TLS connection");
                    None
                }
            });
        let service = TonicRaftPeerService::with_serving_lease_issuer(
            decisions.clone(),
            self.pins.clone(),
            leases,
        )
        .into_server();
        let payload_service = PayloadPeerService::new(
            self.node_id,
            store.clone(),
            self.data_transport.clone(),
            erasure_profile,
            decisions.clone(),
            self.pins.clone(),
            max_blob_bytes,
        )
        .into_server();
        let join_service = JoinPeerService::new(
            decisions.clone(),
            self.node_id,
            self.pins.clone(),
            activation_gate,
        )
        .into_server();
        let cluster_service = ClusterPeerService::new(
            self.node_id,
            store.clone(),
            decisions.clone(),
            self.pins.clone(),
            Arc::new(self.list_authorizer.clone()),
            self.fresh_authorization.clone(),
            self.distributed_control.clone(),
            self.name_resolution.clone(),
            self.index_artifacts.clone(),
            self.routed_index_queries.clone(),
            self.routed_public_handlers.clone(),
            self.routed_authz_handlers.clone(),
        )
        .into_server();
        let data_service = DataPeerService::new(
            store,
            self.pins.clone(),
            decisions,
            self.node_id,
            erasure_profile,
            maximum_unary_time,
            max_blob_bytes,
        )?
        .into_server();
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .add_service(data_service)
                .add_service(payload_service)
                .add_service(join_service)
                .add_service(cluster_service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = stopped.await;
                })
                .await
                .context("serve mandatory-mTLS private peer listener")
        });
        tracing::info!(address = %peer_listen, "private Raft peer listener ready");
        self.clear_pins_on_drop = false;
        Ok(PeerServerHandle {
            shutdown: Some(shutdown),
            task: Some(task),
            pins: self.pins.clone(),
        })
    }
}

async fn open_with_identity(
    config: &OpenPeerConfig<'_>,
    identity: &LocalNodeIdentity,
) -> Result<(DecisionRaft, PeerRuntime)> {
    let tls_identity = Arc::new(
        PeerTlsIdentity::from_pem(
            identity
                .presented_peer_identity()
                .certificate_pem()
                .as_bytes(),
            identity
                .presented_peer_identity()
                .private_key_pem()
                .as_bytes(),
        )
        .context("parse persisted peer TLS identity")?,
    );
    let bootstrap_pins = identity.pending_join().map(|pending| {
        Arc::new(JoinBootstrapPins::new(
            identity.cluster_id(),
            pending.seeds(),
        ))
    });
    let pins = Arc::new(RaftCommittedPeerPins::new(
        identity.cluster_id(),
        config.node_id,
        bootstrap_pins.clone(),
    ));
    let connector =
        PeerTlsConnector::new(tls_identity.clone(), pins.clone(), PeerTlsConfig::default())
            .context("configure mandatory peer mTLS connector")?;
    let data_transport =
        DataPeerTransport::new(identity.cluster_id(), config.node_id, connector.clone())
            .context("configure typed data-peer transport")?;
    let join_transport = identity
        .pending_join()
        .cloned()
        .zip(bootstrap_pins.clone())
        .map(|(pending, bootstrap_pins)| {
            JoinPeerTransport::new(
                identity.cluster_id(),
                config.node_id,
                pending,
                connector.clone(),
                bootstrap_pins,
            )
        });
    let transport = TonicPeerTransport::new(identity.cluster_id(), config.node_id, connector)
        .context("configure private Raft transport")?;
    let decisions = DecisionRaft::open_with_transport(
        config.data_dir.join("decisions"),
        config.node_id.0,
        config.max_commit_entries,
        config.max_commit_bytes,
        Arc::new(transport.clone()),
    )
    .await
    .context("open bounded decision Raft with private transport")?;
    pins.install(decisions.clone())?;
    Ok((
        decisions,
        PeerRuntime {
            node_id: config.node_id,
            identity: tls_identity,
            pins,
            transport,
            data_transport,
            routed_public_handlers: RoutedPublicHandlers::default(),
            routed_authz_handlers: RoutedAuthzHandlers::default(),
            list_authorizer: LateBoundListAuthorizer::default(),
            fresh_authorization: LateBoundFreshAuthorization::default(),
            distributed_control: LateBoundDistributedControl::default(),
            name_resolution: LateBoundLogicalNameResolution::default(),
            program_quiescence: LateBoundProgramQuiescence::default(),
            index_artifacts: LateBoundIndexArtifactPublication::default(),
            routed_index_queries: RoutedIndexQueryHandlers::default(),
            join_transport,
            bootstrap_pins,
            clear_pins_on_drop: true,
        },
    ))
}

async fn ensure_cluster_identity(decisions: &DecisionRaft, expected: ClusterId) -> Result<()> {
    match decisions.state()?.cluster_id() {
        Some(committed) => {
            anyhow::ensure!(
                committed == expected,
                "persisted node identity belongs to another cluster"
            );
        }
        None => {
            let committed = decisions
                .submit(Command::InitializeCluster {
                    cluster_id: expected,
                })
                .await
                .context("commit persisted genesis cluster identity")?;
            anyhow::ensure!(
                matches!(
                    committed.result,
                    ApplyResult::ClusterInitialized { cluster_id } if cluster_id == expected
                ),
                "cluster identity command returned an unexpected result"
            );
        }
    }
    Ok(())
}

async fn admit_genesis_descriptor(
    decisions: &DecisionRaft,
    node_id: NodeId,
    peer_address: &PeerAddress,
    peer_pin: PeerSpkiSha256,
) -> Result<()> {
    let descriptor = NodeDescriptor {
        node_id,
        peer_address: peer_address.clone(),
        storage_weight_millionths: GENESIS_STORAGE_WEIGHT_MILLIONTHS,
        state: NodeState::Joining,
        current_peer_spki_sha256: peer_pin,
        overlap_peer_spki_sha256: None,
        join_capability_hash: Some(genesis_transition_hash(node_id, peer_pin)),
        supported_protocol: CapabilityRange {
            min: PEER_PROTOCOL_VERSION,
            max: PEER_PROTOCOL_VERSION,
        },
        supported_storage_format: CapabilityRange {
            min: STORAGE_FORMAT_VERSION,
            max: STORAGE_FORMAT_VERSION,
        },
    };
    let begin = match decisions
        .submit(Command::BeginAddNode {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            descriptor: descriptor.clone(),
        })
        .await
    {
        Ok(begin) => begin,
        Err(DecisionRaftError::Rejected(ApplyError::RaftMemberAddressMismatch { .. })) => {
            migrate_legacy_peer_address(decisions, peer_address).await?;
            decisions
                .submit(Command::BeginAddNode {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    descriptor,
                })
                .await
                .context("admit genesis node after legacy address migration")?
        }
        Err(error) => return Err(error).context("admit genesis node descriptor"),
    };
    let started_log_index = match begin.result {
        ApplyResult::MembershipTransitionBegun(transition)
            if transition.kind == MembershipTransitionKind::Add
                && transition.node_id == node_id =>
        {
            transition.started_log_index
        }
        result => bail!("genesis node admission returned unexpected result {result:?}"),
    };

    for _ in 0..2 {
        let state = decisions.state()?;
        let Some(transition) = state.cluster_control().transition() else {
            break;
        };
        anyhow::ensure!(
            transition.kind == MembershipTransitionKind::Add
                && transition.node_id == node_id
                && transition.started_log_index == started_log_index,
            "another membership transition replaced genesis node admission"
        );
        decisions
            .submit(Command::CompleteMembershipTransition {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                started_log_index,
            })
            .await
            .context("complete genesis node admission")?;
    }
    anyhow::ensure!(
        decisions.state()?.cluster_control().transition().is_none(),
        "genesis node admission did not finish in its bounded two steps"
    );
    Ok(())
}

fn validate_restart_state(
    decisions: &DecisionRaft,
    identity: &LocalNodeIdentity,
    configured_address: &PeerAddress,
    presented_pin: PeerSpkiSha256,
) -> Result<()> {
    let state = decisions.state()?;
    anyhow::ensure!(
        state.cluster_id() == Some(identity.cluster_id()),
        "persisted node identity does not match committed cluster identity"
    );
    let descriptor = state
        .cluster_control()
        .nodes()
        .get(&identity.node_id())
        .context("local node has no committed cluster descriptor")?;
    anyhow::ensure!(
        descriptor.state == NodeState::Active,
        "local node is not ACTIVE in committed cluster state"
    );
    anyhow::ensure!(
        descriptor.peer_address == *configured_address,
        "configured peer advertise address does not match committed node descriptor"
    );
    let committed = CommittedPeerPins {
        current: descriptor.current_peer_spki_sha256,
        overlap: descriptor.overlap_peer_spki_sha256,
    };
    anyhow::ensure!(
        committed.contains(presented_pin),
        "persisted presented peer certificate is not committed for this node"
    );
    if let Some(overlap) = identity.overlap_peer_identity() {
        let overlap = PeerTlsIdentity::from_pem(
            overlap.certificate_pem().as_bytes(),
            overlap.private_key_pem().as_bytes(),
        )
        .context("parse persisted overlap peer identity")?;
        anyhow::ensure!(
            committed.contains(overlap.spki_sha256()),
            "persisted overlap peer certificate is not committed for this node"
        );
    }
    Ok(())
}

fn validate_joining_restart_state(
    decisions: &DecisionRaft,
    identity: &LocalNodeIdentity,
    configured_address: &PeerAddress,
    presented_pin: PeerSpkiSha256,
) -> Result<()> {
    let pending = identity
        .pending_join()
        .context("JOINING restart has no persisted one-time join material")?;
    let state = decisions.state()?;
    anyhow::ensure!(
        state.cluster_id() == Some(identity.cluster_id()),
        "JOINING identity does not match the installed cluster snapshot"
    );
    let descriptor = state
        .cluster_control()
        .nodes()
        .get(&identity.node_id())
        .context("JOINING node has no committed cluster descriptor")?;
    anyhow::ensure!(
        descriptor.peer_address == *configured_address
            && descriptor.peer_address == *pending.peer_address(),
        "JOINING peer address differs from its committed descriptor"
    );
    anyhow::ensure!(
        descriptor.storage_weight_millionths == pending.storage_weight_millionths(),
        "JOINING storage weight differs from its committed descriptor"
    );
    anyhow::ensure!(
        descriptor.current_peer_spki_sha256 == presented_pin,
        "JOINING certificate differs from its committed descriptor"
    );
    match descriptor.state {
        NodeState::Joining => {
            anyhow::ensure!(
                descriptor.join_capability_hash
                    == Some(crate::join_bundle::hash_capability(pending.capability())),
                "JOINING capability differs from its committed descriptor"
            );
            let transition = state
                .cluster_control()
                .transition()
                .context("JOINING descriptor has no membership transition")?;
            anyhow::ensure!(
                transition.kind == MembershipTransitionKind::Add
                    && transition.node_id == identity.node_id(),
                "JOINING descriptor is not the current ADD transition"
            );
        }
        NodeState::Active => {
            anyhow::ensure!(
                descriptor.join_capability_hash.is_none(),
                "ACTIVE descriptor retained a join capability"
            );
        }
    }
    Ok(())
}

fn genesis_transition_hash(node_id: NodeId, peer_pin: PeerSpkiSha256) -> JoinCapabilityHash {
    let mut hasher = blake3::Hasher::new_derive_key("anvil.cluster/genesis-transition/v1");
    hasher.update(&node_id.0.to_be_bytes());
    hasher.update(&peer_pin.0);
    JoinCapabilityHash(*hasher.finalize().as_bytes())
}

fn legacy_decision_state_exists(data_dir: &Path) -> Result<bool> {
    let path = data_dir.join("decisions");
    path.try_exists()
        .with_context(|| format!("inspect legacy decision state at {}", path.display()))
}

async fn migrate_legacy_peer_address(
    decisions: &DecisionRaft,
    peer_address: &PeerAddress,
) -> Result<()> {
    decisions
        .migrate_released_single_node_address(peer_address.0.clone())
        .await
        .context("replace released one-node Raft peer address")
}

/// Open only the released one-node decision state long enough to bind its
/// existing cluster identity to new private peer material and replace the
/// synthetic `anvil-local://N` address. No public or peer listener is exposed
/// during this bounded migration.
async fn migrate_released_identity(
    config: &OpenPeerConfig<'_>,
) -> Result<Option<LocalNodeIdentity>> {
    let decisions = DecisionRaft::open(
        config.data_dir.join("decisions"),
        config.node_id.0,
        config.max_commit_entries,
        config.max_commit_bytes,
    )
    .await
    .context("open released one-node decision state for peer migration")?;
    if !decisions.is_initialized().await? {
        decisions.shutdown().await?;
        return Ok(None);
    }
    decisions
        .wait_for_leader(config.leader_timeout)
        .await
        .context("elect released one-node leader for peer migration")?;
    let state = decisions.state()?;
    anyhow::ensure!(
        state.cluster_control().nodes().is_empty(),
        "node identity is missing from an already admitted cluster node"
    );
    let cluster_id = state
        .cluster_id()
        .context("released decision state has no committed cluster identity")?;
    let identity = node_identity::generate(cluster_id, config.node_id)
        .context("generate peer identity for released one-node state")?;
    node_identity::create(config.data_dir, &identity)
        .context("persist migrated mode-0600 node identity")?;
    migrate_legacy_peer_address(&decisions, &config.peer_address).await?;
    decisions.shutdown().await?;
    tracing::info!(
        path = %node_identity::identity_path(config.data_dir).display(),
        "migrated released one-node state to its mode-0600 cluster identity"
    );
    Ok(Some(identity))
}

struct RaftCommittedPeerPins {
    cluster_id: ClusterId,
    local_node_id: NodeId,
    decisions: RwLock<Option<DecisionRaft>>,
    bootstrap: Option<Arc<JoinBootstrapPins>>,
}

impl RaftCommittedPeerPins {
    fn new(
        cluster_id: ClusterId,
        local_node_id: NodeId,
        bootstrap: Option<Arc<JoinBootstrapPins>>,
    ) -> Self {
        Self {
            cluster_id,
            local_node_id,
            decisions: RwLock::new(None),
            bootstrap,
        }
    }

    fn install(&self, decisions: DecisionRaft) -> Result<()> {
        let mut installed = self
            .decisions
            .write()
            .map_err(|_| anyhow::anyhow!("committed peer-pin provider lock is poisoned"))?;
        anyhow::ensure!(
            installed.is_none(),
            "committed peer-pin provider was already installed"
        );
        *installed = Some(decisions);
        Ok(())
    }

    fn clear(&self) {
        if let Ok(mut installed) = self.decisions.write() {
            *installed = None;
        }
    }

    fn committed_state(&self) -> Option<anvil_consensus::StateMachine> {
        let decisions = self.decisions.read().ok()?.clone()?;
        decisions.state().ok()
    }
}

impl CommittedPeerPinProvider for RaftCommittedPeerPins {
    fn connection_pins(&self, node_id: NodeId) -> Option<CommittedPeerPins> {
        let state = self.committed_state()?;
        if !state
            .cluster_control()
            .nodes()
            .contains_key(&self.local_node_id)
        {
            return self.bootstrap.as_ref()?.connection_pins(node_id);
        }
        match state.cluster_id() {
            Some(cluster_id) if cluster_id == self.cluster_id => {
                let descriptor = state.cluster_control().nodes().get(&node_id)?;
                Some(CommittedPeerPins {
                    current: descriptor.current_peer_spki_sha256,
                    overlap: descriptor.overlap_peer_spki_sha256,
                })
            }
            Some(_) => None,
            None => self.bootstrap.as_ref()?.connection_pins(node_id),
        }
    }

    fn authorized_rpc_pins(
        &self,
        cluster_id: ClusterId,
        node_id: NodeId,
        kind: PeerRpcKind,
    ) -> Option<CommittedPeerPins> {
        if cluster_id != self.cluster_id {
            return None;
        }
        let state = self.committed_state()?;
        if !state
            .cluster_control()
            .nodes()
            .contains_key(&self.local_node_id)
        {
            return self
                .bootstrap
                .as_ref()?
                .authorized_catch_up_pins(cluster_id, node_id, kind);
        }
        if let Some(committed_cluster) = state.cluster_id() {
            if committed_cluster != self.cluster_id {
                return None;
            }
            let descriptor = state.cluster_control().nodes().get(&node_id)?;
            let allowed = match kind {
                PeerRpcKind::JoinControl => {
                    matches!(descriptor.state, NodeState::Active | NodeState::Joining)
                }
                PeerRpcKind::AppendEntries
                | PeerRpcKind::Vote
                | PeerRpcKind::InstallSnapshot
                | PeerRpcKind::ServingLease
                | PeerRpcKind::DataPlane
                | PeerRpcKind::StateTransfer => descriptor.state == NodeState::Active,
            };
            return allowed.then_some(CommittedPeerPins {
                current: descriptor.current_peer_spki_sha256,
                overlap: descriptor.overlap_peer_spki_sha256,
            });
        }
        self.bootstrap
            .as_ref()?
            .authorized_catch_up_pins(cluster_id, node_id, kind)
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use anvil_store::StoreOptions;

    use super::*;
    use crate::join_bundle::{JoinBundle, JoinSeed};
    use crate::serving_fence::ServingFenceRuntime;

    struct AllowCompletedHandoff;

    #[tonic::async_trait]
    impl JoinActivationGate for AllowCompletedHandoff {
        async fn ensure_handoff_complete(
            &self,
            _descriptor: &NodeDescriptor,
            _transition: &anvil_consensus::MembershipTransition,
        ) -> Result<crate::join_peer::JoinActivationPermit, tonic::Status> {
            Ok(crate::join_peer::JoinActivationPermit::test_only())
        }
    }

    fn unused_loopback() -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    }

    #[test]
    fn concrete_listener_is_the_default_advertised_address() {
        let address = peer_address("127.0.0.1:50052".parse().unwrap(), None).unwrap();
        assert_eq!(address.0, "127.0.0.1:50052");
    }

    #[test]
    fn wildcard_listener_requires_an_explicit_advertised_address() {
        let error = peer_address("0.0.0.0:50052".parse().unwrap(), None).unwrap_err();
        assert!(error.to_string().contains("--peer-advertise is required"));
        let address =
            peer_address("0.0.0.0:50052".parse().unwrap(), Some("localhost:50052")).unwrap();
        assert_eq!(address.0, "localhost:50052");
    }

    #[test]
    fn advertised_address_is_bounded_and_connectable() {
        for invalid in [
            "",
            "localhost",
            "localhost:0",
            "http://localhost:50052",
            "localhost:50 052",
        ] {
            assert!(
                validate_peer_address(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[tokio::test]
    async fn fresh_genesis_persists_identity_and_active_descriptor() {
        let directory = tempfile::tempdir().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let peer_listen = listener.local_addr().unwrap();
        drop(listener);
        let advertised = PeerAddress(peer_listen.to_string());
        let store = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        let (decisions, runtime) = open(OpenPeerConfig {
            data_dir: directory.path(),
            node_id: NodeId(1),
            peer_address: advertised.clone(),
            join_bundle: None,
            run_system_bootstrap: true,
            max_commit_entries: 16,
            max_commit_bytes: 64 * 1024,
            leader_timeout: Duration::from_secs(10),
        })
        .await
        .unwrap();
        let state = decisions.state().unwrap();
        assert_eq!(state.cluster_control().nodes().len(), 1);
        let descriptor = &state.cluster_control().nodes()[&NodeId(1)];
        assert_eq!(descriptor.state, NodeState::Active);
        assert_eq!(descriptor.peer_address, advertised);
        assert_eq!(descriptor.storage_weight_millionths, 1_000_000);
        assert!(state.cluster_control().transition().is_none());
        assert_eq!(
            std::fs::metadata(node_identity::identity_path(directory.path()))
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o600
        );
        let serving_transport = runtime.serving_transport();
        let mut peer_server = runtime
            .start(
                peer_listen,
                decisions.clone(),
                store,
                ErasureProfile::default(),
                Duration::from_secs(30),
                16 * 1024 * 1024,
            )
            .await
            .unwrap();
        // Plain TCP cannot reach a Tonic handler, and one rejected handshake
        // must not take the private listener down.
        drop(tokio::net::TcpStream::connect(peer_listen).await.unwrap());
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!peer_server.task_mut().is_finished());
        let serving = ServingFenceRuntime::start(
            decisions.clone(),
            serving_transport,
            Duration::from_secs(8),
        )
        .await
        .unwrap();
        assert!(serving.authority().has_valid_lease());
        tokio::time::sleep(Duration::from_millis(750)).await;
        assert!(serving.authority().has_valid_lease());
        serving.shutdown().await;
        peer_server.shutdown().await.unwrap();
        decisions.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn released_one_node_state_migrates_in_place_before_listening() {
        let directory = tempfile::tempdir().unwrap();
        let released = DecisionRaft::open(directory.path().join("decisions"), 1, 16, 64 * 1024)
            .await
            .unwrap();
        released.ensure_one_node().await.unwrap();
        released
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        released
            .submit(Command::InitializeCluster {
                cluster_id: ClusterId([44; 16]),
            })
            .await
            .unwrap();
        released.shutdown().await.unwrap();
        drop(released);

        let advertised = PeerAddress("127.0.0.1:51052".into());
        let (decisions, runtime) = open(OpenPeerConfig {
            data_dir: directory.path(),
            node_id: NodeId(1),
            peer_address: advertised.clone(),
            join_bundle: None,
            run_system_bootstrap: false,
            max_commit_entries: 16,
            max_commit_bytes: 64 * 1024,
            leader_timeout: Duration::from_secs(10),
        })
        .await
        .unwrap();

        let identity = node_identity::load_for_node(directory.path(), NodeId(1)).unwrap();
        assert_eq!(identity.cluster_id(), ClusterId([44; 16]));
        let state = decisions.state().unwrap();
        assert_eq!(state.cluster_id(), Some(ClusterId([44; 16])));
        assert_eq!(
            state.cluster_control().nodes()[&NodeId(1)].peer_address,
            advertised
        );
        assert_eq!(
            state.cluster_control().nodes()[&NodeId(1)].state,
            NodeState::Active
        );

        drop(runtime);
        decisions.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn copied_bundle_joins_as_learner_then_activates_with_exact_fixed_voters() {
        let first_directory = tempfile::tempdir().unwrap();
        let second_directory = tempfile::tempdir().unwrap();
        let first_address = unused_loopback();
        let second_address = unused_loopback();
        let first_peer = PeerAddress(first_address.to_string());
        let second_peer = PeerAddress(second_address.to_string());
        let first_store = Store::open(StoreOptions::new(first_directory.path(), 1))
            .await
            .unwrap();
        let second_store = Store::open(StoreOptions::new(second_directory.path(), 2))
            .await
            .unwrap();

        let (first_decisions, first_runtime) = open(OpenPeerConfig {
            data_dir: first_directory.path(),
            node_id: NodeId(1),
            peer_address: first_peer.clone(),
            join_bundle: None,
            run_system_bootstrap: true,
            max_commit_entries: 16,
            max_commit_bytes: 64 * 1024,
            leader_timeout: Duration::from_secs(10),
        })
        .await
        .unwrap();
        let cluster_id = first_decisions.state().unwrap().cluster_id().unwrap();
        let first_pin = first_runtime.identity.spki_sha256();
        let bundle = JoinBundle::generate(
            cluster_id,
            NodeId(2),
            second_peer.clone(),
            500_000,
            vec![JoinSeed {
                node_id: NodeId(1),
                peer_address: first_peer.clone(),
                current_peer_spki_sha256: first_pin,
                overlap_peer_spki_sha256: None,
            }],
        )
        .unwrap();
        let copied_bundle = second_directory.path().join("copied-join.json");
        crate::join_bundle::write(&copied_bundle, &bundle).unwrap();
        let begun = first_decisions
            .submit(Command::BeginAddNode {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                descriptor: NodeDescriptor {
                    node_id: NodeId(2),
                    peer_address: second_peer.clone(),
                    storage_weight_millionths: 500_000,
                    state: NodeState::Joining,
                    current_peer_spki_sha256: bundle.peer_spki_sha256().unwrap(),
                    overlap_peer_spki_sha256: None,
                    join_capability_hash: Some(bundle.capability_hash()),
                    supported_protocol: CapabilityRange { min: 1, max: 1 },
                    supported_storage_format: CapabilityRange { min: 1, max: 1 },
                },
            })
            .await
            .unwrap();
        assert!(matches!(
            begun.result,
            ApplyResult::MembershipTransitionBegun(_)
        ));

        let mut first_server = first_runtime
            .start_with_activation_gate(
                first_address,
                first_decisions.clone(),
                first_store,
                ErasureProfile::default(),
                Duration::from_secs(30),
                16 * 1024 * 1024,
                ServingLeaseIssuer::new(),
                Arc::new(AllowCompletedHandoff),
            )
            .await
            .unwrap();
        let (second_decisions, second_runtime) = open(OpenPeerConfig {
            data_dir: second_directory.path(),
            node_id: NodeId(2),
            peer_address: second_peer,
            join_bundle: Some(&copied_bundle),
            run_system_bootstrap: true,
            max_commit_entries: 16,
            max_commit_bytes: 64 * 1024,
            leader_timeout: Duration::from_secs(10),
        })
        .await
        .unwrap();
        assert!(!copied_bundle.exists());
        assert!(!second_decisions.is_initialized().await.unwrap());
        let join = second_runtime.join_transport().unwrap();
        let mut second_server = second_runtime
            .start(
                second_address,
                second_decisions.clone(),
                second_store,
                ErasureProfile::default(),
                Duration::from_secs(30),
                16 * 1024 * 1024,
            )
            .await
            .unwrap();

        complete_pending_join(
            &join,
            &second_decisions,
            second_directory.path(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert_eq!(
            first_decisions.state().unwrap().cluster_control().nodes()[&NodeId(2)].state,
            NodeState::Active
        );
        assert_eq!(
            second_decisions.state().unwrap().cluster_control().nodes()[&NodeId(2)].state,
            NodeState::Active
        );
        assert_eq!(
            first_decisions.committed_voter_ids().unwrap(),
            std::collections::BTreeSet::from([NodeId(1), NodeId(2)])
        );
        assert_eq!(
            second_decisions.committed_voter_ids().unwrap(),
            std::collections::BTreeSet::from([NodeId(1), NodeId(2)])
        );
        assert!(
            node_identity::load_for_node(second_directory.path(), NodeId(2))
                .unwrap()
                .pending_join()
                .is_none()
        );

        second_server.shutdown().await.unwrap();
        first_server.shutdown().await.unwrap();
        second_decisions.shutdown().await.unwrap();
        first_decisions.shutdown().await.unwrap();
    }
}
