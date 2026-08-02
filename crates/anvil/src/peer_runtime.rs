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
    PeerTlsConnector, PeerTlsError, PeerTlsIdentity, TonicPeerTransport, TonicRaftPeerService,
};
use anyhow::{Context, Result, bail};
use tonic::codegen::tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use uuid::Uuid;

use crate::node_identity::{self, LocalNodeIdentity};

const GENESIS_STORAGE_WEIGHT_MILLIONTHS: u32 = 1_000_000;
const PEER_PROTOCOL_VERSION: u16 = 1;
const STORAGE_FORMAT_VERSION: u16 = 1;

pub(crate) struct OpenPeerConfig<'a> {
    pub(crate) data_dir: &'a Path,
    pub(crate) node_id: NodeId,
    pub(crate) peer_address: PeerAddress,
    pub(crate) run_system_bootstrap: bool,
    pub(crate) max_commit_entries: u32,
    pub(crate) max_commit_bytes: u64,
    pub(crate) leader_timeout: Duration,
}

pub(crate) struct PeerRuntime {
    identity: Arc<PeerTlsIdentity>,
    pins: Arc<RaftCommittedPeerPins>,
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
    let migrated_identity = if !identity_exists && legacy_decision_state_exists(config.data_dir)? {
        migrate_released_identity(&config).await?
    } else {
        None
    };

    let identity = if identity_exists {
        node_identity::load_for_node(config.data_dir, config.node_id)
            .context("load local node identity")?
    } else if let Some(identity) = migrated_identity {
        identity
    } else {
        anyhow::ensure!(
            config.run_system_bootstrap,
            "an empty node requires --run-system-bootstrap when no seed nodes are configured"
        );
        let cluster_id = ClusterId(*Uuid::new_v4().as_bytes());
        let identity = node_identity::generate(cluster_id, config.node_id)
            .context("generate local peer identity")?;
        node_identity::create(config.data_dir, &identity).context("persist local peer identity")?;
        tracing::info!(
            path = %node_identity::identity_path(config.data_dir).display(),
            "generated mode-0600 cluster node identity"
        );
        identity
    };

    let (decisions, runtime) = open_with_identity(&config, &identity).await?;
    if !decisions.is_initialized().await? {
        decisions
            .initialize_genesis(std::collections::BTreeMap::from([(
                config.node_id.0,
                PeerNode::new(config.peer_address.0.clone()),
            )]))
            .await
            .context("initialize one-voter genesis Raft group")?;
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

impl PeerRuntime {
    pub(crate) async fn start(
        mut self,
        peer_listen: SocketAddr,
        decisions: DecisionRaft,
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
        let service = TonicRaftPeerService::new(decisions, self.pins.clone()).into_server();
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
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
    let pins = Arc::new(RaftCommittedPeerPins::new(identity.cluster_id()));
    let connector =
        PeerTlsConnector::new(tls_identity.clone(), pins.clone(), PeerTlsConfig::default())
            .context("configure mandatory peer mTLS connector")?;
    let transport = Arc::new(
        TonicPeerTransport::new(identity.cluster_id(), config.node_id, connector)
            .context("configure private Raft transport")?,
    );
    let decisions = DecisionRaft::open_with_transport(
        config.data_dir.join("decisions"),
        config.node_id.0,
        config.max_commit_entries,
        config.max_commit_bytes,
        transport,
    )
    .await
    .context("open bounded decision Raft with private transport")?;
    pins.install(decisions.clone())?;
    Ok((
        decisions,
        PeerRuntime {
            identity: tls_identity,
            pins,
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
    decisions: RwLock<Option<DecisionRaft>>,
}

impl RaftCommittedPeerPins {
    fn new(cluster_id: ClusterId) -> Self {
        Self {
            cluster_id,
            decisions: RwLock::new(None),
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

    fn pins(&self, node_id: NodeId) -> Option<CommittedPeerPins> {
        let decisions = self.decisions.read().ok()?.clone()?;
        let state = decisions.state().ok()?;
        if state.cluster_id()? != self.cluster_id {
            return None;
        }
        let descriptor = state.cluster_control().nodes().get(&node_id)?;
        Some(CommittedPeerPins {
            current: descriptor.current_peer_spki_sha256,
            overlap: descriptor.overlap_peer_spki_sha256,
        })
    }
}

impl CommittedPeerPinProvider for RaftCommittedPeerPins {
    fn connection_pins(&self, node_id: NodeId) -> Option<CommittedPeerPins> {
        self.pins(node_id)
    }

    fn authorized_rpc_pins(
        &self,
        cluster_id: ClusterId,
        node_id: NodeId,
        _kind: PeerRpcKind,
    ) -> Option<CommittedPeerPins> {
        if cluster_id != self.cluster_id {
            return None;
        }
        self.pins(node_id)
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

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
        let advertised = PeerAddress("127.0.0.1:50052".into());
        let (decisions, runtime) = open(OpenPeerConfig {
            data_dir: directory.path(),
            node_id: NodeId(1),
            peer_address: advertised.clone(),
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
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let peer_listen = listener.local_addr().unwrap();
        drop(listener);
        let mut peer_server = runtime.start(peer_listen, decisions.clone()).await.unwrap();
        // Plain TCP cannot reach a Tonic handler, and one rejected handshake
        // must not take the private listener down.
        drop(tokio::net::TcpStream::connect(peer_listen).await.unwrap());
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!peer_server.task_mut().is_finished());
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
}
