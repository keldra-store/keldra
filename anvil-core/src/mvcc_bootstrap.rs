//! Mandatory construction of the MVCC-under-Raft node subsystem.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use anvil_mvcc_consensus::{ConsensusNode, NodeId, OpenRaftConsensus, RocksRaftStore};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tonic::{Status, metadata::MetadataMap};

use crate::{
    Config,
    anvil_api::{ConsensusSessionOpen, ReplicationSessionOpen},
    auth,
    bundle_replication::{
        AppendOnlyPreparedBundleStore, BundleTarget, ObjectEvidenceRegistry,
        StreamingBundleReplicator,
    },
    local_object_store::LocalObjectStore,
    mesh_lifecycle::NodeCapability,
    mvcc_apply_worker::{ApplyWorkerState, MvccApplyWorker},
    mvcc_node_runtime::MvccNodeRuntime,
    mvcc_open_transactions::OpenTransactionRegistry,
    mvcc_store::LocalMvccStore,
    mvcc_transaction::{
        ClusterOwnershipClaim, ClusterOwnershipResolver, DurabilityPolicy, NodeIncarnation,
        OwnedResource,
    },
    replication::AuthenticatedPeer,
    replication_client::{
        ReplicationPeer, ReplicationStreamOptions, TonicReplicationStreamManager,
    },
    services::{
        consensus_transport::{
            ConsensusConnectionAuthorizer, ConsensusTransportService, TonicConsensusRpcFactory,
        },
        replication::{ReplicationConnectionAuthorizer, ReplicationServiceImpl},
    },
    shard_placement::ShardTarget,
    system_realm,
};

pub type ProductMvccRuntime = MvccNodeRuntime<
    AppendOnlyPreparedBundleStore,
    StreamingBundleReplicator<TonicReplicationStreamManager>,
    OpenRaftConsensus,
>;

#[derive(Clone)]
struct RaftClusterOwnershipResolver {
    cluster_id: String,
    consensus: Arc<OpenRaftConsensus>,
}

impl ClusterOwnershipResolver for RaftClusterOwnershipResolver {
    fn validate_claim(
        &self,
        transaction_cluster_id: &str,
        claim: &ClusterOwnershipClaim,
    ) -> Result<()> {
        if transaction_cluster_id != self.cluster_id || claim.cluster_id() != self.cluster_id {
            bail!("transaction resource belongs to another cluster");
        }
        if let OwnedResource::OutboxEvent {
            destination_partition_id,
            ..
        } = claim.resource()
        {
            let snapshot = self.consensus.applied_control_snapshot()?;
            if !snapshot
                .partitions
                .iter()
                .any(|(partition_id, _)| partition_id == destination_partition_id)
            {
                bail!(
                    "outbox destination partition {destination_partition_id} has no applied Raft assignment"
                );
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MvccPeerConfig {
    pub cluster_id: String,
    pub raft_node_id: u64,
    pub node_id: String,
    pub incarnation: u64,
    pub endpoint: String,
    pub failure_domain: String,
    #[serde(default = "default_voter")]
    pub voter: bool,
}

fn default_voter() -> bool {
    true
}

#[derive(Clone)]
pub struct NodeConnectionAuthorizer {
    cluster_id: Arc<str>,
    token: Arc<str>,
    raft_nodes: Arc<BTreeMap<u64, MvccPeerConfig>>,
    replication_nodes: Arc<BTreeMap<(String, u64), MvccPeerConfig>>,
    storage: crate::storage::Storage,
    core_store: crate::core_store::CoreStore,
    mesh_id: Arc<str>,
    allow_test_bypass: bool,
    consensus: Arc<OpenRaftConsensus>,
}

impl NodeConnectionAuthorizer {
    fn new(
        cluster_id: impl Into<Arc<str>>,
        token: impl Into<Arc<str>>,
        peers: &[MvccPeerConfig],
        storage: crate::storage::Storage,
        core_store: crate::core_store::CoreStore,
        mesh_id: impl Into<Arc<str>>,
        allow_test_bypass: bool,
        consensus: Arc<OpenRaftConsensus>,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            token: token.into(),
            raft_nodes: Arc::new(
                peers
                    .iter()
                    .cloned()
                    .map(|peer| (peer.raft_node_id, peer))
                    .collect(),
            ),
            replication_nodes: Arc::new(
                peers
                    .iter()
                    .cloned()
                    .map(|peer| ((peer.node_id.clone(), peer.incarnation), peer))
                    .collect(),
            ),
            storage,
            core_store,
            mesh_id: mesh_id.into(),
            allow_test_bypass,
            consensus,
        }
    }

    fn authorize_token(&self, metadata: &MetadataMap) -> Result<(), Status> {
        let presented = metadata
            .get("x-anvil-node-token")
            .and_then(|value| value.to_str().ok());
        if presented != Some(self.token.as_ref()) {
            return Err(Status::unauthenticated("invalid node connection token"));
        }
        Ok(())
    }

    async fn authorize_zanzibar(&self, node_id: &str) -> Result<(), Status> {
        if self.allow_test_bypass {
            return Ok(());
        }
        let claims = auth::Claims {
            sub: node_id.to_string(),
            exp: usize::MAX,
            tenant_id: crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
            jti: None,
        };
        let allowed = system_realm::check_internal_node_access(
            &self.storage,
            &self.core_store,
            &self.mesh_id,
            &claims,
            node_id,
            NodeCapability::Metadata,
        )
        .await
        .map_err(|error| Status::permission_denied(error.to_string()))?;
        if !allowed {
            return Err(Status::permission_denied(
                "active Zanzibar node grant and metadata capability required",
            ));
        }
        Ok(())
    }

    fn authorize_control_incarnation(&self, peer: &MvccPeerConfig) -> Result<(), Status> {
        let snapshot = self
            .consensus
            .applied_control_snapshot()
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let installed = snapshot.nodes.iter().any(|(node_id, incarnation, domain)| {
            *node_id == consensus_control_node_id(&peer.node_id)
                && *incarnation == peer.incarnation
                && domain == &peer.failure_domain
        });
        if !installed {
            return Err(Status::permission_denied(
                "node incarnation or failure-domain assignment is stale in Raft control state",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ConsensusConnectionAuthorizer for NodeConnectionAuthorizer {
    async fn authorize(
        &self,
        metadata: &MetadataMap,
        open: &ConsensusSessionOpen,
    ) -> Result<(), Status> {
        self.authorize_token(metadata)?;
        if open.cluster_id != self.cluster_id.as_ref() {
            return Err(Status::permission_denied(
                "consensus stream belongs to another cluster",
            ));
        }
        let peer = self
            .raft_nodes
            .get(&open.node_id)
            .ok_or_else(|| Status::permission_denied("node is not in Raft peer configuration"))?;
        if peer.incarnation != open.node_incarnation {
            return Err(Status::permission_denied("stale Raft node incarnation"));
        }
        self.authorize_control_incarnation(peer)?;
        self.authorize_zanzibar(&peer.node_id).await?;
        Ok(())
    }
}

#[async_trait]
impl ReplicationConnectionAuthorizer for NodeConnectionAuthorizer {
    async fn authorize(
        &self,
        metadata: &MetadataMap,
        open: &ReplicationSessionOpen,
    ) -> Result<AuthenticatedPeer, Status> {
        self.authorize_token(metadata)?;
        if open.cluster_id != self.cluster_id.as_ref() {
            return Err(Status::permission_denied(
                "replication stream belongs to another cluster",
            ));
        }
        let peer = self
            .replication_nodes
            .get(&(open.node_id.clone(), open.node_incarnation))
            .ok_or_else(|| {
                Status::permission_denied("node incarnation is not in peer configuration")
            })?;
        self.authorize_control_incarnation(peer)?;
        self.authorize_zanzibar(&peer.node_id).await?;
        AuthenticatedPeer::new_bound(
            open.node_id.clone(),
            open.node_incarnation,
            peer.endpoint.clone(),
        )
        .map_err(|error| Status::permission_denied(error.to_string()))
    }
}

pub struct MvccSubsystem {
    pub consensus: Arc<OpenRaftConsensus>,
    pub runtime: Arc<ProductMvccRuntime>,
    pub open_transactions: Arc<OpenTransactionRegistry>,
    pub replication_client: TonicReplicationStreamManager,
    pub object_evidence: ObjectEvidenceRegistry,
    pub local_objects: LocalObjectStore,
    pub materialisation_storage: crate::storage::Storage,
    pub materialisation_signing_key: Arc<[u8]>,
    pub materialisation_embedding_providers: crate::embedding_provider::EmbeddingProviderRegistry,
    pub consensus_service: ConsensusTransportService<NodeConnectionAuthorizer>,
    pub replication_service: ReplicationServiceImpl<NodeConnectionAuthorizer>,
    pub peers: Arc<[MvccPeerConfig]>,
    pub local_node: NodeIncarnation,
    pub apply_worker_state: Arc<tokio::sync::Mutex<ApplyWorkerState>>,
    apply_shutdown: tokio::sync::watch::Sender<bool>,
    apply_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    object_materialisation_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    shard_repair_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    shard_rebalance_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    outbox_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    assignment_reconciler_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl fmt::Debug for MvccSubsystem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MvccSubsystem")
            .field("peers", &self.peers)
            .finish_non_exhaustive()
    }
}

impl Drop for MvccSubsystem {
    fn drop(&mut self) {
        let _ = self.apply_shutdown.send(true);
    }
}

impl MvccSubsystem {
    pub fn live_shard_placement(&self) -> Result<(Arc<[ShardTarget]>, usize, u64)> {
        let snapshot = self.consensus.applied_control_snapshot()?;
        if snapshot.durability_policy.generation == 0 {
            bail!("Raft durability policy is not installed");
        }
        let mut candidates = Vec::new();
        for (raft_node_id, incarnation, failure_domain) in snapshot.nodes {
            let peer = self
                .peers
                .iter()
                .find(|peer| consensus_control_node_id(&peer.node_id) == raft_node_id)
                .context("Raft control state names a node without a transport route")?;
            if peer.incarnation != incarnation {
                bail!("Raft control state node incarnation is newer than its transport route");
            }
            candidates.push(ShardTarget {
                cluster_id: self.cluster_id().to_string(),
                node: NodeIncarnation {
                    node_id: peer.node_id.clone(),
                    incarnation,
                },
                failure_domain,
            });
        }
        candidates.sort_by(|left, right| left.node.cmp(&right.node));
        Ok((
            candidates.into(),
            usize::from(snapshot.durability_policy.tolerated_failure_domains),
            snapshot.topology_epoch,
        ))
    }

    pub fn cluster_id(&self) -> &str {
        self.peers
            .first()
            .map(|peer| peer.cluster_id.as_str())
            .expect("validated MVCC topology is non-empty")
    }

    pub async fn bootstrap(config: &Config, core_meta_db: Arc<DB>) -> Result<Self> {
        let peers = parse_and_validate_peers(config)?;
        validate_secure_peer_transport(config, &peers)?;
        let local = peers
            .iter()
            .find(|peer| peer.raft_node_id == config.mvcc_raft_node_id)
            .context("MVCC peer configuration does not contain the local Raft node ID")?;
        if local.node_id != config.node_id || local.incarnation != config.mvcc_node_incarnation {
            bail!("local MVCC peer identity does not match node configuration");
        }

        validate_cluster_id(&config.mvcc_cluster_id)?;
        let paths = MvccPaths::new(&config.storage_path, &config.mvcc_cluster_id);
        std::fs::create_dir_all(&paths.base)?;
        let raft_store = RocksRaftStore::from_db(core_meta_db.clone(), config.mvcc_raft_group_id)
            .context("attach MVCC Raft store to CoreMeta RocksDB")?;
        let raft_is_empty = raft_store
            .last_log_index()
            .context("inspect MVCC Raft log")?
            .is_none();
        let token = config.mvcc_node_token()?;
        let raft_network = Arc::new(TonicConsensusRpcFactory::new(
            config.mvcc_cluster_id.clone(),
            NodeId(config.mvcc_raft_node_id),
            config.mvcc_node_incarnation,
            token.clone(),
            Duration::from_millis(config.mvcc_rpc_timeout_ms),
        ));
        let consensus = Arc::new(
            OpenRaftConsensus::new(
                NodeId(config.mvcc_raft_node_id),
                raft_store,
                cluster_id_hash(&config.mvcc_cluster_id),
                config.mvcc_cluster_id.clone(),
                raft_network,
            )
            .await
            .context("start MVCC Raft runtime")?,
        );
        if config.mvcc_bootstrap_membership && raft_is_empty {
            let members = peers
                .iter()
                .filter(|peer| peer.voter)
                .map(|peer| {
                    (
                        NodeId(peer.raft_node_id),
                        ConsensusNode {
                            address: peer.endpoint.clone(),
                        },
                    )
                })
                .collect();
            consensus
                .initialize(members)
                .await
                .context("initialize MVCC Raft membership")?;
            for _ in 0..100 {
                if consensus.is_leader() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            if !consensus.is_leader() {
                bail!(
                    "local node did not become leader while installing initial Raft control state"
                );
            }
            let cluster_hash = cluster_id_hash(&config.mvcc_cluster_id);
            for peer in &peers {
                consensus
                    .install_node(
                        cluster_hash,
                        anvil_mvcc_consensus::NodeIncarnation {
                            node_id: consensus_control_node_id(&peer.node_id),
                            incarnation: peer.incarnation,
                        },
                        peer.failure_domain.clone(),
                    )
                    .await
                    .context("install initial node incarnation in Raft control state")?;
            }
            consensus
                .set_durability_policy(
                    cluster_hash,
                    1,
                    u16::try_from(config.mvcc_bundle_quorum_holders)?,
                    u16::try_from(config.mvcc_tolerated_failure_domains)?,
                )
                .await
                .context("install initial durability policy in Raft control state")?;
        }

        let local_incarnation = NodeIncarnation {
            node_id: local.node_id.clone(),
            incarnation: local.incarnation,
        };
        let replication_peers = peers
            .iter()
            .map(|peer| ReplicationPeer {
                cluster_id: config.mvcc_cluster_id.clone(),
                node: NodeIncarnation {
                    node_id: peer.node_id.clone(),
                    incarnation: peer.incarnation,
                },
                endpoint: peer.endpoint.clone(),
            })
            .collect::<Vec<_>>();
        let replication_client = TonicReplicationStreamManager::new(
            config.mvcc_cluster_id.clone(),
            local_incarnation.clone(),
            token.clone(),
            replication_peers,
            ReplicationStreamOptions {
                operation_timeout: Duration::from_millis(config.mvcc_rpc_timeout_ms),
                allow_insecure_transport_for_tests: config.allow_test_only_insecure_mvcc_transport,
                ..ReplicationStreamOptions::default()
            },
        )?;
        let targets = peers
            .iter()
            .filter(|peer| peer.raft_node_id != config.mvcc_raft_node_id)
            .map(|peer| BundleTarget {
                cluster_id: config.mvcc_cluster_id.clone(),
                node: NodeIncarnation {
                    node_id: peer.node_id.clone(),
                    incarnation: peer.incarnation,
                },
                failure_domain: peer.failure_domain.clone(),
            })
            .collect();
        let prepared = AppendOnlyPreparedBundleStore::open(
            &paths.prepared_bundles,
            config.mvcc_cluster_id.clone(),
            local_incarnation.clone(),
            local.failure_domain.clone(),
        )?;
        let local_objects = LocalObjectStore::open(
            &paths.local_objects,
            config.mvcc_cluster_id.clone(),
            local_incarnation.clone(),
            local.failure_domain.clone(),
        )?;
        let object_evidence = ObjectEvidenceRegistry::default();
        let replicator = StreamingBundleReplicator::new(
            replication_client.clone(),
            targets,
            object_evidence.clone(),
        )?;
        let local_store = LocalMvccStore::from_db(core_meta_db.clone(), &config.mvcc_cluster_id)?;
        let materialisation_storage = crate::storage::Storage::new_at(&config.storage_path).await?;
        let materialisation_signing_key =
            Arc::<[u8]>::from(hex::decode(&config.anvil_secret_encryption_key)?);
        let materialisation_embedding_providers =
            crate::embedding_provider::EmbeddingProviderRegistry::from_config(config)?;
        let runtime = Arc::new(MvccNodeRuntime::new_with_ownership_resolver(
            prepared.clone(),
            replicator,
            consensus.as_ref().clone(),
            DurabilityPolicy {
                bundle_quorum_holders: config.mvcc_bundle_quorum_holders,
                tolerated_failure_domains: config.mvcc_tolerated_failure_domains,
            },
            local_store.clone(),
            Arc::new(RaftClusterOwnershipResolver {
                cluster_id: config.mvcc_cluster_id.clone(),
                consensus: consensus.clone(),
            }),
        )?);
        crate::mvcc_assignment_reconciler::BackgroundAssignmentReconciler::new(
            config.mvcc_cluster_id.clone(),
            consensus.clone(),
            local_store.clone(),
        )?
        .run_once()
        .await
        .context("install initial background-work partition assignments")?;
        let open_transactions = Arc::new(OpenTransactionRegistry::from_db(core_meta_db)?);
        let authorization_core_store =
            crate::core_store::CoreStore::new(materialisation_storage.clone()).await?;
        let authorizer = NodeConnectionAuthorizer::new(
            config.mvcc_cluster_id.clone(),
            token,
            &peers,
            materialisation_storage.clone(),
            authorization_core_store,
            config.mesh_id.clone(),
            config.allow_test_only_insecure_mvcc_transport,
            consensus.clone(),
        );
        let consensus_service =
            ConsensusTransportService::new(consensus.clone(), authorizer.clone());
        let replication_service =
            ReplicationServiceImpl::open(authorizer, &paths.replication_inbox)?;
        let remote_nodes = peers
            .iter()
            .filter(|peer| peer.raft_node_id != config.mvcc_raft_node_id)
            .map(|peer| NodeIncarnation {
                node_id: peer.node_id.clone(),
                incarnation: peer.incarnation,
            })
            .collect::<Vec<_>>();
        let worker = MvccApplyWorker::new(
            consensus.clone(),
            config.mvcc_cluster_id.clone(),
            prepared,
            replication_client.clone(),
            remote_nodes,
            local_store,
        )
        .with_prepared_bundle_gc_grace(config.mvcc_prepared_bundle_gc_grace_ms)
        .context("configure prepared bundle GC grace")?;
        let apply_worker_state = worker.state_handle();
        let (apply_shutdown, apply_shutdown_rx) = tokio::sync::watch::channel(false);
        let apply_task = tokio::spawn(worker.run(apply_shutdown_rx));

        Ok(Self {
            consensus,
            runtime,
            open_transactions,
            replication_client,
            object_evidence,
            local_objects,
            materialisation_storage,
            materialisation_signing_key,
            materialisation_embedding_providers,
            consensus_service,
            replication_service,
            peers: peers.into(),
            local_node: local_incarnation,
            apply_worker_state,
            apply_shutdown,
            apply_task: Mutex::new(Some(apply_task)),
            object_materialisation_task: Mutex::new(None),
            shard_repair_task: Mutex::new(None),
            shard_rebalance_task: Mutex::new(None),
            outbox_task: Mutex::new(None),
            assignment_reconciler_task: Mutex::new(None),
        })
    }

    pub fn start_background_work(
        self: &Arc<Self>,
        core_store: crate::core_store::CoreStore,
        observability: crate::observability::Observability,
    ) -> Result<()> {
        let assignment_reconciler =
            crate::mvcc_assignment_reconciler::BackgroundAssignmentReconciler::new(
                self.cluster_id(),
                self.consensus.clone(),
                self.runtime.local_store().clone(),
            )?;
        let assignment_reconciler_task =
            tokio::spawn(assignment_reconciler.run(self.apply_shutdown.subscribe()));
        let mut assignment_slot = self
            .assignment_reconciler_task
            .lock()
            .map_err(|_| anyhow::anyhow!("assignment reconciler task lock poisoned"))?;
        if assignment_slot.is_some() {
            assignment_reconciler_task.abort();
            bail!("background assignment reconciler is already started");
        }
        *assignment_slot = Some(assignment_reconciler_task);
        drop(assignment_slot);
        let executor = Arc::new(
            crate::object_materialisation_runner::MvccObjectMaterialisationExecutor::new(
                self.clone(),
            ),
        );
        let worker_id = format!("object-materialisation/{}", self.peers[0].node_id);
        let runner = crate::object_materialisation_runner::ObjectMaterialisationRunner::new(
            self.clone(),
            executor,
            worker_id,
        )?;
        let task = tokio::spawn(runner.run(self.apply_shutdown.subscribe()));
        let mut slot = self
            .object_materialisation_task
            .lock()
            .map_err(|_| anyhow::anyhow!("object materialisation task lock poisoned"))?;
        if slot.is_some() {
            task.abort();
            bail!("object materialisation runner is already started");
        }
        *slot = Some(task);
        drop(slot);
        let repair = crate::mvcc_shard_repair::ShardRepairRunner::new(
            self.clone(),
            format!("shard-repair/{}", self.peers[0].node_id),
        )?;
        let repair_task = tokio::spawn(repair.run(self.apply_shutdown.subscribe()));
        let mut repair_slot = self
            .shard_repair_task
            .lock()
            .map_err(|_| anyhow::anyhow!("shard repair task lock poisoned"))?;
        if repair_slot.is_some() {
            repair_task.abort();
            bail!("shard repair runner is already started");
        }
        *repair_slot = Some(repair_task);
        let reconciler = crate::mvcc_shard_repair::ShardRebalanceReconciler::new(
            self.clone(),
            format!("shard-rebalance/{}", self.peers[0].node_id),
        )?;
        let rebalance_task = tokio::spawn(reconciler.run(self.apply_shutdown.subscribe()));
        let mut rebalance_slot = self
            .shard_rebalance_task
            .lock()
            .map_err(|_| anyhow::anyhow!("shard rebalance task lock poisoned"))?;
        if rebalance_slot.is_some() {
            rebalance_task.abort();
            bail!("shard rebalance reconciler is already started");
        }
        *rebalance_slot = Some(rebalance_task);
        let outbox = crate::mvcc_outbox::MvccOutboxRunner::new(
            self.runtime.local_store().clone(),
            self.consensus.clone(),
            consensus_control_node_id(&self.local_node.node_id),
            self.local_node.clone(),
            core_store,
            observability,
        )?;
        let outbox_task = tokio::spawn(outbox.run(self.apply_shutdown.subscribe()));
        let mut outbox_slot = self
            .outbox_task
            .lock()
            .map_err(|_| anyhow::anyhow!("outbox task lock poisoned"))?;
        if outbox_slot.is_some() {
            outbox_task.abort();
            bail!("outbox runner is already started");
        }
        *outbox_slot = Some(outbox_task);
        Ok(())
    }

    pub async fn shutdown(&self) {
        let _ = self.apply_shutdown.send(true);
        let task = self.apply_task.lock().ok().and_then(|mut task| task.take());
        if let Some(task) = task {
            let _ = task.await;
        }
        let object_task = self
            .object_materialisation_task
            .lock()
            .ok()
            .and_then(|mut task| task.take());
        if let Some(task) = object_task {
            let _ = task.await;
        }
        let repair_task = self
            .shard_repair_task
            .lock()
            .ok()
            .and_then(|mut task| task.take());
        if let Some(task) = repair_task {
            let _ = task.await;
        }
        let rebalance_task = self
            .shard_rebalance_task
            .lock()
            .ok()
            .and_then(|mut task| task.take());
        if let Some(task) = rebalance_task {
            let _ = task.await;
        }
        let outbox_task = self
            .outbox_task
            .lock()
            .ok()
            .and_then(|mut task| task.take());
        if let Some(task) = outbox_task {
            let _ = task.await;
        }
        let assignment_task = self
            .assignment_reconciler_task
            .lock()
            .ok()
            .and_then(|mut task| task.take());
        if let Some(task) = assignment_task {
            let _ = task.await;
        }
        let _ = self.consensus.shutdown().await;
    }
}

struct MvccPaths {
    base: PathBuf,
    prepared_bundles: PathBuf,
    replication_inbox: PathBuf,
    local_objects: PathBuf,
}

impl MvccPaths {
    fn new(storage_path: impl AsRef<Path>, cluster_id: &str) -> Self {
        let base = storage_path.as_ref().join("mvcc").join(cluster_id);
        Self {
            prepared_bundles: base.join("prepared-bundles"),
            replication_inbox: base.join("replication-inbox"),
            local_objects: base.join("local-objects"),
            base,
        }
    }
}

fn parse_and_validate_peers(config: &Config) -> Result<Vec<MvccPeerConfig>> {
    let mut peers: Vec<MvccPeerConfig> =
        serde_json::from_str(&config.mvcc_peers_json).context("parse MVCC peer configuration")?;
    if peers.is_empty() {
        peers.push(MvccPeerConfig {
            cluster_id: config.mvcc_cluster_id.clone(),
            raft_node_id: config.mvcc_raft_node_id,
            node_id: config.node_id.clone(),
            incarnation: config.mvcc_node_incarnation,
            endpoint: normalize_endpoint(&config.public_api_addr),
            failure_domain: config.mvcc_failure_domain.clone(),
            voter: true,
        });
    }
    let mut raft_ids = BTreeSet::new();
    let mut incarnations = BTreeSet::new();
    for peer in &mut peers {
        peer.endpoint = normalize_endpoint(&peer.endpoint);
        if peer.cluster_id != config.mvcc_cluster_id
            || peer.raft_node_id == 0
            || peer.node_id.trim().is_empty()
            || peer.incarnation == 0
            || peer.endpoint.trim().is_empty()
            || peer.failure_domain.trim().is_empty()
            || !raft_ids.insert(peer.raft_node_id)
            || !incarnations.insert((peer.node_id.clone(), peer.incarnation))
        {
            bail!("MVCC peers require unique, non-empty node identities and endpoints");
        }
    }
    if config.mvcc_bundle_quorum_holders == 0
        || config.mvcc_bundle_quorum_holders > peers.len()
        || config.mvcc_rpc_timeout_ms == 0
    {
        bail!("invalid MVCC durability or timeout configuration");
    }
    Ok(peers)
}

fn validate_cluster_id(cluster_id: &str) -> Result<()> {
    if cluster_id.is_empty()
        || !cluster_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("MVCC cluster ID must contain only ASCII letters, digits, '-', '_' or '.'");
    }
    Ok(())
}

fn validate_secure_peer_transport(config: &Config, peers: &[MvccPeerConfig]) -> Result<()> {
    if config.allow_test_only_insecure_mvcc_transport {
        return Ok(());
    }
    if let Some(peer) = peers
        .iter()
        .find(|peer| !peer.endpoint.starts_with("https://"))
    {
        bail!(
            "MVCC node transport requires TLS; peer {} endpoint must use https://",
            peer.node_id
        );
    }
    Ok(())
}

pub(crate) fn cluster_id_hash(cluster_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    let domain = b"anvil.mvcc.cluster-id.v1";
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain);
    hasher.update((cluster_id.len() as u64).to_be_bytes());
    hasher.update(cluster_id.as_bytes());
    hasher.finalize().into()
}

pub(crate) fn consensus_control_node_id(node_id: &str) -> NodeId {
    let mut hasher = Sha256::new();
    let domain = b"anvil.node-id.v1";
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain);
    hasher.update((node_id.len() as u64).to_be_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    NodeId(u64::from_be_bytes(bytes))
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_peer_transport_requires_https() {
        let config = Config::default();
        let peer = MvccPeerConfig {
            cluster_id: "default".into(),
            raft_node_id: 1,
            node_id: "node-a".into(),
            incarnation: 1,
            endpoint: "http://node-a.example".into(),
            failure_domain: "zone-a".into(),
            voter: true,
        };
        assert!(validate_secure_peer_transport(&config, &[peer]).is_err());
    }

    fn config(directory: &Path) -> Config {
        Config {
            node_id: "node-a".into(),
            public_api_addr: "127.0.0.1:50051".into(),
            storage_path: directory.to_string_lossy().into_owned(),
            allow_test_only_insecure_mvcc_transport: true,
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            ..Config::default()
        }
    }

    #[tokio::test]
    async fn bootstraps_all_mandatory_local_stores_and_single_node_membership() {
        let directory = tempfile::tempdir().unwrap();
        let db_path = directory.path().join("coremeta");
        let coremeta = crate::core_store::CoreMetaStore::open(&db_path).unwrap();
        let subsystem = MvccSubsystem::bootstrap(&config(directory.path()), coremeta.database())
            .await
            .unwrap();

        assert_eq!(subsystem.peers.len(), 1);
        assert_eq!(subsystem.peers[0].endpoint, "http://127.0.0.1:50051");
        assert_eq!(subsystem.runtime.applied_version().unwrap(), 0);
        for child in ["prepared-bundles", "replication-inbox"] {
            assert!(
                directory
                    .path()
                    .join("mvcc")
                    .join("default")
                    .join(child)
                    .exists()
            );
        }
    }

    #[test]
    fn rejects_duplicate_peer_identities_before_opening_storage() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = config(directory.path());
        let duplicate = MvccPeerConfig {
            cluster_id: "default".into(),
            raft_node_id: 1,
            node_id: "node-a".into(),
            incarnation: 1,
            endpoint: "127.0.0.1:50051".into(),
            failure_domain: "zone-a".into(),
            voter: true,
        };
        config.mvcc_peers_json =
            serde_json::to_string(&vec![duplicate.clone(), duplicate]).unwrap();

        assert!(parse_and_validate_peers(&config).is_err());
        assert!(!directory.path().join("mvcc").exists());
    }
}
