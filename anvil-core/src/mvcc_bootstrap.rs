//! Mandatory construction of the MVCC-under-Raft node subsystem.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use anvil_mvcc_consensus::{
    CommitVersion, Consensus as _, ConsensusNode, NodeId, OpenRaftConsensus, RocksRaftStore,
};
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
    mvcc_apply_worker::{ApplyWorkerReadiness, ApplyWorkerState, MvccApplyWorker},
    mvcc_gc_coordinator::MvccGarbageCollectionCoordinator,
    mvcc_node_runtime::MvccNodeRuntime,
    mvcc_open_transactions::OpenTransactionRegistry,
    mvcc_store::LocalMvccStore,
    mvcc_transaction::{
        ClusterOwnershipClaim, ClusterOwnershipResolver, DurabilityPolicy, NodeIncarnation,
    },
    replication::AuthenticatedPeer,
    replication_client::{
        ReplicationPeer, ReplicationStreamOptions, TonicReplicationStreamManager,
    },
    services::{
        consensus_transport::{
            AppliedWatermarkReports, ConsensusConnectionAuthorizer, ConsensusTransportService,
            LocalGcSafetyReport, TonicConsensusRpcFactory,
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

fn raft_store_is_empty(store: &RocksRaftStore) -> Result<bool> {
    let has_retained_log = store
        .last_log_index()
        .context("inspect MVCC Raft log")?
        .is_some();
    let has_purged_log = store
        .last_purged_index()
        .context("inspect MVCC Raft purge boundary")?
        .is_some();
    Ok(!has_retained_log && !has_purged_log)
}

#[derive(Clone)]
struct RaftClusterOwnershipResolver {
    cluster_id: String,
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
    raft_nodes: Arc<RwLock<BTreeMap<u64, MvccPeerConfig>>>,
    replication_nodes: Arc<RwLock<BTreeMap<String, MvccPeerConfig>>>,
    storage: crate::storage::Storage,
    core_store: crate::core_store::CoreStore,
    runtime: Arc<ProductMvccRuntime>,
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
        runtime: Arc<ProductMvccRuntime>,
        mesh_id: impl Into<Arc<str>>,
        allow_test_bypass: bool,
        consensus: Arc<OpenRaftConsensus>,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            token: token.into(),
            raft_nodes: Arc::new(RwLock::new(
                peers
                    .iter()
                    .cloned()
                    .map(|peer| (peer.raft_node_id, peer))
                    .collect(),
            )),
            replication_nodes: Arc::new(RwLock::new(
                peers
                    .iter()
                    .cloned()
                    .map(|peer| (peer.node_id.clone(), peer))
                    .collect(),
            )),
            storage,
            core_store,
            runtime,
            mesh_id: mesh_id.into(),
            allow_test_bypass,
            consensus,
        }
    }

    fn replace_peer(
        &self,
        replaced_raft_node_id: u64,
        raft_node_id: u64,
        node: &NodeIncarnation,
        failure_domain: &str,
        endpoint: &str,
    ) -> Result<()> {
        if replaced_raft_node_id == 0
            || raft_node_id == 0
            || replaced_raft_node_id == raft_node_id
            || node.node_id.trim().is_empty()
            || node.incarnation == 0
            || failure_domain.trim().is_empty()
            || endpoint.trim().is_empty()
        {
            bail!("replacement authorization route is incomplete");
        }
        let current = self
            .replication_nodes
            .read()
            .map_err(|_| anyhow::anyhow!("replication authorization map lock poisoned"))?
            .get(&node.node_id)
            .cloned()
            .context("replacement node is not in peer configuration")?;
        if current.cluster_id != self.cluster_id.as_ref() {
            bail!("replacement authorization route belongs to another cluster");
        }
        if current.raft_node_id != replaced_raft_node_id && current.raft_node_id != raft_node_id {
            bail!("replacement names neither the configured old nor new Raft node ID");
        }
        let replacement = MvccPeerConfig {
            cluster_id: current.cluster_id,
            raft_node_id,
            node_id: node.node_id.clone(),
            incarnation: node.incarnation,
            endpoint: endpoint.to_string(),
            failure_domain: failure_domain.to_string(),
            voter: current.voter,
        };
        let mut raft_nodes = self
            .raft_nodes
            .write()
            .map_err(|_| anyhow::anyhow!("Raft authorization map lock poisoned"))?;
        if raft_nodes
            .get(&raft_node_id)
            .is_some_and(|configured| configured.node_id != node.node_id)
        {
            bail!("replacement Raft node ID is already bound to another logical node");
        }
        if raft_nodes
            .get(&replaced_raft_node_id)
            .is_some_and(|configured| configured.node_id == node.node_id)
        {
            raft_nodes.remove(&replaced_raft_node_id);
        }
        raft_nodes.insert(raft_node_id, replacement.clone());
        drop(raft_nodes);
        self.replication_nodes
            .write()
            .map_err(|_| anyhow::anyhow!("replication authorization map lock poisoned"))?
            .insert(node.node_id.clone(), replacement);
        Ok(())
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
        if !system_realm::bootstrap_marker_exists_in_runtime(self.runtime.as_ref(), &self.mesh_id)
            .map_err(|error| Status::unavailable(error.to_string()))?
        {
            // The first system-realm transaction needs replication and Raft
            // traffic before Zanzibar has any node tuples to evaluate. Static
            // peer membership plus the cluster token is the bootstrap
            // authority until the certified marker is visible locally.
            return Ok(());
        }
        if self
            .consensus
            .applied_control_snapshot()
            .map_err(|error| Status::unavailable(error.to_string()))?
            .durability_policy
            .generation
            == 0
        {
            // Zanzibar itself is installed by the first certified product
            // transaction. Before that point, possession of the cluster token
            // plus membership in the static peer configuration is the only
            // non-circular bootstrap authority.
            return Ok(());
        }
        let claims = auth::Claims {
            sub: node_id.to_string(),
            exp: usize::MAX,
            tenant_id: crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
            jti: None,
        };
        let allowed = system_realm::check_internal_node_access(
            &self.core_store,
            self.runtime.as_ref(),
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

    fn authorize_control_incarnation(
        &self,
        peer: &MvccPeerConfig,
        presented_incarnation: u64,
    ) -> Result<(), Status> {
        let snapshot = self
            .consensus
            .applied_control_snapshot()
            .map_err(|error| Status::unavailable(error.to_string()))?;
        // Initial membership has to exchange Raft traffic before Raft can
        // certify its own node-incarnation records. During that closed
        // bootstrap window the authenticated, statically configured peer set
        // is the authority. Installing the first durability policy closes the
        // window; every reconnect thereafter must match Raft control state.
        if snapshot.durability_policy.generation == 0 {
            return if presented_incarnation == peer.incarnation {
                Ok(())
            } else {
                Err(Status::permission_denied(
                    "node incarnation does not match bootstrap peer configuration",
                ))
            };
        }
        let installed =
            snapshot
                .nodes
                .iter()
                .any(|(node_id, raft_node_id, incarnation, domain)| {
                    *node_id == consensus_control_node_id(&peer.node_id)
                        && raft_node_id.0 == peer.raft_node_id
                        && *incarnation == presented_incarnation
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
            .read()
            .map_err(|_| Status::unavailable("Raft authorization map lock poisoned"))?
            .get(&open.node_id)
            .cloned()
            .ok_or_else(|| Status::permission_denied("node is not in Raft peer configuration"))?;
        self.authorize_control_incarnation(&peer, open.node_incarnation)?;
        self.authorize_zanzibar(&peer.node_id).await?;
        Ok(())
    }

    fn authorize_incarnation(&self, node_id: u64, incarnation: u64) -> Result<(), Status> {
        let peer = self
            .raft_nodes
            .read()
            .map_err(|_| Status::unavailable("Raft authorization map lock poisoned"))?
            .get(&node_id)
            .cloned()
            .ok_or_else(|| Status::permission_denied("node is not in Raft peer configuration"))?;
        self.authorize_control_incarnation(&peer, incarnation)
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
            .read()
            .map_err(|_| Status::unavailable("replication authorization map lock poisoned"))?
            .get(&open.node_id)
            .cloned()
            .ok_or_else(|| Status::permission_denied("node is not in peer configuration"))?;
        self.authorize_control_incarnation(&peer, open.node_incarnation)?;
        self.authorize_zanzibar(&peer.node_id).await?;
        AuthenticatedPeer::new_bound(
            open.node_id.clone(),
            open.node_incarnation,
            peer.endpoint.clone(),
        )
        .map_err(|error| Status::permission_denied(error.to_string()))
    }

    fn authorize_incarnation(&self, node_id: &str, incarnation: u64) -> Result<(), Status> {
        let peer = self
            .replication_nodes
            .read()
            .map_err(|_| Status::unavailable("replication authorization map lock poisoned"))?
            .get(node_id)
            .cloned()
            .ok_or_else(|| Status::permission_denied("node is not in peer configuration"))?;
        self.authorize_control_incarnation(&peer, incarnation)
    }
}

pub struct MvccSubsystem {
    pub consensus: Arc<OpenRaftConsensus>,
    pub runtime: Arc<ProductMvccRuntime>,
    pub open_transactions: Arc<OpenTransactionRegistry>,
    pub replication_client: TonicReplicationStreamManager,
    pub object_evidence: ObjectEvidenceRegistry,
    pub local_objects: LocalObjectStore,
    prepared_bundles: AppendOnlyPreparedBundleStore,
    bundle_replicator: StreamingBundleReplicator<TonicReplicationStreamManager>,
    pub materialisation_storage: crate::storage::Storage,
    pub materialisation_signing_key: Arc<[u8]>,
    pub materialisation_embedding_providers: crate::embedding_provider::EmbeddingProviderRegistry,
    pub consensus_service: ConsensusTransportService<NodeConnectionAuthorizer>,
    pub replication_service: ReplicationServiceImpl<NodeConnectionAuthorizer>,
    connection_authorizer: NodeConnectionAuthorizer,
    applied_reports: AppliedWatermarkReports,
    membership_change: tokio::sync::Mutex<()>,
    pub peers: Arc<[MvccPeerConfig]>,
    pub local_node: NodeIncarnation,
    pub apply_worker_state: Arc<tokio::sync::Mutex<ApplyWorkerState>>,
    apply_worker_readiness: ApplyWorkerReadiness,
    apply_shutdown: tokio::sync::watch::Sender<bool>,
    apply_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    object_materialisation_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    shard_repair_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    shard_rebalance_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    durability_upgrade_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    outbox_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    assignment_reconciler_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    gc_shutdown: tokio::sync::watch::Sender<bool>,
    gc_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
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
        let _ = self.gc_shutdown.send(true);
    }
}

impl MvccSubsystem {
    #[cfg(feature = "test-cluster-transport-faults")]
    pub async fn initialize_configured_test_membership(
        &self,
        cluster_id: &str,
        bundle_quorum_holders: usize,
        tolerated_failure_domains: usize,
    ) -> Result<()> {
        validate_cluster_id(cluster_id)?;
        if self.peers.iter().any(|peer| peer.cluster_id != cluster_id) {
            bail!("test MVCC membership contains a peer from another cluster");
        }
        let members = self
            .peers
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
        self.consensus
            .initialize(members)
            .await
            .context("initialize configured test MVCC membership")?;
        tokio::time::timeout(Duration::from_secs(15), async {
            while !self.consensus.is_leader() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("test MVCC bootstrap node did not become leader")?;
        let cluster_hash = cluster_id_hash(cluster_id);
        for peer in self.peers.iter() {
            self.consensus
                .install_node(
                    cluster_hash,
                    anvil_mvcc_consensus::NodeIncarnation {
                        node_id: consensus_control_node_id(&peer.node_id),
                        incarnation: peer.incarnation,
                    },
                    NodeId(peer.raft_node_id),
                    peer.failure_domain.clone(),
                )
                .await
                .context("install configured test MVCC node")?;
        }
        self.consensus
            .set_durability_policy(
                cluster_hash,
                1,
                u16::try_from(bundle_quorum_holders)?,
                u16::try_from(tolerated_failure_domains)?,
            )
            .await
            .context("install configured test MVCC durability policy")?;
        Ok(())
    }

    #[cfg(feature = "test-cluster-transport-faults")]
    pub fn prepared_bundle(
        &self,
        identity: &crate::mvcc_transaction::BundleIdentity,
    ) -> Result<Option<Vec<u8>>> {
        self.prepared_bundles.read(identity)
    }

    /// Durable operator-visible records for committed `local` writes whose
    /// sole holder incarnation has been removed from cluster control state.
    pub fn local_durability_violations(
        &self,
    ) -> Result<Vec<crate::mvcc_store::LocalDurabilityViolationRecord>> {
        self.runtime.local_store().local_durability_violations()
    }

    pub fn live_shard_placement(&self) -> Result<(Arc<[ShardTarget]>, usize, u64)> {
        let snapshot = self.consensus.applied_control_snapshot()?;
        if snapshot.durability_policy.generation == 0 {
            bail!("Raft durability policy is not installed");
        }
        let routed = self
            .replication_client
            .routed_node_incarnations(self.cluster_id())?;
        let mut candidates = Vec::new();
        for (control_node_id, _raft_node_id, incarnation, failure_domain) in snapshot.nodes {
            let node = routed
                .iter()
                .find(|node| {
                    consensus_control_node_id(&node.node_id) == control_node_id
                        && node.incarnation == incarnation
                })
                .context("Raft control state names a node without a transport route")?;
            candidates.push(ShardTarget {
                cluster_id: self.cluster_id().to_string(),
                node: node.clone(),
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

    pub fn apply_worker_is_ready(&self) -> bool {
        self.apply_worker_is_ready_at(self.consensus.observed_commit_version().0)
    }

    pub fn apply_worker_is_ready_at(&self, commit_version: u64) -> bool {
        self.apply_worker_readiness
            .is_ready_at(CommitVersion(commit_version))
    }

    pub fn apply_worker_applied_watermark(&self) -> u64 {
        self.apply_worker_readiness.applied_watermark()
    }

    pub fn observed_commit_version(&self) -> u64 {
        self.consensus.observed_commit_version().0
    }

    pub async fn confirm_cluster_commit_barrier(&self) -> Result<u64> {
        self.consensus
            .linearized_read_barrier()
            .await
            .map(|version| version.0)
            .context("confirm cluster consensus read barrier")
    }

    pub(crate) fn replace_runtime_peer_projection(
        &self,
        replaced_raft_node_id: u64,
        raft_node_id: u64,
        node: &NodeIncarnation,
        failure_domain: &str,
        endpoint: &str,
    ) -> Result<()> {
        self.replication_client.replace_peer_incarnation(
            self.cluster_id(),
            node,
            endpoint.to_string(),
        )?;
        self.bundle_replicator.replace_target_incarnation(
            self.cluster_id(),
            node,
            failure_domain,
        )?;
        self.connection_authorizer.replace_peer(
            replaced_raft_node_id,
            raft_node_id,
            node,
            failure_domain,
            endpoint,
        )
    }

    pub(crate) async fn membership_change_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.membership_change.lock().await
    }

    pub(crate) async fn wait_for_node_applied(
        &self,
        raft_node_id: NodeId,
        incarnation: u64,
        watermark: u64,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if !self.consensus.is_leader() {
                bail!("cluster leadership changed during replacement catch-up");
            }
            if self
                .applied_reports
                .node(raft_node_id)
                .is_some_and(|report| {
                    report.incarnation == incarnation && report.watermark >= watermark
                })
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "replacement Raft node {} incarnation {} did not apply through MVCC watermark {}",
                    raft_node_id.0,
                    incarnation,
                    watermark
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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
        #[cfg(any(test, debug_assertions))]
        let raft_store = raft_store.with_log_write_fault_hook(Arc::new(|| {
            crate::mvcc_fault_injection::hit(crate::mvcc_fault_injection::FaultPoint::RaftLogWrite)
                .map_err(|error| error.to_string())
        }));
        // A valid snapshot may cover and purge every retained log entry.
        // Absence of a current log therefore does not make an existing group
        // eligible for membership initialization.
        let raft_is_empty = raft_store_is_empty(&raft_store)?;
        #[cfg(test)]
        if !raft_is_empty {
            crate::mvcc_fault_injection::hit(
                crate::mvcc_fault_injection::FaultPoint::RestartRecovery,
            )?;
        }
        let token = config.mvcc_node_token()?;
        let applied_watermark_report = LocalGcSafetyReport::default();
        let raft_network = Arc::new(TonicConsensusRpcFactory::new(
            config.mvcc_cluster_id.clone(),
            NodeId(config.mvcc_raft_node_id),
            config.mvcc_node_incarnation,
            token.clone(),
            Duration::from_millis(config.mvcc_rpc_timeout_ms),
        ));
        let applied_reports = raft_network.applied_reports();
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
            tokio::time::timeout(Duration::from_secs(15), async {
                while !consensus.is_leader() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context(
                "local node did not become leader while installing initial Raft control state",
            )?;
            let cluster_hash = cluster_id_hash(&config.mvcc_cluster_id);
            for peer in &peers {
                consensus
                    .install_node(
                        cluster_hash,
                        anvil_mvcc_consensus::NodeIncarnation {
                            node_id: consensus_control_node_id(&peer.node_id),
                            incarnation: peer.incarnation,
                        },
                        NodeId(peer.raft_node_id),
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
                voter: peer.voter,
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
            replicator.clone(),
            consensus.as_ref().clone(),
            DurabilityPolicy {
                bundle_quorum_holders: config.mvcc_bundle_quorum_holders,
                tolerated_failure_domains: config.mvcc_tolerated_failure_domains,
            },
            local_store.clone(),
            Arc::new(RaftClusterOwnershipResolver {
                cluster_id: config.mvcc_cluster_id.clone(),
            }),
        )?);
        let initial_assignment_reconciler =
            crate::mvcc_assignment_reconciler::BackgroundAssignmentReconciler::new(
                config.mvcc_cluster_id.clone(),
                consensus.clone(),
                local_store.clone(),
            )?;
        if let Err(error) = initial_assignment_reconciler.run_once().await {
            // Startup is not an assignment transaction. Leadership can change
            // between the cheap leader check and the proposal, and a
            // restarting node may not have transport quorum until its service
            // is listening. The continuous reconciler below owns retry.
            tracing::debug!(
                error = %error,
                "initial background assignment reconciliation deferred"
            );
        }
        let open_transactions = Arc::new(OpenTransactionRegistry::from_db(core_meta_db)?);
        let authorization_core_store =
            crate::core_store::CoreStore::new(materialisation_storage.clone()).await?;
        let authorizer = NodeConnectionAuthorizer::new(
            config.mvcc_cluster_id.clone(),
            token,
            &peers,
            materialisation_storage.clone(),
            authorization_core_store,
            runtime.clone(),
            config.mesh_id.clone(),
            config.allow_test_only_insecure_mvcc_transport,
            consensus.clone(),
        );
        let consensus_service =
            ConsensusTransportService::new(consensus.clone(), authorizer.clone())
                .with_applied_watermark_report(
                    NodeId(config.mvcc_raft_node_id),
                    config.mvcc_node_incarnation,
                    applied_watermark_report.clone(),
                );
        let replication_service =
            ReplicationServiceImpl::open(authorizer.clone(), &paths.replication_inbox)?
                .with_prepared_bundles(prepared.clone())
                .with_mvcc_checkpoint_store(local_store.clone());
        let worker = MvccApplyWorker::new(
            consensus.clone(),
            config.mvcc_cluster_id.clone(),
            prepared.clone(),
            replication_client.clone(),
            local_store,
        )
        .with_shard_transfer_receiver(replication_service.receiver());
        let apply_worker_state = worker.state_handle();
        let apply_worker_readiness = worker.readiness_handle();
        let (apply_shutdown, apply_shutdown_rx) = tokio::sync::watch::channel(false);
        let apply_task = tokio::spawn(worker.run(apply_shutdown_rx));
        let (gc_shutdown, gc_shutdown_rx) = tokio::sync::watch::channel(false);
        let gc_task = if crate::mvcc_gc::MVCC_GARBAGE_COLLECTION_ENABLED {
            let gc_coordinator = MvccGarbageCollectionCoordinator::new(
                config.mvcc_cluster_id.clone(),
                NodeId(config.mvcc_raft_node_id),
                consensus.clone(),
                open_transactions.clone(),
                runtime.local_store().clone(),
                applied_reports.clone(),
                applied_watermark_report.clone(),
                Duration::from_secs(1),
            )?;
            Some(tokio::spawn(gc_coordinator.run(gc_shutdown_rx)))
        } else {
            tracing::info!(
                operation = "gc.disabled",
                "MVCC and physical garbage collection are disabled for Anvil v0.4.0"
            );
            None
        };

        Ok(Self {
            consensus,
            runtime,
            open_transactions,
            replication_client,
            object_evidence,
            local_objects,
            prepared_bundles: prepared,
            bundle_replicator: replicator,
            materialisation_storage,
            materialisation_signing_key,
            materialisation_embedding_providers,
            consensus_service,
            replication_service,
            connection_authorizer: authorizer,
            applied_reports,
            membership_change: tokio::sync::Mutex::new(()),
            peers: peers.into(),
            local_node: local_incarnation,
            apply_worker_state,
            apply_worker_readiness,
            apply_shutdown,
            apply_task: Mutex::new(Some(apply_task)),
            object_materialisation_task: Mutex::new(None),
            shard_repair_task: Mutex::new(None),
            shard_rebalance_task: Mutex::new(None),
            durability_upgrade_task: Mutex::new(None),
            outbox_task: Mutex::new(None),
            assignment_reconciler_task: Mutex::new(None),
            gc_shutdown,
            gc_task: Mutex::new(gc_task),
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
        let worker_id = background_worker_id("object-materialisation", &self.local_node);
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
            background_worker_id("shard-repair", &self.local_node),
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
            background_worker_id("shard-rebalance", &self.local_node),
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
        drop(rebalance_slot);
        let upgrade = crate::mvcc_local_durability_upgrade::LocalDurabilityUpgradeRunner::new(
            self.local_objects.clone(),
            self.replication_client.clone(),
            self.clone(),
            self.clone(),
            crate::mvcc_local_durability_upgrade::PreparedBundleUpgradePublisher {
                store: self.prepared_bundles.clone(),
                replicator: self.bundle_replicator.clone(),
            },
            self.consensus.as_ref().clone(),
        );
        let upgrade_task = tokio::spawn(upgrade.run(
            self.runtime.local_store().clone(),
            self.local_node.clone(),
            background_worker_id("local-durability-upgrade", &self.local_node),
            self.apply_shutdown.subscribe(),
        ));
        let mut upgrade_slot = self
            .durability_upgrade_task
            .lock()
            .map_err(|_| anyhow::anyhow!("durability upgrade task lock poisoned"))?;
        if upgrade_slot.is_some() {
            upgrade_task.abort();
            bail!("local durability upgrade runner is already started");
        }
        *upgrade_slot = Some(upgrade_task);
        drop(upgrade_slot);
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
        let _ = self.gc_shutdown.send(true);
        let task = self.apply_task.lock().ok().and_then(|mut task| task.take());
        if let Some(task) = task {
            let _ = task.await;
        }
        let gc_task = self.gc_task.lock().ok().and_then(|mut task| task.take());
        if let Some(task) = gc_task {
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
        let upgrade_task = self
            .durability_upgrade_task
            .lock()
            .ok()
            .and_then(|mut task| task.take());
        if let Some(task) = upgrade_task {
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

fn background_worker_id(kind: &str, node: &NodeIncarnation) -> String {
    format!("{kind}/{}/{}", node.node_id, node.incarnation)
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
    fn purged_raft_history_is_not_a_new_consensus_group() {
        let directory = tempfile::tempdir().unwrap();
        let store = RocksRaftStore::open(directory.path(), 7).unwrap();
        assert!(raft_store_is_empty(&store).unwrap());

        store.append_logs(&[(0, vec![0])]).unwrap();
        store.purge_logs(0).unwrap();

        assert_eq!(store.last_log_index().unwrap(), None);
        assert_eq!(store.last_purged_index().unwrap(), Some(0));
        assert!(!raft_store_is_empty(&store).unwrap());
    }

    #[test]
    fn background_worker_identity_names_the_local_incarnation() {
        assert_eq!(
            background_worker_id(
                "shard-repair",
                &NodeIncarnation {
                    node_id: "node-b".to_string(),
                    incarnation: 7,
                },
            ),
            "shard-repair/node-b/7"
        );
    }

    #[test]
    fn production_peer_transport_requires_https() {
        let config = Config {
            allow_test_only_insecure_mvcc_transport: false,
            ..Config::default()
        };
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

    #[tokio::test]
    async fn assignment_epoch_replacement_aborts_all_staged_personaldb_rows() {
        let directory = tempfile::tempdir().unwrap();
        let db_path = directory.path().join("coremeta");
        let coremeta = crate::core_store::CoreMetaStore::open(&db_path).unwrap();
        let subsystem = MvccSubsystem::bootstrap(&config(directory.path()), coremeta.database())
            .await
            .unwrap();
        let identity = "tenant/1/personaldb/assignment-race";
        let guard = subsystem
            .reconcile_work_assignment("personaldb-write", identity)
            .await
            .unwrap()
            .unwrap();
        let now = 10;
        let handle = subsystem
            .open_transactions
            .begin(
                subsystem.runtime.as_ref(),
                subsystem.cluster_id(),
                "alice",
                "assignment-race",
                std::time::Duration::from_secs(30),
                crate::mvcc_transaction::DurabilityLevel::Quorum,
                crate::mvcc_transaction::ReadConsistency::Linearized,
                now,
            )
            .await
            .unwrap();
        let head_key = crate::mvcc_transaction::LogicalKey {
            table_id: crate::core_store::TABLE_PERSONALDB_GROUP_ROW,
            application_key: b"head".to_vec(),
        };
        let watch_key = crate::mvcc_transaction::LogicalKey {
            table_id: 0x0607,
            application_key: b"watch".to_vec(),
        };
        subsystem
            .stage_product_mutations(
                &handle.transaction_id,
                "alice",
                vec![
                    crate::mvcc_product::ProductMutation::put(
                        head_key.clone(),
                        b"head-value".to_vec(),
                    ),
                    crate::mvcc_product::ProductMutation::put(
                        watch_key.clone(),
                        b"watch-value".to_vec(),
                    ),
                ],
                now,
            )
            .unwrap();
        for key in [head_key.clone(), watch_key.clone()] {
            subsystem
                .stage_predicate(
                    &handle.transaction_id,
                    "alice",
                    key,
                    crate::mvcc_transaction::PredicateKind::Absent,
                    now,
                )
                .unwrap();
        }
        subsystem
            .stage_assignment_guard(&handle.transaction_id, "alice", &guard, now)
            .unwrap();
        subsystem
            .consensus
            .assign_partition(
                cluster_id_hash(subsystem.cluster_id()),
                guard.partition_id,
                anvil_mvcc_consensus::NodeIncarnation {
                    node_id: consensus_control_node_id(&subsystem.local_node.node_id),
                    incarnation: subsystem.local_node.incarnation,
                },
                guard.assignment_epoch + 1,
            )
            .await
            .unwrap();

        let outcome = subsystem
            .open_transactions
            .commit(
                subsystem.runtime.as_ref(),
                &handle.transaction_id,
                "alice",
                now,
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome.certification,
            crate::mvcc_transaction::CertificationResult::Aborted { .. }
        ));
        assert!(
            subsystem
                .runtime
                .local_store()
                .read_latest(&head_key)
                .unwrap()
                .is_none()
        );
        assert!(
            subsystem
                .runtime
                .local_store()
                .read_latest(&watch_key)
                .unwrap()
                .is_none()
        );
        subsystem.shutdown().await;
    }

    #[tokio::test]
    async fn restart_recovery_fault_fails_before_runtime_reopens_and_is_retryable() {
        let directory = tempfile::tempdir().unwrap();
        let db_path = directory.path().join("coremeta");
        let coremeta = crate::core_store::CoreMetaStore::open(&db_path).unwrap();
        let config = config(directory.path());
        let subsystem = MvccSubsystem::bootstrap(&config, coremeta.database())
            .await
            .unwrap();
        subsystem.shutdown().await;
        drop(subsystem);

        crate::mvcc_fault_injection::install(
            crate::mvcc_fault_injection::DeterministicFaults::default()
                .fail_at(crate::mvcc_fault_injection::FaultPoint::RestartRecovery, 1),
        );
        let error = MvccSubsystem::bootstrap(&config, coremeta.database())
            .await
            .unwrap_err();
        crate::mvcc_fault_injection::clear();
        assert!(error.to_string().contains("RestartRecovery"));

        let recovered = MvccSubsystem::bootstrap(&config, coremeta.database())
            .await
            .unwrap();
        recovered.shutdown().await;
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
