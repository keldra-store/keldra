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
    bundle_replication::{
        AppendOnlyPreparedBundleStore, BundleTarget, ObjectEvidenceRegistry,
        StreamingBundleReplicator,
    },
    local_object_store::LocalObjectStore,
    mvcc_apply_worker::{ApplyWorkerState, MvccApplyWorker},
    mvcc_node_runtime::MvccNodeRuntime,
    mvcc_open_transactions::OpenTransactionRegistry,
    mvcc_store::LocalMvccStore,
    mvcc_transaction::{DurabilityPolicy, NodeIncarnation},
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
};

pub type ProductMvccRuntime = MvccNodeRuntime<
    AppendOnlyPreparedBundleStore,
    StreamingBundleReplicator<TonicReplicationStreamManager>,
    OpenRaftConsensus,
>;

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
}

impl NodeConnectionAuthorizer {
    fn new(
        cluster_id: impl Into<Arc<str>>,
        token: impl Into<Arc<str>>,
        peers: &[MvccPeerConfig],
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
        self.replication_nodes
            .get(&(open.node_id.clone(), open.node_incarnation))
            .ok_or_else(|| {
                Status::permission_denied("node incarnation is not in peer configuration")
            })?;
        AuthenticatedPeer::new(open.node_id.clone(), open.node_incarnation)
            .map_err(|error| Status::permission_denied(error.to_string()))
    }
}

pub struct MvccSubsystem {
    pub consensus: Arc<OpenRaftConsensus>,
    pub runtime: Arc<ProductMvccRuntime>,
    pub open_transactions: Arc<OpenTransactionRegistry>,
    pub replication_client: TonicReplicationStreamManager,
    pub object_evidence: ObjectEvidenceRegistry,
    pub shard_candidates: Arc<[ShardTarget]>,
    pub local_objects: LocalObjectStore,
    pub consensus_service: ConsensusTransportService<NodeConnectionAuthorizer>,
    pub replication_service: ReplicationServiceImpl<NodeConnectionAuthorizer>,
    pub peers: Arc<[MvccPeerConfig]>,
    pub apply_worker_state: Arc<tokio::sync::Mutex<ApplyWorkerState>>,
    apply_shutdown: tokio::sync::watch::Sender<bool>,
    apply_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
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
    pub fn cluster_id(&self) -> &str {
        self.peers
            .first()
            .map(|peer| peer.cluster_id.as_str())
            .expect("validated MVCC topology is non-empty")
    }

    pub async fn bootstrap(config: &Config, core_meta_db: Arc<DB>) -> Result<Self> {
        let peers = parse_and_validate_peers(config)?;
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
            local_incarnation,
            local.failure_domain.clone(),
        )?;
        let object_evidence = ObjectEvidenceRegistry::default();
        let replicator = StreamingBundleReplicator::new(
            replication_client.clone(),
            targets,
            object_evidence.clone(),
        )?;
        let shard_candidates = peers
            .iter()
            .map(|peer| ShardTarget {
                cluster_id: config.mvcc_cluster_id.clone(),
                node: NodeIncarnation {
                    node_id: peer.node_id.clone(),
                    incarnation: peer.incarnation,
                },
                failure_domain: peer.failure_domain.clone(),
            })
            .collect::<Vec<_>>();
        let local_store = LocalMvccStore::from_db(core_meta_db.clone(), &config.mvcc_cluster_id)?;
        let runtime = Arc::new(MvccNodeRuntime::new(
            prepared.clone(),
            replicator,
            consensus.as_ref().clone(),
            DurabilityPolicy {
                bundle_quorum_holders: config.mvcc_bundle_quorum_holders,
                tolerated_failure_domains: config.mvcc_tolerated_failure_domains,
            },
            local_store.clone(),
        )?);
        let open_transactions = Arc::new(OpenTransactionRegistry::from_db(core_meta_db)?);
        let authorizer =
            NodeConnectionAuthorizer::new(config.mvcc_cluster_id.clone(), token, &peers);
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
        );
        let apply_worker_state = worker.state_handle();
        let (apply_shutdown, apply_shutdown_rx) = tokio::sync::watch::channel(false);
        let apply_task = tokio::spawn(worker.run(apply_shutdown_rx));

        Ok(Self {
            consensus,
            runtime,
            open_transactions,
            replication_client,
            object_evidence,
            shard_candidates: shard_candidates.into(),
            local_objects,
            consensus_service,
            replication_service,
            peers: peers.into(),
            apply_worker_state,
            apply_shutdown,
            apply_task: Mutex::new(Some(apply_task)),
        })
    }

    pub async fn shutdown(&self) {
        let _ = self.apply_shutdown.send(true);
        let task = self.apply_task.lock().ok().and_then(|mut task| task.take());
        if let Some(task) = task {
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

fn cluster_id_hash(cluster_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    let domain = b"anvil.mvcc.cluster-id.v1";
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain);
    hasher.update((cluster_id.len() as u64).to_be_bytes());
    hasher.update(cluster_id.as_bytes());
    hasher.finalize().into()
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

    fn config(directory: &Path) -> Config {
        Config {
            node_id: "node-a".into(),
            public_api_addr: "127.0.0.1:50051".into(),
            storage_path: directory.to_string_lossy().into_owned(),
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
