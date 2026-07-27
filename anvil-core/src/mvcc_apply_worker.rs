//! Ordered application of committed, metadata-only Raft decisions.

use std::{sync::Arc, time::Duration};

use anvil_mvcc_consensus::{
    AppliedDecision, BundleHash, CommitVersion, ConsensusError, LocalDurabilityViolation,
    OpenRaftConsensus,
};
use anyhow::{Context, Result, anyhow, bail};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, watch};

use crate::{
    bundle_replication::AppendOnlyPreparedBundleStore,
    mvcc_store::{LocalDurabilityViolationRecord, LocalMvccStore},
    mvcc_transaction::NodeIncarnation,
    mvcc_transaction::{BundleIdentity, PreparedBundleStore, TransactionBundle},
    replication_client::{TonicReplicationStreamManager, bundle_transfer_id},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyWorkerState {
    Running,
    Stopped,
    Unrecoverable(String),
}

pub struct MvccApplyWorker {
    consensus: Arc<dyn DecisionSource>,
    cluster_id: String,
    prepared: AppendOnlyPreparedBundleStore,
    replication: TonicReplicationStreamManager,
    peers: Arc<[NodeIncarnation]>,
    local: LocalMvccStore,
    cluster_id_hash: [u8; 32],
    state: Arc<Mutex<ApplyWorkerState>>,
    prepared_bundle_gc_grace_ms: Option<u64>,
    shard_transfers: Option<Arc<std::sync::Mutex<crate::replication::TransferReceiver>>>,
}

pub trait DecisionSource: Send + Sync {
    fn applied_decisions_after(
        &self,
        position: CommitVersion,
    ) -> std::result::Result<Vec<AppliedDecision>, ConsensusError>;
    fn gc_safety_watermark(&self) -> std::result::Result<CommitVersion, ConsensusError>;
    fn observed_commit_version(&self) -> CommitVersion;
    fn local_durability_violations(
        &self,
    ) -> std::result::Result<Vec<LocalDurabilityViolation>, ConsensusError> {
        Ok(Vec::new())
    }
}

impl DecisionSource for OpenRaftConsensus {
    fn applied_decisions_after(
        &self,
        position: CommitVersion,
    ) -> std::result::Result<Vec<AppliedDecision>, ConsensusError> {
        self.applied_decisions_after(position)
    }

    fn gc_safety_watermark(&self) -> std::result::Result<CommitVersion, ConsensusError> {
        self.gc_safety_watermark()
    }

    fn observed_commit_version(&self) -> CommitVersion {
        anvil_mvcc_consensus::Consensus::observed_commit_version(self)
    }

    fn local_durability_violations(
        &self,
    ) -> std::result::Result<Vec<LocalDurabilityViolation>, ConsensusError> {
        self.local_durability_violations()
    }
}

impl MvccApplyWorker {
    pub fn new(
        consensus: Arc<dyn DecisionSource>,
        cluster_id: impl Into<String>,
        prepared: AppendOnlyPreparedBundleStore,
        replication: TonicReplicationStreamManager,
        peers: impl Into<Arc<[NodeIncarnation]>>,
        local: LocalMvccStore,
    ) -> Self {
        let cluster_id = cluster_id.into();
        Self {
            consensus,
            cluster_id_hash: cluster_id_hash(&cluster_id),
            cluster_id,
            prepared,
            replication,
            peers: peers.into(),
            local,
            state: Arc::new(Mutex::new(ApplyWorkerState::Stopped)),
            prepared_bundle_gc_grace_ms: None,
            shard_transfers: None,
        }
    }

    pub fn with_prepared_bundle_gc_grace(mut self, grace_ms: u64) -> Result<Self> {
        if grace_ms == 0 {
            bail!("prepared bundle GC grace must be non-zero");
        }
        self.prepared_bundle_gc_grace_ms = Some(grace_ms);
        Ok(self)
    }

    pub fn with_shard_transfer_retirement(
        mut self,
        receiver: Arc<std::sync::Mutex<crate::replication::TransferReceiver>>,
    ) -> Self {
        self.shard_transfers = Some(receiver);
        self
    }

    pub fn state_handle(&self) -> Arc<Mutex<ApplyWorkerState>> {
        self.state.clone()
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        *self.state.lock().await = ApplyWorkerState::Running;
        loop {
            if *shutdown.borrow() {
                *self.state.lock().await = ApplyWorkerState::Stopped;
                return;
            }
            match self.apply_available().await {
                Ok(_) => {}
                Err(error) if is_unrecoverable(&error) => {
                    *self.state.lock().await = ApplyWorkerState::Unrecoverable(error.to_string());
                    return;
                }
                Err(_) => {}
            }
            tokio::select! {
                _ = shutdown.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    }

    pub async fn apply_available(&self) -> Result<usize> {
        self.persist_local_durability_violations()?;
        let mut watermark = CommitVersion(self.local.decision_watermark()?);
        let observed_commit = self.consensus.observed_commit_version();
        crate::perf::record_mvcc_state(watermark.0, observed_commit.0, 0);
        let gc = self.consensus.gc_safety_watermark()?;
        if watermark < gc {
            bail!(
                "unrecoverable MVCC catch-up: applied decision watermark {} is below GC watermark {}",
                watermark.0,
                gc.0
            );
        }
        let decisions = self.consensus.applied_decisions_after(watermark)?;
        let mut applied = 0;
        for decision in decisions {
            let expected = watermark.0.saturating_add(1);
            if decision.position.0 != expected {
                bail!(
                    "unrecoverable MVCC decision gap: expected {}, found {}",
                    expected,
                    decision.position.0
                );
            }
            if let Some(committed) = decision.committed_bundle {
                if committed.cluster_id_hash != self.cluster_id_hash {
                    bail!("unrecoverable MVCC bundle belongs to another cluster");
                }
                let identity = bundle_identity(committed.bundle_hash, committed.bundle_length);
                let bytes = self
                    .fetch_bundle(&identity, &committed.durable_holders)
                    .await?;
                let mut bundle: TransactionBundle = serde_json::from_slice(&bytes)
                    .context("unrecoverable MVCC: decode canonical transaction bundle")?;
                bundle
                    .canonicalize()
                    .context("unrecoverable MVCC: canonicalize transaction bundle")?;
                if bundle.cluster_id != self.cluster_id
                    || bundle
                        .canonical_bytes()
                        .context("unrecoverable MVCC: encode canonical transaction bundle")?
                        != bytes
                    || bundle
                        .identity()
                        .context("unrecoverable MVCC: identify canonical transaction bundle")?
                        != identity
                {
                    bail!("unrecoverable MVCC bundle cluster or canonical identity mismatch");
                }
                self.local.apply_certified_bundle_and_advance(
                    decision.position.0,
                    &bundle,
                    decision.position.0,
                )?;
                tracing::debug!(
                    operation = "transaction.apply",
                    transaction_id = %bundle.transaction_id,
                    commit_version = decision.position.0,
                    source = "consensus_catch_up",
                    "applied committed bundle from ordered consensus decisions"
                );
                crate::perf::record_mvcc_state(
                    decision.position.0,
                    observed_commit.0,
                    bundle.writes.len() as u64,
                );
            } else {
                self.local.advance_decision_watermark(decision.position.0)?;
            }
            watermark = decision.position;
            crate::perf::record_mvcc_state(watermark.0, observed_commit.0, 0);
            applied += 1;
        }
        let advanced_gc = self.local.gc_watermark()? < gc.0;
        if advanced_gc {
            self.local
                .garbage_collect(gc.0)
                .context("apply consensus-approved MVCC GC watermark locally")?;
        }
        if advanced_gc && let Some(grace_ms) = self.prepared_bundle_gc_grace_ms {
            let reachable_bundles = self
                .consensus
                .applied_decisions_after(CommitVersion(gc.0.saturating_sub(1)))?
                .into_iter()
                .filter_map(|decision| decision.committed_bundle)
                .map(|bundle| bundle_identity(bundle.bundle_hash, bundle.bundle_length))
                .collect::<Vec<_>>();
            let unfinished_transaction_ids = self.local.unfinished_work_pins()?.transaction_ids;
            let retain = self.prepared.retain_plan(
                &reachable_bundles,
                &unfinished_transaction_ids,
                unix_time_ms()?,
                grace_ms,
            )?;
            self.prepared.compact_authorised(&retain)?;
        }
        // This is intentionally retried even when the watermark did not move:
        // a crash after MVCC GC but before physical unlink must converge on
        // restart, and the receiver operation is idempotent.
        if let Some(receiver) = &self.shard_transfers {
            let authorised = self.local.retirable_object_shard_transfers()?;
            let protected = self.local.protected_object_shard_transfers()?;
            let mut receiver = receiver
                .lock()
                .map_err(|_| anyhow!("replication receiver lock poisoned"))?;
            let removed = receiver.retire_complete_object_shards(&authorised)?;
            let orphaned = if let Some(grace_ms) = self.prepared_bundle_gc_grace_ms {
                receiver.retire_orphan_provisional_object_shards(
                    gc.0,
                    unix_time_ms()?,
                    grace_ms,
                    &protected,
                )?
            } else {
                0
            };
            if removed != 0 {
                tracing::info!(
                    operation = "gc.shard_transfer",
                    removed_transfers = removed,
                    gc_watermark = gc.0,
                    "retired unreferenced physical shard transfers"
                );
            }
            if orphaned != 0 {
                tracing::info!(
                    operation = "gc.provisional_shard",
                    removed_transfers = orphaned,
                    gc_watermark = gc.0,
                    "retired orphan provisional shard transfers"
                );
            }
        }
        Ok(applied)
    }

    fn persist_local_durability_violations(&self) -> Result<()> {
        for violation in self.consensus.local_durability_violations()? {
            let inserted =
                self.local
                    .record_local_durability_violation(&LocalDurabilityViolationRecord {
                        commit_version: violation.commit_version.0,
                        bundle_hash: violation.bundle_hash.0,
                        lost_holder_node_id: violation.lost_holder.node_id.0,
                        lost_holder_incarnation: violation.lost_holder.incarnation,
                        detected_at_log_index: violation.detected_at_log_index,
                    })?;
            if inserted {
                crate::perf::record_local_durability_violation("sole_holder_removed");
                tracing::error!(
                    commit_version = violation.commit_version.0,
                    lost_holder_node_id = violation.lost_holder.node_id.0,
                    lost_holder_incarnation = violation.lost_holder.incarnation,
                    detected_at_log_index = violation.detected_at_log_index,
                    "committed local-durability data has lost its sole durable holder"
                );
            }
        }
        crate::perf::record_local_durability_violation_status(
            self.local.local_durability_violations()?.len() as u64,
        );
        Ok(())
    }

    async fn fetch_bundle(
        &self,
        identity: &BundleIdentity,
        durable_holders: &[anvil_mvcc_consensus::NodeIncarnation],
    ) -> Result<Vec<u8>> {
        if let Some(bytes) = self.prepared.read(identity)? {
            return Ok(bytes);
        }
        let transfer_id = bundle_transfer_id(identity)?;
        let expected_hash = parse_hash(&identity.hash)?;
        let mut failures = Vec::new();
        let peers = durable_peer_candidates(&self.peers, durable_holders);
        if peers.is_empty() {
            bail!(
                "canonical bundle {} is not locally available and no committed durable holder is reachable",
                identity.hash
            );
        }
        for peer in peers {
            match self
                .replication
                .read_complete_transfer(
                    &self.cluster_id,
                    peer,
                    transfer_id,
                    identity.length,
                    expected_hash,
                )
                .await
            {
                Ok(bytes) => {
                    match self.prepared.persist(identity, &bytes).await {
                        Ok(()) => return Ok(bytes),
                        Err(error) => {
                            failures.push(format!(
                                "{}:{} returned bytes outside the committed identity: {error}",
                                peer.node_id, peer.incarnation
                            ));
                        }
                    }
                }
                Err(error) => failures.push(error.to_string()),
            }
        }
        Err(anyhow!(
            "canonical bundle {} is not locally available; peer failures: {}",
            identity.hash,
            failures.join("; ")
        ))
    }
}

fn durable_peer_candidates<'a>(
    peers: &'a [NodeIncarnation],
    durable_holders: &[anvil_mvcc_consensus::NodeIncarnation],
) -> Vec<&'a NodeIncarnation> {
    peers
        .iter()
        .filter(|peer| {
            durable_holders.iter().any(|holder| {
                holder.node_id
                    == crate::mvcc_bootstrap::consensus_control_node_id(&peer.node_id)
                    && holder.incarnation == peer.incarnation
            })
        })
        .collect()
}

fn unix_time_ms() -> Result<u64> {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )
    .context("system time exceeds u64 milliseconds")
}

fn bundle_identity(hash: BundleHash, length: u64) -> BundleIdentity {
    BundleIdentity {
        hash: format!("sha256:{}", hex::encode(hash.0)),
        length,
    }
}

fn parse_hash(value: &str) -> Result<[u8; 32]> {
    let value = value
        .strip_prefix("sha256:")
        .context("bundle hash must use sha256")?;
    hex::decode(value)?
        .try_into()
        .map_err(|_| anyhow!("bundle hash must contain 32 bytes"))
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

fn is_unrecoverable(error: &anyhow::Error) -> bool {
    error.to_string().contains("unrecoverable MVCC")
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use anvil_mvcc_consensus::{AppliedDecision, CommittedBundleDecision};
    use async_trait::async_trait;
    use tempfile::tempdir;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Status, metadata::MetadataMap, transport::Server};

    use super::*;
    use crate::{
        anvil_api::{ReplicationSessionOpen, replication_service_server::ReplicationServiceServer},
        bundle_replication::{BundleTarget, BundleTargetStream},
        mvcc_transaction::{HierarchicalRangeStampScheme, LogicalKey, TransactionBundleBuilder},
        replication::AuthenticatedPeer,
        replication_client::{ReplicationPeer, ReplicationStreamOptions},
        services::replication::{ReplicationConnectionAuthorizer, ReplicationServiceImpl},
    };

    struct TestAuthorizer;

    #[async_trait]
    impl ReplicationConnectionAuthorizer for TestAuthorizer {
        async fn authorize(
            &self,
            _metadata: &MetadataMap,
            open: &ReplicationSessionOpen,
        ) -> std::result::Result<AuthenticatedPeer, Status> {
            AuthenticatedPeer::new(open.node_id.clone(), open.node_incarnation)
                .map_err(|error| Status::permission_denied(error.to_string()))
        }
    }

    struct Source {
        decisions: StdMutex<Vec<AppliedDecision>>,
        gc: CommitVersion,
        violations: Vec<LocalDurabilityViolation>,
    }

    impl DecisionSource for Source {
        fn applied_decisions_after(
            &self,
            position: CommitVersion,
        ) -> std::result::Result<Vec<AppliedDecision>, ConsensusError> {
            Ok(self
                .decisions
                .lock()
                .unwrap()
                .iter()
                .filter(|decision| decision.position > position)
                .cloned()
                .collect())
        }

        fn gc_safety_watermark(&self) -> std::result::Result<CommitVersion, ConsensusError> {
            Ok(self.gc)
        }

        fn observed_commit_version(&self) -> CommitVersion {
            self.decisions
                .lock()
                .unwrap()
                .last()
                .map_or(CommitVersion(0), |decision| decision.position)
        }

        fn local_durability_violations(
            &self,
        ) -> std::result::Result<Vec<LocalDurabilityViolation>, ConsensusError> {
            Ok(self.violations.clone())
        }
    }

    fn bundle() -> TransactionBundle {
        let mut builder = TransactionBundleBuilder::new(
            "cluster",
            "tx",
            0,
            "principal",
            HierarchicalRangeStampScheme::new(),
        );
        builder.put(
            LogicalKey {
                table_id: 1,
                application_key: b"key".to_vec(),
            },
            b"value".to_vec(),
        );
        builder.build().unwrap()
    }

    fn worker(
        source: Arc<Source>,
        prepared: AppendOnlyPreparedBundleStore,
        local: LocalMvccStore,
    ) -> MvccApplyWorker {
        MvccApplyWorker::new(
            source,
            "cluster",
            prepared,
            TonicReplicationStreamManager::new(
                "cluster",
                NodeIncarnation {
                    node_id: "node-a".into(),
                    incarnation: 1,
                },
                "token",
                [],
                ReplicationStreamOptions::default(),
            )
            .unwrap(),
            Vec::<NodeIncarnation>::new(),
            local,
        )
    }

    fn committed(bundle: &TransactionBundle, position: u64) -> AppliedDecision {
        let identity = bundle.identity().unwrap();
        AppliedDecision {
            position: CommitVersion(position),
            committed_bundle: Some(CommittedBundleDecision {
                cluster_id_hash: cluster_id_hash(&bundle.cluster_id),
                bundle_hash: BundleHash(parse_hash(&identity.hash).unwrap()),
                bundle_length: identity.length,
                durability: anvil_mvcc_consensus::DurabilityLevel::Quorum,
                durable_holders: Vec::new(),
            }),
        }
    }

    #[tokio::test]
    async fn follower_lag_retries_then_restart_uses_persisted_watermark() {
        let prepared_directory = tempdir().unwrap();
        let local_directory = tempdir().unwrap();
        let prepared = AppendOnlyPreparedBundleStore::open(
            prepared_directory.path(),
            "cluster",
            NodeIncarnation {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            "zone-a",
        )
        .unwrap();
        let local = LocalMvccStore::open(local_directory.path()).unwrap();
        let bundle = bundle();
        let source = Arc::new(Source {
            decisions: StdMutex::new(vec![committed(&bundle, 1)]),
            gc: CommitVersion(0),
            violations: vec![],
        });
        let running = worker(source.clone(), prepared.clone(), local.clone());

        assert!(running.apply_available().await.is_err());
        let bytes = bundle.canonical_bytes().unwrap();
        prepared
            .persist(&bundle.identity().unwrap(), &bytes)
            .await
            .unwrap();
        assert_eq!(running.apply_available().await.unwrap(), 1);
        assert_eq!(local.decision_watermark().unwrap(), 1);
        assert_eq!(
            worker(source, prepared, local.clone())
                .apply_available()
                .await
                .unwrap(),
            0
        );
        assert_eq!(local.applied_version().unwrap(), 1);
    }

    #[tokio::test]
    async fn follower_fetches_bundle_over_persistent_stream_and_applies_it() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let inbox = tempdir().unwrap();
        let service = ReplicationServiceImpl::open(TestAuthorizer, inbox.path()).unwrap();
        let server = tokio::spawn(
            Server::builder()
                .add_service(ReplicationServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let remote = NodeIncarnation {
            node_id: "node-b".into(),
            incarnation: 1,
        };
        let peer = ReplicationPeer {
            cluster_id: "cluster".into(),
            node: remote.clone(),
            endpoint: format!("http://{address}"),
        };
        let options = ReplicationStreamOptions::default();
        let uploader = TonicReplicationStreamManager::new(
            "cluster",
            NodeIncarnation {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            "token",
            [peer.clone()],
            options.clone(),
        )
        .unwrap();
        let bundle = bundle();
        let bytes = bundle.canonical_bytes().unwrap();
        let identity = bundle.identity().unwrap();
        uploader
            .send_bundle(
                &BundleTarget {
                    cluster_id: "cluster".into(),
                    node: remote.clone(),
                    failure_domain: "zone-b".into(),
                    voter: true,
                },
                &identity,
                &bytes,
            )
            .await
            .unwrap();

        let prepared_directory = tempdir().unwrap();
        let local_directory = tempdir().unwrap();
        let prepared = AppendOnlyPreparedBundleStore::open(
            prepared_directory.path(),
            "cluster",
            NodeIncarnation {
                node_id: "node-c".into(),
                incarnation: 1,
            },
            "zone-c",
        )
        .unwrap();
        let local = LocalMvccStore::open(local_directory.path()).unwrap();
        let mut decision = committed(&bundle, 1);
        decision
            .committed_bundle
            .as_mut()
            .unwrap()
            .durable_holders = vec![anvil_mvcc_consensus::NodeIncarnation {
            node_id: crate::mvcc_bootstrap::consensus_control_node_id(&remote.node_id),
            incarnation: remote.incarnation,
        }];
        let source = Arc::new(Source {
            decisions: StdMutex::new(vec![decision]),
            gc: CommitVersion(0),
            violations: vec![],
        });
        let downloader = TonicReplicationStreamManager::new(
            "cluster",
            NodeIncarnation {
                node_id: "node-c".into(),
                incarnation: 1,
            },
            "token",
            [peer],
            options,
        )
        .unwrap();
        let worker = MvccApplyWorker::new(
            source,
            "cluster",
            prepared.clone(),
            downloader,
            [remote],
            local.clone(),
        );

        assert_eq!(worker.apply_available().await.unwrap(), 1);
        assert_eq!(local.applied_version().unwrap(), 1);
        assert_eq!(local.decision_watermark().unwrap(), 1);
        assert_eq!(
            local
                .read_at(
                    &LogicalKey {
                        table_id: 1,
                        application_key: b"key".to_vec(),
                    },
                    1,
                )
                .unwrap()
                .unwrap()
                .value,
            b"value"
        );
        assert_eq!(prepared.read(&identity).unwrap(), Some(bytes));
        server.abort();
    }

    #[test]
    fn recovery_contacts_only_exact_incarnation_committed_holders() {
        let holder = NodeIncarnation {
            node_id: "holder".into(),
            incarnation: 3,
        };
        let stale_holder = NodeIncarnation {
            node_id: "holder".into(),
            incarnation: 2,
        };
        let non_holder = NodeIncarnation {
            node_id: "other".into(),
            incarnation: 3,
        };
        let durable = [anvil_mvcc_consensus::NodeIncarnation {
            node_id: crate::mvcc_bootstrap::consensus_control_node_id(&holder.node_id),
            incarnation: holder.incarnation,
        }];

        assert_eq!(
            durable_peer_candidates(
                &[stale_holder, non_holder, holder.clone()],
                &durable,
            ),
            vec![&holder]
        );
    }

    #[tokio::test]
    async fn detects_a_decision_gap_and_gc_loss_as_unrecoverable() {
        let prepared_directory = tempdir().unwrap();
        let local_directory = tempdir().unwrap();
        let prepared = AppendOnlyPreparedBundleStore::open(
            prepared_directory.path(),
            "cluster",
            NodeIncarnation {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            "zone-a",
        )
        .unwrap();
        let local = LocalMvccStore::open(local_directory.path()).unwrap();
        let gap = Arc::new(Source {
            decisions: StdMutex::new(vec![AppliedDecision {
                position: CommitVersion(2),
                committed_bundle: None,
            }]),
            gc: CommitVersion(0),
            violations: vec![],
        });
        let error = worker(gap, prepared.clone(), local.clone())
            .apply_available()
            .await
            .unwrap_err();
        assert!(is_unrecoverable(&error));

        let collected = Arc::new(Source {
            decisions: StdMutex::new(Vec::new()),
            gc: CommitVersion(1),
            violations: vec![],
        });
        let error = worker(collected, prepared, local)
            .apply_available()
            .await
            .unwrap_err();
        assert!(is_unrecoverable(&error));
    }

    #[tokio::test]
    async fn applies_consensus_gc_watermark_only_after_local_catch_up() {
        let prepared_directory = tempdir().unwrap();
        let local_directory = tempdir().unwrap();
        let prepared = AppendOnlyPreparedBundleStore::open(
            prepared_directory.path(),
            "cluster",
            NodeIncarnation {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            "zone-a",
        )
        .unwrap();
        let local = LocalMvccStore::open(local_directory.path()).unwrap();
        local.advance_decision_watermark(1).unwrap();
        let source = Arc::new(Source {
            decisions: StdMutex::new(Vec::new()),
            gc: CommitVersion(1),
            violations: vec![],
        });

        assert_eq!(
            worker(source, prepared, local.clone())
                .apply_available()
                .await
                .unwrap(),
            0
        );
        assert_eq!(local.gc_watermark().unwrap(), 1);
    }

    #[tokio::test]
    async fn rejects_a_foreign_cluster_decision_before_fetching() {
        let prepared_directory = tempdir().unwrap();
        let local_directory = tempdir().unwrap();
        let prepared = AppendOnlyPreparedBundleStore::open(
            prepared_directory.path(),
            "cluster",
            NodeIncarnation {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            "zone-a",
        )
        .unwrap();
        let local = LocalMvccStore::open(local_directory.path()).unwrap();
        let bundle = bundle();
        let mut decision = committed(&bundle, 1);
        decision.committed_bundle.as_mut().unwrap().cluster_id_hash =
            cluster_id_hash("another-cluster");
        let source = Arc::new(Source {
            decisions: StdMutex::new(vec![decision]),
            gc: CommitVersion(0),
            violations: vec![],
        });

        let error = worker(source, prepared, local)
            .apply_available()
            .await
            .unwrap_err();
        assert!(is_unrecoverable(&error));
    }

    #[tokio::test]
    async fn local_holder_loss_is_reported_durably_and_idempotently() {
        let prepared_directory = tempdir().unwrap();
        let local_directory = tempdir().unwrap();
        let prepared = AppendOnlyPreparedBundleStore::open(
            prepared_directory.path(),
            "cluster",
            NodeIncarnation {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            "zone-a",
        )
        .unwrap();
        let local = LocalMvccStore::open(local_directory.path()).unwrap();
        let violation = LocalDurabilityViolation {
            commit_version: CommitVersion(7),
            bundle_hash: BundleHash([3; 32]),
            lost_holder: anvil_mvcc_consensus::NodeIncarnation {
                node_id: anvil_mvcc_consensus::NodeId(11),
                incarnation: 2,
            },
            detected_at_log_index: 9,
        };
        let source = Arc::new(Source {
            decisions: StdMutex::new(Vec::new()),
            gc: CommitVersion(0),
            violations: vec![violation],
        });
        let worker = worker(source, prepared, local.clone());

        assert_eq!(worker.apply_available().await.unwrap(), 0);
        assert_eq!(worker.apply_available().await.unwrap(), 0);
        let records = local.local_durability_violations().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].commit_version, 7);
        assert_eq!(records[0].lost_holder_node_id, 11);
        assert_eq!(records[0].lost_holder_incarnation, 2);
        assert_eq!(records[0].detected_at_log_index, 9);
    }
}
