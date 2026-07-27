//! Dependency-injected node facade for the complete MVCC transaction path.

use anyhow::{Result, bail};

use crate::{
    mvcc_consensus_adapter::ConsensusTransactionCertifier,
    mvcc_store::{ApplyOutcome, LocalMvccStore, VisibleRow},
    mvcc_transaction::{
        BundleReplicator, CertificationResult, ClusterOwnershipResolver, DurabilityLevel,
        DurabilityPolicy, LogicalKey, PreparedBundleStore, ReadConsistency, TransactionBundle,
        TransactionCoordinator,
    },
};

/// Owns the product data path while leaving transport, persistence location,
/// and the concrete consensus runtime injectable by node startup.
pub struct MvccNodeRuntime<S, R, C> {
    coordinator: TransactionCoordinator<S, R, ConsensusTransactionCertifier<C>>,
    local: LocalMvccStore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOutcome {
    pub certification: CertificationResult,
    pub local_apply: Option<ApplyOutcome>,
}

impl<S, R, C> MvccNodeRuntime<S, R, C>
where
    S: PreparedBundleStore,
    R: BundleReplicator,
    C: anvil_mvcc_consensus::Consensus,
{
    pub fn new(
        prepared_bundles: S,
        replicator: R,
        consensus: C,
        policy: DurabilityPolicy,
        local: LocalMvccStore,
    ) -> Result<Self> {
        Ok(Self {
            coordinator: TransactionCoordinator::new(
                prepared_bundles,
                replicator,
                ConsensusTransactionCertifier::new(consensus),
                policy,
            )?,
            local,
        })
    }

    pub fn new_with_ownership_resolver(
        prepared_bundles: S,
        replicator: R,
        consensus: C,
        policy: DurabilityPolicy,
        local: LocalMvccStore,
        ownership_resolver: std::sync::Arc<dyn ClusterOwnershipResolver>,
    ) -> Result<Self> {
        Ok(Self {
            coordinator: TransactionCoordinator::new(
                prepared_bundles,
                replicator,
                ConsensusTransactionCertifier::new(consensus),
                policy,
            )?
            .with_ownership_resolver(ownership_resolver),
            local,
        })
    }

    pub async fn snapshot(&self, consistency: ReadConsistency) -> Result<u64> {
        // A consensus snapshot can race local GC application: the ordered
        // target may already be below the local store's watermark by the
        // time the caller starts reading.  Returning that stale version
        // would make every subsequent read fail closed.  Use the local GC
        // watermark as a floor and wait until that floor is readable.
        let target = if consistency == ReadConsistency::LocalSnapshot {
            0
        } else {
            self.coordinator.snapshot(consistency).await?
        };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let readable = self.local.readable_version()?;
            let gc_watermark = self.local.gc_watermark()?;
            let required = target.max(gc_watermark);
            if readable >= required {
                return Ok(if consistency == ReadConsistency::LocalSnapshot {
                    readable
                } else {
                    required
                });
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "linearized MVCC snapshot {target} is not locally applied; readable watermark is {readable}"
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// Persists and replicates the bundle, certifies it, then makes a committed
    /// result locally visible. An abort never reaches local MVCC application.
    pub async fn commit(
        &self,
        bundle: TransactionBundle,
        durability: DurabilityLevel,
    ) -> Result<CommitOutcome> {
        let total_started_at = std::time::Instant::now();
        let certification = self.coordinator.commit(bundle.clone(), durability).await?;
        let outcome = self.apply_certification(bundle, certification)?;
        crate::perf::record_transaction_duration(
            "total",
            if outcome.local_apply.is_some() {
                "committed"
            } else {
                "aborted"
            },
            total_started_at.elapsed(),
        );
        Ok(outcome)
    }

    /// Applies an outcome received during restart recovery or replica catch-up.
    ///
    /// The caller must obtain `certification` from the verified consensus
    /// decision stream. Passing an aborted decision is deliberately a no-op.
    pub fn apply_certification(
        &self,
        bundle: TransactionBundle,
        certification: CertificationResult,
    ) -> Result<CommitOutcome> {
        let local_apply = match certification {
            CertificationResult::Committed { commit_version } => {
                let apply_started_at = std::time::Instant::now();
                #[cfg(test)]
                crate::mvcc_fault_injection::hit(
                    crate::mvcc_fault_injection::FaultPoint::BeforeApply,
                )?;
                let outcome = self.local.apply_certified_bundle_and_advance(
                    commit_version,
                    &bundle,
                    commit_version,
                )?;
                #[cfg(test)]
                crate::mvcc_fault_injection::hit(
                    crate::mvcc_fault_injection::FaultPoint::AfterApply,
                )?;
                crate::perf::record_mvcc_state(
                    commit_version,
                    commit_version,
                    bundle.writes.len() as u64,
                );
                crate::perf::record_transaction_duration(
                    "apply",
                    "committed",
                    apply_started_at.elapsed(),
                );
                tracing::debug!(
                    operation = "transaction.apply",
                    transaction_id = %bundle.transaction_id,
                    commit_version,
                    "applied certified transaction bundle"
                );
                Some(outcome)
            }
            CertificationResult::Aborted { .. } => None,
        };
        Ok(CommitOutcome {
            certification,
            local_apply,
        })
    }

    pub fn read_at(&self, key: &LogicalKey, snapshot: u64) -> Result<Option<VisibleRow>> {
        self.local.read_at(key, snapshot)
    }

    pub fn read_latest(&self, key: &LogicalKey) -> Result<Option<VisibleRow>> {
        self.local.read_latest(key)
    }

    pub fn scan_table_prefix_at(
        &self,
        table_id: u16,
        application_prefix: &[u8],
        snapshot: u64,
    ) -> Result<Vec<(LogicalKey, VisibleRow)>> {
        self.local
            .scan_table_prefix_at(table_id, application_prefix, snapshot)
    }

    pub fn applied_version(&self) -> Result<u64> {
        self.local.applied_version()
    }

    pub fn local_store(&self) -> &LocalMvccStore {
        &self.local
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use anyhow::Result;
    use async_trait::async_trait;
    use tempfile::tempdir;

    use super::*;
    use crate::mvcc_transaction::{
        BundleDurabilityEvidence, BundleIdentity, CertificationAbort, HierarchicalRangeStampScheme,
        NodeIncarnation, ObjectShardManifestReference, ReplicationEvidence,
        TransactionBundleBuilder,
    };
    use anvil_mvcc_consensus as consensus;

    #[derive(Clone)]
    struct Prepared {
        stages: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl PreparedBundleStore for Prepared {
        async fn persist(
            &self,
            _identity: &BundleIdentity,
            _bytes: &[u8],
        ) -> Result<BundleDurabilityEvidence> {
            self.stages.lock().unwrap().push("persist");
            Ok(BundleDurabilityEvidence {
                cluster_id: "cluster".into(),
                node: NodeIncarnation {
                    node_id: "node-a".into(),
                    incarnation: 1,
                },
                failure_domain: "zone-a".into(),
                complete: true,
                hash_verified: true,
                fsynced: true,
            })
        }
    }

    #[derive(Clone)]
    struct Replicator {
        stages: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl BundleReplicator for Replicator {
        async fn replicate(
            &self,
            _identity: &BundleIdentity,
            _bytes: &[u8],
            _objects: &[ObjectShardManifestReference],
            _durability: DurabilityLevel,
        ) -> Result<ReplicationEvidence> {
            self.stages.lock().unwrap().push("replicate");
            Ok(ReplicationEvidence {
                bundle_holders: Vec::new(),
                objects: Vec::new(),
            })
        }
    }

    #[derive(Clone)]
    struct Consensus {
        state: Arc<Mutex<ConsensusState>>,
        stages: Arc<Mutex<Vec<&'static str>>>,
    }

    struct ConsensusState {
        next_version: u64,
        abort_next: bool,
        decisions: BTreeMap<consensus::TransactionId, consensus::CertificationResult>,
    }

    #[async_trait]
    impl consensus::Consensus for Consensus {
        async fn certify(
            &self,
            command: consensus::CertifyTransaction,
        ) -> std::result::Result<consensus::CertificationResult, consensus::ConsensusError>
        {
            self.stages.lock().unwrap().push("certify");
            let mut state = self.state.lock().unwrap();
            if let Some(result) = state.decisions.get(&command.transaction_id) {
                return Ok(result.clone());
            }
            let version = consensus::CommitVersion(state.next_version);
            state.next_version += 1;
            let result = if std::mem::take(&mut state.abort_next) {
                consensus::CertificationResult::Aborted {
                    at_version: version,
                    bundle_hash: command.bundle_hash,
                    reason: consensus::CertificationAbort::InvalidCommand("test abort".into()),
                }
            } else {
                consensus::CertificationResult::Committed {
                    commit_version: version,
                    bundle_hash: command.bundle_hash,
                }
            };
            state
                .decisions
                .insert(command.transaction_id, result.clone());
            Ok(result)
        }

        async fn linearized_read_barrier(
            &self,
        ) -> std::result::Result<consensus::CommitVersion, consensus::ConsensusError> {
            Ok(self.observed_commit_version())
        }

        fn observed_commit_version(&self) -> consensus::CommitVersion {
            consensus::CommitVersion(self.state.lock().unwrap().next_version.saturating_sub(1))
        }
    }

    fn runtime(
        path: &std::path::Path,
        consensus: Consensus,
        stages: Arc<Mutex<Vec<&'static str>>>,
    ) -> MvccNodeRuntime<Prepared, Replicator, Consensus> {
        MvccNodeRuntime::new(
            Prepared {
                stages: stages.clone(),
            },
            Replicator { stages },
            consensus,
            DurabilityPolicy {
                bundle_quorum_holders: 1,
                tolerated_failure_domains: 0,
            },
            LocalMvccStore::open(path).unwrap(),
        )
        .unwrap()
    }

    fn bundle(id: &str, key: LogicalKey, value: &[u8]) -> TransactionBundle {
        let mut builder = TransactionBundleBuilder::new(
            "cluster",
            id,
            0,
            "principal",
            HierarchicalRangeStampScheme::new(),
        );
        builder.put(key, value.to_vec());
        builder.build().unwrap()
    }

    fn consensus(stages: Arc<Mutex<Vec<&'static str>>>) -> Consensus {
        Consensus {
            state: Arc::new(Mutex::new(ConsensusState {
                next_version: 1,
                abort_next: false,
                decisions: BTreeMap::new(),
            })),
            stages,
        }
    }

    #[tokio::test]
    async fn commit_orders_durability_and_consensus_before_local_visibility() {
        let temp = tempdir().unwrap();
        let stages = Arc::new(Mutex::new(Vec::new()));
        let runtime = runtime(temp.path(), consensus(stages.clone()), stages.clone());
        let key = LogicalKey {
            table_id: 1,
            application_key: b"key".to_vec(),
        };

        let outcome = runtime
            .commit(bundle("tx", key.clone(), b"value"), DurabilityLevel::Local)
            .await
            .unwrap();

        assert!(matches!(
            outcome,
            CommitOutcome {
                certification: CertificationResult::Committed { commit_version: 1 },
                local_apply: Some(ApplyOutcome::Applied),
            }
        ));
        assert_eq!(*stages.lock().unwrap(), ["persist", "replicate", "certify"]);
        assert_eq!(runtime.read_latest(&key).unwrap().unwrap().value, b"value");
    }

    #[tokio::test]
    async fn abort_never_applies_product_rows() {
        let temp = tempdir().unwrap();
        let stages = Arc::new(Mutex::new(Vec::new()));
        let consensus = consensus(stages.clone());
        consensus.state.lock().unwrap().abort_next = true;
        let runtime = runtime(temp.path(), consensus, stages);
        let key = LogicalKey {
            table_id: 2,
            application_key: b"aborted".to_vec(),
        };

        let outcome = runtime
            .commit(
                bundle("abort", key.clone(), b"forbidden"),
                DurabilityLevel::Local,
            )
            .await
            .unwrap();

        assert!(matches!(
            outcome.certification,
            CertificationResult::Aborted {
                reason: CertificationAbort::InvalidCommand(_)
            }
        ));
        assert_eq!(outcome.local_apply, None);
        assert_eq!(runtime.applied_version().unwrap(), 0);
    }

    #[tokio::test]
    async fn retry_after_restart_replays_the_same_commit_idempotently() {
        let temp = tempdir().unwrap();
        let stages = Arc::new(Mutex::new(Vec::new()));
        let consensus = consensus(stages.clone());
        let key = LogicalKey {
            table_id: 3,
            application_key: b"retry".to_vec(),
        };
        let transaction = bundle("stable-id", key.clone(), b"value");
        {
            let runtime = runtime(temp.path(), consensus.clone(), stages.clone());
            runtime
                .commit(transaction.clone(), DurabilityLevel::Local)
                .await
                .unwrap();
        }

        let restarted = runtime(temp.path(), consensus, stages);
        let retry = restarted
            .commit(transaction, DurabilityLevel::Local)
            .await
            .unwrap();
        assert_eq!(retry.local_apply, Some(ApplyOutcome::Replayed));
        assert_eq!(restarted.applied_version().unwrap(), 1);
        assert_eq!(
            restarted.read_latest(&key).unwrap().unwrap().value,
            b"value"
        );
    }

    #[test]
    fn catch_up_applies_only_verified_committed_decisions() {
        let temp = tempdir().unwrap();
        let stages = Arc::new(Mutex::new(Vec::new()));
        let runtime = runtime(temp.path(), consensus(stages.clone()), stages);
        let key = LogicalKey {
            table_id: 4,
            application_key: b"catch-up".to_vec(),
        };
        let transaction = bundle("catch-up", key.clone(), b"value");
        let aborted = runtime
            .apply_certification(
                transaction.clone(),
                CertificationResult::Aborted {
                    reason: CertificationAbort::InvalidCommand("aborted upstream".into()),
                },
            )
            .unwrap();
        assert_eq!(aborted.local_apply, None);

        let committed = runtime
            .apply_certification(
                transaction,
                CertificationResult::Committed { commit_version: 7 },
            )
            .unwrap();
        assert_eq!(committed.local_apply, Some(ApplyOutcome::Applied));
        assert_eq!(runtime.read_latest(&key).unwrap().unwrap().value, b"value");
    }
}
