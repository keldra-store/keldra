//! Cluster-wide MVCC garbage-collection planning and consensus advancement.

use anvil_mvcc_consensus::{
    CommitVersion as ConsensusVersion, ControlApplyResult, GarbageCollectionPins,
    GarbageCollectionSafetyError, OpenRaftConsensus,
};
use anyhow::{Result, bail};

use crate::{
    mvcc_open_transactions::OpenTransactionRegistry, mvcc_store::LocalMvccStore,
    mvcc_transaction::CommitVersion,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GarbageCollectionProposal {
    pub current_watermark: CommitVersion,
    pub watermark: CommitVersion,
    pub pins: GarbageCollectionPins,
}

/// Combines policy and replica reports with durable transaction and work pins.
///
/// `reported_pins` must include every supported voting or catch-up replica.
/// Absence must block proposal construction at the reporting layer; it is never
/// interpreted here as permission to collect.
pub fn plan_garbage_collection(
    registry: &OpenTransactionRegistry,
    store: &LocalMvccStore,
    now_unix_ms: u64,
    current_watermark: CommitVersion,
    requested_watermark: CommitVersion,
    cluster_head: CommitVersion,
    mut reported_pins: GarbageCollectionPins,
) -> Result<GarbageCollectionProposal> {
    reported_pins.active_snapshots.extend(
        registry
            .active_snapshot_pins(now_unix_ms)?
            .into_iter()
            .map(ConsensusVersion),
    );
    reported_pins.unfinished_work_pins.extend(
        store
            .unfinished_work_pins()?
            .all()
            .into_iter()
            .map(ConsensusVersion),
    );
    let watermark = reported_pins
        .safe_watermark(
            ConsensusVersion(current_watermark),
            ConsensusVersion(requested_watermark),
            ConsensusVersion(cluster_head),
        )?
        .0;
    Ok(GarbageCollectionProposal {
        current_watermark,
        watermark,
        pins: reported_pins,
    })
}

/// Advances through the existing compact Raft control command.
pub async fn advance_garbage_collection_watermark(
    consensus: &OpenRaftConsensus,
    cluster_id_hash: [u8; 32],
    proposal: &GarbageCollectionProposal,
) -> Result<CommitVersion> {
    if proposal.watermark < proposal.current_watermark {
        return Err(GarbageCollectionSafetyError::WatermarkMovedBackwards {
            current: ConsensusVersion(proposal.current_watermark),
            requested: ConsensusVersion(proposal.watermark),
        }
        .into());
    }
    match consensus
        .advance_gc_watermark(cluster_id_hash, ConsensusVersion(proposal.watermark))
        .await?
    {
        ControlApplyResult::GcWatermarkAdvanced(watermark) if watermark.0 == proposal.watermark => {
            Ok(watermark.0)
        }
        _ => bail!("consensus returned an unexpected GC control result"),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use anvil_mvcc_consensus::{CommitVersion as ConsensusVersion, NodeId, NodeIncarnation};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        mvcc_node_runtime::CommitOutcome,
        mvcc_open_transactions::TransactionRuntime,
        mvcc_transaction::{
            CertificationResult, DurabilityLevel, ReadConsistency, TransactionBundle,
        },
    };

    struct Runtime;

    #[async_trait::async_trait]
    impl TransactionRuntime for Runtime {
        async fn transaction_snapshot(&self, _: ReadConsistency) -> Result<CommitVersion> {
            Ok(40)
        }

        async fn commit_transaction_bundle(
            &self,
            _: TransactionBundle,
            _: DurabilityLevel,
        ) -> Result<CommitOutcome> {
            unreachable!()
        }

        fn apply_transaction_decision(
            &self,
            _: TransactionBundle,
            _: CertificationResult,
        ) -> Result<CommitOutcome> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn durable_sessions_and_replica_watermarks_constrain_proposal() {
        let registry_dir = tempdir().unwrap();
        let store_dir = tempdir().unwrap();
        let registry = OpenTransactionRegistry::open(registry_dir.path()).unwrap();
        let store = LocalMvccStore::open(store_dir.path()).unwrap();
        registry
            .begin(
                &Runtime,
                "cluster",
                "principal",
                "gc-pin",
                Duration::from_secs(30),
                DurabilityLevel::Local,
                ReadConsistency::Local,
                1_000,
            )
            .await
            .unwrap();
        let replica = NodeIncarnation {
            node_id: NodeId(2),
            incarnation: 1,
        };
        let pins = GarbageCollectionPins {
            replica_applied_watermarks: BTreeMap::from([(replica, ConsensusVersion(35))]),
            ..Default::default()
        };

        let proposal =
            plan_garbage_collection(&registry, &store, 1_001, 10, 100, 90, pins).unwrap();
        assert_eq!(proposal.watermark, 35);
        assert!(
            proposal
                .pins
                .active_snapshots
                .contains(&ConsensusVersion(40))
        );
    }
}
