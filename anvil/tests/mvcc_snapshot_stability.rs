#![recursion_limit = "512"]

use std::time::Duration;

use anvil::mvcc_transaction::{DurabilityLevel, LogicalKey, ReadConsistency};
use anvil_test_utils::mvcc_cluster::RealMvccCluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn open_transaction_keeps_one_snapshot_across_a_concurrent_commit() {
    let cluster = RealMvccCluster::start().await.unwrap();
    let leader = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let key = LogicalKey {
        table_id: 0x7a01,
        application_key: b"snapshot-stability/account".to_vec(),
    };

    let initial = cluster
        .commit(leader, "snapshot-stability-initial", key.clone(), b"old".to_vec())
        .await
        .unwrap();
    let initial_version = match initial.certification {
        anvil::mvcc_transaction::CertificationResult::Committed { commit_version } => {
            commit_version
        }
        anvil::mvcc_transaction::CertificationResult::Aborted { reason } => {
            panic!("initial transaction aborted: {reason:?}")
        }
    };
    let mvcc = &cluster.state(leader).mvcc;
    let reader = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id().to_string(),
            "snapshot-reader",
            "snapshot-stability-reader",
            Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            10,
        )
        .await
        .unwrap();
    assert!(reader.snapshot_version >= initial_version);
    let fixed_snapshot = reader.snapshot_version;
    assert_eq!(
        mvcc.read_transaction_value(&reader.transaction_id, "snapshot-reader", &key)
            .unwrap(),
        Some(b"old".to_vec())
    );

    let update = cluster
        .commit(leader, "snapshot-stability-update", key.clone(), b"new".to_vec())
        .await
        .unwrap();
    let update_version = match update.certification {
        anvil::mvcc_transaction::CertificationResult::Committed { commit_version } => {
            commit_version
        }
        anvil::mvcc_transaction::CertificationResult::Aborted { reason } => {
            panic!("concurrent transaction aborted: {reason:?}")
        }
    };
    assert!(update_version > reader.snapshot_version);
    assert_eq!(
        mvcc.runtime
            .read_at(&key, update_version)
            .unwrap()
            .unwrap()
            .value,
        b"new"
    );

    // Both reads remain anchored to the transaction's original snapshot even
    // after the newer version is locally applied and visible to latest reads.
    for _ in 0..2 {
        assert_eq!(
            mvcc.read_transaction_value(&reader.transaction_id, "snapshot-reader", &key)
                .unwrap(),
            Some(b"old".to_vec())
        );
        assert_eq!(
            mvcc.open_transactions
                .handle(&reader.transaction_id)
                .unwrap()
                .snapshot_version,
            fixed_snapshot
        );
    }
}
