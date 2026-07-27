#![recursion_limit = "512"]

use std::time::Duration;

use anvil::{
    mvcc_node_runtime::CommitOutcome,
    mvcc_transaction::{
        CertificationAbort, CertificationResult, DurabilityLevel, LogicalKey, ReadConsistency,
    },
};
use anvil_test_utils::mvcc_cluster::RealMvccCluster;

async fn begin(
    cluster: &RealMvccCluster,
    node: usize,
    idempotency_key: &str,
) -> anvil::mvcc_open_transactions::TransactionHandle {
    let mvcc = &cluster.state(node).mvcc;
    mvcc.open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id().to_string(),
            "conflict-e2e-principal",
            idempotency_key,
            Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            1,
        )
        .await
        .unwrap()
}

fn committed_version(outcome: &CommitOutcome) -> u64 {
    match &outcome.certification {
        CertificationResult::Committed { commit_version } => *commit_version,
        CertificationResult::Aborted { reason } => {
            panic!("expected committed transaction, got {reason:?}")
        }
    }
}

async fn wait_for_value(
    cluster: &RealMvccCluster,
    commit_version: u64,
    key: &LogicalKey,
    expected: &[u8],
) {
    for node in 0..3 {
        cluster
            .wait_for_applied_version(node, commit_version)
            .await
            .unwrap();
        let row = cluster
            .state(node)
            .mvcc
            .runtime
            .read_at(key, commit_version)
            .unwrap()
            .expect("committed row converged to every node");
        assert_eq!(row.value, expected);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_point_writes_commit_exactly_one_and_converge() {
    let cluster = RealMvccCluster::start().await.unwrap();
    let leader = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let other = (leader + 1) % 3;
    let key = LogicalKey {
        table_id: 0x5101,
        application_key: b"conflict/same-key".to_vec(),
    };
    let first = begin(&cluster, leader, "point-conflict-first").await;
    let second = begin(&cluster, other, "point-conflict-second").await;
    for (node, handle, value) in [
        (leader, &first, b"first".as_slice()),
        (other, &second, b"second".as_slice()),
    ] {
        let mvcc = &cluster.state(node).mvcc;
        mvcc.open_transactions
            .observe_point(
                &handle.transaction_id,
                mvcc.cluster_id(),
                key.clone(),
                None,
                2,
            )
            .unwrap();
        mvcc.open_transactions
            .put(
                &handle.transaction_id,
                mvcc.cluster_id(),
                key.clone(),
                value.to_vec(),
                3,
            )
            .unwrap();
    }

    let winner = cluster
        .state(leader)
        .mvcc
        .open_transactions
        .commit(
            cluster.state(leader).mvcc.runtime.as_ref(),
            &first.transaction_id,
            "conflict-e2e-principal",
            4,
        )
        .await
        .unwrap();
    let loser = cluster
        .state(other)
        .mvcc
        .open_transactions
        .commit(
            cluster.state(other).mvcc.runtime.as_ref(),
            &second.transaction_id,
            "conflict-e2e-principal",
            4,
        )
        .await
        .unwrap();
    let version = committed_version(&winner);
    assert!(matches!(
        loser.certification,
        CertificationResult::Aborted {
            reason: CertificationAbort::PointConflict { .. }
        }
    ));
    wait_for_value(&cluster, version, &key, b"first").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_range_observer_rejects_phantom_and_cluster_converges() {
    let cluster = RealMvccCluster::start().await.unwrap();
    let leader = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let observer_node = (leader + 1) % 3;
    let phantom_key = LogicalKey {
        table_id: 0x5102,
        application_key: b"orders/middle".to_vec(),
    };
    let observer_write = LogicalKey {
        table_id: 0x5103,
        application_key: b"observer/must-not-commit".to_vec(),
    };
    let inserter = begin(&cluster, leader, "range-conflict-inserter").await;
    let observer = begin(&cluster, observer_node, "range-conflict-observer").await;
    let observer_mvcc = &cluster.state(observer_node).mvcc;
    observer_mvcc
        .open_transactions
        .observe_range(
            &observer.transaction_id,
            observer_mvcc.cluster_id(),
            phantom_key.table_id,
            Some(b"orders/a".to_vec()),
            Some(b"orders/z".to_vec()),
            None,
            2,
        )
        .unwrap();
    observer_mvcc
        .open_transactions
        .put(
            &observer.transaction_id,
            observer_mvcc.cluster_id(),
            observer_write.clone(),
            b"must-abort".to_vec(),
            3,
        )
        .unwrap();

    let inserter_mvcc = &cluster.state(leader).mvcc;
    inserter_mvcc
        .open_transactions
        .put(
            &inserter.transaction_id,
            inserter_mvcc.cluster_id(),
            phantom_key.clone(),
            b"inserted".to_vec(),
            2,
        )
        .unwrap();
    let inserted = inserter_mvcc
        .open_transactions
        .commit(
            inserter_mvcc.runtime.as_ref(),
            &inserter.transaction_id,
            "conflict-e2e-principal",
            4,
        )
        .await
        .unwrap();
    let rejected = observer_mvcc
        .open_transactions
        .commit(
            observer_mvcc.runtime.as_ref(),
            &observer.transaction_id,
            "conflict-e2e-principal",
            4,
        )
        .await
        .unwrap();
    let version = committed_version(&inserted);
    assert!(matches!(
        rejected.certification,
        CertificationResult::Aborted {
            reason: CertificationAbort::RangeConflict { .. }
        }
    ));
    wait_for_value(&cluster, version, &phantom_key, b"inserted").await;
    for node in 0..3 {
        assert!(
            cluster
                .state(node)
                .mvcc
                .runtime
                .read_at(&observer_write, version)
                .unwrap()
                .is_none(),
            "the range-conflicted transaction was not applied"
        );
    }
}
