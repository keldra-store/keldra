#![recursion_limit = "512"]

use anvil::mvcc_transaction::{CertificationResult, LogicalKey};
use anvil_test_utils::mvcc_cluster::RealMvccCluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn real_cluster_quorum_commit_is_readable_after_node_restart() {
    let mut cluster = RealMvccCluster::start().await.unwrap();
    let leader = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let key = LogicalKey {
        table_id: 1,
        application_key: b"fixture/smoke".to_vec(),
    };
    let outcome = cluster
        .commit(leader, "fixture-smoke", key.clone(), b"value".to_vec())
        .await
        .unwrap();
    let commit_version = match outcome.certification {
        CertificationResult::Committed { commit_version } => commit_version,
        CertificationResult::Aborted { reason } => panic!("fixture smoke aborted: {reason:?}"),
    };

    cluster.restart_node(leader).await.unwrap();
    let row = cluster
        .state(leader)
        .mvcc
        .runtime
        .read_at(&key, commit_version)
        .unwrap()
        .expect("committed row remains readable after restart");
    assert_eq!(row.value, b"value");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn real_cluster_elects_a_new_leader_and_catches_up_crashed_leader() {
    let mut cluster = RealMvccCluster::start().await.unwrap();
    let original_leader = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != original_leader)
        .collect::<Vec<_>>();

    cluster.partition(original_leader);
    cluster
        .state(original_leader)
        .mvcc
        .consensus
        .shutdown()
        .await
        .unwrap();
    let replacement_leader = cluster.wait_for_any_leader(&survivors).await.unwrap();
    let key = LogicalKey {
        table_id: 2,
        application_key: b"fixture/leader-recovery".to_vec(),
    };
    let outcome = cluster
        .commit(
            replacement_leader,
            "fixture-leader-recovery",
            key.clone(),
            b"committed-with-one-node-down".to_vec(),
        )
        .await
        .unwrap();
    let commit_version = match outcome.certification {
        CertificationResult::Committed { commit_version } => commit_version,
        CertificationResult::Aborted { reason } => {
            panic!("replacement leader transaction aborted: {reason:?}")
        }
    };

    cluster.restart_node(original_leader).await.unwrap();
    cluster
        .wait_for_applied_version(original_leader, commit_version)
        .await
        .unwrap();
    let row = cluster
        .state(original_leader)
        .mvcc
        .runtime
        .read_at(&key, commit_version)
        .unwrap()
        .expect("restarted former leader catches up the committed bundle");
    assert_eq!(row.value, b"committed-with-one-node-down");
}
