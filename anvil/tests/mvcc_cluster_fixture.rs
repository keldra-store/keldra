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
