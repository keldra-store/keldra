use anvil::mvcc_transaction::{CertificationResult, LogicalKey};
use anvil_test_utils::mvcc_cluster::RealMvccCluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn bidirectional_partition_elects_rejects_minority_then_heals_and_converges() {
    let cluster = RealMvccCluster::start().await.unwrap();
    let old_leader = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let majority = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != old_leader)
        .collect::<Vec<_>>();
    cluster.partition(old_leader);
    let new_leader = cluster.wait_for_any_leader(&majority).await.unwrap();

    let rejected = cluster
        .commit(
            old_leader,
            "partitioned-minority-write",
            LogicalKey {
                table_id: 9,
                application_key: b"fixture/minority-rejected".to_vec(),
            },
            b"must-not-commit".to_vec(),
        )
        .await;
    assert!(rejected.is_err(), "partitioned minority must not commit");

    let key = LogicalKey {
        table_id: 9,
        application_key: b"fixture/majority-progress".to_vec(),
    };
    let outcome = cluster
        .commit(
            new_leader,
            "partition-majority-progress",
            key.clone(),
            b"majority-value".to_vec(),
        )
        .await
        .unwrap();
    let commit_version = match outcome.certification {
        CertificationResult::Committed { commit_version } => commit_version,
        CertificationResult::Aborted { reason } => {
            panic!("majority-side transaction aborted: {reason:?}")
        }
    };

    cluster.heal(old_leader);
    cluster
        .wait_for_applied_version(old_leader, commit_version)
        .await
        .unwrap();
    let row = cluster
        .state(old_leader)
        .mvcc
        .runtime
        .read_at(&key, commit_version)
        .unwrap()
        .expect("healed minority catches up");
    assert_eq!(row.value, b"majority-value");
}
