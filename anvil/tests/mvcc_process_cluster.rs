#![recursion_limit = "512"]

use std::{path::PathBuf, time::Duration};

use anvil::anvil_api::{MvccReadConsistency, WriteState};
use anvil_test_utils::{
    GrpcLostResponseProxy, mvcc_process_cluster::ProcessMvccCluster,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_commit_survives_lost_response_and_coordinator_sigkill() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_anvil-server"));
    let mut cluster = ProcessMvccCluster::start(binary).await.unwrap();
    let coordinator = cluster.wait_for_leader(&[0, 1, 2]).await.unwrap();
    let transaction = cluster
        .begin_transaction(coordinator, MvccReadConsistency::Linearized)
        .await
        .unwrap();

    let mut proxy = GrpcLostResponseProxy::start(&cluster.public_endpoint(coordinator)).await;
    let commit = cluster.commit_transaction(
        proxy.endpoint().to_string(),
        transaction.transaction_id,
    );
    let (commit_result, dropped) = tokio::join!(
        commit,
        proxy.wait_until_response_dropped(Duration::from_secs(10))
    );
    assert!(commit_result.is_err(), "the commit acknowledgement must be lost");
    dropped.unwrap();

    // The proxy only drops after the server has produced its unary response,
    // so this is the real after-proposal boundary.
    cluster.sigkill(coordinator).await.unwrap();
    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != coordinator)
        .collect::<Vec<_>>();
    let survivor = cluster.wait_for_leader(&survivors).await.unwrap();
    let survivor_snapshot = cluster
        .begin_transaction(survivor, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    assert!(
        survivor_snapshot.snapshot_version > transaction.snapshot_version,
        "the surviving quorum must retain the acknowledged proposal"
    );

    cluster.restart(coordinator).await.unwrap();
    let restarted_snapshot = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(snapshot) = cluster
                .begin_transaction(coordinator, MvccReadConsistency::LocalSnapshot)
                .await
            {
                if snapshot.snapshot_version >= survivor_snapshot.snapshot_version {
                    return snapshot;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("restarted coordinator catches up from its original RocksDB directory");
    assert_eq!(restarted_snapshot.state, "open");

    let follow_up = cluster
        .commit_transaction(
            cluster.public_endpoint(survivor),
            survivor_snapshot.transaction_id,
        )
        .await
        .unwrap();
    assert_eq!(follow_up.state, WriteState::Committed as i32);
}
