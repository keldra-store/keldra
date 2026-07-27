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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_object_batch_recovers_after_leader_crashes_before_local_batch_write() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_anvil-server"));
    let mut cluster = ProcessMvccCluster::start(binary).await.unwrap();
    let leader = cluster.wait_for_leader(&[0, 1, 2]).await.unwrap();
    let bucket_name = format!("process-crash-{}", uuid::Uuid::new_v4().simple());
    let bucket_id = cluster.create_bucket(leader, &bucket_name).await.unwrap();
    let transaction = cluster
        .begin_transaction(leader, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    let keys = ["crash-batch/one.bin", "crash-batch/two.bin"];
    let staged = cluster
        .stage_object_puts(
            leader,
            &bucket_name,
            bucket_id,
            &transaction.transaction_id,
            &[(keys[0], b"one"), (keys[1], b"two")],
        )
        .await
        .unwrap();
    assert_eq!(staged.write_state, WriteState::Staged as i32);
    for key in keys {
        assert!(!cluster.object_exists(leader, &bucket_name, key).await.unwrap());
    }

    cluster.arm_hard_crash(leader, "MvccBatchWrite").unwrap();
    let commit = cluster.commit_transaction(
        cluster.public_endpoint(leader),
        transaction.transaction_id.clone(),
    );
    let (commit_result, crash_result) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(10), commit),
        cluster.wait_for_hard_crash(leader, Duration::from_secs(10)),
    );
    crash_result.unwrap();
    assert!(
        !matches!(commit_result, Ok(Ok(_))),
        "a process abort before its local RocksDB batch must not return success"
    );

    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != leader)
        .collect::<Vec<_>>();
    let survivor = cluster.wait_for_leader(&survivors).await.unwrap();
    let stable = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(status) = cluster
                .get_transaction(survivor, &transaction.transaction_id)
                .await
            {
                if status.state == "committed" {
                    return status;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("surviving quorum resolves the commit to one stable outcome");
    assert_eq!(stable.state, "committed");
    let retried = cluster
        .commit_transaction(
            cluster.public_endpoint(survivor),
            transaction.transaction_id.clone(),
        )
        .await
        .unwrap();
    assert_eq!(retried.state, WriteState::Committed as i32);
    for key in keys {
        assert!(cluster.object_exists(survivor, &bucket_name, key).await.unwrap());
    }

    cluster.restart(leader).await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let status = cluster
                .get_transaction(leader, &transaction.transaction_id)
                .await;
            let both_visible = matches!(
                cluster.object_exists(leader, &bucket_name, keys[0]).await,
                Ok(true)
            ) && matches!(
                cluster.object_exists(leader, &bucket_name, keys[1]).await,
                Ok(true)
            );
            if status
                .is_ok_and(|status| status.state == "committed")
                && both_visible
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("restarted original disk converges without partial object visibility");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_node_is_replaced_by_higher_incarnation_and_catches_up() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_anvil-server"));
    let mut cluster = ProcessMvccCluster::start(binary).await.unwrap();
    let old_leader = cluster.wait_for_leader(&[0, 1, 2]).await.unwrap();
    let replaced = (0..3).find(|node| *node != old_leader).unwrap();
    cluster.sigkill(replaced).await.unwrap();

    let survivors = [0, 1, 2]
        .into_iter()
        .filter(|node| *node != replaced)
        .collect::<Vec<_>>();
    let leader = cluster.wait_for_leader(&survivors).await.unwrap();
    cluster.spawn_replacement(replaced, 2).await.unwrap();
    cluster.apply_replacement(leader, replaced, true).await.unwrap();
    for survivor in survivors.iter().copied().filter(|node| *node != leader) {
        cluster
            .apply_replacement(survivor, replaced, false)
            .await
            .unwrap();
    }

    let before = cluster
        .begin_transaction(leader, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    let committed = cluster
        .commit_transaction(cluster.public_endpoint(leader), before.transaction_id)
        .await
        .unwrap();
    assert_eq!(committed.state, WriteState::Committed as i32);
    let replacement = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(snapshot) = cluster
                .begin_transaction(replaced, MvccReadConsistency::LocalSnapshot)
                .await
            {
                if snapshot.snapshot_version > before.snapshot_version {
                    return snapshot;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("higher incarnation catches up and serves its applied snapshot");
    assert_eq!(replacement.state, "open");

    let obsolete_endpoint = cluster.spawn_obsolete_incarnation(replaced).await.unwrap();
    let obsolete_local = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(snapshot) = cluster
                .begin_transaction_at(
                    obsolete_endpoint.clone(),
                    MvccReadConsistency::LocalSnapshot,
                )
                .await
            {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("obsolete process starts from its retired disk");
    assert!(obsolete_local.snapshot_version <= replacement.snapshot_version);
    let obsolete_attempt = tokio::time::timeout(
        Duration::from_secs(5),
        cluster.begin_transaction_at(
            obsolete_endpoint,
            MvccReadConsistency::Linearized,
        ),
    )
    .await;
    assert!(
        !matches!(obsolete_attempt, Ok(Ok(_))),
        "obsolete incarnation must not regain linearized consensus participation"
    );

    let healthy = cluster
        .begin_transaction(leader, MvccReadConsistency::Linearized)
        .await
        .unwrap();
    let healthy_commit = cluster
        .commit_transaction(cluster.public_endpoint(leader), healthy.transaction_id)
        .await
        .unwrap();
    assert_eq!(healthy_commit.state, WriteState::Committed as i32);
}
