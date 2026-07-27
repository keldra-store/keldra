#![recursion_limit = "512"]

use anvil::{
    mvcc_shard_repair::{
        MissingShardTarget, ShardMaintenanceKind, ShardRepairJob, ShardRepairState,
        resolve_manifest_at_snapshot,
    },
    mvcc_transaction::{
        CertificationResult, DurabilityLevel, LogicalKey, ReadConsistency,
    },
    object_shard_manifest::PhysicalObjectShardManifest,
    shard_placement::{DistributedIngest, ShardPlacementPolicy},
    streaming_erasure::ErasureProfile,
};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn real_cluster_reconstructs_a_deleted_shard_and_publishes_repaired_placement() {
    let cluster = RealMvccCluster::start().await.unwrap();
    let leader = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let mvcc = &cluster.state(leader).mvcc;
    let (candidates, tolerated_failure_domains, _) = mvcc.live_shard_placement().unwrap();
    let profile = ErasureProfile {
        data_shards: 2,
        parity_shards: 1,
        shard_bytes: 64 * 1024,
    };
    let policy = ShardPlacementPolicy {
        tolerated_failure_domains,
    };
    let object_identity = uuid::Uuid::new_v4();
    let plan = policy
        .plan(object_identity, 1, profile, &candidates)
        .unwrap();
    let payload = vec![37_u8; 512 * 1024];
    let mut reader = payload.as_slice();
    let ingest = DistributedIngest::encode(
        &mvcc.replication_client,
        &plan,
        policy,
        profile,
        DurabilityLevel::Erasure,
        &mut reader,
        object_identity,
        None,
        1,
    )
    .await
    .unwrap();
    let manifest = PhysicalObjectShardManifest::from_ingest(
        mvcc.cluster_id(),
        object_identity,
        1,
        profile.data_shards,
        profile.parity_shards,
        profile.shard_bytes,
        &ingest,
    )
    .unwrap();
    let lost = manifest.placements[0].clone();
    let lost_node = cluster.node_index(&lost.node_id).unwrap();
    let lost_path = cluster.replication_transfer_path(lost_node, lost.transfer_id);
    assert!(lost_path.is_file());
    cluster
        .remove_replication_transfer(lost_node, lost.transfer_id)
        .unwrap();
    assert!(!lost_path.exists(), "the selected durable shard was removed");

    let principal = "e2e-repair-producer";
    let handle = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id().to_string(),
            principal,
            format!("repair-loss-{object_identity}"),
            std::time::Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            10,
        )
        .await
        .unwrap();
    let target = plan.targets_by_ordinal[usize::from(lost.shard_ordinal)].clone();
    let job = ShardRepairJob {
        schema: ShardRepairJob::SCHEMA.to_string(),
        cluster_id: mvcc.cluster_id().to_string(),
        transaction_id: handle.transaction_id.clone(),
        kind: ShardMaintenanceKind::Repair,
        target_logical_identity: format!(
            "cluster/{}/object/{}",
            mvcc.cluster_id(),
            manifest.object_hash
        ),
        source_manifest: manifest.clone(),
        source_manifest_hash: hex::encode(
            blake3::hash(&manifest.canonical_bytes().unwrap()).as_bytes(),
        ),
        missing: vec![MissingShardTarget {
            stripe_ordinal: lost.stripe_ordinal,
            shard_ordinal: lost.shard_ordinal,
            target,
        }],
        retiring: Vec::new(),
        originating_snapshot_version: handle.snapshot_version,
        requested_at_unix_ms: 10,
    };
    let job_id = job.job_id().unwrap();
    mvcc.open_transactions
        .add_job(&handle.transaction_id, job.canonical_bytes().unwrap(), 11)
        .unwrap();
    let committed = mvcc
        .open_transactions
        .commit(
            mvcc.runtime.as_ref(),
            &handle.transaction_id,
            principal,
            12,
        )
        .await
        .unwrap();
    assert!(matches!(
        committed.certification,
        CertificationResult::Committed { .. }
    ));

    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let completed = (0..3).any(|node| {
                cluster.state(node).mvcc.runtime.local_store()
                    .shard_repair_record(&job_id)
                    .ok()
                    .flatten()
                    .is_some_and(|record| record.state == ShardRepairState::Complete)
            });
            if completed && lost_path.is_file() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("durable shard repair completed");

    let snapshot = mvcc.runtime.local_store().readable_version().unwrap();
    let repaired =
        resolve_manifest_at_snapshot(mvcc.runtime.local_store(), &manifest, snapshot).unwrap();
    assert!(repaired.placements.iter().any(|placement| {
        placement.stripe_ordinal == lost.stripe_ordinal
            && placement.shard_ordinal == lost.shard_ordinal
            && placement.node_id == lost.node_id
            && placement.node_incarnation == lost.node_incarnation
    }));
    let reconstructed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    repaired
        .read_range_chunks(
            &mvcc.replication_client,
            0,
            repaired.object_length,
            {
                let reconstructed = reconstructed.clone();
                move |chunk| {
                    let reconstructed = reconstructed.clone();
                    async move {
                        reconstructed.lock().unwrap().extend_from_slice(&chunk);
                        Ok(())
                    }
                }
            },
        )
        .await
        .unwrap();
    assert_eq!(*reconstructed.lock().unwrap(), payload);
}
