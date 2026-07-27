use std::{collections::BTreeMap, time::Duration};

use anvil::{
    mvcc_gc::{advance_garbage_collection_watermark, plan_garbage_collection},
    mvcc_transaction::{
        BundleIdentity, CertificationResult, DurabilityLevel, LogicalKey, ReadConsistency,
    },
};
use anvil_mvcc_consensus::{
    CommitVersion, GarbageCollectionPins, NodeId, NodeIncarnation,
};
use anvil_test_utils::mvcc_cluster::RealMvccCluster;
use sha2::{Digest, Sha256};

fn committed_version(result: &CertificationResult) -> u64 {
    match result {
        CertificationResult::Committed { commit_version } => *commit_version,
        CertificationResult::Aborted { reason } => panic!("transaction aborted: {reason:?}"),
    }
}

fn cluster_hash(cluster_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    let domain = b"anvil.mvcc.cluster-id.v1";
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain);
    hasher.update((cluster_id.len() as u64).to_be_bytes());
    hasher.update(cluster_id.as_bytes());
    hasher.finalize().into()
}

fn replica_pins(cluster: &RealMvccCluster) -> GarbageCollectionPins {
    GarbageCollectionPins {
        replica_applied_watermarks: (0..3)
            .map(|node| {
                (
                    NodeIncarnation {
                        node_id: NodeId(node as u64 + 1),
                        incarnation: 1,
                    },
                    CommitVersion(
                        cluster
                            .state(node)
                            .mvcc
                            .runtime
                            .local_store()
                            .readable_version()
                            .unwrap(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>(),
        ..Default::default()
    }
}

async fn advance_gc(
    cluster: &RealMvccCluster,
    leader: usize,
    requested: u64,
    now_unix_ms: u64,
) -> u64 {
    let state = cluster.state(leader);
    let current = state
        .mvcc
        .runtime
        .local_store()
        .gc_watermark()
        .unwrap();
    let head = state.mvcc.runtime.applied_version().unwrap();
    let proposal = plan_garbage_collection(
        &state.mvcc.open_transactions,
        state.mvcc.runtime.local_store(),
        now_unix_ms,
        current,
        requested,
        head,
        replica_pins(cluster),
    )
    .unwrap();
    advance_garbage_collection_watermark(
        &state.mvcc.consensus,
        cluster_hash(state.mvcc.cluster_id()),
        &proposal,
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn real_cluster_gc_respects_snapshot_and_lagging_replica_then_reclaims_after_catchup() {
    let cluster = RealMvccCluster::start().await.unwrap();
    let leader = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let lagging = [0, 1, 2]
        .into_iter()
        .find(|node| *node != leader)
        .unwrap();
    let key = LogicalKey {
        table_id: 17,
        application_key: b"gc/retained-history".to_vec(),
    };

    let first = cluster
        .commit(leader, "gc-history-1", key.clone(), b"one".to_vec())
        .await
        .unwrap();
    let first_version = committed_version(&first.certification);
    let second = cluster
        .commit(leader, "gc-history-2", key.clone(), b"two".to_vec())
        .await
        .unwrap();
    let snapshot_version = committed_version(&second.certification);
    for node in 0..3 {
        cluster
            .wait_for_applied_version(node, snapshot_version)
            .await
            .unwrap();
    }

    let principal = "gc-snapshot-principal";
    let snapshot = cluster
        .state(leader)
        .mvcc
        .open_transactions
        .begin(
            cluster.state(leader).mvcc.runtime.as_ref(),
            cluster.state(leader).mvcc.cluster_id(),
            principal,
            "gc-active-snapshot",
            Duration::from_secs(60),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            1_000,
        )
        .await
        .unwrap();
    assert!(snapshot.snapshot_version >= snapshot_version);
    let snapshot_version = snapshot.snapshot_version;
    for node in 0..3 {
        cluster
            .wait_for_readable_version(node, snapshot_version)
            .await
            .unwrap();
    }

    cluster.partition(lagging);
    let third = cluster
        .commit(leader, "gc-history-3", key.clone(), b"three".to_vec())
        .await
        .unwrap();
    let third_version = committed_version(&third.certification);
    let fourth = cluster
        .commit(leader, "gc-history-4", key.clone(), b"four".to_vec())
        .await
        .unwrap();
    let head = committed_version(&fourth.certification);

    let decisions = cluster
        .state(leader)
        .mvcc
        .consensus
        .applied_decisions_after(CommitVersion(snapshot_version))
        .unwrap();
    let prepared = decisions
        .iter()
        .filter_map(|decision| decision.committed_bundle.as_ref())
        .map(|bundle| BundleIdentity {
            hash: format!("sha256:{}", hex::encode(bundle.bundle_hash.0)),
            length: bundle.bundle_length,
        })
        .collect::<Vec<_>>();
    assert!(!prepared.is_empty());
    for identity in &prepared {
        assert!(
            cluster
                .state(leader)
                .mvcc
                .prepared_bundle(identity)
                .unwrap()
                .is_some()
        );
    }

    let pinned = advance_gc(&cluster, leader, head, 1_001).await;
    assert_eq!(pinned, snapshot_version);
    cluster
        .wait_for_gc_watermark(leader, snapshot_version)
        .await
        .unwrap();
    let retained = cluster
        .state(leader)
        .mvcc
        .runtime
        .read_at(&key, snapshot_version)
        .unwrap()
        .expect("active snapshot history remains readable");
    assert_eq!(retained.value, b"two");
    assert!(
        cluster
            .state(leader)
            .mvcc
            .runtime
            .read_at(&key, first_version)
            .is_err(),
        "history below the approved watermark is rejected"
    );

    cluster
        .state(leader)
        .mvcc
        .open_transactions
        .rollback(&snapshot.transaction_id, principal, 1_002)
        .unwrap();
    let still_lagging = advance_gc(&cluster, leader, head, 1_003).await;
    assert!(
        still_lagging < third_version,
        "the partitioned replica must continue to pin collection"
    );

    cluster.heal(lagging);
    cluster
        .wait_for_applied_version(lagging, head)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let final_watermark = advance_gc(&cluster, leader, head, 1_004).await;
    assert_eq!(final_watermark, head);
    for node in 0..3 {
        cluster.wait_for_gc_watermark(node, head).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let obsolete_removed = decisions
                .iter()
                .filter(|decision| decision.position.0 < head)
                .filter_map(|decision| decision.committed_bundle.as_ref())
                .map(|bundle| BundleIdentity {
                    hash: format!("sha256:{}", hex::encode(bundle.bundle_hash.0)),
                    length: bundle.bundle_length,
                })
                .any(|identity| {
                    cluster
                        .state(leader)
                        .mvcc
                        .prepared_bundle(&identity)
                        .unwrap()
                        .is_none()
                });
            if obsolete_removed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("obsolete prepared bundles are compacted after GC");
    assert!(
        cluster
            .state(leader)
            .mvcc
            .runtime
            .read_at(&key, snapshot_version)
            .is_err(),
        "released snapshot history is below the final watermark"
    );
}
