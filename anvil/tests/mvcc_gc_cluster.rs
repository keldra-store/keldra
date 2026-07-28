#![recursion_limit = "512"]

use std::{collections::BTreeMap, time::Duration};

use anvil::{
    mvcc_gc::{
        MVCC_GARBAGE_COLLECTION_ENABLED, advance_garbage_collection_watermark,
        plan_garbage_collection,
    },
    mvcc_transaction::{BundleIdentity, CertificationResult, LogicalKey},
};
use anvil_mvcc_consensus::{CommitVersion, GarbageCollectionPins, NodeId, NodeIncarnation};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn v040_rejects_gc_advancement_and_retains_physical_history() {
    assert!(!MVCC_GARBAGE_COLLECTION_ENABLED);

    let cluster = RealMvccCluster::start().await.unwrap();
    let leader = cluster.wait_for_any_leader(&[0, 1, 2]).await.unwrap();
    let key = LogicalKey {
        table_id: 17,
        application_key: b"gc/retained-history".to_vec(),
    };

    let first = cluster
        .commit(leader, "gc-disabled-1", key.clone(), b"one".to_vec())
        .await
        .unwrap();
    let first_version = committed_version(&first.certification);
    let second = cluster
        .commit(leader, "gc-disabled-2", key.clone(), b"two".to_vec())
        .await
        .unwrap();
    let head = committed_version(&second.certification);
    for node in 0..3 {
        cluster.wait_for_readable_version(node, head).await.unwrap();
    }

    let state = cluster.state(leader);
    let proposal = plan_garbage_collection(
        &state.mvcc.open_transactions,
        state.mvcc.runtime.local_store(),
        1_000,
        0,
        head,
        head,
        replica_pins(&cluster),
    )
    .unwrap();
    assert!(proposal.watermark > 0);
    let error = advance_garbage_collection_watermark(
        &state.mvcc.consensus,
        cluster_hash(state.mvcc.cluster_id()),
        &proposal,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("garbage collection is disabled in Anvil v0.4.0")
    );

    let decisions = state
        .mvcc
        .consensus
        .applied_decisions_after(CommitVersion(0))
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

    // Wait across multiple former coordinator ticks. An accidental restart of
    // automatic GC would advance at least one watermark or retire history.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert_eq!(
        state.mvcc.consensus.gc_safety_watermark().unwrap(),
        CommitVersion(0)
    );
    for node in 0..3 {
        assert_eq!(
            cluster
                .state(node)
                .mvcc
                .runtime
                .local_store()
                .gc_watermark()
                .unwrap(),
            0
        );
    }

    let retained = state
        .mvcc
        .runtime
        .read_at(&key, first_version)
        .unwrap()
        .expect("v0.4.0 retains historical MVCC rows");
    assert_eq!(retained.value, b"one");
    for identity in prepared {
        assert!(
            state.mvcc.prepared_bundle(&identity).unwrap().is_some(),
            "v0.4.0 retains prepared bundle bytes"
        );
    }
}
