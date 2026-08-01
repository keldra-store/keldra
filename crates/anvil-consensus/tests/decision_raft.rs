use std::{sync::Arc, time::Duration};

use anvil_consensus::{
    ApplyError, ApplyResult, BundleHash, BundleRef, Command, CommitBatch, DecisionRaft,
    DecisionRaftError, DurabilityClass, DurabilityEvidenceHash, InvocationFingerprint,
    InvocationId, NoPeerTransport, NodeId, PeerNode, ProgramHash, ProgramPathHash,
};

fn batch(nomination_log_index: u64, id: u8) -> CommitBatch {
    CommitBatch {
        executor: NodeId(1),
        nomination_log_index,
        program_path_hash: ProgramPathHash([3; 32]),
        program_hash: ProgramHash([4; 32]),
        invocation_id: InvocationId([id; 32]),
        input_fingerprint: InvocationFingerprint([id.wrapping_add(1); 32]),
        bundle_ref: BundleRef([id.wrapping_add(2); 32]),
        bundle_hash: BundleHash([id.wrapping_add(3); 32]),
        durability_class: DurabilityClass([2; 32]),
        durability_evidence_hash: DurabilityEvidenceHash([id.wrapping_add(4); 32]),
    }
}

async fn open(path: &std::path::Path) -> DecisionRaft {
    DecisionRaft::open(path, 1, 4, 64 * 1024, Arc::new(NoPeerTransport))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_node_decisions_keep_original_commit_cursors_across_restart_and_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let peer = PeerNode::new("node-1");

    let raft = open(directory.path()).await;
    raft.ensure_one_node(peer.clone()).await.unwrap();
    raft.ensure_one_node(peer.clone()).await.unwrap();
    assert_eq!(
        raft.wait_for_leader(Duration::from_secs(5)).await.unwrap(),
        1
    );

    assert!(matches!(
        raft.submit(Command::NominateExecutor {
            executor: NodeId(2),
        })
        .await,
        Err(DecisionRaftError::Rejected(
            ApplyError::ExecutorNotCurrentMember {
                executor: NodeId(2)
            }
        ))
    ));

    let nominated = raft
        .submit(Command::NominateExecutor {
            executor: NodeId(1),
        })
        .await
        .unwrap();
    let ApplyResult::ExecutorNominated(nomination) = nominated.result else {
        panic!("nomination returned the wrong domain result")
    };
    assert_eq!(nomination.nomination_log_index, nominated.log_index);

    let first_batch = batch(nomination.nomination_log_index, 10);
    let committed = raft
        .submit(Command::CommitBatch(first_batch))
        .await
        .unwrap();
    let ApplyResult::BatchCommitted(first) = committed.result else {
        panic!("commit returned the wrong domain result")
    };
    assert!(!first.replayed);
    assert_eq!(
        first.receipt.committed_batch.commit_cursor,
        committed.log_index
    );

    raft.shutdown().await.unwrap();
    drop(raft);

    // No explicit snapshot was made: the state is rebuilt from the compact
    // applied journal, including the original commit cursor.
    let raft = open(directory.path()).await;
    raft.bootstrap_one_node(peer.clone()).await.unwrap();
    assert_eq!(
        raft.wait_for_leader(Duration::from_secs(5)).await.unwrap(),
        1
    );
    assert_eq!(
        raft.state()
            .unwrap()
            .invocation_receipt(first_batch.invocation_id),
        Some(first.receipt)
    );

    let replayed = raft
        .submit(Command::CommitBatch(first_batch))
        .await
        .unwrap();
    assert!(replayed.log_index > committed.log_index);
    let ApplyResult::BatchCommitted(replayed_result) = replayed.result else {
        panic!("retry returned the wrong domain result")
    };
    assert!(replayed_result.replayed);
    assert_eq!(replayed_result.receipt, first.receipt);
    assert_eq!(
        replayed_result.receipt.committed_batch.commit_cursor,
        committed.log_index
    );

    let second_batch = batch(nomination.nomination_log_index, 20);
    let second_committed = raft
        .submit(Command::CommitBatch(second_batch))
        .await
        .unwrap();
    let ApplyResult::BatchCommitted(second) = second_committed.result else {
        panic!("second commit returned the wrong domain result")
    };

    raft.submit(Command::FinalizedThrough {
        executor: NodeId(1),
        nomination_log_index: nomination.nomination_log_index,
        through_commit_cursor: first.receipt.committed_batch.commit_cursor,
    })
    .await
    .unwrap();
    assert_eq!(
        raft.state()
            .unwrap()
            .invocation_receipt(first_batch.invocation_id),
        None
    );
    assert_eq!(
        raft.state()
            .unwrap()
            .invocation_receipt(second_batch.invocation_id),
        Some(second.receipt)
    );

    raft.snapshot(Duration::from_secs(5)).await.unwrap();
    raft.shutdown().await.unwrap();
    drop(raft);

    let raft = open(directory.path()).await;
    raft.ensure_one_node(peer).await.unwrap();
    assert_eq!(
        raft.wait_for_leader(Duration::from_secs(5)).await.unwrap(),
        1
    );
    let state = raft.state().unwrap();
    assert_eq!(state.finalized_through(), Some(committed.log_index));
    assert_eq!(state.commit_suffix_len(), 1);
    assert_eq!(
        state.invocation_receipt(second_batch.invocation_id),
        Some(second.receipt)
    );
    raft.shutdown().await.unwrap();
}
