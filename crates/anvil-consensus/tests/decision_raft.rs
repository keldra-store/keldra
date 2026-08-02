use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use anvil_consensus::{
    ATOMIC_REPLAY_RETENTION_MILLIS, ApplyError, ApplyResult, BundleHash, BundleRef, Command,
    CommitBatch, DecisionRaft, DecisionRaftError, DurabilityClass, DurabilityEvidenceHash,
    InMemoryPeerTransport, InvocationFingerprint, InvocationId, NodeId, PeerNode, ProgramHash,
    ProgramPathHash,
};

fn batch(nomination_log_index: u64, id: u8) -> CommitBatch {
    CommitBatch {
        executor: NodeId(1),
        nomination_log_index,
        program_path_hash: ProgramPathHash([3; 32]),
        program_hash: ProgramHash([4; 32]),
        invocation_id: InvocationId([id; 32]),
        input_fingerprint: InvocationFingerprint([id.wrapping_add(1); 32]),
        bundle_ref: BundleRef {
            hash: [id.wrapping_add(2); 32],
            length: u64::from(id) + 1,
        },
        bundle_hash: BundleHash([id.wrapping_add(3); 32]),
        durability_class: DurabilityClass([2; 32]),
        durability_evidence_hash: DurabilityEvidenceHash([id.wrapping_add(4); 32]),
        proposal_at_unix_millis: 1_000 + u64::from(id),
        replay_expires_at_unix_millis: 1_000 + u64::from(id) + ATOMIC_REPLAY_RETENTION_MILLIS,
    }
}

async fn open(path: &std::path::Path) -> DecisionRaft {
    DecisionRaft::open(path, 1, 4, 64 * 1024).await.unwrap()
}

async fn open_peer(
    path: &std::path::Path,
    node_id: u64,
    transport: &InMemoryPeerTransport,
) -> DecisionRaft {
    DecisionRaft::open_with_transport(path, node_id, 4, 64 * 1024, Arc::new(transport.clone()))
        .await
        .unwrap()
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition did not become true before timeout"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_node_decisions_keep_original_commit_cursors_across_restart_and_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let raft = open(directory.path()).await;
    raft.ensure_one_node().await.unwrap();
    raft.ensure_one_node().await.unwrap();
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
        first.invocation.committed_batch.commit_cursor,
        committed.log_index
    );

    raft.shutdown().await.unwrap();
    drop(raft);

    // No explicit snapshot was made: the state is rebuilt from the compact
    // applied journal, including the original commit cursor.
    let raft = open(directory.path()).await;
    raft.ensure_one_node().await.unwrap();
    assert_eq!(
        raft.wait_for_leader(Duration::from_secs(5)).await.unwrap(),
        1
    );
    assert_eq!(
        raft.state()
            .unwrap()
            .replay_entry(first_batch.invocation_id, 2_000),
        Some(first.invocation)
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
    assert_eq!(replayed_result.invocation, first.invocation);
    assert_eq!(
        replayed_result.invocation.committed_batch.commit_cursor,
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
        through_commit_cursor: first.invocation.committed_batch.commit_cursor,
    })
    .await
    .unwrap();
    assert_eq!(
        raft.state()
            .unwrap()
            .replay_entry(first_batch.invocation_id, 2_000),
        Some(first.invocation)
    );
    assert_eq!(
        raft.state()
            .unwrap()
            .replay_entry(second_batch.invocation_id, 2_000),
        Some(second.invocation)
    );

    raft.snapshot(Duration::from_secs(5)).await.unwrap();
    raft.shutdown().await.unwrap();
    drop(raft);

    let raft = open(directory.path()).await;
    raft.ensure_one_node().await.unwrap();
    assert_eq!(
        raft.wait_for_leader(Duration::from_secs(5)).await.unwrap(),
        1
    );
    let state = raft.state().unwrap();
    assert_eq!(state.finalized_through(), Some(committed.log_index));
    assert_eq!(state.unfinalized_commit_len(), 1);
    assert_eq!(
        state.replay_entry(second_batch.invocation_id, 2_000),
        Some(second.invocation)
    );
    raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_elect_replicate_add_learners_and_replace_a_failed_leader() {
    let first_directory = tempfile::tempdir().unwrap();
    let second_directory = tempfile::tempdir().unwrap();
    let third_directory = tempfile::tempdir().unwrap();
    let transport = InMemoryPeerTransport::new();

    let first = open_peer(first_directory.path(), 1, &transport).await;
    let second = open_peer(second_directory.path(), 2, &transport).await;
    let third = open_peer(third_directory.path(), 3, &transport).await;
    transport.register(1, first.clone()).unwrap();
    transport.register(2, second.clone()).unwrap();
    transport.register(3, third.clone()).unwrap();

    first
        .initialize_genesis(BTreeMap::from([(1, PeerNode::new("memory://1"))]))
        .await
        .unwrap();
    assert_eq!(
        first.wait_for_leader(Duration::from_secs(5)).await.unwrap(),
        1
    );

    first
        .add_learner(2, PeerNode::new("memory://2"), true)
        .await
        .unwrap();
    let nomination = first
        .submit(Command::NominateExecutor {
            executor: NodeId(2),
        })
        .await
        .unwrap();
    let ApplyResult::ExecutorNominated(nomination) = nomination.result else {
        panic!("nomination returned the wrong domain result")
    };
    wait_until(|| second.state().unwrap().executor() == Some(nomination)).await;

    first
        .add_learner(3, PeerNode::new("memory://3"), true)
        .await
        .unwrap();
    wait_until(|| third.state().unwrap().executor() == Some(nomination)).await;

    first
        .change_membership(BTreeSet::from([1, 2, 3]), false)
        .await
        .unwrap();
    wait_until(|| second.current_leader() == Some(1) && third.current_leader() == Some(1)).await;

    first.shutdown().await.unwrap();
    transport.unregister(1).unwrap();

    wait_until(|| {
        second.current_leader() == third.current_leader()
            && second.current_leader().is_some_and(|leader| leader != 1)
    })
    .await;
    let replacement = match second.current_leader() {
        Some(2) => &second,
        Some(3) => &third,
        leader => panic!("unexpected replacement leader {leader:?}"),
    };
    let replacement_nomination = replacement
        .submit(Command::NominateExecutor {
            executor: NodeId(3),
        })
        .await
        .unwrap();
    let ApplyResult::ExecutorNominated(replacement_nomination) = replacement_nomination.result
    else {
        panic!("replacement nomination returned the wrong domain result")
    };
    wait_until(|| {
        second.state().unwrap().executor() == Some(replacement_nomination)
            && third.state().unwrap().executor() == Some(replacement_nomination)
    })
    .await;

    second.shutdown().await.unwrap();
    third.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consensus_database_reopen_is_bound_to_its_first_local_node_id() {
    let directory = tempfile::tempdir().unwrap();
    let first = DecisionRaft::open(directory.path(), 7, 4, 64 * 1024)
        .await
        .unwrap();
    first.shutdown().await.unwrap();
    drop(first);

    let mismatch = match DecisionRaft::open(directory.path(), 8, 4, 64 * 1024).await {
        Ok(raft) => {
            raft.shutdown().await.unwrap();
            panic!("opening a bound consensus database under another node succeeded")
        }
        Err(error) => error,
    };
    assert!(matches!(
        mismatch,
        DecisionRaftError::Storage(message)
            if message.contains("bound to Raft node 7, not requested node 8")
    ));

    let reopened = DecisionRaft::open(directory.path(), 7, 4, 64 * 1024)
        .await
        .unwrap();
    reopened.shutdown().await.unwrap();
}
