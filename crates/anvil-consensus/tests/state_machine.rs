use anvil_consensus::{
    ApplyError, ApplyResult, BundleHash, BundleRef, Command, CommitBatch, CommitResult,
    DurabilityClass, DurabilityEvidenceHash, ExecutorNomination, InvocationFingerprint,
    InvocationId, NodeId, ProgramHash, ProgramPathHash, StateMachine,
};

fn node(node_id: u64) -> NodeId {
    NodeId(node_id)
}

#[derive(Clone, Copy)]
struct Program {
    path_hash: ProgramPathHash,
    object_hash: ProgramHash,
}

fn program(path: u8, definition: u8) -> Program {
    Program {
        path_hash: ProgramPathHash([path; 32]),
        object_hash: ProgramHash([definition; 32]),
    }
}

fn batch(
    executor: NodeId,
    nomination_log_index: u64,
    program: Program,
    invocation: u8,
    input: u8,
) -> CommitBatch {
    CommitBatch {
        executor,
        nomination_log_index,
        program_path_hash: program.path_hash,
        program_hash: program.object_hash,
        invocation_id: InvocationId([invocation; 32]),
        input_fingerprint: InvocationFingerprint([input; 32]),
        bundle_ref: BundleRef([invocation.wrapping_add(1); 32]),
        bundle_hash: BundleHash([invocation.wrapping_add(2); 32]),
        durability_class: DurabilityClass([2; 32]),
        durability_evidence_hash: DurabilityEvidenceHash([invocation.wrapping_add(3); 32]),
    }
}

fn nominate(state: &mut StateMachine, log_index: u64, executor: NodeId) -> ExecutorNomination {
    let result = state
        .apply(log_index, &Command::NominateExecutor { executor })
        .unwrap();
    let ApplyResult::ExecutorNominated(nomination) = result else {
        unreachable!()
    };
    nomination
}

fn commit(
    state: &mut StateMachine,
    log_index: u64,
    batch: CommitBatch,
) -> Result<CommitResult, ApplyError> {
    let result = state.apply(log_index, &Command::CommitBatch(batch))?;
    let ApplyResult::BatchCommitted(result) = result else {
        unreachable!()
    };
    Ok(result)
}

#[test]
fn configuration_requires_both_hard_bounds() {
    assert_eq!(
        StateMachine::new(0, 1),
        Err(ApplyError::InvalidCommitEntryLimit)
    );
    assert_eq!(
        StateMachine::new(1, 0),
        Err(ApplyError::InvalidCommitByteLimit)
    );

    let state = StateMachine::new(8, 64 * 1024).unwrap();
    assert_eq!(state.max_commit_entries(), 8);
    assert_eq!(state.max_commit_bytes(), 64 * 1024);
    assert_eq!(state.executor(), None);
    assert_eq!(state.commit_suffix_len(), 0);
    assert_eq!(state.commit_suffix_bytes(), 0);
    assert_eq!(state.finalized_through(), None);
}

#[test]
fn nomination_uses_the_committed_log_index_as_its_only_fence() {
    let mut state = StateMachine::new(8, 64 * 1024).unwrap();
    let first = nominate(&mut state, 11, node(7));
    assert_eq!(
        first,
        ExecutorNomination {
            executor: node(7),
            nomination_log_index: 11,
        }
    );

    let replacement = nominate(&mut state, 19, node(8));
    assert_eq!(state.executor(), Some(replacement));
    assert_eq!(replacement.nomination_log_index, 19);
    assert_eq!(
        state.apply(20, &Command::NominateExecutor { executor: node(0) }),
        Err(ApplyError::InvalidNodeId)
    );
}

#[test]
fn commit_is_fenced_by_the_current_executor_and_pins_an_external_program() {
    let mut state = StateMachine::new(8, 64 * 1024).unwrap();
    let executor = node(1);
    let code = program(3, 9);

    assert_eq!(
        commit(&mut state, 3, batch(executor, 1, code, 1, 2)),
        Err(ApplyError::ExecutorNotNominated)
    );
    nominate(&mut state, 4, executor);
    assert_eq!(
        commit(&mut state, 5, batch(executor, 3, code, 1, 2)),
        Err(ApplyError::NominationFenceMismatch {
            expected: 4,
            requested: 3,
        })
    );
    assert_eq!(
        commit(&mut state, 6, batch(node(2), 4, code, 1, 2)),
        Err(ApplyError::NotCurrentExecutor {
            current: executor,
            requested: node(2),
        })
    );
    let committed = commit(&mut state, 7, batch(executor, 4, code, 1, 2)).unwrap();
    assert_eq!(committed.receipt.committed_batch.commit_cursor, 7);
    assert_eq!(state.last_commit_cursor(), Some(7));
    assert_eq!(
        committed.receipt.committed_batch.program_path_hash,
        code.path_hash
    );
    assert_eq!(
        committed.receipt.committed_batch.program_hash,
        code.object_hash
    );
}

#[test]
fn commits_are_globally_ordered_by_their_raft_indexes() {
    let mut state = StateMachine::new(8, 64 * 1024).unwrap();
    let executor = node(1);
    let code = program(1, 10);
    nominate(&mut state, 3, executor);

    let first = commit(&mut state, 8, batch(executor, 3, code, 1, 11)).unwrap();
    let second = commit(&mut state, 13, batch(executor, 3, code, 2, 12)).unwrap();
    assert_eq!(first.receipt.committed_batch.commit_cursor, 8);
    assert_eq!(second.receipt.committed_batch.commit_cursor, 13);
    assert_eq!(
        state
            .committed_batches()
            .map(|receipt| receipt.committed_batch.commit_cursor)
            .collect::<Vec<_>>(),
        vec![8, 13]
    );
    assert_eq!(state.committed_batch(8), Some(first.receipt));
    assert_eq!(state.committed_batch(13), Some(second.receipt));
}

#[test]
fn idempotent_retry_returns_the_original_commit_cursor_after_renomination() {
    let mut state = StateMachine::new(8, 64 * 1024).unwrap();
    let first_executor = node(1);
    let next_executor = node(2);
    let code = program(1, 10);
    nominate(&mut state, 3, first_executor);

    let original_batch = batch(first_executor, 3, code, 1, 21);
    let original = commit(&mut state, 7, original_batch).unwrap();
    assert!(!original.replayed);

    nominate(&mut state, 8, next_executor);
    let mut recovered = batch(next_executor, 8, code, 1, 21);
    recovered.bundle_ref = BundleRef([88; 32]);
    recovered.bundle_hash = BundleHash([89; 32]);
    recovered.durability_class = DurabilityClass([90; 32]);
    recovered.durability_evidence_hash = DurabilityEvidenceHash([91; 32]);
    let replay = commit(&mut state, 9, recovered).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, original.receipt);
    assert_eq!(replay.receipt.committed_batch.commit_cursor, 7);
    assert_eq!(
        replay.receipt.committed_batch.durability_class,
        original_batch.durability_class
    );
    assert_eq!(
        replay.receipt.committed_batch.durability_evidence_hash,
        original_batch.durability_evidence_hash
    );
    assert_eq!(state.last_commit_cursor(), Some(7));
    assert_eq!(state.commit_suffix_len(), 1);

    let conflicting = batch(next_executor, 8, code, 1, 22);
    assert_eq!(
        commit(&mut state, 10, conflicting),
        Err(ApplyError::IdempotencyConflict {
            invocation_id: original_batch.invocation_id,
        })
    );
}

#[test]
fn stale_executor_cannot_even_replay_a_retained_commit() {
    let mut state = StateMachine::new(8, 64 * 1024).unwrap();
    let first_executor = node(1);
    let next_executor = node(2);
    let code = program(1, 10);
    nominate(&mut state, 3, first_executor);
    let original = batch(first_executor, 3, code, 1, 21);
    commit(&mut state, 5, original).unwrap();
    nominate(&mut state, 6, next_executor);

    assert_eq!(
        commit(&mut state, 7, original),
        Err(ApplyError::NotCurrentExecutor {
            current: next_executor,
            requested: first_executor,
        })
    );
}

#[test]
fn finalized_through_prunes_a_global_prefix_and_frees_capacity() {
    let mut state = StateMachine::new(2, 64 * 1024).unwrap();
    let executor = node(1);
    let code = program(1, 10);
    nominate(&mut state, 2, executor);
    let first = commit(&mut state, 5, batch(executor, 2, code, 1, 11)).unwrap();
    let second = commit(&mut state, 7, batch(executor, 2, code, 2, 12)).unwrap();
    let bytes_before = state.commit_suffix_bytes();

    assert!(matches!(
        commit(&mut state, 8, batch(executor, 2, code, 3, 13)),
        Err(ApplyError::CommitTailFull {
            entries: 2,
            max_entries: 2,
            ..
        })
    ));

    let advanced = state
        .apply(
            9,
            &Command::FinalizedThrough {
                executor,
                nomination_log_index: 2,
                through_commit_cursor: first.receipt.committed_batch.commit_cursor,
            },
        )
        .unwrap();
    let ApplyResult::FinalizationAdvanced {
        through_commit_cursor,
        pruned_entries,
        pruned_bytes,
    } = advanced
    else {
        unreachable!()
    };
    assert_eq!(through_commit_cursor, 5);
    assert_eq!(pruned_entries, 1);
    assert!(pruned_bytes > 0);
    assert_eq!(state.commit_suffix_bytes(), bytes_before - pruned_bytes);
    assert_eq!(state.invocation_receipt(first.receipt.invocation_id), None);
    assert_eq!(
        state.invocation_receipt(second.receipt.invocation_id),
        Some(second.receipt)
    );

    let third = commit(&mut state, 10, batch(executor, 2, code, 3, 13)).unwrap();
    assert_eq!(third.receipt.committed_batch.commit_cursor, 10);
}

#[test]
fn byte_bound_also_backpressures_without_mutating_state() {
    let mut state = StateMachine::new(100, 1).unwrap();
    let executor = node(1);
    let code = program(1, 10);
    nominate(&mut state, 2, executor);

    assert!(matches!(
        commit(&mut state, 4, batch(executor, 2, code, 1, 11)),
        Err(ApplyError::CommitTailFull {
            entries: 0,
            bytes: 0,
            max_entries: 100,
            max_bytes: 1,
            ..
        })
    ));
    assert_eq!(state.commit_suffix_len(), 0);
    assert_eq!(state.commit_suffix_bytes(), 0);
    assert_eq!(state.last_commit_cursor(), None);
}

#[test]
fn finalization_is_monotonic_bounded_and_fenced() {
    let mut state = StateMachine::new(8, 64 * 1024).unwrap();
    let executor = node(1);
    let code = program(1, 10);
    nominate(&mut state, 2, executor);
    commit(&mut state, 5, batch(executor, 2, code, 1, 11)).unwrap();
    commit(&mut state, 9, batch(executor, 2, code, 2, 12)).unwrap();

    assert_eq!(
        state.apply(
            10,
            &Command::FinalizedThrough {
                executor,
                nomination_log_index: 1,
                through_commit_cursor: 5,
            },
        ),
        Err(ApplyError::NominationFenceMismatch {
            expected: 2,
            requested: 1,
        })
    );
    assert_eq!(
        state.apply(
            11,
            &Command::FinalizedThrough {
                executor,
                nomination_log_index: 2,
                through_commit_cursor: 10,
            },
        ),
        Err(ApplyError::FinalizedBeyondLastCommit {
            last_commit_cursor: 9,
            requested: 10,
        })
    );
    state
        .apply(
            12,
            &Command::FinalizedThrough {
                executor,
                nomination_log_index: 2,
                through_commit_cursor: 5,
            },
        )
        .unwrap();
    assert_eq!(
        state.apply(
            13,
            &Command::FinalizedThrough {
                executor,
                nomination_log_index: 2,
                through_commit_cursor: 4,
            },
        ),
        Err(ApplyError::FinalizedThroughRegressed {
            current: 5,
            requested: 4,
        })
    );
    assert_eq!(state.finalized_through(), Some(5));
}

#[test]
fn malformed_compact_identifiers_are_rejected() {
    let mut state = StateMachine::new(8, 64 * 1024).unwrap();
    let executor = node(1);
    let code = program(1, 10);
    nominate(&mut state, 2, executor);

    let mut malformed = batch(executor, 2, code, 1, 11);
    malformed.bundle_ref = BundleRef([0; 32]);
    assert_eq!(
        commit(&mut state, 4, malformed),
        Err(ApplyError::InvalidBundleRef)
    );
    malformed = batch(executor, 2, code, 1, 11);
    malformed.durability_class = DurabilityClass([0; 32]);
    assert_eq!(
        commit(&mut state, 5, malformed),
        Err(ApplyError::InvalidDurabilityClass)
    );
    malformed = batch(executor, 2, code, 1, 11);
    malformed.durability_evidence_hash = DurabilityEvidenceHash([0; 32]);
    assert_eq!(
        commit(&mut state, 6, malformed),
        Err(ApplyError::InvalidDurabilityEvidenceHash)
    );
}
