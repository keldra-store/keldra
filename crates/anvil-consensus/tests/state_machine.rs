use anvil_consensus::{
    ATOMIC_REPLAY_RETENTION_MILLIS, ApplyError, ApplyResult, BundleHash, BundleRef, Command,
    CommitBatch, CommitResult, DurabilityClass, DurabilityEvidenceHash, ExecutorNomination,
    InvocationFingerprint, InvocationId, MAX_COMMITTED_INVOCATION_BYTES, MAX_COMMITTED_INVOCATIONS,
    NodeId, ProgramHash, ProgramPathHash, StateMachine,
};

fn node(node_id: u64) -> NodeId {
    NodeId(node_id)
}

fn cluster(seed: u8) -> [u8; 16] {
    [seed; 16]
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
        bundle_ref: BundleRef {
            hash: [invocation.wrapping_add(1).max(1); 32],
            length: u64::from(invocation) + 1,
        },
        bundle_hash: BundleHash([invocation.wrapping_add(2).max(1); 32]),
        durability_class: DurabilityClass([2; 32]),
        durability_evidence_hash: DurabilityEvidenceHash([invocation.wrapping_add(3).max(1); 32]),
        proposal_at_unix_millis: 1_000 + u64::from(invocation),
        replay_expires_at_unix_millis: 1_000
            + u64::from(invocation)
            + ATOMIC_REPLAY_RETENTION_MILLIS,
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
    assert_eq!(state.cluster_id(), None);
    assert!(!state.system_bootstrap().is_complete());
    assert_eq!(state.system_bootstrap().version(), None);
    assert_eq!(state.system_bootstrap().committed_log_index(), None);
    assert_eq!(state.executor(), None);
    assert_eq!(state.unfinalized_commit_len(), 0);
    assert_eq!(state.unfinalized_commit_bytes().unwrap(), 0);
    assert_eq!(state.finalized_through(), None);
}

#[test]
fn cluster_identity_is_validated_and_initialized_exactly_once() {
    let mut state = StateMachine::new(8, 64 * 1024).unwrap();
    assert_eq!(
        state.apply(
            1,
            &Command::InitializeCluster {
                cluster_id: cluster(0).into(),
            },
        ),
        Err(ApplyError::InvalidClusterId)
    );

    let identity = cluster(7);
    assert_eq!(
        state
            .apply(
                2,
                &Command::InitializeCluster {
                    cluster_id: identity.into(),
                },
            )
            .unwrap(),
        ApplyResult::ClusterInitialized {
            cluster_id: identity.into(),
        }
    );
    assert_eq!(state.cluster_id().map(|id| id.into_bytes()), Some(identity));

    // Applying the same logical initialization at another committed index is
    // an idempotent success and cannot replace the stable identity.
    assert_eq!(
        state
            .apply(
                3,
                &Command::InitializeCluster {
                    cluster_id: identity.into(),
                },
            )
            .unwrap(),
        ApplyResult::ClusterInitialized {
            cluster_id: identity.into(),
        }
    );

    let conflicting = cluster(8);
    assert_eq!(
        state.apply(
            4,
            &Command::InitializeCluster {
                cluster_id: conflicting.into(),
            },
        ),
        Err(ApplyError::ClusterIdentityConflict {
            current: identity.into(),
            requested: conflicting.into(),
        })
    );
    assert_eq!(state.cluster_id().map(|id| id.into_bytes()), Some(identity));
}

#[test]
fn system_bootstrap_completion_is_fenced_versioned_and_idempotent() {
    let mut state = StateMachine::new(8, 64 * 1024).unwrap();
    let first_executor = node(3);
    let next_executor = node(4);
    nominate(&mut state, 1, first_executor);

    assert_eq!(
        state.apply(
            2,
            &Command::CompleteSystemBootstrap {
                executor: first_executor,
                nomination_log_index: 1,
                bootstrap_version: 1,
            },
        ),
        Err(ApplyError::ClusterNotInitialized)
    );

    state
        .apply(
            3,
            &Command::InitializeCluster {
                cluster_id: cluster(9).into(),
            },
        )
        .unwrap();
    assert_eq!(
        state.apply(
            4,
            &Command::CompleteSystemBootstrap {
                executor: first_executor,
                nomination_log_index: 1,
                bootstrap_version: 0,
            },
        ),
        Err(ApplyError::UnsupportedSystemBootstrapVersion { requested: 0 })
    );
    assert_eq!(
        state.apply(
            5,
            &Command::CompleteSystemBootstrap {
                executor: first_executor,
                nomination_log_index: 2,
                bootstrap_version: 1,
            },
        ),
        Err(ApplyError::NominationFenceMismatch {
            expected: 1,
            requested: 2,
        })
    );

    let completed = state
        .apply(
            6,
            &Command::CompleteSystemBootstrap {
                executor: first_executor,
                nomination_log_index: 1,
                bootstrap_version: 1,
            },
        )
        .unwrap();
    let ApplyResult::SystemBootstrapCompleted(completed) = completed else {
        unreachable!()
    };
    assert!(completed.is_complete());
    assert_eq!(completed.version(), Some(1));
    assert_eq!(completed.committed_log_index(), Some(6));

    nominate(&mut state, 7, next_executor);
    let replayed = state
        .apply(
            8,
            &Command::CompleteSystemBootstrap {
                executor: next_executor,
                nomination_log_index: 7,
                bootstrap_version: 1,
            },
        )
        .unwrap();
    let ApplyResult::SystemBootstrapCompleted(replayed) = replayed else {
        unreachable!()
    };
    assert_eq!(replayed, completed);
    assert_eq!(replayed.committed_log_index(), Some(6));

    assert_eq!(
        state.apply(
            9,
            &Command::CompleteSystemBootstrap {
                executor: next_executor,
                nomination_log_index: 7,
                bootstrap_version: 2,
            },
        ),
        Err(ApplyError::UnsupportedSystemBootstrapVersion { requested: 2 })
    );
    assert_eq!(state.system_bootstrap(), completed);
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
    assert_eq!(committed.invocation.committed_batch.commit_cursor, 7);
    assert_eq!(state.last_commit_cursor(), Some(7));
    assert_eq!(
        committed.invocation.committed_batch.program_path_hash,
        code.path_hash
    );
    assert_eq!(
        committed.invocation.committed_batch.program_hash,
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
    assert_eq!(first.invocation.committed_batch.commit_cursor, 8);
    assert_eq!(second.invocation.committed_batch.commit_cursor, 13);
    assert_eq!(
        state
            .unfinalized_invocations()
            .map(|invocation| invocation.committed_batch.commit_cursor)
            .collect::<Vec<_>>(),
        vec![8, 13]
    );
    assert_eq!(state.committed_invocation(8), Some(first.invocation));
    assert_eq!(state.committed_invocation(13), Some(second.invocation));
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
    recovered.bundle_ref = BundleRef {
        hash: [88; 32],
        length: 88,
    };
    recovered.bundle_hash = BundleHash([89; 32]);
    recovered.durability_class = DurabilityClass([90; 32]);
    recovered.durability_evidence_hash = DurabilityEvidenceHash([91; 32]);
    let replay = commit(&mut state, 9, recovered).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.invocation, original.invocation);
    assert_eq!(replay.invocation.committed_batch.commit_cursor, 7);
    assert_eq!(
        replay.invocation.committed_batch.durability_class,
        original_batch.durability_class
    );
    assert_eq!(
        replay.invocation.committed_batch.durability_evidence_hash,
        original_batch.durability_evidence_hash
    );
    assert_eq!(state.last_commit_cursor(), Some(7));
    assert_eq!(state.unfinalized_commit_len(), 1);

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
fn finalized_through_frees_recovery_capacity_but_retains_replay() {
    let mut state = StateMachine::new(2, 64 * 1024).unwrap();
    let executor = node(1);
    let code = program(1, 10);
    nominate(&mut state, 2, executor);
    let first = commit(&mut state, 5, batch(executor, 2, code, 1, 11)).unwrap();
    let second = commit(&mut state, 7, batch(executor, 2, code, 2, 12)).unwrap();
    let bytes_before = state.unfinalized_commit_bytes().unwrap();

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
                through_commit_cursor: first.invocation.committed_batch.commit_cursor,
            },
        )
        .unwrap();
    let ApplyResult::FinalizationAdvanced {
        through_commit_cursor,
    } = advanced
    else {
        unreachable!()
    };
    assert_eq!(through_commit_cursor, 5);
    assert_eq!(state.unfinalized_commit_len(), 1);
    assert!(state.unfinalized_commit_bytes().unwrap() < bytes_before);
    assert_eq!(
        state.replay_entry(first.invocation.invocation_id, 2_000),
        Some(first.invocation)
    );
    assert_eq!(
        state.replay_entry(second.invocation.invocation_id, 2_000),
        Some(second.invocation)
    );

    let third = commit(&mut state, 10, batch(executor, 2, code, 3, 13)).unwrap();
    assert_eq!(third.invocation.committed_batch.commit_cursor, 10);
}

#[test]
fn next_commit_prunes_only_expired_finalized_replay_entries() {
    let mut state = StateMachine::new(8, 64 * 1024).unwrap();
    let executor = node(1);
    let code = program(1, 10);
    nominate(&mut state, 2, executor);
    let first = commit(&mut state, 5, batch(executor, 2, code, 1, 11)).unwrap();
    let first_expiry = first.invocation.replay_expires_at_unix_millis;
    state
        .apply(
            6,
            &Command::FinalizedThrough {
                executor,
                nomination_log_index: 2,
                through_commit_cursor: 5,
            },
        )
        .unwrap();

    assert_eq!(
        state.replay_entry(first.invocation.invocation_id, first_expiry - 1),
        Some(first.invocation)
    );
    assert_eq!(
        state.replay_entry(first.invocation.invocation_id, first_expiry),
        None
    );
    assert_eq!(state.committed_invocation_len(), 1);

    let mut next = batch(executor, 2, code, 1, 12);
    next.proposal_at_unix_millis = first_expiry;
    next.replay_expires_at_unix_millis = first_expiry + ATOMIC_REPLAY_RETENTION_MILLIS;
    commit(&mut state, 7, next).unwrap();
    assert_eq!(state.committed_invocation(5), None);
    assert_eq!(state.committed_invocation_len(), 1);
}

#[test]
fn unexpired_committed_invocations_backpressure_at_the_fixed_entry_bound() {
    let mut state = StateMachine::new(
        MAX_COMMITTED_INVOCATIONS + 1,
        MAX_COMMITTED_INVOCATION_BYTES,
    )
    .unwrap();
    let executor = node(1);
    let code = program(1, 10);
    nominate(&mut state, 1, executor);
    let mut log_index = 2_u64;

    for sequence in 0..MAX_COMMITTED_INVOCATIONS {
        let seed = (sequence % 254 + 1) as u8;
        let mut candidate = batch(executor, 1, code, seed, seed.wrapping_add(1));
        let mut invocation_id = [1_u8; 32];
        invocation_id[..4].copy_from_slice(&sequence.to_be_bytes());
        candidate.invocation_id = InvocationId(invocation_id);
        candidate.proposal_at_unix_millis = 1_000 + u64::from(sequence);
        candidate.replay_expires_at_unix_millis =
            candidate.proposal_at_unix_millis + ATOMIC_REPLAY_RETENTION_MILLIS;
        let committed = commit(&mut state, log_index, candidate).unwrap();
        state
            .apply(
                log_index + 1,
                &Command::FinalizedThrough {
                    executor,
                    nomination_log_index: 1,
                    through_commit_cursor: committed.invocation.committed_batch.commit_cursor,
                },
            )
            .unwrap();
        log_index += 2;
    }

    assert_eq!(state.committed_invocation_len(), MAX_COMMITTED_INVOCATIONS);
    assert!(state.committed_invocation_bytes() <= MAX_COMMITTED_INVOCATION_BYTES);
    let mut overflow = batch(executor, 1, code, 255, 254);
    overflow.invocation_id = InvocationId([0xfe; 32]);
    overflow.proposal_at_unix_millis = 10_000;
    overflow.replay_expires_at_unix_millis = 10_000 + ATOMIC_REPLAY_RETENTION_MILLIS;
    assert!(matches!(
        commit(&mut state, log_index, overflow),
        Err(ApplyError::CommittedInvocationWindowFull {
            entries: MAX_COMMITTED_INVOCATIONS,
            ..
        })
    ));
    assert_eq!(state.committed_invocation_len(), MAX_COMMITTED_INVOCATIONS);
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
    assert_eq!(state.unfinalized_commit_len(), 0);
    assert_eq!(state.unfinalized_commit_bytes().unwrap(), 0);
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
    malformed.bundle_ref = BundleRef {
        hash: [0; 32],
        length: 1,
    };
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
    malformed = batch(executor, 2, code, 1, 11);
    malformed.replay_expires_at_unix_millis += 1;
    assert_eq!(
        commit(&mut state, 7, malformed),
        Err(ApplyError::InvalidReplayExpiry)
    );
}
