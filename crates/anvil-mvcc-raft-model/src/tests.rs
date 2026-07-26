use std::collections::BTreeSet;

use proptest::prelude::*;
use stateright::{Checker, Model};

use super::*;

const A: LogicalKey = LogicalKey::new(0, 0, 0);
const B: LogicalKey = LogicalKey::new(1, 1, 0);
const C: LogicalKey = LogicalKey::new(2, 2, 0);
const RANGE: RangeId = RangeId(0);

fn apply(state: MvccRaftState, action: Action) -> MvccRaftState {
    state.apply(action).expect("action should be enabled")
}

fn begin(mut state: MvccRaftState, id: TransactionId) -> MvccRaftState {
    state = apply(state, Action::Begin(id));
    state
}

fn persist(mut state: MvccRaftState, id: TransactionId, nodes: &[NodeId]) -> MvccRaftState {
    for node in nodes {
        let incarnation = state.nodes[node].incarnation;
        state = apply(state, Action::PersistBundle(id, *node, incarnation));
    }
    state
}

fn propose_and_commit(
    mut state: MvccRaftState,
    id: TransactionId,
    durability: Durability,
) -> MvccRaftState {
    state = apply(state, Action::Propose(id, durability));
    apply(state, Action::CommitNext)
}

fn committed_version(state: &MvccRaftState, id: TransactionId) -> u8 {
    match state.transactions[&id].status {
        TransactionStatus::Committed { version } => version,
        status => panic!("expected committed transaction, got {status:?}"),
    }
}

#[test]
fn conflicting_point_transactions_have_one_winner() {
    let one = TransactionId(1);
    let two = TransactionId(2);
    let mut state = begin(MvccRaftState::default(), one);
    state = apply(state, Action::ObservePoint(one, A));
    state = apply(state, Action::Write(one, A, RANGE));
    state = persist(state, one, &[NodeId(0)]);
    state = begin(state, two);
    state = apply(state, Action::ObservePoint(two, A));
    state = apply(state, Action::Write(two, A, RANGE));
    state = persist(state, two, &[NodeId(0)]);
    state = apply(state, Action::Propose(one, Durability::Local));
    state = apply(state, Action::Propose(two, Durability::Local));
    state = apply(state, Action::CommitNext);
    state = apply(state, Action::CommitNext);

    assert!(matches!(
        state.transactions[&one].status,
        TransactionStatus::Committed { .. }
    ));
    assert_eq!(state.transactions[&two].status, TransactionStatus::Aborted);
}

#[test]
fn non_conflicting_transactions_commit_in_raft_order() {
    let mut state = MvccRaftState::default();
    for (id, key, range) in [
        (TransactionId(1), A, RangeId(0)),
        (TransactionId(2), B, RangeId(1)),
    ] {
        state = begin(state, id);
        state = apply(state, Action::ObservePoint(id, key));
        state = apply(state, Action::Write(id, key, range));
        state = persist(state, id, &[NodeId(0)]);
        state = apply(state, Action::Propose(id, Durability::Local));
    }
    state = apply(state, Action::CommitNext);
    state = apply(state, Action::CommitNext);
    assert_eq!(committed_version(&state, TransactionId(1)), 1);
    assert_eq!(committed_version(&state, TransactionId(2)), 2);
}

#[test]
fn transaction_observations_remain_pinned_to_the_begin_snapshot() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::ExternalInsert(A, RANGE));
    state = apply(state, Action::ObservePoint(id, A));
    state = apply(state, Action::ObserveRange(id, RANGE));
    assert_eq!(state.transactions[&id].point_observations[&A], 0);
    assert_eq!(state.transactions[&id].range_observations[&RANGE], 0);
}

#[test]
fn one_transaction_atomically_spans_tables_and_partitions() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    for (key, range) in [(A, RangeId(0)), (B, RangeId(1)), (C, RangeId(2))] {
        state = apply(state, Action::Write(id, key, range));
    }
    state = persist(state, id, &[NodeId(0), NodeId(1)]);
    state = propose_and_commit(state, id, Durability::Quorum);
    let version = committed_version(&state, id);
    assert!(
        state.transactions[&id]
            .writes
            .iter()
            .all(|key| state.visible_rows.get(&(*key, version)) == Some(&id))
    );
}

#[test]
fn one_conflicting_observation_aborts_the_entire_large_transaction() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::ObservePoint(id, A));
    for (key, range) in [(A, RangeId(0)), (B, RangeId(1)), (C, RangeId(2))] {
        state = apply(state, Action::Write(id, key, range));
    }
    state = apply(state, Action::ExternalInsert(A, RANGE));
    state = persist(state, id, &[NodeId(0)]);
    state = propose_and_commit(state, id, Durability::Local);
    assert_eq!(state.transactions[&id].status, TransactionStatus::Aborted);
    assert!(!state.visible_rows.values().any(|visible| visible == &id));
}

#[test]
fn range_insertion_phantom_aborts() {
    phantom_action_aborts(Action::ExternalInsert(A, RANGE));
}

#[test]
fn range_deletion_phantom_aborts() {
    phantom_action_aborts(Action::ExternalDelete(A, RANGE));
}

fn phantom_action_aborts(phantom: Action) {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::ObserveRange(id, RANGE));
    state = apply(state, Action::Write(id, B, RangeId(1)));
    state = apply(state, phantom);
    state = persist(state, id, &[NodeId(0)]);
    state = propose_and_commit(state, id, Durability::Local);
    assert_eq!(state.transactions[&id].status, TransactionStatus::Aborted);
}

#[test]
fn duplicate_certification_proposal_preserves_one_outcome() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::Write(id, A, RANGE));
    state = persist(state, id, &[NodeId(0)]);
    state = apply(state, Action::Propose(id, Durability::Local));
    let proposal = state.transactions[&id].status;
    state = apply(state, Action::Propose(id, Durability::Erasure));
    assert_eq!(state.transactions[&id].status, proposal);
    state = apply(state, Action::CommitNext);
    let outcome = state.transactions[&id].status;
    state = apply(state, Action::Propose(id, Durability::Local));
    assert_eq!(state.transactions[&id].status, outcome);
}

#[test]
fn coordinator_crash_before_proposal_leaves_bundle_invisible() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::Write(id, A, RANGE));
    state = persist(state, id, &[NodeId(0)]);
    state = apply(state, Action::CrashCoordinator(id));
    assert!(
        state
            .apply(Action::Propose(id, Durability::Local))
            .is_none()
    );
    assert!(state.prepared_bundles_are_invisible());
}

#[test]
fn coordinator_crash_after_proposal_does_not_prevent_commit() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::Write(id, A, RANGE));
    state = persist(state, id, &[NodeId(0)]);
    state = apply(state, Action::Propose(id, Durability::Local));
    state = apply(state, Action::CrashCoordinator(id));
    state = apply(state, Action::CommitNext);
    assert_eq!(committed_version(&state, id), 1);
}

#[test]
fn leader_crash_before_commit_allows_new_leader_to_finish_proposal() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::Write(id, A, RANGE));
    state = persist(state, id, &[NodeId(0)]);
    state = apply(state, Action::Propose(id, Durability::Local));
    state = apply(state, Action::FailNode(NodeId(0)));
    assert!(state.apply(Action::CommitNext).is_none());
    state = apply(state, Action::ElectLeader(NodeId(1)));
    state = apply(state, Action::CommitNext);
    assert_eq!(committed_version(&state, id), 1);
}

#[test]
fn leader_crash_after_commit_cannot_change_outcome() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::Write(id, A, RANGE));
    state = persist(state, id, &[NodeId(0)]);
    state = propose_and_commit(state, id, Durability::Local);
    let outcome = state.transactions[&id].status;
    state = apply(state, Action::FailNode(NodeId(0)));
    state = apply(state, Action::ElectLeader(NodeId(1)));
    assert_eq!(state.transactions[&id].status, outcome);
}

#[test]
fn bundle_may_be_missing_from_one_follower_without_partial_visibility() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::Write(id, A, RANGE));
    state = persist(state, id, &[NodeId(0), NodeId(1)]);
    state = propose_and_commit(state, id, Durability::Quorum);
    assert!(
        !state.transactions[&id]
            .bundle_holders
            .contains_key(&NodeId(2))
    );
    assert!(state.bundle_available(id));
    assert!(state.committed_writes_are_atomic());
}

#[test]
fn durable_holder_minority_failure_remains_recoverable() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::Write(id, A, RANGE));
    state = persist(state, id, &[NodeId(0), NodeId(1)]);
    state = propose_and_commit(state, id, Durability::Quorum);
    state = apply(state, Action::FailNode(NodeId(0)));
    assert!(state.bundle_available(id));
}

#[test]
fn every_cluster_quorum_intersects_every_other_quorum() {
    let quorums = [
        BTreeSet::from([NodeId(0), NodeId(1)]),
        BTreeSet::from([NodeId(0), NodeId(2)]),
        BTreeSet::from([NodeId(1), NodeId(2)]),
    ];
    for left in &quorums {
        for right in &quorums {
            assert!(!left.is_disjoint(right));
        }
    }
}

#[test]
fn erasure_durability_retains_reconstruction_threshold_after_one_failure() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::Write(id, A, RANGE));
    state = persist(state, id, &[NodeId(0), NodeId(1), NodeId(2)]);
    state = propose_and_commit(state, id, Durability::Erasure);
    state = apply(state, Action::FailNode(NodeId(1)));
    assert!(state.bundle_available(id));
    assert!(state.minority_failure_is_reconstructable());
}

#[test]
fn network_partition_and_leader_change_preserve_proposal_order() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::Write(id, A, RANGE));
    state = persist(state, id, &[NodeId(0), NodeId(1)]);
    state = apply(state, Action::Propose(id, Durability::Quorum));
    state = apply(state, Action::FailNode(NodeId(0)));
    state = apply(state, Action::ElectLeader(NodeId(2)));
    state = apply(state, Action::CommitNext);
    assert_eq!(committed_version(&state, id), 1);
}

#[test]
fn node_incarnation_replacement_fences_old_durability_receipt() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = persist(state, id, &[NodeId(0), NodeId(1)]);
    state = apply(state, Action::ReplaceNode(NodeId(1)));
    assert_eq!(state.valid_holder_count(id), Some(1));
    assert!(
        state
            .apply(Action::Propose(id, Durability::Quorum))
            .is_none()
    );
}

#[test]
fn local_durability_loss_is_explicitly_reported() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::Write(id, A, RANGE));
    state = persist(state, id, &[NodeId(0)]);
    state = propose_and_commit(state, id, Durability::Local);
    state = apply(state, Action::FailNode(NodeId(0)));
    assert!(!state.bundle_available(id));
    state = apply(state, Action::ReportDataLoss(id));
    assert!(state.transactions[&id].data_lost_reported);
}

#[test]
fn duplicate_repair_worker_execution_is_idempotent() {
    let id = TransactionId(1);
    let mut state = begin(MvccRaftState::default(), id);
    state = apply(state, Action::Write(id, A, RANGE));
    state = persist(state, id, &[NodeId(0)]);
    state = propose_and_commit(state, id, Durability::Local);
    state = apply(state, Action::Repair(id, NodeId(1)));
    state = apply(state, Action::Repair(id, NodeId(1)));
    assert_eq!(state.repair_runs.len(), 1);
    assert_eq!(state.transactions[&id].bundle_holders.len(), 2);
}

#[test]
fn garbage_collection_is_pinned_by_active_snapshot() {
    let id = TransactionId(1);
    let mut state = MvccRaftState::default();
    for node in 0..NODE_COUNT {
        state = apply(state, Action::ApplyCommitted(NodeId(node)));
    }
    state = begin(state, id);
    state = apply(state, Action::ExternalInsert(A, RANGE));
    state = apply(state, Action::GarbageCollect(1));
    assert_eq!(state.gc_watermark, 0);
}

#[test]
fn garbage_collection_is_pinned_by_lagging_replica() {
    let mut state = apply(MvccRaftState::default(), Action::ExternalInsert(A, RANGE));
    state = apply(state, Action::ApplyCommitted(NodeId(0)));
    state = apply(state, Action::ApplyCommitted(NodeId(1)));
    state = apply(state, Action::GarbageCollect(1));
    assert_eq!(state.nodes[&NodeId(2)].applied, 0);
    assert_eq!(state.gc_watermark, 0);
}

#[test]
fn bounded_stateright_model_preserves_protocol_invariants() {
    MvccRaftModel::small()
        .checker()
        .spawn_dfs()
        .join()
        .assert_properties();
}

proptest! {
    #[test]
    fn generated_action_sequences_preserve_core_invariants(
        choices in prop::collection::vec(0_u8..18, 0..80)
    ) {
        let mut state = MvccRaftState::default();
        for choice in choices {
            let id = TransactionId(choice % 3);
            let node = NodeId(choice % NODE_COUNT);
            let key = LogicalKey::new(choice % 3, (choice / 3) % 3, choice % 2);
            let range = RangeId(choice % 3);
            let action = match choice {
                0 => Action::Begin(id),
                1 => Action::ObservePoint(id, key),
                2 => Action::ObserveRange(id, range),
                3 => Action::Write(id, key, range),
                4 => Action::PersistBundle(id, node, state.nodes[&node].incarnation),
                5 => Action::Propose(id, Durability::Local),
                6 => Action::Propose(id, Durability::Quorum),
                7 => Action::Propose(id, Durability::Erasure),
                8 => Action::CommitNext,
                9 => Action::CrashCoordinator(id),
                10 => Action::FailNode(node),
                11 => Action::ReplaceNode(node),
                12 => Action::ElectLeader(node),
                13 => Action::ApplyCommitted(node),
                14 => Action::Repair(id, node),
                15 => Action::ExternalInsert(key, range),
                16 => Action::ExternalDelete(key, range),
                _ => Action::GarbageCollect(state.commit_version),
            };
            if let Some(next) = state.apply(action) {
                state = next;
                prop_assert!(state.outcomes_are_unique());
                prop_assert!(state.committed_writes_are_atomic());
                prop_assert!(state.prepared_bundles_are_invisible());
                prop_assert!(state.applied_watermarks_are_bounded());
                prop_assert!(state.durability_claims_are_honest());
                prop_assert!(state.gc_respects_readers());
            }
        }
    }
}
