use proptest::prelude::*;
use stateright::{Checker, Model};

use super::*;

fn state() -> CoreStoreState {
    CoreStoreState::new(2)
}

fn apply(state: CoreStoreState, action: Action) -> CoreStoreState {
    state.apply(action).expect("model action should be enabled")
}

fn start_batch(state: CoreStoreState, batch: BatchId, root: RootKey) -> CoreStoreState {
    apply(state, Action::StartImplicitBatch(batch, root))
}

fn prepare_quorum(mut state: CoreStoreState, batch: BatchId) -> CoreStoreState {
    state = apply(state, Action::Prepare(batch, NodeId(0)));
    apply(state, Action::Prepare(batch, NodeId(1)))
}

fn certify_quorum(mut state: CoreStoreState, batch: BatchId) -> CoreStoreState {
    state = apply(state, Action::BuildCommitCertificate(batch));
    state = apply(state, Action::PersistCommitCertificate(batch, NodeId(0)));
    apply(state, Action::PersistCommitCertificate(batch, NodeId(1)))
}

fn publish_implicit(mut state: CoreStoreState, batch: BatchId, root: RootKey) -> CoreStoreState {
    state = start_batch(state, batch, root);
    state = prepare_quorum(state, batch);
    state = certify_quorum(state, batch);
    apply(state, Action::AttemptPublish(batch, OwnerFence(1)))
}

fn check_small_model(max_steps: u8) {
    CoreStoreModel {
        max_steps,
        roots: 2,
        batches: 2,
    }
    .checker()
    .spawn_dfs()
    .join()
    .assert_properties();
}

#[test]
fn model_root_generation_single_winner() {
    let mut state = state();
    state = start_batch(state, BatchId(0), RootKey(0));
    state = start_batch(state, BatchId(1), RootKey(0));

    for batch in [BatchId(0), BatchId(1)] {
        state = prepare_quorum(state, batch);
        state = certify_quorum(state, batch);
    }

    state = apply(state, Action::AttemptPublish(BatchId(0), OwnerFence(1)));
    state = apply(state, Action::AttemptPublish(BatchId(1), OwnerFence(1)));

    assert_eq!(state.latest_generation(RootKey(0)), Some(1));
    assert_eq!(
        state
            .published
            .keys()
            .filter(|(root, generation)| *root == RootKey(0) && *generation == 1)
            .count(),
        1
    );
    assert!(state.one_anchor_per_root_generation());
}

#[test]
fn model_root_generation_never_regresses() {
    check_small_model(7);
}

#[test]
fn model_commit_certificate_persisted_before_root() {
    let mut state = start_batch(state(), BatchId(0), RootKey(0));
    state = prepare_quorum(state, BatchId(0));
    state = apply(state, Action::BuildCommitCertificate(BatchId(0)));
    state = apply(state, Action::AttemptPublish(BatchId(0), OwnerFence(1)));
    assert_eq!(state.latest_generation(RootKey(0)), Some(0));

    state = apply(
        state,
        Action::PersistCommitCertificate(BatchId(0), NodeId(0)),
    );
    state = apply(state, Action::AttemptPublish(BatchId(0), OwnerFence(1)));
    assert_eq!(state.latest_generation(RootKey(0)), Some(0));

    state = apply(
        state,
        Action::PersistCommitCertificate(BatchId(0), NodeId(1)),
    );
    state = apply(state, Action::AttemptPublish(BatchId(0), OwnerFence(1)));
    assert_eq!(state.latest_generation(RootKey(0)), Some(1));
    assert!(state.visible_generations_have_persisted_certificates());
}

#[test]
fn model_quorum_prepare_required() {
    let mut state = start_batch(state(), BatchId(0), RootKey(0));
    state = apply(state, Action::Prepare(BatchId(0), NodeId(0)));

    assert!(
        state
            .apply(Action::BuildCommitCertificate(BatchId(0)))
            .is_none()
    );
    assert!(state.certificates_require_quorum_prepare());
}

#[test]
fn model_staged_rows_are_invisible() {
    let mut state = state();
    state = apply(state, Action::BeginTransaction(TxId(0), RootKey(0)));
    state = apply(state, Action::StageMutation(TxId(0), RootKey(0)));

    assert!(state.read_latest_committed(RootKey(0)).unwrap().is_empty());
    assert!(state.staged_rows_are_invisible());
}

#[test]
fn model_transaction_scope_mismatch_rejected() {
    let mut state = state();
    state = apply(state, Action::BeginTransaction(TxId(0), RootKey(0)));
    state = apply(state, Action::StageMutation(TxId(0), RootKey(1)));

    let tx = state.transactions.get(&TxId(0)).unwrap();
    assert_eq!(tx.status, TransactionStatus::ScopeMismatch);
    assert_eq!(state.rejected_scope_mismatches, 1);
    assert!(state.apply(Action::CommitTransaction(TxId(0))).is_none());
    assert_eq!(state.latest_generation(RootKey(0)), Some(0));
}

#[test]
fn model_read_committed_dirty_read_forbidden() {
    let mut state = state();
    state = apply(state, Action::BeginTransaction(TxId(0), RootKey(0)));
    state = apply(state, Action::StageMutation(TxId(0), RootKey(0)));
    let staged = state.transactions[&TxId(0)].staged_rows.clone();

    let latest = state.read_latest_committed(RootKey(0)).unwrap();
    assert!(staged.iter().all(|row| !latest.contains(row)));

    state = apply(state, Action::RollbackTransaction(TxId(0)));
    let latest_after_rollback = state.read_latest_committed(RootKey(0)).unwrap();
    assert!(
        staged
            .iter()
            .all(|row| !latest_after_rollback.contains(row))
    );
}

#[test]
fn model_read_committed_pinned_generation_repeatable() {
    let mut state = state();
    state = apply(state, Action::BeginTransaction(TxId(0), RootKey(0)));
    state = apply(state, Action::StageMutation(TxId(0), RootKey(0)));
    state = apply(state, Action::CommitTransaction(TxId(0)));
    let pinned_generation = state.latest_generation(RootKey(0)).unwrap();
    let pinned_read = state.read_at_generation(RootKey(0), pinned_generation);

    state = publish_implicit(state, BatchId(0), RootKey(0));
    assert_eq!(
        state.latest_generation(RootKey(0)),
        Some(pinned_generation + 1)
    );
    assert_eq!(
        state.read_at_generation(RootKey(0), pinned_generation),
        pinned_read
    );
    assert_ne!(
        state.read_latest_committed(RootKey(0)).unwrap(),
        pinned_read
    );
}

#[test]
fn model_committed_transaction_rows_stay_on_scoped_root() {
    let mut state = state();
    state = apply(state, Action::BeginTransaction(TxId(0), RootKey(0)));
    state = apply(state, Action::StageMutation(TxId(0), RootKey(0)));
    let staged = state.transactions[&TxId(0)].staged_rows.clone();

    state = apply(state, Action::CommitTransaction(TxId(0)));

    let root_zero_rows = state.read_latest_committed(RootKey(0)).unwrap();
    let root_one_rows = state.read_latest_committed(RootKey(1)).unwrap();
    assert!(staged.iter().all(|row| root_zero_rows.contains(row)));
    assert!(staged.iter().all(|row| !root_one_rows.contains(row)));
}

#[test]
fn model_byte_backed_batch_cannot_publish_without_shard_certificate() {
    let mut state = state();
    state = apply(state, Action::StartByteWrite(ByteWriteId(0)));
    state = apply(
        state,
        Action::StartByteBackedBatch(BatchId(0), RootKey(0), ByteWriteId(0)),
    );
    state = prepare_quorum(state, BatchId(0));
    state = certify_quorum(state, BatchId(0));

    state = apply(state, Action::AttemptPublish(BatchId(0), OwnerFence(1)));
    assert_eq!(state.latest_generation(RootKey(0)), Some(0));
    assert!(state.rejected_missing_byte_cert_commits > 0);
    assert!(state.published_batches_reference_durable_bytes());
}

#[test]
fn model_byte_backed_batch_publishes_after_all_shard_receipts() {
    let mut state = state();
    state = apply(state, Action::StartByteWrite(ByteWriteId(0)));
    for shard in 0..STORAGE_SHARD_COUNT {
        state = apply(state, Action::ShardFsync(ByteWriteId(0), ShardId(shard)));
    }
    state = apply(state, Action::BuildShardCertificate(ByteWriteId(0)));
    state = apply(
        state,
        Action::StartByteBackedBatch(BatchId(0), RootKey(0), ByteWriteId(0)),
    );
    state = prepare_quorum(state, BatchId(0));
    state = certify_quorum(state, BatchId(0));
    state = apply(state, Action::AttemptPublish(BatchId(0), OwnerFence(1)));

    assert_eq!(state.latest_generation(RootKey(0)), Some(1));
    assert!(state.published_batches_reference_durable_bytes());
}

#[test]
fn model_stale_owner_rejected_by_fence() {
    let mut state = start_batch(state(), BatchId(0), RootKey(0));
    state = prepare_quorum(state, BatchId(0));
    state = certify_quorum(state, BatchId(0));
    state = apply(state, Action::BumpOwnerFence(RootKey(0)));
    state = apply(state, Action::AttemptPublish(BatchId(0), OwnerFence(1)));

    assert_eq!(state.latest_generation(RootKey(0)), Some(0));
    assert!(state.rejected_stale_owner_publishes > 0);
    assert!(state.stale_owner_never_published());
}

#[test]
fn model_rollback_and_expiry_keep_rows_invisible() {
    let mut rolled_back = state();
    rolled_back = apply(rolled_back, Action::BeginTransaction(TxId(0), RootKey(0)));
    rolled_back = apply(rolled_back, Action::StageMutation(TxId(0), RootKey(0)));
    rolled_back = apply(rolled_back, Action::RollbackTransaction(TxId(0)));
    assert!(rolled_back.staged_rows_are_invisible());

    let mut expired = state();
    expired = apply(expired, Action::BeginTransaction(TxId(0), RootKey(0)));
    expired = apply(expired, Action::StageMutation(TxId(0), RootKey(0)));
    expired = apply(expired, Action::AdvanceClock);
    expired = apply(expired, Action::AdvanceClock);
    expired = apply(expired, Action::ExpireTransaction(TxId(0)));
    assert!(expired.apply(Action::CommitTransaction(TxId(0))).is_none());
    assert!(expired.staged_rows_are_invisible());
}

proptest! {
    #[test]
    fn generated_histories_preserve_core_invariants(actions in proptest::collection::vec(0u8..19, 0..24)) {
        let mut state = state();
        for action in actions {
            let candidate = match action {
                0 => Action::StartImplicitBatch(BatchId(0), RootKey(0)),
                1 => Action::Prepare(BatchId(0), NodeId(0)),
                2 => Action::Prepare(BatchId(0), NodeId(1)),
                3 => Action::BuildCommitCertificate(BatchId(0)),
                4 => Action::PersistCommitCertificate(BatchId(0), NodeId(0)),
                5 => Action::PersistCommitCertificate(BatchId(0), NodeId(1)),
                6 => Action::AttemptPublish(BatchId(0), OwnerFence(1)),
                7 => Action::BumpOwnerFence(RootKey(0)),
                8 => Action::BeginTransaction(TxId(0), RootKey(0)),
                9 => Action::StageMutation(TxId(0), RootKey(0)),
                10 => Action::StageMutation(TxId(0), RootKey(1)),
                11 => Action::CommitTransaction(TxId(0)),
                12 => Action::RollbackTransaction(TxId(0)),
                13 => Action::StartByteWrite(ByteWriteId(0)),
                14 => Action::ShardFsync(ByteWriteId(0), ShardId(0)),
                15 => Action::ShardFsync(ByteWriteId(0), ShardId(1)),
                16 => Action::ShardFsync(ByteWriteId(0), ShardId(2)),
                17 => Action::ShardFsync(ByteWriteId(0), ShardId(3)),
                18 => Action::BuildShardCertificate(ByteWriteId(0)),
                _ => Action::AdvanceClock,
            };
            if let Some(next) = state.apply(candidate) {
                state = next;
            }
            prop_assert!(state.root_generations_never_regress());
            prop_assert!(state.one_anchor_per_root_generation());
            prop_assert!(state.visible_generations_have_persisted_certificates());
            prop_assert!(state.certificates_require_quorum_prepare());
            prop_assert!(state.staged_rows_are_invisible());
            prop_assert!(state.published_batches_reference_durable_bytes());
        }
    }
}
