//! Source-journal transitions staged from one commit-fenced baseline.

use super::commit_lock::CommitLockGuard;
use super::journal_capacity::SourceJournalAdmission;
use super::*;

/// One baseline captured for immediate consumption under the guard supplied to
/// [`Store::fenced_local_journal_state`]. It is deliberately non-cloneable so
/// callers cannot reuse one snapshot for multiple transitions.
pub(super) struct FencedLocalJournalState {
    status: WatchJournalStatus,
    reference_cursor: u64,
}

pub(super) struct StagedLocalJournalTransition {
    status: WatchJournalStatus,
    reference_effects: LocalReferenceEffects,
    appended: bool,
}

impl FencedLocalJournalState {
    pub(super) fn status(&self) -> WatchJournalStatus {
        self.status
    }

    pub(super) fn reference_cursor(&self) -> u64 {
        self.reference_cursor
    }
}

impl Store {
    pub(super) fn fenced_local_journal_state(
        &self,
        _commit_guard: &CommitLockGuard<'_>,
    ) -> Result<FencedLocalJournalState, MutationError> {
        let status = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let reference_cursor = self
            .reference_delta_cursor(status.source_id)
            .map_err(|error| {
                MutationError::Storage(format!(
                    "cannot read local reference cursor before source-journal append: {error}"
                ))
            })?;
        validate_reference_cursor(status, reference_cursor)?;
        Ok(FencedLocalJournalState {
            status,
            reference_cursor,
        })
    }

    pub(super) fn stage_fenced_local_changes(
        &self,
        _commit_guard: &CommitLockGuard<'_>,
        batch: &mut WriteBatch,
        changes: &[PendingLocalChange],
        reference_effects: LocalReferenceEffects,
        admission: SourceJournalAdmission,
        baseline: FencedLocalJournalState,
    ) -> Result<StagedLocalJournalTransition, MutationError> {
        self.stage_local_changes_from_baseline(
            batch,
            changes,
            reference_effects,
            admission,
            baseline,
        )
    }

    pub(super) fn finish_fenced_local_changes_after_write(
        &self,
        _commit_guard: &CommitLockGuard<'_>,
        transition: StagedLocalJournalTransition,
        write_result: Result<(), MutationError>,
    ) -> Result<(), MutationError> {
        write_result?;
        if !transition.appended
            || transition.reference_effects != LocalReferenceEffects::AppliedInline
        {
            return Ok(());
        }
        let reference_safe = self
            .source_journal_reference_safe_through
            .load(std::sync::atomic::Ordering::Acquire);
        if reference_safe > transition.status.tail {
            return Err(MutationError::Storage(format!(
                "source journal reference-safe cursor {reference_safe} is beyond tail {}",
                transition.status.tail,
            )));
        }
        self.source_journal_reference_safe_through
            .store(transition.status.tail, std::sync::atomic::Ordering::Release);
        self.mutation_capacity_notify.notify_waiters();
        Ok(())
    }

    pub(crate) fn stage_local_changes(
        &self,
        batch: &mut WriteBatch,
        changes: &[PendingLocalChange],
        reference_effects: LocalReferenceEffects,
    ) -> Result<(), MutationError> {
        self.stage_local_changes_with_admission(
            batch,
            changes,
            reference_effects,
            SourceJournalAdmission::Bounded,
        )
    }

    pub(super) fn stage_local_changes_with_admission(
        &self,
        batch: &mut WriteBatch,
        changes: &[PendingLocalChange],
        reference_effects: LocalReferenceEffects,
        admission: SourceJournalAdmission,
    ) -> Result<(), MutationError> {
        if changes.is_empty() {
            return Ok(());
        }
        let status = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let reference_cursor = self
            .reference_delta_cursor(status.source_id)
            .map_err(|error| {
                MutationError::Storage(format!(
                    "cannot read local reference cursor before source-journal append: {error}"
                ))
            })?;
        validate_reference_cursor(status, reference_cursor)?;
        self.stage_local_changes_from_baseline(
            batch,
            changes,
            reference_effects,
            admission,
            FencedLocalJournalState {
                status,
                reference_cursor,
            },
        )?;
        Ok(())
    }

    fn stage_local_changes_from_baseline(
        &self,
        batch: &mut WriteBatch,
        changes: &[PendingLocalChange],
        reference_effects: LocalReferenceEffects,
        admission: SourceJournalAdmission,
        baseline: FencedLocalJournalState,
    ) -> Result<StagedLocalJournalTransition, MutationError> {
        let mut status = baseline.status;
        let local_source = SourceId {
            node_id: self.node_id,
            source_epoch: self.watch_source_epoch,
        };
        if status.source_id != local_source {
            return Err(MutationError::Storage(
                "fenced source-journal baseline belongs to another source".into(),
            ));
        }
        if changes.is_empty() {
            return Ok(StagedLocalJournalTransition {
                status,
                reference_effects,
                appended: false,
            });
        }

        let journal = self.cf(CF_LOCAL_INVALIDATIONS)?;
        let metadata = self.cf(CF_METADATA)?;
        let retained_entries_before = status.retained_entries;
        let retained_bytes_before = status.retained_bytes;
        if admission == SourceJournalAdmission::Bounded
            && (status.retained_entries > self.watch_retention.max_entries
                || status.retained_bytes > self.watch_retention.max_bytes)
        {
            return Err(MutationError::SourceJournalCapacity);
        }
        let old_tail = status.tail;
        let local_reference_cursor = match reference_effects {
            LocalReferenceEffects::AppliedInline => {
                if baseline.reference_cursor != old_tail {
                    return Err(MutationError::Storage(format!(
                        "local reference cursor {} does not match source-journal tail {old_tail}",
                        baseline.reference_cursor,
                    )));
                }
                Some(status.source_id)
            }
            LocalReferenceEffects::NoReferenceEffects => {
                if changes
                    .iter()
                    .any(PendingLocalChange::has_reference_effects)
                {
                    return Err(MutationError::Storage(
                        "source-journal append declared no reference effects but carried a reference delta"
                            .into(),
                    ));
                }
                (baseline.reference_cursor == old_tail).then_some(status.source_id)
            }
            LocalReferenceEffects::Deferred => None,
        };
        let mut appended = VecDeque::new();
        for pending in changes {
            status.tail = status.tail.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation offset is exhausted".into())
            })?;
            let change = pending.at_offset(status.tail);
            let encoded = encode_local_change(&change).map_err(storage_error)?;
            let logical_bytes = invalidation_record_bytes(encoded.len())
                .saturating_add(super::journal_routes::journal_route_logical_bytes(&change));
            if admission == SourceJournalAdmission::Bounded
                && logical_bytes > self.watch_retention.max_bytes
            {
                return Err(MutationError::SourceJournalRecordTooLarge {
                    bytes: logical_bytes,
                    maximum: self.watch_retention.max_bytes,
                });
            }
            self.stage_journal_routes(batch, status.source_id.source_epoch, &change)?;
            status.retained_entries = status.retained_entries.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation entry count is exhausted".into())
            })?;
            status.retained_bytes = status
                .retained_bytes
                .checked_add(logical_bytes)
                .ok_or_else(|| {
                    MutationError::Storage("local invalidation byte count is exhausted".into())
                })?;
            appended.push_back((status.tail, encoded));
        }

        if admission == SourceJournalAdmission::Bounded {
            let appended_entries = status
                .retained_entries
                .saturating_sub(retained_entries_before);
            let appended_bytes = status.retained_bytes.saturating_sub(retained_bytes_before);
            if appended_entries > self.watch_retention.max_entries
                || appended_bytes > self.watch_retention.max_bytes
            {
                return Err(MutationError::SourceJournalTransitionTooLarge {
                    entries: appended_entries,
                    bytes: appended_bytes,
                    maximum_entries: self.watch_retention.max_entries,
                    maximum_bytes: self.watch_retention.max_bytes,
                });
            }
            if status.retained_entries > self.watch_retention.max_entries
                || status.retained_bytes > self.watch_retention.max_bytes
            {
                return Err(MutationError::SourceJournalCapacity);
            }
        }
        for (offset, encoded) in appended {
            batch.put_cf(journal, invalidation_key(offset), encoded);
        }
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_OFFSET_KEY,
            status.tail.to_be_bytes(),
        );
        if reference_effects != LocalReferenceEffects::Deferred
            && status.settled_through == old_tail
        {
            status.settled_through = status.tail;
            batch.put_cf(
                metadata,
                LOCAL_INVALIDATION_SETTLED_KEY,
                status.tail.to_be_bytes(),
            );
        }
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_FLOOR_KEY,
            status.retention_floor.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_COUNT_KEY,
            status.retained_entries.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_BYTES_KEY,
            status.retained_bytes.to_be_bytes(),
        );
        if let Some(source) = local_reference_cursor {
            self.stage_reference_delta_cursor(batch, source, status.tail)?;
        }
        Ok(StagedLocalJournalTransition {
            status,
            reference_effects,
            appended: true,
        })
    }
}

fn validate_reference_cursor(
    status: WatchJournalStatus,
    reference_cursor: u64,
) -> Result<(), MutationError> {
    if reference_cursor > status.tail {
        return Err(MutationError::Storage(format!(
            "local reference cursor {reference_cursor} is ahead of source-journal tail {}",
            status.tail,
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aggregate_change(revision: u64) -> PendingLocalChange {
        PendingLocalChange::AggregateChanged {
            aggregate_kind: AggregateKind::LogicalRecord,
            aggregate_key: revision.to_be_bytes().to_vec(),
            revision,
        }
    }

    fn write_transition(
        store: &Store,
        commit_guard: &CommitLockGuard<'_>,
        baseline: FencedLocalJournalState,
        reference_effects: LocalReferenceEffects,
        revision: u64,
    ) -> Result<(), MutationError> {
        let mut batch = WriteBatch::default();
        let transition = store.stage_fenced_local_changes(
            commit_guard,
            &mut batch,
            &[aggregate_change(revision)],
            reference_effects,
            SourceJournalAdmission::Bounded,
            baseline,
        )?;
        let mut options = WriteOptions::default();
        options.set_sync(store.sync_writes);
        let result = store.db.write_opt(batch, &options).map_err(storage_error);
        store.finish_fenced_local_changes_after_write(commit_guard, transition, result)
    }

    #[tokio::test]
    async fn failed_write_cannot_advance_inline_reference_safety() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let commit_guard = store.lock_commit("journal_transition_test").await;
        let status = store.local_watch_status().unwrap();
        let transition = StagedLocalJournalTransition {
            status: WatchJournalStatus {
                tail: status.tail + 1,
                settled_through: status.tail + 1,
                retained_entries: status.retained_entries + 1,
                ..status
            },
            reference_effects: LocalReferenceEffects::AppliedInline,
            appended: true,
        };

        let failure = MutationError::Storage("injected write failure".into());
        assert_eq!(
            store.finish_fenced_local_changes_after_write(
                &commit_guard,
                transition,
                Err(failure.clone()),
            ),
            Err(failure)
        );
        assert_eq!(
            store
                .source_journal_reference_safe_through
                .load(std::sync::atomic::Ordering::Acquire),
            status.retention_floor
        );
        assert_eq!(store.local_watch_status().unwrap(), status);
    }

    #[tokio::test]
    async fn caught_up_reference_cursor_does_not_close_an_existing_settlement_gap() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();

        let commit_guard = store.lock_commit("journal_transition_test").await;
        let baseline = store.fenced_local_journal_state(&commit_guard).unwrap();
        write_transition(
            &store,
            &commit_guard,
            baseline,
            LocalReferenceEffects::Deferred,
            1,
        )
        .unwrap();
        let deferred = store.local_watch_status().unwrap();
        assert_eq!((deferred.tail, deferred.settled_through), (1, 0));

        let mut catch_up = WriteBatch::default();
        store
            .stage_reference_delta_cursor(&mut catch_up, deferred.source_id, deferred.tail)
            .unwrap();
        store.db.write(catch_up).unwrap();
        let baseline = store.fenced_local_journal_state(&commit_guard).unwrap();
        assert_eq!(baseline.reference_cursor(), baseline.status().tail);
        write_transition(
            &store,
            &commit_guard,
            baseline,
            LocalReferenceEffects::AppliedInline,
            2,
        )
        .unwrap();

        let status = store.local_watch_status().unwrap();
        assert_eq!((status.tail, status.settled_through), (2, 0));
        assert_eq!(
            store.reference_delta_cursor(status.source_id).unwrap(),
            status.tail
        );
        assert_eq!(
            store
                .source_journal_reference_safe_through
                .load(std::sync::atomic::Ordering::Acquire),
            status.tail
        );
    }

    #[tokio::test]
    async fn lagging_reference_cursor_keeps_a_settled_baseline_deferred() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();

        let commit_guard = store.lock_commit("journal_transition_test").await;
        let baseline = store.fenced_local_journal_state(&commit_guard).unwrap();
        write_transition(
            &store,
            &commit_guard,
            baseline,
            LocalReferenceEffects::NoReferenceEffects,
            1,
        )
        .unwrap();
        let settled = store.local_watch_status().unwrap();
        assert_eq!((settled.tail, settled.settled_through), (1, 1));

        let mut lag = WriteBatch::default();
        store
            .stage_reference_delta_cursor(&mut lag, settled.source_id, 0)
            .unwrap();
        store.db.write(lag).unwrap();
        let baseline = store.fenced_local_journal_state(&commit_guard).unwrap();
        assert!(baseline.reference_cursor() < baseline.status().tail);
        write_transition(
            &store,
            &commit_guard,
            baseline,
            LocalReferenceEffects::Deferred,
            2,
        )
        .unwrap();

        let status = store.local_watch_status().unwrap();
        assert_eq!((status.tail, status.settled_through), (2, 1));
        assert_eq!(store.reference_delta_cursor(status.source_id).unwrap(), 0);
        assert_eq!(
            store
                .source_journal_reference_safe_through
                .load(std::sync::atomic::Ordering::Acquire),
            settled.retention_floor
        );
    }
}
