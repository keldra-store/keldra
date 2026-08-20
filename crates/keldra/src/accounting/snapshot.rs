use keldra_store::{AccountingHeadTransition, LocalChange, ReferenceDelta, RetainedObjectSnapshot};
use tonic::Status;

use crate::index_runtime::events::IndexJournalPage;

use super::{LoadedAccountingDefinition, StoredAccountingRollup, includes_path};

/// Constant-memory aggregate state. A cold start/recovery scan reduces current
/// metadata directly into these two scalars; ordered transition evidence then
/// advances them without retaining a path map.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AccountingObjectSnapshot {
    logical_stored_bytes: u64,
    object_count: u64,
}

impl AccountingObjectSnapshot {
    pub(crate) fn from_rollup(rollup: &StoredAccountingRollup) -> Self {
        Self {
            logical_stored_bytes: rollup.logical_stored_bytes,
            object_count: rollup.object_count,
        }
    }

    pub(crate) fn apply(
        &mut self,
        definition: &LoadedAccountingDefinition,
        page: &IndexJournalPage,
    ) -> Result<bool, AccountingAdvanceError> {
        let mut next = *self;
        let mut changed = false;
        for change in &page.changes {
            match &change.change {
                LocalChange::ObjectHead(head)
                    if head.tenant_id == definition.tenant_id
                        && head.bucket_id == definition.bucket_id
                        && includes_path(&definition.stored.path_prefix, &head.exact_path) =>
                {
                    apply_reference_deltas(&mut next.logical_stored_bytes, &head.reference_deltas)?;
                    apply_head_transition(
                        &mut next.object_count,
                        head.accounting_transition
                            .ok_or(AccountingAdvanceError::TransitionEvidenceUnavailable)?,
                    )?;
                    changed = true;
                }
                LocalChange::RetainedVersionDeleted(retained)
                    if retained.tenant_id == definition.tenant_id
                        && retained.bucket_id == definition.bucket_id
                        && includes_path(&definition.stored.path_prefix, &retained.exact_path) =>
                {
                    apply_reference_deltas(
                        &mut next.logical_stored_bytes,
                        &retained.reference_deltas,
                    )?;
                    apply_head_transition(
                        &mut next.object_count,
                        retained
                            .accounting_transition
                            .ok_or(AccountingAdvanceError::TransitionEvidenceUnavailable)?,
                    )?;
                    changed = true;
                }
                _ => {}
            }
        }
        *self = next;
        Ok(changed)
    }

    pub(crate) const fn object_count(self) -> u64 {
        self.object_count
    }

    pub(crate) const fn logical_stored_bytes(self) -> u64 {
        self.logical_stored_bytes
    }
}

/// Constant-memory reducer for the snapshot-bound retained-version stream.
///
/// The stream repeats current-head state for every retained version and keeps
/// each source in `(path, version)` order. Remembering only the preceding path
/// is therefore enough to count a live object once while still accounting for
/// every retained payload version, including a path whose history spans many
/// frames.
#[derive(Debug, Default)]
pub(crate) struct AccountingBaselineAccumulator {
    snapshot: AccountingObjectSnapshot,
    previous_path: Option<String>,
}

impl AccountingBaselineAccumulator {
    pub(crate) fn apply_frame(
        &mut self,
        definition: &LoadedAccountingDefinition,
        records: &[RetainedObjectSnapshot],
    ) -> Result<(), Status> {
        for record in records {
            record
                .validate()
                .map_err(|error| Status::data_loss(error.to_string()))?;
            if record.tenant_id != definition.tenant_id
                || record.bucket_id != definition.bucket_id
                || !includes_path(&definition.stored.path_prefix, &record.exact_path)
            {
                return Err(Status::data_loss(
                    "retained accounting snapshot escaped its requested scope",
                ));
            }
            if self.previous_path.as_deref() != Some(record.exact_path.as_str()) {
                if !record.current_head.deleted {
                    self.snapshot.object_count =
                        self.snapshot.object_count.checked_add(1).ok_or_else(|| {
                            Status::resource_exhausted("accounting object baseline overflow")
                        })?;
                }
                self.previous_path = Some(record.exact_path.clone());
            }
            if let Some(blob) = record.version.blob.as_ref() {
                self.snapshot.logical_stored_bytes = self
                    .snapshot
                    .logical_stored_bytes
                    .checked_add(blob.length)
                    .ok_or_else(|| {
                        Status::resource_exhausted("accounting logical byte baseline overflow")
                    })?;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> AccountingObjectSnapshot {
        self.snapshot
    }
}

fn apply_reference_deltas(
    total: &mut u64,
    deltas: &[ReferenceDelta],
) -> Result<(), AccountingAdvanceError> {
    let mut next = i128::from(*total);
    for delta in deltas {
        next = next
            .checked_add(i128::from(delta.blob.length) * i128::from(delta.change))
            .ok_or(AccountingAdvanceError::Overflow)?;
    }
    *total = u64::try_from(next).map_err(|_| AccountingAdvanceError::Underflow)?;
    Ok(())
}

fn apply_head_transition(
    object_count: &mut u64,
    transition: AccountingHeadTransition,
) -> Result<(), AccountingAdvanceError> {
    transition
        .validate()
        .map_err(|_| AccountingAdvanceError::TransitionEvidenceUnavailable)?;
    match (
        transition.previous_live_length.is_some(),
        transition.current_live_length.is_some(),
    ) {
        (false, true) => {
            *object_count = object_count
                .checked_add(1)
                .ok_or(AccountingAdvanceError::Overflow)?;
        }
        (true, false) => {
            *object_count = object_count
                .checked_sub(1)
                .ok_or(AccountingAdvanceError::Underflow)?;
        }
        (false, false) | (true, true) => {}
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum AccountingAdvanceError {
    #[error("accounting journal entry predates compact head-transition evidence")]
    TransitionEvidenceUnavailable,
    #[error("accounting aggregate overflow")]
    Overflow,
    #[error("accounting aggregate underflow")]
    Underflow,
}

#[cfg(test)]
mod tests {
    use keldra_store::{BlobRef, RetainedHeadState, Version, VersionId};

    use super::*;

    #[test]
    fn transition_updates_live_count_without_per_path_state() {
        let mut count = 4;
        apply_head_transition(&mut count, AccountingHeadTransition::new(None, Some(8))).unwrap();
        assert_eq!(count, 5);
        apply_head_transition(&mut count, AccountingHeadTransition::new(Some(8), Some(9))).unwrap();
        assert_eq!(count, 5);
        apply_head_transition(&mut count, AccountingHeadTransition::new(Some(9), None)).unwrap();
        assert_eq!(count, 4);
    }

    #[test]
    fn retained_logical_bytes_follow_reference_deltas() {
        let mut bytes = 50;
        apply_reference_deltas(
            &mut bytes,
            &[
                ReferenceDelta {
                    blob: BlobRef {
                        hash: [1; 32],
                        length: 30,
                    },
                    change: 1,
                },
                ReferenceDelta {
                    blob: BlobRef {
                        hash: [2; 32],
                        length: 10,
                    },
                    change: -1,
                },
            ],
        )
        .unwrap();
        assert_eq!(bytes, 70);
    }

    #[test]
    fn retained_history_spanning_frames_counts_one_live_object() {
        let definition = LoadedAccountingDefinition {
            tenant_id: 11,
            bucket_id: 12,
            version: VersionId(4),
            stored: super::super::StoredAccountingDefinition::create(
                "tenant".into(),
                "bucket".into(),
                "docs".into(),
                11,
                12,
            )
            .unwrap(),
        };
        let record = |version, length| RetainedObjectSnapshot {
            tenant_id: 11,
            bucket_id: 12,
            exact_path: "docs/a".into(),
            version: Version {
                id: VersionId(version),
                blob: Some(BlobRef {
                    hash: [version as u8; 32],
                    length,
                }),
                content_type: None,
                deleted: false,
                committed_at_unix_millis: version,
            },
            current_head: RetainedHeadState {
                version: VersionId(3),
                deleted: false,
            },
        };
        let mut baseline = AccountingBaselineAccumulator::default();
        baseline.apply_frame(&definition, &[record(1, 10)]).unwrap();
        baseline
            .apply_frame(&definition, &[record(2, 20), record(3, 30)])
            .unwrap();
        let snapshot = baseline.finish();
        assert_eq!(snapshot.object_count(), 1);
        assert_eq!(snapshot.logical_stored_bytes(), 60);
    }
}
