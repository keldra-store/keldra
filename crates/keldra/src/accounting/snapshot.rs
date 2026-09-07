use keldra_store::{AccountingHeadTransition, LocalChange, RetainedObjectSnapshot};
use tonic::Status;

use crate::index_runtime::events::IndexJournalPage;

use super::{LoadedAccountingDefinition, StoredAccountingRollup, includes_path};

/// Constant-memory aggregate state. A cold start/recovery scan reduces logical
/// retained bytes and live object count into these two scalars; ordered
/// transition evidence then advances them without retaining a path map.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AccountingObjectSnapshot {
    billable_logical_bytes: u64,
    retained_non_billable_logical_bytes: u64,
    visible_file_count: u64,
}

impl AccountingObjectSnapshot {
    pub(crate) fn from_rollup(rollup: &StoredAccountingRollup) -> Self {
        Self {
            billable_logical_bytes: rollup.billable_logical_bytes,
            retained_non_billable_logical_bytes: rollup.retained_non_billable_logical_bytes,
            visible_file_count: rollup.visible_file_count,
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
                    // Atomic publication may journal its protected physical
                    // descriptor and logical aliases on different source pages.
                    // Only a fresh snapshot after the atomic watermark can
                    // represent that logical-name set without exposing a
                    // private descriptor or a partial batch.
                    if head.program_commit_cursor.is_some() {
                        return Err(AccountingAdvanceError::TransitionEvidenceUnavailable);
                    }
                    apply_head_transition(
                        &mut next,
                        head.accounting_transition
                            .ok_or(AccountingAdvanceError::TransitionEvidenceUnavailable)?,
                        head.canonical_path.is_none(),
                    )?;
                    changed = true;
                }
                LocalChange::RetainedVersionDeleted(retained)
                    if retained.tenant_id == definition.tenant_id
                        && retained.bucket_id == definition.bucket_id
                        && includes_path(&definition.stored.path_prefix, &retained.exact_path) =>
                {
                    apply_head_transition(
                        &mut next,
                        retained
                            .accounting_transition
                            .ok_or(AccountingAdvanceError::TransitionEvidenceUnavailable)?,
                        false,
                    )?;
                    changed = true;
                }
                LocalChange::ContentLifecycleChanged(lifecycle)
                    if lifecycle
                        .accounting_transition
                        .as_ref()
                        .is_some_and(|transition| {
                            transition.tenant_id == definition.tenant_id
                                && transition.bucket_id == definition.bucket_id
                                && includes_path(
                                    &definition.stored.path_prefix,
                                    &transition.exact_path,
                                )
                        }) =>
                {
                    let removed = lifecycle
                        .accounting_transition
                        .as_ref()
                        .expect("matching transition was checked")
                        .retained_bytes_removed;
                    next.retained_non_billable_logical_bytes = next
                        .retained_non_billable_logical_bytes
                        .checked_sub(removed)
                        .ok_or(AccountingAdvanceError::Underflow)?;
                    changed = true;
                }
                _ => {}
            }
        }
        *self = next;
        Ok(changed)
    }

    pub(crate) const fn visible_file_count(self) -> u64 {
        self.visible_file_count
    }

    pub(crate) const fn billable_logical_bytes(self) -> u64 {
        self.billable_logical_bytes
    }

    pub(crate) const fn retained_non_billable_logical_bytes(self) -> u64 {
        self.retained_non_billable_logical_bytes
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
    previous_path: Option<(String, u32)>,
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
            if record.tenant_id != definition.tenant_id || record.bucket_id != definition.bucket_id
            {
                return Err(Status::data_loss(
                    "retained accounting snapshot escaped its requested scope",
                ));
            }
            if record.version.protected_link_descriptor {
                continue;
            }
            if !record.user_retained && record.version.id != record.current_head.version {
                if includes_path(&definition.stored.path_prefix, &record.exact_path)
                    && let Some(blob) = record.version.blob.as_ref()
                {
                    self.snapshot.retained_non_billable_logical_bytes = self
                        .snapshot
                        .retained_non_billable_logical_bytes
                        .checked_add(blob.length)
                        .ok_or_else(|| {
                            Status::resource_exhausted(
                                "accounting retained non-billable byte baseline overflow",
                            )
                        })?;
                }
                continue;
            }
            if let Some((previous_path, previous_count)) = self.previous_path.as_ref()
                && previous_path == &record.exact_path
                && *previous_count != record.scope_name_count
            {
                return Err(Status::data_loss(
                    "retained accounting snapshot changed one lineage's logical name count",
                ));
            }
            if self
                .previous_path
                .as_ref()
                .is_none_or(|(path, _)| path != &record.exact_path)
            {
                if !record.current_head.deleted {
                    self.snapshot.visible_file_count = self
                        .snapshot
                        .visible_file_count
                        .checked_add(u64::from(record.scope_name_count))
                        .ok_or_else(|| {
                            Status::resource_exhausted("accounting object baseline overflow")
                        })?;
                }
                self.previous_path = Some((record.exact_path.clone(), record.scope_name_count));
            }
            if let Some(blob) = record.version.blob.as_ref() {
                let logical_bytes = blob
                    .length
                    .checked_mul(u64::from(record.scope_name_count))
                    .ok_or_else(|| {
                        Status::resource_exhausted("accounting logical byte baseline overflow")
                    })?;
                self.snapshot.billable_logical_bytes = self
                    .snapshot
                    .billable_logical_bytes
                    .checked_add(logical_bytes)
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

fn apply_head_transition(
    snapshot: &mut AccountingObjectSnapshot,
    transition: AccountingHeadTransition,
    retain_removed_bytes: bool,
) -> Result<(), AccountingAdvanceError> {
    transition
        .validate()
        .map_err(|_| AccountingAdvanceError::TransitionEvidenceUnavailable)?;
    let billable_logical_bytes = i128::from(snapshot.billable_logical_bytes)
        .checked_add(i128::from(transition.current_live_length.unwrap_or(0)))
        .and_then(|total| total.checked_sub(i128::from(transition.logical_bytes_removed)))
        .ok_or(AccountingAdvanceError::Overflow)?;
    snapshot.billable_logical_bytes = u64::try_from(billable_logical_bytes).map_err(|_| {
        if billable_logical_bytes < 0 {
            AccountingAdvanceError::Underflow
        } else {
            AccountingAdvanceError::Overflow
        }
    })?;
    if retain_removed_bytes {
        snapshot.retained_non_billable_logical_bytes = snapshot
            .retained_non_billable_logical_bytes
            .checked_add(transition.logical_bytes_removed)
            .ok_or(AccountingAdvanceError::Overflow)?;
    }
    match (
        transition.previous_live_length.is_some(),
        transition.current_live_length.is_some(),
    ) {
        (false, true) => {
            snapshot.visible_file_count = snapshot
                .visible_file_count
                .checked_add(1)
                .ok_or(AccountingAdvanceError::Overflow)?;
        }
        (true, false) => {
            snapshot.visible_file_count = snapshot
                .visible_file_count
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
    use keldra_consensus::NodeId;
    use keldra_store::{
        BlobRef, ContentAccountingTransition, ContentLifecycleChanged, OBJECT_LINK_CONTENT_TYPE,
        ObjectHeadChange, ObjectHeadChangeKind, PlacementLogId, ReferenceDelta, RetainedHeadState,
        SourceId, Version, VersionId,
    };

    use super::*;
    use crate::index_runtime::events::{AtomicProgramWatermark, IndexBarrier, IndexJournalChange};

    fn definition() -> LoadedAccountingDefinition {
        LoadedAccountingDefinition {
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
        }
    }

    #[test]
    fn unversioned_overwrite_and_delete_apply_logical_bytes_immediately() {
        let mut snapshot = AccountingObjectSnapshot::default();
        apply_head_transition(
            &mut snapshot,
            AccountingHeadTransition::new(None, Some(8), 0),
            true,
        )
        .unwrap();
        apply_head_transition(
            &mut snapshot,
            AccountingHeadTransition::new(Some(8), Some(9), 8),
            true,
        )
        .unwrap();
        assert_eq!(snapshot.visible_file_count(), 1);
        assert_eq!(snapshot.billable_logical_bytes(), 9);
        assert_eq!(snapshot.retained_non_billable_logical_bytes(), 8);
        apply_head_transition(
            &mut snapshot,
            AccountingHeadTransition::new(Some(9), None, 9),
            true,
        )
        .unwrap();
        assert_eq!(snapshot.billable_logical_bytes(), 0);
        assert_eq!(snapshot.visible_file_count(), 0);
        assert_eq!(snapshot.retained_non_billable_logical_bytes(), 17);
    }

    #[test]
    fn versioned_overwrite_retains_bytes_until_explicit_version_deletion() {
        let mut snapshot = AccountingObjectSnapshot::default();
        apply_head_transition(
            &mut snapshot,
            AccountingHeadTransition::new(None, Some(8), 0),
            false,
        )
        .unwrap();
        apply_head_transition(
            &mut snapshot,
            AccountingHeadTransition::new(Some(8), Some(9), 0),
            false,
        )
        .unwrap();
        assert_eq!(snapshot.billable_logical_bytes(), 17);
        assert_eq!(snapshot.visible_file_count(), 1);
        apply_head_transition(
            &mut snapshot,
            AccountingHeadTransition::new(None, None, 8),
            false,
        )
        .unwrap();
        assert_eq!(snapshot.billable_logical_bytes(), 9);
        assert_eq!(snapshot.visible_file_count(), 1);
        assert_eq!(snapshot.retained_non_billable_logical_bytes(), 0);
    }

    #[test]
    fn delayed_content_lifecycle_retires_non_billable_logical_bytes_only() {
        let mut snapshot = AccountingObjectSnapshot {
            billable_logical_bytes: 8,
            retained_non_billable_logical_bytes: 8,
            visible_file_count: 1,
        };
        let page = IndexJournalPage {
            changes: vec![IndexJournalChange {
                node: NodeId(1),
                change: LocalChange::ContentLifecycleChanged(ContentLifecycleChanged {
                    offset: 1,
                    blob_identity: vec![1],
                    revision: 1,
                    reference_deltas: vec![ReferenceDelta {
                        blob: BlobRef {
                            hash: [1; 32],
                            length: 8,
                        },
                        change: -1,
                    }],
                    accounting_transition: Some(ContentAccountingTransition {
                        tenant_id: 11,
                        bucket_id: 12,
                        exact_path: "docs/a".into(),
                        retained_bytes_removed: 8,
                    }),
                }),
            }],
            through: IndexBarrier {
                fence: PlacementLogId { term: 1, index: 1 },
                atomic: AtomicProgramWatermark::new(None, None, 0),
                sources: std::collections::BTreeMap::from([(
                    NodeId(1),
                    crate::index_runtime::events::IndexSourceCursor {
                        source: SourceId {
                            node_id: 1,
                            source_epoch: [1; 32],
                        },
                        next_offset: 2,
                    },
                )]),
            },
            encoded_bytes: 1,
        };
        assert!(snapshot.apply(&definition(), &page).unwrap());
        assert_eq!(snapshot.billable_logical_bytes(), 8);
        assert_eq!(snapshot.retained_non_billable_logical_bytes(), 0);
        assert_eq!(snapshot.visible_file_count(), 1);
    }

    #[test]
    fn atomic_path_event_forces_a_baseline_before_accounting_publication() {
        let mut snapshot = AccountingObjectSnapshot {
            billable_logical_bytes: 8,
            retained_non_billable_logical_bytes: 0,
            visible_file_count: 1,
        };
        let before = snapshot;
        let page = IndexJournalPage {
            changes: vec![IndexJournalChange {
                node: NodeId(1),
                change: LocalChange::ObjectHead(ObjectHeadChange {
                    offset: 1,
                    tenant_id: 11,
                    bucket_id: 12,
                    exact_path: "docs/link".into(),
                    canonical_path: None,
                    path_version: VersionId(9),
                    kind: ObjectHeadChangeKind::Put,
                    program_commit_cursor: Some(7),
                    reference_deltas: Vec::new(),
                    accounting_transition: Some(AccountingHeadTransition::new(Some(8), Some(9), 8)),
                    definition_transition: None,
                }),
            }],
            through: IndexBarrier {
                fence: PlacementLogId { term: 1, index: 1 },
                atomic: AtomicProgramWatermark::new(Some(7), Some(7), 0),
                sources: std::collections::BTreeMap::from([(
                    NodeId(1),
                    crate::index_runtime::events::IndexSourceCursor {
                        source: SourceId {
                            node_id: 1,
                            source_epoch: [1; 32],
                        },
                        next_offset: 2,
                    },
                )]),
            },
            encoded_bytes: 1,
        };

        assert_eq!(
            snapshot.apply(&definition(), &page),
            Err(AccountingAdvanceError::TransitionEvidenceUnavailable)
        );
        assert_eq!(snapshot, before);
    }

    #[test]
    fn baseline_separates_billable_and_journal_pending_descriptors() {
        let record = |path: &str, version, current, length, user_retained| RetainedObjectSnapshot {
            tenant_id: 11,
            bucket_id: 12,
            exact_path: path.into(),
            version: Version {
                id: VersionId(version),
                blob: Some(BlobRef {
                    hash: [version as u8; 32],
                    length,
                }),
                content_type: None,
                deleted: false,
                committed_at_unix_millis: version,
                protected_link_descriptor: false,
            },
            current_head: RetainedHeadState {
                version: VersionId(current),
                deleted: false,
            },
            user_retained,
            scope_name_count: 1,
        };
        let mut baseline = AccountingBaselineAccumulator::default();
        baseline
            .apply_frame(&definition(), &[record("docs/a", 1, 3, 10, false)])
            .unwrap();
        baseline
            .apply_frame(
                &definition(),
                &[
                    record("docs/a", 2, 3, 20, false),
                    record("docs/a", 3, 3, 30, false),
                    record("docs/b", 4, 5, 40, true),
                ],
            )
            .unwrap();
        baseline
            .apply_frame(
                &definition(),
                &[
                    record("docs/b", 5, 5, 50, true),
                    record("docs/c", 6, 7, 60, false),
                ],
            )
            .unwrap();
        let snapshot = baseline.finish();
        assert_eq!(snapshot.visible_file_count(), 2);
        assert_eq!(snapshot.billable_logical_bytes(), 120);
        assert_eq!(snapshot.retained_non_billable_logical_bytes(), 90);
    }

    #[test]
    fn baseline_counts_full_retained_lineage_once_per_visible_name() {
        let record = |version, length, scope_name_count| RetainedObjectSnapshot {
            tenant_id: 11,
            bucket_id: 12,
            // The canonical target may be outside the definition prefix when
            // one of its aliases is inside it. The source-side scalar is the
            // authoritative scope join.
            exact_path: "targets/item".into(),
            version: Version {
                id: VersionId(version),
                blob: Some(BlobRef {
                    hash: [version as u8; 32],
                    length,
                }),
                content_type: None,
                deleted: false,
                committed_at_unix_millis: version,
                protected_link_descriptor: false,
            },
            current_head: RetainedHeadState {
                version: VersionId(2),
                deleted: false,
            },
            user_retained: true,
            scope_name_count,
        };

        let mut alias_only = AccountingBaselineAccumulator::default();
        alias_only
            .apply_frame(&definition(), &[record(1, 10, 1), record(2, 20, 1)])
            .unwrap();
        let alias_only = alias_only.finish();
        assert_eq!(alias_only.visible_file_count(), 1);
        assert_eq!(alias_only.billable_logical_bytes(), 30);

        let mut whole_bucket = AccountingBaselineAccumulator::default();
        whole_bucket
            .apply_frame(&definition(), &[record(1, 10, 2), record(2, 20, 2)])
            .unwrap();
        let whole_bucket = whole_bucket.finish();
        assert_eq!(whole_bucket.visible_file_count(), 2);
        assert_eq!(whole_bucket.billable_logical_bytes(), 60);
    }

    #[test]
    fn baseline_excludes_protected_link_descriptors() {
        let descriptor = RetainedObjectSnapshot {
            tenant_id: 11,
            bucket_id: 12,
            exact_path: "docs/link".into(),
            version: Version {
                id: VersionId(1),
                blob: Some(BlobRef {
                    hash: [1; 32],
                    length: 99,
                }),
                content_type: Some(OBJECT_LINK_CONTENT_TYPE.into()),
                deleted: false,
                committed_at_unix_millis: 1,
                protected_link_descriptor: true,
            },
            current_head: RetainedHeadState {
                version: VersionId(1),
                deleted: false,
            },
            user_retained: true,
            scope_name_count: 1,
        };
        let mut baseline = AccountingBaselineAccumulator::default();
        baseline.apply_frame(&definition(), &[descriptor]).unwrap();
        assert_eq!(baseline.finish(), AccountingObjectSnapshot::default());
    }
}
