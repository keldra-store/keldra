use anvil_store::{AccountingHeadTransition, LocalChange, ReferenceDelta};
use tonic::Status;

use crate::cluster_peer::IndexHeadScanScope;
use crate::index_runtime::events::IndexJournalBatch;
use crate::index_runtime::scanner::ClusterIndexScanner;

use super::{LoadedAccountingDefinition, includes_path};

/// Constant-memory aggregate state. A cold start/recovery scan reduces current
/// metadata directly into these two scalars; ordered transition evidence then
/// advances them without retaining a path map.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AccountingObjectSnapshot {
    logical_stored_bytes: u64,
    object_count: u64,
}

impl AccountingObjectSnapshot {
    pub(crate) async fn initial(
        definition: &LoadedAccountingDefinition,
        scanner: &ClusterIndexScanner,
    ) -> Result<Self, Status> {
        let heads = scanner
            .scan(IndexHeadScanScope::AccountingSourceObjects {
                tenant_id: definition.tenant_id,
                bucket_id: definition.bucket_id,
                path_prefix: definition.stored.path_prefix.clone(),
            })
            .await?;
        heads
            .into_iter()
            .try_fold(Self::default(), |mut total, head| {
                if !includes_path(&definition.stored.path_prefix, &head.exact_path) {
                    return Ok(total);
                }
                // Logical stored bytes include every retained live payload version,
                // not physical EC/replica overhead. Current tombstones contribute
                // no object and no bytes of their own.
                for version in head.versions {
                    if let Some(blob) = version.blob {
                        total.logical_stored_bytes = total
                            .logical_stored_bytes
                            .checked_add(blob.length)
                            .ok_or_else(|| {
                                Status::resource_exhausted(
                                    "accounting logical byte baseline overflow",
                                )
                            })?;
                    }
                }
                if !head.head.deleted {
                    total.object_count = total.object_count.checked_add(1).ok_or_else(|| {
                        Status::resource_exhausted("accounting object baseline overflow")
                    })?;
                }
                Ok(total)
            })
    }

    pub(crate) fn apply(
        &mut self,
        definition: &LoadedAccountingDefinition,
        batch: &IndexJournalBatch,
    ) -> Result<bool, AccountingAdvanceError> {
        let mut next = *self;
        let mut changed = false;
        for change in &batch.changes {
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
    use anvil_store::BlobRef;

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
}
