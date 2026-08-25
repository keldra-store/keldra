//! Admission proof for indivisible source-journal transitions.

use super::*;

impl Store {
    /// Proves that one indivisible source-journal transition can fit an empty
    /// configured journal before an authority decision makes it mandatory.
    pub(crate) fn preflight_source_journal_transition(
        &self,
        changes: &[LocalChange],
    ) -> Result<(), MutationError> {
        let entries = u64::try_from(changes.len()).map_err(|_| {
            MutationError::Storage("source-journal transition count is exhausted".into())
        })?;
        let bytes = changes.iter().try_fold(0_u64, |total, change| {
            let encoded = encode_local_change(change).map_err(storage_error)?;
            let change_bytes = invalidation_record_bytes(encoded.len())
                .checked_add(super::journal_routes::journal_route_logical_bytes(change))
                .ok_or_else(|| {
                    MutationError::Storage("source-journal transition bytes are exhausted".into())
                })?;
            total.checked_add(change_bytes).ok_or_else(|| {
                MutationError::Storage("source-journal transition bytes are exhausted".into())
            })
        })?;
        if entries > self.watch_retention.max_entries || bytes > self.watch_retention.max_bytes {
            return Err(MutationError::SourceJournalTransitionTooLarge {
                entries,
                bytes,
                maximum_entries: self.watch_retention.max_entries,
                maximum_bytes: self.watch_retention.max_bytes,
            });
        }
        Ok(())
    }
}
