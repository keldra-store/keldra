use super::*;
use crate::{ReferenceDeltaApplied, ReferenceDeltaBatch, ReferenceDeltaError};

const REFERENCE_CURSOR_KEY_PREFIX: &[u8] = b"\x01reference_cursor/";

impl Store {
    /// Apply one destination-filtered, contiguous source prefix exactly once.
    ///
    /// The lifecycle changes and source cursor share one synchronous RocksDB
    /// batch. Callers must install the named bytes before sending a positive
    /// effect; this operation never creates payload bytes or a side record.
    pub async fn apply_reference_deltas(
        &self,
        request: ReferenceDeltaBatch,
    ) -> Result<ReferenceDeltaApplied, ReferenceDeltaError> {
        if request.through < request.after {
            return Err(ReferenceDeltaError::InvalidRange);
        }
        if request.deltas.iter().any(|delta| delta.change == 0) {
            return Err(ReferenceDeltaError::ZeroChange);
        }

        let _commit_guard = self.commit_lock.lock().await;
        let cursor_key = reference_cursor_key(request.source);
        let cursor = self.reference_delta_cursor_by_key(&cursor_key)?;
        if request.through <= cursor {
            return Ok(ReferenceDeltaApplied {
                through: cursor,
                replayed: true,
            });
        }
        if request.after < cursor {
            return Err(ReferenceDeltaError::PartialOverlap {
                cursor,
                after: request.after,
                through: request.through,
            });
        }
        if request.after > cursor {
            return Err(ReferenceDeltaError::Gap {
                expected: cursor,
                received: request.after,
            });
        }

        let now = now_unix_millis().map_err(ReferenceDeltaError::from)?;
        let mut states = PendingBlobReferences::new();
        for delta in request.deltas {
            let key = blob_reference_key(&delta.blob);
            let state = match states.get(&key).copied() {
                Some(state) => state,
                None => self
                    .read_blob_reference_state(&key)
                    .map_err(ReferenceDeltaError::from)?
                    .ok_or(ReferenceDeltaError::BlobNotFound)?,
            };
            let next = apply_reference_change(state, delta.change, now)?;
            states.insert(key, next);
        }

        let mut batch = WriteBatch::default();
        for (key, state) in states {
            batch.put_cf(
                self.cf(CF_BLOB_REFERENCES)
                    .map_err(ReferenceDeltaError::from)?,
                key,
                encode_blob_reference_state(state),
            );
        }
        batch.put_cf(
            self.cf(CF_METADATA).map_err(ReferenceDeltaError::from)?,
            cursor_key,
            request.through.to_be_bytes(),
        );
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .write_opt(batch, &options)
            .map_err(|error| ReferenceDeltaError::Storage(error.to_string()))?;

        Ok(ReferenceDeltaApplied {
            through: request.through,
            replayed: false,
        })
    }

    pub fn reference_delta_cursor(&self, source: SourceId) -> Result<u64, ReferenceDeltaError> {
        self.reference_delta_cursor_by_key(&reference_cursor_key(source))
    }

    fn reference_delta_cursor_by_key(&self, key: &[u8]) -> Result<u64, ReferenceDeltaError> {
        let encoded = self
            .db
            .get_cf(
                self.cf(CF_METADATA).map_err(ReferenceDeltaError::from)?,
                key,
            )
            .map_err(|error| ReferenceDeltaError::Storage(error.to_string()))?;
        let Some(encoded) = encoded else {
            return Ok(0);
        };
        let bytes = encoded.as_slice().try_into().map_err(|_| {
            ReferenceDeltaError::Storage("reference-delta cursor is malformed".into())
        })?;
        Ok(u64::from_be_bytes(bytes))
    }
}

fn reference_cursor_key(source: SourceId) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        REFERENCE_CURSOR_KEY_PREFIX.len() + size_of::<u16>() + source.source_epoch.len(),
    );
    key.extend_from_slice(REFERENCE_CURSOR_KEY_PREFIX);
    key.extend_from_slice(&source.node_id.to_be_bytes());
    key.extend_from_slice(&source.source_epoch);
    key
}

fn apply_reference_change(
    mut state: BlobReferenceState,
    change: i64,
    now_unix_millis: u64,
) -> Result<BlobReferenceState, ReferenceDeltaError> {
    validate_blob_reference_state(state).map_err(ReferenceDeltaError::from)?;
    if change > 0 {
        let increment = u64::try_from(change).map_err(|_| ReferenceDeltaError::Overflow)?;
        if state.flags & AWAITING_PUBLISH != 0 {
            state.flags &= !AWAITING_PUBLISH;
            state.ref_count = state
                .ref_count
                .checked_add(increment - 1)
                .ok_or(ReferenceDeltaError::Overflow)?;
        } else {
            state.ref_count = state
                .ref_count
                .checked_add(increment)
                .ok_or(ReferenceDeltaError::Overflow)?;
        }
    } else {
        if state.flags & AWAITING_PUBLISH != 0 {
            return Err(ReferenceDeltaError::Underflow);
        }
        let decrement = change.unsigned_abs();
        state.ref_count = state
            .ref_count
            .checked_sub(decrement)
            .ok_or(ReferenceDeltaError::Underflow)?;
    }
    state.updated_at = state.updated_at.max(now_unix_millis);
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ReferenceDelta, StoreOptions};

    fn source() -> SourceId {
        SourceId {
            node_id: 7,
            source_epoch: [9; 32],
        }
    }

    fn batch(after: u64, through: u64, blob: &BlobRef, change: i64) -> ReferenceDeltaBatch {
        ReferenceDeltaBatch {
            source: source(),
            after,
            through,
            deltas: vec![ReferenceDelta {
                blob: blob.clone(),
                change,
            }],
        }
    }

    #[tokio::test]
    async fn cursor_and_positive_effect_commit_once() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let blob = store.stage_blob(b"replicated bytes").await.unwrap();

        let applied = store
            .apply_reference_deltas(batch(0, 4, &blob, 1))
            .await
            .unwrap();
        assert_eq!(applied.through, 4);
        assert!(!applied.replayed);
        assert_eq!(store.reference_delta_cursor(source()).unwrap(), 4);
        let state = store.blob_reference_state(&blob).unwrap().unwrap();
        assert_eq!(state.ref_count, 1);
        assert_eq!(state.flags, 0);

        let replayed = store
            .apply_reference_deltas(batch(0, 4, &blob, 1))
            .await
            .unwrap();
        assert!(replayed.replayed);
        assert_eq!(store.blob_reference_state(&blob).unwrap().unwrap(), state);
    }

    #[tokio::test]
    async fn empty_destination_batch_advances_contiguous_source_prefix() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();

        let applied = store
            .apply_reference_deltas(ReferenceDeltaBatch {
                source: source(),
                after: 0,
                through: 19,
                deltas: Vec::new(),
            })
            .await
            .unwrap();

        assert_eq!(applied.through, 19);
        assert_eq!(store.reference_delta_cursor(source()).unwrap(), 19);
    }

    #[tokio::test]
    async fn gaps_partial_overlaps_and_underflow_fail_without_advancing() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let blob = store.stage_blob(b"replicated bytes").await.unwrap();
        store
            .apply_reference_deltas(batch(0, 4, &blob, 1))
            .await
            .unwrap();

        assert_eq!(
            store.apply_reference_deltas(batch(6, 7, &blob, 1)).await,
            Err(ReferenceDeltaError::Gap {
                expected: 4,
                received: 6,
            })
        );
        assert_eq!(
            store.apply_reference_deltas(batch(2, 7, &blob, 1)).await,
            Err(ReferenceDeltaError::PartialOverlap {
                cursor: 4,
                after: 2,
                through: 7,
            })
        );
        assert_eq!(
            store.apply_reference_deltas(batch(4, 5, &blob, -2)).await,
            Err(ReferenceDeltaError::Underflow)
        );
        assert_eq!(store.reference_delta_cursor(source()).unwrap(), 4);
        assert_eq!(
            store
                .blob_reference_state(&blob)
                .unwrap()
                .unwrap()
                .ref_count,
            1
        );
    }
}
