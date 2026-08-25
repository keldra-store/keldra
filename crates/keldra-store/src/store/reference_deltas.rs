use super::*;
use crate::{
    DestinationReferenceArtifact, FRAGMENT_FORMAT_VERSION, ReferenceDeltaApplied,
    ReferenceDeltaBatch, ReferenceDeltaError,
};

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
        let deltas = request
            .deltas
            .into_iter()
            .map(|delta| {
                let key = artifact_lifecycle_key(&delta.artifact)?;
                Ok((delta.artifact, key, delta.change))
            })
            .collect::<Result<Vec<_>, ReferenceDeltaError>>()?;

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
        let mut verified_artifacts = BTreeSet::new();
        for (artifact, key, change) in deltas {
            let state = match states.get(&key).copied() {
                Some(state) => state,
                None => self
                    .read_blob_reference_state(&key)
                    .map_err(ReferenceDeltaError::from)?
                    .ok_or(ReferenceDeltaError::ArtifactNotFound)?,
            };
            if change > 0 && verified_artifacts.insert(key.clone()) {
                let exists = match &artifact {
                    DestinationReferenceArtifact::CompleteBlob(reference) => self
                        .contains_blob(reference)
                        .await
                        .map_err(ReferenceDeltaError::from)?,
                    DestinationReferenceArtifact::Shard(identity) => self
                        .contains_shard_artifact(identity)
                        .map_err(|error| ReferenceDeltaError::Storage(error.to_string()))?,
                };
                if !exists {
                    return Err(ReferenceDeltaError::ArtifactNotFound);
                }
            }
            let next = apply_reference_change(state, change, now)?;
            states.insert(key, next);
        }

        let mut batch = WriteBatch::default();
        let mut lifecycle_changes = Vec::with_capacity(states.len());
        let mut staged_states = PendingBlobReferences::new();
        for (key, state) in states {
            self.stage_blob_reference_update(&mut batch, &mut staged_states, key.clone(), state)
                .map_err(ReferenceDeltaError::from)?;
            lifecycle_changes.push(PendingLocalChange::ContentLifecycleChanged {
                blob_identity: key,
                revision: state.updated_at,
                // These are destination lifecycle invalidations. Re-emitting
                // the source reference effects would apply them twice.
                reference_deltas: Vec::new(),
                accounting_transition: None,
            });
        }
        batch.put_cf(
            self.cf(CF_METADATA).map_err(ReferenceDeltaError::from)?,
            cursor_key,
            request.through.to_be_bytes(),
        );
        self.stage_local_changes(
            &mut batch,
            &lifecycle_changes,
            LocalReferenceEffects::NoReferenceEffects,
        )
        .map_err(ReferenceDeltaError::from)?;
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .write_opt(batch, &options)
            .map_err(|error| ReferenceDeltaError::Storage(error.to_string()))?;
        if !lifecycle_changes.is_empty() {
            self.notify_local_invalidations();
        }

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

    pub(crate) fn stage_reference_delta_cursor(
        &self,
        batch: &mut WriteBatch,
        source: SourceId,
        through: u64,
    ) -> Result<(), MutationError> {
        batch.put_cf(
            self.cf(CF_METADATA)?,
            reference_cursor_key(source),
            through.to_be_bytes(),
        );
        Ok(())
    }
}

fn artifact_lifecycle_key(
    artifact: &DestinationReferenceArtifact,
) -> Result<Vec<u8>, ReferenceDeltaError> {
    match artifact {
        DestinationReferenceArtifact::CompleteBlob(reference) => Ok(blob_reference_key(reference)),
        DestinationReferenceArtifact::Shard(identity) => {
            if identity.fragment_format_version() != FRAGMENT_FORMAT_VERSION {
                return Err(ReferenceDeltaError::InvalidArtifact);
            }
            Ok(identity.encode().to_vec())
        }
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
    use std::io::Cursor;

    use super::*;
    use crate::{
        DestinationReferenceDelta, ErasureCodec, ErasureProfile, ShardIdentity, StoreOptions,
    };

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
            deltas: vec![DestinationReferenceDelta {
                artifact: DestinationReferenceArtifact::CompleteBlob(blob.clone()),
                change,
            }],
        }
    }

    fn encoded_shards(bytes: &[u8]) -> (ErasureCodec, BlobRef, Vec<Vec<u8>>) {
        let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
        let reference = BlobRef {
            hash: *blake3::hash(bytes).as_bytes(),
            length: bytes.len() as u64,
        };
        let mut shards = (0..codec.profile().total_shards())
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        codec
            .encode(Cursor::new(bytes), &reference, &mut shards)
            .unwrap();
        (codec, reference, shards)
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
        let changes = store.scan_local_changes(0, 10).unwrap();
        let Some(LocalChange::ContentLifecycleChanged(lifecycle)) = changes.last() else {
            panic!("reference application must append a lifecycle invalidation")
        };
        assert_eq!(lifecycle.blob_identity, blob_reference_key(&blob));
        assert!(lifecycle.reference_deltas.is_empty());

        let replayed = store
            .apply_reference_deltas(batch(0, 4, &blob, 1))
            .await
            .unwrap();
        assert!(replayed.replayed);
        assert_eq!(store.blob_reference_state(&blob).unwrap().unwrap(), state);
        assert_eq!(store.scan_local_changes(0, 10).unwrap(), changes);
    }

    #[tokio::test]
    async fn multiple_shard_ordinals_update_their_exact_lifecycle_records() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let bytes = vec![0x61; SMALL_BLOB_MAX_BYTES + 31];
        let (codec, reference, shards) = encoded_shards(&bytes);
        let first = ShardIdentity::new(reference.clone(), 0);
        let second = ShardIdentity::new(reference, 1);
        store
            .seal_shard(&codec, &first, Cursor::new(&shards[0]))
            .await
            .unwrap();
        store
            .seal_shard(&codec, &second, Cursor::new(&shards[1]))
            .await
            .unwrap();

        store
            .apply_reference_deltas(ReferenceDeltaBatch {
                source: source(),
                after: 0,
                through: 1,
                deltas: vec![
                    DestinationReferenceDelta {
                        artifact: DestinationReferenceArtifact::Shard(first.clone()),
                        change: 1,
                    },
                    DestinationReferenceDelta {
                        artifact: DestinationReferenceArtifact::Shard(second.clone()),
                        change: 1,
                    },
                ],
            })
            .await
            .unwrap();

        for identity in [&first, &second] {
            let state = store.shard_reference_state(identity).unwrap().unwrap();
            assert_eq!(state.ref_count, 1);
            assert_eq!(state.flags, 0);
        }
    }

    #[tokio::test]
    async fn complete_copy_and_shard_commit_in_one_mixed_batch() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let small = store.stage_blob(b"small complete copy").await.unwrap();
        let bytes = vec![0x42; SMALL_BLOB_MAX_BYTES + 47];
        let (codec, reference, shards) = encoded_shards(&bytes);
        let shard = ShardIdentity::new(reference, 2);
        store
            .seal_shard(&codec, &shard, Cursor::new(&shards[2]))
            .await
            .unwrap();

        store
            .apply_reference_deltas(ReferenceDeltaBatch {
                source: source(),
                after: 0,
                through: 8,
                deltas: vec![
                    DestinationReferenceDelta {
                        artifact: DestinationReferenceArtifact::CompleteBlob(small.clone()),
                        change: 1,
                    },
                    DestinationReferenceDelta {
                        artifact: DestinationReferenceArtifact::Shard(shard.clone()),
                        change: 1,
                    },
                ],
            })
            .await
            .unwrap();

        let small_state = store.blob_reference_state(&small).unwrap().unwrap();
        let shard_state = store.shard_reference_state(&shard).unwrap().unwrap();
        assert_eq!((small_state.ref_count, small_state.flags), (1, 0));
        assert_eq!((shard_state.ref_count, shard_state.flags), (1, 0));
        assert_eq!(store.reference_delta_cursor(source()).unwrap(), 8);
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

    #[tokio::test]
    async fn cursor_and_artifact_effects_survive_restart() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("store");
        let blob;
        {
            let store = Store::open(StoreOptions::new(&root, 1)).await.unwrap();
            blob = store.stage_blob(b"restart-safe copy").await.unwrap();
            store
                .apply_reference_deltas(batch(0, 4, &blob, 1))
                .await
                .unwrap();
        }

        {
            let store = Store::open(StoreOptions::new(&root, 1)).await.unwrap();
            let replayed = store
                .apply_reference_deltas(batch(0, 4, &blob, 1))
                .await
                .unwrap();
            assert!(replayed.replayed);
            assert_eq!(
                store
                    .blob_reference_state(&blob)
                    .unwrap()
                    .unwrap()
                    .ref_count,
                1
            );
            store
                .apply_reference_deltas(batch(4, 5, &blob, -1))
                .await
                .unwrap();
        }

        let store = Store::open(StoreOptions::new(&root, 1)).await.unwrap();
        assert_eq!(store.reference_delta_cursor(source()).unwrap(), 5);
        assert_eq!(
            store
                .blob_reference_state(&blob)
                .unwrap()
                .unwrap()
                .ref_count,
            0
        );
    }

    #[tokio::test]
    async fn positive_effect_requires_the_complete_bytes_to_still_exist() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let blob = store
            .stage_blob(&vec![0x7c; SMALL_BLOB_MAX_BYTES + 1])
            .await
            .unwrap();
        store.blobs.remove(&blob).unwrap();

        assert_eq!(
            store.apply_reference_deltas(batch(0, 1, &blob, 1)).await,
            Err(ReferenceDeltaError::ArtifactNotFound)
        );
        assert_eq!(store.reference_delta_cursor(source()).unwrap(), 0);
        let state = store.blob_reference_state(&blob).unwrap().unwrap();
        assert_eq!((state.ref_count, state.flags), (1, AWAITING_PUBLISH));
    }
}
