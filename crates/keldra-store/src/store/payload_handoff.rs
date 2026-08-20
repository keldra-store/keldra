//! Bounded typed payload state transfer for membership handoff.
//!
//! Artifact bytes remain in the ordinary inline/blob/shard planes. This module
//! only enumerates their existing lifecycle records and, after exact bytes
//! have been durably verified, installs that same lifecycle state.

use rocksdb::{Direction, IteratorMode, WriteOptions};
use serde::{Deserialize, Serialize};

use super::*;
use crate::{ErasureCodec, FRAGMENT_FORMAT_VERSION};

pub const MAX_PAYLOAD_HANDOFF_EXPORT_RECORDS: u32 = 1_000;
const COMPLETE_HANDOFF_KIND: u8 = 0;
const SHARD_HANDOFF_KIND: u8 = 1;
const COMPLETE_HANDOFF_KEY_BYTES: usize = 32 + size_of::<u64>() + 1;
const SHARD_HANDOFF_KEY_BYTES: usize = COMPLETE_HANDOFF_KEY_BYTES + size_of::<u16>();

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "identity", rename_all = "snake_case")]
pub enum PayloadArtifactIdentity {
    Complete(BlobRef),
    Shard(ShardIdentity),
}

impl PayloadArtifactIdentity {
    fn key(&self) -> Vec<u8> {
        match self {
            Self::Complete(reference) => blob_reference_key(reference),
            Self::Shard(identity) => identity.encode().to_vec(),
        }
    }

    pub fn blob(&self) -> &BlobRef {
        match self {
            Self::Complete(reference) => reference,
            Self::Shard(identity) => identity.blob(),
        }
    }

    /// Ephemeral blob-first key used only to merge bounded handoff pages.
    ///
    /// The persisted complete and shard keys have different layouts. This
    /// normalized order keeps every artifact for one blob adjacent without
    /// changing either durable layout.
    pub fn handoff_order_key(&self) -> Vec<u8> {
        let reference = self.blob();
        let mut key = Vec::with_capacity(match self {
            Self::Complete(_) => COMPLETE_HANDOFF_KEY_BYTES,
            Self::Shard(_) => SHARD_HANDOFF_KEY_BYTES,
        });
        key.extend_from_slice(&reference.hash);
        key.extend_from_slice(&reference.length.to_be_bytes());
        match self {
            Self::Complete(_) => key.push(COMPLETE_HANDOFF_KIND),
            Self::Shard(identity) => {
                key.push(SHARD_HANDOFF_KIND);
                key.extend_from_slice(&identity.ordinal().to_be_bytes());
            }
        }
        key
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PayloadArtifactSnapshot {
    pub identity: PayloadArtifactIdentity,
    pub lifecycle: BlobReferenceState,
}

impl PayloadArtifactSnapshot {
    pub fn validate(&self) -> Result<(), MutationError> {
        validate_blob_reference_state(self.lifecycle)?;
        let key = self.identity.key();
        match &self.identity {
            PayloadArtifactIdentity::Complete(_) => {
                if key.len() != 40 {
                    return Err(storage_error(
                        "payload handoff has a malformed complete identity",
                    ));
                }
            }
            PayloadArtifactIdentity::Shard(_) => {
                ShardIdentity::decode(&key).map_err(storage_error)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PayloadArtifactCursor(Vec<u8>);

impl PayloadArtifactCursor {
    pub fn from_key(key: Vec<u8>) -> Result<Self, MutationError> {
        decode_handoff_key(&key)?;
        Ok(Self(key))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PayloadArtifactSnapshotPage {
    pub artifacts: Vec<PayloadArtifactSnapshot>,
    pub next_cursor: Option<PayloadArtifactCursor>,
}

impl Store {
    /// Enumerate the existing lifecycle column family without inventing a
    /// placement inventory. The cursor is an ephemeral normalized order key.
    pub fn export_payload_artifact_snapshots(
        &self,
        cursor: Option<&PayloadArtifactCursor>,
        max_records: u32,
    ) -> Result<PayloadArtifactSnapshotPage, MutationError> {
        if max_records == 0 || max_records > MAX_PAYLOAD_HANDOFF_EXPORT_RECORDS {
            return Err(storage_error("payload handoff page limit is invalid"));
        }
        let after = cursor
            .map(|cursor| decode_handoff_key(&cursor.0))
            .transpose()?;
        let complete_start = after
            .as_ref()
            .map_or_else(Vec::new, |identity| blob_reference_key(identity.blob()));
        let shard_start = after.as_ref().map_or_else(
            || FRAGMENT_FORMAT_VERSION.to_be_bytes().to_vec(),
            |identity| {
                let ordinal = match identity {
                    PayloadArtifactIdentity::Complete(_) => 0,
                    PayloadArtifactIdentity::Shard(identity) => identity.ordinal(),
                };
                ShardIdentity::new(identity.blob().clone(), ordinal)
                    .encode()
                    .to_vec()
            },
        );
        let family = self.cf(CF_BLOB_REFERENCES)?;
        let mut completes = self.db.iterator_cf(
            family,
            IteratorMode::From(&complete_start, Direction::Forward),
        );
        let mut shards = self
            .db
            .iterator_cf(family, IteratorMode::From(&shard_start, Direction::Forward));
        let after_key = cursor.map(|cursor| cursor.0.as_slice());
        let mut next_complete = next_artifact(&mut completes, ArtifactKind::Complete, after_key)?;
        let mut next_shard = next_artifact(&mut shards, ArtifactKind::Shard, after_key)?;
        let mut artifacts = Vec::with_capacity(max_records as usize);
        let mut last_key = None;
        while artifacts.len() < max_records as usize {
            let take_complete = match (&next_complete, &next_shard) {
                (Some(complete), Some(shard)) => complete.order_key < shard.order_key,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let next = if take_complete {
                let next = next_complete
                    .take()
                    .expect("complete handoff candidate was selected");
                next_complete = next_artifact(&mut completes, ArtifactKind::Complete, after_key)?;
                next
            } else {
                let next = next_shard
                    .take()
                    .expect("shard handoff candidate was selected");
                next_shard = next_artifact(&mut shards, ArtifactKind::Shard, after_key)?;
                next
            };
            last_key = Some(next.order_key);
            artifacts.push(next.artifact);
        }
        let has_more = next_complete.is_some() || next_shard.is_some();
        Ok(PayloadArtifactSnapshotPage {
            artifacts,
            next_cursor: has_more
                .then(|| PayloadArtifactCursor(last_key.expect("a full page has a last key"))),
        })
    }

    /// Replace the temporary seal reservation with the exact lifecycle state
    /// copied during handoff. The ordinary seal path must have durably written
    /// and verified the bytes first; a crash before this call leaves only a
    /// safe age-gated reservation, and retrying completes the same transfer.
    pub async fn install_payload_artifact_lifecycle(
        &self,
        codec: &ErasureCodec,
        artifact: &PayloadArtifactSnapshot,
    ) -> Result<(), MutationError> {
        artifact.validate()?;
        match &artifact.identity {
            PayloadArtifactIdentity::Complete(reference) => {
                if self.open_blob(reference).await.is_err() {
                    return Err(storage_error(
                        "payload handoff lifecycle cannot precede verified complete bytes",
                    ));
                }
            }
            PayloadArtifactIdentity::Shard(identity) => {
                self.validate_shard(codec, identity)
                    .map_err(storage_error)?;
            }
        }

        let _guard = self.commit_lock.lock().await;
        let key = artifact.identity.key();
        let present = match &artifact.identity {
            PayloadArtifactIdentity::Complete(reference) if is_small_blob(reference) => self
                .db
                .get_cf(self.cf(CF_SMALL_BLOBS)?, &key)
                .map_err(storage_error)?
                .is_some(),
            PayloadArtifactIdentity::Complete(reference) => self
                .blobs
                .contains(reference)
                .await
                .map_err(storage_error)?,
            PayloadArtifactIdentity::Shard(identity) => self
                .contains_shard_artifact(identity)
                .map_err(storage_error)?,
        };
        if !present {
            return Err(MutationError::BlobNotFound);
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        let lifecycle = match self
            .db
            .get_cf(self.cf(CF_BLOB_REFERENCES)?, &key)
            .map_err(storage_error)?
        {
            Some(encoded) => {
                let current = decode_blob_reference_state(&encoded)?;
                BlobReferenceState {
                    ref_count: artifact.lifecycle.ref_count,
                    flags: artifact.lifecycle.flags,
                    created_at: artifact.lifecycle.created_at.min(current.created_at),
                    updated_at: artifact.lifecycle.updated_at.max(current.updated_at),
                }
            }
            None => artifact.lifecycle,
        };
        let mut batch = rocksdb::WriteBatch::default();
        let mut pending = PendingBlobReferences::new();
        self.stage_blob_reference_update(&mut batch, &mut pending, key, lifecycle)?;
        self.db.write_opt(batch, &options).map_err(storage_error)
    }

    /// Retire one physical artifact which the caller has proved is no longer
    /// selected by the current cluster placement.
    ///
    /// The caller-supplied fence check runs while the ordinary store commit
    /// lock is held. The lifecycle must still exactly match the snapshot the
    /// caller inspected; a concurrent touch or reference change makes this a
    /// harmless no-op. Retirement does not remove bytes. It starts the normal
    /// inactivity grace, after which ordinary blob garbage collection performs
    /// the physical removal.
    pub async fn retire_payload_artifact_if_unchanged<F>(
        &self,
        expected: &PayloadArtifactSnapshot,
        placement_is_still_current: F,
    ) -> Result<bool, MutationError>
    where
        F: FnOnce() -> bool,
    {
        self.retire_payload_artifact_if_unchanged_inner(expected, None, placement_is_still_current)
            .await
    }

    #[cfg(test)]
    async fn retire_payload_artifact_if_unchanged_at<F>(
        &self,
        expected: &PayloadArtifactSnapshot,
        now_unix_millis: u64,
        placement_is_still_current: F,
    ) -> Result<bool, MutationError>
    where
        F: FnOnce() -> bool,
    {
        self.retire_payload_artifact_if_unchanged_inner(
            expected,
            Some(now_unix_millis),
            placement_is_still_current,
        )
        .await
    }

    async fn retire_payload_artifact_if_unchanged_inner<F>(
        &self,
        expected: &PayloadArtifactSnapshot,
        fixed_now_unix_millis: Option<u64>,
        placement_is_still_current: F,
    ) -> Result<bool, MutationError>
    where
        F: FnOnce() -> bool,
    {
        expected.validate()?;
        let _guard = self.commit_lock.lock().await;
        let key = expected.identity.key();
        let Some(encoded) = self
            .db
            .get_cf(self.cf(CF_BLOB_REFERENCES)?, &key)
            .map_err(storage_error)?
        else {
            return Ok(false);
        };
        let current = decode_blob_reference_state(&encoded)?;
        if current != expected.lifecycle
            || (current.ref_count == 0 && current.flags == 0)
            || !placement_is_still_current()
        {
            return Ok(false);
        }
        let now_unix_millis = fixed_now_unix_millis.map_or_else(now_unix_millis, Ok)?;

        let retired = BlobReferenceState {
            ref_count: 0,
            flags: 0,
            created_at: current.created_at,
            updated_at: current.updated_at.max(now_unix_millis),
        };
        let mut batch = rocksdb::WriteBatch::default();
        let mut pending = PendingBlobReferences::new();
        self.stage_blob_reference_update(&mut batch, &mut pending, key.clone(), retired)?;
        self.stage_local_changes(
            &mut batch,
            &[PendingLocalChange::ContentLifecycleChanged {
                blob_identity: key,
                revision: retired.updated_at,
                reference_deltas: Vec::new(),
            }],
            LocalReferenceEffects::NoReferenceEffects,
        )?;
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
        self.notify_local_invalidations();
        Ok(true)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactKind {
    Complete,
    Shard,
}

struct OrderedArtifact {
    order_key: Vec<u8>,
    artifact: PayloadArtifactSnapshot,
}

fn next_artifact(
    entries: &mut impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>,
    wanted: ArtifactKind,
    after: Option<&[u8]>,
) -> Result<Option<OrderedArtifact>, MutationError> {
    for entry in entries {
        let (key, encoded) = entry.map_err(storage_error)?;
        let kind = match key.len() {
            40 => ArtifactKind::Complete,
            44 => ArtifactKind::Shard,
            _ => {
                return Err(storage_error(
                    "payload lifecycle key has a malformed identity",
                ));
            }
        };
        if kind != wanted {
            continue;
        }
        let artifact = PayloadArtifactSnapshot {
            identity: decode_artifact_identity(&key)?,
            lifecycle: decode_blob_reference_state(&encoded)?,
        };
        artifact.validate()?;
        let order_key = artifact.identity.handoff_order_key();
        if after.is_some_and(|after| order_key.as_slice() <= after) {
            continue;
        }
        return Ok(Some(OrderedArtifact {
            order_key,
            artifact,
        }));
    }
    Ok(None)
}

fn decode_handoff_key(key: &[u8]) -> Result<PayloadArtifactIdentity, MutationError> {
    if key.len() == COMPLETE_HANDOFF_KEY_BYTES && key[40] == COMPLETE_HANDOFF_KIND {
        return blob_reference_from_key(&key[..40]).map(PayloadArtifactIdentity::Complete);
    }
    if key.len() == SHARD_HANDOFF_KEY_BYTES && key[40] == SHARD_HANDOFF_KIND {
        let blob = blob_reference_from_key(&key[..40])?;
        let ordinal = u16::from_be_bytes(
            key[41..]
                .try_into()
                .expect("handoff shard ordinal width was checked"),
        );
        return Ok(PayloadArtifactIdentity::Shard(ShardIdentity::new(
            blob, ordinal,
        )));
    }
    Err(storage_error("payload handoff cursor is malformed"))
}

fn decode_artifact_identity(key: &[u8]) -> Result<PayloadArtifactIdentity, MutationError> {
    match key.len() {
        40 => blob_reference_from_key(key).map(PayloadArtifactIdentity::Complete),
        44 => ShardIdentity::decode(key)
            .map(PayloadArtifactIdentity::Shard)
            .map_err(storage_error),
        _ => Err(storage_error(
            "payload lifecycle key has a malformed identity",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::{ErasureProfile, StoreOptions};

    #[tokio::test]
    async fn handoff_preserves_logical_state_without_regressing_timestamps() {
        let source_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let source = Store::open(StoreOptions::new(source_dir.path(), 1))
            .await
            .unwrap();
        let target = Store::open(StoreOptions::new(target_dir.path(), 2))
            .await
            .unwrap();
        let small = source.stage_blob(b"small handoff").await.unwrap();
        let large_bytes = vec![7_u8; SMALL_BLOB_MAX_BYTES + 1];
        let large = source.stage_blob(&large_bytes).await.unwrap();
        let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
        let mut shards = vec![Vec::new(); usize::from(codec.profile().total_shards())];
        source
            .encode_sealed_source(&codec, &large, &mut shards)
            .await
            .unwrap();
        let shard = ShardIdentity::new(large, 0);
        source
            .seal_shard(&codec, &shard, Cursor::new(&shards[0]))
            .await
            .unwrap();

        let page = source.export_payload_artifact_snapshots(None, 100).unwrap();
        let small_snapshot = page
            .artifacts
            .iter()
            .find(|entry| entry.identity == PayloadArtifactIdentity::Complete(small.clone()))
            .unwrap();
        target
            .seal_small_copy(&small, b"small handoff")
            .await
            .unwrap();
        let target_small_before = target.blob_reference_state(&small).unwrap().unwrap();
        target
            .install_payload_artifact_lifecycle(&codec, small_snapshot)
            .await
            .unwrap();
        let target_small_after = target.blob_reference_state(&small).unwrap().unwrap();
        assert_eq!(
            target_small_after.ref_count,
            small_snapshot.lifecycle.ref_count
        );
        assert_eq!(target_small_after.flags, small_snapshot.lifecycle.flags);
        assert_eq!(
            target_small_after.created_at,
            target_small_before
                .created_at
                .min(small_snapshot.lifecycle.created_at)
        );
        assert_eq!(
            target_small_after.updated_at,
            target_small_before
                .updated_at
                .max(small_snapshot.lifecycle.updated_at)
        );

        let shard_snapshot = page
            .artifacts
            .iter()
            .find(|entry| entry.identity == PayloadArtifactIdentity::Shard(shard.clone()))
            .unwrap();
        target
            .seal_shard(&codec, &shard, Cursor::new(&shards[0]))
            .await
            .unwrap();
        let target_shard_before = target.shard_reference_state(&shard).unwrap().unwrap();
        target
            .install_payload_artifact_lifecycle(&codec, shard_snapshot)
            .await
            .unwrap();
        let target_shard_after = target.shard_reference_state(&shard).unwrap().unwrap();
        assert_eq!(
            target_shard_after.ref_count,
            shard_snapshot.lifecycle.ref_count
        );
        assert_eq!(target_shard_after.flags, shard_snapshot.lifecycle.flags);
        assert_eq!(
            target_shard_after.created_at,
            target_shard_before
                .created_at
                .min(shard_snapshot.lifecycle.created_at)
        );
        assert_eq!(
            target_shard_after.updated_at,
            target_shard_before
                .updated_at
                .max(shard_snapshot.lifecycle.updated_at)
        );
    }

    #[tokio::test]
    async fn payload_export_is_blob_first_across_one_record_pages() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(dir.path(), 1)).await.unwrap();
        let small = store.stage_blob(b"small page boundary").await.unwrap();
        let large_bytes = vec![11_u8; SMALL_BLOB_MAX_BYTES + 1];
        let large = store.stage_blob(&large_bytes).await.unwrap();
        let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
        let mut shards = vec![Vec::new(); usize::from(codec.profile().total_shards())];
        store
            .encode_sealed_source(&codec, &large, &mut shards)
            .await
            .unwrap();
        for ordinal in 0..2 {
            store
                .seal_shard(
                    &codec,
                    &ShardIdentity::new(large.clone(), ordinal),
                    Cursor::new(&shards[usize::from(ordinal)]),
                )
                .await
                .unwrap();
        }

        let mut cursor = None;
        let mut exported = Vec::new();
        loop {
            let page = store
                .export_payload_artifact_snapshots(cursor.as_ref(), 1)
                .unwrap();
            assert!(page.artifacts.len() <= 1);
            exported.extend(page.artifacts.into_iter().map(|entry| entry.identity));
            let Some(next) = page.next_cursor else {
                break;
            };
            cursor = Some(next);
        }

        let mut expected = vec![
            PayloadArtifactIdentity::Complete(small),
            PayloadArtifactIdentity::Complete(large.clone()),
            PayloadArtifactIdentity::Shard(ShardIdentity::new(large.clone(), 0)),
            PayloadArtifactIdentity::Shard(ShardIdentity::new(large, 1)),
        ];
        expected.sort_by_key(PayloadArtifactIdentity::handoff_order_key);
        assert_eq!(exported, expected);
        assert!(
            exported
                .windows(2)
                .all(|pair| pair[0].handoff_order_key() < pair[1].handoff_order_key())
        );
    }

    #[tokio::test]
    async fn lifecycle_install_rejects_missing_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(dir.path(), 1)).await.unwrap();
        let artifact = PayloadArtifactSnapshot {
            identity: PayloadArtifactIdentity::Complete(BlobRef {
                hash: *blake3::hash(b"missing").as_bytes(),
                length: 7,
            }),
            lifecycle: BlobReferenceState {
                ref_count: 9,
                flags: 0,
                created_at: 1,
                updated_at: 2,
            },
        };
        let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
        assert!(
            store
                .install_payload_artifact_lifecycle(&codec, &artifact)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn lifecycle_install_accepts_verified_zero_reference_complete_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(dir.path(), 1)).await.unwrap();
        let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
        for bytes in [
            b"zero-reference inline handoff".to_vec(),
            vec![23_u8; SMALL_BLOB_MAX_BYTES + 1],
        ] {
            let reference = store.stage_blob(&bytes).await.unwrap();
            let initial = store.blob_reference_state(&reference).unwrap().unwrap();
            let artifact = PayloadArtifactSnapshot {
                identity: PayloadArtifactIdentity::Complete(reference.clone()),
                lifecycle: BlobReferenceState {
                    ref_count: 0,
                    flags: 0,
                    created_at: initial.created_at,
                    updated_at: initial.updated_at + 1,
                },
            };

            store
                .install_payload_artifact_lifecycle(&codec, &artifact)
                .await
                .unwrap();
            // Handoff retries the same record after failures. Verification
            // must use retained bytes once lifecycle is already zero.
            store
                .install_payload_artifact_lifecycle(&codec, &artifact)
                .await
                .unwrap();

            assert_eq!(
                store.blob_reference_state(&reference).unwrap(),
                Some(artifact.lifecycle)
            );
            assert_eq!(
                store.complete_copy_state(&reference).await.unwrap(),
                PayloadArtifactState::Missing
            );
            store.open_blob(&reference).await.unwrap();
        }
    }

    #[tokio::test]
    async fn former_artifact_retirement_is_fenced_exact_and_age_gated() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            Store::open(StoreOptions::new(dir.path(), 1).with_awaiting_publish_ttl_seconds(1))
                .await
                .unwrap();
        let bytes = vec![19_u8; SMALL_BLOB_MAX_BYTES + 1];
        let reference = store.stage_blob(&bytes).await.unwrap();
        let initial = store.blob_reference_state(&reference).unwrap().unwrap();
        let observed = PayloadArtifactSnapshot {
            identity: PayloadArtifactIdentity::Complete(reference.clone()),
            lifecycle: initial,
        };

        assert!(
            !store
                .retire_payload_artifact_if_unchanged_at(&observed, initial.updated_at + 10, || {
                    false
                })
                .await
                .unwrap()
        );
        assert_eq!(
            store.blob_reference_state(&reference).unwrap(),
            Some(initial)
        );

        assert!(
            store
                .retire_payload_artifact_if_unchanged_at(&observed, initial.updated_at + 20, || {
                    true
                })
                .await
                .unwrap()
        );
        let retired = store.blob_reference_state(&reference).unwrap().unwrap();
        assert_eq!((retired.ref_count, retired.flags), (0, 0));
        assert_eq!(retired.updated_at, initial.updated_at + 20);
        let due_identity = blob_reference_key(&reference);
        assert!(
            store
                .db
                .iterator_cf(store.cf(CF_BLOB_GC_DUE).unwrap(), IteratorMode::Start)
                .any(|entry| entry.unwrap().0.ends_with(&due_identity))
        );

        assert!(
            !store
                .retire_payload_artifact_if_unchanged_at(&observed, initial.updated_at + 30, || {
                    true
                })
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .collect_blob_garbage_at(retired.updated_at)
                .await
                .unwrap(),
            0
        );
        assert!(store.contains_blob(&reference).await.unwrap());
        assert_eq!(
            store
                .collect_blob_garbage_at(retired.updated_at + 1_000)
                .await
                .unwrap(),
            1
        );
        assert!(!store.contains_blob(&reference).await.unwrap());
    }
}
