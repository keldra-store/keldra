use std::fs::File;
use std::io::Write;

use thiserror::Error;

use super::*;
use crate::{ErasureCodec, ErasureError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompleteCopySealOutcome {
    Created,
    AlreadyPresent,
}

/// Integrity state of one complete content copy on this node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadArtifactState {
    Missing,
    Valid,
    Corrupt,
}

/// Exact verified payload artifacts currently usable on this node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalPayloadPresence {
    complete_copy: PayloadArtifactState,
    shard_ordinals: Vec<u16>,
    corrupt_shard_ordinals: Vec<u16>,
}

impl LocalPayloadPresence {
    pub const fn complete_copy(&self) -> PayloadArtifactState {
        self.complete_copy
    }

    pub fn shard_ordinals(&self) -> &[u16] {
        &self.shard_ordinals
    }

    pub fn corrupt_shard_ordinals(&self) -> &[u16] {
        &self.corrupt_shard_ordinals
    }
}

#[derive(Debug, Error)]
pub enum PayloadStoreError {
    #[error("complete-copy operation requires content of at most 64 KiB")]
    NotSmall,
    #[error("erasure operation requires content larger than 64 KiB")]
    NotLarge,
    #[error("complete content copy is not present on this node")]
    CompleteCopyMissing,
    #[error("complete content copy is corrupt")]
    CompleteCopyCorrupt,
    #[error(transparent)]
    Mutation(#[from] MutationError),
    #[error(transparent)]
    Shard(#[from] ShardStoreError),
    #[error(transparent)]
    Erasure(#[from] ErasureError),
    #[error("payload storage failed: {0}")]
    Storage(String),
}

impl Store {
    /// Atomically publish one streamed complete large-object source only when
    /// its final bytes match the caller's exact immutable identity.
    ///
    /// The upload already lives in the ordinary blob `.staging` directory.
    /// No final path or lifecycle reservation is created for a mismatch.
    pub async fn seal_complete_source_upload(
        &self,
        expected: &BlobRef,
        upload: crate::BlobUpload,
    ) -> Result<CompleteCopySealOutcome, PayloadStoreError> {
        if is_small_blob(expected) {
            return Err(PayloadStoreError::NotLarge);
        }
        let previous = self.complete_copy_state(expected).await?;
        let staged = upload
            .finish_staged_expected(expected)
            .await
            .map_err(|error| PayloadStoreError::Storage(error.to_string()))?;
        if staged.reference() != expected {
            return Err(PayloadStoreError::Storage(
                "sealed complete source changed content identity".into(),
            ));
        }
        loop {
            let commit_guard = self.commit_lock.lock().await;
            if !staged.path().is_file()
                && !self
                    .blobs
                    .contains(expected)
                    .await
                    .map_err(|error| PayloadStoreError::Storage(error.to_string()))?
            {
                return Err(PayloadStoreError::CompleteCopyMissing);
            }
            let reservation = self.reserve_sealed_blob(expected, now_unix_millis()?);
            drop(commit_guard);
            match reservation {
                Ok(()) => break,
                Err(MutationError::SourceJournalCapacity) => {
                    self.wait_for_mutation_capacity().await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        self.blobs
            .publish_staged(staged)
            .await
            .map_err(|error| PayloadStoreError::Storage(error.to_string()))?;
        Ok(if previous == PayloadArtifactState::Valid {
            CompleteCopySealOutcome::AlreadyPresent
        } else {
            CompleteCopySealOutcome::Created
        })
    }

    /// Install one exact complete small-object copy in `small_blobs`.
    ///
    /// The supplied content identity is authoritative. Hash or length
    /// disagreement fails before any write, and an existing corrupt value is
    /// never replaced under the same identity.
    pub async fn seal_small_copy(
        &self,
        expected: &BlobRef,
        bytes: &[u8],
    ) -> Result<CompleteCopySealOutcome, PayloadStoreError> {
        if !is_small_blob(expected) {
            return Err(PayloadStoreError::NotSmall);
        }
        validate_small_blob(expected, bytes)?;
        let previous = self.complete_copy_state(expected).await?;
        let actual = self.stage_blob(bytes).await?;
        if &actual != expected {
            return Err(PayloadStoreError::Storage(
                "sealed small copy changed content identity".into(),
            ));
        }
        Ok(if previous == PayloadArtifactState::Valid {
            CompleteCopySealOutcome::AlreadyPresent
        } else {
            CompleteCopySealOutcome::Created
        })
    }

    /// Obtain and verify one exact complete small-object copy.
    pub fn read_small_copy(&self, reference: &BlobRef) -> Result<Vec<u8>, PayloadStoreError> {
        if !is_small_blob(reference) {
            return Err(PayloadStoreError::NotSmall);
        }
        let state = self
            .blob_reference_state(reference)?
            .filter(|state| state.ref_count != 0)
            .ok_or(PayloadStoreError::CompleteCopyMissing)?;
        validate_blob_reference_state(state)?;
        let bytes = self
            .db
            .get_cf(self.cf(CF_SMALL_BLOBS)?, blob_reference_key(reference))
            .map_err(|error| PayloadStoreError::Storage(error.to_string()))?
            .ok_or(PayloadStoreError::CompleteCopyMissing)?
            .to_vec();
        validate_small_blob(reference, &bytes)
            .map_err(|_| PayloadStoreError::CompleteCopyCorrupt)?;
        Ok(bytes)
    }

    /// Verify the complete small copy or large upload source on this node.
    pub async fn complete_copy_state(
        &self,
        reference: &BlobRef,
    ) -> Result<PayloadArtifactState, PayloadStoreError> {
        let Some(state) = self.blob_reference_state(reference)? else {
            return Ok(PayloadArtifactState::Missing);
        };
        validate_blob_reference_state(state)?;
        if state.ref_count == 0 {
            return Ok(PayloadArtifactState::Missing);
        }

        if is_small_blob(reference) {
            let Some(bytes) = self
                .db
                .get_cf(self.cf(CF_SMALL_BLOBS)?, blob_reference_key(reference))
                .map_err(|error| PayloadStoreError::Storage(error.to_string()))?
            else {
                return Ok(PayloadArtifactState::Missing);
            };
            return Ok(if validate_small_blob(reference, &bytes).is_ok() {
                PayloadArtifactState::Valid
            } else {
                PayloadArtifactState::Corrupt
            });
        }

        if !self
            .blobs
            .contains(reference)
            .await
            .map_err(|error| PayloadStoreError::Storage(error.to_string()))?
        {
            return Ok(PayloadArtifactState::Missing);
        }
        Ok(match self.blobs.open_verified(reference).await {
            Ok(_) => PayloadArtifactState::Valid,
            Err(_) => PayloadArtifactState::Corrupt,
        })
    }

    /// Report every locally valid artifact for one content identity.
    ///
    /// Corrupt shard ordinals are reported separately and never count as
    /// present. The returned ordinal vectors are sorted and duplicate-free.
    pub async fn local_payload_presence(
        &self,
        codec: &ErasureCodec,
        reference: &BlobRef,
    ) -> Result<LocalPayloadPresence, PayloadStoreError> {
        let complete_copy = self.complete_copy_state(reference).await?;
        let mut shard_ordinals = Vec::new();
        let mut corrupt_shard_ordinals = Vec::new();
        if !is_small_blob(reference) {
            for ordinal in 0..codec.profile().total_shards() {
                let identity = ShardIdentity::new(reference.clone(), ordinal);
                match self.validate_shard(codec, &identity) {
                    Ok(())
                        if self
                            .shard_reference_state(&identity)?
                            .is_some_and(|s| s.ref_count > 0) =>
                    {
                        shard_ordinals.push(ordinal);
                    }
                    Ok(()) | Err(ShardStoreError::NotFound) => {}
                    Err(ShardStoreError::Erasure(_))
                    | Err(ShardStoreError::MalformedIdentity)
                    | Err(ShardStoreError::UnsupportedFragmentFormat(_)) => {
                        corrupt_shard_ordinals.push(ordinal);
                    }
                    Err(error @ ShardStoreError::Storage(_)) => return Err(error.into()),
                }
            }
        }
        Ok(LocalPayloadPresence {
            complete_copy,
            shard_ordinals,
            corrupt_shard_ordinals,
        })
    }

    /// Encode one durably sealed complete large source into exactly `K + M`
    /// ordinal writers using the caller's committed cluster profile.
    ///
    /// Writers may be peer streams or local staging files. This operation does
    /// not create a second persistence plane or place multiple final ordinals
    /// on this node.
    pub async fn encode_sealed_source<W: Write>(
        &self,
        codec: &ErasureCodec,
        reference: &BlobRef,
        shards: &mut [W],
    ) -> Result<(), PayloadStoreError> {
        if is_small_blob(reference) {
            return Err(PayloadStoreError::NotLarge);
        }
        match self.complete_copy_state(reference).await? {
            PayloadArtifactState::Missing => return Err(PayloadStoreError::CompleteCopyMissing),
            PayloadArtifactState::Corrupt => return Err(PayloadStoreError::CompleteCopyCorrupt),
            PayloadArtifactState::Valid => {}
        }
        let source = File::open(self.blobs.path(&reference.hash))
            .map_err(|error| PayloadStoreError::Storage(error.to_string()))?;
        codec.encode(source, reference, shards)?;
        Ok(())
    }

    /// Reconstruct one complete blob from the valid local shard subset.
    ///
    /// This is useful for focused recovery tests and a future repair worker;
    /// ordinary distributed reads pass peer-provided ordinal streams directly
    /// to [`ErasureCodec::reconstruct_available`].
    pub fn reconstruct_from_local_shards<W: Write>(
        &self,
        codec: &ErasureCodec,
        reference: &BlobRef,
        output: &mut W,
    ) -> Result<(), PayloadStoreError> {
        if is_small_blob(reference) {
            return Err(PayloadStoreError::NotLarge);
        }
        let mut shards = Vec::new();
        for ordinal in 0..codec.profile().total_shards() {
            let identity = ShardIdentity::new(reference.clone(), ordinal);
            match self.get_shard(codec, &identity) {
                Ok(reader) => shards.push((ordinal, reader)),
                Err(ShardStoreError::NotFound) | Err(ShardStoreError::Erasure(_)) => {}
                Err(error) => return Err(error.into()),
            }
        }
        codec.reconstruct_available(reference, shards, output)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::{ErasureProfile, SMALL_BLOB_MAX_BYTES, StoreOptions};

    async fn open_store(root: &Path) -> Store {
        Store::open(StoreOptions::new(root, 1)).await.unwrap()
    }

    fn blob(bytes: &[u8]) -> BlobRef {
        BlobRef {
            hash: *blake3::hash(bytes).as_bytes(),
            length: bytes.len() as u64,
        }
    }

    fn shard_path(root: &Path, identity: &ShardIdentity) -> PathBuf {
        let hash = hex::encode(identity.blob().hash);
        root.join("blobs")
            .join(&hash[..2])
            .join(hex::encode(identity.encode()))
    }

    #[tokio::test]
    async fn exact_small_copy_survives_restart_in_small_blobs() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("store");
        let bytes = b"one exact small copy";
        let reference = blob(bytes);

        {
            let store = open_store(&root).await;
            assert_eq!(
                store.seal_small_copy(&reference, bytes).await.unwrap(),
                CompleteCopySealOutcome::Created
            );
            assert_eq!(store.read_small_copy(&reference).unwrap(), bytes);
            assert_eq!(
                store.seal_small_copy(&reference, bytes).await.unwrap(),
                CompleteCopySealOutcome::AlreadyPresent
            );
        }

        let store = open_store(&root).await;
        assert_eq!(
            store.complete_copy_state(&reference).await.unwrap(),
            PayloadArtifactState::Valid
        );
        assert_eq!(store.read_small_copy(&reference).unwrap(), bytes);
        assert!(!store.blobs.path(&reference.hash).exists());
    }

    #[tokio::test]
    async fn corrupt_small_copy_is_never_reported_or_replaced_as_valid() {
        let temporary = tempfile::tempdir().unwrap();
        let store = open_store(temporary.path()).await;
        let bytes = b"small value";
        let reference = blob(bytes);
        store.seal_small_copy(&reference, bytes).await.unwrap();
        store
            .db
            .put_cf(
                store.cf(CF_SMALL_BLOBS).unwrap(),
                blob_reference_key(&reference),
                b"wrong value",
            )
            .unwrap();

        assert_eq!(
            store.complete_copy_state(&reference).await.unwrap(),
            PayloadArtifactState::Corrupt
        );
        assert!(matches!(
            store.read_small_copy(&reference),
            Err(PayloadStoreError::CompleteCopyCorrupt)
        ));
        assert!(store.seal_small_copy(&reference, bytes).await.is_err());
        assert_eq!(
            store.complete_copy_state(&reference).await.unwrap(),
            PayloadArtifactState::Corrupt
        );
    }

    #[tokio::test]
    async fn default_two_plus_one_encodes_and_reconstructs_from_any_two() {
        let temporary = tempfile::tempdir().unwrap();
        let store = open_store(temporary.path()).await;
        let source = (0..SMALL_BLOB_MAX_BYTES + 47)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let reference = store.stage_blob(&source).await.unwrap();
        let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
        assert_eq!(
            (
                codec.profile().data_shards(),
                codec.profile().parity_shards()
            ),
            (2, 1)
        );
        let mut encoded = vec![Vec::new(); usize::from(codec.profile().total_shards())];
        store
            .encode_sealed_source(&codec, &reference, &mut encoded)
            .await
            .unwrap();
        for (ordinal, bytes) in encoded.iter().enumerate() {
            assert_eq!(
                codec
                    .encoded_shard_length(&reference, ordinal as u16)
                    .unwrap(),
                bytes.len() as u64
            );
        }

        for ordinal in [0, 2] {
            let identity = ShardIdentity::new(reference.clone(), ordinal);
            store
                .seal_shard(
                    &codec,
                    &identity,
                    Cursor::new(&encoded[usize::from(ordinal)]),
                )
                .await
                .unwrap();
        }
        let mut reconstructed = Vec::new();
        codec
            .reconstruct_available(
                &reference,
                [(0, Cursor::new(&encoded[0])), (2, Cursor::new(&encoded[2]))],
                &mut reconstructed,
            )
            .unwrap();
        assert_eq!(reconstructed, source);

        let presence = store
            .local_payload_presence(&codec, &reference)
            .await
            .unwrap();
        assert_eq!(presence.complete_copy(), PayloadArtifactState::Valid);
        assert_eq!(presence.shard_ordinals(), [0, 2]);
        let mut reconstructed = Vec::new();
        store
            .reconstruct_from_local_shards(&codec, &reference, &mut reconstructed)
            .unwrap();
        assert_eq!(reconstructed, source);
    }

    #[tokio::test]
    async fn insufficient_or_corrupt_shards_fail_closed() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path();
        let store = open_store(root).await;
        let source = vec![0x6d; SMALL_BLOB_MAX_BYTES + 31];
        let reference = store.stage_blob(&source).await.unwrap();
        let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
        let mut encoded = vec![Vec::new(); usize::from(codec.profile().total_shards())];
        store
            .encode_sealed_source(&codec, &reference, &mut encoded)
            .await
            .unwrap();
        for ordinal in 0..codec.profile().total_shards() {
            let identity = ShardIdentity::new(reference.clone(), ordinal);
            store
                .seal_shard(
                    &codec,
                    &identity,
                    Cursor::new(&encoded[usize::from(ordinal)]),
                )
                .await
                .unwrap();
        }

        let corrupt = ShardIdentity::new(reference.clone(), 1);
        let path = shard_path(root, &corrupt);
        let mut bytes = std::fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        let presence = store
            .local_payload_presence(&codec, &reference)
            .await
            .unwrap();
        assert_eq!(presence.shard_ordinals(), [0, 2]);
        assert_eq!(presence.corrupt_shard_ordinals(), [1]);
        let mut reconstructed = Vec::new();
        store
            .reconstruct_from_local_shards(&codec, &reference, &mut reconstructed)
            .unwrap();
        assert_eq!(reconstructed, source);

        store
            .remove_shard(&ShardIdentity::new(reference.clone(), 2))
            .await
            .unwrap();
        let mut insufficient = Vec::new();
        assert!(matches!(
            store.reconstruct_from_local_shards(&codec, &reference, &mut insufficient),
            Err(PayloadStoreError::Erasure(
                ErasureError::TooFewValidChunks {
                    required: 2,
                    available: 1,
                    ..
                }
            ))
        ));
    }

    #[tokio::test]
    async fn source_and_shard_presence_survive_restart() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("store");
        let source = vec![0x93; SMALL_BLOB_MAX_BYTES + 19];
        let reference;

        {
            let store = open_store(&root).await;
            reference = store.stage_blob(&source).await.unwrap();
            let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
            let mut encoded = vec![Vec::new(); usize::from(codec.profile().total_shards())];
            store
                .encode_sealed_source(&codec, &reference, &mut encoded)
                .await
                .unwrap();
            let identity = ShardIdentity::new(reference.clone(), 2);
            store
                .seal_shard(&codec, &identity, Cursor::new(&encoded[2]))
                .await
                .unwrap();
        }

        let store = open_store(&root).await;
        let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
        let presence = store
            .local_payload_presence(&codec, &reference)
            .await
            .unwrap();
        assert_eq!(presence.complete_copy(), PayloadArtifactState::Valid);
        assert_eq!(presence.shard_ordinals(), [2]);
        assert!(presence.corrupt_shard_ordinals().is_empty());
    }

    #[test]
    fn repeated_ordinal_is_rejected_before_decode() {
        let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
        let reference = blob(&vec![0x44; SMALL_BLOB_MAX_BYTES + 1]);
        let mut output = Vec::new();
        assert!(matches!(
            codec.reconstruct_available(
                &reference,
                [
                    (0, Cursor::new(Vec::<u8>::new())),
                    (0, Cursor::new(Vec::new()))
                ],
                &mut output,
            ),
            Err(ErasureError::DuplicateShardOrdinal { ordinal: 0 })
        ));
    }
}
