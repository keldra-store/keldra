use std::io::{self, Read};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

use super::journal_capacity::SourceJournalAdmission;
#[cfg(test)]
use super::payload_artifacts::shard_inline_key;
use super::payload_artifacts::{ArtifactLayout, ArtifactManifest, RocksArtifactReader};
use super::*;
use crate::{ErasureCodec, ErasureError, FRAGMENT_FORMAT_VERSION};

const SHARD_IDENTITY_BYTES: usize = 2 + 32 + 8 + 2;

/// The complete stable identity of one erasure-coded shard.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardIdentity {
    fragment_format_version: u16,
    blob: BlobRef,
    ordinal: u16,
}

impl ShardIdentity {
    pub fn new(blob: BlobRef, ordinal: u16) -> Self {
        Self {
            fragment_format_version: FRAGMENT_FORMAT_VERSION,
            blob,
            ordinal,
        }
    }

    pub const fn fragment_format_version(&self) -> u16 {
        self.fragment_format_version
    }

    pub const fn blob(&self) -> &BlobRef {
        &self.blob
    }

    pub const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    /// Canonical placement and persistence identity.
    pub fn encode(&self) -> [u8; SHARD_IDENTITY_BYTES] {
        let mut encoded = [0_u8; SHARD_IDENTITY_BYTES];
        encoded[..2].copy_from_slice(&self.fragment_format_version.to_be_bytes());
        encoded[2..34].copy_from_slice(&self.blob.hash);
        encoded[34..42].copy_from_slice(&self.blob.length.to_be_bytes());
        encoded[42..].copy_from_slice(&self.ordinal.to_be_bytes());
        encoded
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self, ShardStoreError> {
        if encoded.len() != SHARD_IDENTITY_BYTES {
            return Err(ShardStoreError::MalformedIdentity);
        }
        let fragment_format_version = u16::from_be_bytes(
            encoded[..2]
                .try_into()
                .expect("fragment format version width was checked"),
        );
        if fragment_format_version != FRAGMENT_FORMAT_VERSION {
            return Err(ShardStoreError::UnsupportedFragmentFormat(
                fragment_format_version,
            ));
        }
        let hash = encoded[2..34]
            .try_into()
            .expect("shard blob hash width was checked");
        let length = u64::from_be_bytes(
            encoded[34..42]
                .try_into()
                .expect("shard blob length width was checked"),
        );
        let ordinal = u16::from_be_bytes(
            encoded[42..]
                .try_into()
                .expect("shard ordinal width was checked"),
        );
        Ok(Self {
            fragment_format_version,
            blob: BlobRef { hash, length },
            ordinal,
        })
    }

    fn validate_for(&self, codec: &ErasureCodec) -> Result<(), ShardStoreError> {
        if self.fragment_format_version != FRAGMENT_FORMAT_VERSION {
            return Err(ShardStoreError::UnsupportedFragmentFormat(
                self.fragment_format_version,
            ));
        }
        if self.ordinal >= codec.profile().total_shards() {
            return Err(ErasureError::InvalidShardOrdinal {
                ordinal: self.ordinal,
                total: codec.profile().total_shards(),
            }
            .into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardSealOutcome {
    Created,
    AlreadyPresent,
}

/// A complete shard that was validated before being returned.
#[derive(Debug)]
pub struct ShardReader {
    reader: RocksArtifactReader,
}

impl Read for ShardReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buffer)
    }
}

#[derive(Debug, Error)]
pub enum ShardStoreError {
    #[error("shard identity is malformed")]
    MalformedIdentity,
    #[error("unsupported fragment format {0}")]
    UnsupportedFragmentFormat(u16),
    #[error("shard is not present on this node")]
    NotFound,
    #[error(transparent)]
    Erasure(#[from] ErasureError),
    #[error("shard storage failed: {0}")]
    Storage(String),
}

impl Store {
    /// Durably seals one already-encoded shard.
    ///
    /// Retrying the same identity validates the existing immutable artifact and
    /// refreshes its one awaiting-publication reservation without increasing
    /// the reference count.
    pub async fn seal_shard<R: Read>(
        &self,
        codec: &ErasureCodec,
        identity: &ShardIdentity,
        mut encoded_shard: R,
    ) -> Result<ShardSealOutcome, ShardStoreError> {
        identity.validate_for(codec)?;
        let Some((manifest, start)) = self.prepare_shard_install(codec, identity).await? else {
            return Ok(ShardSealOutcome::AlreadyPresent);
        };
        discard_exact(
            &mut encoded_shard,
            u64::from(start) * PAYLOAD_ARTIFACT_CHUNK_BYTES as u64,
        )?;
        let total = artifact_chunk_count(&manifest);
        let mut buffer = vec![0_u8; PAYLOAD_ARTIFACT_CHUNK_BYTES];
        for ordinal in start..total {
            let expected = artifact_chunk_length(&manifest, ordinal)?;
            encoded_shard
                .read_exact(&mut buffer[..expected])
                .map_err(shard_storage_error)?;
            self.persist_shard_chunk(identity, &manifest, ordinal, &buffer[..expected])
                .await?;
        }
        let mut trailing = [0_u8; 1];
        if encoded_shard
            .read(&mut trailing)
            .map_err(shard_storage_error)?
            != 0
        {
            return Err(ShardStoreError::Storage(
                "encoded shard exceeds its exact expected length".into(),
            ));
        }
        self.finish_shard_install(codec, identity, &manifest)
            .await?;
        Ok(ShardSealOutcome::Created)
    }

    /// Durably seals one already-encoded shard from an asynchronous source.
    ///
    /// This is the bounded-memory ingress used by peer streaming. It shares
    /// the same staging, validation, atomic publish, and lifecycle path as
    /// [`Store::seal_shard`].
    pub async fn seal_shard_stream<R: AsyncRead + Unpin>(
        &self,
        codec: &ErasureCodec,
        identity: &ShardIdentity,
        mut encoded_shard: R,
    ) -> Result<ShardSealOutcome, ShardStoreError> {
        identity.validate_for(codec)?;
        let Some((manifest, start)) = self.prepare_shard_install(codec, identity).await? else {
            return Ok(ShardSealOutcome::AlreadyPresent);
        };
        let mut buffer = vec![0_u8; PAYLOAD_ARTIFACT_CHUNK_BYTES];
        let mut discard = u64::from(start) * PAYLOAD_ARTIFACT_CHUNK_BYTES as u64;
        while discard != 0 {
            let count =
                usize::try_from(discard.min(buffer.len() as u64)).map_err(shard_storage_error)?;
            encoded_shard
                .read_exact(&mut buffer[..count])
                .await
                .map_err(shard_storage_error)?;
            discard -= count as u64;
        }
        let total = artifact_chunk_count(&manifest);
        for ordinal in start..total {
            let expected = artifact_chunk_length(&manifest, ordinal)?;
            encoded_shard
                .read_exact(&mut buffer[..expected])
                .await
                .map_err(shard_storage_error)?;
            self.persist_shard_chunk(identity, &manifest, ordinal, &buffer[..expected])
                .await?;
        }
        let mut trailing = [0_u8; 1];
        if encoded_shard
            .read(&mut trailing)
            .await
            .map_err(shard_storage_error)?
            != 0
        {
            return Err(ShardStoreError::Storage(
                "encoded shard exceeds its exact expected length".into(),
            ));
        }
        self.finish_shard_install(codec, identity, &manifest)
            .await?;
        Ok(ShardSealOutcome::Created)
    }

    async fn prepare_shard_install(
        &self,
        codec: &ErasureCodec,
        identity: &ShardIdentity,
    ) -> Result<Option<(ArtifactManifest, u32)>, ShardStoreError> {
        if self
            .read_shard_manifest(identity)
            .map_err(shard_error)?
            .is_some()
        {
            self.validate_shard(codec, identity)?;
            let state = self.shard_reference_state(identity)?.ok_or_else(|| {
                ShardStoreError::Storage("shard manifest exists without lifecycle authority".into())
            })?;
            validate_blob_reference_state(state).map_err(shard_error)?;
            self.reserve_sealed_artifact_with_admission_wait(
                &identity.encode(),
                now_unix_millis().map_err(shard_error)?,
                SourceJournalAdmission::Bounded,
            )
            .await
            .map_err(shard_error)?;
            return Ok(None);
        }
        let encoded_length = codec.encoded_shard_length(identity.blob(), identity.ordinal())?;
        let manifest = ArtifactManifest::shard(identity, encoded_length).map_err(shard_error)?;
        let start = self
            .begin_sealed_artifact_install_with_admission_wait(
                &identity.encode(),
                manifest.clone(),
                now_unix_millis().map_err(shard_error)?,
                SourceJournalAdmission::Bounded,
            )
            .await
            .map_err(shard_error)?;
        Ok(Some((manifest, start)))
    }

    async fn persist_shard_chunk(
        &self,
        identity: &ShardIdentity,
        manifest: &ArtifactManifest,
        ordinal: u32,
        bytes: &[u8],
    ) -> Result<(), ShardStoreError> {
        let _guard = self.lock_commit("shard_install").await;
        self.advance_artifact_install(
            &identity.encode(),
            manifest,
            ordinal,
            bytes,
            now_unix_millis().map_err(shard_error)?,
            None,
        )
        .map_err(shard_error)
    }

    async fn finish_shard_install(
        &self,
        codec: &ErasureCodec,
        identity: &ShardIdentity,
        manifest: &ArtifactManifest,
    ) -> Result<(), ShardStoreError> {
        let mut validation = RocksArtifactReader::new(self.db.clone(), manifest.clone());
        if let Err(error) =
            codec.validate_shard(identity.blob(), identity.ordinal(), &mut validation)
        {
            let _guard = self.lock_commit("shard_install").await;
            let mut batch = WriteBatch::default();
            self.reset_artifact_install(
                &mut batch,
                &identity.encode(),
                manifest,
                now_unix_millis().map_err(shard_error)?,
            )
            .map_err(shard_error)?;
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db
                .write_opt(batch, &options)
                .map_err(shard_storage_error)?;
            return Err(error.into());
        }
        let _guard = self.lock_commit("shard_install").await;
        let mut batch = WriteBatch::default();
        self.finish_artifact_install(&mut batch, &identity.encode(), manifest)
            .map_err(shard_error)?;
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .write_opt(batch, &options)
            .map_err(shard_storage_error)
    }

    /// Validates the complete persisted shard identity and every inline CRC.
    pub fn validate_shard(
        &self,
        codec: &ErasureCodec,
        identity: &ShardIdentity,
    ) -> Result<(), ShardStoreError> {
        identity.validate_for(codec)?;
        let mut reader = self.open_shard_artifact(identity)?;
        codec
            .validate_shard(identity.blob(), identity.ordinal(), &mut reader)
            .map_err(Into::into)
    }

    /// Opens one complete shard only after validating its framing and CRCs.
    pub fn get_shard(
        &self,
        codec: &ErasureCodec,
        identity: &ShardIdentity,
    ) -> Result<ShardReader, ShardStoreError> {
        let state = self
            .shard_reference_state(identity)?
            .ok_or(ShardStoreError::NotFound)?;
        if state.ref_count == 0 {
            return Err(ShardStoreError::NotFound);
        }
        identity.validate_for(codec)?;
        let mut validation = self.open_shard_artifact(identity)?;
        codec.validate_shard(identity.blob(), identity.ordinal(), &mut validation)?;
        Ok(ShardReader {
            reader: self.open_shard_artifact(identity)?,
        })
    }

    pub fn shard_reference_state(
        &self,
        identity: &ShardIdentity,
    ) -> Result<Option<BlobReferenceState>, ShardStoreError> {
        self.read_blob_reference_state(&identity.encode())
            .map_err(shard_error)
    }

    pub(super) fn contains_shard_artifact(
        &self,
        identity: &ShardIdentity,
    ) -> Result<bool, ShardStoreError> {
        self.read_shard_manifest(identity)
            .map(|manifest| manifest.is_some())
            .map_err(shard_error)
    }

    fn open_shard_artifact(
        &self,
        identity: &ShardIdentity,
    ) -> Result<RocksArtifactReader, ShardStoreError> {
        let manifest = self
            .read_shard_manifest(identity)
            .map_err(shard_error)?
            .ok_or(ShardStoreError::NotFound)?;
        Ok(RocksArtifactReader::new(self.db.clone(), manifest))
    }

    /// Removes one local shard and its local lifecycle record.
    ///
    /// Placement and reference-delta callers decide when removal is safe; this
    /// byte-plane primitive deliberately has no cluster policy.
    pub async fn remove_shard(&self, identity: &ShardIdentity) -> Result<bool, ShardStoreError> {
        let had_state = {
            let _commit_guard = self.lock_commit("shard_state").await;
            let key = identity.encode();
            let state = self.read_blob_reference_state(&key).map_err(shard_error)?;
            let had_state = state.is_some();
            let manifest = self.read_shard_manifest(identity).map_err(shard_error)?;
            if let Some(state) = state {
                let mut batch = WriteBatch::default();
                if let Some(manifest) = manifest.as_ref() {
                    self.stage_artifact_delete(&mut batch, &key, manifest)
                        .map_err(shard_error)?;
                }
                self.stage_blob_reference_delete(&mut batch, &key, state)
                    .map_err(shard_error)?;
                let mut options = WriteOptions::default();
                options.set_sync(self.sync_writes);
                self.db
                    .write_opt(batch, &options)
                    .map_err(shard_storage_error)?;
            }
            had_state || manifest.is_some()
        };
        Ok(had_state)
    }
}

fn artifact_chunk_count(manifest: &ArtifactManifest) -> u32 {
    match manifest.layout {
        ArtifactLayout::Inline => 1,
        ArtifactLayout::Chunked { chunk_count } => chunk_count,
    }
}

fn artifact_chunk_length(
    manifest: &ArtifactManifest,
    ordinal: u32,
) -> Result<usize, ShardStoreError> {
    let offset = u64::from(ordinal) * PAYLOAD_ARTIFACT_CHUNK_BYTES as u64;
    usize::try_from(
        manifest
            .encoded_length
            .saturating_sub(offset)
            .min(PAYLOAD_ARTIFACT_CHUNK_BYTES as u64),
    )
    .map_err(shard_storage_error)
}

fn discard_exact(reader: &mut impl Read, mut bytes: u64) -> Result<(), ShardStoreError> {
    let mut buffer = vec![0_u8; PAYLOAD_ARTIFACT_CHUNK_BYTES];
    while bytes != 0 {
        let count = usize::try_from(bytes.min(buffer.len() as u64)).map_err(shard_storage_error)?;
        reader
            .read_exact(&mut buffer[..count])
            .map_err(shard_storage_error)?;
        bytes -= count as u64;
    }
    Ok(())
}

fn shard_error(error: MutationError) -> ShardStoreError {
    ShardStoreError::Storage(error.to_string())
}

fn shard_storage_error(error: impl std::fmt::Display) -> ShardStoreError {
    ShardStoreError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read};
    use std::path::Path;

    use super::*;
    use crate::{AWAITING_PUBLISH, ErasureProfile, StoreOptions};

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

    async fn open_store(root: &Path) -> Store {
        Store::open(StoreOptions::new(root, 1)).await.unwrap()
    }

    #[test]
    fn identity_encoding_is_exact_and_length_sensitive() {
        let reference = BlobRef {
            hash: [0xa5; 32],
            length: 73,
        };
        let identity = ShardIdentity::new(reference.clone(), 4);
        let encoded = identity.encode();
        assert_eq!(&encoded[..2], &FRAGMENT_FORMAT_VERSION.to_be_bytes());
        assert_eq!(&encoded[2..34], &reference.hash);
        assert_eq!(&encoded[34..42], &reference.length.to_be_bytes());
        assert_eq!(&encoded[42..], &4_u16.to_be_bytes());

        let different_length = ShardIdentity::new(
            BlobRef {
                length: reference.length + 1,
                ..reference
            },
            4,
        );
        assert_ne!(identity.encode(), different_length.encode());
    }

    #[tokio::test]
    async fn sealed_shard_and_lifecycle_survive_restart() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("store");
        let source = vec![0x5a; SMALL_BLOB_MAX_BYTES + 17];
        let (codec, reference, shards) = encoded_shards(&source);
        let identity = ShardIdentity::new(reference, 0);

        {
            let store = open_store(&root).await;
            assert_eq!(
                store
                    .seal_shard(&codec, &identity, Cursor::new(&shards[0]))
                    .await
                    .unwrap(),
                ShardSealOutcome::Created
            );
            let state = store.shard_reference_state(&identity).unwrap().unwrap();
            assert_eq!(state.ref_count, 1);
            assert_eq!(state.flags, AWAITING_PUBLISH);
        }

        let store = open_store(&root).await;
        let state = store.shard_reference_state(&identity).unwrap().unwrap();
        assert_eq!(state.ref_count, 1);
        assert_eq!(state.flags, AWAITING_PUBLISH);
        let mut reader = store.get_shard(&codec, &identity).unwrap();
        let mut persisted = Vec::new();
        reader.read_to_end(&mut persisted).unwrap();
        assert_eq!(persisted, shards[0]);
        assert_eq!(
            store
                .seal_shard(&codec, &identity, Cursor::new(&shards[0]))
                .await
                .unwrap(),
            ShardSealOutcome::AlreadyPresent
        );
        assert_eq!(
            store
                .shard_reference_state(&identity)
                .unwrap()
                .unwrap()
                .ref_count,
            1
        );
    }

    #[tokio::test]
    async fn erasure_shard_above_eight_mib_uses_the_same_chunked_artifact_store() {
        let temporary = tempfile::tempdir().unwrap();
        let store = open_store(temporary.path()).await;
        let source = vec![0x2c; 17 * 1024 * 1024];
        let (codec, reference, shards) = encoded_shards(&source);
        let identity = ShardIdentity::new(reference, 0);
        assert!(shards[0].len() > PAYLOAD_ARTIFACT_CHUNK_BYTES);

        store
            .seal_shard(&codec, &identity, Cursor::new(&shards[0]))
            .await
            .unwrap();

        let manifest = store.read_shard_manifest(&identity).unwrap().unwrap();
        assert!(matches!(
            manifest.layout,
            ArtifactLayout::Chunked { chunk_count } if chunk_count >= 2
        ));
        let mut persisted = Vec::new();
        store
            .get_shard(&codec, &identity)
            .unwrap()
            .read_to_end(&mut persisted)
            .unwrap();
        assert_eq!(persisted, shards[0]);
        assert!(store.remove_shard(&identity).await.unwrap());
        assert!(store.read_shard_manifest(&identity).unwrap().is_none());
    }

    #[tokio::test]
    async fn corrupted_existing_shard_is_rejected_not_replaced() {
        let temporary = tempfile::tempdir().unwrap();
        let store = open_store(temporary.path()).await;
        let source = vec![0x37; SMALL_BLOB_MAX_BYTES + 1];
        let (codec, reference, shards) = encoded_shards(&source);
        let identity = ShardIdentity::new(reference, 0);
        store
            .seal_shard(&codec, &identity, Cursor::new(&shards[0]))
            .await
            .unwrap();

        let mut corrupted = shards[0].clone();
        let last = corrupted.last_mut().unwrap();
        *last ^= 0xff;
        store
            .db
            .put_cf(
                store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(),
                shard_inline_key(&identity),
                corrupted,
            )
            .unwrap();

        assert!(matches!(
            store.validate_shard(&codec, &identity),
            Err(ShardStoreError::Erasure(
                ErasureError::ChunkChecksumMismatch { .. }
            ))
        ));
        assert!(
            store
                .seal_shard(&codec, &identity, Cursor::new(&shards[0]))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn malformed_first_install_resets_to_enumerable_retryable_state() {
        let temporary = tempfile::tempdir().unwrap();
        let store = open_store(temporary.path()).await;
        let source = vec![0x38; SMALL_BLOB_MAX_BYTES + 5];
        let (codec, reference, shards) = encoded_shards(&source);
        let identity = ShardIdentity::new(reference, 0);
        let mut malformed = shards[0].clone();
        *malformed.last_mut().unwrap() ^= 0xff;

        assert!(
            store
                .seal_shard(&codec, &identity, Cursor::new(malformed))
                .await
                .is_err()
        );
        assert!(store.shard_reference_state(&identity).unwrap().is_some());
        assert!(store.read_shard_manifest(&identity).unwrap().is_none());
        assert!(
            store
                .read_artifact_install_manifest(&identity.encode())
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .db
                .get_cf(
                    store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(),
                    shard_inline_key(&identity),
                )
                .unwrap()
                .is_none()
        );

        assert_eq!(
            store
                .seal_shard(&codec, &identity, Cursor::new(&shards[0]))
                .await
                .unwrap(),
            ShardSealOutcome::Created
        );
        store.validate_shard(&codec, &identity).unwrap();
    }

    #[tokio::test]
    async fn truncated_existing_shard_is_rejected() {
        let temporary = tempfile::tempdir().unwrap();
        let store = open_store(temporary.path()).await;
        let source = vec![0x91; SMALL_BLOB_MAX_BYTES + 3];
        let (codec, reference, shards) = encoded_shards(&source);
        let identity = ShardIdentity::new(reference, 1);
        store
            .seal_shard(&codec, &identity, Cursor::new(&shards[1]))
            .await
            .unwrap();

        store
            .db
            .put_cf(
                store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(),
                shard_inline_key(&identity),
                &shards[1][..shards[1].len() - 1],
            )
            .unwrap();

        assert!(matches!(
            store.validate_shard(&codec, &identity),
            Err(ShardStoreError::Erasure(ErasureError::Io(error)))
                if error.kind() == io::ErrorKind::InvalidData
        ));
        assert!(store.get_shard(&codec, &identity).is_err());
    }

    #[tokio::test]
    async fn shard_ordinals_have_separate_identity_and_storage() {
        let temporary = tempfile::tempdir().unwrap();
        let store = open_store(temporary.path()).await;
        let source = (0..SMALL_BLOB_MAX_BYTES + 29)
            .map(|index| index as u8)
            .collect::<Vec<_>>();
        let (codec, reference, shards) = encoded_shards(&source);
        let first = ShardIdentity::new(reference.clone(), 0);
        let second = ShardIdentity::new(reference, 1);
        assert_ne!(first.encode(), second.encode());
        assert_ne!(shard_inline_key(&first), shard_inline_key(&second));

        store
            .seal_shard(&codec, &first, Cursor::new(&shards[0]))
            .await
            .unwrap();
        store
            .seal_shard(&codec, &second, Cursor::new(&shards[1]))
            .await
            .unwrap();
        let mut first_bytes = Vec::new();
        store
            .get_shard(&codec, &first)
            .unwrap()
            .read_to_end(&mut first_bytes)
            .unwrap();
        let mut second_bytes = Vec::new();
        store
            .get_shard(&codec, &second)
            .unwrap()
            .read_to_end(&mut second_bytes)
            .unwrap();
        assert_eq!(first_bytes, shards[0]);
        assert_eq!(second_bytes, shards[1]);
    }

    #[tokio::test]
    async fn removal_clears_bytes_and_lifecycle_idempotently() {
        let temporary = tempfile::tempdir().unwrap();
        let store = open_store(temporary.path()).await;
        let source = vec![0x1d; SMALL_BLOB_MAX_BYTES + 9];
        let (codec, reference, shards) = encoded_shards(&source);
        let identity = ShardIdentity::new(reference, 2);
        store
            .seal_shard(&codec, &identity, Cursor::new(&shards[2]))
            .await
            .unwrap();

        assert!(store.remove_shard(&identity).await.unwrap());
        assert!(store.shard_reference_state(&identity).unwrap().is_none());
        assert!(matches!(
            store.get_shard(&codec, &identity),
            Err(ShardStoreError::NotFound)
        ));
        assert!(!store.remove_shard(&identity).await.unwrap());
    }

    #[tokio::test]
    async fn ordinary_age_gated_gc_removes_abandoned_shards() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_awaiting_publish_ttl_seconds(1),
        )
        .await
        .unwrap();
        let source = vec![0x64; SMALL_BLOB_MAX_BYTES + 11];
        let (codec, reference, shards) = encoded_shards(&source);
        let identity = ShardIdentity::new(reference, 0);
        store
            .seal_shard(&codec, &identity, Cursor::new(&shards[0]))
            .await
            .unwrap();
        let updated_at = store
            .shard_reference_state(&identity)
            .unwrap()
            .unwrap()
            .updated_at;

        assert_eq!(
            store
                .collect_blob_garbage_at(updated_at + 999)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .collect_blob_garbage_at(updated_at + 1_000)
                .await
                .unwrap(),
            1
        );
        assert!(store.shard_reference_state(&identity).unwrap().is_none());
        assert!(matches!(
            store.get_shard(&codec, &identity),
            Err(ShardStoreError::NotFound)
        ));
    }
}
