use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncRead;

use super::*;
use crate::blob::create_directory_all_durable;
use crate::{ErasureCodec, ErasureError, FRAGMENT_FORMAT_VERSION};

const SHARD_IDENTITY_BYTES: usize = 2 + 32 + 8 + 2;
static NEXT_SHARD_UPLOAD_ID: AtomicU64 = AtomicU64::new(1);

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

    fn path(&self, root: &Path) -> PathBuf {
        let hash = hex::encode(self.blob.hash);
        root.join(&hash[..2]).join(hex::encode(self.encode()))
    }

    pub(crate) fn decode_file_name(hash_prefix: &str, name: &str) -> Result<Self, ShardStoreError> {
        if name.len() != SHARD_IDENTITY_BYTES * 2
            || !name.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(ShardStoreError::MalformedIdentity);
        }
        let mut encoded = [0_u8; SHARD_IDENTITY_BYTES];
        hex::decode_to_slice(name, &mut encoded).map_err(|_| ShardStoreError::MalformedIdentity)?;
        let identity = Self::decode(&encoded)?;
        let canonical = hex::encode(identity.encode());
        let blob_hash = hex::encode(identity.blob.hash);
        if canonical != name || !blob_hash.starts_with(hash_prefix) {
            return Err(ShardStoreError::MalformedIdentity);
        }
        Ok(identity)
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
    file: File,
}

impl Read for ShardReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.file.read(buffer)
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
    /// Retrying the same identity validates the existing immutable file and
    /// refreshes its one awaiting-publication reservation without increasing
    /// the reference count.
    pub async fn seal_shard<R: Read>(
        &self,
        codec: &ErasureCodec,
        identity: &ShardIdentity,
        mut encoded_shard: R,
    ) -> Result<ShardSealOutcome, ShardStoreError> {
        identity.validate_for(codec)?;
        let staging = self.blobs.root().join(".staging");
        create_directory_all_durable(&staging)
            .await
            .map_err(shard_storage_error)?;
        let temporary = staging.join(format!(
            "shard-{}-{}-{}.tmp",
            std::process::id(),
            NEXT_SHARD_UPLOAD_ID.fetch_add(1, Ordering::Relaxed),
            hex::encode(identity.encode())
        ));
        let temporary_guard = TemporaryShard::new(temporary.clone());
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(shard_storage_error)?;
        io::copy(&mut encoded_shard, &mut output).map_err(shard_storage_error)?;
        output.sync_all().map_err(shard_storage_error)?;
        drop(output);
        validate_shard_file(codec, identity, &temporary)?;

        self.commit_staged_shard(codec, identity, &staging, &temporary, temporary_guard)
            .await
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
        let staging = self.blobs.root().join(".staging");
        create_directory_all_durable(&staging)
            .await
            .map_err(shard_storage_error)?;
        let temporary = staging.join(format!(
            "shard-{}-{}-{}.tmp",
            std::process::id(),
            NEXT_SHARD_UPLOAD_ID.fetch_add(1, Ordering::Relaxed),
            hex::encode(identity.encode())
        ));
        let temporary_guard = TemporaryShard::new(temporary.clone());
        let mut output = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(shard_storage_error)?;
        tokio::io::copy(&mut encoded_shard, &mut output)
            .await
            .map_err(shard_storage_error)?;
        output.sync_all().await.map_err(shard_storage_error)?;
        drop(output);
        validate_shard_file(codec, identity, &temporary)?;

        self.commit_staged_shard(codec, identity, &staging, &temporary, temporary_guard)
            .await
    }

    async fn commit_staged_shard(
        &self,
        codec: &ErasureCodec,
        identity: &ShardIdentity,
        staging: &Path,
        temporary: &Path,
        temporary_guard: TemporaryShard,
    ) -> Result<ShardSealOutcome, ShardStoreError> {
        let final_path = identity.path(self.blobs.root());
        let parent = final_path
            .parent()
            .ok_or_else(|| ShardStoreError::Storage("shard path has no parent".into()))?;
        let created;
        {
            let _directory_guard = self.blobs.directory_lock.lock().await;
            create_directory_all_durable(parent)
                .await
                .map_err(shard_storage_error)?;
            if final_path.exists() {
                validate_shard_file(codec, identity, &final_path)?;
                created = false;
            } else {
                std::fs::rename(&temporary, &final_path).map_err(shard_storage_error)?;
                sync_directory(parent)?;
                sync_directory(&staging)?;
                created = true;
            }
        }

        loop {
            let commit_guard = self.commit_lock.lock().await;
            if !final_path.is_file() {
                return Err(ShardStoreError::NotFound);
            }
            let reservation = self.reserve_sealed_artifact(
                &identity.encode(),
                now_unix_millis().map_err(shard_error)?,
            );
            drop(commit_guard);
            match reservation {
                Ok(_) => break,
                Err(MutationError::SourceJournalCapacity) => {
                    self.wait_for_mutation_capacity().await;
                }
                Err(error) => return Err(shard_error(error)),
            }
        }
        drop(temporary_guard);
        Ok(if created {
            ShardSealOutcome::Created
        } else {
            ShardSealOutcome::AlreadyPresent
        })
    }

    /// Validates the complete persisted shard identity and every inline CRC.
    pub fn validate_shard(
        &self,
        codec: &ErasureCodec,
        identity: &ShardIdentity,
    ) -> Result<(), ShardStoreError> {
        identity.validate_for(codec)?;
        validate_shard_file(codec, identity, &identity.path(self.blobs.root()))
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
        let path = identity.path(self.blobs.root());
        let mut file = open_shard_file(&path)?;
        codec.validate_shard(identity.blob(), identity.ordinal(), &mut file)?;
        file.seek(SeekFrom::Start(0)).map_err(shard_storage_error)?;
        Ok(ShardReader { file })
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
        match std::fs::symlink_metadata(identity.path(self.blobs.root())) {
            Ok(metadata) => Ok(metadata.file_type().is_file()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(shard_storage_error(error)),
        }
    }

    /// Removes one local shard and its local lifecycle record.
    ///
    /// Placement and reference-delta callers decide when removal is safe; this
    /// byte-plane primitive deliberately has no cluster policy.
    pub async fn remove_shard(&self, identity: &ShardIdentity) -> Result<bool, ShardStoreError> {
        let _commit_guard = self.commit_lock.lock().await;
        let key = identity.encode();
        let had_state = self
            .read_blob_reference_state(&key)
            .map_err(shard_error)?
            .is_some();
        if had_state {
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db
                .delete_cf_opt(
                    self.cf(CF_BLOB_REFERENCES).map_err(shard_error)?,
                    key,
                    &options,
                )
                .map_err(shard_storage_error)?;
        }
        let removed = remove_shard_file(identity, self.blobs.root())?;
        Ok(had_state || removed)
    }

    pub(super) fn remove_shard_file(
        &self,
        identity: &ShardIdentity,
    ) -> Result<bool, MutationError> {
        remove_shard_file(identity, self.blobs.root()).map_err(storage_error)
    }
}

fn open_shard_file(path: &Path) -> Result<File, ShardStoreError> {
    match File::open(path) {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Err(ShardStoreError::NotFound),
        Err(error) => Err(shard_storage_error(error)),
    }
}

fn validate_shard_file(
    codec: &ErasureCodec,
    identity: &ShardIdentity,
    path: &Path,
) -> Result<(), ShardStoreError> {
    let file = open_shard_file(path)?;
    codec
        .validate_shard(identity.blob(), identity.ordinal(), file)
        .map_err(Into::into)
}

fn remove_shard_file(identity: &ShardIdentity, root: &Path) -> Result<bool, ShardStoreError> {
    let path = identity.path(root);
    match std::fs::remove_file(&path) {
        Ok(()) => {
            let parent = path
                .parent()
                .ok_or_else(|| ShardStoreError::Storage("shard path has no parent".into()))?;
            sync_directory(parent)?;
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(shard_storage_error(error)),
    }
}

fn sync_directory(path: &Path) -> Result<(), ShardStoreError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(shard_storage_error)
}

fn shard_error(error: MutationError) -> ShardStoreError {
    ShardStoreError::Storage(error.to_string())
}

fn shard_storage_error(error: impl std::fmt::Display) -> ShardStoreError {
    ShardStoreError::Storage(error.to_string())
}

struct TemporaryShard {
    path: PathBuf,
}

impl TemporaryShard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for TemporaryShard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read};

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

        let path = identity.path(store.blobs.root());
        let mut corrupted = std::fs::read(&path).unwrap();
        let last = corrupted.last_mut().unwrap();
        *last ^= 0xff;
        std::fs::write(&path, corrupted).unwrap();

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

        let path = identity.path(store.blobs.root());
        let file = OpenOptions::new().write(true).open(path).unwrap();
        file.set_len(shards[1].len() as u64 - 1).unwrap();
        file.sync_all().unwrap();

        assert!(matches!(
            store.validate_shard(&codec, &identity),
            Err(ShardStoreError::Erasure(ErasureError::Io(error)))
                if error.kind() == io::ErrorKind::UnexpectedEof
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
        assert_ne!(
            first.path(store.blobs.root()),
            second.path(store.blobs.root())
        );

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
