use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::store::{PAYLOAD_ARTIFACT_CHUNK_BYTES, Store};

pub const DEFAULT_PENDING_UPLOAD_MAX_BYTES: u64 = 16 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    pub hash: [u8; 32],
    pub length: u64,
}

/// The only durable lifecycle metadata kept for one sealed blob.
///
/// Timestamps are Unix milliseconds. A set [`AWAITING_PUBLISH`] bit means the
/// initial reference is a sealed-upload reservation rather than a published
/// immutable version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobReferenceState {
    pub ref_count: u64,
    pub flags: u8,
    pub created_at: u64,
    pub updated_at: u64,
}

pub const AWAITING_PUBLISH: u8 = 1;

#[derive(Clone, Debug)]
pub(crate) struct BlobStore {
    pending_upload_budget: Arc<PendingUploadBudget>,
}

pub struct BlobUpload {
    store: Store,
    upload_id: [u8; 32],
    buffer: Vec<u8>,
    hasher: blake3::Hasher,
    length: u64,
    persisted_chunks: u32,
    finished: bool,
    pending_upload_budget: Arc<PendingUploadBudget>,
    reserved_bytes: u64,
}

#[derive(Debug)]
struct PendingUploadBudget {
    maximum_bytes: u64,
    used_bytes: AtomicU64,
}

impl PendingUploadBudget {
    fn new(maximum_bytes: u64) -> Result<Self> {
        if maximum_bytes == 0 {
            bail!("pending upload byte limit must be non-zero");
        }
        Ok(Self {
            maximum_bytes,
            used_bytes: AtomicU64::new(0),
        })
    }

    fn reserve(&self, bytes: u64) -> Result<()> {
        let result = self
            .used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.maximum_bytes)
            });
        if let Err(used) = result {
            bail!(
                "pending upload byte limit exhausted: {used} bytes in use, {bytes} requested, {} maximum",
                self.maximum_bytes
            );
        }
        Ok(())
    }

    fn release(&self, bytes: u64) {
        let released = self
            .used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_sub(bytes)
            });
        debug_assert!(released.is_ok(), "pending upload reservation underflow");
    }

    #[cfg(test)]
    fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Acquire)
    }
}

/// A fully hashed upload whose complete chunks are already lifecycle-owned in
/// RocksDB and whose final partial chunk remains in bounded memory.
pub(crate) struct StagedBlob {
    reference: BlobRef,
    upload_id: [u8; 32],
    final_chunk: Vec<u8>,
    persisted_chunks: u32,
    pending_upload_budget: Arc<PendingUploadBudget>,
    reserved_bytes: u64,
}

impl Drop for StagedBlob {
    fn drop(&mut self) {
        self.pending_upload_budget.release(self.reserved_bytes);
        self.reserved_bytes = 0;
    }
}

impl StagedBlob {
    pub(crate) fn reference(&self) -> &BlobRef {
        &self.reference
    }

    pub(crate) const fn upload_id(&self) -> [u8; 32] {
        self.upload_id
    }

    pub(crate) fn final_chunk(&self) -> &[u8] {
        &self.final_chunk
    }

    pub(crate) const fn persisted_chunks(&self) -> u32 {
        self.persisted_chunks
    }
}

/// A verified, bounded-memory reader for one immutable published blob.
///
/// Reads hash the immutable RocksDB artifact again so an unexpected mutation
/// is detected while it is being consumed.
pub struct BlobReader {
    source: BlobReaderSource,
    reference: BlobRef,
    hasher: blake3::Hasher,
    position: u64,
    finished: bool,
}

enum BlobReaderSource {
    RocksDb(crate::store::payload_artifacts::RocksArtifactReader),
}

impl BlobStore {
    /// Construct the process-wide admission bound for active uploads. Durable
    /// chunks are owned by RocksDB installation records and therefore need no
    /// filesystem spool or startup directory sweep.
    pub fn new(pending_upload_max_bytes: u64) -> Result<Self> {
        Ok(Self {
            pending_upload_budget: Arc::new(PendingUploadBudget::new(pending_upload_max_bytes)?),
        })
    }

    pub fn begin_upload(&self, store: Store) -> Result<BlobUpload> {
        let mut upload_id = [0_u8; 32];
        getrandom::fill(&mut upload_id)
            .map_err(|error| anyhow::anyhow!("generate payload upload identity: {error}"))?;
        Ok(BlobUpload {
            store,
            upload_id,
            buffer: Vec::with_capacity(PAYLOAD_ARTIFACT_CHUNK_BYTES),
            hasher: blake3::Hasher::new(),
            length: 0,
            persisted_chunks: 0,
            finished: false,
            pending_upload_budget: self.pending_upload_budget.clone(),
            reserved_bytes: 0,
        })
    }
}

impl BlobReader {
    pub(crate) fn from_rocksdb(
        reference: &BlobRef,
        reader: crate::store::payload_artifacts::RocksArtifactReader,
    ) -> Self {
        Self {
            source: BlobReaderSource::RocksDb(reader),
            reference: reference.clone(),
            hasher: blake3::Hasher::new(),
            position: 0,
            finished: false,
        }
    }

    /// Reads at most `buffer.len()` verified blob bytes.
    ///
    /// A return value of zero means the complete blob has been read. The
    /// caller controls the memory bound by choosing the buffer size.
    pub async fn read(&mut self, buffer: &mut [u8]) -> Result<usize> {
        if buffer.is_empty() {
            bail!("blob read buffer must not be empty");
        }
        if self.finished {
            return Ok(0);
        }

        let remaining = self
            .reference
            .length
            .checked_sub(self.position)
            .context("blob reader advanced beyond the expected length")?;
        if remaining == 0 {
            self.finish().await?;
            return Ok(0);
        }

        let read = usize::try_from(remaining.min(buffer.len() as u64))
            .context("blob chunk length does not fit in memory")?;
        match &mut self.source {
            BlobReaderSource::RocksDb(reader) => {
                use std::io::Read;
                reader.read_exact(&mut buffer[..read])?;
            }
        }
        self.hasher.update(&buffer[..read]);
        self.position += read as u64;
        if self.position == self.reference.length {
            self.finish().await?;
        }
        Ok(read)
    }

    async fn finish(&mut self) -> Result<()> {
        if self.hasher.finalize().as_bytes() != &self.reference.hash {
            bail!("blob changed after verification");
        }
        self.finished = true;
        Ok(())
    }
}

impl BlobUpload {
    pub async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        if self.finished {
            bail!("blob upload is already finished");
        }
        let additional = u64::try_from(bytes.len()).context("blob upload chunk length overflow")?;
        let next_reserved = self
            .reserved_bytes
            .checked_add(additional)
            .context("pending upload reservation overflow")?;
        let next_length = self
            .length
            .checked_add(additional)
            .context("blob length overflow")?;
        if let Err(error) = self.pending_upload_budget.reserve(additional) {
            self.finished = true;
            return Err(error);
        }
        self.reserved_bytes = next_reserved;
        self.hasher.update(bytes);
        self.length = next_length;
        let mut remaining = bytes;
        while !remaining.is_empty() {
            let available = PAYLOAD_ARTIFACT_CHUNK_BYTES - self.buffer.len();
            let take = available.min(remaining.len());
            self.buffer.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];
            if self.buffer.len() == PAYLOAD_ARTIFACT_CHUNK_BYTES {
                let chunk = std::mem::replace(
                    &mut self.buffer,
                    Vec::with_capacity(PAYLOAD_ARTIFACT_CHUNK_BYTES),
                );
                if let Err(error) = self
                    .store
                    .persist_pending_upload_chunk(self.upload_id, self.persisted_chunks, chunk)
                    .await
                {
                    self.finished = true;
                    return Err(anyhow::anyhow!(error.to_string()));
                }
                self.persisted_chunks = self
                    .persisted_chunks
                    .checked_add(1)
                    .context("payload upload chunk ordinal overflow")?;
            }
        }
        Ok(())
    }

    /// Finish only if the streamed bytes have the caller's exact immutable
    /// identity. A mismatch leaves any flushed chunks under their temporary
    /// upload identity for bounded garbage collection and never publishes
    /// them under a different hash.
    pub(crate) async fn finish_staged(mut self) -> Result<StagedBlob> {
        self.finish_staged_inner(None).await
    }

    pub(crate) async fn finish_staged_expected(mut self, expected: &BlobRef) -> Result<StagedBlob> {
        self.finish_staged_inner(Some(expected)).await
    }

    async fn finish_staged_inner(&mut self, expected: Option<&BlobRef>) -> Result<StagedBlob> {
        if self.finished {
            bail!("blob upload is already finished");
        }
        self.finished = true;
        let reference = BlobRef {
            hash: *self.hasher.finalize().as_bytes(),
            length: self.length,
        };
        if expected.is_some_and(|expected| expected != &reference) {
            bail!("blob failed expected length or hash verification");
        }
        let final_chunk = std::mem::take(&mut self.buffer);
        let reserved_bytes = std::mem::take(&mut self.reserved_bytes);
        Ok(StagedBlob {
            reference,
            upload_id: self.upload_id,
            final_chunk,
            persisted_chunks: self.persisted_chunks,
            pending_upload_budget: self.pending_upload_budget.clone(),
            reserved_bytes,
        })
    }
}

impl Drop for BlobUpload {
    fn drop(&mut self) {
        let bytes = std::mem::take(&mut self.reserved_bytes);
        self.pending_upload_budget.release(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StoreOptions;

    #[tokio::test]
    async fn finished_upload_uses_no_filesystem_spool_and_releases_admission() {
        let temporary = tempfile::tempdir().unwrap();
        let store =
            Store::open(StoreOptions::new(temporary.path(), 1).with_pending_upload_max_bytes(1024))
                .await
                .unwrap();
        let mut upload = store.begin_blob_upload().await.unwrap();

        upload.write(b"identified after finish").await.unwrap();
        assert_eq!(store.blobs.pending_upload_budget.used_bytes(), 23);

        store.seal_blob_upload(upload).await.unwrap();
        assert_eq!(store.blobs.pending_upload_budget.used_bytes(), 0);
        assert!(!temporary.path().join("blobs/.upload-spool").exists());
    }

    #[tokio::test]
    async fn pending_upload_limit_fails_and_releases_capacity_cleanly() {
        let temporary = tempfile::tempdir().unwrap();
        let store =
            Store::open(StoreOptions::new(temporary.path(), 1).with_pending_upload_max_bytes(8))
                .await
                .unwrap();
        let mut first = store.begin_blob_upload().await.unwrap();
        first.write(b"12345678").await.unwrap();
        let mut blocked = store.begin_blob_upload().await.unwrap();

        let error = blocked.write(b"x").await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("pending upload byte limit exhausted")
        );
        assert_eq!(store.blobs.pending_upload_budget.used_bytes(), 8);
        drop(first);
        assert_eq!(store.blobs.pending_upload_budget.used_bytes(), 0);

        let mut replacement = store.begin_blob_upload().await.unwrap();
        replacement.write(b"12345678").await.unwrap();
        drop(replacement.finish_staged().await.unwrap());
        assert_eq!(store.blobs.pending_upload_budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn abandoned_flushed_upload_chunks_are_removed_by_blob_gc() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1)
                .with_pending_upload_max_bytes((PAYLOAD_ARTIFACT_CHUNK_BYTES * 2) as u64)
                .with_awaiting_publish_ttl_seconds(1),
        )
        .await
        .unwrap();
        let mut upload = store.begin_blob_upload().await.unwrap();
        let upload_id = upload.upload_id;
        upload
            .write(&vec![0x42; PAYLOAD_ARTIFACT_CHUNK_BYTES])
            .await
            .unwrap();
        assert!(store.has_pending_upload_install(&upload_id).unwrap());
        drop(upload);

        assert_eq!(store.collect_blob_garbage_at(u64::MAX).await.unwrap(), 1);
        assert!(!store.has_pending_upload_install(&upload_id).unwrap());
        assert!(!temporary.path().join("blobs/.upload-spool").exists());
    }

    #[tokio::test]
    async fn multi_chunk_upload_is_readable_without_a_filesystem_payload_file() {
        let temporary = tempfile::tempdir().unwrap();
        let length = PAYLOAD_ARTIFACT_CHUNK_BYTES + 31;
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1)
                .with_pending_upload_max_bytes((length * 2) as u64),
        )
        .await
        .unwrap();
        let bytes = vec![0x5d; length];
        let reference = store.stage_blob(&bytes).await.unwrap();

        assert_eq!(store.read_blob_bytes(&reference).await.unwrap(), bytes);
        assert!(!temporary.path().join("blobs/.upload-spool").exists());
    }
}
