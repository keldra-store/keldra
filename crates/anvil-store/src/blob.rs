use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

static NEXT_UPLOAD_ID: AtomicU64 = AtomicU64::new(1);
const VERIFY_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    pub hash: [u8; 32],
    pub length: u64,
}

#[derive(Clone, Debug)]
pub struct BlobStore {
    root: PathBuf,
}

pub struct BlobUpload {
    root: PathBuf,
    temporary: PathBuf,
    file: Option<tokio::fs::File>,
    hasher: blake3::Hasher,
    length: u64,
}

/// A verified, bounded-memory reader for one immutable published blob.
///
/// [`BlobStore::open_verified`] validates the complete file before returning
/// this reader. Reads hash the file again so an unexpected mutation of a
/// published blob is still detected while it is being consumed.
pub struct BlobReader {
    file: tokio::fs::File,
    reference: BlobRef,
    hasher: blake3::Hasher,
    position: u64,
    finished: bool,
}

impl BlobStore {
    pub async fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&root).await?;
        Ok(Self { root })
    }

    pub async fn put(&self, bytes: &[u8]) -> Result<BlobRef> {
        let mut upload = self.begin_upload().await?;
        upload.write(bytes).await?;
        upload.finish().await
    }

    pub async fn begin_upload(&self) -> Result<BlobUpload> {
        let staging = self.root.join(".staging");
        tokio::fs::create_dir_all(&staging).await?;
        let temporary = staging.join(format!(
            "upload-{}-{}.tmp",
            std::process::id(),
            NEXT_UPLOAD_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await
            .with_context(|| format!("create blob staging file {}", temporary.display()))?;
        Ok(BlobUpload {
            root: self.root.clone(),
            temporary,
            file: Some(file),
            hasher: blake3::Hasher::new(),
            length: 0,
        })
    }

    pub async fn get(&self, reference: &BlobRef) -> Result<Vec<u8>> {
        let bytes = tokio::fs::read(self.path(&reference.hash)).await?;
        if bytes.len() as u64 != reference.length
            || blake3::hash(&bytes).as_bytes() != &reference.hash
        {
            bail!("blob failed length or hash verification");
        }
        Ok(bytes)
    }

    /// Opens a published blob after verifying its length and content hash.
    ///
    /// Verification reads through a fixed-size buffer and then rewinds the
    /// same file handle. The blob store never modifies a published file, so a
    /// caller can subsequently stream it without retaining the whole value in
    /// memory.
    pub async fn open_verified(&self, reference: &BlobRef) -> Result<BlobReader> {
        let mut file = tokio::fs::File::open(self.path(&reference.hash)).await?;
        let mut hasher = blake3::Hasher::new();
        let mut length = 0_u64;
        let mut buffer = vec![0_u8; VERIFY_BUFFER_BYTES];
        loop {
            let read = file.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            length = length
                .checked_add(read as u64)
                .context("blob length overflow")?;
            hasher.update(&buffer[..read]);
        }
        if length != reference.length || hasher.finalize().as_bytes() != &reference.hash {
            bail!("blob failed length or hash verification");
        }
        file.seek(std::io::SeekFrom::Start(0)).await?;
        Ok(BlobReader {
            file,
            reference: reference.clone(),
            hasher: blake3::Hasher::new(),
            position: 0,
            finished: false,
        })
    }

    pub async fn contains(&self, reference: &BlobRef) -> Result<bool> {
        match tokio::fs::metadata(self.path(&reference.hash)).await {
            Ok(metadata) => Ok(metadata.is_file() && metadata.len() == reference.length),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn path(&self, hash: &[u8; 32]) -> PathBuf {
        let encoded = hex::encode(hash);
        self.root.join(&encoded[..2]).join(encoded)
    }
}

impl BlobReader {
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
        self.file.read_exact(&mut buffer[..read]).await?;
        self.hasher.update(&buffer[..read]);
        self.position += read as u64;
        if self.position == self.reference.length {
            self.finish().await?;
        }
        Ok(read)
    }

    async fn finish(&mut self) -> Result<()> {
        let mut trailing = [0_u8; 1];
        if self.file.read(&mut trailing).await? != 0
            || self.hasher.finalize().as_bytes() != &self.reference.hash
        {
            bail!("blob changed after verification");
        }
        self.finished = true;
        Ok(())
    }
}

impl BlobUpload {
    pub async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        let file = self
            .file
            .as_mut()
            .context("blob upload is already finished")?;
        file.write_all(bytes).await?;
        self.hasher.update(bytes);
        self.length = self
            .length
            .checked_add(bytes.len() as u64)
            .context("blob length overflow")?;
        Ok(())
    }

    pub async fn finish(mut self) -> Result<BlobRef> {
        let file = self
            .file
            .take()
            .context("blob upload is already finished")?;
        file.sync_all().await?;
        drop(file);
        let reference = BlobRef {
            hash: *self.hasher.finalize().as_bytes(),
            length: self.length,
        };
        let encoded = hex::encode(reference.hash);
        let final_path = self.root.join(&encoded[..2]).join(encoded);
        let parent = final_path.parent().context("blob path has no parent")?;
        tokio::fs::create_dir_all(parent).await?;
        if tokio::fs::try_exists(&final_path).await? {
            tokio::fs::remove_file(&self.temporary).await?;
        } else {
            match tokio::fs::rename(&self.temporary, &final_path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    tokio::fs::remove_file(&self.temporary).await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        tokio::fs::File::open(parent).await?.sync_all().await?;
        Ok(reference)
    }
}

impl Drop for BlobUpload {
    fn drop(&mut self) {
        if self.file.is_some() {
            let _ = std::fs::remove_file(&self.temporary);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn concurrent_identical_uploads_publish_one_verified_blob() {
        let temporary = tempfile::tempdir().unwrap();
        let store = BlobStore::open(temporary.path()).await.unwrap();
        let left_store = store.clone();
        let right_store = store.clone();
        let bytes = vec![42u8; 128 * 1024];
        let left_bytes = bytes.clone();
        let right_bytes = bytes.clone();
        let (left, right) =
            tokio::join!(left_store.put(&left_bytes), right_store.put(&right_bytes));
        let left = left.unwrap();
        let right = right.unwrap();
        assert_eq!(left, right);
        assert_eq!(store.get(&left).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn verified_reader_streams_large_blob_in_caller_bounded_chunks() {
        let temporary = tempfile::tempdir().unwrap();
        let store = BlobStore::open(temporary.path()).await.unwrap();
        let bytes = (0..(3 * VERIFY_BUFFER_BYTES + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let reference = store.put(&bytes).await.unwrap();
        let mut reader = store.open_verified(&reference).await.unwrap();
        let mut buffer = vec![0_u8; 7 * 1024];
        let mut actual = Vec::new();

        loop {
            let read = reader.read(&mut buffer).await.unwrap();
            if read == 0 {
                break;
            }
            assert!(read <= buffer.len());
            actual.extend_from_slice(&buffer[..read]);
        }

        assert_eq!(actual, bytes);
        assert_eq!(reader.read(&mut buffer).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn verified_reader_rejects_corruption_before_returning_a_reader() {
        let temporary = tempfile::tempdir().unwrap();
        let store = BlobStore::open(temporary.path()).await.unwrap();
        let reference = store.put(b"original bytes").await.unwrap();
        tokio::fs::write(store.path(&reference.hash), b"corrupted data")
            .await
            .unwrap();

        let error = match store.open_verified(&reference).await {
            Ok(_) => panic!("corrupted blob unexpectedly passed verification"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("failed length or hash"));
    }
}
