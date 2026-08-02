use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

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

/// The only durable lifecycle metadata kept for one sealed blob.
///
/// Timestamps are Unix milliseconds. A set [`AWAITING_PUBLISH`] bit means the
/// initial reference is a sealed-upload reservation rather than a published
/// immutable version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobReferenceState {
    pub ref_count: u64,
    pub flags: u8,
    pub created_at: u64,
    pub updated_at: u64,
}

pub const AWAITING_PUBLISH: u8 = 1;

#[derive(Clone, Debug)]
pub struct BlobStore {
    root: PathBuf,
    pub(crate) directory_lock: Arc<tokio::sync::Mutex<()>>,
}

pub struct BlobUpload {
    root: PathBuf,
    directory_lock: Arc<tokio::sync::Mutex<()>>,
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
    source: BlobReaderSource,
    reference: BlobRef,
    hasher: blake3::Hasher,
    position: u64,
    finished: bool,
}

enum BlobReaderSource {
    File(tokio::fs::File),
    Memory(Vec<u8>),
}

impl BlobStore {
    pub async fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        create_directory_all_durable(&root).await?;
        // Also fences a root or hash-prefix entry left visible but not
        // parent-synchronised by an older process before this store starts
        // acknowledging writes.
        sync_directory(parent_directory(&root)?).await?;
        sync_directory(&root).await?;
        Ok(Self {
            root,
            directory_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
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
            directory_lock: self.directory_lock.clone(),
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
        verify_open_blob(&mut file, reference).await?;
        file.seek(std::io::SeekFrom::Start(0)).await?;
        Ok(BlobReader {
            source: BlobReaderSource::File(file),
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

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn remove(&self, reference: &BlobRef) -> Result<()> {
        let path = self.path(&reference.hash);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let parent = path.parent().context("blob path has no parent")?;
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    }

    fn path(&self, hash: &[u8; 32]) -> PathBuf {
        let encoded = hex::encode(hash);
        self.root.join(&encoded[..2]).join(encoded)
    }
}

fn parent_directory(path: &Path) -> Result<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .or_else(|| (!path.is_absolute()).then_some(Path::new(".")))
        .or_else(|| path.is_absolute().then_some(path))
        .context("directory has no parent")
}

async fn sync_directory(path: &Path) -> Result<()> {
    tokio::fs::File::open(path).await?.sync_all().await?;
    Ok(())
}

/// Creates every missing component and synchronises the directory that names
/// it before moving on to the next component.
pub(crate) async fn create_directory_all_durable(path: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut current = path.to_path_buf();
    loop {
        match tokio::fs::metadata(&current).await {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => bail!("{} exists but is not a directory", current.display()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(current.clone());
                current = parent_directory(&current)?.to_path_buf();
            }
            Err(error) => return Err(error.into()),
        }
    }

    for directory in missing.into_iter().rev() {
        match tokio::fs::create_dir(&directory).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if !tokio::fs::metadata(&directory).await?.is_dir() {
                    bail!("{} exists but is not a directory", directory.display());
                }
            }
            Err(error) => return Err(error.into()),
        }
        sync_directory(parent_directory(&directory)?).await?;
    }
    Ok(())
}

async fn verify_open_blob(file: &mut tokio::fs::File, reference: &BlobRef) -> Result<()> {
    let metadata = file.metadata().await?;
    if !metadata.is_file() || metadata.len() != reference.length {
        bail!("blob failed length or hash verification");
    }
    file.seek(std::io::SeekFrom::Start(0)).await?;
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
    Ok(())
}

async fn verify_existing_blob(path: &Path, reference: &BlobRef) -> Result<()> {
    let mut file = tokio::fs::File::open(path).await?;
    verify_open_blob(&mut file, reference).await
}

impl BlobReader {
    pub(crate) fn from_bytes(reference: &BlobRef, bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() as u64 != reference.length
            || blake3::hash(&bytes).as_bytes() != &reference.hash
        {
            bail!("blob failed length or hash verification");
        }
        Ok(Self {
            source: BlobReaderSource::Memory(bytes),
            reference: reference.clone(),
            hasher: blake3::Hasher::new(),
            position: 0,
            finished: false,
        })
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
            BlobReaderSource::File(file) => {
                file.read_exact(&mut buffer[..read]).await?;
            }
            BlobReaderSource::Memory(bytes) => {
                let start = usize::try_from(self.position)
                    .context("blob position does not fit in memory")?;
                let end = start.checked_add(read).context("blob position overflow")?;
                buffer[..read].copy_from_slice(&bytes[start..end]);
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
        let trailing = match &mut self.source {
            BlobReaderSource::File(file) => {
                let mut trailing = [0_u8; 1];
                file.read(&mut trailing).await? != 0
            }
            BlobReaderSource::Memory(bytes) => bytes.len() as u64 != self.reference.length,
        };
        if trailing || self.hasher.finalize().as_bytes() != &self.reference.hash {
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
        {
            // A concurrent upload must not observe a newly created prefix and
            // publish into it before the creator synchronises the blob root.
            let _directory_guard = self.directory_lock.lock().await;
            create_directory_all_durable(parent).await?;
        }
        if tokio::fs::try_exists(&final_path).await? {
            verify_existing_blob(&final_path, &reference).await?;
            tokio::fs::remove_file(&self.temporary).await?;
        } else {
            match tokio::fs::rename(&self.temporary, &final_path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    verify_existing_blob(&final_path, &reference).await?;
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
        let _ = std::fs::remove_file(&self.temporary);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_and_first_put_establish_blob_directory_ancestry() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("nested").join("blobs");
        let store = BlobStore::open(&root).await.unwrap();
        let bytes = vec![0x3c; VERIFY_BUFFER_BYTES + 1];

        let reference = store.put(&bytes).await.unwrap();

        let encoded = hex::encode(reference.hash);
        assert!(root.is_dir());
        assert!(root.join(&encoded[..2]).is_dir());
        assert!(root.join(&encoded[..2]).join(encoded).is_file());
        assert_eq!(store.get(&reference).await.unwrap(), bytes);
    }

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
    async fn existing_identical_blob_is_verified_before_deduplication() {
        let temporary = tempfile::tempdir().unwrap();
        let store = BlobStore::open(temporary.path()).await.unwrap();
        let bytes = vec![0x4d; 2 * VERIFY_BUFFER_BYTES + 17];
        let first = store.put(&bytes).await.unwrap();

        let second = store.put(&bytes).await.unwrap();

        assert_eq!(second, first);
        assert_eq!(store.get(&second).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn existing_same_length_corruption_is_rejected_without_repair() {
        let temporary = tempfile::tempdir().unwrap();
        let store = BlobStore::open(temporary.path()).await.unwrap();
        let bytes = (0..(3 * VERIFY_BUFFER_BYTES + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let reference = store.put(&bytes).await.unwrap();
        let path = store.path(&reference.hash);
        let mut corrupted = bytes.clone();
        corrupted[VERIFY_BUFFER_BYTES + 3] ^= 0xff;
        tokio::fs::write(&path, &corrupted).await.unwrap();

        let error = store.put(&bytes).await.unwrap_err();

        assert!(error.to_string().contains("failed length or hash"));
        assert_eq!(tokio::fs::read(path).await.unwrap(), corrupted);
        assert_eq!(
            std::fs::read_dir(store.root().join(".staging"))
                .unwrap()
                .count(),
            0
        );
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
