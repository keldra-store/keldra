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
const UPLOAD_BOOT_NONCE_BYTES: usize = 16;
const UPLOAD_SPOOL_STARTUP_CLEANUP_FILES: usize = 256;
pub const DEFAULT_UPLOAD_SPOOL_MAX_BYTES: u64 = 16 * 1024 * 1024 * 1024;

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
pub struct BlobStore {
    root: PathBuf,
    pub(crate) directory_lock: Arc<tokio::sync::Mutex<()>>,
    upload_boot_nonce: [u8; UPLOAD_BOOT_NONCE_BYTES],
    upload_spool: PathBuf,
    upload_spool_budget: Arc<UploadSpoolBudget>,
}

pub struct BlobUpload {
    root: PathBuf,
    directory_lock: Arc<tokio::sync::Mutex<()>>,
    upload_boot_nonce: [u8; UPLOAD_BOOT_NONCE_BYTES],
    temporary: PathBuf,
    file: Option<tokio::fs::File>,
    hasher: blake3::Hasher,
    length: u64,
    spool_budget: Arc<UploadSpoolBudget>,
    spool_reserved_bytes: u64,
}

#[derive(Debug)]
struct UploadSpoolBudget {
    maximum_bytes: u64,
    used_bytes: AtomicU64,
}

impl UploadSpoolBudget {
    fn new(maximum_bytes: u64) -> Result<Self> {
        if maximum_bytes == 0 {
            bail!("upload spool byte limit must be non-zero");
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
                "upload spool byte limit exhausted: {used} bytes in use, {bytes} requested, {} maximum",
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
        debug_assert!(released.is_ok(), "upload spool reservation underflow");
    }

    #[cfg(test)]
    fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Acquire)
    }
}

/// A fully hashed and fsync'd blob which lives under the payload root's
/// `.staging` directory.
///
/// The store records its awaiting-publication lifecycle state before moving
/// these bytes to the canonical content-addressed path. There is deliberately
/// no drop cleanup: once the identity is known, bounded maintenance can either
/// recover a lifecycle-backed stage or age out an untracked crash orphan.
pub(crate) struct StagedBlob {
    reference: BlobRef,
    path: PathBuf,
}

impl StagedBlob {
    pub(crate) fn reference(&self) -> &BlobRef {
        &self.reference
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
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
        let upload_spool = root.join(".upload-spool");
        Self::open_with_upload_spool(root, upload_spool, DEFAULT_UPLOAD_SPOOL_MAX_BYTES).await
    }

    /// Open the durable payload root with a separate disposable upload spool.
    ///
    /// The byte limit is shared by every upload started by this store or one
    /// of its clones. A write which would exceed it aborts that upload instead
    /// of allowing concurrent streams to deadlock while each holds capacity.
    pub async fn open_with_upload_spool(
        root: impl AsRef<Path>,
        upload_spool: impl AsRef<Path>,
        upload_spool_max_bytes: u64,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let upload_spool = upload_spool.as_ref().to_path_buf();
        let mut upload_boot_nonce = [0_u8; UPLOAD_BOOT_NONCE_BYTES];
        getrandom::fill(&mut upload_boot_nonce)
            .map_err(|error| anyhow::anyhow!("generate blob upload boot nonce: {error}"))?;
        create_directory_all_durable(&root).await?;
        create_directory_all_durable(&upload_spool).await?;
        clear_abandoned_upload_spool_tick(&upload_spool).await?;
        // Also fences a root or hash-prefix entry left visible but not
        // parent-synchronised by an older process before this store starts
        // acknowledging writes.
        sync_directory(parent_directory(&root)?).await?;
        sync_directory(&root).await?;
        Ok(Self {
            root,
            directory_lock: Arc::new(tokio::sync::Mutex::new(())),
            upload_boot_nonce,
            upload_spool,
            upload_spool_budget: Arc::new(UploadSpoolBudget::new(upload_spool_max_bytes)?),
        })
    }

    pub async fn put(&self, bytes: &[u8]) -> Result<BlobRef> {
        let mut upload = self.begin_upload().await?;
        upload.write(bytes).await?;
        upload.finish().await
    }

    pub async fn begin_upload(&self) -> Result<BlobUpload> {
        let temporary = self.upload_spool.join(upload_staging_name(
            std::process::id(),
            &self.upload_boot_nonce,
            NEXT_UPLOAD_ID.fetch_add(1, Ordering::Relaxed),
        ));
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await
            .with_context(|| format!("create blob upload spool file {}", temporary.display()))?;
        Ok(BlobUpload {
            root: self.root.clone(),
            directory_lock: self.directory_lock.clone(),
            upload_boot_nonce: self.upload_boot_nonce,
            temporary,
            file: Some(file),
            hasher: blake3::Hasher::new(),
            length: 0,
            spool_budget: self.upload_spool_budget.clone(),
            spool_reserved_bytes: 0,
        })
    }

    pub(crate) fn upload_boot_nonce(&self) -> &[u8] {
        &self.upload_boot_nonce
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

    #[cfg(test)]
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

    pub(crate) fn path(&self, hash: &[u8; 32]) -> PathBuf {
        let encoded = hex::encode(hash);
        self.root.join(&encoded[..2]).join(encoded)
    }

    /// Publish one identified stage after its lifecycle reservation is durable.
    pub(crate) async fn publish_staged(&self, staged: StagedBlob) -> Result<BlobRef> {
        self.publish_identified_staging(&staged.path, &staged.reference)
            .await?;
        Ok(staged.reference)
    }

    /// Recover or finish publication of one lifecycle-backed identified stage.
    pub(crate) async fn publish_identified_staging(
        &self,
        staging_path: &Path,
        reference: &BlobRef,
    ) -> Result<()> {
        publish_identified_staging_at(&self.root, &self.directory_lock, staging_path, reference)
            .await
    }
}

async fn publish_identified_staging_at(
    root: &Path,
    directory_lock: &tokio::sync::Mutex<()>,
    staging_path: &Path,
    reference: &BlobRef,
) -> Result<()> {
    let encoded = hex::encode(reference.hash);
    let final_path = root.join(&encoded[..2]).join(encoded);
    let parent = final_path.parent().context("blob path has no parent")?;
    let staging_parent = staging_path
        .parent()
        .context("blob staging path has no parent")?;
    let _directory_guard = directory_lock.lock().await;
    create_directory_all_durable(parent).await?;
    if tokio::fs::try_exists(&final_path).await? {
        verify_existing_blob(&final_path, reference).await?;
        match tokio::fs::remove_file(staging_path).await {
            Ok(()) => sync_directory(staging_parent).await?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        return Ok(());
    }
    match tokio::fs::rename(staging_path, &final_path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A concurrent recovery may already have completed the same
            // immutable publication.
            verify_existing_blob(&final_path, reference).await?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    }
    sync_directory(parent).await?;
    sync_directory(staging_parent).await?;
    Ok(())
}

fn upload_staging_name(
    process_id: u32,
    boot_nonce: &[u8; UPLOAD_BOOT_NONCE_BYTES],
    upload_id: u64,
) -> String {
    format!(
        "upload-{process_id}-{}-{upload_id}.tmp",
        hex::encode(boot_nonce)
    )
}

fn identified_blob_staging_name(
    process_id: u32,
    boot_nonce: &[u8; UPLOAD_BOOT_NONCE_BYTES],
    upload_id: u64,
    reference: &BlobRef,
) -> String {
    format!(
        "blob-{process_id}-{}-{upload_id}-{}.tmp",
        hex::encode(boot_nonce),
        hex::encode(blob_identity_bytes(reference)),
    )
}

pub(crate) fn blob_reference_from_staging_name(name: &str) -> Option<BlobRef> {
    const IDENTITY_HEX_BYTES: usize = (32 + size_of::<u64>()) * 2;
    let body = name.strip_prefix("blob-")?.strip_suffix(".tmp")?;
    let fields = body.split('-').collect::<Vec<_>>();
    let [process_id, nonce, upload_id, identity] = fields.as_slice() else {
        return None;
    };
    if process_id.parse::<u32>().is_err()
        || nonce.len() != UPLOAD_BOOT_NONCE_BYTES * 2
        || !is_lower_hex(nonce)
        || upload_id.parse::<u64>().is_err()
        || identity.len() != IDENTITY_HEX_BYTES
        || !is_lower_hex(identity)
    {
        return None;
    }
    let mut encoded = [0_u8; 32 + size_of::<u64>()];
    hex::decode_to_slice(identity, &mut encoded).ok()?;
    Some(BlobRef {
        hash: encoded[..32].try_into().ok()?,
        length: u64::from_be_bytes(encoded[32..].try_into().ok()?),
    })
}

fn blob_identity_bytes(reference: &BlobRef) -> [u8; 32 + size_of::<u64>()] {
    let mut encoded = [0_u8; 32 + size_of::<u64>()];
    encoded[..32].copy_from_slice(&reference.hash);
    encoded[32..].copy_from_slice(&reference.length.to_be_bytes());
    encoded
}

fn is_upload_staging_name(name: &str) -> bool {
    let Some(body) = name
        .strip_prefix("upload-")
        .and_then(|name| name.strip_suffix(".tmp"))
    else {
        return false;
    };
    let fields = body.split('-').collect::<Vec<_>>();
    match fields.as_slice() {
        // 0.8.1 and earlier used only the process and in-process counter.
        [process_id, upload_id] => {
            process_id.parse::<u32>().is_ok() && upload_id.parse::<u64>().is_ok()
        }
        [process_id, nonce, upload_id] => {
            process_id.parse::<u32>().is_ok()
                && nonce.len() == UPLOAD_BOOT_NONCE_BYTES * 2
                && is_lower_hex(nonce)
                && upload_id.parse::<u64>().is_ok()
        }
        _ => false,
    }
}

async fn clear_abandoned_upload_spool_tick(upload_spool: &Path) -> Result<()> {
    let mut entries = tokio::fs::read_dir(upload_spool).await?;
    let mut removed = 0_usize;
    while removed < UPLOAD_SPOOL_STARTUP_CLEANUP_FILES {
        let Some(entry) = entries.next_entry().await? else {
            break;
        };
        let file_type = entry.file_type().await?;
        let name = entry.file_name();
        let name = name.to_str().with_context(|| {
            format!(
                "upload spool entry name is not UTF-8: {}",
                entry.path().display()
            )
        })?;
        if !file_type.is_file() || !is_upload_staging_name(name) {
            bail!(
                "upload spool contains an unexpected entry: {}",
                entry.path().display()
            );
        }
        tokio::fs::remove_file(entry.path()).await?;
        removed += 1;
    }
    if removed != 0 {
        sync_directory(upload_spool).await?;
    }
    if removed == UPLOAD_SPOOL_STARTUP_CLEANUP_FILES {
        tracing::warn!(
            cleanup.files = removed,
            cleanup.maximum_files = UPLOAD_SPOOL_STARTUP_CLEANUP_FILES,
            "bounded upload-spool startup cleanup reached its file limit; later restarts may remove additional abandoned uploads"
        );
    }
    Ok(())
}

#[cfg(test)]
fn is_shard_staging_name(name: &str) -> bool {
    const SHARD_IDENTITY_HEX_BYTES: usize = (2 + 32 + 8 + 2) * 2;
    let Some(body) = name
        .strip_prefix("shard-")
        .and_then(|name| name.strip_suffix(".tmp"))
    else {
        return false;
    };
    let fields = body.split('-').collect::<Vec<_>>();
    let valid_identity =
        |identity: &str| identity.len() == SHARD_IDENTITY_HEX_BYTES && is_lower_hex(identity);
    match fields.as_slice() {
        // 0.8.1 and earlier used only the process and in-process counter.
        [process_id, upload_id, identity] => {
            process_id.parse::<u32>().is_ok()
                && upload_id.parse::<u64>().is_ok()
                && valid_identity(identity)
        }
        [process_id, nonce, upload_id, identity] => {
            process_id.parse::<u32>().is_ok()
                && nonce.len() == UPLOAD_BOOT_NONCE_BYTES * 2
                && is_lower_hex(nonce)
                && upload_id.parse::<u64>().is_ok()
                && valid_identity(identity)
        }
        _ => false,
    }
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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
        if self.file.is_none() {
            bail!("blob upload is already finished");
        }
        let additional = u64::try_from(bytes.len()).context("blob upload chunk length overflow")?;
        let next_reserved = self
            .spool_reserved_bytes
            .checked_add(additional)
            .context("blob upload spool reservation overflow")?;
        let next_length = self
            .length
            .checked_add(additional)
            .context("blob length overflow")?;
        if let Err(error) = self.spool_budget.reserve(additional) {
            self.abort_spool().await;
            return Err(error);
        }
        self.spool_reserved_bytes = next_reserved;
        if let Err(error) = self
            .file
            .as_mut()
            .expect("blob upload file was checked")
            .write_all(bytes)
            .await
        {
            self.abort_spool().await;
            return Err(error.into());
        }
        self.hasher.update(bytes);
        self.length = next_length;
        Ok(())
    }

    pub async fn finish(mut self) -> Result<BlobRef> {
        let staged = self.finish_staged_inner(None).await?;
        publish_identified_staging_at(
            &self.root,
            &self.directory_lock,
            staged.path(),
            staged.reference(),
        )
        .await?;
        Ok(staged.reference)
    }

    /// Finish only if the streamed bytes have the caller's exact immutable
    /// identity. A mismatch removes the raw spool file without ever
    /// publishing it under a different hash.
    pub async fn finish_expected(mut self, expected: &BlobRef) -> Result<BlobRef> {
        let staged = self.finish_staged_inner(Some(expected)).await?;
        publish_identified_staging_at(
            &self.root,
            &self.directory_lock,
            staged.path(),
            staged.reference(),
        )
        .await?;
        Ok(staged.reference)
    }

    pub(crate) async fn finish_staged(mut self) -> Result<StagedBlob> {
        self.finish_staged_inner(None).await
    }

    pub(crate) async fn finish_staged_expected(mut self, expected: &BlobRef) -> Result<StagedBlob> {
        self.finish_staged_inner(Some(expected)).await
    }

    async fn finish_staged_inner(&mut self, expected: Option<&BlobRef>) -> Result<StagedBlob> {
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
        if expected.is_some_and(|expected| expected != &reference) {
            self.abort_spool().await;
            bail!("blob failed expected length or hash verification");
        }
        let staging = self.root.join(".staging");
        create_directory_all_durable(&staging).await?;
        let identified = staging.join(identified_blob_staging_name(
            std::process::id(),
            &self.upload_boot_nonce,
            NEXT_UPLOAD_ID.fetch_add(1, Ordering::Relaxed),
            &reference,
        ));
        self.move_to_identified_staging(&identified, &reference)
            .await?;
        Ok(StagedBlob {
            reference,
            path: identified,
        })
    }

    async fn move_to_identified_staging(
        &mut self,
        identified: &Path,
        reference: &BlobRef,
    ) -> Result<()> {
        let spool = self
            .temporary
            .parent()
            .context("blob upload spool path has no parent")?
            .to_path_buf();
        let staging = identified
            .parent()
            .context("identified blob staging path has no parent")?;
        match tokio::fs::rename(&self.temporary, identified).await {
            Ok(()) => {
                self.release_spool_reservation();
                sync_directory(staging).await?;
                sync_directory(&spool).await?;
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::CrossesDevices => {}
            Err(error) => return Err(error.into()),
        }

        let transfer = staging.join(format!(
            "copy-{}-{}-{}.tmp",
            std::process::id(),
            hex::encode(self.upload_boot_nonce),
            NEXT_UPLOAD_ID.fetch_add(1, Ordering::Relaxed),
        ));
        let copy_result = async {
            let mut source = tokio::fs::File::open(&self.temporary).await?;
            let mut destination = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&transfer)
                .await?;
            let copied = tokio::io::copy(&mut source, &mut destination).await?;
            if copied != reference.length {
                bail!("blob upload changed while moving into identified staging");
            }
            destination.sync_all().await?;
            drop(destination);
            verify_existing_blob(&transfer, reference).await?;
            tokio::fs::rename(&transfer, identified).await?;
            sync_directory(staging).await?;
            tokio::fs::remove_file(&self.temporary).await?;
            self.release_spool_reservation();
            sync_directory(&spool).await?;
            Result::<()>::Ok(())
        }
        .await;
        if copy_result.is_err() {
            let _ = tokio::fs::remove_file(&transfer).await;
        }
        copy_result
    }

    async fn abort_spool(&mut self) {
        self.file.take();
        match tokio::fs::remove_file(&self.temporary).await {
            Ok(()) => self.release_spool_reservation(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.release_spool_reservation();
            }
            Err(_) => {}
        }
    }

    fn release_spool_reservation(&mut self) {
        let bytes = std::mem::take(&mut self.spool_reserved_bytes);
        self.spool_budget.release(bytes);
    }
}

impl Drop for BlobUpload {
    fn drop(&mut self) {
        match std::fs::remove_file(&self.temporary) {
            Ok(()) => self.release_spool_reservation(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.release_spool_reservation();
            }
            Err(_) => {}
        }
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
        assert!(root.join(".upload-spool").is_dir());
        assert_eq!(
            std::fs::read_dir(root.join(".upload-spool"))
                .unwrap()
                .count(),
            0
        );
        assert_eq!(store.get(&reference).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn raw_upload_spool_is_separate_from_identified_staging() {
        let temporary = tempfile::tempdir().unwrap();
        let payload_root = temporary.path().join("payload");
        let upload_spool = temporary.path().join("upload-spool");
        let store = BlobStore::open_with_upload_spool(&payload_root, &upload_spool, 1024)
            .await
            .unwrap();
        let mut upload = store.begin_upload().await.unwrap();

        assert_eq!(upload.temporary.parent(), Some(upload_spool.as_path()));
        upload.write(b"identified after finish").await.unwrap();
        assert_eq!(store.upload_spool_budget.used_bytes(), 23);

        let staged = upload.finish_staged().await.unwrap();

        assert_eq!(
            staged.path().parent(),
            Some(payload_root.join(".staging").as_path())
        );
        assert!(staged.path().is_file());
        assert_eq!(store.upload_spool_budget.used_bytes(), 0);
        assert_eq!(std::fs::read_dir(upload_spool).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn upload_spool_limit_fails_and_releases_capacity_cleanly() {
        let temporary = tempfile::tempdir().unwrap();
        let payload_root = temporary.path().join("payload");
        let upload_spool = temporary.path().join("upload-spool");
        let store = BlobStore::open_with_upload_spool(&payload_root, &upload_spool, 8)
            .await
            .unwrap();
        let mut first = store.begin_upload().await.unwrap();
        first.write(b"12345678").await.unwrap();
        let mut blocked = store.begin_upload().await.unwrap();

        let error = blocked.write(b"x").await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("upload spool byte limit exhausted")
        );
        assert!(blocked.file.is_none());
        assert_eq!(store.upload_spool_budget.used_bytes(), 8);
        drop(first);
        assert_eq!(store.upload_spool_budget.used_bytes(), 0);

        let mut replacement = store.begin_upload().await.unwrap();
        replacement.write(b"12345678").await.unwrap();
        replacement.finish_staged().await.unwrap();
        assert_eq!(store.upload_spool_budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn opening_upload_spool_removes_only_recognised_crash_orphans() {
        let temporary = tempfile::tempdir().unwrap();
        let payload_root = temporary.path().join("payload");
        let upload_spool = temporary.path().join("upload-spool");
        tokio::fs::create_dir_all(&upload_spool).await.unwrap();
        tokio::fs::write(upload_spool.join("upload-1-1.tmp"), b"orphan")
            .await
            .unwrap();

        BlobStore::open_with_upload_spool(&payload_root, &upload_spool, 8)
            .await
            .unwrap();

        assert_eq!(std::fs::read_dir(upload_spool).unwrap().count(), 0);
    }

    #[test]
    fn upload_staging_names_are_unique_across_process_boots() {
        let first = upload_staging_name(1, &[0x11; UPLOAD_BOOT_NONCE_BYTES], 1);
        let second = upload_staging_name(1, &[0x22; UPLOAD_BOOT_NONCE_BYTES], 1);

        assert_ne!(first, second);
        assert!(is_upload_staging_name(&first));
        assert!(is_upload_staging_name(&second));
        assert!(is_upload_staging_name("upload-1-1.tmp"));
        assert!(!is_upload_staging_name("upload-1-invalid-1.tmp"));
        assert!(!is_upload_staging_name("shard-1-1-deadbeef.tmp"));
    }

    #[test]
    fn identified_blob_staging_name_round_trips_exact_identity() {
        let reference = BlobRef {
            hash: [0x7b; 32],
            length: 98_765,
        };
        let name =
            identified_blob_staging_name(7, &[0x31; UPLOAD_BOOT_NONCE_BYTES], 11, &reference);

        assert_eq!(blob_reference_from_staging_name(&name), Some(reference));
        assert!(blob_reference_from_staging_name("blob-invalid.tmp").is_none());
    }

    #[test]
    fn shard_staging_name_recognises_legacy_and_boot_nonce_formats() {
        let identity = "ab".repeat(2 + 32 + 8 + 2);

        assert!(is_shard_staging_name(&format!("shard-1-1-{identity}.tmp")));
        assert!(is_shard_staging_name(&format!(
            "shard-1-{}-1-{identity}.tmp",
            "cd".repeat(UPLOAD_BOOT_NONCE_BYTES)
        )));
        assert!(!is_shard_staging_name("shard-1-1-deadbeef.tmp"));
        assert!(!is_shard_staging_name(&format!(
            "shard-1-invalid-1-{identity}.tmp"
        )));
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
    async fn expected_identity_mismatch_never_leaves_a_published_blob() {
        let temporary = tempfile::tempdir().unwrap();
        let store = BlobStore::open(temporary.path()).await.unwrap();
        let expected_bytes = vec![0x2a; VERIFY_BUFFER_BYTES + 1];
        let actual_bytes = vec![0x7c; VERIFY_BUFFER_BYTES + 1];
        let expected = BlobRef {
            hash: *blake3::hash(&expected_bytes).as_bytes(),
            length: expected_bytes.len() as u64,
        };
        let actual = BlobRef {
            hash: *blake3::hash(&actual_bytes).as_bytes(),
            length: actual_bytes.len() as u64,
        };
        let mut upload = store.begin_upload().await.unwrap();
        upload.write(&actual_bytes).await.unwrap();

        assert!(upload.finish_expected(&expected).await.is_err());
        assert!(!store.contains(&expected).await.unwrap());
        assert!(!store.contains(&actual).await.unwrap());
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
            1
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
