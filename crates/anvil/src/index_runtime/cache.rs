//! Shared, disposable local materialisation of immutable index segments.
//!
//! Authoritative bytes always remain ordinary Anvil objects. Cache files and
//! mappings can be deleted at any time and are reconstructed through the
//! supplied segment fetcher.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use memmap2::Mmap;
use thiserror::Error;

const CACHE_FORMAT_DIRECTORY: &str = "v1";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct IndexSegmentId {
    pub blake3: [u8; 32],
    pub length: u64,
}

impl IndexSegmentId {
    pub(crate) fn new(blake3: [u8; 32], length: u64) -> Result<Self, IndexCacheError> {
        if length == 0 {
            return Err(IndexCacheError::InvalidLayout(
                "index segments must not be empty".into(),
            ));
        }
        Ok(Self { blake3, length })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexSegment {
    pub logical_offset: u64,
    pub id: IndexSegmentId,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexFileLayout {
    segments: Vec<IndexSegment>,
    logical_length: u64,
}

impl IndexFileLayout {
    pub(crate) fn new(segments: Vec<IndexSegment>) -> Result<Self, IndexCacheError> {
        let mut expected = 0_u64;
        for segment in &segments {
            if segment.logical_offset != expected {
                return Err(IndexCacheError::InvalidLayout(
                    "index file segments must be contiguous and ordered".into(),
                ));
            }
            expected = expected.checked_add(segment.id.length).ok_or_else(|| {
                IndexCacheError::InvalidLayout("index file length overflow".into())
            })?;
        }
        Ok(Self {
            segments,
            logical_length: expected,
        })
    }

    fn segment_at(&self, offset: u64) -> Option<IndexSegment> {
        if offset >= self.logical_length {
            return None;
        }
        let index = self
            .segments
            .partition_point(|segment| segment.logical_offset <= offset)
            .checked_sub(1)?;
        self.segments.get(index).copied()
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct IndexCacheConfig {
    pub disk_bytes: u64,
    pub memory_bytes: u64,
}

impl IndexCacheConfig {
    pub(crate) fn new(disk_bytes: u64, memory_bytes: u64) -> Result<Self, IndexCacheError> {
        if disk_bytes == 0 || memory_bytes == 0 {
            return Err(IndexCacheError::InvalidConfiguration(
                "index cache disk and memory budgets must be positive".into(),
            ));
        }
        Ok(Self {
            disk_bytes,
            memory_bytes,
        })
    }
}

#[tonic::async_trait]
pub(crate) trait IndexSegmentFetcher: Send + Sync + 'static {
    async fn fetch(&self, segment: IndexSegmentId) -> Result<Vec<u8>, IndexCacheError>;
}

#[derive(Clone)]
pub(crate) struct IndexCache {
    inner: Arc<IndexCacheInner>,
}

struct IndexCacheInner {
    directory: PathBuf,
    config: IndexCacheConfig,
    fetcher: Arc<dyn IndexSegmentFetcher>,
    state: Mutex<CacheState>,
}

#[derive(Default)]
struct CacheState {
    entries: BTreeMap<IndexSegmentId, CacheEntry>,
    clock: u64,
    disk_bytes: u64,
    memory_bytes: u64,
}

struct CacheEntry {
    mapped: Arc<Mmap>,
    memory: Option<Arc<[u8]>>,
    path: PathBuf,
    touched: u64,
}

impl IndexCache {
    pub(crate) fn new(
        directory: impl AsRef<Path>,
        config: IndexCacheConfig,
        fetcher: Arc<dyn IndexSegmentFetcher>,
    ) -> Result<Self, IndexCacheError> {
        let directory = directory.as_ref().join(CACHE_FORMAT_DIRECTORY);
        fs::create_dir_all(&directory).map_err(IndexCacheError::Io)?;
        Ok(Self {
            inner: Arc::new(IndexCacheInner {
                directory,
                config,
                fetcher,
                state: Mutex::new(CacheState::default()),
            }),
        })
    }

    pub(crate) fn open(&self, layout: IndexFileLayout) -> IndexFile {
        IndexFile {
            cache: self.clone(),
            layout: Arc::new(layout),
        }
    }

    async fn materialize(&self, id: IndexSegmentId) -> Result<Arc<Mmap>, IndexCacheError> {
        if let Some(mapped) = self.cached(id)? {
            return Ok(mapped);
        }

        // Concurrent cold misses may duplicate one fetch in this first bounded
        // implementation. Content identity plus atomic rename keeps the result
        // correct; request coalescing is a documented optimization gap.
        let bytes = self.inner.fetcher.fetch(id).await?;
        verify_bytes(id, &bytes)?;
        let directory = self.inner.directory.clone();
        let path = cache_path(&directory, id);
        let mapped =
            tokio::task::spawn_blocking(move || persist_and_map(&directory, &path, id, &bytes))
                .await
                .map_err(|error| IndexCacheError::Task(error.to_string()))??;
        let mapped = Arc::new(mapped);

        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| IndexCacheError::Poisoned)?;
        state.clock = state.clock.wrapping_add(1);
        let touched = state.clock;
        if let Some(existing) = state.entries.get_mut(&id) {
            existing.touched = touched;
            return Ok(existing.mapped.clone());
        }
        state.disk_bytes = state.disk_bytes.saturating_add(id.length);
        state.entries.insert(
            id,
            CacheEntry {
                mapped: mapped.clone(),
                memory: None,
                path: cache_path(&self.inner.directory, id),
                touched,
            },
        );
        evict_unpinned_disk(&mut state, self.inner.config.disk_bytes);
        Ok(mapped)
    }

    fn cached(&self, id: IndexSegmentId) -> Result<Option<Arc<Mmap>>, IndexCacheError> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| IndexCacheError::Poisoned)?;
        state.clock = state.clock.wrapping_add(1);
        let touched = state.clock;
        let selected = state.entries.get_mut(&id).map(|entry| {
            entry.touched = touched;
            entry.mapped.clone()
        });
        evict_unpinned_memory(&mut state, self.inner.config.memory_bytes);
        evict_unpinned_disk(&mut state, self.inner.config.disk_bytes);
        Ok(selected)
    }

    async fn backing(&self, id: IndexSegmentId) -> Result<IndexSliceBacking, IndexCacheError> {
        let mapped = self.materialize(id).await?;
        if id.length > self.inner.config.memory_bytes {
            return Ok(IndexSliceBacking::Mapped(mapped));
        }

        let cached_memory = {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| IndexCacheError::Poisoned)?;
            state.clock = state.clock.wrapping_add(1);
            let touched = state.clock;
            if let Some(entry) = state.entries.get_mut(&id) {
                entry.touched = touched;
                let selected = entry.memory.clone();
                evict_unpinned_memory(&mut state, self.inner.config.memory_bytes);
                selected
            } else {
                None
            }
        };
        if let Some(memory) = cached_memory {
            return Ok(IndexSliceBacking::Memory(memory));
        }

        let source = mapped.clone();
        let memory = tokio::task::spawn_blocking(move || Arc::<[u8]>::from(&source[..]))
            .await
            .map_err(|error| IndexCacheError::Task(error.to_string()))?;
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| IndexCacheError::Poisoned)?;
        state.clock = state.clock.wrapping_add(1);
        let touched = state.clock;
        let selected = match state.entries.get_mut(&id) {
            Some(entry) => {
                entry.touched = touched;
                if let Some(existing) = &entry.memory {
                    existing.clone()
                } else {
                    entry.memory = Some(memory.clone());
                    state.memory_bytes = state.memory_bytes.saturating_add(id.length);
                    memory
                }
            }
            None => return Ok(IndexSliceBacking::Mapped(mapped)),
        };
        evict_unpinned_memory(&mut state, self.inner.config.memory_bytes);
        Ok(IndexSliceBacking::Memory(selected))
    }
}

#[derive(Clone)]
pub(crate) struct IndexFile {
    cache: IndexCache,
    layout: Arc<IndexFileLayout>,
}

#[derive(Clone)]
pub(crate) struct IndexDirectory {
    cache: IndexCache,
    files: Arc<BTreeMap<String, IndexFileLayout>>,
}

impl IndexDirectory {
    pub(crate) fn new(
        cache: IndexCache,
        files: BTreeMap<String, IndexFileLayout>,
    ) -> Result<Self, IndexCacheError> {
        if files.is_empty() || files.keys().any(|name| !valid_file_name(name)) {
            return Err(IndexCacheError::InvalidLayout(
                "index directory requires canonical relative file names".into(),
            ));
        }
        Ok(Self {
            cache,
            files: Arc::new(files),
        })
    }
}

impl anvil_index::IndexDirectoryRead for IndexDirectory {
    type File = IndexFile;

    async fn open_file(&self, name: &str) -> Result<Self::File, anvil_index::IndexError> {
        let layout = self
            .files
            .get(name)
            .cloned()
            .ok_or_else(|| anvil_index::IndexError::FileNotFound(name.to_owned()))?;
        Ok(self.cache.open(layout))
    }
}

impl IndexFile {
    /// Read at most `max_length` contiguous bytes starting at `offset`.
    ///
    /// The returned owned slice pins its immutable mapping. It can be shorter
    /// than requested at a logical segment boundary and is empty at EOF.
    pub(crate) async fn read_at(
        &self,
        offset: u64,
        max_length: usize,
    ) -> Result<IndexSlice, IndexCacheError> {
        if max_length == 0 {
            return Ok(IndexSlice::empty());
        }
        let Some(segment) = self.layout.segment_at(offset) else {
            return Ok(IndexSlice::empty());
        };
        let backing = self.cache.backing(segment.id).await?;
        let within = usize::try_from(offset - segment.logical_offset)
            .map_err(|_| IndexCacheError::AddressSpace)?;
        let available = backing
            .data()
            .len()
            .checked_sub(within)
            .ok_or(IndexCacheError::CorruptCache)?;
        let length = available.min(max_length);
        Ok(IndexSlice {
            backing: Some(backing),
            start: within,
            end: within + length,
        })
    }

    pub(crate) async fn prefetch(
        &self,
        offset: u64,
        max_length: usize,
    ) -> Result<(), IndexCacheError> {
        let mut next = offset;
        let mut remaining = max_length as u64;
        while remaining > 0 {
            let Some(segment) = self.layout.segment_at(next) else {
                break;
            };
            self.cache.materialize(segment.id).await?;
            let available = segment
                .logical_offset
                .checked_add(segment.id.length)
                .and_then(|end| end.checked_sub(next))
                .ok_or(IndexCacheError::CorruptCache)?;
            let consumed = available.min(remaining);
            next = next
                .checked_add(consumed)
                .ok_or(IndexCacheError::AddressSpace)?;
            remaining -= consumed;
        }
        Ok(())
    }
}

impl anvil_index::IndexFileRead for IndexFile {
    type Slice = IndexSlice;

    async fn read_at(
        &self,
        offset: u64,
        max_length: usize,
    ) -> Result<Self::Slice, anvil_index::IndexError> {
        IndexFile::read_at(self, offset, max_length)
            .await
            .map_err(|error| anvil_index::IndexError::Io(error.to_string()))
    }
}

/// Immutable data returned from one asynchronous index read.
///
/// No borrow crosses an await boundary. The mapping remains alive until every
/// clone of this value is dropped.
#[derive(Clone)]
pub(crate) struct IndexSlice {
    backing: Option<IndexSliceBacking>,
    start: usize,
    end: usize,
}

#[derive(Clone)]
enum IndexSliceBacking {
    Mapped(Arc<Mmap>),
    Memory(Arc<[u8]>),
}

impl IndexSliceBacking {
    fn data(&self) -> &[u8] {
        match self {
            Self::Mapped(mapped) => mapped,
            Self::Memory(memory) => memory,
        }
    }
}

impl IndexSlice {
    fn empty() -> Self {
        Self {
            backing: None,
            start: 0,
            end: 0,
        }
    }

    pub(crate) fn data(&self) -> &[u8] {
        self.backing
            .as_ref()
            .map_or(&[], |backing| &backing.data()[self.start..self.end])
    }

    #[cfg(test)]
    fn is_memory_backed(&self) -> bool {
        matches!(&self.backing, Some(IndexSliceBacking::Memory(_)))
    }
}

impl AsRef<[u8]> for IndexSlice {
    fn as_ref(&self) -> &[u8] {
        self.data()
    }
}

impl Deref for IndexSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

fn cache_path(directory: &Path, id: IndexSegmentId) -> PathBuf {
    directory.join(format!("{}-{}", hex::encode(id.blake3), id.length))
}

fn valid_file_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('/')
        && !name.ends_with('/')
        && !name
            .split('/')
            .any(|segment| segment.is_empty() || segment == "..")
        && !name.contains('\0')
}

fn persist_and_map(
    directory: &Path,
    path: &Path,
    id: IndexSegmentId,
    bytes: &[u8],
) -> Result<Mmap, IndexCacheError> {
    if !path.exists() {
        let temporary = directory.join(format!(
            ".{}-{}-{}.tmp",
            hex::encode(id.blake3),
            id.length,
            uuid::Uuid::new_v4()
        ));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            match fs::rename(&temporary, path) {
                Ok(()) => Ok(()),
                Err(_error) if path.exists() => Ok(()),
                Err(error) => Err(error),
            }
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result.map_err(IndexCacheError::Io)?;
    }

    let mut file = File::open(path).map_err(IndexCacheError::Io)?;
    let metadata = file.metadata().map_err(IndexCacheError::Io)?;
    if metadata.len() != id.length {
        return Err(IndexCacheError::CorruptCache);
    }
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(IndexCacheError::Io)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    if hasher.finalize().as_bytes() != &id.blake3 {
        return Err(IndexCacheError::CorruptCache);
    }
    // SAFETY: cache files are immutable after atomic publication. Anvil never
    // writes through this mapping and eviction only unlinks the file after no
    // external IndexSlice retains the Arc<Mmap>.
    unsafe { Mmap::map(&file) }.map_err(IndexCacheError::Io)
}

fn verify_bytes(id: IndexSegmentId, bytes: &[u8]) -> Result<(), IndexCacheError> {
    if bytes.len() as u64 != id.length || blake3::hash(bytes).as_bytes() != &id.blake3 {
        return Err(IndexCacheError::InvalidFetchedSegment);
    }
    Ok(())
}

fn evict_unpinned_memory(state: &mut CacheState, budget: u64) {
    while state.memory_bytes > budget {
        let candidate = state
            .entries
            .iter()
            .filter(|(_, entry)| {
                entry
                    .memory
                    .as_ref()
                    .is_some_and(|memory| Arc::strong_count(memory) == 1)
            })
            .min_by_key(|(_, entry)| entry.touched)
            .map(|(id, _)| *id);
        let Some(candidate) = candidate else {
            break;
        };
        if let Some(entry) = state.entries.get_mut(&candidate)
            && entry.memory.take().is_some()
        {
            state.memory_bytes = state.memory_bytes.saturating_sub(candidate.length);
        }
    }
}

fn evict_unpinned_disk(state: &mut CacheState, budget: u64) {
    while state.disk_bytes > budget {
        let candidate = state
            .entries
            .iter()
            .filter(|(_, entry)| {
                Arc::strong_count(&entry.mapped) == 1
                    && entry
                        .memory
                        .as_ref()
                        .is_none_or(|memory| Arc::strong_count(memory) == 1)
            })
            .min_by_key(|(_, entry)| entry.touched)
            .map(|(id, _)| *id);
        let Some(candidate) = candidate else {
            break;
        };
        if let Some(entry) = state.entries.remove(&candidate) {
            state.disk_bytes = state.disk_bytes.saturating_sub(candidate.length);
            if entry.memory.is_some() {
                state.memory_bytes = state.memory_bytes.saturating_sub(candidate.length);
            }
            let _ = fs::remove_file(entry.path);
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum IndexCacheError {
    #[error("invalid index cache configuration: {0}")]
    InvalidConfiguration(String),
    #[error("invalid index file layout: {0}")]
    InvalidLayout(String),
    #[error("fetched index segment differs from its immutable identity")]
    InvalidFetchedSegment,
    #[error("cached index segment is corrupt")]
    CorruptCache,
    #[error("index offset cannot be represented in this address space")]
    AddressSpace,
    #[error("index cache state lock is poisoned")]
    Poisoned,
    #[error("index cache task failed: {0}")]
    Task(String),
    #[error("index segment fetch failed: {0}")]
    Fetch(String),
    #[error("index cache I/O failed: {0}")]
    Io(#[from] io::Error),
}

#[cfg(test)]
#[path = "cache/tests.rs"]
mod tests;
