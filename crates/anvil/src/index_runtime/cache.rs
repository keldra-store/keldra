//! Shared, disposable local materialisation of immutable index segments.
//!
//! Authoritative bytes always remain ordinary Anvil objects. Cache files and
//! mappings can be deleted at any time and are reconstructed through the
//! supplied segment fetcher.

use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use memmap2::Mmap;
use thiserror::Error;
use tokio::sync::Notify;

const CACHE_FORMAT_DIRECTORY: &str = "v2";

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
    fetch_budget: CacheFetchBudget,
    state: Mutex<CacheState>,
}

#[derive(Clone)]
struct CacheFetchBudget {
    inner: Arc<CacheFetchBudgetInner>,
}

struct CacheFetchBudgetInner {
    limit: u64,
    state: Mutex<CacheFetchBudgetState>,
    changed: Notify,
}

#[derive(Default)]
struct CacheFetchBudgetState {
    used: u64,
    next_ticket: u64,
    waiters: VecDeque<(u64, u64)>,
}

impl CacheFetchBudget {
    fn new(limit: u64) -> Self {
        Self {
            inner: Arc::new(CacheFetchBudgetInner {
                limit,
                state: Mutex::new(CacheFetchBudgetState::default()),
                changed: Notify::new(),
            }),
        }
    }

    async fn acquire(&self, requested: u64) -> CacheFetchPermit {
        // One block larger than the configured in-flight memory budget is
        // still readable, but occupies the whole fetch allowance by itself.
        let charged = requested.min(self.inner.limit);
        let ticket = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let ticket = state.next_ticket;
            state.next_ticket = state.next_ticket.wrapping_add(1);
            state.waiters.push_back((ticket, charged));
            ticket
        };
        let mut queued = CacheFetchWaiter {
            budget: self.clone(),
            ticket: Some(ticket),
        };
        loop {
            let changed = self.inner.changed.notified();
            let admitted = {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state
                    .waiters
                    .front()
                    .is_some_and(|(front, _)| *front == ticket)
                    && state.used <= self.inner.limit.saturating_sub(charged)
                {
                    state.waiters.pop_front();
                    state.used += charged;
                    tracing::info!(
                        gauge.anvil_index_cache_fetch_admitted_bytes = state.used,
                        gauge.anvil_index_cache_fetch_waiting = state.waiters.len() as u64,
                        "index cache fetch budget state"
                    );
                    true
                } else {
                    false
                }
            };
            if admitted {
                queued.ticket = None;
                self.inner.changed.notify_waiters();
                return CacheFetchPermit {
                    budget: self.clone(),
                    charged,
                };
            }
            changed.await;
        }
    }
}

struct CacheFetchWaiter {
    budget: CacheFetchBudget,
    ticket: Option<u64>,
}

impl Drop for CacheFetchWaiter {
    fn drop(&mut self) {
        let Some(ticket) = self.ticket else {
            return;
        };
        let mut state = self
            .budget
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(index) = state
            .waiters
            .iter()
            .position(|(waiting, _)| *waiting == ticket)
        {
            state.waiters.remove(index);
        }
        drop(state);
        self.budget.inner.changed.notify_waiters();
    }
}

struct CacheFetchPermit {
    budget: CacheFetchBudget,
    charged: u64,
}

impl Drop for CacheFetchPermit {
    fn drop(&mut self) {
        let mut state = self
            .budget
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.used = state.used.saturating_sub(self.charged);
        tracing::info!(
            gauge.anvil_index_cache_fetch_admitted_bytes = state.used,
            gauge.anvil_index_cache_fetch_waiting = state.waiters.len() as u64,
            "index cache fetch budget state"
        );
        drop(state);
        self.budget.inner.changed.notify_waiters();
    }
}

#[derive(Default)]
struct CacheState {
    entries: BTreeMap<IndexSegmentId, CacheEntry>,
    in_flight: BTreeMap<IndexSegmentId, Arc<CacheFlight>>,
    clock: u64,
    disk_bytes: u64,
    in_flight_bytes: u64,
}

struct CacheFlight {
    complete: AtomicBool,
    changed: Notify,
}

struct CacheFlightLeader {
    cache: IndexCache,
    id: IndexSegmentId,
    flight: Arc<CacheFlight>,
    active: bool,
}

impl CacheFlightLeader {
    fn new(cache: IndexCache, id: IndexSegmentId, flight: Arc<CacheFlight>) -> Self {
        Self {
            cache,
            id,
            flight,
            active: true,
        }
    }

    fn disarm(mut self) {
        self.active = false;
    }
}

impl Drop for CacheFlightLeader {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = self.cache.inner.state.lock()
            && state
                .in_flight
                .get(&self.id)
                .is_some_and(|registered| Arc::ptr_eq(registered, &self.flight))
        {
            state.in_flight.remove(&self.id);
            state.in_flight_bytes = state.in_flight_bytes.saturating_sub(self.id.length);
        }
        self.flight.finish();
    }
}

impl CacheFlight {
    fn new() -> Self {
        Self {
            complete: AtomicBool::new(false),
            changed: Notify::new(),
        }
    }

    async fn wait(&self) {
        loop {
            let changed = self.changed.notified();
            if self.complete.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }

    fn finish(&self) {
        self.complete.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
}

struct CacheEntry {
    mapped: Arc<Mmap>,
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
        reset_disposable_cache(&directory)?;
        Ok(Self {
            inner: Arc::new(IndexCacheInner {
                directory,
                config,
                fetcher,
                fetch_budget: CacheFetchBudget::new(config.memory_bytes),
                state: Mutex::new(CacheState::default()),
            }),
        })
    }

    pub(crate) fn open(&self, id: IndexSegmentId) -> IndexFile {
        IndexFile {
            cache: self.clone(),
            id,
        }
    }

    async fn materialize(&self, id: IndexSegmentId) -> Result<Arc<Mmap>, IndexCacheError> {
        loop {
            if let Some(mapped) = self.cached(id)? {
                tracing::info!(
                    monotonic_counter.anvil_index_cache_hits_total = 1_u64,
                    monotonic_counter.anvil_index_cache_hit_bytes_total = id.length,
                    "index cache hit"
                );
                return Ok(mapped);
            }

            let (flight, leader, in_flight_bytes) = {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .map_err(|_| IndexCacheError::Poisoned)?;
                if let Some(existing) = state.entries.get(&id) {
                    return Ok(existing.mapped.clone());
                }
                if let Some(flight) = state.in_flight.get(&id) {
                    (flight.clone(), false, state.in_flight_bytes)
                } else {
                    let flight = Arc::new(CacheFlight::new());
                    state.in_flight.insert(id, flight.clone());
                    state.in_flight_bytes = state.in_flight_bytes.saturating_add(id.length);
                    (flight, true, state.in_flight_bytes)
                }
            };
            tracing::info!(
                monotonic_counter.anvil_index_cache_misses_total = 1_u64,
                gauge.anvil_index_cache_fetch_in_flight_bytes = in_flight_bytes,
                "index cache miss"
            );
            if !leader {
                tracing::info!(
                    monotonic_counter.anvil_index_cache_coalesced_total = 1_u64,
                    "index cache cold miss coalesced"
                );
                flight.wait().await;
                continue;
            }
            let flight_leader = CacheFlightLeader::new(self.clone(), id, flight.clone());

            let fetched = self.fetch_and_map(id).await;
            let (result, evicted_bytes, snapshot) = {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .map_err(|_| IndexCacheError::Poisoned)?;
                state.in_flight.remove(&id);
                state.in_flight_bytes = state.in_flight_bytes.saturating_sub(id.length);
                let mut evicted_bytes = 0_u64;
                let result = fetched.map(|mapped| {
                    let mapped = Arc::new(mapped);
                    state.clock = state.clock.wrapping_add(1);
                    let touched = state.clock;
                    state.disk_bytes = state.disk_bytes.saturating_add(id.length);
                    state.entries.insert(
                        id,
                        CacheEntry {
                            mapped: mapped.clone(),
                            path: cache_path(&self.inner.directory, id),
                            touched,
                        },
                    );
                    evicted_bytes = evict_unpinned_disk(&mut state, self.inner.config.disk_bytes);
                    mapped
                });
                (result, evicted_bytes, cache_snapshot(&state))
            };
            flight.finish();
            flight_leader.disarm();
            emit_cache_snapshot(snapshot);
            if evicted_bytes != 0 {
                tracing::info!(
                    monotonic_counter.anvil_index_cache_eviction_bytes_total = evicted_bytes,
                    "index cache blocks evicted"
                );
            }
            return result;
        }
    }

    async fn fetch_and_map(&self, id: IndexSegmentId) -> Result<Mmap, IndexCacheError> {
        let _fetch_permit = self.inner.fetch_budget.acquire(id.length).await;
        tracing::info!(
            monotonic_counter.anvil_index_cache_fetches_total = 1_u64,
            "index cache block fetch"
        );
        let bytes = self.inner.fetcher.fetch(id).await?;
        if let Err(error) = verify_bytes(id, &bytes) {
            tracing::info!(
                monotonic_counter.anvil_index_cache_verification_failures_total = 1_u64,
                "index cache block verification failed"
            );
            return Err(error);
        }
        tracing::info!(
            monotonic_counter.anvil_index_cache_fetch_bytes_total = id.length,
            "index cache block fetched"
        );
        let directory = self.inner.directory.clone();
        let path = cache_path(&directory, id);
        tokio::task::spawn_blocking(move || persist_and_map(&directory, &path, id, &bytes))
            .await
            .map_err(|error| IndexCacheError::Task(error.to_string()))?
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
        let disk_evicted = evict_unpinned_disk(&mut state, self.inner.config.disk_bytes);
        let snapshot = cache_snapshot(&state);
        drop(state);
        emit_cache_snapshot(snapshot);
        if disk_evicted != 0 {
            tracing::info!(
                monotonic_counter.anvil_index_cache_eviction_bytes_total = disk_evicted,
                "index cache blocks evicted"
            );
        }
        Ok(selected)
    }
}

#[derive(Clone)]
pub(crate) struct IndexFile {
    cache: IndexCache,
    id: IndexSegmentId,
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
        if offset >= self.id.length {
            return Ok(IndexSlice::empty());
        }
        let backing = self.cache.materialize(self.id).await?;
        let within = usize::try_from(offset).map_err(|_| IndexCacheError::AddressSpace)?;
        let available = backing
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
    backing: Option<Arc<Mmap>>,
    start: usize,
    end: usize,
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
            .map_or(&[], |backing| &backing[self.start..self.end])
    }

    #[cfg(test)]
    fn is_mmap_backed(&self) -> bool {
        self.backing.is_some()
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

fn reset_disposable_cache(directory: &Path) -> Result<(), IndexCacheError> {
    match fs::remove_dir_all(directory) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(IndexCacheError::Io(error)),
    }
    fs::create_dir_all(directory).map_err(IndexCacheError::Io)
}

fn persist_and_map(
    directory: &Path,
    path: &Path,
    id: IndexSegmentId,
    bytes: &[u8],
) -> Result<Mmap, IndexCacheError> {
    if path.exists() {
        match map_verified_cache_file(path, id) {
            Ok(mapped) => return Ok(mapped),
            Err(IndexCacheError::CorruptCache) => {}
            Err(error) => return Err(error),
        }
    }

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
        // Replacing an existing corrupt cache file is atomic. Authoritative
        // bytes remain the ordinary object fetched and verified by the caller.
        fs::rename(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(IndexCacheError::Io)?;

    map_verified_cache_file(path, id)
}

fn map_verified_cache_file(path: &Path, id: IndexSegmentId) -> Result<Mmap, IndexCacheError> {
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

fn evict_unpinned_disk(state: &mut CacheState, budget: u64) -> u64 {
    let mut evicted = 0_u64;
    while state.disk_bytes > budget {
        let candidate = state
            .entries
            .iter()
            .filter(|(_, entry)| Arc::strong_count(&entry.mapped) == 1)
            .min_by_key(|(_, entry)| entry.touched)
            .map(|(id, _)| *id);
        let Some(candidate) = candidate else {
            break;
        };
        if let Some(entry) = state.entries.remove(&candidate) {
            state.disk_bytes = state.disk_bytes.saturating_sub(candidate.length);
            let _ = fs::remove_file(entry.path);
            evicted = evicted.saturating_add(candidate.length);
        }
    }
    evicted
}

#[derive(Clone, Copy)]
struct CacheSnapshot {
    disk_bytes: u64,
    in_flight_bytes: u64,
    open_mappings: u64,
}

fn cache_snapshot(state: &CacheState) -> CacheSnapshot {
    CacheSnapshot {
        disk_bytes: state.disk_bytes,
        in_flight_bytes: state.in_flight_bytes,
        open_mappings: state
            .entries
            .values()
            .filter(|entry| Arc::strong_count(&entry.mapped) > 1)
            .count() as u64,
    }
}

fn emit_cache_snapshot(snapshot: CacheSnapshot) {
    tracing::info!(
        gauge.anvil_index_cache_disk_bytes = snapshot.disk_bytes,
        gauge.anvil_index_cache_memory_bytes = 0_u64,
        gauge.anvil_index_cache_fetch_in_flight_bytes = snapshot.in_flight_bytes,
        gauge.anvil_index_cache_open_mappings = snapshot.open_mappings,
        gauge.anvil_index_cache_pinned_decoded_bytes = 0_u64,
        "index cache state"
    );
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
