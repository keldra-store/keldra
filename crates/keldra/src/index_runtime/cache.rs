//! Shared, disposable local materialisation of immutable index segments.
//!
//! Authoritative bytes always remain ordinary Anvil objects. Cache files and
//! mappings can be deleted at any time and are reconstructed through the
//! supplied segment fetcher.

use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, SeekFrom, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime};

use keldra_index::IndexError;
use keldra_index::v4::build::{MergeScratchFile, MergeScratchSpace};
use memmap2::Mmap;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Notify;

use crate::startup_scan_evidence::{StartupScanEvidence, StartupScanExtent, StartupScanKind};

const CACHE_FORMAT_DIRECTORY: &str = "v4";
const SCRATCH_FORMAT_DIRECTORY: &str = "v4";
const DEFAULT_SCRATCH_DIRECTORY: &str = "scratch";
const CACHE_TEMPORARY_FILE_GRACE: Duration = Duration::from_secs(60 * 60);
// Each mmap consumes a virtual-memory area and bookkeeping even for a tiny
// file. Charging at least one ordinary page prevents a large disk budget from
// retaining millions of one-byte mappings under a byte-only memory limit.
const CACHE_MAPPING_MINIMUM_BYTES: u64 = 4 * 1024;

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
    reconcile: CacheReconcileConfig,
}

impl IndexCacheConfig {
    const DEFAULT_RECONCILE: CacheReconcileConfig = CacheReconcileConfig {
        interval: Duration::from_secs(30),
        max_records: 256,
        max_bytes: 64 * 1024 * 1024,
        max_time: Duration::from_millis(10),
    };

    pub(crate) fn new(disk_bytes: u64, memory_bytes: u64) -> Result<Self, IndexCacheError> {
        if disk_bytes == 0 || memory_bytes == 0 {
            return Err(IndexCacheError::InvalidConfiguration(
                "index cache disk and memory budgets must be positive".into(),
            ));
        }
        Ok(Self {
            disk_bytes,
            memory_bytes,
            reconcile: Self::DEFAULT_RECONCILE,
        })
    }

    pub(crate) fn with_reconciliation(
        mut self,
        reconcile: CacheReconcileConfig,
    ) -> Result<Self, IndexCacheError> {
        reconcile.validate()?;
        self.reconcile = reconcile;
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CacheReconcileConfig {
    pub(crate) interval: Duration,
    pub(crate) max_records: usize,
    pub(crate) max_bytes: u64,
    pub(crate) max_time: Duration,
}

impl CacheReconcileConfig {
    pub(crate) fn new(
        interval: Duration,
        max_records: usize,
        max_bytes: u64,
        max_time: Duration,
    ) -> Result<Self, IndexCacheError> {
        let value = Self {
            interval,
            max_records,
            max_bytes,
            max_time,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(self) -> Result<(), IndexCacheError> {
        if self.interval.is_zero()
            || self.max_records == 0
            || self.max_bytes == 0
            || self.max_time.is_zero()
        {
            return Err(IndexCacheError::InvalidConfiguration(
                "cache reconciliation interval and work budgets must be positive".into(),
            ));
        }
        Ok(())
    }
}

#[tonic::async_trait]
pub(crate) trait IndexSegmentFetcher: Send + Sync + 'static {
    /// Open one already-authoritatively-verified immutable segment for a
    /// bounded streaming copy into the disposable cache.
    async fn fetch(&self, segment: IndexSegmentId)
    -> Result<Box<dyn Read + Send>, IndexCacheError>;
}

#[derive(Clone)]
pub(crate) struct IndexCache {
    inner: Arc<IndexCacheInner>,
}

struct IndexCacheInner {
    directory: PathBuf,
    scratch_directory: PathBuf,
    config: IndexCacheConfig,
    fetcher: Arc<dyn IndexSegmentFetcher>,
    fetch_budget: CacheFetchBudget,
    state: Mutex<CacheState>,
    reconcile: Mutex<CacheReconcileState>,
    reconciler_started: AtomicBool,
    startup_scan_evidence: Option<StartupScanEvidence>,
}

#[derive(Default)]
struct CacheReconcileState {
    directory: Option<fs::ReadDir>,
    pending: Option<fs::DirEntry>,
    retained_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CacheReconcileProgress {
    records: usize,
    bytes: u64,
    removed_bytes: u64,
    completed_cycle: bool,
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
                        gauge.keldra_index_cache_fetch_admitted_bytes = state.used,
                        gauge.keldra_index_cache_fetch_waiting = state.waiters.len() as u64,
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
            gauge.keldra_index_cache_fetch_admitted_bytes = state.used,
            gauge.keldra_index_cache_fetch_waiting = state.waiters.len() as u64,
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
    active_scratch: std::collections::BTreeSet<PathBuf>,
    clock: u64,
    disk_bytes: u64,
    memory_bytes: u64,
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
    mapped: Option<Arc<Mmap>>,
    path: PathBuf,
    touched: u64,
}

impl IndexCache {
    #[cfg(test)]
    pub(crate) fn new(
        directory: impl AsRef<Path>,
        config: IndexCacheConfig,
        fetcher: Arc<dyn IndexSegmentFetcher>,
    ) -> Result<Self, IndexCacheError> {
        let scratch_directory = directory.as_ref().join(DEFAULT_SCRATCH_DIRECTORY);
        Self::new_inner(directory, scratch_directory, config, fetcher, None)
    }

    pub(crate) fn new_with_startup_scan_evidence(
        directory: impl AsRef<Path>,
        config: IndexCacheConfig,
        fetcher: Arc<dyn IndexSegmentFetcher>,
        startup_scan_evidence: StartupScanEvidence,
    ) -> Result<Self, IndexCacheError> {
        let scratch_directory = directory.as_ref().join(DEFAULT_SCRATCH_DIRECTORY);
        Self::new_inner(
            directory,
            scratch_directory,
            config,
            fetcher,
            Some(startup_scan_evidence),
        )
    }

    pub(crate) fn new_with_directories_and_startup_scan_evidence(
        directory: impl AsRef<Path>,
        scratch_directory: impl AsRef<Path>,
        config: IndexCacheConfig,
        fetcher: Arc<dyn IndexSegmentFetcher>,
        startup_scan_evidence: StartupScanEvidence,
    ) -> Result<Self, IndexCacheError> {
        Self::new_inner(
            directory,
            scratch_directory,
            config,
            fetcher,
            Some(startup_scan_evidence),
        )
    }

    fn new_inner(
        directory: impl AsRef<Path>,
        scratch_directory: impl AsRef<Path>,
        config: IndexCacheConfig,
        fetcher: Arc<dyn IndexSegmentFetcher>,
        startup_scan_evidence: Option<StartupScanEvidence>,
    ) -> Result<Self, IndexCacheError> {
        let directory = directory.as_ref().join(CACHE_FORMAT_DIRECTORY);
        let scratch_directory = scratch_directory.as_ref().join(SCRATCH_FORMAT_DIRECTORY);
        fs::create_dir_all(&directory).map_err(IndexCacheError::Io)?;
        fs::create_dir_all(&scratch_directory).map_err(IndexCacheError::Io)?;
        let cache = Self {
            inner: Arc::new(IndexCacheInner {
                directory,
                scratch_directory,
                config,
                fetcher,
                fetch_budget: CacheFetchBudget::new(config.memory_bytes),
                state: Mutex::new(CacheState::default()),
                reconcile: Mutex::new(CacheReconcileState::default()),
                reconciler_started: AtomicBool::new(false),
                startup_scan_evidence,
            }),
        };
        Ok(cache)
    }

    pub(crate) fn open(&self, id: IndexSegmentId) -> IndexFile {
        IndexFile {
            cache: self.clone(),
            id,
        }
    }

    /// Open one restart-disposable merge workspace in the separately
    /// configured scratch root. Callers receive no general path authority.
    pub(crate) fn merge_scratch(&self) -> IndexMergeScratchSpace {
        IndexMergeScratchSpace {
            inner: Arc::new(IndexMergeScratchSpaceInner {
                directory: self.inner.scratch_directory.clone(),
                cache: Arc::downgrade(&self.inner),
                nonce: uuid::Uuid::new_v4().simple().to_string(),
                next_file: AtomicU64::new(0),
            }),
        }
    }

    async fn materialize(&self, id: IndexSegmentId) -> Result<Arc<Mmap>, IndexCacheError> {
        loop {
            if let Some(mapped) = self.cached(id)? {
                tracing::debug!(
                    monotonic_counter.keldra_index_cache_hits_total = 1_u64,
                    monotonic_counter.keldra_index_cache_hit_bytes_total = id.length,
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
                if let Some(mapped) = state
                    .entries
                    .get(&id)
                    .and_then(|entry| entry.mapped.as_ref())
                {
                    return Ok(mapped.clone());
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
                monotonic_counter.keldra_index_cache_misses_total = 1_u64,
                gauge.keldra_index_cache_fetch_in_flight_bytes = in_flight_bytes,
                "index cache miss"
            );
            if !leader {
                tracing::info!(
                    monotonic_counter.keldra_index_cache_coalesced_total = 1_u64,
                    "index cache cold miss coalesced"
                );
                flight.wait().await;
                continue;
            }
            let flight_leader = CacheFlightLeader::new(self.clone(), id, flight.clone());

            let materialized = self.materialize_leader(id).await;
            let (result, evicted_bytes, snapshot) = {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .map_err(|_| IndexCacheError::Poisoned)?;
                state.in_flight.remove(&id);
                state.in_flight_bytes = state.in_flight_bytes.saturating_sub(id.length);
                let mut evicted_bytes = 0_u64;
                let result = materialized.map(|mapped| {
                    let mapped = Arc::new(mapped);
                    state.clock = state.clock.wrapping_add(1);
                    let touched = state.clock;
                    state.memory_bytes =
                        state.memory_bytes.saturating_add(cache_mapping_charge(id));
                    if let Some(entry) = state.entries.get_mut(&id) {
                        debug_assert!(entry.mapped.is_none());
                        entry.mapped = Some(mapped.clone());
                        entry.touched = touched;
                    } else {
                        state.disk_bytes = state.disk_bytes.saturating_add(id.length);
                        state.entries.insert(
                            id,
                            CacheEntry {
                                mapped: Some(mapped.clone()),
                                path: cache_path(&self.inner.directory, id),
                                touched,
                            },
                        );
                    }
                    evicted_bytes = evict_unpinned_disk(&mut state, self.inner.config.disk_bytes);
                    evicted_bytes = evicted_bytes.saturating_add(evict_unpinned_memory(
                        &mut state,
                        self.inner.config.memory_bytes,
                    ));
                    mapped
                });
                (result, evicted_bytes, cache_snapshot(&state))
            };
            flight.finish();
            flight_leader.disarm();
            emit_cache_snapshot(snapshot);
            if evicted_bytes != 0 {
                tracing::info!(
                    monotonic_counter.keldra_index_cache_eviction_bytes_total = evicted_bytes,
                    "index cache blocks evicted"
                );
            }
            return result;
        }
    }

    async fn materialize_leader(&self, id: IndexSegmentId) -> Result<Mmap, IndexCacheError> {
        let path = cache_path(&self.inner.directory, id);
        let existing = tokio::task::spawn_blocking(move || map_existing_cache_file(&path, id))
            .await
            .map_err(|error| IndexCacheError::Task(error.to_string()))??;
        if let Some(mapped) = existing {
            tracing::debug!(
                monotonic_counter.keldra_index_cache_lazy_validations_total = 1_u64,
                "reused a verified warm index cache block"
            );
            return Ok(mapped);
        }
        self.fetch_and_map(id).await
    }

    async fn fetch_and_map(&self, id: IndexSegmentId) -> Result<Mmap, IndexCacheError> {
        let _fetch_permit = self.inner.fetch_budget.acquire(id.length).await;
        tracing::info!(
            monotonic_counter.keldra_index_cache_fetches_total = 1_u64,
            "index cache block fetch"
        );
        let source = self.inner.fetcher.fetch(id).await?;
        let directory = self.inner.directory.clone();
        let path = cache_path(&directory, id);
        let materialized = tokio::task::spawn_blocking(move || {
            persist_verified_stream_and_map(&directory, &path, id, source)
        })
        .await
        .map_err(|error| IndexCacheError::Task(error.to_string()))?;
        match &materialized {
            Err(IndexCacheError::InvalidFetchedSegment | IndexCacheError::CorruptCache) => {
                tracing::info!(
                    monotonic_counter.keldra_index_cache_verification_failures_total = 1_u64,
                    "index cache block verification failed"
                );
            }
            Ok(_) => {
                tracing::info!(
                    monotonic_counter.keldra_index_cache_fetch_bytes_total = id.length,
                    "index cache block fetched"
                );
            }
            Err(_) => {}
        }
        materialized
    }

    fn cached(&self, id: IndexSegmentId) -> Result<Option<Arc<Mmap>>, IndexCacheError> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| IndexCacheError::Poisoned)?;
        state.clock = state.clock.wrapping_add(1);
        let touched = state.clock;
        let selected = state.entries.get_mut(&id).and_then(|entry| {
            entry.touched = touched;
            entry.mapped.clone()
        });
        let disk_evicted = evict_unpinned_disk(&mut state, self.inner.config.disk_bytes);
        let memory_evicted = evict_unpinned_memory(&mut state, self.inner.config.memory_bytes);
        let evicted_bytes = disk_evicted.saturating_add(memory_evicted);
        let snapshot = (evicted_bytes != 0).then(|| cache_snapshot(&state));
        drop(state);
        if let Some(snapshot) = snapshot {
            emit_cache_snapshot(snapshot);
        }
        if evicted_bytes != 0 {
            tracing::info!(
                monotonic_counter.keldra_index_cache_eviction_bytes_total = evicted_bytes,
                "index cache blocks evicted"
            );
        }
        Ok(selected)
    }

    /// Start bounded disposable-cache reconciliation after public listeners
    /// are ready. Construction itself performs no inventory and starts no
    /// background task.
    pub(crate) fn start_reconciler(&self) -> bool {
        if self
            .inner
            .reconciler_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.inner
                .reconciler_started
                .store(false, Ordering::Release);
            return false;
        };
        let weak = Arc::downgrade(&self.inner);
        handle.spawn(async move {
            let config = weak
                .upgrade()
                .map(|inner| inner.config.reconcile)
                .expect("cache exists while its reconciler is spawned");
            let mut interval = tokio::time::interval(config.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Do not inventory the cache while the server is starting.
            interval.tick().await;
            loop {
                interval.tick().await;
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let result = tokio::task::spawn_blocking(move || {
                    reconcile_cache_step(
                        &inner,
                        config.max_records,
                        config.max_bytes,
                        config.max_time,
                    )
                })
                .await;
                match result {
                    Ok(Ok(progress)) => tracing::debug!(
                        gauge.keldra_index_cache_reconcile_records = progress.records as u64,
                        gauge.keldra_index_cache_reconcile_bytes = progress.bytes,
                        monotonic_counter.keldra_index_cache_reconcile_removed_bytes_total =
                            progress.removed_bytes,
                        "bounded index cache reconciliation step completed"
                    ),
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "bounded index cache reconciliation failed");
                    }
                    Err(error) => {
                        tracing::warn!(%error, "index cache reconciliation task failed");
                    }
                }
            }
        });
        true
    }
}

#[derive(Clone)]
pub(crate) struct IndexMergeScratchSpace {
    inner: Arc<IndexMergeScratchSpaceInner>,
}

struct IndexMergeScratchSpaceInner {
    directory: PathBuf,
    cache: Weak<IndexCacheInner>,
    nonce: String,
    next_file: AtomicU64,
}

#[derive(Clone)]
pub(crate) struct IndexMergeScratchFile {
    inner: Arc<IndexMergeScratchFileInner>,
}

struct IndexMergeScratchFileInner {
    path: PathBuf,
    cache: Weak<IndexCacheInner>,
    file: tokio::sync::Mutex<Option<tokio::fs::File>>,
}

impl MergeScratchSpace for IndexMergeScratchSpace {
    type File = IndexMergeScratchFile;

    async fn create_file(&self) -> Result<Self::File, IndexError> {
        loop {
            let counter = self
                .inner
                .next_file
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    value.checked_add(1)
                })
                .map_err(|_| IndexError::OffsetOverflow)?;
            let path = self
                .inner
                .directory
                .join(format!(".merge-{}-{counter}.tmp", self.inner.nonce));
            let file = match tokio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .await
            {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(scratch_io(error)),
            };
            if let Some(cache) = self.inner.cache.upgrade() {
                let inserted = cache
                    .state
                    .lock()
                    .map_err(|_| IndexError::Io("index cache state lock is poisoned".into()))?
                    .active_scratch
                    .insert(path.clone());
                if !inserted {
                    drop(file);
                    let _ = fs::remove_file(&path);
                    return Err(IndexError::Io(
                        "merge scratch path is already active".into(),
                    ));
                }
            }
            return Ok(IndexMergeScratchFile {
                inner: Arc::new(IndexMergeScratchFileInner {
                    path,
                    cache: self.inner.cache.clone(),
                    file: tokio::sync::Mutex::new(Some(file)),
                }),
            });
        }
    }
}

impl MergeScratchFile for IndexMergeScratchFile {
    async fn resize_zeroed(&self, length: u64) -> Result<(), IndexError> {
        let guard = self.inner.file.lock().await;
        let file = guard
            .as_ref()
            .ok_or_else(|| IndexError::Io("merge scratch file is closed".into()))?;
        let current = file.metadata().await.map_err(scratch_io)?.len();
        if length < current {
            return Err(IndexError::InvalidDefinition(
                "merge scratch resize cannot truncate bytes".into(),
            ));
        }
        file.set_len(length).await.map_err(scratch_io)
    }

    async fn write_all_at(&self, offset: u64, bytes: Vec<u8>) -> Result<(), IndexError> {
        let length = u64::try_from(bytes.len()).map_err(|_| IndexError::OffsetOverflow)?;
        let expected = offset
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        let mut guard = self.inner.file.lock().await;
        let file = guard
            .as_mut()
            .ok_or_else(|| IndexError::Io("merge scratch file is closed".into()))?;
        let actual = file.metadata().await.map_err(scratch_io)?.len();
        if expected > actual {
            return Err(IndexError::UnexpectedEof { expected, actual });
        }
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(scratch_io)?;
        file.write_all(&bytes).await.map_err(scratch_io)?;
        file.flush().await.map_err(scratch_io)
    }

    async fn append(&self, bytes: Vec<u8>) -> Result<u64, IndexError> {
        let mut guard = self.inner.file.lock().await;
        let file = guard
            .as_mut()
            .ok_or_else(|| IndexError::Io("merge scratch file is closed".into()))?;
        let offset = file.seek(SeekFrom::End(0)).await.map_err(scratch_io)?;
        let length = u64::try_from(bytes.len()).map_err(|_| IndexError::OffsetOverflow)?;
        offset
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        file.write_all(&bytes).await.map_err(scratch_io)?;
        file.flush().await.map_err(scratch_io)?;
        Ok(offset)
    }

    async fn read_exact_at(&self, offset: u64, length: usize) -> Result<Vec<u8>, IndexError> {
        let requested = u64::try_from(length).map_err(|_| IndexError::OffsetOverflow)?;
        let expected = offset
            .checked_add(requested)
            .ok_or(IndexError::OffsetOverflow)?;
        let mut guard = self.inner.file.lock().await;
        let file = guard
            .as_mut()
            .ok_or_else(|| IndexError::Io("merge scratch file is closed".into()))?;
        let actual = file.metadata().await.map_err(scratch_io)?.len();
        if expected > actual {
            return Err(IndexError::UnexpectedEof { expected, actual });
        }
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(scratch_io)?;
        let mut bytes = vec![0_u8; length];
        file.read_exact(&mut bytes).await.map_err(scratch_io)?;
        Ok(bytes)
    }

    async fn len(&self) -> Result<u64, IndexError> {
        let guard = self.inner.file.lock().await;
        let file = guard
            .as_ref()
            .ok_or_else(|| IndexError::Io("merge scratch file is closed".into()))?;
        Ok(file.metadata().await.map_err(scratch_io)?.len())
    }
}

impl Drop for IndexMergeScratchFileInner {
    fn drop(&mut self) {
        if let Ok(mut file) = self.file.try_lock() {
            file.take();
        }
        if let Some(cache) = self.cache.upgrade()
            && let Ok(mut state) = cache.state.lock()
        {
            state.active_scratch.remove(&self.path);
        }
        match fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => tracing::warn!(
                path = %self.path.display(),
                %error,
                "merge scratch cleanup failed"
            ),
        }
    }
}

fn scratch_io(error: io::Error) -> IndexError {
    IndexError::Io(error.to_string())
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
            cache: Some(Arc::downgrade(&self.cache.inner)),
            start: within,
            end: within + length,
        })
    }
}

impl keldra_index::IndexFileRead for IndexFile {
    type Slice = IndexSlice;

    async fn read_at(
        &self,
        offset: u64,
        max_length: usize,
    ) -> Result<Self::Slice, keldra_index::IndexError> {
        IndexFile::read_at(self, offset, max_length)
            .await
            .map_err(|error| keldra_index::IndexError::Io(error.to_string()))
    }
}

/// Immutable data returned from one asynchronous index read.
///
/// No borrow crosses an await boundary. The mapping remains alive until every
/// clone of this value is dropped.
#[derive(Clone)]
pub(crate) struct IndexSlice {
    backing: Option<Arc<Mmap>>,
    cache: Option<Weak<IndexCacheInner>>,
    start: usize,
    end: usize,
}

impl IndexSlice {
    fn empty() -> Self {
        Self {
            backing: None,
            cache: None,
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

impl Drop for IndexSlice {
    fn drop(&mut self) {
        // Drop this slice's pin before checking the shared budget. If another
        // slice still holds the same mapping, the strong-count guard keeps it
        // alive and the overage remains attributable to that live handle.
        self.backing.take();
        let Some(inner) = self.cache.take().and_then(|cache| cache.upgrade()) else {
            return;
        };
        let (evicted_bytes, snapshot) = {
            let Ok(mut state) = inner.state.lock() else {
                return;
            };
            let evicted_bytes = evict_unpinned_memory(&mut state, inner.config.memory_bytes);
            let snapshot = (evicted_bytes != 0).then(|| cache_snapshot(&state));
            (evicted_bytes, snapshot)
        };
        if let Some(snapshot) = snapshot {
            emit_cache_snapshot(snapshot);
        }
        if evicted_bytes != 0 {
            tracing::info!(
                monotonic_counter.keldra_index_cache_eviction_bytes_total = evicted_bytes,
                "unpinned index cache mappings evicted"
            );
        }
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

fn map_existing_cache_file(
    path: &Path,
    id: IndexSegmentId,
) -> Result<Option<Mmap>, IndexCacheError> {
    match map_verified_cache_file(path, id) {
        Ok(mapped) => Ok(Some(mapped)),
        Err(IndexCacheError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(IndexCacheError::CorruptCache) => {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(IndexCacheError::Io(error)),
            }
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn reconcile_cache_step(
    inner: &IndexCacheInner,
    max_records: usize,
    max_bytes: u64,
    max_time: Duration,
) -> Result<CacheReconcileProgress, IndexCacheError> {
    if max_records == 0 || max_bytes == 0 || max_time.is_zero() {
        return Err(IndexCacheError::InvalidConfiguration(
            "cache reconciliation budgets must be positive".into(),
        ));
    }
    let started = Instant::now();
    let tracked_bytes = inner
        .state
        .lock()
        .map_err(|_| IndexCacheError::Poisoned)?
        .disk_bytes;
    let mut reconcile = inner
        .reconcile
        .lock()
        .map_err(|_| IndexCacheError::Poisoned)?;
    if reconcile.directory.is_none() {
        if let Some(evidence) = &inner.startup_scan_evidence {
            evidence.record(StartupScanKind::Cache, StartupScanExtent::Global);
        }
        reconcile.directory = Some(fs::read_dir(&inner.directory).map_err(IndexCacheError::Io)?);
        reconcile.retained_bytes = tracked_bytes;
    }

    let mut progress = CacheReconcileProgress::default();
    loop {
        let next = reconcile.pending.take().map(Ok).or_else(|| {
            reconcile
                .directory
                .as_mut()
                .expect("cache reconciliation directory is initialized")
                .next()
        });
        let Some(entry) = next else {
            reconcile.directory = None;
            reconcile.retained_bytes = 0;
            progress.completed_cycle = true;
            break;
        };
        let entry = entry.map_err(IndexCacheError::Io)?;
        let entry_bytes = cache_reconcile_entry_bytes(&entry);
        if progress.records != 0 && progress.bytes.saturating_add(entry_bytes) > max_bytes {
            reconcile.pending = Some(entry);
            break;
        }
        progress.records += 1;
        // An individual platform directory entry can theoretically exceed an
        // unusually tiny configured byte budget. It consumes that whole tick
        // instead of pinning the cursor forever.
        progress.bytes = progress.bytes.saturating_add(entry_bytes.min(max_bytes));
        let file_type = entry.file_type().map_err(IndexCacheError::Io)?;
        if file_type.is_file() {
            let metadata = entry.metadata().map_err(IndexCacheError::Io)?;
            let actual_bytes = metadata.len();
            let active_scratch = inner
                .state
                .lock()
                .map_err(|_| IndexCacheError::Poisoned)?
                .active_scratch
                .contains(&entry.path());
            if active_scratch {
                reconcile.retained_bytes = reconcile.retained_bytes.saturating_add(actual_bytes);
            } else if let Some(id) = cache_id_from_file_name(&entry.file_name()) {
                let tracked = {
                    let state = inner.state.lock().map_err(|_| IndexCacheError::Poisoned)?;
                    state.entries.contains_key(&id) || state.in_flight.contains_key(&id)
                };
                if !tracked {
                    let valid_length = actual_bytes == id.length;
                    let projected = reconcile.retained_bytes.saturating_add(actual_bytes);
                    if !valid_length || projected > inner.config.disk_bytes {
                        remove_cache_file(&entry.path(), actual_bytes, &mut progress)?;
                    } else {
                        reconcile.retained_bytes = projected;
                    }
                }
            } else if recent_cache_temporary(&entry.file_name(), &metadata) {
                reconcile.retained_bytes = reconcile.retained_bytes.saturating_add(actual_bytes);
            } else {
                remove_cache_file(&entry.path(), actual_bytes, &mut progress)?;
            }
        } else {
            tracing::warn!(
                path = %entry.path().display(),
                "index cache reconciliation left a non-regular entry untouched"
            );
        }
        if progress.records >= max_records
            || progress.bytes >= max_bytes
            || started.elapsed() >= max_time
        {
            break;
        }
    }
    let evicted_bytes = {
        let mut state = inner.state.lock().map_err(|_| IndexCacheError::Poisoned)?;
        let disk = evict_unpinned_disk(&mut state, inner.config.disk_bytes);
        evict_unpinned_memory(&mut state, inner.config.memory_bytes);
        disk
    };
    progress.removed_bytes = progress.removed_bytes.saturating_add(evicted_bytes);
    Ok(progress)
}

fn cache_reconcile_entry_bytes(entry: &fs::DirEntry) -> u64 {
    // Reconciliation reads directory metadata, not file contents. Charge the
    // variable path plus a fixed allowance for the metadata record.
    entry.path().as_os_str().as_encoded_bytes().len() as u64 + 128
}

fn remove_cache_file(
    path: &Path,
    actual_bytes: u64,
    progress: &mut CacheReconcileProgress,
) -> Result<(), IndexCacheError> {
    match fs::remove_file(path) {
        Ok(()) => {
            progress.removed_bytes = progress.removed_bytes.saturating_add(actual_bytes);
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(IndexCacheError::Io(error)),
    }
    Ok(())
}

fn recent_cache_temporary(name: &std::ffi::OsStr, metadata: &fs::Metadata) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    if !name.starts_with('.') || !name.ends_with(".tmp") {
        return false;
    }
    metadata
        .modified()
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age < CACHE_TEMPORARY_FILE_GRACE)
}

fn cache_id_from_file_name(name: &std::ffi::OsStr) -> Option<IndexSegmentId> {
    let name = name.to_str()?;
    let (hash, length) = name.split_once('-')?;
    if hash.len() != 64 || length.is_empty() || length.bytes().any(|byte| !byte.is_ascii_digit()) {
        return None;
    }
    let hash: [u8; 32] = hex::decode(hash).ok()?.try_into().ok()?;
    let length = length.parse().ok()?;
    IndexSegmentId::new(hash, length).ok()
}

fn persist_verified_stream_and_map(
    directory: &Path,
    path: &Path,
    id: IndexSegmentId,
    mut source: Box<dyn Read + Send>,
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
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        let mut hasher = blake3::Hasher::new();
        let mut observed = 0_u64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = source
                .read(&mut buffer)
                .map_err(|error| IndexCacheError::Fetch(error.to_string()))?;
            if read == 0 {
                break;
            }
            observed = observed.saturating_add(read as u64);
            if observed > id.length {
                return Err(IndexCacheError::InvalidFetchedSegment);
            }
            hasher.update(&buffer[..read]);
            file.write_all(&buffer[..read])?;
        }
        if observed != id.length || hasher.finalize().as_bytes() != &id.blake3 {
            return Err(IndexCacheError::InvalidFetchedSegment);
        }
        file.sync_all()?;
        // Replacing an existing corrupt cache file is atomic. Authoritative
        // bytes remain the ordinary object fetched and verified by the caller.
        fs::rename(&temporary, path)?;
        // SAFETY: this exact file handle was populated and identity-verified
        // above, then atomically published. Anvil never mutates cache files.
        unsafe { Mmap::map(&file) }.map_err(IndexCacheError::Io)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
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

fn cache_mapping_charge(id: IndexSegmentId) -> u64 {
    id.length.max(CACHE_MAPPING_MINIMUM_BYTES)
}

fn evict_unpinned_disk(state: &mut CacheState, budget: u64) -> u64 {
    let mut evicted = 0_u64;
    while state.disk_bytes > budget {
        let candidate = state
            .entries
            .iter()
            .filter(|(_, entry)| {
                entry
                    .mapped
                    .as_ref()
                    .is_none_or(|mapped| Arc::strong_count(mapped) == 1)
            })
            .min_by_key(|(_, entry)| entry.touched)
            .map(|(id, _)| *id);
        let Some(candidate) = candidate else {
            break;
        };
        if let Some(entry) = state.entries.remove(&candidate) {
            state.disk_bytes = state.disk_bytes.saturating_sub(candidate.length);
            if entry.mapped.is_some() {
                state.memory_bytes = state
                    .memory_bytes
                    .saturating_sub(cache_mapping_charge(candidate));
            }
            let _ = fs::remove_file(entry.path);
            evicted = evicted.saturating_add(candidate.length);
        }
    }
    evicted
}

fn evict_unpinned_memory(state: &mut CacheState, budget: u64) -> u64 {
    let mut evicted = 0_u64;
    while state.memory_bytes > budget {
        let candidate = state
            .entries
            .iter()
            .filter(|(_, entry)| {
                entry
                    .mapped
                    .as_ref()
                    .is_some_and(|mapped| Arc::strong_count(mapped) == 1)
            })
            .min_by_key(|(_, entry)| entry.touched)
            .map(|(id, _)| *id);
        let Some(candidate) = candidate else {
            break;
        };
        if let Some(entry) = state.entries.get_mut(&candidate) {
            // The immutable cache file remains available for a verified lazy
            // remap. Only the retained mmap and its bookkeeping are evicted.
            entry.mapped = None;
            let charge = cache_mapping_charge(candidate);
            state.memory_bytes = state.memory_bytes.saturating_sub(charge);
            evicted = evicted.saturating_add(charge);
        }
    }
    evicted
}

#[derive(Clone, Copy)]
struct CacheSnapshot {
    disk_bytes: u64,
    memory_bytes: u64,
    in_flight_bytes: u64,
    open_mappings: u64,
}

fn cache_snapshot(state: &CacheState) -> CacheSnapshot {
    CacheSnapshot {
        disk_bytes: state.disk_bytes,
        memory_bytes: state.memory_bytes,
        in_flight_bytes: state.in_flight_bytes,
        open_mappings: state
            .entries
            .values()
            .filter(|entry| entry.mapped.is_some())
            .count() as u64,
    }
}

fn emit_cache_snapshot(snapshot: CacheSnapshot) {
    tracing::debug!(
        gauge.keldra_index_cache_disk_bytes = snapshot.disk_bytes,
        gauge.keldra_index_cache_memory_bytes = snapshot.memory_bytes,
        gauge.keldra_index_cache_fetch_in_flight_bytes = snapshot.in_flight_bytes,
        gauge.keldra_index_cache_open_mappings = snapshot.open_mappings,
        gauge.keldra_index_cache_pinned_decoded_bytes = 0_u64,
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
