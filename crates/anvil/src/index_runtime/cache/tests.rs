use std::sync::atomic::{AtomicUsize, Ordering};

use tempfile::TempDir;

use anvil_index::v4::build::{MergeScratchFile as _, MergeScratchSpace as _};

use super::*;

struct MemoryFetcher {
    values: BTreeMap<IndexSegmentId, Vec<u8>>,
    reads: AtomicUsize,
}

struct GatedFetcher {
    value: Vec<u8>,
    reads: AtomicUsize,
    release: tokio::sync::Semaphore,
}

struct MultiGatedFetcher {
    values: BTreeMap<IndexSegmentId, Vec<u8>>,
    reads: AtomicUsize,
    release: tokio::sync::Semaphore,
}

struct InterruptedFetcher {
    value: Vec<u8>,
}

struct InterruptedReader {
    value: std::io::Cursor<Vec<u8>>,
    interrupted: bool,
}

impl Read for InterruptedReader {
    fn read(&mut self, destination: &mut [u8]) -> std::io::Result<usize> {
        if self.interrupted {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "fixture interrupted the immutable stream",
            ));
        }
        let maximum = destination.len().min(4);
        let read = std::io::Read::read(&mut self.value, &mut destination[..maximum])?;
        self.interrupted = read != 0;
        Ok(read)
    }
}

#[tonic::async_trait]
impl IndexSegmentFetcher for InterruptedFetcher {
    async fn fetch(
        &self,
        _segment: IndexSegmentId,
    ) -> Result<Box<dyn Read + Send>, IndexCacheError> {
        Ok(Box::new(InterruptedReader {
            value: std::io::Cursor::new(self.value.clone()),
            interrupted: false,
        }))
    }
}

#[tonic::async_trait]
impl IndexSegmentFetcher for MultiGatedFetcher {
    async fn fetch(
        &self,
        segment: IndexSegmentId,
    ) -> Result<Box<dyn Read + Send>, IndexCacheError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.release.acquire().await.unwrap().forget();
        self.values
            .get(&segment)
            .cloned()
            .map(|bytes| Box::new(std::io::Cursor::new(bytes)) as Box<dyn Read + Send>)
            .ok_or(IndexCacheError::InvalidFetchedSegment)
    }
}

#[tonic::async_trait]
impl IndexSegmentFetcher for GatedFetcher {
    async fn fetch(
        &self,
        _segment: IndexSegmentId,
    ) -> Result<Box<dyn Read + Send>, IndexCacheError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.release.acquire().await.unwrap().forget();
        Ok(Box::new(std::io::Cursor::new(self.value.clone())))
    }
}

#[tonic::async_trait]
impl IndexSegmentFetcher for MemoryFetcher {
    async fn fetch(
        &self,
        segment: IndexSegmentId,
    ) -> Result<Box<dyn Read + Send>, IndexCacheError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.values
            .get(&segment)
            .cloned()
            .map(|bytes| Box::new(std::io::Cursor::new(bytes)) as Box<dyn Read + Send>)
            .ok_or(IndexCacheError::InvalidFetchedSegment)
    }
}

fn id(bytes: &[u8]) -> IndexSegmentId {
    IndexSegmentId::new(*blake3::hash(bytes).as_bytes(), bytes.len() as u64).unwrap()
}

fn fixture(
    root: &TempDir,
    disk_bytes: u64,
    memory_bytes: u64,
) -> (
    IndexCache,
    Arc<MemoryFetcher>,
    IndexSegmentId,
    IndexSegmentId,
) {
    let left = b"abcdefgh";
    let right = b"ijklmnop";
    let left_id = id(left);
    let right_id = id(right);
    let fetcher = Arc::new(MemoryFetcher {
        values: BTreeMap::from([(left_id, left.to_vec()), (right_id, right.to_vec())]),
        reads: AtomicUsize::new(0),
    });
    let cache = IndexCache::new(
        root.path(),
        IndexCacheConfig::new(disk_bytes, memory_bytes).unwrap(),
        fetcher.clone(),
    )
    .unwrap();
    (cache, fetcher, left_id, right_id)
}

#[tokio::test]
async fn read_at_returns_owned_exact_available_slice_without_a_mut_borrow() {
    let root = tempfile::tempdir().unwrap();
    let (cache, fetcher, left, right) = fixture(&root, 1024, 1024);
    let file = cache.open(left);

    let slice = file.read_at(2, 20).await.unwrap();
    assert_eq!(slice.data(), b"cdefgh");
    assert_eq!(fetcher.reads.load(Ordering::Relaxed), 1);

    let next = cache.open(right).read_at(0, 3).await.unwrap();
    assert_eq!(&*next, b"ijk");
    assert_eq!(file.read_at(8, 10).await.unwrap().data(), b"");
    assert_eq!(file.read_at(0, 0).await.unwrap().data(), b"");
}

#[tokio::test]
async fn repeated_reads_reuse_the_verified_mapping() {
    let root = tempfile::tempdir().unwrap();
    let (cache, fetcher, left, _right) = fixture(&root, 1024, 1024);
    let file = cache.open(left);

    let first = file.read_at(0, 4).await.unwrap();
    let second = file.read_at(4, 4).await.unwrap();
    assert_eq!(first.data(), b"abcd");
    assert_eq!(second.data(), b"efgh");
    assert_eq!(fetcher.reads.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn successful_stream_copy_atomically_publishes_the_verified_file() {
    let root = tempfile::tempdir().unwrap();
    let bytes = (0..(3 * 64 * 1024 + 17))
        .map(|offset| (offset % 251) as u8)
        .collect::<Vec<_>>();
    let segment = id(&bytes);
    let fetcher = Arc::new(MemoryFetcher {
        values: BTreeMap::from([(segment, bytes.clone())]),
        reads: AtomicUsize::new(0),
    });
    let cache = IndexCache::new(
        root.path(),
        IndexCacheConfig::new(bytes.len() as u64, bytes.len() as u64).unwrap(),
        fetcher.clone(),
    )
    .unwrap();

    let slice = cache.open(segment).read_at(0, bytes.len()).await.unwrap();

    assert_eq!(slice.data(), bytes);
    assert_eq!(fetcher.reads.load(Ordering::Relaxed), 1);
    assert_eq!(
        std::fs::read(cache_path(&cache.inner.directory, segment)).unwrap(),
        bytes
    );
    assert_eq!(
        std::fs::read_dir(&cache.inner.directory).unwrap().count(),
        1,
        "verified publication must leave no staging file"
    );
}

#[tokio::test]
async fn concurrent_cold_reads_share_one_fetch() {
    let root = tempfile::tempdir().unwrap();
    let (cache, fetcher, left, _right) = fixture(&root, 1024, 1024);
    let first = cache.open(left);
    let second = cache.open(left);

    let (first, second) = tokio::join!(first.read_at(0, 8), second.read_at(0, 8));
    assert_eq!(first.unwrap().data(), b"abcdefgh");
    assert_eq!(second.unwrap().data(), b"abcdefgh");
    assert_eq!(fetcher.reads.load(Ordering::Relaxed), 1);
    assert_eq!(cache.inner.state.lock().unwrap().in_flight_bytes, 0);
}

#[tokio::test]
async fn cancelled_cold_fetch_releases_single_flight_waiters() {
    let root = tempfile::tempdir().unwrap();
    let value = b"cancel-safe".to_vec();
    let segment = id(&value);
    let fetcher = Arc::new(GatedFetcher {
        value,
        reads: AtomicUsize::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let cache = IndexCache::new(
        root.path(),
        IndexCacheConfig::new(1024, 1024).unwrap(),
        fetcher.clone(),
    )
    .unwrap();

    let first_file = cache.open(segment);
    let first = tokio::spawn(async move { first_file.read_at(0, 32).await });
    while fetcher.reads.load(Ordering::Relaxed) == 0 {
        tokio::task::yield_now().await;
    }
    first.abort();
    let _ = first.await;

    let second_file = cache.open(segment);
    let second = tokio::spawn(async move { second_file.read_at(0, 32).await });
    while fetcher.reads.load(Ordering::Relaxed) < 2 {
        tokio::task::yield_now().await;
    }
    fetcher.release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .data(),
        b"cancel-safe"
    );
    assert_eq!(cache.inner.state.lock().unwrap().in_flight_bytes, 0);
}

#[tokio::test]
async fn distinct_cold_fetches_share_the_aggregate_memory_allowance() {
    let root = tempfile::tempdir().unwrap();
    let left_bytes = b"abcdefgh".to_vec();
    let right_bytes = b"ijklmnop".to_vec();
    let left = id(&left_bytes);
    let right = id(&right_bytes);
    let fetcher = Arc::new(MultiGatedFetcher {
        values: BTreeMap::from([(left, left_bytes), (right, right_bytes)]),
        reads: AtomicUsize::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let cache = IndexCache::new(
        root.path(),
        IndexCacheConfig::new(1024, 8).unwrap(),
        fetcher.clone(),
    )
    .unwrap();
    let left_file = cache.open(left);
    let right_file = cache.open(right);
    let left_task = tokio::spawn(async move { left_file.read_at(0, 8).await });
    let right_task = tokio::spawn(async move { right_file.read_at(0, 8).await });

    while fetcher.reads.load(Ordering::Relaxed) == 0 {
        tokio::task::yield_now().await;
    }
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let queued = {
                let state = cache.inner.fetch_budget.inner.state.lock().unwrap();
                state.used == 8 && state.waiters.len() == 1
            };
            if queued {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the second cold fetch must queue behind the aggregate memory permit");
    assert_eq!(fetcher.reads.load(Ordering::Relaxed), 1);
    {
        let state = cache.inner.fetch_budget.inner.state.lock().unwrap();
        assert_eq!(state.used, 8);
        assert_eq!(state.waiters.len(), 1);
    }

    fetcher.release.add_permits(1);
    while fetcher.reads.load(Ordering::Relaxed) < 2 {
        tokio::task::yield_now().await;
    }
    fetcher.release.add_permits(1);
    assert_eq!(left_task.await.unwrap().unwrap().len(), 8);
    assert_eq!(right_task.await.unwrap().unwrap().len(), 8);
    assert_eq!(cache.inner.fetch_budget.inner.state.lock().unwrap().used, 0);
}

#[tokio::test]
async fn pinned_slices_may_temporarily_exceed_the_disk_budget() {
    let root = tempfile::tempdir().unwrap();
    let (cache, _fetcher, left, right) = fixture(&root, 8, 8);
    let file = cache.open(left);
    let other_file = cache.open(right);

    let held = file.read_at(0, 8).await.unwrap();
    let other = other_file.read_at(0, 8).await.unwrap();
    assert_eq!(held.data(), b"abcdefgh");
    assert_eq!(other.data(), b"ijklmnop");

    // Both mappings remain valid even though the configured pool holds only
    // one segment. A later cache operation can evict them after these guards
    // are dropped.
    assert_eq!(held.data(), b"abcdefgh");
}

#[tokio::test]
async fn tiny_segments_cannot_leave_retained_mappings_above_the_memory_budget() {
    let root = tempfile::tempdir().unwrap();
    let values = (0_u8..32)
        .map(|byte| {
            let bytes = vec![byte];
            (id(&bytes), bytes)
        })
        .collect::<BTreeMap<_, _>>();
    let fetcher = Arc::new(MemoryFetcher {
        values: values.clone(),
        reads: AtomicUsize::new(0),
    });
    let memory_bytes = CACHE_MAPPING_MINIMUM_BYTES * 3;
    let cache = IndexCache::new(
        root.path(),
        IndexCacheConfig::new(1024, memory_bytes).unwrap(),
        fetcher.clone(),
    )
    .unwrap();

    for (segment, bytes) in &values {
        let slice = cache.open(*segment).read_at(0, 1).await.unwrap();
        assert_eq!(slice.data(), bytes);
        drop(slice);
        let state = cache.inner.state.lock().unwrap();
        assert!(state.memory_bytes <= memory_bytes);
        assert!(
            state
                .entries
                .values()
                .filter(|entry| entry.mapped.is_some())
                .count()
                <= 3
        );
    }

    assert_eq!(fetcher.reads.load(Ordering::Relaxed), values.len());
    assert_eq!(
        std::fs::read_dir(&cache.inner.directory).unwrap().count(),
        values.len(),
        "memory eviction must retain independently budgeted disk files"
    );
}

#[tokio::test]
async fn memory_eviction_keeps_disk_entries_until_the_independent_disk_limit() {
    let root = tempfile::tempdir().unwrap();
    let values = (0_u8..4)
        .map(|byte| {
            let bytes = vec![byte];
            (id(&bytes), bytes)
        })
        .collect::<BTreeMap<_, _>>();
    let fetcher = Arc::new(MemoryFetcher {
        values: values.clone(),
        reads: AtomicUsize::new(0),
    });
    let cache = IndexCache::new(
        root.path(),
        IndexCacheConfig::new(3, CACHE_MAPPING_MINIMUM_BYTES).unwrap(),
        fetcher.clone(),
    )
    .unwrap();

    for (segment, bytes) in &values {
        let slice = cache.open(*segment).read_at(0, 1).await.unwrap();
        assert_eq!(slice.data(), bytes);
        drop(slice);
    }

    let state = cache.inner.state.lock().unwrap();
    assert_eq!(state.disk_bytes, 3);
    assert_eq!(state.memory_bytes, CACHE_MAPPING_MINIMUM_BYTES);
    assert_eq!(state.entries.len(), 3);
    assert_eq!(
        state
            .entries
            .values()
            .filter(|entry| entry.mapped.is_some())
            .count(),
        1
    );
    drop(state);
    assert_eq!(
        std::fs::read_dir(&cache.inner.directory).unwrap().count(),
        3
    );

    let disk_only = cache
        .inner
        .state
        .lock()
        .unwrap()
        .entries
        .iter()
        .find_map(|(id, entry)| entry.mapped.is_none().then_some(*id))
        .unwrap();
    let remapped = cache.open(disk_only).read_at(0, 1).await.unwrap();
    assert_eq!(remapped.data(), values.get(&disk_only).unwrap());
    assert_eq!(fetcher.reads.load(Ordering::Relaxed), 4);
    let state = cache.inner.state.lock().unwrap();
    assert_eq!(state.disk_bytes, 3, "a remap must not charge disk twice");
    assert_eq!(state.memory_bytes, CACHE_MAPPING_MINIMUM_BYTES);
}

#[tokio::test]
async fn live_slices_pin_mappings_until_the_last_over_budget_pin_drops() {
    let root = tempfile::tempdir().unwrap();
    let (cache, _fetcher, left, right) = fixture(&root, 1024, CACHE_MAPPING_MINIMUM_BYTES);

    let held = cache.open(left).read_at(0, 8).await.unwrap();
    let held_clone = held.clone();
    let temporary = cache.open(right).read_at(0, 8).await.unwrap();
    assert_eq!(cache.inner.state.lock().unwrap().memory_bytes, 8 * 1024);
    assert_eq!(held.data(), b"abcdefgh");
    assert_eq!(temporary.data(), b"ijklmnop");

    drop(temporary);
    assert_eq!(cache.inner.state.lock().unwrap().memory_bytes, 4 * 1024);
    assert_eq!(held.data(), b"abcdefgh");
    drop(held);
    assert_eq!(held_clone.data(), b"abcdefgh");
}

#[tokio::test]
async fn reconciliation_reclaims_a_temporary_pin_overrun_after_the_slice_drops() {
    let root = tempfile::tempdir().unwrap();
    let (cache, _fetcher, left, _right) = fixture(&root, 4, 8);
    let path = cache_path(&cache.inner.directory, left);

    let held = cache.open(left).read_at(0, 8).await.unwrap();
    assert!(path.exists());
    assert_eq!(cache.inner.state.lock().unwrap().disk_bytes, 8);

    let pinned =
        reconcile_cache_step(&cache.inner, 8, u64::MAX, std::time::Duration::from_secs(1)).unwrap();
    assert_eq!(pinned.removed_bytes, 0);
    assert!(path.exists());

    drop(held);
    let unpinned =
        reconcile_cache_step(&cache.inner, 8, u64::MAX, std::time::Duration::from_secs(1)).unwrap();
    assert_eq!(unpinned.removed_bytes, 8);
    assert!(!path.exists());
    assert_eq!(cache.inner.state.lock().unwrap().disk_bytes, 0);
}

#[tokio::test]
async fn corrupt_fetched_bytes_are_never_materialized() {
    let root = tempfile::tempdir().unwrap();
    let expected = id(b"expected");
    let fetcher = Arc::new(MemoryFetcher {
        values: BTreeMap::from([(expected, b"different".to_vec())]),
        reads: AtomicUsize::new(0),
    });
    let cache = IndexCache::new(
        root.path(),
        IndexCacheConfig::new(1024, 1024).unwrap(),
        fetcher,
    )
    .unwrap();
    let file = cache.open(expected);

    assert!(matches!(
        file.read_at(0, 8).await,
        Err(IndexCacheError::InvalidFetchedSegment)
    ));
    assert!(!cache_path(&cache.inner.directory, expected).exists());
    assert_eq!(
        std::fs::read_dir(&cache.inner.directory).unwrap().count(),
        0,
        "failed verification must remove its unpublished cache temporary"
    );
}

#[tokio::test]
async fn interrupted_stream_never_publishes_a_partial_cache_file() {
    let root = tempfile::tempdir().unwrap();
    let value = b"interrupted immutable artifact".to_vec();
    let segment = id(&value);
    let cache = IndexCache::new(
        root.path(),
        IndexCacheConfig::new(1024, 1024).unwrap(),
        Arc::new(InterruptedFetcher { value }),
    )
    .unwrap();

    assert!(matches!(
        cache.open(segment).read_at(0, 32).await,
        Err(IndexCacheError::Fetch(reason)) if reason.contains("fixture interrupted")
    ));
    assert!(!cache_path(&cache.inner.directory, segment).exists());
    assert_eq!(
        std::fs::read_dir(&cache.inner.directory).unwrap().count(),
        0,
        "an interrupted copy must remove its unpublished cache temporary"
    );
}

#[tokio::test]
async fn corrupt_existing_cache_file_is_atomically_replaced() {
    let root = tempfile::tempdir().unwrap();
    let (cache, fetcher, left, _right) = fixture(&root, 1024, 1024);
    let path = cache_path(&cache.inner.directory, left);
    std::fs::write(&path, b"corrupt!").unwrap();

    let slice = cache.open(left).read_at(0, 8).await.unwrap();

    assert_eq!(slice.data(), b"abcdefgh");
    assert_eq!(std::fs::read(path).unwrap(), b"abcdefgh");
    assert_eq!(fetcher.reads.load(Ordering::Relaxed), 1);
}

#[test]
fn startup_preserves_the_disposable_v4_cache_directory_without_inventory() {
    let root = tempfile::tempdir().unwrap();
    let cache_directory = root.path().join(CACHE_FORMAT_DIRECTORY);
    let sibling = root.path().join("must-remain");
    std::fs::create_dir_all(&cache_directory).unwrap();
    std::fs::write(cache_directory.join("stale-cache-file"), b"stale").unwrap();
    std::fs::write(&sibling, b"ordinary-data").unwrap();
    let fetcher = Arc::new(MemoryFetcher {
        values: BTreeMap::new(),
        reads: AtomicUsize::new(0),
    });

    let cache = IndexCache::new(
        root.path(),
        IndexCacheConfig::new(1024, 1024).unwrap(),
        fetcher,
    )
    .unwrap();

    assert!(cache_directory.is_dir());
    assert_eq!(
        std::fs::read(cache_directory.join("stale-cache-file")).unwrap(),
        b"stale"
    );
    assert_eq!(std::fs::read(sibling).unwrap(), b"ordinary-data");
    assert!(!cache.inner.reconciler_started.load(Ordering::Acquire));
}

#[tokio::test]
async fn cache_reconciler_starts_explicitly_and_only_once() {
    let root = tempfile::tempdir().unwrap();
    let (cache, _fetcher, _left, _right) = fixture(&root, 1024, 1024);

    assert!(!cache.inner.reconciler_started.load(Ordering::Acquire));
    assert!(cache.start_reconciler());
    assert!(cache.inner.reconciler_started.load(Ordering::Acquire));
    assert!(!cache.start_reconciler());
}

#[test]
fn cache_inventory_is_counted_when_it_runs_inside_the_startup_window() {
    let root = tempfile::tempdir().unwrap();
    let evidence = crate::startup_scan_evidence::StartupScanEvidence::begin();
    let cache = IndexCache::new_with_startup_scan_evidence(
        root.path(),
        IndexCacheConfig::new(1024, 1024).unwrap(),
        Arc::new(MemoryFetcher {
            values: BTreeMap::new(),
            reads: AtomicUsize::new(0),
        }),
        evidence.clone(),
    )
    .unwrap();

    reconcile_cache_step(&cache.inner, 1, u64::MAX, std::time::Duration::from_secs(1)).unwrap();

    assert_eq!(evidence.finish().global_cache_scans_total, 1);
}

#[tokio::test]
async fn valid_warm_file_is_verified_and_reused_without_a_fetch() {
    let root = tempfile::tempdir().unwrap();
    let bytes = b"warm-cache".to_vec();
    let segment = id(&bytes);
    let directory = root.path().join(CACHE_FORMAT_DIRECTORY);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(cache_path(&directory, segment), &bytes).unwrap();
    let fetcher = Arc::new(MemoryFetcher {
        values: BTreeMap::from([(segment, bytes.clone())]),
        reads: AtomicUsize::new(0),
    });

    let cache = IndexCache::new(
        root.path(),
        IndexCacheConfig::new(1024, 1024).unwrap(),
        fetcher.clone(),
    )
    .unwrap();
    let slice = cache.open(segment).read_at(0, bytes.len()).await.unwrap();

    assert_eq!(slice.data(), bytes);
    assert_eq!(fetcher.reads.load(Ordering::Relaxed), 0);
}

#[test]
fn reconciliation_advances_in_bounded_steps_and_evicts_only_after_budget() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join(CACHE_FORMAT_DIRECTORY);
    std::fs::create_dir_all(&directory).unwrap();
    for value in [b"first".as_slice(), b"second", b"third"] {
        let segment = id(value);
        std::fs::write(cache_path(&directory, segment), value).unwrap();
    }
    let cache = IndexCache::new(
        root.path(),
        IndexCacheConfig::new(6, 1024).unwrap(),
        Arc::new(MemoryFetcher {
            values: BTreeMap::new(),
            reads: AtomicUsize::new(0),
        }),
    )
    .unwrap();

    let mut completed = false;
    for _ in 0..8 {
        let progress =
            reconcile_cache_step(&cache.inner, 1, u64::MAX, std::time::Duration::from_secs(1))
                .unwrap();
        assert!(progress.records <= 1);
        completed |= progress.completed_cycle;
        if completed {
            break;
        }
    }

    assert!(completed);
    let retained = std::fs::read_dir(directory).unwrap().count();
    assert_eq!(retained, 1);
}

#[test]
fn reconciliation_removes_unknown_regular_files_but_not_directories() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join(CACHE_FORMAT_DIRECTORY);
    std::fs::create_dir_all(directory.join("operator-directory")).unwrap();
    std::fs::write(directory.join("interrupted-junk"), b"junk").unwrap();
    let cache = IndexCache::new(
        root.path(),
        IndexCacheConfig::new(1024, 1024).unwrap(),
        Arc::new(MemoryFetcher {
            values: BTreeMap::new(),
            reads: AtomicUsize::new(0),
        }),
    )
    .unwrap();

    let mut completed = false;
    for _ in 0..8 {
        completed |=
            reconcile_cache_step(&cache.inner, 1, u64::MAX, std::time::Duration::from_secs(1))
                .unwrap()
                .completed_cycle;
        if completed {
            break;
        }
    }

    assert!(completed);
    assert!(!directory.join("interrupted-junk").exists());
    assert!(directory.join("operator-directory").is_dir());
}

#[test]
fn configurable_reconciliation_rejects_zero_budgets() {
    let invalid = CacheReconcileConfig::new(
        std::time::Duration::from_secs(1),
        0,
        1,
        std::time::Duration::from_millis(1),
    );
    assert!(matches!(
        invalid,
        Err(IndexCacheError::InvalidConfiguration(_))
    ));
}

#[tokio::test]
async fn small_segments_remain_mmap_backed() {
    let root = tempfile::tempdir().unwrap();
    let (cache, fetcher, left, _right) = fixture(&root, 1024, 8);
    let file = cache.open(left);

    let first = file.read_at(0, 4).await.unwrap();
    let second = file.read_at(4, 4).await.unwrap();
    assert!(first.is_mmap_backed());
    assert!(second.is_mmap_backed());
    assert_eq!(fetcher.reads.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn oversized_segments_remain_mmap_backed() {
    let root = tempfile::tempdir().unwrap();
    let (cache, _fetcher, left, _right) = fixture(&root, 1024, 4);
    let file = cache.open(left);

    let slice = file.read_at(0, 8).await.unwrap();
    assert!(slice.is_mmap_backed());
    assert_eq!(slice.data(), b"abcdefgh");
    assert_eq!(
        cache.inner.state.lock().unwrap().memory_bytes,
        CACHE_MAPPING_MINIMUM_BYTES
    );
    drop(slice);
    let state = cache.inner.state.lock().unwrap();
    assert_eq!(state.memory_bytes, 0);
    assert!(state.entries.get(&left).unwrap().mapped.is_none());
}

#[tokio::test]
async fn merge_scratch_is_random_access_and_removed_on_drop() {
    let root = tempfile::tempdir().unwrap();
    let (cache, _fetcher, _left, _right) = fixture(&root, 1024, 1024);
    let scratch = cache.merge_scratch();
    let file = scratch.create_file().await.unwrap();
    let path = file.inner.path.clone();

    file.resize_zeroed(8).await.unwrap();
    assert_eq!(file.read_exact_at(0, 8).await.unwrap(), vec![0; 8]);
    assert!(matches!(
        file.resize_zeroed(7).await,
        Err(IndexError::InvalidDefinition(_))
    ));
    assert_eq!(
        file.write_all_at(7, vec![1, 2]).await.unwrap_err(),
        IndexError::UnexpectedEof {
            expected: 9,
            actual: 8,
        }
    );
    file.write_all_at(2, vec![1, 2, 3]).await.unwrap();
    assert_eq!(file.append(vec![9, 8]).await.unwrap(), 8);
    assert_eq!(file.len().await.unwrap(), 10);
    assert_eq!(
        file.read_exact_at(1, 8).await.unwrap(),
        vec![0, 1, 2, 3, 0, 0, 0, 9]
    );
    assert!(
        cache
            .inner
            .state
            .lock()
            .unwrap()
            .active_scratch
            .contains(&path)
    );

    let second_lane = file.clone();
    drop(file);
    assert!(path.exists());
    assert_eq!(second_lane.read_exact_at(8, 2).await.unwrap(), vec![9, 8]);
    drop(second_lane);
    assert!(!path.exists());
    assert!(
        !cache
            .inner
            .state
            .lock()
            .unwrap()
            .active_scratch
            .contains(&path)
    );
}

#[tokio::test]
async fn merge_scratch_uses_its_configured_root_and_remains_disposable() {
    let root = tempfile::tempdir().unwrap();
    let cache_root = root.path().join("cache");
    let scratch_root = root.path().join("scratch");
    let cache = IndexCache::new_with_directories_and_startup_scan_evidence(
        &cache_root,
        &scratch_root,
        IndexCacheConfig::new(1024, 1024).unwrap(),
        Arc::new(MemoryFetcher {
            values: BTreeMap::new(),
            reads: AtomicUsize::new(0),
        }),
        crate::startup_scan_evidence::StartupScanEvidence::begin(),
    )
    .unwrap();
    let file = cache.merge_scratch().create_file().await.unwrap();
    let path = file.inner.path.clone();

    assert!(path.starts_with(scratch_root.join(SCRATCH_FORMAT_DIRECTORY)));
    assert!(!path.starts_with(cache_root.join(CACHE_FORMAT_DIRECTORY)));
    assert!(path.exists());

    drop(cache);
    assert!(path.exists());
    drop(file);
    assert!(!path.exists());
}

#[tokio::test]
async fn merge_scratch_short_read_reports_exact_file_length() {
    let root = tempfile::tempdir().unwrap();
    let (cache, _fetcher, _left, _right) = fixture(&root, 1024, 1024);
    let scratch = cache.merge_scratch();
    let file = scratch.create_file().await.unwrap();
    file.append(vec![1, 2, 3]).await.unwrap();

    assert_eq!(
        file.read_exact_at(2, 2).await.unwrap_err(),
        IndexError::UnexpectedEof {
            expected: 4,
            actual: 3,
        }
    );
}
