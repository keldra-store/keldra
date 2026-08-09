use std::sync::atomic::{AtomicUsize, Ordering};

use tempfile::TempDir;

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

#[tonic::async_trait]
impl IndexSegmentFetcher for MultiGatedFetcher {
    async fn fetch(&self, segment: IndexSegmentId) -> Result<Vec<u8>, IndexCacheError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.release.acquire().await.unwrap().forget();
        self.values
            .get(&segment)
            .cloned()
            .ok_or(IndexCacheError::InvalidFetchedSegment)
    }
}

#[tonic::async_trait]
impl IndexSegmentFetcher for GatedFetcher {
    async fn fetch(&self, _segment: IndexSegmentId) -> Result<Vec<u8>, IndexCacheError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.release.acquire().await.unwrap().forget();
        Ok(self.value.clone())
    }
}

#[tonic::async_trait]
impl IndexSegmentFetcher for MemoryFetcher {
    async fn fetch(&self, segment: IndexSegmentId) -> Result<Vec<u8>, IndexCacheError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.values
            .get(&segment)
            .cloned()
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
    tokio::task::yield_now().await;
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
fn startup_clears_only_the_disposable_v2_cache_directory() {
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

    let _cache = IndexCache::new(
        root.path(),
        IndexCacheConfig::new(1024, 1024).unwrap(),
        fetcher,
    )
    .unwrap();

    assert!(cache_directory.is_dir());
    assert_eq!(std::fs::read_dir(cache_directory).unwrap().count(), 0);
    assert_eq!(std::fs::read(sibling).unwrap(), b"ordinary-data");
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
}
