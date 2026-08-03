use std::sync::atomic::{AtomicUsize, Ordering};

use tempfile::TempDir;

use super::*;

struct MemoryFetcher {
    values: BTreeMap<IndexSegmentId, Vec<u8>>,
    reads: AtomicUsize,
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

fn file(cache: &IndexCache, left: IndexSegmentId, right: IndexSegmentId) -> IndexFile {
    cache.open(
        IndexFileLayout::new(vec![
            IndexSegment {
                logical_offset: 0,
                id: left,
            },
            IndexSegment {
                logical_offset: left.length,
                id: right,
            },
        ])
        .unwrap(),
    )
}

#[tokio::test]
async fn read_at_returns_owned_exact_available_slice_without_a_mut_borrow() {
    let root = tempfile::tempdir().unwrap();
    let (cache, fetcher, left, right) = fixture(&root, 1024, 1024);
    let file = file(&cache, left, right);

    let slice = file.read_at(2, 20).await.unwrap();
    assert_eq!(slice.data(), b"cdefgh");
    assert_eq!(fetcher.reads.load(Ordering::Relaxed), 1);

    let next = file.read_at(8, 3).await.unwrap();
    assert_eq!(&*next, b"ijk");
    assert_eq!(file.read_at(16, 10).await.unwrap().data(), b"");
    assert_eq!(file.read_at(0, 0).await.unwrap().data(), b"");
}

#[tokio::test]
async fn repeated_reads_reuse_the_verified_mapping() {
    let root = tempfile::tempdir().unwrap();
    let (cache, fetcher, left, right) = fixture(&root, 1024, 1024);
    let file = file(&cache, left, right);

    let first = file.read_at(0, 4).await.unwrap();
    let second = file.read_at(4, 4).await.unwrap();
    assert_eq!(first.data(), b"abcd");
    assert_eq!(second.data(), b"efgh");
    assert_eq!(fetcher.reads.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn pinned_slices_may_temporarily_exceed_the_disk_budget() {
    let root = tempfile::tempdir().unwrap();
    let (cache, _fetcher, left, right) = fixture(&root, 8, 8);
    let file = file(&cache, left, right);

    let held = file.read_at(0, 8).await.unwrap();
    let other = file.read_at(8, 8).await.unwrap();
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
    let file = cache.open(
        IndexFileLayout::new(vec![IndexSegment {
            logical_offset: 0,
            id: expected,
        }])
        .unwrap(),
    );

    assert!(matches!(
        file.read_at(0, 8).await,
        Err(IndexCacheError::InvalidFetchedSegment)
    ));
}

#[tokio::test]
async fn small_segments_use_the_shared_memory_tier() {
    let root = tempfile::tempdir().unwrap();
    let (cache, fetcher, left, right) = fixture(&root, 1024, 8);
    let file = file(&cache, left, right);

    let first = file.read_at(0, 4).await.unwrap();
    let second = file.read_at(4, 4).await.unwrap();
    assert!(first.is_memory_backed());
    assert!(second.is_memory_backed());
    assert_eq!(fetcher.reads.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn oversized_segments_remain_mmap_backed() {
    let root = tempfile::tempdir().unwrap();
    let (cache, _fetcher, left, right) = fixture(&root, 1024, 4);
    let file = file(&cache, left, right);

    let slice = file.read_at(0, 8).await.unwrap();
    assert!(!slice.is_memory_backed());
    assert_eq!(slice.data(), b"abcdefgh");
}

#[tokio::test]
async fn pinned_memory_slices_may_temporarily_exceed_the_budget() {
    let root = tempfile::tempdir().unwrap();
    let (cache, _fetcher, left, right) = fixture(&root, 1024, 8);
    let file = file(&cache, left, right);

    let held = file.read_at(0, 8).await.unwrap();
    let other = file.read_at(8, 8).await.unwrap();
    assert!(held.is_memory_backed());
    assert!(other.is_memory_backed());
    assert_eq!(held.data(), b"abcdefgh");
    assert_eq!(other.data(), b"ijklmnop");

    drop(held);
    drop(other);
    let _ = file.read_at(0, 1).await.unwrap();
    let state = cache.inner.state.lock().unwrap();
    assert!(state.memory_bytes <= 8);
}
