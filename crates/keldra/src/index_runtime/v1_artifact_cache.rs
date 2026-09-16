use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use keldra_index::v1::{DecodedQueryBlock, ProjectionQueryRunDescriptor, QueryBlockDescriptor};
use keldra_store::BlobRef;
use tonic::Status;

const CAPACITY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct Authority {
    tenant_id: u64,
    bucket_id: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Key {
    authority: Authority,
    path: Arc<str>,
    hash: [u8; 32],
    object_version: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct BlobKey {
    hash: [u8; 32],
    length: u64,
}

impl From<&BlobRef> for BlobKey {
    fn from(blob: &BlobRef) -> Self {
        Self {
            hash: blob.hash,
            length: blob.length,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CacheKey {
    Path(Key),
    Blob(BlobKey),
    QueryRun(BlobKey),
    QueryBlock(QueryBlockKey),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct QueryBlockKey {
    generation: [u8; 32],
    block: BlobKey,
}

struct State {
    capacity_bytes: usize,
    bytes: usize,
    // Nest by authority and path so hits can compare the caller's borrowed
    // path without allocating another owned copy.
    entries: HashMap<Authority, HashMap<Arc<str>, HashMap<([u8; 32], Option<u64>), Bytes>>>,
    blobs: HashMap<BlobKey, CachedBlob>,
    query_runs: HashMap<BlobKey, CachedQueryRun>,
    query_blocks: HashMap<QueryBlockKey, CachedQueryBlock>,
    query_run_loading: HashMap<BlobKey, tokio::sync::watch::Sender<bool>>,
    query_block_loading: HashMap<QueryBlockKey, tokio::sync::watch::Sender<bool>>,
    loading: HashMap<Key, tokio::sync::watch::Sender<bool>>,
    fifo: VecDeque<CacheKey>,
}

struct CachedBlob {
    bytes: Bytes,
}

struct CachedQueryRun {
    descriptor: Arc<ProjectionQueryRunDescriptor>,
    resident_bytes: usize,
}

struct CachedQueryBlock {
    block: Arc<DecodedQueryBlock>,
    resident_bytes: usize,
}

impl Default for State {
    fn default() -> Self {
        Self {
            capacity_bytes: CAPACITY_BYTES,
            bytes: 0,
            entries: HashMap::new(),
            blobs: HashMap::new(),
            query_runs: HashMap::new(),
            query_blocks: HashMap::new(),
            query_run_loading: HashMap::new(),
            query_block_loading: HashMap::new(),
            loading: HashMap::new(),
            fifo: VecDeque::new(),
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct ImmutableArtifactCache(Arc<Mutex<State>>);

struct PathLoadGuard {
    cache: ImmutableArtifactCache,
    key: Option<Key>,
}

impl Drop for PathLoadGuard {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        let completed = self
            .cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .loading
            .remove(&key);
        if let Some(completed) = completed {
            completed.send_replace(true);
        }
    }
}

enum PathLoad {
    Hit(Bytes),
    Wait(tokio::sync::watch::Receiver<bool>),
    Lead(PathLoadGuard),
}

#[derive(Clone, Copy)]
enum DecodeKey {
    Run(BlobKey),
    Block(QueryBlockKey),
}

pub(super) struct DecodeLoadGuard {
    cache: ImmutableArtifactCache,
    key: Option<DecodeKey>,
}

impl Drop for DecodeLoadGuard {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        let mut state = self
            .cache
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let completed = match key {
            DecodeKey::Run(key) => state.query_run_loading.remove(&key),
            DecodeKey::Block(key) => state.query_block_loading.remove(&key),
        };
        if let Some(completed) = completed {
            completed.send_replace(true);
        }
    }
}

pub(super) enum DecodePopulation {
    Completed,
    Lead(DecodeLoadGuard),
}

impl ImmutableArtifactCache {
    pub(super) async fn coordinate_query_run(
        &self,
        blob: &BlobRef,
    ) -> Result<DecodePopulation, Status> {
        let key = BlobKey::from(blob);
        let wait = {
            let mut state = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(completed) = state.query_run_loading.get(&key) {
                Some(completed.subscribe())
            } else {
                let (completed, _) = tokio::sync::watch::channel(false);
                state.query_run_loading.insert(key, completed);
                None
            }
        };
        self.finish_decode_coordination(DecodeKey::Run(key), wait)
            .await
    }

    pub(super) async fn coordinate_query_block(
        &self,
        blob: &BlobRef,
        generation: [u8; 32],
    ) -> Result<DecodePopulation, Status> {
        let key = QueryBlockKey {
            generation,
            block: BlobKey::from(blob),
        };
        let wait = {
            let mut state = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(completed) = state.query_block_loading.get(&key) {
                Some(completed.subscribe())
            } else {
                let (completed, _) = tokio::sync::watch::channel(false);
                state.query_block_loading.insert(key, completed);
                None
            }
        };
        self.finish_decode_coordination(DecodeKey::Block(key), wait)
            .await
    }

    async fn finish_decode_coordination(
        &self,
        key: DecodeKey,
        wait: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Result<DecodePopulation, Status> {
        if let Some(mut completed) = wait {
            if !*completed.borrow() {
                completed
                    .changed()
                    .await
                    .map_err(|_| Status::internal("v1 decoded artifact load coordinator closed"))?;
            }
            Ok(DecodePopulation::Completed)
        } else {
            Ok(DecodePopulation::Lead(DecodeLoadGuard {
                cache: self.clone(),
                key: Some(key),
            }))
        }
    }
    /// Coalesce simultaneous misses for one exact immutable object identity.
    ///
    /// Projection lanes commonly seek different records through the same
    /// stream page or pack. The first lane performs the authoritative read;
    /// peers retain their own loader and retry only if that read failed before
    /// populating the cache.
    pub(super) async fn get_or_load<F, Fut>(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        hash: [u8; 32],
        object_version: Option<u64>,
        maximum_bytes: usize,
        load: F,
    ) -> Result<Option<Bytes>, Status>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Option<Bytes>, Status>>,
    {
        let mut load = Some(load);
        loop {
            match self.begin_path_load(
                tenant_id,
                bucket_id,
                path,
                hash,
                object_version,
                maximum_bytes,
            )? {
                PathLoad::Hit(bytes) => return Ok(Some(bytes)),
                PathLoad::Wait(mut completed) => {
                    if !*completed.borrow() {
                        completed.changed().await.map_err(|_| {
                            Status::internal("v1 immutable artifact load coordinator closed")
                        })?;
                    }
                }
                PathLoad::Lead(guard) => {
                    let load = load
                        .take()
                        .expect("one artifact caller can lead at most once");
                    let result = load().await;
                    if let Ok(Some(bytes)) = &result {
                        if bytes.len() > maximum_bytes {
                            return Err(Status::data_loss(
                                "v1 projection artifact violates its exact byte bound",
                            ));
                        }
                        self.insert(
                            tenant_id,
                            bucket_id,
                            path,
                            hash,
                            object_version,
                            bytes.clone(),
                        );
                    }
                    drop(guard);
                    return result;
                }
            }
        }
    }

    fn begin_path_load(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        hash: [u8; 32],
        object_version: Option<u64>,
        maximum_bytes: usize,
    ) -> Result<PathLoad, Status> {
        let authority = Authority {
            tenant_id,
            bucket_id,
        };
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(bytes) = state
            .entries
            .get(&authority)
            .and_then(|paths| paths.get(path))
            .and_then(|hashes| hashes.get(&(hash, object_version)))
        {
            if bytes.len() > maximum_bytes {
                return Err(Status::data_loss(
                    "v1 projection artifact violates its exact byte bound",
                ));
            }
            return Ok(PathLoad::Hit(bytes.clone()));
        }
        let key = Key {
            authority,
            path: Arc::from(path),
            hash,
            object_version,
        };
        if let Some(completed) = state.loading.get(&key) {
            return Ok(PathLoad::Wait(completed.subscribe()));
        }
        let (completed, _) = tokio::sync::watch::channel(false);
        state.loading.insert(key.clone(), completed);
        Ok(PathLoad::Lead(PathLoadGuard {
            cache: self.clone(),
            key: Some(key),
        }))
    }

    #[cfg(test)]
    pub(super) fn get(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        hash: [u8; 32],
        object_version: Option<u64>,
        maximum_bytes: usize,
    ) -> Result<Option<Bytes>, Status> {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let authority = Authority {
            tenant_id,
            bucket_id,
        };
        let Some(bytes) = state
            .entries
            .get(&authority)
            .and_then(|paths| paths.get(path))
            .and_then(|hashes| hashes.get(&(hash, object_version)))
        else {
            return Ok(None);
        };
        if bytes.len() > maximum_bytes {
            return Err(Status::data_loss(
                "v1 projection artifact violates its exact byte bound",
            ));
        }
        Ok(Some(bytes.clone()))
    }

    pub(super) fn insert(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        hash: [u8; 32],
        object_version: Option<u64>,
        bytes: Bytes,
    ) {
        if bytes.is_empty() {
            return;
        }
        let authority = Authority {
            tenant_id,
            bucket_id,
        };
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if bytes.len() > state.capacity_bytes
            || state
                .entries
                .get(&authority)
                .and_then(|paths| paths.get(path))
                .is_some_and(|hashes| hashes.contains_key(&(hash, object_version)))
        {
            return;
        }
        let path = Arc::<str>::from(path);
        state.bytes += bytes.len();
        state
            .entries
            .entry(authority)
            .or_default()
            .entry(path.clone())
            .or_default()
            .insert((hash, object_version), bytes);
        state.fifo.push_back(CacheKey::Path(Key {
            authority,
            path,
            hash,
            object_version,
        }));
        Self::evict_to_capacity(&mut state);
    }

    pub(super) fn get_blob(
        &self,
        blob: &BlobRef,
        maximum_bytes: usize,
    ) -> Result<Option<Bytes>, Status> {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(cached) = state.blobs.get(&BlobKey::from(blob)) else {
            return Ok(None);
        };
        let bytes = &cached.bytes;
        if bytes.len() > maximum_bytes || bytes.len() as u64 != blob.length {
            return Err(Status::data_loss(
                "v1 projection artifact violates its exact byte bound",
            ));
        }
        Ok(Some(bytes.clone()))
    }

    pub(super) fn insert_blob(&self, blob: &BlobRef, bytes: Bytes) {
        if bytes.is_empty() || bytes.len() as u64 != blob.length {
            return;
        }
        let key = BlobKey::from(blob);
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if bytes.len() > state.capacity_bytes || state.blobs.contains_key(&key) {
            return;
        }
        state.bytes += bytes.len();
        state.blobs.insert(key, CachedBlob { bytes });
        state.fifo.push_back(CacheKey::Blob(key));
        Self::evict_to_capacity(&mut state);
    }

    pub(super) fn get_query_run(
        &self,
        blob: &BlobRef,
        maximum_bytes: usize,
    ) -> Result<Option<Arc<ProjectionQueryRunDescriptor>>, Status> {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if blob.length > maximum_bytes as u64 {
            return Err(Status::data_loss(
                "v1 projection artifact violates its exact byte bound",
            ));
        }
        Ok(state
            .query_runs
            .get(&BlobKey::from(blob))
            .map(|cached| cached.descriptor.clone()))
    }

    pub(super) fn insert_query_run(
        &self,
        blob: &BlobRef,
        descriptor: Arc<ProjectionQueryRunDescriptor>,
    ) {
        let key = BlobKey::from(blob);
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.query_runs.contains_key(&key) {
            return;
        }
        let descriptor_bytes = resident_query_run_bytes(&descriptor);
        if descriptor_bytes > state.capacity_bytes {
            return;
        }
        state.query_runs.insert(
            key,
            CachedQueryRun {
                descriptor,
                resident_bytes: descriptor_bytes,
            },
        );
        state.bytes = state.bytes.saturating_add(descriptor_bytes);
        state.fifo.push_back(CacheKey::QueryRun(key));
        Self::evict_to_capacity(&mut state);
    }

    pub(super) fn get_query_block(
        &self,
        blob: &BlobRef,
        generation: [u8; 32],
        maximum_bytes: usize,
    ) -> Result<Option<Arc<DecodedQueryBlock>>, Status> {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if blob.length > maximum_bytes as u64 {
            return Err(Status::data_loss(
                "v1 projection artifact violates its exact byte bound",
            ));
        }
        let key = QueryBlockKey {
            generation,
            block: BlobKey::from(blob),
        };
        Ok(state
            .query_blocks
            .get(&key)
            .map(|cached| cached.block.clone()))
    }

    pub(super) fn insert_query_block(
        &self,
        blob: &BlobRef,
        generation: [u8; 32],
        block: Arc<DecodedQueryBlock>,
    ) {
        let key = QueryBlockKey {
            generation,
            block: BlobKey::from(blob),
        };
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Ok(encoded_bytes) = usize::try_from(blob.length) else {
            return;
        };
        if !block.matches_content(blob.hash, encoded_bytes) || state.query_blocks.contains_key(&key)
        {
            return;
        }
        // Packed children are right-sized before decode, so their retained
        // allocation is exactly the child length rather than the whole pack.
        let resident_bytes = block
            .resident_index_bytes()
            .saturating_add(block.encoded_bytes());
        if resident_bytes > state.capacity_bytes {
            return;
        }
        state.query_blocks.insert(
            key,
            CachedQueryBlock {
                block,
                resident_bytes,
            },
        );
        state.bytes = state.bytes.saturating_add(resident_bytes);
        state.fifo.push_back(CacheKey::QueryBlock(key));
        Self::evict_to_capacity(&mut state);
    }

    fn evict_to_capacity(state: &mut State) {
        while state.bytes > state.capacity_bytes {
            let oldest = state
                .fifo
                .pop_front()
                .expect("non-empty cache exceeds capacity");
            let evicted_bytes = match oldest {
                CacheKey::Path(oldest) => {
                    let mut evicted_bytes = 0;
                    let mut remove_authority = false;
                    if let Some(paths) = state.entries.get_mut(&oldest.authority) {
                        let mut remove_path = false;
                        if let Some(hashes) = paths.get_mut(oldest.path.as_ref()) {
                            if let Some(evicted) =
                                hashes.remove(&(oldest.hash, oldest.object_version))
                            {
                                evicted_bytes = evicted.len();
                            }
                            remove_path = hashes.is_empty();
                        }
                        if remove_path {
                            paths.remove(oldest.path.as_ref());
                        }
                        remove_authority = paths.is_empty();
                    }
                    if remove_authority {
                        state.entries.remove(&oldest.authority);
                    }
                    evicted_bytes
                }
                CacheKey::Blob(oldest) => state
                    .blobs
                    .remove(&oldest)
                    .map_or(0, |evicted| evicted.bytes.len()),
                CacheKey::QueryRun(oldest) => state
                    .query_runs
                    .remove(&oldest)
                    .map_or(0, |evicted| evicted.resident_bytes),
                CacheKey::QueryBlock(oldest) => state
                    .query_blocks
                    .remove(&oldest)
                    .map_or(0, |evicted| evicted.resident_bytes),
            };
            state.bytes -= evicted_bytes;
        }
    }

    #[cfg(test)]
    fn with_capacity_bytes(capacity_bytes: usize) -> Self {
        Self(Arc::new(Mutex::new(State {
            capacity_bytes,
            ..State::default()
        })))
    }
}

fn resident_query_run_bytes(descriptor: &ProjectionQueryRunDescriptor) -> usize {
    std::mem::size_of::<ProjectionQueryRunDescriptor>()
        .saturating_add(std::mem::size_of::<keldra_index::v1::ArtifactPackTable>())
        .saturating_add(
            descriptor
                .pack_table
                .entries()
                .len()
                .saturating_mul(std::mem::size_of::<keldra_index::v1::ArtifactPackReference>()),
        )
        .saturating_add(
            descriptor
                .pack_table
                .entries()
                .iter()
                .map(|pack| pack.canonical_path.len())
                .sum::<usize>(),
        )
        .saturating_add(
            descriptor
                .blocks
                .capacity()
                .saturating_mul(std::mem::size_of::<QueryBlockDescriptor>()),
        )
        .saturating_add(descriptor.blocks.iter().fold(0usize, |bytes, block| {
            bytes
                .saturating_add(block.minimum_key.capacity())
                .saturating_add(block.maximum_key.capacity())
        }))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::Notify;
    use tonic::Code;

    use super::*;

    #[test]
    fn cached_bytes_are_reference_counted() {
        let cache = ImmutableArtifactCache::default();
        let bytes = Bytes::from_static(b"artifact");
        cache.insert(1, 2, "/family/packs/hash", [3; 32], None, bytes.clone());

        let cached = cache
            .get(1, 2, "/family/packs/hash", [3; 32], None, bytes.len())
            .unwrap()
            .unwrap();

        assert_eq!(cached, bytes);
        assert_eq!(cached.as_ptr(), bytes.as_ptr());
    }

    #[tokio::test]
    async fn simultaneous_path_misses_perform_one_authoritative_load() {
        let cache = ImmutableArtifactCache::default();
        let load_count = Arc::new(AtomicUsize::new(0));
        let leader_entered = Arc::new(Notify::new());
        let release_leader = Arc::new(Notify::new());
        let entered = leader_entered.notified();

        let leader = {
            let cache = cache.clone();
            let load_count = load_count.clone();
            let leader_entered = leader_entered.clone();
            let release_leader = release_leader.clone();
            tokio::spawn(async move {
                cache
                    .get_or_load(
                        1,
                        2,
                        "/family/pages/hash",
                        [3; 32],
                        None,
                        8,
                        || async move {
                            load_count.fetch_add(1, Ordering::SeqCst);
                            leader_entered.notify_one();
                            release_leader.notified().await;
                            Ok(Some(Bytes::from(vec![1; 8])))
                        },
                    )
                    .await
            })
        };
        entered.await;
        let follower = {
            let cache = cache.clone();
            let load_count = load_count.clone();
            tokio::spawn(async move {
                cache
                    .get_or_load(
                        1,
                        2,
                        "/family/pages/hash",
                        [3; 32],
                        None,
                        8,
                        || async move {
                            load_count.fetch_add(1, Ordering::SeqCst);
                            Ok(Some(Bytes::from(vec![2; 8])))
                        },
                    )
                    .await
            })
        };

        tokio::task::yield_now().await;
        release_leader.notify_one();
        let leader_bytes = leader.await.unwrap().unwrap().unwrap();
        let follower_bytes = follower.await.unwrap().unwrap().unwrap();

        assert_eq!(load_count.load(Ordering::SeqCst), 1);
        assert_eq!(leader_bytes.as_ptr(), follower_bytes.as_ptr());
    }

    #[tokio::test]
    async fn failed_path_load_releases_a_waiter_to_retry() {
        let cache = ImmutableArtifactCache::default();
        let load_count = Arc::new(AtomicUsize::new(0));
        let leader_entered = Arc::new(Notify::new());
        let release_leader = Arc::new(Notify::new());
        let entered = leader_entered.notified();

        let leader = {
            let cache = cache.clone();
            let load_count = load_count.clone();
            let leader_entered = leader_entered.clone();
            let release_leader = release_leader.clone();
            tokio::spawn(async move {
                cache
                    .get_or_load(
                        1,
                        2,
                        "/family/pages/hash",
                        [3; 32],
                        None,
                        8,
                        || async move {
                            load_count.fetch_add(1, Ordering::SeqCst);
                            leader_entered.notify_one();
                            release_leader.notified().await;
                            Err(Status::unavailable("authoritative read failed"))
                        },
                    )
                    .await
            })
        };
        entered.await;
        let follower = {
            let cache = cache.clone();
            let load_count = load_count.clone();
            tokio::spawn(async move {
                cache
                    .get_or_load(
                        1,
                        2,
                        "/family/pages/hash",
                        [3; 32],
                        None,
                        8,
                        || async move {
                            load_count.fetch_add(1, Ordering::SeqCst);
                            Ok(Some(Bytes::from(vec![4; 8])))
                        },
                    )
                    .await
            })
        };

        tokio::task::yield_now().await;
        release_leader.notify_one();

        assert_eq!(leader.await.unwrap().unwrap_err().code(), Code::Unavailable);
        assert_eq!(follower.await.unwrap().unwrap().unwrap().as_ref(), &[4; 8]);
        assert_eq!(load_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn concurrent_decoded_block_population_has_one_leader() {
        let cache = ImmutableArtifactCache::default();
        let blob = BlobRef {
            hash: [11; 32],
            length: 128,
        };
        let leader = match cache.coordinate_query_block(&blob, [12; 32]).await.unwrap() {
            DecodePopulation::Lead(guard) => guard,
            DecodePopulation::Completed => panic!("first population must lead"),
        };
        let waiter = {
            let cache = cache.clone();
            let blob = blob.clone();
            tokio::spawn(async move { cache.coordinate_query_block(&blob, [12; 32]).await })
        };
        tokio::task::yield_now().await;
        drop(leader);
        assert!(matches!(
            waiter.await.unwrap().unwrap(),
            DecodePopulation::Completed
        ));
        assert!(matches!(
            cache.coordinate_query_block(&blob, [12; 32]).await.unwrap(),
            DecodePopulation::Lead(_)
        ));
    }

    #[tokio::test]
    async fn concurrent_decoded_run_population_has_one_leader() {
        let cache = ImmutableArtifactCache::default();
        let blob = BlobRef {
            hash: [13; 32],
            length: 256,
        };
        let leader = match cache.coordinate_query_run(&blob).await.unwrap() {
            DecodePopulation::Lead(guard) => guard,
            DecodePopulation::Completed => panic!("first population must lead"),
        };
        let waiter = {
            let cache = cache.clone();
            let blob = blob.clone();
            tokio::spawn(async move { cache.coordinate_query_run(&blob).await })
        };
        tokio::task::yield_now().await;
        drop(leader);
        assert!(matches!(
            waiter.await.unwrap().unwrap(),
            DecodePopulation::Completed
        ));
    }

    #[tokio::test]
    async fn loaded_path_bytes_must_fit_the_callers_exact_bound() {
        let cache = ImmutableArtifactCache::default();

        let error = cache
            .get_or_load(1, 2, "/family/pages/hash", [3; 32], None, 7, || async {
                Ok(Some(Bytes::from_static(b"artifact")))
            })
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::DataLoss);
        assert!(
            cache
                .get(1, 2, "/family/pages/hash", [3; 32], None, 8)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn cache_entries_are_isolated_by_path_tenant_and_bucket() {
        let cache = ImmutableArtifactCache::default();
        let path = "/family/packs/hash";
        let hash = [4; 32];
        cache.insert(1, 2, path, hash, None, Bytes::from_static(b"artifact"));

        assert!(cache.get(1, 2, path, hash, None, 8).unwrap().is_some());
        assert!(
            cache
                .get(1, 2, "/other-family/packs/hash", hash, None, 8)
                .unwrap()
                .is_none()
        );
        assert!(cache.get(9, 2, path, hash, None, 8).unwrap().is_none());
        assert!(cache.get(1, 9, path, hash, None, 8).unwrap().is_none());
    }

    #[test]
    fn exact_object_versions_do_not_share_path_cache_entries() {
        let cache = ImmutableArtifactCache::default();
        let path = "/family/packs/hash";
        let hash = [5; 32];
        cache.insert(1, 2, path, hash, Some(7), Bytes::from_static(b"artifact"));

        assert!(cache.get(1, 2, path, hash, Some(7), 8).unwrap().is_some());
        assert!(cache.get(1, 2, path, hash, Some(8), 8).unwrap().is_none());
        assert!(cache.get(1, 2, path, hash, None, 8).unwrap().is_none());
    }

    #[test]
    fn cache_hits_enforce_the_callers_byte_bound() {
        let cache = ImmutableArtifactCache::default();
        let path = "/family/packs/hash";
        let hash = [5; 32];
        cache.insert(1, 2, path, hash, None, Bytes::from_static(b"artifact"));

        let error = cache.get(1, 2, path, hash, None, 7).unwrap_err();

        assert_eq!(error.code(), Code::DataLoss);
    }

    #[test]
    fn insertion_evicts_oldest_entries_to_the_byte_capacity() {
        let cache = ImmutableArtifactCache::with_capacity_bytes(5);
        cache.insert(1, 2, "/first", [1; 32], None, Bytes::from_static(b"123"));
        cache.insert(1, 2, "/second", [2; 32], None, Bytes::from_static(b"456"));

        assert!(
            cache
                .get(1, 2, "/first", [1; 32], None, 3)
                .unwrap()
                .is_none()
        );
        assert!(
            cache
                .get(1, 2, "/second", [2; 32], None, 3)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn blob_entries_are_reference_counted_and_length_is_part_of_the_identity() {
        let cache = ImmutableArtifactCache::default();
        let blob = BlobRef {
            hash: [6; 32],
            length: 8,
        };
        let bytes = Bytes::from_static(b"artifact");
        cache.insert_blob(&blob, bytes.clone());

        let cached = cache.get_blob(&blob, 8).unwrap().unwrap();
        assert_eq!(cached.as_ptr(), bytes.as_ptr());
        assert!(
            cache
                .get_blob(
                    &BlobRef {
                        hash: blob.hash,
                        length: 7,
                    },
                    8,
                )
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn blob_cache_hits_enforce_the_callers_byte_bound() {
        let cache = ImmutableArtifactCache::default();
        let blob = BlobRef {
            hash: [7; 32],
            length: 8,
        };
        cache.insert_blob(&blob, Bytes::from_static(b"artifact"));

        let error = cache.get_blob(&blob, 7).unwrap_err();

        assert_eq!(error.code(), Code::DataLoss);
    }

    #[test]
    fn blob_insert_rejects_bytes_that_disagree_with_the_identity() {
        let cache = ImmutableArtifactCache::default();
        let blob = BlobRef {
            hash: [8; 32],
            length: 9,
        };
        cache.insert_blob(&blob, Bytes::from_static(b"artifact"));

        assert!(cache.get_blob(&blob, 9).unwrap().is_none());
    }

    #[test]
    fn blob_and_path_entries_share_one_capacity_bound() {
        let cache = ImmutableArtifactCache::with_capacity_bytes(5);
        cache.insert(1, 2, "/first", [1; 32], None, Bytes::from_static(b"123"));
        let blob = BlobRef {
            hash: [2; 32],
            length: 3,
        };
        cache.insert_blob(&blob, Bytes::from_static(b"456"));

        assert!(
            cache
                .get(1, 2, "/first", [1; 32], None, 3)
                .unwrap()
                .is_none()
        );
        assert!(cache.get_blob(&blob, 3).unwrap().is_some());
    }

    #[test]
    fn decoded_query_runs_are_keyed_by_their_own_identity_not_raw_blob_residency() {
        let cache = ImmutableArtifactCache::default();
        let blob = BlobRef {
            hash: [9; 32],
            length: 8,
        };
        let descriptor = Arc::new(ProjectionQueryRunDescriptor {
            partition: keldra_index::v1::ProjectionPartitionIdentity::new(
                [1; 32], 2, [3; 32], 4, 5, 6,
            )
            .unwrap(),
            physical_catalog_generation: [7; 32],
            sequence: 1,
            source_start_offset: 1,
            next_offset: 2,
            through_atomic_position: 1,
            pack_table: Arc::new(keldra_index::v1::ArtifactPackTable::empty()),
            blocks: Vec::new(),
        });

        cache.insert_query_run(&blob, descriptor.clone());
        let cached = cache.get_query_run(&blob, 8).unwrap().unwrap();

        assert!(Arc::ptr_eq(&cached, &descriptor));
    }
}
