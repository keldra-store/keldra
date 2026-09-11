use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use keldra_store::BlobRef;
use tonic::Status;

const CAPACITY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct Authority {
    tenant_id: u64,
    bucket_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Key {
    authority: Authority,
    path: Arc<str>,
    hash: [u8; 32],
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
}

struct State {
    capacity_bytes: usize,
    bytes: usize,
    // Nest by authority and path so hits can compare the caller's borrowed
    // path without allocating another owned copy.
    entries: HashMap<Authority, HashMap<Arc<str>, HashMap<[u8; 32], Bytes>>>,
    blobs: HashMap<BlobKey, Bytes>,
    fifo: VecDeque<CacheKey>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            capacity_bytes: CAPACITY_BYTES,
            bytes: 0,
            entries: HashMap::new(),
            blobs: HashMap::new(),
            fifo: VecDeque::new(),
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct ImmutableArtifactCache(Arc<Mutex<State>>);

impl ImmutableArtifactCache {
    pub(super) fn get(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        hash: [u8; 32],
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
            .and_then(|hashes| hashes.get(&hash))
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
                .is_some_and(|hashes| hashes.contains_key(&hash))
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
            .insert(hash, bytes);
        state.fifo.push_back(CacheKey::Path(Key {
            authority,
            path,
            hash,
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
        let Some(bytes) = state.blobs.get(&BlobKey::from(blob)) else {
            return Ok(None);
        };
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
        state.blobs.insert(key, bytes);
        state.fifo.push_back(CacheKey::Blob(key));
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
                            if let Some(evicted) = hashes.remove(&oldest.hash) {
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
                    .map_or(0, |evicted| evicted.len()),
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

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;

    #[test]
    fn cached_bytes_are_reference_counted() {
        let cache = ImmutableArtifactCache::default();
        let bytes = Bytes::from_static(b"artifact");
        cache.insert(1, 2, "/family/packs/hash", [3; 32], bytes.clone());

        let cached = cache
            .get(1, 2, "/family/packs/hash", [3; 32], bytes.len())
            .unwrap()
            .unwrap();

        assert_eq!(cached, bytes);
        assert_eq!(cached.as_ptr(), bytes.as_ptr());
    }

    #[test]
    fn cache_entries_are_isolated_by_path_tenant_and_bucket() {
        let cache = ImmutableArtifactCache::default();
        let path = "/family/packs/hash";
        let hash = [4; 32];
        cache.insert(1, 2, path, hash, Bytes::from_static(b"artifact"));

        assert!(cache.get(1, 2, path, hash, 8).unwrap().is_some());
        assert!(
            cache
                .get(1, 2, "/other-family/packs/hash", hash, 8)
                .unwrap()
                .is_none()
        );
        assert!(cache.get(9, 2, path, hash, 8).unwrap().is_none());
        assert!(cache.get(1, 9, path, hash, 8).unwrap().is_none());
    }

    #[test]
    fn cache_hits_enforce_the_callers_byte_bound() {
        let cache = ImmutableArtifactCache::default();
        let path = "/family/packs/hash";
        let hash = [5; 32];
        cache.insert(1, 2, path, hash, Bytes::from_static(b"artifact"));

        let error = cache.get(1, 2, path, hash, 7).unwrap_err();

        assert_eq!(error.code(), Code::DataLoss);
    }

    #[test]
    fn insertion_evicts_oldest_entries_to_the_byte_capacity() {
        let cache = ImmutableArtifactCache::with_capacity_bytes(5);
        cache.insert(1, 2, "/first", [1; 32], Bytes::from_static(b"123"));
        cache.insert(1, 2, "/second", [2; 32], Bytes::from_static(b"456"));

        assert!(cache.get(1, 2, "/first", [1; 32], 3).unwrap().is_none());
        assert!(cache.get(1, 2, "/second", [2; 32], 3).unwrap().is_some());
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
        cache.insert(1, 2, "/first", [1; 32], Bytes::from_static(b"123"));
        let blob = BlobRef {
            hash: [2; 32],
            length: 3,
        };
        cache.insert_blob(&blob, Bytes::from_static(b"456"));

        assert!(cache.get(1, 2, "/first", [1; 32], 3).unwrap().is_none());
        assert!(cache.get_blob(&blob, 3).unwrap().is_some());
    }
}
