use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
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

struct State {
    capacity_bytes: usize,
    bytes: usize,
    // Nest by authority and path so hits can compare the caller's borrowed
    // path without allocating another owned copy.
    entries: HashMap<Authority, HashMap<Arc<str>, HashMap<[u8; 32], Bytes>>>,
    fifo: VecDeque<Key>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            capacity_bytes: CAPACITY_BYTES,
            bytes: 0,
            entries: HashMap::new(),
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
        state.fifo.push_back(Key {
            authority,
            path,
            hash,
        });
        while state.bytes > state.capacity_bytes {
            let oldest = state
                .fifo
                .pop_front()
                .expect("non-empty cache exceeds capacity");
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
            state.bytes -= evicted_bytes;
            if remove_authority {
                state.entries.remove(&oldest.authority);
            }
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
        assert!(cache
            .get(1, 2, "/other-family/packs/hash", hash, 8)
            .unwrap()
            .is_none());
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
}
