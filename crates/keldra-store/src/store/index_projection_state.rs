//! Disposable producer-local projection state keyed by exact source path.
//!
//! Immutable format-v1 generations remain authoritative. The generation marker
//! makes every local-cache read fail closed when this node's materialization is
//! not aligned with the caller's loaded Current.

use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions};

use super::{CF_METADATA, Store};
use crate::MutationError;
use crate::key::STORAGE_KEY_FORMAT_VERSION;

const MARKER_DOMAIN: u8 = b'M';
const STATE_DOMAIN: u8 = b'S';
const GARBAGE_DOMAIN: u8 = b'G';
const KEY_PREFIX: [u8; 3] = [STORAGE_KEY_FORMAT_VERSION, b'I', b'P'];
const MARKER_FORMAT: u8 = 1;
const MARKER_BYTES: usize = 1 + 32 + 32;
const MAX_PARTITION_IDENTITY_BYTES: usize = 256;
const MAX_RECORD_KEY_BYTES: usize = 256;
const MAX_CACHE_VALUE_BYTES: usize = 64 * 1024 * 1024;
const CLEANUP_KEYS_PER_WRITE: usize = 128;

#[derive(Clone, Copy)]
struct Marker {
    current_generation: [u8; 32],
    cache_epoch: [u8; 32],
}

impl Store {
    /// Returns one exact keyed state only when the partition marker names the
    /// supplied authoritative Current generation. A mismatch rotates to an
    /// empty epoch in O(1); old epochs are reclaimed in bounded later steps.
    #[doc(hidden)]
    pub fn index_projection_state(
        &self,
        partition: &[u8],
        current_generation: [u8; 32],
        record_key: &[u8],
    ) -> Result<Option<Option<Vec<u8>>>, MutationError> {
        validate_identity(partition, current_generation, record_key)?;
        let _guard = self
            .index_projection_state_lock
            .lock()
            .map_err(|_| storage("index projection state lock is poisoned"))?;
        let marker = self.read_or_rotate_marker(partition, current_generation)?;
        self.db
            .get_cf(
                self.cf(CF_METADATA)?,
                state_key(partition, marker.cache_epoch, record_key)?,
            )
            .map_err(storage_error)
            .and_then(|value| value.map(|value| decode_cache_value(&value)).transpose())
    }

    /// Populates one authoritative fallback result if the cache epoch still
    /// names the same Current generation that was scanned.
    #[doc(hidden)]
    pub fn cache_index_projection_state(
        &self,
        partition: &[u8],
        current_generation: [u8; 32],
        record_key: &[u8],
        state: Option<&[u8]>,
    ) -> Result<bool, MutationError> {
        validate_identity(partition, current_generation, record_key)?;
        if let Some(state) = state {
            validate_value(state)?;
        }
        let _guard = self
            .index_projection_state_lock
            .lock()
            .map_err(|_| storage("index projection state lock is poisoned"))?;
        let Some(marker) = self.read_marker(partition)? else {
            return Ok(false);
        };
        if marker.current_generation != current_generation {
            return Ok(false);
        }
        let mut options = WriteOptions::default();
        options.set_sync(false);
        self.db
            .put_cf_opt(
                self.cf(CF_METADATA)?,
                state_key(partition, marker.cache_epoch, record_key)?,
                encode_cache_value(state),
                &options,
            )
            .map_err(storage_error)?;
        Ok(true)
    }

    /// Advances the disposable cache after the authoritative Current CAS. If
    /// the predecessor marker is unavailable, the new updates seed a fresh
    /// partial epoch; an absent key continues to fall back to immutable state.
    #[doc(hidden)]
    pub fn advance_index_projection_state(
        &self,
        partition: &[u8],
        predecessor_generation: Option<[u8; 32]>,
        current_generation: [u8; 32],
        updates: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<(), MutationError> {
        validate_partition(partition)?;
        validate_generation(current_generation)?;
        for (record_key, value) in updates {
            validate_record_key(record_key)?;
            if let Some(value) = value {
                validate_value(value)?;
            }
        }
        let _guard = self
            .index_projection_state_lock
            .lock()
            .map_err(|_| storage("index projection state lock is poisoned"))?;
        let previous = self.read_marker(partition)?;
        let preserve = previous
            .is_some_and(|marker| predecessor_generation == Some(marker.current_generation));
        let epoch = if preserve {
            previous.expect("preserved marker exists").cache_epoch
        } else {
            current_generation
        };
        let mut batch = WriteBatch::default();
        if !preserve && let Some(previous) = previous {
            batch.put_cf(
                self.cf(CF_METADATA)?,
                garbage_key(partition, previous.cache_epoch),
                [],
            );
        }
        for (record_key, value) in updates {
            batch.put_cf(
                self.cf(CF_METADATA)?,
                state_key(partition, epoch, record_key)?,
                encode_cache_value(value.as_deref()),
            );
        }
        batch.put_cf(
            self.cf(CF_METADATA)?,
            marker_key(partition),
            encode_marker(Marker {
                current_generation,
                cache_epoch: epoch,
            }),
        );
        self.stage_bounded_garbage_cleanup(&mut batch, partition, epoch)?;
        self.write_cache_batch(batch)
    }

    fn read_or_rotate_marker(
        &self,
        partition: &[u8],
        current_generation: [u8; 32],
    ) -> Result<Marker, MutationError> {
        let previous = self.read_marker(partition)?;
        if let Some(marker) = previous
            && marker.current_generation == current_generation
        {
            return Ok(marker);
        }
        let marker = Marker {
            current_generation,
            cache_epoch: current_generation,
        };
        let mut batch = WriteBatch::default();
        if let Some(previous) = previous {
            batch.put_cf(
                self.cf(CF_METADATA)?,
                garbage_key(partition, previous.cache_epoch),
                [],
            );
        }
        batch.put_cf(
            self.cf(CF_METADATA)?,
            marker_key(partition),
            encode_marker(marker),
        );
        self.stage_bounded_garbage_cleanup(&mut batch, partition, marker.cache_epoch)?;
        self.write_cache_batch(batch)?;
        Ok(marker)
    }

    fn read_marker(&self, partition: &[u8]) -> Result<Option<Marker>, MutationError> {
        self.db
            .get_cf(self.cf(CF_METADATA)?, marker_key(partition))
            .map_err(storage_error)?
            .map(|value| decode_marker(&value))
            .transpose()
    }

    fn stage_bounded_garbage_cleanup(
        &self,
        batch: &mut WriteBatch,
        partition: &[u8],
        active_epoch: [u8; 32],
    ) -> Result<(), MutationError> {
        let metadata = self.cf(CF_METADATA)?;
        let garbage_prefix = domain_prefix(partition, GARBAGE_DOMAIN);
        let Some(item) = self
            .db
            .iterator_cf(
                metadata,
                IteratorMode::From(&garbage_prefix, Direction::Forward),
            )
            .next()
        else {
            return Ok(());
        };
        let (garbage_key_bytes, _) = item.map_err(storage_error)?;
        if !garbage_key_bytes.starts_with(&garbage_prefix) {
            return Ok(());
        }
        let retired_epoch = decode_garbage_epoch(&garbage_key_bytes, &garbage_prefix)?;
        if retired_epoch == active_epoch {
            batch.delete_cf(metadata, garbage_key_bytes);
            return Ok(());
        }
        let retired_prefix = state_epoch_prefix(partition, retired_epoch);
        let mut deleted = 0usize;
        let mut exhausted = true;
        for item in self.db.iterator_cf(
            metadata,
            IteratorMode::From(&retired_prefix, Direction::Forward),
        ) {
            let (key, _) = item.map_err(storage_error)?;
            if !key.starts_with(&retired_prefix) {
                break;
            }
            if deleted == CLEANUP_KEYS_PER_WRITE {
                exhausted = false;
                break;
            }
            batch.delete_cf(metadata, key);
            deleted += 1;
        }
        if exhausted {
            batch.delete_cf(metadata, garbage_key_bytes);
        }
        Ok(())
    }

    fn write_cache_batch(&self, batch: WriteBatch) -> Result<(), MutationError> {
        let mut options = WriteOptions::default();
        // This state is disposable. Retain the WAL's atomic batch recovery but
        // never force an additional fsync onto authoritative Current progress.
        options.set_sync(false);
        self.db.write_opt(batch, &options).map_err(storage_error)
    }
}

fn validate_identity(
    partition: &[u8],
    generation: [u8; 32],
    record_key: &[u8],
) -> Result<(), MutationError> {
    validate_partition(partition)?;
    validate_generation(generation)?;
    validate_record_key(record_key)
}

fn validate_partition(partition: &[u8]) -> Result<(), MutationError> {
    if partition.is_empty() || partition.len() > MAX_PARTITION_IDENTITY_BYTES {
        Err(storage("index projection partition identity is invalid"))
    } else {
        Ok(())
    }
}

fn validate_generation(generation: [u8; 32]) -> Result<(), MutationError> {
    if generation == [0; 32] {
        Err(storage("index projection generation is invalid"))
    } else {
        Ok(())
    }
}

fn validate_record_key(key: &[u8]) -> Result<(), MutationError> {
    if key.is_empty() || key.len() > MAX_RECORD_KEY_BYTES {
        Err(storage("index projection record key is invalid"))
    } else {
        Ok(())
    }
}

fn validate_value(value: &[u8]) -> Result<(), MutationError> {
    if value.is_empty() || value.len() > MAX_CACHE_VALUE_BYTES {
        Err(storage("index projection cached state is invalid"))
    } else {
        Ok(())
    }
}

fn partition_prefix(partition: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(KEY_PREFIX.len() + 2 + partition.len());
    key.extend_from_slice(&KEY_PREFIX);
    key.extend_from_slice(&(partition.len() as u16).to_be_bytes());
    key.extend_from_slice(partition);
    key
}

fn domain_prefix(partition: &[u8], domain: u8) -> Vec<u8> {
    let mut key = partition_prefix(partition);
    key.push(domain);
    key
}

fn marker_key(partition: &[u8]) -> Vec<u8> {
    domain_prefix(partition, MARKER_DOMAIN)
}

fn state_epoch_prefix(partition: &[u8], epoch: [u8; 32]) -> Vec<u8> {
    let mut key = domain_prefix(partition, STATE_DOMAIN);
    key.extend_from_slice(&epoch);
    key
}

fn state_key(
    partition: &[u8],
    epoch: [u8; 32],
    record_key: &[u8],
) -> Result<Vec<u8>, MutationError> {
    validate_record_key(record_key)?;
    let mut key = state_epoch_prefix(partition, epoch);
    key.extend_from_slice(&(record_key.len() as u16).to_be_bytes());
    key.extend_from_slice(record_key);
    Ok(key)
}

fn encode_cache_value(value: Option<&[u8]>) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(1 + value.map_or(0, <[u8]>::len));
    encoded.push(u8::from(value.is_some()));
    if let Some(value) = value {
        encoded.extend_from_slice(value);
    }
    encoded
}

fn decode_cache_value(encoded: &[u8]) -> Result<Option<Vec<u8>>, MutationError> {
    match encoded.split_first() {
        Some((0, [])) => Ok(None),
        Some((1, value)) if !value.is_empty() && value.len() <= MAX_CACHE_VALUE_BYTES => {
            Ok(Some(value.to_vec()))
        }
        _ => Err(storage("index projection cached value is malformed")),
    }
}

fn garbage_key(partition: &[u8], epoch: [u8; 32]) -> Vec<u8> {
    let mut key = domain_prefix(partition, GARBAGE_DOMAIN);
    key.extend_from_slice(&epoch);
    key
}

fn decode_garbage_epoch(key: &[u8], prefix: &[u8]) -> Result<[u8; 32], MutationError> {
    key.strip_prefix(prefix)
        .and_then(|value| value.try_into().ok())
        .ok_or_else(|| storage("index projection garbage key is malformed"))
}

fn encode_marker(marker: Marker) -> [u8; MARKER_BYTES] {
    let mut encoded = [0u8; MARKER_BYTES];
    encoded[0] = MARKER_FORMAT;
    encoded[1..33].copy_from_slice(&marker.current_generation);
    encoded[33..].copy_from_slice(&marker.cache_epoch);
    encoded
}

fn decode_marker(encoded: &[u8]) -> Result<Marker, MutationError> {
    if encoded.len() != MARKER_BYTES || encoded[0] != MARKER_FORMAT {
        return Err(storage("index projection cache marker is malformed"));
    }
    let current_generation = encoded[1..33]
        .try_into()
        .expect("validated marker generation width");
    let cache_epoch = encoded[33..]
        .try_into()
        .expect("validated marker epoch width");
    validate_generation(current_generation)?;
    validate_generation(cache_epoch)?;
    Ok(Marker {
        current_generation,
        cache_epoch,
    })
}

fn storage(message: impl Into<String>) -> MutationError {
    MutationError::Storage(message.into())
}

fn storage_error(error: rocksdb::Error) -> MutationError {
    storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::StoreOptions;

    async fn store() -> (TempDir, Store) {
        let dir = TempDir::new().unwrap();
        let store = Store::open(StoreOptions::new(dir.path(), 1)).await.unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn exact_generation_marker_gates_keyed_state() {
        let (_dir, store) = store().await;
        let partition = b"partition-a";
        assert_eq!(
            store
                .index_projection_state(partition, [1; 32], b"a")
                .unwrap(),
            None
        );
        assert!(
            store
                .cache_index_projection_state(partition, [1; 32], b"a", Some(b"one"))
                .unwrap()
        );
        assert!(
            store
                .cache_index_projection_state(partition, [1; 32], b"deleted", None)
                .unwrap()
        );
        assert_eq!(
            store
                .index_projection_state(partition, [1; 32], b"a")
                .unwrap(),
            Some(Some(b"one".to_vec()))
        );
        assert_eq!(
            store
                .index_projection_state(partition, [1; 32], b"deleted")
                .unwrap(),
            Some(None)
        );
        assert_eq!(
            store
                .index_projection_state(partition, [2; 32], b"a")
                .unwrap(),
            None
        );
        assert!(
            !store
                .cache_index_projection_state(partition, [1; 32], b"a", Some(b"stale"))
                .unwrap()
        );
    }

    #[tokio::test]
    async fn advancing_from_exact_predecessor_preserves_untouched_keys() {
        let (_dir, store) = store().await;
        let partition = b"partition-a";
        store
            .index_projection_state(partition, [1; 32], b"a")
            .unwrap();
        store
            .cache_index_projection_state(partition, [1; 32], b"a", Some(b"one"))
            .unwrap();
        store
            .advance_index_projection_state(
                partition,
                Some([1; 32]),
                [2; 32],
                &[(b"b".to_vec(), Some(b"two".to_vec()))],
            )
            .unwrap();
        assert_eq!(
            store
                .index_projection_state(partition, [2; 32], b"a")
                .unwrap(),
            Some(Some(b"one".to_vec()))
        );
        assert_eq!(
            store
                .index_projection_state(partition, [2; 32], b"b")
                .unwrap(),
            Some(Some(b"two".to_vec()))
        );
    }

    #[tokio::test]
    async fn advancing_without_exact_predecessor_starts_safe_partial_epoch() {
        let (_dir, store) = store().await;
        let partition = b"partition-a";
        store
            .index_projection_state(partition, [1; 32], b"a")
            .unwrap();
        store
            .cache_index_projection_state(partition, [1; 32], b"a", Some(b"old"))
            .unwrap();
        store
            .advance_index_projection_state(
                partition,
                Some([9; 32]),
                [2; 32],
                &[(b"b".to_vec(), Some(b"new".to_vec()))],
            )
            .unwrap();
        assert_eq!(
            store
                .index_projection_state(partition, [2; 32], b"a")
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .index_projection_state(partition, [2; 32], b"b")
                .unwrap(),
            Some(Some(b"new".to_vec()))
        );
    }
}
