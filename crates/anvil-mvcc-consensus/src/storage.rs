use std::{
    path::Path,
    sync::{Arc, Mutex},
};

#[cfg(test)]
use std::{
    cell::Cell,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(test)]
pub(crate) fn fail_sync_write_at(ordinal: u64) {
    SYNC_WRITE_FAILURE.with(|failure| failure.set(Some((ordinal, 0))));
}

#[cfg(test)]
thread_local! {
    // Failure injection is thread-local so concurrently executing storage tests
    // cannot consume each other's write ordinal.
    static SYNC_WRITE_FAILURE: Cell<Option<(u64, u64)>> = const { Cell::new(None) };
}

use bincode::{
    config,
    serde::{decode_from_slice, encode_to_vec},
};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DB, Options, WriteBatch, WriteOptions};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{CertificationState, CommitVersion};

pub const CF_RAFT_VOTE: &str = "cf_raft_vote";
pub const CF_RAFT_LOG: &str = "cf_raft_log";
pub const CF_RAFT_META: &str = "cf_raft_meta";
pub const CF_CONSENSUS_STATE: &str = "cf_consensus_state";

pub const CONSENSUS_COLUMN_FAMILIES: [&str; 4] =
    [CF_RAFT_VOTE, CF_RAFT_LOG, CF_RAFT_META, CF_CONSENSUS_STATE];

const KEY_VOTE: &[u8] = b"vote";
const KEY_LAST_LOG_INDEX: &[u8] = b"last-log-index";
const KEY_LAST_PURGED_INDEX: &[u8] = b"last-purged-index";
const KEY_CERTIFICATION_STATE: &[u8] = b"certification-state";
pub(crate) const KEY_OPENRAFT_STATE: &[u8] = b"openraft-state-v1";
pub(crate) const KEY_LAST_PURGED_LOG_ID: &[u8] = b"last-purged-log-id";

/// Certification state and its Raft application position.
///
/// These are written together in one RocksDB batch so a restart cannot observe
/// new conflict state paired with an old applied position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedConsensusState {
    pub state: CertificationState,
    pub last_applied: CommitVersion,
}

#[derive(Debug, Error)]
pub enum RaftStorageError {
    #[error("RocksDB error: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("consensus codec error: {0}")]
    Codec(String),
    #[error("missing required column family {0}")]
    MissingColumnFamily(&'static str),
    #[error("Raft log append would create a hole: expected {expected}, received {received}")]
    LogHole { expected: u64, received: u64 },
    #[error("Raft log entries must be consecutive: previous {previous}, next {next}")]
    NonConsecutive { previous: u64, next: u64 },
    #[error("Raft log truncation at {from} crosses purged boundary {purged}")]
    TruncatePurged { from: u64, purged: u64 },
    #[error("Raft log purge would move backward: current {current}, requested {requested}")]
    PurgeRegression { current: u64, requested: u64 },
    #[error("Raft storage writer lock was poisoned")]
    WriterPoisoned,
}

/// RocksDB primitives used by the OpenRaft adapter.
///
/// The enclosing adapter serializes calls to this type through one executor.
/// Every mutating operation is synchronous and returns only after RocksDB's WAL
/// has been synced, which is the boundary at which OpenRaft's flush callback may
/// be completed.
#[derive(Clone)]
pub struct RocksRaftStore {
    db: Arc<DB>,
    group_id: u64,
    writer: Arc<Mutex<()>>,
    log_write_fault_hook: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    #[cfg(test)]
    active_sync_writes: Arc<AtomicU64>,
    #[cfg(test)]
    max_active_sync_writes: Arc<AtomicU64>,
}

impl RocksRaftStore {
    /// Open a standalone RocksDB suitable for tests or a consensus-only node.
    ///
    /// Production uses [`Self::from_db`] with the existing CoreMeta database.
    pub fn open(path: impl AsRef<Path>, group_id: u64) -> Result<Self, RaftStorageError> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let descriptors = CONSENSUS_COLUMN_FAMILIES.map(|name| {
            let mut cf_options = Options::default();
            cf_options.set_compression_type(rocksdb::DBCompressionType::Lz4);
            ColumnFamilyDescriptor::new(name, cf_options)
        });
        let db = DB::open_cf_descriptors(&options, path, descriptors)?;
        Self::from_db(Arc::new(db), group_id)
    }

    /// Attach to the same RocksDB instance used by CoreMeta.
    pub fn from_db(db: Arc<DB>, group_id: u64) -> Result<Self, RaftStorageError> {
        for name in CONSENSUS_COLUMN_FAMILIES {
            if db.cf_handle(name).is_none() {
                return Err(RaftStorageError::MissingColumnFamily(name));
            }
        }
        Ok(Self {
            db,
            group_id,
            writer: Arc::new(Mutex::new(())),
            log_write_fault_hook: None,
            #[cfg(test)]
            active_sync_writes: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            max_active_sync_writes: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn database(&self) -> &Arc<DB> {
        &self.db
    }

    /// Installs the caller's deterministic fault catalog at the exact durable
    /// Raft-log append boundary. Production construction leaves this unset.
    #[doc(hidden)]
    pub fn with_log_write_fault_hook(
        mut self,
        hook: Arc<dyn Fn() -> Result<(), String> + Send + Sync>,
    ) -> Self {
        self.log_write_fault_hook = Some(hook);
        self
    }

    pub fn save_vote<V: Serialize>(&self, vote: &V) -> Result<(), RaftStorageError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| RaftStorageError::WriterPoisoned)?;
        let bytes = encode(vote)?;
        let cf = self.cf(CF_RAFT_VOTE)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, self.scoped_key(KEY_VOTE), bytes);
        self.sync_write(batch)
    }

    pub fn read_vote<V: DeserializeOwned>(&self) -> Result<Option<V>, RaftStorageError> {
        let cf = self.cf(CF_RAFT_VOTE)?;
        self.db
            .get_cf(cf, self.scoped_key(KEY_VOTE))?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    /// Append already encoded OpenRaft entries.
    ///
    /// Keeping encoding at the adapter edge allows this storage primitive to
    /// remain stable across contained OpenRaft API changes.
    pub fn append_logs(&self, entries: &[(u64, Vec<u8>)]) -> Result<(), RaftStorageError> {
        if entries.is_empty() {
            return Ok(());
        }
        if let Some(hook) = &self.log_write_fault_hook {
            hook().map_err(RaftStorageError::Codec)?;
        }
        let _writer = self
            .writer
            .lock()
            .map_err(|_| RaftStorageError::WriterPoisoned)?;
        for pair in entries.windows(2) {
            if pair[1].0 != pair[0].0 + 1 {
                return Err(RaftStorageError::NonConsecutive {
                    previous: pair[0].0,
                    next: pair[1].0,
                });
            }
        }
        let expected = self.last_log_index()?.map_or_else(
            || self.last_purged_index().map(|v| v.map_or(0, |i| i + 1)),
            |v| Ok(v + 1),
        )?;
        if entries[0].0 != expected {
            return Err(RaftStorageError::LogHole {
                expected,
                received: entries[0].0,
            });
        }

        let cf_log = self.cf(CF_RAFT_LOG)?;
        let cf_meta = self.cf(CF_RAFT_META)?;
        let mut batch = WriteBatch::default();
        for (index, entry) in entries {
            batch.put_cf(cf_log, self.log_key(*index), entry);
        }
        batch.put_cf(
            cf_meta,
            self.scoped_key(KEY_LAST_LOG_INDEX),
            entries.last().expect("non-empty").0.to_be_bytes(),
        );
        self.sync_write(batch)
    }

    pub fn get_log(&self, index: u64) -> Result<Option<Vec<u8>>, RaftStorageError> {
        Ok(self.db.get_cf(self.cf(CF_RAFT_LOG)?, self.log_key(index))?)
    }

    pub(crate) fn read_meta<T: DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<T>, RaftStorageError> {
        self.db
            .get_cf(self.cf(CF_RAFT_META)?, self.scoped_key(key))?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    pub(crate) fn read_state_value<T: DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<T>, RaftStorageError> {
        self.db
            .get_cf(self.cf(CF_CONSENSUS_STATE)?, self.scoped_key(key))?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    pub(crate) fn sync_state_value<T: Serialize>(
        &self,
        key: &[u8],
        value: &T,
    ) -> Result<(), RaftStorageError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| RaftStorageError::WriterPoisoned)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(
            self.cf(CF_CONSENSUS_STATE)?,
            self.scoped_key(key),
            encode(value)?,
        );
        self.sync_write(batch)
    }

    pub fn scan_logs(&self, start: u64, end: u64) -> Result<Vec<(u64, Vec<u8>)>, RaftStorageError> {
        let cf = self.cf(CF_RAFT_LOG)?;
        let mode = rocksdb::IteratorMode::From(&self.log_key(start), rocksdb::Direction::Forward);
        let mut logs = Vec::new();
        for item in self.db.iterator_cf(cf, mode) {
            let (key, value) = item?;
            let Some(index) = self.decode_log_key(&key) else {
                continue;
            };
            if index >= end {
                break;
            }
            logs.push((index, value.to_vec()));
        }
        Ok(logs)
    }

    /// Remove `from` and every later entry.
    pub fn truncate_logs(&self, from: u64) -> Result<(), RaftStorageError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| RaftStorageError::WriterPoisoned)?;
        let cf_log = self.cf(CF_RAFT_LOG)?;
        let cf_meta = self.cf(CF_RAFT_META)?;
        if let Some(purged) = self.last_purged_index()? {
            if from <= purged {
                return Err(RaftStorageError::TruncatePurged { from, purged });
            }
        }
        let mut batch = WriteBatch::default();
        if let Some(last) = self.last_log_index()? {
            if from > last {
                return Ok(());
            }
            for index in from..=last {
                batch.delete_cf(cf_log, self.log_key(index));
            }
            if from == 0 {
                batch.delete_cf(cf_meta, self.scoped_key(KEY_LAST_LOG_INDEX));
            } else {
                batch.put_cf(
                    cf_meta,
                    self.scoped_key(KEY_LAST_LOG_INDEX),
                    (from - 1).to_be_bytes(),
                );
            }
        }
        self.sync_write(batch)
    }

    /// Purge every entry through `through`, inclusive.
    pub fn purge_logs(&self, through: u64) -> Result<(), RaftStorageError> {
        self.purge_logs_inner::<u64>(through, None)
    }

    pub(crate) fn purge_logs_with_id<T: Serialize>(
        &self,
        through: u64,
        log_id: &T,
    ) -> Result<(), RaftStorageError> {
        self.purge_logs_inner(through, Some(log_id))
    }

    fn purge_logs_inner<T: Serialize>(
        &self,
        through: u64,
        log_id: Option<&T>,
    ) -> Result<(), RaftStorageError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| RaftStorageError::WriterPoisoned)?;
        let cf_log = self.cf(CF_RAFT_LOG)?;
        let cf_meta = self.cf(CF_RAFT_META)?;
        let prior = self.last_purged_index()?;
        if let Some(current) = prior {
            if through < current {
                return Err(RaftStorageError::PurgeRegression {
                    current,
                    requested: through,
                });
            }
        }
        let first = prior.map_or(0, |prior| prior.saturating_add(1));
        let mut batch = WriteBatch::default();
        for index in first..=through {
            batch.delete_cf(cf_log, self.log_key(index));
        }
        batch.put_cf(
            cf_meta,
            self.scoped_key(KEY_LAST_PURGED_INDEX),
            through.to_be_bytes(),
        );
        if self.last_log_index()?.is_some_and(|last| last <= through) {
            batch.delete_cf(cf_meta, self.scoped_key(KEY_LAST_LOG_INDEX));
        }
        if let Some(log_id) = log_id {
            batch.put_cf(
                cf_meta,
                self.scoped_key(KEY_LAST_PURGED_LOG_ID),
                encode(log_id)?,
            );
        }
        self.sync_write(batch)
    }

    pub fn last_log_index(&self) -> Result<Option<u64>, RaftStorageError> {
        self.read_u64_meta(KEY_LAST_LOG_INDEX)
    }

    pub fn last_purged_index(&self) -> Result<Option<u64>, RaftStorageError> {
        self.read_u64_meta(KEY_LAST_PURGED_INDEX)
    }

    pub fn save_consensus_state(&self, state: &CertificationState) -> Result<(), RaftStorageError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| RaftStorageError::WriterPoisoned)?;
        let cf = self.cf(CF_CONSENSUS_STATE)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, self.scoped_key(KEY_CERTIFICATION_STATE), encode(state)?);
        self.sync_write(batch)
    }

    pub fn load_consensus_state(
        &self,
    ) -> Result<Option<PersistedConsensusState>, RaftStorageError> {
        let cf = self.cf(CF_CONSENSUS_STATE)?;
        self.db
            .get_cf(cf, self.scoped_key(KEY_CERTIFICATION_STATE))?
            .map(|bytes| {
                let state: CertificationState = decode(&bytes)?;
                Ok(PersistedConsensusState {
                    last_applied: state.last_applied(),
                    state,
                })
            })
            .transpose()
    }

    /// Build the compact application snapshot transferred by OpenRaft.
    ///
    /// Transaction bundles and product rows are deliberately absent.
    pub fn build_consensus_snapshot(&self) -> Result<Option<Vec<u8>>, RaftStorageError> {
        self.load_consensus_state()?
            .map(|persisted| encode(&persisted.state))
            .transpose()
    }

    /// Atomically install a compact certification snapshot.
    pub fn install_consensus_snapshot(&self, bytes: &[u8]) -> Result<(), RaftStorageError> {
        let state: CertificationState = decode(bytes)?;
        self.save_consensus_state(&state)
    }

    fn read_u64_meta(&self, key: &[u8]) -> Result<Option<u64>, RaftStorageError> {
        self.db
            .get_cf(self.cf(CF_RAFT_META)?, self.scoped_key(key))?
            .map(|bytes| {
                bytes
                    .as_slice()
                    .try_into()
                    .map(u64::from_be_bytes)
                    .map_err(|_| RaftStorageError::Codec("invalid u64 metadata value".into()))
            })
            .transpose()
    }

    fn sync_write(&self, batch: WriteBatch) -> Result<(), RaftStorageError> {
        #[cfg(test)]
        {
            let active = self.active_sync_writes.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active_sync_writes
                .fetch_max(active, Ordering::SeqCst);
            let _active_guard = ActiveSyncWriteGuard(&self.active_sync_writes);
            let fail = SYNC_WRITE_FAILURE.with(|failure| {
                let Some((target, completed)) = failure.get() else {
                    return false;
                };
                let ordinal = completed + 1;
                failure.set(if ordinal == target {
                    None
                } else {
                    Some((target, ordinal))
                });
                ordinal == target
            });
            if fail {
                return Err(RaftStorageError::Codec(
                    "injected Raft durable write failure".into(),
                ));
            }
        }
        let mut options = WriteOptions::default();
        options.set_sync(true);
        self.db.write_opt(batch, &options)?;
        Ok(())
    }

    #[cfg(test)]
    fn max_concurrent_sync_writes(&self) -> u64 {
        self.max_active_sync_writes.load(Ordering::SeqCst)
    }

    fn cf(&self, name: &'static str) -> Result<&ColumnFamily, RaftStorageError> {
        self.db
            .cf_handle(name)
            .ok_or(RaftStorageError::MissingColumnFamily(name))
    }

    fn scoped_key(&self, suffix: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(8 + 1 + suffix.len());
        key.extend_from_slice(&self.group_id.to_be_bytes());
        key.push(0);
        key.extend_from_slice(suffix);
        key
    }

    fn log_key(&self, index: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(16);
        key.extend_from_slice(&self.group_id.to_be_bytes());
        key.extend_from_slice(&index.to_be_bytes());
        key
    }

    fn decode_log_key(&self, key: &[u8]) -> Option<u64> {
        if key.len() != 16 || key[..8] != self.group_id.to_be_bytes() {
            return None;
        }
        Some(u64::from_be_bytes(key[8..].try_into().ok()?))
    }
}

#[cfg(test)]
struct ActiveSyncWriteGuard<'a>(&'a AtomicU64);

#[cfg(test)]
impl Drop for ActiveSyncWriteGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, RaftStorageError> {
    encode_to_vec(value, config::standard())
        .map_err(|error| RaftStorageError::Codec(error.to_string()))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, RaftStorageError> {
    decode_from_slice(bytes, config::standard())
        .and_then(|(value, consumed)| {
            if consumed == bytes.len() {
                Ok(value)
            } else {
                Err(bincode::error::DecodeError::OtherString(
                    "trailing bytes in consensus value".into(),
                ))
            }
        })
        .map_err(|error| RaftStorageError::Codec(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use tempfile::TempDir;

    use super::*;
    use crate::{
        BundleHash, CertifyTransaction, DurabilityLevel, NodeId, NodeIncarnation, TransactionId,
    };

    fn store(path: &Path) -> RocksRaftStore {
        RocksRaftStore::open(path, 7).unwrap()
    }

    #[test]
    fn log_keys_preserve_numeric_order_and_restart() {
        let dir = TempDir::new().unwrap();
        {
            let store = store(dir.path());
            store
                .append_logs(
                    &(0..=11)
                        .map(|index| (index, vec![index as u8]))
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            assert_eq!(
                store.scan_logs(8, 12).unwrap(),
                vec![
                    (8, vec![8]),
                    (9, vec![9]),
                    (10, vec![10]),
                    (11, vec![11]),
                ]
            );
        }
        let store = store(dir.path());
        assert_eq!(store.last_log_index().unwrap(), Some(11));
        assert_eq!(store.get_log(10).unwrap(), Some(vec![10]));
    }

    #[test]
    fn append_rejects_holes() {
        let dir = TempDir::new().unwrap();
        let store = store(dir.path());
        assert!(matches!(
            store.append_logs(&[(1, vec![])]),
            Err(RaftStorageError::LogHole {
                expected: 0,
                received: 1
            })
        ));
        store.append_logs(&[(0, vec![])]).unwrap();
        assert!(matches!(
            store.append_logs(&[(1, vec![]), (3, vec![])]),
            Err(RaftStorageError::NonConsecutive {
                previous: 1,
                next: 3
            })
        ));
    }

    #[test]
    fn truncate_and_purge_preserve_boundaries() {
        let dir = TempDir::new().unwrap();
        let store = store(dir.path());
        store
            .append_logs(&(0..=4).map(|i| (i, vec![i as u8])).collect::<Vec<_>>())
            .unwrap();
        store.truncate_logs(3).unwrap();
        assert_eq!(store.last_log_index().unwrap(), Some(2));
        assert!(store.get_log(3).unwrap().is_none());
        store.purge_logs(1).unwrap();
        assert_eq!(store.last_purged_index().unwrap(), Some(1));
        assert!(store.get_log(0).unwrap().is_none());
        assert_eq!(store.get_log(2).unwrap(), Some(vec![2]));
    }

    #[test]
    fn purging_through_last_log_clears_stale_last_log_pointer() {
        let dir = TempDir::new().unwrap();
        let store = store(dir.path());
        store
            .append_logs(&(0..=2).map(|i| (i, vec![i as u8])).collect::<Vec<_>>())
            .unwrap();
        store.purge_logs(2).unwrap();
        assert_eq!(store.last_purged_index().unwrap(), Some(2));
        assert_eq!(store.last_log_index().unwrap(), None);
        assert!(store.get_log(2).unwrap().is_none());
    }

    #[test]
    fn purge_is_monotonic_and_truncate_cannot_cross_purged_boundary() {
        let dir = TempDir::new().unwrap();
        let store = store(dir.path());
        store
            .append_logs(&(0..=5).map(|i| (i, vec![i as u8])).collect::<Vec<_>>())
            .unwrap();
        store.purge_logs(2).unwrap();

        assert!(matches!(
            store.purge_logs(1),
            Err(RaftStorageError::PurgeRegression {
                current: 2,
                requested: 1
            })
        ));
        assert!(matches!(
            store.truncate_logs(2),
            Err(RaftStorageError::TruncatePurged { from: 2, purged: 2 })
        ));
        assert_eq!(store.last_purged_index().unwrap(), Some(2));
        assert_eq!(store.last_log_index().unwrap(), Some(5));
        assert_eq!(store.get_log(3).unwrap(), Some(vec![3]));
    }

    #[test]
    fn vote_round_trips_after_durable_write() {
        let dir = TempDir::new().unwrap();
        let store = store(dir.path());
        store.save_vote(&("leader", 12_u64)).unwrap();
        assert_eq!(
            store.read_vote::<(String, u64)>().unwrap(),
            Some(("leader".into(), 12))
        );
    }

    #[test]
    fn concurrent_vote_and_log_writes_share_one_serialization_boundary() {
        let dir = TempDir::new().unwrap();
        let store = store(dir.path());
        let barrier = Arc::new(Barrier::new(3));

        let vote_store = store.clone();
        let vote_barrier = barrier.clone();
        assert!(Arc::ptr_eq(&store.writer, &vote_store.writer));
        let vote = thread::spawn(move || {
            vote_barrier.wait();
            vote_store.save_vote(&("leader", 14_u64))
        });

        let log_store = store.clone();
        let log_barrier = barrier.clone();
        assert!(Arc::ptr_eq(&store.writer, &log_store.writer));
        let log = thread::spawn(move || {
            log_barrier.wait();
            log_store.append_logs(&[(0, vec![7])])
        });

        barrier.wait();
        vote.join().unwrap().unwrap();
        log.join().unwrap().unwrap();

        assert_eq!(store.max_concurrent_sync_writes(), 1);
        assert_eq!(
            store.read_vote::<(String, u64)>().unwrap(),
            Some(("leader".into(), 14))
        );
        assert_eq!(store.get_log(0).unwrap(), Some(vec![7]));
        assert_eq!(store.last_log_index().unwrap(), Some(0));
    }

    #[test]
    fn raft_log_write_fault_leaves_log_and_index_unchanged() {
        let dir = TempDir::new().unwrap();
        let store = store(dir.path()).with_log_write_fault_hook(Arc::new(|| Ok(())));
        store.append_logs(&[(0, vec![0])]).unwrap();
        let store =
            store.with_log_write_fault_hook(Arc::new(|| Err("injected RaftLogWrite fault".into())));

        let error = store.append_logs(&[(1, vec![1])]).unwrap_err();
        assert!(error.to_string().contains("RaftLogWrite"));
        assert_eq!(store.last_log_index().unwrap(), Some(0));
        assert_eq!(store.get_log(0).unwrap(), Some(vec![0]));
        assert_eq!(store.get_log(1).unwrap(), None);
    }

    #[test]
    fn certification_state_is_identical_after_restart() {
        let dir = TempDir::new().unwrap();
        {
            let store = store(dir.path());
            let mut state = CertificationState::new([1; 32]).unwrap();
            state
                .apply(
                    CommitVersion(8),
                    &CertifyTransaction {
                        cluster_id_hash: [1; 32],
                        transaction_id: TransactionId([1; 16]),
                        snapshot_version: CommitVersion(0),
                        point_observations: vec![],
                        range_observations: vec![],
                        predicates: vec![],
                        assignment_predicates: vec![],
                        written_point_keys: vec![],
                        written_points: vec![],
                        advanced_range_stamps: vec![],
                        bundle_hash: BundleHash([2; 32]),
                        bundle_length: 4,
                        durability: DurabilityLevel::Local,
                        durable_holders: vec![NodeIncarnation {
                            node_id: NodeId(1),
                            incarnation: 3,
                        }],
                    },
                )
                .unwrap();
            store.save_consensus_state(&state).unwrap();
        }
        let recovered = store(dir.path()).load_consensus_state().unwrap().unwrap();
        assert_eq!(recovered.last_applied, CommitVersion(8));
        assert_eq!(recovered.state.last_applied(), CommitVersion(8));
    }

    #[test]
    fn consensus_snapshot_installs_into_empty_store() {
        let source_dir = TempDir::new().unwrap();
        let source = store(source_dir.path());
        let mut state = CertificationState::new([1; 32]).unwrap();
        state
            .apply(
                CommitVersion(11),
                &CertifyTransaction {
                    cluster_id_hash: [1; 32],
                    transaction_id: TransactionId([4; 16]),
                    snapshot_version: CommitVersion(0),
                    point_observations: vec![],
                    range_observations: vec![],
                    predicates: vec![],
                    assignment_predicates: vec![],
                    written_point_keys: vec![],
                    written_points: vec![],
                    advanced_range_stamps: vec![],
                    bundle_hash: BundleHash([5; 32]),
                    bundle_length: 4,
                    durability: DurabilityLevel::Local,
                    durable_holders: vec![NodeIncarnation {
                        node_id: NodeId(1),
                        incarnation: 3,
                    }],
                },
            )
            .unwrap();
        source.save_consensus_state(&state).unwrap();
        let snapshot = source.build_consensus_snapshot().unwrap().unwrap();

        let target_dir = TempDir::new().unwrap();
        {
            let target = store(target_dir.path());
            target.install_consensus_snapshot(&snapshot).unwrap();
        }
        let target = store(target_dir.path());
        assert_eq!(
            target.load_consensus_state().unwrap().unwrap().last_applied,
            CommitVersion(11)
        );
    }

    #[test]
    fn invalid_snapshot_install_preserves_durable_state() {
        let dir = TempDir::new().unwrap();
        let store = store(dir.path());
        let state = CertificationState::new([7; 32]).unwrap();
        store.save_consensus_state(&state).unwrap();

        assert!(matches!(
            store.install_consensus_snapshot(b"not-a-certification-snapshot"),
            Err(RaftStorageError::Codec(_))
        ));
        assert_eq!(
            store.load_consensus_state().unwrap().unwrap().state,
            state
        );
    }
}
