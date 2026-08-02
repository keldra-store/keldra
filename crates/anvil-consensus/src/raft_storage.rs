use std::{
    fmt::Debug,
    ops::{Bound, RangeBounds},
    path::Path,
    sync::{Arc, Mutex},
};

use openraft::{BasicNode, Entry, LogId, SnapshotMeta, Vote};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, Options, WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{codec, raft::DecisionRaftConfig};

const CF_LOG: &str = "raft_log";
const CF_META: &str = "raft_meta";
const CF_APPLIED: &str = "applied_journal";

const KEY_STORAGE_CONFIG: &[u8] = b"storage-config-v2";
const KEY_VOTE: &[u8] = b"vote";
const KEY_LAST_LOG_ID: &[u8] = b"last-log-id";
const KEY_LAST_PURGED_LOG_ID: &[u8] = b"last-purged-log-id";
const KEY_SNAPSHOT_META: &[u8] = b"snapshot-meta";
const KEY_SNAPSHOT_DATA: &[u8] = b"snapshot-data";

pub(crate) type RaftEntry = Entry<DecisionRaftConfig>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StorageConfig {
    pub max_commit_entries: u32,
    pub max_commit_bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct DurableSnapshot {
    pub meta: SnapshotMeta<u64, BasicNode>,
    pub data: Vec<u8>,
}

#[derive(Debug, Error)]
pub(crate) enum DurableStorageError {
    #[error("RocksDB error: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error(transparent)]
    Codec(#[from] codec::CodecError),
    #[error("missing RocksDB column family {0}")]
    MissingColumnFamily(&'static str),
    #[error("consensus database was created with {stored:?}, not requested {requested:?}")]
    ConfigurationMismatch {
        stored: StorageConfig,
        requested: StorageConfig,
    },
    #[error("Raft log append would create a hole: expected {expected}, received {received}")]
    LogHole { expected: u64, received: u64 },
    #[error("Raft log entries are not consecutive: {previous} then {next}")]
    NonConsecutive { previous: u64, next: u64 },
    #[error("cannot truncate at {from} across purged log index {purged}")]
    TruncatePurged { from: u64, purged: u64 },
    #[error("purge index regressed from {current} to {requested}")]
    PurgeRegression { current: u64, requested: u64 },
    #[error("durable snapshot metadata and data are incomplete")]
    IncompleteSnapshot,
    #[error("consensus RocksDB writer lock was poisoned")]
    WriterPoisoned,
}

/// One dedicated RocksDB containing only Raft protocol state and compact
/// decision-state recovery records.
#[derive(Clone)]
pub(crate) struct DurableStore {
    db: Arc<DB>,
    writer: Arc<Mutex<()>>,
    config: StorageConfig,
}

impl DurableStore {
    pub(crate) fn open(
        path: impl AsRef<Path>,
        requested: StorageConfig,
    ) -> Result<Self, DurableStorageError> {
        let mut options = uncompressed_options();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let descriptors = [CF_LOG, CF_META, CF_APPLIED]
            .map(|name| ColumnFamilyDescriptor::new(name, uncompressed_options()));
        let store = Self {
            db: Arc::new(DB::open_cf_descriptors(&options, path, descriptors)?),
            writer: Arc::new(Mutex::new(())),
            config: requested,
        };

        match store.read_meta::<StorageConfig>(KEY_STORAGE_CONFIG)? {
            Some(stored) if stored != requested => {
                Err(DurableStorageError::ConfigurationMismatch { stored, requested })
            }
            Some(_) => Ok(store),
            None => {
                store.write_meta_sync(KEY_STORAGE_CONFIG, &requested)?;
                Ok(store)
            }
        }
    }

    pub(crate) fn config(&self) -> StorageConfig {
        self.config
    }

    pub(crate) fn save_vote(&self, vote: &Vote<u64>) -> Result<(), DurableStorageError> {
        self.write_meta_sync(KEY_VOTE, vote)
    }

    pub(crate) fn read_vote(&self) -> Result<Option<Vote<u64>>, DurableStorageError> {
        self.read_meta(KEY_VOTE)
    }

    pub(crate) fn last_purged_log_id(&self) -> Result<Option<LogId<u64>>, DurableStorageError> {
        self.read_meta(KEY_LAST_PURGED_LOG_ID)
    }

    pub(crate) fn last_log_id(&self) -> Result<Option<LogId<u64>>, DurableStorageError> {
        self.read_meta(KEY_LAST_LOG_ID)
    }

    pub(crate) fn append_logs(&self, entries: &[RaftEntry]) -> Result<(), DurableStorageError> {
        if entries.is_empty() {
            return Ok(());
        }
        for pair in entries.windows(2) {
            if pair[1].log_id.index != pair[0].log_id.index + 1 {
                return Err(DurableStorageError::NonConsecutive {
                    previous: pair[0].log_id.index,
                    next: pair[1].log_id.index,
                });
            }
        }

        let _writer = self
            .writer
            .lock()
            .map_err(|_| DurableStorageError::WriterPoisoned)?;
        let expected = self
            .last_log_id()?
            .or(self.last_purged_log_id()?)
            .map_or(0, |log_id| log_id.index.saturating_add(1));
        if entries[0].log_id.index != expected {
            return Err(DurableStorageError::LogHole {
                expected,
                received: entries[0].log_id.index,
            });
        }

        let log_cf = self.cf(CF_LOG)?;
        let meta_cf = self.cf(CF_META)?;
        let mut batch = WriteBatch::default();
        for entry in entries {
            batch.put_cf(log_cf, log_key(entry.log_id.index), codec::encode(entry)?);
        }
        let last = entries.last().expect("non-empty entries");
        batch.put_cf(meta_cf, KEY_LAST_LOG_ID, codec::encode(&last.log_id)?);
        self.sync_write(batch)
    }

    pub(crate) fn scan_logs<R>(&self, range: R) -> Result<Vec<RaftEntry>, DurableStorageError>
    where
        R: RangeBounds<u64> + Debug,
    {
        let start = range_start(&range);
        let end = range_end(&range);
        let mode = rocksdb::IteratorMode::From(&log_key(start), rocksdb::Direction::Forward);
        let mut entries = Vec::new();
        for item in self.db.iterator_cf(self.cf(CF_LOG)?, mode) {
            let (key, bytes) = item?;
            let Some(index) = decode_index(&key) else {
                continue;
            };
            if index >= end {
                break;
            }
            entries.push(codec::decode(&bytes)?);
        }
        Ok(entries)
    }

    pub(crate) fn truncate_logs(&self, from: u64) -> Result<(), DurableStorageError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| DurableStorageError::WriterPoisoned)?;
        if let Some(purged) = self.last_purged_log_id()?
            && from <= purged.index
        {
            return Err(DurableStorageError::TruncatePurged {
                from,
                purged: purged.index,
            });
        }

        let log_cf = self.cf(CF_LOG)?;
        let meta_cf = self.cf(CF_META)?;
        let mut batch = WriteBatch::default();
        for entry in self.scan_logs(from..)? {
            batch.delete_cf(log_cf, log_key(entry.log_id.index));
        }
        let previous = if from == 0 {
            None
        } else {
            self.scan_logs(from - 1..from)?.into_iter().next()
        };
        if let Some(previous) = previous {
            batch.put_cf(meta_cf, KEY_LAST_LOG_ID, codec::encode(&previous.log_id)?);
        } else {
            batch.delete_cf(meta_cf, KEY_LAST_LOG_ID);
        }
        self.sync_write(batch)
    }

    pub(crate) fn purge_logs(&self, through: LogId<u64>) -> Result<(), DurableStorageError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| DurableStorageError::WriterPoisoned)?;
        if let Some(current) = self.last_purged_log_id()?
            && through.index < current.index
        {
            return Err(DurableStorageError::PurgeRegression {
                current: current.index,
                requested: through.index,
            });
        }

        let log_cf = self.cf(CF_LOG)?;
        let meta_cf = self.cf(CF_META)?;
        let mut batch = WriteBatch::default();
        for entry in self.scan_logs(..=through.index)? {
            batch.delete_cf(log_cf, log_key(entry.log_id.index));
        }
        batch.put_cf(meta_cf, KEY_LAST_PURGED_LOG_ID, codec::encode(&through)?);
        if self
            .last_log_id()?
            .is_some_and(|last| last.index <= through.index)
        {
            batch.delete_cf(meta_cf, KEY_LAST_LOG_ID);
        }
        self.sync_write(batch)
    }

    /// Append state-machine input records rather than rewriting the entire
    /// materialized state on every decision. A durable snapshot periodically
    /// compacts this journal.
    pub(crate) fn append_applied(&self, entries: &[RaftEntry]) -> Result<(), DurableStorageError> {
        if entries.is_empty() {
            return Ok(());
        }
        let _writer = self
            .writer
            .lock()
            .map_err(|_| DurableStorageError::WriterPoisoned)?;
        let applied_cf = self.cf(CF_APPLIED)?;
        let mut batch = WriteBatch::default();
        for entry in entries {
            batch.put_cf(
                applied_cf,
                log_key(entry.log_id.index),
                codec::encode(entry)?,
            );
        }
        self.sync_write(batch)
    }

    pub(crate) fn scan_applied(&self) -> Result<Vec<RaftEntry>, DurableStorageError> {
        let mut entries = Vec::new();
        for item in self
            .db
            .iterator_cf(self.cf(CF_APPLIED)?, rocksdb::IteratorMode::Start)
        {
            let (_, bytes) = item?;
            entries.push(codec::decode(&bytes)?);
        }
        Ok(entries)
    }

    pub(crate) fn save_snapshot(
        &self,
        snapshot: &DurableSnapshot,
        clear_all_applied: bool,
    ) -> Result<(), DurableStorageError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| DurableStorageError::WriterPoisoned)?;
        let meta_cf = self.cf(CF_META)?;
        let applied_cf = self.cf(CF_APPLIED)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(meta_cf, KEY_SNAPSHOT_META, codec::encode(&snapshot.meta)?);
        batch.put_cf(meta_cf, KEY_SNAPSHOT_DATA, &snapshot.data);

        let through = snapshot.meta.last_log_id.map(|log_id| log_id.index);
        for entry in self.scan_applied()? {
            if clear_all_applied || through.is_some_and(|index| entry.log_id.index <= index) {
                batch.delete_cf(applied_cf, log_key(entry.log_id.index));
            }
        }
        self.sync_write(batch)
    }

    pub(crate) fn load_snapshot(&self) -> Result<Option<DurableSnapshot>, DurableStorageError> {
        let meta = self.read_meta::<SnapshotMeta<u64, BasicNode>>(KEY_SNAPSHOT_META)?;
        let data = self.db.get_cf(self.cf(CF_META)?, KEY_SNAPSHOT_DATA)?;
        match (meta, data) {
            (None, None) => Ok(None),
            (Some(meta), Some(data)) => Ok(Some(DurableSnapshot {
                meta,
                data: data.to_vec(),
            })),
            _ => Err(DurableStorageError::IncompleteSnapshot),
        }
    }

    fn read_meta<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>, DurableStorageError> {
        self.db
            .get_cf(self.cf(CF_META)?, key)?
            .map(|bytes| codec::decode(&bytes).map_err(Into::into))
            .transpose()
    }

    fn write_meta_sync<T: Serialize + ?Sized>(
        &self,
        key: &[u8],
        value: &T,
    ) -> Result<(), DurableStorageError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| DurableStorageError::WriterPoisoned)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(self.cf(CF_META)?, key, codec::encode(value)?);
        self.sync_write(batch)
    }

    fn sync_write(&self, batch: WriteBatch) -> Result<(), DurableStorageError> {
        let mut options = WriteOptions::default();
        options.set_sync(true);
        self.db.write_opt(batch, &options)?;
        Ok(())
    }

    fn cf(&self, name: &'static str) -> Result<&ColumnFamily, DurableStorageError> {
        self.db
            .cf_handle(name)
            .ok_or(DurableStorageError::MissingColumnFamily(name))
    }
}

fn log_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

fn uncompressed_options() -> Options {
    let mut options = Options::default();
    options.set_compression_type(DBCompressionType::None);
    options.set_bottommost_compression_type(DBCompressionType::None);
    options
}

fn decode_index(bytes: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(bytes.try_into().ok()?))
}

fn range_start(range: &impl RangeBounds<u64>) -> u64 {
    match range.start_bound() {
        Bound::Included(value) => *value,
        Bound::Excluded(value) => value.saturating_add(1),
        Bound::Unbounded => 0,
    }
}

fn range_end(range: &impl RangeBounds<u64>) -> u64 {
    match range.end_bound() {
        Bound::Included(value) => value.saturating_add(1),
        Bound::Excluded(value) => *value,
        Bound::Unbounded => u64::MAX,
    }
}
