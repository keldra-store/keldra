use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anvil_atomic_program::MAX_OBJECT_PATH_BYTES;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rocksdb::{Direction, IteratorMode};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use super::{CF_HEADS, CF_METADATA, CF_VERSIONS};
use crate::key::{BucketId, BucketIdentity, STORAGE_KEY_FORMAT_VERSION, TenantId};
use crate::watch::{LOCAL_INVALIDATION_EPOCH_KEY, LOCAL_INVALIDATION_OFFSET_KEY};
use crate::{
    Head, MAX_CONTENT_TYPE_BYTES, MUTATION_STAMP_FORMAT, ObjectKey, SourceId, Store, Version,
    VersionId,
};

use super::object_snapshot::ObjectSnapshotError;

const CURRENT_HEAD_CURSOR_FORMAT: u8 = 1;
const CURRENT_HEAD_CURSOR_EXACT_PATH_DOMAIN: u8 = 0;
const MAX_CURRENT_HEAD_CURSOR_KEY_BYTES: usize = 16 * 1024;

/// One current object head and its exact immutable version descriptor.
///
/// Payload bytes and retained historical descriptors are deliberately absent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentObjectSnapshot {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub exact_path: String,
    pub head: Head,
    pub version: Version,
}

impl CurrentObjectSnapshot {
    pub fn validate(&self) -> Result<(), ObjectSnapshotError> {
        require_nonzero(self.tenant_id, "tenant ID")?;
        require_nonzero(self.bucket_id, "bucket ID")?;
        validate_exact_path(&self.exact_path)?;
        if self.head.version.0 == 0
            || self.version.id != self.head.version
            || self.version.deleted != self.head.deleted
            || self
                .version
                .content_type
                .as_ref()
                .is_some_and(|content_type| content_type.len() > MAX_CONTENT_TYPE_BYTES)
        {
            return Err(invalid_snapshot(
                "current head and exact version descriptor disagree or are malformed",
            ));
        }
        if let Some(stamp) = self.head.mutation_stamp {
            if stamp.format != MUTATION_STAMP_FORMAT
                || stamp.predecessor_version == Some(self.head.version)
                || stamp
                    .predecessor_version
                    .is_some_and(|predecessor| predecessor >= self.head.version)
                || stamp.program_commit_cursor == Some(0)
                || stamp.serving_fence_term == 0
                || stamp.source_id.node_id == 0
                || stamp.source_id.source_epoch == [0; 32]
                || stamp.source_journal_position == 0
            {
                return Err(invalid_snapshot("head mutation stamp is malformed"));
            }
        }
        super::version_blob_reference(&self.version)
            .map_err(|error| invalid_snapshot(error.to_string()))?;
        Ok(())
    }
}

/// Opaque continuation for one stable-ID current-head prefix scan.
#[derive(Clone, PartialEq, Eq)]
pub struct CurrentHeadCursor(String);

impl CurrentHeadCursor {
    pub fn from_token(token: impl Into<String>) -> Result<Self, ObjectSnapshotError> {
        let cursor = Self(token.into());
        cursor.decode()?;
        Ok(cursor)
    }

    pub fn as_token(&self) -> &str {
        &self.0
    }

    fn from_key(key: &[u8]) -> Self {
        // Match ObjectRecordCursor's exact-path domain so the existing opaque
        // peer cursor can carry either global or stable-prefix pages without
        // exposing a second wire format.
        let mut encoded = Vec::with_capacity(key.len() + 2);
        encoded.push(CURRENT_HEAD_CURSOR_FORMAT);
        encoded.push(CURRENT_HEAD_CURSOR_EXACT_PATH_DOMAIN);
        encoded.extend_from_slice(key);
        Self(URL_SAFE_NO_PAD.encode(encoded))
    }

    fn decode(&self) -> Result<Vec<u8>, ObjectSnapshotError> {
        let encoded = URL_SAFE_NO_PAD
            .decode(&self.0)
            .map_err(|_| ObjectSnapshotError::InvalidCursor)?;
        if encoded.first() != Some(&CURRENT_HEAD_CURSOR_FORMAT)
            || encoded.get(1) != Some(&CURRENT_HEAD_CURSOR_EXACT_PATH_DOMAIN)
            || encoded.len() <= BucketIdentity::ENCODED_BYTES + 2
            || encoded.len() > MAX_CURRENT_HEAD_CURSOR_KEY_BYTES + 2
        {
            return Err(ObjectSnapshotError::InvalidCursor);
        }
        let key = encoded[2..].to_vec();
        BucketIdentity::decode(&key[..BucketIdentity::ENCODED_BYTES])
            .map_err(|_| ObjectSnapshotError::InvalidCursor)?;
        std::str::from_utf8(&key[BucketIdentity::ENCODED_BYTES..])
            .map_err(|_| ObjectSnapshotError::InvalidCursor)?;
        Ok(key)
    }
}

impl fmt::Debug for CurrentHeadCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CurrentHeadCursor")
            .field("token", &"[OPAQUE]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentObjectSnapshotPage {
    pub heads: Vec<CurrentObjectSnapshot>,
    pub next_cursor: Option<CurrentHeadCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentObjectSnapshotFrame {
    pub heads: Vec<CurrentObjectSnapshot>,
}

enum SnapshotCommand {
    Pull(oneshot::Sender<Result<Option<CurrentObjectSnapshotFrame>, ObjectSnapshotError>>),
}

/// A credit-driven cursor backed by one local RocksDB snapshot.
///
/// The worker does not decode or retain a frame until `next_frame` supplies one
/// pull credit. Dropping this value closes the command channel and immediately
/// releases the snapshot.
pub struct CurrentObjectSnapshotScan {
    source: SourceId,
    captured_tail: u64,
    heads_visited: Arc<AtomicU64>,
    commands: mpsc::Sender<SnapshotCommand>,
    complete: bool,
}

impl CurrentObjectSnapshotScan {
    pub fn source(&self) -> SourceId {
        self.source
    }

    pub fn captured_tail(&self) -> u64 {
        self.captured_tail
    }

    /// Number of current-head records physically visited by this scoped
    /// RocksDB iterator. This includes records rejected by the caller's filter.
    pub fn heads_visited(&self) -> u64 {
        self.heads_visited.load(Ordering::Relaxed)
    }

    pub async fn next_frame(
        &mut self,
    ) -> Result<Option<CurrentObjectSnapshotFrame>, ObjectSnapshotError> {
        if self.complete {
            return Ok(None);
        }
        let (sender, receiver) = oneshot::channel();
        self.commands
            .send(SnapshotCommand::Pull(sender))
            .await
            .map_err(|_| object_storage("current-head snapshot worker stopped before pull"))?;
        match receiver.await {
            Ok(Ok(Some(frame))) => Ok(Some(frame)),
            Ok(Ok(None)) => {
                self.complete = true;
                Ok(None)
            }
            Ok(Err(error)) => {
                self.complete = true;
                Err(error)
            }
            Err(_) => {
                self.complete = true;
                Err(object_storage(
                    "current-head snapshot worker stopped before responding",
                ))
            }
        }
    }
}

impl Store {
    /// Reads one exact current head and its immutable descriptor by stable
    /// storage identity. Historical descriptors are not iterated or decoded.
    pub fn export_current_object_snapshot(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
    ) -> Result<Option<CurrentObjectSnapshot>, ObjectSnapshotError> {
        require_nonzero(tenant_id, "tenant ID")?;
        require_nonzero(bucket_id, "bucket ID")?;
        validate_exact_path(exact_path)?;
        let identity = stable_identity(tenant_id, bucket_id);
        let head_key = identity.head_key(exact_path);
        let snapshot = self.db.snapshot();
        let heads_cf = self.cf(CF_HEADS).map_err(object_storage)?;
        let Some(encoded_head) = snapshot
            .get_cf(heads_cf, &head_key)
            .map_err(object_storage)?
        else {
            return Ok(None);
        };
        decode_current_head(
            identity,
            &head_key,
            &encoded_head,
            &snapshot,
            self.cf(CF_VERSIONS).map_err(object_storage)?,
        )
        .map(Some)
    }

    /// Reads one bounded, sorted page across all local current heads. This is
    /// reserved for accepted cold discovery when stable bucket IDs are not yet
    /// known; scoped consumers should always use the prefix form below.
    pub fn export_all_current_heads(
        &self,
        cursor: Option<&CurrentHeadCursor>,
        max_records: u32,
        max_bytes: u64,
    ) -> Result<CurrentObjectSnapshotPage, ObjectSnapshotError> {
        super::object_snapshot::validate_limits(max_records, max_bytes)?;
        let prefix = [STORAGE_KEY_FORMAT_VERSION];
        let after = cursor.map(CurrentHeadCursor::decode).transpose()?;
        let start = after.as_deref().unwrap_or(&prefix);
        let snapshot = self.db.snapshot();
        let heads_cf = self.cf(CF_HEADS).map_err(object_storage)?;
        let versions_cf = self.cf(CF_VERSIONS).map_err(object_storage)?;
        let mut heads = Vec::with_capacity(max_records as usize);
        let mut encoded_bytes = 0_u64;
        let mut last_key = None;

        for item in snapshot.iterator_cf(heads_cf, IteratorMode::From(start, Direction::Forward)) {
            let (key, encoded_head) = item.map_err(object_storage)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if after
                .as_ref()
                .is_some_and(|after| key.as_ref() <= after.as_slice())
            {
                continue;
            }
            if key.len() <= BucketIdentity::ENCODED_BYTES {
                return Err(object_storage("current head key is malformed"));
            }
            let identity = BucketIdentity::decode(&key[..BucketIdentity::ENCODED_BYTES])
                .map_err(object_storage)?;
            let record =
                decode_current_head(identity, &key, &encoded_head, &snapshot, versions_cf)?;
            let record_bytes = encoded_record_bytes(&record)?;
            if record_bytes > max_bytes {
                return Err(ObjectSnapshotError::ExportRecordTooLarge {
                    required_bytes: record_bytes,
                });
            }
            if heads.len() == max_records as usize
                || encoded_bytes.saturating_add(record_bytes) > max_bytes
            {
                return Ok(CurrentObjectSnapshotPage {
                    heads,
                    next_cursor: last_key.as_deref().map(CurrentHeadCursor::from_key),
                });
            }
            encoded_bytes += record_bytes;
            last_key = Some(key.to_vec());
            heads.push(record);
        }

        Ok(CurrentObjectSnapshotPage {
            heads,
            next_cursor: None,
        })
    }

    /// Reads one bounded, sorted page beneath a stable tenant/bucket/path
    /// prefix. This uses the prefix-sortable head keys directly and creates no
    /// secondary catalogue or authoritative state.
    pub fn export_current_heads_by_prefix(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path_prefix: &str,
        cursor: Option<&CurrentHeadCursor>,
        max_records: u32,
        max_bytes: u64,
    ) -> Result<CurrentObjectSnapshotPage, ObjectSnapshotError> {
        validate_scan_request(tenant_id, bucket_id, path_prefix, max_records, max_bytes)?;
        let identity = stable_identity(tenant_id, bucket_id);
        let prefix = identity.head_key(path_prefix);
        let after = cursor.map(CurrentHeadCursor::decode).transpose()?;
        if let Some(after) = after.as_ref() {
            if !after.starts_with(&prefix) {
                return Err(ObjectSnapshotError::InvalidCursor);
            }
            let after_path = identity
                .decode_head_path(after)
                .map_err(|_| ObjectSnapshotError::InvalidCursor)?;
            if !path_is_within_prefix(after_path, path_prefix) {
                return Err(ObjectSnapshotError::InvalidCursor);
            }
        }
        let start = after.as_deref().unwrap_or(&prefix);
        let snapshot = self.db.snapshot();
        let heads_cf = self.cf(CF_HEADS).map_err(object_storage)?;
        let versions_cf = self.cf(CF_VERSIONS).map_err(object_storage)?;
        let mut heads = Vec::with_capacity(max_records as usize);
        let mut encoded_bytes = 0_u64;
        let mut last_key = None;

        for item in snapshot.iterator_cf(heads_cf, IteratorMode::From(start, Direction::Forward)) {
            let (key, encoded_head) = item.map_err(object_storage)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if after
                .as_ref()
                .is_some_and(|after| key.as_ref() <= after.as_slice())
            {
                continue;
            }
            let record =
                decode_current_head(identity, &key, &encoded_head, &snapshot, versions_cf)?;
            if !path_is_within_prefix(&record.exact_path, path_prefix) {
                continue;
            }
            let record_bytes = encoded_record_bytes(&record)?;
            if record_bytes > max_bytes {
                return Err(ObjectSnapshotError::ExportRecordTooLarge {
                    required_bytes: record_bytes,
                });
            }
            if heads.len() == max_records as usize
                || encoded_bytes.saturating_add(record_bytes) > max_bytes
            {
                return Ok(CurrentObjectSnapshotPage {
                    heads,
                    next_cursor: last_key.as_deref().map(CurrentHeadCursor::from_key),
                });
            }
            encoded_bytes += record_bytes;
            last_key = Some(key.to_vec());
            heads.push(record);
        }

        Ok(CurrentObjectSnapshotPage {
            heads,
            next_cursor: None,
        })
    }

    /// Captures one source epoch/tail and all matching current heads under the
    /// same local RocksDB snapshot, then streams bounded sorted frames.
    pub async fn start_current_head_snapshot_scan<F>(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path_prefix: &str,
        max_records: u32,
        max_bytes: u64,
        include: F,
    ) -> Result<CurrentObjectSnapshotScan, ObjectSnapshotError>
    where
        F: Fn(&CurrentObjectSnapshot) -> bool + Send + Sync + 'static,
    {
        validate_scan_request(tenant_id, bucket_id, path_prefix, max_records, max_bytes)?;
        let identity = stable_identity(tenant_id, bucket_id);
        let prefix = identity.head_key(path_prefix);
        let path_prefix = path_prefix.to_owned();
        let store = self.clone();
        let heads_visited = Arc::new(AtomicU64::new(0));
        let worker_heads_visited = heads_visited.clone();
        let (command_sender, command_receiver) = mpsc::channel(1);
        let (ready_sender, ready_receiver) = oneshot::channel();

        // Do not capture a locally visible metadata candidate whose quorum
        // proof is still pending. Waiting happens without the commit lock;
        // after the cut drains, the helper locks and rechecks before the
        // worker captures both the RocksDB snapshot and journal checkpoint.
        let commit_guard = self
            .lock_settled_source_snapshot()
            .await
            .map_err(object_storage)?;
        std::thread::Builder::new()
            .name(format!("anvil-head-snapshot-{}", self.node_id))
            .spawn(move || {
                run_snapshot_worker(
                    store,
                    identity,
                    prefix,
                    path_prefix,
                    max_records,
                    max_bytes,
                    include,
                    worker_heads_visited,
                    ready_sender,
                    command_receiver,
                );
            })
            .map_err(object_storage)?;
        let ready = ready_receiver
            .await
            .map_err(|_| object_storage("current-head snapshot worker failed during capture"))?;
        drop(commit_guard);
        let (source, captured_tail) = ready?;
        Ok(CurrentObjectSnapshotScan {
            source,
            captured_tail,
            heads_visited,
            commands: command_sender,
            complete: false,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn run_snapshot_worker<F>(
    store: Store,
    identity: BucketIdentity,
    prefix: Vec<u8>,
    path_prefix: String,
    max_records: u32,
    max_bytes: u64,
    include: F,
    heads_visited: Arc<AtomicU64>,
    ready: oneshot::Sender<Result<(SourceId, u64), ObjectSnapshotError>>,
    mut commands: mpsc::Receiver<SnapshotCommand>,
) where
    F: Fn(&CurrentObjectSnapshot) -> bool,
{
    let snapshot = store.db.snapshot();
    let metadata = match store.cf(CF_METADATA).map_err(object_storage) {
        Ok(metadata) => metadata,
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    let capture = (|| {
        let encoded_epoch = snapshot
            .get_cf(metadata, LOCAL_INVALIDATION_EPOCH_KEY)
            .map_err(object_storage)?
            .ok_or_else(|| object_storage("local source epoch is missing"))?;
        let source_epoch: [u8; 32] = encoded_epoch
            .as_slice()
            .try_into()
            .map_err(|_| object_storage("local source epoch is malformed"))?;
        if source_epoch == [0; 32] {
            return Err(object_storage("local source epoch is all zero"));
        }
        let encoded_tail = snapshot
            .get_cf(metadata, LOCAL_INVALIDATION_OFFSET_KEY)
            .map_err(object_storage)?
            .ok_or_else(|| object_storage("local source tail is missing"))?;
        let captured_tail = decode_counter(&encoded_tail)?;
        Ok((
            SourceId {
                node_id: store.node_id,
                source_epoch,
            },
            captured_tail,
        ))
    })();
    if ready.send(capture.clone()).is_err() || capture.is_err() {
        return;
    }

    let result: Result<(), ObjectSnapshotError> = (|| {
        let heads_cf = store.cf(CF_HEADS).map_err(object_storage)?;
        let versions_cf = store.cf(CF_VERSIONS).map_err(object_storage)?;
        let mut iterator =
            snapshot.iterator_cf(heads_cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut pending = None;
        let mut exhausted = false;
        while let Some(SnapshotCommand::Pull(response)) = commands.blocking_recv() {
            let frame = pull_snapshot_frame(
                &mut iterator,
                &prefix,
                identity,
                &path_prefix,
                max_records,
                max_bytes,
                &include,
                &heads_visited,
                &snapshot,
                versions_cf,
                &mut pending,
                &mut exhausted,
            );
            let failed = frame.is_err();
            if response.send(frame).is_err() {
                return Ok(());
            }
            if failed {
                return Ok(());
            }
        }
        Ok(())
    })();
    let _ = result;
}

#[allow(clippy::too_many_arguments)]
fn pull_snapshot_frame<F, I>(
    iterator: &mut I,
    prefix: &[u8],
    identity: BucketIdentity,
    path_prefix: &str,
    max_records: u32,
    max_bytes: u64,
    include: &F,
    heads_visited: &AtomicU64,
    snapshot: &rocksdb::SnapshotWithThreadMode<'_, rocksdb::DB>,
    versions_cf: &rocksdb::ColumnFamily,
    pending: &mut Option<CurrentObjectSnapshot>,
    exhausted: &mut bool,
) -> Result<Option<CurrentObjectSnapshotFrame>, ObjectSnapshotError>
where
    F: Fn(&CurrentObjectSnapshot) -> bool,
    I: Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>,
{
    if *exhausted && pending.is_none() {
        return Ok(None);
    }
    let mut heads = Vec::with_capacity(max_records as usize);
    let mut encoded_bytes = 0_u64;
    loop {
        let record = match pending.take() {
            Some(record) => Some(record),
            None => loop {
                let Some(item) = iterator.next() else {
                    *exhausted = true;
                    break None;
                };
                let (key, encoded_head) = item.map_err(object_storage)?;
                if !key.starts_with(prefix) {
                    *exhausted = true;
                    break None;
                }
                heads_visited.fetch_add(1, Ordering::Relaxed);
                let record =
                    decode_current_head(identity, &key, &encoded_head, snapshot, versions_cf)?;
                if path_is_within_prefix(&record.exact_path, path_prefix) && include(&record) {
                    break Some(record);
                }
            },
        };
        let Some(record) = record else {
            return if heads.is_empty() {
                Ok(None)
            } else {
                Ok(Some(CurrentObjectSnapshotFrame { heads }))
            };
        };
        let record_bytes = encoded_record_bytes(&record)?;
        if record_bytes > max_bytes {
            return Err(ObjectSnapshotError::ExportRecordTooLarge {
                required_bytes: record_bytes,
            });
        }
        if !heads.is_empty()
            && (heads.len() == max_records as usize
                || encoded_bytes.saturating_add(record_bytes) > max_bytes)
        {
            *pending = Some(record);
            return Ok(Some(CurrentObjectSnapshotFrame { heads }));
        }
        encoded_bytes = encoded_bytes
            .checked_add(record_bytes)
            .ok_or_else(|| object_storage("current-head snapshot frame length overflow"))?;
        heads.push(record);
        if heads.len() == max_records as usize {
            return Ok(Some(CurrentObjectSnapshotFrame { heads }));
        }
    }
}

fn decode_current_head(
    identity: BucketIdentity,
    encoded_head_key: &[u8],
    encoded_head: &[u8],
    snapshot: &rocksdb::SnapshotWithThreadMode<'_, rocksdb::DB>,
    versions_cf: &rocksdb::ColumnFamily,
) -> Result<CurrentObjectSnapshot, ObjectSnapshotError> {
    let exact_path = identity
        .decode_head_path(encoded_head_key)
        .map_err(object_storage)?
        .to_owned();
    let head: Head = serde_json::from_slice(encoded_head).map_err(object_storage)?;
    let encoded_version = snapshot
        .get_cf(
            versions_cf,
            exact_version_key(encoded_head_key, head.version),
        )
        .map_err(object_storage)?
        .ok_or_else(|| object_storage("current head references a missing version descriptor"))?;
    let version: Version = serde_json::from_slice(&encoded_version).map_err(object_storage)?;
    let record = CurrentObjectSnapshot {
        tenant_id: identity.tenant_id.0,
        bucket_id: identity.bucket_id.0,
        exact_path,
        head,
        version,
    };
    record.validate()?;
    Ok(record)
}

fn validate_scan_request(
    tenant_id: u64,
    bucket_id: u64,
    path_prefix: &str,
    max_records: u32,
    max_bytes: u64,
) -> Result<(), ObjectSnapshotError> {
    require_nonzero(tenant_id, "tenant ID")?;
    require_nonzero(bucket_id, "bucket ID")?;
    validate_path_prefix(path_prefix)?;
    super::object_snapshot::validate_limits(max_records, max_bytes)
}

fn validate_path_prefix(prefix: &str) -> Result<(), ObjectSnapshotError> {
    let path = prefix.strip_suffix('/').unwrap_or(prefix);
    if prefix.len() > MAX_OBJECT_PATH_BYTES
        || prefix.starts_with('/')
        || prefix.contains('\0')
        || prefix.chars().any(char::is_control)
        || (!path.is_empty()
            && path
                .split('/')
                .any(|segment| segment.is_empty() || matches!(segment, "." | "..")))
    {
        return Err(invalid_snapshot("object path prefix is invalid"));
    }
    Ok(())
}

fn path_is_within_prefix(path: &str, prefix: &str) -> bool {
    let prefix = prefix.strip_suffix('/').unwrap_or(prefix);
    prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn validate_exact_path(path: &str) -> Result<(), ObjectSnapshotError> {
    ObjectKey::new("t", "b", path)
        .map(|_| ())
        .map_err(|error| invalid_snapshot(error.to_string()))
}

fn stable_identity(tenant_id: u64, bucket_id: u64) -> BucketIdentity {
    BucketIdentity {
        tenant_id: TenantId(tenant_id),
        bucket_id: BucketId(bucket_id),
    }
}

fn exact_version_key(head_key: &[u8], version: VersionId) -> Vec<u8> {
    let mut key = Vec::with_capacity(head_key.len() + 9);
    key.extend_from_slice(head_key);
    key.push(0);
    key.extend_from_slice(&version.0.to_be_bytes());
    key
}

fn encoded_record_bytes(record: &CurrentObjectSnapshot) -> Result<u64, ObjectSnapshotError> {
    u64::try_from(serde_json::to_vec(record).map_err(object_storage)?.len())
        .map_err(|_| object_storage("current-head record size overflow"))
}

fn decode_counter(encoded: &[u8]) -> Result<u64, ObjectSnapshotError> {
    let encoded: [u8; 8] = encoded
        .try_into()
        .map_err(|_| object_storage("local source tail is malformed"))?;
    Ok(u64::from_be_bytes(encoded))
}

fn require_nonzero(value: u64, label: &str) -> Result<(), ObjectSnapshotError> {
    if value == 0 {
        Err(invalid_snapshot(format!("{label} must be non-zero")))
    } else {
        Ok(())
    }
}

fn invalid_snapshot(error: impl fmt::Display) -> ObjectSnapshotError {
    ObjectSnapshotError::InvalidRecord(error.to_string())
}

fn object_storage(error: impl fmt::Display) -> ObjectSnapshotError {
    ObjectSnapshotError::Storage(error.to_string())
}

#[cfg(test)]
mod tests;
