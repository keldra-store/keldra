use std::fmt;

use anvil_atomic_program::MAX_OBJECT_PATH_BYTES;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use tokio::sync::{mpsc, oneshot};

use rocksdb::{Direction, IteratorMode};
use serde::{Deserialize, Serialize};

use super::{CF_HEADS, CF_METADATA, CF_VERSIONS, Store};
use crate::key::{BucketId, BucketIdentity, TenantId};
use crate::watch::{LOCAL_INVALIDATION_EPOCH_KEY, LOCAL_INVALIDATION_OFFSET_KEY};
use crate::{Head, ObjectKey, SourceId, Version, VersionId};

use super::object_snapshot::ObjectSnapshotError;

const RETAINED_CURSOR_FORMAT: u8 = 1;
const RETAINED_CURSOR_EXACT_PATH_DOMAIN: u8 = 0;
const MAX_RETAINED_CURSOR_KEY_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedHeadState {
    pub version: VersionId,
    pub deleted: bool,
}

/// One retained immutable descriptor in stable `(path, version)` order.
/// Repeating the tiny head state avoids ever collecting all versions of one
/// path merely to decide whether that path is currently live.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedObjectSnapshot {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub exact_path: String,
    pub version: Version,
    pub current_head: RetainedHeadState,
}

impl RetainedObjectSnapshot {
    pub fn validate(&self) -> Result<(), ObjectSnapshotError> {
        if self.tenant_id == 0 || self.bucket_id == 0 {
            return Err(invalid_snapshot("stable IDs must be non-zero"));
        }
        ObjectKey::new("typed", "retained", &self.exact_path)
            .map_err(|error| invalid_snapshot(error.to_string()))?;
        if self.version.id.0 == 0
            || self.current_head.version.0 == 0
            || self.version.id > self.current_head.version
            || self.version.deleted != self.version.blob.is_none()
        {
            return Err(invalid_snapshot(
                "retained descriptor or current head state is malformed",
            ));
        }
        if self.version.id == self.current_head.version
            && self.version.deleted != self.current_head.deleted
        {
            return Err(invalid_snapshot(
                "current retained descriptor disagrees with head state",
            ));
        }
        super::version_blob_reference(&self.version)
            .map_err(|error| invalid_snapshot(error.to_string()))?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedVersionCursor {
    pub exact_path: String,
    pub version: VersionId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetainedObjectSnapshotFrame {
    pub records: Vec<RetainedObjectSnapshot>,
    pub through: RetainedVersionCursor,
}

/// Opaque continuation for a stable-ID retained-version prefix page.
///
/// The token deliberately uses the same outer envelope as the existing object
/// cursor. Cluster peers can therefore forward it opaquely without teaching
/// the wire protocol about RocksDB keys, while this type still validates the
/// exact retained-version position before storage uses it.
#[derive(Clone, PartialEq, Eq)]
pub struct RetainedObjectCursor(String);

impl RetainedObjectCursor {
    pub fn from_token(token: impl Into<String>) -> Result<Self, ObjectSnapshotError> {
        let cursor = Self(token.into());
        cursor.decode()?;
        Ok(cursor)
    }

    pub fn as_token(&self) -> &str {
        &self.0
    }

    fn from_key(key: &[u8]) -> Self {
        let mut encoded = Vec::with_capacity(key.len() + 2);
        encoded.push(RETAINED_CURSOR_FORMAT);
        encoded.push(RETAINED_CURSOR_EXACT_PATH_DOMAIN);
        encoded.extend_from_slice(key);
        Self(URL_SAFE_NO_PAD.encode(encoded))
    }

    fn decode(&self) -> Result<Vec<u8>, ObjectSnapshotError> {
        let encoded = URL_SAFE_NO_PAD
            .decode(&self.0)
            .map_err(|_| ObjectSnapshotError::InvalidCursor)?;
        if encoded.first() != Some(&RETAINED_CURSOR_FORMAT)
            || encoded.get(1) != Some(&RETAINED_CURSOR_EXACT_PATH_DOMAIN)
            || encoded.len() <= BucketIdentity::ENCODED_BYTES + 11
            || encoded.len() > MAX_RETAINED_CURSOR_KEY_BYTES + 2
        {
            return Err(ObjectSnapshotError::InvalidCursor);
        }
        let key = encoded[2..].to_vec();
        validate_version_key(&key).map_err(|_| ObjectSnapshotError::InvalidCursor)?;
        Ok(key)
    }
}

impl fmt::Debug for RetainedObjectCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedObjectCursor")
            .field("token", &"[OPAQUE]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetainedObjectSnapshotPage {
    pub records: Vec<RetainedObjectSnapshot>,
    pub next_cursor: Option<RetainedObjectCursor>,
}

enum RetainedSnapshotCommand {
    Pull(oneshot::Sender<Result<Option<RetainedObjectSnapshotFrame>, ObjectSnapshotError>>),
}

/// Credit-driven stream over one held RocksDB snapshot. Each pull decodes at
/// most one configured frame, so one path with millions of versions remains
/// bounded by the same record and byte limits as millions of paths.
pub struct RetainedObjectSnapshotScan {
    source: SourceId,
    captured_tail: u64,
    commands: mpsc::Sender<RetainedSnapshotCommand>,
    complete: bool,
}

impl RetainedObjectSnapshotScan {
    pub fn source(&self) -> SourceId {
        self.source
    }

    pub fn captured_tail(&self) -> u64 {
        self.captured_tail
    }

    pub async fn next_frame(
        &mut self,
    ) -> Result<Option<RetainedObjectSnapshotFrame>, ObjectSnapshotError> {
        if self.complete {
            return Ok(None);
        }
        let (sender, receiver) = oneshot::channel();
        self.commands
            .send(RetainedSnapshotCommand::Pull(sender))
            .await
            .map_err(|_| object_storage("retained snapshot worker stopped before pull"))?;
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
                    "retained snapshot worker stopped before responding",
                ))
            }
        }
    }
}

impl Store {
    /// Reads one bounded page of retained descriptors beneath one stable
    /// tenant/bucket/path prefix. The iterator seeks directly into
    /// `CF_VERSIONS`; unrelated object heads and versions are never walked.
    pub fn export_retained_objects_by_prefix(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path_prefix: &str,
        cursor: Option<&RetainedObjectCursor>,
        max_records: u32,
        max_bytes: u64,
    ) -> Result<RetainedObjectSnapshotPage, ObjectSnapshotError> {
        validate_request(tenant_id, bucket_id, path_prefix, max_records, max_bytes)?;
        let identity = BucketIdentity {
            tenant_id: TenantId(tenant_id),
            bucket_id: BucketId(bucket_id),
        };
        let prefix = identity.head_key(path_prefix);
        let after = cursor.map(RetainedObjectCursor::decode).transpose()?;
        if let Some(after) = after.as_ref() {
            if !after.starts_with(&prefix) {
                return Err(ObjectSnapshotError::InvalidCursor);
            }
            let head_key =
                validate_version_key(after).map_err(|_| ObjectSnapshotError::InvalidCursor)?;
            let after_path = identity
                .decode_head_path(head_key)
                .map_err(|_| ObjectSnapshotError::InvalidCursor)?;
            if !path_is_within_prefix(after_path, path_prefix) {
                return Err(ObjectSnapshotError::InvalidCursor);
            }
        }

        let snapshot = self.db.snapshot();
        let heads = self.cf(CF_HEADS).map_err(object_storage)?;
        let versions = self.cf(CF_VERSIONS).map_err(object_storage)?;
        let start = after.as_deref().unwrap_or(&prefix);
        let mut iterator =
            snapshot.iterator_cf(versions, IteratorMode::From(start, Direction::Forward));
        let mut records = Vec::with_capacity(max_records as usize);
        let mut encoded_bytes = 0_u64;
        let mut last_key = None;
        let mut cached_head = None;

        for item in &mut iterator {
            let (key, encoded) = item.map_err(object_storage)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if after
                .as_ref()
                .is_some_and(|after| key.as_ref() <= after.as_slice())
            {
                continue;
            }
            let record = decode_retained_record(
                identity,
                &key,
                &encoded,
                &snapshot,
                heads,
                &mut cached_head,
            )?;
            if !path_is_within_prefix(&record.exact_path, path_prefix) {
                continue;
            }
            let record_bytes =
                u64::try_from(serde_json::to_vec(&record).map_err(object_storage)?.len())
                    .map_err(|_| object_storage("retained snapshot record size overflow"))?;
            if record_bytes > max_bytes {
                return Err(ObjectSnapshotError::ExportRecordTooLarge {
                    required_bytes: record_bytes,
                });
            }
            if records.len() == max_records as usize
                || encoded_bytes.saturating_add(record_bytes) > max_bytes
            {
                return Ok(RetainedObjectSnapshotPage {
                    records,
                    next_cursor: last_key.as_deref().map(RetainedObjectCursor::from_key),
                });
            }
            encoded_bytes = encoded_bytes
                .checked_add(record_bytes)
                .ok_or_else(|| object_storage("retained snapshot page size overflow"))?;
            last_key = Some(key.to_vec());
            records.push(record);
        }

        Ok(RetainedObjectSnapshotPage {
            records,
            next_cursor: None,
        })
    }

    /// Captures source identity/tail and a stable scoped retained-descriptor
    /// stream under one local RocksDB snapshot.
    pub async fn start_retained_object_snapshot_scan<F>(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path_prefix: &str,
        max_records: u32,
        max_bytes: u64,
        include: F,
    ) -> Result<RetainedObjectSnapshotScan, ObjectSnapshotError>
    where
        F: Fn(&RetainedObjectSnapshot) -> bool + Send + Sync + 'static,
    {
        validate_request(tenant_id, bucket_id, path_prefix, max_records, max_bytes)?;
        let identity = BucketIdentity {
            tenant_id: TenantId(tenant_id),
            bucket_id: BucketId(bucket_id),
        };
        let prefix = identity.head_key(path_prefix);
        let path_prefix = path_prefix.to_owned();
        let store = self.clone();
        let (commands, receiver) = mpsc::channel(1);
        let (ready, captured) = oneshot::channel();

        let commit_guard = self
            .lock_settled_source_snapshot()
            .await
            .map_err(object_storage)?;
        std::thread::Builder::new()
            .name(format!("anvil-retained-snapshot-{}", self.node_id))
            .spawn(move || {
                run_retained_worker(
                    store,
                    identity,
                    prefix,
                    path_prefix,
                    max_records,
                    max_bytes,
                    include,
                    ready,
                    receiver,
                );
            })
            .map_err(object_storage)?;
        let capture = captured
            .await
            .map_err(|_| object_storage("retained snapshot worker failed during capture"))?;
        drop(commit_guard);
        let (source, captured_tail) = capture?;
        Ok(RetainedObjectSnapshotScan {
            source,
            captured_tail,
            commands,
            complete: false,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn run_retained_worker<F>(
    store: Store,
    identity: BucketIdentity,
    prefix: Vec<u8>,
    path_prefix: String,
    max_records: u32,
    max_bytes: u64,
    include: F,
    ready: oneshot::Sender<Result<(SourceId, u64), ObjectSnapshotError>>,
    mut commands: mpsc::Receiver<RetainedSnapshotCommand>,
) where
    F: Fn(&RetainedObjectSnapshot) -> bool,
{
    let snapshot = store.db.snapshot();
    let capture = capture_source(&store, &snapshot);
    if ready.send(capture.clone()).is_err() || capture.is_err() {
        return;
    }
    let versions = match store.cf(CF_VERSIONS).map_err(object_storage) {
        Ok(column) => column,
        Err(_) => return,
    };
    let heads = match store.cf(CF_HEADS).map_err(object_storage) {
        Ok(column) => column,
        Err(_) => return,
    };
    let mut iterator =
        snapshot.iterator_cf(versions, IteratorMode::From(&prefix, Direction::Forward));
    let mut pending = None;
    let mut cached_head = None;
    let mut exhausted = false;
    while let Some(RetainedSnapshotCommand::Pull(response)) = commands.blocking_recv() {
        let frame = pull_retained_frame(
            &mut iterator,
            &snapshot,
            heads,
            identity,
            &prefix,
            &path_prefix,
            max_records,
            max_bytes,
            &include,
            &mut pending,
            &mut cached_head,
            &mut exhausted,
        );
        let failed = frame.is_err();
        if response.send(frame).is_err() || failed {
            return;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn pull_retained_frame<F, I>(
    iterator: &mut I,
    snapshot: &rocksdb::SnapshotWithThreadMode<'_, rocksdb::DB>,
    heads: &rocksdb::ColumnFamily,
    identity: BucketIdentity,
    prefix: &[u8],
    path_prefix: &str,
    max_records: u32,
    max_bytes: u64,
    include: &F,
    pending: &mut Option<RetainedObjectSnapshot>,
    cached_head: &mut Option<(Vec<u8>, RetainedHeadState)>,
    exhausted: &mut bool,
) -> Result<Option<RetainedObjectSnapshotFrame>, ObjectSnapshotError>
where
    F: Fn(&RetainedObjectSnapshot) -> bool,
    I: Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>,
{
    if *exhausted && pending.is_none() {
        return Ok(None);
    }
    let mut records = Vec::with_capacity(max_records as usize);
    let mut encoded_bytes = 0_u64;
    loop {
        let record = match pending.take() {
            Some(record) => Some(record),
            None => loop {
                let Some(item) = iterator.next() else {
                    *exhausted = true;
                    break None;
                };
                let (key, encoded) = item.map_err(object_storage)?;
                if !key.starts_with(prefix) {
                    *exhausted = true;
                    break None;
                }
                let record =
                    decode_retained_record(identity, &key, &encoded, snapshot, heads, cached_head)?;
                if path_is_within_prefix(&record.exact_path, path_prefix) && include(&record) {
                    break Some(record);
                }
            },
        };
        let Some(record) = record else {
            return if records.is_empty() {
                Ok(None)
            } else {
                Ok(Some(frame(records)))
            };
        };
        let record_bytes =
            u64::try_from(serde_json::to_vec(&record).map_err(object_storage)?.len())
                .map_err(|_| object_storage("retained snapshot record size overflow"))?;
        if record_bytes > max_bytes {
            return Err(ObjectSnapshotError::ExportRecordTooLarge {
                required_bytes: record_bytes,
            });
        }
        if !records.is_empty()
            && (records.len() == max_records as usize
                || encoded_bytes.saturating_add(record_bytes) > max_bytes)
        {
            *pending = Some(record);
            return Ok(Some(frame(records)));
        }
        encoded_bytes = encoded_bytes
            .checked_add(record_bytes)
            .ok_or_else(|| object_storage("retained snapshot frame size overflow"))?;
        records.push(record);
        if records.len() == max_records as usize {
            return Ok(Some(frame(records)));
        }
    }
}

fn frame(records: Vec<RetainedObjectSnapshot>) -> RetainedObjectSnapshotFrame {
    let last = records.last().expect("retained frame is non-empty");
    RetainedObjectSnapshotFrame {
        through: RetainedVersionCursor {
            exact_path: last.exact_path.clone(),
            version: last.version.id,
        },
        records,
    }
}

fn decode_retained_record(
    identity: BucketIdentity,
    key: &[u8],
    encoded: &[u8],
    snapshot: &rocksdb::SnapshotWithThreadMode<'_, rocksdb::DB>,
    heads: &rocksdb::ColumnFamily,
    cached_head: &mut Option<(Vec<u8>, RetainedHeadState)>,
) -> Result<RetainedObjectSnapshot, ObjectSnapshotError> {
    let head_key = validate_version_key(key)?;
    let exact_path = identity
        .decode_head_path(head_key)
        .map_err(object_storage)?
        .to_owned();
    let key_version = VersionId(u64::from_be_bytes(
        key[key.len() - 8..].try_into().expect("fixed slice"),
    ));
    let version: Version = serde_json::from_slice(encoded).map_err(object_storage)?;
    if version.id != key_version {
        return Err(object_storage(
            "retained version key and descriptor disagree",
        ));
    }
    let current_head = match cached_head {
        Some((cached_key, state)) if cached_key.as_slice() == head_key => *state,
        _ => {
            let encoded_head = snapshot
                .get_cf(heads, head_key)
                .map_err(object_storage)?
                .ok_or_else(|| object_storage("retained descriptor has no current head"))?;
            let head: Head = serde_json::from_slice(&encoded_head).map_err(object_storage)?;
            let state = RetainedHeadState {
                version: head.version,
                deleted: head.deleted,
            };
            *cached_head = Some((head_key.to_vec(), state));
            state
        }
    };
    let record = RetainedObjectSnapshot {
        tenant_id: identity.tenant_id.0,
        bucket_id: identity.bucket_id.0,
        exact_path,
        version,
        current_head,
    };
    record.validate()?;
    Ok(record)
}

fn validate_version_key(key: &[u8]) -> Result<&[u8], ObjectSnapshotError> {
    let minimum = BucketIdentity::ENCODED_BYTES + 1 + 8 + 1;
    if key.len() < minimum || key[key.len() - 9] != 0 {
        return Err(object_storage("retained version key is malformed"));
    }
    BucketIdentity::decode(&key[..BucketIdentity::ENCODED_BYTES]).map_err(object_storage)?;
    let head_key = &key[..key.len() - 9];
    let path =
        std::str::from_utf8(&head_key[BucketIdentity::ENCODED_BYTES..]).map_err(object_storage)?;
    if path.len() > MAX_OBJECT_PATH_BYTES {
        return Err(object_storage("retained version path is too long"));
    }
    let version = u64::from_be_bytes(key[key.len() - 8..].try_into().expect("fixed slice"));
    if version == 0 {
        return Err(object_storage("retained version key has zero version"));
    }
    Ok(head_key)
}

fn capture_source(
    store: &Store,
    snapshot: &rocksdb::SnapshotWithThreadMode<'_, rocksdb::DB>,
) -> Result<(SourceId, u64), ObjectSnapshotError> {
    let metadata = store.cf(CF_METADATA).map_err(object_storage)?;
    let epoch = snapshot
        .get_cf(metadata, LOCAL_INVALIDATION_EPOCH_KEY)
        .map_err(object_storage)?
        .ok_or_else(|| object_storage("local source epoch is missing"))?;
    let source_epoch: [u8; 32] = epoch
        .as_slice()
        .try_into()
        .map_err(|_| object_storage("local source epoch is malformed"))?;
    if source_epoch == [0; 32] {
        return Err(object_storage("local source epoch is all zero"));
    }
    let tail = snapshot
        .get_cf(metadata, LOCAL_INVALIDATION_OFFSET_KEY)
        .map_err(object_storage)?
        .ok_or_else(|| object_storage("local source tail is missing"))?;
    let captured_tail = u64::from_be_bytes(
        tail.as_slice()
            .try_into()
            .map_err(|_| object_storage("local source tail is malformed"))?,
    );
    Ok((
        SourceId {
            node_id: store.node_id,
            source_epoch,
        },
        captured_tail,
    ))
}

fn validate_request(
    tenant_id: u64,
    bucket_id: u64,
    path_prefix: &str,
    max_records: u32,
    max_bytes: u64,
) -> Result<(), ObjectSnapshotError> {
    if tenant_id == 0 || bucket_id == 0 {
        return Err(invalid_snapshot("stable IDs must be non-zero"));
    }
    let trimmed = path_prefix.strip_suffix('/').unwrap_or(path_prefix);
    if path_prefix.starts_with('/')
        || path_prefix.contains('\0')
        || path_prefix.chars().any(char::is_control)
        || (!trimmed.is_empty()
            && trimmed
                .split('/')
                .any(|part| part.is_empty() || matches!(part, "." | "..")))
    {
        return Err(invalid_snapshot("object path prefix is invalid"));
    }
    super::object_snapshot::validate_limits(max_records, max_bytes)
}

fn path_is_within_prefix(path: &str, prefix: &str) -> bool {
    let prefix = prefix.strip_suffix('/').unwrap_or(prefix);
    prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn invalid_snapshot(error: impl std::fmt::Display) -> ObjectSnapshotError {
    ObjectSnapshotError::InvalidRecord(error.to_string())
}

fn object_storage(error: impl std::fmt::Display) -> ObjectSnapshotError {
    ObjectSnapshotError::Storage(error.to_string())
}

#[cfg(test)]
mod tests;
