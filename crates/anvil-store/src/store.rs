use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anvil_atomic_program::{LocalLockManager, ObjectPath};
use anyhow::{Context, Result};
use rocksdb::{
    ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};

use crate::watch::{
    LocalInvalidation, MAX_LOCAL_INVALIDATION_SCAN_RECORDS, invalidation_key, offset_from_key,
};
use crate::{
    BatchOperation, BatchOutcome, BlobReader, BlobRef, BlobStore, BucketPolicy, DeleteRequest,
    Head, INLINE_PAYLOAD_MAX_BYTES, InlinePayload, MutationError, MutationReceipt, Object,
    ObjectKey, Precondition, PreparedArtifactRepository, PublishRequest, PutRequest, Version,
    VersionClock, VersionId,
};

const PROGRAM_DEFINITION_PREFIX: &str = "_anvil/programs/";

pub(crate) const CF_HEADS: &str = "heads";
pub(crate) const CF_VERSIONS: &str = "versions";
const CF_RECEIPTS: &str = "receipts";
const CF_POLICIES: &str = "policies";
const CF_LOCAL_INVALIDATIONS: &str = "local_invalidations";
pub(crate) const CF_PROGRAM_ARTIFACTS: &str = "program_artifacts";
pub(crate) const CF_PROGRAM_COMMITS: &str = "program_commits";
pub(crate) const CF_OUTBOX: &str = "outbox";
pub(crate) const CF_METADATA: &str = "metadata";
pub(crate) const CF_AUTHZ_TENANTS: &str = "authz_tenants";
pub(crate) const CF_AUTHZ_SCHEMAS: &str = "authz_schemas";
pub(crate) const CF_AUTHZ_BINDINGS: &str = "authz_bindings";
pub(crate) const CF_AUTHZ_TUPLES: &str = "authz_tuples";
pub(crate) const CF_AUTHZ_RECEIPTS: &str = "authz_receipts";
pub(crate) const VERSION_HIGH_WATERMARK_KEY: &[u8] = b"version_high_watermark";
const LOCAL_INVALIDATION_OFFSET_KEY: &[u8] = b"local_invalidation_offset";
const FORMAT_MARKER_NAME: &str = ".anvil-format";
const FORMAT_MARKER: &[u8] = b"anvil-store-format:0.5\n";
const COLUMN_FAMILIES: &[&str] = &[
    CF_HEADS,
    CF_VERSIONS,
    CF_RECEIPTS,
    CF_POLICIES,
    CF_LOCAL_INVALIDATIONS,
    CF_PROGRAM_ARTIFACTS,
    CF_PROGRAM_COMMITS,
    CF_OUTBOX,
    CF_METADATA,
    CF_AUTHZ_TENANTS,
    CF_AUTHZ_SCHEMAS,
    CF_AUTHZ_BINDINGS,
    CF_AUTHZ_TUPLES,
    CF_AUTHZ_RECEIPTS,
];

#[derive(Clone, Debug)]
pub struct StoreOptions {
    pub root: PathBuf,
    pub node_id: u16,
    pub sync_writes: bool,
}

impl StoreOptions {
    pub fn new(root: impl AsRef<Path>, node_id: u16) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            node_id,
            sync_writes: true,
        }
    }
}

#[derive(Clone)]
pub struct Store {
    pub(crate) db: Arc<DB>,
    pub(crate) blobs: BlobStore,
    pub(crate) clock: Arc<VersionClock>,
    /// Serialises ordinary one-path mutations while their RocksDB batch is
    /// evaluated and committed. This is deliberately separate from the
    /// singleton executor's lock table: ordinary object calls never
    /// participate in an atomic program.
    pub(crate) ordinary_locks: LocalLockManager,
    /// Exact-path locks owned only by the nominated atomic-program executor.
    pub(crate) program_locks: LocalLockManager,
    pub(crate) commit_lock: Arc<tokio::sync::Mutex<()>>,
    pub(crate) policy_gate: Arc<tokio::sync::RwLock<()>>,
    pub(crate) authz_write_lock: Arc<std::sync::Mutex<()>>,
    pub(crate) program_artifacts: Arc<dyn PreparedArtifactRepository>,
    pub(crate) sync_writes: bool,
    #[cfg(test)]
    policy_lookup_count: Arc<std::sync::atomic::AtomicUsize>,
}

/// Immutable version descriptors selected for one batch read.
///
/// Selection resolves current heads from one local RocksDB snapshot. The
/// descriptors can therefore be measured before any referenced blob is read,
/// then materialised without observing a later head movement.
#[derive(Debug)]
pub struct BatchGetSelection {
    entries: Vec<(ObjectKey, Result<Option<Version>, MutationError>)>,
}

impl BatchGetSelection {
    /// Sum of the declared lengths of structurally valid, present payloads.
    ///
    /// Missing versions, tombstones, selection errors and malformed version
    /// descriptors contribute no bytes. Those retain their existing per-item
    /// outcomes when the selection is materialised.
    pub fn declared_present_payload_bytes(&self) -> u64 {
        self.entries.iter().fold(0_u64, |total, (_, selected)| {
            let length = match selected {
                Ok(Some(version)) => match (&version.inline, &version.blob, version.deleted) {
                    (Some(inline), None, false) if inline.is_valid() => inline.length,
                    (None, Some(blob), false) => blob.length,
                    (None, None, true) => 0,
                    _ => 0,
                },
                Ok(None) | Err(_) => 0,
            };
            total.saturating_add(length)
        })
    }
}

impl std::fmt::Debug for Store {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Store").finish_non_exhaustive()
    }
}

#[derive(Clone)]
enum PreparedOperation {
    Put {
        request: PutRequest,
        payload: PreparedPayload,
        fingerprint: [u8; 32],
    },
    Publish {
        request: PublishRequest,
        fingerprint: [u8; 32],
    },
    Delete {
        request: DeleteRequest,
        fingerprint: [u8; 32],
    },
}

#[derive(Clone)]
enum PreparedPayload {
    Inline(InlinePayload),
    Blob(BlobRef),
}

impl PreparedPayload {
    fn reference(&self) -> BlobRef {
        match self {
            Self::Inline(payload) => BlobRef {
                hash: payload.hash,
                length: payload.length,
            },
            Self::Blob(reference) => reference.clone(),
        }
    }
}

impl PreparedOperation {
    fn key(&self) -> &ObjectKey {
        match self {
            Self::Put { request, .. } => &request.key,
            Self::Publish { request, .. } => &request.key,
            Self::Delete { request, .. } => &request.key,
        }
    }

    fn command_id(&self) -> Option<&str> {
        match self {
            Self::Put { request, .. } => request.command_id.as_deref(),
            Self::Publish { request, .. } => request.command_id.as_deref(),
            Self::Delete { request, .. } => request.command_id.as_deref(),
        }
    }

    fn precondition(&self) -> Precondition {
        match self {
            Self::Put { request, .. } => request.precondition,
            Self::Publish { request, .. } => request.precondition,
            Self::Delete { request, .. } => request.precondition,
        }
    }

    fn fingerprint(&self) -> [u8; 32] {
        match self {
            Self::Put { fingerprint, .. }
            | Self::Publish { fingerprint, .. }
            | Self::Delete { fingerprint, .. } => *fingerprint,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredReceipt {
    fingerprint: [u8; 32],
    version: VersionId,
    deleted: bool,
}

impl Store {
    pub async fn open(options: StoreOptions) -> Result<Self> {
        ensure_format_marker(&options.root).await?;
        let metadata_path = options.root.join("metadata");
        let mut db_options = Options::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        let descriptors = COLUMN_FAMILIES
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect::<Vec<_>>();
        let db = DB::open_cf_descriptors(&db_options, &metadata_path, descriptors)
            .with_context(|| format!("open Anvil metadata at {}", metadata_path.display()))?;
        let metadata_cf = db
            .cf_handle(CF_METADATA)
            .context("missing metadata column family")?;
        let high_watermark = db
            .get_cf(metadata_cf, VERSION_HIGH_WATERMARK_KEY)?
            .map(|encoded| serde_json::from_slice::<VersionId>(&encoded))
            .transpose()?;
        let db = Arc::new(db);
        let program_artifacts = Arc::new(crate::program::LocalPreparedArtifactRepository::new(
            db.clone(),
            options.node_id,
            options.sync_writes,
        ));
        Ok(Self {
            db,
            blobs: BlobStore::open(options.root.join("blobs")).await?,
            clock: Arc::new(VersionClock::with_high_watermark(
                options.node_id,
                high_watermark,
            )?),
            ordinary_locks: LocalLockManager::default(),
            program_locks: LocalLockManager::default(),
            commit_lock: Arc::new(tokio::sync::Mutex::new(())),
            policy_gate: Arc::new(tokio::sync::RwLock::new(())),
            authz_write_lock: Arc::new(std::sync::Mutex::new(())),
            program_artifacts,
            sync_writes: options.sync_writes,
            #[cfg(test)]
            policy_lookup_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }

    /// Replaces the executor-local prepared-artifact repository with the
    /// configured remote durability provider. Ordinary object storage is
    /// unaffected.
    pub fn with_prepared_artifact_repository(
        mut self,
        repository: Arc<dyn PreparedArtifactRepository>,
    ) -> Self {
        self.program_artifacts = repository;
        self
    }

    /// Adds create-once namespaces. Existing immutable policy may only become
    /// stricter; removing a prefix would silently invalidate a durability
    /// promise made to existing callers.
    pub async fn set_bucket_policy(
        &self,
        tenant: &str,
        bucket: &str,
        policy: BucketPolicy,
    ) -> Result<(), MutationError> {
        policy.validate()?;
        let _policy_guard = self.policy_gate.write().await;
        let key = bucket_key(tenant, bucket)?;
        let policy_path = ObjectPath::new(tenant, bucket, "_anvil/policy")
            .map_err(MutationError::InvalidPolicy)?;
        let _guard = self.ordinary_locks.acquire(&[policy_path]).await;
        if let Some(existing) = self.bucket_policy_by_key(&key)? {
            let requested_create_once = policy.create_once_prefixes.iter().collect::<BTreeSet<_>>();
            if existing
                .create_once_prefixes
                .iter()
                .any(|prefix| !requested_create_once.contains(prefix))
            {
                return Err(MutationError::InvalidPolicy(
                    "create-once prefixes cannot be removed".into(),
                ));
            }
            let requested_program_only =
                policy.program_only_prefixes.iter().collect::<BTreeSet<_>>();
            if existing
                .program_only_prefixes
                .iter()
                .any(|prefix| !requested_program_only.contains(prefix))
            {
                return Err(MutationError::InvalidPolicy(
                    "program-only prefixes cannot be removed".into(),
                ));
            }
        }
        let value = serde_json::to_vec(&policy).map_err(storage_error)?;
        let mut write_options = WriteOptions::default();
        write_options.set_sync(self.sync_writes);
        self.db
            .put_cf_opt(self.cf(CF_POLICIES)?, key, value, &write_options)
            .map_err(storage_error)
    }

    pub fn bucket_policy(&self, tenant: &str, bucket: &str) -> Result<BucketPolicy, MutationError> {
        let key = bucket_key(tenant, bucket)?;
        Ok(self.bucket_policy_by_key(&key)?.unwrap_or_default())
    }

    pub fn head(&self, key: &ObjectKey) -> Result<Option<Head>, MutationError> {
        self.read_json(CF_HEADS, &key.encode())
    }

    /// Returns the last durable offset in this store's local invalidation
    /// journal. Zero means that no ordinary head mutation has been appended.
    pub fn local_invalidation_offset(&self) -> Result<u64, MutationError> {
        let Some(encoded) = self
            .db
            .get_cf(self.cf(CF_METADATA)?, LOCAL_INVALIDATION_OFFSET_KEY)
            .map_err(storage_error)?
        else {
            return Ok(0);
        };
        decode_offset(&encoded)
    }

    /// Reads one exact source-local invalidation offset.
    pub fn read_local_invalidation(
        &self,
        offset: u64,
    ) -> Result<Option<LocalInvalidation>, MutationError> {
        if offset == 0 {
            return Ok(None);
        }
        let Some(encoded) = self
            .db
            .get_cf(self.cf(CF_LOCAL_INVALIDATIONS)?, invalidation_key(offset))
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        let invalidation =
            serde_json::from_slice::<LocalInvalidation>(&encoded).map_err(storage_error)?;
        if invalidation.offset != offset {
            return Err(MutationError::Storage(
                "local invalidation key does not match its stored offset".into(),
            ));
        }
        Ok(Some(invalidation))
    }

    /// Scans source-local invalidations after one offset in ascending local
    /// order. The result is capped independently of the requested limit.
    pub fn scan_local_invalidations(
        &self,
        after_offset: u64,
        limit: usize,
    ) -> Result<Vec<LocalInvalidation>, MutationError> {
        let limit = limit.min(MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
        let Some(first_offset) = after_offset.checked_add(1).filter(|_| limit > 0) else {
            return Ok(Vec::new());
        };
        let first_key = invalidation_key(first_offset);
        let iterator = self.db.iterator_cf(
            self.cf(CF_LOCAL_INVALIDATIONS)?,
            IteratorMode::From(&first_key, Direction::Forward),
        );
        let mut invalidations = Vec::with_capacity(limit);
        for entry in iterator.take(limit) {
            let (key, encoded) = entry.map_err(storage_error)?;
            let stored_offset = offset_from_key(&key).ok_or_else(|| {
                MutationError::Storage("local invalidation key is malformed".into())
            })?;
            let invalidation =
                serde_json::from_slice::<LocalInvalidation>(&encoded).map_err(storage_error)?;
            if invalidation.offset != stored_offset {
                return Err(MutationError::Storage(
                    "local invalidation key does not match its stored offset".into(),
                ));
            }
            invalidations.push(invalidation);
        }
        Ok(invalidations)
    }

    pub async fn get(&self, key: &ObjectKey) -> Result<Option<Object>, MutationError> {
        let Some(head) = self.head(key)? else {
            return Ok(None);
        };
        if head.deleted {
            return Ok(None);
        }
        self.get_version(key, head.version).await
    }

    pub async fn get_version(
        &self,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Option<Object>, MutationError> {
        let Some(version) =
            self.read_json::<Version>(CF_VERSIONS, &version_key(key, version_id))?
        else {
            return Ok(None);
        };
        let bytes = match (&version.inline, &version.blob, version.deleted) {
            (Some(inline), None, false) if inline.is_valid() => inline.bytes.clone(),
            (None, Some(blob), false) => self.blobs.get(blob).await.map_err(storage_error)?,
            (None, None, true) => Vec::new(),
            _ => {
                return Err(MutationError::Storage(
                    "version has an invalid payload shape".into(),
                ));
            }
        };
        Ok(Some(Object {
            key: key.clone(),
            version,
            bytes,
        }))
    }

    pub fn version_metadata(
        &self,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Option<Version>, MutationError> {
        self.read_json(CF_VERSIONS, &version_key(key, version_id))
    }

    /// Resolves every requested head and immutable version descriptor from one
    /// local RocksDB snapshot without reading referenced blob payloads.
    pub fn select_batch_get(
        &self,
        requests: &[(ObjectKey, Option<VersionId>)],
    ) -> BatchGetSelection {
        let entries = {
            let snapshot = self.db.snapshot();
            requests
                .iter()
                .map(|(key, requested_version)| {
                    let selected = (|| {
                        let version_id = match requested_version {
                            Some(version) => Some(*version),
                            None => snapshot
                                .get_cf(self.cf(CF_HEADS)?, key.encode())
                                .map_err(storage_error)?
                                .map(|bytes| {
                                    serde_json::from_slice::<Head>(&bytes).map_err(storage_error)
                                })
                                .transpose()?
                                .map(|head| head.version),
                        };
                        let Some(version_id) = version_id else {
                            return Ok(None);
                        };
                        snapshot
                            .get_cf(self.cf(CF_VERSIONS)?, version_key(key, version_id))
                            .map_err(storage_error)?
                            .map(|bytes| {
                                serde_json::from_slice::<Version>(&bytes).map_err(storage_error)
                            })
                            .transpose()
                    })();
                    (key.clone(), selected)
                })
                .collect::<Vec<_>>()
        };
        BatchGetSelection { entries }
    }

    /// Reads payloads for descriptors previously selected by
    /// [`Store::select_batch_get`]. Version descriptors and blobs are
    /// immutable, so this does not need to retain the RocksDB snapshot.
    pub async fn read_batch_get_selection(
        &self,
        selection: BatchGetSelection,
    ) -> Vec<Result<Option<Object>, MutationError>> {
        let mut outcomes = Vec::with_capacity(selection.entries.len());
        for (key, version) in selection.entries {
            let outcome = match version {
                Ok(Some(version)) => match (&version.inline, &version.blob, version.deleted) {
                    (Some(inline), None, false) if inline.is_valid() => Ok(Some(Object {
                        key,
                        bytes: inline.bytes.clone(),
                        version,
                    })),
                    (None, Some(blob), false) => self
                        .blobs
                        .get(blob)
                        .await
                        .map(|bytes| {
                            Some(Object {
                                key,
                                version,
                                bytes,
                            })
                        })
                        .map_err(storage_error),
                    (None, None, true) => Ok(Some(Object {
                        key,
                        version,
                        bytes: Vec::new(),
                    })),
                    _ => Err(MutationError::Storage(
                        "version has an invalid payload shape".into(),
                    )),
                },
                Ok(None) => Ok(None),
                Err(error) => Err(error),
            };
            outcomes.push(outcome);
        }
        outcomes
    }

    /// Resolves one snapshot and materialises its selected payloads.
    pub async fn batch_get(
        &self,
        requests: &[(ObjectKey, Option<VersionId>)],
    ) -> Vec<Result<Option<Object>, MutationError>> {
        let selection = self.select_batch_get(requests);
        self.read_batch_get_selection(selection).await
    }

    pub async fn stage_blob(&self, bytes: &[u8]) -> Result<BlobRef, MutationError> {
        self.blobs.put(bytes).await.map_err(storage_error)
    }

    pub fn lock_manager(&self) -> LocalLockManager {
        self.program_locks.clone()
    }

    pub async fn begin_blob_upload(&self) -> Result<crate::BlobUpload, MutationError> {
        self.blobs.begin_upload().await.map_err(storage_error)
    }

    pub async fn open_blob(&self, reference: &BlobRef) -> Result<BlobReader, MutationError> {
        self.blobs
            .open_verified(reference)
            .await
            .map_err(storage_error)
    }

    pub async fn put(&self, request: PutRequest) -> Result<MutationReceipt, MutationError> {
        self.bulk_write(vec![BatchOperation::Put(request)])
            .await
            .pop()
            .expect("one operation has one outcome")
            .result
    }

    pub async fn publish(&self, request: PublishRequest) -> Result<MutationReceipt, MutationError> {
        self.bulk_write(vec![BatchOperation::Publish(request)])
            .await
            .pop()
            .expect("one operation has one outcome")
            .result
    }

    pub async fn delete(&self, request: DeleteRequest) -> Result<MutationReceipt, MutationError> {
        self.bulk_write(vec![BatchOperation::Delete(request)])
            .await
            .pop()
            .expect("one operation has one outcome")
            .result
    }

    /// Evaluates independent operations in request order and persists all
    /// successful outcomes with one physical RocksDB write. A failed
    /// precondition is an item result, not a reason to retry the whole bulk.
    pub async fn bulk_write(&self, operations: Vec<BatchOperation>) -> Vec<BatchOutcome> {
        let _policy_guard = self.policy_gate.read().await;
        let mut prepared = Vec::with_capacity(operations.len());
        let mut early = BTreeMap::new();
        for (index, operation) in operations.into_iter().enumerate() {
            match self.prepare(operation).await {
                Ok(operation) => prepared.push((index, operation)),
                Err(error) => {
                    early.insert(index, error);
                }
            }
        }

        let _guards = self
            .ordinary_locks
            .acquire(
                &prepared
                    .iter()
                    .map(|(_, operation)| object_path(operation.key()))
                    .collect::<Vec<_>>(),
            )
            .await;
        let _commit_guard = self.commit_lock.lock().await;
        let mut batch = WriteBatch::default();
        let mut pending_heads = BTreeMap::<Vec<u8>, Head>::new();
        let mut pending_receipts = BTreeMap::<Vec<u8>, StoredReceipt>::new();
        let mut policy_cache = BTreeMap::<Vec<u8>, Result<BucketPolicy, MutationError>>::new();
        let mut results = BTreeMap::<usize, Result<MutationReceipt, MutationError>>::new();
        let mut batch_high_watermark = None;
        let mut pending_invalidations = Vec::new();
        for (index, operation) in prepared {
            let outcome = self.evaluate_operation(
                &operation,
                &mut batch,
                &mut pending_heads,
                &mut pending_receipts,
                &mut policy_cache,
            );
            if let Ok(receipt) = &outcome
                && !receipt.replayed
            {
                batch_high_watermark = Some(
                    batch_high_watermark.map_or(receipt.version, |current: VersionId| {
                        current.max(receipt.version)
                    }),
                );
                pending_invalidations.push((
                    operation.key().clone(),
                    receipt.version,
                    receipt.deleted,
                ));
            }
            results.insert(index, outcome);
        }

        let persistence = (|| {
            self.stage_local_invalidations(&mut batch, &pending_invalidations)?;
            if let Some(high_watermark) = batch_high_watermark {
                batch.put_cf(
                    self.cf(CF_METADATA)?,
                    VERSION_HIGH_WATERMARK_KEY,
                    serde_json::to_vec(&high_watermark).map_err(storage_error)?,
                );
            }
            if batch.is_empty() {
                return Ok(());
            }
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)
        })();
        if let Err(error) = persistence {
            let message = error.to_string();
            for result in results.values_mut() {
                if result.is_ok() {
                    *result = Err(MutationError::Storage(message.clone()));
                }
            }
        }
        results.extend(early.into_iter().map(|(index, error)| (index, Err(error))));
        results
            .into_iter()
            .map(|(index, result)| BatchOutcome { index, result })
            .collect()
    }

    fn stage_local_invalidations(
        &self,
        batch: &mut WriteBatch,
        changes: &[(ObjectKey, VersionId, bool)],
    ) -> Result<(), MutationError> {
        if changes.is_empty() {
            return Ok(());
        }

        let journal = self.cf(CF_LOCAL_INVALIDATIONS)?;
        let metadata = self.cf(CF_METADATA)?;
        let mut offset = self.local_invalidation_offset()?;
        for (key, version, deleted) in changes {
            offset = offset.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation offset is exhausted".into())
            })?;
            let invalidation = LocalInvalidation::new(offset, key.clone(), *version, *deleted);
            batch.put_cf(
                journal,
                invalidation_key(offset),
                serde_json::to_vec(&invalidation).map_err(storage_error)?,
            );
        }
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_OFFSET_KEY,
            offset.to_be_bytes(),
        );
        Ok(())
    }

    async fn prepare(&self, operation: BatchOperation) -> Result<PreparedOperation, MutationError> {
        match operation {
            BatchOperation::Put(mut request) => {
                validate_command_id(request.command_id.as_deref())?;
                let bytes = std::mem::take(&mut request.bytes);
                let payload = if bytes.len() <= INLINE_PAYLOAD_MAX_BYTES {
                    PreparedPayload::Inline(InlinePayload::new(bytes))
                } else {
                    PreparedPayload::Blob(self.stage_blob(&bytes).await?)
                };
                let fingerprint = put_fingerprint(
                    &request.key,
                    request.precondition,
                    request.content_type.as_deref(),
                    &request.durability_class,
                    &payload.reference(),
                );
                Ok(PreparedOperation::Put {
                    request,
                    payload,
                    fingerprint,
                })
            }
            BatchOperation::Publish(request) => {
                validate_command_id(request.command_id.as_deref())?;
                if !self
                    .blobs
                    .contains(&request.blob)
                    .await
                    .map_err(storage_error)?
                {
                    return Err(MutationError::BlobNotFound);
                }
                let fingerprint = publish_fingerprint(&request);
                Ok(PreparedOperation::Publish {
                    request,
                    fingerprint,
                })
            }
            BatchOperation::Delete(request) => {
                validate_command_id(request.command_id.as_deref())?;
                let fingerprint = delete_fingerprint(&request);
                Ok(PreparedOperation::Delete {
                    request,
                    fingerprint,
                })
            }
        }
    }

    fn evaluate_operation(
        &self,
        operation: &PreparedOperation,
        batch: &mut WriteBatch,
        pending_heads: &mut BTreeMap<Vec<u8>, Head>,
        pending_receipts: &mut BTreeMap<Vec<u8>, StoredReceipt>,
        policy_cache: &mut BTreeMap<Vec<u8>, Result<BucketPolicy, MutationError>>,
    ) -> Result<MutationReceipt, MutationError> {
        let key = operation.key();
        let encoded_key = key.encode();
        let receipt_key = operation
            .command_id()
            .map(|command_id| receipt_key(key, command_id));
        if let Some(receipt_key) = receipt_key.as_ref() {
            let existing = match pending_receipts.get(receipt_key) {
                Some(receipt) => Some(receipt.clone()),
                None => self.read_json(CF_RECEIPTS, receipt_key)?,
            };
            if let Some(existing) = existing {
                if existing.fingerprint != operation.fingerprint() {
                    return Err(MutationError::IdempotencyConflict);
                }
                return Ok(MutationReceipt {
                    command_id: operation.command_id().map(str::to_owned),
                    fingerprint: existing.fingerprint,
                    version: existing.version,
                    deleted: existing.deleted,
                    replayed: true,
                });
            }
        }

        let current = match pending_heads.get(&encoded_key) {
            Some(head) => Some(head.clone()),
            None => self.head(key)?,
        };
        let policy = policy_cache
            .entry(key.bucket_key())
            .or_insert_with(|| self.bucket_policy(key.tenant(), key.bucket()))
            .as_ref()
            .map_err(Clone::clone)?;
        let program_definition = is_program_definition_path(key.path());
        if policy.is_program_only(key.path()) && !program_definition {
            return Err(MutationError::ProgramOnly);
        }
        if program_definition
            && current.is_none()
            && !matches!(operation.precondition(), Precondition::Absent)
        {
            return Err(MutationError::PreconditionFailed { current: None });
        }
        if policy.is_create_once(key.path()) || program_definition {
            if matches!(operation, PreparedOperation::Delete { .. }) {
                return Err(MutationError::Immutable);
            }
            if let Some(current) = current.as_ref() {
                let existing = self
                    .version_metadata(key, current.version)?
                    .ok_or_else(|| {
                        MutationError::Storage("head references a missing version".into())
                    })?;
                let requested_payload = match operation {
                    PreparedOperation::Put { payload, .. } => payload.reference(),
                    PreparedOperation::Publish { request, .. } => request.blob.clone(),
                    PreparedOperation::Delete { .. } => unreachable!(),
                };
                let requested_content_type = match operation {
                    PreparedOperation::Put { request, .. } => request.content_type.as_ref(),
                    PreparedOperation::Publish { request, .. } => request.content_type.as_ref(),
                    PreparedOperation::Delete { .. } => unreachable!(),
                };
                if !current.deleted
                    && version_payload_reference(&existing).as_ref() == Some(&requested_payload)
                    && existing.content_type.as_ref() == requested_content_type
                {
                    let stored = StoredReceipt {
                        fingerprint: operation.fingerprint(),
                        version: current.version,
                        deleted: false,
                    };
                    if let Some(receipt_key) = receipt_key {
                        batch.put_cf(
                            self.cf(CF_RECEIPTS)?,
                            &receipt_key,
                            serde_json::to_vec(&stored).map_err(storage_error)?,
                        );
                        pending_receipts.insert(receipt_key, stored.clone());
                    }
                    return Ok(MutationReceipt {
                        command_id: operation.command_id().map(str::to_owned),
                        fingerprint: stored.fingerprint,
                        version: stored.version,
                        deleted: false,
                        replayed: true,
                    });
                }
                return Err(MutationError::Immutable);
            }
        }
        check_precondition(operation.precondition(), current.as_ref())?;

        let id = self.clock.next().map_err(storage_error)?;
        let deleted = matches!(operation, PreparedOperation::Delete { .. });
        let version = Version {
            id,
            blob: match operation {
                PreparedOperation::Put {
                    payload: PreparedPayload::Blob(blob),
                    ..
                } => Some(blob.clone()),
                PreparedOperation::Publish { request, .. } => Some(request.blob.clone()),
                PreparedOperation::Put {
                    payload: PreparedPayload::Inline(_),
                    ..
                }
                | PreparedOperation::Delete { .. } => None,
            },
            inline: match operation {
                PreparedOperation::Put {
                    payload: PreparedPayload::Inline(payload),
                    ..
                } => Some(payload.clone()),
                PreparedOperation::Put {
                    payload: PreparedPayload::Blob(_),
                    ..
                }
                | PreparedOperation::Publish { .. }
                | PreparedOperation::Delete { .. } => None,
            },
            content_type: match operation {
                PreparedOperation::Put { request, .. } => request.content_type.clone(),
                PreparedOperation::Publish { request, .. } => request.content_type.clone(),
                PreparedOperation::Delete { .. } => None,
            },
            deleted,
            committed_at_unix_millis: now_unix_millis()?,
        };
        let head = Head {
            version: id,
            deleted,
        };
        batch.put_cf(
            self.cf(CF_VERSIONS)?,
            version_key(key, id),
            serde_json::to_vec(&version).map_err(storage_error)?,
        );
        batch.put_cf(
            self.cf(CF_HEADS)?,
            &encoded_key,
            serde_json::to_vec(&head).map_err(storage_error)?,
        );
        pending_heads.insert(encoded_key, head);

        let stored = StoredReceipt {
            fingerprint: operation.fingerprint(),
            version: id,
            deleted,
        };
        if let Some(receipt_key) = receipt_key {
            batch.put_cf(
                self.cf(CF_RECEIPTS)?,
                &receipt_key,
                serde_json::to_vec(&stored).map_err(storage_error)?,
            );
            pending_receipts.insert(receipt_key, stored.clone());
        }
        Ok(MutationReceipt {
            command_id: operation.command_id().map(str::to_owned),
            fingerprint: stored.fingerprint,
            version: id,
            deleted,
            replayed: false,
        })
    }

    fn bucket_policy_by_key(&self, key: &[u8]) -> Result<Option<BucketPolicy>, MutationError> {
        #[cfg(test)]
        self.policy_lookup_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.read_json(CF_POLICIES, key)
    }

    fn read_json<T: for<'de> Deserialize<'de>>(
        &self,
        cf: &'static str,
        key: &[u8],
    ) -> Result<Option<T>, MutationError> {
        self.db
            .get_cf(self.cf(cf)?, key)
            .map_err(storage_error)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(storage_error))
            .transpose()
    }

    pub(crate) fn cf(&self, name: &'static str) -> Result<&rocksdb::ColumnFamily, MutationError> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| MutationError::Storage(format!("missing column family {name}")))
    }
}

pub(crate) fn is_program_definition_path(path: &str) -> bool {
    path.strip_prefix(PROGRAM_DEFINITION_PREFIX)
        .is_some_and(|name| !name.is_empty())
}

async fn ensure_format_marker(root: &Path) -> Result<()> {
    tokio::fs::create_dir_all(root).await?;
    let marker_path = root.join(FORMAT_MARKER_NAME);
    match tokio::fs::read(&marker_path).await {
        Ok(marker) if marker == FORMAT_MARKER => return Ok(()),
        Ok(_) => anyhow::bail!("Anvil data directory has an incompatible format marker"),
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
        Err(_) => {}
    }

    let mut entries = tokio::fs::read_dir(root).await?;
    if entries.next_entry().await?.is_some() {
        anyhow::bail!("non-empty Anvil data directory has no 0.5 format marker");
    }

    match tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&marker_path)
        .await
    {
        Ok(mut marker) => {
            use tokio::io::AsyncWriteExt;
            marker.write_all(FORMAT_MARKER).await?;
            marker.sync_all().await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if tokio::fs::read(&marker_path).await? != FORMAT_MARKER {
                anyhow::bail!("Anvil data directory has an incompatible format marker");
            }
        }
        Err(error) => return Err(error.into()),
    }
    tokio::fs::File::open(root).await?.sync_all().await?;
    Ok(())
}

fn check_precondition(
    precondition: Precondition,
    current: Option<&Head>,
) -> Result<(), MutationError> {
    let matches = match precondition {
        Precondition::Any => true,
        Precondition::Absent => current.is_none() || current.is_some_and(|head| head.deleted),
        Precondition::Version(expected) => current.is_some_and(|head| head.version == expected),
    };
    if matches {
        Ok(())
    } else {
        Err(MutationError::PreconditionFailed {
            current: current.map(|head| head.version),
        })
    }
}

fn bucket_key(tenant: &str, bucket: &str) -> Result<Vec<u8>, MutationError> {
    ObjectKey::new(tenant, bucket, "_policy")
        .map(|key| key.bucket_key())
        .map_err(|error| MutationError::InvalidPolicy(error.to_string()))
}

fn decode_offset(encoded: &[u8]) -> Result<u64, MutationError> {
    let bytes: [u8; size_of::<u64>()] = encoded.try_into().map_err(|_| {
        MutationError::Storage("durable local invalidation offset is malformed".into())
    })?;
    Ok(u64::from_be_bytes(bytes))
}

pub(crate) fn version_key(key: &ObjectKey, version: VersionId) -> Vec<u8> {
    let mut encoded = key.encode();
    encoded.extend_from_slice(&version.0.to_be_bytes());
    encoded
}

pub(crate) fn object_path(key: &ObjectKey) -> ObjectPath {
    ObjectPath::new(key.tenant(), key.bucket(), key.path())
        .expect("validated store key is a valid atomic-program path")
}

fn receipt_key(key: &ObjectKey, command_id: &str) -> Vec<u8> {
    let mut encoded = key.bucket_key();
    encoded.extend_from_slice(&(command_id.len() as u16).to_be_bytes());
    encoded.extend_from_slice(command_id.as_bytes());
    encoded
}

fn validate_command_id(command_id: Option<&str>) -> Result<(), MutationError> {
    if command_id.is_some_and(|value| value.is_empty() || value.len() > 256 || value.contains('\0'))
    {
        Err(MutationError::InvalidCommandId)
    } else {
        Ok(())
    }
}

fn version_payload_reference(version: &Version) -> Option<BlobRef> {
    match (&version.inline, &version.blob, version.deleted) {
        (Some(inline), None, false) if inline.is_valid() => Some(BlobRef {
            hash: inline.hash,
            length: inline.length,
        }),
        (None, Some(blob), false) => Some(blob.clone()),
        _ => None,
    }
}

fn put_fingerprint(
    key: &ObjectKey,
    precondition: Precondition,
    content_type: Option<&str>,
    durability_class: &str,
    blob: &BlobRef,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.put.v1");
    hasher.update(&key.encode());
    hash_precondition(&mut hasher, precondition);
    hash_optional_string(&mut hasher, content_type);
    hash_string(&mut hasher, durability_class);
    hasher.update(&blob.hash);
    hasher.update(&blob.length.to_be_bytes());
    *hasher.finalize().as_bytes()
}

fn delete_fingerprint(request: &DeleteRequest) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.delete.v1");
    hasher.update(&request.key.encode());
    hash_precondition(&mut hasher, request.precondition);
    hash_string(&mut hasher, &request.durability_class);
    *hasher.finalize().as_bytes()
}

fn publish_fingerprint(request: &PublishRequest) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.publish.v1");
    hasher.update(&request.key.encode());
    hash_precondition(&mut hasher, request.precondition);
    hash_optional_string(&mut hasher, request.content_type.as_deref());
    hash_string(&mut hasher, &request.durability_class);
    hasher.update(&request.blob.hash);
    hasher.update(&request.blob.length.to_be_bytes());
    *hasher.finalize().as_bytes()
}

fn hash_precondition(hasher: &mut blake3::Hasher, precondition: Precondition) {
    match precondition {
        Precondition::Any => hasher.update(&[0]),
        Precondition::Absent => hasher.update(&[1]),
        Precondition::Version(version) => {
            hasher.update(&[2]);
            hasher.update(&version.0.to_be_bytes())
        }
    };
}

fn hash_optional_string(hasher: &mut blake3::Hasher, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hash_string(hasher, value);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hash_string(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

pub(crate) fn now_unix_millis() -> Result<u64, MutationError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .map_err(storage_error)
}

fn storage_error(error: impl std::fmt::Display) -> MutationError {
    MutationError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use rocksdb::WriteBatchIteratorCf;
    use tempfile::TempDir;

    use super::*;

    async fn store() -> (TempDir, Store) {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        (temporary, store)
    }

    fn key(path: &str) -> ObjectKey {
        ObjectKey::new("tenant", "bucket", path).unwrap()
    }

    fn put(path: &str, bytes: &[u8], precondition: Precondition, command: &str) -> PutRequest {
        PutRequest {
            key: key(path),
            bytes: bytes.to_vec(),
            content_type: Some("application/octet-stream".into()),
            precondition,
            command_id: Some(command.into()),
            durability_class: "test-default".into(),
        }
    }

    #[derive(Default)]
    struct WalOperationCounter {
        puts: usize,
        deletes: usize,
        merges: usize,
        high_watermark_puts: usize,
        invalidation_offset_puts: usize,
    }

    impl WriteBatchIteratorCf for WalOperationCounter {
        fn put_cf(&mut self, _cf_id: u32, key: &[u8], _value: &[u8]) {
            self.puts += 1;
            if key == VERSION_HIGH_WATERMARK_KEY {
                self.high_watermark_puts += 1;
            }
            if key == LOCAL_INVALIDATION_OFFSET_KEY {
                self.invalidation_offset_puts += 1;
            }
        }

        fn delete_cf(&mut self, _cf_id: u32, _key: &[u8]) {
            self.deletes += 1;
        }

        fn merge_cf(&mut self, _cf_id: u32, _key: &[u8], _value: &[u8]) {
            self.merges += 1;
        }
    }

    #[tokio::test]
    async fn put_is_versioned_and_exact_cas_moves_the_head() {
        let (_temporary, store) = store().await;
        let first = store
            .put(put("a", b"one", Precondition::Absent, "one"))
            .await
            .unwrap();
        let second = store
            .put(put(
                "a",
                b"two",
                Precondition::Version(first.version),
                "two",
            ))
            .await
            .unwrap();
        assert!(second.version > first.version);
        assert_eq!(store.get(&key("a")).await.unwrap().unwrap().bytes, b"two");
        assert_eq!(
            store
                .get_version(&key("a"), first.version)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            b"one"
        );
    }

    #[tokio::test]
    async fn idempotency_is_checked_before_the_precondition() {
        let (_temporary, store) = store().await;
        let request = put("a", b"one", Precondition::Absent, "same-command");
        let first = store.put(request.clone()).await.unwrap();
        let replay = store.put(request).await.unwrap();
        assert!(!first.replayed);
        assert!(replay.replayed);
        assert_eq!(replay.version, first.version);
        assert_eq!(replay.fingerprint, first.fingerprint);
        let conflict = store
            .put(put("a", b"different", Precondition::Absent, "same-command"))
            .await
            .unwrap_err();
        assert_eq!(conflict, MutationError::IdempotencyConflict);
    }

    #[tokio::test]
    async fn durability_class_is_part_of_every_write_fingerprint() {
        let (_temporary, store) = store().await;

        let put_request = put("put", b"value", Precondition::Absent, "put-command");
        store.put(put_request.clone()).await.unwrap();
        let mut changed_put = put_request;
        changed_put.durability_class = "other".into();
        assert_eq!(
            store.put(changed_put).await.unwrap_err(),
            MutationError::IdempotencyConflict
        );

        let blob = store.stage_blob(b"published").await.unwrap();
        let publish_request = PublishRequest {
            key: key("publish"),
            blob,
            content_type: Some("application/octet-stream".into()),
            precondition: Precondition::Absent,
            command_id: Some("publish-command".into()),
            durability_class: "requested".into(),
        };
        store.publish(publish_request.clone()).await.unwrap();
        let mut changed_publish = publish_request;
        changed_publish.durability_class = "other".into();
        assert_eq!(
            store.publish(changed_publish).await.unwrap_err(),
            MutationError::IdempotencyConflict
        );

        let created = store
            .put(put(
                "delete",
                b"value",
                Precondition::Absent,
                "create-delete-target",
            ))
            .await
            .unwrap();
        let delete_request = DeleteRequest {
            key: key("delete"),
            precondition: Precondition::Version(created.version),
            command_id: Some("delete-command".into()),
            durability_class: "requested".into(),
        };
        store.delete(delete_request.clone()).await.unwrap();
        let mut changed_delete = delete_request;
        changed_delete.durability_class = "other".into();
        assert_eq!(
            store.delete(changed_delete).await.unwrap_err(),
            MutationError::IdempotencyConflict
        );
    }

    #[tokio::test]
    async fn bulk_retry_with_a_different_durability_class_conflicts() {
        let (_temporary, store) = store().await;
        let request = put("bulk", b"value", Precondition::Absent, "bulk-command");
        let first = store
            .bulk_write(vec![BatchOperation::Put(request.clone())])
            .await;
        assert!(first[0].result.is_ok());

        let mut changed = request;
        changed.durability_class = "other".into();
        let replay = store.bulk_write(vec![BatchOperation::Put(changed)]).await;
        assert_eq!(replay[0].result, Err(MutationError::IdempotencyConflict));
    }

    #[tokio::test]
    async fn create_once_policy_applies_to_every_write_surface() {
        let (_temporary, store) = store().await;
        store
            .set_bucket_policy(
                "tenant",
                "bucket",
                BucketPolicy {
                    create_once_prefixes: vec!["ledger".into()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let first = store
            .put(put(
                "ledger/entry-1",
                b"entry",
                Precondition::Absent,
                "entry",
            ))
            .await
            .unwrap();
        let identical = store
            .put(put(
                "ledger/entry-1",
                b"entry",
                Precondition::Absent,
                "same-entry-new-command",
            ))
            .await
            .unwrap();
        assert_eq!(identical.version, first.version);
        assert_eq!(
            store
                .put(put(
                    "ledger/entry-1",
                    b"replacement",
                    Precondition::Version(first.version),
                    "replace",
                ))
                .await
                .unwrap_err(),
            MutationError::Immutable
        );
        assert_eq!(
            store
                .delete(DeleteRequest {
                    key: key("ledger/entry-1"),
                    precondition: Precondition::Version(first.version),
                    command_id: Some("delete".into()),
                    durability_class: "test-default".into(),
                })
                .await
                .unwrap_err(),
            MutationError::Immutable
        );
    }

    #[tokio::test]
    async fn an_exact_tombstone_version_can_be_used_to_recreate() {
        let (_temporary, store) = store().await;
        let first = store
            .put(put("a", b"one", Precondition::Absent, "create"))
            .await
            .unwrap();
        let deleted = store
            .delete(DeleteRequest {
                key: key("a"),
                precondition: Precondition::Version(first.version),
                command_id: Some("delete".into()),
                durability_class: "test-default".into(),
            })
            .await
            .unwrap();
        let recreated = store
            .put(put(
                "a",
                b"two",
                Precondition::Version(deleted.version),
                "recreate",
            ))
            .await
            .unwrap();
        assert!(recreated.version > deleted.version);
    }

    #[tokio::test]
    async fn bulk_returns_per_item_results_and_persists_successes_once() {
        let (_temporary, store) = store().await;
        let outcomes = store
            .bulk_write(vec![
                BatchOperation::Put(put("a", b"a", Precondition::Absent, "a")),
                BatchOperation::Put(put("a", b"bad", Precondition::Absent, "bad")),
                BatchOperation::Put(put("b", b"b", Precondition::Absent, "b")),
            ])
            .await;
        assert!(outcomes[0].result.is_ok());
        assert!(matches!(
            outcomes[1].result,
            Err(MutationError::PreconditionFailed { .. })
        ));
        assert!(outcomes[2].result.is_ok());
        assert_eq!(store.get(&key("a")).await.unwrap().unwrap().bytes, b"a");
        assert_eq!(store.get(&key("b")).await.unwrap().unwrap().bytes, b"b");
    }

    #[tokio::test]
    async fn bulk_wal_contains_one_high_watermark_and_replay_adds_no_write() {
        let (_temporary, store) = store().await;
        let operations = vec![
            BatchOperation::Put(put("a", b"a", Precondition::Absent, "a")),
            BatchOperation::Put(put("b", b"b", Precondition::Absent, "b")),
            BatchOperation::Put(put("c", b"c", Precondition::Absent, "c")),
        ];
        let before = store.db.latest_sequence_number();
        let outcomes = store.bulk_write(operations.clone()).await;
        assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));

        let updates = store
            .db
            .get_updates_since(before)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(updates.len(), 1);
        let mut counter = WalOperationCounter::default();
        updates[0].1.iterate_cf(&mut counter);
        // Three versions, heads, receipts and invalidations, plus one version
        // watermark and one invalidation offset. Both cursors are persisted
        // once for the physical batch rather than once per logical mutation.
        assert_eq!(counter.puts, 14);
        assert_eq!(counter.high_watermark_puts, 1);
        assert_eq!(counter.invalidation_offset_puts, 1);
        assert_eq!(counter.deletes, 0);
        assert_eq!(counter.merges, 0);

        let expected_high_watermark = outcomes
            .iter()
            .map(|outcome| outcome.result.as_ref().unwrap().version)
            .max()
            .unwrap();
        assert_eq!(
            store
                .read_json::<VersionId>(CF_METADATA, VERSION_HIGH_WATERMARK_KEY)
                .unwrap(),
            Some(expected_high_watermark)
        );

        let sequence_after_first_write = store.db.latest_sequence_number();
        let replay = store.bulk_write(operations).await;
        assert!(replay.iter().all(|outcome| {
            outcome
                .result
                .as_ref()
                .is_ok_and(|receipt| receipt.replayed)
        }));
        assert_eq!(
            store.db.latest_sequence_number(),
            sequence_after_first_write
        );
    }

    #[tokio::test]
    async fn prepared_put_retains_only_one_payload_copy() {
        let (_temporary, store) = store().await;
        let inline_bytes = vec![7_u8; INLINE_PAYLOAD_MAX_BYTES];
        let inline = store
            .prepare(BatchOperation::Put(put(
                "inline",
                &inline_bytes,
                Precondition::Absent,
                "inline",
            )))
            .await
            .unwrap();
        match inline {
            PreparedOperation::Put {
                request,
                payload: PreparedPayload::Inline(payload),
                ..
            } => {
                assert!(request.bytes.is_empty());
                assert_eq!(payload.bytes, inline_bytes);
            }
            _ => panic!("small put was not prepared inline"),
        }

        let blob_bytes = vec![9_u8; INLINE_PAYLOAD_MAX_BYTES + 1];
        let blob = store
            .prepare(BatchOperation::Put(put(
                "blob",
                &blob_bytes,
                Precondition::Absent,
                "blob",
            )))
            .await
            .unwrap();
        match blob {
            PreparedOperation::Put {
                request,
                payload: PreparedPayload::Blob(reference),
                ..
            } => {
                assert!(request.bytes.is_empty());
                assert_eq!(reference.length, blob_bytes.len() as u64);
                assert_eq!(store.blobs.get(&reference).await.unwrap(), blob_bytes);
            }
            _ => panic!("large put was not prepared as a blob"),
        }
    }

    #[tokio::test]
    async fn bulk_loads_each_distinct_bucket_policy_once() {
        let (_temporary, store) = store().await;
        let put_in = |bucket: &str, path: &str, command: &str| {
            BatchOperation::Put(PutRequest {
                key: ObjectKey::new("tenant", bucket, path).unwrap(),
                bytes: path.as_bytes().to_vec(),
                content_type: None,
                precondition: Precondition::Absent,
                command_id: Some(command.into()),
                durability_class: "test-default".into(),
            })
        };

        let outcomes = store
            .bulk_write(vec![
                put_in("first", "a", "first-a"),
                put_in("first", "b", "first-b"),
                put_in("second", "a", "second-a"),
                put_in("first", "c", "first-c"),
                put_in("second", "b", "second-b"),
            ])
            .await;

        assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));
        assert_eq!(
            store
                .policy_lookup_count
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    #[tokio::test]
    async fn bulk_inlines_small_values_and_round_trips_mixed_payloads() {
        let (_temporary, store) = store().await;
        let small = vec![7u8; INLINE_PAYLOAD_MAX_BYTES];
        let large = vec![9u8; INLINE_PAYLOAD_MAX_BYTES + 1];
        let outcomes = store
            .bulk_write(vec![
                BatchOperation::Put(put("small", &small, Precondition::Absent, "small")),
                BatchOperation::Put(put("large", &large, Precondition::Absent, "large")),
            ])
            .await;
        assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));
        let small_version = store
            .version_metadata(&key("small"), outcomes[0].result.as_ref().unwrap().version)
            .unwrap()
            .unwrap();
        let large_version = store
            .version_metadata(&key("large"), outcomes[1].result.as_ref().unwrap().version)
            .unwrap()
            .unwrap();
        assert!(small_version.inline.is_some() && small_version.blob.is_none());
        assert!(large_version.inline.is_none() && large_version.blob.is_some());
        assert_eq!(
            store.get(&key("small")).await.unwrap().unwrap().bytes,
            small
        );
        assert_eq!(
            store.get(&key("large")).await.unwrap().unwrap().bytes,
            large
        );
    }

    #[tokio::test]
    async fn batch_get_preserves_tombstone_version_and_never_existed() {
        let (_temporary, store) = store().await;
        let created = store
            .put(put(
                "deleted",
                b"value",
                Precondition::Absent,
                "create-deleted",
            ))
            .await
            .unwrap();
        let deleted = store
            .delete(DeleteRequest {
                key: key("deleted"),
                precondition: Precondition::Version(created.version),
                command_id: Some("delete-current".into()),
                durability_class: "test-default".into(),
            })
            .await
            .unwrap();
        let results = store
            .batch_get(&[(key("deleted"), None), (key("never"), None)])
            .await;
        let tombstone = results[0].as_ref().unwrap().as_ref().unwrap();
        assert!(tombstone.version.deleted);
        assert_eq!(tombstone.version.id, deleted.version);
        assert!(results[1].as_ref().unwrap().is_none());
    }

    #[tokio::test]
    async fn batch_get_selection_is_measured_before_blob_reads_and_survives_head_movement() {
        let (_temporary, store) = store().await;
        let old = store
            .put(put("moving", b"old", Precondition::Absent, "moving-old"))
            .await
            .unwrap();
        let large_payload = vec![9_u8; INLINE_PAYLOAD_MAX_BYTES + 1];
        let large = store
            .put(put("large", &large_payload, Precondition::Absent, "large"))
            .await
            .unwrap();
        let created = store
            .put(put(
                "deleted",
                b"value",
                Precondition::Absent,
                "deleted-create",
            ))
            .await
            .unwrap();
        let deleted = store
            .delete(DeleteRequest {
                key: key("deleted"),
                precondition: Precondition::Version(created.version),
                command_id: Some("deleted-delete".into()),
                durability_class: "test-default".into(),
            })
            .await
            .unwrap();

        let selection = store.select_batch_get(&[
            (key("moving"), None),
            (key("large"), Some(large.version)),
            (key("deleted"), None),
            (key("never"), None),
            (key("moving"), Some(VersionId(u64::MAX))),
        ]);
        assert_eq!(
            selection.declared_present_payload_bytes(),
            (b"old".len() + large_payload.len()) as u64
        );

        let current = store
            .put(put(
                "moving",
                b"new head",
                Precondition::Version(old.version),
                "moving-new",
            ))
            .await
            .unwrap();
        let results = store.read_batch_get_selection(selection).await;

        let selected_old = results[0].as_ref().unwrap().as_ref().unwrap();
        assert_eq!(selected_old.version.id, old.version);
        assert_eq!(selected_old.bytes, b"old");
        assert_eq!(
            results[1].as_ref().unwrap().as_ref().unwrap().bytes,
            large_payload
        );
        let selected_tombstone = results[2].as_ref().unwrap().as_ref().unwrap();
        assert_eq!(selected_tombstone.version.id, deleted.version);
        assert!(selected_tombstone.version.deleted);
        assert!(results[3].as_ref().unwrap().is_none());
        assert!(results[4].as_ref().unwrap().is_none());
        assert_eq!(
            store.head(&key("moving")).unwrap().unwrap().version,
            current.version
        );
    }

    #[tokio::test]
    async fn reserved_program_definitions_require_absent_then_replay_same_content() {
        let (_temporary, store) = store().await;
        let path = "_anvil/programs/import_osv@1";
        store
            .set_bucket_policy(
                "tenant",
                "bucket",
                BucketPolicy {
                    program_only_prefixes: vec!["_anvil".into()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(
            store
                .put(put(path, b"definition", Precondition::Any, "unsafe-define"))
                .await
                .unwrap_err(),
            MutationError::PreconditionFailed { current: None }
        );
        assert!(store.head(&key(path)).unwrap().is_none());

        let first = store
            .put(put(path, b"definition", Precondition::Absent, "define"))
            .await
            .unwrap();

        let replay = store
            .put(put(path, b"definition", Precondition::Any, "define-again"))
            .await
            .unwrap();
        assert_eq!(replay.version, first.version);
        assert_eq!(
            store
                .put(put(
                    path,
                    b"different",
                    Precondition::Version(first.version),
                    "replace-definition",
                ))
                .await
                .unwrap_err(),
            MutationError::Immutable
        );
        assert_eq!(
            store
                .delete(DeleteRequest {
                    key: key(path),
                    precondition: Precondition::Version(first.version),
                    command_id: Some("delete-definition".into()),
                    durability_class: "test-default".into(),
                })
                .await
                .unwrap_err(),
            MutationError::Immutable
        );

        let published_path = "_anvil/programs/published@1";
        let blob = store.stage_blob(b"published-definition").await.unwrap();
        assert_eq!(
            store
                .publish(PublishRequest {
                    key: key(published_path),
                    blob: blob.clone(),
                    content_type: Some("application/json".into()),
                    precondition: Precondition::Any,
                    command_id: Some("unsafe-publish".into()),
                    durability_class: "test-default".into(),
                })
                .await
                .unwrap_err(),
            MutationError::PreconditionFailed { current: None }
        );
        assert!(store.head(&key(published_path)).unwrap().is_none());

        let published = store
            .publish(PublishRequest {
                key: key(published_path),
                blob: blob.clone(),
                content_type: Some("application/json".into()),
                precondition: Precondition::Absent,
                command_id: Some("publish".into()),
                durability_class: "test-default".into(),
            })
            .await
            .unwrap();
        let replay = store
            .publish(PublishRequest {
                key: key(published_path),
                blob,
                content_type: Some("application/json".into()),
                precondition: Precondition::Any,
                command_id: Some("publish-again".into()),
                durability_class: "test-default".into(),
            })
            .await
            .unwrap();
        assert_eq!(replay.version, published.version);
    }

    #[tokio::test]
    async fn program_only_policy_blocks_every_direct_write_kind() {
        let (_temporary, store) = store().await;
        store
            .set_bucket_policy(
                "tenant",
                "bucket",
                BucketPolicy {
                    program_only_prefixes: vec!["managed".into()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .put(put(
                    "managed/a",
                    b"value",
                    Precondition::Absent,
                    "managed-put"
                ))
                .await
                .unwrap_err(),
            MutationError::ProgramOnly
        );
        let blob = store.stage_blob(b"value").await.unwrap();
        assert_eq!(
            store
                .publish(PublishRequest {
                    key: key("managed/a"),
                    blob,
                    content_type: None,
                    precondition: Precondition::Absent,
                    command_id: Some("managed-publish".into()),
                    durability_class: "test-default".into(),
                })
                .await
                .unwrap_err(),
            MutationError::ProgramOnly
        );
        assert_eq!(
            store
                .delete(DeleteRequest {
                    key: key("managed/a"),
                    precondition: Precondition::Any,
                    command_id: Some("managed-delete".into()),
                    durability_class: "test-default".into(),
                })
                .await
                .unwrap_err(),
            MutationError::ProgramOnly
        );
        assert!(
            store
                .put(put("managed-other", b"ok", Precondition::Absent, "outside"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn reopen_seeds_version_clock_above_persisted_high_watermark() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let first = store
            .put(put("first", b"one", Precondition::Absent, "first-version"))
            .await
            .unwrap();
        let forced = VersionId(first.version.0 + (1 << 22));
        store
            .db
            .put_cf(
                store.cf(CF_METADATA).unwrap(),
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&forced).unwrap(),
            )
            .unwrap();
        drop(store);

        let reopened = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let next = reopened
            .put(put(
                "second",
                b"two",
                Precondition::Absent,
                "second-version",
            ))
            .await
            .unwrap();
        assert!(next.version > forced);
    }

    #[tokio::test]
    async fn format_marker_rejects_legacy_or_mismatched_directories_without_deleting_them() {
        let legacy = tempfile::tempdir().unwrap();
        tokio::fs::write(legacy.path().join("old-data"), b"keep")
            .await
            .unwrap();
        assert!(
            Store::open(StoreOptions::new(legacy.path(), 1))
                .await
                .is_err()
        );
        assert_eq!(
            tokio::fs::read(legacy.path().join("old-data"))
                .await
                .unwrap(),
            b"keep"
        );

        let mismatched = tempfile::tempdir().unwrap();
        tokio::fs::write(
            mismatched.path().join(FORMAT_MARKER_NAME),
            b"anvil-store-format:0.4\n",
        )
        .await
        .unwrap();
        assert!(
            Store::open(StoreOptions::new(mismatched.path(), 1))
                .await
                .is_err()
        );
    }
}
