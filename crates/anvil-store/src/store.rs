use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anvil_atomic_program::{LocalLockManager, ObjectPath};
use anyhow::{Context, Result};
use rocksdb::{
    ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};

use crate::key::{
    BucketId, BucketIdentity, STORAGE_KEY_FORMAT_VERSION, TenantId, bucket_name_key,
    contains_reserved_anvil_segment, decode_identity_value, tenant_name_key,
};
use crate::watch::{
    InvalidationStateHint, LOCAL_INVALIDATION_BYTES_KEY, LOCAL_INVALIDATION_COUNT_KEY,
    LOCAL_INVALIDATION_EPOCH_KEY, LOCAL_INVALIDATION_FLOOR_KEY, LOCAL_INVALIDATION_OFFSET_KEY,
    LOCAL_INVALIDATION_TOKEN_KEY, LocalChange, LocalInvalidation,
    MAX_LOCAL_INVALIDATION_SCAN_RECORDS, ObjectHeadChangeKind, SourceId, StoredLocalChange,
    WatchCursor, WatchError, WatchJournalStatus, WatchPage, WatchRetention, WatchScope, WatchStart,
    decode_local_change, decode_resume_token, encode_local_change, encode_resume_token,
    invalidation_key, invalidation_record_bytes, offset_from_key,
};
use crate::{
    AWAITING_PUBLISH, BatchOperation, BatchOutcome, BlobReader, BlobRef, BlobReferenceState,
    BlobStore, BucketPolicy, DeleteRequest, DeleteRetainedVersionOutcome, Durability, Head,
    MutationError, MutationReceipt, Object, ObjectKey, ObjectVersioning, Precondition,
    PublishRequest, PutMode, PutRequest, ReferenceDelta, SMALL_BLOB_MAX_BYTES, Version,
    VersionClock, VersionId,
};

const PROGRAM_DEFINITION_PREFIX: &str = "_anvil/programs/";

pub(crate) const CF_HEADS: &str = "heads";
pub(crate) const CF_VERSIONS: &str = "versions";
pub(crate) const CF_BLOB_REFERENCES: &str = "blob_references";
pub(crate) const CF_SMALL_BLOBS: &str = "small_blobs";
pub(crate) const CF_BUCKET_OPTIONS: &str = "bucket_options";
pub(crate) const CF_NAMES: &str = "names";
const CF_RECEIPTS: &str = "receipts";
const CF_POLICIES: &str = "policies";
const CF_LOCAL_INVALIDATIONS: &str = "local_invalidations";
pub(crate) const CF_METADATA: &str = "metadata";
pub(crate) const CF_AUTHZ_TENANTS: &str = "authz_tenants";
pub(crate) const CF_AUTHZ_SCHEMAS: &str = "authz_schemas";
pub(crate) const CF_AUTHZ_BINDINGS: &str = "authz_bindings";
pub(crate) const CF_AUTHZ_TUPLES: &str = "authz_tuples";
pub(crate) const CF_AUTHZ_RECEIPTS: &str = "authz_receipts";
pub(crate) const CF_CREDENTIALS: &str = "credentials";
pub(crate) const VERSION_HIGH_WATERMARK_KEY: &[u8] = b"version_high_watermark";
const MUTATION_RECEIPT_COUNT_KEY: &[u8] = b"mutation_receipt_count";
const MUTATION_RECEIPT_BYTES_KEY: &[u8] = b"mutation_receipt_bytes";
const RECEIPT_RECORD_PREFIX: u8 = 0;
const RECEIPT_EXPIRY_PREFIX: u8 = 1;

pub const DEFAULT_MUTATION_RECEIPT_RETENTION_SECONDS: u64 = 24 * 60 * 60;
pub const DEFAULT_MUTATION_RECEIPT_MAX_ENTRIES: u64 = 2_000_000;
pub const DEFAULT_MUTATION_RECEIPT_MAX_BYTES: u64 = 512 * 1024 * 1024;
pub const DEFAULT_AWAITING_PUBLISH_TTL_SECONDS: u64 = 24 * 60 * 60;
pub const MAX_LIST_OBJECTS: usize = 1_000;
pub const MAX_LIST_OBJECT_VERSIONS: usize = 1_000;
const FORMAT_MARKER_NAME: &str = ".anvil-format";
const FORMAT_MARKER: &[u8] = b"anvil-store-format:0.5\n";
pub(crate) const COLUMN_FAMILIES: &[&str] = &[
    CF_HEADS,
    CF_VERSIONS,
    CF_BLOB_REFERENCES,
    CF_SMALL_BLOBS,
    CF_BUCKET_OPTIONS,
    CF_NAMES,
    CF_RECEIPTS,
    CF_POLICIES,
    CF_LOCAL_INVALIDATIONS,
    CF_METADATA,
    CF_AUTHZ_TENANTS,
    CF_AUTHZ_SCHEMAS,
    CF_AUTHZ_BINDINGS,
    CF_AUTHZ_TUPLES,
    CF_AUTHZ_RECEIPTS,
    CF_CREDENTIALS,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutationReceiptRetention {
    pub retention_seconds: u64,
    pub max_entries: u64,
    pub max_bytes: u64,
}

impl MutationReceiptRetention {
    pub fn new(
        retention_seconds: u64,
        max_entries: u64,
        max_bytes: u64,
    ) -> Result<Self, MutationError> {
        if retention_seconds == 0 || max_entries == 0 || max_bytes == 0 {
            return Err(MutationError::Storage(
                "mutation receipt retention values must be non-zero".into(),
            ));
        }
        retention_seconds.checked_mul(1_000).ok_or_else(|| {
            MutationError::Storage("mutation receipt retention duration is too large".into())
        })?;
        Ok(Self {
            retention_seconds,
            max_entries,
            max_bytes,
        })
    }

    fn retention_millis(self) -> u64 {
        self.retention_seconds * 1_000
    }
}

impl Default for MutationReceiptRetention {
    fn default() -> Self {
        Self {
            retention_seconds: DEFAULT_MUTATION_RECEIPT_RETENTION_SECONDS,
            max_entries: DEFAULT_MUTATION_RECEIPT_MAX_ENTRIES,
            max_bytes: DEFAULT_MUTATION_RECEIPT_MAX_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MutationReceiptStatus {
    entries: u64,
    bytes: u64,
}

#[derive(Clone, Debug)]
pub(crate) enum PendingLocalChange {
    ObjectHead {
        identity: BucketIdentity,
        exact_path: String,
        path_version: VersionId,
        deleted: bool,
        reference_deltas: Vec<ReferenceDelta>,
    },
    RetainedVersionDeleted {
        identity: BucketIdentity,
        exact_path: String,
        deleted_version: VersionId,
        resulting_head_version: Option<VersionId>,
        reference_deltas: Vec<ReferenceDelta>,
    },
}

#[derive(Clone, Debug)]
pub struct StoreOptions {
    pub root: PathBuf,
    pub node_id: u16,
    pub sync_writes: bool,
    pub watch_retention: WatchRetention,
    pub mutation_receipt_retention: MutationReceiptRetention,
    /// Blob inactivity grace. The production server requires this to cover
    /// its fixed 24-hour atomic-replay window; short values are only useful to
    /// embedded callers such as focused garbage-collection tests.
    pub awaiting_publish_ttl_seconds: u64,
}

impl StoreOptions {
    pub fn new(root: impl AsRef<Path>, node_id: u16) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            node_id,
            sync_writes: true,
            watch_retention: WatchRetention::default(),
            mutation_receipt_retention: MutationReceiptRetention::default(),
            awaiting_publish_ttl_seconds: DEFAULT_AWAITING_PUBLISH_TTL_SECONDS,
        }
    }

    pub fn with_watch_retention(mut self, watch_retention: WatchRetention) -> Self {
        self.watch_retention = watch_retention;
        self
    }

    pub fn with_mutation_receipt_retention(
        mut self,
        mutation_receipt_retention: MutationReceiptRetention,
    ) -> Self {
        self.mutation_receipt_retention = mutation_receipt_retention;
        self
    }

    pub fn with_awaiting_publish_ttl_seconds(mut self, ttl_seconds: u64) -> Self {
        self.awaiting_publish_ttl_seconds = ttl_seconds;
        self
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
    pub(crate) bucket_options_lock: Arc<std::sync::Mutex<()>>,
    pub(crate) node_id: u16,
    pub(crate) sync_writes: bool,
    pub(crate) watch_retention: WatchRetention,
    pub(crate) mutation_receipt_retention: MutationReceiptRetention,
    awaiting_publish_ttl_millis: u64,
    watch_source_epoch: [u8; 32],
    watch_token_key: [u8; 32],
    watch_notify: tokio::sync::watch::Sender<()>,
    #[cfg(test)]
    policy_lookup_count: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    test_identity_lock: Arc<std::sync::Mutex<()>>,
}

/// Immutable version descriptors selected for one batch read.
///
/// Selection resolves current heads from one local RocksDB snapshot. The
/// descriptors can therefore be measured before any referenced blob is read,
/// then materialised after releasing the short commit fence. Immutable bytes
/// remain protected by the universal blob-GC inactivity grace.
pub struct BatchGetSelection {
    entries: Vec<(ObjectKey, Result<Option<Version>, MutationError>)>,
}

/// One immutable descriptor selected under the commit fence. Its payload is
/// opened after releasing that fence; the universal blob-GC inactivity grace
/// protects this immediate handoff.
pub struct OpenedObject {
    pub version: Version,
    pub reader: Option<BlobReader>,
}

/// One stateless read-committed page of current live object paths.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListObjectsPage {
    pub paths: Vec<String>,
    pub has_more: bool,
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
                Ok(Some(version)) => match (&version.blob, version.deleted) {
                    (Some(blob), false) => blob.length,
                    (None, true) => 0,
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
        identity: BucketIdentity,
        payload: PreparedPayload,
        fingerprint: [u8; 32],
    },
    Publish {
        request: PublishRequest,
        identity: BucketIdentity,
        fingerprint: [u8; 32],
    },
    Delete {
        request: DeleteRequest,
        identity: BucketIdentity,
        fingerprint: [u8; 32],
    },
}

#[derive(Clone)]
enum PreparedPayload {
    Small { reference: BlobRef, bytes: Vec<u8> },
    Large(BlobRef),
}

impl PreparedPayload {
    fn reference(&self) -> &BlobRef {
        match self {
            Self::Small { reference, .. } | Self::Large(reference) => reference,
        }
    }

    fn small_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Small { bytes, .. } => Some(bytes),
            Self::Large(_) => None,
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

    fn identity(&self) -> BucketIdentity {
        match self {
            Self::Put { identity, .. }
            | Self::Publish { identity, .. }
            | Self::Delete { identity, .. } => *identity,
        }
    }

    fn encoded_head_key(&self) -> Vec<u8> {
        self.identity().head_key(self.key().path())
    }

    fn precondition(&self) -> Precondition {
        match self {
            Self::Put { request, .. } => request.mode.precondition(),
            Self::Publish { request, .. } => request.mode.precondition(),
            Self::Delete { request, .. } => request.precondition,
        }
    }

    fn put_mode(&self) -> Option<PutMode> {
        match self {
            Self::Put { request, .. } => Some(request.mode),
            Self::Publish { request, .. } => Some(request.mode),
            Self::Delete { .. } => None,
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
    expires_at_unix_millis: u64,
}

pub(crate) type PendingBlobReferences = BTreeMap<Vec<u8>, BlobReferenceState>;

impl Store {
    pub async fn open(options: StoreOptions) -> Result<Self> {
        WatchRetention::new(
            options.watch_retention.max_entries,
            options.watch_retention.max_bytes,
        )?;
        MutationReceiptRetention::new(
            options.mutation_receipt_retention.retention_seconds,
            options.mutation_receipt_retention.max_entries,
            options.mutation_receipt_retention.max_bytes,
        )?;
        if options.awaiting_publish_ttl_seconds == 0 {
            anyhow::bail!("awaiting-publish blob TTL must be non-zero");
        }
        let awaiting_publish_ttl_millis = options
            .awaiting_publish_ttl_seconds
            .checked_mul(1_000)
            .context("awaiting-publish blob TTL is too large")?;
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
        let (watch_source_epoch, watch_token_key) =
            initialize_local_watch_metadata(&db, metadata_cf, options.sync_writes)?;
        initialize_mutation_receipt_metadata(&db, metadata_cf, options.sync_writes)?;
        let db = Arc::new(db);
        let store = Self {
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
            bucket_options_lock: Arc::new(std::sync::Mutex::new(())),
            node_id: options.node_id,
            sync_writes: options.sync_writes,
            watch_retention: options.watch_retention,
            mutation_receipt_retention: options.mutation_receipt_retention,
            awaiting_publish_ttl_millis,
            watch_source_epoch,
            watch_token_key,
            watch_notify: tokio::sync::watch::channel(()).0,
            #[cfg(test)]
            policy_lookup_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            test_identity_lock: Arc::new(std::sync::Mutex::new(())),
        };
        store.enforce_local_watch_retention()?;
        Ok(store)
    }

    pub(crate) fn tenant_id_by_name(
        &self,
        tenant: &str,
    ) -> Result<Option<TenantId>, MutationError> {
        self.db
            .get_cf(self.cf(CF_NAMES)?, tenant_name_key(tenant))
            .map_err(storage_error)?
            .map(|encoded| {
                decode_identity_value(&encoded)
                    .map(TenantId)
                    .map_err(storage_error)
            })
            .transpose()
    }

    pub(crate) fn bucket_id_by_name(
        &self,
        tenant_id: TenantId,
        bucket: &str,
    ) -> Result<Option<BucketId>, MutationError> {
        self.db
            .get_cf(self.cf(CF_NAMES)?, bucket_name_key(tenant_id, bucket))
            .map_err(storage_error)?
            .map(|encoded| {
                decode_identity_value(&encoded)
                    .map(BucketId)
                    .map_err(storage_error)
            })
            .transpose()
    }

    pub(crate) fn resolve_bucket_identity(
        &self,
        tenant: &str,
        bucket: &str,
    ) -> Result<BucketIdentity, MutationError> {
        let Some(tenant_id) = self.tenant_id_by_name(tenant)? else {
            #[cfg(test)]
            return self.install_test_bucket_identity(tenant, bucket);
            #[cfg(not(test))]
            return Err(MutationError::Storage(format!(
                "tenant `{tenant}` has no stable identity"
            )));
        };
        let Some(bucket_id) = self.bucket_id_by_name(tenant_id, bucket)? else {
            #[cfg(test)]
            return self.install_test_bucket_identity(tenant, bucket);
            #[cfg(not(test))]
            return Err(MutationError::Storage(format!(
                "bucket `{tenant}/{bucket}` has no stable identity"
            )));
        };
        Ok(BucketIdentity {
            tenant_id,
            bucket_id,
        })
    }

    #[cfg(test)]
    fn install_test_bucket_identity(
        &self,
        tenant: &str,
        bucket: &str,
    ) -> Result<BucketIdentity, MutationError> {
        let _guard = self
            .test_identity_lock
            .lock()
            .map_err(|_| MutationError::Storage("test identity lock is poisoned".into()))?;
        let tenant_id = match self.tenant_id_by_name(tenant)? {
            Some(id) => id,
            None => TenantId(self.clock.next().map_err(storage_error)?.0),
        };
        let bucket_id = match self.bucket_id_by_name(tenant_id, bucket)? {
            Some(id) => id,
            None => BucketId(self.clock.next().map_err(storage_error)?.0),
        };
        let identity = BucketIdentity {
            tenant_id,
            bucket_id,
        };
        let mut batch = WriteBatch::default();
        batch.put_cf(
            self.cf(CF_NAMES)?,
            tenant_name_key(tenant),
            tenant_id.0.to_be_bytes(),
        );
        batch.put_cf(
            self.cf(CF_NAMES)?,
            bucket_name_key(tenant_id, bucket),
            bucket_id.0.to_be_bytes(),
        );
        let watermark = VersionId(tenant_id.0.max(bucket_id.0));
        batch.put_cf(
            self.cf(CF_METADATA)?,
            VERSION_HIGH_WATERMARK_KEY,
            serde_json::to_vec(&watermark).map_err(storage_error)?,
        );
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
        Ok(identity)
    }

    pub(crate) fn head_storage_key(&self, key: &ObjectKey) -> Result<Vec<u8>, MutationError> {
        Ok(self
            .resolve_bucket_identity(key.tenant(), key.bucket())?
            .head_key(key.path()))
    }

    /// Adds immutable namespaces. Existing immutable policy may only become
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
        let key = self.resolve_bucket_identity(tenant, bucket)?.encode();
        let policy_path = ObjectPath::new(tenant, bucket, "_anvil/policy")
            .map_err(MutationError::InvalidPolicy)?;
        let _guard = self.ordinary_locks.acquire(&[policy_path]).await;
        if let Some(existing) = self.bucket_policy_by_key(&key)? {
            let requested_immutable = policy.immutable_prefixes.iter().collect::<BTreeSet<_>>();
            if existing
                .immutable_prefixes
                .iter()
                .any(|prefix| !requested_immutable.contains(prefix))
            {
                return Err(MutationError::InvalidPolicy(
                    "immutable prefixes cannot be removed".into(),
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
        let key = self.resolve_bucket_identity(tenant, bucket)?.encode();
        Ok(self.bucket_policy_by_key(&key)?.unwrap_or_default())
    }

    pub fn bucket_versioning(
        &self,
        tenant: &str,
        bucket: &str,
    ) -> Result<ObjectVersioning, MutationError> {
        let key = self.resolve_bucket_identity(tenant, bucket)?.encode();
        self.bucket_versioning_by_key(&key)
    }

    pub(crate) fn bucket_versioning_by_key(
        &self,
        key: &[u8],
    ) -> Result<ObjectVersioning, MutationError> {
        self.db
            .get_cf(self.cf(CF_BUCKET_OPTIONS)?, key)
            .map_err(storage_error)?
            .map(|encoded| decode_object_versioning(&encoded))
            .transpose()
            .map(|versioning| versioning.unwrap_or_default())
    }

    /// Enables retained object versions for one bucket. Absence is the
    /// durable default (`Unversioned`); there is deliberately no disable path.
    pub async fn enable_bucket_versioning(
        &self,
        tenant: &str,
        bucket: &str,
    ) -> Result<bool, MutationError> {
        // Enabling version retention and an ordinary/program commit must have
        // one order. Once this write returns, no mutation may still commit
        // using the old unversioned replacement rule.
        let _commit_guard = self.commit_lock.lock().await;
        let _guard = self
            .bucket_options_lock
            .lock()
            .map_err(|_| MutationError::Storage("bucket-options lock is poisoned".into()))?;
        let identity = self.resolve_bucket_identity(tenant, bucket)?;
        if self.bucket_versioning_by_key(&identity.encode())? == ObjectVersioning::Enabled {
            return Ok(false);
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .put_cf_opt(
                self.cf(CF_BUCKET_OPTIONS)?,
                identity.encode(),
                encode_object_versioning(ObjectVersioning::Enabled),
                &options,
            )
            .map_err(storage_error)?;
        Ok(true)
    }

    pub(crate) fn stage_bucket_versioning(
        &self,
        batch: &mut WriteBatch,
        identity: BucketIdentity,
        versioning: ObjectVersioning,
    ) -> Result<(), MutationError> {
        batch.put_cf(
            self.cf(CF_BUCKET_OPTIONS)?,
            identity.encode(),
            encode_object_versioning(versioning),
        );
        Ok(())
    }

    pub(crate) fn bucket_policy_by_key(
        &self,
        key: &[u8],
    ) -> Result<Option<BucketPolicy>, MutationError> {
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

mod blob_references;
mod mutations;
mod reads;
mod reference_deltas;
mod watch_journal;

pub(crate) fn is_program_definition_path(path: &str) -> bool {
    let Some(name_and_version) = path.strip_prefix(PROGRAM_DEFINITION_PREFIX) else {
        return false;
    };
    if name_and_version.contains('/') || name_and_version.matches('@').count() != 1 {
        return false;
    }
    name_and_version
        .split_once('@')
        .is_some_and(|(name, version)| !name.is_empty() && !version.is_empty())
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

fn initialize_local_watch_metadata(
    db: &DB,
    metadata: &rocksdb::ColumnFamily,
    sync_writes: bool,
) -> Result<([u8; 32], [u8; 32])> {
    let epoch = db.get_cf(metadata, LOCAL_INVALIDATION_EPOCH_KEY)?;
    let token_key = db.get_cf(metadata, LOCAL_INVALIDATION_TOKEN_KEY)?;
    let offset = db.get_cf(metadata, LOCAL_INVALIDATION_OFFSET_KEY)?;
    let floor = db.get_cf(metadata, LOCAL_INVALIDATION_FLOOR_KEY)?;
    let count = db.get_cf(metadata, LOCAL_INVALIDATION_COUNT_KEY)?;
    let bytes = db.get_cf(metadata, LOCAL_INVALIDATION_BYTES_KEY)?;
    let all_absent = epoch.is_none()
        && token_key.is_none()
        && offset.is_none()
        && floor.is_none()
        && count.is_none()
        && bytes.is_none();
    let all_present = epoch.is_some()
        && token_key.is_some()
        && offset.is_some()
        && floor.is_some()
        && count.is_some()
        && bytes.is_some();
    if !all_absent && !all_present {
        anyhow::bail!("local watch metadata is only partially initialized");
    }
    if all_absent {
        let mut source_epoch = [0_u8; 32];
        let mut integrity_key = [0_u8; 32];
        getrandom::fill(&mut source_epoch)
            .map_err(|error| anyhow::anyhow!("generate local watch source epoch: {error}"))?;
        getrandom::fill(&mut integrity_key)
            .map_err(|error| anyhow::anyhow!("generate local watch token key: {error}"))?;
        let mut batch = WriteBatch::default();
        batch.put_cf(metadata, LOCAL_INVALIDATION_EPOCH_KEY, source_epoch);
        batch.put_cf(metadata, LOCAL_INVALIDATION_TOKEN_KEY, integrity_key);
        for key in [
            LOCAL_INVALIDATION_OFFSET_KEY,
            LOCAL_INVALIDATION_FLOOR_KEY,
            LOCAL_INVALIDATION_COUNT_KEY,
            LOCAL_INVALIDATION_BYTES_KEY,
        ] {
            batch.put_cf(metadata, key, 0_u64.to_be_bytes());
        }
        let mut options = WriteOptions::default();
        options.set_sync(sync_writes);
        db.write_opt(batch, &options)?;
        return Ok((source_epoch, integrity_key));
    }

    let source_epoch = epoch
        .expect("checked present")
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("local watch source epoch is malformed"))?;
    let integrity_key = token_key
        .expect("checked present")
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("local watch token key is malformed"))?;
    Ok((source_epoch, integrity_key))
}

fn initialize_mutation_receipt_metadata(
    db: &DB,
    metadata: &rocksdb::ColumnFamily,
    sync_writes: bool,
) -> Result<()> {
    let count = db.get_cf(metadata, MUTATION_RECEIPT_COUNT_KEY)?;
    let bytes = db.get_cf(metadata, MUTATION_RECEIPT_BYTES_KEY)?;
    match (count, bytes) {
        (None, None) => {
            let receipts = db
                .cf_handle(CF_RECEIPTS)
                .context("missing receipts column family")?;
            if db
                .iterator_cf(receipts, IteratorMode::Start)
                .next()
                .is_some()
            {
                anyhow::bail!("mutation receipts exist without retention metadata");
            }
            let mut batch = WriteBatch::default();
            batch.put_cf(metadata, MUTATION_RECEIPT_COUNT_KEY, 0_u64.to_be_bytes());
            batch.put_cf(metadata, MUTATION_RECEIPT_BYTES_KEY, 0_u64.to_be_bytes());
            let mut options = WriteOptions::default();
            options.set_sync(sync_writes);
            db.write_opt(batch, &options)?;
        }
        (Some(count), Some(bytes)) => {
            decode_offset(&count).map_err(|error| anyhow::anyhow!(error.to_string()))?;
            decode_offset(&bytes).map_err(|error| anyhow::anyhow!(error.to_string()))?;
        }
        _ => anyhow::bail!("mutation receipt retention metadata is only partially initialized"),
    }
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

fn fail_prepared_operations(
    early: BTreeMap<usize, MutationError>,
    prepared: Vec<(usize, PreparedOperation)>,
    error: MutationError,
) -> Vec<BatchOutcome> {
    let mut results = early
        .into_iter()
        .map(|(index, error)| (index, Err(error)))
        .collect::<BTreeMap<_, _>>();
    results.extend(
        prepared
            .into_iter()
            .map(|(index, _)| (index, Err(error.clone()))),
    );
    results
        .into_iter()
        .map(|(index, result)| BatchOutcome { index, result })
        .collect()
}

fn encode_object_versioning(versioning: ObjectVersioning) -> [u8; 1] {
    [match versioning {
        ObjectVersioning::Unversioned => 0,
        ObjectVersioning::Enabled => 1,
    }]
}

fn decode_object_versioning(encoded: &[u8]) -> Result<ObjectVersioning, MutationError> {
    match encoded {
        [0] => Ok(ObjectVersioning::Unversioned),
        [1] => Ok(ObjectVersioning::Enabled),
        _ => Err(MutationError::Storage(
            "bucket object-versioning option is malformed".into(),
        )),
    }
}

fn decode_offset(encoded: &[u8]) -> Result<u64, MutationError> {
    let bytes: [u8; size_of::<u64>()] = encoded.try_into().map_err(|_| {
        MutationError::Storage("durable local invalidation offset is malformed".into())
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn decode_watch_u64(encoded: &[u8]) -> Result<u64, WatchError> {
    let bytes: [u8; size_of::<u64>()] = encoded
        .try_into()
        .map_err(|_| WatchError::Storage("local watch counter is malformed".into()))?;
    Ok(u64::from_be_bytes(bytes))
}

pub(crate) fn version_prefix(identity: BucketIdentity, key: &ObjectKey) -> Vec<u8> {
    let mut encoded = identity.head_key(key.path());
    // Canonical paths cannot contain NUL, so this one-byte terminator makes
    // exact-path version iteration unambiguous without a length field.
    encoded.push(0);
    encoded
}

pub(crate) fn version_key(
    identity: BucketIdentity,
    key: &ObjectKey,
    version: VersionId,
) -> Vec<u8> {
    let mut encoded = version_prefix(identity, key);
    encoded.extend_from_slice(&version.0.to_be_bytes());
    encoded
}

fn blob_reference_key(reference: &BlobRef) -> Vec<u8> {
    let mut key = Vec::with_capacity(32 + size_of::<u64>());
    key.extend_from_slice(&reference.hash);
    key.extend_from_slice(&reference.length.to_be_bytes());
    key
}

fn blob_reference_for_bytes(bytes: &[u8]) -> BlobRef {
    BlobRef {
        hash: *blake3::hash(bytes).as_bytes(),
        length: bytes.len() as u64,
    }
}

fn is_small_blob(reference: &BlobRef) -> bool {
    reference.length <= SMALL_BLOB_MAX_BYTES as u64
}

fn validate_small_blob(reference: &BlobRef, bytes: &[u8]) -> Result<(), MutationError> {
    if !is_small_blob(reference)
        || bytes.len() as u64 != reference.length
        || blake3::hash(bytes).as_bytes() != &reference.hash
    {
        return Err(MutationError::Storage(
            "small blob failed length or hash verification".into(),
        ));
    }
    Ok(())
}

fn blob_reference_from_key(encoded: &[u8]) -> Result<BlobRef, MutationError> {
    if encoded.len() != 32 + size_of::<u64>() {
        return Err(MutationError::Storage(
            "blob reference metadata key is malformed".into(),
        ));
    }
    let hash = encoded[..32]
        .try_into()
        .expect("blob reference hash length was checked");
    let length = u64::from_be_bytes(
        encoded[32..]
            .try_into()
            .expect("blob reference length was checked"),
    );
    Ok(BlobRef { hash, length })
}

fn blob_reference_from_file(
    file: &std::fs::DirEntry,
    shard: &str,
) -> Result<BlobRef, MutationError> {
    let name = file.file_name();
    let name = name
        .to_str()
        .ok_or_else(|| MutationError::Storage("blob file name is not valid UTF-8".into()))?;
    if name.len() != 64
        || !name.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !name.starts_with(shard)
    {
        return Err(MutationError::Storage(
            "blob file name does not match its content-address shard".into(),
        ));
    }
    let mut hash = [0_u8; 32];
    hex::decode_to_slice(name, &mut hash)
        .map_err(|_| MutationError::Storage("blob file name is malformed".into()))?;
    if hex::encode(hash) != name {
        return Err(MutationError::Storage(
            "blob file name is not canonical".into(),
        ));
    }
    let length = file.metadata().map_err(storage_error)?.len();
    Ok(BlobRef { hash, length })
}

fn remove_file_and_sync_parent(path: &Path) -> Result<(), MutationError> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(storage_error(error)),
    }
    let parent = path
        .parent()
        .ok_or_else(|| MutationError::Storage("blob path has no parent".into()))?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(storage_error)
}

fn encode_blob_reference_state(state: BlobReferenceState) -> [u8; 25] {
    let mut encoded = [0_u8; 25];
    encoded[..8].copy_from_slice(&state.ref_count.to_be_bytes());
    encoded[8] = state.flags;
    encoded[9..17].copy_from_slice(&state.created_at.to_be_bytes());
    encoded[17..25].copy_from_slice(&state.updated_at.to_be_bytes());
    encoded
}

fn decode_blob_reference_state(encoded: &[u8]) -> Result<BlobReferenceState, MutationError> {
    if encoded.len() != 25 {
        return Err(MutationError::Storage(
            "blob reference metadata value is malformed".into(),
        ));
    }
    let state = BlobReferenceState {
        ref_count: u64::from_be_bytes(
            encoded[..8]
                .try_into()
                .expect("blob reference count length was checked"),
        ),
        flags: encoded[8],
        created_at: u64::from_be_bytes(
            encoded[9..17]
                .try_into()
                .expect("blob reference creation timestamp length was checked"),
        ),
        updated_at: u64::from_be_bytes(
            encoded[17..25]
                .try_into()
                .expect("blob reference update timestamp length was checked"),
        ),
    };
    validate_blob_reference_state(state)?;
    Ok(state)
}

fn validate_blob_reference_state(state: BlobReferenceState) -> Result<(), MutationError> {
    if state.flags & !AWAITING_PUBLISH != 0 {
        return Err(MutationError::Storage(
            "blob reference metadata has unknown flags".into(),
        ));
    }
    if state.created_at > state.updated_at {
        return Err(MutationError::Storage(
            "blob reference metadata timestamps are inconsistent".into(),
        ));
    }
    if state.flags & AWAITING_PUBLISH != 0 && state.ref_count != 1 {
        return Err(MutationError::Storage(
            "awaiting-publish blob must have exactly one reservation".into(),
        ));
    }
    Ok(())
}

fn advance_blob_reference_publication(
    mut state: BlobReferenceState,
    now_unix_millis: u64,
) -> Result<BlobReferenceState, MutationError> {
    validate_blob_reference_state(state)?;
    if state.ref_count == 0 {
        state.ref_count = 1;
        state.flags = 0;
        state.updated_at = state.updated_at.max(now_unix_millis);
        return Ok(state);
    }
    if state.flags & AWAITING_PUBLISH != 0 {
        if state.ref_count != 1 {
            return Err(MutationError::Storage(
                "awaiting-publish blob must have exactly one reservation".into(),
            ));
        }
        state.flags &= !AWAITING_PUBLISH;
    } else {
        state.ref_count = state
            .ref_count
            .checked_add(1)
            .ok_or_else(|| MutationError::Storage("blob reference count is exhausted".into()))?;
    }
    state.updated_at = state.updated_at.max(now_unix_millis);
    Ok(state)
}

fn blob_reference_is_garbage(
    state: BlobReferenceState,
    now_unix_millis: u64,
    awaiting_publish_ttl_millis: u64,
) -> bool {
    (state.ref_count == 0 || state.flags & AWAITING_PUBLISH != 0)
        && now_unix_millis.saturating_sub(state.updated_at) >= awaiting_publish_ttl_millis
}

pub(crate) fn object_path(key: &ObjectKey) -> ObjectPath {
    ObjectPath::new(key.tenant(), key.bucket(), key.path())
        .expect("validated store key is a valid atomic-program path")
}

fn receipt_key(identity: BucketIdentity, command_id: &str) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.mutation-receipt-key.v1");
    hasher.update(&identity.encode());
    hash_string(&mut hasher, command_id);
    let mut encoded = Vec::with_capacity(2 + 32);
    encoded.extend_from_slice(&[STORAGE_KEY_FORMAT_VERSION, RECEIPT_RECORD_PREFIX]);
    encoded.extend_from_slice(hasher.finalize().as_bytes());
    encoded
}

fn receipt_expiry_key(
    expires_at_unix_millis: u64,
    primary_key: &[u8],
) -> Result<Vec<u8>, MutationError> {
    if primary_key.len() != 34
        || primary_key[..2] != [STORAGE_KEY_FORMAT_VERSION, RECEIPT_RECORD_PREFIX]
    {
        return Err(MutationError::Storage(
            "mutation receipt primary key is malformed".into(),
        ));
    }
    let mut encoded = Vec::with_capacity(2 + 8 + primary_key.len());
    encoded.extend_from_slice(&[STORAGE_KEY_FORMAT_VERSION, RECEIPT_EXPIRY_PREFIX]);
    encoded.extend_from_slice(&expires_at_unix_millis.to_be_bytes());
    encoded.extend_from_slice(primary_key);
    Ok(encoded)
}

fn parse_receipt_expiry_key(encoded: &[u8]) -> Result<Option<(u64, Vec<u8>)>, MutationError> {
    if !encoded.starts_with(&[STORAGE_KEY_FORMAT_VERSION, RECEIPT_EXPIRY_PREFIX]) {
        return Ok(None);
    }
    if encoded.len() != 2 + 8 + 34 {
        return Err(MutationError::Storage(
            "mutation receipt expiry key is malformed".into(),
        ));
    }
    let expires_at = u64::from_be_bytes(
        encoded[2..10]
            .try_into()
            .expect("length checked before expiry decoding"),
    );
    let primary_key = encoded[10..].to_vec();
    if !primary_key.starts_with(&[STORAGE_KEY_FORMAT_VERSION, RECEIPT_RECORD_PREFIX]) {
        return Err(MutationError::Storage(
            "mutation receipt expiry key has a malformed primary key".into(),
        ));
    }
    Ok(Some((expires_at, primary_key)))
}

fn mutation_receipt_logical_bytes(
    primary_key_bytes: usize,
    value_bytes: usize,
    expiry_key_bytes: usize,
) -> u64 {
    (primary_key_bytes as u64)
        .saturating_add(value_bytes as u64)
        .saturating_add(expiry_key_bytes as u64)
}

fn validate_command_id(command_id: Option<&str>) -> Result<(), MutationError> {
    if command_id.is_some_and(|value| value.is_empty() || value.len() > 256 || value.contains('\0'))
    {
        Err(MutationError::InvalidCommandId)
    } else {
        Ok(())
    }
}

pub(crate) fn version_blob_reference(version: &Version) -> Result<Option<BlobRef>, MutationError> {
    match (&version.blob, version.deleted) {
        (Some(blob), false) => Ok(Some(blob.clone())),
        (None, true) => Ok(None),
        _ => Err(MutationError::Storage(
            "version has an invalid payload shape".into(),
        )),
    }
}

fn validate_selected_head(head: &Head, version: &Version) -> Result<(), MutationError> {
    validate_selected_version_id(head.version, version)?;
    if version.deleted != head.deleted {
        return Err(MutationError::Storage(
            "selected version descriptor disagrees with its head".into(),
        ));
    }
    version_blob_reference(version).map(|_| ())
}

fn validate_selected_version_id(
    selected_version: VersionId,
    version: &Version,
) -> Result<(), MutationError> {
    if version.id != selected_version {
        Err(MutationError::Storage(
            "selected version descriptor disagrees with its key".into(),
        ))
    } else {
        Ok(())
    }
}

fn put_fingerprint(
    encoded_head_key: &[u8],
    mode: PutMode,
    content_type: Option<&str>,
    durability: Durability,
    blob: &BlobRef,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.put.v1");
    hasher.update(encoded_head_key);
    hash_put_mode(&mut hasher, mode);
    hash_optional_string(&mut hasher, content_type);
    hash_durability(&mut hasher, durability);
    hasher.update(&blob.hash);
    hasher.update(&blob.length.to_be_bytes());
    *hasher.finalize().as_bytes()
}

fn delete_fingerprint(request: &DeleteRequest, identity: BucketIdentity) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.delete.v1");
    hasher.update(&identity.head_key(request.key.path()));
    hash_precondition(&mut hasher, request.precondition);
    hash_durability(&mut hasher, request.durability);
    *hasher.finalize().as_bytes()
}

fn publish_fingerprint(request: &PublishRequest, identity: BucketIdentity) -> [u8; 32] {
    // Publish is an internal staging detail for a streamed Put. Its canonical
    // idempotency identity must therefore be identical to an inline/bulk Put
    // with the same logical input.
    put_fingerprint(
        &identity.head_key(request.key.path()),
        request.mode,
        request.content_type.as_deref(),
        request.durability,
        &request.blob,
    )
}

fn hash_put_mode(hasher: &mut blake3::Hasher, mode: PutMode) {
    match mode {
        PutMode::Put => hasher.update(&[0]),
        PutMode::PutIfAbsent => hasher.update(&[1]),
        PutMode::PutIfVersion(version) => {
            hasher.update(&[2]);
            hasher.update(&version.0.to_be_bytes())
        }
        PutMode::PutImmutable => hasher.update(&[3]),
    };
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

fn hash_durability(hasher: &mut blake3::Hasher, durability: Durability) {
    hasher.update(&[match durability {
        Durability::Local => 0,
        Durability::Replicated => 1,
    }]);
}

fn require_local_durability(durability: Durability) -> Result<(), MutationError> {
    match durability {
        Durability::Local => Ok(()),
        Durability::Replicated => Err(MutationError::DurabilityUnavailable),
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
mod tests;
