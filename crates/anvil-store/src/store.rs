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
    decode_identity_value, tenant_name_key,
};
use crate::watch::{
    LOCAL_INVALIDATION_BYTES_KEY, LOCAL_INVALIDATION_COUNT_KEY, LOCAL_INVALIDATION_EPOCH_KEY,
    LOCAL_INVALIDATION_FLOOR_KEY, LOCAL_INVALIDATION_OFFSET_KEY, LOCAL_INVALIDATION_TOKEN_KEY,
    LocalInvalidation, MAX_LOCAL_INVALIDATION_SCAN_RECORDS, WatchCursor, WatchError,
    WatchJournalStatus, WatchPage, WatchRetention, WatchScope, WatchStart, decode_resume_token,
    encode_resume_token, invalidation_key, invalidation_record_bytes, offset_from_key,
};
use crate::{
    AWAITING_PUBLISH, BatchOperation, BatchOutcome, BlobReader, BlobRef, BlobReferenceState,
    BlobStore, BucketPolicy, DeleteRequest, DeleteRetainedVersionOutcome, Durability, Head,
    MutationError, MutationReceipt, Object, ObjectKey, ObjectVersioning, Precondition,
    PublishRequest, PutMode, PutRequest, SMALL_BLOB_MAX_BYTES, Version, VersionClock, VersionId,
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

    pub fn head(&self, key: &ObjectKey) -> Result<Option<Head>, MutationError> {
        self.head_by_storage_key(&self.head_storage_key(key)?)
    }

    pub(crate) fn head_by_storage_key(
        &self,
        encoded_key: &[u8],
    ) -> Result<Option<Head>, MutationError> {
        self.read_json(CF_HEADS, encoded_key)
    }

    /// Lists current live paths directly from the prefix-sortable head keys.
    /// No listing projection or side index is maintained: the iterator seeks
    /// to `[format][tenant ID][bucket ID][literal prefix]` and stops as soon as
    /// that byte prefix no longer matches.
    pub fn list_objects(
        &self,
        tenant: &str,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ListObjectsPage, MutationError> {
        let limit = limit.min(MAX_LIST_OBJECTS);
        if limit == 0 {
            return Ok(ListObjectsPage {
                paths: Vec::new(),
                has_more: false,
            });
        }

        let identity = self.resolve_bucket_identity(tenant, bucket)?;
        let bucket_prefix = identity.encode();
        let mut range_prefix = Vec::with_capacity(bucket_prefix.len() + prefix.len());
        range_prefix.extend_from_slice(&bucket_prefix);
        range_prefix.extend_from_slice(prefix.as_bytes());
        let mut seek = range_prefix.clone();
        if let Some(cursor) = start_after
            && cursor.as_bytes() > prefix.as_bytes()
        {
            seek.truncate(bucket_prefix.len());
            seek.extend_from_slice(cursor.as_bytes());
        }
        let snapshot = self.db.snapshot();
        let mut paths = Vec::with_capacity(limit.saturating_add(1));
        for entry in snapshot.iterator_cf(
            self.cf(CF_HEADS)?,
            IteratorMode::From(&seek, Direction::Forward),
        ) {
            let (stored_key, encoded_head) = entry.map_err(storage_error)?;
            if !stored_key.starts_with(&range_prefix) {
                break;
            }
            let path = identity
                .decode_head_path(&stored_key)
                .map_err(storage_error)?;
            let head = serde_json::from_slice::<Head>(&encoded_head).map_err(storage_error)?;
            if head.deleted || start_after.is_some_and(|cursor| path <= cursor) {
                continue;
            }
            paths.push(path.to_owned());
            if paths.len() > limit {
                break;
            }
        }

        let has_more = paths.len() > limit;
        paths.truncate(limit);
        Ok(ListObjectsPage { paths, has_more })
    }

    /// Returns the last durable offset in this store's local invalidation
    /// journal. Zero means that no ordinary or atomic head change has been
    /// appended.
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

    pub fn local_watch_status(&self) -> Result<WatchJournalStatus, WatchError> {
        let metadata = self
            .cf(CF_METADATA)
            .map_err(|error| WatchError::Storage(error.to_string()))?;
        let snapshot = self.db.snapshot();
        let read_counter = |key: &[u8]| {
            let encoded = snapshot
                .get_cf(metadata, key)
                .map_err(|error| WatchError::Storage(error.to_string()))?
                .ok_or_else(|| WatchError::Storage("local watch metadata is missing".into()))?;
            decode_watch_u64(&encoded)
        };
        let tail = read_counter(LOCAL_INVALIDATION_OFFSET_KEY)?;
        let retention_floor = read_counter(LOCAL_INVALIDATION_FLOOR_KEY)?;
        let retained_entries = read_counter(LOCAL_INVALIDATION_COUNT_KEY)?;
        let retained_bytes = read_counter(LOCAL_INVALIDATION_BYTES_KEY)?;
        if retention_floor > tail || retained_entries != tail - retention_floor {
            return Err(WatchError::Storage(
                "local invalidation retention metadata is inconsistent".into(),
            ));
        }
        Ok(WatchJournalStatus {
            source_epoch: self.watch_source_epoch,
            tail,
            retention_floor,
            retained_entries,
            retained_bytes,
        })
    }

    pub fn start_watch(
        &self,
        scope: &WatchScope,
        start: WatchStart,
    ) -> Result<WatchCursor, WatchError> {
        let status = self.local_watch_status()?;
        let cursor = match start {
            WatchStart::Now => WatchCursor::new(status.tail),
            WatchStart::RetainedBeginning => WatchCursor::new(status.retention_floor),
            WatchStart::Resume(token) => decode_resume_token(
                &token,
                scope,
                self.watch_source_epoch,
                &self.watch_token_key,
                self.watch_retention,
            )?,
        };
        if cursor.offset() < status.retention_floor || cursor.offset() > status.tail {
            return Err(WatchError::ResumeExpired);
        }
        Ok(cursor)
    }

    pub fn watch_checkpoint(
        &self,
        scope: &WatchScope,
        cursor: WatchCursor,
    ) -> Result<Vec<u8>, WatchError> {
        let status = self.local_watch_status()?;
        if cursor.offset() < status.retention_floor || cursor.offset() > status.tail {
            return Err(WatchError::ResumeExpired);
        }
        encode_resume_token(
            scope,
            cursor,
            self.watch_source_epoch,
            &self.watch_token_key,
            self.watch_retention,
        )
    }

    /// Scans a bounded number of retained source records, filtering only
    /// after each record has been represented in the returned checkpoint.
    /// This allows unrelated paths to advance a prefix-specific cursor without
    /// silently stepping over a matching invalidation.
    pub async fn scan_watch_page(
        &self,
        scope: &WatchScope,
        cursor: WatchCursor,
        limit: usize,
    ) -> Result<WatchPage, WatchError> {
        let _commit_guard = self.commit_lock.lock().await;
        let status = self.local_watch_status()?;
        if cursor.offset() < status.retention_floor || cursor.offset() > status.tail {
            return Err(WatchError::ResumeExpired);
        }
        let limit = limit.min(MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
        if limit == 0 || cursor.offset() == status.tail {
            return Ok(WatchPage {
                invalidations: Vec::new(),
                checkpoint: cursor,
            });
        }
        let through = cursor
            .offset()
            .saturating_add(limit as u64)
            .min(status.tail);
        let mut invalidations = Vec::new();
        let first_offset = cursor.offset() + 1;
        let first_key = invalidation_key(first_offset);
        let iterator = self.db.iterator_cf(
            self.cf(CF_LOCAL_INVALIDATIONS)
                .map_err(|error| WatchError::Storage(error.to_string()))?,
            IteratorMode::From(&first_key, Direction::Forward),
        );
        let expected_records = usize::try_from(through - first_offset + 1)
            .expect("watch page is bounded by a usize limit");
        let mut records_seen = 0_usize;
        for entry in iterator.take(limit) {
            let (key, encoded) = entry.map_err(|error| WatchError::Storage(error.to_string()))?;
            let offset = offset_from_key(&key)
                .ok_or_else(|| WatchError::Storage("local invalidation key is malformed".into()))?;
            let expected = first_offset + records_seen as u64;
            if offset != expected || offset > through {
                return Err(WatchError::Storage(format!(
                    "retained local invalidation offset {expected} is missing"
                )));
            }
            let invalidation = serde_json::from_slice::<LocalInvalidation>(&encoded)
                .map_err(|error| WatchError::Storage(error.to_string()))?;
            if invalidation.offset != offset {
                return Err(WatchError::Storage(
                    "local invalidation key does not match its stored offset".into(),
                ));
            }
            if scope.contains(&invalidation.key) {
                invalidations.push(invalidation);
            }
            records_seen += 1;
        }
        if records_seen != expected_records {
            let missing = first_offset + records_seen as u64;
            return Err(WatchError::Storage(format!(
                "retained local invalidation offset {missing} is missing"
            )));
        }
        Ok(WatchPage {
            invalidations,
            checkpoint: WatchCursor::new(through),
        })
    }

    /// Waits until a scan after `cursor` may return a record or an expiry.
    /// Registering the notification before rereading the tail avoids a lost
    /// wake-up between the caller's empty scan and this wait.
    pub async fn wait_for_watch_change(&self, cursor: WatchCursor) -> Result<(), WatchError> {
        let mut notifications = self.watch_notify.subscribe();
        loop {
            let status = self.local_watch_status()?;
            if cursor.offset() < status.retention_floor || cursor.offset() > status.tail {
                return Err(WatchError::ResumeExpired);
            }
            if cursor.offset() < status.tail {
                return Ok(());
            }
            notifications
                .changed()
                .await
                .map_err(|_| WatchError::Storage("local watch notifier closed".into()))?;
        }
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
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        let selected = {
            let _commit_guard = self.commit_lock.lock().await;
            let Some(head) = self.head_by_storage_key(&identity.head_key(key.path()))? else {
                return Ok(None);
            };
            if head.deleted {
                return Ok(None);
            }
            let version = self
                .version_metadata_by_identity(identity, key, head.version)?
                .ok_or_else(|| {
                    MutationError::Storage("head references a missing version descriptor".into())
                })?;
            validate_selected_head(&head, &version)?;
            version
        };
        self.materialize_selected_object(key, selected)
            .await
            .map(Some)
    }

    pub async fn get_version(
        &self,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Option<Object>, MutationError> {
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        if self.bucket_versioning_by_key(&identity.encode())? != ObjectVersioning::Enabled {
            return Err(MutationError::ObjectVersioningNotEnabled);
        }
        let selected = {
            let _commit_guard = self.commit_lock.lock().await;
            let selected = self.version_metadata_by_identity(identity, key, version_id)?;
            if let Some(version) = &selected {
                validate_selected_version_id(version_id, version)?;
            }
            selected
        };
        match selected {
            Some(version) => self
                .materialize_selected_object(key, version)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    async fn materialize_selected_object(
        &self,
        key: &ObjectKey,
        version: Version,
    ) -> Result<Object, MutationError> {
        let bytes = match (&version.blob, version.deleted) {
            (Some(blob), false) => self.read_blob_bytes(blob).await?,
            (None, true) => Vec::new(),
            _ => {
                return Err(MutationError::Storage(
                    "version has an invalid payload shape".into(),
                ));
            }
        };
        Ok(Object {
            key: key.clone(),
            version,
            bytes,
        })
    }

    pub fn version_metadata(
        &self,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Option<Version>, MutationError> {
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        self.version_metadata_by_identity(identity, key, version_id)
    }

    pub(crate) fn version_metadata_by_identity(
        &self,
        identity: BucketIdentity,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Option<Version>, MutationError> {
        self.read_json(CF_VERSIONS, &version_key(identity, key, version_id))
    }

    /// Returns the current descriptor without loading its payload.
    ///
    /// The head and descriptor are selected under the commit fence so an
    /// unversioned replacement cannot retire the descriptor between the two
    /// reads. This is the cheap metadata path used by `HeadObject`.
    pub async fn current_version_metadata(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<Version>, MutationError> {
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        let _commit_guard = self.commit_lock.lock().await;
        let Some(head) = self.head_by_storage_key(&identity.head_key(key.path()))? else {
            return Ok(None);
        };
        let version = self
            .version_metadata_by_identity(identity, key, head.version)?
            .ok_or_else(|| {
                MutationError::Storage("head references a missing version descriptor".into())
            })?;
        validate_selected_head(&head, &version)?;
        Ok(Some(version))
    }

    pub async fn open_object(
        &self,
        key: &ObjectKey,
        requested_version: Option<VersionId>,
    ) -> Result<Option<OpenedObject>, MutationError> {
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        if requested_version.is_some()
            && self.bucket_versioning_by_key(&identity.encode())? != ObjectVersioning::Enabled
        {
            return Err(MutationError::ObjectVersioningNotEnabled);
        }
        let version = {
            let _commit_guard = self.commit_lock.lock().await;
            let (version_id, selected_head) = match requested_version {
                Some(version) => (version, None),
                None => match self.head_by_storage_key(&identity.head_key(key.path()))? {
                    Some(head) => (head.version, Some(head)),
                    None => return Ok(None),
                },
            };
            let Some(version) = self.version_metadata_by_identity(identity, key, version_id)?
            else {
                return if selected_head.is_some() {
                    Err(MutationError::Storage(
                        "head references a missing version descriptor".into(),
                    ))
                } else {
                    Ok(None)
                };
            };
            match &selected_head {
                Some(head) => validate_selected_head(head, &version)?,
                None => validate_selected_version_id(version_id, &version)?,
            }
            version
        };
        let reader = match version_blob_reference(&version)? {
            Some(reference) => Some(self.open_blob(&reference).await?),
            None => None,
        };
        Ok(Some(OpenedObject { version, reader }))
    }

    /// Lists retained descriptors for one exact path in ascending version
    /// order. `after` is exclusive and the store always applies its own cap.
    pub fn list_object_versions(
        &self,
        key: &ObjectKey,
        after: Option<VersionId>,
        limit: usize,
    ) -> Result<Vec<Version>, MutationError> {
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        if self.bucket_versioning_by_key(&identity.encode())? != ObjectVersioning::Enabled {
            return Err(MutationError::ObjectVersioningNotEnabled);
        }
        let limit = limit.min(MAX_LIST_OBJECT_VERSIONS);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = version_prefix(identity, key);
        let start = after.map_or_else(
            || prefix.clone(),
            |version| version_key(identity, key, version),
        );
        let mut versions = Vec::with_capacity(limit);
        for entry in self.db.iterator_cf(
            self.cf(CF_VERSIONS)?,
            IteratorMode::From(&start, Direction::Forward),
        ) {
            let (stored_key, encoded) = entry.map_err(storage_error)?;
            if !stored_key.starts_with(&prefix) || stored_key.len() != prefix.len() + 8 {
                break;
            }
            let stored_id = VersionId(u64::from_be_bytes(
                stored_key[prefix.len()..]
                    .try_into()
                    .expect("retained version key length was checked"),
            ));
            if after.is_some_and(|after| stored_id <= after) {
                continue;
            }
            let version = serde_json::from_slice::<Version>(&encoded).map_err(storage_error)?;
            if version.id != stored_id {
                return Err(MutationError::Storage(
                    "retained version key and descriptor disagree".into(),
                ));
            }
            version_blob_reference(&version)?;
            versions.push(version);
            if versions.len() == limit {
                break;
            }
        }
        Ok(versions)
    }

    pub async fn delete_retained_version(
        &self,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<DeleteRetainedVersionOutcome, MutationError> {
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        if self.bucket_versioning_by_key(&identity.encode())? != ObjectVersioning::Enabled {
            return Err(MutationError::ObjectVersioningNotEnabled);
        }
        let _policy_guard = self.policy_gate.read().await;
        let _path_guard = self.ordinary_locks.acquire(&[object_path(key)]).await;
        let _commit_guard = self.commit_lock.lock().await;
        let policy = self
            .bucket_policy_by_key(&identity.encode())?
            .unwrap_or_default();
        if policy.is_program_only(key.path()) && !is_program_definition_path(key.path()) {
            return Err(MutationError::ProgramConcurrencyViolation);
        }
        if policy.is_immutable(key.path()) || is_program_definition_path(key.path()) {
            return Err(MutationError::Immutable);
        }
        let Some(head) = self.head_by_storage_key(&identity.head_key(key.path()))? else {
            return Ok(DeleteRetainedVersionOutcome::NotFound);
        };
        let Some(target) = self.version_metadata_by_identity(identity, key, version_id)? else {
            if head.version == version_id {
                return Err(MutationError::Storage(
                    "head references a missing retained version".into(),
                ));
            }
            return Ok(DeleteRetainedVersionOutcome::NotFound);
        };
        if target.id != version_id || target.deleted != (target.blob.is_none()) {
            return Err(MutationError::Storage(
                "retained version descriptor is malformed".into(),
            ));
        }

        let mut batch = WriteBatch::default();
        let mut pending_references = PendingBlobReferences::new();
        if let Some(reference) = version_blob_reference(&target)? {
            let (reference_key, state) = self.prepare_blob_reference_retirement(
                &reference,
                &pending_references,
                now_unix_millis()?,
            )?;
            self.stage_blob_reference_update(
                &mut batch,
                &mut pending_references,
                reference_key,
                state,
            )?;
        }
        batch.delete_cf(
            self.cf(CF_VERSIONS)?,
            version_key(identity, key, version_id),
        );

        let (outcome, invalidation) = if head.version != version_id {
            (DeleteRetainedVersionOutcome::DeletedNonCurrent, None)
        } else {
            if target.deleted {
                return Err(MutationError::CurrentTombstoneCannotBeDeleted);
            }
            let tombstone_id = self.clock.next().map_err(storage_error)?;
            let tombstone = Version {
                id: tombstone_id,
                blob: None,
                content_type: None,
                deleted: true,
                committed_at_unix_millis: now_unix_millis()?,
            };
            batch.put_cf(
                self.cf(CF_VERSIONS)?,
                version_key(identity, key, tombstone_id),
                serde_json::to_vec(&tombstone).map_err(storage_error)?,
            );
            batch.put_cf(
                self.cf(CF_HEADS)?,
                identity.head_key(key.path()),
                serde_json::to_vec(&Head {
                    version: tombstone_id,
                    deleted: true,
                })
                .map_err(storage_error)?,
            );
            batch.put_cf(
                self.cf(CF_METADATA)?,
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&tombstone_id).map_err(storage_error)?,
            );
            self.stage_local_invalidations(&mut batch, &[(key.clone(), tombstone_id, true)])?;
            (
                DeleteRetainedVersionOutcome::ReplacedCurrentWithTombstone {
                    version: tombstone_id,
                },
                Some(tombstone_id),
            )
        };
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
        if invalidation.is_some() {
            self.notify_local_invalidations();
        }
        Ok(outcome)
    }

    /// Resolves every requested head and immutable version descriptor from one
    /// local RocksDB snapshot without reading referenced blob payloads.
    pub async fn select_batch_get(
        &self,
        requests: &[(ObjectKey, Option<VersionId>)],
    ) -> BatchGetSelection {
        let commit_guard = self.commit_lock.lock().await;
        let entries = {
            let snapshot = self.db.snapshot();
            let mut identity_cache =
                BTreeMap::<(String, String), Result<BucketIdentity, MutationError>>::new();
            let mut entries = Vec::with_capacity(requests.len());
            for (key, requested_version) in requests {
                let cache_key = (key.tenant().to_owned(), key.bucket().to_owned());
                let identity = identity_cache
                    .entry(cache_key)
                    .or_insert_with(|| self.resolve_bucket_identity(key.tenant(), key.bucket()))
                    .clone();
                let selected = identity.and_then(|identity| {
                    let selected_head = match requested_version {
                        Some(_) => {
                            if self.bucket_versioning_by_key(&identity.encode())?
                                != ObjectVersioning::Enabled
                            {
                                return Err(MutationError::ObjectVersioningNotEnabled);
                            }
                            None
                        }
                        None => snapshot
                            .get_cf(self.cf(CF_HEADS)?, identity.head_key(key.path()))
                            .map_err(storage_error)?
                            .map(|bytes| {
                                serde_json::from_slice::<Head>(&bytes).map_err(storage_error)
                            })
                            .transpose()?,
                    };
                    let version_id = requested_version
                        .as_ref()
                        .copied()
                        .or_else(|| selected_head.as_ref().map(|head| head.version));
                    let Some(version_id) = version_id else {
                        return Ok(None);
                    };
                    let selected = snapshot
                        .get_cf(
                            self.cf(CF_VERSIONS)?,
                            version_key(identity, key, version_id),
                        )
                        .map_err(storage_error)?
                        .map(|bytes| {
                            serde_json::from_slice::<Version>(&bytes).map_err(storage_error)
                        })
                        .transpose()?;
                    let Some(version) = selected else {
                        return if selected_head.is_some() {
                            Err(MutationError::Storage(
                                "head references a missing version descriptor".into(),
                            ))
                        } else {
                            Ok(None)
                        };
                    };
                    match &selected_head {
                        Some(head) => validate_selected_head(head, &version)?,
                        None => validate_selected_version_id(version_id, &version)?,
                    }
                    Ok(Some(version))
                });
                entries.push((key.clone(), selected));
            }
            entries
        };
        drop(commit_guard);
        BatchGetSelection { entries }
    }

    /// Reads payloads for descriptors previously selected by
    /// [`Store::select_batch_get`]. Immutable descriptors are materialised
    /// after the short commit fence has already been released.
    pub async fn read_batch_get_selection(
        &self,
        selection: BatchGetSelection,
    ) -> Vec<Result<Option<Object>, MutationError>> {
        let BatchGetSelection { entries } = selection;
        let mut outcomes = Vec::with_capacity(entries.len());
        for (key, version) in entries {
            let outcome = match version {
                Ok(Some(version)) => match (&version.blob, version.deleted) {
                    (Some(blob), false) => self
                        .read_blob_bytes(blob)
                        .await
                        .map(|bytes| {
                            Some(Object {
                                key,
                                version,
                                bytes,
                            })
                        })
                        .map_err(storage_error),
                    (None, true) => Ok(Some(Object {
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
        let selection = self.select_batch_get(requests).await;
        self.read_batch_get_selection(selection).await
    }

    pub async fn stage_blob(&self, bytes: &[u8]) -> Result<BlobRef, MutationError> {
        if bytes.len() <= SMALL_BLOB_MAX_BYTES {
            let reference = blob_reference_for_bytes(bytes);
            let _commit_guard = self.commit_lock.lock().await;
            self.persist_small_blob_seal(&reference, bytes, now_unix_millis()?)?;
            return Ok(reference);
        }
        let mut upload = self.begin_blob_upload().await?;
        upload.write(bytes).await.map_err(storage_error)?;
        self.seal_blob_upload(upload).await
    }

    pub fn lock_manager(&self) -> LocalLockManager {
        self.program_locks.clone()
    }

    pub async fn begin_blob_upload(&self) -> Result<crate::BlobUpload, MutationError> {
        self.blobs.begin_upload().await.map_err(storage_error)
    }

    /// Seals one physical upload and records its single awaiting-publication
    /// reservation before returning it to the caller.
    pub async fn seal_blob_upload(
        &self,
        upload: crate::BlobUpload,
    ) -> Result<BlobRef, MutationError> {
        // Hashing, fsync, rename and parent-directory fsync are byte-plane IO,
        // so complete them before taking the short metadata commit fence.
        let reference = upload.finish().await.map_err(storage_error)?;
        let now = now_unix_millis()?;
        if is_small_blob(&reference) {
            let bytes = self.blobs.get(&reference).await.map_err(storage_error)?;
            {
                let _commit_guard = self.commit_lock.lock().await;
                self.persist_small_blob_seal(&reference, &bytes, now)?;
            }
            // A crash before this cleanup leaves only a normal untracked copy,
            // which the existing age-gated orphan scan removes.
            self.blobs.remove(&reference).map_err(storage_error)?;
        } else {
            let _commit_guard = self.commit_lock.lock().await;
            // GC may have removed a stale deduplication target while finish was
            // outside the fence. Never recreate lifecycle state without bytes.
            if !self
                .blobs
                .contains(&reference)
                .await
                .map_err(storage_error)?
            {
                return Err(MutationError::BlobNotFound);
            }
            self.reserve_sealed_blob(&reference, now)?;
        }
        Ok(reference)
    }

    pub(crate) async fn read_blob_bytes(
        &self,
        reference: &BlobRef,
    ) -> Result<Vec<u8>, MutationError> {
        if is_small_blob(reference) {
            let bytes = self
                .db
                .get_cf(self.cf(CF_SMALL_BLOBS)?, blob_reference_key(reference))
                .map_err(storage_error)?
                .ok_or(MutationError::BlobNotFound)?
                .to_vec();
            validate_small_blob(reference, &bytes)?;
            Ok(bytes)
        } else {
            self.blobs.get(reference).await.map_err(storage_error)
        }
    }

    async fn contains_blob(&self, reference: &BlobRef) -> Result<bool, MutationError> {
        if is_small_blob(reference) {
            let Some(bytes) = self
                .db
                .get_cf(self.cf(CF_SMALL_BLOBS)?, blob_reference_key(reference))
                .map_err(storage_error)?
            else {
                return Ok(false);
            };
            validate_small_blob(reference, &bytes)?;
            Ok(true)
        } else {
            self.blobs.contains(reference).await.map_err(storage_error)
        }
    }

    /// Returns the authoritative lifecycle state for one sealed blob.
    pub fn blob_reference_state(
        &self,
        reference: &BlobRef,
    ) -> Result<Option<BlobReferenceState>, MutationError> {
        self.read_blob_reference_state(&blob_reference_key(reference))
    }

    /// Removes every unreferenced blob and every awaiting blob whose inactivity
    /// has reached the configured TTL. The full metadata column family is
    /// streamed without retaining a second in-memory index.
    pub async fn collect_blob_garbage(&self) -> Result<u64, MutationError> {
        let _commit_guard = self.commit_lock.lock().await;
        self.collect_blob_garbage_at(now_unix_millis()?)
    }

    pub(crate) fn collect_blob_garbage_at(
        &self,
        now_unix_millis: u64,
    ) -> Result<u64, MutationError> {
        let references = self.cf(CF_BLOB_REFERENCES)?;
        let mut removed = 0_u64;
        for entry in self.db.iterator_cf(references, IteratorMode::Start) {
            let (key, encoded) = entry.map_err(storage_error)?;
            let reference = blob_reference_from_key(&key)?;
            let state = decode_blob_reference_state(&encoded)?;
            if !blob_reference_is_garbage(state, now_unix_millis, self.awaiting_publish_ttl_millis)
            {
                continue;
            }

            let mut batch = WriteBatch::default();
            if is_small_blob(&reference) {
                batch.delete_cf(self.cf(CF_SMALL_BLOBS)?, &key);
            } else {
                self.blobs.remove(&reference).map_err(storage_error)?;
            }
            batch.delete_cf(references, &key);
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
            removed = removed
                .checked_add(1)
                .ok_or_else(|| MutationError::Storage("blob GC count is exhausted".into()))?;
        }
        removed
            .checked_add(self.collect_untracked_blob_files_at(now_unix_millis)?)
            .ok_or_else(|| MutationError::Storage("blob GC count is exhausted".into()))
    }

    fn collect_untracked_blob_files_at(&self, now_unix_millis: u64) -> Result<u64, MutationError> {
        let mut removed = 0_u64;
        for entry in std::fs::read_dir(self.blobs.root()).map_err(storage_error)? {
            let entry = entry.map_err(storage_error)?;
            let file_type = entry.file_type().map_err(storage_error)?;
            let name = entry.file_name();
            if name.as_os_str() == std::ffi::OsStr::new(".staging") {
                if !file_type.is_dir() {
                    return Err(MutationError::Storage(
                        "blob staging path is not a directory".into(),
                    ));
                }
                for staged in std::fs::read_dir(entry.path()).map_err(storage_error)? {
                    let staged = staged.map_err(storage_error)?;
                    if !staged.file_type().map_err(storage_error)?.is_file() {
                        return Err(MutationError::Storage(
                            "blob staging directory contains a non-file entry".into(),
                        ));
                    }
                    let modified = staged
                        .metadata()
                        .map_err(storage_error)?
                        .modified()
                        .map_err(storage_error)?
                        .duration_since(UNIX_EPOCH)
                        .map_err(storage_error)?
                        .as_millis() as u64;
                    if now_unix_millis.saturating_sub(modified) < self.awaiting_publish_ttl_millis {
                        continue;
                    }
                    remove_file_and_sync_parent(&staged.path())?;
                    removed = removed.checked_add(1).ok_or_else(|| {
                        MutationError::Storage("blob GC count is exhausted".into())
                    })?;
                }
                continue;
            }
            if !file_type.is_dir() {
                return Err(MutationError::Storage(
                    "blob root contains an unexpected non-directory entry".into(),
                ));
            }
            let shard = name.to_str().ok_or_else(|| {
                MutationError::Storage("blob shard directory name is not UTF-8".into())
            })?;
            if shard.len() != 2 || !shard.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(MutationError::Storage(
                    "blob shard directory name is malformed".into(),
                ));
            }
            for file in std::fs::read_dir(entry.path()).map_err(storage_error)? {
                let file = file.map_err(storage_error)?;
                if !file.file_type().map_err(storage_error)?.is_file() {
                    return Err(MutationError::Storage(
                        "blob shard directory contains a non-file entry".into(),
                    ));
                }
                let reference = blob_reference_from_file(&file, shard)?;
                let modified = file
                    .metadata()
                    .map_err(storage_error)?
                    .modified()
                    .map_err(storage_error)?
                    .duration_since(UNIX_EPOCH)
                    .map_err(storage_error)?
                    .as_millis() as u64;
                if now_unix_millis.saturating_sub(modified) < self.awaiting_publish_ttl_millis {
                    continue;
                }
                if !is_small_blob(&reference) && self.blob_reference_state(&reference)?.is_some() {
                    continue;
                }
                self.blobs.remove(&reference).map_err(storage_error)?;
                removed = removed
                    .checked_add(1)
                    .ok_or_else(|| MutationError::Storage("blob GC count is exhausted".into()))?;
            }
        }
        Ok(removed)
    }

    fn prepare_sealed_blob_reservation(
        &self,
        reference: &BlobRef,
        now_unix_millis: u64,
    ) -> Result<Option<BlobReferenceState>, MutationError> {
        let key = blob_reference_key(reference);
        let current = self.read_blob_reference_state(&key)?;
        if let Some(current) = current {
            validate_blob_reference_state(current)?;
        }
        let next = match current {
            None => BlobReferenceState {
                ref_count: 1,
                flags: AWAITING_PUBLISH,
                created_at: now_unix_millis,
                updated_at: now_unix_millis,
            },
            Some(mut current) if current.ref_count == 0 => {
                current.ref_count = 1;
                current.flags = AWAITING_PUBLISH;
                current.updated_at = current.updated_at.max(now_unix_millis);
                current
            }
            Some(mut current) => {
                if current.flags & AWAITING_PUBLISH == 0 {
                    current.updated_at = current.updated_at.max(now_unix_millis);
                    return Ok(Some(current));
                }
                if current.ref_count != 1 {
                    return Err(MutationError::Storage(
                        "awaiting-publish blob must have exactly one reservation".into(),
                    ));
                }
                current.updated_at = current.updated_at.max(now_unix_millis);
                current
            }
        };
        Ok(Some(next))
    }

    fn reserve_sealed_blob(
        &self,
        reference: &BlobRef,
        now_unix_millis: u64,
    ) -> Result<(), MutationError> {
        let Some(next) = self.prepare_sealed_blob_reservation(reference, now_unix_millis)? else {
            return Ok(());
        };
        let key = blob_reference_key(reference);
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .put_cf_opt(
                self.cf(CF_BLOB_REFERENCES)?,
                key,
                encode_blob_reference_state(next),
                &options,
            )
            .map_err(storage_error)
    }

    fn persist_small_blob_seal(
        &self,
        reference: &BlobRef,
        bytes: &[u8],
        now_unix_millis: u64,
    ) -> Result<(), MutationError> {
        validate_small_blob(reference, bytes)?;
        let pending = BTreeSet::new();
        let value = self.prepare_small_blob_value(reference, bytes, &pending)?;
        let state = self
            .prepare_sealed_blob_reservation(reference, now_unix_millis)?
            .ok_or_else(|| MutationError::Storage("small blob reservation is missing".into()))?;
        let key = blob_reference_key(reference);
        let mut batch = WriteBatch::default();
        if let Some((small_key, small_bytes)) = value {
            batch.put_cf(self.cf(CF_SMALL_BLOBS)?, &small_key, small_bytes);
        }
        batch.put_cf(
            self.cf(CF_BLOB_REFERENCES)?,
            key,
            encode_blob_reference_state(state),
        );
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)
    }

    pub(crate) fn prepare_blob_reference_publication(
        &self,
        reference: &BlobRef,
        pending: &PendingBlobReferences,
        now_unix_millis: u64,
    ) -> Result<(Vec<u8>, BlobReferenceState), MutationError> {
        let key = blob_reference_key(reference);
        let state = match pending.get(&key).copied() {
            Some(state) => state,
            None => self
                .read_blob_reference_state(&key)?
                .ok_or(MutationError::BlobNotFound)?,
        };
        advance_blob_reference_publication(state, now_unix_millis).map(|state| (key, state))
    }

    /// Publishes bytes materialised by an inline put. Small bytes join the
    /// final RocksDB batch; large bytes are already durable in the byte plane.
    /// Neither needs a separate awaiting-publication lifecycle write.
    fn prepare_materialized_blob_publication(
        &self,
        reference: &BlobRef,
        pending: &PendingBlobReferences,
        now_unix_millis: u64,
    ) -> Result<(Vec<u8>, BlobReferenceState), MutationError> {
        let key = blob_reference_key(reference);
        let state = match pending.get(&key).copied() {
            Some(state) => advance_blob_reference_publication(state, now_unix_millis)?,
            None => match self.read_blob_reference_state(&key)? {
                Some(state) => advance_blob_reference_publication(state, now_unix_millis)?,
                None => BlobReferenceState {
                    ref_count: 1,
                    flags: 0,
                    created_at: now_unix_millis,
                    updated_at: now_unix_millis,
                },
            },
        };
        Ok((key, state))
    }

    fn prepare_small_blob_value(
        &self,
        reference: &BlobRef,
        bytes: &[u8],
        pending: &BTreeSet<Vec<u8>>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, MutationError> {
        validate_small_blob(reference, bytes)?;
        let key = blob_reference_key(reference);
        if pending.contains(&key) {
            return Ok(None);
        }
        let existing = self
            .db
            .get_cf(self.cf(CF_SMALL_BLOBS)?, &key)
            .map_err(storage_error)?;
        match existing {
            Some(existing) => {
                validate_small_blob(reference, &existing)?;
                if existing.as_slice() != bytes {
                    return Err(MutationError::Storage(
                        "small blob content-address collision".into(),
                    ));
                }
                Ok(None)
            }
            None => Ok(Some((key, bytes.to_vec()))),
        }
    }

    /// Stages the lifecycle half of a future immutable-version retirement.
    /// The caller must put this update in the same RocksDB batch that removes
    /// the corresponding version descriptor.
    #[allow(dead_code)]
    pub(crate) fn prepare_blob_reference_retirement(
        &self,
        reference: &BlobRef,
        pending: &PendingBlobReferences,
        now_unix_millis: u64,
    ) -> Result<(Vec<u8>, BlobReferenceState), MutationError> {
        let key = blob_reference_key(reference);
        let mut state = match pending.get(&key).copied() {
            Some(state) => state,
            None => self.read_blob_reference_state(&key)?.ok_or_else(|| {
                MutationError::Storage(
                    "retired version references missing blob lifecycle metadata".into(),
                )
            })?,
        };
        validate_blob_reference_state(state)?;
        if state.flags & AWAITING_PUBLISH != 0 || state.ref_count == 0 {
            return Err(MutationError::Storage(
                "retired version has no published blob reference".into(),
            ));
        }
        state.ref_count -= 1;
        state.updated_at = state.updated_at.max(now_unix_millis);
        Ok((key, state))
    }

    /// Releases the one generic awaiting-publication reservation created when
    /// a prepared-program bundle was sealed. If these bytes were already
    /// published, sealing only refreshed their inactivity timestamp and there
    /// is no temporary reference to remove.
    pub(crate) fn prepare_awaiting_blob_release(
        &self,
        reference: &BlobRef,
        pending: &PendingBlobReferences,
        now_unix_millis: u64,
    ) -> Result<Option<(Vec<u8>, BlobReferenceState)>, MutationError> {
        let key = blob_reference_key(reference);
        let mut state = match pending.get(&key).copied() {
            Some(state) => state,
            None => self.read_blob_reference_state(&key)?.ok_or_else(|| {
                MutationError::Storage(
                    "prepared bundle references missing blob lifecycle metadata".into(),
                )
            })?,
        };
        validate_blob_reference_state(state)?;
        if state.flags & AWAITING_PUBLISH == 0 {
            return Ok(None);
        }
        if state.ref_count != 1 {
            return Err(MutationError::Storage(
                "awaiting-publish blob must have exactly one reservation".into(),
            ));
        }
        state.ref_count = 0;
        state.flags = 0;
        state.updated_at = state.updated_at.max(now_unix_millis);
        Ok(Some((key, state)))
    }

    pub(crate) fn stage_blob_reference_update(
        &self,
        batch: &mut WriteBatch,
        pending: &mut PendingBlobReferences,
        key: Vec<u8>,
        state: BlobReferenceState,
    ) -> Result<(), MutationError> {
        batch.put_cf(
            self.cf(CF_BLOB_REFERENCES)?,
            &key,
            encode_blob_reference_state(state),
        );
        pending.insert(key, state);
        Ok(())
    }

    fn read_blob_reference_state(
        &self,
        key: &[u8],
    ) -> Result<Option<BlobReferenceState>, MutationError> {
        self.db
            .get_cf(self.cf(CF_BLOB_REFERENCES)?, key)
            .map_err(storage_error)?
            .map(|encoded| decode_blob_reference_state(&encoded))
            .transpose()
    }

    pub async fn open_blob(&self, reference: &BlobRef) -> Result<BlobReader, MutationError> {
        if is_small_blob(reference) {
            BlobReader::from_bytes(reference, self.read_blob_bytes(reference).await?)
                .map_err(storage_error)
        } else {
            self.blobs
                .open_verified(reference)
                .await
                .map_err(storage_error)
        }
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
        let mut identity_cache =
            BTreeMap::<(String, String), Result<BucketIdentity, MutationError>>::new();
        for (index, operation) in operations.into_iter().enumerate() {
            let logical_key = match &operation {
                BatchOperation::Put(request) => &request.key,
                BatchOperation::Publish(request) => &request.key,
                BatchOperation::Delete(request) => &request.key,
            };
            let cache_key = (
                logical_key.tenant().to_owned(),
                logical_key.bucket().to_owned(),
            );
            let identity = identity_cache
                .entry(cache_key)
                .or_insert_with(|| {
                    self.resolve_bucket_identity(logical_key.tenant(), logical_key.bucket())
                })
                .clone();
            let result = match identity {
                Ok(identity) => self.prepare(operation, identity).await,
                Err(error) => Err(error),
            };
            match result {
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
        let now = match now_unix_millis() {
            Ok(now) => now,
            Err(error) => {
                return fail_prepared_operations(early, prepared, error);
            }
        };
        let mut receipt_status = match self.mutation_receipt_status() {
            Ok(status) => status,
            Err(error) => {
                return fail_prepared_operations(early, prepared, error);
            }
        };
        let initial_receipt_status = receipt_status;
        let pruned_receipts =
            match self.stage_expired_mutation_receipts(&mut batch, now, &mut receipt_status) {
                Ok(pruned) => pruned,
                Err(error) => {
                    return fail_prepared_operations(early, prepared, error);
                }
            };
        let mut pending_heads = BTreeMap::<Vec<u8>, Head>::new();
        let mut pending_versions = BTreeMap::<Vec<u8>, Version>::new();
        let mut pending_receipts = BTreeMap::<Vec<u8>, StoredReceipt>::new();
        let mut pending_blob_references = PendingBlobReferences::new();
        let mut pending_small_blobs = BTreeSet::<Vec<u8>>::new();
        let mut policy_cache = BTreeMap::<Vec<u8>, Result<BucketPolicy, MutationError>>::new();
        let mut versioning_cache =
            BTreeMap::<Vec<u8>, Result<ObjectVersioning, MutationError>>::new();
        let mut results = BTreeMap::<usize, Result<MutationReceipt, MutationError>>::new();
        let mut batch_high_watermark = None;
        let mut pending_invalidations = Vec::new();
        for (index, operation) in prepared {
            let outcome = self
                .evaluate_operation(
                    &operation,
                    &mut batch,
                    &mut pending_heads,
                    &mut pending_versions,
                    &mut pending_receipts,
                    &mut pending_blob_references,
                    &mut pending_small_blobs,
                    &mut policy_cache,
                    &mut versioning_cache,
                    &pruned_receipts,
                    &mut receipt_status,
                    now,
                )
                .await;
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
            if receipt_status != initial_receipt_status {
                self.stage_mutation_receipt_status(&mut batch, receipt_status)?;
            }
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
        match persistence {
            Ok(()) => {
                if !pending_invalidations.is_empty() {
                    self.notify_local_invalidations();
                }
            }
            Err(error) => {
                let message = error.to_string();
                for result in results.values_mut() {
                    if result.is_ok() {
                        *result = Err(MutationError::Storage(message.clone()));
                    }
                }
            }
        }
        results.extend(early.into_iter().map(|(index, error)| (index, Err(error))));
        results
            .into_iter()
            .map(|(index, result)| BatchOutcome { index, result })
            .collect()
    }

    pub(crate) fn stage_local_invalidations(
        &self,
        batch: &mut WriteBatch,
        changes: &[(ObjectKey, VersionId, bool)],
    ) -> Result<(), MutationError> {
        if changes.is_empty() {
            return Ok(());
        }

        let journal = self.cf(CF_LOCAL_INVALIDATIONS)?;
        let metadata = self.cf(CF_METADATA)?;
        let mut status = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let old_tail = status.tail;
        let first_old_key = invalidation_key(status.retention_floor.saturating_add(1));
        let mut old_entries = self.db.iterator_cf(
            journal,
            IteratorMode::From(&first_old_key, Direction::Forward),
        );
        let mut appended = VecDeque::new();
        for (key, version, deleted) in changes {
            status.tail = status.tail.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation offset is exhausted".into())
            })?;
            let invalidation = LocalInvalidation::new(status.tail, key.clone(), *version, *deleted);
            let encoded = serde_json::to_vec(&invalidation).map_err(storage_error)?;
            status.retained_entries = status.retained_entries.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation entry count is exhausted".into())
            })?;
            status.retained_bytes = status
                .retained_bytes
                .checked_add(invalidation_record_bytes(encoded.len()))
                .ok_or_else(|| {
                    MutationError::Storage("local invalidation byte count is exhausted".into())
                })?;
            appended.push_back((status.tail, encoded));
        }

        while status.retained_entries > self.watch_retention.max_entries
            || status.retained_bytes > self.watch_retention.max_bytes
        {
            let pruned = status.retention_floor.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation retention floor is exhausted".into())
            })?;
            let encoded = if pruned <= old_tail {
                let (stored_key, encoded) = old_entries
                    .next()
                    .ok_or_else(|| {
                        MutationError::Storage(format!(
                            "retained local invalidation offset {pruned} is missing"
                        ))
                    })?
                    .map_err(storage_error)?;
                if offset_from_key(&stored_key) != Some(pruned) {
                    return Err(MutationError::Storage(format!(
                        "retained local invalidation offset {pruned} is missing"
                    )));
                }
                encoded.to_vec()
            } else {
                let (offset, encoded) = appended.pop_front().ok_or_else(|| {
                    MutationError::Storage(
                        "local invalidation retention accounting is inconsistent".into(),
                    )
                })?;
                if offset != pruned {
                    return Err(MutationError::Storage(
                        "local invalidation retention offsets are inconsistent".into(),
                    ));
                }
                encoded
            };
            batch.delete_cf(journal, invalidation_key(pruned));
            status.retention_floor = pruned;
            status.retained_entries -= 1;
            status.retained_bytes = status
                .retained_bytes
                .checked_sub(invalidation_record_bytes(encoded.len()))
                .ok_or_else(|| {
                    MutationError::Storage(
                        "local invalidation byte accounting is inconsistent".into(),
                    )
                })?;
        }
        for (offset, encoded) in appended {
            batch.put_cf(journal, invalidation_key(offset), encoded);
        }
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_OFFSET_KEY,
            status.tail.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_FLOOR_KEY,
            status.retention_floor.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_COUNT_KEY,
            status.retained_entries.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_BYTES_KEY,
            status.retained_bytes.to_be_bytes(),
        );
        Ok(())
    }

    pub(crate) fn notify_local_invalidations(&self) {
        self.watch_notify.send_replace(());
    }

    fn enforce_local_watch_retention(&self) -> Result<(), WatchError> {
        let journal = self
            .cf(CF_LOCAL_INVALIDATIONS)
            .map_err(|error| WatchError::Storage(error.to_string()))?;
        let metadata = self
            .cf(CF_METADATA)
            .map_err(|error| WatchError::Storage(error.to_string()))?;
        let mut status = self.local_watch_status()?;
        if status.retained_entries <= self.watch_retention.max_entries
            && status.retained_bytes <= self.watch_retention.max_bytes
        {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        while status.retained_entries > self.watch_retention.max_entries
            || status.retained_bytes > self.watch_retention.max_bytes
        {
            let offset = status.retention_floor.checked_add(1).ok_or_else(|| {
                WatchError::Storage("local invalidation retention floor is exhausted".into())
            })?;
            let encoded = self
                .db
                .get_cf(journal, invalidation_key(offset))
                .map_err(|error| WatchError::Storage(error.to_string()))?
                .ok_or_else(|| {
                    WatchError::Storage(format!(
                        "retained local invalidation offset {offset} is missing"
                    ))
                })?;
            batch.delete_cf(journal, invalidation_key(offset));
            status.retention_floor = offset;
            status.retained_entries -= 1;
            status.retained_bytes = status
                .retained_bytes
                .checked_sub(invalidation_record_bytes(encoded.len()))
                .ok_or_else(|| {
                    WatchError::Storage("local invalidation byte accounting is inconsistent".into())
                })?;
        }
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_FLOOR_KEY,
            status.retention_floor.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_COUNT_KEY,
            status.retained_entries.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_BYTES_KEY,
            status.retained_bytes.to_be_bytes(),
        );
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .write_opt(batch, &options)
            .map_err(|error| WatchError::Storage(error.to_string()))
    }

    async fn prepare(
        &self,
        operation: BatchOperation,
        identity: BucketIdentity,
    ) -> Result<PreparedOperation, MutationError> {
        match operation {
            BatchOperation::Put(mut request) => {
                validate_command_id(request.command_id.as_deref())?;
                require_local_durability(request.durability)?;
                let bytes = std::mem::take(&mut request.bytes);
                let payload = if bytes.len() <= SMALL_BLOB_MAX_BYTES {
                    let reference = blob_reference_for_bytes(&bytes);
                    PreparedPayload::Small { reference, bytes }
                } else {
                    PreparedPayload::Large(self.blobs.put(&bytes).await.map_err(storage_error)?)
                };
                let fingerprint = put_fingerprint(
                    &identity.head_key(request.key.path()),
                    request.mode,
                    request.content_type.as_deref(),
                    request.durability,
                    payload.reference(),
                );
                Ok(PreparedOperation::Put {
                    request,
                    identity,
                    payload,
                    fingerprint,
                })
            }
            BatchOperation::Publish(request) => {
                validate_command_id(request.command_id.as_deref())?;
                require_local_durability(request.durability)?;
                if !self.contains_blob(&request.blob).await? {
                    return Err(MutationError::BlobNotFound);
                }
                let fingerprint = publish_fingerprint(&request, identity);
                Ok(PreparedOperation::Publish {
                    request,
                    identity,
                    fingerprint,
                })
            }
            BatchOperation::Delete(request) => {
                validate_command_id(request.command_id.as_deref())?;
                require_local_durability(request.durability)?;
                let fingerprint = delete_fingerprint(&request, identity);
                Ok(PreparedOperation::Delete {
                    request,
                    identity,
                    fingerprint,
                })
            }
        }
    }

    fn mutation_receipt_status(&self) -> Result<MutationReceiptStatus, MutationError> {
        let metadata = self.cf(CF_METADATA)?;
        let read = |key: &[u8]| {
            self.db
                .get_cf(metadata, key)
                .map_err(storage_error)?
                .ok_or_else(|| {
                    MutationError::Storage("mutation receipt metadata is missing".into())
                })
                .and_then(|encoded| decode_offset(&encoded))
        };
        Ok(MutationReceiptStatus {
            entries: read(MUTATION_RECEIPT_COUNT_KEY)?,
            bytes: read(MUTATION_RECEIPT_BYTES_KEY)?,
        })
    }

    fn stage_expired_mutation_receipts(
        &self,
        batch: &mut WriteBatch,
        now_unix_millis: u64,
        status: &mut MutationReceiptStatus,
    ) -> Result<BTreeSet<Vec<u8>>, MutationError> {
        let receipts = self.cf(CF_RECEIPTS)?;
        let mut pruned = BTreeSet::new();
        let iterator = self.db.iterator_cf(
            receipts,
            IteratorMode::From(
                &[STORAGE_KEY_FORMAT_VERSION, RECEIPT_EXPIRY_PREFIX],
                Direction::Forward,
            ),
        );
        for entry in iterator {
            let (index_key, _) = entry.map_err(storage_error)?;
            let Some((expires_at, primary_key)) = parse_receipt_expiry_key(&index_key)? else {
                break;
            };
            if expires_at > now_unix_millis {
                break;
            }
            if pruned.contains(&primary_key) {
                return Err(MutationError::Storage(
                    "mutation receipt has duplicate expiry indexes".into(),
                ));
            }
            let encoded = self
                .db
                .get_cf(receipts, &primary_key)
                .map_err(storage_error)?
                .ok_or_else(|| {
                    MutationError::Storage(
                        "mutation receipt expiry index references a missing receipt".into(),
                    )
                })?;
            let receipt =
                serde_json::from_slice::<StoredReceipt>(&encoded).map_err(storage_error)?;
            if receipt.expires_at_unix_millis != expires_at {
                return Err(MutationError::Storage(
                    "mutation receipt expiry index disagrees with its receipt".into(),
                ));
            }
            let logical_bytes =
                mutation_receipt_logical_bytes(primary_key.len(), encoded.len(), index_key.len());
            status.entries = status.entries.checked_sub(1).ok_or_else(|| {
                MutationError::Storage("mutation receipt count is inconsistent".into())
            })?;
            status.bytes = status.bytes.checked_sub(logical_bytes).ok_or_else(|| {
                MutationError::Storage("mutation receipt byte accounting is inconsistent".into())
            })?;
            batch.delete_cf(receipts, &primary_key);
            batch.delete_cf(receipts, &index_key);
            pruned.insert(primary_key);
        }
        Ok(pruned)
    }

    #[allow(clippy::too_many_arguments)]
    fn stage_mutation_receipt(
        &self,
        batch: &mut WriteBatch,
        primary_key: Option<Vec<u8>>,
        fingerprint: [u8; 32],
        version: VersionId,
        deleted: bool,
        now_unix_millis: u64,
        status: &mut MutationReceiptStatus,
        pending_receipts: &mut BTreeMap<Vec<u8>, StoredReceipt>,
    ) -> Result<u64, MutationError> {
        let Some(primary_key) = primary_key else {
            return Ok(0);
        };
        let expires_at_unix_millis = now_unix_millis
            .checked_add(self.mutation_receipt_retention.retention_millis())
            .ok_or_else(|| MutationError::Storage("mutation receipt expiry overflow".into()))?;
        let stored = StoredReceipt {
            fingerprint,
            version,
            deleted,
            expires_at_unix_millis,
        };
        let encoded = serde_json::to_vec(&stored).map_err(storage_error)?;
        let expiry_key = receipt_expiry_key(expires_at_unix_millis, &primary_key)?;
        let logical_bytes =
            mutation_receipt_logical_bytes(primary_key.len(), encoded.len(), expiry_key.len());
        let next_entries = status
            .entries
            .checked_add(1)
            .ok_or_else(|| MutationError::Storage("mutation receipt count is exhausted".into()))?;
        let next_bytes = status.bytes.checked_add(logical_bytes).ok_or_else(|| {
            MutationError::Storage("mutation receipt byte accounting is exhausted".into())
        })?;
        if next_entries > self.mutation_receipt_retention.max_entries
            || next_bytes > self.mutation_receipt_retention.max_bytes
        {
            return Err(MutationError::ReceiptCapacity);
        }
        batch.put_cf(self.cf(CF_RECEIPTS)?, &primary_key, encoded);
        batch.put_cf(self.cf(CF_RECEIPTS)?, expiry_key, []);
        pending_receipts.insert(primary_key, stored);
        status.entries = next_entries;
        status.bytes = next_bytes;
        Ok(expires_at_unix_millis)
    }

    fn stage_mutation_receipt_status(
        &self,
        batch: &mut WriteBatch,
        status: MutationReceiptStatus,
    ) -> Result<(), MutationError> {
        let metadata = self.cf(CF_METADATA)?;
        batch.put_cf(
            metadata,
            MUTATION_RECEIPT_COUNT_KEY,
            status.entries.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            MUTATION_RECEIPT_BYTES_KEY,
            status.bytes.to_be_bytes(),
        );
        Ok(())
    }

    async fn evaluate_operation(
        &self,
        operation: &PreparedOperation,
        batch: &mut WriteBatch,
        pending_heads: &mut BTreeMap<Vec<u8>, Head>,
        pending_versions: &mut BTreeMap<Vec<u8>, Version>,
        pending_receipts: &mut BTreeMap<Vec<u8>, StoredReceipt>,
        pending_blob_references: &mut PendingBlobReferences,
        pending_small_blobs: &mut BTreeSet<Vec<u8>>,
        policy_cache: &mut BTreeMap<Vec<u8>, Result<BucketPolicy, MutationError>>,
        versioning_cache: &mut BTreeMap<Vec<u8>, Result<ObjectVersioning, MutationError>>,
        pruned_receipts: &BTreeSet<Vec<u8>>,
        receipt_status: &mut MutationReceiptStatus,
        now_unix_millis: u64,
    ) -> Result<MutationReceipt, MutationError> {
        let key = operation.key();
        let encoded_key = operation.encoded_head_key();
        let receipt_key = operation
            .command_id()
            .map(|command_id| receipt_key(operation.identity(), command_id));
        if let Some(receipt_key) = receipt_key.as_ref() {
            let existing = match pending_receipts.get(receipt_key) {
                Some(receipt) => Some(receipt.clone()),
                None if pruned_receipts.contains(receipt_key) => None,
                None => self.read_json(CF_RECEIPTS, receipt_key)?,
            };
            if let Some(existing) = existing {
                if existing.expires_at_unix_millis <= now_unix_millis {
                    return Err(MutationError::Storage(
                        "expired mutation receipt escaped pruning".into(),
                    ));
                }
                if existing.fingerprint != operation.fingerprint() {
                    return Err(MutationError::IdempotencyConflict);
                }
                return Ok(MutationReceipt {
                    command_id: operation.command_id().map(str::to_owned),
                    fingerprint: existing.fingerprint,
                    version: existing.version,
                    deleted: existing.deleted,
                    replayed: true,
                    replay_guarantee_expires_at_unix_millis: existing.expires_at_unix_millis,
                });
            }
        }

        let current = match pending_heads.get(&encoded_key) {
            Some(head) => Some(head.clone()),
            None => self.head_by_storage_key(&encoded_key)?,
        };
        let current_version = match current.as_ref() {
            Some(head) => match pending_versions.get(&encoded_key) {
                Some(version) => Some(version.clone()),
                None => Some(
                    self.version_metadata_by_identity(operation.identity(), key, head.version)?
                        .ok_or_else(|| {
                            MutationError::Storage("head references a missing version".into())
                        })?,
                ),
            },
            None => None,
        };
        if current_version
            .as_ref()
            .zip(current.as_ref())
            .is_some_and(|(version, head)| {
                version.id != head.version || version.deleted != head.deleted
            })
        {
            return Err(MutationError::Storage(
                "head and current version descriptor disagree".into(),
            ));
        }
        let encoded_bucket = operation.identity().encode().to_vec();
        let policy = policy_cache
            .entry(encoded_bucket.clone())
            .or_insert_with(|| {
                self.bucket_policy_by_key(&encoded_bucket)
                    .map(Option::unwrap_or_default)
            })
            .as_ref()
            .map_err(Clone::clone)?;
        let versioning = *versioning_cache
            .entry(encoded_bucket)
            .or_insert_with(|| self.bucket_versioning_by_key(&operation.identity().encode()))
            .as_ref()
            .map_err(Clone::clone)?;
        let program_definition = is_program_definition_path(key.path());
        if policy.is_program_only(key.path()) && !program_definition {
            return Err(MutationError::ProgramConcurrencyViolation);
        }
        let immutable_path = policy.is_immutable(key.path()) || program_definition;
        match operation.put_mode() {
            Some(PutMode::PutImmutable) if !immutable_path => {
                return Err(MutationError::ImmutablePolicyRequired);
            }
            Some(PutMode::PutImmutable) => {
                // Handled below: publish once or return an identical-content
                // semantic replay without advancing the path version.
            }
            Some(_) | None if immutable_path => {
                return Err(MutationError::Immutable);
            }
            Some(_) | None => {}
        }
        if matches!(operation.put_mode(), Some(PutMode::PutImmutable)) {
            if let Some(current) = current.as_ref() {
                let existing = current_version.as_ref().ok_or_else(|| {
                    MutationError::Storage("head references a missing version".into())
                })?;
                let requested_payload = match operation {
                    PreparedOperation::Put { payload, .. } => payload.reference().clone(),
                    PreparedOperation::Publish { request, .. } => request.blob.clone(),
                    PreparedOperation::Delete { .. } => unreachable!(),
                };
                let requested_content_type = match operation {
                    PreparedOperation::Put { request, .. } => request.content_type.as_ref(),
                    PreparedOperation::Publish { request, .. } => request.content_type.as_ref(),
                    PreparedOperation::Delete { .. } => unreachable!(),
                };
                if !current.deleted
                    && version_blob_reference(existing)?.as_ref() == Some(&requested_payload)
                    && existing.content_type.as_ref() == requested_content_type
                {
                    let fingerprint = operation.fingerprint();
                    let expires_at = self.stage_mutation_receipt(
                        batch,
                        receipt_key,
                        fingerprint,
                        current.version,
                        false,
                        now_unix_millis,
                        receipt_status,
                        pending_receipts,
                    )?;
                    return Ok(MutationReceipt {
                        command_id: operation.command_id().map(str::to_owned),
                        fingerprint,
                        version: current.version,
                        deleted: false,
                        replayed: true,
                        replay_guarantee_expires_at_unix_millis: expires_at,
                    });
                }
                return Err(MutationError::Immutable);
            }
        }
        check_precondition(operation.precondition(), current.as_ref())?;

        let id = self.clock.next().map_err(storage_error)?;
        let deleted = matches!(operation, PreparedOperation::Delete { .. });
        let new_blob = match operation {
            PreparedOperation::Put { payload, .. } => Some(payload.reference().clone()),
            PreparedOperation::Publish { request, .. } => Some(request.blob.clone()),
            PreparedOperation::Delete { .. } => None,
        };
        if let PreparedOperation::Put { payload, .. } = operation
            && payload.small_bytes().is_none()
            && !self.contains_blob(payload.reference()).await?
        {
            return Err(MutationError::BlobNotFound);
        }
        let version = Version {
            id,
            blob: new_blob.clone(),
            content_type: match operation {
                PreparedOperation::Put { request, .. } => request.content_type.clone(),
                PreparedOperation::Publish { request, .. } => request.content_type.clone(),
                PreparedOperation::Delete { .. } => None,
            },
            deleted,
            committed_at_unix_millis: now_unix_millis,
        };
        let head = Head {
            version: id,
            deleted,
        };
        let encoded_version = serde_json::to_vec(&version).map_err(storage_error)?;
        let encoded_head = serde_json::to_vec(&head).map_err(storage_error)?;
        let versions = self.cf(CF_VERSIONS)?;
        let heads = self.cf(CF_HEADS)?;
        let encoded_version_key = version_key(operation.identity(), key, id);
        let fingerprint = operation.fingerprint();
        let old_blob = current_version
            .as_ref()
            .map(version_blob_reference)
            .transpose()?
            .flatten();
        let mut blob_reference_updates = Vec::with_capacity(2);
        let references_changed = old_blob.as_ref() != new_blob.as_ref();
        if versioning == ObjectVersioning::Unversioned && references_changed {
            if let Some(reference) = old_blob.as_ref() {
                blob_reference_updates.push(self.prepare_blob_reference_retirement(
                    reference,
                    pending_blob_references,
                    now_unix_millis,
                )?);
            }
        }
        let small_blob_value = match operation {
            PreparedOperation::Put { payload, .. } => match payload.small_bytes() {
                Some(bytes) => {
                    self.prepare_small_blob_value(payload.reference(), bytes, pending_small_blobs)?
                }
                None => None,
            },
            PreparedOperation::Publish { .. } | PreparedOperation::Delete { .. } => None,
        };
        if let Some(reference) = new_blob.as_ref()
            && (versioning == ObjectVersioning::Enabled || references_changed)
        {
            let update = match operation {
                PreparedOperation::Put { .. } => self.prepare_materialized_blob_publication(
                    reference,
                    pending_blob_references,
                    now_unix_millis,
                )?,
                PreparedOperation::Publish { .. } => self.prepare_blob_reference_publication(
                    reference,
                    pending_blob_references,
                    now_unix_millis,
                )?,
                PreparedOperation::Delete { .. } => unreachable!(),
            };
            blob_reference_updates.push(update);
        }
        let expires_at = self.stage_mutation_receipt(
            batch,
            receipt_key,
            fingerprint,
            id,
            deleted,
            now_unix_millis,
            receipt_status,
            pending_receipts,
        )?;
        if let Some((key, bytes)) = small_blob_value {
            batch.put_cf(self.cf(CF_SMALL_BLOBS)?, &key, bytes);
            pending_small_blobs.insert(key);
        }
        for (key, state) in blob_reference_updates {
            self.stage_blob_reference_update(batch, pending_blob_references, key, state)?;
        }
        if versioning == ObjectVersioning::Unversioned
            && let Some(previous) = current_version.as_ref()
        {
            batch.delete_cf(
                versions,
                version_key(operation.identity(), key, previous.id),
            );
        }
        batch.put_cf(versions, encoded_version_key, encoded_version);
        batch.put_cf(heads, &encoded_key, encoded_head);
        pending_heads.insert(encoded_key.clone(), head);
        pending_versions.insert(encoded_key, version);
        Ok(MutationReceipt {
            command_id: operation.command_id().map(str::to_owned),
            fingerprint,
            version: id,
            deleted,
            replayed: false,
            replay_guarantee_expires_at_unix_millis: expires_at,
        })
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
            mode: match precondition {
                Precondition::Any => PutMode::Put,
                Precondition::Absent => PutMode::PutIfAbsent,
                Precondition::Version(version) => PutMode::PutIfVersion(version),
            },
            command_id: Some(command.into()),
            durability: Durability::Local,
        }
    }

    fn immutable_put(path: &str, bytes: &[u8], command: &str) -> PutRequest {
        PutRequest {
            key: key(path),
            bytes: bytes.to_vec(),
            content_type: Some("application/octet-stream".into()),
            mode: PutMode::PutImmutable,
            command_id: Some(command.into()),
            durability: Durability::Local,
        }
    }

    fn publish(path: &str, blob: BlobRef, command: &str) -> PublishRequest {
        PublishRequest {
            key: key(path),
            blob,
            content_type: Some("application/octet-stream".into()),
            mode: PutMode::Put,
            command_id: Some(command.into()),
            durability: Durability::Local,
        }
    }

    fn blob_file_path(store: &Store, reference: &BlobRef) -> PathBuf {
        let hash = hex::encode(reference.hash);
        store.blobs.root().join(&hash[..2]).join(hash)
    }

    #[test]
    fn blob_reference_state_is_exactly_twenty_five_bytes() {
        let state = BlobReferenceState {
            ref_count: 1,
            flags: AWAITING_PUBLISH,
            created_at: 11,
            updated_at: 13,
        };
        let encoded = encode_blob_reference_state(state);
        assert_eq!(encoded.len(), 25);
        assert_eq!(decode_blob_reference_state(&encoded).unwrap(), state);

        let mut unknown_flag = encoded;
        unknown_flag[8] = 1 << 7;
        assert!(matches!(
            decode_blob_reference_state(&unknown_flag),
            Err(MutationError::Storage(message)) if message.contains("unknown flags")
        ));
        assert!(decode_blob_reference_state(&encoded[..24]).is_err());

        let mut invalid_reservation = encoded;
        invalid_reservation[..8].copy_from_slice(&2_u64.to_be_bytes());
        assert!(matches!(
            decode_blob_reference_state(&invalid_reservation),
            Err(MutationError::Storage(message))
                if message.contains("exactly one reservation")
        ));
    }

    #[tokio::test]
    async fn sealing_creates_one_reservation_and_reuse_only_refreshes_it() {
        let (_temporary, store) = store().await;
        let blob = store.stage_blob(b"sealed once").await.unwrap();
        let first = store.blob_reference_state(&blob).unwrap().unwrap();
        assert_eq!(first.ref_count, 1);
        assert_eq!(first.flags, AWAITING_PUBLISH);
        assert_eq!(first.created_at, first.updated_at);
        assert_eq!(
            store
                .db
                .get_cf(
                    store.cf(CF_BLOB_REFERENCES).unwrap(),
                    blob_reference_key(&blob),
                )
                .unwrap()
                .unwrap()
                .len(),
            25
        );

        store
            .reserve_sealed_blob(&blob, first.updated_at + 10)
            .unwrap();
        let refreshed = store.blob_reference_state(&blob).unwrap().unwrap();
        assert_eq!(refreshed.ref_count, 1);
        assert_eq!(refreshed.flags, AWAITING_PUBLISH);
        assert_eq!(refreshed.created_at, first.created_at);
        assert_eq!(refreshed.updated_at, first.updated_at + 10);
    }

    #[tokio::test]
    async fn streamed_seal_finishes_byte_plane_io_before_waiting_for_commit_fence() {
        let (_temporary, store) = store().await;
        let bytes = vec![0x5a; SMALL_BLOB_MAX_BYTES + 1];
        let expected = blob_reference_for_bytes(&bytes);
        let mut upload = store.begin_blob_upload().await.unwrap();
        upload.write(&bytes).await.unwrap();

        let commit_guard = store.commit_lock.lock().await;
        let sealing_store = store.clone();
        let sealing = tokio::spawn(async move { sealing_store.seal_blob_upload(upload).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if store.blobs.contains(&expected).await.unwrap() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("physical upload must finish before seal waits for the commit fence");
        assert!(!sealing.is_finished());

        drop(commit_guard);
        assert_eq!(sealing.await.unwrap().unwrap(), expected);
        let state = store.blob_reference_state(&expected).unwrap().unwrap();
        assert_eq!(state.ref_count, 1);
        assert_eq!(state.flags, AWAITING_PUBLISH);
        assert!(state.created_at <= state.updated_at);
    }

    #[tokio::test]
    async fn streamed_seal_fails_cleanly_when_gc_wins_before_reservation() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_awaiting_publish_ttl_seconds(1),
        )
        .await
        .unwrap();
        let bytes = vec![0x5a; SMALL_BLOB_MAX_BYTES + 1];
        let blob = store.stage_blob(&bytes).await.unwrap();
        let published = store
            .publish(publish("stale", blob.clone(), "publish-stale"))
            .await
            .unwrap();
        store
            .delete(DeleteRequest {
                key: key("stale"),
                precondition: Precondition::Version(published.version),
                command_id: Some("delete-stale".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();
        let retired = store.blob_reference_state(&blob).unwrap().unwrap();
        assert_eq!(retired.ref_count, 0);

        let mut upload = store.begin_blob_upload().await.unwrap();
        upload.write(&bytes).await.unwrap();
        let commit_guard = store.commit_lock.lock().await;
        let sealing_store = store.clone();
        let sealing = tokio::spawn(async move { sealing_store.seal_blob_upload(upload).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if std::fs::read_dir(store.blobs.root().join(".staging"))
                    .unwrap()
                    .next()
                    .is_none()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("physical deduplication must finish before seal waits for the commit fence");

        assert_eq!(
            store
                .collect_blob_garbage_at(retired.updated_at + store.awaiting_publish_ttl_millis)
                .unwrap(),
            1
        );
        drop(commit_guard);

        assert_eq!(
            sealing.await.unwrap().unwrap_err(),
            MutationError::BlobNotFound
        );
        assert!(store.blob_reference_state(&blob).unwrap().is_none());
        assert!(!store.contains_blob(&blob).await.unwrap());
    }

    #[tokio::test]
    async fn small_blob_boundary_and_streamed_seal_use_only_rocksdb() {
        let (_temporary, store) = store().await;
        let boundary_bytes = vec![7_u8; SMALL_BLOB_MAX_BYTES];
        let boundary = store.stage_blob(&boundary_bytes).await.unwrap();
        assert_eq!(boundary.length, SMALL_BLOB_MAX_BYTES as u64);
        assert_eq!(
            store.read_blob_bytes(&boundary).await.unwrap(),
            boundary_bytes
        );
        assert!(!store.blobs.contains(&boundary).await.unwrap());
        let mut reader = store.open_blob(&boundary).await.unwrap();
        let mut read_back = Vec::new();
        let mut chunk = [0_u8; 4_096];
        loop {
            let read = reader.read(&mut chunk).await.unwrap();
            if read == 0 {
                break;
            }
            read_back.extend_from_slice(&chunk[..read]);
        }
        assert_eq!(read_back, boundary_bytes);

        let mut upload = store.begin_blob_upload().await.unwrap();
        upload.write(b"streamed small payload").await.unwrap();
        let streamed = store.seal_blob_upload(upload).await.unwrap();
        assert_eq!(
            store.read_blob_bytes(&streamed).await.unwrap(),
            b"streamed small payload"
        );
        assert!(!blob_file_path(&store, &streamed).exists());

        let large_bytes = vec![9_u8; SMALL_BLOB_MAX_BYTES + 1];
        let large = store.stage_blob(&large_bytes).await.unwrap();
        assert!(store.blobs.contains(&large).await.unwrap());
        assert!(
            store
                .db
                .get_cf(
                    store.cf(CF_SMALL_BLOBS).unwrap(),
                    blob_reference_key(&large)
                )
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn publication_consumes_then_increments_without_counting_replays() {
        let (_temporary, store) = store().await;
        let blob = store.stage_blob(b"shared payload").await.unwrap();
        let first_request = publish("first", blob.clone(), "first-command");
        let outcomes = store
            .bulk_write(vec![
                BatchOperation::Publish(first_request.clone()),
                BatchOperation::Publish(publish("second", blob.clone(), "second-command")),
            ])
            .await;
        assert!(outcomes.iter().all(|outcome| {
            outcome
                .result
                .as_ref()
                .is_ok_and(|receipt| !receipt.replayed)
        }));
        let published_state = store.blob_reference_state(&blob).unwrap().unwrap();
        assert_eq!(published_state.ref_count, 2);
        assert_eq!(published_state.flags, 0);

        let replay = store.publish(first_request).await.unwrap();
        assert!(replay.replayed);
        assert_eq!(
            store.blob_reference_state(&blob).unwrap().unwrap(),
            published_state
        );
    }

    #[tokio::test]
    async fn retirement_reaches_zero_but_gc_waits_for_the_inactivity_ttl() {
        let (_temporary, store) = store().await;
        let blob = store.stage_blob(b"retired payload").await.unwrap();
        store
            .publish(publish("first", blob.clone(), "first-command"))
            .await
            .unwrap();
        store
            .publish(publish("second", blob.clone(), "second-command"))
            .await
            .unwrap();

        let mut pending = PendingBlobReferences::new();
        let mut batch = WriteBatch::default();
        for now in [100_u64, 101] {
            let (key, state) = store
                .prepare_blob_reference_retirement(&blob, &pending, now)
                .unwrap();
            store
                .stage_blob_reference_update(&mut batch, &mut pending, key, state)
                .unwrap();
        }
        store.db.write(batch).unwrap();
        let retired = store.blob_reference_state(&blob).unwrap().unwrap();
        assert_eq!(retired.ref_count, 0);
        assert_eq!(
            store
                .collect_blob_garbage_at(
                    retired.updated_at + store.awaiting_publish_ttl_millis - 1,
                )
                .unwrap(),
            0
        );
        assert!(store.contains_blob(&blob).await.unwrap());
        assert_eq!(
            store
                .collect_blob_garbage_at(retired.updated_at + store.awaiting_publish_ttl_millis,)
                .unwrap(),
            1
        );
        assert!(store.blob_reference_state(&blob).unwrap().is_none());
        assert!(!store.contains_blob(&blob).await.unwrap());
    }

    #[tokio::test]
    async fn gc_uses_awaiting_inactivity_and_removes_untracked_crash_files() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_awaiting_publish_ttl_seconds(1),
        )
        .await
        .unwrap();
        let awaiting = store.stage_blob(b"awaiting").await.unwrap();
        let state = store.blob_reference_state(&awaiting).unwrap().unwrap();
        assert_eq!(
            store
                .collect_blob_garbage_at(state.updated_at + 999)
                .unwrap(),
            0
        );
        assert!(store.contains_blob(&awaiting).await.unwrap());
        assert_eq!(
            store
                .collect_blob_garbage_at(state.updated_at + 1_000)
                .unwrap(),
            1
        );
        assert!(store.blob_reference_state(&awaiting).unwrap().is_none());
        assert!(!store.contains_blob(&awaiting).await.unwrap());

        let orphan = store.blobs.put(b"crash orphan").await.unwrap();
        assert!(store.blob_reference_state(&orphan).unwrap().is_none());
        let encoded_orphan_hash = hex::encode(orphan.hash);
        let orphan_path = store
            .blobs
            .root()
            .join(&encoded_orphan_hash[..2])
            .join(encoded_orphan_hash);
        let orphan_modified = orphan_path
            .metadata()
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert_eq!(
            store
                .collect_blob_garbage_at(orphan_modified + 999)
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .collect_blob_garbage_at(orphan_modified + 1_000)
                .unwrap(),
            1
        );
        assert!(!store.blobs.contains(&orphan).await.unwrap());

        let transition = store.stage_blob(b"sealed small transition").await.unwrap();
        store
            .publish(publish(
                "sealed-small-transition",
                transition.clone(),
                "sealed-small-transition",
            ))
            .await
            .unwrap();
        assert_eq!(
            store.blobs.put(b"sealed small transition").await.unwrap(),
            transition
        );
        let transition_path = blob_file_path(&store, &transition);
        let transition_modified = transition_path
            .metadata()
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert_eq!(
            store
                .collect_blob_garbage_at(transition_modified + 1_000)
                .unwrap(),
            1
        );
        assert!(!transition_path.exists());
        assert!(store.contains_blob(&transition).await.unwrap());

        let staged = store.blobs.root().join(".staging").join("crash-orphan.tmp");
        std::fs::write(&staged, b"abandoned staging bytes").unwrap();
        let modified = staged
            .metadata()
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert_eq!(
            store
                .collect_blob_garbage_at(modified + store.awaiting_publish_ttl_millis)
                .unwrap(),
            1
        );
        assert!(!staged.exists());
    }

    #[tokio::test]
    async fn identical_seals_share_one_reservation_and_zero_count_content_can_be_reused() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_awaiting_publish_ttl_seconds(1),
        )
        .await
        .unwrap();
        let first = store.stage_blob(b"shared bytes").await.unwrap();
        let initial = store.blob_reference_state(&first).unwrap().unwrap();
        let second = store.stage_blob(b"shared bytes").await.unwrap();
        assert_eq!(second, first);
        let resealed = store.blob_reference_state(&first).unwrap().unwrap();
        assert_eq!(resealed.ref_count, 1);
        assert_eq!(resealed.flags, AWAITING_PUBLISH);
        assert_eq!(resealed.created_at, initial.created_at);
        assert!(resealed.updated_at >= initial.updated_at);

        let first_receipt = store
            .publish(publish("first", first.clone(), "publish-first"))
            .await
            .unwrap();
        let second_receipt = store
            .publish(publish("second", first.clone(), "publish-second"))
            .await
            .unwrap();
        assert_eq!(
            store
                .blob_reference_state(&first)
                .unwrap()
                .unwrap()
                .ref_count,
            2
        );
        for (path, version, command) in [
            ("first", first_receipt.version, "delete-first"),
            ("second", second_receipt.version, "delete-second"),
        ] {
            store
                .delete(DeleteRequest {
                    key: key(path),
                    precondition: Precondition::Version(version),
                    command_id: Some(command.into()),
                    durability: Durability::Local,
                })
                .await
                .unwrap();
        }
        let retired = store.blob_reference_state(&first).unwrap().unwrap();
        assert_eq!(retired.ref_count, 0);
        assert_eq!(
            store
                .collect_blob_garbage_at(retired.updated_at + 999)
                .unwrap(),
            0
        );

        let reused = store.stage_blob(b"shared bytes").await.unwrap();
        assert_eq!(reused, first);
        let reserved_again = store.blob_reference_state(&reused).unwrap().unwrap();
        assert_eq!(reserved_again.ref_count, 1);
        assert_eq!(reserved_again.flags, AWAITING_PUBLISH);
        assert_eq!(reserved_again.created_at, initial.created_at);
        store
            .publish(publish("third", reused.clone(), "publish-third"))
            .await
            .unwrap();
        let published_again = store.blob_reference_state(&reused).unwrap().unwrap();
        assert_eq!(published_again.ref_count, 1);
        assert_eq!(published_again.flags, 0);
    }

    #[test]
    fn program_definition_paths_are_direct_versioned_children() {
        assert!(is_program_definition_path("_anvil/programs/import_osv@1"));
        assert!(!is_program_definition_path("_anvil/programs/import_osv"));
        assert!(!is_program_definition_path("_anvil/programs/@1"));
        assert!(!is_program_definition_path("_anvil/programs/import_osv@"));
        assert!(!is_program_definition_path(
            "_anvil/programs/nested/import_osv@1"
        ));
        assert!(!is_program_definition_path(
            "_anvil/programs/import_osv@1@copy"
        ));
    }

    #[derive(Default)]
    struct WalOperationCounter {
        puts: usize,
        deletes: usize,
        merges: usize,
        high_watermark_puts: usize,
        invalidation_metadata_puts: usize,
        receipt_metadata_puts: usize,
    }

    impl WriteBatchIteratorCf for WalOperationCounter {
        fn put_cf(&mut self, _cf_id: u32, key: &[u8], _value: &[u8]) {
            self.puts += 1;
            if key == VERSION_HIGH_WATERMARK_KEY {
                self.high_watermark_puts += 1;
            }
            if [
                LOCAL_INVALIDATION_OFFSET_KEY,
                LOCAL_INVALIDATION_FLOOR_KEY,
                LOCAL_INVALIDATION_COUNT_KEY,
                LOCAL_INVALIDATION_BYTES_KEY,
            ]
            .contains(&key)
            {
                self.invalidation_metadata_puts += 1;
            }
            if [MUTATION_RECEIPT_COUNT_KEY, MUTATION_RECEIPT_BYTES_KEY].contains(&key) {
                self.receipt_metadata_puts += 1;
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
    async fn unversioned_put_replaces_the_descriptor_and_exact_cas_moves_the_head() {
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
        assert!(
            store
                .version_metadata(&key("a"), first.version)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .get_version(&key("a"), first.version)
                .await
                .unwrap_err(),
            MutationError::ObjectVersioningNotEnabled
        );
    }

    #[tokio::test]
    async fn enabled_versioning_retains_descriptors_and_payload_references() {
        let (_temporary, store) = store().await;
        assert!(
            store
                .enable_bucket_versioning("tenant", "bucket")
                .await
                .unwrap()
        );
        assert!(
            !store
                .enable_bucket_versioning("tenant", "bucket")
                .await
                .unwrap()
        );
        let first = store
            .put(put("a", b"same", Precondition::Absent, "first"))
            .await
            .unwrap();
        let second = store
            .put(put(
                "a",
                b"same",
                Precondition::Version(first.version),
                "second",
            ))
            .await
            .unwrap();

        assert_eq!(
            store
                .get_version(&key("a"), first.version)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            b"same"
        );
        assert_eq!(
            store
                .list_object_versions(&key("a"), None, MAX_LIST_OBJECT_VERSIONS)
                .unwrap()
                .into_iter()
                .map(|version| version.id)
                .collect::<Vec<_>>(),
            vec![first.version, second.version]
        );
        let reference = blob_reference_for_bytes(b"same");
        assert_eq!(
            store
                .blob_reference_state(&reference)
                .unwrap()
                .unwrap()
                .ref_count,
            2
        );
    }

    #[tokio::test]
    async fn get_version_rejects_a_descriptor_id_that_disagrees_with_its_key() {
        let (_temporary, store) = store().await;
        store
            .enable_bucket_versioning("tenant", "bucket")
            .await
            .unwrap();
        let created = store
            .put(put(
                "corrupt-version-id",
                b"value",
                Precondition::Absent,
                "corrupt-version-id-create",
            ))
            .await
            .unwrap();
        let object_key = key("corrupt-version-id");
        let identity = store
            .resolve_bucket_identity(object_key.tenant(), object_key.bucket())
            .unwrap();
        let mut descriptor = store
            .version_metadata(&object_key, created.version)
            .unwrap()
            .unwrap();
        descriptor.id = VersionId(u64::MAX);
        store
            .db
            .put_cf(
                store.cf(CF_VERSIONS).unwrap(),
                version_key(identity, &object_key, created.version),
                serde_json::to_vec(&descriptor).unwrap(),
            )
            .unwrap();

        assert!(matches!(
            store.get_version(&object_key, created.version).await,
            Err(MutationError::Storage(message)) if message.contains("disagrees with its key")
        ));
    }

    #[tokio::test]
    async fn batch_get_rejects_a_descriptor_that_disagrees_with_its_current_head() {
        let (_temporary, store) = store().await;
        let created = store
            .put(put(
                "corrupt-current-head",
                b"value",
                Precondition::Absent,
                "corrupt-current-head-create",
            ))
            .await
            .unwrap();
        let object_key = key("corrupt-current-head");
        let identity = store
            .resolve_bucket_identity(object_key.tenant(), object_key.bucket())
            .unwrap();
        let mut descriptor = store
            .version_metadata(&object_key, created.version)
            .unwrap()
            .unwrap();
        descriptor.blob = None;
        descriptor.deleted = true;
        store
            .db
            .put_cf(
                store.cf(CF_VERSIONS).unwrap(),
                version_key(identity, &object_key, created.version),
                serde_json::to_vec(&descriptor).unwrap(),
            )
            .unwrap();

        let results = store.batch_get(&[(object_key, None)]).await;
        assert!(matches!(
            &results[0],
            Err(MutationError::Storage(message)) if message.contains("disagrees with its head")
        ));
    }

    #[tokio::test]
    async fn enabling_versioning_retains_the_existing_current_value_and_survives_reopen() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let first = store
            .put(put("a", b"first", Precondition::Absent, "first"))
            .await
            .unwrap();
        assert!(
            store
                .enable_bucket_versioning("tenant", "bucket")
                .await
                .unwrap()
        );
        let second = store
            .put(put(
                "a",
                b"second",
                Precondition::Version(first.version),
                "second",
            ))
            .await
            .unwrap();
        drop(store);

        let reopened = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        assert_eq!(
            reopened.bucket_versioning("tenant", "bucket").unwrap(),
            ObjectVersioning::Enabled
        );
        assert_eq!(
            reopened
                .get_version(&key("a"), first.version)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            b"first"
        );
        assert_eq!(
            reopened
                .get_version(&key("a"), second.version)
                .await
                .unwrap()
                .unwrap()
                .bytes,
            b"second"
        );
    }

    #[tokio::test]
    async fn unversioned_reference_delta_handles_same_content_replace_and_delete() {
        let (_temporary, store) = store().await;
        let same_reference = blob_reference_for_bytes(b"same");
        let other_reference = blob_reference_for_bytes(b"other");
        let first = store
            .put(put("a", b"same", Precondition::Absent, "first"))
            .await
            .unwrap();
        let second = store
            .put(put(
                "a",
                b"same",
                Precondition::Version(first.version),
                "same-again",
            ))
            .await
            .unwrap();
        assert_eq!(
            store
                .blob_reference_state(&same_reference)
                .unwrap()
                .unwrap()
                .ref_count,
            1
        );
        assert!(
            store
                .version_metadata(&key("a"), first.version)
                .unwrap()
                .is_none()
        );

        let third = store
            .put(put(
                "a",
                b"other",
                Precondition::Version(second.version),
                "replace",
            ))
            .await
            .unwrap();
        assert_eq!(
            store
                .blob_reference_state(&same_reference)
                .unwrap()
                .unwrap()
                .ref_count,
            0
        );
        assert_eq!(
            store
                .blob_reference_state(&other_reference)
                .unwrap()
                .unwrap()
                .ref_count,
            1
        );

        let deleted = store
            .delete(DeleteRequest {
                key: key("a"),
                precondition: Precondition::Version(third.version),
                command_id: Some("delete".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();
        assert_eq!(
            store
                .blob_reference_state(&other_reference)
                .unwrap()
                .unwrap()
                .ref_count,
            0
        );
        assert!(
            store
                .version_metadata(&key("a"), third.version)
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .version_metadata(&key("a"), deleted.version)
                .unwrap()
                .unwrap()
                .deleted
        );
    }

    #[tokio::test]
    async fn retained_version_deletion_never_reveals_an_older_value() {
        let (_temporary, store) = store().await;
        store
            .enable_bucket_versioning("tenant", "bucket")
            .await
            .unwrap();
        let first = store
            .put(put("a", b"first", Precondition::Absent, "first"))
            .await
            .unwrap();
        let second = store
            .put(put(
                "a",
                b"second",
                Precondition::Version(first.version),
                "second",
            ))
            .await
            .unwrap();
        let third = store
            .put(put(
                "a",
                b"third",
                Precondition::Version(second.version),
                "third",
            ))
            .await
            .unwrap();

        assert_eq!(
            store
                .delete_retained_version(&key("a"), first.version)
                .await
                .unwrap(),
            DeleteRetainedVersionOutcome::DeletedNonCurrent
        );
        assert_eq!(
            store.head(&key("a")).unwrap().unwrap().version,
            third.version
        );
        assert_eq!(
            store
                .list_object_versions(&key("a"), Some(first.version), 1)
                .unwrap()
                .into_iter()
                .map(|version| version.id)
                .collect::<Vec<_>>(),
            vec![second.version]
        );

        let tombstone = match store
            .delete_retained_version(&key("a"), third.version)
            .await
            .unwrap()
        {
            DeleteRetainedVersionOutcome::ReplacedCurrentWithTombstone { version } => version,
            other => panic!("unexpected retained-version deletion outcome: {other:?}"),
        };
        assert!(tombstone > third.version);
        assert_eq!(store.head(&key("a")).unwrap().unwrap().version, tombstone);
        assert!(store.head(&key("a")).unwrap().unwrap().deleted);
        assert!(
            store
                .version_metadata(&key("a"), third.version)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .list_object_versions(&key("a"), None, MAX_LIST_OBJECT_VERSIONS)
                .unwrap()
                .into_iter()
                .map(|version| version.id)
                .collect::<Vec<_>>(),
            vec![second.version, tombstone]
        );
        assert_eq!(
            store
                .delete_retained_version(&key("a"), tombstone)
                .await
                .unwrap_err(),
            MutationError::CurrentTombstoneCannotBeDeleted
        );
        assert_eq!(
            store
                .delete_retained_version(&key("a"), VersionId(u64::MAX))
                .await
                .unwrap(),
            DeleteRetainedVersionOutcome::NotFound
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
    async fn unexpired_receipts_backpressure_new_commands_but_never_their_replay() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_mutation_receipt_retention(
                MutationReceiptRetention::new(60, 1, 1024 * 1024).unwrap(),
            ),
        )
        .await
        .unwrap();
        let request = put("first", b"one", Precondition::Absent, "first-command");
        let applied = store.put(request.clone()).await.unwrap();
        assert!(applied.replay_guarantee_expires_at_unix_millis > now_unix_millis().unwrap());
        let replay = store.put(request).await.unwrap();
        assert!(replay.replayed);
        assert_eq!(
            replay.replay_guarantee_expires_at_unix_millis,
            applied.replay_guarantee_expires_at_unix_millis
        );
        assert_eq!(
            store
                .put(put(
                    "second",
                    b"two",
                    Precondition::Absent,
                    "second-command",
                ))
                .await
                .unwrap_err(),
            MutationError::ReceiptCapacity
        );
        assert!(store.head(&key("second")).unwrap().is_none());
    }

    #[tokio::test]
    async fn expired_receipts_are_pruned_and_the_command_id_can_be_new_again() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_mutation_receipt_retention(
                MutationReceiptRetention::new(1, 1, 1024 * 1024).unwrap(),
            ),
        )
        .await
        .unwrap();
        let first = store
            .put(put("path", b"value", Precondition::Any, "command"))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        let second = store
            .put(put("path", b"value", Precondition::Any, "command"))
            .await
            .unwrap();
        assert!(!second.replayed);
        assert!(second.version > first.version);
        assert_eq!(store.mutation_receipt_status().unwrap().entries, 1);
    }

    #[tokio::test]
    async fn replicated_durability_is_rejected_before_any_head_change() {
        let (_temporary, store) = store().await;

        let mut replicated_put = put("put", b"value", Precondition::Absent, "put-command");
        replicated_put.durability = Durability::Replicated;
        assert_eq!(
            store.put(replicated_put).await.unwrap_err(),
            MutationError::DurabilityUnavailable
        );
        assert!(store.head(&key("put")).unwrap().is_none());

        let blob = store.stage_blob(b"published").await.unwrap();
        let replicated_publish = PublishRequest {
            key: key("publish"),
            blob,
            content_type: Some("application/octet-stream".into()),
            mode: PutMode::PutIfAbsent,
            command_id: Some("publish-command".into()),
            durability: Durability::Replicated,
        };
        assert_eq!(
            store.publish(replicated_publish).await.unwrap_err(),
            MutationError::DurabilityUnavailable
        );
        assert!(store.head(&key("publish")).unwrap().is_none());

        let created = store
            .put(put(
                "delete",
                b"value",
                Precondition::Absent,
                "create-delete-target",
            ))
            .await
            .unwrap();
        let replicated_delete = DeleteRequest {
            key: key("delete"),
            precondition: Precondition::Version(created.version),
            command_id: Some("delete-command".into()),
            durability: Durability::Replicated,
        };
        assert_eq!(
            store.delete(replicated_delete).await.unwrap_err(),
            MutationError::DurabilityUnavailable
        );
        assert_eq!(
            store.head(&key("delete")).unwrap().unwrap().version,
            created.version
        );
    }

    #[tokio::test]
    async fn internal_publish_and_inline_put_share_one_canonical_fingerprint() {
        let (_temporary, store) = store().await;
        let bytes = b"same logical object";
        let blob = store.stage_blob(bytes).await.unwrap();
        let published = store
            .publish(PublishRequest {
                key: key("streamed"),
                blob: blob.clone(),
                content_type: Some("application/octet-stream".into()),
                mode: PutMode::PutIfAbsent,
                command_id: Some("streamed-command".into()),
                durability: Durability::Local,
            })
            .await;
        let published = published.unwrap();
        let replay = store
            .put(put(
                "streamed",
                bytes,
                Precondition::Absent,
                "streamed-command",
            ))
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.version, published.version);
        assert_eq!(replay.fingerprint, published.fingerprint);

        let inline = store
            .put(put("bulk", bytes, Precondition::Absent, "bulk-command"))
            .await
            .unwrap();
        let replay = store
            .publish(PublishRequest {
                key: key("bulk"),
                blob,
                content_type: Some("application/octet-stream".into()),
                mode: PutMode::PutIfAbsent,
                command_id: Some("bulk-command".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.version, inline.version);
        assert_eq!(replay.fingerprint, inline.fingerprint);
    }

    #[tokio::test]
    async fn create_once_policy_applies_to_every_write_surface() {
        let (_temporary, store) = store().await;
        store
            .set_bucket_policy(
                "tenant",
                "bucket",
                BucketPolicy {
                    immutable_prefixes: vec!["ledger".into()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .put(put(
                    "ledger/entry-1",
                    b"entry",
                    Precondition::Absent,
                    "ordinary-entry",
                ))
                .await
                .unwrap_err(),
            MutationError::Immutable
        );
        assert!(store.head(&key("ledger/entry-1")).unwrap().is_none());
        let first = store
            .put(immutable_put("ledger/entry-1", b"entry", "entry"))
            .await
            .unwrap();
        let identical = store
            .put(immutable_put(
                "ledger/entry-1",
                b"entry",
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
                .put(immutable_put("mutable/entry", b"entry", "wrong-policy"))
                .await
                .unwrap_err(),
            MutationError::ImmutablePolicyRequired
        );
        assert_eq!(
            store
                .delete(DeleteRequest {
                    key: key("ledger/entry-1"),
                    precondition: Precondition::Version(first.version),
                    command_id: Some("delete".into()),
                    durability: Durability::Local,
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
                durability: Durability::Local,
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
        let rejected = blob_reference_for_bytes(b"bad");
        assert!(
            store
                .db
                .get_cf(
                    store.cf(CF_SMALL_BLOBS).unwrap(),
                    blob_reference_key(&rejected),
                )
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn bulk_wal_contains_one_high_watermark_and_replay_adds_no_write() {
        let (_temporary, store) = store().await;
        store.resolve_bucket_identity("tenant", "bucket").unwrap();
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
        // Three small raw values, blob lifecycle records, versions, heads, receipts,
        // receipt-expiry indexes and invalidations, plus one version watermark,
        // four watch counters and two receipt counters. All metadata moves in
        // this one physical batch rather than once per mutation.
        assert_eq!(counter.puts, 28);
        assert_eq!(counter.high_watermark_puts, 1);
        assert_eq!(counter.invalidation_metadata_puts, 4);
        assert_eq!(counter.receipt_metadata_puts, 2);
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
    async fn prepared_put_keeps_small_bytes_in_memory_and_materializes_large_bytes() {
        let (_temporary, store) = store().await;
        let identity = store.resolve_bucket_identity("tenant", "bucket").unwrap();
        let first_bytes = b"small payload".to_vec();
        let first = store
            .prepare(
                BatchOperation::Put(put("first", &first_bytes, Precondition::Absent, "first")),
                identity,
            )
            .await
            .unwrap();
        match first {
            PreparedOperation::Put {
                request,
                payload: PreparedPayload::Small { reference, bytes },
                ..
            } => {
                assert!(request.bytes.is_empty());
                assert_eq!(reference.length, first_bytes.len() as u64);
                assert_eq!(bytes, first_bytes);
                assert!(!store.contains_blob(&reference).await.unwrap());
            }
            _ => panic!("small put was not retained in memory"),
        }

        let blob_bytes = vec![9_u8; SMALL_BLOB_MAX_BYTES + 1];
        let sequence_before_prepare = store.db.latest_sequence_number();
        let blob = store
            .prepare(
                BatchOperation::Put(put("blob", &blob_bytes, Precondition::Absent, "blob")),
                identity,
            )
            .await
            .unwrap();
        match blob {
            PreparedOperation::Put {
                request,
                payload: PreparedPayload::Large(reference),
                ..
            } => {
                assert!(request.bytes.is_empty());
                assert_eq!(reference.length, blob_bytes.len() as u64);
                assert_eq!(store.blobs.get(&reference).await.unwrap(), blob_bytes);
                assert!(store.blob_reference_state(&reference).unwrap().is_none());
                assert_eq!(store.db.latest_sequence_number(), sequence_before_prepare);
            }
            _ => panic!("large put was not durably materialized"),
        }
    }

    #[tokio::test]
    async fn bulk_publishes_identical_large_payloads_in_one_rocksdb_batch() {
        let (temporary, store) = store().await;
        store.resolve_bucket_identity("tenant", "bucket").unwrap();
        let bytes = vec![0x5a; SMALL_BLOB_MAX_BYTES + 1];
        let reference = blob_reference_for_bytes(&bytes);
        let operations = vec![
            BatchOperation::Put(put("first", &bytes, Precondition::Absent, "first-large")),
            BatchOperation::Put(put("second", &bytes, Precondition::Absent, "second-large")),
        ];
        let before = store.db.latest_sequence_number();

        let outcomes = store.bulk_write(operations).await;

        assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));
        let updates = store
            .db
            .get_updates_since(before)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(updates.len(), 1);
        let state = store.blob_reference_state(&reference).unwrap().unwrap();
        assert_eq!(state.ref_count, 2);
        assert_eq!(state.flags, 0);

        drop(store);
        let reopened = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        for path in ["first", "second"] {
            assert_eq!(
                reopened.get(&key(path)).await.unwrap().unwrap().bytes,
                bytes
            );
        }
    }

    #[tokio::test]
    async fn rejected_large_inline_put_leaves_only_an_age_gated_orphan() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_awaiting_publish_ttl_seconds(1),
        )
        .await
        .unwrap();
        store
            .put(put("occupied", b"current", Precondition::Absent, "create"))
            .await
            .unwrap();
        let bytes = vec![0x6b; SMALL_BLOB_MAX_BYTES + 1];
        let reference = blob_reference_for_bytes(&bytes);

        let rejected = store
            .put(put(
                "occupied",
                &bytes,
                Precondition::Absent,
                "rejected-large",
            ))
            .await;

        assert!(matches!(
            rejected,
            Err(MutationError::PreconditionFailed { .. })
        ));
        assert!(store.blob_reference_state(&reference).unwrap().is_none());
        assert!(store.blobs.contains(&reference).await.unwrap());
        let modified = blob_file_path(&store, &reference)
            .metadata()
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert_eq!(store.collect_blob_garbage_at(modified + 999).unwrap(), 0);
        assert!(store.blobs.contains(&reference).await.unwrap());
        assert_eq!(store.collect_blob_garbage_at(modified + 1_000).unwrap(), 1);
        assert!(!store.blobs.contains(&reference).await.unwrap());
    }

    #[tokio::test]
    async fn bulk_loads_each_distinct_bucket_policy_once() {
        let (_temporary, store) = store().await;
        let put_in = |bucket: &str, path: &str, command: &str| {
            BatchOperation::Put(PutRequest {
                key: ObjectKey::new("tenant", bucket, path).unwrap(),
                bytes: path.as_bytes().to_vec(),
                content_type: None,
                mode: PutMode::PutIfAbsent,
                command_id: Some(command.into()),
                durability: Durability::Local,
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
    async fn bulk_routes_payloads_to_the_deterministic_small_or_large_plane() {
        let (_temporary, store) = store().await;
        let small = b"small".to_vec();
        let large = vec![9u8; SMALL_BLOB_MAX_BYTES + 1];
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
        let small_reference = small_version.blob.as_ref().unwrap();
        assert_eq!(small_reference, &blob_reference_for_bytes(&small));
        assert!(large_version.blob.is_some());
        assert_eq!(
            store
                .db
                .get_cf(
                    store.cf(CF_SMALL_BLOBS).unwrap(),
                    blob_reference_key(small_reference),
                )
                .unwrap()
                .unwrap()
                .as_slice(),
            small.as_slice()
        );
        assert!(!store.blobs.contains(small_reference).await.unwrap());
        assert!(
            store
                .db
                .get_cf(
                    store.cf(CF_SMALL_BLOBS).unwrap(),
                    blob_reference_key(large_version.blob.as_ref().unwrap()),
                )
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .blobs
                .contains(large_version.blob.as_ref().unwrap())
                .await
                .unwrap()
        );
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
                durability: Durability::Local,
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
    async fn batch_get_selection_releases_the_fence_before_blob_reads_and_keeps_selected_bytes() {
        let (_temporary, store) = store().await;
        store
            .enable_bucket_versioning("tenant", "bucket")
            .await
            .unwrap();
        let old = store
            .put(put("moving", b"old", Precondition::Absent, "moving-old"))
            .await
            .unwrap();
        let old_version = old.version;
        let large_payload = vec![9_u8; SMALL_BLOB_MAX_BYTES + 1];
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
                durability: Durability::Local,
            })
            .await
            .unwrap();

        let selection = store
            .select_batch_get(&[
                (key("moving"), None),
                (key("large"), Some(large.version)),
                (key("deleted"), None),
                (key("never"), None),
                (key("moving"), Some(VersionId(u64::MAX))),
            ])
            .await;
        assert_eq!(
            selection.declared_present_payload_bytes(),
            (b"old".len() + large_payload.len()) as u64
        );

        let moving_store = store.clone();
        let movement = tokio::spawn(async move {
            moving_store
                .put(put(
                    "moving",
                    b"new head",
                    Precondition::Version(old_version),
                    "moving-new",
                ))
                .await
                .unwrap()
        });
        let current = tokio::time::timeout(std::time::Duration::from_secs(1), movement)
            .await
            .expect("holding an immutable batch selection must not fence an unrelated commit")
            .unwrap();
        let results = store.read_batch_get_selection(selection).await;

        let selected_old = results[0].as_ref().unwrap().as_ref().unwrap();
        assert_eq!(selected_old.version.id, old_version);
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
    async fn unversioned_retirement_does_not_block_or_break_selected_blob_reads() {
        let (_temporary, store) = store().await;
        let original_bytes = vec![0x41; SMALL_BLOB_MAX_BYTES + 1];
        let original = store
            .put(put(
                "retired-after-selection",
                &original_bytes,
                Precondition::Absent,
                "retired-original",
            ))
            .await
            .unwrap();
        let original_metadata = store
            .version_metadata(&key("retired-after-selection"), original.version)
            .unwrap()
            .unwrap();
        let original_blob = original_metadata.blob.unwrap();
        let selection = store
            .select_batch_get(&[(key("retired-after-selection"), None)])
            .await;

        let replacement = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.put(put(
                "retired-after-selection",
                b"replacement",
                Precondition::Version(original.version),
                "retired-replacement",
            )),
        )
        .await
        .expect("an outstanding immutable selection must not fence replacement")
        .unwrap();
        assert!(replacement.version > original.version);
        assert!(
            store
                .version_metadata(&key("retired-after-selection"), original.version)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .blob_reference_state(&original_blob)
                .unwrap()
                .unwrap()
                .ref_count,
            0
        );

        let selected = store.read_batch_get_selection(selection).await;
        assert_eq!(
            selected[0].as_ref().unwrap().as_ref().unwrap().bytes,
            original_bytes
        );
    }

    #[tokio::test]
    async fn reserved_program_definitions_require_put_immutable_then_replay_same_content() {
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
            MutationError::Immutable
        );
        assert!(store.head(&key(path)).unwrap().is_none());

        let first = store
            .put(immutable_put(path, b"definition", "define"))
            .await
            .unwrap();

        let replay = store
            .put(immutable_put(path, b"definition", "define-again"))
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
                    durability: Durability::Local,
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
                    mode: PutMode::Put,
                    command_id: Some("unsafe-publish".into()),
                    durability: Durability::Local,
                })
                .await
                .unwrap_err(),
            MutationError::Immutable
        );
        assert!(store.head(&key(published_path)).unwrap().is_none());

        let published = store
            .publish(PublishRequest {
                key: key(published_path),
                blob: blob.clone(),
                content_type: Some("application/json".into()),
                mode: PutMode::PutImmutable,
                command_id: Some("publish".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();
        let replay = store
            .publish(PublishRequest {
                key: key(published_path),
                blob,
                content_type: Some("application/json".into()),
                mode: PutMode::PutImmutable,
                command_id: Some("publish-again".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();
        assert_eq!(replay.version, published.version);
    }

    #[tokio::test]
    async fn program_only_policy_reports_concurrency_violation_for_every_direct_write_kind() {
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
            MutationError::ProgramConcurrencyViolation
        );
        let blob = store.stage_blob(b"value").await.unwrap();
        assert_eq!(
            store
                .publish(PublishRequest {
                    key: key("managed/a"),
                    blob,
                    content_type: None,
                    mode: PutMode::PutIfAbsent,
                    command_id: Some("managed-publish".into()),
                    durability: Durability::Local,
                })
                .await
                .unwrap_err(),
            MutationError::ProgramConcurrencyViolation
        );
        assert_eq!(
            store
                .delete(DeleteRequest {
                    key: key("managed/a"),
                    precondition: Precondition::Any,
                    command_id: Some("managed-delete".into()),
                    durability: Durability::Local,
                })
                .await
                .unwrap_err(),
            MutationError::ProgramConcurrencyViolation
        );
        assert!(
            store
                .put(put("managed-other", b"ok", Precondition::Absent, "outside"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn list_objects_seeks_the_stable_id_head_prefix_and_returns_lexical_live_pages() {
        let (_temporary, store) = store().await;
        for (index, path) in ["z", "aa", "b", "foo", "foo/bar", "foobar", "foo/deleted"]
            .into_iter()
            .enumerate()
        {
            store
                .put(put(
                    path,
                    path.as_bytes(),
                    Precondition::Absent,
                    &format!("list-{index}"),
                ))
                .await
                .unwrap();
        }
        store
            .delete(DeleteRequest {
                key: key("foo/deleted"),
                precondition: Precondition::Any,
                command_id: Some("list-delete".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();
        store
            .put(PutRequest {
                key: ObjectKey::new("tenant", "other-bucket", "foo/hidden").unwrap(),
                bytes: b"other bucket".to_vec(),
                content_type: None,
                mode: PutMode::PutIfAbsent,
                command_id: Some("list-other-bucket".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();

        let identity = store.resolve_bucket_identity("tenant", "bucket").unwrap();
        assert_eq!(
            store.head_storage_key(&key("foo")).unwrap(),
            [identity.encode().to_vec(), b"foo".to_vec()].concat()
        );
        assert!(
            store
                .head_storage_key(&key("foo"))
                .unwrap()
                .windows(b"tenant".len())
                .all(|window| window != b"tenant")
        );
        assert!(
            store
                .head_storage_key(&key("foo"))
                .unwrap()
                .windows(b"bucket".len())
                .all(|window| window != b"bucket")
        );

        let all = store
            .list_objects("tenant", "bucket", "", None, MAX_LIST_OBJECTS)
            .unwrap();
        assert_eq!(
            all.paths,
            ["aa", "b", "foo", "foo/bar", "foobar", "z"]
                .map(str::to_owned)
                .to_vec()
        );
        assert!(!all.has_more);

        // Prefix matching is literal, not path-segment aware: `foo` includes
        // both `foo/bar` and `foobar`. The tombstone is not a listed object.
        let first = store
            .list_objects("tenant", "bucket", "foo", None, 2)
            .unwrap();
        assert_eq!(first.paths, ["foo", "foo/bar"].map(str::to_owned).to_vec());
        assert!(first.has_more);
        let second = store
            .list_objects(
                "tenant",
                "bucket",
                "foo",
                first.paths.last().map(String::as_str),
                2,
            )
            .unwrap();
        assert_eq!(second.paths, vec!["foobar".to_owned()]);
        assert!(!second.has_more);
    }

    #[tokio::test]
    async fn retained_version_keys_use_one_nul_terminator_after_the_raw_path() {
        let (_temporary, store) = store().await;
        let logical = key("a");
        let identity = store
            .resolve_bucket_identity(logical.tenant(), logical.bucket())
            .unwrap();
        let version = VersionId(0x0102_0304_0506_0708);
        assert_eq!(
            version_key(identity, &logical, version),
            [
                identity.encode().to_vec(),
                b"a".to_vec(),
                vec![0],
                version.0.to_be_bytes().to_vec(),
            ]
            .concat()
        );
        assert!(
            !identity
                .head_key("a/b")
                .starts_with(&version_prefix(identity, &logical))
        );
    }

    #[tokio::test]
    async fn list_objects_pages_are_read_committed_not_a_cross_page_snapshot() {
        let (_temporary, store) = store().await;
        store
            .put(put("a", b"a", Precondition::Absent, "page-a"))
            .await
            .unwrap();
        store
            .put(put("c", b"c", Precondition::Absent, "page-c"))
            .await
            .unwrap();

        let first = store.list_objects("tenant", "bucket", "", None, 1).unwrap();
        assert_eq!(first.paths, vec!["a".to_owned()]);
        assert!(first.has_more);

        // A later page observes commits made after the first page. `b` appears
        // and the newly deleted `c` disappears.
        store
            .put(put("b", b"b", Precondition::Absent, "page-b"))
            .await
            .unwrap();
        store
            .delete(DeleteRequest {
                key: key("c"),
                precondition: Precondition::Any,
                command_id: Some("page-delete-c".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();
        let second = store
            .list_objects("tenant", "bucket", "", Some("a"), 10)
            .unwrap();
        assert_eq!(second.paths, vec!["b".to_owned()]);
        assert!(!second.has_more);
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
