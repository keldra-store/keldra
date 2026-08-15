use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anvil_atomic_program::{LocalLockManager, ObjectPath};
use anyhow::{Context, Result};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DEFAULT_COLUMN_FAMILY_NAME, Direction,
    IteratorMode, Options, WriteBatch, WriteBufferManager, WriteOptions, properties,
};
use serde::{Deserialize, Serialize};

use crate::key::{
    BucketId, BucketIdentity, STORAGE_KEY_FORMAT_VERSION, TenantId, bucket_name_key,
    contains_reserved_anvil_segment, tenant_name_key,
};
use crate::logical_record::decode_current_value;
use crate::watch::{
    AggregateKind, DecodedLocalChange, InvalidationStateHint, LOCAL_INVALIDATION_BYTES_KEY,
    LOCAL_INVALIDATION_COUNT_KEY, LOCAL_INVALIDATION_EPOCH_KEY, LOCAL_INVALIDATION_FLOOR_KEY,
    LOCAL_INVALIDATION_OFFSET_KEY, LOCAL_INVALIDATION_SETTLED_KEY, LOCAL_INVALIDATION_TOKEN_KEY,
    LocalChange, LocalChangePage, LocalInvalidation, MAX_LOCAL_INVALIDATION_SCAN_RECORDS,
    ObjectHeadChangeKind, OversizeLocalChange, SourceId, WatchCursor, WatchError,
    WatchJournalStatus, WatchPage, WatchRetention, WatchScope, WatchStart, decode_local_change,
    decode_local_change_with_length, decode_resume_token, encode_local_change, encode_resume_token,
    invalidation_key, invalidation_record_bytes, offset_from_key,
};
use crate::{
    AWAITING_PUBLISH, AccountingHeadTransition, BatchOperation, BatchOutcome, BlobReader, BlobRef,
    BlobReferenceState, BlobStore, BucketPolicy, DefinitionTransition, DeleteRequest,
    DeleteRetainedVersionOutcome, Durability, Head, INDEX_DEFINITION_PREFIX, MutationError,
    MutationReceipt, Object, ObjectKey, ObjectVersioning, Precondition, PublishRequest, PutMode,
    PutRequest, ReferenceDelta, SMALL_BLOB_MAX_BYTES, StorageTenantId, Version, VersionClock,
    VersionId,
};

const PROGRAM_DEFINITION_PREFIX: &str = "_anvil/programs/";
const DEFINITION_ASSIGNMENT_NOTIFICATION_CAPACITY: usize = 64;

struct MutationBackpressureWait {
    capacity: &'static str,
    started: std::time::Instant,
    finished: bool,
}

impl MutationBackpressureWait {
    fn start(capacity: &'static str) -> Self {
        tracing::info!(
            capacity,
            counter.anvil_mutation_backpressure_waiting = 1_i64,
            monotonic_counter.anvil_mutation_backpressure_waits_total = 1_u64,
            "object mutation is waiting for bounded durable state"
        );
        Self {
            capacity,
            started: std::time::Instant::now(),
            finished: false,
        }
    }

    fn complete(mut self) {
        self.emit("capacity_available", false);
        self.finished = true;
    }

    fn emit(&self, outcome: &'static str, cancelled: bool) {
        tracing::info!(
            capacity = self.capacity,
            counter.anvil_mutation_backpressure_waiting = -1_i64,
            "object mutation capacity wait released"
        );
        tracing::info!(
            capacity = self.capacity,
            backpressure.outcome = outcome,
            monotonic_counter.anvil_mutation_backpressure_wait_cancellations_total =
                u64::from(cancelled),
            histogram.anvil_mutation_backpressure_wait_duration_seconds =
                self.started.elapsed().as_secs_f64(),
            "object mutation capacity wait finished"
        );
    }
}

impl Drop for MutationBackpressureWait {
    fn drop(&mut self) {
        if !self.finished {
            self.emit("cancelled", true);
        }
    }
}

// RocksDB otherwise allocates one 64 MiB write buffer and one independent
// block cache per column family. Anvil's metadata workload writes several
// families together, so those defaults multiply native memory without buying
// useful locality. These process-local resources are shared by every metadata
// column family, including RocksDB's unused default family:
//
// - 64 MiB keeps frequently-read table blocks warm without one cache per CF;
// - 128 MiB bounds all mutable and immutable memtables and stalls writers when
//   flush pressure reaches that soft limit; and
// - 16 MiB per memtable lets eight active families share the global allowance
//   before the manager must flush, while avoiding the default 64 MiB per CF.
//
// The two dominant configurable native pools therefore total 192 MiB. This
// leaves 320 MiB of the 512 MiB qualification allowance for table readers,
// compaction buffers, allocator slack, and the rest of the process.
const METADATA_BLOCK_CACHE_BYTES: usize = 64 * 1024 * 1024;
const METADATA_WRITE_BUFFER_MANAGER_BYTES: usize = 128 * 1024 * 1024;
const METADATA_COLUMN_FAMILY_WRITE_BUFFER_BYTES: usize = 16 * 1024 * 1024;

pub(crate) const CF_HEADS: &str = "heads";
pub(crate) const CF_VERSIONS: &str = "versions";
pub(crate) const CF_BLOB_REFERENCES: &str = "blob_references";
pub(crate) const CF_BLOB_GC_DUE: &str = "blob_gc_due";
pub(crate) const CF_SMALL_BLOBS: &str = "small_blobs";
pub(crate) const CF_BUCKET_OPTIONS: &str = "bucket_options";
pub(crate) const CF_NAMES: &str = "names";
const CF_RECEIPTS: &str = "receipts";
pub(crate) const CF_POLICIES: &str = "policies";
const CF_LOCAL_INVALIDATIONS: &str = "local_invalidations";
pub(crate) const CF_METADATA: &str = "metadata";
pub(crate) const CF_AUTHZ_TENANTS: &str = "authz_tenants";
pub(crate) const CF_AUTHZ_SCHEMAS: &str = "authz_schemas";
pub(crate) const CF_AUTHZ_BINDINGS: &str = "authz_bindings";
pub(crate) const CF_AUTHZ_TUPLES: &str = "authz_tuples";
pub(crate) const CF_AUTHZ_RECEIPTS: &str = "authz_receipts";
pub(crate) const CF_CREDENTIALS: &str = "credentials";
pub(crate) const CF_DEFINITION_STATE: &str = "definition_state";
pub(crate) const CF_JOURNAL_ROUTES: &str = "journal_routes";
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
pub(crate) const COLUMN_FAMILIES: &[&str] = &[
    CF_HEADS,
    CF_VERSIONS,
    CF_BLOB_REFERENCES,
    CF_BLOB_GC_DUE,
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
    CF_DEFINITION_STATE,
    CF_JOURNAL_ROUTES,
];

struct MetadataMemoryResources {
    block_cache: Cache,
    write_buffer_manager: WriteBufferManager,
}

/// Point-in-time resource and backpressure signals from the metadata database.
///
/// These values are process-local and observational. They are never persisted
/// or used to make storage decisions.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetadataRuntimeMetrics {
    pub block_cache_capacity_bytes: u64,
    pub block_cache_usage_bytes: u64,
    pub block_cache_pinned_bytes: u64,
    pub write_buffer_capacity_bytes: u64,
    pub write_buffer_usage_bytes: u64,
    pub active_memtable_bytes: Option<u64>,
    pub all_memtable_bytes: Option<u64>,
    pub table_reader_bytes: Option<u64>,
    pub pending_compaction_bytes: Option<u64>,
    pub immutable_memtables: Option<u64>,
    pub running_compactions: Option<u64>,
    pub running_flushes: Option<u64>,
    pub compaction_pending_column_families: Option<u64>,
    pub flush_pending_column_families: Option<u64>,
    pub actual_delayed_write_rate_bytes_per_second: Option<u64>,
    pub write_stopped: Option<u64>,
    pub background_errors: Option<u64>,
    pub mutation_receipt_entries: Option<u64>,
    pub mutation_receipt_bytes: Option<u64>,
    pub mutation_receipt_max_entries: u64,
    pub mutation_receipt_max_bytes: u64,
    pub mutation_receipt_oldest_age_seconds: Option<u64>,
    pub unavailable_properties: u64,
    pub property_collection_failures: u64,
    pub first_unavailable_property: Option<&'static str>,
    pub first_collection_error: Option<String>,
}

impl MetadataMemoryResources {
    fn new() -> Self {
        Self {
            block_cache: Cache::new_lru_cache(METADATA_BLOCK_CACHE_BYTES),
            write_buffer_manager: WriteBufferManager::new_write_buffer_manager(
                METADATA_WRITE_BUFFER_MANAGER_BYTES,
                true,
            ),
        }
    }

    fn column_family_options(&self) -> Options {
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&self.block_cache);

        let mut options = Options::default();
        options.set_block_based_table_factory(&table);
        options.set_write_buffer_manager(&self.write_buffer_manager);
        options.set_write_buffer_size(METADATA_COLUMN_FAMILY_WRITE_BUFFER_BYTES);
        options
    }
}

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
        accounting_transition: Option<crate::AccountingHeadTransition>,
        definition_transition: Option<DefinitionTransition>,
    },
    RetainedVersionDeleted {
        identity: BucketIdentity,
        exact_path: String,
        deleted_version: VersionId,
        resulting_head_version: Option<VersionId>,
        reference_deltas: Vec<ReferenceDelta>,
        accounting_transition: Option<crate::AccountingHeadTransition>,
    },
    AggregateChanged {
        aggregate_kind: AggregateKind,
        aggregate_key: Vec<u8>,
        revision: u64,
    },
    ContentLifecycleChanged {
        blob_identity: Vec<u8>,
        revision: u64,
        reference_deltas: Vec<ReferenceDelta>,
    },
}

/// Declares whether the reference effects carried by a source-journal append
/// were applied by the same RocksDB batch. Keeping this explicit prevents the
/// storage kernel from guessing whether its caller selected the one-node or
/// distributed mutation path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LocalReferenceEffects {
    AppliedInline,
    NoReferenceEffects,
    Deferred,
}

impl PendingLocalChange {
    fn has_reference_effects(&self) -> bool {
        match self {
            Self::ObjectHead {
                reference_deltas, ..
            }
            | Self::RetainedVersionDeleted {
                reference_deltas, ..
            }
            | Self::ContentLifecycleChanged {
                reference_deltas, ..
            } => !reference_deltas.is_empty(),
            Self::AggregateChanged { .. } => false,
        }
    }
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
    // Keep the shared native resources alive for exactly as long as the DB.
    // RocksDB also retains shared ownership internally; this field makes that
    // lifetime and the one-resource-per-Store contract explicit.
    _metadata_memory: Arc<MetadataMemoryResources>,
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
    pub(crate) definition_state_lock: Arc<std::sync::Mutex<()>>,
    pub(crate) node_id: u16,
    pub(crate) sync_writes: bool,
    pub(crate) watch_retention: WatchRetention,
    pub(crate) mutation_receipt_retention: MutationReceiptRetention,
    awaiting_publish_ttl_millis: u64,
    watch_source_epoch: [u8; 32],
    watch_token_key: [u8; 32],
    /// Highest source-journal offset known safe for retention compaction.
    /// Restart initializes this to the durable floor and waits for current
    /// destination cursors before allowing any further compaction.
    pub(crate) source_journal_reference_safe_through: Arc<std::sync::atomic::AtomicU64>,
    /// Process-lifetime high-water marks for trusted publication progress
    /// debt. Current debt remains derived from durable journal occupancy.
    pub(crate) source_journal_progress_debt_peak_entries: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) source_journal_progress_debt_peak_bytes: Arc<std::sync::atomic::AtomicU64>,
    /// Wakes object writers when receipt expiry or source-journal progress may
    /// have released capacity. A short timer fallback covers receipt expiry
    /// when no other writer is active.
    mutation_capacity_notify: Arc<tokio::sync::Notify>,
    watch_notify: tokio::sync::watch::Sender<()>,
    definition_assignment_notify:
        tokio::sync::broadcast::Sender<Vec<crate::DefinitionAssignmentMutation>>,
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
    Small {
        reference: BlobRef,
        bytes: Vec<u8>,
    },
    Large(BlobRef),
    /// Complete ordinary awaiting-publication source retained while a
    /// distributed metadata mutation is placed and delivered.
    Sealed(BlobRef),
}

impl PreparedPayload {
    fn reference(&self) -> &BlobRef {
        match self {
            Self::Small { reference, .. } | Self::Large(reference) | Self::Sealed(reference) => {
                reference
            }
        }
    }

    fn small_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Small { bytes, .. } => Some(bytes),
            Self::Large(_) | Self::Sealed(_) => None,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredReceipt {
    fingerprint: [u8; 32],
    version: VersionId,
    deleted: bool,
    expires_at_unix_millis: u64,
    /// Present for 0.5.1 distributed object mutations and bounded by this
    /// receipt's existing expiry. Released 0.5.0 receipts decode as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    object_mutation: Option<crate::model::ObjectMutation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    definition_transition: Option<DefinitionTransition>,
}

pub(crate) type PendingBlobReferences = BTreeMap<Vec<u8>, BlobReferenceState>;

impl Store {
    /// Allocate one node-scoped Snowflake identity for an ordinary derived
    /// object such as a format-v4 index segment. A durable publication made
    /// afterward advances the same persisted high-water mark, so an identity
    /// lost before publication is harmless and a published identity cannot be
    /// reused after restart.
    pub fn allocate_snowflake_id(&self) -> Result<u64> {
        Ok(self.clock.next()?.0)
    }

    /// Configured local directory for anonymous, non-authoritative payload
    /// work files. Callers must not create durable records in this directory.
    pub fn payload_spool_directory(&self) -> &std::path::Path {
        self.blobs.root()
    }

    /// Read bounded operational signals from RocksDB and its shared metadata
    /// memory resources.
    pub fn metadata_runtime_metrics(&self) -> MetadataRuntimeMetrics {
        let mut metrics = MetadataRuntimeMetrics {
            block_cache_capacity_bytes: METADATA_BLOCK_CACHE_BYTES as u64,
            block_cache_usage_bytes: self._metadata_memory.block_cache.get_usage() as u64,
            block_cache_pinned_bytes: self._metadata_memory.block_cache.get_pinned_usage() as u64,
            write_buffer_capacity_bytes: self
                ._metadata_memory
                .write_buffer_manager
                .get_buffer_size() as u64,
            write_buffer_usage_bytes: self._metadata_memory.write_buffer_manager.get_usage() as u64,
            mutation_receipt_max_entries: self.mutation_receipt_retention.max_entries,
            mutation_receipt_max_bytes: self.mutation_receipt_retention.max_bytes,
            ..MetadataRuntimeMetrics::default()
        };

        match self.mutation_receipt_runtime_metrics() {
            Ok((entries, bytes, oldest_age_seconds)) => {
                metrics.mutation_receipt_entries = Some(entries);
                metrics.mutation_receipt_bytes = Some(bytes);
                metrics.mutation_receipt_oldest_age_seconds = Some(oldest_age_seconds);
            }
            Err(error) => {
                metrics.note_failure(format!("read mutation receipt runtime metrics: {error}"))
            }
        }

        let value = self.db_property(
            properties::NUM_RUNNING_COMPACTIONS,
            "running_compactions",
            &mut metrics,
        );
        metrics.running_compactions = value;
        let value = self.db_property(
            properties::NUM_RUNNING_FLUSHES,
            "running_flushes",
            &mut metrics,
        );
        metrics.running_flushes = value;
        let value = self.db_property(
            properties::ACTUAL_DELAYED_WRITE_RATE,
            "actual_delayed_write_rate_bytes_per_second",
            &mut metrics,
        );
        metrics.actual_delayed_write_rate_bytes_per_second = value;
        let value = self.db_property(properties::IS_WRITE_STOPPED, "write_stopped", &mut metrics);
        metrics.write_stopped = value;
        let value = self.db_property(
            properties::BACKGROUND_ERRORS,
            "background_errors",
            &mut metrics,
        );
        metrics.background_errors = value;
        let value = self.column_family_property_sum(
            properties::CUR_SIZE_ACTIVE_MEM_TABLE,
            "active_memtable_bytes",
            &mut metrics,
        );
        metrics.active_memtable_bytes = value;
        let value = self.column_family_property_sum(
            properties::CUR_SIZE_ALL_MEM_TABLES,
            "all_memtable_bytes",
            &mut metrics,
        );
        metrics.all_memtable_bytes = value;
        let value = self.column_family_property_sum(
            properties::ESTIMATE_TABLE_READERS_MEM,
            "table_reader_bytes",
            &mut metrics,
        );
        metrics.table_reader_bytes = value;
        let value = self.column_family_property_sum(
            properties::ESTIMATE_PENDING_COMPACTION_BYTES,
            "pending_compaction_bytes",
            &mut metrics,
        );
        metrics.pending_compaction_bytes = value;
        let value = self.column_family_property_sum(
            properties::NUM_IMMUTABLE_MEM_TABLE,
            "immutable_memtables",
            &mut metrics,
        );
        metrics.immutable_memtables = value;
        let value = self.column_family_property_sum(
            properties::COMPACTION_PENDING,
            "compaction_pending_column_families",
            &mut metrics,
        );
        metrics.compaction_pending_column_families = value;
        let value = self.column_family_property_sum(
            properties::MEM_TABLE_FLUSH_PENDING,
            "flush_pending_column_families",
            &mut metrics,
        );
        metrics.flush_pending_column_families = value;
        metrics
    }

    fn mutation_receipt_runtime_metrics(&self) -> Result<(u64, u64, u64), MutationError> {
        let status = self.mutation_receipt_status()?;
        let receipts = self.cf(CF_RECEIPTS)?;
        let mut expiry = self.db.iterator_cf(
            receipts,
            IteratorMode::From(
                &[STORAGE_KEY_FORMAT_VERSION, RECEIPT_EXPIRY_PREFIX],
                Direction::Forward,
            ),
        );
        let oldest_expiry = match expiry.next() {
            Some(entry) => {
                let (key, _) = entry.map_err(storage_error)?;
                parse_receipt_expiry_key(&key)?.map(|(expires_at, _)| expires_at)
            }
            None => None,
        };
        if status.entries != 0 && oldest_expiry.is_none() {
            return Err(MutationError::Storage(
                "mutation receipt count is non-zero without an expiry index".into(),
            ));
        }
        let retention_millis = self
            .mutation_receipt_retention
            .retention_seconds
            .checked_mul(1_000)
            .ok_or_else(|| MutationError::Storage("mutation receipt retention overflow".into()))?;
        let now = now_unix_millis()?;
        let oldest_age_seconds = if status.entries == 0 {
            0
        } else {
            let expires_at = oldest_expiry.expect("non-zero receipt count checked above");
            now.saturating_sub(expires_at.saturating_sub(retention_millis)) / 1_000
        };
        Ok((status.entries, status.bytes, oldest_age_seconds))
    }

    fn db_property(
        &self,
        property: &rocksdb::properties::PropName,
        signal: &'static str,
        metrics: &mut MetadataRuntimeMetrics,
    ) -> Option<u64> {
        match self.db.property_int_value(property) {
            Ok(Some(value)) => Some(value),
            Ok(None) => {
                metrics.note_unavailable(signal);
                None
            }
            Err(error) => {
                metrics.note_failure(format!("read RocksDB property {signal}: {error}"));
                None
            }
        }
    }

    fn column_family_property_sum(
        &self,
        property: &rocksdb::properties::PropName,
        signal: &'static str,
        metrics: &mut MetadataRuntimeMetrics,
    ) -> Option<u64> {
        let mut total = 0_u64;
        for name in
            std::iter::once(DEFAULT_COLUMN_FAMILY_NAME).chain(COLUMN_FAMILIES.iter().copied())
        {
            let Some(column_family) = self.db.cf_handle(name) else {
                metrics.note_failure(format!("missing metadata column family {name}"));
                return None;
            };
            let value = match self.db.property_int_value_cf(column_family, property) {
                Ok(Some(value)) => value,
                Ok(None) => {
                    metrics.note_unavailable(signal);
                    return None;
                }
                Err(error) => {
                    metrics.note_failure(format!(
                        "read RocksDB property {signal} for {name}: {error}"
                    ));
                    return None;
                }
            };
            let Some(next) = total.checked_add(value) else {
                metrics.note_failure(format!("RocksDB metric {signal} overflowed u64"));
                return None;
            };
            total = next;
        }
        Some(total)
    }

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
        tokio::fs::create_dir_all(&options.root).await?;
        let metadata_path = options.root.join("metadata");
        let mut db_options = Options::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        let metadata_memory = Arc::new(MetadataMemoryResources::new());
        let descriptors = std::iter::once(DEFAULT_COLUMN_FAMILY_NAME)
            .chain(COLUMN_FAMILIES.iter().copied())
            .map(|name| ColumnFamilyDescriptor::new(name, metadata_memory.column_family_options()))
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
        let blobs = BlobStore::open(options.root.join("blobs")).await?;
        let store = Self {
            db,
            _metadata_memory: metadata_memory,
            blobs,
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
            definition_state_lock: Arc::new(std::sync::Mutex::new(())),
            node_id: options.node_id,
            sync_writes: options.sync_writes,
            watch_retention: options.watch_retention,
            mutation_receipt_retention: options.mutation_receipt_retention,
            awaiting_publish_ttl_millis,
            watch_source_epoch,
            watch_token_key,
            source_journal_reference_safe_through: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            source_journal_progress_debt_peak_entries: Arc::new(std::sync::atomic::AtomicU64::new(
                0,
            )),
            source_journal_progress_debt_peak_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            mutation_capacity_notify: Arc::new(tokio::sync::Notify::new()),
            watch_notify: tokio::sync::watch::channel(()).0,
            definition_assignment_notify: tokio::sync::broadcast::channel(
                DEFINITION_ASSIGNMENT_NOTIFICATION_CAPACITY,
            )
            .0,
            #[cfg(test)]
            policy_lookup_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            test_identity_lock: Arc::new(std::sync::Mutex::new(())),
        };
        let durable_floor = store.local_watch_status()?.retention_floor;
        store
            .source_journal_reference_safe_through
            .store(durable_floor, std::sync::atomic::Ordering::Release);
        store.enforce_local_watch_retention()?;
        Ok(store)
    }

    pub(crate) fn tenant_id_by_name(
        &self,
        tenant: &str,
    ) -> Result<Option<TenantId>, MutationError> {
        let storage_tenant = StorageTenantId::parse(tenant)
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        self.db
            .get_cf(self.cf(CF_NAMES)?, tenant_name_key(tenant))
            .map_err(storage_error)?
            .map(|encoded| {
                let id = crate::LogicalRecordId::TenantNameClaim {
                    storage_tenant: storage_tenant.clone(),
                };
                match decode_current_value(&id, &encoded).map_err(storage_error)? {
                    crate::LogicalRecordValue::TenantNameClaim { tenant_id, .. } => {
                        Ok(TenantId(tenant_id))
                    }
                    _ => Err(MutationError::Storage(
                        "tenant name claim has the wrong logical type".into(),
                    )),
                }
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
                let id = crate::LogicalRecordId::BucketNameClaim {
                    tenant_id: tenant_id.0,
                    bucket: bucket.to_owned(),
                };
                match decode_current_value(&id, &encoded).map_err(storage_error)? {
                    crate::LogicalRecordValue::BucketNameClaim { bucket_id, .. } => {
                        Ok(BucketId(bucket_id))
                    }
                    _ => Err(MutationError::Storage(
                        "bucket name claim has the wrong logical type".into(),
                    )),
                }
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

    /// Resolves mutable tenant and bucket names to their permanent storage
    /// identities without exposing RocksDB column families or key encodings.
    pub fn resolve_bucket_ids(
        &self,
        tenant: &str,
        bucket: &str,
    ) -> Result<(u64, u64), MutationError> {
        let identity = self.resolve_bucket_identity(tenant, bucket)?;
        if identity.tenant_id.0 == 0 || identity.bucket_id.0 == 0 {
            return Err(MutationError::Storage(
                "stable tenant and bucket IDs must be non-zero".into(),
            ));
        }
        Ok((identity.tenant_id.0, identity.bucket_id.0))
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

    /// Runs one internal orchestration step while holding the same exact
    /// logical-path lock used by every ordinary Put/Delete/Publish mutation.
    /// This does not use the atomic-program lock table and owns no additional
    /// authority or persistent state.
    pub async fn with_ordinary_object_path_lock<T, F, Fut>(
        &self,
        key: &ObjectKey,
        operation: F,
    ) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let _guard = self.ordinary_locks.acquire(&[object_path(key)]).await;
        operation().await
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
        let Some(encoded) = self
            .db
            .get_cf(self.cf(CF_BUCKET_OPTIONS)?, key)
            .map_err(storage_error)?
        else {
            return Ok(ObjectVersioning::default());
        };
        let identity = BucketIdentity::decode(key).map_err(storage_error)?;
        let id = crate::LogicalRecordId::BucketOptions {
            tenant_id: identity.tenant_id.0,
            bucket_id: identity.bucket_id.0,
        };
        match decode_current_value(&id, &encoded).map_err(storage_error)? {
            crate::LogicalRecordValue::BucketOptions {
                tenant_id,
                bucket_id,
                versioning,
            } if tenant_id == identity.tenant_id.0 && bucket_id == identity.bucket_id.0 => {
                Ok(versioning)
            }
            _ => Err(MutationError::Storage(
                "bucket options have the wrong logical type or identity".into(),
            )),
        }
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
        let Some(encoded) = self
            .db
            .get_cf(self.cf(CF_POLICIES)?, key)
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        let identity = BucketIdentity::decode(key).map_err(storage_error)?;
        let id = crate::LogicalRecordId::BucketPolicy {
            tenant_id: identity.tenant_id.0,
            bucket_id: identity.bucket_id.0,
        };
        match decode_current_value(&id, &encoded).map_err(storage_error)? {
            crate::LogicalRecordValue::BucketPolicy {
                tenant_id,
                bucket_id,
                policy,
            } if tenant_id == identity.tenant_id.0 && bucket_id == identity.bucket_id.0 => {
                Ok(Some(policy))
            }
            _ => Err(MutationError::Storage(
                "bucket policy has the wrong logical type or identity".into(),
            )),
        }
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

impl MetadataRuntimeMetrics {
    fn note_unavailable(&mut self, property: &'static str) {
        self.unavailable_properties = self.unavailable_properties.saturating_add(1);
        self.first_unavailable_property.get_or_insert(property);
    }

    fn note_failure(&mut self, error: String) {
        self.property_collection_failures = self.property_collection_failures.saturating_add(1);
        self.first_collection_error.get_or_insert(error);
    }
}

mod authz_journal;
mod blob_references;
pub(crate) mod definition_state;
mod delete_version;
mod derived_consumers;
mod distributed_publish_batch;
mod index_retention_due;
mod journal_capacity;
mod journal_routes;
mod mutations;
mod object_mutation_replica_batch;
mod object_snapshot;
mod object_snapshot_scan;
mod payload;
mod payload_handoff;
mod reads;
mod reference_deltas;
mod reference_proofs;
mod retained_snapshot_scan;
mod shards;
mod watch_journal;

pub use object_snapshot::{
    MAX_OBJECT_RECORD_EXPORT_BYTES, MAX_OBJECT_RECORD_EXPORT_RECORDS, ObjectPathSnapshot,
    ObjectRecordCursor, ObjectRecordExport, ObjectRecordExportPage, ObjectSnapshotApplied,
    ObjectSnapshotError,
};
pub use object_snapshot_scan::{
    CurrentHeadCursor, CurrentObjectSnapshot, CurrentObjectSnapshotFrame,
    CurrentObjectSnapshotPage, CurrentObjectSnapshotScan, MAX_CURRENT_HEAD_SNAPSHOT_BYTES,
    MAX_CURRENT_HEAD_SNAPSHOT_RECORDS,
};
pub use payload::{
    CompleteCopySealOutcome, LocalPayloadPresence, PayloadArtifactState, PayloadStoreError,
};
pub use payload_handoff::{
    MAX_PAYLOAD_HANDOFF_EXPORT_RECORDS, PayloadArtifactCursor, PayloadArtifactIdentity,
    PayloadArtifactSnapshot, PayloadArtifactSnapshotPage,
};
pub use reference_proofs::{
    MAX_REFERENCE_PROOF_EXPORT_BYTES, MAX_REFERENCE_PROOF_EXPORT_RECORDS,
    MAX_REFERENCE_PROOF_PRUNE_BYTES, MAX_REFERENCE_PROOF_PRUNE_RECORDS, ReferenceProofCursor,
    ReferenceProofExportError, ReferenceProofPage, ReferenceProofPruneError,
    ReferenceProofPruneResult,
};
pub use retained_snapshot_scan::{
    RetainedHeadState, RetainedObjectCursor, RetainedObjectSnapshot, RetainedObjectSnapshotFrame,
    RetainedObjectSnapshotPage, RetainedObjectSnapshotScan, RetainedVersionCursor,
};
pub use shards::{ShardIdentity, ShardReader, ShardSealOutcome, ShardStoreError};

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

fn initialize_local_watch_metadata(
    db: &DB,
    metadata: &rocksdb::ColumnFamily,
    sync_writes: bool,
) -> Result<([u8; 32], [u8; 32])> {
    let epoch = db.get_cf(metadata, LOCAL_INVALIDATION_EPOCH_KEY)?;
    let token_key = db.get_cf(metadata, LOCAL_INVALIDATION_TOKEN_KEY)?;
    let offset = db.get_cf(metadata, LOCAL_INVALIDATION_OFFSET_KEY)?;
    let settled = db.get_cf(metadata, LOCAL_INVALIDATION_SETTLED_KEY)?;
    let floor = db.get_cf(metadata, LOCAL_INVALIDATION_FLOOR_KEY)?;
    let count = db.get_cf(metadata, LOCAL_INVALIDATION_COUNT_KEY)?;
    let bytes = db.get_cf(metadata, LOCAL_INVALIDATION_BYTES_KEY)?;
    let all_absent = epoch.is_none()
        && token_key.is_none()
        && offset.is_none()
        && settled.is_none()
        && floor.is_none()
        && count.is_none()
        && bytes.is_none();
    let all_present = epoch.is_some()
        && token_key.is_some()
        && offset.is_some()
        && settled.is_some()
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
            LOCAL_INVALIDATION_SETTLED_KEY,
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
    let decode_counter = |encoded: Vec<u8>, name: &str| -> Result<u64> {
        let bytes: [u8; size_of::<u64>()] = encoded
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("local watch {name} is malformed"))?;
        Ok(u64::from_be_bytes(bytes))
    };
    let tail = decode_counter(offset.expect("checked present"), "tail")?;
    let floor = decode_counter(floor.expect("checked present"), "retention floor")?;
    let settled = decode_counter(settled.expect("checked present"), "settled cursor")?;
    if floor > settled || settled > tail {
        anyhow::bail!(
            "local watch settled cursor {settled} is outside retention floor {floor} through tail {tail}"
        );
    }
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
    mut results: BTreeMap<usize, Result<MutationReceipt, MutationError>>,
    early: BTreeMap<usize, MutationError>,
    prepared: Vec<(usize, PreparedOperation)>,
    error: MutationError,
) -> Vec<BatchOutcome> {
    results.extend(early.into_iter().map(|(index, error)| (index, Err(error))));
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

pub(crate) fn encode_object_versioning(versioning: ObjectVersioning) -> [u8; 1] {
    [match versioning {
        ObjectVersioning::Unversioned => 0,
        ObjectVersioning::Enabled => 1,
    }]
}

pub(crate) fn decode_object_versioning(encoded: &[u8]) -> Result<ObjectVersioning, MutationError> {
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
