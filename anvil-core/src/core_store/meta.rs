use anyhow::{Context, Result, anyhow, bail};
use blake3::Hasher;
use prost::{Enumeration, Message};
use rocksdb::{
    ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, ReadOptions, Snapshot,
    WriteBatch, WriteOptions,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex as StdMutex, Weak};
use std::time::Instant;

use super::deterministic_proto::sha256_hex;
use super::transaction_manifest_proto::{
    decode_manifest_locator_proto, encode_manifest_locator_proto,
};
use super::types::CoreManifestLocator;

mod key;
mod scans;
pub use key::{CoreMetaTuplePart, core_meta_tuple_key};
use key::{
    core_meta_key, decode_core_meta_table_id, decode_core_meta_tuple_key,
    exclusive_prefix_successor,
};

pub const CF_META_VERSION: &str = "cf_meta_version";
pub const CF_ROOT_CACHE: &str = "cf_root_cache";
pub const CF_TRANSACTIONS: &str = "cf_transactions";
pub const CF_OBJECT_HEADS: &str = "cf_object_heads";
pub const CF_OBJECT_VERSIONS: &str = "cf_object_versions";
pub const CF_INLINE_PAYLOADS: &str = "cf_inline_payloads";
pub const CF_STREAM_HEADS: &str = "cf_stream_heads";
pub const CF_STREAM_RECORDS: &str = "cf_stream_records";
pub const CF_INDEX_DEFS: &str = "cf_index_defs";
pub const CF_INDEX_ROWS: &str = "cf_index_rows";
pub const CF_BOUNDARY: &str = "cf_boundary";
pub const CF_AUTHZ: &str = "cf_authz";
pub const CF_PERSONALDB: &str = "cf_personaldb";
pub const CF_REGISTRY: &str = "cf_registry";
pub const CF_MESH: &str = "cf_mesh";
pub const CF_LEASES_FENCES: &str = "cf_leases_fences";
pub const CF_MATERIALISATION: &str = "cf_materialisation";
pub const CF_REFCOUNTS: &str = "cf_refcounts";
pub const CF_OBSERVABILITY: &str = "cf_observability";

pub const TABLE_META_SCHEMA_VERSION_ROW: u16 = 0x8001;
pub const TABLE_ROOT_CACHE_ROW: u16 = 0x8002;
pub const TABLE_TRANSACTION_LOCATOR_ROW: u16 = 0x8003;
pub const TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW: u16 = 0x8004;
pub const TABLE_INLINE_MANIFEST_BODY_ROW: u16 = 0x8005;
pub const TABLE_EXPLICIT_TRANSACTION_ROW: u16 = 0x8006;
pub const TABLE_PENDING_MUTATION_ROW: u16 = 0x8007;
pub const TABLE_NATIVE_IDEMPOTENCY_ROW: u16 = 0x8008;
pub const TABLE_LOCAL_ADMISSION_EVIDENCE_ROW: u16 = 0x8009;
pub const TABLE_TRANSACTION_MANIFEST_BODY_ROW: u16 = 0x800a;
pub const TABLE_ROOT_PUBLICATION_INTENT_ROW: u16 = 0x800b;
pub const TABLE_OBJECT_HEAD_ROW: u16 = 0x8101;
pub const TABLE_OBJECT_VERSION_META_ROW: u16 = 0x8102;
pub const TABLE_INLINE_PAYLOAD_ROW: u16 = 0x8103;
pub const TABLE_MANIFEST_CAS_CURRENT_ROW: u16 = 0x8104;
pub const TABLE_MULTIPART_UPLOAD_CURRENT_ROW: u16 = 0x8105;
pub const TABLE_MULTIPART_PART_CURRENT_ROW: u16 = 0x8106;
pub const TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW: u16 = 0x8107;
pub const TABLE_OBJECT_WATCH_CURSOR_ROW: u16 = 0x8108;
pub const TABLE_OBJECT_SHARD_REPAIR_ROW: u16 = 0x8109;
pub const TABLE_STREAM_HEAD_ROW: u16 = 0x8201;
pub const TABLE_STREAM_RECORD_INDEX_ROW: u16 = 0x8202;
pub const TABLE_STREAM_IDEMPOTENCY_ROW: u16 = 0x8203;
pub const TABLE_INDEX_DEFINITION_ROW: u16 = 0x8301;
pub const TABLE_INDEX_ROW: u16 = 0x8302;
pub const TABLE_DERIVED_INDEX_PROOF_ROW: u16 = 0x8303;
pub const TABLE_BOUNDARY_SCHEMA_ROW: u16 = 0x8401;
pub const TABLE_BOUNDARY_VALUE_ROW: u16 = 0x8402;
pub const TABLE_BOUNDARY_MIGRATION_ROW: u16 = 0x8403;
pub const TABLE_BOUNDARY_SCHEMA_CURRENT_ROW: u16 = 0x8404;
pub const TABLE_AUTHZ_SCHEMA_ROW: u16 = 0x8501;
pub const TABLE_AUTHZ_TUPLE_PAGE_ROW: u16 = 0x8502;
pub const TABLE_AUTHZ_IDEMPOTENCY_RECEIPT_ROW: u16 = 0x8503;
pub const TABLE_AUTHZ_HEAD_ROW: u16 = 0x8504;
pub const TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW: u16 = 0x8505;
pub const TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW: u16 = 0x8506;
pub const TABLE_PERSONALDB_GROUP_ROW: u16 = 0x8601;
pub const TABLE_PERSONALDB_DATA_LOCATOR_ROW: u16 = 0x8602;
pub const TABLE_PERSONALDB_PROPOSAL_CLAIM_ROW: u16 = 0x8603;
pub const TABLE_PERSONALDB_PROPOSAL_SLOT_ROW: u16 = 0x8604;
pub const TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW: u16 = 0x8605;
pub const TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW: u16 = 0x8606;
pub const TABLE_PERSONALDB_WITNESS_RECEIPT_ROW: u16 = 0x8607;
pub const TABLE_REGISTRY_VERSION_ROW: u16 = 0x8701;
pub const TABLE_REGISTRY_BLOB_LOCATOR_ROW: u16 = 0x8702;
pub const TABLE_GATEWAY_METADATA_ROW: u16 = 0x8703;
pub const TABLE_GIT_SOURCE_MANIFEST_ROW: u16 = 0x8704;
pub const TABLE_GATEWAY_MOUNT_ROUTE_ROW: u16 = 0x8705;
pub const TABLE_MESH_NODE_ROW: u16 = 0x8801;
pub const TABLE_MESH_PARTITION_ROW: u16 = 0x8802;
pub const TABLE_REPAIR_FINDING_ROW: u16 = 0x8803;
pub const TABLE_BUCKET_CURRENT_BY_NAME_ROW: u16 = 0x8804;
pub const TABLE_BUCKET_CURRENT_BY_ID_ROW: u16 = 0x8805;
pub const TABLE_CONTROL_CURRENT_ROW: u16 = 0x8806;
pub const TABLE_SYSTEM_BOOTSTRAP_MARKER_ROW: u16 = 0x8807;
pub const TABLE_ROOT_CATALOG_CURRENT_ROW: u16 = 0x8808;
pub const TABLE_QUORUM_PROFILE_CURRENT_ROW: u16 = 0x8809;
pub const TABLE_REPAIR_FINDING_HEAD_ROW: u16 = 0x880a;
pub const TABLE_REPAIR_FINDING_ID_ROW: u16 = 0x880b;
pub const TABLE_BUCKET_ID_ALLOCATOR_ROW: u16 = 0x880c;
pub const TABLE_BUCKET_EVENT_HEAD_ROW: u16 = 0x880d;
pub const TABLE_OWNERSHIP_FENCE_ROW: u16 = 0x8901;
pub const TABLE_PARTITION_OWNER_ROW: u16 = 0x8902;
pub const TABLE_TASK_LEASE_ROW: u16 = 0x8903;
pub const TABLE_TASK_CURRENT_ROW: u16 = 0x8904;
pub const TABLE_CORE_FENCE_ROW: u16 = 0x8905;
pub const TABLE_ROOT_FAILOVER_VOTE_ROW: u16 = 0x8906;
pub const TABLE_ROOT_FAILOVER_CERTIFICATE_ROW: u16 = 0x8907;
pub const TABLE_MATERIALISATION_CURSOR_ROW: u16 = 0x8a01;
pub const TABLE_WRITER_SEGMENT_ROW: u16 = 0x8a02;
pub const TABLE_WATCH_CHECKPOINT_ROW: u16 = 0x8a03;
pub const TABLE_WRITER_HEAD_ROW: u16 = 0x8a04;
pub const TABLE_LANDED_BYTE_REF_ROW: u16 = 0x8b02;
pub const TABLE_REFCOUNT_ROW: u16 = 0x8b01;
pub const TABLE_OBSERVABILITY_CURSOR_ROW: u16 = 0x8c01;
pub const TABLE_DIAGNOSTIC_ROW: u16 = 0x8c02;
pub const TABLE_NODE_SIGNING_KEYPAIR_ROW: u16 = 0x8d01;
pub const TABLE_PERSONALDB_SIGNING_KEY_ROW: u16 = 0x8d02;
pub const TABLE_PERSONALDB_SIGNING_KEY_HEAD_ROW: u16 = 0x8d03;
pub const TABLE_LOCAL_NODE_IDENTITY_ROW: u16 = 0x8d04;

const CORE_META_VALUE_SCHEMA_VERSION: u32 = 1;
pub(crate) const CORE_META_MAX_VALUE_BYTES: usize = 64 * 1024;
pub(crate) const CORE_META_MAX_INLINE_PAYLOAD_BYTES: usize = 32 * 1024;
pub(crate) const CORE_META_INLINE_MANIFEST_BODY_MAX_BYTES: usize = 32 * 1024;
pub(crate) const CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES: usize = 16 * 1024;
const CORE_META_OBJECT_VERSION_MAX_PAYLOAD_BYTES: usize = CORE_META_MAX_VALUE_BYTES;
pub(crate) const CORE_META_MAX_SCAN_PAGE_ROWS: usize = 4096;

static META_DB_CACHE: LazyLock<StdMutex<BTreeMap<PathBuf, Weak<DB>>>> =
    LazyLock::new(|| StdMutex::new(BTreeMap::new()));

#[cfg(feature = "coremeta-perf-gate")]
static CORE_META_GET_PROBE: LazyLock<StdMutex<BTreeMap<(String, u16), u64>>> =
    LazyLock::new(|| StdMutex::new(BTreeMap::new()));
#[cfg(feature = "coremeta-perf-gate")]
static CORE_META_GET_PROBE_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(feature = "coremeta-perf-gate")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreMetaGetProbeEntry {
    pub column_family: String,
    pub table_id: u16,
    pub calls: u64,
}

#[cfg(feature = "coremeta-perf-gate")]
pub fn reset_coremeta_get_probe() {
    CORE_META_GET_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
    CORE_META_GET_PROBE_ENABLED.store(true, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(feature = "coremeta-perf-gate")]
pub fn take_coremeta_get_probe() -> Vec<CoreMetaGetProbeEntry> {
    CORE_META_GET_PROBE_ENABLED.store(false, std::sync::atomic::Ordering::Relaxed);
    let mut counts = CORE_META_GET_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    std::mem::take(&mut *counts)
        .into_iter()
        .map(|((column_family, table_id), calls)| CoreMetaGetProbeEntry {
            column_family,
            table_id,
            calls,
        })
        .collect()
}

#[cfg(feature = "coremeta-perf-gate")]
fn record_coremeta_get_probe(column_family: &str, table_id: u16) {
    if !CORE_META_GET_PROBE_ENABLED.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let mut counts = CORE_META_GET_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let calls = counts
        .entry((column_family.to_string(), table_id))
        .or_default();
    *calls = calls.saturating_add(1);
}

#[derive(Clone)]
pub struct CoreMetaStore {
    db: Arc<DB>,
}

pub(crate) struct CoreMetaReadSnapshot<'a> {
    store: &'a CoreMetaStore,
    snapshot: Snapshot<'a>,
}

pub(crate) trait CoreMetaReader {
    fn get(&self, cf: &'static str, table_id: u16, tuple_key: &[u8]) -> Result<Option<Vec<u8>>>;
}

impl std::fmt::Debug for CoreMetaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreMetaStore").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct CoreMetaEncodedRow<'a> {
    pub cf: &'a str,
    pub core_meta_key: &'a [u8],
    pub value_envelope: &'a [u8],
    pub delete_marker: bool,
}

#[derive(Debug, Clone)]
pub struct CoreMetaEncodedOwnedRow {
    pub cf: String,
    pub core_meta_key: Vec<u8>,
    pub value_envelope: Vec<u8>,
    pub delete_marker: bool,
    pub root_key_hash: String,
    pub root_generation: u64,
    pub visibility_state: CoreMetaVisibilityState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreMetaEncodedRowsCursor {
    pub cf: String,
    pub core_meta_key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CoreMetaEncodedRowsPage {
    pub rows: Vec<CoreMetaEncodedOwnedRow>,
    pub next_cursor: Option<CoreMetaEncodedRowsCursor>,
}

pub struct CoreMetaRecord {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaValueEnvelope {
    #[prost(uint32, tag = "1")]
    table_id: u32,
    #[prost(uint32, tag = "2")]
    schema_version: u32,
    #[prost(bytes, tag = "3")]
    payload: Vec<u8>,
    #[prost(string, tag = "4")]
    payload_hash: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
#[repr(i32)]
pub enum CoreMetaVisibilityState {
    Unspecified = 0,
    Pending = 1,
    Committed = 2,
    Aborted = 3,
    RolledBack = 4,
}

#[derive(Clone, PartialEq, Message)]
pub struct CoreMetaRowCommonProto {
    #[prost(string, tag = "1")]
    pub realm_id: String,
    #[prost(string, tag = "2")]
    pub root_key_hash: String,
    #[prost(uint64, tag = "3")]
    pub root_generation: u64,
    #[prost(string, tag = "4")]
    pub transaction_id: String,
    #[prost(enumeration = "CoreMetaVisibilityState", tag = "5")]
    pub visibility_state: i32,
    #[prost(uint64, tag = "6")]
    pub created_at_unix_nanos: u64,
    #[prost(uint32, tag = "7")]
    pub payload_schema_version: u32,
}

impl CoreMetaRowCommonProto {
    pub(crate) fn visibility_state_enum(&self) -> CoreMetaVisibilityState {
        CoreMetaVisibilityState::try_from(self.visibility_state)
            .unwrap_or(CoreMetaVisibilityState::Unspecified)
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct CoreMetaLocatorProto {
    #[prost(string, tag = "1")]
    pub storage_kind: String,
    #[prost(string, tag = "2")]
    pub manifest_hash: String,
    #[prost(string, tag = "3")]
    pub root_key_hash: String,
    #[prost(uint64, tag = "4")]
    pub root_generation: u64,
    #[prost(string, tag = "5")]
    pub locator_hash: String,
    #[prost(bytes, tag = "6")]
    pub encoded_locator: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CoreMetaInlineOrLocatorProto {
    #[prost(bytes, tag = "1")]
    pub inline_payload: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub locator: Option<CoreMetaLocatorProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaCommonPrefixProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaSchemaVersionRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(uint32, tag = "2")]
    core_meta_schema_version: u32,
    #[prost(string, tag = "3")]
    created_by_binary_version: String,
    #[prost(string, tag = "4")]
    minimum_supported_binary_version: String,
    #[prost(string, tag = "5")]
    column_family_set_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct InlinePayloadRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    payload_hash: String,
    #[prost(uint64, tag = "3")]
    raw_payload_length: u64,
    #[prost(bytes, tag = "4")]
    payload_bytes: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaCommitEvidenceRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    certificate_hash: String,
    #[prost(string, tag = "3")]
    committed_batch_hash: String,
    #[prost(bytes, tag = "4")]
    certificate_bytes: Vec<u8>,
    #[prost(string, repeated, tag = "5")]
    certificate_persist_receipt_hashes: Vec<String>,
    #[prost(bytes = "vec", repeated, tag = "6")]
    certificate_persist_receipt_bytes: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaPendingBatchMarkerRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    pending_batch_hash: String,
    #[prost(string, tag = "3")]
    root_key_hash: String,
    #[prost(uint64, tag = "4")]
    expected_root_generation: u64,
    #[prost(uint64, tag = "5")]
    post_root_generation: u64,
    #[prost(string, tag = "6")]
    transaction_id: String,
    #[prost(uint64, tag = "7")]
    core_meta_row_count: u64,
}

impl CoreMetaStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(existing) = META_DB_CACHE
            .lock()
            .expect("CoreStore metadata DB cache lock poisoned")
            .get(&path)
            .and_then(Weak::upgrade)
        {
            let store = Self { db: existing };
            store.ensure_schema_version_row()?;
            return Ok(store);
        }

        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_max_open_files(512);
        let descriptors = column_families()
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, cf_options(name)))
            .collect::<Vec<_>>();
        let db = Arc::new(
            DB::open_cf_descriptors(&options, &path, descriptors).with_context(|| {
                format!("open CoreStore RocksDB metadata at {}", path.display())
            })?,
        );
        let store = Self { db };
        store.ensure_schema_version_row()?;
        META_DB_CACHE
            .lock()
            .expect("CoreStore metadata DB cache lock poisoned")
            .insert(path, Arc::downgrade(&store.db));
        Ok(store)
    }

    fn ensure_schema_version_row(&self) -> Result<()> {
        let key = core_meta_schema_key();
        if self
            .get(CF_META_VERSION, TABLE_META_SCHEMA_VERSION_ROW, &key)?
            .is_some()
        {
            return Ok(());
        }
        if self.contains_any_row()? {
            bail!(
                "CoreMeta store has no current physical-format marker; delete and recreate this pre-release store"
            );
        }
        let row = CoreMetaSchemaVersionRowProto {
            common: Some(CoreMetaRowCommonProto {
                realm_id: String::new(),
                root_key_hash: String::new(),
                root_generation: 0,
                transaction_id: String::new(),
                visibility_state: CoreMetaVisibilityState::Committed as i32,
                created_at_unix_nanos: 0,
                payload_schema_version: CORE_META_VALUE_SCHEMA_VERSION,
            }),
            core_meta_schema_version: CORE_META_VALUE_SCHEMA_VERSION,
            created_by_binary_version: env!("CARGO_PKG_VERSION").to_string(),
            minimum_supported_binary_version: env!("CARGO_PKG_VERSION").to_string(),
            column_family_set_hash: column_family_set_hash(),
        };
        let mut payload = Vec::new();
        row.encode(&mut payload)?;
        self.put(
            CF_META_VERSION,
            TABLE_META_SCHEMA_VERSION_ROW,
            &key,
            &payload,
        )
    }

    fn contains_any_row(&self) -> Result<bool> {
        for cf_name in column_families() {
            let cf = self.cf(cf_name)?;
            if let Some(item) = self.db.iterator_cf(&cf, IteratorMode::Start).next() {
                let _ = item?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn put(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_key: &[u8],
        payload: &[u8],
    ) -> Result<()> {
        self.put_named(cf, table_id, tuple_key, payload)
    }

    pub fn put_named(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
        payload: &[u8],
    ) -> Result<()> {
        let key = core_meta_key(table_id, 0, tuple_key)?;
        let value = encode_envelope(cf, table_id, payload)?;
        let bytes = (key.len() + value.len()) as u64;
        let cf_name = cf;
        let cf = self.cf(cf_name)?;
        let started_at = Instant::now();
        self.db
            .put_cf_opt(&cf, key, value, &durable_write_options())?;
        crate::perf::record_coremeta_duration(
            "put",
            cf_name,
            table_id,
            1,
            bytes,
            started_at.elapsed(),
        );
        Ok(())
    }

    pub fn delete(&self, cf: &'static str, table_id: u16, tuple_key: &[u8]) -> Result<()> {
        self.delete_named(cf, table_id, tuple_key)
    }

    pub fn delete_named(&self, cf: &str, table_id: u16, tuple_key: &[u8]) -> Result<()> {
        validate_meta_payload(cf, table_id, 0)?;
        let key = core_meta_key(table_id, 0, tuple_key)?;
        let bytes = key.len() as u64;
        let cf_name = cf;
        let cf = self.cf(cf_name)?;
        let started_at = Instant::now();
        self.db.delete_cf_opt(&cf, key, &durable_write_options())?;
        crate::perf::record_coremeta_duration(
            "delete",
            cf_name,
            table_id,
            1,
            bytes,
            started_at.elapsed(),
        );
        Ok(())
    }

    pub fn put_inline_payload(&self, tuple_key: &[u8], raw_payload: &[u8]) -> Result<()> {
        let payload =
            encode_core_meta_inline_payload_row(raw_payload, local_committed_row_common())?;
        self.put(
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            tuple_key,
            &payload,
        )
    }

    pub fn get_inline_payload(&self, tuple_key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, tuple_key)?
            .map(|payload| decode_inline_payload_row(&payload))
            .transpose()
    }

    pub fn get(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        self.get_named(cf, table_id, tuple_key)
    }

    pub fn get_named(&self, cf: &str, table_id: u16, tuple_key: &[u8]) -> Result<Option<Vec<u8>>> {
        validate_meta_payload(cf, table_id, 0)?;
        let key = core_meta_key(table_id, 0, tuple_key)?;
        let key_bytes = key.len() as u64;
        let cf_name = cf;
        let cf = self.cf(cf_name)?;
        let started_at = Instant::now();
        let value = self.db.get_cf(&cf, key)?;
        #[cfg(feature = "coremeta-perf-gate")]
        record_coremeta_get_probe(cf_name, table_id);
        let bytes = key_bytes + value.as_ref().map(|value| value.len() as u64).unwrap_or(0);
        crate::perf::record_coremeta_duration(
            "get",
            cf_name,
            table_id,
            u64::from(value.is_some()),
            bytes,
            started_at.elapsed(),
        );
        value
            .map(|value| decode_envelope(cf_name, table_id, &value))
            .transpose()
    }

    pub(crate) fn read_snapshot(&self) -> CoreMetaReadSnapshot<'_> {
        CoreMetaReadSnapshot {
            store: self,
            snapshot: self.db.snapshot(),
        }
    }

    pub fn write_batch(&self, ops: &[CoreMetaBatchOp<'_>]) -> Result<()> {
        let mut batch = WriteBatch::default();
        let mut bytes = 0_u64;
        for op in ops {
            let cf = self.cf(op.cf)?;
            let key = core_meta_key(op.table_id, 0, op.tuple_key)?;
            match op.kind {
                CoreMetaBatchOpKind::Put(payload) => {
                    let common = match op.common.clone() {
                        Some(common) => common,
                        None => common_for_envelope_payload(payload)?,
                    };
                    let value = encode_envelope_with_common(op.cf, op.table_id, payload, common)?;
                    bytes = bytes.saturating_add((key.len() + value.len()) as u64);
                    batch.put_cf(&cf, key, value);
                }
                CoreMetaBatchOpKind::Delete => {
                    validate_meta_payload(op.cf, op.table_id, 0)?;
                    if let Some(common) = op.common.as_ref() {
                        validate_coremeta_common_shape(common)?;
                    }
                    bytes = bytes.saturating_add(key.len() as u64);
                    batch.delete_cf(&cf, key);
                }
            }
        }
        let started_at = Instant::now();
        self.db.write_opt(batch, &durable_write_options())?;
        crate::perf::record_coremeta_duration(
            "write_batch",
            "multi",
            0,
            ops.len() as u64,
            bytes,
            started_at.elapsed(),
        );
        Ok(())
    }

    pub(crate) fn write_local_committed_batch(&self, ops: &[CoreMetaBatchOp<'_>]) -> Result<()> {
        let owned = self.encode_batch_ops(ops)?;
        let borrowed = owned
            .iter()
            .map(|row| CoreMetaEncodedRow {
                cf: row.cf.as_str(),
                core_meta_key: &row.core_meta_key,
                value_envelope: &row.value_envelope,
                delete_marker: row.delete_marker,
            })
            .collect::<Vec<_>>();
        self.write_encoded_rows(&borrowed)
    }

    pub fn encode_batch_ops(
        &self,
        ops: &[CoreMetaBatchOp<'_>],
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        let mut rows = Vec::with_capacity(ops.len());
        for op in ops {
            let key = core_meta_key(op.table_id, 0, op.tuple_key)?;
            match op.kind {
                CoreMetaBatchOpKind::Put(payload) => {
                    let common = match op.common.clone() {
                        Some(common) => common,
                        None => common_for_envelope_payload(payload)?,
                    };
                    let value_envelope =
                        encode_envelope_with_common(op.cf, op.table_id, payload, common.clone())?;
                    rows.push(CoreMetaEncodedOwnedRow {
                        cf: op.cf.to_string(),
                        core_meta_key: key,
                        value_envelope,
                        delete_marker: false,
                        root_key_hash: common.root_key_hash.clone(),
                        root_generation: common.root_generation,
                        visibility_state: common.visibility_state_enum(),
                    });
                }
                CoreMetaBatchOpKind::Delete => {
                    validate_meta_payload(op.cf, op.table_id, 0)?;
                    let common = op.common.clone().unwrap_or_else(local_committed_row_common);
                    validate_coremeta_common_shape(&common)?;
                    rows.push(CoreMetaEncodedOwnedRow {
                        cf: op.cf.to_string(),
                        core_meta_key: key,
                        value_envelope: Vec::new(),
                        delete_marker: true,
                        root_key_hash: common.root_key_hash.clone(),
                        root_generation: common.root_generation,
                        visibility_state: common.visibility_state_enum(),
                    });
                }
            }
        }
        Ok(rows)
    }

    pub(crate) fn rebind_encoded_row_common(
        &self,
        row: &mut CoreMetaEncodedOwnedRow,
        common: CoreMetaRowCommonProto,
    ) -> Result<()> {
        validate_coremeta_common_shape(&common)?;
        if row.delete_marker {
            if !row.value_envelope.is_empty() {
                bail!("CoreMeta delete row must not carry a value envelope");
            }
        } else {
            let table_id = decode_core_meta_table_id(&row.core_meta_key)?;
            let (payload, _) = decode_envelope_with_common(&row.cf, table_id, &row.value_envelope)?;
            let payload = replace_core_meta_row_common(&payload, &common)?;
            row.value_envelope =
                encode_envelope_with_common(&row.cf, table_id, &payload, common.clone())?;
        }
        row.root_key_hash = common.root_key_hash.clone();
        row.root_generation = common.root_generation;
        row.visibility_state = common.visibility_state_enum();
        Ok(())
    }

    pub(crate) fn rebind_encoded_row_publication(
        &self,
        row: &mut CoreMetaEncodedOwnedRow,
        root_generation: u64,
        transaction_id: &str,
    ) -> Result<()> {
        if root_generation == 0 {
            bail!("CoreMeta rooted row publication generation must be nonzero");
        }
        if row.delete_marker {
            row.root_generation = root_generation;
            return Ok(());
        }
        let table_id = decode_core_meta_table_id(&row.core_meta_key)?;
        let (payload, mut common) =
            decode_envelope_with_common(&row.cf, table_id, &row.value_envelope)?;
        common.root_generation = root_generation;
        common.transaction_id = transaction_id.to_string();
        let payload = replace_core_meta_row_common(&payload, &common)?;
        row.value_envelope =
            encode_envelope_with_common(&row.cf, table_id, &payload, common.clone())?;
        row.root_generation = root_generation;
        row.visibility_state = common.visibility_state_enum();
        Ok(())
    }

    pub(super) fn write_encoded_rows(&self, rows: &[CoreMetaEncodedRow<'_>]) -> Result<()> {
        let mut batch = WriteBatch::default();
        let mut bytes = 0_u64;
        for row in rows {
            let cf = self.cf(row.cf)?;
            let table_id = decode_core_meta_table_id(row.core_meta_key)?;
            if row.delete_marker {
                if !row.value_envelope.is_empty() {
                    bail!("CoreMeta delete row must not carry a value envelope");
                }
                validate_meta_payload(row.cf, table_id, 0)?;
                bytes = bytes.saturating_add(row.core_meta_key.len() as u64);
                batch.delete_cf(&cf, row.core_meta_key);
                continue;
            }
            let (payload, common) =
                decode_envelope_with_common(row.cf, table_id, row.value_envelope)?;
            let canonical_envelope =
                encode_envelope_with_common(row.cf, table_id, &payload, common)?;
            if canonical_envelope != row.value_envelope {
                bail!("CoreMeta encoded row envelope is not canonical");
            }
            bytes =
                bytes.saturating_add((row.core_meta_key.len() + row.value_envelope.len()) as u64);
            batch.put_cf(&cf, row.core_meta_key, row.value_envelope);
        }
        let started_at = Instant::now();
        self.db.write_opt(batch, &durable_write_options())?;
        crate::perf::record_coremeta_duration(
            "write_encoded_rows",
            "multi",
            0,
            rows.len() as u64,
            bytes,
            started_at.elapsed(),
        );
        Ok(())
    }

    pub(crate) fn decode_encoded_owned_row(
        row: &CoreMetaEncodedOwnedRow,
    ) -> Result<(u16, Vec<u8>, Vec<u8>)> {
        if row.delete_marker {
            bail!("cannot decode a deleted CoreMeta row as snapshot state");
        }
        let table_id = decode_core_meta_table_id(&row.core_meta_key)?;
        let tuple_key = decode_core_meta_tuple_key(&row.core_meta_key)?.to_vec();
        let (payload, common) =
            decode_envelope_with_common(&row.cf, table_id, &row.value_envelope)?;
        let canonical = encode_envelope_with_common(&row.cf, table_id, &payload, common)?;
        if canonical != row.value_envelope {
            bail!("CoreMeta encoded snapshot row envelope is not canonical");
        }
        Ok((table_id, tuple_key, payload))
    }

    pub(crate) fn validate_and_own_encoded_rows(
        &self,
        rows: &[CoreMetaEncodedRow<'_>],
        delete_common: Option<&CoreMetaRowCommonProto>,
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        rows.iter()
            .map(|row| {
                let table_id = decode_core_meta_table_id(row.core_meta_key)?;
                validate_meta_payload(row.cf, table_id, 0)?;
                if row.delete_marker {
                    if !row.value_envelope.is_empty() {
                        bail!("CoreMeta delete row must not carry a value envelope");
                    }
                    let common = delete_common.ok_or_else(|| {
                        anyhow!("CoreMeta delete row requires explicit common metadata")
                    })?;
                    validate_coremeta_common_shape(common)?;
                    return Ok(CoreMetaEncodedOwnedRow {
                        cf: row.cf.to_string(),
                        core_meta_key: row.core_meta_key.to_vec(),
                        value_envelope: Vec::new(),
                        delete_marker: true,
                        root_key_hash: common.root_key_hash.clone(),
                        root_generation: common.root_generation,
                        visibility_state: common.visibility_state_enum(),
                    });
                }
                let (payload, common) =
                    decode_envelope_with_common(row.cf, table_id, row.value_envelope)?;
                let canonical =
                    encode_envelope_with_common(row.cf, table_id, &payload, common.clone())?;
                if canonical != row.value_envelope {
                    bail!("CoreMeta encoded row is not canonical");
                }
                Ok(CoreMetaEncodedOwnedRow {
                    cf: row.cf.to_string(),
                    core_meta_key: row.core_meta_key.to_vec(),
                    value_envelope: row.value_envelope.to_vec(),
                    delete_marker: false,
                    root_key_hash: common.root_key_hash.clone(),
                    root_generation: common.root_generation,
                    visibility_state: common.visibility_state_enum(),
                })
            })
            .collect()
    }

    pub fn scan_encoded_rows_page(
        &self,
        after: Option<&CoreMetaEncodedRowsCursor>,
        limit: usize,
    ) -> Result<CoreMetaEncodedRowsPage> {
        validate_scan_limit(limit)?;
        let start_cf_index = match after {
            Some(cursor) => {
                if cursor.core_meta_key.is_empty() {
                    bail!("CoreMeta encoded-row cursor key must not be empty");
                }
                let table_id = decode_core_meta_table_id(&cursor.core_meta_key)?;
                if table_spec(table_id)?.cf != cursor.cf.as_str() {
                    bail!("CoreMeta encoded-row cursor key is outside its column family");
                }
                column_families()
                    .iter()
                    .position(|cf| *cf == cursor.cf.as_str())
                    .context("CoreMeta encoded-row cursor column family is unknown")?
            }
            None => 0,
        };
        let mut rows = Vec::with_capacity(limit + 1);
        let started_at = Instant::now();
        let mut scanned = 0_u64;
        let mut bytes = 0_u64;

        'column_families: for (cf_index, cf_name) in
            column_families().iter().enumerate().skip(start_cf_index)
        {
            let cf = self.cf(cf_name)?;
            let after_key = after
                .filter(|_| cf_index == start_cf_index)
                .map(|cursor| cursor.core_meta_key.as_slice());
            let start_key = after_key.unwrap_or_default();
            let mut read_options = ReadOptions::default();
            read_options.set_iterate_lower_bound(start_key.to_vec());
            let iter = self.db.iterator_cf_opt(
                &cf,
                read_options,
                IteratorMode::From(start_key, Direction::Forward),
            );
            for item in iter {
                let (key, value) = item?;
                if after_key.is_some_and(|after_key| key.as_ref() <= after_key) {
                    continue;
                }
                scanned = scanned.saturating_add(1);
                bytes = bytes.saturating_add((key.len() + value.len()) as u64);
                let table_id = decode_core_meta_table_id(&key)?;
                let (payload, common) = decode_envelope_with_common(cf_name, table_id, &value)?;
                let canonical_envelope =
                    encode_envelope_with_common(cf_name, table_id, &payload, common.clone())?;
                rows.push(CoreMetaEncodedOwnedRow {
                    cf: (*cf_name).to_string(),
                    core_meta_key: key.to_vec(),
                    value_envelope: canonical_envelope,
                    delete_marker: false,
                    root_key_hash: common.root_key_hash.clone(),
                    root_generation: common.root_generation,
                    visibility_state: common.visibility_state_enum(),
                });
                if rows.len() > limit {
                    break 'column_families;
                }
            }
        }

        crate::perf::record_coremeta_duration(
            "scan_encoded_rows_page",
            "multi",
            0,
            scanned,
            bytes,
            started_at.elapsed(),
        );
        let has_more = rows.len() > limit;
        if has_more {
            rows.truncate(limit);
        }
        let next_cursor = has_more.then(|| {
            let row = rows
                .last()
                .expect("a full CoreMeta encoded-row page must have a final row");
            CoreMetaEncodedRowsCursor {
                cf: row.cf.clone(),
                core_meta_key: row.core_meta_key.clone(),
            }
        });
        Ok(CoreMetaEncodedRowsPage { rows, next_cursor })
    }

    fn cf(&self, name: &str) -> Result<&rocksdb::ColumnFamily> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| anyhow!("missing CoreStore RocksDB column family {name}"))
    }
}

impl CoreMetaReadSnapshot<'_> {
    pub(crate) fn get(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        validate_meta_payload(cf, table_id, 0)?;
        let key = core_meta_key(table_id, 0, tuple_key)?;
        let key_bytes = key.len() as u64;
        let column_family = self.store.cf(cf)?;
        let started_at = Instant::now();
        let value = self.snapshot.get_cf(&column_family, key)?;
        #[cfg(feature = "coremeta-perf-gate")]
        record_coremeta_get_probe(cf, table_id);
        let bytes = key_bytes + value.as_ref().map(|value| value.len() as u64).unwrap_or(0);
        crate::perf::record_coremeta_duration(
            "snapshot_get",
            cf,
            table_id,
            u64::from(value.is_some()),
            bytes,
            started_at.elapsed(),
        );
        value
            .map(|value| decode_envelope(cf, table_id, &value))
            .transpose()
    }
}

impl CoreMetaReader for CoreMetaStore {
    fn get(&self, cf: &'static str, table_id: u16, tuple_key: &[u8]) -> Result<Option<Vec<u8>>> {
        CoreMetaStore::get(self, cf, table_id, tuple_key)
    }
}

impl CoreMetaReader for CoreMetaReadSnapshot<'_> {
    fn get(&self, cf: &'static str, table_id: u16, tuple_key: &[u8]) -> Result<Option<Vec<u8>>> {
        CoreMetaReadSnapshot::get(self, cf, table_id, tuple_key)
    }
}

pub fn encode_core_meta_inline_payload_row(
    raw_payload: &[u8],
    common: CoreMetaRowCommonProto,
) -> Result<Vec<u8>> {
    if raw_payload.len() > CORE_META_MAX_INLINE_PAYLOAD_BYTES {
        bail!(
            "CoreStore inline payload is {} bytes before RocksDB compression, exceeding {} bytes",
            raw_payload.len(),
            CORE_META_MAX_INLINE_PAYLOAD_BYTES
        );
    }
    let row = InlinePayloadRowProto {
        common: Some(common),
        payload_hash: core_meta_inline_payload_hash(raw_payload),
        raw_payload_length: raw_payload.len() as u64,
        payload_bytes: raw_payload.to_vec(),
    };
    let mut payload = Vec::new();
    row.encode(&mut payload)?;
    Ok(payload)
}

pub fn core_meta_record_tuple_key(encoded_key: &[u8]) -> Result<&[u8]> {
    decode_core_meta_tuple_key(encoded_key)
}

pub(crate) fn core_meta_record_table_id(encoded_key: &[u8]) -> Result<u16> {
    decode_core_meta_table_id(encoded_key)
}

pub fn core_meta_committed_row_common(
    realm_id: impl Into<String>,
    root_key_hash: impl Into<String>,
    root_generation: u64,
    transaction_id: impl Into<String>,
    created_at_unix_nanos: u64,
) -> CoreMetaRowCommonProto {
    core_meta_row_common_with_visibility(
        realm_id,
        root_key_hash,
        root_generation,
        transaction_id,
        created_at_unix_nanos,
        CoreMetaVisibilityState::Committed,
    )
}

pub fn core_meta_pending_row_common(
    realm_id: impl Into<String>,
    root_key_hash: impl Into<String>,
    root_generation: u64,
    transaction_id: impl Into<String>,
    created_at_unix_nanos: u64,
) -> CoreMetaRowCommonProto {
    core_meta_row_common_with_visibility(
        realm_id,
        root_key_hash,
        root_generation,
        transaction_id,
        created_at_unix_nanos,
        CoreMetaVisibilityState::Pending,
    )
}

pub fn core_meta_row_common_with_visibility(
    realm_id: impl Into<String>,
    root_key_hash: impl Into<String>,
    root_generation: u64,
    transaction_id: impl Into<String>,
    created_at_unix_nanos: u64,
    visibility_state: CoreMetaVisibilityState,
) -> CoreMetaRowCommonProto {
    CoreMetaRowCommonProto {
        realm_id: realm_id.into(),
        root_key_hash: root_key_hash.into(),
        root_generation,
        transaction_id: transaction_id.into(),
        visibility_state: visibility_state as i32,
        created_at_unix_nanos,
        payload_schema_version: CORE_META_VALUE_SCHEMA_VERSION,
    }
}

pub fn core_meta_root_key_hash(root_anchor_key: &str) -> String {
    let mut bytes = Vec::new();
    for part in ["anvil.root.key.v1", root_anchor_key] {
        bytes.extend_from_slice(&(part.len() as u64).to_le_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    format!("sha256:{}", sha256_hex(&bytes))
}

pub fn core_meta_locator_from_manifest_locator(
    locator: &CoreManifestLocator,
) -> Result<CoreMetaLocatorProto> {
    let encoded_locator = encode_manifest_locator_proto(locator)?;
    Ok(CoreMetaLocatorProto {
        storage_kind: "corestore-blocks".to_string(),
        manifest_hash: locator.manifest_hash.clone(),
        root_key_hash: core_meta_root_key_hash(&locator.manifest_ref.logical_file_id),
        root_generation: locator.manifest_ref.writer_generation,
        locator_hash: format!("sha256:{}", sha256_hex(&encoded_locator)),
        encoded_locator,
    })
}

pub fn core_meta_locator_to_manifest_locator(
    locator: &CoreMetaLocatorProto,
) -> Result<CoreManifestLocator> {
    if locator.storage_kind != "corestore-blocks" {
        bail!(
            "CoreMeta locator storage kind {} is not corestore-blocks",
            locator.storage_kind
        );
    }
    let expected_hash = format!("sha256:{}", sha256_hex(&locator.encoded_locator));
    if locator.locator_hash != expected_hash {
        bail!("CoreMeta locator hash mismatch");
    }
    let decoded = decode_manifest_locator_proto(&locator.encoded_locator)?;
    if decoded.manifest_hash != locator.manifest_hash {
        bail!("CoreMeta locator manifest hash mismatch");
    }
    if locator.root_key_hash != core_meta_root_key_hash(&decoded.manifest_ref.logical_file_id) {
        bail!("CoreMeta locator root key hash mismatch");
    }
    if locator.root_generation != decoded.manifest_ref.writer_generation {
        bail!("CoreMeta locator root generation mismatch");
    }
    Ok(decoded)
}

pub struct CoreMetaBatchOp<'a> {
    pub cf: &'static str,
    pub table_id: u16,
    pub tuple_key: &'a [u8],
    pub common: Option<CoreMetaRowCommonProto>,
    pub kind: CoreMetaBatchOpKind<'a>,
}

pub enum CoreMetaBatchOpKind<'a> {
    Put(&'a [u8]),
    Delete,
}

fn column_families() -> &'static [&'static str] {
    &[
        CF_META_VERSION,
        CF_ROOT_CACHE,
        CF_TRANSACTIONS,
        CF_OBJECT_HEADS,
        CF_OBJECT_VERSIONS,
        CF_INLINE_PAYLOADS,
        CF_STREAM_HEADS,
        CF_STREAM_RECORDS,
        CF_INDEX_DEFS,
        CF_INDEX_ROWS,
        CF_BOUNDARY,
        CF_AUTHZ,
        CF_PERSONALDB,
        CF_REGISTRY,
        CF_MESH,
        CF_LEASES_FENCES,
        CF_MATERIALISATION,
        CF_REFCOUNTS,
        CF_OBSERVABILITY,
    ]
}

pub fn core_meta_column_families() -> &'static [&'static str] {
    column_families()
}

fn cf_options(name: &str) -> Options {
    let mut options = Options::default();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(11));
    options.optimize_for_point_lookup(64);
    if matches!(
        name,
        CF_STREAM_RECORDS | CF_INDEX_ROWS | CF_OBJECT_VERSIONS | CF_INLINE_PAYLOADS | CF_AUTHZ
    ) {
        options.set_compression_type(rocksdb::DBCompressionType::Zstd);
    }
    options
}

fn durable_write_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}

fn local_committed_row_common() -> CoreMetaRowCommonProto {
    CoreMetaRowCommonProto {
        realm_id: String::new(),
        root_key_hash: String::new(),
        root_generation: 0,
        transaction_id: String::new(),
        visibility_state: CoreMetaVisibilityState::Committed as i32,
        created_at_unix_nanos: 0,
        payload_schema_version: CORE_META_VALUE_SCHEMA_VERSION,
    }
}

pub(super) fn core_meta_bootstrap_row_common(
    realm_id: impl Into<String>,
    created_at_unix_nanos: u64,
) -> CoreMetaRowCommonProto {
    CoreMetaRowCommonProto {
        realm_id: realm_id.into(),
        root_key_hash: String::new(),
        root_generation: 0,
        transaction_id: String::new(),
        visibility_state: CoreMetaVisibilityState::Committed as i32,
        created_at_unix_nanos,
        payload_schema_version: CORE_META_VALUE_SCHEMA_VERSION,
    }
}

fn decode_inline_payload_row(payload: &[u8]) -> Result<Vec<u8>> {
    let row = InlinePayloadRowProto::decode(payload)?;
    let mut canonical = Vec::new();
    row.encode(&mut canonical)?;
    if canonical != payload {
        bail!("CoreStore inline payload row is not deterministic protobuf");
    }
    if row.raw_payload_length != row.payload_bytes.len() as u64 {
        bail!("CoreStore inline payload row length mismatch");
    }
    if row.payload_bytes.len() > CORE_META_MAX_INLINE_PAYLOAD_BYTES {
        bail!("CoreStore inline payload row exceeds bounded raw payload size");
    }
    if row.payload_hash != core_meta_inline_payload_hash(&row.payload_bytes) {
        bail!("CoreStore inline payload row hash mismatch");
    }
    Ok(row.payload_bytes)
}

fn extract_row_common_from_payload(payload: &[u8]) -> Result<CoreMetaRowCommonProto> {
    CoreMetaCommonPrefixProto::decode(payload)
        .context("decode CoreMeta row common prefix")?
        .common
        .ok_or_else(|| anyhow!("CoreMeta row payload is missing common field"))
}

pub(crate) fn core_meta_row_common_from_payload(payload: &[u8]) -> Result<CoreMetaRowCommonProto> {
    extract_row_common_from_payload(payload)
}

/// Replace the canonical field-one common envelope without decoding the
/// feature-owned remainder of the row. Every CoreMeta row schema reserves
/// field one for `CoreMetaRowCommonProto`, while the rest of the payload is
/// owned by its writer family.
pub(crate) fn replace_core_meta_row_common(
    payload: &[u8],
    common: &CoreMetaRowCommonProto,
) -> Result<Vec<u8>> {
    let mut offset = 0_usize;
    let key = decode_proto_varint(payload, &mut offset)?;
    if key != 0x0a {
        bail!("CoreMeta row payload common field must be canonical field one");
    }
    let common_len = usize::try_from(decode_proto_varint(payload, &mut offset)?)
        .context("CoreMeta row common field length exceeds addressable memory")?;
    let common_end = offset
        .checked_add(common_len)
        .filter(|end| *end <= payload.len())
        .ok_or_else(|| anyhow!("CoreMeta row payload common field is truncated"))?;

    let mut encoded_common = Vec::new();
    common.encode(&mut encoded_common)?;
    let mut rebound = Vec::with_capacity(
        1 + encoded_varint_len(encoded_common.len() as u64)
            + encoded_common.len()
            + payload.len().saturating_sub(common_end),
    );
    rebound.push(0x0a);
    encode_proto_varint(encoded_common.len() as u64, &mut rebound);
    rebound.extend_from_slice(&encoded_common);
    rebound.extend_from_slice(&payload[common_end..]);

    if extract_row_common_from_payload(&rebound)? != *common {
        bail!("CoreMeta row common replacement did not round-trip");
    }
    Ok(rebound)
}

fn decode_proto_varint(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let mut value = 0_u64;
    for shift in (0..=63).step_by(7) {
        let byte = *bytes
            .get(*offset)
            .ok_or_else(|| anyhow!("CoreMeta protobuf varint is truncated"))?;
        *offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("CoreMeta protobuf varint exceeds u64")
}

fn encode_proto_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn encoded_varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn core_meta_inline_payload_hash(payload: &[u8]) -> String {
    let mut hasher = Hasher::new();
    hasher.update(b"anvil.coremeta.inline_payload.v1");
    hasher.update(&[0]);
    hasher.update(payload);
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn core_meta_schema_key() -> Vec<u8> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("schema")])
        .expect("static CoreMeta schema tuple key must be valid")
}

fn column_family_set_hash() -> String {
    let mut hasher = Hasher::new();
    hasher.update(b"anvil.coremeta.column_families.v1");
    for name in column_families() {
        hasher.update(&[0]);
        hasher.update(name.as_bytes());
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

#[derive(Debug, Clone, Copy)]
struct CoreMetaTableSpec {
    cf: &'static str,
    max_payload_bytes: usize,
}

fn table_spec(table_id: u16) -> Result<CoreMetaTableSpec> {
    let spec = match table_id {
        TABLE_META_SCHEMA_VERSION_ROW => CoreMetaTableSpec {
            cf: CF_META_VERSION,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_ROOT_CACHE_ROW => CoreMetaTableSpec {
            cf: CF_ROOT_CACHE,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_TRANSACTION_LOCATOR_ROW
        | TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW
        | TABLE_INLINE_MANIFEST_BODY_ROW
        | TABLE_EXPLICIT_TRANSACTION_ROW
        | TABLE_PENDING_MUTATION_ROW
        | TABLE_NATIVE_IDEMPOTENCY_ROW
        | TABLE_LOCAL_ADMISSION_EVIDENCE_ROW
        | TABLE_TRANSACTION_MANIFEST_BODY_ROW
        | TABLE_ROOT_PUBLICATION_INTENT_ROW
        | super::local::local_coremeta_history::TABLE_COREMETA_GENERATION_DESCRIPTOR_ROW
        | super::local::local_coremeta_history::TABLE_COREMETA_GENERATION_MUTATION_ROW
        | super::local::local_coremeta_history::TABLE_COREMETA_GENERATION_ENVELOPE_CHUNK_ROW
        | super::local::local_coremeta_history::TABLE_COREMETA_GENERATION_INSTALL_ROW => {
            CoreMetaTableSpec {
                cf: CF_TRANSACTIONS,
                max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
            }
        }
        TABLE_OBJECT_HEAD_ROW
        | TABLE_MANIFEST_CAS_CURRENT_ROW
        | TABLE_MULTIPART_UPLOAD_CURRENT_ROW
        | TABLE_MULTIPART_PART_CURRENT_ROW
        | TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW => CoreMetaTableSpec {
            cf: CF_OBJECT_HEADS,
            max_payload_bytes: CORE_META_OBJECT_VERSION_MAX_PAYLOAD_BYTES,
        },
        TABLE_OBJECT_VERSION_META_ROW
        | TABLE_OBJECT_WATCH_CURSOR_ROW
        | TABLE_OBJECT_SHARD_REPAIR_ROW => CoreMetaTableSpec {
            cf: CF_OBJECT_VERSIONS,
            max_payload_bytes: CORE_META_OBJECT_VERSION_MAX_PAYLOAD_BYTES,
        },
        TABLE_INLINE_PAYLOAD_ROW => CoreMetaTableSpec {
            cf: CF_INLINE_PAYLOADS,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_STREAM_HEAD_ROW => CoreMetaTableSpec {
            cf: CF_STREAM_HEADS,
            max_payload_bytes: CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES,
        },
        TABLE_STREAM_RECORD_INDEX_ROW | TABLE_STREAM_IDEMPOTENCY_ROW => CoreMetaTableSpec {
            cf: CF_STREAM_RECORDS,
            max_payload_bytes: CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES,
        },
        TABLE_INDEX_DEFINITION_ROW => CoreMetaTableSpec {
            cf: CF_INDEX_DEFS,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_INDEX_ROW | TABLE_DERIVED_INDEX_PROOF_ROW => CoreMetaTableSpec {
            cf: CF_INDEX_ROWS,
            max_payload_bytes: CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES,
        },
        TABLE_BOUNDARY_SCHEMA_ROW
        | TABLE_BOUNDARY_VALUE_ROW
        | TABLE_BOUNDARY_MIGRATION_ROW
        | TABLE_BOUNDARY_SCHEMA_CURRENT_ROW => CoreMetaTableSpec {
            cf: CF_BOUNDARY,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_AUTHZ_SCHEMA_ROW
        | TABLE_AUTHZ_TUPLE_PAGE_ROW
        | TABLE_AUTHZ_IDEMPOTENCY_RECEIPT_ROW
        | TABLE_AUTHZ_HEAD_ROW
        | TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW
        | TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW => CoreMetaTableSpec {
            cf: CF_AUTHZ,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_PERSONALDB_GROUP_ROW
        | TABLE_PERSONALDB_DATA_LOCATOR_ROW
        | TABLE_PERSONALDB_PROPOSAL_CLAIM_ROW
        | TABLE_PERSONALDB_PROPOSAL_SLOT_ROW
        | TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW
        | TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW
        | TABLE_PERSONALDB_WITNESS_RECEIPT_ROW => CoreMetaTableSpec {
            cf: CF_PERSONALDB,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_REGISTRY_VERSION_ROW
        | TABLE_REGISTRY_BLOB_LOCATOR_ROW
        | TABLE_GATEWAY_METADATA_ROW
        | TABLE_GATEWAY_MOUNT_ROUTE_ROW
        | TABLE_GIT_SOURCE_MANIFEST_ROW => CoreMetaTableSpec {
            cf: CF_REGISTRY,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_MESH_NODE_ROW
        | TABLE_MESH_PARTITION_ROW
        | TABLE_REPAIR_FINDING_ROW
        | TABLE_REPAIR_FINDING_HEAD_ROW
        | TABLE_REPAIR_FINDING_ID_ROW
        | TABLE_BUCKET_CURRENT_BY_NAME_ROW
        | TABLE_BUCKET_CURRENT_BY_ID_ROW
        | TABLE_BUCKET_ID_ALLOCATOR_ROW
        | TABLE_BUCKET_EVENT_HEAD_ROW
        | TABLE_CONTROL_CURRENT_ROW
        | TABLE_SYSTEM_BOOTSTRAP_MARKER_ROW
        | TABLE_ROOT_CATALOG_CURRENT_ROW
        | TABLE_QUORUM_PROFILE_CURRENT_ROW
        | TABLE_NODE_SIGNING_KEYPAIR_ROW
        | TABLE_LOCAL_NODE_IDENTITY_ROW
        | TABLE_PERSONALDB_SIGNING_KEY_ROW
        | TABLE_PERSONALDB_SIGNING_KEY_HEAD_ROW => CoreMetaTableSpec {
            cf: CF_MESH,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_OWNERSHIP_FENCE_ROW
        | TABLE_PARTITION_OWNER_ROW
        | TABLE_TASK_LEASE_ROW
        | TABLE_TASK_CURRENT_ROW
        | TABLE_CORE_FENCE_ROW
        | TABLE_ROOT_FAILOVER_VOTE_ROW
        | TABLE_ROOT_FAILOVER_CERTIFICATE_ROW => CoreMetaTableSpec {
            cf: CF_LEASES_FENCES,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_MATERIALISATION_CURSOR_ROW
        | TABLE_WRITER_SEGMENT_ROW
        | TABLE_WATCH_CHECKPOINT_ROW
        | TABLE_WRITER_HEAD_ROW
        | TABLE_LANDED_BYTE_REF_ROW => CoreMetaTableSpec {
            cf: CF_MATERIALISATION,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_REFCOUNT_ROW => CoreMetaTableSpec {
            cf: CF_REFCOUNTS,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_OBSERVABILITY_CURSOR_ROW | TABLE_DIAGNOSTIC_ROW => CoreMetaTableSpec {
            cf: CF_OBSERVABILITY,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        _ => bail!("unknown CoreStore metadata table id {table_id:#06x}"),
    };
    Ok(spec)
}

fn validate_meta_payload(cf: &str, table_id: u16, payload_len: usize) -> Result<()> {
    let spec = table_spec(table_id)?;
    if spec.cf != cf {
        bail!(
            "CoreStore metadata table {table_id:#06x} belongs to {}, not {cf}",
            spec.cf
        );
    }
    if payload_len > spec.max_payload_bytes {
        bail!(
            "CoreStore metadata table {table_id:#06x} payload is {payload_len} bytes, exceeding {} bytes",
            spec.max_payload_bytes
        );
    }
    Ok(())
}

fn validate_scan_limit(limit: usize) -> Result<()> {
    if !(1..=CORE_META_MAX_SCAN_PAGE_ROWS).contains(&limit) {
        bail!("CoreMeta scan limit must be between 1 and {CORE_META_MAX_SCAN_PAGE_ROWS}");
    }
    Ok(())
}

pub fn validate_coremeta_operation_key(cf: &str, table_id: u16, tuple_key: &[u8]) -> Result<()> {
    validate_meta_payload(cf, table_id, 0)?;
    let _ = core_meta_key(table_id, 0, tuple_key)?;
    Ok(())
}

pub fn canonical_coremeta_cf_name(cf: &str) -> Result<&'static str> {
    match cf {
        CF_META_VERSION => Ok(CF_META_VERSION),
        CF_ROOT_CACHE => Ok(CF_ROOT_CACHE),
        CF_TRANSACTIONS => Ok(CF_TRANSACTIONS),
        CF_OBJECT_HEADS => Ok(CF_OBJECT_HEADS),
        CF_OBJECT_VERSIONS => Ok(CF_OBJECT_VERSIONS),
        CF_INLINE_PAYLOADS => Ok(CF_INLINE_PAYLOADS),
        CF_STREAM_HEADS => Ok(CF_STREAM_HEADS),
        CF_STREAM_RECORDS => Ok(CF_STREAM_RECORDS),
        CF_INDEX_DEFS => Ok(CF_INDEX_DEFS),
        CF_INDEX_ROWS => Ok(CF_INDEX_ROWS),
        CF_BOUNDARY => Ok(CF_BOUNDARY),
        CF_AUTHZ => Ok(CF_AUTHZ),
        CF_PERSONALDB => Ok(CF_PERSONALDB),
        CF_REGISTRY => Ok(CF_REGISTRY),
        CF_MESH => Ok(CF_MESH),
        CF_LEASES_FENCES => Ok(CF_LEASES_FENCES),
        CF_MATERIALISATION => Ok(CF_MATERIALISATION),
        CF_REFCOUNTS => Ok(CF_REFCOUNTS),
        CF_OBSERVABILITY => Ok(CF_OBSERVABILITY),
        _ => bail!("unknown CoreStore metadata column family {cf}"),
    }
}

pub fn validate_coremeta_operation_payload(
    cf: &str,
    table_id: u16,
    tuple_key: &[u8],
    payload: &[u8],
) -> Result<()> {
    validate_meta_payload(cf, table_id, payload.len())?;
    let _ = core_meta_key(table_id, 0, tuple_key)?;
    validate_table_payload_schema(table_id, payload)?;
    Ok(())
}

fn encode_envelope(cf: &str, table_id: u16, payload: &[u8]) -> Result<Vec<u8>> {
    validate_meta_payload(cf, table_id, payload.len())?;
    let common = common_for_envelope_payload(payload)?;
    encode_envelope_with_common(cf, table_id, payload, common)
}

fn encode_envelope_with_common(
    cf: &str,
    table_id: u16,
    payload: &[u8],
    common: CoreMetaRowCommonProto,
) -> Result<Vec<u8>> {
    validate_meta_payload(cf, table_id, payload.len())?;
    validate_table_payload_schema(table_id, payload)?;
    validate_envelope_common_matches_payload(payload, &common)?;
    let payload_hash = core_meta_payload_hash(table_id, CORE_META_VALUE_SCHEMA_VERSION, payload);
    let envelope = CoreMetaValueEnvelope {
        table_id: u32::from(table_id),
        schema_version: CORE_META_VALUE_SCHEMA_VERSION,
        payload: payload.to_vec(),
        payload_hash,
    };
    let mut bytes = Vec::new();
    envelope.encode(&mut bytes)?;
    if bytes.len() > CORE_META_MAX_VALUE_BYTES {
        bail!(
            "CoreStore metadata table {table_id:#06x} value envelope is {} bytes, exceeding {} bytes",
            bytes.len(),
            CORE_META_MAX_VALUE_BYTES
        );
    }
    Ok(bytes)
}

fn decode_envelope(cf: &str, expected_table_id: u16, bytes: &[u8]) -> Result<Vec<u8>> {
    decode_envelope_with_common(cf, expected_table_id, bytes).map(|(payload, _)| payload)
}

fn decode_envelope_with_common(
    cf: &str,
    expected_table_id: u16,
    bytes: &[u8],
) -> Result<(Vec<u8>, CoreMetaRowCommonProto)> {
    if bytes.len() > CORE_META_MAX_VALUE_BYTES {
        bail!(
            "CoreMetaValueEnvelope is {} bytes, exceeding {} bytes",
            bytes.len(),
            CORE_META_MAX_VALUE_BYTES
        );
    }
    let envelope = CoreMetaValueEnvelope::decode(bytes)?;
    if envelope.table_id != u32::from(expected_table_id) {
        bail!(
            "CoreMetaValueEnvelope table id mismatch: expected {expected_table_id:#06x}, got {:#06x}",
            envelope.table_id
        );
    }
    if envelope.schema_version != CORE_META_VALUE_SCHEMA_VERSION {
        bail!(
            "CoreMetaValueEnvelope unsupported schema version {}",
            envelope.schema_version
        );
    }
    let actual = core_meta_payload_hash(
        expected_table_id,
        envelope.schema_version,
        &envelope.payload,
    );
    if actual != envelope.payload_hash {
        bail!("CoreMetaValueEnvelope payload hash mismatch");
    }
    let common = common_for_envelope_payload(&envelope.payload)?;
    validate_coremeta_common_shape(&common)?;
    validate_meta_payload(cf, expected_table_id, envelope.payload.len())?;
    validate_table_payload_schema(expected_table_id, &envelope.payload)?;
    Ok((envelope.payload, common))
}

fn common_for_envelope_payload(payload: &[u8]) -> Result<CoreMetaRowCommonProto> {
    extract_row_common_from_payload(payload)
}

fn validate_envelope_common_matches_payload(
    payload: &[u8],
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    let payload_common = extract_row_common_from_payload(payload)?;
    if payload_common != *common {
        bail!("CoreMetaValueEnvelope common metadata does not match payload common metadata");
    }
    Ok(())
}

fn validate_table_payload_schema(table_id: u16, payload: &[u8]) -> Result<()> {
    match table_id {
        TABLE_META_SCHEMA_VERSION_ROW => {
            let row = CoreMetaSchemaVersionRowProto::decode(payload)?;
            let mut canonical = Vec::new();
            row.encode(&mut canonical)?;
            if canonical != payload {
                bail!("CoreMeta schema version row is not deterministic protobuf");
            }
        }
        TABLE_INLINE_PAYLOAD_ROW => {
            let _ = decode_inline_payload_row(payload)?;
        }
        TABLE_TRANSACTION_LOCATOR_ROW => {
            validate_coremeta_pending_batch_marker_row(payload)?;
        }
        TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW => {
            validate_coremeta_commit_evidence_row(payload)?;
        }
        TABLE_LOCAL_ADMISSION_EVIDENCE_ROW => {
            super::pending_mutation::decode_local_admission_evidence(payload)?;
        }
        TABLE_TRANSACTION_MANIFEST_BODY_ROW => {
            super::local::validate_transaction_manifest_body_row(payload)?;
        }
        TABLE_ROOT_PUBLICATION_INTENT_ROW => {
            super::local::validate_root_publication_intent_row(payload)?;
        }
        super::local::local_coremeta_history::TABLE_COREMETA_GENERATION_DESCRIPTOR_ROW
        | super::local::local_coremeta_history::TABLE_COREMETA_GENERATION_MUTATION_ROW
        | super::local::local_coremeta_history::TABLE_COREMETA_GENERATION_ENVELOPE_CHUNK_ROW
        | super::local::local_coremeta_history::TABLE_COREMETA_GENERATION_INSTALL_ROW => {
            super::local::local_coremeta_history::validate_coremeta_history_row(table_id, payload)?;
        }
        _ => {}
    }
    validate_table_schema_marker(table_id, payload)?;
    Ok(())
}

fn validate_table_schema_marker(table_id: u16, payload: &[u8]) -> Result<()> {
    let Some(allowed) = expected_schema_markers(table_id) else {
        return Ok(());
    };
    let Some(actual) = protobuf_field_two_string(payload)? else {
        bail!("CoreStore metadata table {table_id:#06x} payload is missing schema marker field");
    };
    if !allowed.contains(&actual.as_str()) {
        bail!(
            "CoreStore metadata table {table_id:#06x} payload schema {actual} is not one of {allowed:?}"
        );
    }
    Ok(())
}

fn expected_schema_markers(table_id: u16) -> Option<&'static [&'static str]> {
    match table_id {
        TABLE_INLINE_MANIFEST_BODY_ROW => Some(&["anvil.core.inline_manifest_body.v1"]),
        TABLE_TRANSACTION_MANIFEST_BODY_ROW => Some(&["anvil.core.transaction_manifest_body.v1"]),
        TABLE_ROOT_PUBLICATION_INTENT_ROW => Some(&[
            "anvil.core.root_publication_intent.v2",
            "anvil.core.root_publication_root.v1",
            "anvil.core.root_publication_row.v1",
            "anvil.core.root_publication_chunk.v1",
        ]),
        super::local::local_coremeta_history::TABLE_COREMETA_GENERATION_DESCRIPTOR_ROW => {
            Some(&["anvil.coremeta.generation_descriptor.v1"])
        }
        super::local::local_coremeta_history::TABLE_COREMETA_GENERATION_MUTATION_ROW => {
            Some(&["anvil.coremeta.generation_mutation.v1"])
        }
        super::local::local_coremeta_history::TABLE_COREMETA_GENERATION_ENVELOPE_CHUNK_ROW => {
            Some(&["anvil.coremeta.generation_envelope_chunk.v1"])
        }
        super::local::local_coremeta_history::TABLE_COREMETA_GENERATION_INSTALL_ROW => {
            Some(&["anvil.coremeta.generation_install.v1"])
        }
        TABLE_EXPLICIT_TRANSACTION_ROW => Some(&[
            "anvil.core.transaction_header_row.v1",
            "anvil.core.transaction_staged_update_row.v1",
            "anvil.core.transaction_precondition_row.v3",
        ]),
        TABLE_PENDING_MUTATION_ROW => Some(&[
            "anvil.core.pending_mutation_row.v1",
            "anvil.core.pending_mutation_finalisation_index.v1",
        ]),
        TABLE_LOCAL_ADMISSION_EVIDENCE_ROW => Some(&["anvil.admission.local_evidence.v1"]),
        TABLE_OBJECT_HEAD_ROW => Some(&["anvil.core.object_metadata.v1"]),
        TABLE_OBJECT_VERSION_META_ROW => Some(&[
            "anvil.core.object_manifest.v1",
            "anvil.core.object_metadata.v1",
            "anvil.core.object_metadata_counter.v1",
        ]),
        TABLE_OBJECT_WATCH_CURSOR_ROW => Some(&["anvil.core.object_watch_cursor.v1"]),
        TABLE_OBJECT_SHARD_REPAIR_ROW => Some(&["anvil.core.object_shard_repair.v1"]),
        TABLE_STREAM_HEAD_ROW => Some(&["anvil.core.stream_head.v1"]),
        TABLE_STREAM_RECORD_INDEX_ROW => Some(&[
            "anvil.core.watch_event.v1",
            "anvil.core.stream_record_index.v1",
        ]),
        TABLE_STREAM_IDEMPOTENCY_ROW => Some(&["anvil.core.stream_idempotency.v1"]),
        TABLE_MANIFEST_CAS_CURRENT_ROW => Some(&["anvil.core.manifest_cas.current_row.v1"]),
        TABLE_MULTIPART_UPLOAD_CURRENT_ROW => Some(&["anvil.multipart.upload_current_row.v1"]),
        TABLE_MULTIPART_PART_CURRENT_ROW => Some(&["anvil.multipart.part_current_row.v1"]),
        TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW => {
            Some(&["anvil.coremeta.object_metadata_partition_manifest.v1"])
        }
        TABLE_INDEX_DEFINITION_ROW => Some(&[
            "anvil.coremeta.index_definition_current.v1",
            "anvil.coremeta.index_definition_state.v1",
        ]),
        TABLE_INDEX_ROW => Some(&["anvil.coremeta.index_segment_row.v1"]),
        TABLE_DERIVED_INDEX_PROOF_ROW => Some(&["anvil.coremeta.derived_index_proof.v1"]),
        TABLE_BOUNDARY_SCHEMA_ROW | TABLE_BOUNDARY_SCHEMA_CURRENT_ROW => {
            Some(&["anvil.core.boundary_schema.v1"])
        }
        TABLE_BOUNDARY_VALUE_ROW => Some(&["anvil.core.boundary_value_row.v1"]),
        TABLE_BOUNDARY_MIGRATION_ROW => Some(&["anvil.boundary_migration.v1"]),
        TABLE_AUTHZ_SCHEMA_ROW | TABLE_AUTHZ_TUPLE_PAGE_ROW => Some(&[
            "anvil.authz.coremeta_payload_row.v1",
            "anvil.authz.derived_userset_index_row.v1",
        ]),
        TABLE_AUTHZ_IDEMPOTENCY_RECEIPT_ROW => Some(&["anvil.authz.idempotency_receipt.v2"]),
        TABLE_AUTHZ_HEAD_ROW => Some(&["anvil.authz.head.v1"]),
        TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW | TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW => {
            Some(&["anvil.authz.coremeta_payload_row.v1"])
        }
        TABLE_GATEWAY_METADATA_ROW => Some(&["anvil.gateway.coremeta_record.v1"]),
        TABLE_GATEWAY_MOUNT_ROUTE_ROW => Some(&["anvil.gateway.mount_route.v1"]),
        TABLE_GIT_SOURCE_MANIFEST_ROW => Some(&["anvil.coremeta.git_source_manifest.v1"]),
        TABLE_MESH_NODE_ROW => Some(&[
            "anvil.coremeta.mesh_lifecycle_projection.v1",
            "anvil.coremeta.mesh_directory_projection.v1",
        ]),
        TABLE_MESH_PARTITION_ROW => Some(&[
            "anvil.coremeta.mesh_lifecycle_projection.v1",
            "anvil.coremeta.mesh_directory_projection.v1",
            "anvil.mesh.control_checkpoint.v1",
        ]),
        TABLE_BUCKET_CURRENT_BY_NAME_ROW | TABLE_BUCKET_CURRENT_BY_ID_ROW => {
            Some(&["anvil.core.bucket_current.v1"])
        }
        TABLE_BUCKET_ID_ALLOCATOR_ROW => Some(&["anvil.core.bucket_id_allocator.v1"]),
        TABLE_BUCKET_EVENT_HEAD_ROW => Some(&["anvil.core.bucket_event_head.v1"]),
        TABLE_CONTROL_CURRENT_ROW => Some(&["anvil.control.current.v1"]),
        TABLE_ROOT_CATALOG_CURRENT_ROW => {
            Some(&["anvil.control.current.v1", "anvil.core.root_catalog.v1"])
        }
        TABLE_QUORUM_PROFILE_CURRENT_ROW => {
            Some(&["anvil.control.current.v1", "anvil.core.quorum_profile.v1"])
        }
        TABLE_REPAIR_FINDING_HEAD_ROW => Some(&["anvil.repair.finding_head.v1"]),
        TABLE_REPAIR_FINDING_ID_ROW => Some(&["anvil.repair.finding_id.v1"]),
        TABLE_LOCAL_NODE_IDENTITY_ROW => Some(&["anvil.coremeta.local_node_identity.v1"]),
        TABLE_TASK_CURRENT_ROW => Some(&["anvil.core.task_queue_row.v1"]),
        TABLE_CORE_FENCE_ROW => Some(&["anvil.control.current.v1", "anvil.core.fence.v1"]),
        TABLE_ROOT_FAILOVER_VOTE_ROW => Some(&["anvil.core.root_failover_vote.v1"]),
        TABLE_ROOT_FAILOVER_CERTIFICATE_ROW => Some(&["anvil.core.root_failover_certificate.v1"]),
        TABLE_MATERIALISATION_CURSOR_ROW => Some(&[
            "anvil.core.materialisation_cursor.v1",
            "anvil.core.pending_mutation_finalisation.v1",
            "anvil.core.pending_mutation_finalisation_index.v1",
            "anvil.coremeta.watch_checkpoint_lag.v1",
        ]),
        TABLE_LANDED_BYTE_REF_ROW => Some(&["anvil.core.landed_byte_ref.v1"]),
        TABLE_REFCOUNT_ROW => Some(&["anvil.core.payload_reference.v1"]),
        TABLE_WRITER_SEGMENT_ROW => Some(&["anvil.coremeta.writer_segment_locator.v1"]),
        TABLE_WRITER_HEAD_ROW => Some(&["anvil.coremeta.writer_head.v1"]),
        TABLE_WATCH_CHECKPOINT_ROW => Some(&[
            "anvil.coremeta.watch_checkpoint.v1",
            "anvil.coremeta.watch_checkpoint_lag.v1",
        ]),
        _ => None,
    }
}

fn protobuf_field_two_string(payload: &[u8]) -> Result<Option<String>> {
    let mut offset = 0usize;
    while offset < payload.len() {
        let (key, next) = read_proto_varint(payload, offset)?;
        offset = next;
        let field_number = key >> 3;
        let wire_type = key & 0x07;
        match wire_type {
            0 => {
                let (_, next) = read_proto_varint(payload, offset)?;
                offset = next;
            }
            1 => {
                offset = offset
                    .checked_add(8)
                    .ok_or_else(|| anyhow!("protobuf fixed64 field length overflow"))?;
            }
            2 => {
                let (len, next) = read_proto_varint(payload, offset)?;
                offset = next;
                let len = usize::try_from(len).context("protobuf length exceeds usize")?;
                let end = offset
                    .checked_add(len)
                    .ok_or_else(|| anyhow!("protobuf length-delimited field overflow"))?;
                if end > payload.len() {
                    bail!("protobuf length-delimited field is truncated");
                }
                if field_number == 2 {
                    return Ok(Some(
                        std::str::from_utf8(&payload[offset..end])
                            .context("protobuf field 2 schema marker is not UTF-8")?
                            .to_string(),
                    ));
                }
                offset = end;
            }
            5 => {
                offset = offset
                    .checked_add(4)
                    .ok_or_else(|| anyhow!("protobuf fixed32 field length overflow"))?;
            }
            other => bail!("protobuf schema marker scan hit unsupported wire type {other}"),
        }
        if offset > payload.len() {
            bail!("protobuf field exceeds payload length");
        }
    }
    Ok(None)
}

fn read_proto_varint(payload: &[u8], mut offset: usize) -> Result<(u64, usize)> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *payload
            .get(offset)
            .ok_or_else(|| anyhow!("protobuf varint is truncated"))?;
        offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, offset));
        }
    }
    bail!("protobuf varint is too long")
}

fn validate_coremeta_commit_evidence_row(payload: &[u8]) -> Result<()> {
    let row = CoreMetaCommitEvidenceRowProto::decode(payload)?;
    let mut canonical = Vec::new();
    row.encode(&mut canonical)?;
    if canonical != payload {
        bail!("CoreMeta commit evidence row is not deterministic protobuf");
    }
    validate_coremeta_hash(
        &row.certificate_hash,
        "CoreMeta commit evidence certificate hash",
    )?;
    validate_coremeta_hash(
        &row.committed_batch_hash,
        "CoreMeta commit evidence committed batch hash",
    )?;
    if row.certificate_bytes.is_empty() {
        bail!("CoreMeta commit evidence row is missing certificate bytes");
    }
    for receipt_hash in &row.certificate_persist_receipt_hashes {
        validate_coremeta_hash(receipt_hash, "CoreMeta certificate persist receipt hash")?;
    }
    if row.certificate_persist_receipt_bytes.len() < row.certificate_persist_receipt_hashes.len() {
        bail!("CoreMeta commit evidence row is missing certificate persistence receipt bytes");
    }
    let common = row
        .common
        .ok_or_else(|| anyhow!("CoreMeta commit evidence row is missing common metadata"))?;
    validate_coremeta_common(&common, CoreMetaVisibilityState::Committed)?;
    Ok(())
}

fn validate_coremeta_pending_batch_marker_row(payload: &[u8]) -> Result<()> {
    let row = CoreMetaPendingBatchMarkerRowProto::decode(payload)?;
    let mut canonical = Vec::new();
    row.encode(&mut canonical)?;
    if canonical != payload {
        bail!("CoreMeta pending batch marker row is not deterministic protobuf");
    }
    validate_coremeta_hash(&row.pending_batch_hash, "CoreMeta pending batch hash")?;
    validate_coremeta_hash(&row.root_key_hash, "CoreMeta pending batch root key hash")?;
    validate_coremeta_logical_id(&row.transaction_id, "CoreMeta pending batch transaction id")?;
    if row.core_meta_row_count == 0 {
        bail!("CoreMeta pending batch marker must reference at least one CoreMeta row");
    }
    if row.post_root_generation <= row.expected_root_generation {
        bail!(
            "CoreMeta pending batch marker root generation must advance: root_key_hash={}, transaction_id={}, expected_root_generation={}, post_root_generation={}",
            row.root_key_hash,
            row.transaction_id,
            row.expected_root_generation,
            row.post_root_generation
        );
    }
    let common = row
        .common
        .ok_or_else(|| anyhow!("CoreMeta pending batch marker row is missing common metadata"))?;
    validate_coremeta_common(&common, CoreMetaVisibilityState::Pending)?;
    if common.root_key_hash != row.root_key_hash
        || common.root_generation != row.post_root_generation
        || common.transaction_id != row.transaction_id
    {
        bail!("CoreMeta pending batch marker common metadata does not match row scope");
    }
    Ok(())
}

mod payload_validation;
pub use payload_validation::core_meta_payload_digest;
use payload_validation::*;
#[cfg(test)]
#[path = "meta_tests.rs"]
mod tests;
