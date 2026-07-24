use super::block_shard::{
    BlockShardExpectation, BlockShardHeaderInput, ShardReceiptPayloadInput, boundary_summary_hash,
    encode_block_shard_file, encode_boundary_values_b64, read_block_shard_file,
    shard_receipt_payload_hash, validate_boundary_summary_fields,
};
#[cfg(test)]
use super::block_shard::{BlockShardHeaderProto, CORE_BLOCK_SHARD_MAGIC};
use super::coremeta_quorum::{
    CoreMetaCertificatePersistReceipt, CoreMetaCommitCertificate, CoreMetaCommittedBatchInput,
    CoreMetaPendingBatchInput, CoreMetaPrepareReceipt, CoreMetaQuorumProfile,
    build_commit_certificate, certificate_persist_receipt_payload_hash, committed_batch_hash,
    core_meta_encoded_row_hash_with_delete, pending_batch_hash, prepare_receipt_payload_hash,
    validate_commit_evidence_with_verifier,
};
use super::deterministic_proto::{decode_deterministic_proto, encode_deterministic_proto};
use super::manifest_proto::{
    CoreInlineManifestBodyRow, decode_inline_manifest_body_row, decode_logical_file_manifest_proto,
    encode_inline_manifest_body_row, encode_logical_file_manifest_proto,
};
#[cfg(test)]
use super::meta::CORE_META_MAX_VALUE_BYTES;
use super::meta::{
    CF_BOUNDARY, CF_INLINE_PAYLOADS, CF_LEASES_FENCES, CF_MATERIALISATION, CF_MESH,
    CF_OBJECT_HEADS, CF_OBJECT_VERSIONS, CF_REFCOUNTS, CF_ROOT_CACHE, CF_STREAM_HEADS,
    CF_STREAM_RECORDS, CF_TRANSACTIONS, CORE_META_INLINE_MANIFEST_BODY_MAX_BYTES,
    CORE_META_MAX_INLINE_PAYLOAD_BYTES, CORE_META_MAX_SCAN_PAGE_ROWS,
    CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES, CoreMetaBatchOp, CoreMetaBatchOpKind,
    CoreMetaEncodedOwnedRow, CoreMetaEncodedRow, CoreMetaLocatorProto, CoreMetaReadSnapshot,
    CoreMetaReader, CoreMetaRecord, CoreMetaRowCommonProto, CoreMetaStore, CoreMetaTuplePart,
    CoreMetaVisibilityState, TABLE_BOUNDARY_SCHEMA_CURRENT_ROW, TABLE_BOUNDARY_SCHEMA_ROW,
    TABLE_BOUNDARY_VALUE_ROW, TABLE_CORE_FENCE_ROW, TABLE_EXPLICIT_TRANSACTION_ROW,
    TABLE_INLINE_MANIFEST_BODY_ROW, TABLE_INLINE_PAYLOAD_ROW, TABLE_LANDED_BYTE_REF_ROW,
    TABLE_LOCAL_ADMISSION_EVIDENCE_ROW, TABLE_LOCAL_NODE_IDENTITY_ROW,
    TABLE_MATERIALISATION_CURSOR_ROW, TABLE_NODE_SIGNING_KEYPAIR_ROW, TABLE_OBJECT_HEAD_ROW,
    TABLE_OBJECT_SHARD_REPAIR_ROW, TABLE_OBJECT_VERSION_META_ROW, TABLE_PENDING_MUTATION_ROW,
    TABLE_QUORUM_PROFILE_CURRENT_ROW, TABLE_REFCOUNT_ROW, TABLE_ROOT_CACHE_ROW,
    TABLE_ROOT_CATALOG_CURRENT_ROW, TABLE_ROOT_FAILOVER_CERTIFICATE_ROW,
    TABLE_ROOT_FAILOVER_VOTE_ROW, TABLE_ROOT_PUBLICATION_INTENT_ROW, TABLE_STREAM_HEAD_ROW,
    TABLE_STREAM_IDEMPOTENCY_ROW, TABLE_STREAM_RECORD_INDEX_ROW,
    TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW, TABLE_TRANSACTION_LOCATOR_ROW,
    TABLE_TRANSACTION_MANIFEST_BODY_ROW, canonical_coremeta_cf_name,
    core_meta_bootstrap_row_common, core_meta_committed_row_common,
    core_meta_locator_from_manifest_locator, core_meta_locator_to_manifest_locator,
    core_meta_payload_digest, core_meta_pending_row_common, core_meta_record_table_id,
    core_meta_record_tuple_key, core_meta_root_key_hash, core_meta_row_common_from_payload,
    core_meta_tuple_key, encode_core_meta_inline_payload_row, replace_core_meta_row_common,
    validate_coremeta_operation_key, validate_coremeta_operation_payload,
};
use super::pending_mutation::*;
use super::root_proto::{decode_root_anchor_proto, encode_root_anchor_proto};
use super::storage_profile::{
    CoreByteStorageProfile, CoreInlinePayloadPolicy, CoreMetadataProfile, CoreStorageClass,
    CoreStorageClassCatalog,
};
#[cfg(test)]
use super::stream_segment::CORE_STREAM_SEGMENT_MAGIC;
use super::stream_segment::{decode_stream_segment, encode_stream_segment};
use super::transaction_manifest_proto::{
    decode_manifest_locator_proto, decode_transaction_manifest_proto,
    encode_transaction_manifest_body_proto, encode_transaction_manifest_header_proto,
};
use super::types::*;
use crate::error_codes::AnvilErrorCode;
use crate::formats::writer::{WriterFamily, canonical_logical_file_id};
use crate::node_signing::NodeSigningKeypair;
use crate::storage::Storage;
use aes_gcm_siv::aead::{Aead, AeadCore, OsRng, Payload};
use aes_gcm_siv::{Aes256GcmSiv, Nonce};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use fs2::FileExt;
use hmac::{Hmac, Mac};
#[cfg(test)]
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex as StdMutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Notify, OwnedMutexGuard};
use tonic::transport::{Channel, Endpoint};

const CORE_PROCESS_LOCK_RETRY_ATTEMPTS: usize = 12_000;
const CORE_PROCESS_LOCK_RETRY_DELAY: Duration = Duration::from_millis(5);
const CORE_CONTROL_READ_RETRY_ATTEMPTS: usize = 400;
const CORE_INTERNAL_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const CORE_INTERNAL_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CORE_INTERNAL_REQUEST_ATTEMPTS: usize = 4;
const LOCAL_ERASURE_PROFILE_ID: &str = "ec-4-2";
const LOCAL_PLACEMENT_EPOCH: u64 = 1;
const LOCAL_SHARD_FSYNC_SEQUENCE: u64 = 1;
#[cfg(test)]
const LOCAL_DATA_SHARDS: usize = 4;
#[cfg(test)]
const LOCAL_PARITY_SHARDS: usize = 2;
const LOCAL_NODE_ID_PREFIX: &str = "local-node";
const LOCAL_CONTROL_REPLICA_COUNT: usize = 5;
const LOCAL_CONTROL_NODE_ID_PREFIX: &str = "local-control-node";
const LOCAL_ERASURE_SET_ID: &str = "local-erasure-set";

#[derive(Debug, thiserror::Error)]
pub enum CoreStoreCommitError {
    #[error(
        "CoreStore stream {stream_id} head mismatch: expected {expected_last_sequence}/{expected_last_event_hash}, got {actual_sequence}/{actual_event_hash}"
    )]
    StreamHeadMismatch {
        stream_id: String,
        expected_last_sequence: u64,
        expected_last_event_hash: String,
        actual_sequence: u64,
        actual_event_hash: String,
    },
    #[error("CoreMeta row {cf}/{table_id:#06x}/{tuple_key_hex} precondition failed: {reason}")]
    CoreMetaRowPreconditionFailed {
        cf: String,
        table_id: u16,
        tuple_key_hex: String,
        reason: String,
    },
    #[error(
        "CoreMeta root {root_key_hash} changed before durable staging: expected generation {expected_generation} hash {expected_hash}, got generation {actual_generation} hash {actual_hash}"
    )]
    RootChangedBeforeDurableStaging {
        root_key_hash: String,
        expected_generation: u64,
        expected_hash: String,
        actual_generation: u64,
        actual_hash: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum CoreStoreAvailabilityError {
    #[error("CoreStore mesh topology is not ready to verify node {node_id}")]
    MeshTopologyUnavailable { node_id: String },
    #[error(
        "CoreMeta quorum unavailable during {operation}: required {required}, received {received}: {details}"
    )]
    QuorumUnavailable {
        operation: &'static str,
        required: usize,
        received: usize,
        details: String,
    },
    #[error("CoreStore peer unavailable during {operation} at {endpoint}: {details}")]
    PeerUnavailable {
        operation: String,
        endpoint: String,
        details: String,
    },
    #[error(
        "CoreStore shard quorum unavailable during {operation}: required {required}, received {received}: {details}"
    )]
    ShardQuorumUnavailable {
        operation: &'static str,
        required: usize,
        received: usize,
        details: String,
    },
}

pub fn is_core_store_unavailable(error: &anyhow::Error) -> bool {
    core_store_availability_error(error).is_some()
}

pub fn core_store_availability_error(error: &anyhow::Error) -> Option<&CoreStoreAvailabilityError> {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<CoreStoreAvailabilityError>())
}

pub fn is_stream_head_mismatch(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<CoreStoreCommitError>(),
            Some(CoreStoreCommitError::StreamHeadMismatch { .. })
        )
    })
}

pub fn is_retryable_mutation_conflict(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<CoreStoreCommitError>(),
            Some(
                CoreStoreCommitError::StreamHeadMismatch { .. }
                    | CoreStoreCommitError::CoreMetaRowPreconditionFailed { .. }
                    | CoreStoreCommitError::RootChangedBeforeDurableStaging { .. }
            )
        )
    })
}

const ZERO_HASH: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const MAX_CORE_FENCE_TTL_MS: u64 = 120_000;
const CORE_ROOT_ANCHOR_MAGIC: &[u8; 8] = b"ANROOT1\0";
const CORE_TRANSACTION_MANIFEST_MAGIC: &[u8; 8] = b"ANXACT1\0";
const CORE_ROOT_ANCHOR_VERSION: u16 = 1;
const CORE_TRANSACTION_MANIFEST_VERSION: u16 = 1;
const CORE_PENDING_MUTATION_SOFT_LIMIT_ROWS: u64 = 1_000_000;
const CORE_PENDING_MUTATION_HARD_LIMIT_ROWS: u64 = 2_000_000;
const CORE_PENDING_MUTATION_SOFT_LIMIT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const CORE_PENDING_MUTATION_HARD_LIMIT_BYTES: u64 = 12 * 1024 * 1024 * 1024;
const CORE_PENDING_MUTATION_SOFT_LAG_SECONDS: u64 = 60;
const CORE_PENDING_MUTATION_HARD_LAG_SECONDS: u64 = 300;
const CORE_LANDED_BYTES_SOFT_LIMIT_BYTES: u64 = 2 * CORE_PENDING_MUTATION_SOFT_LIMIT_BYTES;
const CORE_LANDED_BYTES_HARD_LIMIT_BYTES: u64 = 3 * CORE_PENDING_MUTATION_SOFT_LIMIT_BYTES;
const CORE_PENDING_MUTATION_SOFT_BACKPRESSURE_DELAY: Duration = Duration::from_millis(1);
const CORE_TRANSACTION_STREAM_ID: &str = "core_transactions";
const CORE_TRANSACTION_PARTITION_ID: &str = "core-control";
const CORE_TRANSACTION_ROOT_PARTITION_ID: u64 = 0;
const CORE_TRANSACTION_RECORD_KIND: &str = "core_transaction";
const CORE_PIPELINE_KEY_LEN: usize = 32;
const CORE_PIPELINE_NONCE_LEN: usize = 12;
const LOCAL_INLINE_PAYLOAD_PROFILE_ID: &str = "inline-rocksdb";
const LOCAL_INLINE_PAYLOAD_BLOCK_PREFIX: &str = "inline-payload";

type HmacSha256 = Hmac<Sha256>;

static CORE_STORE_STARTUP_RECOVERY_LOCKS: LazyLock<StdMutex<BTreeMap<PathBuf, Weak<Mutex<()>>>>> =
    LazyLock::new(|| StdMutex::new(BTreeMap::new()));
static CORE_STORE_NAMED_LOCKS: LazyLock<StdMutex<BTreeMap<PathBuf, Weak<Mutex<()>>>>> =
    LazyLock::new(|| StdMutex::new(BTreeMap::new()));
static CORE_STORE_INSTANCE_REGISTRY: LazyLock<StdMutex<BTreeMap<PathBuf, CoreStore>>> =
    LazyLock::new(|| StdMutex::new(BTreeMap::new()));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalErasureProfile {
    id: &'static str,
    codec_id: &'static str,
    data_shards: usize,
    parity_shards: usize,
    minimum_read_shards: usize,
    minimum_write_ack_shards: usize,
    logical_block_target_bytes: u64,
    max_shard_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalShardPlacement {
    node_id: String,
    region_id: String,
    cell_id: String,
    failure_domain: String,
    region_weight: u32,
    cell_weight: u32,
    public_api_addr: String,
    is_local: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoreMetaPeerTarget {
    pub(crate) node_id: String,
    pub(crate) public_api_addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoreMetaPeerRoute {
    pub(crate) local_replica: bool,
    pub(crate) remote_targets: Vec<CoreMetaPeerTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CoreMetaWriteRoute {
    Local,
    Remote(CoreMetaPeerTarget),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalPlacementCellInfo {
    failure_domain: String,
    region_weight: u32,
    cell_weight: u32,
}

#[derive(Debug, Clone)]
struct MaterializedLogicalBlock {
    object_manifest: CoreObjectManifest,
    logical_offset: u64,
    logical_length: u64,
    compressed_length: u64,
    plaintext_hash: String,
    compression: CoreCompressionDescriptor,
    encryption: CoreEncryptionDescriptor,
}

impl LocalErasureProfile {
    fn total_shards(self) -> usize {
        self.data_shards + self.parity_shards
    }
}

const LOCAL_EC_4_2_PROFILE: LocalErasureProfile = LocalErasureProfile {
    id: "ec-4-2",
    codec_id: "rs-gf256-vandermonde-0x11d-v1/ec-4-2",
    data_shards: 4,
    parity_shards: 2,
    minimum_read_shards: 4,
    minimum_write_ack_shards: 6,
    logical_block_target_bytes: 64 * 1024 * 1024,
    max_shard_size_bytes: 16 * 1024 * 1024,
};

const LOCAL_EC_8_3_PROFILE: LocalErasureProfile = LocalErasureProfile {
    id: "ec-8-3",
    codec_id: "rs-gf256-vandermonde-0x11d-v1/ec-8-3",
    data_shards: 8,
    parity_shards: 3,
    minimum_read_shards: 8,
    minimum_write_ack_shards: 11,
    logical_block_target_bytes: 128 * 1024 * 1024,
    max_shard_size_bytes: 16 * 1024 * 1024,
};

const LOCAL_REPLICATED_3_PROFILE: LocalErasureProfile = LocalErasureProfile {
    id: "replicated-3",
    codec_id: "rs-gf256-vandermonde-0x11d-v1/replicated-3",
    data_shards: 1,
    parity_shards: 2,
    minimum_read_shards: 1,
    minimum_write_ack_shards: 3,
    logical_block_target_bytes: 16 * 1024 * 1024,
    max_shard_size_bytes: 16 * 1024 * 1024,
};

#[derive(Debug, Clone, Copy)]
struct CoreAdmissionCapacityLimits {
    pending_mutation_soft_limit_rows: u64,
    pending_mutation_hard_limit_rows: u64,
    pending_mutation_soft_limit_bytes: u64,
    pending_mutation_hard_limit_bytes: u64,
    pending_mutation_soft_lag_seconds: u64,
    pending_mutation_hard_lag_seconds: u64,
    landed_bytes_soft_limit_bytes: u64,
    landed_bytes_hard_limit_bytes: u64,
}

impl CoreAdmissionCapacityLimits {
    const fn production() -> Self {
        Self {
            pending_mutation_soft_limit_rows: CORE_PENDING_MUTATION_SOFT_LIMIT_ROWS,
            pending_mutation_hard_limit_rows: CORE_PENDING_MUTATION_HARD_LIMIT_ROWS,
            pending_mutation_soft_limit_bytes: CORE_PENDING_MUTATION_SOFT_LIMIT_BYTES,
            pending_mutation_hard_limit_bytes: CORE_PENDING_MUTATION_HARD_LIMIT_BYTES,
            pending_mutation_soft_lag_seconds: CORE_PENDING_MUTATION_SOFT_LAG_SECONDS,
            pending_mutation_hard_lag_seconds: CORE_PENDING_MUTATION_HARD_LAG_SECONDS,
            landed_bytes_soft_limit_bytes: CORE_LANDED_BYTES_SOFT_LIMIT_BYTES,
            landed_bytes_hard_limit_bytes: CORE_LANDED_BYTES_HARD_LIMIT_BYTES,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoreStore {
    storage: Storage,
    meta: CoreMetaStore,
    startup_recovery_lock: Arc<Mutex<()>>,
    internal_channels: Arc<Mutex<BTreeMap<String, Channel>>>,
    coremeta_streams: Arc<Mutex<BTreeMap<String, local_coremeta_stream::CoreMetaPeerStream>>>,
    coremeta_recovery: Arc<local_coremeta_recovery::CoreMetaRecoveryState>,
    repair_task_scheduler: Arc<OnceLock<local_repair_task_scheduler::RepairTaskScheduler>>,
    root_owner_failure_tracker: Arc<Mutex<local_root_failover::RootOwnerFailureTracker>>,
    pipeline_keyring: Option<Arc<CorePipelineKeyring>>,
    storage_classes: CoreStorageClassCatalog,
    node_signing_keypair: Arc<NodeSigningKeypair>,
    admission_mutation_epoch: u64,
    node_identity: CoreStoreNodeIdentity,
    startup_recovery_deferred: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreStoreStartupRecovery {
    Immediate,
    Distributed,
}

impl CoreStore {
    pub(crate) async fn internal_grpc_channel(
        &self,
        public_api_addr: &str,
        operation_label: &str,
    ) -> Result<Channel> {
        let endpoint = normalise_grpc_endpoint(public_api_addr)?;
        if let Some(channel) = self.internal_channels.lock().await.get(&endpoint).cloned() {
            return Ok(channel);
        }

        let channel = Endpoint::from_shared(endpoint.clone())?
            .connect_timeout(CORE_INTERNAL_CONNECT_TIMEOUT)
            .timeout(CORE_INTERNAL_REQUEST_TIMEOUT)
            .connect()
            .await
            .with_context(|| format!("connect {operation_label} replica at {endpoint}"))?;
        let mut channels = self.internal_channels.lock().await;
        Ok(channels
            .entry(endpoint)
            .or_insert_with(|| channel.clone())
            .clone())
    }

    pub(super) async fn internal_grpc_request<T, F, Fut>(
        &self,
        public_api_addr: &str,
        operation_label: &str,
        mut call: F,
    ) -> Result<T>
    where
        F: FnMut(Channel) -> Fut,
        Fut: Future<Output = std::result::Result<T, tonic::Status>>,
    {
        let total_started_at = Instant::now();
        let endpoint = normalise_grpc_endpoint(public_api_addr)?;
        let mut failures = Vec::new();
        for attempt in 0..CORE_INTERNAL_REQUEST_ATTEMPTS {
            let channel_started_at = Instant::now();
            let channel = match self
                .internal_grpc_channel(public_api_addr, operation_label)
                .await
            {
                Ok(channel) => channel,
                Err(error) => {
                    failures.push(format!("connect attempt {}: {error}", attempt + 1));
                    self.internal_channels.lock().await.remove(&endpoint);
                    if attempt + 1 < CORE_INTERNAL_REQUEST_ATTEMPTS {
                        tokio::time::sleep(core_internal_retry_delay(attempt)).await;
                        continue;
                    }
                    break;
                }
            };
            crate::emit_test_timing(
                format!("coremeta.internal.client {operation_label} channel"),
                channel_started_at.elapsed(),
            );

            let call_started_at = Instant::now();
            match call(channel).await {
                Ok(value) => {
                    crate::emit_test_timing(
                        format!("coremeta.internal.client {operation_label} call"),
                        call_started_at.elapsed(),
                    );
                    crate::emit_test_timing(
                        format!("coremeta.internal.client {operation_label} total"),
                        total_started_at.elapsed(),
                    );
                    return Ok(value);
                }
                Err(status) if retryable_internal_status(&status) => {
                    failures.push(format!(
                        "request attempt {}: code={:?} message={}",
                        attempt + 1,
                        status.code(),
                        status.message()
                    ));
                    self.internal_channels.lock().await.remove(&endpoint);
                    if attempt + 1 < CORE_INTERNAL_REQUEST_ATTEMPTS {
                        tokio::time::sleep(core_internal_retry_delay(attempt)).await;
                    }
                }
                Err(status) => {
                    return Err(anyhow!(
                        "{operation_label} request to {endpoint} failed: code={:?} message={}",
                        status.code(),
                        status.message()
                    ));
                }
            }
        }

        Err(CoreStoreAvailabilityError::PeerUnavailable {
            operation: operation_label.to_string(),
            endpoint,
            details: format!(
                "failed after {CORE_INTERNAL_REQUEST_ATTEMPTS} attempts: {}",
                failures.join("; ")
            ),
        }
        .into())
    }
}

fn core_internal_retry_delay(attempt: usize) -> Duration {
    Duration::from_millis(50_u64.saturating_mul(1_u64 << attempt.min(4)))
}

fn retryable_internal_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
    ) || (status.code() == tonic::Code::Unknown
        && ["transport", "service was not ready", "connection"]
            .iter()
            .any(|needle| status.message().to_ascii_lowercase().contains(needle)))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreStoreNodeIdentity {
    pub mesh_id: String,
    pub node_id: String,
    pub region_id: String,
    pub cell_id: String,
    pub public_api_addr: String,
    pub internal_bearer_token: Option<String>,
}

impl Default for CoreStoreNodeIdentity {
    fn default() -> Self {
        Self {
            mesh_id: "local".to_string(),
            node_id: "local-corestore-node".to_string(),
            region_id: "local".to_string(),
            cell_id: "local-cell-1".to_string(),
            public_api_addr: String::new(),
            internal_bearer_token: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorePipelineKeyring {
    active_key_id: String,
    keys: BTreeMap<String, [u8; CORE_PIPELINE_KEY_LEN]>,
}

impl CorePipelineKeyring {
    pub fn new(
        active_key_id: impl Into<String>,
        active_key: [u8; CORE_PIPELINE_KEY_LEN],
    ) -> Result<Self> {
        let active_key_id = validate_pipeline_key_id(active_key_id.into())?;
        let mut keys = BTreeMap::new();
        keys.insert(active_key_id.clone(), active_key);
        Ok(Self {
            active_key_id,
            keys,
        })
    }

    pub fn from_hex_config(
        active_key_id: &str,
        active_key_hex: &str,
        previous_keys: &str,
    ) -> Result<Self> {
        let mut keyring = Self::new(active_key_id, decode_pipeline_key_hex(active_key_hex)?)?;
        for item in previous_keys
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            let (key_id, key_hex) = item
                .split_once(':')
                .ok_or_else(|| anyhow!("previous CoreStore pipeline keys must be key_id:hex"))?;
            keyring.insert_previous_key(key_id, decode_pipeline_key_hex(key_hex)?)?;
        }
        Ok(keyring)
    }

    pub fn active_key_id(&self) -> &str {
        &self.active_key_id
    }

    pub fn insert_previous_key(
        &mut self,
        key_id: &str,
        key: [u8; CORE_PIPELINE_KEY_LEN],
    ) -> Result<()> {
        let key_id = validate_pipeline_key_id(key_id.to_string())?;
        if key_id == self.active_key_id {
            bail!("previous CoreStore pipeline key id must not equal active key id");
        }
        if self.keys.insert(key_id.clone(), key).is_some() {
            bail!("duplicate CoreStore pipeline key id '{key_id}'");
        }
        Ok(())
    }

    fn active_key(&self) -> Result<&[u8; CORE_PIPELINE_KEY_LEN]> {
        self.keys
            .get(&self.active_key_id)
            .ok_or_else(|| anyhow!("active CoreStore pipeline key is not present in keyring"))
    }

    fn key(&self, key_id: &str) -> Result<&[u8; CORE_PIPELINE_KEY_LEN]> {
        self.keys
            .get(key_id)
            .ok_or_else(|| anyhow!("CoreStore pipeline key id '{key_id}' is not configured"))
    }
}

fn startup_recovery_lock(storage_root: PathBuf) -> Arc<Mutex<()>> {
    let mut locks = CORE_STORE_STARTUP_RECOVERY_LOCKS
        .lock()
        .expect("CoreStore startup-recovery lock registry poisoned");
    if let Some(existing) = locks.get(&storage_root).and_then(Weak::upgrade) {
        return existing;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(storage_root, Arc::downgrade(&lock));
    lock
}

fn process_named_lock(lock_path: PathBuf) -> Arc<Mutex<()>> {
    let mut locks = CORE_STORE_NAMED_LOCKS
        .lock()
        .expect("CoreStore named-lock registry poisoned");
    if let Some(existing) = locks.get(&lock_path).and_then(Weak::upgrade) {
        return existing;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(lock_path, Arc::downgrade(&lock));
    lock
}

#[derive(Debug, Clone)]
struct PipelineBlockBytes {
    stored: Vec<u8>,
    encryption: CoreEncryptionDescriptor,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStreamRecord {
    schema: String,
    stream_id: String,
    partition_id: String,
    sequence: u64,
    cursor: String,
    previous_event_hash: String,
    event_hash: String,
    record_kind: String,
    payload_hash: String,
    payload: Vec<u8>,
    content_type: Option<String>,
    user_metadata_json: String,
    #[serde(default)]
    authenticated_principal: String,
    transaction_id: Option<String>,
    idempotency_key_hash: Option<String>,
    created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStreamRecordIndexRow {
    schema: String,
    stream_id: String,
    partition_id: String,
    sequence: u64,
    cursor: String,
    previous_event_hash: String,
    event_hash: String,
    record_kind: String,
    payload_hash: String,
    payload_len: u64,
    content_type: Option<String>,
    user_metadata_json: String,
    #[serde(default)]
    authenticated_principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    inline_payload: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_locator: Option<CoreManifestLocator>,
    transaction_id: Option<String>,
    idempotency_key_hash: Option<String>,
    created_at: String,
}

#[derive(Debug, Clone)]
struct StoredStreamIdempotencyRow {
    schema: String,
    stream_id: String,
    sequence: u64,
    cursor: String,
    event_hash: String,
    record_kind: String,
    payload_hash: String,
    authenticated_principal: String,
    transaction_id: Option<String>,
    idempotency_key_hash: String,
    created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CoreRootAnchorRecord {
    pub(crate) schema: String,
    pub(crate) root_anchor_key: String,
    pub(crate) root_key_hash: String,
    pub(crate) root_generation: u64,
    pub(crate) previous_root_hash: String,
    pub(crate) transaction_manifest: Option<CoreManifestLocator>,
    pub(crate) checkpoint_manifest: Option<CoreManifestLocator>,
    pub(crate) core_meta_commit_certificate_hash: Option<String>,
    pub(crate) certificate_persist_receipt_hashes: Vec<String>,
    pub(crate) publisher_node_id: String,
    pub(crate) publisher_epoch: u64,
    pub(crate) partition_owner_fence: u64,
    pub(crate) created_at_unix_nanos: u64,
    pub(crate) root_state: String,
    pub(crate) mutation_first: Option<String>,
    pub(crate) mutation_last: Option<String>,
    pub(crate) writer_families: Vec<String>,
    pub(crate) manifest_count: u64,
    pub(crate) final_block_count: u64,
    pub(super) genesis_bundle: Option<CoreGenesisBundle>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct CoreGenesisBundle {
    pub(super) schema: String,
    pub(super) genesis_config_hash: String,
    pub(super) mesh_control_segment: Vec<u8>,
    pub(super) authz_reserved_schema_segment: Vec<u8>,
    pub(super) initial_root_keys: Vec<String>,
    pub(super) initial_partition_map: Vec<CoreGenesisPartition>,
    pub(super) created_at_unix_nanos: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct CoreGenesisPartition {
    pub(super) root_anchor_key: String,
    pub(super) root_partition_id: u64,
    pub(super) owner_node_id: String,
    pub(super) owner_epoch: u64,
    pub(super) owner_fence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct CoreTransactionManifestRecord {
    pub(super) schema: String,
    pub(super) root_key_hash: String,
    pub(super) coordinator_root_key_hash: Option<String>,
    pub(super) coordinator_root_generation: Option<u64>,
    pub(super) mutation_ids: Vec<String>,
    pub(super) idempotency_key_hashes: Vec<String>,
    pub(super) pre_root_generation: u64,
    pub(super) post_root_generation: u64,
    pub(super) logical_manifests: Vec<CoreManifestLocator>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CoreStoredStreamHead {
    schema: String,
    stream_id: String,
    last_sequence: u64,
    last_event_hash: String,
    record_count: u64,
    idempotency_index_complete: bool,
    updated_at: String,
}

#[derive(Debug, Clone)]
struct StreamAppendOutcome {
    receipt: StreamAppendReceipt,
    state_locator: Option<CoreManifestLocator>,
}

struct PreparedStreamMetadataWrite {
    transaction_id: String,
    owned_ops: Vec<local_tx_rows::OwnedCoreMetaBatchOp>,
    root_publications: Vec<CoreMetaRootPublication>,
}

struct PreparedStreamAppend {
    outcome: StreamAppendOutcome,
    record: Option<StreamRecord>,
    metadata: PreparedStreamMetadataWrite,
}

struct CoreStoreLock {
    path: PathBuf,
    file: std::fs::File,
    _process_guard: OwnedMutexGuard<()>,
}

impl Drop for CoreStoreLock {
    fn drop(&mut self) {
        let started_at = Instant::now();
        let _ = FileExt::unlock(&self.file);
        crate::perf::record_io_duration(
            "core_store",
            "lock_advisory_unlock",
            &self.path,
            0,
            started_at.elapsed(),
        );
    }
}

impl From<StoredStreamRecord> for StreamRecord {
    fn from(record: StoredStreamRecord) -> Self {
        Self {
            schema: record.schema,
            stream_id: record.stream_id,
            partition_id: record.partition_id,
            sequence: record.sequence,
            cursor: record.cursor,
            previous_event_hash: record.previous_event_hash,
            event_hash: record.event_hash,
            record_kind: record.record_kind,
            payload_hash: record.payload_hash,
            payload: record.payload,
            content_type: record.content_type,
            user_metadata_json: record.user_metadata_json,
            authenticated_principal: record.authenticated_principal,
            transaction_id: record.transaction_id,
            idempotency_key_hash: record.idempotency_key_hash,
            created_at: record.created_at,
        }
    }
}

impl From<&StreamRecord> for StoredStreamRecord {
    fn from(record: &StreamRecord) -> Self {
        Self {
            schema: record.schema.clone(),
            stream_id: record.stream_id.clone(),
            partition_id: record.partition_id.clone(),
            sequence: record.sequence,
            cursor: record.cursor.clone(),
            previous_event_hash: record.previous_event_hash.clone(),
            event_hash: record.event_hash.clone(),
            record_kind: record.record_kind.clone(),
            payload_hash: record.payload_hash.clone(),
            payload: record.payload.clone(),
            content_type: record.content_type.clone(),
            user_metadata_json: record.user_metadata_json.clone(),
            authenticated_principal: record.authenticated_principal.clone(),
            transaction_id: record.transaction_id.clone(),
            idempotency_key_hash: record.idempotency_key_hash.clone(),
            created_at: record.created_at.clone(),
        }
    }
}

impl StoredStreamRecordIndexRow {
    fn new(
        record: &StreamRecord,
        inline_payload: Option<Vec<u8>>,
        payload_locator: Option<CoreManifestLocator>,
    ) -> Self {
        Self {
            schema: "anvil.core.stream_record_index.v1".to_string(),
            stream_id: record.stream_id.clone(),
            partition_id: record.partition_id.clone(),
            sequence: record.sequence,
            cursor: record.cursor.clone(),
            previous_event_hash: record.previous_event_hash.clone(),
            event_hash: record.event_hash.clone(),
            record_kind: record.record_kind.clone(),
            payload_hash: record.payload_hash.clone(),
            payload_len: record.payload.len() as u64,
            content_type: record.content_type.clone(),
            user_metadata_json: record.user_metadata_json.clone(),
            authenticated_principal: record.authenticated_principal.clone(),
            inline_payload,
            payload_locator,
            transaction_id: record.transaction_id.clone(),
            idempotency_key_hash: record.idempotency_key_hash.clone(),
            created_at: record.created_at.clone(),
        }
    }
}

impl StoredStreamIdempotencyRow {
    fn from_record_index(row: &StoredStreamRecordIndexRow) -> Option<Self> {
        Some(Self {
            schema: "anvil.core.stream_idempotency.v1".to_string(),
            stream_id: row.stream_id.clone(),
            sequence: row.sequence,
            cursor: row.cursor.clone(),
            event_hash: row.event_hash.clone(),
            record_kind: row.record_kind.clone(),
            payload_hash: row.payload_hash.clone(),
            authenticated_principal: row.authenticated_principal.clone(),
            transaction_id: row.transaction_id.clone(),
            idempotency_key_hash: row.idempotency_key_hash.clone()?,
            created_at: row.created_at.clone(),
        })
    }
}

#[path = "local_admission.rs"]
mod local_admission;
#[path = "local_blob_read.rs"]
mod local_blob_read;
#[path = "local_block_distribution.rs"]
mod local_block_distribution;
#[path = "local_boundaries.rs"]
mod local_boundaries;
#[path = "local_codec.rs"]
mod local_codec;
#[path = "local_coremeta_history.rs"]
pub(in crate::core_store) mod local_coremeta_history;
#[path = "local_coremeta_quorum.rs"]
mod local_coremeta_quorum;
#[path = "local_coremeta_recovery.rs"]
mod local_coremeta_recovery;
#[path = "local_coremeta_stream.rs"]
mod local_coremeta_stream;
#[path = "local_erasure.rs"]
mod local_erasure;
#[path = "local_init_blob.rs"]
mod local_init_blob;
#[path = "local_internal_coremeta.rs"]
mod local_internal_coremeta;
#[path = "local_internal_roots.rs"]
mod local_internal_roots;
#[path = "local_internal_shards.rs"]
mod local_internal_shards;
#[path = "local_internal_signing.rs"]
mod local_internal_signing;
#[path = "local_io.rs"]
mod local_io;
#[path = "local_key_helpers.rs"]
mod local_key_helpers;
#[path = "local_logical_file_path.rs"]
mod local_logical_file_path;
#[path = "local_logical_files.rs"]
mod local_logical_files;
#[path = "local_mutation_preparation.rs"]
mod local_mutation_preparation;
#[path = "local_object_metadata.rs"]
mod local_object_metadata;
pub(crate) use local_object_metadata::{
    CurrentObjectMetadataPage, ObjectMetadataMutationGuard, ObjectMetadataPageCursor,
    ObjectMetadataPreconditionSnapshot, ObjectMetadataProjectionMutation,
    ObjectVersionsMetadataPage, PreparedObjectMetadataProjection,
};
#[path = "local_pending_finalisation.rs"]
mod local_pending_finalisation;
#[path = "local_refcounts.rs"]
mod local_refcounts;
#[path = "local_repair_task_scheduler.rs"]
mod local_repair_task_scheduler;
pub(crate) use self::local_codec::{decode_core_object_ref_target, encode_core_object_ref_target};
pub(crate) use self::local_coremeta_quorum::commit_coremeta_batch_for_storage;
pub(crate) use self::local_io::{record_corestore_trace_event, write_file_atomic};
pub(crate) use self::local_roots::decode_root_anchor_record;

/// Derives the physical publication identity from a stable logical mutation
/// identity and the exact guards protecting this attempt.
///
/// Logical/content idempotency must continue to use `logical_id`; a renewed
/// lease produces a new publication attempt without changing that identity.
pub(crate) fn core_mutation_publication_attempt_id(
    logical_id: &str,
    preconditions: &[CoreMutationPrecondition],
) -> Result<String> {
    let preconditions_hash = core_mutation_preconditions_hash(preconditions)?;
    Ok(format!("{logical_id}:attempt:{preconditions_hash}"))
}

#[path = "local_root_binding.rs"]
mod local_root_binding;
#[path = "local_root_failover.rs"]
mod local_root_failover;
#[path = "local_root_publication.rs"]
mod local_root_publication;
#[path = "local_root_publication_evidence.rs"]
mod local_root_publication_evidence;
#[path = "local_root_publication_recovery.rs"]
mod local_root_publication_recovery;
#[cfg(any(test, feature = "root-publication-test-control"))]
#[path = "local_root_publication_test_control.rs"]
mod local_root_publication_test_control;
#[path = "local_root_register.rs"]
mod local_root_register;
#[path = "local_roots.rs"]
mod local_roots;
#[path = "local_roots_layout.rs"]
mod local_roots_layout;
#[path = "local_shard_recovery.rs"]
mod local_shard_recovery;
#[path = "local_stream_control.rs"]
mod local_stream_control;
#[path = "local_stream_publication_recovery.rs"]
mod local_stream_publication_recovery;
#[path = "local_stream_records.rs"]
mod local_stream_records;
#[path = "local_transaction_finalise.rs"]
mod local_transaction_finalise;
#[path = "local_transaction_visibility.rs"]
mod local_transaction_visibility;
#[path = "local_transactions.rs"]
mod local_transactions;
#[path = "local_tx_helpers.rs"]
mod local_tx_helpers;
#[path = "local_tx_rows.rs"]
mod local_tx_rows;

use self::local_block_distribution::*;
use self::local_boundaries::*;
use self::local_codec::*;
use self::local_coremeta_history::*;
use self::local_coremeta_quorum::*;
pub use self::local_coremeta_recovery::CoreMetaRecoverySnapshot;
use self::local_erasure::*;
use self::local_io::*;
use self::local_key_helpers::*;
use self::local_logical_files::*;
pub(crate) use self::local_root_publication::CoreMetaRootPublication;
use self::local_root_publication::PreparedRootPublication;
pub(in crate::core_store) use self::local_root_publication::validate_transaction_manifest_body_row;
pub(in crate::core_store) use self::local_root_publication_recovery::validate_root_publication_intent_row;
use self::local_root_publication_recovery::{
    RootPublicationIntent, RootPublicationIntentRoot, build_root_publication_intent,
    publication_intent_local_rows_match, publication_intent_retry_matches,
    root_publication_plan_hash,
};
use self::local_root_register::*;
pub(crate) use self::local_root_register::{
    root_prepare_receipt_payload_hash, root_register_cohort_hash,
};
pub(crate) use self::local_roots::encode_root_anchor_record;
use self::local_roots::*;
pub(crate) use self::local_shard_recovery::{ShardInventoryState, shard_inventory_response};
use self::local_stream_records::*;
use self::local_tx_helpers::*;
use self::local_tx_rows::*;

#[cfg(test)]
#[path = "local_tests/mod.rs"]
mod local_tests;
