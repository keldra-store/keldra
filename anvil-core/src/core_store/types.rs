use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::manifest_proto::encode_logical_file_manifest_proto;
use super::transaction_manifest_proto::encode_manifest_locator_proto;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const CORE_OBJECT_MANIFEST_SCHEMA: &str = "anvil.core.object_manifest.v1";
pub const CORE_LOGICAL_FILE_MANIFEST_SCHEMA: &str = "anvil.core.logical_file_manifest.v1";
pub const CORE_LOGICAL_FILE_INLINE_REF_PREFIX: &str = "core-logical-file-inline:";
pub const CORE_LOGICAL_FILE_LOCATOR_REF_PREFIX: &str = "core-logical-file-locator:";
pub const CORE_TRANSACTION_SCHEMA: &str = "anvil.core.transaction.v1";
pub const CORE_WATCH_EVENT_SCHEMA: &str = "anvil.core.watch_event.v1";
pub const CORE_FENCE_SCHEMA: &str = "anvil.core.fence.v1";
pub const CORE_ROOT_CATALOG_SCHEMA: &str = "anvil.core.root_catalog.v1";
pub const CORE_QUORUM_PROFILE_SCHEMA: &str = "anvil.core.quorum_profile.v1";
pub const CORE_BOUNDARY_SCHEMA_SCHEMA: &str = "anvil.core.boundary_schema.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PutBlob {
    pub logical_name: String,
    pub bytes: Vec<u8>,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub region_id: String,
    pub mutation_id: String,
}

#[cfg(test)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CorePayloadReferenceSummary {
    pub payload_identity: String,
    pub storage_kind: String,
    pub reference_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetBlob {
    pub object_ref: CoreObjectRef,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreByteRange {
    pub start: u64,
    pub end_exclusive: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetBlobRange {
    pub object_ref: CoreObjectRef,
    pub range: CoreByteRange,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteLogicalFileRequest {
    pub writer_family: String,
    pub generation: u64,
    pub logical_file_id: String,
    pub source: Vec<u8>,
    pub range_hints: Vec<CoreLogicalRangeHint>,
    pub pipeline_policy: CorePipelinePolicy,
    pub trace_context: CoreTraceContext,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub mutation_id: String,
    pub region_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteLogicalFilePathRequest {
    pub writer_family: String,
    pub generation: u64,
    pub logical_file_id: String,
    pub source_path: PathBuf,
    pub source_len: u64,
    pub source_hash: String,
    pub range_hints: Vec<CoreLogicalRangeHint>,
    pub pipeline_policy: CorePipelinePolicy,
    pub trace_context: CoreTraceContext,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub mutation_id: String,
    pub region_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadLogicalRangeRequest {
    pub manifest: CoreLogicalFileManifest,
    pub ranges: Vec<CoreByteRange>,
    pub authz_scope: AuthzScopeRef,
    pub expected_boundary: Option<Vec<CoreBoundaryValue>>,
    pub prefetch_policy: CorePrefetchPolicy,
    pub trace_context: CoreTraceContext,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreLogicalRangeHint {
    pub range_id: String,
    pub byte_start: u64,
    pub byte_end: u64,
    pub writer_record_kind: String,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub writer_statistics: Vec<u8>,
    pub preferred_block_boundary: String,
    pub boundary_dimension_ids: Vec<u32>,
    pub prefetch_next_range_ids: Vec<String>,
    pub shared_range: Option<CoreSharedRangeMarker>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreSharedRangeMarker {
    pub record_kind: String,
    pub reason: String,
    pub boundary_dimension_ids: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CorePipelinePolicy {
    pub compression: String,
    pub encryption: String,
    pub erasure_profile_id: String,
    pub placement_scope: String,
    pub target_block_size: u64,
    pub boundary_mode: String,
}

impl Default for CorePipelinePolicy {
    fn default() -> Self {
        Self {
            compression: "zstd".to_string(),
            encryption: "none".to_string(),
            erasure_profile_id: "ec-4-2".to_string(),
            placement_scope: "region".to_string(),
            target_block_size: 64 * 1024 * 1024,
            boundary_mode: "honour".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CoreTraceContext {
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CorePrefetchPolicy {
    pub enabled: bool,
    pub max_ranges: u32,
}

impl Default for CorePrefetchPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_ranges: 4,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreLogicalFileManifest {
    pub schema: String,
    pub logical_file_id: String,
    pub writer_family: String,
    pub writer_generation: u64,
    pub logical_size: u64,
    pub content_hash: String,
    pub boundary_schema_generation: u64,
    pub ranges: Vec<CoreLogicalRange>,
    pub blocks: Vec<CoreLogicalBlockRef>,
    pub compression: CoreCompressionDescriptor,
    pub encryption: CoreEncryptionDescriptor,
    pub erasure_profile_id: String,
    pub placement_epoch: u64,
    pub created_by_mutation_id: String,
    pub codec_id: String,
    pub data_shards: u32,
    pub parity_shards: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreLogicalFileWrite {
    pub manifest: CoreLogicalFileManifest,
    pub locator: CoreManifestLocator,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreManifestRef {
    pub logical_file_id: String,
    pub writer_family: String,
    pub writer_generation: u64,
    pub manifest_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreManifestLocator {
    pub manifest_ref: CoreManifestRef,
    pub manifest_encoding: String,
    pub manifest_length: u64,
    pub manifest_hash: String,
    pub block_locators: Vec<CoreBlockLocator>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreBlockLocator {
    pub logical_start: u64,
    pub logical_end: u64,
    pub block_id: String,
    pub codec_id: String,
    pub data_shards: u32,
    pub parity_shards: u32,
    pub plaintext_block_len: u64,
    pub shard_payload_len: u64,
    pub padding_len: u64,
    pub block_plain_hash: String,
    pub block_encoded_hash: String,
    pub compression: CoreCompressionDescriptor,
    pub encryption: CoreEncryptionDescriptor,
    pub erasure_profile_id: String,
    pub placement_epoch: u64,
    pub boundary_summary_hash: String,
    pub boundary_values_b64: String,
    pub shard_receipts: Vec<CoreShardReceiptSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreShardReceiptSummary {
    pub node_id: String,
    pub region_id: String,
    pub cell_id: String,
    pub shard_index: u32,
    pub shard_hash: String,
    pub shard_length: u64,
    pub fsync_sequence: u64,
    pub written_at_unix_nanos: u64,
    pub signed_payload_hash: String,
    pub signature_algorithm: String,
    pub receipt_signature: Vec<u8>,
    pub boundary_summary_hash: String,
    pub boundary_values_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreLogicalRange {
    pub range_id: String,
    pub byte_start: u64,
    pub byte_end: u64,
    pub writer_record_kind: String,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub writer_statistics: Vec<u8>,
    pub block_ids: Vec<String>,
    pub prefetch_next_range_ids: Vec<String>,
    pub preferred_block_boundary: String,
    pub boundary_dimension_ids: Vec<u32>,
    pub shared_range: Option<CoreSharedRangeMarker>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreLogicalBlockRef {
    pub block_id: String,
    pub logical_offset: u64,
    pub logical_length: u64,
    pub compressed_length: u64,
    pub encrypted_length: u64,
    pub content_hash: String,
    pub compression: CoreCompressionDescriptor,
    pub encryption: CoreEncryptionDescriptor,
    pub erasure_set_id: String,
    pub shards: Vec<CoreLogicalShardRef>,
    pub codec_id: String,
    pub data_shards: u32,
    pub parity_shards: u32,
    pub plaintext_block_len: u64,
    pub shard_payload_len: u64,
    pub padding_len: u64,
    pub block_encoded_hash: String,
    pub boundary_summary_hash: String,
    pub boundary_values_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreLogicalShardRef {
    pub node_id: String,
    pub region_id: String,
    pub cell_id: String,
    pub shard_index: u32,
    pub shard_hash: String,
    pub stored_length: u64,
    pub generation: u64,
    pub placement_epoch: u64,
    pub fsync_sequence: u64,
    pub written_at_unix_nanos: u64,
    pub signed_payload_hash: String,
    pub signature_algorithm: String,
    pub receipt_signature: Vec<u8>,
    pub boundary_summary_hash: String,
    pub boundary_values_b64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreCompressionDescriptor {
    pub algorithm: String,
    pub level: u32,
    pub uncompressed_length: u64,
    pub compressed_length: u64,
    pub dictionary_id: String,
    pub descriptor_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreEncryptionDescriptor {
    pub algorithm: String,
    pub key_id: String,
    pub nonce: Vec<u8>,
    pub aad_hash: String,
    pub plaintext_hash: String,
    pub ciphertext_hash: String,
    pub descriptor_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreLogicalFileVerificationReport {
    pub verified: bool,
    pub logical_file_id: String,
    pub checked_blocks: u64,
    pub checked_shards: u64,
    pub content_hash: String,
}

pub fn core_object_ref_from_logical_file_manifest(
    manifest: &CoreLogicalFileManifest,
) -> CoreObjectRef {
    let manifest_bytes = encode_logical_file_manifest_proto(manifest)
        .expect("CoreLogicalFileManifest produced by CoreStore should encode deterministically");
    let inline_manifest_ref = format!(
        "{}{}",
        CORE_LOGICAL_FILE_INLINE_REF_PREFIX,
        URL_SAFE_NO_PAD.encode(manifest_bytes)
    );
    core_object_ref_from_logical_file_ref(manifest, inline_manifest_ref)
}

pub fn core_object_ref_from_logical_file_write(write: &CoreLogicalFileWrite) -> CoreObjectRef {
    let locator_bytes = encode_manifest_locator_proto(&write.locator)
        .expect("CoreManifestLocator produced by CoreStore should encode deterministically");
    let locator_ref = format!(
        "{}{}",
        CORE_LOGICAL_FILE_LOCATOR_REF_PREFIX,
        URL_SAFE_NO_PAD.encode(locator_bytes)
    );
    core_object_ref_from_logical_file_ref(&write.manifest, locator_ref)
}

fn core_object_ref_from_logical_file_ref(
    manifest: &CoreLogicalFileManifest,
    manifest_ref: String,
) -> CoreObjectRef {
    CoreObjectRef {
        hash: manifest.content_hash.clone(),
        logical_size: manifest.logical_size,
        manifest_ref,
        encoding: CoreObjectEncoding {
            block_id: manifest.logical_file_id.clone(),
            profile_id: manifest.erasure_profile_id.clone(),
            data_shards: manifest.data_shards as u16,
            parity_shards: manifest.parity_shards as u16,
            minimum_read_shards: manifest.data_shards as u16,
            minimum_write_ack_shards: (manifest.data_shards + manifest.parity_shards) as u16,
            stripe_size: manifest
                .blocks
                .iter()
                .map(|block| {
                    block
                        .shard_payload_len
                        .saturating_mul(manifest.data_shards as u64)
                })
                .max()
                .unwrap_or(manifest.logical_size),
            placement_scope: "region".to_string(),
            repair_priority: "normal".to_string(),
            stored_hash: manifest.content_hash.clone(),
            compression: manifest.compression.clone(),
            encryption: manifest.encryption.algorithm.clone(),
        },
        placements: manifest
            .blocks
            .iter()
            .flat_map(|block| {
                block.shards.iter().map(|shard| CoreObjectPlacement {
                    shard_index: shard.shard_index as u16,
                    node_id: shard.node_id.clone(),
                    region_id: shard.region_id.clone(),
                    cell_id: shard.cell_id.clone(),
                    shard_hash: shard.shard_hash.clone(),
                    stored_size: shard.stored_length,
                    generation: shard.generation,
                    placement_epoch: shard.placement_epoch,
                    fsync_sequence: shard.fsync_sequence,
                    written_at_unix_nanos: shard.written_at_unix_nanos,
                    signed_payload_hash: shard.signed_payload_hash.clone(),
                    signature_algorithm: shard.signature_algorithm.clone(),
                    receipt_signature: shard.receipt_signature.clone(),
                })
            })
            .collect(),
    }
}

fn core_descriptor_hash(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("sha256:{:x}", hasher.finalize())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreObjectRef {
    pub hash: String,
    pub logical_size: u64,
    pub manifest_ref: String,
    pub encoding: CoreObjectEncoding,
    pub placements: Vec<CoreObjectPlacement>,
}

#[cfg(test)]
impl CoreObjectRef {
    pub fn test_unlocated(hash: String, logical_size: u64, manifest_ref: String) -> Self {
        Self {
            hash: hash.clone(),
            logical_size,
            manifest_ref,
            encoding: CoreObjectEncoding {
                block_id: String::new(),
                profile_id: "ec-4-2".to_string(),
                data_shards: 4,
                parity_shards: 2,
                minimum_read_shards: 4,
                minimum_write_ack_shards: 6,
                stripe_size: logical_size,
                placement_scope: "region".to_string(),
                repair_priority: "normal".to_string(),
                stored_hash: hash.clone(),
                compression: CoreCompressionDescriptor {
                    algorithm: "none".to_string(),
                    level: 0,
                    uncompressed_length: logical_size,
                    compressed_length: logical_size,
                    dictionary_id: String::new(),
                    descriptor_hash: String::new(),
                },
                encryption: "none".to_string(),
            },
            placements: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreObjectManifest {
    pub schema: String,
    pub mesh_id: String,
    pub region_id: String,
    pub object_hash: String,
    pub logical_size: u64,
    pub logical_file_id: String,
    pub logical_offset: u64,
    pub writer_family: String,
    pub encryption_algorithm: String,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub encoding: CoreObjectEncoding,
    pub placements: Vec<CoreObjectPlacement>,
    pub created_at: String,
    pub mutation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreObjectEncoding {
    pub block_id: String,
    pub profile_id: String,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub minimum_read_shards: u16,
    pub minimum_write_ack_shards: u16,
    pub stripe_size: u64,
    pub placement_scope: String,
    pub repair_priority: String,
    pub stored_hash: String,
    pub compression: CoreCompressionDescriptor,
    pub encryption: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreObjectPlacement {
    pub shard_index: u16,
    pub node_id: String,
    pub region_id: String,
    pub cell_id: String,
    pub shard_hash: String,
    pub stored_size: u64,
    pub generation: u64,
    pub placement_epoch: u64,
    pub fsync_sequence: u64,
    pub written_at_unix_nanos: u64,
    pub signed_payload_hash: String,
    pub signature_algorithm: String,
    pub receipt_signature: Vec<u8>,
}

pub fn boundary_schema_bucket_key(anvil_storage_tenant_id: i64, bucket_name: &str) -> String {
    format!("tenant:{anvil_storage_tenant_id}/bucket:{bucket_name}")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreBoundarySchema {
    pub schema: String,
    pub bucket: String,
    pub generation: u64,
    pub dimensions: Vec<CoreBoundaryDimension>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreBoundaryDimension {
    pub name: String,
    pub source: CoreBoundarySource,
    pub value_type: String,
    pub categories: Vec<String>,
    pub required: bool,
    pub cardinality: String,
    pub max_values_per_block: u32,
    pub placement_affinity: String,
    pub compaction_scope: String,
    pub shared_ranges_allowed: bool,
    pub shared_record_kinds: Vec<String>,
    pub deprecated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct CoreBoundaryValue {
    pub schema_generation: u64,
    pub name: String,
    pub value_type: String,
    pub value: String,
    pub categories: Vec<String>,
    pub source_kind: String,
    pub required: bool,
    pub max_values_per_block: u32,
    pub placement_affinity: String,
    pub compaction_scope: String,
    pub shared_ranges_allowed: bool,
    pub shared_record_kinds: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoreBoundarySource {
    UserMetadataJsonPointer {
        pointer: String,
    },
    SystemMetadataField {
        field: String,
    },
    PathTemplate {
        template: String,
    },
    BodyJsonPointer {
        pointer: String,
        max_body_bytes: u64,
    },
    WriterSuppliedBoundary {
        writer_family: String,
        field: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PutBoundarySchema {
    pub schema: CoreBoundarySchema,
    pub expected_generation: Option<u64>,
    pub mutation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoundarySchemaReceipt {
    pub bucket: String,
    pub generation: u64,
    pub row_generation: u64,
    pub schema_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendStreamRecord {
    pub stream_id: String,
    pub partition_id: String,
    pub record_kind: String,
    pub payload: Vec<u8>,
    pub content_type: Option<String>,
    pub user_metadata_json: String,
    pub fence: Option<CoreFencePrecondition>,
    pub transaction_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadStream {
    pub stream_id: String,
    pub after_sequence: u64,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamAppendReceipt {
    pub stream_id: String,
    pub sequence: u64,
    pub cursor: String,
    pub event_hash: String,
    pub idempotent_replay: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamRecord {
    pub schema: String,
    pub stream_id: String,
    pub partition_id: String,
    pub sequence: u64,
    pub cursor: String,
    pub previous_event_hash: String,
    pub event_hash: String,
    pub record_kind: String,
    pub payload_hash: String,
    pub payload: Vec<u8>,
    pub content_type: Option<String>,
    pub user_metadata_json: String,
    pub authenticated_principal: String,
    pub transaction_id: Option<String>,
    pub idempotency_key_hash: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoreStreamRecordMetadata {
    pub sequence: u64,
    pub event_hash: String,
    pub record_kind: String,
    pub payload_len: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealStreamSegment {
    pub stream_id: String,
    pub partition_id: String,
    pub through_sequence: Option<u64>,
    pub segment_kind: String,
    pub mutation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreSegmentRef {
    pub stream_id: String,
    pub partition_id: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub record_count: u64,
    pub segment_kind: String,
    pub object_ref: CoreObjectRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WatchRequest {
    pub stream_prefix: String,
    pub after_cursor: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WatchEvent {
    pub stream_id: String,
    pub sequence: u64,
    pub cursor: String,
    pub previous_event_hash: String,
    pub event_hash: String,
    pub event_type: String,
    pub record_kind: String,
    pub payload_hash: String,
    pub transaction_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WatchPage {
    pub events: Vec<WatchEvent>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadStreamPage {
    pub records: Vec<StreamRecord>,
    pub next_sequence: u64,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreHistoryPage<T> {
    pub records: Vec<T>,
    pub next_sequence: u64,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AcquireFence {
    pub fence_name: String,
    pub authenticated_principal: String,
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReleaseFence {
    pub fence_name: String,
    pub authenticated_principal: String,
    pub fence_token: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FencedPermit {
    pub fence_name: String,
    pub owner_principal: String,
    pub fence_token: u64,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreFenceRecord {
    pub schema: String,
    pub fence_name: String,
    pub owner_principal: String,
    pub fence_token: u64,
    pub expires_at_ms: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreRootCatalog {
    pub schema: String,
    pub mesh_id: String,
    pub generation: u64,
    pub previous_hash: String,
    pub root_partitions: Vec<CoreRootPartition>,
    pub placement_catalog_ref: String,
    pub stream_directory_ref: String,
    pub authz_system_realm_ref: String,
    pub created_at: String,
    pub signed_by: String,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreRootPartition {
    pub partition_id: String,
    pub owner_node_id: String,
    pub fence: u64,
    pub placement_group: String,
    pub embedded_head_segment_manifest: CoreObjectManifest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreRootCatalogReceipt {
    pub mesh_id: String,
    pub generation: u64,
    pub catalog_hash: String,
    pub row_generation: u64,
    pub watch_cursor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreQuorumProfile {
    pub schema: String,
    pub placement_group: String,
    pub replica_count: u16,
    pub write_quorum: u16,
    pub read_quorum: u16,
    pub fence_quorum: u16,
    pub epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreQuorumProfileReceipt {
    pub placement_group: String,
    pub epoch: u64,
    pub profile_hash: String,
    pub row_generation: u64,
    pub watch_cursor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreFencePrecondition {
    pub fence_name: String,
    pub fence_token: u64,
    pub authenticated_principal: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreTransaction {
    pub schema: String,
    pub transaction_id: String,
    pub scope_partition: String,
    pub state: CoreTransactionState,
    pub preconditions_hash: String,
    pub operations_hash: String,
    pub writer_families: Vec<String>,
    pub visible_updates: Vec<CoreTransactionUpdate>,
    pub finalisation_error: Option<String>,
    pub committed_at: String,
    pub committed_by_principal: String,
    pub created_at_unix_nanos: u64,
    pub expires_at_unix_nanos: u64,
    pub root_anchor_key: String,
    pub root_key_hash: String,
    pub committed_root_generation: Option<u64>,
    pub purpose: String,
    pub failure_evidence: Option<String>,
    pub outcome: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoreTransactionState {
    Open,
    Prepared,
    Committed,
    FinalisationFailed,
    Aborted,
    RolledBack,
    Expired,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoreTransactionUpdate {
    StreamAppend {
        partition_id: String,
        stream_id: String,
        record_kind: String,
        payload: Vec<u8>,
        idempotency_key_hash: Option<String>,
        visible_sequence: u64,
        previous_event_hash: String,
        prepared_record_hash: String,
        created_at: String,
    },
    CoreMetaPut {
        cf: String,
        table_id: u16,
        tuple_key: Vec<u8>,
        previous_payload_hash: Option<String>,
        payload: Vec<u8>,
        payload_hash: String,
    },
    CoreMetaDelete {
        cf: String,
        table_id: u16,
        tuple_key: Vec<u8>,
        previous_payload_hash: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreMutationBatch {
    pub transaction_id: String,
    pub scope_partition: String,
    pub committed_by_principal: String,
    pub root_publications: Vec<CoreMutationRootPublication>,
    pub preconditions: Vec<CoreMutationPrecondition>,
    pub operations: Vec<CoreMutationOperation>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreMutationBatchAdditions {
    pub root_publications: Vec<CoreMutationRootPublication>,
    pub preconditions: Vec<CoreMutationPrecondition>,
    pub operations: Vec<CoreMutationOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreMutationRootPublication {
    pub root_anchor_key: String,
    pub writer_families: Vec<String>,
    pub transaction_coordinator: bool,
}

impl CoreMutationRootPublication {
    pub fn new(root_anchor_key: impl Into<String>, writer_family: impl Into<String>) -> Self {
        Self::with_writer_families(root_anchor_key, vec![writer_family.into()])
    }

    pub fn with_writer_families(
        root_anchor_key: impl Into<String>,
        writer_families: Vec<String>,
    ) -> Self {
        Self {
            root_anchor_key: root_anchor_key.into(),
            writer_families,
            transaction_coordinator: false,
        }
    }

    pub fn coordinator(mut self) -> Self {
        self.transaction_coordinator = true;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoreMutationPrecondition {
    Fence {
        fence_name: String,
        fence_token: u64,
    },
    CoreMetaRow {
        cf: String,
        table_id: u16,
        tuple_key: Vec<u8>,
        expected_payload_hash: Option<String>,
        require_absent: bool,
        require_present: bool,
    },
    /// Exact row-version fence which is valid only before a wall-clock deadline.
    CoreMetaLease {
        cf: String,
        table_id: u16,
        tuple_key: Vec<u8>,
        expected_payload_hash: String,
        expires_at_unix_nanos: u64,
    },
    StreamHead {
        stream_id: String,
        expected_last_sequence: u64,
        expected_last_event_hash: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoreMutationOperation {
    StreamAppend {
        partition_id: String,
        stream_id: String,
        record_kind: String,
        payload: Vec<u8>,
        idempotency_key: Option<String>,
    },
    CoreMetaPut {
        partition_id: String,
        cf: String,
        table_id: u16,
        tuple_key: Vec<u8>,
        payload: Vec<u8>,
    },
    CoreMetaDelete {
        partition_id: String,
        cf: String,
        table_id: u16,
        tuple_key: Vec<u8>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreMutationBatchReceipt {
    pub transaction_id: String,
    pub scope_partition: String,
    pub state: CoreTransactionState,
    pub visible_updates: Vec<CoreTransactionUpdate>,
    pub finalisation_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreBeginTransaction {
    pub idempotency_key: String,
    pub root_anchor_key: String,
    pub root_key_hash: String,
    pub scope_partition: String,
    pub ttl_ms: u64,
    pub purpose: String,
    pub principal: String,
    pub preconditions_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceId {
    pub schema: String,
    pub mesh_id: String,
    pub anvil_storage_tenant_id: String,
    pub authz_scope: AuthzScopeRef,
    pub kind: SourceKind,
    pub resource_namespace: String,
    pub resource_id: String,
    pub generation: u64,
    pub tombstone: bool,
    pub variant: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzScopeRef {
    pub anvil_storage_tenant_id: String,
    pub authz_realm_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    ObjectCurrent,
    ObjectVersion,
    AppendRecord,
    AuthzResource,
    PackageRepository,
    PackageVersion,
    PackageFile,
    PackageTag,
    GitObject,
    PersonalDatabaseRecord,
    MeshControlRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreInternalPutShard {
    pub logical_file_id: String,
    pub logical_offset: u64,
    pub block_id: String,
    pub shard_index: u16,
    pub erasure_profile_id: String,
    pub placement_epoch: u64,
    pub shard_bytes: Vec<u8>,
    pub shard_hash: String,
    pub boundary_summary_hash: String,
    pub boundary_values_b64: String,
    pub compression_algorithm: String,
    pub encryption_algorithm: String,
    pub writer_family: String,
    pub mutation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreInternalGetShard {
    pub block_id: String,
    pub shard_index: u16,
    pub erasure_profile_id: String,
    pub placement_epoch: u64,
    pub shard_hash: String,
    pub boundary_summary_hash: Option<String>,
    pub range: Option<CoreByteRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreInternalShardReceipt {
    pub node_id: String,
    pub region_id: String,
    pub cell_id: String,
    pub block_id: String,
    pub shard_index: u16,
    pub shard_hash: String,
    pub shard_length: u64,
    pub fsync_sequence: u64,
    pub written_at_unix_nanos: u64,
    pub signed_payload_hash: String,
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreInternalRootAnchorRead {
    pub root_key_hash: String,
    pub generation: u64,
    pub root_anchor_record: Vec<u8>,
    pub root_anchor_hash: String,
}
