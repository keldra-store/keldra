use anyhow::{Result, anyhow, bail};
use prost::{Message, Oneof};

use super::super::{CoreStoredStreamHead, StoredStreamIdempotencyRow, StoredStreamRecordIndexRow};
use crate::core_store::types::{
    CoreBlockLocator, CoreBoundaryDimension, CoreBoundarySchema, CoreBoundarySource,
    CoreBoundaryValue, CoreCompressionDescriptor, CoreEncryptionDescriptor, CoreFenceRecord,
    CoreManifestLocator, CoreManifestRef, CoreObjectEncoding, CoreObjectManifest,
    CoreObjectPlacement, CoreQuorumProfile, CoreRootCatalog, CoreRootPartition,
    CoreShardReceiptSummary,
};
use crate::core_store::{
    CoreMetaRowCommonProto, core_meta_committed_row_common, core_meta_root_key_hash,
};

#[derive(Clone, PartialEq, Message)]
struct BoundarySchemaProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    bucket: String,
    #[prost(uint64, tag = "4")]
    generation: u64,
    #[prost(message, repeated, tag = "5")]
    dimensions: Vec<BoundaryDimensionProto>,
    #[prost(string, tag = "6")]
    created_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct BoundaryDimensionProto {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(message, optional, tag = "2")]
    source: Option<BoundarySourceProto>,
    #[prost(string, tag = "3")]
    value_type: String,
    #[prost(string, repeated, tag = "4")]
    categories: Vec<String>,
    #[prost(bool, tag = "5")]
    required: bool,
    #[prost(string, tag = "6")]
    cardinality: String,
    #[prost(uint32, tag = "7")]
    max_values_per_block: u32,
    #[prost(string, tag = "8")]
    placement_affinity: String,
    #[prost(string, tag = "9")]
    compaction_scope: String,
    #[prost(bool, tag = "10")]
    shared_ranges_allowed: bool,
    #[prost(string, repeated, tag = "11")]
    shared_record_kinds: Vec<String>,
    #[prost(bool, tag = "12")]
    deprecated: bool,
}

#[derive(Clone, PartialEq, Message)]
struct BoundarySourceProto {
    #[prost(oneof = "boundary_source_proto::Kind", tags = "1, 2, 3, 4, 5")]
    kind: Option<boundary_source_proto::Kind>,
}

mod boundary_source_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Kind {
        #[prost(string, tag = "1")]
        UserMetadataJsonPointer(String),
        #[prost(string, tag = "2")]
        PathTemplate(String),
        #[prost(message, tag = "3")]
        BodyJsonPointer(super::BodyJsonPointerProto),
        #[prost(string, tag = "4")]
        SystemMetadataField(String),
        #[prost(message, tag = "5")]
        WriterSuppliedBoundary(super::WriterSuppliedBoundaryProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct BodyJsonPointerProto {
    #[prost(string, tag = "1")]
    pointer: String,
    #[prost(uint64, tag = "2")]
    max_body_bytes: u64,
}

#[derive(Clone, PartialEq, Message)]
struct WriterSuppliedBoundaryProto {
    #[prost(string, tag = "1")]
    writer_family: String,
    #[prost(string, tag = "2")]
    field: String,
}

#[derive(Clone, PartialEq, Message)]
struct StreamRecordIndexRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    stream_id: String,
    #[prost(string, tag = "4")]
    partition_id: String,
    #[prost(uint64, tag = "5")]
    sequence: u64,
    #[prost(string, tag = "6")]
    cursor: String,
    #[prost(string, tag = "7")]
    previous_event_hash: String,
    #[prost(string, tag = "8")]
    event_hash: String,
    #[prost(string, tag = "9")]
    record_kind: String,
    #[prost(string, tag = "10")]
    payload_hash: String,
    #[prost(uint64, tag = "11")]
    payload_len: u64,
    #[prost(bytes, optional, tag = "12")]
    inline_payload: Option<Vec<u8>>,
    #[prost(message, optional, tag = "13")]
    payload_locator: Option<CoreManifestLocatorProto>,
    #[prost(string, optional, tag = "14")]
    transaction_id: Option<String>,
    #[prost(string, optional, tag = "15")]
    idempotency_key_hash: Option<String>,
    #[prost(string, tag = "16")]
    created_at: String,
    #[prost(string, optional, tag = "17")]
    content_type: Option<String>,
    #[prost(string, tag = "18")]
    user_metadata_json: String,
    #[prost(string, tag = "19")]
    authenticated_principal: String,
}

#[derive(Clone, PartialEq, Message)]
struct StreamIdempotencyRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    stream_id: String,
    #[prost(uint64, tag = "4")]
    sequence: u64,
    #[prost(string, tag = "5")]
    cursor: String,
    #[prost(string, tag = "6")]
    event_hash: String,
    #[prost(string, tag = "7")]
    record_kind: String,
    #[prost(string, tag = "8")]
    payload_hash: String,
    #[prost(string, optional, tag = "9")]
    transaction_id: Option<String>,
    #[prost(string, tag = "10")]
    idempotency_key_hash: String,
    #[prost(string, tag = "11")]
    created_at: String,
    #[prost(string, tag = "12")]
    authenticated_principal: String,
}

#[derive(Clone, PartialEq, Message)]
struct StreamHeadProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    stream_id: String,
    #[prost(uint64, tag = "4")]
    last_sequence: u64,
    #[prost(string, tag = "5")]
    last_event_hash: String,
    #[prost(uint64, tag = "6")]
    record_count: u64,
    #[prost(string, tag = "7")]
    updated_at: String,
    #[prost(bool, tag = "8")]
    idempotency_index_complete: bool,
}

#[derive(Clone, PartialEq, Message)]
struct RootCatalogProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    mesh_id: String,
    #[prost(uint64, tag = "3")]
    generation: u64,
    #[prost(string, tag = "4")]
    previous_hash: String,
    #[prost(message, repeated, tag = "5")]
    root_partitions: Vec<RootPartitionProto>,
    #[prost(string, tag = "6")]
    placement_catalog_ref: String,
    #[prost(string, tag = "7")]
    stream_directory_ref: String,
    #[prost(string, tag = "9")]
    authz_system_realm_ref: String,
    #[prost(string, tag = "10")]
    created_at: String,
    #[prost(string, tag = "11")]
    signed_by: String,
    #[prost(string, tag = "12")]
    signature: String,
}

#[derive(Clone, PartialEq, Message)]
struct RootPartitionProto {
    #[prost(string, tag = "1")]
    partition_id: String,
    #[prost(string, tag = "2")]
    owner_node_id: String,
    #[prost(uint64, tag = "3")]
    fence: u64,
    #[prost(string, tag = "4")]
    placement_group: String,
    #[prost(message, optional, tag = "5")]
    embedded_head_segment_manifest: Option<CoreObjectManifestProto>,
}

#[derive(Clone, PartialEq, Message)]
struct QuorumProfileProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    placement_group: String,
    #[prost(uint32, tag = "3")]
    replica_count: u32,
    #[prost(uint32, tag = "4")]
    write_quorum: u32,
    #[prost(uint32, tag = "5")]
    read_quorum: u32,
    #[prost(uint32, tag = "6")]
    fence_quorum: u32,
    #[prost(uint64, tag = "7")]
    epoch: u64,
}

#[derive(Clone, PartialEq, Message)]
struct TransactionStreamCheckpointProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    stream_id: String,
    #[prost(uint64, tag = "3")]
    record_count: u64,
    #[prost(uint64, tag = "4")]
    last_sequence: u64,
    #[prost(string, tag = "5")]
    last_event_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreFenceRecordProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    fence_name: String,
    #[prost(string, tag = "3")]
    owner_principal: String,
    #[prost(uint64, tag = "4")]
    fence_token: u64,
    #[prost(int64, tag = "5")]
    expires_at_ms: i64,
    #[prost(string, tag = "6")]
    updated_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectManifestProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    mesh_id: String,
    #[prost(string, tag = "4")]
    region_id: String,
    #[prost(string, tag = "5")]
    object_hash: String,
    #[prost(uint64, tag = "6")]
    logical_size: u64,
    #[prost(message, repeated, tag = "7")]
    boundary_values: Vec<CoreBoundaryValueProto>,
    #[prost(message, optional, tag = "8")]
    encoding: Option<CoreObjectEncodingProto>,
    #[prost(message, repeated, tag = "9")]
    placements: Vec<CoreObjectPlacementProto>,
    #[prost(string, tag = "10")]
    created_at: String,
    #[prost(string, tag = "11")]
    mutation_id: String,
    #[prost(string, tag = "12")]
    logical_file_id: String,
    #[prost(uint64, tag = "13")]
    logical_offset: u64,
    #[prost(string, tag = "14")]
    writer_family: String,
    #[prost(string, tag = "15")]
    encryption_algorithm: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectEncodingProto {
    #[prost(string, tag = "1")]
    block_id: String,
    #[prost(string, tag = "2")]
    profile_id: String,
    #[prost(uint32, tag = "3")]
    data_shards: u32,
    #[prost(uint32, tag = "4")]
    parity_shards: u32,
    #[prost(uint32, tag = "5")]
    minimum_read_shards: u32,
    #[prost(uint32, tag = "6")]
    minimum_write_ack_shards: u32,
    #[prost(uint64, tag = "7")]
    stripe_size: u64,
    #[prost(string, tag = "8")]
    placement_scope: String,
    #[prost(string, tag = "9")]
    repair_priority: String,
    #[prost(string, tag = "10")]
    encryption: String,
    #[prost(string, tag = "11")]
    stored_hash: String,
    #[prost(message, optional, tag = "12")]
    compression: Option<CoreCompressionDescriptorProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectPlacementProto {
    #[prost(uint32, tag = "1")]
    shard_index: u32,
    #[prost(string, tag = "2")]
    node_id: String,
    #[prost(string, tag = "3")]
    region_id: String,
    #[prost(string, tag = "4")]
    cell_id: String,
    #[prost(string, tag = "5")]
    shard_hash: String,
    #[prost(uint64, tag = "6")]
    stored_size: u64,
    #[prost(uint64, tag = "7")]
    generation: u64,
    #[prost(uint64, tag = "8")]
    placement_epoch: u64,
    #[prost(uint64, tag = "9")]
    fsync_sequence: u64,
    #[prost(uint64, tag = "10")]
    written_at_unix_nanos: u64,
    #[prost(string, tag = "11")]
    signed_payload_hash: String,
    #[prost(string, tag = "12")]
    signature_algorithm: String,
    #[prost(bytes, tag = "13")]
    receipt_signature: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreBoundaryValueProto {
    #[prost(uint64, tag = "1")]
    schema_generation: u64,
    #[prost(string, tag = "2")]
    name: String,
    #[prost(string, tag = "3")]
    value_type: String,
    #[prost(string, tag = "4")]
    value: String,
    #[prost(string, repeated, tag = "5")]
    categories: Vec<String>,
    #[prost(string, tag = "6")]
    source_kind: String,
    #[prost(bool, tag = "7")]
    required: bool,
    #[prost(uint32, tag = "8")]
    max_values_per_block: u32,
    #[prost(string, tag = "9")]
    compaction_scope: String,
    #[prost(bool, tag = "10")]
    shared_ranges_allowed: bool,
    #[prost(string, repeated, tag = "11")]
    shared_record_kinds: Vec<String>,
    #[prost(string, tag = "12")]
    placement_affinity: String,
}

#[derive(Clone, PartialEq, Message)]
struct BoundaryValueRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    bucket: String,
    #[prost(string, tag = "4")]
    object_ref: String,
    #[prost(string, tag = "5")]
    range_ref: String,
    #[prost(message, optional, tag = "6")]
    value: Option<CoreBoundaryValueProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreManifestLocatorProto {
    #[prost(message, optional, tag = "1")]
    manifest_ref: Option<CoreManifestRefProto>,
    #[prost(string, tag = "2")]
    manifest_encoding: String,
    #[prost(uint64, tag = "3")]
    manifest_length: u64,
    #[prost(string, tag = "4")]
    manifest_hash: String,
    #[prost(message, repeated, tag = "5")]
    block_locators: Vec<CoreBlockLocatorProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreManifestRefProto {
    #[prost(string, tag = "1")]
    logical_file_id: String,
    #[prost(string, tag = "2")]
    writer_family: String,
    #[prost(uint64, tag = "3")]
    writer_generation: u64,
    #[prost(string, tag = "4")]
    manifest_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreBlockLocatorProto {
    #[prost(uint64, tag = "1")]
    logical_start: u64,
    #[prost(uint64, tag = "2")]
    logical_end: u64,
    #[prost(string, tag = "3")]
    block_id: String,
    #[prost(string, tag = "4")]
    codec_id: String,
    #[prost(uint32, tag = "5")]
    data_shards: u32,
    #[prost(uint32, tag = "6")]
    parity_shards: u32,
    #[prost(uint64, tag = "7")]
    plaintext_block_len: u64,
    #[prost(uint64, tag = "8")]
    shard_payload_len: u64,
    #[prost(uint64, tag = "9")]
    padding_len: u64,
    #[prost(string, tag = "10")]
    block_plain_hash: String,
    #[prost(string, tag = "11")]
    block_encoded_hash: String,
    #[prost(message, optional, tag = "12")]
    compression: Option<CoreCompressionDescriptorProto>,
    #[prost(message, optional, tag = "13")]
    encryption: Option<CoreEncryptionDescriptorProto>,
    #[prost(string, tag = "14")]
    erasure_profile_id: String,
    #[prost(uint64, tag = "15")]
    placement_epoch: u64,
    #[prost(message, repeated, tag = "16")]
    shard_receipts: Vec<CoreShardReceiptSummaryProto>,
    #[prost(string, tag = "17")]
    boundary_summary_hash: String,
    #[prost(string, tag = "18")]
    boundary_values_b64: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreShardReceiptSummaryProto {
    #[prost(string, tag = "1")]
    node_id: String,
    #[prost(string, tag = "2")]
    region_id: String,
    #[prost(string, tag = "3")]
    cell_id: String,
    #[prost(uint32, tag = "4")]
    shard_index: u32,
    #[prost(string, tag = "5")]
    shard_hash: String,
    #[prost(uint64, tag = "6")]
    shard_length: u64,
    #[prost(uint64, tag = "7")]
    fsync_sequence: u64,
    #[prost(uint64, tag = "8")]
    written_at_unix_nanos: u64,
    #[prost(string, tag = "9")]
    signed_payload_hash: String,
    #[prost(string, tag = "10")]
    signature_algorithm: String,
    #[prost(bytes, tag = "11")]
    receipt_signature: Vec<u8>,
    #[prost(string, tag = "12")]
    boundary_summary_hash: String,
    #[prost(string, tag = "13")]
    boundary_values_b64: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreCompressionDescriptorProto {
    #[prost(string, tag = "1")]
    algorithm: String,
    #[prost(uint32, tag = "2")]
    level: u32,
    #[prost(uint64, tag = "3")]
    uncompressed_length: u64,
    #[prost(uint64, tag = "4")]
    compressed_length: u64,
    #[prost(string, tag = "5")]
    dictionary_id: String,
    #[prost(string, tag = "6")]
    descriptor_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreEncryptionDescriptorProto {
    #[prost(string, tag = "1")]
    algorithm: String,
    #[prost(string, tag = "2")]
    key_id: String,
    #[prost(bytes, tag = "3")]
    nonce: Vec<u8>,
    #[prost(string, tag = "4")]
    aad_hash: String,
    #[prost(string, tag = "5")]
    plaintext_hash: String,
    #[prost(string, tag = "6")]
    ciphertext_hash: String,
    #[prost(string, tag = "7")]
    descriptor_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::core_store::local) struct CoreTransactionStreamCheckpointRecord {
    pub(in crate::core_store::local) schema: String,
    pub(in crate::core_store::local) stream_id: String,
    pub(in crate::core_store::local) record_count: u64,
    pub(in crate::core_store::local) last_sequence: u64,
    pub(in crate::core_store::local) last_event_hash: String,
}

pub(in crate::core_store::local) fn encode_boundary_schema_record(
    schema: &CoreBoundarySchema,
) -> Result<Vec<u8>> {
    encode_det(&boundary_schema_to_proto(schema))
}

pub(in crate::core_store::local) fn decode_boundary_schema_record(
    bytes: &[u8],
) -> Result<CoreBoundarySchema> {
    let proto = BoundarySchemaProto::decode(bytes)?;
    ensure_det(&proto, bytes, "boundary schema")?;
    boundary_schema_from_proto(proto)
}

pub(in crate::core_store::local) fn encode_boundary_value_row(
    bucket: &str,
    object_ref: &str,
    range_ref: &str,
    value: &CoreBoundaryValue,
) -> Result<Vec<u8>> {
    encode_det(&BoundaryValueRowProto {
        common: Some(core_meta_committed_row_common(
            boundary_realm_id(bucket),
            boundary_root_key_hash(bucket),
            value.schema_generation,
            String::new(),
            0,
        )),
        schema: "anvil.core.boundary_value_row.v1".to_string(),
        bucket: bucket.to_string(),
        object_ref: object_ref.to_string(),
        range_ref: range_ref.to_string(),
        value: Some(boundary_value_to_proto(value)),
    })
}

pub(in crate::core_store::local) fn decode_boundary_value_row(
    bytes: &[u8],
) -> Result<(String, String, String, CoreBoundaryValue)> {
    let proto = BoundaryValueRowProto::decode(bytes)?;
    ensure_det(&proto, bytes, "boundary value row")?;
    if proto.schema != "anvil.core.boundary_value_row.v1" {
        bail!("CoreStore boundary value row has invalid schema");
    }
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore boundary value row missing CoreMeta common"))?;
    Ok((
        proto.bucket,
        proto.object_ref,
        proto.range_ref,
        boundary_value_from_proto(
            proto
                .value
                .ok_or_else(|| anyhow!("CoreStore boundary value row is missing value"))?,
        ),
    ))
}

pub(in crate::core_store::local) fn encode_stream_record_index_row(
    row: &StoredStreamRecordIndexRow,
    common: CoreMetaRowCommonProto,
) -> Result<Vec<u8>> {
    encode_det(&stream_record_index_row_to_proto(row, common))
}

pub(in crate::core_store::local) fn decode_stream_record_index_row(
    bytes: &[u8],
) -> Result<StoredStreamRecordIndexRow> {
    let proto = StreamRecordIndexRowProto::decode(bytes)?;
    ensure_det(&proto, bytes, "stream record index row")?;
    stream_record_index_row_from_proto(proto)
}

pub(in crate::core_store::local) fn encode_stream_idempotency_row(
    row: &StoredStreamIdempotencyRow,
    common: CoreMetaRowCommonProto,
) -> Result<Vec<u8>> {
    encode_det(&stream_idempotency_row_to_proto(row, common))
}

pub(in crate::core_store::local) fn decode_stream_idempotency_row(
    bytes: &[u8],
) -> Result<StoredStreamIdempotencyRow> {
    let proto = StreamIdempotencyRowProto::decode(bytes)?;
    ensure_det(&proto, bytes, "stream idempotency row")?;
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore stream idempotency row missing CoreMeta common"))?;
    Ok(StoredStreamIdempotencyRow {
        schema: proto.schema,
        stream_id: proto.stream_id,
        sequence: proto.sequence,
        cursor: proto.cursor,
        event_hash: proto.event_hash,
        record_kind: proto.record_kind,
        payload_hash: proto.payload_hash,
        authenticated_principal: proto.authenticated_principal,
        transaction_id: proto.transaction_id,
        idempotency_key_hash: proto.idempotency_key_hash,
        created_at: proto.created_at,
    })
}

pub(in crate::core_store::local) fn encode_stream_head_record(
    head: &CoreStoredStreamHead,
    common: CoreMetaRowCommonProto,
) -> Result<Vec<u8>> {
    encode_det(&stream_head_to_proto(head, common))
}

pub(in crate::core_store::local) fn decode_stream_head_record(
    bytes: &[u8],
) -> Result<CoreStoredStreamHead> {
    let proto = StreamHeadProto::decode(bytes)?;
    ensure_det(&proto, bytes, "stream head")?;
    Ok(CoreStoredStreamHead {
        schema: proto.schema,
        stream_id: proto.stream_id,
        last_sequence: proto.last_sequence,
        last_event_hash: proto.last_event_hash,
        record_count: proto.record_count,
        idempotency_index_complete: proto.idempotency_index_complete,
        updated_at: proto.updated_at,
    })
}

pub(in crate::core_store::local) fn encode_root_catalog_record(
    catalog: &CoreRootCatalog,
) -> Result<Vec<u8>> {
    encode_det(&root_catalog_to_proto(catalog)?)
}

pub(in crate::core_store::local) fn decode_root_catalog_record(
    bytes: &[u8],
) -> Result<CoreRootCatalog> {
    let proto = RootCatalogProto::decode(bytes)?;
    ensure_det(&proto, bytes, "root catalog")?;
    root_catalog_from_proto(proto)
}

pub(in crate::core_store::local) fn encode_quorum_profile_record(
    profile: &CoreQuorumProfile,
) -> Result<Vec<u8>> {
    encode_det(&quorum_profile_to_proto(profile))
}

pub(in crate::core_store::local) fn decode_quorum_profile_record(
    bytes: &[u8],
) -> Result<CoreQuorumProfile> {
    let proto = QuorumProfileProto::decode(bytes)?;
    ensure_det(&proto, bytes, "quorum profile")?;
    quorum_profile_from_proto(proto)
}

pub(in crate::core_store::local) fn encode_transaction_stream_checkpoint_record(
    checkpoint: &CoreTransactionStreamCheckpointRecord,
) -> Result<Vec<u8>> {
    encode_det(&TransactionStreamCheckpointProto {
        schema: checkpoint.schema.clone(),
        stream_id: checkpoint.stream_id.clone(),
        record_count: checkpoint.record_count,
        last_sequence: checkpoint.last_sequence,
        last_event_hash: checkpoint.last_event_hash.clone(),
    })
}

pub(in crate::core_store::local) fn decode_transaction_stream_checkpoint_record(
    bytes: &[u8],
) -> Result<CoreTransactionStreamCheckpointRecord> {
    let proto = TransactionStreamCheckpointProto::decode(bytes)?;
    ensure_det(&proto, bytes, "transaction stream checkpoint")?;
    Ok(CoreTransactionStreamCheckpointRecord {
        schema: proto.schema,
        stream_id: proto.stream_id,
        record_count: proto.record_count,
        last_sequence: proto.last_sequence,
        last_event_hash: proto.last_event_hash,
    })
}

pub(in crate::core_store::local) fn encode_core_fence_record(
    record: &CoreFenceRecord,
) -> Result<Vec<u8>> {
    encode_det(&CoreFenceRecordProto {
        schema: record.schema.clone(),
        fence_name: record.fence_name.clone(),
        owner_principal: record.owner_principal.clone(),
        fence_token: record.fence_token,
        expires_at_ms: record.expires_at_ms,
        updated_at: record.updated_at.clone(),
    })
}

pub(in crate::core_store::local) fn decode_core_fence_record(
    bytes: &[u8],
) -> Result<CoreFenceRecord> {
    let proto = CoreFenceRecordProto::decode(bytes)?;
    ensure_det(&proto, bytes, "fence record")?;
    Ok(CoreFenceRecord {
        schema: proto.schema,
        fence_name: proto.fence_name,
        owner_principal: proto.owner_principal,
        fence_token: proto.fence_token,
        expires_at_ms: proto.expires_at_ms,
        updated_at: proto.updated_at,
    })
}

pub(in crate::core_store::local) fn encode_object_manifest_record(
    manifest: &CoreObjectManifest,
) -> Result<Vec<u8>> {
    encode_det(&object_manifest_to_proto(manifest)?)
}

pub(in crate::core_store::local) fn decode_object_manifest_record(
    bytes: &[u8],
) -> Result<CoreObjectManifest> {
    let proto = CoreObjectManifestProto::decode(bytes)?;
    ensure_det(&proto, bytes, "object manifest")?;
    object_manifest_from_proto(proto)
}

fn encode_det<M: Message>(message: &M) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_det<M: Message>(message: &M, bytes: &[u8], label: &str) -> Result<()> {
    let mut canonical = Vec::new();
    message.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("CoreStore {label} is not deterministic protobuf");
    }
    Ok(())
}

fn boundary_realm_id(bucket: &str) -> String {
    bucket
        .split_once('/')
        .map(|(tenant, _)| format!("tenant/{tenant}"))
        .unwrap_or_else(|| "system".to_string())
}

fn boundary_root_key_hash(bucket: &str) -> String {
    core_meta_root_key_hash(&format!("boundary/{bucket}"))
}

fn boundary_schema_to_proto(value: &CoreBoundarySchema) -> BoundarySchemaProto {
    BoundarySchemaProto {
        common: Some(core_meta_committed_row_common(
            boundary_realm_id(&value.bucket),
            boundary_root_key_hash(&value.bucket),
            value.generation,
            String::new(),
            0,
        )),
        schema: value.schema.clone(),
        bucket: value.bucket.clone(),
        generation: value.generation,
        dimensions: value
            .dimensions
            .iter()
            .map(boundary_dimension_to_proto)
            .collect(),
        created_at: value.created_at.clone(),
    }
}

fn boundary_schema_from_proto(value: BoundarySchemaProto) -> Result<CoreBoundarySchema> {
    value
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore boundary schema row missing CoreMeta common"))?;
    Ok(CoreBoundarySchema {
        schema: value.schema,
        bucket: value.bucket,
        generation: value.generation,
        dimensions: value
            .dimensions
            .into_iter()
            .map(boundary_dimension_from_proto)
            .collect::<Result<Vec<_>>>()?,
        created_at: value.created_at,
    })
}

fn boundary_dimension_to_proto(value: &CoreBoundaryDimension) -> BoundaryDimensionProto {
    BoundaryDimensionProto {
        name: value.name.clone(),
        source: Some(boundary_source_to_proto(&value.source)),
        value_type: value.value_type.clone(),
        categories: value.categories.clone(),
        required: value.required,
        cardinality: value.cardinality.clone(),
        max_values_per_block: value.max_values_per_block,
        placement_affinity: value.placement_affinity.clone(),
        compaction_scope: value.compaction_scope.clone(),
        shared_ranges_allowed: value.shared_ranges_allowed,
        shared_record_kinds: value.shared_record_kinds.clone(),
        deprecated: value.deprecated,
    }
}

fn boundary_dimension_from_proto(value: BoundaryDimensionProto) -> Result<CoreBoundaryDimension> {
    Ok(CoreBoundaryDimension {
        name: value.name,
        source: boundary_source_from_proto(
            value
                .source
                .ok_or_else(|| anyhow!("CoreStore boundary dimension is missing source"))?,
        )?,
        value_type: value.value_type,
        categories: value.categories,
        required: value.required,
        cardinality: value.cardinality,
        max_values_per_block: value.max_values_per_block,
        placement_affinity: value.placement_affinity,
        compaction_scope: value.compaction_scope,
        shared_ranges_allowed: value.shared_ranges_allowed,
        shared_record_kinds: value.shared_record_kinds,
        deprecated: value.deprecated,
    })
}

fn boundary_source_to_proto(value: &CoreBoundarySource) -> BoundarySourceProto {
    let kind = match value {
        CoreBoundarySource::UserMetadataJsonPointer { pointer } => {
            boundary_source_proto::Kind::UserMetadataJsonPointer(pointer.clone())
        }
        CoreBoundarySource::SystemMetadataField { field } => {
            boundary_source_proto::Kind::SystemMetadataField(field.clone())
        }
        CoreBoundarySource::PathTemplate { template } => {
            boundary_source_proto::Kind::PathTemplate(template.clone())
        }
        CoreBoundarySource::BodyJsonPointer {
            pointer,
            max_body_bytes,
        } => boundary_source_proto::Kind::BodyJsonPointer(BodyJsonPointerProto {
            pointer: pointer.clone(),
            max_body_bytes: *max_body_bytes,
        }),
        CoreBoundarySource::WriterSuppliedBoundary {
            writer_family,
            field,
        } => boundary_source_proto::Kind::WriterSuppliedBoundary(WriterSuppliedBoundaryProto {
            writer_family: writer_family.clone(),
            field: field.clone(),
        }),
    };
    BoundarySourceProto { kind: Some(kind) }
}

fn boundary_source_from_proto(value: BoundarySourceProto) -> Result<CoreBoundarySource> {
    match value
        .kind
        .ok_or_else(|| anyhow!("CoreStore boundary source is missing kind"))?
    {
        boundary_source_proto::Kind::UserMetadataJsonPointer(pointer) => {
            Ok(CoreBoundarySource::UserMetadataJsonPointer { pointer })
        }
        boundary_source_proto::Kind::SystemMetadataField(field) => {
            Ok(CoreBoundarySource::SystemMetadataField { field })
        }
        boundary_source_proto::Kind::PathTemplate(template) => {
            Ok(CoreBoundarySource::PathTemplate { template })
        }
        boundary_source_proto::Kind::BodyJsonPointer(body) => {
            Ok(CoreBoundarySource::BodyJsonPointer {
                pointer: body.pointer,
                max_body_bytes: body.max_body_bytes,
            })
        }
        boundary_source_proto::Kind::WriterSuppliedBoundary(writer) => {
            Ok(CoreBoundarySource::WriterSuppliedBoundary {
                writer_family: writer.writer_family,
                field: writer.field,
            })
        }
    }
}

fn stream_record_index_row_to_proto(
    value: &StoredStreamRecordIndexRow,
    common: CoreMetaRowCommonProto,
) -> StreamRecordIndexRowProto {
    StreamRecordIndexRowProto {
        common: Some(common),
        schema: value.schema.clone(),
        stream_id: value.stream_id.clone(),
        partition_id: value.partition_id.clone(),
        sequence: value.sequence,
        cursor: value.cursor.clone(),
        previous_event_hash: value.previous_event_hash.clone(),
        event_hash: value.event_hash.clone(),
        record_kind: value.record_kind.clone(),
        payload_hash: value.payload_hash.clone(),
        payload_len: value.payload_len,
        inline_payload: value.inline_payload.clone(),
        payload_locator: value
            .payload_locator
            .as_ref()
            .map(manifest_locator_to_proto),
        transaction_id: value.transaction_id.clone(),
        idempotency_key_hash: value.idempotency_key_hash.clone(),
        created_at: value.created_at.clone(),
        content_type: value.content_type.clone(),
        user_metadata_json: value.user_metadata_json.clone(),
        authenticated_principal: value.authenticated_principal.clone(),
    }
}

fn stream_idempotency_row_to_proto(
    value: &StoredStreamIdempotencyRow,
    common: CoreMetaRowCommonProto,
) -> StreamIdempotencyRowProto {
    StreamIdempotencyRowProto {
        common: Some(common),
        schema: value.schema.clone(),
        stream_id: value.stream_id.clone(),
        sequence: value.sequence,
        cursor: value.cursor.clone(),
        event_hash: value.event_hash.clone(),
        record_kind: value.record_kind.clone(),
        payload_hash: value.payload_hash.clone(),
        transaction_id: value.transaction_id.clone(),
        idempotency_key_hash: value.idempotency_key_hash.clone(),
        created_at: value.created_at.clone(),
        authenticated_principal: value.authenticated_principal.clone(),
    }
}

fn stream_record_index_row_from_proto(
    value: StreamRecordIndexRowProto,
) -> Result<StoredStreamRecordIndexRow> {
    value
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore stream record row missing CoreMeta common"))?;
    Ok(StoredStreamRecordIndexRow {
        schema: value.schema,
        stream_id: value.stream_id,
        partition_id: value.partition_id,
        sequence: value.sequence,
        cursor: value.cursor,
        previous_event_hash: value.previous_event_hash,
        event_hash: value.event_hash,
        record_kind: value.record_kind,
        payload_hash: value.payload_hash,
        payload_len: value.payload_len,
        content_type: value.content_type,
        user_metadata_json: if value.user_metadata_json.is_empty() {
            "{}".to_string()
        } else {
            value.user_metadata_json
        },
        authenticated_principal: value.authenticated_principal,
        inline_payload: value.inline_payload,
        payload_locator: value
            .payload_locator
            .map(manifest_locator_from_proto)
            .transpose()?,
        transaction_id: value.transaction_id,
        idempotency_key_hash: value.idempotency_key_hash,
        created_at: value.created_at,
    })
}

fn stream_head_to_proto(
    value: &CoreStoredStreamHead,
    common: CoreMetaRowCommonProto,
) -> StreamHeadProto {
    StreamHeadProto {
        common: Some(common),
        schema: value.schema.clone(),
        stream_id: value.stream_id.clone(),
        last_sequence: value.last_sequence,
        last_event_hash: value.last_event_hash.clone(),
        record_count: value.record_count,
        updated_at: value.updated_at.clone(),
        idempotency_index_complete: value.idempotency_index_complete,
    }
}

fn root_catalog_to_proto(value: &CoreRootCatalog) -> Result<RootCatalogProto> {
    Ok(RootCatalogProto {
        schema: value.schema.clone(),
        mesh_id: value.mesh_id.clone(),
        generation: value.generation,
        previous_hash: value.previous_hash.clone(),
        root_partitions: value
            .root_partitions
            .iter()
            .map(root_partition_to_proto)
            .collect::<Result<Vec<_>>>()?,
        placement_catalog_ref: value.placement_catalog_ref.clone(),
        stream_directory_ref: value.stream_directory_ref.clone(),
        authz_system_realm_ref: value.authz_system_realm_ref.clone(),
        created_at: value.created_at.clone(),
        signed_by: value.signed_by.clone(),
        signature: value.signature.clone(),
    })
}

fn root_catalog_from_proto(value: RootCatalogProto) -> Result<CoreRootCatalog> {
    Ok(CoreRootCatalog {
        schema: value.schema,
        mesh_id: value.mesh_id,
        generation: value.generation,
        previous_hash: value.previous_hash,
        root_partitions: value
            .root_partitions
            .into_iter()
            .map(root_partition_from_proto)
            .collect::<Result<Vec<_>>>()?,
        placement_catalog_ref: value.placement_catalog_ref,
        stream_directory_ref: value.stream_directory_ref,
        authz_system_realm_ref: value.authz_system_realm_ref,
        created_at: value.created_at,
        signed_by: value.signed_by,
        signature: value.signature,
    })
}

fn root_partition_to_proto(value: &CoreRootPartition) -> Result<RootPartitionProto> {
    Ok(RootPartitionProto {
        partition_id: value.partition_id.clone(),
        owner_node_id: value.owner_node_id.clone(),
        fence: value.fence,
        placement_group: value.placement_group.clone(),
        embedded_head_segment_manifest: Some(object_manifest_to_proto(
            &value.embedded_head_segment_manifest,
        )?),
    })
}

fn root_partition_from_proto(value: RootPartitionProto) -> Result<CoreRootPartition> {
    Ok(CoreRootPartition {
        partition_id: value.partition_id,
        owner_node_id: value.owner_node_id,
        fence: value.fence,
        placement_group: value.placement_group,
        embedded_head_segment_manifest: object_manifest_from_proto(
            value.embedded_head_segment_manifest.ok_or_else(|| {
                anyhow!("CoreStore root partition is missing embedded head segment manifest")
            })?,
        )?,
    })
}

fn quorum_profile_to_proto(value: &CoreQuorumProfile) -> QuorumProfileProto {
    QuorumProfileProto {
        schema: value.schema.clone(),
        placement_group: value.placement_group.clone(),
        replica_count: u32::from(value.replica_count),
        write_quorum: u32::from(value.write_quorum),
        read_quorum: u32::from(value.read_quorum),
        fence_quorum: u32::from(value.fence_quorum),
        epoch: value.epoch,
    }
}

fn quorum_profile_from_proto(value: QuorumProfileProto) -> Result<CoreQuorumProfile> {
    Ok(CoreQuorumProfile {
        schema: value.schema,
        placement_group: value.placement_group,
        replica_count: value
            .replica_count
            .try_into()
            .map_err(|_| anyhow!("CoreStore quorum profile replica_count exceeds u16"))?,
        write_quorum: value
            .write_quorum
            .try_into()
            .map_err(|_| anyhow!("CoreStore quorum profile write_quorum exceeds u16"))?,
        read_quorum: value
            .read_quorum
            .try_into()
            .map_err(|_| anyhow!("CoreStore quorum profile read_quorum exceeds u16"))?,
        fence_quorum: value
            .fence_quorum
            .try_into()
            .map_err(|_| anyhow!("CoreStore quorum profile fence_quorum exceeds u16"))?,
        epoch: value.epoch,
    })
}

fn object_manifest_to_proto(value: &CoreObjectManifest) -> Result<CoreObjectManifestProto> {
    Ok(CoreObjectManifestProto {
        common: Some(object_manifest_common(value)?),
        schema: value.schema.clone(),
        mesh_id: value.mesh_id.clone(),
        region_id: value.region_id.clone(),
        object_hash: value.object_hash.clone(),
        logical_size: value.logical_size,
        logical_file_id: value.logical_file_id.clone(),
        logical_offset: value.logical_offset,
        writer_family: value.writer_family.clone(),
        encryption_algorithm: value.encryption_algorithm.clone(),
        boundary_values: value
            .boundary_values
            .iter()
            .map(boundary_value_to_proto)
            .collect(),
        encoding: Some(object_encoding_to_proto(&value.encoding)),
        placements: value
            .placements
            .iter()
            .map(object_placement_to_proto)
            .collect(),
        created_at: value.created_at.clone(),
        mutation_id: value.mutation_id.clone(),
    })
}

fn object_manifest_from_proto(value: CoreObjectManifestProto) -> Result<CoreObjectManifest> {
    let common = value
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore object manifest missing CoreMeta common"))?;
    validate_object_manifest_common(&value, common)?;
    Ok(CoreObjectManifest {
        schema: value.schema,
        mesh_id: value.mesh_id,
        region_id: value.region_id,
        object_hash: value.object_hash,
        logical_size: value.logical_size,
        logical_file_id: value.logical_file_id,
        logical_offset: value.logical_offset,
        writer_family: value.writer_family,
        encryption_algorithm: value.encryption_algorithm,
        boundary_values: value
            .boundary_values
            .into_iter()
            .map(boundary_value_from_proto)
            .collect(),
        encoding: object_encoding_from_proto(
            value
                .encoding
                .ok_or_else(|| anyhow!("CoreStore object manifest is missing encoding"))?,
        )?,
        placements: value
            .placements
            .into_iter()
            .map(object_placement_from_proto)
            .collect::<Result<Vec<_>>>()?,
        created_at: value.created_at,
        mutation_id: value.mutation_id,
    })
}

fn object_manifest_common(value: &CoreObjectManifest) -> Result<CoreMetaRowCommonProto> {
    Ok(core_meta_committed_row_common(
        format!("mesh/{}/region/{}", value.mesh_id, value.region_id),
        object_manifest_root_key_hash(&value.object_hash),
        object_manifest_root_generation(value.logical_size),
        value.mutation_id.clone(),
        rfc3339_unix_nanos(&value.created_at)?,
    ))
}

fn validate_object_manifest_common(
    value: &CoreObjectManifestProto,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    if common.realm_id != format!("mesh/{}/region/{}", value.mesh_id, value.region_id) {
        return Err(anyhow!("CoreStore object manifest CoreMeta realm mismatch"));
    }
    if common.root_key_hash != object_manifest_root_key_hash(&value.object_hash) {
        return Err(anyhow!("CoreStore object manifest CoreMeta root mismatch"));
    }
    if common.root_generation == 0 {
        return Err(anyhow!(
            "CoreStore object manifest CoreMeta generation mismatch"
        ));
    }
    if common.transaction_id.is_empty() {
        return Err(anyhow!(
            "CoreStore object manifest CoreMeta transaction mismatch"
        ));
    }
    if common.visibility_state_enum() != crate::core_store::CoreMetaVisibilityState::Committed {
        return Err(anyhow!(
            "CoreStore object manifest CoreMeta row is not committed"
        ));
    }
    Ok(())
}

fn object_manifest_root_key_hash(object_hash: &str) -> String {
    core_meta_root_key_hash(&format!("object-manifest/{object_hash}"))
}

pub(in crate::core_store::local) fn object_manifest_root_generation(_logical_size: u64) -> u64 {
    // A content-addressed manifest owns one immutable root, so its sole
    // publication is always generation one regardless of object size.
    1
}

fn rfc3339_unix_nanos(value: &str) -> Result<u64> {
    let parsed = chrono::DateTime::parse_from_rfc3339(value)
        .map_err(|error| anyhow!("CoreStore RFC3339 timestamp is invalid: {error}"))?;
    let nanos = parsed
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("CoreStore RFC3339 timestamp is out of range"))?;
    u64::try_from(nanos).map_err(|_| anyhow!("CoreStore RFC3339 timestamp is negative"))
}

fn object_encoding_to_proto(value: &CoreObjectEncoding) -> CoreObjectEncodingProto {
    CoreObjectEncodingProto {
        block_id: value.block_id.clone(),
        profile_id: value.profile_id.clone(),
        data_shards: u32::from(value.data_shards),
        parity_shards: u32::from(value.parity_shards),
        minimum_read_shards: u32::from(value.minimum_read_shards),
        minimum_write_ack_shards: u32::from(value.minimum_write_ack_shards),
        stripe_size: value.stripe_size,
        placement_scope: value.placement_scope.clone(),
        repair_priority: value.repair_priority.clone(),
        stored_hash: value.stored_hash.clone(),
        compression: Some(compression_descriptor_to_proto(&value.compression)),
        encryption: value.encryption.clone(),
    }
}

fn object_encoding_from_proto(value: CoreObjectEncodingProto) -> Result<CoreObjectEncoding> {
    Ok(CoreObjectEncoding {
        block_id: value.block_id,
        profile_id: value.profile_id,
        data_shards: value
            .data_shards
            .try_into()
            .map_err(|_| anyhow!("CoreStore object encoding data_shards exceeds u16"))?,
        parity_shards: value
            .parity_shards
            .try_into()
            .map_err(|_| anyhow!("CoreStore object encoding parity_shards exceeds u16"))?,
        minimum_read_shards: value
            .minimum_read_shards
            .try_into()
            .map_err(|_| anyhow!("CoreStore object encoding minimum_read_shards exceeds u16"))?,
        minimum_write_ack_shards: value.minimum_write_ack_shards.try_into().map_err(|_| {
            anyhow!("CoreStore object encoding minimum_write_ack_shards exceeds u16")
        })?,
        stripe_size: value.stripe_size,
        placement_scope: value.placement_scope,
        repair_priority: value.repair_priority,
        stored_hash: value.stored_hash,
        compression: value
            .compression
            .map(compression_descriptor_from_proto)
            .ok_or_else(|| {
                anyhow!("CoreStore object encoding is missing compression descriptor")
            })?,
        encryption: value.encryption,
    })
}

fn object_placement_to_proto(value: &CoreObjectPlacement) -> CoreObjectPlacementProto {
    CoreObjectPlacementProto {
        shard_index: u32::from(value.shard_index),
        node_id: value.node_id.clone(),
        region_id: value.region_id.clone(),
        cell_id: value.cell_id.clone(),
        shard_hash: value.shard_hash.clone(),
        stored_size: value.stored_size,
        generation: value.generation,
        placement_epoch: value.placement_epoch,
        fsync_sequence: value.fsync_sequence,
        written_at_unix_nanos: value.written_at_unix_nanos,
        signed_payload_hash: value.signed_payload_hash.clone(),
        signature_algorithm: value.signature_algorithm.clone(),
        receipt_signature: value.receipt_signature.clone(),
    }
}

fn object_placement_from_proto(value: CoreObjectPlacementProto) -> Result<CoreObjectPlacement> {
    Ok(CoreObjectPlacement {
        shard_index: value
            .shard_index
            .try_into()
            .map_err(|_| anyhow!("CoreStore object placement shard_index exceeds u16"))?,
        node_id: value.node_id,
        region_id: value.region_id,
        cell_id: value.cell_id,
        shard_hash: value.shard_hash,
        stored_size: value.stored_size,
        generation: value.generation,
        placement_epoch: value.placement_epoch,
        fsync_sequence: value.fsync_sequence,
        written_at_unix_nanos: value.written_at_unix_nanos,
        signed_payload_hash: value.signed_payload_hash,
        signature_algorithm: value.signature_algorithm,
        receipt_signature: value.receipt_signature,
    })
}

fn boundary_value_to_proto(value: &CoreBoundaryValue) -> CoreBoundaryValueProto {
    CoreBoundaryValueProto {
        schema_generation: value.schema_generation,
        name: value.name.clone(),
        value_type: value.value_type.clone(),
        value: value.value.clone(),
        categories: value.categories.clone(),
        source_kind: value.source_kind.clone(),
        required: value.required,
        max_values_per_block: value.max_values_per_block,
        placement_affinity: value.placement_affinity.clone(),
        compaction_scope: value.compaction_scope.clone(),
        shared_ranges_allowed: value.shared_ranges_allowed,
        shared_record_kinds: value.shared_record_kinds.clone(),
    }
}

fn boundary_value_from_proto(value: CoreBoundaryValueProto) -> CoreBoundaryValue {
    CoreBoundaryValue {
        schema_generation: value.schema_generation,
        name: value.name,
        value_type: value.value_type,
        value: value.value,
        categories: value.categories,
        source_kind: value.source_kind,
        required: value.required,
        max_values_per_block: value.max_values_per_block,
        placement_affinity: value.placement_affinity,
        compaction_scope: value.compaction_scope,
        shared_ranges_allowed: value.shared_ranges_allowed,
        shared_record_kinds: value.shared_record_kinds,
    }
}

fn manifest_locator_to_proto(value: &CoreManifestLocator) -> CoreManifestLocatorProto {
    CoreManifestLocatorProto {
        manifest_ref: Some(CoreManifestRefProto {
            logical_file_id: value.manifest_ref.logical_file_id.clone(),
            writer_family: value.manifest_ref.writer_family.clone(),
            writer_generation: value.manifest_ref.writer_generation,
            manifest_hash: value.manifest_ref.manifest_hash.clone(),
        }),
        manifest_encoding: value.manifest_encoding.clone(),
        manifest_length: value.manifest_length,
        manifest_hash: value.manifest_hash.clone(),
        block_locators: value
            .block_locators
            .iter()
            .map(block_locator_to_proto)
            .collect(),
    }
}

fn manifest_locator_from_proto(value: CoreManifestLocatorProto) -> Result<CoreManifestLocator> {
    let manifest_ref = value
        .manifest_ref
        .ok_or_else(|| anyhow!("CoreStore manifest locator is missing manifest_ref"))?;
    Ok(CoreManifestLocator {
        manifest_ref: CoreManifestRef {
            logical_file_id: manifest_ref.logical_file_id,
            writer_family: manifest_ref.writer_family,
            writer_generation: manifest_ref.writer_generation,
            manifest_hash: manifest_ref.manifest_hash,
        },
        manifest_encoding: value.manifest_encoding,
        manifest_length: value.manifest_length,
        manifest_hash: value.manifest_hash,
        block_locators: value
            .block_locators
            .into_iter()
            .map(block_locator_from_proto)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn block_locator_to_proto(value: &CoreBlockLocator) -> CoreBlockLocatorProto {
    CoreBlockLocatorProto {
        logical_start: value.logical_start,
        logical_end: value.logical_end,
        block_id: value.block_id.clone(),
        codec_id: value.codec_id.clone(),
        data_shards: value.data_shards,
        parity_shards: value.parity_shards,
        plaintext_block_len: value.plaintext_block_len,
        shard_payload_len: value.shard_payload_len,
        padding_len: value.padding_len,
        block_plain_hash: value.block_plain_hash.clone(),
        block_encoded_hash: value.block_encoded_hash.clone(),
        compression: Some(compression_descriptor_to_proto(&value.compression)),
        encryption: Some(encryption_descriptor_to_proto(&value.encryption)),
        erasure_profile_id: value.erasure_profile_id.clone(),
        placement_epoch: value.placement_epoch,
        boundary_summary_hash: value.boundary_summary_hash.clone(),
        boundary_values_b64: value.boundary_values_b64.clone(),
        shard_receipts: value
            .shard_receipts
            .iter()
            .map(shard_receipt_to_proto)
            .collect(),
    }
}

fn block_locator_from_proto(value: CoreBlockLocatorProto) -> Result<CoreBlockLocator> {
    Ok(CoreBlockLocator {
        logical_start: value.logical_start,
        logical_end: value.logical_end,
        block_id: value.block_id,
        codec_id: value.codec_id,
        data_shards: value.data_shards,
        parity_shards: value.parity_shards,
        plaintext_block_len: value.plaintext_block_len,
        shard_payload_len: value.shard_payload_len,
        padding_len: value.padding_len,
        block_plain_hash: value.block_plain_hash,
        block_encoded_hash: value.block_encoded_hash,
        compression: compression_descriptor_from_proto(
            value.compression.ok_or_else(|| {
                anyhow!("CoreStore manifest block locator is missing compression")
            })?,
        ),
        encryption: encryption_descriptor_from_proto(
            value
                .encryption
                .ok_or_else(|| anyhow!("CoreStore manifest block locator is missing encryption"))?,
        ),
        erasure_profile_id: value.erasure_profile_id,
        placement_epoch: value.placement_epoch,
        boundary_summary_hash: value.boundary_summary_hash,
        boundary_values_b64: value.boundary_values_b64,
        shard_receipts: value
            .shard_receipts
            .into_iter()
            .map(shard_receipt_from_proto)
            .collect(),
    })
}

fn shard_receipt_to_proto(value: &CoreShardReceiptSummary) -> CoreShardReceiptSummaryProto {
    CoreShardReceiptSummaryProto {
        node_id: value.node_id.clone(),
        region_id: value.region_id.clone(),
        cell_id: value.cell_id.clone(),
        shard_index: value.shard_index,
        shard_hash: value.shard_hash.clone(),
        shard_length: value.shard_length,
        fsync_sequence: value.fsync_sequence,
        written_at_unix_nanos: value.written_at_unix_nanos,
        signed_payload_hash: value.signed_payload_hash.clone(),
        signature_algorithm: value.signature_algorithm.clone(),
        receipt_signature: value.receipt_signature.clone(),
        boundary_summary_hash: value.boundary_summary_hash.clone(),
        boundary_values_b64: value.boundary_values_b64.clone(),
    }
}

fn shard_receipt_from_proto(value: CoreShardReceiptSummaryProto) -> CoreShardReceiptSummary {
    CoreShardReceiptSummary {
        node_id: value.node_id,
        region_id: value.region_id,
        cell_id: value.cell_id,
        shard_index: value.shard_index,
        shard_hash: value.shard_hash,
        shard_length: value.shard_length,
        fsync_sequence: value.fsync_sequence,
        written_at_unix_nanos: value.written_at_unix_nanos,
        signed_payload_hash: value.signed_payload_hash,
        signature_algorithm: value.signature_algorithm,
        receipt_signature: value.receipt_signature,
        boundary_summary_hash: value.boundary_summary_hash,
        boundary_values_b64: value.boundary_values_b64,
    }
}

fn compression_descriptor_to_proto(
    value: &CoreCompressionDescriptor,
) -> CoreCompressionDescriptorProto {
    CoreCompressionDescriptorProto {
        algorithm: value.algorithm.clone(),
        level: value.level,
        uncompressed_length: value.uncompressed_length,
        compressed_length: value.compressed_length,
        dictionary_id: value.dictionary_id.clone(),
        descriptor_hash: value.descriptor_hash.clone(),
    }
}

fn compression_descriptor_from_proto(
    value: CoreCompressionDescriptorProto,
) -> CoreCompressionDescriptor {
    CoreCompressionDescriptor {
        algorithm: value.algorithm,
        level: value.level,
        uncompressed_length: value.uncompressed_length,
        compressed_length: value.compressed_length,
        dictionary_id: value.dictionary_id,
        descriptor_hash: value.descriptor_hash,
    }
}

fn encryption_descriptor_to_proto(
    value: &CoreEncryptionDescriptor,
) -> CoreEncryptionDescriptorProto {
    CoreEncryptionDescriptorProto {
        algorithm: value.algorithm.clone(),
        key_id: value.key_id.clone(),
        nonce: value.nonce.clone(),
        aad_hash: value.aad_hash.clone(),
        plaintext_hash: value.plaintext_hash.clone(),
        ciphertext_hash: value.ciphertext_hash.clone(),
        descriptor_hash: value.descriptor_hash.clone(),
    }
}

fn encryption_descriptor_from_proto(
    value: CoreEncryptionDescriptorProto,
) -> CoreEncryptionDescriptor {
    CoreEncryptionDescriptor {
        algorithm: value.algorithm,
        key_id: value.key_id,
        nonce: value.nonce,
        aad_hash: value.aad_hash,
        plaintext_hash: value.plaintext_hash,
        ciphertext_hash: value.ciphertext_hash,
        descriptor_hash: value.descriptor_hash,
    }
}
