use anyhow::{Result, anyhow, bail};
use prost::Message;

use super::types::{
    CoreBoundaryValue, CoreCompressionDescriptor, CoreEncryptionDescriptor, CoreLogicalBlockRef,
    CoreLogicalFileManifest, CoreLogicalRange, CoreLogicalShardRef, CoreSharedRangeMarker,
};
use crate::core_store::{
    CoreMetaRowCommonProto, CoreMetaVisibilityState, core_meta_committed_row_common,
};

#[derive(Clone, PartialEq, Message)]
struct CoreLogicalFileManifestProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    logical_file_id: String,
    #[prost(string, tag = "3")]
    writer_family: String,
    #[prost(uint64, tag = "4")]
    writer_generation: u64,
    #[prost(uint64, tag = "5")]
    logical_size: u64,
    #[prost(string, tag = "6")]
    content_hash: String,
    #[prost(uint64, tag = "7")]
    boundary_schema_generation: u64,
    #[prost(message, repeated, tag = "8")]
    ranges: Vec<CoreLogicalRangeProto>,
    #[prost(message, repeated, tag = "9")]
    blocks: Vec<CoreLogicalBlockRefProto>,
    #[prost(message, optional, tag = "10")]
    compression: Option<CoreCompressionDescriptorProto>,
    #[prost(message, optional, tag = "11")]
    encryption: Option<CoreEncryptionDescriptorProto>,
    #[prost(string, tag = "12")]
    erasure_profile_id: String,
    #[prost(uint64, tag = "13")]
    placement_epoch: u64,
    #[prost(string, tag = "14")]
    created_by_mutation_id: String,
    #[prost(string, tag = "15")]
    codec_id: String,
    #[prost(uint32, tag = "16")]
    data_shards: u32,
    #[prost(uint32, tag = "17")]
    parity_shards: u32,
}

#[derive(Clone, PartialEq, Message)]
struct CoreLogicalRangeProto {
    #[prost(string, tag = "1")]
    range_id: String,
    #[prost(uint64, tag = "2")]
    byte_start: u64,
    #[prost(uint64, tag = "3")]
    byte_end: u64,
    #[prost(string, tag = "4")]
    writer_record_kind: String,
    #[prost(message, repeated, tag = "5")]
    boundary_values: Vec<CoreBoundaryValueProto>,
    #[prost(bytes, tag = "6")]
    writer_statistics: Vec<u8>,
    #[prost(string, repeated, tag = "7")]
    block_ids: Vec<String>,
    #[prost(string, repeated, tag = "8")]
    prefetch_next_range_ids: Vec<String>,
    #[prost(string, tag = "9")]
    preferred_block_boundary: String,
    #[prost(uint32, repeated, tag = "10")]
    boundary_dimension_ids: Vec<u32>,
    #[prost(message, optional, tag = "11")]
    shared_range: Option<CoreSharedRangeMarkerProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreSharedRangeMarkerProto {
    #[prost(string, tag = "1")]
    record_kind: String,
    #[prost(string, tag = "2")]
    reason: String,
    #[prost(uint32, repeated, tag = "3")]
    boundary_dimension_ids: Vec<u32>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreLogicalBlockRefProto {
    #[prost(string, tag = "1")]
    block_id: String,
    #[prost(uint64, tag = "2")]
    logical_offset: u64,
    #[prost(uint64, tag = "3")]
    logical_length: u64,
    #[prost(uint64, tag = "4")]
    compressed_length: u64,
    #[prost(uint64, tag = "5")]
    encrypted_length: u64,
    #[prost(string, tag = "6")]
    content_hash: String,
    #[prost(message, optional, tag = "19")]
    compression: Option<CoreCompressionDescriptorProto>,
    #[prost(message, optional, tag = "7")]
    encryption: Option<CoreEncryptionDescriptorProto>,
    #[prost(string, tag = "8")]
    erasure_set_id: String,
    #[prost(message, repeated, tag = "9")]
    shards: Vec<CoreLogicalShardRefProto>,
    #[prost(string, tag = "10")]
    codec_id: String,
    #[prost(uint32, tag = "11")]
    data_shards: u32,
    #[prost(uint32, tag = "12")]
    parity_shards: u32,
    #[prost(uint64, tag = "13")]
    plaintext_block_len: u64,
    #[prost(uint64, tag = "14")]
    shard_payload_len: u64,
    #[prost(uint64, tag = "15")]
    padding_len: u64,
    #[prost(string, tag = "16")]
    block_encoded_hash: String,
    #[prost(string, tag = "17")]
    boundary_summary_hash: String,
    #[prost(string, tag = "18")]
    boundary_values_b64: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreLogicalShardRefProto {
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
    stored_length: u64,
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
    #[prost(string, tag = "14")]
    boundary_summary_hash: String,
    #[prost(string, tag = "15")]
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

pub(super) fn encode_logical_file_manifest_proto(
    manifest: &CoreLogicalFileManifest,
) -> Result<Vec<u8>> {
    let proto = logical_file_manifest_to_proto(manifest);
    let mut bytes = Vec::new();
    proto.encode(&mut bytes)?;
    Ok(bytes)
}

pub(super) fn decode_logical_file_manifest_proto(bytes: &[u8]) -> Result<CoreLogicalFileManifest> {
    let proto = CoreLogicalFileManifestProto::decode(bytes)?;
    let mut canonical = Vec::new();
    proto.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("CoreStore logical file manifest is not deterministic protobuf");
    }
    logical_file_manifest_from_proto(proto)
}

pub(super) struct CoreInlineManifestBodyRow {
    pub schema: String,
    pub logical_file_id: String,
    pub writer_family: String,
    pub writer_generation: u64,
    pub manifest_hash: String,
    pub manifest_encoding: String,
    pub manifest_length: u64,
    pub body: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreInlineManifestBodyRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    logical_file_id: String,
    #[prost(string, tag = "4")]
    writer_family: String,
    #[prost(uint64, tag = "5")]
    writer_generation: u64,
    #[prost(string, tag = "6")]
    manifest_hash: String,
    #[prost(string, tag = "7")]
    manifest_encoding: String,
    #[prost(uint64, tag = "8")]
    manifest_length: u64,
    #[prost(bytes, tag = "9")]
    body: Vec<u8>,
}

pub(super) fn encode_inline_manifest_body_row(row: &CoreInlineManifestBodyRow) -> Result<Vec<u8>> {
    let proto = CoreInlineManifestBodyRowProto {
        common: Some(inline_manifest_body_common(row)),
        schema: row.schema.clone(),
        logical_file_id: row.logical_file_id.clone(),
        writer_family: row.writer_family.clone(),
        writer_generation: row.writer_generation,
        manifest_hash: row.manifest_hash.clone(),
        manifest_encoding: row.manifest_encoding.clone(),
        manifest_length: row.manifest_length,
        body: row.body.clone(),
    };
    let mut bytes = Vec::new();
    proto.encode(&mut bytes)?;
    Ok(bytes)
}

pub(super) fn decode_inline_manifest_body_row(bytes: &[u8]) -> Result<CoreInlineManifestBodyRow> {
    let proto = CoreInlineManifestBodyRowProto::decode(bytes)?;
    let mut canonical = Vec::new();
    proto.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("CoreStore inline manifest body row is not deterministic protobuf");
    }
    let common = proto
        .common
        .clone()
        .ok_or_else(|| anyhow!("CoreStore inline manifest body row missing CoreMeta common"))?;
    let row = CoreInlineManifestBodyRow {
        schema: proto.schema,
        logical_file_id: proto.logical_file_id,
        writer_family: proto.writer_family,
        writer_generation: proto.writer_generation,
        manifest_hash: proto.manifest_hash,
        manifest_encoding: proto.manifest_encoding,
        manifest_length: proto.manifest_length,
        body: proto.body,
    };
    validate_inline_manifest_body_common(&row, &common)?;
    Ok(row)
}

fn inline_manifest_body_common(_row: &CoreInlineManifestBodyRow) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common("", "", 0, "", 0)
}

fn validate_inline_manifest_body_common(
    _row: &CoreInlineManifestBodyRow,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    if !common.realm_id.is_empty() {
        return Err(anyhow!(
            "CoreStore inline manifest body CoreMeta realm mismatch"
        ));
    }
    if !common.root_key_hash.is_empty() {
        return Err(anyhow!(
            "CoreStore inline manifest body CoreMeta root mismatch"
        ));
    }
    if common.root_generation != 0 {
        return Err(anyhow!(
            "CoreStore inline manifest body CoreMeta generation mismatch"
        ));
    }
    if !common.transaction_id.is_empty() {
        return Err(anyhow!(
            "CoreStore inline manifest body CoreMeta transaction mismatch"
        ));
    }
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        return Err(anyhow!(
            "CoreStore inline manifest body CoreMeta row is not committed"
        ));
    }
    Ok(())
}

fn logical_file_manifest_to_proto(value: &CoreLogicalFileManifest) -> CoreLogicalFileManifestProto {
    CoreLogicalFileManifestProto {
        schema: value.schema.clone(),
        logical_file_id: value.logical_file_id.clone(),
        writer_family: value.writer_family.clone(),
        writer_generation: value.writer_generation,
        logical_size: value.logical_size,
        content_hash: value.content_hash.clone(),
        boundary_schema_generation: value.boundary_schema_generation,
        ranges: value.ranges.iter().map(logical_range_to_proto).collect(),
        blocks: value
            .blocks
            .iter()
            .map(logical_block_ref_to_proto)
            .collect(),
        compression: Some(compression_descriptor_to_proto(&value.compression)),
        encryption: Some(encryption_descriptor_to_proto(&value.encryption)),
        erasure_profile_id: value.erasure_profile_id.clone(),
        placement_epoch: value.placement_epoch,
        created_by_mutation_id: value.created_by_mutation_id.clone(),
        codec_id: value.codec_id.clone(),
        data_shards: value.data_shards,
        parity_shards: value.parity_shards,
    }
}

fn logical_file_manifest_from_proto(
    value: CoreLogicalFileManifestProto,
) -> Result<CoreLogicalFileManifest> {
    Ok(CoreLogicalFileManifest {
        schema: value.schema,
        logical_file_id: value.logical_file_id,
        writer_family: value.writer_family,
        writer_generation: value.writer_generation,
        logical_size: value.logical_size,
        content_hash: value.content_hash,
        boundary_schema_generation: value.boundary_schema_generation,
        ranges: value
            .ranges
            .into_iter()
            .map(logical_range_from_proto)
            .collect::<Result<Vec<_>>>()?,
        blocks: value
            .blocks
            .into_iter()
            .map(logical_block_ref_from_proto)
            .collect::<Result<Vec<_>>>()?,
        compression: compression_descriptor_from_proto(
            value
                .compression
                .ok_or_else(|| anyhow!("CoreStore logical file manifest is missing compression"))?,
        ),
        encryption: encryption_descriptor_from_proto(
            value
                .encryption
                .ok_or_else(|| anyhow!("CoreStore logical file manifest is missing encryption"))?,
        ),
        erasure_profile_id: value.erasure_profile_id,
        placement_epoch: value.placement_epoch,
        created_by_mutation_id: value.created_by_mutation_id,
        codec_id: value.codec_id,
        data_shards: value.data_shards,
        parity_shards: value.parity_shards,
    })
}

fn logical_range_to_proto(value: &CoreLogicalRange) -> CoreLogicalRangeProto {
    CoreLogicalRangeProto {
        range_id: value.range_id.clone(),
        byte_start: value.byte_start,
        byte_end: value.byte_end,
        writer_record_kind: value.writer_record_kind.clone(),
        boundary_values: value
            .boundary_values
            .iter()
            .map(boundary_value_to_proto)
            .collect(),
        writer_statistics: value.writer_statistics.clone(),
        block_ids: value.block_ids.clone(),
        prefetch_next_range_ids: value.prefetch_next_range_ids.clone(),
        preferred_block_boundary: value.preferred_block_boundary.clone(),
        boundary_dimension_ids: value.boundary_dimension_ids.clone(),
        shared_range: value.shared_range.as_ref().map(shared_range_to_proto),
    }
}

fn logical_range_from_proto(value: CoreLogicalRangeProto) -> Result<CoreLogicalRange> {
    Ok(CoreLogicalRange {
        range_id: value.range_id,
        byte_start: value.byte_start,
        byte_end: value.byte_end,
        writer_record_kind: value.writer_record_kind,
        boundary_values: value
            .boundary_values
            .into_iter()
            .map(boundary_value_from_proto)
            .collect(),
        writer_statistics: value.writer_statistics,
        block_ids: value.block_ids,
        prefetch_next_range_ids: value.prefetch_next_range_ids,
        preferred_block_boundary: value.preferred_block_boundary,
        boundary_dimension_ids: value.boundary_dimension_ids,
        shared_range: value.shared_range.map(shared_range_from_proto),
    })
}

fn shared_range_to_proto(value: &CoreSharedRangeMarker) -> CoreSharedRangeMarkerProto {
    CoreSharedRangeMarkerProto {
        record_kind: value.record_kind.clone(),
        reason: value.reason.clone(),
        boundary_dimension_ids: value.boundary_dimension_ids.clone(),
    }
}

fn shared_range_from_proto(value: CoreSharedRangeMarkerProto) -> CoreSharedRangeMarker {
    CoreSharedRangeMarker {
        record_kind: value.record_kind,
        reason: value.reason,
        boundary_dimension_ids: value.boundary_dimension_ids,
    }
}

fn logical_block_ref_to_proto(value: &CoreLogicalBlockRef) -> CoreLogicalBlockRefProto {
    CoreLogicalBlockRefProto {
        block_id: value.block_id.clone(),
        logical_offset: value.logical_offset,
        logical_length: value.logical_length,
        compressed_length: value.compressed_length,
        encrypted_length: value.encrypted_length,
        content_hash: value.content_hash.clone(),
        compression: Some(compression_descriptor_to_proto(&value.compression)),
        encryption: Some(encryption_descriptor_to_proto(&value.encryption)),
        erasure_set_id: value.erasure_set_id.clone(),
        shards: value
            .shards
            .iter()
            .map(logical_shard_ref_to_proto)
            .collect(),
        codec_id: value.codec_id.clone(),
        data_shards: value.data_shards,
        parity_shards: value.parity_shards,
        plaintext_block_len: value.plaintext_block_len,
        shard_payload_len: value.shard_payload_len,
        padding_len: value.padding_len,
        block_encoded_hash: value.block_encoded_hash.clone(),
        boundary_summary_hash: value.boundary_summary_hash.clone(),
        boundary_values_b64: value.boundary_values_b64.clone(),
    }
}

fn logical_block_ref_from_proto(value: CoreLogicalBlockRefProto) -> Result<CoreLogicalBlockRef> {
    Ok(CoreLogicalBlockRef {
        block_id: value.block_id,
        logical_offset: value.logical_offset,
        logical_length: value.logical_length,
        compressed_length: value.compressed_length,
        encrypted_length: value.encrypted_length,
        content_hash: value.content_hash,
        compression: compression_descriptor_from_proto(
            value
                .compression
                .ok_or_else(|| anyhow!("CoreStore logical file block is missing compression"))?,
        ),
        encryption: encryption_descriptor_from_proto(
            value
                .encryption
                .ok_or_else(|| anyhow!("CoreStore logical file block is missing encryption"))?,
        ),
        erasure_set_id: value.erasure_set_id,
        shards: value
            .shards
            .into_iter()
            .map(logical_shard_ref_from_proto)
            .collect(),
        codec_id: value.codec_id,
        data_shards: value.data_shards,
        parity_shards: value.parity_shards,
        plaintext_block_len: value.plaintext_block_len,
        shard_payload_len: value.shard_payload_len,
        padding_len: value.padding_len,
        block_encoded_hash: value.block_encoded_hash,
        boundary_summary_hash: value.boundary_summary_hash,
        boundary_values_b64: value.boundary_values_b64,
    })
}

fn logical_shard_ref_to_proto(value: &CoreLogicalShardRef) -> CoreLogicalShardRefProto {
    CoreLogicalShardRefProto {
        node_id: value.node_id.clone(),
        region_id: value.region_id.clone(),
        cell_id: value.cell_id.clone(),
        shard_index: value.shard_index,
        shard_hash: value.shard_hash.clone(),
        stored_length: value.stored_length,
        generation: value.generation,
        placement_epoch: value.placement_epoch,
        fsync_sequence: value.fsync_sequence,
        written_at_unix_nanos: value.written_at_unix_nanos,
        signed_payload_hash: value.signed_payload_hash.clone(),
        signature_algorithm: value.signature_algorithm.clone(),
        receipt_signature: value.receipt_signature.clone(),
        boundary_summary_hash: value.boundary_summary_hash.clone(),
        boundary_values_b64: value.boundary_values_b64.clone(),
    }
}

fn logical_shard_ref_from_proto(value: CoreLogicalShardRefProto) -> CoreLogicalShardRef {
    CoreLogicalShardRef {
        node_id: value.node_id,
        region_id: value.region_id,
        cell_id: value.cell_id,
        shard_index: value.shard_index,
        shard_hash: value.shard_hash,
        stored_length: value.stored_length,
        generation: value.generation,
        placement_epoch: value.placement_epoch,
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
