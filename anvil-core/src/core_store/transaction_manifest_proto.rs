use anyhow::{Result, anyhow, bail};
use prost::Message;

use super::local::CoreTransactionManifestRecord;
use super::types::{
    CoreBlockLocator, CoreCompressionDescriptor, CoreEncryptionDescriptor, CoreManifestLocator,
    CoreManifestRef, CoreShardReceiptSummary,
};

#[derive(Clone, PartialEq, Message)]
struct TransactionManifestHeaderProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(uint64, tag = "2")]
    pre_root_generation: u64,
    #[prost(uint64, tag = "3")]
    post_root_generation: u64,
    #[prost(uint64, tag = "4")]
    mutation_count: u64,
    #[prost(uint64, tag = "5")]
    logical_manifest_count: u64,
    #[prost(string, tag = "6")]
    root_key_hash: String,
    #[prost(string, optional, tag = "7")]
    coordinator_root_key_hash: Option<String>,
    #[prost(uint64, optional, tag = "8")]
    coordinator_root_generation: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
struct TransactionManifestBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, repeated, tag = "2")]
    mutation_ids: Vec<String>,
    #[prost(string, repeated, tag = "3")]
    idempotency_key_hashes: Vec<String>,
    #[prost(uint64, tag = "4")]
    pre_root_generation: u64,
    #[prost(uint64, tag = "5")]
    post_root_generation: u64,
    #[prost(message, repeated, tag = "6")]
    logical_manifests: Vec<CoreManifestLocatorProto>,
    #[prost(string, tag = "7")]
    root_key_hash: String,
    #[prost(string, optional, tag = "8")]
    coordinator_root_key_hash: Option<String>,
    #[prost(uint64, optional, tag = "9")]
    coordinator_root_generation: Option<u64>,
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

pub(super) fn encode_transaction_manifest_header_proto(
    transaction: &CoreTransactionManifestRecord,
) -> Result<Vec<u8>> {
    let proto = TransactionManifestHeaderProto {
        schema: transaction.schema.clone(),
        pre_root_generation: transaction.pre_root_generation,
        post_root_generation: transaction.post_root_generation,
        mutation_count: transaction.mutation_ids.len() as u64,
        logical_manifest_count: transaction.logical_manifests.len() as u64,
        root_key_hash: transaction.root_key_hash.clone(),
        coordinator_root_key_hash: transaction.coordinator_root_key_hash.clone(),
        coordinator_root_generation: transaction.coordinator_root_generation,
    };
    let mut bytes = Vec::new();
    proto.encode(&mut bytes)?;
    Ok(bytes)
}

pub(super) fn encode_transaction_manifest_body_proto(
    transaction: &CoreTransactionManifestRecord,
) -> Result<Vec<u8>> {
    let proto = TransactionManifestBodyProto {
        schema: transaction.schema.clone(),
        mutation_ids: transaction.mutation_ids.clone(),
        idempotency_key_hashes: transaction.idempotency_key_hashes.clone(),
        pre_root_generation: transaction.pre_root_generation,
        post_root_generation: transaction.post_root_generation,
        logical_manifests: transaction
            .logical_manifests
            .iter()
            .map(manifest_locator_to_proto)
            .collect(),
        root_key_hash: transaction.root_key_hash.clone(),
        coordinator_root_key_hash: transaction.coordinator_root_key_hash.clone(),
        coordinator_root_generation: transaction.coordinator_root_generation,
    };
    let mut bytes = Vec::new();
    proto.encode(&mut bytes)?;
    Ok(bytes)
}

pub(super) fn decode_transaction_manifest_proto(
    header_bytes: &[u8],
    body_bytes: &[u8],
) -> Result<CoreTransactionManifestRecord> {
    let header = TransactionManifestHeaderProto::decode(header_bytes)?;
    ensure_canonical_header(&header, header_bytes)?;
    let body = TransactionManifestBodyProto::decode(body_bytes)?;
    ensure_canonical_body(&body, body_bytes)?;
    if header.schema != body.schema
        || header.pre_root_generation != body.pre_root_generation
        || header.post_root_generation != body.post_root_generation
        || header.mutation_count != body.mutation_ids.len() as u64
        || header.logical_manifest_count != body.logical_manifests.len() as u64
        || header.root_key_hash != body.root_key_hash
        || header.coordinator_root_key_hash != body.coordinator_root_key_hash
        || header.coordinator_root_generation != body.coordinator_root_generation
    {
        bail!("CoreStore transaction manifest header/body mismatch");
    }
    Ok(CoreTransactionManifestRecord {
        schema: body.schema,
        root_key_hash: body.root_key_hash,
        coordinator_root_key_hash: body.coordinator_root_key_hash,
        coordinator_root_generation: body.coordinator_root_generation,
        mutation_ids: body.mutation_ids,
        idempotency_key_hashes: body.idempotency_key_hashes,
        pre_root_generation: body.pre_root_generation,
        post_root_generation: body.post_root_generation,
        logical_manifests: body
            .logical_manifests
            .into_iter()
            .map(manifest_locator_from_proto)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn ensure_canonical_header(header: &TransactionManifestHeaderProto, bytes: &[u8]) -> Result<()> {
    let mut canonical = Vec::new();
    header.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("CoreStore transaction manifest header is not deterministic protobuf");
    }
    Ok(())
}

fn ensure_canonical_body(body: &TransactionManifestBodyProto, bytes: &[u8]) -> Result<()> {
    let mut canonical = Vec::new();
    body.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("CoreStore transaction manifest body is not deterministic protobuf");
    }
    Ok(())
}

pub(crate) fn encode_manifest_locator_proto(value: &CoreManifestLocator) -> Result<Vec<u8>> {
    let proto = manifest_locator_to_proto(value);
    let mut bytes = Vec::with_capacity(proto.encoded_len());
    proto.encode(&mut bytes)?;
    Ok(bytes)
}

pub(crate) fn decode_manifest_locator_proto(bytes: &[u8]) -> Result<CoreManifestLocator> {
    let proto = CoreManifestLocatorProto::decode(bytes)?;
    let mut canonical = Vec::with_capacity(proto.encoded_len());
    proto.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("CoreStore manifest locator is not deterministic protobuf");
    }
    manifest_locator_from_proto(proto)
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
