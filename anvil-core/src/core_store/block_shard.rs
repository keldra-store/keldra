use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use prost::Message;
use sha2::{Digest, Sha256};
use std::io;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

use crate::formats::writer::WriterFamily;

use super::types::{CoreBoundaryValue, CoreInternalShardReceipt};

pub(super) const CORE_BLOCK_SHARD_MAGIC: &[u8; 8] = b"ANBLK\n\0\0";
const CORE_BLOCK_SHARD_VERSION: u16 = 1;
const CORE_BLOCK_SHARD_HEADER_SCHEMA: &str = "anvil.core.block_shard.v1";
const CORE_BLOCK_SHARD_REPAIR_RECORD_SCHEMA: &str = "anvil.core.block_shard_repair.v1";
pub(super) const CORE_BLOCK_SHARD_MAX_HEADER_BYTES: usize = 16 * 1024 * 1024;
pub(super) const CORE_BLOCK_SHARD_MAX_REPAIR_RECORD_BYTES: u64 = 1024 * 1024;
const CORE_BLOCK_SHARD_FILE_HASH_BYTES: usize = 32;
const CORE_BLOCK_SHARD_READ_CHUNK_BYTES: usize = 64 * 1024;
const CORE_BLOCK_SHARD_FALLBACK_MAX_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(super) struct BlockShardHeaderInput {
    pub(super) block_id: String,
    pub(super) erasure_set_id: String,
    pub(super) shard_index: u16,
    pub(super) erasure_profile_id: String,
    pub(super) logical_file_id: String,
    pub(super) logical_offset: u64,
    pub(super) logical_length: u64,
    pub(super) payload_plain_hash: String,
    pub(super) payload_stored_hash: String,
    pub(super) compression: String,
    pub(super) encryption: String,
    pub(super) placement_epoch: u64,
    pub(super) boundary_summary_hash: String,
    pub(super) boundary_values_b64: String,
    pub(super) writer_family: String,
    pub(super) created_by_mutation_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct BlockShardHeaderProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    pub(super) block_id: String,
    #[prost(string, tag = "3")]
    pub(super) erasure_set_id: String,
    #[prost(uint32, tag = "4")]
    shard_index: u32,
    #[prost(string, tag = "5")]
    pub(super) erasure_profile_id: String,
    #[prost(string, tag = "6")]
    pub(super) logical_file_id: String,
    #[prost(uint64, tag = "7")]
    pub(super) logical_offset: u64,
    #[prost(uint64, tag = "8")]
    pub(super) logical_length: u64,
    #[prost(string, tag = "9")]
    pub(super) payload_plain_hash: String,
    #[prost(string, tag = "10")]
    pub(super) payload_stored_hash: String,
    #[prost(string, tag = "11")]
    pub(super) compression: String,
    #[prost(string, tag = "12")]
    pub(super) encryption: String,
    #[prost(uint64, tag = "13")]
    pub(super) placement_epoch: u64,
    #[prost(string, tag = "14")]
    pub(super) boundary_summary_hash: String,
    #[prost(string, tag = "15")]
    pub(super) boundary_values_b64: String,
    #[prost(string, tag = "16")]
    pub(super) writer_family: String,
    #[prost(string, tag = "17")]
    pub(super) created_by_mutation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BlockShardValidState {
    pub(super) block_id: String,
    pub(super) shard_index: u16,
    pub(super) placement_epoch: u64,
    pub(super) file_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BlockShardStoredState {
    Absent,
    Valid(BlockShardValidState),
    Corrupt,
}

struct ValidatedBlockShard {
    header: BlockShardHeaderProto,
    shard_index: u16,
    payload: Option<Vec<u8>>,
    file_hash: String,
}

enum BlockShardReadFailure {
    Io(io::Error),
    Corrupt(anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
#[error("CoreStore block shard allocation failed: {0}")]
struct BlockShardAllocationError(&'static str);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BlockShardRepairRecord {
    pub(super) operation_id: String,
    pub(super) expected_file_present: bool,
    pub(super) expected_placement_epoch: u64,
    pub(super) expected_file_hash: String,
    pub(super) new_placement_epoch: u64,
    pub(super) new_file_hash: String,
    pub(super) receipt: CoreInternalShardReceipt,
}

#[derive(Clone, PartialEq, Message)]
struct BlockShardRepairRecordProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    operation_id: String,
    #[prost(bool, tag = "3")]
    expected_file_present: bool,
    #[prost(uint64, tag = "4")]
    expected_placement_epoch: u64,
    #[prost(string, tag = "5")]
    expected_file_hash: String,
    #[prost(uint64, tag = "6")]
    new_placement_epoch: u64,
    #[prost(string, tag = "7")]
    new_file_hash: String,
    #[prost(message, optional, tag = "8")]
    receipt: Option<BlockShardRepairReceiptProto>,
}

#[derive(Clone, PartialEq, Message)]
struct BlockShardRepairReceiptProto {
    #[prost(string, tag = "1")]
    node_id: String,
    #[prost(string, tag = "2")]
    region_id: String,
    #[prost(string, tag = "3")]
    cell_id: String,
    #[prost(string, tag = "4")]
    block_id: String,
    #[prost(uint32, tag = "5")]
    shard_index: u32,
    #[prost(string, tag = "6")]
    shard_hash: String,
    #[prost(uint64, tag = "7")]
    shard_length: u64,
    #[prost(uint64, tag = "8")]
    fsync_sequence: u64,
    #[prost(uint64, tag = "9")]
    written_at_unix_nanos: u64,
    #[prost(string, tag = "10")]
    signed_payload_hash: String,
    #[prost(bytes = "vec", tag = "11")]
    signature: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct BlockShardExpectation<'a> {
    pub(super) block_id: &'a str,
    pub(super) shard_index: u16,
    pub(super) erasure_profile_id: &'a str,
    pub(super) placement_epoch: u64,
    pub(super) payload_hash: &'a str,
    pub(super) payload_len: u64,
    pub(super) boundary_summary_hash: Option<&'a str>,
    pub(super) boundary_values_b64: Option<&'a str>,
}

pub(super) struct ShardReceiptPayloadInput<'a> {
    pub(super) block_id: &'a str,
    pub(super) shard_index: u16,
    pub(super) erasure_profile: &'a str,
    pub(super) node_id: &'a str,
    pub(super) region_id: &'a str,
    pub(super) cell_id: &'a str,
    pub(super) placement_epoch: u64,
    pub(super) shard_length: u64,
    pub(super) shard_hash: &'a str,
    pub(super) fsync_sequence: u64,
    pub(super) written_at_unix_nanos: u64,
    pub(super) boundary_summary_hash: &'a str,
}

#[derive(Clone, PartialEq, Message)]
struct BoundaryValuesProto {
    #[prost(message, repeated, tag = "1")]
    values: Vec<BoundaryValueProto>,
}

#[derive(Clone, PartialEq, Message)]
struct BoundaryValueProto {
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
struct ShardReceiptPayloadProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    block_id: String,
    #[prost(string, tag = "3")]
    shard_id: String,
    #[prost(uint32, tag = "4")]
    shard_index: u32,
    #[prost(string, tag = "5")]
    erasure_profile: String,
    #[prost(string, tag = "6")]
    node_id: String,
    #[prost(string, tag = "7")]
    region_id: String,
    #[prost(string, tag = "8")]
    cell_id: String,
    #[prost(uint64, tag = "9")]
    placement_epoch: u64,
    #[prost(uint64, tag = "10")]
    shard_length: u64,
    #[prost(string, tag = "11")]
    shard_hash: String,
    #[prost(uint64, tag = "12")]
    fsync_sequence: u64,
    #[prost(uint64, tag = "13")]
    written_at_unix_nanos: u64,
    #[prost(string, tag = "14")]
    boundary_summary_hash: String,
}

pub(super) fn shard_receipt_payload_hash(input: ShardReceiptPayloadInput<'_>) -> String {
    let shard_id = format!("{}:{}", input.block_id, input.shard_index);
    let proto = ShardReceiptPayloadProto {
        schema: "anvil.core.shard_receipt.v1".to_string(),
        block_id: input.block_id.to_string(),
        shard_id,
        shard_index: u32::from(input.shard_index),
        erasure_profile: input.erasure_profile.to_string(),
        node_id: input.node_id.to_string(),
        region_id: input.region_id.to_string(),
        cell_id: input.cell_id.to_string(),
        placement_epoch: input.placement_epoch,
        shard_length: input.shard_length,
        shard_hash: input.shard_hash.to_string(),
        fsync_sequence: input.fsync_sequence,
        written_at_unix_nanos: input.written_at_unix_nanos,
        boundary_summary_hash: input.boundary_summary_hash.to_string(),
    };
    let bytes = proto.encode_to_vec();
    descriptor_hash(&["anvil.shard.receipt.v1", &hex::encode(bytes)])
}

pub(super) fn encode_block_shard_file(
    header: BlockShardHeaderInput,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let payload_len = u64::try_from(payload.len())
        .map_err(|_| anyhow!("CoreStore block shard payload length exceeds u64"))?;
    if header.logical_length != payload_len {
        bail!("CoreStore block shard header logical length mismatch");
    }
    let header_proto = encode_block_shard_header_proto(header)?;
    let file_len = CORE_BLOCK_SHARD_MAGIC
        .len()
        .checked_add(2 + 4)
        .and_then(|len| len.checked_add(header_proto.len()))
        .and_then(|len| len.checked_add(8))
        .and_then(|len| len.checked_add(payload.len()))
        .and_then(|len| len.checked_add(4 + CORE_BLOCK_SHARD_FILE_HASH_BYTES))
        .ok_or_else(|| anyhow!("CoreStore block shard length overflow"))?;
    let mut out = Vec::new();
    out.try_reserve_exact(file_len)
        .map_err(|_| anyhow!("CoreStore block shard allocation exceeds addressable memory"))?;
    out.extend_from_slice(CORE_BLOCK_SHARD_MAGIC);
    out.extend_from_slice(&CORE_BLOCK_SHARD_VERSION.to_le_bytes());
    write_u32_le(&mut out, header_proto.len())?;
    out.extend_from_slice(&header_proto);
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(payload);
    let crc = crc32c_finish(crc32c_update(
        crc32c_update(crc32c_start(), &header_proto),
        payload,
    ));
    out.extend_from_slice(&crc.to_le_bytes());
    let file_hash = Sha256::digest(&out);
    out.extend_from_slice(file_hash.as_ref());
    Ok(out)
}

pub(super) fn block_shard_repair_head_path(shard_path: &PathBuf) -> PathBuf {
    shard_path.with_extension("anb.repair.head")
}

pub(super) fn block_shard_repair_operation_path(
    shard_path: &PathBuf,
    operation_id: &str,
) -> PathBuf {
    let operation_hash = sha256_hex(operation_id.as_bytes());
    shard_path.with_extension(format!("anb.repair.operation-{operation_hash}"))
}

pub(super) fn block_shard_file_hash(bytes: &[u8]) -> Result<String> {
    let file_hash_offset = bytes
        .len()
        .checked_sub(CORE_BLOCK_SHARD_FILE_HASH_BYTES)
        .ok_or_else(|| anyhow!("CoreStore block shard is missing its file hash"))?;
    let expected_file_hash = &bytes[file_hash_offset..];
    let actual_file_hash = Sha256::digest(&bytes[..file_hash_offset]);
    if expected_file_hash != actual_file_hash.as_slice() {
        bail!("CoreStore block shard file hash mismatch");
    }
    Ok(format!("sha256:{}", hex::encode(actual_file_hash)))
}

pub(super) async fn read_block_shard_stored_state_bounded(
    path: &PathBuf,
    max_payload_bytes: u64,
) -> Result<BlockShardStoredState> {
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(BlockShardStoredState::Absent);
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("open CoreStore block shard state {}", path.display()));
        }
    };
    match read_validated_block_shard(file, max_payload_bytes, false).await {
        Ok(validated) => Ok(BlockShardStoredState::Valid(BlockShardValidState {
            block_id: validated.header.block_id,
            shard_index: validated.shard_index,
            placement_epoch: validated.header.placement_epoch,
            file_hash: validated.file_hash,
        })),
        Err(BlockShardReadFailure::Corrupt(_)) => Ok(BlockShardStoredState::Corrupt),
        Err(BlockShardReadFailure::Io(error)) => Err(error)
            .with_context(|| format!("probe CoreStore block shard state {}", path.display())),
    }
}

async fn read_validated_block_shard(
    mut file: tokio::fs::File,
    max_payload_bytes: u64,
    retain_payload: bool,
) -> std::result::Result<ValidatedBlockShard, BlockShardReadFailure> {
    let metadata = file.metadata().await.map_err(BlockShardReadFailure::Io)?;
    if !metadata.is_file() {
        return Err(BlockShardReadFailure::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "CoreStore block shard path is not a regular file",
        )));
    }
    let file_len = metadata.len();
    let mut whole_file_hasher = Sha256::new();

    let mut prefix = [0u8; CORE_BLOCK_SHARD_MAGIC.len() + 2 + 4];
    read_block_shard_frame_exact(&mut file, &mut prefix, "prefix").await?;
    whole_file_hasher.update(prefix);
    if &prefix[..CORE_BLOCK_SHARD_MAGIC.len()] != CORE_BLOCK_SHARD_MAGIC {
        return Err(corrupt_block_shard("invalid magic"));
    }
    let version_offset = CORE_BLOCK_SHARD_MAGIC.len();
    let version = u16::from_le_bytes(
        prefix[version_offset..version_offset + 2]
            .try_into()
            .map_err(|_| corrupt_block_shard("invalid version field"))?,
    );
    if version != CORE_BLOCK_SHARD_VERSION {
        return Err(corrupt_block_shard(format!(
            "unsupported version {version}"
        )));
    }
    let header_len_offset = version_offset + 2;
    let header_len = u32::from_le_bytes(
        prefix[header_len_offset..]
            .try_into()
            .map_err(|_| corrupt_block_shard("invalid header length field"))?,
    ) as usize;
    if header_len > CORE_BLOCK_SHARD_MAX_HEADER_BYTES {
        return Err(corrupt_block_shard("header exceeds bounded size"));
    }
    let mut header_bytes = Vec::new();
    header_bytes.try_reserve_exact(header_len).map_err(|_| {
        BlockShardReadFailure::Io(io::Error::other(
            "CoreStore block shard header allocation failed",
        ))
    })?;
    header_bytes.resize(header_len, 0);
    read_block_shard_frame_exact(&mut file, &mut header_bytes, "header").await?;
    whole_file_hasher.update(&header_bytes);
    let header = decode_block_shard_header_proto(&header_bytes)
        .map_err(classify_block_shard_validation_error)?;
    let shard_index =
        validate_stored_block_shard_header(&header).map_err(BlockShardReadFailure::Corrupt)?;

    let mut payload_len_bytes = [0u8; 8];
    read_block_shard_frame_exact(&mut file, &mut payload_len_bytes, "payload length").await?;
    whole_file_hasher.update(payload_len_bytes);
    let payload_len = u64::from_le_bytes(payload_len_bytes);
    if payload_len > max_payload_bytes {
        return Err(corrupt_block_shard(format!(
            "payload length {payload_len} exceeds bounded maximum {max_payload_bytes}"
        )));
    }
    if header.logical_length != payload_len {
        return Err(corrupt_block_shard("header logical length mismatch"));
    }
    let expected_file_len = u64::try_from(prefix.len())
        .ok()
        .and_then(|len| len.checked_add(header_len as u64))
        .and_then(|len| len.checked_add(8))
        .and_then(|len| len.checked_add(payload_len))
        .and_then(|len| len.checked_add(4 + CORE_BLOCK_SHARD_FILE_HASH_BYTES as u64))
        .ok_or_else(|| corrupt_block_shard("file length overflow"))?;
    if file_len != expected_file_len {
        return Err(corrupt_block_shard("file length mismatch"));
    }

    let mut payload = if retain_payload {
        let capacity = usize::try_from(payload_len)
            .map_err(|_| corrupt_block_shard("payload length exceeds addressable memory"))?;
        let mut payload = Vec::new();
        payload.try_reserve_exact(capacity).map_err(|_| {
            BlockShardReadFailure::Io(io::Error::other(
                "CoreStore block shard payload allocation failed",
            ))
        })?;
        Some(payload)
    } else {
        None
    };
    let mut payload_hasher = Sha256::new();
    let mut crc = crc32c_update(crc32c_start(), &header_bytes);
    let mut remaining = payload_len;
    let mut chunk = [0u8; CORE_BLOCK_SHARD_READ_CHUNK_BYTES];
    while remaining > 0 {
        let chunk_len = usize::try_from(remaining.min(chunk.len() as u64))
            .map_err(|_| corrupt_block_shard("payload chunk length exceeds usize"))?;
        let bytes = &mut chunk[..chunk_len];
        read_block_shard_frame_exact(&mut file, bytes, "payload").await?;
        whole_file_hasher.update(&*bytes);
        payload_hasher.update(&*bytes);
        crc = crc32c_update(crc, bytes);
        if let Some(payload) = payload.as_mut() {
            payload.extend_from_slice(bytes);
        }
        remaining -= chunk_len as u64;
    }
    let actual_payload_hash = format!("sha256:{}", hex::encode(payload_hasher.finalize()));
    if actual_payload_hash != header.payload_stored_hash {
        return Err(corrupt_block_shard("stored payload hash mismatch"));
    }

    let mut expected_crc_bytes = [0u8; 4];
    read_block_shard_frame_exact(&mut file, &mut expected_crc_bytes, "checksum").await?;
    whole_file_hasher.update(expected_crc_bytes);
    let expected_crc = u32::from_le_bytes(expected_crc_bytes);
    if crc32c_finish(crc) != expected_crc {
        return Err(corrupt_block_shard("checksum mismatch"));
    }

    let mut expected_file_hash = [0u8; CORE_BLOCK_SHARD_FILE_HASH_BYTES];
    read_block_shard_frame_exact(&mut file, &mut expected_file_hash, "file hash").await?;
    let actual_file_hash = whole_file_hasher.finalize();
    if expected_file_hash.as_slice() != actual_file_hash.as_slice() {
        return Err(corrupt_block_shard("file hash mismatch"));
    }
    let mut trailing = [0u8; 1];
    match file.read(&mut trailing).await {
        Ok(0) => {}
        Ok(_) => return Err(corrupt_block_shard("trailing bytes")),
        Err(error) => return Err(BlockShardReadFailure::Io(error)),
    }

    Ok(ValidatedBlockShard {
        header,
        shard_index,
        payload,
        file_hash: format!("sha256:{}", hex::encode(actual_file_hash)),
    })
}

async fn read_block_shard_frame_exact(
    file: &mut tokio::fs::File,
    bytes: &mut [u8],
    field: &'static str,
) -> std::result::Result<(), BlockShardReadFailure> {
    file.read_exact(bytes).await.map(|_| ()).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            corrupt_block_shard(format!("frame ended while reading {field}"))
        } else {
            BlockShardReadFailure::Io(error)
        }
    })
}

fn corrupt_block_shard(message: impl std::fmt::Display) -> BlockShardReadFailure {
    BlockShardReadFailure::Corrupt(anyhow!("CoreStore block shard {message}"))
}

fn classify_block_shard_validation_error(error: anyhow::Error) -> BlockShardReadFailure {
    if error.downcast_ref::<BlockShardAllocationError>().is_some() {
        BlockShardReadFailure::Io(io::Error::other(error.to_string()))
    } else {
        BlockShardReadFailure::Corrupt(error)
    }
}

fn validate_stored_block_shard_header(header: &BlockShardHeaderProto) -> Result<u16> {
    if header.schema != CORE_BLOCK_SHARD_HEADER_SCHEMA {
        bail!("CoreStore block shard header has invalid schema");
    }
    for (value, label) in [
        (header.block_id.as_str(), "block_id"),
        (header.erasure_set_id.as_str(), "erasure_set_id"),
        (header.erasure_profile_id.as_str(), "erasure_profile_id"),
        (
            header.created_by_mutation_id.as_str(),
            "created_by_mutation_id",
        ),
    ] {
        if value.is_empty() || value.contains('\0') || value.contains("..") {
            bail!("CoreStore block shard header has invalid {label}");
        }
    }
    let shard_index = u16::try_from(header.shard_index)
        .map_err(|_| anyhow!("CoreStore block shard header shard_index exceeds u16"))?;
    if header.placement_epoch == 0 {
        bail!("CoreStore block shard header placement_epoch must be greater than zero");
    }
    validate_shard_logical_file_id(&header.logical_file_id)?;
    validate_sha256_hash(&header.payload_plain_hash, "block shard plain payload hash")?;
    validate_sha256_hash(
        &header.payload_stored_hash,
        "block shard stored payload hash",
    )?;
    match header.compression.as_str() {
        "none" | "zstd" => {}
        other => bail!("CoreStore block shard header has unknown compression {other}"),
    }
    match header.encryption.as_str() {
        "none" | "aes_gcm_siv" => {}
        other => bail!("CoreStore block shard header has unknown encryption {other}"),
    }
    if WriterFamily::from_name(&header.writer_family).is_none() {
        bail!(
            "CoreStore block shard header has unknown writer_family {}",
            header.writer_family
        );
    }
    validate_boundary_summary_fields(&header.boundary_summary_hash, &header.boundary_values_b64)?;
    Ok(shard_index)
}

pub(super) fn encode_block_shard_repair_record(record: &BlockShardRepairRecord) -> Result<Vec<u8>> {
    validate_block_shard_repair_record(record)?;
    let proto = BlockShardRepairRecordProto {
        schema: CORE_BLOCK_SHARD_REPAIR_RECORD_SCHEMA.to_string(),
        operation_id: record.operation_id.clone(),
        expected_file_present: record.expected_file_present,
        expected_placement_epoch: record.expected_placement_epoch,
        expected_file_hash: record.expected_file_hash.clone(),
        new_placement_epoch: record.new_placement_epoch,
        new_file_hash: record.new_file_hash.clone(),
        receipt: Some(BlockShardRepairReceiptProto {
            node_id: record.receipt.node_id.clone(),
            region_id: record.receipt.region_id.clone(),
            cell_id: record.receipt.cell_id.clone(),
            block_id: record.receipt.block_id.clone(),
            shard_index: u32::from(record.receipt.shard_index),
            shard_hash: record.receipt.shard_hash.clone(),
            shard_length: record.receipt.shard_length,
            fsync_sequence: record.receipt.fsync_sequence,
            written_at_unix_nanos: record.receipt.written_at_unix_nanos,
            signed_payload_hash: record.receipt.signed_payload_hash.clone(),
            signature: record.receipt.signature.clone(),
        }),
    };
    let encoded_len = proto.encoded_len();
    if encoded_len as u64 > CORE_BLOCK_SHARD_MAX_REPAIR_RECORD_BYTES {
        bail!("CoreStore block shard repair record exceeds bounded size");
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(encoded_len).map_err(|_| {
        anyhow!("CoreStore block shard repair record allocation exceeds addressable memory")
    })?;
    proto.encode(&mut bytes)?;
    Ok(bytes)
}

pub(super) async fn read_block_shard_repair_record(
    path: &PathBuf,
) -> Result<Option<BlockShardRepairRecord>> {
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "open CoreStore block shard repair record {}",
                    path.display()
                )
            });
        }
    };
    let file_len = file
        .metadata()
        .await
        .with_context(|| {
            format!(
                "stat CoreStore block shard repair record {}",
                path.display()
            )
        })?
        .len();
    if file_len > CORE_BLOCK_SHARD_MAX_REPAIR_RECORD_BYTES {
        bail!("CoreStore block shard repair record exceeds bounded size");
    }
    let capacity = usize::try_from(file_len)
        .map_err(|_| anyhow!("CoreStore block shard repair record exceeds addressable memory"))?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(capacity).map_err(|_| {
        anyhow!("CoreStore block shard repair record allocation exceeds addressable memory")
    })?;
    file.take(CORE_BLOCK_SHARD_MAX_REPAIR_RECORD_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .with_context(|| {
            format!(
                "read CoreStore block shard repair record {}",
                path.display()
            )
        })?;
    if bytes.len() as u64 > CORE_BLOCK_SHARD_MAX_REPAIR_RECORD_BYTES {
        bail!("CoreStore block shard repair record exceeds bounded size");
    }
    Ok(Some(decode_block_shard_repair_record(&bytes)?))
}

fn decode_block_shard_repair_record(bytes: &[u8]) -> Result<BlockShardRepairRecord> {
    let proto = BlockShardRepairRecordProto::decode(bytes)?;
    if proto.encode_to_vec() != bytes {
        bail!("CoreStore block shard repair record is not deterministic protobuf");
    }
    if proto.schema != CORE_BLOCK_SHARD_REPAIR_RECORD_SCHEMA {
        bail!("CoreStore block shard repair record has invalid schema");
    }
    let receipt = proto
        .receipt
        .ok_or_else(|| anyhow!("CoreStore block shard repair record is missing its receipt"))?;
    let record = BlockShardRepairRecord {
        operation_id: proto.operation_id,
        expected_file_present: proto.expected_file_present,
        expected_placement_epoch: proto.expected_placement_epoch,
        expected_file_hash: proto.expected_file_hash,
        new_placement_epoch: proto.new_placement_epoch,
        new_file_hash: proto.new_file_hash,
        receipt: CoreInternalShardReceipt {
            node_id: receipt.node_id,
            region_id: receipt.region_id,
            cell_id: receipt.cell_id,
            block_id: receipt.block_id,
            shard_index: u16::try_from(receipt.shard_index)
                .map_err(|_| anyhow!("CoreStore block shard repair receipt index exceeds u16"))?,
            shard_hash: receipt.shard_hash,
            shard_length: receipt.shard_length,
            fsync_sequence: receipt.fsync_sequence,
            written_at_unix_nanos: receipt.written_at_unix_nanos,
            signed_payload_hash: receipt.signed_payload_hash,
            signature: receipt.signature,
        },
    };
    validate_block_shard_repair_record(&record)?;
    Ok(record)
}

fn validate_block_shard_repair_record(record: &BlockShardRepairRecord) -> Result<()> {
    if record.operation_id.trim().is_empty() {
        bail!("CoreStore block shard repair operation id is required");
    }
    if record.expected_file_present {
        validate_sha256_hash(
            &record.expected_file_hash,
            "block shard repair expected file hash",
        )?;
    } else if record.expected_placement_epoch != 0 || !record.expected_file_hash.is_empty() {
        bail!("CoreStore repair expectation without a valid shard must use epoch zero");
    }
    if record.new_placement_epoch <= record.expected_placement_epoch {
        bail!("CoreStore block shard repair epoch must advance");
    }
    validate_sha256_hash(&record.new_file_hash, "block shard repair new file hash")?;
    if record.receipt.node_id.trim().is_empty()
        || record.receipt.region_id.trim().is_empty()
        || record.receipt.cell_id.trim().is_empty()
        || record.receipt.block_id.trim().is_empty()
        || record.receipt.fsync_sequence == 0
        || record.receipt.written_at_unix_nanos == 0
        || record.receipt.signature.is_empty()
    {
        bail!("CoreStore block shard repair receipt is incomplete");
    }
    validate_sha256_hash(
        &record.receipt.shard_hash,
        "block shard repair receipt shard hash",
    )?;
    validate_sha256_hash(
        &record.receipt.signed_payload_hash,
        "block shard repair receipt signed payload hash",
    )?;
    Ok(())
}

pub(super) async fn read_block_shard_file(
    path: &PathBuf,
    expectation: BlockShardExpectation<'_>,
    operation: &'static str,
) -> Result<Vec<u8>> {
    let max_payload_bytes = if expectation.payload_len == 0 {
        CORE_BLOCK_SHARD_FALLBACK_MAX_PAYLOAD_BYTES
    } else {
        expectation
            .payload_len
            .min(CORE_BLOCK_SHARD_FALLBACK_MAX_PAYLOAD_BYTES)
    };
    read_block_shard_file_bounded(path, expectation, max_payload_bytes, operation).await
}

pub(super) async fn read_block_shard_file_bounded(
    path: &PathBuf,
    expectation: BlockShardExpectation<'_>,
    max_payload_bytes: u64,
    operation: &'static str,
) -> Result<Vec<u8>> {
    let file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("{operation}: open CoreStore block shard {}", path.display()))?;
    let validated = match read_validated_block_shard(file, max_payload_bytes, true).await {
        Ok(validated) => validated,
        Err(BlockShardReadFailure::Io(error)) => {
            return Err(error).with_context(|| {
                format!("{operation}: read CoreStore block shard {}", path.display())
            });
        }
        Err(BlockShardReadFailure::Corrupt(error)) => {
            return Err(error).with_context(|| {
                format!(
                    "{operation}: validate CoreStore block shard {}",
                    path.display()
                )
            });
        }
    };
    validate_block_shard_header(&validated.header, expectation)?;
    let payload = validated
        .payload
        .ok_or_else(|| anyhow!("CoreStore block shard reader did not retain its payload"))?;
    if expectation.payload_len > 0 && payload.len() as u64 != expectation.payload_len {
        bail!("CoreStore block shard payload length mismatch");
    }
    Ok(payload)
}

fn encode_block_shard_header_proto(header: BlockShardHeaderInput) -> Result<Vec<u8>> {
    let proto = BlockShardHeaderProto {
        schema: CORE_BLOCK_SHARD_HEADER_SCHEMA.to_string(),
        block_id: header.block_id,
        erasure_set_id: header.erasure_set_id,
        shard_index: u32::from(header.shard_index),
        erasure_profile_id: header.erasure_profile_id,
        logical_file_id: header.logical_file_id,
        logical_offset: header.logical_offset,
        logical_length: header.logical_length,
        payload_plain_hash: header.payload_plain_hash,
        payload_stored_hash: header.payload_stored_hash,
        compression: header.compression,
        encryption: header.encryption,
        placement_epoch: header.placement_epoch,
        boundary_summary_hash: header.boundary_summary_hash,
        boundary_values_b64: header.boundary_values_b64,
        writer_family: header.writer_family,
        created_by_mutation_id: header.created_by_mutation_id,
    };
    let encoded_len = proto.encoded_len();
    if encoded_len > CORE_BLOCK_SHARD_MAX_HEADER_BYTES {
        bail!("CoreStore block shard header exceeds bounded size");
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(encoded_len).map_err(|_| {
        anyhow!("CoreStore block shard header allocation exceeds addressable memory")
    })?;
    proto.encode(&mut bytes)?;
    Ok(bytes)
}

fn decode_block_shard_header_proto(bytes: &[u8]) -> Result<BlockShardHeaderProto> {
    if bytes.len() > CORE_BLOCK_SHARD_MAX_HEADER_BYTES {
        bail!("CoreStore block shard header exceeds bounded size");
    }
    let header = BlockShardHeaderProto::decode(bytes)?;
    let encoded_len = header.encoded_len();
    if encoded_len > CORE_BLOCK_SHARD_MAX_HEADER_BYTES {
        bail!("CoreStore block shard header exceeds bounded size");
    }
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(encoded_len)
        .map_err(|_| BlockShardAllocationError("canonical header"))?;
    header.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("CoreStore block shard header is not deterministic protobuf");
    }
    Ok(header)
}

fn validate_block_shard_header(
    header: &BlockShardHeaderProto,
    expectation: BlockShardExpectation<'_>,
) -> Result<()> {
    if header.schema != CORE_BLOCK_SHARD_HEADER_SCHEMA {
        bail!("CoreStore block shard header has invalid schema");
    }
    if header.block_id != expectation.block_id {
        bail!(
            "CoreStore block shard header block_id mismatch: expected {}, got {}",
            expectation.block_id,
            header.block_id
        );
    }
    if header.shard_index != u32::from(expectation.shard_index) {
        bail!(
            "CoreStore block shard header shard_index mismatch: expected {}, got {}",
            expectation.shard_index,
            header.shard_index
        );
    }
    if header.erasure_profile_id != expectation.erasure_profile_id {
        bail!(
            "CoreStore block shard header erasure_profile_id mismatch: expected {}, got {}",
            expectation.erasure_profile_id,
            header.erasure_profile_id
        );
    }
    validate_shard_logical_file_id(&header.logical_file_id)?;
    if WriterFamily::from_name(&header.writer_family).is_none() {
        bail!(
            "CoreStore block shard header has unknown writer_family {}",
            header.writer_family
        );
    }
    if header.placement_epoch != expectation.placement_epoch {
        bail!(
            "CoreStore block shard header placement_epoch mismatch: expected {}, got {}",
            expectation.placement_epoch,
            header.placement_epoch
        );
    }
    if header.payload_stored_hash != expectation.payload_hash {
        bail!(
            "CoreStore block shard header payload_stored_hash mismatch: expected {}, got {}",
            expectation.payload_hash,
            header.payload_stored_hash
        );
    }
    if header.payload_plain_hash != expectation.payload_hash {
        bail!(
            "CoreStore block shard header payload_plain_hash mismatch: expected {}, got {}",
            expectation.payload_hash,
            header.payload_plain_hash
        );
    }
    validate_boundary_summary_fields(&header.boundary_summary_hash, &header.boundary_values_b64)?;
    if let Some(expected) = expectation.boundary_summary_hash
        && header.boundary_summary_hash != expected
    {
        bail!(
            "CoreStore block shard header boundary_summary_hash mismatch: expected {}, got {}",
            expected,
            header.boundary_summary_hash
        );
    }
    if let Some(expected) = expectation.boundary_values_b64
        && header.boundary_values_b64 != expected
    {
        bail!("CoreStore block shard header boundary_values_b64 mismatch");
    }
    if expectation.payload_len > 0 && header.logical_length != expectation.payload_len {
        bail!(
            "CoreStore block shard header logical_length mismatch: expected {}, got {}",
            expectation.payload_len,
            header.logical_length
        );
    }
    Ok(())
}

fn validate_shard_logical_file_id(value: &str) -> Result<()> {
    let Some(hash) = value.strip_prefix("lf_") else {
        bail!("CoreStore block shard logical_file_id must use canonical lf_ prefix");
    };
    if hash.len() != 64 || !hash.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!(
            "CoreStore block shard logical_file_id must be lf_ followed by a 64 byte hex digest: got {value:?} (digest chars {})",
            hash.len()
        );
    }
    Ok(())
}

pub(super) fn boundary_summary_hash(boundary_values: &[CoreBoundaryValue]) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        sha256_hex(&encode_boundary_values_record(boundary_values)?)
    ))
}

pub(super) fn encode_boundary_values_b64(boundary_values: &[CoreBoundaryValue]) -> Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(encode_boundary_values_record(boundary_values)?))
}

pub(super) fn validate_boundary_summary_fields(
    boundary_summary_hash: &str,
    boundary_values_b64: &str,
) -> Result<()> {
    validate_sha256_hash(boundary_summary_hash, "block shard boundary summary hash")?;
    if boundary_values_b64.len() > CORE_BLOCK_SHARD_MAX_HEADER_BYTES {
        bail!("CoreStore block shard boundary values exceed bounded size");
    }
    let bytes = if boundary_values_b64.is_empty() {
        Vec::new()
    } else {
        URL_SAFE_NO_PAD
            .decode(boundary_values_b64)
            .context("decode CoreStore block shard boundary values")?
    };
    let decoded = BoundaryValuesProto::decode(bytes.as_slice())?;
    let canonical = decoded.encode_to_vec();
    if canonical != bytes {
        bail!("CoreStore block shard boundary values are not deterministic protobuf");
    }
    let actual = format!("sha256:{}", sha256_hex(&canonical));
    if actual != boundary_summary_hash {
        bail!("CoreStore block shard boundary summary hash mismatch");
    }
    Ok(())
}

pub(super) fn encode_boundary_values_record(
    boundary_values: &[CoreBoundaryValue],
) -> Result<Vec<u8>> {
    let proto = BoundaryValuesProto {
        values: boundary_values
            .iter()
            .map(|value| BoundaryValueProto {
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
            })
            .collect(),
    };
    let encoded_len = proto.encoded_len();
    if encoded_len > CORE_BLOCK_SHARD_MAX_HEADER_BYTES {
        bail!("CoreStore block shard boundary values exceed bounded size");
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(encoded_len)
        .map_err(|_| anyhow!("CoreStore boundary value allocation exceeds addressable memory"))?;
    proto.encode(&mut bytes)?;
    let decoded = BoundaryValuesProto::decode(bytes.as_slice())?;
    if decoded.encode_to_vec() != bytes {
        bail!("CoreStore block shard boundary values are not deterministic protobuf");
    }
    Ok(bytes)
}

fn descriptor_hash(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn validate_sha256_hash(hash: &str, label: &str) -> Result<()> {
    let Some(value) = hash.strip_prefix("sha256:") else {
        bail!("CoreStore {label} must have sha256: prefix");
    };
    if value.len() != 64 || !value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("CoreStore {label} must be a sha256 hash");
    }
    Ok(())
}

fn write_u32_le(out: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u32::try_from(value).map_err(|_| anyhow!("CoreStore frame length exceeds u32"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn crc32c_start() -> u32 {
    !0u32
}

fn crc32c_update(mut crc: u32, bytes: &[u8]) -> u32 {
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0x82f6_3b78;
            } else {
                crc >>= 1;
            }
        }
    }
    crc
}

fn crc32c_finish(crc: u32) -> u32 {
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_record_round_trip_preserves_original_receipt() {
        let record = BlockShardRepairRecord {
            operation_id: "repair-finding-1".to_string(),
            expected_file_present: true,
            expected_placement_epoch: 4,
            expected_file_hash:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            new_placement_epoch: 5,
            new_file_hash:
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_string(),
            receipt: CoreInternalShardReceipt {
                node_id: "node-a".to_string(),
                region_id: "region-a".to_string(),
                cell_id: "cell-a".to_string(),
                block_id: "blk_object".to_string(),
                shard_index: 2,
                shard_hash:
                    "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                        .to_string(),
                shard_length: 1024,
                fsync_sequence: 1,
                written_at_unix_nanos: 123_456,
                signed_payload_hash:
                    "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                        .to_string(),
                signature: vec![1, 2, 3, 4],
            },
        };

        let encoded = encode_block_shard_repair_record(&record).unwrap();
        let decoded = decode_block_shard_repair_record(&encoded).unwrap();

        assert_eq!(decoded, record);
        assert_eq!(encode_block_shard_repair_record(&decoded).unwrap(), encoded);
    }

    #[tokio::test]
    async fn stored_state_probe_classifies_absent_valid_and_crc_corrupt() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shard.anb");
        assert_eq!(
            read_block_shard_stored_state_bounded(&path, 1024)
                .await
                .unwrap(),
            BlockShardStoredState::Absent
        );

        let mut encoded = test_block_shard_file(b"canonical shard", 7);
        tokio::fs::write(&path, &encoded).await.unwrap();
        let expected_file_hash = block_shard_file_hash(&encoded).unwrap();
        assert_eq!(
            read_block_shard_stored_state_bounded(&path, 1024)
                .await
                .unwrap(),
            BlockShardStoredState::Valid(BlockShardValidState {
                block_id: "blk_test".to_string(),
                shard_index: 2,
                placement_epoch: 7,
                file_hash: expected_file_hash,
            })
        );

        // Reseal the whole-file hash after damaging the checksum so CRC must fail first.
        let checksum_offset = encoded.len() - CORE_BLOCK_SHARD_FILE_HASH_BYTES - 4;
        encoded[checksum_offset] ^= 0x80;
        let file_hash_offset = encoded.len() - CORE_BLOCK_SHARD_FILE_HASH_BYTES;
        let resealed_hash = Sha256::digest(&encoded[..file_hash_offset]);
        encoded[file_hash_offset..].copy_from_slice(&resealed_hash);
        tokio::fs::write(&path, &encoded).await.unwrap();
        assert_eq!(
            read_block_shard_stored_state_bounded(&path, 1024)
                .await
                .unwrap(),
            BlockShardStoredState::Corrupt
        );
    }

    #[tokio::test]
    async fn stored_state_probe_verifies_whole_file_hash_and_payload_bound() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shard.anb");
        let mut encoded = test_block_shard_file(b"bounded shard", 9);

        let last_hash_byte = encoded.len() - 1;
        encoded[last_hash_byte] ^= 0x01;
        tokio::fs::write(&path, &encoded).await.unwrap();
        assert_eq!(
            read_block_shard_stored_state_bounded(&path, 1024)
                .await
                .unwrap(),
            BlockShardStoredState::Corrupt
        );

        let mut oversized = test_block_shard_file(b"bounded shard", 9);
        let header_len = u32::from_le_bytes(oversized[10..14].try_into().unwrap()) as usize;
        let payload_len_offset = 14 + header_len;
        oversized[payload_len_offset..payload_len_offset + 8]
            .copy_from_slice(&1025u64.to_le_bytes());
        tokio::fs::write(&path, &oversized).await.unwrap();
        assert_eq!(
            read_block_shard_stored_state_bounded(&path, 1024)
                .await
                .unwrap(),
            BlockShardStoredState::Corrupt
        );
    }

    #[tokio::test]
    async fn stored_state_probe_does_not_classify_io_failures_as_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let directory_path = directory.path().to_path_buf();

        assert!(
            read_block_shard_stored_state_bounded(&directory_path, 1024)
                .await
                .is_err()
        );
    }

    fn test_block_shard_file(payload: &[u8], placement_epoch: u64) -> Vec<u8> {
        let payload_hash = format!("sha256:{}", sha256_hex(payload));
        encode_block_shard_file(
            BlockShardHeaderInput {
                block_id: "blk_test".to_string(),
                erasure_set_id: "local-erasure-set".to_string(),
                shard_index: 2,
                erasure_profile_id: "ec-4-2".to_string(),
                logical_file_id:
                    "lf_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                logical_offset: 0,
                logical_length: payload.len() as u64,
                payload_plain_hash: payload_hash.clone(),
                payload_stored_hash: payload_hash,
                compression: "none".to_string(),
                encryption: "none".to_string(),
                placement_epoch,
                boundary_summary_hash: boundary_summary_hash(&[]).unwrap(),
                boundary_values_b64: encode_boundary_values_b64(&[]).unwrap(),
                writer_family: WriterFamily::ObjectBlob.as_str().to_string(),
                created_by_mutation_id: "mutation-test".to_string(),
            },
            payload,
        )
        .unwrap()
    }
}
