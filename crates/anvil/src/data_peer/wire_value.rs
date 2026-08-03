//! Small conversions shared by the data-peer server and client transport.

use anvil_consensus::ClusterId;
use anvil_store::{BlobRef, FRAGMENT_FORMAT_VERSION, ShardIdentity};
use tonic::Status;

use super::{DATA_PEER_SCHEMA_VERSION, wire};

pub(super) fn parse_cluster_id(encoded: &[u8]) -> Result<ClusterId, Status> {
    let bytes = encoded
        .try_into()
        .map_err(|_| Status::invalid_argument("cluster id must contain exactly 16 bytes"))?;
    Ok(ClusterId(bytes))
}

pub(super) fn require_response_schema(schema_version: u32) -> Result<(), Status> {
    if schema_version != DATA_PEER_SCHEMA_VERSION {
        return Err(Status::failed_precondition(format!(
            "peer returned unsupported data-peer schema {schema_version}"
        )));
    }
    Ok(())
}

pub(super) fn wire_blob(reference: &BlobRef) -> wire::BlobIdentity {
    wire::BlobIdentity {
        blake3: reference.hash.to_vec(),
        length: reference.length,
    }
}

#[allow(
    dead_code,
    reason = "used by the typed shard client when distributed payload orchestration is connected"
)]
pub(super) fn wire_shard(
    context: wire::PeerContext,
    identity: &ShardIdentity,
) -> wire::ShardRequest {
    wire::ShardRequest {
        peer: Some(context),
        fragment_format_version: u32::from(identity.fragment_format_version()),
        blob: Some(wire_blob(identity.blob())),
        ordinal: u32::from(identity.ordinal()),
    }
}

pub(super) fn parse_blob(value: Option<&wire::BlobIdentity>) -> Result<BlobRef, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("blob identity is required"))?;
    let hash =
        value.blake3.as_slice().try_into().map_err(|_| {
            Status::invalid_argument("BLAKE3 identity must contain exactly 32 bytes")
        })?;
    Ok(BlobRef {
        hash,
        length: value.length,
    })
}

pub(super) fn parse_small_blob(value: Option<&wire::BlobIdentity>) -> Result<BlobRef, Status> {
    let reference = parse_blob(value)?;
    if reference.length > anvil_store::SMALL_BLOB_MAX_BYTES as u64 {
        return Err(Status::invalid_argument(
            "content identity is not a small blob",
        ));
    }
    Ok(reference)
}

pub(super) fn parse_shard(value: &wire::ShardRequest) -> Result<ShardIdentity, Status> {
    let fragment_format_version = u16::try_from(value.fragment_format_version)
        .map_err(|_| Status::invalid_argument("fragment format does not fit u16"))?;
    if fragment_format_version != FRAGMENT_FORMAT_VERSION {
        return Err(Status::failed_precondition(format!(
            "unsupported fragment format {fragment_format_version}"
        )));
    }
    let ordinal = u16::try_from(value.ordinal)
        .map_err(|_| Status::invalid_argument("shard ordinal does not fit u16"))?;
    Ok(ShardIdentity::new(
        parse_blob(value.blob.as_ref())?,
        ordinal,
    ))
}

pub(super) fn content_frame(offset: u64, content: Vec<u8>) -> wire::ContentFrame {
    wire::ContentFrame {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        offset,
        content,
        end: false,
    }
}

pub(super) fn content_end(offset: u64) -> wire::ContentFrame {
    wire::ContentFrame {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        offset,
        content: Vec::new(),
        end: true,
    }
}
