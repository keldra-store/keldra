use crate::anvil_api::{
    CoreMetaBatchGroupRequest, CoreMetaPersistCommitGroupRequest, InternalRequestHeader,
    PublishPendingMutationFinalisationRequest, PutShardRequest, RepairShardRequest,
};
use crate::core_store::{self, CoreByteRange};
use crate::formats::writer::WriterFamily;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use prost::Message;
use tonic::Status;

pub(super) const MAX_INTERNAL_RPC_REQUEST_BYTES: usize = 64 * 1024 * 1024;
pub(super) const MAX_INTERNAL_HEADER_FIELD_BYTES: usize = 1024;
pub(super) const MAX_REPAIR_FINDING_JSON_BYTES: usize = 1024 * 1024;
pub(super) const MAX_PROXY_NATIVE_BODY_BYTES: usize = 16 * 1024 * 1024;
pub(super) const MAX_PROXY_AUTHZ_CONTEXT_BYTES: usize = 64 * 1024;

pub(super) const MAX_COREMETA_HISTORY_PAGE_ROWS: usize = 4096;
const MIN_COREMETA_HISTORY_PAGE_BYTES: u64 = 128 * 1024;
pub(super) const MAX_COREMETA_HISTORY_PAGE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_COREMETA_GROUP_ROOTS: usize = 4096;
const MAX_COREMETA_GROUP_ROWS: usize = 65_536;
const MAX_COREMETA_RECEIPTS_PER_ROOT: usize = 16;
const MAX_INTERNAL_HEADER_SIGNATURE_BYTES: usize = 16 * 1024;
const MAX_INTERNAL_IDENTITY_BYTES: usize = 1024;
const MAX_INTERNAL_SHARD_BYTES: usize = 16 * 1024 * 1024;
const MAX_INTERNAL_SHARD_CONTEXT_BYTES: usize = 16 * 1024 * 1024;
const MAX_REPAIR_FINDING_ID_BYTES: usize = 256;
const MAX_PENDING_FINALISATION_RECORD_BYTES: usize = 16 * 1024 * 1024;

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

#[derive(Clone, Copy)]
struct ShardWriteRequest<'a> {
    block_id: &'a str,
    shard_index: u32,
    placement_epoch: u64,
    shard_bytes: &'a [u8],
    shard_hash: &'a str,
    erasure_profile_id: &'a str,
    boundary_summary_hash: &'a str,
    boundary_values_b64: &'a str,
    writer_family: &'a str,
    mutation_id: &'a str,
    logical_file_id: &'a str,
    compression_algorithm: &'a str,
    encryption_algorithm: &'a str,
}

pub(super) fn validate_put_shard_request(request: &PutShardRequest) -> Result<(), Status> {
    validate_shard_write_request(ShardWriteRequest {
        block_id: &request.block_id,
        shard_index: request.shard_index,
        placement_epoch: request.placement_epoch,
        shard_bytes: &request.shard_bytes,
        shard_hash: &request.shard_hash,
        erasure_profile_id: &request.erasure_profile_id,
        boundary_summary_hash: &request.boundary_summary_hash,
        boundary_values_b64: &request.boundary_values_b64,
        writer_family: &request.writer_family,
        mutation_id: &request.mutation_id,
        logical_file_id: &request.logical_file_id,
        compression_algorithm: &request.compression_algorithm,
        encryption_algorithm: &request.encryption_algorithm,
    })
}

pub(super) fn validate_repair_shard_request(request: &RepairShardRequest) -> Result<(), Status> {
    validate_shard_write_request(ShardWriteRequest {
        block_id: &request.block_id,
        shard_index: request.shard_index,
        placement_epoch: request.placement_epoch,
        shard_bytes: &request.shard_bytes,
        shard_hash: &request.shard_hash,
        erasure_profile_id: &request.erasure_profile_id,
        boundary_summary_hash: &request.boundary_summary_hash,
        boundary_values_b64: &request.boundary_values_b64,
        writer_family: &request.writer_family,
        mutation_id: &request.mutation_id,
        logical_file_id: &request.logical_file_id,
        compression_algorithm: &request.compression_algorithm,
        encryption_algorithm: &request.encryption_algorithm,
    })?;
    validate_repair_finding_id(&request.repair_finding_id)
}

fn validate_shard_write_request(request: ShardWriteRequest<'_>) -> Result<(), Status> {
    ensure_len_at_most(
        request.shard_bytes.len(),
        MAX_INTERNAL_SHARD_BYTES,
        "shard_bytes",
    )?;
    ensure_len_at_most(
        request.boundary_values_b64.len(),
        MAX_INTERNAL_SHARD_CONTEXT_BYTES,
        "boundary_values_b64",
    )?;
    for (value, label) in [
        (request.block_id, "block_id"),
        (request.erasure_profile_id, "erasure_profile_id"),
        (request.writer_family, "writer_family"),
        (request.logical_file_id, "logical_file_id"),
        (request.compression_algorithm, "compression_algorithm"),
        (request.encryption_algorithm, "encryption_algorithm"),
    ] {
        ensure_bounded_identity(value, label)?;
    }
    if !request.mutation_id.is_empty() {
        ensure_bounded_identity(request.mutation_id, "mutation_id")?;
    }
    validate_logical_file_id(request.logical_file_id)?;
    validate_sha256_hash(request.shard_hash, "shard_hash")?;
    validate_boundary_context(request.boundary_summary_hash, request.boundary_values_b64)?;
    let total_shards = profile_total_shards(request.erasure_profile_id)?;
    if usize::try_from(request.shard_index).map_or(true, |index| index >= total_shards) {
        return Err(Status::invalid_argument(
            "shard_index exceeds erasure profile shard count",
        ));
    }
    if request.placement_epoch == 0 {
        return Err(Status::invalid_argument(
            "placement_epoch must be greater than zero",
        ));
    }
    if WriterFamily::from_name(request.writer_family).is_none() {
        return Err(Status::invalid_argument("writer_family is not registered"));
    }
    if !matches!(request.compression_algorithm, "none" | "zstd") {
        return Err(Status::invalid_argument(
            "compression_algorithm is not supported",
        ));
    }
    if !matches!(request.encryption_algorithm, "none" | "aes_gcm_siv") {
        return Err(Status::invalid_argument(
            "encryption_algorithm is not supported",
        ));
    }
    let actual_hash = format!("sha256:{}", core_store::sha256_hex(request.shard_bytes));
    if actual_hash != request.shard_hash {
        return Err(Status::invalid_argument(
            "shard_bytes do not match shard_hash",
        ));
    }
    Ok(())
}

pub(super) fn validate_shard_read_scope(
    block_id: &str,
    shard_index: u32,
    erasure_profile_id: &str,
    shard_hash: &str,
    boundary_summary_hash: &str,
) -> Result<(), Status> {
    ensure_bounded_identity(block_id, "block_id")?;
    ensure_bounded_identity(erasure_profile_id, "erasure_profile_id")?;
    validate_sha256_hash(shard_hash, "shard_hash")?;
    if !boundary_summary_hash.is_empty() {
        validate_sha256_hash(boundary_summary_hash, "boundary_summary_hash")?;
    }
    let total_shards = profile_total_shards(erasure_profile_id)?;
    if usize::try_from(shard_index).map_or(true, |index| index >= total_shards) {
        return Err(Status::invalid_argument(
            "shard_index exceeds erasure profile shard count",
        ));
    }
    Ok(())
}

pub(super) fn bounded_shard_range(
    start: u64,
    end_exclusive: u64,
) -> Result<Option<CoreByteRange>, Status> {
    if start == 0 && end_exclusive == 0 {
        return Ok(None);
    }
    if start > end_exclusive {
        return Err(Status::invalid_argument("shard range start exceeds end"));
    }
    if end_exclusive - start > MAX_INTERNAL_SHARD_BYTES as u64 {
        return Err(Status::invalid_argument(
            "shard range exceeds bounded shard size",
        ));
    }
    Ok(Some(CoreByteRange {
        start,
        end_exclusive,
    }))
}

pub(super) fn repair_mutation_id(request: &RepairShardRequest) -> Result<String, Status> {
    if !request.mutation_id.is_empty() {
        ensure_bounded_identity(&request.mutation_id, "mutation_id")?;
        return Ok(request.mutation_id.clone());
    }
    validate_repair_finding_id(&request.repair_finding_id)?;

    let mut identity = Vec::new();
    for part in [
        b"anvil.internal.repair.mutation.v1".as_slice(),
        request.repair_finding_id.as_bytes(),
        request.logical_file_id.as_bytes(),
        request.block_id.as_bytes(),
        request.erasure_profile_id.as_bytes(),
        request.shard_hash.as_bytes(),
        request.boundary_summary_hash.as_bytes(),
        request.compression_algorithm.as_bytes(),
        request.encryption_algorithm.as_bytes(),
        request.writer_family.as_bytes(),
    ] {
        append_identity_component(&mut identity, part);
    }
    append_identity_component(&mut identity, &request.shard_index.to_be_bytes());
    append_identity_component(&mut identity, &request.placement_epoch.to_be_bytes());
    append_identity_component(&mut identity, &request.logical_offset.to_be_bytes());
    Ok(format!("repair-{}", core_store::sha256_hex(&identity)))
}

fn append_identity_component(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(&(value.len() as u64).to_be_bytes());
    output.extend_from_slice(value);
}

fn validate_boundary_context(summary_hash: &str, encoded: &str) -> Result<(), Status> {
    validate_sha256_hash(summary_hash, "boundary_summary_hash")?;
    let bytes = if encoded.is_empty() {
        Vec::new()
    } else {
        URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| Status::invalid_argument("boundary_values_b64 is invalid base64"))?
    };
    let values = BoundaryValuesProto::decode(bytes.as_slice())
        .map_err(|_| Status::invalid_argument("boundary_values_b64 is invalid protobuf"))?;
    let canonical = values.encode_to_vec();
    if canonical != bytes {
        return Err(Status::invalid_argument(
            "boundary_values_b64 is not deterministic protobuf",
        ));
    }
    let actual = format!("sha256:{}", core_store::sha256_hex(&canonical));
    if actual != summary_hash {
        return Err(Status::invalid_argument(
            "boundary_values_b64 does not match boundary_summary_hash",
        ));
    }
    Ok(())
}

fn validate_logical_file_id(value: &str) -> Result<(), Status> {
    let Some(digest) = value.strip_prefix("lf_") else {
        return Err(Status::invalid_argument(
            "logical_file_id must use the canonical lf_ prefix",
        ));
    };
    if digest.len() != 64 || !digest.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        return Err(Status::invalid_argument(
            "logical_file_id must contain a 64 character hex digest",
        ));
    }
    Ok(())
}

fn validate_sha256_hash(value: &str, label: &str) -> Result<(), Status> {
    let Some(digest) = value.strip_prefix("sha256:") else {
        return Err(Status::invalid_argument(format!("{label} must use sha256")));
    };
    if digest.len() != 64 || !digest.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        return Err(Status::invalid_argument(format!(
            "{label} must contain a 64 character hex digest"
        )));
    }
    Ok(())
}

fn profile_total_shards(profile: &str) -> Result<usize, Status> {
    match profile {
        "ec-4-2" => Ok(6),
        "ec-8-3" => Ok(11),
        "replicated-3" => Ok(3),
        _ => Err(Status::invalid_argument(
            "erasure_profile_id is not supported",
        )),
    }
}

pub(super) fn validate_repair_finding_id(finding_id: &str) -> Result<(), Status> {
    if finding_id.is_empty()
        || finding_id.len() > MAX_REPAIR_FINDING_ID_BYTES
        || finding_id.trim() != finding_id
        || finding_id.contains('/')
        || finding_id.contains('\\')
        || finding_id.chars().any(char::is_control)
    {
        return Err(Status::invalid_argument(
            "repair finding id is not a safe bounded identity",
        ));
    }
    Ok(())
}

pub(super) fn validate_coremeta_batch_group_bounds(
    request: &CoreMetaBatchGroupRequest,
) -> Result<(), Status> {
    ensure_count_at_most(
        request.batches.len(),
        MAX_COREMETA_GROUP_ROOTS,
        "CoreMeta batch group roots",
    )?;
    ensure_len_at_most(
        request.publication_intent.len(),
        MAX_COREMETA_HISTORY_PAGE_BYTES as usize,
        "CoreMeta publication_intent",
    )?;
    let rows = request.batches.iter().try_fold(0usize, |total, batch| {
        total
            .checked_add(batch.mutations.len())
            .ok_or_else(|| Status::invalid_argument("CoreMeta batch group mutation count overflow"))
    })?;
    ensure_count_at_most(rows, MAX_COREMETA_GROUP_ROWS, "CoreMeta batch group rows")
}

pub(super) fn validate_coremeta_commit_group_bounds(
    request: &CoreMetaPersistCommitGroupRequest,
) -> Result<(), Status> {
    ensure_count_at_most(
        request.commits.len(),
        MAX_COREMETA_GROUP_ROOTS,
        "CoreMeta commit group roots",
    )?;
    let rows = request.commits.iter().try_fold(0usize, |total, commit| {
        if let Some(certificate) = &commit.commit_certificate {
            ensure_count_at_most(
                certificate.prepare_receipts.len(),
                MAX_COREMETA_RECEIPTS_PER_ROOT,
                "CoreMeta prepare receipts",
            )?;
        }
        total
            .checked_add(commit.committed_rows.len())
            .ok_or_else(|| {
                Status::invalid_argument("CoreMeta commit group mutation count overflow")
            })
    })?;
    ensure_count_at_most(rows, MAX_COREMETA_GROUP_ROWS, "CoreMeta commit group rows")
}

pub(super) fn validate_pending_mutation_finalisation_request(
    request: &PublishPendingMutationFinalisationRequest,
) -> Result<(), Status> {
    if request.finalisation_record.is_empty() {
        return Err(Status::invalid_argument(
            "pending mutation finalisation record is required",
        ));
    }
    ensure_len_at_most(
        request.finalisation_record.len(),
        MAX_PENDING_FINALISATION_RECORD_BYTES,
        "pending mutation finalisation record",
    )?;
    validate_sha256_hash(
        &request.payload_hash,
        "pending mutation finalisation payload_hash",
    )?;
    let expected = core_store::CoreStore::pending_mutation_finalisation_rpc_payload_hash(
        &request.finalisation_record,
    );
    if request.payload_hash != expected {
        return Err(Status::invalid_argument(
            "pending mutation finalisation payload_hash mismatch",
        ));
    }
    if request.source_signature.is_empty() {
        return Err(Status::invalid_argument(
            "pending mutation finalisation source signature is required",
        ));
    }
    ensure_len_at_most(
        request.source_signature.len(),
        MAX_INTERNAL_HEADER_SIGNATURE_BYTES,
        "pending mutation finalisation source signature",
    )
}

pub(super) fn bounded_coremeta_history_page(
    rows: u32,
    bytes: u64,
    label: &str,
) -> Result<usize, Status> {
    let rows = usize::try_from(rows)
        .map_err(|_| Status::invalid_argument(format!("{label} rows exceed usize")))?;
    if !(1..=MAX_COREMETA_HISTORY_PAGE_ROWS).contains(&rows) {
        return Err(Status::invalid_argument(format!(
            "{label} rows must be between 1 and {MAX_COREMETA_HISTORY_PAGE_ROWS}"
        )));
    }
    if !(MIN_COREMETA_HISTORY_PAGE_BYTES..=MAX_COREMETA_HISTORY_PAGE_BYTES).contains(&bytes) {
        return Err(Status::invalid_argument(format!(
            "{label} bytes must be between {MIN_COREMETA_HISTORY_PAGE_BYTES} and {MAX_COREMETA_HISTORY_PAGE_BYTES}"
        )));
    }
    Ok(rows)
}

pub(super) fn bounded_root_directory_page(entries: u32, bytes: u64) -> Result<usize, Status> {
    let entries = usize::try_from(entries)
        .map_err(|_| Status::invalid_argument("root-directory entries exceed usize"))?;
    if !(1..=MAX_COREMETA_HISTORY_PAGE_ROWS).contains(&entries) {
        return Err(Status::invalid_argument(format!(
            "root-directory entries must be between 1 and {MAX_COREMETA_HISTORY_PAGE_ROWS}"
        )));
    }
    if !(1..=MAX_COREMETA_HISTORY_PAGE_BYTES).contains(&bytes) {
        return Err(Status::invalid_argument(format!(
            "root-directory bytes must be between 1 and {MAX_COREMETA_HISTORY_PAGE_BYTES}"
        )));
    }
    Ok(entries)
}

pub(super) fn ensure_message_size<M: Message>(
    message: &M,
    maximum: usize,
    label: &str,
) -> Result<(), Status> {
    ensure_len_at_most(message.encoded_len(), maximum, label)
}

pub(super) fn ensure_len_at_most(actual: usize, maximum: usize, label: &str) -> Result<(), Status> {
    if actual > maximum {
        return Err(Status::invalid_argument(format!(
            "{label} must not exceed {maximum} bytes"
        )));
    }
    Ok(())
}

fn ensure_count_at_most(actual: usize, maximum: usize, label: &str) -> Result<(), Status> {
    if actual > maximum {
        return Err(Status::invalid_argument(format!(
            "{label} must not exceed {maximum} entries"
        )));
    }
    Ok(())
}

pub(super) fn ensure_bounded_identity(value: &str, label: &str) -> Result<(), Status> {
    if value.is_empty()
        || value.len() > MAX_INTERNAL_IDENTITY_BYTES
        || value.trim() != value
        || value.contains('\0')
        || value.contains("..")
        || value.chars().any(char::is_control)
    {
        return Err(Status::invalid_argument(format!(
            "{label} is not a safe bounded identity"
        )));
    }
    Ok(())
}

pub(super) fn validate_internal_header_bounds(
    header: &InternalRequestHeader,
) -> Result<(), Status> {
    for (value, label) in [
        (&header.request_id, "internal request_id"),
        (&header.trace_id, "internal trace_id"),
        (&header.source_node_id, "internal source_node_id"),
    ] {
        ensure_len_at_most(value.len(), MAX_INTERNAL_HEADER_FIELD_BYTES, label)?;
        if value.chars().any(char::is_control) {
            return Err(Status::invalid_argument(format!(
                "{label} contains control characters"
            )));
        }
    }
    ensure_len_at_most(
        header.signature.len(),
        MAX_INTERNAL_HEADER_SIGNATURE_BYTES,
        "internal header signature",
    )
}

pub(super) fn request_id_from_header(header: Option<&InternalRequestHeader>) -> String {
    header
        .map(|header| header.request_id.clone())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
}

pub(super) fn internal_status(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("{error:#}"))
}

pub(super) fn core_store_internal_status(error: anyhow::Error) -> Status {
    if let Some(status) = crate::services::core_store_status::availability_status(&error) {
        status
    } else {
        internal_status(error)
    }
}

pub(super) fn repair_shard_status(error: anyhow::Error) -> Status {
    let message = format!("{error:#}");
    if core_store::CoreStore::is_internal_shard_repair_precondition(&error)
        || message.contains("CoreStore internal shard repair precondition failed")
    {
        Status::failed_precondition(message)
    } else if let Some(status) = crate::services::core_store_status::availability_status(&error) {
        status
    } else if is_repair_invalid_argument(&error) {
        Status::invalid_argument(message)
    } else if is_repair_data_loss(&error) {
        Status::data_loss(message)
    } else {
        Status::internal(message)
    }
}

fn is_repair_invalid_argument(error: &anyhow::Error) -> bool {
    const MARKERS: &[&str] = &[
        "internal shard block id",
        "internal shard logical file id",
        "internal shard writer family",
        "internal shard hash",
        "block shard boundary",
        "unsupported object blob",
        "unsupported erasure profile",
        "internal shard index exceeds",
        "internal shard repair operation id is required",
        "block shard header exceeds bounded size",
    ];
    error.chain().any(|cause| {
        let message = cause.to_string().to_ascii_lowercase();
        MARKERS.iter().any(|marker| message.contains(marker))
    })
}

fn is_repair_data_loss(error: &anyhow::Error) -> bool {
    const MARKERS: &[&str] = &[
        "block shard has invalid magic",
        "block shard has unsupported version",
        "block shard frame ended unexpectedly",
        "block shard logical length mismatch",
        "block shard file length mismatch",
        "block shard repair record",
        "internal shard write found a mismatched shard scope",
        "internal shard repair found a mismatched shard scope",
        "internal shard repair record",
        "receipt signature verification failed",
    ];
    error.chain().any(|cause| {
        if cause.downcast_ref::<prost::DecodeError>().is_some() {
            return true;
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>()
            && matches!(
                io.kind(),
                std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
            )
        {
            return true;
        }
        let message = cause.to_string().to_ascii_lowercase();
        MARKERS.iter().any(|marker| message.contains(marker))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_mutation_identity_ignores_transport_request_id() {
        let mut first = repair_request();
        first.header = Some(InternalRequestHeader {
            request_id: "request-a".to_string(),
            ..Default::default()
        });
        let mut retry = first.clone();
        retry.header.as_mut().unwrap().request_id = "request-b".to_string();

        assert_eq!(
            repair_mutation_id(&first).unwrap(),
            repair_mutation_id(&retry).unwrap()
        );

        retry.shard_index += 1;
        assert_ne!(
            repair_mutation_id(&first).unwrap(),
            repair_mutation_id(&retry).unwrap()
        );
    }

    #[test]
    fn explicit_repair_mutation_identity_is_preserved() {
        let mut request = repair_request();
        request.mutation_id = "explicit-repair-mutation".to_string();

        assert_eq!(
            repair_mutation_id(&request).unwrap(),
            "explicit-repair-mutation"
        );
    }

    #[test]
    fn repair_status_distinguishes_client_state_corruption_and_availability() {
        let invalid = repair_shard_status(anyhow::anyhow!(
            "CoreStore unsupported erasure profile unknown"
        ));
        assert_eq!(invalid.code(), tonic::Code::InvalidArgument);

        let precondition = repair_shard_status(anyhow::anyhow!(
            "CoreStore internal shard repair precondition failed: stale epoch"
        ));
        assert_eq!(precondition.code(), tonic::Code::FailedPrecondition);

        let corrupt = repair_shard_status(anyhow::anyhow!(
            "CoreStore block shard repair record has invalid schema"
        ));
        assert_eq!(corrupt.code(), tonic::Code::DataLoss);

        let unavailable = repair_shard_status(
            core_store::CoreStoreAvailabilityError::MeshTopologyUnavailable {
                node_id: "node-b".to_string(),
            }
            .into(),
        );
        assert_eq!(unavailable.code(), tonic::Code::Unavailable);

        let internal = repair_shard_status(anyhow::anyhow!("receipt signing failed"));
        assert_eq!(internal.code(), tonic::Code::Internal);
    }

    #[test]
    fn shard_and_history_limits_are_explicitly_bounded() {
        assert!(bounded_shard_range(0, MAX_INTERNAL_SHARD_BYTES as u64).is_ok());
        assert_eq!(
            bounded_shard_range(0, MAX_INTERNAL_SHARD_BYTES as u64 + 1)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert!(
            bounded_coremeta_history_page(
                MAX_COREMETA_HISTORY_PAGE_ROWS as u32,
                MAX_COREMETA_HISTORY_PAGE_BYTES,
                "history",
            )
            .is_ok()
        );
        assert_eq!(
            bounded_coremeta_history_page(
                MAX_COREMETA_HISTORY_PAGE_ROWS as u32 + 1,
                MAX_COREMETA_HISTORY_PAGE_BYTES,
                "history",
            )
            .unwrap_err()
            .code(),
            tonic::Code::InvalidArgument
        );

        let mut request = repair_request();
        request.placement_epoch = 0;
        assert_eq!(
            validate_repair_shard_request(&request).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn pending_finalisation_request_binds_hash_and_signature() {
        let finalisation_record = b"canonical proposal".to_vec();
        let mut request = PublishPendingMutationFinalisationRequest {
            finalisation_record: finalisation_record.clone(),
            payload_hash: core_store::CoreStore::pending_mutation_finalisation_rpc_payload_hash(
                &finalisation_record,
            ),
            source_signature: vec![1],
            ..Default::default()
        };
        validate_pending_mutation_finalisation_request(&request).unwrap();

        request.finalisation_record.push(0);
        assert_eq!(
            validate_pending_mutation_finalisation_request(&request)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        request.finalisation_record = finalisation_record;
        request.payload_hash =
            core_store::CoreStore::pending_mutation_finalisation_rpc_payload_hash(
                &request.finalisation_record,
            );
        request.source_signature.clear();
        assert_eq!(
            validate_pending_mutation_finalisation_request(&request)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn repair_claim_owner_is_stable_across_fanout_request_ids() {
        let first = InternalRequestHeader {
            request_id: "request-a".to_string(),
            source_node_id: "node-a".to_string(),
            ..Default::default()
        };
        let second = InternalRequestHeader {
            request_id: "request-b".to_string(),
            source_node_id: "node-a".to_string(),
            ..Default::default()
        };

        assert_eq!(
            super::super::repair_claim_owner(Some(&first)).unwrap(),
            super::super::repair_claim_owner(Some(&second)).unwrap()
        );
    }

    fn repair_request() -> RepairShardRequest {
        let shard_bytes = b"repair shard".to_vec();
        RepairShardRequest {
            block_id: "blk_repair".to_string(),
            shard_index: 1,
            shard_hash: format!("sha256:{}", core_store::sha256_hex(&shard_bytes)),
            shard_bytes,
            repair_finding_id: "repair-finding".to_string(),
            erasure_profile_id: "ec-4-2".to_string(),
            placement_epoch: 2,
            boundary_summary_hash: format!("sha256:{}", core_store::sha256_hex(&[])),
            writer_family: WriterFamily::ObjectBlob.as_str().to_string(),
            logical_file_id: "lf_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
            compression_algorithm: "none".to_string(),
            encryption_algorithm: "none".to_string(),
            ..Default::default()
        }
    }
}
