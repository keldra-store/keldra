use crate::core_store::{
    CF_OBJECT_HEADS, CF_STREAM_HEADS, CF_STREAM_RECORDS, CoreCompressionDescriptor,
    CoreMetaTuplePart, CoreObjectEncoding, CoreObjectPlacement, CoreObjectRef,
    TABLE_MULTIPART_PART_CURRENT_ROW, TABLE_MULTIPART_UPLOAD_CURRENT_ROW, TABLE_STREAM_HEAD_ROW,
    TABLE_STREAM_RECORD_INDEX_ROW, core_meta_committed_row_common, core_meta_root_key_hash,
    core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32};
use crate::mvcc_transaction::WriteOperation as CoreWriteOperation;
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::{
    MetadataMutationReceipt, MultipartAbortMutation, MultipartCompletionMutation,
    MultipartPartsPage, MultipartUpload, MultipartUploadMutation, MultipartUploadPart,
    MultipartUploadPartMutation, MultipartUploadsPage,
};
use crate::storage::Storage;
use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, Utc};
use prost::Message;
use std::collections::BTreeMap;

mod codec;
mod current_rows;
pub use codec::multipart_metadata_partition_id;
use codec::{
    current_part_payload, current_upload_payload, decode_committed_part_current_row,
    decode_committed_upload_current_row, decode_part_current_row, decode_upload_current_row,
    encode_multipart_event, encode_part_current_row, encode_upload_current_row,
    multipart_metadata_partition_principal, multipart_part_row_key, multipart_upload_row_key,
};
use current_rows::{
    active_count_value, multipart_active_count_key, multipart_current_row_operations,
    multipart_current_row_update,
};

const MULTIPART_UPLOAD_SCHEMA: &str = "anvil.multipart.upload.v1";
const MULTIPART_PART_SCHEMA: &str = "anvil.multipart.part.v1";
const MULTIPART_EVENT_SCHEMA: &str = "anvil.multipart.event.v1";
const MULTIPART_UPLOAD_CURRENT_ROW_SCHEMA: &str = "anvil.multipart.upload_current_row.v1";
const MULTIPART_PART_CURRENT_ROW_SCHEMA: &str = "anvil.multipart.part_current_row.v1";
const MULTIPART_CURRENT_ROW_KEY_PREFIX: &str = "multipart_current";
const MULTIPART_CURRENT_ROW_CANDIDATE_GENERATION: u64 = 1;
const MULTIPART_CURRENT_ROW_CANDIDATE_TRANSACTION_ID: &str = "multipart-current-candidate";
const MULTIPART_MAX_CURRENT_PROTO_BYTES: usize = 16 * 1024;
const MULTIPART_PAGE_MAX: usize = 1000;
const MULTIPART_PART_NUMBER_MAX: i32 = 10_000;
const MULTIPART_PART_COUNT_MAX: usize = MULTIPART_PART_NUMBER_MAX as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MultipartMutationKind {
    CreateUpload,
    UpsertPart,
    CompleteUpload,
    AbortUpload,
}

impl MultipartMutationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::CreateUpload => "create_upload",
            Self::UpsertPart => "upsert_part",
            Self::CompleteUpload => "complete_upload",
            Self::AbortUpload => "abort_upload",
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct MultipartUploadProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    id: i64,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(string, tag = "5")]
    key: String,
    #[prost(bytes, tag = "6")]
    upload_uuid: Vec<u8>,
    #[prost(int64, tag = "7")]
    created_at_unix_nanos: i64,
    #[prost(int64, optional, tag = "8")]
    completed_at_unix_nanos: Option<i64>,
    #[prost(int64, optional, tag = "9")]
    aborted_at_unix_nanos: Option<i64>,
}

#[derive(Clone, PartialEq, Message)]
struct MultipartPartProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    id: i64,
    #[prost(int64, tag = "3")]
    upload_id: i64,
    #[prost(int32, tag = "4")]
    part_number: i32,
    #[prost(string, tag = "5")]
    content_hash: String,
    #[prost(message, optional, tag = "6")]
    object_ref: Option<CoreObjectRefProto>,
    #[prost(int64, tag = "7")]
    size: i64,
    #[prost(string, tag = "8")]
    etag: String,
    #[prost(int64, tag = "9")]
    created_at_unix_nanos: i64,
}

#[derive(Clone, PartialEq, Message)]
struct MultipartEventProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    event: String,
    #[prost(message, optional, tag = "3")]
    upload: Option<MultipartUploadProto>,
    #[prost(message, optional, tag = "4")]
    part: Option<MultipartPartProto>,
    #[prost(int64, tag = "5")]
    emitted_at_unix_nanos: i64,
    #[prost(uint64, tag = "6")]
    fence_token: u64,
    #[prost(string, tag = "7")]
    mutation_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct MultipartUploadCurrentRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    upload: Option<MultipartUploadProto>,
    #[prost(uint64, tag = "4")]
    logical_revision: u64,
}

#[derive(Clone, PartialEq, Message)]
struct MultipartPartCurrentRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(message, optional, tag = "5")]
    part: Option<MultipartPartProto>,
    #[prost(uint64, tag = "6")]
    logical_revision: u64,
}

#[derive(Debug, Clone)]
struct MultipartUploadCurrentRow {
    upload: MultipartUpload,
    logical_revision: u64,
}

#[derive(Debug, Clone)]
struct MultipartPartCurrentRow {
    tenant_id: i64,
    bucket_id: i64,
    part: MultipartUploadPart,
    logical_revision: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectRefProto {
    #[prost(string, tag = "1")]
    hash: String,
    #[prost(uint64, tag = "2")]
    logical_size: u64,
    #[prost(string, tag = "3")]
    manifest_ref: String,
    #[prost(message, optional, tag = "4")]
    encoding: Option<CoreObjectEncodingProto>,
    #[prost(message, repeated, tag = "5")]
    placements: Vec<CoreObjectPlacementProto>,
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
    compression: Option<CoreObjectCompressionProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectCompressionProto {
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

pub(crate) async fn create_multipart_upload_with_permit(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<MultipartUploadMutation> {
    require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    let _ = partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_multipart_upload_inner(
        mvcc,
        tenant_id,
        bucket_id,
        key,
        permit.fence_token,
        None,
        None,
    )
    .await
}

pub(crate) async fn create_multipart_upload_with_permit_in_transaction(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<MultipartUploadMutation> {
    require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    let _ = partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_multipart_upload_inner(
        mvcc,
        tenant_id,
        bucket_id,
        key,
        permit.fence_token,
        None,
        Some((transaction_id, transaction_principal)),
    )
    .await
}

async fn create_multipart_upload_inner(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    fence_token: u64,
    partition_precondition: Option<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    transaction: Option<(&str, &str)>,
) -> Result<MultipartUploadMutation> {
    let upload_id = uuid::Uuid::new_v4();
    let upload = MultipartUpload {
        id: multipart_upload_row_id(upload_id),
        tenant_id,
        bucket_id,
        key: key.to_string(),
        upload_id,
        created_at: Utc::now(),
        completed_at: None,
        aborted_at: None,
    };
    let receipt = append_body(
        mvcc,
        tenant_id,
        bucket_id,
        MultipartMutationKind::CreateUpload,
        Some(upload.clone()),
        None,
        fence_token,
        partition_precondition,
        transaction,
    )
    .await?;
    Ok(MultipartUploadMutation { upload, receipt })
}

pub async fn get_active_multipart_upload(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
) -> Result<Option<MultipartUpload>> {
    get_active_multipart_upload_for_optional_transaction(
        mvcc, tenant_id, bucket_id, key, upload_id, None,
    )
    .await
}

pub async fn get_active_multipart_upload_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Option<MultipartUpload>> {
    get_active_multipart_upload_for_optional_transaction(
        mvcc,
        tenant_id,
        bucket_id,
        key,
        upload_id,
        Some((transaction_id, transaction_principal)),
    )
    .await
}

async fn get_active_multipart_upload_for_optional_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    transaction: Option<(&str, &str)>,
) -> Result<Option<MultipartUpload>> {
    let tuple_key = multipart_active_upload_key(bucket_id, key, upload_id)?;
    let transaction_scoped = transaction.is_some();
    let payload = if let Some((transaction_id, principal)) = transaction {
        let logical_key = crate::mvcc_product::coremeta_logical_key(
            CF_OBJECT_HEADS,
            TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
            &tuple_key,
        )?;
        mvcc.read_transaction_value(transaction_id, principal, &logical_key)?
    } else {
        let snapshot = mvcc.runtime.applied_version()?;
        mvcc.runtime
            .read_at(
                &crate::mvcc_product::coremeta_logical_key(
                    CF_OBJECT_HEADS,
                    TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                    &tuple_key,
                )?,
                snapshot,
            )?
            .map(|row| row.value)
    };
    let Some(payload) = payload else {
        return Ok(None);
    };
    let upload = if transaction_scoped {
        decode_upload_current_row(&payload)?
    } else {
        decode_committed_upload_current_row(&payload)?
    }
    .upload;
    if upload.tenant_id != tenant_id
        || upload.bucket_id != bucket_id
        || upload.key != key
        || upload.upload_id != upload_id
    {
        return Err(anyhow!("multipart active upload head scope mismatch"));
    }
    if upload.completed_at.is_some() || upload.aborted_at.is_some() {
        return Ok(None);
    }
    Ok(Some(upload))
}

pub async fn has_active_multipart_upload(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket_id: i64,
) -> Result<bool> {
    let snapshot = mvcc.runtime.applied_version()?;
    let Some(payload) = mvcc
        .runtime
        .read_at(
            &crate::mvcc_product::coremeta_logical_key(
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &multipart_active_count_key(bucket_id)?,
            )?,
            snapshot,
        )?
        .map(|row| row.value)
    else {
        return Ok(false);
    };
    Ok(active_count_value(&payload, bucket_id)? > 0)
}

pub(crate) async fn upsert_multipart_part_with_permit(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
    part_number: i32,
    object_ref: CoreObjectRef,
    size: i64,
    etag: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<MultipartUploadPartMutation> {
    upsert_multipart_part_inner(
        storage,
        mvcc,
        upload_row_id,
        part_number,
        object_ref,
        size,
        etag,
        Some((permit, partition_owner_signing_key)),
        None,
    )
    .await
}

pub(crate) async fn upsert_multipart_part_with_permit_in_transaction(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
    part_number: i32,
    object_ref: CoreObjectRef,
    size: i64,
    etag: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<MultipartUploadPartMutation> {
    upsert_multipart_part_inner(
        storage,
        mvcc,
        upload_row_id,
        part_number,
        object_ref,
        size,
        etag,
        Some((permit, partition_owner_signing_key)),
        Some((transaction_id, transaction_principal)),
    )
    .await
}

async fn upsert_multipart_part_inner(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
    part_number: i32,
    object_ref: CoreObjectRef,
    size: i64,
    etag: &str,
    permit: Option<(&PartitionWritePermit, &[u8])>,
    transaction: Option<(&str, &str)>,
) -> Result<MultipartUploadPartMutation> {
    if !(1..=MULTIPART_PART_NUMBER_MAX).contains(&part_number) {
        return Err(anyhow!(
            "multipart part number must be between 1 and {MULTIPART_PART_NUMBER_MAX}"
        ));
    }
    let (tenant_id, bucket_id, upload) =
        find_upload_for_optional_transaction(mvcc, upload_row_id, transaction)
            .await?
            .ok_or_else(|| anyhow!("multipart upload not found"))?;
    if upload.completed_at.is_some() || upload.aborted_at.is_some() {
        return Err(anyhow!("multipart upload is no longer active"));
    }
    let fence_token = if let Some((permit, signing_key)) = permit {
        require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
        let _ = partition_write_precondition(storage, permit, signing_key).await?;
        permit.fence_token
    } else {
        0
    };
    let current = read_current_part_for_optional_transaction(
        mvcc,
        tenant_id,
        bucket_id,
        upload_row_id,
        part_number,
        transaction,
    )
    .await?;
    let part = MultipartUploadPart {
        id: current
            .as_ref()
            .map(|part| part.id)
            .unwrap_or_else(|| multipart_part_row_id(upload_row_id, part_number)),
        upload_id: upload_row_id,
        part_number,
        content_hash: object_ref.hash.clone(),
        object_ref,
        size,
        etag: etag.to_string(),
        created_at: Utc::now(),
    };
    let receipt = append_body(
        mvcc,
        tenant_id,
        bucket_id,
        MultipartMutationKind::UpsertPart,
        None,
        Some(part.clone()),
        fence_token,
        None,
        transaction,
    )
    .await?;
    Ok(MultipartUploadPartMutation { part, receipt })
}

pub async fn list_multipart_parts(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
) -> Result<Vec<MultipartUploadPart>> {
    list_multipart_parts_for_optional_transaction(mvcc, upload_row_id, None).await
}

pub async fn list_multipart_parts_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Vec<MultipartUploadPart>> {
    list_multipart_parts_for_optional_transaction(
        mvcc,
        upload_row_id,
        Some((transaction_id, transaction_principal)),
    )
    .await
}

pub async fn list_multipart_parts_page(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
    part_number_marker: i32,
    limit: i32,
) -> Result<MultipartPartsPage> {
    if !(0..=MULTIPART_PART_NUMBER_MAX).contains(&part_number_marker) {
        return Err(anyhow!(
            "multipart part number marker must be between 0 and {MULTIPART_PART_NUMBER_MAX}"
        ));
    }
    let page_size = multipart_page_size(limit)?;
    let snapshot = mvcc.runtime.applied_version()?;
    let Some((tenant_id, bucket_id, _)) = find_upload_at(mvcc, upload_row_id, snapshot)? else {
        return Ok(MultipartPartsPage {
            parts: Vec::new(),
            is_truncated: false,
            next_part_number_marker: None,
        });
    };
    page_multipart_parts_at(
        mvcc,
        tenant_id,
        bucket_id,
        upload_row_id,
        part_number_marker,
        page_size,
        snapshot,
    )
}

pub async fn list_active_multipart_uploads(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket_id: i64,
    prefix: &str,
    key_marker: &str,
    upload_id_marker: Option<uuid::Uuid>,
    limit: i32,
) -> Result<MultipartUploadsPage> {
    let page_size = multipart_page_size(limit)?;
    let tuple_prefix = multipart_active_upload_bucket_prefix(bucket_id)?;
    let after_tuple_key =
        multipart_active_upload_scan_after(bucket_id, prefix, key_marker, upload_id_marker)?;
    let snapshot = mvcc.runtime.applied_version()?;
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_OBJECT_HEADS, &tuple_prefix)?;
    let namespace = crate::mvcc_product::coremeta_application_prefix(CF_OBJECT_HEADS, &[])?;
    let mut records = mvcc.runtime.scan_table_prefix_at(
        TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
        &application_prefix,
        snapshot,
    )?;
    if let Some(after) = after_tuple_key {
        records.retain(|(key, _)| {
            key.application_key
                .strip_prefix(&namespace)
                .is_some_and(|tuple| tuple > after.as_slice())
        });
    }
    records.truncate(page_size + 1);
    let mut uploads = Vec::with_capacity(page_size);
    let mut is_truncated = false;
    let mut last_source_upload = None;
    let mut source_count = 0;
    for (_, record) in records {
        let upload = decode_upload_current_row(&record.value)?.upload;
        if upload.bucket_id != bucket_id {
            return Err(anyhow!("multipart active upload bucket scope mismatch"));
        }
        if !upload.key.starts_with(prefix) {
            break;
        }
        if source_count == page_size {
            is_truncated = true;
            break;
        }
        source_count += 1;
        last_source_upload = Some((upload.key.clone(), upload.upload_id));
        if upload.completed_at.is_none() && upload.aborted_at.is_none() {
            uploads.push(upload);
        }
    }
    let (next_key_marker, next_upload_id_marker) = if is_truncated {
        last_source_upload
            .map(|(key, upload_id)| (Some(key), Some(upload_id)))
            .unwrap_or((None, None))
    } else {
        (None, None)
    };
    Ok(MultipartUploadsPage {
        uploads,
        is_truncated,
        next_key_marker,
        next_upload_id_marker,
    })
}

pub(crate) async fn complete_multipart_upload_with_permit(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<MultipartCompletionMutation> {
    complete_multipart_upload_inner(
        storage,
        mvcc,
        upload_row_id,
        Some((permit, partition_owner_signing_key)),
        None,
    )
    .await
}

pub(crate) async fn complete_multipart_upload_with_permit_in_transaction(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<MultipartCompletionMutation> {
    complete_multipart_upload_inner(
        storage,
        mvcc,
        upload_row_id,
        Some((permit, partition_owner_signing_key)),
        Some((transaction_id, transaction_principal)),
    )
    .await
}

async fn complete_multipart_upload_inner(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
    permit: Option<(&PartitionWritePermit, &[u8])>,
    transaction: Option<(&str, &str)>,
) -> Result<MultipartCompletionMutation> {
    let Some((tenant_id, bucket_id, mut upload)) =
        find_upload_for_optional_transaction(mvcc, upload_row_id, transaction).await?
    else {
        return Ok(MultipartCompletionMutation {
            completed: false,
            receipt: None,
        });
    };
    if upload.completed_at.is_some() || upload.aborted_at.is_some() {
        return Ok(MultipartCompletionMutation {
            completed: false,
            receipt: None,
        });
    }
    let fence_token = if let Some((permit, signing_key)) = permit {
        require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
        let _ = partition_write_precondition(storage, permit, signing_key).await?;
        permit.fence_token
    } else {
        0
    };
    upload.completed_at = Some(Utc::now());
    let receipt = append_body(
        mvcc,
        tenant_id,
        bucket_id,
        MultipartMutationKind::CompleteUpload,
        Some(upload),
        None,
        fence_token,
        None,
        transaction,
    )
    .await?;
    Ok(MultipartCompletionMutation {
        completed: true,
        receipt: Some(receipt),
    })
}

pub(crate) async fn abort_multipart_upload_with_permit(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<MultipartAbortMutation> {
    require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    let _ = partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    abort_multipart_upload_inner(
        mvcc,
        tenant_id,
        bucket_id,
        key,
        upload_id,
        permit.fence_token,
        None,
        None,
    )
    .await
}

pub(crate) async fn abort_multipart_upload_with_permit_in_transaction(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<MultipartAbortMutation> {
    require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    let _ = partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    abort_multipart_upload_inner(
        mvcc,
        tenant_id,
        bucket_id,
        key,
        upload_id,
        permit.fence_token,
        None,
        Some((transaction_id, transaction_principal)),
    )
    .await
}

async fn abort_multipart_upload_inner(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    fence_token: u64,
    partition_precondition: Option<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    transaction: Option<(&str, &str)>,
) -> Result<MultipartAbortMutation> {
    let Some(mut upload) = get_active_multipart_upload_for_optional_transaction(
        mvcc,
        tenant_id,
        bucket_id,
        key,
        upload_id,
        transaction,
    )
    .await?
    else {
        return Ok(MultipartAbortMutation {
            aborted: false,
            receipt: None,
        });
    };
    upload.aborted_at = Some(Utc::now());
    let receipt = append_body(
        mvcc,
        tenant_id,
        bucket_id,
        MultipartMutationKind::AbortUpload,
        Some(upload),
        None,
        fence_token,
        partition_precondition,
        transaction,
    )
    .await?;
    Ok(MultipartAbortMutation {
        aborted: true,
        receipt: Some(receipt),
    })
}

pub fn find_multipart_upload_partition(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
) -> Result<Option<(i64, i64)>> {
    Ok(find_upload(mvcc, upload_row_id)?.map(|(tenant_id, bucket_id, _)| (tenant_id, bucket_id)))
}

pub async fn find_multipart_upload_partition_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Option<(i64, i64)>> {
    Ok(find_upload_for_optional_transaction(
        mvcc,
        upload_row_id,
        Some((transaction_id, transaction_principal)),
    )
    .await?
    .map(|(tenant_id, bucket_id, _)| (tenant_id, bucket_id)))
}

fn find_upload(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
) -> Result<Option<(i64, i64, MultipartUpload)>> {
    let snapshot = mvcc.runtime.applied_version()?;
    find_upload_at(mvcc, upload_row_id, snapshot)
}

async fn find_upload_for_optional_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
    transaction: Option<(&str, &str)>,
) -> Result<Option<(i64, i64, MultipartUpload)>> {
    if transaction.is_none() {
        return find_upload(mvcc, upload_row_id);
    }
    let tuple_key = multipart_upload_id_head_key(upload_row_id)?;
    let payload = if let Some((transaction_id, principal)) = transaction {
        let logical_key = crate::mvcc_product::coremeta_logical_key(
            CF_OBJECT_HEADS,
            TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
            &tuple_key,
        )?;
        mvcc.read_transaction_value(transaction_id, principal, &logical_key)?
    } else {
        mvcc.read_latest_value(&crate::mvcc_product::coremeta_logical_key(
            CF_OBJECT_HEADS,
            TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
            &tuple_key,
        )?)?
    };
    decode_upload_id_head(payload.as_deref(), upload_row_id, false)
}

async fn read_current_part_for_optional_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    upload_row_id: i64,
    part_number: i32,
    transaction: Option<(&str, &str)>,
) -> Result<Option<MultipartUploadPart>> {
    let tuple_key = multipart_part_row_key(tenant_id, bucket_id, upload_row_id, part_number)?;
    let transaction_scoped = transaction.is_some();
    let payload = if let Some((transaction_id, principal)) = transaction {
        let logical_key = crate::mvcc_product::coremeta_logical_key(
            CF_OBJECT_HEADS,
            TABLE_MULTIPART_PART_CURRENT_ROW,
            &tuple_key,
        )?;
        mvcc.read_transaction_value(transaction_id, principal, &logical_key)?
    } else {
        mvcc.read_latest_value(&crate::mvcc_product::coremeta_logical_key(
            CF_OBJECT_HEADS,
            TABLE_MULTIPART_PART_CURRENT_ROW,
            &tuple_key,
        )?)?
    };
    let Some(payload) = payload else {
        return Ok(None);
    };
    let row = if transaction_scoped {
        decode_part_current_row(&payload)?
    } else {
        decode_committed_part_current_row(&payload)?
    };
    if row.tenant_id != tenant_id
        || row.bucket_id != bucket_id
        || row.part.upload_id != upload_row_id
        || row.part.part_number != part_number
    {
        return Err(anyhow!("multipart part current row scope mismatch"));
    }
    Ok(Some(row.part))
}

async fn list_multipart_parts_for_optional_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
    transaction: Option<(&str, &str)>,
) -> Result<Vec<MultipartUploadPart>> {
    let snapshot = if let Some((transaction_id, _)) = transaction {
        mvcc.open_transactions
            .handle(transaction_id)?
            .snapshot_version
    } else {
        mvcc.runtime.applied_version()?
    };
    let upload = if transaction.is_some() {
        find_upload_for_optional_transaction(mvcc, upload_row_id, transaction).await?
    } else {
        find_upload_at(mvcc, upload_row_id, snapshot)?
    };
    let Some((tenant_id, bucket_id, _)) = upload else {
        return Ok(Vec::new());
    };
    let tuple_prefix = multipart_upload_part_rows_prefix(tenant_id, bucket_id, upload_row_id)?;
    let logical_prefix = crate::mvcc_product::coremeta_logical_key(
        CF_OBJECT_HEADS,
        TABLE_MULTIPART_PART_CURRENT_ROW,
        &tuple_prefix,
    )?;
    let mut parts = if let Some((transaction_id, _)) = transaction {
        let mut visible = BTreeMap::new();
        for (_, row) in mvcc.runtime.scan_table_prefix_at(
            TABLE_MULTIPART_PART_CURRENT_ROW,
            &logical_prefix.application_key,
            snapshot,
        )? {
            let row = decode_part_current_row(&row.value)?;
            if row.tenant_id != tenant_id
                || row.bucket_id != bucket_id
                || row.part.upload_id != upload_row_id
            {
                bail!("multipart snapshot part row scope mismatch");
            }
            visible.insert(row.part.part_number, row.part);
        }
        visible
    } else {
        read_all_multipart_parts_bounded(mvcc, tenant_id, bucket_id, upload_row_id, snapshot)?
    };
    if let Some((transaction_id, principal)) = transaction {
        for write in mvcc
            .open_transactions
            .staged_writes(transaction_id, principal)?
        {
            if write.key().table_id != TABLE_MULTIPART_PART_CURRENT_ROW
                || !write
                    .key()
                    .application_key
                    .starts_with(&logical_prefix.application_key)
            {
                continue;
            }
            match write {
                CoreWriteOperation::Put { value, .. } => {
                    let row = decode_part_current_row(&value)?;
                    if row.tenant_id != tenant_id
                        || row.bucket_id != bucket_id
                        || row.part.upload_id != upload_row_id
                    {
                        bail!("multipart staged part row scope mismatch");
                    }
                    parts.insert(row.part.part_number, row.part);
                }
                CoreWriteOperation::Delete { .. } => {}
            }
        }
    }
    if parts.len() > MULTIPART_PART_COUNT_MAX {
        return Err(anyhow!(
            "multipart upload exceeds the bounded part count of {MULTIPART_PART_COUNT_MAX}"
        ));
    }
    Ok(parts.into_values().collect())
}

fn read_all_multipart_parts_bounded(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    upload_row_id: i64,
    snapshot: u64,
) -> Result<BTreeMap<i32, MultipartUploadPart>> {
    let prefix = multipart_upload_part_rows_prefix(tenant_id, bucket_id, upload_row_id)?;
    let logical_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_OBJECT_HEADS, &prefix)?;
    let mut parts = BTreeMap::new();
    for (_, record) in mvcc.runtime.scan_table_prefix_at(
        TABLE_MULTIPART_PART_CURRENT_ROW,
        &logical_prefix,
        snapshot,
    )? {
        if parts.len() == MULTIPART_PART_COUNT_MAX {
            return Err(anyhow!(
                "multipart upload exceeds the bounded part count of {MULTIPART_PART_COUNT_MAX}"
            ));
        }
        let row = decode_part_current_row(&record.value)?;
        if row.tenant_id != tenant_id
            || row.bucket_id != bucket_id
            || row.part.upload_id != upload_row_id
        {
            return Err(anyhow!("multipart part page scope mismatch"));
        }
        if parts.insert(row.part.part_number, row.part).is_some() {
            return Err(anyhow!(
                "multipart part table contains a duplicate part number"
            ));
        }
    }
    Ok(parts)
}

fn find_upload_at(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    upload_row_id: i64,
    snapshot: u64,
) -> Result<Option<(i64, i64, MultipartUpload)>> {
    let key = crate::mvcc_product::coremeta_logical_key(
        CF_OBJECT_HEADS,
        TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
        &multipart_upload_id_head_key(upload_row_id)?,
    )?;
    let payload = mvcc.runtime.read_at(&key, snapshot)?;
    decode_upload_id_head(
        payload.as_ref().map(|row| row.value.as_slice()),
        upload_row_id,
        false,
    )
}

fn decode_upload_id_head(
    payload: Option<&[u8]>,
    upload_row_id: i64,
    committed: bool,
) -> Result<Option<(i64, i64, MultipartUpload)>> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    let upload = if committed {
        decode_committed_upload_current_row(payload)?
    } else {
        decode_upload_current_row(payload)?
    }
    .upload;
    if upload.id != upload_row_id {
        return Err(anyhow!("multipart upload id head scope mismatch"));
    }
    Ok(Some((upload.tenant_id, upload.bucket_id, upload)))
}

fn page_multipart_parts_at(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    upload_row_id: i64,
    part_number_marker: i32,
    page_size: usize,
    snapshot: u64,
) -> Result<MultipartPartsPage> {
    if !(1..=MULTIPART_PAGE_MAX).contains(&page_size) {
        return Err(anyhow!(
            "multipart page size must be between 1 and {MULTIPART_PAGE_MAX}"
        ));
    }
    let prefix = multipart_upload_part_rows_prefix(tenant_id, bucket_id, upload_row_id)?;
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_OBJECT_HEADS, &prefix)?;
    let mut records = mvcc.runtime.scan_table_prefix_at(
        TABLE_MULTIPART_PART_CURRENT_ROW,
        &application_prefix,
        snapshot,
    )?;
    records.retain(|(_, record)| {
        decode_part_current_row(&record.value)
            .map(|row| row.part.part_number > part_number_marker)
            .unwrap_or(true)
    });
    let is_truncated = records.len() > page_size;
    if is_truncated {
        records.truncate(page_size);
    }
    let mut parts = Vec::with_capacity(records.len());
    for (_, record) in records {
        let row = decode_part_current_row(&record.value)?;
        if row.tenant_id != tenant_id
            || row.bucket_id != bucket_id
            || row.part.upload_id != upload_row_id
            || row.part.part_number <= part_number_marker
        {
            return Err(anyhow!("multipart part page scope mismatch"));
        }
        parts.push(row.part);
    }
    let next_part_number_marker = if is_truncated {
        parts.last().map(|part| part.part_number)
    } else {
        None
    };
    Ok(MultipartPartsPage {
        parts,
        is_truncated,
        next_part_number_marker,
    })
}

fn multipart_page_size(limit: i32) -> Result<usize> {
    if limit == 0 {
        return Ok(MULTIPART_PAGE_MAX);
    }
    let page_size =
        usize::try_from(limit).map_err(|_| anyhow!("multipart page size must be positive"))?;
    if !(1..=MULTIPART_PAGE_MAX).contains(&page_size) {
        return Err(anyhow!(
            "multipart page size must be between 1 and {MULTIPART_PAGE_MAX}"
        ));
    }
    Ok(page_size)
}

fn multipart_upload_id_head_key(upload_row_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(MULTIPART_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::Utf8("upload_id_head"),
        CoreMetaTuplePart::I64(upload_row_id),
    ])
}

fn multipart_event_head_logical_key(
    tenant_id: i64,
    bucket_id: i64,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_STREAM_HEADS,
        TABLE_STREAM_HEAD_ROW,
        &core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("multipart-event-head"),
            CoreMetaTuplePart::I64(tenant_id),
            CoreMetaTuplePart::I64(bucket_id),
        ])?,
    )
}

fn multipart_event_logical_key(
    tenant_id: i64,
    bucket_id: i64,
    sequence: u64,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_STREAM_RECORDS,
        TABLE_STREAM_RECORD_INDEX_ROW,
        &core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("multipart-event"),
            CoreMetaTuplePart::I64(tenant_id),
            CoreMetaTuplePart::I64(bucket_id),
            CoreMetaTuplePart::U64(sequence),
        ])?,
    )
}

fn decode_event_head(payload: Option<&[u8]>) -> Result<u64> {
    let Some(payload) = payload else {
        return Ok(0);
    };
    Ok(u64::from_be_bytes(payload.try_into().map_err(|_| {
        anyhow!("multipart event head has invalid length")
    })?))
}

fn multipart_active_upload_bucket_prefix(bucket_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(MULTIPART_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::Utf8("active_upload"),
        CoreMetaTuplePart::I64(bucket_id),
    ])
}

fn multipart_active_upload_object_prefix(bucket_id: i64, key: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(MULTIPART_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::Utf8("active_upload"),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::Raw(key.as_bytes()),
    ])
}

fn multipart_active_upload_key(
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(MULTIPART_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::Utf8("active_upload"),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::Raw(key.as_bytes()),
        CoreMetaTuplePart::Raw(upload_id.as_bytes()),
    ])
}

fn multipart_active_upload_scan_after(
    bucket_id: i64,
    prefix: &str,
    key_marker: &str,
    upload_id_marker: Option<uuid::Uuid>,
) -> Result<Option<Vec<u8>>> {
    if key_marker.is_empty() && upload_id_marker.is_some() {
        return Err(anyhow!(
            "multipart upload id marker requires a nonempty key marker"
        ));
    }
    let marker_is_start = !key_marker.is_empty() && key_marker.as_bytes() >= prefix.as_bytes();
    let start_key = if marker_is_start { key_marker } else { prefix };
    if start_key.is_empty() {
        return Ok(None);
    }
    if marker_is_start {
        if let Some(upload_id) = upload_id_marker {
            return Ok(Some(multipart_active_upload_key(
                bucket_id, start_key, upload_id,
            )?));
        }
    }
    Ok(Some(multipart_active_upload_object_prefix(
        bucket_id, start_key,
    )?))
}

fn multipart_upload_part_rows_prefix(
    tenant_id: i64,
    bucket_id: i64,
    upload_row_id: i64,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(MULTIPART_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::Utf8("part"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::I64(upload_row_id),
    ])
}

fn multipart_upload_row_id(upload_id: uuid::Uuid) -> i64 {
    multipart_positive_row_id(format!("multipart-upload:{upload_id}").as_bytes())
}

fn multipart_part_row_id(upload_row_id: i64, part_number: i32) -> i64 {
    multipart_positive_row_id(format!("multipart-part:{upload_row_id}:{part_number}").as_bytes())
}

fn multipart_positive_row_id(seed: &[u8]) -> i64 {
    let digest = hash32(seed);
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    let value = u64::from_be_bytes(bytes) & (i64::MAX as u64);
    i64::try_from(value.max(1)).expect("positive multipart row id must fit i64")
}

async fn append_body(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    event: MultipartMutationKind,
    upload: Option<MultipartUpload>,
    part: Option<MultipartUploadPart>,
    fence_token: u64,
    partition_precondition: Option<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    transaction: Option<(&str, &str)>,
) -> Result<MetadataMutationReceipt> {
    let mutation_id = uuid::Uuid::new_v4();
    let internal_transaction_id = format!("multipart-metadata:{mutation_id}");
    let body = encode_multipart_event(
        event,
        upload.as_ref(),
        part.as_ref(),
        fence_token,
        mutation_id,
    )?;
    let payload_hash = hex::encode(hash32(&body));
    let head_key = multipart_event_head_logical_key(tenant_id, bucket_id)?;
    let preexisting_staged_keys = if let Some((transaction_id, principal)) = transaction {
        mvcc.open_transactions
            .staged_writes(transaction_id, principal)?
            .into_iter()
            .map(|write| write.key().clone())
            .collect::<std::collections::BTreeSet<_>>()
    } else {
        std::collections::BTreeSet::new()
    };
    let head_payload = if let Some((transaction_id, principal)) = transaction {
        mvcc.read_transaction_value(transaction_id, principal, &head_key)?
    } else {
        mvcc.read_latest_value(&head_key)?
    };
    let sequence = decode_event_head(head_payload.as_deref())?
        .checked_add(1)
        .ok_or_else(|| anyhow!("multipart event cursor overflow"))?;
    let read_current = |table_id, tuple_key: &[u8]| {
        let key = crate::mvcc_product::coremeta_logical_key(CF_OBJECT_HEADS, table_id, tuple_key)?;
        if let Some((transaction_id, principal)) = transaction {
            mvcc.read_transaction_value(transaction_id, principal, &key)
        } else {
            mvcc.read_latest_value(&key)
        }
    };
    let current_update = multipart_current_row_update(
        read_current,
        tenant_id,
        bucket_id,
        event,
        upload.as_ref(),
        part.as_ref(),
    )?;
    let mut preconditions = partition_precondition.into_iter().collect::<Vec<_>>();
    preconditions.extend(current_update.preconditions.clone());
    let event_key = multipart_event_logical_key(tenant_id, bucket_id, sequence)?;
    preconditions.extend([
        (
            event_key.clone(),
            crate::mvcc_transaction::PredicateKind::Absent,
        ),
        (
            head_key.clone(),
            head_payload.as_ref().map_or(
                crate::mvcc_transaction::PredicateKind::Absent,
                |payload| {
                    crate::mvcc_transaction::PredicateKind::ValueHash(
                        *blake3::hash(payload).as_bytes(),
                    )
                },
            ),
        ),
    ]);
    if let Some((transaction_id, _)) = transaction {
        let snapshot = mvcc
            .open_transactions
            .handle(transaction_id)?
            .snapshot_version;
        preconditions = preconditions
            .into_iter()
            .filter_map(|(key, _)| {
                if preexisting_staged_keys.contains(&key) {
                    return None;
                }
                let predicate = match mvcc.runtime.read_at(&key, snapshot) {
                    Ok(Some(row)) => crate::mvcc_transaction::PredicateKind::ValueHash(
                        *blake3::hash(&row.value).as_bytes(),
                    ),
                    Ok(None) => crate::mvcc_transaction::PredicateKind::Absent,
                    Err(error) => return Some(Err(error)),
                };
                Some(Ok((key, predicate)))
            })
            .collect::<Result<Vec<_>>>()?;
    }
    let mut operations = vec![
        crate::mvcc_product::ProductMutation::put(event_key, body.clone()),
        crate::mvcc_product::ProductMutation::put(head_key, sequence.to_be_bytes().to_vec()),
    ];
    operations.extend(multipart_current_row_operations(&current_update)?);
    let committed_by_principal = transaction
        .map(|(_, principal)| principal.to_string())
        .unwrap_or_else(|| multipart_metadata_partition_principal(tenant_id, bucket_id));
    if transaction.is_some() {
        let (transaction_id, principal) =
            transaction.ok_or_else(|| anyhow!("transaction binding is required"))?;
        mvcc.stage_product_mutations(
            transaction_id,
            principal,
            operations,
            u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default(),
        )?;
        for (key, predicate) in preconditions {
            mvcc.stage_predicate(
                transaction_id,
                principal,
                key,
                predicate,
                u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default(),
            )?;
        }
        return Ok(MetadataMutationReceipt {
            mutation_id,
            payload_hash: payload_hash.clone(),
            record_hash: payload_hash,
            watch_cursor: sequence,
        });
    }
    mvcc.autocommit_product_mutations_with_predicates(
        &committed_by_principal,
        &internal_transaction_id,
        operations,
        preconditions,
        crate::mvcc_transaction::DurabilityLevel::Quorum,
        u64::try_from(Utc::now().timestamp_millis())
            .map_err(|_| anyhow!("multipart timestamp predates Unix epoch"))?,
    )
    .await?;
    Ok(MetadataMutationReceipt {
        mutation_id,
        payload_hash,
        record_hash: hex::encode(hash32(&body)),
        watch_cursor: sequence,
    })
}

fn require_multipart_metadata_permit(
    tenant_id: i64,
    bucket_id: i64,
    permit: &PartitionWritePermit,
) -> Result<()> {
    let expected_partition_id = hex::encode(multipart_metadata_partition_id(tenant_id, bucket_id));
    if permit.partition_family != "multipart_metadata"
        || permit.partition_id != expected_partition_id
    {
        anyhow::bail!("multipart metadata write permit targets a different partition");
    }
    Ok(())
}
