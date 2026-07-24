use crate::core_store::{
    CF_OBJECT_HEADS, CoreCompressionDescriptor, CoreMetaRecord, CoreMetaTuplePart,
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreMutationRootPublication, CoreObjectEncoding, CoreObjectPlacement, CoreObjectRef, CoreStore,
    CoreTransaction, CoreTransactionUpdate, TABLE_MULTIPART_PART_CURRENT_ROW,
    TABLE_MULTIPART_UPLOAD_CURRENT_ROW, canonical_coremeta_cf_name, core_meta_committed_row_common,
    core_meta_record_tuple_key, core_meta_root_key_hash, core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32, writer::WriterFamily};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::{
    MetadataMutationReceipt, MultipartAbortMutation, MultipartCompletionMutation,
    MultipartPartsPage, MultipartUpload, MultipartUploadMutation, MultipartUploadPart,
    MultipartUploadPartMutation, MultipartUploadsPage,
};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use prost::Message;
use std::collections::BTreeMap;

mod codec;
mod current_rows;
#[cfg(test)]
use crate::core_store::{
    CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaRootPublication, CoreMetaStore,
    core_meta_row_common_from_payload,
};
pub use codec::multipart_metadata_partition_id;
use codec::{
    current_part_payload, current_upload_payload, decode_committed_part_current_row,
    decode_committed_upload_current_row, decode_part_current_record, decode_part_current_row,
    decode_upload_current_row, encode_multipart_event, encode_part_current_row,
    encode_upload_current_row, multipart_current_root_key, multipart_metadata_partition_principal,
    multipart_metadata_stream_id, multipart_part_row_key, multipart_upload_row_key,
};
#[cfg(test)]
use codec::{decode_multipart_event, decode_multipart_event_fence};
use current_rows::*;
#[cfg(test)]
use current_rows::{
    MultipartActiveCountCurrentRow, MultipartCurrentRowUpdate, encode_active_count_current_row,
    stage_active_count_update,
};
use current_rows::{
    active_count_value, multipart_active_count_key, multipart_current_row_operations,
    multipart_current_row_update, multipart_current_row_update_with_transaction,
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

#[derive(Debug, Clone, Default)]
struct MultipartState {
    uploads: BTreeMap<i64, MultipartUpload>,
    parts: BTreeMap<(i64, i32), MultipartUploadPart>,
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
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<MultipartUploadMutation> {
    require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_multipart_upload_inner(
        storage,
        tenant_id,
        bucket_id,
        key,
        permit.fence_token,
        Some(partition_precondition),
        None,
    )
    .await
}

pub(crate) async fn create_multipart_upload_with_permit_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<MultipartUploadMutation> {
    require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_multipart_upload_inner(
        storage,
        tenant_id,
        bucket_id,
        key,
        permit.fence_token,
        Some(partition_precondition),
        Some((transaction_id, transaction_principal)),
    )
    .await
}

async fn create_multipart_upload_inner(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
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
        storage,
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
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
) -> Result<Option<MultipartUpload>> {
    get_active_multipart_upload_for_optional_transaction(
        storage, tenant_id, bucket_id, key, upload_id, None,
    )
    .await
}

pub async fn get_active_multipart_upload_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Option<MultipartUpload>> {
    get_active_multipart_upload_for_optional_transaction(
        storage,
        tenant_id,
        bucket_id,
        key,
        upload_id,
        Some((transaction_id, transaction_principal)),
    )
    .await
}

async fn get_active_multipart_upload_for_optional_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    transaction: Option<(&str, &str)>,
) -> Result<Option<MultipartUpload>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let tuple_key = multipart_active_upload_key(bucket_id, key, upload_id)?;
    let transaction_scoped = transaction.is_some();
    let payload = if let Some(transaction) = transaction {
        let transaction = read_transaction_for_optional_scope(&core_store, Some(transaction))
            .await?
            .ok_or_else(|| anyhow!("multipart transaction scope is missing"))?;
        coremeta_payload_visible_to_transaction(
            &core_store,
            &transaction,
            CF_OBJECT_HEADS,
            TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
            &tuple_key,
        )?
    } else {
        core_store.read_coremeta_row(
            CF_OBJECT_HEADS,
            TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
            &tuple_key,
        )?
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

pub async fn has_active_multipart_upload(storage: &Storage, bucket_id: i64) -> Result<bool> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(payload) = store.read_coremeta_row(
        CF_OBJECT_HEADS,
        TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
        &multipart_active_count_key(bucket_id)?,
    )?
    else {
        return Ok(false);
    };
    Ok(active_count_value(&payload, bucket_id)? > 0)
}

pub(crate) async fn upsert_multipart_part_with_permit(
    storage: &Storage,
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
        find_upload_for_optional_transaction(storage, upload_row_id, transaction)
            .await?
            .ok_or_else(|| anyhow!("multipart upload not found"))?;
    if upload.completed_at.is_some() || upload.aborted_at.is_some() {
        return Err(anyhow!("multipart upload is no longer active"));
    }
    let (fence_token, partition_precondition) = if let Some((permit, signing_key)) = permit {
        require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
        (
            permit.fence_token,
            Some(partition_write_precondition(storage, permit, signing_key).await?),
        )
    } else {
        (0, None)
    };
    let current = read_current_part_for_optional_transaction(
        storage,
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
        storage,
        tenant_id,
        bucket_id,
        MultipartMutationKind::UpsertPart,
        None,
        Some(part.clone()),
        fence_token,
        partition_precondition,
        transaction,
    )
    .await?;
    Ok(MultipartUploadPartMutation { part, receipt })
}

pub async fn list_multipart_parts(
    storage: &Storage,
    upload_row_id: i64,
) -> Result<Vec<MultipartUploadPart>> {
    list_multipart_parts_for_optional_transaction(storage, upload_row_id, None).await
}

pub async fn list_multipart_parts_in_transaction(
    storage: &Storage,
    upload_row_id: i64,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Vec<MultipartUploadPart>> {
    list_multipart_parts_for_optional_transaction(
        storage,
        upload_row_id,
        Some((transaction_id, transaction_principal)),
    )
    .await
}

pub async fn list_multipart_parts_page(
    storage: &Storage,
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
    let Some((tenant_id, bucket_id, _)) = find_upload(storage, upload_row_id).await? else {
        return Ok(MultipartPartsPage {
            parts: Vec::new(),
            is_truncated: false,
            next_part_number_marker: None,
        });
    };
    let store = CoreStore::new(storage.clone()).await?;
    page_multipart_parts_from_store(
        &store,
        tenant_id,
        bucket_id,
        upload_row_id,
        part_number_marker,
        page_size,
    )
}

pub async fn list_active_multipart_uploads(
    storage: &Storage,
    bucket_id: i64,
    prefix: &str,
    key_marker: &str,
    upload_id_marker: Option<uuid::Uuid>,
    limit: i32,
) -> Result<MultipartUploadsPage> {
    let page_size = multipart_page_size(limit)?;
    let store = CoreStore::new(storage.clone()).await?;
    let tuple_prefix = multipart_active_upload_bucket_prefix(bucket_id)?;
    let after_tuple_key =
        multipart_active_upload_scan_after(bucket_id, prefix, key_marker, upload_id_marker)?;
    let records = store.scan_coremeta_prefix_page(
        CF_OBJECT_HEADS,
        TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
        &tuple_prefix,
        after_tuple_key.as_deref(),
        page_size + 1,
    )?;
    let mut uploads = Vec::with_capacity(page_size);
    let mut is_truncated = false;
    let mut last_source_upload = None;
    let mut source_count = 0;
    for record in records {
        let upload = decode_active_upload_record(&record)?;
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
    upload_row_id: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<MultipartCompletionMutation> {
    complete_multipart_upload_inner(
        storage,
        upload_row_id,
        Some((permit, partition_owner_signing_key)),
        None,
    )
    .await
}

pub(crate) async fn complete_multipart_upload_with_permit_in_transaction(
    storage: &Storage,
    upload_row_id: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<MultipartCompletionMutation> {
    complete_multipart_upload_inner(
        storage,
        upload_row_id,
        Some((permit, partition_owner_signing_key)),
        Some((transaction_id, transaction_principal)),
    )
    .await
}

async fn complete_multipart_upload_inner(
    storage: &Storage,
    upload_row_id: i64,
    permit: Option<(&PartitionWritePermit, &[u8])>,
    transaction: Option<(&str, &str)>,
) -> Result<MultipartCompletionMutation> {
    let Some((tenant_id, bucket_id, mut upload)) =
        find_upload_for_optional_transaction(storage, upload_row_id, transaction).await?
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
    let (fence_token, partition_precondition) = if let Some((permit, signing_key)) = permit {
        require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
        (
            permit.fence_token,
            Some(partition_write_precondition(storage, permit, signing_key).await?),
        )
    } else {
        (0, None)
    };
    upload.completed_at = Some(Utc::now());
    let receipt = append_body(
        storage,
        tenant_id,
        bucket_id,
        MultipartMutationKind::CompleteUpload,
        Some(upload),
        None,
        fence_token,
        partition_precondition,
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
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<MultipartAbortMutation> {
    require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    abort_multipart_upload_inner(
        storage,
        tenant_id,
        bucket_id,
        key,
        upload_id,
        permit.fence_token,
        Some(partition_precondition),
        None,
    )
    .await
}

pub(crate) async fn abort_multipart_upload_with_permit_in_transaction(
    storage: &Storage,
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
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    abort_multipart_upload_inner(
        storage,
        tenant_id,
        bucket_id,
        key,
        upload_id,
        permit.fence_token,
        Some(partition_precondition),
        Some((transaction_id, transaction_principal)),
    )
    .await
}

async fn abort_multipart_upload_inner(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction: Option<(&str, &str)>,
) -> Result<MultipartAbortMutation> {
    let Some(mut upload) = get_active_multipart_upload_for_optional_transaction(
        storage,
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
        storage,
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

pub async fn find_multipart_upload_partition(
    storage: &Storage,
    upload_row_id: i64,
) -> Result<Option<(i64, i64)>> {
    Ok(find_upload(storage, upload_row_id)
        .await?
        .map(|(tenant_id, bucket_id, _)| (tenant_id, bucket_id)))
}

pub async fn find_multipart_upload_partition_in_transaction(
    storage: &Storage,
    upload_row_id: i64,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Option<(i64, i64)>> {
    Ok(find_upload_for_optional_transaction(
        storage,
        upload_row_id,
        Some((transaction_id, transaction_principal)),
    )
    .await?
    .map(|(tenant_id, bucket_id, _)| (tenant_id, bucket_id)))
}

async fn find_upload(
    storage: &Storage,
    upload_row_id: i64,
) -> Result<Option<(i64, i64, MultipartUpload)>> {
    let store = CoreStore::new(storage.clone()).await?;
    read_upload_id_head(&store, upload_row_id)
}

async fn find_upload_for_optional_transaction(
    storage: &Storage,
    upload_row_id: i64,
    transaction: Option<(&str, &str)>,
) -> Result<Option<(i64, i64, MultipartUpload)>> {
    if transaction.is_none() {
        return find_upload(storage, upload_row_id).await;
    }
    let core_store = CoreStore::new(storage.clone()).await?;
    let transaction = read_transaction_for_optional_scope(&core_store, transaction).await?;
    let tuple_key = multipart_upload_id_head_key(upload_row_id)?;
    let payload = if let Some(transaction) = transaction.as_ref() {
        coremeta_payload_visible_to_transaction(
            &core_store,
            transaction,
            CF_OBJECT_HEADS,
            TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
            &tuple_key,
        )?
    } else {
        core_store.read_coremeta_row(
            CF_OBJECT_HEADS,
            TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
            &tuple_key,
        )?
    };
    decode_upload_id_head(payload.as_deref(), upload_row_id, false)
}

async fn read_transaction_for_optional_scope<'a>(
    core_store: &CoreStore,
    transaction: Option<(&'a str, &'a str)>,
) -> Result<Option<CoreTransaction>> {
    let Some((transaction_id, principal)) = transaction else {
        return Ok(None);
    };
    Ok(Some(
        core_store
            .read_explicit_transaction_for_principal(transaction_id, principal)
            .await?,
    ))
}

async fn read_current_part_for_optional_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    upload_row_id: i64,
    part_number: i32,
    transaction: Option<(&str, &str)>,
) -> Result<Option<MultipartUploadPart>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let tuple_key = multipart_part_row_key(tenant_id, bucket_id, upload_row_id, part_number)?;
    let transaction_scoped = transaction.is_some();
    let payload = if let Some(transaction) = transaction {
        let transaction = read_transaction_for_optional_scope(&core_store, Some(transaction))
            .await?
            .ok_or_else(|| anyhow!("multipart transaction scope is missing"))?;
        coremeta_payload_visible_to_transaction(
            &core_store,
            &transaction,
            CF_OBJECT_HEADS,
            TABLE_MULTIPART_PART_CURRENT_ROW,
            &tuple_key,
        )?
    } else {
        core_store.read_coremeta_row(
            CF_OBJECT_HEADS,
            TABLE_MULTIPART_PART_CURRENT_ROW,
            &tuple_key,
        )?
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
    storage: &Storage,
    upload_row_id: i64,
    transaction: Option<(&str, &str)>,
) -> Result<Vec<MultipartUploadPart>> {
    let Some((tenant_id, bucket_id, _)) =
        find_upload_for_optional_transaction(storage, upload_row_id, transaction).await?
    else {
        return Ok(Vec::new());
    };
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut parts =
        read_all_multipart_parts_bounded(&core_store, tenant_id, bucket_id, upload_row_id)?;
    if let Some(transaction) = transaction {
        let transaction = read_transaction_for_optional_scope(&core_store, Some(transaction))
            .await?
            .ok_or_else(|| anyhow!("multipart transaction scope is missing"))?;
        apply_transaction_parts(
            &transaction,
            tenant_id,
            bucket_id,
            upload_row_id,
            &mut parts,
        )?;
    }
    if parts.len() > MULTIPART_PART_COUNT_MAX {
        return Err(anyhow!(
            "multipart upload exceeds the bounded part count of {MULTIPART_PART_COUNT_MAX}"
        ));
    }
    Ok(parts.into_values().collect())
}

fn read_all_multipart_parts_bounded(
    store: &CoreStore,
    tenant_id: i64,
    bucket_id: i64,
    upload_row_id: i64,
) -> Result<BTreeMap<i32, MultipartUploadPart>> {
    let prefix = multipart_upload_part_rows_prefix(tenant_id, bucket_id, upload_row_id)?;
    let mut after_tuple_key = None;
    let mut parts = BTreeMap::new();
    loop {
        let remaining = MULTIPART_PART_COUNT_MAX
            .saturating_add(1)
            .saturating_sub(parts.len());
        let scan_limit = remaining.min(MULTIPART_PAGE_MAX);
        let records = store.scan_coremeta_prefix_page(
            CF_OBJECT_HEADS,
            TABLE_MULTIPART_PART_CURRENT_ROW,
            &prefix,
            after_tuple_key.as_deref(),
            scan_limit,
        )?;
        if records.is_empty() {
            return Ok(parts);
        }
        for record in &records {
            if parts.len() == MULTIPART_PART_COUNT_MAX {
                return Err(anyhow!(
                    "multipart upload exceeds the bounded part count of {MULTIPART_PART_COUNT_MAX}"
                ));
            }
            let row = decode_part_current_record(record)?;
            if row.tenant_id != tenant_id
                || row.bucket_id != bucket_id
                || row.part.upload_id != upload_row_id
            {
                return Err(anyhow!("multipart part page scope mismatch"));
            }
            after_tuple_key = Some(core_meta_record_tuple_key(&record.key)?.to_vec());
            if parts.insert(row.part.part_number, row.part).is_some() {
                return Err(anyhow!(
                    "multipart part table contains a duplicate part number"
                ));
            }
        }
        if records.len() < scan_limit {
            return Ok(parts);
        }
    }
}

fn read_upload_id_head(
    store: &CoreStore,
    upload_row_id: i64,
) -> Result<Option<(i64, i64, MultipartUpload)>> {
    let payload = store.read_coremeta_row(
        CF_OBJECT_HEADS,
        TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
        &multipart_upload_id_head_key(upload_row_id)?,
    )?;
    decode_upload_id_head(payload.as_deref(), upload_row_id, true)
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

fn page_multipart_parts_from_store(
    store: &CoreStore,
    tenant_id: i64,
    bucket_id: i64,
    upload_row_id: i64,
    part_number_marker: i32,
    page_size: usize,
) -> Result<MultipartPartsPage> {
    if !(1..=MULTIPART_PAGE_MAX).contains(&page_size) {
        return Err(anyhow!(
            "multipart page size must be between 1 and {MULTIPART_PAGE_MAX}"
        ));
    }
    let prefix = multipart_upload_part_rows_prefix(tenant_id, bucket_id, upload_row_id)?;
    let after_tuple_key = if part_number_marker == 0 {
        None
    } else {
        Some(multipart_part_row_key(
            tenant_id,
            bucket_id,
            upload_row_id,
            part_number_marker,
        )?)
    };
    let mut records = store.scan_coremeta_prefix_page(
        CF_OBJECT_HEADS,
        TABLE_MULTIPART_PART_CURRENT_ROW,
        &prefix,
        after_tuple_key.as_deref(),
        page_size + 1,
    )?;
    let is_truncated = records.len() > page_size;
    if is_truncated {
        records.truncate(page_size);
    }
    let mut parts = Vec::with_capacity(records.len());
    for record in records {
        let row = decode_part_current_record(&record)?;
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

fn apply_transaction_parts(
    transaction: &CoreTransaction,
    tenant_id: i64,
    bucket_id: i64,
    upload_row_id: i64,
    parts: &mut BTreeMap<i32, MultipartUploadPart>,
) -> Result<()> {
    let prefix = multipart_upload_part_rows_prefix(tenant_id, bucket_id, upload_row_id)?;
    for update in &transaction.visible_updates {
        match update {
            CoreTransactionUpdate::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                payload,
                ..
            } => {
                if canonical_coremeta_cf_name(cf)? != CF_OBJECT_HEADS
                    || *table_id != TABLE_MULTIPART_PART_CURRENT_ROW
                    || !tuple_key.starts_with(&prefix)
                {
                    continue;
                }
                let row = decode_part_current_row(payload)?;
                if row.tenant_id != tenant_id
                    || row.bucket_id != bucket_id
                    || row.part.upload_id != upload_row_id
                    || tuple_key
                        != &multipart_part_row_key(
                            tenant_id,
                            bucket_id,
                            upload_row_id,
                            row.part.part_number,
                        )?
                {
                    return Err(anyhow!("multipart transaction part row scope mismatch"));
                }
                parts.insert(row.part.part_number, row.part);
                if parts.len() > MULTIPART_PART_COUNT_MAX {
                    return Err(anyhow!(
                        "multipart upload exceeds the bounded part count of {MULTIPART_PART_COUNT_MAX}"
                    ));
                }
            }
            CoreTransactionUpdate::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                ..
            } => {
                if canonical_coremeta_cf_name(cf)? != CF_OBJECT_HEADS
                    || *table_id != TABLE_MULTIPART_PART_CURRENT_ROW
                    || !tuple_key.starts_with(&prefix)
                {
                    continue;
                }
                let mut deleted_part_number = None;
                for part_number in parts.keys().copied() {
                    if tuple_key
                        == &multipart_part_row_key(
                            tenant_id,
                            bucket_id,
                            upload_row_id,
                            part_number,
                        )?
                    {
                        deleted_part_number = Some(part_number);
                        break;
                    }
                }
                if let Some(part_number) = deleted_part_number {
                    parts.remove(&part_number);
                }
            }
            CoreTransactionUpdate::StreamAppend { .. } => {}
        }
    }
    Ok(())
}

fn decode_active_upload_record(record: &CoreMetaRecord) -> Result<MultipartUpload> {
    let upload = decode_committed_upload_current_row(&record.payload)?.upload;
    if core_meta_record_tuple_key(&record.key)?
        != multipart_active_upload_key(upload.bucket_id, &upload.key, upload.upload_id)?
    {
        return Err(anyhow!("multipart active upload physical row key mismatch"));
    }
    Ok(upload)
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
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    event: MultipartMutationKind,
    upload: Option<MultipartUpload>,
    part: Option<MultipartUploadPart>,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction: Option<(&str, &str)>,
) -> Result<MetadataMutationReceipt> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = multipart_metadata_stream_id(tenant_id, bucket_id);
    let mutation_id = uuid::Uuid::new_v4();
    let internal_transaction_id = format!("multipart-metadata:{mutation_id}");
    let transaction_record_id = transaction
        .map(|(transaction_id, _)| transaction_id.to_string())
        .unwrap_or_else(|| internal_transaction_id.clone());
    let body = encode_multipart_event(
        event,
        upload.as_ref(),
        part.as_ref(),
        fence_token,
        mutation_id,
    )?;
    let payload_hash = hex::encode(hash32(&body));
    let partition_id = hex::encode(multipart_metadata_partition_id(tenant_id, bucket_id));
    let data_root = multipart_current_root_key(tenant_id, bucket_id);
    let transaction_state = read_transaction_for_optional_scope(&core_store, transaction).await?;
    let scope_partition = transaction_state
        .as_ref()
        .map(|transaction| transaction.scope_partition.clone())
        .unwrap_or(partition_id);
    let root_publications = multipart_root_publications(data_root, scope_partition.clone());
    let current_update = if let Some(transaction_state) = transaction_state.as_ref() {
        multipart_current_row_update_with_transaction(
            &core_store,
            transaction_state,
            tenant_id,
            bucket_id,
            event,
            upload.as_ref(),
            part.as_ref(),
        )?
    } else {
        multipart_current_row_update(
            &core_store,
            tenant_id,
            bucket_id,
            event,
            upload.as_ref(),
            part.as_ref(),
        )?
    };
    let mut preconditions = partition_precondition.into_iter().collect::<Vec<_>>();
    preconditions.extend(current_update.preconditions.clone());
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: scope_partition.clone(),
        stream_id: stream_id.clone(),
        record_kind: "multipart_metadata".to_string(),
        payload: body.clone(),
        idempotency_key: Some(format!("multipart-metadata:{mutation_id}")),
    }];
    operations.extend(multipart_current_row_operations(
        &current_update,
        &scope_partition,
    )?);
    let committed_by_principal = transaction
        .map(|(_, principal)| principal.to_string())
        .unwrap_or_else(|| multipart_metadata_partition_principal(tenant_id, bucket_id));
    let batch = CoreMutationBatch {
        transaction_id: transaction_record_id,
        scope_partition,
        committed_by_principal,
        root_publications,
        preconditions,
        operations,
    };
    let batch_receipt = if transaction.is_some() {
        core_store.stage_explicit_transaction_batch(batch).await?
    } else {
        core_store.commit_mutation_batch(batch).await?
    };
    let stream_update = batch_receipt
        .visible_updates
        .iter()
        .find_map(|update| match update {
            CoreTransactionUpdate::StreamAppend {
                visible_sequence,
                prepared_record_hash,
                ..
            } => Some((*visible_sequence, prepared_record_hash.clone())),
            CoreTransactionUpdate::CoreMetaPut { .. }
            | CoreTransactionUpdate::CoreMetaDelete { .. } => None,
        })
        .ok_or_else(|| anyhow!("multipart metadata batch did not append stream record"))?;
    Ok(MetadataMutationReceipt {
        mutation_id,
        payload_hash,
        record_hash: stream_update.1,
        watch_cursor: stream_update.0,
    })
}

fn multipart_root_publications(
    data_root: String,
    coordinator_root: String,
) -> Vec<CoreMutationRootPublication> {
    if data_root == coordinator_root {
        return vec![CoreMutationRootPublication {
            root_anchor_key: data_root,
            writer_families: vec![
                WriterFamily::CoreControl.as_str().to_string(),
                WriterFamily::ObjectBlob.as_str().to_string(),
            ],
            transaction_coordinator: true,
        }];
    }

    vec![
        CoreMutationRootPublication::new(coordinator_root, WriterFamily::CoreControl.as_str())
            .coordinator(),
        CoreMutationRootPublication::new(data_root, WriterFamily::ObjectBlob.as_str()),
    ]
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

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) use tests::read_multipart_frame_fences as read_multipart_frame_fences_for_test;
