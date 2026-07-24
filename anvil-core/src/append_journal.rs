use crate::core_store::{
    CoreCompressionDescriptor, CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreMutationRootPublication, CoreObjectEncoding, CoreObjectPlacement, CoreObjectRef, CoreStore,
    CoreTransaction, CoreTransactionUpdate, ReadStream, StreamRecord,
};
use crate::formats::{Hash32, hash32, writer::WriterFamily};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::{
    AppendStream, AppendStreamMutation, AppendStreamRecord, AppendStreamRecordMutation,
    MetadataMutationReceipt, SealAppendStreamMutation,
};
use crate::storage::Storage;
use anyhow::{Result, anyhow, bail};
use chrono::Utc;
use prost::{Message, Oneof};

const APPEND_METADATA_BODY_SCHEMA: &str = "anvil.core.append_metadata.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendMutationKind {
    CreateStream,
    AppendRecord,
    SealStream,
}

impl AppendMutationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::CreateStream => "create_stream",
            Self::AppendRecord => "append_record",
            Self::SealStream => "seal_stream",
        }
    }
}

#[derive(Debug, Clone)]
struct AppendBody {
    event: String,
    stream: Option<AppendStream>,
    record: Option<AppendStreamRecord>,
    emitted_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct AppendBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    emitted_at: String,
    #[prost(uint64, tag = "3")]
    fence_token: u64,
    #[prost(string, tag = "4")]
    mutation_id: String,
    #[prost(oneof = "append_body_proto::Event", tags = "10, 11, 12")]
    event: Option<append_body_proto::Event>,
}

mod append_body_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Event {
        #[prost(message, tag = "10")]
        CreateStream(super::AppendStreamProto),
        #[prost(message, tag = "11")]
        AppendRecord(super::AppendStreamRecordProto),
        #[prost(message, tag = "12")]
        SealStream(super::AppendStreamProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct AppendStreamProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(int64, tag = "3")]
    bucket_id: i64,
    #[prost(string, tag = "4")]
    bucket_name: String,
    #[prost(string, tag = "5")]
    stream_key: String,
    #[prost(string, tag = "6")]
    stream_id: String,
    #[prost(string, tag = "7")]
    created_at: String,
    #[prost(string, optional, tag = "8")]
    sealed_at: Option<String>,
    #[prost(string, optional, tag = "9")]
    segment_hash: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct AppendStreamRecordProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    stream_id: i64,
    #[prost(int64, tag = "3")]
    record_sequence: i64,
    #[prost(string, tag = "4")]
    payload_hash: String,
    #[prost(message, optional, tag = "5")]
    payload_object_ref: Option<CoreObjectRefProto>,
    #[prost(int64, tag = "6")]
    payload_size: i64,
    #[prost(string, optional, tag = "7")]
    content_type: Option<String>,
    #[prost(bytes = "vec", tag = "8")]
    user_meta_json: Vec<u8>,
    #[prost(bool, tag = "9")]
    has_user_meta: bool,
    #[prost(string, tag = "10")]
    created_at: String,
    #[prost(string, tag = "11")]
    authenticated_principal: String,
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
    #[prost(bytes = "vec", tag = "13")]
    receipt_signature: Vec<u8>,
}

mod read;

pub use read::{
    AppendStreamPage, AppendStreamRecordPage, append_record_source_cursor,
    append_stream_has_records, get_active_append_stream, get_active_append_stream_in_transaction,
    list_append_stream_records_page, list_append_streams_page,
};
use read::{
    append_record_cursor_stream_id, append_record_stream_id, append_state_stream_id,
    get_active_append_stream_for_optional_transaction,
};

#[cfg(test)]
async fn create_append_stream(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &str,
    stream_key: &str,
) -> Result<AppendStreamMutation> {
    create_append_stream_inner(
        storage,
        tenant_id,
        bucket_id,
        bucket_name,
        stream_key,
        0,
        None,
        None,
        None,
    )
    .await
}

pub(crate) async fn create_append_stream_with_permit(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &str,
    stream_key: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<AppendStreamMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_append_stream_inner(
        storage,
        tenant_id,
        bucket_id,
        bucket_name,
        stream_key,
        permit.fence_token,
        Some(partition_precondition),
        None,
        None,
    )
    .await
}

pub(crate) async fn create_append_stream_with_permit_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &str,
    stream_key: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<AppendStreamMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_append_stream_inner(
        storage,
        tenant_id,
        bucket_id,
        bucket_name,
        stream_key,
        permit.fence_token,
        Some(partition_precondition),
        Some(transaction_id),
        Some(transaction_principal),
    )
    .await
}

async fn create_append_stream_inner(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &str,
    stream_key: &str,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<AppendStreamMutation> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let explicit_transaction =
        read_optional_explicit_transaction(&core_store, transaction_id, transaction_principal)
            .await?;
    let id = i64::try_from(
        next_stream_sequence_visible_to_transaction(
            &core_store,
            &append_metadata_stream_id(tenant_id, bucket_id),
            explicit_transaction.as_ref(),
        )
        .await?,
    )
    .map_err(|_| anyhow!("append stream id overflow"))?;
    let stream = AppendStream {
        id,
        tenant_id,
        bucket_id,
        bucket_name: bucket_name.to_string(),
        stream_key: stream_key.to_string(),
        stream_id: uuid::Uuid::new_v4(),
        created_at: Utc::now(),
        sealed_at: None,
        segment_hash: None,
    };
    let receipt = append_body(
        storage,
        tenant_id,
        bucket_id,
        AppendMutationKind::CreateStream,
        Some(stream.clone()),
        None,
        fence_token,
        partition_precondition,
        transaction_id,
        transaction_principal,
        transaction_principal,
    )
    .await?;
    Ok(AppendStreamMutation { stream, receipt })
}

#[cfg(test)]
async fn append_stream_record(
    storage: &Storage,
    stream: &AppendStream,
    payload_object_ref: CoreObjectRef,
    payload_size: i64,
) -> Result<AppendStreamRecordMutation> {
    append_stream_record_inner(
        storage,
        stream,
        payload_object_ref,
        payload_size,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn append_stream_record_with_permit(
    storage: &Storage,
    stream: &AppendStream,
    payload_object_ref: CoreObjectRef,
    payload_size: i64,
    content_type: Option<String>,
    user_meta: Option<serde_json::Value>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<AppendStreamRecordMutation> {
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    append_stream_record_inner(
        storage,
        stream,
        payload_object_ref,
        payload_size,
        content_type,
        user_meta,
        Some(permit),
        Some(partition_precondition),
        None,
        None,
        None,
    )
    .await
}

pub(crate) async fn append_stream_record_with_permit_in_partition(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    stream: &AppendStream,
    payload_object_ref: CoreObjectRef,
    payload_size: i64,
    content_type: Option<String>,
    user_meta: Option<serde_json::Value>,
    authenticated_principal: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<AppendStreamRecordMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    append_stream_record_inner(
        storage,
        stream,
        payload_object_ref,
        payload_size,
        content_type,
        user_meta,
        Some(permit),
        Some(partition_precondition),
        None,
        None,
        Some(authenticated_principal),
    )
    .await
}

pub(crate) async fn append_stream_record_with_permit_in_partition_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    stream: &AppendStream,
    payload_object_ref: CoreObjectRef,
    payload_size: i64,
    content_type: Option<String>,
    user_meta: Option<serde_json::Value>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<AppendStreamRecordMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    append_stream_record_inner(
        storage,
        stream,
        payload_object_ref,
        payload_size,
        content_type,
        user_meta,
        Some(permit),
        Some(partition_precondition),
        Some(transaction_id),
        Some(transaction_principal),
        Some(transaction_principal),
    )
    .await
}

async fn append_stream_record_inner(
    storage: &Storage,
    stream: &AppendStream,
    payload_object_ref: CoreObjectRef,
    payload_size: i64,
    content_type: Option<String>,
    user_meta: Option<serde_json::Value>,
    permit: Option<&PartitionWritePermit>,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
    authenticated_principal: Option<&str>,
) -> Result<AppendStreamRecordMutation> {
    let tenant_id = stream.tenant_id;
    let bucket_id = stream.bucket_id;
    if let Some(permit) = permit {
        require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    }
    let fence_token = permit.map(|permit| permit.fence_token).unwrap_or(0);
    let current = get_active_append_stream_for_optional_transaction(
        storage,
        tenant_id,
        bucket_id,
        &stream.stream_key,
        stream.stream_id,
        transaction_id.zip(transaction_principal),
    )
    .await?
    .ok_or_else(|| anyhow!("append stream not found"))?;
    if current.id != stream.id {
        bail!("append stream row id does not match stream identity");
    }
    let core_store = CoreStore::new(storage.clone()).await?;
    let explicit_transaction =
        read_optional_explicit_transaction(&core_store, transaction_id, transaction_principal)
            .await?;
    let next_seq = i64::try_from(
        next_stream_sequence_visible_to_transaction(
            &core_store,
            &append_record_stream_id(&current)?,
            explicit_transaction.as_ref(),
        )
        .await?,
    )
    .map_err(|_| anyhow!("append record sequence overflow"))?;
    let next_record_id = i64::try_from(
        next_stream_sequence_visible_to_transaction(
            &core_store,
            &append_metadata_stream_id(tenant_id, bucket_id),
            explicit_transaction.as_ref(),
        )
        .await?,
    )
    .map_err(|_| anyhow!("append record id overflow"))?;
    let record = AppendStreamRecord {
        id: next_record_id,
        stream_id: stream.id,
        record_sequence: next_seq,
        payload_hash: payload_object_ref.hash.clone(),
        payload_object_ref,
        payload_size,
        content_type,
        user_meta,
        authenticated_principal: authenticated_principal.unwrap_or_default().to_string(),
        created_at: Utc::now(),
    };
    let receipt = append_body(
        storage,
        tenant_id,
        bucket_id,
        AppendMutationKind::AppendRecord,
        Some(current),
        Some(record.clone()),
        fence_token,
        partition_precondition,
        transaction_id,
        transaction_principal,
        authenticated_principal,
    )
    .await?;
    Ok(AppendStreamRecordMutation { record, receipt })
}

#[cfg(test)]
async fn seal_append_stream(
    storage: &Storage,
    stream: &AppendStream,
    segment_hash: &str,
) -> Result<SealAppendStreamMutation> {
    seal_append_stream_inner(storage, stream, segment_hash, None, None, None, None).await
}

#[cfg(test)]
pub(crate) async fn seal_append_stream_with_permit(
    storage: &Storage,
    stream: &AppendStream,
    segment_hash: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<SealAppendStreamMutation> {
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    seal_append_stream_inner(
        storage,
        stream,
        segment_hash,
        Some(permit),
        Some(partition_precondition),
        None,
        None,
    )
    .await
}

pub(crate) async fn seal_append_stream_with_permit_in_partition(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    stream: &AppendStream,
    segment_hash: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<SealAppendStreamMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    seal_append_stream_inner(
        storage,
        stream,
        segment_hash,
        Some(permit),
        Some(partition_precondition),
        None,
        None,
    )
    .await
}

pub(crate) async fn seal_append_stream_with_permit_in_partition_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    stream: &AppendStream,
    segment_hash: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<SealAppendStreamMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    seal_append_stream_inner(
        storage,
        stream,
        segment_hash,
        Some(permit),
        Some(partition_precondition),
        Some(transaction_id),
        Some(transaction_principal),
    )
    .await
}

async fn seal_append_stream_inner(
    storage: &Storage,
    expected_stream: &AppendStream,
    segment_hash: &str,
    permit: Option<&PartitionWritePermit>,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<SealAppendStreamMutation> {
    let tenant_id = expected_stream.tenant_id;
    let bucket_id = expected_stream.bucket_id;
    let Some(mut stream) = get_active_append_stream_for_optional_transaction(
        storage,
        tenant_id,
        bucket_id,
        &expected_stream.stream_key,
        expected_stream.stream_id,
        transaction_id.zip(transaction_principal),
    )
    .await?
    else {
        return Ok(SealAppendStreamMutation {
            sealed: false,
            receipt: None,
        });
    };
    if stream.id != expected_stream.id {
        bail!("append stream row id does not match stream identity");
    }
    if let Some(permit) = permit {
        require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    }
    let fence_token = permit.map(|permit| permit.fence_token).unwrap_or(0);
    stream.sealed_at = Some(Utc::now());
    stream.segment_hash = Some(segment_hash.to_string());
    let receipt = append_body(
        storage,
        tenant_id,
        bucket_id,
        AppendMutationKind::SealStream,
        Some(stream),
        None,
        fence_token,
        partition_precondition,
        transaction_id,
        transaction_principal,
        transaction_principal,
    )
    .await?;
    Ok(SealAppendStreamMutation {
        sealed: true,
        receipt: Some(receipt),
    })
}

pub async fn materialize_committed_append_streams_transaction(
    storage: &Storage,
    transaction: &CoreTransaction,
) -> Result<Vec<AppendStream>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut streams = Vec::new();
    for update in &transaction.visible_updates {
        let CoreTransactionUpdate::StreamAppend {
            stream_id,
            visible_sequence,
            prepared_record_hash,
            ..
        } = update
        else {
            continue;
        };
        if !stream_id.starts_with("append_metadata:tenant:") {
            continue;
        }
        let after_sequence = visible_sequence.saturating_sub(1);
        let records = core_store
            .read_stream(ReadStream {
                stream_id: stream_id.clone(),
                after_sequence,
                limit: 1,
            })
            .await?;
        for record in records {
            if record.sequence != *visible_sequence || record.event_hash != *prepared_record_hash {
                continue;
            }
            let body = decode_append_body(&record.payload)?;
            if body.event == "create_stream"
                && let Some(stream) = body.stream
            {
                streams.push(stream);
            }
        }
    }
    Ok(streams)
}

async fn append_body(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    event: AppendMutationKind,
    stream: Option<AppendStream>,
    record: Option<AppendStreamRecord>,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
    committed_by_principal: Option<&str>,
) -> Result<MetadataMutationReceipt> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let explicit_transaction = match (transaction_id, transaction_principal) {
        (Some(transaction_id), Some(transaction_principal)) => Some(
            core_store
                .read_explicit_transaction_for_principal(transaction_id, transaction_principal)
                .await?,
        ),
        (None, None) => None,
        _ => {
            return Err(anyhow!(
                "append transaction id and principal must be provided together"
            ));
        }
    };
    let journal_stream_id = append_metadata_stream_id(tenant_id, bucket_id);
    let stream = stream.ok_or_else(|| anyhow!("append event is missing stream identity"))?;
    if stream.tenant_id != tenant_id || stream.bucket_id != bucket_id {
        bail!("append event stream identity does not match journal partition");
    }
    let exact_stream_id = match event {
        AppendMutationKind::CreateStream | AppendMutationKind::SealStream => {
            append_state_stream_id(&stream)?
        }
        AppendMutationKind::AppendRecord => append_record_stream_id(&stream)?,
    };
    let journal_precondition = core_store
        .stream_head_precondition_visible_to_transaction(
            &journal_stream_id,
            explicit_transaction.as_ref(),
        )
        .await?;
    let exact_precondition = core_store
        .stream_head_precondition_visible_to_transaction(
            &exact_stream_id,
            explicit_transaction.as_ref(),
        )
        .await?;
    let record_cursor_stream_id = matches!(event, AppendMutationKind::AppendRecord)
        .then(|| append_record_cursor_stream_id(tenant_id, bucket_id));
    let record_cursor_precondition = if let Some(stream_id) = record_cursor_stream_id.as_deref() {
        Some(
            core_store
                .stream_head_precondition_visible_to_transaction(
                    stream_id,
                    explicit_transaction.as_ref(),
                )
                .await?,
        )
    } else {
        None
    };
    let expected_journal_sequence = expected_stream_next_sequence(&journal_precondition)?;
    let expected_exact_sequence = expected_stream_next_sequence(&exact_precondition)?;
    match event {
        AppendMutationKind::CreateStream => {
            if u64::try_from(stream.id).ok() != Some(expected_journal_sequence) {
                bail!("append stream id does not match the journal head");
            }
            if expected_exact_sequence != 1 {
                bail!("append stream identity already exists");
            }
        }
        AppendMutationKind::AppendRecord => {
            let append_record = record
                .as_ref()
                .ok_or_else(|| anyhow!("append record event is missing record"))?;
            if u64::try_from(append_record.id).ok() != Some(expected_journal_sequence)
                || u64::try_from(append_record.record_sequence).ok()
                    != Some(expected_exact_sequence)
                || append_record.stream_id != stream.id
            {
                bail!("append record ids do not match journal heads");
            }
        }
        AppendMutationKind::SealStream => {
            if record.is_some() {
                bail!("append stream seal event must not contain a record");
            }
        }
    }
    let mutation_id = uuid::Uuid::new_v4();
    let body = AppendBody {
        event: event.as_str().to_string(),
        stream: Some(stream),
        record,
        emitted_at: Utc::now().to_rfc3339(),
    };
    let payload = encode_append_body(&body, fence_token, mutation_id)?;
    let payload_hash = hex::encode(hash32(&payload));
    let partition_id = hex::encode(append_metadata_partition_id(tenant_id, bucket_id));
    let scope_partition = explicit_transaction
        .as_ref()
        .map(|transaction| transaction.scope_partition.clone())
        .unwrap_or(partition_id);
    let mut preconditions: Vec<_> = partition_precondition.into_iter().collect();
    preconditions.push(journal_precondition);
    preconditions.push(exact_precondition);
    preconditions.extend(record_cursor_precondition);
    let mut operations = vec![
        CoreMutationOperation::StreamAppend {
            partition_id: scope_partition.clone(),
            stream_id: journal_stream_id.clone(),
            record_kind: "append_metadata".to_string(),
            payload: payload.clone(),
            idempotency_key: Some(format!(
                "append-metadata-journal:{tenant_id}:{bucket_id}:{mutation_id}"
            )),
        },
        CoreMutationOperation::StreamAppend {
            partition_id: scope_partition.clone(),
            stream_id: exact_stream_id,
            record_kind: match event {
                AppendMutationKind::CreateStream | AppendMutationKind::SealStream => {
                    "append_metadata.state"
                }
                AppendMutationKind::AppendRecord => "append_metadata.record",
            }
            .to_string(),
            payload: payload.clone(),
            idempotency_key: Some(format!(
                "append-metadata-exact:{tenant_id}:{bucket_id}:{mutation_id}"
            )),
        },
    ];
    if let Some(stream_id) = record_cursor_stream_id {
        operations.push(CoreMutationOperation::StreamAppend {
            partition_id: scope_partition.clone(),
            stream_id,
            record_kind: "append_metadata.record_cursor".to_string(),
            payload,
            idempotency_key: Some(format!(
                "append-metadata-record-cursor:{tenant_id}:{bucket_id}:{mutation_id}"
            )),
        });
    }
    let batch = CoreMutationBatch {
        transaction_id: transaction_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("append-metadata:{tenant_id}:{bucket_id}:{mutation_id}")),
        scope_partition: scope_partition.clone(),
        committed_by_principal: transaction_principal
            .or(committed_by_principal)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| append_metadata_partition_principal(tenant_id, bucket_id)),
        root_publications: vec![
            CoreMutationRootPublication::new(scope_partition, WriterFamily::CoreControl.as_str())
                .coordinator(),
        ],
        preconditions,
        operations,
    };
    let batch_receipt = if transaction_id.is_some() {
        core_store.stage_explicit_transaction_batch(batch).await?
    } else {
        core_store.commit_mutation_batch(batch).await?
    };
    let stream_update = batch_receipt
        .visible_updates
        .iter()
        .find_map(|update| match update {
            CoreTransactionUpdate::StreamAppend {
                stream_id,
                visible_sequence,
                prepared_record_hash,
                ..
            } if stream_id == &journal_stream_id => {
                Some((*visible_sequence, prepared_record_hash.clone()))
            }
            _ => None,
        })
        .ok_or_else(|| anyhow!("append metadata batch did not append stream record"))?;
    Ok(MetadataMutationReceipt {
        mutation_id,
        payload_hash,
        record_hash: stream_update.1,
        watch_cursor: stream_update.0,
    })
}

fn expected_stream_next_sequence(precondition: &CoreMutationPrecondition) -> Result<u64> {
    let CoreMutationPrecondition::StreamHead {
        expected_last_sequence,
        ..
    } = precondition
    else {
        bail!("append journal expected a stream-head precondition");
    };
    expected_last_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("append stream sequence overflow"))
}

async fn read_optional_explicit_transaction(
    core_store: &CoreStore,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<Option<CoreTransaction>> {
    match (transaction_id, transaction_principal) {
        (Some(transaction_id), Some(transaction_principal)) => core_store
            .read_explicit_transaction_for_principal(transaction_id, transaction_principal)
            .await
            .map(Some),
        (None, None) => Ok(None),
        _ => Err(anyhow!(
            "append transaction id and principal must be provided together"
        )),
    }
}

async fn next_stream_sequence_visible_to_transaction(
    core_store: &CoreStore,
    stream_id: &str,
    transaction: Option<&CoreTransaction>,
) -> Result<u64> {
    expected_stream_next_sequence(
        &core_store
            .stream_head_precondition_visible_to_transaction(stream_id, transaction)
            .await?,
    )
}

fn encode_append_body(
    body: &AppendBody,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    let event = match body.event.as_str() {
        "create_stream" => append_body_proto::Event::CreateStream(stream_to_proto(
            body.stream
                .as_ref()
                .ok_or_else(|| anyhow!("append create body is missing stream"))?,
        )),
        "append_record" => append_body_proto::Event::AppendRecord(record_to_proto(
            body.record
                .as_ref()
                .ok_or_else(|| anyhow!("append record body is missing record"))?,
        )?),
        "seal_stream" => append_body_proto::Event::SealStream(stream_to_proto(
            body.stream
                .as_ref()
                .ok_or_else(|| anyhow!("append seal body is missing stream"))?,
        )),
        other => return Err(anyhow!("unknown append metadata event {other}")),
    };
    encode_deterministic_proto(&AppendBodyProto {
        schema: APPEND_METADATA_BODY_SCHEMA.to_string(),
        emitted_at: body.emitted_at.clone(),
        fence_token,
        mutation_id: mutation_id.to_string(),
        event: Some(event),
    })
}

fn decode_append_body(bytes: &[u8]) -> Result<AppendBody> {
    let proto = AppendBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "append metadata body")?;
    if proto.schema != APPEND_METADATA_BODY_SCHEMA {
        return Err(anyhow!("append metadata body schema mismatch"));
    }
    let _mutation_id = uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("append metadata body has invalid mutation id"))?;
    let event = proto
        .event
        .ok_or_else(|| anyhow!("append metadata body is missing event"))?;
    let emitted_at = proto.emitted_at;
    Ok(match event {
        append_body_proto::Event::CreateStream(stream) => AppendBody {
            event: "create_stream".to_string(),
            stream: Some(stream_from_proto(stream)?),
            record: None,
            emitted_at,
        },
        append_body_proto::Event::AppendRecord(record) => AppendBody {
            event: "append_record".to_string(),
            stream: None,
            record: Some(record_from_proto(record)?),
            emitted_at,
        },
        append_body_proto::Event::SealStream(stream) => AppendBody {
            event: "seal_stream".to_string(),
            stream: Some(stream_from_proto(stream)?),
            record: None,
            emitted_at,
        },
    })
}

#[cfg(test)]
fn decode_append_body_fence(bytes: &[u8]) -> Result<u64> {
    let proto = AppendBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "append metadata body")?;
    if proto.schema != APPEND_METADATA_BODY_SCHEMA {
        return Err(anyhow!("append metadata body schema mismatch"));
    }
    Ok(proto.fence_token)
}

fn stream_to_proto(stream: &AppendStream) -> AppendStreamProto {
    AppendStreamProto {
        id: stream.id,
        tenant_id: stream.tenant_id,
        bucket_id: stream.bucket_id,
        bucket_name: stream.bucket_name.clone(),
        stream_key: stream.stream_key.clone(),
        stream_id: stream.stream_id.to_string(),
        created_at: stream.created_at.to_rfc3339(),
        sealed_at: stream.sealed_at.as_ref().map(chrono::DateTime::to_rfc3339),
        segment_hash: stream.segment_hash.clone(),
    }
}

fn stream_from_proto(proto: AppendStreamProto) -> Result<AppendStream> {
    Ok(AppendStream {
        id: proto.id,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        bucket_name: proto.bucket_name,
        stream_key: proto.stream_key,
        stream_id: uuid::Uuid::parse_str(&proto.stream_id)?,
        created_at: chrono::DateTime::parse_from_rfc3339(&proto.created_at)?.with_timezone(&Utc),
        sealed_at: proto
            .sealed_at
            .map(|value| chrono::DateTime::parse_from_rfc3339(&value))
            .transpose()?
            .map(|value| value.with_timezone(&Utc)),
        segment_hash: proto.segment_hash,
    })
}

fn record_to_proto(record: &AppendStreamRecord) -> Result<AppendStreamRecordProto> {
    Ok(AppendStreamRecordProto {
        id: record.id,
        stream_id: record.stream_id,
        record_sequence: record.record_sequence,
        payload_hash: record.payload_hash.clone(),
        payload_object_ref: Some(object_ref_to_proto(&record.payload_object_ref)),
        payload_size: record.payload_size,
        content_type: record.content_type.clone(),
        user_meta_json: record
            .user_meta
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()?
            .unwrap_or_default(),
        has_user_meta: record.user_meta.is_some(),
        created_at: record.created_at.to_rfc3339(),
        authenticated_principal: record.authenticated_principal.clone(),
    })
}

fn record_from_proto(proto: AppendStreamRecordProto) -> Result<AppendStreamRecord> {
    Ok(AppendStreamRecord {
        id: proto.id,
        stream_id: proto.stream_id,
        record_sequence: proto.record_sequence,
        payload_hash: proto.payload_hash,
        payload_object_ref: object_ref_from_proto(
            proto
                .payload_object_ref
                .ok_or_else(|| anyhow!("append record body is missing payload object ref"))?,
        )?,
        payload_size: proto.payload_size,
        content_type: proto.content_type,
        user_meta: if proto.has_user_meta {
            Some(serde_json::from_slice(&proto.user_meta_json)?)
        } else {
            None
        },
        authenticated_principal: proto.authenticated_principal,
        created_at: chrono::DateTime::parse_from_rfc3339(&proto.created_at)?.with_timezone(&Utc),
    })
}

fn object_ref_to_proto(value: &CoreObjectRef) -> CoreObjectRefProto {
    CoreObjectRefProto {
        hash: value.hash.clone(),
        logical_size: value.logical_size,
        manifest_ref: value.manifest_ref.clone(),
        encoding: Some(CoreObjectEncodingProto {
            block_id: value.encoding.block_id.clone(),
            profile_id: value.encoding.profile_id.clone(),
            data_shards: value.encoding.data_shards as u32,
            parity_shards: value.encoding.parity_shards as u32,
            minimum_read_shards: value.encoding.minimum_read_shards as u32,
            minimum_write_ack_shards: value.encoding.minimum_write_ack_shards as u32,
            stripe_size: value.encoding.stripe_size,
            placement_scope: value.encoding.placement_scope.clone(),
            repair_priority: value.encoding.repair_priority.clone(),
            stored_hash: value.encoding.stored_hash.clone(),
            compression: Some(object_compression_to_proto(&value.encoding.compression)),
            encryption: value.encoding.encryption.clone(),
        }),
        placements: value
            .placements
            .iter()
            .map(|placement| CoreObjectPlacementProto {
                shard_index: placement.shard_index as u32,
                node_id: placement.node_id.clone(),
                region_id: placement.region_id.clone(),
                cell_id: placement.cell_id.clone(),
                shard_hash: placement.shard_hash.clone(),
                stored_size: placement.stored_size,
                generation: placement.generation,
                placement_epoch: placement.placement_epoch,
                fsync_sequence: placement.fsync_sequence,
                written_at_unix_nanos: placement.written_at_unix_nanos,
                signed_payload_hash: placement.signed_payload_hash.clone(),
                signature_algorithm: placement.signature_algorithm.clone(),
                receipt_signature: placement.receipt_signature.clone(),
            })
            .collect(),
    }
}

fn object_ref_from_proto(value: CoreObjectRefProto) -> Result<CoreObjectRef> {
    let encoding = value
        .encoding
        .ok_or_else(|| anyhow!("append payload object ref is missing encoding"))?;
    Ok(CoreObjectRef {
        hash: value.hash,
        logical_size: value.logical_size,
        manifest_ref: value.manifest_ref,
        encoding: CoreObjectEncoding {
            block_id: encoding.block_id,
            profile_id: encoding.profile_id,
            data_shards: encoding.data_shards as u16,
            parity_shards: encoding.parity_shards as u16,
            minimum_read_shards: encoding.minimum_read_shards as u16,
            minimum_write_ack_shards: encoding.minimum_write_ack_shards as u16,
            stripe_size: encoding.stripe_size,
            placement_scope: encoding.placement_scope,
            repair_priority: encoding.repair_priority,
            stored_hash: encoding.stored_hash,
            compression: object_compression_from_proto(encoding.compression.ok_or_else(|| {
                anyhow!("append payload object ref is missing compression descriptor")
            })?),
            encryption: encoding.encryption,
        },
        placements: value
            .placements
            .into_iter()
            .map(|placement| CoreObjectPlacement {
                shard_index: placement.shard_index as u16,
                node_id: placement.node_id,
                region_id: placement.region_id,
                cell_id: placement.cell_id,
                shard_hash: placement.shard_hash,
                stored_size: placement.stored_size,
                generation: placement.generation,
                placement_epoch: placement.placement_epoch,
                fsync_sequence: placement.fsync_sequence,
                written_at_unix_nanos: placement.written_at_unix_nanos,
                signed_payload_hash: placement.signed_payload_hash,
                signature_algorithm: placement.signature_algorithm,
                receipt_signature: placement.receipt_signature,
            })
            .collect(),
    })
}

fn object_compression_to_proto(value: &CoreCompressionDescriptor) -> CoreObjectCompressionProto {
    CoreObjectCompressionProto {
        algorithm: value.algorithm.clone(),
        level: value.level,
        uncompressed_length: value.uncompressed_length,
        compressed_length: value.compressed_length,
        dictionary_id: value.dictionary_id.clone(),
        descriptor_hash: value.descriptor_hash.clone(),
    }
}

fn object_compression_from_proto(value: CoreObjectCompressionProto) -> CoreCompressionDescriptor {
    CoreCompressionDescriptor {
        algorithm: value.algorithm,
        level: value.level,
        uncompressed_length: value.uncompressed_length,
        compressed_length: value.compressed_length,
        dictionary_id: value.dictionary_id,
        descriptor_hash: value.descriptor_hash,
    }
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_deterministic_proto(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    if encode_deterministic_proto(message)? != bytes {
        return Err(anyhow!("{label} is not deterministically encoded"));
    }
    Ok(())
}

pub fn append_metadata_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/append").as_bytes())
}

#[cfg(test)]
pub(crate) async fn read_append_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = append_metadata_stream_id(tenant_id, bucket_id);
    let mut after_sequence = 0;
    let mut fences = Vec::new();
    loop {
        let page = core_store
            .read_stream_page(ReadStream {
                stream_id: stream_id.clone(),
                after_sequence,
                limit: 256,
            })
            .await?;
        for record in page.records {
            if record.record_kind == "append_metadata" {
                fences.push(decode_append_body_fence(&record.payload)?);
            }
        }
        if !page.has_more {
            return Ok(fences);
        }
        after_sequence = page.next_sequence;
    }
}

fn append_metadata_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("append_metadata:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn append_metadata_partition_principal(tenant_id: i64, bucket_id: i64) -> String {
    format!("partition-owner:append_metadata:{tenant_id}:{bucket_id}")
}

fn require_append_metadata_permit(
    tenant_id: i64,
    bucket_id: i64,
    permit: &PartitionWritePermit,
) -> Result<()> {
    let expected_partition_id = hex::encode(append_metadata_partition_id(tenant_id, bucket_id));
    if permit.partition_family != "append_metadata" || permit.partition_id != expected_partition_id
    {
        anyhow::bail!("append metadata write permit targets a different partition");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const KEY: &[u8] = b"append journal partition owner key";

    #[tokio::test]
    async fn append_journal_replays_stream_records_and_seal() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let stream = create_append_stream(&storage, 1, 2, "bucket", "stream")
            .await
            .unwrap()
            .stream;
        append_stream_record(&storage, &stream, payload_ref("hash-a", 10), 10)
            .await
            .unwrap();
        append_stream_record(&storage, &stream, payload_ref("hash-b", 20), 20)
            .await
            .unwrap();
        assert_eq!(
            list_append_stream_records_page(&storage, &stream, 0, 100)
                .await
                .unwrap()
                .records
                .len(),
            2
        );
        assert!(
            get_active_append_stream(&storage, 1, 2, "stream", stream.stream_id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            seal_append_stream(&storage, &stream, "seg")
                .await
                .unwrap()
                .sealed
        );
        assert!(
            get_active_append_stream(&storage, 1, 2, "stream", stream.stream_id)
                .await
                .unwrap()
                .is_some()
        );
        append_stream_record(&storage, &stream, payload_ref("hash-c", 30), 30)
            .await
            .unwrap();
        assert_eq!(
            list_append_stream_records_page(&storage, &stream, 0, 100)
                .await
                .unwrap()
                .records
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn append_journal_pages_exact_streams_and_commits_both_projections() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let left = create_append_stream(&storage, 1, 2, "bucket", "left")
            .await
            .unwrap()
            .stream;
        let right = create_append_stream(&storage, 1, 2, "bucket", "right")
            .await
            .unwrap()
            .stream;

        for (label, size) in [("left-a", 10), ("left-b", 20), ("left-c", 30)] {
            append_stream_record_inner(
                &storage,
                &left,
                payload_ref(label, size),
                i64::try_from(size).unwrap(),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        }
        append_stream_record_inner(
            &storage,
            &right,
            payload_ref("right-a", 40),
            40,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let first = list_append_stream_records_page(&storage, &left, 0, 2)
            .await
            .unwrap();
        assert_eq!(first.records.len(), 2);
        assert_eq!(first.next_sequence, 2);
        assert!(first.has_more);
        let second = list_append_stream_records_page(&storage, &left, first.next_sequence, 2)
            .await
            .unwrap();
        assert_eq!(second.records.len(), 1);
        assert_eq!(second.records[0].record_sequence, 3);
        assert!(!second.has_more);

        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        assert_eq!(
            core_store
                .stream_head_sequence(&append_state_stream_id(&left).unwrap())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            core_store
                .stream_head_sequence(&append_record_stream_id(&left).unwrap())
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            core_store
                .stream_head_sequence(&append_record_cursor_stream_id(1, 2))
                .await
                .unwrap(),
            4
        );
        assert_eq!(
            append_record_source_cursor(&storage, 1, 2).await.unwrap(),
            6
        );
        assert!(
            list_append_stream_records_page(&storage, &left, 0, 0)
                .await
                .unwrap_err()
                .to_string()
                .contains("page size")
        );
    }

    #[tokio::test]
    pub(crate) async fn append_journal_with_permit_writes_fenced_frames_and_header() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let permit = owner.write_permit().unwrap();

        let stream =
            create_append_stream_with_permit(&storage, 1, 2, "bucket", "stream", &permit, KEY)
                .await
                .unwrap();
        append_stream_record_with_permit_in_partition(
            &storage,
            1,
            2,
            &stream.stream,
            payload_ref("hash-a", 10),
            10,
            None,
            None,
            "tenant/1/principal/producer-a",
            &permit,
            KEY,
        )
        .await
        .unwrap();
        let records = list_append_stream_records_page(&storage, &stream.stream, 0, 100)
            .await
            .unwrap()
            .records;
        assert_eq!(
            records[0].authenticated_principal,
            "tenant/1/principal/producer-a"
        );
        seal_append_stream_with_permit(&storage, &stream.stream, "segment-a", &permit, KEY)
            .await
            .unwrap();

        let fences = read_append_frame_fences_for_test(&storage, 1, 2)
            .await
            .unwrap();
        assert_eq!(fences.len(), 3);
        assert!(fences.iter().all(|fence| *fence == permit.fence_token));
    }

    #[tokio::test]
    pub(crate) async fn append_journal_with_permit_rejects_stale_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let stream = create_append_stream_with_permit(
            &storage,
            1,
            2,
            "bucket",
            "stream",
            &stale_permit,
            KEY,
        )
        .await
        .unwrap();
        let newer = ready_owner(&storage, 1, 2, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = append_stream_record_with_permit(
            &storage,
            &stream.stream,
            payload_ref("hash-a", 10),
            10,
            None,
            None,
            &stale_permit,
            KEY,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("write permit owner is not current")
        );
    }

    #[tokio::test]
    pub(crate) async fn append_journal_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let stream = create_append_stream_with_permit(
            &storage,
            1,
            2,
            "bucket",
            "stream",
            &stale_permit,
            KEY,
        )
        .await
        .unwrap();
        let stale_precondition = partition_write_precondition(&storage, &stale_permit, KEY)
            .await
            .unwrap();
        let newer = ready_owner(&storage, 1, 2, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = append_stream_record_inner(
            &storage,
            &stream.stream,
            payload_ref("hash-a", 10),
            10,
            None,
            None,
            Some(&stale_permit),
            Some(stale_precondition),
            None,
            None,
            None,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("target mismatch")
                || err.to_string().contains("generation mismatch")
                || err.to_string().contains("payload hash mismatch"),
            "unexpected error: {err:?}"
        );
    }

    async fn ready_owner(
        storage: &Storage,
        tenant_id: i64,
        bucket_id: i64,
        owner_node_id: &str,
    ) -> crate::partition_fence::PartitionOwnerState {
        let family = "append_metadata".to_string();
        let id = hex::encode(append_metadata_partition_id(tenant_id, bucket_id));
        crate::partition_fence::ready_partition_owner_for_test(
            storage,
            family,
            id,
            owner_node_id,
            0,
            hex::encode([0; 32]),
            hex::encode([1; 32]),
            KEY,
        )
        .await
    }

    fn payload_ref(label: &str, logical_size: u64) -> CoreObjectRef {
        CoreObjectRef::test_unlocated(
            format!(
                "sha256:{}",
                hex::encode(blake3::hash(label.as_bytes()).as_bytes())
            ),
            logical_size,
            format!("manifest:{label}"),
        )
    }
}
