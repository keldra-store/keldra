use crate::core_store::{
    CoreCompressionDescriptor, CoreObjectEncoding, CoreObjectPlacement, CoreObjectRef,
};
use crate::formats::{Hash32, hash32, writer::WriterFamily};
use crate::partition_fence::PartitionWritePermit;
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
    AppendStreamPage, AppendStreamRecordPage, append_record_source_cursor_mvcc,
    append_stream_has_records, get_active_append_stream_in_transaction,
    get_active_append_stream_mvcc, list_append_stream_records_page_mvcc,
    list_append_streams_page_mvcc,
};
use read::{append_record_cursor_stream_id, append_record_stream_id, append_state_stream_id};

pub(crate) async fn create_append_stream_with_permit_mvcc(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &str,
    stream_key: &str,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<AppendStreamMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    let journal_head_key = crate::mvcc_product::stream_logical_key(
        crate::core_store::TABLE_STREAM_HEAD_ROW,
        &append_metadata_stream_id(tenant_id, bucket_id),
        None,
    )?;
    let id = mvcc
        .read_latest_value(&journal_head_key)?
        .map(|payload| decode_append_body(&payload))
        .transpose()?
        .and_then(|body| body.stream.map(|stream| stream.id))
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow!("append stream id overflow"))?;
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
    let (receipt, mutations) = append_body_mvcc_mutations(
        tenant_id,
        bucket_id,
        AppendMutationKind::CreateStream,
        stream.clone(),
        None,
        permit.fence_token,
    )?;
    commit_append_mutations(
        mvcc,
        tenant_id,
        bucket_id,
        &append_metadata_partition_principal(tenant_id, bucket_id),
        &format!("append-create:{}", receipt.mutation_id),
        mutations,
    )
    .await?;
    Ok(AppendStreamMutation { stream, receipt })
}

pub(crate) async fn create_append_stream_with_permit_in_transaction(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &str,
    stream_key: &str,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<AppendStreamMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    stage_append_assignment_guard(
        mvcc,
        tenant_id,
        bucket_id,
        transaction_id,
        transaction_principal,
    )
    .await?;
    let journal_head = crate::mvcc_product::stream_logical_key(
        crate::core_store::TABLE_STREAM_HEAD_ROW,
        &append_metadata_stream_id(tenant_id, bucket_id),
        None,
    )?;
    let id = next_append_id_in_transaction(
        mvcc,
        transaction_id,
        transaction_principal,
        &journal_head,
        false,
    )?;
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
    let receipt = stage_append_body_mvcc(
        mvcc,
        tenant_id,
        bucket_id,
        AppendMutationKind::CreateStream,
        stream.clone(),
        None,
        permit.fence_token,
        transaction_id,
        transaction_principal,
    )?;
    Ok(AppendStreamMutation { stream, receipt })
}

pub(crate) async fn append_stream_record_with_permit_in_partition(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    stream: &AppendStream,
    payload_object_ref: CoreObjectRef,
    payload_size: i64,
    content_type: Option<String>,
    user_meta: Option<serde_json::Value>,
    authenticated_principal: &str,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<AppendStreamRecordMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    let current = get_active_append_stream_mvcc(
        mvcc,
        tenant_id,
        bucket_id,
        &stream.stream_key,
        stream.stream_id,
    )
    .await?
    .ok_or_else(|| anyhow!("append stream not found"))?;
    let journal_head = crate::mvcc_product::stream_logical_key(
        crate::core_store::TABLE_STREAM_HEAD_ROW,
        &append_metadata_stream_id(tenant_id, bucket_id),
        None,
    )?;
    let record_head = crate::mvcc_product::stream_logical_key(
        crate::core_store::TABLE_STREAM_HEAD_ROW,
        &append_record_stream_id(&current)?,
        None,
    )?;
    let next_record_id = next_append_id_from_mvcc_head(mvcc, &journal_head, false)?;
    let next_sequence = next_append_id_from_mvcc_head(mvcc, &record_head, true)?;
    let record = AppendStreamRecord {
        id: next_record_id,
        stream_id: current.id,
        record_sequence: next_sequence,
        payload_hash: payload_object_ref.hash.clone(),
        payload_object_ref,
        payload_size,
        content_type,
        user_meta,
        authenticated_principal: authenticated_principal.to_string(),
        created_at: Utc::now(),
    };
    let (receipt, mutations) = append_body_mvcc_mutations(
        tenant_id,
        bucket_id,
        AppendMutationKind::AppendRecord,
        current,
        Some(record.clone()),
        permit.fence_token,
    )?;
    commit_append_mutations(
        mvcc,
        tenant_id,
        bucket_id,
        authenticated_principal,
        &format!("append-record:{}", receipt.mutation_id),
        mutations,
    )
    .await?;
    Ok(AppendStreamRecordMutation { record, receipt })
}

pub(crate) async fn append_stream_record_with_permit_in_partition_transaction(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    stream: &AppendStream,
    payload_object_ref: CoreObjectRef,
    payload_size: i64,
    content_type: Option<String>,
    user_meta: Option<serde_json::Value>,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<AppendStreamRecordMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    stage_append_assignment_guard(
        mvcc,
        tenant_id,
        bucket_id,
        transaction_id,
        transaction_principal,
    )
    .await?;
    let journal_head = crate::mvcc_product::stream_logical_key(
        crate::core_store::TABLE_STREAM_HEAD_ROW,
        &append_metadata_stream_id(tenant_id, bucket_id),
        None,
    )?;
    let record_head = crate::mvcc_product::stream_logical_key(
        crate::core_store::TABLE_STREAM_HEAD_ROW,
        &append_record_stream_id(stream)?,
        None,
    )?;
    let record = AppendStreamRecord {
        id: next_append_id_in_transaction(
            mvcc,
            transaction_id,
            transaction_principal,
            &journal_head,
            false,
        )?,
        stream_id: stream.id,
        record_sequence: next_append_id_in_transaction(
            mvcc,
            transaction_id,
            transaction_principal,
            &record_head,
            true,
        )?,
        payload_hash: payload_object_ref.hash.clone(),
        payload_object_ref,
        payload_size,
        content_type,
        user_meta,
        authenticated_principal: transaction_principal.to_string(),
        created_at: Utc::now(),
    };
    let receipt = stage_append_body_mvcc(
        mvcc,
        tenant_id,
        bucket_id,
        AppendMutationKind::AppendRecord,
        stream.clone(),
        Some(record.clone()),
        permit.fence_token,
        transaction_id,
        transaction_principal,
    )?;
    Ok(AppendStreamRecordMutation { record, receipt })
}

pub(crate) async fn seal_append_stream_with_permit_in_partition(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    stream: &AppendStream,
    segment_hash: &str,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<SealAppendStreamMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    let mut sealed = get_active_append_stream_mvcc(
        mvcc,
        tenant_id,
        bucket_id,
        &stream.stream_key,
        stream.stream_id,
    )
    .await?
    .ok_or_else(|| anyhow!("append stream not found"))?;
    if sealed.sealed_at.is_some() {
        return Ok(SealAppendStreamMutation {
            sealed: false,
            receipt: None,
        });
    }
    sealed.sealed_at = Some(Utc::now());
    sealed.segment_hash = Some(segment_hash.to_string());
    let (receipt, mutations) = append_body_mvcc_mutations(
        tenant_id,
        bucket_id,
        AppendMutationKind::SealStream,
        sealed,
        None,
        permit.fence_token,
    )?;
    let principal = append_metadata_partition_principal(tenant_id, bucket_id);
    commit_append_mutations(
        mvcc,
        tenant_id,
        bucket_id,
        &principal,
        &format!("append-seal:{}", receipt.mutation_id),
        mutations,
    )
    .await?;
    Ok(SealAppendStreamMutation {
        sealed: true,
        receipt: Some(receipt),
    })
}

pub(crate) async fn seal_append_stream_with_permit_in_partition_transaction(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    stream: &AppendStream,
    segment_hash: &str,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<SealAppendStreamMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    stage_append_assignment_guard(
        mvcc,
        tenant_id,
        bucket_id,
        transaction_id,
        transaction_principal,
    )
    .await?;
    let mut sealed = stream.clone();
    sealed.sealed_at = Some(Utc::now());
    sealed.segment_hash = Some(segment_hash.to_string());
    let receipt = stage_append_body_mvcc(
        mvcc,
        tenant_id,
        bucket_id,
        AppendMutationKind::SealStream,
        sealed,
        None,
        permit.fence_token,
        transaction_id,
        transaction_principal,
    )?;
    Ok(SealAppendStreamMutation {
        sealed: true,
        receipt: Some(receipt),
    })
}

fn stage_append_body_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    event: AppendMutationKind,
    stream: AppendStream,
    record: Option<AppendStreamRecord>,
    fence_token: u64,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<MetadataMutationReceipt> {
    let (receipt, mutations) =
        append_body_mvcc_mutations(tenant_id, bucket_id, event, stream, record, fence_token)?;
    let now = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
    for mutation in &mutations {
        let observed =
            mvcc.read_transaction_value(transaction_id, transaction_principal, &mutation.key)?;
        mvcc.stage_predicate(
            transaction_id,
            transaction_principal,
            mutation.key.clone(),
            value_predicate(observed.as_deref()),
            now,
        )?;
    }
    mvcc.stage_product_mutations(transaction_id, transaction_principal, mutations, now)?;
    Ok(receipt)
}

async fn commit_append_mutations(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    principal: &str,
    logical_idempotency_key: &str,
    mutations: Vec<crate::mvcc_product::ProductMutation>,
) -> Result<()> {
    let now = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
    let handle = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id(),
            principal,
            logical_idempotency_key,
            std::time::Duration::from_secs(30),
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            crate::mvcc_transaction::ReadConsistency::Linearized,
            now,
        )
        .await?;
    for mutation in &mutations {
        let observed =
            mvcc.read_transaction_value(&handle.transaction_id, principal, &mutation.key)?;
        mvcc.stage_predicate(
            &handle.transaction_id,
            principal,
            mutation.key.clone(),
            value_predicate(observed.as_deref()),
            now,
        )?;
    }
    mvcc.stage_product_mutations(&handle.transaction_id, principal, mutations, now)?;
    stage_append_assignment_guard(
        mvcc,
        tenant_id,
        bucket_id,
        &handle.transaction_id,
        principal,
    )
    .await?;
    let outcome = mvcc
        .open_transactions
        .commit(
            mvcc.runtime.as_ref(),
            &handle.transaction_id,
            principal,
            u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default(),
        )
        .await?;
    match outcome.certification {
        crate::mvcc_transaction::CertificationResult::Committed { .. } => Ok(()),
        crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
            Err(anyhow!("append journal transaction aborted: {reason:?}"))
        }
    }
}

async fn stage_append_assignment_guard(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    transaction_id: &str,
    principal: &str,
) -> Result<()> {
    let identity = format!("{tenant_id}:{bucket_id}");
    let assignment = mvcc
        .reconcile_work_assignment("append-journal", &identity)
        .await?
        .ok_or_else(|| anyhow!("this node does not own the append journal assignment"))?;
    mvcc.stage_assignment_guard(
        transaction_id,
        principal,
        &assignment,
        u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default(),
    )
}

fn value_predicate(value: Option<&[u8]>) -> crate::mvcc_transaction::PredicateKind {
    value
        .map(|payload| {
            crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(payload).as_bytes())
        })
        .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent)
}

fn append_body_mvcc_mutations(
    tenant_id: i64,
    bucket_id: i64,
    event: AppendMutationKind,
    stream: AppendStream,
    record: Option<AppendStreamRecord>,
    fence_token: u64,
) -> Result<(
    MetadataMutationReceipt,
    Vec<crate::mvcc_product::ProductMutation>,
)> {
    let mutation_id = uuid::Uuid::new_v4();
    let body = AppendBody {
        event: event.as_str().to_string(),
        stream: Some(stream.clone()),
        record,
        emitted_at: Utc::now().to_rfc3339(),
    };
    let payload = encode_append_body(&body, fence_token, mutation_id)?;
    let payload_hash = hex::encode(hash32(&payload));
    let journal_stream_id = append_metadata_stream_id(tenant_id, bucket_id);
    let exact_stream_id = match event {
        AppendMutationKind::CreateStream | AppendMutationKind::SealStream => {
            append_state_stream_id(&stream)?
        }
        AppendMutationKind::AppendRecord => append_record_stream_id(&stream)?,
    };
    let idempotency_key = mutation_id.to_string();
    let journal_record_value = crate::mvcc_product::encode_stream_record_value(
        &journal_stream_id,
        "append_metadata.record",
        &idempotency_key,
        &payload,
    )?;
    let exact_record_value = crate::mvcc_product::encode_stream_record_value(
        &exact_stream_id,
        "append_metadata.record",
        &idempotency_key,
        &payload,
    )?;
    let mut mutations = vec![
        crate::mvcc_product::ProductMutation::put(
            crate::mvcc_product::stream_logical_key(
                crate::core_store::TABLE_STREAM_HEAD_ROW,
                &journal_stream_id,
                None,
            )?,
            payload.clone(),
        ),
        crate::mvcc_product::ProductMutation::put(
            crate::mvcc_product::stream_logical_key(
                crate::core_store::TABLE_STREAM_RECORD_INDEX_ROW,
                &journal_stream_id,
                Some(stable_append_ordinal(mutation_id)),
            )?,
            journal_record_value,
        ),
        crate::mvcc_product::ProductMutation::put(
            crate::mvcc_product::stream_logical_key(
                crate::core_store::TABLE_STREAM_HEAD_ROW,
                &exact_stream_id,
                None,
            )?,
            payload.clone(),
        ),
    ];
    if matches!(event, AppendMutationKind::AppendRecord) {
        mutations.push(crate::mvcc_product::ProductMutation::put(
            crate::mvcc_product::stream_logical_key(
                crate::core_store::TABLE_STREAM_RECORD_INDEX_ROW,
                &exact_stream_id,
                Some(stable_append_ordinal(mutation_id)),
            )?,
            exact_record_value,
        ));
        mutations.push(crate::mvcc_product::ProductMutation::put(
            crate::mvcc_product::stream_logical_key(
                crate::core_store::TABLE_STREAM_HEAD_ROW,
                &append_record_cursor_stream_id(tenant_id, bucket_id),
                None,
            )?,
            payload,
        ));
    }
    Ok((
        MetadataMutationReceipt {
            mutation_id,
            payload_hash: payload_hash.clone(),
            record_hash: payload_hash,
            watch_cursor: 0,
        },
        mutations,
    ))
}

fn stable_append_ordinal(mutation_id: uuid::Uuid) -> u64 {
    u64::from_be_bytes(mutation_id.as_bytes()[..8].try_into().expect("UUID prefix"))
}

fn next_append_id_from_mvcc_head(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    key: &crate::mvcc_transaction::LogicalKey,
    record_sequence: bool,
) -> Result<i64> {
    let previous = mvcc
        .read_latest_value(key)?
        .map(|payload| decode_append_body(&payload))
        .transpose()?
        .and_then(|body| {
            if record_sequence {
                body.record.map(|record| record.record_sequence)
            } else {
                body.record
                    .map(|record| record.id)
                    .or_else(|| body.stream.map(|stream| stream.id))
            }
        })
        .unwrap_or(0);
    previous
        .checked_add(1)
        .ok_or_else(|| anyhow!("append MVCC head counter overflow"))
}

fn next_append_id_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    key: &crate::mvcc_transaction::LogicalKey,
    record_sequence: bool,
) -> Result<i64> {
    let previous = mvcc
        .read_transaction_value(transaction_id, principal, key)?
        .map(|payload| decode_append_body(&payload))
        .transpose()?
        .and_then(|body| {
            if record_sequence {
                body.record.map(|record| record.record_sequence)
            } else {
                body.record
                    .map(|record| record.id)
                    .or_else(|| body.stream.map(|stream| stream.id))
            }
        })
        .unwrap_or(0);
    previous
        .checked_add(1)
        .ok_or_else(|| anyhow!("append transaction head counter overflow"))
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
