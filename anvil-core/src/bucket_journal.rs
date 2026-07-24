use crate::core_store::{
    CF_MESH, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreMutationRootPublication, CoreStore, CoreTransaction, CoreTransactionState,
    CoreTransactionUpdate, ReadStream, TABLE_BUCKET_CURRENT_BY_ID_ROW,
    TABLE_BUCKET_CURRENT_BY_NAME_ROW, TABLE_BUCKET_EVENT_HEAD_ROW, TABLE_BUCKET_ID_ALLOCATOR_ROW,
    core_meta_committed_row_common, core_meta_payload_digest, core_meta_record_tuple_key,
    core_meta_root_key_hash, core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32, writer::WriterFamily};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::{Bucket, BucketMetadataEvent};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use prost::Message;
use serde_json::{Value as JsonValue, json};
use std::time::{SystemTime, UNIX_EPOCH};

const BUCKET_CURRENT_ROW_SCHEMA: &str = "anvil.core.bucket_current.v1";
const BUCKET_EVENT_HEAD_ROW_SCHEMA: &str = "anvil.core.bucket_event_head.v1";
const BUCKET_ID_ALLOCATOR_ROW_SCHEMA: &str = "anvil.core.bucket_id_allocator.v1";
const BUCKET_ID_ALLOCATION_ATTEMPTS: usize = 32;
const BUCKET_METADATA_BODY_SCHEMA: &str = "anvil.core.bucket_metadata.v1";
const BUCKET_METADATA_RECORD_KIND: &str = "bucket_metadata";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketJournalMutation {
    Create,
    Update,
    Delete,
}

impl BucketJournalMutation {
    fn event_name(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BucketJournalBody {
    event: String,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: String,
    region: String,
    is_public_read: bool,
    mutation_id: String,
    fence_token: u64,
    created_at: String,
    emitted_at: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct BucketJournalBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    event: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(string, tag = "5")]
    bucket_name: String,
    #[prost(string, tag = "6")]
    region: String,
    #[prost(bool, tag = "7")]
    is_public_read: bool,
    #[prost(string, tag = "8")]
    mutation_id: String,
    #[prost(string, tag = "9")]
    created_at: String,
    #[prost(string, optional, tag = "10")]
    emitted_at: Option<String>,
    #[prost(uint64, tag = "11")]
    fence_token: u64,
}

#[derive(Clone, PartialEq, Message)]
struct BucketCurrentRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(bool, tag = "3")]
    deleted: bool,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(int64, tag = "5")]
    tenant_id: i64,
    #[prost(string, tag = "6")]
    bucket_name: String,
    #[prost(string, tag = "7")]
    region: String,
    #[prost(string, tag = "8")]
    created_at: String,
    #[prost(bool, tag = "9")]
    is_public_read: bool,
}

#[derive(Clone, PartialEq, Message)]
struct BucketIdAllocatorRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    max_allocated_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct BucketEventHeadRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(string, tag = "4")]
    bucket_name: String,
    #[prost(uint64, tag = "5")]
    stream_sequence: u64,
    #[prost(bytes, tag = "6")]
    event_payload: Vec<u8>,
}

#[cfg(test)]
async fn append_bucket_mutation(
    storage: &Storage,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
) -> Result<()> {
    append_bucket_mutation_to_stream(
        storage,
        bucket,
        mutation,
        BucketJournalScope::Tenant(bucket.tenant_id),
        0,
        None,
    )
    .await?;
    append_bucket_mutation_to_stream(
        storage,
        bucket,
        mutation,
        BucketJournalScope::Global,
        0,
        None,
    )
    .await
}

pub(crate) async fn append_bucket_mutation_with_permits(
    storage: &Storage,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    tenant_permit: &PartitionWritePermit,
    global_permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let total_start = std::time::Instant::now();
    let tenant_scope = BucketJournalScope::Tenant(bucket.tenant_id);
    let global_scope = BucketJournalScope::Global;
    require_bucket_scope_permit(tenant_scope, tenant_permit)?;
    require_bucket_scope_permit(global_scope, global_permit)?;
    let tenant_precondition = async {
        let step_start = std::time::Instant::now();
        let precondition =
            partition_write_precondition(storage, tenant_permit, partition_owner_signing_key).await;
        crate::emit_test_timing(
            "bucket_journal.append_bucket_mutation tenant_precondition",
            step_start.elapsed(),
        );
        precondition
    };
    let global_precondition = async {
        let step_start = std::time::Instant::now();
        let precondition =
            partition_write_precondition(storage, global_permit, partition_owner_signing_key).await;
        crate::emit_test_timing(
            "bucket_journal.append_bucket_mutation global_precondition",
            step_start.elapsed(),
        );
        precondition
    };
    let (tenant_precondition, global_precondition) =
        tokio::join!(tenant_precondition, global_precondition);
    let tenant_precondition = tenant_precondition?;
    let global_precondition = global_precondition?;

    let tenant_append = async {
        let step_start = std::time::Instant::now();
        let result = append_bucket_mutation_to_stream(
            storage,
            bucket,
            mutation,
            tenant_scope,
            tenant_permit.fence_token,
            Some(tenant_precondition),
        )
        .await;
        crate::emit_test_timing(
            "bucket_journal.append_bucket_mutation tenant_append",
            step_start.elapsed(),
        );
        result
    };
    let global_append = async {
        let step_start = std::time::Instant::now();
        let result = append_bucket_mutation_to_stream(
            storage,
            bucket,
            mutation,
            global_scope,
            global_permit.fence_token,
            Some(global_precondition),
        )
        .await;
        crate::emit_test_timing(
            "bucket_journal.append_bucket_mutation global_append",
            step_start.elapsed(),
        );
        result
    };
    let (tenant_result, global_result) = tokio::join!(tenant_append, global_append);
    tenant_result?;
    global_result?;
    crate::emit_test_timing(
        "bucket_journal.append_bucket_mutation total",
        total_start.elapsed(),
    );
    Ok(())
}

pub(crate) async fn stage_bucket_mutation_in_transaction(
    storage: &Storage,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let transaction = core_store
        .read_explicit_transaction_for_principal(transaction_id, transaction_principal)
        .await?;
    let mutation_id = uuid::Uuid::new_v4().to_string();
    let row_generation = current_unix_nanos();
    let tenant_scope = BucketJournalScope::Tenant(bucket.tenant_id);
    let common_realm_id = tenant_scope.realm_id();
    let common_root_key_hash = tenant_scope.root_key_hash();
    let mut preconditions = Vec::new();
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: transaction.scope_partition.clone(),
        stream_id: tenant_scope.stream_id(),
        record_kind: BUCKET_METADATA_RECORD_KIND.to_string(),
        payload: encode_bucket_journal_body(&BucketJournalBody {
            event: mutation.event_name().to_string(),
            tenant_id: bucket.tenant_id,
            bucket_id: bucket.id,
            bucket_name: bucket.name.clone(),
            region: bucket.region.clone(),
            is_public_read: bucket.is_public_read,
            mutation_id: mutation_id.clone(),
            fence_token: 0,
            created_at: bucket.created_at.to_rfc3339(),
            emitted_at: Some(chrono::Utc::now().to_rfc3339()),
        })?,
        idempotency_key: Some(format!(
            "bucket-metadata:{}:{}",
            tenant_scope.stream_id(),
            mutation_id
        )),
    }];

    for scope in [tenant_scope, BucketJournalScope::Global] {
        preconditions.push(
            bucket_current_coremeta_precondition(
                &core_store,
                scope,
                bucket,
                mutation,
                transaction_id,
                transaction_principal,
            )
            .await?,
        );
        operations.extend(bucket_current_coremeta_operations_with_root(
            scope,
            bucket,
            mutation,
            &transaction.scope_partition,
            transaction_id,
            row_generation,
            common_realm_id.clone(),
            common_root_key_hash.clone(),
        )?);
    }
    let root_publications =
        bucket_root_publications(&transaction.root_anchor_key, tenant_scope.root_anchor_key());

    core_store
        .stage_explicit_transaction_batch(CoreMutationBatch {
            transaction_id: transaction_id.to_string(),
            scope_partition: transaction.scope_partition,
            committed_by_principal: transaction_principal.to_string(),
            root_publications,
            preconditions,
            operations,
        })
        .await?;
    tracing::debug!(
        transaction_id,
        mutation_id,
        bucket_id = bucket.id,
        bucket_name = bucket.name.as_str(),
        "staged bucket metadata mutation in explicit transaction"
    );
    Ok(())
}

pub async fn read_current_bucket_by_id(
    storage: &Storage,
    bucket_id: i64,
) -> Result<Option<Bucket>> {
    let current = read_current_bucket_by_id_row(storage, bucket_id).await?;
    let Some(current) = current else {
        return Ok(None);
    };
    if current.bucket.id != bucket_id {
        return Err(anyhow!("CoreStore bucket current id row scope mismatch"));
    }
    Ok(current.into_active_bucket())
}

pub async fn next_bucket_id(storage: &Storage) -> Result<i64> {
    read_bucket_id_allocator(storage)
        .await?
        .max_allocated_id
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("bucket id overflow"))
}

pub(crate) async fn reserve_next_bucket_id_with_permit(
    storage: &Storage,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<i64> {
    let scope = BucketJournalScope::Global;
    require_bucket_scope_permit(scope, permit)?;

    for attempt in 0..BUCKET_ID_ALLOCATION_ATTEMPTS {
        let partition_precondition =
            partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
        let snapshot = read_bucket_id_allocator(storage).await?;
        let next_id = snapshot
            .max_allocated_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("bucket id overflow"))?;
        let mutation_id = uuid::Uuid::new_v4().to_string();
        let partition_id = hex::encode(scope.partition_id());
        let result = CoreStore::new(storage.clone())
            .await?
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: format!("bucket-id-allocation:{mutation_id}"),
                scope_partition: partition_id.clone(),
                committed_by_principal: scope.partition_principal(),
                root_publications: bucket_root_publications(&partition_id, scope.root_anchor_key()),
                preconditions: vec![
                    partition_precondition,
                    bucket_id_allocator_precondition(&snapshot)?,
                ],
                operations: vec![bucket_id_allocator_put(
                    next_id,
                    &partition_id,
                    &mutation_id,
                    current_unix_nanos(),
                )?],
            })
            .await;
        match result {
            Ok(_) => return Ok(next_id),
            Err(error)
                if attempt + 1 < BUCKET_ID_ALLOCATION_ATTEMPTS
                    && is_bucket_id_allocator_conflict(&error) =>
            {
                crate::perf::record_counter(
                    "bucket_id_allocator_conflicts_total",
                    &[("outcome", "retry")],
                    1,
                );
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("bounded bucket id allocation loop returns on its final attempt")
}

async fn append_bucket_mutation_to_stream(
    storage: &Storage,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    scope: BucketJournalScope,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mutation_id = uuid::Uuid::new_v4();
    let row_generation = current_unix_nanos();
    let body = BucketJournalBody {
        event: mutation.event_name().to_string(),
        tenant_id: bucket.tenant_id,
        bucket_id: bucket.id,
        bucket_name: bucket.name.clone(),
        region: bucket.region.clone(),
        is_public_read: bucket.is_public_read,
        mutation_id: mutation_id.to_string(),
        fence_token,
        created_at: bucket.created_at.to_rfc3339(),
        emitted_at: Some(chrono::Utc::now().to_rfc3339()),
    };
    let payload = encode_bucket_journal_body(&body)?;

    let partition_id = hex::encode(scope.partition_id());
    let stream_id = scope.stream_id();
    let stream_precondition = core_store.stream_head_precondition(&stream_id).await?;
    let expected_stream_sequence = match &stream_precondition {
        CoreMutationPrecondition::StreamHead {
            expected_last_sequence,
            ..
        } => expected_last_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("bucket metadata stream sequence overflow"))?,
        _ => unreachable!("stream head helper returned a non-stream precondition"),
    };
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: partition_id.clone(),
        stream_id: stream_id.clone(),
        record_kind: BUCKET_METADATA_RECORD_KIND.to_string(),
        payload: payload.clone(),
        idempotency_key: Some(format!("bucket-metadata:{}:{}", stream_id, mutation_id)),
    }];
    operations.extend(bucket_current_coremeta_operations(
        scope,
        bucket,
        mutation,
        &partition_id,
        &mutation_id.to_string(),
        row_generation,
    )?);
    if scope == BucketJournalScope::Tenant(bucket.tenant_id) {
        operations.push(bucket_event_head_put(
            bucket,
            &payload,
            expected_stream_sequence,
            &partition_id,
            &mutation_id.to_string(),
            row_generation,
        )?);
    }

    let mut preconditions = partition_precondition.into_iter().collect::<Vec<_>>();
    preconditions.push(stream_precondition);
    if scope == BucketJournalScope::Global && mutation == BucketJournalMutation::Create {
        let allocator = read_bucket_id_allocator(storage).await?;
        if bucket.id > allocator.max_allocated_id {
            preconditions.push(bucket_id_allocator_precondition(&allocator)?);
            operations.push(bucket_id_allocator_put(
                bucket.id,
                &partition_id,
                &mutation_id.to_string(),
                row_generation,
            )?);
        }
    }

    let receipt = core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("bucket-metadata:{stream_id}:{mutation_id}"),
            scope_partition: partition_id.clone(),
            committed_by_principal: scope.partition_principal(),
            root_publications: bucket_root_publications(&partition_id, scope.root_anchor_key()),
            preconditions,
            operations,
        })
        .await?;
    if receipt.state != CoreTransactionState::Committed {
        return Err(anyhow!(
            "bucket metadata mutation {} did not commit: {}",
            receipt.transaction_id,
            receipt
                .finalisation_error
                .as_deref()
                .unwrap_or("unknown finalisation failure")
        ));
    }
    Ok(())
}

pub async fn read_current_bucket(
    storage: &Storage,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<Option<Bucket>> {
    if let Some(current) =
        read_current_bucket_for_tenant_row(storage, tenant_id, bucket_name).await?
    {
        ensure_bucket_tenant_name_matches(&current.bucket, tenant_id, bucket_name)?;
        return Ok(current.into_active_bucket());
    }

    Ok(None)
}

fn bucket_event_head_put(
    bucket: &Bucket,
    event_payload: &[u8],
    stream_sequence: u64,
    partition_id: &str,
    mutation_id: &str,
    row_generation: u64,
) -> Result<CoreMutationOperation> {
    if stream_sequence == 0 || event_payload.is_empty() {
        return Err(anyhow!("bucket event head must reference a durable event"));
    }
    let scope = BucketJournalScope::Tenant(bucket.tenant_id);
    let payload = encode_deterministic_proto(&BucketEventHeadRowProto {
        common: Some(core_meta_committed_row_common(
            scope.realm_id(),
            scope.root_key_hash(),
            row_generation,
            mutation_id,
            current_unix_nanos(),
        )),
        schema: BUCKET_EVENT_HEAD_ROW_SCHEMA.to_string(),
        tenant_id: bucket.tenant_id,
        bucket_name: bucket.name.clone(),
        stream_sequence,
        event_payload: event_payload.to_vec(),
    })?;
    Ok(CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.to_string(),
        cf: CF_MESH.to_string(),
        table_id: TABLE_BUCKET_EVENT_HEAD_ROW,
        tuple_key: bucket_event_head_tuple_key(bucket.tenant_id, &bucket.name)?,
        payload,
    })
}

fn bucket_event_head_tuple_key(tenant_id: i64, bucket_name: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(bucket_name),
    ])
}

pub async fn latest_bucket_metadata_event(
    storage: &Storage,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<Option<BucketMetadataEvent>> {
    let tuple_key = bucket_event_head_tuple_key(tenant_id, bucket_name)?;
    let Some(payload) = CoreStore::new(storage.clone()).await?.read_coremeta_row(
        CF_MESH,
        TABLE_BUCKET_EVENT_HEAD_ROW,
        &tuple_key,
    )?
    else {
        return Ok(None);
    };
    let row = BucketEventHeadRowProto::decode(payload.as_slice())?;
    ensure_deterministic_proto(&row, &payload, "bucket event head row")?;
    let scope = BucketJournalScope::Tenant(tenant_id);
    let common = row
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("bucket event head row is missing CoreMeta common"))?;
    if row.schema != BUCKET_EVENT_HEAD_ROW_SCHEMA
        || row.tenant_id != tenant_id
        || row.bucket_name != bucket_name
        || row.stream_sequence == 0
        || row.event_payload.is_empty()
        || common.realm_id != scope.realm_id()
        || common.root_key_hash != scope.root_key_hash()
    {
        return Err(anyhow!("bucket event head row scope mismatch"));
    }
    let body = decode_bucket_journal_body(&row.event_payload)?;
    if body.tenant_id != tenant_id || body.bucket_name != bucket_name {
        return Err(anyhow!("bucket event head payload scope mismatch"));
    }
    bucket_event_from_body(row.stream_sequence, body).map(Some)
}

pub async fn materialize_committed_bucket_metadata_transaction(
    storage: &Storage,
    transaction: &CoreTransaction,
) -> Result<Vec<BucketMetadataEvent>> {
    if transaction.state != CoreTransactionState::Committed {
        return Ok(Vec::new());
    }

    let core_store = CoreStore::new(storage.clone()).await?;
    let mut events = Vec::new();
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
        if !stream_id.starts_with("bucket_metadata:tenant:") {
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
        let record = records
            .into_iter()
            .find(|record| {
                record.sequence == *visible_sequence
                    && record.event_hash == *prepared_record_hash
                    && record.record_kind == BUCKET_METADATA_RECORD_KIND
            })
            .ok_or_else(|| {
                anyhow!("committed bucket metadata transaction stream record missing")
            })?;
        events.push(bucket_event_from_body(
            record.sequence,
            decode_bucket_journal_body(&record.payload)?,
        )?);
    }
    Ok(events)
}

#[derive(Debug, Clone)]
pub struct BucketMetadataEventPage {
    pub events: Vec<BucketMetadataEvent>,
    pub next_cursor: i64,
    pub has_more: bool,
}

pub async fn list_bucket_metadata_event_page(
    storage: &Storage,
    tenant_id: i64,
    bucket_name: &str,
    after_cursor: i64,
    limit: usize,
) -> Result<BucketMetadataEventPage> {
    if after_cursor < 0 {
        return Err(anyhow!("bucket metadata watch cursor must be non-negative"));
    }
    let core_store = CoreStore::new(storage.clone()).await?;
    let page = core_store
        .read_stream_page(ReadStream {
            stream_id: BucketJournalScope::Tenant(tenant_id).stream_id(),
            after_sequence: u64::try_from(after_cursor)?,
            limit,
        })
        .await?;
    let next_cursor = i64::try_from(page.next_sequence)
        .map_err(|_| anyhow!("bucket metadata watch cursor exceeds i64"))?;
    let mut events = Vec::with_capacity(page.records.len());
    for record in page.records {
        if record.record_kind != BUCKET_METADATA_RECORD_KIND {
            continue;
        }
        let body = decode_bucket_journal_body(&record.payload)
            .with_context(|| format!("decode bucket metadata stream record {}", record.cursor))?;
        if !bucket_name.is_empty() && body.bucket_name != bucket_name {
            continue;
        }
        events.push(bucket_event_from_body(record.sequence, body)?);
    }
    Ok(BucketMetadataEventPage {
        events,
        next_cursor,
        has_more: page.has_more,
    })
}

pub(crate) fn tenant_bucket_metadata_stream_id(tenant_id: i64) -> String {
    BucketJournalScope::Tenant(tenant_id).stream_id()
}

#[derive(Debug, Clone)]
struct BucketCurrentRow {
    deleted: bool,
    bucket: Bucket,
}

#[derive(Debug, Clone)]
struct BucketIdAllocatorSnapshot {
    max_allocated_id: i64,
    expected_payload_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CurrentBucketPage {
    pub buckets: Vec<Bucket>,
    pub next_tuple_key: Option<Vec<u8>>,
}

impl BucketCurrentRow {
    fn into_active_bucket(self) -> Option<Bucket> {
        if self.deleted {
            None
        } else {
            Some(self.bucket)
        }
    }
}

async fn read_bucket_id_allocator(storage: &Storage) -> Result<BucketIdAllocatorSnapshot> {
    let tuple_key = bucket_id_allocator_tuple_key()?;
    let payload = CoreStore::new(storage.clone()).await?.read_coremeta_row(
        CF_MESH,
        TABLE_BUCKET_ID_ALLOCATOR_ROW,
        &tuple_key,
    )?;
    let Some(payload) = payload else {
        return Ok(BucketIdAllocatorSnapshot {
            max_allocated_id: 0,
            expected_payload_hash: None,
        });
    };
    let row = BucketIdAllocatorRowProto::decode(payload.as_slice())?;
    ensure_deterministic_proto(&row, &payload, "bucket id allocator row")?;
    if row.schema != BUCKET_ID_ALLOCATOR_ROW_SCHEMA {
        return Err(anyhow!("bucket id allocator row schema mismatch"));
    }
    let common = row
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("bucket id allocator row is missing CoreMeta common"))?;
    let scope = BucketJournalScope::Global;
    if common.realm_id != scope.realm_id() || common.root_key_hash != scope.root_key_hash() {
        return Err(anyhow!("bucket id allocator row scope mismatch"));
    }
    if row.max_allocated_id < 0 {
        return Err(anyhow!("bucket id allocator row is negative"));
    }
    Ok(BucketIdAllocatorSnapshot {
        max_allocated_id: row.max_allocated_id,
        expected_payload_hash: Some(core_meta_payload_digest(
            TABLE_BUCKET_ID_ALLOCATOR_ROW,
            &payload,
        )),
    })
}

fn bucket_id_allocator_precondition(
    snapshot: &BucketIdAllocatorSnapshot,
) -> Result<CoreMutationPrecondition> {
    Ok(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_MESH.to_string(),
        table_id: TABLE_BUCKET_ID_ALLOCATOR_ROW,
        tuple_key: bucket_id_allocator_tuple_key()?,
        expected_payload_hash: snapshot.expected_payload_hash.clone(),
        require_absent: snapshot.expected_payload_hash.is_none(),
        require_present: snapshot.expected_payload_hash.is_some(),
    })
}

fn bucket_id_allocator_put(
    max_allocated_id: i64,
    partition_id: &str,
    transaction_id: &str,
    row_generation: u64,
) -> Result<CoreMutationOperation> {
    if max_allocated_id <= 0 {
        return Err(anyhow!("bucket id allocator must be positive"));
    }
    let scope = BucketJournalScope::Global;
    let payload = encode_deterministic_proto(&BucketIdAllocatorRowProto {
        common: Some(core_meta_committed_row_common(
            scope.realm_id(),
            scope.root_key_hash(),
            row_generation,
            transaction_id.to_string(),
            current_unix_nanos(),
        )),
        schema: BUCKET_ID_ALLOCATOR_ROW_SCHEMA.to_string(),
        max_allocated_id,
    })?;
    Ok(CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.to_string(),
        cf: CF_MESH.to_string(),
        table_id: TABLE_BUCKET_ID_ALLOCATOR_ROW,
        tuple_key: bucket_id_allocator_tuple_key()?,
        payload,
    })
}

fn bucket_id_allocator_tuple_key() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("bucket-id-allocator")])
}

fn is_bucket_id_allocator_conflict(error: &anyhow::Error) -> bool {
    if crate::core_store::is_retryable_mutation_conflict(error) {
        return true;
    }
    let message = format!("{error:#}");
    message.contains(&format!("{TABLE_BUCKET_ID_ALLOCATOR_ROW:#06x}"))
        && (message.contains("target mismatch") || message.contains("must be absent"))
}

fn bucket_current_coremeta_operations(
    scope: BucketJournalScope,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    operation_partition_id: &str,
    mutation_id: &str,
    row_generation: u64,
) -> Result<Vec<CoreMutationOperation>> {
    bucket_current_coremeta_operations_with_root(
        scope,
        bucket,
        mutation,
        operation_partition_id,
        mutation_id,
        row_generation,
        scope.realm_id(),
        scope.root_key_hash(),
    )
}

fn bucket_current_coremeta_operations_with_root(
    scope: BucketJournalScope,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    operation_partition_id: &str,
    mutation_id: &str,
    row_generation: u64,
    common_realm_id: String,
    common_root_key_hash: String,
) -> Result<Vec<CoreMutationOperation>> {
    let operations = match scope {
        BucketJournalScope::Tenant(tenant_id) if mutation == BucketJournalMutation::Delete => {
            vec![CoreMutationOperation::CoreMetaDelete {
                partition_id: operation_partition_id.to_string(),
                cf: CF_MESH.to_string(),
                table_id: TABLE_BUCKET_CURRENT_BY_NAME_ROW,
                tuple_key: tenant_bucket_name_current_tuple_key(tenant_id, &bucket.name)?,
            }]
        }
        BucketJournalScope::Tenant(tenant_id) => vec![CoreMutationOperation::CoreMetaPut {
            partition_id: operation_partition_id.to_string(),
            cf: CF_MESH.to_string(),
            table_id: TABLE_BUCKET_CURRENT_BY_NAME_ROW,
            tuple_key: tenant_bucket_name_current_tuple_key(tenant_id, &bucket.name)?,
            payload: encode_bucket_current_row_with_root(
                bucket,
                false,
                mutation_id,
                row_generation,
                common_realm_id,
                common_root_key_hash,
            )?,
        }],
        BucketJournalScope::Global => vec![CoreMutationOperation::CoreMetaPut {
            partition_id: operation_partition_id.to_string(),
            cf: CF_MESH.to_string(),
            table_id: TABLE_BUCKET_CURRENT_BY_ID_ROW,
            tuple_key: global_bucket_id_current_tuple_key(bucket.id)?,
            payload: encode_bucket_current_row_with_root(
                bucket,
                mutation == BucketJournalMutation::Delete,
                mutation_id,
                row_generation,
                common_realm_id,
                common_root_key_hash,
            )?,
        }],
    };
    Ok(operations)
}

async fn bucket_current_coremeta_precondition(
    core_store: &CoreStore,
    scope: BucketJournalScope,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<CoreMutationPrecondition> {
    let (table_id, tuple_key) = scope.bucket_current_tuple_key(bucket)?;
    let current = core_store
        .read_coremeta_row_visible_to_transaction(
            CF_MESH,
            table_id,
            &tuple_key,
            transaction_id,
            transaction_principal,
        )
        .await?;
    Ok(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_MESH.to_string(),
        table_id,
        tuple_key,
        expected_payload_hash: current
            .as_ref()
            .map(|payload| core_meta_payload_digest(table_id, payload)),
        require_absent: mutation == BucketJournalMutation::Create,
        require_present: mutation != BucketJournalMutation::Create,
    })
}

async fn read_current_bucket_for_tenant_row(
    storage: &Storage,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<Option<BucketCurrentRow>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let tuple_key = tenant_bucket_name_current_tuple_key(tenant_id, bucket_name)?;
    let Some(payload) =
        core_store.read_coremeta_row(CF_MESH, TABLE_BUCKET_CURRENT_BY_NAME_ROW, &tuple_key)?
    else {
        return Ok(None);
    };
    decode_bucket_current_row(&payload)
        .with_context(|| format!("decode bucket current CoreMeta row {tenant_id}/{bucket_name}"))
        .map(Some)
}

async fn read_current_bucket_by_id_row(
    storage: &Storage,
    bucket_id: i64,
) -> Result<Option<BucketCurrentRow>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let tuple_key = global_bucket_id_current_tuple_key(bucket_id)?;
    let Some(payload) =
        core_store.read_coremeta_row(CF_MESH, TABLE_BUCKET_CURRENT_BY_ID_ROW, &tuple_key)?
    else {
        return Ok(None);
    };
    decode_bucket_current_row(&payload)
        .with_context(|| format!("decode bucket current CoreMeta row id/{bucket_id}"))
        .map(Some)
}

pub async fn current_bucket_collection_revision(
    storage: &Storage,
    tenant_id: i64,
) -> Result<String> {
    let scope = BucketJournalScope::Tenant(tenant_id);
    let core_store = CoreStore::new(storage.clone()).await?;
    let (sequence, event_hash) = core_store.raw_stream_head(&scope.stream_id()).await?;
    Ok(format!("{sequence}:{event_hash}"))
}

pub async fn page_current_buckets(
    storage: &Storage,
    tenant_id: i64,
    expected_revision: &str,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<CurrentBucketPage> {
    if !(1..=1000).contains(&page_size) {
        return Err(anyhow!("bucket page size must be between 1 and 1000"));
    }
    if current_bucket_collection_revision(storage, tenant_id).await? != expected_revision {
        return Err(anyhow!("bucket collection revision changed"));
    }

    let core_store = CoreStore::new(storage.clone()).await?;
    let prefix = tenant_bucket_name_current_tuple_prefix(tenant_id)?;
    let mut rows = core_store.scan_coremeta_prefix_page(
        CF_MESH,
        TABLE_BUCKET_CURRENT_BY_NAME_ROW,
        &prefix,
        after_tuple_key,
        page_size + 1,
    )?;
    let has_more = rows.len() > page_size;
    if has_more {
        rows.truncate(page_size);
    }
    let next_tuple_key = if has_more {
        let last = rows
            .last()
            .ok_or_else(|| anyhow!("bucket page continuation has no last row"))?;
        Some(core_meta_record_tuple_key(&last.key)?.to_vec())
    } else {
        None
    };
    let mut buckets = Vec::with_capacity(rows.len());
    for row in rows {
        let current = decode_bucket_current_row(&row.payload)
            .with_context(|| "decode bucket list CoreMeta row")?;
        ensure_bucket_scope_matches(BucketJournalScope::Tenant(tenant_id), &current.bucket)?;
        if current.deleted {
            return Err(anyhow!(
                "tenant bucket current table contains a deleted row"
            ));
        }
        buckets.push(current.bucket);
    }
    if current_bucket_collection_revision(storage, tenant_id).await? != expected_revision {
        return Err(anyhow!("bucket collection changed during page read"));
    }
    Ok(CurrentBucketPage {
        buckets,
        next_tuple_key,
    })
}

fn encode_bucket_current_row_with_root(
    bucket: &Bucket,
    deleted: bool,
    mutation_id: &str,
    row_generation: u64,
    common_realm_id: String,
    common_root_key_hash: String,
) -> Result<Vec<u8>> {
    let row = BucketCurrentRowProto {
        common: Some(core_meta_committed_row_common(
            common_realm_id,
            common_root_key_hash,
            row_generation,
            mutation_id.to_string(),
            row_generation,
        )),
        schema: BUCKET_CURRENT_ROW_SCHEMA.to_string(),
        deleted,
        bucket_id: bucket.id,
        tenant_id: bucket.tenant_id,
        bucket_name: bucket.name.clone(),
        region: bucket.region.clone(),
        created_at: bucket.created_at.to_rfc3339(),
        is_public_read: bucket.is_public_read,
    };
    encode_deterministic_proto(&row)
}

fn decode_bucket_current_row(bytes: &[u8]) -> Result<BucketCurrentRow> {
    let row = BucketCurrentRowProto::decode(bytes)?;
    ensure_deterministic_proto(&row, bytes, "bucket current row")?;
    if row.schema != BUCKET_CURRENT_ROW_SCHEMA {
        return Err(anyhow!("CoreStore bucket current row has invalid schema"));
    }
    let common = row
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore bucket current row missing CoreMeta common"))?;
    if common.root_key_hash.is_empty() {
        return Err(anyhow!("CoreStore bucket current row missing root hash"));
    }
    let bucket = Bucket {
        id: row.bucket_id,
        tenant_id: row.tenant_id,
        name: row.bucket_name,
        region: row.region,
        created_at: chrono::DateTime::parse_from_rfc3339(&row.created_at)?
            .with_timezone(&chrono::Utc),
        is_public_read: row.is_public_read,
    };
    Ok(BucketCurrentRow {
        deleted: row.deleted,
        bucket,
    })
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct BucketJournalEntry {
    sequence: u64,
    body: BucketJournalBody,
}

#[cfg(test)]
async fn read_bucket_journal_entries(
    storage: &Storage,
    scope: BucketJournalScope,
) -> Result<Vec<BucketJournalEntry>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut entries = Vec::new();
    let mut after_sequence = 0;
    loop {
        let page = core_store
            .read_stream_page(ReadStream {
                stream_id: scope.stream_id(),
                after_sequence,
                limit: 256,
            })
            .await?;
        for record in page.records {
            if record.record_kind != BUCKET_METADATA_RECORD_KIND {
                continue;
            }
            entries.push(BucketJournalEntry {
                sequence: record.sequence,
                body: decode_bucket_journal_body(&record.payload).with_context(|| {
                    format!("decode bucket metadata stream record {}", record.cursor)
                })?,
            });
        }
        if !page.has_more || page.next_sequence == after_sequence {
            break;
        }
        after_sequence = page.next_sequence;
    }
    Ok(entries)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketJournalScope {
    Tenant(i64),
    Global,
}

impl BucketJournalScope {
    fn stream_id(self) -> String {
        match self {
            Self::Tenant(tenant_id) => format!("bucket_metadata:tenant:{tenant_id}"),
            Self::Global => "bucket_metadata:global".to_string(),
        }
    }

    fn partition_id(self) -> Hash32 {
        match self {
            Self::Tenant(tenant_id) => tenant_bucket_partition_id(tenant_id),
            Self::Global => global_bucket_partition_id(),
        }
    }

    fn partition_principal(self) -> String {
        match self {
            Self::Tenant(tenant_id) => {
                format!("partition-owner:bucket_metadata:tenant:{tenant_id}")
            }
            Self::Global => "partition-owner:bucket_metadata:global".to_string(),
        }
    }

    fn bucket_current_tuple_prefix(self) -> Result<(u16, Vec<u8>)> {
        match self {
            Self::Tenant(tenant_id) => Ok((
                TABLE_BUCKET_CURRENT_BY_NAME_ROW,
                tenant_bucket_name_current_tuple_prefix(tenant_id)?,
            )),
            Self::Global => Ok((
                TABLE_BUCKET_CURRENT_BY_ID_ROW,
                global_bucket_id_current_tuple_prefix()?,
            )),
        }
    }

    fn bucket_current_tuple_key(self, bucket: &Bucket) -> Result<(u16, Vec<u8>)> {
        match self {
            Self::Tenant(tenant_id) => Ok((
                TABLE_BUCKET_CURRENT_BY_NAME_ROW,
                tenant_bucket_name_current_tuple_key(tenant_id, &bucket.name)?,
            )),
            Self::Global => Ok((
                TABLE_BUCKET_CURRENT_BY_ID_ROW,
                global_bucket_id_current_tuple_key(bucket.id)?,
            )),
        }
    }

    fn realm_id(self) -> String {
        match self {
            Self::Tenant(tenant_id) => format!("tenant/{tenant_id}"),
            Self::Global => "system".to_string(),
        }
    }

    fn root_anchor_key(self) -> String {
        match self {
            Self::Tenant(tenant_id) => format!("bucket-current/tenant/{tenant_id}"),
            Self::Global => "bucket-current/global".to_string(),
        }
    }

    fn root_key_hash(self) -> String {
        core_meta_root_key_hash(&self.root_anchor_key())
    }
}

pub(crate) fn tenant_bucket_root_key_hash(tenant_id: i64) -> String {
    BucketJournalScope::Tenant(tenant_id).root_key_hash()
}

fn bucket_root_publications(
    coordinator_root: &str,
    data_root: String,
) -> Vec<CoreMutationRootPublication> {
    if coordinator_root == data_root {
        return vec![CoreMutationRootPublication {
            root_anchor_key: data_root,
            writer_families: vec![
                WriterFamily::CoreControl.as_str().to_string(),
                WriterFamily::MeshControl.as_str().to_string(),
            ],
            transaction_coordinator: true,
        }];
    }
    vec![
        CoreMutationRootPublication::new(coordinator_root, WriterFamily::CoreControl.as_str())
            .coordinator(),
        CoreMutationRootPublication::new(data_root, WriterFamily::MeshControl.as_str()),
    ]
}

fn ensure_bucket_tenant_name_matches(
    bucket: &Bucket,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<()> {
    if bucket.tenant_id != tenant_id || bucket.name != bucket_name {
        return Err(anyhow!(
            "CoreStore bucket current tenant/name row scope mismatch"
        ));
    }
    Ok(())
}

fn ensure_bucket_scope_matches(scope: BucketJournalScope, bucket: &Bucket) -> Result<()> {
    if let BucketJournalScope::Tenant(tenant_id) = scope {
        if bucket.tenant_id != tenant_id {
            return Err(anyhow!("CoreStore bucket current list row scope mismatch"));
        }
    }
    Ok(())
}

pub fn tenant_bucket_partition_id(tenant_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket_metadata").as_bytes())
}

pub fn global_bucket_partition_id() -> Hash32 {
    hash32(b"bucket_metadata/global")
}

#[cfg(test)]
pub(crate) async fn read_bucket_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
) -> Result<(Vec<u64>, Vec<u64>)> {
    let tenant = read_bucket_journal_entries(storage, BucketJournalScope::Tenant(tenant_id))
        .await?
        .into_iter()
        .map(|entry| entry.body.fence_token)
        .collect();
    let global = read_bucket_journal_entries(storage, BucketJournalScope::Global)
        .await?
        .into_iter()
        .map(|entry| entry.body.fence_token)
        .collect();
    Ok((tenant, global))
}

fn require_bucket_scope_permit(
    scope: BucketJournalScope,
    permit: &PartitionWritePermit,
) -> Result<()> {
    if permit.partition_family != "bucket_metadata"
        || permit.partition_id != hex::encode(scope.partition_id())
    {
        return Err(anyhow!(
            "partition write permit does not target this bucket metadata partition"
        ));
    }
    Ok(())
}

fn tenant_bucket_name_current_tuple_key(tenant_id: i64, bucket_name: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(bucket_name),
    ])
}

fn tenant_bucket_name_current_tuple_prefix(tenant_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::I64(tenant_id)])
}

fn global_bucket_id_current_tuple_key(bucket_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::I64(bucket_id)])
}

fn global_bucket_id_current_tuple_prefix() -> Result<Vec<u8>> {
    Ok(Vec::new())
}

fn current_unix_nanos() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_secs()
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::from(now.subsec_nanos()))
}

fn bucket_event_from_body(sequence: u64, body: BucketJournalBody) -> Result<BucketMetadataEvent> {
    let id = i64::try_from(sequence).context("bucket metadata cursor exceeds i64")?;
    let bucket_created_at =
        chrono::DateTime::parse_from_rfc3339(&body.created_at)?.with_timezone(&chrono::Utc);
    let event_created_at = body
        .emitted_at
        .as_deref()
        .map(chrono::DateTime::parse_from_rfc3339)
        .transpose()?
        .map(|value| value.with_timezone(&chrono::Utc))
        .unwrap_or(bucket_created_at);
    let deleted = body.event == "delete";
    Ok(BucketMetadataEvent {
        id,
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        bucket_name: body.bucket_name.clone(),
        event_type: bucket_event_type(&body.event).to_string(),
        mutation_id: uuid::Uuid::parse_str(&body.mutation_id)?,
        bucket_metadata: bucket_metadata_json(&body, deleted),
        created_at: event_created_at,
    })
}

fn bucket_event_type(event: &str) -> &str {
    match event {
        "update" => "policy_update",
        other => other,
    }
}

fn bucket_metadata_json(body: &BucketJournalBody, deleted: bool) -> JsonValue {
    json!({
        "bucket_id": body.bucket_id,
        "name": body.bucket_name,
        "creation_date": body.created_at,
        "region": body.region,
        "is_public_read": body.is_public_read,
        "deleted": deleted,
    })
}

fn encode_bucket_journal_body(body: &BucketJournalBody) -> Result<Vec<u8>> {
    let proto = BucketJournalBodyProto {
        schema: BUCKET_METADATA_BODY_SCHEMA.to_string(),
        event: body.event.clone(),
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        bucket_name: body.bucket_name.clone(),
        region: body.region.clone(),
        is_public_read: body.is_public_read,
        mutation_id: body.mutation_id.clone(),
        fence_token: body.fence_token,
        created_at: body.created_at.clone(),
        emitted_at: body.emitted_at.clone(),
    };
    encode_deterministic_proto(&proto)
}

fn decode_bucket_journal_body(bytes: &[u8]) -> Result<BucketJournalBody> {
    let proto = BucketJournalBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "bucket metadata body")?;
    if proto.schema != BUCKET_METADATA_BODY_SCHEMA {
        return Err(anyhow!("bucket metadata body has invalid schema"));
    }
    uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("bucket metadata body has invalid mutation id"))?;
    Ok(BucketJournalBody {
        event: proto.event,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        bucket_name: proto.bucket_name,
        region: proto.region,
        is_public_read: proto.is_public_read,
        mutation_id: proto.mutation_id,
        fence_token: proto.fence_token,
        created_at: proto.created_at,
        emitted_at: proto.emitted_at,
    })
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

#[cfg(test)]
mod tests;
