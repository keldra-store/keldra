use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreMutationRootPublication, CoreStore, CoreTransaction, CoreTransactionState,
    CoreTransactionUpdate, ReadStream,
};
use crate::formats::{Hash32, hash32, writer::WriterFamily};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
#[cfg(test)]
use crate::persistence::Bucket;
use crate::persistence::{IndexDefinition, IndexDefinitionEvent};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use prost::Message;
use serde_json::Value as JsonValue;
use serde_json::json;

mod current_definitions;

const INDEX_EVENT_BODY_SCHEMA: &str = "anvil.core.index_definition_event.v1";
const INDEX_DEFINITION_RECORD_KIND: &str = "index_definition";

#[derive(Clone, PartialEq, Message)]
struct IndexDefinitionFieldsProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(int64, tag = "3")]
    bucket_id: i64,
    #[prost(string, tag = "4")]
    name: String,
    #[prost(string, tag = "5")]
    kind: String,
    #[prost(string, tag = "6")]
    selector_json: String,
    #[prost(string, tag = "7")]
    extractor_json: String,
    #[prost(string, tag = "8")]
    authorization_mode: String,
    #[prost(string, tag = "9")]
    build_policy_json: String,
    #[prost(bool, tag = "10")]
    enabled: bool,
    #[prost(int64, tag = "11")]
    version: i64,
    #[prost(string, tag = "12")]
    created_at: String,
    #[prost(string, tag = "13")]
    updated_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct IndexEventBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    cursor: i64,
    #[prost(string, tag = "3")]
    bucket_name: String,
    #[prost(string, tag = "4")]
    event_type: String,
    #[prost(int64, tag = "5")]
    index_version: i64,
    #[prost(string, tag = "6")]
    event_created_at: String,
    #[prost(message, optional, tag = "7")]
    definition: Option<IndexDefinitionFieldsProto>,
    #[prost(uint64, tag = "8")]
    fence_token: u64,
    #[prost(string, tag = "9")]
    mutation_id: String,
}

#[derive(Debug, Clone)]
struct IndexCurrentRef {
    event: IndexDefinitionEvent,
}

#[derive(Debug, Clone, Copy)]
struct IndexCurrentState {
    latest_cursor: i64,
    max_index_id: i64,
}

#[derive(Debug)]
pub(crate) struct CurrentIndexDefinitionPage {
    pub(crate) events: Vec<IndexDefinitionEvent>,
    pub(crate) next_tuple_key: Option<Vec<u8>>,
    #[cfg(test)]
    pub(crate) rows_visited: usize,
}

#[cfg(test)]
async fn append_index_definition_event(
    storage: &Storage,
    event: &IndexDefinitionEvent,
) -> Result<()> {
    append_index_definition_event_inner(storage, event, 0, None, None, None).await
}

pub(crate) async fn append_index_definition_event_with_permit(
    storage: &Storage,
    event: &IndexDefinitionEvent,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    append_index_definition_event_with_permit_in_transaction(
        storage,
        event,
        permit,
        partition_owner_signing_key,
        None,
        None,
    )
    .await
}

pub(crate) async fn append_index_definition_event_with_permit_in_transaction(
    storage: &Storage,
    event: &IndexDefinitionEvent,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<()> {
    require_index_definition_permit(event.tenant_id, event.bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    append_index_definition_event_inner(
        storage,
        event,
        permit.fence_token,
        Some(partition_precondition),
        transaction_id,
        transaction_principal,
    )
    .await
}

async fn append_index_definition_event_inner(
    storage: &Storage,
    event: &IndexDefinitionEvent,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = index_definition_stream_id(event.tenant_id, event.bucket_id);
    let effective_transaction_id = transaction_id.map(ToOwned::to_owned).unwrap_or_else(|| {
        format!(
            "index-definition:{}:{}:{}",
            event.tenant_id, event.bucket_id, event.mutation_id
        )
    });
    if transaction_id.is_none()
        && core_store
            .read_transaction(&effective_transaction_id)
            .await?
            .is_some_and(|transaction| transaction.state == CoreTransactionState::Committed)
    {
        return Ok(());
    }
    let stream_head = core_store.stream_head_sequence(&stream_id).await?;
    let expected_cursor = stream_head
        .checked_add(1)
        .ok_or_else(|| anyhow!("index definition stream cursor overflow"))?;
    if u64::try_from(event.id)? != expected_cursor {
        return Err(anyhow!(
            "index definition event cursor {} does not follow durable stream head {}",
            event.id,
            stream_head
        ));
    }
    let payload = encode_index_event_body(event, fence_token)?;
    let partition_id = hex::encode(index_definition_partition_id(
        event.tenant_id,
        event.bucket_id,
    ));
    let data_root =
        current_definitions::projection_root_anchor_key(event.tenant_id, event.bucket_id);
    let explicit_transaction = match (transaction_id, transaction_principal) {
        (Some(transaction_id), Some(transaction_principal)) => Some(
            core_store
                .read_explicit_transaction_for_principal(transaction_id, transaction_principal)
                .await?,
        ),
        (None, None) => None,
        _ => {
            return Err(anyhow!(
                "index definition transaction id and principal must be provided together"
            ));
        }
    };
    if explicit_transaction
        .as_ref()
        .is_some_and(|transaction| transaction.root_anchor_key != data_root)
    {
        return Err(anyhow!(
            "index definition transaction targets a different CoreMeta root"
        ));
    }
    let scope_partition = explicit_transaction
        .as_ref()
        .map(|transaction| transaction.scope_partition.clone())
        .unwrap_or_else(|| partition_id.clone());
    let root_publications = index_definition_root_publications(data_root, scope_partition.clone());
    let projection = current_definitions::prepare_projection_mutation(
        storage,
        event,
        &payload,
        &scope_partition,
        &effective_transaction_id,
    )
    .await?;
    let mut preconditions: Vec<_> = partition_precondition.into_iter().collect();
    preconditions.push(projection.precondition);
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: scope_partition.clone(),
        stream_id,
        record_kind: INDEX_DEFINITION_RECORD_KIND.to_string(),
        payload,
        idempotency_key: Some(format!(
            "index-definition:{}:{}:{}",
            event.tenant_id, event.bucket_id, event.mutation_id
        )),
    }];
    operations.extend(projection.operations);
    let batch =
        CoreMutationBatch {
            transaction_id: effective_transaction_id,
            scope_partition,
            committed_by_principal: transaction_principal.map(ToOwned::to_owned).unwrap_or_else(
                || index_definition_partition_principal(event.tenant_id, event.bucket_id),
            ),
            root_publications,
            preconditions,
            operations,
        };
    if transaction_id.is_some() {
        core_store.stage_explicit_transaction_batch(batch).await?;
    } else {
        let receipt = core_store.commit_mutation_batch(batch).await?;
        if receipt.state != CoreTransactionState::Committed {
            return Err(anyhow!(
                "index definition mutation {} did not commit: {}",
                receipt.transaction_id,
                receipt
                    .finalisation_error
                    .as_deref()
                    .unwrap_or("unknown finalisation failure")
            ));
        }
    }
    Ok(())
}

pub async fn materialize_committed_index_definition_transaction(
    storage: &Storage,
    transaction: &CoreTransaction,
) -> Result<Vec<IndexDefinitionEvent>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut materialized = Vec::new();
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
        let Some((tenant_id, bucket_id)) = parse_index_definition_stream_id(stream_id) else {
            continue;
        };
        let records = core_store
            .read_stream(ReadStream {
                stream_id: stream_id.clone(),
                after_sequence: visible_sequence.saturating_sub(1),
                limit: 1,
            })
            .await?;
        let Some(record) = records.into_iter().find(|record| {
            record.sequence == *visible_sequence && &record.event_hash == prepared_record_hash
        }) else {
            return Err(anyhow!(
                "index definition transaction {} committed stream record {stream_id}:{visible_sequence} is not readable",
                transaction.transaction_id
            ));
        };
        if record.record_kind != INDEX_DEFINITION_RECORD_KIND {
            continue;
        }
        let event = index_event_body_from_proto(decode_index_event_body(&record.payload)?)?;
        if event.tenant_id != tenant_id || event.bucket_id != bucket_id {
            return Err(anyhow!(
                "index definition transaction {} stream scope does not match payload",
                transaction.transaction_id
            ));
        }
        materialized.push(event);
    }
    Ok(materialized)
}

#[cfg(test)]
async fn write_index_definition_event(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    event_type: &str,
) -> Result<IndexDefinitionEvent> {
    write_index_definition_event_inner(storage, bucket, index, event_type, 0, None).await
}

#[cfg(test)]
pub(crate) async fn write_index_definition_event_with_permit(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    event_type: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<IndexDefinitionEvent> {
    require_index_definition_permit(bucket.tenant_id, bucket.id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    write_index_definition_event_inner(
        storage,
        bucket,
        index,
        event_type,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

#[cfg(test)]
async fn write_index_definition_event_inner(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    event_type: &str,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<IndexDefinitionEvent> {
    let cursor = read_index_current_state(storage, bucket.tenant_id, bucket.id)
        .await?
        .map(|state| state.latest_cursor)
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("index definition cursor overflow"))?;
    let event = IndexDefinitionEvent {
        id: cursor,
        tenant_id: bucket.tenant_id,
        bucket_id: bucket.id,
        bucket_name: bucket.name.clone(),
        index_id: index.id,
        index_name: index.name.clone(),
        event_type: event_type.to_string(),
        index_version: index.version,
        mutation_id: uuid::Uuid::new_v4(),
        definition: index_definition_json(&bucket.name, index),
        created_at: chrono::Utc::now(),
    };
    append_index_definition_event_inner(
        storage,
        &event,
        fence_token,
        partition_precondition,
        None,
        None,
    )
    .await?;
    Ok(event)
}

pub async fn read_index_definition_events(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    after_cursor: i64,
    limit: usize,
) -> Result<Vec<IndexDefinitionEvent>> {
    Ok(
        read_index_definition_event_page(storage, tenant_id, bucket_id, after_cursor, limit)
            .await?
            .events,
    )
}

#[derive(Debug, Clone)]
pub struct IndexDefinitionEventPage {
    pub events: Vec<IndexDefinitionEvent>,
    pub next_cursor: i64,
    pub has_more: bool,
}

pub async fn read_index_definition_event_page(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    after_cursor: i64,
    limit: usize,
) -> Result<IndexDefinitionEventPage> {
    if after_cursor < 0 {
        return Err(anyhow!(
            "index definition watch cursor must be non-negative"
        ));
    }
    let core_store = CoreStore::new(storage.clone()).await?;
    let page = core_store
        .read_stream_page(ReadStream {
            stream_id: index_definition_stream_id(tenant_id, bucket_id),
            after_sequence: u64::try_from(after_cursor)?,
            limit,
        })
        .await?;
    let next_cursor = i64::try_from(page.next_sequence)
        .map_err(|_| anyhow!("index definition watch cursor exceeds i64"))?;
    let mut events = Vec::with_capacity(page.records.len());
    for record in page.records {
        if record.record_kind != INDEX_DEFINITION_RECORD_KIND {
            return Err(anyhow!("index definition stream record kind mismatch"));
        }
        let event = index_event_body_from_proto(decode_index_event_body(&record.payload)?)?;
        if event.tenant_id != tenant_id
            || event.bucket_id != bucket_id
            || event.id != i64::try_from(record.sequence)?
        {
            return Err(anyhow!(
                "index definition stream record scope or cursor mismatch"
            ));
        }
        events.push(event);
    }
    Ok(IndexDefinitionEventPage {
        events,
        next_cursor,
        has_more: page.has_more,
    })
}

pub async fn read_current_index_definition_events(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    include_disabled: bool,
) -> Result<Vec<IndexDefinitionEvent>> {
    let revision =
        current_index_definition_collection_revision(storage, tenant_id, bucket_id).await?;
    let mut events = Vec::new();
    let mut after_tuple_key = None;
    loop {
        let page = page_current_index_definition_events(
            storage,
            tenant_id,
            bucket_id,
            include_disabled,
            revision,
            after_tuple_key.as_deref(),
            1_000,
        )
        .await?;
        events.extend(page.events);
        let Some(next_tuple_key) = page.next_tuple_key else {
            break;
        };
        after_tuple_key = Some(next_tuple_key);
    }
    Ok(events)
}

pub(crate) async fn current_index_definition_collection_revision(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<i64> {
    current_definitions::collection_revision(storage, tenant_id, bucket_id).await
}

pub(crate) async fn page_current_index_definition_events(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    include_disabled: bool,
    expected_revision: i64,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<CurrentIndexDefinitionPage> {
    let page = current_definitions::page(
        storage,
        tenant_id,
        bucket_id,
        include_disabled,
        expected_revision,
        after_tuple_key,
        page_size,
    )
    .await?;
    let mut events = Vec::with_capacity(page.records.len());
    for row in page.records {
        let current = index_current_from_coremeta_row(row)?;
        ensure_index_event_scope_matches(&current.event, tenant_id, bucket_id)?;
        let enabled = current
            .event
            .definition
            .get("enabled")
            .and_then(JsonValue::as_bool)
            .ok_or_else(|| anyhow!("CoreMeta index definition row is missing enabled state"))?;
        if !include_disabled && !enabled {
            return Err(anyhow!(
                "CoreMeta enabled index projection contains a disabled definition"
            ));
        }
        events.push(current.event);
    }
    Ok(CurrentIndexDefinitionPage {
        events,
        next_tuple_key: page.next_tuple_key,
        #[cfg(test)]
        rows_visited: page.rows_visited,
    })
}

pub async fn read_current_index_definitions(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    include_disabled: bool,
) -> Result<Vec<IndexDefinition>> {
    read_current_index_definition_events(storage, tenant_id, bucket_id, include_disabled)
        .await?
        .into_iter()
        .map(|event| index_definition_from_event(&event))
        .collect()
}

pub async fn read_current_index_definition(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    name: &str,
) -> Result<Option<IndexDefinition>> {
    let current = read_index_current_row(storage, tenant_id, bucket_id, name).await?;
    let Some(current) = current else {
        return Ok(None);
    };
    ensure_index_event_name_matches(&current.event, tenant_id, bucket_id, name)?;
    index_definition_from_event(&current.event).map(Some)
}

pub async fn next_index_definition_id(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<i64> {
    read_index_current_state(storage, tenant_id, bucket_id)
        .await?
        .map(|state| state.max_index_id)
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("index definition id overflow"))
}

pub async fn next_index_definition_cursor(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<i64> {
    let projected_cursor = read_index_current_state(storage, tenant_id, bucket_id)
        .await?
        .map(|state| state.latest_cursor)
        .unwrap_or_default();
    let stream_cursor = CoreStore::new(storage.clone())
        .await?
        .stream_head_sequence(&index_definition_stream_id(tenant_id, bucket_id))
        .await?;
    if u64::try_from(projected_cursor)? != stream_cursor {
        return Err(anyhow!(
            "index definition projection cursor {} differs from durable stream head {}",
            projected_cursor,
            stream_cursor
        ));
    }
    projected_cursor
        .checked_add(1)
        .ok_or_else(|| anyhow!("index definition cursor overflow"))
}

pub fn index_storage_id(tenant_id: i64, bucket_id: i64, index_id: i64) -> String {
    format!("tenant-{tenant_id}-bucket-{bucket_id}-index-{index_id}")
}

#[cfg(test)]
async fn read_index_journal_bodies(
    core_store: &CoreStore,
    stream_id: &str,
) -> Result<Vec<IndexEventBodyProto>> {
    let mut bodies = Vec::new();
    let mut after_sequence = 0;
    loop {
        let page = core_store
            .read_stream_page(ReadStream {
                stream_id: stream_id.to_string(),
                after_sequence,
                limit: 1_000,
            })
            .await?;
        for record in page.records {
            if record.record_kind != INDEX_DEFINITION_RECORD_KIND {
                continue;
            }
            bodies.push(decode_index_event_body(&record.payload)?);
        }
        if !page.has_more {
            break;
        }
        if page.next_sequence <= after_sequence {
            return Err(anyhow!("index definition journal cursor did not advance"));
        }
        after_sequence = page.next_sequence;
    }
    Ok(bodies)
}

pub fn index_definition_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/index_definition").as_bytes())
}

pub(crate) fn index_definition_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("index_definition:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn parse_index_definition_stream_id(stream_id: &str) -> Option<(i64, i64)> {
    let rest = stream_id.strip_prefix("index_definition:tenant:")?;
    let (tenant, bucket_part) = rest.split_once(":bucket:")?;
    Some((tenant.parse().ok()?, bucket_part.parse().ok()?))
}

fn index_definition_partition_principal(tenant_id: i64, bucket_id: i64) -> String {
    format!("partition-owner:index_definition:{tenant_id}:{bucket_id}")
}

#[cfg(test)]
pub(crate) async fn read_index_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(read_index_journal_bodies(
        &core_store,
        &index_definition_stream_id(tenant_id, bucket_id),
    )
    .await?
    .into_iter()
    .map(|body| body.fence_token)
    .collect())
}

fn require_index_definition_permit(
    tenant_id: i64,
    bucket_id: i64,
    permit: &PartitionWritePermit,
) -> Result<()> {
    if permit.partition_family != "index_definition"
        || permit.partition_id != hex::encode(index_definition_partition_id(tenant_id, bucket_id))
    {
        return Err(anyhow!(
            "partition write permit does not target this index definition partition"
        ));
    }
    Ok(())
}

#[cfg(test)]
async fn write_index_current_coremeta_rows(
    storage: &Storage,
    event: &IndexDefinitionEvent,
    event_payload: &[u8],
) -> Result<()> {
    let partition_id = hex::encode(index_definition_partition_id(
        event.tenant_id,
        event.bucket_id,
    ));
    let transaction_id = format!(
        "index-definition-projection-test:{}:{}:{}",
        event.tenant_id, event.bucket_id, event.id
    );
    let projection = current_definitions::prepare_projection_mutation(
        storage,
        event,
        event_payload,
        &partition_id,
        &transaction_id,
    )
    .await?;
    let root_publications = index_definition_root_publications(
        current_definitions::projection_root_anchor_key(event.tenant_id, event.bucket_id),
        partition_id.clone(),
    );
    CoreStore::new(storage.clone())
        .await?
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: partition_id,
            committed_by_principal: index_definition_partition_principal(
                event.tenant_id,
                event.bucket_id,
            ),
            root_publications,
            preconditions: vec![projection.precondition],
            operations: projection.operations,
        })
        .await?;
    Ok(())
}

fn index_definition_root_publications(
    data_root: String,
    coordinator_root: String,
) -> Vec<CoreMutationRootPublication> {
    if data_root == coordinator_root {
        return vec![CoreMutationRootPublication {
            root_anchor_key: data_root,
            writer_families: vec![
                WriterFamily::CoreControl.as_str().to_string(),
                WriterFamily::TypedMetadata.as_str().to_string(),
            ],
            transaction_coordinator: true,
        }];
    }

    vec![
        CoreMutationRootPublication::new(coordinator_root, WriterFamily::CoreControl.as_str())
            .coordinator(),
        CoreMutationRootPublication::new(data_root, WriterFamily::TypedMetadata.as_str()),
    ]
}

async fn read_index_current_row(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    index_name: &str,
) -> Result<Option<IndexCurrentRef>> {
    let Some(row) =
        current_definitions::read_current(storage, tenant_id, bucket_id, index_name).await?
    else {
        return Ok(None);
    };
    index_current_from_coremeta_row(row).map(Some)
}

async fn read_index_current_state(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Option<IndexCurrentState>> {
    current_definitions::read_state(storage, tenant_id, bucket_id).await
}

fn encode_index_event_body(event: &IndexDefinitionEvent, fence_token: u64) -> Result<Vec<u8>> {
    let definition = index_definition_from_event(event)?;
    let proto = IndexEventBodyProto {
        schema: INDEX_EVENT_BODY_SCHEMA.to_string(),
        cursor: event.id,
        bucket_name: event.bucket_name.clone(),
        event_type: event.event_type.clone(),
        index_version: event.index_version,
        event_created_at: event.created_at.to_rfc3339(),
        definition: Some(index_definition_to_proto(&definition)?),
        fence_token,
        mutation_id: event.mutation_id.to_string(),
    };
    encode_deterministic_proto(&proto)
}

fn decode_index_event_body(bytes: &[u8]) -> Result<IndexEventBodyProto> {
    let proto = IndexEventBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "index definition event body")?;
    if proto.schema != INDEX_EVENT_BODY_SCHEMA {
        return Err(anyhow!("index definition event body has invalid schema"));
    }
    uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("index definition event body has invalid mutation id"))?;
    Ok(proto)
}

fn index_event_body_from_proto(proto: IndexEventBodyProto) -> Result<IndexDefinitionEvent> {
    let definition = index_definition_from_proto(
        proto
            .definition
            .ok_or_else(|| anyhow!("index definition event body missing definition"))?,
    )?;
    let mutation_id = uuid::Uuid::parse_str(&proto.mutation_id)?;
    index_event_from_parts(
        proto.cursor,
        proto.bucket_name,
        proto.event_type,
        proto.index_version,
        mutation_id,
        proto.event_created_at,
        definition,
    )
}

fn index_current_from_coremeta_row(
    row: current_definitions::CurrentDefinitionRecord,
) -> Result<IndexCurrentRef> {
    let event = index_event_body_from_proto(decode_index_event_body(&row.event_payload)?)?;
    if event.tenant_id != row.tenant_id
        || event.bucket_id != row.bucket_id
        || event.index_name != row.index_name
        || event.id != row.cursor
        || event.index_version != row.index_version
    {
        return Err(anyhow!(
            "CoreMeta index definition current row payload scope mismatch"
        ));
    }
    if event.event_type == "drop" {
        return Err(anyhow!("CoreMeta current table contains a dropped index"));
    }
    Ok(IndexCurrentRef { event })
}

fn index_definition_to_proto(index: &IndexDefinition) -> Result<IndexDefinitionFieldsProto> {
    Ok(IndexDefinitionFieldsProto {
        id: index.id,
        tenant_id: index.tenant_id,
        bucket_id: index.bucket_id,
        name: index.name.clone(),
        kind: index.kind.clone(),
        selector_json: serde_json::to_string(&index.selector)?,
        extractor_json: serde_json::to_string(&index.extractor)?,
        authorization_mode: index.authorization_mode.clone(),
        build_policy_json: serde_json::to_string(&index.build_policy)?,
        enabled: index.enabled,
        version: index.version,
        created_at: index.created_at.to_rfc3339(),
        updated_at: index.updated_at.to_rfc3339(),
    })
}

fn index_definition_from_proto(proto: IndexDefinitionFieldsProto) -> Result<IndexDefinition> {
    Ok(IndexDefinition {
        id: proto.id,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        name: proto.name,
        kind: proto.kind,
        selector: serde_json::from_str(&proto.selector_json)
            .context("parse index selector from current row")?,
        extractor: serde_json::from_str(&proto.extractor_json)
            .context("parse index extractor from current row")?,
        authorization_mode: proto.authorization_mode,
        build_policy: serde_json::from_str(&proto.build_policy_json)
            .context("parse index build policy from current row")?,
        enabled: proto.enabled,
        version: proto.version,
        created_at: chrono::DateTime::parse_from_rfc3339(&proto.created_at)?
            .with_timezone(&chrono::Utc),
        updated_at: chrono::DateTime::parse_from_rfc3339(&proto.updated_at)?
            .with_timezone(&chrono::Utc),
    })
}

fn index_event_from_parts(
    cursor: i64,
    bucket_name: String,
    event_type: String,
    index_version: i64,
    mutation_id: uuid::Uuid,
    event_created_at: String,
    definition: IndexDefinition,
) -> Result<IndexDefinitionEvent> {
    Ok(IndexDefinitionEvent {
        id: cursor,
        tenant_id: definition.tenant_id,
        bucket_id: definition.bucket_id,
        bucket_name: bucket_name.clone(),
        index_id: definition.id,
        index_name: definition.name.clone(),
        event_type,
        index_version,
        mutation_id,
        definition: index_definition_json(&bucket_name, &definition),
        created_at: chrono::DateTime::parse_from_rfc3339(&event_created_at)?
            .with_timezone(&chrono::Utc),
    })
}

fn ensure_index_event_scope_matches(
    event: &IndexDefinitionEvent,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<()> {
    if event.tenant_id != tenant_id || event.bucket_id != bucket_id {
        return Err(anyhow!("CoreMeta index current list row scope mismatch"));
    }
    Ok(())
}

fn ensure_index_event_name_matches(
    event: &IndexDefinitionEvent,
    tenant_id: i64,
    bucket_id: i64,
    index_name: &str,
) -> Result<()> {
    ensure_index_event_scope_matches(event, tenant_id, bucket_id)?;
    if event.index_name != index_name {
        return Err(anyhow!("CoreMeta index current name row scope mismatch"));
    }
    Ok(())
}

fn index_definition_json(bucket_name: &str, index: &IndexDefinition) -> JsonValue {
    json!({
        "index_id": index.id,
        "bucket_name": bucket_name,
        "name": index.name,
        "kind": index.kind,
        "selector_json": index.selector.to_string(),
        "extractor_json": index.extractor.to_string(),
        "authorization_mode": index.authorization_mode,
        "build_policy_json": index.build_policy.to_string(),
        "enabled": index.enabled,
        "version": index.version,
        "created_at": index.created_at.to_rfc3339(),
        "updated_at": index.updated_at.to_rfc3339(),
    })
}

fn event_time_unix_nanos(event_time: chrono::DateTime<chrono::Utc>) -> Result<u64> {
    let nanos = event_time
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("index definition timestamp cannot be represented as nanos"))?;
    u64::try_from(nanos).map_err(|_| anyhow!("index definition timestamp is before unix epoch"))
}

fn index_definition_from_event(event: &IndexDefinitionEvent) -> Result<IndexDefinition> {
    let definition = &event.definition;
    let field = |name: &'static str| -> Result<&JsonValue> {
        definition
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("index definition missing {name}"))
    };
    let string_field = |name: &'static str| -> Result<String> {
        field(name)?
            .as_str()
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("index definition field {name} is not a string"))
    };
    let json_string_field = |name: &'static str| -> Result<JsonValue> {
        let raw = string_field(name)?;
        serde_json::from_str(&raw)
            .with_context(|| format!("parse index definition JSON field {name}"))
    };
    Ok(IndexDefinition {
        id: field("index_id")?
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("index_id is not an integer"))?,
        tenant_id: event.tenant_id,
        bucket_id: event.bucket_id,
        name: string_field("name")?,
        kind: string_field("kind")?,
        selector: json_string_field("selector_json")?,
        extractor: json_string_field("extractor_json")?,
        authorization_mode: string_field("authorization_mode")?,
        build_policy: json_string_field("build_policy_json")?,
        enabled: field("enabled")?
            .as_bool()
            .ok_or_else(|| anyhow::anyhow!("enabled is not a bool"))?,
        version: field("version")?
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("version is not an integer"))?,
        created_at: parse_definition_time(definition.get("created_at"), event.created_at)?,
        updated_at: parse_definition_time(definition.get("updated_at"), event.created_at)?,
    })
}

pub fn index_definition_from_event_for_projection(
    event: &IndexDefinitionEvent,
) -> Result<IndexDefinition> {
    index_definition_from_event(event)
}

fn parse_definition_time(
    value: Option<&JsonValue>,
    default_time: chrono::DateTime<chrono::Utc>,
) -> Result<chrono::DateTime<chrono::Utc>> {
    let Some(value) = value.and_then(JsonValue::as_str) else {
        return Ok(default_time);
    };
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&chrono::Utc))
        .or_else(|_| {
            chrono::DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S %Z")
                .map(|value| value.with_timezone(&chrono::Utc))
        })
        .or(Ok(default_time))
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
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use tempfile::tempdir;

    const PARTITION_OWNER_KEY: &[u8] = b"index definition partition owner signing key";

    fn event(cursor: i64, name: &str, event_type: &str, enabled: bool) -> IndexDefinitionEvent {
        IndexDefinitionEvent {
            id: cursor,
            tenant_id: 42,
            bucket_id: 7,
            bucket_name: "docs".to_string(),
            index_id: 100,
            index_name: name.to_string(),
            event_type: event_type.to_string(),
            index_version: cursor,
            mutation_id: uuid::Uuid::new_v4(),
            definition: json!({
                "index_id": 100,
                "bucket_name": "docs",
                "name": name,
                "kind": "full_text",
                "selector_json": "{}",
                "extractor_json": "{}",
                "authorization_mode": "object_acl",
                "build_policy_json": "{}",
                "enabled": enabled,
                "version": cursor,
                "created_at": "2026-01-01 00:00:00 UTC",
                "updated_at": "2026-01-01 00:00:00 UTC",
            }),
            created_at: Utc::now(),
        }
    }

    fn bucket() -> Bucket {
        Bucket {
            id: 7,
            tenant_id: 42,
            name: "docs".to_string(),
            region: "test-region".to_string(),
            created_at: Utc::now(),
            is_public_read: false,
        }
    }

    fn index(version: i64, enabled: bool) -> IndexDefinition {
        IndexDefinition {
            id: 100,
            tenant_id: 42,
            bucket_id: 7,
            name: "body".to_string(),
            kind: "full_text".to_string(),
            selector: json!({"prefix": "docs/"}),
            extractor: json!({"field": "body"}),
            authorization_mode: "inherit_object".to_string(),
            build_policy: json!({}),
            enabled,
            version,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    async fn ready_index_permit(storage: &Storage, owner_node_id: &str) -> PartitionWritePermit {
        crate::partition_fence::ready_partition_owner_for_test(
            storage,
            "index_definition".to_string(),
            hex::encode(index_definition_partition_id(42, 7)),
            owner_node_id,
            0,
            hex::encode([0; 32]),
            hex::encode([4; 32]),
            PARTITION_OWNER_KEY,
        )
        .await
        .write_permit()
        .unwrap()
    }

    #[tokio::test]
    async fn index_journal_recovers_events_and_current_definitions() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        append_index_definition_event(&storage, &event(1, "body", "create", true))
            .await
            .unwrap();
        append_index_definition_event(&storage, &event(2, "body", "update", true))
            .await
            .unwrap();
        append_index_definition_event(&storage, &event(3, "title", "create", true))
            .await
            .unwrap();
        append_index_definition_event(&storage, &event(4, "body", "disable", false))
            .await
            .unwrap();
        append_index_definition_event(&storage, &event(5, "title", "drop", true))
            .await
            .unwrap();

        let events = read_index_definition_events(&storage, 42, 7, 2, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].id, 3);

        let active = read_current_index_definition_events(&storage, 42, 7, false)
            .await
            .unwrap();
        assert!(active.is_empty());

        let with_disabled = read_current_index_definition_events(&storage, 42, 7, true)
            .await
            .unwrap();
        assert_eq!(with_disabled.len(), 1);
        assert_eq!(with_disabled[0].index_name, "body");
        assert_eq!(with_disabled[0].event_type, "disable");
    }

    #[tokio::test]
    async fn index_journal_allocates_native_event_cursors() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = bucket();
        let first = write_index_definition_event(&storage, &bucket, &index(1, true), "create")
            .await
            .unwrap();
        let second = write_index_definition_event(&storage, &bucket, &index(2, false), "disable")
            .await
            .unwrap();

        assert_eq!(first.id, 1);
        assert_eq!(second.id, 2);
        let events = read_index_definition_events(&storage, 42, 7, 0, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].event_type, "disable");
        assert_eq!(events[1].definition["enabled"].as_bool(), Some(false));
    }

    #[tokio::test]
    async fn index_current_rows_and_id_allocation_do_not_replay_history_stream() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        let create = event(1, "body", "create", true);
        let create_payload = encode_index_event_body(&create, 0).unwrap();
        write_index_current_coremeta_rows(&storage, &create, &create_payload)
            .await
            .unwrap();

        assert!(
            read_index_definition_events(&storage, 42, 7, 0, 10)
                .await
                .unwrap()
                .is_empty(),
            "test setup must not write index history records"
        );
        let current = read_current_index_definition(&storage, 42, 7, "body")
            .await
            .unwrap()
            .expect("current definition should come from CoreMeta current row");
        assert_eq!(current.name, "body");
        assert_eq!(
            next_index_definition_id(&storage, 42, 7).await.unwrap(),
            101
        );

        let row = current_definitions::read_current(&storage, 42, 7, "body")
            .await
            .unwrap()
            .expect("current index row should exist");
        assert_eq!(row.event_payload, create_payload);

        let drop = event(2, "body", "drop", true);
        let drop_payload = encode_index_event_body(&drop, 0).unwrap();
        write_index_current_coremeta_rows(&storage, &drop, &drop_payload)
            .await
            .unwrap();

        assert!(
            read_current_index_definition(&storage, 42, 7, "body")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            current_definitions::read_current(&storage, 42, 7, "body")
                .await
                .unwrap()
                .is_none(),
            "dropped definitions must be physically absent from the current table"
        );
        assert_eq!(
            next_index_definition_id(&storage, 42, 7).await.unwrap(),
            101
        );
    }

    #[tokio::test]
    async fn current_definition_pages_follow_physical_name_order() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for (cursor, name) in [(1, "charlie"), (2, "alpha"), (3, "bravo")] {
            append_index_definition_event(&storage, &event(cursor, name, "create", true))
                .await
                .unwrap();
        }

        let revision = current_index_definition_collection_revision(&storage, 42, 7)
            .await
            .unwrap();
        let first = page_current_index_definition_events(&storage, 42, 7, true, revision, None, 2)
            .await
            .unwrap();
        assert_eq!(
            first
                .events
                .iter()
                .map(|event| event.index_name.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "bravo"]
        );
        assert_eq!(first.rows_visited, 3);

        let second = page_current_index_definition_events(
            &storage,
            42,
            7,
            true,
            revision,
            first.next_tuple_key.as_deref(),
            2,
        )
        .await
        .unwrap();
        assert_eq!(
            second
                .events
                .iter()
                .map(|event| event.index_name.as_str())
                .collect::<Vec<_>>(),
            ["charlie"]
        );
        assert!(second.next_tuple_key.is_none());
        assert_eq!(second.rows_visited, 1);
    }

    #[tokio::test]
    async fn current_definition_page_rejects_stale_revision() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_index_definition_event(&storage, &event(1, "alpha", "create", true))
            .await
            .unwrap();
        let stale_revision = current_index_definition_collection_revision(&storage, 42, 7)
            .await
            .unwrap();
        append_index_definition_event(&storage, &event(2, "bravo", "create", true))
            .await
            .unwrap();

        let error =
            page_current_index_definition_events(&storage, 42, 7, true, stale_revision, None, 10)
                .await
                .unwrap_err();
        assert!(error.to_string().contains("collection revision changed"));
    }

    #[tokio::test]
    async fn enabled_projection_keeps_each_page_read_bounded() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for (cursor, name, enabled) in [
            (1, "alpha", false),
            (2, "bravo", false),
            (3, "charlie", true),
            (4, "delta", true),
        ] {
            append_index_definition_event(&storage, &event(cursor, name, "create", enabled))
                .await
                .unwrap();
        }

        let revision = current_index_definition_collection_revision(&storage, 42, 7)
            .await
            .unwrap();
        let first = page_current_index_definition_events(&storage, 42, 7, false, revision, None, 1)
            .await
            .unwrap();
        assert_eq!(first.events[0].index_name, "charlie");
        assert!(first.next_tuple_key.is_some());
        assert_eq!(first.rows_visited, 2);

        let second = page_current_index_definition_events(
            &storage,
            42,
            7,
            false,
            revision,
            first.next_tuple_key.as_deref(),
            1,
        )
        .await
        .unwrap();
        assert_eq!(second.events[0].index_name, "delta");
        assert!(second.next_tuple_key.is_none());
        assert_eq!(second.rows_visited, 1);

        let all = page_current_index_definition_events(&storage, 42, 7, true, revision, None, 2)
            .await
            .unwrap();
        assert_eq!(
            all.events
                .iter()
                .map(|event| event.index_name.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "bravo"]
        );
        assert!(all.next_tuple_key.is_some());
        assert_eq!(all.rows_visited, 3);
    }

    #[tokio::test]
    async fn index_journal_permit_sets_protobuf_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let permit = ready_index_permit(&storage, "node-a").await;

        append_index_definition_event_with_permit(
            &storage,
            &event(1, "body", "create", true),
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();

        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let bodies = read_index_journal_bodies(&core_store, &index_definition_stream_id(42, 7))
            .await
            .unwrap();
        assert_eq!(bodies.len(), 1);
        assert_eq!(bodies[0].fence_token, permit.fence_token);
    }

    #[tokio::test]
    async fn index_journal_rejects_stale_partition_permit() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let stale = ready_index_permit(&storage, "node-a").await;
        let fresh = ready_index_permit(&storage, "node-b").await;
        assert!(fresh.fence_token > stale.fence_token);

        let rejected = append_index_definition_event_with_permit(
            &storage,
            &event(1, "body", "create", true),
            &stale,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap_err();
        assert!(rejected.to_string().contains("PartitionNotOwned"));

        append_index_definition_event_with_permit(
            &storage,
            &event(1, "body", "create", true),
            &fresh,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn index_journal_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let stale = ready_index_permit(&storage, "node-a").await;
        let stale_precondition =
            partition_write_precondition(&storage, &stale, PARTITION_OWNER_KEY)
                .await
                .unwrap();
        let fresh = ready_index_permit(&storage, "node-b").await;
        assert!(fresh.fence_token > stale.fence_token);

        let rejected = append_index_definition_event_inner(
            &storage,
            &event(1, "body", "create", true),
            stale.fence_token,
            Some(stale_precondition),
            None,
            None,
        )
        .await
        .unwrap_err();
        let message = rejected.to_string();
        assert!(
            message.contains("generation mismatch")
                || message.contains("target mismatch")
                || message.contains("precondition failed"),
            "unexpected stale precondition error: {message}"
        );
        assert!(
            read_current_index_definition(&storage, 42, 7, "body")
                .await
                .unwrap()
                .is_none(),
            "a rejected stream append must not publish its current projection"
        );
        assert_eq!(
            CoreStore::new(storage.clone())
                .await
                .unwrap()
                .stream_head_sequence(&index_definition_stream_id(42, 7))
                .await
                .unwrap(),
            0
        );

        append_index_definition_event_with_permit(
            &storage,
            &event(1, "body", "create", true),
            &fresh,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    pub(crate) async fn index_write_with_permit_allocates_cursor_under_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let permit = ready_index_permit(&storage, "node-a").await;
        let written = write_index_definition_event_with_permit(
            &storage,
            &bucket(),
            &index(1, true),
            "create",
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();

        assert_eq!(written.id, 1);
        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let bodies = read_index_journal_bodies(&core_store, &index_definition_stream_id(42, 7))
            .await
            .unwrap();
        assert_eq!(bodies[0].fence_token, permit.fence_token);
    }
}
