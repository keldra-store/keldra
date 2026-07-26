use crate::core_store::{CoreMutationOperation, CoreMutationPrecondition};
use crate::formats::{Hash32, hash32};
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

#[cfg(any())]
async fn append_index_definition_event(
    storage: &Storage,
    event: &IndexDefinitionEvent,
) -> Result<()> {
    append_index_definition_event_inner(storage, None, event, 0, None, None, None).await
}

pub(crate) async fn append_index_definition_event_with_permit_mvcc(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    event: &IndexDefinitionEvent,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    append_index_definition_event_with_permit_in_transaction(
        storage,
        mvcc,
        event,
        permit,
        partition_owner_signing_key,
        None,
        None,
    )
    .await
}

#[cfg(any())]
pub(crate) async fn append_index_definition_event_with_permit(
    storage: &Storage,
    event: &IndexDefinitionEvent,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    append_index_definition_event_with_permit_in_transaction(
        storage,
        None,
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
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
        mvcc,
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    event: &IndexDefinitionEvent,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<()> {
    let stream_id = index_definition_stream_id(event.tenant_id, event.bucket_id);
    let effective_transaction_id = transaction_id.map(ToOwned::to_owned).unwrap_or_else(|| {
        format!(
            "index-definition:{}:{}:{}",
            event.tenant_id, event.bucket_id, event.mutation_id
        )
    });
    let payload = encode_index_event_body(event, fence_token)?;
    let partition_id = hex::encode(index_definition_partition_id(
        event.tenant_id,
        event.bucket_id,
    ));
    match (transaction_id, transaction_principal) {
        (Some(_), Some(_)) | (None, None) => {}
        _ => {
            return Err(anyhow!(
                "index definition transaction id and principal must be provided together"
            ));
        }
    }
    let scope_partition = partition_id;
    let projection = current_definitions::prepare_projection_mutation(
        mvcc,
        event,
        &payload,
        &scope_partition,
        &effective_transaction_id,
        transaction_principal,
    )
    .await?;
    let _ = partition_precondition;
    let _ = projection.precondition;
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
    let mutations = crate::mvcc_product::product_mutations_from_operations(operations)?;
    let now_unix_ms = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default();
    if let Some(transaction_id) = transaction_id {
        let principal =
            transaction_principal.ok_or_else(|| anyhow!("transaction principal is required"))?;
        let snapshot = mvcc
            .open_transactions
            .handle(transaction_id)?
            .snapshot_version;
        let predicates = mutations
            .iter()
            .map(|mutation| {
                let visible = mvcc.runtime.read_at(&mutation.key, snapshot)?;
                let kind = visible.map_or(crate::mvcc_transaction::PredicateKind::Absent, |row| {
                    crate::mvcc_transaction::PredicateKind::ValueHash(
                        *blake3::hash(&row.value).as_bytes(),
                    )
                });
                Ok((mutation.key.clone(), kind))
            })
            .collect::<Result<Vec<_>>>()?;
        mvcc.stage_product_mutations(transaction_id, principal, mutations, now_unix_ms)?;
        for (key, kind) in predicates {
            mvcc.stage_predicate(transaction_id, principal, key, kind, now_unix_ms)?;
        }
    } else {
        let snapshot = mvcc.runtime.applied_version()?;
        let predicates = mutations
            .iter()
            .map(|mutation| {
                let visible = mvcc.runtime.read_at(&mutation.key, snapshot)?;
                let kind = visible.map_or(crate::mvcc_transaction::PredicateKind::Absent, |row| {
                    crate::mvcc_transaction::PredicateKind::ValueHash(
                        *blake3::hash(&row.value).as_bytes(),
                    )
                });
                Ok((mutation.key.clone(), kind))
            })
            .collect::<Result<Vec<_>>>()?;
        mvcc.autocommit_product_mutations_with_predicates(
            &index_definition_partition_principal(event.tenant_id, event.bucket_id),
            &effective_transaction_id,
            mutations,
            predicates,
            crate::mvcc_transaction::DurabilityLevel::Local,
            now_unix_ms,
        )
        .await?;
    }
    Ok(())
}

#[cfg(any())]
async fn write_index_definition_event(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    event_type: &str,
) -> Result<IndexDefinitionEvent> {
    write_index_definition_event_inner(storage, bucket, index, event_type, 0, None).await
}

#[cfg(any())]
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

#[cfg(any())]
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
        None,
        &event,
        fence_token,
        partition_precondition,
        None,
        None,
    )
    .await?;
    Ok(event)
}

#[cfg(any())]
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

pub fn read_index_definition_event_page_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
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
    let stream_id = index_definition_stream_id(tenant_id, bucket_id);
    let prefix = crate::mvcc_product::stream_logical_key(
        crate::core_store::TABLE_STREAM_RECORD_INDEX_ROW,
        &stream_id,
        None,
    )?;
    let snapshot = mvcc.runtime.applied_version()?;
    let mut events = mvcc
        .runtime
        .scan_table_prefix_at(
            crate::core_store::TABLE_STREAM_RECORD_INDEX_ROW,
            &prefix.application_key,
            snapshot,
        )?
        .into_iter()
        .map(|(_, row)| {
            let (record_kind, payload) =
                crate::mvcc_product::decode_stream_record_value(&row.value)?;
            if record_kind != INDEX_DEFINITION_RECORD_KIND {
                return Err(anyhow!("index definition stream record kind mismatch"));
            }
            let event = index_event_body_from_proto(decode_index_event_body(&payload)?)?;
            ensure_index_event_scope_matches(&event, tenant_id, bucket_id)?;
            Ok(event)
        })
        .collect::<Result<Vec<_>>>()?;
    events.retain(|event| event.id > after_cursor);
    events.sort_by_key(|event| event.id);
    let page_size = limit.max(1);
    let has_more = events.len() > page_size;
    events.truncate(page_size);
    let next_cursor = events.last().map(|event| event.id).unwrap_or(after_cursor);
    Ok(IndexDefinitionEventPage {
        events,
        next_cursor,
        has_more,
    })
}

#[derive(Debug, Clone)]
pub struct IndexDefinitionEventPage {
    pub events: Vec<IndexDefinitionEvent>,
    pub next_cursor: i64,
    pub has_more: bool,
}

#[cfg(any())]
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

#[cfg(any())]
pub(crate) async fn current_index_definition_collection_revision(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<i64> {
    current_definitions::collection_revision_mvcc(mvcc, tenant_id, bucket_id)
}

#[cfg(any())]
pub(crate) async fn page_current_index_definition_events(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    include_disabled: bool,
    expected_revision: i64,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<CurrentIndexDefinitionPage> {
    let page = current_definitions::page_mvcc_window(
        mvcc,
        tenant_id,
        bucket_id,
        include_disabled,
        expected_revision,
        after_tuple_key,
        page_size,
    )?;
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

#[cfg(any())]
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

#[cfg(any())]
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

pub fn read_current_index_definition_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    name: &str,
) -> Result<Option<IndexDefinition>> {
    let Some(row) = current_definitions::read_current_mvcc(mvcc, tenant_id, bucket_id, name)?
    else {
        return Ok(None);
    };
    let current = index_current_from_coremeta_row(row)?;
    ensure_index_event_name_matches(&current.event, tenant_id, bucket_id, name)?;
    index_definition_from_event(&current.event).map(Some)
}

pub fn read_current_index_definition_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    name: &str,
    transaction_id: &str,
    principal: &str,
) -> Result<Option<IndexDefinition>> {
    let Some(row) = current_definitions::read_current_in_transaction(
        mvcc,
        tenant_id,
        bucket_id,
        name,
        transaction_id,
        principal,
    )?
    else {
        return Ok(None);
    };
    let current = index_current_from_coremeta_row(row)?;
    ensure_index_event_name_matches(&current.event, tenant_id, bucket_id, name)?;
    index_definition_from_event(&current.event).map(Some)
}

pub fn read_current_index_definitions_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    include_disabled: bool,
) -> Result<Vec<IndexDefinition>> {
    current_definitions::page_mvcc(mvcc, tenant_id, bucket_id, include_disabled)?
        .into_iter()
        .map(index_current_from_coremeta_row)
        .map(|current| current.and_then(|current| index_definition_from_event(&current.event)))
        .collect()
}

pub fn next_index_definition_cursor_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<i64> {
    current_definitions::read_state_mvcc(mvcc, tenant_id, bucket_id)?
        .map(|state| state.latest_cursor)
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("index definition cursor overflow"))
}

pub fn next_index_definition_id_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<i64> {
    current_definitions::read_state_mvcc(mvcc, tenant_id, bucket_id)?
        .map(|state| state.max_index_id)
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("index definition id overflow"))
}

pub fn next_index_definition_id_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    transaction_id: &str,
    principal: &str,
) -> Result<i64> {
    current_definitions::read_state_in_transaction(
        mvcc,
        tenant_id,
        bucket_id,
        transaction_id,
        principal,
    )?
    .map(|state| state.max_index_id)
    .unwrap_or(0)
    .checked_add(1)
    .ok_or_else(|| anyhow::anyhow!("index definition id overflow"))
}

#[cfg(any())]
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

pub fn index_storage_id(tenant_id: i64, bucket_id: i64, index_id: i64) -> String {
    format!("tenant-{tenant_id}-bucket-{bucket_id}-index-{index_id}")
}

pub fn index_definition_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/index_definition").as_bytes())
}

pub(crate) fn index_definition_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("index_definition:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn index_definition_partition_principal(tenant_id: i64, bucket_id: i64) -> String {
    format!("partition-owner:index_definition:{tenant_id}:{bucket_id}")
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
