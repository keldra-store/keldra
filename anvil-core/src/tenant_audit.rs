use crate::{
    core_store::{
        CF_OBSERVABILITY, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation,
        CoreMutationPrecondition, CoreMutationRootPublication, CoreStore,
        TABLE_OBSERVABILITY_CURSOR_ROW, core_meta_committed_row_common, core_meta_record_tuple_key,
        core_meta_root_key_hash, core_meta_tuple_key, decode_deterministic_proto,
        encode_deterministic_proto,
    },
    formats::writer::WriterFamily,
    storage::Storage,
};
use anyhow::{Result, anyhow};
use prost::Message;
use serde::{Deserialize, Serialize};

pub const TENANT_AUDIT_EVENT_SCHEMA: &str = "anvil.tenant.audit_event.v1";
pub const TENANT_AUDIT_PAGE_MAX: usize = 1000;
const TENANT_AUDIT_PROJECTION_SCHEMA: &str = "anvil.tenant.audit_projection.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantAuditEvent {
    pub schema: String,
    pub audit_event_id: String,
    pub request_id: String,
    pub tenant_id: i64,
    pub principal_id: String,
    pub resource_id: String,
    pub action: String,
    pub created_at: String,
    pub details_json: String,
}

#[derive(Clone, PartialEq, Message)]
struct TenantAuditEventProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    audit_event_id: String,
    #[prost(string, tag = "3")]
    request_id: String,
    #[prost(int64, tag = "4")]
    tenant_id: i64,
    #[prost(string, tag = "5")]
    principal_id: String,
    #[prost(string, tag = "6")]
    resource_id: String,
    #[prost(string, tag = "7")]
    action: String,
    #[prost(string, tag = "8")]
    created_at: String,
    #[prost(string, tag = "9")]
    details_json: String,
}

#[derive(Clone, PartialEq, Message)]
struct TenantAuditProjectionProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    event: Option<TenantAuditEventProto>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TenantAuditEventFilter<'a> {
    pub principal_id: Option<&'a str>,
    pub resource_id: Option<&'a str>,
    pub action: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct TenantAuditEventPage {
    pub events: Vec<TenantAuditEvent>,
    pub next_cursor: Option<Vec<u8>>,
    pub revision: String,
}

pub async fn append_tenant_audit_event(storage: &Storage, event: &TenantAuditEvent) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = tenant_audit_stream_id(event.tenant_id);
    let stream_precondition = core_store.stream_head_precondition(&stream_id).await?;
    let root_generation = next_stream_generation(&stream_precondition)?;
    let transaction_id = format!("tenant-audit:{}:{}", event.tenant_id, event.audit_event_id);
    let projection_root_anchor_key = tenant_audit_projection_root_anchor_key(&stream_id);
    let projection =
        encode_tenant_audit_projection(event, &stream_id, root_generation, &transaction_id);
    let partition_id = format!("tenant:{}", event.tenant_id);
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: partition_id.clone(),
        stream_id,
        record_kind: "tenant_audit_event".to_string(),
        payload: encode_tenant_audit_event(event),
        idempotency_key: Some(event.audit_event_id.clone()),
    }];
    for tuple_key in tenant_audit_projection_keys(event)? {
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.clone(),
            cf: CF_OBSERVABILITY.to_string(),
            table_id: TABLE_OBSERVABILITY_CURSOR_ROW,
            tuple_key,
            payload: projection.clone(),
        });
    }
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: partition_id.clone(),
            committed_by_principal: format!("tenant:{}:audit", event.tenant_id),
            root_publications: vec![
                CoreMutationRootPublication::new(partition_id, WriterFamily::CoreControl.as_str())
                    .coordinator(),
                CoreMutationRootPublication::new(
                    projection_root_anchor_key,
                    WriterFamily::Stream.as_str(),
                ),
            ],
            preconditions: vec![stream_precondition],
            operations,
        })
        .await?;
    Ok(())
}

pub async fn list_tenant_audit_event_page(
    storage: &Storage,
    tenant_id: i64,
    filter: TenantAuditEventFilter<'_>,
) -> Result<TenantAuditEventPage> {
    list_tenant_audit_event_page_after(storage, tenant_id, filter, None, TENANT_AUDIT_PAGE_MAX)
        .await
}

pub async fn list_tenant_audit_event_page_after(
    storage: &Storage,
    tenant_id: i64,
    filter: TenantAuditEventFilter<'_>,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<TenantAuditEventPage> {
    if !(1..=TENANT_AUDIT_PAGE_MAX).contains(&limit) {
        return Err(anyhow!(
            "tenant audit page size must be between 1 and {TENANT_AUDIT_PAGE_MAX}"
        ));
    }
    let prefix = tenant_audit_projection_prefix(tenant_id, &filter)?;
    let store = CoreStore::new(storage.clone()).await?;
    let mut rows = store.scan_coremeta_prefix_page(
        CF_OBSERVABILITY,
        TABLE_OBSERVABILITY_CURSOR_ROW,
        &prefix,
        after_cursor,
        limit + 1,
    )?;
    let has_more = rows.len() > limit;
    if has_more {
        rows.truncate(limit);
    }
    let next_cursor = if has_more {
        Some(
            core_meta_record_tuple_key(
                &rows
                    .last()
                    .ok_or_else(|| anyhow!("tenant audit continuation has no row"))?
                    .key,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let events = rows
        .into_iter()
        .map(|row| decode_tenant_audit_projection(&row.payload))
        .collect::<Result<Vec<_>>>()?;
    if events
        .iter()
        .any(|event| event.tenant_id != tenant_id || !matches_filter(event, &filter))
    {
        return Err(anyhow!("tenant audit projection scope mismatch"));
    }
    Ok(TenantAuditEventPage {
        events,
        next_cursor,
        revision: tenant_audit_collection_revision(storage, tenant_id).await?,
    })
}

pub async fn tenant_audit_collection_revision(storage: &Storage, tenant_id: i64) -> Result<String> {
    Ok(CoreStore::new(storage.clone())
        .await?
        .stream_head_sequence(&tenant_audit_stream_id(tenant_id))
        .await?
        .to_string())
}

pub fn audit_event_position(event: &TenantAuditEvent) -> String {
    format!("{}:{}", event.created_at, event.audit_event_id)
}

pub fn audit_event_revision_generation(event: &TenantAuditEvent) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil-tenant-audit-event-revision-v1");
    for part in [
        event.schema.as_bytes(),
        event.audit_event_id.as_bytes(),
        event.request_id.as_bytes(),
        &event.tenant_id.to_le_bytes(),
        event.principal_id.as_bytes(),
        event.resource_id.as_bytes(),
        event.action.as_bytes(),
        event.created_at.as_bytes(),
        event.details_json.as_bytes(),
    ] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    u64::from_le_bytes(
        hasher.finalize().as_bytes()[0..8]
            .try_into()
            .expect("blake3 digest"),
    )
}

pub fn collection_revision<'a>(events: impl IntoIterator<Item = &'a TenantAuditEvent>) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"tenant-audit-collection-revision-v1");
    for event in events {
        let position = audit_event_position(event);
        hasher.update(&(position.len() as u64).to_le_bytes());
        hasher.update(position.as_bytes());
        hasher.update(&audit_event_revision_generation(event).to_le_bytes());
    }
    hex::encode(hasher.finalize().as_bytes())
}

fn tenant_audit_stream_id(tenant_id: i64) -> String {
    format!("tenant_audit:{tenant_id}")
}

fn next_stream_generation(precondition: &CoreMutationPrecondition) -> Result<u64> {
    let CoreMutationPrecondition::StreamHead {
        expected_last_sequence,
        ..
    } = precondition
    else {
        return Err(anyhow!("tenant audit stream precondition has wrong kind"));
    };
    expected_last_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("tenant audit stream sequence overflow"))
}

fn tenant_audit_projection_keys(event: &TenantAuditEvent) -> Result<Vec<Vec<u8>>> {
    (0_u64..8)
        .map(|mask| {
            let mut parts = tenant_audit_projection_scope_parts(
                event.tenant_id,
                mask,
                Some(event.principal_id.as_str()),
                Some(event.resource_id.as_str()),
                Some(event.action.as_str()),
            )?;
            parts.push(CoreMetaTuplePart::Utf8(&event.created_at));
            parts.push(CoreMetaTuplePart::Utf8(&event.audit_event_id));
            core_meta_tuple_key(&parts)
        })
        .collect()
}

fn tenant_audit_projection_prefix(
    tenant_id: i64,
    filter: &TenantAuditEventFilter<'_>,
) -> Result<Vec<u8>> {
    let mask = u64::from(filter.principal_id.is_some())
        | (u64::from(filter.resource_id.is_some()) << 1)
        | (u64::from(filter.action.is_some()) << 2);
    core_meta_tuple_key(&tenant_audit_projection_scope_parts(
        tenant_id,
        mask,
        filter.principal_id,
        filter.resource_id,
        filter.action,
    )?)
}

fn tenant_audit_projection_scope_parts<'a>(
    tenant_id: i64,
    mask: u64,
    principal_id: Option<&'a str>,
    resource_id: Option<&'a str>,
    action: Option<&'a str>,
) -> Result<Vec<CoreMetaTuplePart<'a>>> {
    let mut parts = vec![
        CoreMetaTuplePart::Utf8("tenant-audit"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::U64(mask),
    ];
    if mask & 1 != 0 {
        parts.push(CoreMetaTuplePart::Utf8(principal_id.ok_or_else(|| {
            anyhow!("tenant audit principal scope is missing")
        })?));
    }
    if mask & 2 != 0 {
        parts.push(CoreMetaTuplePart::Utf8(resource_id.ok_or_else(|| {
            anyhow!("tenant audit resource scope is missing")
        })?));
    }
    if mask & 4 != 0 {
        parts.push(CoreMetaTuplePart::Utf8(
            action.ok_or_else(|| anyhow!("tenant audit action scope is missing"))?,
        ));
    }
    Ok(parts)
}

fn encode_tenant_audit_projection(
    event: &TenantAuditEvent,
    stream_id: &str,
    root_generation: u64,
    transaction_id: &str,
) -> Vec<u8> {
    encode_deterministic_proto(&TenantAuditProjectionProto {
        common: Some(core_meta_committed_row_common(
            "system",
            core_meta_root_key_hash(&tenant_audit_projection_root_anchor_key(stream_id)),
            root_generation,
            transaction_id,
            root_generation,
        )),
        schema: TENANT_AUDIT_PROJECTION_SCHEMA.to_string(),
        event: Some(tenant_audit_event_to_proto(event)),
    })
}

fn tenant_audit_projection_root_anchor_key(stream_id: &str) -> String {
    format!("stream/{stream_id}")
}

fn decode_tenant_audit_projection(bytes: &[u8]) -> Result<TenantAuditEvent> {
    let projection =
        decode_deterministic_proto::<TenantAuditProjectionProto>(bytes, "tenant audit projection")?;
    if projection.common.is_none() || projection.schema != TENANT_AUDIT_PROJECTION_SCHEMA {
        return Err(anyhow!("tenant audit projection schema mismatch"));
    }
    tenant_audit_event_from_proto(
        projection
            .event
            .ok_or_else(|| anyhow!("tenant audit projection is missing event"))?,
    )
}

fn encode_tenant_audit_event(event: &TenantAuditEvent) -> Vec<u8> {
    encode_deterministic_proto(&tenant_audit_event_to_proto(event))
}

fn tenant_audit_event_to_proto(event: &TenantAuditEvent) -> TenantAuditEventProto {
    TenantAuditEventProto {
        schema: event.schema.clone(),
        audit_event_id: event.audit_event_id.clone(),
        request_id: event.request_id.clone(),
        tenant_id: event.tenant_id,
        principal_id: event.principal_id.clone(),
        resource_id: event.resource_id.clone(),
        action: event.action.clone(),
        created_at: event.created_at.clone(),
        details_json: event.details_json.clone(),
    }
}

fn decode_tenant_audit_event(bytes: &[u8]) -> Result<TenantAuditEvent> {
    let proto =
        decode_deterministic_proto::<TenantAuditEventProto>(bytes, "tenant audit event payload")?;
    tenant_audit_event_from_proto(proto)
}

fn tenant_audit_event_from_proto(proto: TenantAuditEventProto) -> Result<TenantAuditEvent> {
    if proto.schema != TENANT_AUDIT_EVENT_SCHEMA {
        return Err(anyhow!("tenant audit event schema mismatch"));
    }
    Ok(TenantAuditEvent {
        schema: proto.schema,
        audit_event_id: proto.audit_event_id,
        request_id: proto.request_id,
        tenant_id: proto.tenant_id,
        principal_id: proto.principal_id,
        resource_id: proto.resource_id,
        action: proto.action,
        created_at: proto.created_at,
        details_json: proto.details_json,
    })
}

fn matches_filter(event: &TenantAuditEvent, filter: &TenantAuditEventFilter<'_>) -> bool {
    filter
        .principal_id
        .is_none_or(|value| event.principal_id == value)
        && filter
            .resource_id
            .is_none_or(|value| event.resource_id == value)
        && filter.action.is_none_or(|value| event.action == value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn event(id: &str, tenant_id: i64, principal: &str) -> TenantAuditEvent {
        TenantAuditEvent {
            schema: TENANT_AUDIT_EVENT_SCHEMA.to_string(),
            audit_event_id: id.to_string(),
            request_id: format!("request-{id}"),
            tenant_id,
            principal_id: principal.to_string(),
            resource_id: "bucket-a".to_string(),
            action: "write".to_string(),
            created_at: format!("2026-07-02T20:00:{id}Z"),
            details_json: "{}".to_string(),
        }
    }

    #[tokio::test]
    async fn tenant_audit_pages_are_bounded_and_scope_unrelated_history() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for index in 0..48 {
            append_tenant_audit_event(&storage, &event(&format!("{index:02}"), 11, "unrelated"))
                .await
                .unwrap();
        }
        for index in 50..53 {
            append_tenant_audit_event(&storage, &event(&format!("{index:02}"), 11, "target"))
                .await
                .unwrap();
        }
        for index in 0..32 {
            append_tenant_audit_event(&storage, &event(&format!("{index:02}"), 12, "target"))
                .await
                .unwrap();
        }

        let filter = TenantAuditEventFilter {
            principal_id: Some("target"),
            resource_id: Some("bucket-a"),
            action: Some("write"),
        };
        let first = list_tenant_audit_event_page_after(&storage, 11, filter.clone(), None, 2)
            .await
            .unwrap();
        assert_eq!(first.events.len(), 2);
        let second = list_tenant_audit_event_page_after(
            &storage,
            11,
            filter,
            first.next_cursor.as_deref(),
            2,
        )
        .await
        .unwrap();
        assert_eq!(second.events.len(), 1);
        assert!(second.next_cursor.is_none());
        assert!(second.events.iter().all(|event| event.tenant_id == 11));
    }
}
