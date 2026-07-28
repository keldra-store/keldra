use crate::core_store::{
    CF_OBSERVABILITY, CoreMetaTuplePart, CoreMutationOperation, TABLE_OBSERVABILITY_CURSOR_ROW,
    core_meta_committed_row_common, core_meta_root_key_hash, core_meta_tuple_key,
    decode_deterministic_proto, encode_deterministic_proto,
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

pub async fn append_tenant_audit_event_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    event: &TenantAuditEvent,
) -> Result<()> {
    require_direct_tenant_audit_action(event)?;
    let generation = audit_event_revision_generation(event).max(1);
    let transaction_id = format!("tenant-audit:{}:{}", event.tenant_id, event.audit_event_id);
    let mut plan = tenant_audit_mvcc_plan(event, generation, &transaction_id)?;
    plan.predicates.extend(
        tenant_audit_projection_keys(event)?
            .into_iter()
            .map(|tuple_key| {
                Ok((
                    crate::mvcc_product::coremeta_logical_key(
                        CF_OBSERVABILITY,
                        TABLE_OBSERVABILITY_CURSOR_ROW,
                        &tuple_key,
                    )?,
                    crate::mvcc_transaction::PredicateKind::Absent,
                ))
            })
            .collect::<Result<Vec<_>>>()?,
    );
    mvcc.autocommit_product_mutations_with_predicates_and_outbox(
        &format!("tenant:{}:audit", event.tenant_id),
        &transaction_id,
        plan.mutations,
        plan.predicates,
        plan.outbox_events,
        crate::mvcc_transaction::DurabilityLevel::Quorum,
        chrono::Utc::now().timestamp_millis().max(0) as u64,
    )
    .await?;
    Ok(())
}

/// Contract for tenant audit consequences whose authority lives in the
/// mesh-routing control plane rather than in a cluster product transaction.
/// All cluster-local product mutations must compose `tenant_audit_mvcc_plan`
/// into their originating MVCC transaction instead.
pub fn require_direct_tenant_audit_action(event: &TenantAuditEvent) -> Result<()> {
    if !matches!(
        event.action.as_str(),
        "host_alias.create" | "host_alias.verify" | "host_alias.delete"
    ) {
        return Err(anyhow!(
            "tenant action {} must publish audit in its originating cluster MVCC transaction",
            event.action
        ));
    }
    Ok(())
}

pub(crate) fn tenant_audit_mvcc_plan(
    event: &TenantAuditEvent,
    generation: u64,
    transaction_id: &str,
) -> Result<crate::mvcc_product::ProductMutationPlan> {
    let stream_id = tenant_audit_stream_id(event.tenant_id);
    let partition_id = format!("tenant:{}", event.tenant_id);
    let projection = encode_tenant_audit_projection(event, &stream_id, generation, transaction_id);
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
    crate::mvcc_product::product_mutations_and_outbox_from_operations(operations)
}

pub fn list_tenant_audit_event_page_after_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
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
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_OBSERVABILITY, &prefix)?;
    let snapshot = mvcc.runtime.applied_version()?;
    let mut rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_OBSERVABILITY_CURSOR_ROW,
        &application_prefix,
        snapshot,
    )?;
    rows.sort_by(|(left, _), (right, _)| left.application_key.cmp(&right.application_key));
    if let Some(after_cursor) = after_cursor {
        rows.retain(|(key, _)| {
            crate::mvcc_product::coremeta_tuple_from_logical_key(key, CF_OBSERVABILITY)
                .is_ok_and(|tuple| tuple > after_cursor)
        });
    }
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let next_cursor = if has_more {
        rows.last()
            .map(|(key, _)| {
                crate::mvcc_product::coremeta_tuple_from_logical_key(key, CF_OBSERVABILITY)
                    .map(Vec::from)
            })
            .transpose()?
    } else {
        None
    };
    let events = rows
        .into_iter()
        .map(|(_, row)| decode_tenant_audit_projection(&row.value))
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
        revision: tenant_audit_collection_revision_mvcc(mvcc, tenant_id)?,
    })
}

pub fn tenant_audit_collection_revision_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
) -> Result<String> {
    let prefix = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("tenant-audit"),
        CoreMetaTuplePart::I64(tenant_id),
    ])?;
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_OBSERVABILITY, &prefix)?;
    let snapshot = mvcc.runtime.applied_version()?;
    let rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_OBSERVABILITY_CURSOR_ROW,
        &application_prefix,
        snapshot,
    )?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil-tenant-audit-mvcc-revision-v1");
    for (key, row) in rows {
        hasher.update(&key.application_key);
        hasher.update(&row.commit_version.to_le_bytes());
    }
    Ok(hex::encode(hasher.finalize().as_bytes()))
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

    fn event(id: &str, tenant_id: i64, principal: &str) -> TenantAuditEvent {
        TenantAuditEvent {
            schema: TENANT_AUDIT_EVENT_SCHEMA.to_string(),
            audit_event_id: id.to_string(),
            request_id: format!("request-{id}"),
            tenant_id,
            principal_id: principal.to_string(),
            resource_id: "host_alias:docs.example.com".to_string(),
            action: "host_alias.create".to_string(),
            created_at: format!("2026-07-02T20:00:{id}Z"),
            details_json: "{}".to_string(),
        }
    }

    #[test]
    fn direct_tenant_audit_accepts_only_external_host_alias_actions() {
        for action in [
            "host_alias.create",
            "host_alias.verify",
            "host_alias.delete",
        ] {
            let mut candidate = event("01", 11, "principal");
            candidate.action = action.to_string();
            require_direct_tenant_audit_action(&candidate).unwrap();
        }

        for action in [
            "object.put",
            "bucket.update",
            "policy.grant",
            "host_alias.activate",
        ] {
            let mut candidate = event("01", 11, "principal");
            candidate.action = action.to_string();
            let error = require_direct_tenant_audit_action(&candidate).unwrap_err();
            assert!(error.to_string().contains("originating cluster MVCC"));
        }
    }
}
