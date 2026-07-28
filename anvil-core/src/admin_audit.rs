use crate::core_store::{
    CF_OBSERVABILITY, CoreMetaTuplePart, CoreMutationOperation, TABLE_OBSERVABILITY_CURSOR_ROW,
    core_meta_committed_row_common, core_meta_root_key_hash, core_meta_tuple_key,
    decode_deterministic_proto, encode_deterministic_proto, sha256_hex,
};
use anyhow::{Result, anyhow};
use prost::Message;
use serde::{Deserialize, Serialize};

pub const ADMIN_AUDIT_EVENT_SCHEMA: &str = "anvil.admin.audit_event.v1";
const ADMIN_AUDIT_STREAM_PREFIX: &str = "admin_audit:shard";
const ADMIN_AUDIT_SHARD_COUNT: u16 = 256;
pub const ADMIN_AUDIT_PAGE_MAX: usize = 1000;
const ADMIN_AUDIT_PROJECTION_SCHEMA: &str = "anvil.admin.audit_projection.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminAuditEvent {
    pub schema: String,
    pub audit_event_id: String,
    pub request_id: String,
    pub principal_id: String,
    pub resource_id: String,
    pub action: String,
    pub audit_reason: String,
    pub created_at: String,
    pub details_json: String,
}

#[derive(Clone, PartialEq, Message)]
struct AdminAuditEventProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    audit_event_id: String,
    #[prost(string, tag = "3")]
    request_id: String,
    #[prost(string, tag = "4")]
    principal_id: String,
    #[prost(string, tag = "5")]
    resource_id: String,
    #[prost(string, tag = "6")]
    action: String,
    #[prost(string, tag = "7")]
    audit_reason: String,
    #[prost(string, tag = "8")]
    created_at: String,
    #[prost(string, tag = "9")]
    details_json: String,
}

#[derive(Clone, PartialEq, Message)]
struct AdminAuditProjectionProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    event: Option<AdminAuditEventProto>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditEventFilter<'a> {
    pub principal_id: Option<&'a str>,
    pub resource_id: Option<&'a str>,
    pub action: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct AdminAuditEventPage {
    pub events: Vec<AdminAuditEvent>,
    pub next_cursor: Option<Vec<u8>>,
    pub revision: String,
}

pub async fn append_audit_event_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    event: &AdminAuditEvent,
) -> Result<()> {
    require_direct_audit_action(event)?;
    let generation = audit_event_revision_generation(event).max(1);
    let transaction_id = format!("admin-audit:{}", event.audit_event_id);
    let mut plan = admin_audit_mvcc_plan(event, generation, &transaction_id)?;
    plan.predicates.extend(
        audit_projection_keys(event)?
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
        "system:admin-audit",
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

/// Contract for the deliberately small set of control-plane consequences that
/// are outside a cluster MVCC transaction. Product mutations must compose
/// `admin_audit_mvcc_plan` into the transaction that changes their state.
pub fn require_direct_audit_action(event: &AdminAuditEvent) -> Result<()> {
    let allowed = matches!(
        event.action.as_str(),
        "admin.cell.activate"
            | "admin.cell.drain"
            | "admin.cell.register"
            | "admin.cell.remove"
            | "admin.host_alias.activate"
            | "admin.host_alias.create"
            | "admin.host_alias.delete"
            | "admin.host_alias.suspend"
            | "admin.node.activate"
            | "admin.node.drain"
            | "admin.node.force_offline"
            | "admin.node.register"
            | "admin.node.remove"
            | "admin.region.activate"
            | "admin.region.bucket_disposition"
            | "admin.region.create"
            | "admin.region.drain"
            | "admin.region.read_only.set"
            | "admin.region.remove"
            | "admin.routing_record.repair"
            | "admin.secret_encryption_key.rotate"
    ) || (event.action == "admin.repair.run"
        && serde_json::from_str::<serde_json::Value>(&event.details_json)
            .ok()
            .and_then(|details| details.get("repair_kind").and_then(|kind| kind.as_i64()))
            == Some(5));
    if !allowed {
        return Err(anyhow!(
            "admin action {} must publish audit in its originating cluster MVCC transaction",
            event.action
        ));
    }
    Ok(())
}

pub(crate) fn admin_audit_mvcc_plan(
    event: &AdminAuditEvent,
    generation: u64,
    transaction_id: &str,
) -> Result<crate::mvcc_product::ProductMutationPlan> {
    let stream_id = audit_stream_id(&event.audit_event_id);
    let projection = encode_audit_projection(event, &stream_id, generation, transaction_id);
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: "global".to_string(),
        stream_id,
        record_kind: "admin_audit_event".to_string(),
        payload: encode_audit_event(event),
        idempotency_key: Some(event.audit_event_id.clone()),
    }];
    for tuple_key in audit_projection_keys(event)? {
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: "global".to_string(),
            cf: CF_OBSERVABILITY.to_string(),
            table_id: TABLE_OBSERVABILITY_CURSOR_ROW,
            tuple_key,
            payload: projection.clone(),
        });
    }
    crate::mvcc_product::product_mutations_and_outbox_from_operations(operations)
}

pub fn list_audit_event_page_after_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    filter: AuditEventFilter<'_>,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<AdminAuditEventPage> {
    if !(1..=ADMIN_AUDIT_PAGE_MAX).contains(&limit) {
        return Err(anyhow!(
            "admin audit page size must be between 1 and {ADMIN_AUDIT_PAGE_MAX}"
        ));
    }
    let prefix = audit_projection_prefix(&filter)?;
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
        .map(|(_, row)| decode_audit_projection(&row.value))
        .collect::<Result<Vec<_>>>()?;
    if events.iter().any(|event| !matches_filter(event, &filter)) {
        return Err(anyhow!("admin audit projection scope mismatch"));
    }
    Ok(AdminAuditEventPage {
        events,
        next_cursor,
        revision: audit_collection_revision_mvcc(mvcc)?,
    })
}

pub fn audit_collection_revision_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
) -> Result<String> {
    let prefix = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("admin-audit")])?;
    audit_projection_revision_mvcc(mvcc, &prefix, b"anvil-admin-audit-mvcc-revision-v1")
}

fn audit_projection_revision_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tuple_prefix: &[u8],
    domain: &[u8],
) -> Result<String> {
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_OBSERVABILITY, tuple_prefix)?;
    let snapshot = mvcc.runtime.applied_version()?;
    let rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_OBSERVABILITY_CURSOR_ROW,
        &application_prefix,
        snapshot,
    )?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    for (key, row) in rows {
        hasher.update(&key.application_key);
        hasher.update(&row.commit_version.to_le_bytes());
    }
    Ok(hex::encode(hasher.finalize().as_bytes()))
}

fn audit_stream_id(audit_event_id: &str) -> String {
    let digest = sha256_hex(audit_event_id.as_bytes());
    let shard = u16::from_str_radix(&digest[0..2], 16).expect("sha256 hex prefix is valid");
    audit_stream_id_for_shard(shard)
}

fn audit_stream_id_for_shard(shard: u16) -> String {
    debug_assert!(shard < ADMIN_AUDIT_SHARD_COUNT);
    format!("{ADMIN_AUDIT_STREAM_PREFIX}:{shard:02x}")
}

fn audit_projection_keys(event: &AdminAuditEvent) -> Result<Vec<Vec<u8>>> {
    (0_u64..8)
        .map(|mask| {
            let mut parts = audit_projection_scope_parts(
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

fn audit_projection_prefix(filter: &AuditEventFilter<'_>) -> Result<Vec<u8>> {
    let mask = u64::from(filter.principal_id.is_some())
        | (u64::from(filter.resource_id.is_some()) << 1)
        | (u64::from(filter.action.is_some()) << 2);
    core_meta_tuple_key(&audit_projection_scope_parts(
        mask,
        filter.principal_id,
        filter.resource_id,
        filter.action,
    )?)
}

fn audit_projection_scope_parts<'a>(
    mask: u64,
    principal_id: Option<&'a str>,
    resource_id: Option<&'a str>,
    action: Option<&'a str>,
) -> Result<Vec<CoreMetaTuplePart<'a>>> {
    let mut parts = vec![
        CoreMetaTuplePart::Utf8("admin-audit"),
        CoreMetaTuplePart::U64(mask),
    ];
    if mask & 1 != 0 {
        parts.push(CoreMetaTuplePart::Utf8(principal_id.ok_or_else(|| {
            anyhow!("admin audit principal scope is missing")
        })?));
    }
    if mask & 2 != 0 {
        parts.push(CoreMetaTuplePart::Utf8(
            resource_id.ok_or_else(|| anyhow!("admin audit resource scope is missing"))?,
        ));
    }
    if mask & 4 != 0 {
        parts.push(CoreMetaTuplePart::Utf8(
            action.ok_or_else(|| anyhow!("admin audit action scope is missing"))?,
        ));
    }
    Ok(parts)
}

fn encode_audit_projection(
    event: &AdminAuditEvent,
    stream_id: &str,
    root_generation: u64,
    transaction_id: &str,
) -> Vec<u8> {
    encode_deterministic_proto(&AdminAuditProjectionProto {
        common: Some(core_meta_committed_row_common(
            "system",
            core_meta_root_key_hash(&audit_projection_root_anchor_key(stream_id)),
            root_generation,
            transaction_id,
            root_generation,
        )),
        schema: ADMIN_AUDIT_PROJECTION_SCHEMA.to_string(),
        event: Some(audit_event_to_proto(event)),
    })
}

fn audit_projection_root_anchor_key(stream_id: &str) -> String {
    format!("stream/{stream_id}")
}

fn decode_audit_projection(bytes: &[u8]) -> Result<AdminAuditEvent> {
    let projection =
        decode_deterministic_proto::<AdminAuditProjectionProto>(bytes, "admin audit projection")?;
    if projection.common.is_none() || projection.schema != ADMIN_AUDIT_PROJECTION_SCHEMA {
        return Err(anyhow!("admin audit projection schema mismatch"));
    }
    audit_event_from_proto(
        projection
            .event
            .ok_or_else(|| anyhow!("admin audit projection is missing event"))?,
    )
}

fn encode_audit_event(event: &AdminAuditEvent) -> Vec<u8> {
    encode_deterministic_proto(&audit_event_to_proto(event))
}

fn audit_event_to_proto(event: &AdminAuditEvent) -> AdminAuditEventProto {
    AdminAuditEventProto {
        schema: event.schema.clone(),
        audit_event_id: event.audit_event_id.clone(),
        request_id: event.request_id.clone(),
        principal_id: event.principal_id.clone(),
        resource_id: event.resource_id.clone(),
        action: event.action.clone(),
        audit_reason: event.audit_reason.clone(),
        created_at: event.created_at.clone(),
        details_json: event.details_json.clone(),
    }
}

fn decode_audit_event(bytes: &[u8]) -> Result<AdminAuditEvent> {
    let proto =
        decode_deterministic_proto::<AdminAuditEventProto>(bytes, "admin audit event payload")?;
    audit_event_from_proto(proto)
}

fn audit_event_from_proto(proto: AdminAuditEventProto) -> Result<AdminAuditEvent> {
    if proto.schema != ADMIN_AUDIT_EVENT_SCHEMA {
        return Err(anyhow!("admin audit event schema mismatch"));
    }
    Ok(AdminAuditEvent {
        schema: proto.schema,
        audit_event_id: proto.audit_event_id,
        request_id: proto.request_id,
        principal_id: proto.principal_id,
        resource_id: proto.resource_id,
        action: proto.action,
        audit_reason: proto.audit_reason,
        created_at: proto.created_at,
        details_json: proto.details_json,
    })
}

pub fn audit_event_position(event: &AdminAuditEvent) -> String {
    format!("{}:{}", event.created_at, event.audit_event_id)
}

pub fn audit_event_revision_generation(event: &AdminAuditEvent) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil-admin-audit-event-revision-v1");
    update_hash_part(&mut hasher, event.schema.as_bytes());
    update_hash_part(&mut hasher, event.audit_event_id.as_bytes());
    update_hash_part(&mut hasher, event.request_id.as_bytes());
    update_hash_part(&mut hasher, event.principal_id.as_bytes());
    update_hash_part(&mut hasher, event.resource_id.as_bytes());
    update_hash_part(&mut hasher, event.action.as_bytes());
    update_hash_part(&mut hasher, event.audit_reason.as_bytes());
    update_hash_part(&mut hasher, event.created_at.as_bytes());
    update_hash_part(&mut hasher, event.details_json.as_bytes());
    let digest = hasher.finalize();
    u64::from_le_bytes(digest.as_bytes()[0..8].try_into().expect("blake3 digest"))
}

fn matches_filter(event: &AdminAuditEvent, filter: &AuditEventFilter<'_>) -> bool {
    filter
        .principal_id
        .is_none_or(|principal_id| event.principal_id == principal_id)
        && filter
            .resource_id
            .is_none_or(|resource_id| event.resource_id == resource_id)
        && filter.action.is_none_or(|action| event.action == action)
}

fn update_hash_part(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: &str, principal: &str, resource: &str, action: &str) -> AdminAuditEvent {
        AdminAuditEvent {
            schema: ADMIN_AUDIT_EVENT_SCHEMA.to_string(),
            audit_event_id: id.to_string(),
            request_id: format!("req-{id}"),
            principal_id: principal.to_string(),
            resource_id: resource.to_string(),
            action: action.to_string(),
            audit_reason: "test".to_string(),
            created_at: "2026-07-02T20:00:00Z".to_string(),
            details_json: "{}".to_string(),
        }
    }

    #[test]
    fn direct_audit_contract_keeps_product_repairs_inside_mvcc() {
        let mut repair = event("audit-1", "admin-1", "repair-1", "admin.repair.run");
        repair.details_json = serde_json::json!({ "repair_kind": 1 }).to_string();
        assert!(require_direct_audit_action(&repair).is_err());

        repair.details_json = serde_json::json!({ "repair_kind": 5 }).to_string();
        assert!(require_direct_audit_action(&repair).is_ok());

        let product = event("audit-2", "admin-1", "tenant-1", "admin.tenant.create");
        assert!(require_direct_audit_action(&product).is_err());
    }
}
