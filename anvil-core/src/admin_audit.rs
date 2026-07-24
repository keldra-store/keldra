use crate::{
    core_store::{
        CF_OBSERVABILITY, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation,
        CoreMutationPrecondition, CoreMutationRootPublication, CoreStore,
        TABLE_OBSERVABILITY_CURSOR_ROW, core_meta_committed_row_common, core_meta_record_tuple_key,
        core_meta_root_key_hash, core_meta_tuple_key, decode_deterministic_proto,
        encode_deterministic_proto, sha256_hex,
    },
    formats::writer::WriterFamily,
    storage::Storage,
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

pub async fn append_audit_event(storage: &Storage, event: &AdminAuditEvent) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = audit_stream_id(&event.audit_event_id);
    let stream_precondition = core_store.stream_head_precondition(&stream_id).await?;
    let root_generation = next_stream_generation(&stream_precondition)?;
    let transaction_id = format!("admin-audit:{}", event.audit_event_id);
    let projection_root_anchor_key = audit_projection_root_anchor_key(&stream_id);
    let projection = encode_audit_projection(event, &stream_id, root_generation, &transaction_id);
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
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: "global".to_string(),
            committed_by_principal: "system:admin-audit".to_string(),
            root_publications: vec![
                CoreMutationRootPublication::new("global", WriterFamily::CoreControl.as_str())
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

pub async fn list_audit_event_page(
    storage: &Storage,
    filter: AuditEventFilter<'_>,
) -> Result<AdminAuditEventPage> {
    list_audit_event_page_after(storage, filter, None, ADMIN_AUDIT_PAGE_MAX).await
}

pub async fn list_audit_event_page_after(
    storage: &Storage,
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
                    .ok_or_else(|| anyhow!("admin audit continuation has no row"))?
                    .key,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let events = rows
        .into_iter()
        .map(|row| decode_audit_projection(&row.payload))
        .collect::<Result<Vec<_>>>()?;
    if events.iter().any(|event| !matches_filter(event, &filter)) {
        return Err(anyhow!("admin audit projection scope mismatch"));
    }
    Ok(AdminAuditEventPage {
        events,
        next_cursor,
        revision: audit_collection_revision(storage).await?,
    })
}

pub async fn audit_collection_revision(storage: &Storage) -> Result<String> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil-admin-audit-collection-revision-v2");
    for shard in 0..ADMIN_AUDIT_SHARD_COUNT {
        hasher.update(
            &core_store
                .stream_head_sequence(&audit_stream_id_for_shard(shard))
                .await?
                .to_le_bytes(),
        );
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

fn next_stream_generation(precondition: &CoreMutationPrecondition) -> Result<u64> {
    let CoreMutationPrecondition::StreamHead {
        expected_last_sequence,
        ..
    } = precondition
    else {
        return Err(anyhow!("admin audit stream precondition has wrong kind"));
    };
    expected_last_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("admin audit stream sequence overflow"))
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
    use crate::core_store::ReadStream;
    use crate::storage::Storage;
    use tempfile::tempdir;

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

    #[tokio::test]
    async fn audit_events_are_durable_and_filterable() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_audit_event(&storage, &event("audit-a", "admin-a", "bucket-a", "create"))
            .await
            .unwrap();
        append_audit_event(&storage, &event("audit-b", "admin-b", "bucket-b", "delete"))
            .await
            .unwrap();

        let raw = CoreStore::new(storage.clone())
            .await
            .unwrap()
            .read_stream(ReadStream {
                stream_id: audit_stream_id("audit-a"),
                after_sequence: 0,
                limit: 1,
            })
            .await
            .unwrap();
        assert_ne!(raw[0].payload.first().copied(), Some(b'{'));
        assert!(decode_audit_event(&raw[0].payload).is_ok());

        let all = list_audit_event_page_after(&storage, AuditEventFilter::default(), None, 10)
            .await
            .unwrap();
        assert_eq!(all.events.len(), 2);

        let filtered = list_audit_event_page_after(
            &storage,
            AuditEventFilter {
                principal_id: Some("admin-a"),
                resource_id: Some("bucket-a"),
                action: Some("create"),
            },
            None,
            10,
        )
        .await
        .unwrap();
        assert_eq!(filtered.events.len(), 1);
        assert_eq!(filtered.events[0].audit_event_id, "audit-a");
    }

    #[tokio::test]
    async fn filtered_pages_do_not_read_unrelated_audit_history() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for index in 0..64 {
            append_audit_event(
                &storage,
                &event(
                    &format!("noise-{index:03}"),
                    "unrelated",
                    "other-bucket",
                    "read",
                ),
            )
            .await
            .unwrap();
        }
        for index in 0..3 {
            append_audit_event(
                &storage,
                &event(
                    &format!("target-{index:03}"),
                    "admin-a",
                    "bucket-a",
                    "create",
                ),
            )
            .await
            .unwrap();
        }

        let filter = AuditEventFilter {
            principal_id: Some("admin-a"),
            resource_id: Some("bucket-a"),
            action: Some("create"),
        };
        let first = list_audit_event_page_after(&storage, filter.clone(), None, 2)
            .await
            .unwrap();
        assert_eq!(first.events.len(), 2);
        let second = list_audit_event_page_after(&storage, filter, first.next_cursor.as_deref(), 2)
            .await
            .unwrap();
        assert_eq!(second.events.len(), 1);
        assert!(second.next_cursor.is_none());
    }
}
