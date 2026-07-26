use crate::{
    core_store::{decode_deterministic_proto, encode_deterministic_proto},
    formats::{Hash32, hash32, watch::WatchRecord},
    mvcc_bootstrap::MvccSubsystem,
    mvcc_product::ProductMutation,
    mvcc_transaction::{DurabilityLevel, LogicalKey, PredicateKind, ReadConsistency},
};
use anyhow::{Result, anyhow};
use prost::Message;
use serde::{Deserialize, Serialize};

const AUTHZ_NAMESPACE_PARTITION_FAMILY: u16 = 9;
const AUTHZ_NAMESPACE_RECORD_KIND: u16 = 1;
const TABLE_AUTHZ_NAMESPACE_WATCH: u16 = 0x0509;

#[derive(Clone, PartialEq, Message)]
struct AuthzNamespaceWatchPayloadProto {
    #[prost(string, tag = "1")]
    namespace: String,
    #[prost(string, tag = "2")]
    event_type: String,
    #[prost(uint64, tag = "3")]
    authz_revision: u64,
    #[prost(string, tag = "4")]
    schema_hash: String,
    #[prost(bool, tag = "5")]
    invalidates_derived_usersets: bool,
    #[prost(string, tag = "6")]
    emitted_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzNamespaceWatchPayload {
    pub namespace: String,
    pub event_type: String,
    pub authz_revision: u64,
    pub schema_hash: String,
    pub invalidates_derived_usersets: bool,
    pub emitted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzNamespaceWatchEvent {
    pub cursor: u128,
    pub mutation_id: [u8; 16],
    pub authz_revision: u64,
    pub payload: AuthzNamespaceWatchPayload,
}

pub async fn append_authz_namespace_watch_record(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    mutation_id: [u8; 16],
    payload: AuthzNamespaceWatchPayload,
) -> Result<u128> {
    validate_payload(&payload)?;
    let key = watch_key(tenant_id, &payload.namespace, mutation_id)?;
    let record = WatchRecord::new(
        0,
        AUTHZ_NAMESPACE_PARTITION_FAMILY,
        partition_id(tenant_id, &payload.namespace),
        mutation_id,
        AUTHZ_NAMESPACE_RECORD_KIND,
        payload.authz_revision,
        0,
        0,
        encode_authz_namespace_watch_payload(&payload)?,
    );
    let committed = mvcc
        .autocommit_product_mutations_with_predicates(
            "system/authz-namespace-watch",
            &format!(
                "authz-namespace-watch:{tenant_id}:{}:{}",
                payload.namespace,
                hex::encode(mutation_id)
            ),
            vec![ProductMutation::put(key.clone(), record.encode())],
            vec![(key, PredicateKind::Unique)],
            DurabilityLevel::Quorum,
            now_unix_ms(),
        )
        .await?;
    Ok(u128::from(committed))
}

pub async fn list_authz_namespace_watch_events(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    namespace: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<AuthzNamespaceWatchEvent>> {
    Ok(
        list_authz_namespace_watch_event_page(mvcc, tenant_id, namespace, after_cursor, limit)
            .await?
            .events,
    )
}

#[derive(Debug, Clone)]
pub struct AuthzNamespaceWatchEventPage {
    pub events: Vec<AuthzNamespaceWatchEvent>,
    pub next_cursor: u128,
    pub has_more: bool,
}

pub async fn list_authz_namespace_watch_event_page(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    namespace: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<AuthzNamespaceWatchEventPage> {
    if limit == 0 {
        return Err(anyhow!(
            "authorization namespace watch limit must be nonzero"
        ));
    }
    let after_version = u64::try_from(after_cursor)
        .map_err(|_| anyhow!("authorization namespace watch cursor exceeds u64"))?;
    let snapshot = mvcc.runtime.snapshot(ReadConsistency::Linearized).await?;
    let prefix = watch_prefix(tenant_id, namespace)?;
    let mut page =
        mvcc.runtime
            .scan_table_prefix_at(TABLE_AUTHZ_NAMESPACE_WATCH, &prefix, snapshot)?;
    page.retain(|(_, row)| row.commit_version > after_version);
    page.sort_by_key(|(_, row)| row.commit_version);
    let has_more = page.len() > limit;
    page.truncate(limit);
    let expected_partition = partition_id(tenant_id, namespace);
    let mut events = Vec::with_capacity(page.len());
    for (_, source) in page {
        let (mut record, used) = WatchRecord::decode(&source.value)?;
        if used != source.value.len() {
            return Err(anyhow!(
                "authorization namespace watch record has trailing bytes"
            ));
        }
        record.cursor = u128::from(source.commit_version);
        if record.partition_family != AUTHZ_NAMESPACE_PARTITION_FAMILY
            || record.record_kind != AUTHZ_NAMESPACE_RECORD_KIND
            || record.partition_id != expected_partition
        {
            return Err(anyhow!(
                "authorization namespace watch record scope mismatch"
            ));
        }
        let payload: AuthzNamespaceWatchPayload =
            decode_authz_namespace_watch_payload(&record.payload)?;
        if payload.namespace != namespace {
            return Err(anyhow!(
                "authorization namespace watch payload scope mismatch"
            ));
        }
        validate_payload(&payload)?;
        events.push(AuthzNamespaceWatchEvent {
            cursor: record.cursor,
            mutation_id: record.mutation_id,
            authz_revision: record.authz_revision,
            payload,
        });
    }
    let next_cursor = events
        .last()
        .map(|event| event.cursor)
        .unwrap_or(after_cursor);
    Ok(AuthzNamespaceWatchEventPage {
        events,
        next_cursor,
        has_more,
    })
}

pub async fn latest_authz_namespace_watch_cursor(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    namespace: &str,
) -> Result<Option<u128>> {
    let snapshot = mvcc.runtime.snapshot(ReadConsistency::Linearized).await?;
    Ok(mvcc
        .runtime
        .scan_table_prefix_at(
            TABLE_AUTHZ_NAMESPACE_WATCH,
            &watch_prefix(tenant_id, namespace)?,
            snapshot,
        )?
        .into_iter()
        .map(|(_, row)| u128::from(row.commit_version))
        .max())
}

fn watch_prefix(tenant_id: i64, namespace: &str) -> Result<Vec<u8>> {
    require_nonempty(namespace, "namespace")?;
    let mut key = Vec::new();
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&(namespace.len() as u32).to_be_bytes());
    key.extend_from_slice(namespace.as_bytes());
    Ok(key)
}

fn watch_key(tenant_id: i64, namespace: &str, mutation_id: [u8; 16]) -> Result<LogicalKey> {
    let mut application_key = watch_prefix(tenant_id, namespace)?;
    application_key.extend_from_slice(&mutation_id);
    Ok(LogicalKey {
        table_id: TABLE_AUTHZ_NAMESPACE_WATCH,
        application_key,
    })
}

fn now_unix_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn encode_authz_namespace_watch_payload(payload: &AuthzNamespaceWatchPayload) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(
        &AuthzNamespaceWatchPayloadProto {
            namespace: payload.namespace.clone(),
            event_type: payload.event_type.clone(),
            authz_revision: payload.authz_revision,
            schema_hash: payload.schema_hash.clone(),
            invalidates_derived_usersets: payload.invalidates_derived_usersets,
            emitted_at: payload.emitted_at.clone(),
        },
    ))
}

fn decode_authz_namespace_watch_payload(bytes: &[u8]) -> Result<AuthzNamespaceWatchPayload> {
    let proto = decode_deterministic_proto::<AuthzNamespaceWatchPayloadProto>(
        bytes,
        "AuthzNamespaceWatchPayload payload",
    )?;
    Ok(AuthzNamespaceWatchPayload {
        namespace: proto.namespace,
        event_type: proto.event_type,
        authz_revision: proto.authz_revision,
        schema_hash: proto.schema_hash,
        invalidates_derived_usersets: proto.invalidates_derived_usersets,
        emitted_at: proto.emitted_at,
    })
}

fn validate_payload(payload: &AuthzNamespaceWatchPayload) -> Result<()> {
    require_nonempty(&payload.namespace, "namespace")?;
    require_nonempty(&payload.event_type, "event_type")?;
    if payload.authz_revision == 0 {
        return Err(anyhow!(
            "authorization namespace watch revision must be nonzero"
        ));
    }
    validate_hex32(&payload.schema_hash, "schema_hash")?;
    require_nonempty(&payload.emitted_at, "emitted_at")?;
    Ok(())
}

fn partition_id(tenant_id: i64, namespace: &str) -> Hash32 {
    hash32(format!("tenant:{tenant_id}:authz-namespace:{namespace}").as_bytes())
}

pub(crate) fn authz_namespace_watch_stream_id(tenant_id: i64, namespace: &str) -> String {
    format!("watch:authz_namespace:tenant:{tenant_id}:namespace:{namespace}")
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mvcc_watch_keys_scope_tenant_namespace_and_mutation() {
        let first = watch_key(5, "document", [1; 16]).unwrap();
        assert_eq!(first.table_id, TABLE_AUTHZ_NAMESPACE_WATCH);
        assert_ne!(first, watch_key(6, "document", [1; 16]).unwrap());
        assert_ne!(first, watch_key(5, "folder", [1; 16]).unwrap());
        assert_ne!(first, watch_key(5, "document", [2; 16]).unwrap());
        assert!(watch_prefix(5, "").is_err());
    }
}
