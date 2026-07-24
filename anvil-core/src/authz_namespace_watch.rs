use crate::{
    core_store::{
        AppendStreamRecord, CoreStore, ReadStream, decode_deterministic_proto,
        encode_deterministic_proto,
    },
    formats::{Hash32, hash32, watch::WatchRecord},
    storage::Storage,
};
use anyhow::{Result, anyhow};
use prost::Message;
use serde::{Deserialize, Serialize};

const AUTHZ_NAMESPACE_PARTITION_FAMILY: u16 = 9;
const AUTHZ_NAMESPACE_RECORD_KIND: u16 = 1;

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
    storage: &Storage,
    tenant_id: i64,
    mutation_id: [u8; 16],
    payload: AuthzNamespaceWatchPayload,
) -> Result<u128> {
    validate_payload(&payload)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = authz_namespace_watch_stream_id(tenant_id, &payload.namespace);

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
    let receipt = core_store
        .append_stream(AppendStreamRecord {
            stream_id,
            partition_id: hex::encode(partition_id(tenant_id, &payload.namespace)),
            record_kind: "authz_namespace_watch".to_string(),
            payload: record.encode(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!(
                "authz-namespace-watch:{tenant_id}:{}:{}",
                payload.namespace,
                hex::encode(mutation_id)
            )),
        })
        .await?;
    Ok(u128::from(receipt.sequence))
}

pub async fn list_authz_namespace_watch_events(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<AuthzNamespaceWatchEvent>> {
    Ok(
        list_authz_namespace_watch_event_page(storage, tenant_id, namespace, after_cursor, limit)
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
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<AuthzNamespaceWatchEventPage> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let after_sequence = u64::try_from(after_cursor)
        .map_err(|_| anyhow!("authorization namespace watch cursor exceeds u64"))?;
    let page = core_store
        .read_stream_page(ReadStream {
            stream_id: authz_namespace_watch_stream_id(tenant_id, namespace),
            after_sequence,
            limit,
        })
        .await?;
    let expected_partition = partition_id(tenant_id, namespace);
    let mut events = Vec::with_capacity(page.records.len());
    for source in page.records {
        if source.record_kind != "authz_namespace_watch" {
            return Err(anyhow!(
                "authorization namespace watch stream record kind mismatch"
            ));
        }
        let (mut record, used) = WatchRecord::decode(&source.payload)?;
        if used != source.payload.len() {
            return Err(anyhow!(
                "authorization namespace watch record has trailing bytes"
            ));
        }
        record.cursor = u128::from(source.sequence);
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
    Ok(AuthzNamespaceWatchEventPage {
        events,
        next_cursor: u128::from(page.next_sequence),
        has_more: page.has_more,
    })
}

pub async fn latest_authz_namespace_watch_cursor(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
) -> Result<Option<u128>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let sequence = core_store
        .stream_head_sequence(&authz_namespace_watch_stream_id(tenant_id, namespace))
        .await?;
    Ok((sequence != 0).then_some(u128::from(sequence)))
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
    use crate::storage::Storage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn authz_namespace_watch_appends_lists_and_tracks_latest() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_authz_namespace_watch_record(&storage, 5, [1; 16], payload(10))
            .await
            .unwrap();
        append_authz_namespace_watch_record(&storage, 5, [2; 16], payload(11))
            .await
            .unwrap();
        assert_eq!(
            authz_namespace_watch_stream_id(5, "document"),
            "watch:authz_namespace:tenant:5:namespace:document"
        );

        let events = list_authz_namespace_watch_events(&storage, 5, "document", 1, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, 2);
        assert_eq!(events[0].authz_revision, 11);
        assert!(events[0].payload.invalidates_derived_usersets);
        assert_eq!(
            latest_authz_namespace_watch_cursor(&storage, 5, "document")
                .await
                .unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn authz_namespace_watch_rejects_invalid_payloads_and_idempotency_conflicts() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_authz_namespace_watch_record(&storage, 5, [1; 16], payload(10))
            .await
            .unwrap();
        assert!(
            append_authz_namespace_watch_record(&storage, 5, [1; 16], payload(11))
                .await
                .is_err()
        );
        let mut invalid = payload(12);
        invalid.schema_hash = "not-hex".to_string();
        assert!(
            append_authz_namespace_watch_record(&storage, 5, [3; 16], invalid)
                .await
                .is_err()
        );
        let mut invalid = payload(13);
        invalid.namespace = "../escape".to_string();
        assert!(
            append_authz_namespace_watch_record(&storage, 5, [4; 16], invalid)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn authz_namespace_watch_requires_a_bounded_page_limit() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for cursor in 1..=3 {
            append_authz_namespace_watch_record(
                &storage,
                5,
                [cursor as u8; 16],
                payload(9 + cursor as u64),
            )
            .await
            .unwrap();
        }
        let error = list_authz_namespace_watch_events(&storage, 5, "document", 0, 0)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("limit"));
    }

    fn payload(authz_revision: u64) -> AuthzNamespaceWatchPayload {
        AuthzNamespaceWatchPayload {
            namespace: "document".to_string(),
            event_type: "schema_changed".to_string(),
            authz_revision,
            schema_hash: hex::encode([4; 32]),
            invalidates_derived_usersets: true,
            emitted_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        }
    }
}
