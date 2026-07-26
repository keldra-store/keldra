use crate::{
    core_store::{decode_deterministic_proto, encode_deterministic_proto},
    formats::{Hash32, hash32, watch::WatchRecord},
    mvcc_bootstrap::MvccSubsystem,
    mvcc_product::ProductMutation,
    mvcc_transaction::{DurabilityLevel, LogicalKey, PredicateKind, ReadConsistency},
};
use anyhow::{Result, anyhow, bail};
use prost::Message;
use serde::{Deserialize, Serialize};

const AUTHZ_DERIVED_LAG_PARTITION_FAMILY: u16 = 8;
const AUTHZ_DERIVED_LAG_RECORD_KIND: u16 = 1;
const TABLE_AUTHZ_DERIVED_LAG_WATCH: u16 = 0x050a;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzDerivedLagWatchPayload {
    pub derived_index_id: String,
    pub derived_index_kind: String,
    pub processed_revision: u64,
    pub latest_revision: u64,
    pub source_cursor: u128,
    pub source_manifest_hash: String,
    pub generation: u64,
    pub emitted_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzDerivedLagWatchPayloadProto {
    #[prost(string, tag = "1")]
    derived_index_id: String,
    #[prost(string, tag = "2")]
    derived_index_kind: String,
    #[prost(uint64, tag = "3")]
    processed_revision: u64,
    #[prost(uint64, tag = "4")]
    latest_revision: u64,
    #[prost(string, tag = "5")]
    source_cursor: String,
    #[prost(string, tag = "6")]
    source_manifest_hash: String,
    #[prost(uint64, tag = "7")]
    generation: u64,
    #[prost(string, tag = "8")]
    emitted_at: String,
}

impl AuthzDerivedLagWatchPayload {
    pub fn revision_lag(&self) -> u64 {
        self.latest_revision.saturating_sub(self.processed_revision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzDerivedLagWatchEvent {
    pub cursor: u128,
    pub mutation_id: [u8; 16],
    pub authz_revision: u64,
    pub index_generation: u64,
    pub payload: AuthzDerivedLagWatchPayload,
}

pub async fn append_authz_derived_lag_watch_record(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    mutation_id: [u8; 16],
    payload: AuthzDerivedLagWatchPayload,
) -> Result<u128> {
    validate_payload(&payload)?;
    let prepared = prepare_lag_watch_record(tenant_id, mutation_id, &payload);
    let key = watch_key(tenant_id, &payload.derived_index_id, mutation_id)?;
    Ok(u128::from(
        mvcc.autocommit_product_mutations_with_predicates(
            &format!("tenant:{tenant_id}:authz-derived-lag"),
            &prepared.idempotency_key,
            vec![ProductMutation::put(key.clone(), prepared.record_payload)],
            vec![(key, PredicateKind::Unique)],
            DurabilityLevel::Quorum,
            now_unix_ms(),
        )
        .await?,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedLagWatchRecord {
    idempotency_key: String,
    record_payload: Vec<u8>,
}

fn prepare_lag_watch_record(
    tenant_id: i64,
    mutation_id: [u8; 16],
    payload: &AuthzDerivedLagWatchPayload,
) -> PreparedLagWatchRecord {
    let partition = partition_id(tenant_id, &payload.derived_index_id);
    let record = WatchRecord::new(
        0,
        AUTHZ_DERIVED_LAG_PARTITION_FAMILY,
        partition,
        mutation_id,
        AUTHZ_DERIVED_LAG_RECORD_KIND,
        payload.latest_revision,
        payload.generation,
        0,
        encode_lag_watch_payload(payload),
    );
    PreparedLagWatchRecord {
        idempotency_key: format!(
            "authz-derived-lag-watch:{tenant_id}:{}:{}",
            payload.derived_index_id,
            hex::encode(mutation_id)
        ),
        record_payload: record.encode(),
    }
}

pub async fn list_authz_derived_lag_watch_events(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<AuthzDerivedLagWatchEvent>> {
    Ok(list_authz_derived_lag_watch_event_page(
        mvcc,
        tenant_id,
        derived_index_id,
        after_cursor,
        limit,
    )
    .await?
    .events)
}

#[derive(Debug, Clone)]
pub struct AuthzDerivedLagWatchEventPage {
    pub events: Vec<AuthzDerivedLagWatchEvent>,
    pub next_cursor: u128,
    pub has_more: bool,
}

pub async fn list_authz_derived_lag_watch_event_page(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<AuthzDerivedLagWatchEventPage> {
    if limit == 0 {
        bail!("authz derived lag watch limit must be nonzero");
    }
    let after_version = u64::try_from(after_cursor)
        .map_err(|_| anyhow!("authz derived lag watch cursor exceeds u64"))?;
    let snapshot = mvcc.runtime.snapshot(ReadConsistency::Linearized).await?;
    let mut page = mvcc.runtime.scan_table_prefix_at(
        TABLE_AUTHZ_DERIVED_LAG_WATCH,
        &watch_prefix(tenant_id, derived_index_id)?,
        snapshot,
    )?;
    page.retain(|(_, row)| row.commit_version > after_version);
    page.sort_by_key(|(_, row)| row.commit_version);
    let has_more = page.len() > limit;
    page.truncate(limit);
    let expected_partition = partition_id(tenant_id, derived_index_id);
    let mut events = Vec::with_capacity(page.len());
    for (_, source) in page {
        let (mut record, used) = WatchRecord::decode(&source.value)?;
        if used != source.value.len() {
            return Err(anyhow!(
                "authz derived lag watch MVCC record has trailing bytes"
            ));
        }
        record.cursor = u128::from(source.commit_version);
        if record.partition_family != AUTHZ_DERIVED_LAG_PARTITION_FAMILY
            || record.record_kind != AUTHZ_DERIVED_LAG_RECORD_KIND
            || record.partition_id != expected_partition
        {
            return Err(anyhow!("authz derived lag watch record scope mismatch"));
        }
        let payload = decode_lag_watch_payload(&record.payload)?;
        if payload.derived_index_id != derived_index_id {
            return Err(anyhow!("authz derived lag watch payload scope mismatch"));
        }
        validate_payload(&payload)?;
        events.push(AuthzDerivedLagWatchEvent {
            cursor: record.cursor,
            mutation_id: record.mutation_id,
            authz_revision: record.authz_revision,
            index_generation: record.index_generation,
            payload,
        });
    }
    let next_cursor = events
        .last()
        .map(|event| event.cursor)
        .unwrap_or(after_cursor);
    Ok(AuthzDerivedLagWatchEventPage {
        events,
        next_cursor,
        has_more,
    })
}

pub async fn latest_authz_derived_lag_watch_event(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
) -> Result<Option<AuthzDerivedLagWatchEvent>> {
    let snapshot = mvcc.runtime.snapshot(ReadConsistency::Linearized).await?;
    let mut rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_AUTHZ_DERIVED_LAG_WATCH,
        &watch_prefix(tenant_id, derived_index_id)?,
        snapshot,
    )?;
    rows.sort_by_key(|(_, row)| row.commit_version);
    let Some((_, row)) = rows.pop() else {
        return Ok(None);
    };
    decode_event(tenant_id, derived_index_id, row.commit_version, &row.value).map(Some)
}

fn watch_prefix(tenant_id: i64, derived_index_id: &str) -> Result<Vec<u8>> {
    require_safe_component(derived_index_id, "derived_index_id")?;
    let mut key = Vec::new();
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&(derived_index_id.len() as u32).to_be_bytes());
    key.extend_from_slice(derived_index_id.as_bytes());
    Ok(key)
}

fn watch_key(tenant_id: i64, derived_index_id: &str, mutation_id: [u8; 16]) -> Result<LogicalKey> {
    let mut application_key = watch_prefix(tenant_id, derived_index_id)?;
    application_key.extend_from_slice(&mutation_id);
    Ok(LogicalKey {
        table_id: TABLE_AUTHZ_DERIVED_LAG_WATCH,
        application_key,
    })
}

fn decode_event(
    tenant_id: i64,
    derived_index_id: &str,
    commit_version: u64,
    bytes: &[u8],
) -> Result<AuthzDerivedLagWatchEvent> {
    let (mut record, used) = WatchRecord::decode(bytes)?;
    if used != bytes.len()
        || record.partition_family != AUTHZ_DERIVED_LAG_PARTITION_FAMILY
        || record.record_kind != AUTHZ_DERIVED_LAG_RECORD_KIND
        || record.partition_id != partition_id(tenant_id, derived_index_id)
    {
        bail!("authz derived lag watch record scope mismatch");
    }
    record.cursor = u128::from(commit_version);
    let payload = decode_lag_watch_payload(&record.payload)?;
    if payload.derived_index_id != derived_index_id {
        bail!("authz derived lag watch payload scope mismatch");
    }
    validate_payload(&payload)?;
    Ok(AuthzDerivedLagWatchEvent {
        cursor: record.cursor,
        mutation_id: record.mutation_id,
        authz_revision: record.authz_revision,
        index_generation: record.index_generation,
        payload,
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

fn encode_lag_watch_payload(payload: &AuthzDerivedLagWatchPayload) -> Vec<u8> {
    encode_deterministic_proto(&AuthzDerivedLagWatchPayloadProto {
        derived_index_id: payload.derived_index_id.clone(),
        derived_index_kind: payload.derived_index_kind.clone(),
        processed_revision: payload.processed_revision,
        latest_revision: payload.latest_revision,
        source_cursor: payload.source_cursor.to_string(),
        source_manifest_hash: payload.source_manifest_hash.clone(),
        generation: payload.generation,
        emitted_at: payload.emitted_at.clone(),
    })
}

fn decode_lag_watch_payload(bytes: &[u8]) -> Result<AuthzDerivedLagWatchPayload> {
    let proto = decode_deterministic_proto::<AuthzDerivedLagWatchPayloadProto>(
        bytes,
        "authorization derived lag watch payload",
    )?;
    Ok(AuthzDerivedLagWatchPayload {
        derived_index_id: proto.derived_index_id,
        derived_index_kind: proto.derived_index_kind,
        processed_revision: proto.processed_revision,
        latest_revision: proto.latest_revision,
        source_cursor: proto
            .source_cursor
            .parse()
            .map_err(|_| anyhow!("authorization derived lag source_cursor is not u128"))?,
        source_manifest_hash: proto.source_manifest_hash,
        generation: proto.generation,
        emitted_at: proto.emitted_at,
    })
}

fn validate_payload(payload: &AuthzDerivedLagWatchPayload) -> Result<()> {
    require_safe_component(&payload.derived_index_id, "derived_index_id")?;
    require_safe_component(&payload.derived_index_kind, "derived_index_kind")?;
    validate_hex32(&payload.source_manifest_hash, "source_manifest_hash")?;
    if payload.generation == 0 {
        return Err(anyhow!(
            "authorization derived lag generation must be nonzero"
        ));
    }
    if payload.processed_revision > payload.latest_revision {
        return Err(anyhow!(
            "authorization derived lag processed revision is after latest revision"
        ));
    }
    require_nonempty(&payload.emitted_at, "emitted_at")?;
    Ok(())
}

fn partition_id(tenant_id: i64, derived_index_id: &str) -> Hash32 {
    hash32(format!("tenant:{tenant_id}:authz-derived-lag:{derived_index_id}").as_bytes())
}

pub(crate) fn authz_derived_lag_watch_stream_id(tenant_id: i64, derived_index_id: &str) -> String {
    format!("watch:authz_derived_lag:tenant:{tenant_id}:derived:{derived_index_id}")
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

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    require_nonempty(value, field)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("{field} is not a safe component"));
    }
    Ok(())
}
