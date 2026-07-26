use crate::{
    core_store::{decode_deterministic_proto, encode_deterministic_proto},
    formats::{Hash32, hash32, watch::WatchRecord},
    mvcc_bootstrap::MvccSubsystem,
    mvcc_product::ProductMutation,
    mvcc_transaction::{DurabilityLevel, LogicalKey, PredicateKind},
};
use anyhow::{Result, anyhow};
use prost::Message;
use serde::{Deserialize, Serialize};

const PERSONALDB_GROUP_PARTITION_FAMILY: u16 = 4;
const PERSONALDB_GROUP_RECORD_KIND: u16 = 1;
const PERSONALDB_PROJECTION_PARTITION_FAMILY: u16 = 5;
const PERSONALDB_PROJECTION_RECORD_KIND: u16 = 1;
const TABLE_PERSONALDB_GROUP_WATCH: u16 = 0x0607;
const TABLE_PERSONALDB_PROJECTION_WATCH: u16 = 0x0608;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbGroupWatchPayload {
    pub database_id: String,
    pub event_type: String,
    pub log_index: u64,
    pub log_hash: String,
    pub changeset_payload_hash: String,
    pub certificate_hash: String,
    pub committed_head_hash: String,
    pub emitted_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbGroupWatchPayloadProto {
    #[prost(string, tag = "1")]
    database_id: String,
    #[prost(string, tag = "2")]
    event_type: String,
    #[prost(uint64, tag = "3")]
    log_index: u64,
    #[prost(string, tag = "4")]
    log_hash: String,
    #[prost(string, tag = "5")]
    changeset_payload_hash: String,
    #[prost(string, tag = "6")]
    certificate_hash: String,
    #[prost(string, tag = "7")]
    committed_head_hash: String,
    #[prost(string, tag = "8")]
    emitted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbGroupWatchEvent {
    pub cursor: u128,
    pub mutation_id: [u8; 16],
    pub authz_revision: u64,
    pub payload: PersonalDbGroupWatchPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbProjectionWatchPayload {
    pub database_id: String,
    pub projection_id: String,
    pub event_type: String,
    pub source_database_id: String,
    pub source_log_index: u64,
    pub source_log_hash: String,
    pub projection_log_index: u64,
    pub projection_log_hash: String,
    pub definition_hash: String,
    pub emitted_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbProjectionWatchPayloadProto {
    #[prost(string, tag = "1")]
    database_id: String,
    #[prost(string, tag = "2")]
    projection_id: String,
    #[prost(string, tag = "3")]
    event_type: String,
    #[prost(string, tag = "4")]
    source_database_id: String,
    #[prost(uint64, tag = "5")]
    source_log_index: u64,
    #[prost(string, tag = "6")]
    source_log_hash: String,
    #[prost(uint64, tag = "7")]
    projection_log_index: u64,
    #[prost(string, tag = "8")]
    projection_log_hash: String,
    #[prost(string, tag = "9")]
    definition_hash: String,
    #[prost(string, tag = "10")]
    emitted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbProjectionWatchEvent {
    pub cursor: u128,
    pub mutation_id: [u8; 16],
    pub authz_revision: u64,
    pub payload: PersonalDbProjectionWatchPayload,
}

pub async fn append_personaldb_group_watch_record(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    mutation_id: [u8; 16],
    authz_revision: u64,
    payload: PersonalDbGroupWatchPayload,
) -> Result<u128> {
    validate_payload(database_id, &payload)?;
    let key = group_watch_key(tenant_id, database_id, mutation_id)?;
    let record = WatchRecord::new(
        0,
        PERSONALDB_GROUP_PARTITION_FAMILY,
        partition_id(tenant_id, database_id),
        mutation_id,
        PERSONALDB_GROUP_RECORD_KIND,
        authz_revision,
        0,
        payload.log_index,
        encode_group_watch_payload(&payload),
    );
    let committed = mvcc
        .autocommit_product_mutations_with_predicates(
            "system/personaldb-group-watch",
            &format!(
                "personaldb-group-watch:{tenant_id}:{database_id}:{}",
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

pub async fn append_personaldb_projection_watch_record(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
    mutation_id: [u8; 16],
    authz_revision: u64,
    payload: PersonalDbProjectionWatchPayload,
) -> Result<u128> {
    validate_projection_payload(database_id, projection_id, &payload)?;
    let key = projection_watch_key(tenant_id, database_id, projection_id, mutation_id)?;
    let record = WatchRecord::new(
        0,
        PERSONALDB_PROJECTION_PARTITION_FAMILY,
        projection_partition_id(tenant_id, database_id, projection_id),
        mutation_id,
        PERSONALDB_PROJECTION_RECORD_KIND,
        authz_revision,
        0,
        payload.projection_log_index,
        encode_projection_watch_payload(&payload),
    );
    let committed = mvcc
        .autocommit_product_mutations_with_predicates(
            "system/personaldb-projection-watch",
            &format!(
                "personaldb-projection-watch:{tenant_id}:{database_id}:{projection_id}:{}",
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

pub async fn list_personaldb_group_watch_events(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    after_cursor: u128,
    limit: usize,
    snapshot_version: u64,
) -> Result<Vec<PersonalDbGroupWatchEvent>> {
    Ok(list_personaldb_group_watch_event_page(
        mvcc,
        tenant_id,
        database_id,
        after_cursor,
        limit,
        snapshot_version,
    )
    .await?
    .events)
}

#[derive(Debug, Clone)]
pub struct PersonalDbGroupWatchEventPage {
    pub events: Vec<PersonalDbGroupWatchEvent>,
    pub next_cursor: u128,
    pub has_more: bool,
}

pub async fn list_personaldb_group_watch_event_page(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    after_cursor: u128,
    limit: usize,
    snapshot_version: u64,
) -> Result<PersonalDbGroupWatchEventPage> {
    let (records, next_cursor, has_more) = read_watch_page(
        mvcc,
        TABLE_PERSONALDB_GROUP_WATCH,
        &group_watch_prefix(tenant_id, database_id)?,
        after_cursor,
        limit,
        snapshot_version,
    )?;
    let mut events = Vec::with_capacity(records.len());
    for record in records {
        if record.partition_family != PERSONALDB_GROUP_PARTITION_FAMILY
            || record.record_kind != PERSONALDB_GROUP_RECORD_KIND
            || record.partition_id != partition_id(tenant_id, database_id)
        {
            return Err(anyhow!("personaldb group watch record scope mismatch"));
        }
        let payload = decode_group_watch_payload(&record.payload)?;
        validate_payload(database_id, &payload)?;
        events.push(PersonalDbGroupWatchEvent {
            cursor: record.cursor,
            mutation_id: record.mutation_id,
            authz_revision: record.authz_revision,
            payload,
        });
    }
    Ok(PersonalDbGroupWatchEventPage {
        events,
        next_cursor,
        has_more,
    })
}

pub async fn list_personaldb_projection_watch_events(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
    after_cursor: u128,
    limit: usize,
    snapshot_version: u64,
) -> Result<Vec<PersonalDbProjectionWatchEvent>> {
    Ok(list_personaldb_projection_watch_event_page(
        mvcc,
        tenant_id,
        database_id,
        projection_id,
        after_cursor,
        limit,
        snapshot_version,
    )
    .await?
    .events)
}

#[derive(Debug, Clone)]
pub struct PersonalDbProjectionWatchEventPage {
    pub events: Vec<PersonalDbProjectionWatchEvent>,
    pub next_cursor: u128,
    pub has_more: bool,
}

pub async fn list_personaldb_projection_watch_event_page(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
    after_cursor: u128,
    limit: usize,
    snapshot_version: u64,
) -> Result<PersonalDbProjectionWatchEventPage> {
    let (records, next_cursor, has_more) = read_watch_page(
        mvcc,
        TABLE_PERSONALDB_PROJECTION_WATCH,
        &projection_watch_prefix(tenant_id, database_id, projection_id)?,
        after_cursor,
        limit,
        snapshot_version,
    )?;
    let mut events = Vec::with_capacity(records.len());
    for record in records {
        if record.partition_family != PERSONALDB_PROJECTION_PARTITION_FAMILY
            || record.record_kind != PERSONALDB_PROJECTION_RECORD_KIND
            || record.partition_id != projection_partition_id(tenant_id, database_id, projection_id)
        {
            return Err(anyhow!("personaldb projection watch record scope mismatch"));
        }
        let payload = decode_projection_watch_payload(&record.payload)?;
        validate_projection_payload(database_id, projection_id, &payload)?;
        events.push(PersonalDbProjectionWatchEvent {
            cursor: record.cursor,
            mutation_id: record.mutation_id,
            authz_revision: record.authz_revision,
            payload,
        });
    }
    Ok(PersonalDbProjectionWatchEventPage {
        events,
        next_cursor,
        has_more,
    })
}

pub async fn latest_personaldb_group_watch_cursor(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    snapshot_version: u64,
) -> Result<Option<u128>> {
    latest_watch_cursor(
        mvcc,
        TABLE_PERSONALDB_GROUP_WATCH,
        &group_watch_prefix(tenant_id, database_id)?,
        snapshot_version,
    )
}

pub async fn latest_personaldb_projection_watch_cursor(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
    snapshot_version: u64,
) -> Result<Option<u128>> {
    latest_watch_cursor(
        mvcc,
        TABLE_PERSONALDB_PROJECTION_WATCH,
        &projection_watch_prefix(tenant_id, database_id, projection_id)?,
        snapshot_version,
    )
}

fn read_watch_page(
    mvcc: &MvccSubsystem,
    table_id: u16,
    prefix: &[u8],
    after_cursor: u128,
    limit: usize,
    snapshot_version: u64,
) -> Result<(Vec<WatchRecord>, u128, bool)> {
    if limit == 0 {
        return Err(anyhow!("personaldb watch limit must be nonzero"));
    }
    let after_version =
        u64::try_from(after_cursor).map_err(|_| anyhow!("personaldb watch cursor exceeds u64"))?;
    let mut page = mvcc
        .runtime
        .scan_table_prefix_at(table_id, prefix, snapshot_version)?;
    page.retain(|(_, row)| row.commit_version > after_version);
    page.sort_by_key(|(_, row)| row.commit_version);
    let has_more = page.len() > limit;
    page.truncate(limit);
    let mut records = Vec::with_capacity(page.len());
    for (_, source) in page {
        let (mut record, used) = WatchRecord::decode(&source.value)?;
        if used != source.value.len() {
            return Err(anyhow!("personaldb watch record has trailing bytes"));
        }
        record.cursor = u128::from(source.commit_version);
        records.push(record);
    }
    let next_cursor = records
        .last()
        .map(|record| record.cursor)
        .unwrap_or(after_cursor);
    Ok((records, next_cursor, has_more))
}

fn latest_watch_cursor(
    mvcc: &MvccSubsystem,
    table_id: u16,
    prefix: &[u8],
    snapshot_version: u64,
) -> Result<Option<u128>> {
    Ok(mvcc
        .runtime
        .scan_table_prefix_at(table_id, prefix, snapshot_version)?
        .into_iter()
        .map(|(_, row)| u128::from(row.commit_version))
        .max())
}

fn group_watch_prefix(tenant_id: i64, database_id: &str) -> Result<Vec<u8>> {
    scoped_watch_prefix(tenant_id, database_id, None)
}

fn projection_watch_prefix(
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
) -> Result<Vec<u8>> {
    scoped_watch_prefix(tenant_id, database_id, Some(projection_id))
}

fn scoped_watch_prefix(
    tenant_id: i64,
    database_id: &str,
    projection_id: Option<&str>,
) -> Result<Vec<u8>> {
    if tenant_id < 0 || database_id.is_empty() || projection_id.is_some_and(str::is_empty) {
        return Err(anyhow!("personaldb watch scope is invalid"));
    }
    let mut key = Vec::new();
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&(database_id.len() as u32).to_be_bytes());
    key.extend_from_slice(database_id.as_bytes());
    if let Some(projection_id) = projection_id {
        key.extend_from_slice(&(projection_id.len() as u32).to_be_bytes());
        key.extend_from_slice(projection_id.as_bytes());
    }
    Ok(key)
}

fn group_watch_key(tenant_id: i64, database_id: &str, mutation_id: [u8; 16]) -> Result<LogicalKey> {
    let mut application_key = group_watch_prefix(tenant_id, database_id)?;
    application_key.extend_from_slice(&mutation_id);
    Ok(LogicalKey {
        table_id: TABLE_PERSONALDB_GROUP_WATCH,
        application_key,
    })
}

fn projection_watch_key(
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
    mutation_id: [u8; 16],
) -> Result<LogicalKey> {
    let mut application_key = projection_watch_prefix(tenant_id, database_id, projection_id)?;
    application_key.extend_from_slice(&mutation_id);
    Ok(LogicalKey {
        table_id: TABLE_PERSONALDB_PROJECTION_WATCH,
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

fn validate_payload(database_id: &str, payload: &PersonalDbGroupWatchPayload) -> Result<()> {
    if payload.database_id != database_id {
        return Err(anyhow!("personaldb watch payload database mismatch"));
    }
    if payload.event_type.is_empty() || payload.emitted_at.is_empty() {
        return Err(anyhow!("personaldb watch payload is incomplete"));
    }
    validate_hex32(&payload.log_hash, "log_hash")?;
    validate_hex32(&payload.changeset_payload_hash, "changeset_payload_hash")?;
    validate_hex32(&payload.certificate_hash, "certificate_hash")?;
    validate_hex32(&payload.committed_head_hash, "committed_head_hash")?;
    Ok(())
}

fn validate_projection_payload(
    database_id: &str,
    projection_id: &str,
    payload: &PersonalDbProjectionWatchPayload,
) -> Result<()> {
    if payload.database_id != database_id || payload.projection_id != projection_id {
        return Err(anyhow!(
            "personaldb projection watch payload scope mismatch"
        ));
    }
    if payload.event_type.is_empty()
        || payload.source_database_id.is_empty()
        || payload.emitted_at.is_empty()
    {
        return Err(anyhow!("personaldb projection watch payload is incomplete"));
    }
    validate_hex32(&payload.source_log_hash, "source_log_hash")?;
    validate_hex32(&payload.projection_log_hash, "projection_log_hash")?;
    validate_hex32(&payload.definition_hash, "definition_hash")?;
    Ok(())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn encode_group_watch_payload(payload: &PersonalDbGroupWatchPayload) -> Vec<u8> {
    encode_deterministic_proto(&PersonalDbGroupWatchPayloadProto {
        database_id: payload.database_id.clone(),
        event_type: payload.event_type.clone(),
        log_index: payload.log_index,
        log_hash: payload.log_hash.clone(),
        changeset_payload_hash: payload.changeset_payload_hash.clone(),
        certificate_hash: payload.certificate_hash.clone(),
        committed_head_hash: payload.committed_head_hash.clone(),
        emitted_at: payload.emitted_at.clone(),
    })
}

fn decode_group_watch_payload(bytes: &[u8]) -> Result<PersonalDbGroupWatchPayload> {
    let proto = decode_deterministic_proto::<PersonalDbGroupWatchPayloadProto>(
        bytes,
        "personaldb group watch payload",
    )?;
    Ok(PersonalDbGroupWatchPayload {
        database_id: proto.database_id,
        event_type: proto.event_type,
        log_index: proto.log_index,
        log_hash: proto.log_hash,
        changeset_payload_hash: proto.changeset_payload_hash,
        certificate_hash: proto.certificate_hash,
        committed_head_hash: proto.committed_head_hash,
        emitted_at: proto.emitted_at,
    })
}

fn encode_projection_watch_payload(payload: &PersonalDbProjectionWatchPayload) -> Vec<u8> {
    encode_deterministic_proto(&PersonalDbProjectionWatchPayloadProto {
        database_id: payload.database_id.clone(),
        projection_id: payload.projection_id.clone(),
        event_type: payload.event_type.clone(),
        source_database_id: payload.source_database_id.clone(),
        source_log_index: payload.source_log_index,
        source_log_hash: payload.source_log_hash.clone(),
        projection_log_index: payload.projection_log_index,
        projection_log_hash: payload.projection_log_hash.clone(),
        definition_hash: payload.definition_hash.clone(),
        emitted_at: payload.emitted_at.clone(),
    })
}

fn decode_projection_watch_payload(bytes: &[u8]) -> Result<PersonalDbProjectionWatchPayload> {
    let proto = decode_deterministic_proto::<PersonalDbProjectionWatchPayloadProto>(
        bytes,
        "personaldb projection watch payload",
    )?;
    Ok(PersonalDbProjectionWatchPayload {
        database_id: proto.database_id,
        projection_id: proto.projection_id,
        event_type: proto.event_type,
        source_database_id: proto.source_database_id,
        source_log_index: proto.source_log_index,
        source_log_hash: proto.source_log_hash,
        projection_log_index: proto.projection_log_index,
        projection_log_hash: proto.projection_log_hash,
        definition_hash: proto.definition_hash,
        emitted_at: proto.emitted_at,
    })
}

fn partition_id(tenant_id: i64, database_id: &str) -> Hash32 {
    hash32(format!("tenant:{tenant_id}:personaldb:{database_id}:watch:group").as_bytes())
}

fn projection_partition_id(tenant_id: i64, database_id: &str, projection_id: &str) -> Hash32 {
    hash32(
        format!("tenant:{tenant_id}:personaldb:{database_id}:projection:{projection_id}:watch")
            .as_bytes(),
    )
}

#[cfg(any())]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn personaldb_group_watch_appends_lists_and_tracks_latest_cursor() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_personaldb_group_watch_record(&storage, 4, "db-alpha", [1; 16], 7, payload(1))
            .await
            .unwrap();
        append_personaldb_group_watch_record(&storage, 4, "db-alpha", [2; 16], 8, payload(2))
            .await
            .unwrap();

        assert_eq!(
            personaldb_group_watch_stream_id(4, "db-alpha"),
            "watch:personaldb_group:tenant:4:database:db-alpha"
        );
        let events = list_personaldb_group_watch_events(&storage, 4, "db-alpha", 1, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, 2);
        assert_eq!(events[0].authz_revision, 8);
        assert_eq!(events[0].payload.log_index, 2);
        assert_eq!(
            latest_personaldb_group_watch_cursor(&storage, 4, "db-alpha")
                .await
                .unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn personaldb_group_watch_rejects_idempotency_conflicts_and_bad_payload() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_personaldb_group_watch_record(&storage, 4, "db-alpha", [1; 16], 7, payload(1))
            .await
            .unwrap();
        assert!(
            append_personaldb_group_watch_record(&storage, 4, "db-alpha", [1; 16], 7, payload(2),)
                .await
                .is_err()
        );

        let mut bad = payload(3);
        bad.database_id = "db-beta".to_string();
        assert!(
            append_personaldb_group_watch_record(&storage, 4, "db-alpha", [3; 16], 7, bad)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn personaldb_projection_watch_appends_lists_and_tracks_latest_cursor() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_personaldb_projection_watch_record(
            &storage,
            4,
            "projection-db",
            "projection-a",
            [1; 16],
            9,
            projection_payload(1),
        )
        .await
        .unwrap();
        append_personaldb_projection_watch_record(
            &storage,
            4,
            "projection-db",
            "projection-a",
            [2; 16],
            10,
            projection_payload(2),
        )
        .await
        .unwrap();

        assert_eq!(
            personaldb_projection_watch_stream_id(4, "projection-db", "projection-a"),
            "watch:personaldb_projection:tenant:4:database:projection-db:projection:projection-a"
        );
        let events = list_personaldb_projection_watch_events(
            &storage,
            4,
            "projection-db",
            "projection-a",
            1,
            10,
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, 2);
        assert_eq!(events[0].authz_revision, 10);
        assert_eq!(events[0].payload.projection_log_index, 2);
        assert_eq!(
            latest_personaldb_projection_watch_cursor(&storage, 4, "projection-db", "projection-a")
                .await
                .unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn personaldb_projection_watch_rejects_idempotency_conflicts_and_bad_payload() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_personaldb_projection_watch_record(
            &storage,
            4,
            "projection-db",
            "projection-a",
            [1; 16],
            9,
            projection_payload(1),
        )
        .await
        .unwrap();
        assert!(
            append_personaldb_projection_watch_record(
                &storage,
                4,
                "projection-db",
                "projection-a",
                [1; 16],
                9,
                projection_payload(2),
            )
            .await
            .is_err()
        );

        let mut bad = projection_payload(3);
        bad.projection_id = "projection-b".to_string();
        assert!(
            append_personaldb_projection_watch_record(
                &storage,
                4,
                "projection-db",
                "projection-a",
                [3; 16],
                9,
                bad,
            )
            .await
            .is_err()
        );
    }

    fn payload(log_index: u64) -> PersonalDbGroupWatchPayload {
        PersonalDbGroupWatchPayload {
            database_id: "db-alpha".to_string(),
            event_type: "committed".to_string(),
            log_index,
            log_hash: hex::encode([log_index as u8; 32]),
            changeset_payload_hash: hex::encode([2; 32]),
            certificate_hash: hex::encode([3; 32]),
            committed_head_hash: hex::encode([4; 32]),
            emitted_at: "2026-06-27T00:00:00.000000000Z".to_string(),
        }
    }

    fn projection_payload(log_index: u64) -> PersonalDbProjectionWatchPayload {
        PersonalDbProjectionWatchPayload {
            database_id: "projection-db".to_string(),
            projection_id: "projection-a".to_string(),
            event_type: "projection_committed".to_string(),
            source_database_id: "source-db".to_string(),
            source_log_index: log_index + 10,
            source_log_hash: hex::encode([5; 32]),
            projection_log_index: log_index,
            projection_log_hash: hex::encode([log_index as u8; 32]),
            definition_hash: hex::encode([6; 32]),
            emitted_at: "2026-06-27T00:00:00.000000000Z".to_string(),
        }
    }
}
