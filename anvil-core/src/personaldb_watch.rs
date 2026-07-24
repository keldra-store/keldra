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

const PERSONALDB_GROUP_PARTITION_FAMILY: u16 = 4;
const PERSONALDB_GROUP_RECORD_KIND: u16 = 1;
const PERSONALDB_PROJECTION_PARTITION_FAMILY: u16 = 5;
const PERSONALDB_PROJECTION_RECORD_KIND: u16 = 1;

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
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    mutation_id: [u8; 16],
    authz_revision: u64,
    payload: PersonalDbGroupWatchPayload,
) -> Result<u128> {
    validate_payload(database_id, &payload)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = personaldb_group_watch_stream_id(tenant_id, database_id);

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
    let receipt = core_store
        .append_stream(AppendStreamRecord {
            stream_id,
            partition_id: hex::encode(partition_id(tenant_id, database_id)),
            record_kind: "personaldb_group_watch".to_string(),
            payload: record.encode(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!(
                "personaldb-group-watch:{tenant_id}:{database_id}:{}",
                hex::encode(mutation_id)
            )),
        })
        .await?;
    Ok(u128::from(receipt.sequence))
}

pub async fn append_personaldb_projection_watch_record(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
    mutation_id: [u8; 16],
    authz_revision: u64,
    payload: PersonalDbProjectionWatchPayload,
) -> Result<u128> {
    validate_projection_payload(database_id, projection_id, &payload)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = personaldb_projection_watch_stream_id(tenant_id, database_id, projection_id);

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
    let receipt = core_store
        .append_stream(AppendStreamRecord {
            stream_id,
            partition_id: hex::encode(projection_partition_id(
                tenant_id,
                database_id,
                projection_id,
            )),
            record_kind: "personaldb_projection_watch".to_string(),
            payload: record.encode(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!(
                "personaldb-projection-watch:{tenant_id}:{database_id}:{projection_id}:{}",
                hex::encode(mutation_id)
            )),
        })
        .await?;
    Ok(u128::from(receipt.sequence))
}

pub async fn list_personaldb_group_watch_events(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<PersonalDbGroupWatchEvent>> {
    Ok(
        list_personaldb_group_watch_event_page(
            storage,
            tenant_id,
            database_id,
            after_cursor,
            limit,
        )
        .await?
        .events,
    )
}

#[derive(Debug, Clone)]
pub struct PersonalDbGroupWatchEventPage {
    pub events: Vec<PersonalDbGroupWatchEvent>,
    pub next_cursor: u128,
    pub has_more: bool,
}

pub async fn list_personaldb_group_watch_event_page(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<PersonalDbGroupWatchEventPage> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let (records, next_cursor, has_more) = read_watch_page(
        &core_store,
        &personaldb_group_watch_stream_id(tenant_id, database_id),
        "personaldb_group_watch",
        after_cursor,
        limit,
    )
    .await?;
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
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<PersonalDbProjectionWatchEvent>> {
    Ok(list_personaldb_projection_watch_event_page(
        storage,
        tenant_id,
        database_id,
        projection_id,
        after_cursor,
        limit,
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
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<PersonalDbProjectionWatchEventPage> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let (records, next_cursor, has_more) = read_watch_page(
        &core_store,
        &personaldb_projection_watch_stream_id(tenant_id, database_id, projection_id),
        "personaldb_projection_watch",
        after_cursor,
        limit,
    )
    .await?;
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
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
) -> Result<Option<u128>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let sequence = core_store
        .stream_head_sequence(&personaldb_group_watch_stream_id(tenant_id, database_id))
        .await?;
    Ok((sequence != 0).then_some(u128::from(sequence)))
}

pub async fn latest_personaldb_projection_watch_cursor(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
) -> Result<Option<u128>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let sequence = core_store
        .stream_head_sequence(&personaldb_projection_watch_stream_id(
            tenant_id,
            database_id,
            projection_id,
        ))
        .await?;
    Ok((sequence != 0).then_some(u128::from(sequence)))
}

async fn read_watch_page(
    core_store: &CoreStore,
    stream_id: &str,
    record_kind: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<(Vec<WatchRecord>, u128, bool)> {
    let after_sequence =
        u64::try_from(after_cursor).map_err(|_| anyhow!("personaldb watch cursor exceeds u64"))?;
    let page = core_store
        .read_stream_page(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence,
            limit,
        })
        .await?;
    let mut records = Vec::with_capacity(page.records.len());
    for source in page.records {
        if source.record_kind != record_kind {
            return Err(anyhow!("personaldb watch stream record kind mismatch"));
        }
        let (mut record, used) = WatchRecord::decode(&source.payload)?;
        if used != source.payload.len() {
            return Err(anyhow!("personaldb watch record has trailing bytes"));
        }
        record.cursor = u128::from(source.sequence);
        records.push(record);
    }
    Ok((records, u128::from(page.next_sequence), page.has_more))
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

pub(crate) fn personaldb_group_watch_stream_id(tenant_id: i64, database_id: &str) -> String {
    format!("watch:personaldb_group:tenant:{tenant_id}:database:{database_id}")
}

pub(crate) fn personaldb_projection_watch_stream_id(
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
) -> String {
    format!(
        "watch:personaldb_projection:tenant:{tenant_id}:database:{database_id}:projection:{projection_id}"
    )
}

#[cfg(test)]
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
