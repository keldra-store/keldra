use crate::{
    core_store::{
        CF_REGISTRY, CoreMetaTuplePart, TABLE_STREAM_RECORD_INDEX_ROW, core_meta_tuple_key,
        decode_deterministic_proto, encode_deterministic_proto,
    },
    formats::{Hash32, hash32, watch::WatchRecord},
};
use anyhow::{Result, anyhow};
use prost::Message;
use serde::{Deserialize, Serialize};

const GIT_SOURCE_PARTITION_FAMILY: u16 = 6;
const GIT_SOURCE_RECORD_KIND: u16 = 1;

#[derive(Clone, PartialEq, Message)]
struct GitSourceWatchPayloadProto {
    #[prost(string, tag = "1")]
    repository_id: String,
    #[prost(string, tag = "2")]
    event_type: String,
    #[prost(uint64, tag = "3")]
    generation: u64,
    #[prost(string, tag = "4")]
    source_hash: String,
    #[prost(string, tag = "5")]
    index_path: String,
    #[prost(string, optional, tag = "6")]
    pack_object_version_id: Option<String>,
    #[prost(string, tag = "7")]
    emitted_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitSourceWatchPayload {
    pub repository_id: String,
    pub event_type: String,
    pub generation: u64,
    pub source_hash: String,
    pub index_path: String,
    pub pack_object_version_id: Option<String>,
    pub emitted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitSourceWatchEvent {
    pub cursor: u128,
    pub mutation_id: [u8; 16],
    pub authz_revision: u64,
    pub index_generation: u64,
    pub payload: GitSourceWatchPayload,
}

pub async fn append_git_source_watch_record(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    repository_id: &str,
    mutation_id: [u8; 16],
    authz_revision: u64,
    payload: GitSourceWatchPayload,
) -> Result<u128> {
    validate_payload(repository_id, &payload)?;
    let mut after = 0;
    loop {
        let page =
            list_git_source_watch_event_page(mvcc, tenant_id, repository_id, after, 1_000).await?;
        if let Some(existing) = page
            .events
            .iter()
            .find(|event| event.mutation_id == mutation_id)
        {
            if existing.authz_revision != authz_revision || existing.payload != payload {
                return Err(anyhow!(
                    "git source watch mutation ID identifies divergent content"
                ));
            }
            return Ok(existing.cursor);
        }
        if !page.has_more || page.next_cursor == after {
            break;
        }
        after = page.next_cursor;
    }
    let head_key = watch_head_key(tenant_id, repository_id)?;
    let head_payload = mvcc.read_latest_value(&head_key)?;
    let current = decode_watch_head(head_payload.as_deref())?;
    let sequence = current
        .checked_add(1)
        .ok_or_else(|| anyhow!("git source watch cursor overflow"))?;

    let record = WatchRecord::new(
        u128::from(sequence),
        GIT_SOURCE_PARTITION_FAMILY,
        partition_id(tenant_id, repository_id),
        mutation_id,
        GIT_SOURCE_RECORD_KIND,
        authz_revision,
        payload.generation,
        0,
        encode_git_source_watch_payload(&payload)?,
    );
    let event_key = watch_event_key(tenant_id, repository_id, sequence)?;
    mvcc.autocommit_product_mutations_with_predicates(
        "git-source-watch",
        &format!(
            "git-source-watch:{tenant_id}:{repository_id}:{}",
            hex::encode(mutation_id)
        ),
        vec![
            crate::mvcc_product::ProductMutation::put(event_key.clone(), record.encode()),
            crate::mvcc_product::ProductMutation::put(
                head_key.clone(),
                sequence.to_be_bytes().to_vec(),
            ),
        ],
        vec![
            (event_key, crate::mvcc_transaction::PredicateKind::Absent),
            (
                head_key,
                match head_payload {
                    Some(payload) => crate::mvcc_transaction::PredicateKind::ValueHash(
                        *blake3::hash(&payload).as_bytes(),
                    ),
                    None => crate::mvcc_transaction::PredicateKind::Absent,
                },
            ),
        ],
        crate::mvcc_transaction::DurabilityLevel::Quorum,
        u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| anyhow!("git watch timestamp predates Unix epoch"))?,
    )
    .await?;
    Ok(u128::from(sequence))
}

pub async fn list_git_source_watch_events(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    repository_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<GitSourceWatchEvent>> {
    Ok(
        list_git_source_watch_event_page(mvcc, tenant_id, repository_id, after_cursor, limit)
            .await?
            .events,
    )
}

#[derive(Debug, Clone)]
pub struct GitSourceWatchEventPage {
    pub events: Vec<GitSourceWatchEvent>,
    pub next_cursor: u128,
    pub has_more: bool,
}

pub async fn list_git_source_watch_event_page(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    repository_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<GitSourceWatchEventPage> {
    let after_sequence =
        u64::try_from(after_cursor).map_err(|_| anyhow!("git source watch cursor exceeds u64"))?;
    if limit == 0 || limit > 1_000 {
        return Err(anyhow!(
            "git source watch page limit must be between 1 and 1000"
        ));
    }
    let snapshot = mvcc.runtime.applied_version()?;
    let head = mvcc
        .runtime
        .read_at(&watch_head_key(tenant_id, repository_id)?, snapshot)?
        .map(|row| decode_watch_head(Some(&row.value)))
        .transpose()?
        .unwrap_or(0);
    let prefix = crate::mvcc_product::coremeta_application_prefix(
        CF_REGISTRY,
        &watch_event_prefix(tenant_id, repository_id)?,
    )?;
    let mut rows =
        mvcc.runtime
            .scan_table_prefix_at(TABLE_STREAM_RECORD_INDEX_ROW, &prefix, snapshot)?;
    rows.retain(|(_, row)| {
        WatchRecord::decode(&row.value)
            .map(|(record, _)| u64::try_from(record.cursor).unwrap_or(0) > after_sequence)
            .unwrap_or(true)
    });
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let mut events = Vec::with_capacity(rows.len());
    for (_, source) in rows {
        let (mut record, used) = WatchRecord::decode(&source.value)?;
        if used != source.value.len() {
            return Err(anyhow!("git source watch record has trailing bytes"));
        }
        let sequence = u64::try_from(record.cursor)
            .map_err(|_| anyhow!("git source watch record cursor exceeds u64"))?;
        if record.partition_family != GIT_SOURCE_PARTITION_FAMILY
            || record.record_kind != GIT_SOURCE_RECORD_KIND
            || record.partition_id != partition_id(tenant_id, repository_id)
        {
            return Err(anyhow!("git source watch record scope mismatch"));
        }
        let payload: GitSourceWatchPayload = decode_git_source_watch_payload(&record.payload)?;
        validate_payload(repository_id, &payload)?;
        events.push(GitSourceWatchEvent {
            cursor: record.cursor,
            mutation_id: record.mutation_id,
            authz_revision: record.authz_revision,
            index_generation: record.index_generation,
            payload,
        });
    }
    Ok(GitSourceWatchEventPage {
        next_cursor: events
            .last()
            .map(|event| event.cursor)
            .unwrap_or(after_cursor),
        events,
        has_more: has_more && u128::from(head) > after_cursor,
    })
}

pub async fn latest_git_source_watch_cursor(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    repository_id: &str,
) -> Result<Option<u128>> {
    let sequence = decode_watch_head(
        mvcc.read_latest_value(&watch_head_key(tenant_id, repository_id)?)?
            .as_deref(),
    )?;
    Ok((sequence != 0).then_some(u128::from(sequence)))
}

fn encode_git_source_watch_payload(payload: &GitSourceWatchPayload) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&GitSourceWatchPayloadProto {
        repository_id: payload.repository_id.clone(),
        event_type: payload.event_type.clone(),
        generation: payload.generation,
        source_hash: payload.source_hash.clone(),
        index_path: payload.index_path.clone(),
        pack_object_version_id: payload.pack_object_version_id.clone(),
        emitted_at: payload.emitted_at.clone(),
    }))
}

fn decode_git_source_watch_payload(bytes: &[u8]) -> Result<GitSourceWatchPayload> {
    let proto = decode_deterministic_proto::<GitSourceWatchPayloadProto>(
        bytes,
        "GitSourceWatchPayload payload",
    )?;
    Ok(GitSourceWatchPayload {
        repository_id: proto.repository_id,
        event_type: proto.event_type,
        generation: proto.generation,
        source_hash: proto.source_hash,
        index_path: proto.index_path,
        pack_object_version_id: proto.pack_object_version_id,
        emitted_at: proto.emitted_at,
    })
}

fn validate_payload(repository_id: &str, payload: &GitSourceWatchPayload) -> Result<()> {
    if payload.repository_id != repository_id {
        return Err(anyhow!("git source watch payload repository mismatch"));
    }
    if payload.event_type.is_empty()
        || payload.index_path.is_empty()
        || payload.emitted_at.is_empty()
    {
        return Err(anyhow!("git source watch payload is incomplete"));
    }
    validate_hex32(&payload.source_hash, "source_hash")?;
    if payload
        .pack_object_version_id
        .as_deref()
        .is_some_and(str::is_empty)
    {
        return Err(anyhow!("pack_object_version_id must not be empty"));
    }
    Ok(())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn partition_id(tenant_id: i64, repository_id: &str) -> Hash32 {
    hash32(format!("tenant:{tenant_id}:git:{repository_id}:watch:source").as_bytes())
}

pub(crate) fn git_source_watch_stream_id(tenant_id: i64, repository_id: &str) -> String {
    format!("watch:git_source:tenant:{tenant_id}:repository:{repository_id}")
}

fn watch_head_key(
    tenant_id: i64,
    repository_id: &str,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_REGISTRY,
        TABLE_STREAM_RECORD_INDEX_ROW,
        &core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("git-source-watch-head"),
            CoreMetaTuplePart::I64(tenant_id),
            CoreMetaTuplePart::Utf8(repository_id),
        ])?,
    )
}

fn watch_event_prefix(tenant_id: i64, repository_id: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("git-source-watch-event"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(repository_id),
    ])
}

fn watch_event_key(
    tenant_id: i64,
    repository_id: &str,
    sequence: u64,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_REGISTRY,
        TABLE_STREAM_RECORD_INDEX_ROW,
        &core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("git-source-watch-event"),
            CoreMetaTuplePart::I64(tenant_id),
            CoreMetaTuplePart::Utf8(repository_id),
            CoreMetaTuplePart::U64(sequence),
        ])?,
    )
}

fn decode_watch_head(payload: Option<&[u8]>) -> Result<u64> {
    let Some(payload) = payload else {
        return Ok(0);
    };
    let bytes: [u8; 8] = payload
        .try_into()
        .map_err(|_| anyhow!("git source watch head has invalid length"))?;
    Ok(u64::from_be_bytes(bytes))
}
