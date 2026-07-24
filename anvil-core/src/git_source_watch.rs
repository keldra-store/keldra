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
    storage: &Storage,
    tenant_id: i64,
    repository_id: &str,
    mutation_id: [u8; 16],
    authz_revision: u64,
    payload: GitSourceWatchPayload,
) -> Result<u128> {
    validate_payload(repository_id, &payload)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = git_source_watch_stream_id(tenant_id, repository_id);

    let record = WatchRecord::new(
        0,
        GIT_SOURCE_PARTITION_FAMILY,
        partition_id(tenant_id, repository_id),
        mutation_id,
        GIT_SOURCE_RECORD_KIND,
        authz_revision,
        payload.generation,
        0,
        encode_git_source_watch_payload(&payload)?,
    );
    let receipt = core_store
        .append_stream(AppendStreamRecord {
            stream_id,
            partition_id: hex::encode(partition_id(tenant_id, repository_id)),
            record_kind: "git_source_watch".to_string(),
            payload: record.encode(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!(
                "git-source-watch:{tenant_id}:{repository_id}:{}",
                hex::encode(mutation_id)
            )),
        })
        .await?;
    Ok(u128::from(receipt.sequence))
}

pub async fn list_git_source_watch_events(
    storage: &Storage,
    tenant_id: i64,
    repository_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<GitSourceWatchEvent>> {
    Ok(
        list_git_source_watch_event_page(storage, tenant_id, repository_id, after_cursor, limit)
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
    storage: &Storage,
    tenant_id: i64,
    repository_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<GitSourceWatchEventPage> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let after_sequence =
        u64::try_from(after_cursor).map_err(|_| anyhow!("git source watch cursor exceeds u64"))?;
    let page = core_store
        .read_stream_page(ReadStream {
            stream_id: git_source_watch_stream_id(tenant_id, repository_id),
            after_sequence,
            limit,
        })
        .await?;
    let mut events = Vec::with_capacity(page.records.len());
    for source in page.records {
        if source.record_kind != "git_source_watch" {
            return Err(anyhow!("git source watch stream record kind mismatch"));
        }
        let (mut record, used) = WatchRecord::decode(&source.payload)?;
        if used != source.payload.len() {
            return Err(anyhow!("git source watch record has trailing bytes"));
        }
        record.cursor = u128::from(source.sequence);
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
        events,
        next_cursor: u128::from(page.next_sequence),
        has_more: page.has_more,
    })
}

pub async fn latest_git_source_watch_cursor(
    storage: &Storage,
    tenant_id: i64,
    repository_id: &str,
) -> Result<Option<u128>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let sequence = core_store
        .stream_head_sequence(&git_source_watch_stream_id(tenant_id, repository_id))
        .await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn git_source_watch_appends_lists_and_tracks_latest_cursor() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_git_source_watch_record(&storage, 9, "repo-alpha", [1; 16], 2, payload(1))
            .await
            .unwrap();
        append_git_source_watch_record(&storage, 9, "repo-alpha", [2; 16], 3, payload(2))
            .await
            .unwrap();

        assert_eq!(
            git_source_watch_stream_id(9, "repo-alpha"),
            "watch:git_source:tenant:9:repository:repo-alpha"
        );
        let events = list_git_source_watch_events(&storage, 9, "repo-alpha", 1, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, 2);
        assert_eq!(events[0].index_generation, 2);
        assert_eq!(events[0].payload.generation, 2);
        assert_eq!(
            latest_git_source_watch_cursor(&storage, 9, "repo-alpha")
                .await
                .unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn git_source_watch_rejects_idempotency_conflicts_and_bad_payload() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_git_source_watch_record(&storage, 9, "repo-alpha", [1; 16], 2, payload(1))
            .await
            .unwrap();
        assert!(
            append_git_source_watch_record(&storage, 9, "repo-alpha", [1; 16], 2, payload(2))
                .await
                .is_err()
        );

        let mut bad = payload(3);
        bad.repository_id = "repo-beta".to_string();
        assert!(
            append_git_source_watch_record(&storage, 9, "repo-alpha", [3; 16], 2, bad)
                .await
                .is_err()
        );
    }

    fn payload(generation: u64) -> GitSourceWatchPayload {
        GitSourceWatchPayload {
            repository_id: "repo-alpha".to_string(),
            event_type: "index_published".to_string(),
            generation,
            source_hash: hex::encode([generation as u8; 32]),
            index_path: format!(
                "_anvil/git/tenants/tenant-9/repositories/repo-alpha/indexes/generation-{generation:020}-source.angit"
            ),
            pack_object_version_id: Some("00000000-0000-0000-0000-000000000001".to_string()),
            emitted_at: "2026-06-27T00:00:00.000000000Z".to_string(),
        }
    }
}
