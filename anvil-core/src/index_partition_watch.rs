use crate::{
    core_store::{
        CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
        CoreMutationRootPublication, CoreStore, CoreTransactionState, ReadStream,
        core_mutation_publication_attempt_id, decode_deterministic_proto,
        encode_deterministic_proto,
    },
    formats::{Hash32, hash32, watch::WatchRecord},
    partition_fence::{
        OWNERSHIP_OWNER_MISMATCH, OwnershipPrincipal, OwnershipResource, OwnershipResourceKind,
        ownership_fence_precondition,
    },
    storage::Storage,
};
use anyhow::{Result, anyhow};
use prost::Message;
use serde::{Deserialize, Serialize};

const INDEX_PARTITION_FAMILY: u16 = 7;
const INDEX_PARTITION_RECORD_KIND: u16 = 1;
const MAX_INDEX_PARTITION_SEGMENT_HASHES: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexPartitionWatchPayload {
    pub index_id: String,
    pub index_kind: String,
    pub event_type: String,
    pub generation: u64,
    pub source_cursor: u128,
    pub source_manifest_hash: String,
    pub proof_hash: String,
    pub segment_hashes: Vec<String>,
    pub emitted_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct IndexPartitionWatchPayloadProto {
    #[prost(string, tag = "1")]
    index_id: String,
    #[prost(string, tag = "2")]
    index_kind: String,
    #[prost(string, tag = "3")]
    event_type: String,
    #[prost(uint64, tag = "4")]
    generation: u64,
    #[prost(string, tag = "5")]
    source_cursor: String,
    #[prost(string, tag = "6")]
    source_manifest_hash: String,
    #[prost(string, tag = "7")]
    proof_hash: String,
    #[prost(string, repeated, tag = "8")]
    segment_hashes: Vec<String>,
    #[prost(string, tag = "9")]
    emitted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPartitionWatchEvent {
    pub cursor: u128,
    pub mutation_id: [u8; 16],
    pub authz_revision: u64,
    pub index_generation: u64,
    pub payload: IndexPartitionWatchPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPartitionWatchWriteAuthority {
    pub owner_node_id: String,
    pub fence: u64,
    pub resource_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedIndexPartitionWatch {
    partition_id: String,
    stream_id: String,
    logical_id: String,
    payload: Vec<u8>,
    stream_precondition: CoreMutationPrecondition,
    ownership_precondition: CoreMutationPrecondition,
    next_sequence: u64,
}

pub async fn append_index_partition_watch_record(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    partition_id: &str,
    mutation_id: [u8; 16],
    authz_revision: u64,
    payload: IndexPartitionWatchPayload,
    authority: IndexPartitionWatchWriteAuthority,
    signing_key: &[u8],
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<u128> {
    let prepared = prepare_index_partition_watch_record(
        storage,
        tenant_id,
        bucket_id,
        partition_id,
        mutation_id,
        authz_revision,
        payload,
        authority,
        signing_key,
    )
    .await?;
    publish_prepared_index_partition_watch(storage, prepared, additional_preconditions).await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_index_partition_watch_record(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    partition_id: &str,
    mutation_id: [u8; 16],
    authz_revision: u64,
    payload: IndexPartitionWatchPayload,
    authority: IndexPartitionWatchWriteAuthority,
    signing_key: &[u8],
) -> Result<PreparedIndexPartitionWatch> {
    validate_payload(partition_id, &payload)?;
    let ownership_precondition = validate_write_authority(
        storage,
        tenant_id,
        bucket_id,
        partition_id,
        &payload,
        &authority,
        signing_key,
    )
    .await?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id =
        index_partition_watch_stream_id(tenant_id, bucket_id, &payload.index_id, partition_id);

    let record = WatchRecord::new(
        0,
        INDEX_PARTITION_FAMILY,
        watch_partition_id(tenant_id, bucket_id, &payload.index_id, partition_id),
        mutation_id,
        INDEX_PARTITION_RECORD_KIND,
        authz_revision,
        payload.generation,
        0,
        encode_index_partition_watch_payload(&payload),
    );
    let stream_precondition = core_store.stream_head_precondition(&stream_id).await?;
    let next_sequence = next_stream_sequence(&stream_precondition)?;
    let logical_id = format!(
        "index-partition-watch:{tenant_id}:{bucket_id}:{}:{partition_id}:{}",
        payload.index_id,
        hex::encode(mutation_id)
    );
    Ok(PreparedIndexPartitionWatch {
        partition_id: hex::encode(watch_partition_id(
            tenant_id,
            bucket_id,
            &payload.index_id,
            partition_id,
        )),
        stream_id,
        logical_id,
        payload: record.encode(),
        stream_precondition,
        ownership_precondition,
        next_sequence,
    })
}

pub(crate) async fn publish_prepared_index_partition_watch(
    storage: &Storage,
    prepared: PreparedIndexPartitionWatch,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<u128> {
    let core_store = CoreStore::new(storage.clone()).await?;
    if let Some(sequence) = committed_replay_sequence(&core_store, &prepared).await? {
        return Ok(u128::from(sequence));
    }
    let mut preconditions = Vec::with_capacity(additional_preconditions.len() + 2);
    preconditions.push(prepared.ownership_precondition);
    preconditions.push(prepared.stream_precondition);
    preconditions.extend_from_slice(additional_preconditions);
    let transaction_id =
        core_mutation_publication_attempt_id(&prepared.logical_id, &preconditions)?;
    let receipt = core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: prepared.partition_id.clone(),
            committed_by_principal: "index-partition-watch".to_string(),
            root_publications: vec![
                CoreMutationRootPublication::new(
                    prepared.partition_id.clone(),
                    crate::formats::writer::WriterFamily::CoreControl.as_str(),
                )
                .coordinator(),
            ],
            preconditions,
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id: prepared.partition_id,
                stream_id: prepared.stream_id,
                record_kind: "index_partition_watch".to_string(),
                payload: prepared.payload,
                idempotency_key: Some(prepared.logical_id),
            }],
        })
        .await?;
    if receipt.state != CoreTransactionState::Committed {
        return Err(anyhow!(
            "index partition watch publication {} did not commit: {}",
            receipt.transaction_id,
            receipt
                .finalisation_error
                .as_deref()
                .unwrap_or("unknown finalisation failure")
        ));
    }
    Ok(u128::from(prepared.next_sequence))
}

async fn committed_replay_sequence(
    core_store: &CoreStore,
    prepared: &PreparedIndexPartitionWatch,
) -> Result<Option<u64>> {
    let Some(record) = core_store
        .read_stream_record_by_idempotency_key(&prepared.stream_id, &prepared.logical_id)
        .await?
    else {
        return Ok(None);
    };
    if record.record_kind != "index_partition_watch"
        || record.payload != prepared.payload
        || record.authenticated_principal != "index-partition-watch"
    {
        return Err(anyhow!(
            "index partition watch logical id identifies different committed content"
        ));
    }
    Ok(Some(record.sequence))
}

pub fn index_partition_watch_resource_id(
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
) -> String {
    format!(
        "watch/index_partition/tenant/{tenant_id}/bucket/{bucket_id}/index/{index_id}/partition/{partition_id}"
    )
}

pub async fn list_index_partition_watch_events(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<IndexPartitionWatchEvent>> {
    Ok(list_index_partition_watch_event_page(
        storage,
        tenant_id,
        bucket_id,
        index_id,
        partition_id,
        after_cursor,
        limit,
    )
    .await?
    .events)
}

#[derive(Debug, Clone)]
pub struct IndexPartitionWatchEventPage {
    pub events: Vec<IndexPartitionWatchEvent>,
    pub next_cursor: u128,
    pub has_more: bool,
}

pub async fn list_index_partition_watch_event_page(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<IndexPartitionWatchEventPage> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let after_sequence = u64::try_from(after_cursor)
        .map_err(|_| anyhow!("index partition watch cursor exceeds u64"))?;
    let page = core_store
        .read_stream_page(ReadStream {
            stream_id: index_partition_watch_stream_id(
                tenant_id,
                bucket_id,
                index_id,
                partition_id,
            ),
            after_sequence,
            limit,
        })
        .await?;
    let expected_partition = watch_partition_id(tenant_id, bucket_id, index_id, partition_id);
    let mut events = Vec::with_capacity(page.records.len());
    for source in page.records {
        if source.record_kind != "index_partition_watch" {
            return Err(anyhow!("index partition watch stream record kind mismatch"));
        }
        let (mut record, used) = WatchRecord::decode(&source.payload)?;
        if used != source.payload.len() {
            return Err(anyhow!("index partition watch record has trailing bytes"));
        }
        record.cursor = u128::from(source.sequence);
        if record.partition_family != INDEX_PARTITION_FAMILY
            || record.record_kind != INDEX_PARTITION_RECORD_KIND
            || record.partition_id != expected_partition
        {
            return Err(anyhow!("index partition watch record scope mismatch"));
        }
        let payload = decode_index_partition_watch_payload(&record.payload)?;
        if payload.index_id != index_id {
            return Err(anyhow!("index partition watch payload scope mismatch"));
        }
        validate_payload(partition_id, &payload)?;
        events.push(IndexPartitionWatchEvent {
            cursor: record.cursor,
            mutation_id: record.mutation_id,
            authz_revision: record.authz_revision,
            index_generation: record.index_generation,
            payload,
        });
    }
    Ok(IndexPartitionWatchEventPage {
        events,
        next_cursor: u128::from(page.next_sequence),
        has_more: page.has_more,
    })
}

async fn validate_write_authority(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    partition_id: &str,
    payload: &IndexPartitionWatchPayload,
    authority: &IndexPartitionWatchWriteAuthority,
    signing_key: &[u8],
) -> Result<CoreMutationPrecondition> {
    if authority.fence == 0 {
        return Err(anyhow!("index partition watch write fence must be nonzero"));
    }
    let expected_resource_id =
        index_partition_watch_resource_id(tenant_id, bucket_id, &payload.index_id, partition_id);
    if authority.resource_id != expected_resource_id {
        return Err(anyhow!(
            "{OWNERSHIP_OWNER_MISMATCH}: index partition watch authority resource mismatch"
        ));
    }
    let resource = OwnershipResource {
        resource_kind: OwnershipResourceKind::WatchPartition,
        resource_id: authority.resource_id.clone(),
    };
    let now_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("index partition watch timestamp overflow"))?;
    ownership_fence_precondition(
        storage,
        0,
        &resource,
        &OwnershipPrincipal::node(authority.owner_node_id.clone()),
        authority.fence,
        now_nanos,
        signing_key,
    )
    .await
}

fn next_stream_sequence(precondition: &CoreMutationPrecondition) -> Result<u64> {
    let CoreMutationPrecondition::StreamHead {
        expected_last_sequence,
        ..
    } = precondition
    else {
        return Err(anyhow!(
            "index partition watch stream precondition has wrong kind"
        ));
    };
    expected_last_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("index partition watch cursor overflow"))
}

pub async fn latest_index_partition_watch_cursor(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
) -> Result<Option<u128>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let sequence = core_store
        .stream_head_sequence(&index_partition_watch_stream_id(
            tenant_id,
            bucket_id,
            index_id,
            partition_id,
        ))
        .await?;
    Ok((sequence != 0).then_some(u128::from(sequence)))
}

pub(crate) fn index_partition_watch_stream_id_for_scope(
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
) -> String {
    index_partition_watch_stream_id(tenant_id, bucket_id, index_id, partition_id)
}

fn validate_payload(partition_id: &str, payload: &IndexPartitionWatchPayload) -> Result<()> {
    require_nonempty(&payload.index_id, "index_id")?;
    require_nonempty(&payload.index_kind, "index_kind")?;
    require_nonempty(&payload.event_type, "event_type")?;
    validate_hex32(partition_id, "partition_id")?;
    if payload.generation == 0 {
        return Err(anyhow!("index partition watch generation must be nonzero"));
    }
    validate_hex32(&payload.source_manifest_hash, "source_manifest_hash")?;
    validate_hex32(&payload.proof_hash, "proof_hash")?;
    if payload.segment_hashes.is_empty() {
        return Err(anyhow!("index partition watch requires segment hashes"));
    }
    if payload.segment_hashes.len() > MAX_INDEX_PARTITION_SEGMENT_HASHES {
        return Err(anyhow!(
            "index partition watch must contain no more than {MAX_INDEX_PARTITION_SEGMENT_HASHES} segment hashes"
        ));
    }
    for segment_hash in &payload.segment_hashes {
        validate_hex32(segment_hash, "segment_hash")?;
    }
    require_nonempty(&payload.emitted_at, "emitted_at")?;
    Ok(())
}

fn watch_partition_id(
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
) -> Hash32 {
    hash32(
        format!("tenant:{tenant_id}:bucket:{bucket_id}:index:{index_id}:partition:{partition_id}")
            .as_bytes(),
    )
}

fn index_partition_watch_stream_id(
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
) -> String {
    format!(
        "watch:index_partition:tenant:{tenant_id}:bucket:{bucket_id}:index:{index_id}:partition:{partition_id}"
    )
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

fn encode_index_partition_watch_payload(payload: &IndexPartitionWatchPayload) -> Vec<u8> {
    encode_deterministic_proto(&IndexPartitionWatchPayloadProto {
        index_id: payload.index_id.clone(),
        index_kind: payload.index_kind.clone(),
        event_type: payload.event_type.clone(),
        generation: payload.generation,
        source_cursor: payload.source_cursor.to_string(),
        source_manifest_hash: payload.source_manifest_hash.clone(),
        proof_hash: payload.proof_hash.clone(),
        segment_hashes: payload.segment_hashes.clone(),
        emitted_at: payload.emitted_at.clone(),
    })
}

fn decode_index_partition_watch_payload(bytes: &[u8]) -> Result<IndexPartitionWatchPayload> {
    let proto = decode_deterministic_proto::<IndexPartitionWatchPayloadProto>(
        bytes,
        "index partition watch payload",
    )?;
    Ok(IndexPartitionWatchPayload {
        index_id: proto.index_id,
        index_kind: proto.index_kind,
        event_type: proto.event_type,
        generation: proto.generation,
        source_cursor: proto
            .source_cursor
            .parse()
            .map_err(|_| anyhow!("index partition watch source_cursor is not u128"))?,
        source_manifest_hash: proto.source_manifest_hash,
        proof_hash: proto.proof_hash,
        segment_hashes: proto.segment_hashes,
        emitted_at: proto.emitted_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_fence::{
        AcquireOwnership, ForceExpireOwnership, MAX_OWNERSHIP_LEASE_MS, OwnershipPrincipal,
        OwnershipResource, OwnershipResourceKind, acquire_ownership, force_expire_ownership,
    };
    use crate::storage::Storage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn index_partition_watch_appends_lists_and_tracks_latest_cursor() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let partition_id = hex::encode([5; 32]);
        let first_payload = payload(1);
        let first_authority = authority(&storage, 3, 9, &partition_id, &first_payload).await;
        append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            [1; 16],
            7,
            first_payload,
            first_authority,
            KEY,
            &[],
        )
        .await
        .unwrap();
        let second_payload = payload(2);
        let second_authority = authority(&storage, 3, 9, &partition_id, &second_payload).await;
        append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            [2; 16],
            8,
            second_payload,
            second_authority,
            KEY,
            &[],
        )
        .await
        .unwrap();
        assert_eq!(
            index_partition_watch_stream_id(3, 9, "full-text-alpha", &partition_id),
            format!(
                "watch:index_partition:tenant:3:bucket:9:index:full-text-alpha:partition:{partition_id}"
            )
        );

        let events = list_index_partition_watch_events(
            &storage,
            3,
            9,
            "full-text-alpha",
            &partition_id,
            1,
            10,
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, 2);
        assert_eq!(events[0].index_generation, 2);
        assert_eq!(events[0].authz_revision, 8);
        assert_eq!(events[0].payload.generation, 2);
        assert_eq!(
            latest_index_partition_watch_cursor(&storage, 3, 9, "full-text-alpha", &partition_id)
                .await
                .unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn index_partition_watch_exact_retry_reuses_the_committed_record() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let partition_id = hex::encode([5; 32]);
        let event = payload(1);
        let write_authority = authority(&storage, 3, 9, &partition_id, &event).await;

        let first = append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            [1; 16],
            7,
            event.clone(),
            write_authority.clone(),
            KEY,
            &[],
        )
        .await
        .unwrap();
        let replay = append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            [1; 16],
            7,
            event,
            write_authority,
            KEY,
            &[],
        )
        .await
        .unwrap();

        assert_eq!(replay, first);
        assert_eq!(
            list_index_partition_watch_events(
                &storage,
                3,
                9,
                "full-text-alpha",
                &partition_id,
                0,
                10,
            )
            .await
            .unwrap()
            .len(),
            1
        );
    }

    #[tokio::test]
    async fn index_partition_watch_rejects_idempotency_conflicts_and_invalid_payloads() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let partition_id = hex::encode([5; 32]);
        let first_payload = payload(1);
        let first_authority = authority(&storage, 3, 9, &partition_id, &first_payload).await;
        append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            [1; 16],
            7,
            first_payload,
            first_authority,
            KEY,
            &[],
        )
        .await
        .unwrap();
        let second_payload = payload(2);
        let second_authority = authority(&storage, 3, 9, &partition_id, &second_payload).await;
        assert!(
            append_index_partition_watch_record(
                &storage,
                3,
                9,
                &partition_id,
                [1; 16],
                7,
                second_payload,
                second_authority,
                KEY,
                &[]
            )
            .await
            .is_err()
        );
        let mut invalid = payload(3);
        invalid.segment_hashes.clear();
        let invalid_authority = IndexPartitionWatchWriteAuthority {
            owner_node_id: "node-a".to_string(),
            fence: 1,
            resource_id: index_partition_watch_resource_id(3, 9, "full-text-alpha", &partition_id),
        };
        assert!(
            append_index_partition_watch_record(
                &storage,
                3,
                9,
                &partition_id,
                [3; 16],
                7,
                invalid,
                invalid_authority,
                KEY,
                &[]
            )
            .await
            .is_err()
        );
        assert!(validate_payload("../partition", &payload(4)).is_err());
    }

    #[tokio::test]
    async fn index_partition_watch_requires_a_bounded_page_limit() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let partition_id = hex::encode([5; 32]);
        for generation in 1..=3 {
            let next_payload = payload(generation);
            let next_authority = authority(&storage, 3, 9, &partition_id, &next_payload).await;
            append_index_partition_watch_record(
                &storage,
                3,
                9,
                &partition_id,
                [generation as u8; 16],
                7,
                next_payload,
                next_authority,
                KEY,
                &[],
            )
            .await
            .unwrap();
        }
        let error = list_index_partition_watch_events(
            &storage,
            3,
            9,
            "full-text-alpha",
            &partition_id,
            0,
            0,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("limit"));
    }

    #[tokio::test]
    async fn index_partition_watch_rejects_stale_or_mismatched_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let partition_id = hex::encode([5; 32]);
        let next_payload = payload(1);
        let valid = authority(&storage, 3, 9, &partition_id, &next_payload).await;
        let stale = IndexPartitionWatchWriteAuthority {
            fence: valid.fence.saturating_add(1),
            ..valid.clone()
        };
        let err = append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            [1; 16],
            7,
            next_payload.clone(),
            stale,
            KEY,
            &[],
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("StaleFence"));

        let wrong_resource = IndexPartitionWatchWriteAuthority {
            resource_id: "watch/index_partition/wrong".to_string(),
            ..valid
        };
        let err = append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            [1; 16],
            7,
            next_payload,
            wrong_resource,
            KEY,
            &[],
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("OwnershipOwnerMismatch"));

        let next_payload = payload(2);
        let stale_after_failover = authority(&storage, 3, 9, &partition_id, &next_payload).await;
        replace_index_partition_watch_owner(&storage, &stale_after_failover).await;
        let err = append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            [2; 16],
            7,
            next_payload,
            stale_after_failover,
            KEY,
            &[],
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("OwnershipOwnerMismatch"));
    }

    fn payload(generation: u64) -> IndexPartitionWatchPayload {
        IndexPartitionWatchPayload {
            index_id: "full-text-alpha".to_string(),
            index_kind: "full_text".to_string(),
            event_type: "partition_published".to_string(),
            generation,
            source_cursor: 40 + u128::from(generation),
            source_manifest_hash: hex::encode([6; 32]),
            proof_hash: hex::encode([7; 32]),
            segment_hashes: vec![hex::encode([8; 32])],
            emitted_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        }
    }

    const KEY: &[u8] = b"index partition watch signing key";

    async fn authority(
        storage: &Storage,
        tenant_id: i64,
        bucket_id: i64,
        partition_id: &str,
        payload: &IndexPartitionWatchPayload,
    ) -> IndexPartitionWatchWriteAuthority {
        let resource_id = index_partition_watch_resource_id(
            tenant_id,
            bucket_id,
            &payload.index_id,
            partition_id,
        );
        let outcome = acquire_ownership(
            storage,
            AcquireOwnership {
                request_id: format!("test-index-watch-{resource_id}"),
                idempotency_key: format!("test-index-watch-{resource_id}"),
                resource: OwnershipResource {
                    resource_kind: OwnershipResourceKind::WatchPartition,
                    resource_id: resource_id.clone(),
                },
                owner: OwnershipPrincipal {
                    tenant_id: 0,
                    principal_kind: "node".to_string(),
                    principal_id: "node-a".to_string(),
                    actor_instance_id: "node-a".to_string(),
                    display_name: "node-a".to_string(),
                    region: "test-region".to_string(),
                    cell: "default".to_string(),
                },
                now_nanos: chrono::Utc::now().timestamp_nanos_opt().unwrap(),
                ttl_nanos: i64::try_from(MAX_OWNERSHIP_LEASE_MS)
                    .unwrap()
                    .saturating_mul(1_000_000),
            },
            KEY,
        )
        .await
        .unwrap();
        IndexPartitionWatchWriteAuthority {
            owner_node_id: "node-a".to_string(),
            fence: outcome.record.fence,
            resource_id,
        }
    }

    async fn replace_index_partition_watch_owner(
        storage: &Storage,
        stale_authority: &IndexPartitionWatchWriteAuthority,
    ) {
        let resource = OwnershipResource {
            resource_kind: OwnershipResourceKind::WatchPartition,
            resource_id: stale_authority.resource_id.clone(),
        };
        let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        force_expire_ownership(
            storage,
            ForceExpireOwnership {
                request_id: format!("test-index-watch-expire-{}", stale_authority.resource_id),
                idempotency_key: format!("test-index-watch-expire-{}", stale_authority.resource_id),
                resource: resource.clone(),
                admin: OwnershipPrincipal::node("admin-node"),
                reason: "test ownership failover".to_string(),
                now_nanos,
            },
            KEY,
        )
        .await
        .unwrap();
        acquire_ownership(
            storage,
            AcquireOwnership {
                request_id: format!(
                    "test-index-watch-replacement-{}",
                    stale_authority.resource_id
                ),
                idempotency_key: format!(
                    "test-index-watch-replacement-{}",
                    stale_authority.resource_id
                ),
                resource,
                owner: OwnershipPrincipal::node("node-b"),
                now_nanos: now_nanos.saturating_add(1),
                ttl_nanos: i64::try_from(MAX_OWNERSHIP_LEASE_MS)
                    .unwrap()
                    .saturating_mul(1_000_000),
            },
            KEY,
        )
        .await
        .unwrap();
    }
}
