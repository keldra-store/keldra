use crate::{
    core_store::{
        CF_INDEX_ROWS, CoreMetaTuplePart, TABLE_STREAM_RECORD_INDEX_ROW, core_meta_tuple_key,
        decode_deterministic_proto, encode_deterministic_proto,
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
    tenant_id: i64,
    bucket_id: i64,
    index_id: String,
    partition_id: String,
    logical_id: String,
    payload: Vec<u8>,
    head_key: crate::mvcc_transaction::LogicalKey,
    head_payload: Option<Vec<u8>>,
    next_sequence: u64,
}

pub async fn append_index_partition_watch_record(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    partition_id: &str,
    mutation_id: [u8; 16],
    authz_revision: u64,
    payload: IndexPartitionWatchPayload,
    authority: IndexPartitionWatchWriteAuthority,
    signing_key: &[u8],
    additional_preconditions: &[(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )],
) -> Result<u128> {
    let prepared = prepare_index_partition_watch_record(
        storage,
        mvcc,
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
    publish_prepared_index_partition_watch(mvcc, prepared, additional_preconditions).await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_index_partition_watch_record(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
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
    validate_write_authority(
        storage,
        tenant_id,
        bucket_id,
        partition_id,
        &payload,
        &authority,
        signing_key,
    )
    .await?;
    let head_key = watch_head_key(tenant_id, bucket_id, &payload.index_id, partition_id)?;
    let head_payload = mvcc.read_latest_value(&head_key)?;
    let next_sequence = decode_watch_head(head_payload.as_deref())?
        .checked_add(1)
        .ok_or_else(|| anyhow!("index partition watch cursor overflow"))?;
    let record = WatchRecord::new(
        u128::from(next_sequence),
        INDEX_PARTITION_FAMILY,
        watch_partition_id(tenant_id, bucket_id, &payload.index_id, partition_id),
        mutation_id,
        INDEX_PARTITION_RECORD_KIND,
        authz_revision,
        payload.generation,
        0,
        encode_index_partition_watch_payload(&payload),
    );
    let logical_id = format!(
        "index-partition-watch:{tenant_id}:{bucket_id}:{}:{partition_id}:{}",
        payload.index_id,
        hex::encode(mutation_id)
    );
    Ok(PreparedIndexPartitionWatch {
        tenant_id,
        bucket_id,
        index_id: payload.index_id,
        partition_id: partition_id.to_string(),
        logical_id,
        payload: record.encode(),
        head_key,
        head_payload,
        next_sequence,
    })
}

pub(crate) async fn publish_prepared_index_partition_watch(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    prepared: PreparedIndexPartitionWatch,
    additional_preconditions: &[(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )],
) -> Result<u128> {
    let event_key = watch_event_key(
        prepared.tenant_id,
        prepared.bucket_id,
        &prepared.index_id,
        &prepared.partition_id,
        prepared.next_sequence,
    )?;
    let mut predicates = additional_preconditions.to_vec();
    predicates.extend([
        (
            event_key.clone(),
            crate::mvcc_transaction::PredicateKind::Absent,
        ),
        (
            prepared.head_key.clone(),
            match prepared.head_payload {
                Some(payload) => crate::mvcc_transaction::PredicateKind::ValueHash(
                    *blake3::hash(&payload).as_bytes(),
                ),
                None => crate::mvcc_transaction::PredicateKind::Absent,
            },
        ),
    ]);
    mvcc.autocommit_product_mutations_with_predicates(
        "index-partition-watch",
        &prepared.logical_id,
        vec![
            crate::mvcc_product::ProductMutation::put(event_key, prepared.payload),
            crate::mvcc_product::ProductMutation::put(
                prepared.head_key,
                prepared.next_sequence.to_be_bytes().to_vec(),
            ),
        ],
        predicates,
        crate::mvcc_transaction::DurabilityLevel::Quorum,
        u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| anyhow!("index partition watch timestamp predates Unix epoch"))?,
    )
    .await?;
    Ok(u128::from(prepared.next_sequence))
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<IndexPartitionWatchEvent>> {
    Ok(list_index_partition_watch_event_page(
        mvcc,
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<IndexPartitionWatchEventPage> {
    let after_sequence = u64::try_from(after_cursor)
        .map_err(|_| anyhow!("index partition watch cursor exceeds u64"))?;
    if limit == 0 || limit > 1_000 {
        return Err(anyhow!(
            "index partition watch page limit must be between 1 and 1000"
        ));
    }
    let snapshot = mvcc.runtime.applied_version()?;
    let head = mvcc
        .runtime
        .read_at(
            &watch_head_key(tenant_id, bucket_id, index_id, partition_id)?,
            snapshot,
        )?
        .map(|row| decode_watch_head(Some(&row.value)))
        .transpose()?
        .unwrap_or(0);
    let prefix = crate::mvcc_product::coremeta_application_prefix(
        CF_INDEX_ROWS,
        &watch_event_prefix(tenant_id, bucket_id, index_id, partition_id)?,
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
    let expected_partition = watch_partition_id(tenant_id, bucket_id, index_id, partition_id);
    let mut events = Vec::with_capacity(rows.len());
    for (_, source) in rows {
        let (record, used) = WatchRecord::decode(&source.value)?;
        if used != source.value.len() {
            return Err(anyhow!("index partition watch record has trailing bytes"));
        }
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
        next_cursor: events
            .last()
            .map(|event| event.cursor)
            .unwrap_or(after_cursor),
        events,
        has_more: has_more && u128::from(head) > after_cursor,
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
) -> Result<()> {
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
    let _ = ownership_fence_precondition(
        storage,
        0,
        &resource,
        &OwnershipPrincipal::node(authority.owner_node_id.clone()),
        authority.fence,
        now_nanos,
        signing_key,
    )
    .await?;
    Ok(())
}

pub async fn latest_index_partition_watch_cursor(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
) -> Result<Option<u128>> {
    let sequence = decode_watch_head(
        mvcc.read_latest_value(&watch_head_key(
            tenant_id,
            bucket_id,
            index_id,
            partition_id,
        )?)?
        .as_deref(),
    )?;
    Ok((sequence != 0).then_some(u128::from(sequence)))
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

fn watch_head_key(
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_INDEX_ROWS,
        TABLE_STREAM_RECORD_INDEX_ROW,
        &core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("index-partition-watch-head"),
            CoreMetaTuplePart::I64(tenant_id),
            CoreMetaTuplePart::I64(bucket_id),
            CoreMetaTuplePart::Utf8(index_id),
            CoreMetaTuplePart::Utf8(partition_id),
        ])?,
    )
}

fn watch_event_prefix(
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("index-partition-watch-event"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::Utf8(index_id),
        CoreMetaTuplePart::Utf8(partition_id),
    ])
}

fn watch_event_key(
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
    sequence: u64,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_INDEX_ROWS,
        TABLE_STREAM_RECORD_INDEX_ROW,
        &core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("index-partition-watch-event"),
            CoreMetaTuplePart::I64(tenant_id),
            CoreMetaTuplePart::I64(bucket_id),
            CoreMetaTuplePart::Utf8(index_id),
            CoreMetaTuplePart::Utf8(partition_id),
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
        .map_err(|_| anyhow!("index partition watch head has invalid length"))?;
    Ok(u64::from_be_bytes(bytes))
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
