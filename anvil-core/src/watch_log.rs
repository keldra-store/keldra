mod projection;

#[cfg(test)]
mod tests;

use crate::core_store::{
    CoreMetaRowCommonProto, CoreMutationOperation, CoreMutationPrecondition, CoreStore,
    CoreTransaction, ReadStream, StreamAppendReceipt, core_meta_committed_row_common,
    decode_deterministic_proto, encode_deterministic_proto,
};
use crate::formats::{hash32, watch::WatchRecord};
use crate::persistence::{Bucket, Object, ObjectWatchEvent};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow, bail};
use prost::Message;

const OBJECT_WATCH_PARTITION_FAMILY: u16 = 1;
const OBJECT_WATCH_RECORD_KIND: u16 = 1;
const OBJECT_WATCH_PAGE_MAX: usize = 1_000;

#[derive(Debug, Clone)]
struct ObjectWatchPayload {
    bucket_name: String,
    key: String,
    event_type: String,
    version_id: Option<String>,
    mutation_id: Option<String>,
    payload_hash: Option<String>,
    etag: Option<String>,
    size: i64,
    is_delete_marker: bool,
    emitted_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct ObjectWatchPayloadProto {
    #[prost(string, tag = "1")]
    bucket_name: String,
    #[prost(string, tag = "2")]
    key: String,
    #[prost(string, tag = "3")]
    event_type: String,
    #[prost(string, optional, tag = "4")]
    version_id: Option<String>,
    #[prost(string, optional, tag = "5")]
    mutation_id: Option<String>,
    #[prost(string, optional, tag = "6")]
    payload_hash: Option<String>,
    #[prost(string, optional, tag = "7")]
    etag: Option<String>,
    #[prost(int64, tag = "8")]
    size: i64,
    #[prost(bool, tag = "9")]
    is_delete_marker: bool,
    #[prost(string, tag = "10")]
    emitted_at: String,
}

#[derive(Debug, Clone)]
pub struct ObjectWatchEventPage {
    pub events: Vec<ObjectWatchEvent>,
    pub next_cursor: i64,
    pub has_more: bool,
}

pub(crate) struct PreparedObjectWatchAppend {
    pub(crate) stream_id: String,
    pub(crate) preconditions: Vec<CoreMutationPrecondition>,
    pub(crate) operations: Vec<CoreMutationOperation>,
}

pub(crate) async fn prepare_object_watch_append(
    core_store: &CoreStore,
    bucket: &Bucket,
    object: &Object,
    event: &ObjectWatchEvent,
    scope_partition: &str,
    projection_root_key_hash: &str,
    projection_root_generation: Option<u64>,
    projection_transaction_id: &str,
    explicit_transaction: Option<&CoreTransaction>,
) -> Result<PreparedObjectWatchAppend> {
    validate_event_scope(bucket, object, event)?;
    let stream_id = object_watch_stream_id(bucket.tenant_id, bucket.id);
    let stream_precondition = core_store
        .stream_head_precondition_visible_to_transaction(&stream_id, explicit_transaction)
        .await?;
    let sequence = next_sequence(&stream_precondition)?;
    prepare_object_watch_append_at_sequence(
        bucket,
        object,
        event,
        scope_partition,
        projection_root_key_hash,
        projection_root_generation,
        projection_transaction_id,
        sequence,
        Some(stream_precondition),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_object_watch_append_at_sequence(
    bucket: &Bucket,
    object: &Object,
    event: &ObjectWatchEvent,
    scope_partition: &str,
    projection_root_key_hash: &str,
    projection_root_generation: Option<u64>,
    projection_transaction_id: &str,
    sequence: u64,
    stream_precondition: Option<CoreMutationPrecondition>,
) -> Result<PreparedObjectWatchAppend> {
    validate_event_scope(bucket, object, event)?;
    let stream_id = object_watch_stream_id(bucket.tenant_id, bucket.id);
    let record = object_watch_record(bucket, object, event, sequence)?;
    let projection_common = object_watch_projection_common(
        event,
        projection_root_generation.unwrap_or(sequence),
        projection_transaction_id,
        projection_root_key_hash,
    )?;
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: scope_partition.to_string(),
        stream_id: stream_id.clone(),
        record_kind: "object_watch".to_string(),
        payload: record.encode(),
        idempotency_key: Some(object_watch_idempotency_key(bucket, event)),
    }];
    operations.extend(projection::put_operations(
        event,
        sequence,
        scope_partition,
        projection_common,
    )?);
    let mut preconditions = stream_precondition.into_iter().collect::<Vec<_>>();
    preconditions.push(projection::absent_precondition(
        bucket.tenant_id,
        bucket.id,
        object.version_id,
        object.mutation_id,
    )?);
    Ok(PreparedObjectWatchAppend {
        stream_id,
        preconditions,
        operations,
    })
}

pub(crate) async fn committed_object_watch_receipt(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    event: &ObjectWatchEvent,
) -> Result<StreamAppendReceipt> {
    validate_event_scope(bucket, object, event)?;
    existing_object_watch_receipt(storage, bucket, object, event)
        .await?
        .ok_or_else(|| anyhow!("object mutation has no atomically committed watch event"))
}

async fn existing_object_watch_receipt(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    event: &ObjectWatchEvent,
) -> Result<Option<StreamAppendReceipt>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(projected) = projection::read_exact_object_watch_cursor(
        &store,
        bucket.tenant_id,
        bucket.id,
        object.version_id,
        object.mutation_id,
    )?
    else {
        return Ok(None);
    };
    if projected.mutation_id != event.mutation_id || projected.event_type != event.event_type {
        bail!("object watch cursor projection conflicts with the requested event");
    }
    let stream_id = object_watch_stream_id(bucket.tenant_id, bucket.id);
    let record = store
        .read_stream(ReadStream {
            stream_id: stream_id.clone(),
            after_sequence: projected.sequence.saturating_sub(1),
            limit: 1,
        })
        .await?
        .into_iter()
        .find(|record| record.sequence == projected.sequence)
        .ok_or_else(|| anyhow!("object watch cursor projection has no committed stream record"))?;
    verify_object_watch_stream_record(bucket, object, event, &record)?;
    Ok(Some(watch_receipt(
        stream_id,
        projected.sequence,
        record.event_hash,
        true,
    )))
}

fn verify_object_watch_stream_record(
    bucket: &Bucket,
    object: &Object,
    event: &ObjectWatchEvent,
    record: &crate::core_store::StreamRecord,
) -> Result<()> {
    if record.record_kind != "object_watch" {
        bail!("object watch cursor points to an unexpected stream record kind");
    }
    let (mut watch_record, used) = WatchRecord::decode(&record.payload)?;
    if used != record.payload.len() {
        bail!("object watch CoreStore record has trailing bytes");
    }
    watch_record.cursor = u128::from(record.sequence);
    let actual = object_watch_event_from_payload(
        bucket.tenant_id,
        bucket.id,
        watch_record.cursor,
        decode_object_watch_payload(&watch_record.payload)?,
    )?;
    if actual.bucket_name != event.bucket_name
        || actual.key != object.key
        || actual.event_type != event.event_type
        || actual.version_id != Some(object.version_id)
        || actual.mutation_id != object.mutation_id
        || actual.payload_hash != event.payload_hash
        || actual.etag != event.etag
        || actual.size != event.size
        || actual.is_delete_marker != event.is_delete_marker
        || actual.created_at != event.created_at
    {
        bail!("object watch cursor points to a different committed event");
    }
    Ok(())
}

fn object_watch_projection_common(
    event: &ObjectWatchEvent,
    root_generation: u64,
    transaction_id: &str,
    root_key_hash: &str,
) -> Result<CoreMetaRowCommonProto> {
    let created_at_unix_nanos = event
        .created_at
        .timestamp_nanos_opt()
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| anyhow!("object watch event timestamp is outside the supported range"))?;
    Ok(core_meta_committed_row_common(
        format!("tenant/{}", event.tenant_id),
        root_key_hash,
        root_generation,
        transaction_id,
        created_at_unix_nanos,
    ))
}

fn watch_receipt(
    stream_id: String,
    sequence: u64,
    event_hash: String,
    idempotent_replay: bool,
) -> StreamAppendReceipt {
    StreamAppendReceipt {
        cursor: format!("{stream_id}:{sequence:020}"),
        stream_id,
        sequence,
        event_hash,
        idempotent_replay,
    }
}

pub async fn latest_object_watch_cursor(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
) -> Result<Option<u128>> {
    let store = CoreStore::new(storage.clone()).await?;
    Ok(
        projection::read_object_watch_cursor(&store, tenant_id, bucket_id, version_id)?
            .map(|row| u128::from(row.sequence)),
    )
}

pub async fn exact_object_watch_cursor(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
    mutation_id: uuid::Uuid,
) -> Result<Option<u128>> {
    let store = CoreStore::new(storage.clone()).await?;
    Ok(projection::read_exact_object_watch_cursor(
        &store,
        tenant_id,
        bucket_id,
        version_id,
        mutation_id,
    )?
    .map(|row| u128::from(row.sequence)))
}

pub async fn latest_object_watch_stream_cursor(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<u64> {
    CoreStore::new(storage.clone())
        .await?
        .stream_head_sequence(&object_watch_stream_id(tenant_id, bucket_id))
        .await
}

pub async fn list_object_watch_event_page(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    prefix: &str,
    after_cursor: i64,
    limit: usize,
) -> Result<ObjectWatchEventPage> {
    if after_cursor < 0 {
        bail!("object watch cursor must be non-negative");
    }
    if !(1..=OBJECT_WATCH_PAGE_MAX).contains(&limit) {
        bail!("object watch page limit must be between 1 and {OBJECT_WATCH_PAGE_MAX}");
    }
    // The limit bounds source rows, not prefix matches. Advancing by the source
    // cursor keeps filtered consumers gap-free without searching unboundedly
    // for enough matching events.
    let core_store = CoreStore::new(storage.clone()).await?;
    let page = core_store
        .read_stream_page(ReadStream {
            stream_id: object_watch_stream_id(tenant_id, bucket_id),
            after_sequence: u64::try_from(after_cursor)?,
            limit,
        })
        .await?;
    let next_cursor = i64::try_from(page.next_sequence)
        .map_err(|_| anyhow!("object watch cursor exceeds i64"))?;
    let mut events = Vec::with_capacity(page.records.len());
    for record in page.records {
        if record.record_kind != "object_watch" {
            bail!("object watch stream contains an unexpected record kind");
        }
        let (mut watch_record, used) = WatchRecord::decode(&record.payload)?;
        if used != record.payload.len() {
            bail!("object watch CoreStore record has trailing bytes");
        }
        watch_record.cursor = u128::from(record.sequence);
        let payload = decode_object_watch_payload(&watch_record.payload)?;
        if payload.key.starts_with(prefix) {
            events.push(object_watch_event_from_payload(
                tenant_id,
                bucket_id,
                watch_record.cursor,
                payload,
            )?);
        }
    }
    Ok(ObjectWatchEventPage {
        events,
        next_cursor,
        has_more: page.has_more,
    })
}

fn validate_event_scope(bucket: &Bucket, object: &Object, event: &ObjectWatchEvent) -> Result<()> {
    if object.tenant_id != bucket.tenant_id
        || object.bucket_id != bucket.id
        || event.tenant_id != bucket.tenant_id
        || event.bucket_id != bucket.id
        || event.bucket_name != bucket.name
        || event.key != object.key
        || event.version_id != Some(object.version_id)
        || event.mutation_id != object.mutation_id
        || event.payload_hash != object.content_hash
        || event.etag.as_deref() != Some(object.etag.as_str())
        || event.size != object.size
        || event.created_at != object.created_at
    {
        bail!("object watch event does not match its bucket and object scope");
    }
    Ok(())
}

fn object_watch_record(
    bucket: &Bucket,
    object: &Object,
    event: &ObjectWatchEvent,
    sequence: u64,
) -> Result<WatchRecord> {
    let payload = encode_object_watch_payload(&ObjectWatchPayload {
        bucket_name: event.bucket_name.clone(),
        key: event.key.clone(),
        event_type: event.event_type.clone(),
        version_id: event.version_id.map(|id| id.to_string()),
        mutation_id: Some(event.mutation_id.to_string()),
        payload_hash: Some(event.payload_hash.clone()),
        etag: event.etag.clone(),
        size: event.size,
        is_delete_marker: event.is_delete_marker,
        emitted_at: event
            .created_at
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    });
    Ok(WatchRecord::new(
        u128::from(sequence),
        OBJECT_WATCH_PARTITION_FAMILY,
        partition_id(bucket.tenant_id, bucket.id),
        *object.mutation_id.as_bytes(),
        OBJECT_WATCH_RECORD_KIND,
        u64::try_from(object.authz_revision)
            .context("object authz revision must be non-negative")?,
        0,
        0,
        payload,
    ))
}

fn partition_id(tenant_id: i64, bucket_id: i64) -> [u8; 32] {
    hash32(format!("tenant:{tenant_id}:bucket:{bucket_id}:watch:object").as_bytes())
}

fn next_sequence(precondition: &CoreMutationPrecondition) -> Result<u64> {
    let CoreMutationPrecondition::StreamHead {
        expected_last_sequence,
        ..
    } = precondition
    else {
        bail!("object watch stream precondition has wrong kind");
    };
    expected_last_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("object watch stream sequence overflow"))
}

fn object_watch_event_from_payload(
    tenant_id: i64,
    bucket_id: i64,
    cursor: u128,
    payload: ObjectWatchPayload,
) -> Result<ObjectWatchEvent> {
    let id = i64::try_from(cursor).map_err(|_| anyhow!("watch cursor exceeds i64"))?;
    let version_id = payload
        .version_id
        .as_deref()
        .map(uuid::Uuid::parse_str)
        .transpose()?;
    let mutation_id = payload
        .mutation_id
        .as_deref()
        .map(uuid::Uuid::parse_str)
        .transpose()?
        .ok_or_else(|| anyhow!("object watch event is missing its mutation id"))?;
    let created_at =
        chrono::DateTime::parse_from_rfc3339(&payload.emitted_at)?.with_timezone(&chrono::Utc);
    Ok(ObjectWatchEvent {
        id,
        tenant_id,
        bucket_id,
        bucket_name: payload.bucket_name,
        key: payload.key,
        event_type: payload.event_type,
        version_id,
        mutation_id,
        payload_hash: payload.payload_hash.unwrap_or_default(),
        etag: payload.etag,
        size: payload.size,
        is_delete_marker: payload.is_delete_marker,
        created_at,
    })
}

fn encode_object_watch_payload(payload: &ObjectWatchPayload) -> Vec<u8> {
    encode_deterministic_proto(&ObjectWatchPayloadProto {
        bucket_name: payload.bucket_name.clone(),
        key: payload.key.clone(),
        event_type: payload.event_type.clone(),
        version_id: payload.version_id.clone(),
        mutation_id: payload.mutation_id.clone(),
        payload_hash: payload.payload_hash.clone(),
        etag: payload.etag.clone(),
        size: payload.size,
        is_delete_marker: payload.is_delete_marker,
        emitted_at: payload.emitted_at.clone(),
    })
}

fn decode_object_watch_payload(bytes: &[u8]) -> Result<ObjectWatchPayload> {
    let proto =
        decode_deterministic_proto::<ObjectWatchPayloadProto>(bytes, "object watch payload")?;
    Ok(ObjectWatchPayload {
        bucket_name: proto.bucket_name,
        key: proto.key,
        event_type: proto.event_type,
        version_id: proto.version_id,
        mutation_id: proto.mutation_id,
        payload_hash: proto.payload_hash,
        etag: proto.etag,
        size: proto.size,
        is_delete_marker: proto.is_delete_marker,
        emitted_at: proto.emitted_at,
    })
}

fn object_watch_idempotency_key(bucket: &Bucket, event: &ObjectWatchEvent) -> String {
    let object_identity = event
        .version_id
        .map(|version_id| version_id.to_string())
        .unwrap_or_else(|| hex::encode(hash32(event.key.as_bytes())));
    format!(
        "object-watch:{}:{}:{}:{object_identity}:{}",
        bucket.tenant_id, bucket.id, event.mutation_id, event.event_type
    )
}

pub(crate) fn object_watch_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("object_watch:tenant:{tenant_id}:bucket:{bucket_id}")
}
