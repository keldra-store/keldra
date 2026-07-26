use crate::{
    core_store::{
        TABLE_OBJECT_WATCH_CURSOR_ROW, TABLE_STREAM_HEAD_ROW, TABLE_STREAM_RECORD_INDEX_ROW,
    },
    formats::{hash32, watch::WatchRecord},
    mvcc_bootstrap::MvccSubsystem,
    mvcc_product::{ProductMutation, stream_logical_key},
    mvcc_transaction::{LogicalKey, PredicateKind},
    persistence::{Bucket, Object, ObjectWatchEvent},
};
use anyhow::{Context, Result, anyhow, bail};
use prost::Message;
use serde::{Deserialize, Serialize};

const OBJECT_WATCH_PARTITION_FAMILY: u16 = 1;
const OBJECT_WATCH_RECORD_KIND: u16 = 1;
const OBJECT_WATCH_PAGE_MAX: usize = 1_000;
const OBJECT_WATCH_HEAD_SCHEMA: &str = "anvil.object-watch.head.v2";
const OBJECT_WATCH_EVENT_SCHEMA: &str = "anvil.object-watch.event.v2";
const OBJECT_WATCH_RECEIPT_SCHEMA: &str = "anvil.object-watch.receipt.v2";

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectWatchHead {
    schema: String,
    last_sequence: u64,
    last_event_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectWatchEnvelope {
    schema: String,
    sequence: u64,
    previous_event_hash: String,
    event_hash: String,
    mutation_id: String,
    payload_ref: String,
    record: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectWatchReceiptRow {
    schema: String,
    stream_id: String,
    sequence: u64,
    mutation_id: String,
    event_type: String,
    event_hash: String,
}

#[derive(Debug, Clone)]
pub struct ObjectWatchEventPage {
    pub events: Vec<ObjectWatchEvent>,
    pub next_cursor: i64,
    pub has_more: bool,
    /// The fixed MVCC snapshot used for both the head and event rows.
    pub snapshot_version: u64,
}

#[derive(Debug)]
pub(crate) struct ObjectWatchPlan {
    pub mutations: Vec<ProductMutation>,
    pub predicates: Vec<(LogicalKey, PredicateKind)>,
}

/// Plan one or more contiguous watch records against the same fixed transaction
/// snapshot as the object mutation. The returned writes must be staged in that
/// transaction; publishing them separately would violate watch atomicity.
pub(crate) fn plan_object_watch_appends(
    mvcc: &MvccSubsystem,
    bucket: &Bucket,
    events: &[(&Object, &ObjectWatchEvent)],
    transaction: Option<(&str, &str)>,
) -> Result<ObjectWatchPlan> {
    if events.is_empty() {
        bail!("object watch plan requires at least one event");
    }
    let stream_id = object_watch_stream_id(bucket.tenant_id, bucket.id);
    let head_key = stream_logical_key(TABLE_STREAM_HEAD_ROW, &stream_id, None)?;
    let snapshot = transaction
        .map(|(transaction_id, _)| {
            mvcc.open_transactions
                .handle(transaction_id)
                .map(|handle| handle.snapshot_version)
        })
        .transpose()?
        .unwrap_or(mvcc.runtime.applied_version()?);
    let base_head = mvcc
        .runtime
        .read_at(&head_key, snapshot)?
        .map(|row| row.value);
    let visible_head = if let Some((transaction_id, principal)) = transaction {
        mvcc.read_transaction_value(transaction_id, principal, &head_key)?
    } else {
        base_head.clone()
    };
    let mut head = visible_head
        .as_deref()
        .map(decode_head)
        .transpose()?
        .unwrap_or(ObjectWatchHead {
            schema: OBJECT_WATCH_HEAD_SCHEMA.to_string(),
            last_sequence: 0,
            last_event_hash: String::new(),
        });
    let mut mutations = Vec::with_capacity(events.len() * 3 + 1);
    let mut predicates = vec![(
        head_key.clone(),
        predicate_for_observed(base_head.as_deref()),
    )];
    for (object, event) in events {
        validate_event_scope(bucket, object, event)?;
        head.last_sequence = head
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("object watch sequence overflow"))?;
        let record = object_watch_record(bucket, object, event, head.last_sequence)?.encode();
        let payload_ref = format!("inline:sha256:{}", hex::encode(hash32(&record)));
        let event_hash = watch_event_hash(
            head.last_sequence,
            &head.last_event_hash,
            object.mutation_id,
            &payload_ref,
        );
        let envelope = ObjectWatchEnvelope {
            schema: OBJECT_WATCH_EVENT_SCHEMA.to_string(),
            sequence: head.last_sequence,
            previous_event_hash: head.last_event_hash.clone(),
            event_hash: event_hash.clone(),
            mutation_id: object.mutation_id.to_string(),
            payload_ref,
            record,
        };
        let event_key = stream_logical_key(
            TABLE_STREAM_RECORD_INDEX_ROW,
            &stream_id,
            Some(head.last_sequence),
        )?;
        predicates.push((event_key.clone(), PredicateKind::Absent));
        mutations.push(ProductMutation::put(
            event_key,
            serde_json::to_vec(&envelope)?,
        ));
        let receipt = ObjectWatchReceiptRow {
            schema: OBJECT_WATCH_RECEIPT_SCHEMA.to_string(),
            stream_id: stream_id.clone(),
            sequence: head.last_sequence,
            mutation_id: object.mutation_id.to_string(),
            event_type: event.event_type.clone(),
            event_hash: event_hash.clone(),
        };
        let receipt_payload = serde_json::to_vec(&receipt)?;
        mutations.push(ProductMutation::put(
            latest_receipt_key(bucket.tenant_id, bucket.id, object.version_id)?,
            receipt_payload.clone(),
        ));
        let exact_key = exact_receipt_key(
            bucket.tenant_id,
            bucket.id,
            object.version_id,
            object.mutation_id,
        )?;
        predicates.push((exact_key.clone(), PredicateKind::Absent));
        mutations.push(ProductMutation::put(exact_key, receipt_payload));
        head.last_event_hash = event_hash;
    }
    mutations.push(ProductMutation::put(head_key, serde_json::to_vec(&head)?));
    Ok(ObjectWatchPlan {
        mutations,
        predicates,
    })
}

pub(crate) fn committed_object_watch_receipt(
    mvcc: &MvccSubsystem,
    bucket: &Bucket,
    object: &Object,
    event: &ObjectWatchEvent,
) -> Result<crate::core_store::StreamAppendReceipt> {
    committed_object_watch_receipt_at_snapshot(
        mvcc,
        bucket,
        object,
        event,
        mvcc.runtime.applied_version()?,
    )
}

pub(crate) fn committed_object_watch_receipt_at_snapshot(
    mvcc: &MvccSubsystem,
    bucket: &Bucket,
    object: &Object,
    event: &ObjectWatchEvent,
    snapshot: u64,
) -> Result<crate::core_store::StreamAppendReceipt> {
    validate_event_scope(bucket, object, event)?;
    let key = exact_receipt_key(
        bucket.tenant_id,
        bucket.id,
        object.version_id,
        object.mutation_id,
    )?;
    let row = mvcc
        .runtime
        .read_at(&key, snapshot)?
        .ok_or_else(|| anyhow!("object mutation has no atomically committed watch event"))?;
    let receipt = decode_receipt(&row.value)?;
    if receipt.mutation_id != object.mutation_id.to_string()
        || receipt.event_type != event.event_type
    {
        bail!("object watch receipt conflicts with the requested event");
    }
    let event_key = stream_logical_key(
        TABLE_STREAM_RECORD_INDEX_ROW,
        &receipt.stream_id,
        Some(receipt.sequence),
    )?;
    let event_row = mvcc
        .runtime
        .read_at(&event_key, snapshot)?
        .ok_or_else(|| anyhow!("object watch receipt has no immutable event row"))?;
    let envelope = decode_envelope(&event_row.value)?;
    let previous_hash = if receipt.sequence == 1 {
        String::new()
    } else {
        let previous_key = stream_logical_key(
            TABLE_STREAM_RECORD_INDEX_ROW,
            &receipt.stream_id,
            Some(receipt.sequence - 1),
        )?;
        mvcc.runtime
            .read_at(&previous_key, snapshot)?
            .map(|row| decode_envelope(&row.value).map(|previous| previous.event_hash))
            .transpose()?
            .ok_or_else(|| anyhow!("object watch receipt event chain is incomplete"))?
    };
    validate_envelope(&envelope, receipt.sequence, &previous_hash)?;
    if envelope.event_hash != receipt.event_hash {
        bail!("object watch receipt event hash mismatch");
    }
    verify_envelope_event(bucket, object, event, &envelope)?;
    Ok(crate::core_store::StreamAppendReceipt {
        cursor: format!("{}:{:020}", receipt.stream_id, receipt.sequence),
        stream_id: receipt.stream_id,
        sequence: receipt.sequence,
        event_hash: receipt.event_hash,
        idempotent_replay: true,
    })
}

fn verify_envelope_event(
    bucket: &Bucket,
    object: &Object,
    expected: &ObjectWatchEvent,
    envelope: &ObjectWatchEnvelope,
) -> Result<()> {
    let (mut record, used) = WatchRecord::decode(&envelope.record)?;
    if used != envelope.record.len() {
        bail!("object watch MVCC receipt record has trailing bytes");
    }
    record.cursor = u128::from(envelope.sequence);
    let actual = object_watch_event_from_payload(
        bucket.tenant_id,
        bucket.id,
        record.cursor,
        decode_object_watch_payload(&record.payload)?,
    )?;
    if actual.bucket_name != expected.bucket_name
        || actual.key != object.key
        || actual.event_type != expected.event_type
        || actual.version_id != Some(object.version_id)
        || actual.mutation_id != object.mutation_id
        || actual.payload_hash != expected.payload_hash
        || actual.etag != expected.etag
        || actual.size != expected.size
        || actual.is_delete_marker != expected.is_delete_marker
        || actual.created_at != expected.created_at
    {
        bail!("object watch receipt points to a different committed event");
    }
    Ok(())
}

pub fn latest_object_watch_cursor(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
) -> Result<Option<u128>> {
    latest_object_watch_cursor_at_snapshot(
        mvcc,
        tenant_id,
        bucket_id,
        version_id,
        mvcc.runtime.applied_version()?,
    )
}

pub fn latest_object_watch_cursor_at_snapshot(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
    snapshot_version: u64,
) -> Result<Option<u128>> {
    read_receipt_at(
        mvcc,
        &latest_receipt_key(tenant_id, bucket_id, version_id)?,
        snapshot_version,
    )
    .map(|receipt| receipt.map(|row| u128::from(row.sequence)))
}

pub fn exact_object_watch_cursor(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
    mutation_id: uuid::Uuid,
) -> Result<Option<u128>> {
    exact_object_watch_cursor_at_snapshot(
        mvcc,
        tenant_id,
        bucket_id,
        version_id,
        mutation_id,
        mvcc.runtime.applied_version()?,
    )
}

pub fn exact_object_watch_cursor_at_snapshot(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
    mutation_id: uuid::Uuid,
    snapshot_version: u64,
) -> Result<Option<u128>> {
    read_receipt_at(
        mvcc,
        &exact_receipt_key(tenant_id, bucket_id, version_id, mutation_id)?,
        snapshot_version,
    )
    .map(|receipt| receipt.map(|row| u128::from(row.sequence)))
}

pub fn latest_object_watch_stream_cursor(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<u64> {
    let snapshot = mvcc.runtime.applied_version()?;
    let key = stream_logical_key(
        TABLE_STREAM_HEAD_ROW,
        &object_watch_stream_id(tenant_id, bucket_id),
        None,
    )?;
    Ok(mvcc
        .runtime
        .read_at(&key, snapshot)?
        .map(|row| decode_head(&row.value))
        .transpose()?
        .map(|head| head.last_sequence)
        .unwrap_or(0))
}

pub fn list_object_watch_event_page(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    prefix: &str,
    after_cursor: i64,
    limit: usize,
) -> Result<ObjectWatchEventPage> {
    list_object_watch_event_page_at_snapshot(
        mvcc,
        tenant_id,
        bucket_id,
        prefix,
        after_cursor,
        limit,
        mvcc.runtime.applied_version()?,
    )
}

pub fn list_object_watch_event_page_at_snapshot(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    prefix: &str,
    after_cursor: i64,
    limit: usize,
    snapshot: u64,
) -> Result<ObjectWatchEventPage> {
    if after_cursor < 0 {
        bail!("object watch cursor must be non-negative");
    }
    if !(1..=OBJECT_WATCH_PAGE_MAX).contains(&limit) {
        bail!("object watch page limit must be between 1 and {OBJECT_WATCH_PAGE_MAX}");
    }
    let after_sequence = u64::try_from(after_cursor)?;
    let stream_id = object_watch_stream_id(tenant_id, bucket_id);
    let head_key = stream_logical_key(TABLE_STREAM_HEAD_ROW, &stream_id, None)?;
    let head = mvcc
        .runtime
        .read_at(&head_key, snapshot)?
        .map(|row| decode_head(&row.value))
        .transpose()?
        .map(|head| head.last_sequence)
        .unwrap_or(0);
    let event_prefix =
        stream_logical_key(TABLE_STREAM_RECORD_INDEX_ROW, &stream_id, None)?.application_key;
    let mut rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_STREAM_RECORD_INDEX_ROW,
        &event_prefix,
        snapshot,
    )?;
    rows.retain(|(key, _)| {
        sequence_from_event_key(key, &event_prefix).is_ok_and(|sequence| sequence > after_sequence)
    });
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let mut events = Vec::with_capacity(rows.len());
    let mut next_sequence = after_sequence;
    let mut expected_previous_hash = if after_sequence == 0 {
        String::new()
    } else {
        let previous_key = stream_logical_key(
            TABLE_STREAM_RECORD_INDEX_ROW,
            &stream_id,
            Some(after_sequence),
        )?;
        mvcc.runtime
            .read_at(&previous_key, snapshot)?
            .map(|row| decode_envelope(&row.value).map(|event| event.event_hash))
            .transpose()?
            .unwrap_or_default()
    };
    for (key, row) in rows {
        let sequence = sequence_from_event_key(&key, &event_prefix)?;
        if sequence != next_sequence.saturating_add(1) {
            bail!("object watch MVCC event sequence is discontinuous");
        }
        let envelope = decode_envelope(&row.value)?;
        validate_envelope(&envelope, sequence, &expected_previous_hash)?;
        let (mut record, used) = WatchRecord::decode(&envelope.record)?;
        if used != envelope.record.len() {
            bail!("object watch MVCC record has trailing bytes");
        }
        record.cursor = u128::from(sequence);
        let payload = decode_object_watch_payload(&record.payload)?;
        if payload.key.starts_with(prefix) {
            events.push(object_watch_event_from_payload(
                tenant_id,
                bucket_id,
                record.cursor,
                payload,
            )?);
        }
        expected_previous_hash = envelope.event_hash;
        next_sequence = sequence;
    }
    Ok(ObjectWatchEventPage {
        events,
        next_cursor: i64::try_from(next_sequence)
            .map_err(|_| anyhow!("object watch cursor exceeds i64"))?,
        has_more: has_more && next_sequence < head,
        snapshot_version: snapshot,
    })
}

fn decode_head(payload: &[u8]) -> Result<ObjectWatchHead> {
    let head: ObjectWatchHead = serde_json::from_slice(payload)?;
    if head.schema != OBJECT_WATCH_HEAD_SCHEMA
        || (head.last_sequence == 0) != head.last_event_hash.is_empty()
    {
        bail!("object watch MVCC head is invalid");
    }
    Ok(head)
}

fn decode_envelope(payload: &[u8]) -> Result<ObjectWatchEnvelope> {
    let event: ObjectWatchEnvelope = serde_json::from_slice(payload)?;
    if event.schema != OBJECT_WATCH_EVENT_SCHEMA {
        bail!("object watch MVCC event schema mismatch");
    }
    Ok(event)
}

fn validate_envelope(
    event: &ObjectWatchEnvelope,
    sequence: u64,
    previous_hash: &str,
) -> Result<()> {
    if event.sequence != sequence
        || event.previous_event_hash != previous_hash
        || event.payload_ref != format!("inline:sha256:{}", hex::encode(hash32(&event.record)))
        || event.event_hash
            != watch_event_hash(
                sequence,
                previous_hash,
                uuid::Uuid::parse_str(&event.mutation_id)?,
                &event.payload_ref,
            )
    {
        bail!("object watch MVCC event hash chain is invalid");
    }
    Ok(())
}

fn decode_receipt(payload: &[u8]) -> Result<ObjectWatchReceiptRow> {
    let receipt: ObjectWatchReceiptRow = serde_json::from_slice(payload)?;
    if receipt.schema != OBJECT_WATCH_RECEIPT_SCHEMA
        || receipt.sequence == 0
        || receipt.stream_id.is_empty()
        || receipt.event_type.is_empty()
    {
        bail!("object watch MVCC receipt is invalid");
    }
    uuid::Uuid::parse_str(&receipt.mutation_id)?;
    Ok(receipt)
}

fn read_receipt_at(
    mvcc: &MvccSubsystem,
    key: &LogicalKey,
    snapshot: u64,
) -> Result<Option<ObjectWatchReceiptRow>> {
    mvcc.runtime
        .read_at(key, snapshot)?
        .map(|row| decode_receipt(&row.value))
        .transpose()
}

fn latest_receipt_key(
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
) -> Result<LogicalKey> {
    receipt_key(tenant_id, bucket_id, version_id, None)
}

fn exact_receipt_key(
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
    mutation_id: uuid::Uuid,
) -> Result<LogicalKey> {
    receipt_key(tenant_id, bucket_id, version_id, Some(mutation_id))
}

fn receipt_key(
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
    mutation_id: Option<uuid::Uuid>,
) -> Result<LogicalKey> {
    let mut stream_id = format!("object-watch-receipt:{tenant_id}:{bucket_id}:{version_id}");
    if let Some(mutation_id) = mutation_id {
        stream_id.push(':');
        stream_id.push_str(&mutation_id.to_string());
    }
    stream_logical_key(TABLE_OBJECT_WATCH_CURSOR_ROW, &stream_id, None)
}

fn sequence_from_event_key(key: &LogicalKey, prefix: &[u8]) -> Result<u64> {
    let suffix = key
        .application_key
        .strip_prefix(prefix)
        .ok_or_else(|| anyhow!("object watch event key has the wrong stream prefix"))?;
    let bytes: [u8; 8] = suffix
        .try_into()
        .map_err(|_| anyhow!("object watch event key has an invalid sequence"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn predicate_for_observed(payload: Option<&[u8]>) -> PredicateKind {
    payload
        .map(|payload| PredicateKind::ValueHash(*blake3::hash(payload).as_bytes()))
        .unwrap_or(PredicateKind::Absent)
}

fn watch_event_hash(
    sequence: u64,
    previous_hash: &str,
    mutation_id: uuid::Uuid,
    payload_ref: &str,
) -> String {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&sequence.to_be_bytes());
    bytes.extend_from_slice(previous_hash.as_bytes());
    bytes.extend_from_slice(mutation_id.as_bytes());
    bytes.extend_from_slice(payload_ref.as_bytes());
    hex::encode(hash32(&bytes))
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
    crate::core_store::encode_deterministic_proto(&ObjectWatchPayloadProto {
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
    let proto = crate::core_store::decode_deterministic_proto::<ObjectWatchPayloadProto>(
        bytes,
        "object watch payload",
    )?;
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

pub(crate) fn object_watch_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("object_watch:tenant:{tenant_id}:bucket:{bucket_id}")
}
