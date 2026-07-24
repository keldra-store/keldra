use super::{ObjectWatchEvent, object_watch_stream_id};
use crate::core_store::{
    CF_OBJECT_VERSIONS, CoreMetaRowCommonProto, CoreMetaTuplePart, CoreMetaVisibilityState,
    CoreMutationOperation, CoreMutationPrecondition, CoreStore, TABLE_OBJECT_WATCH_CURSOR_ROW,
    core_meta_root_key_hash, core_meta_tuple_key, decode_deterministic_proto,
    encode_deterministic_proto,
};
use anyhow::{Result, anyhow};
use prost::Message;

const OBJECT_WATCH_CURSOR_SCHEMA: &str = "anvil.core.object_watch_cursor.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ObjectWatchCursorProjection {
    pub sequence: u64,
    pub mutation_id: uuid::Uuid,
    pub event_type: String,
}

#[derive(Clone, PartialEq, Message)]
struct ObjectWatchCursorRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(bytes, tag = "5")]
    version_id: Vec<u8>,
    #[prost(string, tag = "6")]
    stream_id: String,
    #[prost(uint64, tag = "7")]
    sequence: u64,
    #[prost(bytes, tag = "8")]
    mutation_id: Vec<u8>,
    #[prost(string, tag = "9")]
    event_type: String,
}

pub(super) fn read_object_watch_cursor(
    store: &CoreStore,
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
) -> Result<Option<ObjectWatchCursorProjection>> {
    let tuple_key = object_watch_cursor_key(tenant_id, bucket_id, version_id)?;
    read_object_watch_cursor_at_key(store, tenant_id, bucket_id, version_id, &tuple_key)
}

pub(super) fn read_exact_object_watch_cursor(
    store: &CoreStore,
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
    mutation_id: uuid::Uuid,
) -> Result<Option<ObjectWatchCursorProjection>> {
    let projection = read_object_watch_cursor_at_key(
        store,
        tenant_id,
        bucket_id,
        version_id,
        &exact_object_watch_cursor_key(tenant_id, bucket_id, version_id, mutation_id)?,
    )?;
    if projection
        .as_ref()
        .is_some_and(|projection| projection.mutation_id != mutation_id)
    {
        return Err(anyhow!(
            "exact object watch cursor projection mutation mismatch"
        ));
    }
    Ok(projection)
}

fn read_object_watch_cursor_at_key(
    store: &CoreStore,
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
    tuple_key: &[u8],
) -> Result<Option<ObjectWatchCursorProjection>> {
    let Some(payload) =
        store.read_coremeta_row(CF_OBJECT_VERSIONS, TABLE_OBJECT_WATCH_CURSOR_ROW, tuple_key)?
    else {
        return Ok(None);
    };
    decode_cursor_projection(&payload, tenant_id, bucket_id, version_id)
}

fn decode_cursor_projection(
    payload: &[u8],
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
) -> Result<Option<ObjectWatchCursorProjection>> {
    let row = decode_deterministic_proto::<ObjectWatchCursorRowProto>(
        payload,
        "object watch cursor projection",
    )?;
    let common = row
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("object watch cursor projection is missing CoreMeta common"))?;
    if row.schema != OBJECT_WATCH_CURSOR_SCHEMA {
        return Err(anyhow!("object watch cursor projection schema mismatch"));
    }
    let root_anchor = hex::encode(crate::metadata_journal::object_metadata_partition_id(
        tenant_id, bucket_id,
    ));
    if common.realm_id != format!("tenant/{tenant_id}")
        || common.root_key_hash != core_meta_root_key_hash(&root_anchor)
        || common.root_generation == 0
        || common.transaction_id.is_empty()
        || common.visibility_state_enum() != CoreMetaVisibilityState::Committed
    {
        return Err(anyhow!(
            "object watch cursor projection CoreMeta scope mismatch"
        ));
    }
    if row.tenant_id != tenant_id
        || row.bucket_id != bucket_id
        || row.version_id.as_slice() != version_id.as_bytes()
        || row.stream_id != object_watch_stream_id(tenant_id, bucket_id)
    {
        return Err(anyhow!("object watch cursor projection scope mismatch"));
    }
    if row.sequence == 0 {
        return Err(anyhow!("object watch cursor projection has zero sequence"));
    }
    let mutation_id = uuid::Uuid::from_slice(&row.mutation_id)
        .map_err(|error| anyhow!("object watch cursor mutation id is invalid: {error}"))?;
    if row.event_type.is_empty() {
        return Err(anyhow!(
            "object watch cursor projection has empty event type"
        ));
    }
    Ok(Some(ObjectWatchCursorProjection {
        sequence: row.sequence,
        mutation_id,
        event_type: row.event_type,
    }))
}

pub(super) fn absent_precondition(
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
    mutation_id: uuid::Uuid,
) -> Result<CoreMutationPrecondition> {
    Ok(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_OBJECT_VERSIONS.to_string(),
        table_id: TABLE_OBJECT_WATCH_CURSOR_ROW,
        tuple_key: exact_object_watch_cursor_key(tenant_id, bucket_id, version_id, mutation_id)?,
        expected_payload_hash: None,
        require_absent: true,
        require_present: false,
    })
}

pub(super) fn put_operations(
    event: &ObjectWatchEvent,
    sequence: u64,
    partition_id: &str,
    common: CoreMetaRowCommonProto,
) -> Result<Vec<CoreMutationOperation>> {
    let version_id = event
        .version_id
        .ok_or_else(|| anyhow!("object watch cursor projection requires a version id"))?;
    let stream_id = object_watch_stream_id(event.tenant_id, event.bucket_id);
    let payload = encode_deterministic_proto(&ObjectWatchCursorRowProto {
        common: Some(common),
        schema: OBJECT_WATCH_CURSOR_SCHEMA.to_string(),
        tenant_id: event.tenant_id,
        bucket_id: event.bucket_id,
        version_id: version_id.as_bytes().to_vec(),
        stream_id,
        sequence,
        mutation_id: event.mutation_id.as_bytes().to_vec(),
        event_type: event.event_type.clone(),
    });
    let latest_key = object_watch_cursor_key(event.tenant_id, event.bucket_id, version_id)?;
    let exact_key = exact_object_watch_cursor_key(
        event.tenant_id,
        event.bucket_id,
        version_id,
        event.mutation_id,
    )?;
    Ok(vec![
        CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.to_string(),
            cf: CF_OBJECT_VERSIONS.to_string(),
            table_id: TABLE_OBJECT_WATCH_CURSOR_ROW,
            tuple_key: latest_key,
            payload: payload.clone(),
        },
        CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.to_string(),
            cf: CF_OBJECT_VERSIONS.to_string(),
            table_id: TABLE_OBJECT_WATCH_CURSOR_ROW,
            tuple_key: exact_key,
            payload,
        },
    ])
}

fn exact_object_watch_cursor_key(
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
    mutation_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("object-watch-cursor-exact"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::Raw(version_id.as_bytes()),
        CoreMetaTuplePart::Raw(mutation_id.as_bytes()),
    ])
}

pub(super) fn object_watch_cursor_key(
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("object-watch-cursor"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::Raw(version_id.as_bytes()),
    ])
}
