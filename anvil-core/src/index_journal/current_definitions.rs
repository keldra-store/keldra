use super::{IndexCurrentState, event_time_unix_nanos};
use crate::{
    core_store::{
        CF_INDEX_DEFS, CoreMetaRowCommonProto, CoreMetaTuplePart, CoreMetaVisibilityState,
        CoreMutationOperation, CoreMutationPrecondition, CoreStore, TABLE_INDEX_DEFINITION_ROW,
        core_meta_committed_row_common, core_meta_payload_digest, core_meta_record_tuple_key,
        core_meta_root_key_hash, core_meta_tuple_key, decode_deterministic_proto,
        encode_deterministic_proto,
    },
    persistence::IndexDefinitionEvent,
    storage::Storage,
};
use anyhow::{Result, anyhow, bail};
use prost::Message;

const CURRENT_ROW_SCHEMA: &str = "anvil.coremeta.index_definition_current.v1";
const STATE_ROW_SCHEMA: &str = "anvil.coremeta.index_definition_state.v1";
const CURRENT_ROW_KIND: &str = "index_definition_current";
const ENABLED_ROW_KIND: &str = "index_definition_enabled";
const STATE_ROW_KIND: &str = "index_definition_state";
const PAGE_SIZE_MAX: usize = 1_000;

#[derive(Debug, Clone)]
pub(super) struct CurrentDefinitionRecord {
    pub(super) tenant_id: i64,
    pub(super) bucket_id: i64,
    pub(super) index_name: String,
    pub(super) cursor: i64,
    pub(super) index_version: i64,
    pub(super) event_payload: Vec<u8>,
    pub(super) updated_at_unix_nanos: u64,
}

#[derive(Debug)]
pub(super) struct CurrentDefinitionPage {
    pub(super) records: Vec<CurrentDefinitionRecord>,
    pub(super) next_tuple_key: Option<Vec<u8>>,
    #[cfg(test)]
    pub(super) rows_visited: usize,
}

#[derive(Debug)]
pub(super) struct ProjectionMutation {
    pub(super) precondition: CoreMutationPrecondition,
    pub(super) operations: Vec<CoreMutationOperation>,
}

#[derive(Clone, PartialEq, Message)]
struct CurrentDefinitionRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(string, tag = "5")]
    index_name: String,
    #[prost(bool, tag = "6")]
    deleted: bool,
    #[prost(int64, tag = "7")]
    cursor: i64,
    #[prost(int64, tag = "8")]
    index_version: i64,
    #[prost(bytes, tag = "9")]
    event_payload: Vec<u8>,
    #[prost(uint64, tag = "10")]
    updated_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CurrentDefinitionStateProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(int64, tag = "5")]
    latest_cursor: i64,
    #[prost(int64, tag = "6")]
    max_index_id: i64,
    #[prost(uint64, tag = "7")]
    updated_at_unix_nanos: u64,
}

pub(super) async fn prepare_projection_mutation(
    storage: &Storage,
    event: &IndexDefinitionEvent,
    event_payload: &[u8],
    partition_id: &str,
    transaction_id: &str,
) -> Result<ProjectionMutation> {
    validate_scope(event.tenant_id, event.bucket_id)?;
    validate_index_name(&event.index_name)?;
    if event.id <= 0 {
        bail!("index definition projection cursor must be positive");
    }
    if event.index_version < 0 {
        bail!("index definition projection version must not be negative");
    }
    if event.index_id < 0 {
        bail!("index definition projection id must not be negative");
    }
    if event_payload.is_empty() {
        bail!("index definition projection must carry an event payload");
    }
    if partition_id.is_empty() || transaction_id.is_empty() {
        bail!("index definition projection mutation scope must not be empty");
    }
    let enabled = event
        .definition
        .get("enabled")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| anyhow!("index definition projection is missing enabled state"))?;

    let store = CoreStore::new(storage.clone()).await?;
    let state_key = state_tuple_key(event.tenant_id, event.bucket_id)?;
    let existing_payload =
        store.read_coremeta_row(CF_INDEX_DEFS, TABLE_INDEX_DEFINITION_ROW, &state_key)?;
    let existing = existing_payload
        .as_deref()
        .map(|payload| decode_state(payload, event.tenant_id, event.bucket_id))
        .transpose()?;
    let previous_cursor = existing
        .map(|state| state.latest_cursor)
        .unwrap_or_default();
    let next_cursor = previous_cursor
        .checked_add(1)
        .ok_or_else(|| anyhow!("index definition projection cursor overflow"))?;
    if event.id != next_cursor {
        bail!(
            "index definition projection cursor {} is not next after {}",
            event.id,
            previous_cursor
        );
    }
    let state = IndexCurrentState {
        latest_cursor: event.id,
        max_index_id: existing
            .map(|state| state.max_index_id)
            .unwrap_or_default()
            .max(event.index_id),
    };

    let updated_at_unix_nanos = event_time_unix_nanos(event.created_at)?;
    let common = projection_common(
        event.tenant_id,
        event.bucket_id,
        event.id,
        transaction_id,
        updated_at_unix_nanos,
    )?;
    let current_key = current_tuple_key(event.tenant_id, event.bucket_id, &event.index_name)?;
    let enabled_key = enabled_tuple_key(event.tenant_id, event.bucket_id, &event.index_name)?;
    let retained = event.event_type != "drop";
    let current_payload = retained.then(|| {
        encode_current_record(
            &CurrentDefinitionRecord {
                tenant_id: event.tenant_id,
                bucket_id: event.bucket_id,
                index_name: event.index_name.clone(),
                cursor: event.id,
                index_version: event.index_version,
                event_payload: event_payload.to_vec(),
                updated_at_unix_nanos,
            },
            common.clone(),
        )
    });
    let enabled_payload = if retained && enabled {
        Some(
            current_payload
                .clone()
                .ok_or_else(|| anyhow!("enabled index definition is missing current payload"))?,
        )
    } else {
        None
    };
    let state_payload = encode_state(
        event.tenant_id,
        event.bucket_id,
        state,
        updated_at_unix_nanos,
        common.clone(),
    );
    let current_op = if let Some(payload) = current_payload {
        CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.to_string(),
            cf: CF_INDEX_DEFS.to_string(),
            table_id: TABLE_INDEX_DEFINITION_ROW,
            tuple_key: current_key,
            payload,
        }
    } else {
        CoreMutationOperation::CoreMetaDelete {
            partition_id: partition_id.to_string(),
            cf: CF_INDEX_DEFS.to_string(),
            table_id: TABLE_INDEX_DEFINITION_ROW,
            tuple_key: current_key,
        }
    };
    let enabled_op = if let Some(payload) = enabled_payload {
        CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.to_string(),
            cf: CF_INDEX_DEFS.to_string(),
            table_id: TABLE_INDEX_DEFINITION_ROW,
            tuple_key: enabled_key,
            payload,
        }
    } else {
        CoreMutationOperation::CoreMetaDelete {
            partition_id: partition_id.to_string(),
            cf: CF_INDEX_DEFS.to_string(),
            table_id: TABLE_INDEX_DEFINITION_ROW,
            tuple_key: enabled_key,
        }
    };
    let state_op = CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.to_string(),
        cf: CF_INDEX_DEFS.to_string(),
        table_id: TABLE_INDEX_DEFINITION_ROW,
        tuple_key: state_key.clone(),
        payload: state_payload,
    };
    let precondition = CoreMutationPrecondition::CoreMetaRow {
        cf: CF_INDEX_DEFS.to_string(),
        table_id: TABLE_INDEX_DEFINITION_ROW,
        tuple_key: state_key,
        expected_payload_hash: existing_payload
            .as_deref()
            .map(|payload| core_meta_payload_digest(TABLE_INDEX_DEFINITION_ROW, payload)),
        require_absent: existing_payload.is_none(),
        require_present: existing_payload.is_some(),
    };
    Ok(ProjectionMutation {
        precondition,
        operations: vec![current_op, enabled_op, state_op],
    })
}

pub(super) async fn read_current(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    index_name: &str,
) -> Result<Option<CurrentDefinitionRecord>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(payload) = store.read_coremeta_row(
        CF_INDEX_DEFS,
        TABLE_INDEX_DEFINITION_ROW,
        &current_tuple_key(tenant_id, bucket_id, index_name)?,
    )?
    else {
        return Ok(None);
    };
    decode_current_record(&payload, tenant_id, bucket_id).map(Some)
}

pub(super) async fn read_state(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Option<IndexCurrentState>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(payload) = store.read_coremeta_row(
        CF_INDEX_DEFS,
        TABLE_INDEX_DEFINITION_ROW,
        &state_tuple_key(tenant_id, bucket_id)?,
    )?
    else {
        return Ok(None);
    };
    decode_state(&payload, tenant_id, bucket_id).map(Some)
}

pub(super) async fn collection_revision(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<i64> {
    Ok(read_state(storage, tenant_id, bucket_id)
        .await?
        .map(|state| state.latest_cursor)
        .unwrap_or_default())
}

pub(super) async fn page(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    include_disabled: bool,
    expected_revision: i64,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<CurrentDefinitionPage> {
    if !(1..=PAGE_SIZE_MAX).contains(&page_size) {
        bail!("index definition page size must be between 1 and {PAGE_SIZE_MAX}");
    }
    if collection_revision(storage, tenant_id, bucket_id).await? != expected_revision {
        bail!("index definition collection revision changed");
    }

    let store = CoreStore::new(storage.clone()).await?;
    let prefix = definition_tuple_prefix(
        if include_disabled {
            CURRENT_ROW_KIND
        } else {
            ENABLED_ROW_KIND
        },
        tenant_id,
        bucket_id,
    )?;
    let mut rows = store.scan_coremeta_prefix_page(
        CF_INDEX_DEFS,
        TABLE_INDEX_DEFINITION_ROW,
        &prefix,
        after_tuple_key,
        page_size + 1,
    )?;
    #[cfg(test)]
    let rows_visited = rows.len();
    let has_more = rows.len() > page_size;
    if has_more {
        rows.truncate(page_size);
    }
    let next_tuple_key = if has_more {
        Some(
            core_meta_record_tuple_key(
                &rows
                    .last()
                    .ok_or_else(|| anyhow!("index definition continuation has no last row"))?
                    .key,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let records = rows
        .into_iter()
        .map(|row| decode_current_record(&row.payload, tenant_id, bucket_id))
        .collect::<Result<Vec<_>>>()?;
    if collection_revision(storage, tenant_id, bucket_id).await? != expected_revision {
        bail!("index definition collection changed during page read");
    }
    Ok(CurrentDefinitionPage {
        records,
        next_tuple_key,
        #[cfg(test)]
        rows_visited,
    })
}

fn encode_current_record(
    record: &CurrentDefinitionRecord,
    common: CoreMetaRowCommonProto,
) -> Vec<u8> {
    encode_deterministic_proto(&CurrentDefinitionRecordProto {
        common: Some(common),
        schema: CURRENT_ROW_SCHEMA.to_string(),
        tenant_id: record.tenant_id,
        bucket_id: record.bucket_id,
        index_name: record.index_name.clone(),
        deleted: false,
        cursor: record.cursor,
        index_version: record.index_version,
        event_payload: record.event_payload.clone(),
        updated_at_unix_nanos: record.updated_at_unix_nanos,
    })
}

fn decode_current_record(
    payload: &[u8],
    tenant_id: i64,
    bucket_id: i64,
) -> Result<CurrentDefinitionRecord> {
    let proto = decode_deterministic_proto::<CurrentDefinitionRecordProto>(
        payload,
        "index definition current row",
    )?;
    if proto.schema != CURRENT_ROW_SCHEMA
        || proto.tenant_id != tenant_id
        || proto.bucket_id != bucket_id
        || proto.deleted
        || proto.cursor <= 0
        || proto.index_version < 0
        || proto.event_payload.is_empty()
    {
        bail!("CoreMeta index definition current row is invalid");
    }
    validate_index_name(&proto.index_name)?;
    validate_common(
        proto
            .common
            .as_ref()
            .ok_or_else(|| anyhow!("index definition current row is missing common fields"))?,
        tenant_id,
        bucket_id,
        proto.updated_at_unix_nanos,
    )?;
    Ok(CurrentDefinitionRecord {
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        index_name: proto.index_name,
        cursor: proto.cursor,
        index_version: proto.index_version,
        event_payload: proto.event_payload,
        updated_at_unix_nanos: proto.updated_at_unix_nanos,
    })
}

fn encode_state(
    tenant_id: i64,
    bucket_id: i64,
    state: IndexCurrentState,
    updated_at_unix_nanos: u64,
    common: CoreMetaRowCommonProto,
) -> Vec<u8> {
    encode_deterministic_proto(&CurrentDefinitionStateProto {
        common: Some(common),
        schema: STATE_ROW_SCHEMA.to_string(),
        tenant_id,
        bucket_id,
        latest_cursor: state.latest_cursor,
        max_index_id: state.max_index_id,
        updated_at_unix_nanos,
    })
}

fn decode_state(payload: &[u8], tenant_id: i64, bucket_id: i64) -> Result<IndexCurrentState> {
    let proto = decode_deterministic_proto::<CurrentDefinitionStateProto>(
        payload,
        "index definition state row",
    )?;
    if proto.schema != STATE_ROW_SCHEMA
        || proto.tenant_id != tenant_id
        || proto.bucket_id != bucket_id
        || proto.latest_cursor <= 0
        || proto.max_index_id < 0
    {
        bail!("CoreMeta index definition state row is invalid");
    }
    validate_common(
        proto
            .common
            .as_ref()
            .ok_or_else(|| anyhow!("index definition state row is missing common fields"))?,
        tenant_id,
        bucket_id,
        proto.updated_at_unix_nanos,
    )?;
    Ok(IndexCurrentState {
        latest_cursor: proto.latest_cursor,
        max_index_id: proto.max_index_id,
    })
}

fn projection_common(
    tenant_id: i64,
    bucket_id: i64,
    cursor: i64,
    transaction_id: &str,
    updated_at_unix_nanos: u64,
) -> Result<CoreMetaRowCommonProto> {
    Ok(core_meta_committed_row_common(
        format!("tenant:{tenant_id}"),
        projection_root_key_hash(tenant_id, bucket_id),
        u64::try_from(cursor).map_err(|_| anyhow!("index definition cursor is negative"))?,
        transaction_id,
        updated_at_unix_nanos,
    ))
}

fn validate_common(
    common: &CoreMetaRowCommonProto,
    tenant_id: i64,
    bucket_id: i64,
    updated_at_unix_nanos: u64,
) -> Result<()> {
    if common.realm_id != format!("tenant:{tenant_id}")
        || common.root_key_hash != projection_root_key_hash(tenant_id, bucket_id)
        || common.root_generation == 0
        || common.transaction_id.trim().is_empty()
        || common.visibility_state_enum() != CoreMetaVisibilityState::Committed
        || common.created_at_unix_nanos != updated_at_unix_nanos
        || common.payload_schema_version != 1
    {
        bail!("CoreMeta index definition row common fields are invalid");
    }
    Ok(())
}

fn definition_tuple_prefix(kind: &'static str, tenant_id: i64, bucket_id: i64) -> Result<Vec<u8>> {
    validate_scope(tenant_id, bucket_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(kind),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
    ])
}

fn current_tuple_key(tenant_id: i64, bucket_id: i64, index_name: &str) -> Result<Vec<u8>> {
    definition_tuple_key(CURRENT_ROW_KIND, tenant_id, bucket_id, index_name)
}

fn enabled_tuple_key(tenant_id: i64, bucket_id: i64, index_name: &str) -> Result<Vec<u8>> {
    definition_tuple_key(ENABLED_ROW_KIND, tenant_id, bucket_id, index_name)
}

fn definition_tuple_key(
    kind: &'static str,
    tenant_id: i64,
    bucket_id: i64,
    index_name: &str,
) -> Result<Vec<u8>> {
    validate_index_name(index_name)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(kind),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::Utf8(index_name),
    ])
}

fn state_tuple_key(tenant_id: i64, bucket_id: i64) -> Result<Vec<u8>> {
    validate_scope(tenant_id, bucket_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(STATE_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
    ])
}

fn projection_root_key_hash(tenant_id: i64, bucket_id: i64) -> String {
    core_meta_root_key_hash(&projection_root_anchor_key(tenant_id, bucket_id))
}

pub(super) fn projection_root_anchor_key(tenant_id: i64, bucket_id: i64) -> String {
    format!("tenant/{tenant_id}/bucket/{bucket_id}/index_definition")
}

fn validate_scope(tenant_id: i64, bucket_id: i64) -> Result<()> {
    if tenant_id < 0 || bucket_id < 0 {
        bail!("index definition scope ids must not be negative");
    }
    Ok(())
}

fn validate_index_name(index_name: &str) -> Result<()> {
    if index_name.trim().is_empty() || index_name.chars().any(char::is_control) {
        bail!("index definition name is invalid");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_common_accepts_independent_physical_root_generation() {
        let cursor = 3;
        let updated_at = 17;
        let mut common = projection_common(42, 7, cursor, "tx-index", updated_at).unwrap();
        common.root_generation = 91;

        validate_common(&common, 42, 7, updated_at).unwrap();
        assert_ne!(common.root_generation, cursor as u64);
    }
}
