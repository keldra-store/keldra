use crate::core_store::{
    CF_OBSERVABILITY, CoreMetaTuplePart, TABLE_DIAGNOSTIC_ROW, TABLE_STREAM_RECORD_INDEX_ROW,
    core_meta_committed_row_common, core_meta_root_key_hash, core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32, writer::WriterFamily};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::IndexDiagnostic;
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use prost::{Message, Oneof};
use serde_json::Value as JsonValue;

const INDEX_DIAGNOSTIC_BODY_SCHEMA: &str = "anvil.core.index_diagnostic.journal_body.v1";
const INDEX_DIAGNOSTIC_PROJECTION_SCHEMA: &str = "anvil.index.diagnostic_projection.v1";
pub const INDEX_DIAGNOSTIC_PAGE_MAX: usize = 1001;

#[derive(Debug, Clone)]
pub(crate) struct PreparedIndexDiagnostic {
    diagnostic: IndexDiagnostic,
    fence_token: u64,
    mutation_id: uuid::Uuid,
    head_key: crate::mvcc_transaction::LogicalKey,
    head_payload: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDiagnosticBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(message, optional, tag = "2")]
    diagnostic: Option<IndexDiagnosticProto>,
    #[prost(uint64, tag = "3")]
    fence_token: u64,
    #[prost(string, tag = "4")]
    mutation_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDiagnosticProjectionProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    diagnostic: Option<IndexDiagnosticProto>,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDiagnosticProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(int64, tag = "3")]
    bucket_id: i64,
    #[prost(string, tag = "4")]
    bucket_name: String,
    #[prost(int64, optional, tag = "5")]
    index_id: Option<i64>,
    #[prost(string, tag = "6")]
    index_name: String,
    #[prost(string, tag = "7")]
    object_key: String,
    #[prost(string, optional, tag = "8")]
    version_id: Option<String>,
    #[prost(string, tag = "9")]
    severity: String,
    #[prost(string, tag = "10")]
    code: String,
    #[prost(string, tag = "11")]
    message: String,
    #[prost(message, optional, tag = "12")]
    details: Option<IndexDiagnosticJsonValueProto>,
    #[prost(int64, tag = "13")]
    created_at_unix_nanos: i64,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDiagnosticJsonValueProto {
    #[prost(
        oneof = "index_diagnostic_json_value_proto::Kind",
        tags = "1, 2, 3, 4, 5, 6, 7, 8"
    )]
    kind: Option<index_diagnostic_json_value_proto::Kind>,
}

mod index_diagnostic_json_value_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Kind {
        #[prost(bool, tag = "1")]
        Null(bool),
        #[prost(bool, tag = "2")]
        Bool(bool),
        #[prost(int64, tag = "3")]
        I64(i64),
        #[prost(uint64, tag = "4")]
        U64(u64),
        #[prost(double, tag = "5")]
        F64(f64),
        #[prost(string, tag = "6")]
        String(String),
        #[prost(message, tag = "7")]
        Array(super::IndexDiagnosticJsonArrayProto),
        #[prost(message, tag = "8")]
        Object(super::IndexDiagnosticJsonObjectProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct IndexDiagnosticJsonArrayProto {
    #[prost(message, repeated, tag = "1")]
    values: Vec<IndexDiagnosticJsonValueProto>,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDiagnosticJsonObjectProto {
    #[prost(message, repeated, tag = "1")]
    entries: Vec<IndexDiagnosticJsonObjectEntryProto>,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDiagnosticJsonObjectEntryProto {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(message, optional, tag = "2")]
    value: Option<IndexDiagnosticJsonValueProto>,
}

pub(crate) async fn write_index_diagnostic_with_permit(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    diagnostic: IndexDiagnostic,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<IndexDiagnostic> {
    require_index_diagnostic_permit(diagnostic.tenant_id, diagnostic.bucket_id, permit)?;
    let _ = partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    write_index_diagnostic_inner(
        mvcc,
        diagnostic,
        permit.fence_token,
        Vec::new(),
        uuid::Uuid::new_v4(),
    )
    .await
}

pub(crate) async fn prepare_index_diagnostic_for_task(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    mut diagnostic: IndexDiagnostic,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    mutation_id: [u8; 16],
) -> Result<PreparedIndexDiagnostic> {
    require_index_diagnostic_permit(diagnostic.tenant_id, diagnostic.bucket_id, permit)?;
    let _ = partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    let head_key = diagnostic_head_key(diagnostic.tenant_id, diagnostic.bucket_id)?;
    let head_payload = mvcc.read_latest_value(&head_key)?;
    diagnostic.id = i64::try_from(next_diagnostic_sequence(head_payload.as_deref())?)
        .map_err(|_| anyhow!("index diagnostic cursor exceeds i64"))?;
    Ok(PreparedIndexDiagnostic {
        diagnostic,
        // The task lease is an exact MVCC predicate supplied at publication.
        // Keeping an ephemeral mesh fence out of task-produced bytes makes
        // retries stable across an ownership handoff.
        fence_token: 0,
        mutation_id: uuid::Uuid::from_bytes(mutation_id),
        head_key,
        head_payload,
    })
}

pub(crate) async fn publish_prepared_index_diagnostic(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    prepared: PreparedIndexDiagnostic,
    additional_preconditions: &[(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )],
) -> Result<IndexDiagnostic> {
    append_diagnostic_mvcc(
        mvcc,
        &prepared.diagnostic,
        prepared.fence_token,
        additional_preconditions.to_vec(),
        prepared.head_key,
        prepared.head_payload,
        prepared.mutation_id,
    )
    .await?;
    Ok(prepared.diagnostic)
}

async fn write_index_diagnostic_inner(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    mut diagnostic: IndexDiagnostic,
    fence_token: u64,
    additional_preconditions: Vec<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    mutation_id: uuid::Uuid,
) -> Result<IndexDiagnostic> {
    let head_key = diagnostic_head_key(diagnostic.tenant_id, diagnostic.bucket_id)?;
    let head_payload = mvcc.read_latest_value(&head_key)?;
    diagnostic.id = i64::try_from(next_diagnostic_sequence(head_payload.as_deref())?)
        .map_err(|_| anyhow!("index diagnostic cursor exceeds i64"))?;
    append_diagnostic_mvcc(
        mvcc,
        &diagnostic,
        fence_token,
        additional_preconditions,
        head_key,
        head_payload,
        mutation_id,
    )
    .await?;
    Ok(diagnostic)
}

pub async fn read_index_diagnostics(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
    index_name: &str,
    severity: &str,
    after_cursor: i64,
    limit: usize,
) -> Result<Vec<IndexDiagnostic>> {
    if !(1..=INDEX_DIAGNOSTIC_PAGE_MAX).contains(&limit) {
        return Err(anyhow!(
            "index diagnostic page size must be between 1 and {INDEX_DIAGNOSTIC_PAGE_MAX}"
        ));
    }
    let prefix = index_diagnostic_projection_prefix(
        tenant_id,
        bucket_id,
        none_if_empty(index_name),
        none_if_empty(severity),
    )?;
    let after = (after_cursor > 0)
        .then(|| {
            index_diagnostic_projection_key(
                tenant_id,
                bucket_id,
                none_if_empty(index_name),
                none_if_empty(severity),
                u64::try_from(after_cursor)?,
            )
        })
        .transpose()?;
    let snapshot = mvcc.runtime.applied_version()?;
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_OBSERVABILITY, &prefix)?;
    let namespace = crate::mvcc_product::coremeta_application_prefix(CF_OBSERVABILITY, &[])?;
    let mut rows =
        mvcc.runtime
            .scan_table_prefix_at(TABLE_DIAGNOSTIC_ROW, &application_prefix, snapshot)?;
    if let Some(after) = after {
        rows.retain(|(key, _)| {
            key.application_key
                .strip_prefix(&namespace)
                .is_some_and(|tuple| tuple > after.as_slice())
        });
    }
    rows.truncate(limit);
    rows.into_iter()
        .map(|(_, row)| {
            decode_index_diagnostic_projection(
                &row.value,
                tenant_id,
                bucket_id,
                none_if_empty(index_name),
                none_if_empty(severity),
            )
        })
        .collect()
}

pub async fn index_diagnostic_revision(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<String> {
    Ok(decode_diagnostic_head(
        mvcc.read_latest_value(&diagnostic_head_key(tenant_id, bucket_id)?)?
            .as_deref(),
    )?
    .to_string())
}

async fn append_diagnostic_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    diagnostic: &IndexDiagnostic,
    fence_token: u64,
    mut additional_preconditions: Vec<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    head_key: crate::mvcc_transaction::LogicalKey,
    head_payload: Option<Vec<u8>>,
    mutation_id: uuid::Uuid,
) -> Result<()> {
    let stream_id = index_diagnostic_stream_id(diagnostic.tenant_id, diagnostic.bucket_id);
    let payload = encode_index_diagnostic_body(diagnostic, fence_token, mutation_id)?;
    let partition_id = hex::encode(index_diagnostic_partition_id(
        diagnostic.tenant_id,
        diagnostic.bucket_id,
    ));
    let logical_id =
        index_diagnostic_logical_id(diagnostic.tenant_id, diagnostic.bucket_id, mutation_id);
    let transaction_id = logical_id;
    let event_key = diagnostic_event_key(
        diagnostic.tenant_id,
        diagnostic.bucket_id,
        u64::try_from(diagnostic.id)?,
    )?;
    let mut mutations = vec![crate::mvcc_product::ProductMutation::put(
        event_key.clone(),
        payload,
    )];
    let projection = encode_index_diagnostic_projection(
        diagnostic,
        &stream_id,
        u64::try_from(diagnostic.id)?,
        &transaction_id,
    )?;
    for tuple_key in index_diagnostic_projection_keys(diagnostic)? {
        let key = crate::mvcc_product::coremeta_logical_key(
            CF_OBSERVABILITY,
            TABLE_DIAGNOSTIC_ROW,
            &tuple_key,
        )?;
        mutations.push(crate::mvcc_product::ProductMutation::put(
            key.clone(),
            projection.clone(),
        ));
        additional_preconditions.push((key, crate::mvcc_transaction::PredicateKind::Absent));
    }
    mutations.push(crate::mvcc_product::ProductMutation::put(
        head_key.clone(),
        u64::try_from(diagnostic.id)?.to_be_bytes().to_vec(),
    ));
    additional_preconditions.extend([
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
    ]);
    mvcc.autocommit_product_mutations_with_predicates(
        &index_diagnostic_partition_principal(diagnostic.tenant_id, diagnostic.bucket_id),
        &transaction_id,
        mutations,
        additional_preconditions,
        crate::mvcc_transaction::DurabilityLevel::Quorum,
        u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| anyhow!("index diagnostic timestamp predates Unix epoch"))?,
    )
    .await?;
    Ok(())
}

fn index_diagnostic_logical_id(tenant_id: i64, bucket_id: i64, mutation_id: uuid::Uuid) -> String {
    format!("index-diagnostic:{tenant_id}:{bucket_id}:{mutation_id}")
}

pub fn index_diagnostic_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/index_diagnostic").as_bytes())
}

fn index_diagnostic_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("index_diagnostic:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn index_diagnostic_partition_principal(tenant_id: i64, bucket_id: i64) -> String {
    format!("partition-owner:index_diagnostic:{tenant_id}:{bucket_id}")
}

fn diagnostic_head_key(
    tenant_id: i64,
    bucket_id: i64,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_OBSERVABILITY,
        TABLE_STREAM_RECORD_INDEX_ROW,
        &core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("index-diagnostic-head"),
            CoreMetaTuplePart::I64(tenant_id),
            CoreMetaTuplePart::I64(bucket_id),
        ])?,
    )
}

fn diagnostic_event_key(
    tenant_id: i64,
    bucket_id: i64,
    sequence: u64,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_OBSERVABILITY,
        TABLE_STREAM_RECORD_INDEX_ROW,
        &core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("index-diagnostic-event"),
            CoreMetaTuplePart::I64(tenant_id),
            CoreMetaTuplePart::I64(bucket_id),
            CoreMetaTuplePart::U64(sequence),
        ])?,
    )
}

fn decode_diagnostic_head(payload: Option<&[u8]>) -> Result<u64> {
    let Some(payload) = payload else {
        return Ok(0);
    };
    let bytes: [u8; 8] = payload
        .try_into()
        .map_err(|_| anyhow!("index diagnostic head has invalid length"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn next_diagnostic_sequence(payload: Option<&[u8]>) -> Result<u64> {
    decode_diagnostic_head(payload)?
        .checked_add(1)
        .ok_or_else(|| anyhow!("index diagnostic cursor overflow"))
}

fn none_if_empty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn index_diagnostic_projection_keys(diagnostic: &IndexDiagnostic) -> Result<Vec<Vec<u8>>> {
    [
        (None, None),
        (Some(diagnostic.index_name.as_str()), None),
        (None, Some(diagnostic.severity.as_str())),
        (
            Some(diagnostic.index_name.as_str()),
            Some(diagnostic.severity.as_str()),
        ),
    ]
    .into_iter()
    .map(|(index_name, severity)| {
        index_diagnostic_projection_key(
            diagnostic.tenant_id,
            diagnostic.bucket_id,
            index_name,
            severity,
            u64::try_from(diagnostic.id)?,
        )
    })
    .collect()
}

fn index_diagnostic_projection_prefix(
    tenant_id: i64,
    bucket_id: i64,
    index_name: Option<&str>,
    severity: Option<&str>,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&index_diagnostic_projection_parts(
        tenant_id, bucket_id, index_name, severity,
    ))
}

fn index_diagnostic_projection_key(
    tenant_id: i64,
    bucket_id: i64,
    index_name: Option<&str>,
    severity: Option<&str>,
    cursor: u64,
) -> Result<Vec<u8>> {
    let mut parts = index_diagnostic_projection_parts(tenant_id, bucket_id, index_name, severity);
    parts.push(CoreMetaTuplePart::U64(cursor));
    core_meta_tuple_key(&parts)
}

fn index_diagnostic_projection_parts<'a>(
    tenant_id: i64,
    bucket_id: i64,
    index_name: Option<&'a str>,
    severity: Option<&'a str>,
) -> Vec<CoreMetaTuplePart<'a>> {
    let mask = u64::from(index_name.is_some()) | (u64::from(severity.is_some()) << 1);
    let mut parts = vec![
        CoreMetaTuplePart::Utf8("index-diagnostic"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::U64(mask),
    ];
    if let Some(index_name) = index_name {
        parts.push(CoreMetaTuplePart::Utf8(index_name));
    }
    if let Some(severity) = severity {
        parts.push(CoreMetaTuplePart::Utf8(severity));
    }
    parts
}

fn encode_index_diagnostic_projection(
    diagnostic: &IndexDiagnostic,
    stream_id: &str,
    root_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    encode_deterministic_proto(
        &IndexDiagnosticProjectionProto {
            common: Some(core_meta_committed_row_common(
                "system",
                core_meta_root_key_hash(&index_diagnostic_projection_root_anchor_key(stream_id)),
                root_generation,
                transaction_id,
                root_generation,
            )),
            schema: INDEX_DIAGNOSTIC_PROJECTION_SCHEMA.to_string(),
            diagnostic: Some(index_diagnostic_to_proto(diagnostic)?),
        },
        "index diagnostic projection",
    )
}

fn index_diagnostic_projection_root_anchor_key(stream_id: &str) -> String {
    format!("stream/{stream_id}")
}

fn decode_index_diagnostic_projection(
    bytes: &[u8],
    tenant_id: i64,
    bucket_id: i64,
    index_name: Option<&str>,
    severity: Option<&str>,
) -> Result<IndexDiagnostic> {
    let projection = decode_deterministic_proto::<IndexDiagnosticProjectionProto>(
        bytes,
        "index diagnostic projection",
    )?;
    if projection.common.is_none() || projection.schema != INDEX_DIAGNOSTIC_PROJECTION_SCHEMA {
        return Err(anyhow!("index diagnostic projection schema mismatch"));
    }
    let diagnostic = index_diagnostic_from_proto(
        projection
            .diagnostic
            .ok_or_else(|| anyhow!("index diagnostic projection is missing diagnostic"))?,
    )?;
    if diagnostic.tenant_id != tenant_id
        || diagnostic.bucket_id != bucket_id
        || index_name.is_some_and(|value| diagnostic.index_name != value)
        || severity.is_some_and(|value| diagnostic.severity != value)
    {
        return Err(anyhow!("index diagnostic projection scope mismatch"));
    }
    Ok(diagnostic)
}

fn require_index_diagnostic_permit(
    tenant_id: i64,
    bucket_id: i64,
    permit: &PartitionWritePermit,
) -> Result<()> {
    if permit.partition_family != "index_diagnostic"
        || permit.partition_id != hex::encode(index_diagnostic_partition_id(tenant_id, bucket_id))
    {
        return Err(anyhow!(
            "partition write permit does not target this index diagnostic partition"
        ));
    }
    Ok(())
}

fn encode_index_diagnostic_body(
    diagnostic: &IndexDiagnostic,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    encode_deterministic_proto(
        &IndexDiagnosticBodyProto {
            schema: INDEX_DIAGNOSTIC_BODY_SCHEMA.to_string(),
            diagnostic: Some(index_diagnostic_to_proto(diagnostic)?),
            fence_token,
            mutation_id: mutation_id.to_string(),
        },
        "index diagnostic body",
    )
}

fn index_diagnostic_to_proto(diagnostic: &IndexDiagnostic) -> Result<IndexDiagnosticProto> {
    Ok(IndexDiagnosticProto {
        id: diagnostic.id,
        tenant_id: diagnostic.tenant_id,
        bucket_id: diagnostic.bucket_id,
        bucket_name: diagnostic.bucket_name.clone(),
        index_id: diagnostic.index_id,
        index_name: diagnostic.index_name.clone(),
        object_key: diagnostic.object_key.clone(),
        version_id: diagnostic.version_id.as_ref().map(ToString::to_string),
        severity: diagnostic.severity.clone(),
        code: diagnostic.code.clone(),
        message: diagnostic.message.clone(),
        details: Some(json_value_to_proto(&diagnostic.details)?),
        created_at_unix_nanos: diagnostic.created_at.timestamp_nanos_opt().ok_or_else(|| {
            anyhow!("index diagnostic timestamp cannot be represented in nanoseconds")
        })?,
    })
}

fn index_diagnostic_from_proto(proto: IndexDiagnosticProto) -> Result<IndexDiagnostic> {
    Ok(IndexDiagnostic {
        id: proto.id,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        bucket_name: proto.bucket_name,
        index_id: proto.index_id,
        index_name: proto.index_name,
        object_key: proto.object_key,
        version_id: proto
            .version_id
            .map(|value| {
                uuid::Uuid::parse_str(&value).context("index diagnostic version_id is not a UUID")
            })
            .transpose()?,
        severity: proto.severity,
        code: proto.code,
        message: proto.message,
        details: json_value_from_proto(
            proto
                .details
                .ok_or_else(|| anyhow!("index diagnostic body is missing details"))?,
        )?,
        created_at: chrono::DateTime::from_timestamp_nanos(proto.created_at_unix_nanos),
    })
}

fn json_value_to_proto(value: &JsonValue) -> Result<IndexDiagnosticJsonValueProto> {
    let kind = match value {
        JsonValue::Null => index_diagnostic_json_value_proto::Kind::Null(true),
        JsonValue::Bool(value) => index_diagnostic_json_value_proto::Kind::Bool(*value),
        JsonValue::Number(number) => {
            if let Some(value) = number.as_i64() {
                index_diagnostic_json_value_proto::Kind::I64(value)
            } else if let Some(value) = number.as_u64() {
                index_diagnostic_json_value_proto::Kind::U64(value)
            } else {
                index_diagnostic_json_value_proto::Kind::F64(number.as_f64().ok_or_else(|| {
                    anyhow!("index diagnostic JSON number cannot be represented deterministically")
                })?)
            }
        }
        JsonValue::String(value) => index_diagnostic_json_value_proto::Kind::String(value.clone()),
        JsonValue::Array(values) => {
            index_diagnostic_json_value_proto::Kind::Array(IndexDiagnosticJsonArrayProto {
                values: values
                    .iter()
                    .map(json_value_to_proto)
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        JsonValue::Object(map) => {
            let mut entries = map
                .iter()
                .map(|(key, value)| {
                    Ok(IndexDiagnosticJsonObjectEntryProto {
                        key: key.clone(),
                        value: Some(json_value_to_proto(value)?),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            entries.sort_by(|left, right| left.key.cmp(&right.key));
            index_diagnostic_json_value_proto::Kind::Object(IndexDiagnosticJsonObjectProto {
                entries,
            })
        }
    };
    Ok(IndexDiagnosticJsonValueProto { kind: Some(kind) })
}

fn json_value_from_proto(proto: IndexDiagnosticJsonValueProto) -> Result<JsonValue> {
    let kind = proto
        .kind
        .ok_or_else(|| anyhow!("index diagnostic JSON value is missing kind"))?;
    Ok(match kind {
        index_diagnostic_json_value_proto::Kind::Null(marker) => {
            if !marker {
                return Err(anyhow!("index diagnostic JSON null marker must be true"));
            }
            JsonValue::Null
        }
        index_diagnostic_json_value_proto::Kind::Bool(value) => JsonValue::Bool(value),
        index_diagnostic_json_value_proto::Kind::I64(value) => JsonValue::Number(value.into()),
        index_diagnostic_json_value_proto::Kind::U64(value) => JsonValue::Number(value.into()),
        index_diagnostic_json_value_proto::Kind::F64(value) => JsonValue::Number(
            serde_json::Number::from_f64(value)
                .ok_or_else(|| anyhow!("index diagnostic JSON f64 is not finite"))?,
        ),
        index_diagnostic_json_value_proto::Kind::String(value) => JsonValue::String(value),
        index_diagnostic_json_value_proto::Kind::Array(array) => JsonValue::Array(
            array
                .values
                .into_iter()
                .map(json_value_from_proto)
                .collect::<Result<Vec<_>>>()?,
        ),
        index_diagnostic_json_value_proto::Kind::Object(object) => {
            let mut previous_key: Option<String> = None;
            let mut map = serde_json::Map::new();
            for entry in object.entries {
                if previous_key
                    .as_ref()
                    .is_some_and(|previous| previous >= &entry.key)
                {
                    return Err(anyhow!(
                        "index diagnostic JSON object entries are not strictly sorted"
                    ));
                }
                previous_key = Some(entry.key.clone());
                let value = entry.value.ok_or_else(|| {
                    anyhow!("index diagnostic JSON object entry is missing value")
                })?;
                map.insert(entry.key, json_value_from_proto(value)?);
            }
            JsonValue::Object(map)
        }
    })
}

fn encode_deterministic_proto<M>(message: &M, label: &str) -> Result<Vec<u8>>
where
    M: Message + Default,
{
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    let decoded = M::decode(bytes.as_slice())?;
    let mut canonical = Vec::with_capacity(decoded.encoded_len());
    decoded.encode(&mut canonical)?;
    if canonical != bytes {
        return Err(anyhow!("{label} is not deterministic protobuf"));
    }
    Ok(bytes)
}

fn decode_deterministic_proto<M>(bytes: &[u8], label: &str) -> Result<M>
where
    M: Message + Default,
{
    let value = M::decode(bytes)?;
    let mut canonical = Vec::with_capacity(value.encoded_len());
    value.encode(&mut canonical)?;
    if canonical != bytes {
        return Err(anyhow!("{label} is not deterministic protobuf"));
    }
    Ok(value)
}
