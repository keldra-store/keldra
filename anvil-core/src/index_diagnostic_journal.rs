use crate::core_store::{
    CF_OBSERVABILITY, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation,
    CoreMutationPrecondition, CoreMutationRootPublication, CoreStore, TABLE_DIAGNOSTIC_ROW,
    core_meta_committed_row_common, core_meta_root_key_hash, core_meta_tuple_key,
    core_mutation_publication_attempt_id,
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
    base_preconditions: Vec<CoreMutationPrecondition>,
    stream_precondition: CoreMutationPrecondition,
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

#[cfg(test)]
async fn write_index_diagnostic(
    storage: &Storage,
    diagnostic: IndexDiagnostic,
) -> Result<IndexDiagnostic> {
    write_index_diagnostic_inner(storage, diagnostic, 0, Vec::new(), uuid::Uuid::new_v4()).await
}

pub(crate) async fn write_index_diagnostic_with_permit(
    storage: &Storage,
    diagnostic: IndexDiagnostic,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<IndexDiagnostic> {
    require_index_diagnostic_permit(diagnostic.tenant_id, diagnostic.bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    write_index_diagnostic_inner(
        storage,
        diagnostic,
        permit.fence_token,
        vec![partition_precondition],
        uuid::Uuid::new_v4(),
    )
    .await
}

pub(crate) async fn prepare_index_diagnostic_for_task(
    storage: &Storage,
    mut diagnostic: IndexDiagnostic,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    mutation_id: [u8; 16],
) -> Result<PreparedIndexDiagnostic> {
    require_index_diagnostic_permit(diagnostic.tenant_id, diagnostic.bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = index_diagnostic_stream_id(diagnostic.tenant_id, diagnostic.bucket_id);
    let stream_precondition = core_store.stream_head_precondition(&stream_id).await?;
    diagnostic.id = i64::try_from(next_stream_generation(&stream_precondition)?)
        .map_err(|_| anyhow!("index diagnostic cursor exceeds i64"))?;
    Ok(PreparedIndexDiagnostic {
        diagnostic,
        // The exact partition and task fences travel as CoreStore
        // preconditions. Keeping an ephemeral fence out of the task-produced
        // body makes retry bytes stable across an ownership handoff.
        fence_token: 0,
        mutation_id: uuid::Uuid::from_bytes(mutation_id),
        base_preconditions: vec![partition_precondition],
        stream_precondition,
    })
}

pub(crate) async fn publish_prepared_index_diagnostic(
    storage: &Storage,
    prepared: PreparedIndexDiagnostic,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<IndexDiagnostic> {
    let core_store = CoreStore::new(storage.clone()).await?;
    if let Some(existing) = read_committed_diagnostic_replay(&core_store, &prepared).await? {
        return Ok(existing);
    }
    let mut preconditions = prepared.base_preconditions;
    preconditions.extend_from_slice(additional_preconditions);
    append_diagnostic(
        &core_store,
        &prepared.diagnostic,
        prepared.fence_token,
        preconditions,
        prepared.stream_precondition,
        prepared.mutation_id,
    )
    .await?;
    Ok(prepared.diagnostic)
}

async fn read_committed_diagnostic_replay(
    core_store: &CoreStore,
    prepared: &PreparedIndexDiagnostic,
) -> Result<Option<IndexDiagnostic>> {
    let logical_id = index_diagnostic_logical_id(
        prepared.diagnostic.tenant_id,
        prepared.diagnostic.bucket_id,
        prepared.mutation_id,
    );
    let stream_id =
        index_diagnostic_stream_id(prepared.diagnostic.tenant_id, prepared.diagnostic.bucket_id);
    let Some(record) = core_store
        .read_stream_record_by_idempotency_key(&stream_id, &logical_id)
        .await?
    else {
        return Ok(None);
    };
    if record.record_kind != "index_diagnostic"
        || record.authenticated_principal
            != index_diagnostic_partition_principal(
                prepared.diagnostic.tenant_id,
                prepared.diagnostic.bucket_id,
            )
    {
        return Err(anyhow!(
            "index diagnostic logical id identifies different committed content"
        ));
    }
    let mut diagnostic = decode_index_diagnostic_body(&record.payload)?;
    diagnostic.id = i64::try_from(record.sequence)
        .map_err(|_| anyhow!("index diagnostic cursor exceeds i64"))?;
    let mut expected = prepared.diagnostic.clone();
    expected.id = diagnostic.id;
    if !same_diagnostic(&diagnostic, &expected) {
        return Err(anyhow!(
            "index diagnostic deterministic replay payload diverged"
        ));
    }
    Ok(Some(diagnostic))
}

fn same_diagnostic(left: &IndexDiagnostic, right: &IndexDiagnostic) -> bool {
    left.id == right.id
        && left.tenant_id == right.tenant_id
        && left.bucket_id == right.bucket_id
        && left.bucket_name == right.bucket_name
        && left.index_id == right.index_id
        && left.index_name == right.index_name
        && left.object_key == right.object_key
        && left.version_id == right.version_id
        && left.severity == right.severity
        && left.code == right.code
        && left.message == right.message
        && left.details == right.details
        && left.created_at == right.created_at
}

async fn write_index_diagnostic_inner(
    storage: &Storage,
    mut diagnostic: IndexDiagnostic,
    fence_token: u64,
    additional_preconditions: Vec<CoreMutationPrecondition>,
    mutation_id: uuid::Uuid,
) -> Result<IndexDiagnostic> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = index_diagnostic_stream_id(diagnostic.tenant_id, diagnostic.bucket_id);
    let stream_precondition = core_store.stream_head_precondition(&stream_id).await?;
    diagnostic.id = i64::try_from(next_stream_generation(&stream_precondition)?)
        .map_err(|_| anyhow!("index diagnostic cursor exceeds i64"))?;
    append_diagnostic(
        &core_store,
        &diagnostic,
        fence_token,
        additional_preconditions,
        stream_precondition,
        mutation_id,
    )
    .await?;
    Ok(diagnostic)
}

pub async fn read_index_diagnostics(
    storage: &Storage,
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
    CoreStore::new(storage.clone())
        .await?
        .scan_coremeta_prefix_page(
            CF_OBSERVABILITY,
            TABLE_DIAGNOSTIC_ROW,
            &prefix,
            after.as_deref(),
            limit,
        )?
        .into_iter()
        .map(|row| {
            decode_index_diagnostic_projection(
                &row.payload,
                tenant_id,
                bucket_id,
                none_if_empty(index_name),
                none_if_empty(severity),
            )
        })
        .collect()
}

pub async fn index_diagnostic_revision(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<String> {
    Ok(CoreStore::new(storage.clone())
        .await?
        .stream_head_sequence(&index_diagnostic_stream_id(tenant_id, bucket_id))
        .await?
        .to_string())
}

async fn append_diagnostic(
    core_store: &CoreStore,
    diagnostic: &IndexDiagnostic,
    fence_token: u64,
    additional_preconditions: Vec<CoreMutationPrecondition>,
    stream_precondition: CoreMutationPrecondition,
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
    let mut preconditions = additional_preconditions;
    preconditions.push(stream_precondition);
    let transaction_id = core_mutation_publication_attempt_id(&logical_id, &preconditions)?;
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: partition_id.clone(),
        stream_id: stream_id.clone(),
        record_kind: "index_diagnostic".to_string(),
        payload,
        idempotency_key: Some(logical_id),
    }];
    let projection = encode_index_diagnostic_projection(
        diagnostic,
        &stream_id,
        u64::try_from(diagnostic.id)?,
        &transaction_id,
    )?;
    for tuple_key in index_diagnostic_projection_keys(diagnostic)? {
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.clone(),
            cf: CF_OBSERVABILITY.to_string(),
            table_id: TABLE_DIAGNOSTIC_ROW,
            tuple_key,
            payload: projection.clone(),
        });
    }
    let projection_root = index_diagnostic_projection_root_anchor_key(&stream_id);
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: partition_id.clone(),
            committed_by_principal: index_diagnostic_partition_principal(
                diagnostic.tenant_id,
                diagnostic.bucket_id,
            ),
            root_publications: vec![
                CoreMutationRootPublication::new(partition_id, WriterFamily::CoreControl.as_str())
                    .coordinator(),
                CoreMutationRootPublication::new(
                    projection_root,
                    WriterFamily::TypedMetadata.as_str(),
                ),
            ],
            preconditions,
            operations,
        })
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

fn next_stream_generation(precondition: &CoreMutationPrecondition) -> Result<u64> {
    let CoreMutationPrecondition::StreamHead {
        expected_last_sequence,
        ..
    } = precondition
    else {
        return Err(anyhow!(
            "index diagnostic stream precondition has wrong kind"
        ));
    };
    expected_last_sequence
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

#[cfg(test)]
pub(crate) async fn read_index_diagnostic_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut after_sequence = 0;
    let mut fences = Vec::new();
    loop {
        let page = core_store
            .read_stream_page(crate::core_store::ReadStream {
                stream_id: index_diagnostic_stream_id(tenant_id, bucket_id),
                after_sequence,
                limit: 256,
            })
            .await?;
        for record in page.records {
            if record.record_kind == "index_diagnostic" {
                fences.push(decode_index_diagnostic_body_fence(&record.payload)?);
            }
        }
        if !page.has_more || page.next_sequence == after_sequence {
            break;
        }
        after_sequence = page.next_sequence;
    }
    Ok(fences)
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

fn decode_index_diagnostic_body(bytes: &[u8]) -> Result<IndexDiagnostic> {
    let body =
        decode_deterministic_proto::<IndexDiagnosticBodyProto>(bytes, "index diagnostic body")?;
    if body.schema != INDEX_DIAGNOSTIC_BODY_SCHEMA {
        return Err(anyhow!("index diagnostic body schema mismatch"));
    }
    let diagnostic = index_diagnostic_from_proto(
        body.diagnostic
            .ok_or_else(|| anyhow!("index diagnostic body is missing diagnostic"))?,
    )?;
    Ok(diagnostic)
}

#[cfg(test)]
fn decode_index_diagnostic_body_fence(bytes: &[u8]) -> Result<u64> {
    let body =
        decode_deterministic_proto::<IndexDiagnosticBodyProto>(bytes, "index diagnostic body")?;
    if body.schema != INDEX_DIAGNOSTIC_BODY_SCHEMA {
        return Err(anyhow!("index diagnostic body schema mismatch"));
    }
    Ok(body.fence_token)
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use tempfile::tempdir;

    const PARTITION_OWNER_KEY: &[u8] = b"index diagnostic partition owner signing key";

    fn diagnostic(index_name: &str, severity: &str) -> IndexDiagnostic {
        IndexDiagnostic {
            id: 0,
            tenant_id: 42,
            bucket_id: 7,
            bucket_name: "docs".to_string(),
            index_id: Some(10),
            index_name: index_name.to_string(),
            object_key: "doc.txt".to_string(),
            version_id: None,
            severity: severity.to_string(),
            code: "parse_failed".to_string(),
            message: "parse failed".to_string(),
            details: json!({"line": 1}),
            created_at: Utc::now(),
        }
    }

    fn assert_same_diagnostic(left: &IndexDiagnostic, right: &IndexDiagnostic) {
        assert_eq!(left.id, right.id);
        assert_eq!(left.tenant_id, right.tenant_id);
        assert_eq!(left.bucket_id, right.bucket_id);
        assert_eq!(left.bucket_name, right.bucket_name);
        assert_eq!(left.index_id, right.index_id);
        assert_eq!(left.index_name, right.index_name);
        assert_eq!(left.object_key, right.object_key);
        assert_eq!(left.version_id, right.version_id);
        assert_eq!(left.severity, right.severity);
        assert_eq!(left.code, right.code);
        assert_eq!(left.message, right.message);
        assert_eq!(left.details, right.details);
        assert_eq!(left.created_at, right.created_at);
    }

    fn push_varint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    fn append_length_delimited(out: &mut Vec<u8>, field_number: u64, value: &[u8]) {
        push_varint(out, (field_number << 3) | 2);
        push_varint(out, value.len() as u64);
        out.extend_from_slice(value);
    }

    async fn ready_diagnostic_permit(
        storage: &Storage,
        owner_node_id: &str,
    ) -> PartitionWritePermit {
        crate::partition_fence::ready_partition_owner_for_test(
            storage,
            "index_diagnostic".to_string(),
            hex::encode(index_diagnostic_partition_id(42, 7)),
            owner_node_id,
            0,
            hex::encode([0; 32]),
            hex::encode([5; 32]),
            PARTITION_OWNER_KEY,
        )
        .await
        .write_permit()
        .unwrap()
    }

    #[tokio::test]
    async fn index_diagnostic_journal_replays_and_filters() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        write_index_diagnostic(&storage, diagnostic("a", "warning"))
            .await
            .unwrap();
        write_index_diagnostic(&storage, diagnostic("b", "error"))
            .await
            .unwrap();

        let all = read_index_diagnostics(&storage, 42, 7, "", "", 0, 10)
            .await
            .unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].id, 1);
        assert_eq!(all[1].id, 2);
        assert_eq!(
            read_index_diagnostics(&storage, 42, 7, "b", "error", 0, 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn index_diagnostic_frame_body_is_deterministic_protobuf() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let mut input = diagnostic("a", "warning");
        input.version_id = Some(uuid::Uuid::from_u128(
            0x12345678_9abc_def0_1234_56789abcdef0,
        ));
        input.details = json!({
            "z": [1, true, null],
            "a": {"nested": "value"},
        });

        let written = write_index_diagnostic(&storage, input).await.unwrap();
        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let records = core_store
            .read_stream(crate::core_store::ReadStream {
                stream_id: index_diagnostic_stream_id(42, 7),
                after_sequence: 0,
                limit: 1,
            })
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_kind, "index_diagnostic");
        assert!(
            !records[0].payload.starts_with(b"{"),
            "index diagnostic stream payload must not use JSON"
        );
        let decoded = decode_index_diagnostic_body(&records[0].payload).unwrap();
        assert_same_diagnostic(&decoded, &written);
    }

    #[tokio::test]
    async fn task_diagnostic_retry_reuses_the_byte_identical_committed_record() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let permit = ready_diagnostic_permit(&storage, "node-a").await;
        let mut input = diagnostic("a", "warning");
        input.created_at = chrono::DateTime::<Utc>::from_timestamp_nanos(1_700_000_000_000_000_000);
        let mutation_id = [7; 16];

        let first = publish_prepared_index_diagnostic(
            &storage,
            prepare_index_diagnostic_for_task(
                &storage,
                input.clone(),
                &permit,
                PARTITION_OWNER_KEY,
                mutation_id,
            )
            .await
            .unwrap(),
            &[],
        )
        .await
        .unwrap();
        let replay = publish_prepared_index_diagnostic(
            &storage,
            prepare_index_diagnostic_for_task(
                &storage,
                input,
                &permit,
                PARTITION_OWNER_KEY,
                mutation_id,
            )
            .await
            .unwrap(),
            &[],
        )
        .await
        .unwrap();

        assert_same_diagnostic(&first, &replay);
        let records = CoreStore::new(storage)
            .await
            .unwrap()
            .read_stream(crate::core_store::ReadStream {
                stream_id: index_diagnostic_stream_id(42, 7),
                after_sequence: 0,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_same_diagnostic(
            &decode_index_diagnostic_body(&records[0].payload).unwrap(),
            &first,
        );
    }

    #[tokio::test]
    async fn diagnostic_filter_page_does_not_scan_unrelated_history() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for index in 0..64 {
            write_index_diagnostic(
                &storage,
                diagnostic(&format!("unrelated-{index:03}"), "warning"),
            )
            .await
            .unwrap();
        }
        for _ in 0..3 {
            write_index_diagnostic(&storage, diagnostic("target", "error"))
                .await
                .unwrap();
        }

        let first = read_index_diagnostics(&storage, 42, 7, "target", "error", 0, 2)
            .await
            .unwrap();
        assert_eq!(first.len(), 2);
        let second = read_index_diagnostics(
            &storage,
            42,
            7,
            "target",
            "error",
            first.last().unwrap().id,
            2,
        )
        .await
        .unwrap();
        assert_eq!(second.len(), 1);
    }

    #[test]
    fn index_diagnostic_body_rejects_reencoded_protobuf() {
        let mut value = diagnostic("a", "warning");
        value.id = 1;
        let diagnostic = index_diagnostic_to_proto(&value).unwrap();
        let diagnostic_bytes = encode_deterministic_proto(&diagnostic, "test diagnostic").unwrap();
        let mut reencoded = Vec::new();
        append_length_delimited(&mut reencoded, 2, &diagnostic_bytes);
        append_length_delimited(&mut reencoded, 1, INDEX_DIAGNOSTIC_BODY_SCHEMA.as_bytes());

        let mutation_id = uuid::Uuid::from_u128(0xfeed_beef_feed_beef_feed_beef_feed_beef);
        assert_ne!(
            reencoded,
            encode_index_diagnostic_body(&value, 0, mutation_id).unwrap()
        );
        let err = decode_index_diagnostic_body(&reencoded).unwrap_err();
        assert!(
            err.to_string().contains("not deterministic protobuf"),
            "unexpected re-encoded body error: {err}"
        );
    }

    #[test]
    fn index_diagnostic_body_rejects_schema_mismatch() {
        let mut value = diagnostic("a", "warning");
        value.id = 1;
        let bytes = encode_deterministic_proto(
            &IndexDiagnosticBodyProto {
                schema: "anvil.core.index_diagnostic.wrong.v1".to_string(),
                diagnostic: Some(index_diagnostic_to_proto(&value).unwrap()),
                fence_token: 0,
                mutation_id: uuid::Uuid::nil().to_string(),
            },
            "test index diagnostic body",
        )
        .unwrap();

        let err = decode_index_diagnostic_body(&bytes).unwrap_err();
        assert!(
            err.to_string().contains("schema mismatch"),
            "unexpected schema mismatch error: {err}"
        );
    }

    #[tokio::test]
    async fn index_diagnostic_permit_sets_frame_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let permit = ready_diagnostic_permit(&storage, "node-a").await;

        let written = write_index_diagnostic_with_permit(
            &storage,
            diagnostic("a", "warning"),
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
        assert_eq!(written.id, 1);
        let fences = read_index_diagnostic_frame_fences_for_test(&storage, 42, 7)
            .await
            .unwrap();
        assert_eq!(fences, vec![permit.fence_token]);
    }

    #[tokio::test]
    async fn index_diagnostic_rejects_stale_partition_permit() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let stale = ready_diagnostic_permit(&storage, "node-a").await;
        let fresh = ready_diagnostic_permit(&storage, "node-b").await;
        assert!(fresh.fence_token > stale.fence_token);

        let rejected = write_index_diagnostic_with_permit(
            &storage,
            diagnostic("a", "warning"),
            &stale,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap_err();
        assert!(rejected.to_string().contains("PartitionNotOwned"));

        write_index_diagnostic_with_permit(
            &storage,
            diagnostic("a", "warning"),
            &fresh,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn index_diagnostic_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let stale = ready_diagnostic_permit(&storage, "node-a").await;
        let stale_precondition =
            partition_write_precondition(&storage, &stale, PARTITION_OWNER_KEY)
                .await
                .unwrap();
        let fresh = ready_diagnostic_permit(&storage, "node-b").await;
        assert!(fresh.fence_token > stale.fence_token);

        let rejected = write_index_diagnostic_inner(
            &storage,
            diagnostic("a", "warning"),
            stale.fence_token,
            vec![stale_precondition],
            uuid::Uuid::from_bytes([9; 16]),
        )
        .await
        .unwrap_err();
        let message = rejected.to_string();
        assert!(
            message.contains("generation mismatch")
                || message.contains("target mismatch")
                || message.contains("precondition failed"),
            "unexpected stale precondition error: {message}"
        );

        write_index_diagnostic_with_permit(
            &storage,
            diagnostic("a", "warning"),
            &fresh,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }
}
