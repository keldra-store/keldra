use crate::anvil_api::{ModelManifest, TensorIndexRow};
use crate::core_store::{
    CF_OBSERVABILITY, CoreMetaTuplePart, TABLE_OBSERVABILITY_CURSOR_ROW, TABLE_STREAM_HEAD_ROW,
    TABLE_STREAM_RECORD_INDEX_ROW, core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32};
use crate::mvcc_bootstrap::MvccSubsystem;
use crate::mvcc_product::{
    ProductMutation, coremeta_application_prefix, coremeta_logical_key,
    coremeta_tuple_from_logical_key, stream_logical_key,
};
use crate::mvcc_transaction::{LogicalKey, PredicateKind};
use crate::partition_fence::PartitionWritePermit;
use anyhow::{Result, anyhow, bail};
use prost::{Message, Oneof};
use serde::{Deserialize, Serialize};

const MODEL_METADATA_BODY_SCHEMA: &str = "anvil.core.model_metadata.v1";
const MODEL_ARTIFACT_PROJECTION_SCHEMA: &str = "anvil.model.artifact_projection.v2";
const MODEL_TENSOR_PROJECTION_SCHEMA: &str = "anvil.model.tensor_projection.v2";
const MODEL_TENSOR_SCAN_PAGE_MAX: usize = 4096;
const MODEL_TENSOR_PAGE_MAX: usize = MODEL_TENSOR_SCAN_PAGE_MAX - 1;

#[derive(Debug, Clone)]
pub struct ModelTensorPage {
    pub tensors: Vec<TensorIndexRow>,
    pub next_cursor: Option<Vec<u8>>,
    pub snapshot_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModelJournalHead {
    schema: String,
    last_sequence: u64,
    last_event_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModelJournalEvent {
    schema: String,
    sequence: u64,
    previous_event_hash: String,
    event_hash: String,
    mutation_id: String,
    payload_ref: String,
    payload: Vec<u8>,
}

const MODEL_JOURNAL_HEAD_SCHEMA: &str = "anvil.model.journal-head.v2";
const MODEL_JOURNAL_EVENT_SCHEMA: &str = "anvil.model.journal-event.v2";

#[derive(Debug, Clone)]
enum ModelEventBody {
    ArtifactUpsert {
        artifact_id: String,
        bucket_id: i64,
        key: String,
        manifest: ModelManifest,
    },
    TensorsReplace {
        artifact_id: String,
        tensors: Vec<TensorIndexRow>,
    },
}

#[derive(Clone, PartialEq, Message)]
struct ModelEventBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(uint64, tag = "2")]
    fence_token: u64,
    #[prost(string, tag = "3")]
    mutation_id: String,
    #[prost(oneof = "model_event_body_proto::Event", tags = "10, 11")]
    event: Option<model_event_body_proto::Event>,
}

mod model_event_body_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Event {
        #[prost(message, tag = "10")]
        ArtifactUpsert(super::ModelArtifactUpsertProto),
        #[prost(message, tag = "11")]
        TensorsReplace(super::ModelTensorsReplaceProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ModelArtifactUpsertProto {
    #[prost(string, tag = "1")]
    artifact_id: String,
    #[prost(int64, tag = "2")]
    bucket_id: i64,
    #[prost(string, tag = "3")]
    key: String,
    #[prost(message, optional, tag = "4")]
    manifest: Option<ModelManifest>,
}

#[derive(Clone, PartialEq, Message)]
struct ModelTensorsReplaceProto {
    #[prost(string, tag = "1")]
    artifact_id: String,
    #[prost(message, repeated, tag = "2")]
    tensors: Vec<TensorIndexRow>,
}

#[derive(Clone, PartialEq, Message)]
struct ModelArtifactProjectionProto {
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    artifact_id: String,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(string, tag = "5")]
    key: String,
    #[prost(message, optional, tag = "6")]
    manifest: Option<ModelManifest>,
}

#[derive(Clone, PartialEq, Message)]
struct ModelTensorProjectionProto {
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    artifact_id: String,
    #[prost(message, optional, tag = "4")]
    tensor: Option<TensorIndexRow>,
}

pub(crate) async fn create_model_artifact_with_permit(
    mvcc: &MvccSubsystem,
    artifact_id: &str,
    bucket_id: i64,
    key: &str,
    manifest: &ModelManifest,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_model_permit(mvcc, permit)?;
    create_model_artifact_inner(
        mvcc,
        artifact_id,
        bucket_id,
        key,
        manifest,
        permit.fence_token,
    )
    .await
}

async fn create_model_artifact_inner(
    mvcc: &MvccSubsystem,
    artifact_id: &str,
    bucket_id: i64,
    key: &str,
    manifest: &ModelManifest,
    fence_token: u64,
) -> Result<()> {
    require_nonempty(artifact_id, "artifact_id")?;
    require_nonempty(key, "model key")?;
    append_model_event(
        mvcc,
        ModelEventBody::ArtifactUpsert {
            artifact_id: artifact_id.to_string(),
            bucket_id,
            key: key.to_string(),
            manifest: manifest.clone(),
        },
        fence_token,
    )
    .await
}

pub(crate) async fn create_model_tensors_with_permit(
    mvcc: &MvccSubsystem,
    artifact_id: &str,
    tensors: &[TensorIndexRow],
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_model_permit(mvcc, permit)?;
    create_model_tensors_inner(mvcc, artifact_id, tensors, permit.fence_token).await
}

async fn create_model_tensors_inner(
    mvcc: &MvccSubsystem,
    artifact_id: &str,
    tensors: &[TensorIndexRow],
    fence_token: u64,
) -> Result<()> {
    require_nonempty(artifact_id, "artifact_id")?;
    append_model_event(
        mvcc,
        ModelEventBody::TensorsReplace {
            artifact_id: artifact_id.to_string(),
            tensors: tensors.to_vec(),
        },
        fence_token,
    )
    .await
}

pub async fn list_tensor_page(
    mvcc: &MvccSubsystem,
    artifact_id: &str,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<ModelTensorPage> {
    list_tensor_page_at_snapshot(
        mvcc,
        artifact_id,
        after_cursor,
        limit,
        mvcc.runtime.applied_version()?,
    )
}

pub fn list_tensor_page_at_snapshot(
    mvcc: &MvccSubsystem,
    artifact_id: &str,
    after_cursor: Option<&[u8]>,
    limit: usize,
    snapshot_version: u64,
) -> Result<ModelTensorPage> {
    if !(1..=MODEL_TENSOR_PAGE_MAX).contains(&limit) {
        return Err(anyhow!(
            "model tensor page size must be between 1 and {MODEL_TENSOR_PAGE_MAX}"
        ));
    }
    let prefix = coremeta_application_prefix(CF_OBSERVABILITY, &model_tensor_prefix(artifact_id)?)?;
    let mut rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_OBSERVABILITY_CURSOR_ROW,
        &prefix,
        snapshot_version,
    )?;
    if let Some(after) = after_cursor {
        rows.retain(|(key, _)| {
            coremeta_tuple_from_logical_key(key, CF_OBSERVABILITY).is_ok_and(|tuple| tuple > after)
        });
    }
    let has_more = rows.len() > limit;
    if has_more {
        rows.truncate(limit);
    }
    let next_cursor = if has_more {
        Some(
            coremeta_tuple_from_logical_key(
                &rows
                    .last()
                    .ok_or_else(|| anyhow!("model tensor continuation has no row"))?
                    .0,
                CF_OBSERVABILITY,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let tensors = rows
        .into_iter()
        .map(|(_, row)| decode_model_tensor_projection(&row.value, artifact_id))
        .collect::<Result<Vec<_>>>()?;
    Ok(ModelTensorPage {
        tensors,
        next_cursor,
        snapshot_version,
    })
}

pub fn get_tensor_metadata(
    mvcc: &MvccSubsystem,
    artifact_id: &str,
    tensor_name: &str,
) -> Result<Option<TensorIndexRow>> {
    get_tensor_metadata_at_snapshot(
        mvcc,
        artifact_id,
        tensor_name,
        mvcc.runtime.applied_version()?,
    )
}

pub fn get_tensor_metadata_at_snapshot(
    mvcc: &MvccSubsystem,
    artifact_id: &str,
    tensor_name: &str,
    snapshot: u64,
) -> Result<Option<TensorIndexRow>> {
    let Some(payload) = mvcc.runtime.read_at(
        &model_tensor_logical_key(artifact_id, tensor_name)?,
        snapshot,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(decode_model_tensor_projection(
        &payload.value,
        artifact_id,
    )?))
}

pub fn get_model_artifact(
    mvcc: &MvccSubsystem,
    artifact_id: &str,
) -> Result<Option<ModelManifest>> {
    get_model_artifact_at_snapshot(mvcc, artifact_id, mvcc.runtime.applied_version()?)
}

pub fn get_model_artifact_at_snapshot(
    mvcc: &MvccSubsystem,
    artifact_id: &str,
    snapshot: u64,
) -> Result<Option<ModelManifest>> {
    let Some(payload) = mvcc
        .runtime
        .read_at(&model_artifact_logical_key(artifact_id)?, snapshot)?
    else {
        return Ok(None);
    };
    Ok(Some(decode_model_artifact_projection(
        &payload.value,
        artifact_id,
    )?))
}

async fn append_model_event(
    mvcc: &MvccSubsystem,
    event: ModelEventBody,
    fence_token: u64,
) -> Result<()> {
    let mutation_id = uuid::Uuid::new_v4();
    let payload = encode_model_event_body(&event, fence_token, mutation_id)?;
    let stream_id = model_metadata_stream_id();
    let head_key = stream_logical_key(TABLE_STREAM_HEAD_ROW, &stream_id, None)?;
    let snapshot = mvcc.runtime.applied_version()?;
    let observed_head = mvcc
        .runtime
        .read_at(&head_key, snapshot)?
        .map(|row| row.value);
    let mut head = observed_head
        .as_deref()
        .map(decode_model_head)
        .transpose()?
        .unwrap_or(ModelJournalHead {
            schema: MODEL_JOURNAL_HEAD_SCHEMA.to_string(),
            last_sequence: 0,
            last_event_hash: String::new(),
        });
    head.last_sequence = head
        .last_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("model journal sequence overflow"))?;
    let payload_ref = format!("inline:sha256:{}", hex::encode(hash32(&payload)));
    let event_hash = model_event_hash(
        head.last_sequence,
        &head.last_event_hash,
        mutation_id,
        &payload_ref,
    );
    let journal_event = ModelJournalEvent {
        schema: MODEL_JOURNAL_EVENT_SCHEMA.to_string(),
        sequence: head.last_sequence,
        previous_event_hash: head.last_event_hash.clone(),
        event_hash: event_hash.clone(),
        mutation_id: mutation_id.to_string(),
        payload_ref,
        payload,
    };
    head.last_event_hash = event_hash;
    let transaction_id = format!("model-metadata:{mutation_id}");
    let (mut mutations, mut predicates) = model_projection_mutations(mvcc, &event, snapshot)?;
    let event_key = stream_logical_key(
        TABLE_STREAM_RECORD_INDEX_ROW,
        &stream_id,
        Some(head.last_sequence),
    )?;
    predicates.push((event_key.clone(), PredicateKind::Absent));
    predicates.push((head_key.clone(), predicate_for(observed_head.as_deref())));
    mutations.push(ProductMutation::put(
        event_key,
        serde_json::to_vec(&journal_event)?,
    ));
    mutations.push(ProductMutation::put(head_key, serde_json::to_vec(&head)?));
    commit_model_mutations(mvcc, &transaction_id, mutations, predicates).await?;
    Ok(())
}

pub fn model_partition_id() -> Hash32 {
    hash32(b"model_metadata/global")
}

fn model_projection_mutations(
    mvcc: &MvccSubsystem,
    event: &ModelEventBody,
    snapshot: u64,
) -> Result<(Vec<ProductMutation>, Vec<(LogicalKey, PredicateKind)>)> {
    let mut predicates = Vec::new();
    match event {
        ModelEventBody::ArtifactUpsert {
            artifact_id,
            bucket_id,
            key,
            manifest,
        } => {
            let logical_key = model_artifact_logical_key(artifact_id)?;
            let observed = mvcc.runtime.read_at(&logical_key, snapshot)?;
            predicates.push((
                logical_key.clone(),
                predicate_for(observed.as_ref().map(|row| row.value.as_slice())),
            ));
            Ok((
                vec![ProductMutation::put(
                    logical_key,
                    encode_deterministic_proto(&ModelArtifactProjectionProto {
                        schema: MODEL_ARTIFACT_PROJECTION_SCHEMA.to_string(),
                        artifact_id: artifact_id.clone(),
                        bucket_id: *bucket_id,
                        key: key.clone(),
                        manifest: Some(manifest.clone()),
                    })?,
                )],
                predicates,
            ))
        }
        ModelEventBody::TensorsReplace {
            artifact_id,
            tensors,
        } => {
            let mut names = std::collections::BTreeSet::new();
            let mut replacement_keys = std::collections::BTreeSet::new();
            for tensor in tensors {
                if !names.insert(tensor.tensor_name.as_str()) {
                    return Err(anyhow!(
                        "model tensor replacement contains duplicate tensor name {}",
                        tensor.tensor_name
                    ));
                }
                replacement_keys.insert(model_tensor_key(artifact_id, &tensor.tensor_name)?);
            }
            let tuple_prefix = model_tensor_prefix(artifact_id)?;
            if after_cursor.is_some_and(|cursor| !cursor.starts_with(&tuple_prefix)) {
                bail!("model tensor cursor is outside the requested prefix");
            }
            let prefix = coremeta_application_prefix(CF_OBSERVABILITY, &tuple_prefix)?;
            let rows = mvcc.runtime.scan_table_prefix_at(
                TABLE_OBSERVABILITY_CURSOR_ROW,
                &prefix,
                snapshot,
            )?;
            let mut mutations = Vec::new();
            for (logical_key, row) in rows {
                let tuple_key =
                    coremeta_tuple_from_logical_key(&logical_key, CF_OBSERVABILITY)?.to_vec();
                predicates.push((
                    logical_key.clone(),
                    PredicateKind::ValueHash(*blake3::hash(&row.value).as_bytes()),
                ));
                if replacement_keys.contains(&tuple_key) {
                    continue;
                }
                mutations.push(ProductMutation::delete(logical_key));
            }
            for tensor in tensors {
                let logical_key = model_tensor_logical_key(artifact_id, &tensor.tensor_name)?;
                if !predicates.iter().any(|(key, _)| key == &logical_key) {
                    predicates.push((logical_key.clone(), PredicateKind::Absent));
                }
                mutations.push(ProductMutation::put(
                    logical_key,
                    encode_deterministic_proto(&ModelTensorProjectionProto {
                        schema: MODEL_TENSOR_PROJECTION_SCHEMA.to_string(),
                        artifact_id: artifact_id.clone(),
                        tensor: Some(tensor.clone()),
                    })?,
                ));
            }
            Ok((mutations, predicates))
        }
    }
}

fn model_artifact_key(artifact_id: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("model"),
        CoreMetaTuplePart::Utf8("artifact"),
        CoreMetaTuplePart::Utf8(artifact_id),
    ])
}

fn model_tensor_prefix(artifact_id: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("model"),
        CoreMetaTuplePart::Utf8("tensor"),
        CoreMetaTuplePart::Utf8(artifact_id),
    ])
}

fn model_tensor_key(artifact_id: &str, tensor_name: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("model"),
        CoreMetaTuplePart::Utf8("tensor"),
        CoreMetaTuplePart::Utf8(artifact_id),
        CoreMetaTuplePart::Utf8(tensor_name),
    ])
}

fn model_artifact_logical_key(artifact_id: &str) -> Result<LogicalKey> {
    coremeta_logical_key(
        CF_OBSERVABILITY,
        TABLE_OBSERVABILITY_CURSOR_ROW,
        &model_artifact_key(artifact_id)?,
    )
}

fn model_tensor_logical_key(artifact_id: &str, tensor_name: &str) -> Result<LogicalKey> {
    coremeta_logical_key(
        CF_OBSERVABILITY,
        TABLE_OBSERVABILITY_CURSOR_ROW,
        &model_tensor_key(artifact_id, tensor_name)?,
    )
}

fn decode_model_artifact_projection(bytes: &[u8], artifact_id: &str) -> Result<ModelManifest> {
    let row = ModelArtifactProjectionProto::decode(bytes)?;
    ensure_deterministic_proto(&row, bytes, "model artifact projection")?;
    if row.schema != MODEL_ARTIFACT_PROJECTION_SCHEMA || row.artifact_id != artifact_id {
        return Err(anyhow!("model artifact projection scope mismatch"));
    }
    row.manifest
        .ok_or_else(|| anyhow!("model artifact projection is missing manifest"))
}

fn decode_model_tensor_projection(bytes: &[u8], artifact_id: &str) -> Result<TensorIndexRow> {
    let row = ModelTensorProjectionProto::decode(bytes)?;
    ensure_deterministic_proto(&row, bytes, "model tensor projection")?;
    if row.schema != MODEL_TENSOR_PROJECTION_SCHEMA || row.artifact_id != artifact_id {
        return Err(anyhow!("model tensor projection scope mismatch"));
    }
    row.tensor
        .ok_or_else(|| anyhow!("model tensor projection is missing tensor"))
}

fn encode_model_event_body(
    event: &ModelEventBody,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    let proto = match event {
        ModelEventBody::ArtifactUpsert {
            artifact_id,
            bucket_id,
            key,
            manifest,
        } => ModelEventBodyProto {
            schema: MODEL_METADATA_BODY_SCHEMA.to_string(),
            fence_token,
            mutation_id: mutation_id.to_string(),
            event: Some(model_event_body_proto::Event::ArtifactUpsert(
                ModelArtifactUpsertProto {
                    artifact_id: artifact_id.clone(),
                    bucket_id: *bucket_id,
                    key: key.clone(),
                    manifest: Some(manifest.clone()),
                },
            )),
        },
        ModelEventBody::TensorsReplace {
            artifact_id,
            tensors,
        } => ModelEventBodyProto {
            schema: MODEL_METADATA_BODY_SCHEMA.to_string(),
            fence_token,
            mutation_id: mutation_id.to_string(),
            event: Some(model_event_body_proto::Event::TensorsReplace(
                ModelTensorsReplaceProto {
                    artifact_id: artifact_id.clone(),
                    tensors: tensors.clone(),
                },
            )),
        },
    };
    encode_deterministic_proto(&proto)
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_deterministic_proto(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    if encode_deterministic_proto(message)? != bytes {
        return Err(anyhow!("{label} is not deterministically encoded"));
    }
    Ok(())
}

fn model_metadata_stream_id() -> String {
    "model_metadata:global".to_string()
}

fn model_partition_principal() -> String {
    "partition-owner:model_metadata:global".to_string()
}

fn decode_model_head(payload: &[u8]) -> Result<ModelJournalHead> {
    let head: ModelJournalHead = serde_json::from_slice(payload)?;
    if head.schema != MODEL_JOURNAL_HEAD_SCHEMA
        || (head.last_sequence == 0) != head.last_event_hash.is_empty()
    {
        bail!("model journal head is invalid");
    }
    Ok(head)
}

fn model_event_hash(
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

fn predicate_for(payload: Option<&[u8]>) -> PredicateKind {
    payload
        .map(|payload| PredicateKind::ValueHash(*blake3::hash(payload).as_bytes()))
        .unwrap_or(PredicateKind::Absent)
}

fn now_unix_ms() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default()
}

async fn commit_model_mutations(
    mvcc: &MvccSubsystem,
    idempotency_key: &str,
    mutations: Vec<ProductMutation>,
    predicates: Vec<(LogicalKey, PredicateKind)>,
) -> Result<()> {
    let principal = model_partition_principal();
    let assignment = mvcc
        .reconcile_work_assignment("model-metadata", "global")
        .await?
        .ok_or_else(|| anyhow!("local node does not own the model metadata assignment"))?;
    let now = now_unix_ms();
    let handle = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id(),
            &principal,
            idempotency_key,
            std::time::Duration::from_secs(30),
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            crate::mvcc_transaction::ReadConsistency::Linearized,
            now,
        )
        .await?;
    let status = mvcc
        .open_transactions
        .status(&handle.transaction_id, &principal, now)?;
    if status.state == "open" {
        mvcc.stage_product_mutations(&handle.transaction_id, &principal, mutations, now)?;
        for (key, kind) in predicates {
            mvcc.stage_predicate(&handle.transaction_id, &principal, key, kind, now)?;
        }
        mvcc.stage_assignment_guard(&handle.transaction_id, &principal, &assignment, now)?;
    }
    let outcome = mvcc
        .open_transactions
        .commit(
            mvcc.runtime.as_ref(),
            &handle.transaction_id,
            &principal,
            now_unix_ms(),
        )
        .await?;
    match outcome.certification {
        crate::mvcc_transaction::CertificationResult::Committed { .. } => Ok(()),
        crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
            bail!("model metadata transaction aborted: {reason:?}")
        }
    }
}

fn require_model_permit(mvcc: &MvccSubsystem, permit: &PartitionWritePermit) -> Result<()> {
    if permit.partition_family != "model_metadata"
        || permit.partition_id != hex::encode(model_partition_id())
        || permit.owner_node_id != mvcc.local_node.node_id
    {
        anyhow::bail!("model metadata write permit targets a different partition");
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}
