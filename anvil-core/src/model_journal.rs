use crate::anvil_api::{ModelManifest, TensorIndexRow};
use crate::core_store::{
    CF_OBSERVABILITY, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation,
    CoreMutationPrecondition, CoreMutationRootPublication, CoreStore,
    TABLE_OBSERVABILITY_CURSOR_ROW, core_meta_committed_row_common, core_meta_record_tuple_key,
    core_meta_root_key_hash, core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32, writer::WriterFamily};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use prost::{Message, Oneof};

const MODEL_METADATA_BODY_SCHEMA: &str = "anvil.core.model_metadata.v1";
const MODEL_ARTIFACT_PROJECTION_SCHEMA: &str = "anvil.model.artifact_projection.v1";
const MODEL_TENSOR_PROJECTION_SCHEMA: &str = "anvil.model.tensor_projection.v1";
const MODEL_TENSOR_SCAN_PAGE_MAX: usize = 4096;
const MODEL_TENSOR_PAGE_MAX: usize = MODEL_TENSOR_SCAN_PAGE_MAX - 1;

#[derive(Debug, Clone)]
pub struct ModelTensorPage {
    pub tensors: Vec<TensorIndexRow>,
    pub next_cursor: Option<Vec<u8>>,
}

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
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
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
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    artifact_id: String,
    #[prost(message, optional, tag = "4")]
    tensor: Option<TensorIndexRow>,
}

#[cfg(test)]
async fn create_model_artifact(
    storage: &Storage,
    artifact_id: &str,
    bucket_id: i64,
    key: &str,
    manifest: &ModelManifest,
) -> Result<()> {
    create_model_artifact_inner(storage, artifact_id, bucket_id, key, manifest, 0, None).await
}

pub(crate) async fn create_model_artifact_with_permit(
    storage: &Storage,
    artifact_id: &str,
    bucket_id: i64,
    key: &str,
    manifest: &ModelManifest,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let partition_precondition =
        model_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_model_artifact_inner(
        storage,
        artifact_id,
        bucket_id,
        key,
        manifest,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn create_model_artifact_inner(
    storage: &Storage,
    artifact_id: &str,
    bucket_id: i64,
    key: &str,
    manifest: &ModelManifest,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    require_nonempty(artifact_id, "artifact_id")?;
    require_nonempty(key, "model key")?;
    append_model_event(
        storage,
        ModelEventBody::ArtifactUpsert {
            artifact_id: artifact_id.to_string(),
            bucket_id,
            key: key.to_string(),
            manifest: manifest.clone(),
        },
        fence_token,
        partition_precondition,
    )
    .await
}

#[cfg(test)]
async fn create_model_tensors(
    storage: &Storage,
    artifact_id: &str,
    tensors: &[TensorIndexRow],
) -> Result<()> {
    create_model_tensors_inner(storage, artifact_id, tensors, 0, None).await
}

pub(crate) async fn create_model_tensors_with_permit(
    storage: &Storage,
    artifact_id: &str,
    tensors: &[TensorIndexRow],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let partition_precondition =
        model_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_model_tensors_inner(
        storage,
        artifact_id,
        tensors,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn create_model_tensors_inner(
    storage: &Storage,
    artifact_id: &str,
    tensors: &[TensorIndexRow],
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    require_nonempty(artifact_id, "artifact_id")?;
    append_model_event(
        storage,
        ModelEventBody::TensorsReplace {
            artifact_id: artifact_id.to_string(),
            tensors: tensors.to_vec(),
        },
        fence_token,
        partition_precondition,
    )
    .await
}

pub async fn list_tensor_page(
    storage: &Storage,
    artifact_id: &str,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<ModelTensorPage> {
    if !(1..=MODEL_TENSOR_PAGE_MAX).contains(&limit) {
        return Err(anyhow!(
            "model tensor page size must be between 1 and {MODEL_TENSOR_PAGE_MAX}"
        ));
    }
    let mut rows = CoreStore::new(storage.clone())
        .await?
        .scan_coremeta_prefix_page(
            CF_OBSERVABILITY,
            TABLE_OBSERVABILITY_CURSOR_ROW,
            &model_tensor_prefix(artifact_id)?,
            after_cursor,
            limit + 1,
        )?;
    let has_more = rows.len() > limit;
    if has_more {
        rows.truncate(limit);
    }
    let next_cursor = if has_more {
        Some(
            core_meta_record_tuple_key(
                &rows
                    .last()
                    .ok_or_else(|| anyhow!("model tensor continuation has no row"))?
                    .key,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let tensors = rows
        .into_iter()
        .map(|row| decode_model_tensor_projection(&row.payload, artifact_id))
        .collect::<Result<Vec<_>>>()?;
    Ok(ModelTensorPage {
        tensors,
        next_cursor,
    })
}

pub async fn get_tensor_metadata(
    storage: &Storage,
    artifact_id: &str,
    tensor_name: &str,
) -> Result<Option<TensorIndexRow>> {
    let Some(payload) = CoreStore::new(storage.clone()).await?.read_coremeta_row(
        CF_OBSERVABILITY,
        TABLE_OBSERVABILITY_CURSOR_ROW,
        &model_tensor_key(artifact_id, tensor_name)?,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(decode_model_tensor_projection(&payload, artifact_id)?))
}

pub async fn get_model_artifact(
    storage: &Storage,
    artifact_id: &str,
) -> Result<Option<ModelManifest>> {
    let Some(payload) = CoreStore::new(storage.clone()).await?.read_coremeta_row(
        CF_OBSERVABILITY,
        TABLE_OBSERVABILITY_CURSOR_ROW,
        &model_artifact_key(artifact_id)?,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(decode_model_artifact_projection(
        &payload,
        artifact_id,
    )?))
}

async fn append_model_event(
    storage: &Storage,
    event: ModelEventBody,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mutation_id = uuid::Uuid::new_v4();
    let payload = encode_model_event_body(&event, fence_token, mutation_id)?;
    let partition_id = hex::encode(model_partition_id());
    let stream_id = model_metadata_stream_id();
    let stream_precondition = core_store.stream_head_precondition(&stream_id).await?;
    let root_generation = next_stream_generation(&stream_precondition)?;
    let transaction_id = format!("model-metadata:{mutation_id}");
    let projection_operations = model_projection_operations(
        &core_store,
        &event,
        &stream_id,
        root_generation,
        &transaction_id,
        &partition_id,
    )?;
    let mut root_publications = vec![
        CoreMutationRootPublication::new(partition_id.clone(), WriterFamily::CoreControl.as_str())
            .coordinator(),
    ];
    if !projection_operations.is_empty() {
        root_publications.push(CoreMutationRootPublication::new(
            model_projection_root_anchor_key(&stream_id),
            WriterFamily::Vector.as_str(),
        ));
    }
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: partition_id.clone(),
        stream_id: stream_id.clone(),
        record_kind: "model_metadata".to_string(),
        payload,
        idempotency_key: Some(transaction_id.clone()),
    }];
    operations.extend(projection_operations);
    let mut preconditions: Vec<_> = partition_precondition.into_iter().collect();
    preconditions.push(stream_precondition);
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: partition_id.clone(),
            committed_by_principal: model_partition_principal(),
            root_publications,
            preconditions,
            operations,
        })
        .await?;
    Ok(())
}

pub fn model_partition_id() -> Hash32 {
    hash32(b"model_metadata/global")
}

fn next_stream_generation(precondition: &CoreMutationPrecondition) -> Result<u64> {
    let CoreMutationPrecondition::StreamHead {
        expected_last_sequence,
        ..
    } = precondition
    else {
        return Err(anyhow!("model stream precondition has wrong kind"));
    };
    expected_last_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("model stream sequence overflow"))
}

fn model_projection_operations(
    core_store: &CoreStore,
    event: &ModelEventBody,
    stream_id: &str,
    root_generation: u64,
    transaction_id: &str,
    partition_id: &str,
) -> Result<Vec<CoreMutationOperation>> {
    let root_key_hash = core_meta_root_key_hash(&model_projection_root_anchor_key(stream_id));
    match event {
        ModelEventBody::ArtifactUpsert {
            artifact_id,
            bucket_id,
            key,
            manifest,
        } => Ok(vec![CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.to_string(),
            cf: CF_OBSERVABILITY.to_string(),
            table_id: TABLE_OBSERVABILITY_CURSOR_ROW,
            tuple_key: model_artifact_key(artifact_id)?,
            payload: encode_deterministic_proto(&ModelArtifactProjectionProto {
                common: Some(core_meta_committed_row_common(
                    "system",
                    root_key_hash,
                    root_generation,
                    transaction_id,
                    root_generation,
                )),
                schema: MODEL_ARTIFACT_PROJECTION_SCHEMA.to_string(),
                artifact_id: artifact_id.clone(),
                bucket_id: *bucket_id,
                key: key.clone(),
                manifest: Some(manifest.clone()),
            })?,
        }]),
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
            let prefix = model_tensor_prefix(artifact_id)?;
            let mut after = None;
            let mut operations = Vec::new();
            loop {
                let rows = core_store.scan_coremeta_prefix_page(
                    CF_OBSERVABILITY,
                    TABLE_OBSERVABILITY_CURSOR_ROW,
                    &prefix,
                    after.as_deref(),
                    MODEL_TENSOR_SCAN_PAGE_MAX,
                )?;
                if rows.is_empty() {
                    break;
                }
                let row_count = rows.len();
                after = Some(
                    core_meta_record_tuple_key(
                        &rows
                            .last()
                            .ok_or_else(|| anyhow!("model tensor page has no last row"))?
                            .key,
                    )?
                    .to_vec(),
                );
                for row in rows {
                    let tuple_key = core_meta_record_tuple_key(&row.key)?.to_vec();
                    if replacement_keys.contains(&tuple_key) {
                        continue;
                    }
                    operations.push(CoreMutationOperation::CoreMetaDelete {
                        partition_id: partition_id.to_string(),
                        cf: CF_OBSERVABILITY.to_string(),
                        table_id: TABLE_OBSERVABILITY_CURSOR_ROW,
                        tuple_key,
                    });
                }
                if row_count < MODEL_TENSOR_SCAN_PAGE_MAX {
                    break;
                }
            }
            for tensor in tensors {
                operations.push(CoreMutationOperation::CoreMetaPut {
                    partition_id: partition_id.to_string(),
                    cf: CF_OBSERVABILITY.to_string(),
                    table_id: TABLE_OBSERVABILITY_CURSOR_ROW,
                    tuple_key: model_tensor_key(artifact_id, &tensor.tensor_name)?,
                    payload: encode_deterministic_proto(&ModelTensorProjectionProto {
                        common: Some(core_meta_committed_row_common(
                            "system",
                            root_key_hash.clone(),
                            root_generation,
                            transaction_id,
                            root_generation,
                        )),
                        schema: MODEL_TENSOR_PROJECTION_SCHEMA.to_string(),
                        artifact_id: artifact_id.clone(),
                        tensor: Some(tensor.clone()),
                    })?,
                });
            }
            Ok(operations)
        }
    }
}

fn model_projection_root_anchor_key(stream_id: &str) -> String {
    format!("stream/{stream_id}")
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

fn decode_model_artifact_projection(bytes: &[u8], artifact_id: &str) -> Result<ModelManifest> {
    let row = ModelArtifactProjectionProto::decode(bytes)?;
    ensure_deterministic_proto(&row, bytes, "model artifact projection")?;
    if row.common.is_none()
        || row.schema != MODEL_ARTIFACT_PROJECTION_SCHEMA
        || row.artifact_id != artifact_id
    {
        return Err(anyhow!("model artifact projection scope mismatch"));
    }
    row.manifest
        .ok_or_else(|| anyhow!("model artifact projection is missing manifest"))
}

fn decode_model_tensor_projection(bytes: &[u8], artifact_id: &str) -> Result<TensorIndexRow> {
    let row = ModelTensorProjectionProto::decode(bytes)?;
    ensure_deterministic_proto(&row, bytes, "model tensor projection")?;
    if row.common.is_none()
        || row.schema != MODEL_TENSOR_PROJECTION_SCHEMA
        || row.artifact_id != artifact_id
    {
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

fn decode_model_event_body(bytes: &[u8]) -> Result<ModelEventBody> {
    let proto = ModelEventBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "model metadata body")?;
    if proto.schema != MODEL_METADATA_BODY_SCHEMA {
        return Err(anyhow!("model metadata body has invalid schema"));
    }
    let _mutation_id = uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("model metadata body has invalid mutation id"))?;
    Ok(
        match proto
            .event
            .ok_or_else(|| anyhow!("model metadata body is missing event"))?
        {
            model_event_body_proto::Event::ArtifactUpsert(value) => {
                ModelEventBody::ArtifactUpsert {
                    artifact_id: value.artifact_id,
                    bucket_id: value.bucket_id,
                    key: value.key,
                    manifest: value.manifest.ok_or_else(|| {
                        anyhow!("model metadata artifact body is missing manifest")
                    })?,
                }
            }
            model_event_body_proto::Event::TensorsReplace(value) => {
                ModelEventBody::TensorsReplace {
                    artifact_id: value.artifact_id,
                    tensors: value.tensors,
                }
            }
        },
    )
}

#[cfg(test)]
fn decode_model_event_body_fence(bytes: &[u8]) -> Result<u64> {
    let proto = ModelEventBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "model metadata body")?;
    if proto.schema != MODEL_METADATA_BODY_SCHEMA {
        return Err(anyhow!("model metadata body has invalid schema"));
    }
    Ok(proto.fence_token)
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

#[cfg(test)]
pub(crate) async fn read_model_frame_fences_for_test(storage: &Storage) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut after_sequence = 0;
    let mut fences = Vec::new();
    loop {
        let page = core_store
            .read_stream_page(crate::core_store::ReadStream {
                stream_id: model_metadata_stream_id(),
                after_sequence,
                limit: 256,
            })
            .await?;
        for record in page.records {
            if record.record_kind == "model_metadata" {
                fences.push(decode_model_event_body_fence(&record.payload)?);
            }
        }
        if !page.has_more || page.next_sequence == after_sequence {
            break;
        }
        after_sequence = page.next_sequence;
    }
    Ok(fences)
}

async fn model_write_precondition(
    storage: &Storage,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<CoreMutationPrecondition> {
    require_model_permit(permit)?;
    Ok(partition_write_precondition(storage, permit, partition_owner_signing_key).await?)
}

fn require_model_permit(permit: &PartitionWritePermit) -> Result<()> {
    if permit.partition_family != "model_metadata"
        || permit.partition_id != hex::encode(model_partition_id())
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const KEY: &[u8] = b"model metadata partition owner key";

    fn manifest(base: &str) -> ModelManifest {
        ModelManifest {
            schema_version: "1".to_string(),
            artifact_id: "artifact-a".to_string(),
            name: "artifact-a".to_string(),
            format: "test".to_string(),
            components: Vec::new(),
            base_artifact_id: base.to_string(),
            delta_artifact_ids: Vec::new(),
            signatures: Vec::new(),
            merkle_root: "abc".to_string(),
            meta: std::collections::HashMap::new(),
        }
    }

    fn tensor(name: &str) -> TensorIndexRow {
        TensorIndexRow {
            tensor_name: name.to_string(),
            file_path: "weights.bin".to_string(),
            file_offset: 0,
            byte_length: 4,
            dtype: 3,
            shape: vec![1],
            layout: "row_major".to_string(),
            block_bytes: 4,
            blocks: Vec::new(),
        }
    }

    #[tokio::test]
    async fn model_journal_replays_artifacts_and_tensors() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        create_model_artifact(&storage, "artifact-a", 1, "models/a", &manifest(""))
            .await
            .unwrap();
        create_model_tensors(&storage, "artifact-a", &[tensor("z"), tensor("a")])
            .await
            .unwrap();

        assert!(
            get_model_artifact(&storage, "artifact-a")
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            list_tensor_page(&storage, "artifact-a", None, 10)
                .await
                .unwrap()
                .tensors
                .into_iter()
                .map(|row| row.tensor_name)
                .collect::<Vec<_>>(),
            vec!["a".to_string(), "z".to_string()]
        );
        assert_eq!(
            get_tensor_metadata(&storage, "artifact-a", "z")
                .await
                .unwrap()
                .unwrap()
                .tensor_name,
            "z"
        );
    }

    #[tokio::test]
    pub(crate) async fn model_journal_with_permit_writes_fenced_frames_and_header() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let permit = owner.write_permit().unwrap();

        create_model_artifact_with_permit(
            &storage,
            "artifact-a",
            1,
            "models/a",
            &manifest(""),
            &permit,
            KEY,
        )
        .await
        .unwrap();
        create_model_tensors_with_permit(&storage, "artifact-a", &[tensor("z")], &permit, KEY)
            .await
            .unwrap();

        let fences = read_model_frame_fences_for_test(&storage).await.unwrap();
        assert_eq!(fences.len(), 2);
        assert!(fences.iter().all(|fence| *fence == permit.fence_token));
    }

    #[tokio::test]
    pub(crate) async fn model_journal_with_permit_rejects_stale_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let newer = ready_owner(&storage, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = create_model_artifact_with_permit(
            &storage,
            "artifact-a",
            1,
            "models/a",
            &manifest(""),
            &stale_permit,
            KEY,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("write permit owner is not current")
        );
    }

    #[tokio::test]
    pub(crate) async fn model_journal_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let stale_precondition = partition_write_precondition(&storage, &stale_permit, KEY)
            .await
            .unwrap();
        let newer = ready_owner(&storage, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = create_model_artifact_inner(
            &storage,
            "artifact-a",
            1,
            "models/a",
            &manifest(""),
            stale_permit.fence_token,
            Some(stale_precondition),
        )
        .await
        .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("generation mismatch")
                || message.contains("target mismatch")
                || message.contains("precondition failed"),
            "unexpected stale precondition error: {message}"
        );

        create_model_artifact_with_permit(
            &storage,
            "artifact-a",
            1,
            "models/a",
            &manifest(""),
            &newer.write_permit().unwrap(),
            KEY,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn current_model_projection_does_not_replay_unrelated_history() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        create_model_artifact(&storage, "artifact-a", 1, "models/a", &manifest(""))
            .await
            .unwrap();
        for index in 0..64 {
            let artifact_id = format!("unrelated-{index:03}");
            create_model_artifact(
                &storage,
                &artifact_id,
                1,
                &format!("models/{artifact_id}"),
                &manifest(""),
            )
            .await
            .unwrap();
        }

        assert!(
            get_model_artifact(&storage, "artifact-a")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn tensor_cursor_pages_are_bounded_and_scoped_to_the_requested_artifact() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        for index in 0..64 {
            create_model_tensors(
                &storage,
                &format!("unrelated-{index:03}"),
                &[tensor(&format!("noise-{index:03}"))],
            )
            .await
            .unwrap();
        }
        create_model_tensors(
            &storage,
            "artifact-a",
            &[tensor("c"), tensor("a"), tensor("b")],
        )
        .await
        .unwrap();

        let first = list_tensor_page(&storage, "artifact-a", None, 2)
            .await
            .unwrap();
        assert_eq!(
            first
                .tensors
                .iter()
                .map(|row| row.tensor_name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert!(first.next_cursor.is_some());

        let second = list_tensor_page(&storage, "artifact-a", first.next_cursor.as_deref(), 2)
            .await
            .unwrap();
        assert_eq!(
            second
                .tensors
                .iter()
                .map(|row| row.tensor_name.as_str())
                .collect::<Vec<_>>(),
            vec!["c"]
        );
        assert!(second.next_cursor.is_none());

        let error = list_tensor_page(&storage, "unrelated-000", first.next_cursor.as_deref(), 2)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("outside the requested prefix"));

        create_model_tensors(&storage, "artifact-a", &[tensor("b"), tensor("d")])
            .await
            .unwrap();
        assert_eq!(
            list_tensor_page(&storage, "artifact-a", None, 10)
                .await
                .unwrap()
                .tensors
                .into_iter()
                .map(|row| row.tensor_name)
                .collect::<Vec<_>>(),
            vec!["b".to_string(), "d".to_string()]
        );
        assert!(
            get_tensor_metadata(&storage, "artifact-a", "a")
                .await
                .unwrap()
                .is_none()
        );

        assert!(
            list_tensor_page(&storage, "artifact-a", None, 0)
                .await
                .unwrap_err()
                .to_string()
                .contains("page size")
        );
    }

    async fn ready_owner(
        storage: &Storage,
        owner_node_id: &str,
    ) -> crate::partition_fence::PartitionOwnerState {
        let family = "model_metadata".to_string();
        let id = hex::encode(model_partition_id());
        crate::partition_fence::ready_partition_owner_for_test(
            storage,
            family,
            id,
            owner_node_id,
            0,
            hex::encode([0; 32]),
            hex::encode([1; 32]),
            KEY,
        )
        .await
    }
}
