use crate::core_store::{
    AuthzScopeRef, CoreBoundaryValue, CoreByteRange, CoreManifestLocator, CoreObjectRef,
    CorePrefetchPolicy, CoreStore, GetBlob, ReadLogicalRangeRequest, SourceId, SourceKind,
    decode_core_object_ref_target, decode_manifest_locator_proto,
};
use crate::embedding_provider::{EmbeddingProviderRegistry, TEST_ONLY_EMBEDDING_PROVIDER};
use crate::formats::{
    full_text::FullTextDocument,
    full_text::FullTextIndexDefinition,
    hash32,
    vector::{VectorIndexDefinition, VectorMetric, VectorPayload, VectorRecord},
};
use crate::full_text_segment::{self, FullTextDocumentTableRow, FullTextSegmentWrite};
use crate::index_partition_watch::{self, IndexPartitionWatchPayload};
use crate::media_extraction::{
    DerivedAssetPolicy, DerivedOutputKind, MediaExtractionRequest, MediaObjectRef,
    execute_media_extraction,
};
use crate::metadata_journal;
use crate::partition_fence::{
    AcquireOwnership, MAX_OWNERSHIP_LEASE_MS, OwnershipPrincipal, OwnershipResource,
    OwnershipResourceKind, acquire_ownership,
};
use crate::persistence::{AppendStream, AppendStreamRecord, Bucket, IndexDefinition, Object};
use crate::storage::Storage;
use crate::typed_field_segment::{
    self, TypedFieldSegmentRow, TypedFieldSegmentWrite, encode_row_values, source_id_binary,
};
use crate::vector_segment::{self, VectorSegmentEntry, VectorSegmentWrite};
use crate::{
    derived_index_proof::{self, DerivedIndexProofWrite},
    watch_checkpoint::{self, WatchCheckpointUpdate, WatchCheckpointWriteAuthority},
};
use crate::{index_coremeta, index_journal};
use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use prost::Message;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};

mod authority;
use authority::IndexBuildOwnership;
pub(crate) use authority::{DirectRepairIndexBuildAuthority, IndexBuildAuthority};

#[derive(Debug, Clone, PartialEq)]
pub struct IndexBuildOutcome {
    pub index_storage_id: String,
    pub index_kind: String,
    pub generation: u64,
    pub item_count: usize,
    pub source_cursor: u128,
    pub segment_hashes: Vec<String>,
    pub diagnostics: Vec<IndexBuildDiagnostic>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexBuildDiagnostic {
    pub object_key: String,
    pub version_id: Option<uuid::Uuid>,
    pub severity: String,
    pub code: String,
    pub message: String,
    pub details: JsonValue,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDefinitionDigestProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    index_kind: String,
    #[prost(bytes = "vec", tag = "3")]
    selector_json: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    extractor_json: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    build_policy_json: Vec<u8>,
}

#[derive(Debug, Clone)]
struct OwnedFullTextDocument {
    document_id: u64,
    field_id: u16,
    object_version_id: [u8; 16],
    authz_label_hash: [u8; 32],
    authz_revision: i64,
    object_key: String,
    text: String,
}

#[derive(Debug, Clone)]
struct ExtractedTextField {
    text: String,
}

#[derive(Debug, Clone)]
struct TextExtractionDiagnostic {
    code: String,
    message: String,
    details: JsonValue,
}

#[derive(Debug, Clone)]
struct TextExtraction {
    fields: Vec<ExtractedTextField>,
    diagnostics: Vec<TextExtractionDiagnostic>,
}

#[derive(Debug, Clone)]
struct OwnedVectorDocument {
    vector_id: u64,
    source_id_binary: Vec<u8>,
    source_generation: u64,
    object_version_id: [u8; 16],
    chunk_id: u32,
    source_start: u64,
    source_len: u32,
    authz_label_hash: [u8; 32],
    authz_revision: i64,
    metadata_filter_bits: u64,
    values: Vec<f32>,
}

#[derive(Debug, Clone)]
struct TypedJsonBuildDefinition {
    source_kind: String,
    fields: Vec<TypedJsonBuildField>,
}

#[derive(Debug, Clone)]
struct TypedJsonBuildField {
    name: String,
    extractor: String,
    required: bool,
}

async fn boundary_values_for_objects(
    storage: &Storage,
    objects: &[Object],
) -> Result<Vec<CoreBoundaryValue>> {
    let store = CoreStore::new(storage.clone()).await?;
    let mut values: Vec<CoreBoundaryValue> = Vec::new();
    for object in objects {
        let Some(shard_map) = object.shard_map.as_ref() else {
            continue;
        };
        if shard_map.get("schema").and_then(JsonValue::as_str)
            != Some("anvil.core.object_data_target.v1")
        {
            continue;
        }
        let Some(kind) = shard_map.get("kind").and_then(JsonValue::as_str) else {
            continue;
        };
        let Some(target) = shard_map.get("target").and_then(JsonValue::as_str) else {
            continue;
        };
        match kind {
            "logical_file" => {
                let bytes = URL_SAFE_NO_PAD.decode(target)?;
                let locator = decode_manifest_locator_proto(&bytes)?;
                let manifest = store.read_logical_file_manifest(&locator).await?;
                for range in manifest.ranges {
                    for value in range.boundary_values {
                        if !values.contains(&value) {
                            values.push(value);
                        }
                    }
                }
            }
            _ => continue,
        }
    }
    Ok(values)
}

fn index_definition_hash(
    index_kind: &str,
    selector: &JsonValue,
    extractor: &JsonValue,
    build_policy: &JsonValue,
) -> Result<String> {
    let proto = IndexDefinitionDigestProto {
        schema: "anvil.index.definition_digest.v1".to_string(),
        index_kind: index_kind.to_string(),
        selector_json: canonical_json_bytes(selector)?,
        extractor_json: canonical_json_bytes(extractor)?,
        build_policy_json: canonical_json_bytes(build_policy)?,
    };
    Ok(blake3::hash(&encode_deterministic_proto(&proto)?)
        .to_hex()
        .to_string())
}

fn canonical_json_bytes(value: &JsonValue) -> Result<Vec<u8>> {
    serde_json::to_vec(&canonical_json(value)).map_err(Into::into)
}

fn canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(values) => JsonValue::Array(values.iter().map(canonical_json).collect()),
        JsonValue::Object(values) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonical_json(&values[key]));
            }
            JsonValue::Object(sorted)
        }
        scalar => scalar.clone(),
    }
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

async fn latest_index_segment_generation(storage: &Storage, index_storage_id: &str) -> Result<u64> {
    Ok(
        index_coremeta::latest_index_segment_coremeta_record(storage, index_storage_id)
            .await?
            .map(|record| record.generation)
            .unwrap_or(0),
    )
}

pub(crate) async fn build_full_text_index(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    builder_node_id: &str,
    authority: IndexBuildAuthority<'_>,
) -> Result<IndexBuildOutcome> {
    if index.kind != "full_text" {
        return Err(anyhow!("index build only supports full_text indexes"));
    }
    if !index.enabled {
        return Err(anyhow!("index build requires an enabled index"));
    }
    let definition = FullTextIndexDefinition::from_json(&index.build_policy)
        .map_err(|error| anyhow!("invalid full text index definition: {error}"))?;
    let index_storage_id =
        index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
    let latest_generation = latest_index_segment_generation(storage, &index_storage_id).await?;
    let latest_checkpoint_generation =
        latest_index_checkpoint_generation(storage, &index_storage_id, partition_owner_signing_key)
            .await?;
    let generation = latest_generation
        .max(latest_checkpoint_generation)
        .saturating_add(1)
        .max(u64::try_from(source_cursor).unwrap_or(u64::MAX))
        .max(1);

    let objects = metadata_journal::read_current_objects_through_sequence(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let boundary_values = boundary_values_for_objects(storage, &objects).await?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut owned_documents = Vec::new();
    let mut diagnostics = Vec::new();
    for object in objects {
        if object.deleted_at.is_some() || !selector_matches(&index.selector, &object) {
            continue;
        }
        let payload = read_object_payload(&core_store, &object).await?;
        let extracted = extract_text_fields(&index.extractor, &object, &payload);
        let diagnostic_count = extracted.diagnostics.len();
        for diagnostic in extracted.diagnostics {
            diagnostics.push(IndexBuildDiagnostic {
                object_key: object.key.clone(),
                version_id: Some(object.version_id),
                severity: "warning".to_string(),
                code: diagnostic.code,
                message: diagnostic.message,
                details: diagnostic.details,
            });
        }
        if extracted.fields.is_empty() && diagnostic_count == 0 {
            diagnostics.push(IndexBuildDiagnostic {
                object_key: object.key.clone(),
                version_id: Some(object.version_id),
                severity: "warning".to_string(),
                code: "TextExtractionEmpty".to_string(),
                message: "text extractor produced no fields for object version".to_string(),
                details: serde_json::json!({ "extractor": index.extractor }),
            });
        }
        for (field_idx, field) in extracted.fields.into_iter().enumerate() {
            if field.text.trim().is_empty() {
                continue;
            }
            let field_id = u16::try_from(field_idx.saturating_add(1)).unwrap_or(u16::MAX);
            owned_documents.push(OwnedFullTextDocument {
                document_id: object.id.max(0) as u64,
                field_id,
                object_version_id: *object.version_id.as_bytes(),
                authz_label_hash: object_authz_label_hash(bucket, &object),
                authz_revision: object.authz_revision,
                object_key: object.key.clone(),
                text: field.text,
            });
        }
    }

    let borrowed_documents = owned_documents
        .iter()
        .map(|document| FullTextDocument {
            document_id: document.document_id,
            field_id: document.field_id,
            object_version_id: document.object_version_id,
            authz_label_hash: document.authz_label_hash,
            text: document.text.as_str(),
        })
        .collect::<Vec<_>>();
    let built = crate::formats::full_text::build_full_text_postings(
        &borrowed_documents,
        &definition.tokenizer,
    );
    let document_table_rows = owned_documents
        .iter()
        .map(|document| FullTextDocumentTableRow {
            document_id: document.document_id,
            field_id: document.field_id,
            object_key: document.object_key.clone(),
            version_id: uuid::Uuid::from_bytes(document.object_version_id),
        })
        .collect::<Vec<_>>();
    let document_table = full_text_segment::encode_full_text_document_table(&document_table_rows)?;
    let staged_segment = full_text_segment::stage_full_text_segment(
        storage,
        FullTextSegmentWrite {
            index_id: &index_storage_id,
            generation,
            tokenizer: serde_json::to_value(&index.build_policy)?,
            scorer: serde_json::json!({ "kind": "bm25" }),
            source_cursor: u64::try_from(source_cursor).unwrap_or(u64::MAX),
            authz_revision: latest_authz_revision_for_documents(&owned_documents),
            boundary_values: &boundary_values,
            built_postings: &built,
            document_table: &document_table,
        },
    )
    .await?;
    let ownership = IndexBuildOwnership::acquire(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    let staged_for_catalog = &staged_segment;
    authority
        .publish_with(
            storage,
            &ownership,
            partition_owner_signing_key,
            |preconditions| async move {
                full_text_segment::publish_full_text_segment_catalog(
                    storage,
                    staged_for_catalog,
                    &preconditions,
                )
                .await
            },
        )
        .await?;
    let staged_for_locator = &staged_segment;
    authority
        .publish_with(
            storage,
            &ownership,
            partition_owner_signing_key,
            |preconditions| async move {
                full_text_segment::publish_full_text_segment_locator(
                    storage,
                    staged_for_locator,
                    &preconditions,
                )
                .await
            },
        )
        .await?;
    let segment_hash = staged_segment.segment_hash.clone();
    let partition_id = hex::encode(hash32(index_storage_id.as_bytes()));
    let source_manifest_hash = metadata_journal::object_metadata_source_checkpoint_hash(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let proof = publish_index_build_proof_and_checkpoint(
        storage,
        bucket,
        &index_storage_id,
        &index.kind,
        &partition_id,
        source_cursor,
        &source_manifest_hash,
        generation,
        &[segment_hash.clone()],
        builder_node_id,
        partition_owner_signing_key,
        authority,
        &ownership,
    )
    .await?;
    let watch_payload = IndexPartitionWatchPayload {
        index_id: index_storage_id.clone(),
        index_kind: index.kind.clone(),
        event_type: "segment_built".to_string(),
        generation,
        source_cursor,
        source_manifest_hash,
        proof_hash: proof
            .proof_hash
            .clone()
            .ok_or_else(|| anyhow!("derived index proof was not sealed"))?,
        segment_hashes: vec![segment_hash.clone()],
        emitted_at: String::new(),
    };
    publish_index_build_watch(
        storage,
        index.tenant_id,
        index.bucket_id,
        &partition_id,
        latest_authz_revision_for_documents(&owned_documents),
        watch_payload,
        builder_node_id,
        partition_owner_signing_key,
        authority,
        &ownership,
    )
    .await?;

    Ok(IndexBuildOutcome {
        index_storage_id,
        index_kind: "full_text".to_string(),
        generation,
        item_count: owned_documents.len(),
        source_cursor,
        segment_hashes: vec![segment_hash],
        diagnostics,
    })
}

pub(crate) async fn build_typed_json_index(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    builder_node_id: &str,
    authority: IndexBuildAuthority<'_>,
) -> Result<IndexBuildOutcome> {
    if index.kind != "typed_json" {
        return Err(anyhow!("index build only supports typed_json indexes"));
    }
    if !index.enabled {
        return Err(anyhow!("index build requires an enabled index"));
    }
    let definition = parse_typed_json_build_definition(index)?;
    let index_storage_id =
        index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
    let latest_generation = latest_index_segment_generation(storage, &index_storage_id).await?;
    let latest_checkpoint_generation =
        latest_index_checkpoint_generation(storage, &index_storage_id, partition_owner_signing_key)
            .await?;
    let generation = latest_generation
        .max(latest_checkpoint_generation)
        .saturating_add(1)
        .max(u64::try_from(source_cursor).unwrap_or(u64::MAX))
        .max(1);

    let core_store = CoreStore::new(storage.clone()).await?;
    let (rows, diagnostics, boundary_values) = match definition.source_kind.as_str() {
        "object_current" => {
            build_typed_json_object_rows(
                storage,
                bucket,
                index,
                &definition,
                &core_store,
                partition_owner_signing_key,
                source_cursor,
            )
            .await?
        }
        "append_record" => {
            build_typed_json_append_rows(bucket, index, &definition, &core_store, source_cursor)
                .await?
        }
        _ => return Err(anyhow!("unsupported typed_json source kind")),
    };

    let field_names = definition
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    let definition_hash = index_definition_hash(
        "typed_json",
        &index.selector,
        &index.extractor,
        &index.build_policy,
    )?;
    let staged_segment = typed_field_segment::stage_typed_field_segment(
        storage,
        TypedFieldSegmentWrite {
            index_id: &index_storage_id,
            generation,
            source_kind: &definition.source_kind,
            source_cursor: u64::try_from(source_cursor).unwrap_or(u64::MAX),
            authz_revision: latest_authz_revision_for_typed_rows(&rows),
            boundary_values: &boundary_values,
            definition_hash: &definition_hash,
            field_names: &field_names,
            rows: &rows,
        },
    )
    .await?;
    let ownership = IndexBuildOwnership::acquire(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    let staged_for_catalog = &staged_segment;
    authority
        .publish_with(
            storage,
            &ownership,
            partition_owner_signing_key,
            |preconditions| async move {
                typed_field_segment::publish_typed_field_segment_catalog(
                    storage,
                    staged_for_catalog,
                    &preconditions,
                )
                .await
            },
        )
        .await?;
    let staged_for_locator = &staged_segment;
    authority
        .publish_with(
            storage,
            &ownership,
            partition_owner_signing_key,
            |preconditions| async move {
                typed_field_segment::publish_typed_field_segment_locator(
                    storage,
                    staged_for_locator,
                    &preconditions,
                )
                .await
            },
        )
        .await?;
    let segment_hash = staged_segment.segment_hash.clone();
    let partition_id = hex::encode(hash32(index_storage_id.as_bytes()));
    let source_manifest_hash = typed_json_source_manifest_hash(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
        &definition.source_kind,
    )
    .await?;
    let proof = publish_index_build_proof_and_checkpoint(
        storage,
        bucket,
        &index_storage_id,
        &index.kind,
        &partition_id,
        source_cursor,
        &source_manifest_hash,
        generation,
        &[segment_hash.clone()],
        builder_node_id,
        partition_owner_signing_key,
        authority,
        &ownership,
    )
    .await?;
    let watch_payload = IndexPartitionWatchPayload {
        index_id: index_storage_id.clone(),
        index_kind: index.kind.clone(),
        event_type: "segment_built".to_string(),
        generation,
        source_cursor,
        source_manifest_hash,
        proof_hash: proof
            .proof_hash
            .clone()
            .ok_or_else(|| anyhow!("derived index proof was not sealed"))?,
        segment_hashes: vec![segment_hash.clone()],
        emitted_at: String::new(),
    };
    publish_index_build_watch(
        storage,
        index.tenant_id,
        index.bucket_id,
        &partition_id,
        latest_authz_revision_for_typed_rows(&rows),
        watch_payload,
        builder_node_id,
        partition_owner_signing_key,
        authority,
        &ownership,
    )
    .await?;

    Ok(IndexBuildOutcome {
        index_storage_id,
        index_kind: index.kind.clone(),
        generation,
        item_count: rows.len(),
        source_cursor,
        segment_hashes: vec![segment_hash],
        diagnostics,
    })
}

pub(crate) async fn build_metadata_backed_index(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    builder_node_id: &str,
    authority: IndexBuildAuthority<'_>,
) -> Result<IndexBuildOutcome> {
    if !matches!(index.kind.as_str(), "path" | "metadata_filter") {
        return Err(anyhow!(
            "index build only supports path and metadata_filter indexes"
        ));
    }
    if !index.enabled {
        return Err(anyhow!("index build requires an enabled index"));
    }
    let index_storage_id =
        index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
    let latest_generation = latest_index_segment_generation(storage, &index_storage_id).await?;
    let latest_checkpoint_generation =
        latest_index_checkpoint_generation(storage, &index_storage_id, partition_owner_signing_key)
            .await?;
    let generation = latest_generation
        .max(latest_checkpoint_generation)
        .saturating_add(1)
        .max(u64::try_from(source_cursor).unwrap_or(u64::MAX))
        .max(1);

    let objects = metadata_journal::read_current_objects_through_sequence(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let boundary_values = boundary_values_for_objects(storage, &objects).await?;

    let mut rows = Vec::new();
    let mut field_names = BTreeMap::<String, ()>::new();
    for object in objects {
        if object.deleted_at.is_some() || !selector_matches(&index.selector, &object) {
            continue;
        }
        let row = metadata_backed_row_from_object(bucket, &object)?;
        for field in row.values.keys() {
            field_names.insert(field.clone(), ());
        }
        rows.push(row);
    }
    let field_names = field_names.into_keys().collect::<Vec<_>>();
    let definition_hash = index_definition_hash(
        &index.kind,
        &index.selector,
        &index.extractor,
        &index.build_policy,
    )?;
    let staged_segment = typed_field_segment::stage_typed_field_segment(
        storage,
        TypedFieldSegmentWrite {
            index_id: &index_storage_id,
            generation,
            source_kind: "object_metadata",
            source_cursor: u64::try_from(source_cursor).unwrap_or(u64::MAX),
            authz_revision: latest_authz_revision_for_typed_rows(&rows),
            boundary_values: &boundary_values,
            definition_hash: &definition_hash,
            field_names: &field_names,
            rows: &rows,
        },
    )
    .await?;
    let ownership = IndexBuildOwnership::acquire(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    let staged_for_catalog = &staged_segment;
    authority
        .publish_with(
            storage,
            &ownership,
            partition_owner_signing_key,
            |preconditions| async move {
                typed_field_segment::publish_typed_field_segment_catalog(
                    storage,
                    staged_for_catalog,
                    &preconditions,
                )
                .await
            },
        )
        .await?;
    let staged_for_locator = &staged_segment;
    authority
        .publish_with(
            storage,
            &ownership,
            partition_owner_signing_key,
            |preconditions| async move {
                typed_field_segment::publish_typed_field_segment_locator(
                    storage,
                    staged_for_locator,
                    &preconditions,
                )
                .await
            },
        )
        .await?;
    let segment_hash = staged_segment.segment_hash.clone();
    let partition_id = hex::encode(hash32(index_storage_id.as_bytes()));
    let source_manifest_hash = metadata_journal::object_metadata_source_checkpoint_hash(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let proof = publish_index_build_proof_and_checkpoint(
        storage,
        bucket,
        &index_storage_id,
        &index.kind,
        &partition_id,
        source_cursor,
        &source_manifest_hash,
        generation,
        &[segment_hash.clone()],
        builder_node_id,
        partition_owner_signing_key,
        authority,
        &ownership,
    )
    .await?;
    let watch_payload = IndexPartitionWatchPayload {
        index_id: index_storage_id.clone(),
        index_kind: index.kind.clone(),
        event_type: "segment_built".to_string(),
        generation,
        source_cursor,
        source_manifest_hash,
        proof_hash: proof
            .proof_hash
            .clone()
            .ok_or_else(|| anyhow!("derived index proof was not sealed"))?,
        segment_hashes: vec![segment_hash.clone()],
        emitted_at: String::new(),
    };
    publish_index_build_watch(
        storage,
        index.tenant_id,
        index.bucket_id,
        &partition_id,
        latest_authz_revision_for_typed_rows(&rows),
        watch_payload,
        builder_node_id,
        partition_owner_signing_key,
        authority,
        &ownership,
    )
    .await?;

    Ok(IndexBuildOutcome {
        index_storage_id,
        index_kind: index.kind.clone(),
        generation,
        item_count: rows.len(),
        source_cursor,
        segment_hashes: vec![segment_hash],
        diagnostics: Vec::new(),
    })
}

pub(crate) async fn build_vector_index(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    builder_node_id: &str,
    embedding_providers: &EmbeddingProviderRegistry,
    authority: IndexBuildAuthority<'_>,
) -> Result<IndexBuildOutcome> {
    if index.kind != "vector" {
        return Err(anyhow!("index build only supports vector indexes"));
    }
    build_vector_index_with_policy(
        storage,
        bucket,
        index,
        &index.build_policy,
        &index.extractor,
        partition_owner_signing_key,
        source_cursor,
        builder_node_id,
        "vector",
        embedding_providers,
        authority,
    )
    .await
}

pub(crate) async fn build_hybrid_index(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    builder_node_id: &str,
    embedding_providers: &EmbeddingProviderRegistry,
    authority: IndexBuildAuthority<'_>,
) -> Result<IndexBuildOutcome> {
    if index.kind != "hybrid" {
        return Err(anyhow!("index build only supports hybrid indexes"));
    }
    if !index.enabled {
        return Err(anyhow!("index build requires an enabled index"));
    }
    let full_text_policy = index
        .build_policy
        .get("full_text")
        .ok_or_else(|| anyhow!("hybrid index build policy is missing full_text"))?;
    let vector_policy = index
        .build_policy
        .get("vector")
        .ok_or_else(|| anyhow!("hybrid index build policy is missing vector"))?;
    let text_extractor = index.extractor.get("text").unwrap_or(&index.extractor);
    let vector_extractor = index.extractor.get("vector").unwrap_or(&index.extractor);

    let text_index = IndexDefinition {
        kind: "full_text".to_string(),
        build_policy: full_text_policy.clone(),
        extractor: text_extractor.clone(),
        ..index.clone()
    };
    let text_outcome = build_full_text_index(
        storage,
        bucket,
        &text_index,
        partition_owner_signing_key,
        source_cursor,
        builder_node_id,
        authority,
    )
    .await?;
    let vector_outcome = build_vector_index_with_policy(
        storage,
        bucket,
        index,
        vector_policy,
        vector_extractor,
        partition_owner_signing_key,
        source_cursor,
        builder_node_id,
        "hybrid",
        embedding_providers,
        authority,
    )
    .await?;

    let mut segment_hashes = text_outcome.segment_hashes;
    segment_hashes.extend(vector_outcome.segment_hashes);
    let mut diagnostics = text_outcome.diagnostics;
    diagnostics.extend(vector_outcome.diagnostics);
    Ok(IndexBuildOutcome {
        index_storage_id: vector_outcome.index_storage_id,
        index_kind: "hybrid".to_string(),
        generation: text_outcome.generation.max(vector_outcome.generation),
        item_count: text_outcome
            .item_count
            .saturating_add(vector_outcome.item_count),
        source_cursor,
        segment_hashes,
        diagnostics,
    })
}

async fn build_vector_index_with_policy(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    build_policy: &JsonValue,
    _extractor: &JsonValue,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    builder_node_id: &str,
    outcome_kind: &str,
    embedding_providers: &EmbeddingProviderRegistry,
    authority: IndexBuildAuthority<'_>,
) -> Result<IndexBuildOutcome> {
    if !index.enabled {
        return Err(anyhow!("index build requires an enabled index"));
    }
    let definition = VectorIndexDefinition::from_json(build_policy)
        .map_err(|error| anyhow!("invalid vector index definition: {error}"))?;
    let index_storage_id =
        index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
    let latest_generation = latest_index_segment_generation(storage, &index_storage_id).await?;
    let latest_checkpoint_generation =
        latest_index_checkpoint_generation(storage, &index_storage_id, partition_owner_signing_key)
            .await?;
    let generation = latest_generation
        .max(latest_checkpoint_generation)
        .saturating_add(1)
        .max(u64::try_from(source_cursor).unwrap_or(u64::MAX))
        .max(1);

    let objects = metadata_journal::read_current_objects_through_sequence(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let boundary_values = boundary_values_for_objects(storage, &objects).await?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut vector_documents = Vec::new();
    let mut diagnostics = Vec::new();
    for object in objects {
        if object.deleted_at.is_some() || !selector_matches(&index.selector, &object) {
            continue;
        }
        let payload = read_object_payload(&core_store, &object).await?;
        let extraction = extract_vectors(
            &definition.extractor,
            &payload,
            &definition,
            embedding_providers,
        )
        .await;
        let diagnostic_count = extraction.diagnostics.len();
        for diagnostic in extraction.diagnostics {
            diagnostics.push(IndexBuildDiagnostic {
                object_key: object.key.clone(),
                version_id: Some(object.version_id),
                severity: "error".to_string(),
                code: diagnostic.code,
                message: diagnostic.message,
                details: diagnostic.details,
            });
        }
        if extraction.vectors.is_empty() {
            if diagnostic_count == 0 {
                diagnostics.push(IndexBuildDiagnostic {
                    object_key: object.key.clone(),
                    version_id: Some(object.version_id),
                    severity: "warning".to_string(),
                    code: "VectorExtractionEmpty".to_string(),
                    message: "vector extractor produced no vectors for object version".to_string(),
                    details: serde_json::json!({
                        "content_type": object.content_type,
                        "extractor": &definition.extractor,
                    }),
                });
            }
            continue;
        }
        for vector in extraction.vectors {
            vector_documents.push(OwnedVectorDocument {
                vector_id: vector_documents.len().saturating_add(1) as u64,
                source_id_binary: source_id_binary(&object_current_source_id(bucket, &object))?,
                source_generation: object.id.max(0) as u64,
                object_version_id: *object.version_id.as_bytes(),
                chunk_id: vector.chunk_id,
                source_start: vector.source_start,
                source_len: vector.source_len,
                authz_label_hash: object_authz_label_hash(bucket, &object),
                authz_revision: object.authz_revision,
                metadata_filter_bits: 0,
                values: vector.values,
            });
        }
    }

    let entries = vector_documents
        .iter()
        .map(|document| VectorSegmentEntry {
            source_id_binary: document.source_id_binary.clone(),
            source_generation: document.source_generation,
            labels: if document.metadata_filter_bits == 0 {
                Vec::new()
            } else {
                vec![document.metadata_filter_bits]
            },
            record: VectorRecord {
                vector_id: document.vector_id,
                object_version_id: document.object_version_id,
                chunk_id: document.chunk_id,
                modality: definition.modality as u8,
                metric: definition.metric as u8,
                dimension: definition.dimension,
                vector_payload_offset: 0,
                source_start: document.source_start,
                source_len: document.source_len,
                authz_label_hash: document.authz_label_hash,
                metadata_filter_bits: document.metadata_filter_bits,
            },
            payload: VectorPayload {
                dimension: definition.dimension,
                values: document.values.clone(),
            },
        })
        .collect::<Vec<_>>();
    let deleted_bitset = vec![0; entries.len().div_ceil(8)];
    let definition_hash = definition.definition_hash.clone();
    let staged_segment = vector_segment::stage_vector_segment(
        storage,
        VectorSegmentWrite {
            index_id: &index_storage_id,
            definition_hash: &definition_hash,
            generation,
            dimension: definition.dimension,
            metric: definition.metric,
            embedding_provider: &definition.embedding_provider,
            embedding_model: &definition.embedding_model,
            embedding_model_version: definition.embedding_model_version.as_deref(),
            embedding_normalisation: &definition.normalisation,
            embedding_chunking_hash: &definition.chunking_hash,
            extractor_definition_hash: &definition.extractor_hash,
            embedding_provenance_hash: &definition.provenance_hash,
            modality: definition.modality,
            hnsw_m: definition.hnsw_m,
            hnsw_ef_construction: definition.hnsw_ef_construction,
            source_cursor: u64::try_from(source_cursor).unwrap_or(u64::MAX),
            authz_revision: latest_authz_revision_for_vectors(&vector_documents),
            boundary_values: &boundary_values,
            entries: &entries,
            deleted_bitset: &deleted_bitset,
        },
    )
    .await?;
    let ownership = IndexBuildOwnership::acquire(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    let staged_for_catalog = &staged_segment;
    authority
        .publish_with(
            storage,
            &ownership,
            partition_owner_signing_key,
            |preconditions| async move {
                vector_segment::publish_vector_segment_catalog(
                    storage,
                    staged_for_catalog,
                    &preconditions,
                )
                .await
            },
        )
        .await?;
    let staged_for_locator = &staged_segment;
    authority
        .publish_with(
            storage,
            &ownership,
            partition_owner_signing_key,
            |preconditions| async move {
                vector_segment::publish_vector_segment_locator(
                    storage,
                    staged_for_locator,
                    &preconditions,
                )
                .await
            },
        )
        .await?;
    let segment_hash = staged_segment.segment_hash.clone();
    let partition_id = hex::encode(hash32(index_storage_id.as_bytes()));
    let source_manifest_hash = metadata_journal::object_metadata_source_checkpoint_hash(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let proof = publish_index_build_proof_and_checkpoint(
        storage,
        bucket,
        &index_storage_id,
        outcome_kind,
        &partition_id,
        source_cursor,
        &source_manifest_hash,
        generation,
        &[segment_hash.clone()],
        builder_node_id,
        partition_owner_signing_key,
        authority,
        &ownership,
    )
    .await?;
    let watch_payload = IndexPartitionWatchPayload {
        index_id: index_storage_id.clone(),
        index_kind: outcome_kind.to_string(),
        event_type: "segment_built".to_string(),
        generation,
        source_cursor,
        source_manifest_hash,
        proof_hash: proof
            .proof_hash
            .clone()
            .ok_or_else(|| anyhow!("derived index proof was not sealed"))?,
        segment_hashes: vec![segment_hash.clone()],
        emitted_at: String::new(),
    };
    publish_index_build_watch(
        storage,
        index.tenant_id,
        index.bucket_id,
        &partition_id,
        latest_authz_revision_for_vectors(&vector_documents),
        watch_payload,
        builder_node_id,
        partition_owner_signing_key,
        authority,
        &ownership,
    )
    .await?;

    Ok(IndexBuildOutcome {
        index_storage_id,
        index_kind: outcome_kind.to_string(),
        generation,
        item_count: entries.len(),
        source_cursor,
        segment_hashes: vec![segment_hash],
        diagnostics,
    })
}

#[allow(clippy::too_many_arguments)]
async fn publish_index_build_proof_and_checkpoint(
    storage: &Storage,
    bucket: &Bucket,
    index_storage_id: &str,
    index_kind: &str,
    index_partition_id: &str,
    source_cursor: u128,
    source_manifest_hash: &str,
    generation: u64,
    segment_hashes: &[String],
    builder_node_id: &str,
    signing_key: &[u8],
    authority: IndexBuildAuthority<'_>,
    ownership: &IndexBuildOwnership,
) -> Result<derived_index_proof::DerivedIndexProof> {
    let payload_actor = authority.deterministic_payload_actor(builder_node_id).await;
    let publication_digest = deterministic_build_digest(source_manifest_hash, segment_hashes);
    let built_at_nanos = index_coremeta::deterministic_index_publication_nanos(
        index_storage_id,
        "proof_checkpoint",
        generation,
        source_cursor,
        &publication_digest,
    );
    let prepared_proof = derived_index_proof::prepare_derived_index_proof(
        DerivedIndexProofWrite {
            index_id: index_storage_id.to_string(),
            index_kind: index_kind.to_string(),
            partition_family: "index".to_string(),
            partition_id: index_partition_id.to_string(),
            source_watch_stream_id: "object_metadata".to_string(),
            source_cursor,
            source_manifest_hash: source_manifest_hash.to_string(),
            generation,
            segment_hashes: segment_hashes.to_vec(),
            built_by_node: payload_actor.clone(),
            built_at_nanos,
        },
        signing_key,
    )?;
    let proof_for_publication = &prepared_proof;
    let proof = authority
        .publish_with(
            storage,
            ownership,
            signing_key,
            |preconditions| async move {
                derived_index_proof::publish_prepared_derived_index_proof(
                    storage,
                    proof_for_publication,
                    &preconditions,
                )
                .await
            },
        )
        .await?;
    let checkpoint_update = WatchCheckpointUpdate {
        watch_stream_id: "object_metadata".to_string(),
        partition_family: "object_metadata".to_string(),
        partition_id: hex::encode(metadata_journal::object_metadata_partition_id(
            bucket.tenant_id,
            bucket.id,
        )),
        consumer_id: index_storage_id.to_string(),
        cursor: source_cursor,
        source_cursor_high: source_cursor,
        lag_record_count_hint: 0,
        source_manifest_hash: source_manifest_hash.to_string(),
        generation,
        updated_by_node: payload_actor.clone(),
        updated_at_nanos: built_at_nanos,
    };
    let checkpoint_authority = acquire_watch_checkpoint_authority(
        storage,
        &checkpoint_update,
        &payload_actor,
        signing_key,
    )
    .await?;
    let prepared_checkpoint = watch_checkpoint::prepare_watch_checkpoint(
        storage,
        checkpoint_update,
        checkpoint_authority,
        signing_key,
    )
    .await?;
    let checkpoint_for_publication = &prepared_checkpoint;
    let checkpoint = authority
        .publish_with(
            storage,
            ownership,
            signing_key,
            |preconditions| async move {
                watch_checkpoint::publish_prepared_watch_checkpoint(
                    storage,
                    checkpoint_for_publication,
                    &preconditions,
                )
                .await
            },
        )
        .await?;
    watch_checkpoint::record_watch_checkpoint_lag(storage, &checkpoint).await?;
    Ok(proof)
}

fn deterministic_build_digest(source_manifest_hash: &str, segment_hashes: &[String]) -> String {
    let mut input = Vec::new();
    input.extend_from_slice(source_manifest_hash.as_bytes());
    input.push(0);
    for segment_hash in segment_hashes {
        input.extend_from_slice(segment_hash.as_bytes());
        input.push(0);
    }
    blake3::hash(&input).to_hex().to_string()
}

#[allow(clippy::too_many_arguments)]
async fn publish_index_build_watch(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    partition_id: &str,
    authz_revision: u64,
    mut payload: IndexPartitionWatchPayload,
    builder_node_id: &str,
    signing_key: &[u8],
    authority: IndexBuildAuthority<'_>,
    ownership: &IndexBuildOwnership,
) -> Result<()> {
    let digest = deterministic_build_digest(&payload.proof_hash, &payload.segment_hashes);
    let emitted_at_nanos = index_coremeta::deterministic_index_publication_nanos(
        &payload.index_id,
        "partition_watch",
        payload.generation,
        payload.source_cursor,
        &digest,
    );
    payload.emitted_at = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(emitted_at_nanos)
        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let mutation_id = index_coremeta::deterministic_index_mutation_id(
        &payload.index_id,
        "partition_watch",
        payload.generation,
        payload.source_cursor,
        &digest,
    );
    let watch_authority = acquire_index_partition_watch_authority(
        storage,
        tenant_id,
        bucket_id,
        partition_id,
        &payload,
        builder_node_id,
        signing_key,
    )
    .await?;
    let prepared = index_partition_watch::prepare_index_partition_watch_record(
        storage,
        tenant_id,
        bucket_id,
        partition_id,
        mutation_id,
        authz_revision,
        payload,
        watch_authority,
        signing_key,
    )
    .await?;
    authority
        .publish_with(
            storage,
            ownership,
            signing_key,
            |preconditions| async move {
                index_partition_watch::publish_prepared_index_partition_watch(
                    storage,
                    prepared,
                    &preconditions,
                )
                .await
                .map(|_| ())
            },
        )
        .await
}

async fn latest_index_checkpoint_generation(
    storage: &Storage,
    index_storage_id: &str,
    signing_key: &[u8],
) -> Result<u64> {
    Ok(watch_checkpoint::read_watch_checkpoint(
        storage,
        "object_metadata",
        index_storage_id,
        signing_key,
    )
    .await?
    .map(|checkpoint| checkpoint.generation)
    .unwrap_or(0))
}

async fn acquire_watch_checkpoint_authority(
    storage: &Storage,
    update: &WatchCheckpointUpdate,
    builder_node_id: &str,
    signing_key: &[u8],
) -> Result<WatchCheckpointWriteAuthority> {
    let resource_id = watch_checkpoint::watch_checkpoint_resource_id(
        &update.watch_stream_id,
        &update.partition_id,
        &update.consumer_id,
    );
    let now_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp cannot be represented in nanoseconds"))?;
    let outcome = acquire_ownership(
        storage,
        AcquireOwnership {
            request_id: format!("watch-checkpoint-{resource_id}-{builder_node_id}"),
            idempotency_key: format!("watch-checkpoint-{resource_id}-{builder_node_id}"),
            resource: OwnershipResource {
                resource_kind: OwnershipResourceKind::WatchPartition,
                resource_id: resource_id.clone(),
            },
            owner: OwnershipPrincipal::node(builder_node_id),
            now_nanos,
            ttl_nanos: i64::try_from(MAX_OWNERSHIP_LEASE_MS)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000_000),
        },
        signing_key,
    )
    .await?;
    Ok(WatchCheckpointWriteAuthority {
        owner_node_id: builder_node_id.to_string(),
        fence: outcome.record.fence,
        resource_id,
    })
}

#[allow(clippy::too_many_arguments)]
async fn acquire_index_partition_watch_authority(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    partition_id: &str,
    payload: &IndexPartitionWatchPayload,
    builder_node_id: &str,
    signing_key: &[u8],
) -> Result<index_partition_watch::IndexPartitionWatchWriteAuthority> {
    let resource_id = index_partition_watch::index_partition_watch_resource_id(
        tenant_id,
        bucket_id,
        &payload.index_id,
        partition_id,
    );
    let now_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp cannot be represented in nanoseconds"))?;
    let outcome = acquire_ownership(
        storage,
        AcquireOwnership {
            request_id: format!("index-watch-{resource_id}-{builder_node_id}"),
            idempotency_key: format!("index-watch-{resource_id}-{builder_node_id}"),
            resource: OwnershipResource {
                resource_kind: OwnershipResourceKind::WatchPartition,
                resource_id: resource_id.clone(),
            },
            owner: OwnershipPrincipal::node(builder_node_id),
            now_nanos,
            ttl_nanos: i64::try_from(MAX_OWNERSHIP_LEASE_MS)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000_000),
        },
        signing_key,
    )
    .await?;
    Ok(index_partition_watch::IndexPartitionWatchWriteAuthority {
        owner_node_id: builder_node_id.to_string(),
        fence: outcome.record.fence,
        resource_id,
    })
}

#[derive(Debug, Clone)]
struct ExtractedVector {
    chunk_id: u32,
    source_start: u64,
    source_len: u32,
    values: Vec<f32>,
}

#[derive(Debug, Clone)]
struct VectorExtractionDiagnostic {
    code: String,
    message: String,
    details: JsonValue,
}

#[derive(Debug, Clone)]
struct VectorExtraction {
    vectors: Vec<ExtractedVector>,
    diagnostics: Vec<VectorExtractionDiagnostic>,
}

#[derive(Debug, Deserialize)]
struct JsonVectorRecord {
    vector: Option<Vec<f32>>,
    values: Option<Vec<f32>>,
    embedding: Option<Vec<f32>>,
    chunk_id: Option<u32>,
    source_start: Option<u64>,
    source_len: Option<u32>,
}

async fn extract_vectors(
    extractor: &JsonValue,
    payload: &[u8],
    definition: &VectorIndexDefinition,
    embedding_providers: &EmbeddingProviderRegistry,
) -> VectorExtraction {
    let kind = extractor
        .get("kind")
        .and_then(JsonValue::as_str)
        .unwrap_or("object_body_utf8");
    match kind {
        "object_body_json_vector" | "object_body_json" | "json_vector" => {
            extract_json_vectors(extractor, payload, definition)
        }
        "object_body_f32_le" | "f32_le" => extract_f32_le_vectors(payload, definition),
        "object_body_utf8" | "utf8" | "body" => {
            extract_provider_embedding(extractor, payload, definition, embedding_providers).await
        }
        _ => VectorExtraction {
            vectors: Vec::new(),
            diagnostics: vec![VectorExtractionDiagnostic {
                code: "UnsupportedVectorExtractor".to_string(),
                message: format!("unsupported vector extractor kind `{kind}`"),
                details: serde_json::json!({ "kind": kind }),
            }],
        },
    }
}

mod helpers;
use helpers::*;

#[cfg(test)]
mod tests;
