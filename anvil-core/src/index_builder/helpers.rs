use super::*;

pub(super) async fn extract_provider_embedding(
    extractor: &JsonValue,
    payload: &[u8],
    definition: &VectorIndexDefinition,
    embedding_providers: &EmbeddingProviderRegistry,
) -> VectorExtraction {
    if definition.embedding_provider == TEST_ONLY_EMBEDDING_PROVIDER {
        if embedding_providers.is_test_only_allowed() {
            return extract_test_only_embedding(payload, definition);
        }
        return VectorExtraction {
            vectors: Vec::new(),
            diagnostics: vec![VectorExtractionDiagnostic {
                code: "TestOnlyEmbeddingProviderDisabled".to_string(),
                message: "test_only vector embedding provider is disabled for this server"
                    .to_string(),
                details: serde_json::json!({ "provider": "test_only" }),
            }],
        };
    }

    let response = match embedding_providers
        .embed_text(definition, extractor, payload)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return VectorExtraction {
                vectors: Vec::new(),
                diagnostics: vec![VectorExtractionDiagnostic {
                    code: if embedding_providers.has_provider(&definition.embedding_provider) {
                        "EmbeddingProviderFailed"
                    } else {
                        "EmbeddingProviderNotConfigured"
                    }
                    .to_string(),
                    message: error.to_string(),
                    details: serde_json::json!({ "provider": definition.embedding_provider }),
                }],
            };
        }
    };
    if let (Some(expected), Some(actual)) = (
        definition.embedding_model_version.as_deref(),
        response.model_version.as_deref(),
    ) && expected != actual
    {
        return VectorExtraction {
            vectors: Vec::new(),
            diagnostics: vec![VectorExtractionDiagnostic {
                code: "EmbeddingProviderModelVersionMismatch".to_string(),
                message: "embedding provider returned a different model version than the index definition".to_string(),
                details: serde_json::json!({
                    "provider": definition.embedding_provider,
                    "expected_model_version": expected,
                    "actual_model_version": actual,
                }),
            }],
        };
    }
    vector_extraction_from_vectors(
        response
            .vectors
            .into_iter()
            .map(|vector| ExtractedVector {
                chunk_id: vector.chunk_id.unwrap_or(0),
                source_start: vector.source_start.unwrap_or(0),
                source_len: vector
                    .source_len
                    .unwrap_or_else(|| u32::try_from(payload.len()).unwrap_or(u32::MAX)),
                values: vector.values,
            })
            .collect(),
        definition,
    )
}

pub(super) fn extract_json_vectors(
    extractor: &JsonValue,
    payload: &[u8],
    definition: &VectorIndexDefinition,
) -> VectorExtraction {
    let Ok(json) = serde_json::from_slice::<JsonValue>(payload) else {
        return VectorExtraction {
            vectors: Vec::new(),
            diagnostics: vec![VectorExtractionDiagnostic {
                code: "VectorJsonDecodeFailed".to_string(),
                message: "object body is not valid JSON for vector extraction".to_string(),
                details: JsonValue::Null,
            }],
        };
    };
    let selected = extractor
        .get("json_pointer")
        .or_else(|| extractor.get("vector_pointer"))
        .or_else(|| extractor.get("pointer"))
        .and_then(JsonValue::as_str)
        .and_then(|pointer| json.pointer(pointer))
        .unwrap_or(&json);
    vectors_from_json_value(selected, definition)
}

pub(super) fn vectors_from_json_value(
    value: &JsonValue,
    definition: &VectorIndexDefinition,
) -> VectorExtraction {
    if let Some(vector) = parse_json_vector(value) {
        return vector_extraction_from_vectors(
            vec![ExtractedVector {
                chunk_id: 0,
                source_start: 0,
                source_len: 0,
                values: vector,
            }],
            definition,
        );
    }
    if let Ok(record) = serde_json::from_value::<JsonVectorRecord>(value.clone()) {
        if let Some(values) = record.vector.or(record.values).or(record.embedding) {
            return vector_extraction_from_vectors(
                vec![ExtractedVector {
                    chunk_id: record.chunk_id.unwrap_or(0),
                    source_start: record.source_start.unwrap_or(0),
                    source_len: record.source_len.unwrap_or(0),
                    values,
                }],
                definition,
            );
        }
    }
    if let Some(array) = value
        .as_object()
        .and_then(|object| object.get("vectors"))
        .and_then(JsonValue::as_array)
        .or_else(|| value.as_array())
    {
        let mut vectors = Vec::new();
        let mut diagnostics = Vec::new();
        for (idx, item) in array.iter().enumerate() {
            if let Some(values) = parse_json_vector(item) {
                vectors.push(ExtractedVector {
                    chunk_id: u32::try_from(idx).unwrap_or(u32::MAX),
                    source_start: 0,
                    source_len: 0,
                    values,
                });
                continue;
            }
            match serde_json::from_value::<JsonVectorRecord>(item.clone()) {
                Ok(record) => {
                    if let Some(values) = record.vector.or(record.values).or(record.embedding) {
                        vectors.push(ExtractedVector {
                            chunk_id: record
                                .chunk_id
                                .unwrap_or_else(|| u32::try_from(idx).unwrap_or(u32::MAX)),
                            source_start: record.source_start.unwrap_or(0),
                            source_len: record.source_len.unwrap_or(0),
                            values,
                        });
                    }
                }
                Err(error) => diagnostics.push(VectorExtractionDiagnostic {
                    code: "VectorJsonEntryDecodeFailed".to_string(),
                    message: "JSON vector entry could not be decoded".to_string(),
                    details: serde_json::json!({ "entry_index": idx, "error": error.to_string() }),
                }),
            }
        }
        let mut extraction = vector_extraction_from_vectors(vectors, definition);
        extraction.diagnostics.extend(diagnostics);
        return extraction;
    }
    VectorExtraction {
        vectors: Vec::new(),
        diagnostics: vec![VectorExtractionDiagnostic {
            code: "VectorJsonShapeUnsupported".to_string(),
            message: "JSON payload does not contain a vector or vector record".to_string(),
            details: JsonValue::Null,
        }],
    }
}

pub(super) fn parse_json_vector(value: &JsonValue) -> Option<Vec<f32>> {
    let array = value.as_array()?;
    let mut values = Vec::with_capacity(array.len());
    for item in array {
        values.push(item.as_f64()? as f32);
    }
    Some(values)
}

pub(super) fn extract_f32_le_vectors(
    payload: &[u8],
    definition: &VectorIndexDefinition,
) -> VectorExtraction {
    if !payload.len().is_multiple_of(4) {
        return VectorExtraction {
            vectors: Vec::new(),
            diagnostics: vec![VectorExtractionDiagnostic {
                code: "VectorPayloadLengthInvalid".to_string(),
                message: "raw f32 vector payload length is not divisible by four".to_string(),
                details: serde_json::json!({ "byte_len": payload.len() }),
            }],
        };
    }
    let values = payload
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();
    vector_extraction_from_vectors(
        vec![ExtractedVector {
            chunk_id: 0,
            source_start: 0,
            source_len: u32::try_from(payload.len()).unwrap_or(u32::MAX),
            values,
        }],
        definition,
    )
}

pub(super) fn extract_test_only_embedding(
    payload: &[u8],
    definition: &VectorIndexDefinition,
) -> VectorExtraction {
    let mut values = Vec::with_capacity(usize::from(definition.dimension));
    let mut counter = 0u64;
    while values.len() < usize::from(definition.dimension) {
        let mut hasher = blake3::Hasher::new();
        hasher.update(definition.embedding_model.as_bytes());
        hasher.update(definition.modality.as_name().as_bytes());
        hasher.update(payload);
        hasher.update(&counter.to_le_bytes());
        let digest = hasher.finalize();
        for chunk in digest.as_bytes().chunks_exact(4) {
            if values.len() == usize::from(definition.dimension) {
                break;
            }
            let raw = u32::from_le_bytes(chunk.try_into().unwrap());
            let normalized = (raw as f32 / u32::MAX as f32) * 2.0 - 1.0;
            values.push(normalized);
        }
        counter = counter.saturating_add(1);
    }
    normalize_vector(&mut values, definition.metric);
    vector_extraction_from_vectors(
        vec![ExtractedVector {
            chunk_id: 0,
            source_start: 0,
            source_len: u32::try_from(payload.len()).unwrap_or(u32::MAX),
            values,
        }],
        definition,
    )
}

pub(super) fn vector_extraction_from_vectors(
    vectors: Vec<ExtractedVector>,
    definition: &VectorIndexDefinition,
) -> VectorExtraction {
    let mut accepted = Vec::new();
    let mut diagnostics = Vec::new();
    for (idx, vector) in vectors.into_iter().enumerate() {
        if vector.values.len() != usize::from(definition.dimension) {
            diagnostics.push(VectorExtractionDiagnostic {
                code: "VectorDimensionMismatch".to_string(),
                message: "extracted vector dimension does not match index definition".to_string(),
                details: serde_json::json!({
                    "vector_index": idx,
                    "expected_dimension": definition.dimension,
                    "actual_dimension": vector.values.len(),
                }),
            });
            continue;
        }
        accepted.push(vector);
    }
    VectorExtraction {
        vectors: accepted,
        diagnostics,
    }
}

pub(super) fn normalize_vector(values: &mut [f32], metric: VectorMetric) {
    if metric != VectorMetric::Cosine {
        return;
    }
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm <= f32::EPSILON {
        return;
    }
    for value in values {
        *value /= norm;
    }
}

pub(super) fn selector_matches(selector: &JsonValue, object: &Object) -> bool {
    if selector.is_null() {
        return true;
    }
    let Some(selector) = selector.as_object() else {
        return true;
    };
    if let Some(prefix) = selector.get("prefix").and_then(JsonValue::as_str) {
        if !object.key.starts_with(prefix) {
            return false;
        }
    }
    if let Some(content_type) = selector.get("content_type").and_then(JsonValue::as_str) {
        if object.content_type.as_deref() != Some(content_type) {
            return false;
        }
    }
    true
}

pub(super) fn metadata_backed_row_from_object(
    bucket: &Bucket,
    object: &Object,
) -> Result<TypedFieldSegmentRow> {
    let mut values = BTreeMap::new();
    if let Some(metadata) = object.user_meta.as_ref().and_then(JsonValue::as_object) {
        for (key, value) in metadata {
            values.insert(key.clone(), value.clone());
            insert_json_pointer_metadata_values(
                &mut values,
                format!("/{}", json_pointer_escape(key)),
                value,
            );
        }
    }
    values.insert(
        "object_key".to_string(),
        JsonValue::String(object.key.clone()),
    );
    Ok(TypedFieldSegmentRow {
        object_key: object.key.clone(),
        object_version_id: object.version_id.to_string(),
        source_identity: format!("{}#{}", object.key, object.version_id),
        encoded_values: encode_row_values(&values)?,
        source_id_binary: source_id_binary(&object_current_source_id(bucket, object))?,
        value_flags: 0,
        values,
        authz_label_hash: hex::encode(object_authz_label_hash(bucket, object)),
        authz_revision: u64::try_from(object.authz_revision).unwrap_or(0),
    })
}

fn insert_json_pointer_metadata_values(
    values: &mut BTreeMap<String, JsonValue>,
    pointer: String,
    value: &JsonValue,
) {
    values.insert(pointer.clone(), value.clone());
    if let Some(object) = value.as_object() {
        for (key, child) in object {
            insert_json_pointer_metadata_values(
                values,
                format!("{}/{}", pointer, json_pointer_escape(key)),
                child,
            );
        }
    }
}

fn json_pointer_escape(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

pub(super) async fn build_typed_json_object_rows(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    definition: &TypedJsonBuildDefinition,
    core_store: &CoreStore,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
) -> Result<(
    Vec<TypedFieldSegmentRow>,
    Vec<IndexBuildDiagnostic>,
    Vec<CoreBoundaryValue>,
)> {
    let objects = metadata_journal::read_current_objects_through_sequence(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let boundary_values = boundary_values_for_objects(storage, &objects).await?;
    let mut rows = Vec::new();
    let mut diagnostics = Vec::new();
    for object in objects {
        if object.deleted_at.is_some() || !selector_matches(&index.selector, &object) {
            continue;
        }
        let payload = match read_object_payload(core_store, &object).await {
            Ok(payload) => payload,
            Err(error) => {
                diagnostics.push(IndexBuildDiagnostic {
                    object_key: object.key.clone(),
                    version_id: Some(object.version_id),
                    severity: "error".to_string(),
                    code: "TypedJsonObjectPayloadUnavailable".to_string(),
                    message: error.to_string(),
                    details: serde_json::json!({ "source_kind": definition.source_kind }),
                });
                continue;
            }
        };
        let json = match serde_json::from_slice::<JsonValue>(&payload) {
            Ok(json) => json,
            Err(error) => {
                diagnostics.push(IndexBuildDiagnostic {
                    object_key: object.key.clone(),
                    version_id: Some(object.version_id),
                    severity: "error".to_string(),
                    code: "TypedJsonObjectInvalid".to_string(),
                    message: error.to_string(),
                    details: serde_json::json!({ "content_type": object.content_type }),
                });
                continue;
            }
        };
        match typed_json_row_from_object(bucket, definition, &object, &json) {
            Ok(row) => rows.push(row),
            Err(error) => diagnostics.push(IndexBuildDiagnostic {
                object_key: object.key.clone(),
                version_id: Some(object.version_id),
                severity: "error".to_string(),
                code: "TypedJsonRowExtractionFailed".to_string(),
                message: error.to_string(),
                details: serde_json::json!({ "fields": index.build_policy.get("fields") }),
            }),
        }
    }
    Ok((rows, diagnostics, boundary_values))
}

pub(super) async fn build_typed_json_append_rows(
    bucket: &Bucket,
    index: &IndexDefinition,
    definition: &TypedJsonBuildDefinition,
    core_store: &CoreStore,
    source_cursor: u128,
) -> Result<(
    Vec<TypedFieldSegmentRow>,
    Vec<IndexBuildDiagnostic>,
    Vec<CoreBoundaryValue>,
)> {
    let mut rows = Vec::new();
    let mut diagnostics = Vec::new();
    let mut boundary_values = BTreeSet::new();
    let mut after_stream_id = None;
    loop {
        let stream_page = crate::append_journal::list_append_streams_page(
            core_store.storage(),
            bucket.tenant_id,
            bucket.id,
            after_stream_id.as_deref(),
            128,
        )
        .await?;
        for stream in stream_page.streams {
            let mut after_sequence = 0;
            loop {
                let record_page = crate::append_journal::list_append_stream_records_page(
                    core_store.storage(),
                    &stream,
                    after_sequence,
                    512,
                )
                .await?;
                for record in record_page.records {
                    if (record.id.max(0) as u128) > source_cursor {
                        continue;
                    }
                    if !selector_matches_append(&index.selector, &stream, &record) {
                        continue;
                    }
                    if let Ok(manifest) = core_store
                        .read_object_manifest(&record.payload_object_ref)
                        .await
                    {
                        boundary_values.extend(manifest.boundary_values.into_iter());
                    }
                    let payload = match core_store
                        .get_blob(GetBlob {
                            object_ref: record.payload_object_ref.clone(),
                        })
                        .await
                    {
                        Ok(payload) => payload,
                        Err(error) => {
                            diagnostics.push(IndexBuildDiagnostic {
                                object_key: stream.stream_key.clone(),
                                version_id: None,
                                severity: "error".to_string(),
                                code: "TypedJsonAppendPayloadUnavailable".to_string(),
                                message: error.to_string(),
                                details: serde_json::json!({
                                    "record_sequence": record.record_sequence
                                }),
                            });
                            continue;
                        }
                    };
                    let json = match serde_json::from_slice::<JsonValue>(&payload) {
                        Ok(json) => json,
                        Err(error) => {
                            diagnostics.push(IndexBuildDiagnostic {
                                object_key: stream.stream_key.clone(),
                                version_id: None,
                                severity: "error".to_string(),
                                code: "TypedJsonAppendPayloadInvalid".to_string(),
                                message: error.to_string(),
                                details: serde_json::json!({
                                    "content_type": record.content_type
                                }),
                            });
                            continue;
                        }
                    };
                    match typed_json_row_from_append_record(
                        bucket, definition, &stream, &record, &json,
                    ) {
                        Ok(row) => rows.push(row),
                        Err(error) => diagnostics.push(IndexBuildDiagnostic {
                            object_key: stream.stream_key.clone(),
                            version_id: None,
                            severity: "error".to_string(),
                            code: "TypedJsonAppendRowExtractionFailed".to_string(),
                            message: error.to_string(),
                            details: serde_json::json!({
                                "fields": index.build_policy.get("fields")
                            }),
                        }),
                    }
                }
                if !record_page.has_more {
                    break;
                }
                after_sequence = record_page.next_sequence;
            }
        }
        let Some(next_stream_id) = stream_page.next_stream_id else {
            break;
        };
        after_stream_id = Some(next_stream_id);
    }
    Ok((rows, diagnostics, boundary_values.into_iter().collect()))
}

pub(super) fn selector_matches_append(
    selector: &JsonValue,
    stream: &AppendStream,
    record: &AppendStreamRecord,
) -> bool {
    if selector.is_null() {
        return true;
    }
    let Some(selector) = selector.as_object() else {
        return true;
    };
    if let Some(prefix) = selector.get("prefix").and_then(JsonValue::as_str)
        && !stream.stream_key.starts_with(prefix)
    {
        return false;
    }
    if let Some(content_type) = selector.get("content_type").and_then(JsonValue::as_str)
        && record.content_type.as_deref() != Some(content_type)
    {
        return false;
    }
    true
}

pub(super) async fn typed_json_source_manifest_hash(
    storage: &Storage,
    bucket: &Bucket,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    source_kind: &str,
) -> Result<String> {
    if source_kind == "append_record" {
        return Ok(blake3::hash(
            format!(
                "append_record:{}:{}:{}",
                bucket.tenant_id, bucket.id, source_cursor
            )
            .as_bytes(),
        )
        .to_hex()
        .to_string());
    }
    metadata_journal::object_metadata_source_checkpoint_hash(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await
}

pub(super) fn parse_typed_json_build_definition(
    index: &IndexDefinition,
) -> Result<TypedJsonBuildDefinition> {
    let source_kind = json_optional_string_field(&index.build_policy, "source_kind")
        .or_else(|| json_optional_string_field(&index.build_policy, "source"))
        .unwrap_or_else(|| "object_current".to_string());
    let fields_json = index
        .build_policy
        .get("fields")
        .or_else(|| index.extractor.get("fields"))
        .ok_or_else(|| anyhow!("typed_json index requires fields"))?;
    let JsonValue::Array(field_values) = fields_json else {
        return Err(anyhow!("typed_json fields must be an array"));
    };
    let mut fields = Vec::with_capacity(field_values.len());
    for value in field_values {
        let name = json_optional_string_field(value, "name")
            .ok_or_else(|| anyhow!("typed_json field requires name"))?;
        let extractor = json_optional_string_field(value, "extractor")
            .or_else(|| json_optional_string_field(value, "json_pointer"))
            .ok_or_else(|| anyhow!("typed_json field requires extractor"))?;
        validate_typed_json_extractor(&source_kind, &extractor)?;
        fields.push(TypedJsonBuildField {
            name,
            extractor,
            required: value
                .get("required")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false),
        });
    }
    Ok(TypedJsonBuildDefinition {
        source_kind,
        fields,
    })
}

pub(super) fn typed_json_row_from_object(
    bucket: &Bucket,
    definition: &TypedJsonBuildDefinition,
    object: &Object,
    json: &JsonValue,
) -> Result<TypedFieldSegmentRow> {
    let mut values = serde_json::Map::new();
    for field in &definition.fields {
        let value = match field.extractor.as_str() {
            "object_key" => JsonValue::String(object.key.clone()),
            "object_content_type" => object
                .content_type
                .clone()
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
            "created_at" => JsonValue::String(object.created_at.to_rfc3339()),
            extractor if extractor.starts_with("object_body_json_pointer:") => json
                .pointer(extractor.trim_start_matches("object_body_json_pointer:"))
                .cloned()
                .unwrap_or(JsonValue::Null),
            extractor if extractor.starts_with("object_user_metadata_json_pointer:") => object
                .user_meta
                .as_ref()
                .and_then(|metadata| {
                    metadata
                        .pointer(extractor.trim_start_matches("object_user_metadata_json_pointer:"))
                })
                .cloned()
                .unwrap_or(JsonValue::Null),
            pointer if pointer.starts_with('/') => {
                json.pointer(pointer).cloned().unwrap_or(JsonValue::Null)
            }
            _ => JsonValue::Null,
        };
        if value.is_null() && field.required {
            return Err(anyhow!("typed_json required field missing: {}", field.name));
        }
        values.insert(field.name.clone(), value);
    }
    let values = values.into_iter().collect();
    Ok(TypedFieldSegmentRow {
        object_key: object.key.clone(),
        object_version_id: object.version_id.to_string(),
        source_identity: format!("{}#{}", object.key, object.version_id),
        encoded_values: encode_row_values(&values)?,
        source_id_binary: source_id_binary(&object_current_source_id(bucket, object))?,
        value_flags: 0,
        values,
        authz_label_hash: hex::encode(object_authz_label_hash(bucket, object)),
        authz_revision: u64::try_from(object.authz_revision).unwrap_or(0),
    })
}

pub(super) fn typed_json_row_from_append_record(
    bucket: &Bucket,
    definition: &TypedJsonBuildDefinition,
    stream: &AppendStream,
    record: &AppendStreamRecord,
    json: &JsonValue,
) -> Result<TypedFieldSegmentRow> {
    let mut values = serde_json::Map::new();
    for field in &definition.fields {
        let value = match field.extractor.as_str() {
            "append_stream_key" => JsonValue::String(stream.stream_key.clone()),
            "append_record_sequence" => JsonValue::Number(record.record_sequence.into()),
            "append_content_type" => record
                .content_type
                .clone()
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
            "created_at" => JsonValue::String(record.created_at.to_rfc3339()),
            extractor if extractor.starts_with("append_payload_json_pointer:") => json
                .pointer(extractor.trim_start_matches("append_payload_json_pointer:"))
                .cloned()
                .unwrap_or(JsonValue::Null),
            extractor if extractor.starts_with("append_user_metadata_json_pointer:") => record
                .user_meta
                .as_ref()
                .and_then(|metadata| {
                    metadata
                        .pointer(extractor.trim_start_matches("append_user_metadata_json_pointer:"))
                })
                .cloned()
                .unwrap_or(JsonValue::Null),
            pointer if pointer.starts_with('/') => {
                json.pointer(pointer).cloned().unwrap_or(JsonValue::Null)
            }
            _ => JsonValue::Null,
        };
        if value.is_null() && field.required {
            return Err(anyhow!("typed_json required field missing: {}", field.name));
        }
        values.insert(field.name.clone(), value);
    }
    let values = values.into_iter().collect();
    let source_identity = format!("{}#{}", stream.stream_key, record.record_sequence);
    Ok(TypedFieldSegmentRow {
        object_key: stream.stream_key.clone(),
        object_version_id: record.record_sequence.to_string(),
        source_identity,
        encoded_values: encode_row_values(&values)?,
        source_id_binary: source_id_binary(&append_record_source_id(bucket, stream, record))?,
        value_flags: 0,
        values,
        authz_label_hash: hex::encode(hash32(
            format!(
                "tenant:{}:bucket:{}:append:{}:record:{}",
                bucket.tenant_id, bucket.id, stream.stream_key, record.record_sequence
            )
            .as_bytes(),
        )),
        authz_revision: 0,
    })
}

pub(super) fn object_current_source_id(bucket: &Bucket, object: &Object) -> SourceId {
    let storage_tenant = bucket.tenant_id.to_string();
    SourceId {
        schema: "anvil.query.source_id.v1".to_string(),
        mesh_id: "default".to_string(),
        anvil_storage_tenant_id: storage_tenant.clone(),
        authz_scope: AuthzScopeRef {
            anvil_storage_tenant_id: storage_tenant,
            authz_realm_id: format!("tenant:{}", bucket.tenant_id),
        },
        kind: SourceKind::ObjectCurrent,
        resource_namespace: "anvil_object".to_string(),
        resource_id: format!("{}/{}/{}", bucket.tenant_id, bucket.name, object.key),
        generation: object.id.max(0) as u64,
        tombstone: object.deleted_at.is_some(),
        variant: BTreeMap::from([
            ("bucket_id".to_string(), bucket.id.to_string()),
            ("version_id".to_string(), object.version_id.to_string()),
        ]),
    }
}

pub(super) fn append_record_source_id(
    bucket: &Bucket,
    stream: &AppendStream,
    record: &AppendStreamRecord,
) -> SourceId {
    let storage_tenant = bucket.tenant_id.to_string();
    SourceId {
        schema: "anvil.query.source_id.v1".to_string(),
        mesh_id: "default".to_string(),
        anvil_storage_tenant_id: storage_tenant.clone(),
        authz_scope: AuthzScopeRef {
            anvil_storage_tenant_id: storage_tenant,
            authz_realm_id: format!("tenant:{}", bucket.tenant_id),
        },
        kind: SourceKind::AppendRecord,
        resource_namespace: "anvil_append_record".to_string(),
        resource_id: format!(
            "{}/{}/{}/{}",
            bucket.tenant_id, bucket.name, stream.stream_key, record.record_sequence
        ),
        generation: record.id.max(0) as u64,
        tombstone: false,
        variant: BTreeMap::from([
            ("bucket_id".to_string(), bucket.id.to_string()),
            ("stream_id".to_string(), stream.stream_id.to_string()),
            (
                "record_sequence".to_string(),
                record.record_sequence.to_string(),
            ),
        ]),
    }
}

pub(super) fn validate_typed_json_extractor(source_kind: &str, extractor: &str) -> Result<()> {
    let pointer_valid = |value: &str| value.starts_with('/');
    match (source_kind, extractor) {
        (_, "created_at") => Ok(()),
        ("object_current" | "object_version", "object_key" | "object_content_type") => Ok(()),
        ("object_current" | "object_version", value) if pointer_valid(value) => Ok(()),
        ("object_current" | "object_version", value)
            if value
                .strip_prefix("object_body_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        ("object_current" | "object_version", value)
            if value
                .strip_prefix("object_user_metadata_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        (
            "append_record",
            "append_stream_key" | "append_record_sequence" | "append_content_type",
        ) => Ok(()),
        ("append_record", value) if pointer_valid(value) => Ok(()),
        ("append_record", value)
            if value
                .strip_prefix("append_payload_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        ("append_record", value)
            if value
                .strip_prefix("append_user_metadata_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        _ => Err(anyhow!("invalid typed_json field extractor")),
    }
}

pub(super) fn json_optional_string_field(value: &JsonValue, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
}

pub(super) fn extract_text_fields(
    extractor: &JsonValue,
    object: &Object,
    payload: &[u8],
) -> TextExtraction {
    let mut fields = Vec::new();
    let mut diagnostics = Vec::new();
    if let Some(field_specs) = extractor.get("fields").and_then(JsonValue::as_array) {
        for (idx, field) in field_specs.iter().enumerate() {
            let source = field
                .get("source")
                .and_then(JsonValue::as_str)
                .unwrap_or("object_body_utf8");
            match extract_text_source(source, field, object, payload) {
                Ok(Some(text)) => fields.push(ExtractedTextField { text }),
                Ok(None) => {}
                Err(diagnostic) => diagnostics.push(TextExtractionDiagnostic {
                    details: merge_details(
                        diagnostic.details,
                        serde_json::json!({ "field_index": idx }),
                    ),
                    ..diagnostic
                }),
            }
        }
        return TextExtraction {
            fields,
            diagnostics,
        };
    }
    if let Some(source) = extractor.get("source").and_then(JsonValue::as_str) {
        match extract_text_source(source, extractor, object, payload) {
            Ok(Some(text)) => fields.push(ExtractedTextField { text }),
            Ok(None) => {}
            Err(diagnostic) => diagnostics.push(diagnostic),
        }
        return TextExtraction {
            fields,
            diagnostics,
        };
    }
    if extractor.get("encoding").and_then(JsonValue::as_str) == Some("utf8") {
        match decode_utf8_text(payload) {
            Ok(text) => fields.push(ExtractedTextField { text }),
            Err(diagnostic) => diagnostics.push(diagnostic),
        }
        return TextExtraction {
            fields,
            diagnostics,
        };
    }
    match decode_utf8_text(payload) {
        Ok(text) => fields.push(ExtractedTextField { text }),
        Err(diagnostic) => diagnostics.push(diagnostic),
    }
    TextExtraction {
        fields,
        diagnostics,
    }
}

pub(super) fn extract_text_source(
    source: &str,
    extractor: &JsonValue,
    object: &Object,
    payload: &[u8],
) -> Result<Option<String>, TextExtractionDiagnostic> {
    match source {
        "object_body_utf8" | "utf8" | "body" | "git_blob_text" => {
            decode_utf8_text(payload).map(Some)
        }
        "object_key" | "key" => Ok(Some(object.key.clone())),
        "content_type" => Ok(object.content_type.clone()),
        "json_pointer" => {
            let payload_text = decode_utf8_text(payload)?;
            extract_json_pointer_text(extractor, &payload_text)
        }
        "metadata_field" => extract_metadata_field_text(extractor, object),
        "media_transcript" => extract_media_transcript_text(object, payload),
        "personaldb_table_column" => extract_personaldb_table_column_text(extractor, payload),
        other => Err(TextExtractionDiagnostic {
            code: "UnsupportedTextExtractor".to_string(),
            message: format!("unsupported text extractor source `{other}`"),
            details: serde_json::json!({ "source": other }),
        }),
    }
}

pub(super) fn decode_utf8_text(payload: &[u8]) -> Result<String, TextExtractionDiagnostic> {
    String::from_utf8(payload.to_vec()).map_err(|error| TextExtractionDiagnostic {
        code: "TextPayloadNotUtf8".to_string(),
        message: "object body is not valid UTF-8 for text extraction".to_string(),
        details: serde_json::json!({ "error": error.to_string() }),
    })
}

pub(super) fn extract_media_transcript_text(
    object: &Object,
    payload: &[u8],
) -> Result<Option<String>, TextExtractionDiagnostic> {
    let content_type = object
        .content_type
        .as_deref()
        .ok_or_else(|| TextExtractionDiagnostic {
            code: "MediaContentTypeMissing".to_string(),
            message: "media_transcript text extractor requires an object content type".to_string(),
            details: serde_json::json!({ "object_key": object.key.clone() }),
        })?;
    let extraction = execute_media_extraction(
        MediaExtractionRequest {
            object: MediaObjectRef {
                tenant_id: object.tenant_id,
                bucket_id: object.bucket_id,
                object_key: object.key.clone(),
                version_id: object.version_id.to_string(),
                content_hash: object.content_hash.clone(),
                size_bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            },
            content_type: content_type.to_string(),
            asset_policy: DerivedAssetPolicy::InternalOnly,
        },
        payload,
    )
    .map_err(|error| TextExtractionDiagnostic {
        code: "MediaTranscriptExtractionFailed".to_string(),
        message: error.to_string(),
        details: serde_json::json!({ "content_type": content_type }),
    })?;
    extraction
        .outputs
        .into_iter()
        .find(|output| output.kind == DerivedOutputKind::TextTranscript)
        .map(|output| {
            String::from_utf8(output.bytes).map_err(|error| TextExtractionDiagnostic {
                code: "MediaTranscriptNotUtf8".to_string(),
                message: "media transcript output is not valid UTF-8".to_string(),
                details: serde_json::json!({ "error": error.to_string() }),
            })
        })
        .transpose()
}

pub(super) fn extract_json_pointer_text(
    extractor: &JsonValue,
    payload_text: &str,
) -> Result<Option<String>, TextExtractionDiagnostic> {
    let pointer = extractor
        .get("json_pointer")
        .or_else(|| extractor.get("pointer"))
        .or_else(|| extractor.get("path"))
        .and_then(JsonValue::as_str)
        .ok_or_else(|| TextExtractionDiagnostic {
            code: "JsonPointerMissing".to_string(),
            message: "json_pointer text extractor requires a JSON pointer".to_string(),
            details: serde_json::json!({ "extractor": extractor }),
        })?;
    let body = serde_json::from_str::<JsonValue>(payload_text).map_err(|error| {
        TextExtractionDiagnostic {
            code: "JsonPointerDecodeFailed".to_string(),
            message: "object body is not valid JSON for json_pointer text extraction".to_string(),
            details: serde_json::json!({ "pointer": pointer, "error": error.to_string() }),
        }
    })?;
    let Some(value) = body.pointer(pointer) else {
        return Err(TextExtractionDiagnostic {
            code: "JsonPointerNotFound".to_string(),
            message: "JSON pointer did not match a value in the object body".to_string(),
            details: serde_json::json!({ "pointer": pointer }),
        });
    };
    Ok(json_value_to_text(value))
}

pub(super) fn extract_personaldb_table_column_text(
    extractor: &JsonValue,
    payload: &[u8],
) -> Result<Option<String>, TextExtractionDiagnostic> {
    let column = extractor
        .get("column")
        .or_else(|| extractor.get("column_name"))
        .or_else(|| extractor.get("field"))
        .and_then(JsonValue::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| TextExtractionDiagnostic {
            code: "PersonalDbTableColumnMissing".to_string(),
            message: "personaldb_table_column text extractor requires a column name".to_string(),
            details: serde_json::json!({ "extractor": extractor }),
        })?;
    let payload_text = decode_utf8_text(payload)?;
    let row = serde_json::from_str::<JsonValue>(&payload_text).map_err(|error| {
        TextExtractionDiagnostic {
            code: "PersonalDbTableColumnDecodeFailed".to_string(),
            message: "object body is not valid JSON for personaldb_table_column extraction"
                .to_string(),
            details: serde_json::json!({ "column": column, "error": error.to_string() }),
        }
    })?;
    if let Some(expected_table) = extractor
        .get("table")
        .or_else(|| extractor.get("table_name"))
        .and_then(JsonValue::as_str)
        .filter(|value| !value.trim().is_empty())
        && !personaldb_table_matches(&row, expected_table)
    {
        return Ok(None);
    }
    let Some(value) = personaldb_column_value(&row, column) else {
        return Err(TextExtractionDiagnostic {
            code: "PersonalDbTableColumnNotFound".to_string(),
            message: "personaldb_table_column did not match a value in the row payload".to_string(),
            details: serde_json::json!({ "column": column }),
        });
    };
    Ok(json_value_to_text(value))
}

pub(super) fn personaldb_table_matches(row: &JsonValue, expected_table: &str) -> bool {
    row.get("table_name")
        .or_else(|| row.get("table"))
        .and_then(JsonValue::as_str)
        == Some(expected_table)
}

pub(super) fn personaldb_column_value<'a>(
    row: &'a JsonValue,
    column: &str,
) -> Option<&'a JsonValue> {
    if column.starts_with('/') {
        return row.pointer(column);
    }
    row.get("columns")
        .and_then(|columns| columns.get(column))
        .or_else(|| row.get("row").and_then(|row| row.get(column)))
        .or_else(|| row.get("new_values").and_then(|values| values.get(column)))
        .or_else(|| row.get("values").and_then(|values| values.get(column)))
        .or_else(|| row.get(column))
}

pub(super) fn extract_metadata_field_text(
    extractor: &JsonValue,
    object: &Object,
) -> Result<Option<String>, TextExtractionDiagnostic> {
    let field = extractor
        .get("field")
        .or_else(|| extractor.get("metadata_field"))
        .or_else(|| extractor.get("key"))
        .or_else(|| extractor.get("path"))
        .and_then(JsonValue::as_str)
        .ok_or_else(|| TextExtractionDiagnostic {
            code: "MetadataFieldMissing".to_string(),
            message: "metadata_field text extractor requires a field name".to_string(),
            details: serde_json::json!({ "extractor": extractor }),
        })?;
    let Some(metadata) = object.user_meta.as_ref() else {
        return Err(TextExtractionDiagnostic {
            code: "MetadataFieldNotFound".to_string(),
            message: "object has no user metadata for metadata_field text extraction".to_string(),
            details: serde_json::json!({ "field": field }),
        });
    };
    let value = if field.starts_with('/') {
        metadata.pointer(field)
    } else {
        metadata.get(field)
    };
    let Some(value) = value else {
        return Err(TextExtractionDiagnostic {
            code: "MetadataFieldNotFound".to_string(),
            message: "metadata field did not match a value in object user metadata".to_string(),
            details: serde_json::json!({ "field": field }),
        });
    };
    Ok(json_value_to_text(value))
}

pub(super) fn json_value_to_text(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::Null => None,
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        JsonValue::Bool(value) => Some(value.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Some(value.to_string()),
    }
}

pub(super) fn merge_details(left: JsonValue, right: JsonValue) -> JsonValue {
    let mut merged = serde_json::Map::new();
    if let JsonValue::Object(values) = left {
        merged.extend(values);
    } else if !left.is_null() {
        merged.insert("details".to_string(), left);
    }
    if let JsonValue::Object(values) = right {
        merged.extend(values);
    }
    JsonValue::Object(merged)
}

pub(super) async fn read_object_payload(
    core_store: &CoreStore,
    object: &Object,
) -> Result<Vec<u8>> {
    let target = core_object_data_target_from_shard_map(object).with_context(|| {
        format!(
            "object {} version {} is not CoreStore-backed",
            object.key, object.version_id
        )
    })?;
    match target {
        CoreObjectDataTarget::LogicalFile(locator) => {
            let manifest = core_store
                .read_logical_file_manifest(&locator)
                .await
                .with_context(|| format!("read CoreStore logical manifest for {}", object.key))?;
            core_store
                .read_logical_range(ReadLogicalRangeRequest {
                    ranges: vec![CoreByteRange {
                        start: 0,
                        end_exclusive: manifest.logical_size,
                    }],
                    manifest,
                    authz_scope: AuthzScopeRef {
                        anvil_storage_tenant_id: object.tenant_id.to_string(),
                        authz_realm_id: format!("object:{}", object.bucket_id),
                    },
                    expected_boundary: None,
                    prefetch_policy: CorePrefetchPolicy::default(),
                    trace_context: Default::default(),
                })
                .await
                .with_context(|| format!("read CoreStore logical payload for {}", object.key))
        }
        CoreObjectDataTarget::ObjectRef(object_ref) => core_store
            .get_blob(GetBlob { object_ref })
            .await
            .with_context(|| format!("read CoreStore object-ref payload for {}", object.key)),
    }
}

pub(super) enum CoreObjectDataTarget {
    LogicalFile(CoreManifestLocator),
    ObjectRef(CoreObjectRef),
}

pub(super) fn core_object_data_target_from_shard_map(
    object: &Object,
) -> Result<CoreObjectDataTarget> {
    let value = object
        .shard_map
        .as_ref()
        .ok_or_else(|| anyhow!("object shard map is missing"))?;
    if value.get("schema").and_then(JsonValue::as_str) != Some("anvil.core.object_data_target.v1") {
        anyhow::bail!("object shard map is not a canonical CoreStore object data target");
    }
    let kind = value
        .get("kind")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("object data target kind is missing"))?;
    let target = value
        .get("target")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("object data target bytes are missing"))?;
    match kind {
        "logical_file" => {
            let bytes = URL_SAFE_NO_PAD.decode(target)?;
            Ok(CoreObjectDataTarget::LogicalFile(
                decode_manifest_locator_proto(&bytes)?,
            ))
        }
        "object_ref" => Ok(CoreObjectDataTarget::ObjectRef(
            decode_core_object_ref_target(target)?,
        )),
        other => anyhow::bail!("unsupported CoreStore object data target kind {other}"),
    }
}

pub(super) fn object_authz_label_hash(bucket: &Bucket, object: &Object) -> [u8; 32] {
    hash32(
        format!(
            "tenant:{}:bucket:{}:object:{}:authz:{}",
            bucket.tenant_id, bucket.id, object.key, object.authz_revision
        )
        .as_bytes(),
    )
}

pub(super) fn latest_authz_revision_for_documents(documents: &[OwnedFullTextDocument]) -> u64 {
    documents
        .iter()
        .filter_map(|document| u64::try_from(document.authz_revision).ok())
        .max()
        .unwrap_or(0)
}

pub(super) fn latest_authz_revision_for_vectors(documents: &[OwnedVectorDocument]) -> u64 {
    documents
        .iter()
        .filter_map(|document| u64::try_from(document.authz_revision).ok())
        .max()
        .unwrap_or(0)
}

pub(super) fn latest_authz_revision_for_typed_rows(rows: &[TypedFieldSegmentRow]) -> u64 {
    rows.iter().map(|row| row.authz_revision).max().unwrap_or(0)
}
