use super::*;

pub(super) fn canonical_json_bytes(value: &JsonValue) -> AnyhowResult<Vec<u8>> {
    serde_json::to_vec(&canonical_json(value)).map_err(Into::into)
}

pub(super) fn canonical_json(value: &JsonValue) -> JsonValue {
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

pub(crate) fn extract_object_boundary_values(
    schema: &CoreBoundarySchema,
    tenant_id: i64,
    bucket_name: &str,
    object_key: &str,
    content_type: Option<&str>,
    user_metadata: Option<&JsonValue>,
    payload_len: u64,
    payload: &[u8],
) -> AnyhowResult<Vec<CoreBoundaryValue>> {
    let mut values = Vec::new();
    for dimension in &schema.dimensions {
        let (source_kind, raw_value) = match &dimension.source {
            CoreBoundarySource::UserMetadataJsonPointer { pointer } => (
                "user_metadata_json_pointer",
                user_metadata
                    .and_then(|metadata| metadata.pointer(pointer))
                    .cloned(),
            ),
            CoreBoundarySource::SystemMetadataField { field } => (
                "system_metadata_field",
                object_boundary_system_metadata(
                    tenant_id,
                    bucket_name,
                    object_key,
                    content_type,
                    payload_len,
                    field,
                ),
            ),
            CoreBoundarySource::PathTemplate { template } => (
                "path_template",
                extract_path_template_capture(template, object_key, &dimension.name),
            ),
            CoreBoundarySource::BodyJsonPointer {
                pointer,
                max_body_bytes,
            } => {
                if !content_type.is_some_and(is_json_content_type) {
                    bail!(
                        "{}: boundary dimension {} requires JSON content type",
                        AnvilErrorCode::BoundaryExtractorUnsupportedContentType.as_str(),
                        dimension.name
                    );
                }
                if payload.len() as u64 > *max_body_bytes {
                    bail!(
                        "{}: boundary dimension {} body exceeds {} bytes",
                        AnvilErrorCode::BoundaryExtractorBodyTooLarge.as_str(),
                        dimension.name,
                        max_body_bytes
                    );
                }
                let body: JsonValue = serde_json::from_slice(payload).map_err(|error| {
                    anyhow!(
                        "{}: boundary dimension {} body is not valid JSON: {error}",
                        AnvilErrorCode::BoundaryTypeMismatch.as_str(),
                        dimension.name
                    )
                })?;
                ("body_json_pointer", body.pointer(pointer).cloned())
            }
            CoreBoundarySource::WriterSuppliedBoundary {
                writer_family,
                field,
            } => {
                if writer_family != WriterFamily::ObjectBlob.as_str() {
                    bail!(
                        "{}: boundary dimension {} requires writer family {}, not {}",
                        AnvilErrorCode::BoundaryTypeMismatch.as_str(),
                        dimension.name,
                        writer_family,
                        WriterFamily::ObjectBlob.as_str()
                    );
                }
                (
                    "writer_supplied_boundary",
                    user_metadata.and_then(|metadata| {
                        metadata
                            .get("_anvil_writer_boundaries")
                            .and_then(|boundaries| boundaries.get(field))
                            .cloned()
                    }),
                )
            }
        };

        let Some(raw_value) = raw_value else {
            if dimension.required {
                bail!(
                    "{}: required boundary dimension {} is missing",
                    AnvilErrorCode::BoundaryRequiredMissing.as_str(),
                    dimension.name
                );
            }
            continue;
        };
        let value = normalise_boundary_value(&dimension.value_type, &raw_value)
            .map_err(|error| anyhow!("{} for dimension {}", error, dimension.name))?;
        values.push(CoreBoundaryValue {
            schema_generation: schema.generation,
            name: dimension.name.clone(),
            value_type: dimension.value_type.clone(),
            value,
            categories: dimension.categories.clone(),
            source_kind: source_kind.to_string(),
            required: dimension.required,
            max_values_per_block: dimension.max_values_per_block,
            placement_affinity: dimension.placement_affinity.clone(),
            compaction_scope: dimension.compaction_scope.clone(),
            shared_ranges_allowed: dimension.shared_ranges_allowed,
            shared_record_kinds: dimension.shared_record_kinds.clone(),
        });
    }
    Ok(values)
}

fn object_boundary_system_metadata(
    tenant_id: i64,
    bucket_name: &str,
    object_key: &str,
    content_type: Option<&str>,
    payload_len: u64,
    field: &str,
) -> Option<JsonValue> {
    match field {
        "tenant_id" => Some(JsonValue::Number(tenant_id.into())),
        "bucket_name" => Some(JsonValue::String(bucket_name.to_string())),
        "object_key" => Some(JsonValue::String(object_key.to_string())),
        "content_type" => content_type.map(|value| JsonValue::String(value.to_string())),
        "payload_length" => Some(JsonValue::Number(payload_len.into())),
        _ => None,
    }
}

fn is_json_content_type(content_type: &str) -> bool {
    let content_type = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();
    content_type == "application/json" || content_type.ends_with("+json")
}

fn extract_path_template_capture(
    template: &str,
    object_key: &str,
    capture_name: &str,
) -> Option<JsonValue> {
    let template_segments = template
        .trim_start_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    let object_segments = object_key
        .trim_start_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    let mut captures = serde_json::Map::new();
    let mut object_index = 0usize;
    for segment in template_segments {
        if segment == "**" {
            break;
        }
        let object_segment = object_segments.get(object_index)?;
        object_index += 1;
        if let Some(capture) = segment
            .strip_prefix('{')
            .and_then(|value| value.strip_suffix('}'))
        {
            let name = capture.split(':').next().unwrap_or(capture);
            captures.insert(
                name.to_string(),
                JsonValue::String((*object_segment).to_string()),
            );
        } else if segment != *object_segment {
            return None;
        }
    }
    captures.remove(capture_name)
}

fn normalise_boundary_value(value_type: &str, value: &JsonValue) -> AnyhowResult<String> {
    match value_type {
        "string" => value.as_str().map(str::to_string).ok_or_else(|| {
            anyhow!(
                "{}: expected string boundary value",
                AnvilErrorCode::BoundaryTypeMismatch.as_str()
            )
        }),
        "uuid" => {
            let value = value.as_str().ok_or_else(|| {
                anyhow!(
                    "{}: expected uuid string boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            let uuid = uuid::Uuid::parse_str(value).map_err(|_| {
                anyhow!(
                    "{}: expected canonical uuid boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            Ok(uuid.to_string())
        }
        "u64" => value
            .as_u64()
            .map(|value| value.to_string())
            .or_else(|| {
                value
                    .as_str()?
                    .parse::<u64>()
                    .ok()
                    .map(|value| value.to_string())
            })
            .ok_or_else(|| {
                anyhow!(
                    "{}: expected u64 boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            }),
        "i64" => value
            .as_i64()
            .map(|value| value.to_string())
            .or_else(|| {
                value
                    .as_str()?
                    .parse::<i64>()
                    .ok()
                    .map(|value| value.to_string())
            })
            .ok_or_else(|| {
                anyhow!(
                    "{}: expected i64 boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            }),
        "date" => {
            let value = value.as_str().ok_or_else(|| {
                anyhow!(
                    "{}: expected date string boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
                anyhow!(
                    "{}: expected YYYY-MM-DD boundary date",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            Ok(date.to_string())
        }
        "timestamp" => {
            let value = value.as_str().ok_or_else(|| {
                anyhow!(
                    "{}: expected timestamp string boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            let timestamp = chrono::DateTime::parse_from_rfc3339(value).map_err(|_| {
                anyhow!(
                    "{}: expected RFC3339 boundary timestamp",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            Ok(timestamp.to_rfc3339())
        }
        _ => bail!("unsupported boundary value type {value_type}"),
    }
}
