//! Conversion between public index definitions and isolated engine formats.
//!
//! Builders pass authoritative current object snapshots here. This module is
//! deliberately synchronous and storage-neutral so CPU-heavy generation work
//! can run in one bounded blocking task without teaching engines about Anvil's
//! object, cluster, or authorization layers.

use std::collections::BTreeMap;

use anvil_api::v1::index_specification::Specification;
use anvil_api::v1::{IndexSpecification, VectorMetric as ApiVectorMetric};
use anvil_index::full_text::{FullTextDocument, FullTextEngine};
use anvil_index::hybrid::{HybridDefinition, HybridDocument, HybridEngine};
use anvil_index::ordered::{PathDocument, PathEngine};
use anvil_index::projections::{
    GitSourceEngine, GitSourceRecord, HuggingFaceManifestEngine, HuggingFaceManifestRecord,
    TensorProjectionEngine, TensorRecord,
};
use anvil_index::typed_json::{
    MetadataDocument, MetadataFilterEngine, TypedField, TypedJsonDefinition, TypedJsonDocument,
    TypedJsonEngine,
};
use anvil_index::vector::{VectorDefinition, VectorDocument, VectorEngine, VectorMetric};
use anvil_index::{DocumentRef, IndexArtifacts, IndexError};
use serde_json::{Map, Value, json};

#[derive(Clone, Debug)]
pub(crate) struct IndexBuildObject {
    pub path: String,
    pub version: u64,
    pub content_type: Option<String>,
    pub content_hash: [u8; 32],
    pub content_length: u64,
    pub committed_at_unix_millis: u64,
    /// Payload bytes are omitted for path and fixed object-metadata indexes.
    pub payload: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct IndexBuildDiagnostics {
    pub accepted_objects: u64,
    pub skipped_objects: u64,
}

#[derive(Debug)]
pub(crate) struct BuiltIndexGeneration {
    pub artifacts: IndexArtifacts,
    pub diagnostics: IndexBuildDiagnostics,
}

pub(crate) fn build_generation(
    specification: &IndexSpecification,
    objects: Vec<IndexBuildObject>,
) -> Result<BuiltIndexGeneration, IndexError> {
    match specification.specification.as_ref() {
        Some(Specification::Path(_)) => build_path(objects),
        Some(Specification::MetadataFilter(specification)) => {
            build_metadata(&specification.fields, objects)
        }
        Some(Specification::TypedJson(specification)) => {
            let definition = typed_definition(&specification.fields);
            build_typed(&definition, objects)
        }
        Some(Specification::FullText(specification)) => build_full_text(specification, objects),
        Some(Specification::Vector(specification)) => build_vector(specification, objects),
        Some(Specification::Hybrid(specification)) => build_hybrid(specification, objects),
        Some(Specification::GitSource(specification)) => {
            build_git(&specification.repository_id, objects)
        }
        Some(Specification::Tensor(specification)) => {
            build_tensor(&specification.model_id, objects)
        }
        None => Err(IndexError::InvalidDefinition(
            "index specification is required".into(),
        )),
    }
}

pub(crate) fn build_tensor_projection(
    tensors: Vec<TensorRecord>,
) -> Result<BuiltIndexGeneration, IndexError> {
    let accepted_objects = tensors.len() as u64;
    Ok(BuiltIndexGeneration {
        artifacts: TensorProjectionEngine::build(tensors)?,
        diagnostics: IndexBuildDiagnostics {
            accepted_objects,
            skipped_objects: 0,
        },
    })
}

pub(crate) fn build_hugging_face_projection(
    records: Vec<HuggingFaceManifestRecord>,
) -> Result<BuiltIndexGeneration, IndexError> {
    let accepted_objects = records.len() as u64;
    Ok(BuiltIndexGeneration {
        artifacts: HuggingFaceManifestEngine::build(records)?,
        diagnostics: IndexBuildDiagnostics {
            accepted_objects,
            skipped_objects: 0,
        },
    })
}

fn build_path(objects: Vec<IndexBuildObject>) -> Result<BuiltIndexGeneration, IndexError> {
    let accepted_objects = objects.len() as u64;
    let documents = objects.into_iter().map(|object| PathDocument {
        path: object.path,
        version: object.version,
    });
    Ok(BuiltIndexGeneration {
        artifacts: PathEngine::build(documents)?,
        diagnostics: IndexBuildDiagnostics {
            accepted_objects,
            skipped_objects: 0,
        },
    })
}

fn build_metadata(
    fields: &[String],
    objects: Vec<IndexBuildObject>,
) -> Result<BuiltIndexGeneration, IndexError> {
    let accepted_objects = objects.len() as u64;
    let documents = objects.into_iter().map(|object| {
        let mut metadata = Map::new();
        for field in fields {
            let value = match field.as_str() {
                "path" => Value::String(object.path.clone()),
                "version" => json!(object.version),
                "content_type" => object
                    .content_type
                    .clone()
                    .map_or(Value::Null, Value::String),
                "content_length" => json!(object.content_length),
                "content_hash" => Value::String(hex::encode(object.content_hash)),
                "committed_at_unix_millis" => json!(object.committed_at_unix_millis),
                _ => Value::Null,
            };
            metadata.insert(field.clone(), value);
        }
        MetadataDocument {
            document: document_ref(&object),
            metadata,
        }
    });
    let (_, artifacts) = MetadataFilterEngine::build_fields(fields.iter().cloned(), documents)?;
    Ok(BuiltIndexGeneration {
        artifacts,
        diagnostics: IndexBuildDiagnostics {
            accepted_objects,
            skipped_objects: 0,
        },
    })
}

fn build_typed(
    definition: &TypedJsonDefinition,
    objects: Vec<IndexBuildObject>,
) -> Result<BuiltIndexGeneration, IndexError> {
    let mut diagnostics = IndexBuildDiagnostics::default();
    let mut documents = Vec::new();
    for object in objects {
        let Some(value) = parse_payload_json(&object) else {
            diagnostics.skipped_objects += 1;
            continue;
        };
        diagnostics.accepted_objects += 1;
        documents.push(TypedJsonDocument {
            document: document_ref(&object),
            value,
        });
    }
    Ok(BuiltIndexGeneration {
        artifacts: TypedJsonEngine::build(definition, documents)?,
        diagnostics,
    })
}

fn build_full_text(
    specification: &anvil_api::v1::FullTextIndexSpec,
    objects: Vec<IndexBuildObject>,
) -> Result<BuiltIndexGeneration, IndexError> {
    let mut diagnostics = IndexBuildDiagnostics::default();
    let mut documents = Vec::new();
    for object in objects {
        let Some(value) = parse_payload_json(&object) else {
            diagnostics.skipped_objects += 1;
            continue;
        };
        let mut fields = BTreeMap::new();
        for field in &specification.fields {
            if let Some(text) = value.pointer(&field.json_pointer).and_then(Value::as_str) {
                fields.insert(field.name.clone(), text.to_owned());
            }
        }
        if fields.is_empty() {
            diagnostics.skipped_objects += 1;
            continue;
        }
        diagnostics.accepted_objects += 1;
        documents.push(FullTextDocument {
            document: document_ref(&object),
            fields,
        });
    }
    Ok(BuiltIndexGeneration {
        artifacts: FullTextEngine::build(documents)?,
        diagnostics,
    })
}

fn build_vector(
    specification: &anvil_api::v1::VectorIndexSpec,
    objects: Vec<IndexBuildObject>,
) -> Result<BuiltIndexGeneration, IndexError> {
    let definition = vector_definition(specification)?;
    let mut diagnostics = IndexBuildDiagnostics::default();
    let mut documents = Vec::new();
    for object in objects {
        let values = parse_payload_json(&object)
            .as_ref()
            .and_then(|value| value.pointer(&specification.json_pointer))
            .and_then(|value| vector_values(value, definition.dimension, specification.normalize));
        let Some(values) = values else {
            diagnostics.skipped_objects += 1;
            continue;
        };
        diagnostics.accepted_objects += 1;
        documents.push(VectorDocument {
            document: document_ref(&object),
            values,
        });
    }
    Ok(BuiltIndexGeneration {
        artifacts: VectorEngine::build(&definition, documents)?,
        diagnostics,
    })
}

fn build_hybrid(
    specification: &anvil_api::v1::HybridIndexSpec,
    objects: Vec<IndexBuildObject>,
) -> Result<BuiltIndexGeneration, IndexError> {
    let text = specification
        .full_text
        .as_ref()
        .ok_or_else(|| IndexError::InvalidDefinition("hybrid full-text spec is required".into()))?;
    let vector = specification
        .vector
        .as_ref()
        .ok_or_else(|| IndexError::InvalidDefinition("hybrid vector spec is required".into()))?;
    let definition = HybridDefinition {
        vector: vector_definition(vector)?,
        text_weight: effective_weight(specification.full_text_weight),
        vector_weight: effective_weight(specification.vector_weight),
    };
    let mut diagnostics = IndexBuildDiagnostics::default();
    let mut documents = Vec::new();
    for object in objects {
        let Some(value) = parse_payload_json(&object) else {
            diagnostics.skipped_objects += 1;
            continue;
        };
        let mut text_fields = BTreeMap::new();
        for field in &text.fields {
            if let Some(value) = value.pointer(&field.json_pointer).and_then(Value::as_str) {
                text_fields.insert(field.name.clone(), value.to_owned());
            }
        }
        let values = value
            .pointer(&vector.json_pointer)
            .and_then(|value| vector_values(value, definition.vector.dimension, vector.normalize));
        let Some(values) = values else {
            diagnostics.skipped_objects += 1;
            continue;
        };
        if text_fields.is_empty() {
            diagnostics.skipped_objects += 1;
            continue;
        }
        diagnostics.accepted_objects += 1;
        documents.push(HybridDocument {
            document: document_ref(&object),
            text_fields,
            vector: values,
        });
    }
    Ok(BuiltIndexGeneration {
        artifacts: HybridEngine::build(&definition, documents)?,
        diagnostics,
    })
}

fn build_git(
    repository_id: &str,
    objects: Vec<IndexBuildObject>,
) -> Result<BuiltIndexGeneration, IndexError> {
    let mut diagnostics = IndexBuildDiagnostics::default();
    let mut records = Vec::new();
    for object in objects {
        let Some(payload) = object.payload.as_deref() else {
            diagnostics.skipped_objects += 1;
            continue;
        };
        let parsed = serde_json::from_slice::<GitSourceRecord>(payload)
            .map(|record| vec![record])
            .or_else(|_| serde_json::from_slice::<Vec<GitSourceRecord>>(payload));
        let Ok(mut parsed) = parsed else {
            diagnostics.skipped_objects += 1;
            continue;
        };
        parsed.retain(|record| record.repository_id == repository_id);
        if parsed.is_empty() {
            diagnostics.skipped_objects += 1;
            continue;
        }
        diagnostics.accepted_objects += 1;
        records.extend(parsed);
    }
    Ok(BuiltIndexGeneration {
        artifacts: GitSourceEngine::build(records)?,
        diagnostics,
    })
}

fn build_tensor(
    model_id: &str,
    objects: Vec<IndexBuildObject>,
) -> Result<BuiltIndexGeneration, IndexError> {
    let mut diagnostics = IndexBuildDiagnostics::default();
    let mut records = Vec::new();
    for object in objects {
        let Some(payload) = object.payload.as_deref() else {
            diagnostics.skipped_objects += 1;
            continue;
        };
        let parsed = serde_json::from_slice::<TensorRecord>(payload)
            .map(|record| vec![record])
            .or_else(|_| serde_json::from_slice::<Vec<TensorRecord>>(payload));
        let Ok(mut parsed) = parsed else {
            diagnostics.skipped_objects += 1;
            continue;
        };
        parsed.retain(|record| {
            record.model_id == model_id
                && !record.tensor_name.is_empty()
                && !record.source_path.is_empty()
                && record.source_version > 0
        });
        if parsed.is_empty() {
            diagnostics.skipped_objects += 1;
            continue;
        }
        diagnostics.accepted_objects += 1;
        records.extend(parsed);
    }
    Ok(BuiltIndexGeneration {
        artifacts: TensorProjectionEngine::build(records)?,
        diagnostics,
    })
}

fn typed_definition(fields: &[anvil_api::v1::IndexField]) -> TypedJsonDefinition {
    TypedJsonDefinition {
        fields: fields
            .iter()
            .map(|field| TypedField {
                name: field.name.clone(),
                json_pointer: field.json_pointer.clone(),
            })
            .collect(),
    }
}

fn vector_definition(
    specification: &anvil_api::v1::VectorIndexSpec,
) -> Result<VectorDefinition, IndexError> {
    let metric = match ApiVectorMetric::try_from(specification.metric)
        .map_err(|_| IndexError::InvalidDefinition("unknown vector metric".into()))?
    {
        ApiVectorMetric::Cosine => VectorMetric::Cosine,
        ApiVectorMetric::Dot => VectorMetric::DotProduct,
        ApiVectorMetric::Euclidean => VectorMetric::Euclidean,
    };
    Ok(VectorDefinition {
        dimension: specification.dimensions as usize,
        metric,
    })
}

fn vector_values(value: &Value, dimension: usize, normalize: bool) -> Option<Vec<f32>> {
    let array = value.as_array()?;
    if array.len() != dimension {
        return None;
    }
    let mut values = array
        .iter()
        .map(|value| value.as_f64().map(|value| value as f32))
        .collect::<Option<Vec<_>>>()?;
    if values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    if normalize {
        let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
        if norm == 0.0 || !norm.is_finite() {
            return None;
        }
        for value in &mut values {
            *value /= norm;
        }
    }
    Some(values)
}

fn effective_weight(weight: f32) -> f32 {
    if weight == 0.0 { 1.0 } else { weight }
}

fn document_ref(object: &IndexBuildObject) -> DocumentRef {
    DocumentRef {
        path: object.path.clone(),
        version: object.version,
    }
}

fn parse_payload_json(object: &IndexBuildObject) -> Option<Value> {
    serde_json::from_slice(object.payload.as_deref()?).ok()
}

#[cfg(test)]
mod tests {
    use anvil_api::v1::{
        IndexField, TensorIndexSpec, TypedJsonIndexSpec, VectorIndexSpec, VectorMetric,
        index_specification,
    };

    use super::*;

    fn object(path: &str, payload: &[u8]) -> IndexBuildObject {
        IndexBuildObject {
            path: path.into(),
            version: 4,
            content_type: Some("application/json".into()),
            content_hash: *blake3::hash(payload).as_bytes(),
            content_length: payload.len() as u64,
            committed_at_unix_millis: 7,
            payload: Some(payload.to_vec()),
        }
    }

    #[test]
    fn malformed_json_is_a_generation_diagnostic_not_a_build_failure() {
        let specification = IndexSpecification {
            specification: Some(index_specification::Specification::TypedJson(
                TypedJsonIndexSpec {
                    fields: vec![IndexField {
                        name: "state".into(),
                        json_pointer: "/state".into(),
                    }],
                },
            )),
        };
        let built = build_generation(
            &specification,
            vec![
                object("good", br#"{"state":"open"}"#),
                object("bad", b"not-json"),
            ],
        )
        .unwrap();
        assert_eq!(built.diagnostics.accepted_objects, 1);
        assert_eq!(built.diagnostics.skipped_objects, 1);
        assert!(!built.artifacts.is_empty());
    }

    #[test]
    fn metadata_build_never_needs_payload_bytes() {
        let specification = IndexSpecification {
            specification: Some(index_specification::Specification::MetadataFilter(
                anvil_api::v1::MetadataFilterIndexSpec {
                    fields: vec!["content_type".into(), "content_length".into()],
                },
            )),
        };
        let mut input = object("a", b"opaque");
        input.payload = None;
        let built = build_generation(&specification, vec![input]).unwrap();
        assert_eq!(built.diagnostics.accepted_objects, 1);
    }

    #[test]
    fn tensor_build_accepts_only_addressable_records_for_the_selected_model() {
        let specification = IndexSpecification {
            specification: Some(index_specification::Specification::Tensor(
                TensorIndexSpec {
                    model_id: "model-a".into(),
                },
            )),
        };
        let built = build_generation(
            &specification,
            vec![
                object(
                    "manifests/a.json",
                    br#"{"model_id":"model-a","tensor_name":"encoder.weight","source_path":"weights/a.bin","source_version":7,"offset":0,"length":128,"dtype":"f32","shape":[8,4]}"#,
                ),
                object(
                    "manifests/b.json",
                    br#"{"model_id":"model-b","tensor_name":"encoder.weight","source_path":"weights/b.bin","source_version":8,"offset":0,"length":128,"dtype":"f32","shape":[8,4]}"#,
                ),
            ],
        )
        .unwrap();
        assert_eq!(built.diagnostics.accepted_objects, 1);
        assert_eq!(built.diagnostics.skipped_objects, 1);
    }

    #[test]
    fn vector_build_applies_its_json_pointer_once() {
        let specification = IndexSpecification {
            specification: Some(index_specification::Specification::Vector(
                VectorIndexSpec {
                    json_pointer: "/embedding".into(),
                    dimensions: 3,
                    metric: VectorMetric::Cosine as i32,
                    normalize: true,
                },
            )),
        };
        let built = build_generation(
            &specification,
            vec![object("document.json", br#"{"embedding":[1.0,0.0,0.0]}"#)],
        )
        .unwrap();
        assert_eq!(built.diagnostics.accepted_objects, 1);
        assert_eq!(built.diagnostics.skipped_objects, 0);
    }
}
