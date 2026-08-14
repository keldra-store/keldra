//! Bounded conversion of ordinary object state into format-v4 source mutations.
//!
//! This is the only runtime boundary which interprets a source payload for a
//! native segment. It emits format-v4 records or a versioned tombstone; it
//! projects directly into native format-v4 records.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;

use anvil_index::IndexError;
use anvil_index::v4::build::{
    MergeMutation, ProjectedDocValue, ProjectedPoint, ProjectedRecord, ProjectedSource,
    ProjectedTerm, ProjectedVector,
};
use anvil_index::v4::{
    Cardinality, DocValueCell, FIELD_PRESENCE_TERM, FieldCapabilities, FieldComponents, FieldId,
    FieldType, INDEX_TERM_BYTES, IndexKind, IndexSemantics, ObjectIdentity, ScalarValue, Schema,
    SortValue, TERM_TYPE_FIELD_PRESENCE, analyze_unicode_alphanumeric_lowercase,
    encode_physical_order_key, scalar_term, text_term,
};
use serde::{Deserialize, Serialize};

use crate::index_service::path_matches_prefix;

use super::json_projection::{
    ProjectedJson, ProjectionSelection, SelectedScalarField, SelectedScalarFields, project_json,
    projection_floor_bytes,
};
use super::source::{IndexBuildDiagnostics, IndexBuildObject, IndexSourceMutation};

const PROJECTION_FIXED_BYTES: usize = 256;
const RECORD_PROJECTION_EXPANSION: u64 = 16;
const TEXT_VALUE_POSITION_GAP: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct GitSourceRecord {
    repository_id: String,
    commit_id: String,
    tree_path: String,
    object_id: String,
    pack_path: String,
    pack_version: u64,
    offset: u64,
    length: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct TensorRecord {
    model_id: String,
    tensor_name: String,
    source_path: String,
    source_version: u64,
    offset: u64,
    length: u64,
    dtype: String,
    shape: Vec<u64>,
}

/// Minimum selected-state reservation required before source bytes are read.
///
/// The caller holds this reservation against the process-wide budget for the
/// index kind. Selective JSON charges only definition state here and enforces
/// the remaining bound while parsing. Whole-record Git and Tensor projections
/// conservatively charge a multiple of their exact ordinary-object length.
pub(crate) fn projection_admission_bytes(
    schema: &Schema,
    source: &IndexSourceMutation,
) -> Result<u64, IndexError> {
    schema.validate()?;
    let base = source_base_bytes(source)?;
    let extra = match &schema.semantics {
        IndexSemantics::Path => 0,
        IndexSemantics::MetadataFilter => {
            schema.fields.iter().try_fold(0usize, |total, field| {
                checked_add(total, field.name.len().saturating_add(24))
            })?
        }
        IndexSemantics::TypedJson => projection_floor_bytes(&scalar_selection(schema))?,
        IndexSemantics::FullText { .. } => projection_floor_bytes(&text_selection(schema))?,
        IndexSemantics::Vector {
            dimensions,
            normalization,
            ..
        } => projection_floor_bytes(&vector_selection(
            schema,
            *dimensions,
            normalize(*normalization),
        )?)?,
        IndexSemantics::Hybrid {
            dimensions,
            normalization,
            ..
        } => projection_floor_bytes(&hybrid_selection(
            schema,
            *dimensions,
            normalize(*normalization),
        )?)?,
        IndexSemantics::GitSource { repository_scope } => {
            whole_record_projection_bytes(source, repository_scope.len())?
        }
        IndexSemantics::Tensor { model_scope } => {
            whole_record_projection_bytes(source, model_scope.len())?
        }
    };
    let needed = checked_add(base, extra)?;
    u64::try_from(needed).map_err(|_| IndexError::OffsetOverflow)
}

/// Project one source version into a native upsert or an explicit tombstone.
///
/// Invalid JSON, a source outside the definition scope, and a valid payload
/// which produces no records all become a tombstone at the source version. An
/// older native segment can therefore never resurrect stale results.
pub(crate) fn project_mutation(
    schema: &Schema,
    source: IndexSourceMutation,
    payload: Option<&mut dyn Read>,
    max_projection_bytes: usize,
) -> Result<(MergeMutation, IndexBuildDiagnostics), IndexError> {
    let source_base = source_base_bytes(&source)?;
    let admitted = usize::try_from(projection_admission_bytes(schema, &source)?)
        .map_err(|_| IndexError::OffsetOverflow)?;
    if admitted > max_projection_bytes {
        return Err(IndexError::ResourceLimit {
            needed: admitted,
            limit: max_projection_bytes,
        });
    }
    let selected_limit =
        max_projection_bytes
            .checked_sub(source_base)
            .ok_or(IndexError::ResourceLimit {
                needed: source_base,
                limit: max_projection_bytes,
            })?;
    let object = match source {
        IndexSourceMutation::Upsert(object) => object,
        IndexSourceMutation::Remove(identity) => {
            return bounded_mutation(
                MergeMutation::Delete(identity),
                accepted(),
                max_projection_bytes,
            );
        }
    };

    if !source_matches_schema(schema, &object) {
        return bounded_mutation(
            MergeMutation::Delete(object_identity(&object)),
            skipped(),
            max_projection_bytes,
        );
    }

    let projected = match schema.kind {
        IndexKind::Path => Some(project_path(schema, &object)?),
        IndexKind::MetadataFilter => Some(project_metadata(schema, &object)?),
        IndexKind::TypedJson => project_typed_json(schema, &object, payload, selected_limit)?,
        IndexKind::FullText => project_full_text(schema, &object, payload, selected_limit)?,
        IndexKind::Vector => project_vector(schema, &object, payload, selected_limit)?,
        IndexKind::Hybrid => project_hybrid(schema, &object, payload, selected_limit)?,
        IndexKind::GitSource => project_git(schema, &object, payload)?,
        IndexKind::Tensor => project_tensor(schema, &object, payload)?,
    };
    match projected {
        Some(projected) => {
            projected.validate()?;
            bounded_mutation(
                MergeMutation::Upsert(projected),
                accepted(),
                max_projection_bytes,
            )
        }
        None => bounded_mutation(
            MergeMutation::Delete(object_identity(&object)),
            skipped(),
            max_projection_bytes,
        ),
    }
}

fn project_path(schema: &Schema, object: &IndexBuildObject) -> Result<ProjectedSource, IndexError> {
    let field = schema
        .fields
        .first()
        .ok_or_else(|| IndexError::InvalidDefinition("path schema has no field".into()))?;
    let value = ScalarValue::String(object.path.clone());
    Ok(source_with_records(
        object,
        vec![record(
            None,
            Vec::new(),
            scalar_terms(field.id, &[value])?,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )],
    ))
}

fn project_metadata(
    schema: &Schema,
    object: &IndexBuildObject,
) -> Result<ProjectedSource, IndexError> {
    let selected = schema
        .fields
        .iter()
        .map(|field| {
            Ok((
                field.name.clone(),
                SelectedScalarField {
                    values: vec![metadata_value(object, &field.name)?],
                    from_array: false,
                },
            ))
        })
        .collect::<Result<SelectedScalarFields, IndexError>>()?;
    let projected = scalar_record(schema, &selected)?;
    Ok(source_with_records(object, vec![projected]))
}

fn project_typed_json(
    schema: &Schema,
    object: &IndexBuildObject,
    payload: Option<&mut dyn Read>,
    selected_limit: usize,
) -> Result<Option<ProjectedSource>, IndexError> {
    let Some(ProjectedJson::Scalars(selected)) =
        project_selected_json(payload, scalar_selection(schema), selected_limit)?
    else {
        return Ok(None);
    };
    let selected = normalize_scalar_fields(schema, enforce_scalar_cardinality(schema, selected)?)?;
    require_projection_capacity(scalar_projection_bytes(schema, &selected)?, selected_limit)?;
    Ok(Some(source_with_records(
        object,
        vec![scalar_record(schema, &selected)?],
    )))
}

fn project_full_text(
    schema: &Schema,
    object: &IndexBuildObject,
    payload: Option<&mut dyn Read>,
    selected_limit: usize,
) -> Result<Option<ProjectedSource>, IndexError> {
    let Some(ProjectedJson::Strings(selected)) =
        project_selected_json(payload, text_selection(schema), selected_limit)?
    else {
        return Ok(None);
    };
    require_projection_capacity(text_projection_bytes(&selected, 0)?, selected_limit)?;
    Ok(Some(source_with_records(
        object,
        vec![text_record(schema, &selected, Vec::new())?],
    )))
}

fn project_vector(
    schema: &Schema,
    object: &IndexBuildObject,
    payload: Option<&mut dyn Read>,
    selected_limit: usize,
) -> Result<Option<ProjectedSource>, IndexError> {
    let IndexSemantics::Vector {
        dimensions,
        normalization,
        ..
    } = &schema.semantics
    else {
        return Err(IndexError::InvalidDefinition(
            "vector kind has non-vector semantics".into(),
        ));
    };
    let Some(ProjectedJson::Vector(values)) = project_selected_json(
        payload,
        vector_selection(schema, *dimensions, normalize(*normalization))?,
        selected_limit,
    )?
    else {
        return Ok(None);
    };
    require_projection_capacity(vector_projection_bytes(values.len())?, selected_limit)?;
    let field_id = vector_field(schema)?.id;
    Ok(Some(source_with_records(
        object,
        vec![record(
            None,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![ProjectedVector { field_id, values }],
            Vec::new(),
        )],
    )))
}

fn project_hybrid(
    schema: &Schema,
    object: &IndexBuildObject,
    payload: Option<&mut dyn Read>,
    selected_limit: usize,
) -> Result<Option<ProjectedSource>, IndexError> {
    let IndexSemantics::Hybrid {
        dimensions,
        normalization,
        ..
    } = &schema.semantics
    else {
        return Err(IndexError::InvalidDefinition(
            "hybrid kind has non-hybrid semantics".into(),
        ));
    };
    let Some(ProjectedJson::Hybrid { strings, vector }) = project_selected_json(
        payload,
        hybrid_selection(schema, *dimensions, normalize(*normalization))?,
        selected_limit,
    )?
    else {
        return Ok(None);
    };
    require_projection_capacity(
        text_projection_bytes(&strings, vector.len())?,
        selected_limit,
    )?;
    Ok(Some(source_with_records(
        object,
        vec![text_record(
            schema,
            &strings,
            vec![ProjectedVector {
                field_id: vector_field(schema)?.id,
                values: vector,
            }],
        )?],
    )))
}

fn project_git(
    schema: &Schema,
    object: &IndexBuildObject,
    payload: Option<&mut dyn Read>,
) -> Result<Option<ProjectedSource>, IndexError> {
    let IndexSemantics::GitSource { repository_scope } = &schema.semantics else {
        return Err(IndexError::InvalidDefinition(
            "Git kind has non-Git semantics".into(),
        ));
    };
    let records = parse_records::<GitSourceRecord>(payload)?
        .unwrap_or_default()
        .into_iter()
        .filter(|value| value.repository_id == *repository_scope && valid_git_record(value))
        .collect::<Vec<_>>();
    let mut keys = BTreeSet::new();
    if records.iter().any(|value| {
        !keys.insert((
            value.repository_id.as_str(),
            value.commit_id.as_str(),
            value.tree_path.as_str(),
        ))
    }) {
        return Ok(None);
    }
    let records = records
        .into_iter()
        .map(|value| git_record(schema, value))
        .collect::<Result<Vec<_>, IndexError>>()?;
    Ok((!records.is_empty()).then(|| source_with_records(object, records)))
}

fn project_tensor(
    schema: &Schema,
    object: &IndexBuildObject,
    payload: Option<&mut dyn Read>,
) -> Result<Option<ProjectedSource>, IndexError> {
    let IndexSemantics::Tensor { model_scope } = &schema.semantics else {
        return Err(IndexError::InvalidDefinition(
            "Tensor kind has non-Tensor semantics".into(),
        ));
    };
    let records = parse_records::<TensorRecord>(payload)?
        .unwrap_or_default()
        .into_iter()
        .filter(|value| value.model_id == *model_scope && valid_tensor_record(value))
        .collect::<Vec<_>>();
    let mut keys = BTreeSet::new();
    if records
        .iter()
        .any(|value| !keys.insert((value.model_id.as_str(), value.tensor_name.as_str())))
    {
        return Ok(None);
    }
    let records = records
        .into_iter()
        .map(|value| tensor_record(schema, value))
        .collect::<Result<Vec<_>, IndexError>>()?;
    Ok((!records.is_empty()).then(|| source_with_records(object, records)))
}

fn require_projection_capacity(needed: usize, limit: usize) -> Result<(), IndexError> {
    if needed > limit {
        Err(IndexError::ResourceLimit { needed, limit })
    } else {
        Ok(())
    }
}

/// Conservatively account for the selected scalar tree, the final projected
/// record, and temporary term/value representations before any of
/// those derived representations are allocated.
fn scalar_projection_bytes(
    schema: &Schema,
    selected: &SelectedScalarFields,
) -> Result<usize, IndexError> {
    let mut selected_bytes = std::mem::size_of::<SelectedScalarFields>();
    let mut output_bytes = std::mem::size_of::<ProjectedSource>()
        .checked_add(std::mem::size_of::<ProjectedRecord>())
        .ok_or(IndexError::OffsetOverflow)?;
    let mut temporary_bytes = 0usize;
    for field in &schema.fields {
        let Some((selected_name, values)) = selected.get_key_value(&field.name) else {
            continue;
        };
        selected_bytes = checked_add(
            selected_bytes,
            std::mem::size_of::<(String, SelectedScalarField)>()
                .checked_add(3 * std::mem::size_of::<usize>())
                .and_then(|bytes| bytes.checked_add(selected_name.capacity()))
                .and_then(|bytes| {
                    bytes.checked_add(
                        values
                            .values
                            .capacity()
                            .checked_mul(std::mem::size_of::<ScalarValue>())?,
                    )
                })
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        output_bytes = checked_add(
            output_bytes,
            std::mem::size_of::<ProjectedPoint>()
                .checked_add(std::mem::size_of::<ProjectedDocValue>())
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        output_bytes = checked_add(
            output_bytes,
            std::mem::size_of::<ProjectedTerm>()
                .checked_add(FIELD_PRESENCE_TERM.len())
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        for value in &values.values {
            let string_bytes = match value {
                ScalarValue::String(value) => value.capacity(),
                _ => 0,
            };
            selected_bytes = checked_add(selected_bytes, string_bytes)?;
            if field.field_type == FieldType::Text {
                let text = match value {
                    ScalarValue::Null => continue,
                    ScalarValue::String(text) => text,
                    _ => {
                        return Err(IndexError::Decode(format!(
                            "Typed JSON text field `{}` contains a non-string value",
                            field.name
                        )));
                    }
                };
                let (token_count, normalized_bytes) = analyzed_token_measure(text)?;
                let positions_bytes = token_count
                    .checked_mul(std::mem::size_of::<u32>())
                    .ok_or(IndexError::OffsetOverflow)?;
                output_bytes = checked_add(
                    output_bytes,
                    token_count
                        .checked_mul(std::mem::size_of::<ProjectedTerm>())
                        .and_then(|bytes| bytes.checked_add(normalized_bytes))
                        .and_then(|bytes| bytes.checked_add(positions_bytes))
                        .ok_or(IndexError::OffsetOverflow)?,
                )?;
                temporary_bytes = checked_add(
                    temporary_bytes,
                    token_count
                        .checked_mul(
                            std::mem::size_of::<(String, u32)>()
                                + std::mem::size_of::<(String, Vec<u32>)>()
                                + 3 * std::mem::size_of::<usize>(),
                        )
                        .and_then(|bytes| bytes.checked_add(normalized_bytes))
                        .and_then(|bytes| bytes.checked_add(positions_bytes))
                        .ok_or(IndexError::OffsetOverflow)?,
                )?;
                continue;
            }
            let term_bytes = scalar_term_bytes(value)?;
            output_bytes = checked_add(
                output_bytes,
                std::mem::size_of::<ProjectedTerm>()
                    .checked_add(std::mem::size_of::<ScalarValue>())
                    .and_then(|bytes| bytes.checked_add(string_bytes))
                    .and_then(|bytes| bytes.checked_add(term_bytes))
                    .ok_or(IndexError::OffsetOverflow)?,
            )?;
            // `scalar_terms` temporarily owns one ordered-map node per
            // distinct value. Charging one per input value is conservative.
            temporary_bytes = checked_add(
                temporary_bytes,
                std::mem::size_of::<((u8, Vec<u8>), u32)>()
                    .checked_add(3 * std::mem::size_of::<usize>())
                    .and_then(|bytes| bytes.checked_add(term_bytes))
                    .and_then(|bytes| bytes.checked_add(string_bytes))
                    .ok_or(IndexError::OffsetOverflow)?,
            )?;
        }
    }
    for order in &schema.physical_order {
        let ordered = schema
            .fields
            .iter()
            .find(|field| field.id == order.field_id)
            .and_then(|field| selected.get(&field.name))
            .and_then(|values| values.values.first());
        let maximum = match ordered {
            Some(ScalarValue::String(value)) => value
                .len()
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(3))
                .ok_or(IndexError::OffsetOverflow)?,
            Some(_) => 10,
            None => 1,
        };
        output_bytes = checked_add(output_bytes, maximum)?;
    }
    checked_add(selected_bytes, checked_add(output_bytes, temporary_bytes)?)
}

fn scalar_term_bytes(value: &ScalarValue) -> Result<usize, IndexError> {
    Ok(match value {
        ScalarValue::Null | ScalarValue::Boolean(_) => 1,
        ScalarValue::Signed(_) | ScalarValue::Number(_) | ScalarValue::Unsigned(_) => 8,
        ScalarValue::String(value) => value
            .len()
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?,
    })
}

/// Account for selected strings plus the analyzer's finite token vector,
/// ordered term aggregation, positions, and optional vector.
fn text_projection_bytes(
    selected: &BTreeMap<String, String>,
    vector_dimensions: usize,
) -> Result<usize, IndexError> {
    let mut selected_bytes = std::mem::size_of::<BTreeMap<String, String>>();
    let mut token_count = 0usize;
    let mut normalized_bytes = 0usize;
    for (name, text) in selected {
        selected_bytes = checked_add(
            selected_bytes,
            std::mem::size_of::<(String, String)>()
                .checked_add(3 * std::mem::size_of::<usize>())
                .and_then(|bytes| bytes.checked_add(name.capacity()))
                .and_then(|bytes| bytes.checked_add(text.capacity()))
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let (tokens, bytes) = analyzed_token_measure(text)?;
        token_count = checked_add(token_count, tokens)?;
        normalized_bytes = checked_add(normalized_bytes, bytes)?;
    }
    let positions_bytes = token_count
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or(IndexError::OffsetOverflow)?;
    let term_structures = token_count
        .checked_mul(std::mem::size_of::<ProjectedTerm>())
        .ok_or(IndexError::OffsetOverflow)?;
    let analyzer_structures = token_count
        .checked_mul(
            std::mem::size_of::<(String, u32)>()
                + std::mem::size_of::<(String, Vec<u32>)>()
                + 3 * std::mem::size_of::<usize>(),
        )
        .ok_or(IndexError::OffsetOverflow)?;
    let vector_bytes = vector_dimensions
        .checked_mul(std::mem::size_of::<f32>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<ProjectedVector>()))
        .ok_or(IndexError::OffsetOverflow)?;
    [
        selected_bytes,
        std::mem::size_of::<ProjectedSource>(),
        std::mem::size_of::<ProjectedRecord>(),
        term_structures,
        analyzer_structures,
        normalized_bytes,
        positions_bytes,
        vector_bytes,
    ]
    .into_iter()
    .try_fold(0usize, checked_add)
}

fn analyzed_token_measure(text: &str) -> Result<(usize, usize), IndexError> {
    let tokens = analyze_unicode_alphanumeric_lowercase(text, usize::MAX)?;
    let bytes = tokens
        .iter()
        .try_fold(0usize, |total, (token, _)| checked_add(total, token.len()))?;
    Ok((tokens.len(), bytes))
}

fn vector_projection_bytes(dimensions: usize) -> Result<usize, IndexError> {
    dimensions
        .checked_mul(std::mem::size_of::<f32>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<ProjectedVector>()))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<ProjectedRecord>()))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<ProjectedSource>()))
        .ok_or(IndexError::OffsetOverflow)
}

fn scalar_record(
    schema: &Schema,
    selected: &SelectedScalarFields,
) -> Result<ProjectedRecord, IndexError> {
    let mut terms = Vec::new();
    let mut points = Vec::new();
    let mut doc_values = Vec::new();
    let mut field_lengths = Vec::new();
    for field in &schema.fields {
        let Some(values) = selected.get(&field.name) else {
            continue;
        };
        let values = values.values.clone();
        if field.components.contains(FieldComponents::TERMS) {
            terms.push(ProjectedTerm {
                field_id: field.id,
                term_type: TERM_TYPE_FIELD_PRESENCE,
                term: FIELD_PRESENCE_TERM.to_vec(),
                frequency: 1,
                positions: Vec::new(),
            });
            if field.field_type == FieldType::Text {
                project_analyzed_values(field, &values, &mut terms, &mut field_lengths)?;
            } else {
                terms.extend(scalar_terms(field.id, &values)?);
            }
        }
        let null = values
            .iter()
            .any(|value| matches!(value, ScalarValue::Null));
        let non_null = values
            .iter()
            .filter(|value| !matches!(value, ScalarValue::Null))
            .cloned()
            .collect::<Vec<_>>();
        if field.components.contains(FieldComponents::POINTS) {
            points.push(ProjectedPoint {
                field_id: field.id,
                present: true,
                null,
                values: non_null.clone(),
            });
        }
        if field.components.contains(FieldComponents::DOC_VALUES) {
            doc_values.push(ProjectedDocValue {
                field_id: field.id,
                multi_valued: field.cardinality == Cardinality::Multi,
                cell: DocValueCell {
                    present: true,
                    null,
                    values: non_null,
                },
            });
        }
    }
    terms.sort_by(projected_term_order);
    let order = schema
        .physical_order
        .iter()
        .map(|order| {
            let value = schema
                .fields
                .iter()
                .find(|field| field.id == order.field_id)
                .and_then(|field| selected.get(&field.name))
                .and_then(|field| field.values.first())
                .cloned()
                .map_or(SortValue::Missing, SortValue::Value);
            (value, order.direction)
        })
        .collect::<Vec<_>>();
    let order_key = encode_physical_order_key(&order)?;
    Ok(record(
        None,
        order_key,
        terms,
        points,
        doc_values,
        Vec::new(),
        field_lengths,
    ))
}

fn text_record(
    schema: &Schema,
    selected: &BTreeMap<String, String>,
    vectors: Vec<ProjectedVector>,
) -> Result<ProjectedRecord, IndexError> {
    let mut terms = Vec::new();
    let mut lengths = Vec::new();
    for field in schema
        .fields
        .iter()
        .filter(|field| field.components.contains(FieldComponents::POSITIONS))
    {
        let Some(text) = selected.get(&field.name) else {
            continue;
        };
        project_analyzed_text(field.id, text, &mut terms, &mut lengths)?;
    }
    terms.sort_by(projected_term_order);
    Ok(record(
        None,
        Vec::new(),
        terms,
        Vec::new(),
        Vec::new(),
        vectors,
        lengths,
    ))
}

fn project_analyzed_text(
    field_id: FieldId,
    text: &str,
    terms: &mut Vec<ProjectedTerm>,
    lengths: &mut Vec<(FieldId, u32)>,
) -> Result<(), IndexError> {
    project_analyzed_strings(field_id, std::iter::once(text), terms, lengths)
}

fn project_analyzed_values(
    field: &anvil_index::v4::FieldSchema,
    values: &[ScalarValue],
    terms: &mut Vec<ProjectedTerm>,
    lengths: &mut Vec<(FieldId, u32)>,
) -> Result<(), IndexError> {
    let strings = values
        .iter()
        .filter(|value| !matches!(value, ScalarValue::Null))
        .map(|value| match value {
            ScalarValue::String(value) => Ok(value.as_str()),
            _ => Err(IndexError::Decode(format!(
                "text field `{}` contains a non-string value",
                field.name
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    project_analyzed_strings(field.id, strings, terms, lengths)
}

fn project_analyzed_strings<'a>(
    field_id: FieldId,
    strings: impl IntoIterator<Item = &'a str>,
    terms: &mut Vec<ProjectedTerm>,
    lengths: &mut Vec<(FieldId, u32)>,
) -> Result<(), IndexError> {
    let mut by_term = BTreeMap::<String, Vec<u32>>::new();
    let mut next_position = 0_u32;
    let mut field_length = 0_u32;
    for text in strings {
        let analyzed = analyze_unicode_alphanumeric_lowercase(text, usize::MAX)?;
        let value_length = u32::try_from(analyzed.len()).map_err(|_| IndexError::OffsetOverflow)?;
        for (token, position) in analyzed {
            let position = next_position
                .checked_add(position)
                .ok_or(IndexError::OffsetOverflow)?;
            by_term.entry(token).or_default().push(position);
        }
        field_length = field_length
            .checked_add(value_length)
            .ok_or(IndexError::OffsetOverflow)?;
        next_position = next_position
            .checked_add(value_length)
            .and_then(|position| position.checked_add(TEXT_VALUE_POSITION_GAP))
            .ok_or(IndexError::OffsetOverflow)?;
    }
    lengths.push((field_id, field_length));
    for (token, positions) in by_term {
        let (term_type, term) = text_term(&token)?;
        terms.push(ProjectedTerm {
            field_id,
            term_type,
            term,
            frequency: u32::try_from(positions.len()).map_err(|_| IndexError::OffsetOverflow)?,
            positions,
        });
    }
    Ok(())
}

fn git_record(schema: &Schema, value: GitSourceRecord) -> Result<ProjectedRecord, IndexError> {
    let scalar = vec![
        ScalarValue::String(value.repository_id.clone()),
        ScalarValue::String(value.commit_id.clone()),
        ScalarValue::String(value.tree_path.clone()),
    ];
    fixed_record(
        schema,
        scalar,
        ObjectIdentity {
            path: value.pack_path.clone(),
            version: value.pack_version,
        },
    )
}

fn tensor_record(schema: &Schema, value: TensorRecord) -> Result<ProjectedRecord, IndexError> {
    let scalar = vec![
        vec![ScalarValue::String(value.model_id.clone())],
        vec![ScalarValue::String(value.tensor_name.clone())],
    ];
    fixed_multi_record(
        schema,
        scalar,
        ObjectIdentity {
            path: value.source_path.clone(),
            version: value.source_version,
        },
    )
}

fn fixed_record(
    schema: &Schema,
    values: Vec<ScalarValue>,
    result_identity: ObjectIdentity,
) -> Result<ProjectedRecord, IndexError> {
    fixed_multi_record(
        schema,
        values.into_iter().map(|value| vec![value]).collect(),
        result_identity,
    )
}

fn fixed_multi_record(
    schema: &Schema,
    values: Vec<Vec<ScalarValue>>,
    result_identity: ObjectIdentity,
) -> Result<ProjectedRecord, IndexError> {
    if values.len() != schema.fields.len() {
        return Err(IndexError::InvalidDefinition(
            "fixed projection differs from its schema".into(),
        ));
    }
    let mut terms = Vec::new();
    for (field, values) in schema.fields.iter().zip(values) {
        if field.components.contains(FieldComponents::TERMS) {
            terms.extend(scalar_terms(field.id, &values)?);
        }
        if field.components.contains(FieldComponents::DOC_VALUES) {
            return Err(IndexError::InvalidDefinition(
                "fixed projection unexpectedly requires doc values".into(),
            ));
        }
    }
    terms.sort_by(projected_term_order);
    Ok(record(
        Some(result_identity),
        Vec::new(),
        terms,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    ))
}

fn scalar_terms(
    field_id: FieldId,
    values: &[ScalarValue],
) -> Result<Vec<ProjectedTerm>, IndexError> {
    let mut terms = BTreeMap::<(u8, Vec<u8>), u32>::new();
    for value in values {
        let key = scalar_term(value)?;
        let frequency = terms.entry(key).or_default();
        *frequency = frequency.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
    }
    Ok(terms
        .into_iter()
        .map(|((term_type, term), frequency)| ProjectedTerm {
            field_id,
            term_type,
            term,
            frequency,
            positions: Vec::new(),
        })
        .collect())
}

fn projected_term_order(left: &ProjectedTerm, right: &ProjectedTerm) -> std::cmp::Ordering {
    left.field_id
        .cmp(&right.field_id)
        .then_with(|| left.term_type.cmp(&right.term_type))
        .then_with(|| left.term.cmp(&right.term))
}

#[allow(clippy::too_many_arguments)]
fn record(
    result_identity: Option<ObjectIdentity>,
    order_key: Vec<u8>,
    terms: Vec<ProjectedTerm>,
    points: Vec<ProjectedPoint>,
    doc_values: Vec<ProjectedDocValue>,
    vectors: Vec<ProjectedVector>,
    field_lengths: Vec<(FieldId, u32)>,
) -> ProjectedRecord {
    ProjectedRecord {
        result_identity,
        order_key,
        terms,
        points,
        doc_values,
        vectors,
        field_lengths,
    }
}

fn source_with_records(
    object: &IndexBuildObject,
    records: Vec<ProjectedRecord>,
) -> ProjectedSource {
    ProjectedSource {
        source_identity: object_identity(object),
        records,
    }
}

fn object_identity(object: &IndexBuildObject) -> ObjectIdentity {
    object.identity()
}

fn source_matches_schema(schema: &Schema, object: &IndexBuildObject) -> bool {
    path_matches_prefix(&object.path, &schema.path_prefix)
        && !object.path.split('/').any(|segment| segment == "_anvil")
        && schema
            .content_type_scope
            .as_deref()
            .is_none_or(|expected| object.content_type.as_deref() == Some(expected))
}

fn scalar_selection(schema: &Schema) -> ProjectionSelection {
    ProjectionSelection::Scalars(
        schema
            .fields
            .iter()
            .map(|field| (field.name.clone(), field.source_selector.clone()))
            .collect(),
    )
}

fn text_selection(schema: &Schema) -> ProjectionSelection {
    ProjectionSelection::Strings(
        schema
            .fields
            .iter()
            .filter(|field| field.components.contains(FieldComponents::POSITIONS))
            .map(|field| (field.name.clone(), field.source_selector.clone()))
            .collect(),
    )
}

fn vector_selection(
    schema: &Schema,
    dimensions: u32,
    normalize: bool,
) -> Result<ProjectionSelection, IndexError> {
    Ok(ProjectionSelection::Vector {
        pointer: vector_field(schema)?.source_selector.clone(),
        dimensions: usize::try_from(dimensions).map_err(|_| IndexError::OffsetOverflow)?,
        normalize,
    })
}

fn hybrid_selection(
    schema: &Schema,
    dimensions: u32,
    normalize: bool,
) -> Result<ProjectionSelection, IndexError> {
    let ProjectionSelection::Strings(strings) = text_selection(schema) else {
        unreachable!();
    };
    Ok(ProjectionSelection::Hybrid {
        strings,
        vector_pointer: vector_field(schema)?.source_selector.clone(),
        dimensions: usize::try_from(dimensions).map_err(|_| IndexError::OffsetOverflow)?,
        normalize,
    })
}

fn vector_field(schema: &Schema) -> Result<&anvil_index::v4::FieldSchema, IndexError> {
    schema
        .fields
        .iter()
        .find(|field| field.components.contains(FieldComponents::VECTOR))
        .ok_or_else(|| IndexError::InvalidDefinition("vector schema has no vector field".into()))
}

fn normalize(value: anvil_index::v4::VectorNormalization) -> bool {
    value == anvil_index::v4::VectorNormalization::L2
}

fn enforce_scalar_cardinality(
    schema: &Schema,
    selected: SelectedScalarFields,
) -> Result<SelectedScalarFields, IndexError> {
    for field in &schema.fields {
        let Some(values) = selected.get(&field.name) else {
            continue;
        };
        if field.cardinality == Cardinality::Single && values.from_array {
            return Err(IndexError::Decode(format!(
                "Typed JSON field `{}` is declared single-valued but its source value is an array",
                field.name
            )));
        }
        if field.cardinality == Cardinality::Single && values.values.len() > 1 {
            return Err(IndexError::Decode(format!(
                "Typed JSON field `{}` is declared single-valued but its source produced {} values",
                field.name,
                values.values.len()
            )));
        }
    }
    Ok(selected)
}

fn normalize_scalar_fields(
    schema: &Schema,
    mut selected: SelectedScalarFields,
) -> Result<SelectedScalarFields, IndexError> {
    for field in &schema.fields {
        let Some(selected_field) = selected.get_mut(&field.name) else {
            continue;
        };
        for value in &mut selected_field.values {
            if matches!(value, ScalarValue::Null) && field.allow_null {
                continue;
            }
            *value = normalize_scalar(field, std::mem::replace(value, ScalarValue::Null))?;
        }
    }
    Ok(selected)
}

fn normalize_scalar(
    field: &anvil_index::v4::FieldSchema,
    value: ScalarValue,
) -> Result<ScalarValue, IndexError> {
    let invalid = || {
        IndexError::Decode(format!(
            "Typed JSON field `{}` contains a value outside its declared type",
            field.name
        ))
    };
    Ok(match (field.field_type, value) {
        (FieldType::Boolean, ScalarValue::Boolean(value)) => ScalarValue::Boolean(value),
        (FieldType::SignedInteger, ScalarValue::Signed(value)) => ScalarValue::Signed(value),
        (FieldType::SignedInteger, ScalarValue::Unsigned(value)) => {
            ScalarValue::Signed(i64::try_from(value).map_err(|_| invalid())?)
        }
        (FieldType::UnsignedInteger, ScalarValue::Unsigned(value)) => ScalarValue::Unsigned(value),
        (FieldType::UnsignedInteger, ScalarValue::Signed(0)) => ScalarValue::Unsigned(0),
        (FieldType::Float, ScalarValue::Number(bits)) => ScalarValue::Number(bits),
        (FieldType::Float, ScalarValue::Signed(value)) => ScalarValue::number(value as f64)?,
        (FieldType::Float, ScalarValue::Unsigned(value)) => ScalarValue::number(value as f64)?,
        (FieldType::Keyword | FieldType::Text, ScalarValue::String(value)) => {
            if field.field_type == FieldType::Keyword
                && value.len() > INDEX_TERM_BYTES
                && field.capabilities != FieldCapabilities::EXACT
            {
                return Err(IndexError::ResourceLimit {
                    needed: value.len(),
                    limit: INDEX_TERM_BYTES,
                });
            }
            ScalarValue::String(value)
        }
        _ => return Err(invalid()),
    })
}

fn metadata_value(object: &IndexBuildObject, name: &str) -> Result<ScalarValue, IndexError> {
    Ok(match name {
        "path" => ScalarValue::String(object.path.clone()),
        "version" => ScalarValue::Unsigned(object.version),
        "content_type" => object
            .content_type
            .clone()
            .map_or(ScalarValue::Null, ScalarValue::String),
        "content_length" => ScalarValue::Unsigned(object.content_length),
        "content_hash" => ScalarValue::String(hex::encode(object.content_hash)),
        "committed_at_unix_millis" => ScalarValue::Unsigned(object.committed_at_unix_millis),
        _ => {
            return Err(IndexError::InvalidDefinition(
                "metadata schema contains an unsupported field".into(),
            ));
        }
    })
}

fn valid_git_record(value: &GitSourceRecord) -> bool {
    valid_text(&value.repository_id)
        && valid_text(&value.commit_id)
        && valid_text(&value.tree_path)
        && valid_text(&value.object_id)
        && valid_text(&value.pack_path)
        && value.pack_version > 0
}

fn valid_tensor_record(value: &TensorRecord) -> bool {
    valid_text(&value.model_id)
        && valid_text(&value.tensor_name)
        && valid_text(&value.source_path)
        && valid_text(&value.dtype)
        && value.source_version > 0
}

fn valid_text(value: &str) -> bool {
    !value.is_empty() && !value.contains('\0')
}

fn project_selected_json(
    payload: Option<&mut dyn Read>,
    selection: ProjectionSelection,
    max_projection_bytes: usize,
) -> Result<Option<ProjectedJson>, IndexError> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    project_json(payload, &selection, max_projection_bytes)
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

fn parse_records<T: serde::de::DeserializeOwned>(
    payload: Option<&mut dyn Read>,
) -> Result<Option<Vec<T>>, IndexError> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    match serde_json::from_reader::<_, OneOrMany<T>>(payload) {
        Ok(OneOrMany::One(record)) => Ok(Some(vec![record])),
        Ok(OneOrMany::Many(records)) => Ok(Some(records)),
        Err(error) if error.is_syntax() || error.is_data() || error.is_eof() => Ok(None),
        Err(error) => Err(IndexError::Io(error.to_string())),
    }
}

fn bounded_mutation(
    mutation: MergeMutation,
    diagnostics: IndexBuildDiagnostics,
    limit: usize,
) -> Result<(MergeMutation, IndexBuildDiagnostics), IndexError> {
    let resident = match &mutation {
        MergeMutation::Upsert(source) => source.resident_bytes()?,
        MergeMutation::Delete(identity) => {
            std::mem::size_of::<MergeMutation>().saturating_add(identity.path.len())
        }
    };
    let needed = resident.saturating_add(PROJECTION_FIXED_BYTES);
    if needed > limit {
        return Err(IndexError::ResourceLimit { needed, limit });
    }
    Ok((mutation, diagnostics))
}

fn source_base_bytes(source: &IndexSourceMutation) -> Result<usize, IndexError> {
    let bytes = match source {
        IndexSourceMutation::Upsert(object) => object
            .path
            .len()
            .checked_add(object.content_type.as_ref().map_or(0, String::len))
            .and_then(|value| value.checked_add(PROJECTION_FIXED_BYTES + 32)),
        IndexSourceMutation::Remove(document) => {
            document.path.len().checked_add(PROJECTION_FIXED_BYTES)
        }
    };
    bytes.ok_or(IndexError::OffsetOverflow)
}

fn whole_record_projection_bytes(
    source: &IndexSourceMutation,
    fixed_value_bytes: usize,
) -> Result<usize, IndexError> {
    let content_length = match source {
        IndexSourceMutation::Upsert(object) => object.content_length,
        IndexSourceMutation::Remove(_) => 0,
    };
    usize::try_from(
        content_length
            .checked_mul(RECORD_PROJECTION_EXPANSION)
            .ok_or(IndexError::OffsetOverflow)?,
    )
    .map_err(|_| IndexError::OffsetOverflow)?
    .checked_add(fixed_value_bytes)
    .ok_or(IndexError::OffsetOverflow)
}

fn checked_add(left: usize, right: usize) -> Result<usize, IndexError> {
    left.checked_add(right).ok_or(IndexError::OffsetOverflow)
}

fn accepted() -> IndexBuildDiagnostics {
    IndexBuildDiagnostics {
        accepted_objects: 1,
        skipped_objects: 0,
    }
}

fn skipped() -> IndexBuildDiagnostics {
    IndexBuildDiagnostics {
        accepted_objects: 0,
        skipped_objects: 1,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use anvil_api::v1::index_specification::Specification;
    use anvil_api::v1::{
        FullTextField, FullTextIndexSpec, GitSourceIndexSpec, HybridIndexSpec, IndexField,
        IndexFieldCapability, IndexFieldCardinality, IndexOrder, IndexOrderDirection,
        IndexSpecification, KeywordIndexField, MetadataFilterIndexSpec, PathIndexSpec,
        SignedIntegerIndexField, TensorIndexSpec, TextIndexField, TypedJsonIndexSpec,
        VectorIndexSpec, VectorMetric as ApiVectorMetric, index_field,
    };
    use anvil_index::v4::{
        FIELD_PRESENCE_TERM, TERM_TYPE_FIELD_PRESENCE, TERM_TYPE_NULL, TERM_TYPE_TEXT,
        VectorNormalization,
    };

    use super::*;
    use crate::index_runtime::v4_schema::compile_schema;

    const LIMIT: usize = 256 * 1024;

    fn object(path: &str, length: u64) -> IndexBuildObject {
        IndexBuildObject {
            path: path.into(),
            version: 7,
            content_type: Some("application/json".into()),
            content_hash: [0xab; 32],
            content_length: length,
            committed_at_unix_millis: 19,
        }
    }

    fn schema(specification: Specification) -> Schema {
        compile_schema(
            "records",
            Some("application/json"),
            &IndexSpecification {
                specification: Some(specification),
            },
        )
        .unwrap()
    }

    fn keyword_field(name: &str, pointer: &str, multi: bool) -> IndexField {
        IndexField {
            name: name.into(),
            json_pointer: pointer.into(),
            cardinality: if multi {
                IndexFieldCardinality::Multi
            } else {
                IndexFieldCardinality::Single
            } as i32,
            capabilities: vec![
                IndexFieldCapability::Exact as i32,
                IndexFieldCapability::Facet as i32,
            ],
            field_type: Some(index_field::FieldType::Keyword(KeywordIndexField {})),
        }
    }

    fn ordered_signed_field(name: &str, pointer: &str) -> IndexField {
        IndexField {
            name: name.into(),
            json_pointer: pointer.into(),
            cardinality: IndexFieldCardinality::Single as i32,
            capabilities: vec![
                IndexFieldCapability::Range as i32,
                IndexFieldCapability::Order as i32,
            ],
            field_type: Some(index_field::FieldType::SignedInteger(
                SignedIntegerIndexField {},
            )),
        }
    }

    fn text_field(name: &str, pointer: &str, multi: bool) -> IndexField {
        IndexField {
            name: name.into(),
            json_pointer: pointer.into(),
            cardinality: if multi {
                IndexFieldCardinality::Multi
            } else {
                IndexFieldCardinality::Single
            } as i32,
            capabilities: vec![IndexFieldCapability::FullText as i32],
            field_type: Some(index_field::FieldType::Text(TextIndexField::default())),
        }
    }

    fn upsert(schema: &Schema, body: Option<&[u8]>) -> (ProjectedSource, IndexBuildDiagnostics) {
        let mut input = body.map(Cursor::new);
        let payload = input.as_mut().map(|value| value as &mut dyn Read);
        let (mutation, diagnostics) = project_mutation(
            schema,
            IndexSourceMutation::Upsert(object(
                "records/source.json",
                body.map_or(0, |value| value.len()) as u64,
            )),
            payload,
            LIMIT,
        )
        .unwrap();
        let MergeMutation::Upsert(source) = mutation else {
            panic!("expected native upsert")
        };
        (source, diagnostics)
    }

    #[test]
    fn path_projects_one_canonical_term() {
        let schema = schema(Specification::Path(PathIndexSpec {}));
        let (source, diagnostics) = upsert(&schema, None);
        assert_eq!(source.records.len(), 1);
        assert_eq!(source.records[0].terms.len(), 1);
        assert_eq!(diagnostics.accepted_objects, 1);
        assert_eq!(source.records[0].result_identity, None);
    }

    #[test]
    fn metadata_projects_terms_and_numeric_points_without_source_copies() {
        let schema = schema(Specification::MetadataFilter(MetadataFilterIndexSpec {
            fields: vec![
                "path".into(),
                "content_length".into(),
                "content_type".into(),
            ],
        }));
        let (source, _) = upsert(&schema, None);
        let record = &source.records[0];
        assert_eq!(record.points.len(), 1);
        assert_eq!(record.points[0].values, [ScalarValue::Unsigned(0)]);
        assert!(record.doc_values.is_empty());
        assert!(
            record
                .terms
                .iter()
                .any(|term| term.field_id == FieldId::new(0))
        );
    }

    #[test]
    fn typed_json_projects_tagged_scalars_and_declared_physical_order() {
        let schema = schema(Specification::TypedJson(TypedJsonIndexSpec {
            fields: vec![
                ordered_signed_field("modified", "/modified"),
                keyword_field("tags", "/tags", true),
            ],
            physical_order: vec![IndexOrder {
                field: "modified".into(),
                direction: IndexOrderDirection::Descending as i32,
            }],
        }));
        let (source, _) = upsert(
            &schema,
            Some(br#"{"modified":9,"tags":["rust",null,"rust"]}"#),
        );
        let record = &source.records[0];
        assert!(!record.order_key.is_empty());
        assert!(record.doc_values[1].cell.null);
        assert_eq!(record.doc_values[1].cell.values.len(), 2);
        assert!(record.terms.iter().any(|term| term.frequency == 2));
    }

    #[test]
    fn typed_multi_text_analyzes_every_value_without_cross_value_phrases() {
        let schema = schema(Specification::TypedJson(TypedJsonIndexSpec {
            fields: vec![text_field("body", "/body", true)],
            physical_order: Vec::new(),
        }));
        let (source, _) = upsert(
            &schema,
            Some(br#"{"body":["alpha beta",null,"gamma delta"]}"#),
        );
        let record = &source.records[0];
        let beta = record
            .terms
            .iter()
            .find(|term| term.term == b"beta")
            .unwrap();
        let gamma = record
            .terms
            .iter()
            .find(|term| term.term == b"gamma")
            .unwrap();

        assert_eq!(beta.positions, [1]);
        assert_eq!(gamma.positions, [3]);
        assert_eq!(record.field_lengths, [(FieldId::new(0), 4)]);
    }

    #[test]
    fn point_and_doc_value_only_field_emits_no_presence_term() {
        let schema = schema(Specification::TypedJson(TypedJsonIndexSpec {
            fields: vec![ordered_signed_field("modified", "/modified")],
            physical_order: vec![IndexOrder {
                field: "modified".into(),
                direction: IndexOrderDirection::Descending as i32,
            }],
        }));
        let (source, _) = upsert(&schema, Some(br#"{"modified":9}"#));
        let record = &source.records[0];

        assert!(record.terms.is_empty());
        assert_eq!(record.points.len(), 1);
        assert!(record.points[0].present);
        assert!(!record.points[0].null);
        assert_eq!(record.doc_values.len(), 1);

        let (source, _) = upsert(&schema, Some(br#"{"modified":null}"#));
        let record = &source.records[0];
        assert!(record.terms.is_empty());
        assert!(record.points[0].present);
        assert!(record.points[0].null);
        assert!(record.points[0].values.is_empty());
        assert!(record.doc_values[0].cell.null);
    }

    #[test]
    fn typed_json_indexes_valid_documents_when_every_selected_field_is_missing() {
        let schema = schema(Specification::TypedJson(TypedJsonIndexSpec {
            fields: vec![ordered_signed_field("modified", "/modified")],
            physical_order: vec![IndexOrder {
                field: "modified".into(),
                direction: IndexOrderDirection::Descending as i32,
            }],
        }));
        let (source, diagnostics) = upsert(&schema, Some(br#"{"other":9}"#));
        let record = &source.records[0];

        assert_eq!(diagnostics.accepted_objects, 1);
        assert_eq!(diagnostics.skipped_objects, 0);
        assert!(record.terms.is_empty());
        assert!(record.points.is_empty());
        assert!(record.doc_values.is_empty());
        assert!(!record.order_key.is_empty());
    }

    #[test]
    fn typed_cardinality_distinguishes_empty_multi_value_and_rejects_single_value_arrays() {
        let multi = schema(Specification::TypedJson(TypedJsonIndexSpec {
            fields: vec![keyword_field("tags", "/tags", true)],
            physical_order: Vec::new(),
        }));
        let (source, _) = upsert(&multi, Some(br#"{"tags":[]}"#));
        assert!(source.records[0].doc_values[0].cell.present);
        assert!(source.records[0].doc_values[0].cell.values.is_empty());
        assert_eq!(source.records[0].terms.len(), 1);
        assert_eq!(
            source.records[0].terms[0].term_type,
            TERM_TYPE_FIELD_PRESENCE
        );
        assert_eq!(source.records[0].terms[0].term, FIELD_PRESENCE_TERM);

        let single = schema(Specification::TypedJson(TypedJsonIndexSpec {
            fields: vec![keyword_field("state", "/state", false)],
            physical_order: Vec::new(),
        }));
        let (source, _) = upsert(&single, Some(br#"{"state":null}"#));
        let record = &source.records[0];
        assert!(record.doc_values[0].cell.present);
        assert!(record.doc_values[0].cell.null);
        assert!(record.doc_values[0].cell.values.is_empty());
        assert!(
            record
                .terms
                .iter()
                .any(|term| term.term_type == TERM_TYPE_FIELD_PRESENCE
                    && term.term == FIELD_PRESENCE_TERM)
        );
        assert!(
            record
                .terms
                .iter()
                .any(|term| term.term_type == TERM_TYPE_NULL)
        );

        let mut input = Cursor::new(br#"{"state":["active"]}"#);
        let error = project_mutation(
            &single,
            IndexSourceMutation::Upsert(object("records/single.json", 20)),
            Some(&mut input),
            LIMIT,
        )
        .unwrap_err();
        assert_eq!(
            error,
            IndexError::Decode(
                "Typed JSON field `state` is declared single-valued but its source value is an array"
                    .into()
            )
        );
    }

    #[test]
    fn full_text_projects_unicode_tokens_frequency_positions_and_norms() {
        let schema = schema(Specification::FullText(FullTextIndexSpec {
            fields: vec![FullTextField {
                name: "body".into(),
                json_pointer: "/body".into(),
            }],
        }));
        let (source, _) = upsert(&schema, Some(r#"{"body":"Rust, RUST café"}"#.as_bytes()));
        let record = &source.records[0];
        let rust = record
            .terms
            .iter()
            .find(|term| term.term == b"rust")
            .unwrap();
        assert_eq!(rust.term_type, TERM_TYPE_TEXT);
        assert_eq!(rust.frequency, 2);
        assert_eq!(rust.positions, [0, 1]);
        assert_eq!(record.field_lengths, [(FieldId::new(0), 3)]);
        assert!(record.doc_values.is_empty());
    }

    #[test]
    fn full_text_rejects_one_token_over_the_format_bound() {
        let schema = schema(Specification::FullText(FullTextIndexSpec {
            fields: vec![FullTextField {
                name: "body".into(),
                json_pointer: "/body".into(),
            }],
        }));
        let body = serde_json::to_vec(&serde_json::json!({
            "body": "A".repeat(INDEX_TERM_BYTES + 1),
        }))
        .unwrap();
        let mut payload = Cursor::new(&body);
        let error = project_mutation(
            &schema,
            IndexSourceMutation::Upsert(object("records/text.json", body.len() as u64)),
            Some(&mut payload),
            LIMIT,
        )
        .unwrap_err();
        assert!(matches!(error, IndexError::ResourceLimit { .. }));
    }

    #[test]
    fn vector_projects_normalized_fixed_width_values() {
        let schema = schema(Specification::Vector(VectorIndexSpec {
            json_pointer: "/embedding".into(),
            dimensions: 2,
            metric: ApiVectorMetric::Cosine as i32,
            normalize: true,
        }));
        assert!(matches!(
            schema.semantics,
            IndexSemantics::Vector {
                normalization: VectorNormalization::L2,
                ..
            }
        ));
        let (source, _) = upsert(&schema, Some(br#"{"embedding":[3,4]}"#));
        assert_eq!(source.records[0].vectors.len(), 1);
        assert!((source.records[0].vectors[0].values[0] - 0.6).abs() < f32::EPSILON);
    }

    #[test]
    fn hybrid_shares_one_record_identity_for_text_and_vector_components() {
        let schema = schema(Specification::Hybrid(HybridIndexSpec {
            full_text: Some(FullTextIndexSpec {
                fields: vec![FullTextField {
                    name: "title".into(),
                    json_pointer: "/title".into(),
                }],
            }),
            vector: Some(VectorIndexSpec {
                json_pointer: "/embedding".into(),
                dimensions: 2,
                metric: ApiVectorMetric::Dot as i32,
                normalize: false,
            }),
            full_text_weight: 1.0,
            vector_weight: 1.0,
        }));
        let (source, _) = upsert(
            &schema,
            Some(br#"{"title":"Fast search","embedding":[1,2]}"#),
        );
        assert_eq!(source.records.len(), 1);
        assert_eq!(source.records[0].vectors.len(), 1);
        assert_eq!(source.records[0].field_lengths, [(FieldId::new(0), 2)]);
    }

    #[test]
    fn git_projection_separates_projection_source_from_returned_pack_identity() {
        let schema = schema(Specification::GitSource(GitSourceIndexSpec {
            repository_id: "repo".into(),
        }));
        let record = GitSourceRecord {
            repository_id: "repo".into(),
            commit_id: "abc".into(),
            tree_path: "src/lib.rs".into(),
            object_id: "def".into(),
            pack_path: "packs/one.pack".into(),
            pack_version: 4,
            offset: 12,
            length: 90,
        };
        let body = serde_json::to_vec(&record).unwrap();
        let (source, _) = upsert(&schema, Some(&body));
        assert_eq!(source.source_identity.path, "records/source.json");
        assert_eq!(
            source.records[0].result_identity,
            Some(ObjectIdentity {
                path: "packs/one.pack".into(),
                version: 4,
            })
        );
        assert_eq!(source.records[0].terms.len(), 3);
        assert!(source.records[0].doc_values.is_empty());
    }

    #[test]
    fn tensor_projection_reconstructs_shape_and_returned_blob_identity() {
        let schema = schema(Specification::Tensor(TensorIndexSpec {
            model_id: "model".into(),
        }));
        let record = TensorRecord {
            model_id: "model".into(),
            tensor_name: "layer.weight".into(),
            source_path: "weights/model.bin".into(),
            source_version: 9,
            offset: 16,
            length: 128,
            dtype: "f32".into(),
            shape: vec![4, 8],
        };
        let body = serde_json::to_vec(&record).unwrap();
        let (source, _) = upsert(&schema, Some(&body));
        assert_eq!(source.records[0].terms.len(), 2);
        assert_eq!(
            source.records[0].result_identity,
            Some(ObjectIdentity {
                path: "weights/model.bin".into(),
                version: 9,
            })
        );
        assert!(source.records[0].doc_values.is_empty());
    }

    #[test]
    fn duplicate_git_or_tensor_logical_keys_become_removal_evidence() {
        let git_schema = schema(Specification::GitSource(GitSourceIndexSpec {
            repository_id: "repo".into(),
        }));
        let record = GitSourceRecord {
            repository_id: "repo".into(),
            commit_id: "abc".into(),
            tree_path: "src/lib.rs".into(),
            object_id: "def".into(),
            pack_path: "packs/one.pack".into(),
            pack_version: 4,
            offset: 12,
            length: 90,
        };
        let body = serde_json::to_vec(&vec![record.clone(), record]).unwrap();
        let mut input = Cursor::new(&body);
        let (mutation, diagnostics) = project_mutation(
            &git_schema,
            IndexSourceMutation::Upsert(object("records/git.json", body.len() as u64)),
            Some(&mut input),
            LIMIT,
        )
        .unwrap();
        assert!(matches!(mutation, MergeMutation::Delete(_)));
        assert_eq!(diagnostics.skipped_objects, 1);

        let tensor_schema = schema(Specification::Tensor(TensorIndexSpec {
            model_id: "model".into(),
        }));
        let record = TensorRecord {
            model_id: "model".into(),
            tensor_name: "layer.weight".into(),
            source_path: "weights/model.bin".into(),
            source_version: 9,
            offset: 16,
            length: 128,
            dtype: "f32".into(),
            shape: vec![4, 8],
        };
        let body = serde_json::to_vec(&vec![record.clone(), record]).unwrap();
        let mut input = Cursor::new(&body);
        let (mutation, diagnostics) = project_mutation(
            &tensor_schema,
            IndexSourceMutation::Upsert(object("records/tensor.json", body.len() as u64)),
            Some(&mut input),
            LIMIT,
        )
        .unwrap();
        assert!(matches!(mutation, MergeMutation::Delete(_)));
        assert_eq!(diagnostics.skipped_objects, 1);
    }

    #[test]
    fn malformed_out_of_scope_and_explicit_delete_all_emit_versioned_tombstones() {
        let schema = schema(Specification::TypedJson(TypedJsonIndexSpec {
            fields: vec![keyword_field("state", "/state", false)],
            physical_order: Vec::new(),
        }));
        let mut malformed = Cursor::new(b"not-json");
        let (mutation, diagnostics) = project_mutation(
            &schema,
            IndexSourceMutation::Upsert(object("records/bad.json", 8)),
            Some(&mut malformed),
            LIMIT,
        )
        .unwrap();
        assert!(matches!(
            mutation,
            MergeMutation::Delete(ObjectIdentity { version: 7, .. })
        ));
        assert_eq!(diagnostics.skipped_objects, 1);

        let mut valid = Cursor::new(br#"{"state":"active"}"#);
        let (mutation, _) = project_mutation(
            &schema,
            IndexSourceMutation::Upsert(object("outside/bad.json", 18)),
            Some(&mut valid),
            LIMIT,
        )
        .unwrap();
        assert!(matches!(mutation, MergeMutation::Delete(_)));

        let deleted_path = String::from("records/deleted.json");
        let deleted_path_pointer = deleted_path.as_ptr();
        let (mutation, diagnostics) = project_mutation(
            &schema,
            IndexSourceMutation::Remove(ObjectIdentity {
                path: deleted_path,
                version: 11,
            }),
            None,
            LIMIT,
        )
        .unwrap();
        let MergeMutation::Delete(identity) = mutation else {
            panic!("explicit removal did not emit a tombstone");
        };
        assert_eq!(identity.version, 11);
        assert_eq!(identity.path.as_ptr(), deleted_path_pointer);
        assert_eq!(diagnostics.accepted_objects, 1);
    }

    #[test]
    fn projection_admission_rejects_before_reading_an_oversized_whole_record() {
        let schema = schema(Specification::GitSource(GitSourceIndexSpec {
            repository_id: "repo".into(),
        }));
        let source = IndexSourceMutation::Upsert(object("records/git.json", 1024 * 1024));
        let mut payload = Cursor::new(b"{}");
        let error = project_mutation(&schema, source, Some(&mut payload), LIMIT).unwrap_err();
        assert!(matches!(error, IndexError::ResourceLimit { .. }));
        assert_eq!(payload.position(), 0);
    }

    #[test]
    fn text_preflight_matches_the_native_analyzer_token_boundaries() {
        let text = format!("{} RUST café", "A".repeat(INDEX_TERM_BYTES));
        let analyzed = analyze_unicode_alphanumeric_lowercase(&text, usize::MAX).unwrap();
        let (tokens, bytes) = analyzed_token_measure(&text).unwrap();
        assert_eq!(tokens, analyzed.len());
        assert_eq!(
            bytes,
            analyzed.iter().map(|(token, _)| token.len()).sum::<usize>()
        );
    }

    #[test]
    fn downstream_text_expansion_is_rejected_after_selection_but_before_projection() {
        let schema = schema(Specification::FullText(FullTextIndexSpec {
            fields: vec![FullTextField {
                name: "body".into(),
                json_pointer: "/body".into(),
            }],
        }));
        let body = serde_json::to_vec(&serde_json::json!({
            "body": "a ".repeat(64),
        }))
        .unwrap();
        let mut payload = Cursor::new(&body);
        let error = project_mutation(
            &schema,
            IndexSourceMutation::Upsert(object("records/text.json", body.len() as u64)),
            Some(&mut payload),
            4 * 1024,
        )
        .unwrap_err();
        assert!(matches!(error, IndexError::ResourceLimit { .. }));
        assert_eq!(payload.position(), body.len() as u64);
    }

    #[test]
    fn typed_text_expansion_uses_the_same_bounded_preflight() {
        let schema = schema(Specification::TypedJson(TypedJsonIndexSpec {
            fields: vec![text_field("body", "/body", false)],
            physical_order: Vec::new(),
        }));
        let body = serde_json::to_vec(&serde_json::json!({
            "body": "a ".repeat(64),
        }))
        .unwrap();
        let mut payload = Cursor::new(&body);
        let error = project_mutation(
            &schema,
            IndexSourceMutation::Upsert(object("records/typed-text.json", body.len() as u64)),
            Some(&mut payload),
            4 * 1024,
        )
        .unwrap_err();
        assert!(matches!(error, IndexError::ResourceLimit { .. }));
        assert_eq!(payload.position(), body.len() as u64);
    }
}
