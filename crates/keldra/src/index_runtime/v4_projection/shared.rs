//! Definition-neutral Typed JSON fact assembly.

use super::*;

/// Assemble one Typed JSON mutation from definition-neutral scalar facts.
/// Payload selection has already happened once in the source-node mapper; this
/// step applies the exact schema's cardinality, type, analyzer, component and
/// physical-order semantics.
pub(crate) fn project_typed_json_from_shared_scalars(
    schema: &Schema,
    source: IndexSourceMutation,
    shared: Option<&ProjectedScalarPointers>,
    max_projection_bytes: usize,
) -> Result<(MergeMutation, IndexBuildDiagnostics), IndexError> {
    if schema.kind != IndexKind::TypedJson {
        return Err(IndexError::InvalidDefinition(
            "shared scalar assembly requires a Typed JSON schema".into(),
        ));
    }
    let source_base = source_base_bytes(&source)?;
    let selected_limit =
        max_projection_bytes
            .checked_sub(source_base)
            .ok_or(IndexError::ResourceLimit {
                needed: source_base,
                limit: max_projection_bytes,
            })?;
    let object = match source {
        IndexSourceMutation::Remove(identity) => {
            return bounded_mutation(
                MergeMutation::Delete(identity),
                accepted(),
                max_projection_bytes,
            );
        }
        IndexSourceMutation::Upsert(object) => object,
    };
    if !source_matches_schema(schema, &object) {
        return bounded_mutation(
            MergeMutation::Delete(object_identity(&object)),
            skipped(),
            max_projection_bytes,
        );
    }
    let Some(shared) = shared else {
        return bounded_mutation(
            MergeMutation::Delete(object_identity(&object)),
            skipped(),
            max_projection_bytes,
        );
    };
    // The shared cache owns its copy under the mapper account. Preflight the
    // definition-local clone against this builder lane before allocating it.
    require_projection_capacity(shared_scalar_clone_bytes(schema, shared)?, selected_limit)?;
    let selected = schema
        .fields
        .iter()
        .filter_map(|field| {
            shared
                .get(&field.source_selector)
                .cloned()
                .map(|value| (field.name.clone(), value))
        })
        .collect::<SelectedScalarFields>();
    let selected = normalize_scalar_fields(schema, enforce_scalar_cardinality(schema, selected)?)?;
    require_projection_capacity(scalar_projection_bytes(schema, &selected)?, selected_limit)?;
    bounded_mutation(
        MergeMutation::Upsert(source_with_records(
            &object,
            vec![scalar_record(schema, &selected)?],
        )),
        accepted(),
        max_projection_bytes,
    )
}

fn shared_scalar_clone_bytes(
    schema: &Schema,
    shared: &ProjectedScalarPointers,
) -> Result<usize, IndexError> {
    schema.fields.iter().try_fold(
        std::mem::size_of::<SelectedScalarFields>(),
        |bytes, field| {
            let Some(selected) = shared.get(&field.source_selector) else {
                return Ok(bytes);
            };
            let values = selected
                .values
                .len()
                .checked_mul(std::mem::size_of::<ScalarValue>())
                .ok_or(IndexError::OffsetOverflow)?;
            let strings = selected.values.iter().try_fold(0usize, |total, value| {
                total
                    .checked_add(match value {
                        ScalarValue::String(value) => value.len(),
                        _ => 0,
                    })
                    .ok_or(IndexError::OffsetOverflow)
            })?;
            bytes
                .checked_add(std::mem::size_of::<(String, SelectedScalarField)>())
                .and_then(|total| total.checked_add(3 * std::mem::size_of::<usize>()))
                .and_then(|total| total.checked_add(field.name.len()))
                .and_then(|total| total.checked_add(values))
                .and_then(|total| total.checked_add(strings))
                .ok_or(IndexError::OffsetOverflow)
        },
    )
}

pub(crate) fn projected_mutation_resident_bytes(
    mutation: &MergeMutation,
) -> Result<usize, IndexError> {
    match mutation {
        MergeMutation::Upsert(source) => source.resident_bytes(),
        MergeMutation::Delete(identity) => std::mem::size_of::<MergeMutation>()
            .checked_add(identity.path.capacity())
            .ok_or(IndexError::OffsetOverflow),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use keldra_api::v1::index_specification::Specification;
    use keldra_api::v1::{
        IndexField, IndexFieldCapability, IndexFieldCardinality, IndexOrder, IndexOrderDirection,
        IndexSpecification, KeywordIndexField, SignedIntegerIndexField, TypedJsonIndexSpec,
        index_field,
    };

    use super::*;
    use crate::index_runtime::json_projection::project_scalar_pointers;
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

    #[test]
    fn shared_scalar_union_is_byte_exact_with_independent_projection() {
        let first = schema(Specification::TypedJson(TypedJsonIndexSpec {
            fields: vec![
                ordered_signed_field("modified", "/modified"),
                keyword_field("tags", "/tags", true),
            ],
            physical_order: vec![IndexOrder {
                field: "modified".into(),
                direction: IndexOrderDirection::Descending as i32,
            }],
        }));
        let second = schema(Specification::TypedJson(TypedJsonIndexSpec {
            fields: vec![keyword_field("status", "/status", false)],
            physical_order: Vec::new(),
        }));
        let body =
            br#"{"modified":9,"tags":["rust",null],"status":"open","ignored":{"large":"body"}}"#;
        let pointers = vec!["/modified".into(), "/status".into(), "/tags".into()];
        let mut union_input = Cursor::new(body);
        let shared = project_scalar_pointers(&mut union_input, &pointers, LIMIT)
            .unwrap()
            .unwrap();

        for schema in [&first, &second] {
            let source =
                IndexSourceMutation::Upsert(object("records/source.json", body.len() as u64));
            let mut independent_input = Cursor::new(body);
            let independent =
                project_mutation(schema, source.clone(), Some(&mut independent_input), LIMIT)
                    .unwrap();
            let assembled =
                project_typed_json_from_shared_scalars(schema, source, Some(&shared), LIMIT)
                    .unwrap();
            assert_eq!(assembled, independent);
        }
    }

    #[test]
    fn one_large_source_mapping_feeds_sixty_four_definition_assemblers() {
        let schema = schema(Specification::TypedJson(TypedJsonIndexSpec {
            fields: vec![keyword_field("status", "/status", false)],
            physical_order: Vec::new(),
        }));
        let mut body = br#"{"unselected":""#.to_vec();
        body.extend(std::iter::repeat_n(b'x', 72 * 1024));
        body.extend_from_slice(br#"","status":"open"}"#);
        let mut input = Cursor::new(&body);
        let shared = project_scalar_pointers(&mut input, &["/status".into()], LIMIT)
            .unwrap()
            .unwrap();
        let source =
            IndexSourceMutation::Upsert(object("records/pathological.json", body.len() as u64));

        let first =
            project_typed_json_from_shared_scalars(&schema, source.clone(), Some(&shared), LIMIT)
                .unwrap();
        for _ in 1..64 {
            assert_eq!(
                project_typed_json_from_shared_scalars(
                    &schema,
                    source.clone(),
                    Some(&shared),
                    LIMIT,
                )
                .unwrap(),
                first
            );
        }
    }
}
