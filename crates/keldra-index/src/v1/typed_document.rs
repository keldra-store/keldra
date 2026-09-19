//! Neutral one-record Typed JSON preparation for the v1 projection pipeline.
//!
//! JSON selection remains a server concern. This module accepts selected
//! scalars and emits complete replacement material. Old documents are excluded
//! by generation-bound liveness, never by reconstructing old field values.

use std::mem::size_of;

use crate::IndexError;
use crate::typed_json::{FieldSchema, ScalarValue, TypedJsonFieldState};

use super::{
    CanonicalRecipeState, DocumentHead, ObjectIdentity, PreparedQueryMembershipDelta,
    PreparedQueryMutationBatch, PreparedQueryRecipeDelta, ProjectedDocumentState,
    QueryBlockCredits, QueryDocumentGate, RecipeIdentity, prepare_typed_json_field_delta,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedJsonSelectedField {
    pub recipe: RecipeIdentity,
    pub field: FieldSchema,
    pub selected: Option<Vec<ScalarValue>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedJsonDocumentInput {
    pub source_scope: [u8; 32],
    pub source_path: String,
    /// Canonical authorization identity when `source_path` names an alias.
    pub canonical_source_path: Option<String>,
    pub source_version: u64,
    pub result: Option<ObjectIdentity>,
    pub live: bool,
    pub membership_recipe: RecipeIdentity,
    /// Strict physical-recipe order. The current Typed JSON projection is one
    /// stable record (`source_record == 0`), not an expanded record set.
    pub fields: Vec<TypedJsonSelectedField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedTypedJsonDocument {
    /// One live stable record, or an empty set for a deletion.
    pub current: Vec<ProjectedDocumentState>,
    pub query: PreparedQueryMutationBatch,
}

/// Prepare one source object's currently supported single Typed JSON record.
/// Preparation has no dependency on the preceding document's field state.
pub fn prepare_typed_json_document(
    input: TypedJsonDocumentInput,
    credits: &mut QueryBlockCredits,
) -> Result<PreparedTypedJsonDocument, IndexError> {
    validate_input(&input)?;
    credits.reserve(preparation_bound(&input)?)?;

    if !input.live {
        let document = StableDocument::from_input(&input)?;
        let mut fields = Vec::with_capacity(input.fields.len());
        for selected in &input.fields {
            let delta = prepare_typed_json_field_delta(
                &selected.field,
                document.key,
                input.source_version,
                None,
                credits,
            )?;
            fields.push(PreparedQueryRecipeDelta {
                recipe: selected.recipe,
                delta,
            });
        }
        let membership = Some(PreparedQueryMembershipDelta {
            recipe: input.membership_recipe,
            gates: vec![QueryDocumentGate {
                document: document.key,
                material_source_version: input.source_version,
                current_source_version: input.source_version,
                live: false,
                selective_source_position: None,
                source_path: Some(input.source_path.clone()),
                canonical_source_path: input.canonical_source_path.clone(),
                result_path: Some(input.source_path.clone()),
                result_version: input.source_version,
            }],
        });
        return Ok(PreparedTypedJsonDocument {
            current: Vec::new(),
            query: PreparedQueryMutationBatch { membership, fields },
        });
    }

    let head = DocumentHead::new(
        input.source_scope,
        input.source_path.clone(),
        0,
        input.source_version,
        input.result.clone(),
        true,
    )?;
    let current = vec![ProjectedDocumentState::new(
        input.source_scope,
        head,
        vec![CanonicalRecipeState::new(input.membership_recipe, vec![1])?],
        // Field-state streams existed solely for predecessor subtraction.
        // Complete native segments now retain postings/points/value columns;
        // writing a second serialized copy of every field has no reader.
        Vec::new(),
    )?];
    let stable_key = current[0].head.stable_key;
    let material_source_version = current[0].head.material_source_version;

    let mut query_fields = Vec::with_capacity(input.fields.len());
    for selected in input.fields {
        // Move the selected values into one transient field at a time. The
        // emitted column/postings own their material; no second all-fields
        // scratch vector or cloned selection remains resident.
        let current_field = TypedJsonFieldState::from_selected(&selected.field, selected.selected)?;
        // Emit only complete new material. The preceding document's values
        // need no inverse mutations because its version is no longer live.
        let delta = prepare_typed_json_field_delta(
            &selected.field,
            stable_key,
            material_source_version,
            Some(&current_field),
            credits,
        )?;
        query_fields.push(PreparedQueryRecipeDelta {
            recipe: selected.recipe,
            delta,
        });
    }
    let result = current[0].head.result_or_source();
    Ok(PreparedTypedJsonDocument {
        current,
        query: PreparedQueryMutationBatch {
            membership: Some(PreparedQueryMembershipDelta {
                recipe: input.membership_recipe,
                gates: vec![QueryDocumentGate {
                    document: stable_key,
                    material_source_version,
                    current_source_version: input.source_version,
                    live: true,
                    selective_source_position: None,
                    source_path: Some(input.source_path.clone()),
                    canonical_source_path: input.canonical_source_path.clone(),
                    result_path: Some(result.path),
                    result_version: result.version,
                }],
            }),
            fields: query_fields,
        },
    })
}

struct StableDocument {
    key: super::StableDocumentKey,
}

impl StableDocument {
    fn from_input(input: &TypedJsonDocumentInput) -> Result<Self, IndexError> {
        Ok(Self {
            key: super::StableDocumentKey::derive(input.source_scope, &input.source_path, 0)?,
        })
    }
}

fn validate_input(input: &TypedJsonDocumentInput) -> Result<(), IndexError> {
    if input.source_scope == [0; 32]
        || input.source_path.is_empty()
        || input.source_path.contains('\0')
        || input.canonical_source_path.as_ref().is_some_and(|path| {
            path.is_empty() || path.contains('\0') || path == &input.source_path
        })
        || input.source_version == 0
        || !input.live && input.result.is_some()
        || !input.live && input.fields.iter().any(|field| field.selected.is_some())
        || input
            .fields
            .windows(2)
            .any(|pair| pair[0].recipe >= pair[1].recipe)
    {
        return Err(IndexError::InvalidDefinition(
            "Typed JSON document preparation input is invalid".into(),
        ));
    }
    for selected in &input.fields {
        selected.field.validate()?;
    }
    Ok(())
}

fn preparation_bound(input: &TypedJsonDocumentInput) -> Result<usize, IndexError> {
    let mut bytes = size_of::<PreparedTypedJsonDocument>()
        .checked_add(input.source_path.len().saturating_mul(2))
        .and_then(|bytes| {
            bytes.checked_add(
                input
                    .canonical_source_path
                    .as_ref()
                    .map_or(0, |path| path.len().saturating_mul(2)),
            )
        })
        .and_then(|bytes| {
            bytes.checked_add(
                input
                    .result
                    .as_ref()
                    .map_or(0, |result| result.path.len().saturating_mul(2)),
            )
        })
        .and_then(|bytes| bytes.checked_add(input.fields.len().saturating_mul(768)))
        .ok_or(IndexError::OffsetOverflow)?;
    for selected in &input.fields {
        for value in selected.selected.iter().flatten() {
            bytes = bytes
                .checked_add(match value {
                    ScalarValue::String(value) => value.len().saturating_mul(8),
                    _ => 64,
                })
                .ok_or(IndexError::OffsetOverflow)?;
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typed_json::{Cardinality, Collation, FieldCapabilities, FieldId, FieldType};
    use crate::v1::{IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage};

    fn recipe(byte: u8) -> RecipeIdentity {
        RecipeIdentity::new([byte; 32]).unwrap()
    }

    fn field() -> FieldSchema {
        FieldSchema {
            id: FieldId::new(1),
            name: "labels".into(),
            source_selector: "/labels".into(),
            field_type: FieldType::Keyword,
            cardinality: Cardinality::Multi,
            allow_missing: true,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::EXACT
                .union(FieldCapabilities::RANGE)
                .union(FieldCapabilities::FACET),
            analyzer: None,
            date_format: None,
        }
    }

    fn credits() -> QueryBlockCredits {
        limited_credits(4 * 1024 * 1024)
    }

    fn limited_credits(bytes: usize) -> QueryBlockCredits {
        let memory = IndexingMemoryCredits::new(
            bytes,
            IndexingMemoryLimits {
                hot_payload_bytes: bytes,
                worker_scratch_bytes: bytes,
                prepared_rows_bytes: bytes,
                replay_input_bytes: bytes,
                projection_accumulator_bytes: bytes,
                seal_scratch_bytes: bytes,
                ordering_catalog_bytes: bytes,
            },
        )
        .unwrap();
        QueryBlockCredits::from_pipeline_permit(
            memory
                .acquire(IndexingMemoryStage::OrderingCatalog, bytes)
                .unwrap(),
        )
    }

    fn input(version: u64, values: Option<Vec<&str>>, live: bool) -> TypedJsonDocumentInput {
        TypedJsonDocumentInput {
            source_scope: [5; 32],
            source_path: "objects/a.json".into(),
            canonical_source_path: None,
            source_version: version,
            result: live.then_some(ObjectIdentity {
                path: format!("results/{version}.json"),
                version,
            }),
            live,
            membership_recipe: recipe(1),
            fields: vec![TypedJsonSelectedField {
                recipe: recipe(2),
                field: field(),
                selected: values.map(|values| {
                    values
                        .into_iter()
                        .map(|value| ScalarValue::String(value.into()))
                        .collect()
                }),
            }],
        }
    }

    #[test]
    fn create_emits_live_native_material_without_duplicate_field_state() {
        let mut memory = credits();
        let prepared =
            prepare_typed_json_document(input(1, Some(vec!["alpha", "beta"]), true), &mut memory)
                .unwrap();
        assert_eq!(prepared.current.len(), 1);
        let state = &prepared.current[0];
        assert_eq!(state.head.source_record, 0);
        assert_eq!(state.head.material_source_version, 1);
        assert_eq!(state.memberships[0].recipe, recipe(1));
        assert!(state.fields.is_empty());
        assert!(prepared.query.membership.as_ref().unwrap().gates[0].live);
        assert_eq!(
            prepared.query.fields[0]
                .delta
                .doc_value
                .as_ref()
                .unwrap()
                .value,
            Some(vec![
                ScalarValue::String("alpha".into()),
                ScalarValue::String("beta".into())
            ])
        );
        assert_eq!(prepared.query.fields[0].delta.terms.len(), 2);
        assert!(
            prepared.query.fields[0]
                .delta
                .terms
                .iter()
                .all(|term| term.live)
        );
    }

    #[test]
    fn update_and_shrink_emit_only_complete_new_material() {
        let mut first_memory = credits();
        let first = prepare_typed_json_document(
            input(1, Some(vec!["alpha", "beta"]), true),
            &mut first_memory,
        )
        .unwrap();
        assert_eq!(first.query.fields[0].delta.terms.len(), 2);
        let mut second_memory = credits();
        let second =
            prepare_typed_json_document(input(2, Some(vec!["beta"]), true), &mut second_memory)
                .unwrap();
        assert_eq!(second.current[0].head.material_source_version, 2);
        let terms = &second.query.fields[0].delta.terms;
        assert_eq!(terms.len(), 1);
        assert!(
            second.query.fields[0]
                .delta
                .points
                .iter()
                .all(|point| point.live && point.material_source_version == 2)
        );
        assert!(terms.iter().all(|term| term.live));
        assert!(
            !terms
                .iter()
                .any(|term| term.term == ScalarValue::String("alpha".into()))
        );
        assert!(
            !terms
                .iter()
                .any(|term| { term.term == ScalarValue::String("beta".into()) && !term.live })
        );
        assert!(
            terms
                .iter()
                .any(|term| { term.term == ScalarValue::String("beta".into()) && term.live })
        );
        assert_eq!(
            second.query.membership.as_ref().unwrap().gates[0].material_source_version,
            2
        );
    }

    #[test]
    fn unchanged_fields_still_emit_new_exact_material_version() {
        let mut first_memory = credits();
        let first =
            prepare_typed_json_document(input(1, Some(vec!["stable"]), true), &mut first_memory)
                .unwrap();
        assert_eq!(first.current[0].head.material_source_version, 1);
        let mut second_memory = credits();
        let second =
            prepare_typed_json_document(input(2, Some(vec!["stable"]), true), &mut second_memory)
                .unwrap();
        assert_eq!(second.current[0].head.source_version, 2);
        assert_eq!(second.current[0].head.material_source_version, 2);
        let gate = &second.query.membership.as_ref().unwrap().gates[0];
        assert_eq!(gate.material_source_version, 2);
        assert_eq!(gate.current_source_version, 2);
        assert_eq!(gate.result_version, 2);
        assert_eq!(second.query.fields.len(), 1);
        assert_eq!(
            second.query.fields[0].delta.terms[0].material_source_version,
            2
        );
    }

    #[test]
    fn coalesced_window_emits_final_version_without_previous_material() {
        let mut durable_memory = credits();
        let durable =
            prepare_typed_json_document(input(1, Some(vec!["alpha"]), true), &mut durable_memory)
                .unwrap();
        assert_eq!(durable.current[0].head.material_source_version, 1);

        // Coalescing publishes only the final exact version, even when its
        // values happen to equal an earlier durable document.
        let mut intermediate_memory = credits();
        let intermediate = prepare_typed_json_document(
            input(2, Some(vec!["beta"]), true),
            &mut intermediate_memory,
        )
        .unwrap();
        assert_eq!(intermediate.current[0].head.material_source_version, 2);

        let mut final_memory = credits();
        let final_document =
            prepare_typed_json_document(input(3, Some(vec!["alpha"]), true), &mut final_memory)
                .unwrap();
        assert_eq!(final_document.current[0].head.source_version, 3);
        assert_eq!(final_document.current[0].head.material_source_version, 3);
        assert_eq!(final_document.query.fields.len(), 1);
        let gate = &final_document.query.membership.unwrap().gates[0];
        assert_eq!(gate.material_source_version, 3);
        assert_eq!(gate.current_source_version, 3);
    }

    #[test]
    fn delete_emits_authoritative_dead_gates_without_old_value_tombstones() {
        let mut first_memory = credits();
        let first = prepare_typed_json_document(
            input(1, Some(vec!["alpha", "beta"]), true),
            &mut first_memory,
        )
        .unwrap();
        let stable_key = first.current[0].head.stable_key;
        let mut delete_memory = credits();
        let deleted =
            prepare_typed_json_document(input(2, None, false), &mut delete_memory).unwrap();
        assert!(deleted.current.is_empty());
        let gate = &deleted.query.membership.as_ref().unwrap().gates[0];
        assert_eq!(gate.document, stable_key);
        assert!(!gate.live);
        assert_eq!(gate.material_source_version, 2);
        let field = &deleted.query.fields[0].delta;
        assert!(!field.presence.live);
        assert!(field.terms.is_empty());
        assert!(field.points.is_empty());
        assert_eq!(field.doc_value.as_ref().unwrap().value, None);
    }

    #[test]
    fn delete_without_previous_state_still_invalidates_every_old_version() {
        let mut memory = credits();
        let deleted = prepare_typed_json_document(input(7, None, false), &mut memory).unwrap();
        let gate = &deleted.query.membership.as_ref().unwrap().gates[0];
        assert!(!gate.live);
        assert_eq!(gate.material_source_version, 7);
        assert_eq!(gate.current_source_version, 7);
        assert_eq!(deleted.query.fields.len(), 1);
        assert!(!deleted.query.fields[0].delta.presence.live);
        assert_eq!(
            deleted.query.fields[0]
                .delta
                .presence
                .material_source_version,
            7
        );
    }

    #[test]
    fn alias_live_and_delete_gates_preserve_canonical_authorization_identity() {
        let mut live_input = input(1, Some(vec!["alpha"]), true);
        live_input.source_path = "aliases/reserved.json".into();
        live_input.canonical_source_path = Some("objects/target.json".into());
        let mut live_memory = credits();
        let live = prepare_typed_json_document(live_input, &mut live_memory).unwrap();
        let live_gate = &live.query.membership.as_ref().unwrap().gates[0];
        assert_eq!(
            live_gate.source_path.as_deref(),
            Some("aliases/reserved.json")
        );
        assert_eq!(
            live_gate.canonical_source_path.as_deref(),
            Some("objects/target.json")
        );

        let mut delete_input = input(2, None, false);
        delete_input.source_path = "aliases/reserved.json".into();
        delete_input.canonical_source_path = Some("objects/target.json".into());
        let mut delete_memory = credits();
        let deleted = prepare_typed_json_document(delete_input, &mut delete_memory).unwrap();
        let delete_gate = &deleted.query.membership.as_ref().unwrap().gates[0];
        assert!(!delete_gate.live);
        assert_eq!(
            delete_gate.canonical_source_path.as_deref(),
            Some("objects/target.json")
        );
        assert_eq!(
            delete_gate.source_path.as_deref(),
            Some("aliases/reserved.json")
        );
    }

    #[test]
    fn preparation_refuses_before_uncredited_state_is_built() {
        let mut memory = limited_credits(1);
        assert!(matches!(
            prepare_typed_json_document(input(1, Some(vec!["alpha", "beta"]), true), &mut memory),
            Err(IndexError::ResourceLimit { .. })
        ));
        assert_eq!(memory.remaining(), 1);
    }
}
