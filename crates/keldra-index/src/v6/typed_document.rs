//! Neutral one-record Typed JSON preparation for the v6 projection pipeline.
//!
//! JSON selection remains a server concern. This module accepts selected
//! scalars and the exact preceding v6 state, then produces canonical component
//! state and matching sparse query deltas without any legacy index types.

use std::mem::size_of;

use crate::IndexError;
use crate::typed_json::{
    FieldSchema, ScalarValue, TypedJsonFieldState, decode_typed_json_field_state,
    encode_typed_json_field_state,
};

use super::{
    CanonicalRecipeState, DocumentHead, ObjectIdentity, PreparedQueryFieldDelta,
    PreparedQueryMembershipDelta, PreparedQueryMutationBatch, PreparedQueryRecipeDelta,
    ProjectedDocumentState, QueryBlockCredits, QueryDocumentGate, RecipeIdentity,
    inherit_projection_preserving_versions, prepare_typed_json_field_delta,
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
/// `previous` is the exact state loaded through the preceding generation's
/// source-record locator and component roots.
pub fn prepare_typed_json_document(
    input: TypedJsonDocumentInput,
    previous: Vec<ProjectedDocumentState>,
    credits: &mut QueryBlockCredits,
) -> Result<PreparedTypedJsonDocument, IndexError> {
    validate_input(&input, &previous)?;
    credits.reserve(preparation_bound(&input, &previous)?)?;

    let previous_state = previous.first();
    let previous_fields = previous_state
        .map(|state| {
            state
                .fields
                .iter()
                .zip(&input.fields)
                .map(|(canonical, selected)| {
                    if canonical.recipe != selected.recipe {
                        return Err(IndexError::InvalidDefinition(
                            "Typed JSON preceding field recipe order changed".into(),
                        ));
                    }
                    decode_typed_json_field_state(&selected.field, &canonical.value)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    if !input.live {
        if previous_state.is_none() {
            return Ok(PreparedTypedJsonDocument {
                current: Vec::new(),
                query: PreparedQueryMutationBatch::default(),
            });
        }
        let document = StableDocument::from_input(&input)?;
        let mut fields = Vec::with_capacity(input.fields.len());
        for (index, selected) in input.fields.iter().enumerate() {
            let previous = previous_fields.get(index);
            let delta = prepare_typed_json_field_delta(
                &selected.field,
                document.key,
                input.source_version,
                previous,
                None,
                credits,
            )?;
            fields.push(PreparedQueryRecipeDelta {
                recipe: selected.recipe,
                delta,
            });
        }
        let membership = previous_state.map(|_| PreparedQueryMembershipDelta {
            recipe: input.membership_recipe,
            gates: vec![QueryDocumentGate {
                document: document.key,
                material_source_version: input.source_version,
                current_source_version: input.source_version,
                live: false,
                source_path: Some(input.source_path.clone()),
                result_path: Some(previous_state.unwrap().head.result_or_source().path),
                result_version: previous_state.unwrap().head.result_or_source().version,
            }],
        });
        return Ok(PreparedTypedJsonDocument {
            current: Vec::new(),
            query: PreparedQueryMutationBatch { membership, fields },
        });
    }

    let mut typed_fields = Vec::with_capacity(input.fields.len());
    let mut canonical_fields = Vec::with_capacity(input.fields.len());
    for selected in &input.fields {
        let state = TypedJsonFieldState::from_selected(&selected.field, selected.selected.clone())?;
        let bytes = encode_typed_json_field_state(&selected.field, &state)?;
        typed_fields.push(state);
        canonical_fields.push(CanonicalRecipeState::new(selected.recipe, bytes)?);
    }
    let head = DocumentHead::new(
        input.source_scope,
        input.source_path.clone(),
        0,
        input.source_version,
        input.result.clone(),
        true,
    )?;
    let mut current = vec![ProjectedDocumentState::new(
        input.source_scope,
        head,
        vec![CanonicalRecipeState::new(input.membership_recipe, vec![1])?],
        canonical_fields,
    )?];
    inherit_projection_preserving_versions(&mut current, &previous)?;
    let stable_key = current[0].head.stable_key;
    let material_source_version = current[0].head.material_source_version;
    let material_changed = previous_state.is_none()
        || material_source_version != previous_state.unwrap().head.material_source_version;
    if !material_changed {
        let result = current[0].head.result_or_source();
        return Ok(PreparedTypedJsonDocument {
            current,
            query: PreparedQueryMutationBatch {
                membership: Some(PreparedQueryMembershipDelta {
                    recipe: input.membership_recipe,
                    gates: vec![QueryDocumentGate {
                        document: stable_key,
                        material_source_version,
                        current_source_version: input.source_version,
                        live: true,
                        source_path: Some(input.source_path.clone()),
                        result_path: Some(result.path),
                        result_version: result.version,
                    }],
                }),
                fields: Vec::new(),
            },
        });
    }

    let mut query_fields = Vec::with_capacity(input.fields.len());
    for (index, selected) in input.fields.iter().enumerate() {
        let previous = previous_fields.get(index);
        let current_field = &typed_fields[index];
        // Reassert every current value at the new document material version,
        // while retaining removals for values absent from the new state.
        let delta = prepare_material_change(
            &selected.field,
            stable_key,
            material_source_version,
            previous,
            current_field,
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
                    source_path: Some(input.source_path.clone()),
                    result_path: Some(result.path),
                    result_version: result.version,
                }],
            }),
            fields: query_fields,
        },
    })
}

fn prepare_material_change(
    field: &FieldSchema,
    document: super::StableDocumentKey,
    material_source_version: u64,
    previous: Option<&TypedJsonFieldState>,
    current: &TypedJsonFieldState,
    credits: &mut QueryBlockCredits,
) -> Result<PreparedQueryFieldDelta, IndexError> {
    let changed = prepare_typed_json_field_delta(
        field,
        document,
        material_source_version,
        previous,
        Some(current),
        credits,
    )?;
    let refresh = prepare_typed_json_field_delta(
        field,
        document,
        material_source_version,
        None,
        Some(current),
        credits,
    )?;
    let mut terms = changed
        .terms
        .into_iter()
        .filter(|term| !term.live)
        .collect::<Vec<_>>();
    terms.extend(refresh.terms);
    let mut points = changed
        .points
        .into_iter()
        .filter(|point| !point.live)
        .collect::<Vec<_>>();
    points.extend(refresh.points);
    Ok(PreparedQueryFieldDelta {
        presence: refresh.presence,
        doc_value: refresh.doc_value.or(changed.doc_value),
        terms,
        points,
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

fn validate_input(
    input: &TypedJsonDocumentInput,
    previous: &[ProjectedDocumentState],
) -> Result<(), IndexError> {
    if input.source_scope == [0; 32]
        || input.source_path.is_empty()
        || input.source_path.contains('\0')
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
    if previous.len() > 1 {
        return Err(IndexError::InvalidDefinition(
            "Typed JSON v6 supports one stable projected record".into(),
        ));
    }
    if let Some(previous) = previous.first() {
        previous.validate()?;
        if previous.source_scope != input.source_scope
            || previous.head.source_path != input.source_path
            || previous.head.source_record != 0
            || !previous.head.live
            || previous.head.source_version >= input.source_version
            || previous.memberships.len() != 1
            || previous.memberships[0].recipe != input.membership_recipe
            || previous.memberships[0].value.as_slice() != [1]
            || previous.fields.len() != input.fields.len()
            || previous
                .fields
                .iter()
                .zip(&input.fields)
                .any(|(old, new)| old.recipe != new.recipe)
        {
            return Err(IndexError::InvalidDefinition(
                "Typed JSON preceding state does not match this stable record/catalog".into(),
            ));
        }
    }
    Ok(())
}

fn preparation_bound(
    input: &TypedJsonDocumentInput,
    previous: &[ProjectedDocumentState],
) -> Result<usize, IndexError> {
    let mut bytes = size_of::<PreparedTypedJsonDocument>()
        .checked_add(input.source_path.len().saturating_mul(2))
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
    for state in previous {
        bytes = bytes
            .checked_add(state.resident_bytes()?.saturating_mul(2))
            .ok_or(IndexError::OffsetOverflow)?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typed_json::{Cardinality, Collation, FieldCapabilities, FieldId, FieldType};
    use crate::v6::{IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage};

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
    fn create_emits_canonical_state_membership_and_live_field_material() {
        let mut memory = credits();
        let prepared = prepare_typed_json_document(
            input(1, Some(vec!["alpha", "beta"]), true),
            Vec::new(),
            &mut memory,
        )
        .unwrap();
        assert_eq!(prepared.current.len(), 1);
        let state = &prepared.current[0];
        assert_eq!(state.head.source_record, 0);
        assert_eq!(state.head.material_source_version, 1);
        assert_eq!(state.memberships[0].recipe, recipe(1));
        let decoded = decode_typed_json_field_state(&field(), &state.fields[0].value).unwrap();
        assert_eq!(decoded.values.len(), 2);
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
    fn update_and_shrink_emit_old_removals_and_new_material_version() {
        let mut first_memory = credits();
        let first = prepare_typed_json_document(
            input(1, Some(vec!["alpha", "beta"]), true),
            Vec::new(),
            &mut first_memory,
        )
        .unwrap();
        let mut second_memory = credits();
        let second = prepare_typed_json_document(
            input(2, Some(vec!["beta"]), true),
            first.current,
            &mut second_memory,
        )
        .unwrap();
        assert_eq!(second.current[0].head.material_source_version, 2);
        let terms = &second.query.fields[0].delta.terms;
        assert!(
            terms
                .iter()
                .any(|term| { term.term == ScalarValue::String("alpha".into()) && !term.live })
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
    fn projection_preserving_update_advances_current_gate_without_reindexing_fields() {
        let mut first_memory = credits();
        let first = prepare_typed_json_document(
            input(1, Some(vec!["stable"]), true),
            Vec::new(),
            &mut first_memory,
        )
        .unwrap();
        let previous = first.current[0].clone();
        let mut second_memory = credits();
        let second = prepare_typed_json_document(
            input(2, Some(vec!["stable"]), true),
            first.current,
            &mut second_memory,
        )
        .unwrap();
        assert_eq!(second.current[0].head.source_version, 2);
        assert_eq!(second.current[0].head.material_source_version, 1);
        let gate = &second.query.membership.as_ref().unwrap().gates[0];
        assert_eq!(gate.material_source_version, 1);
        assert_eq!(gate.current_source_version, 2);
        assert_eq!(gate.result_version, 2);
        assert!(second.query.fields.is_empty());
        assert!(
            second.current[0]
                .delta_from(Some(&previous))
                .unwrap()
                .is_head_only()
        );
    }

    #[test]
    fn delete_emits_membership_presence_and_old_value_tombstones() {
        let mut first_memory = credits();
        let first = prepare_typed_json_document(
            input(1, Some(vec!["alpha", "beta"]), true),
            Vec::new(),
            &mut first_memory,
        )
        .unwrap();
        let stable_key = first.current[0].head.stable_key;
        let mut delete_memory = credits();
        let deleted =
            prepare_typed_json_document(input(2, None, false), first.current, &mut delete_memory)
                .unwrap();
        assert!(deleted.current.is_empty());
        let gate = &deleted.query.membership.as_ref().unwrap().gates[0];
        assert_eq!(gate.document, stable_key);
        assert!(!gate.live);
        assert_eq!(gate.material_source_version, 2);
        let field = &deleted.query.fields[0].delta;
        assert!(!field.presence.live);
        assert_eq!(field.terms.len(), 2);
        assert!(field.terms.iter().all(|term| !term.live));
        assert!(field.points.iter().all(|point| !point.live));
        assert_eq!(field.doc_value.as_ref().unwrap().value, None);
    }

    #[test]
    fn preparation_refuses_before_uncredited_state_is_built() {
        let mut memory = limited_credits(1);
        assert!(matches!(
            prepare_typed_json_document(
                input(1, Some(vec!["alpha", "beta"]), true),
                Vec::new(),
                &mut memory
            ),
            Err(IndexError::ResourceLimit { .. })
        ));
        assert_eq!(memory.remaining(), 1);
    }
}
