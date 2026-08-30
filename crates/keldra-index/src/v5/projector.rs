use crate::IndexError;
use crate::v4::build::{ProjectedRecord, ProjectedSource};
use crate::v4::{FieldId, ScalarValue, Schema};

use super::{CanonicalRecipeState, DocumentHead, ProjectedDocumentState, RecipeIdentity};

const CANONICAL_FIELD_STATE_VERSION: u16 = 1;

/// Convert the deterministic native projection into exact recipe-local state.
///
/// Source and result versions live only in the document head. Field bytes are
/// definition-neutral and therefore remain equal across a version-only update.
pub fn projected_document_states(
    schema: &Schema,
    source: &ProjectedSource,
) -> Result<Vec<ProjectedDocumentState>, IndexError> {
    schema.validate()?;
    source.validate()?;
    let recipes = schema.recipe_fingerprints()?;
    let membership = RecipeIdentity::new(recipes.membership)?;
    let field_recipes = recipes
        .fields
        .into_iter()
        .map(RecipeIdentity::new)
        .collect::<Result<Vec<_>, _>>()?;
    if field_recipes.len() != schema.fields.len() {
        return Err(IndexError::InvalidDefinition(
            "schema field recipe catalogue is incomplete".into(),
        ));
    }
    let mut states = Vec::with_capacity(source.records.len());
    for (source_record, record) in source.records.iter().enumerate() {
        let source_record = u32::try_from(source_record).map_err(|_| IndexError::OffsetOverflow)?;
        let head = DocumentHead::new(
            recipes.membership,
            source.source_identity.path.clone(),
            source_record,
            source.source_identity.version,
            record.result_identity.clone(),
            true,
        )?;
        let memberships = vec![CanonicalRecipeState::new(membership, vec![1])?];
        let mut fields = schema
            .fields
            .iter()
            .zip(&field_recipes)
            .map(|(field, recipe)| {
                Ok(CanonicalRecipeState::new(
                    *recipe,
                    encode_field_state(record, field.id)?,
                )?)
            })
            .collect::<Result<Vec<_>, IndexError>>()?;
        fields.sort_by_key(|state| state.recipe);
        states.push(ProjectedDocumentState::new(
            recipes.membership,
            head,
            memberships,
            fields,
        )?);
    }
    Ok(states)
}

fn encode_field_state(record: &ProjectedRecord, field_id: FieldId) -> Result<Vec<u8>, IndexError> {
    let mut out = Vec::new();
    put_u16(&mut out, CANONICAL_FIELD_STATE_VERSION);

    let mut terms = record
        .terms
        .iter()
        .filter(|term| term.field_id == field_id)
        .collect::<Vec<_>>();
    terms.sort_by(|left, right| {
        (
            left.term_type,
            left.term.as_slice(),
            left.frequency,
            left.positions.as_slice(),
        )
            .cmp(&(
                right.term_type,
                right.term.as_slice(),
                right.frequency,
                right.positions.as_slice(),
            ))
    });
    put_len(&mut out, terms.len())?;
    for term in terms {
        out.push(term.term_type);
        put_bytes(&mut out, &term.term)?;
        put_u32(&mut out, term.frequency);
        put_len(&mut out, term.positions.len())?;
        for position in &term.positions {
            put_u32(&mut out, *position);
        }
    }

    let points = record
        .points
        .iter()
        .filter(|point| point.field_id == field_id)
        .collect::<Vec<_>>();
    put_len(&mut out, points.len())?;
    for point in points {
        out.push(u8::from(point.present));
        out.push(u8::from(point.null));
        put_len(&mut out, point.values.len())?;
        for value in &point.values {
            put_scalar(&mut out, value)?;
        }
    }

    let columns = record
        .doc_values
        .iter()
        .filter(|column| column.field_id == field_id)
        .collect::<Vec<_>>();
    put_len(&mut out, columns.len())?;
    for column in columns {
        out.push(u8::from(column.multi_valued));
        out.push(u8::from(column.cell.present));
        out.push(u8::from(column.cell.null));
        put_len(&mut out, column.cell.values.len())?;
        for value in &column.cell.values {
            put_scalar(&mut out, value)?;
        }
    }

    let vectors = record
        .vectors
        .iter()
        .filter(|vector| vector.field_id == field_id)
        .collect::<Vec<_>>();
    put_len(&mut out, vectors.len())?;
    for vector in vectors {
        put_len(&mut out, vector.values.len())?;
        for value in &vector.values {
            put_u32(&mut out, value.to_bits());
        }
    }

    let lengths = record
        .field_lengths
        .iter()
        .filter(|(field, _)| *field == field_id)
        .collect::<Vec<_>>();
    put_len(&mut out, lengths.len())?;
    for (_, length) in lengths {
        put_u32(&mut out, *length);
    }
    Ok(out)
}

fn put_scalar(out: &mut Vec<u8>, value: &ScalarValue) -> Result<(), IndexError> {
    match value {
        ScalarValue::Null => out.push(0),
        ScalarValue::Boolean(value) => {
            out.push(1);
            out.push(u8::from(*value));
        }
        ScalarValue::Signed(value) => {
            out.push(2);
            out.extend_from_slice(&value.to_le_bytes());
        }
        ScalarValue::Unsigned(value) => {
            out.push(3);
            out.extend_from_slice(&value.to_le_bytes());
        }
        ScalarValue::Number(value) => {
            out.push(4);
            out.extend_from_slice(&value.to_le_bytes());
        }
        ScalarValue::String(value) => {
            out.push(5);
            put_bytes(out, value.as_bytes())?;
        }
    }
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), IndexError> {
    put_len(out, value.len())?;
    out.extend_from_slice(value);
    Ok(())
}

fn put_len(out: &mut Vec<u8>, value: usize) -> Result<(), IndexError> {
    put_u32(
        out,
        u32::try_from(value).map_err(|_| IndexError::OffsetOverflow)?,
    );
    Ok(())
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use crate::v4::build::{ProjectedDocValue, ProjectedRecord};
    use crate::v4::{
        Cardinality, Collation, ComponentKind, ComponentVersion, DocValueCell, FieldCapabilities,
        FieldComponents, FieldSchema, FieldType, IndexKind, IndexSemantics, ObjectIdentity,
        ScalarValue,
    };

    use super::*;

    fn schema() -> Schema {
        let mut field = FieldSchema {
            id: FieldId::new(0),
            name: "state".into(),
            source_selector: "/state".into(),
            field_type: FieldType::Keyword,
            cardinality: Cardinality::Single,
            allow_missing: false,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::FACET,
            analyzer: None,
            date_format: None,
            components: FieldComponents::DOC_VALUES,
        };
        field.components = field.compiled_components().unwrap();
        Schema {
            kind: IndexKind::TypedJson,
            path_prefix: "objects/".into(),
            content_type_scope: Some("application/json".into()),
            fields: vec![field],
            semantics: IndexSemantics::TypedJson,
            physical_order: Vec::new(),
            component_versions: [
                ComponentKind::SEGMENT_ROOT,
                ComponentKind::ROUTING_NODE,
                ComponentKind::IDENTITY_TABLE,
                ComponentKind::LIVE_MASK,
                ComponentKind::PATH_LOCATOR,
                ComponentKind::TERM_DICTIONARY,
                ComponentKind::POSTINGS,
                ComponentKind::POINTS,
                ComponentKind::DOC_VALUES,
                ComponentKind::POSITIONS,
                ComponentKind::NORMS,
                ComponentKind::VECTORS,
                ComponentKind::SCORING_STATISTICS,
            ]
            .into_iter()
            .map(|component_kind| ComponentVersion {
                component_kind,
                codec_version: 1,
            })
            .collect(),
        }
        .canonicalize_physical_fields()
        .unwrap()
    }

    fn source(version: u64, value: &str) -> ProjectedSource {
        ProjectedSource {
            source_identity: ObjectIdentity {
                path: "objects/a".into(),
                version,
            },
            records: vec![ProjectedRecord {
                result_identity: None,
                order_key: Vec::new(),
                terms: Vec::new(),
                points: Vec::new(),
                doc_values: vec![ProjectedDocValue {
                    field_id: FieldId::new(0),
                    multi_valued: false,
                    cell: DocValueCell::value(ScalarValue::String(value.into())),
                }],
                vectors: Vec::new(),
                field_lengths: Vec::new(),
            }],
        }
    }

    #[test]
    fn native_projection_versions_change_only_the_document_head() {
        let schema = schema();
        let old = projected_document_states(&schema, &source(7, "open")).unwrap();
        let new = projected_document_states(&schema, &source(8, "open")).unwrap();
        assert!(new[0].delta_from(Some(&old[0])).unwrap().is_head_only());
    }

    #[test]
    fn native_projection_field_change_is_recipe_local() {
        let schema = schema();
        let old = projected_document_states(&schema, &source(7, "open")).unwrap();
        let new = projected_document_states(&schema, &source(8, "fixed")).unwrap();
        let delta = new[0].delta_from(Some(&old[0])).unwrap();
        assert_eq!(delta.fields.len(), 1);
        assert_eq!(delta.fields[0].recipe, old[0].fields[0].recipe);
    }
}
