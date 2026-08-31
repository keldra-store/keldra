use crate::IndexError;
use crate::v4::build::{
    MergeMutation, ProjectedDocValue, ProjectedPoint, ProjectedRecord, ProjectedSource,
    ProjectedTerm, ProjectedVector,
};
use crate::v4::{DocValueCell, FieldId, ScalarValue, Schema};

use super::{CanonicalRecipeState, DocumentHead, ProjectedDocumentState, RecipeIdentity};

/// Merge independently compiled logical schemas over the same exact source
/// into one canonical physical family projection.
///
/// Public aliases and field subsets disappear at this boundary. Repeated
/// recipes must produce byte-identical canonical state; otherwise sharing
/// fails closed rather than selecting one definition's interpretation.
pub fn merge_projected_document_states(
    projected: Vec<Vec<ProjectedDocumentState>>,
) -> Result<Vec<ProjectedDocumentState>, IndexError> {
    let mut projected = projected.into_iter();
    let Some(mut merged) = projected.next() else {
        return Ok(Vec::new());
    };
    for states in projected {
        if states.len() != merged.len() {
            return Err(IndexError::InvalidDefinition(
                "logical schemas expanded one source into different record sets".into(),
            ));
        }
        for (target, incoming) in merged.iter_mut().zip(states) {
            target.validate()?;
            incoming.validate()?;
            if target.source_scope != incoming.source_scope || target.head != incoming.head {
                return Err(IndexError::InvalidDefinition(
                    "logical schemas do not share one exact document universe".into(),
                ));
            }
            merge_recipe_states(&mut target.memberships, incoming.memberships)?;
            merge_recipe_states(&mut target.fields, incoming.fields)?;
            target.validate()?;
        }
    }
    Ok(merged)
}

fn merge_recipe_states(
    target: &mut Vec<CanonicalRecipeState>,
    incoming: Vec<CanonicalRecipeState>,
) -> Result<(), IndexError> {
    let mut merged = Vec::with_capacity(target.len().saturating_add(incoming.len()));
    let (mut left, mut right) = (0, 0);
    while left < target.len() || right < incoming.len() {
        match (target.get(left), incoming.get(right)) {
            (Some(existing), Some(candidate)) if existing.recipe == candidate.recipe => {
                if existing != candidate {
                    return Err(IndexError::InvalidDefinition(
                        "one physical recipe produced conflicting canonical state".into(),
                    ));
                }
                merged.push(existing.clone());
                left += 1;
                right += 1;
            }
            (Some(existing), Some(candidate)) if existing.recipe < candidate.recipe => {
                merged.push(existing.clone());
                left += 1;
            }
            (_, Some(candidate)) => {
                merged.push(candidate.clone());
                right += 1;
            }
            (Some(existing), None) => {
                merged.push(existing.clone());
                left += 1;
            }
            (None, None) => break,
        }
    }
    *target = merged;
    Ok(())
}

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
        let mut head = DocumentHead::new(
            recipes.membership,
            source.source_identity.path.clone(),
            source_record,
            source.source_identity.version,
            record.result_identity.clone(),
            true,
        )?;
        head.order_key = record.order_key.clone();
        head.validate(recipes.membership)?;
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

/// Reconstruct one native query-cache field from its canonical format-v5
/// recipe state.
///
/// The durable projection remains field-local and definition-neutral. The
/// returned record deliberately has no result identity or physical order: the
/// assembler obtains document identity from `DocumentHead` and combines the
/// separately bound field recipes selected by the logical definition.
pub fn decode_canonical_field_state(
    field_id: FieldId,
    bytes: &[u8],
) -> Result<ProjectedRecord, IndexError> {
    let mut decoder = FieldStateDecoder::new(bytes);
    if decoder.u16()? != CANONICAL_FIELD_STATE_VERSION {
        return Err(IndexError::InvalidFormat(
            "unsupported canonical field-state version",
        ));
    }

    let term_count = decoder.count(14)?;
    let mut terms = Vec::with_capacity(term_count);
    for _ in 0..term_count {
        let term_type = decoder.u8()?;
        let term = decoder.bytes()?.to_vec();
        let frequency = decoder.u32()?;
        let position_count = decoder.count(4)?;
        let mut positions = Vec::with_capacity(position_count);
        for _ in 0..position_count {
            positions.push(decoder.u32()?);
        }
        terms.push(ProjectedTerm {
            field_id,
            term_type,
            term,
            frequency,
            positions,
        });
    }

    let point_count = decoder.count(7)?;
    let mut points = Vec::with_capacity(point_count);
    for _ in 0..point_count {
        let present = decoder.boolean()?;
        let null = decoder.boolean()?;
        let value_count = decoder.count(1)?;
        let mut values = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            values.push(decoder.scalar()?);
        }
        points.push(ProjectedPoint {
            field_id,
            present,
            null,
            values,
        });
    }

    let doc_value_count = decoder.count(8)?;
    let mut doc_values = Vec::with_capacity(doc_value_count);
    for _ in 0..doc_value_count {
        let multi_valued = decoder.boolean()?;
        let present = decoder.boolean()?;
        let null = decoder.boolean()?;
        let value_count = decoder.count(1)?;
        let mut values = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            values.push(decoder.scalar()?);
        }
        doc_values.push(ProjectedDocValue {
            field_id,
            multi_valued,
            cell: DocValueCell {
                present,
                null,
                values,
            },
        });
    }

    let vector_count = decoder.count(8)?;
    let mut vectors = Vec::with_capacity(vector_count);
    for _ in 0..vector_count {
        let value_count = decoder.count(4)?;
        let mut values = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            let value = f32::from_bits(decoder.u32()?);
            if !value.is_finite() {
                return Err(IndexError::Integrity);
            }
            values.push(value);
        }
        vectors.push(ProjectedVector { field_id, values });
    }

    let field_length_count = decoder.count(4)?;
    let mut field_lengths = Vec::with_capacity(field_length_count);
    for _ in 0..field_length_count {
        field_lengths.push((field_id, decoder.u32()?));
    }
    decoder.finish()?;

    let record = ProjectedRecord {
        result_identity: None,
        order_key: Vec::new(),
        terms,
        points,
        doc_values,
        vectors,
        field_lengths,
    };
    record.validate()?;
    Ok(record)
}

/// Assemble only records whose query-visible material changed into disposable
/// native cache mutations. Projection-preserving source versions therefore do
/// not enter a segment writer, while removals invalidate the exact stable key.
pub fn query_cache_mutations(
    schema: &Schema,
    source_version: u64,
    current: &[ProjectedDocumentState],
    previous: &[ProjectedDocumentState],
) -> Result<Vec<MergeMutation>, IndexError> {
    schema.validate()?;
    if source_version == 0 {
        return Err(IndexError::InvalidDefinition(
            "query-cache source version must be non-zero".into(),
        ));
    }
    let recipes = schema.recipe_fingerprints()?.fields;
    if recipes.len() != schema.fields.len() {
        return Err(IndexError::InvalidDefinition(
            "query-cache field recipe catalogue is incomplete".into(),
        ));
    }
    let mut previous_by_key = previous
        .iter()
        .map(|state| {
            state.validate()?;
            Ok((state.head.stable_key, state))
        })
        .collect::<Result<std::collections::BTreeMap<_, _>, IndexError>>()?;
    if previous_by_key.len() != previous.len() {
        return Err(IndexError::InvalidDefinition(
            "query-cache predecessor contains duplicate stable keys".into(),
        ));
    }
    let mut mutations = Vec::new();
    for state in current {
        state.validate()?;
        let predecessor = previous_by_key.remove(&state.head.stable_key);
        if predecessor.is_some_and(|previous| {
            previous.head.material_source_version == state.head.material_source_version
        }) {
            continue;
        }
        let mut record = ProjectedRecord {
            result_identity: Some(state.head.result_or_source()),
            order_key: state.head.order_key.clone(),
            terms: Vec::new(),
            points: Vec::new(),
            doc_values: Vec::new(),
            vectors: Vec::new(),
            field_lengths: Vec::new(),
        };
        for (field, recipe) in schema.fields.iter().zip(&recipes) {
            let recipe = RecipeIdentity::new(*recipe)?;
            let canonical = state
                .fields
                .binary_search_by_key(&recipe, |field| field.recipe)
                .ok()
                .and_then(|position| state.fields.get(position))
                .ok_or_else(|| {
                    IndexError::InvalidDefinition(
                        "query-cache state omits a physical field recipe".into(),
                    )
                })?;
            let decoded = decode_canonical_field_state(field.id, &canonical.value)?;
            record.terms.extend(decoded.terms);
            record.points.extend(decoded.points);
            record.doc_values.extend(decoded.doc_values);
            record.vectors.extend(decoded.vectors);
            record.field_lengths.extend(decoded.field_lengths);
        }
        record.points.sort_by_key(|point| point.field_id);
        record.doc_values.sort_by_key(|column| column.field_id);
        record.vectors.sort_by_key(|vector| vector.field_id);
        record.field_lengths.sort_by_key(|(field, _)| *field);
        record.validate()?;
        let source_identity = state
            .head
            .stable_key
            .query_cache_identity(state.head.material_source_version)?;
        mutations.push(MergeMutation::Upsert(ProjectedSource {
            source_identity,
            records: vec![record],
        }));
    }
    for previous in previous_by_key.into_values() {
        mutations.push(MergeMutation::Delete(
            previous
                .head
                .stable_key
                .query_cache_identity(source_version)?,
        ));
    }
    mutations.sort_by(|left, right| cache_mutation_path(left).cmp(cache_mutation_path(right)));
    Ok(mutations)
}

fn cache_mutation_path(mutation: &MergeMutation) -> &str {
    match mutation {
        MergeMutation::Upsert(source) => &source.source_identity.path,
        MergeMutation::Delete(identity) => &identity.path,
    }
}

struct FieldStateDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> FieldStateDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], IndexError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(IndexError::Integrity)?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, IndexError> {
        Ok(self.take(1)?[0])
    }

    fn boolean(&mut self) -> Result<bool, IndexError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(IndexError::Integrity),
        }
    }

    fn u16(&mut self) -> Result<u16, IndexError> {
        Ok(u16::from_le_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, IndexError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, IndexError> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        ))
    }

    fn count(&mut self, minimum_item_bytes: usize) -> Result<usize, IndexError> {
        let count = usize::try_from(self.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        if minimum_item_bytes == 0 || count > self.remaining() / minimum_item_bytes {
            return Err(IndexError::Integrity);
        }
        Ok(count)
    }

    fn bytes(&mut self) -> Result<&'a [u8], IndexError> {
        let length = usize::try_from(self.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        self.take(length)
    }

    fn scalar(&mut self) -> Result<ScalarValue, IndexError> {
        match self.u8()? {
            0 => Ok(ScalarValue::Null),
            1 => Ok(ScalarValue::Boolean(self.boolean()?)),
            2 => Ok(ScalarValue::Signed(i64::from_le_bytes(
                self.take(8)?
                    .try_into()
                    .map_err(|_| IndexError::Integrity)?,
            ))),
            3 => Ok(ScalarValue::Unsigned(self.u64()?)),
            4 => {
                let bits = self.u64()?;
                let value = f64::from_bits(bits);
                if !value.is_finite() || (value == 0.0 && bits != 0) {
                    return Err(IndexError::Integrity);
                }
                Ok(ScalarValue::Number(bits))
            }
            5 => Ok(ScalarValue::String(
                std::str::from_utf8(self.bytes()?)
                    .map_err(|_| IndexError::Integrity)?
                    .to_owned(),
            )),
            _ => Err(IndexError::Integrity),
        }
    }

    fn finish(self) -> Result<(), IndexError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(IndexError::Integrity)
        }
    }
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
    use crate::v4::build::{
        ProjectedDocValue, ProjectedPoint, ProjectedRecord, ProjectedTerm, ProjectedVector,
    };
    use crate::v4::{
        Cardinality, Collation, ComponentKind, ComponentVersion, DocValueCell, FieldCapabilities,
        FieldComponents, FieldSchema, FieldType, IndexKind, IndexSemantics, ObjectIdentity,
        ScalarValue, TERM_TYPE_STRING, TERM_TYPE_TEXT,
    };
    use crate::v5::{StableDocumentKey, inherit_projection_preserving_versions};

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

    #[test]
    fn projection_preserving_update_emits_no_native_cache_mutation() {
        let schema = schema();
        let previous = projected_document_states(&schema, &source(7, "open")).unwrap();
        let mut current = projected_document_states(&schema, &source(19, "open")).unwrap();
        inherit_projection_preserving_versions(&mut current, &previous).unwrap();
        assert_eq!(current[0].head.source_version, 19);
        assert_eq!(current[0].head.material_source_version, 7);
        assert!(
            query_cache_mutations(&schema, 19, &current, &previous)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn material_change_emits_one_stable_native_cache_record() {
        let schema = schema();
        let previous = projected_document_states(&schema, &source(7, "open")).unwrap();
        let mut current = projected_document_states(&schema, &source(19, "fixed")).unwrap();
        inherit_projection_preserving_versions(&mut current, &previous).unwrap();
        let mutations = query_cache_mutations(&schema, 19, &current, &previous).unwrap();
        let MergeMutation::Upsert(projected) = &mutations[0] else {
            panic!("material change must upsert its stable cache record");
        };
        assert_eq!(mutations.len(), 1);
        assert_eq!(projected.source_identity.version, 19);
        assert_eq!(
            StableDocumentKey::from_query_cache_identity(&projected.source_identity).unwrap(),
            current[0].head.stable_key
        );
        assert_eq!(
            projected.records[0].result_identity,
            Some(ObjectIdentity {
                path: "objects/a".into(),
                version: 19,
            })
        );
    }

    #[test]
    fn removed_expanded_record_emits_one_stable_tombstone() {
        let schema = schema();
        let previous = projected_document_states(&schema, &source(7, "open")).unwrap();
        let mutations = query_cache_mutations(&schema, 19, &[], &previous).unwrap();
        let MergeMutation::Delete(identity) = &mutations[0] else {
            panic!("removed record must tombstone its stable cache identity");
        };
        assert_eq!(mutations.len(), 1);
        assert_eq!(identity.version, 19);
        assert_eq!(
            StableDocumentKey::from_query_cache_identity(identity).unwrap(),
            previous[0].head.stable_key
        );
    }

    #[test]
    fn canonical_field_state_round_trips_every_native_query_component() {
        let field_id = FieldId::new(7);
        let expected = ProjectedRecord {
            result_identity: None,
            order_key: Vec::new(),
            terms: vec![
                ProjectedTerm {
                    field_id,
                    term_type: TERM_TYPE_STRING,
                    term: b"\0open".to_vec(),
                    frequency: 1,
                    positions: Vec::new(),
                },
                ProjectedTerm {
                    field_id,
                    term_type: TERM_TYPE_TEXT,
                    term: b"token".to_vec(),
                    frequency: 2,
                    positions: vec![3, 9],
                },
            ],
            points: vec![ProjectedPoint {
                field_id,
                present: true,
                null: false,
                values: vec![
                    ScalarValue::Signed(-12),
                    ScalarValue::Unsigned(19),
                    ScalarValue::number(2.5).unwrap(),
                ],
            }],
            doc_values: vec![ProjectedDocValue {
                field_id,
                multi_valued: true,
                cell: DocValueCell {
                    present: true,
                    null: false,
                    values: vec![
                        ScalarValue::String("open".into()),
                        ScalarValue::String("resolved".into()),
                    ],
                },
            }],
            vectors: vec![ProjectedVector {
                field_id,
                values: vec![0.25, -4.5],
            }],
            field_lengths: vec![(field_id, 17)],
        };
        expected.validate().unwrap();
        let encoded = encode_field_state(&expected, field_id).unwrap();
        assert_eq!(
            decode_canonical_field_state(field_id, &encoded).unwrap(),
            expected
        );
    }

    #[test]
    fn canonical_field_state_decoder_rejects_truncation_and_trailing_bytes() {
        let field_id = FieldId::new(0);
        let record = &source(7, "open").records[0];
        let encoded = encode_field_state(record, field_id).unwrap();
        assert!(decode_canonical_field_state(field_id, &encoded[..encoded.len() - 1]).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_canonical_field_state(field_id, &trailing).is_err());
    }

    #[test]
    fn aliases_and_subsets_merge_to_one_canonical_recipe_union() {
        let first = family_state(&[(2, b"first")]);
        let alias = family_state(&[(2, b"first")]);
        let second = family_state(&[(3, b"second")]);
        let merged =
            merge_projected_document_states(vec![vec![first], vec![alias], vec![second]]).unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].memberships.len(), 1);
        assert_eq!(merged[0].fields.len(), 2);
    }

    #[test]
    fn family_merge_rejects_conflicting_expansion_or_recipe_bytes() {
        let first = family_state(&[(2, b"first")]);
        let conflicting = family_state(&[(2, b"conflict")]);
        assert!(merge_projected_document_states(vec![vec![first], vec![conflicting]]).is_err());

        let first = family_state(&[(2, b"first")]);
        assert!(merge_projected_document_states(vec![vec![first], Vec::new()]).is_err());
    }

    fn family_state(fields: &[(u8, &[u8])]) -> ProjectedDocumentState {
        let scope = [1; 32];
        ProjectedDocumentState::new(
            scope,
            DocumentHead::new(scope, "objects/a".into(), 0, 7, None, true).unwrap(),
            vec![
                CanonicalRecipeState::new(RecipeIdentity::new([1; 32]).unwrap(), vec![1]).unwrap(),
            ],
            fields
                .iter()
                .map(|(identity, value)| {
                    CanonicalRecipeState::new(
                        RecipeIdentity::new([*identity; 32]).unwrap(),
                        value.to_vec(),
                    )
                    .unwrap()
                })
                .collect(),
        )
        .unwrap()
    }
}
