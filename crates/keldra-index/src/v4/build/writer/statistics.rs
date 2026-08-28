use crate::IndexError;

use super::layout::{DocumentRef, charged_vec, order_bytes, record, source};
use crate::v4::build::ProjectedSource;
use crate::v4::{
    ComponentStatistics, FieldComponents, FieldStatistics, FieldType, IndexSemantics,
    PhysicalOrderBounds, ScalarValue, Schema, SegmentStatistics, TERM_TYPE_BOOLEAN,
    TERM_TYPE_FIELD_PRESENCE, TERM_TYPE_HASHED_KEYWORD, TERM_TYPE_NULL, TERM_TYPE_STRING,
};

pub(super) struct StatisticsAccumulator {
    fields: Vec<FieldStatistics>,
}

impl StatisticsAccumulator {
    pub fn from_documents(
        schema: &Schema,
        sources: &[ProjectedSource],
        documents: &[DocumentRef],
    ) -> Result<Self, IndexError> {
        let mut fields = charged_vec(schema.fields.len())?;
        fields.extend(schema.fields.iter().map(|field| FieldStatistics {
            field_id: field.id,
            present_documents: 0,
            null_documents: 0,
            value_count: 0,
            unique_terms: 0,
            total_term_frequency: 0,
            total_field_length: 0,
            minimum_field_length: None,
            maximum_field_length: None,
            vector_count: 0,
            vector_dimensions: if field.components.contains(FieldComponents::VECTOR) {
                match &schema.semantics {
                    IndexSemantics::Vector { dimensions, .. }
                    | IndexSemantics::Hybrid { dimensions, .. } => Some(*dimensions),
                    _ => None,
                }
            } else {
                None
            },
            multi_valued_documents: 0,
            boolean_values: 0,
            number_values: 0,
            unsigned_values: 0,
            string_values: 0,
        }));
        let mut seen = charged_vec(fields.len())?;
        seen.resize(fields.len(), u32::MAX);
        for (doc_id, document) in documents.iter().copied().enumerate() {
            let marker = u32::try_from(doc_id).map_err(|_| IndexError::OffsetOverflow)?;
            let record = record(sources, document);
            for column in &record.doc_values {
                if column.cell.present {
                    mark_present(
                        &mut fields,
                        &mut seen,
                        marker,
                        column.field_id.get() as usize,
                    )?;
                }
                let field = &mut fields[column.field_id.get() as usize];
                field.null_documents = field
                    .null_documents
                    .checked_add(u64::from(column.cell.null))
                    .ok_or(IndexError::OffsetOverflow)?;
                field.value_count = field
                    .value_count
                    .checked_add(
                        u64::try_from(column.cell.values.len())
                            .map_err(|_| IndexError::OffsetOverflow)?,
                    )
                    .ok_or(IndexError::OffsetOverflow)?;
                field.multi_valued_documents = field
                    .multi_valued_documents
                    .checked_add(u64::from(
                        column
                            .cell
                            .values
                            .len()
                            .saturating_add(usize::from(column.cell.null))
                            > 1,
                    ))
                    .ok_or(IndexError::OffsetOverflow)?;
                for value in &column.cell.values {
                    let counter = match value {
                        ScalarValue::Null => {
                            return Err(IndexError::InvalidDefinition(
                                "fast-column values must encode null separately".into(),
                            ));
                        }
                        ScalarValue::Boolean(_) => &mut field.boolean_values,
                        ScalarValue::Signed(_) | ScalarValue::Number(_) => &mut field.number_values,
                        ScalarValue::Unsigned(_) => &mut field.unsigned_values,
                        ScalarValue::String(_) => &mut field.string_values,
                    };
                    *counter = counter.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
                }
            }
            for point in &record.points {
                let index = point.field_id.get() as usize;
                if schema.fields[index]
                    .components
                    .contains(FieldComponents::DOC_VALUES)
                {
                    continue;
                }
                mark_present(&mut fields, &mut seen, marker, index)?;
                let field = &mut fields[index];
                field.null_documents = field
                    .null_documents
                    .checked_add(u64::from(point.null))
                    .ok_or(IndexError::OffsetOverflow)?;
                field.value_count = field
                    .value_count
                    .checked_add(
                        u64::try_from(point.values.len())
                            .map_err(|_| IndexError::OffsetOverflow)?,
                    )
                    .ok_or(IndexError::OffsetOverflow)?;
                field.multi_valued_documents = field
                    .multi_valued_documents
                    .checked_add(u64::from(
                        point.values.len().saturating_add(usize::from(point.null)) > 1,
                    ))
                    .ok_or(IndexError::OffsetOverflow)?;
                for value in &point.values {
                    let counter = match value {
                        ScalarValue::Signed(_) | ScalarValue::Number(_) => &mut field.number_values,
                        ScalarValue::Unsigned(_) => &mut field.unsigned_values,
                        _ => {
                            return Err(IndexError::InvalidDefinition(
                                "point values must be numeric".into(),
                            ));
                        }
                    };
                    *counter = counter.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
                }
            }
            for term in &record.terms {
                let index = term.field_id.get() as usize;
                mark_present(&mut fields, &mut seen, marker, index)?;
                if term.term_type == TERM_TYPE_FIELD_PRESENCE {
                    continue;
                }
                let field = &mut fields[index];
                field.total_term_frequency = field
                    .total_term_frequency
                    .checked_add(u64::from(term.frequency))
                    .ok_or(IndexError::OffsetOverflow)?;
                if terms_are_scalar_authority(schema, index) {
                    observe_scalar_term(field, term.term_type, term.frequency)?;
                }
            }
            for (field_id, length) in &record.field_lengths {
                let index = field_id.get() as usize;
                mark_present(&mut fields, &mut seen, marker, index)?;
                let field = &mut fields[index];
                field.total_field_length = field
                    .total_field_length
                    .checked_add(u64::from(*length))
                    .ok_or(IndexError::OffsetOverflow)?;
                field.minimum_field_length = Some(
                    field
                        .minimum_field_length
                        .map_or(*length, |minimum| minimum.min(*length)),
                );
                field.maximum_field_length = Some(
                    field
                        .maximum_field_length
                        .map_or(*length, |maximum| maximum.max(*length)),
                );
            }
            for vector in &record.vectors {
                let index = vector.field_id.get() as usize;
                mark_present(&mut fields, &mut seen, marker, index)?;
                fields[index].vector_count = fields[index]
                    .vector_count
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
            }
        }
        Ok(Self { fields })
    }

    pub fn observe_unique_term(&mut self, field_ordinal: usize) -> Result<(), IndexError> {
        let field = self
            .fields
            .get_mut(field_ordinal)
            .ok_or_else(|| IndexError::InvalidDefinition("term field is outside schema".into()))?;
        field.unique_terms = field
            .unique_terms
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(())
    }

    pub fn minimum_field_length(&self, field_ordinal: usize) -> Option<u32> {
        self.fields
            .get(field_ordinal)
            .and_then(|field| field.minimum_field_length)
    }

    pub fn finish(
        self,
        source_count: u64,
        schema: &Schema,
        sources: &[ProjectedSource],
        documents: &[DocumentRef],
        components: Vec<ComponentStatistics>,
    ) -> Result<SegmentStatistics, IndexError> {
        let unique_terms = self.fields.iter().try_fold(0u64, |sum, field| {
            sum.checked_add(field.unique_terms)
                .ok_or(IndexError::OffsetOverflow)
        })?;
        let physical_order_bounds = (!schema.physical_order.is_empty()).then(|| {
            let first = documents.first().expect("nonempty sealed segment");
            let last = documents.last().expect("nonempty sealed segment");
            PhysicalOrderBounds {
                minimum_key: order_bytes(source(sources, *first), record(sources, *first)).to_vec(),
                maximum_key: order_bytes(source(sources, *last), record(sources, *last)).to_vec(),
            }
        });
        SegmentStatistics::new(
            source_count,
            u64::try_from(documents.len()).map_err(|_| IndexError::OffsetOverflow)?,
            unique_terms,
            physical_order_bounds,
            self.fields,
            components,
        )
    }
}

fn terms_are_scalar_authority(schema: &Schema, field_ordinal: usize) -> bool {
    let field = &schema.fields[field_ordinal];
    !field.components.contains(FieldComponents::DOC_VALUES)
        && !field.components.contains(FieldComponents::POINTS)
        && matches!(field.field_type, FieldType::Boolean | FieldType::Keyword)
}

fn observe_scalar_term(
    field: &mut FieldStatistics,
    term_type: u8,
    frequency: u32,
) -> Result<(), IndexError> {
    if term_type == TERM_TYPE_NULL {
        field.null_documents = field
            .null_documents
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        return Ok(());
    }
    let frequency = u64::from(frequency);
    let counter = match term_type {
        TERM_TYPE_BOOLEAN => &mut field.boolean_values,
        TERM_TYPE_STRING | TERM_TYPE_HASHED_KEYWORD => &mut field.string_values,
        _ => return Ok(()),
    };
    field.value_count = field
        .value_count
        .checked_add(frequency)
        .ok_or(IndexError::OffsetOverflow)?;
    *counter = counter
        .checked_add(frequency)
        .ok_or(IndexError::OffsetOverflow)?;
    Ok(())
}

fn mark_present(
    fields: &mut [FieldStatistics],
    seen: &mut [u32],
    marker: u32,
    index: usize,
) -> Result<(), IndexError> {
    if seen.get(index).copied() == Some(marker) {
        return Ok(());
    }
    let field = fields
        .get_mut(index)
        .ok_or_else(|| IndexError::InvalidDefinition("projected field is outside schema".into()))?;
    seen[index] = marker;
    field.present_documents = field
        .present_documents
        .checked_add(1)
        .ok_or(IndexError::OffsetOverflow)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v4::build::{ProjectedDocValue, ProjectedRecord, ProjectedTerm};
    use crate::v4::{
        Cardinality, Collation, DocValueCell, FieldCapabilities, FieldId, FieldSchema, IndexKind,
        ObjectIdentity, TERM_TYPE_FIELD_PRESENCE, scalar_term,
    };

    fn schema(components: FieldComponents) -> Schema {
        Schema {
            kind: IndexKind::TypedJson,
            path_prefix: "objects/".into(),
            content_type_scope: Some("application/json".into()),
            fields: vec![FieldSchema {
                id: FieldId::new(0),
                name: "state".into(),
                source_selector: "/state".into(),
                field_type: FieldType::Keyword,
                cardinality: Cardinality::Multi,
                allow_missing: true,
                allow_null: true,
                collation: Collation::BinaryUtf8,
                capabilities: FieldCapabilities::EXACT,
                analyzer: None,
                date_format: None,
                components,
            }],
            semantics: IndexSemantics::TypedJson,
            physical_order: Vec::new(),
            component_versions: Vec::new(),
        }
    }

    fn projected_term(value: ScalarValue, frequency: u32) -> ProjectedTerm {
        let (term_type, term) = scalar_term(&value).unwrap();
        ProjectedTerm {
            field_id: FieldId::new(0),
            term_type,
            term,
            frequency,
            positions: Vec::new(),
        }
    }

    fn presence() -> ProjectedTerm {
        ProjectedTerm {
            field_id: FieldId::new(0),
            term_type: TERM_TYPE_FIELD_PRESENCE,
            term: crate::v4::FIELD_PRESENCE_TERM.to_vec(),
            frequency: 1,
            positions: Vec::new(),
        }
    }

    fn source(records: Vec<ProjectedRecord>) -> Vec<ProjectedSource> {
        vec![ProjectedSource {
            source_identity: ObjectIdentity {
                path: "objects/source".into(),
                version: 1,
            },
            records,
        }]
    }

    fn record(terms: Vec<ProjectedTerm>, doc_values: Vec<ProjectedDocValue>) -> ProjectedRecord {
        ProjectedRecord {
            result_identity: None,
            order_key: Vec::new(),
            terms,
            points: Vec::new(),
            doc_values,
            vectors: Vec::new(),
            field_lengths: Vec::new(),
        }
    }

    #[test]
    fn terms_only_statistics_distinguish_presence_null_and_values() {
        let schema = schema(FieldComponents::TERMS);
        let sources = source(vec![
            record(
                vec![presence(), projected_term(ScalarValue::Null, 1)],
                Vec::new(),
            ),
            record(
                vec![
                    presence(),
                    projected_term(ScalarValue::String("active".into()), 2),
                ],
                Vec::new(),
            ),
            record(Vec::new(), Vec::new()),
        ]);
        let documents = (0..3)
            .map(|source_record| DocumentRef {
                source_ordinal: 0,
                source_record,
            })
            .collect::<Vec<_>>();
        let accumulator =
            StatisticsAccumulator::from_documents(&schema, &sources, &documents).unwrap();
        let field = &accumulator.fields[0];
        assert_eq!(field.present_documents, 2);
        assert_eq!(field.null_documents, 1);
        assert_eq!(field.value_count, 2);
        assert_eq!(field.string_values, 2);
        assert_eq!(field.total_term_frequency, 3);
    }

    #[test]
    fn doc_values_remain_the_only_scalar_count_authority() {
        let schema = schema(FieldComponents::TERMS.union(FieldComponents::DOC_VALUES));
        let sources = source(vec![record(
            vec![
                presence(),
                projected_term(ScalarValue::String("active".into()), 1),
            ],
            vec![ProjectedDocValue {
                field_id: FieldId::new(0),
                multi_valued: true,
                cell: DocValueCell::value(ScalarValue::String("active".into())),
            }],
        )]);
        let documents = vec![DocumentRef {
            source_ordinal: 0,
            source_record: 0,
        }];
        let accumulator =
            StatisticsAccumulator::from_documents(&schema, &sources, &documents).unwrap();
        let field = &accumulator.fields[0];
        assert_eq!(field.present_documents, 1);
        assert_eq!(field.value_count, 1);
        assert_eq!(field.string_values, 1);
        assert_eq!(field.total_term_frequency, 1);
    }
}
