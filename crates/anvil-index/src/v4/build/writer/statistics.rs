use crate::IndexError;

use super::layout::{DocumentRef, charged_vec, order_bytes, record, source};
use crate::v4::build::ProjectedSource;
use crate::v4::{
    ComponentStatistics, FieldComponents, FieldStatistics, IndexSemantics, PhysicalOrderBounds,
    ScalarValue, Schema, SegmentStatistics,
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
            for column in &record.columns {
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
                        ScalarValue::Number(_) => &mut field.number_values,
                        ScalarValue::Unsigned(_) => &mut field.unsigned_values,
                        ScalarValue::String(_) => &mut field.string_values,
                    };
                    *counter = counter.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
                }
            }
            for term in &record.terms {
                mark_present(&mut fields, &mut seen, marker, term.field_id.get() as usize)?;
                fields[term.field_id.get() as usize].total_term_frequency = fields
                    [term.field_id.get() as usize]
                    .total_term_frequency
                    .checked_add(u64::from(term.frequency))
                    .ok_or(IndexError::OffsetOverflow)?;
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
