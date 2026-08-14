use crate::IndexError;

use super::codec::{COMPONENT_HEADER_BYTES, Decoder, Encoder};
use super::model::{
    ComponentKind, DocId, INDEX_COMPONENT_BYTES, INDEX_DECODE_BYTES, INDEX_ROUTING_KEY_BYTES,
};
use super::schema::FieldId;

const POSITIONS_CODEC_VERSION: u16 = 1;
const NORMS_CODEC_VERSION: u16 = 1;
const STATISTICS_CODEC_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PositionEntry {
    pub doc_id: DocId,
    pub positions: Vec<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PositionsBlock {
    entries: Vec<PositionEntry>,
}

impl PositionsBlock {
    pub fn new(entries: Vec<PositionEntry>) -> Result<Self, IndexError> {
        if entries.is_empty()
            || entries
                .windows(2)
                .any(|pair| pair[0].doc_id >= pair[1].doc_id)
            || entries.iter().any(|entry| {
                entry.positions.is_empty()
                    || entry.positions.windows(2).any(|pair| pair[0] >= pair[1])
            })
        {
            return Err(IndexError::InvalidDefinition(
                "positions require ordered DocIds and ordered non-empty offsets".into(),
            ));
        }
        let block = Self { entries };
        let needed = block.encode_payload()?.len() + COMPONENT_HEADER_BYTES;
        if needed > INDEX_COMPONENT_BYTES {
            return Err(IndexError::ResourceLimit {
                needed,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        Ok(block)
    }

    pub fn entries(&self) -> &[PositionEntry] {
        &self.entries
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        let mut out = Encoder::default();
        out.u16(POSITIONS_CODEC_VERSION);
        out.usize_u32(self.entries.len())?;
        for entry in &self.entries {
            out.u32(entry.doc_id.get());
            out.usize_u32(entry.positions.len())?;
            for position in &entry.positions {
                out.u32(*position);
            }
        }
        Ok(out.finish())
    }

    pub fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.u16()? != POSITIONS_CODEC_VERSION {
            return Err(IndexError::InvalidFormat("positions codec version"));
        }
        let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        input.claim(count.saturating_mul(std::mem::size_of::<PositionEntry>()))?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let doc_id = DocId::new(input.u32()?);
            let positions =
                usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
            input.claim(positions.saturating_mul(4))?;
            let mut values = Vec::with_capacity(positions);
            for _ in 0..positions {
                values.push(input.u32()?);
            }
            entries.push(PositionEntry {
                doc_id,
                positions: values,
            });
        }
        input.finish()?;
        Self::new(entries)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormBlock {
    pub field_id: FieldId,
    pub first_doc_id: DocId,
    values: Vec<Option<u32>>,
}

impl NormBlock {
    pub fn new(
        field_id: FieldId,
        first_doc_id: DocId,
        values: Vec<Option<u32>>,
    ) -> Result<Self, IndexError> {
        if values.is_empty() {
            return Err(IndexError::InvalidDefinition("norm block is empty".into()));
        }
        first_doc_id
            .get()
            .checked_add(u32::try_from(values.len() - 1).map_err(|_| IndexError::OffsetOverflow)?)
            .ok_or(IndexError::OffsetOverflow)?;
        let block = Self {
            field_id,
            first_doc_id,
            values,
        };
        if block.encode_payload()?.len() + COMPONENT_HEADER_BYTES > INDEX_COMPONENT_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: block.encode_payload()?.len() + COMPONENT_HEADER_BYTES,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        Ok(block)
    }

    pub fn values(&self) -> &[Option<u32>] {
        &self.values
    }

    pub fn get(&self, doc_id: DocId) -> Option<u32> {
        let offset = doc_id.get().checked_sub(self.first_doc_id.get())?;
        self.values.get(offset as usize).copied().flatten()
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        let mut out = Encoder::default();
        out.u16(NORMS_CODEC_VERSION);
        out.u32(self.field_id.get());
        out.u32(self.first_doc_id.get());
        out.usize_u32(self.values.len())?;
        for value in &self.values {
            out.bool(value.is_some());
            if let Some(value) = value {
                out.u32(*value);
            }
        }
        Ok(out.finish())
    }

    pub fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.u16()? != NORMS_CODEC_VERSION {
            return Err(IndexError::InvalidFormat("norm codec version"));
        }
        let field_id = FieldId::new(input.u32()?);
        let first_doc_id = DocId::new(input.u32()?);
        let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        input.claim(count.saturating_mul(std::mem::size_of::<Option<u32>>()))?;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(input.bool()?.then(|| input.u32()).transpose()?);
        }
        input.finish()?;
        Self::new(field_id, first_doc_id, values)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldStatistics {
    pub field_id: FieldId,
    pub present_documents: u64,
    pub null_documents: u64,
    pub value_count: u64,
    pub unique_terms: u64,
    pub total_term_frequency: u64,
    pub total_field_length: u64,
    pub minimum_field_length: Option<u32>,
    pub maximum_field_length: Option<u32>,
    pub vector_count: u64,
    pub vector_dimensions: Option<u32>,
    pub multi_valued_documents: u64,
    pub boolean_values: u64,
    pub number_values: u64,
    pub unsigned_values: u64,
    pub string_values: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalOrderBounds {
    pub minimum_key: Vec<u8>,
    pub maximum_key: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentStatistics {
    pub role: ComponentKind,
    pub field_id: Option<FieldId>,
    pub leaf_count: u64,
    pub component_count: u64,
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub decoded_bytes_upper_bound: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentStatistics {
    pub source_count: u64,
    pub document_count: u64,
    pub unique_terms: u64,
    pub physical_order_bounds: Option<PhysicalOrderBounds>,
    pub fields: Vec<FieldStatistics>,
    pub components: Vec<ComponentStatistics>,
}

impl SegmentStatistics {
    pub fn new(
        source_count: u64,
        document_count: u64,
        unique_terms: u64,
        physical_order_bounds: Option<PhysicalOrderBounds>,
        fields: Vec<FieldStatistics>,
        components: Vec<ComponentStatistics>,
    ) -> Result<Self, IndexError> {
        let field_unique_terms = fields.iter().try_fold(0u64, |sum, field| {
            sum.checked_add(field.unique_terms)
                .ok_or(IndexError::OffsetOverflow)
        })?;
        if source_count == 0
            || document_count == 0
            || source_count > document_count
            || field_unique_terms != unique_terms
            || fields
                .windows(2)
                .any(|pair| pair[0].field_id >= pair[1].field_id)
            || fields.iter().any(|field| {
                field.present_documents > document_count
                    || field.null_documents > field.present_documents
                    || field.vector_count > field.present_documents
                    || field.multi_valued_documents > field.present_documents
                    || field.total_term_frequency < field.unique_terms
                    || field
                        .boolean_values
                        .checked_add(field.number_values)
                        .and_then(|value| value.checked_add(field.unsigned_values))
                        .and_then(|value| value.checked_add(field.string_values))
                        != Some(field.value_count)
                    || field.minimum_field_length.is_some() != field.maximum_field_length.is_some()
                    || field
                        .minimum_field_length
                        .zip(field.maximum_field_length)
                        .is_some_and(|(minimum, maximum)| minimum > maximum)
                    || field.minimum_field_length.is_none() && field.total_field_length != 0
                    || field.vector_dimensions == Some(0)
                    || field.vector_count != 0 && field.vector_dimensions.is_none()
            })
            || physical_order_bounds.as_ref().is_some_and(|bounds| {
                bounds.minimum_key.is_empty()
                    || bounds.maximum_key.is_empty()
                    || bounds.minimum_key.len() > INDEX_ROUTING_KEY_BYTES
                    || bounds.maximum_key.len() > INDEX_ROUTING_KEY_BYTES
                    || bounds.minimum_key > bounds.maximum_key
            })
            || components
                .windows(2)
                .any(|pair| (pair[0].role, pair[0].field_id) >= (pair[1].role, pair[1].field_id))
            || components.iter().any(|component| {
                let expected_decoded = component
                    .component_count
                    .checked_mul(INDEX_DECODE_BYTES as u64);
                !tracks_component_statistics(component.role)
                    || matches!(
                        component.role,
                        ComponentKind::POSTINGS
                            | ComponentKind::FAST_COLUMN
                            | ComponentKind::POSITIONS
                            | ComponentKind::VECTORS
                    ) && component.field_id.is_none()
                    || component.role == ComponentKind::STORED_FIELDS
                        && component.field_id.is_some()
                    || component.leaf_count == 0
                    || component.component_count < component.leaf_count
                    || component.encoded_bytes == 0
                    || component.logical_bytes == 0
                    || component.decoded_bytes_upper_bound < component.logical_bytes
                    || expected_decoded != Some(component.decoded_bytes_upper_bound)
            })
        {
            return Err(IndexError::InvalidDefinition(
                "segment statistics are inconsistent".into(),
            ));
        }
        Ok(Self {
            source_count,
            document_count,
            unique_terms,
            physical_order_bounds,
            fields,
            components,
        })
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        let mut out = Encoder::default();
        out.u16(STATISTICS_CODEC_VERSION);
        out.u64(self.source_count);
        out.u64(self.document_count);
        out.u64(self.unique_terms);
        out.bool(self.physical_order_bounds.is_some());
        if let Some(bounds) = &self.physical_order_bounds {
            out.bytes(&bounds.minimum_key)?;
            out.bytes(&bounds.maximum_key)?;
        }
        out.usize_u32(self.fields.len())?;
        for field in &self.fields {
            out.u32(field.field_id.get());
            out.u64(field.present_documents);
            out.u64(field.null_documents);
            out.u64(field.value_count);
            out.u64(field.unique_terms);
            out.u64(field.total_term_frequency);
            out.u64(field.total_field_length);
            encode_optional_u32(&mut out, field.minimum_field_length);
            encode_optional_u32(&mut out, field.maximum_field_length);
            out.u64(field.vector_count);
            encode_optional_u32(&mut out, field.vector_dimensions);
            out.u64(field.multi_valued_documents);
            out.u64(field.boolean_values);
            out.u64(field.number_values);
            out.u64(field.unsigned_values);
            out.u64(field.string_values);
        }
        out.usize_u32(self.components.len())?;
        for component in &self.components {
            out.u16(component.role.get());
            encode_optional_u32(&mut out, component.field_id.map(FieldId::get));
            out.u64(component.leaf_count);
            out.u64(component.component_count);
            out.u64(component.encoded_bytes);
            out.u64(component.logical_bytes);
            out.u64(component.decoded_bytes_upper_bound);
        }
        Ok(out.finish())
    }

    pub fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.u16()? != STATISTICS_CODEC_VERSION {
            return Err(IndexError::InvalidFormat("statistics codec version"));
        }
        let source_count = input.u64()?;
        let document_count = input.u64()?;
        let unique_terms = input.u64()?;
        let physical_order_bounds = if input.bool()? {
            Some(PhysicalOrderBounds {
                minimum_key: input.owned_bytes()?,
                maximum_key: input.owned_bytes()?,
            })
        } else {
            None
        };
        let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        input.claim(count.saturating_mul(std::mem::size_of::<FieldStatistics>()))?;
        let mut fields = Vec::with_capacity(count);
        for _ in 0..count {
            fields.push(FieldStatistics {
                field_id: FieldId::new(input.u32()?),
                present_documents: input.u64()?,
                null_documents: input.u64()?,
                value_count: input.u64()?,
                unique_terms: input.u64()?,
                total_term_frequency: input.u64()?,
                total_field_length: input.u64()?,
                minimum_field_length: decode_optional_u32(&mut input)?,
                maximum_field_length: decode_optional_u32(&mut input)?,
                vector_count: input.u64()?,
                vector_dimensions: decode_optional_u32(&mut input)?,
                multi_valued_documents: input.u64()?,
                boolean_values: input.u64()?,
                number_values: input.u64()?,
                unsigned_values: input.u64()?,
                string_values: input.u64()?,
            });
        }
        let component_count =
            usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        input.claim(component_count.saturating_mul(std::mem::size_of::<ComponentStatistics>()))?;
        let mut components = Vec::with_capacity(component_count);
        for _ in 0..component_count {
            components.push(ComponentStatistics {
                role: ComponentKind::new(input.u16()?)
                    .map_err(|_| IndexError::InvalidFormat("statistics component role"))?,
                field_id: decode_optional_u32(&mut input)?.map(FieldId::new),
                leaf_count: input.u64()?,
                component_count: input.u64()?,
                encoded_bytes: input.u64()?,
                logical_bytes: input.u64()?,
                decoded_bytes_upper_bound: input.u64()?,
            });
        }
        input.finish()?;
        Self::new(
            source_count,
            document_count,
            unique_terms,
            physical_order_bounds,
            fields,
            components,
        )
    }
}

pub(crate) fn tracks_component_statistics(role: ComponentKind) -> bool {
    matches!(
        role,
        ComponentKind::POSTINGS
            | ComponentKind::FAST_COLUMN
            | ComponentKind::STORED_FIELDS
            | ComponentKind::POSITIONS
            | ComponentKind::VECTORS
    )
}

fn encode_optional_u32(out: &mut Encoder, value: Option<u32>) {
    out.bool(value.is_some());
    if let Some(value) = value {
        out.u32(value);
    }
}

fn decode_optional_u32(input: &mut Decoder<'_>) -> Result<Option<u32>, IndexError> {
    input.bool()?.then(|| input.u32()).transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_norms_and_statistics_round_trip() {
        let positions = PositionsBlock::new(vec![PositionEntry {
            doc_id: DocId::new(2),
            positions: vec![1, 4, 9],
        }])
        .unwrap();
        assert_eq!(
            PositionsBlock::decode_payload(&positions.encode_payload().unwrap()).unwrap(),
            positions
        );
        let norms = NormBlock::new(FieldId::new(1), DocId::new(2), vec![Some(3), None]).unwrap();
        assert_eq!(
            NormBlock::decode_payload(&norms.encode_payload().unwrap()).unwrap(),
            norms
        );
        let statistics = SegmentStatistics::new(
            1,
            2,
            3,
            Some(PhysicalOrderBounds {
                minimum_key: vec![1],
                maximum_key: vec![9],
            }),
            vec![FieldStatistics {
                field_id: FieldId::new(0),
                present_documents: 2,
                null_documents: 1,
                value_count: 3,
                unique_terms: 3,
                total_term_frequency: 5,
                total_field_length: 7,
                minimum_field_length: Some(2),
                maximum_field_length: Some(5),
                vector_count: 2,
                vector_dimensions: Some(4),
                multi_valued_documents: 1,
                boolean_values: 1,
                number_values: 1,
                unsigned_values: 0,
                string_values: 1,
            }],
            vec![ComponentStatistics {
                role: ComponentKind::POSTINGS,
                field_id: Some(FieldId::new(0)),
                leaf_count: 1,
                component_count: 2,
                encoded_bytes: 200,
                logical_bytes: 100,
                decoded_bytes_upper_bound: 2 * INDEX_DECODE_BYTES as u64,
            }],
        )
        .unwrap();
        assert_eq!(
            SegmentStatistics::decode_payload(&statistics.encode_payload().unwrap()).unwrap(),
            statistics
        );
    }
}
