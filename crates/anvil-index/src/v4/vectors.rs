use crate::IndexError;

use super::codec::{COMPONENT_HEADER_BYTES, Decoder, Encoder};
use super::model::{DocId, INDEX_COMPONENT_BYTES};
use super::schema::FieldId;

const VECTOR_CODEC_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq)]
pub struct VectorBlock {
    pub field_id: FieldId,
    pub first_doc_id: DocId,
    pub dimensions: u32,
    values: Vec<Option<Vec<f32>>>,
}

impl VectorBlock {
    pub fn new(
        field_id: FieldId,
        first_doc_id: DocId,
        dimensions: u32,
        values: Vec<Option<Vec<f32>>>,
    ) -> Result<Self, IndexError> {
        if dimensions == 0 || values.is_empty() {
            return Err(IndexError::InvalidDefinition(
                "vector blocks require dimensions and documents".into(),
            ));
        }
        first_doc_id
            .get()
            .checked_add(u32::try_from(values.len() - 1).map_err(|_| IndexError::OffsetOverflow)?)
            .ok_or(IndexError::OffsetOverflow)?;
        if values.iter().flatten().any(|vector| {
            vector.len() != dimensions as usize || vector.iter().any(|value| !value.is_finite())
        }) {
            return Err(IndexError::InvalidDefinition(
                "vectors must have the declared dimensions and finite values".into(),
            ));
        }
        let block = Self {
            field_id,
            first_doc_id,
            dimensions,
            values,
        };
        let needed = block.encode_payload()?.len() + COMPONENT_HEADER_BYTES;
        if needed > INDEX_COMPONENT_BYTES {
            return Err(IndexError::ResourceLimit {
                needed,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        Ok(block)
    }

    pub fn values(&self) -> &[Option<Vec<f32>>] {
        &self.values
    }

    pub fn get(&self, doc_id: DocId) -> Option<&[f32]> {
        let offset = doc_id.get().checked_sub(self.first_doc_id.get())?;
        self.values.get(offset as usize)?.as_deref()
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        let mut presence = vec![0u8; self.values.len().div_ceil(8)];
        let mut out = Encoder::default();
        out.u16(VECTOR_CODEC_VERSION);
        out.u32(self.field_id.get());
        out.u32(self.first_doc_id.get());
        out.usize_u32(self.values.len())?;
        out.u32(self.dimensions);
        for (index, value) in self.values.iter().enumerate() {
            if value.is_some() {
                presence[index / 8] |= 1 << (index % 8);
            }
        }
        out.bytes(&presence)?;
        for vector in self.values.iter().flatten() {
            for value in vector {
                out.u32(value.to_bits());
            }
        }
        Ok(out.finish())
    }

    pub fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.u16()? != VECTOR_CODEC_VERSION {
            return Err(IndexError::InvalidFormat("vector codec version"));
        }
        let field_id = FieldId::new(input.u32()?);
        let first_doc_id = DocId::new(input.u32()?);
        let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        let dimensions = input.u32()?;
        let presence = input.owned_bytes()?;
        if presence.len() != count.div_ceil(8)
            || count % 8 != 0
                && presence
                    .last()
                    .is_some_and(|byte| *byte >> (count % 8) != 0)
        {
            return Err(IndexError::InvalidFormat("vector presence bitmap"));
        }
        let present = presence
            .iter()
            .map(|byte| byte.count_ones() as usize)
            .sum::<usize>();
        let scalar_count = present
            .checked_mul(dimensions as usize)
            .ok_or(IndexError::OffsetOverflow)?;
        input.claim(
            count
                .checked_mul(std::mem::size_of::<Option<Vec<f32>>>())
                .and_then(|bytes| bytes.checked_add(scalar_count.saturating_mul(4)))
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut values = Vec::with_capacity(count);
        for index in 0..count {
            if presence[index / 8] & (1 << (index % 8)) == 0 {
                values.push(None);
                continue;
            }
            let mut vector = Vec::with_capacity(dimensions as usize);
            for _ in 0..dimensions {
                vector.push(f32::from_bits(input.u32()?));
            }
            values.push(Some(vector));
        }
        input.finish()?;
        Self::new(field_id, first_doc_id, dimensions, values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_and_present_vectors_round_trip() {
        let block = VectorBlock::new(
            FieldId::new(2),
            DocId::new(7),
            2,
            vec![Some(vec![1.0, 2.0]), None, Some(vec![3.0, 4.0])],
        )
        .unwrap();
        let decoded = VectorBlock::decode_payload(&block.encode_payload().unwrap()).unwrap();
        assert_eq!(decoded, block);
        assert_eq!(decoded.get(DocId::new(9)), Some(&[3.0, 4.0][..]));
    }
}
