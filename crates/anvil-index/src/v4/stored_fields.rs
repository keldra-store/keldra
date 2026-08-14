use crate::IndexError;

use super::codec::{COMPONENT_HEADER_BYTES, Decoder, Encoder};
use super::model::{DocId, INDEX_COMPONENT_BYTES};

pub const STORED_FIELDS_COMPONENT_CODEC_VERSION: u16 = 2;
pub(crate) const MAX_STORED_FIELDS_PAYLOAD_BYTES: usize =
    INDEX_COMPONENT_BYTES - COMPONENT_HEADER_BYTES;

/// Late-materialized projected values for one contiguous DocId range.
///
/// `None` means no stored projection for the document; an empty stored value
/// is represented by `Some(&[])` and remains distinct.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredFieldsBlock {
    pub first_doc_id: DocId,
    values: Vec<Option<Vec<u8>>>,
}

impl StoredFieldsBlock {
    pub fn new(first_doc_id: DocId, values: Vec<Option<Vec<u8>>>) -> Result<Self, IndexError> {
        if values.is_empty() {
            return Err(IndexError::InvalidDefinition(
                "stored-fields block must not be empty".into(),
            ));
        }
        first_doc_id
            .get()
            .checked_add(u32::try_from(values.len() - 1).map_err(|_| IndexError::OffsetOverflow)?)
            .ok_or(IndexError::OffsetOverflow)?;
        let value = Self {
            first_doc_id,
            values,
        };
        let needed = value.encode_payload()?.len();
        if needed > MAX_STORED_FIELDS_PAYLOAD_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: needed + COMPONENT_HEADER_BYTES,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        Ok(value)
    }

    pub fn document_count(&self) -> usize {
        self.values.len()
    }

    pub fn get(&self, doc_id: DocId) -> Option<&[u8]> {
        let offset = doc_id.get().checked_sub(self.first_doc_id.get())?;
        self.values.get(offset as usize)?.as_deref()
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        let mut presence = vec![0u8; self.values.len().div_ceil(8)];
        let mut offsets = Vec::with_capacity(self.values.len() + 1);
        let mut payload = Vec::new();
        offsets.push(0u32);
        for (index, value) in self.values.iter().enumerate() {
            if let Some(value) = value {
                presence[index / 8] |= 1 << (index % 8);
                payload.extend_from_slice(value);
            }
            offsets.push(u32::try_from(payload.len()).map_err(|_| IndexError::OffsetOverflow)?);
        }
        let mut out = Encoder::default();
        out.u16(STORED_FIELDS_COMPONENT_CODEC_VERSION);
        out.u32(self.first_doc_id.get());
        out.usize_u32(self.values.len())?;
        out.bytes(&presence)?;
        out.usize_u32(offsets.len())?;
        for offset in offsets {
            out.u32(offset);
        }
        out.bytes(&payload)?;
        Ok(out.finish())
    }

    pub fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.u16()? != STORED_FIELDS_COMPONENT_CODEC_VERSION {
            return Err(IndexError::InvalidFormat("stored-fields codec version"));
        }
        let first_doc_id = DocId::new(input.u32()?);
        let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        let presence = input.owned_bytes()?;
        if presence.len() != count.div_ceil(8) {
            return Err(IndexError::InvalidFormat("stored-fields presence length"));
        }
        let remainder = count % 8;
        if remainder != 0 && presence.last().is_some_and(|byte| *byte >> remainder != 0) {
            return Err(IndexError::InvalidFormat("stored-fields presence padding"));
        }
        let offset_count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        if offset_count != count.saturating_add(1) {
            return Err(IndexError::InvalidFormat("stored-fields offset count"));
        }
        input.claim(offset_count.saturating_mul(4))?;
        let mut offsets = Vec::with_capacity(offset_count);
        for _ in 0..offset_count {
            offsets.push(input.u32()?);
        }
        let payload = input.owned_bytes()?;
        if offsets.first() != Some(&0)
            || offsets.last().copied() != Some(payload.len() as u32)
            || offsets.windows(2).any(|pair| pair[0] > pair[1])
        {
            return Err(IndexError::InvalidFormat("stored-fields offsets"));
        }
        input.claim(
            count
                .checked_mul(std::mem::size_of::<Option<Vec<u8>>>())
                .and_then(|bytes| bytes.checked_add(payload.len()))
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        input.finish()?;
        let mut values = Vec::with_capacity(count);
        for index in 0..count {
            if presence[index / 8] & (1 << (index % 8)) == 0 {
                if offsets[index] != offsets[index + 1] {
                    return Err(IndexError::InvalidFormat(
                        "absent stored field owns payload bytes",
                    ));
                }
                values.push(None);
                continue;
            }
            let range = offsets[index] as usize..offsets[index + 1] as usize;
            values.push(Some(
                payload
                    .get(range)
                    .ok_or(IndexError::InvalidFormat("stored-fields value range"))?
                    .to_vec(),
            ));
        }
        Self::new(first_doc_id, values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_empty_and_nonempty_values_are_distinct() {
        let block = StoredFieldsBlock::new(
            DocId::new(4),
            vec![None, Some(Vec::new()), Some(b"value".to_vec())],
        )
        .unwrap();
        let decoded = StoredFieldsBlock::decode_payload(&block.encode_payload().unwrap()).unwrap();
        assert_eq!(decoded, block);
        assert_eq!(decoded.get(DocId::new(4)), None);
        assert_eq!(decoded.get(DocId::new(5)), Some(&[][..]));
        assert_eq!(decoded.get(DocId::new(6)), Some(&b"value"[..]));
    }
}
