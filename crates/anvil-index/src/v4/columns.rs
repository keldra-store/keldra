use std::cmp::Ordering;

use crate::IndexError;

use super::codec::{COMPONENT_HEADER_BYTES, Decoder, Encoder};
use super::model::{DocId, INDEX_COMPONENT_BYTES};
use super::schema::FieldId;

const FAST_COLUMN_CODEC_VERSION: u16 = 1;
const MAX_PAYLOAD_BYTES: usize = INDEX_COMPONENT_BYTES - COMPONENT_HEADER_BYTES;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    /// Canonical finite IEEE-754 binary64 bits. Negative zero is normalized.
    Number(u64),
    Unsigned(u64),
    String(String),
}

impl ScalarValue {
    pub fn number(value: f64) -> Result<Self, IndexError> {
        if !value.is_finite() {
            return Err(IndexError::InvalidDefinition(
                "format-v4 numbers must be finite".into(),
            ));
        }
        Ok(Self::Number(if value == 0.0 {
            0.0f64.to_bits()
        } else {
            value.to_bits()
        }))
    }

    pub fn as_number(&self) -> Option<f64> {
        match self {
            Self::Number(bits) => Some(f64::from_bits(*bits)),
            _ => None,
        }
    }

    fn tag(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::Number(_) => 2,
            Self::Unsigned(_) => 3,
            Self::String(_) => 4,
        }
    }
}

impl Ord for ScalarValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tag()
            .cmp(&other.tag())
            .then_with(|| match (self, other) {
                (Self::Null, Self::Null) => Ordering::Equal,
                (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
                (Self::Number(left), Self::Number(right)) => {
                    f64::from_bits(*left).total_cmp(&f64::from_bits(*right))
                }
                (Self::Unsigned(left), Self::Unsigned(right)) => left.cmp(right),
                (Self::String(left), Self::String(right)) => left.as_bytes().cmp(right.as_bytes()),
                _ => Ordering::Equal,
            })
    }
}

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FastColumnCell {
    /// Distinguishes an empty multi-valued field from a missing field.
    pub present: bool,
    /// At least one explicit JSON null occurred.
    pub null: bool,
    /// Non-null values in source order.
    pub values: Vec<ScalarValue>,
}

impl FastColumnCell {
    pub fn missing() -> Self {
        Self {
            present: false,
            null: false,
            values: Vec::new(),
        }
    }

    pub fn null() -> Self {
        Self {
            present: true,
            null: true,
            values: Vec::new(),
        }
    }

    pub fn value(value: ScalarValue) -> Self {
        Self {
            present: true,
            null: false,
            values: vec![value],
        }
    }

    pub fn validate(&self, multi_valued: bool) -> Result<(), IndexError> {
        validate_cell(self, multi_valued)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FastColumnBlock {
    pub field_id: FieldId,
    pub first_doc_id: DocId,
    pub multi_valued: bool,
    cells: Vec<FastColumnCell>,
    pub value_count: u32,
    pub null_count: u32,
    pub minimum: Option<ScalarValue>,
    pub maximum: Option<ScalarValue>,
}

impl FastColumnBlock {
    pub fn new(
        field_id: FieldId,
        first_doc_id: DocId,
        multi_valued: bool,
        cells: Vec<FastColumnCell>,
    ) -> Result<Self, IndexError> {
        if cells.is_empty() {
            return Err(IndexError::InvalidDefinition(
                "fast-column block must not be empty".into(),
            ));
        }
        first_doc_id
            .get()
            .checked_add(u32::try_from(cells.len() - 1).map_err(|_| IndexError::OffsetOverflow)?)
            .ok_or(IndexError::OffsetOverflow)?;
        let mut value_count = 0u32;
        let mut null_count = 0u32;
        let (mut minimum, mut maximum) = (None, None);
        for cell in &cells {
            validate_cell(cell, multi_valued)?;
            null_count = null_count
                .checked_add(u32::from(cell.null))
                .ok_or(IndexError::OffsetOverflow)?;
            value_count = value_count
                .checked_add(
                    u32::try_from(cell.values.len()).map_err(|_| IndexError::OffsetOverflow)?,
                )
                .ok_or(IndexError::OffsetOverflow)?;
            for value in &cell.values {
                if minimum.as_ref().is_none_or(|current| value < current) {
                    minimum = Some(value.clone());
                }
                if maximum.as_ref().is_none_or(|current| value > current) {
                    maximum = Some(value.clone());
                }
            }
        }
        let block = Self {
            field_id,
            first_doc_id,
            multi_valued,
            cells,
            value_count,
            null_count,
            minimum,
            maximum,
        };
        let needed = block.encode_payload()?.len();
        if needed > MAX_PAYLOAD_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: needed + COMPONENT_HEADER_BYTES,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        Ok(block)
    }

    pub fn cells(&self) -> &[FastColumnCell] {
        &self.cells
    }

    pub fn get(&self, doc_id: DocId) -> Option<&FastColumnCell> {
        let offset = doc_id.get().checked_sub(self.first_doc_id.get())?;
        self.cells.get(offset as usize)
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        let mut presence = vec![0u8; self.cells.len().div_ceil(8)];
        let mut nulls = vec![0u8; self.cells.len().div_ceil(8)];
        let mut value_count = 0usize;
        for (doc, cell) in self.cells.iter().enumerate() {
            validate_cell(cell, self.multi_valued)?;
            if cell.present {
                presence[doc / 8] |= 1 << (doc % 8);
            }
            if cell.null {
                nulls[doc / 8] |= 1 << (doc % 8);
            }
            value_count = value_count
                .checked_add(cell.values.len())
                .ok_or(IndexError::OffsetOverflow)?;
        }
        if u32::try_from(value_count).map_err(|_| IndexError::OffsetOverflow)? != self.value_count {
            return Err(IndexError::InvalidDefinition(
                "fast-column value count differs from its cells".into(),
            ));
        }
        let mut out = Encoder::default();
        out.u16(FAST_COLUMN_CODEC_VERSION);
        out.u32(self.field_id.get());
        out.u32(self.first_doc_id.get());
        out.usize_u32(self.cells.len())?;
        out.bool(self.multi_valued);
        out.u32(self.value_count);
        out.u32(self.null_count);
        encode_optional_scalar(&mut out, self.minimum.as_ref())?;
        encode_optional_scalar(&mut out, self.maximum.as_ref())?;
        out.bytes(&presence)?;
        out.bytes(&nulls)?;
        out.usize_u32(
            self.cells
                .len()
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut offset = 0usize;
        out.u32(0);
        for cell in &self.cells {
            offset = offset
                .checked_add(cell.values.len())
                .ok_or(IndexError::OffsetOverflow)?;
            out.u32(u32::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?);
        }
        out.usize_u32(value_count)?;
        for cell in &self.cells {
            for value in &cell.values {
                encode_scalar(&mut out, value)?;
            }
        }
        Ok(out.finish())
    }

    pub fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.u16()? != FAST_COLUMN_CODEC_VERSION {
            return Err(IndexError::InvalidFormat("fast-column codec version"));
        }
        let field_id = FieldId::new(input.u32()?);
        let first_doc_id = DocId::new(input.u32()?);
        let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        let multi_valued = input.bool()?;
        let encoded_value_count = input.u32()?;
        let encoded_null_count = input.u32()?;
        let encoded_minimum = decode_optional_scalar(&mut input)?;
        let encoded_maximum = decode_optional_scalar(&mut input)?;
        let presence = input.owned_bytes()?;
        let nulls = input.owned_bytes()?;
        if presence.len() != count.div_ceil(8) || nulls.len() != count.div_ceil(8) {
            return Err(IndexError::InvalidFormat("fast-column bitmap length"));
        }
        validate_padding(&presence, count)?;
        validate_padding(&nulls, count)?;
        let offset_count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        if offset_count != count.saturating_add(1) {
            return Err(IndexError::InvalidFormat("fast-column offset count"));
        }
        input.claim(offset_count.saturating_mul(4))?;
        let mut offsets = Vec::with_capacity(offset_count);
        for _ in 0..offset_count {
            offsets.push(input.u32()?);
        }
        let value_count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        input.claim(value_count.saturating_mul(std::mem::size_of::<ScalarValue>()))?;
        let mut values = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            values.push(decode_scalar(&mut input)?);
        }
        let cloned_string_bytes = values.iter().try_fold(0usize, |sum, value| {
            sum.checked_add(match value {
                ScalarValue::String(value) => value.len(),
                _ => 0,
            })
            .ok_or(IndexError::OffsetOverflow)
        })?;
        input.claim(
            count
                .checked_mul(std::mem::size_of::<FastColumnCell>())
                .and_then(|bytes| {
                    bytes
                        .checked_add(value_count.saturating_mul(std::mem::size_of::<ScalarValue>()))
                })
                .and_then(|bytes| bytes.checked_add(cloned_string_bytes))
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        input.finish()?;
        if offsets.first() != Some(&0)
            || offsets.last().copied() != Some(value_count as u32)
            || offsets.windows(2).any(|pair| pair[0] > pair[1])
        {
            return Err(IndexError::InvalidFormat("fast-column offsets"));
        }
        let mut cells = Vec::with_capacity(count);
        for doc in 0..count {
            let start = offsets[doc] as usize;
            let end = offsets[doc + 1] as usize;
            let present = presence[doc / 8] & (1 << (doc % 8)) != 0;
            let null = nulls[doc / 8] & (1 << (doc % 8)) != 0;
            cells.push(FastColumnCell {
                present,
                null,
                values: values
                    .get(start..end)
                    .ok_or(IndexError::InvalidFormat("fast-column value range"))?
                    .to_vec(),
            });
        }
        let block = Self::new(field_id, first_doc_id, multi_valued, cells)?;
        if block.value_count != encoded_value_count
            || block.null_count != encoded_null_count
            || block.minimum != encoded_minimum
            || block.maximum != encoded_maximum
        {
            return Err(IndexError::InvalidFormat("fast-column statistics"));
        }
        Ok(block)
    }
}

fn validate_cell(cell: &FastColumnCell, multi: bool) -> Result<(), IndexError> {
    if (!cell.present && (cell.null || !cell.values.is_empty()))
        || (!multi && usize::from(cell.null) + cell.values.len() > 1)
        || cell
            .values
            .iter()
            .any(|value| matches!(value, ScalarValue::Null))
    {
        return Err(IndexError::InvalidDefinition(
            "fast-column missing/null/cardinality state is invalid".into(),
        ));
    }
    Ok(())
}

fn encode_optional_scalar(
    out: &mut Encoder,
    value: Option<&ScalarValue>,
) -> Result<(), IndexError> {
    out.bool(value.is_some());
    if let Some(value) = value {
        encode_scalar(out, value)?;
    }
    Ok(())
}

fn decode_optional_scalar(input: &mut Decoder<'_>) -> Result<Option<ScalarValue>, IndexError> {
    input.bool()?.then(|| decode_scalar(input)).transpose()
}

pub(crate) fn encode_scalar(out: &mut Encoder, value: &ScalarValue) -> Result<(), IndexError> {
    out.u8(value.tag());
    match value {
        ScalarValue::Null => {}
        ScalarValue::Boolean(value) => out.bool(*value),
        ScalarValue::Number(bits) => {
            let value = f64::from_bits(*bits);
            if !value.is_finite() || value == 0.0 && *bits != 0.0f64.to_bits() {
                return Err(IndexError::InvalidDefinition(
                    "non-canonical format-v4 number".into(),
                ));
            }
            out.u64(*bits);
        }
        ScalarValue::Unsigned(value) => out.u64(*value),
        ScalarValue::String(value) => out.string(value)?,
    }
    Ok(())
}

pub(crate) fn decode_scalar(input: &mut Decoder<'_>) -> Result<ScalarValue, IndexError> {
    let value = match input.u8()? {
        0 => ScalarValue::Null,
        1 => ScalarValue::Boolean(input.bool()?),
        2 => ScalarValue::Number(input.u64()?),
        3 => ScalarValue::Unsigned(input.u64()?),
        4 => ScalarValue::String(input.string()?),
        _ => return Err(IndexError::InvalidFormat("fast-column scalar tag")),
    };
    let mut sink = Encoder::default();
    encode_scalar(&mut sink, &value)
        .map_err(|_| IndexError::InvalidFormat("fast-column scalar"))?;
    Ok(value)
}

fn validate_padding(bitmap: &[u8], count: usize) -> Result<(), IndexError> {
    let remainder = count % 8;
    if remainder != 0 && bitmap.last().is_some_and(|byte| *byte >> remainder != 0) {
        return Err(IndexError::InvalidFormat("fast-column bitmap padding"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_with_flattened_values(block: &FastColumnBlock) -> Vec<u8> {
        let mut presence = vec![0u8; block.cells.len().div_ceil(8)];
        let mut nulls = vec![0u8; block.cells.len().div_ceil(8)];
        let mut offsets = Vec::with_capacity(block.cells.len() + 1);
        let mut values = Vec::new();
        offsets.push(0u32);
        for (doc, cell) in block.cells.iter().enumerate() {
            if cell.present {
                presence[doc / 8] |= 1 << (doc % 8);
            }
            if cell.null {
                nulls[doc / 8] |= 1 << (doc % 8);
            }
            values.extend(cell.values.iter().cloned());
            offsets.push(values.len() as u32);
        }
        let mut out = Encoder::default();
        out.u16(FAST_COLUMN_CODEC_VERSION);
        out.u32(block.field_id.get());
        out.u32(block.first_doc_id.get());
        out.usize_u32(block.cells.len()).unwrap();
        out.bool(block.multi_valued);
        out.u32(block.value_count);
        out.u32(block.null_count);
        encode_optional_scalar(&mut out, block.minimum.as_ref()).unwrap();
        encode_optional_scalar(&mut out, block.maximum.as_ref()).unwrap();
        out.bytes(&presence).unwrap();
        out.bytes(&nulls).unwrap();
        out.usize_u32(offsets.len()).unwrap();
        for offset in offsets {
            out.u32(offset);
        }
        out.usize_u32(values.len()).unwrap();
        for value in &values {
            encode_scalar(&mut out, value).unwrap();
        }
        out.finish()
    }

    #[test]
    fn missing_null_empty_and_multivalue_remain_distinct() {
        let block = FastColumnBlock::new(
            FieldId::new(2),
            DocId::new(10),
            true,
            vec![
                FastColumnCell::missing(),
                FastColumnCell::null(),
                FastColumnCell {
                    present: true,
                    null: false,
                    values: vec![],
                },
                FastColumnCell {
                    present: true,
                    null: true,
                    values: vec![ScalarValue::Unsigned(7), ScalarValue::String("x".into())],
                },
            ],
        )
        .unwrap();
        let encoded = block.encode_payload().unwrap();
        assert_eq!(encoded, encode_with_flattened_values(&block));
        let decoded = FastColumnBlock::decode_payload(&encoded).unwrap();
        assert_eq!(decoded, block);
        assert!(!decoded.get(DocId::new(10)).unwrap().present);
        assert!(decoded.get(DocId::new(11)).unwrap().null);
        assert!(decoded.get(DocId::new(12)).unwrap().values.is_empty());
    }

    #[test]
    fn tagged_total_order_and_negative_zero_are_canonical() {
        assert!(ScalarValue::Null < ScalarValue::Boolean(false));
        assert!(ScalarValue::Boolean(true) < ScalarValue::number(-1.0).unwrap());
        assert!(ScalarValue::number(100.0).unwrap() < ScalarValue::Unsigned(0));
        assert_eq!(
            ScalarValue::number(-0.0).unwrap(),
            ScalarValue::number(0.0).unwrap()
        );
        assert!(ScalarValue::number(f64::NAN).is_err());
    }
}
