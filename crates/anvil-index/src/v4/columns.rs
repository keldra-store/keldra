use std::cmp::Ordering;

use crate::IndexError;

use super::codec::{COMPONENT_HEADER_BYTES, Decoder, Encoder};
use super::model::{DocId, INDEX_COMPONENT_BYTES, INDEX_TERM_BYTES};
use super::schema::FieldId;

pub const DOC_VALUES_COMPONENT_CODEC_VERSION: u16 = 1;
const MAX_PAYLOAD_BYTES: usize = INDEX_COMPONENT_BYTES - COMPONENT_HEADER_BYTES;
const FLAT_VALUES_ENCODING: u8 = 0;
const KEYWORD_ORDINALS_ENCODING: u8 = 1;
const STRING_SCALAR_TAG: u8 = 5;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Signed(i64),
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
            Self::Signed(_) => 2,
            Self::Unsigned(_) => 3,
            Self::Number(_) => 4,
            Self::String(_) => 5,
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
                (Self::Signed(left), Self::Signed(right)) => left.cmp(right),
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
pub struct DocValueCell {
    /// Distinguishes an empty multi-valued field from a missing field.
    pub present: bool,
    /// At least one explicit JSON null occurred.
    pub null: bool,
    /// Non-null values. Multi-valued keywords are canonicalized into sorted,
    /// distinct UTF-8 byte order; other types retain source order.
    pub values: Vec<ScalarValue>,
}

impl DocValueCell {
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
pub struct DocValueBlock {
    pub field_id: FieldId,
    pub first_doc_id: DocId,
    pub multi_valued: bool,
    cells: Vec<DocValueCell>,
    pub value_count: u32,
    pub null_count: u32,
    pub minimum: Option<ScalarValue>,
    pub maximum: Option<ScalarValue>,
}

impl DocValueBlock {
    pub fn new(
        field_id: FieldId,
        first_doc_id: DocId,
        multi_valued: bool,
        mut cells: Vec<DocValueCell>,
    ) -> Result<Self, IndexError> {
        if cells.is_empty() {
            return Err(IndexError::InvalidDefinition(
                "doc-value block must not be empty".into(),
            ));
        }
        first_doc_id
            .get()
            .checked_add(u32::try_from(cells.len() - 1).map_err(|_| IndexError::OffsetOverflow)?)
            .ok_or(IndexError::OffsetOverflow)?;
        let mut value_type = None::<u8>;
        for cell in &cells {
            validate_cell(cell, multi_valued)?;
            for value in &cell.values {
                if value_type.is_some_and(|expected| expected != value.tag()) {
                    return Err(IndexError::InvalidDefinition(
                        "one doc-value block cannot mix scalar types".into(),
                    ));
                }
                value_type = Some(value.tag());
            }
        }
        if value_type == Some(STRING_SCALAR_TAG) {
            for cell in &mut cells {
                cell.values.sort_unstable();
                cell.values.dedup();
            }
        }
        let mut value_count = 0u32;
        let mut null_count = 0u32;
        let (mut minimum, mut maximum) = (None, None);
        for cell in &cells {
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

    pub fn cells(&self) -> &[DocValueCell] {
        &self.cells
    }

    pub fn get(&self, doc_id: DocId) -> Option<&DocValueCell> {
        let offset = doc_id.get().checked_sub(self.first_doc_id.get())?;
        self.cells.get(offset as usize)
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        let (presence, nulls, value_count, null_count) =
            encode_cell_metadata(&self.cells, self.multi_valued)?;
        if u32::try_from(value_count).map_err(|_| IndexError::OffsetOverflow)? != self.value_count {
            return Err(IndexError::InvalidDefinition(
                "doc-value count differs from its cells".into(),
            ));
        }
        if u32::try_from(null_count).map_err(|_| IndexError::OffsetOverflow)? != self.null_count {
            return Err(IndexError::InvalidDefinition(
                "doc-value null count differs from its cells".into(),
            ));
        }
        let mut out = Encoder::default();
        out.u16(DOC_VALUES_COMPONENT_CODEC_VERSION);
        out.u32(self.field_id.get());
        out.u32(self.first_doc_id.get());
        out.usize_u32(self.cells.len())?;
        out.bool(self.multi_valued);
        out.u32(self.value_count);
        out.u32(self.null_count);
        let keyword = value_count != 0
            && self
                .cells
                .iter()
                .flat_map(|cell| &cell.values)
                .all(|value| matches!(value, ScalarValue::String(_)));
        out.u8(if keyword {
            KEYWORD_ORDINALS_ENCODING
        } else {
            FLAT_VALUES_ENCODING
        });
        if keyword {
            encode_keyword_values(self, &mut out, &presence, &nulls)?;
        } else {
            encode_flat_values(self, &mut out, &presence, &nulls, value_count)?;
        }
        Ok(out.finish())
    }

    pub fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.u16()? != DOC_VALUES_COMPONENT_CODEC_VERSION {
            return Err(IndexError::InvalidFormat("doc-value codec version"));
        }
        let field_id = FieldId::new(input.u32()?);
        let first_doc_id = DocId::new(input.u32()?);
        let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        let multi_valued = input.bool()?;
        let encoded_value_count = input.u32()?;
        let encoded_null_count = input.u32()?;
        let block = match input.u8()? {
            FLAT_VALUES_ENCODING => decode_flat_values(
                &mut input,
                field_id,
                first_doc_id,
                count,
                multi_valued,
                encoded_value_count,
                encoded_null_count,
            )?,
            KEYWORD_ORDINALS_ENCODING => decode_keyword_values(
                &mut input,
                field_id,
                first_doc_id,
                count,
                multi_valued,
                encoded_value_count,
                encoded_null_count,
            )?,
            _ => return Err(IndexError::InvalidFormat("doc-value encoding")),
        };
        input.finish()?;
        Ok(block)
    }
}

fn encode_flat_values(
    block: &DocValueBlock,
    out: &mut Encoder,
    presence: &[u8],
    nulls: &[u8],
    value_count: usize,
) -> Result<(), IndexError> {
    if block
        .cells
        .iter()
        .flat_map(|cell| &cell.values)
        .any(|value| matches!(value, ScalarValue::String(_)))
    {
        return Err(IndexError::InvalidDefinition(
            "keyword doc values require ordinal encoding".into(),
        ));
    }
    let (minimum, maximum) = scalar_bounds(&block.cells);
    if block.minimum != minimum || block.maximum != maximum {
        return Err(IndexError::InvalidDefinition(
            "doc-value statistics differ from their cells".into(),
        ));
    }
    encode_optional_scalar(out, block.minimum.as_ref())?;
    encode_optional_scalar(out, block.maximum.as_ref())?;
    encode_presence_offsets(out, &block.cells, presence, nulls)?;
    out.usize_u32(value_count)?;
    for cell in &block.cells {
        for value in &cell.values {
            encode_scalar(out, value)?;
        }
    }
    Ok(())
}

fn encode_keyword_values(
    block: &DocValueBlock,
    out: &mut Encoder,
    presence: &[u8],
    nulls: &[u8],
) -> Result<(), IndexError> {
    let mut dictionary = block
        .cells
        .iter()
        .flat_map(|cell| &cell.values)
        .map(|value| match value {
            ScalarValue::String(value) => Ok(value.as_str()),
            _ => Err(IndexError::InvalidDefinition(
                "keyword doc-value block mixed scalar types".into(),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    dictionary.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    dictionary.dedup();
    if dictionary.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "keyword ordinal encoding requires values".into(),
        ));
    }
    let minimum = ScalarValue::String(dictionary[0].to_owned());
    let maximum = ScalarValue::String(dictionary[dictionary.len() - 1].to_owned());
    if block.minimum.as_ref() != Some(&minimum) || block.maximum.as_ref() != Some(&maximum) {
        return Err(IndexError::InvalidDefinition(
            "keyword doc-value statistics differ from their dictionary".into(),
        ));
    }
    out.usize_u32(dictionary.len())?;
    for value in &dictionary {
        out.string(value)?;
    }
    encode_optional_ordinal(out, Some(0));
    encode_optional_ordinal(
        out,
        Some(u32::try_from(dictionary.len() - 1).map_err(|_| IndexError::OffsetOverflow)?),
    );
    encode_presence_offsets(out, &block.cells, presence, nulls)?;
    out.u32(block.value_count);
    for cell in &block.cells {
        let mut previous = None;
        for value in &cell.values {
            let ScalarValue::String(value) = value else {
                return Err(IndexError::InvalidDefinition(
                    "keyword doc-value block mixed scalar types".into(),
                ));
            };
            let ordinal = dictionary
                .binary_search_by(|candidate| candidate.as_bytes().cmp(value.as_bytes()))
                .map_err(|_| IndexError::InvalidDefinition("keyword dictionary mismatch".into()))?;
            let ordinal = u32::try_from(ordinal).map_err(|_| IndexError::OffsetOverflow)?;
            if previous.is_some_and(|previous| previous >= ordinal) {
                return Err(IndexError::InvalidDefinition(
                    "multi-valued keyword ordinals must be sorted and distinct".into(),
                ));
            }
            out.u32(ordinal);
            previous = Some(ordinal);
        }
    }
    Ok(())
}

fn encode_cell_metadata(
    cells: &[DocValueCell],
    multi_valued: bool,
) -> Result<(Vec<u8>, Vec<u8>, usize, usize), IndexError> {
    let mut presence = vec![0u8; cells.len().div_ceil(8)];
    let mut nulls = vec![0u8; cells.len().div_ceil(8)];
    let mut value_count = 0usize;
    let mut null_count = 0usize;
    for (doc, cell) in cells.iter().enumerate() {
        validate_cell(cell, multi_valued)?;
        if cell.present {
            presence[doc / 8] |= 1 << (doc % 8);
        }
        if cell.null {
            nulls[doc / 8] |= 1 << (doc % 8);
            null_count = null_count
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        value_count = value_count
            .checked_add(cell.values.len())
            .ok_or(IndexError::OffsetOverflow)?;
    }
    Ok((presence, nulls, value_count, null_count))
}

fn encode_presence_offsets(
    out: &mut Encoder,
    cells: &[DocValueCell],
    presence: &[u8],
    nulls: &[u8],
) -> Result<(), IndexError> {
    out.bytes(presence)?;
    out.bytes(nulls)?;
    out.usize_u32(
        cells
            .len()
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let mut offset = 0usize;
    out.u32(0);
    for cell in cells {
        offset = offset
            .checked_add(cell.values.len())
            .ok_or(IndexError::OffsetOverflow)?;
        out.u32(u32::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn decode_flat_values(
    input: &mut Decoder<'_>,
    field_id: FieldId,
    first_doc_id: DocId,
    count: usize,
    multi_valued: bool,
    encoded_value_count: u32,
    encoded_null_count: u32,
) -> Result<DocValueBlock, IndexError> {
    let encoded_minimum = decode_optional_scalar(input)?;
    let encoded_maximum = decode_optional_scalar(input)?;
    let (presence, nulls, offsets) = decode_presence_offsets(input, count)?;
    let value_count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
    input.claim(
        value_count
            .checked_mul(std::mem::size_of::<ScalarValue>())
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        let value = decode_scalar(input)?;
        if matches!(value, ScalarValue::String(_)) {
            return Err(IndexError::InvalidFormat(
                "keyword doc value used flat encoding",
            ));
        }
        values.push(value);
    }
    let cells = decode_cells(input, count, &presence, &nulls, &offsets, &values)?;
    let block = DocValueBlock::new(field_id, first_doc_id, multi_valued, cells)?;
    if block.value_count != encoded_value_count
        || block.null_count != encoded_null_count
        || block.minimum != encoded_minimum
        || block.maximum != encoded_maximum
    {
        return Err(IndexError::InvalidFormat("doc-value statistics"));
    }
    Ok(block)
}

#[allow(clippy::too_many_arguments)]
fn decode_keyword_values(
    input: &mut Decoder<'_>,
    field_id: FieldId,
    first_doc_id: DocId,
    count: usize,
    multi_valued: bool,
    encoded_value_count: u32,
    encoded_null_count: u32,
) -> Result<DocValueBlock, IndexError> {
    let dictionary_count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
    if dictionary_count == 0 {
        return Err(IndexError::InvalidFormat("empty keyword dictionary"));
    }
    input.claim(
        dictionary_count
            .checked_mul(std::mem::size_of::<String>())
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let mut dictionary = Vec::with_capacity(dictionary_count);
    for _ in 0..dictionary_count {
        let value = input.string()?;
        if value.len() > INDEX_TERM_BYTES {
            return Err(IndexError::InvalidFormat("doc-value keyword length"));
        }
        dictionary.push(value);
    }
    if dictionary
        .windows(2)
        .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
    {
        return Err(IndexError::InvalidFormat("keyword dictionary order"));
    }
    let encoded_minimum = decode_optional_ordinal(input)?;
    let encoded_maximum = decode_optional_ordinal(input)?;
    if encoded_minimum != Some(0)
        || encoded_maximum
            != Some(u32::try_from(dictionary.len() - 1).map_err(|_| IndexError::OffsetOverflow)?)
    {
        return Err(IndexError::InvalidFormat("keyword ordinal statistics"));
    }
    let (presence, nulls, offsets) = decode_presence_offsets(input, count)?;
    let ordinal_count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
    input.claim(
        ordinal_count
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let mut ordinals = Vec::with_capacity(ordinal_count);
    for _ in 0..ordinal_count {
        let ordinal = input.u32()?;
        if usize::try_from(ordinal)
            .ok()
            .is_none_or(|ordinal| ordinal >= dictionary.len())
        {
            return Err(IndexError::InvalidFormat("keyword ordinal"));
        }
        ordinals.push(ordinal);
    }
    validate_offsets(&offsets, ordinal_count)?;
    let cloned_bytes = ordinals.iter().try_fold(0usize, |sum, ordinal| {
        let ordinal = usize::try_from(*ordinal).map_err(|_| IndexError::OffsetOverflow)?;
        sum.checked_add(dictionary[ordinal].len())
            .ok_or(IndexError::OffsetOverflow)
    })?;
    let statistics_bytes = dictionary[0]
        .len()
        .checked_add(dictionary[dictionary.len() - 1].len())
        .ok_or(IndexError::OffsetOverflow)?;
    input.claim(
        count
            .checked_mul(std::mem::size_of::<DocValueCell>())
            .and_then(|bytes| {
                ordinal_count
                    .checked_mul(std::mem::size_of::<ScalarValue>())
                    .and_then(|values| bytes.checked_add(values))
            })
            .and_then(|bytes| bytes.checked_add(cloned_bytes))
            .and_then(|bytes| bytes.checked_add(statistics_bytes))
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let mut cells = Vec::with_capacity(count);
    for doc in 0..count {
        let start = usize::try_from(offsets[doc]).map_err(|_| IndexError::OffsetOverflow)?;
        let end = usize::try_from(offsets[doc + 1]).map_err(|_| IndexError::OffsetOverflow)?;
        let document_ordinals = ordinals
            .get(start..end)
            .ok_or(IndexError::InvalidFormat("keyword ordinal range"))?;
        if document_ordinals.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(IndexError::InvalidFormat(
                "multi-valued keyword ordinal order",
            ));
        }
        cells.push(DocValueCell {
            present: bit(&presence, doc),
            null: bit(&nulls, doc),
            values: document_ordinals
                .iter()
                .map(|ordinal| {
                    usize::try_from(*ordinal)
                        .map(|ordinal| ScalarValue::String(dictionary[ordinal].clone()))
                        .map_err(|_| IndexError::OffsetOverflow)
                })
                .collect::<Result<Vec<_>, _>>()?,
        });
    }
    let block = DocValueBlock::new(field_id, first_doc_id, multi_valued, cells)?;
    if block.value_count != encoded_value_count
        || block.null_count != encoded_null_count
        || block.minimum != Some(ScalarValue::String(dictionary[0].clone()))
        || block.maximum
            != Some(ScalarValue::String(
                dictionary[dictionary.len() - 1].clone(),
            ))
    {
        return Err(IndexError::InvalidFormat("keyword doc-value statistics"));
    }
    Ok(block)
}

fn decode_presence_offsets(
    input: &mut Decoder<'_>,
    count: usize,
) -> Result<(Vec<u8>, Vec<u8>, Vec<u32>), IndexError> {
    let presence = input.owned_bytes()?;
    let nulls = input.owned_bytes()?;
    if presence.len() != count.div_ceil(8) || nulls.len() != count.div_ceil(8) {
        return Err(IndexError::InvalidFormat("doc-value bitmap length"));
    }
    validate_padding(&presence, count)?;
    validate_padding(&nulls, count)?;
    let offset_count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
    if offset_count != count.checked_add(1).ok_or(IndexError::OffsetOverflow)? {
        return Err(IndexError::InvalidFormat("doc-value offset count"));
    }
    input.claim(
        offset_count
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let mut offsets = Vec::with_capacity(offset_count);
    for _ in 0..offset_count {
        offsets.push(input.u32()?);
    }
    Ok((presence, nulls, offsets))
}

fn decode_cells(
    input: &mut Decoder<'_>,
    count: usize,
    presence: &[u8],
    nulls: &[u8],
    offsets: &[u32],
    values: &[ScalarValue],
) -> Result<Vec<DocValueCell>, IndexError> {
    validate_offsets(offsets, values.len())?;
    input.claim(
        count
            .checked_mul(std::mem::size_of::<DocValueCell>())
            .and_then(|bytes| {
                values
                    .len()
                    .checked_mul(std::mem::size_of::<ScalarValue>())
                    .and_then(|values| bytes.checked_add(values))
            })
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let mut cells = Vec::with_capacity(count);
    for doc in 0..count {
        let start = usize::try_from(offsets[doc]).map_err(|_| IndexError::OffsetOverflow)?;
        let end = usize::try_from(offsets[doc + 1]).map_err(|_| IndexError::OffsetOverflow)?;
        cells.push(DocValueCell {
            present: bit(presence, doc),
            null: bit(nulls, doc),
            values: values
                .get(start..end)
                .ok_or(IndexError::InvalidFormat("doc-value value range"))?
                .to_vec(),
        });
    }
    Ok(cells)
}

fn validate_offsets(offsets: &[u32], value_count: usize) -> Result<(), IndexError> {
    if offsets.first() != Some(&0)
        || offsets.last().copied() != u32::try_from(value_count).ok()
        || offsets.windows(2).any(|pair| pair[0] > pair[1])
    {
        return Err(IndexError::InvalidFormat("doc-value offsets"));
    }
    Ok(())
}

fn scalar_bounds(cells: &[DocValueCell]) -> (Option<ScalarValue>, Option<ScalarValue>) {
    let mut values = cells.iter().flat_map(|cell| &cell.values);
    let Some(first) = values.next() else {
        return (None, None);
    };
    let (mut minimum, mut maximum) = (first, first);
    for value in values {
        minimum = minimum.min(value);
        maximum = maximum.max(value);
    }
    (Some(minimum.clone()), Some(maximum.clone()))
}

fn bit(bitmap: &[u8], index: usize) -> bool {
    bitmap[index / 8] & (1 << (index % 8)) != 0
}

fn encode_optional_ordinal(out: &mut Encoder, value: Option<u32>) {
    out.bool(value.is_some());
    if let Some(value) = value {
        out.u32(value);
    }
}

fn decode_optional_ordinal(input: &mut Decoder<'_>) -> Result<Option<u32>, IndexError> {
    input.bool()?.then(|| input.u32()).transpose()
}

fn validate_cell(cell: &DocValueCell, multi: bool) -> Result<(), IndexError> {
    if (!cell.present && (cell.null || !cell.values.is_empty()))
        || (!multi && usize::from(cell.null) + cell.values.len() > 1)
        || cell.values.iter().any(|value| {
            matches!(value, ScalarValue::Null)
                || matches!(value, ScalarValue::String(value) if value.len() > INDEX_TERM_BYTES)
        })
    {
        return Err(IndexError::InvalidDefinition(
            "doc-value missing/null/cardinality state is invalid".into(),
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
        ScalarValue::Signed(value) => out.u64(*value as u64),
        ScalarValue::Unsigned(value) => out.u64(*value),
        ScalarValue::Number(bits) => {
            let value = f64::from_bits(*bits);
            if !value.is_finite() || value == 0.0 && *bits != 0.0f64.to_bits() {
                return Err(IndexError::InvalidDefinition(
                    "non-canonical format-v4 number".into(),
                ));
            }
            out.u64(*bits);
        }
        ScalarValue::String(value) => {
            if value.len() > INDEX_TERM_BYTES {
                return Err(IndexError::ResourceLimit {
                    needed: value.len(),
                    limit: INDEX_TERM_BYTES,
                });
            }
            out.string(value)?;
        }
    }
    Ok(())
}

pub(crate) fn decode_scalar(input: &mut Decoder<'_>) -> Result<ScalarValue, IndexError> {
    let value = match input.u8()? {
        0 => ScalarValue::Null,
        1 => ScalarValue::Boolean(input.bool()?),
        2 => ScalarValue::Signed(input.u64()? as i64),
        3 => ScalarValue::Unsigned(input.u64()?),
        4 => ScalarValue::Number(input.u64()?),
        5 => ScalarValue::String(input.string()?),
        _ => return Err(IndexError::InvalidFormat("doc-value scalar tag")),
    };
    let mut sink = Encoder::default();
    encode_scalar(&mut sink, &value).map_err(|_| IndexError::InvalidFormat("doc-value scalar"))?;
    if matches!(&value, ScalarValue::String(value) if value.len() > INDEX_TERM_BYTES) {
        return Err(IndexError::InvalidFormat("doc-value keyword length"));
    }
    Ok(value)
}

fn validate_padding(bitmap: &[u8], count: usize) -> Result<(), IndexError> {
    let remainder = count % 8;
    if remainder != 0 && bitmap.last().is_some_and(|byte| *byte >> remainder != 0) {
        return Err(IndexError::InvalidFormat("doc-value bitmap padding"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_with_flattened_values(block: &DocValueBlock) -> Vec<u8> {
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
        out.u16(DOC_VALUES_COMPONENT_CODEC_VERSION);
        out.u32(block.field_id.get());
        out.u32(block.first_doc_id.get());
        out.usize_u32(block.cells.len()).unwrap();
        out.bool(block.multi_valued);
        out.u32(block.value_count);
        out.u32(block.null_count);
        out.u8(FLAT_VALUES_ENCODING);
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
        let block = DocValueBlock::new(
            FieldId::new(2),
            DocId::new(10),
            true,
            vec![
                DocValueCell::missing(),
                DocValueCell::null(),
                DocValueCell {
                    present: true,
                    null: false,
                    values: vec![],
                },
                DocValueCell {
                    present: true,
                    null: true,
                    values: vec![ScalarValue::Unsigned(7), ScalarValue::Unsigned(9)],
                },
            ],
        )
        .unwrap();
        let encoded = block.encode_payload().unwrap();
        assert_eq!(encoded, encode_with_flattened_values(&block));
        let decoded = DocValueBlock::decode_payload(&encoded).unwrap();
        assert_eq!(decoded, block);
        assert!(!decoded.get(DocId::new(10)).unwrap().present);
        assert!(decoded.get(DocId::new(11)).unwrap().null);
        assert!(decoded.get(DocId::new(12)).unwrap().values.is_empty());
    }

    #[test]
    fn tagged_total_order_and_negative_zero_are_canonical() {
        assert!(ScalarValue::Null < ScalarValue::Boolean(false));
        assert!(ScalarValue::Boolean(true) < ScalarValue::Signed(-1));
        assert!(ScalarValue::Signed(100) < ScalarValue::Unsigned(0));
        assert!(ScalarValue::Unsigned(0) < ScalarValue::number(-1.0).unwrap());
        assert_eq!(
            ScalarValue::number(-0.0).unwrap(),
            ScalarValue::number(0.0).unwrap()
        );
        assert!(ScalarValue::number(f64::NAN).is_err());
    }

    #[test]
    fn keyword_dictionary_is_deterministic_distinct_and_ordinal_backed() {
        let repeated = "keyword-dictionary-value".to_owned();
        let block = DocValueBlock::new(
            FieldId::new(3),
            DocId::MIN,
            true,
            vec![
                DocValueCell {
                    present: true,
                    null: false,
                    values: vec![
                        ScalarValue::String("zeta".into()),
                        ScalarValue::String(repeated.clone()),
                        ScalarValue::String("alpha".into()),
                        ScalarValue::String(repeated.clone()),
                    ],
                },
                DocValueCell::value(ScalarValue::String(repeated.clone())),
            ],
        )
        .unwrap();
        assert_eq!(
            block.cells()[0].values,
            vec![
                ScalarValue::String("alpha".into()),
                ScalarValue::String(repeated.clone()),
                ScalarValue::String("zeta".into()),
            ]
        );
        let encoded = block.encode_payload().unwrap();
        assert_eq!(
            encoded
                .windows(repeated.len())
                .filter(|window| *window == repeated.as_bytes())
                .count(),
            1,
            "dictionary values, including min/max and repeated cells, serialize once",
        );
        assert_eq!(DocValueBlock::decode_payload(&encoded).unwrap(), block);
        assert_eq!(block.encode_payload().unwrap(), encoded);
    }

    #[test]
    fn keyword_ordinals_fail_closed_on_corruption() {
        let block = DocValueBlock::new(
            FieldId::new(4),
            DocId::MIN,
            false,
            vec![DocValueCell::value(ScalarValue::String("only".into()))],
        )
        .unwrap();
        let mut encoded = block.encode_payload().unwrap();
        let ordinal = encoded.len() - 4;
        encoded[ordinal..].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(DocValueBlock::decode_payload(&encoded).is_err());
    }

    #[test]
    fn keyword_doc_value_raw_length_boundary_is_exact() {
        let maximum = "x".repeat(INDEX_TERM_BYTES);
        let block = DocValueBlock::new(
            FieldId::new(5),
            DocId::MIN,
            false,
            vec![DocValueCell::value(ScalarValue::String(maximum))],
        )
        .unwrap();
        assert_eq!(
            DocValueBlock::decode_payload(&block.encode_payload().unwrap()).unwrap(),
            block
        );
        assert!(
            DocValueBlock::new(
                FieldId::new(5),
                DocId::MIN,
                false,
                vec![DocValueCell::value(ScalarValue::String(
                    "x".repeat(INDEX_TERM_BYTES + 1),
                ))],
            )
            .is_err()
        );
    }
}
