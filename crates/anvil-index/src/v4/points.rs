use crate::IndexError;

use super::codec::{COMPONENT_HEADER_BYTES, Decoder, Encoder};
use super::columns::{decode_scalar, encode_scalar};
use super::{DocId, FieldId, INDEX_COMPONENT_BYTES, ScalarValue, canonical_term_key, scalar_term};

pub const POINTS_COMPONENT_CODEC_VERSION: u16 = 1;
/// Lucene-style point leaves remain deliberately small so an exact or narrow
/// range traversal reads a bounded leaf instead of the component ceiling.
pub(crate) const POINT_BLOCK_ENTRIES: usize = 512;
const MAX_PAYLOAD_BYTES: usize = INDEX_COMPONENT_BYTES - COMPONENT_HEADER_BYTES;
const POINT_RECORD_PRESENCE: u8 = 1;
const POINT_RECORD_NULL: u8 = 2;
const POINT_RECORD_VALUE: u8 = 3;
// POINTS use their own key namespace. These tags intentionally sort in the
// same order as `PointValue`: presence, null, then the numeric scalar tags.
const POINT_PRESENCE_TERM_TYPE: u8 = 1;
const POINT_PRESENCE_TERM: &[u8] = &[0];
const POINT_NULL_TERM_TYPE: u8 = 2;
const POINT_NULL_TERM: &[u8] = &[0];

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PointValue {
    /// Exactly one record for every present field, including explicit null.
    Presence,
    /// Exactly one record when at least one explicit null occurred.
    Null,
    Value(ScalarValue),
}

impl Ord for PointValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Presence, Self::Presence) => std::cmp::Ordering::Equal,
            (Self::Presence, _) => std::cmp::Ordering::Less,
            (_, Self::Presence) => std::cmp::Ordering::Greater,
            (Self::Null, Self::Null) => std::cmp::Ordering::Equal,
            (Self::Null, Self::Value(_)) => std::cmp::Ordering::Less,
            (Self::Value(_), Self::Null) => std::cmp::Ordering::Greater,
            (Self::Value(left), Self::Value(right)) => left.cmp(right),
        }
    }
}

impl PartialOrd for PointValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointEntry {
    pub value: PointValue,
    pub doc_id: DocId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointBlock {
    pub field_id: FieldId,
    entries: Vec<PointEntry>,
}

impl PointBlock {
    pub fn new(field_id: FieldId, entries: Vec<PointEntry>) -> Result<Self, IndexError> {
        if entries.is_empty()
            || entries
                .iter()
                .any(|entry| matches!(&entry.value, PointValue::Value(value) if !is_numeric(value)))
            || entries.windows(2).any(|pair| {
                pair[0].value > pair[1].value
                    || pair[0].value == pair[1].value && pair[0].doc_id >= pair[1].doc_id
            })
        {
            return Err(IndexError::InvalidDefinition(
                "point entries require one numeric type and sorted value/DocId pairs".into(),
            ));
        }
        let value_type = entries.iter().find_map(|entry| match &entry.value {
            PointValue::Presence | PointValue::Null => None,
            PointValue::Value(value) => Some(std::mem::discriminant(value)),
        });
        if entries.iter().any(|entry| {
            matches!(&entry.value, PointValue::Value(value)
                if value_type.is_some_and(|expected| expected != std::mem::discriminant(value)))
        }) || entries.windows(2).any(|pair| {
            pair[0].doc_id == pair[1].doc_id
                && pair[0].value == pair[1].value
                && matches!(pair[0].value, PointValue::Presence | PointValue::Null)
        }) {
            return Err(IndexError::InvalidDefinition(
                "one point block cannot mix numeric types or duplicate presence".into(),
            ));
        }
        let block = Self { field_id, entries };
        let needed = block.encode_payload()?.len();
        if needed > MAX_PAYLOAD_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: needed + COMPONENT_HEADER_BYTES,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        Ok(block)
    }

    pub fn entries(&self) -> &[PointEntry] {
        &self.entries
    }

    pub fn minimum(&self) -> &PointValue {
        &self.entries[0].value
    }

    pub fn maximum(&self) -> &PointValue {
        &self.entries[self.entries.len() - 1].value
    }

    pub fn minimum_entry(&self) -> &PointEntry {
        &self.entries[0]
    }

    pub fn maximum_entry(&self) -> &PointEntry {
        &self.entries[self.entries.len() - 1]
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        let mut out = Encoder::default();
        out.u16(POINTS_COMPONENT_CODEC_VERSION);
        out.u32(self.field_id.get());
        out.usize_u32(self.entries.len())?;
        for entry in &self.entries {
            match &entry.value {
                PointValue::Presence => out.u8(POINT_RECORD_PRESENCE),
                PointValue::Null => out.u8(POINT_RECORD_NULL),
                PointValue::Value(value) => {
                    out.u8(POINT_RECORD_VALUE);
                    encode_scalar(&mut out, value)?;
                }
            }
            out.u32(entry.doc_id.get());
        }
        Ok(out.finish())
    }

    pub fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.u16()? != POINTS_COMPONENT_CODEC_VERSION {
            return Err(IndexError::InvalidFormat("point codec version"));
        }
        let field_id = FieldId::new(input.u32()?);
        let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        input.claim(
            count
                .checked_mul(std::mem::size_of::<PointEntry>())
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let value = match input.u8()? {
                POINT_RECORD_PRESENCE => PointValue::Presence,
                POINT_RECORD_NULL => PointValue::Null,
                POINT_RECORD_VALUE => PointValue::Value(decode_scalar(&mut input)?),
                _ => return Err(IndexError::InvalidFormat("point record kind")),
            };
            entries.push(PointEntry {
                value,
                doc_id: DocId::new(input.u32()?),
            });
        }
        input.finish()?;
        Self::new(field_id, entries).map_err(|_| IndexError::InvalidFormat("point entries"))
    }
}

pub fn point_value_key(field_id: FieldId, value: &PointValue) -> Result<Vec<u8>, IndexError> {
    if value == &PointValue::Presence {
        return canonical_term_key(field_id, POINT_PRESENCE_TERM_TYPE, POINT_PRESENCE_TERM);
    }
    if value == &PointValue::Null {
        return canonical_term_key(field_id, POINT_NULL_TERM_TYPE, POINT_NULL_TERM);
    }
    let PointValue::Value(value) = value else {
        unreachable!()
    };
    if !is_numeric(value) {
        return Err(IndexError::InvalidDefinition(
            "point values must be numeric".into(),
        ));
    }
    let (kind, value) = scalar_term(value)?;
    canonical_term_key(field_id, kind, &value)
}

pub fn point_scalar_key(field_id: FieldId, value: &ScalarValue) -> Result<Vec<u8>, IndexError> {
    point_value_key(field_id, &PointValue::Value(value.clone()))
}

pub fn point_presence_key(field_id: FieldId) -> Result<Vec<u8>, IndexError> {
    point_value_key(field_id, &PointValue::Presence)
}

/// A routed point-leaf key. The DocId suffix keeps adjacent leaves disjoint
/// when many documents share one numeric value.
pub fn point_entry_key(
    field_id: FieldId,
    value: &PointValue,
    doc_id: DocId,
) -> Result<Vec<u8>, IndexError> {
    let mut key = point_value_key(field_id, value)?;
    key.extend_from_slice(&doc_id.get().to_be_bytes());
    Ok(key)
}

pub fn point_value_range(
    field_id: FieldId,
    value: &PointValue,
) -> Result<(Vec<u8>, Vec<u8>), IndexError> {
    Ok((
        point_entry_key(field_id, value, DocId::new(0))?,
        point_entry_key(field_id, value, DocId::new(u32::MAX))?,
    ))
}

fn is_numeric(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Signed(_) | ScalarValue::Unsigned(_) | ScalarValue::Number(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_block_round_trips_and_rejects_mixed_types() {
        let block = PointBlock::new(
            FieldId::new(2),
            vec![
                PointEntry {
                    value: PointValue::Presence,
                    doc_id: DocId::new(8),
                },
                PointEntry {
                    value: PointValue::Null,
                    doc_id: DocId::new(8),
                },
                PointEntry {
                    value: PointValue::Value(ScalarValue::Signed(9)),
                    doc_id: DocId::new(1),
                },
            ],
        )
        .unwrap();
        assert_eq!(
            PointBlock::decode_payload(&block.encode_payload().unwrap()).unwrap(),
            block
        );
        assert!(
            PointBlock::new(
                FieldId::new(2),
                vec![
                    PointEntry {
                        value: PointValue::Value(ScalarValue::Signed(1)),
                        doc_id: DocId::new(0)
                    },
                    PointEntry {
                        value: PointValue::Value(ScalarValue::Unsigned(1)),
                        doc_id: DocId::new(1)
                    },
                ],
            )
            .is_err()
        );
    }

    #[test]
    fn duplicate_presence_for_one_document_is_rejected() {
        assert!(
            PointBlock::new(
                FieldId::new(1),
                vec![
                    PointEntry {
                        value: PointValue::Presence,
                        doc_id: DocId::new(4)
                    },
                    PointEntry {
                        value: PointValue::Presence,
                        doc_id: DocId::new(4)
                    },
                ],
            )
            .is_err()
        );
    }

    #[test]
    fn routed_keys_disambiguate_equal_values_by_doc_id() {
        let value = PointValue::Value(ScalarValue::Signed(7));
        let first = point_entry_key(FieldId::new(1), &value, DocId::new(1)).unwrap();
        let second = point_entry_key(FieldId::new(1), &value, DocId::new(2)).unwrap();
        assert!(first < second);
        let (minimum, maximum) = point_value_range(FieldId::new(1), &value).unwrap();
        assert!(minimum <= first && second <= maximum);
    }

    #[test]
    fn routed_key_order_matches_point_value_order() {
        let field = FieldId::new(1);
        let entries = [
            PointEntry {
                value: PointValue::Presence,
                doc_id: DocId::new(3),
            },
            PointEntry {
                value: PointValue::Null,
                doc_id: DocId::new(3),
            },
            PointEntry {
                value: PointValue::Value(ScalarValue::Signed(-1)),
                doc_id: DocId::new(3),
            },
            PointEntry {
                value: PointValue::Value(ScalarValue::Signed(1)),
                doc_id: DocId::new(3),
            },
        ];
        for pair in entries.windows(2) {
            assert!(compare_entries(&pair[0], &pair[1]).is_lt());
            assert!(
                point_entry_key(field, &pair[0].value, pair[0].doc_id).unwrap()
                    < point_entry_key(field, &pair[1].value, pair[1].doc_id).unwrap()
            );
        }
    }

    fn compare_entries(left: &PointEntry, right: &PointEntry) -> std::cmp::Ordering {
        left.value
            .cmp(&right.value)
            .then_with(|| left.doc_id.cmp(&right.doc_id))
    }
}
