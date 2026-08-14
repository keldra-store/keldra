use crate::IndexError;

use super::codec::{COMPONENT_HEADER_BYTES, Decoder, Encoder};
use super::keys::{
    TERM_TYPE_BOOLEAN, TERM_TYPE_FIELD_PRESENCE, TERM_TYPE_HASHED_KEYWORD, TERM_TYPE_NULL,
    TERM_TYPE_NUMBER, TERM_TYPE_SIGNED, TERM_TYPE_STRING, TERM_TYPE_TEXT, TERM_TYPE_UNSIGNED,
};
use super::model::{INDEX_COMPONENT_BYTES, INDEX_TERM_BYTES, validate_term_routing_key};

const TERM_DICTIONARY_CODEC_VERSION: u16 = 1;
const MAX_PAYLOAD_BYTES: usize = INDEX_COMPONENT_BYTES - COMPONENT_HEADER_BYTES;

/// A bounded range of leaf ordinals in the field's routed `POSTINGS` stream.
/// It never denotes entries in the segment manifest: one field has one
/// postings root regardless of its term count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PostingReference {
    pub document_frequency: u64,
    pub total_term_frequency: u64,
    pub first_component_ordinal: u32,
    pub component_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TermEntry {
    /// Canonical bytes beginning with FieldId and scalar/token type.
    pub term: Vec<u8>,
    pub postings: PostingReference,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TermDictionary {
    entries: Vec<TermEntry>,
}

impl TermDictionary {
    pub fn new(entries: Vec<TermEntry>) -> Result<Self, IndexError> {
        validate_entries(&entries)?;
        let value = Self { entries };
        let length = value.encode_payload()?.len();
        if length > MAX_PAYLOAD_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: length + COMPONENT_HEADER_BYTES,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        Ok(value)
    }

    pub fn entries(&self) -> &[TermEntry] {
        &self.entries
    }

    pub fn exact(&self, term: &[u8]) -> Option<&TermEntry> {
        self.entries
            .binary_search_by(|entry| entry.term.as_slice().cmp(term))
            .ok()
            .map(|index| &self.entries[index])
    }

    pub fn lower_bound(&self, term: &[u8]) -> usize {
        self.entries
            .partition_point(|entry| entry.term.as_slice() < term)
    }

    pub fn prefix<'a>(&'a self, prefix: &'a [u8]) -> impl Iterator<Item = &'a TermEntry> + 'a {
        self.entries[self.lower_bound(prefix)..]
            .iter()
            .take_while(move |entry| entry.term.starts_with(prefix))
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        validate_entries(&self.entries)?;
        let mut out = Encoder::default();
        out.u16(TERM_DICTIONARY_CODEC_VERSION);
        out.usize_u32(self.entries.len())?;
        for entry in &self.entries {
            out.bytes(&entry.term)?;
            out.u64(entry.postings.document_frequency);
            out.u64(entry.postings.total_term_frequency);
            out.u32(entry.postings.first_component_ordinal);
            out.u32(entry.postings.component_count);
        }
        Ok(out.finish())
    }

    pub fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.u16()? != TERM_DICTIONARY_CODEC_VERSION {
            return Err(IndexError::InvalidFormat("term dictionary codec version"));
        }
        let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        input.claim(
            count
                .checked_mul(std::mem::size_of::<TermEntry>())
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(TermEntry {
                term: input.owned_bytes()?,
                postings: PostingReference {
                    document_frequency: input.u64()?,
                    total_term_frequency: input.u64()?,
                    first_component_ordinal: input.u32()?,
                    component_count: input.u32()?,
                },
            });
        }
        input.finish()?;
        Self::new(entries)
    }

    pub fn split(entries: Vec<TermEntry>) -> Result<Vec<Self>, IndexError> {
        validate_entries(&entries)?;
        let mut blocks = Vec::new();
        let mut pending = Vec::new();
        let mut bytes = 6usize;
        for entry in entries {
            let row = 4usize
                .checked_add(entry.term.len())
                .and_then(|value| value.checked_add(8 + 8 + 4 + 4))
                .ok_or(IndexError::OffsetOverflow)?;
            if !pending.is_empty() && bytes.saturating_add(row) > MAX_PAYLOAD_BYTES {
                blocks.push(Self::new(std::mem::take(&mut pending))?);
                bytes = 6;
            }
            if bytes.saturating_add(row) > MAX_PAYLOAD_BYTES {
                return Err(IndexError::ResourceLimit {
                    needed: bytes.saturating_add(row) + COMPONENT_HEADER_BYTES,
                    limit: INDEX_COMPONENT_BYTES,
                });
            }
            bytes += row;
            pending.push(entry);
        }
        if !pending.is_empty() {
            blocks.push(Self::new(pending)?);
        }
        Ok(blocks)
    }
}

fn validate_entries(entries: &[TermEntry]) -> Result<(), IndexError> {
    if entries.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "term dictionary must not be empty".into(),
        ));
    }
    for entry in entries {
        if validate_dictionary_term(&entry.term).is_err()
            || entry.postings.document_frequency == 0
            || entry.postings.total_term_frequency < entry.postings.document_frequency
            || entry.postings.component_count == 0
            || entry
                .postings
                .first_component_ordinal
                .checked_add(entry.postings.component_count)
                .is_none()
        {
            return Err(IndexError::InvalidDefinition(
                "term or posting reference is invalid".into(),
            ));
        }
    }
    if entries.windows(2).any(|pair| pair[0].term >= pair[1].term) {
        return Err(IndexError::UnsortedRecords);
    }
    Ok(())
}

fn validate_dictionary_term(term: &[u8]) -> Result<(), IndexError> {
    validate_term_routing_key(term)?;
    let value = term.get(5..).ok_or_else(|| {
        IndexError::InvalidDefinition("dictionary term has no canonical field/type prefix".into())
    })?;
    let valid = match term[4] {
        TERM_TYPE_NULL | TERM_TYPE_FIELD_PRESENCE => value == [0],
        TERM_TYPE_BOOLEAN => matches!(value, [0] | [1]),
        TERM_TYPE_NUMBER | TERM_TYPE_SIGNED | TERM_TYPE_UNSIGNED => value.len() == 8,
        TERM_TYPE_STRING if value.first() == Some(&0) && value.len() <= INDEX_TERM_BYTES + 1 => {
            true
        }
        TERM_TYPE_TEXT => !value.is_empty() && value.len() <= INDEX_TERM_BYTES,
        TERM_TYPE_HASHED_KEYWORD => value.len() == 40,
        _ => false,
    };
    if !valid {
        return Err(IndexError::InvalidDefinition(
            "dictionary term is not canonical".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(term: &[u8], ordinal: u32) -> TermEntry {
        TermEntry {
            term: term.to_vec(),
            postings: PostingReference {
                document_frequency: 3,
                total_term_frequency: 5,
                first_component_ordinal: ordinal,
                component_count: 1,
            },
        }
    }

    #[test]
    fn exact_lower_bound_and_prefix_survive_round_trip() {
        let key = |value: &[u8]| {
            let mut term = vec![0, 0, 0, 1, TERM_TYPE_STRING, 0];
            term.extend_from_slice(value);
            term
        };
        let a = key(b"a");
        let aa = key(b"aa");
        let ab = key(b"ab");
        let ac = key(b"ac");
        let b = key(b"b");
        let z = key(b"z");
        let dictionary = TermDictionary::new(vec![
            entry(&a, 1),
            entry(&ab, 2),
            entry(&ac, 3),
            entry(&b, 4),
        ])
        .unwrap();
        let decoded =
            TermDictionary::decode_payload(&dictionary.encode_payload().unwrap()).unwrap();
        assert_eq!(
            decoded.exact(&ab).unwrap().postings.first_component_ordinal,
            2
        );
        assert_eq!(decoded.lower_bound(&aa), 1);
        assert_eq!(decoded.prefix(&a).count(), 3);
        assert!(decoded.exact(&z).is_none());
    }

    #[test]
    fn malformed_short_or_unknown_terms_fail_closed() {
        assert!(TermDictionary::new(vec![entry(b"a", 1)]).is_err());
        assert!(TermDictionary::new(vec![entry(&[0, 0, 0, 1, u8::MAX, 0], 1)]).is_err());
    }

    #[test]
    fn ordered_terms_accept_every_supported_long_boundary() {
        for raw_length in [4_091, INDEX_TERM_BYTES] {
            let mut term = vec![0, 0, 0, 1, TERM_TYPE_STRING, 0];
            term.extend(vec![b'x'; raw_length]);
            let dictionary = TermDictionary::new(vec![entry(&term, 1)]).unwrap();
            assert_eq!(
                TermDictionary::decode_payload(&dictionary.encode_payload().unwrap()).unwrap(),
                dictionary
            );
        }
        let mut oversized = vec![0, 0, 0, 1, TERM_TYPE_STRING, 0];
        oversized.extend(vec![b'x'; INDEX_TERM_BYTES + 1]);
        assert!(TermDictionary::new(vec![entry(&oversized, 1)]).is_err());
    }
}
