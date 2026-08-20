use crate::IndexError;

use super::FieldId;
use super::{INDEX_TERM_BYTES, OrderDirection, ScalarValue, SortValue};

pub const TERM_TYPE_NULL: u8 = 1;
pub const TERM_TYPE_BOOLEAN: u8 = 2;
pub const TERM_TYPE_NUMBER: u8 = 3;
pub const TERM_TYPE_UNSIGNED: u8 = 4;
pub const TERM_TYPE_STRING: u8 = 5;
pub const TERM_TYPE_TEXT: u8 = 6;
/// Reserved exact term used to materialize field presence. Its distinct type
/// keeps it disjoint from every user scalar, including null and empty string.
pub const TERM_TYPE_FIELD_PRESENCE: u8 = 7;
pub const TERM_TYPE_HASHED_KEYWORD: u8 = 8;
pub const TERM_TYPE_SIGNED: u8 = 9;
pub const FIELD_PRESENCE_TERM: &[u8] = &[0];

pub fn canonical_term_key(
    field_id: FieldId,
    term_type: u8,
    term: &[u8],
) -> Result<Vec<u8>, IndexError> {
    if term_type == 0 || term.is_empty() {
        return Err(IndexError::InvalidQuery(
            "canonical terms require a type and value".into(),
        ));
    }
    let limit = match term_type {
        // STRING includes the one-byte empty-string/order marker in addition
        // to the RFC's raw 32,766-byte keyword limit.
        TERM_TYPE_STRING if term.first() == Some(&0) => INDEX_TERM_BYTES + 1,
        TERM_TYPE_HASHED_KEYWORD if term.len() == 40 => 40,
        _ => INDEX_TERM_BYTES,
    };
    if term.len() > limit
        || term_type == TERM_TYPE_STRING && term.first() != Some(&0)
        || term_type == TERM_TYPE_HASHED_KEYWORD && term.len() != 40
    {
        return Err(IndexError::ResourceLimit {
            needed: term.len(),
            limit,
        });
    }
    let mut key = Vec::with_capacity(5usize.saturating_add(term.len()));
    key.extend_from_slice(&field_id.get().to_be_bytes());
    key.push(term_type);
    key.extend_from_slice(term);
    Ok(key)
}

/// Canonical type-exact term bytes. Numeric encodings preserve their natural
/// order, allowing bounded same-type range enumeration in the dictionary.
pub fn scalar_term(value: &ScalarValue) -> Result<(u8, Vec<u8>), IndexError> {
    Ok(match value {
        ScalarValue::Null => (TERM_TYPE_NULL, vec![0]),
        ScalarValue::Boolean(value) => (TERM_TYPE_BOOLEAN, vec![u8::from(*value)]),
        ScalarValue::Signed(value) => (
            TERM_TYPE_SIGNED,
            ((*value as u64) ^ (1 << 63)).to_be_bytes().to_vec(),
        ),
        ScalarValue::Number(bits) => {
            let number = f64::from_bits(*bits);
            if !number.is_finite() {
                return Err(IndexError::InvalidDefinition(
                    "format-v4 scalar term number must be finite".into(),
                ));
            }
            (
                TERM_TYPE_NUMBER,
                sortable_f64(number).to_be_bytes().to_vec(),
            )
        }
        ScalarValue::Unsigned(value) => (TERM_TYPE_UNSIGNED, value.to_be_bytes().to_vec()),
        // The leading byte makes the empty string a valid non-empty term while
        // preserving bytewise exact/prefix/range order for every UTF-8 value.
        ScalarValue::String(value) => {
            if value.len() > INDEX_TERM_BYTES {
                let mut bytes = Vec::with_capacity(40);
                bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
                bytes.extend_from_slice(blake3::hash(value.as_bytes()).as_bytes());
                return Ok((TERM_TYPE_HASHED_KEYWORD, bytes));
            }
            let mut bytes = Vec::with_capacity(value.len() + 1);
            bytes.push(0);
            bytes.extend_from_slice(value.as_bytes());
            (TERM_TYPE_STRING, bytes)
        }
    })
}

pub fn text_term(token: &str) -> Result<(u8, Vec<u8>), IndexError> {
    if token.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "full-text token must not be empty".into(),
        ));
    }
    if token.len() > INDEX_TERM_BYTES {
        return Err(IndexError::ResourceLimit {
            needed: token.len(),
            limit: INDEX_TERM_BYTES,
        });
    }
    Ok((TERM_TYPE_TEXT, token.as_bytes().to_vec()))
}

/// Encode a definition-declared physical order. Missing placement and scalar
/// order exactly follow RFC 0014. The stable result identity remains a
/// separate final ascending tie-break in the writer and cursor.
pub fn encode_physical_order_key(
    values: &[(SortValue, OrderDirection)],
) -> Result<Vec<u8>, IndexError> {
    let mut output = Vec::new();
    for (value, direction) in values {
        let start = output.len();
        match value {
            SortValue::Missing => output.push(0xff),
            SortValue::Value(value) => {
                output.push(0);
                encode_order_scalar(value, &mut output)?;
            }
        }
        if *direction == OrderDirection::Descending {
            for byte in &mut output[start..] {
                *byte = !*byte;
            }
        }
    }
    Ok(output)
}

fn encode_order_scalar(value: &ScalarValue, output: &mut Vec<u8>) -> Result<(), IndexError> {
    match value {
        ScalarValue::Null => output.push(0),
        ScalarValue::Boolean(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        ScalarValue::Signed(value) => {
            output.push(2);
            output.extend_from_slice(&((*value as u64) ^ (1 << 63)).to_be_bytes());
        }
        ScalarValue::Number(bits) => {
            let number = f64::from_bits(*bits);
            if !number.is_finite() {
                return Err(IndexError::InvalidDefinition(
                    "format-v4 order number must be finite".into(),
                ));
            }
            output.push(3);
            output.extend_from_slice(&sortable_f64(number).to_be_bytes());
        }
        ScalarValue::Unsigned(value) => {
            output.push(4);
            output.extend_from_slice(&value.to_be_bytes());
        }
        ScalarValue::String(value) => {
            if value.len() > INDEX_TERM_BYTES {
                return Err(IndexError::ResourceLimit {
                    needed: value.len(),
                    limit: INDEX_TERM_BYTES,
                });
            }
            output.push(5);
            encode_terminated_bytes(value.as_bytes(), output);
        }
    }
    Ok(())
}

fn sortable_f64(value: f64) -> u64 {
    let bits = if value == 0.0 {
        0.0f64.to_bits()
    } else {
        value.to_bits()
    };
    if bits >> 63 == 1 {
        !bits
    } else {
        bits ^ (1 << 63)
    }
}

/// Zero is escaped as `00 ff`; `00 00` terminates. This is prefix-free while
/// retaining unsigned byte order for arbitrary UTF-8.
fn encode_terminated_bytes(bytes: &[u8], output: &mut Vec<u8>) {
    for byte in bytes {
        if *byte == 0 {
            output.extend_from_slice(&[0, 0xff]);
        } else {
            output.push(*byte);
        }
    }
    output.extend_from_slice(&[0, 0]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_terms_are_type_exact_and_numeric_terms_are_ordered() {
        let negative = scalar_term(&ScalarValue::number(-2.0).unwrap()).unwrap();
        let zero = scalar_term(&ScalarValue::number(-0.0).unwrap()).unwrap();
        let positive = scalar_term(&ScalarValue::number(7.0).unwrap()).unwrap();
        assert_eq!(negative.0, TERM_TYPE_NUMBER);
        assert!(negative.1 < zero.1 && zero.1 < positive.1);
        assert_ne!(scalar_term(&ScalarValue::Unsigned(7)).unwrap(), positive);
        assert_eq!(
            scalar_term(&ScalarValue::String(String::new())).unwrap().1,
            vec![0]
        );
    }

    #[test]
    fn physical_order_places_missing_and_reverses_only_declared_fields() {
        let ascending_present = encode_physical_order_key(&[(
            SortValue::Value(ScalarValue::String("a".into())),
            OrderDirection::Ascending,
        )])
        .unwrap();
        let ascending_missing =
            encode_physical_order_key(&[(SortValue::Missing, OrderDirection::Ascending)]).unwrap();
        assert!(ascending_present < ascending_missing);

        let descending_present = encode_physical_order_key(&[(
            SortValue::Value(ScalarValue::String("a".into())),
            OrderDirection::Descending,
        )])
        .unwrap();
        let descending_missing =
            encode_physical_order_key(&[(SortValue::Missing, OrderDirection::Descending)]).unwrap();
        assert!(descending_missing < descending_present);

        let first = encode_physical_order_key(&[
            (
                SortValue::Value(ScalarValue::String("a".into())),
                OrderDirection::Ascending,
            ),
            (
                SortValue::Value(ScalarValue::Unsigned(2)),
                OrderDirection::Descending,
            ),
        ])
        .unwrap();
        let second = encode_physical_order_key(&[
            (
                SortValue::Value(ScalarValue::String("a".into())),
                OrderDirection::Ascending,
            ),
            (
                SortValue::Value(ScalarValue::Unsigned(1)),
                OrderDirection::Descending,
            ),
        ])
        .unwrap();
        assert!(first < second);
    }

    #[test]
    fn terminated_strings_retain_unsigned_utf8_order_and_boundaries() {
        for (left, right) in [("", "a"), ("a", "aa"), ("a\0", "a\0b"), ("z", "é")] {
            let left = encode_physical_order_key(&[(
                SortValue::Value(ScalarValue::String(left.into())),
                OrderDirection::Ascending,
            )])
            .unwrap();
            let right = encode_physical_order_key(&[(
                SortValue::Value(ScalarValue::String(right.into())),
                OrderDirection::Ascending,
            )])
            .unwrap();
            assert!(left < right);
        }
    }

    #[test]
    fn raw_keyword_limit_accounts_for_the_order_marker_exactly() {
        let maximum = ScalarValue::String("x".repeat(INDEX_TERM_BYTES));
        let (term_type, term) = scalar_term(&maximum).unwrap();
        assert_eq!(term_type, TERM_TYPE_STRING);
        assert_eq!(term.len(), INDEX_TERM_BYTES + 1);
        assert_eq!(
            canonical_term_key(FieldId::new(3), term_type, &term)
                .unwrap()
                .len(),
            INDEX_TERM_BYTES + 6
        );

        let oversized = ScalarValue::String("x".repeat(INDEX_TERM_BYTES + 1));
        let (term_type, term) = scalar_term(&oversized).unwrap();
        assert_eq!(term_type, TERM_TYPE_HASHED_KEYWORD);
        assert_eq!(term.len(), 40);
        assert!(canonical_term_key(FieldId::new(3), term_type, &term).is_ok());
    }
}
