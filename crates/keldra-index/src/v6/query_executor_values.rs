use crate::IndexError;
use crate::typed_json::{FieldType, ScalarValue};

pub(super) fn resident_scalar_bytes(value: &ScalarValue) -> usize {
    std::mem::size_of::<ScalarValue>()
        + match value {
            ScalarValue::String(value) => value.len(),
            _ => 0,
        }
}

pub(super) fn scalar_number(value: &ScalarValue) -> Result<f64, IndexError> {
    match value {
        ScalarValue::Signed(value) => Ok(*value as f64),
        ScalarValue::Unsigned(value) => Ok(*value as f64),
        ScalarValue::Number(_) => Ok(value.as_number().expect("number")),
        _ => Err(IndexError::InvalidQuery(
            "average requires numeric values".into(),
        )),
    }
}

pub(super) fn verify_hash(expected: [u8; 32], bytes: &[u8]) -> Result<(), IndexError> {
    if expected == [0; 32] || *blake3::hash(bytes).as_bytes() != expected {
        Err(IndexError::Integrity)
    } else {
        Ok(())
    }
}

pub(super) fn resource<T>(needed: usize, limit: usize) -> Result<T, IndexError> {
    Err(IndexError::ResourceLimit { needed, limit })
}

pub(super) fn validate_scalar(
    field_type: FieldType,
    value: &ScalarValue,
) -> Result<(), IndexError> {
    let valid = matches!(
        (field_type, value),
        (_, ScalarValue::Null)
            | (FieldType::Boolean, ScalarValue::Boolean(_))
            | (
                FieldType::SignedInteger | FieldType::Date,
                ScalarValue::Signed(_)
            )
            | (FieldType::UnsignedInteger, ScalarValue::Unsigned(_))
            | (FieldType::Float, ScalarValue::Number(_))
            | (FieldType::Keyword | FieldType::Text, ScalarValue::String(_))
    );
    if valid {
        Ok(())
    } else {
        Err(IndexError::InvalidQuery(
            "query scalar type does not match its field".into(),
        ))
    }
}
