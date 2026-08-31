use crate::IndexError;
use crate::typed_json::{
    AggregateOperation, FieldCapabilities, FieldId, FieldSchema, FieldType, Predicate, ScalarValue,
};

pub(super) fn reduce_streaming<'a>(
    operation: AggregateOperation,
    values: impl Iterator<Item = &'a ScalarValue>,
) -> Result<(Option<ScalarValue>, u64), IndexError> {
    let mut count = 0u64;
    let mut value = None;
    let mut average_sum = 0.0;
    for next in values {
        count = count.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        match operation {
            AggregateOperation::Count => {}
            AggregateOperation::Minimum => {
                if value.as_ref().is_none_or(|current| next < current) {
                    value = Some(next.clone());
                }
            }
            AggregateOperation::Maximum => {
                if value.as_ref().is_none_or(|current| next > current) {
                    value = Some(next.clone());
                }
            }
            AggregateOperation::Sum => {
                value = Some(match value.take() {
                    None => next.clone(),
                    Some(current) => sum_pair(current, next)?,
                });
            }
            AggregateOperation::Average => average_sum += scalar_number(next)?,
        }
    }
    let value = match operation {
        AggregateOperation::Count => Some(ScalarValue::Unsigned(count)),
        AggregateOperation::Average if count != 0 => {
            Some(ScalarValue::number(average_sum / count as f64)?)
        }
        AggregateOperation::Average => None,
        _ => value,
    };
    Ok((value, count))
}

fn sum_pair(current: ScalarValue, next: &ScalarValue) -> Result<ScalarValue, IndexError> {
    match (current, next) {
        (ScalarValue::Signed(left), ScalarValue::Signed(right)) => left
            .checked_add(*right)
            .map(ScalarValue::Signed)
            .ok_or(IndexError::OffsetOverflow),
        (ScalarValue::Unsigned(left), ScalarValue::Unsigned(right)) => left
            .checked_add(*right)
            .map(ScalarValue::Unsigned)
            .ok_or(IndexError::OffsetOverflow),
        (current @ ScalarValue::Number(_), ScalarValue::Number(_)) => {
            ScalarValue::number(scalar_number(&current)? + scalar_number(next)?)
        }
        _ => Err(IndexError::InvalidQuery(
            "aggregate scalar types differ".into(),
        )),
    }
}

pub(super) fn leaf_field(predicate: &Predicate) -> Option<FieldId> {
    match predicate {
        Predicate::Equal { field_id, .. }
        | Predicate::In { field_id, .. }
        | Predicate::Prefix { field_id, .. }
        | Predicate::Range { field_id, .. }
        | Predicate::Exists { field_id, .. }
        | Predicate::FullText { field_id, .. }
        | Predicate::Phrase { field_id, .. } => Some(*field_id),
        _ => None,
    }
}

pub(super) fn validate_leaf_capability(
    field: &FieldSchema,
    predicate: &Predicate,
) -> Result<(), IndexError> {
    let required = match predicate {
        Predicate::Equal { .. } | Predicate::In { .. } | Predicate::Exists { .. } => {
            FieldCapabilities::EXACT
        }
        Predicate::Prefix { .. } => FieldCapabilities::PREFIX,
        Predicate::Range { .. } => FieldCapabilities::RANGE,
        Predicate::FullText { .. } | Predicate::Phrase { .. } => FieldCapabilities::FULL_TEXT,
        _ => return Err(IndexError::InvalidQuery("expected leaf predicate".into())),
    };
    if !field.capabilities.contains(required) {
        return Err(IndexError::InvalidQuery(
            "field lacks query capability".into(),
        ));
    }
    let values = match predicate {
        Predicate::Equal { value, .. } => std::slice::from_ref(value),
        Predicate::In { values, .. } => values.as_slice(),
        Predicate::Range { lower, upper, .. } => {
            for bound in lower.iter().chain(upper.iter()) {
                validate_scalar(field.field_type, &bound.value)?;
                if matches!(bound.value, ScalarValue::Null) {
                    return Err(IndexError::InvalidQuery(
                        "range does not accept null".into(),
                    ));
                }
            }
            return Ok(());
        }
        _ => return Ok(()),
    };
    for value in values {
        if matches!(value, ScalarValue::Null) && !field.allow_null {
            return Err(IndexError::InvalidQuery("field does not admit null".into()));
        }
        validate_scalar(field.field_type, value)?;
    }
    Ok(())
}

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
