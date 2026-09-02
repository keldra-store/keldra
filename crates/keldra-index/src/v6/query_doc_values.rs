//! Canonical, bounded repeated doc-value records for query mini-runs.

use crate::IndexError;
use crate::typed_json::{ScalarValue, decode_scalar_sort_key, encode_scalar_sort_key};

use super::{
    QueryBlockLimits, QueryBlockRecord, QueryBlockRecordRef, QueryDocValue, StableDocumentKey,
};

pub fn encode_doc_value(
    value: &QueryDocValue,
    limits: QueryBlockLimits,
) -> Result<QueryBlockRecord, IndexError> {
    let limits = limits.validate()?;
    if value.material_source_version == 0 {
        return Err(IndexError::InvalidDefinition(
            "v6 doc value version is zero".into(),
        ));
    }
    let mut encoded = value.material_source_version.to_be_bytes().to_vec();
    match &value.value {
        Some(values) => {
            validate_values(values, limits, true)?;
            encoded.push(1);
            put_u32(&mut encoded, values.len())?;
            for value in values {
                let scalar = encode_scalar_sort_key(value)?;
                put_bytes(&mut encoded, &scalar)?;
            }
        }
        None => encoded.push(0),
    }
    if encoded.len() > limits.maximum_value_bytes {
        return resource(encoded.len(), limits.maximum_value_bytes);
    }
    Ok(QueryBlockRecord {
        key: value.document.bytes().to_vec(),
        value: encoded,
    })
}

pub fn decode_doc_value(
    record: QueryBlockRecordRef<'_>,
    limits: QueryBlockLimits,
) -> Result<QueryDocValue, IndexError> {
    let limits = limits.validate()?;
    if record.value.len() > limits.maximum_value_bytes {
        return resource(record.value.len(), limits.maximum_value_bytes);
    }
    let document = StableDocumentKey::from_bytes(
        record
            .key
            .try_into()
            .map_err(|_| IndexError::InvalidFormat("v6 doc value key"))?,
    )?;
    let material_source_version = read_u64(record.value, 0)?;
    if material_source_version == 0 {
        return Err(IndexError::InvalidFormat("v6 doc value version"));
    }
    let value = match record.value.get(8) {
        Some(0) if record.value.len() == 9 => None,
        Some(1) => {
            let count = usize::try_from(read_u32(record.value, 9)?)
                .map_err(|_| IndexError::OffsetOverflow)?;
            if count > limits.maximum_records || count > record.value.len().saturating_sub(13) / 4 {
                return Err(IndexError::InvalidFormat("v6 doc value count is unbounded"));
            }
            let mut offset = 13usize;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                let length = usize::try_from(read_u32(record.value, offset)?)
                    .map_err(|_| IndexError::OffsetOverflow)?;
                offset = offset.checked_add(4).ok_or(IndexError::OffsetOverflow)?;
                if length > limits.maximum_key_bytes {
                    return resource(length, limits.maximum_key_bytes);
                }
                let end = offset
                    .checked_add(length)
                    .ok_or(IndexError::OffsetOverflow)?;
                let encoded = record
                    .value
                    .get(offset..end)
                    .ok_or(IndexError::UnexpectedEof {
                        expected: end as u64,
                        actual: record.value.len() as u64,
                    })?;
                let (value, used) = decode_scalar_sort_key(encoded)?;
                if used != encoded.len() {
                    return Err(IndexError::InvalidFormat("v6 doc value scalar"));
                }
                values.push(value);
                offset = end;
            }
            if offset != record.value.len() {
                return Err(IndexError::InvalidFormat("v6 doc value trailing bytes"));
            }
            validate_values(&values, limits, false)?;
            Some(values)
        }
        _ => return Err(IndexError::InvalidFormat("v6 doc value")),
    };
    Ok(QueryDocValue {
        document,
        material_source_version,
        value,
    })
}

fn validate_values(
    values: &[ScalarValue],
    limits: QueryBlockLimits,
    definition: bool,
) -> Result<(), IndexError> {
    if values.len() > limits.maximum_records {
        return if definition {
            resource(values.len(), limits.maximum_records)
        } else {
            Err(IndexError::InvalidFormat("v6 doc value count is unbounded"))
        };
    }
    if values.windows(2).any(|pair| pair[0] > pair[1]) {
        return Err(if definition {
            IndexError::InvalidDefinition("v6 doc values are noncanonical".into())
        } else {
            IndexError::InvalidFormat("v6 doc values are noncanonical")
        });
    }
    let mut encoded_bytes = 13usize;
    for value in values {
        let needed = encode_scalar_sort_key(value)?.len();
        if needed > limits.maximum_key_bytes {
            return resource(needed, limits.maximum_key_bytes);
        }
        encoded_bytes = encoded_bytes
            .checked_add(4)
            .and_then(|bytes| bytes.checked_add(needed))
            .ok_or(IndexError::OffsetOverflow)?;
    }
    if encoded_bytes > limits.maximum_value_bytes {
        return resource(encoded_bytes, limits.maximum_value_bytes);
    }
    Ok(())
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, IndexError> {
    Ok(u32::from_be_bytes(
        bytes
            .get(offset..offset.saturating_add(4))
            .ok_or(IndexError::UnexpectedEof {
                expected: offset.saturating_add(4) as u64,
                actual: bytes.len() as u64,
            })?
            .try_into()
            .map_err(|_| IndexError::Integrity)?,
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, IndexError> {
    Ok(u64::from_be_bytes(
        bytes
            .get(offset..offset.saturating_add(8))
            .ok_or(IndexError::UnexpectedEof {
                expected: offset.saturating_add(8) as u64,
                actual: bytes.len() as u64,
            })?
            .try_into()
            .map_err(|_| IndexError::Integrity)?,
    ))
}

fn put_u32(out: &mut Vec<u8>, value: usize) -> Result<(), IndexError> {
    out.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| IndexError::OffsetOverflow)?
            .to_be_bytes(),
    );
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), IndexError> {
    put_u32(out, value.len())?;
    out.extend_from_slice(value);
    Ok(())
}

fn resource<T>(needed: usize, limit: usize) -> Result<T, IndexError> {
    Err(IndexError::ResourceLimit { needed, limit })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> StableDocumentKey {
        StableDocumentKey::from_bytes([7; 32]).unwrap()
    }

    fn record_ref(record: &QueryBlockRecord) -> QueryBlockRecordRef<'_> {
        QueryBlockRecordRef {
            key: &record.key,
            value: &record.value,
        }
    }

    #[test]
    fn repeated_and_tombstone_doc_values_are_distinct_and_canonical() {
        let limits = QueryBlockLimits::default_for_memory();
        for value in [
            None,
            Some(Vec::new()),
            Some(vec![ScalarValue::Unsigned(2), ScalarValue::Unsigned(9)]),
        ] {
            let expected = QueryDocValue {
                document: key(),
                material_source_version: 5,
                value,
            };
            let encoded = encode_doc_value(&expected, limits).unwrap();
            assert_eq!(
                decode_doc_value(record_ref(&encoded), limits).unwrap(),
                expected
            );
        }
        let noncanonical = QueryDocValue {
            document: key(),
            material_source_version: 5,
            value: Some(vec![ScalarValue::Unsigned(9), ScalarValue::Unsigned(2)]),
        };
        assert!(encode_doc_value(&noncanonical, limits).is_err());
    }

    #[test]
    fn repeated_doc_value_decode_refuses_count_beyond_the_bound() {
        let limits = QueryBlockLimits {
            maximum_records: 1,
            ..QueryBlockLimits::default_for_memory()
        };
        let value = QueryDocValue {
            document: key(),
            material_source_version: 5,
            value: Some(vec![ScalarValue::Unsigned(2), ScalarValue::Unsigned(9)]),
        };
        assert!(encode_doc_value(&value, limits).is_err());
    }
}
