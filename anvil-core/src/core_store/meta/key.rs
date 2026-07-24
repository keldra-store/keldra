use anyhow::{Context, Result, bail};
use unicode_normalization::UnicodeNormalization;

const CORE_META_KEY_VERSION: u8 = 1;
const CORE_META_KEY_HEADER_BYTES: usize = 11;
const CORE_META_MAX_TUPLE_KEY_BYTES: usize = u16::MAX as usize;

const KIND_UTF8: u8 = 0x01;
const KIND_U64: u8 = 0x02;
const KIND_I64: u8 = 0x03;
const KIND_HASH: u8 = 0x04;
const KIND_RAW: u8 = 0x05;
const KIND_BOOL: u8 = 0x06;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreMetaTuplePart<'a> {
    Utf8(&'a str),
    U64(u64),
    I64(i64),
    Hash(&'a str),
    Raw(&'a [u8]),
    Bool(bool),
}

pub fn core_meta_tuple_key(parts: &[CoreMetaTuplePart<'_>]) -> Result<Vec<u8>> {
    let mut key = Vec::new();
    for part in parts {
        encode_tuple_part(&mut key, *part)?;
    }
    if key.len() > CORE_META_MAX_TUPLE_KEY_BYTES {
        bail!("CoreMetaTupleKey exceeds the maximum encoded size");
    }
    validate_core_meta_tuple_key(&key)?;
    Ok(key)
}

pub(super) fn core_meta_key(table_id: u16, partition_id: u64, tuple_key: &[u8]) -> Result<Vec<u8>> {
    if tuple_key.len() > CORE_META_MAX_TUPLE_KEY_BYTES {
        bail!("CoreMetaTupleKey exceeds the maximum encoded size");
    }
    validate_core_meta_tuple_key(tuple_key)?;
    let mut key = core_meta_partition_prefix(table_id, partition_id);
    key.reserve(tuple_key.len());
    key.extend_from_slice(tuple_key);
    Ok(key)
}

pub(super) fn core_meta_partition_prefix(table_id: u16, partition_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(CORE_META_KEY_HEADER_BYTES);
    key.push(CORE_META_KEY_VERSION);
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(&partition_id.to_be_bytes());
    key
}

pub(super) fn decode_core_meta_table_id(key: &[u8]) -> Result<u16> {
    validate_core_meta_key_header(key)?;
    let tuple_key = &key[CORE_META_KEY_HEADER_BYTES..];
    validate_core_meta_tuple_key(tuple_key)?;
    Ok(u16::from_be_bytes([key[1], key[2]]))
}

pub(super) fn decode_core_meta_tuple_key(key: &[u8]) -> Result<&[u8]> {
    validate_core_meta_key_header(key)?;
    let tuple_key = &key[CORE_META_KEY_HEADER_BYTES..];
    validate_core_meta_tuple_key(tuple_key)?;
    Ok(tuple_key)
}

pub(super) fn exclusive_prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    let index = successor.iter().rposition(|byte| *byte != u8::MAX)?;
    successor[index] += 1;
    successor.truncate(index + 1);
    Some(successor)
}

pub(super) fn validate_core_meta_tuple_key(tuple_key: &[u8]) -> Result<()> {
    let mut offset = 0;
    while offset < tuple_key.len() {
        let kind = tuple_key[offset];
        offset += 1;
        match kind {
            KIND_UTF8 | KIND_HASH | KIND_RAW => {
                let (value, next_offset) = decode_escaped_value(tuple_key, offset)?;
                validate_variable_part(kind, &value)?;
                offset = next_offset;
            }
            KIND_U64 | KIND_I64 => {
                offset = offset
                    .checked_add(8)
                    .context("CoreMetaTupleKey integer part length overflow")?;
                if offset > tuple_key.len() {
                    bail!("CoreMetaTupleKey integer part is truncated");
                }
            }
            KIND_BOOL => {
                let value = *tuple_key
                    .get(offset)
                    .context("CoreMetaTupleKey bool part is truncated")?;
                if !matches!(value, 0 | 1) {
                    bail!("CoreMetaTupleKey bool part must be 0x00 or 0x01");
                }
                offset += 1;
            }
            other => bail!("CoreMetaTupleKey has unknown part kind {other:#04x}"),
        }
    }
    Ok(())
}

fn validate_core_meta_key_header(key: &[u8]) -> Result<()> {
    if key.len() < CORE_META_KEY_HEADER_BYTES {
        bail!("CoreMetaKey is shorter than fixed header");
    }
    if key[0] != CORE_META_KEY_VERSION {
        bail!("CoreMetaKey has unsupported version {}", key[0]);
    }
    Ok(())
}

fn encode_tuple_part(key: &mut Vec<u8>, part: CoreMetaTuplePart<'_>) -> Result<()> {
    match part {
        CoreMetaTuplePart::Utf8(value) => {
            validate_utf8_part(value)?;
            key.push(KIND_UTF8);
            encode_escaped_value(key, value.as_bytes());
        }
        CoreMetaTuplePart::U64(value) => {
            key.push(KIND_U64);
            key.extend_from_slice(&value.to_be_bytes());
        }
        CoreMetaTuplePart::I64(value) => {
            key.push(KIND_I64);
            key.extend_from_slice(&((value as u64) ^ (1_u64 << 63)).to_be_bytes());
        }
        CoreMetaTuplePart::Hash(value) => {
            let value = normalise_tuple_hash_part(value);
            validate_hash_part(&value)?;
            key.push(KIND_HASH);
            encode_escaped_value(key, value.as_bytes());
        }
        CoreMetaTuplePart::Raw(value) => {
            key.push(KIND_RAW);
            encode_escaped_value(key, value);
        }
        CoreMetaTuplePart::Bool(value) => {
            key.push(KIND_BOOL);
            key.push(u8::from(value));
        }
    }
    Ok(())
}

fn encode_escaped_value(key: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            key.extend_from_slice(&[0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
    key.extend_from_slice(&[0, 0]);
}

fn decode_escaped_value(bytes: &[u8], mut offset: usize) -> Result<(Vec<u8>, usize)> {
    let mut value = Vec::new();
    while offset < bytes.len() {
        let byte = bytes[offset];
        if byte != 0 {
            value.push(byte);
            offset += 1;
            continue;
        }
        let escape = *bytes
            .get(offset + 1)
            .context("CoreMetaTupleKey variable part has a trailing NUL")?;
        match escape {
            0 => return Ok((value, offset + 2)),
            0xff => {
                value.push(0);
                offset += 2;
            }
            _ => bail!("CoreMetaTupleKey variable part has an invalid NUL escape"),
        }
    }
    bail!("CoreMetaTupleKey variable part is missing its terminator")
}

fn validate_variable_part(kind: u8, value: &[u8]) -> Result<()> {
    match kind {
        KIND_UTF8 => {
            let value =
                std::str::from_utf8(value).context("CoreMetaTupleKey utf8 part is invalid")?;
            validate_utf8_part(value)
        }
        KIND_HASH => {
            let value =
                std::str::from_utf8(value).context("CoreMetaTupleKey hash part is invalid")?;
            validate_hash_part(value)
        }
        KIND_RAW => Ok(()),
        _ => unreachable!("caller only passes variable-width tuple kinds"),
    }
}

fn validate_utf8_part(value: &str) -> Result<()> {
    if value.as_bytes().contains(&0) {
        bail!("CoreMetaTupleKey utf8 part contains NUL");
    }
    if !value.chars().eq(value.nfc()) {
        bail!("CoreMetaTupleKey utf8 part must be NFC-normalized");
    }
    Ok(())
}

fn validate_hash_part(value: &str) -> Result<()> {
    let Some((algorithm, hex_value)) = value.split_once(':') else {
        bail!("CoreMetaTupleKey hash part must be algorithm:hex ASCII");
    };
    if algorithm.is_empty()
        || hex_value.is_empty()
        || !value.is_ascii()
        || !algorithm
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || hex_value
            .bytes()
            .any(|byte| !(byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')))
    {
        bail!("CoreMetaTupleKey hash part must be canonical algorithm:hex ASCII");
    }
    Ok(())
}

fn normalise_tuple_hash_part(value: &str) -> String {
    if value.contains(':') {
        return value.to_string();
    }
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return format!("blake3:{value}");
    }
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normative_key_vectors_match_the_storage_contract() {
        assert_eq!(
            hex::encode(core_meta_partition_prefix(0x8501, 0)),
            "0185010000000000000000"
        );
        assert_tuple_vector(&[CoreMetaTuplePart::Utf8("")], "010000");
        assert_tuple_vector(&[CoreMetaTuplePart::Utf8("a")], "01610000");
        assert_tuple_vector(&[CoreMetaTuplePart::Utf8("aa")], "0161610000");
        assert_tuple_vector(
            &[CoreMetaTuplePart::Raw(&[0x41, 0, 0x42])],
            "054100ff420000",
        );
        assert_tuple_vector(&[CoreMetaTuplePart::I64(i64::MIN)], "030000000000000000");
        assert_tuple_vector(&[CoreMetaTuplePart::I64(-1)], "037fffffffffffffff");
        assert_tuple_vector(&[CoreMetaTuplePart::I64(0)], "038000000000000000");
        assert_tuple_vector(&[CoreMetaTuplePart::I64(i64::MAX)], "03ffffffffffffffff");

        let prefix_tuple = core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("schema_revision"),
            CoreMetaTuplePart::I64(2),
        ])
        .unwrap();
        let prefix = core_meta_key(0x8501, 0, &prefix_tuple).unwrap();
        assert_eq!(
            hex::encode(&prefix),
            "018501000000000000000001736368656d615f7265766973696f6e0000038000000000000002"
        );

        let row_tuple = core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("schema_revision"),
            CoreMetaTuplePart::I64(2),
            CoreMetaTuplePart::Utf8("system"),
            CoreMetaTuplePart::U64(100),
        ])
        .unwrap();
        let row = core_meta_key(0x8501, 0, &row_tuple).unwrap();
        assert!(row.starts_with(&prefix));
        assert_eq!(
            hex::encode(row),
            "018501000000000000000001736368656d615f7265766973696f6e00000380000000000000020173797374656d0000020000000000000064"
        );
        assert_eq!(
            hex::encode(exclusive_prefix_successor(&prefix).unwrap()),
            "018501000000000000000001736368656d615f7265766973696f6e0000038000000000000003"
        );
    }

    #[test]
    fn variable_parts_are_prefix_preserving_and_ordered() {
        let mut values = vec![
            b"".to_vec(),
            b"a".to_vec(),
            b"aa".to_vec(),
            b"b".to_vec(),
            vec![0],
            vec![0, 0],
            vec![0, 1],
        ];
        values.sort();
        let encoded = values
            .iter()
            .map(|value| core_meta_tuple_key(&[CoreMetaTuplePart::Raw(value)]).unwrap())
            .collect::<Vec<_>>();

        for key in &encoded {
            validate_core_meta_tuple_key(key).unwrap();
        }
        for pair in encoded.windows(2) {
            assert!(
                pair[0] < pair[1],
                "physical encoding must preserve byte order"
            );
        }
    }

    #[test]
    fn raw_nul_bytes_round_trip_through_the_canonical_escape_encoding() {
        let values = [
            vec![0],
            vec![0, 0],
            vec![b'a', 0, b'b'],
            vec![0, 0xff, 0],
            (0..=u8::MAX).collect::<Vec<_>>(),
        ];

        for value in values {
            let encoded = core_meta_tuple_key(&[CoreMetaTuplePart::Raw(value.as_slice())]).unwrap();
            let (decoded, next_offset) = decode_escaped_value(&encoded, 1).unwrap();

            assert_eq!(decoded, value);
            assert_eq!(next_offset, encoded.len());
            validate_core_meta_tuple_key(&encoded).unwrap();
        }
    }

    #[test]
    fn raw_nul_bytes_preserve_complete_tuple_prefixes() {
        let prefix = core_meta_tuple_key(&[CoreMetaTuplePart::Raw(b"tenant\0bucket")]).unwrap();
        let descendant = core_meta_tuple_key(&[
            CoreMetaTuplePart::Raw(b"tenant\0bucket"),
            CoreMetaTuplePart::U64(42),
        ])
        .unwrap();

        assert!(descendant.starts_with(&prefix));
    }

    #[test]
    fn length_delimited_tuple_shape_is_not_canonical_escape_encoding() {
        let value = b"tenant\0bucket";
        let mut length_delimited = Vec::new();
        length_delimited.extend_from_slice(&1_u16.to_le_bytes());
        length_delimited.push(KIND_RAW);
        length_delimited.push(0);
        length_delimited.extend_from_slice(&(value.len() as u16).to_le_bytes());
        length_delimited.extend_from_slice(value);

        let error = validate_core_meta_tuple_key(&length_delimited).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("variable part has an invalid NUL escape")
        );
    }

    #[test]
    fn signed_and_unsigned_integer_encodings_preserve_order() {
        let signed = [i64::MIN, -9, -1, 0, 1, 9, i64::MAX];
        let signed_keys =
            signed.map(|value| core_meta_tuple_key(&[CoreMetaTuplePart::I64(value)]).unwrap());
        assert!(signed_keys.windows(2).all(|pair| pair[0] < pair[1]));

        let unsigned = [0, 1, 9, u64::MAX - 1, u64::MAX];
        let unsigned_keys =
            unsigned.map(|value| core_meta_tuple_key(&[CoreMetaTuplePart::U64(value)]).unwrap());
        assert!(unsigned_keys.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn generated_raw_values_preserve_lexicographic_order() {
        let mut seed = 0x4d59_5df4_d0f3_3173_u64;
        let mut values = Vec::with_capacity(4_096);
        for index in 0..4_096 {
            seed = xorshift(seed);
            let len = (seed as usize + index) % 48;
            let mut value = Vec::with_capacity(len);
            for _ in 0..len {
                seed = xorshift(seed);
                value.push(seed as u8);
            }
            values.push(value);
        }
        values.sort();
        values.dedup();

        let keys = values
            .iter()
            .map(|value| core_meta_tuple_key(&[CoreMetaTuplePart::Raw(value)]).unwrap())
            .collect::<Vec<_>>();
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn malformed_tuple_parts_are_rejected() {
        for malformed in [
            vec![KIND_UTF8],
            vec![KIND_UTF8, 0],
            vec![KIND_UTF8, 0, 1],
            vec![KIND_U64, 0, 0, 0],
            vec![KIND_BOOL, 2],
            vec![0xff],
        ] {
            assert!(validate_core_meta_tuple_key(&malformed).is_err());
        }
    }

    fn assert_tuple_vector(parts: &[CoreMetaTuplePart<'_>], expected: &str) {
        assert_eq!(hex::encode(core_meta_tuple_key(parts).unwrap()), expected);
    }

    fn xorshift(mut value: u64) -> u64 {
        value ^= value << 13;
        value ^= value >> 7;
        value ^ (value << 17)
    }
}
