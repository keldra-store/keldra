//! Query-ready identity and exact-current gate codec.

use crate::IndexError;

use super::{QueryBlockRecord, QueryBlockRecordRef, StableDocumentKey};

/// Mirrors Keldra's public ordinary-object path contract without coupling the
/// storage-neutral index crate to its protocol/model crate.
pub const MAX_QUERY_DOCUMENT_PATH_BYTES: usize = 4_096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryDocumentGate {
    pub document: StableDocumentKey,
    pub material_source_version: u64,
    pub current_source_version: u64,
    pub live: bool,
    pub source_path: Option<String>,
    pub result_path: Option<String>,
    pub result_version: u64,
}

pub fn encode_document_gate(gate: QueryDocumentGate) -> Result<QueryBlockRecord, IndexError> {
    if gate.material_source_version == 0
        || gate.current_source_version < gate.material_source_version
        || !valid_identity(&gate)
    {
        return Err(IndexError::InvalidDefinition(
            "v6 gate version or identity is invalid".into(),
        ));
    }
    let mut value = gate.material_source_version.to_be_bytes().to_vec();
    value.extend_from_slice(&gate.current_source_version.to_be_bytes());
    value.extend_from_slice(&gate.result_version.to_be_bytes());
    value.push(u8::from(gate.live));
    for path in [&gate.source_path, &gate.result_path] {
        let path = path.as_deref().unwrap_or_default().as_bytes();
        value.extend_from_slice(&(path.len() as u32).to_be_bytes());
        value.extend_from_slice(path);
    }
    Ok(QueryBlockRecord {
        key: gate.document.bytes().to_vec(),
        value,
    })
}

pub fn decode_document_gate(
    record: QueryBlockRecordRef<'_>,
) -> Result<QueryDocumentGate, IndexError> {
    if record.value.len() < 33 {
        return Err(IndexError::InvalidFormat("v6 document gate"));
    }
    let material_source_version = read_u64(record.value, 0)?;
    let current_source_version = read_u64(record.value, 8)?;
    let result_version = read_u64(record.value, 16)?;
    let live = match record.value[24] {
        0 => false,
        1 => true,
        _ => return Err(IndexError::InvalidFormat("v6 document gate")),
    };
    let source_len = read_u32(record.value, 25)? as usize;
    if material_source_version == 0
        || current_source_version < material_source_version
        || source_len > MAX_QUERY_DOCUMENT_PATH_BYTES
    {
        return Err(IndexError::InvalidFormat("v6 document gate"));
    }
    let result_length_offset = 29usize
        .checked_add(source_len)
        .ok_or(IndexError::OffsetOverflow)?;
    let result_len = read_u32(record.value, result_length_offset)? as usize;
    let paths_end = result_length_offset
        .checked_add(4)
        .and_then(|offset| offset.checked_add(result_len))
        .ok_or(IndexError::OffsetOverflow)?;
    if result_len > MAX_QUERY_DOCUMENT_PATH_BYTES || paths_end != record.value.len() {
        return Err(IndexError::InvalidFormat("v6 document gate path"));
    }
    let gate = QueryDocumentGate {
        document: StableDocumentKey::from_bytes(
            record
                .key
                .try_into()
                .map_err(|_| IndexError::InvalidFormat("v6 stable document key"))?,
        )?,
        material_source_version,
        current_source_version,
        live,
        source_path: decode_path(&record.value[29..result_length_offset])?,
        result_path: decode_path(&record.value[result_length_offset + 4..paths_end])?,
        result_version,
    };
    if !valid_identity(&gate) {
        return Err(IndexError::InvalidFormat("v6 document gate identity"));
    }
    Ok(gate)
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, IndexError> {
    Ok(u64::from_be_bytes(
        bytes
            .get(offset..offset.saturating_add(8))
            .ok_or(IndexError::InvalidFormat("v6 document gate"))?
            .try_into()
            .map_err(|_| IndexError::Integrity)?,
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, IndexError> {
    Ok(u32::from_be_bytes(
        bytes
            .get(offset..offset.saturating_add(4))
            .ok_or(IndexError::InvalidFormat("v6 document gate"))?
            .try_into()
            .map_err(|_| IndexError::Integrity)?,
    ))
}

fn decode_path(bytes: &[u8]) -> Result<Option<String>, IndexError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let path = std::str::from_utf8(bytes)
        .map_err(|_| IndexError::InvalidFormat("v6 document gate path"))?;
    if path.contains('\0') {
        return Err(IndexError::InvalidFormat("v6 document gate path"));
    }
    Ok(Some(path.to_owned()))
}

fn valid_identity(gate: &QueryDocumentGate) -> bool {
    match (&gate.source_path, &gate.result_path, gate.result_version) {
        (None, None, 0) => true,
        (Some(source), Some(result), version) => {
            version != 0
                && [source, result].into_iter().all(|path| {
                    !path.is_empty()
                        && path.len() <= MAX_QUERY_DOCUMENT_PATH_BYTES
                        && !path.contains('\0')
                })
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(path: String) -> QueryDocumentGate {
        QueryDocumentGate {
            document: StableDocumentKey::from_bytes([7; 32]).unwrap(),
            material_source_version: 3,
            current_source_version: 5,
            live: true,
            source_path: Some(path.clone()),
            result_path: Some(path),
            result_version: 5,
        }
    }

    #[test]
    fn identity_round_trip_binds_material_current_and_result_versions() {
        let expected = gate("objects/a.json".into());
        let encoded = encode_document_gate(expected.clone()).unwrap();
        assert_eq!(
            decode_document_gate(QueryBlockRecordRef {
                key: &encoded.key,
                value: &encoded.value,
            })
            .unwrap(),
            expected
        );
    }

    #[test]
    fn path_limit_matches_and_enforces_the_public_contract() {
        assert_eq!(MAX_QUERY_DOCUMENT_PATH_BYTES, 4_096);
        assert!(encode_document_gate(gate("p".repeat(4_096))).is_ok());
        assert!(encode_document_gate(gate("p".repeat(4_097))).is_err());
    }
}
