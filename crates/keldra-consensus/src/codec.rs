use bincode::Options;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

pub(crate) const MAX_ENCODED_BYTES: usize = 64 * 1024 * 1024;

const RECORD_MAGIC: &[u8; 8] = b"ANVLREC\0";
const RECORD_FORMAT_V1: u8 = 1;
const RECORD_LENGTH_BYTES: usize = std::mem::size_of::<u32>();
const RECORD_HEADER_BYTES: usize = RECORD_MAGIC.len() + 1 + RECORD_LENGTH_BYTES;
const MAX_RECORD_BYTES: usize = RECORD_HEADER_BYTES + MAX_ENCODED_BYTES;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum CodecError {
    #[error("encoded consensus value exceeds {MAX_ENCODED_BYTES} bytes")]
    TooLarge,
    #[error("unsupported consensus record format version {0}")]
    UnsupportedRecordVersion(u8),
    #[error("invalid consensus record envelope: {0}")]
    InvalidRecord(&'static str),
    #[error("consensus binary codec error: {0}")]
    Invalid(String),
}

fn options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_ENCODED_BYTES as u64)
        .reject_trailing_bytes()
}

pub(crate) fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, CodecError> {
    let bytes = options()
        .serialize(value)
        .map_err(|error| CodecError::Invalid(error.to_string()))?;
    if bytes.len() > MAX_ENCODED_BYTES {
        return Err(CodecError::TooLarge);
    }
    Ok(bytes)
}

pub(crate) fn encoded_len<T: Serialize + ?Sized>(value: &T) -> Result<u64, CodecError> {
    let bytes = options()
        .serialized_size(value)
        .map_err(|error| CodecError::Invalid(error.to_string()))?;
    if bytes > MAX_ENCODED_BYTES as u64 {
        return Err(CodecError::TooLarge);
    }
    Ok(bytes)
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    if bytes.len() > MAX_ENCODED_BYTES {
        return Err(CodecError::TooLarge);
    }
    options()
        .deserialize(bytes)
        .map_err(|error| CodecError::Invalid(error.to_string()))
}

/// Encode a value for durable consensus storage.
///
/// `encode` remains the raw payload codec for in-memory size accounting.
/// Durable records always use this self-identifying v1 envelope.
pub(crate) fn encode_record<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, CodecError> {
    wrap_record(&encode(value)?)
}

/// Encode a value under an explicitly selected durable record format.
#[cfg(test)]
pub(crate) fn encode_record_at_version<T: Serialize + ?Sized>(
    value: &T,
    version: u8,
) -> Result<Vec<u8>, CodecError> {
    wrap_record_at_version(&encode(value)?, version)
}

/// Decode one durable v1 record.
pub(crate) fn decode_record<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    decode(record_payload(bytes)?)
}

/// Wrap an already encoded payload without changing its bytes.
pub(crate) fn wrap_record(payload: &[u8]) -> Result<Vec<u8>, CodecError> {
    wrap_record_at_version(payload, RECORD_FORMAT_V1)
}

fn wrap_record_at_version(payload: &[u8], version: u8) -> Result<Vec<u8>, CodecError> {
    if payload.len() > MAX_ENCODED_BYTES {
        return Err(CodecError::TooLarge);
    }

    let payload_len = u32::try_from(payload.len()).map_err(|_| CodecError::TooLarge)?;
    let mut record = Vec::with_capacity(RECORD_HEADER_BYTES + payload.len());
    record.extend_from_slice(RECORD_MAGIC);
    record.push(version);
    record.extend_from_slice(&payload_len.to_be_bytes());
    record.extend_from_slice(payload);
    Ok(record)
}

/// Return the unchanged payload from a v1 envelope.
pub(crate) fn record_payload(bytes: &[u8]) -> Result<&[u8], CodecError> {
    let (version, payload) = record_version_and_payload(bytes)?;
    match version {
        RECORD_FORMAT_V1 => Ok(payload),
        version => Err(CodecError::UnsupportedRecordVersion(version)),
    }
}

/// Return an envelope's explicit format and unchanged payload.
pub(crate) fn record_version_and_payload(bytes: &[u8]) -> Result<(u8, &[u8]), CodecError> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(CodecError::TooLarge);
    }
    if !bytes.starts_with(RECORD_MAGIC) {
        return Err(CodecError::InvalidRecord("magic is invalid"));
    }
    if bytes.len() < RECORD_HEADER_BYTES {
        return Err(CodecError::InvalidRecord("truncated header"));
    }

    let version = bytes[RECORD_MAGIC.len()];

    let length_start = RECORD_MAGIC.len() + 1;
    let length_end = length_start + RECORD_LENGTH_BYTES;
    let payload_len = u32::from_be_bytes(
        bytes[length_start..length_end]
            .try_into()
            .expect("record length field has a fixed width"),
    ) as usize;
    if payload_len > MAX_ENCODED_BYTES {
        return Err(CodecError::TooLarge);
    }
    if bytes.len() != RECORD_HEADER_BYTES + payload_len {
        return Err(CodecError::InvalidRecord(
            "payload length does not match header",
        ));
    }
    Ok((version, &bytes[RECORD_HEADER_BYTES..]))
}

#[cfg(test)]
mod tests {
    use openraft::{CommittedLeaderId, EntryPayload, LogId};

    use crate::{
        Command, NodeId,
        raft_storage::{RaftEntry, StorageConfig},
    };

    use super::*;

    #[test]
    fn durable_record_envelope_preserves_the_raw_payload() {
        let value = StorageConfig {
            max_commit_entries: 4,
            max_commit_bytes: 65_536,
        };
        let raw = encode(&value).unwrap();
        let record = encode_record(&value).unwrap();

        assert_eq!(encoded_len(&value).unwrap(), raw.len() as u64);
        assert_eq!(record_payload(&record).unwrap(), raw);
        assert_eq!(decode_record::<StorageConfig>(&record).unwrap(), value);
        assert_eq!(record.len(), raw.len() + RECORD_HEADER_BYTES);
    }

    #[test]
    fn raw_unenveloped_storage_records_are_rejected() {
        const RAW_STORAGE_CONFIG: &[u8] = &[
            0x04, 0x00, 0x00, 0x00, // max_commit_entries
            0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, // max_commit_bytes
        ];
        const RAW_BLANK_LOG_ENTRY: &[u8] = &[
            0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // leader term
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // leader node id
            0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log index
            0x00, 0x00, 0x00, 0x00, // EntryPayload::Blank
        ];

        assert_eq!(
            decode_record::<StorageConfig>(RAW_STORAGE_CONFIG),
            Err(CodecError::InvalidRecord("magic is invalid"))
        );
        assert_eq!(
            decode_record::<RaftEntry>(RAW_BLANK_LOG_ENTRY),
            Err(CodecError::InvalidRecord("magic is invalid"))
        );
    }

    #[test]
    fn malformed_or_unknown_envelopes_fail_closed() {
        let mut unknown = wrap_record(b"value").unwrap();
        unknown[RECORD_MAGIC.len()] = RECORD_FORMAT_V1 + 1;
        assert_eq!(
            record_payload(&unknown),
            Err(CodecError::UnsupportedRecordVersion(RECORD_FORMAT_V1 + 1))
        );

        let mut wrong_length = wrap_record(b"value").unwrap();
        wrong_length.pop();
        assert_eq!(
            record_payload(&wrong_length),
            Err(CodecError::InvalidRecord(
                "payload length does not match header"
            ))
        );

        assert_eq!(
            record_payload(RECORD_MAGIC),
            Err(CodecError::InvalidRecord("truncated header"))
        );
    }

    #[test]
    fn v1_command_discriminants_are_stable() {
        let commands = [
            Command::NominateExecutor {
                executor: NodeId(1),
            },
            Command::FinalizedThrough {
                executor: NodeId(1),
                nomination_log_index: 2,
                through_commit_cursor: 3,
            },
            Command::InitializeCluster {
                cluster_id: crate::ClusterId([4; 16]),
            },
            Command::CompleteSystemBootstrap {
                executor: NodeId(1),
                nomination_log_index: 2,
                bootstrap_version: 1,
            },
        ];

        for (expected, command) in commands.into_iter().enumerate() {
            assert_eq!(
                &encode(&command).unwrap()[..4],
                &(expected as u32).to_le_bytes()
            );
        }
    }
}
