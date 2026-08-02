use bincode::Options;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

pub(crate) const MAX_ENCODED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum CodecError {
    #[error("encoded consensus value exceeds {MAX_ENCODED_BYTES} bytes")]
    TooLarge,
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

#[cfg(test)]
mod tests {
    use crate::{
        BundleHash, BundleRef, Command, CommitBatch, DurabilityClass, DurabilityEvidenceHash,
        InvocationFingerprint, InvocationId, NodeId, ProgramHash, ProgramPathHash,
    };

    use super::*;

    #[test]
    fn commit_batch_is_a_small_fixed_identity_record() {
        let command = Command::CommitBatch(CommitBatch {
            executor: NodeId(1),
            nomination_log_index: 2,
            program_path_hash: ProgramPathHash([3; 32]),
            program_hash: ProgramHash([4; 32]),
            invocation_id: InvocationId([5; 32]),
            input_fingerprint: InvocationFingerprint([6; 32]),
            bundle_ref: BundleRef {
                hash: [7; 32],
                length: 17,
            },
            bundle_hash: BundleHash([8; 32]),
            durability_class: DurabilityClass([9; 32]),
            durability_evidence_hash: DurabilityEvidenceHash([10; 32]),
            proposal_at_unix_millis: 1_000,
            replay_expires_at_unix_millis: 1_000 + crate::ATOMIC_REPLAY_RETENTION_MILLIS,
        });

        // The type contains only fixed-size identities and cursors. This
        // regression bound makes accidentally adding an object body, path
        // inventory, program definition, or prepared bundle immediately
        // visible in the consensus crate's tests.
        assert!(encode(&command).unwrap().len() < 512);
    }
}
