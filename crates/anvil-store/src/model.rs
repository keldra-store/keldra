use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{BlobRef, ObjectKey};

/// Values at or below this size stay inside the metadata WriteBatch, avoiding
/// one filesystem fsync per small bulk item.
pub const INLINE_PAYLOAD_MAX_BYTES: usize = 64 * 1024;

/// A bucket policy is deliberately small enough to validate on every write.
pub const MAX_BUCKET_POLICY_PREFIXES: usize = 64;
/// Combined UTF-8 bytes across both prefix lists in one bucket policy.
pub const MAX_BUCKET_POLICY_PREFIX_BYTES: usize = 8 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InlinePayload {
    #[serde(with = "base64_bytes")]
    pub bytes: Vec<u8>,
    pub hash: [u8; 32],
    pub length: u64,
}

mod base64_bytes {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        STANDARD.decode(encoded).map_err(serde::de::Error::custom)
    }
}

impl InlinePayload {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            hash: *blake3::hash(&bytes).as_bytes(),
            length: bytes.len() as u64,
            bytes,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.bytes.len() <= INLINE_PAYLOAD_MAX_BYTES
            && self.bytes.len() as u64 == self.length
            && blake3::hash(&self.bytes).as_bytes() == &self.hash
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VersionId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head {
    pub version: VersionId,
    pub deleted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub id: VersionId,
    pub blob: Option<BlobRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline: Option<InlinePayload>,
    pub content_type: Option<String>,
    pub deleted: bool,
    pub committed_at_unix_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Object {
    pub key: ObjectKey,
    pub version: Version,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Precondition {
    #[default]
    Any,
    Absent,
    Version(VersionId),
}

#[derive(Clone, Debug)]
pub struct PutRequest {
    pub key: ObjectKey,
    pub bytes: Vec<u8>,
    pub content_type: Option<String>,
    pub precondition: Precondition,
    pub command_id: Option<String>,
    pub durability_class: String,
}

#[derive(Clone, Debug)]
pub struct PublishRequest {
    pub key: ObjectKey,
    pub blob: BlobRef,
    pub content_type: Option<String>,
    pub precondition: Precondition,
    pub command_id: Option<String>,
    pub durability_class: String,
}

#[derive(Clone, Debug)]
pub struct DeleteRequest {
    pub key: ObjectKey,
    pub precondition: Precondition,
    pub command_id: Option<String>,
    pub durability_class: String,
}

#[derive(Clone, Debug)]
pub enum BatchOperation {
    Put(PutRequest),
    Publish(PublishRequest),
    Delete(DeleteRequest),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchOutcome {
    pub index: usize,
    pub result: Result<MutationReceipt, MutationError>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationReceipt {
    pub command_id: Option<String>,
    pub fingerprint: [u8; 32],
    pub version: VersionId,
    pub deleted: bool,
    pub replayed: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketPolicy {
    /// Canonical prefixes whose matching paths can be created exactly once.
    #[serde(default)]
    pub create_once_prefixes: Vec<String>,
    /// Canonical prefixes writable only through an invoked atomic program.
    #[serde(default)]
    pub program_only_prefixes: Vec<String>,
}

impl BucketPolicy {
    pub fn validate(&self) -> Result<(), MutationError> {
        let prefix_count = self
            .create_once_prefixes
            .len()
            .saturating_add(self.program_only_prefixes.len());
        if prefix_count > MAX_BUCKET_POLICY_PREFIXES {
            return Err(MutationError::InvalidPolicy(format!(
                "a bucket policy may contain at most {MAX_BUCKET_POLICY_PREFIXES} prefixes in total"
            )));
        }
        let prefixes = self
            .create_once_prefixes
            .iter()
            .chain(&self.program_only_prefixes);
        let encoded_bytes =
            prefixes.fold(0_usize, |total, prefix| total.saturating_add(prefix.len()));
        if encoded_bytes > MAX_BUCKET_POLICY_PREFIX_BYTES {
            return Err(MutationError::InvalidPolicy(format!(
                "bucket policy prefixes may contain at most {MAX_BUCKET_POLICY_PREFIX_BYTES} UTF-8 bytes in total"
            )));
        }
        validate_prefixes("immutable", &self.create_once_prefixes)?;
        validate_prefixes("program-only", &self.program_only_prefixes)?;
        for immutable in &self.create_once_prefixes {
            for program_only in &self.program_only_prefixes {
                if prefix_matches(immutable, program_only)
                    || prefix_matches(program_only, immutable)
                {
                    return Err(MutationError::InvalidPolicy(format!(
                        "immutable prefix `{immutable}` and program-only prefix `{program_only}` must not overlap"
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn is_create_once(&self, path: &str) -> bool {
        matches_prefix(&self.create_once_prefixes, path)
    }

    pub fn is_program_only(&self, path: &str) -> bool {
        matches_prefix(&self.program_only_prefixes, path)
    }
}

fn validate_prefixes(kind: &str, prefixes: &[String]) -> Result<(), MutationError> {
    let mut previous: Option<&str> = None;
    for prefix in prefixes {
        if prefix.is_empty()
            || prefix.starts_with('/')
            || prefix.ends_with('/')
            || prefix.contains("//")
        {
            return Err(MutationError::InvalidPolicy(format!(
                "{kind} prefixes must be canonical and non-empty"
            )));
        }
        if let Some(previous) = previous
            && prefix.as_str() <= previous
        {
            return Err(MutationError::InvalidPolicy(format!(
                "{kind} prefixes must be sorted and unique"
            )));
        }
        previous = Some(prefix);
    }
    Ok(())
}

fn matches_prefix(prefixes: &[String], path: &str) -> bool {
    prefixes.iter().any(|prefix| prefix_matches(prefix, path))
}

fn prefix_matches(prefix: &str, path: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum MutationError {
    #[error("precondition failed; current version is {current:?}")]
    PreconditionFailed { current: Option<VersionId> },
    #[error("path belongs to a create-once namespace")]
    Immutable,
    #[error("ordinary mutation cannot write a PROGRAM_ONLY path")]
    ProgramConcurrencyViolation,
    #[error("command id was reused with different input")]
    IdempotencyConflict,
    #[error("invalid command id")]
    InvalidCommandId,
    #[error("blob is not present on this node")]
    BlobNotFound,
    #[error("invalid bucket policy: {0}")]
    InvalidPolicy(String),
    #[error("storage error: {0}")]
    Storage(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numbered_prefixes(label: &str, count: usize) -> Vec<String> {
        (0..count)
            .map(|index| format!("{label}-{index:03}"))
            .collect()
    }

    #[test]
    fn inline_payload_json_encoding_is_base64_sized_and_round_trips() {
        let payload = InlinePayload::new(vec![0xabu8; INLINE_PAYLOAD_MAX_BYTES]);
        let encoded = serde_json::to_vec(&payload).unwrap();
        assert!(encoded.len() <= INLINE_PAYLOAD_MAX_BYTES * 4 / 3 + 512);
        let decoded: InlinePayload = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, payload);
        assert!(decoded.is_valid());
    }

    #[test]
    fn bucket_policy_prefix_count_bound_applies_to_both_lists_together() {
        let boundary = BucketPolicy {
            create_once_prefixes: numbered_prefixes("immutable", 32),
            program_only_prefixes: numbered_prefixes("program", 32),
        };
        assert_eq!(
            boundary.create_once_prefixes.len() + boundary.program_only_prefixes.len(),
            MAX_BUCKET_POLICY_PREFIXES
        );
        boundary.validate().unwrap();

        let over_limit = BucketPolicy {
            create_once_prefixes: numbered_prefixes("immutable", 32),
            program_only_prefixes: numbered_prefixes("program", 33),
        };
        assert!(matches!(
            over_limit.validate(),
            Err(MutationError::InvalidPolicy(message))
                if message.contains("at most 64 prefixes")
        ));
    }

    #[test]
    fn bucket_policy_byte_bound_counts_utf8_bytes_across_both_lists() {
        let half = MAX_BUCKET_POLICY_PREFIX_BYTES / 2;
        let boundary = BucketPolicy {
            create_once_prefixes: vec!["a".repeat(half)],
            program_only_prefixes: vec!["é".repeat(half / "é".len())],
        };
        let boundary_bytes = boundary
            .create_once_prefixes
            .iter()
            .chain(&boundary.program_only_prefixes)
            .map(String::len)
            .sum::<usize>();
        assert_eq!(boundary_bytes, MAX_BUCKET_POLICY_PREFIX_BYTES);
        boundary.validate().unwrap();

        let over_limit = BucketPolicy {
            create_once_prefixes: boundary.create_once_prefixes,
            program_only_prefixes: vec![format!("{}x", boundary.program_only_prefixes[0])],
        };
        assert!(matches!(
            over_limit.validate(),
            Err(MutationError::InvalidPolicy(message))
                if message.contains("UTF-8 bytes")
        ));
    }

    #[test]
    fn bucket_policy_rejects_overlap_between_immutable_and_program_only_prefixes() {
        for (immutable, program_only) in [
            ("shared", "shared"),
            ("shared", "shared/child"),
            ("shared/child", "shared"),
        ] {
            let policy = BucketPolicy {
                create_once_prefixes: vec![immutable.into()],
                program_only_prefixes: vec![program_only.into()],
            };
            assert!(matches!(
                policy.validate(),
                Err(MutationError::InvalidPolicy(message))
                    if message.contains("must not overlap")
            ));
        }

        BucketPolicy {
            create_once_prefixes: vec!["shared/child".into()],
            program_only_prefixes: vec!["shared/childish".into()],
        }
        .validate()
        .unwrap();
    }
}
