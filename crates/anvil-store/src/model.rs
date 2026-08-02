use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{BlobRef, ObjectKey};

/// A bucket policy is deliberately small enough to validate on every write.
pub const MAX_BUCKET_POLICY_PREFIXES: usize = 64;
/// Combined UTF-8 bytes across both prefix lists in one bucket policy.
pub const MAX_BUCKET_POLICY_PREFIX_BYTES: usize = 8 * 1024;
/// Payloads at or below this size use the RocksDB-backed small-byte plane.
pub const SMALL_BLOB_MAX_BYTES: usize = 64 * 1024;

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
    pub content_type: Option<String>,
    pub deleted: bool,
    pub committed_at_unix_millis: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectVersioning {
    #[default]
    Unversioned,
    Enabled,
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

/// The four explicit public put operations. Keeping intent distinct from the
/// derived head precondition is necessary because PutIfAbsent and PutImmutable
/// both compare against absence but have different path-policy admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PutMode {
    Put,
    PutIfAbsent,
    PutIfVersion(VersionId),
    PutImmutable,
}

impl PutMode {
    pub fn precondition(self) -> Precondition {
        match self {
            Self::Put => Precondition::Any,
            Self::PutIfAbsent | Self::PutImmutable => Precondition::Absent,
            Self::PutIfVersion(version) => Precondition::Version(version),
        }
    }
}

/// Per-request durability for ordinary object mutations. Local is deliberately
/// the default fast path. Replicated is retained in the stable API but cannot
/// be satisfied by the single-node 0.5.0 store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    #[default]
    Local,
    Replicated,
}

#[derive(Clone, Debug)]
pub struct PutRequest {
    pub key: ObjectKey,
    pub bytes: Vec<u8>,
    pub content_type: Option<String>,
    pub mode: PutMode,
    pub command_id: Option<String>,
    pub durability: Durability,
}

#[derive(Clone, Debug)]
pub struct PublishRequest {
    pub key: ObjectKey,
    pub blob: BlobRef,
    pub content_type: Option<String>,
    pub mode: PutMode,
    pub command_id: Option<String>,
    pub durability: Durability,
}

#[derive(Clone, Debug)]
pub struct DeleteRequest {
    pub key: ObjectKey,
    pub precondition: Precondition,
    pub command_id: Option<String>,
    pub durability: Durability,
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
    /// Zero only for internal callers that deliberately omitted a command ID.
    pub replay_guarantee_expires_at_unix_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeleteRetainedVersionOutcome {
    NotFound,
    DeletedNonCurrent,
    ReplacedCurrentWithTombstone { version: VersionId },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketPolicy {
    /// Canonical prefixes whose matching paths can be created exactly once.
    #[serde(default)]
    pub immutable_prefixes: Vec<String>,
    /// Canonical prefixes writable only through an invoked atomic program.
    #[serde(default)]
    pub program_only_prefixes: Vec<String>,
}

impl BucketPolicy {
    pub fn validate(&self) -> Result<(), MutationError> {
        let prefix_count = self
            .immutable_prefixes
            .len()
            .saturating_add(self.program_only_prefixes.len());
        if prefix_count > MAX_BUCKET_POLICY_PREFIXES {
            return Err(MutationError::InvalidPolicy(format!(
                "a bucket policy may contain at most {MAX_BUCKET_POLICY_PREFIXES} prefixes in total"
            )));
        }
        let prefixes = self
            .immutable_prefixes
            .iter()
            .chain(&self.program_only_prefixes);
        let encoded_bytes =
            prefixes.fold(0_usize, |total, prefix| total.saturating_add(prefix.len()));
        if encoded_bytes > MAX_BUCKET_POLICY_PREFIX_BYTES {
            return Err(MutationError::InvalidPolicy(format!(
                "bucket policy prefixes may contain at most {MAX_BUCKET_POLICY_PREFIX_BYTES} UTF-8 bytes in total"
            )));
        }
        validate_prefixes("immutable", &self.immutable_prefixes)?;
        validate_prefixes("program-only", &self.program_only_prefixes)?;
        for immutable in &self.immutable_prefixes {
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

    pub fn is_immutable(&self, path: &str) -> bool {
        matches_prefix(&self.immutable_prefixes, path)
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
    #[error("path belongs to an immutable namespace")]
    Immutable,
    #[error("PutImmutable requires an immutable path policy")]
    ImmutablePolicyRequired,
    #[error("ordinary mutation cannot write a PROGRAM_ONLY path")]
    ProgramConcurrencyViolation,
    #[error("command id was reused with different input")]
    IdempotencyConflict,
    #[error("invalid command id")]
    InvalidCommandId,
    #[error("blob is not present on this node")]
    BlobNotFound,
    #[error("requested durability is unavailable")]
    DurabilityUnavailable,
    #[error("mutation receipt capacity is exhausted by unexpired guarantees")]
    ReceiptCapacity,
    #[error("source journal capacity is exhausted before required consumers are durable")]
    SourceJournalCapacity,
    #[error("object versioning is not enabled for this bucket")]
    ObjectVersioningNotEnabled,
    #[error("the current tombstone is the path's CAS/ABA fence and cannot be deleted")]
    CurrentTombstoneCannotBeDeleted,
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
    fn bucket_policy_prefix_count_bound_applies_to_both_lists_together() {
        let boundary = BucketPolicy {
            immutable_prefixes: numbered_prefixes("immutable", 32),
            program_only_prefixes: numbered_prefixes("program", 32),
        };
        assert_eq!(
            boundary.immutable_prefixes.len() + boundary.program_only_prefixes.len(),
            MAX_BUCKET_POLICY_PREFIXES
        );
        boundary.validate().unwrap();

        let over_limit = BucketPolicy {
            immutable_prefixes: numbered_prefixes("immutable", 32),
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
            immutable_prefixes: vec!["a".repeat(half)],
            program_only_prefixes: vec!["é".repeat(half / "é".len())],
        };
        let boundary_bytes = boundary
            .immutable_prefixes
            .iter()
            .chain(&boundary.program_only_prefixes)
            .map(String::len)
            .sum::<usize>();
        assert_eq!(boundary_bytes, MAX_BUCKET_POLICY_PREFIX_BYTES);
        boundary.validate().unwrap();

        let over_limit = BucketPolicy {
            immutable_prefixes: boundary.immutable_prefixes,
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
                immutable_prefixes: vec![immutable.into()],
                program_only_prefixes: vec![program_only.into()],
            };
            assert!(matches!(
                policy.validate(),
                Err(MutationError::InvalidPolicy(message))
                    if message.contains("must not overlap")
            ));
        }

        BucketPolicy {
            immutable_prefixes: vec!["shared/child".into()],
            program_only_prefixes: vec!["shared/childish".into()],
        }
        .validate()
        .unwrap();
    }
}
