use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{BlobRef, MutationError, ShardIdentity, SourceId};

/// One logical content-reference effect stored in a source journal.
///
/// Placement expands this blob identity into the exact physical artifacts
/// held by one destination. Shard identities never enter the source journal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceDelta {
    pub blob: BlobRef,
    pub change: i64,
}

/// One exact physical content artifact held by a destination node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "identity", rename_all = "snake_case")]
pub enum DestinationReferenceArtifact {
    /// A complete small-object copy or complete large-object upload source.
    CompleteBlob(BlobRef),
    /// One independently placed erasure-coded shard.
    Shard(ShardIdentity),
}

/// One reference-count effect for an exact destination artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationReferenceDelta {
    pub artifact: DestinationReferenceArtifact,
    pub change: i64,
}

/// One contiguous source-journal prefix advance and the effects relevant to
/// this destination. Events for other destinations are represented only by
/// advancing `through`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceDeltaBatch {
    pub source: SourceId,
    pub after: u64,
    pub through: u64,
    pub deltas: Vec<DestinationReferenceDelta>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceDeltaApplied {
    pub through: u64,
    pub replayed: bool,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ReferenceDeltaError {
    #[error("reference-delta range ends before it starts")]
    InvalidRange,
    #[error("reference-delta change must not be zero")]
    ZeroChange,
    #[error("reference-delta artifact identity is invalid")]
    InvalidArtifact,
    #[error("reference-delta source gap: expected after {expected}, received {received}")]
    Gap { expected: u64, received: u64 },
    #[error(
        "reference-delta batch partially overlaps the durable cursor {cursor}: after {after}, through {through}"
    )]
    PartialOverlap {
        cursor: u64,
        after: u64,
        through: u64,
    },
    #[error("reference-delta count overflow")]
    Overflow,
    #[error("reference-delta count underflow")]
    Underflow,
    #[error("reference-delta references an artifact that is not sealed on this node")]
    ArtifactNotFound,
    #[error("reference-delta storage error: {0}")]
    Storage(String),
}

impl From<MutationError> for ReferenceDeltaError {
    fn from(error: MutationError) -> Self {
        match error {
            MutationError::BlobNotFound => Self::ArtifactNotFound,
            other => Self::Storage(other.to_string()),
        }
    }
}
