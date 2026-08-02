use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{BlobRef, MutationError, SourceId};

/// One reference-count effect selected from a source journal for this owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceDelta {
    pub blob: BlobRef,
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
    pub deltas: Vec<ReferenceDelta>,
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
    #[error("reference-delta references bytes that are not sealed on this node")]
    BlobNotFound,
    #[error("reference-delta storage error: {0}")]
    Storage(String),
}

impl From<MutationError> for ReferenceDeltaError {
    fn from(error: MutationError) -> Self {
        match error {
            MutationError::BlobNotFound => Self::BlobNotFound,
            other => Self::Storage(other.to_string()),
        }
    }
}
