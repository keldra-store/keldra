use thiserror::Error;

use crate::{DefinitionKind, LocalChange, OversizeLocalChange, SourceId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalRoute {
    Definition(DefinitionKind),
    Bucket { tenant_id: u64, bucket_id: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedLocalChangePage {
    pub source_id: SourceId,
    pub changes: Vec<LocalChange>,
    pub encoded_bytes: u64,
    /// Highest source offset for which this page proves that every matching
    /// route was returned. It can advance on an empty sparse page.
    pub through_offset: u64,
    pub oversize: Option<OversizeLocalChange>,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RoutedJournalError {
    #[error("routed journal limits are invalid")]
    InvalidLimits,
    #[error("routed journal source node does not match this store")]
    SourceNodeMismatch,
    #[error("routed journal source epoch does not match the retained journal")]
    SourceEpochMismatch,
    #[error("routed journal cursor {cursor} is below retention floor {retention_floor}")]
    CursorExpired { cursor: u64, retention_floor: u64 },
    #[error("routed journal cursor {cursor} is beyond tail {tail}")]
    CursorFuture { cursor: u64, tail: u64 },
    #[error("routed journal target {target} precedes cursor {cursor}")]
    TargetBeforeCursor { cursor: u64, target: u64 },
    #[error("routed journal target {target} is beyond tail {tail}")]
    TargetFuture { target: u64, tail: u64 },
    #[error("routed journal offset {offset} has no authoritative source event")]
    MissingPrimary { offset: u64 },
    #[error("routed journal route disagrees with source event at offset {offset}")]
    RouteMismatch { offset: u64 },
    #[error("routed journal storage failed: {0}")]
    Storage(String),
}
