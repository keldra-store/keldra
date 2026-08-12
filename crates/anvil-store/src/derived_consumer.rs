use thiserror::Error;

use crate::{PlacementLogId, SourceId};

/// The checkpoint encoding uses one non-zero `u16` identity per ACTIVE node.
pub const MAX_DERIVED_CONSUMER_NODES: usize = u16::MAX as usize;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum DerivedConsumerKind {
    Index = 1,
    Accounting = 2,
}

impl DerivedConsumerKind {
    pub const ALL: [Self; 2] = [Self::Index, Self::Accounting];
}

/// One consumer node's aggregate retention proof for one source journal.
/// `next_offset` is the first source offset not covered by the proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DerivedConsumerCheckpoint {
    pub consumer_kind: DerivedConsumerKind,
    pub source_id: SourceId,
    pub consumer_node_id: u16,
    pub next_offset: u64,
    pub observed_fence: PlacementLogId,
}

impl DerivedConsumerCheckpoint {
    pub fn validate(self) -> Result<(), DerivedConsumerError> {
        if self.source_id.node_id == 0
            || self.source_id.source_epoch == [0; 32]
            || self.consumer_node_id == 0
            || self.observed_fence.term == 0
            || self.observed_fence.index == 0
            || self.next_offset == 0
        {
            return Err(DerivedConsumerError::Malformed(
                "derived consumer checkpoint identity is invalid".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedConsumerStatus {
    pub source_id: SourceId,
    pub observed_fence: PlacementLogId,
    pub active_consumer_nodes: Vec<u16>,
    pub index_safe_through: u64,
    pub accounting_safe_through: u64,
}

/// Point-in-time, node-local source-journal capacity and safety signals.
/// These values are observational and never participate in pruning decisions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceJournalRuntimeMetrics {
    pub tail: u64,
    pub settled_through: u64,
    pub retention_floor: u64,
    pub reference_safe_through: u64,
    pub index_safe_through: u64,
    pub accounting_safe_through: u64,
    pub retained_entries: u64,
    pub retained_bytes: u64,
    pub max_entries: u64,
    pub max_bytes: u64,
}

impl SourceJournalRuntimeMetrics {
    pub fn prune_safe_through(self) -> u64 {
        self.settled_through
            .min(self.reference_safe_through)
            .min(self.index_safe_through)
            .min(self.accounting_safe_through)
    }
}

impl DerivedConsumerStatus {
    pub fn safe_through(&self) -> u64 {
        self.index_safe_through.min(self.accounting_safe_through)
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DerivedConsumerError {
    #[error("derived-consumer record is malformed: {0}")]
    Malformed(String),
    #[error("derived-consumer checkpoint names another source journal")]
    SourceMismatch,
    #[error("derived-consumer membership fence would regress")]
    FenceRegression,
    #[error("derived-consumer ACTIVE set disagrees at the same membership fence")]
    MembershipMismatch,
    #[error("derived-consumer checkpoint comes from a node outside the ACTIVE set")]
    InactiveConsumer,
    #[error("derived-consumer checkpoint would regress")]
    CheckpointRegression,
    #[error("derived-consumer checkpoint is below the retained source floor")]
    CheckpointExpired,
    #[error("derived-consumer checkpoint is beyond the settled source tail")]
    CheckpointFuture,
    #[error("derived-consumer storage failed: {0}")]
    Storage(String),
}
