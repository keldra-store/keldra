//! Disposable sparse-route aggregation for derived-consumer retention.
//!
//! The tracker contains only definitions with a routed effect not yet covered
//! by a published source-complete commit or rollup. Construction snapshots
//! are not durable publication evidence and therefore cannot release retained
//! source history. The tracker cannot emit a checkpoint until the caller has
//! completed one explicit assigned-definition inventory.

use std::collections::BTreeMap;

use keldra_consensus::NodeId;
use keldra_store::{
    DefinitionAssignment, DefinitionCheckpoint, DefinitionConsumerKind, DefinitionKind,
    DerivedConsumerCheckpoint, DerivedConsumerKind, PlacementLogId, SourceId, VersionId,
    WatchJournalStatus,
};
use thiserror::Error;

use crate::index_runtime::events::IndexBarrier;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DerivedDefinitionIdentity {
    pub(crate) kind: DefinitionKind,
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) definition_id: u64,
    pub(crate) object_version: VersionId,
}

impl DerivedDefinitionIdentity {
    pub(crate) fn from_assignment(assignment: &DefinitionAssignment) -> Self {
        Self {
            kind: assignment.kind,
            tenant_id: assignment.tenant_id,
            bucket_id: assignment.bucket_id,
            definition_id: assignment.definition_id,
            object_version: assignment.object_version,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DerivedBarrierEvidence {
    Published(IndexBarrier),
}

impl DerivedBarrierEvidence {
    fn barrier(&self) -> &IndexBarrier {
        match self {
            Self::Published(barrier) => barrier,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingEffect {
    /// First source offset not represented by the current proof.
    proof_next: u64,
    /// First source offset after the newest routed effect observed so far.
    required_next: u64,
    required_atomic_cursor: Option<u64>,
    atomic_hold_next: Option<u64>,
    proof_atomic_through: Option<u64>,
}

impl PendingEffect {
    fn is_satisfied(self) -> bool {
        self.proof_next >= self.required_next
            && self.required_atomic_cursor.is_none_or(|required| {
                self.proof_atomic_through
                    .is_some_and(|through| through >= required)
            })
    }

    fn retention_next(self) -> u64 {
        let source_hold = (self.proof_next < self.required_next).then_some(self.proof_next);
        let atomic_hold = self
            .required_atomic_cursor
            .is_some_and(|required| {
                !self
                    .proof_atomic_through
                    .is_some_and(|through| through >= required)
            })
            .then_some(self.atomic_hold_next)
            .flatten();
        source_hold
            .into_iter()
            .chain(atomic_hold)
            .min()
            .unwrap_or(self.proof_next)
    }
}

#[derive(Debug)]
struct SourceProgress {
    status: WatchJournalStatus,
    baseline_next: u64,
    /// True only when `baseline_next` was durably acknowledged for this exact
    /// consumer/source/fence identity. A newly fenced tracker must publish its
    /// initial floor even when there is no offset advance.
    baseline_acknowledged: bool,
    pending: BTreeMap<DerivedDefinitionIdentity, PendingEffect>,
    proof_minima: BTreeMap<u64, usize>,
}

impl SourceProgress {
    fn new(
        kind: DerivedConsumerKind,
        fence: PlacementLogId,
        status: WatchJournalStatus,
        current: Option<DefinitionCheckpoint>,
    ) -> Result<Self, SparseTrackerError> {
        validate_source(status)?;
        let floor_next = status
            .retention_floor
            .checked_add(1)
            .ok_or(SparseTrackerError::OffsetOverflow)?;
        let settled_next = status
            .settled_through
            .checked_add(1)
            .ok_or(SparseTrackerError::OffsetOverflow)?;
        let (baseline_next, baseline_acknowledged) = match current {
            Some(current)
                if current.consumer_kind == retention_kind(kind)
                    && current.source_id == status.source_id
                    && current.observed_fence == fence =>
            {
                (current.next_offset, true)
            }
            Some(current) if fence_order(current.observed_fence) > fence_order(fence) => {
                return Err(SparseTrackerError::FutureLocalFence);
            }
            _ => (floor_next, false),
        };
        if baseline_next < floor_next || baseline_next > settled_next {
            return Err(SparseTrackerError::InvalidLocalCheckpoint);
        }
        Ok(Self {
            status,
            baseline_next,
            baseline_acknowledged,
            pending: BTreeMap::new(),
            proof_minima: BTreeMap::new(),
        })
    }

    fn observe(
        &mut self,
        identity: DerivedDefinitionIdentity,
        required_next: u64,
        proof_next: u64,
        required_atomic_cursor: Option<u64>,
        atomic_hold_next: Option<u64>,
        proof_atomic_through: Option<u64>,
    ) -> Result<(), SparseTrackerError> {
        let settled_next = self
            .status
            .settled_through
            .checked_add(1)
            .ok_or(SparseTrackerError::OffsetOverflow)?;
        let floor_next = self
            .status
            .retention_floor
            .checked_add(1)
            .ok_or(SparseTrackerError::OffsetOverflow)?;
        if required_next <= floor_next || required_next > settled_next {
            return Err(SparseTrackerError::RoutedOffset {
                source_id: self.status.source_id,
                identity,
                required_next,
                floor_next,
                settled_next,
            });
        }
        // Publication reports race this tracker's 100 ms settled-status poll.
        // A valid same-source/fence proof may therefore be ahead of the
        // captured settled tail. It proves every currently settled offset but
        // must never advance the retention checkpoint beyond that tail.
        let proof_next = proof_next.min(settled_next).max(self.baseline_next);
        let previous = self.pending.remove(&identity);
        if let Some(previous) = previous {
            self.remove_minimum(previous.retention_next())?;
        }
        let effect = PendingEffect {
            proof_next: previous.map_or(proof_next, |previous| previous.proof_next.max(proof_next)),
            required_next: previous.map_or(required_next, |previous| {
                previous.required_next.max(required_next)
            }),
            required_atomic_cursor: previous.map_or(required_atomic_cursor, |previous| {
                previous.required_atomic_cursor.max(required_atomic_cursor)
            }),
            atomic_hold_next: previous
                .and_then(|previous| previous.atomic_hold_next)
                .into_iter()
                .chain(atomic_hold_next)
                .min(),
            proof_atomic_through: previous.map_or(proof_atomic_through, |previous| {
                previous.proof_atomic_through.max(proof_atomic_through)
            }),
        };
        if !effect.is_satisfied() {
            self.add_minimum(effect.retention_next())?;
            self.pending.insert(identity, effect);
        }
        Ok(())
    }

    fn observe_proof(
        &mut self,
        identity: DerivedDefinitionIdentity,
        proof_next: u64,
        proof_atomic_through: Option<u64>,
    ) -> Result<(), SparseTrackerError> {
        let settled_next = self
            .status
            .settled_through
            .checked_add(1)
            .ok_or(SparseTrackerError::OffsetOverflow)?;
        let proof_next = proof_next.min(settled_next);
        let Some(previous) = self.pending.remove(&identity) else {
            return Ok(());
        };
        self.remove_minimum(previous.retention_next())?;
        let effect = PendingEffect {
            proof_next: previous.proof_next.max(proof_next.max(self.baseline_next)),
            required_next: previous.required_next,
            required_atomic_cursor: previous.required_atomic_cursor,
            atomic_hold_next: previous.atomic_hold_next,
            proof_atomic_through: previous.proof_atomic_through.max(proof_atomic_through),
        };
        if !effect.is_satisfied() {
            self.add_minimum(effect.retention_next())?;
            self.pending.insert(identity, effect);
        }
        Ok(())
    }

    fn remove(&mut self, identity: DerivedDefinitionIdentity) -> Result<(), SparseTrackerError> {
        if let Some(previous) = self.pending.remove(&identity) {
            self.remove_minimum(previous.retention_next())?;
        }
        Ok(())
    }

    fn checkpoint_next(&self) -> Result<u64, SparseTrackerError> {
        let settled_next = self
            .status
            .settled_through
            .checked_add(1)
            .ok_or(SparseTrackerError::OffsetOverflow)?;
        Ok(self
            .proof_minima
            .first_key_value()
            .map_or(settled_next, |(minimum, _)| *minimum)
            .max(self.baseline_next))
    }

    fn update_status(&mut self, status: WatchJournalStatus) -> Result<(), SparseTrackerError> {
        validate_source(status)?;
        if status.source_id != self.status.source_id {
            return Err(SparseTrackerError::SourceChanged);
        }
        if status.retention_floor < self.status.retention_floor
            || status.settled_through < self.status.settled_through
            || status.tail < self.status.tail
        {
            return Err(SparseTrackerError::SourceRegression);
        }
        let floor_next = status
            .retention_floor
            .checked_add(1)
            .ok_or(SparseTrackerError::OffsetOverflow)?;
        if self.baseline_next < floor_next {
            return Err(SparseTrackerError::CheckpointExpired);
        }
        if self
            .pending
            .values()
            .any(|pending| pending.required_next > status.settled_through.saturating_add(1))
        {
            return Err(SparseTrackerError::SourceRegression);
        }
        self.status = status;
        Ok(())
    }

    fn add_minimum(&mut self, next: u64) -> Result<(), SparseTrackerError> {
        let count = self.proof_minima.entry(next).or_default();
        *count = count
            .checked_add(1)
            .ok_or(SparseTrackerError::CountOverflow)?;
        Ok(())
    }

    fn remove_minimum(&mut self, next: u64) -> Result<(), SparseTrackerError> {
        let count = self
            .proof_minima
            .get_mut(&next)
            .ok_or(SparseTrackerError::CorruptMinimum)?;
        *count = count
            .checked_sub(1)
            .ok_or(SparseTrackerError::CorruptMinimum)?;
        if *count == 0 {
            self.proof_minima.remove(&next);
        }
        Ok(())
    }
}

/// Inventory state intentionally has no checkpoint accessor. The caller must
/// visit the complete assigned-definition inventory before `finish` makes any
/// aggregate acknowledgement available.
pub(crate) struct SparseDerivedInventory {
    kind: DerivedConsumerKind,
    consumer_node: NodeId,
    fence: PlacementLogId,
    sources: BTreeMap<SourceId, SourceProgress>,
}

impl SparseDerivedInventory {
    pub(crate) fn begin(
        kind: DerivedConsumerKind,
        consumer_node: NodeId,
        fence: PlacementLogId,
        sources: impl IntoIterator<Item = (WatchJournalStatus, Option<DefinitionCheckpoint>)>,
    ) -> Result<Self, SparseTrackerError> {
        validate_identity(consumer_node, fence)?;
        let mut progress = BTreeMap::new();
        for (status, current) in sources {
            let source_id = status.source_id;
            if progress
                .insert(
                    source_id,
                    SourceProgress::new(kind, fence, status, current)?,
                )
                .is_some()
            {
                return Err(SparseTrackerError::DuplicateSource);
            }
        }
        if progress.is_empty() {
            return Err(SparseTrackerError::EmptySources);
        }
        Ok(Self {
            kind,
            consumer_node,
            fence,
            sources: progress,
        })
    }

    /// Records one definition/source pair only when a sparse route scan found
    /// an effect after the definition's proof. Unaffected definitions consume
    /// no tracker memory.
    pub(crate) fn record_affected(
        &mut self,
        assignment: &DefinitionAssignment,
        source_id: SourceId,
        latest_routed_next: u64,
        required_atomic_cursor: Option<u64>,
        atomic_hold_next: Option<u64>,
        evidence: Option<&DerivedBarrierEvidence>,
    ) -> Result<(), SparseTrackerError> {
        validate_assignment(self.kind, self.fence, assignment)?;
        let identity = DerivedDefinitionIdentity::from_assignment(assignment);
        let source = self
            .sources
            .get_mut(&source_id)
            .ok_or(SparseTrackerError::UnknownSource)?;
        let (proof_next, proof_atomic_through) = proof(self.fence, source, evidence)?;
        source.observe(
            identity,
            latest_routed_next,
            proof_next,
            required_atomic_cursor,
            atomic_hold_next,
            proof_atomic_through,
        )
    }

    pub(crate) fn finish(self) -> SparseDerivedTracker {
        SparseDerivedTracker {
            kind: self.kind,
            consumer_node: self.consumer_node,
            fence: self.fence,
            sources: self.sources,
        }
    }
}

/// Completed disposable tracker. Membership cutover creates a new inventory;
/// this value never changes its fence or ACTIVE-node identity in place.
pub(crate) struct SparseDerivedTracker {
    kind: DerivedConsumerKind,
    consumer_node: NodeId,
    fence: PlacementLogId,
    sources: BTreeMap<SourceId, SourceProgress>,
}

impl SparseDerivedTracker {
    pub(crate) fn observe_routed_effect(
        &mut self,
        assignment: &DefinitionAssignment,
        source_id: SourceId,
        routed_next: u64,
        required_atomic_cursor: Option<u64>,
        atomic_hold_next: Option<u64>,
        evidence: Option<&DerivedBarrierEvidence>,
    ) -> Result<(), SparseTrackerError> {
        validate_assignment(self.kind, self.fence, assignment)?;
        let identity = DerivedDefinitionIdentity::from_assignment(assignment);
        let source = self
            .sources
            .get_mut(&source_id)
            .ok_or(SparseTrackerError::UnknownSource)?;
        let (proof_next, proof_atomic_through) = proof(self.fence, source, evidence)?;
        source.observe(
            identity,
            routed_next,
            proof_next,
            required_atomic_cursor,
            atomic_hold_next,
            proof_atomic_through,
        )
    }

    pub(crate) fn observe_proof(
        &mut self,
        assignment: &DefinitionAssignment,
        evidence: &DerivedBarrierEvidence,
    ) -> Result<(), SparseTrackerError> {
        validate_assignment(self.kind, self.fence, assignment)?;
        let identity = DerivedDefinitionIdentity::from_assignment(assignment);
        self.observe_proof_identity(identity, evidence)
    }

    pub(crate) fn observe_proof_identity(
        &mut self,
        identity: DerivedDefinitionIdentity,
        evidence: &DerivedBarrierEvidence,
    ) -> Result<(), SparseTrackerError> {
        if identity.kind != definition_kind(self.kind) {
            return Err(SparseTrackerError::WrongAssignment);
        }
        for source in self.sources.values_mut() {
            let (next, atomic_through) = proof(self.fence, source, Some(evidence))?;
            source.observe_proof(identity, next, atomic_through)?;
        }
        Ok(())
    }

    pub(crate) fn remove_assignment(
        &mut self,
        assignment: &DefinitionAssignment,
    ) -> Result<(), SparseTrackerError> {
        validate_assignment(self.kind, self.fence, assignment)?;
        let identity = DerivedDefinitionIdentity::from_assignment(assignment);
        self.remove_identity(identity)
    }

    pub(crate) fn remove_identity(
        &mut self,
        identity: DerivedDefinitionIdentity,
    ) -> Result<(), SparseTrackerError> {
        if identity.kind != definition_kind(self.kind) {
            return Err(SparseTrackerError::WrongAssignment);
        }
        for source in self.sources.values_mut() {
            source.remove(identity)?;
        }
        Ok(())
    }

    pub(crate) fn update_source_status(
        &mut self,
        status: WatchJournalStatus,
    ) -> Result<(), SparseTrackerError> {
        self.sources
            .get_mut(&status.source_id)
            .ok_or(SparseTrackerError::UnknownSource)?
            .update_status(status)
    }

    pub(crate) fn checkpoints(&self) -> Result<Vec<DerivedConsumerCheckpoint>, SparseTrackerError> {
        let consumer_node_id = u16::try_from(self.consumer_node.0)
            .map_err(|_| SparseTrackerError::InvalidConsumerNode)?;
        let mut checkpoints = Vec::with_capacity(self.sources.len());
        for source in self.sources.values() {
            let next_offset = source.checkpoint_next()?;
            if source.baseline_acknowledged && next_offset == source.baseline_next {
                continue;
            }
            checkpoints.push(DerivedConsumerCheckpoint {
                consumer_kind: self.kind,
                source_id: source.status.source_id,
                consumer_node_id,
                next_offset,
                observed_fence: self.fence,
            });
        }
        Ok(checkpoints)
    }

    /// Advances the disposable baseline only after the publisher has durably
    /// accepted this exact checkpoint locally and at its source node.
    pub(crate) fn acknowledge(
        &mut self,
        checkpoint: DerivedConsumerCheckpoint,
    ) -> Result<(), SparseTrackerError> {
        let consumer_node_id = u16::try_from(self.consumer_node.0)
            .map_err(|_| SparseTrackerError::InvalidConsumerNode)?;
        if checkpoint.consumer_kind != self.kind
            || checkpoint.consumer_node_id != consumer_node_id
            || checkpoint.observed_fence != self.fence
        {
            return Err(SparseTrackerError::CheckpointIdentity);
        }
        let source = self
            .sources
            .get_mut(&checkpoint.source_id)
            .ok_or(SparseTrackerError::UnknownSource)?;
        let available = source.checkpoint_next()?;
        if checkpoint.next_offset < source.baseline_next || checkpoint.next_offset > available {
            return Err(SparseTrackerError::CheckpointOffset);
        }
        source.baseline_next = checkpoint.next_offset;
        source.baseline_acknowledged = true;
        Ok(())
    }

    #[cfg(test)]
    fn affected_len(&self, source: SourceId) -> usize {
        self.sources
            .get(&source)
            .map_or(0, |source| source.pending.len())
    }
}

fn proof(
    fence: PlacementLogId,
    source: &SourceProgress,
    evidence: Option<&DerivedBarrierEvidence>,
) -> Result<(u64, Option<u64>), SparseTrackerError> {
    let Some(evidence) = evidence else {
        return Ok((source.baseline_next, None));
    };
    let barrier = evidence.barrier();
    if barrier.fence != fence {
        return Err(SparseTrackerError::EvidenceFence);
    }
    let node = NodeId(u64::from(source.status.source_id.node_id));
    let cursor = barrier
        .sources
        .get(&node)
        .ok_or(SparseTrackerError::EvidenceSourceMissing)?;
    if cursor.source != source.status.source_id {
        return Err(SparseTrackerError::EvidenceSourceChanged);
    }
    Ok((cursor.next_offset, barrier.atomic.finalized_through()))
}

fn validate_assignment(
    kind: DerivedConsumerKind,
    fence: PlacementLogId,
    assignment: &DefinitionAssignment,
) -> Result<(), SparseTrackerError> {
    assignment
        .validate()
        .map_err(|error| SparseTrackerError::Assignment(error.to_string()))?;
    if assignment.kind != definition_kind(kind) || assignment.rank != 0 {
        return Err(SparseTrackerError::WrongAssignment);
    }
    if assignment.observed_fence != fence {
        return Err(SparseTrackerError::AssignmentFence);
    }
    Ok(())
}

fn validate_identity(
    consumer_node: NodeId,
    fence: PlacementLogId,
) -> Result<(), SparseTrackerError> {
    if consumer_node.0 == 0
        || consumer_node.0 > u64::from(u16::MAX)
        || fence.term == 0
        || fence.index == 0
    {
        return Err(SparseTrackerError::InvalidIdentity);
    }
    Ok(())
}

fn validate_source(source: WatchJournalStatus) -> Result<(), SparseTrackerError> {
    if source.source_id.node_id == 0
        || source.source_id.source_epoch == [0; 32]
        || source.retention_floor > source.settled_through
        || source.settled_through > source.tail
    {
        return Err(SparseTrackerError::InvalidSource);
    }
    Ok(())
}

pub(crate) const fn retention_kind(kind: DerivedConsumerKind) -> DefinitionConsumerKind {
    match kind {
        DerivedConsumerKind::Index => DefinitionConsumerKind::IndexRetention,
        DerivedConsumerKind::Accounting => DefinitionConsumerKind::AccountingRetention,
    }
}

const fn definition_kind(kind: DerivedConsumerKind) -> DefinitionKind {
    match kind {
        DerivedConsumerKind::Index => DefinitionKind::Index,
        DerivedConsumerKind::Accounting => DefinitionKind::Accounting,
    }
}

const fn fence_order(fence: PlacementLogId) -> (u64, u64) {
    (fence.term, fence.index)
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum SparseTrackerError {
    #[error("derived sparse tracker identity is invalid")]
    InvalidIdentity,
    #[error("derived sparse tracker consumer node is invalid")]
    InvalidConsumerNode,
    #[error("derived sparse tracker source is invalid")]
    InvalidSource,
    #[error("derived sparse tracker has no sources")]
    EmptySources,
    #[error("derived sparse tracker contains a duplicate source")]
    DuplicateSource,
    #[error("derived sparse tracker does not contain this source")]
    UnknownSource,
    #[error("derived sparse tracker offset overflowed")]
    OffsetOverflow,
    #[error("local aggregate checkpoint is invalid for the current source")]
    InvalidLocalCheckpoint,
    #[error("local aggregate checkpoint comes from a future membership fence")]
    FutureLocalFence,
    #[error("derived assignment is invalid: {0}")]
    Assignment(String),
    #[error("derived assignment is not a rank-zero assignment of this consumer kind")]
    WrongAssignment,
    #[error("derived assignment carries another membership fence")]
    AssignmentFence,
    #[error("derived proof carries another membership fence")]
    EvidenceFence,
    #[error("derived proof omits the source")]
    EvidenceSourceMissing,
    #[error("derived proof names another source incarnation")]
    EvidenceSourceChanged,
    #[error(
        "derived routed offset is outside retained settled history: source={source_id:?} definition={identity:?} required_next={required_next} floor_next={floor_next} settled_next={settled_next}"
    )]
    RoutedOffset {
        source_id: SourceId,
        identity: DerivedDefinitionIdentity,
        required_next: u64,
        floor_next: u64,
        settled_next: u64,
    },
    #[error("derived source incarnation changed")]
    SourceChanged,
    #[error("derived source status regressed")]
    SourceRegression,
    #[error("derived source pruned beyond the acknowledged checkpoint")]
    CheckpointExpired,
    #[error("derived sparse tracker minimum count overflowed")]
    CountOverflow,
    #[error("derived sparse tracker minimum state is inconsistent")]
    CorruptMinimum,
    #[error("derived sparse tracker acknowledgement identity is invalid")]
    CheckpointIdentity,
    #[error("derived sparse tracker acknowledgement is outside proven progress")]
    CheckpointOffset,
}

#[cfg(test)]
#[path = "tracker/tests.rs"]
mod tests;
