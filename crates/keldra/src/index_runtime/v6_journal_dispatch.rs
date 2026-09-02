//! Source-owner delivery after all-source atomic finalization.
//!
//! A source owner consumes its own ordered journal records. The atomic-control
//! dispatcher observes publication metadata from every source, because the
//! executor source can finalize mutations belonging to any owner. It emits
//! only source incarnations assigned to this producer at one placement fence.

use std::collections::{BTreeMap, BTreeSet};

use keldra_index::v6::IndexingMemoryPermit;
use keldra_store::{LocalChange, ObjectHeadChange, PlacementLogId, SourceId};
use tonic::Status;

use super::v6_atomic_dispatch::{AtomicFinalizationDispatcher, FinalizedAtomicMutation};

/// One indivisible source-local slice of a finalized atomic batch. A pipeline
/// owns or flushes this group as one unit; it must not split it at a memory or
/// segment boundary.
#[derive(Clone, Debug)]
pub(crate) struct V6FinalizedAtomicGroup {
    pub(crate) source: SourceId,
    pub(crate) cursor: u64,
    pub(crate) mutations: Vec<FinalizedAtomicMutation>,
}

#[derive(Clone, Debug)]
pub(crate) enum V6SourceDispatch {
    OrdinaryHead {
        source: SourceId,
        head: ObjectHeadChange,
    },
    FinalizedAtomic(V6FinalizedAtomicGroup),
}

/// Keeps all source streams gap-free without making atomic publication
/// ownership depend on the executor node. The assignment identifies source
/// incarnations, not original nodes: a `SourceId` can move to a successor
/// while its directory predecessor remains query-visible.
pub(crate) struct V6OrderedSourceDispatcher {
    atomic: AtomicFinalizationDispatcher,
    assignment_fence: PlacementLogId,
    assigned_sources: BTreeSet<SourceId>,
    ordinary: BTreeMap<SourceId, BTreeMap<u64, ObjectHeadChange>>,
    maximum_control_entries: usize,
    _control_memory: IndexingMemoryPermit,
}

impl V6OrderedSourceDispatcher {
    pub(crate) fn new(
        assignment_fence: PlacementLogId,
        assigned_sources: BTreeSet<SourceId>,
        control_memory: IndexingMemoryPermit,
        control_memory_bytes: usize,
    ) -> Self {
        Self {
            atomic: AtomicFinalizationDispatcher::default(),
            assignment_fence,
            assigned_sources,
            ordinary: BTreeMap::new(),
            maximum_control_entries: (control_memory_bytes / 512).max(1),
            _control_memory: control_memory,
        }
    }

    /// Install one placement-fenced assignment. A successor rebuilds control
    /// state from the retained all-source suffix before it delivers work at a
    /// new fence; durable roots/checkpoints remain handoff authority.
    pub(crate) fn replace_assignment(
        &mut self,
        assignment_fence: PlacementLogId,
        assigned_sources: BTreeSet<SourceId>,
    ) -> Result<(), Status> {
        let current = (self.assignment_fence.term, self.assignment_fence.index);
        let replacement = (assignment_fence.term, assignment_fence.index);
        if replacement < current {
            return Err(Status::failed_precondition(
                "v6 source assignment fence moved backward",
            ));
        }
        if replacement == current && assigned_sources != self.assigned_sources {
            return Err(Status::data_loss(
                "one placement fence assigned different v6 source incarnations",
            ));
        }
        self.assignment_fence = assignment_fence;
        self.assigned_sources = assigned_sources;
        Ok(())
    }

    /// Observe one record from the all-source stream.  A program-held object
    /// head is intentionally not delivered as an ordinary mutation: only the
    /// matching finalized batch may advance that source position.
    pub(crate) fn observe(
        &mut self,
        event_source: SourceId,
        change: &LocalChange,
    ) -> Result<Vec<V6SourceDispatch>, Status> {
        let ordinary_addition = usize::from(
            self.is_assigned(event_source)
                && matches!(change, LocalChange::ObjectHead(head) if head.program_commit_cursor.is_none()),
        );
        let atomic_addition = self.atomic.maximum_added_entries(change)?;
        let retained = self
            .atomic
            .retained_entries()
            .checked_add(self.ordinary.values().map(BTreeMap::len).sum())
            .and_then(|entries| entries.checked_add(ordinary_addition))
            .and_then(|entries| entries.checked_add(atomic_addition))
            .ok_or_else(|| Status::resource_exhausted("v6 control backlog size overflow"))?;
        if retained > self.maximum_control_entries {
            return Err(Status::resource_exhausted(
                "v6 ordered control backlog exhausted its pipeline credit",
            ));
        }
        let mut delivered = Vec::new();
        let released = self.atomic.observe_all(event_source, change)?;
        let mut affected = released.keys().copied().collect::<BTreeSet<_>>();
        if self.is_assigned(event_source)
            && let LocalChange::ObjectHead(head) = change
            && head.program_commit_cursor.is_none()
        {
            let previous = self
                .ordinary
                .entry(event_source)
                .or_default()
                .insert(head.offset, head.clone());
            if previous.as_ref().is_some_and(|existing| existing != head) {
                return Err(Status::data_loss(
                    "one source position has conflicting ordinary object heads",
                ));
            }
            affected.insert(event_source);
        }
        for (source, mutations) in released {
            if self.is_assigned(source) {
                let mut groups = BTreeMap::<u64, Vec<FinalizedAtomicMutation>>::new();
                for mutation in mutations {
                    groups.entry(mutation.cursor).or_default().push(mutation);
                }
                delivered.extend(groups.into_iter().map(|(cursor, mutations)| {
                    V6SourceDispatch::FinalizedAtomic(V6FinalizedAtomicGroup {
                        source,
                        cursor,
                        mutations,
                    })
                }));
            }
        }
        for source in affected {
            if self.is_assigned(source) {
                delivered.extend(self.take_runnable_ordinary(source));
            }
        }
        Ok(delivered)
    }

    /// The caller must persist no farther than this source-local offset.
    /// `proposed_next` is the first offset after work it has fully prepared.
    pub(crate) fn checkpoint_limit(&self, source: SourceId, proposed_next: u64) -> u64 {
        self.atomic.checkpoint_limit(source, proposed_next)
    }

    pub(crate) const fn assignment_fence(&self) -> PlacementLogId {
        self.assignment_fence
    }

    fn is_assigned(&self, source: SourceId) -> bool {
        self.assigned_sources.contains(&source)
    }

    fn take_runnable_ordinary(&mut self, source: SourceId) -> Vec<V6SourceDispatch> {
        let held = self.atomic.first_held_position(source).unwrap_or(u64::MAX);
        let Some(queued) = self.ordinary.get_mut(&source) else {
            return Vec::new();
        };
        let later = queued.split_off(&held);
        let ready = std::mem::replace(queued, later);
        if queued.is_empty() {
            self.ordinary.remove(&source);
        }
        ready
            .into_values()
            .map(|head| V6SourceDispatch::OrdinaryHead { source, head })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use keldra_store::{
        AtomicBatchMutation, AtomicBatchPublished, AtomicBatchRoute, ObjectHeadChangeKind,
        PlacementLogId, PreparedBundleHash, VersionId,
    };

    use super::*;
    use keldra_index::v6::{IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage};

    fn source(node_id: u16) -> SourceId {
        SourceId {
            node_id,
            source_epoch: [node_id as u8; 32],
        }
    }

    fn held(source: SourceId, position: u64, cursor: u64) -> LocalChange {
        LocalChange::ObjectHead(ObjectHeadChange {
            offset: position,
            tenant_id: 1,
            bucket_id: 2,
            exact_path: format!("items/{position}"),
            canonical_path: None,
            path_version: VersionId(position),
            kind: ObjectHeadChangeKind::Put,
            program_commit_cursor: Some(cursor),
            reference_deltas: Vec::new(),
            accounting_transition: None,
            definition_transition: None,
        })
    }

    fn ordinary(position: u64) -> LocalChange {
        LocalChange::ObjectHead(ObjectHeadChange {
            offset: position,
            tenant_id: 1,
            bucket_id: 2,
            exact_path: format!("items/{position}"),
            canonical_path: None,
            path_version: VersionId(position),
            kind: ObjectHeadChangeKind::Put,
            program_commit_cursor: None,
            reference_deltas: Vec::new(),
            accounting_transition: None,
            definition_transition: None,
        })
    }

    fn published(cursor: u64, mutations: Vec<(SourceId, u64)>) -> LocalChange {
        LocalChange::AtomicBatchPublished(AtomicBatchPublished {
            offset: 10,
            cursor,
            bundle_hash: PreparedBundleHash([cursor as u8; 32]),
            affected_routes: vec![AtomicBatchRoute {
                tenant_id: 1,
                bucket_id: 2,
            }],
            mutations: mutations
                .into_iter()
                .map(|(source_id, source_journal_position)| AtomicBatchMutation {
                    tenant_id: 1,
                    bucket_id: 2,
                    exact_path: format!("items/{source_journal_position}"),
                    canonical_path: None,
                    path_version: VersionId(source_journal_position),
                    deleted: false,
                    source_id,
                    source_journal_position,
                })
                .collect(),
        })
    }

    fn assignment(
        fence_index: u64,
        sources: impl IntoIterator<Item = SourceId>,
    ) -> V6OrderedSourceDispatcher {
        let bytes = 1024 * 1024;
        let credits = IndexingMemoryCredits::new(
            bytes,
            IndexingMemoryLimits {
                hot_payload_bytes: bytes,
                worker_scratch_bytes: bytes,
                prepared_rows_bytes: bytes,
                replay_input_bytes: bytes,
                projection_accumulator_bytes: bytes,
                seal_scratch_bytes: bytes,
                ordering_catalog_bytes: bytes,
            },
        )
        .unwrap();
        V6OrderedSourceDispatcher::new(
            PlacementLogId {
                term: 1,
                index: fence_index,
            },
            sources.into_iter().collect(),
            credits
                .acquire(IndexingMemoryStage::ReplayInput, bytes)
                .unwrap(),
            bytes,
        )
    }

    #[test]
    fn executor_event_releases_only_the_source_incarnation_assigned_here() {
        let local = source(7);
        let remote = source(8);
        let executor = source(9);
        let mut dispatcher = assignment(2, [local]);

        assert!(
            dispatcher
                .observe(local, &held(local, 4, 12))
                .unwrap()
                .is_empty()
        );
        assert!(
            dispatcher
                .observe(remote, &held(remote, 5, 12))
                .unwrap()
                .is_empty()
        );
        let delivered = dispatcher
            .observe(executor, &published(12, vec![(local, 4), (remote, 5)]))
            .unwrap();

        assert!(matches!(
            delivered.as_slice(),
            [V6SourceDispatch::FinalizedAtomic(group)]
                if group.source == local
                    && group.mutations.iter().all(|mutation| mutation.mutation.source_id == local)
        ));
        assert_eq!(dispatcher.checkpoint_limit(local, 6), 6);
        // An unassigned remote source cannot constrain this producer's local
        // checkpoint; its hold is retained only for all-source finalization.
        assert_eq!(dispatcher.checkpoint_limit(remote, 6), 6);
    }

    #[test]
    fn only_non_program_heads_are_delivered_as_ordinary_source_work() {
        let local = source(7);
        let mut dispatcher = assignment(2, [local]);
        let ordinary = dispatcher.observe(local, &ordinary(3)).unwrap();
        assert!(matches!(
            ordinary.as_slice(),
            [V6SourceDispatch::OrdinaryHead { head, .. }] if head.offset == 3
        ));
        assert!(
            dispatcher
                .observe(local, &held(local, 4, 12))
                .unwrap()
                .is_empty()
        );
        assert_eq!(dispatcher.checkpoint_limit(local, 5), 4);
    }

    #[test]
    fn ordinary_work_after_a_held_position_waits_for_the_contiguous_prefix() {
        let local = source(7);
        let executor = source(9);
        let mut dispatcher = assignment(2, [local]);

        assert!(
            dispatcher
                .observe(local, &held(local, 4, 12))
                .unwrap()
                .is_empty()
        );
        assert!(dispatcher.observe(local, &ordinary(5)).unwrap().is_empty());

        let delivered = dispatcher
            .observe(executor, &published(12, vec![(local, 4)]))
            .unwrap();
        assert!(matches!(
            delivered.as_slice(),
            [
                V6SourceDispatch::FinalizedAtomic(finalized),
                V6SourceDispatch::OrdinaryHead { head, .. }
            ] if finalized.mutations.len() == 1
                && finalized.mutations[0].mutation.source_journal_position == 4
                && head.offset == 5
        ));
    }

    #[test]
    fn successor_can_receive_an_old_source_incarnation_after_handoff() {
        let old_source = source(7);
        let executor = source(9);
        let mut old_producer = assignment(2, [old_source]);
        old_producer
            .replace_assignment(PlacementLogId { term: 1, index: 3 }, BTreeSet::new())
            .unwrap();
        assert!(
            old_producer
                .observe(old_source, &ordinary(3))
                .unwrap()
                .is_empty()
        );

        // The successor rebuilt atomic control from the retained all-source
        // suffix and is assigned the original source incarnation at its new
        // fence; original source_node does not determine the producer.
        let mut successor = assignment(3, [old_source]);
        successor
            .observe(old_source, &held(old_source, 4, 12))
            .unwrap();
        let delivered = successor
            .observe(executor, &published(12, vec![(old_source, 4)]))
            .unwrap();
        assert!(matches!(
            delivered.as_slice(),
            [V6SourceDispatch::FinalizedAtomic(group)]
                if group.source == old_source
        ));
        assert_eq!(
            successor.assignment_fence(),
            PlacementLogId { term: 1, index: 3 }
        );
    }
}
