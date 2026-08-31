//! Gap-free dispatch of finalized atomic mutations to source partitions.
//!
//! Path-scoped program records reserve source positions but are never
//! projected independently. A finalized batch supplies the one indivisible
//! mutation unit, including mutations whose source journal belongs to another
//! node. Durable partition checkpoints cannot cross a held position until the
//! finalized batch has been applied.

use std::collections::{BTreeMap, BTreeSet};

use keldra_store::{AtomicBatchMutation, LocalChange, SourceId};
use tonic::Status;

#[derive(Clone, Debug)]
pub(crate) struct FinalizedAtomicMutation {
    pub(crate) cursor: u64,
    pub(crate) mutation: AtomicBatchMutation,
}

#[derive(Default)]
pub(crate) struct AtomicFinalizationDispatcher {
    holds: BTreeMap<SourceId, BTreeMap<u64, u64>>,
    finalized: BTreeMap<SourceId, BTreeMap<u64, FinalizedAtomicMutation>>,
    batches: BTreeMap<u64, ([u8; 32], BTreeMap<SourceId, BTreeSet<u64>>)>,
    seen_bundles: BTreeSet<(u64, [u8; 32])>,
    /// The consumer advances this only after the matching all-source work is
    /// durable. It bounds replay identity retention; it is not an index cut.
    acknowledged_through: u64,
    highest_observed_cursor: u64,
}

impl AtomicFinalizationDispatcher {
    pub(crate) fn retained_entries(&self) -> usize {
        self.holds.values().map(BTreeMap::len).sum::<usize>()
            + self.finalized.values().map(BTreeMap::len).sum::<usize>()
            + self.batches.len()
            + self.seen_bundles.len()
    }

    pub(crate) fn maximum_added_entries(&self, change: &LocalChange) -> Result<usize, Status> {
        match change {
            LocalChange::ObjectHead(head) if head.program_commit_cursor.is_some() => Ok(1),
            LocalChange::AtomicBatchPublished(batch) => batch
                .mutations
                .len()
                .checked_mul(2)
                .and_then(|entries| entries.checked_add(2))
                .ok_or_else(|| Status::resource_exhausted("v6 atomic control backlog overflow")),
            _ => Ok(0),
        }
    }

    /// Observe one all-source journal record and drain every source partition
    /// made runnable by it.  Atomic finalization normally arrives on the
    /// executor's source journal, while its mutations belong to arbitrary
    /// source owners; callers must use this rather than draining only the
    /// event source.
    pub(crate) fn observe_all(
        &mut self,
        event_source: SourceId,
        change: &LocalChange,
    ) -> Result<BTreeMap<SourceId, Vec<FinalizedAtomicMutation>>, Status> {
        let mut ready = BTreeMap::new();
        let event_ready = self.observe(event_source, change)?;
        if !event_ready.is_empty() {
            ready.insert(event_source, event_ready);
        }
        if let LocalChange::AtomicBatchPublished(batch) = change {
            let sources = batch
                .mutations
                .iter()
                .map(|mutation| mutation.source_id)
                .collect::<BTreeSet<_>>();
            for source in sources {
                let released = self.take_ready(source)?;
                if !released.is_empty() {
                    ready.entry(source).or_default().extend(released);
                }
            }
        }
        Ok(ready)
    }

    pub(crate) fn observe(
        &mut self,
        event_source: SourceId,
        change: &LocalChange,
    ) -> Result<Vec<FinalizedAtomicMutation>, Status> {
        match change {
            LocalChange::ObjectHead(head) => {
                if let Some(cursor) = head.program_commit_cursor {
                    self.hold(event_source, head.offset, cursor)?;
                }
            }
            LocalChange::AtomicBatchPublished(batch) => {
                batch
                    .validate()
                    .map_err(|message| Status::data_loss(message.to_owned()))?;
                if batch.cursor <= self.acknowledged_through {
                    return Ok(Vec::new());
                }
                let expected = batch.mutations.iter().fold(
                    BTreeMap::<SourceId, BTreeSet<u64>>::new(),
                    |mut expected, mutation| {
                        expected
                            .entry(mutation.source_id)
                            .or_default()
                            .insert(mutation.source_journal_position);
                        expected
                    },
                );
                match self.batches.entry(batch.cursor) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert((batch.bundle_hash.0, expected));
                    }
                    std::collections::btree_map::Entry::Occupied(slot)
                        if slot.get() != &(batch.bundle_hash.0, expected) =>
                    {
                        return Err(Status::data_loss(
                            "one atomic cursor has conflicting finalized batch contents",
                        ));
                    }
                    std::collections::btree_map::Entry::Occupied(_) => {}
                }
                if !self
                    .seen_bundles
                    .insert((batch.cursor, batch.bundle_hash.0))
                {
                    return Ok(Vec::new());
                }
                self.highest_observed_cursor = self.highest_observed_cursor.max(batch.cursor);
                for mutation in &batch.mutations {
                    let candidate = FinalizedAtomicMutation {
                        cursor: batch.cursor,
                        mutation: mutation.clone(),
                    };
                    let slot = self.finalized.entry(mutation.source_id).or_default();
                    if let Some(existing) = slot.get(&mutation.source_journal_position) {
                        if existing.cursor != candidate.cursor
                            || existing.mutation != candidate.mutation
                        {
                            return Err(Status::data_loss(
                                "one source position is finalized by conflicting atomic batches",
                            ));
                        }
                    } else {
                        slot.insert(mutation.source_journal_position, candidate);
                    }
                }
            }
            _ => {}
        }
        self.take_ready(event_source)
    }

    pub(crate) fn take_ready(
        &mut self,
        source: SourceId,
    ) -> Result<Vec<FinalizedAtomicMutation>, Status> {
        let mut ready = Vec::new();
        loop {
            let next = self.holds.get(&source).and_then(|holds| {
                holds
                    .first_key_value()
                    .map(|(&position, &cursor)| (position, cursor))
            });
            let Some((position, cursor)) = next else {
                break;
            };
            let expected = self
                .batches
                .get(&cursor)
                .and_then(|(_, sources)| sources.get(&source))
                .cloned();
            let Some(expected) = expected else {
                return Err(Status::data_loss(
                    "held atomic source position has no finalized batch membership",
                ));
            };
            let group = expected
                .iter()
                .map(|position| {
                    let held = self
                        .holds
                        .get(&source)
                        .and_then(|holds| holds.get(position))
                        .copied();
                    let finalized = self
                        .finalized
                        .get(&source)
                        .and_then(|mutations| mutations.get(position))
                        .filter(|mutation| mutation.cursor == cursor)
                        .cloned();
                    match (held, finalized) {
                        (Some(held_cursor), Some(finalized)) if held_cursor == cursor => {
                            Some(finalized)
                        }
                        _ => None,
                    }
                })
                .collect::<Option<Vec<_>>>();
            let Some(group) = group else {
                // A later finalized program cannot cross this source-local
                // held position.  The partition may buffer later work, but
                // its durable checkpoint remains before the first hold.
                break;
            };
            for position in expected {
                self.holds
                    .get_mut(&source)
                    .expect("held source remains registered")
                    .remove(&position);
                self.finalized
                    .get_mut(&source)
                    .expect("finalized source remains registered")
                    .remove(&position);
            }
            ready.extend(group);
        }
        Ok(ready)
    }

    pub(crate) fn checkpoint_limit(&self, source: SourceId, proposed_next: u64) -> u64 {
        self.holds
            .get(&source)
            .and_then(|holds| holds.keys().next().copied())
            .map_or(proposed_next, |held| proposed_next.min(held))
    }

    pub(crate) fn first_held_position(&self, source: SourceId) -> Option<u64> {
        self.holds
            .get(&source)
            .and_then(|holds| holds.first_key_value().map(|(&position, _)| position))
    }

    /// Drop replay identities only after the owning pipeline has persisted a
    /// complete all-source dispatch checkpoint through `cursor`. This keeps
    /// replay bookkeeping bounded without using an in-memory set as durable
    /// authority.
    pub(crate) fn acknowledge_dispatch_through(&mut self, cursor: u64) {
        if cursor <= self.acknowledged_through {
            return;
        }
        self.acknowledged_through = cursor;
        if cursor == u64::MAX {
            self.seen_bundles.clear();
        } else {
            self.seen_bundles = self.seen_bundles.split_off(&(cursor + 1, [0; 32]));
        }
        self.finalized.retain(|_, mutations| {
            mutations.retain(|_, mutation| mutation.cursor > cursor);
            !mutations.is_empty()
        });
        self.batches.retain(|batch, _| *batch > cursor);
    }

    /// Diagnostic only. Observing a later publication never proves that every
    /// preceding atomic batch has been dispatched to every source owner.
    pub(crate) const fn highest_observed_cursor(&self) -> u64 {
        self.highest_observed_cursor
    }

    /// A root cut is limited both by consensus' finalized watermark and by the
    /// caller's durable all-source dispatch completion. In particular, the
    /// maximum cursor merely observed locally is deliberately excluded.
    pub(crate) const fn root_cut_through(
        &self,
        consensus_finalized_through: u64,
        dispatched_through: u64,
    ) -> u64 {
        if consensus_finalized_through < dispatched_through {
            consensus_finalized_through
        } else {
            dispatched_through
        }
    }

    fn hold(&mut self, source: SourceId, position: u64, cursor: u64) -> Result<(), Status> {
        let previous = self
            .holds
            .entry(source)
            .or_default()
            .insert(position, cursor);
        if previous.is_some_and(|previous| previous != cursor) {
            return Err(Status::data_loss(
                "one source position is held by conflicting atomic cursors",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use keldra_store::{
        AtomicBatchPublished, AtomicBatchRoute, ObjectHeadChange, ObjectHeadChangeKind,
        PreparedBundleHash, VersionId,
    };

    use super::*;

    fn source(node: u16) -> SourceId {
        SourceId {
            node_id: node,
            source_epoch: [node as u8; 32],
        }
    }

    fn held(source: SourceId, offset: u64, cursor: u64) -> LocalChange {
        LocalChange::ObjectHead(ObjectHeadChange {
            offset,
            tenant_id: 1,
            bucket_id: 2,
            exact_path: format!("objects/{offset}"),
            canonical_path: None,
            path_version: VersionId(offset),
            kind: ObjectHeadChangeKind::Put,
            program_commit_cursor: Some(cursor),
            reference_deltas: Vec::new(),
            accounting_transition: None,
            definition_transition: None,
        })
    }

    fn published(event_offset: u64, cursor: u64, mutations: Vec<(SourceId, u64)>) -> LocalChange {
        LocalChange::AtomicBatchPublished(AtomicBatchPublished {
            offset: event_offset,
            cursor,
            bundle_hash: PreparedBundleHash([cursor as u8; 32]),
            affected_routes: vec![AtomicBatchRoute {
                tenant_id: 1,
                bucket_id: 2,
            }],
            mutations: mutations
                .into_iter()
                .map(|(source_id, position)| AtomicBatchMutation {
                    tenant_id: 1,
                    bucket_id: 2,
                    exact_path: format!("objects/{position}"),
                    canonical_path: None,
                    path_version: VersionId(position),
                    deleted: false,
                    source_id,
                    source_journal_position: position,
                })
                .collect(),
        })
    }

    #[test]
    fn remote_finalization_releases_each_source_only_as_one_complete_unit() {
        let a = source(1);
        let b = source(2);
        let executor = source(3);
        let mut dispatcher = AtomicFinalizationDispatcher::default();
        assert!(dispatcher.observe(a, &held(a, 7, 41)).unwrap().is_empty());
        assert!(dispatcher.observe(b, &held(b, 9, 41)).unwrap().is_empty());
        assert_eq!(dispatcher.checkpoint_limit(a, 100), 7);
        assert_eq!(dispatcher.checkpoint_limit(b, 100), 9);

        let event = published(12, 41, vec![(a, 7), (b, 9)]);
        assert!(dispatcher.observe(executor, &event).unwrap().is_empty());
        let ready_a = dispatcher.take_ready(a).unwrap();
        let ready_b = dispatcher.take_ready(b).unwrap();
        assert_eq!(ready_a[0].mutation.source_journal_position, 7);
        assert_eq!(ready_b[0].mutation.source_journal_position, 9);
        assert_eq!(dispatcher.checkpoint_limit(a, 100), 100);
        assert_eq!(dispatcher.checkpoint_limit(b, 100), 100);
        assert_eq!(dispatcher.highest_observed_cursor(), 41);
    }

    #[test]
    fn all_source_observation_drains_remote_source_owners() {
        let a = source(1);
        let b = source(2);
        let executor = source(3);
        let mut dispatcher = AtomicFinalizationDispatcher::default();
        dispatcher.observe(a, &held(a, 7, 41)).unwrap();
        dispatcher.observe(b, &held(b, 9, 41)).unwrap();
        let ready = dispatcher
            .observe_all(executor, &published(12, 41, vec![(a, 7), (b, 9)]))
            .unwrap();
        assert_eq!(ready[&a][0].mutation.source_journal_position, 7);
        assert_eq!(ready[&b][0].mutation.source_journal_position, 9);
    }

    #[test]
    fn finalization_seen_before_a_source_path_waits_for_the_hold() {
        let a = source(1);
        let executor = source(2);
        let mut dispatcher = AtomicFinalizationDispatcher::default();
        let event = published(3, 10, vec![(a, 8)]);
        dispatcher.observe(executor, &event).unwrap();
        let ready = dispatcher.observe(a, &held(a, 8, 10)).unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].cursor, 10);
    }

    #[test]
    fn later_finalization_cannot_cross_an_earlier_source_hold() {
        let a = source(1);
        let executor = source(2);
        let mut dispatcher = AtomicFinalizationDispatcher::default();
        dispatcher.observe(a, &held(a, 7, 10)).unwrap();
        dispatcher.observe(a, &held(a, 9, 11)).unwrap();
        dispatcher
            .observe(executor, &published(4, 11, vec![(a, 9)]))
            .unwrap();
        assert!(dispatcher.take_ready(a).unwrap().is_empty());
        assert_eq!(dispatcher.checkpoint_limit(a, 100), 7);
        dispatcher
            .observe(executor, &published(5, 10, vec![(a, 7)]))
            .unwrap();
        let ready = dispatcher.take_ready(a).unwrap();
        assert_eq!(
            ready
                .iter()
                .map(|mutation| mutation.mutation.source_journal_position)
                .collect::<Vec<_>>(),
            vec![7, 9]
        );
    }

    #[test]
    fn conflicting_finalization_for_one_source_position_fails_closed() {
        let a = source(1);
        let executor = source(2);
        let mut dispatcher = AtomicFinalizationDispatcher::default();
        dispatcher
            .observe(executor, &published(4, 10, vec![(a, 7)]))
            .unwrap();
        let error = dispatcher
            .observe(executor, &published(5, 11, vec![(a, 7)]))
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn later_observation_cannot_advance_the_common_root_cut() {
        let a = source(1);
        let executor = source(2);
        let mut dispatcher = AtomicFinalizationDispatcher::default();
        dispatcher
            .observe(executor, &published(4, 12, vec![(a, 9)]))
            .unwrap();
        assert_eq!(dispatcher.highest_observed_cursor(), 12);
        // The earlier atomic batch is neither observed nor durably delivered.
        // A v6 root may therefore advance only through the proven cursor 10.
        assert_eq!(dispatcher.root_cut_through(12, 10), 10);
        dispatcher.acknowledge_dispatch_through(10);
        assert_eq!(dispatcher.root_cut_through(12, 10), 10);
    }
}
