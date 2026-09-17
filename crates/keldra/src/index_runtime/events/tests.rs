use std::sync::Mutex;

use keldra_store::{
    DefinitionKind, DefinitionOperation, DefinitionTransition, ObjectHeadChange,
    ObjectHeadChangeKind, ReferenceDelta, VersionId,
};

use super::*;

#[derive(Clone)]
struct MemoryAuthority {
    sequence: Arc<Mutex<Vec<IndexEventPlacement>>>,
}

impl IndexEventAuthority for MemoryAuthority {
    fn current(&self) -> Result<IndexEventPlacement, String> {
        let mut sequence = self.sequence.lock().unwrap();
        if sequence.len() > 1 {
            Ok(sequence.remove(0))
        } else {
            Ok(sequence[0].clone())
        }
    }
}

#[derive(Clone, Default)]
struct MemorySources {
    journals: Arc<Mutex<BTreeMap<NodeId, (WatchJournalStatus, Vec<LocalChange>)>>>,
    status_reads: Arc<Mutex<Vec<NodeId>>>,
    status_sequences: Arc<Mutex<BTreeMap<NodeId, VecDeque<WatchJournalStatus>>>>,
    reads: Arc<Mutex<Vec<(NodeId, u64, u64)>>>,
    definition_reads: Arc<Mutex<Vec<(NodeId, keldra_store::DefinitionKind)>>>,
    raw_reads: Arc<Mutex<Vec<NodeId>>>,
}

#[tonic::async_trait]
impl IndexEventSources for MemorySources {
    async fn status(&self, source: &IndexSource) -> Result<WatchJournalStatus, IndexEventError> {
        self.status_reads.lock().unwrap().push(source.node);
        if let Some(status) = self
            .status_sequences
            .lock()
            .unwrap()
            .get_mut(&source.node)
            .and_then(VecDeque::pop_front)
        {
            return Ok(status);
        }
        self.journals
            .lock()
            .unwrap()
            .get(&source.node)
            .map(|entry| entry.0)
            .ok_or_else(|| IndexEventError::Source {
                node: source.node,
                message: "missing test source".into(),
            })
    }

    async fn read_raw_page(
        &self,
        source: &IndexSource,
        expected_source: SourceId,
        after_offset: u64,
        target_offset: u64,
        limit: usize,
        max_bytes: u64,
    ) -> Result<IndexSourcePage, IndexEventError> {
        self.raw_reads.lock().unwrap().push(source.node);
        memory_page(
            &self.journals,
            source,
            expected_source,
            after_offset,
            target_offset,
            limit,
            max_bytes,
            |_| true,
        )
    }

    async fn read_page(
        &self,
        source: &IndexSource,
        expected_source: SourceId,
        after_offset: u64,
        target_offset: u64,
        tenant_id: u64,
        bucket_id: u64,
        limit: usize,
        max_bytes: u64,
    ) -> Result<IndexSourcePage, IndexEventError> {
        self.reads
            .lock()
            .unwrap()
            .push((source.node, tenant_id, bucket_id));
        memory_page(
            &self.journals,
            source,
            expected_source,
            after_offset,
            target_offset,
            limit,
            max_bytes,
            |change| {
                change_bucket(change) == Some((tenant_id, bucket_id))
                    || matches!(change, LocalChange::AtomicBatchPublished(batch)
                    if batch.mutations.iter().any(|mutation|
                        mutation.tenant_id == tenant_id && mutation.bucket_id == bucket_id))
            },
        )
    }

    async fn read_definition_page(
        &self,
        source: &IndexSource,
        expected_source: SourceId,
        after_offset: u64,
        target_offset: u64,
        kind: keldra_store::DefinitionKind,
        limit: usize,
        max_bytes: u64,
    ) -> Result<IndexSourcePage, IndexEventError> {
        self.definition_reads
            .lock()
            .unwrap()
            .push((source.node, kind));
        memory_page(
            &self.journals,
            source,
            expected_source,
            after_offset,
            target_offset,
            limit,
            max_bytes,
            |change| {
                matches!(
                    change,
                    LocalChange::ObjectHead(head)
                        if head.definition_transition.as_ref().is_some_and(|transition| transition.kind == kind)
                )
            },
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn memory_page(
    journals: &Mutex<BTreeMap<NodeId, (WatchJournalStatus, Vec<LocalChange>)>>,
    source: &IndexSource,
    expected_source: SourceId,
    after_offset: u64,
    target_offset: u64,
    limit: usize,
    max_bytes: u64,
    include: impl Fn(&LocalChange) -> bool,
) -> Result<IndexSourcePage, IndexEventError> {
    let journals = journals.lock().unwrap();
    let (status, changes) = journals.get(&source.node).unwrap();
    let mut selected = Vec::new();
    let mut change_encoded_bytes = Vec::new();
    let mut encoded_bytes = 0_u64;
    let mut oversize = None;
    let matching = changes
        .iter()
        .filter(|change| {
            change.offset() > after_offset && change.offset() <= target_offset && include(change)
        })
        .collect::<Vec<_>>();
    for change in matching.iter().copied().take(limit) {
        let bytes = encoded_len(change)?;
        let projected = encoded_bytes + bytes;
        if projected > max_bytes && selected.is_empty() {
            oversize = Some(OversizeLocalChange {
                offset: change.offset(),
                encoded_bytes: bytes,
            });
            break;
        }
        if projected > max_bytes {
            break;
        }
        encoded_bytes = projected;
        change_encoded_bytes.push(bytes);
        selected.push(change.clone());
    }
    let through_offset = if oversize.is_some() {
        after_offset
    } else if selected.len() < matching.len() {
        selected.last().map_or(after_offset, LocalChange::offset)
    } else {
        target_offset
    };
    Ok(IndexSourcePage {
        source_id: if status.source_id == expected_source {
            status.source_id
        } else {
            expected_source
        },
        changes: selected,
        change_encoded_bytes,
        encoded_bytes,
        through_offset,
        oversize,
    })
}

fn source_id(node: u16) -> SourceId {
    SourceId {
        node_id: node,
        source_epoch: [node as u8; 32],
    }
}

fn change(node: u16, offset: u64) -> LocalChange {
    change_in_bucket(node, offset, 1, 2)
}

fn change_in_bucket(node: u16, offset: u64, tenant_id: u64, bucket_id: u64) -> LocalChange {
    change_at_path(
        node,
        offset,
        tenant_id,
        bucket_id,
        &format!("source-{node}/{offset}"),
    )
}

fn change_at_path(
    _node: u16,
    offset: u64,
    tenant_id: u64,
    bucket_id: u64,
    path: &str,
) -> LocalChange {
    LocalChange::ObjectHead(ObjectHeadChange {
        offset,
        tenant_id,
        bucket_id,
        exact_path: path.to_owned(),
        canonical_path: None,
        path_version: VersionId(offset),
        kind: ObjectHeadChangeKind::Put,
        program_commit_cursor: None,
        reference_deltas: Vec::<ReferenceDelta>::new(),
        accounting_transition: None,
        definition_transition: None,
    })
}

fn definition_change(offset: u64, kind: DefinitionKind) -> LocalChange {
    let mut change = change_at_path(1, offset, 1, 2, "_keldra/indexes/example.json");
    let LocalChange::ObjectHead(head) = &mut change else {
        unreachable!("test helper always creates an object-head change");
    };
    head.definition_transition = Some(DefinitionTransition {
        kind,
        tenant_id: 1,
        bucket_id: 2,
        definition_id: 3,
        path: head.exact_path.clone(),
        object_version: head.path_version,
        operation: DefinitionOperation::Upsert,
    });
    change
}

fn change_bucket(change: &LocalChange) -> Option<(u64, u64)> {
    match change {
        LocalChange::ObjectHead(change) => Some((change.tenant_id, change.bucket_id)),
        LocalChange::RetainedVersionDeleted(change) => Some((change.tenant_id, change.bucket_id)),
        LocalChange::AggregateChanged(_) | LocalChange::ContentLifecycleBatchChanged(_) => None,
        _ => None,
    }
}

fn status(node: u16, tail: u64) -> WatchJournalStatus {
    WatchJournalStatus {
        source_id: source_id(node),
        tail,
        settled_through: tail,
        retention_floor: 0,
        retained_entries: tail,
        retained_bytes: tail * 10,
    }
}

fn placement(atomic: AtomicProgramWatermark) -> IndexEventPlacement {
    IndexEventPlacement {
        fence: PlacementLogId { term: 3, index: 7 },
        sources: vec![
            IndexSource {
                node: NodeId(1),
                address: "one:50052".into(),
            },
            IndexSource {
                node: NodeId(2),
                address: "two:50052".into(),
            },
        ],
        atomic,
    }
}

fn journal(
    placement_sequence: Vec<IndexEventPlacement>,
    sources: &MemorySources,
) -> IndexEventJournal {
    IndexEventJournal::new(
        Arc::new(MemoryAuthority {
            sequence: Arc::new(Mutex::new(placement_sequence)),
        }),
        Arc::new(sources.clone()),
    )
    .with_page_size(1)
}

#[tokio::test]
async fn retained_replay_start_uses_source_specific_floors_and_keeps_foreign_finalizers() {
    use crate::index_runtime::v1_journal_dispatch::{V1OrderedSourceDispatcher, V1SourceDispatch};
    use keldra_index::v1::{IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage};
    use keldra_store::{AtomicBatchMutation, AtomicBatchPublished, PreparedBundleHash};
    use std::collections::BTreeSet;

    let atomic = AtomicProgramWatermark::new(Some(7), Some(7), 0);
    let sources = MemorySources::default();
    let mut owned_status = status(1, 12);
    owned_status.retention_floor = 11;
    owned_status.retained_entries = 1;
    let mut other_status = status(2, 401);
    other_status.retention_floor = 400;
    other_status.retained_entries = 1;
    let mut head = change_at_path(1, 12, 1, 2, "items/12");
    if let LocalChange::ObjectHead(head) = &mut head {
        head.program_commit_cursor = Some(7);
    }
    let finalizer = LocalChange::AtomicBatchPublished(AtomicBatchPublished {
        offset: 401,
        cursor: 7,
        bundle_hash: PreparedBundleHash([7; 32]),
        mutations: vec![AtomicBatchMutation {
            tenant_id: 1,
            bucket_id: 2,
            exact_path: "items/12".into(),
            canonical_path: None,
            path_version: VersionId(12),
            deleted: false,
            source_id: source_id(1),
            source_journal_position: 12,
        }],
    });
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (owned_status, vec![head]));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (other_status, vec![finalizer]));
    let journal = journal(vec![placement(atomic)], &sources);
    let target = barrier(atomic, 13, 402);
    let mut start = journal.retained_replay_start(&target).await.unwrap();
    assert_eq!(start.sources[&NodeId(1)].next_offset, 12);
    assert_eq!(start.sources[&NodeId(2)].next_offset, 401);
    assert_eq!(sources.status_reads.lock().unwrap().len(), 4);
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
    let mut dispatcher = V1OrderedSourceDispatcher::new(
        target.fence,
        BTreeSet::from([source_id(1)]),
        credits
            .acquire(IndexingMemoryStage::ReplayInput, 1)
            .unwrap(),
        bytes,
    );
    let mut delivered = Vec::new();
    while let Some(page) = journal
        .next_page(1, 2, &start, &target, 64 * 1024)
        .await
        .unwrap()
    {
        for change in &page.changes {
            delivered.extend(
                dispatcher
                    .observe(page.through.sources[&change.node].source, &change.change)
                    .unwrap(),
            );
        }
        start = page.through;
    }
    assert!(
        matches!(delivered.as_slice(), [V1SourceDispatch::FinalizedAtomic(batch)]
        if batch.mutations.len() == 1
        && batch.mutations[0].mutation.source_journal_position == 12)
    );
    assert_eq!(start, target);
}

#[tokio::test]
async fn retained_replay_start_rejects_epoch_and_placement_changes() {
    let atomic = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (status(1, 3), vec![]));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 3), vec![]));
    let original = placement(atomic);
    let mut changed = original.clone();
    changed.fence.index += 1;
    assert!(
        journal(vec![original.clone(), changed], &sources)
            .retained_replay_start(&barrier(atomic, 4, 4))
            .await
            .is_err()
    );
    sources
        .journals
        .lock()
        .unwrap()
        .get_mut(&NodeId(2))
        .unwrap()
        .0
        .source_id
        .source_epoch = [99; 32];
    assert!(matches!(
        journal(vec![original], &sources)
            .retained_replay_start(&barrier(atomic, 4, 4))
            .await,
        Err(IndexEventError::SourceEpochChanged(NodeId(2)))
    ));
}

#[tokio::test]
async fn retained_replay_start_rechecks_pruning_and_epoch_after_all_source_reads() {
    let atomic = AtomicProgramWatermark::new(None, None, 0);
    for rotate in [false, true] {
        let sources = MemorySources::default();
        sources
            .journals
            .lock()
            .unwrap()
            .insert(NodeId(1), (status(1, 3), vec![]));
        sources
            .journals
            .lock()
            .unwrap()
            .insert(NodeId(2), (status(2, 3), vec![]));
        let first = status(1, 3);
        let mut second = first;
        if rotate {
            second.source_id.source_epoch = [99; 32];
        } else {
            second.retention_floor = 1;
            second.retained_entries = 2;
        }
        sources
            .status_sequences
            .lock()
            .unwrap()
            .insert(NodeId(1), VecDeque::from([first, second]));
        assert!(matches!(
            journal(vec![placement(atomic)], &sources)
                .retained_replay_start(&barrier(atomic, 4, 4))
                .await,
            Err(IndexEventError::SourceEpochChanged(NodeId(1)))
        ));
    }
}

#[tokio::test]
async fn skipped_cut_source_wide_proof_finds_other_bucket_data_but_not_artifact_feedback() {
    let atomic = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources.journals.lock().unwrap().insert(
        NodeId(1),
        (
            status(1, 3),
            vec![
                change_at_path(1, 1, 1, 2, "_keldra/index-projections/v1/current"),
                change_at_path(1, 2, 9, 10, "other-bucket/object.json"),
                change_at_path(1, 3, 1, 2, "_keldra/index-projections/v1/generation"),
            ],
        ),
    );
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let journal = journal(vec![placement(atomic)], &sources);
    let target = barrier(atomic, 4, 1);
    assert!(
        super::super::v1_consumer::skipped_interval_has_external_change(
            &journal,
            source_id(1),
            1,
            4,
            &target,
            4096
        )
        .await
        .unwrap()
    );
    assert_eq!(
        sources.raw_reads.lock().unwrap().len(),
        3,
        "proof spans every bounded page, not only first match"
    );
    assert!(
        !super::super::v1_consumer::skipped_interval_has_external_change(
            &journal,
            source_id(1),
            3,
            4,
            &target,
            4096
        )
        .await
        .unwrap(),
        "generation/Current feedback cannot arm another empty publication"
    );
    assert!(
        sources
            .raw_reads
            .lock()
            .unwrap()
            .iter()
            .all(|node| *node == NodeId(1))
    );
}

#[tokio::test]
async fn skipped_cut_proof_fails_closed_after_pruning_or_source_epoch_change() {
    let atomic = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    let mut pruned = status(1, 3);
    pruned.retention_floor = 2;
    pruned.retained_entries = 1;
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (pruned, vec![change(1, 3)]));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let journal = journal(vec![placement(atomic)], &sources);
    let target = barrier(atomic, 4, 1);
    assert!(
        super::super::v1_consumer::skipped_interval_has_external_change(
            &journal,
            source_id(1),
            1,
            4,
            &target,
            4096
        )
        .await
        .is_err()
    );
    let mut wrong_source = source_id(1);
    wrong_source.source_epoch = [99; 32];
    assert!(
        super::super::v1_consumer::skipped_interval_has_external_change(
            &journal,
            wrong_source,
            3,
            4,
            &target,
            4096
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn skipped_cut_proof_refuses_oversized_page_and_changed_placement_fence() {
    let atomic = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (status(1, 1), vec![change(1, 1)]));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let target = barrier(atomic, 2, 1);
    let capped = journal(vec![placement(atomic)], &sources);
    assert!(
        super::super::v1_consumer::skipped_interval_has_external_change(
            &capped,
            source_id(1),
            1,
            2,
            &target,
            1
        )
        .await
        .is_err()
    );
    let mut changed = placement(atomic);
    changed.fence.index += 1;
    let fenced = journal(vec![placement(atomic), changed], &sources);
    assert!(
        super::super::v1_consumer::skipped_interval_has_external_change(
            &fenced,
            source_id(1),
            1,
            2,
            &target,
            4096
        )
        .await
        .is_err()
    );
}

fn barrier(atomic: AtomicProgramWatermark, node_one_next: u64, node_two_next: u64) -> IndexBarrier {
    IndexBarrier {
        fence: PlacementLogId { term: 3, index: 7 },
        atomic,
        sources: BTreeMap::from([
            (
                NodeId(1),
                IndexSourceCursor {
                    source: source_id(1),
                    next_offset: node_one_next,
                },
            ),
            (
                NodeId(2),
                IndexSourceCursor {
                    source: source_id(2),
                    next_offset: node_two_next,
                },
            ),
        ]),
    }
}

#[test]
fn later_atomic_state_covers_a_clear_captured_watermark() {
    let captured = AtomicProgramWatermark::new(Some(40), Some(40), 0);

    assert!(AtomicProgramWatermark::new(Some(41), Some(41), 0).covers(captured));
    assert!(AtomicProgramWatermark::new(Some(42), Some(40), 1).covers(captured));
    assert!(!AtomicProgramWatermark::new(Some(40), Some(39), 1).covers(captured));
    assert!(
        !AtomicProgramWatermark::new(Some(41), Some(41), 0).covers(AtomicProgramWatermark::new(
            Some(42),
            Some(41),
            1
        ))
    );
}

#[test]
fn snapshot_tails_form_the_exact_active_source_vector() {
    let clear = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let journal = journal(vec![placement(clear)], &MemorySources::default());
    let barrier = journal
        .barrier_from_snapshot_tails(
            PlacementLogId { term: 3, index: 7 },
            clear,
            &[(NodeId(1), source_id(1), 8), (NodeId(2), source_id(2), 13)],
        )
        .unwrap();

    assert_eq!(barrier.atomic, clear);
    assert_eq!(barrier.sources[&NodeId(1)].next_offset, 9);
    assert_eq!(barrier.sources[&NodeId(2)].next_offset, 14);
}

#[test]
fn snapshot_tails_reject_an_incomplete_or_pending_boundary() {
    let clear = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let incomplete = journal(vec![placement(clear)], &MemorySources::default())
        .barrier_from_snapshot_tails(
            PlacementLogId { term: 3, index: 7 },
            clear,
            &[(NodeId(1), source_id(1), 8)],
        )
        .unwrap_err();
    assert_eq!(incomplete, IndexEventError::IncompleteSources);

    let pending = AtomicProgramWatermark::new(Some(41), Some(40), 1);
    let pending = journal(vec![placement(pending)], &MemorySources::default())
        .barrier_from_snapshot_tails(
            PlacementLogId { term: 3, index: 7 },
            pending,
            &[(NodeId(1), source_id(1), 8), (NodeId(2), source_id(2), 13)],
        )
        .unwrap_err();
    assert_eq!(pending, IndexEventError::BarrierChanged);
}

#[tokio::test]
async fn barrier_captures_every_active_source_tail() {
    let clear = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let sources = MemorySources::default();
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (status(1, 2), vec![change(1, 1), change(1, 2)]));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 1), vec![change(2, 1)]));

    let barrier = journal(vec![placement(clear)], &sources)
        .capture_barrier()
        .await
        .unwrap();

    assert_eq!(barrier.atomic.finalized_through(), Some(40));
    assert_eq!(barrier.sources[&NodeId(1)].next_offset, 3);
    assert_eq!(barrier.sources[&NodeId(2)].next_offset, 2);
}

#[tokio::test]
async fn barrier_stops_at_each_sources_proof_backed_settled_boundary() {
    let clear = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let sources = MemorySources::default();
    let mut pending = status(1, 2);
    pending.settled_through = 1;
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (pending, vec![change(1, 1), change(1, 2)]));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));

    let barrier = journal(vec![placement(clear)], &sources)
        .capture_barrier()
        .await
        .unwrap();

    assert_eq!(barrier.sources[&NodeId(1)].next_offset, 2);
    assert_eq!(barrier.sources[&NodeId(2)].next_offset, 1);
}

#[tokio::test]
async fn concurrent_atomic_commit_invalidates_tail_capture() {
    let before = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let after = AtomicProgramWatermark::new(Some(41), Some(41), 0);
    let sources = MemorySources::default();
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (status(1, 0), Vec::new()));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));

    let error = journal(vec![placement(before), placement(after)], &sources)
        .capture_barrier()
        .await
        .unwrap_err();
    assert_eq!(error, IndexEventError::BarrierChanged);
}

#[tokio::test]
async fn unfinalized_atomic_tail_cannot_form_a_commit_barrier() {
    let pending = AtomicProgramWatermark::new(Some(41), Some(40), 1);
    let error = journal(vec![placement(pending)], &MemorySources::default())
        .capture_barrier()
        .await
        .unwrap_err();
    assert_eq!(error, IndexEventError::AtomicProgramInProgress);
}

#[tokio::test]
async fn bounded_pages_advance_every_source_through_the_exact_vector() {
    let clear = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let sources = MemorySources::default();
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (status(1, 2), vec![change(1, 1), change(1, 2)]));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 1), vec![change(2, 1)]));
    let from = IndexBarrier {
        fence: PlacementLogId { term: 3, index: 7 },
        atomic: clear,
        sources: BTreeMap::from([
            (
                NodeId(1),
                IndexSourceCursor {
                    source: source_id(1),
                    next_offset: 1,
                },
            ),
            (
                NodeId(2),
                IndexSourceCursor {
                    source: source_id(2),
                    next_offset: 1,
                },
            ),
        ]),
    };
    let target = journal(vec![placement(clear)], &sources)
        .capture_barrier()
        .await
        .unwrap();
    let journal = journal(vec![placement(clear)], &sources);
    let mut cursor = from;
    let mut offsets = Vec::new();
    while let Some(page) = journal
        .next_page(1, 2, &cursor, &target, MAX_INDEX_EVENT_PAGE_BYTES)
        .await
        .unwrap()
    {
        assert_eq!(page.changes.len(), 1);
        assert!(page.encoded_bytes > 0);
        offsets.extend(
            page.changes
                .iter()
                .map(|change| (change.node, change.change.offset())),
        );
        cursor = page.through;
    }
    offsets.sort_unstable();
    assert_eq!(offsets, [(NodeId(1), 1), (NodeId(1), 2), (NodeId(2), 1)]);
    assert_eq!(cursor, target);
}

#[tokio::test]
async fn routed_index_effects_do_no_source_reads_for_an_idle_vector() {
    let clear = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (status(1, 0), Vec::new()));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let barrier = journal(vec![placement(clear)], &sources)
        .capture_barrier()
        .await
        .unwrap();

    let effects = journal(vec![placement(clear)], &sources)
        .routed_index_effects(1, 2, &barrier, &barrier)
        .await
        .unwrap();

    assert!(effects.is_empty());
    assert!(sources.reads.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sparse_definition_page_skips_ordinary_rows_and_keeps_exact_source_coverage() {
    let clear = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources.journals.lock().unwrap().insert(
        NodeId(1),
        (
            status(1, 10_000),
            vec![
                change(1, 1),
                definition_change(4_000, DefinitionKind::Index),
                change(1, 10_000),
            ],
        ),
    );
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let target = journal(vec![placement(clear)], &sources)
        .capture_barrier()
        .await
        .unwrap();
    let from = barrier(clear, 1, 1);

    let page = journal(vec![placement(clear)], &sources)
        .next_definition_page(
            DefinitionKind::Index,
            &from,
            &target,
            MAX_INDEX_EVENT_PAGE_BYTES,
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(page.changes.len(), 1);
    assert_eq!(page.changes[0].change.offset(), 4_000);
    assert_eq!(page.through, target);
    assert!(sources.raw_reads.lock().unwrap().is_empty());
    assert_eq!(
        sources.definition_reads.lock().unwrap().as_slice(),
        &[(NodeId(1), DefinitionKind::Index)]
    );
}

#[tokio::test]
async fn empty_definition_route_advances_through_irrelevant_source_traffic() {
    let clear = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (status(1, 10_000), vec![change(1, 10_000)]));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let target = journal(vec![placement(clear)], &sources)
        .capture_barrier()
        .await
        .unwrap();
    let from = barrier(clear, 1, 1);

    let page = journal(vec![placement(clear)], &sources)
        .next_definition_page(
            DefinitionKind::Index,
            &from,
            &target,
            MAX_INDEX_EVENT_PAGE_BYTES,
        )
        .await
        .unwrap()
        .unwrap();

    assert!(page.changes.is_empty());
    assert_eq!(page.encoded_bytes, 0);
    assert_eq!(page.through, target);
}

#[tokio::test]
async fn routed_index_effects_never_fetch_or_process_an_irrelevant_bucket() {
    let clear = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources.journals.lock().unwrap().insert(
        NodeId(1),
        (status(1, 1), vec![change_in_bucket(1, 1, 9, 9)]),
    );
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let target = journal(vec![placement(clear)], &sources)
        .capture_barrier()
        .await
        .unwrap();
    let mut from = target.clone();
    from.sources.get_mut(&NodeId(1)).unwrap().next_offset = 1;

    let effects = journal(vec![placement(clear)], &sources)
        .routed_index_effects(1, 2, &from, &target)
        .await
        .unwrap();

    assert!(effects.is_empty());
    assert!(
        sources
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|(_, tenant, bucket)| (*tenant, *bucket) == (1, 2))
    );
}

#[tokio::test]
async fn routed_index_effects_report_only_the_relevant_sources_newest_offset() {
    let clear = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources.journals.lock().unwrap().insert(
        NodeId(1),
        (
            status(1, 3),
            vec![
                change_in_bucket(1, 1, 1, 2),
                change_in_bucket(1, 2, 9, 9),
                change_in_bucket(1, 3, 1, 2),
            ],
        ),
    );
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let target = journal(vec![placement(clear)], &sources)
        .capture_barrier()
        .await
        .unwrap();
    let mut from = target.clone();
    from.sources.get_mut(&NodeId(1)).unwrap().next_offset = 1;

    let effects = journal(vec![placement(clear)], &sources)
        .routed_index_effects(1, 2, &from, &target)
        .await
        .unwrap();

    assert_eq!(
        effects,
        BTreeMap::from([(
            source_id(1),
            RoutedSourceEffect {
                next_offset: 4,
                required_atomic_cursor: None,
                atomic_hold_next: None,
            },
        )])
    );
    assert!(
        sources
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|(_, tenant, bucket)| (*tenant, *bucket) == (1, 2))
    );
}

#[tokio::test]
async fn routed_index_source_next_distinguishes_irrelevant_and_relevant_suffixes() {
    let clear = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources.journals.lock().unwrap().insert(
        NodeId(1),
        (
            status(1, 3),
            vec![
                change_in_bucket(1, 1, 7, 8),
                change_in_bucket(1, 2, 1, 2),
                change_in_bucket(1, 3, 9, 9),
            ],
        ),
    );
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let events = journal(vec![placement(clear)], &sources);
    let target = events.capture_barrier().await.unwrap();

    assert_eq!(
        events
            .routed_index_source_next(1, 2, source_id(1), 1, &target)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        events
            .routed_index_source_next(5, 6, source_id(1), 1, &target)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        events
            .routed_index_source_next(7, 8, source_id(1), 1, &target)
            .await
            .unwrap(),
        2,
        "the first real journal offset must participate in routed lag"
    );
}

#[tokio::test]
async fn routed_accounting_effects_ignore_rollup_publication() {
    let clear = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources.journals.lock().unwrap().insert(
        NodeId(1),
        (
            status(1, 1),
            vec![change_at_path(1, 1, 1, 2, "_keldra/accounting/7/current")],
        ),
    );
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let target = journal(vec![placement(clear)], &sources)
        .capture_barrier()
        .await
        .unwrap();
    let mut from = target.clone();
    from.sources.get_mut(&NodeId(1)).unwrap().next_offset = 1;

    let effects = journal(vec![placement(clear)], &sources)
        .routed_accounting_effects(1, 2, &from, &target)
        .await
        .unwrap();

    assert!(effects.is_empty());
}

#[tokio::test]
async fn query_bucket_barrier_ignores_an_unrelated_bucket() {
    let clear = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources.journals.lock().unwrap().insert(
        NodeId(1),
        (status(1, 1), vec![change_in_bucket(1, 1, 9, 9)]),
    );
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let events = journal(vec![placement(clear)], &sources);
    let target = events.capture_barrier().await.unwrap();
    let mut indexed = target.clone();
    indexed.sources.get_mut(&NodeId(1)).unwrap().next_offset = 1;

    let observed = events
        .capture_index_bucket_barrier(1, 2, Some(&indexed))
        .await
        .unwrap();

    assert_eq!(observed, indexed);
    assert!(
        sources
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|(_, tenant, bucket)| (*tenant, *bucket) == (1, 2))
    );
}

#[tokio::test]
async fn bucket_barrier_promotes_the_captured_atomic_watermark() {
    let published = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let captured = AtomicProgramWatermark::new(Some(42), Some(42), 0);
    let sources = MemorySources::default();
    sources.journals.lock().unwrap().insert(
        NodeId(1),
        (status(1, 1), vec![change_in_bucket(1, 1, 1, 2)]),
    );
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let events = journal(vec![placement(captured)], &sources);

    let observed = events
        .capture_index_bucket_barrier(1, 2, Some(&barrier(published, 1, 1)))
        .await
        .unwrap();

    assert_eq!(observed.atomic, captured);
    assert_eq!(observed.sources[&NodeId(1)].next_offset, 2);
    assert_eq!(observed.sources[&NodeId(2)].next_offset, 1);
}

#[tokio::test]
async fn unrelated_bucket_activity_advances_only_the_atomic_proof() {
    let published = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let captured = AtomicProgramWatermark::new(Some(42), Some(42), 0);
    let sources = MemorySources::default();
    sources.journals.lock().unwrap().insert(
        NodeId(1),
        (status(1, 1), vec![change_in_bucket(1, 1, 9, 9)]),
    );
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let events = journal(vec![placement(captured)], &sources);

    let observed = events
        .capture_index_bucket_barrier(1, 2, Some(&barrier(published, 1, 1)))
        .await
        .unwrap();

    assert_eq!(observed.atomic, captured);
    assert_eq!(observed.sources[&NodeId(1)].next_offset, 1);
    assert_eq!(observed.sources[&NodeId(2)].next_offset, 1);
}

#[tokio::test]
async fn later_in_progress_program_does_not_invalidate_a_bounded_page() {
    let captured = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let later_pending = AtomicProgramWatermark::new(Some(41), Some(40), 1);
    let sources = MemorySources::default();
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (status(1, 1), vec![change(1, 1)]));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let events = journal(vec![placement(later_pending)], &sources);

    let page = events
        .next_page(
            1,
            2,
            &barrier(captured, 1, 1),
            &barrier(captured, 2, 1),
            MAX_INDEX_EVENT_PAGE_BYTES,
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(page.changes.len(), 1);
    assert_eq!(page.through, barrier(captured, 2, 1));
}

#[tokio::test]
async fn later_in_progress_program_does_not_invalidate_publication() {
    let captured = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let later_pending = AtomicProgramWatermark::new(Some(41), Some(40), 1);
    let sources = MemorySources::default();
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (status(1, 1), vec![change(1, 1)]));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let events = journal(vec![placement(later_pending)], &sources);

    events
        .validate_publication_barrier(&barrier(captured, 2, 1))
        .await
        .unwrap();
}

#[tokio::test]
async fn publication_cohort_observes_each_source_once() {
    let clear = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let sources = MemorySources::default();
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (status(1, 2), Vec::new()));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 1), Vec::new()));
    let events = journal(vec![placement(clear)], &sources);

    let outcomes = events
        .validate_publication_barriers(&[
            barrier(clear, 2, 1),
            barrier(clear, 3, 2),
            barrier(clear, 1, 1),
        ])
        .await;

    assert!(outcomes.into_iter().all(|outcome| outcome.is_ok()));
    let mut reads = sources.status_reads.lock().unwrap().clone();
    reads.sort();
    assert_eq!(reads, vec![NodeId(1), NodeId(2)]);
}

#[tokio::test]
async fn publication_cohort_rejects_a_candidate_from_an_old_source_epoch() {
    let clear = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let sources = MemorySources::default();
    let mut restarted = status(1, 2);
    restarted.source_id.source_epoch = [9; 32];
    let restarted_source = restarted.source_id;
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (restarted, Vec::new()));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 1), Vec::new()));
    let events = journal(vec![placement(clear)], &sources);
    let old_epoch = barrier(clear, 2, 1);
    let mut current_epoch = old_epoch.clone();
    current_epoch.sources.get_mut(&NodeId(1)).unwrap().source = restarted_source;

    let outcomes = events
        .validate_publication_barriers(&[old_epoch, current_epoch])
        .await;

    assert_eq!(
        outcomes,
        vec![Err(IndexEventError::IncompleteSources), Ok(())]
    );
}

#[tokio::test]
async fn reserved_index_artifacts_cannot_advance_or_rewake_their_index() {
    let clear = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources.journals.lock().unwrap().insert(
        NodeId(1),
        (
            status(1, 1),
            vec![change_at_path(
                1,
                1,
                1,
                2,
                "_keldra/index-projections/v1/family/partitions",
            )],
        ),
    );
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let events = journal(vec![placement(clear)], &sources);
    let target = events.capture_barrier().await.unwrap();
    let mut indexed = target.clone();
    indexed.sources.get_mut(&NodeId(1)).unwrap().next_offset = 1;

    assert!(
        events
            .routed_index_effects(1, 2, &indexed, &target)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        events
            .capture_index_bucket_barrier(1, 2, Some(&indexed))
            .await
            .unwrap(),
        indexed
    );
}

#[tokio::test]
async fn raw_interval_reads_one_bounded_page_without_bucket_probes() {
    let clear = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources.journals.lock().unwrap().insert(
        NodeId(1),
        (status(1, 1), vec![change_in_bucket(1, 1, 9, 9)]),
    );
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let target = journal(vec![placement(clear)], &sources)
        .capture_barrier()
        .await
        .unwrap();
    let mut from = target.clone();
    from.sources.get_mut(&NodeId(1)).unwrap().next_offset = 1;

    let page = journal(vec![placement(clear)], &sources)
        .next_raw_page(&from, &target, MAX_INDEX_EVENT_PAGE_BYTES)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(page.through, target);
    assert_eq!(page.changes.len(), 1);
    assert_eq!(change_bucket(&page.changes[0].change), Some((9, 9)));
    assert_eq!(*sources.raw_reads.lock().unwrap(), [NodeId(1)]);
    assert!(sources.reads.lock().unwrap().is_empty());
}

#[tokio::test]
async fn page_is_rejected_before_it_can_be_retained_over_the_byte_cap() {
    let clear = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (status(1, 1), vec![change(1, 1)]));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let from = IndexBarrier {
        fence: PlacementLogId { term: 3, index: 7 },
        atomic: clear,
        sources: BTreeMap::from([
            (
                NodeId(1),
                IndexSourceCursor {
                    source: source_id(1),
                    next_offset: 1,
                },
            ),
            (
                NodeId(2),
                IndexSourceCursor {
                    source: source_id(2),
                    next_offset: 1,
                },
            ),
        ]),
    };
    let target = IndexBarrier {
        sources: BTreeMap::from([
            (
                NodeId(1),
                IndexSourceCursor {
                    source: source_id(1),
                    next_offset: 2,
                },
            ),
            (
                NodeId(2),
                IndexSourceCursor {
                    source: source_id(2),
                    next_offset: 1,
                },
            ),
        ]),
        ..from.clone()
    };
    let error = journal(vec![placement(clear)], &sources)
        .next_page(1, 2, &from, &target, 1)
        .await
        .unwrap_err();
    assert!(matches!(error, IndexEventError::PageBytesExceeded { .. }));
}

#[tokio::test]
async fn byte_cap_returns_a_nonempty_prefix_without_advancing_past_it() {
    let clear = AtomicProgramWatermark::new(None, None, 0);
    let sources = MemorySources::default();
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(1), (status(1, 2), vec![change(1, 1), change(1, 2)]));
    sources
        .journals
        .lock()
        .unwrap()
        .insert(NodeId(2), (status(2, 0), Vec::new()));
    let from = IndexBarrier {
        fence: PlacementLogId { term: 3, index: 7 },
        atomic: clear,
        sources: BTreeMap::from([
            (
                NodeId(1),
                IndexSourceCursor {
                    source: source_id(1),
                    next_offset: 1,
                },
            ),
            (
                NodeId(2),
                IndexSourceCursor {
                    source: source_id(2),
                    next_offset: 1,
                },
            ),
        ]),
    };
    let target = IndexBarrier {
        sources: BTreeMap::from([
            (
                NodeId(1),
                IndexSourceCursor {
                    source: source_id(1),
                    next_offset: 3,
                },
            ),
            (
                NodeId(2),
                IndexSourceCursor {
                    source: source_id(2),
                    next_offset: 1,
                },
            ),
        ]),
        ..from.clone()
    };
    let journal = IndexEventJournal::new(
        Arc::new(MemoryAuthority {
            sequence: Arc::new(Mutex::new(vec![placement(clear)])),
        }),
        Arc::new(sources),
    )
    .with_page_size(2);
    let first_size = encoded_len(&change(1, 1)).unwrap();

    let first = journal
        .next_page(1, 2, &from, &target, first_size)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.changes.len(), 1);
    assert_eq!(first.changes[0].change.offset(), 1);
    assert_eq!(first.through.sources[&NodeId(1)].next_offset, 2);

    let second = journal
        .next_page(1, 2, &first.through, &target, first_size)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.changes.len(), 1);
    assert_eq!(second.changes[0].change.offset(), 2);
    assert_eq!(second.through, target);
}

#[test]
fn publication_watermark_defers_later_atomic_heads() {
    let barrier = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    assert!(barrier.permits(None));
    assert!(barrier.permits(Some(40)));
    assert!(!barrier.permits(Some(41)));
    assert!(!barrier.permits(Some(0)));
}

#[test]
fn shared_bucket_page_is_trimmed_without_changing_its_cached_source() {
    let key = BucketPageKey {
        node: NodeId(1),
        source: source_id(1),
        tenant_id: 1,
        bucket_id: 2,
        after_offset: 0,
    };
    let first = change(1, 1);
    let second = change(1, 2);
    // Deliberately differ from the changes' JSON sizes: cache trimming must
    // use the exact charges preserved by the source scan, not re-encode them.
    let first_bytes = 17;
    let second_bytes = 29;
    let page = IndexSourcePage {
        source_id: source_id(1),
        changes: vec![first, second],
        change_encoded_bytes: vec![first_bytes, second_bytes],
        encoded_bytes: first_bytes + second_bytes,
        through_offset: 2,
        oversize: None,
    };
    let mut cache = BucketPageCache::default();
    cache.insert(key.clone(), 2, first_bytes + second_bytes, page.clone());

    let trimmed = cache.get(&key, 1, first_bytes).unwrap().unwrap();
    assert_eq!(trimmed.changes.len(), 1);
    assert_eq!(trimmed.changes[0].offset(), 1);
    assert_eq!(trimmed.through_offset, 1);
    assert_eq!(trimmed.encoded_bytes, first_bytes);
    assert_eq!(trimmed.change_encoded_bytes, [first_bytes]);

    let complete = cache
        .get(&key, 2, first_bytes + second_bytes)
        .unwrap()
        .unwrap();
    assert_eq!(complete.changes, page.changes);
    assert_eq!(complete.change_encoded_bytes, page.change_encoded_bytes);
    assert_eq!(complete.through_offset, 2);
}

#[test]
fn cached_bucket_page_rejects_missing_per_change_byte_evidence() {
    let page = IndexSourcePage {
        source_id: source_id(1),
        changes: vec![change(1, 1)],
        change_encoded_bytes: Vec::new(),
        encoded_bytes: 17,
        through_offset: 1,
        oversize: None,
    };

    assert!(matches!(
        trim_cached_page(&page, 0, 1, 17),
        Err(IndexEventError::PageLengthMismatch { .. })
    ));
}

#[test]
fn shared_bucket_cache_evicts_old_pages_at_its_process_bound() {
    let mut cache = BucketPageCache::default();
    for after_offset in [0, 1] {
        let key = BucketPageKey {
            node: NodeId(1),
            source: source_id(1),
            tenant_id: 1,
            bucket_id: 2,
            after_offset,
        };
        cache.insert(
            key,
            after_offset + 1,
            10 * 1024 * 1024,
            IndexSourcePage {
                source_id: source_id(1),
                changes: vec![change(1, after_offset + 1)],
                change_encoded_bytes: vec![10 * 1024 * 1024],
                encoded_bytes: 10 * 1024 * 1024,
                through_offset: after_offset + 1,
                oversize: None,
            },
        );
    }

    assert!(cache.charged_bytes <= BUCKET_PAGE_CACHE_BYTES);
    assert_eq!(cache.pages.len(), 1);
    assert!(!cache.pages.keys().any(|key| key.after_offset == 0));
    assert!(cache.pages.keys().any(|key| key.after_offset == 1));
}

#[test]
fn cached_terminal_page_is_not_used_for_a_later_target() {
    let key = BucketPageKey {
        node: NodeId(1),
        source: source_id(1),
        tenant_id: 1,
        bucket_id: 2,
        after_offset: 0,
    };
    let mut cache = BucketPageCache::default();
    cache.insert(
        key.clone(),
        1,
        4096,
        IndexSourcePage {
            source_id: source_id(1),
            changes: vec![change(1, 1)],
            change_encoded_bytes: vec![encoded_len(&change(1, 1)).unwrap()],
            encoded_bytes: encoded_len(&change(1, 1)).unwrap(),
            through_offset: 1,
            oversize: None,
        },
    );

    assert!(cache.get(&key, 2, 4096).unwrap().is_none());
}

#[test]
fn local_routed_history_failures_keep_their_recovery_classification() {
    for error in [
        keldra_store::RoutedJournalError::CursorExpired {
            cursor: 4,
            retention_floor: 5,
        },
        keldra_store::RoutedJournalError::CursorFuture { cursor: 8, tail: 7 },
        keldra_store::RoutedJournalError::MissingPrimary { offset: 6 },
        keldra_store::RoutedJournalError::RouteMismatch { offset: 6 },
    ] {
        assert_eq!(
            local_routed_source_error(NodeId(2), error),
            IndexEventError::SourceHistoryGap(NodeId(2))
        );
    }
    assert_eq!(
        local_routed_source_error(
            NodeId(2),
            keldra_store::RoutedJournalError::SourceEpochMismatch,
        ),
        IndexEventError::SourceEpochChanged(NodeId(2))
    );
    assert!(matches!(
        local_routed_source_error(
            NodeId(2),
            keldra_store::RoutedJournalError::Storage("temporarily unavailable".into()),
        ),
        IndexEventError::Source {
            node: NodeId(2),
            ..
        }
    ));
}

#[test]
fn peer_status_codes_preserve_gap_but_leave_unavailability_transient() {
    assert_eq!(
        remote_routed_source_error(NodeId(3), tonic::Status::out_of_range("history gap")),
        IndexEventError::SourceHistoryGap(NodeId(3))
    );
    assert_eq!(
        remote_routed_source_error(
            NodeId(3),
            tonic::Status::failed_precondition("source epoch changed"),
        ),
        IndexEventError::SourceEpochChanged(NodeId(3))
    );
    assert!(matches!(
        remote_routed_source_error(NodeId(3), tonic::Status::unavailable("retry")),
        IndexEventError::Source {
            node: NodeId(3),
            ..
        }
    ));
}
