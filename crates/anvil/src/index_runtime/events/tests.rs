use std::sync::Mutex;

use anvil_store::{ObjectHeadChange, ObjectHeadChangeKind, ReferenceDelta, VersionId};

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
}

#[tonic::async_trait]
impl IndexEventSources for MemorySources {
    async fn status(&self, source: &IndexSource) -> Result<WatchJournalStatus, IndexEventError> {
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

    async fn read_page(
        &self,
        source: &IndexSource,
        expected_source: SourceId,
        after_offset: u64,
        limit: usize,
        max_bytes: u64,
    ) -> Result<IndexSourcePage, IndexEventError> {
        let journals = self.journals.lock().unwrap();
        let (status, changes) = journals.get(&source.node).unwrap();
        let mut selected = Vec::new();
        let mut encoded_bytes = 0_u64;
        let mut oversize = None;
        for change in changes
            .iter()
            .filter(|change| change.offset() > after_offset)
            .take(limit)
        {
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
            selected.push(change.clone());
        }
        Ok(IndexSourcePage {
            source_id: if status.source_id == expected_source {
                status.source_id
            } else {
                expected_source
            },
            changes: selected,
            encoded_bytes,
            oversize,
        })
    }
}

fn source_id(node: u16) -> SourceId {
    SourceId {
        node_id: node,
        source_epoch: [node as u8; 32],
    }
}

fn change(node: u16, offset: u64) -> LocalChange {
    LocalChange::ObjectHead(ObjectHeadChange {
        offset,
        tenant_id: 1,
        bucket_id: 2,
        exact_path: format!("source-{node}/{offset}"),
        path_version: VersionId(offset),
        kind: ObjectHeadChangeKind::Put,
        reference_deltas: Vec::<ReferenceDelta>::new(),
        accounting_transition: None,
    })
}

fn status(node: u16, tail: u64) -> WatchJournalStatus {
    WatchJournalStatus {
        source_id: source_id(node),
        tail,
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

#[test]
fn snapshot_tails_form_the_exact_active_source_vector() {
    let clear = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let journal = journal(vec![placement(clear)], &MemorySources::default());
    let barrier = journal
        .barrier_from_snapshot_tails(
            PlacementLogId { term: 3, index: 7 },
            &[(NodeId(1), source_id(1), 8), (NodeId(2), source_id(2), 13)],
        )
        .unwrap();

    assert_eq!(barrier.atomic, clear);
    assert_eq!(barrier.sources[&NodeId(1)].next_offset, 9);
    assert_eq!(barrier.sources[&NodeId(2)].next_offset, 14);
    assert_eq!(journal.last_observed_barrier(), Some(barrier));
}

#[test]
fn snapshot_tails_reject_an_incomplete_or_pending_boundary() {
    let clear = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    let incomplete = journal(vec![placement(clear)], &MemorySources::default())
        .barrier_from_snapshot_tails(
            PlacementLogId { term: 3, index: 7 },
            &[(NodeId(1), source_id(1), 8)],
        )
        .unwrap_err();
    assert_eq!(incomplete, IndexEventError::IncompleteSources);

    let pending = AtomicProgramWatermark::new(Some(41), Some(40), 1);
    let pending = journal(vec![placement(pending)], &MemorySources::default())
        .barrier_from_snapshot_tails(
            PlacementLogId { term: 3, index: 7 },
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
async fn unfinalized_atomic_tail_cannot_form_a_generation_barrier() {
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
        .next_page(&cursor, &target, MAX_INDEX_EVENT_PAGE_BYTES)
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
        .next_page(&from, &target, 1)
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
        .next_page(&from, &target, first_size)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.changes.len(), 1);
    assert_eq!(first.changes[0].change.offset(), 1);
    assert_eq!(first.through.sources[&NodeId(1)].next_offset, 2);

    let second = journal
        .next_page(&first.through, &target, first_size)
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
