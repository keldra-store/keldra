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
    ) -> Result<IndexSourcePage, IndexEventError> {
        let journals = self.journals.lock().unwrap();
        let (status, changes) = journals.get(&source.node).unwrap();
        Ok(IndexSourcePage {
            source_id: if status.source_id == expected_source {
                status.source_id
            } else {
                expected_source
            },
            changes: changes
                .iter()
                .filter(|change| change.offset() > after_offset)
                .take(limit)
                .cloned()
                .collect(),
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
async fn drain_reads_every_source_through_the_exact_vector() {
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
    let batch = journal(vec![placement(clear)], &sources)
        .drain(&from, target.clone())
        .await
        .unwrap();

    let mut offsets = batch
        .changes
        .iter()
        .map(|change| (change.node, change.change.offset()))
        .collect::<Vec<_>>();
    offsets.sort_unstable();
    assert_eq!(offsets, [(NodeId(1), 1), (NodeId(1), 2), (NodeId(2), 1)]);
    assert_eq!(batch.through, target);
}

#[test]
fn publication_watermark_defers_later_atomic_heads() {
    let barrier = AtomicProgramWatermark::new(Some(40), Some(40), 0);
    assert!(barrier.permits(None));
    assert!(barrier.permits(Some(40)));
    assert!(!barrier.permits(Some(41)));
    assert!(!barrier.permits(Some(0)));
}
