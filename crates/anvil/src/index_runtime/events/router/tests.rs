use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use anvil_consensus::NodeId;
use anvil_store::{
    LocalChange, ObjectHeadChange, ObjectHeadChangeKind, PlacementLogId, ReferenceDelta, SourceId,
    VersionId, WatchJournalStatus,
};

use super::*;
use crate::index_runtime::events::{
    AtomicProgramWatermark, IndexEventAuthority, IndexEventPlacement, IndexEventSources,
    IndexJournalChange, IndexSource, IndexSourceCursor, IndexSourcePage,
};

#[derive(Clone)]
struct MutableAuthority {
    placement: Arc<Mutex<IndexEventPlacement>>,
}

impl IndexEventAuthority for MutableAuthority {
    fn current(&self) -> Result<IndexEventPlacement, String> {
        Ok(self.placement.lock().unwrap().clone())
    }
}

#[derive(Clone, Default)]
struct CountingSources {
    journals: Arc<Mutex<BTreeMap<NodeId, (WatchJournalStatus, Vec<LocalChange>)>>>,
    page_reads: Arc<AtomicUsize>,
}

impl CountingSources {
    fn set(&self, node: u16, tail: u64, changes: Vec<LocalChange>) {
        self.journals
            .lock()
            .unwrap()
            .insert(NodeId(u64::from(node)), (status(node, tail), changes));
    }
}

#[tonic::async_trait]
impl IndexEventSources for CountingSources {
    async fn status(&self, source: &IndexSource) -> Result<WatchJournalStatus, IndexEventError> {
        self.journals
            .lock()
            .unwrap()
            .get(&source.node)
            .map(|(status, _)| *status)
            .ok_or_else(|| IndexEventError::Source {
                node: source.node,
                message: "missing source".into(),
            })
    }

    async fn read_page(
        &self,
        source: &IndexSource,
        _expected_source: SourceId,
        after_offset: u64,
        limit: usize,
    ) -> Result<IndexSourcePage, IndexEventError> {
        self.page_reads.fetch_add(1, Ordering::Relaxed);
        let journals = self.journals.lock().unwrap();
        let (status, changes) = journals.get(&source.node).unwrap();
        Ok(IndexSourcePage {
            source_id: status.source_id,
            changes: changes
                .iter()
                .filter(|change| change.offset() > after_offset)
                .take(limit)
                .cloned()
                .collect(),
        })
    }
}

fn atomic() -> AtomicProgramWatermark {
    AtomicProgramWatermark::new(Some(8), Some(8), 0)
}

fn placement(fence_index: u64) -> IndexEventPlacement {
    IndexEventPlacement {
        fence: PlacementLogId {
            term: 2,
            index: fence_index,
        },
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
        atomic: atomic(),
    }
}

fn source_id(node: u16, epoch: u8) -> SourceId {
    SourceId {
        node_id: node,
        source_epoch: [epoch; 32],
    }
}

fn status(node: u16, tail: u64) -> WatchJournalStatus {
    WatchJournalStatus {
        source_id: source_id(node, node as u8),
        tail,
        retention_floor: 0,
        retained_entries: tail,
        retained_bytes: tail.saturating_mul(16),
    }
}

fn change(node: u16, offset: u64) -> LocalChange {
    LocalChange::ObjectHead(ObjectHeadChange {
        offset,
        tenant_id: 4,
        bucket_id: 5,
        exact_path: format!("node-{node}/{offset}"),
        path_version: VersionId(offset),
        kind: ObjectHeadChangeKind::Put,
        reference_deltas: Vec::<ReferenceDelta>::new(),
    })
}

fn barrier(fence_index: u64, epoch: u8, one_next: u64, two_next: u64) -> IndexBarrier {
    IndexBarrier {
        fence: PlacementLogId {
            term: 2,
            index: fence_index,
        },
        atomic: atomic(),
        sources: BTreeMap::from([
            (
                NodeId(1),
                IndexSourceCursor {
                    source: source_id(1, epoch),
                    next_offset: one_next,
                },
            ),
            (
                NodeId(2),
                IndexSourceCursor {
                    source: source_id(2, epoch),
                    next_offset: two_next,
                },
            ),
        ]),
    }
}

fn batch(through: IndexBarrier, changes: &[(u16, u64)]) -> IndexJournalBatch {
    IndexJournalBatch {
        changes: changes
            .iter()
            .map(|(node, offset)| IndexJournalChange {
                node: NodeId(u64::from(*node)),
                source: source_id(*node, 1),
                change: change(*node, *offset),
            })
            .collect(),
        through,
    }
}

fn available(result: IndexEventCatchUp) -> (Vec<Arc<IndexJournalBatch>>, IndexBarrier) {
    match result {
        IndexEventCatchUp::Available { batches, through } => (batches, through),
        IndexEventCatchUp::RescanRequired { reason, .. } => {
            panic!("unexpected rescan requirement: {reason:?}")
        }
    }
}

#[tokio::test]
async fn two_builders_share_one_background_fetch() {
    let authority = MutableAuthority {
        placement: Arc::new(Mutex::new(placement(7))),
    };
    let sources = CountingSources::default();
    sources.set(1, 0, Vec::new());
    sources.set(2, 0, Vec::new());
    let journal = Arc::new(IndexEventJournal::new(
        Arc::new(authority),
        Arc::new(sources.clone()),
    ));
    let retention = IndexEventRouterRetention::new(8, 100).unwrap();
    let (router, task) = IndexEventRouter::start(journal, retention, Duration::from_millis(5))
        .await
        .unwrap();
    let baseline = router.current_barrier().await;

    sources.set(1, 1, vec![change(1, 1)]);
    sources.set(2, 1, vec![change(2, 1)]);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let current = router.current_barrier().await;
            if current
                .sources
                .values()
                .all(|cursor| cursor.next_offset == 2)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    let reads_before_requests = sources.page_reads.load(Ordering::Relaxed);
    let (first, _) = available(router.changes_after(&baseline).await);
    let (second, _) = available(router.changes_after(&baseline).await);
    assert_eq!(
        first.iter().map(|batch| batch.changes.len()).sum::<usize>(),
        2
    );
    assert_eq!(first.len(), second.len());
    assert!(
        first
            .iter()
            .zip(&second)
            .all(|(left, right)| Arc::ptr_eq(left, right))
    );
    assert_eq!(
        sources.page_reads.load(Ordering::Relaxed),
        reads_before_requests
    );
    task.shutdown().await.unwrap();
}

#[tokio::test]
async fn exact_barriers_return_only_later_complete_batches() {
    let initial = barrier(7, 1, 1, 1);
    let first = barrier(7, 1, 2, 1);
    let second = barrier(7, 1, 2, 2);
    let mut state = RouterState::new(initial.clone());
    let retention = IndexEventRouterRetention::new(8, 100).unwrap();
    state.append(batch(first.clone(), &[(1, 1)]), retention);
    state.append(batch(second.clone(), &[(2, 1)]), retention);
    let router = IndexEventRouter {
        state: Arc::new(RwLock::new(state)),
    };

    let (all, through) = available(router.changes_after(&initial).await);
    assert_eq!(all.len(), 2);
    assert_eq!(through, second);
    let (later, _) = available(router.changes_after(&first).await);
    assert_eq!(later.len(), 1);
    assert_eq!(later[0].through, second);
    let (none, _) = available(router.changes_after(&second).await);
    assert!(none.is_empty());
}

#[tokio::test]
async fn bounded_rollover_and_epoch_loss_require_rescan() {
    let initial = barrier(7, 1, 1, 1);
    let first = barrier(7, 1, 2, 1);
    let second = barrier(7, 1, 2, 2);
    let mut state = RouterState::new(initial.clone());
    let retention = IndexEventRouterRetention::new(1, 100).unwrap();
    state.append(batch(first.clone(), &[(1, 1)]), retention);
    state.append(batch(second.clone(), &[(2, 1)]), retention);
    let router = IndexEventRouter {
        state: Arc::new(RwLock::new(state)),
    };

    assert!(matches!(
        router.changes_after(&initial).await,
        IndexEventCatchUp::RescanRequired {
            reason: IndexRescanReason::HistoryUnavailable,
            ..
        }
    ));
    let next_epoch = barrier(7, 9, 1, 1);
    router.state.write().await.rebase(next_epoch);
    assert!(matches!(
        router.changes_after(&second).await,
        IndexEventCatchUp::RescanRequired {
            reason: IndexRescanReason::SourceEpochUnavailable,
            ..
        }
    ));
}

#[tokio::test]
async fn placement_change_rebases_and_requires_rescan() {
    let authority = MutableAuthority {
        placement: Arc::new(Mutex::new(placement(7))),
    };
    let sources = CountingSources::default();
    sources.set(1, 0, Vec::new());
    sources.set(2, 0, Vec::new());
    let journal = IndexEventJournal::new(Arc::new(authority.clone()), Arc::new(sources.clone()));
    let initial = journal.capture_barrier().await.unwrap();
    let state = RwLock::new(RouterState::new(initial.clone()));
    *authority.placement.lock().unwrap() = placement(8);

    assert!(
        collect_once(
            &journal,
            &state,
            IndexEventRouterRetention::new(8, 100).unwrap(),
        )
        .await
        .is_err()
    );
    let router = IndexEventRouter {
        state: Arc::new(state),
    };
    assert!(matches!(
        router.changes_after(&initial).await,
        IndexEventCatchUp::RescanRequired {
            reason: IndexRescanReason::PlacementFenceUnavailable,
            ..
        }
    ));
    assert_eq!(router.current_barrier().await.fence.index, 8);
}
