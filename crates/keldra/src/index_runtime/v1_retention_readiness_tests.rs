use super::super::events::{AtomicProgramWatermark, IndexBarrier};
use super::*;
use keldra_store::{PlacementLogId, SourceId};

#[test]
fn retention_uses_only_snapshot_paired_cut_and_exact_source_fence() {
    let catalog = IndexCatalog::default();
    let source = SourceId {
        node_id: 1,
        source_epoch: [9; 32],
    };
    let fence = PlacementLogId { term: 1, index: 12 };
    let barrier = IndexBarrier {
        fence,
        atomic: AtomicProgramWatermark::new(None, None, 0),
        sources: [(
            NodeId(1),
            IndexSourceCursor {
                source,
                next_offset: 47,
            },
        )]
        .into_iter()
        .collect(),
    };
    assert!(catalog.retention_snapshot().unwrap().is_none());
    let generation = catalog.catalog_replay_generation().unwrap();
    assert!(
        catalog
            .complete_catalog_replay(generation, &barrier)
            .unwrap()
    );
    let snapshot = catalog.retention_snapshot().unwrap().unwrap();
    let live = IndexSourceCursor {
        source,
        next_offset: 4130,
    };
    assert_eq!(
        catalog_checkpoint_limit(&snapshot, live, fence).unwrap(),
        Some(47)
    );
    assert_eq!(
        catalog_checkpoint_limit(
            &snapshot,
            IndexSourceCursor {
                next_offset: 40,
                ..live
            },
            fence
        )
        .unwrap(),
        Some(40)
    );
    assert_eq!(
        catalog_checkpoint_limit(&snapshot, live, PlacementLogId { index: 13, ..fence }).unwrap(),
        None
    );
    assert_eq!(
        catalog_checkpoint_limit(
            &snapshot,
            IndexSourceCursor {
                source: SourceId {
                    source_epoch: [10; 32],
                    ..source
                },
                ..live
            },
            fence
        )
        .unwrap(),
        None
    );
    assert_eq!(
        catalog_checkpoint_limit(
            &snapshot,
            IndexSourceCursor {
                source: SourceId {
                    node_id: 2,
                    ..source
                },
                ..live
            },
            fence
        )
        .unwrap(),
        None
    );
}
