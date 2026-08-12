use std::collections::BTreeMap;

use anvil_store::VersionId;

use super::*;
use crate::index_runtime::events::{AtomicProgramWatermark, IndexSourceCursor};

fn fence() -> PlacementLogId {
    PlacementLogId { term: 3, index: 9 }
}

fn source(node_id: u16, settled: u64) -> WatchJournalStatus {
    WatchJournalStatus {
        source_id: SourceId {
            node_id,
            source_epoch: [node_id as u8; 32],
        },
        tail: settled,
        settled_through: settled,
        retention_floor: 0,
        retained_entries: settled,
        retained_bytes: settled * 32,
    }
}

fn assignment(kind: DefinitionKind, id: u64) -> DefinitionAssignment {
    DefinitionAssignment {
        kind,
        tenant_id: 2,
        bucket_id: 3,
        definition_id: id,
        definition_path: format!("_anvil/definitions/{id}"),
        object_version: VersionId(id + 100),
        observed_fence: fence(),
        rank: 0,
    }
}

fn barrier(cursors: &[(WatchJournalStatus, u64)]) -> DerivedBarrierEvidence {
    DerivedBarrierEvidence::Published(IndexBarrier {
        fence: fence(),
        atomic: AtomicProgramWatermark::new(Some(2), Some(2), 0),
        sources: cursors
            .iter()
            .map(|(status, next_offset)| {
                (
                    NodeId(u64::from(status.source_id.node_id)),
                    IndexSourceCursor {
                        source: status.source_id,
                        next_offset: *next_offset,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>(),
    })
}

fn current(
    kind: DerivedConsumerKind,
    status: WatchJournalStatus,
    next_offset: u64,
) -> DefinitionCheckpoint {
    DefinitionCheckpoint {
        consumer_kind: retention_kind(kind),
        source_id: status.source_id,
        next_offset,
        observed_fence: fence(),
    }
}

#[test]
fn completed_empty_inventory_advances_each_source_to_its_settled_tail() {
    let first = source(1, 20);
    let second = source(2, 8);
    let tracker = SparseDerivedInventory::begin(
        DerivedConsumerKind::Index,
        NodeId(3),
        fence(),
        [(first, None), (second, None)],
    )
    .unwrap()
    .finish();
    let checkpoints = tracker.checkpoints().unwrap();
    assert_eq!(checkpoints[0].source_id, first.source_id);
    assert_eq!(checkpoints[0].next_offset, 21);
    assert_eq!(checkpoints[1].source_id, second.source_id);
    assert_eq!(checkpoints[1].next_offset, 9);
}

#[test]
fn inventory_retains_only_sparse_affected_definitions_and_uses_the_minimum_proof() {
    let status = source(1, 20);
    let first = assignment(DefinitionKind::Index, 1);
    let second = assignment(DefinitionKind::Index, 2);
    let proof = barrier(&[(status, 12)]);
    let older = barrier(&[(status, 7)]);
    let mut inventory = SparseDerivedInventory::begin(
        DerivedConsumerKind::Index,
        NodeId(3),
        fence(),
        [(status, Some(current(DerivedConsumerKind::Index, status, 5)))],
    )
    .unwrap();
    inventory
        .record_affected(&first, status.source_id, 18, Some(&proof))
        .unwrap();
    inventory
        .record_affected(&second, status.source_id, 15, Some(&older))
        .unwrap();
    let tracker = inventory.finish();
    assert_eq!(tracker.affected_len(status.source_id), 2);
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 7);
}

#[test]
fn published_or_snapshot_proof_releases_only_the_effects_it_covers() {
    let status = source(1, 20);
    let assignment = assignment(DefinitionKind::Index, 1);
    let initial = barrier(&[(status, 6)]);
    let mut inventory = SparseDerivedInventory::begin(
        DerivedConsumerKind::Index,
        NodeId(3),
        fence(),
        [(status, None)],
    )
    .unwrap();
    inventory
        .record_affected(&assignment, status.source_id, 14, Some(&initial))
        .unwrap();
    let mut tracker = inventory.finish();
    tracker
        .observe_proof(
            &assignment,
            &DerivedBarrierEvidence::ScopedSnapshot(match barrier(&[(status, 14)]) {
                DerivedBarrierEvidence::Published(barrier) => barrier,
                DerivedBarrierEvidence::ScopedSnapshot(_) => unreachable!(),
            }),
        )
        .unwrap();
    assert_eq!(tracker.affected_len(status.source_id), 0);
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 21);
}

#[test]
fn incremental_routed_effect_adds_one_sparse_pin_then_publication_removes_it() {
    let status = source(1, 20);
    let assignment = assignment(DefinitionKind::Accounting, 1);
    let initial = barrier(&[(status, 10)]);
    let mut tracker = SparseDerivedInventory::begin(
        DerivedConsumerKind::Accounting,
        NodeId(3),
        fence(),
        [(
            status,
            Some(current(DerivedConsumerKind::Accounting, status, 8)),
        )],
    )
    .unwrap()
    .finish();
    tracker
        .observe_routed_effect(&assignment, status.source_id, 13, Some(&initial))
        .unwrap();
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 10);
    tracker
        .observe_proof(&assignment, &barrier(&[(status, 13)]))
        .unwrap();
    assert_eq!(tracker.affected_len(status.source_id), 0);
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 21);
}

#[test]
fn durable_acknowledgement_becomes_the_floor_for_later_missing_proof() {
    let status = source(1, 20);
    let first = assignment(DefinitionKind::Index, 1);
    let second = assignment(DefinitionKind::Index, 2);
    let mut tracker = SparseDerivedInventory::begin(
        DerivedConsumerKind::Index,
        NodeId(3),
        fence(),
        [(status, None)],
    )
    .unwrap()
    .finish();
    let checkpoint = tracker.checkpoints().unwrap()[0];
    tracker.acknowledge(checkpoint).unwrap();
    tracker
        .observe_routed_effect(&first, status.source_id, 21, None)
        .unwrap();
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 21);
    tracker.remove_assignment(&first).unwrap();
    tracker
        .observe_routed_effect(&second, status.source_id, 21, None)
        .unwrap();
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 21);
}

#[test]
fn missing_or_behind_proof_never_regresses_the_durable_baseline() {
    let status = source(1, 20);
    let assignment = assignment(DefinitionKind::Index, 1);
    let mut inventory = SparseDerivedInventory::begin(
        DerivedConsumerKind::Index,
        NodeId(3),
        fence(),
        [(
            status,
            Some(current(DerivedConsumerKind::Index, status, 12)),
        )],
    )
    .unwrap();
    inventory
        .record_affected(&assignment, status.source_id, 18, None)
        .unwrap();
    let tracker = inventory.finish();
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 12);
}

#[test]
fn wrong_kind_or_query_replica_cannot_enter_the_inventory() {
    let status = source(1, 20);
    let mut inventory = SparseDerivedInventory::begin(
        DerivedConsumerKind::Index,
        NodeId(3),
        fence(),
        [(status, None)],
    )
    .unwrap();
    assert_eq!(
        inventory.record_affected(
            &assignment(DefinitionKind::Accounting, 1),
            status.source_id,
            2,
            None,
        ),
        Err(SparseTrackerError::WrongAssignment)
    );
    let mut replica = assignment(DefinitionKind::Index, 2);
    replica.rank = 1;
    assert_eq!(
        inventory.record_affected(&replica, status.source_id, 2, None),
        Err(SparseTrackerError::WrongAssignment)
    );
}
