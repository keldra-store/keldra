use std::collections::BTreeMap;

use keldra_store::VersionId;

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
        definition_path: format!("_keldra/definitions/{id}"),
        object_version: VersionId(id + 100),
        observed_fence: fence(),
        rank: 0,
    }
}

fn barrier(cursors: &[(WatchJournalStatus, u64)]) -> DerivedBarrierEvidence {
    barrier_atomic(cursors, 2)
}

fn barrier_atomic(
    cursors: &[(WatchJournalStatus, u64)],
    atomic_through: u64,
) -> DerivedBarrierEvidence {
    DerivedBarrierEvidence::Published(IndexBarrier {
        fence: fence(),
        atomic: AtomicProgramWatermark::new(Some(atomic_through), Some(atomic_through), 0),
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

#[test]
fn atomic_effect_holds_its_earliest_source_offset_until_atomic_proof_covers_it() {
    let status = source(1, 20);
    let assignment = assignment(DefinitionKind::Index, 1);
    let mut inventory = SparseDerivedInventory::begin(
        DerivedConsumerKind::Index,
        NodeId(3),
        fence(),
        [(status, None)],
    )
    .unwrap();
    inventory
        .record_affected(
            &assignment,
            status.source_id,
            5,
            Some(20),
            Some(4),
            Some(&barrier_atomic(&[(status, 5)], 15)),
        )
        .unwrap();
    inventory
        .record_affected(
            &assignment,
            status.source_id,
            8,
            Some(25),
            Some(7),
            Some(&barrier_atomic(&[(status, 8)], 15)),
        )
        .unwrap();
    inventory
        .record_affected(
            &assignment,
            status.source_id,
            10,
            None,
            None,
            Some(&barrier_atomic(&[(status, 10)], 15)),
        )
        .unwrap();
    let mut tracker = inventory.finish();
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 4);

    tracker
        .observe_proof(&assignment, &barrier_atomic(&[(status, 10)], 20))
        .unwrap();
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 4);
    tracker
        .observe_proof(&assignment, &barrier_atomic(&[(status, 10)], 25))
        .unwrap();
    assert_eq!(tracker.affected_len(status.source_id), 0);
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 21);
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
fn a_new_membership_fence_publishes_its_floor_once_without_an_offset_advance() {
    let status = source(1, 0);
    let mut previous = current(DerivedConsumerKind::Index, status, 1);
    previous.observed_fence = PlacementLogId { term: 2, index: 8 };
    let mut tracker = SparseDerivedInventory::begin(
        DerivedConsumerKind::Index,
        NodeId(3),
        fence(),
        [(status, Some(previous))],
    )
    .unwrap()
    .finish();

    let checkpoints = tracker.checkpoints().unwrap();
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0].next_offset, 1);
    assert_eq!(checkpoints[0].observed_fence, fence());
    tracker.acknowledge(checkpoints[0]).unwrap();
    assert!(tracker.checkpoints().unwrap().is_empty());
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
        .record_affected(&first, status.source_id, 18, None, None, Some(&proof))
        .unwrap();
    inventory
        .record_affected(&second, status.source_id, 15, None, None, Some(&older))
        .unwrap();
    let tracker = inventory.finish();
    assert_eq!(tracker.affected_len(status.source_id), 2);
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 7);
}

#[test]
fn published_proof_releases_only_the_effects_it_covers() {
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
        .record_affected(
            &assignment,
            status.source_id,
            14,
            None,
            None,
            Some(&initial),
        )
        .unwrap();
    let mut tracker = inventory.finish();
    tracker
        .observe_proof(&assignment, &barrier(&[(status, 14)]))
        .unwrap();
    assert_eq!(tracker.affected_len(status.source_id), 0);
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 21);
}

#[test]
fn unpublished_construction_state_cannot_release_a_routed_effect() {
    let status = source(1, 20);
    let assignment = assignment(DefinitionKind::Index, 1);
    let mut inventory = SparseDerivedInventory::begin(
        DerivedConsumerKind::Index,
        NodeId(3),
        fence(),
        [(status, None)],
    )
    .unwrap();
    inventory
        .record_affected(&assignment, status.source_id, 14, None, None, None)
        .unwrap();

    let tracker = inventory.finish();
    assert_eq!(tracker.affected_len(status.source_id), 1);
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 1);
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
        .observe_routed_effect(
            &assignment,
            status.source_id,
            13,
            None,
            None,
            Some(&initial),
        )
        .unwrap();
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 10);
    tracker
        .observe_proof(&assignment, &barrier(&[(status, 13)]))
        .unwrap();
    assert_eq!(tracker.affected_len(status.source_id), 0);
    assert_eq!(tracker.checkpoints().unwrap()[0].next_offset, 21);
}

#[test]
fn newly_settled_status_must_precede_new_routed_effect_validation() {
    let old = source(1, 5);
    let current = assignment(DefinitionKind::Index, 1);
    let mut tracker = SparseDerivedInventory::begin(
        DerivedConsumerKind::Index,
        NodeId(3),
        fence(),
        [(old, None)],
    )
    .unwrap()
    .finish();

    assert!(matches!(
        tracker.observe_routed_effect(&current, old.source_id, 10, None, None, None),
        Err(SparseTrackerError::RoutedOffset {
            source_id,
            identity,
            required_next: 10,
            floor_next: 1,
            settled_next: 6,
        }) if source_id == old.source_id && identity == DerivedDefinitionIdentity::from_assignment(&current)
    ));

    tracker.update_source_status(source(1, 20)).unwrap();
    tracker
        .observe_routed_effect(&current, old.source_id, 10, None, None, None)
        .unwrap();
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
        .observe_routed_effect(&first, status.source_id, 21, None, None, None)
        .unwrap();
    assert!(tracker.checkpoints().unwrap().is_empty());
    tracker.remove_assignment(&first).unwrap();
    tracker
        .observe_routed_effect(&second, status.source_id, 21, None, None, None)
        .unwrap();
    assert!(tracker.checkpoints().unwrap().is_empty());
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
        .record_affected(&assignment, status.source_id, 18, None, None, None)
        .unwrap();
    let tracker = inventory.finish();
    assert!(tracker.checkpoints().unwrap().is_empty());
}

#[test]
fn only_unacknowledged_checkpoint_advances_are_emitted_and_failures_retry() {
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
        .record_affected(&assignment, status.source_id, 18, None, None, None)
        .unwrap();
    let mut tracker = inventory.finish();

    // Proof that cannot pass the existing durable baseline causes no write.
    assert!(tracker.checkpoints().unwrap().is_empty());
    tracker
        .observe_proof(&assignment, &barrier(&[(status, 12)]))
        .unwrap();
    assert!(tracker.checkpoints().unwrap().is_empty());

    // A real proof advance is emitted. Withholding acknowledgement models a
    // failed durable publication, so the exact checkpoint remains retryable.
    tracker
        .observe_proof(&assignment, &barrier(&[(status, 18)]))
        .unwrap();
    let advanced = tracker.checkpoints().unwrap();
    assert_eq!(advanced.len(), 1);
    assert_eq!(advanced[0].next_offset, 21);
    assert_eq!(tracker.checkpoints().unwrap(), advanced);

    tracker.acknowledge(advanced[0]).unwrap();
    assert!(tracker.checkpoints().unwrap().is_empty());
    tracker
        .observe_proof(&assignment, &barrier(&[(status, 18)]))
        .unwrap();
    assert!(tracker.checkpoints().unwrap().is_empty());

    // Settled source progress beyond the durable baseline is a new advance.
    tracker.update_source_status(source(1, 25)).unwrap();
    let advanced = tracker.checkpoints().unwrap();
    assert_eq!(advanced.len(), 1);
    assert_eq!(advanced[0].next_offset, 26);
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
            None,
            None,
        ),
        Err(SparseTrackerError::WrongAssignment)
    );
    let mut replica = assignment(DefinitionKind::Index, 2);
    replica.rank = 1;
    assert_eq!(
        inventory.record_affected(&replica, status.source_id, 2, None, None, None),
        Err(SparseTrackerError::WrongAssignment)
    );
}
