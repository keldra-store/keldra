//! True-gap definition assignment reconciliation.
//!
//! This is deliberately page-bounded and uses only ordinary definition
//! objects, disposable assignment records, sparse locators, and routed source
//! journals. It creates no additional authority or registry.

use std::collections::BTreeMap;

use anvil_consensus::NodeId;
use anvil_store::{
    DefinitionAssignment, DefinitionAssignmentCursor, DefinitionAssignmentMutation,
    DefinitionCheckpoint, DefinitionDeletion, DefinitionKind, DefinitionOperation, JournalRoute,
    MAX_DEFINITION_STATE_SCAN_RECORDS, Store, WatchJournalStatus,
};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::DataPeerTransport;

use super::{
    ClusterDefinitionLocatorScanner, MAX_INDEX_EVENT_PAGE_BYTES, apply_assignment_page,
    apply_assignment_transfer, assignment_placement, consumer_kind, definition_reference_matches,
    definition_transition, internal_status, join_status, read_destination_checkpoint,
    require_placement,
};

#[derive(Clone)]
struct ReconcileSource {
    node: NodeId,
    address: String,
    captured: WatchJournalStatus,
}

pub(super) async fn reconcile_kind(
    kind: DefinitionKind,
    local_node: NodeId,
    decisions: &anvil_consensus::DecisionRaft,
    store: &Store,
    peers: &DataPeerTransport,
    cluster_peers: &ClusterPeerTransport,
    reader: &ClusterObjectReader,
    placement: &ClusterPlacement,
) -> Result<BTreeMap<u16, u64>, Status> {
    require_placement(decisions, placement.fence())?;
    let sources = capture_sources(local_node, store, peers, placement).await?;
    reconcile_existing_assignments(kind, local_node, decisions, store, peers, reader, placement)
        .await?;
    reconcile_locators(kind, local_node, decisions, store, peers, reader, placement).await?;
    let through = replay_suffixes(
        kind,
        local_node,
        decisions,
        store,
        peers,
        cluster_peers,
        placement,
        &sources,
    )
    .await?;
    advance_local_destination_barrier(
        kind, local_node, store, peers, placement, &sources, &through,
    )
    .await?;
    if kind == DefinitionKind::Accounting {
        super::clear_accounting_matcher_caches(cluster_peers, placement).await?;
    }
    require_placement(decisions, placement.fence())?;
    Ok(through)
}

async fn capture_sources(
    local_node: NodeId,
    store: &Store,
    peers: &DataPeerTransport,
    placement: &ClusterPlacement,
) -> Result<Vec<ReconcileSource>, Status> {
    let mut sources = Vec::new();
    for node in placement.active_node_ids() {
        let address = placement
            .address(node)
            .ok_or_else(|| Status::unavailable("ACTIVE definition source has no address"))?
            .0
            .clone();
        let captured = source_status(local_node, node, &address, store, peers).await?;
        if u64::from(captured.source_id.node_id) != node.0
            || captured.source_id.source_epoch == [0; 32]
            || captured.retention_floor > captured.tail
            || captured.settled_through < captured.retention_floor
            || captured.settled_through > captured.tail
            || captured.retained_entries != captured.tail - captured.retention_floor
        {
            return Err(Status::data_loss(
                "definition source status has invalid identity or bounds",
            ));
        }
        sources.push(ReconcileSource {
            node,
            address,
            captured,
        });
    }
    Ok(sources)
}

async fn source_status(
    local_node: NodeId,
    node: NodeId,
    address: &str,
    store: &Store,
    peers: &DataPeerTransport,
) -> Result<WatchJournalStatus, Status> {
    if node == local_node {
        let store = store.clone();
        tokio::task::spawn_blocking(move || store.local_watch_status())
            .await
            .map_err(join_status)?
            .map_err(internal_status)
    } else {
        peers.source_journal_status(node, address).await
    }
}

async fn reconcile_existing_assignments(
    kind: DefinitionKind,
    local_node: NodeId,
    decisions: &anvil_consensus::DecisionRaft,
    store: &Store,
    peers: &DataPeerTransport,
    reader: &ClusterObjectReader,
    placement: &ClusterPlacement,
) -> Result<(), Status> {
    for destination in placement.active_node_ids() {
        let address = placement
            .address(destination)
            .ok_or_else(|| Status::unavailable("ACTIVE assignment source has no address"))?
            .0
            .clone();
        let mut cursor: Option<DefinitionAssignmentCursor> = None;
        loop {
            let page = if destination == local_node {
                let store = store.clone();
                let cursor = cursor.clone();
                tokio::task::spawn_blocking(move || {
                    store.scan_definition_assignments_by_kind(
                        kind,
                        cursor.as_ref(),
                        MAX_DEFINITION_STATE_SCAN_RECORDS,
                    )
                })
                .await
                .map_err(join_status)?
                .map_err(internal_status)?
            } else {
                peers
                    .scan_definition_assignments_by_kind(
                        destination,
                        &address,
                        kind,
                        cursor.as_ref(),
                        MAX_DEFINITION_STATE_SCAN_RECORDS,
                    )
                    .await?
            };
            let mut corrected = Vec::with_capacity(page.assignments.len());
            for assignment in page.assignments {
                corrected
                    .push(correct_assignment(destination, assignment, reader, placement).await?);
            }
            if !corrected.is_empty() {
                apply_assignment_transfer(
                    local_node,
                    destination,
                    placement,
                    store,
                    peers,
                    &corrected,
                )
                .await?;
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
            require_placement(decisions, placement.fence())?;
        }
    }
    Ok(())
}

async fn correct_assignment(
    destination: NodeId,
    existing: DefinitionAssignment,
    reader: &ClusterObjectReader,
    placement: &ClusterPlacement,
) -> Result<DefinitionAssignmentMutation, Status> {
    existing
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    let live = definition_reference_matches(
        reader,
        existing.tenant_id,
        existing.bucket_id,
        &existing.definition_path,
        existing.object_version,
        DefinitionOperation::Upsert,
    )
    .await?;
    let owners = assignment_placement(
        existing.tenant_id,
        existing.bucket_id,
        existing.definition_id,
        placement,
    )?;
    if live && let Some(rank) = owners.rank_of(destination) {
        return Ok(DefinitionAssignmentMutation::Upsert(DefinitionAssignment {
            observed_fence: placement.fence(),
            rank,
            ..existing
        }));
    }
    Ok(DefinitionAssignmentMutation::Remove {
        kind: existing.kind,
        tenant_id: existing.tenant_id,
        bucket_id: existing.bucket_id,
        definition_id: existing.definition_id,
        object_version: existing.object_version,
        observed_fence: placement.fence(),
    })
}

async fn reconcile_locators(
    kind: DefinitionKind,
    local_node: NodeId,
    decisions: &anvil_consensus::DecisionRaft,
    store: &Store,
    peers: &DataPeerTransport,
    reader: &ClusterObjectReader,
    placement: &ClusterPlacement,
) -> Result<(), Status> {
    let scanner = ClusterDefinitionLocatorScanner::new(
        local_node,
        decisions.clone(),
        store.clone(),
        peers.clone(),
    );
    let mut scan = scanner.begin_kind(kind, placement)?;
    while let Some(page) = scan
        .next_page(MAX_DEFINITION_STATE_SCAN_RECORDS as usize)
        .await?
    {
        let mut by_destination = BTreeMap::<NodeId, Vec<DefinitionAssignmentMutation>>::new();
        for locator in page {
            locator
                .validate()
                .map_err(|error| Status::data_loss(error.to_string()))?;
            if !definition_reference_matches(
                reader,
                locator.tenant_id,
                locator.bucket_id,
                &locator.path,
                locator.object_version,
                locator.operation,
            )
            .await?
            {
                continue;
            }
            let owners = assignment_placement(
                locator.tenant_id,
                locator.bucket_id,
                locator.definition_id,
                placement,
            )?;
            queue_locator_assignments(
                kind,
                &locator,
                &owners,
                placement.fence(),
                &mut by_destination,
            );
        }
        for (destination, mutations) in by_destination {
            apply_assignment_transfer(local_node, destination, placement, store, peers, &mutations)
                .await?;
        }
        require_placement(decisions, placement.fence())?;
    }
    Ok(())
}

fn queue_locator_assignments(
    kind: DefinitionKind,
    locator: &anvil_store::DefinitionLocator,
    owners: &super::IndexPlacement,
    fence: anvil_store::PlacementLogId,
    by_destination: &mut BTreeMap<NodeId, Vec<DefinitionAssignmentMutation>>,
) {
    for (rank, destination) in owners.query_replicas().iter().copied().enumerate() {
        by_destination
            .entry(destination)
            .or_default()
            .push(match locator.operation {
                DefinitionOperation::Upsert => {
                    DefinitionAssignmentMutation::Upsert(DefinitionAssignment {
                        kind,
                        tenant_id: locator.tenant_id,
                        bucket_id: locator.bucket_id,
                        definition_id: locator.definition_id,
                        definition_path: locator.path.clone(),
                        object_version: locator.object_version,
                        observed_fence: fence,
                        rank: rank as u8,
                    })
                }
                DefinitionOperation::Delete => {
                    DefinitionAssignmentMutation::Delete(DefinitionDeletion {
                        kind,
                        tenant_id: locator.tenant_id,
                        bucket_id: locator.bucket_id,
                        definition_id: locator.definition_id,
                        definition_path: locator.path.clone(),
                        object_version: locator.object_version,
                        observed_fence: fence,
                        rank: rank as u8,
                    })
                }
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_runtime::placement::IndexPlacement;
    use crate::index_service::definition_path;
    use anvil_store::{DefinitionLocator, StoreOptions, VersionId};

    #[tokio::test]
    async fn membership_change_repairs_an_unassigned_locator_on_the_new_top_three() {
        let previous_fence = anvil_store::PlacementLogId { term: 2, index: 8 };
        let current_fence = anvil_store::PlacementLogId { term: 2, index: 9 };
        assert_ne!(previous_fence, current_fence);

        let roots = [
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        ];
        let mut stores = BTreeMap::new();
        for (node, root) in [NodeId(3), NodeId(1), NodeId(2)]
            .into_iter()
            .zip(roots.iter())
        {
            stores.insert(
                node,
                Store::open(StoreOptions::new(root.path(), node.0 as u16))
                    .await
                    .unwrap(),
            );
        }
        let locator = DefinitionLocator {
            kind: DefinitionKind::Index,
            tenant_id: 11,
            bucket_id: 12,
            definition_id: 13,
            path: definition_path("example").unwrap(),
            object_version: VersionId(14),
            operation: DefinitionOperation::Upsert,
        };
        for store in stores.values() {
            assert!(
                store
                    .definition_assignment(locator.kind, 11, 12, 13)
                    .unwrap()
                    .is_none()
            );
        }

        let owners = IndexPlacement::from_ranked(
            vec![NodeId(3), NodeId(1), NodeId(2), NodeId(4)],
            current_fence,
        )
        .unwrap();
        let mut by_destination = BTreeMap::new();
        queue_locator_assignments(
            locator.kind,
            &locator,
            &owners,
            current_fence,
            &mut by_destination,
        );
        assert_eq!(
            by_destination.keys().copied().collect::<Vec<_>>(),
            [NodeId(1), NodeId(2), NodeId(3)]
        );
        assert!(!by_destination.contains_key(&NodeId(4)));
        for (destination, mutations) in by_destination {
            stores[&destination]
                .apply_definition_assignment_mutations(&mutations)
                .unwrap();
        }

        for (rank, destination) in owners.query_replicas().iter().copied().enumerate() {
            let assignment = stores[&destination]
                .definition_assignment(locator.kind, 11, 12, 13)
                .unwrap()
                .unwrap();
            assert_eq!(assignment.rank, rank as u8);
            assert_eq!(assignment.observed_fence, current_fence);
            assert_eq!(assignment.object_version, locator.object_version);
        }
        assert_eq!(owners.builder(), NodeId(3));
        assert_eq!(
            stores[&owners.builder()]
                .definition_assignment(locator.kind, 11, 12, 13)
                .unwrap()
                .unwrap()
                .rank,
            0
        );
    }

    #[test]
    fn membership_reconciliation_rehydrates_deleted_definition_cleanup_on_current_owners() {
        let fence = anvil_store::PlacementLogId { term: 3, index: 9 };
        let locator = DefinitionLocator {
            kind: DefinitionKind::Index,
            tenant_id: 11,
            bucket_id: 12,
            definition_id: 13,
            path: definition_path("deleted").unwrap(),
            object_version: VersionId(15),
            operation: DefinitionOperation::Delete,
        };
        let owners =
            IndexPlacement::from_ranked(vec![NodeId(3), NodeId(1), NodeId(2), NodeId(4)], fence)
                .unwrap();
        let mut by_destination = BTreeMap::new();
        queue_locator_assignments(locator.kind, &locator, &owners, fence, &mut by_destination);
        assert_eq!(by_destination.len(), 3);
        for (rank, destination) in owners.query_replicas().iter().copied().enumerate() {
            assert!(matches!(
                by_destination[&destination].as_slice(),
                [DefinitionAssignmentMutation::Delete(DefinitionDeletion {
                    definition_id: 13,
                    object_version: VersionId(15),
                    rank: actual_rank,
                    ..
                })] if *actual_rank == rank as u8
            ));
        }
    }
}

async fn replay_suffixes(
    kind: DefinitionKind,
    local_node: NodeId,
    decisions: &anvil_consensus::DecisionRaft,
    store: &Store,
    peers: &DataPeerTransport,
    cluster_peers: &ClusterPeerTransport,
    placement: &ClusterPlacement,
    sources: &[ReconcileSource],
) -> Result<BTreeMap<u16, u64>, Status> {
    let mut through = BTreeMap::new();
    for source in sources {
        let latest = source_status(local_node, source.node, &source.address, store, peers).await?;
        if latest.source_id != source.captured.source_id
            || source.captured.settled_through < latest.retention_floor
        {
            return Err(Status::out_of_range(
                "definition reconciliation suffix expired or changed epoch",
            ));
        }
        let mut after = source.captured.settled_through;
        let target = latest.settled_through;
        let mut destination_next = BTreeMap::new();
        while after < target {
            let page = if source.node == local_node {
                let store = store.clone();
                let source_id = latest.source_id;
                tokio::task::spawn_blocking(move || {
                    store.scan_routed_local_changes(
                        JournalRoute::Definition(kind),
                        source_id,
                        after,
                        target,
                        MAX_DEFINITION_STATE_SCAN_RECORDS as usize,
                        MAX_INDEX_EVENT_PAGE_BYTES,
                    )
                })
                .await
                .map_err(join_status)?
                .map_err(internal_status)?
            } else {
                peers
                    .read_routed_source_journal(
                        source.node,
                        &source.address,
                        JournalRoute::Definition(kind),
                        latest.source_id,
                        after,
                        target,
                        MAX_DEFINITION_STATE_SCAN_RECORDS as usize,
                        MAX_INDEX_EVENT_PAGE_BYTES,
                    )
                    .await?
            };
            if page.source_id != latest.source_id
                || page.through_offset <= after
                || page.through_offset > target
                || page.oversize.is_some()
            {
                return Err(Status::data_loss(
                    "definition reconciliation suffix returned invalid advancement",
                ));
            }
            let mut cursorless = BTreeMap::<NodeId, Vec<DefinitionAssignmentMutation>>::new();
            for change in &page.changes {
                let transition = definition_transition(kind, change)?;
                if source.node == local_node {
                    super::deliver_transition(
                        local_node,
                        placement,
                        store,
                        peers,
                        cluster_peers,
                        latest.source_id,
                        change.offset(),
                        transition,
                        &mut destination_next,
                    )
                    .await?;
                } else {
                    queue_cursorless_transition(transition, placement, &mut cursorless)?;
                }
            }
            for (destination, mutations) in cursorless {
                apply_assignment_transfer(
                    local_node,
                    destination,
                    placement,
                    store,
                    peers,
                    &mutations,
                )
                .await?;
            }
            after = page.through_offset;
            require_placement(decisions, placement.fence())?;
        }
        through.insert(latest.source_id.node_id, target);
    }
    Ok(through)
}

fn queue_cursorless_transition(
    transition: &anvil_store::DefinitionTransition,
    placement: &ClusterPlacement,
    by_destination: &mut BTreeMap<NodeId, Vec<DefinitionAssignmentMutation>>,
) -> Result<(), Status> {
    let owners = assignment_placement(
        transition.tenant_id,
        transition.bucket_id,
        transition.definition_id,
        placement,
    )?;
    for (rank, destination) in owners.query_replicas().iter().copied().enumerate() {
        by_destination
            .entry(destination)
            .or_default()
            .push(super::transition_mutation(
                transition,
                placement.fence(),
                rank as u8,
            ));
    }
    Ok(())
}

async fn advance_local_destination_barrier(
    kind: DefinitionKind,
    local_node: NodeId,
    store: &Store,
    peers: &DataPeerTransport,
    placement: &ClusterPlacement,
    sources: &[ReconcileSource],
    through: &BTreeMap<u16, u64>,
) -> Result<(), Status> {
    let consumer_kind = consumer_kind(kind);
    for source in sources {
        // A destination checkpoint is owned by its authenticated journal
        // source. This reconciler may cursorlessly replay another source's
        // bounded suffix, but must leave that source to advance its own
        // checkpoint through normal delivery.
        if source.node != local_node {
            continue;
        }
        let target_next = through
            .get(&source.captured.source_id.node_id)
            .copied()
            .ok_or_else(|| Status::data_loss("definition barrier omitted an ACTIVE source"))?
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("definition barrier exhausted"))?;
        for destination in placement.active_node_ids() {
            let existing = read_destination_checkpoint(
                local_node,
                destination,
                placement,
                store,
                peers,
                consumer_kind,
                source.captured.source_id,
            )
            .await?;
            let same_source = existing.filter(|value| value.source_id == source.captured.source_id);
            if destination_barrier_is_current(same_source, placement.fence(), target_next) {
                continue;
            }
            apply_assignment_page(
                local_node,
                destination,
                placement,
                store,
                peers,
                &[],
                DefinitionCheckpoint {
                    consumer_kind,
                    source_id: source.captured.source_id,
                    next_offset: target_next,
                    observed_fence: placement.fence(),
                },
            )
            .await?;
        }
    }
    Ok(())
}

fn destination_barrier_is_current(
    checkpoint: Option<DefinitionCheckpoint>,
    fence: anvil_store::PlacementLogId,
    target_next: u64,
) -> bool {
    checkpoint.is_some_and(|checkpoint| {
        checkpoint.observed_fence == fence && checkpoint.next_offset >= target_next
    })
}

#[cfg(test)]
mod destination_barrier_tests {
    use anvil_store::{DefinitionConsumerKind, SourceId};

    use super::*;

    fn checkpoint(fence: anvil_store::PlacementLogId, next_offset: u64) -> DefinitionCheckpoint {
        DefinitionCheckpoint {
            consumer_kind: DefinitionConsumerKind::IndexAssignments,
            source_id: SourceId {
                node_id: 1,
                source_epoch: [1; 32],
            },
            next_offset,
            observed_fence: fence,
        }
    }

    #[test]
    fn an_old_fence_offset_never_skips_current_fence_baseline_installation() {
        let current = anvil_store::PlacementLogId { term: 3, index: 9 };
        assert!(!destination_barrier_is_current(
            Some(checkpoint(
                anvil_store::PlacementLogId { term: 3, index: 8 },
                1_000,
            )),
            current,
            10,
        ));
        assert!(!destination_barrier_is_current(
            Some(checkpoint(current, 9)),
            current,
            10,
        ));
        assert!(destination_barrier_is_current(
            Some(checkpoint(current, 10)),
            current,
            10,
        ));
    }
}
