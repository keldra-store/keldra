//! Checkpoint-only advancement for sparse definition delivery.

use std::collections::BTreeMap;

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{DefinitionCheckpoint, DefinitionKind, PlacementLogId, SourceId, Store};
use tonic::Status;

use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::DataPeerTransport;

#[derive(Default)]
pub(super) struct DeliveryProgress {
    pub(super) source: Option<SourceId>,
    pub(super) after_offset: u64,
    pub(super) fence: Option<PlacementLogId>,
    pub(super) destination_next: BTreeMap<NodeId, u64>,
}

impl DeliveryProgress {
    pub(super) fn reset_required(
        &self,
        source: SourceId,
        fence: PlacementLogId,
        retention_floor: u64,
    ) -> bool {
        self.source != Some(source)
            || self.fence != Some(fence)
            || self.after_offset < retention_floor
    }
}

/// Requires the current membership reconciliation to have reached this local
/// assignment destination before normal source delivery can advance it.
///
/// The stable lowest ACTIVE node performs the one cluster-wide reconciliation.
/// For each definition kind, its source checkpoint is written to every ACTIVE
/// destination only after the existing-assignment and live-locator inventories
/// have been reconciled. Reusing that bounded checkpoint avoids another marker,
/// protocol, or authority.
pub(super) async fn require_membership_assignment_baseline(
    kind: DefinitionKind,
    placement: &ClusterPlacement,
    store: &Store,
) -> Result<(), Status> {
    let coordinator = placement
        .active_node_ids()
        .into_iter()
        .min()
        .ok_or_else(|| {
            Status::unavailable("definition reconciliation has no ACTIVE coordinator")
        })?;
    let source_node_id = u16::try_from(coordinator.0)
        .map_err(|_| Status::data_loss("definition reconciliation coordinator ID is invalid"))?;
    let consumer_kind = super::consumer_kind(kind);
    let store = store.clone();
    let checkpoint = tokio::task::spawn_blocking(move || {
        store.definition_checkpoint(consumer_kind, source_node_id)
    })
    .await
    .map_err(super::join_status)?
    .map_err(super::internal_status)?;
    validate_membership_assignment_baseline(
        checkpoint,
        consumer_kind,
        source_node_id,
        placement.fence(),
    )
}

fn validate_membership_assignment_baseline(
    checkpoint: Option<DefinitionCheckpoint>,
    consumer_kind: anvil_store::DefinitionConsumerKind,
    source_node_id: u16,
    fence: PlacementLogId,
) -> Result<(), Status> {
    let Some(checkpoint) = checkpoint else {
        return Err(Status::unavailable(
            "membership assignment reconciliation has not reached this node",
        ));
    };
    if checkpoint.consumer_kind != consumer_kind || checkpoint.source_id.node_id != source_node_id {
        return Err(Status::data_loss(
            "membership assignment reconciliation checkpoint has invalid identity",
        ));
    }
    if checkpoint.observed_fence == fence {
        return Ok(());
    }
    if super::fence_after(checkpoint.observed_fence, fence) {
        return Err(Status::data_loss(
            "membership assignment reconciliation checkpoint is from a future placement",
        ));
    }
    Err(Status::unavailable(
        "membership assignment reconciliation has not reached the current placement",
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn commit_delivery_progress(
    kind: DefinitionKind,
    local_node: NodeId,
    decisions: &DecisionRaft,
    placement: &ClusterPlacement,
    store: &Store,
    peers: &DataPeerTransport,
    source: SourceId,
    through_offset: u64,
    progress: &mut DeliveryProgress,
) -> Result<(), Status> {
    let mut destination_next = BTreeMap::new();
    advance_assignment_checkpoints(
        kind,
        local_node,
        placement,
        store,
        peers,
        source,
        through_offset,
        &mut destination_next,
    )
    .await?;
    super::require_placement(decisions, placement.fence())?;
    super::persist_delivery_checkpoint(kind, store, source, through_offset, placement.fence())
        .await?;

    progress.source = Some(source);
    progress.after_offset = through_offset;
    progress.fence = Some(placement.fence());
    progress.destination_next = destination_next;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn advance_assignment_checkpoints(
    kind: DefinitionKind,
    local_node: NodeId,
    placement: &ClusterPlacement,
    store: &Store,
    peers: &DataPeerTransport,
    source: SourceId,
    through_offset: u64,
    destination_next: &mut BTreeMap<NodeId, u64>,
) -> Result<(), Status> {
    let checkpoint = DefinitionCheckpoint {
        consumer_kind: super::consumer_kind(kind),
        source_id: source,
        next_offset: through_offset
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("definition source offset exhausted"))?,
        observed_fence: placement.fence(),
    };

    for destination in destinations_requiring_checkpoint(
        placement.active_node_ids(),
        destination_next,
        checkpoint.next_offset,
    ) {
        let observed = match destination_next.get(&destination).copied() {
            Some(next) => Some((placement.fence(), next)),
            None => super::read_destination_checkpoint(
                local_node,
                destination,
                placement,
                store,
                peers,
                checkpoint.consumer_kind,
                source,
            )
            .await?
            .map(|current| (current.observed_fence, current.next_offset)),
        };
        let next = if observed.is_some_and(|(fence, next)| {
            fence == checkpoint.observed_fence && next >= checkpoint.next_offset
        }) {
            observed.expect("a satisfying checkpoint was observed").1
        } else {
            super::apply_assignment_page(
                local_node,
                destination,
                placement,
                store,
                peers,
                &[],
                checkpoint,
            )
            .await?;
            checkpoint.next_offset
        };
        destination_next.insert(destination, next);
    }
    Ok(())
}

fn destinations_requiring_checkpoint(
    active_nodes: impl IntoIterator<Item = NodeId>,
    destination_next: &BTreeMap<NodeId, u64>,
    required_next: u64,
) -> Vec<NodeId> {
    active_nodes
        .into_iter()
        .filter(|destination| {
            destination_next
                .get(destination)
                .is_none_or(|next| *next < required_next)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assignment_checkpoint(source_node_id: u16, fence: PlacementLogId) -> DefinitionCheckpoint {
        DefinitionCheckpoint {
            consumer_kind: anvil_store::DefinitionConsumerKind::IndexAssignments,
            source_id: SourceId {
                node_id: source_node_id,
                source_epoch: [source_node_id as u8; 32],
            },
            next_offset: 1,
            observed_fence: fence,
        }
    }

    #[test]
    fn only_the_current_reconciliation_fence_releases_normal_delivery() {
        let current = PlacementLogId { term: 2, index: 4 };
        let kind = anvil_store::DefinitionConsumerKind::IndexAssignments;
        assert!(
            validate_membership_assignment_baseline(
                Some(assignment_checkpoint(1, current)),
                kind,
                1,
                current,
            )
            .is_ok()
        );
        assert_eq!(
            validate_membership_assignment_baseline(None, kind, 1, current)
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            validate_membership_assignment_baseline(
                Some(assignment_checkpoint(
                    1,
                    PlacementLogId { term: 2, index: 3 },
                )),
                kind,
                1,
                current,
            )
            .unwrap_err()
            .code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            validate_membership_assignment_baseline(
                Some(assignment_checkpoint(
                    1,
                    PlacementLogId { term: 2, index: 5 },
                )),
                kind,
                1,
                current,
            )
            .unwrap_err()
            .code(),
            tonic::Code::DataLoss
        );
    }

    #[test]
    fn another_source_or_consumer_cannot_release_normal_delivery() {
        let current = PlacementLogId { term: 2, index: 4 };
        let index = anvil_store::DefinitionConsumerKind::IndexAssignments;
        assert_eq!(
            validate_membership_assignment_baseline(
                Some(assignment_checkpoint(2, current)),
                index,
                1,
                current,
            )
            .unwrap_err()
            .code(),
            tonic::Code::DataLoss
        );
        assert_eq!(
            validate_membership_assignment_baseline(
                Some(DefinitionCheckpoint {
                    consumer_kind: anvil_store::DefinitionConsumerKind::AccountingAssignments,
                    ..assignment_checkpoint(1, current)
                }),
                index,
                1,
                current,
            )
            .unwrap_err()
            .code(),
            tonic::Code::DataLoss
        );
    }

    #[test]
    fn checkpoint_fanout_includes_every_active_destination_without_current_proof() {
        let next = BTreeMap::from([(NodeId(1), 12), (NodeId(2), 11)]);
        assert_eq!(
            destinations_requiring_checkpoint(
                [NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
                &next,
                12,
            ),
            [NodeId(2), NodeId(3), NodeId(4)]
        );
    }

    #[test]
    fn checkpoint_fanout_is_idempotent_after_every_destination_reaches_the_barrier() {
        let next = BTreeMap::from([(NodeId(1), 12), (NodeId(2), 13), (NodeId(3), 12)]);
        assert!(
            destinations_requiring_checkpoint([NodeId(1), NodeId(2), NodeId(3)], &next, 12)
                .is_empty()
        );
    }

    #[test]
    fn checkpoint_fanout_at_zero_still_establishes_every_new_fence_destination() {
        assert_eq!(
            destinations_requiring_checkpoint(
                [NodeId(1), NodeId(2), NodeId(3)],
                &BTreeMap::new(),
                1,
            ),
            [NodeId(1), NodeId(2), NodeId(3)]
        );
    }

    #[test]
    fn an_old_fence_checkpoint_cannot_satisfy_a_new_fence_barrier() {
        let required = PlacementLogId { term: 2, index: 4 };
        let observed = Some((PlacementLogId { term: 2, index: 3 }, 99));
        assert!(!observed.is_some_and(|(fence, next)| fence == required && next >= 10));
    }

    #[test]
    fn source_fence_and_floor_changes_require_epoch_initialization() {
        let source = SourceId {
            node_id: 1,
            source_epoch: [1; 32],
        };
        let fence = PlacementLogId { term: 2, index: 3 };
        let current = DeliveryProgress {
            source: Some(source),
            after_offset: 10,
            fence: Some(fence),
            destination_next: BTreeMap::new(),
        };
        assert!(!current.reset_required(source, fence, 10));
        assert!(current.reset_required(source, PlacementLogId { term: 2, index: 4 }, 10));
        assert!(current.reset_required(source, fence, 11));
        let mut replacement = source;
        replacement.source_epoch[0] = 2;
        assert!(current.reset_required(replacement, fence, 10));
    }
}
