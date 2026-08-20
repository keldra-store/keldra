use keldra_consensus::{DecisionRaft, NodeId};
use keldra_store::PlacementLogId;
use tonic::Status;

use crate::cluster_placement::ClusterPlacement;
use crate::placement::PlacementKind;

use super::model::GroupScope;

#[derive(Clone, Debug)]
pub(super) struct PersonalDbPrimary {
    pub(super) node_id: NodeId,
    pub(super) address: Option<String>,
    pub(super) fence: PlacementLogId,
}

#[derive(Clone)]
pub(super) struct PersonalDbPlacement {
    local_node: NodeId,
    decisions: DecisionRaft,
}

impl PersonalDbPlacement {
    pub(super) fn new(local_node: NodeId, decisions: DecisionRaft) -> Self {
        Self {
            local_node,
            decisions,
        }
    }

    pub(super) fn primary(&self, scope: &GroupScope) -> Result<PersonalDbPrimary, Status> {
        let placement = self.current()?;
        let node_id = placement
            .rank(PlacementKind::FuturePersonalDb, &scope.placement_key())
            .into_iter()
            .next()
            .ok_or_else(|| Status::unavailable("cluster has no PersonalDB primary"))?;
        let address = (node_id != self.local_node)
            .then(|| {
                placement
                    .address(node_id)
                    .map(|address| address.0.clone())
                    .ok_or_else(|| Status::unavailable("PersonalDB primary has no peer address"))
            })
            .transpose()?;
        Ok(PersonalDbPrimary {
            node_id,
            address,
            fence: placement.fence(),
        })
    }

    pub(super) fn require_local_primary(
        &self,
        scope: &GroupScope,
        expected: PlacementLogId,
    ) -> Result<(), Status> {
        let primary = self.primary(scope)?;
        if primary.node_id != self.local_node || primary.fence != expected {
            return Err(Status::failed_precondition(
                "PersonalDB mutation did not reach its current group primary",
            ));
        }
        Ok(())
    }

    pub(super) fn require_unchanged(&self, expected: PlacementLogId) -> Result<(), Status> {
        if self.current()?.fence() == expected {
            Ok(())
        } else {
            Err(Status::unavailable(
                "active placement changed during the PersonalDB operation",
            ))
        }
    }

    fn current(&self) -> Result<ClusterPlacement, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}
