//! Stable placement hooks used by the cluster object-read facade.

use anvil_store::PlacementLogId;
use tonic::Status;

use super::ObjectDistribution;
use crate::cluster_placement::ClusterPlacement;

impl ObjectDistribution {
    pub(crate) fn current_read_placement(&self) -> Result<ClusterPlacement, Status> {
        self.placement()
    }

    pub(crate) fn require_current_read_fence(
        &self,
        expected: PlacementLogId,
    ) -> Result<(), Status> {
        let current = self.serving.mutation_context()?.active_placement_log_id;
        let placement = self.placement()?;
        if current != expected || placement.fence() != expected {
            return Err(Status::unavailable(
                "serving fence changed during the cluster object read",
            ));
        }
        Ok(())
    }
}
