//! Stable placement hooks used by the cluster object-read facade.

use keldra_store::PlacementLogId;
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
        // The public gateway can be JOINING. The applied placement is the
        // read fence; selected ACTIVE peers independently authenticate and
        // validate that same fence before returning authoritative data.
        let placement = self.placement()?;
        if placement.fence() != expected {
            return Err(Status::unavailable(
                "applied placement changed during the cluster object read",
            ));
        }
        Ok(())
    }
}
