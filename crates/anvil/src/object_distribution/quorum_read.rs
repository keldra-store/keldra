//! Exact complete-record quorum reads and read repair.

use anvil_consensus::NodeId;
use anvil_store::{
    ObjectKey, ObjectMutationContext, ObjectPathSnapshot, ObjectSnapshotError, PlacementLogId,
};
use tonic::Status;

use super::ObjectDistribution;

#[derive(Debug)]
struct ReplicaObservation {
    node: NodeId,
    snapshot: Option<ObjectPathSnapshot>,
}

impl ObjectDistribution {
    /// The only multi-node object-mutation admission hook. It repairs the
    /// current exact-path state first, then proves that placement, rank-zero
    /// coordination, and the serving fence still match before returning the
    /// context the store needs to evaluate CAS or any other precondition.
    pub(super) async fn reconcile_before_mutation(
        &self,
        key: &ObjectKey,
        expected_fence: PlacementLogId,
    ) -> Result<ObjectMutationContext, Status> {
        self.reconciled_object_snapshot(key).await?;
        let placement = self.placement()?;
        let group = self.replica_group(&placement, key)?;
        if placement.fence() != expected_fence || group.coordinator() != self.local_node {
            return Err(Status::unavailable(
                "object placement changed while reconciling its mutation state",
            ));
        }
        let context = self.serving.mutation_context()?;
        if context.active_placement_log_id != expected_fence {
            return Err(changed_fence());
        }
        Ok(context)
    }

    /// Reads the selected exact-path replica group, requires exact agreement
    /// from its fixed 1/1, 2/2, or 2/3 quorum, and repairs every responding
    /// minority before returning the complete selected snapshot.
    ///
    /// This is the cluster read hook for HeadObject, GetObject, BatchGet, and
    /// any internal consumer that needs authoritative current object state.
    pub(crate) async fn reconciled_object_snapshot(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<ObjectPathSnapshot>, Status> {
        let initial_fence = self.serving.mutation_context()?.active_placement_log_id;
        let placement = self.placement()?;
        if placement.fence() != initial_fence {
            return Err(changed_fence());
        }
        let (tenant_id, bucket_id) = self
            .store
            .resolve_bucket_ids(key.tenant(), key.bucket())
            .map_err(super::mutation_status)?;
        let group = self.replica_group(&placement, key)?;

        let mut observations = Vec::with_capacity(group.replicas().len());
        let mut reads = tokio::task::JoinSet::new();
        for node in group.replicas().iter().copied() {
            if node == self.local_node {
                match self
                    .store
                    .export_object_path_record(tenant_id, bucket_id, key.path())
                {
                    Ok(snapshot) => observations.push(ReplicaObservation { node, snapshot }),
                    Err(error) => {
                        tracing::warn!(node_id = node.0, %error, "local object replica read failed")
                    }
                }
                continue;
            }
            let Some(address) = placement.address(node).cloned() else {
                tracing::warn!(node_id = node.0, "object replica has no peer address");
                continue;
            };
            let peers = self.peers.clone();
            let exact_path = key.path().to_owned();
            reads.spawn(async move {
                let result = peers
                    .read_object_path_snapshot(node, &address.0, tenant_id, bucket_id, &exact_path)
                    .await;
                (node, result)
            });
        }
        while let Some(result) = reads.join_next().await {
            match result {
                Ok((node, Ok(snapshot))) => {
                    observations.push(ReplicaObservation { node, snapshot });
                }
                Ok((node, Err(error))) => {
                    tracing::warn!(node_id = node.0, %error, "remote object replica read failed");
                }
                Err(error) => tracing::warn!(%error, "object replica read task failed"),
            }
        }

        let selected = select_exact_quorum(
            &observations,
            group.required_acknowledgements(),
            group.replicas().len(),
        )?;
        for observation in observations
            .iter()
            .filter(|observation| observation.snapshot != selected)
        {
            self.repair_observation(
                &placement,
                observation.node,
                tenant_id,
                bucket_id,
                key.path(),
                observation.snapshot.as_ref(),
                selected.as_ref(),
            )
            .await?;
        }

        self.require_unchanged_read_fence(initial_fence)?;
        Ok(selected)
    }

    async fn repair_observation(
        &self,
        placement: &crate::cluster_placement::ClusterPlacement,
        node: NodeId,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
        expected: Option<&ObjectPathSnapshot>,
        selected: Option<&ObjectPathSnapshot>,
    ) -> Result<(), Status> {
        if node == self.local_node {
            self.store
                .repair_object_path_snapshot(tenant_id, bucket_id, exact_path, expected, selected)
                .await
                .map_err(snapshot_status)?;
            return Ok(());
        }
        let address = placement.address(node).ok_or_else(|| {
            Status::unavailable(format!(
                "ACTIVE object replica {} has no peer address",
                node.0
            ))
        })?;
        self.peers
            .repair_object_path_snapshot(
                node, &address.0, tenant_id, bucket_id, exact_path, expected, selected,
            )
            .await?;
        Ok(())
    }

    fn require_unchanged_read_fence(&self, initial: PlacementLogId) -> Result<(), Status> {
        let current = self.serving.mutation_context()?.active_placement_log_id;
        let placement = self.placement()?;
        if current != initial || placement.fence() != initial {
            return Err(changed_fence());
        }
        Ok(())
    }
}

fn select_exact_quorum(
    observations: &[ReplicaObservation],
    required: usize,
    replica_count: usize,
) -> Result<Option<ObjectPathSnapshot>, Status> {
    let snapshots = observations
        .iter()
        .map(|observation| observation.snapshot.clone())
        .collect::<Vec<_>>();
    select_object_snapshot_quorum(&snapshots, required, replica_count)
}

/// Pure complete-record selector shared by serving reads and typed ADD
/// handoff. Missing replicas are explicit `None` observations.
pub(crate) fn select_object_snapshot_quorum(
    observations: &[Option<ObjectPathSnapshot>],
    required: usize,
    replica_count: usize,
) -> Result<Option<ObjectPathSnapshot>, Status> {
    if required == 0 || required > replica_count || observations.len() < required {
        return Err(Status::unavailable(format!(
            "object metadata read reached {} of {} required replicas",
            observations.len(),
            required
        )));
    }
    for observation in observations {
        let agreeing = observations
            .iter()
            .filter(|candidate| *candidate == observation)
            .count();
        if agreeing >= required {
            return Ok(observation.clone());
        }
    }
    Err(Status::unavailable(
        "object replicas have no exact complete-record quorum",
    ))
}

fn changed_fence() -> Status {
    Status::unavailable("serving fence changed during the object quorum read")
}

fn snapshot_status(error: ObjectSnapshotError) -> Status {
    match error {
        ObjectSnapshotError::InvalidCursor
        | ObjectSnapshotError::InvalidExportLimit(_)
        | ObjectSnapshotError::InvalidRecord(_)
        | ObjectSnapshotError::SnapshotConflict => Status::data_loss(error.to_string()),
        ObjectSnapshotError::RepairPreconditionFailed => Status::unavailable(error.to_string()),
        ObjectSnapshotError::ExportRecordTooLarge { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        ObjectSnapshotError::Storage(_) => Status::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests;
