//! Complete-record quorum reads and read repair.

use std::collections::BTreeMap;
use std::time::Duration;

use anvil_consensus::NodeId;
use anvil_store::{
    CurrentObjectSnapshot, MAX_OBJECT_RECORD_EXPORT_RECORDS, ObjectKey, ObjectMutationContext,
    ObjectPathSnapshot, ObjectSnapshotError, PlacementLogId, VersionId,
};
use tonic::Status;

use super::ObjectDistribution;

#[derive(Debug)]
struct ReplicaObservation {
    node: NodeId,
    snapshot: Option<ObjectPathSnapshot>,
}

impl ObjectDistribution {
    /// Waits for the highest program cursor named by one exact-current batch.
    /// FinalizedThrough is monotonic, so proving the maximum proves every
    /// lower cursor in the same batch. `true` tells the caller that it waited
    /// and must reread the complete batch before exposing any descriptor.
    pub(crate) async fn wait_for_program_cursors(
        &self,
        cursors: &[u64],
        budget: Duration,
    ) -> Result<bool, Status> {
        let Some(cursor) = cursors.iter().copied().max() else {
            return Ok(false);
        };
        if crate::programs::program_cursor_is_visible(&self.decisions, cursor)? {
            return Ok(false);
        }
        crate::programs::wait_for_program_cursor(&self.decisions, Some(cursor), budget).await?;
        Ok(true)
    }

    /// The only multi-node object-mutation admission hook. It repairs the
    /// current exact-path state first, then proves that placement, rank-zero
    /// coordination, and the serving fence still match before returning the
    /// context the store needs to evaluate CAS or any other precondition.
    pub(super) async fn reconcile_before_mutation(
        &self,
        key: &ObjectKey,
        expected_fence: PlacementLogId,
    ) -> Result<ObjectMutationContext, Status> {
        let (tenant_id, bucket_id) = self
            .store
            .resolve_bucket_ids(key.tenant(), key.bucket())
            .map_err(super::mutation_status)?;
        self.reconcile_before_mutation_stable(key, tenant_id, bucket_id, expected_fence)
            .await
    }

    pub(super) async fn reconcile_before_mutation_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
        expected_fence: PlacementLogId,
    ) -> Result<ObjectMutationContext, Status> {
        self.reconciled_object_snapshot_stable(key, tenant_id, bucket_id)
            .await?;
        let placement = self.placement()?;
        let group = self.replica_group_stable(&placement, tenant_id, bucket_id, key)?;
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

    /// Reads the selected exact-path replica group, requires quorum agreement
    /// from its fixed 1/1, 2/2, or 2/3 replica set, and repairs every
    /// responding minority before returning the complete selected snapshot.
    ///
    /// This is the cluster read hook for HeadObject, GetObject, BatchGet, and
    /// any internal consumer that needs authoritative current object state.
    pub(crate) async fn reconciled_object_snapshot(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<ObjectPathSnapshot>, Status> {
        let (tenant_id, bucket_id) = self
            .store
            .resolve_bucket_ids(key.tenant(), key.bucket())
            .map_err(super::mutation_status)?;
        self.reconciled_object_snapshot_stable(key, tenant_id, bucket_id)
            .await
    }

    pub(crate) async fn reconciled_object_snapshot_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<ObjectPathSnapshot>, Status> {
        let initial_fence = self.serving.mutation_context()?.active_placement_log_id;
        let placement = self.placement()?;
        if placement.fence() != initial_fence {
            return Err(changed_fence());
        }
        let group = self.replica_group_stable(&placement, tenant_id, bucket_id, key)?;

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

        let selected = select_quorum_snapshot(
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

    /// Selects the authoritative current head and only the descriptor it
    /// names. This is the bounded metadata read used by incremental derived
    /// views: retained history is neither transferred nor decoded.
    pub(crate) async fn reconciled_current_object_snapshot_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<CurrentObjectSnapshot>, Status> {
        let (initial_fence, observations, required, replica_count) = self
            .current_object_observations_stable(key, tenant_id, bucket_id)
            .await?;
        let selected =
            select_current_object_snapshot_quorum(&observations, required, replica_count)?;
        self.require_unchanged_read_fence(initial_fence)?;
        Ok(selected)
    }

    /// Selects a bounded set of exact current descriptors under one placement
    /// fence. Paths are grouped by their complete ranked metadata replica
    /// group, and every responding replica serves its group through one
    /// RocksDB multi-get or one peer batch RPC. Results preserve caller order.
    pub(crate) async fn reconciled_current_object_snapshots_stable(
        &self,
        keys: &[ObjectKey],
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Vec<Option<CurrentObjectSnapshot>>, Status> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if keys.len() > MAX_OBJECT_RECORD_EXPORT_RECORDS as usize {
            return Err(Status::resource_exhausted(format!(
                "current object quorum batch must contain at most {MAX_OBJECT_RECORD_EXPORT_RECORDS} paths"
            )));
        }
        let initial_fence = self.serving.mutation_context()?.active_placement_log_id;
        let placement = self.placement()?;
        if placement.fence() != initial_fence {
            return Err(changed_fence());
        }

        let mut grouped = BTreeMap::<Vec<NodeId>, CurrentObjectBatchGroup>::new();
        for (index, key) in keys.iter().enumerate() {
            let group = self.replica_group_stable(&placement, tenant_id, bucket_id, key)?;
            grouped
                .entry(group.replicas().to_vec())
                .or_insert_with(|| CurrentObjectBatchGroup {
                    replicas: group.replicas().to_vec(),
                    required: group.required_acknowledgements(),
                    entries: Vec::new(),
                })
                .entries
                .push((index, key.path().to_owned()));
        }
        let groups = grouped.into_values().collect::<Vec<_>>();
        let mut observations = vec![Vec::new(); groups.len()];
        let mut reads = tokio::task::JoinSet::new();

        for (group_index, group) in groups.iter().enumerate() {
            let exact_paths = group
                .entries
                .iter()
                .map(|(_, path)| path.clone())
                .collect::<Vec<_>>();
            for node in group.replicas.iter().copied() {
                if node == self.local_node {
                    let store = self.store.clone();
                    let exact_paths = exact_paths.clone();
                    reads.spawn(async move {
                        let result = tokio::task::spawn_blocking(move || {
                            store.export_current_object_snapshots(
                                tenant_id,
                                bucket_id,
                                &exact_paths,
                            )
                        })
                        .await
                        .map_err(|error| {
                            Status::internal(format!(
                                "local current-object batch read task failed: {error}"
                            ))
                        })?
                        .map_err(snapshot_status);
                        Ok::<_, Status>((group_index, node, result))
                    });
                    continue;
                }
                let Some(address) = placement.address(node).cloned() else {
                    tracing::warn!(node_id = node.0, "object replica has no peer address");
                    continue;
                };
                let peers = self.peers.clone();
                let exact_paths = exact_paths.clone();
                reads.spawn(async move {
                    let result = peers
                        .read_current_object_snapshots(
                            node,
                            &address.0,
                            tenant_id,
                            bucket_id,
                            &exact_paths,
                        )
                        .await;
                    Ok::<_, Status>((group_index, node, result))
                });
            }
        }
        while let Some(result) = reads.join_next().await {
            match result {
                Ok(Ok((group_index, _node, Ok(batch)))) => observations[group_index].push(batch),
                Ok(Ok((_group_index, node, Err(error)))) => tracing::warn!(
                    node_id = node.0,
                    %error,
                    "current-object batch replica read failed"
                ),
                Ok(Err(error)) => tracing::warn!(
                    %error,
                    "current-object batch replica read setup failed"
                ),
                Err(error) => tracing::warn!(
                    %error,
                    "current-object batch replica read task failed"
                ),
            }
        }

        let mut selected = vec![None; keys.len()];
        for (group, observations) in groups.iter().zip(&observations) {
            let group_selected = select_current_object_snapshot_batch_quorum(
                observations,
                group.required,
                group.replicas.len(),
                group.entries.len(),
            )?;
            for ((index, exact_path), snapshot) in group.entries.iter().zip(group_selected) {
                if let Some(snapshot) = snapshot.as_ref()
                    && (snapshot.tenant_id != tenant_id
                        || snapshot.bucket_id != bucket_id
                        || snapshot.exact_path != *exact_path)
                {
                    return Err(Status::data_loss(
                        "current object batch quorum returned another object identity",
                    ));
                }
                selected[*index] = Some(snapshot);
            }
        }
        self.require_unchanged_read_fence(initial_fence)?;
        selected
            .into_iter()
            .map(|snapshot| {
                snapshot.ok_or_else(|| {
                    Status::internal("current object batch quorum omitted a requested path")
                })
            })
            .collect()
    }

    /// Guarded derived-view publication is stricter than an ordinary read. The
    /// expected live definition must itself have an exact quorum, and no
    /// successfully read replica may expose a different current candidate.
    /// This prevents a lower quorum from authorizing publication while the
    /// definition coordinator has already durably written a successor and is
    /// still replicating it.
    pub(crate) async fn guarded_current_object_snapshot_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
        expected_version: VersionId,
    ) -> Result<Option<CurrentObjectSnapshot>, Status> {
        let (initial_fence, observations, required, replica_count) = self
            .current_object_observations_stable(key, tenant_id, bucket_id)
            .await?;
        let selected = select_guarded_current_object_snapshot_quorum(
            &observations,
            expected_version,
            required,
            replica_count,
        )?;
        self.require_unchanged_read_fence(initial_fence)?;
        Ok(Some(selected))
    }

    async fn current_object_observations_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<
        (
            PlacementLogId,
            Vec<Option<CurrentObjectSnapshot>>,
            usize,
            usize,
        ),
        Status,
    > {
        let initial_fence = self.serving.mutation_context()?.active_placement_log_id;
        let placement = self.placement()?;
        if placement.fence() != initial_fence {
            return Err(changed_fence());
        }
        let group = self.replica_group_stable(&placement, tenant_id, bucket_id, key)?;

        let mut observations = Vec::with_capacity(group.replicas().len());
        let mut reads = tokio::task::JoinSet::new();
        for node in group.replicas().iter().copied() {
            if node == self.local_node {
                match self
                    .store
                    .export_current_object_snapshot(tenant_id, bucket_id, key.path())
                {
                    Ok(snapshot) => observations.push(snapshot),
                    Err(error) => tracing::warn!(
                        node_id = node.0,
                        %error,
                        "local current-object replica read failed"
                    ),
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
                    .read_current_object_snapshot(
                        node,
                        &address.0,
                        tenant_id,
                        bucket_id,
                        &exact_path,
                    )
                    .await;
                (node, result)
            });
        }
        while let Some(result) = reads.join_next().await {
            match result {
                Ok((_node, Ok(snapshot))) => observations.push(snapshot),
                Ok((node, Err(error))) => tracing::warn!(
                    node_id = node.0,
                    %error,
                    "remote current-object replica read failed"
                ),
                Err(error) => tracing::warn!(%error, "current-object replica read task failed"),
            }
        }

        Ok((
            initial_fence,
            observations,
            group.required_acknowledgements(),
            group.replicas().len(),
        ))
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
                node,
                &address.0,
                placement.fence(),
                tenant_id,
                bucket_id,
                exact_path,
                expected,
                selected,
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

struct CurrentObjectBatchGroup {
    replicas: Vec<NodeId>,
    required: usize,
    entries: Vec<(usize, String)>,
}

fn select_quorum_snapshot(
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

fn select_current_object_snapshot_quorum(
    observations: &[Option<CurrentObjectSnapshot>],
    required: usize,
    replica_count: usize,
) -> Result<Option<CurrentObjectSnapshot>, Status> {
    if required == 0 || required > replica_count || observations.len() < required {
        return Err(Status::unavailable(format!(
            "current object metadata read reached {} of {} required replicas",
            observations.len(),
            required
        )));
    }
    for observation in observations {
        if let Some(snapshot) = observation {
            snapshot.validate().map_err(snapshot_status)?;
        }
        let agreeing = observations
            .iter()
            .filter(|candidate| *candidate == observation)
            .count();
        if agreeing >= required {
            return Ok(observation.clone());
        }
    }

    if required == 2 && replica_count == 2 && observations.len() == 2 {
        if is_current_direct_successor(&observations[0], &observations[1]) {
            return Ok(observations[1].clone());
        }
        if is_current_direct_successor(&observations[1], &observations[0]) {
            return Ok(observations[0].clone());
        }
    }

    Err(Status::unavailable(
        "current object replicas have neither an exact quorum nor one direct predecessor-linked successor",
    ))
}

fn select_current_object_snapshot_batch_quorum(
    observations: &[Vec<Option<CurrentObjectSnapshot>>],
    required: usize,
    replica_count: usize,
    expected_records: usize,
) -> Result<Vec<Option<CurrentObjectSnapshot>>, Status> {
    if observations
        .iter()
        .any(|observation| observation.len() != expected_records)
    {
        return Err(Status::data_loss(
            "current object replica batch returned the wrong result count",
        ));
    }
    let mut selected = Vec::with_capacity(expected_records);
    for index in 0..expected_records {
        let candidates = observations
            .iter()
            .map(|observation| observation[index].clone())
            .collect::<Vec<_>>();
        selected.push(select_current_object_snapshot_quorum(
            &candidates,
            required,
            replica_count,
        )?);
    }
    Ok(selected)
}

fn select_guarded_current_object_snapshot_quorum(
    observations: &[Option<CurrentObjectSnapshot>],
    expected_version: VersionId,
    required: usize,
    replica_count: usize,
) -> Result<CurrentObjectSnapshot, Status> {
    if expected_version.0 == 0 {
        return Err(Status::invalid_argument(
            "guarded definition version must be non-zero",
        ));
    }
    if required == 0 || required > replica_count || observations.len() < required {
        return Err(Status::unavailable(format!(
            "guarded definition read reached {} of {} required replicas",
            observations.len(),
            required
        )));
    }
    for observation in observations.iter().flatten() {
        observation.validate().map_err(snapshot_status)?;
    }
    let Some(expected) = observations
        .iter()
        .flatten()
        .find(|snapshot| {
            !snapshot.head.deleted
                && snapshot.head.version == expected_version
                && snapshot.version.id == expected_version
                && !snapshot.version.deleted
        })
        .cloned()
    else {
        return if observations.iter().any(Option::is_some) {
            Err(Status::failed_precondition(
                "definition changed before guarded artifact publication",
            ))
        } else {
            Err(Status::failed_precondition(
                "definition was deleted before guarded artifact publication",
            ))
        };
    };
    let agreeing = observations
        .iter()
        .filter(|candidate| candidate.as_ref() == Some(&expected))
        .count();
    if agreeing < required {
        return Err(Status::unavailable(
            "guarded definition version has not reached an exact metadata quorum",
        ));
    }
    if observations
        .iter()
        .flatten()
        .any(|candidate| candidate != &expected)
    {
        return Err(Status::unavailable(
            "a conflicting definition candidate is still visible during guarded publication",
        ));
    }
    Ok(expected)
}

/// Pure complete-record selector shared by serving reads and typed ADD
/// handoff. Missing objects are explicit `None` observations.
///
/// Exact quorum agreement always wins. A two-replica group is the one case
/// where a write can durably reach one replica but return an unknown outcome:
/// with both replicas readable, the stamped direct successor is then the only
/// state that can complete that interrupted write. Gaps, siblings, and
/// unrelated object identities remain unavailable rather than being guessed.
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
        if let Some(snapshot) = observation {
            snapshot.validate().map_err(snapshot_status)?;
        }
        let agreeing = observations
            .iter()
            .filter(|candidate| *candidate == observation)
            .count();
        if agreeing >= required {
            return Ok(observation.clone());
        }
    }

    if required == 2 && replica_count == 2 && observations.len() == 2 {
        if is_direct_successor(&observations[0], &observations[1]) {
            return Ok(observations[1].clone());
        }
        if is_direct_successor(&observations[1], &observations[0]) {
            return Ok(observations[0].clone());
        }
    }

    Err(Status::unavailable(
        "object replicas have neither an exact quorum nor one direct predecessor-linked successor",
    ))
}

fn is_direct_successor(
    predecessor: &Option<ObjectPathSnapshot>,
    candidate: &Option<ObjectPathSnapshot>,
) -> bool {
    let Some(candidate) = candidate else {
        return false;
    };
    let Some(stamp) = candidate.head.mutation_stamp else {
        return false;
    };
    match predecessor {
        None => stamp.predecessor_version.is_none(),
        Some(predecessor) => {
            predecessor.tenant_id == candidate.tenant_id
                && predecessor.bucket_id == candidate.bucket_id
                && predecessor.exact_path == candidate.exact_path
                && stamp.predecessor_version == Some(predecessor.head.version)
        }
    }
}

fn is_current_direct_successor(
    predecessor: &Option<CurrentObjectSnapshot>,
    candidate: &Option<CurrentObjectSnapshot>,
) -> bool {
    let Some(candidate) = candidate else {
        return false;
    };
    let Some(stamp) = candidate.head.mutation_stamp else {
        return false;
    };
    match predecessor {
        None => stamp.predecessor_version.is_none(),
        Some(predecessor) => {
            predecessor.tenant_id == candidate.tenant_id
                && predecessor.bucket_id == candidate.bucket_id
                && predecessor.exact_path == candidate.exact_path
                && stamp.predecessor_version == Some(predecessor.head.version)
        }
    }
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
