use std::time::{Duration, Instant};

use anvil_consensus::{ExecutorNomination, NodeId};
use anvil_store::{
    ObjectMutationContext, ProgramPathMutation, ProgramPathStage, ProgramStoreError,
};
use tonic::{Request, Response, Status};

use super::{CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, decode_json, encode_json, wire};
use crate::cluster_placement::ClusterPlacement;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::PlacementKind;

impl ClusterPeerService {
    pub(super) async fn stage_program_path_call(
        &self,
        request: Request<wire::ProgramStagePathRequest>,
    ) -> Result<Response<wire::ProgramStagePathResponse>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let nomination = self.require_program_executor(
            &admitted.placement,
            admitted.authenticated.node_id,
            request.get_ref().executor_nomination_log_index,
        )?;
        let stage: ProgramPathStage = decode_json(&request.get_ref().stage_json)?;
        let group = program_group(&admitted.placement, &stage)?;
        require_program_replica(&group, self.local_node)?;
        let store = self.store.clone();
        let persisted = tokio::time::timeout(admitted.timeout, async move {
            store.persist_program_path_stage(&stage).await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("program path staging deadline exceeded"))?
        .map_err(program_status)?;
        self.require_program_fence(admitted.placement.fence(), nomination)?;
        Ok(Response::new(wire::ProgramStagePathResponse {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            stage_blob_hash: persisted.hash.to_vec(),
            stage_blob_length: persisted.length,
        }))
    }

    pub(super) async fn coordinate_program_path_finalization_call(
        &self,
        request: Request<wire::ProgramCoordinatePathFinalizationRequest>,
    ) -> Result<Response<wire::ProgramCoordinatedPathFinalization>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let nomination = self.require_program_executor(
            &admitted.placement,
            admitted.authenticated.node_id,
            request.get_ref().executor_nomination_log_index,
        )?;
        let stage: ProgramPathStage = decode_json(&request.get_ref().stage_json)?;
        let group = program_group(&admitted.placement, &stage)?;
        if group.coordinator() != self.local_node {
            return Err(Status::failed_precondition(
                "program finalization reached a node that is not the exact-path authority",
            ));
        }
        let store = self.store.clone();
        let commit_cursor = request.get_ref().commit_cursor;
        let deadline = Instant::now()
            .checked_add(admitted.timeout)
            .ok_or_else(|| Status::invalid_argument("program deadline overflowed"))?;
        self.wait_for_program_commit(commit_cursor, &stage, nomination, deadline)
            .await?;
        let context = ObjectMutationContext {
            active_placement_log_id: admitted.placement.fence(),
            // The nomination log index is the atomic executor's Raft fence.
            // Program mutations never borrow the ordinary serving lease.
            serving_fence_term: nomination.nomination_log_index,
        };
        let coordinated = tokio::time::timeout(remaining(deadline)?, async move {
            store
                .coordinate_program_path_finalization(stage, commit_cursor, context)
                .await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("program finalization deadline exceeded"))?
        .map_err(program_status)?;
        self.require_program_fence(admitted.placement.fence(), nomination)?;
        Ok(Response::new(wire::ProgramCoordinatedPathFinalization {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            mutation_json: encode_json(&coordinated.mutation)?,
            replayed: coordinated.replayed,
        }))
    }

    pub(super) async fn apply_program_path_finalization_call(
        &self,
        request: Request<wire::ProgramApplyPathFinalizationRequest>,
    ) -> Result<Response<wire::ProgramPathFinalizationApplied>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let nomination = self.require_program_executor(
            &admitted.placement,
            admitted.authenticated.node_id,
            request.get_ref().executor_nomination_log_index,
        )?;
        let mutation: ProgramPathMutation = decode_json(&request.get_ref().mutation_json)?;
        let group = program_group(&admitted.placement, &mutation.stage)?;
        require_program_replica(&group, self.local_node)?;
        if mutation.stamp.active_placement_log_id != admitted.placement.fence()
            || u64::from(mutation.stamp.source_id.node_id) != group.coordinator().0
        {
            return Err(Status::unavailable(
                "program mutation does not carry its current path authority and placement fence",
            ));
        }
        let deadline = Instant::now()
            .checked_add(admitted.timeout)
            .ok_or_else(|| Status::invalid_argument("program deadline overflowed"))?;
        self.wait_for_program_commit(
            mutation.commit_cursor,
            &mutation.stage,
            nomination,
            deadline,
        )
        .await?;
        let store = self.store.clone();
        let applied = tokio::time::timeout(remaining(deadline)?, async move {
            store
                .apply_program_path_finalization_replica(&mutation)
                .await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("program replica finalization deadline exceeded"))?
        .map_err(program_status)?;
        self.require_program_fence(admitted.placement.fence(), nomination)?;
        Ok(Response::new(wire::ProgramPathFinalizationApplied {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            version: applied.version.0,
            replayed: applied.replayed,
        }))
    }

    fn require_program_executor(
        &self,
        placement: &ClusterPlacement,
        authenticated_source: NodeId,
        nomination_log_index: u64,
    ) -> Result<ExecutorNomination, Status> {
        let nomination = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("atomic executor state is unavailable"))?
            .executor()
            .ok_or_else(|| Status::unavailable("EXECUTOR_MOVED: no executor is nominated"))?;
        if nomination.executor != authenticated_source
            || nomination.nomination_log_index != nomination_log_index
            || !placement.active_node_ids().contains(&nomination.executor)
        {
            return Err(Status::unavailable(
                "EXECUTOR_MOVED: peer is not the current nominated atomic executor",
            ));
        }
        Ok(nomination)
    }

    fn require_program_fence(
        &self,
        expected_placement: anvil_store::PlacementLogId,
        expected_nomination: ExecutorNomination,
    ) -> Result<(), Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("atomic executor state is unavailable"))?;
        let placement = ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if placement.fence() != expected_placement || state.executor() != Some(expected_nomination)
        {
            return Err(Status::unavailable(
                "EXECUTOR_MOVED: placement or atomic executor changed during the operation",
            ));
        }
        Ok(())
    }

    async fn wait_for_program_commit(
        &self,
        commit_cursor: u64,
        stage: &ProgramPathStage,
        nomination: ExecutorNomination,
        deadline: Instant,
    ) -> Result<(), Status> {
        if commit_cursor == 0 {
            return Err(Status::invalid_argument(
                "program commit cursor must be non-zero",
            ));
        }
        loop {
            {
                let state = self
                    .decisions
                    .state()
                    .map_err(|_| Status::unavailable("atomic commit state is unavailable"))?;
                if let Some(invocation) = state.committed_invocation(commit_cursor) {
                    let batch = invocation.committed_batch;
                    if batch.executor != nomination.executor
                        || batch.nomination_log_index != nomination.nomination_log_index
                        || batch.bundle_hash.0 != stage.bundle_hash.0
                        || batch.program_hash.0 != stage.program_hash.0
                    {
                        return Err(Status::data_loss(
                            "program path stage does not match its committed Raft batch",
                        ));
                    }
                    return Ok(());
                }
                if state
                    .last_commit_cursor()
                    .is_some_and(|last| last >= commit_cursor)
                {
                    return Err(Status::data_loss(
                        "program finalization names an unknown committed Raft batch",
                    ));
                }
            }
            tokio::time::sleep(remaining(deadline)?.min(Duration::from_millis(5))).await;
        }
    }
}

fn remaining(deadline: Instant) -> Result<Duration, Status> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(Status::deadline_exceeded(
            "program finalization deadline exceeded",
        ))
    } else {
        Ok(remaining)
    }
}

fn program_group(
    placement: &ClusterPlacement,
    stage: &ProgramPathStage,
) -> Result<MutableRecordReplicaGroup, Status> {
    let mut key = Vec::with_capacity(16 + stage.path.path.len());
    key.extend_from_slice(&stage.tenant_id.to_be_bytes());
    key.extend_from_slice(&stage.bucket_id.to_be_bytes());
    key.extend_from_slice(stage.path.path.as_bytes());
    MutableRecordReplicaGroup::select(
        PlacementKind::Object,
        placement.cluster_id(),
        &key,
        placement.placement_nodes(),
    )
    .ok_or_else(|| Status::unavailable("cluster has no active object metadata owner"))
}

fn require_program_replica(
    group: &MutableRecordReplicaGroup,
    local_node: NodeId,
) -> Result<(), Status> {
    if group.replicas().contains(&local_node) {
        Ok(())
    } else {
        Err(Status::failed_precondition(
            "program path operation reached a node outside its current replica group",
        ))
    }
}

fn program_status(error: ProgramStoreError) -> Status {
    match error {
        ProgramStoreError::ProgramPolicy { .. }
        | ProgramStoreError::PreconditionFailed { .. }
        | ProgramStoreError::Immutable { .. } => {
            Status::failed_precondition(format!("PROGRAM_CONCURRENCY_VIOLATION: {error}"))
        }
        ProgramStoreError::InvalidDefinition(_) | ProgramStoreError::InvalidBundle(_) => {
            Status::invalid_argument(error.to_string())
        }
        ProgramStoreError::PreparedBundleNotFound(_) => Status::not_found(error.to_string()),
        ProgramStoreError::CommitCorruption { .. }
        | ProgramStoreError::PreparedBundleMismatch
        | ProgramStoreError::DurabilityEvidenceMismatch => Status::data_loss(error.to_string()),
        ProgramStoreError::ExecutorLocalDurability
        | ProgramStoreError::OutOfOrderCommit { .. }
        | ProgramStoreError::DurabilityClassMismatch => {
            Status::failed_precondition(error.to_string())
        }
        ProgramStoreError::SourceJournalCapacity => Status::unavailable(error.to_string()),
        ProgramStoreError::ProgramHashMismatch => {
            Status::failed_precondition(format!("PROGRAM_VERSION_MISMATCH: {error}"))
        }
        ProgramStoreError::Storage(_) => Status::internal(error.to_string()),
    }
}
