use std::time::{Duration, Instant};

use keldra_consensus::{CommittedInvocation, ExecutorNomination, NodeId, PreparedBatch};
use keldra_store::{
    ObjectMutationContext, PlacementLogId, ProgramAliasRegistryMutation, ProgramAliasRegistryStage,
    ProgramBundleAuthority, ProgramPathMutation, ProgramPathStage, ProgramReservation,
    ProgramReservationState, ProgramStoreError,
};
use tonic::{Request, Response, Status};

use super::{CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, decode_json, encode_json, wire};
use crate::cluster_placement::ClusterPlacement;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::PlacementKind;

impl ClusterPeerService {
    pub(super) async fn reserve_program_participant_call(
        &self,
        request: Request<wire::ProgramReserveParticipantRequest>,
    ) -> Result<Response<wire::ProgramParticipantReservationApplied>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let nomination = self.require_program_executor(
            &admitted.placement,
            admitted.authenticated.node_id,
            request.get_ref().executor_nomination_log_index,
        )?;
        let reservation: ProgramReservation = decode_json(&request.get_ref().reservation_json)?;
        self.require_reservation_authority(
            &admitted.placement,
            nomination,
            &reservation,
            ReservationOperation::Reserve,
        )?;
        let group = reservation_group(&admitted.placement, &reservation)?;
        require_program_replica(&group, self.local_node)?;
        let store = self.store.clone();
        tokio::time::timeout(admitted.timeout, async move {
            store.reserve_program_participant(&reservation).await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("program reservation deadline exceeded"))?
        .map_err(program_mutation_status)?;
        self.require_program_fence(admitted.placement.fence(), nomination)?;
        Ok(Response::new(wire::ProgramParticipantReservationApplied {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        }))
    }

    pub(super) async fn commit_program_participant_call(
        &self,
        request: Request<wire::ProgramCommitParticipantRequest>,
    ) -> Result<Response<wire::ProgramParticipantReservationApplied>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let nomination = self.require_program_executor(
            &admitted.placement,
            admitted.authenticated.node_id,
            request.get_ref().executor_nomination_log_index,
        )?;
        let reservation: ProgramReservation = decode_json(&request.get_ref().reservation_json)?;
        let commit_cursor = request.get_ref().commit_cursor;
        self.require_reservation_authority(
            &admitted.placement,
            nomination,
            &reservation,
            ReservationOperation::Commit { commit_cursor },
        )?;
        let group = reservation_group(&admitted.placement, &reservation)?;
        require_program_replica(&group, self.local_node)?;
        let store = self.store.clone();
        tokio::time::timeout(admitted.timeout, async move {
            store
                .commit_program_participant(&reservation, commit_cursor)
                .await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("program reservation commit deadline exceeded"))?
        .map_err(program_mutation_status)?;
        self.require_program_fence(admitted.placement.fence(), nomination)?;
        Ok(Response::new(wire::ProgramParticipantReservationApplied {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        }))
    }

    pub(super) async fn release_program_participant_call(
        &self,
        request: Request<wire::ProgramReleaseParticipantRequest>,
    ) -> Result<Response<wire::ProgramParticipantReservationApplied>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let nomination = self.require_program_executor(
            &admitted.placement,
            admitted.authenticated.node_id,
            request.get_ref().executor_nomination_log_index,
        )?;
        let reservation: ProgramReservation = decode_json(&request.get_ref().reservation_json)?;
        let finalized_commit_cursor = (request.get_ref().finalized_commit_cursor != 0)
            .then_some(request.get_ref().finalized_commit_cursor);
        let operation = if let Some(commit_cursor) = finalized_commit_cursor {
            ReservationOperation::ReleaseFinalized { commit_cursor }
        } else {
            ReservationOperation::ReleaseAborted
        };
        self.require_reservation_authority(
            &admitted.placement,
            nomination,
            &reservation,
            operation,
        )?;
        let group = reservation_group(&admitted.placement, &reservation)?;
        require_program_replica(&group, self.local_node)?;
        let store = self.store.clone();
        tokio::time::timeout(admitted.timeout, async move {
            store
                .release_program_participant(&reservation, finalized_commit_cursor)
                .await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("program reservation release deadline exceeded"))?
        .map_err(program_mutation_status)?;
        self.require_program_fence(admitted.placement.fence(), nomination)?;
        Ok(Response::new(wire::ProgramParticipantReservationApplied {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        }))
    }

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

    pub(super) async fn coordinate_program_alias_registry_finalization_call(
        &self,
        request: Request<wire::ProgramCoordinateAliasRegistryFinalizationRequest>,
    ) -> Result<Response<wire::ProgramCoordinatedAliasRegistryFinalization>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let nomination = self.require_program_executor(
            &admitted.placement,
            admitted.authenticated.node_id,
            request.get_ref().executor_nomination_log_index,
        )?;
        let stage: ProgramAliasRegistryStage = decode_json(&request.get_ref().stage_json)?;
        let group = program_alias_group(&admitted.placement, &stage)?;
        if group.coordinator() != self.local_node {
            return Err(Status::failed_precondition(
                "alias-registry finalization reached a node that is not the target authority",
            ));
        }
        let commit_cursor = request.get_ref().commit_cursor;
        let deadline = Instant::now()
            .checked_add(admitted.timeout)
            .ok_or_else(|| Status::invalid_argument("program deadline overflowed"))?;
        self.wait_for_alias_registry_commit(commit_cursor, &stage, nomination, deadline)
            .await?;
        let context = ObjectMutationContext {
            active_placement_log_id: admitted.placement.fence(),
            serving_fence_term: nomination.nomination_log_index,
        };
        let store = self.store.clone();
        let mutation = tokio::time::timeout(remaining(deadline)?, async move {
            store
                .coordinate_program_alias_registry_finalization(stage, commit_cursor, context)
                .await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("alias-registry finalization deadline exceeded"))?
        .map_err(program_status)?;
        self.require_program_fence(admitted.placement.fence(), nomination)?;
        Ok(Response::new(
            wire::ProgramCoordinatedAliasRegistryFinalization {
                schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                mutation_json: encode_json(&mutation)?,
            },
        ))
    }

    pub(super) async fn apply_program_alias_registry_finalization_call(
        &self,
        request: Request<wire::ProgramApplyAliasRegistryFinalizationRequest>,
    ) -> Result<Response<wire::ProgramAliasRegistryFinalizationApplied>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let nomination = self.require_program_executor(
            &admitted.placement,
            admitted.authenticated.node_id,
            request.get_ref().executor_nomination_log_index,
        )?;
        let mutation: ProgramAliasRegistryMutation = decode_json(&request.get_ref().mutation_json)?;
        let group = program_alias_group(&admitted.placement, &mutation.stage)?;
        require_program_replica(&group, self.local_node)?;
        let deadline = Instant::now()
            .checked_add(admitted.timeout)
            .ok_or_else(|| Status::invalid_argument("program deadline overflowed"))?;
        self.wait_for_alias_registry_commit(
            mutation.commit_cursor,
            &mutation.stage,
            nomination,
            deadline,
        )
        .await?;
        let context = ObjectMutationContext {
            active_placement_log_id: admitted.placement.fence(),
            serving_fence_term: nomination.nomination_log_index,
        };
        let store = self.store.clone();
        let changed = tokio::time::timeout(remaining(deadline)?, async move {
            store
                .apply_program_alias_registry_finalization_replica(&mutation, context)
                .await
        })
        .await
        .map_err(|_| Status::deadline_exceeded("alias-registry replica deadline exceeded"))?
        .map_err(program_status)?;
        self.require_program_fence(admitted.placement.fence(), nomination)?;
        Ok(Response::new(
            wire::ProgramAliasRegistryFinalizationApplied {
                schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                changed,
            },
        ))
    }

    pub(super) async fn read_program_alias_registry_call(
        &self,
        request: Request<wire::ProgramReadAliasRegistryRequest>,
    ) -> Result<Response<wire::ProgramAliasRegistryRead>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let nomination = self.require_program_executor(
            &admitted.placement,
            admitted.authenticated.node_id,
            request.get_ref().executor_nomination_log_index,
        )?;
        let group = program_alias_group_parts(
            &admitted.placement,
            request.get_ref().tenant_id,
            request.get_ref().bucket_id,
            &request.get_ref().canonical_path,
        )?;
        if group.coordinator() != self.local_node {
            return Err(Status::failed_precondition(
                "alias-registry read reached a node that is not the target authority",
            ));
        }
        let registry = self
            .store
            .object_alias_registry(
                request.get_ref().tenant_id,
                request.get_ref().bucket_id,
                &request.get_ref().canonical_path,
            )
            .map_err(program_mutation_status)?;
        self.require_program_fence(admitted.placement.fence(), nomination)?;
        Ok(Response::new(wire::ProgramAliasRegistryRead {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            registry_json: registry.as_ref().map(encode_json).transpose()?,
        }))
    }

    pub(super) fn require_program_executor(
        &self,
        placement: &ClusterPlacement,
        expected_executor: NodeId,
        nomination_log_index: u64,
    ) -> Result<ExecutorNomination, Status> {
        let nomination = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("atomic executor state is unavailable"))?
            .executor()
            .ok_or_else(|| Status::unavailable("EXECUTOR_MOVED: no executor is nominated"))?;
        if nomination.executor != expected_executor
            || nomination.nomination_log_index != nomination_log_index
            || !placement.active_node_ids().contains(&nomination.executor)
        {
            return Err(Status::unavailable(
                "EXECUTOR_MOVED: peer is not the current nominated atomic executor",
            ));
        }
        Ok(nomination)
    }

    fn require_reservation_authority(
        &self,
        placement: &ClusterPlacement,
        nomination: ExecutorNomination,
        reservation: &ProgramReservation,
        operation: ReservationOperation,
    ) -> Result<(), Status> {
        let identity = reservation_identity(reservation);
        if identity.executor_node_id != nomination.executor.0
            || identity.nomination_log_index != nomination.nomination_log_index
            || identity.placement != placement.fence()
            || !matches!(identity.state, ProgramReservationState::Prepared)
        {
            return Err(Status::unavailable(
                "atomic reservation does not carry the current executor and placement fence",
            ));
        }
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("atomic reservation authority is unavailable"))?;
        if !crate::cluster_capabilities::generalized_atomic_paths_active(&state) {
            return Err(Status::failed_precondition(
                "generalized atomic path reservations are not active for this cluster",
            ));
        }
        match operation {
            ReservationOperation::Reserve => {
                let preparing_matches = state
                    .preparing_batch()
                    .is_some_and(|batch| reservation_matches_prepared(identity, batch));
                let committed_matches = state
                    .unfinalized_invocations()
                    .any(|invocation| reservation_matches_committed(identity, invocation));
                if !preparing_matches && !committed_matches {
                    return Err(Status::failed_precondition(
                        "atomic reservation has no matching prepared or unfinalized Raft authority",
                    ));
                }
            }
            ReservationOperation::Commit { commit_cursor } => {
                let invocation = state.committed_invocation(commit_cursor).ok_or_else(|| {
                    Status::failed_precondition(
                        "atomic reservation commit has no matching Raft decision",
                    )
                })?;
                if state.finalized_through().unwrap_or(0) >= commit_cursor
                    || !reservation_matches_committed(identity, invocation)
                {
                    return Err(Status::failed_precondition(
                        "atomic reservation commit does not match an unfinalized Raft decision",
                    ));
                }
            }
            ReservationOperation::ReleaseAborted => {
                if state
                    .preparing_batch()
                    .is_some_and(|batch| reservation_matches_prepared(identity, batch))
                    || state
                        .unfinalized_invocations()
                        .any(|invocation| reservation_matches_committed(identity, invocation))
                {
                    return Err(Status::failed_precondition(
                        "atomic reservation cannot be released before durable Abort or FinalizedThrough",
                    ));
                }
            }
            ReservationOperation::ReleaseFinalized { commit_cursor } => {
                if state.finalized_through().unwrap_or(0) < commit_cursor {
                    return Err(Status::failed_precondition(
                        "atomic reservation cannot be released before its exact commit is finalized",
                    ));
                }
                if let Some(invocation) = state.committed_invocation(commit_cursor)
                    && !reservation_matches_committed(identity, invocation)
                {
                    return Err(Status::failed_precondition(
                        "atomic reservation release does not match its retained Raft decision",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) fn require_program_fence(
        &self,
        expected_placement: keldra_store::PlacementLogId,
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
        self.wait_for_committed_stage(
            commit_cursor,
            stage.bundle_hash,
            stage.begin_cursor,
            stage.authority,
            stage.participant_manifest_hash,
            nomination,
            deadline,
        )
        .await
    }

    async fn wait_for_alias_registry_commit(
        &self,
        commit_cursor: u64,
        stage: &ProgramAliasRegistryStage,
        nomination: ExecutorNomination,
        deadline: Instant,
    ) -> Result<(), Status> {
        self.wait_for_committed_stage(
            commit_cursor,
            stage.bundle_hash,
            stage.begin_cursor,
            stage.authority,
            stage.participant_manifest_hash,
            nomination,
            deadline,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn wait_for_committed_stage(
        &self,
        commit_cursor: u64,
        bundle_hash: keldra_store::PreparedBundleHash,
        begin_cursor: u64,
        authority: ProgramBundleAuthority,
        participant_manifest_hash: [u8; 32],
        _nomination: ExecutorNomination,
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
                    if batch.bundle_hash.0 != bundle_hash.0
                        || batch.begin_cursor != begin_cursor
                        || super::super::programs::store_bundle_authority(batch.authority)
                            != authority
                        || batch.participant_manifest_hash.0 != participant_manifest_hash
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

#[derive(Clone, Copy)]
enum ReservationOperation {
    Reserve,
    Commit { commit_cursor: u64 },
    ReleaseAborted,
    ReleaseFinalized { commit_cursor: u64 },
}

#[derive(Clone, Copy)]
struct ReservationIdentity {
    begin_cursor: u64,
    invocation_id: [u8; 32],
    bundle_hash: [u8; 32],
    participant_manifest_hash: [u8; 32],
    authority: ProgramBundleAuthority,
    executor_node_id: u64,
    nomination_log_index: u64,
    placement: PlacementLogId,
    state: ProgramReservationState,
}

fn reservation_identity(reservation: &ProgramReservation) -> ReservationIdentity {
    match reservation {
        ProgramReservation::Object(value) => ReservationIdentity {
            begin_cursor: value.begin_cursor,
            invocation_id: value.invocation_id,
            bundle_hash: value.bundle_hash,
            participant_manifest_hash: value.participant_manifest_hash,
            authority: value.authority,
            executor_node_id: value.executor_node_id,
            nomination_log_index: value.nomination_log_index,
            placement: value.placement,
            state: value.state,
        },
        ProgramReservation::Governance(value) => ReservationIdentity {
            begin_cursor: value.begin_cursor,
            invocation_id: value.invocation_id,
            bundle_hash: value.bundle_hash,
            participant_manifest_hash: value.participant_manifest_hash,
            authority: value.authority,
            executor_node_id: value.executor_node_id,
            nomination_log_index: value.nomination_log_index,
            placement: value.placement,
            state: value.state,
        },
    }
}

fn reservation_matches_prepared(reservation: ReservationIdentity, prepared: PreparedBatch) -> bool {
    reservation.begin_cursor == prepared.begin_cursor
        && reservation.invocation_id == prepared.request.invocation_id.0
        && reservation.bundle_hash == prepared.request.bundle_hash.0
        && reservation.participant_manifest_hash == prepared.request.participant_manifest_hash.0
        && reservation.authority
            == super::super::programs::store_bundle_authority(prepared.request.authority)
        && reservation.executor_node_id == prepared.request.executor.0
        && reservation.nomination_log_index == prepared.request.nomination_log_index
}

fn reservation_matches_committed(
    reservation: ReservationIdentity,
    invocation: CommittedInvocation,
) -> bool {
    let committed = invocation.committed_batch;
    reservation.begin_cursor == committed.begin_cursor
        && reservation.invocation_id == invocation.invocation_id.0
        && reservation.bundle_hash == committed.bundle_hash.0
        && reservation.participant_manifest_hash == committed.participant_manifest_hash.0
        && reservation.authority
            == super::super::programs::store_bundle_authority(committed.authority)
}

fn reservation_group(
    placement: &ClusterPlacement,
    reservation: &ProgramReservation,
) -> Result<MutableRecordReplicaGroup, Status> {
    let (tenant_id, bucket_id) = reservation.stable_bucket_ids();
    let path = reservation.path();
    let mut key = Vec::with_capacity(16 + path.path.len());
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&bucket_id.to_be_bytes());
    key.extend_from_slice(path.path.as_bytes());
    MutableRecordReplicaGroup::select(
        PlacementKind::Object,
        placement.cluster_id(),
        &key,
        placement.placement_nodes(),
    )
    .ok_or_else(|| Status::unavailable("cluster has no active reservation metadata owner"))
}

fn program_mutation_status(error: keldra_store::MutationError) -> Status {
    super::super::programs::mutation_status(error)
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

fn program_alias_group(
    placement: &ClusterPlacement,
    stage: &ProgramAliasRegistryStage,
) -> Result<MutableRecordReplicaGroup, Status> {
    program_alias_group_parts(
        placement,
        stage.tenant_id,
        stage.bucket_id,
        &stage.target.path,
    )
}

fn program_alias_group_parts(
    placement: &ClusterPlacement,
    tenant_id: u64,
    bucket_id: u64,
    path: &str,
) -> Result<MutableRecordReplicaGroup, Status> {
    let mut key = Vec::with_capacity(16 + path.len());
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&bucket_id.to_be_bytes());
    key.extend_from_slice(path.as_bytes());
    MutableRecordReplicaGroup::select(
        PlacementKind::Object,
        placement.cluster_id(),
        &key,
        placement.placement_nodes(),
    )
    .ok_or_else(|| Status::unavailable("cluster has no active alias target metadata owner"))
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
        ProgramStoreError::PreconditionFailed { .. } | ProgramStoreError::Immutable { .. } => {
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
        ProgramStoreError::SourceJournalTransitionTooLarge { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        ProgramStoreError::ProgramHashMismatch => {
            Status::failed_precondition(format!("PROGRAM_VERSION_MISMATCH: {error}"))
        }
        ProgramStoreError::Storage(_) => Status::internal(error.to_string()),
    }
}
