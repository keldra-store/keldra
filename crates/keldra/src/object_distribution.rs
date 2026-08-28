//! Cluster coordination for exact-path object mutations.
//!
//! The storage crate evaluates and applies typed mutations. This module adds
//! only the placement and acknowledgement policy from KELDRA-0010: rank zero
//! coordinates one exact path and the first three HRW owners hold complete
//! logical replicas. Ownership itself is never persisted here or in Raft.

mod batch;
mod quorum_read;
mod serving_read;

pub(crate) use quorum_read::select_object_snapshot_quorum;

use std::future::Future;
use std::time::Duration;

use keldra_consensus::{DecisionRaft, NodeId};
use keldra_store::{
    BatchOperation, BlobRef, CloneRequest, CoordinatedObjectMutation,
    CoordinatedRetainedVersionDelete, DefinitionMutationIntent, DeleteRetainedVersionOutcome,
    Durability, ErasureProfile, MutationError, MutationReceipt, ObjectKey,
    ObjectMutationGovernance, PublishRequest, PutRequest, Store, VersionId,
};
use tonic::Status;

use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::DataPeerTransport;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::payload_distribution::{
    PayloadDistribution, PayloadDistributionError, PayloadPeerTransport,
};
use crate::payload_placement::{NodePayloadEvidence, select_payload_placement};
use crate::placement::PlacementKind;
use crate::reference_delivery::ReferenceRuntimeHandle;
use crate::serving_fence::ServingAuthority;

struct DistributedMutationBackpressureWait {
    capacity: &'static str,
    started: std::time::Instant,
    finished: bool,
}

impl DistributedMutationBackpressureWait {
    fn start(capacity: &'static str) -> Self {
        tracing::info!(
            capacity,
            counter.keldra_distributed_mutation_backpressure_waiting = 1_i64,
            monotonic_counter.keldra_distributed_mutation_backpressure_waits_total = 1_u64,
            "distributed object mutation is waiting for bounded durable state"
        );
        Self {
            capacity,
            started: std::time::Instant::now(),
            finished: false,
        }
    }

    fn complete(mut self) {
        self.emit("capacity_available", false);
        self.finished = true;
    }

    fn emit(&self, outcome: &'static str, cancelled: bool) {
        tracing::info!(
            capacity = self.capacity,
            counter.keldra_distributed_mutation_backpressure_waiting = -1_i64,
            "distributed object mutation capacity wait released"
        );
        tracing::info!(
            capacity = self.capacity,
            backpressure.outcome = outcome,
            monotonic_counter.keldra_distributed_mutation_backpressure_wait_cancellations_total =
                u64::from(cancelled),
            histogram.keldra_distributed_mutation_backpressure_wait_duration_seconds =
                self.started.elapsed().as_secs_f64(),
            "distributed object mutation capacity wait finished"
        );
    }
}

impl Drop for DistributedMutationBackpressureWait {
    fn drop(&mut self) {
        if !self.finished {
            self.emit("cancelled", true);
        }
    }
}

#[derive(Clone)]
pub(crate) struct ObjectDistribution {
    local_node: NodeId,
    store: Store,
    decisions: DecisionRaft,
    serving: ServingAuthority,
    peers: DataPeerTransport,
    payload: PayloadDistribution,
    payload_peers: PayloadPeerTransport,
    erasure_profile: ErasureProfile,
    references: ReferenceRuntimeHandle,
    reference_acknowledgement_timeout: Duration,
    mutation_admission: crate::mutation_admission::MutationAdmission,
}

impl ObjectDistribution {
    pub(crate) fn new(
        local_node: NodeId,
        store: Store,
        decisions: DecisionRaft,
        serving: ServingAuthority,
        peers: DataPeerTransport,
        erasure_profile: ErasureProfile,
        references: ReferenceRuntimeHandle,
        reference_acknowledgement_timeout: Duration,
        mutation_admission: crate::mutation_admission::MutationAdmission,
    ) -> Self {
        let payload = PayloadDistribution::new(
            local_node,
            store.clone(),
            std::sync::Arc::new(peers.clone()),
            erasure_profile,
        );
        let payload_peers = PayloadPeerTransport::new(peers.clone());
        Self {
            local_node,
            store,
            decisions,
            serving,
            peers,
            payload,
            payload_peers,
            erasure_profile,
            references,
            reference_acknowledgement_timeout,
            mutation_admission,
        }
    }

    pub(crate) const fn local_node(&self) -> NodeId {
        self.local_node
    }

    /// Retains the existing membership-cutover gate across an internal
    /// orchestration step that must not straddle two object placements.
    pub(crate) fn enter_mutation(
        &self,
    ) -> Result<crate::mutation_admission::MutationPermit, Status> {
        self.mutation_admission.enter()
    }

    /// Publish content sealed by the upload-source node named in the ready
    /// capability. Payload evidence and the exact-path metadata quorum remain
    /// separate, explicit response boundaries.
    pub(crate) async fn publish_from_source(
        &self,
        request: PublishRequest,
        upload_source: NodeId,
    ) -> Result<MutationReceipt, Status> {
        let (tenant_id, bucket_id) = self
            .store
            .resolve_bucket_ids(request.key.tenant(), request.key.bucket())
            .map_err(mutation_status)?;
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: self
                .store
                .bucket_versioning(request.key.tenant(), request.key.bucket())
                .map_err(mutation_status)?,
            policy: self
                .store
                .bucket_policy(request.key.tenant(), request.key.bucket())
                .map_err(mutation_status)?,
        };
        self.publish_from_source_with_governance(request, upload_source, governance)
            .await
    }

    /// Publishes a second logical reference to an already-placed immutable
    /// payload. No payload bytes are read, reconstructed, or transferred.
    pub(crate) async fn clone_reference(
        &self,
        request: CloneRequest,
        governance: ObjectMutationGovernance,
    ) -> Result<MutationReceipt, Status> {
        if self.is_single_node()? {
            let _permit = self.mutation_admission.enter()?;
            return self
                .store
                .mutate_with_governance_and_backpressure(BatchOperation::Clone(request), governance)
                .await
                .map_err(mutation_status);
        }
        Err(Status::unavailable(
            "distributed CloneObject requires an exact retained-version atomic precondition and is not enabled",
        ))
    }

    pub(crate) async fn publish_from_source_with_governance(
        &self,
        request: PublishRequest,
        upload_source: NodeId,
        governance: ObjectMutationGovernance,
    ) -> Result<MutationReceipt, Status> {
        self.publish_from_source_with_governance_and_definition_intent(
            request,
            upload_source,
            governance,
            None,
        )
        .await
    }

    /// Publish one bounded set of independently receipted objects whose exact
    /// paths select the same metadata replica group under `placement`.
    /// Payload preparation and metadata quorum rules remain identical to the
    /// unary path; only coordinator persistence is physically grouped.
    pub(crate) async fn publish_many_from_source_with_governance(
        &self,
        requests: Vec<PublishRequest>,
        upload_source: NodeId,
        governance: ObjectMutationGovernance,
        placement: ClusterPlacement,
    ) -> Result<Vec<Result<MutationReceipt, Status>>, Status> {
        self.publish_many_from_source_with_governance_and_admission(
            requests,
            upload_source,
            governance,
            placement,
            false,
        )
        .await
    }

    pub(crate) async fn publish_many_derived_progress_from_source_with_governance(
        &self,
        requests: Vec<PublishRequest>,
        upload_source: NodeId,
        governance: ObjectMutationGovernance,
        placement: ClusterPlacement,
    ) -> Result<Vec<Result<MutationReceipt, Status>>, Status> {
        self.publish_many_from_source_with_governance_and_admission(
            requests,
            upload_source,
            governance,
            placement,
            true,
        )
        .await
    }

    async fn publish_many_from_source_with_governance_and_admission(
        &self,
        requests: Vec<PublishRequest>,
        upload_source: NodeId,
        governance: ObjectMutationGovernance,
        placement: ClusterPlacement,
        derived_progress: bool,
    ) -> Result<Vec<Result<MutationReceipt, Status>>, Status> {
        if requests.is_empty() {
            return Err(Status::invalid_argument(
                "grouped publication requires at least one object",
            ));
        }
        governance.validate().map_err(mutation_status)?;
        if self.placement()?.fence() != placement.fence() {
            return Err(Status::unavailable(
                "object placement changed before grouped publication",
            ));
        }
        let group = self.replica_group_stable(
            &placement,
            governance.tenant_id,
            governance.bucket_id,
            &requests[0].key,
        )?;
        if group.coordinator() != self.local_node {
            return Err(Status::failed_precondition(format!(
                "object group is coordinated by node {}",
                group.coordinator().0
            )));
        }
        for request in &requests[1..] {
            let candidate = self.replica_group_stable(
                &placement,
                governance.tenant_id,
                governance.bucket_id,
                &request.key,
            )?;
            if candidate != group {
                return Err(Status::invalid_argument(
                    "grouped publication spans metadata replica groups",
                ));
            }
        }

        if placement.active_node_ids().len() == 1 {
            if upload_source != self.local_node {
                return Err(Status::failed_precondition(
                    "the ready capability names another upload source",
                ));
            }
            let _permit = self.mutation_admission.enter()?;
            let outcomes = if derived_progress {
                self.store
                    .bulk_write_derived_progress_with_backpressure(requests)
                    .await
            } else {
                self.store
                    .bulk_write_with_backpressure(
                        requests.into_iter().map(BatchOperation::Publish).collect(),
                    )
                    .await
            };
            return Ok(outcomes
                .into_iter()
                .map(|outcome| outcome.result.map_err(mutation_status))
                .collect());
        }
        if !placement.active_node_ids().contains(&upload_source) {
            return Err(Status::failed_precondition(
                "the upload source is not ACTIVE in the current placement",
            ));
        }

        let mut evidence = Vec::with_capacity(requests.len());
        for request in &requests {
            let prepared = self
                .prepare_payload(&placement, upload_source, &request.blob, request.durability)
                .await?;
            self.payload
                .verify_on_path_coordinator(
                    &placement,
                    &request.blob,
                    request.durability,
                    upload_source,
                    &prepared,
                )
                .await
                .map_err(payload_status)?;
            evidence.push(prepared);
        }

        loop {
            let permit = self.mutation_admission.enter()?;
            let mut context = None;
            for request in &requests {
                let candidate = self
                    .reconcile_before_mutation_stable(
                        &request.key,
                        governance.tenant_id,
                        governance.bucket_id,
                        placement.fence(),
                    )
                    .await?;
                if context.is_some_and(|current| current != candidate) {
                    return Err(Status::unavailable(
                        "serving authority changed during grouped publication",
                    ));
                }
                context = Some(candidate);
            }
            let context = context.expect("non-empty publication has a mutation context");
            let completion = self.clone();
            let completion_requests = requests.clone();
            let completion_governance = governance.clone();
            let completion_placement = placement.clone();
            let completion_group = group.clone();
            let completed = complete_metadata(async move {
                let _permit = permit;
                let coordinated = if derived_progress {
                    completion
                        .store
                        .coordinate_derived_progress_publish_batch_with_governance(
                            completion_requests.clone(),
                            completion_governance,
                            context,
                        )
                        .await
                } else {
                    completion
                        .store
                        .coordinate_distributed_publish_batch_with_governance(
                            completion_requests.clone(),
                            completion_governance,
                            context,
                        )
                        .await
                }
                .map_err(mutation_status)?;
                let mut outcomes = Vec::with_capacity(coordinated.len());
                let mut quorum_proven_positions = Vec::new();
                for coordinated in coordinated {
                    let outcome = match coordinated {
                        Err(error) => Err(mutation_status(error)),
                        Ok(coordinated) => {
                            let replayed = coordinated.receipt.replayed;
                            if let Some((source, positions)) = completion
                                .replicate_without_settlement(
                                    &completion_placement,
                                    &completion_group,
                                    &coordinated,
                                )
                                .await?
                                && !replayed
                            {
                                quorum_proven_positions
                                    .extend(positions.into_iter().map(|offset| (source, offset)));
                            }
                            Ok(coordinated)
                        }
                    };
                    outcomes.push(outcome);
                }
                if let Some((source, _)) = quorum_proven_positions.first().copied()
                    && let Err(error) = completion
                        .store
                        .settle_source_journal_positions_if_contiguous(
                            source,
                            &quorum_proven_positions
                                .iter()
                                .map(|(_, offset)| *offset)
                                .collect::<Vec<_>>(),
                        )
                        .await
                {
                    tracing::warn!(
                        source = ?source,
                        count = quorum_proven_positions.len(),
                        %error,
                        "grouped metadata quorum succeeded but source settlement failed"
                    );
                }
                Ok::<_, Status>(outcomes)
            })
            .await;
            if let Err(error) = &completed
                && let Some(capacity) = mutation_capacity_kind(error)
            {
                self.wait_for_mutation_capacity(capacity).await;
                continue;
            }
            let coordinated = completed?;
            let mut outcomes = Vec::with_capacity(coordinated.len());
            for ((request, evidence), coordinated) in
                requests.iter().zip(&evidence).zip(coordinated)
            {
                let outcome = match coordinated {
                    Err(error) => Err(error),
                    Ok(coordinated) => {
                        match request.durability {
                            Durability::Local => {
                                self.continue_payload_placement(upload_source, request.blob.clone())
                            }
                            Durability::Replicated => {
                                self.wait_for_replicated_reference(
                                    &placement,
                                    &request.blob,
                                    evidence,
                                    &coordinated,
                                )
                                .await?;
                            }
                        }
                        Ok(coordinated.receipt)
                    }
                };
                outcomes.push(outcome);
            }
            if self.placement()?.fence() != placement.fence() {
                return Err(Status::unavailable(
                    "object placement changed during grouped publication",
                ));
            }
            return Ok(outcomes);
        }
    }

    pub(crate) async fn publish_from_source_with_governance_and_definition_intent(
        &self,
        request: PublishRequest,
        upload_source: NodeId,
        governance: ObjectMutationGovernance,
        definition_intent: Option<DefinitionMutationIntent>,
    ) -> Result<MutationReceipt, Status> {
        self.publish_from_source_with_governance_and_admission(
            request,
            upload_source,
            governance,
            definition_intent,
            false,
        )
        .await
    }

    pub(crate) async fn publish_derived_progress_from_source_with_governance(
        &self,
        request: PublishRequest,
        upload_source: NodeId,
        governance: ObjectMutationGovernance,
    ) -> Result<MutationReceipt, Status> {
        self.publish_from_source_with_governance_and_admission(
            request,
            upload_source,
            governance,
            None,
            true,
        )
        .await
    }

    async fn publish_from_source_with_governance_and_admission(
        &self,
        request: PublishRequest,
        upload_source: NodeId,
        governance: ObjectMutationGovernance,
        definition_intent: Option<DefinitionMutationIntent>,
        derived_progress: bool,
    ) -> Result<MutationReceipt, Status> {
        if self.is_single_node()? {
            if upload_source != self.local_node {
                return Err(Status::failed_precondition(
                    "the ready capability names another upload source",
                ));
            }
            let _permit = self.mutation_admission.enter()?;
            return match (definition_intent, derived_progress) {
                (Some(intent), false) => {
                    self.store
                        .mutate_definition_with_governance_and_backpressure(
                            BatchOperation::Publish(request),
                            governance,
                            intent,
                        )
                        .await
                }
                (None, false) => {
                    self.store
                        .mutate_with_governance_and_backpressure(
                            BatchOperation::Publish(request),
                            governance,
                        )
                        .await
                }
                (None, true) => {
                    self.store
                        .mutate_derived_progress_with_governance_and_backpressure(
                            request, governance,
                        )
                        .await
                }
                (Some(_), true) => Err(MutationError::InvalidObjectMutation(
                    "definition publication cannot claim derived progress admission".into(),
                )),
            }
            .map_err(mutation_status);
        }
        loop {
            let result = self
                .publish_from_source_with_governance_and_definition_intent_once(
                    request.clone(),
                    upload_source,
                    governance.clone(),
                    definition_intent,
                    derived_progress,
                )
                .await;
            if let Err(error) = &result
                && let Some(capacity) = mutation_capacity_kind(error)
            {
                self.wait_for_mutation_capacity(capacity).await;
                continue;
            }
            return result;
        }
    }

    async fn publish_from_source_with_governance_and_definition_intent_once(
        &self,
        request: PublishRequest,
        upload_source: NodeId,
        governance: ObjectMutationGovernance,
        definition_intent: Option<DefinitionMutationIntent>,
        derived_progress: bool,
    ) -> Result<MutationReceipt, Status> {
        governance.validate().map_err(mutation_status)?;
        let placement = self.placement()?;
        if placement.active_node_ids().len() == 1 {
            if upload_source != self.local_node {
                return Err(Status::failed_precondition(
                    "the ready capability names another upload source",
                ));
            }
            let _permit = self.mutation_admission.enter()?;
            return match (definition_intent, derived_progress) {
                (Some(intent), false) => {
                    self.store
                        .mutate_definition_with_governance(
                            BatchOperation::Publish(request),
                            governance,
                            intent,
                        )
                        .await
                }
                (None, false) => {
                    self.store
                        .mutate_with_governance(BatchOperation::Publish(request), governance)
                        .await
                }
                (None, true) => {
                    self.store
                        .mutate_derived_progress_with_governance_and_backpressure(
                            request, governance,
                        )
                        .await
                }
                (Some(_), true) => Err(MutationError::InvalidObjectMutation(
                    "definition publication cannot claim derived progress admission".into(),
                )),
            }
            .map_err(mutation_status);
        }

        let group = self.replica_group_stable(
            &placement,
            governance.tenant_id,
            governance.bucket_id,
            &request.key,
        )?;
        if group.coordinator() != self.local_node {
            return Err(Status::failed_precondition(format!(
                "object path is coordinated by node {}",
                group.coordinator().0
            )));
        }
        if !placement.active_node_ids().contains(&upload_source) {
            return Err(Status::failed_precondition(
                "the upload source is not ACTIVE in the current placement",
            ));
        }

        let reference = request.blob.clone();
        let evidence = self
            .prepare_payload(&placement, upload_source, &reference, request.durability)
            .await?;
        self.payload
            .verify_on_path_coordinator(
                &placement,
                &reference,
                request.durability,
                upload_source,
                &evidence,
            )
            .await
            .map_err(payload_status)?;

        let permit = self.mutation_admission.enter()?;
        let context = self
            .reconcile_before_mutation_stable(
                &request.key,
                governance.tenant_id,
                governance.bucket_id,
                placement.fence(),
            )
            .await?;
        let durability = request.durability;
        let completion = self.clone();
        let completion_placement = placement.clone();
        let coordinated = complete_metadata(async move {
            let _permit = permit;
            let coordinated = match (definition_intent, derived_progress) {
                (Some(intent), false) => {
                    completion
                        .store
                        .coordinate_distributed_definition_publish_with_governance(
                            request, governance, context, intent,
                        )
                        .await
                }
                (None, false) => {
                    completion
                        .store
                        .coordinate_distributed_publish_with_governance(
                            request, governance, context,
                        )
                        .await
                }
                (None, true) => {
                    completion
                        .store
                        .coordinate_derived_progress_publish_with_governance(
                            request, governance, context,
                        )
                        .await
                }
                (Some(_), true) => Err(MutationError::InvalidObjectMutation(
                    "definition publication cannot claim derived progress admission".into(),
                )),
            }
            .map_err(mutation_status)?;
            completion
                .replicate(&completion_placement, &group, &coordinated)
                .await?;
            Ok::<_, Status>(coordinated)
        })
        .await?;

        match durability {
            Durability::Local => self.continue_payload_placement(upload_source, reference),
            Durability::Replicated => {
                self.wait_for_replicated_reference(&placement, &reference, &evidence, &coordinated)
                    .await?;
            }
        }
        Ok(coordinated.receipt)
    }

    /// Apply one operation locally when this is the released one-node shape,
    /// otherwise require this node to be the current exact-path coordinator
    /// and durably replicate the resulting typed mutation to its quorum.
    pub(crate) async fn mutate(
        &self,
        operation: BatchOperation,
    ) -> Result<MutationReceipt, Status> {
        let key = operation_key(&operation);
        let (tenant_id, bucket_id) = self
            .store
            .resolve_bucket_ids(key.tenant(), key.bucket())
            .map_err(mutation_status)?;
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: self
                .store
                .bucket_versioning(key.tenant(), key.bucket())
                .map_err(mutation_status)?,
            policy: self
                .store
                .bucket_policy(key.tenant(), key.bucket())
                .map_err(mutation_status)?,
        };
        self.mutate_with_governance_and_definition_intent(operation, governance, None)
            .await
    }

    pub(crate) async fn mutate_with_definition_intent(
        &self,
        operation: BatchOperation,
        intent: DefinitionMutationIntent,
    ) -> Result<MutationReceipt, Status> {
        let key = operation_key(&operation);
        let (tenant_id, bucket_id) = self
            .store
            .resolve_bucket_ids(key.tenant(), key.bucket())
            .map_err(mutation_status)?;
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: self
                .store
                .bucket_versioning(key.tenant(), key.bucket())
                .map_err(mutation_status)?,
            policy: self
                .store
                .bucket_policy(key.tenant(), key.bucket())
                .map_err(mutation_status)?,
        };
        self.mutate_with_governance_and_definition_intent(operation, governance, Some(intent))
            .await
    }

    pub(crate) async fn mutate_with_governance(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
    ) -> Result<MutationReceipt, Status> {
        self.mutate_with_governance_and_definition_intent(operation, governance, None)
            .await
    }

    pub(crate) async fn mutate_with_governance_and_definition_intent(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
        definition_intent: Option<DefinitionMutationIntent>,
    ) -> Result<MutationReceipt, Status> {
        if self.is_single_node()? {
            let _permit = self.mutation_admission.enter()?;
            return match definition_intent {
                Some(intent) => {
                    self.store
                        .mutate_definition_with_governance_and_backpressure(
                            operation, governance, intent,
                        )
                        .await
                }
                None => {
                    self.store
                        .mutate_with_governance_and_backpressure(operation, governance)
                        .await
                }
            }
            .map_err(mutation_status);
        }
        // Seal a distributed inline payload once before any bounded-state
        // backpressure retries. Retries then copy only the compact descriptor.
        let operation = match operation {
            BatchOperation::Put(request) => {
                let publish = stage_distributed_put(&self.store, request).await?;
                return self
                    .publish_from_source_with_governance_and_definition_intent(
                        publish,
                        self.local_node,
                        governance,
                        definition_intent,
                    )
                    .await;
            }
            BatchOperation::Clone(_) => {
                return Err(Status::unavailable(
                    "distributed CloneObject requires an exact retained-version atomic precondition and is not enabled",
                ));
            }
            operation => operation,
        };
        loop {
            let result = self
                .mutate_with_governance_and_definition_intent_once(
                    operation.clone(),
                    governance.clone(),
                    definition_intent,
                )
                .await;
            if let Err(error) = &result
                && let Some(capacity) = mutation_capacity_kind(error)
            {
                self.wait_for_mutation_capacity(capacity).await;
                continue;
            }
            return result;
        }
    }

    async fn mutate_with_governance_and_definition_intent_once(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
        definition_intent: Option<DefinitionMutationIntent>,
    ) -> Result<MutationReceipt, Status> {
        governance.validate().map_err(mutation_status)?;
        let placement = self.placement()?;
        if placement.active_node_ids().len() == 1 {
            let _permit = self.mutation_admission.enter()?;
            return match definition_intent {
                Some(intent) => {
                    self.store
                        .mutate_definition_with_governance(operation, governance, intent)
                        .await
                }
                None => {
                    self.store
                        .mutate_with_governance(operation, governance)
                        .await
                }
            }
            .map_err(mutation_status);
        }

        // A unary bulk put arrives with inline bytes rather than a previously
        // sealed upload token. Seal those bytes on this path coordinator, then
        // use the same payload preparation and verified Publish path as PutEnd.
        // Metadata is not evaluated until the requested payload durability has
        // been proved.
        let operation = match operation {
            BatchOperation::Put(request) => {
                let publish = stage_distributed_put(&self.store, request).await?;
                return self
                    .publish_from_source_with_governance_and_definition_intent(
                        publish,
                        self.local_node,
                        governance,
                        definition_intent,
                    )
                    .await;
            }
            BatchOperation::Clone(_) => {
                return Err(Status::unavailable(
                    "distributed CloneObject requires an exact retained-version atomic precondition and is not enabled",
                ));
            }
            operation => operation,
        };

        let key = operation_key(&operation);
        let group =
            self.replica_group_stable(&placement, governance.tenant_id, governance.bucket_id, key)?;
        if group.coordinator() != self.local_node {
            return Err(Status::failed_precondition(format!(
                "object path is coordinated by node {}",
                group.coordinator().0
            )));
        }

        let permit = self.mutation_admission.enter()?;
        let context = self
            .reconcile_before_mutation_stable(
                key,
                governance.tenant_id,
                governance.bucket_id,
                placement.fence(),
            )
            .await?;
        let completion = self.clone();
        let completion_placement = placement.clone();
        let coordinated = complete_metadata(async move {
            let _permit = permit;
            let coordinated = match definition_intent {
                Some(intent) => {
                    completion
                        .store
                        .coordinate_definition_object_mutation_with_governance(
                            operation, governance, context, intent,
                        )
                        .await
                }
                None => {
                    completion
                        .store
                        .coordinate_object_mutation_with_governance(operation, governance, context)
                        .await
                }
            }
            .map_err(mutation_status)?;
            completion
                .replicate(&completion_placement, &group, &coordinated)
                .await?;
            Ok::<_, Status>(coordinated)
        })
        .await?;
        Ok(coordinated.receipt)
    }

    pub(crate) async fn delete_retained_version_with_governance(
        &self,
        key: &ObjectKey,
        version: VersionId,
        governance: ObjectMutationGovernance,
    ) -> Result<DeleteRetainedVersionOutcome, Status> {
        loop {
            let result = self
                .delete_retained_version_with_governance_once(key, version, governance.clone())
                .await;
            if let Err(error) = &result
                && let Some(capacity) = mutation_capacity_kind(error)
            {
                self.wait_for_mutation_capacity(capacity).await;
                continue;
            }
            return result;
        }
    }

    async fn delete_retained_version_with_governance_once(
        &self,
        key: &ObjectKey,
        version: VersionId,
        governance: ObjectMutationGovernance,
    ) -> Result<DeleteRetainedVersionOutcome, Status> {
        governance.validate().map_err(mutation_status)?;
        let placement = self.placement()?;
        if placement.active_node_ids().len() == 1 {
            let _permit = self.mutation_admission.enter()?;
            return self
                .store
                .coordinate_local_retained_version_delete(
                    key,
                    version,
                    governance,
                    self.serving.mutation_context()?,
                )
                .await
                .map(|coordinated| coordinated.outcome)
                .map_err(mutation_status);
        }
        let group =
            self.replica_group_stable(&placement, governance.tenant_id, governance.bucket_id, key)?;
        if group.coordinator() != self.local_node {
            return Err(Status::failed_precondition(format!(
                "object path is coordinated by node {}",
                group.coordinator().0
            )));
        }
        let permit = self.mutation_admission.enter()?;
        let context = self
            .reconcile_before_mutation_stable(
                key,
                governance.tenant_id,
                governance.bucket_id,
                placement.fence(),
            )
            .await?;
        let completion = self.clone();
        let completion_placement = placement.clone();
        let key = key.clone();
        let coordinated = complete_metadata(async move {
            let _permit = permit;
            let coordinated = completion
                .store
                .coordinate_retained_version_delete(&key, version, governance, context)
                .await
                .map_err(mutation_status)?;
            completion
                .replicate_retained_version_delete(&completion_placement, &group, &coordinated)
                .await?;
            Ok::<_, Status>(coordinated)
        })
        .await?;
        Ok(coordinated.outcome)
    }

    pub(crate) fn coordinator(&self, key: &ObjectKey) -> Result<NodeId, Status> {
        let placement = self.placement()?;
        Ok(self.replica_group(&placement, key)?.coordinator())
    }

    /// Resolve one remote coordinator and its address under the same applied
    /// placement. `None` means this node is already the coordinator.
    pub(crate) fn routing_target(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<(NodeId, String)>, Status> {
        let placement = self.placement()?;
        let coordinator = self.replica_group(&placement, key)?.coordinator();
        if coordinator == self.local_node {
            return Ok(None);
        }
        let address = placement.address(coordinator).ok_or_else(|| {
            Status::unavailable(format!(
                "ACTIVE object coordinator {} has no peer address",
                coordinator.0
            ))
        })?;
        Ok(Some((coordinator, address.0.clone())))
    }

    pub(crate) fn routing_target_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<(NodeId, String)>, Status> {
        let placement = self.placement()?;
        let coordinator = self
            .replica_group_stable(&placement, tenant_id, bucket_id, key)?
            .coordinator();
        if coordinator == self.local_node {
            return Ok(None);
        }
        let address = placement.address(coordinator).ok_or_else(|| {
            Status::unavailable(format!(
                "ACTIVE object coordinator {} has no peer address",
                coordinator.0
            ))
        })?;
        Ok(Some((coordinator, address.0.clone())))
    }

    pub(crate) fn object_coordinator_stable(
        &self,
        placement: &ClusterPlacement,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<NodeId, Status> {
        Ok(self
            .replica_group_stable(placement, tenant_id, bucket_id, key)?
            .coordinator())
    }

    pub(crate) fn object_replica_group_stable(
        &self,
        placement: &ClusterPlacement,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<MutableRecordReplicaGroup, Status> {
        self.replica_group_stable(placement, tenant_id, bucket_id, key)
    }

    pub(crate) fn is_single_node(&self) -> Result<bool, Status> {
        Ok(self.placement()?.active_node_ids().len() == 1)
    }

    /// Reject a response guarantee which the committed ACTIVE membership
    /// cannot satisfy before the caller starts expensive or mutating work.
    ///
    /// Ordinary Put/Delete still perform their authoritative durability check
    /// at publication. This early check shares the same committed placement
    /// authority and only closes the one-node case where `REPLICATED` is
    /// impossible by definition. Membership may change afterwards, so the
    /// publication-time check remains mandatory.
    pub(crate) fn require_durability_available(
        &self,
        durability: Durability,
    ) -> Result<(), Status> {
        if durability == Durability::Replicated && self.is_single_node()? {
            Err(mutation_status(MutationError::DurabilityUnavailable))
        } else {
            Ok(())
        }
    }

    async fn wait_for_mutation_capacity(&self, capacity: &'static str) {
        let wait = DistributedMutationBackpressureWait::start(capacity);
        self.store.wait_for_mutation_capacity().await;
        wait.complete();
    }

    pub(crate) fn current_program_placement(&self) -> Result<ClusterPlacement, Status> {
        self.placement()
    }

    pub(crate) fn program_mutation_context(
        &self,
    ) -> Result<keldra_store::ObjectMutationContext, Status> {
        self.serving.mutation_context()
    }

    pub(crate) fn program_replica_group(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
    ) -> Result<MutableRecordReplicaGroup, Status> {
        let placement = self.placement()?;
        MutableRecordReplicaGroup::select(
            PlacementKind::Object,
            placement.cluster_id(),
            &object_placement_key(tenant_id, bucket_id, exact_path),
            placement.placement_nodes(),
        )
        .ok_or_else(|| Status::unavailable("cluster has no active object owner"))
    }

    /// Make an executor-local ordinary blob recoverable under the cluster's
    /// failure-tolerant payload rule before an atomic visibility decision.
    pub(crate) async fn prepare_program_blob(&self, reference: &BlobRef) -> Result<(), Status> {
        self.prepare_program_blob_from_source(reference, self.local_node)
            .await
    }

    /// Make a blob sealed by one exact ACTIVE upload source recoverable before
    /// a built-in atomic transaction becomes visible on the nominated executor.
    pub(crate) async fn prepare_program_blob_from_source(
        &self,
        reference: &BlobRef,
        upload_source: NodeId,
    ) -> Result<(), Status> {
        let placement = self.placement()?;
        if !placement.active_node_ids().contains(&upload_source) {
            return Err(Status::failed_precondition(
                "built-in transaction upload source is not ACTIVE",
            ));
        }
        let evidence = self
            .prepare_payload(&placement, upload_source, reference, Durability::Replicated)
            .await?;
        self.payload
            .verify_on_path_coordinator(
                &placement,
                reference,
                Durability::Replicated,
                upload_source,
                &evidence,
            )
            .await
            .map_err(payload_status)?;
        if self.placement()?.fence() != placement.fence() {
            return Err(Status::unavailable(
                "payload placement changed during atomic preparation",
            ));
        }
        Ok(())
    }

    fn placement(&self) -> Result<ClusterPlacement, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))
    }

    async fn prepare_payload(
        &self,
        placement: &ClusterPlacement,
        upload_source: NodeId,
        reference: &BlobRef,
        durability: Durability,
    ) -> Result<crate::payload_distribution::PreparedPayloadEvidence, Status> {
        if upload_source == self.local_node {
            return self
                .payload
                .prepare_on_upload_source(placement, reference, durability)
                .await
                .map_err(payload_status);
        }
        let address = placement.address(upload_source).ok_or_else(|| {
            Status::unavailable(format!(
                "ACTIVE upload source {} has no peer address",
                upload_source.0
            ))
        })?;
        self.payload_peers
            .prepare_payload(upload_source, &address.0, reference, durability)
            .await
    }

    fn continue_payload_placement(&self, upload_source: NodeId, reference: BlobRef) {
        let distribution = self.clone();
        tokio::spawn(async move {
            let result = async {
                let placement = distribution.placement()?;
                distribution
                    .prepare_payload(
                        &placement,
                        upload_source,
                        &reference,
                        Durability::Replicated,
                    )
                    .await
                    .map(|_| ())
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(
                    upload_source = upload_source.0,
                    blob_length = reference.length,
                    %error,
                    "background payload placement did not complete"
                );
            }
        });
    }

    async fn wait_for_replicated_reference(
        &self,
        placement: &ClusterPlacement,
        reference: &BlobRef,
        evidence: &crate::payload_distribution::PreparedPayloadEvidence,
        coordinated: &CoordinatedObjectMutation,
    ) -> Result<(), Status> {
        self.wait_for_replicated_reference_evidence(
            placement,
            reference,
            evidence.artifacts(),
            coordinated,
        )
        .await
    }

    async fn wait_for_replicated_reference_evidence(
        &self,
        placement: &ClusterPlacement,
        reference: &BlobRef,
        evidence: &[NodePayloadEvidence],
        coordinated: &CoordinatedObjectMutation,
    ) -> Result<(), Status> {
        let Some(mutation) = coordinated.mutation.as_ref() else {
            return Ok(());
        };
        if !mutation
            .reference_deltas
            .iter()
            .any(|delta| delta.change > 0 && delta.blob == *reference)
        {
            return Ok(());
        }
        let owners = select_payload_placement(
            placement.cluster_id(),
            reference,
            self.erasure_profile,
            placement.placement_nodes(),
        )
        .replicated_reference_owners(evidence)
        .map_err(|error| payload_status(error.into()))?;
        self.references
            .wait_for_reference_effects(
                placement,
                mutation.stamp.source_id,
                mutation.stamp.source_journal_position,
                &owners,
                self.reference_acknowledgement_timeout,
            )
            .await
    }

    fn replica_group(
        &self,
        placement: &ClusterPlacement,
        key: &ObjectKey,
    ) -> Result<MutableRecordReplicaGroup, Status> {
        let (tenant_id, bucket_id) = self
            .store
            .resolve_bucket_ids(key.tenant(), key.bucket())
            .map_err(mutation_status)?;
        self.replica_group_stable(placement, tenant_id, bucket_id, key)
    }

    fn replica_group_stable(
        &self,
        placement: &ClusterPlacement,
        tenant_id: u64,
        bucket_id: u64,
        key: &ObjectKey,
    ) -> Result<MutableRecordReplicaGroup, Status> {
        if tenant_id == 0 || bucket_id == 0 {
            return Err(Status::invalid_argument(
                "stable tenant and bucket IDs must be non-zero",
            ));
        }
        let placement_key = object_placement_key(tenant_id, bucket_id, key.path());
        MutableRecordReplicaGroup::select(
            PlacementKind::Object,
            placement.cluster_id(),
            &placement_key,
            placement.placement_nodes(),
        )
        .ok_or_else(|| Status::unavailable("cluster has no active object owner"))
    }

    async fn replicate(
        &self,
        placement: &ClusterPlacement,
        group: &MutableRecordReplicaGroup,
        coordinated: &CoordinatedObjectMutation,
    ) -> Result<(), Status> {
        if let Some((source, offsets)) = self
            .replicate_without_settlement(placement, group, coordinated)
            .await?
            && let Err(error) = self
                .store
                .settle_source_journal_positions_if_contiguous(source, &offsets)
                .await
        {
            tracing::warn!(
                source = ?source,
                offsets = ?offsets,
                %error,
                "metadata quorum succeeded but direct source settlement failed"
            );
        }
        Ok(())
    }

    async fn replicate_without_settlement(
        &self,
        placement: &ClusterPlacement,
        group: &MutableRecordReplicaGroup,
        coordinated: &CoordinatedObjectMutation,
    ) -> Result<Option<(keldra_store::SourceId, Vec<u64>)>, Status> {
        let Some(mutation) = coordinated.mutation.as_ref() else {
            // The local command receipt proved an exact idempotent replay.
            return Ok(None);
        };
        let mut durable = Vec::with_capacity(group.replicas().len());
        let mut failures = Vec::new();
        match self.store.apply_object_mutation_replica(mutation).await {
            Ok(applied) if applied.version == coordinated.receipt.version => {
                durable.push(self.local_node);
            }
            Ok(_) => failures.push("local replica returned another version".into()),
            Err(error) => failures.push(format!("local replica: {error}")),
        }
        for node in group
            .replicas()
            .iter()
            .copied()
            .filter(|node| *node != self.local_node)
        {
            let address = placement.address(node).ok_or_else(|| {
                Status::unavailable(format!("ACTIVE node {} has no peer address", node.0))
            })?;
            match self
                .peers
                .apply_object_mutation(node, &address.0, mutation)
                .await
            {
                Ok(applied) if applied.version == coordinated.receipt.version => {
                    durable.push(node);
                }
                Ok(_) => failures.push(format!("node {} returned another version", node.0)),
                Err(error) => failures.push(format!("node {}: {error}", node.0)),
            }
        }
        if group.is_acknowledged_by(&durable) {
            return Ok(Some((
                mutation.stamp.source_id,
                mutation_journal_positions(mutation)?,
            )));
        }
        tracing::warn!(
            durable = durable.len(),
            required = group.required_acknowledgements(),
            failures = ?failures,
            "object mutation did not reach its complete-record quorum"
        );
        Err(Status::unavailable(format!(
            "object metadata reached {} of {} required replicas",
            durable.len(),
            group.required_acknowledgements()
        )))
    }

    async fn replicate_retained_version_delete(
        &self,
        placement: &ClusterPlacement,
        group: &MutableRecordReplicaGroup,
        coordinated: &CoordinatedRetainedVersionDelete,
    ) -> Result<(), Status> {
        let Some(mutation) = coordinated.mutation.as_ref() else {
            return Ok(());
        };
        let mut durable = Vec::with_capacity(group.replicas().len());
        let mut failures = Vec::new();
        match self
            .store
            .apply_retained_version_delete_replica(mutation)
            .await
        {
            Ok(applied) if applied.outcome == coordinated.outcome => durable.push(self.local_node),
            Ok(_) => failures.push("local replica returned another deletion outcome".into()),
            Err(error) => failures.push(format!("local replica: {error}")),
        }
        for node in group
            .replicas()
            .iter()
            .copied()
            .filter(|node| *node != self.local_node)
        {
            let address = placement.address(node).ok_or_else(|| {
                Status::unavailable(format!("ACTIVE node {} has no peer address", node.0))
            })?;
            match self
                .peers
                .apply_retained_version_delete(node, &address.0, mutation)
                .await
            {
                Ok(applied) if applied.outcome == coordinated.outcome => durable.push(node),
                Ok(_) => {
                    failures.push(format!("node {} returned another deletion outcome", node.0))
                }
                Err(error) => failures.push(format!("node {}: {error}", node.0)),
            }
        }
        if group.is_acknowledged_by(&durable) {
            if let Err(error) = self
                .store
                .settle_source_journal_position_if_contiguous(
                    mutation.stamp.source_id,
                    mutation.stamp.source_journal_position,
                )
                .await
            {
                tracing::warn!(
                    source = ?mutation.stamp.source_id,
                    offset = mutation.stamp.source_journal_position,
                    %error,
                    "retained-version metadata quorum succeeded but direct source settlement failed"
                );
            }
            return Ok(());
        }
        tracing::warn!(
            durable = durable.len(),
            required = group.required_acknowledgements(),
            failures = ?failures,
            "retained-version deletion did not reach its metadata quorum"
        );
        Err(Status::unavailable(format!(
            "object metadata reached {} of {} required replicas",
            durable.len(),
            group.required_acknowledgements()
        )))
    }
}

async fn stage_distributed_put(
    store: &Store,
    request: PutRequest,
) -> Result<PublishRequest, Status> {
    let PutRequest {
        key,
        bytes,
        content_type,
        mode,
        command_id,
        durability,
    } = request;
    let blob = store.stage_blob(&bytes).await.map_err(mutation_status)?;
    Ok(PublishRequest {
        key,
        blob,
        content_type,
        mode,
        command_id,
        durability,
    })
}

fn operation_key(operation: &BatchOperation) -> &ObjectKey {
    match operation {
        BatchOperation::Put(request) => &request.key,
        BatchOperation::Publish(request) => &request.key,
        BatchOperation::Clone(request) => &request.destination,
        BatchOperation::Delete(request) => &request.key,
    }
}

pub(super) fn mutation_journal_positions(
    mutation: &keldra_store::ObjectMutation,
) -> Result<Vec<u64>, Status> {
    let count = 1 + mutation
        .alias_snapshot
        .as_ref()
        .map_or(0, |snapshot| snapshot.registry.aliases.len());
    (0..count)
        .map(|offset| {
            mutation
                .stamp
                .source_journal_position
                .checked_add(offset as u64)
                .ok_or_else(|| Status::data_loss("object mutation journal range is exhausted"))
        })
        .collect()
}

fn object_placement_key(tenant_id: u64, bucket_id: u64, path: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + path.len());
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&bucket_id.to_be_bytes());
    key.extend_from_slice(path.as_bytes());
    key
}

fn mutation_status(error: MutationError) -> Status {
    match error {
        MutationError::PreconditionFailed { .. }
        | MutationError::Immutable
        | MutationError::ImmutablePolicyRequired
        | MutationError::ProgramConcurrencyViolation
        | MutationError::InvalidCommandId
        | MutationError::InvalidPolicy(_)
        | MutationError::InvalidObjectMutation(_)
        | MutationError::ObjectMutationConflict
        | MutationError::ObjectMutationLineageGap { .. }
        | MutationError::ObjectMutationSibling { .. }
        | MutationError::ObjectVersioningNotEnabled
        | MutationError::ObjectHasInboundAliases
        | MutationError::CurrentTombstoneCannotBeDeleted => {
            Status::failed_precondition(error.to_string())
        }
        MutationError::IdempotencyConflict => Status::already_exists(error.to_string()),
        MutationError::SourceJournalCapacity
        | MutationError::DurabilityUnavailable
        | MutationError::ReceiptCapacity => Status::unavailable(error.to_string()),
        MutationError::ReceiptTooLarge { .. }
        | MutationError::SourceJournalRecordTooLarge { .. }
        | MutationError::SourceJournalTransitionTooLarge { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        _ => Status::internal(error.to_string()),
    }
}

fn mutation_capacity_kind(error: &Status) -> Option<&'static str> {
    if error.message() == MutationError::ReceiptCapacity.to_string() {
        Some("receipt")
    } else if error.message() == MutationError::SourceJournalCapacity.to_string() {
        Some("source_journal")
    } else {
        None
    }
}

fn metadata_completion_join(error: tokio::task::JoinError) -> Status {
    Status::internal(format!("object metadata completion task failed: {error}"))
}

async fn complete_metadata<T, F>(completion: F) -> Result<T, Status>
where
    T: Send + 'static,
    F: Future<Output = Result<T, Status>> + Send + 'static,
{
    tokio::spawn(completion)
        .await
        .map_err(metadata_completion_join)?
}

fn payload_status(error: PayloadDistributionError) -> Status {
    match error {
        PayloadDistributionError::UploadSourceMissing
        | PayloadDistributionError::InvalidEvidence
        | PayloadDistributionError::Readiness(_)
        | PayloadDistributionError::SourceLocationUnavailable => {
            Status::failed_precondition(error.to_string())
        }
        PayloadDistributionError::UploadSourceCorrupt => Status::data_loss(error.to_string()),
        PayloadDistributionError::OwnerAddressMissing { .. }
        | PayloadDistributionError::OwnerArtifactMissing { .. }
        | PayloadDistributionError::OwnerArtifactThreshold { .. }
        | PayloadDistributionError::Peer { .. }
        | PayloadDistributionError::Encoding(_)
        | PayloadDistributionError::CompleteSource(_) => Status::unavailable(error.to_string()),
        PayloadDistributionError::Store(_) | PayloadDistributionError::Erasure(_) => {
            Status::internal(error.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keldra_store::{
        LogicalRecordMutationContext, LogicalRecordValue, PlacementLogId, StorageTenantId,
        StoreOptions, VersionId,
    };
    use tempfile::TempDir;

    #[test]
    fn object_placement_key_is_stable_and_unambiguous() {
        assert_eq!(
            object_placement_key(1, 2, "/a"),
            [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, b'/', b'a']
        );
        assert_ne!(
            object_placement_key(1, 23, "/a"),
            object_placement_key(12, 3, "/a")
        );
    }

    #[tokio::test]
    async fn staged_bulk_put_uses_the_ordinary_publish_path() {
        let (_temporary, store) = store().await;
        let key = ObjectKey::new("tenant", "bucket", "bulk/value").unwrap();
        let publish = stage_distributed_put(
            &store,
            PutRequest {
                key: key.clone(),
                bytes: b"ordinary payload".to_vec(),
                content_type: Some("application/octet-stream".into()),
                mode: keldra_store::PutMode::PutIfAbsent,
                command_id: Some("bulk-publish".into()),
                durability: Durability::Local,
            },
        )
        .await
        .unwrap();

        assert!(store.get(&key).await.unwrap().is_none());
        store.publish(publish).await.unwrap();
        assert_eq!(
            store.get(&key).await.unwrap().unwrap().bytes,
            b"ordinary payload"
        );
    }

    #[tokio::test]
    async fn staged_bulk_put_preserves_replicated_durability_before_metadata() {
        let (_temporary, store) = store().await;
        let key = ObjectKey::new("tenant", "bucket", "bulk/replicated").unwrap();
        let publish = stage_distributed_put(
            &store,
            PutRequest {
                key: key.clone(),
                bytes: b"replicated payload".to_vec(),
                content_type: None,
                mode: keldra_store::PutMode::PutIfAbsent,
                command_id: Some("bulk-replicated".into()),
                durability: Durability::Replicated,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            store.publish(publish).await.unwrap_err(),
            MutationError::DurabilityUnavailable
        );
        assert!(store.get(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn metadata_completion_outlives_cancelled_request_future() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        let request = tokio::spawn(complete_metadata(async move {
            let _ = started_tx.send(());
            let _ = release_rx.await;
            let _ = completed_tx.send(());
            Ok::<_, Status>(())
        }));
        started_rx.await.unwrap();

        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), completed_rx)
            .await
            .expect("detached metadata completion timed out")
            .expect("detached metadata completion stopped");
    }

    async fn store() -> (TempDir, Store) {
        let temporary = TempDir::new().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        install_test_identity(&store);
        (temporary, store)
    }

    fn install_test_identity(store: &Store) {
        let tenant = StorageTenantId::parse("tenant").unwrap();
        for (record_version, typed_value) in [
            (
                101,
                LogicalRecordValue::TenantNameClaim {
                    storage_tenant: tenant,
                    tenant_id: 1,
                },
            ),
            (
                102,
                LogicalRecordValue::BucketNameClaim {
                    tenant_id: 1,
                    bucket: "bucket".into(),
                    bucket_id: 1,
                },
            ),
        ] {
            let mutation = store
                .construct_logical_record_mutation(
                    typed_value,
                    LogicalRecordMutationContext {
                        record_version: VersionId(record_version),
                        active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                        serving_fence_term: 1,
                    },
                )
                .unwrap();
            store.commit_logical_record_mutation(&mutation).unwrap();
        }
    }
}
