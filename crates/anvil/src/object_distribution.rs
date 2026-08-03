//! Cluster coordination for exact-path object mutations.
//!
//! The storage crate evaluates and applies typed mutations. This module adds
//! only the placement and acknowledgement policy from ANVIL-0010: rank zero
//! coordinates one exact path and the first three HRW owners hold complete
//! logical replicas. Ownership itself is never persisted here or in Raft.

mod quorum_read;
mod serving_read;

pub(crate) use quorum_read::select_object_snapshot_quorum;

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{
    BatchOperation, BlobRef, CoordinatedObjectMutation, CoordinatedRetainedVersionDelete,
    DeleteRetainedVersionOutcome, Durability, ErasureProfile, MutationError, MutationReceipt,
    ObjectKey, ObjectMutationGovernance, PublishRequest, PutRequest, Store, VersionId,
};
use tonic::Status;

use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::DataPeerTransport;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::payload_distribution::{
    PayloadDistribution, PayloadDistributionError, PayloadPeerTransport,
};
use crate::placement::PlacementKind;
use crate::serving_fence::ServingAuthority;

#[derive(Clone)]
pub(crate) struct ObjectDistribution {
    local_node: NodeId,
    store: Store,
    decisions: DecisionRaft,
    serving: ServingAuthority,
    peers: DataPeerTransport,
    payload: PayloadDistribution,
    payload_peers: PayloadPeerTransport,
}

impl ObjectDistribution {
    pub(crate) fn new(
        local_node: NodeId,
        store: Store,
        decisions: DecisionRaft,
        serving: ServingAuthority,
        peers: DataPeerTransport,
        erasure_profile: ErasureProfile,
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
        }
    }

    pub(crate) const fn local_node(&self) -> NodeId {
        self.local_node
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

    pub(crate) async fn publish_from_source_with_governance(
        &self,
        request: PublishRequest,
        upload_source: NodeId,
        governance: ObjectMutationGovernance,
    ) -> Result<MutationReceipt, Status> {
        governance.validate().map_err(mutation_status)?;
        let placement = self.placement()?;
        if placement.active_node_ids().len() == 1 {
            if upload_source != self.local_node {
                return Err(Status::failed_precondition(
                    "the ready capability names another upload source",
                ));
            }
            return self
                .store
                .mutate_with_governance(BatchOperation::Publish(request), governance)
                .await
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

        let context = self
            .reconcile_before_mutation_stable(
                &request.key,
                governance.tenant_id,
                governance.bucket_id,
                placement.fence(),
            )
            .await?;
        let durability = request.durability;
        let coordinated = self
            .store
            .coordinate_distributed_publish_with_governance(request, governance, context)
            .await
            .map_err(mutation_status)?;
        self.replicate(&placement, &group, &coordinated).await?;

        if durability == Durability::Local {
            self.continue_payload_placement(upload_source, reference);
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
        self.mutate_with_governance(operation, governance).await
    }

    pub(crate) async fn mutate_with_governance(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
    ) -> Result<MutationReceipt, Status> {
        governance.validate().map_err(mutation_status)?;
        let placement = self.placement()?;
        if placement.active_node_ids().len() == 1 {
            return self
                .store
                .mutate_with_governance(operation, governance)
                .await
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
                    .publish_from_source_with_governance(publish, self.local_node, governance)
                    .await;
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

        let context = self
            .reconcile_before_mutation_stable(
                key,
                governance.tenant_id,
                governance.bucket_id,
                placement.fence(),
            )
            .await?;
        let coordinated = self
            .store
            .coordinate_object_mutation_with_governance(operation, governance, context)
            .await
            .map_err(mutation_status)?;
        self.replicate(&placement, &group, &coordinated).await?;
        Ok(coordinated.receipt)
    }

    pub(crate) async fn delete_retained_version_with_governance(
        &self,
        key: &ObjectKey,
        version: VersionId,
        governance: ObjectMutationGovernance,
    ) -> Result<DeleteRetainedVersionOutcome, Status> {
        governance.validate().map_err(mutation_status)?;
        let placement = self.placement()?;
        if placement.active_node_ids().len() == 1 {
            return self
                .store
                .coordinate_retained_version_delete(
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
        let context = self
            .reconcile_before_mutation_stable(
                key,
                governance.tenant_id,
                governance.bucket_id,
                placement.fence(),
            )
            .await?;
        let coordinated = self
            .store
            .coordinate_retained_version_delete(key, version, governance, context)
            .await
            .map_err(mutation_status)?;
        self.replicate_retained_version_delete(&placement, &group, &coordinated)
            .await?;
        Ok(coordinated.outcome)
    }

    /// Preserve the released one-node physical WriteBatch fast path. A
    /// multi-node batch remains a collection of independent exact-path
    /// mutations and is never presented as an atomic transaction.
    pub(crate) async fn mutate_many(
        &self,
        operations: Vec<BatchOperation>,
    ) -> Vec<Result<MutationReceipt, Status>> {
        match self.is_single_node() {
            Ok(true) => self
                .store
                .bulk_write(operations)
                .await
                .into_iter()
                .map(|outcome| outcome.result.map_err(mutation_status))
                .collect(),
            Ok(false) => {
                let mut outcomes = Vec::with_capacity(operations.len());
                for operation in operations {
                    outcomes.push(self.mutate(operation).await);
                }
                outcomes
            }
            Err(error) => (0..operations.len()).map(|_| Err(error.clone())).collect(),
        }
    }

    pub(crate) async fn mutate_many_with_governance(
        &self,
        operations: Vec<(BatchOperation, ObjectMutationGovernance)>,
    ) -> Vec<Result<MutationReceipt, Status>> {
        let mut outcomes = Vec::with_capacity(operations.len());
        for (operation, governance) in operations {
            outcomes.push(self.mutate_with_governance(operation, governance).await);
        }
        outcomes
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

    pub(crate) fn is_single_node(&self) -> Result<bool, Status> {
        Ok(self.placement()?.active_node_ids().len() == 1)
    }

    pub(crate) fn current_program_placement(&self) -> Result<ClusterPlacement, Status> {
        self.placement()
    }

    pub(crate) fn program_mutation_context(
        &self,
    ) -> Result<anvil_store::ObjectMutationContext, Status> {
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
        let placement = self.placement()?;
        let evidence = self
            .payload
            .prepare_on_upload_source(&placement, reference, Durability::Replicated)
            .await
            .map_err(payload_status)?;
        self.payload
            .verify_on_path_coordinator(
                &placement,
                reference,
                Durability::Replicated,
                self.local_node,
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
        let Some(mutation) = coordinated.mutation.as_ref() else {
            // The local command receipt proved an exact idempotent replay.
            return Ok(());
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
            return Ok(());
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
        BatchOperation::Delete(request) => &request.key,
    }
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
        | MutationError::IdempotencyConflict
        | MutationError::InvalidCommandId
        | MutationError::InvalidPolicy(_)
        | MutationError::InvalidObjectMutation(_)
        | MutationError::ObjectMutationConflict
        | MutationError::ObjectMutationLineageGap { .. }
        | MutationError::ObjectMutationSibling { .. }
        | MutationError::ObjectVersioningNotEnabled
        | MutationError::CurrentTombstoneCannotBeDeleted => {
            Status::failed_precondition(error.to_string())
        }
        MutationError::SourceJournalCapacity
        | MutationError::DurabilityUnavailable
        | MutationError::ReceiptCapacity => Status::unavailable(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
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
        | PayloadDistributionError::Encoding(_) => Status::unavailable(error.to_string()),
        PayloadDistributionError::Store(_) | PayloadDistributionError::Erasure(_) => {
            Status::internal(error.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anvil_store::{
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
                mode: anvil_store::PutMode::PutIfAbsent,
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
                mode: anvil_store::PutMode::PutIfAbsent,
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
