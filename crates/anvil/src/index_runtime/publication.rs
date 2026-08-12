//! Narrow ordinary-object publication boundary for index generations.

use std::sync::{Arc, OnceLock};

use anvil_consensus::NodeId;
use anvil_store::{
    BatchOperation, BlobRef, DefinitionKind, DefinitionMutationIntent, DeleteRequest,
    DeleteRetainedVersionOutcome, Durability, ObjectKey, ObjectVersioning, Precondition,
    PublishRequest, PutMode, Store, VersionId,
};
use tonic::Status;
use tracing::Instrument;

use crate::bucket_governance::BucketGovernance;
use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_placement::ClusterPlacement;
use crate::object_distribution::ObjectDistribution;

use super::placement::{IndexIdentity, IndexPlacement};

const INDEX_ARTIFACT_CONTENT_TYPE: &str = "application/vnd.anvil.index-artifact";
const ACCOUNTING_ARTIFACT_CONTENT_TYPE: &str = "application/vnd.anvil.accounting+json";
pub(crate) const MAX_INDEX_ARTIFACT_BATCH_ITEMS: usize = 1_000;
pub(crate) const MAX_INDEX_ARTIFACT_BATCH_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DefinitionVersionGuard {
    pub kind: DefinitionKind,
    pub exact_path: String,
    pub expected_version: VersionId,
}

impl DefinitionVersionGuard {
    fn key(&self, storage_tenant: &str, bucket: &str) -> Result<ObjectKey, Status> {
        ObjectKey::new(storage_tenant, bucket, &self.exact_path)
            .map_err(|error| Status::invalid_argument(error.to_string()))
    }

    fn validate(
        &self,
        request: &IndexArtifactPublish,
        artifact_kind: ArtifactPathKind,
    ) -> Result<(), Status> {
        if self.expected_version.0 == 0 {
            return Err(Status::invalid_argument(
                "guarded definition version must be non-zero",
            ));
        }
        let valid_path = match (artifact_kind, self.kind) {
            (ArtifactPathKind::Current, DefinitionKind::Index) => {
                index_definition_name(&self.exact_path).is_some()
            }
            (ArtifactPathKind::AccountingMutable, DefinitionKind::Accounting) => {
                crate::accounting::definition_path(request.index_id)
                    .ok()
                    .as_deref()
                    == Some(self.exact_path.as_str())
            }
            _ => false,
        };
        if !valid_path {
            return Err(Status::invalid_argument(
                "guarded publication does not name its exact definition path",
            ));
        }
        self.key(&request.storage_tenant, &request.bucket)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IndexArtifactPublish {
    pub storage_tenant: String,
    pub bucket: String,
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub index_id: u64,
    pub exact_path: String,
    pub blob: BlobRef,
    /// Absence creates an immutable generation artifact/current pointer.
    /// Presence replaces only the current pointer at this exact version.
    pub expected_version: Option<VersionId>,
    pub command_id: String,
    /// Exact authoritative definition revision which must remain live while
    /// the mutable current pointer or accounting rollup is committed.
    pub definition_guard: Option<DefinitionVersionGuard>,
    /// Trusted typed evidence for an ordinary definition mutation. Generic
    /// generation, current, rollup, and traffic-source artifacts leave this
    /// absent.
    pub definition_intent: Option<DefinitionMutationIntent>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexArtifactOutcome {
    pub version: VersionId,
    pub replayed: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexArtifactDelete {
    pub storage_tenant: String,
    pub bucket: String,
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub index_id: u64,
    pub exact_path: String,
    pub expected_version: VersionId,
    pub command_id: String,
    pub definition_intent: Option<DefinitionMutationIntent>,
}

impl IndexArtifactDelete {
    pub(crate) fn key(&self) -> Result<ObjectKey, Status> {
        ObjectKey::new(&self.storage_tenant, &self.bucket, &self.exact_path)
            .map_err(|error| Status::invalid_argument(error.to_string()))
    }

    fn validate(&self) -> Result<ArtifactPathKind, Status> {
        if self.tenant_id == 0
            || self.bucket_id == 0
            || self.index_id == 0
            || self.expected_version.0 == 0
            || self.command_id.is_empty()
        {
            return Err(Status::invalid_argument(
                "index artifact identity, expected version, and command ID must be non-empty",
            ));
        }
        let kind = parse_artifact_path(&self.exact_path, self.index_id)?;
        validate_definition_intent(
            kind,
            &self.exact_path,
            self.index_id,
            self.definition_intent,
        )?;
        Ok(kind)
    }
}

impl IndexArtifactPublish {
    pub(crate) fn key(&self) -> Result<ObjectKey, Status> {
        ObjectKey::new(&self.storage_tenant, &self.bucket, &self.exact_path)
            .map_err(|error| Status::invalid_argument(error.to_string()))
    }

    fn validate(&self) -> Result<ArtifactPathKind, Status> {
        if self.tenant_id == 0
            || self.bucket_id == 0
            || self.index_id == 0
            || self.blob.length == 0
            || self.command_id.is_empty()
        {
            return Err(Status::invalid_argument(
                "index artifact identity, bytes, and command ID must be non-empty",
            ));
        }
        let kind = parse_artifact_path(&self.exact_path, self.index_id)?;
        validate_definition_intent(
            kind,
            &self.exact_path,
            self.index_id,
            self.definition_intent,
        )?;
        match (kind, self.expected_version) {
            (ArtifactPathKind::Current, Some(VersionId(0))) => Err(Status::invalid_argument(
                "index current-pointer expected version must be non-zero",
            )),
            (ArtifactPathKind::AccountingMutable, Some(VersionId(0))) => Err(
                Status::invalid_argument("accounting artifact expected version must be non-zero"),
            ),
            (ArtifactPathKind::Current | ArtifactPathKind::AccountingMutable, _)
            | (ArtifactPathKind::Immutable, None) => Ok(kind),
            (ArtifactPathKind::Immutable, Some(_)) => Err(Status::invalid_argument(
                "immutable index generation artifacts cannot be replaced",
            )),
        }?;
        let guard_required = kind == ArtifactPathKind::Current
            || (kind == ArtifactPathKind::AccountingMutable
                && crate::accounting::current_path(self.index_id)
                    .ok()
                    .as_deref()
                    == Some(self.exact_path.as_str()));
        match (guard_required, self.definition_guard.as_ref()) {
            (true, Some(guard)) => guard.validate(self, kind)?,
            (false, None) => {}
            (true, None) => {
                return Err(Status::invalid_argument(
                    "mutable index/accounting publication requires an exact definition guard",
                ));
            }
            (false, Some(_)) => {
                return Err(Status::invalid_argument(
                    "definition guards are valid only for current index/accounting publication",
                ));
            }
        }
        Ok(kind)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactPathKind {
    Current,
    Immutable,
    AccountingMutable,
}

/// Destination-side late-bound handler on the mandatory-mTLS listener.
#[tonic::async_trait]
pub(crate) trait IndexArtifactPublication: Send + Sync + 'static {
    async fn publish(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        request: IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status>;

    async fn publish_many(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactOutcome>, Status>;

    async fn commit_guarded(
        &self,
        authenticated_definition_coordinator: NodeId,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        request: IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status>;

    async fn delete(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        request: IndexArtifactDelete,
    ) -> Result<IndexArtifactOutcome, Status>;
}

#[derive(Clone, Default)]
pub(crate) struct LateBoundIndexArtifactPublication {
    inner: Arc<OnceLock<Arc<dyn IndexArtifactPublication>>>,
}

impl LateBoundIndexArtifactPublication {
    pub(crate) fn install(
        &self,
        handler: Arc<dyn IndexArtifactPublication>,
    ) -> Result<(), Arc<dyn IndexArtifactPublication>> {
        self.inner.set(handler)
    }
}

#[tonic::async_trait]
impl IndexArtifactPublication for LateBoundIndexArtifactPublication {
    async fn publish(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        request: IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status> {
        let handler = self
            .inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("index artifact publisher is not ready"))?;
        handler
            .publish(authenticated_builder, placement, request)
            .await
    }

    async fn publish_many(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactOutcome>, Status> {
        let handler = self
            .inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("index artifact publisher is not ready"))?;
        handler
            .publish_many(authenticated_builder, placement, requests)
            .await
    }

    async fn delete(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        request: IndexArtifactDelete,
    ) -> Result<IndexArtifactOutcome, Status> {
        let handler = self
            .inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("index artifact publisher is not ready"))?;
        handler
            .delete(authenticated_builder, placement, request)
            .await
    }

    async fn commit_guarded(
        &self,
        authenticated_definition_coordinator: NodeId,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        request: IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status> {
        let handler = self
            .inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("index artifact publisher is not ready"))?;
        handler
            .commit_guarded(
                authenticated_definition_coordinator,
                authenticated_builder,
                placement,
                request,
            )
            .await
    }
}

/// Validates the builder/fence/path and enters the existing ordinary object
/// coordinator. It owns no bytes or metadata persistence of its own.
#[derive(Clone)]
pub(crate) struct IndexArtifactCoordinator {
    store: Store,
    objects: ObjectDistribution,
    governance: BucketGovernance,
    peers: ClusterPeerTransport,
}

impl IndexArtifactCoordinator {
    pub(crate) fn new(
        store: Store,
        objects: ObjectDistribution,
        governance: BucketGovernance,
        peers: ClusterPeerTransport,
    ) -> Self {
        Self {
            store,
            objects,
            governance,
            peers,
        }
    }

    fn validate_index_builder(
        &self,
        authenticated_builder: NodeId,
        placement: &ClusterPlacement,
        identity: IndexIdentity,
    ) -> Result<(), Status> {
        let assignment = IndexPlacement::derive(identity, placement)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if assignment.builder() != authenticated_builder {
            return Err(Status::permission_denied(
                "index artifact caller is not the current weighted-HRW builder",
            ));
        }
        Ok(())
    }

    fn validate_builder(
        &self,
        authenticated_builder: NodeId,
        placement: &ClusterPlacement,
        identity: IndexIdentity,
        key: &ObjectKey,
    ) -> Result<(), Status> {
        self.validate_index_builder(authenticated_builder, placement, identity)?;
        if self
            .objects
            .routing_target_stable(key, identity.tenant_id(), identity.bucket_id())?
            .is_some()
        {
            return Err(Status::failed_precondition(
                "index artifact request reached a node that is not its object coordinator",
            ));
        }
        Ok(())
    }

    async fn publish_guarded(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        request: IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status> {
        request.validate()?;
        // The public/peer middleware already admits routed calls. Builders can
        // also publish locally, so explicitly retain the same existing
        // membership-cutover permit across the definition lock and artifact
        // mutation in that case.
        let _permit = self.objects.enter_mutation()?;
        self.require_fence(placement.fence())?;
        let identity = IndexIdentity::new(request.tenant_id, request.bucket_id, request.index_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.validate_index_builder(authenticated_builder, &placement, identity)?;
        let guard = request
            .definition_guard
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("guarded publication has no guard"))?;
        let definition_key = guard.key(&request.storage_tenant, &request.bucket)?;
        let definition_coordinator = self.objects.object_coordinator_stable(
            &placement,
            &definition_key,
            request.tenant_id,
            request.bucket_id,
        )?;
        if definition_coordinator != self.objects.local_node() {
            return Err(Status::failed_precondition(
                "guarded publication did not reach the definition-path coordinator",
            ));
        }

        let guarded_definition_key = definition_key.clone();
        self.store
            .with_ordinary_object_path_lock(&definition_key, move || async move {
                self.require_current_definition(&placement, &guarded_definition_key, &request)
                    .await?;
                let artifact_key = request.key()?;
                let artifact_coordinator = self.objects.object_coordinator_stable(
                    &placement,
                    &artifact_key,
                    request.tenant_id,
                    request.bucket_id,
                )?;
                let outcome = if artifact_coordinator == self.objects.local_node() {
                    self.publish_unguarded(authenticated_builder, placement.clone(), request)
                        .await?
                } else {
                    let address = placement.address(artifact_coordinator).ok_or_else(|| {
                        Status::unavailable(format!(
                            "ACTIVE artifact coordinator {} has no peer address",
                            artifact_coordinator.0
                        ))
                    })?;
                    self.peers
                        .commit_guarded_index_artifact(
                            artifact_coordinator,
                            &address.0,
                            placement.fence(),
                            authenticated_builder,
                            &request,
                        )
                        .await?
                };
                self.require_fence(placement.fence())?;
                Ok(outcome)
            })
            .await
    }

    async fn publish_unguarded(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        request: IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status> {
        let kind = request.validate()?;
        let identity = IndexIdentity::new(request.tenant_id, request.bucket_id, request.index_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let key = request.key()?;
        let governance = self
            .governance
            .resolve(&request.storage_tenant, &request.bucket)
            .await?;
        if (governance.tenant_id, governance.bucket_id)
            != (identity.tenant_id(), identity.bucket_id())
        {
            return Err(Status::failed_precondition(
                "index artifact mutable names no longer bind the supplied stable IDs",
            ));
        }
        self.validate_builder(authenticated_builder, &placement, identity, &key)?;
        let mode = match (kind, request.expected_version) {
            (ArtifactPathKind::Current | ArtifactPathKind::AccountingMutable, Some(version)) => {
                PutMode::PutIfVersion(version)
            }
            (
                ArtifactPathKind::Current
                | ArtifactPathKind::Immutable
                | ArtifactPathKind::AccountingMutable,
                None,
            ) => PutMode::PutIfAbsent,
            (ArtifactPathKind::Immutable, Some(_)) => unreachable!("validated above"),
        };
        let content_type = match kind {
            ArtifactPathKind::AccountingMutable => ACCOUNTING_ARTIFACT_CONTENT_TYPE,
            ArtifactPathKind::Current | ArtifactPathKind::Immutable => INDEX_ARTIFACT_CONTENT_TYPE,
        };
        let durability = artifact_durability(kind, placement.placement_nodes().len());
        let receipt = self
            .objects
            .publish_from_source_with_governance_and_definition_intent(
                PublishRequest {
                    key,
                    blob: request.blob,
                    content_type: Some(content_type.into()),
                    mode,
                    command_id: Some(request.command_id),
                    durability,
                },
                authenticated_builder,
                governance,
                request.definition_intent,
            )
            .await?;
        Ok(IndexArtifactOutcome {
            version: receipt.version,
            replayed: receipt.replayed,
        })
    }

    async fn publish_immutable_many(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactOutcome>, Status> {
        validate_immutable_batch(&requests)?;
        let first = &requests[0];
        let identity = IndexIdentity::new(first.tenant_id, first.bucket_id, first.index_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.validate_index_builder(authenticated_builder, &placement, identity)?;
        let governance = self
            .governance
            .resolve(&first.storage_tenant, &first.bucket)
            .await?;
        if (governance.tenant_id, governance.bucket_id)
            != (identity.tenant_id(), identity.bucket_id())
        {
            return Err(Status::failed_precondition(
                "index artifact mutable names no longer bind the supplied stable IDs",
            ));
        }
        let first_key = first.key()?;
        let group = self.objects.object_replica_group_stable(
            &placement,
            &first_key,
            first.tenant_id,
            first.bucket_id,
        )?;
        if group.coordinator() != self.objects.local_node() {
            return Err(Status::failed_precondition(
                "grouped index artifacts reached the wrong object coordinator",
            ));
        }
        for request in &requests[1..] {
            let key = request.key()?;
            let candidate = self.objects.object_replica_group_stable(
                &placement,
                &key,
                request.tenant_id,
                request.bucket_id,
            )?;
            if candidate != group {
                return Err(Status::invalid_argument(
                    "grouped index artifacts span metadata replica groups",
                ));
            }
        }
        let durability = artifact_durability(
            ArtifactPathKind::Immutable,
            placement.placement_nodes().len(),
        );
        let publishes = requests
            .into_iter()
            .map(|request| {
                Ok(PublishRequest {
                    key: request.key()?,
                    blob: request.blob,
                    content_type: Some(INDEX_ARTIFACT_CONTENT_TYPE.into()),
                    mode: PutMode::PutIfAbsent,
                    command_id: Some(request.command_id),
                    durability,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        self.objects
            .publish_many_from_source_with_governance(
                publishes,
                authenticated_builder,
                governance,
                placement,
            )
            .await?
            .into_iter()
            .map(|outcome| {
                outcome.map(|receipt| IndexArtifactOutcome {
                    version: receipt.version,
                    replayed: receipt.replayed,
                })
            })
            .collect()
    }

    async fn require_current_definition(
        &self,
        placement: &ClusterPlacement,
        key: &ObjectKey,
        request: &IndexArtifactPublish,
    ) -> Result<(), Status> {
        let expected = request
            .definition_guard
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("guarded publication has no guard"))?;
        let current = self
            .objects
            .guarded_current_object_snapshot_stable(
                key,
                request.tenant_id,
                request.bucket_id,
                expected.expected_version,
            )
            .await?;
        self.require_fence(placement.fence())?;
        require_guarded_definition(current.as_ref(), expected.expected_version)
    }

    fn require_fence(&self, expected: anvil_store::PlacementLogId) -> Result<(), Status> {
        if self.objects.current_program_placement()?.fence() != expected {
            return Err(Status::unavailable(
                "index placement changed during guarded publication",
            ));
        }
        Ok(())
    }
}

/// Builder-side router. The builder owns orchestration, while every artifact
/// still enters the ordinary coordinator selected for its exact object path.
struct GroupedPublishTelemetry {
    span: tracing::Span,
    started: std::time::Instant,
    requested_items: u64,
    requested_bytes: u64,
    groups: u64,
    batches: u64,
    local_batches: u64,
    remote_batches: u64,
    attempted_items: u64,
    attempted_bytes: u64,
    finished: bool,
}

impl GroupedPublishTelemetry {
    fn start(requests: &[IndexArtifactPublish]) -> Self {
        let first = &requests[0];
        let requested_items = requests.len() as u64;
        let requested_bytes = requests.iter().fold(0_u64, |total, request| {
            total.saturating_add(request.blob.length)
        });
        let span = tracing::info_span!(
            "anvil.index.grouped_publish",
            index.id = first.index_id,
            tenant.id = first.tenant_id,
            bucket.id = first.bucket_id,
            publish.requested_items = requested_items,
            publish.requested_bytes = requested_bytes,
            publish.groups = tracing::field::Empty,
            publish.batches = tracing::field::Empty,
            publish.local_batches = tracing::field::Empty,
            publish.remote_batches = tracing::field::Empty,
            publish.attempted_items = tracing::field::Empty,
            publish.attempted_bytes = tracing::field::Empty,
            publish.elapsed_seconds = tracing::field::Empty,
            publish.outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        span.in_scope(|| {
            tracing::info!(
                operation = "index_artifact_grouped_publish",
                counter.anvil_index_grouped_publish_active = 1_i64,
                monotonic_counter.anvil_index_grouped_publish_attempts_total = 1_u64,
                "grouped index artifact publication started"
            );
        });
        Self {
            span,
            started: std::time::Instant::now(),
            requested_items,
            requested_bytes,
            groups: 0,
            batches: 0,
            local_batches: 0,
            remote_batches: 0,
            attempted_items: 0,
            attempted_bytes: 0,
            finished: false,
        }
    }

    fn record_batch(&mut self, local: bool, batch: &[(usize, IndexArtifactPublish)]) {
        self.batches = self.batches.saturating_add(1);
        if local {
            self.local_batches = self.local_batches.saturating_add(1);
        } else {
            self.remote_batches = self.remote_batches.saturating_add(1);
        }
        self.attempted_items = self.attempted_items.saturating_add(batch.len() as u64);
        self.attempted_bytes = self.attempted_bytes.saturating_add(
            batch
                .iter()
                .map(|(_, request)| request.blob.length)
                .sum::<u64>(),
        );
    }

    fn finish(&mut self, failed: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        let elapsed_seconds = self.started.elapsed().as_secs_f64();
        let outcome = if failed { "failed" } else { "completed" };
        self.span.record("publish.groups", self.groups);
        self.span.record("publish.batches", self.batches);
        self.span
            .record("publish.local_batches", self.local_batches);
        self.span
            .record("publish.remote_batches", self.remote_batches);
        self.span
            .record("publish.attempted_items", self.attempted_items);
        self.span
            .record("publish.attempted_bytes", self.attempted_bytes);
        self.span.record("publish.elapsed_seconds", elapsed_seconds);
        self.span.record("publish.outcome", outcome);
        self.span
            .record("otel.status_code", if failed { "error" } else { "ok" });
        self.span.in_scope(|| {
            tracing::info!(
                operation = "index_artifact_grouped_publish",
                counter.anvil_index_grouped_publish_active = -1_i64,
                "grouped index artifact publication released"
            );
            tracing::info!(
                operation = "index_artifact_grouped_publish",
                publish.outcome = outcome,
                monotonic_counter.anvil_index_grouped_publish_failures_total = u64::from(failed),
                monotonic_counter.anvil_index_grouped_publish_batches_total = self.batches,
                monotonic_counter.anvil_index_grouped_publish_local_batches_total =
                    self.local_batches,
                monotonic_counter.anvil_index_grouped_publish_remote_batches_total =
                    self.remote_batches,
                monotonic_counter.anvil_index_grouped_publish_items_total = self.attempted_items,
                monotonic_counter.anvil_index_grouped_publish_bytes_total = self.attempted_bytes,
                histogram.anvil_index_grouped_publish_requested_items = self.requested_items,
                histogram.anvil_index_grouped_publish_requested_bytes = self.requested_bytes,
                histogram.anvil_index_grouped_publish_replica_groups = self.groups,
                histogram.anvil_index_grouped_publish_batch_count = self.batches,
                histogram.anvil_index_grouped_publish_duration_seconds = elapsed_seconds,
                "grouped index artifact publication finished"
            );
        });
    }
}

impl Drop for GroupedPublishTelemetry {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(true);
        }
    }
}

#[derive(Clone)]
pub(crate) struct IndexArtifactRouter {
    local_node: NodeId,
    coordinator: IndexArtifactCoordinator,
    objects: ObjectDistribution,
    peers: ClusterPeerTransport,
}

impl IndexArtifactRouter {
    pub(crate) fn new(
        local_node: NodeId,
        coordinator: IndexArtifactCoordinator,
        objects: ObjectDistribution,
        peers: ClusterPeerTransport,
    ) -> Self {
        Self {
            local_node,
            coordinator,
            objects,
            peers,
        }
    }

    pub(crate) async fn publish(
        &self,
        request: IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status> {
        request.validate()?;
        let placement =
            self.require_local_builder(request.tenant_id, request.bucket_id, request.index_id)?;
        let fence = placement.fence();
        let key = match request.definition_guard.as_ref() {
            Some(guard) => guard.key(&request.storage_tenant, &request.bucket)?,
            None => request.key()?,
        };
        let outcome =
            match self
                .objects
                .routing_target_stable(&key, request.tenant_id, request.bucket_id)?
            {
                Some((target, address)) => {
                    self.peers
                        .publish_index_artifact(target, &address, fence, &request)
                        .await?
                }
                None => {
                    let receipt = self
                        .coordinator
                        .publish(self.local_node, placement, request)
                        .await?;
                    IndexArtifactOutcome {
                        version: receipt.version,
                        replayed: receipt.replayed,
                    }
                }
            };
        self.require_fence(fence)?;
        Ok(outcome)
    }

    pub(crate) async fn publish_many(
        &self,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactOutcome>, Status> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut telemetry = GroupedPublishTelemetry::start(&requests);
        let span = telemetry.span.clone();
        let result = self
            .publish_many_inner(requests, &mut telemetry)
            .instrument(span)
            .await;
        telemetry.finish(result.is_err());
        result
    }

    async fn publish_many_inner(
        &self,
        requests: Vec<IndexArtifactPublish>,
        telemetry: &mut GroupedPublishTelemetry,
    ) -> Result<Vec<IndexArtifactOutcome>, Status> {
        let first = &requests[0];
        let identity = (
            first.storage_tenant.clone(),
            first.bucket.clone(),
            first.tenant_id,
            first.bucket_id,
            first.index_id,
        );
        let placement = self.require_local_builder(identity.2, identity.3, identity.4)?;
        let fence = placement.fence();
        let mut groups =
            std::collections::BTreeMap::<Vec<NodeId>, Vec<(usize, IndexArtifactPublish)>>::new();
        for (index, request) in requests.into_iter().enumerate() {
            request.validate()?;
            if request.storage_tenant != identity.0
                || request.bucket != identity.1
                || request.tenant_id != identity.2
                || request.bucket_id != identity.3
                || request.index_id != identity.4
            {
                return Err(Status::invalid_argument(
                    "one grouped publication candidate must share its index identity",
                ));
            }
            let key = request.key()?;
            let group = self.objects.object_replica_group_stable(
                &placement,
                &key,
                request.tenant_id,
                request.bucket_id,
            )?;
            groups
                .entry(group.replicas().to_vec())
                .or_default()
                .push((index, request));
        }
        telemetry.groups = groups.len() as u64;
        let outcome_count = groups.values().map(Vec::len).sum();
        let mut outcomes = vec![None; outcome_count];
        for (replicas, group) in groups {
            let coordinator = replicas[0];
            for batch in bounded_artifact_batches(group)? {
                telemetry.record_batch(coordinator == self.local_node, &batch);
                let (indices, publications): (Vec<_>, Vec<_>) = batch.into_iter().unzip();
                let published = if coordinator == self.local_node {
                    self.coordinator
                        .publish_many(self.local_node, placement.clone(), publications)
                        .await?
                } else {
                    let address = placement.address(coordinator).ok_or_else(|| {
                        Status::unavailable(format!(
                            "ACTIVE artifact coordinator {} has no peer address",
                            coordinator.0
                        ))
                    })?;
                    self.peers
                        .publish_index_artifacts(coordinator, &address.0, fence, &publications)
                        .await?
                };
                record_grouped_artifact_outcomes(&mut outcomes, indices, published)?;
                self.require_fence(fence)?;
            }
        }
        ordered_grouped_artifact_outcomes(outcomes)
    }

    pub(crate) async fn delete(
        &self,
        request: IndexArtifactDelete,
    ) -> Result<IndexArtifactOutcome, Status> {
        request.validate()?;
        let placement =
            self.require_local_builder(request.tenant_id, request.bucket_id, request.index_id)?;
        let fence = placement.fence();
        let key = request.key()?;
        let outcome =
            match self
                .objects
                .routing_target_stable(&key, request.tenant_id, request.bucket_id)?
            {
                Some((target, address)) => {
                    self.peers
                        .delete_index_artifact(target, &address, &request)
                        .await?
                }
                None => {
                    let receipt = self
                        .coordinator
                        .delete(self.local_node, placement, request)
                        .await?;
                    IndexArtifactOutcome {
                        version: receipt.version,
                        replayed: receipt.replayed,
                    }
                }
            };
        self.require_fence(fence)?;
        Ok(outcome)
    }

    fn require_local_builder(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    ) -> Result<ClusterPlacement, Status> {
        let placement = self.objects.current_program_placement()?;
        let identity = IndexIdentity::new(tenant_id, bucket_id, index_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let assignment = IndexPlacement::derive(identity, &placement)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if assignment.builder() != self.local_node {
            return Err(Status::failed_precondition(
                "this node is not the current weighted-HRW index builder",
            ));
        }
        Ok(placement)
    }

    fn require_fence(&self, expected: anvil_store::PlacementLogId) -> Result<(), Status> {
        if self.objects.current_program_placement()?.fence() != expected {
            return Err(Status::unavailable(
                "index placement changed during artifact mutation",
            ));
        }
        Ok(())
    }
}

fn bounded_artifact_batches(
    requests: Vec<(usize, IndexArtifactPublish)>,
) -> Result<Vec<Vec<(usize, IndexArtifactPublish)>>, Status> {
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut batch_bytes = 0_u64;
    for request in requests {
        let item_bytes = request.1.blob.length;
        if item_bytes > MAX_INDEX_ARTIFACT_BATCH_BYTES {
            return Err(Status::resource_exhausted(
                "one index artifact exceeds the grouped publication byte bound",
            ));
        }
        let next_bytes = batch_bytes.checked_add(item_bytes).ok_or_else(|| {
            Status::resource_exhausted("index artifact batch byte count overflow")
        })?;
        if !batch.is_empty()
            && (batch.len() == MAX_INDEX_ARTIFACT_BATCH_ITEMS
                || next_bytes > MAX_INDEX_ARTIFACT_BATCH_BYTES)
        {
            batches.push(std::mem::take(&mut batch));
            batch_bytes = 0;
        }
        batch_bytes = batch_bytes
            .checked_add(item_bytes)
            .ok_or_else(|| Status::resource_exhausted("index artifact byte count overflow"))?;
        batch.push(request);
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    Ok(batches)
}

fn record_grouped_artifact_outcomes(
    outcomes: &mut [Option<IndexArtifactOutcome>],
    indices: Vec<usize>,
    published: Vec<IndexArtifactOutcome>,
) -> Result<(), Status> {
    if published.len() != indices.len() {
        return Err(Status::data_loss(
            "grouped index artifact outcome count differs from its request",
        ));
    }
    for (index, outcome) in indices.into_iter().zip(published) {
        let slot = outcomes.get_mut(index).ok_or_else(|| {
            Status::data_loss("grouped index artifact outcome index is out of bounds")
        })?;
        if slot.replace(outcome).is_some() {
            return Err(Status::data_loss(
                "grouped index artifact outcome was recorded more than once",
            ));
        }
    }
    Ok(())
}

fn ordered_grouped_artifact_outcomes(
    outcomes: Vec<Option<IndexArtifactOutcome>>,
) -> Result<Vec<IndexArtifactOutcome>, Status> {
    outcomes
        .into_iter()
        .map(|outcome| {
            outcome.ok_or_else(|| Status::data_loss("grouped index artifact outcome is missing"))
        })
        .collect()
}

#[tonic::async_trait]
impl IndexArtifactPublication for IndexArtifactCoordinator {
    async fn publish(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        request: IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status> {
        if request.definition_guard.is_some() {
            self.publish_guarded(authenticated_builder, placement, request)
                .await
        } else {
            self.publish_unguarded(authenticated_builder, placement, request)
                .await
        }
    }

    async fn publish_many(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactOutcome>, Status> {
        self.publish_immutable_many(authenticated_builder, placement, requests)
            .await
    }

    async fn commit_guarded(
        &self,
        authenticated_definition_coordinator: NodeId,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        request: IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status> {
        request.validate()?;
        let guard = request
            .definition_guard
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("guarded commit has no definition guard"))?;
        let identity = IndexIdentity::new(request.tenant_id, request.bucket_id, request.index_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.validate_index_builder(authenticated_builder, &placement, identity)?;
        let definition_key = guard.key(&request.storage_tenant, &request.bucket)?;
        if self.objects.object_coordinator_stable(
            &placement,
            &definition_key,
            request.tenant_id,
            request.bucket_id,
        )? != authenticated_definition_coordinator
        {
            return Err(Status::permission_denied(
                "guarded artifact commit caller is not the definition-path coordinator",
            ));
        }
        self.publish_unguarded(authenticated_builder, placement, request)
            .await
    }

    async fn delete(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        request: IndexArtifactDelete,
    ) -> Result<IndexArtifactOutcome, Status> {
        let kind = request.validate()?;
        let identity = IndexIdentity::new(request.tenant_id, request.bucket_id, request.index_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let key = request.key()?;
        let governance = self
            .governance
            .resolve(&request.storage_tenant, &request.bucket)
            .await?;
        if (governance.tenant_id, governance.bucket_id)
            != (identity.tenant_id(), identity.bucket_id())
        {
            return Err(Status::failed_precondition(
                "index artifact mutable names no longer bind the supplied stable IDs",
            ));
        }
        self.validate_builder(authenticated_builder, &placement, identity, &key)?;
        if request.definition_intent.is_none() && governance.versioning == ObjectVersioning::Enabled
        {
            let outcome = self
                .objects
                .delete_retained_version_with_governance(&key, request.expected_version, governance)
                .await?;
            return Ok(retained_delete_outcome(outcome, request.expected_version));
        }
        if kind == ArtifactPathKind::Current {
            // An unversioned replacement already retired the predecessor.
            return Ok(IndexArtifactOutcome {
                version: request.expected_version,
                replayed: true,
            });
        }
        let durability = artifact_durability(kind, placement.placement_nodes().len());
        let receipt = self
            .objects
            .mutate_with_governance_and_definition_intent(
                BatchOperation::Delete(DeleteRequest {
                    key,
                    precondition: Precondition::Version(request.expected_version),
                    command_id: Some(request.command_id),
                    durability,
                }),
                governance,
                request.definition_intent,
            )
            .await?;
        Ok(IndexArtifactOutcome {
            version: receipt.version,
            replayed: receipt.replayed,
        })
    }
}

fn retained_delete_outcome(
    outcome: DeleteRetainedVersionOutcome,
    expected: VersionId,
) -> IndexArtifactOutcome {
    match outcome {
        DeleteRetainedVersionOutcome::NotFound => IndexArtifactOutcome {
            version: expected,
            replayed: true,
        },
        DeleteRetainedVersionOutcome::DeletedNonCurrent => IndexArtifactOutcome {
            version: expected,
            replayed: false,
        },
        DeleteRetainedVersionOutcome::ReplacedCurrentWithTombstone { version } => {
            IndexArtifactOutcome {
                version,
                replayed: false,
            }
        }
    }
}

fn require_guarded_definition(
    current: Option<&anvil_store::CurrentObjectSnapshot>,
    expected: VersionId,
) -> Result<(), Status> {
    match current {
        Some(current)
            if !current.head.deleted
                && current.head.version == expected
                && current.version.id == expected
                && !current.version.deleted =>
        {
            Ok(())
        }
        Some(_) => Err(Status::failed_precondition(
            "definition changed before guarded artifact publication",
        )),
        None => Err(Status::failed_precondition(
            "definition was deleted before guarded artifact publication",
        )),
    }
}

fn validate_immutable_batch(requests: &[IndexArtifactPublish]) -> Result<(), Status> {
    if requests.is_empty() || requests.len() > MAX_INDEX_ARTIFACT_BATCH_ITEMS {
        return Err(Status::resource_exhausted(format!(
            "index artifact batch must contain 1..={MAX_INDEX_ARTIFACT_BATCH_ITEMS} items"
        )));
    }
    let first = &requests[0];
    let mut bytes = 0_u64;
    for request in requests {
        if request.validate()? != ArtifactPathKind::Immutable {
            return Err(Status::invalid_argument(
                "grouped index publication accepts immutable artifacts only",
            ));
        }
        if request.storage_tenant != first.storage_tenant
            || request.bucket != first.bucket
            || request.tenant_id != first.tenant_id
            || request.bucket_id != first.bucket_id
            || request.index_id != first.index_id
        {
            return Err(Status::invalid_argument(
                "grouped index artifacts must share one exact index identity",
            ));
        }
        bytes = bytes.checked_add(request.blob.length).ok_or_else(|| {
            Status::resource_exhausted("index artifact batch byte count overflow")
        })?;
    }
    if bytes > MAX_INDEX_ARTIFACT_BATCH_BYTES {
        return Err(Status::resource_exhausted(format!(
            "index artifact batch exceeds {MAX_INDEX_ARTIFACT_BATCH_BYTES} logical bytes"
        )));
    }
    Ok(())
}

fn artifact_durability(kind: ArtifactPathKind, active_nodes: usize) -> Durability {
    match kind {
        // Accounting artifacts remain ordinary placed objects. LOCAL is only
        // their acknowledgement threshold while normal placement converges.
        ArtifactPathKind::AccountingMutable => Durability::Local,
        // A one-node topology cannot honestly satisfy REPLICATED.
        // Once more than one node is ACTIVE, keep the stronger request and let
        // the ordinary object path fail closed unless its exact requirements
        // can be met.
        ArtifactPathKind::Current | ArtifactPathKind::Immutable if active_nodes == 1 => {
            Durability::Local
        }
        ArtifactPathKind::Current | ArtifactPathKind::Immutable => Durability::Replicated,
    }
}

fn parse_artifact_path(path: &str, expected_index: u64) -> Result<ArtifactPathKind, Status> {
    if crate::accounting::is_artifact_path(path, expected_index) {
        return Ok(ArtifactPathKind::AccountingMutable);
    }
    let parts = path.split('/').collect::<Vec<_>>();
    if parts.len() < 5
        || parts[0] != "_anvil"
        || parts[1] != "indexes"
        || parts[2] != "v3"
        || parse_canonical_u64(parts[3]) != Some(expected_index)
    {
        return Err(Status::invalid_argument(
            "index artifact path is outside its reserved index namespace",
        ));
    }
    match parts.as_slice() {
        [_, _, _, _, "current"] => Ok(ArtifactPathKind::Current),
        [_, _, _, _, "manifests", digest] if valid_digest(digest) => {
            Ok(ArtifactPathKind::Immutable)
        }
        [_, _, _, _, "runs", run, "root"] if valid_digest(run) => Ok(ArtifactPathKind::Immutable),
        [_, _, _, _, "runs", run, "packs", ordinal]
            if valid_digest(run) && parse_canonical_u32(ordinal).is_some() =>
        {
            Ok(ArtifactPathKind::Immutable)
        }
        _ => Err(Status::invalid_argument(
            "index artifact path does not name a v3 current pointer, manifest, run, or component",
        )),
    }
}

fn validate_definition_intent(
    kind: ArtifactPathKind,
    path: &str,
    expected_id: u64,
    intent: Option<DefinitionMutationIntent>,
) -> Result<(), Status> {
    let is_definition = kind == ArtifactPathKind::AccountingMutable
        && crate::accounting::definition_path(expected_id)
            .ok()
            .as_deref()
            == Some(path);
    match (is_definition, intent) {
        (true, Some(intent))
            if intent.kind == DefinitionKind::Accounting && intent.definition_id == expected_id =>
        {
            Ok(())
        }
        (false, None) => Ok(()),
        (true, None) => Err(Status::invalid_argument(
            "accounting definition mutation requires trusted typed intent",
        )),
        _ => Err(Status::invalid_argument(
            "definition intent does not match the accounting definition path",
        )),
    }
}

fn parse_canonical_u64(value: &str) -> Option<u64> {
    let parsed = value.parse::<u64>().ok()?;
    (parsed != 0 && parsed.to_string() == value).then_some(parsed)
}

fn parse_canonical_u32(value: &str) -> Option<u32> {
    let parsed = value.parse::<u32>().ok()?;
    (parsed.to_string() == value).then_some(parsed)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn index_definition_name(path: &str) -> Option<&str> {
    let parts = path.split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        ["_anvil", "indexes", "v3", "definitions", name] if valid_definition_name(name) => {
            Some(name)
        }
        _ => None,
    }
}

fn valid_definition_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains('\0')
}

pub(crate) fn manifest_path(index_id: u64, digest: [u8; 32]) -> String {
    format!(
        "_anvil/indexes/v3/{index_id}/manifests/{}",
        hex::encode(digest)
    )
}

pub(crate) fn run_root_path(index_id: u64, run_digest: [u8; 32]) -> String {
    format!("{}root", run_prefix(index_id, run_digest))
}

pub(crate) fn run_prefix(index_id: u64, run_digest: [u8; 32]) -> String {
    format!(
        "_anvil/indexes/v3/{index_id}/runs/{}/",
        hex::encode(run_digest)
    )
}

/// Extract a run identity only from one canonical format-3 root/pack path.
/// Prefix retention uses this instead of textual starts-with matching so an
/// adjacent digest or an extra slash cannot widen a deletion scope.
pub(crate) fn run_hash_from_artifact_path(index_id: u64, path: &str) -> Option<[u8; 32]> {
    let parts = path.split('/').collect::<Vec<_>>();
    let digest = match parts.as_slice() {
        [
            "_anvil",
            "indexes",
            "v3",
            encoded_index,
            "runs",
            digest,
            "root",
        ] if parse_canonical_u64(encoded_index) == Some(index_id) && valid_digest(digest) => {
            *digest
        }
        [
            "_anvil",
            "indexes",
            "v3",
            encoded_index,
            "runs",
            run,
            "packs",
            ordinal,
        ] if parse_canonical_u64(encoded_index) == Some(index_id)
            && valid_digest(run)
            && parse_canonical_u32(ordinal).is_some() =>
        {
            *run
        }
        _ => return None,
    };
    let decoded = hex::decode(digest).ok()?;
    decoded.try_into().ok()
}

pub(crate) fn is_manifest_artifact_path(index_id: u64, path: &str) -> bool {
    let parts = path.split('/').collect::<Vec<_>>();
    matches!(
        parts.as_slice(),
        ["_anvil", "indexes", "v3", encoded_index, "manifests", digest]
            if parse_canonical_u64(encoded_index) == Some(index_id) && valid_digest(digest)
    )
}

pub(crate) fn run_pack_path(index_id: u64, run_digest: [u8; 32], pack_id: u32) -> String {
    format!("{}packs/{}", run_prefix(index_id, run_digest), pack_id)
}

pub(crate) fn current_path(index_id: u64) -> String {
    format!("_anvil/indexes/v3/{index_id}/current")
}

pub(crate) fn is_index_recovery_path(path: &str, index_id: u64) -> bool {
    parse_artifact_path(path, index_id).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact_publish(
        exact_path: String,
        definition_guard: Option<DefinitionVersionGuard>,
    ) -> IndexArtifactPublish {
        IndexArtifactPublish {
            storage_tenant: "tenant".into(),
            bucket: "bucket".into(),
            tenant_id: 1,
            bucket_id: 2,
            index_id: 7,
            exact_path,
            blob: BlobRef {
                hash: [3; 32],
                length: 1,
            },
            expected_version: None,
            command_id: "publish-guard-test".into(),
            definition_guard,
            definition_intent: None,
        }
    }

    #[test]
    fn only_exact_reserved_artifact_shapes_are_accepted() {
        assert_eq!(
            parse_artifact_path("_anvil/indexes/v3/7/current", 7).unwrap(),
            ArtifactPathKind::Current
        );
        let digest = "a".repeat(64);
        assert_eq!(
            parse_artifact_path(&format!("_anvil/indexes/v3/7/manifests/{digest}"), 7).unwrap(),
            ArtifactPathKind::Immutable
        );
        assert!(
            parse_artifact_path(&format!("_anvil/indexes/v3/7/runs/{digest}/packs/0"), 7).is_ok()
        );
        for invalid in [
            "_anvil/indexes/v3/7/definition",
            "_anvil/indexes/v3/7/runs/name/descriptor",
            "_anvil/indexes/v3/07/current",
            "_anvil/indexes/7/current",
            "ordinary/path",
        ] {
            assert!(parse_artifact_path(invalid, 7).is_err(), "{invalid}");
        }
    }

    #[test]
    fn definition_discovery_accepts_only_the_dedicated_path_shape() {
        assert_eq!(
            index_definition_name("_anvil/indexes/v3/definitions/search"),
            Some("search")
        );
        assert_eq!(
            index_definition_name("_anvil/indexes/v3/12/definition"),
            None
        );
        assert_eq!(
            index_definition_name("_anvil/indexes/v3/definitions/a/b"),
            None
        );
    }

    #[test]
    fn current_publication_requires_its_exact_typed_definition_guard() {
        let current = current_path(7);
        assert!(artifact_publish(current.clone(), None).validate().is_err());

        let valid = DefinitionVersionGuard {
            kind: DefinitionKind::Index,
            exact_path: "_anvil/indexes/v3/definitions/search".into(),
            expected_version: VersionId(9),
        };
        assert_eq!(
            artifact_publish(current.clone(), Some(valid.clone()))
                .validate()
                .unwrap(),
            ArtifactPathKind::Current
        );

        let mut wrong_kind = valid.clone();
        wrong_kind.kind = DefinitionKind::Accounting;
        assert!(
            artifact_publish(current.clone(), Some(wrong_kind))
                .validate()
                .is_err()
        );

        let mut zero_version = valid;
        zero_version.expected_version = VersionId(0);
        assert!(
            artifact_publish(current, Some(zero_version))
                .validate()
                .is_err()
        );
    }

    #[test]
    fn guards_are_rejected_on_immutable_or_wrong_accounting_paths() {
        let index_guard = DefinitionVersionGuard {
            kind: DefinitionKind::Index,
            exact_path: "_anvil/indexes/v3/definitions/search".into(),
            expected_version: VersionId(9),
        };
        assert!(
            artifact_publish(manifest_path(7, [4; 32]), Some(index_guard))
                .validate()
                .is_err()
        );

        let accounting_guard = DefinitionVersionGuard {
            kind: DefinitionKind::Accounting,
            exact_path: crate::accounting::definition_path(7).unwrap(),
            expected_version: VersionId(9),
        };
        assert_eq!(
            artifact_publish(
                crate::accounting::current_path(7).unwrap(),
                Some(accounting_guard.clone()),
            )
            .validate()
            .unwrap(),
            ArtifactPathKind::AccountingMutable
        );
        let mut wrong_path = accounting_guard;
        wrong_path.exact_path = crate::accounting::definition_path(8).unwrap();
        assert!(
            artifact_publish(
                crate::accounting::current_path(7).unwrap(),
                Some(wrong_path),
            )
            .validate()
            .is_err()
        );
    }

    #[test]
    fn accounting_definition_create_and_delete_require_typed_intent() {
        let path = crate::accounting::definition_path(7).unwrap();
        let intent = DefinitionMutationIntent::new(DefinitionKind::Accounting, 7).unwrap();
        let kind = parse_artifact_path(&path, 7).unwrap();
        assert_eq!(kind, ArtifactPathKind::AccountingMutable);
        assert!(validate_definition_intent(kind, &path, 7, Some(intent)).is_ok());
        assert!(validate_definition_intent(kind, &path, 7, None).is_err());

        let current = crate::accounting::current_path(7).unwrap();
        let kind = parse_artifact_path(&current, 7).unwrap();
        assert!(validate_definition_intent(kind, &current, 7, Some(intent)).is_err());
        assert!(validate_definition_intent(kind, &current, 7, None).is_ok());
    }

    #[test]
    fn generated_paths_round_trip_through_validation() {
        assert_eq!(current_path(4), "_anvil/indexes/v3/4/current");
        assert!(parse_artifact_path(&manifest_path(4, [2; 32]), 4).is_ok());
        assert!(parse_artifact_path(&run_root_path(4, [3; 32]), 4).is_ok());
        assert!(parse_artifact_path(&run_pack_path(4, [3; 32], 0), 4).is_ok());
        assert_eq!(
            run_hash_from_artifact_path(4, &run_root_path(4, [3; 32])),
            Some([3; 32])
        );
        assert_eq!(
            run_hash_from_artifact_path(4, &run_pack_path(4, [3; 32], 0)),
            Some([3; 32])
        );
        assert!(is_manifest_artifact_path(4, &manifest_path(4, [2; 32])));
    }

    #[test]
    fn run_retention_parser_is_slash_safe_and_v3_only() {
        let digest = hex::encode([3; 32]);
        for invalid in [
            format!("_anvil/indexes/v3/4/runs/{digest}"),
            format!("_anvil/indexes/v3/4/runs/{digest}/"),
            format!("_anvil/indexes/v3/4/runs/{digest}/root/extra"),
            format!("_anvil/indexes/v3/4/runs/{digest}0/root"),
            format!("_anvil/indexes/4/runs/{digest}/root"),
            format!("_anvil/indexes/v3/04/runs/{digest}/root"),
        ] {
            assert_eq!(run_hash_from_artifact_path(4, &invalid), None, "{invalid}");
        }
    }

    #[test]
    fn one_node_index_publication_uses_local_acknowledgement() {
        for kind in [ArtifactPathKind::Current, ArtifactPathKind::Immutable] {
            assert_eq!(artifact_durability(kind, 1), Durability::Local);
        }
    }

    #[test]
    fn clustered_index_publication_keeps_replicated_acknowledgement() {
        for active_nodes in [2, 3, 5] {
            for kind in [ArtifactPathKind::Current, ArtifactPathKind::Immutable] {
                assert_eq!(
                    artifact_durability(kind, active_nodes),
                    Durability::Replicated
                );
            }
        }
        assert_eq!(
            artifact_durability(ArtifactPathKind::AccountingMutable, 3),
            Durability::Local
        );
    }

    #[test]
    fn multiple_packs_share_one_bounded_grouped_mutation() {
        let first = artifact_publish(run_pack_path(7, [4; 32], 0), None);
        let second = artifact_publish(run_pack_path(7, [4; 32], 1), None);
        let batches = bounded_artifact_batches(vec![(0, first), (1, second)]).unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 2);
        assert!(
            validate_immutable_batch(
                &batches[0]
                    .iter()
                    .map(|(_, request)| request.clone())
                    .collect::<Vec<_>>()
            )
            .is_ok()
        );
    }

    #[test]
    fn current_pointer_cannot_enter_a_pack_batch() {
        let pack = artifact_publish(run_pack_path(7, [4; 32], 0), None);
        let current = artifact_publish(
            current_path(7),
            Some(DefinitionVersionGuard {
                kind: DefinitionKind::Index,
                exact_path: "_anvil/indexes/v3/definitions/search".into(),
                expected_version: VersionId(9),
            }),
        );

        assert!(validate_immutable_batch(&[pack, current]).is_err());
    }

    #[test]
    fn grouped_publication_restores_request_order_across_replica_groups() {
        let outcome = |version| IndexArtifactOutcome {
            version: VersionId(version),
            replayed: false,
        };
        let mut slots = vec![None; 4];

        // Replica groups are visited by their placement key, not input order.
        record_grouped_artifact_outcomes(&mut slots, vec![2, 0], vec![outcome(30), outcome(10)])
            .unwrap();
        record_grouped_artifact_outcomes(&mut slots, vec![3, 1], vec![outcome(40), outcome(20)])
            .unwrap();

        let ordered = ordered_grouped_artifact_outcomes(slots).unwrap();
        assert_eq!(
            ordered
                .into_iter()
                .map(|entry| entry.version.0)
                .collect::<Vec<_>>(),
            vec![10, 20, 30, 40]
        );
    }
}
