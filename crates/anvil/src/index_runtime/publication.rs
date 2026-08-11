//! Narrow ordinary-object publication boundary for index generations.

use std::sync::{Arc, OnceLock};

use anvil_consensus::NodeId;
use anvil_store::{
    BatchOperation, BlobRef, DefinitionKind, DefinitionMutationIntent, DeleteRequest,
    DeleteRetainedVersionOutcome, Durability, ObjectKey, ObjectVersioning, Precondition,
    PublishRequest, PutMode, Store, VersionId,
};
use tonic::Status;

use crate::bucket_governance::BucketGovernance;
use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_placement::ClusterPlacement;
use crate::object_distribution::ObjectDistribution;

use super::placement::{IndexIdentity, IndexPlacement};

const INDEX_ARTIFACT_CONTENT_TYPE: &str = "application/vnd.anvil.index-artifact";
const ACCOUNTING_ARTIFACT_CONTENT_TYPE: &str = "application/vnd.anvil.accounting+json";

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
        || parts[2] != "v2"
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
        [_, _, _, _, "runs", run, "blocks", block] if valid_digest(run) && valid_digest(block) => {
            Ok(ArtifactPathKind::Immutable)
        }
        _ => Err(Status::invalid_argument(
            "index artifact path does not name a v2 current pointer, manifest, run, or component",
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

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn index_definition_name(path: &str) -> Option<&str> {
    let parts = path.split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        ["_anvil", "indexes", "v2", "definitions", name] if valid_definition_name(name) => {
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
        "_anvil/indexes/v2/{index_id}/manifests/{}",
        hex::encode(digest)
    )
}

pub(crate) fn run_root_path(index_id: u64, run_digest: [u8; 32]) -> String {
    format!("{}root", run_prefix(index_id, run_digest))
}

pub(crate) fn run_prefix(index_id: u64, run_digest: [u8; 32]) -> String {
    format!(
        "_anvil/indexes/v2/{index_id}/runs/{}/",
        hex::encode(run_digest)
    )
}

/// Extract a run identity only from one canonical format-2 root/block path.
/// Prefix retention uses this instead of textual starts-with matching so an
/// adjacent digest or an extra slash cannot widen a deletion scope.
pub(crate) fn run_hash_from_artifact_path(index_id: u64, path: &str) -> Option<[u8; 32]> {
    let parts = path.split('/').collect::<Vec<_>>();
    let digest = match parts.as_slice() {
        [
            "_anvil",
            "indexes",
            "v2",
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
            "v2",
            encoded_index,
            "runs",
            run,
            "blocks",
            block,
        ] if parse_canonical_u64(encoded_index) == Some(index_id)
            && valid_digest(run)
            && valid_digest(block) =>
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
        ["_anvil", "indexes", "v2", encoded_index, "manifests", digest]
            if parse_canonical_u64(encoded_index) == Some(index_id) && valid_digest(digest)
    )
}

pub(crate) fn run_block_path(
    index_id: u64,
    run_digest: [u8; 32],
    block_digest: [u8; 32],
) -> String {
    format!(
        "{}blocks/{}",
        run_prefix(index_id, run_digest),
        hex::encode(block_digest)
    )
}

pub(crate) fn current_path(index_id: u64) -> String {
    format!("_anvil/indexes/v2/{index_id}/current")
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
            parse_artifact_path("_anvil/indexes/v2/7/current", 7).unwrap(),
            ArtifactPathKind::Current
        );
        let digest = "a".repeat(64);
        assert_eq!(
            parse_artifact_path(&format!("_anvil/indexes/v2/7/manifests/{digest}"), 7).unwrap(),
            ArtifactPathKind::Immutable
        );
        assert!(
            parse_artifact_path(
                &format!("_anvil/indexes/v2/7/runs/{digest}/blocks/{digest}"),
                7
            )
            .is_ok()
        );
        for invalid in [
            "_anvil/indexes/v2/7/definition",
            "_anvil/indexes/v2/7/runs/name/descriptor",
            "_anvil/indexes/v2/07/current",
            "_anvil/indexes/7/current",
            "ordinary/path",
        ] {
            assert!(parse_artifact_path(invalid, 7).is_err(), "{invalid}");
        }
    }

    #[test]
    fn definition_discovery_accepts_only_the_dedicated_path_shape() {
        assert_eq!(
            index_definition_name("_anvil/indexes/v2/definitions/search"),
            Some("search")
        );
        assert_eq!(
            index_definition_name("_anvil/indexes/v2/12/definition"),
            None
        );
        assert_eq!(
            index_definition_name("_anvil/indexes/v2/definitions/a/b"),
            None
        );
    }

    #[test]
    fn current_publication_requires_its_exact_typed_definition_guard() {
        let current = current_path(7);
        assert!(artifact_publish(current.clone(), None).validate().is_err());

        let valid = DefinitionVersionGuard {
            kind: DefinitionKind::Index,
            exact_path: "_anvil/indexes/v2/definitions/search".into(),
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
            exact_path: "_anvil/indexes/v2/definitions/search".into(),
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
        assert_eq!(current_path(4), "_anvil/indexes/v2/4/current");
        assert!(parse_artifact_path(&manifest_path(4, [2; 32]), 4).is_ok());
        assert!(parse_artifact_path(&run_root_path(4, [3; 32]), 4).is_ok());
        assert!(parse_artifact_path(&run_block_path(4, [3; 32], [4; 32]), 4).is_ok());
        assert_eq!(
            run_hash_from_artifact_path(4, &run_root_path(4, [3; 32])),
            Some([3; 32])
        );
        assert_eq!(
            run_hash_from_artifact_path(4, &run_block_path(4, [3; 32], [4; 32])),
            Some([3; 32])
        );
        assert!(is_manifest_artifact_path(4, &manifest_path(4, [2; 32])));
    }

    #[test]
    fn run_retention_parser_is_slash_safe_and_v2_only() {
        let digest = hex::encode([3; 32]);
        for invalid in [
            format!("_anvil/indexes/v2/4/runs/{digest}"),
            format!("_anvil/indexes/v2/4/runs/{digest}/"),
            format!("_anvil/indexes/v2/4/runs/{digest}/root/extra"),
            format!("_anvil/indexes/v2/4/runs/{digest}0/root"),
            format!("_anvil/indexes/4/runs/{digest}/root"),
            format!("_anvil/indexes/v2/04/runs/{digest}/root"),
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
}
