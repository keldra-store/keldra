//! Narrow ordinary-object publication boundary for index commits.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use keldra_consensus::NodeId;
use keldra_store::{
    BatchOperation, BlobRef, DefinitionKind, DefinitionMutationIntent, DeleteRequest,
    DeleteRetainedVersionOutcome, Durability, ObjectKey, ObjectVersioning, PlacementLogId,
    Precondition, PublishRequest, PutMode, SourceId, Store, VersionId,
};
use tonic::Status;

use crate::bucket_governance::BucketGovernance;
use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_placement::ClusterPlacement;
use crate::object_distribution::ObjectDistribution;

use super::placement::{IndexIdentity, IndexPlacement};

mod paths;
mod v6_batch;
use paths::{ArtifactPathKind, immutable_content_hash_from_path, parse_artifact_path};
pub(crate) use paths::{
    artifact_hash_from_path, artifact_path, current_path, index_definition_name,
    is_index_recovery_path, is_manifest_artifact_path, manifest_hash_from_path, manifest_path,
    rebuild_path,
};

const INDEX_ARTIFACT_CONTENT_TYPE: &str = "application/vnd.keldra.index-artifact";
const ACCOUNTING_ARTIFACT_CONTENT_TYPE: &str = "application/vnd.keldra.accounting+json";
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
            (
                ArtifactPathKind::Current | ArtifactPathKind::RebuildMutable,
                DefinitionKind::Index,
            ) => index_definition_name(&self.exact_path).is_some(),
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
    /// Absence creates an immutable commit artifact/current pointer.
    /// Presence replaces only the current pointer at this exact version.
    pub expected_version: Option<VersionId>,
    pub command_id: String,
    /// Exact authoritative definition revision which must remain live while
    /// the mutable current pointer or accounting rollup is committed.
    pub definition_guard: Option<DefinitionVersionGuard>,
    /// Trusted typed evidence for an ordinary definition mutation. Generic
    /// commit, current, rollup, and traffic-source artifacts leave this
    /// absent.
    pub definition_intent: Option<DefinitionMutationIntent>,
    /// Private admission selected by the current authenticated HRW builder.
    /// It is never inferred from a reserved path or exposed to public clients.
    pub admission: DerivedArtifactAdmission,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum DerivedArtifactAdmission {
    #[default]
    Bounded,
    PublicationProgress,
}

impl DerivedArtifactAdmission {
    pub(crate) const fn is_publication_progress(self) -> bool {
        matches!(self, Self::PublicationProgress)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexArtifactOutcome {
    pub version: VersionId,
    pub replayed: bool,
}

pub(crate) type IndexArtifactPublicationOutcome = Result<IndexArtifactOutcome, Status>;

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
        if matches!(
            kind,
            ArtifactPathKind::ProjectionCurrent
                | ArtifactPathKind::ProjectionCatalogMutable
                | ArtifactPathKind::ProjectionImmutable
        ) {
            return Err(Status::failed_precondition(
                "v6 projection artifact reclamation requires partition-directory reachability proof",
            ));
        }
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
        let expected_content_hash = match kind {
            ArtifactPathKind::Immutable => {
                immutable_content_hash_from_path(self.index_id, &self.exact_path)
            }
            ArtifactPathKind::ProjectionImmutable => {
                keldra_index::v6::parse_projection_artifact_path(&self.exact_path)
                    .ok()
                    .and_then(|parsed| parsed.content_hash)
            }
            _ => None,
        };
        if kind.is_immutable() && expected_content_hash != Some(self.blob.hash) {
            return Err(Status::invalid_argument(
                "immutable index artifact path must equal its content hash",
            ));
        }
        validate_definition_intent(
            kind,
            &self.exact_path,
            self.index_id,
            self.definition_intent,
        )?;
        if self.admission.is_publication_progress() {
            let eligible = matches!(
                kind,
                ArtifactPathKind::Current
                    | ArtifactPathKind::RebuildMutable
                    | ArtifactPathKind::Immutable
                    | ArtifactPathKind::ProjectionCurrent
                    | ArtifactPathKind::ProjectionCatalogMutable
                    | ArtifactPathKind::ProjectionImmutable
            ) || (kind == ArtifactPathKind::AccountingMutable
                && crate::accounting::current_path(self.index_id)
                    .ok()
                    .as_deref()
                    == Some(self.exact_path.as_str()));
            if !eligible || self.definition_intent.is_some() {
                return Err(Status::invalid_argument(
                    "publication-progress admission is valid only for an index commit or complete accounting-rollup publication",
                ));
            }
        }
        match (kind, self.expected_version) {
            (ArtifactPathKind::Current, Some(VersionId(0))) => Err(Status::invalid_argument(
                "index current-pointer expected version must be non-zero",
            )),
            (
                ArtifactPathKind::ProjectionCurrent | ArtifactPathKind::ProjectionCatalogMutable,
                Some(VersionId(0)),
            ) => Err(Status::invalid_argument(
                "projection current-pointer expected version must be non-zero",
            )),
            (ArtifactPathKind::RebuildMutable, Some(VersionId(0))) => Err(
                Status::invalid_argument("index rebuild-root expected version must be non-zero"),
            ),
            (ArtifactPathKind::AccountingMutable, Some(VersionId(0))) => Err(
                Status::invalid_argument("accounting artifact expected version must be non-zero"),
            ),
            (
                ArtifactPathKind::Current
                | ArtifactPathKind::ProjectionCurrent
                | ArtifactPathKind::ProjectionCatalogMutable
                | ArtifactPathKind::RebuildMutable
                | ArtifactPathKind::AccountingMutable,
                _,
            )
            | (ArtifactPathKind::Immutable | ArtifactPathKind::ProjectionImmutable, None) => {
                Ok(kind)
            }
            (ArtifactPathKind::Immutable | ArtifactPathKind::ProjectionImmutable, Some(_)) => Err(
                Status::invalid_argument("immutable index commit artifacts cannot be replaced"),
            ),
        }?;
        let guard_required = matches!(
            kind,
            ArtifactPathKind::Current | ArtifactPathKind::RebuildMutable
        ) || (kind == ArtifactPathKind::AccountingMutable
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

fn artifact_placement_identity(
    tenant_id: u64,
    bucket_id: u64,
    index_id: u64,
    kind: ArtifactPathKind,
) -> Result<IndexIdentity, Status> {
    let identity = if matches!(
        kind,
        ArtifactPathKind::AccountingMutable | ArtifactPathKind::ProjectionCatalogMutable
    ) {
        IndexIdentity::new(tenant_id, bucket_id, index_id)
    } else {
        IndexIdentity::projection_partition(tenant_id, bucket_id)
    };
    identity.map_err(|error| Status::invalid_argument(error.to_string()))
}

fn projection_partition_owner(
    path: &str,
    placement: &ClusterPlacement,
) -> Result<Option<keldra_index::v6::ProjectionPartitionIdentity>, Status> {
    if !path.starts_with("_keldra/index-projections/v6/") {
        return Ok(None);
    }
    match keldra_index::v6::parse_projection_artifact_path(path) {
        Ok(artifact) => {
            if artifact.kind != keldra_index::v6::ProjectionArtifactKind::Current {
                return Ok(None);
            }
            let partition = artifact.partition.ok_or_else(|| {
                Status::invalid_argument("projection current has no partition identity")
            })?;
            if (partition.placement_term, partition.placement_index)
                != (placement.fence().term, placement.fence().index)
            {
                return Err(Status::failed_precondition(
                    "projection partition names a stale placement fence",
                ));
            }
            Ok(Some(partition))
        }
        Err(_) if keldra_index::v6::parse_projection_catalog_path(path).is_ok() => Ok(None),
        Err(error) => Err(Status::invalid_argument(error.to_string())),
    }
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
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status>;

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
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
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

    fn validate_active_publisher(
        &self,
        authenticated_node: NodeId,
        placement: &ClusterPlacement,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<(), Status> {
        if !placement.active_node_ids().contains(&authenticated_node) {
            return Err(Status::permission_denied(
                "immutable projection artifact caller is not ACTIVE",
            ));
        }
        if self
            .objects
            .routing_target_stable(key, tenant_id, bucket_id)?
            .is_some()
        {
            return Err(Status::failed_precondition(
                "index artifact request reached a node that is not its object coordinator",
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

    fn validate_catalog_authority(
        &self,
        authenticated_node: NodeId,
        placement: &ClusterPlacement,
        identity: IndexIdentity,
        key: &ObjectKey,
    ) -> Result<(), Status> {
        let authority = IndexPlacement::derive(identity, placement)
            .map_err(|error| Status::unavailable(error.to_string()))?
            .builder();
        if authority != authenticated_node {
            return Err(Status::permission_denied(
                "projection catalog caller is not its deterministic authority",
            ));
        }
        if self
            .objects
            .routing_target_stable(key, identity.tenant_id(), identity.bucket_id())?
            .is_some()
        {
            return Err(Status::failed_precondition(
                "projection catalog request reached a node that is not its object coordinator",
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
        let kind = request.validate()?;
        // The public/peer middleware already admits routed calls. Builders can
        // also publish locally, so explicitly retain the same existing
        // membership-cutover permit across the definition lock and artifact
        // mutation in that case.
        let _permit = self.objects.enter_mutation()?;
        self.require_fence(placement.fence())?;
        let identity = artifact_placement_identity(
            request.tenant_id,
            request.bucket_id,
            request.index_id,
            kind,
        )?;
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
        let identity = artifact_placement_identity(
            request.tenant_id,
            request.bucket_id,
            request.index_id,
            kind,
        )?;
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
        if let Some(partition) = projection_partition_owner(&request.exact_path, &placement)? {
            let source = SourceId {
                node_id: u16::try_from(partition.source_node).map_err(|_| {
                    Status::data_loss("v6 projection partition source node exceeds SourceId range")
                })?,
                source_epoch: partition.source_epoch,
            };
            let expected = super::placement::source_projection_producer(
                request.tenant_id,
                request.bucket_id,
                source,
                &placement,
            )
            .map_err(|error| Status::unavailable(error.to_string()))?;
            if partition.producer_node != expected.0 || expected != authenticated_builder {
                return Err(Status::permission_denied(
                    "projection current caller is not the placement-assigned source producer",
                ));
            }
            if self
                .objects
                .routing_target_stable(&key, identity.tenant_id(), identity.bucket_id())?
                .is_some()
            {
                return Err(Status::failed_precondition(
                    "index artifact request reached a node that is not its object coordinator",
                ));
            }
        } else if kind == ArtifactPathKind::ProjectionCatalogMutable {
            self.validate_catalog_authority(authenticated_builder, &placement, identity, &key)?;
        } else if kind == ArtifactPathKind::ProjectionImmutable {
            // Packs, stream pages, query-run blocks and generation records are
            // content-addressed. Any authenticated ACTIVE node may publish
            // their exact validated bytes; only partition `current` has a
            // producer authority and catalog mutables have catalog authority.
            self.validate_active_publisher(
                authenticated_builder,
                &placement,
                &key,
                request.tenant_id,
                request.bucket_id,
            )?;
        } else {
            self.validate_builder(authenticated_builder, &placement, identity, &key)?;
        }
        let mode = match (kind, request.expected_version) {
            (
                ArtifactPathKind::Current
                | ArtifactPathKind::ProjectionCurrent
                | ArtifactPathKind::ProjectionCatalogMutable
                | ArtifactPathKind::RebuildMutable
                | ArtifactPathKind::AccountingMutable,
                Some(version),
            ) => PutMode::PutIfVersion(version),
            (
                ArtifactPathKind::Current
                | ArtifactPathKind::ProjectionCurrent
                | ArtifactPathKind::ProjectionCatalogMutable
                | ArtifactPathKind::RebuildMutable
                | ArtifactPathKind::Immutable
                | ArtifactPathKind::ProjectionImmutable
                | ArtifactPathKind::AccountingMutable,
                None,
            ) => PutMode::PutIfAbsent,
            (ArtifactPathKind::Immutable | ArtifactPathKind::ProjectionImmutable, Some(_)) => {
                unreachable!("validated above")
            }
        };
        let content_type = match kind {
            ArtifactPathKind::AccountingMutable => ACCOUNTING_ARTIFACT_CONTENT_TYPE,
            ArtifactPathKind::Current
            | ArtifactPathKind::ProjectionCurrent
            | ArtifactPathKind::ProjectionCatalogMutable
            | ArtifactPathKind::RebuildMutable
            | ArtifactPathKind::Immutable
            | ArtifactPathKind::ProjectionImmutable => INDEX_ARTIFACT_CONTENT_TYPE,
        };
        let derived_progress = request.admission.is_publication_progress();
        let durability = artifact_durability(kind, placement.placement_nodes().len());
        let publish = PublishRequest {
            key,
            blob: request.blob,
            content_type: Some(content_type.into()),
            mode,
            command_id: Some(request.command_id),
            durability,
        };
        let receipt = if derived_progress && request.definition_intent.is_none() {
            self.objects
                .publish_derived_progress_from_source_with_governance(
                    publish,
                    authenticated_builder,
                    governance,
                )
                .await?
        } else if !derived_progress {
            self.objects
                .publish_from_source_with_governance_and_definition_intent(
                    publish,
                    authenticated_builder,
                    governance,
                    request.definition_intent,
                )
                .await?
        } else {
            return Err(Status::invalid_argument(
                "derived progress publication cannot mutate a definition",
            ));
        };
        self.require_fence(placement.fence())?;
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

    fn require_fence(&self, expected: keldra_store::PlacementLogId) -> Result<(), Status> {
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
    current_mutations: Arc<Mutex<BTreeMap<u64, Arc<tokio::sync::Mutex<()>>>>>,
}

pub(crate) struct IndexCurrentMutationGuard {
    index_id: u64,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

pub(crate) struct GuardedIndexArtifactPublish {
    pub request: IndexArtifactPublish,
    pub current_guard: IndexCurrentMutationGuard,
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
            current_mutations: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Serializes one current-pointer mutation with a destructive artifact
    /// decision for the same index. Immutable component publication does not
    /// use this gate and remains independently concurrent.
    pub(crate) async fn acquire_current_mutation(
        &self,
        index_id: u64,
    ) -> Result<IndexCurrentMutationGuard, Status> {
        let gate = {
            let mut gates = self.current_mutations.lock().map_err(|_| {
                Status::internal("index current-mutation gate registry is poisoned")
            })?;
            gates
                .entry(index_id)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        Ok(IndexCurrentMutationGuard {
            index_id,
            _guard: gate.lock_owned().await,
        })
    }

    pub(crate) async fn publish(
        &self,
        request: IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status> {
        let kind = request.validate()?;
        let _current_guard = if kind.is_current() {
            Some(self.acquire_current_mutation(request.index_id).await?)
        } else {
            None
        };
        self.publish_while_current_mutation_held(request, _current_guard.as_ref())
            .await
    }

    /// Publish bounded v6 content-addressed artifacts through their ordinary
    /// object coordinators. Immutable paths may originate on any ACTIVE node;
    /// one captured placement fence covers every local or remote subgroup.
    pub(crate) async fn publish_immutable_many(
        &self,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let placement = self.objects.current_program_placement()?;
        let fence = placement.fence();
        let mut groups =
            BTreeMap::<(NodeId, Option<String>), Vec<(usize, IndexArtifactPublish)>>::new();
        for (index, request) in requests.into_iter().enumerate() {
            if request.validate()? != ArtifactPathKind::ProjectionImmutable {
                return Err(Status::invalid_argument(
                    "v6 grouped publication accepts projection immutable artifacts only",
                ));
            }
            self.require_local_builder_for_kind(
                request.tenant_id,
                request.bucket_id,
                request.index_id,
                &request.exact_path,
                ArtifactPathKind::ProjectionImmutable,
            )?;
            let key = request.key()?;
            let target =
                self.objects
                    .routing_target_stable(&key, request.tenant_id, request.bucket_id)?;
            let (coordinator, address) = match target {
                Some((node, address)) => (node, Some(address)),
                None => (self.local_node, None),
            };
            groups
                .entry((coordinator, address))
                .or_default()
                .push((index, request));
        }
        let count = groups.values().map(Vec::len).sum();
        let mut outcomes = std::iter::repeat_with(|| None)
            .take(count)
            .collect::<Vec<_>>();
        for ((coordinator, address), group) in groups {
            for batch in bounded_artifact_batches(group)? {
                let (indices, publications): (Vec<_>, Vec<_>) = batch.into_iter().unzip();
                self.require_fence(fence)?;
                let published = match address.as_deref() {
                    Some(address) => {
                        self.peers
                            .publish_index_artifacts(coordinator, address, fence, &publications)
                            .await
                    }
                    None => {
                        self.coordinator
                            .publish_many(self.local_node, placement.clone(), publications)
                            .await
                    }
                };
                self.require_fence(fence)?;
                match published {
                    Ok(published) => {
                        record_grouped_artifact_outcomes(&mut outcomes, indices, published)?
                    }
                    Err(error) => {
                        for index in indices {
                            let slot = outcomes.get_mut(index).ok_or_else(|| {
                                Status::data_loss(
                                    "grouped immutable outcome index is out of bounds",
                                )
                            })?;
                            if slot.replace(Err(error.clone())).is_some() {
                                return Err(Status::data_loss(
                                    "grouped immutable outcome was recorded twice",
                                ));
                            }
                        }
                    }
                }
            }
        }
        ordered_grouped_artifact_outcomes(outcomes)
    }

    pub(crate) async fn publish_while_current_mutation_held(
        &self,
        request: IndexArtifactPublish,
        guard: Option<&IndexCurrentMutationGuard>,
    ) -> Result<IndexArtifactOutcome, Status> {
        let kind = request.validate()?;
        if kind.is_current() && guard.is_none_or(|guard| guard.index_id != request.index_id) {
            return Err(Status::internal(
                "current-pointer publication has no matching mutation guard",
            ));
        }
        let placement = self.require_local_builder_for_kind(
            request.tenant_id,
            request.bucket_id,
            request.index_id,
            &request.exact_path,
            kind,
        )?;
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
        self.delete_inner(request, None).await
    }

    pub(crate) async fn delete_while_current_mutation_held(
        &self,
        request: IndexArtifactDelete,
        guard: &IndexCurrentMutationGuard,
    ) -> Result<IndexArtifactOutcome, Status> {
        if guard.index_id != request.index_id {
            return Err(Status::internal(
                "artifact deletion has another index's current-mutation guard",
            ));
        }
        self.delete_inner(request, Some(guard)).await
    }

    async fn delete_inner(
        &self,
        request: IndexArtifactDelete,
        _guard: Option<&IndexCurrentMutationGuard>,
    ) -> Result<IndexArtifactOutcome, Status> {
        let kind = request.validate()?;
        let placement = self.require_local_builder_for_kind(
            request.tenant_id,
            request.bucket_id,
            request.index_id,
            &request.exact_path,
            kind,
        )?;
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

    pub(crate) fn is_local_builder(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        _index_id: u64,
    ) -> Result<bool, Status> {
        let placement = self.objects.current_program_placement()?;
        let identity = IndexIdentity::projection_partition(tenant_id, bucket_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let assignment = IndexPlacement::derive(identity, &placement)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        Ok(assignment.builder() == self.local_node)
    }

    /// Current producer assignment for one immutable source incarnation. The
    /// source stays local while ACTIVE; after removal this is the canonical
    /// capacity-weighted HRW successor selected from its source identity.
    pub(crate) fn source_projection_producer(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        source: SourceId,
    ) -> Result<(NodeId, PlacementLogId), Status> {
        let placement = self.objects.current_program_placement()?;
        let producer =
            super::placement::source_projection_producer(tenant_id, bucket_id, source, &placement)
                .map_err(|error| Status::unavailable(error.to_string()))?;
        Ok((producer, placement.fence()))
    }

    /// The single deterministic authority for a family lifecycle object.
    /// Partition `current` objects instead belong to their source owner; do
    /// not use this predicate for them.
    pub(crate) fn is_local_catalog_authority(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        catalog_routing_id: u64,
    ) -> Result<bool, Status> {
        let placement = self.objects.current_program_placement()?;
        let identity = IndexIdentity::new(tenant_id, bucket_id, catalog_routing_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let assignment = IndexPlacement::derive(identity, &placement)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        Ok(assignment.builder() == self.local_node)
    }

    fn require_local_builder(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        _index_id: u64,
    ) -> Result<ClusterPlacement, Status> {
        let placement = self.objects.current_program_placement()?;
        let identity = IndexIdentity::projection_partition(tenant_id, bucket_id)
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

    fn require_local_builder_for_kind(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
        exact_path: &str,
        kind: ArtifactPathKind,
    ) -> Result<ClusterPlacement, Status> {
        let placement = self.objects.current_program_placement()?;
        if kind == ArtifactPathKind::ProjectionImmutable {
            if placement.active_node_ids().contains(&self.local_node) {
                return Ok(placement);
            }
            return Err(Status::failed_precondition(
                "this node is no longer ACTIVE for immutable projection publication",
            ));
        }
        if let Some(partition) = projection_partition_owner(exact_path, &placement)? {
            let source = SourceId {
                node_id: u16::try_from(partition.source_node).map_err(|_| {
                    Status::data_loss("v6 projection partition source node exceeds SourceId range")
                })?,
                source_epoch: partition.source_epoch,
            };
            let expected = super::placement::source_projection_producer(
                tenant_id, bucket_id, source, &placement,
            )
            .map_err(|error| Status::unavailable(error.to_string()))?;
            if partition.producer_node != expected.0 || expected != self.local_node {
                return Err(Status::failed_precondition(
                    "this node is not the placement-assigned projection current producer",
                ));
            }
            return Ok(placement);
        }
        let identity = artifact_placement_identity(tenant_id, bucket_id, index_id, kind)?;
        let assignment = IndexPlacement::derive(identity, &placement)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if assignment.builder() != self.local_node {
            return Err(if kind == ArtifactPathKind::ProjectionCatalogMutable {
                Status::failed_precondition(
                    "this node is not the deterministic projection catalog authority",
                )
            } else {
                Status::failed_precondition(
                    "this node is not the current weighted-HRW index builder",
                )
            });
        }
        Ok(placement)
    }

    fn require_fence(&self, expected: keldra_store::PlacementLogId) -> Result<(), Status> {
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
    outcomes: &mut [Option<IndexArtifactPublicationOutcome>],
    indices: Vec<usize>,
    published: Vec<IndexArtifactPublicationOutcome>,
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

fn record_batch_publication_result(
    outcomes: &mut [Option<IndexArtifactPublicationOutcome>],
    indices: Vec<usize>,
    published: Result<Vec<IndexArtifactPublicationOutcome>, Status>,
) -> Result<(), Status> {
    let published = match published {
        Ok(published) if published.len() == indices.len() => published,
        Ok(_) => repeated_artifact_outcome_error(
            indices.len(),
            Status::internal("physical index artifact outcome count differs from its request"),
        ),
        Err(error) => repeated_artifact_outcome_error(indices.len(), error),
    };
    record_grouped_artifact_outcomes(outcomes, indices, published)
}

fn repeated_artifact_outcome_error(
    count: usize,
    error: Status,
) -> Vec<IndexArtifactPublicationOutcome> {
    (0..count)
        .map(|_| Err(Status::new(error.code(), error.message().to_owned())))
        .collect()
}

fn ordered_grouped_artifact_outcomes(
    outcomes: Vec<Option<IndexArtifactPublicationOutcome>>,
) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
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
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        self.publish_v6_immutable_many(authenticated_builder, placement, requests)
            .await
    }

    async fn commit_guarded(
        &self,
        authenticated_definition_coordinator: NodeId,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        request: IndexArtifactPublish,
    ) -> Result<IndexArtifactOutcome, Status> {
        let kind = request.validate()?;
        let guard = request
            .definition_guard
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("guarded commit has no definition guard"))?;
        let identity = artifact_placement_identity(
            request.tenant_id,
            request.bucket_id,
            request.index_id,
            kind,
        )?;
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
        let identity = artifact_placement_identity(
            request.tenant_id,
            request.bucket_id,
            request.index_id,
            kind,
        )?;
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
    current: Option<&keldra_store::CurrentObjectSnapshot>,
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
        if !request.validate()?.is_immutable() {
            return Err(Status::invalid_argument(
                "grouped index publication accepts immutable artifacts only",
            ));
        }
        if request.storage_tenant != first.storage_tenant
            || request.bucket != first.bucket
            || request.tenant_id != first.tenant_id
            || request.bucket_id != first.bucket_id
            || request.admission != first.admission
        {
            return Err(Status::invalid_argument(
                "grouped index artifacts must share one governed bucket and admission",
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
        ArtifactPathKind::Current
        | ArtifactPathKind::ProjectionCurrent
        | ArtifactPathKind::ProjectionCatalogMutable
        | ArtifactPathKind::RebuildMutable
        | ArtifactPathKind::Immutable
        | ArtifactPathKind::ProjectionImmutable
            if active_nodes == 1 =>
        {
            Durability::Local
        }
        ArtifactPathKind::Current
        | ArtifactPathKind::ProjectionCurrent
        | ArtifactPathKind::ProjectionCatalogMutable
        | ArtifactPathKind::RebuildMutable
        | ArtifactPathKind::Immutable
        | ArtifactPathKind::ProjectionImmutable => Durability::Replicated,
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
            admission: DerivedArtifactAdmission::Bounded,
        }
    }

    #[test]
    fn progress_admission_is_explicit_and_limited_to_complete_derived_artifacts() {
        let mut manifest = artifact_publish(manifest_path(7, [3; 32]), None);
        manifest.admission = DerivedArtifactAdmission::PublicationProgress;
        assert_eq!(manifest.validate().unwrap(), ArtifactPathKind::Immutable);

        let mut outbound =
            artifact_publish(crate::accounting::outbound_source_path(7, 1).unwrap(), None);
        outbound.admission = DerivedArtifactAdmission::PublicationProgress;
        assert!(outbound.validate().is_err());
    }

    #[test]
    fn current_publication_requires_its_exact_typed_definition_guard() {
        let current = current_path(7);
        assert!(artifact_publish(current.clone(), None).validate().is_err());

        let valid = DefinitionVersionGuard {
            kind: DefinitionKind::Index,
            exact_path: "_keldra/indices/v4/definitions/search".into(),
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
    fn v6_projection_paths_bind_full_partition_routing_hash_and_cas_shape() {
        let family = [7; 32];
        let partition =
            keldra_index::v6::ProjectionPartitionIdentity::new(family, 3, [4; 32], 5, 6, 8)
                .unwrap();
        let routing = keldra_index::v6::projection_routing_id(partition);
        let mut immutable = artifact_publish(
            keldra_index::v6::projection_pack_path(partition, [3; 32]),
            None,
        );
        immutable.index_id = routing;
        assert_eq!(
            immutable.validate().unwrap(),
            ArtifactPathKind::ProjectionImmutable
        );

        immutable.index_id = routing.wrapping_add(1).max(1);
        assert!(immutable.validate().is_err());
        immutable.index_id = routing;
        immutable.blob.hash = [4; 32];
        assert!(immutable.validate().is_err());

        let mut current =
            artifact_publish(keldra_index::v6::projection_current_path(partition), None);
        current.index_id = routing;
        assert_eq!(
            current.validate().unwrap(),
            ArtifactPathKind::ProjectionCurrent
        );
        current.expected_version = Some(VersionId(9));
        assert_eq!(
            current.validate().unwrap(),
            ArtifactPathKind::ProjectionCurrent
        );
        current.expected_version = Some(VersionId(0));
        assert!(current.validate().is_err());
    }

    #[test]
    fn multiple_artifacts_share_one_bounded_grouped_mutation() {
        let first = artifact_publish(artifact_path(7, [3; 32]), None);
        let mut second = artifact_publish(artifact_path(7, [5; 32]), None);
        second.blob.hash = [5; 32];
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
    fn grouped_publication_preserves_partial_failure_at_its_request_index() {
        let outcome = IndexArtifactOutcome {
            version: VersionId(10),
            replayed: false,
        };
        let mut slots = std::iter::repeat_with(|| None).take(2).collect::<Vec<_>>();
        record_grouped_artifact_outcomes(
            &mut slots,
            vec![1, 0],
            vec![Err(Status::aborted("lost CAS")), Ok(outcome)],
        )
        .unwrap();
        let ordered = ordered_grouped_artifact_outcomes(slots).unwrap();
        assert_eq!(ordered[0].as_ref().unwrap().version, VersionId(10));
        assert_eq!(
            ordered[1].as_ref().unwrap_err().code(),
            tonic::Code::Aborted
        );
    }
}
