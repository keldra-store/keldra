//! Narrow ordinary-object publication boundary for index commits.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use keldra_consensus::NodeId;
use keldra_store::{
    BatchOperation, BlobRef, DefinitionKind, DefinitionMutationIntent, DeleteRequest,
    DeleteRetainedVersionOutcome, Durability, ObjectKey, ObjectVersioning, PlacementLogId,
    Precondition, PublishRequest, PutMode, Store, VersionId,
};
use tonic::Status;

use crate::bucket_governance::BucketGovernance;
use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_placement::ClusterPlacement;
use crate::object_distribution::ObjectDistribution;

use super::placement::{IndexIdentity, IndexPlacement};

mod cohort;

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

/// Exact physical routing identity for one guarded current-pointer cohort.
/// The placement fence prevents candidates prepared across membership cuts
/// from sharing a queue epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GuardedIndexArtifactCohort {
    storage_tenant: String,
    bucket: String,
    tenant_id: u64,
    bucket_id: u64,
    admission: DerivedArtifactAdmission,
    fence: PlacementLogId,
    definition_replicas: Vec<NodeId>,
    current_replicas: Vec<NodeId>,
}

#[cfg(test)]
impl GuardedIndexArtifactCohort {
    pub(crate) fn test_key(
        definition_replicas: Vec<NodeId>,
        current_replicas: Vec<NodeId>,
    ) -> Self {
        Self {
            storage_tenant: "tenant".into(),
            bucket: "bucket".into(),
            tenant_id: 1,
            bucket_id: 2,
            admission: DerivedArtifactAdmission::PublicationProgress,
            fence: PlacementLogId { term: 3, index: 7 },
            definition_replicas,
            current_replicas,
        }
    }
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
        if kind == ArtifactPathKind::Immutable
            && immutable_content_hash_from_path(self.index_id, &self.exact_path)
                != Some(self.blob.hash)
        {
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
            (ArtifactPathKind::RebuildMutable, Some(VersionId(0))) => Err(
                Status::invalid_argument("index rebuild-root expected version must be non-zero"),
            ),
            (ArtifactPathKind::AccountingMutable, Some(VersionId(0))) => Err(
                Status::invalid_argument("accounting artifact expected version must be non-zero"),
            ),
            (
                ArtifactPathKind::Current
                | ArtifactPathKind::RebuildMutable
                | ArtifactPathKind::AccountingMutable,
                _,
            )
            | (ArtifactPathKind::Immutable, None) => Ok(kind),
            (ArtifactPathKind::Immutable, Some(_)) => Err(Status::invalid_argument(
                "immutable index commit artifacts cannot be replaced",
            )),
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactPathKind {
    Current,
    RebuildMutable,
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
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status>;

    async fn publish_guarded_many(
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

    async fn commit_guarded_many(
        &self,
        authenticated_definition_coordinator: NodeId,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status>;

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

    async fn publish_guarded_many(
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
            .publish_guarded_many(authenticated_builder, placement, requests)
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

    async fn commit_guarded_many(
        &self,
        authenticated_definition_coordinator: NodeId,
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
            .commit_guarded_many(
                authenticated_definition_coordinator,
                authenticated_builder,
                placement,
                requests,
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
            (
                ArtifactPathKind::Current
                | ArtifactPathKind::RebuildMutable
                | ArtifactPathKind::AccountingMutable,
                Some(version),
            ) => PutMode::PutIfVersion(version),
            (
                ArtifactPathKind::Current
                | ArtifactPathKind::RebuildMutable
                | ArtifactPathKind::Immutable
                | ArtifactPathKind::AccountingMutable,
                None,
            ) => PutMode::PutIfAbsent,
            (ArtifactPathKind::Immutable, Some(_)) => unreachable!("validated above"),
        };
        let content_type = match kind {
            ArtifactPathKind::AccountingMutable => ACCOUNTING_ARTIFACT_CONTENT_TYPE,
            ArtifactPathKind::Current
            | ArtifactPathKind::RebuildMutable
            | ArtifactPathKind::Immutable => INDEX_ARTIFACT_CONTENT_TYPE,
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
        request.validate()?;
        let _current_guard = if request.exact_path == current_path(request.index_id) {
            Some(self.acquire_current_mutation(request.index_id).await?)
        } else {
            None
        };
        self.publish_while_current_mutation_held(request, _current_guard.as_ref())
            .await
    }

    pub(crate) async fn publish_while_current_mutation_held(
        &self,
        request: IndexArtifactPublish,
        guard: Option<&IndexCurrentMutationGuard>,
    ) -> Result<IndexArtifactOutcome, Status> {
        request.validate()?;
        if request.exact_path == current_path(request.index_id)
            && guard.is_none_or(|guard| guard.index_id != request.index_id)
        {
            return Err(Status::internal(
                "current-pointer publication has no matching mutation guard",
            ));
        }
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

    pub(crate) fn is_local_builder(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    ) -> Result<bool, Status> {
        let placement = self.objects.current_program_placement()?;
        let identity = IndexIdentity::new(tenant_id, bucket_id, index_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let assignment = IndexPlacement::derive(identity, &placement)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        Ok(assignment.builder() == self.local_node)
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
        self.publish_immutable_many(authenticated_builder, placement, requests)
            .await
    }

    async fn publish_guarded_many(
        &self,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        validate_guarded_batch(&requests)?;
        let _permit = self.objects.enter_mutation()?;
        self.require_fence(placement.fence())?;
        let mut definition_keys = Vec::with_capacity(requests.len());
        for request in &requests {
            let identity =
                IndexIdentity::new(request.tenant_id, request.bucket_id, request.index_id)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
            self.validate_index_builder(authenticated_builder, &placement, identity)?;
            let key = request
                .definition_guard
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("guarded publication has no guard"))?
                .key(&request.storage_tenant, &request.bucket)?;
            if self.objects.object_coordinator_stable(
                &placement,
                &key,
                request.tenant_id,
                request.bucket_id,
            )? != self.objects.local_node()
            {
                return Err(Status::failed_precondition(
                    "grouped guarded publication did not reach the shared definition-path coordinator",
                ));
            }
            definition_keys.push(key);
        }
        let locked_keys = definition_keys.clone();
        self.store
            .with_ordinary_object_path_locks(&definition_keys, move || async move {
                let request_count = requests.len();
                let mut outcomes = std::iter::repeat_with(|| None)
                    .take(request_count)
                    .collect::<Vec<_>>();
                let mut valid = Vec::with_capacity(request_count);
                for (index, (key, request)) in locked_keys.iter().zip(requests).enumerate() {
                    let validation = self
                        .require_current_definition(&placement, key, &request)
                        .await;
                    cohort::record_definition_guard_outcome(
                        &mut outcomes,
                        &mut valid,
                        index,
                        request,
                        validation,
                    )?;
                }
                let Some((_, first)) = valid.first() else {
                    return ordered_grouped_artifact_outcomes(outcomes);
                };
                let artifact_group = self.objects.object_replica_group_stable(
                    &placement,
                    &first.key()?,
                    first.tenant_id,
                    first.bucket_id,
                )?;
                for (_, request) in &valid[1..] {
                    let candidate = self.objects.object_replica_group_stable(
                        &placement,
                        &request.key()?,
                        request.tenant_id,
                        request.bucket_id,
                    )?;
                    if candidate != artifact_group {
                        return Err(Status::invalid_argument(
                            "grouped guarded publication spans current-pointer replica groups",
                        ));
                    }
                }
                let coordinator = artifact_group.coordinator();
                let (indices, publications): (Vec<_>, Vec<_>) = valid.into_iter().unzip();
                let published = if coordinator == self.objects.local_node() {
                    self.publish_mutable_many(
                        authenticated_builder,
                        placement.clone(),
                        publications,
                    )
                    .await?
                } else {
                    let address = placement.address(coordinator).ok_or_else(|| {
                        Status::unavailable(format!(
                            "ACTIVE artifact coordinator {} has no peer address",
                            coordinator.0
                        ))
                    })?;
                    self.peers
                        .commit_guarded_index_artifacts(
                            coordinator,
                            &address.0,
                            placement.fence(),
                            authenticated_builder,
                            &publications,
                        )
                        .await?
                };
                record_grouped_artifact_outcomes(&mut outcomes, indices, published)?;
                self.require_fence(placement.fence())?;
                ordered_grouped_artifact_outcomes(outcomes)
            })
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

    async fn commit_guarded_many(
        &self,
        authenticated_definition_coordinator: NodeId,
        authenticated_builder: NodeId,
        placement: ClusterPlacement,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        validate_guarded_batch(&requests)?;
        for request in &requests {
            let identity =
                IndexIdentity::new(request.tenant_id, request.bucket_id, request.index_id)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
            self.validate_index_builder(authenticated_builder, &placement, identity)?;
            let guard = request.definition_guard.as_ref().ok_or_else(|| {
                Status::invalid_argument("guarded commit has no definition guard")
            })?;
            let definition_key = guard.key(&request.storage_tenant, &request.bucket)?;
            if self.objects.object_coordinator_stable(
                &placement,
                &definition_key,
                request.tenant_id,
                request.bucket_id,
            )? != authenticated_definition_coordinator
            {
                return Err(Status::permission_denied(
                    "guarded artifact batch caller is not every definition-path coordinator",
                ));
            }
        }
        self.publish_mutable_many(authenticated_builder, placement, requests)
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
        if request.validate()? != ArtifactPathKind::Immutable {
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

fn validate_guarded_batch(requests: &[IndexArtifactPublish]) -> Result<(), Status> {
    if requests.is_empty() || requests.len() > MAX_INDEX_ARTIFACT_BATCH_ITEMS {
        return Err(Status::resource_exhausted(format!(
            "guarded index artifact batch must contain 1..={MAX_INDEX_ARTIFACT_BATCH_ITEMS} items"
        )));
    }
    let first = &requests[0];
    let mut bytes = 0_u64;
    for request in requests {
        if request.validate()? != ArtifactPathKind::Current {
            return Err(Status::invalid_argument(
                "grouped guarded publication accepts current pointers only",
            ));
        }
        if request.storage_tenant != first.storage_tenant
            || request.bucket != first.bucket
            || request.tenant_id != first.tenant_id
            || request.bucket_id != first.bucket_id
            || request.admission != first.admission
        {
            return Err(Status::invalid_argument(
                "grouped guarded index artifacts must share one governed bucket and admission",
            ));
        }
        bytes = bytes.checked_add(request.blob.length).ok_or_else(|| {
            Status::resource_exhausted("guarded index artifact batch byte count overflow")
        })?;
    }
    if bytes > MAX_INDEX_ARTIFACT_BATCH_BYTES {
        return Err(Status::resource_exhausted(format!(
            "guarded index artifact batch exceeds {MAX_INDEX_ARTIFACT_BATCH_BYTES} logical bytes"
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
        | ArtifactPathKind::RebuildMutable
        | ArtifactPathKind::Immutable
            if active_nodes == 1 =>
        {
            Durability::Local
        }
        ArtifactPathKind::Current
        | ArtifactPathKind::RebuildMutable
        | ArtifactPathKind::Immutable => Durability::Replicated,
    }
}

fn parse_artifact_path(path: &str, expected_index: u64) -> Result<ArtifactPathKind, Status> {
    if crate::accounting::is_artifact_path(path, expected_index) {
        return Ok(ArtifactPathKind::AccountingMutable);
    }
    let parts = path.split('/').collect::<Vec<_>>();
    if parts.len() < 5
        || parts[0] != "_keldra"
        || parts[1] != "indices"
        || parts[2] != "v4"
        || parse_canonical_u64(parts[3]) != Some(expected_index)
    {
        return Err(Status::invalid_argument(
            "index artifact path is outside its reserved index namespace",
        ));
    }
    match parts.as_slice() {
        [_, _, _, _, "current"] => Ok(ArtifactPathKind::Current),
        [_, _, _, _, "rebuild"] => Ok(ArtifactPathKind::RebuildMutable),
        [_, _, _, _, "manifests", digest] if valid_digest(digest) => {
            Ok(ArtifactPathKind::Immutable)
        }
        [_, _, _, _, "artifacts", digest] if valid_digest(digest) => {
            Ok(ArtifactPathKind::Immutable)
        }
        _ => Err(Status::invalid_argument(
            "index artifact path does not name a v4 current pointer, manifest, or artifact",
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
        ["_keldra", "indices", "v4", "definitions", name] if valid_definition_name(name) => {
            Some(name)
        }
        _ => None,
    }
}

fn valid_definition_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name != "."
        && name != ".."
        && !name.contains(['/', '\0'])
}

pub(crate) fn manifest_path(index_id: u64, digest: [u8; 32]) -> String {
    keldra_index::v4::manifest_path(index_id, digest)
}

pub(crate) fn artifact_path(index_id: u64, digest: [u8; 32]) -> String {
    keldra_index::v4::artifact_path(index_id, digest)
}

/// Extract an artifact identity only from one complete canonical v4 path.
/// Retention uses this instead of textual prefix matching so an adjacent
/// digest or extra segment cannot widen a deletion scope.
pub(crate) fn artifact_hash_from_path(index_id: u64, path: &str) -> Option<[u8; 32]> {
    let parts = path.split('/').collect::<Vec<_>>();
    let digest = match parts.as_slice() {
        [
            "_keldra",
            "indices",
            "v4",
            encoded_index,
            "artifacts",
            digest,
        ] if parse_canonical_u64(encoded_index) == Some(index_id) && valid_digest(digest) => {
            *digest
        }
        _ => return None,
    };
    let decoded = hex::decode(digest).ok()?;
    decoded.try_into().ok()
}

fn immutable_content_hash_from_path(index_id: u64, path: &str) -> Option<[u8; 32]> {
    if let Some(hash) = artifact_hash_from_path(index_id, path) {
        return Some(hash);
    }
    let parts = path.split('/').collect::<Vec<_>>();
    let digest = match parts.as_slice() {
        [
            "_keldra",
            "indices",
            "v4",
            encoded_index,
            "manifests",
            digest,
        ] if parse_canonical_u64(encoded_index) == Some(index_id) && valid_digest(digest) => {
            *digest
        }
        _ => return None,
    };
    hex::decode(digest).ok()?.try_into().ok()
}

pub(crate) fn manifest_hash_from_path(index_id: u64, path: &str) -> Option<[u8; 32]> {
    let hash = immutable_content_hash_from_path(index_id, path)?;
    is_manifest_artifact_path(index_id, path).then_some(hash)
}

pub(crate) fn is_manifest_artifact_path(index_id: u64, path: &str) -> bool {
    let parts = path.split('/').collect::<Vec<_>>();
    matches!(
        parts.as_slice(),
        ["_keldra", "indices", "v4", encoded_index, "manifests", digest]
            if parse_canonical_u64(encoded_index) == Some(index_id) && valid_digest(digest)
    )
}

pub(crate) fn current_path(index_id: u64) -> String {
    keldra_index::v4::current_path(index_id)
}

pub(crate) fn rebuild_path(index_id: u64) -> String {
    format!("_keldra/indices/v4/{index_id}/rebuild")
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
            admission: DerivedArtifactAdmission::Bounded,
        }
    }

    #[test]
    fn only_exact_reserved_artifact_shapes_are_accepted() {
        assert_eq!(
            parse_artifact_path("_keldra/indices/v4/7/current", 7).unwrap(),
            ArtifactPathKind::Current
        );
        let digest = "a".repeat(64);
        assert_eq!(
            parse_artifact_path(&format!("_keldra/indices/v4/7/manifests/{digest}"), 7).unwrap(),
            ArtifactPathKind::Immutable
        );
        assert_eq!(
            parse_artifact_path(&format!("_keldra/indices/v4/7/artifacts/{digest}"), 7).unwrap(),
            ArtifactPathKind::Immutable
        );
        for invalid in [
            "_keldra/indices/v4/7/definition",
            "_keldra/indices/v4/7/runs/name/descriptor",
            "_keldra/indices/v4/07/current",
            "_keldra/indices/7/current",
            "_keldra/indices/v3/7/current",
            "ordinary/path",
        ] {
            assert!(parse_artifact_path(invalid, 7).is_err(), "{invalid}");
        }
        assert!(
            parse_artifact_path(
                &format!("_keldra/indices/v4/7/artifacts/{}", "A".repeat(64)),
                7,
            )
            .is_err()
        );
        assert!(
            parse_artifact_path(&format!("_keldra/indices/v4/8/artifacts/{digest}"), 7).is_err()
        );
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
    fn definition_discovery_accepts_only_the_dedicated_path_shape() {
        assert_eq!(
            index_definition_name("_keldra/indices/v4/definitions/search"),
            Some("search")
        );
        assert_eq!(
            index_definition_name("_keldra/indices/v4/12/definition"),
            None
        );
        assert_eq!(
            index_definition_name("_keldra/indices/v4/definitions/a/b"),
            None
        );
        assert_eq!(
            index_definition_name("_keldra/indices/v3/definitions/search"),
            None
        );
        assert_eq!(
            index_definition_name(&format!(
                "_keldra/indices/v4/definitions/{}",
                "a".repeat(256)
            )),
            None
        );
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
    fn guards_are_rejected_on_immutable_or_wrong_accounting_paths() {
        let index_guard = DefinitionVersionGuard {
            kind: DefinitionKind::Index,
            exact_path: "_keldra/indices/v4/definitions/search".into(),
            expected_version: VersionId(9),
        };
        assert!(
            artifact_publish(manifest_path(7, [3; 32]), Some(index_guard))
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
        assert_eq!(current_path(4), "_keldra/indices/v4/4/current");
        assert!(parse_artifact_path(&manifest_path(4, [2; 32]), 4).is_ok());
        assert!(parse_artifact_path(&artifact_path(4, [3; 32]), 4).is_ok());
        assert_eq!(
            artifact_hash_from_path(4, &artifact_path(4, [3; 32])),
            Some([3; 32])
        );
        assert!(is_manifest_artifact_path(4, &manifest_path(4, [2; 32])));
    }

    #[test]
    fn immutable_publication_path_is_bound_to_the_object_hash() {
        let mismatched = artifact_publish(artifact_path(7, [4; 32]), None);
        assert!(mismatched.validate().is_err());

        let mismatched_manifest = artifact_publish(manifest_path(7, [4; 32]), None);
        assert!(mismatched_manifest.validate().is_err());

        let matched = artifact_publish(artifact_path(7, [3; 32]), None);
        assert_eq!(matched.validate().unwrap(), ArtifactPathKind::Immutable);

        let matched_manifest = artifact_publish(manifest_path(7, [3; 32]), None);
        assert_eq!(
            matched_manifest.validate().unwrap(),
            ArtifactPathKind::Immutable
        );
    }

    #[test]
    fn artifact_retention_parser_is_slash_safe_and_v4_only() {
        let digest = hex::encode([3; 32]);
        for invalid in [
            format!("_keldra/indices/v4/4/artifacts/{digest}/"),
            format!("_keldra/indices/v4/4/artifacts/{digest}/extra"),
            format!("_keldra/indices/v4/4/artifacts/{digest}0"),
            format!("_keldra/indices/4/artifacts/{digest}"),
            format!("_keldra/indices/v4/04/artifacts/{digest}"),
            format!("_keldra/indices/v3/4/artifacts/{digest}"),
        ] {
            assert_eq!(artifact_hash_from_path(4, &invalid), None, "{invalid}");
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
    fn current_pointer_cannot_enter_an_artifact_batch() {
        let artifact = artifact_publish(artifact_path(7, [3; 32]), None);
        let current = artifact_publish(
            current_path(7),
            Some(DefinitionVersionGuard {
                kind: DefinitionKind::Index,
                exact_path: "_keldra/indices/v4/definitions/search".into(),
                expected_version: VersionId(9),
            }),
        );

        assert!(validate_immutable_batch(&[artifact, current]).is_err());
    }

    #[test]
    fn grouped_publication_restores_request_order_across_replica_groups() {
        let outcome = |version| IndexArtifactOutcome {
            version: VersionId(version),
            replayed: false,
        };
        let mut slots = std::iter::repeat_with(|| None).take(4).collect::<Vec<_>>();

        // Replica groups are visited by their placement key, not input order.
        record_grouped_artifact_outcomes(
            &mut slots,
            vec![2, 0],
            vec![Ok(outcome(30)), Ok(outcome(10))],
        )
        .unwrap();
        record_grouped_artifact_outcomes(
            &mut slots,
            vec![3, 1],
            vec![Ok(outcome(40)), Ok(outcome(20))],
        )
        .unwrap();

        let ordered = ordered_grouped_artifact_outcomes(slots).unwrap();
        assert_eq!(
            ordered
                .into_iter()
                .map(|entry| entry.unwrap().version.0)
                .collect::<Vec<_>>(),
            vec![10, 20, 30, 40]
        );
    }

    #[test]
    fn immutable_batch_accepts_multiple_indices_in_one_governed_bucket() {
        let first = artifact_publish(artifact_path(7, [3; 32]), None);
        let mut second = artifact_publish(artifact_path(11, [5; 32]), None);
        second.index_id = 11;
        second.blob.hash = [5; 32];
        assert!(validate_immutable_batch(&[first, second]).is_ok());
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
