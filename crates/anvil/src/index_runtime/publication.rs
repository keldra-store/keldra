//! Narrow ordinary-object publication boundary for index generations.

use std::sync::{Arc, OnceLock};

use anvil_consensus::NodeId;
use anvil_store::{
    BatchOperation, BlobRef, DeleteRequest, DeleteRetainedVersionOutcome, Durability, ObjectKey,
    ObjectVersioning, Precondition, PublishRequest, PutMode, VersionId,
};
use tonic::Status;

use crate::bucket_governance::BucketGovernance;
use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_placement::ClusterPlacement;
use crate::object_distribution::ObjectDistribution;

use super::placement::{IndexIdentity, IndexPlacement};

const INDEX_ARTIFACT_CONTENT_TYPE: &str = "application/vnd.anvil.index-artifact";
const ACCOUNTING_ARTIFACT_CONTENT_TYPE: &str = "application/vnd.anvil.accounting+json";

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
        parse_artifact_path(&self.exact_path, self.index_id)
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
        }
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
}

/// Validates the builder/fence/path and enters the existing ordinary object
/// coordinator. It owns no bytes or metadata persistence of its own.
#[derive(Clone)]
pub(crate) struct IndexArtifactCoordinator {
    objects: ObjectDistribution,
    governance: BucketGovernance,
}

impl IndexArtifactCoordinator {
    pub(crate) fn new(objects: ObjectDistribution, governance: BucketGovernance) -> Self {
        Self {
            objects,
            governance,
        }
    }

    fn validate_builder(
        &self,
        authenticated_builder: NodeId,
        placement: &ClusterPlacement,
        identity: IndexIdentity,
        key: &ObjectKey,
    ) -> Result<(), Status> {
        let assignment = IndexPlacement::derive(identity, placement)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if assignment.builder() != authenticated_builder {
            return Err(Status::permission_denied(
                "index artifact caller is not the current weighted-HRW builder",
            ));
        }
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
        let key = request.key()?;
        let outcome =
            match self
                .objects
                .routing_target_stable(&key, request.tenant_id, request.bucket_id)?
            {
                Some((target, address)) => {
                    self.peers
                        .publish_index_artifact(target, &address, &request)
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
            .publish_from_source_with_governance(
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
            )
            .await?;
        Ok(IndexArtifactOutcome {
            version: receipt.version,
            replayed: receipt.replayed,
        })
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
        if governance.versioning == ObjectVersioning::Enabled {
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
            .mutate_with_governance(
                BatchOperation::Delete(DeleteRequest {
                    key,
                    precondition: Precondition::Version(request.expected_version),
                    command_id: Some(request.command_id),
                    durability,
                }),
                governance,
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
