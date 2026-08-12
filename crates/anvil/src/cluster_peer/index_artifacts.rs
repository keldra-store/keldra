use anvil_store::{
    BlobRef, CurrentHeadCursor, CurrentObjectSnapshot, DefinitionKind, DefinitionMutationIntent,
    Head, ObjectRecordCursor, RetainedObjectCursor, RetainedObjectSnapshot, Version, VersionId,
};
use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status};

use anvil_atomic_program::MAX_OBJECT_PATH_BYTES;
use anvil_consensus::NodeId;

use super::storage::{bounded_blocking, object_coordinator};
use super::{CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, wire};
use crate::index_runtime::publication::{
    DefinitionVersionGuard, IndexArtifactDelete, IndexArtifactPublication, IndexArtifactPublish,
    is_index_recovery_path,
};

const INDEX_HEAD_SCAN_MAX_RECORDS: u32 = 128;
const INDEX_HEAD_SCAN_MAX_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum IndexHeadScanScope {
    Artifacts {
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    },
    Run {
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
        run_hash: [u8; 32],
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct IndexCurrentHead {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub exact_path: String,
    pub head: Head,
    pub version: Version,
    /// The descriptor represented by this record. Artifact-retention pages
    /// carry one descriptor at a time, keeping a deeply-versioned path bounded.
    pub versions: Vec<Version>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexHeadScanPage {
    pub source: anvil_store::SourceId,
    pub placement_fence: anvil_store::PlacementLogId,
    pub heads: Vec<IndexCurrentHead>,
    pub next_cursor: Option<ObjectRecordCursor>,
}

impl ClusterPeerService {
    pub(super) async fn publish_index_artifact_call(
        &self,
        request: Request<wire::PublishIndexArtifactRequest>,
    ) -> Result<Response<wire::IndexArtifactPublished>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let value = decode_request(request.get_ref())?;
        let fence = admitted.placement.fence();
        let receipt = tokio::time::timeout(
            admitted.timeout,
            self.index_artifacts
                .publish(admitted.authenticated.node_id, admitted.placement, value),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("index artifact publication deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(wire::IndexArtifactPublished {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            version: receipt.version.0,
            replayed: receipt.replayed,
        }))
    }

    pub(super) async fn commit_guarded_index_artifact_call(
        &self,
        request: Request<wire::CommitGuardedIndexArtifactRequest>,
    ) -> Result<Response<wire::IndexArtifactPublished>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let request = request.into_inner();
        if request.builder_node_id == 0 {
            return Err(Status::invalid_argument(
                "guarded artifact commit builder must be non-zero",
            ));
        }
        let publication = request
            .publication
            .ok_or_else(|| Status::invalid_argument("guarded artifact publication is required"))?;
        if publication.peer.is_some() {
            return Err(Status::invalid_argument(
                "nested guarded artifact publication must not carry peer context",
            ));
        }
        let value = decode_request(&publication)?;
        if value.definition_guard.is_none() {
            return Err(Status::invalid_argument(
                "guarded artifact commit requires a definition guard",
            ));
        }
        let fence = admitted.placement.fence();
        let receipt = tokio::time::timeout(
            admitted.timeout,
            self.index_artifacts.commit_guarded(
                admitted.authenticated.node_id,
                NodeId(request.builder_node_id),
                admitted.placement,
                value,
            ),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("guarded artifact commit deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(wire::IndexArtifactPublished {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            version: receipt.version.0,
            replayed: receipt.replayed,
        }))
    }

    pub(super) async fn delete_index_artifact_call(
        &self,
        request: Request<wire::DeleteIndexArtifactRequest>,
    ) -> Result<Response<wire::IndexArtifactDeleted>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let value = decode_delete(request.get_ref())?;
        let fence = admitted.placement.fence();
        let receipt = tokio::time::timeout(
            admitted.timeout,
            self.index_artifacts
                .delete(admitted.authenticated.node_id, admitted.placement, value),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("index artifact deletion deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(wire::IndexArtifactDeleted {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            version: receipt.version.0,
            replayed: receipt.replayed,
        }))
    }

    pub(super) async fn scan_index_heads_call(
        &self,
        request: Request<wire::ScanIndexHeadsRequest>,
    ) -> Result<Response<wire::IndexHeadScanPage>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let scope = decode_scan_scope(request.get_ref())?;
        let fence = admitted.placement.fence();
        let store = self.store.clone();
        let (heads, next_cursor) = if matches!(&scope, IndexHeadScanScope::Artifacts { .. }) {
            let cursor = request
                .get_ref()
                .cursor
                .clone()
                .map(RetainedObjectCursor::from_token)
                .transpose()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let (tenant_id, bucket_id, prefix) = scope.prefix();
            let page = bounded_blocking(admitted.timeout, move || {
                store
                    .export_retained_objects_by_prefix(
                        tenant_id,
                        bucket_id,
                        &prefix,
                        cursor.as_ref(),
                        INDEX_HEAD_SCAN_MAX_RECORDS,
                        INDEX_HEAD_SCAN_MAX_BYTES,
                    )
                    .map_err(|error| Status::internal(error.to_string()))
            })
            .await?;
            let heads = page
                .records
                .into_iter()
                .filter(|snapshot| {
                    scope.matches(snapshot.tenant_id, snapshot.bucket_id, &snapshot.exact_path)
                        && source_retained_coordinator(snapshot, &admitted.placement)
                            == Some(self.local_node)
                })
                .map(retained_object_head)
                .collect::<Vec<_>>();
            (
                heads,
                page.next_cursor.map(|cursor| cursor.as_token().to_owned()),
            )
        } else {
            let cursor = request
                .get_ref()
                .cursor
                .clone()
                .map(CurrentHeadCursor::from_token)
                .transpose()
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            let (tenant_id, bucket_id, prefix) = scope.prefix();
            let page = bounded_blocking(admitted.timeout, move || {
                store
                    .export_current_heads_by_prefix(
                        tenant_id,
                        bucket_id,
                        &prefix,
                        cursor.as_ref(),
                        INDEX_HEAD_SCAN_MAX_RECORDS,
                        INDEX_HEAD_SCAN_MAX_BYTES,
                    )
                    .map_err(|error| Status::internal(error.to_string()))
            })
            .await?;
            let heads = page
                .heads
                .into_iter()
                .filter(|snapshot| {
                    include_current_snapshot(
                        &scope,
                        snapshot,
                        self.local_node,
                        source_current_coordinator(snapshot, &admitted.placement),
                    )
                })
                .map(current_object_head)
                .collect::<Vec<_>>();
            (
                heads,
                page.next_cursor.map(|cursor| cursor.as_token().to_owned()),
            )
        };
        let status = self
            .store
            .local_watch_status()
            .map_err(|error| Status::internal(error.to_string()))?;
        self.require_unchanged(fence)?;
        Ok(Response::new(wire::IndexHeadScanPage {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            source_node_id: u64::from(status.source_id.node_id),
            source_epoch: status.source_id.source_epoch.to_vec(),
            placement_term: fence.term,
            placement_index: fence.index,
            heads_json: heads
                .iter()
                .map(super::encode_json)
                .collect::<Result<_, _>>()?,
            next_cursor,
        }))
    }
}

impl IndexHeadScanScope {
    fn matches(&self, candidate_tenant: u64, candidate_bucket: u64, exact_path: &str) -> bool {
        match self {
            Self::Artifacts {
                tenant_id,
                bucket_id,
                index_id,
            } => {
                candidate_tenant == *tenant_id
                    && candidate_bucket == *bucket_id
                    && is_index_recovery_path(exact_path, *index_id)
            }
            Self::Run {
                tenant_id,
                bucket_id,
                index_id,
                run_hash,
            } => {
                candidate_tenant == *tenant_id
                    && candidate_bucket == *bucket_id
                    && exact_path
                        .strip_prefix(&crate::index_runtime::publication::run_prefix(
                            *index_id, *run_hash,
                        ))
                        .is_some_and(|suffix| suffix == "root" || suffix.starts_with("packs/"))
                    && is_index_recovery_path(exact_path, *index_id)
            }
        }
    }

    fn prefix(&self) -> (u64, u64, String) {
        match self {
            Self::Artifacts {
                tenant_id,
                bucket_id,
                index_id,
            } => (
                *tenant_id,
                *bucket_id,
                format!("_anvil/indexes/v3/{index_id}/"),
            ),
            Self::Run {
                tenant_id,
                bucket_id,
                index_id,
                run_hash,
            } => (
                *tenant_id,
                *bucket_id,
                crate::index_runtime::publication::run_prefix(*index_id, *run_hash),
            ),
        }
    }
}

fn source_retained_coordinator(
    snapshot: &RetainedObjectSnapshot,
    placement: &crate::cluster_placement::ClusterPlacement,
) -> Option<NodeId> {
    object_coordinator(
        placement,
        snapshot.tenant_id,
        snapshot.bucket_id,
        &snapshot.exact_path,
    )
}

fn include_current_snapshot(
    scope: &IndexHeadScanScope,
    snapshot: &CurrentObjectSnapshot,
    local_node: NodeId,
    source_coordinator: Option<NodeId>,
) -> bool {
    let matches = match scope {
        IndexHeadScanScope::Artifacts { .. } => {
            scope.matches(snapshot.tenant_id, snapshot.bucket_id, &snapshot.exact_path)
        }
        IndexHeadScanScope::Run { .. } => {
            scope.matches(snapshot.tenant_id, snapshot.bucket_id, &snapshot.exact_path)
        }
    };
    matches && source_coordinator == Some(local_node)
}

fn source_current_coordinator(
    snapshot: &CurrentObjectSnapshot,
    placement: &crate::cluster_placement::ClusterPlacement,
) -> Option<NodeId> {
    object_coordinator(
        placement,
        snapshot.tenant_id,
        snapshot.bucket_id,
        &snapshot.exact_path,
    )
}

pub(super) fn contains_reserved_segment(path: &str) -> bool {
    path.split('/').any(|segment| segment == "_anvil")
}

pub(super) fn valid_source_prefix(prefix: &str) -> bool {
    let path = prefix.strip_suffix('/').unwrap_or(prefix);
    prefix.len() <= MAX_OBJECT_PATH_BYTES
        && !prefix.starts_with('/')
        && !prefix.contains('\0')
        && !prefix.chars().any(char::is_control)
        && (path.is_empty()
            || !path
                .split('/')
                .any(|segment| segment.is_empty() || matches!(segment, "." | ".." | "_anvil")))
}

fn retained_object_head(snapshot: RetainedObjectSnapshot) -> IndexCurrentHead {
    let version = snapshot.version;
    IndexCurrentHead {
        tenant_id: snapshot.tenant_id,
        bucket_id: snapshot.bucket_id,
        exact_path: snapshot.exact_path,
        head: Head {
            version: snapshot.current_head.version,
            deleted: snapshot.current_head.deleted,
            mutation_stamp: None,
        },
        versions: vec![version.clone()],
        version,
    }
}

fn current_object_head(snapshot: CurrentObjectSnapshot) -> IndexCurrentHead {
    IndexCurrentHead {
        tenant_id: snapshot.tenant_id,
        bucket_id: snapshot.bucket_id,
        exact_path: snapshot.exact_path,
        head: snapshot.head,
        versions: vec![snapshot.version.clone()],
        version: snapshot.version,
    }
}

fn decode_scan_scope(request: &wire::ScanIndexHeadsRequest) -> Result<IndexHeadScanScope, Status> {
    match request.scope.as_ref() {
        Some(wire::scan_index_heads_request::Scope::Artifacts(scope))
            if scope.tenant_id != 0 && scope.bucket_id != 0 && scope.index_id != 0 =>
        {
            Ok(IndexHeadScanScope::Artifacts {
                tenant_id: scope.tenant_id,
                bucket_id: scope.bucket_id,
                index_id: scope.index_id,
            })
        }
        Some(wire::scan_index_heads_request::Scope::Artifacts(_)) => Err(Status::invalid_argument(
            "index artifact scan stable IDs must be non-zero",
        )),
        Some(wire::scan_index_heads_request::Scope::Run(scope))
            if scope.tenant_id != 0
                && scope.bucket_id != 0
                && scope.index_id != 0
                && scope.run_blake3.len() == 32 =>
        {
            let run_hash: [u8; 32] =
                scope.run_blake3.as_slice().try_into().map_err(|_| {
                    Status::invalid_argument("index run hash must contain 32 bytes")
                })?;
            Ok(IndexHeadScanScope::Run {
                tenant_id: scope.tenant_id,
                bucket_id: scope.bucket_id,
                index_id: scope.index_id,
                run_hash,
            })
        }
        Some(wire::scan_index_heads_request::Scope::Run(_)) => Err(Status::invalid_argument(
            "index run scan stable IDs and hash must be non-empty",
        )),
        None => Err(Status::invalid_argument(
            "index head scan scope is required",
        )),
    }
}

pub(super) fn decode_request(
    request: &wire::PublishIndexArtifactRequest,
) -> Result<IndexArtifactPublish, Status> {
    let hash: [u8; 32] = request
        .blob_blake3
        .as_slice()
        .try_into()
        .map_err(|_| Status::invalid_argument("index artifact hash must contain 32 bytes"))?;
    Ok(IndexArtifactPublish {
        storage_tenant: request.storage_tenant.clone(),
        bucket: request.bucket.clone(),
        tenant_id: request.tenant_id,
        bucket_id: request.bucket_id,
        index_id: request.index_id,
        exact_path: request.exact_path.clone(),
        blob: BlobRef {
            hash,
            length: request.blob_length,
        },
        expected_version: request.expected_version.map(VersionId),
        command_id: request.command_id.clone(),
        definition_guard: decode_definition_guard(request)?,
        definition_intent: decode_definition_intent(request.definition_kind, request.index_id)?,
    })
}

fn decode_definition_guard(
    request: &wire::PublishIndexArtifactRequest,
) -> Result<Option<DefinitionVersionGuard>, Status> {
    let kind = match wire::RoutedDefinitionKind::try_from(request.guarded_definition_kind) {
        Ok(wire::RoutedDefinitionKind::Unspecified) => None,
        Ok(wire::RoutedDefinitionKind::Index) => Some(DefinitionKind::Index),
        Ok(wire::RoutedDefinitionKind::Accounting) => Some(DefinitionKind::Accounting),
        Err(_) => {
            return Err(Status::invalid_argument(
                "guarded artifact definition kind is invalid",
            ));
        }
    };
    match (
        kind,
        request.guarded_definition_path.is_empty(),
        request.guarded_definition_version,
    ) {
        (None, true, 0) => Ok(None),
        (Some(kind), false, version) if version != 0 => Ok(Some(DefinitionVersionGuard {
            kind,
            exact_path: request.guarded_definition_path.clone(),
            expected_version: VersionId(version),
        })),
        _ => Err(Status::invalid_argument(
            "guarded artifact definition fields must be present together",
        )),
    }
}

fn decode_delete(
    request: &wire::DeleteIndexArtifactRequest,
) -> Result<IndexArtifactDelete, Status> {
    Ok(IndexArtifactDelete {
        storage_tenant: request.storage_tenant.clone(),
        bucket: request.bucket.clone(),
        tenant_id: request.tenant_id,
        bucket_id: request.bucket_id,
        index_id: request.index_id,
        exact_path: request.exact_path.clone(),
        expected_version: VersionId(request.expected_version),
        command_id: request.command_id.clone(),
        definition_intent: decode_definition_intent(request.definition_kind, request.index_id)?,
    })
}

fn decode_definition_intent(
    kind: i32,
    definition_id: u64,
) -> Result<Option<DefinitionMutationIntent>, Status> {
    let kind = match wire::RoutedDefinitionKind::try_from(kind) {
        Ok(wire::RoutedDefinitionKind::Unspecified) => return Ok(None),
        Ok(wire::RoutedDefinitionKind::Index) => DefinitionKind::Index,
        Ok(wire::RoutedDefinitionKind::Accounting) => DefinitionKind::Accounting,
        Err(_) => {
            return Err(Status::invalid_argument(
                "index artifact definition kind is invalid",
            ));
        }
    };
    DefinitionMutationIntent::new(kind, definition_id)
        .map(Some)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

#[cfg(test)]
mod tests {
    use anvil_store::{RetainedHeadState, Version};

    use super::*;

    #[test]
    fn scan_scopes_cannot_become_arbitrary_prefix_scans() {
        let digest = "a".repeat(64);
        let generation = format!("_anvil/indexes/v3/9/manifests/{digest}");
        let current = "_anvil/indexes/v3/9/current";

        let scoped = IndexHeadScanScope::Artifacts {
            tenant_id: 4,
            bucket_id: 5,
            index_id: 9,
        };
        assert!(scoped.matches(4, 5, &generation));
        assert!(scoped.matches(4, 5, current));
        assert!(!scoped.matches(4, 5, "_anvil/indexes/v3/definitions/search"));
        assert!(!scoped.matches(4, 5, "_anvil/indexes/definitions/search"));
        assert!(!scoped.matches(4, 5, "ordinary/path"));
        let run_hash = [3; 32];
        let run_root = crate::index_runtime::publication::run_root_path(9, run_hash);
        let adjacent = format!("{run_root}extra");
        let run = IndexHeadScanScope::Run {
            tenant_id: 4,
            bucket_id: 5,
            index_id: 9,
            run_hash,
        };
        assert!(run.matches(4, 5, &run_root));
        assert!(!run.matches(4, 5, &adjacent));
        assert_eq!(
            run.prefix().2,
            crate::index_runtime::publication::run_prefix(9, run_hash)
        );
        assert!(
            !IndexHeadScanScope::Artifacts {
                tenant_id: 4,
                bucket_id: 6,
                index_id: 9,
            }
            .matches(4, 5, &generation)
        );
    }

    #[test]
    fn retained_scan_record_carries_one_descriptor_and_current_head_state() {
        let source = RetainedObjectSnapshot {
            tenant_id: 4,
            bucket_id: 5,
            exact_path: "_anvil/indexes/v3/9/current".into(),
            version: Version {
                id: VersionId(6),
                blob: Some(BlobRef {
                    hash: [1; 32],
                    length: 10,
                }),
                content_type: None,
                deleted: false,
                committed_at_unix_millis: 1,
            },
            current_head: RetainedHeadState {
                version: VersionId(7),
                deleted: false,
            },
        };

        let selected = retained_object_head(source);
        assert_eq!(selected.head.version, VersionId(7));
        assert_eq!(selected.version.id, VersionId(6));
        assert_eq!(
            selected.versions,
            vec![Version {
                id: VersionId(6),
                blob: Some(BlobRef {
                    hash: [1; 32],
                    length: 10,
                }),
                content_type: None,
                deleted: false,
                committed_at_unix_millis: 1,
            }]
        );
    }

    #[test]
    fn source_prefix_cannot_select_a_reserved_namespace() {
        assert!(valid_source_prefix(""));
        assert!(valid_source_prefix("projects/"));
        assert!(!valid_source_prefix("_anvil/"));
        assert!(!valid_source_prefix("projects/_anvil/"));
        assert!(!valid_source_prefix("projects//nested"));
        assert!(!valid_source_prefix("../projects"));
        assert!(!valid_source_prefix("/projects"));
    }
}
