use anvil_store::{
    BlobRef, Head, ObjectPathSnapshot, ObjectRecordCursor, ObjectRecordExport, Version, VersionId,
};
use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status};

use anvil_atomic_program::MAX_OBJECT_PATH_BYTES;
use anvil_consensus::NodeId;

use super::storage::{bounded_blocking, object_coordinator};
use super::{CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, wire};
use crate::index_runtime::publication::{
    IndexArtifactDelete, IndexArtifactPublication, IndexArtifactPublish, index_definition_name,
    is_index_recovery_path,
};
use crate::index_service::path_matches_prefix;

const INDEX_HEAD_SCAN_MAX_RECORDS: u32 = 128;
const INDEX_HEAD_SCAN_MAX_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum IndexHeadScanScope {
    Definitions,
    AccountingDefinitions,
    Generation {
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    },
    SourceObjects {
        tenant_id: u64,
        bucket_id: u64,
        path_prefix: String,
    },
    AccountingSourceObjects {
        tenant_id: u64,
        bucket_id: u64,
        path_prefix: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct IndexCurrentHead {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub exact_path: String,
    pub head: Head,
    pub version: Version,
    /// Complete retained descriptors for internal retention consumers. Public
    /// index queries still open exactly the current generation.
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
        let cursor = request
            .get_ref()
            .cursor
            .as_ref()
            .map(ObjectRecordCursor::from_token)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let fence = admitted.placement.fence();
        let store = self.store.clone();
        let page = bounded_blocking(admitted.timeout, move || {
            store
                .export_object_records(
                    cursor.as_ref(),
                    INDEX_HEAD_SCAN_MAX_RECORDS,
                    INDEX_HEAD_SCAN_MAX_BYTES,
                )
                .map_err(|error| Status::internal(error.to_string()))
        })
        .await?;
        let status = self
            .store
            .local_watch_status()
            .map_err(|error| Status::internal(error.to_string()))?;
        let heads = page
            .records
            .into_iter()
            .filter_map(|record| match record {
                ObjectRecordExport::ExactPath(snapshot)
                    if include_snapshot(
                        &scope,
                        &snapshot,
                        self.local_node,
                        source_object_coordinator(&scope, &snapshot, &admitted.placement),
                    ) =>
                {
                    Some(current_head(snapshot))
                }
                ObjectRecordExport::ExactPath(_) | ObjectRecordExport::Receipt(_) => None,
            })
            .collect::<Result<Vec<_>, _>>()?;
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
            next_cursor: page.next_cursor.map(|cursor| cursor.as_token().to_owned()),
        }))
    }
}

impl IndexHeadScanScope {
    fn matches(&self, snapshot: &ObjectPathSnapshot) -> bool {
        match self {
            Self::Definitions => index_definition_name(&snapshot.exact_path).is_some(),
            Self::AccountingDefinitions => {
                crate::accounting::definition_id_from_path(&snapshot.exact_path).is_some()
            }
            Self::Generation {
                tenant_id,
                bucket_id,
                index_id,
            } => {
                snapshot.tenant_id == *tenant_id
                    && snapshot.bucket_id == *bucket_id
                    && is_index_recovery_path(&snapshot.exact_path, *index_id)
            }
            Self::SourceObjects {
                tenant_id,
                bucket_id,
                path_prefix,
            } => {
                snapshot.tenant_id == *tenant_id
                    && snapshot.bucket_id == *bucket_id
                    && !snapshot.head.deleted
                    && path_matches_prefix(&snapshot.exact_path, path_prefix)
                    && !contains_reserved_segment(&snapshot.exact_path)
            }
            Self::AccountingSourceObjects {
                tenant_id,
                bucket_id,
                path_prefix,
            } => {
                snapshot.tenant_id == *tenant_id
                    && snapshot.bucket_id == *bucket_id
                    && !snapshot.head.deleted
                    && crate::accounting::includes_path(path_prefix, &snapshot.exact_path)
            }
        }
    }

    fn is_source_objects(&self) -> bool {
        matches!(
            self,
            Self::SourceObjects { .. } | Self::AccountingSourceObjects { .. }
        )
    }
}

fn include_snapshot(
    scope: &IndexHeadScanScope,
    snapshot: &ObjectPathSnapshot,
    local_node: NodeId,
    source_coordinator: Option<NodeId>,
) -> bool {
    scope.matches(snapshot)
        && (!scope.is_source_objects() || source_coordinator == Some(local_node))
}

fn source_object_coordinator(
    scope: &IndexHeadScanScope,
    snapshot: &ObjectPathSnapshot,
    placement: &crate::cluster_placement::ClusterPlacement,
) -> Option<NodeId> {
    if !scope.is_source_objects() {
        return None;
    }
    object_coordinator(
        placement,
        snapshot.tenant_id,
        snapshot.bucket_id,
        &snapshot.exact_path,
    )
}

fn contains_reserved_segment(path: &str) -> bool {
    path.split('/').any(|segment| segment == "_anvil")
}

fn valid_source_prefix(prefix: &str) -> bool {
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

fn current_head(snapshot: ObjectPathSnapshot) -> Result<IndexCurrentHead, Status> {
    let version = snapshot
        .versions
        .iter()
        .find(|version| version.id == snapshot.head.version)
        .cloned()
        .ok_or_else(|| Status::data_loss("index head snapshot omits its current version"))?;
    Ok(IndexCurrentHead {
        tenant_id: snapshot.tenant_id,
        bucket_id: snapshot.bucket_id,
        exact_path: snapshot.exact_path,
        head: snapshot.head,
        version,
        versions: snapshot.versions,
    })
}

fn decode_scan_scope(request: &wire::ScanIndexHeadsRequest) -> Result<IndexHeadScanScope, Status> {
    match request.scope.as_ref() {
        Some(wire::scan_index_heads_request::Scope::Definitions(_)) => {
            Ok(IndexHeadScanScope::Definitions)
        }
        Some(wire::scan_index_heads_request::Scope::AccountingDefinitions(_)) => {
            Ok(IndexHeadScanScope::AccountingDefinitions)
        }
        Some(wire::scan_index_heads_request::Scope::Generation(scope))
            if scope.tenant_id != 0 && scope.bucket_id != 0 && scope.index_id != 0 =>
        {
            Ok(IndexHeadScanScope::Generation {
                tenant_id: scope.tenant_id,
                bucket_id: scope.bucket_id,
                index_id: scope.index_id,
            })
        }
        Some(wire::scan_index_heads_request::Scope::Generation(_)) => Err(
            Status::invalid_argument("index generation scan stable IDs must be non-zero"),
        ),
        Some(wire::scan_index_heads_request::Scope::SourceObjects(scope))
            if scope.tenant_id != 0
                && scope.bucket_id != 0
                && valid_source_prefix(&scope.path_prefix) =>
        {
            Ok(IndexHeadScanScope::SourceObjects {
                tenant_id: scope.tenant_id,
                bucket_id: scope.bucket_id,
                path_prefix: scope.path_prefix.clone(),
            })
        }
        Some(wire::scan_index_heads_request::Scope::SourceObjects(_)) => {
            Err(Status::invalid_argument(
                "index source-object stable IDs and ordinary path prefix are invalid",
            ))
        }
        Some(wire::scan_index_heads_request::Scope::AccountingSourceObjects(scope))
            if scope.tenant_id != 0
                && scope.bucket_id != 0
                && valid_source_prefix(&scope.path_prefix) =>
        {
            Ok(IndexHeadScanScope::AccountingSourceObjects {
                tenant_id: scope.tenant_id,
                bucket_id: scope.bucket_id,
                path_prefix: scope.path_prefix.clone(),
            })
        }
        Some(wire::scan_index_heads_request::Scope::AccountingSourceObjects(_)) => {
            Err(Status::invalid_argument(
                "accounting source-object stable IDs and ordinary path prefix are invalid",
            ))
        }
        None => Err(Status::invalid_argument(
            "index head scan scope is required",
        )),
    }
}

fn decode_request(
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
    })
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
    })
}

#[cfg(test)]
mod tests {
    use anvil_store::{Head, MutationStamp, Version};

    use super::*;

    fn snapshot(tenant_id: u64, bucket_id: u64, path: &str) -> ObjectPathSnapshot {
        ObjectPathSnapshot {
            tenant_id,
            bucket_id,
            exact_path: path.into(),
            head: Head {
                version: VersionId(7),
                deleted: false,
                mutation_stamp: None::<MutationStamp>,
            },
            versions: vec![Version {
                id: VersionId(7),
                blob: Some(BlobRef {
                    hash: [2; 32],
                    length: 12,
                }),
                content_type: None,
                deleted: false,
                committed_at_unix_millis: 1,
            }],
        }
    }

    #[test]
    fn scan_scopes_cannot_become_arbitrary_prefix_scans() {
        let definition = snapshot(4, 5, "_anvil/indexes/definitions/search");
        let generation = snapshot(4, 5, "_anvil/indexes/9/generations/2/manifest");
        let current = snapshot(4, 5, "_anvil/indexes/9/current");
        let unrelated = snapshot(4, 5, "ordinary/path");

        assert!(IndexHeadScanScope::Definitions.matches(&definition));
        assert!(!IndexHeadScanScope::Definitions.matches(&generation));
        let scoped = IndexHeadScanScope::Generation {
            tenant_id: 4,
            bucket_id: 5,
            index_id: 9,
        };
        assert!(scoped.matches(&generation));
        assert!(scoped.matches(&current));
        assert!(!scoped.matches(&definition));
        assert!(!scoped.matches(&unrelated));
        assert!(
            !IndexHeadScanScope::Generation {
                tenant_id: 4,
                bucket_id: 6,
                index_id: 9,
            }
            .matches(&generation)
        );
    }

    #[test]
    fn scan_returns_only_the_current_descriptor() {
        let mut source = snapshot(4, 5, "_anvil/indexes/9/current");
        source.versions.insert(
            0,
            Version {
                id: VersionId(6),
                blob: Some(BlobRef {
                    hash: [1; 32],
                    length: 10,
                }),
                content_type: None,
                deleted: false,
                committed_at_unix_millis: 1,
            },
        );

        let selected = current_head(source).unwrap();
        assert_eq!(selected.head.version, VersionId(7));
        assert_eq!(selected.version.id, VersionId(7));
    }

    #[test]
    fn source_object_scope_is_live_ordinary_prefix_only() {
        let scope = IndexHeadScanScope::SourceObjects {
            tenant_id: 4,
            bucket_id: 5,
            path_prefix: "projects/".into(),
        };
        let ordinary = snapshot(4, 5, "projects/one.json");
        let nested_reserved = snapshot(4, 5, "projects/_anvil/meta.json");
        let root_reserved = snapshot(4, 5, "_anvil/projects/one.json");
        let outside = snapshot(4, 5, "other/one.json");
        let mut deleted = ordinary.clone();
        deleted.head.deleted = true;

        assert!(scope.matches(&ordinary));
        assert!(!scope.matches(&nested_reserved));
        assert!(!scope.matches(&root_reserved));
        assert!(!scope.matches(&outside));
        assert!(!scope.matches(&deleted));
        assert!(include_snapshot(
            &scope,
            &ordinary,
            NodeId(2),
            Some(NodeId(2)),
        ));
        assert!(!include_snapshot(
            &scope,
            &ordinary,
            NodeId(2),
            Some(NodeId(3)),
        ));
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
