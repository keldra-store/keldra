use keldra_store::{BlobRef, DefinitionKind, DefinitionMutationIntent, VersionId};
use tonic::{Request, Response, Status};

use keldra_atomic_program::MAX_OBJECT_PATH_BYTES;
use keldra_consensus::NodeId;

use super::{CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, wire};
use crate::index_runtime::publication::{
    DefinitionVersionGuard, DerivedArtifactAdmission, IndexArtifactDelete,
    IndexArtifactPublication, IndexArtifactPublicationOutcome, IndexArtifactPublish,
    MAX_INDEX_ARTIFACT_BATCH_BYTES, MAX_INDEX_ARTIFACT_BATCH_ITEMS,
};

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
}

pub(super) fn contains_reserved_segment(path: &str) -> bool {
    path.split('/').any(|segment| segment == "_keldra")
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
                .any(|segment| segment.is_empty() || matches!(segment, "." | ".." | "_keldra")))
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
        admission: if request.publication_progress {
            DerivedArtifactAdmission::PublicationProgress
        } else {
            DerivedArtifactAdmission::Bounded
        },
    })
}

pub(super) fn decode_bounded_publications(
    encoded: &[wire::PublishIndexArtifactRequest],
) -> Result<Vec<IndexArtifactPublish>, Status> {
    if encoded.is_empty() || encoded.len() > MAX_INDEX_ARTIFACT_BATCH_ITEMS {
        return Err(Status::resource_exhausted(format!(
            "index artifact batch must contain 1..={MAX_INDEX_ARTIFACT_BATCH_ITEMS} items"
        )));
    }
    let mut logical_bytes = 0_u64;
    let mut publications = Vec::with_capacity(encoded.len());
    for publication in encoded {
        if publication.peer.is_some() {
            return Err(Status::invalid_argument(
                "nested index artifact publication must not carry peer context",
            ));
        }
        logical_bytes = logical_bytes
            .checked_add(publication.blob_length)
            .ok_or_else(|| {
                Status::resource_exhausted("index artifact batch byte count overflow")
            })?;
        if logical_bytes > MAX_INDEX_ARTIFACT_BATCH_BYTES {
            return Err(Status::resource_exhausted(format!(
                "index artifact batch exceeds {MAX_INDEX_ARTIFACT_BATCH_BYTES} logical bytes"
            )));
        }
        publications.push(decode_request(publication)?);
    }
    Ok(publications)
}

pub(super) fn encode_publication_outcomes(
    outcomes: Vec<IndexArtifactPublicationOutcome>,
) -> Vec<wire::IndexedIndexArtifactPublicationOutcome> {
    outcomes
        .into_iter()
        .enumerate()
        .map(
            |(index, outcome)| wire::IndexedIndexArtifactPublicationOutcome {
                schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                request_index: index as u32,
                result: Some(match outcome {
                    Ok(outcome) => {
                        wire::indexed_index_artifact_publication_outcome::Result::Published(
                            wire::IndexArtifactPublished {
                                schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                                version: outcome.version.0,
                                replayed: outcome.replayed,
                            },
                        )
                    }
                    Err(error) => wire::indexed_index_artifact_publication_outcome::Result::Failed(
                        wire::IndexArtifactPublicationFailed {
                            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                            code: error.code() as i32,
                            message: error.message().to_owned(),
                        },
                    ),
                }),
            },
        )
        .collect()
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
    use super::*;
    use crate::index_runtime::publication::IndexArtifactOutcome;

    #[test]
    fn private_artifact_admission_round_trips_over_the_peer_protocol() {
        for admission in [
            DerivedArtifactAdmission::Bounded,
            DerivedArtifactAdmission::PublicationProgress,
        ] {
            let request = IndexArtifactPublish {
                storage_tenant: "tenant".into(),
                bucket: "bucket".into(),
                tenant_id: 4,
                bucket_id: 5,
                index_id: 9,
                exact_path: keldra_index::v1::projection_pack_path(
                    keldra_index::v1::ProjectionPartitionIdentity::new(
                        [1; 32], 2, [3; 32], 4, 5, 6,
                    )
                    .unwrap(),
                    [7; 32],
                ),
                blob: BlobRef {
                    hash: [7; 32],
                    length: 11,
                },
                expected_version: None,
                command_id: "peer-admission-round-trip".into(),
                definition_guard: None,
                definition_intent: None,
                admission,
            };
            let encoded = super::super::transport::wire_index_artifact_publish(&request, None);
            assert_eq!(decode_request(&encoded).unwrap().admission, admission);
        }
    }

    #[test]
    fn plural_outcomes_preserve_request_order_and_individual_failures() {
        let encoded = encode_publication_outcomes(vec![
            Ok(IndexArtifactOutcome {
                version: VersionId(17),
                replayed: true,
            }),
            Err(Status::aborted("pointer compare-and-swap lost")),
            Ok(IndexArtifactOutcome {
                version: VersionId(19),
                replayed: false,
            }),
        ]);

        let decoded = super::super::transport::decode_indexed_artifact_outcomes(encoded, 3)
            .expect("valid indexed outcomes");
        assert_eq!(decoded[0].as_ref().unwrap().version, VersionId(17));
        assert!(decoded[0].as_ref().unwrap().replayed);
        let failed = decoded[1].as_ref().unwrap_err();
        assert_eq!(failed.code(), tonic::Code::Aborted);
        assert_eq!(failed.message(), "pointer compare-and-swap lost");
        assert_eq!(decoded[2].as_ref().unwrap().version, VersionId(19));
    }

    #[test]
    fn plural_outcome_decoder_rejects_duplicate_indices() {
        let mut encoded = encode_publication_outcomes(vec![
            Ok(IndexArtifactOutcome {
                version: VersionId(17),
                replayed: false,
            }),
            Ok(IndexArtifactOutcome {
                version: VersionId(18),
                replayed: false,
            }),
        ]);
        encoded[1].request_index = 0;

        let error =
            super::super::transport::decode_indexed_artifact_outcomes(encoded, 2).unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn source_prefix_cannot_select_a_reserved_namespace() {
        assert!(valid_source_prefix(""));
        assert!(valid_source_prefix("projects/"));
        assert!(!valid_source_prefix("_keldra/"));
        assert!(!valid_source_prefix("projects/_keldra/"));
        assert!(!valid_source_prefix("projects//nested"));
        assert!(!valid_source_prefix("../projects"));
        assert!(!valid_source_prefix("/projects"));
    }
}
