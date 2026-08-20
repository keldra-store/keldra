//! Bounded mandatory-mTLS publication of one immutable artifact replica group.

use tonic::{Request, Response, Status};

use super::index_artifacts::decode_request;
use super::{CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, wire};
use crate::index_runtime::publication::{
    IndexArtifactPublication, MAX_INDEX_ARTIFACT_BATCH_BYTES, MAX_INDEX_ARTIFACT_BATCH_ITEMS,
};

impl ClusterPeerService {
    pub(super) async fn publish_index_artifacts_call(
        &self,
        request: Request<wire::PublishIndexArtifactsRequest>,
    ) -> Result<Response<wire::IndexArtifactsPublished>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let encoded = &request.get_ref().publications;
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
        let fence = admitted.placement.fence();
        let outcomes = tokio::time::timeout(
            admitted.timeout,
            self.index_artifacts.publish_many(
                admitted.authenticated.node_id,
                admitted.placement,
                publications,
            ),
        )
        .await
        .map_err(|_| {
            Status::deadline_exceeded("grouped index artifact publication deadline exceeded")
        })??;
        self.require_unchanged(fence)?;
        Ok(Response::new(wire::IndexArtifactsPublished {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            outcomes: outcomes
                .into_iter()
                .map(|outcome| wire::IndexArtifactPublished {
                    schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                    version: outcome.version.0,
                    replayed: outcome.replayed,
                })
                .collect(),
        }))
    }
}
