//! Bounded mandatory-mTLS publication of one immutable artifact replica group.

use tonic::{Request, Response, Status};

use super::index_artifacts::{decode_bounded_publications, encode_publication_outcomes};
use super::{CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, wire};
use crate::index_runtime::publication::IndexArtifactPublication;

impl ClusterPeerService {
    pub(super) async fn publish_index_artifacts_call(
        &self,
        request: Request<wire::PublishIndexArtifactsRequest>,
    ) -> Result<Response<wire::IndexArtifactsPublished>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let publications = decode_bounded_publications(&request.get_ref().publications)?;
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
            outcomes: encode_publication_outcomes(outcomes),
        }))
    }
}
