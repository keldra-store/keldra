//! One bounded, operation-specific peer RPC for preparing payload evidence.
//!
//! The request goes directly to the upload source named by the public ready
//! capability. Mandatory peer mTLS and acknowledged cluster membership are
//! checked on every call. The operation does not expose storage internals or
//! persist a source-location record.

use std::collections::BTreeSet;
use std::sync::Arc;

use keldra_consensus::{
    AuthenticatedPeer, ClusterId, CommittedPeerPinProvider, DecisionRaft, NodeId, PeerRpcKind,
    PeerSpkiSha256, authorize_peer_rpc,
};
use keldra_store::{BlobRef, Durability, ErasureProfile, Store};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use super::{
    NodePayloadEvidence, PAYLOAD_EVIDENCE_FORMAT, PayloadDistribution, PayloadDistributionError,
    PreparedPayloadEvidence,
};
use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::DataPeerTransport;

pub(crate) mod wire {
    tonic::include_proto!("keldra.payload_peer.v1");
}

const PAYLOAD_PEER_SCHEMA_VERSION: u32 = 1;
const MAX_PAYLOAD_PEER_MESSAGE_BYTES: usize = 64 * 1024;
const MAX_PAYLOAD_EVIDENCE_ARTIFACTS: usize = 257;

#[derive(Clone)]
pub(crate) struct PayloadPeerService {
    local_node: NodeId,
    distribution: PayloadDistribution,
    decisions: DecisionRaft,
    pins: Arc<dyn CommittedPeerPinProvider>,
    max_blob_bytes: u64,
}

pub(crate) type PayloadPeerServer =
    wire::payload_peer_server::PayloadPeerServer<PayloadPeerService>;

impl PayloadPeerService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        local_node: NodeId,
        store: Store,
        peers: DataPeerTransport,
        profile: ErasureProfile,
        decisions: DecisionRaft,
        pins: Arc<dyn CommittedPeerPinProvider>,
        max_blob_bytes: u64,
    ) -> Self {
        Self {
            local_node,
            distribution: PayloadDistribution::new(local_node, store, Arc::new(peers), profile),
            decisions,
            pins,
            max_blob_bytes,
        }
    }

    pub(crate) fn into_server(self) -> PayloadPeerServer {
        PayloadPeerServer::new(self)
            .max_decoding_message_size(MAX_PAYLOAD_PEER_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_PAYLOAD_PEER_MESSAGE_BYTES)
    }

    fn authorize<T>(
        &self,
        request: &Request<T>,
        context: Option<&wire::PeerContext>,
    ) -> Result<AuthenticatedPeer, Status> {
        let pin = request
            .extensions()
            .get::<PeerSpkiSha256>()
            .copied()
            .ok_or_else(|| Status::unauthenticated("peer mTLS identity is missing"))?;
        let context =
            context.ok_or_else(|| Status::invalid_argument("peer context is required"))?;
        if context.schema_version != PAYLOAD_PEER_SCHEMA_VERSION {
            return Err(Status::failed_precondition(format!(
                "unsupported payload-peer schema {}",
                context.schema_version
            )));
        }
        let cluster_id = parse_cluster_id(&context.cluster_id)?;
        if context.source_node_id == 0 {
            return Err(Status::invalid_argument("source node id must not be zero"));
        }
        authorize_peer_rpc(
            self.pins.as_ref(),
            cluster_id,
            NodeId(context.source_node_id),
            PeerRpcKind::DataPlane,
            pin,
        )
        .map_err(|_| Status::permission_denied("peer is not authorized for payload preparation"))
    }

    fn placement(&self) -> Result<ClusterPlacement, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}

#[tonic::async_trait]
impl wire::payload_peer_server::PayloadPeer for PayloadPeerService {
    async fn prepare_payload(
        &self,
        request: Request<wire::PreparePayloadRequest>,
    ) -> Result<Response<wire::PreparedPayloadEvidence>, Status> {
        let authenticated = self.authorize(&request, request.get_ref().peer.as_ref())?;
        let placement = self.placement()?;
        if placement.cluster_id() != authenticated.cluster_id {
            return Err(Status::permission_denied(
                "peer cluster does not match applied membership",
            ));
        }
        if placement.upload_source_address(self.local_node).is_none() {
            return Err(Status::unavailable(
                "upload source is not an acknowledged ACTIVE or JOINING member",
            ));
        }
        let reference = parse_blob(request.get_ref().blob.as_ref(), self.max_blob_bytes)?;
        let durability = parse_durability(request.get_ref().durability)?;
        let evidence = self
            .distribution
            .prepare_on_upload_source(&placement, &reference, durability)
            .await
            .map_err(distribution_status)?;
        Ok(Response::new(wire_evidence(&evidence)))
    }
}

/// Client for asking the exact upload source to prepare bounded evidence.
#[derive(Clone)]
pub(crate) struct PayloadPeerTransport {
    data: DataPeerTransport,
}

impl PayloadPeerTransport {
    pub(crate) fn new(data: DataPeerTransport) -> Self {
        Self { data }
    }

    pub(crate) async fn prepare_payload(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        durability: Durability,
    ) -> Result<PreparedPayloadEvidence, Status> {
        let (cluster_id, source_node_id) = self.data.peer_identity();
        let request = wire::PreparePayloadRequest {
            peer: Some(wire::PeerContext {
                schema_version: PAYLOAD_PEER_SCHEMA_VERSION,
                cluster_id: cluster_id.into_bytes().to_vec(),
                source_node_id: source_node_id.0,
            }),
            blob: Some(wire_blob(reference)),
            durability: wire_durability(durability) as i32,
        };
        let response = self
            .client(target, address)?
            .prepare_payload(request)
            .await?
            .into_inner();
        parse_evidence(response, target, reference)
    }

    fn client(
        &self,
        target: NodeId,
        address: &str,
    ) -> Result<wire::payload_peer_client::PayloadPeerClient<Channel>, Status> {
        Ok(
            wire::payload_peer_client::PayloadPeerClient::new(self.data.channel(target, address)?)
                .max_decoding_message_size(MAX_PAYLOAD_PEER_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_PAYLOAD_PEER_MESSAGE_BYTES),
        )
    }
}

fn parse_cluster_id(encoded: &[u8]) -> Result<ClusterId, Status> {
    let bytes = encoded
        .try_into()
        .map_err(|_| Status::invalid_argument("cluster id must contain exactly 16 bytes"))?;
    if bytes == [0; 16] {
        return Err(Status::invalid_argument("cluster id must not be all zero"));
    }
    Ok(ClusterId(bytes))
}

fn parse_blob(value: Option<&wire::BlobIdentity>, max_blob_bytes: u64) -> Result<BlobRef, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("blob identity is required"))?;
    let hash =
        value.hash.as_slice().try_into().map_err(|_| {
            Status::invalid_argument("BLAKE3 identity must contain exactly 32 bytes")
        })?;
    if value.length > max_blob_bytes {
        return Err(Status::resource_exhausted(
            "content identity exceeds the configured maximum blob size",
        ));
    }
    Ok(BlobRef {
        hash,
        length: value.length,
    })
}

fn parse_durability(encoded: i32) -> Result<Durability, Status> {
    match wire::Durability::try_from(encoded) {
        Ok(wire::Durability::Local) => Ok(Durability::Local),
        Ok(wire::Durability::Replicated) => Ok(Durability::Replicated),
        Err(_) => Err(Status::invalid_argument("durability is not supported")),
    }
}

fn wire_durability(durability: Durability) -> wire::Durability {
    match durability {
        Durability::Local => wire::Durability::Local,
        Durability::Replicated => wire::Durability::Replicated,
    }
}

fn wire_blob(reference: &BlobRef) -> wire::BlobIdentity {
    wire::BlobIdentity {
        hash: reference.hash.to_vec(),
        length: reference.length,
    }
}

fn wire_evidence(evidence: &PreparedPayloadEvidence) -> wire::PreparedPayloadEvidence {
    wire::PreparedPayloadEvidence {
        schema_version: u32::from(evidence.format()),
        placement_fence_term: evidence.placement_fence().term,
        placement_fence_index: evidence.placement_fence().index,
        blob: Some(wire_blob(evidence.blob())),
        upload_source_node_id: evidence.upload_source().0,
        artifacts: evidence
            .artifacts()
            .iter()
            .map(|entry| wire::ArtifactEvidence {
                node_id: entry.node_id().0,
                complete_copy: entry.complete_copy(),
                shard_ordinal: entry.shard_ordinal().map(u32::from),
            })
            .collect(),
    }
}

fn parse_evidence(
    value: wire::PreparedPayloadEvidence,
    expected_source: NodeId,
    expected_blob: &BlobRef,
) -> Result<PreparedPayloadEvidence, Status> {
    if value.schema_version != u32::from(PAYLOAD_EVIDENCE_FORMAT) {
        return Err(Status::failed_precondition(format!(
            "peer returned unsupported payload evidence schema {}",
            value.schema_version
        )));
    }
    if value.upload_source_node_id != expected_source.0 {
        return Err(Status::data_loss(
            "payload evidence does not name the contacted upload source",
        ));
    }
    let blob = parse_blob(value.blob.as_ref(), u64::MAX)?;
    if blob != *expected_blob {
        return Err(Status::data_loss(
            "payload evidence does not match the requested content",
        ));
    }
    if value.artifacts.len() > MAX_PAYLOAD_EVIDENCE_ARTIFACTS {
        return Err(Status::resource_exhausted(
            "payload evidence contains too many artifacts",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut artifacts = Vec::with_capacity(value.artifacts.len());
    for artifact in value.artifacts {
        let node = NodeId(artifact.node_id);
        if node.0 == 0 || !seen.insert(node) {
            return Err(Status::data_loss(
                "payload evidence contains an invalid or duplicate node",
            ));
        }
        let ordinal = artifact
            .shard_ordinal
            .map(|ordinal| {
                u16::try_from(ordinal)
                    .map_err(|_| Status::data_loss("payload shard ordinal exceeds u16"))
            })
            .transpose()?;
        if !artifact.complete_copy && ordinal.is_none() {
            return Err(Status::data_loss(
                "payload evidence contains an empty artifact claim",
            ));
        }
        artifacts.push(NodePayloadEvidence::new(
            node,
            artifact.complete_copy,
            ordinal,
        ));
    }
    if !artifacts
        .iter()
        .any(|artifact| artifact.node_id() == expected_source && artifact.complete_copy())
    {
        return Err(Status::data_loss(
            "payload evidence does not prove a complete upload source",
        ));
    }
    Ok(PreparedPayloadEvidence::from_parts(
        u16::try_from(value.schema_version).expect("validated evidence schema fits u16"),
        keldra_store::PlacementLogId {
            term: value.placement_fence_term,
            index: value.placement_fence_index,
        },
        blob,
        expected_source,
        artifacts,
    ))
}

fn distribution_status(error: PayloadDistributionError) -> Status {
    match error {
        PayloadDistributionError::UploadSourceMissing => {
            Status::failed_precondition(error.to_string())
        }
        PayloadDistributionError::UploadSourceCorrupt => Status::data_loss(error.to_string()),
        PayloadDistributionError::InvalidEvidence
        | PayloadDistributionError::Readiness(_)
        | PayloadDistributionError::SourceLocationUnavailable => {
            Status::failed_precondition(error.to_string())
        }
        PayloadDistributionError::OwnerAddressMissing { .. }
        | PayloadDistributionError::OwnerArtifactMissing { .. }
        | PayloadDistributionError::OwnerArtifactThreshold { .. }
        | PayloadDistributionError::Peer { .. }
        | PayloadDistributionError::Encoding(_)
        | PayloadDistributionError::CompleteSource(_) => Status::unavailable(error.to_string()),
        PayloadDistributionError::Store(_) | PayloadDistributionError::Erasure(_) => {
            Status::internal(error.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob() -> BlobRef {
        BlobRef {
            hash: [7; 32],
            length: 81_920,
        }
    }

    #[test]
    fn evidence_wire_round_trip_is_exact_and_bounded() {
        let reference = blob();
        let evidence = PreparedPayloadEvidence::from_parts(
            PAYLOAD_EVIDENCE_FORMAT,
            keldra_store::PlacementLogId { term: 3, index: 8 },
            reference.clone(),
            NodeId(2),
            vec![
                NodePayloadEvidence::new(NodeId(2), true, Some(0)),
                NodePayloadEvidence::new(NodeId(3), false, Some(1)),
            ],
        );

        let decoded = parse_evidence(wire_evidence(&evidence), NodeId(2), &reference).unwrap();
        assert_eq!(decoded, evidence);
    }

    #[test]
    fn evidence_rejects_wrong_source_duplicate_nodes_and_empty_claims() {
        let reference = blob();
        let mut encoded = wire::PreparedPayloadEvidence {
            schema_version: u32::from(PAYLOAD_EVIDENCE_FORMAT),
            placement_fence_term: 3,
            placement_fence_index: 8,
            blob: Some(wire_blob(&reference)),
            upload_source_node_id: 2,
            artifacts: vec![wire::ArtifactEvidence {
                node_id: 2,
                complete_copy: true,
                shard_ordinal: None,
            }],
        };
        assert_eq!(
            parse_evidence(encoded.clone(), NodeId(3), &reference)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );

        encoded.artifacts.push(encoded.artifacts[0].clone());
        assert_eq!(
            parse_evidence(encoded.clone(), NodeId(2), &reference)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );

        encoded.artifacts = vec![wire::ArtifactEvidence {
            node_id: 2,
            complete_copy: false,
            shard_ordinal: None,
        }];
        assert_eq!(
            parse_evidence(encoded, NodeId(2), &reference)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
    }
}
