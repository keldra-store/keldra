use std::time::Duration;

use anvil_consensus::{
    AuthenticatedPeer, ClusterId, NodeId, PeerRpcKind, PeerSpkiSha256, authorize_peer_rpc,
};
use anvil_store::PlacementLogId;
use tonic::{Request, Status};

use super::{CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, MAX_CLUSTER_OPERATION_TIME, wire};
use crate::cluster_placement::ClusterPlacement;

#[derive(Clone, Debug)]
pub(super) struct AdmittedPeer {
    pub(super) authenticated: AuthenticatedPeer,
    pub(super) placement: ClusterPlacement,
    pub(super) timeout: Duration,
}

impl ClusterPeerService {
    pub(super) fn admit<T>(
        &self,
        request: &Request<T>,
        context: Option<&wire::PeerContext>,
        expected_hop: u32,
    ) -> Result<AdmittedPeer, Status> {
        let pin = request
            .extensions()
            .get::<PeerSpkiSha256>()
            .copied()
            .ok_or_else(|| Status::unauthenticated("peer mTLS identity is missing"))?;
        self.admit_pin(
            pin,
            context.ok_or_else(|| Status::invalid_argument("peer context is required"))?,
            expected_hop,
        )
    }

    pub(super) fn admit_pin(
        &self,
        pin: PeerSpkiSha256,
        context: &wire::PeerContext,
        expected_hop: u32,
    ) -> Result<AdmittedPeer, Status> {
        validate_context(context, expected_hop)?;
        let cluster_id = parse_cluster_id(&context.cluster_id)?;
        let source = NodeId(context.source_node_id);
        let authenticated = authorize_peer_rpc(
            self.pins.as_ref(),
            cluster_id,
            source,
            PeerRpcKind::DataPlane,
            pin,
        )
        .map_err(|_| Status::permission_denied("peer is not authorized for cluster operations"))?;
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        let placement = ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let expected_fence = PlacementLogId {
            term: context.placement_term,
            index: context.placement_index,
        };
        if placement.cluster_id() != cluster_id
            || placement.fence() != expected_fence
            || !placement.active_node_ids().contains(&source)
            || !placement.active_node_ids().contains(&self.local_node)
        {
            return Err(Status::unavailable(
                "cluster identity, ACTIVE membership, or placement fence changed",
            ));
        }
        Ok(AdmittedPeer {
            authenticated,
            placement,
            timeout: Duration::from_millis(u64::from(context.remaining_deadline_millis)),
        })
    }
}

pub(super) fn validate_context(
    context: &wire::PeerContext,
    expected_hop: u32,
) -> Result<(), Status> {
    if context.schema_version != CLUSTER_PEER_SCHEMA_VERSION {
        return Err(Status::failed_precondition(format!(
            "unsupported cluster-peer schema {}",
            context.schema_version
        )));
    }
    if context.source_node_id == 0
        || context.placement_term == 0
        || context.placement_index == 0
        || context.hop_count != expected_hop
    {
        return Err(Status::invalid_argument(
            "peer identity, placement fence, or hop count is invalid",
        ));
    }
    let deadline = Duration::from_millis(u64::from(context.remaining_deadline_millis));
    if deadline.is_zero() || deadline > MAX_CLUSTER_OPERATION_TIME {
        return Err(Status::invalid_argument(
            "remaining cluster-operation deadline must be within 1ms..=30s",
        ));
    }
    Ok(())
}

fn parse_cluster_id(encoded: &[u8]) -> Result<ClusterId, Status> {
    let bytes: [u8; 16] = encoded
        .try_into()
        .map_err(|_| Status::invalid_argument("cluster id must contain exactly 16 bytes"))?;
    if bytes == [0; 16] {
        return Err(Status::invalid_argument("cluster id must not be all zero"));
    }
    Ok(ClusterId(bytes))
}
