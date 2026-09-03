use std::time::Duration;

use keldra_consensus::{
    AuthenticatedPeer, ClusterId, MembershipTransitionKind, NodeId, NodeState, PeerRpcKind,
    PeerSpkiSha256, authorize_peer_rpc,
};
use keldra_store::PlacementLogId;
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
        self.admit_with_timeout_limit(request, context, expected_hop, MAX_CLUSTER_OPERATION_TIME)
    }

    pub(super) fn admit_with_timeout_limit<T>(
        &self,
        request: &Request<T>,
        context: Option<&wire::PeerContext>,
        expected_hop: u32,
        max_timeout: Duration,
    ) -> Result<AdmittedPeer, Status> {
        let pin = request
            .extensions()
            .get::<PeerSpkiSha256>()
            .copied()
            .ok_or_else(|| Status::unauthenticated("peer mTLS identity is missing"))?;
        self.admit_pin_with_timeout_limit(
            pin,
            context.ok_or_else(|| Status::invalid_argument("peer context is required"))?,
            expected_hop,
            max_timeout,
        )
    }

    pub(super) fn admit_pin(
        &self,
        pin: PeerSpkiSha256,
        context: &wire::PeerContext,
        expected_hop: u32,
    ) -> Result<AdmittedPeer, Status> {
        self.admit_pin_with_timeout_limit(pin, context, expected_hop, MAX_CLUSTER_OPERATION_TIME)
    }

    pub(super) fn admit_pin_with_timeout_limit(
        &self,
        pin: PeerSpkiSha256,
        context: &wire::PeerContext,
        expected_hop: u32,
        max_timeout: Duration,
    ) -> Result<AdmittedPeer, Status> {
        validate_context_with_timeout_limit(context, expected_hop, max_timeout)?;
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
        let joining_coordinator = state
            .cluster_control()
            .transition()
            .is_some_and(|transition| {
                transition.kind == MembershipTransitionKind::Add
                    && transition.node_id == source
                    && state
                        .cluster_control()
                        .nodes()
                        .get(&source)
                        .is_some_and(|descriptor| descriptor.state == NodeState::Joining)
            });
        if placement.cluster_id() != cluster_id
            || placement.fence() != expected_fence
            || (!placement.active_node_ids().contains(&source) && !joining_coordinator)
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
    validate_context_with_timeout_limit(context, expected_hop, MAX_CLUSTER_OPERATION_TIME)
}

pub(super) fn validate_context_with_timeout_limit(
    context: &wire::PeerContext,
    expected_hop: u32,
    max_timeout: Duration,
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
    if deadline.is_zero() || deadline > max_timeout {
        return Err(Status::invalid_argument(
            "remaining cluster-operation deadline exceeds this operation's internal limit",
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
