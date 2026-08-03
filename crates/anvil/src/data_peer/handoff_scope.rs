//! Raft-backed authority for one exact ADD handoff.

use anvil_consensus::{
    AuthenticatedPeer, DecisionRaft, MembershipTransition, MembershipTransitionKind,
    NodeDescriptor, NodeId, NodeState,
};
use tonic::Status;

use super::wire;

#[derive(Clone)]
pub(super) struct HandoffAuthority {
    source: HandoffAuthoritySource,
}

#[derive(Clone)]
enum HandoffAuthoritySource {
    Raft {
        decisions: DecisionRaft,
        local_node: NodeId,
    },
    #[cfg(test)]
    Reject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HandoffTarget {
    AnyNode,
    JoiningNode,
}

impl HandoffAuthority {
    pub(super) fn raft(decisions: DecisionRaft, local_node: NodeId) -> Self {
        Self {
            source: HandoffAuthoritySource::Raft {
                decisions,
                local_node,
            },
        }
    }

    #[cfg(test)]
    pub(super) fn reject() -> Self {
        Self {
            source: HandoffAuthoritySource::Reject,
        }
    }

    pub(super) fn validate(
        &self,
        caller: AuthenticatedPeer,
        scope: Option<&wire::HandoffScope>,
        target: HandoffTarget,
    ) -> Result<(), Status> {
        let scope = scope.ok_or_else(|| Status::invalid_argument("handoff scope is required"))?;
        if scope.joining_node_id == 0 || scope.started_log_index == 0 {
            return Err(Status::invalid_argument(
                "handoff scope requires non-zero node and log index",
            ));
        }
        match &self.source {
            HandoffAuthoritySource::Raft {
                decisions,
                local_node,
            } => {
                let state = decisions
                    .state()
                    .map_err(|error| Status::unavailable(error.to_string()))?;
                let joining = NodeId(scope.joining_node_id);
                validate_facts(
                    caller.node_id,
                    *local_node,
                    decisions.current_leader().map(NodeId),
                    state.cluster_control().transition(),
                    state.cluster_control().nodes().get(&joining),
                    scope,
                    target,
                )
            }
            #[cfg(test)]
            HandoffAuthoritySource::Reject => Err(Status::failed_precondition(
                "no ADD handoff is active in this test service",
            )),
        }
    }
}

fn validate_facts(
    caller: NodeId,
    local_node: NodeId,
    current_leader: Option<NodeId>,
    transition: Option<&MembershipTransition>,
    descriptor: Option<&NodeDescriptor>,
    scope: &wire::HandoffScope,
    target: HandoffTarget,
) -> Result<(), Status> {
    if current_leader != Some(caller) {
        return Err(Status::permission_denied(
            "handoff caller is not the current Raft leader",
        ));
    }
    let transition = transition.ok_or_else(|| {
        Status::failed_precondition("handoff does not match an in-progress ADD transition")
    })?;
    if transition.kind != MembershipTransitionKind::Add
        || transition.node_id.0 != scope.joining_node_id
        || transition.started_log_index != scope.started_log_index
    {
        return Err(Status::failed_precondition(
            "handoff does not match the exact in-progress ADD transition",
        ));
    }
    if descriptor.is_none_or(|descriptor| {
        descriptor.node_id != transition.node_id || descriptor.state != NodeState::Joining
    }) {
        return Err(Status::failed_precondition(
            "handoff target is not the current JOINING node",
        ));
    }
    if target == HandoffTarget::JoiningNode && local_node != transition.node_id {
        return Err(Status::failed_precondition(
            "handoff install was sent to a node other than its JOINING target",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use anvil_consensus::{CapabilityRange, PeerAddress, PeerSpkiSha256};

    use super::*;

    fn descriptor(node_id: NodeId) -> NodeDescriptor {
        NodeDescriptor {
            node_id,
            peer_address: PeerAddress("node.internal:50052".into()),
            storage_weight_millionths: 1_000_000,
            state: NodeState::Joining,
            current_peer_spki_sha256: PeerSpkiSha256([7; 32]),
            overlap_peer_spki_sha256: None,
            join_capability_hash: None,
            supported_protocol: CapabilityRange { min: 1, max: 1 },
            supported_storage_format: CapabilityRange { min: 1, max: 1 },
        }
    }

    fn transition() -> MembershipTransition {
        MembershipTransition {
            kind: MembershipTransitionKind::Add,
            node_id: NodeId(2),
            started_log_index: 17,
            target_weight_millionths: Some(1_000_000),
        }
    }

    fn scope() -> wire::HandoffScope {
        wire::HandoffScope {
            joining_node_id: 2,
            started_log_index: 17,
        }
    }

    #[test]
    fn exact_current_leader_and_transition_are_accepted() {
        validate_facts(
            NodeId(1),
            NodeId(2),
            Some(NodeId(1)),
            Some(&transition()),
            Some(&descriptor(NodeId(2))),
            &scope(),
            HandoffTarget::JoiningNode,
        )
        .unwrap();
    }

    #[test]
    fn old_transition_replay_is_rejected() {
        let mut old = scope();
        old.started_log_index -= 1;
        let error = validate_facts(
            NodeId(1),
            NodeId(2),
            Some(NodeId(1)),
            Some(&transition()),
            Some(&descriptor(NodeId(2))),
            &old,
            HandoffTarget::JoiningNode,
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn wrong_leader_and_wrong_install_target_are_rejected() {
        let wrong_leader = validate_facts(
            NodeId(3),
            NodeId(2),
            Some(NodeId(1)),
            Some(&transition()),
            Some(&descriptor(NodeId(2))),
            &scope(),
            HandoffTarget::JoiningNode,
        )
        .unwrap_err();
        assert_eq!(wrong_leader.code(), tonic::Code::PermissionDenied);

        let wrong_target = validate_facts(
            NodeId(1),
            NodeId(3),
            Some(NodeId(1)),
            Some(&transition()),
            Some(&descriptor(NodeId(2))),
            &scope(),
            HandoffTarget::JoiningNode,
        )
        .unwrap_err();
        assert_eq!(wrong_target.code(), tonic::Code::FailedPrecondition);
    }
}
