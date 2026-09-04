//! Immutable view of one applied ACTIVE placement membership.
//!
//! Raft owns descriptors and the placement fence. This module only validates
//! and projects that bounded state into the deterministic HRW inputs used by
//! request coordinators; it persists no ownership decisions.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use keldra_consensus::{ClusterId, NodeDescriptor, NodeId, NodeState, PeerAddress, StateMachine};
use keldra_store::PlacementLogId;
use thiserror::Error;

use crate::placement::{PlacementKind, PlacementNode, rank_nodes};

#[derive(Clone, Debug)]
pub(crate) struct ClusterPlacement {
    cluster_id: ClusterId,
    fence: PlacementLogId,
    nodes: Vec<PlacementNode>,
    addresses: BTreeMap<NodeId, PeerAddress>,
    upload_addresses: BTreeMap<NodeId, PeerAddress>,
}

impl ClusterPlacement {
    pub(crate) fn from_applied(state: &StateMachine) -> Result<Self, ClusterPlacementError> {
        let cluster_id = state
            .cluster_id()
            .ok_or(ClusterPlacementError::ClusterIdentityUnavailable)?;
        let log_id = state
            .cluster_control()
            .active_placement_log_id()
            .ok_or(ClusterPlacementError::FenceUnavailable)?;
        let (nodes, addresses, upload_addresses) = active_nodes(state.cluster_control().nodes())?;
        if nodes.is_empty() {
            return Err(ClusterPlacementError::NoActiveNodes);
        }
        Ok(Self {
            cluster_id,
            fence: PlacementLogId {
                term: log_id.leader_id.term,
                index: log_id.index,
            },
            nodes,
            addresses,
            upload_addresses,
        })
    }

    pub(crate) const fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    pub(crate) const fn fence(&self) -> PlacementLogId {
        self.fence
    }

    pub(crate) fn active_node_ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|node| node.node_id()).collect()
    }

    pub(crate) fn address(&self, node_id: NodeId) -> Option<&PeerAddress> {
        self.addresses.get(&node_id)
    }

    pub(crate) fn upload_source_address(&self, node_id: NodeId) -> Option<&PeerAddress> {
        self.upload_addresses.get(&node_id)
    }

    pub(crate) fn rank(&self, kind: PlacementKind, key: &[u8]) -> Vec<NodeId> {
        rank_nodes(kind, self.cluster_id, key, &self.nodes)
            .into_iter()
            .map(PlacementNode::node_id)
            .collect()
    }

    pub(crate) fn placement_nodes(&self) -> &[PlacementNode] {
        &self.nodes
    }
}

fn active_nodes(
    descriptors: &BTreeMap<NodeId, NodeDescriptor>,
) -> Result<
    (
        Vec<PlacementNode>,
        BTreeMap<NodeId, PeerAddress>,
        BTreeMap<NodeId, PeerAddress>,
    ),
    ClusterPlacementError,
> {
    let mut nodes = Vec::with_capacity(descriptors.len());
    let mut addresses = BTreeMap::new();
    let mut upload_addresses = BTreeMap::new();
    for (node_id, descriptor) in descriptors {
        if descriptor.node_id != *node_id {
            return Err(ClusterPlacementError::DescriptorIdentityMismatch {
                key: *node_id,
                descriptor: descriptor.node_id,
            });
        }
        if matches!(descriptor.state, NodeState::Active | NodeState::Joining) {
            upload_addresses.insert(*node_id, descriptor.peer_address.clone());
        }
        if descriptor.state != NodeState::Active {
            continue;
        }
        let weight = NonZeroU32::new(descriptor.storage_weight_millionths)
            .ok_or(ClusterPlacementError::InvalidWeight { node_id: *node_id })?;
        nodes.push(PlacementNode::new(*node_id, weight));
        addresses.insert(*node_id, descriptor.peer_address.clone());
    }
    Ok((nodes, addresses, upload_addresses))
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum ClusterPlacementError {
    #[error("applied cluster identity is unavailable")]
    ClusterIdentityUnavailable,
    #[error("applied placement fence is unavailable")]
    FenceUnavailable,
    #[error("applied placement contains no ACTIVE node")]
    NoActiveNodes,
    #[error("descriptor map key {key:?} contains descriptor {descriptor:?}")]
    DescriptorIdentityMismatch { key: NodeId, descriptor: NodeId },
    #[error("ACTIVE node {node_id:?} has zero storage weight")]
    InvalidWeight { node_id: NodeId },
}

#[cfg(test)]
mod tests {
    use keldra_consensus::{CapabilityRange, JoinCapabilityHash, PeerSpkiSha256};

    use super::*;

    fn descriptor(node_id: u64, state: NodeState, weight: u32) -> NodeDescriptor {
        NodeDescriptor {
            node_id: NodeId(node_id),
            peer_address: PeerAddress(format!("node-{node_id}:50052")),
            storage_weight_millionths: weight,
            state,
            current_peer_spki_sha256: PeerSpkiSha256([node_id as u8; 32]),
            overlap_peer_spki_sha256: None,
            join_capability_hash: (state == NodeState::Joining)
                .then_some(JoinCapabilityHash([node_id as u8; 32])),
            supported_protocol: CapabilityRange { min: 1, max: 1 },
            supported_storage_format: CapabilityRange { min: 1, max: 1 },
        }
    }

    #[test]
    fn joining_nodes_are_not_projected_into_placement() {
        let descriptors = BTreeMap::from([
            (NodeId(1), descriptor(1, NodeState::Active, 1_000_000)),
            (NodeId(2), descriptor(2, NodeState::Joining, 2_000_000)),
            (NodeId(3), descriptor(3, NodeState::Active, 500_000)),
        ]);

        let (nodes, addresses, upload_addresses) = active_nodes(&descriptors).unwrap();
        assert_eq!(
            nodes.iter().map(|node| node.node_id()).collect::<Vec<_>>(),
            [NodeId(1), NodeId(3)]
        );
        assert_eq!(
            addresses.keys().copied().collect::<Vec<_>>(),
            [NodeId(1), NodeId(3)]
        );
        assert_eq!(
            upload_addresses.keys().copied().collect::<Vec<_>>(),
            [NodeId(1), NodeId(2), NodeId(3)]
        );
    }

    #[test]
    fn malformed_bounded_state_fails_closed() {
        let mismatched = BTreeMap::from([(NodeId(1), descriptor(2, NodeState::Active, 1_000_000))]);
        assert_eq!(
            active_nodes(&mismatched),
            Err(ClusterPlacementError::DescriptorIdentityMismatch {
                key: NodeId(1),
                descriptor: NodeId(2),
            })
        );

        let zero = BTreeMap::from([(NodeId(1), descriptor(1, NodeState::Active, 0))]);
        assert_eq!(
            active_nodes(&zero),
            Err(ClusterPlacementError::InvalidWeight { node_id: NodeId(1) })
        );
    }
}
