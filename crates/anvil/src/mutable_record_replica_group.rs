//! Deterministic complete-record replica-group selection.
//!
//! This module only combines the cluster placement ranking with the fixed
//! mutable-record quorum. Placement-key encoding and replica I/O belong to
//! their respective callers.

use anvil_consensus::{ClusterId, NodeId};

use crate::mutable_record_quorum::MutableRecordQuorum;
use crate::placement::{PlacementKind, PlacementNode, rank_nodes};

/// The ranked replica nodes and acknowledgement rule for one mutable record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MutableRecordReplicaGroup {
    replicas: Vec<NodeId>,
    quorum: MutableRecordQuorum,
}

impl MutableRecordReplicaGroup {
    /// Select a mutable record's replicas from the active placement set.
    ///
    /// `placement_kind` and `key` are already encoded by the caller. Active
    /// membership supplies exactly one [`PlacementNode`] per node.
    pub(crate) fn select(
        placement_kind: PlacementKind,
        cluster_id: ClusterId,
        key: &[u8],
        active_nodes: &[PlacementNode],
    ) -> Option<Self> {
        let quorum = MutableRecordQuorum::for_active_nodes(active_nodes.len())?;
        let replicas = rank_nodes(placement_kind, cluster_id, key, active_nodes)
            .into_iter()
            .take(quorum.replica_count())
            .map(PlacementNode::node_id)
            .collect::<Vec<_>>();

        debug_assert_eq!(replicas.len(), quorum.replica_count());
        debug_assert!(
            replicas
                .iter()
                .enumerate()
                .all(|(index, node_id)| !replicas[..index].contains(node_id)),
            "active placement membership must contain distinct node IDs"
        );

        Some(Self { replicas, quorum })
    }

    pub(crate) fn replicas(&self) -> &[NodeId] {
        &self.replicas
    }

    pub(crate) fn coordinator(&self) -> NodeId {
        self.replicas[0]
    }

    pub(crate) const fn required_acknowledgements(&self) -> usize {
        self.quorum.required_acknowledgements()
    }

    /// Whether distinct selected replicas durably acknowledged the mutation.
    ///
    /// Duplicate acknowledgements and acknowledgements from nodes outside this
    /// replica group do not count.
    pub(crate) fn is_acknowledged_by(&self, durable_nodes: &[NodeId]) -> bool {
        let distinct_durable_replicas = self
            .replicas
            .iter()
            .filter(|replica| durable_nodes.contains(replica))
            .count();

        self.quorum.is_satisfied_by(distinct_durable_replicas)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroU32;

    use super::*;

    const PLACEMENT_KIND: PlacementKind = PlacementKind::Object;
    const PLACEMENT_KEY: &[u8] = b"already-encoded-mutable-record-key";

    fn cluster_id() -> ClusterId {
        ClusterId([42; 16])
    }

    fn node(node_id: u64, weight_millionths: u32) -> PlacementNode {
        PlacementNode::new(
            NodeId(node_id),
            NonZeroU32::new(weight_millionths).expect("test weight must be positive"),
        )
    }

    fn active_nodes(count: usize) -> Vec<PlacementNode> {
        [
            node(7, 1_000_000),
            node(2, 2_000_000),
            node(19, 500_000),
            node(11, 1_500_000),
        ][..count]
            .to_vec()
    }

    fn select(active_nodes: &[PlacementNode]) -> Option<MutableRecordReplicaGroup> {
        MutableRecordReplicaGroup::select(PLACEMENT_KIND, cluster_id(), PLACEMENT_KEY, active_nodes)
    }

    #[test]
    fn no_active_nodes_have_no_replica_group() {
        assert_eq!(select(&[]), None);
    }

    #[test]
    fn one_two_and_four_nodes_select_the_first_one_two_and_three_ranks() {
        for active_count in [1, 2, 4] {
            let active = active_nodes(active_count);
            let expected = rank_nodes(PLACEMENT_KIND, cluster_id(), PLACEMENT_KEY, &active)
                .into_iter()
                .take(active_count.min(3))
                .map(PlacementNode::node_id)
                .collect::<Vec<_>>();
            let group = select(&active).expect("non-empty membership has a replica group");

            assert_eq!(group.replicas(), expected);
            assert_eq!(
                group
                    .replicas()
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>()
                    .len(),
                group.replicas().len()
            );
        }
    }

    #[test]
    fn coordinator_is_rank_zero() {
        let active = active_nodes(4);
        let rank_zero =
            rank_nodes(PLACEMENT_KIND, cluster_id(), PLACEMENT_KEY, &active)[0].node_id();
        let group = select(&active).unwrap();

        assert_eq!(group.coordinator(), rank_zero);
        assert_eq!(group.coordinator(), group.replicas()[0]);
    }

    #[test]
    fn acknowledgements_follow_one_of_one_two_of_two_and_two_of_three() {
        let one = select(&active_nodes(1)).unwrap();
        assert_eq!(one.required_acknowledgements(), 1);
        assert!(!one.is_acknowledged_by(&[]));
        assert!(one.is_acknowledged_by(&[one.replicas()[0]]));

        let two = select(&active_nodes(2)).unwrap();
        assert_eq!(two.required_acknowledgements(), 2);
        assert!(!two.is_acknowledged_by(&[two.replicas()[0]]));
        assert!(!two.is_acknowledged_by(&[two.replicas()[0], two.replicas()[0]]));
        assert!(two.is_acknowledged_by(two.replicas()));

        let three = select(&active_nodes(4)).unwrap();
        assert_eq!(three.required_acknowledgements(), 2);
        assert!(!three.is_acknowledged_by(&[three.replicas()[0], NodeId(88)]));
        assert!(three.is_acknowledged_by(&three.replicas()[..2]));
    }
}
