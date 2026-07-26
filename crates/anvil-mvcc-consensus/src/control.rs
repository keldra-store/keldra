use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    CommitVersion, ConsensusCommand, ConsensusDurabilityPolicy, ControlApplyResult, NodeId,
    PartitionAssignment,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterControlState {
    cluster_id_hash: [u8; 32],
    nodes: BTreeMap<NodeId, u64>,
    raft_node_ids: BTreeMap<NodeId, NodeId>,
    node_failure_domains: BTreeMap<NodeId, String>,
    incarnation_fences: BTreeMap<NodeId, u64>,
    partitions: BTreeMap<u64, PartitionAssignment>,
    durability_policy: ConsensusDurabilityPolicy,
    gc_safety_watermark: CommitVersion,
    topology_epoch: u64,
}

impl ClusterControlState {
    pub fn new(cluster_id_hash: [u8; 32]) -> Result<Self, String> {
        if cluster_id_hash == [0; 32] {
            return Err("cluster identity must be configured".into());
        }
        Ok(Self {
            cluster_id_hash,
            nodes: BTreeMap::new(),
            raft_node_ids: BTreeMap::new(),
            node_failure_domains: BTreeMap::new(),
            incarnation_fences: BTreeMap::new(),
            partitions: BTreeMap::new(),
            durability_policy: ConsensusDurabilityPolicy::default(),
            gc_safety_watermark: CommitVersion(0),
            topology_epoch: 0,
        })
    }

    pub fn cluster_id_hash(&self) -> [u8; 32] {
        self.cluster_id_hash
    }

    pub fn node_incarnation(&self, node_id: NodeId) -> Option<u64> {
        self.nodes.get(&node_id).copied()
    }

    pub fn raft_node_id(&self, node_id: NodeId) -> Option<NodeId> {
        self.raft_node_ids.get(&node_id).copied()
    }

    pub fn nodes(&self) -> impl Iterator<Item = (NodeId, NodeId, u64, &str)> {
        self.nodes.iter().filter_map(|(node_id, incarnation)| {
            self.raft_node_ids.get(node_id).and_then(|raft_node_id| {
                self.node_failure_domains
                    .get(node_id)
                    .map(|domain| (*node_id, *raft_node_id, *incarnation, domain.as_str()))
            })
        })
    }

    pub fn incarnation_fence(&self, node_id: NodeId) -> u64 {
        self.incarnation_fences.get(&node_id).copied().unwrap_or(0)
    }

    pub fn partition(&self, partition_id: u64) -> Option<&PartitionAssignment> {
        self.partitions.get(&partition_id)
    }

    pub fn partitions(&self) -> impl Iterator<Item = (u64, &PartitionAssignment)> {
        self.partitions
            .iter()
            .map(|(partition_id, assignment)| (*partition_id, assignment))
    }

    pub fn durability_policy(&self) -> ConsensusDurabilityPolicy {
        self.durability_policy
    }

    pub fn gc_safety_watermark(&self) -> CommitVersion {
        self.gc_safety_watermark
    }

    pub fn topology_epoch(&self) -> u64 {
        self.topology_epoch
    }

    pub fn apply(&mut self, command: &ConsensusCommand) -> Result<ControlApplyResult, String> {
        if command.cluster_id_hash() != self.cluster_id_hash {
            return Err("control command belongs to another cluster".into());
        }
        match command {
            ConsensusCommand::Certify(_) => Err("certification is not a control command".into()),
            ConsensusCommand::InstallNode {
                node,
                raft_node_id,
                failure_domain,
                ..
            } => {
                if node.node_id.0 == 0
                    || raft_node_id.0 == 0
                    || node.incarnation == 0
                    || failure_domain.trim().is_empty()
                {
                    return Err("node identity and incarnation must be non-zero".into());
                }
                if self
                    .raft_node_ids
                    .iter()
                    .any(|(installed, id)| installed != &node.node_id && id == raft_node_id)
                {
                    return Err("Raft node ID is already bound to another product node".into());
                }
                let fence = self.incarnation_fence(node.node_id);
                let current = self.node_incarnation(node.node_id).unwrap_or(0);
                if node.incarnation <= fence || node.incarnation <= current {
                    return Err("node incarnation must advance its durable fence".into());
                }
                self.nodes.insert(node.node_id, node.incarnation);
                self.raft_node_ids.insert(node.node_id, *raft_node_id);
                self.node_failure_domains
                    .insert(node.node_id, failure_domain.clone());
                self.incarnation_fences
                    .insert(node.node_id, node.incarnation);
                self.topology_epoch = self.topology_epoch.saturating_add(1);
                Ok(ControlApplyResult::NodeInstalled(*node))
            }
            ConsensusCommand::RemoveNode { node, .. } => {
                if self.node_incarnation(node.node_id) != Some(node.incarnation) {
                    return Err("node removal does not match installed incarnation".into());
                }
                if self
                    .partitions
                    .values()
                    .any(|assignment| assignment.owner == *node)
                {
                    return Err("node still owns authoritative partitions".into());
                }
                self.nodes.remove(&node.node_id);
                self.raft_node_ids.remove(&node.node_id);
                self.node_failure_domains.remove(&node.node_id);
                self.incarnation_fences
                    .insert(node.node_id, node.incarnation);
                self.topology_epoch = self.topology_epoch.saturating_add(1);
                Ok(ControlApplyResult::NodeRemoved(*node))
            }
            ConsensusCommand::AssignPartition {
                partition_id,
                owner,
                epoch,
                ..
            } => {
                if *partition_id == 0 || *epoch == 0 {
                    return Err("partition identity and epoch must be non-zero".into());
                }
                if self.node_incarnation(owner.node_id) != Some(owner.incarnation) {
                    return Err("partition owner incarnation is not installed".into());
                }
                if self
                    .partitions
                    .get(partition_id)
                    .is_some_and(|current| *epoch <= current.epoch)
                {
                    return Err("partition epoch must increase".into());
                }
                let assignment = PartitionAssignment {
                    owner: *owner,
                    epoch: *epoch,
                };
                self.partitions.insert(*partition_id, assignment.clone());
                self.topology_epoch = self.topology_epoch.saturating_add(1);
                Ok(ControlApplyResult::PartitionAssigned {
                    partition_id: *partition_id,
                    assignment,
                })
            }
            ConsensusCommand::SetDurabilityPolicy {
                generation,
                bundle_quorum_holders,
                tolerated_failure_domains,
                ..
            } => {
                if *generation == 0
                    || *generation <= self.durability_policy.generation
                    || *bundle_quorum_holders == 0
                {
                    return Err("durability policy generation and quorum must advance".into());
                }
                let policy = ConsensusDurabilityPolicy {
                    generation: *generation,
                    bundle_quorum_holders: *bundle_quorum_holders,
                    tolerated_failure_domains: *tolerated_failure_domains,
                };
                self.durability_policy = policy;
                self.topology_epoch = self.topology_epoch.saturating_add(1);
                Ok(ControlApplyResult::DurabilityPolicySet(policy))
            }
            ConsensusCommand::AdvanceGcWatermark { watermark, .. } => {
                if *watermark < self.gc_safety_watermark {
                    return Err("GC safety watermark cannot move backwards".into());
                }
                self.gc_safety_watermark = *watermark;
                Ok(ControlApplyResult::GcWatermarkAdvanced(*watermark))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeIncarnation;

    const CLUSTER: [u8; 32] = [7; 32];

    fn node(incarnation: u64) -> NodeIncarnation {
        NodeIncarnation {
            node_id: NodeId(1),
            incarnation,
        }
    }

    #[test]
    fn fencing_partition_epochs_policy_and_gc_are_monotonic() {
        let mut state = ClusterControlState::new(CLUSTER).unwrap();
        state
            .apply(&ConsensusCommand::InstallNode {
                cluster_id_hash: CLUSTER,
                node: node(1),
                raft_node_id: NodeId(1),
                failure_domain: "zone-a".into(),
            })
            .unwrap();
        state
            .apply(&ConsensusCommand::AssignPartition {
                cluster_id_hash: CLUSTER,
                partition_id: 4,
                owner: node(1),
                epoch: 1,
            })
            .unwrap();
        assert!(
            state
                .apply(&ConsensusCommand::AssignPartition {
                    cluster_id_hash: CLUSTER,
                    partition_id: 4,
                    owner: node(1),
                    epoch: 1,
                })
                .is_err()
        );
        state
            .apply(&ConsensusCommand::SetDurabilityPolicy {
                cluster_id_hash: CLUSTER,
                generation: 1,
                bundle_quorum_holders: 2,
                tolerated_failure_domains: 1,
            })
            .unwrap();
        assert!(
            state
                .apply(&ConsensusCommand::SetDurabilityPolicy {
                    cluster_id_hash: CLUSTER,
                    generation: 1,
                    bundle_quorum_holders: 3,
                    tolerated_failure_domains: 1,
                })
                .is_err()
        );
        state
            .apply(&ConsensusCommand::AdvanceGcWatermark {
                cluster_id_hash: CLUSTER,
                watermark: CommitVersion(9),
            })
            .unwrap();
        assert!(
            state
                .apply(&ConsensusCommand::AdvanceGcWatermark {
                    cluster_id_hash: CLUSTER,
                    watermark: CommitVersion(8),
                })
                .is_err()
        );
    }

    #[test]
    fn removal_fences_incarnation_and_requires_partition_reassignment() {
        let mut state = ClusterControlState::new(CLUSTER).unwrap();
        state
            .apply(&ConsensusCommand::InstallNode {
                cluster_id_hash: CLUSTER,
                node: node(1),
                raft_node_id: NodeId(1),
                failure_domain: "zone-a".into(),
            })
            .unwrap();
        state
            .apply(&ConsensusCommand::RemoveNode {
                cluster_id_hash: CLUSTER,
                node: node(1),
            })
            .unwrap();
        assert!(
            state
                .apply(&ConsensusCommand::InstallNode {
                    cluster_id_hash: CLUSTER,
                    node: node(1),
                    raft_node_id: NodeId(1),
                    failure_domain: "zone-a".into(),
                })
                .is_err()
        );
        state
            .apply(&ConsensusCommand::InstallNode {
                cluster_id_hash: CLUSTER,
                node: node(2),
                raft_node_id: NodeId(1),
                failure_domain: "zone-a".into(),
            })
            .unwrap();
    }

    #[test]
    fn applied_node_domains_and_policy_advance_topology_epoch() {
        let mut state = ClusterControlState::new(CLUSTER).unwrap();
        state
            .apply(&ConsensusCommand::InstallNode {
                cluster_id_hash: CLUSTER,
                node: node(1),
                raft_node_id: NodeId(1),
                failure_domain: "zone-a".into(),
            })
            .unwrap();
        let node_epoch = state.topology_epoch();
        assert_eq!(
            state.nodes().collect::<Vec<_>>(),
            vec![(NodeId(1), NodeId(1), 1, "zone-a")]
        );
        state
            .apply(&ConsensusCommand::SetDurabilityPolicy {
                cluster_id_hash: CLUSTER,
                generation: 1,
                bundle_quorum_holders: 1,
                tolerated_failure_domains: 0,
            })
            .unwrap();
        assert!(state.topology_epoch() > node_epoch);
        assert_eq!(state.durability_policy().generation, 1);
    }
}
