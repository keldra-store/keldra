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
    incarnation_fences: BTreeMap<NodeId, u64>,
    partitions: BTreeMap<u64, PartitionAssignment>,
    durability_policy: ConsensusDurabilityPolicy,
    gc_safety_watermark: CommitVersion,
}

impl ClusterControlState {
    pub fn new(cluster_id_hash: [u8; 32]) -> Result<Self, String> {
        if cluster_id_hash == [0; 32] {
            return Err("cluster identity must be configured".into());
        }
        Ok(Self {
            cluster_id_hash,
            nodes: BTreeMap::new(),
            incarnation_fences: BTreeMap::new(),
            partitions: BTreeMap::new(),
            durability_policy: ConsensusDurabilityPolicy::default(),
            gc_safety_watermark: CommitVersion(0),
        })
    }

    pub fn cluster_id_hash(&self) -> [u8; 32] {
        self.cluster_id_hash
    }

    pub fn node_incarnation(&self, node_id: NodeId) -> Option<u64> {
        self.nodes.get(&node_id).copied()
    }

    pub fn incarnation_fence(&self, node_id: NodeId) -> u64 {
        self.incarnation_fences.get(&node_id).copied().unwrap_or(0)
    }

    pub fn partition(&self, partition_id: u64) -> Option<&PartitionAssignment> {
        self.partitions.get(&partition_id)
    }

    pub fn durability_policy(&self) -> ConsensusDurabilityPolicy {
        self.durability_policy
    }

    pub fn gc_safety_watermark(&self) -> CommitVersion {
        self.gc_safety_watermark
    }

    pub fn apply(&mut self, command: &ConsensusCommand) -> Result<ControlApplyResult, String> {
        if command.cluster_id_hash() != self.cluster_id_hash {
            return Err("control command belongs to another cluster".into());
        }
        match command {
            ConsensusCommand::Certify(_) => Err("certification is not a control command".into()),
            ConsensusCommand::InstallNode { node, .. } => {
                if node.node_id.0 == 0 || node.incarnation == 0 {
                    return Err("node identity and incarnation must be non-zero".into());
                }
                let fence = self.incarnation_fence(node.node_id);
                let current = self.node_incarnation(node.node_id).unwrap_or(0);
                if node.incarnation <= fence || node.incarnation <= current {
                    return Err("node incarnation must advance its durable fence".into());
                }
                self.nodes.insert(node.node_id, node.incarnation);
                self.incarnation_fences
                    .insert(node.node_id, node.incarnation);
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
                self.incarnation_fences
                    .insert(node.node_id, node.incarnation);
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
                })
                .is_err()
        );
        state
            .apply(&ConsensusCommand::InstallNode {
                cluster_id_hash: CLUSTER,
                node: node(2),
            })
            .unwrap();
    }
}
