//! Compact-Raft authority fences for replicated background work.

use anyhow::{Result, bail};

use crate::{
    mvcc_bootstrap::MvccSubsystem,
    mvcc_transaction::{AssignmentPredicate, NodeIncarnation},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentGuard {
    pub partition_id: u64,
    pub assignment_epoch: u64,
    pub topology_epoch: u64,
    pub owner: NodeIncarnation,
}

impl AssignmentGuard {
    pub fn lease_owner(&self, worker_id: &str) -> String {
        format!(
            "{worker_id}/partition-{}/assignment-{}/topology-{}/{}-{}",
            self.partition_id,
            self.assignment_epoch,
            self.topology_epoch,
            self.owner.node_id,
            self.owner.incarnation
        )
    }
}

pub fn work_partition_id(kind: &str, logical_identity: &str) -> Result<u64> {
    if kind.trim().is_empty() || logical_identity.trim().is_empty() {
        bail!("background work kind and logical identity are required");
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.mvcc.background-work-partition.v1");
    hasher.update(&(kind.len() as u64).to_be_bytes());
    hasher.update(kind.as_bytes());
    hasher.update(&(logical_identity.len() as u64).to_be_bytes());
    hasher.update(logical_identity.as_bytes());
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    let id = u64::from_be_bytes(bytes);
    Ok(if id == 0 { 1 } else { id })
}

impl MvccSubsystem {
    pub fn claim_assignment(
        &self,
        kind: &str,
        logical_identity: &str,
    ) -> Result<Option<AssignmentGuard>> {
        let partition_id = work_partition_id(kind, logical_identity)?;
        let snapshot = self.consensus.applied_control_snapshot()?;
        let Some((_, assignment)) = snapshot
            .partitions
            .iter()
            .find(|(candidate, _)| *candidate == partition_id)
        else {
            return Ok(None);
        };
        let local = anvil_mvcc_consensus::NodeIncarnation {
            node_id: crate::mvcc_bootstrap::consensus_control_node_id(&self.local_node.node_id),
            incarnation: self.local_node.incarnation,
        };
        if assignment.owner != local {
            return Ok(None);
        }
        Ok(Some(AssignmentGuard {
            partition_id,
            assignment_epoch: assignment.epoch,
            topology_epoch: snapshot.topology_epoch,
            owner: self.local_node.clone(),
        }))
    }

    pub fn validate_assignment(&self, guard: &AssignmentGuard) -> Result<()> {
        let snapshot = self.consensus.applied_control_snapshot()?;
        let owner = anvil_mvcc_consensus::NodeIncarnation {
            node_id: crate::mvcc_bootstrap::consensus_control_node_id(&guard.owner.node_id),
            incarnation: guard.owner.incarnation,
        };
        if snapshot.topology_epoch != guard.topology_epoch
            || !snapshot
                .partitions
                .iter()
                .any(|(partition_id, assignment)| {
                    *partition_id == guard.partition_id
                        && assignment.epoch == guard.assignment_epoch
                        && assignment.owner == owner
                })
        {
            bail!("background work assignment changed while execution was in flight");
        }
        Ok(())
    }

    pub fn stage_assignment_guard(
        &self,
        transaction_id: &str,
        principal: &str,
        guard: &AssignmentGuard,
        now_unix_ms: u64,
    ) -> Result<()> {
        self.open_transactions.require_assignment(
            transaction_id,
            principal,
            AssignmentPredicate {
                partition_id: guard.partition_id,
                assignment_epoch: guard.assignment_epoch,
                topology_epoch: guard.topology_epoch,
                owner: guard.owner.clone(),
            },
            now_unix_ms,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_partition_identity_is_stable_and_domain_separated() {
        assert_eq!(
            work_partition_id("materialisation", "object/a").unwrap(),
            work_partition_id("materialisation", "object/a").unwrap()
        );
        assert_ne!(
            work_partition_id("materialisation", "object/a").unwrap(),
            work_partition_id("repair", "object/a").unwrap()
        );
    }
}
