//! Leader reconciliation of compact-Raft assignments for durable background work.

use std::{sync::Arc, time::Duration};

use anvil_mvcc_consensus::{Consensus, NodeIncarnation, OpenRaftConsensus};
use anyhow::{Context, Result, bail};
use tokio::sync::watch;

use crate::{mvcc_bootstrap::cluster_id_hash, mvcc_store::LocalMvccStore};

pub struct BackgroundAssignmentReconciler {
    cluster_id: String,
    consensus: Arc<OpenRaftConsensus>,
    store: LocalMvccStore,
}

impl BackgroundAssignmentReconciler {
    pub fn new(
        cluster_id: impl Into<String>,
        consensus: Arc<OpenRaftConsensus>,
        store: LocalMvccStore,
    ) -> Result<Self> {
        let cluster_id = cluster_id.into();
        if cluster_id.trim().is_empty() {
            bail!("assignment reconciler requires a cluster ID");
        }
        Ok(Self {
            cluster_id,
            consensus,
            store,
        })
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            if let Err(error) = self.run_once().await {
                tracing::warn!(error = %error, "background assignment reconciliation failed");
            }
            tokio::select! {
                _ = shutdown.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }
    }

    /// Installs missing assignments and moves assignments whose deterministic
    /// owner changed. Every proposal advances the per-partition epoch and is
    /// preceded by a fresh applied-state check, making retries idempotent.
    pub async fn run_once(&self) -> Result<usize> {
        if !self.consensus.is_leader() {
            return Ok(0);
        }
        self.consensus.linearized_read_barrier().await?;
        let mut required = self.store.required_background_work_partitions()?;
        required.insert(
            crate::object_materialisation_runner::object_materialisation_outbox_partition(
                &self.cluster_id,
            )?
            .0,
        );
        // Existing assignments remain control-state fences even after their
        // current work drains. Include them so a removed/reincarnated owner
        // cannot leave a partition orphaned and block safe node removal.
        required.extend(
            self.consensus
                .applied_control_snapshot()?
                .partitions
                .into_iter()
                .map(|(partition_id, _)| partition_id),
        );
        let mut changed = 0usize;
        for partition_id in required {
            if !self.consensus.is_leader() {
                break;
            }
            changed = changed.saturating_add(usize::from(
                reconcile_partition_assignment(&self.cluster_id, &self.consensus, partition_id)
                    .await?,
            ));
        }
        Ok(changed)
    }
}

/// Reconciles one deterministic compact-Raft partition assignment.
///
/// This is also used by foreground admission for domains whose first durable
/// row does not exist yet and therefore cannot be discovered by the background
/// store scan.
pub(crate) async fn reconcile_partition_assignment(
    cluster_id: &str,
    consensus: &OpenRaftConsensus,
    partition_id: u64,
) -> Result<bool> {
    if !consensus.is_leader() {
        bail!("compact-Raft leader must reconcile a missing partition assignment");
    }
    consensus.linearized_read_barrier().await?;
    let snapshot = consensus.applied_control_snapshot()?;
    let installed = snapshot
        .nodes
        .iter()
        .map(
            |(node_id, _raft_node_id, incarnation, _failure_domain)| NodeIncarnation {
                node_id: *node_id,
                incarnation: *incarnation,
            },
        )
        .collect::<Vec<_>>();
    let desired = rendezvous_owner(partition_id, &installed)
        .context("partition assignment requires at least one installed compact-Raft node")?;
    let current = snapshot
        .partitions
        .iter()
        .find(|(candidate, _)| *candidate == partition_id)
        .map(|(_, assignment)| assignment);
    if current.is_some_and(|assignment| assignment.owner == desired) {
        return Ok(false);
    }
    let epoch = current
        .map(|assignment| assignment.epoch.saturating_add(1))
        .unwrap_or(1);
    consensus
        .assign_partition(cluster_id_hash(cluster_id), partition_id, desired, epoch)
        .await
        .with_context(|| {
            format!("assign compact-Raft partition {partition_id} at epoch {epoch}")
        })?;
    Ok(true)
}

pub(crate) fn rendezvous_owner(
    partition_id: u64,
    nodes: &[NodeIncarnation],
) -> Option<NodeIncarnation> {
    nodes.iter().copied().max_by_key(|node| {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"anvil.mvcc.background-assignment.v1");
        hasher.update(&partition_id.to_be_bytes());
        hasher.update(&node.node_id.0.to_be_bytes());
        hasher.update(&node.incarnation.to_be_bytes());
        *hasher.finalize().as_bytes()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendezvous_placement_is_stable_and_uses_incarnations() {
        let nodes = [
            NodeIncarnation {
                node_id: anvil_mvcc_consensus::NodeId(1),
                incarnation: 1,
            },
            NodeIncarnation {
                node_id: anvil_mvcc_consensus::NodeId(2),
                incarnation: 1,
            },
        ];
        assert_eq!(rendezvous_owner(7, &nodes), rendezvous_owner(7, &nodes));
        let mut reincarnated = nodes;
        reincarnated[0].incarnation = 2;
        assert!(rendezvous_owner(7, &reincarnated).is_some());
    }
}
