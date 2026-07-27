//! Authorized operational entrypoints for the cluster transaction group.
//!
//! These operations deliberately do not use `OpenTransactionRegistry`: mesh and
//! region topology is outside the cluster transaction group.  A capability can
//! only be minted after a Zanzibar check against the cluster-wide system
//! object, and is permanently bound to one cluster identifier.

use std::collections::BTreeSet;

use anvil_mvcc_consensus::{
    CommitVersion, ConsensusNode, ControlApplyResult, NodeId,
    NodeIncarnation as ConsensusNodeIncarnation,
};
use anyhow::{Context, Result, bail};
use tonic::Status;

use crate::{
    access_control, auth,
    mvcc_bootstrap::{MvccSubsystem, consensus_control_node_id},
    mvcc_transaction::NodeIncarnation,
    storage::Storage,
    system_realm::{SYSTEM_NAMESPACE, SYSTEM_OBJECT_ID},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClusterControlPermission {
    Nodes,
    Policies,
    Partitions,
}

impl ClusterControlPermission {
    fn relation(self) -> &'static str {
        match self {
            Self::Nodes => "manage_nodes",
            Self::Policies => "manage_policies",
            Self::Partitions => "manage_partitions",
        }
    }
}

/// Proof that one authenticated principal may perform one class of cluster
/// control operation. Fields are private so ordinary transaction, mesh, and
/// region code cannot manufacture this proof.
#[derive(Debug)]
pub struct AuthorizedClusterControl {
    cluster_id: String,
    permission: ClusterControlPermission,
}

impl AuthorizedClusterControl {
    fn require(&self, cluster_id: &str, permission: ClusterControlPermission) -> Result<()> {
        if self.cluster_id != cluster_id {
            bail!("cluster control authorization belongs to another cluster");
        }
        if self.permission != permission {
            bail!("cluster control authorization does not cover this operation");
        }
        Ok(())
    }
}

async fn authorize(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    claims: &auth::Claims,
    cluster_id: &str,
    permission: ClusterControlPermission,
) -> Result<AuthorizedClusterControl, Status> {
    if cluster_id.trim().is_empty() {
        return Err(Status::invalid_argument("cluster ID is required"));
    }
    access_control::require_system_realm_permission(
        storage,
        mvcc,
        claims,
        SYSTEM_NAMESPACE,
        SYSTEM_OBJECT_ID,
        permission.relation(),
    )
    .await?;
    Ok(AuthorizedClusterControl {
        cluster_id: cluster_id.to_string(),
        permission,
    })
}

pub async fn authorize_node_control(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    claims: &auth::Claims,
    cluster_id: &str,
) -> Result<AuthorizedClusterControl, Status> {
    authorize(
        storage,
        mvcc,
        claims,
        cluster_id,
        ClusterControlPermission::Nodes,
    )
    .await
}

pub async fn authorize_policy_control(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    claims: &auth::Claims,
    cluster_id: &str,
) -> Result<AuthorizedClusterControl, Status> {
    authorize(
        storage,
        mvcc,
        claims,
        cluster_id,
        ClusterControlPermission::Policies,
    )
    .await
}

pub async fn authorize_gc_control(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    claims: &auth::Claims,
    cluster_id: &str,
) -> Result<AuthorizedClusterControl, Status> {
    authorize(
        storage,
        mvcc,
        claims,
        cluster_id,
        ClusterControlPermission::Partitions,
    )
    .await
}

impl MvccSubsystem {
    /// Add or refresh a learner's OpenRaft route. Retrying the same desired
    /// learner is delegated to OpenRaft's idempotent membership operation.
    pub async fn add_cluster_learner(
        &self,
        authorization: &AuthorizedClusterControl,
        raft_node_id: u64,
        endpoint: String,
        blocking: bool,
    ) -> Result<()> {
        authorization.require(self.cluster_id(), ClusterControlPermission::Nodes)?;
        if raft_node_id == 0 || endpoint.trim().is_empty() {
            bail!("learner node ID and endpoint are required");
        }
        self.consensus
            .add_learner(
                NodeId(raft_node_id),
                ConsensusNode { address: endpoint },
                blocking,
            )
            .await
            .context("add OpenRaft learner")
    }

    /// Atomically install the complete voter set. Passing the current set is an
    /// idempotent OpenRaft membership retry.
    pub async fn set_cluster_voters(
        &self,
        authorization: &AuthorizedClusterControl,
        voters: BTreeSet<u64>,
        retain_removed_as_learners: bool,
    ) -> Result<()> {
        authorization.require(self.cluster_id(), ClusterControlPermission::Nodes)?;
        if voters.is_empty() || voters.contains(&0) {
            bail!("the cluster voter set must contain non-zero node IDs");
        }
        self.consensus
            .change_membership(
                voters.into_iter().map(NodeId).collect(),
                retain_removed_as_learners,
            )
            .await
            .context("change OpenRaft membership")
    }

    /// Install a product node incarnation and failure-domain assignment in
    /// compact Raft control state. Exact retries are answered from applied
    /// state without appending another log entry.
    pub async fn install_cluster_node(
        &self,
        authorization: &AuthorizedClusterControl,
        raft_node_id: u64,
        node: NodeIncarnation,
        failure_domain: String,
    ) -> Result<ControlApplyResult> {
        authorization.require(self.cluster_id(), ClusterControlPermission::Nodes)?;
        let control_node_id = consensus_control_node_id(&node.node_id);
        let snapshot = self.consensus.applied_control_snapshot()?;
        if snapshot
            .nodes
            .iter()
            .any(|(id, installed_raft_id, incarnation, domain)| {
                *id == control_node_id
                    && installed_raft_id.0 == raft_node_id
                    && *incarnation == node.incarnation
                    && *domain == failure_domain
            })
        {
            return Ok(ControlApplyResult::NodeInstalled(
                ConsensusNodeIncarnation {
                    node_id: control_node_id,
                    incarnation: node.incarnation,
                },
            ));
        }
        let replaced = snapshot
            .nodes
            .iter()
            .find(|(id, _raft_node_id, incarnation, _domain)| {
                *id == control_node_id && *incarnation != node.incarnation
            })
            .map(|(_, _, incarnation, _)| ConsensusNodeIncarnation {
                node_id: control_node_id,
                incarnation: *incarnation,
            });
        let installed = ConsensusNodeIncarnation {
            node_id: control_node_id,
            incarnation: node.incarnation,
        };
        let result = self
            .consensus
            .install_node(
                crate::mvcc_bootstrap::cluster_id_hash(self.cluster_id()),
                installed,
                NodeId(raft_node_id),
                failure_domain,
            )
            .await
            .context("install cluster node")?;
        if let Some(replaced) = replaced {
            for (partition_id, assignment) in snapshot
                .partitions
                .iter()
                .filter(|(_, assignment)| assignment.owner == replaced)
            {
                self.consensus
                    .assign_partition(
                        crate::mvcc_bootstrap::cluster_id_hash(self.cluster_id()),
                        *partition_id,
                        installed,
                        assignment.epoch.saturating_add(1),
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "move partition {partition_id} to replacement node incarnation"
                        )
                    })?;
            }
        }
        Ok(result)
    }

    /// Fence and remove an incarnation after its Raft voter membership and
    /// partition ownership have been removed. An already absent incarnation is
    /// a successful retry.
    pub async fn remove_cluster_node(
        &self,
        authorization: &AuthorizedClusterControl,
        node: NodeIncarnation,
    ) -> Result<ControlApplyResult> {
        authorization.require(self.cluster_id(), ClusterControlPermission::Nodes)?;
        let node = ConsensusNodeIncarnation {
            node_id: consensus_control_node_id(&node.node_id),
            incarnation: node.incarnation,
        };
        let snapshot = self.consensus.applied_control_snapshot()?;
        if !snapshot
            .nodes
            .iter()
            .any(|(id, _raft_node_id, incarnation, _failure_domain)| {
                *id == node.node_id && *incarnation == node.incarnation
            })
        {
            return Ok(ControlApplyResult::NodeRemoved(node));
        }
        let remaining = snapshot
            .nodes
            .iter()
            .filter(|(node_id, _raft_node_id, incarnation, _failure_domain)| {
                *node_id != node.node_id || *incarnation != node.incarnation
            })
            .map(|(node_id, _raft_node_id, incarnation, _failure_domain)| {
                ConsensusNodeIncarnation {
                    node_id: *node_id,
                    incarnation: *incarnation,
                }
            })
            .collect::<Vec<_>>();
        for (partition_id, assignment) in snapshot
            .partitions
            .iter()
            .filter(|(_, assignment)| assignment.owner == node)
        {
            let replacement =
                crate::mvcc_assignment_reconciler::rendezvous_owner(*partition_id, &remaining)
                    .context("cannot remove the final node while it owns partitions")?;
            self.consensus
                .assign_partition(
                    crate::mvcc_bootstrap::cluster_id_hash(self.cluster_id()),
                    *partition_id,
                    replacement,
                    assignment.epoch.saturating_add(1),
                )
                .await
                .with_context(|| {
                    format!("reassign partition {partition_id} before node removal")
                })?;
        }
        self.consensus
            .remove_node(
                crate::mvcc_bootstrap::cluster_id_hash(self.cluster_id()),
                node,
            )
            .await
            .context("remove cluster node")
    }

    pub async fn set_cluster_durability_policy(
        &self,
        authorization: &AuthorizedClusterControl,
        generation: u64,
        bundle_quorum_holders: u16,
        tolerated_failure_domains: u16,
    ) -> Result<ControlApplyResult> {
        authorization.require(self.cluster_id(), ClusterControlPermission::Policies)?;
        let current = self.consensus.applied_control_snapshot()?.durability_policy;
        if current.generation == generation {
            if current.bundle_quorum_holders == bundle_quorum_holders
                && current.tolerated_failure_domains == tolerated_failure_domains
            {
                return Ok(ControlApplyResult::DurabilityPolicySet(current));
            }
            bail!("durability policy generation already names different values");
        }
        if generation < current.generation {
            bail!("durability policy generation cannot move backwards");
        }
        self.consensus
            .set_durability_policy(
                crate::mvcc_bootstrap::cluster_id_hash(self.cluster_id()),
                generation,
                bundle_quorum_holders,
                tolerated_failure_domains,
            )
            .await
            .context("set durability policy")
    }

    pub async fn advance_cluster_gc_watermark(
        &self,
        authorization: &AuthorizedClusterControl,
        watermark: CommitVersion,
    ) -> Result<ControlApplyResult> {
        authorization.require(self.cluster_id(), ClusterControlPermission::Partitions)?;
        let current = self.consensus.gc_safety_watermark()?;
        if watermark == current {
            return Ok(ControlApplyResult::GcWatermarkAdvanced(current));
        }
        if watermark < current {
            bail!("GC safety watermark cannot move backwards");
        }
        self.consensus
            .advance_gc_watermark(
                crate::mvcc_bootstrap::cluster_id_hash(self.cluster_id()),
                watermark,
            )
            .await
            .context("advance GC safety watermark")
    }

    /// Replace both the data-stream route and the OpenRaft node route. The
    /// latter is expressed as an idempotent learner metadata refresh and does
    /// not change the voter set.
    pub async fn replace_cluster_endpoint(
        &self,
        authorization: &AuthorizedClusterControl,
        raft_node_id: u64,
        node: &NodeIncarnation,
        endpoint: String,
    ) -> Result<()> {
        authorization.require(self.cluster_id(), ClusterControlPermission::Nodes)?;
        self.replication_client
            .replace_peer_incarnation(self.cluster_id(), node, endpoint.clone())
            .context("replace replication route and incarnation fence")?;
        self.consensus
            .add_learner(
                NodeId(raft_node_id),
                ConsensusNode { address: endpoint },
                true,
            )
            .await
            .context("replace OpenRaft route")
    }

    /// Replace only this coordinator's replication route after the cluster
    /// leader has committed the new incarnation. Operators invoke this on
    /// every surviving coordinator; it cannot mutate compact-Raft membership.
    pub fn replace_local_replication_route(
        &self,
        authorization: &AuthorizedClusterControl,
        node: &NodeIncarnation,
        endpoint: String,
    ) -> Result<()> {
        authorization.require(self.cluster_id(), ClusterControlPermission::Nodes)?;
        let installed = self
            .consensus
            .applied_control_snapshot()?
            .nodes
            .iter()
            .any(|(node_id, _raft_node_id, incarnation, _failure_domain)| {
                *node_id == consensus_control_node_id(&node.node_id)
                    && *incarnation == node.incarnation
            });
        if !installed {
            bail!("replacement incarnation is not committed in local Raft control state");
        }
        self.replication_client
            .replace_peer_incarnation(self.cluster_id(), node, endpoint)
            .context("replace local replication route and incarnation fence")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_capabilities_are_cluster_and_permission_scoped() {
        let nodes = AuthorizedClusterControl {
            cluster_id: "cluster-a".into(),
            permission: ClusterControlPermission::Nodes,
        };
        assert!(
            nodes
                .require("cluster-a", ClusterControlPermission::Nodes)
                .is_ok()
        );
        assert!(
            nodes
                .require("cluster-b", ClusterControlPermission::Nodes)
                .is_err()
        );
        assert!(
            nodes
                .require("cluster-a", ClusterControlPermission::Policies)
                .is_err()
        );
    }
}
