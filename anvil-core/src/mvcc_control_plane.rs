//! Authorized operational entrypoints for the cluster transaction group.
//!
//! These operations deliberately do not use `OpenTransactionRegistry`: mesh and
//! region topology is outside the cluster transaction group.  A capability can
//! only be minted after a Zanzibar check against the cluster-wide system
//! object, and is permanently bound to one cluster identifier.

use std::collections::BTreeSet;

use anvil_mvcc_consensus::{
    AppliedControlSnapshot, CommitVersion, ConsensusNode, ControlApplyResult, NodeId,
    NodeIncarnation as ConsensusNodeIncarnation, NodeReplacementTransition,
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

/// v0.4.0 supports only the fixed voter set supplied at initial cluster
/// bootstrap. Clean-disk replacement and runtime membership changes remain
/// unavailable until their recovery protocol is proven end to end.
pub const DYNAMIC_CLUSTER_MEMBERSHIP_ENABLED: bool = false;

fn require_dynamic_cluster_membership() -> Result<()> {
    if !DYNAMIC_CLUSTER_MEMBERSHIP_ENABLED {
        bail!("dynamic cluster membership is not supported in Anvil v0.4.0");
    }
    Ok(())
}

fn stale_replacement_partitions(
    snapshot: &AppliedControlSnapshot,
    installed: ConsensusNodeIncarnation,
) -> Vec<(u64, anvil_mvcc_consensus::PartitionAssignment)> {
    snapshot
        .partitions
        .iter()
        .filter(|(_, assignment)| {
            assignment.owner.node_id == installed.node_id && assignment.owner != installed
        })
        .cloned()
        .collect()
}

fn authoritative_replacement_transition(
    snapshot: &AppliedControlSnapshot,
    node_id: NodeId,
    replaced_raft_node_id: NodeId,
    replacement_raft_node_id: NodeId,
    replacement_incarnation: u64,
) -> Result<NodeReplacementTransition> {
    let transition = snapshot
        .node_replacements
        .iter()
        .find(|transition| transition.node_id == node_id)
        .copied()
        .context("replacement identity transition is not installed in Raft control state")?;
    if transition.replaced_raft_node_id != replaced_raft_node_id
        || transition.replacement_raft_node_id != replacement_raft_node_id
        || transition.replacement_incarnation != replacement_incarnation
    {
        bail!("replacement request does not match the Raft-authoritative identity transition");
    }
    if !snapshot
        .retired_raft_node_ids
        .contains(&transition.replaced_raft_node_id)
    {
        bail!("Raft-authoritative replacement did not retire its obsolete identity");
    }
    if snapshot
        .nodes
        .iter()
        .any(|(installed_node_id, raft_node_id, _, _)| {
            *installed_node_id != node_id && *raft_node_id == transition.replaced_raft_node_id
        })
    {
        bail!("obsolete Raft node ID is now bound to another logical node");
    }
    Ok(transition)
}

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
        require_dynamic_cluster_membership()?;
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
        require_dynamic_cluster_membership()?;
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
        require_dynamic_cluster_membership()?;
        let control_node_id = consensus_control_node_id(&node.node_id);
        let snapshot = self.consensus.applied_control_snapshot()?;
        let installed = ConsensusNodeIncarnation {
            node_id: control_node_id,
            incarnation: node.incarnation,
        };
        let exact_retry =
            snapshot
                .nodes
                .iter()
                .any(|(id, installed_raft_id, incarnation, domain)| {
                    *id == control_node_id
                        && installed_raft_id.0 == raft_node_id
                        && *incarnation == node.incarnation
                        && *domain == failure_domain
                });
        let result = if exact_retry {
            ControlApplyResult::NodeInstalled(installed)
        } else {
            self.consensus
                .install_node(
                    crate::mvcc_bootstrap::cluster_id_hash(self.cluster_id()),
                    installed,
                    NodeId(raft_node_id),
                    failure_domain,
                )
                .await
                .context("install cluster node")?
        };
        for (partition_id, assignment) in stale_replacement_partitions(&snapshot, installed) {
            self.consensus
                .assign_partition(
                    crate::mvcc_bootstrap::cluster_id_hash(self.cluster_id()),
                    partition_id,
                    installed,
                    assignment.epoch.saturating_add(1),
                )
                .await
                .with_context(|| {
                    format!("move partition {partition_id} to replacement node incarnation")
                })?;
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
        require_dynamic_cluster_membership()?;
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
        if !crate::mvcc_gc::MVCC_GARBAGE_COLLECTION_ENABLED {
            bail!("MVCC garbage collection is disabled in Anvil v0.4.0");
        }
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

    /// Fence a clean-disk replacement, remove the obsolete voter identity,
    /// install the fresh identity as a caught-up learner, and stop before
    /// promotion so operators can refresh every survivor's local routes.
    pub async fn prepare_cluster_node_replacement(
        &self,
        authorization: &AuthorizedClusterControl,
        replaced_raft_node_id: u64,
        raft_node_id: u64,
        node: &NodeIncarnation,
        failure_domain: &str,
        endpoint: String,
    ) -> Result<()> {
        authorization.require(self.cluster_id(), ClusterControlPermission::Nodes)?;
        require_dynamic_cluster_membership()?;
        if replaced_raft_node_id == 0 || raft_node_id == 0 || replaced_raft_node_id == raft_node_id
        {
            bail!("clean-disk replacement requires distinct non-zero old and new Raft node IDs");
        }
        if self.local_node.node_id == node.node_id {
            bail!("the current leader cannot replace its own clean-disk incarnation");
        }
        if !self.consensus.is_leader() {
            bail!("replacement preparation must execute on the current cluster leader");
        }
        let _membership = self.membership_change_guard().await;
        self.consensus
            .linearized_read_barrier_locally()
            .await
            .context("confirm local leadership for replacement preparation")?;
        let snapshot = self.consensus.applied_control_snapshot()?;
        let control_node_id = consensus_control_node_id(&node.node_id);
        let installed = snapshot
            .nodes
            .iter()
            .find(|(node_id, _, _, _)| *node_id == control_node_id)
            .context("replacement logical node is not installed in cluster control")?;
        let replacement_is_installed = installed.1.0 == raft_node_id
            && installed.2 == node.incarnation
            && installed.3 == failure_domain;
        if replacement_is_installed {
            authoritative_replacement_transition(
                &snapshot,
                control_node_id,
                NodeId(replaced_raft_node_id),
                NodeId(raft_node_id),
                node.incarnation,
            )?;
        } else {
            if installed.1.0 != replaced_raft_node_id {
                bail!("replacement old Raft node ID does not match cluster control");
            }
            if node.incarnation <= installed.2 {
                bail!("replacement incarnation must advance");
            }
            if snapshot
                .nodes
                .iter()
                .any(|(other_node_id, installed_raft_node_id, _, _)| {
                    *other_node_id != control_node_id && installed_raft_node_id.0 == raft_node_id
                })
            {
                bail!("replacement Raft node ID is already bound to another logical node");
            }
            if snapshot
                .retired_raft_node_ids
                .contains(&NodeId(raft_node_id))
            {
                bail!("retired Raft node IDs cannot be reused for a replacement");
            }
            if self
                .consensus
                .applied_member_ids()?
                .contains(&NodeId(raft_node_id))
            {
                bail!("fresh replacement Raft node ID is already a voter or learner");
            }
        }
        self.install_cluster_node(
            authorization,
            raft_node_id,
            node.clone(),
            failure_domain.to_string(),
        )
        .await?;

        let installed_snapshot = self.consensus.applied_control_snapshot()?;
        let transition = authoritative_replacement_transition(
            &installed_snapshot,
            control_node_id,
            NodeId(replaced_raft_node_id),
            NodeId(raft_node_id),
            node.incarnation,
        )?;
        let authoritative_old_id = transition.replaced_raft_node_id;
        let authoritative_new_id = transition.replacement_raft_node_id;
        let mut voters = self.consensus.applied_voter_ids()?;
        if voters.contains(&authoritative_old_id) && voters.contains(&authoritative_new_id) {
            bail!("replacement and obsolete Raft voter IDs are both active");
        }
        if voters.contains(&authoritative_new_id) {
            self.replace_runtime_peer_projection(
                authoritative_old_id.0,
                authoritative_new_id.0,
                node,
                failure_domain,
                &endpoint,
            )
            .context("refresh completed replacement runtime routes")?;
            return Ok(());
        }

        if voters.remove(&authoritative_old_id) {
            if voters.is_empty() {
                bail!("cannot replace the cluster's final Raft voter");
            }
            self.consensus
                .change_membership(voters, false)
                .await
                .context("remove obsolete Raft voter before learner installation")?;
        }
        self.replace_runtime_peer_projection(
            authoritative_old_id.0,
            authoritative_new_id.0,
            node,
            failure_domain,
            &endpoint,
        )
        .context("replace runtime routes and incarnation fence")?;

        let checkpoint = self
            .runtime
            .local_store()
            .export_checkpoint()
            .context("capture replacement MVCC checkpoint")?;
        let consensus_gc = self.consensus.gc_safety_watermark()?;
        if checkpoint.decision_watermark < consensus_gc.0 {
            bail!(
                "replacement MVCC checkpoint watermark {} is below consensus GC watermark {}",
                checkpoint.decision_watermark,
                consensus_gc.0
            );
        }
        let checkpoint_watermark = checkpoint.decision_watermark;
        let checkpoint_bytes = checkpoint.encode().context("encode MVCC checkpoint")?;
        self.replication_client
            .send_mvcc_checkpoint(node, checkpoint_watermark, &checkpoint_bytes)
            .await
            .context("transfer and install replacement MVCC checkpoint")?;

        self.consensus
            .add_learner(
                authoritative_new_id,
                ConsensusNode { address: endpoint },
                true,
            )
            .await
            .context("install fresh replacement as an OpenRaft learner")?;
        let delta_barrier = self
            .consensus
            .linearized_read_barrier_locally()
            .await
            .context("capture post-checkpoint replacement delta barrier")?
            .0;
        self.wait_for_node_applied(authoritative_new_id, node.incarnation, delta_barrier)
            .await
            .context("wait for replacement MVCC checkpoint delta catch-up")
    }

    /// Promote a prepared replacement only after the caller has refreshed the
    /// local route projection on every surviving voter.
    pub async fn promote_cluster_node_replacement(
        &self,
        authorization: &AuthorizedClusterControl,
        replaced_raft_node_id: u64,
        raft_node_id: u64,
        node: &NodeIncarnation,
        failure_domain: &str,
        endpoint: String,
    ) -> Result<()> {
        authorization.require(self.cluster_id(), ClusterControlPermission::Nodes)?;
        require_dynamic_cluster_membership()?;
        if replaced_raft_node_id == 0 || raft_node_id == 0 || replaced_raft_node_id == raft_node_id
        {
            bail!("replacement promotion requires distinct non-zero old and new Raft node IDs");
        }
        if !self.consensus.is_leader() {
            bail!("replacement promotion must execute on the current cluster leader");
        }
        let _membership = self.membership_change_guard().await;
        self.consensus
            .linearized_read_barrier_locally()
            .await
            .context("confirm local leadership for replacement promotion")?;
        let snapshot = self.consensus.applied_control_snapshot()?;
        let control_node_id = consensus_control_node_id(&node.node_id);
        let installed = snapshot.nodes.iter().any(
            |(node_id, installed_raft_node_id, incarnation, installed_failure_domain)| {
                *node_id == control_node_id
                    && installed_raft_node_id.0 == raft_node_id
                    && *incarnation == node.incarnation
                    && installed_failure_domain == failure_domain
            },
        );
        if !installed {
            bail!("replacement must be prepared before voter promotion");
        }
        let transition = authoritative_replacement_transition(
            &snapshot,
            control_node_id,
            NodeId(replaced_raft_node_id),
            NodeId(raft_node_id),
            node.incarnation,
        )?;
        let authoritative_old_id = transition.replaced_raft_node_id;
        let authoritative_new_id = transition.replacement_raft_node_id;
        let mut voters = self.consensus.applied_voter_ids()?;
        if voters.contains(&authoritative_old_id) {
            bail!("obsolete Raft voter is still present during replacement promotion");
        }
        self.replace_runtime_peer_projection(
            authoritative_old_id.0,
            authoritative_new_id.0,
            node,
            failure_domain,
            &endpoint,
        )
        .context("refresh leader replacement runtime routes before promotion")?;
        if voters.contains(&authoritative_new_id) {
            return Ok(());
        }
        self.consensus
            .add_learner(
                authoritative_new_id,
                ConsensusNode { address: endpoint },
                true,
            )
            .await
            .context("confirm replacement learner catch-up before promotion")?;
        let delta_barrier = self
            .consensus
            .linearized_read_barrier_locally()
            .await
            .context("capture replacement promotion delta barrier")?
            .0;
        self.wait_for_node_applied(authoritative_new_id, node.incarnation, delta_barrier)
            .await
            .context("confirm replacement product MVCC catch-up before promotion")?;
        voters.insert(authoritative_new_id);
        self.consensus
            .change_membership(voters, false)
            .await
            .context("promote fresh replacement Raft voter")
    }

    /// Replace only this coordinator's replication route after the cluster
    /// leader has committed the new incarnation. Operators invoke this on
    /// every surviving coordinator; it cannot mutate compact-Raft membership.
    pub fn replace_local_replication_route(
        &self,
        authorization: &AuthorizedClusterControl,
        replaced_raft_node_id: u64,
        raft_node_id: u64,
        node: &NodeIncarnation,
        failure_domain: &str,
        endpoint: String,
    ) -> Result<()> {
        authorization.require(self.cluster_id(), ClusterControlPermission::Nodes)?;
        require_dynamic_cluster_membership()?;
        let snapshot = self.consensus.applied_control_snapshot()?;
        let control_node_id = consensus_control_node_id(&node.node_id);
        let installed = snapshot.nodes.iter().any(
            |(node_id, installed_raft_node_id, incarnation, installed_failure_domain)| {
                *node_id == control_node_id
                    && installed_raft_node_id.0 == raft_node_id
                    && *incarnation == node.incarnation
                    && installed_failure_domain == failure_domain
            },
        );
        if !installed {
            bail!("replacement incarnation is not committed in local Raft control state");
        }
        let transition = authoritative_replacement_transition(
            &snapshot,
            control_node_id,
            NodeId(replaced_raft_node_id),
            NodeId(raft_node_id),
            node.incarnation,
        )?;
        self.replace_runtime_peer_projection(
            transition.replaced_raft_node_id.0,
            transition.replacement_raft_node_id.0,
            node,
            failure_domain,
            &endpoint,
        )
        .context("replace local runtime routes and incarnation fence")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anvil_mvcc_consensus::{ConsensusDurabilityPolicy, PartitionAssignment};

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

    #[test]
    fn exact_install_retry_still_reconciles_stale_partition_incarnations() {
        let logical_node = NodeId(7);
        let installed = ConsensusNodeIncarnation {
            node_id: logical_node,
            incarnation: 2,
        };
        let snapshot = AppliedControlSnapshot {
            topology_epoch: 3,
            nodes: vec![(logical_node, NodeId(11), 2, "zone-a".into())],
            retired_raft_node_ids: BTreeSet::new(),
            node_replacements: Vec::new(),
            partitions: vec![
                (
                    1,
                    PartitionAssignment {
                        owner: ConsensusNodeIncarnation {
                            node_id: logical_node,
                            incarnation: 1,
                        },
                        epoch: 4,
                    },
                ),
                (
                    2,
                    PartitionAssignment {
                        owner: installed,
                        epoch: 5,
                    },
                ),
            ],
            durability_policy: ConsensusDurabilityPolicy::default(),
        };

        assert_eq!(
            stale_replacement_partitions(&snapshot, installed),
            vec![(
                1,
                PartitionAssignment {
                    owner: ConsensusNodeIncarnation {
                        node_id: logical_node,
                        incarnation: 1,
                    },
                    epoch: 4,
                },
            )]
        );
    }
}
