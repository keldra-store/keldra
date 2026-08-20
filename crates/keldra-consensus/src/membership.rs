//! Recovery-safe coordination between admitted descriptors and OpenRaft membership.
//!
//! The replicated descriptor transition remains the operation record. These
//! helpers only derive and apply the OpenRaft step which that one transition
//! currently permits; they add no second transition state or Raft payload.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    DecisionRaft, DecisionRaftError, FIXED_VOTER_TARGET, MembershipTransition,
    MembershipTransitionKind, NodeId, NodeState, PeerNode,
};

#[derive(Debug)]
struct MembershipView {
    voters: BTreeSet<NodeId>,
    learners: BTreeSet<NodeId>,
    addresses: BTreeMap<NodeId, String>,
    joint: bool,
}

impl DecisionRaft {
    /// Add the node named by the current ADD transition as a learner and wait
    /// until OpenRaft reports it caught up.
    ///
    /// Retrying this before activation is safe, including after restart. This
    /// method never changes voter membership and rejects an already-voting
    /// JOINING identity.
    pub async fn catch_up_joining_learner(
        &self,
        started_log_index: u64,
    ) -> Result<(), DecisionRaftError> {
        let state = self.state()?;
        let transition = require_transition(&state, started_log_index)?;
        if transition.kind != MembershipTransitionKind::Add {
            return Err(configuration(
                "learner catch-up requires the current ADD transition",
            ));
        }
        let descriptor = state
            .cluster_control()
            .nodes()
            .get(&transition.node_id)
            .ok_or_else(|| configuration("ADD transition node has no committed descriptor"))?;
        if descriptor.state != NodeState::Joining {
            return Err(configuration(
                "learner catch-up must finish before ADD activation commits",
            ));
        }

        let before = self.membership_view()?;
        if before.voters.contains(&transition.node_id) {
            return Err(configuration("a JOINING node must not be a Raft voter"));
        }
        if let Some(address) = before.addresses.get(&transition.node_id)
            && address != &descriptor.peer_address.0
        {
            return Err(configuration(
                "JOINING learner address differs from its committed descriptor",
            ));
        }

        self.add_learner(
            transition.node_id.0,
            PeerNode::new(descriptor.peer_address.0.clone()),
            true,
        )
        .await?;

        let after = self.membership_view()?;
        if after.voters.contains(&transition.node_id) {
            return Err(configuration("learner catch-up promoted a JOINING node"));
        }
        if !after.learners.contains(&transition.node_id) {
            return Err(configuration(
                "JOINING node was not committed as a Raft learner",
            ));
        }
        Ok(())
    }

    /// Apply the fixed voter rule for the current activated ADD or pending
    /// REMOVE transition.
    ///
    /// For ADD, the first `CompleteMembershipTransition` is the activation
    /// boundary: this method refuses to promote while the descriptor is still
    /// JOINING. The caller retains the transition until a second completion
    /// observes this exact membership. For REMOVE, the target is excluded and
    /// removed from OpenRaft membership before descriptor removal can commit.
    pub async fn apply_fixed_voters_for_transition(
        &self,
        started_log_index: u64,
    ) -> Result<BTreeSet<NodeId>, DecisionRaftError> {
        let state = self.state()?;
        let transition = require_transition(&state, started_log_index)?;
        let cluster = state.cluster_control();
        let excluded = match transition.kind {
            MembershipTransitionKind::Add => {
                let descriptor = cluster.nodes().get(&transition.node_id).ok_or_else(|| {
                    configuration("ADD transition node has no committed descriptor")
                })?;
                if descriptor.state != NodeState::Active {
                    return Err(configuration(
                        "ADD activation must commit before voter membership changes",
                    ));
                }
                None
            }
            MembershipTransitionKind::Remove => Some(transition.node_id),
            MembershipTransitionKind::Reweight => {
                return Err(configuration(
                    "REWEIGHT does not change fixed Raft voter membership",
                ));
            }
        };

        let before = self.membership_view()?;
        let eligible = cluster
            .nodes()
            .iter()
            .filter(|(node_id, descriptor)| {
                descriptor.state == NodeState::Active && Some(**node_id) != excluded
            })
            .map(|(node_id, descriptor)| (*node_id, descriptor))
            .collect::<BTreeMap<_, _>>();
        let target = eligible.len().min(FIXED_VOTER_TARGET);
        if target == 0 {
            return Err(configuration("fixed voter membership must not be empty"));
        }
        for (node_id, descriptor) in &eligible {
            match before.addresses.get(node_id) {
                Some(address) if address == &descriptor.peer_address.0 => {}
                Some(_) => {
                    return Err(configuration(
                        "Raft member address differs from its committed descriptor",
                    ));
                }
                None => {
                    return Err(configuration(
                        "an ACTIVE voter candidate is absent from Raft membership",
                    ));
                }
            }
        }

        // Keep every eligible voter. A normal transition can only create a
        // vacancy; it never rotates healthy voters merely because another
        // learner has a lower ID.
        let eligible_ids = eligible.keys().copied().collect::<BTreeSet<_>>();
        let mut desired = before
            .voters
            .intersection(&eligible_ids)
            .copied()
            .collect::<BTreeSet<_>>();
        if desired.len() > target {
            return Err(configuration(
                "committed membership has more eligible voters than the fixed target",
            ));
        }
        for node_id in eligible.keys() {
            if desired.len() == target {
                break;
            }
            desired.insert(*node_id);
        }
        if desired.len() != target {
            return Err(configuration(
                "not enough ACTIVE Raft members to satisfy the fixed voter target",
            ));
        }

        let desired_raw = desired.iter().map(|node_id| node_id.0).collect();
        if before.voters != desired || before.joint {
            self.change_membership(desired_raw, false).await?;
        }

        if let Some(node_id) = excluded
            && self.membership_view()?.addresses.contains_key(&node_id)
        {
            self.remove_learner(node_id.0).await?;
        }

        let after = self.membership_view()?;
        if after.voters != desired || after.joint {
            return Err(configuration(
                "OpenRaft did not commit the exact fixed voter membership",
            ));
        }
        if excluded.is_some_and(|node_id| after.addresses.contains_key(&node_id)) {
            return Err(configuration(
                "REMOVE target remains in committed Raft membership",
            ));
        }
        Ok(desired)
    }

    /// Current committed OpenRaft voters, exposed without duplicating their
    /// role in Keldra's descriptor state.
    pub fn committed_voter_ids(&self) -> Result<BTreeSet<NodeId>, DecisionRaftError> {
        Ok(self.membership_view()?.voters)
    }

    /// Current committed OpenRaft learners, derived from OpenRaft membership.
    pub fn committed_learner_ids(&self) -> Result<BTreeSet<NodeId>, DecisionRaftError> {
        Ok(self.membership_view()?.learners)
    }

    fn membership_view(&self) -> Result<MembershipView, DecisionRaftError> {
        let stored = self.committed_membership()?;
        let membership = stored.membership();
        Ok(MembershipView {
            voters: membership.voter_ids().map(NodeId).collect(),
            learners: membership.learner_ids().map(NodeId).collect(),
            addresses: membership
                .nodes()
                .map(|(node_id, node)| (NodeId(*node_id), node.addr.clone()))
                .collect(),
            joint: membership.get_joint_config().len() > 1,
        })
    }
}

fn require_transition(
    state: &crate::StateMachine,
    started_log_index: u64,
) -> Result<MembershipTransition, DecisionRaftError> {
    let transition = state
        .cluster_control()
        .transition()
        .cloned()
        .ok_or_else(|| configuration("there is no membership transition"))?;
    if transition.started_log_index != started_log_index {
        return Err(configuration(
            "membership transition log identity does not match",
        ));
    }
    Ok(transition)
}

fn configuration(message: &str) -> DecisionRaftError {
    DecisionRaftError::Configuration(message.into())
}
