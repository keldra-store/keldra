//! Bounded cluster admission and membership-transition state.
//!
//! Raft retains only node descriptors, one small transition, used node IDs,
//! and the JWT-key fingerprint. Data movement and progress remain outside it.

use crate::{
    ApplyError, ApplyResult, CLUSTER_CONTROL_COMMAND_VERSION, CapabilityRange, ErasureCodeProfile,
    JoinCapabilityHash, JwtSigningKeyFingerprint, MAX_PEER_ADDRESS_BYTES, MembershipTransition,
    MembershipTransitionKind, NodeDescriptor, NodeId, NodeState, PeerAddress, PeerSpkiSha256,
    StateMachine, types::MAX_RAFT_NODE_ID,
};

impl StateMachine {
    pub(crate) fn begin_add_node(
        &mut self,
        format_version: u16,
        descriptor: NodeDescriptor,
        committed_log_index: u64,
    ) -> Result<ApplyResult, ApplyError> {
        self.require_cluster_control(format_version)?;
        validate_descriptor(&descriptor)?;
        if descriptor.state != NodeState::Joining {
            return Err(ApplyError::AddedNodeMustBeJoining);
        }
        if let Some(existing) = self.cluster_control.transition.as_ref() {
            if existing.kind == MembershipTransitionKind::Add
                && existing.node_id == descriptor.node_id
                && existing.target_weight_millionths == Some(descriptor.storage_weight_millionths)
                && self.cluster_control.nodes.get(&descriptor.node_id) == Some(&descriptor)
            {
                return Ok(ApplyResult::MembershipTransitionBegun(existing.clone()));
            }
            return Err(ApplyError::MembershipTransitionInProgress {
                started_log_index: existing.started_log_index,
            });
        }
        if self
            .cluster_control
            .used_node_ids
            .contains(descriptor.node_id)
        {
            return Err(ApplyError::NodeIdAlreadyUsed {
                node_id: descriptor.node_id,
            });
        }
        ensure_descriptor_unique(&self.cluster_control.nodes, &descriptor)?;

        let transition = MembershipTransition {
            kind: MembershipTransitionKind::Add,
            node_id: descriptor.node_id,
            started_log_index: committed_log_index,
            target_weight_millionths: Some(descriptor.storage_weight_millionths),
        };
        self.cluster_control
            .used_node_ids
            .insert(descriptor.node_id);
        self.cluster_control
            .nodes
            .insert(descriptor.node_id, descriptor);
        self.cluster_control.transition = Some(transition.clone());
        Ok(ApplyResult::MembershipTransitionBegun(transition))
    }

    pub(crate) fn begin_remove_node(
        &mut self,
        format_version: u16,
        node_id: NodeId,
        committed_log_index: u64,
    ) -> Result<ApplyResult, ApplyError> {
        self.require_cluster_control(format_version)?;
        validate_node_id(node_id)?;
        if let Some(existing) = self.cluster_control.transition.as_ref() {
            if existing.kind == MembershipTransitionKind::Remove && existing.node_id == node_id {
                return Ok(ApplyResult::MembershipTransitionBegun(existing.clone()));
            }
            return Err(ApplyError::MembershipTransitionInProgress {
                started_log_index: existing.started_log_index,
            });
        }
        let descriptor = self
            .cluster_control
            .nodes
            .get(&node_id)
            .ok_or(ApplyError::NodeNotAdmitted { node_id })?;
        if descriptor.state != NodeState::Active {
            return Err(ApplyError::NodeNotActive { node_id });
        }
        if self.cluster_control.active_node_count() == 1 {
            return Err(ApplyError::CannotRemoveLastActiveNode);
        }
        let transition = MembershipTransition {
            kind: MembershipTransitionKind::Remove,
            node_id,
            started_log_index: committed_log_index,
            target_weight_millionths: None,
        };
        self.cluster_control.transition = Some(transition.clone());
        Ok(ApplyResult::MembershipTransitionBegun(transition))
    }

    pub(crate) fn begin_reweight_node(
        &mut self,
        format_version: u16,
        node_id: NodeId,
        storage_weight_millionths: u32,
        committed_log_index: u64,
    ) -> Result<ApplyResult, ApplyError> {
        self.require_cluster_control(format_version)?;
        validate_node_id(node_id)?;
        if storage_weight_millionths == 0 {
            return Err(ApplyError::InvalidStorageWeight);
        }
        if let Some(existing) = self.cluster_control.transition.as_ref() {
            if existing.kind == MembershipTransitionKind::Reweight
                && existing.node_id == node_id
                && existing.target_weight_millionths == Some(storage_weight_millionths)
            {
                return Ok(ApplyResult::MembershipTransitionBegun(existing.clone()));
            }
            return Err(ApplyError::MembershipTransitionInProgress {
                started_log_index: existing.started_log_index,
            });
        }
        let descriptor = self
            .cluster_control
            .nodes
            .get(&node_id)
            .ok_or(ApplyError::NodeNotAdmitted { node_id })?;
        if descriptor.state != NodeState::Active {
            return Err(ApplyError::NodeNotActive { node_id });
        }
        if descriptor.storage_weight_millionths == storage_weight_millionths {
            return Err(ApplyError::StorageWeightUnchanged);
        }
        let transition = MembershipTransition {
            kind: MembershipTransitionKind::Reweight,
            node_id,
            started_log_index: committed_log_index,
            target_weight_millionths: Some(storage_weight_millionths),
        };
        self.cluster_control.transition = Some(transition.clone());
        Ok(ApplyResult::MembershipTransitionBegun(transition))
    }

    pub(crate) fn complete_membership_transition(
        &mut self,
        format_version: u16,
        started_log_index: u64,
    ) -> Result<ApplyResult, ApplyError> {
        self.require_cluster_control(format_version)?;
        let transition = self
            .cluster_control
            .transition
            .clone()
            .ok_or(ApplyError::NoMembershipTransition)?;
        if transition.started_log_index != started_log_index {
            return Err(ApplyError::MembershipTransitionFenceMismatch {
                expected: transition.started_log_index,
                requested: started_log_index,
            });
        }
        let finished = match transition.kind {
            MembershipTransitionKind::Add => {
                let descriptor = self
                    .cluster_control
                    .nodes
                    .get_mut(&transition.node_id)
                    .ok_or(ApplyError::MembershipTransitionStateMismatch)?;
                if transition.target_weight_millionths != Some(descriptor.storage_weight_millionths)
                {
                    return Err(ApplyError::MembershipTransitionStateMismatch);
                }
                match descriptor.state {
                    NodeState::Joining => {
                        descriptor.state = NodeState::Active;
                        descriptor.join_capability_hash = None;
                        false
                    }
                    NodeState::Active => true,
                }
            }
            MembershipTransitionKind::Remove => {
                let descriptor = self
                    .cluster_control
                    .nodes
                    .get(&transition.node_id)
                    .ok_or(ApplyError::MembershipTransitionStateMismatch)?;
                if descriptor.state != NodeState::Active
                    || transition.target_weight_millionths.is_some()
                {
                    return Err(ApplyError::MembershipTransitionStateMismatch);
                }
                self.cluster_control.nodes.remove(&transition.node_id);
                true
            }
            MembershipTransitionKind::Reweight => {
                let target_weight = transition
                    .target_weight_millionths
                    .ok_or(ApplyError::MembershipTransitionStateMismatch)?;
                let descriptor = self
                    .cluster_control
                    .nodes
                    .get_mut(&transition.node_id)
                    .ok_or(ApplyError::MembershipTransitionStateMismatch)?;
                if descriptor.state != NodeState::Active {
                    return Err(ApplyError::MembershipTransitionStateMismatch);
                }
                descriptor.storage_weight_millionths = target_weight;
                true
            }
        };
        if finished {
            self.cluster_control.transition = None;
            Ok(ApplyResult::MembershipTransitionFinished { started_log_index })
        } else {
            Ok(ApplyResult::MembershipTransitionAdvanced(transition))
        }
    }

    pub(crate) fn stage_peer_spki_overlap(
        &mut self,
        format_version: u16,
        node_id: NodeId,
        expected_current: PeerSpkiSha256,
        overlap: PeerSpkiSha256,
    ) -> Result<ApplyResult, ApplyError> {
        self.require_cluster_control(format_version)?;
        validate_pin(expected_current)?;
        validate_pin(overlap)?;
        if expected_current == overlap {
            return Err(ApplyError::PeerPinsMustDiffer);
        }
        self.require_node_not_transitioning(node_id)?;
        ensure_pin_unique(&self.cluster_control.nodes, node_id, overlap)?;
        let descriptor = self.active_descriptor_mut(node_id)?;
        if descriptor.current_peer_spki_sha256 != expected_current {
            return Err(ApplyError::PeerCurrentPinMismatch { node_id });
        }
        match descriptor.overlap_peer_spki_sha256 {
            None => descriptor.overlap_peer_spki_sha256 = Some(overlap),
            Some(current) if current == overlap => {}
            Some(_) => return Err(ApplyError::PeerOverlapAlreadySet { node_id }),
        }
        Ok(ApplyResult::PeerSpkiChanged(descriptor.clone()))
    }

    pub(crate) fn promote_peer_spki_overlap(
        &mut self,
        format_version: u16,
        node_id: NodeId,
        expected_current: PeerSpkiSha256,
        expected_overlap: PeerSpkiSha256,
    ) -> Result<ApplyResult, ApplyError> {
        self.require_cluster_control(format_version)?;
        validate_pin(expected_current)?;
        validate_pin(expected_overlap)?;
        self.require_node_not_transitioning(node_id)?;
        let descriptor = self.active_descriptor_mut(node_id)?;
        if descriptor.current_peer_spki_sha256 == expected_overlap
            && descriptor.overlap_peer_spki_sha256 == Some(expected_current)
        {
            return Ok(ApplyResult::PeerSpkiChanged(descriptor.clone()));
        }
        if descriptor.current_peer_spki_sha256 != expected_current
            || descriptor.overlap_peer_spki_sha256 != Some(expected_overlap)
        {
            return Err(ApplyError::PeerPinPairMismatch { node_id });
        }
        descriptor.current_peer_spki_sha256 = expected_overlap;
        descriptor.overlap_peer_spki_sha256 = Some(expected_current);
        Ok(ApplyResult::PeerSpkiChanged(descriptor.clone()))
    }

    pub(crate) fn clear_peer_spki_overlap(
        &mut self,
        format_version: u16,
        node_id: NodeId,
        expected_current: PeerSpkiSha256,
        expected_overlap: PeerSpkiSha256,
    ) -> Result<ApplyResult, ApplyError> {
        self.require_cluster_control(format_version)?;
        validate_pin(expected_current)?;
        validate_pin(expected_overlap)?;
        self.require_node_not_transitioning(node_id)?;
        let descriptor = self.active_descriptor_mut(node_id)?;
        if descriptor.current_peer_spki_sha256 != expected_current {
            return Err(ApplyError::PeerCurrentPinMismatch { node_id });
        }
        match descriptor.overlap_peer_spki_sha256 {
            Some(overlap) if overlap == expected_overlap => {
                descriptor.overlap_peer_spki_sha256 = None;
            }
            None => {}
            Some(_) => return Err(ApplyError::PeerPinPairMismatch { node_id }),
        }
        Ok(ApplyResult::PeerSpkiChanged(descriptor.clone()))
    }

    pub(crate) fn bind_jwt_signing_key_fingerprint(
        &mut self,
        format_version: u16,
        fingerprint: JwtSigningKeyFingerprint,
    ) -> Result<ApplyResult, ApplyError> {
        self.require_cluster_control(format_version)?;
        if fingerprint.0 == [0; 32] {
            return Err(ApplyError::InvalidJwtSigningKeyFingerprint);
        }
        match self.cluster_control.jwt_signing_key_fingerprint {
            None => self.cluster_control.jwt_signing_key_fingerprint = Some(fingerprint),
            Some(current) if current == fingerprint => {}
            Some(current) => {
                return Err(ApplyError::JwtSigningKeyFingerprintConflict {
                    current,
                    requested: fingerprint,
                });
            }
        }
        Ok(ApplyResult::JwtSigningKeyFingerprintBound(fingerprint))
    }

    pub(crate) fn bind_erasure_code_profile(
        &mut self,
        format_version: u16,
        profile: ErasureCodeProfile,
    ) -> Result<ApplyResult, ApplyError> {
        self.require_cluster_control(format_version)?;
        let total_shards = profile
            .data_shards
            .checked_add(profile.parity_shards)
            .ok_or(ApplyError::InvalidErasureCodeProfile)?;
        if profile.data_shards == 0
            || profile.parity_shards == 0
            || profile.stripe_unit == 0
            || total_shards > 256
        {
            return Err(ApplyError::InvalidErasureCodeProfile);
        }
        match self.cluster_control.erasure_code_profile {
            None => self.cluster_control.erasure_code_profile = Some(profile),
            Some(current) if current == profile => {}
            Some(current) => {
                return Err(ApplyError::ErasureCodeProfileConflict {
                    current,
                    requested: profile,
                });
            }
        }
        Ok(ApplyResult::ErasureCodeProfileBound(profile))
    }

    fn require_cluster_control(&self, format_version: u16) -> Result<(), ApplyError> {
        if format_version != CLUSTER_CONTROL_COMMAND_VERSION {
            return Err(ApplyError::UnsupportedClusterControlVersion {
                requested: format_version,
            });
        }
        if self.cluster_id.is_none() {
            return Err(ApplyError::ClusterNotInitialized);
        }
        Ok(())
    }

    fn require_node_not_transitioning(&self, node_id: NodeId) -> Result<(), ApplyError> {
        if self
            .cluster_control
            .transition
            .as_ref()
            .is_some_and(|transition| transition.node_id == node_id)
        {
            return Err(ApplyError::NodeMembershipTransitionInProgress { node_id });
        }
        Ok(())
    }

    fn active_descriptor_mut(
        &mut self,
        node_id: NodeId,
    ) -> Result<&mut NodeDescriptor, ApplyError> {
        validate_node_id(node_id)?;
        let descriptor = self
            .cluster_control
            .nodes
            .get_mut(&node_id)
            .ok_or(ApplyError::NodeNotAdmitted { node_id })?;
        if descriptor.state != NodeState::Active {
            return Err(ApplyError::NodeNotActive { node_id });
        }
        Ok(descriptor)
    }
}

fn validate_descriptor(descriptor: &NodeDescriptor) -> Result<(), ApplyError> {
    validate_node_id(descriptor.node_id)?;
    validate_peer_address(&descriptor.peer_address)?;
    if descriptor.storage_weight_millionths == 0 {
        return Err(ApplyError::InvalidStorageWeight);
    }
    validate_pin(descriptor.current_peer_spki_sha256)?;
    if let Some(overlap) = descriptor.overlap_peer_spki_sha256 {
        validate_pin(overlap)?;
        if overlap == descriptor.current_peer_spki_sha256 {
            return Err(ApplyError::PeerPinsMustDiffer);
        }
    }
    validate_capability_range(descriptor.supported_protocol)?;
    validate_capability_range(descriptor.supported_storage_format)?;
    match descriptor.state {
        NodeState::Joining => {
            let capability = descriptor
                .join_capability_hash
                .ok_or(ApplyError::JoiningNodeRequiresCapability)?;
            validate_join_capability(capability)?;
            if descriptor.overlap_peer_spki_sha256.is_some() {
                return Err(ApplyError::JoiningNodeCannotRotatePeerPin);
            }
        }
        NodeState::Active if descriptor.join_capability_hash.is_some() => {
            return Err(ApplyError::ActiveNodeRetainsJoinCapability);
        }
        NodeState::Active => {}
    }
    Ok(())
}

fn validate_node_id(node_id: NodeId) -> Result<(), ApplyError> {
    if !(1..=MAX_RAFT_NODE_ID).contains(&node_id.0) {
        return Err(ApplyError::InvalidNodeId);
    }
    Ok(())
}

fn validate_capability_range(range: CapabilityRange) -> Result<(), ApplyError> {
    if range.min == 0 || range.min > range.max {
        return Err(ApplyError::InvalidCapabilityRange {
            min: range.min,
            max: range.max,
        });
    }
    Ok(())
}

fn validate_pin(pin: PeerSpkiSha256) -> Result<(), ApplyError> {
    if pin.0 == [0; 32] {
        return Err(ApplyError::InvalidPeerSpki);
    }
    Ok(())
}

fn validate_join_capability(capability: JoinCapabilityHash) -> Result<(), ApplyError> {
    if capability.0 == [0; 32] {
        return Err(ApplyError::InvalidJoinCapabilityHash);
    }
    Ok(())
}

fn ensure_descriptor_unique(
    nodes: &std::collections::BTreeMap<NodeId, NodeDescriptor>,
    candidate: &NodeDescriptor,
) -> Result<(), ApplyError> {
    for descriptor in nodes.values() {
        if descriptor.peer_address == candidate.peer_address {
            return Err(ApplyError::PeerAddressAlreadyUsed);
        }
        if descriptor.current_peer_spki_sha256 == candidate.current_peer_spki_sha256
            || descriptor.overlap_peer_spki_sha256 == Some(candidate.current_peer_spki_sha256)
        {
            return Err(ApplyError::PeerSpkiAlreadyUsed);
        }
        if candidate.join_capability_hash.is_some()
            && candidate.join_capability_hash == descriptor.join_capability_hash
        {
            return Err(ApplyError::JoinCapabilityAlreadyUsed);
        }
    }
    Ok(())
}

fn ensure_pin_unique(
    nodes: &std::collections::BTreeMap<NodeId, NodeDescriptor>,
    node_id: NodeId,
    pin: PeerSpkiSha256,
) -> Result<(), ApplyError> {
    if nodes.iter().any(|(other_id, descriptor)| {
        *other_id != node_id
            && (descriptor.current_peer_spki_sha256 == pin
                || descriptor.overlap_peer_spki_sha256 == Some(pin))
    }) {
        return Err(ApplyError::PeerSpkiAlreadyUsed);
    }
    Ok(())
}

fn validate_peer_address(address: &PeerAddress) -> Result<(), ApplyError> {
    let value = address.0.as_str();
    if value.is_empty()
        || value.len() > MAX_PEER_ADDRESS_BYTES
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(ApplyError::InvalidPeerAddress);
    }
    Ok(())
}
