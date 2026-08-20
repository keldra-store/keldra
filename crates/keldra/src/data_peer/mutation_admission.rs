//! Receiver-side admission for typed mutable data-plane operations.
//!
//! The sender cannot confer placement authority. Every receiver derives the
//! current placement from its locally applied Raft state and rejects stale or
//! misaddressed mutations before touching the store.

use keldra_consensus::{AuthenticatedPeer, ClusterId, DecisionRaft, NodeId};
use keldra_store::{ObjectMutation, PlacementLogId, RetainedVersionDeleteMutation, SourceId};
use tonic::Status;

use crate::cluster_placement::ClusterPlacement;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::{PlacementKind, PlacementNode};

#[derive(Clone)]
pub(super) struct MutationAdmission {
    source: PlacementSource,
    local_node: NodeId,
}

#[derive(Debug)]
pub(super) struct DerivedConsumerAdmission {
    pub(super) fence: PlacementLogId,
    pub(super) active_nodes: Vec<u16>,
}

#[derive(Clone)]
enum PlacementSource {
    Raft(DecisionRaft),
    #[cfg(test)]
    Fixed(AdmissionPlacement),
}

#[derive(Clone)]
struct AdmissionPlacement {
    cluster_id: ClusterId,
    fence: PlacementLogId,
    nodes: Vec<PlacementNode>,
}

impl MutationAdmission {
    pub(super) fn raft(decisions: DecisionRaft, local_node: NodeId) -> Self {
        Self {
            source: PlacementSource::Raft(decisions),
            local_node,
        }
    }

    #[cfg(test)]
    pub(super) fn fixed(
        cluster_id: ClusterId,
        local_node: NodeId,
        active_nodes: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        use std::num::NonZeroU32;

        let weight = NonZeroU32::new(1_000_000).expect("fixed test weight is non-zero");
        Self {
            source: PlacementSource::Fixed(AdmissionPlacement {
                cluster_id,
                fence: PlacementLogId { term: 1, index: 1 },
                nodes: active_nodes
                    .into_iter()
                    .map(|node| PlacementNode::new(node, weight))
                    .collect(),
            }),
            local_node,
        }
    }

    pub(super) fn object_mutation(
        &self,
        peer: AuthenticatedPeer,
        mutation: &ObjectMutation,
    ) -> Result<PlacementLogId, Status> {
        self.object_mutation_facts(
            peer,
            mutation.tenant_id,
            mutation.bucket_id,
            &mutation.exact_path,
            mutation.stamp.active_placement_log_id,
            mutation.stamp.source_id,
        )
    }

    pub(super) fn object_mutation_batch(
        &self,
        peer: AuthenticatedPeer,
        mutations: &[ObjectMutation],
    ) -> Result<PlacementLogId, Status> {
        let placement = self.current()?;
        self.require_active_peer(peer, &placement)?;
        let mut expected_group = None;
        for mutation in mutations {
            let group = self.object_mutation_group(
                peer,
                &placement,
                mutation.tenant_id,
                mutation.bucket_id,
                &mutation.exact_path,
                mutation.stamp.active_placement_log_id,
                mutation.stamp.source_id,
            )?;
            if expected_group
                .as_ref()
                .is_some_and(|expected| expected != &group)
            {
                return Err(Status::failed_precondition(
                    "object mutation batch spans metadata replica groups",
                ));
            }
            expected_group = Some(group);
        }
        if expected_group.is_none() {
            return Err(Status::invalid_argument(
                "object mutation batch must not be empty",
            ));
        }
        Ok(placement.fence)
    }

    pub(super) fn retained_version_delete(
        &self,
        peer: AuthenticatedPeer,
        mutation: &RetainedVersionDeleteMutation,
    ) -> Result<PlacementLogId, Status> {
        self.object_mutation_facts(
            peer,
            mutation.tenant_id,
            mutation.bucket_id,
            &mutation.exact_path,
            mutation.stamp.active_placement_log_id,
            mutation.stamp.source_id,
        )
    }

    pub(super) fn object_repair(
        &self,
        peer: AuthenticatedPeer,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
        request_fence: PlacementLogId,
    ) -> Result<PlacementLogId, Status> {
        let placement = self.current()?;
        self.require_active_peer(peer, &placement)?;
        if request_fence != placement.fence {
            return Err(Status::unavailable(
                "object repair carries a stale placement fence",
            ));
        }
        let group = object_group(&placement, tenant_id, bucket_id, exact_path)?;
        self.require_local_replica(&group)?;
        Ok(placement.fence)
    }

    pub(super) fn reference_deltas(
        &self,
        peer: AuthenticatedPeer,
        source: SourceId,
    ) -> Result<PlacementLogId, Status> {
        let placement = self.current()?;
        self.require_active_peer(peer, &placement)?;
        if NodeId(u64::from(source.node_id)) != peer.node_id {
            return Err(Status::permission_denied(
                "reference source does not match the authenticated peer",
            ));
        }
        if !placement
            .nodes
            .iter()
            .any(|node| node.node_id() == self.local_node)
        {
            return Err(Status::failed_precondition(
                "reference deltas were sent to a node outside current ACTIVE placement",
            ));
        }
        Ok(placement.fence)
    }

    pub(super) fn definition_checkpoint(
        &self,
        peer: AuthenticatedPeer,
        source_node_id: u16,
    ) -> Result<PlacementLogId, Status> {
        let placement = self.current()?;
        self.require_active_peer(peer, &placement)?;
        if peer.node_id.0 != u64::from(source_node_id) {
            return Err(Status::permission_denied(
                "definition checkpoint source does not match the authenticated peer",
            ));
        }
        self.require_local_active(&placement)?;
        Ok(placement.fence)
    }

    pub(super) fn derived_consumer_checkpoint(
        &self,
        peer: AuthenticatedPeer,
        source_node_id: u16,
        consumer_node_id: u16,
        expected_fence: PlacementLogId,
    ) -> Result<DerivedConsumerAdmission, Status> {
        let placement = self.current()?;
        self.require_active_peer(peer, &placement)?;
        self.require_local_active(&placement)?;
        if self.local_node.0 != u64::from(source_node_id) {
            return Err(Status::failed_precondition(
                "derived checkpoint was sent to another source node",
            ));
        }
        if peer.node_id.0 != u64::from(consumer_node_id) {
            return Err(Status::permission_denied(
                "derived checkpoint consumer does not match the authenticated peer",
            ));
        }
        if placement.fence != expected_fence {
            return Err(Status::unavailable(
                "derived checkpoint carries a stale membership fence",
            ));
        }
        let mut active_nodes = placement
            .nodes
            .iter()
            .map(|node| {
                u16::try_from(node.node_id().0).map_err(|_| {
                    Status::data_loss("ACTIVE node ID exceeds the source-journal identity range")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        active_nodes.sort_unstable();
        Ok(DerivedConsumerAdmission {
            fence: placement.fence,
            active_nodes,
        })
    }

    pub(super) fn definition_assignments(
        &self,
        peer: AuthenticatedPeer,
        expected_fence: PlacementLogId,
    ) -> Result<PlacementLogId, Status> {
        let placement = self.current()?;
        self.require_active_peer(peer, &placement)?;
        self.require_local_active(&placement)?;
        if placement.fence != expected_fence {
            return Err(Status::unavailable(
                "definition assignment carries a stale placement fence",
            ));
        }
        Ok(placement.fence)
    }

    pub(super) fn definition_assignment_page(
        &self,
        peer: AuthenticatedPeer,
        source_node_id: u16,
        expected_fence: PlacementLogId,
    ) -> Result<PlacementLogId, Status> {
        if peer.node_id.0 != u64::from(source_node_id) {
            return Err(Status::permission_denied(
                "definition assignment source does not match the authenticated peer",
            ));
        }
        self.definition_assignments(peer, expected_fence)
    }

    pub(super) fn require_fence(&self, expected: PlacementLogId) -> Result<(), Status> {
        if self.current()?.fence != expected {
            return Err(Status::unavailable(
                "placement changed while applying the peer mutation",
            ));
        }
        Ok(())
    }

    fn object_mutation_facts(
        &self,
        peer: AuthenticatedPeer,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
        mutation_fence: PlacementLogId,
        source: SourceId,
    ) -> Result<PlacementLogId, Status> {
        let placement = self.current()?;
        self.require_active_peer(peer, &placement)?;
        self.object_mutation_group(
            peer,
            &placement,
            tenant_id,
            bucket_id,
            exact_path,
            mutation_fence,
            source,
        )?;
        Ok(placement.fence)
    }

    #[allow(clippy::too_many_arguments)]
    fn object_mutation_group(
        &self,
        peer: AuthenticatedPeer,
        placement: &AdmissionPlacement,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
        mutation_fence: PlacementLogId,
        source: SourceId,
    ) -> Result<MutableRecordReplicaGroup, Status> {
        if mutation_fence != placement.fence {
            return Err(Status::unavailable(
                "object mutation carries a stale placement fence",
            ));
        }
        if NodeId(u64::from(source.node_id)) != peer.node_id {
            return Err(Status::permission_denied(
                "object mutation source does not match the authenticated peer",
            ));
        }
        let group = object_group(&placement, tenant_id, bucket_id, exact_path)?;
        self.require_local_replica(&group)?;
        if group.coordinator() != peer.node_id {
            return Err(Status::permission_denied(
                "object mutation did not come from the current path coordinator",
            ));
        }
        Ok(group)
    }

    fn require_active_peer(
        &self,
        peer: AuthenticatedPeer,
        placement: &AdmissionPlacement,
    ) -> Result<(), Status> {
        if peer.cluster_id != placement.cluster_id {
            return Err(Status::permission_denied(
                "peer cluster does not match current placement",
            ));
        }
        if !placement
            .nodes
            .iter()
            .any(|node| node.node_id() == peer.node_id)
        {
            return Err(Status::permission_denied(
                "peer is not ACTIVE in current placement",
            ));
        }
        Ok(())
    }

    fn require_local_replica(&self, group: &MutableRecordReplicaGroup) -> Result<(), Status> {
        if !group.replicas().contains(&self.local_node) {
            return Err(Status::failed_precondition(
                "object operation was sent to a node outside its current replica group",
            ));
        }
        Ok(())
    }

    fn require_local_active(&self, placement: &AdmissionPlacement) -> Result<(), Status> {
        if placement
            .nodes
            .iter()
            .any(|node| node.node_id() == self.local_node)
        {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "definition assignment was sent to a node outside current ACTIVE placement",
            ))
        }
    }

    fn current(&self) -> Result<AdmissionPlacement, Status> {
        match &self.source {
            PlacementSource::Raft(decisions) => {
                let state = decisions
                    .state()
                    .map_err(|error| Status::unavailable(error.to_string()))?;
                let placement = ClusterPlacement::from_applied(&state)
                    .map_err(|error| Status::unavailable(error.to_string()))?;
                Ok(AdmissionPlacement {
                    cluster_id: placement.cluster_id(),
                    fence: placement.fence(),
                    nodes: placement.placement_nodes().to_vec(),
                })
            }
            #[cfg(test)]
            PlacementSource::Fixed(placement) => Ok(placement.clone()),
        }
    }
}

fn object_group(
    placement: &AdmissionPlacement,
    tenant_id: u64,
    bucket_id: u64,
    exact_path: &str,
) -> Result<MutableRecordReplicaGroup, Status> {
    let mut key = Vec::with_capacity(16 + exact_path.len());
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&bucket_id.to_be_bytes());
    key.extend_from_slice(exact_path.as_bytes());
    MutableRecordReplicaGroup::select(
        PlacementKind::Object,
        placement.cluster_id,
        &key,
        &placement.nodes,
    )
    .ok_or_else(|| Status::unavailable("cluster has no active object owner"))
}

#[cfg(test)]
mod tests {
    use keldra_consensus::PeerSpkiSha256;
    use keldra_store::{
        MUTATION_STAMP_FORMAT, MutationStamp, OBJECT_MUTATION_FORMAT, Version, VersionId,
    };
    use tonic::Code;

    use super::*;

    fn cluster_id() -> ClusterId {
        ClusterId(*b"mutation-admit01")
    }

    fn peer(node: u64) -> AuthenticatedPeer {
        AuthenticatedPeer {
            cluster_id: cluster_id(),
            node_id: NodeId(node),
            spki_sha256: PeerSpkiSha256([node as u8; 32]),
        }
    }

    fn source(node: u16) -> SourceId {
        SourceId {
            node_id: node,
            source_epoch: [node as u8; 32],
        }
    }

    fn path_matching(
        authority: &MutationAdmission,
        predicate: impl Fn(&MutableRecordReplicaGroup) -> bool,
    ) -> String {
        let placement = authority.current().unwrap();
        (0..10_000)
            .map(|index| format!("/object-{index}"))
            .find(|path| {
                let group = object_group(&placement, 1, 2, path).unwrap();
                predicate(&group)
            })
            .expect("test membership has a matching HRW path")
    }

    fn mutation(path: String, fence: PlacementLogId, coordinator: NodeId) -> ObjectMutation {
        ObjectMutation {
            format: OBJECT_MUTATION_FORMAT,
            tenant_id: 1,
            bucket_id: 2,
            exact_path: path,
            command_id: "command".into(),
            input_fingerprint: [1; 32],
            version: Version {
                id: VersionId(1),
                blob: None,
                content_type: None,
                deleted: true,
                committed_at_unix_millis: 1,
            },
            retire_predecessor: false,
            receipt_expires_at_unix_millis: 1,
            stamp: MutationStamp {
                format: MUTATION_STAMP_FORMAT,
                predecessor_version: None,
                program_commit_cursor: None,
                mutation_fingerprint: [2; 32],
                active_placement_log_id: fence,
                serving_fence_term: 1,
                source_id: source(coordinator.0 as u16),
                source_journal_position: 1,
            },
            reference_deltas: Vec::new(),
            accounting_transition: None,
            definition_transition: None,
        }
    }

    #[test]
    fn object_mutation_requires_current_fence_coordinator_and_destination() {
        let authority = MutationAdmission::fixed(
            cluster_id(),
            NodeId(2),
            [NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
        );
        let accepted_path = path_matching(&authority, |group| {
            group.coordinator() == NodeId(1) && group.replicas().contains(&NodeId(2))
        });
        let fence = authority.current().unwrap().fence;
        assert_eq!(
            authority
                .object_mutation_facts(peer(1), 1, 2, &accepted_path, fence, source(1),)
                .unwrap(),
            fence
        );

        let stale = authority
            .object_mutation_facts(
                peer(1),
                1,
                2,
                &accepted_path,
                PlacementLogId { term: 1, index: 0 },
                source(1),
            )
            .unwrap_err();
        assert_eq!(stale.code(), Code::Unavailable);

        let spoofed = authority
            .object_mutation_facts(peer(1), 1, 2, &accepted_path, fence, source(3))
            .unwrap_err();
        assert_eq!(spoofed.code(), Code::PermissionDenied);

        let wrong_coordinator = authority
            .object_mutation_facts(peer(2), 1, 2, &accepted_path, fence, source(2))
            .unwrap_err();
        assert_eq!(wrong_coordinator.code(), Code::PermissionDenied);

        let misaddressed = MutationAdmission::fixed(
            cluster_id(),
            NodeId(4),
            [NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
        );
        let excluded_path = path_matching(&misaddressed, |group| {
            !group.replicas().contains(&NodeId(4))
        });
        let group = object_group(&misaddressed.current().unwrap(), 1, 2, &excluded_path).unwrap();
        let coordinator = group.coordinator();
        let error = misaddressed
            .object_mutation_facts(
                peer(coordinator.0),
                1,
                2,
                &excluded_path,
                fence,
                source(coordinator.0 as u16),
            )
            .unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
    }

    #[test]
    fn object_mutation_batch_requires_one_exact_group_and_fence() {
        let authority = MutationAdmission::fixed(
            cluster_id(),
            NodeId(2),
            [NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
        );
        let placement = authority.current().unwrap();
        let first_path = path_matching(&authority, |group| {
            group.coordinator() == NodeId(1) && group.replicas().contains(&NodeId(2))
        });
        let first_group = object_group(&placement, 1, 2, &first_path).unwrap();
        let second_path = (0..100_000)
            .map(|index| format!("/batch-{index}"))
            .find(|path| {
                let group = object_group(&placement, 1, 2, path).unwrap();
                group.coordinator() == NodeId(1)
                    && group.replicas().contains(&NodeId(2))
                    && group != first_group
            })
            .unwrap();
        let first = mutation(first_path, placement.fence, NodeId(1));
        assert!(
            authority
                .object_mutation_batch(peer(1), std::slice::from_ref(&first))
                .is_ok()
        );
        assert_eq!(
            authority
                .object_mutation_batch(
                    peer(1),
                    &[
                        first.clone(),
                        mutation(second_path, placement.fence, NodeId(1))
                    ],
                )
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        let mut stale = first;
        stale.stamp.active_placement_log_id.index += 1;
        assert_eq!(
            authority
                .object_mutation_batch(peer(1), &[stale])
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
    }

    #[test]
    fn repair_accepts_any_active_source_but_only_a_selected_destination() {
        let authority = MutationAdmission::fixed(
            cluster_id(),
            NodeId(2),
            [NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
        );
        let path = path_matching(&authority, |group| group.replicas().contains(&NodeId(2)));
        let fence = authority.current().unwrap().fence;
        assert!(authority.object_repair(peer(4), 1, 2, &path, fence).is_ok());
        let stale = authority
            .object_repair(peer(4), 1, 2, &path, PlacementLogId { term: 1, index: 0 })
            .unwrap_err();
        assert_eq!(stale.code(), Code::Unavailable);

        let excluded = MutationAdmission::fixed(
            cluster_id(),
            NodeId(4),
            [NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
        );
        let path = path_matching(&excluded, |group| !group.replicas().contains(&NodeId(4)));
        assert_eq!(
            excluded
                .object_repair(peer(1), 1, 2, &path, fence)
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
    }

    #[test]
    fn reference_source_is_bound_to_mtls_and_both_nodes_must_be_active() {
        let authority =
            MutationAdmission::fixed(cluster_id(), NodeId(1), [NodeId(1), NodeId(2), NodeId(3)]);
        assert!(authority.reference_deltas(peer(2), source(2)).is_ok());
        assert_eq!(
            authority
                .reference_deltas(peer(2), source(3))
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            authority
                .reference_deltas(peer(4), source(4))
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );

        let inactive_destination =
            MutationAdmission::fixed(cluster_id(), NodeId(4), [NodeId(1), NodeId(2), NodeId(3)]);
        assert_eq!(
            inactive_destination
                .reference_deltas(peer(2), source(2))
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
    }

    #[test]
    fn definition_assignment_page_cannot_advance_another_sources_checkpoint() {
        let authority =
            MutationAdmission::fixed(cluster_id(), NodeId(1), [NodeId(1), NodeId(2), NodeId(3)]);
        let fence = authority.current().unwrap().fence;
        assert_eq!(
            authority
                .definition_assignment_page(peer(2), 3, fence)
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert!(
            authority
                .definition_assignment_page(peer(2), 2, fence)
                .is_ok()
        );
    }

    #[test]
    fn derived_checkpoint_is_bound_to_source_consumer_and_current_fence() {
        let authority =
            MutationAdmission::fixed(cluster_id(), NodeId(1), [NodeId(1), NodeId(2), NodeId(3)]);
        let fence = authority.current().unwrap().fence;
        let accepted = authority
            .derived_consumer_checkpoint(peer(2), 1, 2, fence)
            .unwrap();
        assert_eq!(accepted.fence, fence);
        assert_eq!(accepted.active_nodes, vec![1, 2, 3]);
        assert_eq!(
            authority
                .derived_consumer_checkpoint(peer(2), 1, 3, fence)
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            authority
                .derived_consumer_checkpoint(peer(2), 2, 2, fence)
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            authority
                .derived_consumer_checkpoint(
                    peer(2),
                    1,
                    2,
                    PlacementLogId {
                        term: fence.term,
                        index: fence.index + 1,
                    },
                )
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
    }
}
