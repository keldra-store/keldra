//! Exact metadata replica identity for a batch under its captured placement.

use super::*;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct ImmutablePublicationGroup {
    // Order is part of the receiver's exact group identity, not just a set.
    replicas: Vec<NodeId>,
    required_acknowledgements: usize,
    address: Option<String>,
}

impl ImmutablePublicationGroup {
    pub(super) fn from_placement(
        placement: &ClusterPlacement,
        replicas: &MutableRecordReplicaGroup,
        local_node: NodeId,
    ) -> Result<Self, Status> {
        let coordinator = replicas.coordinator();
        let address = if coordinator == local_node {
            None
        } else {
            Some(
                placement
                    .address(coordinator)
                    .ok_or_else(|| {
                        Status::unavailable(format!(
                            "ACTIVE object coordinator {} has no peer address",
                            coordinator.0,
                        ))
                    })?
                    .0
                    .clone(),
            )
        };
        Ok(Self::new(replicas, address))
    }

    fn new(replicas: &MutableRecordReplicaGroup, address: Option<String>) -> Self {
        Self {
            replicas: replicas.replicas().to_vec(),
            required_acknowledgements: replicas.required_acknowledgements(),
            address,
        }
    }

    pub(super) fn coordinator(&self) -> NodeId {
        self.replicas[0]
    }
    pub(super) fn address(&self) -> Option<&String> {
        self.address.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement::{PlacementKind, PlacementNode};
    use keldra_consensus::ClusterId;
    use std::num::NonZeroU32;

    #[test]
    fn three_node_same_coordinator_different_ranked_replicas_form_distinct_batches() {
        let nodes = (1..=3)
            .map(|id| PlacementNode::new(NodeId(id), NonZeroU32::new(1_000_000).unwrap()))
            .collect::<Vec<_>>();
        let mut found = BTreeMap::<NodeId, Vec<MutableRecordReplicaGroup>>::new();
        let mut pair = None;
        for index in 0_u64..10_000 {
            let group = MutableRecordReplicaGroup::select(
                PlacementKind::Object,
                ClusterId([42; 16]),
                &index.to_be_bytes(),
                &nodes,
            )
            .unwrap();
            let prior = found.entry(group.coordinator()).or_default();
            if let Some(other) = prior.iter().find(|other| **other != group) {
                pair = Some((other.clone(), group));
                break;
            }
            prior.push(group);
        }
        let (first, second) = pair.expect("three-node HRW must expose follower order permutations");
        assert_eq!(first.coordinator(), second.coordinator());
        assert_ne!(first.replicas(), second.replicas());
        assert_eq!(
            first.required_acknowledgements(),
            second.required_acknowledgements()
        );
        let address = Some("https://same-coordinator.example".to_owned());
        let first_route = ImmutablePublicationGroup::new(&first, address.clone());
        let second_route = ImmutablePublicationGroup::new(&second, address);
        let mut groups = BTreeMap::<ImmutablePublicationGroup, Vec<usize>>::new();
        for (index, route) in [
            (0, first_route.clone()),
            (1, second_route.clone()),
            (2, first_route.clone()),
        ] {
            groups.entry(route).or_default().push(index);
        }
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[&first_route], vec![0, 2]);
        assert_eq!(groups[&second_route], vec![1]);
        assert_eq!(first_route.coordinator(), second_route.coordinator());
        let mut slots = std::iter::repeat_with(|| None).take(3).collect::<Vec<_>>();
        // Completion order across independently dispatched replica groups must
        // not alter caller order or move a per-item failure to another request.
        record_grouped_artifact_outcomes(
            &mut slots,
            groups[&second_route].clone(),
            vec![Err(Status::aborted("lost fence"))],
        )
        .unwrap();
        record_grouped_artifact_outcomes(
            &mut slots,
            groups[&first_route].clone(),
            vec![
                Ok(IndexArtifactOutcome {
                    version: VersionId(10),
                    replayed: false,
                }),
                Ok(IndexArtifactOutcome {
                    version: VersionId(12),
                    replayed: true,
                }),
            ],
        )
        .unwrap();
        let ordered = ordered_grouped_artifact_outcomes(slots).unwrap();
        assert_eq!(ordered[0].as_ref().unwrap().version, VersionId(10));
        assert_eq!(
            ordered[1].as_ref().unwrap_err().code(),
            tonic::Code::Aborted
        );
        assert_eq!(ordered[2].as_ref().unwrap().version, VersionId(12));
        assert!(ordered[2].as_ref().unwrap().replayed);
    }
}
