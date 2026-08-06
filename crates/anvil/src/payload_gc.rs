//! Placement-aware retirement of former payload artifacts.
//!
//! Placement remains derived from the committed ACTIVE membership. This
//! worker keeps no inventory: it scans the ordinary lifecycle column family,
//! proves the current selected artifacts healthy, and turns a former local
//! artifact into an ordinary age-gated GC candidate.

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{
    ErasureCodec, ErasureProfile, MAX_PAYLOAD_HANDOFF_EXPORT_RECORDS, PayloadArtifactCursor,
    PayloadArtifactIdentity, PayloadArtifactSnapshot, PayloadArtifactState, PlacementLogId,
    ShardIdentity, Store,
};
use thiserror::Error;

use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::DataPeerTransport;
use crate::payload_distribution::PayloadArtifactPeers;
use crate::payload_placement::{PayloadPlacement, select_payload_placement};
use crate::reference_delivery::ReferenceRuntimeHandle;

#[derive(Clone)]
pub(crate) struct PayloadGarbageCollector {
    local_node: NodeId,
    store: Store,
    decisions: DecisionRaft,
    peers: DataPeerTransport,
    references: ReferenceRuntimeHandle,
    profile: ErasureProfile,
}

impl PayloadGarbageCollector {
    pub(crate) fn new(
        local_node: NodeId,
        store: Store,
        decisions: DecisionRaft,
        peers: DataPeerTransport,
        references: ReferenceRuntimeHandle,
        profile: ErasureProfile,
    ) -> Self {
        Self {
            local_node,
            store,
            decisions,
            peers,
            references,
            profile,
        }
    }

    /// Retire former local copies only under one exact, reference-safe ACTIVE
    /// placement. Physical deletion remains the ordinary Store GC's job after
    /// its configured inactivity grace.
    pub(crate) async fn run_once(&self) -> Result<u64, PayloadGcError> {
        if !self.references.gc_safe().await {
            return Ok(0);
        }
        let placement = current_stable_placement(&self.decisions, self.local_node)?;
        let fence = placement.fence();
        let mut cursor: Option<PayloadArtifactCursor> = None;
        let mut retired = 0_u64;

        loop {
            let page = self
                .store
                .export_payload_artifact_snapshots(
                    cursor.as_ref(),
                    MAX_PAYLOAD_HANDOFF_EXPORT_RECORDS,
                )
                .map_err(|error| PayloadGcError::Store(error.to_string()))?;
            for artifact in page.artifacts {
                if artifact.lifecycle.ref_count == 0 && artifact.lifecycle.flags == 0 {
                    continue;
                }
                let desired = select_payload_placement(
                    placement.cluster_id(),
                    artifact.identity.blob(),
                    self.profile,
                    placement.placement_nodes(),
                );
                if artifact_is_selected(self.local_node, &artifact.identity, &desired) {
                    continue;
                }
                if !self
                    .placement_is_healthy(&placement, &desired, artifact.identity.blob())
                    .await?
                {
                    continue;
                }
                // The health probes can take time. Re-prove reference safety
                // before entering the short Store commit fence.
                if !self.references.gc_safe().await {
                    return Ok(retired);
                }
                let decisions = self.decisions.clone();
                let local_node = self.local_node;
                if self
                    .store
                    .retire_payload_artifact_if_unchanged(&artifact, move || {
                        placement_fence_is_current(&decisions, local_node, fence)
                    })
                    .await
                    .map_err(|error| PayloadGcError::Store(error.to_string()))?
                {
                    retired = retired
                        .checked_add(1)
                        .ok_or(PayloadGcError::RetirementCountExhausted)?;
                }
            }
            let Some(next) = page.next_cursor else {
                return Ok(retired);
            };
            cursor = Some(next);
        }
    }

    async fn placement_is_healthy(
        &self,
        placement: &ClusterPlacement,
        desired: &PayloadPlacement,
        blob: &anvil_store::BlobRef,
    ) -> Result<bool, PayloadGcError> {
        match desired {
            PayloadPlacement::Small(selected) => {
                for owner in selected.owners() {
                    if !self
                        .complete_is_healthy(placement, *owner, blob, true)
                        .await?
                    {
                        return Ok(false);
                    }
                }
            }
            PayloadPlacement::LargeComplete(selected) => {
                for owner in selected.owners() {
                    if !self
                        .complete_is_healthy(placement, *owner, blob, false)
                        .await?
                    {
                        return Ok(false);
                    }
                }
            }
            PayloadPlacement::Large(selected) => {
                let codec = ErasureCodec::new(self.profile)
                    .map_err(|error| PayloadGcError::Placement(error.to_string()))?;
                for shard in selected.shards() {
                    let identity = ShardIdentity::new(blob.clone(), shard.ordinal());
                    let healthy = if shard.owner() == self.local_node {
                        self.store.get_shard(&codec, &identity).is_ok()
                    } else {
                        let address = placement
                            .address(shard.owner())
                            .ok_or(PayloadGcError::MissingAddress(shard.owner()))?;
                        PayloadArtifactPeers::shard_exists(
                            &self.peers,
                            shard.owner(),
                            &address.0,
                            &identity,
                        )
                        .await
                        .map_err(|error| PayloadGcError::Peer(error.to_string()))?
                    };
                    if !healthy {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    async fn complete_is_healthy(
        &self,
        placement: &ClusterPlacement,
        owner: NodeId,
        blob: &anvil_store::BlobRef,
        small: bool,
    ) -> Result<bool, PayloadGcError> {
        if owner == self.local_node {
            return self
                .store
                .complete_copy_state(blob)
                .await
                .map(|state| state == PayloadArtifactState::Valid)
                .map_err(|error| PayloadGcError::Store(error.to_string()));
        }
        let address = placement
            .address(owner)
            .ok_or(PayloadGcError::MissingAddress(owner))?;
        let result = if small {
            PayloadArtifactPeers::small_exists(&self.peers, owner, &address.0, blob).await
        } else {
            PayloadArtifactPeers::complete_exists(&self.peers, owner, &address.0, blob).await
        };
        result.map_err(|error| PayloadGcError::Peer(error.to_string()))
    }
}

fn artifact_is_selected(
    local_node: NodeId,
    artifact: &PayloadArtifactIdentity,
    desired: &PayloadPlacement,
) -> bool {
    match (artifact, desired) {
        (PayloadArtifactIdentity::Complete(_), PayloadPlacement::Small(selected)) => {
            selected.owners().contains(&local_node)
        }
        (PayloadArtifactIdentity::Complete(_), PayloadPlacement::LargeComplete(selected)) => {
            selected.owners().contains(&local_node)
        }
        (PayloadArtifactIdentity::Shard(identity), PayloadPlacement::Large(selected)) => selected
            .shards()
            .iter()
            .any(|shard| shard.owner() == local_node && shard.ordinal() == identity.ordinal()),
        _ => false,
    }
}

fn current_stable_placement(
    decisions: &DecisionRaft,
    local_node: NodeId,
) -> Result<ClusterPlacement, PayloadGcError> {
    let state = decisions
        .state()
        .map_err(|error| PayloadGcError::Placement(error.to_string()))?;
    if state.cluster_control().transition().is_some() {
        return Err(PayloadGcError::TransitionInProgress);
    }
    let placement = ClusterPlacement::from_applied(&state)
        .map_err(|error| PayloadGcError::Placement(error.to_string()))?;
    if !placement.active_node_ids().contains(&local_node) {
        return Err(PayloadGcError::LocalNodeInactive(local_node));
    }
    Ok(placement)
}

fn placement_fence_is_current(
    decisions: &DecisionRaft,
    local_node: NodeId,
    expected: PlacementLogId,
) -> bool {
    current_stable_placement(decisions, local_node)
        .is_ok_and(|placement| placement.fence() == expected)
}

#[derive(Debug, Error)]
pub(crate) enum PayloadGcError {
    #[error("payload GC placement is unavailable: {0}")]
    Placement(String),
    #[error("payload GC is paused during a membership transition")]
    TransitionInProgress,
    #[error("local node {0:?} is not ACTIVE in the current placement")]
    LocalNodeInactive(NodeId),
    #[error("ACTIVE payload owner {0:?} has no peer address")]
    MissingAddress(NodeId),
    #[error("payload replacement health probe failed: {0}")]
    Peer(String),
    #[error("payload lifecycle storage failed: {0}")]
    Store(String),
    #[error("payload GC retirement count is exhausted")]
    RetirementCountExhausted,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use anvil_consensus::ClusterId;
    use anvil_store::{BlobRef, SMALL_BLOB_MAX_BYTES};

    use super::*;
    use crate::placement::PlacementNode;

    fn node(node_id: u64) -> PlacementNode {
        PlacementNode::new(NodeId(node_id), NonZeroU32::new(1_000_000).unwrap())
    }

    #[test]
    fn selection_classifies_displaced_small_and_former_large_complete_copies() {
        let cluster = ClusterId(*b"payload-gc-test!");
        let profile = ErasureProfile::default();
        let nodes = [node(1), node(2), node(3)];
        let small = BlobRef {
            hash: [3; 32],
            length: 7,
        };
        let large = BlobRef {
            hash: [9; 32],
            length: SMALL_BLOB_MAX_BYTES as u64 + 1,
        };
        let small_placement = select_payload_placement(cluster, &small, profile, &nodes);
        let large_placement = select_payload_placement(cluster, &large, profile, &nodes);
        let PayloadPlacement::Small(selected_small) = &small_placement else {
            panic!("expected small placement")
        };
        let displaced = nodes
            .iter()
            .map(|node| node.node_id())
            .find(|node_id| !selected_small.owners().contains(node_id))
            .expect("three nodes contain one node outside a two-copy placement");
        assert!(!artifact_is_selected(
            displaced,
            &PayloadArtifactIdentity::Complete(small.clone()),
            &small_placement,
        ));
        assert!(artifact_is_selected(
            selected_small.owners()[0],
            &PayloadArtifactIdentity::Complete(small),
            &small_placement,
        ));
        assert!(!artifact_is_selected(
            NodeId(1),
            &PayloadArtifactIdentity::Complete(large.clone()),
            &large_placement,
        ));
        let PayloadPlacement::Large(selected_large) = &large_placement else {
            panic!("expected erasure placement")
        };
        let shard = selected_large.shards()[0];
        assert!(artifact_is_selected(
            shard.owner(),
            &PayloadArtifactIdentity::Shard(ShardIdentity::new(large, shard.ordinal())),
            &large_placement,
        ));
    }
}
