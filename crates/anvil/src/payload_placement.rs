//! Deterministic payload-owner selection.
//!
//! This module turns one content identity and the cluster erasure profile into
//! the desired owner set. It deliberately does not decide acknowledgement
//! thresholds, perform I/O, or persist placement decisions.

use std::collections::HashSet;

use anvil_consensus::{ClusterId, NodeId};
use anvil_store::{BlobRef, ErasureProfile, SMALL_BLOB_MAX_BYTES};

use crate::placement::{PlacementNode, rank_nodes};

/// Desired placement for one content-addressed payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PayloadPlacement {
    Small(SmallPayloadPlacement),
    Large(LargePayloadPlacement),
}

impl PayloadPlacement {
    /// Number of distinct owners required by the committed erasure profile.
    pub(crate) fn required_count(&self) -> usize {
        match self {
            Self::Small(placement) => placement.required_copies,
            Self::Large(placement) => placement.required_shards,
        }
    }

    /// Number of distinct owners available in the supplied active membership.
    pub(crate) fn available_count(&self) -> usize {
        match self {
            Self::Small(placement) => placement.owners.len(),
            Self::Large(placement) => placement.shards.len(),
        }
    }

    pub(crate) fn is_under_redundant(&self) -> bool {
        self.available_count() < self.required_count()
    }
}

/// Complete-copy owners for content of at most 64 KiB.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SmallPayloadPlacement {
    required_copies: usize,
    owners: Vec<NodeId>,
}

impl SmallPayloadPlacement {
    pub(crate) fn owners(&self) -> &[NodeId] {
        &self.owners
    }
}

/// One large-payload shard ordinal and its distinct owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShardPlacement {
    ordinal: u16,
    owner: NodeId,
}

impl ShardPlacement {
    pub(crate) const fn ordinal(self) -> u16 {
        self.ordinal
    }

    pub(crate) const fn owner(self) -> NodeId {
        self.owner
    }
}

/// Available ordinal assignments for content larger than 64 KiB.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LargePayloadPlacement {
    required_shards: usize,
    shards: Vec<ShardPlacement>,
}

impl LargePayloadPlacement {
    pub(crate) fn shards(&self) -> &[ShardPlacement] {
        &self.shards
    }
}

/// Select the desired payload owners from the active placement set.
///
/// The caller supplies the protocol's payload placement kind. Input nodes are
/// defensively deduplicated by stable node ID before an owner is assigned.
pub(crate) fn select_payload_placement(
    placement_kind: u8,
    cluster_id: ClusterId,
    content: &BlobRef,
    profile: ErasureProfile,
    active_nodes: &[PlacementNode],
) -> PayloadPlacement {
    let key = content_placement_key(content);
    let ranked = distinct_ranked_nodes(placement_kind, cluster_id, &key, active_nodes);

    if content.length <= SMALL_BLOB_MAX_BYTES as u64 {
        let required_copies = usize::from(profile.parity_shards()) + 1;
        let owners = ranked.into_iter().take(required_copies).collect();
        PayloadPlacement::Small(SmallPayloadPlacement {
            required_copies,
            owners,
        })
    } else {
        let required_shards = usize::from(profile.total_shards());
        let shards = ranked
            .into_iter()
            .take(required_shards)
            .enumerate()
            .map(|(ordinal, owner)| ShardPlacement {
                ordinal: u16::try_from(ordinal)
                    .expect("a valid erasure profile has at most 256 shards"),
                owner,
            })
            .collect();
        PayloadPlacement::Large(LargePayloadPlacement {
            required_shards,
            shards,
        })
    }
}

fn content_placement_key(content: &BlobRef) -> [u8; 40] {
    let mut key = [0_u8; 40];
    key[..32].copy_from_slice(&content.hash);
    key[32..].copy_from_slice(&content.length.to_be_bytes());
    key
}

fn distinct_ranked_nodes(
    placement_kind: u8,
    cluster_id: ClusterId,
    key: &[u8],
    active_nodes: &[PlacementNode],
) -> Vec<NodeId> {
    let mut seen = HashSet::with_capacity(active_nodes.len());
    rank_nodes(placement_kind, cluster_id, key, active_nodes)
        .into_iter()
        .map(PlacementNode::node_id)
        .filter(|node_id| seen.insert(*node_id))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;

    const PLACEMENT_KIND: u8 = 6;

    fn cluster_id() -> ClusterId {
        ClusterId(*b"payload-place-v1")
    }

    fn profile() -> ErasureProfile {
        ErasureProfile::new(4, 2, 16 * 1024).expect("test profile is valid")
    }

    fn content(length: u64) -> BlobRef {
        BlobRef {
            hash: [0x5a; 32],
            length,
        }
    }

    fn node(node_id: u64, weight_millionths: u32) -> PlacementNode {
        PlacementNode::new(
            NodeId(node_id),
            NonZeroU32::new(weight_millionths).expect("test weight must be positive"),
        )
    }

    fn nodes() -> Vec<PlacementNode> {
        vec![
            node(41, 500_000),
            node(3, 2_000_000),
            node(29, 1_000_000),
            node(7, 1_500_000),
            node(61, 750_000),
            node(13, 1_000_000),
            node(53, 1_250_000),
        ]
    }

    fn small_owners(placement: PayloadPlacement) -> Vec<u64> {
        let PayloadPlacement::Small(placement) = placement else {
            panic!("expected small-payload placement")
        };
        placement.owners().iter().map(|owner| owner.0).collect()
    }

    fn large_assignments(placement: PayloadPlacement) -> Vec<(u16, u64)> {
        let PayloadPlacement::Large(placement) = placement else {
            panic!("expected large-payload placement")
        };
        placement
            .shards()
            .iter()
            .map(|shard| (shard.ordinal(), shard.owner().0))
            .collect()
    }

    #[test]
    fn small_weighted_vector_is_frozen_and_input_order_independent() {
        let content = content(SMALL_BLOB_MAX_BYTES as u64);
        let first = nodes();
        let mut reversed = first.clone();
        reversed.reverse();

        let first =
            select_payload_placement(PLACEMENT_KIND, cluster_id(), &content, profile(), &first);
        let reversed =
            select_payload_placement(PLACEMENT_KIND, cluster_id(), &content, profile(), &reversed);

        assert_eq!(small_owners(first), [3, 13, 29]);
        assert_eq!(small_owners(reversed), [3, 13, 29]);
    }

    #[test]
    fn large_weighted_ordinal_vector_is_frozen() {
        let placement = select_payload_placement(
            PLACEMENT_KIND,
            cluster_id(),
            &content(SMALL_BLOB_MAX_BYTES as u64 + 1),
            profile(),
            &nodes(),
        );

        assert_eq!(
            large_assignments(placement),
            [(0, 53), (1, 3), (2, 29), (3, 7), (4, 13), (5, 41)]
        );
    }

    #[test]
    fn insufficient_membership_reports_both_layouts_under_redundant() {
        let active = [node(2, 1_000_000), node(5, 2_000_000)];
        let small = select_payload_placement(
            PLACEMENT_KIND,
            cluster_id(),
            &content(17),
            profile(),
            &active,
        );
        let large = select_payload_placement(
            PLACEMENT_KIND,
            cluster_id(),
            &content(SMALL_BLOB_MAX_BYTES as u64 + 1),
            profile(),
            &active,
        );

        assert_eq!((small.required_count(), small.available_count()), (3, 2));
        assert!(small.is_under_redundant());
        assert_eq!((large.required_count(), large.available_count()), (6, 2));
        assert!(large.is_under_redundant());
        assert_eq!(
            large_assignments(large)
                .into_iter()
                .map(|(ordinal, _)| ordinal)
                .collect::<Vec<_>>(),
            [0, 1]
        );
    }

    #[test]
    fn duplicate_candidates_never_create_duplicate_owners() {
        let active = [
            node(2, 1_000_000),
            node(5, 2_000_000),
            node(2, 1_000_000),
            node(11, 750_000),
        ];
        let placement = select_payload_placement(
            PLACEMENT_KIND,
            cluster_id(),
            &content(SMALL_BLOB_MAX_BYTES as u64 + 1),
            profile(),
            &active,
        );
        let assignments = large_assignments(placement);
        let owners = assignments
            .iter()
            .map(|(_, owner)| *owner)
            .collect::<HashSet<_>>();

        assert_eq!(assignments.len(), 3);
        assert_eq!(owners.len(), assignments.len());
    }

    #[test]
    fn complete_membership_meets_m_plus_one_and_k_plus_m_counts() {
        let active = nodes();
        let small = select_payload_placement(
            PLACEMENT_KIND,
            cluster_id(),
            &content(1),
            profile(),
            &active,
        );
        let large = select_payload_placement(
            PLACEMENT_KIND,
            cluster_id(),
            &content(SMALL_BLOB_MAX_BYTES as u64 + 1),
            profile(),
            &active,
        );

        assert_eq!((small.required_count(), small.available_count()), (3, 3));
        assert!(!small.is_under_redundant());
        assert_eq!((large.required_count(), large.available_count()), (6, 6));
        assert!(!large.is_under_redundant());
    }
}
