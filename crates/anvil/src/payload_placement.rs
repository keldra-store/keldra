//! Deterministic payload-owner selection.
//!
//! This module turns one content identity and the cluster erasure profile into
//! the desired owner set. It deliberately does not decide acknowledgement
//! thresholds, perform I/O, or persist placement decisions.

use std::collections::HashSet;

use anvil_consensus::{ClusterId, NodeId};
use anvil_store::{BlobRef, Durability, ErasureProfile, SMALL_BLOB_MAX_BYTES};
use thiserror::Error;

use crate::placement::{PlacementKind, PlacementNode, rank_nodes};

/// Desired placement for one content-addressed payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PayloadPlacement {
    Small(SmallPayloadPlacement),
    /// A large payload kept as complete content-addressed copies while the
    /// committed ACTIVE membership cannot place every erasure ordinal on a
    /// distinct node.
    LargeComplete(CompletePayloadPlacement),
    Large(LargePayloadPlacement),
}

impl PayloadPlacement {
    /// Number of distinct owners required by the committed erasure profile.
    pub(crate) fn required_count(&self) -> usize {
        match self {
            Self::Small(placement) => placement.required_copies,
            Self::LargeComplete(placement) => placement.required_copies,
            Self::Large(placement) => placement.required_shards,
        }
    }

    /// Number of distinct owners available in the supplied active membership.
    pub(crate) fn available_count(&self) -> usize {
        match self {
            Self::Small(placement) => placement.owners.len(),
            Self::LargeComplete(placement) => placement.owners.len(),
            Self::Large(placement) => placement.shards.len(),
        }
    }

    pub(crate) fn is_under_redundant(&self) -> bool {
        self.available_count() < self.required_count()
    }

    /// Enforce the response boundary using exact per-node durable evidence.
    ///
    /// Logical metadata always needs its independent quorum. Both durability
    /// choices require the explicit upload source's complete content;
    /// `REPLICATED` additionally requires all selected small-copy owners, two
    /// selected complete owners for an undersized large payload, or `K + 1`
    /// correctly placed final shard ordinals.
    pub(crate) fn require_ready(
        &self,
        durability: Durability,
        metadata_quorum: bool,
        upload_source: NodeId,
        evidence: &[NodePayloadEvidence],
    ) -> Result<(), PayloadReadinessError> {
        if !metadata_quorum {
            return Err(PayloadReadinessError::MetadataQuorum);
        }
        ensure_distinct_node_evidence(evidence)?;
        if !evidence
            .iter()
            .any(|entry| entry.node_id == upload_source && entry.complete_copy)
        {
            return Err(PayloadReadinessError::UploadSource);
        }
        if durability == Durability::Local {
            return Ok(());
        }

        match self {
            Self::Small(placement) => {
                let durable = placement
                    .owners
                    .iter()
                    .filter(|owner| {
                        evidence
                            .iter()
                            .any(|entry| entry.node_id == **owner && entry.complete_copy)
                    })
                    .count();
                // Final placement converges to M+1 copies. In an undersized
                // membership, REPLICATED waits for every available selected
                // owner but still requires two distinct copies so one owner
                // can be lost.
                let required = placement.owners.len().max(2);
                if durable < required {
                    return Err(PayloadReadinessError::CompleteCopies { required, durable });
                }
            }
            Self::LargeComplete(placement) => {
                let durable = placement
                    .owners
                    .iter()
                    .filter(|owner| {
                        evidence
                            .iter()
                            .any(|entry| entry.node_id == **owner && entry.complete_copy)
                    })
                    .count();
                // In the complete-copy fallback, REPLICATED proves that one
                // selected owner may be lost. Background placement still
                // converges to M+1 copies when the undersized membership has
                // that many nodes.
                const REQUIRED_REPLICATED_COPIES: usize = 2;
                if durable < REQUIRED_REPLICATED_COPIES {
                    return Err(PayloadReadinessError::CompleteCopies {
                        required: REQUIRED_REPLICATED_COPIES,
                        durable,
                    });
                }
            }
            Self::Large(placement) => {
                let durable = placement
                    .shards
                    .iter()
                    .filter(|expected| {
                        evidence.iter().any(|entry| {
                            entry.node_id == expected.owner
                                && entry.shard_ordinal == Some(expected.ordinal)
                        })
                    })
                    .count();
                if durable < placement.replicated_shards {
                    return Err(PayloadReadinessError::FinalShards {
                        required: placement.replicated_shards,
                        durable,
                    });
                }
            }
        }
        Ok(())
    }
}

/// Durable artifact evidence returned by one exact placement owner.
///
/// One node can hold the coordinator's complete upload source and its one
/// assigned final shard. It cannot claim multiple final ordinals in one
/// record, preserving the distinct-owner placement invariant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NodePayloadEvidence {
    node_id: NodeId,
    complete_copy: bool,
    shard_ordinal: Option<u16>,
}

impl NodePayloadEvidence {
    pub(crate) const fn new(
        node_id: NodeId,
        complete_copy: bool,
        shard_ordinal: Option<u16>,
    ) -> Self {
        Self {
            node_id,
            complete_copy,
            shard_ordinal,
        }
    }

    pub(crate) const fn node_id(self) -> NodeId {
        self.node_id
    }

    pub(crate) const fn complete_copy(self) -> bool {
        self.complete_copy
    }

    pub(crate) const fn shard_ordinal(self) -> Option<u16> {
        self.shard_ordinal
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum PayloadReadinessError {
    #[error("logical metadata quorum is not durable")]
    MetadataQuorum,
    #[error("the upload source has not durably sealed the complete content")]
    UploadSource,
    #[error("node {node_id:?} supplied contradictory payload evidence")]
    ContradictoryNode { node_id: NodeId },
    #[error("only {durable} of {required} required complete copies are durable")]
    CompleteCopies { required: usize, durable: usize },
    #[error("only {durable} of {required} required final shards are durable")]
    FinalShards { required: usize, durable: usize },
}

fn ensure_distinct_node_evidence(
    evidence: &[NodePayloadEvidence],
) -> Result<(), PayloadReadinessError> {
    let mut seen = HashSet::with_capacity(evidence.len());
    for entry in evidence {
        if !seen.insert(entry.node_id) {
            return Err(PayloadReadinessError::ContradictoryNode {
                node_id: entry.node_id,
            });
        }
    }
    Ok(())
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

/// Complete-copy owners for a large payload in an undersized cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompletePayloadPlacement {
    required_copies: usize,
    owners: Vec<NodeId>,
}

impl CompletePayloadPlacement {
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
    replicated_shards: usize,
    shards: Vec<ShardPlacement>,
}

impl LargePayloadPlacement {
    pub(crate) fn shards(&self) -> &[ShardPlacement] {
        &self.shards
    }
}

/// Select the desired payload owners from the active placement set.
///
/// The content size selects the protocol's fixed payload placement domain.
/// Input nodes are defensively deduplicated by stable node ID before an owner
/// is assigned.
pub(crate) fn select_payload_placement(
    cluster_id: ClusterId,
    content: &BlobRef,
    profile: ErasureProfile,
    active_nodes: &[PlacementNode],
) -> PayloadPlacement {
    let key = content_placement_key(content);
    let placement_kind = if content.length <= SMALL_BLOB_MAX_BYTES as u64 {
        PlacementKind::SmallContent
    } else {
        PlacementKind::LargeFragment
    };
    let ranked = distinct_ranked_nodes(placement_kind, cluster_id, &key, active_nodes);

    if content.length <= SMALL_BLOB_MAX_BYTES as u64 {
        let required_copies = usize::from(profile.parity_shards()) + 1;
        let owners = ranked.into_iter().take(required_copies).collect();
        PayloadPlacement::Small(SmallPayloadPlacement {
            required_copies,
            owners,
        })
    } else if ranked.len() < usize::from(profile.total_shards()) {
        let required_copies = usize::from(profile.parity_shards()) + 1;
        let owners = ranked.into_iter().take(required_copies).collect();
        PayloadPlacement::LargeComplete(CompletePayloadPlacement {
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
            replicated_shards: usize::from(profile.data_shards()) + 1,
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
    placement_kind: PlacementKind,
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

    fn large_complete_owners(placement: PayloadPlacement) -> Vec<u64> {
        let PayloadPlacement::LargeComplete(placement) = placement else {
            panic!("expected large complete-copy placement")
        };
        placement.owners().iter().map(|owner| owner.0).collect()
    }

    #[test]
    fn small_weighted_vector_is_frozen_and_input_order_independent() {
        let content = content(SMALL_BLOB_MAX_BYTES as u64);
        let first = nodes();
        let mut reversed = first.clone();
        reversed.reverse();

        let first = select_payload_placement(cluster_id(), &content, profile(), &first);
        let reversed = select_payload_placement(cluster_id(), &content, profile(), &reversed);

        assert_eq!(small_owners(first), [3, 13, 29]);
        assert_eq!(small_owners(reversed), [3, 13, 29]);
    }

    #[test]
    fn large_weighted_ordinal_vector_is_frozen() {
        let placement = select_payload_placement(
            cluster_id(),
            &content(SMALL_BLOB_MAX_BYTES as u64 + 1),
            profile(),
            &nodes(),
        );

        assert_eq!(
            large_assignments(placement),
            [(0, 13), (1, 3), (2, 7), (3, 41), (4, 61), (5, 53)]
        );
    }

    #[test]
    fn insufficient_membership_uses_complete_copies_for_large_payloads() {
        let active = [node(2, 1_000_000), node(5, 2_000_000)];
        let small = select_payload_placement(cluster_id(), &content(17), profile(), &active);
        let large = select_payload_placement(
            cluster_id(),
            &content(SMALL_BLOB_MAX_BYTES as u64 + 1),
            profile(),
            &active,
        );

        assert_eq!((small.required_count(), small.available_count()), (3, 2));
        assert!(small.is_under_redundant());
        assert_eq!((large.required_count(), large.available_count()), (3, 2));
        assert!(large.is_under_redundant());
        assert_eq!(large_complete_owners(large).len(), 2);
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
            cluster_id(),
            &content(SMALL_BLOB_MAX_BYTES as u64 + 1),
            profile(),
            &active,
        );
        let owners = large_complete_owners(placement);
        let distinct = owners.iter().copied().collect::<HashSet<_>>();

        assert_eq!(owners.len(), 3);
        assert_eq!(distinct.len(), owners.len());
    }

    #[test]
    fn complete_membership_meets_m_plus_one_and_k_plus_m_counts() {
        let active = nodes();
        let small = select_payload_placement(cluster_id(), &content(1), profile(), &active);
        let large = select_payload_placement(
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

    #[test]
    fn local_requires_metadata_and_the_explicit_upload_source_only() {
        let placement = select_payload_placement(
            cluster_id(),
            &content(SMALL_BLOB_MAX_BYTES as u64 + 1),
            profile(),
            &nodes(),
        );
        let upload_source = NodeId(97);
        let source = [NodePayloadEvidence::new(upload_source, true, None)];

        assert_eq!(
            placement.require_ready(Durability::Local, false, upload_source, &source),
            Err(PayloadReadinessError::MetadataQuorum)
        );
        assert_eq!(
            placement.require_ready(Durability::Local, true, upload_source, &[]),
            Err(PayloadReadinessError::UploadSource)
        );
        assert_eq!(
            placement.require_ready(Durability::Local, true, upload_source, &source),
            Ok(())
        );
    }

    #[test]
    fn replicated_small_requires_every_m_plus_one_selected_owner() {
        let placement = select_payload_placement(cluster_id(), &content(11), profile(), &nodes());
        let PayloadPlacement::Small(small) = &placement else {
            panic!("expected small placement")
        };
        let mut evidence = small
            .owners()
            .iter()
            .copied()
            .map(|owner| NodePayloadEvidence::new(owner, true, None))
            .collect::<Vec<_>>();
        let upload_source = small.owners()[0];

        assert_eq!(
            placement.require_ready(Durability::Replicated, true, upload_source, &evidence),
            Ok(())
        );
        evidence.pop();
        assert_eq!(
            placement.require_ready(Durability::Replicated, true, upload_source, &evidence),
            Err(PayloadReadinessError::CompleteCopies {
                required: 3,
                durable: 2,
            })
        );
    }

    #[test]
    fn replicated_small_uses_every_available_owner_below_m_plus_one() {
        let active = [node(2, 1_000_000), node(5, 1_000_000)];
        let placement = select_payload_placement(
            cluster_id(),
            &content(11),
            ErasureProfile::new(4, 2, 16 * 1024).unwrap(),
            &active,
        );
        let PayloadPlacement::Small(small) = &placement else {
            panic!("expected small placement")
        };
        let evidence = small
            .owners()
            .iter()
            .copied()
            .map(|owner| NodePayloadEvidence::new(owner, true, None))
            .collect::<Vec<_>>();

        assert_eq!(small.required_copies, 3);
        assert_eq!(small.owners().len(), 2);
        assert_eq!(
            placement.require_ready(Durability::Replicated, true, small.owners()[0], &evidence,),
            Ok(())
        );
        assert_eq!(
            placement.require_ready(
                Durability::Replicated,
                true,
                small.owners()[0],
                &evidence[..1],
            ),
            Err(PayloadReadinessError::CompleteCopies {
                required: 2,
                durable: 1,
            })
        );
    }

    #[test]
    fn default_two_plus_one_requires_three_distinct_final_shards() {
        let placement = select_payload_placement(
            cluster_id(),
            &content(SMALL_BLOB_MAX_BYTES as u64 + 1),
            ErasureProfile::default(),
            &nodes(),
        );
        let PayloadPlacement::Large(large) = &placement else {
            panic!("expected large placement")
        };
        assert_eq!(large.replicated_shards, 3);
        let evidence = large
            .shards()
            .iter()
            .map(|shard| {
                NodePayloadEvidence::new(shard.owner(), shard.ordinal() == 0, Some(shard.ordinal()))
            })
            .collect::<Vec<_>>();
        let upload_source = large.shards()[0].owner();

        assert_eq!(
            placement.require_ready(Durability::Replicated, true, upload_source, &evidence),
            Ok(())
        );
        assert_eq!(
            placement.require_ready(Durability::Replicated, true, upload_source, &evidence[..2],),
            Err(PayloadReadinessError::FinalShards {
                required: 3,
                durable: 2,
            })
        );
    }

    #[test]
    fn default_profile_grows_from_one_to_two_complete_copies_then_shards() {
        let reference = content(SMALL_BLOB_MAX_BYTES as u64 + 1);
        let one = [node(2, 1_000_000)];
        let two = [node(2, 1_000_000), node(5, 1_000_000)];
        let three = [node(2, 1_000_000), node(5, 1_000_000), node(11, 1_000_000)];

        assert_eq!(
            large_complete_owners(select_payload_placement(
                cluster_id(),
                &reference,
                ErasureProfile::default(),
                &one,
            )),
            [2]
        );
        assert_eq!(
            large_complete_owners(select_payload_placement(
                cluster_id(),
                &reference,
                ErasureProfile::default(),
                &two,
            ))
            .len(),
            2
        );
        assert_eq!(
            large_assignments(select_payload_placement(
                cluster_id(),
                &reference,
                ErasureProfile::default(),
                &three,
            ))
            .len(),
            3
        );
    }

    #[test]
    fn large_complete_replicated_requires_two_distinct_selected_copies() {
        let reference = content(SMALL_BLOB_MAX_BYTES as u64 + 1);
        let active = [node(2, 1_000_000), node(5, 1_000_000)];
        let placement =
            select_payload_placement(cluster_id(), &reference, ErasureProfile::default(), &active);
        let PayloadPlacement::LargeComplete(complete) = &placement else {
            panic!("expected complete-copy fallback")
        };
        let upload_source = complete.owners()[0];
        let one = [NodePayloadEvidence::new(upload_source, true, None)];

        assert_eq!(
            placement.require_ready(Durability::Replicated, true, upload_source, &one),
            Err(PayloadReadinessError::CompleteCopies {
                required: 2,
                durable: 1,
            })
        );
        let two = complete
            .owners()
            .iter()
            .copied()
            .map(|owner| NodePayloadEvidence::new(owner, true, None))
            .collect::<Vec<_>>();
        assert_eq!(
            placement.require_ready(Durability::Replicated, true, upload_source, &two),
            Ok(())
        );
    }

    #[test]
    fn wrong_ordinal_and_duplicate_node_evidence_never_inflate_readiness() {
        let placement = select_payload_placement(
            cluster_id(),
            &content(SMALL_BLOB_MAX_BYTES as u64 + 1),
            ErasureProfile::default(),
            &nodes(),
        );
        let PayloadPlacement::Large(large) = &placement else {
            panic!("expected large placement")
        };
        let mut evidence = large
            .shards()
            .iter()
            .map(|shard| {
                NodePayloadEvidence::new(shard.owner(), shard.ordinal() == 0, Some(shard.ordinal()))
            })
            .collect::<Vec<_>>();
        let upload_source = large.shards()[0].owner();
        evidence[2].shard_ordinal = Some(1);
        assert_eq!(
            placement.require_ready(Durability::Replicated, true, upload_source, &evidence),
            Err(PayloadReadinessError::FinalShards {
                required: 3,
                durable: 2,
            })
        );

        evidence.push(evidence[0]);
        assert_eq!(
            placement.require_ready(Durability::Replicated, true, upload_source, &evidence),
            Err(PayloadReadinessError::ContradictoryNode {
                node_id: evidence[0].node_id,
            })
        );
    }
}
