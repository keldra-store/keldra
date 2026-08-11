//! Deterministic capacity-weighted rendezvous placement.
//!
//! Placement is derived locally from committed cluster membership. Nothing in
//! this module persists an ownership decision or changes routing.

use std::cmp::Ordering;
use std::num::NonZeroU32;

use anvil_consensus::{ClusterId, NodeId};

const HASH_CONTEXT: &str = "anvil.storage/weighted-hrw/v1";

/// Stable domain byte in the weighted-HRW wire tuple.
///
/// These values are part of the 0.5.1 placement format. Reserved future
/// values are named now so a later capability cannot accidentally reuse an
/// existing ownership domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum PlacementKind {
    TenantNameClaim = 1,
    TenantOrBucketRecord = 2,
    Object = 3,
    ZanzibarRealm = 4,
    Credential = 5,
    SmallContent = 6,
    LargeFragment = 7,
    FuturePersonalDb = 8,
    FutureIndex = 9,
    AccountingMatcher = 10,
}

impl PlacementKind {
    const fn wire_byte(self) -> u8 {
        self as u8
    }
}

/// One active placement candidate and its configured storage-capacity ratio.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PlacementNode {
    node_id: NodeId,
    weight_millionths: NonZeroU32,
}

impl PlacementNode {
    /// Construct a candidate; `1_000_000` represents a capacity weight of 1.0.
    pub(crate) fn new(node_id: NodeId, weight_millionths: NonZeroU32) -> Self {
        Self {
            node_id,
            weight_millionths,
        }
    }

    pub(crate) fn node_id(self) -> NodeId {
        self.node_id
    }
}

/// Rank active nodes by descending weighted-HRW score.
///
/// Callers provide one candidate per active node. Lower node ID is the final
/// tie-break, making even a quantized-score tie deterministic.
pub(crate) fn rank_nodes(
    placement_kind: PlacementKind,
    cluster_id: ClusterId,
    key: &[u8],
    nodes: &[PlacementNode],
) -> Vec<PlacementNode> {
    let mut scored = nodes
        .iter()
        .copied()
        .map(|node| ScoredNode {
            denominator: score_denominator(placement_kind, cluster_id, key, node.node_id),
            node,
        })
        .collect::<Vec<_>>();

    scored.sort_unstable_by(compare_score);
    scored.into_iter().map(|candidate| candidate.node).collect()
}

#[derive(Clone, Copy, Debug)]
struct ScoredNode {
    node: PlacementNode,
    /// Q64.64 `-log2(H)`, clamped to one least-significant bit.
    denominator: u128,
}

fn compare_score(left: &ScoredNode, right: &ScoredNode) -> Ordering {
    // weight / denominator, compared without division or floating point.
    let left_cross = u128::from(left.node.weight_millionths.get()) * right.denominator;
    let right_cross = u128::from(right.node.weight_millionths.get()) * left.denominator;

    right_cross
        .cmp(&left_cross)
        .then_with(|| left.node.node_id.cmp(&right.node.node_id))
}

fn score_denominator(
    placement_kind: PlacementKind,
    cluster_id: ClusterId,
    key: &[u8],
    node_id: NodeId,
) -> u128 {
    let midpoint = hash_midpoint(placement_kind, cluster_id, key, node_id);
    negative_log2_q64(midpoint)
}

/// Return `r` for the exact open-interval midpoint `H = (2r + 1) / 2^65`.
fn hash_midpoint(
    placement_kind: PlacementKind,
    cluster_id: ClusterId,
    key: &[u8],
    node_id: NodeId,
) -> u64 {
    let mut hasher = blake3::Hasher::new_derive_key(HASH_CONTEXT);
    hasher.update(&[placement_kind.wire_byte()]);
    hasher.update(&cluster_id.into_bytes());
    hasher.update(
        &u64::try_from(key.len())
            .expect("slice length fits u64")
            .to_be_bytes(),
    );
    hasher.update(key);
    hasher.update(&node_id.0.to_be_bytes());

    let mut first_word = [0_u8; 8];
    first_word.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    u64::from_be_bytes(first_word)
}

/// Calculate Q64.64 `-log2((2r + 1) / 2^65)` using exactly 64 integer rounds.
fn negative_log2_q64(r: u64) -> u128 {
    let numerator = (u128::from(r) << 1) | 1;
    let significant_bits = u128::BITS - numerator.leading_zeros();
    let integer_exponent = 66 - significant_bits;

    // Normalize the exact 65-bit-or-smaller numerator to Q2.126 in [1, 2),
    // retaining 62 guard bits beyond the output width.
    let mut normalized = numerator << (127 - significant_bits);
    let mut fractional_log2 = 0_u64;

    for output_bit in (0..64).rev() {
        let square = square_u128(normalized);
        let at_least_two = square[3] & (1_u64 << 61) != 0;
        normalized = shift_product_right(square, if at_least_two { 127 } else { 126 });
        if at_least_two {
            fractional_log2 |= 1_u64 << output_bit;
        }
    }

    let rounded_up = ((u128::from(integer_exponent)) << 64) - u128::from(fractional_log2);
    // The loop truncates log2(H)'s fractional magnitude. Subtracting it would
    // therefore round -log2(H) up. Every odd numerator other than one has a
    // non-zero discarded remainder, so subtract one LSB to specify floor
    // rounding for the final denominator.
    let rounded_down = if numerator == 1 {
        rounded_up
    } else {
        rounded_up.saturating_sub(1)
    };
    rounded_down.max(1)
}

/// Full-width square, returned as little-endian 64-bit limbs.
fn square_u128(value: u128) -> [u64; 4] {
    let low = value as u64;
    let high = (value >> 64) as u64;

    let low_square = u128::from(low) * u128::from(low);
    let cross = u128::from(low) * u128::from(high);
    let high_square = u128::from(high) * u128::from(high);

    let limb0 = low_square as u64;
    let middle = (low_square >> 64) + u128::from(cross as u64) * 2;
    let limb1 = middle as u64;
    let upper = (cross >> 64) * 2 + u128::from(high_square as u64) + (middle >> 64);
    let limb2 = upper as u64;
    let limb3 = ((high_square >> 64) + (upper >> 64)) as u64;

    [limb0, limb1, limb2, limb3]
}

/// Extract the low 128 bits after shifting a 256-bit product by 126 or 127.
fn shift_product_right(product: [u64; 4], shift: u32) -> u128 {
    debug_assert!(matches!(shift, 126 | 127));
    let within_limb = shift - 64;
    let low = (product[1] >> within_limb) | (product[2] << (64 - within_limb));
    let high = (product[2] >> within_limb) | (product[3] << (64 - within_limb));
    (u128::from(high) << 64) | u128::from(low)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn node(node_id: u64, weight_millionths: u32) -> PlacementNode {
        PlacementNode::new(
            NodeId(node_id),
            NonZeroU32::new(weight_millionths).expect("test weight must be positive"),
        )
    }

    #[test]
    fn placement_domain_bytes_are_frozen() {
        assert_eq!(PlacementKind::TenantNameClaim.wire_byte(), 1);
        assert_eq!(PlacementKind::TenantOrBucketRecord.wire_byte(), 2);
        assert_eq!(PlacementKind::Object.wire_byte(), 3);
        assert_eq!(PlacementKind::ZanzibarRealm.wire_byte(), 4);
        assert_eq!(PlacementKind::Credential.wire_byte(), 5);
        assert_eq!(PlacementKind::SmallContent.wire_byte(), 6);
        assert_eq!(PlacementKind::LargeFragment.wire_byte(), 7);
        assert_eq!(PlacementKind::FuturePersonalDb.wire_byte(), 8);
        assert_eq!(PlacementKind::FutureIndex.wire_byte(), 9);
    }

    fn ids(nodes: Vec<PlacementNode>) -> Vec<u64> {
        nodes
            .into_iter()
            .map(|candidate| candidate.node_id().0)
            .collect()
    }

    #[test]
    fn binary_log_boundary_vectors_are_frozen() {
        assert_eq!(negative_log2_q64(0), 65_u128 << 64);
        assert_eq!(negative_log2_q64(1_u64 << 63), (1_u128 << 64) - 2);
        assert_eq!(negative_log2_q64(u64::MAX), 1);
    }

    #[test]
    fn wide_square_and_shift_vectors_are_frozen() {
        let vectors = [
            (
                0x4000_0000_0000_0000_0000_0000_0000_0000_u128,
                [0, 0, 0, 0x1000_0000_0000_0000],
                0x4000_0000_0000_0000_0000_0000_0000_0000_u128,
                0x2000_0000_0000_0000_0000_0000_0000_0000_u128,
            ),
            (
                0x7fff_ffff_ffff_ffff_ffff_ffff_ffff_ffff_u128,
                [1, 0, u64::MAX, 0x3fff_ffff_ffff_ffff],
                0xffff_ffff_ffff_ffff_ffff_ffff_ffff_fffc_u128,
                0x7fff_ffff_ffff_ffff_ffff_ffff_ffff_fffe_u128,
            ),
            (
                0x5a17_c9e4_3b8d_6f20_7135_79bd_f024_68ac_u128,
                [
                    0x1131_6f68_1b2c_3390,
                    0x330d_411b_81e2_006a,
                    0x01b7_5421_9b90_5edf,
                    0x1fb4_bc2a_601a_568b,
                ],
                0x7ed2_f0a9_8069_5a2c_06dd_5086_6e41_7b7c_u128,
                0x3f69_7854_c034_ad16_036e_a843_3720_bdbe_u128,
            ),
        ];

        for (input, expected_square, shifted_126, shifted_127) in vectors {
            let square = square_u128(input);
            assert_eq!(square, expected_square);
            assert_eq!(shift_product_right(square, 126), shifted_126);
            assert_eq!(shift_product_right(square, 127), shifted_127);
        }
    }

    #[test]
    fn wide_square_matches_native_arithmetic_for_small_operands() {
        let mut value = 0x9e37_79b9_7f4a_7c15_u64;
        for _ in 0..1_000 {
            value ^= value << 13;
            value ^= value >> 7;
            value ^= value << 17;

            let expected = u128::from(value) * u128::from(value);
            let square = square_u128(u128::from(value));
            assert_eq!(square, [expected as u64, (expected >> 64) as u64, 0, 0]);
            assert_eq!(shift_product_right(square, 126), expected >> 126);
            assert_eq!(shift_product_right(square, 127), expected >> 127);
        }
    }

    #[test]
    fn score_and_ranking_vectors_are_frozen() {
        let cluster = ClusterId(*b"anvil-cluster-v1");
        let key = b"tenant/42/bucket/7/object/path";

        let vectors = [
            (
                1,
                0x76b9_2592_2a80_df32_u64,
                20_448_982_601_595_807_023_u128,
            ),
            (2, 0xed73_a158_67f9_a9b4_u64, 2_001_653_295_708_815_457_u128),
            (
                17,
                0xc4ec_4780_4f9a_6503_u64,
                6_982_322_030_742_116_481_u128,
            ),
            (
                1_023,
                0xd749_de28_b19c_b160_u64,
                4_609_329_328_017_615_214_u128,
            ),
        ];

        for (node_id, expected_hash_word, expected_denominator) in vectors {
            assert_eq!(
                hash_midpoint(PlacementKind::Object, cluster, key, NodeId(node_id)),
                expected_hash_word
            );
            assert_eq!(
                score_denominator(PlacementKind::Object, cluster, key, NodeId(node_id)),
                expected_denominator
            );
        }

        let ranked = rank_nodes(
            PlacementKind::Object,
            cluster,
            key,
            &[
                node(17, 500_000),
                node(1_023, 2_000_000),
                node(2, 1_000_000),
                node(1, 1_000_000),
            ],
        );
        assert_eq!(ids(ranked), [2, 1_023, 17, 1]);
    }

    #[test]
    fn rank_is_independent_of_input_order() {
        let cluster = ClusterId([7; 16]);
        let first = [node(4, 500_000), node(1, 1_000_000), node(9, 2_000_000)];
        let second = [first[2], first[0], first[1]];

        assert_eq!(
            ids(rank_nodes(
                PlacementKind::FuturePersonalDb,
                cluster,
                b"stable-key",
                &first,
            )),
            ids(rank_nodes(
                PlacementKind::FuturePersonalDb,
                cluster,
                b"stable-key",
                &second,
            ))
        );
    }

    #[test]
    fn configured_weight_controls_deterministic_distribution() {
        let cluster = ClusterId([11; 16]);
        let candidates = [node(1, 1_000_000), node(2, 2_000_000), node(3, 500_000)];
        let mut counts = BTreeMap::<u64, usize>::new();

        for key_number in 0_u64..50_000 {
            let winner = rank_nodes(
                PlacementKind::Credential,
                cluster,
                &key_number.to_be_bytes(),
                &candidates,
            )[0];
            *counts.entry(winner.node_id().0).or_default() += 1;
        }

        // The deterministic corpus should closely follow the exact 2:1:0.5
        // configured ratio without relying on randomness or a flaky tolerance.
        assert_eq!(
            counts,
            BTreeMap::from([(1, 14_188), (2, 28_539), (3, 7_273)])
        );
    }

    #[test]
    fn adding_a_node_only_moves_keys_to_that_node() {
        let cluster = ClusterId([19; 16]);
        let original = [node(1, 1_000_000), node(2, 1_000_000), node(3, 1_000_000)];
        let expanded = [
            node(1, 1_000_000),
            node(2, 1_000_000),
            node(3, 1_000_000),
            node(4, 1_000_000),
        ];
        let mut moved = 0;

        for key_number in 0_u64..20_000 {
            let key = key_number.to_be_bytes();
            let before =
                rank_nodes(PlacementKind::TenantNameClaim, cluster, &key, &original)[0].node_id();
            let after =
                rank_nodes(PlacementKind::TenantNameClaim, cluster, &key, &expanded)[0].node_id();
            if before != after {
                moved += 1;
                assert_eq!(after, NodeId(4));
            }
        }

        assert!(
            moved > 0,
            "the fixed corpus must exercise ownership movement"
        );
    }

    #[test]
    fn lower_node_id_breaks_an_exact_score_tie() {
        let tied = ScoredNode {
            node: node(9, 1_000_000),
            denominator: 42,
        };
        let lower = ScoredNode {
            node: node(3, 1_000_000),
            denominator: 42,
        };

        assert_eq!(compare_score(&lower, &tied), Ordering::Less);
        assert_eq!(compare_score(&tied, &lower), Ordering::Greater);
    }
}
