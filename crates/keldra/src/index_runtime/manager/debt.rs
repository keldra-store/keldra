//! Deterministic immutable-segment compaction debt selection.

use std::collections::BTreeMap;

use keldra_index::v4::SegmentDescriptor;

use super::super::committed_view::{
    LocatorRoot, MAX_LOCATOR_ROOTS_PER_COMMIT, MAX_SEGMENTS_PER_COMMIT,
};

pub(super) const PREFERRED_FAN_IN: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DebtLimits {
    pub(super) maximum_segments: usize,
    pub(super) maximum_bytes: u64,
}

impl DebtLimits {
    pub(super) const fn new(maximum_segments: usize, maximum_bytes: u64) -> Self {
        Self {
            maximum_segments,
            maximum_bytes,
        }
    }

    pub(super) const fn maintenance() -> Self {
        Self::new(PREFERRED_FAN_IN, u64::MAX)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DebtSelection {
    pub(super) tier: u8,
    pub(super) input_segments: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct LocatorDebtSelection {
    pub(super) input_roots: usize,
}

pub(super) fn select(segments: &[SegmentDescriptor], limits: DebtLimits) -> Option<DebtSelection> {
    let summaries = segments
        .iter()
        .map(|segment| {
            (
                segment_size_tier(segment.encoded_bytes),
                segment.identity.segment_id,
                segment.encoded_bytes,
            )
        })
        .collect::<Vec<_>>();
    select_summaries(summaries.iter().copied(), limits).or_else(|| {
        (segments.len() >= MAX_SEGMENTS_PER_COMMIT.saturating_sub(1))
            .then(|| select_headroom_summaries(summaries))
            .flatten()
    })
}

pub(super) fn select_before_locator_limit(
    segments: &[SegmentDescriptor],
    locator_roots: usize,
    limits: DebtLimits,
) -> Option<DebtSelection> {
    (!locator_headroom_requires_compaction(locator_roots))
        .then(|| select(segments, limits))
        .flatten()
}

pub(super) const fn locator_headroom_requires_compaction(locator_roots: usize) -> bool {
    locator_roots >= MAX_LOCATOR_ROOTS_PER_COMMIT.saturating_sub(1)
}

fn select_headroom_summaries(
    summaries: impl IntoIterator<Item = (u8, u64, u64)>,
) -> Option<DebtSelection> {
    let mut tiers = BTreeMap::<u8, usize>::new();
    for (tier, _, _) in summaries {
        *tiers.entry(tier).or_default() += 1;
    }
    tiers
        .into_iter()
        .find(|(_, count)| *count >= 2)
        .map(|(tier, count)| DebtSelection {
            tier,
            input_segments: count.min(PREFERRED_FAN_IN),
        })
}

/// Power-of-two encoded-size tier. It is derived from immutable descriptor
/// bytes, so every node makes the same selection without persistent merge
/// tiers or another source of truth.
pub(super) fn segment_size_tier(encoded_bytes: u64) -> u8 {
    encoded_bytes.max(1).ilog2() as u8
}

/// Select an oldest contiguous locator prefix. The first root is the compacted
/// baseline; only subsequent roots are unmerged bytes. Replacing a prefix with
/// its maximum sequence preserves equal-version relocation order without
/// storing a sequence on every locator entry.
pub(super) fn select_locator_roots(
    roots: &[LocatorRoot],
    limits: DebtLimits,
) -> Option<LocatorDebtSelection> {
    select_locator_summaries(roots.iter().map(|root| root.encoded_bytes), limits)
}

fn select_locator_summaries(
    encoded_bytes: impl IntoIterator<Item = u64>,
    limits: DebtLimits,
) -> Option<LocatorDebtSelection> {
    let roots = encoded_bytes.into_iter().collect::<Vec<_>>();
    if roots.len() < 2 {
        return None;
    }
    // Keep one slot below the durable-format ceiling so a newly sealed delta
    // always has room to enter the in-memory candidate before debt is repaid.
    let maximum_roots = limits
        .maximum_segments
        .min(MAX_LOCATOR_ROOTS_PER_COMMIT.saturating_sub(1));
    let count_debt = roots.len() >= maximum_roots;
    let unmerged_bytes = roots
        .iter()
        .skip(1)
        .fold(0_u64, |total, bytes| total.saturating_add(*bytes));
    let byte_debt = unmerged_bytes > limits.maximum_bytes;
    (count_debt || byte_debt).then_some(LocatorDebtSelection {
        input_roots: roots.len().min(PREFERRED_FAN_IN),
    })
}

fn select_summaries(
    summaries: impl IntoIterator<Item = (u8, u64, u64)>,
    limits: DebtLimits,
) -> Option<DebtSelection> {
    let mut tiers = BTreeMap::<u8, Vec<(u64, u64)>>::new();
    for (tier, sequence, bytes) in summaries {
        tiers.entry(tier).or_default().push((sequence, bytes));
    }
    for (tier, mut segments) in tiers {
        segments.sort_unstable_by_key(|(sequence, _)| *sequence);
        let count_debt = segments.len() > limits.maximum_segments;
        let bytes = segments
            .iter()
            .fold(0_u64, |total, (_, bytes)| total.saturating_add(*bytes));
        let byte_debt = segments.len() >= 2 && bytes > limits.maximum_bytes;
        if count_debt || byte_debt {
            return Some(DebtSelection {
                tier,
                input_segments: segments.len().min(PREFERRED_FAN_IN),
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use keldra_index::v4::SegmentIdentity;

    fn segment(id: u64, encoded_bytes: u64) -> SegmentDescriptor {
        SegmentDescriptor {
            identity: SegmentIdentity::new(1, 1, [1; 32], id).unwrap(),
            document_count: 1,
            live_document_count: 1,
            packs: Vec::new(),
            components: Vec::new(),
            encoded_bytes,
            logical_bytes: encoded_bytes,
        }
    }

    #[test]
    fn count_debt_selects_lowest_tier_and_preferred_fan_in() {
        let segments = (1..=5)
            .map(|sequence| (0, sequence, 1))
            .chain((6..=10).map(|sequence| (1, sequence, 1)));
        assert_eq!(
            select_summaries(segments, DebtLimits::new(4, u64::MAX)),
            Some(DebtSelection {
                tier: 0,
                input_segments: 4,
            })
        );
    }

    #[test]
    fn byte_debt_compacts_two_to_four_oldest_runs() {
        assert_eq!(
            select_summaries([(0, 1, 60), (0, 2, 50)], DebtLimits::new(64, 100)),
            Some(DebtSelection {
                tier: 0,
                input_segments: 2,
            })
        );
        assert_eq!(
            select_summaries(
                [(0, 1, 30), (0, 2, 30), (0, 3, 30), (0, 4, 30), (0, 5, 1)],
                DebtLimits::new(64, 100),
            ),
            Some(DebtSelection {
                tier: 0,
                input_segments: 4,
            })
        );
    }

    #[test]
    fn one_indivisible_oversized_run_is_allowed() {
        assert_eq!(
            select_summaries([(0, 1, 101)], DebtLimits::new(64, 100)),
            None
        );
        assert_eq!(
            select_summaries([(0, 1, 101), (0, 2, 1)], DebtLimits::new(64, 100)),
            Some(DebtSelection {
                tier: 0,
                input_segments: 2,
            })
        );
    }

    #[test]
    fn format_headroom_forces_a_merge_before_the_last_slot_is_consumed() {
        assert_eq!(
            select_headroom_summaries([(1, 1, 2), (1, 2, 2), (2, 3, 4)]),
            Some(DebtSelection {
                tier: 1,
                input_segments: 2,
            })
        );
    }

    #[test]
    fn exhausted_locator_headroom_prioritizes_locator_compaction() {
        let segments = [segment(1, 1), segment(2, 1)];

        assert_eq!(
            select_before_locator_limit(
                &segments,
                MAX_LOCATOR_ROOTS_PER_COMMIT - 1,
                DebtLimits::maintenance(),
            ),
            None
        );
        assert_eq!(
            select_locator_summaries(
                std::iter::repeat_n(1, MAX_LOCATOR_ROOTS_PER_COMMIT - 1),
                DebtLimits::new(MAX_LOCATOR_ROOTS_PER_COMMIT, u64::MAX),
            ),
            Some(LocatorDebtSelection {
                input_roots: PREFERRED_FAN_IN,
            })
        );
    }

    #[test]
    fn a_one_run_limit_compacts_two_runs_and_can_make_progress() {
        assert_eq!(
            select_summaries([(0, 1, 10), (0, 2, 10)], DebtLimits::new(1, u64::MAX)),
            Some(DebtSelection {
                tier: 0,
                input_segments: 2,
            })
        );
    }

    #[test]
    fn locator_debt_treats_the_oldest_root_as_the_compacted_baseline() {
        assert_eq!(
            select_locator_summaries([1_000, 40, 70], DebtLimits::new(4, 100)),
            Some(LocatorDebtSelection { input_roots: 3 })
        );
        assert_eq!(
            select_locator_summaries([10_000], DebtLimits::new(4, 100)),
            None
        );
        assert_eq!(
            select_locator_summaries([10_000, 40, 60], DebtLimits::new(4, 100)),
            None
        );
    }

    #[test]
    fn locator_count_debt_selects_a_small_oldest_prefix() {
        assert_eq!(
            select_locator_summaries([1, 1, 1, 1, 1], DebtLimits::new(4, u64::MAX)),
            Some(LocatorDebtSelection { input_roots: 4 })
        );
        assert_eq!(
            select_locator_summaries([1, 1], DebtLimits::new(1, u64::MAX)),
            Some(LocatorDebtSelection { input_roots: 2 })
        );
    }
}
