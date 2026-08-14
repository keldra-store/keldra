//! Deterministic immutable-segment compaction debt selection.

use std::collections::BTreeMap;

use anvil_index::v4::SegmentDescriptor;

use super::super::generation::{LocatorRoot, MAX_LOCATOR_ROOTS_PER_GENERATION};

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
    select_summaries(
        segments.iter().map(|segment| {
            (
                segment_size_tier(segment.encoded_bytes),
                segment.identity.segment_id,
                segment.encoded_bytes,
            )
        }),
        limits,
    )
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
        .min(MAX_LOCATOR_ROOTS_PER_GENERATION.saturating_sub(1));
    let count_debt = roots.len() > maximum_roots;
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
