//! Deterministic immutable-run compaction debt selection.

use std::collections::BTreeMap;

use super::super::generation::ManifestRun;

pub(super) const PREFERRED_FAN_IN: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DebtLimits {
    pub(super) maximum_runs: usize,
    pub(super) maximum_bytes: u64,
}

impl DebtLimits {
    pub(super) const fn new(maximum_runs: usize, maximum_bytes: u64) -> Self {
        Self {
            maximum_runs,
            maximum_bytes,
        }
    }

    pub(super) const fn maintenance() -> Self {
        Self::new(PREFERRED_FAN_IN, u64::MAX)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DebtSelection {
    pub(super) level: u8,
    pub(super) input_runs: usize,
}

pub(super) fn select(runs: &[ManifestRun], limits: DebtLimits) -> Option<DebtSelection> {
    select_summaries(
        runs.iter()
            .map(|run| (run.level, run.sequence, run.authoritative_bytes)),
        limits,
    )
}

fn select_summaries(
    runs: impl IntoIterator<Item = (u8, u64, u64)>,
    limits: DebtLimits,
) -> Option<DebtSelection> {
    let mut levels = BTreeMap::<u8, Vec<(u64, u64)>>::new();
    for (level, sequence, bytes) in runs {
        levels.entry(level).or_default().push((sequence, bytes));
    }
    for (level, mut runs) in levels {
        runs.sort_unstable_by_key(|(sequence, _)| *sequence);
        let count_debt = runs.len() > limits.maximum_runs;
        let bytes = runs
            .iter()
            .fold(0_u64, |total, (_, bytes)| total.saturating_add(*bytes));
        let byte_debt = runs.len() >= 2 && bytes > limits.maximum_bytes;
        if count_debt || byte_debt {
            return Some(DebtSelection {
                level,
                input_runs: runs.len().min(PREFERRED_FAN_IN),
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_debt_selects_lowest_level_and_preferred_fan_in() {
        let runs = (1..=5)
            .map(|sequence| (0, sequence, 1))
            .chain((6..=10).map(|sequence| (1, sequence, 1)));
        assert_eq!(
            select_summaries(runs, DebtLimits::new(4, u64::MAX)),
            Some(DebtSelection {
                level: 0,
                input_runs: 4,
            })
        );
    }

    #[test]
    fn byte_debt_compacts_two_to_four_oldest_runs() {
        assert_eq!(
            select_summaries([(0, 1, 60), (0, 2, 50)], DebtLimits::new(64, 100)),
            Some(DebtSelection {
                level: 0,
                input_runs: 2,
            })
        );
        assert_eq!(
            select_summaries(
                [(0, 1, 30), (0, 2, 30), (0, 3, 30), (0, 4, 30), (0, 5, 1)],
                DebtLimits::new(64, 100),
            ),
            Some(DebtSelection {
                level: 0,
                input_runs: 4,
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
                level: 0,
                input_runs: 2,
            })
        );
    }

    #[test]
    fn a_one_run_limit_compacts_two_runs_and_can_make_progress() {
        assert_eq!(
            select_summaries([(0, 1, 10), (0, 2, 10)], DebtLimits::new(1, u64::MAX)),
            Some(DebtSelection {
                level: 0,
                input_runs: 2,
            })
        );
    }
}
