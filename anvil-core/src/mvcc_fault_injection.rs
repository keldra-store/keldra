//! Deterministic MVCC fault hooks used by the RFC fault matrix.
//!
//! This module is test-only. Hooks fail exact operation ordinals and never use
//! sleeps, wall-clock races, or production feature flags.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Mutex, OnceLock},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultPoint {
    PreparedBundleWrite,
    ShardWrite,
    MvccBatchWrite,
    RaftLogWrite,
    BeforeProposal,
    AfterProposal,
    BeforeApply,
    AfterApply,
    BeforeCompleteAck,
    AfterCompleteAck,
    ReplicationFrame,
    ReplicationHalfOpen,
    MinorityNodeLoss,
    LeaderChange,
    LaggingFollowerGc,
    RepairApply,
    RestartRecovery,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InjectedFault {
    pub point: FaultPoint,
    pub ordinal: u64,
}

impl std::fmt::Display for InjectedFault {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "injected {:?} fault at operation {}",
            self.point, self.ordinal
        )
    }
}

impl std::error::Error for InjectedFault {}

#[derive(Debug, Default)]
pub struct DeterministicFaults {
    calls: BTreeMap<FaultPoint, u64>,
    failures: BTreeSet<(FaultPoint, u64)>,
}

impl DeterministicFaults {
    pub fn fail_at(mut self, point: FaultPoint, ordinal: u64) -> Self {
        assert!(ordinal > 0, "fault ordinal is one-based");
        self.failures.insert((point, ordinal));
        self
    }

    pub fn check(&mut self, point: FaultPoint) -> Result<(), InjectedFault> {
        let ordinal = self.calls.entry(point).or_default();
        *ordinal += 1;
        if self.failures.contains(&(point, *ordinal)) {
            Err(InjectedFault {
                point,
                ordinal: *ordinal,
            })
        } else {
            Ok(())
        }
    }
}

static INSTALLED: OnceLock<Mutex<Option<DeterministicFaults>>> = OnceLock::new();

pub fn install(faults: DeterministicFaults) {
    *INSTALLED.get_or_init(|| Mutex::new(None)).lock().unwrap() = Some(faults);
}

pub fn clear() {
    *INSTALLED.get_or_init(|| Mutex::new(None)).lock().unwrap() = None;
}

pub fn hit(point: FaultPoint) -> Result<(), InjectedFault> {
    let mut installed = INSTALLED.get_or_init(|| Mutex::new(None)).lock().unwrap();
    match installed.as_mut() {
        Some(faults) => faults.check(point),
        None => Ok(()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameAction {
    Deliver,
    Drop,
    Duplicate,
    Hold,
    HalfOpen,
}

#[derive(Debug, Default)]
pub struct FrameFaultPlan {
    actions: VecDeque<FrameAction>,
    held: Vec<Vec<u8>>,
}

impl FrameFaultPlan {
    pub fn new(actions: impl IntoIterator<Item = FrameAction>) -> Self {
        Self {
            actions: actions.into_iter().collect(),
            held: Vec::new(),
        }
    }

    /// Returns zero, one, or two frames. Held frames are emitted in reverse
    /// order by `flush_reordered`, making reordering deterministic.
    pub fn apply(&mut self, frame: Vec<u8>) -> Result<Vec<Vec<u8>>, InjectedFault> {
        match self.actions.pop_front().unwrap_or(FrameAction::Deliver) {
            FrameAction::Deliver => Ok(vec![frame]),
            FrameAction::Drop => Ok(Vec::new()),
            FrameAction::Duplicate => Ok(vec![frame.clone(), frame]),
            FrameAction::Hold => {
                self.held.push(frame);
                Ok(Vec::new())
            }
            FrameAction::HalfOpen => Err(InjectedFault {
                point: FaultPoint::ReplicationHalfOpen,
                ordinal: 1,
            }),
        }
    }

    pub fn flush_reordered(&mut self) -> Vec<Vec<u8>> {
        self.held.drain(..).rev().collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedInvariant {
    NoCommitWithoutDurability,
    NoAckBeforePersistence,
    AtomicVisibility,
    IdempotentReplay,
    NoGcPastPin,
    RepairExactlyOnceEffect,
    RecoverOrRejectTail,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultScenario {
    pub name: &'static str,
    pub point: FaultPoint,
    pub invariant: ExpectedInvariant,
}

pub const RFC_FAULT_MATRIX: &[FaultScenario] = &[
    FaultScenario {
        name: "disk_full_prepared_bundle",
        point: FaultPoint::PreparedBundleWrite,
        invariant: ExpectedInvariant::NoCommitWithoutDurability,
    },
    FaultScenario {
        name: "disk_full_shard",
        point: FaultPoint::ShardWrite,
        invariant: ExpectedInvariant::NoAckBeforePersistence,
    },
    FaultScenario {
        name: "disk_full_mvcc",
        point: FaultPoint::MvccBatchWrite,
        invariant: ExpectedInvariant::AtomicVisibility,
    },
    FaultScenario {
        name: "disk_full_raft",
        point: FaultPoint::RaftLogWrite,
        invariant: ExpectedInvariant::NoCommitWithoutDurability,
    },
    FaultScenario {
        name: "stop_before_proposal",
        point: FaultPoint::BeforeProposal,
        invariant: ExpectedInvariant::NoCommitWithoutDurability,
    },
    FaultScenario {
        name: "stop_after_proposal",
        point: FaultPoint::AfterProposal,
        invariant: ExpectedInvariant::IdempotentReplay,
    },
    FaultScenario {
        name: "stop_before_apply",
        point: FaultPoint::BeforeApply,
        invariant: ExpectedInvariant::AtomicVisibility,
    },
    FaultScenario {
        name: "stop_after_apply",
        point: FaultPoint::AfterApply,
        invariant: ExpectedInvariant::IdempotentReplay,
    },
    FaultScenario {
        name: "stop_before_ack",
        point: FaultPoint::BeforeCompleteAck,
        invariant: ExpectedInvariant::NoAckBeforePersistence,
    },
    FaultScenario {
        name: "stop_after_ack",
        point: FaultPoint::AfterCompleteAck,
        invariant: ExpectedInvariant::IdempotentReplay,
    },
    FaultScenario {
        name: "frames_reordered_duplicated_dropped",
        point: FaultPoint::ReplicationFrame,
        invariant: ExpectedInvariant::IdempotentReplay,
    },
    FaultScenario {
        name: "half_open_stream",
        point: FaultPoint::ReplicationHalfOpen,
        invariant: ExpectedInvariant::NoAckBeforePersistence,
    },
    FaultScenario {
        name: "minority_loss",
        point: FaultPoint::MinorityNodeLoss,
        invariant: ExpectedInvariant::NoCommitWithoutDurability,
    },
    FaultScenario {
        name: "leader_change",
        point: FaultPoint::LeaderChange,
        invariant: ExpectedInvariant::IdempotentReplay,
    },
    FaultScenario {
        name: "lagging_follower_gc",
        point: FaultPoint::LaggingFollowerGc,
        invariant: ExpectedInvariant::NoGcPastPin,
    },
    FaultScenario {
        name: "duplicate_repair",
        point: FaultPoint::RepairApply,
        invariant: ExpectedInvariant::RepairExactlyOnceEffect,
    },
    FaultScenario {
        name: "restart_recovery",
        point: FaultPoint::RestartRecovery,
        invariant: ExpectedInvariant::RecoverOrRejectTail,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_ordinal_failures_are_repeatable() {
        let mut faults = DeterministicFaults::default().fail_at(FaultPoint::MvccBatchWrite, 2);
        assert!(faults.check(FaultPoint::MvccBatchWrite).is_ok());
        assert_eq!(
            faults.check(FaultPoint::MvccBatchWrite),
            Err(InjectedFault {
                point: FaultPoint::MvccBatchWrite,
                ordinal: 2
            })
        );
        assert!(faults.check(FaultPoint::MvccBatchWrite).is_ok());
    }

    #[test]
    fn frame_faults_cover_drop_duplicate_reorder_and_half_open() {
        let mut plan = FrameFaultPlan::new([
            FrameAction::Hold,
            FrameAction::Hold,
            FrameAction::Drop,
            FrameAction::Duplicate,
            FrameAction::HalfOpen,
        ]);
        assert!(plan.apply(vec![1]).unwrap().is_empty());
        assert!(plan.apply(vec![2]).unwrap().is_empty());
        assert!(plan.apply(vec![3]).unwrap().is_empty());
        assert_eq!(plan.apply(vec![4]).unwrap(), [vec![4], vec![4]]);
        assert_eq!(plan.flush_reordered(), [vec![2], vec![1]]);
        assert!(plan.apply(vec![5]).is_err());
    }

    #[test]
    fn matrix_names_are_unique_and_every_required_fault_point_is_present() {
        let names = RFC_FAULT_MATRIX
            .iter()
            .map(|scenario| scenario.name)
            .collect::<BTreeSet<_>>();
        let points = RFC_FAULT_MATRIX
            .iter()
            .map(|scenario| scenario.point)
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), RFC_FAULT_MATRIX.len());
        assert_eq!(points.len(), 17);
    }
}
