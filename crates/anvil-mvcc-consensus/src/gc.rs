use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{CommitVersion, NodeIncarnation};

/// Cluster-wide facts that can prevent history from being reclaimed.
///
/// Every version is expressed in the cluster's consensus commit coordinate.
/// A pin at version `S` means that state visible at `S` must remain readable, so
/// the GC watermark may advance to `S`, but never beyond it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GarbageCollectionPins {
    pub active_snapshots: BTreeSet<CommitVersion>,
    pub replica_applied_watermarks: BTreeMap<NodeIncarnation, CommitVersion>,
    pub history_retention_floor: Option<CommitVersion>,
    pub backup_pins: BTreeSet<CommitVersion>,
    pub audit_pins: BTreeSet<CommitVersion>,
    pub unfinished_work_pins: BTreeSet<CommitVersion>,
}

impl GarbageCollectionPins {
    /// Returns the greatest watermark allowed by every currently reported pin.
    ///
    /// `requested` is a policy target, while `cluster_head` prevents a caller
    /// from authorising deletion of versions which consensus has not ordered.
    /// Missing replica reports are deliberately not inferred here: the caller
    /// must include every supported voting/catch-up replica or decline to
    /// propose a new watermark.
    pub fn safe_watermark(
        &self,
        current: CommitVersion,
        requested: CommitVersion,
        cluster_head: CommitVersion,
    ) -> Result<CommitVersion, GarbageCollectionSafetyError> {
        if requested < current {
            return Err(GarbageCollectionSafetyError::WatermarkMovedBackwards {
                current,
                requested,
            });
        }
        if current > cluster_head {
            return Err(GarbageCollectionSafetyError::CurrentBeyondClusterHead {
                current,
                cluster_head,
            });
        }

        let mut safe = requested.min(cluster_head);
        for pin in self
            .active_snapshots
            .iter()
            .chain(self.replica_applied_watermarks.values())
            .chain(self.backup_pins.iter())
            .chain(self.audit_pins.iter())
            .chain(self.unfinished_work_pins.iter())
            .chain(self.history_retention_floor.iter())
        {
            safe = safe.min(*pin);
        }

        if safe < current {
            return Err(GarbageCollectionSafetyError::ExistingWatermarkViolatesPin {
                current,
                oldest_pin: safe,
            });
        }
        Ok(safe)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GarbageCollectionSafetyError {
    #[error("GC watermark cannot move backwards from {current:?} to {requested:?}")]
    WatermarkMovedBackwards {
        current: CommitVersion,
        requested: CommitVersion,
    },
    #[error("current GC watermark {current:?} is beyond cluster head {cluster_head:?}")]
    CurrentBeyondClusterHead {
        current: CommitVersion,
        cluster_head: CommitVersion,
    },
    #[error(
        "existing GC watermark {current:?} is already beyond reported safety pin {oldest_pin:?}"
    )]
    ExistingWatermarkViolatesPin {
        current: CommitVersion,
        oldest_pin: CommitVersion,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeId;

    fn replica(id: u64) -> NodeIncarnation {
        NodeIncarnation {
            node_id: NodeId(id),
            incarnation: 1,
        }
    }

    #[test]
    fn every_pin_class_constrains_the_consensus_watermark() {
        let pins = GarbageCollectionPins {
            active_snapshots: [CommitVersion(80)].into_iter().collect(),
            replica_applied_watermarks: [(replica(1), CommitVersion(70))].into_iter().collect(),
            history_retention_floor: Some(CommitVersion(60)),
            backup_pins: [CommitVersion(50)].into_iter().collect(),
            audit_pins: [CommitVersion(40)].into_iter().collect(),
            unfinished_work_pins: [CommitVersion(30)].into_iter().collect(),
        };

        assert_eq!(
            pins.safe_watermark(CommitVersion(20), CommitVersion(100), CommitVersion(90))
                .unwrap(),
            CommitVersion(30)
        );
    }

    #[test]
    fn lagging_replica_and_active_snapshot_cannot_be_bypassed_by_policy_target() {
        let pins = GarbageCollectionPins {
            active_snapshots: [CommitVersion(44)].into_iter().collect(),
            replica_applied_watermarks: [(replica(2), CommitVersion(39))].into_iter().collect(),
            ..Default::default()
        };

        assert_eq!(
            pins.safe_watermark(CommitVersion(10), CommitVersion(1_000), CommitVersion(500))
                .unwrap(),
            CommitVersion(39)
        );
    }

    #[test]
    fn stale_pin_is_reported_instead_of_silently_moving_watermark_backwards() {
        let pins = GarbageCollectionPins {
            backup_pins: [CommitVersion(9)].into_iter().collect(),
            ..Default::default()
        };

        assert!(matches!(
            pins.safe_watermark(CommitVersion(10), CommitVersion(20), CommitVersion(20)),
            Err(GarbageCollectionSafetyError::ExistingWatermarkViolatesPin { .. })
        ));
    }
}
