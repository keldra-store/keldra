use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    CertificationAbort, CertificationResult, CertifyTransaction, CommitVersion, LogicalKeyHash,
    RangeConflictKey, TransactionId,
};

/// Complete deterministic state replicated by the certification state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CertificationState {
    last_applied: CommitVersion,
    point_latest_write: BTreeMap<LogicalKeyHash, CommitVersion>,
    range_latest_write: BTreeMap<RangeConflictKey, CommitVersion>,
    recent_results: BTreeMap<TransactionId, CertificationResult>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CertificationError {
    #[error("commit positions must increase: last {last:?}, proposed {proposed:?}")]
    NonMonotonicPosition {
        last: CommitVersion,
        proposed: CommitVersion,
    },
    #[error("transaction ID was reused with a different bundle hash")]
    TransactionIdentityMismatch,
}

impl CertificationState {
    pub fn last_applied(&self) -> CommitVersion {
        self.last_applied
    }

    pub fn point_version(&self, key: LogicalKeyHash) -> Option<CommitVersion> {
        self.point_latest_write.get(&key).copied()
    }

    pub fn range_stamp(&self, key: RangeConflictKey) -> Option<CommitVersion> {
        self.range_latest_write.get(&key).copied()
    }

    pub fn transaction_result(
        &self,
        transaction_id: TransactionId,
    ) -> Option<&CertificationResult> {
        self.recent_results.get(&transaction_id)
    }

    /// Apply one command at its committed Raft log position.
    ///
    /// Invalid commands are deterministic transaction aborts. Storage/order
    /// violations are adapter errors and therefore returned as `Err`.
    pub fn apply(
        &mut self,
        position: CommitVersion,
        command: &CertifyTransaction,
    ) -> Result<CertificationResult, CertificationError> {
        if position <= self.last_applied {
            return Err(CertificationError::NonMonotonicPosition {
                last: self.last_applied,
                proposed: position,
            });
        }

        if let Some(result) = self.recent_results.get(&command.transaction_id) {
            self.last_applied = position;
            if result.bundle_hash() != command.bundle_hash {
                return Err(CertificationError::TransactionIdentityMismatch);
            }
            return Ok(result.clone());
        }

        let result = match validate_canonical(command) {
            Err(reason) => aborted(position, command, reason),
            Ok(()) => match self.first_conflict(command) {
                Some(reason) => aborted(position, command, reason),
                None => {
                    for key in &command.written_point_keys {
                        self.point_latest_write.insert(*key, position);
                    }
                    for range in &command.advanced_range_stamps {
                        self.range_latest_write.insert(*range, position);
                    }
                    CertificationResult::Committed {
                        commit_version: position,
                        bundle_hash: command.bundle_hash,
                    }
                }
            },
        };

        self.last_applied = position;
        self.recent_results
            .insert(command.transaction_id, result.clone());
        Ok(result)
    }

    fn first_conflict(&self, command: &CertifyTransaction) -> Option<CertificationAbort> {
        for observation in &command.point_observations {
            let actual = self.point_latest_write.get(&observation.key).copied();
            if actual != observation.observed_version {
                return Some(CertificationAbort::PointConflict {
                    key: observation.key,
                    expected: observation.observed_version,
                    actual,
                });
            }
        }
        for observation in &command.range_observations {
            let actual = self.range_latest_write.get(&observation.range).copied();
            if actual != observation.observed_stamp {
                return Some(CertificationAbort::RangeConflict {
                    range: observation.range,
                    expected: observation.observed_stamp,
                    actual,
                });
            }
        }
        None
    }
}

fn aborted(
    position: CommitVersion,
    command: &CertifyTransaction,
    reason: CertificationAbort,
) -> CertificationResult {
    CertificationResult::Aborted {
        at_version: position,
        bundle_hash: command.bundle_hash,
        reason,
    }
}

fn validate_canonical(command: &CertifyTransaction) -> Result<(), CertificationAbort> {
    check_sorted_unique(&command.point_observations, "point observations")?;
    check_sorted_unique(&command.range_observations, "range observations")?;
    check_sorted_unique(&command.written_point_keys, "written point keys")?;
    check_sorted_unique(&command.advanced_range_stamps, "advanced range stamps")?;
    check_sorted_unique(&command.durable_holders, "durable holders")?;
    if command.bundle_length == 0 {
        return Err(CertificationAbort::InvalidCommand(
            "bundle length must be non-zero".into(),
        ));
    }
    if command.durable_holders.is_empty() {
        return Err(CertificationAbort::InvalidCommand(
            "at least one durable holder is required".into(),
        ));
    }
    Ok(())
}

fn check_sorted_unique<T: Ord>(items: &[T], field: &str) -> Result<(), CertificationAbort> {
    if items.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CertificationAbort::InvalidCommand(format!(
            "{field} must be sorted and unique"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BundleHash, DurabilityLevel, NodeId, NodeIncarnation, PointObservation, RangeObservation,
    };

    fn hash(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    fn command(id: u8) -> CertifyTransaction {
        CertifyTransaction {
            transaction_id: TransactionId([id; 16]),
            snapshot_version: CommitVersion(0),
            point_observations: vec![],
            range_observations: vec![],
            written_point_keys: vec![],
            advanced_range_stamps: vec![],
            bundle_hash: BundleHash(hash(id)),
            bundle_length: 1,
            durability: DurabilityLevel::Local,
            durable_holders: vec![NodeIncarnation {
                node_id: NodeId(1),
                incarnation: 1,
            }],
        }
    }

    #[test]
    fn conflicting_point_transaction_aborts_atomically() {
        let key = LogicalKeyHash(hash(1));
        let untouched = LogicalKeyHash(hash(2));
        let mut state = CertificationState::default();

        let mut first = command(1);
        first.written_point_keys = vec![key];
        assert!(matches!(
            state.apply(CommitVersion(1), &first).unwrap(),
            CertificationResult::Committed { .. }
        ));

        let mut second = command(2);
        second.point_observations = vec![PointObservation {
            key,
            observed_version: None,
        }];
        second.written_point_keys = vec![untouched];
        assert!(matches!(
            state.apply(CommitVersion(2), &second).unwrap(),
            CertificationResult::Aborted {
                reason: CertificationAbort::PointConflict { .. },
                ..
            }
        ));
        assert_eq!(state.point_version(untouched), None);
    }

    #[test]
    fn unrelated_transactions_commit() {
        let mut state = CertificationState::default();
        for id in 1..=2 {
            let mut tx = command(id);
            tx.written_point_keys = vec![LogicalKeyHash(hash(id))];
            assert!(matches!(
                state.apply(CommitVersion(id as u64), &tx).unwrap(),
                CertificationResult::Committed { .. }
            ));
        }
    }

    #[test]
    fn range_phantom_aborts() {
        let range = RangeConflictKey(hash(7));
        let mut state = CertificationState::default();
        let mut insertion = command(1);
        insertion.advanced_range_stamps = vec![range];
        state.apply(CommitVersion(1), &insertion).unwrap();

        let mut reader = command(2);
        reader.range_observations = vec![RangeObservation {
            range,
            observed_stamp: None,
        }];
        assert!(matches!(
            state.apply(CommitVersion(2), &reader).unwrap(),
            CertificationResult::Aborted {
                reason: CertificationAbort::RangeConflict { .. },
                ..
            }
        ));
    }

    #[test]
    fn retries_are_stable_and_do_not_consume_a_new_version() {
        let mut state = CertificationState::default();
        let tx = command(1);
        let first = state.apply(CommitVersion(4), &tx).unwrap();
        let retry = state.apply(CommitVersion(99), &tx).unwrap();
        assert_eq!(first, retry);
        assert_eq!(state.last_applied(), CommitVersion(99));
    }

    #[test]
    fn transaction_id_cannot_name_different_bundle() {
        let mut state = CertificationState::default();
        let first = command(1);
        state.apply(CommitVersion(1), &first).unwrap();
        let mut changed = first;
        changed.bundle_hash = BundleHash(hash(9));
        assert_eq!(
            state.apply(CommitVersion(2), &changed),
            Err(CertificationError::TransactionIdentityMismatch)
        );
        assert_eq!(state.last_applied(), CommitVersion(2));
    }

    #[test]
    fn noncanonical_command_is_a_stable_abort() {
        let mut state = CertificationState::default();
        let mut tx = command(1);
        let duplicate = LogicalKeyHash(hash(1));
        tx.written_point_keys = vec![duplicate, duplicate];
        let result = state.apply(CommitVersion(1), &tx).unwrap();
        assert!(matches!(
            result,
            CertificationResult::Aborted {
                reason: CertificationAbort::InvalidCommand(_),
                ..
            }
        ));
        assert_eq!(state.apply(CommitVersion(10), &tx).unwrap(), result);
    }
}
