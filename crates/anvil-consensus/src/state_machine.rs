use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ApplyResult, Command, CommitBatch, CommitResult, CommittedBatch, ExecutorNomination,
    InvocationId, InvocationReceipt, NodeId, ProgramHash, ProgramPathHash, codec,
};

/// Pure deterministic state for Anvil's compact consensus log.
///
/// The state machine deliberately has no cluster-membership table. The
/// OpenRaft adapter checks that a nominated executor is a current voter or
/// learner using OpenRaft's committed membership before calling `apply`.
///
/// Retained batch decisions are globally ordered by their original Raft log
/// index and bounded by both configured entry and encoded-byte limits. Reaching
/// either limit rejects new commits until externally safe finalization advances
/// the checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateMachine {
    max_commit_entries: u32,
    max_commit_bytes: u64,
    executor: Option<ExecutorNomination>,
    commit_suffix: BTreeMap<u64, InvocationReceipt>,
    invocation_cursors: BTreeMap<InvocationId, u64>,
    commit_suffix_bytes: u64,
    last_commit_cursor: Option<u64>,
    finalized_through: Option<u64>,
}

impl StateMachine {
    pub fn new(max_commit_entries: u32, max_commit_bytes: u64) -> Result<Self, ApplyError> {
        if max_commit_entries == 0 {
            return Err(ApplyError::InvalidCommitEntryLimit);
        }
        if max_commit_bytes == 0 {
            return Err(ApplyError::InvalidCommitByteLimit);
        }

        Ok(Self {
            max_commit_entries,
            max_commit_bytes,
            executor: None,
            commit_suffix: BTreeMap::new(),
            invocation_cursors: BTreeMap::new(),
            commit_suffix_bytes: 0,
            last_commit_cursor: None,
            finalized_through: None,
        })
    }

    pub fn max_commit_entries(&self) -> u32 {
        self.max_commit_entries
    }

    pub fn max_commit_bytes(&self) -> u64 {
        self.max_commit_bytes
    }

    pub fn executor(&self) -> Option<ExecutorNomination> {
        self.executor
    }

    pub fn invocation_receipt(&self, invocation_id: InvocationId) -> Option<InvocationReceipt> {
        self.invocation_cursors
            .get(&invocation_id)
            .and_then(|cursor| self.commit_suffix.get(cursor))
            .copied()
    }

    /// Retained decisions in their one global Raft commit order.
    pub fn committed_batches(&self) -> impl Iterator<Item = InvocationReceipt> + '_ {
        self.commit_suffix.values().copied()
    }

    pub fn committed_batch(&self, commit_cursor: u64) -> Option<InvocationReceipt> {
        self.commit_suffix.get(&commit_cursor).copied()
    }

    pub fn commit_suffix_len(&self) -> u32 {
        self.commit_suffix.len() as u32
    }

    pub fn commit_suffix_bytes(&self) -> u64 {
        self.commit_suffix_bytes
    }

    pub fn last_commit_cursor(&self) -> Option<u64> {
        self.last_commit_cursor
    }

    pub fn finalized_through(&self) -> Option<u64> {
        self.finalized_through
    }

    /// Apply a command at its committed Raft log index.
    ///
    /// The index supplies both executor nominations and batch commit cursors;
    /// neither is independently allocated by Anvil.
    pub fn apply(
        &mut self,
        committed_log_index: u64,
        command: &Command,
    ) -> Result<ApplyResult, ApplyError> {
        match command {
            Command::NominateExecutor { executor } => {
                self.nominate_executor(*executor, committed_log_index)
            }
            Command::CommitBatch(batch) => self.commit_batch(committed_log_index, *batch),
            Command::FinalizedThrough {
                executor,
                nomination_log_index,
                through_commit_cursor,
            } => {
                self.advance_finalization(*executor, *nomination_log_index, *through_commit_cursor)
            }
        }
    }

    fn nominate_executor(
        &mut self,
        executor: NodeId,
        committed_log_index: u64,
    ) -> Result<ApplyResult, ApplyError> {
        validate_node(executor)?;
        let nomination = ExecutorNomination {
            executor,
            nomination_log_index: committed_log_index,
        };
        self.executor = Some(nomination);
        Ok(ApplyResult::ExecutorNominated(nomination))
    }

    fn commit_batch(
        &mut self,
        committed_log_index: u64,
        batch: CommitBatch,
    ) -> Result<ApplyResult, ApplyError> {
        validate_commit_batch(batch)?;
        self.require_executor(batch.executor, batch.nomination_log_index)?;

        if let Some(receipt) = self.invocation_receipt(batch.invocation_id) {
            return replay(receipt, batch).map(|receipt| {
                ApplyResult::BatchCommitted(CommitResult {
                    receipt,
                    replayed: true,
                })
            });
        }

        if self
            .last_commit_cursor
            .is_some_and(|last| committed_log_index <= last)
        {
            return Err(ApplyError::CommitCursorDidNotAdvance {
                current: self.last_commit_cursor,
                requested: committed_log_index,
            });
        }
        if self
            .finalized_through
            .is_some_and(|through| committed_log_index <= through)
        {
            return Err(ApplyError::CommitCursorNotAfterFinalization {
                finalized_through: self.finalized_through,
                requested: committed_log_index,
            });
        }

        let receipt = InvocationReceipt {
            invocation_id: batch.invocation_id,
            input_fingerprint: batch.input_fingerprint,
            committed_batch: CommittedBatch {
                commit_cursor: committed_log_index,
                executor: batch.executor,
                nomination_log_index: batch.nomination_log_index,
                program_path_hash: batch.program_path_hash,
                program_hash: batch.program_hash,
                bundle_ref: batch.bundle_ref,
                bundle_hash: batch.bundle_hash,
                durability_class: batch.durability_class,
                durability_evidence_hash: batch.durability_evidence_hash,
            },
        };
        let retained_bytes = retained_bytes(&receipt)?;
        let next_entries = self.commit_suffix_len().saturating_add(1);
        let next_bytes = self
            .commit_suffix_bytes
            .checked_add(retained_bytes)
            .ok_or(ApplyError::CommitTailByteCountExhausted)?;
        if next_entries > self.max_commit_entries || next_bytes > self.max_commit_bytes {
            return Err(ApplyError::CommitTailFull {
                entries: self.commit_suffix_len(),
                bytes: self.commit_suffix_bytes,
                required_bytes: retained_bytes,
                max_entries: self.max_commit_entries,
                max_bytes: self.max_commit_bytes,
            });
        }

        self.commit_suffix.insert(committed_log_index, receipt);
        self.invocation_cursors
            .insert(batch.invocation_id, committed_log_index);
        self.commit_suffix_bytes = next_bytes;
        self.last_commit_cursor = Some(committed_log_index);

        Ok(ApplyResult::BatchCommitted(CommitResult {
            receipt,
            replayed: false,
        }))
    }

    fn advance_finalization(
        &mut self,
        executor: NodeId,
        nomination_log_index: u64,
        through_commit_cursor: u64,
    ) -> Result<ApplyResult, ApplyError> {
        self.require_executor(executor, nomination_log_index)?;

        if let Some(current) = self.finalized_through {
            if through_commit_cursor < current {
                return Err(ApplyError::FinalizedThroughRegressed {
                    current,
                    requested: through_commit_cursor,
                });
            }
            if through_commit_cursor == current {
                return Ok(ApplyResult::FinalizationAdvanced {
                    through_commit_cursor,
                    pruned_entries: 0,
                    pruned_bytes: 0,
                });
            }
        }

        let last_commit_cursor = self
            .last_commit_cursor
            .ok_or(ApplyError::NoCommittedBatchToFinalize)?;
        if through_commit_cursor > last_commit_cursor {
            return Err(ApplyError::FinalizedBeyondLastCommit {
                last_commit_cursor,
                requested: through_commit_cursor,
            });
        }

        let pruned = self
            .commit_suffix
            .range(..=through_commit_cursor)
            .map(|(cursor, receipt)| (*cursor, *receipt))
            .collect::<Vec<_>>();
        let mut pruned_bytes = 0_u64;
        for (cursor, receipt) in &pruned {
            pruned_bytes = pruned_bytes
                .checked_add(retained_bytes(receipt)?)
                .ok_or(ApplyError::CommitTailByteCountExhausted)?;
            self.commit_suffix.remove(cursor);
            self.invocation_cursors.remove(&receipt.invocation_id);
        }
        self.commit_suffix_bytes = self
            .commit_suffix_bytes
            .checked_sub(pruned_bytes)
            .ok_or(ApplyError::CommitTailByteAccountingCorrupt)?;
        self.finalized_through = Some(through_commit_cursor);

        Ok(ApplyResult::FinalizationAdvanced {
            through_commit_cursor,
            pruned_entries: pruned.len() as u32,
            pruned_bytes,
        })
    }

    fn require_executor(
        &self,
        executor: NodeId,
        nomination_log_index: u64,
    ) -> Result<(), ApplyError> {
        validate_node(executor)?;
        let current = self.executor.ok_or(ApplyError::ExecutorNotNominated)?;
        if current.executor != executor {
            return Err(ApplyError::NotCurrentExecutor {
                current: current.executor,
                requested: executor,
            });
        }
        if current.nomination_log_index != nomination_log_index {
            return Err(ApplyError::NominationFenceMismatch {
                expected: current.nomination_log_index,
                requested: nomination_log_index,
            });
        }
        Ok(())
    }
}

fn retained_bytes(receipt: &InvocationReceipt) -> Result<u64, ApplyError> {
    codec::encoded_len(receipt).map_err(|_| ApplyError::CommitReceiptEncodingFailed)
}

fn validate_node(node: NodeId) -> Result<(), ApplyError> {
    if node.0 == 0 {
        return Err(ApplyError::InvalidNodeId);
    }
    Ok(())
}

fn validate_program(
    program_path_hash: ProgramPathHash,
    program_hash: ProgramHash,
) -> Result<(), ApplyError> {
    if program_path_hash.0 == [0; 32] {
        return Err(ApplyError::InvalidProgramPathHash);
    }
    if program_hash.0 == [0; 32] {
        return Err(ApplyError::InvalidProgramHash);
    }
    Ok(())
}

fn validate_commit_batch(batch: CommitBatch) -> Result<(), ApplyError> {
    validate_node(batch.executor)?;
    validate_program(batch.program_path_hash, batch.program_hash)?;
    if batch.invocation_id.0 == [0; 32] {
        return Err(ApplyError::InvalidInvocationId);
    }
    if batch.input_fingerprint.0 == [0; 32] {
        return Err(ApplyError::InvalidInvocationFingerprint);
    }
    if batch.bundle_ref.0 == [0; 32] {
        return Err(ApplyError::InvalidBundleRef);
    }
    if batch.bundle_hash.0 == [0; 32] {
        return Err(ApplyError::InvalidBundleHash);
    }
    if batch.durability_class.0 == [0; 32] {
        return Err(ApplyError::InvalidDurabilityClass);
    }
    if batch.durability_evidence_hash.0 == [0; 32] {
        return Err(ApplyError::InvalidDurabilityEvidenceHash);
    }
    Ok(())
}

fn replay(
    committed: InvocationReceipt,
    requested: CommitBatch,
) -> Result<InvocationReceipt, ApplyError> {
    if committed.committed_batch.program_path_hash != requested.program_path_hash
        || committed.committed_batch.program_hash != requested.program_hash
        || committed.input_fingerprint != requested.input_fingerprint
    {
        return Err(ApplyError::IdempotencyConflict {
            invocation_id: requested.invocation_id,
        });
    }

    // A current replacement executor may have independently rediscovered a
    // prepared bundle while resolving an unknown outcome. The retained commit
    // is authoritative, including its original cursor and durability details.
    Ok(committed)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum ApplyError {
    #[error("commit-tail entry limit must be non-zero")]
    InvalidCommitEntryLimit,
    #[error("commit-tail byte limit must be non-zero")]
    InvalidCommitByteLimit,
    #[error("node identity must be non-zero")]
    InvalidNodeId,
    #[error("node {executor:?} is not a current Raft voter or learner")]
    ExecutorNotCurrentMember { executor: NodeId },
    #[error("no atomic-program executor has been nominated")]
    ExecutorNotNominated,
    #[error("node {requested:?} is not current executor {current:?}")]
    NotCurrentExecutor { current: NodeId, requested: NodeId },
    #[error("executor nomination fence is {expected}, not {requested}")]
    NominationFenceMismatch { expected: u64, requested: u64 },
    #[error("program path hash must be non-zero")]
    InvalidProgramPathHash,
    #[error("program hash must be non-zero")]
    InvalidProgramHash,
    #[error("invocation identity must be non-zero")]
    InvalidInvocationId,
    #[error("invocation fingerprint must be non-zero")]
    InvalidInvocationFingerprint,
    #[error("bundle reference must be non-zero")]
    InvalidBundleRef,
    #[error("bundle hash must be non-zero")]
    InvalidBundleHash,
    #[error("durability class must be non-zero")]
    InvalidDurabilityClass,
    #[error("durability evidence hash must be non-zero")]
    InvalidDurabilityEvidenceHash,
    #[error("invocation {invocation_id:?} was already committed with different logical inputs")]
    IdempotencyConflict { invocation_id: InvocationId },
    #[error("commit cursor {requested} did not advance {current:?}")]
    CommitCursorDidNotAdvance {
        current: Option<u64>,
        requested: u64,
    },
    #[error("commit cursor {requested} is not after finalized-through {finalized_through:?}")]
    CommitCursorNotAfterFinalization {
        finalized_through: Option<u64>,
        requested: u64,
    },
    #[error(
        "commit tail is full: {entries}/{max_entries} entries and {bytes}/{max_bytes} bytes; new receipt requires {required_bytes} bytes"
    )]
    CommitTailFull {
        entries: u32,
        bytes: u64,
        required_bytes: u64,
        max_entries: u32,
        max_bytes: u64,
    },
    #[error("commit-tail byte count overflowed")]
    CommitTailByteCountExhausted,
    #[error("commit-tail byte accounting is corrupt")]
    CommitTailByteAccountingCorrupt,
    #[error("committed receipt could not be measured by the consensus codec")]
    CommitReceiptEncodingFailed,
    #[error("there is no committed batch to finalize")]
    NoCommittedBatchToFinalize,
    #[error("finalized-through cursor {requested} regressed from {current}")]
    FinalizedThroughRegressed { current: u64, requested: u64 },
    #[error("finalized-through cursor {requested} exceeds last commit {last_commit_cursor}")]
    FinalizedBeyondLastCommit {
        last_commit_cursor: u64,
        requested: u64,
    },
}
