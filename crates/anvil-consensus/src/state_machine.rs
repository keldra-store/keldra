use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ATOMIC_REPLAY_RETENTION_MILLIS, ApplyResult, Command, CommitBatch, CommitResult,
    CommittedBatch, CommittedInvocation, ExecutorNomination, InvocationId,
    MAX_COMMITTED_INVOCATION_BYTES, MAX_COMMITTED_INVOCATIONS, NodeId, ProgramHash,
    ProgramPathHash, SYSTEM_BOOTSTRAP_VERSION, codec,
    types::{ClusterId, SystemBootstrapState},
};

/// Pure deterministic state for Anvil's compact consensus log.
///
/// The state machine deliberately has no cluster-membership table. The
/// OpenRaft adapter checks that a nominated executor is a current voter or
/// learner using OpenRaft's committed membership before calling `apply`.
///
/// Committed invocations are globally ordered by their original Raft log index.
/// Configured bounds apply to the unfinalized recovery tail; fixed bounds apply
/// to the independently retained replay window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateMachine {
    max_commit_entries: u32,
    max_commit_bytes: u64,
    cluster_id: Option<ClusterId>,
    system_bootstrap: SystemBootstrapState,
    executor: Option<ExecutorNomination>,
    committed_invocations: BTreeMap<u64, CommittedInvocation>,
    committed_invocation_bytes: u64,
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

        let committed_invocation_bytes =
            codec::encoded_len(&BTreeMap::<u64, CommittedInvocation>::new())
                .map_err(|_| ApplyError::CommittedInvocationEncodingFailed)?;
        Ok(Self {
            max_commit_entries,
            max_commit_bytes,
            cluster_id: None,
            system_bootstrap: SystemBootstrapState::Missing,
            executor: None,
            committed_invocations: BTreeMap::new(),
            committed_invocation_bytes,
            last_commit_cursor: None,
            finalized_through: None,
        })
    }

    /// Convert the exact state-machine body written by Anvil 0.5.0.
    ///
    /// Snapshot decoding owns the legacy wire type; this constructor only
    /// supplies the two fields introduced after that release.
    pub(crate) fn from_v050_snapshot(
        max_commit_entries: u32,
        max_commit_bytes: u64,
        executor: Option<ExecutorNomination>,
        committed_invocations: BTreeMap<u64, CommittedInvocation>,
        committed_invocation_bytes: u64,
        last_commit_cursor: Option<u64>,
        finalized_through: Option<u64>,
    ) -> Self {
        Self {
            max_commit_entries,
            max_commit_bytes,
            cluster_id: None,
            system_bootstrap: SystemBootstrapState::Missing,
            executor,
            committed_invocations,
            committed_invocation_bytes,
            last_commit_cursor,
            finalized_through,
        }
    }

    pub fn max_commit_entries(&self) -> u32 {
        self.max_commit_entries
    }

    pub fn max_commit_bytes(&self) -> u64 {
        self.max_commit_bytes
    }

    pub fn cluster_id(&self) -> Option<ClusterId> {
        self.cluster_id
    }

    pub fn system_bootstrap(&self) -> SystemBootstrapState {
        self.system_bootstrap
    }

    pub fn executor(&self) -> Option<ExecutorNomination> {
        self.executor
    }

    pub fn replay_entry(
        &self,
        invocation_id: InvocationId,
        now_unix_millis: u64,
    ) -> Option<CommittedInvocation> {
        self.committed_invocations
            .values()
            .find(|entry| entry.invocation_id == invocation_id)
            .filter(|entry| entry.replay_expires_at_unix_millis > now_unix_millis)
            .copied()
    }

    /// Unfinalized decisions in their one global Raft commit order.
    pub fn unfinalized_invocations(&self) -> impl Iterator<Item = CommittedInvocation> + '_ {
        let finalized = self.finalized_through.unwrap_or(0);
        self.committed_invocations
            .range((
                std::ops::Bound::Excluded(finalized),
                std::ops::Bound::Unbounded,
            ))
            .map(|(_, invocation)| *invocation)
    }

    pub fn committed_invocation(&self, commit_cursor: u64) -> Option<CommittedInvocation> {
        self.committed_invocations.get(&commit_cursor).copied()
    }

    pub fn unfinalized_commit_len(&self) -> u32 {
        self.unfinalized_invocations().count() as u32
    }

    pub fn unfinalized_commit_bytes(&self) -> Result<u64, ApplyError> {
        let finalized = self.finalized_through.unwrap_or(0);
        self.committed_invocations
            .range((
                std::ops::Bound::Excluded(finalized),
                std::ops::Bound::Unbounded,
            ))
            .try_fold(0_u64, |total, (cursor, entry)| {
                total
                    .checked_add(committed_invocation_entry_bytes(*cursor, entry)?)
                    .ok_or(ApplyError::CommitTailByteCountExhausted)
            })
    }

    pub fn committed_invocation_len(&self) -> u32 {
        self.committed_invocations.len() as u32
    }

    pub fn committed_invocation_bytes(&self) -> u64 {
        self.committed_invocation_bytes
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
            Command::InitializeCluster { cluster_id } => self.initialize_cluster(*cluster_id),
            Command::CompleteSystemBootstrap {
                executor,
                nomination_log_index,
                bootstrap_version,
            } => self.complete_system_bootstrap(
                *executor,
                *nomination_log_index,
                *bootstrap_version,
                committed_log_index,
            ),
        }
    }

    fn initialize_cluster(&mut self, cluster_id: ClusterId) -> Result<ApplyResult, ApplyError> {
        validate_cluster_id(cluster_id)?;
        if let Some(current) = self.cluster_id {
            if current != cluster_id {
                return Err(ApplyError::ClusterIdentityConflict {
                    current,
                    requested: cluster_id,
                });
            }
            return Ok(ApplyResult::ClusterInitialized {
                cluster_id: current,
            });
        }

        self.cluster_id = Some(cluster_id);
        Ok(ApplyResult::ClusterInitialized { cluster_id })
    }

    fn complete_system_bootstrap(
        &mut self,
        executor: NodeId,
        nomination_log_index: u64,
        bootstrap_version: u16,
        committed_log_index: u64,
    ) -> Result<ApplyResult, ApplyError> {
        if bootstrap_version != SYSTEM_BOOTSTRAP_VERSION {
            return Err(ApplyError::UnsupportedSystemBootstrapVersion {
                requested: bootstrap_version,
            });
        }
        if self.cluster_id.is_none() {
            return Err(ApplyError::ClusterNotInitialized);
        }
        self.require_executor(executor, nomination_log_index)?;

        match self.system_bootstrap {
            SystemBootstrapState::Missing => {
                let completed = SystemBootstrapState::Complete {
                    version: bootstrap_version,
                    committed_log_index,
                };
                self.system_bootstrap = completed;
                Ok(ApplyResult::SystemBootstrapCompleted(completed))
            }
            completed @ SystemBootstrapState::Complete { version, .. }
                if version == bootstrap_version =>
            {
                Ok(ApplyResult::SystemBootstrapCompleted(completed))
            }
            SystemBootstrapState::Complete {
                version: current, ..
            } => Err(ApplyError::SystemBootstrapVersionConflict {
                current,
                requested: bootstrap_version,
            }),
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

        let expired = self.expired_finalized_cursors(batch.proposal_at_unix_millis);
        self.prune_committed_invocations(&expired)?;
        let existing = self
            .committed_invocations
            .iter()
            .find(|(_, entry)| entry.invocation_id == batch.invocation_id)
            .map(|(_, entry)| entry)
            .copied();
        if let Some(invocation) = existing {
            let invocation = replay(invocation, batch)?;
            return Ok(ApplyResult::BatchCommitted(CommitResult {
                invocation,
                replayed: true,
            }));
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

        let invocation = CommittedInvocation {
            invocation_id: batch.invocation_id,
            input_fingerprint: batch.input_fingerprint,
            proposal_at_unix_millis: batch.proposal_at_unix_millis,
            replay_expires_at_unix_millis: batch.replay_expires_at_unix_millis,
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
        let invocation_bytes = committed_invocation_entry_bytes(committed_log_index, &invocation)?;
        let retained_entries = self.committed_invocation_len();
        let retained_bytes = self.committed_invocation_bytes;
        let next_replay_entries = retained_entries.saturating_add(1);
        let next_replay_bytes = retained_bytes
            .checked_add(invocation_bytes)
            .ok_or(ApplyError::CommittedInvocationByteCountExhausted)?;
        if next_replay_entries > MAX_COMMITTED_INVOCATIONS
            || next_replay_bytes > MAX_COMMITTED_INVOCATION_BYTES
        {
            return Err(ApplyError::CommittedInvocationWindowFull {
                entries: retained_entries,
                bytes: retained_bytes,
                required_bytes: invocation_bytes,
            });
        }

        let tail_entries = self.unfinalized_commit_len().saturating_add(1);
        let tail_bytes = self
            .unfinalized_commit_bytes()?
            .checked_add(invocation_bytes)
            .ok_or(ApplyError::CommitTailByteCountExhausted)?;
        if tail_entries > self.max_commit_entries || tail_bytes > self.max_commit_bytes {
            return Err(ApplyError::CommitTailFull {
                entries: self.unfinalized_commit_len(),
                bytes: self.unfinalized_commit_bytes()?,
                required_bytes: invocation_bytes,
                max_entries: self.max_commit_entries,
                max_bytes: self.max_commit_bytes,
            });
        }

        self.committed_invocations
            .insert(committed_log_index, invocation);
        self.committed_invocation_bytes = next_replay_bytes;
        self.last_commit_cursor = Some(committed_log_index);

        Ok(ApplyResult::BatchCommitted(CommitResult {
            invocation,
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

        self.finalized_through = Some(through_commit_cursor);

        Ok(ApplyResult::FinalizationAdvanced {
            through_commit_cursor,
        })
    }

    fn expired_finalized_cursors(&self, proposal_at_unix_millis: u64) -> Vec<u64> {
        let Some(finalized) = self.finalized_through else {
            return Vec::new();
        };
        self.committed_invocations
            .range(..=finalized)
            .filter_map(|(cursor, invocation)| {
                (invocation.replay_expires_at_unix_millis <= proposal_at_unix_millis)
                    .then_some(*cursor)
            })
            .collect()
    }

    fn prune_committed_invocations(&mut self, cursors: &[u64]) -> Result<(), ApplyError> {
        for cursor in cursors {
            let invocation = self
                .committed_invocations
                .remove(cursor)
                .ok_or(ApplyError::CommittedInvocationAccountingCorrupt)?;
            self.committed_invocation_bytes = self
                .committed_invocation_bytes
                .checked_sub(committed_invocation_entry_bytes(*cursor, &invocation)?)
                .ok_or(ApplyError::CommittedInvocationAccountingCorrupt)?;
        }
        Ok(())
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

fn committed_invocation_entry_bytes(
    commit_cursor: u64,
    invocation: &CommittedInvocation,
) -> Result<u64, ApplyError> {
    codec::encoded_len(&(commit_cursor, invocation))
        .map_err(|_| ApplyError::CommittedInvocationEncodingFailed)
}

fn validate_node(node: NodeId) -> Result<(), ApplyError> {
    if node.0 == 0 {
        return Err(ApplyError::InvalidNodeId);
    }
    Ok(())
}

fn validate_cluster_id(cluster_id: ClusterId) -> Result<(), ApplyError> {
    if cluster_id.0 == [0; 16] {
        return Err(ApplyError::InvalidClusterId);
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
    if batch.bundle_ref.hash == [0; 32] || batch.bundle_ref.length == 0 {
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
    if batch.proposal_at_unix_millis == 0 {
        return Err(ApplyError::InvalidProposalTime);
    }
    if batch
        .proposal_at_unix_millis
        .checked_add(ATOMIC_REPLAY_RETENTION_MILLIS)
        != Some(batch.replay_expires_at_unix_millis)
    {
        return Err(ApplyError::InvalidReplayExpiry);
    }
    Ok(())
}

fn replay(
    committed: CommittedInvocation,
    requested: CommitBatch,
) -> Result<CommittedInvocation, ApplyError> {
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
    #[error("commit proposal time must be non-zero")]
    InvalidProposalTime,
    #[error("commit replay expiry must be exactly 24 hours after proposal time")]
    InvalidReplayExpiry,
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
        "commit tail is full: {entries}/{max_entries} entries and {bytes}/{max_bytes} bytes; new invocation requires {required_bytes} bytes"
    )]
    CommitTailFull {
        entries: u32,
        bytes: u64,
        required_bytes: u64,
        max_entries: u32,
        max_bytes: u64,
    },
    #[error(
        "committed invocation replay window is full: {entries}/{MAX_COMMITTED_INVOCATIONS} entries and {bytes}/{MAX_COMMITTED_INVOCATION_BYTES} bytes; new entry requires {required_bytes} bytes"
    )]
    CommittedInvocationWindowFull {
        entries: u32,
        bytes: u64,
        required_bytes: u64,
    },
    #[error("commit-tail byte count overflowed")]
    CommitTailByteCountExhausted,
    #[error("committed-invocation byte count overflowed")]
    CommittedInvocationByteCountExhausted,
    #[error("committed-invocation accounting is corrupt")]
    CommittedInvocationAccountingCorrupt,
    #[error("committed invocation could not be measured by the consensus codec")]
    CommittedInvocationEncodingFailed,
    #[error("there is no committed batch to finalize")]
    NoCommittedBatchToFinalize,
    #[error("finalized-through cursor {requested} regressed from {current}")]
    FinalizedThroughRegressed { current: u64, requested: u64 },
    #[error("finalized-through cursor {requested} exceeds last commit {last_commit_cursor}")]
    FinalizedBeyondLastCommit {
        last_commit_cursor: u64,
        requested: u64,
    },
    #[error("cluster identity must be non-zero")]
    InvalidClusterId,
    #[error("cluster identity is already {current:?}, not {requested:?}")]
    ClusterIdentityConflict {
        current: ClusterId,
        requested: ClusterId,
    },
    #[error("cluster identity has not been initialized")]
    ClusterNotInitialized,
    #[error("system-bootstrap version {requested} is unsupported")]
    UnsupportedSystemBootstrapVersion { requested: u16 },
    #[error("system bootstrap is already complete at version {current}, not {requested}")]
    SystemBootstrapVersionConflict { current: u16, requested: u16 },
}
