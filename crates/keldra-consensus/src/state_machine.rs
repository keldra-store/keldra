use std::collections::BTreeMap;

use openraft::LogId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ATOMIC_REPLAY_RETENTION_MILLIS, ApplyResult, ClusterControlState, Command, CommitBatch,
    CommitResult, CommittedBatch, CommittedInvocation, ExecutorNomination, InvocationId,
    MAX_COMMITTED_INVOCATION_BYTES, MAX_COMMITTED_INVOCATIONS, NodeId, ProgramHash,
    ProgramPathHash, SYSTEM_BOOTSTRAP_VERSION, codec,
    types::{ClusterId, MAX_RAFT_NODE_ID, SystemBootstrapState},
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
    pub(crate) cluster_id: Option<ClusterId>,
    system_bootstrap: SystemBootstrapState,
    pub(crate) cluster_control: ClusterControlState,
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
            cluster_control: ClusterControlState::default(),
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
            cluster_control: ClusterControlState::default(),
            executor,
            committed_invocations,
            committed_invocation_bytes,
            last_commit_cursor,
            finalized_through,
        }
    }

    /// Convert the version-one enveloped snapshot written before bounded
    /// cluster-control state was introduced.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_pre_cluster_control_snapshot(
        max_commit_entries: u32,
        max_commit_bytes: u64,
        cluster_id: Option<ClusterId>,
        system_bootstrap: SystemBootstrapState,
        executor: Option<ExecutorNomination>,
        committed_invocations: BTreeMap<u64, CommittedInvocation>,
        committed_invocation_bytes: u64,
        last_commit_cursor: Option<u64>,
        finalized_through: Option<u64>,
    ) -> Self {
        Self {
            max_commit_entries,
            max_commit_bytes,
            cluster_id,
            system_bootstrap,
            cluster_control: ClusterControlState::default(),
            executor,
            committed_invocations,
            committed_invocation_bytes,
            last_commit_cursor,
            finalized_through,
        }
    }

    /// Convert the exact version-two enveloped snapshot written before active
    /// placement changes carried their own committed Raft log ID.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_v2_snapshot(
        max_commit_entries: u32,
        max_commit_bytes: u64,
        cluster_id: Option<ClusterId>,
        system_bootstrap: SystemBootstrapState,
        cluster_control: ClusterControlState,
        executor: Option<ExecutorNomination>,
        committed_invocations: BTreeMap<u64, CommittedInvocation>,
        committed_invocation_bytes: u64,
        last_commit_cursor: Option<u64>,
        finalized_through: Option<u64>,
    ) -> Self {
        Self {
            max_commit_entries,
            max_commit_bytes,
            cluster_id,
            system_bootstrap,
            cluster_control,
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

    pub fn cluster_control(&self) -> &ClusterControlState {
        &self.cluster_control
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

    /// Apply a command at its exact committed Raft log identity.
    ///
    /// The full identity fences active placement. Its index also supplies
    /// executor nominations and batch commit cursors; none is independently
    /// allocated by Anvil.
    pub fn apply(
        &mut self,
        committed_log_id: LogId<u64>,
        command: &Command,
    ) -> Result<ApplyResult, ApplyError> {
        let committed_log_index = committed_log_id.index;
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
            Command::BeginAddNode {
                format_version,
                descriptor,
            } => self.begin_add_node(*format_version, descriptor.clone(), committed_log_index),
            Command::BeginRemoveNode {
                format_version,
                node_id,
            } => self.begin_remove_node(*format_version, *node_id, committed_log_index),
            Command::BeginReweightNode {
                format_version,
                node_id,
                storage_weight_millionths,
            } => self.begin_reweight_node(
                *format_version,
                *node_id,
                *storage_weight_millionths,
                committed_log_index,
            ),
            Command::CompleteMembershipTransition {
                format_version,
                started_log_index,
            } => self.complete_membership_transition(
                *format_version,
                *started_log_index,
                committed_log_id,
            ),
            Command::StagePeerSpkiOverlap {
                format_version,
                node_id,
                expected_current,
                overlap,
            } => {
                self.stage_peer_spki_overlap(*format_version, *node_id, *expected_current, *overlap)
            }
            Command::PromotePeerSpkiOverlap {
                format_version,
                node_id,
                expected_current,
                expected_overlap,
            } => self.promote_peer_spki_overlap(
                *format_version,
                *node_id,
                *expected_current,
                *expected_overlap,
            ),
            Command::ClearPeerSpkiOverlap {
                format_version,
                node_id,
                expected_current,
                expected_overlap,
            } => self.clear_peer_spki_overlap(
                *format_version,
                *node_id,
                *expected_current,
                *expected_overlap,
            ),
            Command::BindJwtSigningKeyFingerprint {
                format_version,
                fingerprint,
            } => self.bind_jwt_signing_key_fingerprint(*format_version, *fingerprint),
            Command::BindErasureCodeProfile {
                format_version,
                profile,
            } => self.bind_erasure_code_profile(*format_version, *profile),
            Command::RefreshJoiningNodePreparation {
                format_version,
                node_id,
                started_log_index,
                expected_peer_spki_sha256,
                expected_join_capability_hash,
                replacement_peer_spki_sha256,
                replacement_join_capability_hash,
            } => self.refresh_joining_node_preparation(
                *format_version,
                *node_id,
                *started_log_index,
                *expected_peer_spki_sha256,
                *expected_join_capability_hash,
                *replacement_peer_spki_sha256,
                *replacement_join_capability_hash,
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
    if !(1..=MAX_RAFT_NODE_ID).contains(&node.0) {
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
    #[error("node identity must be between 1 and 1023")]
    InvalidNodeId,
    #[error("node {executor:?} is not a current Raft voter or learner")]
    ExecutorNotCurrentMember { executor: NodeId },
    #[error("node {executor:?} is not an ACTIVE Raft voter or learner")]
    ExecutorNotActiveMember { executor: NodeId },
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
    #[error("cluster-control command version {requested} is unsupported")]
    UnsupportedClusterControlVersion { requested: u16 },
    #[error("peer address must contain 1 to 255 non-whitespace, non-control UTF-8 bytes")]
    InvalidPeerAddress,
    #[error("storage weight must be a positive number of millionths")]
    InvalidStorageWeight,
    #[error("capability range {min}..={max} is invalid")]
    InvalidCapabilityRange { min: u16, max: u16 },
    #[error("peer SPKI SHA-256 fingerprint must be non-zero")]
    InvalidPeerSpki,
    #[error("join capability hash must be non-zero")]
    InvalidJoinCapabilityHash,
    #[error("a node admitted by ADD must be JOINING")]
    AddedNodeMustBeJoining,
    #[error("a JOINING node requires a single-use join capability hash")]
    JoiningNodeRequiresCapability,
    #[error("a JOINING node cannot have a peer-pin overlap")]
    JoiningNodeCannotRotatePeerPin,
    #[error("an ACTIVE node cannot retain a join capability hash")]
    ActiveNodeRetainsJoinCapability,
    #[error("node {node_id:?} was already used and cannot be admitted again")]
    NodeIdAlreadyUsed { node_id: NodeId },
    #[error("node {node_id:?} is not admitted")]
    NodeNotAdmitted { node_id: NodeId },
    #[error("node {node_id:?} is not ACTIVE")]
    NodeNotActive { node_id: NodeId },
    #[error("the last ACTIVE node cannot be removed")]
    CannotRemoveLastActiveNode,
    #[error("storage weight is unchanged")]
    StorageWeightUnchanged,
    #[error("membership transition at log {started_log_index} is already in progress")]
    MembershipTransitionInProgress { started_log_index: u64 },
    #[error("there is no membership transition")]
    NoMembershipTransition,
    #[error("membership transition fence is {expected}, not {requested}")]
    MembershipTransitionFenceMismatch { expected: u64, requested: u64 },
    #[error("membership transition no longer matches its node descriptor")]
    MembershipTransitionStateMismatch,
    #[error("node {node_id:?} is undergoing a membership transition")]
    NodeMembershipTransitionInProgress { node_id: NodeId },
    #[error("transition node {node_id:?} is not a current Raft voter or learner")]
    TransitionNodeNotCurrentMember { node_id: NodeId },
    #[error("Raft member {node_id:?} has no admitted node descriptor")]
    RaftMemberDescriptorMissing { node_id: NodeId },
    #[error("Raft member {node_id:?} address does not match its admitted descriptor")]
    RaftMemberAddressMismatch { node_id: NodeId },
    #[error("node {node_id:?} cannot complete removal while it remains a Raft member")]
    RemovingNodeIsStillMember { node_id: NodeId },
    #[error("Raft voter target requires {expected} voters, not {actual}")]
    VoterTargetMismatch { expected: u16, actual: u16 },
    #[error("Raft voter {node_id:?} is not ACTIVE")]
    VoterNotActive { node_id: NodeId },
    #[error("peer address is already admitted")]
    PeerAddressAlreadyUsed,
    #[error("peer SPKI fingerprint is already admitted")]
    PeerSpkiAlreadyUsed,
    #[error("join capability hash is already admitted")]
    JoinCapabilityAlreadyUsed,
    #[error("current and overlap peer pins must differ")]
    PeerPinsMustDiffer,
    #[error("node {node_id:?} current peer pin does not match")]
    PeerCurrentPinMismatch { node_id: NodeId },
    #[error("node {node_id:?} already has another overlap peer pin")]
    PeerOverlapAlreadySet { node_id: NodeId },
    #[error("node {node_id:?} peer pin pair does not match")]
    PeerPinPairMismatch { node_id: NodeId },
    #[error("node {node_id:?} JOINING preparation pair does not match")]
    JoiningPreparationPairMismatch { node_id: NodeId },
    #[error("node {node_id:?} JOINING preparation replacement must change both identities")]
    JoiningPreparationUnchanged { node_id: NodeId },
    #[error("node {node_id:?} is already a committed Raft voter or learner")]
    JoiningNodeAlreadyRaftMember { node_id: NodeId },
    #[error("JWT signing-key fingerprint must be non-zero")]
    InvalidJwtSigningKeyFingerprint,
    #[error("JWT signing-key fingerprint is already bound to another value")]
    JwtSigningKeyFingerprintConflict {
        current: crate::JwtSigningKeyFingerprint,
        requested: crate::JwtSigningKeyFingerprint,
    },
    #[error("erasure-code profile requires non-zero K, M, and stripe unit with K+M <= 256")]
    InvalidErasureCodeProfile,
    #[error("erasure-code profile is already bound to another value")]
    ErasureCodeProfileConflict {
        current: crate::ErasureCodeProfile,
        requested: crate::ErasureCodeProfile,
    },
}
