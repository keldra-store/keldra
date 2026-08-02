use serde::{Deserialize, Serialize};

pub const ATOMIC_REPLAY_RETENTION_MILLIS: u64 = 24 * 60 * 60 * 1_000;
pub const MAX_COMMITTED_INVOCATIONS: u32 = 4_096;
pub const MAX_COMMITTED_INVOCATION_BYTES: u64 = 16 * 1024 * 1024;

/// Stable identity of one Raft voter or learner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// Compact identity of a canonical reserved program object path.
///
/// The immutable program object itself is stored through Anvil's ordinary
/// object API at a path such as `_anvil/programs/import_osv@1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProgramPathHash(pub [u8; 32]);

/// Content identity of an immutable, externally stored program definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProgramHash(pub [u8; 32]);

/// Ordinary content-addressed location of one immutable prepared bundle.
/// Hash plus length is the complete fixed-size key used by the byte plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BundleRef {
    pub hash: [u8; 32],
    pub length: u64,
}

/// Content identity used to verify an externally stored prepared bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BundleHash(pub [u8; 32]);

/// Content identity for a durability policy defined outside consensus.
///
/// The exact meaning and evidence requirements belong to the durability
/// capability. A full digest avoids a managed counter and ambiguous compact-ID
/// collisions while keeping the Raft command fixed-size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DurabilityClass(pub [u8; 32]);

/// Hash binding externally stored durability evidence for a prepared bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DurabilityEvidenceHash(pub [u8; 32]);

/// Caller-chosen stable identity for one logical atomic-program invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InvocationId(pub [u8; 32]);

/// Hash of the immutable logical invocation inputs.
///
/// The input bytes themselves never enter consensus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InvocationFingerprint(pub [u8; 32]);

/// The one cluster-wide atomic-program executor and its Raft-derived fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutorNomination {
    pub executor: NodeId,
    pub nomination_log_index: u64,
}

/// A compact request to commit one already prepared atomic-program batch.
///
/// Object paths, payloads, version descriptors, locks, and the bundle itself
/// remain outside Raft.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitBatch {
    pub executor: NodeId,
    pub nomination_log_index: u64,
    pub program_path_hash: ProgramPathHash,
    pub program_hash: ProgramHash,
    pub invocation_id: InvocationId,
    pub input_fingerprint: InvocationFingerprint,
    pub bundle_ref: BundleRef,
    pub bundle_hash: BundleHash,
    pub durability_class: DurabilityClass,
    pub durability_evidence_hash: DurabilityEvidenceHash,
    /// Executor wall-clock observation committed as data so every Raft apply
    /// prunes the same replay entries.
    pub proposal_at_unix_millis: u64,
    /// Exactly `proposal_at_unix_millis + ATOMIC_REPLAY_RETENTION_MILLIS`.
    pub replay_expires_at_unix_millis: u64,
}

/// One retained, globally ordered compact batch decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedBatch {
    /// The original committed Raft log index, retained across idempotent retry.
    pub commit_cursor: u64,
    pub executor: NodeId,
    pub nomination_log_index: u64,
    pub program_path_hash: ProgramPathHash,
    pub program_hash: ProgramHash,
    pub bundle_ref: BundleRef,
    pub bundle_hash: BundleHash,
    pub durability_class: DurabilityClass,
    pub durability_evidence_hash: DurabilityEvidenceHash,
}

/// One bounded committed invocation retained for replay independently of the
/// recovery watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedInvocation {
    pub invocation_id: InvocationId,
    pub input_fingerprint: InvocationFingerprint,
    pub proposal_at_unix_millis: u64,
    pub replay_expires_at_unix_millis: u64,
    pub committed_batch: CommittedBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitResult {
    pub invocation: CommittedInvocation,
    /// `true` when the invocation was already present in the replay window.
    pub replayed: bool,
}

/// Commands admitted to the replicated state machine.
///
/// There is deliberately no transaction lifecycle or payload-bearing command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Nominate one current voter or learner. Membership eligibility is checked
    /// by the OpenRaft adapter because the pure state machine does not own a
    /// second cluster-membership model.
    NominateExecutor {
        executor: NodeId,
    },
    CommitBatch(CommitBatch),
    /// Advance only the external recoverable-finalization watermark. Replay
    /// entries remain until a later CommitBatch deterministically expires them.
    FinalizedThrough {
        executor: NodeId,
        nomination_log_index: u64,
        through_commit_cursor: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyResult {
    ExecutorNominated(ExecutorNomination),
    BatchCommitted(CommitResult),
    FinalizationAdvanced { through_commit_cursor: u64 },
}
