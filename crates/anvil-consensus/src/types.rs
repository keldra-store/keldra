use serde::{Deserialize, Serialize};

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

/// Opaque location of an immutable prepared bundle in the distributed byte plane.
///
/// The reference is fixed-size so one consensus command cannot carry an
/// unbounded locator. Resolving the reference belongs to the storage layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BundleRef(pub [u8; 32]);

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

/// Replay receipt retained with one unfinalized commit reference.
///
/// The current core makes no time-based replay promise: `FinalizedThrough`
/// removes this receipt together with its commit reference. A separate receipt
/// window requires an explicit expiry design and is intentionally not implied
/// by this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationReceipt {
    pub invocation_id: InvocationId,
    pub input_fingerprint: InvocationFingerprint,
    pub committed_batch: CommittedBatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitResult {
    pub receipt: InvocationReceipt,
    /// `true` when the invocation was already present in the retained suffix.
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
    /// Prune committed batch references only after the caller has established
    /// the RFC's external recoverable-finalization criterion. This also ends
    /// the state machine's retained replay evidence for the pruned invocations;
    /// no independent receipt TTL is implemented here.
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
    FinalizationAdvanced {
        through_commit_cursor: u64,
        pruned_entries: u32,
        pruned_bytes: u64,
    },
}
