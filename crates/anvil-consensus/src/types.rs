use serde::{Deserialize, Serialize};

pub const ATOMIC_REPLAY_RETENTION_MILLIS: u64 = 24 * 60 * 60 * 1_000;
pub const MAX_COMMITTED_INVOCATIONS: u32 = 4_096;
pub const MAX_COMMITTED_INVOCATION_BYTES: u64 = 16 * 1024 * 1024;
pub const SYSTEM_BOOTSTRAP_VERSION: u16 = 1;
pub const CLUSTER_CONTROL_COMMAND_VERSION: u16 = 1;
pub const MAX_PEER_ADDRESS_BYTES: usize = 255;
pub const FIXED_VOTER_TARGET: usize = 3;
pub(crate) const MAX_RAFT_NODE_ID: u64 = 1_023;
pub(crate) const USED_NODE_ID_WORDS: usize = 16;

/// Stable identity of one Raft voter or learner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// Stable identity of one Anvil cluster.
///
/// The identity is generated once when the Raft group is created and retained
/// in every state-machine snapshot. An all-zero value is reserved as invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClusterId(pub [u8; 16]);

impl From<[u8; 16]> for ClusterId {
    fn from(value: [u8; 16]) -> Self {
        Self(value)
    }
}

impl ClusterId {
    pub fn into_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Bounded address of one cluster-only peer listener.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerAddress(pub String);

/// Inclusive protocol or storage-format capability range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityRange {
    pub min: u16,
    pub max: u16,
}

/// SHA-256 of a peer certificate's DER-encoded subject-public-key information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerSpkiSha256(pub [u8; 32]);

/// One-way hash of the single-use capability admitted for a joining node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct JoinCapabilityHash(pub [u8; 32]);

/// Domain-separated BLAKE3 fingerprint of the operator-held HS256 secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtSigningKeyFingerprint(pub [u8; 32]);

/// One immutable cluster-wide large-payload erasure-code profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasureCodeProfile {
    pub data_shards: u16,
    pub parity_shards: u16,
    pub stripe_unit: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Joining,
    Active,
}

/// Bounded cluster-control record for one admitted node.
///
/// Voter or learner role remains solely in OpenRaft membership. `overlap` is
/// the one additional pin accepted during preparation or retirement; rotation
/// swaps the two slots before the old pin is cleared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDescriptor {
    pub node_id: NodeId,
    pub peer_address: PeerAddress,
    pub storage_weight_millionths: u32,
    pub state: NodeState,
    pub current_peer_spki_sha256: PeerSpkiSha256,
    pub overlap_peer_spki_sha256: Option<PeerSpkiSha256>,
    pub join_capability_hash: Option<JoinCapabilityHash>,
    pub supported_protocol: CapabilityRange,
    pub supported_storage_format: CapabilityRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipTransitionKind {
    Add,
    Remove,
    Reweight,
}

/// The one bounded cluster membership operation that may be in flight.
///
/// Object movement and progress inventories stay outside Raft. The current
/// descriptor and OpenRaft membership reveal which idempotent step remains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipTransition {
    pub kind: MembershipTransitionKind,
    pub node_id: NodeId,
    pub started_log_index: u64,
    pub target_weight_millionths: Option<u32>,
}

/// Fixed 1024-bit evidence that a stable node ID has ever been admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsedNodeIds(pub(crate) [u64; USED_NODE_ID_WORDS]);

impl Default for UsedNodeIds {
    fn default() -> Self {
        Self([0; USED_NODE_ID_WORDS])
    }
}

impl UsedNodeIds {
    pub fn contains(&self, node_id: NodeId) -> bool {
        let Ok(bit) = usize::try_from(node_id.0) else {
            return false;
        };
        let Some(word) = self.0.get(bit / u64::BITS as usize) else {
            return false;
        };
        word & (1_u64 << (bit % u64::BITS as usize)) != 0
    }

    pub(crate) fn insert(&mut self, node_id: NodeId) {
        let bit = node_id.0 as usize;
        self.0[bit / u64::BITS as usize] |= 1_u64 << (bit % u64::BITS as usize);
    }
}

/// Bounded cluster-control state retained by every Raft snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ClusterControlState {
    pub(crate) nodes: std::collections::BTreeMap<NodeId, NodeDescriptor>,
    pub(crate) used_node_ids: UsedNodeIds,
    pub(crate) transition: Option<MembershipTransition>,
    pub(crate) jwt_signing_key_fingerprint: Option<JwtSigningKeyFingerprint>,
    pub(crate) erasure_code_profile: Option<ErasureCodeProfile>,
}

impl ClusterControlState {
    pub fn nodes(&self) -> &std::collections::BTreeMap<NodeId, NodeDescriptor> {
        &self.nodes
    }

    pub fn used_node_ids(&self) -> &UsedNodeIds {
        &self.used_node_ids
    }

    pub fn transition(&self) -> Option<&MembershipTransition> {
        self.transition.as_ref()
    }

    pub fn jwt_signing_key_fingerprint(&self) -> Option<JwtSigningKeyFingerprint> {
        self.jwt_signing_key_fingerprint
    }

    pub fn erasure_code_profile(&self) -> Option<ErasureCodeProfile> {
        self.erasure_code_profile
    }

    pub fn active_node_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|descriptor| descriptor.state == NodeState::Active)
            .count()
    }

    pub fn voter_target(&self) -> usize {
        self.active_node_count().min(FIXED_VOTER_TARGET)
    }
}

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

/// Whether Anvil's protected system identity has been durably bootstrapped.
///
/// `committed_log_index` is the original completion decision and is preserved
/// when an idempotent retry is applied at a later log index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemBootstrapState {
    Missing,
    Complete {
        version: u16,
        committed_log_index: u64,
    },
}

impl SystemBootstrapState {
    pub fn is_complete(self) -> bool {
        matches!(self, Self::Complete { .. })
    }

    pub fn version(self) -> Option<u16> {
        match self {
            Self::Missing => None,
            Self::Complete { version, .. } => Some(version),
        }
    }

    pub fn committed_log_index(self) -> Option<u64> {
        match self {
            Self::Missing => None,
            Self::Complete {
                committed_log_index,
                ..
            } => Some(committed_log_index),
        }
    }
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Set the stable cluster identity exactly once. This variant is appended
    /// to preserve all command discriminants released in Anvil 0.5.0.
    InitializeCluster {
        cluster_id: ClusterId,
    },
    /// Record that the nominated executor has durably written the protected
    /// system identity. This variant is appended for 0.5.0 compatibility.
    CompleteSystemBootstrap {
        executor: NodeId,
        nomination_log_index: u64,
        bootstrap_version: u16,
    },
    /// Admit one new identity as JOINING and begin its bounded handoff.
    BeginAddNode {
        format_version: u16,
        descriptor: NodeDescriptor,
    },
    /// Begin removing one ACTIVE node. Data movement remains outside Raft.
    BeginRemoveNode {
        format_version: u16,
        node_id: NodeId,
    },
    /// Begin changing one ACTIVE node's stable capacity weight.
    BeginReweightNode {
        format_version: u16,
        node_id: NodeId,
        storage_weight_millionths: u32,
    },
    /// Apply the descriptor cutover and clear the bounded transition.
    CompleteMembershipTransition {
        format_version: u16,
        started_log_index: u64,
    },
    /// Accept one second pin before a node switches its peer certificate.
    StagePeerSpkiOverlap {
        format_version: u16,
        node_id: NodeId,
        expected_current: PeerSpkiSha256,
        overlap: PeerSpkiSha256,
    },
    /// Switch the current and overlap slots while retaining the old pin.
    PromotePeerSpkiOverlap {
        format_version: u16,
        node_id: NodeId,
        expected_current: PeerSpkiSha256,
        expected_overlap: PeerSpkiSha256,
    },
    /// Remove the retiring overlap after every required peer has applied it.
    ClearPeerSpkiOverlap {
        format_version: u16,
        node_id: NodeId,
        expected_current: PeerSpkiSha256,
        expected_overlap: PeerSpkiSha256,
    },
    /// Bind the immutable fingerprint of the operator-held JWT signing key.
    BindJwtSigningKeyFingerprint {
        format_version: u16,
        fingerprint: JwtSigningKeyFingerprint,
    },
    /// Bind the one immutable large-payload erasure-code geometry.
    BindErasureCodeProfile {
        format_version: u16,
        profile: ErasureCodeProfile,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyResult {
    ExecutorNominated(ExecutorNomination),
    BatchCommitted(CommitResult),
    FinalizationAdvanced { through_commit_cursor: u64 },
    ClusterInitialized { cluster_id: ClusterId },
    SystemBootstrapCompleted(SystemBootstrapState),
    MembershipTransitionBegun(MembershipTransition),
    MembershipTransitionAdvanced(MembershipTransition),
    MembershipTransitionFinished { started_log_index: u64 },
    PeerSpkiChanged(NodeDescriptor),
    JwtSigningKeyFingerprintBound(JwtSigningKeyFingerprint),
    ErasureCodeProfileBound(ErasureCodeProfile),
}
