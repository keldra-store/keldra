use serde::{Deserialize, Serialize};

/// Globally ordered MVCC commit coordinate.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct CommitVersion(pub u64);

/// Stable identity for one transaction attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TransactionId(pub [u8; 16]);

/// Content identity of an immutable transaction bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BundleHash(pub [u8; 32]);

/// Compact identity of one logical key. The transaction bundle retains the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LogicalKeyHash(pub [u8; 32]);

/// Compact identity of one deterministic range-stamp bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RangeConflictKey(pub [u8; 32]);

/// Stable node identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// A node identity fenced by the generation of its durable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeIncarnation {
    pub node_id: NodeId,
    pub incarnation: u64,
}

/// Physical durability that must be established before certification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurabilityLevel {
    Local,
    Quorum,
    Erasure,
}

/// Version observed for one logical key. `None` means absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PointObservation {
    pub key: LogicalKeyHash,
    pub observed_version: Option<CommitVersion>,
}

/// Stamp observed for a deterministic range-conflict bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RangeObservation {
    pub range: RangeConflictKey,
    pub observed_stamp: Option<CommitVersion>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PredicateKind {
    Unique,
    Exists,
    Absent,
    ValueHash([u8; 32]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ExplicitPredicate {
    pub key: LogicalKeyHash,
    pub kind: PredicateKind,
    pub observed_version: Option<CommitVersion>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WrittenPoint {
    pub key: LogicalKeyHash,
    pub value_hash: Option<[u8; 32]>,
}

/// Compact application entry placed in Raft.
///
/// It intentionally contains no transaction bundle body or product row value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertifyTransaction {
    pub cluster_id_hash: [u8; 32],
    pub transaction_id: TransactionId,
    pub snapshot_version: CommitVersion,
    pub point_observations: Vec<PointObservation>,
    pub range_observations: Vec<RangeObservation>,
    #[serde(default)]
    pub predicates: Vec<ExplicitPredicate>,
    pub written_point_keys: Vec<LogicalKeyHash>,
    #[serde(default)]
    pub written_points: Vec<WrittenPoint>,
    pub advanced_range_stamps: Vec<RangeConflictKey>,
    pub bundle_hash: BundleHash,
    pub bundle_length: u64,
    pub durability: DurabilityLevel,
    pub durable_holders: Vec<NodeIncarnation>,
}

/// Compact state-machine commands. No variant may contain product data,
/// transaction bundle bytes, mesh topology, regions, or network endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusCommand {
    Certify(CertifyTransaction),
    InstallNode {
        cluster_id_hash: [u8; 32],
        node: NodeIncarnation,
        failure_domain: String,
    },
    RemoveNode {
        cluster_id_hash: [u8; 32],
        node: NodeIncarnation,
    },
    AssignPartition {
        cluster_id_hash: [u8; 32],
        partition_id: u64,
        owner: NodeIncarnation,
        epoch: u64,
    },
    SetDurabilityPolicy {
        cluster_id_hash: [u8; 32],
        generation: u64,
        bundle_quorum_holders: u16,
        tolerated_failure_domains: u16,
    },
    AdvanceGcWatermark {
        cluster_id_hash: [u8; 32],
        watermark: CommitVersion,
    },
}

impl ConsensusCommand {
    pub fn cluster_id_hash(&self) -> [u8; 32] {
        match self {
            Self::Certify(command) => command.cluster_id_hash,
            Self::InstallNode {
                cluster_id_hash, ..
            }
            | Self::RemoveNode {
                cluster_id_hash, ..
            }
            | Self::AssignPartition {
                cluster_id_hash, ..
            }
            | Self::SetDurabilityPolicy {
                cluster_id_hash, ..
            }
            | Self::AdvanceGcWatermark {
                cluster_id_hash, ..
            } => *cluster_id_hash,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub owner: NodeIncarnation,
    pub epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConsensusDurabilityPolicy {
    pub generation: u64,
    pub bundle_quorum_holders: u16,
    pub tolerated_failure_domains: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlApplyResult {
    NodeInstalled(NodeIncarnation),
    NodeRemoved(NodeIncarnation),
    PartitionAssigned {
        partition_id: u64,
        assignment: PartitionAssignment,
    },
    DurabilityPolicySet(ConsensusDurabilityPolicy),
    GcWatermarkAdvanced(CommitVersion),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedBundleDecision {
    pub cluster_id_hash: [u8; 32],
    pub bundle_hash: BundleHash,
    pub bundle_length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedDecision {
    pub position: CommitVersion,
    pub committed_bundle: Option<CommittedBundleDecision>,
}

/// Stable reason for a transaction certification abort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CertificationAbort {
    InvalidCommand(String),
    PointConflict {
        key: LogicalKeyHash,
        expected: Option<CommitVersion>,
        actual: Option<CommitVersion>,
    },
    PredicateConflict {
        key: LogicalKeyHash,
        predicate: PredicateKind,
        expected_version: Option<CommitVersion>,
        actual_version: Option<CommitVersion>,
    },
    RangeConflict {
        range: RangeConflictKey,
        expected: Option<CommitVersion>,
        actual: Option<CommitVersion>,
    },
}

/// Deterministic result stored for transaction retry deduplication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CertificationResult {
    Committed {
        commit_version: CommitVersion,
        bundle_hash: BundleHash,
    },
    Aborted {
        at_version: CommitVersion,
        bundle_hash: BundleHash,
        reason: CertificationAbort,
    },
}

impl CertificationResult {
    pub fn bundle_hash(&self) -> BundleHash {
        match self {
            Self::Committed { bundle_hash, .. } | Self::Aborted { bundle_hash, .. } => *bundle_hash,
        }
    }
}
