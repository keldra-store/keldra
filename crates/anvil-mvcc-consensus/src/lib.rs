//! Minimal consensus boundary for Anvil's MVCC transaction model.
//!
//! Product crates depend on the types and [`Consensus`] interface exported
//! here. OpenRaft and its storage contracts remain implementation details.

/// Maximum encoded payload carried by one consensus RPC or log-like envelope.
///
/// Transport implementations must allow this payload plus their framing
/// overhead. Durable aggregate state and snapshots use a separate, larger
/// private bound because they can contain many individually bounded entries.
pub const MAX_CONSENSUS_RPC_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

mod binary_codec;
mod certification;
mod consensus;
mod control;
mod gc;
// Concrete OpenRaft types stay private until the network-backed Consensus
// runtime is constructed inside this crate.
#[allow(dead_code)]
mod openraft_adapter;
mod storage;
mod types;

pub use certification::{CertificationError, CertificationState};
pub use consensus::{Consensus, ConsensusError};
pub use control::{ClusterControlState, NodeReplacementTransition};
pub use gc::{GarbageCollectionPins, GarbageCollectionSafetyError};
pub use openraft_adapter::{
    AppliedControlSnapshot, ConsensusNode, ConsensusRpc, ConsensusRpcClient, ConsensusRpcError,
    ConsensusRpcFactory, ConsensusRpcKind, OpenRaftConsensus,
};
pub use storage::{
    CONSENSUS_COLUMN_FAMILIES, PersistedConsensusState, RaftStorageError, RocksRaftStore,
};
pub use types::{
    AppliedDecision, AssignmentPredicate, BundleHash, CertificationAbort, CertificationResult,
    CertifyTransaction, CommitVersion, CommittedBundleDecision, ConsensusCommand,
    ConsensusDurabilityPolicy, ControlApplyResult, DurabilityLevel, ExplicitPredicate,
    LocalDurabilityViolation, LogicalKeyHash, NodeId, NodeIncarnation, PartitionAssignment,
    PointObservation, PredicateKind, RangeConflictKey, RangeObservation, TransactionId,
    TransactionOutcome, WrittenPoint,
};
