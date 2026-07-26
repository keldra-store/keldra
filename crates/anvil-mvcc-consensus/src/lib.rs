//! Minimal consensus boundary for Anvil's MVCC transaction model.
//!
//! Product crates depend on the types and [`Consensus`] interface exported
//! here. OpenRaft and its storage contracts remain implementation details.

mod certification;
mod consensus;
// Concrete OpenRaft types stay private until the network-backed Consensus
// runtime is constructed inside this crate.
#[allow(dead_code)]
mod openraft_adapter;
mod storage;
mod types;

pub use certification::{CertificationError, CertificationState};
pub use consensus::{Consensus, ConsensusError};
pub use storage::{
    CONSENSUS_COLUMN_FAMILIES, PersistedConsensusState, RaftStorageError, RocksRaftStore,
};
pub use types::{
    BundleHash, CertificationAbort, CertificationResult, CertifyTransaction, CommitVersion,
    DurabilityLevel, LogicalKeyHash, NodeId, NodeIncarnation, PointObservation, RangeConflictKey,
    RangeObservation, TransactionId,
};
