//! Bounded distributed decisions for Anvil atomic programs.
//!
//! This crate retains one Raft-nominated executor and a bounded globally
//! ordered suffix of compact committed-batch references. Program objects,
//! object paths, payloads, version descriptors, prepared bundles, locks,
//! finalization evidence, and application data remain outside consensus.

mod codec;
mod raft;
mod raft_storage;
mod state_machine;
mod types;

pub use raft::{
    CommittedDecision, DecisionRaft, DecisionRaftError, NoPeerTransport, PeerNode, PeerRpc,
    PeerRpcError, PeerRpcKind, PeerTransport, PeerTransportError,
};
pub use state_machine::{ApplyError, StateMachine};
pub use types::{
    ApplyResult, BundleHash, BundleRef, Command, CommitBatch, CommitResult, CommittedBatch,
    DurabilityClass, DurabilityEvidenceHash, ExecutorNomination, InvocationFingerprint,
    InvocationId, InvocationReceipt, NodeId, ProgramHash, ProgramPathHash,
};
