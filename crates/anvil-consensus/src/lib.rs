//! Bounded distributed decisions for Anvil atomic programs.
//!
//! This crate retains one Raft-nominated executor and a bounded globally
//! ordered suffix of compact committed-batch references. Program objects,
//! object paths, payloads, version descriptors, prepared bundles, locks,
//! finalization evidence, and application data remain outside consensus.

mod codec;
mod peer;
mod peer_tls;
mod raft;
mod raft_storage;
mod state_machine;
mod types;

pub use peer::{
    InMemoryPeerTransport, PeerNode, PeerRpc, PeerRpcError, PeerRpcKind, PeerTransport,
    PeerTransportError, PeerTransportFuture,
};
pub use peer_tls::{
    AcceptedPeerTls, AuthenticatedPeer, CommittedPeerPinProvider, CommittedPeerPins,
    ConnectedPeerTls, DEFAULT_PEER_TLS_HANDSHAKE_TIMEOUT, PeerTlsAcceptor, PeerTlsConfig,
    PeerTlsConnector, PeerTlsError, PeerTlsIdentity, authorize_peer_rpc, peer_spki_sha256,
};
pub use raft::{CommittedDecision, DecisionRaft, DecisionRaftError};
pub use state_machine::{ApplyError, StateMachine};
pub use types::{
    ATOMIC_REPLAY_RETENTION_MILLIS, ApplyResult, BundleHash, BundleRef, ClusterId, Command,
    CommitBatch, CommitResult, CommittedBatch, CommittedInvocation, DurabilityClass,
    DurabilityEvidenceHash, ExecutorNomination, InvocationFingerprint, InvocationId,
    MAX_COMMITTED_INVOCATION_BYTES, MAX_COMMITTED_INVOCATIONS, NodeId, ProgramHash,
    ProgramPathHash, SYSTEM_BOOTSTRAP_VERSION, SystemBootstrapState,
};
