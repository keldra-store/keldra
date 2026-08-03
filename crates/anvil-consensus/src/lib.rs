//! Bounded distributed decisions for Anvil atomic programs.
//!
//! This crate retains one Raft-nominated executor and a bounded globally
//! ordered suffix of compact committed-batch references. Program objects,
//! object paths, payloads, version descriptors, prepared bundles, locks,
//! finalization evidence, and application data remain outside consensus.

mod cluster_control;
mod codec;
mod membership;
mod peer;
mod peer_tls;
mod raft;
mod raft_storage;
mod serving_lease;
mod state_machine;
mod tonic_peer;
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
pub use raft::{
    CommittedDecision, DecisionRaft, DecisionRaftError, LEADER_QUORUM_PROOF_MAX_AGE,
    LeaderQuorumProof,
};
pub use serving_lease::{
    PendingServingLeaseRequest, SERVING_LEASE_CUTOVER_WAIT, SERVING_LEASE_MAX_LIFETIME,
    SERVING_LEASE_RENEW_INTERVAL, ServingLease, ServingLeaseError, ServingLeaseGrant,
    ServingLeaseGrantPause, ServingLeaseIssuer, ServingLeaseRequest, ServingLeaseState,
};
pub use state_machine::{ApplyError, StateMachine};
pub use tonic_peer::{TonicPeerTransport, TonicRaftPeerServer, TonicRaftPeerService};
pub use types::{
    ATOMIC_REPLAY_RETENTION_MILLIS, ApplyResult, BundleHash, BundleRef,
    CLUSTER_CONTROL_COMMAND_VERSION, CapabilityRange, ClusterControlState, ClusterId, Command,
    CommitBatch, CommitResult, CommittedBatch, CommittedInvocation, DurabilityClass,
    DurabilityEvidenceHash, ErasureCodeProfile, ExecutorNomination, FIXED_VOTER_TARGET,
    InvocationFingerprint, InvocationId, JoinCapabilityHash, JwtSigningKeyFingerprint,
    MAX_COMMITTED_INVOCATION_BYTES, MAX_COMMITTED_INVOCATIONS, MAX_PEER_ADDRESS_BYTES,
    MembershipTransition, MembershipTransitionKind, NodeDescriptor, NodeId, NodeState, PeerAddress,
    PeerSpkiSha256, ProgramHash, ProgramPathHash, SYSTEM_BOOTSTRAP_VERSION, SystemBootstrapState,
    UsedNodeIds,
};
