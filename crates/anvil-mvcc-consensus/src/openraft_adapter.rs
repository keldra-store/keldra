//! The only module permitted to name concrete OpenRaft application types.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    io::{self, Cursor},
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use async_trait::async_trait;
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState,
    OptionalSend, RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
    error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    AppliedDecision, CertificationError, CertificationResult, CertificationState,
    CertifyTransaction, ClusterControlState, CommitVersion, CommittedBundleDecision, Consensus,
    ConsensusCommand, ConsensusError, ControlApplyResult, NodeId, NodeIncarnation,
    NodeReplacementTransition, RaftStorageError, RocksRaftStore, TransactionId,
    binary_codec::{self, ValueKind},
    storage::{KEY_LAST_PURGED_LOG_ID, KEY_OPENRAFT_STATE},
};

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_RESTART_RECOVERY: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn fail_next_restart_recovery() {
    FAIL_NEXT_RESTART_RECOVERY.with(|failure| failure.set(true));
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RaftApplyResult {
    Certification(CertificationResult),
    Control(ControlApplyResult),
    Noop,
    Rejected(String),
}

openraft::declare_raft_types!(
    pub(crate) AnvilRaftConfig:
        D = ConsensusCommand,
        R = RaftApplyResult,
        NodeId = u64,
        Node = openraft::BasicNode,
);

pub(crate) type RaftEntry = Entry<AnvilRaftConfig>;

// OpenRaft 0.9.24 uses `heartbeat_interval` as both the cadence for leader
// heartbeats and the complete timeout for the quorum probe performed by
// `ensure_linearizable()`. Its 50 ms default is appropriate for an in-memory
// example, but is not a viable failure detector for Anvil's authenticated,
// multiplexed gRPC streams: a short scheduler or RocksDB stall can consume the
// entire allowance while every peer remains healthy.
//
// Keep the interval comfortably below the election window (Raft §5.2) while
// allowing a linearized read probe to survive ordinary process contention.
// These values do not weaken a read barrier: OpenRaft still requires a
// confirmation from a voter quorum in the current term before returning.
const ANVIL_RAFT_HEARTBEAT_INTERVAL_MS: u64 = 500;
const ANVIL_RAFT_ELECTION_TIMEOUT_MIN_MS: u64 = 1_500;
const ANVIL_RAFT_ELECTION_TIMEOUT_MAX_MS: u64 = 3_000;

fn production_raft_config(cluster_name: String) -> openraft::Config {
    openraft::Config {
        cluster_name,
        heartbeat_interval: ANVIL_RAFT_HEARTBEAT_INTERVAL_MS,
        election_timeout_min: ANVIL_RAFT_ELECTION_TIMEOUT_MIN_MS,
        election_timeout_max: ANVIL_RAFT_ELECTION_TIMEOUT_MAX_MS,
        // A certification command is bounded to 8 MiB. Four entries plus
        // encoding overhead remain comfortably inside the 64 MiB consensus
        // RPC envelope bound.
        max_payload_entries: 4,
        ..Default::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusNode {
    pub address: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusRpcKind {
    AppendEntries,
    Vote,
    InstallSnapshot,
    ForwardCertify,
    ForwardLinearizedRead,
    ForwardTransactionOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusRpc {
    pub schema_version: u16,
    pub kind: ConsensusRpcKind,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ConsensusRpcError {
    #[error("peer is unreachable: {0}")]
    Unreachable(String),
    #[error("consensus RPC failed: {0}")]
    Protocol(String),
}

#[async_trait]
pub trait ConsensusRpcClient: Send + Sync + 'static {
    async fn request(&mut self, rpc: ConsensusRpc) -> Result<Vec<u8>, ConsensusRpcError>;
}

/// Injectable transport boundary. Implementations own connection management;
/// this crate owns the OpenRaft wire types contained in the opaque payload.
pub trait ConsensusRpcFactory: Send + Sync + 'static {
    fn client(&self, target: NodeId, node: &ConsensusNode) -> Box<dyn ConsensusRpcClient>;
}

#[derive(Clone)]
struct NetworkFactoryAdapter {
    inner: Arc<dyn ConsensusRpcFactory>,
}

struct NetworkAdapter {
    client: Box<dyn ConsensusRpcClient>,
    target: u64,
    node: BasicNode,
}

impl RaftNetworkFactory<AnvilRaftConfig> for NetworkFactoryAdapter {
    type Network = NetworkAdapter;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let descriptor = ConsensusNode {
            address: node.addr.clone(),
        };
        NetworkAdapter {
            client: self.inner.client(NodeId(target), &descriptor),
            target,
            node: node.clone(),
        }
    }
}

impl NetworkAdapter {
    async fn call<Req, Resp, AppError>(
        &mut self,
        kind: ConsensusRpcKind,
        request: &Req,
    ) -> Result<Resp, RPCError<u64, BasicNode, RaftError<u64, AppError>>>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de>,
        AppError: std::error::Error + Serialize + for<'de> Deserialize<'de>,
    {
        let payload = encode_rpc_request(kind, request)
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        let response = self
            .client
            .request(ConsensusRpc {
                schema_version: 1,
                kind,
                payload,
            })
            .await
            .map_err(|error| match error {
                ConsensusRpcError::Unreachable(_) => {
                    RPCError::Unreachable(openraft::error::Unreachable::new(&error))
                }
                ConsensusRpcError::Protocol(_) => RPCError::Network(NetworkError::new(&error)),
            })?;
        let remote: Result<Resp, RaftError<u64, AppError>> =
            decode_rpc_response(kind, &response)
                .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        remote.map_err(|error| {
            RPCError::RemoteError(RemoteError::new_with_node(
                self.target,
                self.node.clone(),
                error,
            ))
        })
    }
}

impl RaftNetwork<AnvilRaftConfig> for NetworkAdapter {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<AnvilRaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.call(ConsensusRpcKind::AppendEntries, &rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<AnvilRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        self.call(ConsensusRpcKind::InstallSnapshot, &rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.call(ConsensusRpcKind::Vote, &rpc).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MachineState {
    certification: CertificationState,
    control: ClusterControlState,
    decisions: BTreeMap<CommitVersion, Option<CommittedBundleDecision>>,
    local_durability_violations: BTreeMap<CommitVersion, crate::LocalDurabilityViolation>,
    last_applied_log_id: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    snapshot_generation: u64,
}

impl MachineState {
    fn new(cluster_id_hash: [u8; 32]) -> Result<Self, RaftStorageError> {
        Ok(Self {
            certification: CertificationState::new(cluster_id_hash)
                .map_err(|error| RaftStorageError::Codec(error.to_string()))?,
            control: ClusterControlState::new(cluster_id_hash).map_err(RaftStorageError::Codec)?,
            decisions: BTreeMap::new(),
            local_durability_violations: BTreeMap::new(),
            last_applied_log_id: None,
            membership: StoredMembership::default(),
            snapshot_generation: 0,
        })
    }

    fn verify_cluster(&self, expected: [u8; 32]) -> Result<(), RaftStorageError> {
        if self.certification.cluster_id_hash() != expected {
            return Err(RaftStorageError::Codec(
                "persisted Raft state belongs to another cluster".into(),
            ));
        }
        if self.control.cluster_id_hash() != expected {
            return Err(RaftStorageError::Codec(
                "persisted control state belongs to another cluster".into(),
            ));
        }
        Ok(())
    }
}

/// A holder set intersects every election quorum exactly when its complement
/// cannot itself form an election quorum. For a joint membership, a valid
/// election quorum must contain a majority of every constituent voter set, so
/// holding at least half (rounded up) of any one constituent set is sufficient
/// to force that intersection.
fn holders_intersect_every_election_quorum(
    membership: &StoredMembership<u64, BasicNode>,
    holder_raft_ids: &BTreeSet<u64>,
) -> bool {
    membership
        .membership()
        .get_joint_config()
        .iter()
        .any(|voters| {
            !voters.is_empty()
                && voters
                    .iter()
                    .filter(|node_id| holder_raft_ids.contains(node_id))
                    .count()
                    >= voters.len().div_ceil(2)
        })
}

fn record_lost_local_holder(
    state: &mut MachineState,
    lost_holder: NodeIncarnation,
    detected_at_log_index: u64,
) {
    for (commit_version, decision) in &state.decisions {
        let Some(decision) = decision else {
            continue;
        };
        if decision.durability == crate::DurabilityLevel::Local
            && decision.durable_holders.contains(&lost_holder)
            && !decision.durable_holders.iter().any(|holder| {
                state.control.node_incarnation(holder.node_id) == Some(holder.incarnation)
            })
        {
            state
                .local_durability_violations
                .entry(*commit_version)
                .or_insert_with(|| crate::LocalDurabilityViolation {
                    commit_version: *commit_version,
                    bundle_hash: decision.bundle_hash,
                    lost_holder,
                    detected_at_log_index,
                });
        }
    }
}

fn latest_committed_bundle_version(
    decisions: &BTreeMap<CommitVersion, Option<CommittedBundleDecision>>,
) -> CommitVersion {
    decisions
        .iter()
        .rev()
        .find_map(|(position, bundle)| bundle.as_ref().map(|_| *position))
        .unwrap_or(CommitVersion(0))
}

fn durability_rank(durability: crate::DurabilityLevel) -> u8 {
    match durability {
        crate::DurabilityLevel::Local => 0,
        crate::DurabilityLevel::Quorum => 1,
        crate::DurabilityLevel::Erasure => 2,
    }
}

/// Concrete OpenRaft V2 log store backed by the existing RocksDB.
#[derive(Clone)]
pub(crate) struct OpenRaftLogStore {
    store: RocksRaftStore,
}

impl OpenRaftLogStore {
    /// Append and report completion through the same boundary used by
    /// OpenRaft's `LogFlushed`. The completion is invoked only after the
    /// synchronous RocksDB write has returned, and receives the durable write
    /// error when persistence fails.
    fn append_durable_with_completion<F>(
        &self,
        encoded: &[(u64, Vec<u8>)],
        completion: F,
    ) -> Result<(), RaftStorageError>
    where
        F: FnOnce(Result<(), io::Error>),
    {
        match self.store.append_logs(encoded) {
            Ok(()) => {
                completion(Ok(()));
                Ok(())
            }
            Err(error) => {
                completion(Err(io::Error::other(error.to_string())));
                Err(error)
            }
        }
    }
}

/// Concrete OpenRaft V2 state machine backed by the existing RocksDB.
#[derive(Clone)]
pub(crate) struct OpenRaftStateMachine {
    store: RocksRaftStore,
    cluster_id_hash: [u8; 32],
}

pub(crate) fn stores(
    store: RocksRaftStore,
    cluster_id_hash: [u8; 32],
) -> Result<(OpenRaftLogStore, OpenRaftStateMachine), RaftStorageError> {
    #[cfg(test)]
    if FAIL_NEXT_RESTART_RECOVERY.with(|failure| failure.replace(false)) {
        return Err(RaftStorageError::Codec(
            "injected RestartRecovery fault".into(),
        ));
    }
    let state = match store.read_state_value::<MachineState>(KEY_OPENRAFT_STATE)? {
        Some(state) => {
            state.verify_cluster(cluster_id_hash)?;
            state
        }
        None => {
            let state = MachineState::new(cluster_id_hash)?;
            store.sync_state_value(KEY_OPENRAFT_STATE, &state)?;
            state
        }
    };
    state.verify_cluster(cluster_id_hash)?;
    Ok((
        OpenRaftLogStore {
            store: store.clone(),
        },
        OpenRaftStateMachine {
            store,
            cluster_id_hash,
        },
    ))
}

/// Running consensus service. OpenRaft remains fully contained behind Anvil's
/// [`Consensus`] and transport interfaces.
#[derive(Clone)]
pub struct OpenRaftConsensus {
    raft: openraft::Raft<AnvilRaftConfig>,
    store: RocksRaftStore,
    network: Arc<dyn ConsensusRpcFactory>,
    // Weak entries keep the gate table bounded while every clone of this
    // runtime shares serialization for an active transaction attempt.
    certification_gates:
        Arc<std::sync::Mutex<BTreeMap<TransactionId, std::sync::Weak<tokio::sync::Mutex<()>>>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedControlSnapshot {
    pub topology_epoch: u64,
    pub nodes: Vec<(NodeId, NodeId, u64, String)>,
    pub retired_raft_node_ids: BTreeSet<NodeId>,
    pub node_replacements: Vec<NodeReplacementTransition>,
    pub partitions: Vec<(u64, crate::PartitionAssignment)>,
    pub durability_policy: crate::ConsensusDurabilityPolicy,
}

impl OpenRaftConsensus {
    fn certification_gate(&self, transaction_id: TransactionId) -> Arc<tokio::sync::Mutex<()>> {
        let mut gates = self
            .certification_gates
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        gates.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = gates
            .get(&transaction_id)
            .and_then(std::sync::Weak::upgrade)
        {
            return gate;
        }
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        gates.insert(transaction_id, Arc::downgrade(&gate));
        gate
    }

    fn applied_certification_retry(
        &self,
        command: &CertifyTransaction,
    ) -> Result<Option<CertificationResult>, ConsensusError> {
        let state = self
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
            .ok_or_else(|| ConsensusError::Storage("Raft state machine is missing".into()))?;
        if state.certification.cluster_id_hash() != command.cluster_id_hash {
            return Ok(None);
        }
        let Some(outcome) = state
            .certification
            .transaction_outcome(command.transaction_id)
        else {
            return Ok(None);
        };
        if outcome.result.bundle_hash() != command.bundle_hash
            || outcome.principal_hash != command.principal_hash
            || outcome.snapshot_version != command.snapshot_version
            || outcome.durability != command.durability
        {
            return Err(ConsensusError::Rejected(
                CertificationError::TransactionIdentityMismatch.to_string(),
            ));
        }
        Ok(Some(outcome.result.clone()))
    }

    fn transaction_outcome_locally(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<crate::TransactionOutcome>, ConsensusError> {
        let state = self
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
            .ok_or_else(|| ConsensusError::Storage("Raft state machine is missing".into()))?;
        Ok(state
            .certification
            .transaction_outcome(transaction_id)
            .cloned())
    }

    async fn linearized_transaction_outcome_locally(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<crate::TransactionOutcome>, ConsensusError> {
        self.linearized_read_barrier_locally().await?;
        self.transaction_outcome_locally(transaction_id)
    }

    async fn request_current_leader(
        &self,
        kind: ConsensusRpcKind,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, ConsensusError> {
        // Leadership may change between observing metrics and opening the
        // stream. Retrying from fresh metrics lets any cluster node remain a
        // transaction coordinator without making followers consensus leaders.
        let mut last_error = None;
        for _ in 0..3 {
            let (leader, node) = {
                let metrics = self.raft.metrics();
                let metrics = metrics.borrow();
                let leader = metrics.current_leader.ok_or_else(|| {
                    ConsensusError::Unavailable("consensus leader is not yet known".into())
                })?;
                let node = metrics
                    .membership_config
                    .membership()
                    .get_node(&leader)
                    .cloned()
                    .ok_or_else(|| {
                        ConsensusError::Unavailable(format!(
                            "consensus leader {leader} is absent from membership"
                        ))
                    })?;
                (leader, node)
            };
            if leader == self.raft.metrics().borrow().id {
                return Err(ConsensusError::ForwardToLeader);
            }
            let descriptor = ConsensusNode { address: node.addr };
            let mut client = self.network.client(NodeId(leader), &descriptor);
            match client
                .request(ConsensusRpc {
                    schema_version: 1,
                    kind,
                    payload: payload.clone(),
                })
                .await
            {
                Ok(response) => return Ok(response),
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        Err(ConsensusError::Unavailable(last_error.unwrap_or_else(
            || "consensus leader forwarding failed".into(),
        )))
    }

    pub fn applied_control_snapshot(&self) -> Result<AppliedControlSnapshot, ConsensusError> {
        let state = self
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
            .ok_or_else(|| ConsensusError::Storage("Raft state machine is missing".into()))?;
        Ok(AppliedControlSnapshot {
            topology_epoch: state.control.topology_epoch(),
            nodes: state
                .control
                .nodes()
                .map(|(id, raft_id, incarnation, domain)| {
                    (id, raft_id, incarnation, domain.to_string())
                })
                .collect(),
            retired_raft_node_ids: state.control.retired_raft_node_ids().collect(),
            node_replacements: state.control.replacement_transitions().collect(),
            partitions: state
                .control
                .partitions()
                .map(|(id, assignment)| (id, assignment.clone()))
                .collect(),
            durability_policy: state.control.durability_policy(),
        })
    }

    /// Return the voter IDs from the applied OpenRaft membership.
    ///
    /// Cluster-control code uses this applied view to make node replacement
    /// retries resume from a committed membership transition rather than from
    /// process-local configuration.
    pub fn applied_voter_ids(&self) -> Result<BTreeSet<NodeId>, ConsensusError> {
        let state = self
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
            .ok_or_else(|| ConsensusError::Storage("Raft state machine is missing".into()))?;
        Ok(state
            .membership
            .voter_ids()
            .map(NodeId)
            .collect::<BTreeSet<_>>())
    }

    pub fn applied_member_ids(&self) -> Result<BTreeSet<NodeId>, ConsensusError> {
        let state = self
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
            .ok_or_else(|| ConsensusError::Storage("Raft state machine is missing".into()))?;
        Ok(state
            .membership
            .nodes()
            .map(|(node_id, _)| NodeId(*node_id))
            .collect::<BTreeSet<_>>())
    }

    /// Latest retained consensus position that references ordinary product
    /// MVCC bundle bytes. Control and membership entries intentionally do not
    /// advance this product-data catch-up target.
    pub fn latest_committed_bundle_version(&self) -> Result<CommitVersion, ConsensusError> {
        let state = self
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
            .ok_or_else(|| ConsensusError::Storage("Raft state machine is missing".into()))?;
        Ok(latest_committed_bundle_version(&state.decisions))
    }

    pub fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        metrics.current_leader == Some(metrics.id)
    }

    /// Whether this runtime can still accept Raft RPCs.
    ///
    /// A transport session may outlive the server accept loop during an
    /// in-process restart. Exposing the authoritative OpenRaft running state
    /// lets that stale session terminate instead of continuing to dispatch to
    /// a stopped runtime.
    pub fn is_running(&self) -> bool {
        let metrics = self.raft.metrics();
        let running = metrics.borrow().running_state.is_ok();
        running
    }

    pub fn applied_decisions_after(
        &self,
        position: CommitVersion,
    ) -> Result<Vec<AppliedDecision>, ConsensusError> {
        let state = self
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
            .ok_or_else(|| ConsensusError::Storage("Raft state machine is missing".into()))?;
        Ok(state
            .decisions
            .range(CommitVersion(position.0.saturating_add(1))..)
            .map(|(position, bundle)| AppliedDecision {
                position: *position,
                committed_bundle: bundle.clone(),
            })
            .collect())
    }

    pub fn gc_safety_watermark(&self) -> Result<CommitVersion, ConsensusError> {
        let state = self
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
            .ok_or_else(|| ConsensusError::Storage("Raft state machine is missing".into()))?;
        Ok(state.control.gc_safety_watermark())
    }

    pub fn local_durability_violations(
        &self,
    ) -> Result<Vec<crate::LocalDurabilityViolation>, ConsensusError> {
        let state = self
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?
            .ok_or_else(|| ConsensusError::Storage("Raft state machine is missing".into()))?;
        Ok(state.local_durability_violations.into_values().collect())
    }

    async fn apply_control(
        &self,
        command: ConsensusCommand,
    ) -> Result<ControlApplyResult, ConsensusError> {
        command
            .validate_section9_boundary()
            .map_err(|reason| ConsensusError::Rejected(reason.into()))?;
        let response = self
            .raft
            .client_write(command)
            .await
            .map_err(map_raft_error)?;
        match response.data {
            RaftApplyResult::Control(result) => Ok(result),
            RaftApplyResult::Rejected(reason) => Err(ConsensusError::Rejected(reason)),
            _ => Err(ConsensusError::Rejected(
                "control command produced an unexpected response".into(),
            )),
        }
    }

    async fn certify_locally(
        &self,
        command: CertifyTransaction,
    ) -> Result<CertificationResult, ConsensusError> {
        ConsensusCommand::Certify(command.clone())
            .validate_section9_boundary()
            .map_err(|reason| ConsensusError::Rejected(reason.into()))?;
        let gate = self.certification_gate(command.transaction_id);
        let _guard = gate.lock().await;
        // A retry may arrive on a newly elected leader before its caller knows
        // whether the prior proposal committed. Confirm leadership and wait for
        // the applied state before deciding that this transaction is unseen.
        self.linearized_read_barrier_locally().await?;
        if let Some(result) = self.applied_certification_retry(&command)? {
            return Ok(result);
        }
        let response = self
            .raft
            .client_write(ConsensusCommand::Certify(command))
            .await
            .map_err(map_raft_error)?;
        match response.data {
            RaftApplyResult::Certification(result) => Ok(result),
            RaftApplyResult::Rejected(reason) => Err(ConsensusError::Rejected(reason)),
            RaftApplyResult::Control(_) => Err(ConsensusError::Rejected(
                "certification produced a control response".into(),
            )),
            RaftApplyResult::Noop => Err(ConsensusError::Rejected(
                "certification produced a non-application response".into(),
            )),
        }
    }

    pub async fn linearized_read_barrier_locally(&self) -> Result<CommitVersion, ConsensusError> {
        self.raft
            .ensure_linearizable()
            .await
            .map(|log_id| CommitVersion(log_id.map_or(0, |id| id.index)))
            .map_err(map_raft_error)
    }

    pub async fn install_node(
        &self,
        cluster_id_hash: [u8; 32],
        node: NodeIncarnation,
        raft_node_id: NodeId,
        failure_domain: String,
    ) -> Result<ControlApplyResult, ConsensusError> {
        self.apply_control(ConsensusCommand::InstallNode {
            cluster_id_hash,
            node,
            raft_node_id,
            failure_domain,
        })
        .await
    }

    pub async fn remove_node(
        &self,
        cluster_id_hash: [u8; 32],
        node: NodeIncarnation,
    ) -> Result<ControlApplyResult, ConsensusError> {
        self.apply_control(ConsensusCommand::RemoveNode {
            cluster_id_hash,
            node,
        })
        .await
    }

    pub async fn assign_partition(
        &self,
        cluster_id_hash: [u8; 32],
        partition_id: u64,
        owner: NodeIncarnation,
        epoch: u64,
    ) -> Result<ControlApplyResult, ConsensusError> {
        self.apply_control(ConsensusCommand::AssignPartition {
            cluster_id_hash,
            partition_id,
            owner,
            epoch,
        })
        .await
    }

    pub async fn set_durability_policy(
        &self,
        cluster_id_hash: [u8; 32],
        generation: u64,
        bundle_quorum_holders: u16,
        tolerated_failure_domains: u16,
    ) -> Result<ControlApplyResult, ConsensusError> {
        self.apply_control(ConsensusCommand::SetDurabilityPolicy {
            cluster_id_hash,
            generation,
            bundle_quorum_holders,
            tolerated_failure_domains,
        })
        .await
    }

    pub async fn advance_gc_watermark(
        &self,
        cluster_id_hash: [u8; 32],
        watermark: CommitVersion,
    ) -> Result<ControlApplyResult, ConsensusError> {
        self.apply_control(ConsensusCommand::AdvanceGcWatermark {
            cluster_id_hash,
            watermark,
        })
        .await
    }

    pub async fn upgrade_durability(
        &self,
        cluster_id_hash: [u8; 32],
        commit_version: CommitVersion,
        bundle_hash: crate::BundleHash,
        durability: crate::DurabilityLevel,
        durable_holders: Vec<NodeIncarnation>,
    ) -> Result<ControlApplyResult, ConsensusError> {
        self.apply_control(ConsensusCommand::UpgradeDurability {
            cluster_id_hash,
            commit_version,
            bundle_hash,
            durability,
            durable_holders,
        })
        .await
    }

    /// Handle one opaque RPC received by the injected transport.
    pub async fn handle_rpc(&self, rpc: ConsensusRpc) -> Result<Vec<u8>, ConsensusRpcError> {
        if rpc.schema_version != 1 {
            return Err(ConsensusRpcError::Protocol(format!(
                "unsupported consensus RPC schema {}",
                rpc.schema_version
            )));
        }
        match rpc.kind {
            ConsensusRpcKind::AppendEntries => {
                let request: AppendEntriesRequest<AnvilRaftConfig> =
                    decode_rpc_request(rpc.kind, &rpc.payload)?;
                encode_rpc_response(rpc.kind, &self.raft.append_entries(request).await)
            }
            ConsensusRpcKind::Vote => {
                let request: VoteRequest<u64> = decode_rpc_request(rpc.kind, &rpc.payload)?;
                encode_rpc_response(rpc.kind, &self.raft.vote(request).await)
            }
            ConsensusRpcKind::InstallSnapshot => {
                let request: InstallSnapshotRequest<AnvilRaftConfig> =
                    decode_rpc_request(rpc.kind, &rpc.payload)?;
                encode_rpc_response(rpc.kind, &self.raft.install_snapshot(request).await)
            }
            ConsensusRpcKind::ForwardCertify => {
                let command: CertifyTransaction = decode_rpc_request(rpc.kind, &rpc.payload)?;
                let response = self
                    .certify_locally(command)
                    .await
                    .map_err(|error| ConsensusRpcError::Protocol(error.to_string()))?;
                encode_rpc_response(rpc.kind, &response)
            }
            ConsensusRpcKind::ForwardLinearizedRead => {
                if !rpc.payload.is_empty() {
                    return Err(ConsensusRpcError::Protocol(
                        "linearized-read request payload must be empty".into(),
                    ));
                }
                let response = self
                    .linearized_read_barrier_locally()
                    .await
                    .map_err(|error| ConsensusRpcError::Protocol(error.to_string()))?;
                encode_rpc_response(rpc.kind, &response)
            }
            ConsensusRpcKind::ForwardTransactionOutcome => {
                let transaction_id: TransactionId = decode_rpc_request(rpc.kind, &rpc.payload)?;
                let response = self
                    .linearized_transaction_outcome_locally(transaction_id)
                    .await
                    .map_err(|error| ConsensusRpcError::Protocol(error.to_string()))?;
                encode_rpc_response(rpc.kind, &response)
            }
        }
    }

    /// Returns the consensus-certified terminal outcome after a leader
    /// linearization barrier.
    ///
    /// Followers ask the current leader to perform both the barrier and the
    /// state lookup. Reading follower-local state after merely forwarding a
    /// barrier would be unsafe because that follower may not yet have applied
    /// the returned log position.
    pub async fn linearized_transaction_outcome(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<crate::TransactionOutcome>, ConsensusError> {
        match self
            .linearized_transaction_outcome_locally(transaction_id)
            .await
        {
            Ok(outcome) => Ok(outcome),
            Err(ConsensusError::ForwardToLeader) => {
                let payload = encode_rpc_request(
                    ConsensusRpcKind::ForwardTransactionOutcome,
                    &transaction_id,
                )
                .map_err(|error| ConsensusError::Unavailable(error.to_string()))?;
                let response = match self
                    .request_current_leader(ConsensusRpcKind::ForwardTransactionOutcome, payload)
                    .await
                {
                    Ok(response) => response,
                    Err(ConsensusError::ForwardToLeader) => {
                        return self
                            .linearized_transaction_outcome_locally(transaction_id)
                            .await;
                    }
                    Err(error) => return Err(error),
                };
                decode_rpc_response(ConsensusRpcKind::ForwardTransactionOutcome, &response)
                    .map_err(|error| ConsensusError::Unavailable(error.to_string()))
            }
            Err(error) => Err(error),
        }
    }

    pub async fn from_db(
        node_id: NodeId,
        db: Arc<rocksdb::DB>,
        group_id: u64,
        cluster_id_hash: [u8; 32],
        cluster_name: impl Into<String>,
        network: Arc<dyn ConsensusRpcFactory>,
    ) -> Result<Self, ConsensusError> {
        let store = RocksRaftStore::from_db(db, group_id)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?;
        Self::new(node_id, store, cluster_id_hash, cluster_name, network).await
    }

    pub async fn new(
        node_id: NodeId,
        store: RocksRaftStore,
        cluster_id_hash: [u8; 32],
        cluster_name: impl Into<String>,
        network: Arc<dyn ConsensusRpcFactory>,
    ) -> Result<Self, ConsensusError> {
        let config = production_raft_config(cluster_name.into());
        Self::new_with_config(node_id, store, cluster_id_hash, config, network).await
    }

    async fn new_with_config(
        node_id: NodeId,
        store: RocksRaftStore,
        cluster_id_hash: [u8; 32],
        config: openraft::Config,
        network: Arc<dyn ConsensusRpcFactory>,
    ) -> Result<Self, ConsensusError> {
        let config = config
            .validate()
            .map_err(|error| ConsensusError::Rejected(error.to_string()))?;
        let (log_store, state_machine) = stores(store, cluster_id_hash)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?;
        let runtime_store = state_machine.store.clone();
        let raft = openraft::Raft::new(
            node_id.0,
            Arc::new(config),
            NetworkFactoryAdapter {
                inner: network.clone(),
            },
            log_store,
            state_machine,
        )
        .await
        .map_err(|error| ConsensusError::Storage(error.to_string()))?;
        Ok(Self {
            raft,
            store: runtime_store,
            network,
            certification_gates: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        })
    }

    pub async fn initialize(
        &self,
        members: BTreeMap<NodeId, ConsensusNode>,
    ) -> Result<(), ConsensusError> {
        let members = members
            .into_iter()
            .map(|(id, node)| (id.0, BasicNode::new(node.address)))
            .collect::<BTreeMap<_, _>>();
        self.raft
            .initialize(members)
            .await
            .map_err(|error| ConsensusError::Unavailable(error.to_string()))
    }

    pub async fn add_learner(
        &self,
        node_id: NodeId,
        node: ConsensusNode,
        blocking: bool,
    ) -> Result<(), ConsensusError> {
        self.raft
            .add_learner(node_id.0, BasicNode::new(node.address), blocking)
            .await
            .map(|_| ())
            .map_err(map_raft_error)
    }

    pub async fn change_membership(
        &self,
        voters: BTreeSet<NodeId>,
        retain_removed_as_learners: bool,
    ) -> Result<(), ConsensusError> {
        let voters = voters.into_iter().map(|id| id.0).collect::<BTreeSet<_>>();
        self.raft
            .change_membership(voters, retain_removed_as_learners)
            .await
            .map(|_| ())
            .map_err(map_raft_error)
    }

    pub async fn shutdown(&self) -> Result<(), ConsensusError> {
        self.raft
            .shutdown()
            .await
            .map_err(|error| ConsensusError::Unavailable(error.to_string()))
    }
}

fn rpc_request_value_kind(kind: ConsensusRpcKind) -> Option<ValueKind> {
    match kind {
        ConsensusRpcKind::AppendEntries => Some(ValueKind::AppendEntriesRequest),
        ConsensusRpcKind::Vote => Some(ValueKind::VoteRequest),
        ConsensusRpcKind::InstallSnapshot => Some(ValueKind::InstallSnapshotRequest),
        ConsensusRpcKind::ForwardCertify => Some(ValueKind::ForwardCertifyRequest),
        ConsensusRpcKind::ForwardLinearizedRead => None,
        ConsensusRpcKind::ForwardTransactionOutcome => {
            Some(ValueKind::ForwardTransactionOutcomeRequest)
        }
    }
}

fn rpc_response_value_kind(kind: ConsensusRpcKind) -> ValueKind {
    match kind {
        ConsensusRpcKind::AppendEntries => ValueKind::AppendEntriesResponse,
        ConsensusRpcKind::Vote => ValueKind::VoteResponse,
        ConsensusRpcKind::InstallSnapshot => ValueKind::InstallSnapshotResponse,
        ConsensusRpcKind::ForwardCertify => ValueKind::ForwardCertifyResponse,
        ConsensusRpcKind::ForwardLinearizedRead => ValueKind::ForwardLinearizedReadResponse,
        ConsensusRpcKind::ForwardTransactionOutcome => ValueKind::ForwardTransactionOutcomeResponse,
    }
}

fn decode_rpc_request<T: DeserializeOwned>(
    kind: ConsensusRpcKind,
    bytes: &[u8],
) -> Result<T, ConsensusRpcError> {
    let value_kind = rpc_request_value_kind(kind).ok_or_else(|| {
        ConsensusRpcError::Protocol("this consensus request has no encoded payload".into())
    })?;
    binary_codec::decode(value_kind, bytes)
        .map_err(|error| ConsensusRpcError::Protocol(error.to_string()))
}

fn encode_rpc_request<T: Serialize + ?Sized>(
    kind: ConsensusRpcKind,
    value: &T,
) -> Result<Vec<u8>, ConsensusRpcError> {
    let value_kind = rpc_request_value_kind(kind).ok_or_else(|| {
        ConsensusRpcError::Protocol("this consensus request has no encoded payload".into())
    })?;
    binary_codec::encode(value_kind, value)
        .map_err(|error| ConsensusRpcError::Protocol(error.to_string()))
}

fn decode_rpc_response<T: DeserializeOwned>(
    kind: ConsensusRpcKind,
    bytes: &[u8],
) -> Result<T, ConsensusRpcError> {
    binary_codec::decode(rpc_response_value_kind(kind), bytes)
        .map_err(|error| ConsensusRpcError::Protocol(error.to_string()))
}

fn encode_rpc_response<T: Serialize + ?Sized>(
    kind: ConsensusRpcKind,
    value: &T,
) -> Result<Vec<u8>, ConsensusRpcError> {
    binary_codec::encode(rpc_response_value_kind(kind), value)
        .map_err(|error| ConsensusRpcError::Protocol(error.to_string()))
}

fn map_raft_error<E>(error: openraft::error::RaftError<u64, E>) -> ConsensusError
where
    E: std::error::Error + openraft::TryAsRef<openraft::error::ForwardToLeader<u64, BasicNode>>,
{
    if error.forward_to_leader::<BasicNode>().is_some() {
        ConsensusError::ForwardToLeader
    } else {
        ConsensusError::Unavailable(error.to_string())
    }
}

#[async_trait]
impl Consensus for OpenRaftConsensus {
    async fn certify(
        &self,
        command: CertifyTransaction,
    ) -> Result<CertificationResult, ConsensusError> {
        match self.certify_locally(command.clone()).await {
            Ok(result) => Ok(result),
            Err(ConsensusError::ForwardToLeader) => {
                let payload = encode_rpc_request(ConsensusRpcKind::ForwardCertify, &command)
                    .map_err(|error| ConsensusError::Unavailable(error.to_string()))?;
                let response = match self
                    .request_current_leader(ConsensusRpcKind::ForwardCertify, payload)
                    .await
                {
                    Ok(response) => response,
                    Err(ConsensusError::ForwardToLeader) => {
                        return self.certify_locally(command).await;
                    }
                    Err(error) => return Err(error),
                };
                decode_rpc_response(ConsensusRpcKind::ForwardCertify, &response)
                    .map_err(|error| ConsensusError::Unavailable(error.to_string()))
            }
            Err(error) => Err(error),
        }
    }

    async fn linearized_read_barrier(&self) -> Result<CommitVersion, ConsensusError> {
        match self.linearized_read_barrier_locally().await {
            Ok(version) => Ok(version),
            Err(ConsensusError::ForwardToLeader) => {
                let response = match self
                    .request_current_leader(ConsensusRpcKind::ForwardLinearizedRead, Vec::new())
                    .await
                {
                    Ok(response) => response,
                    Err(ConsensusError::ForwardToLeader) => {
                        return self.linearized_read_barrier_locally().await;
                    }
                    Err(error) => return Err(error),
                };
                decode_rpc_response(ConsensusRpcKind::ForwardLinearizedRead, &response)
                    .map_err(|error| ConsensusError::Unavailable(error.to_string()))
            }
            Err(error) => Err(error),
        }
    }

    fn observed_commit_version(&self) -> CommitVersion {
        let metrics = self.raft.metrics();
        CommitVersion(
            metrics
                .borrow()
                .last_applied
                .map_or(0, |log_id| log_id.index),
        )
    }

    fn durability_policy(&self) -> Option<crate::ConsensusDurabilityPolicy> {
        self.applied_control_snapshot()
            .ok()
            .map(|snapshot| snapshot.durability_policy)
    }
}

fn storage_error(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    error: impl ToString,
) -> StorageError<u64> {
    StorageIOError::new(subject, verb, AnyError::error(error)).into()
}

fn read_error(error: RaftStorageError) -> StorageError<u64> {
    storage_error(ErrorSubject::Store, ErrorVerb::Read, error)
}

fn write_error(error: RaftStorageError) -> StorageError<u64> {
    storage_error(ErrorSubject::Store, ErrorVerb::Write, error)
}

fn range_start<R: RangeBounds<u64>>(range: &R) -> u64 {
    match range.start_bound() {
        Bound::Included(value) => *value,
        Bound::Excluded(value) => value.saturating_add(1),
        Bound::Unbounded => 0,
    }
}

fn range_end<R: RangeBounds<u64>>(range: &R) -> u64 {
    match range.end_bound() {
        Bound::Included(value) => value.saturating_add(1),
        Bound::Excluded(value) => *value,
        Bound::Unbounded => u64::MAX,
    }
}

impl RaftLogReader<AnvilRaftConfig> for OpenRaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<RaftEntry>, StorageError<u64>> {
        self.store
            .scan_logs(range_start(&range), range_end(&range))
            .map_err(read_error)?
            .into_iter()
            .map(|(_, bytes)| {
                binary_codec::decode(ValueKind::StoredLogEntry, &bytes)
                    .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Read, error))
            })
            .collect()
    }
}

impl RaftLogStorage<AnvilRaftConfig> for OpenRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<AnvilRaftConfig>, StorageError<u64>> {
        let last_purged_log_id = self
            .store
            .read_meta(KEY_LAST_PURGED_LOG_ID)
            .map_err(read_error)?;
        let last_log_id = match self.store.last_log_index().map_err(read_error)? {
            Some(index) => {
                let bytes = self
                    .store
                    .get_log(index)
                    .map_err(read_error)?
                    .ok_or_else(|| {
                        storage_error(
                            ErrorSubject::LogIndex(index),
                            ErrorVerb::Read,
                            "last log metadata points to a missing entry",
                        )
                    })?;
                let entry: RaftEntry = binary_codec::decode(ValueKind::StoredLogEntry, &bytes)
                    .map_err(|error| {
                        storage_error(ErrorSubject::LogIndex(index), ErrorVerb::Read, error)
                    })?;
                Some(entry.log_id)
            }
            None => last_purged_log_id,
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.store.save_vote(vote).map_err(write_error)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.store.read_vote().map_err(read_error)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<AnvilRaftConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = RaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let encoded = entries
            .into_iter()
            .map(|entry| {
                let index = entry.log_id.index;
                binary_codec::encode(ValueKind::StoredLogEntry, &entry)
                    .map(|bytes| (index, bytes))
                    .map_err(|error| {
                        storage_error(ErrorSubject::Log(entry.log_id), ErrorVerb::Write, error)
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.append_durable_with_completion(&encoded, |result| {
            callback.log_io_completed(result);
        })
        .map_err(|error| storage_error(ErrorSubject::Logs, ErrorVerb::Write, error))
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.store.truncate_logs(log_id.index).map_err(write_error)
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.store
            .purge_logs_with_id(log_id.index, &log_id)
            .map_err(write_error)
    }
}

#[derive(Clone)]
pub(crate) struct OpenRaftSnapshotBuilder {
    store: RocksRaftStore,
    cluster_id_hash: [u8; 32],
}

impl RaftSnapshotBuilder<AnvilRaftConfig> for OpenRaftSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<AnvilRaftConfig>, StorageError<u64>> {
        let mut state: MachineState = self
            .store
            .read_state_value(KEY_OPENRAFT_STATE)
            .map_err(read_error)?
            .ok_or_else(|| {
                storage_error(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    "initialized cluster state is missing",
                )
            })?;
        state
            .verify_cluster(self.cluster_id_hash)
            .map_err(read_error)?;
        state.snapshot_generation = state.snapshot_generation.saturating_add(1);
        self.store
            .sync_state_value(KEY_OPENRAFT_STATE, &state)
            .map_err(write_error)?;
        let bytes = binary_codec::encode(ValueKind::OpenRaftSnapshot, &state)
            .map_err(|error| storage_error(ErrorSubject::StateMachine, ErrorVerb::Read, error))?;
        let meta = SnapshotMeta {
            last_log_id: state.last_applied_log_id,
            last_membership: state.membership,
            snapshot_id: format!(
                "{}-{}",
                state.last_applied_log_id.map_or(0, |log_id| log_id.index),
                state.snapshot_generation
            ),
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

impl RaftStateMachine<AnvilRaftConfig> for OpenRaftStateMachine {
    type SnapshotBuilder = OpenRaftSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let state: MachineState = self
            .store
            .read_state_value(KEY_OPENRAFT_STATE)
            .map_err(read_error)?
            .ok_or_else(|| {
                storage_error(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    "initialized cluster state is missing",
                )
            })?;
        state
            .verify_cluster(self.cluster_id_hash)
            .map_err(read_error)?;
        Ok((state.last_applied_log_id, state.membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftApplyResult>, StorageError<u64>>
    where
        I: IntoIterator<Item = RaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut state: MachineState = self
            .store
            .read_state_value(KEY_OPENRAFT_STATE)
            .map_err(read_error)?
            .ok_or_else(|| {
                storage_error(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    "initialized cluster state is missing",
                )
            })?;
        state
            .verify_cluster(self.cluster_id_hash)
            .map_err(read_error)?;
        let mut responses = Vec::new();
        for entry in entries {
            let log_id = entry.log_id;
            let mut committed_bundle = None;
            let boundary_rejection = match &entry.payload {
                EntryPayload::Normal(command) => command
                    .validate_section9_boundary()
                    .err()
                    .map(str::to_string),
                EntryPayload::Blank | EntryPayload::Membership(_) => None,
            };
            let response = if let Some(reason) = boundary_rejection {
                RaftApplyResult::Rejected(reason)
            } else {
                match entry.payload {
                    EntryPayload::Blank => RaftApplyResult::Noop,
                    EntryPayload::Membership(membership) => {
                        state.membership = StoredMembership::new(Some(log_id), membership);
                        RaftApplyResult::Noop
                    }
                    EntryPayload::Normal(ConsensusCommand::Certify(command)) => {
                        let position = CommitVersion(log_id.index);
                        match state.certification.replay(position, &command) {
                            Ok(Some(result)) => RaftApplyResult::Certification(result),
                            Err(error) => RaftApplyResult::Rejected(error.to_string()),
                            Ok(None) => {
                                let policy = state.control.durability_policy();
                                let required_holders = match command.durability {
                                    crate::DurabilityLevel::Local => 1,
                                    crate::DurabilityLevel::Quorum
                                    | crate::DurabilityLevel::Erasure => {
                                        usize::from(policy.bundle_quorum_holders)
                                    }
                                };
                                let current_holders = command
                                    .durable_holders
                                    .iter()
                                    .filter(|holder| {
                                        state.control.node_incarnation(holder.node_id)
                                            == Some(holder.incarnation)
                                    })
                                    .collect::<Vec<_>>();
                                let holder_raft_ids = current_holders
                                    .iter()
                                    .filter_map(|holder| state.control.raft_node_id(holder.node_id))
                                    .map(|node_id| node_id.0)
                                    .collect::<BTreeSet<_>>();
                                let voter_safe =
                                    matches!(command.durability, crate::DurabilityLevel::Local)
                                        || holders_intersect_every_election_quorum(
                                            &state.membership,
                                            &holder_raft_ids,
                                        );
                                let assignment_conflict =
                                    command.assignment_predicates.iter().find_map(|predicate| {
                                        let current =
                                            state.control.partition(predicate.partition_id);
                                        (!current.is_some_and(|assignment| {
                                            assignment.epoch == predicate.assignment_epoch
                                                && assignment.owner == predicate.owner
                                        }))
                                        .then_some(
                                            crate::CertificationAbort::AssignmentConflict {
                                                partition_id: predicate.partition_id,
                                                expected_epoch: CommitVersion(
                                                    predicate.assignment_epoch,
                                                ),
                                                actual_epoch: current.map(|assignment| {
                                                    CommitVersion(assignment.epoch)
                                                }),
                                                expected_topology_epoch: CommitVersion(
                                                    predicate.topology_epoch,
                                                ),
                                                actual_topology_epoch: CommitVersion(
                                                    state.control.topology_epoch(),
                                                ),
                                            },
                                        )
                                    });
                                if policy.generation == 0
                                    || required_holders == 0
                                    || current_holders.len() < required_holders
                                    || !voter_safe
                                {
                                    RaftApplyResult::Rejected(
                                        "durability evidence violates applied Raft control state"
                                            .into(),
                                    )
                                } else if let Some(reason) = assignment_conflict {
                                    match state.certification.abort(position, &command, reason) {
                                        Ok(result) => RaftApplyResult::Certification(result),
                                        Err(error) => RaftApplyResult::Rejected(error.to_string()),
                                    }
                                } else {
                                    let result = state.certification.apply(position, &command);
                                    match result {
                                        Ok(result) => {
                                            if matches!(
                                                &result,
                                                CertificationResult::Committed {
                                                    commit_version,
                                                    ..
                                                } if *commit_version == position
                                            ) {
                                                committed_bundle = Some(CommittedBundleDecision {
                                                    cluster_id_hash: command.cluster_id_hash,
                                                    bundle_hash: command.bundle_hash,
                                                    bundle_length: command.bundle_length,
                                                    durability: command.durability,
                                                    durable_holders: command
                                                        .durable_holders
                                                        .clone(),
                                                });
                                            }
                                            RaftApplyResult::Certification(result)
                                        }
                                        Err(error) => RaftApplyResult::Rejected(error.to_string()),
                                    }
                                }
                            }
                        }
                    }
                    EntryPayload::Normal(ConsensusCommand::UpgradeDurability {
                        cluster_id_hash,
                        commit_version,
                        bundle_hash,
                        durability,
                        durable_holders,
                    }) => {
                        let policy = state.control.durability_policy();
                        let live_holders = durable_holders
                            .iter()
                            .filter(|holder| {
                                state.control.node_incarnation(holder.node_id)
                                    == Some(holder.incarnation)
                            })
                            .copied()
                            .collect::<BTreeSet<_>>();
                        let canonical_holders =
                            durable_holders.iter().copied().collect::<BTreeSet<_>>();
                        let holders_are_canonical =
                            canonical_holders.iter().copied().collect::<Vec<_>>()
                                == durable_holders;
                        let holder_raft_ids = live_holders
                            .iter()
                            .filter_map(|holder| state.control.raft_node_id(holder.node_id))
                            .map(|node_id| node_id.0)
                            .collect::<BTreeSet<_>>();
                        let required_holders = usize::from(policy.bundle_quorum_holders);
                        let valid_level = matches!(
                            durability,
                            crate::DurabilityLevel::Quorum | crate::DurabilityLevel::Erasure
                        );
                        let target = state
                            .decisions
                            .get_mut(&commit_version)
                            .and_then(Option::as_mut);
                        match target {
                        Some(decision)
                            if cluster_id_hash == self.cluster_id_hash
                                && decision.bundle_hash == bundle_hash
                                && valid_level
                                && holders_are_canonical
                                && durability_rank(durability)
                                    >= durability_rank(decision.durability)
                                && required_holders > 0
                                && live_holders.len() >= required_holders
                                && holders_intersect_every_election_quorum(
                                    &state.membership,
                                    &holder_raft_ids,
                                ) =>
                        {
                            decision.durability = durability;
                            decision.durable_holders = durable_holders;
                            state.local_durability_violations.remove(&commit_version);
                            RaftApplyResult::Control(ControlApplyResult::DurabilityUpgraded {
                                commit_version,
                                durability,
                            })
                        }
                        _ => RaftApplyResult::Rejected(
                            "durability upgrade evidence violates the retained outcome or applied membership"
                                .into(),
                        ),
                    }
                    }
                    EntryPayload::Normal(command) => {
                        if matches!(
                            &command,
                            ConsensusCommand::AdvanceGcWatermark { watermark, .. }
                                if *watermark > CommitVersion(log_id.index)
                        ) {
                            RaftApplyResult::Rejected(
                            "GC safety watermark cannot exceed its committed consensus position"
                                .into(),
                        )
                        } else {
                            let replaced_incarnation = match &command {
                                ConsensusCommand::InstallNode { node, .. } => state
                                    .control
                                    .node_incarnation(node.node_id)
                                    .filter(|current| *current != node.incarnation)
                                    .map(|incarnation| NodeIncarnation {
                                        node_id: node.node_id,
                                        incarnation,
                                    }),
                                _ => None,
                            };
                            match state.control.apply(&command) {
                                Ok(result) => {
                                    if let ControlApplyResult::NodeRemoved(removed) = &result {
                                        record_lost_local_holder(
                                            &mut state,
                                            *removed,
                                            log_id.index,
                                        );
                                    }
                                    if let Some(replaced) = replaced_incarnation {
                                        record_lost_local_holder(
                                            &mut state,
                                            replaced,
                                            log_id.index,
                                        );
                                    }
                                    if let ControlApplyResult::GcWatermarkAdvanced(watermark) =
                                        &result
                                    {
                                        state.certification.garbage_collect_results(*watermark);
                                        state
                                            .decisions
                                            .retain(|position, _| *position >= *watermark);
                                    }
                                    RaftApplyResult::Control(result)
                                }
                                Err(error) => RaftApplyResult::Rejected(error),
                            }
                        }
                    }
                }
            };
            state
                .decisions
                .insert(CommitVersion(log_id.index), committed_bundle);
            state.last_applied_log_id = Some(log_id);
            responses.push(response);
        }
        self.store
            .sync_state_value(KEY_OPENRAFT_STATE, &state)
            .map_err(write_error)?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        OpenRaftSnapshotBuilder {
            store: self.store.clone(),
            cluster_id_hash: self.cluster_id_hash,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let bytes = snapshot.into_inner();
        let state: MachineState = binary_codec::decode(ValueKind::OpenRaftSnapshot, &bytes)
            .map_err(|error| {
                storage_error(
                    ErrorSubject::Snapshot(Some(meta.signature())),
                    ErrorVerb::Read,
                    error,
                )
            })?;
        if state.last_applied_log_id != meta.last_log_id
            || state.membership != meta.last_membership
            || state.certification.cluster_id_hash() != self.cluster_id_hash
            || state.control.cluster_id_hash() != self.cluster_id_hash
        {
            return Err(storage_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                "snapshot body does not match its metadata or configured cluster",
            ));
        }
        self.store
            .sync_state_value(KEY_OPENRAFT_STATE, &state)
            .map_err(write_error)
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<AnvilRaftConfig>>, StorageError<u64>> {
        if self
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .map_err(read_error)?
            .is_none()
        {
            return Ok(None);
        }
        let mut builder = self.get_snapshot_builder().await;
        builder.build_snapshot().await.map(Some)
    }
}

#[cfg(test)]
#[path = "openraft_adapter/tests.rs"]
mod tests;
