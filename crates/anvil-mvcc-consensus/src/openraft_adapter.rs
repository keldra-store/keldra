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
use serde::{Deserialize, Serialize};

use crate::{
    AppliedDecision, CertificationError, CertificationResult, CertificationState,
    CertifyTransaction, ClusterControlState, CommitVersion, CommittedBundleDecision, Consensus,
    ConsensusCommand, ConsensusError, ControlApplyResult, NodeId, NodeIncarnation,
    RaftStorageError, RocksRaftStore, TransactionId,
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
        let payload = bincode::serde::encode_to_vec(request, bincode::config::standard())
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
            bincode::serde::decode_from_slice(&response, bincode::config::standard())
                .map(|(value, _)| value)
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
        let Some(result) = state
            .certification
            .transaction_result(command.transaction_id)
            .cloned()
        else {
            return Ok(None);
        };
        if result.bundle_hash() != command.bundle_hash {
            return Err(ConsensusError::Rejected(
                CertificationError::TransactionIdentityMismatch.to_string(),
            ));
        }
        Ok(Some(result))
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
            partitions: state
                .control
                .partitions()
                .map(|(id, assignment)| (id, assignment.clone()))
                .collect(),
            durability_policy: state.control.durability_policy(),
        })
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

    async fn linearized_read_barrier_locally(&self) -> Result<CommitVersion, ConsensusError> {
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
                let request: AppendEntriesRequest<AnvilRaftConfig> = decode_rpc(&rpc.payload)?;
                encode_rpc(&self.raft.append_entries(request).await)
            }
            ConsensusRpcKind::Vote => {
                let request: VoteRequest<u64> = decode_rpc(&rpc.payload)?;
                encode_rpc(&self.raft.vote(request).await)
            }
            ConsensusRpcKind::InstallSnapshot => {
                let request: InstallSnapshotRequest<AnvilRaftConfig> = decode_rpc(&rpc.payload)?;
                encode_rpc(&self.raft.install_snapshot(request).await)
            }
            ConsensusRpcKind::ForwardCertify => {
                let command: CertifyTransaction = decode_rpc(&rpc.payload)?;
                let response = self
                    .certify_locally(command)
                    .await
                    .map_err(|error| ConsensusRpcError::Protocol(error.to_string()))?;
                encode_rpc(&response)
            }
            ConsensusRpcKind::ForwardLinearizedRead => {
                let response = self
                    .linearized_read_barrier_locally()
                    .await
                    .map_err(|error| ConsensusRpcError::Protocol(error.to_string()))?;
                encode_rpc(&response)
            }
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

fn decode_rpc<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ConsensusRpcError> {
    let (value, consumed) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|error| ConsensusRpcError::Protocol(error.to_string()))?;
    if consumed != bytes.len() {
        return Err(ConsensusRpcError::Protocol(
            "trailing bytes in consensus RPC".into(),
        ));
    }
    Ok(value)
}

fn encode_rpc<T: Serialize>(value: &T) -> Result<Vec<u8>, ConsensusRpcError> {
    bincode::serde::encode_to_vec(value, bincode::config::standard())
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
                let payload = encode_rpc(&command)
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
                decode_rpc(&response)
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
                    .request_current_leader(
                        ConsensusRpcKind::ForwardLinearizedRead,
                        encode_rpc(&())
                            .map_err(|error| ConsensusError::Unavailable(error.to_string()))?,
                    )
                    .await
                {
                    Ok(response) => response,
                    Err(ConsensusError::ForwardToLeader) => {
                        return self.linearized_read_barrier_locally().await;
                    }
                    Err(error) => return Err(error),
                };
                decode_rpc(&response)
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
                bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                    .map(|(entry, _)| entry)
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
                let (entry, _): (RaftEntry, _) =
                    bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
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
                bincode::serde::encode_to_vec(&entry, bincode::config::standard())
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
        let bytes = bincode::serde::encode_to_vec(&state, bincode::config::standard())
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
        let (state, consumed): (MachineState, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).map_err(
                |error| {
                    storage_error(
                        ErrorSubject::Snapshot(Some(meta.signature())),
                        ErrorVerb::Read,
                        error,
                    )
                },
            )?;
        if consumed != bytes.len()
            || state.last_applied_log_id != meta.last_log_id
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
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use openraft::{CommittedLeaderId, Membership, RaftTypeConfig, storage::RaftLogStorage};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        BundleHash, DurabilityLevel, NodeId, NodeIncarnation, TransactionId,
        storage::fail_sync_write_at,
    };

    struct NoRemoteFactory;

    impl ConsensusRpcFactory for NoRemoteFactory {
        fn client(&self, _target: NodeId, _node: &ConsensusNode) -> Box<dyn ConsensusRpcClient> {
            panic!("single-node runtime must not create a remote client")
        }
    }

    #[test]
    fn application_payload_is_the_compact_certification_command() {
        fn assert_data_type<C: RaftTypeConfig<D = ConsensusCommand>>() {}
        fn assert_response_type<C: RaftTypeConfig<R = RaftApplyResult>>() {}
        assert_data_type::<AnvilRaftConfig>();
        assert_response_type::<AnvilRaftConfig>();
    }

    #[test]
    fn production_timing_allows_networked_linearized_read_confirmation() {
        let config = production_raft_config("cluster-a".into());

        assert_eq!(config.heartbeat_interval, ANVIL_RAFT_HEARTBEAT_INTERVAL_MS);
        assert_eq!(
            config.election_timeout_min,
            ANVIL_RAFT_ELECTION_TIMEOUT_MIN_MS
        );
        assert_eq!(
            config.election_timeout_max,
            ANVIL_RAFT_ELECTION_TIMEOUT_MAX_MS
        );
        assert!(config.election_timeout_min >= config.heartbeat_interval * 3);
        config.validate().expect("production Raft timing is valid");
    }

    #[test]
    fn concrete_types_implement_openraft_v2_storage_contracts() {
        fn assert_log<T: RaftLogStorage<AnvilRaftConfig>>() {}
        fn assert_state<T: RaftStateMachine<AnvilRaftConfig>>() {}
        assert_log::<OpenRaftLogStore>();
        assert_state::<OpenRaftStateMachine>();

        let directory = TempDir::new().unwrap();
        let (log, state) =
            stores(RocksRaftStore::open(directory.path(), 0).unwrap(), [1; 32]).unwrap();
        drop((log, state));
    }

    #[test]
    fn bundle_holders_must_intersect_every_regular_election_quorum() {
        let membership = StoredMembership::new(
            None,
            Membership::new(
                vec![BTreeSet::from([1, 2, 3, 4, 5])],
                BTreeSet::from([1, 2, 3, 4, 5, 9]),
            ),
        );
        assert!(holders_intersect_every_election_quorum(
            &membership,
            &BTreeSet::from([1, 2, 3]),
        ));
        assert!(!holders_intersect_every_election_quorum(
            &membership,
            &BTreeSet::from([1, 2, 9]),
        ));
    }

    #[test]
    fn joint_membership_uses_voters_and_never_counts_arbitrary_learners() {
        let membership = StoredMembership::new(
            None,
            Membership::new(
                vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([3, 4, 5])],
                BTreeSet::from([1, 2, 3, 4, 5, 9, 10]),
            ),
        );
        assert!(holders_intersect_every_election_quorum(
            &membership,
            &BTreeSet::from([1, 2, 9]),
        ));
        assert!(!holders_intersect_every_election_quorum(
            &membership,
            &BTreeSet::from([1, 9, 10]),
        ));
    }

    #[tokio::test]
    async fn upgrades_prevent_false_loss_and_incarnation_replacement_is_detected() {
        let directory = TempDir::new().unwrap();
        let store = RocksRaftStore::open(directory.path(), 0).unwrap();
        let (_, mut machine) = stores(store.clone(), [1; 32]).unwrap();
        let leader = CommittedLeaderId::new(1, 1);
        let holder = NodeIncarnation {
            node_id: NodeId(11),
            incarnation: 1,
        };
        let survivor = NodeIncarnation {
            node_id: NodeId(22),
            incarnation: 1,
        };
        machine
            .apply([
                Entry {
                    log_id: LogId::new(leader, 1),
                    payload: EntryPayload::Membership(Membership::new(
                        vec![BTreeSet::from([1, 2])],
                        (),
                    )),
                },
                Entry {
                    log_id: LogId::new(leader, 2),
                    payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                        cluster_id_hash: [1; 32],
                        node: holder,
                        raft_node_id: NodeId(1),
                        failure_domain: "zone-a".into(),
                    }),
                },
                Entry {
                    log_id: LogId::new(leader, 3),
                    payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                        cluster_id_hash: [1; 32],
                        node: survivor,
                        raft_node_id: NodeId(2),
                        failure_domain: "zone-b".into(),
                    }),
                },
                Entry {
                    log_id: LogId::new(leader, 4),
                    payload: EntryPayload::Normal(ConsensusCommand::SetDurabilityPolicy {
                        cluster_id_hash: [1; 32],
                        generation: 1,
                        bundle_quorum_holders: 1,
                        tolerated_failure_domains: 0,
                    }),
                },
            ])
            .await
            .unwrap();
        let mut command = test_command(7);
        command.durability = DurabilityLevel::Local;
        command.durable_holders = vec![holder];
        machine
            .apply([Entry {
                log_id: LogId::new(leader, 5),
                payload: EntryPayload::Normal(ConsensusCommand::Certify(command)),
            }])
            .await
            .unwrap();
        assert!(
            machine
                .store
                .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
                .unwrap()
                .unwrap()
                .local_durability_violations
                .is_empty()
        );

        machine
            .apply([
                Entry {
                    log_id: LogId::new(leader, 6),
                    payload: EntryPayload::Normal(ConsensusCommand::UpgradeDurability {
                        cluster_id_hash: [1; 32],
                        commit_version: CommitVersion(5),
                        bundle_hash: BundleHash([7; 32]),
                        durability: DurabilityLevel::Quorum,
                        durable_holders: vec![holder, survivor],
                    }),
                },
                Entry {
                    log_id: LogId::new(leader, 7),
                    payload: EntryPayload::Membership(Membership::new(
                        vec![BTreeSet::from([2])],
                        BTreeSet::from([1, 2]),
                    )),
                },
                Entry {
                    log_id: LogId::new(leader, 8),
                    payload: EntryPayload::Normal(ConsensusCommand::RemoveNode {
                        cluster_id_hash: [1; 32],
                        node: holder,
                    }),
                },
            ])
            .await
            .unwrap();
        assert!(
            machine
                .store
                .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
                .unwrap()
                .unwrap()
                .local_durability_violations
                .is_empty()
        );

        let mut second = test_command(8);
        second.durability = DurabilityLevel::Local;
        second.durable_holders = vec![survivor];
        machine
            .apply([
                Entry {
                    log_id: LogId::new(leader, 9),
                    payload: EntryPayload::Normal(ConsensusCommand::Certify(second)),
                },
                Entry {
                    log_id: LogId::new(leader, 10),
                    payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                        cluster_id_hash: [1; 32],
                        node: NodeIncarnation {
                            node_id: survivor.node_id,
                            incarnation: 2,
                        },
                        raft_node_id: NodeId(2),
                        failure_domain: "zone-b".into(),
                    }),
                },
            ])
            .await
            .unwrap();
        let state = machine
            .store
            .read_state_value::<MachineState>(KEY_OPENRAFT_STATE)
            .unwrap()
            .unwrap();
        assert_eq!(
            state
                .decisions
                .get(&CommitVersion(5))
                .and_then(Option::as_ref)
                .unwrap()
                .durability,
            DurabilityLevel::Quorum
        );
        let violations = state.local_durability_violations;
        assert_eq!(violations.len(), 1);
        assert_eq!(
            violations.get(&CommitVersion(9)),
            Some(&crate::LocalDurabilityViolation {
                commit_version: CommitVersion(9),
                bundle_hash: BundleHash([8; 32]),
                lost_holder: survivor,
                detected_at_log_index: 10,
            })
        );
    }

    #[test]
    fn persisted_state_rejects_restart_under_another_cluster() {
        let directory = TempDir::new().unwrap();
        let store = RocksRaftStore::open(directory.path(), 7).unwrap();
        stores(store.clone(), [1; 32]).unwrap();

        let error = match stores(store, [2; 32]) {
            Ok(_) => panic!("cross-cluster restart was accepted"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("persisted Raft state belongs to another cluster")
        );
    }

    #[test]
    fn restart_recovery_fault_is_retryable_without_changing_persisted_state() {
        let directory = TempDir::new().unwrap();
        let store = RocksRaftStore::open(directory.path(), 7).unwrap();
        stores(store.clone(), [1; 32]).unwrap();
        let before: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();

        fail_next_restart_recovery();
        let error = match stores(store.clone(), [1; 32]) {
            Ok(_) => panic!("injected restart recovery unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("RestartRecovery"));

        stores(store.clone(), [1; 32]).unwrap();
        let after: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();
        assert_eq!(after.last_applied_log_id, before.last_applied_log_id);
        assert_eq!(after.membership, before.membership);
        assert_eq!(
            after.certification.last_applied(),
            before.certification.last_applied()
        );
        assert_eq!(
            after.control.topology_epoch(),
            before.control.topology_epoch()
        );
    }

    #[test]
    fn log_flushed_completion_follows_durable_success_and_reports_failed_write() {
        let directory = TempDir::new().unwrap();
        let store = RocksRaftStore::open(directory.path(), 7).unwrap();
        let log_store = OpenRaftLogStore {
            store: store.clone(),
        };

        let successful_completion = Arc::new(Mutex::new(None));
        let observed = successful_completion.clone();
        let callback_store = store.clone();
        log_store
            .append_durable_with_completion(&[(0, vec![1, 2, 3])], move |result| {
                assert_eq!(callback_store.get_log(0).unwrap(), Some(vec![1, 2, 3]));
                assert_eq!(callback_store.last_log_index().unwrap(), Some(0));
                *observed.lock().unwrap() = Some(result);
            })
            .unwrap();
        assert!(
            successful_completion
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .is_ok()
        );

        fail_sync_write_at(1);
        let failed_completion = Arc::new(Mutex::new(None));
        let observed = failed_completion.clone();
        let error = log_store
            .append_durable_with_completion(&[(1, vec![4, 5, 6])], move |result| {
                *observed.lock().unwrap() = Some(result);
            })
            .unwrap_err();
        assert!(error.to_string().contains("injected"));
        assert!(failed_completion.lock().unwrap().take().unwrap().is_err());
        assert_eq!(store.get_log(1).unwrap(), None);
        assert_eq!(store.last_log_index().unwrap(), Some(0));
    }

    #[tokio::test]
    async fn failed_state_machine_write_preserves_state_and_last_applied_atomically() {
        let directory = TempDir::new().unwrap();
        let store = RocksRaftStore::open(directory.path(), 0).unwrap();
        let (_, mut machine) = stores(store.clone(), [3; 32]).unwrap();

        fail_sync_write_at(1);
        let owner = NodeIncarnation {
            node_id: NodeId(9),
            incarnation: 2,
        };
        let error = machine
            .apply([Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
                payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                    cluster_id_hash: [3; 32],
                    node: owner,
                    raft_node_id: NodeId(8),
                    failure_domain: "zone-a".into(),
                }),
            }])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("injected"));

        let persisted: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();
        assert_eq!(persisted.last_applied_log_id, None);
        assert_eq!(persisted.control.node_incarnation(owner.node_id), None);
        assert!(persisted.decisions.is_empty());
    }

    #[tokio::test]
    async fn snapshot_from_another_cluster_is_rejected() {
        let source_directory = TempDir::new().unwrap();
        let source_store = RocksRaftStore::open(source_directory.path(), 1).unwrap();
        stores(source_store.clone(), [2; 32]).unwrap();
        let mut builder = OpenRaftSnapshotBuilder {
            store: source_store,
            cluster_id_hash: [2; 32],
        };
        let snapshot = builder.build_snapshot().await.unwrap();

        let target_directory = TempDir::new().unwrap();
        let (_, mut target) = stores(
            RocksRaftStore::open(target_directory.path(), 1).unwrap(),
            [1; 32],
        )
        .unwrap();
        let error = target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("configured cluster"));
    }

    #[tokio::test]
    async fn control_state_survives_snapshot_install_atomically() {
        let source_directory = TempDir::new().unwrap();
        let source_store = RocksRaftStore::open(source_directory.path(), 1).unwrap();
        let (_, mut source) = stores(source_store.clone(), [4; 32]).unwrap();
        let owner = NodeIncarnation {
            node_id: NodeId(8),
            incarnation: 3,
        };
        let commands = [
            ConsensusCommand::InstallNode {
                cluster_id_hash: [4; 32],
                node: owner,
                raft_node_id: NodeId(8),
                failure_domain: "zone-a".into(),
            },
            ConsensusCommand::AssignPartition {
                cluster_id_hash: [4; 32],
                partition_id: 12,
                owner,
                epoch: 7,
            },
            ConsensusCommand::SetDurabilityPolicy {
                cluster_id_hash: [4; 32],
                generation: 5,
                bundle_quorum_holders: 3,
                tolerated_failure_domains: 1,
            },
            ConsensusCommand::AdvanceGcWatermark {
                cluster_id_hash: [4; 32],
                watermark: CommitVersion(4),
            },
        ];
        source
            .apply(
                commands
                    .into_iter()
                    .enumerate()
                    .map(|(offset, command)| Entry {
                        log_id: LogId::new(CommittedLeaderId::new(1, 1), offset as u64 + 1),
                        payload: EntryPayload::Normal(command),
                    }),
            )
            .await
            .unwrap();
        let mut builder = source.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();

        let target_directory = TempDir::new().unwrap();
        let target_store = RocksRaftStore::open(target_directory.path(), 1).unwrap();
        let (_, mut target) = stores(target_store.clone(), [4; 32]).unwrap();
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        let restored: MachineState = target_store
            .read_state_value(KEY_OPENRAFT_STATE)
            .unwrap()
            .unwrap();
        assert_eq!(restored.control.node_incarnation(NodeId(8)), Some(3));
        assert_eq!(restored.control.partition(12).unwrap().epoch, 7);
        assert_eq!(restored.control.durability_policy().generation, 5);
        assert_eq!(restored.control.gc_safety_watermark(), CommitVersion(4));
    }

    #[tokio::test]
    async fn gc_watermark_cannot_jump_beyond_its_consensus_position() {
        let directory = TempDir::new().unwrap();
        let store = RocksRaftStore::open(directory.path(), 0).unwrap();
        let (_, mut machine) = stores(store.clone(), [1; 32]).unwrap();
        let responses = machine
            .apply([Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 7),
                payload: EntryPayload::Normal(ConsensusCommand::AdvanceGcWatermark {
                    cluster_id_hash: [1; 32],
                    watermark: CommitVersion(8),
                }),
            }])
            .await
            .unwrap();

        assert!(matches!(
            responses.as_slice(),
            [RaftApplyResult::Rejected(reason)] if reason.contains("consensus position")
        ));
        let state: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();
        assert_eq!(state.control.gc_safety_watermark(), CommitVersion(0));
    }

    #[tokio::test]
    async fn duplicate_transaction_advances_raft_applied_id_without_republishing_bundle() {
        let directory = TempDir::new().unwrap();
        let store = RocksRaftStore::open(directory.path(), 0).unwrap();
        let (_, mut machine) = stores(store.clone(), [1; 32]).unwrap();
        let command = CertifyTransaction {
            cluster_id_hash: [1; 32],
            transaction_id: TransactionId([1; 16]),
            snapshot_version: CommitVersion(0),
            point_observations: vec![],
            range_observations: vec![],
            predicates: vec![],
            assignment_predicates: vec![],
            written_point_keys: vec![],
            written_points: vec![],
            advanced_range_stamps: vec![],
            bundle_hash: BundleHash([2; 32]),
            bundle_length: 1,
            durability: DurabilityLevel::Local,
            durable_holders: vec![NodeIncarnation {
                node_id: NodeId(1),
                incarnation: 1,
            }],
        };
        machine
            .apply([
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
                    payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                        cluster_id_hash: [1; 32],
                        node: NodeIncarnation {
                            node_id: NodeId(1),
                            incarnation: 1,
                        },
                        raft_node_id: NodeId(1),
                        failure_domain: "zone-a".into(),
                    }),
                },
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 2),
                    payload: EntryPayload::Normal(ConsensusCommand::SetDurabilityPolicy {
                        cluster_id_hash: [1; 32],
                        generation: 1,
                        bundle_quorum_holders: 1,
                        tolerated_failure_domains: 0,
                    }),
                },
            ])
            .await
            .unwrap();
        let log_id_1 = LogId::new(CommittedLeaderId::new(1, 1), 3);
        let log_id_9 = LogId::new(CommittedLeaderId::new(1, 1), 9);
        let first = machine
            .apply([Entry {
                log_id: log_id_1,
                payload: EntryPayload::Normal(ConsensusCommand::Certify(command.clone())),
            }])
            .await
            .unwrap();
        machine
            .apply([Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
                payload: EntryPayload::Normal(ConsensusCommand::RemoveNode {
                    cluster_id_hash: [1; 32],
                    node: NodeIncarnation {
                        node_id: NodeId(1),
                        incarnation: 1,
                    },
                }),
            }])
            .await
            .unwrap();
        let retry = machine
            .apply([Entry {
                log_id: log_id_9,
                payload: EntryPayload::Normal(ConsensusCommand::Certify(command)),
            }])
            .await
            .unwrap();
        assert_eq!(first, retry);
        assert!(matches!(
            retry.as_slice(),
            [RaftApplyResult::Certification(
                CertificationResult::Committed {
                    commit_version: CommitVersion(3),
                    ..
                }
            )]
        ));
        assert_eq!(machine.applied_state().await.unwrap().0, Some(log_id_9));
        let state: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();
        assert!(matches!(
            state.decisions.get(&CommitVersion(3)),
            Some(Some(decision))
                if decision.bundle_hash == BundleHash([2; 32])
                    && decision.bundle_length == 1
        ));
        assert_eq!(state.decisions.get(&CommitVersion(9)), Some(&None));
    }

    #[tokio::test]
    async fn unrelated_partition_topology_change_preserves_assignment_predicate() {
        let directory = TempDir::new().unwrap();
        let store = RocksRaftStore::open(directory.path(), 0).unwrap();
        let (_, mut machine) = stores(store.clone(), [1; 32]).unwrap();
        let owner = NodeIncarnation {
            node_id: NodeId(1),
            incarnation: 1,
        };
        machine
            .apply([
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
                    payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                        cluster_id_hash: [1; 32],
                        node: owner,
                        raft_node_id: NodeId(1),
                        failure_domain: "zone-a".into(),
                    }),
                },
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 2),
                    payload: EntryPayload::Normal(ConsensusCommand::SetDurabilityPolicy {
                        cluster_id_hash: [1; 32],
                        generation: 1,
                        bundle_quorum_holders: 1,
                        tolerated_failure_domains: 0,
                    }),
                },
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 3),
                    payload: EntryPayload::Normal(ConsensusCommand::AssignPartition {
                        cluster_id_hash: [1; 32],
                        partition_id: 7,
                        owner,
                        epoch: 1,
                    }),
                },
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
                    payload: EntryPayload::Normal(ConsensusCommand::AssignPartition {
                        cluster_id_hash: [1; 32],
                        partition_id: 8,
                        owner,
                        epoch: 1,
                    }),
                },
            ])
            .await
            .unwrap();
        let before: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();
        assert_eq!(before.control.topology_epoch(), 4);
        assert_eq!(
            before.control.partition(7),
            Some(&crate::PartitionAssignment { owner, epoch: 1 })
        );

        let result = machine
            .apply([Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 5),
                payload: EntryPayload::Normal(ConsensusCommand::Certify(CertifyTransaction {
                    cluster_id_hash: [1; 32],
                    transaction_id: TransactionId([11; 16]),
                    snapshot_version: CommitVersion(0),
                    point_observations: vec![],
                    range_observations: vec![],
                    predicates: vec![],
                    assignment_predicates: vec![crate::AssignmentPredicate {
                        partition_id: 7,
                        owner,
                        assignment_epoch: 1,
                        topology_epoch: 3,
                    }],
                    written_point_keys: vec![],
                    written_points: vec![],
                    advanced_range_stamps: vec![],
                    bundle_hash: BundleHash([11; 32]),
                    bundle_length: 1,
                    durability: DurabilityLevel::Local,
                    durable_holders: vec![owner],
                })),
            }])
            .await
            .unwrap();

        assert!(matches!(
            result.as_slice(),
            [RaftApplyResult::Certification(
                CertificationResult::Committed {
                    commit_version: CommitVersion(5),
                    ..
                }
            )]
        ));
    }

    #[tokio::test]
    async fn stale_assignment_predicate_is_a_stable_abort_not_a_rejected_raft_entry() {
        let directory = TempDir::new().unwrap();
        let store = RocksRaftStore::open(directory.path(), 0).unwrap();
        let (_, mut machine) = stores(store.clone(), [1; 32]).unwrap();
        let owner = NodeIncarnation {
            node_id: NodeId(1),
            incarnation: 1,
        };
        machine
            .apply([
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
                    payload: EntryPayload::Normal(ConsensusCommand::InstallNode {
                        cluster_id_hash: [1; 32],
                        node: owner,
                        raft_node_id: NodeId(1),
                        failure_domain: "zone-a".into(),
                    }),
                },
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 2),
                    payload: EntryPayload::Normal(ConsensusCommand::SetDurabilityPolicy {
                        cluster_id_hash: [1; 32],
                        generation: 1,
                        bundle_quorum_holders: 1,
                        tolerated_failure_domains: 0,
                    }),
                },
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 3),
                    payload: EntryPayload::Normal(ConsensusCommand::AssignPartition {
                        cluster_id_hash: [1; 32],
                        partition_id: 7,
                        owner,
                        epoch: 1,
                    }),
                },
                Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
                    payload: EntryPayload::Normal(ConsensusCommand::AssignPartition {
                        cluster_id_hash: [1; 32],
                        partition_id: 7,
                        owner,
                        epoch: 2,
                    }),
                },
            ])
            .await
            .unwrap();
        let command = CertifyTransaction {
            cluster_id_hash: [1; 32],
            transaction_id: TransactionId([9; 16]),
            snapshot_version: CommitVersion(0),
            point_observations: vec![],
            range_observations: vec![],
            predicates: vec![],
            assignment_predicates: vec![crate::AssignmentPredicate {
                partition_id: 7,
                owner,
                assignment_epoch: 1,
                topology_epoch: 3,
            }],
            written_point_keys: vec![],
            written_points: vec![],
            advanced_range_stamps: vec![],
            bundle_hash: BundleHash([8; 32]),
            bundle_length: 1,
            durability: DurabilityLevel::Local,
            durable_holders: vec![owner],
        };

        let first = machine
            .apply([Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 5),
                payload: EntryPayload::Normal(ConsensusCommand::Certify(command.clone())),
            }])
            .await
            .unwrap();
        assert!(matches!(
            first.as_slice(),
            [RaftApplyResult::Certification(
                CertificationResult::Aborted {
                    at_version: CommitVersion(5),
                    reason: crate::CertificationAbort::AssignmentConflict {
                        partition_id: 7,
                        expected_epoch: CommitVersion(1),
                        actual_epoch: Some(CommitVersion(2)),
                        ..
                    },
                    ..
                }
            )]
        ));

        let retry = machine
            .apply([Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 6),
                payload: EntryPayload::Normal(ConsensusCommand::Certify(command.clone())),
            }])
            .await
            .unwrap();
        assert_eq!(retry, first);
        let state: MachineState = store.read_state_value(KEY_OPENRAFT_STATE).unwrap().unwrap();
        assert_eq!(state.decisions.get(&CommitVersion(5)), Some(&None));
        assert_eq!(state.decisions.get(&CommitVersion(6)), Some(&None));

        let mut malformed = command;
        malformed.transaction_id = TransactionId([10; 16]);
        malformed.bundle_hash = BundleHash([10; 32]);
        malformed.assignment_predicates[0].topology_epoch = 0;
        let malformed_result = machine
            .apply([Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 7),
                payload: EntryPayload::Normal(ConsensusCommand::Certify(malformed)),
            }])
            .await
            .unwrap();
        assert!(matches!(
            malformed_result.as_slice(),
            [RaftApplyResult::Certification(
                CertificationResult::Aborted {
                    at_version: CommitVersion(7),
                    reason: crate::CertificationAbort::InvalidCommand(reason),
                    ..
                }
            )] if reason.contains("non-zero exact authority")
        ));
    }

    #[tokio::test]
    async fn single_node_runtime_initializes_certifies_and_linearizes() {
        let directory = TempDir::new().unwrap();
        let runtime = OpenRaftConsensus::new(
            NodeId(1),
            RocksRaftStore::open(directory.path(), 0).unwrap(),
            [1; 32],
            "test-cluster",
            Arc::new(NoRemoteFactory),
        )
        .await
        .unwrap();
        runtime
            .initialize(BTreeMap::from([(
                NodeId(1),
                ConsensusNode {
                    address: "in-process".into(),
                },
            )]))
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while runtime.raft.metrics().borrow().current_leader != Some(1) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "single node did not elect itself"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(matches!(
            runtime
                .install_node(
                    [1; 32],
                    NodeIncarnation {
                        node_id: NodeId(1),
                        incarnation: 1,
                    },
                    NodeId(1),
                    "zone-a".into(),
                )
                .await
                .unwrap(),
            ControlApplyResult::NodeInstalled(_)
        ));
        assert!(matches!(
            runtime
                .set_durability_policy([1; 32], 1, 1, 0)
                .await
                .unwrap(),
            ControlApplyResult::DurabilityPolicySet(_)
        ));
        let command = test_command(8);
        let (first, concurrent_retry) = tokio::join!(
            runtime.certify(command.clone()),
            runtime.certify(command.clone())
        );
        let first = first.unwrap();
        assert_eq!(concurrent_retry.unwrap(), first);
        let committed = match first {
            CertificationResult::Committed { commit_version, .. } => commit_version,
            other => panic!("unexpected result: {other:?}"),
        };
        assert_eq!(
            runtime.observed_commit_version(),
            committed,
            "concurrent retry must not allocate another Raft position"
        );

        let mut mismatched = command;
        mismatched.bundle_hash = BundleHash([9; 32]);
        let cursor_before_mismatch = runtime.observed_commit_version();
        assert!(matches!(
            runtime.certify(mismatched).await,
            Err(ConsensusError::Rejected(reason))
                if reason == CertificationError::TransactionIdentityMismatch.to_string()
        ));
        assert_eq!(
            runtime.observed_commit_version(),
            cursor_before_mismatch,
            "bundle identity mismatch must be rejected before Raft"
        );
        assert_eq!(
            runtime.linearized_read_barrier().await.unwrap(),
            runtime.observed_commit_version()
        );
        assert!(runtime.observed_commit_version() >= committed);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn automatic_snapshot_purges_logs_and_restart_keeps_certification_state() {
        let directory = TempDir::new().unwrap();
        let store = RocksRaftStore::open(directory.path(), 0).unwrap();
        let runtime = OpenRaftConsensus::new_with_config(
            NodeId(1),
            store.clone(),
            [1; 32],
            openraft::Config {
                cluster_name: "snapshot-purge-test".into(),
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(4),
                max_in_snapshot_log_to_keep: 1,
                replication_lag_threshold: 20,
                purge_batch_size: 1,
                ..Default::default()
            },
            Arc::new(NoRemoteFactory),
        )
        .await
        .unwrap();
        runtime
            .initialize(BTreeMap::from([(
                NodeId(1),
                ConsensusNode {
                    address: "in-process".into(),
                },
            )]))
            .await
            .unwrap();
        wait_for_single_node_leader(&runtime).await;
        runtime
            .install_node(
                [1; 32],
                NodeIncarnation {
                    node_id: NodeId(1),
                    incarnation: 1,
                },
                NodeId(1),
                "zone-a".into(),
            )
            .await
            .unwrap();
        runtime
            .set_durability_policy([1; 32], 1, 1, 0)
            .await
            .unwrap();

        let mut retained = None;
        for id in 20..36 {
            let result = runtime.certify(test_command(id)).await.unwrap();
            if id == 25 {
                retained = Some((id, result));
            }
        }
        let (retained_id, retained_result) = retained.unwrap();
        let retained_version = match &retained_result {
            CertificationResult::Committed { commit_version, .. } => *commit_version,
            other => panic!("unexpected result: {other:?}"),
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            if store
                .last_purged_index()
                .unwrap()
                .is_some_and(|purged| purged >= retained_version.0)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "OpenRaft did not snapshot and purge the covered log"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            store.get_log(0).unwrap().is_none(),
            "purged entries must be absent from RocksDB"
        );
        runtime.shutdown().await.unwrap();

        let restarted = OpenRaftConsensus::new(
            NodeId(1),
            store,
            [1; 32],
            "snapshot-purge-test",
            Arc::new(NoRemoteFactory),
        )
        .await
        .unwrap();
        wait_for_single_node_leader(&restarted).await;
        let cursor_before_retry = restarted.observed_commit_version();
        assert_eq!(
            restarted.certify(test_command(retained_id)).await.unwrap(),
            retained_result,
            "certification retry state must survive snapshot-backed log purge"
        );
        assert_eq!(
            restarted.observed_commit_version(),
            cursor_before_retry,
            "restart retry must not allocate another Raft position"
        );
        restarted.shutdown().await.unwrap();
    }

    async fn wait_for_single_node_leader(runtime: &OpenRaftConsensus) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while runtime.raft.metrics().borrow().current_leader != Some(1) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "single node did not elect itself"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn test_command(id: u8) -> CertifyTransaction {
        CertifyTransaction {
            cluster_id_hash: [1; 32],
            transaction_id: TransactionId([id; 16]),
            snapshot_version: CommitVersion(0),
            point_observations: vec![],
            range_observations: vec![],
            predicates: vec![],
            assignment_predicates: vec![],
            written_point_keys: vec![],
            written_points: vec![],
            advanced_range_stamps: vec![],
            bundle_hash: BundleHash([id; 32]),
            bundle_length: 1,
            durability: DurabilityLevel::Local,
            durable_holders: vec![NodeIncarnation {
                node_id: NodeId(1),
                incarnation: 1,
            }],
        }
    }
}
