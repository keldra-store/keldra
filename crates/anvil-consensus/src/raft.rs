use std::{
    collections::BTreeMap,
    fmt::Debug,
    io::Cursor,
    ops::RangeBounds,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use openraft::{
    AnyError, BasicNode, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend,
    RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote,
    error::{ClientWriteError, InitializeError, RaftError},
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ApplyError, ApplyResult, Command, CommittedInvocation, ExecutorNomination, StateMachine, codec,
    peer::{PeerNetworkFactory, PeerTransport, UnreachablePeerTransport},
    raft_storage::{DurableSnapshot, DurableStorageError, DurableStore, RaftEntry, StorageConfig},
    types::{MAX_RAFT_NODE_ID, MembershipTransitionKind, NodeState},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DecisionApplyResult {
    Applied(ApplyResult),
    Rejected(ApplyError),
    Noop,
}

openraft::declare_raft_types!(
    pub(crate) DecisionRaftConfig:
        D = Command,
        R = DecisionApplyResult,
        NodeId = u64,
        Node = BasicNode,
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MachineState {
    decisions: StateMachine,
    last_applied_log_id: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    snapshot_generation: u64,
}

/// Exact state-machine layout in version-one enveloped snapshots.
#[derive(Debug, Serialize, Deserialize)]
struct LegacyStateMachinePreClusterControl {
    max_commit_entries: u32,
    max_commit_bytes: u64,
    cluster_id: Option<crate::ClusterId>,
    system_bootstrap: crate::SystemBootstrapState,
    executor: Option<ExecutorNomination>,
    committed_invocations: BTreeMap<u64, CommittedInvocation>,
    committed_invocation_bytes: u64,
    last_commit_cursor: Option<u64>,
    finalized_through: Option<u64>,
}

/// Exact outer layout in version-one enveloped snapshots.
#[derive(Debug, Serialize, Deserialize)]
struct LegacyMachineStatePreClusterControl {
    decisions: LegacyStateMachinePreClusterControl,
    last_applied_log_id: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    snapshot_generation: u64,
}

/// Exact cluster-control layout in unreleased version-two snapshots.
#[derive(Debug, Serialize, Deserialize)]
struct LegacyClusterControlStateV2 {
    nodes: BTreeMap<crate::NodeId, crate::NodeDescriptor>,
    used_node_ids: crate::UsedNodeIds,
    transition: Option<crate::MembershipTransition>,
    jwt_signing_key_fingerprint: Option<crate::JwtSigningKeyFingerprint>,
    erasure_code_profile: Option<crate::ErasureCodeProfile>,
}

/// Exact state-machine layout in unreleased version-two snapshots.
#[derive(Debug, Serialize, Deserialize)]
struct LegacyStateMachineV2 {
    max_commit_entries: u32,
    max_commit_bytes: u64,
    cluster_id: Option<crate::ClusterId>,
    system_bootstrap: crate::SystemBootstrapState,
    cluster_control: LegacyClusterControlStateV2,
    executor: Option<ExecutorNomination>,
    committed_invocations: BTreeMap<u64, CommittedInvocation>,
    committed_invocation_bytes: u64,
    last_commit_cursor: Option<u64>,
    finalized_through: Option<u64>,
}

/// Exact outer layout in unreleased version-two snapshots.
#[derive(Debug, Serialize, Deserialize)]
struct LegacyMachineStateV2 {
    decisions: LegacyStateMachineV2,
    last_applied_log_id: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    snapshot_generation: u64,
}

/// Exact state-machine layout written by Anvil 0.5.0 snapshots.
#[derive(Debug, Serialize, Deserialize)]
struct LegacyStateMachineV050 {
    max_commit_entries: u32,
    max_commit_bytes: u64,
    executor: Option<ExecutorNomination>,
    committed_invocations: BTreeMap<u64, CommittedInvocation>,
    committed_invocation_bytes: u64,
    last_commit_cursor: Option<u64>,
    finalized_through: Option<u64>,
}

/// Exact outer state layout written by Anvil 0.5.0 snapshots.
#[derive(Debug, Serialize, Deserialize)]
struct LegacyMachineStateV050 {
    decisions: LegacyStateMachineV050,
    last_applied_log_id: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    snapshot_generation: u64,
}

impl From<LegacyMachineStateV050> for MachineState {
    fn from(legacy: LegacyMachineStateV050) -> Self {
        let decisions = legacy.decisions;
        Self {
            decisions: StateMachine::from_v050_snapshot(
                decisions.max_commit_entries,
                decisions.max_commit_bytes,
                decisions.executor,
                decisions.committed_invocations,
                decisions.committed_invocation_bytes,
                decisions.last_commit_cursor,
                decisions.finalized_through,
            ),
            last_applied_log_id: legacy.last_applied_log_id,
            membership: legacy.membership,
            snapshot_generation: legacy.snapshot_generation,
        }
    }
}

impl From<LegacyMachineStatePreClusterControl> for MachineState {
    fn from(legacy: LegacyMachineStatePreClusterControl) -> Self {
        let decisions = legacy.decisions;
        Self {
            decisions: StateMachine::from_pre_cluster_control_snapshot(
                decisions.max_commit_entries,
                decisions.max_commit_bytes,
                decisions.cluster_id,
                decisions.system_bootstrap,
                decisions.executor,
                decisions.committed_invocations,
                decisions.committed_invocation_bytes,
                decisions.last_commit_cursor,
                decisions.finalized_through,
            ),
            last_applied_log_id: legacy.last_applied_log_id,
            membership: legacy.membership,
            snapshot_generation: legacy.snapshot_generation,
        }
    }
}

impl From<LegacyMachineStateV2> for MachineState {
    fn from(legacy: LegacyMachineStateV2) -> Self {
        let decisions = legacy.decisions;
        let cluster_control = decisions.cluster_control;
        Self {
            decisions: StateMachine::from_v2_snapshot(
                decisions.max_commit_entries,
                decisions.max_commit_bytes,
                decisions.cluster_id,
                decisions.system_bootstrap,
                crate::ClusterControlState {
                    nodes: cluster_control.nodes,
                    used_node_ids: cluster_control.used_node_ids,
                    transition: cluster_control.transition,
                    jwt_signing_key_fingerprint: cluster_control.jwt_signing_key_fingerprint,
                    erasure_code_profile: cluster_control.erasure_code_profile,
                    active_placement_log_id: None,
                },
                decisions.executor,
                decisions.committed_invocations,
                decisions.committed_invocation_bytes,
                decisions.last_commit_cursor,
                decisions.finalized_through,
            ),
            last_applied_log_id: legacy.last_applied_log_id,
            membership: legacy.membership,
            snapshot_generation: legacy.snapshot_generation,
        }
    }
}

impl MachineState {
    fn new(config: StorageConfig) -> Result<Self, ApplyError> {
        Ok(Self {
            decisions: StateMachine::new(config.max_commit_entries, config.max_commit_bytes)?,
            last_applied_log_id: None,
            membership: StoredMembership::default(),
            snapshot_generation: 0,
        })
    }

    fn validate_config(&self, config: StorageConfig) -> Result<(), DecisionRaftError> {
        if self.decisions.max_commit_entries() != config.max_commit_entries
            || self.decisions.max_commit_bytes() != config.max_commit_bytes
        {
            return Err(DecisionRaftError::Storage(
                "snapshot state-machine configuration does not match this node".into(),
            ));
        }
        Ok(())
    }

    fn validate_cluster_membership(&self) -> Result<(), String> {
        let cluster = self.decisions.cluster_control();
        if cluster.nodes().is_empty() {
            // Released 0.5.0 state has OpenRaft membership but no descriptors.
            if cluster.transition().is_some() {
                return Err("membership transition has no admitted node descriptor".into());
            }
            return Ok(());
        }

        let membership = self.membership.membership();
        for (raft_node_id, raft_node) in membership.nodes() {
            let node_id = crate::NodeId(*raft_node_id);
            let descriptor = cluster.nodes().get(&node_id).ok_or_else(|| {
                format!("Raft member {raft_node_id} has no admitted node descriptor")
            })?;
            if descriptor.peer_address.0 != raft_node.addr {
                return Err(format!(
                    "Raft member {raft_node_id} address does not match its admitted descriptor"
                ));
            }
        }

        for (node_id, descriptor) in cluster.nodes() {
            if descriptor.node_id != *node_id {
                return Err(format!(
                    "admitted node {} is stored under descriptor key {}",
                    descriptor.node_id.0, node_id.0
                ));
            }
            if !cluster.used_node_ids().contains(*node_id) {
                return Err(format!(
                    "admitted node {} is absent from the never-reuse bitmap",
                    node_id.0
                ));
            }
            let is_member = membership.get_node(&node_id.0).is_some();
            let transition = cluster
                .transition()
                .filter(|transition| transition.node_id == *node_id);
            let membership_optional = matches!(
                (
                    transition.map(|transition| transition.kind),
                    descriptor.state
                ),
                (Some(MembershipTransitionKind::Add), NodeState::Joining)
                    | (Some(MembershipTransitionKind::Remove), NodeState::Active)
            );
            if !is_member && !membership_optional {
                return Err(format!(
                    "admitted node {} is not present in OpenRaft membership",
                    node_id.0
                ));
            }
            if descriptor.state == NodeState::Joining
                && !matches!(
                    transition.map(|transition| transition.kind),
                    Some(MembershipTransitionKind::Add)
                )
            {
                return Err(format!(
                    "JOINING node {} is not the current ADD transition",
                    node_id.0
                ));
            }
        }

        if let Some(transition) = cluster.transition()
            && !cluster.nodes().contains_key(&transition.node_id)
        {
            return Err(format!(
                "membership transition references missing node {}",
                transition.node_id.0
            ));
        }

        match cluster.transition() {
            None => validate_fixed_voters(membership, &self.decisions)
                .map_err(|error| error.to_string())?,
            Some(transition) if transition.kind == MembershipTransitionKind::Reweight => {
                validate_fixed_voters(membership, &self.decisions)
                    .map_err(|error| error.to_string())?;
            }
            Some(transition) if transition.kind == MembershipTransitionKind::Add => {
                let descriptor = &cluster.nodes()[&transition.node_id];
                if descriptor.state == NodeState::Joining {
                    validate_fixed_voters(membership, &self.decisions)
                        .map_err(|error| error.to_string())?;
                }
            }
            Some(_) => {}
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DecisionRaftError {
    #[error("Raft node identity must be between 1 and 1023")]
    InvalidNodeId,
    #[error("consensus configuration rejected: {0}")]
    Configuration(String),
    #[error("consensus storage failed: {0}")]
    Storage(String),
    #[error("decision was committed but rejected by the state machine: {0}")]
    Rejected(ApplyError),
    #[error("request must be sent to leader {leader_id:?} at {leader_address:?}")]
    ForwardToLeader {
        leader_id: Option<u64>,
        leader_address: Option<String>,
    },
    #[error("consensus is unavailable: {0}")]
    Unavailable(String),
    #[error("timed out waiting for a consensus leader")]
    LeaderTimeout,
    #[error("timed out waiting for a durable consensus snapshot")]
    SnapshotTimeout,
    #[error("consensus state lock was poisoned")]
    StatePoisoned,
}

/// A domain decision together with its globally ordered Raft recovery cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedDecision {
    pub log_index: u64,
    pub result: ApplyResult,
}

impl From<DurableStorageError> for DecisionRaftError {
    fn from(error: DurableStorageError) -> Self {
        Self::Storage(error.to_string())
    }
}

impl From<codec::CodecError> for DecisionRaftError {
    fn from(error: codec::CodecError) -> Self {
        Self::Storage(error.to_string())
    }
}

#[derive(Clone)]
struct OpenRaftLogStore {
    store: DurableStore,
}

impl RaftLogReader<DecisionRaftConfig> for OpenRaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<RaftEntry>, StorageError<u64>> {
        self.store.scan_logs(range).map_err(read_error)
    }
}

impl RaftLogStorage<DecisionRaftConfig> for OpenRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<DecisionRaftConfig>, StorageError<u64>> {
        let last_purged_log_id = self.store.last_purged_log_id().map_err(read_error)?;
        let last_log_id = self
            .store
            .last_log_id()
            .map_err(read_error)?
            .or(last_purged_log_id);
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
        callback: LogFlushed<DecisionRaftConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = RaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        match self.store.append_logs(&entries) {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                callback.log_io_completed(Err(std::io::Error::other(message.clone())));
                Err(storage_error(ErrorSubject::Logs, ErrorVerb::Write, message))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.store.truncate_logs(log_id.index).map_err(write_error)
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.store.purge_logs(log_id).map_err(write_error)
    }
}

#[derive(Clone)]
struct OpenRaftSnapshotBuilder {
    store: DurableStore,
    machine: Arc<Mutex<MachineState>>,
}

impl RaftSnapshotBuilder<DecisionRaftConfig> for OpenRaftSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<DecisionRaftConfig>, StorageError<u64>> {
        let mut current = self.machine.lock().map_err(|_| {
            storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                "state lock poisoned",
            )
        })?;
        let mut snapshot_state = current.clone();
        snapshot_state.snapshot_generation = snapshot_state.snapshot_generation.saturating_add(1);
        snapshot_state
            .validate_cluster_membership()
            .map_err(|error| storage_error(ErrorSubject::StateMachine, ErrorVerb::Read, error))?;
        let data =
            codec::encode_record_at_version(&snapshot_state, codec::SNAPSHOT_RECORD_FORMAT_V3)
                .map_err(|error| {
                    storage_error(ErrorSubject::StateMachine, ErrorVerb::Read, error)
                })?;
        let meta = SnapshotMeta {
            last_log_id: snapshot_state.last_applied_log_id,
            last_membership: snapshot_state.membership.clone(),
            snapshot_id: format!(
                "{}-{}",
                snapshot_state
                    .last_applied_log_id
                    .map_or(0, |log_id| log_id.index),
                snapshot_state.snapshot_generation
            ),
        };
        self.store
            .save_snapshot(
                &DurableSnapshot {
                    meta: meta.clone(),
                    data: data.clone(),
                },
                false,
            )
            .map_err(write_error)?;
        current.snapshot_generation = snapshot_state.snapshot_generation;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[derive(Clone)]
struct OpenRaftStateMachine {
    store: DurableStore,
    machine: Arc<Mutex<MachineState>>,
}

impl RaftStateMachine<DecisionRaftConfig> for OpenRaftStateMachine {
    type SnapshotBuilder = OpenRaftSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let state = self.machine.lock().map_err(|_| {
            storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Read,
                "state lock poisoned",
            )
        })?;
        Ok((state.last_applied_log_id, state.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<DecisionApplyResult>, StorageError<u64>>
    where
        I: IntoIterator<Item = RaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let mut current = self.machine.lock().map_err(|_| {
            storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                "state lock poisoned",
            )
        })?;
        let mut next = current.clone();
        let mut responses = Vec::with_capacity(entries.len());
        for entry in &entries {
            responses.push(apply_entry(&mut next, entry).map_err(|error| {
                storage_error(ErrorSubject::StateMachine, ErrorVerb::Write, error)
            })?);
        }
        self.store.append_applied(&entries).map_err(write_error)?;
        *current = next;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        OpenRaftSnapshotBuilder {
            store: self.store.clone(),
            machine: self.machine.clone(),
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
        let data = snapshot.into_inner();
        let state = decode_machine_snapshot(&data).map_err(|error| {
            storage_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                error,
            )
        })?;
        if state.last_applied_log_id != meta.last_log_id
            || state.membership != meta.last_membership
            || state.decisions.max_commit_entries() != self.store.config().max_commit_entries
            || state.decisions.max_commit_bytes() != self.store.config().max_commit_bytes
        {
            return Err(storage_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                "snapshot body does not match metadata or fixed cluster configuration",
            ));
        }
        state.validate_cluster_membership().map_err(|error| {
            storage_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                error,
            )
        })?;

        let mut current = self.machine.lock().map_err(|_| {
            storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                "state lock poisoned",
            )
        })?;
        self.store
            .save_snapshot(
                &DurableSnapshot {
                    meta: meta.clone(),
                    data,
                },
                true,
            )
            .map_err(write_error)?;
        *current = state;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<DecisionRaftConfig>>, StorageError<u64>> {
        self.store
            .load_snapshot()
            .map_err(read_error)
            .map(|snapshot| {
                snapshot.map(|snapshot| Snapshot {
                    meta: snapshot.meta,
                    snapshot: Box::new(Cursor::new(snapshot.data)),
                })
            })
    }
}

/// Running compact decision consensus service.
#[derive(Clone)]
pub struct DecisionRaft {
    pub(crate) node_id: u64,
    pub(crate) raft: openraft::Raft<DecisionRaftConfig>,
    store: DurableStore,
    machine: Arc<Mutex<MachineState>>,
}

impl DecisionRaft {
    /// Open a Raft node with no reachable peers.
    ///
    /// This remains the small convenience used by the one-node 0.5.0 server
    /// and focused storage tests. Multi-node callers use
    /// [`Self::open_with_transport`].
    pub async fn open(
        path: impl AsRef<Path>,
        node_id: u64,
        max_commit_entries: u32,
        max_commit_bytes: u64,
    ) -> Result<Self, DecisionRaftError> {
        Self::open_with_transport(
            path,
            node_id,
            max_commit_entries,
            max_commit_bytes,
            Arc::new(UnreachablePeerTransport),
        )
        .await
    }

    /// Open a Raft node whose OpenRaft network is backed by the supplied
    /// transport. Opening never initializes membership.
    pub async fn open_with_transport(
        path: impl AsRef<Path>,
        node_id: u64,
        max_commit_entries: u32,
        max_commit_bytes: u64,
        transport: Arc<dyn PeerTransport>,
    ) -> Result<Self, DecisionRaftError> {
        if !(1..=MAX_RAFT_NODE_ID).contains(&node_id) {
            return Err(DecisionRaftError::InvalidNodeId);
        }
        StateMachine::new(max_commit_entries, max_commit_bytes)
            .map_err(DecisionRaftError::Rejected)?;
        let storage_config = StorageConfig {
            max_commit_entries,
            max_commit_bytes,
        };
        let store = DurableStore::open(path, storage_config, node_id)?;
        let machine = Arc::new(Mutex::new(load_machine(&store)?));
        let config = openraft::Config {
            cluster_name: "anvil-decisions".into(),
            max_payload_entries: 64,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(256),
            max_in_snapshot_log_to_keep: 64,
            ..Default::default()
        }
        .validate()
        .map_err(|error| DecisionRaftError::Configuration(error.to_string()))?;
        let raft = openraft::Raft::new(
            node_id,
            Arc::new(config),
            PeerNetworkFactory { transport },
            OpenRaftLogStore {
                store: store.clone(),
            },
            OpenRaftStateMachine {
                store: store.clone(),
                machine: machine.clone(),
            },
        )
        .await
        .map_err(|error| DecisionRaftError::Storage(error.to_string()))?;
        Ok(Self {
            node_id,
            raft,
            store,
            machine,
        })
    }

    pub async fn is_initialized(&self) -> Result<bool, DecisionRaftError> {
        self.raft
            .is_initialized()
            .await
            .map_err(|error| DecisionRaftError::Unavailable(error.to_string()))
    }

    pub fn current_leader(&self) -> Option<u64> {
        self.raft.metrics().borrow().current_leader
    }

    /// Initialize a pristine single-node cluster, or leave an already initialized
    /// cluster untouched. This is safe to call on every process start.
    pub async fn ensure_one_node(&self) -> Result<(), DecisionRaftError> {
        if self
            .raft
            .is_initialized()
            .await
            .map_err(|error| DecisionRaftError::Unavailable(error.to_string()))?
        {
            return Ok(());
        }

        let members = BTreeMap::from([(
            self.node_id,
            BasicNode::new(format!("anvil-local://{}", self.node_id)),
        )]);
        match self.raft.initialize(members).await {
            Ok(()) | Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            Err(error) => Err(DecisionRaftError::Unavailable(error.to_string())),
        }
    }

    pub async fn submit(&self, command: Command) -> Result<CommittedDecision, DecisionRaftError> {
        let response = self
            .raft
            .client_write(command)
            .await
            .map_err(map_client_write_error)?;
        match response.data {
            DecisionApplyResult::Applied(result) => Ok(CommittedDecision {
                log_index: response.log_id.index,
                result,
            }),
            DecisionApplyResult::Rejected(error) => Err(DecisionRaftError::Rejected(error)),
            DecisionApplyResult::Noop => Err(DecisionRaftError::Unavailable(
                "normal command produced an empty Raft response".into(),
            )),
        }
    }

    pub fn state(&self) -> Result<StateMachine, DecisionRaftError> {
        self.machine
            .lock()
            .map(|state| state.decisions.clone())
            .map_err(|_| DecisionRaftError::StatePoisoned)
    }

    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<u64, DecisionRaftError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(leader) = self.raft.metrics().borrow().current_leader {
                return Ok(leader);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(DecisionRaftError::LeaderTimeout);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Trigger and wait for a snapshot that is durably present in RocksDB.
    pub async fn snapshot(&self, timeout: Duration) -> Result<(), DecisionRaftError> {
        let target = self
            .machine
            .lock()
            .map_err(|_| DecisionRaftError::StatePoisoned)?
            .last_applied_log_id;
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|error| DecisionRaftError::Unavailable(error.to_string()))?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.store.load_snapshot()?.is_some_and(|snapshot| {
                snapshot.meta.last_log_id.map(|id| id.index) >= target.map(|id| id.index)
            }) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(DecisionRaftError::SnapshotTimeout);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn shutdown(&self) -> Result<(), DecisionRaftError> {
        self.raft
            .shutdown()
            .await
            .map_err(|error| DecisionRaftError::Unavailable(error.to_string()))
    }
}

fn load_machine(store: &DurableStore) -> Result<MachineState, DecisionRaftError> {
    let mut state = match store.load_snapshot()? {
        Some(snapshot) => {
            let state = decode_machine_snapshot(&snapshot.data)?;
            if state.last_applied_log_id != snapshot.meta.last_log_id
                || state.membership != snapshot.meta.last_membership
            {
                return Err(DecisionRaftError::Storage(
                    "durable snapshot body does not match metadata".into(),
                ));
            }
            state.validate_config(store.config())?;
            state
        }
        None => MachineState::new(store.config()).map_err(DecisionRaftError::Rejected)?,
    };

    for entry in store.scan_applied()? {
        if state
            .last_applied_log_id
            .is_some_and(|last| entry.log_id.index <= last.index)
        {
            continue;
        }
        apply_entry(&mut state, &entry)
            .map_err(|error| DecisionRaftError::Storage(error.to_string()))?;
    }
    state
        .validate_cluster_membership()
        .map_err(DecisionRaftError::Storage)?;
    Ok(state)
}

/// Decode a self-identifying current snapshot or the released raw 0.5.0 body.
/// A body beginning with the record magic is always treated as an envelope, so
/// malformed or unknown current records cannot silently fall back to legacy.
fn decode_machine_snapshot(data: &[u8]) -> Result<MachineState, codec::CodecError> {
    let (version, payload) = codec::record_version_and_payload(data)?;
    match version {
        None => codec::decode::<LegacyMachineStateV050>(payload).map(Into::into),
        Some(1) => codec::decode::<LegacyMachineStatePreClusterControl>(payload).map(Into::into),
        Some(codec::SNAPSHOT_RECORD_FORMAT_V2) => {
            codec::decode::<LegacyMachineStateV2>(payload).map(Into::into)
        }
        Some(codec::SNAPSHOT_RECORD_FORMAT_V3) => codec::decode(payload),
        Some(version) => Err(codec::CodecError::UnsupportedRecordVersion(version)),
    }
}

fn apply_entry(
    state: &mut MachineState,
    entry: &RaftEntry,
) -> Result<DecisionApplyResult, &'static str> {
    let expected = state
        .last_applied_log_id
        .map_or(0, |log_id| log_id.index.saturating_add(1));
    if entry.log_id.index != expected {
        return Err("applied-state journal is not consecutive");
    }

    let response = match &entry.payload {
        EntryPayload::Blank => DecisionApplyResult::Noop,
        EntryPayload::Membership(membership) => {
            state.membership = StoredMembership::new(Some(entry.log_id), membership.clone());
            DecisionApplyResult::Noop
        }
        EntryPayload::Normal(command) => {
            match validate_membership_command(&state.membership, &state.decisions, command)
                .and_then(|()| state.decisions.apply(entry.log_id.index, command))
            {
                Ok(result) => DecisionApplyResult::Applied(result),
                Err(error) => DecisionApplyResult::Rejected(error),
            }
        }
    };
    state.last_applied_log_id = Some(entry.log_id);
    Ok(response)
}

fn validate_membership_command(
    membership: &StoredMembership<u64, BasicNode>,
    decisions: &StateMachine,
    command: &Command,
) -> Result<(), ApplyError> {
    if let Command::BeginAddNode { descriptor, .. } = command {
        for (raft_node_id, raft_node) in membership.membership().nodes() {
            let node_id = crate::NodeId(*raft_node_id);
            if node_id == descriptor.node_id {
                if raft_node.addr != descriptor.peer_address.0 {
                    return Err(ApplyError::RaftMemberAddressMismatch { node_id });
                }
            } else if !decisions.cluster_control().nodes().contains_key(&node_id) {
                return Err(ApplyError::RaftMemberDescriptorMissing { node_id });
            }
        }
    }
    let executor = match command {
        Command::NominateExecutor { executor } => Some(*executor),
        Command::CommitBatch(batch) => Some(batch.executor),
        Command::FinalizedThrough { executor, .. } => Some(*executor),
        Command::InitializeCluster { .. } => None,
        Command::CompleteSystemBootstrap { executor, .. } => Some(*executor),
        Command::BeginAddNode { .. }
        | Command::BeginRemoveNode { .. }
        | Command::BeginReweightNode { .. }
        | Command::CompleteMembershipTransition { .. }
        | Command::StagePeerSpkiOverlap { .. }
        | Command::PromotePeerSpkiOverlap { .. }
        | Command::ClearPeerSpkiOverlap { .. }
        | Command::BindJwtSigningKeyFingerprint { .. }
        | Command::BindErasureCodeProfile { .. } => None,
    };
    if let Some(executor) = executor
        && membership.membership().get_node(&executor.0).is_none()
    {
        return Err(ApplyError::ExecutorNotCurrentMember { executor });
    }
    if let Some(executor) = executor
        && !decisions.cluster_control().nodes().is_empty()
        && decisions
            .cluster_control()
            .nodes()
            .get(&executor)
            .is_none_or(|descriptor| descriptor.state != NodeState::Active)
    {
        return Err(ApplyError::ExecutorNotActiveMember { executor });
    }
    if let Command::CompleteMembershipTransition {
        started_log_index, ..
    } = command
    {
        let transition = decisions
            .cluster_control()
            .transition()
            .ok_or(ApplyError::NoMembershipTransition)?;
        if transition.started_log_index != *started_log_index {
            return Err(ApplyError::MembershipTransitionFenceMismatch {
                expected: transition.started_log_index,
                requested: *started_log_index,
            });
        }
        let raft_membership = membership.membership();
        match transition.kind {
            MembershipTransitionKind::Add => {
                if raft_membership.get_node(&transition.node_id.0).is_none() {
                    return Err(ApplyError::TransitionNodeNotCurrentMember {
                        node_id: transition.node_id,
                    });
                }
                let descriptor = decisions
                    .cluster_control()
                    .nodes()
                    .get(&transition.node_id)
                    .ok_or(ApplyError::MembershipTransitionStateMismatch)?;
                if descriptor.state == NodeState::Active {
                    validate_fixed_voters(raft_membership, decisions)?;
                }
            }
            MembershipTransitionKind::Reweight => {
                if raft_membership.get_node(&transition.node_id.0).is_none() {
                    return Err(ApplyError::TransitionNodeNotCurrentMember {
                        node_id: transition.node_id,
                    });
                }
                validate_fixed_voters(raft_membership, decisions)?;
            }
            MembershipTransitionKind::Remove => {
                if raft_membership.get_node(&transition.node_id.0).is_some() {
                    return Err(ApplyError::RemovingNodeIsStillMember {
                        node_id: transition.node_id,
                    });
                }
                validate_voters(
                    raft_membership,
                    decisions,
                    decisions
                        .cluster_control()
                        .active_node_count()
                        .saturating_sub(1)
                        .min(crate::FIXED_VOTER_TARGET),
                )?;
            }
        }
    }
    Ok(())
}

fn validate_fixed_voters(
    membership: &openraft::Membership<u64, BasicNode>,
    decisions: &StateMachine,
) -> Result<(), ApplyError> {
    validate_voters(
        membership,
        decisions,
        decisions.cluster_control().voter_target(),
    )
}

fn validate_voters(
    membership: &openraft::Membership<u64, BasicNode>,
    decisions: &StateMachine,
    expected: usize,
) -> Result<(), ApplyError> {
    let voters = membership
        .voter_ids()
        .collect::<std::collections::BTreeSet<_>>();
    if voters.len() != expected {
        return Err(ApplyError::VoterTargetMismatch {
            expected: expected as u16,
            actual: voters.len() as u16,
        });
    }
    for voter in voters {
        let node_id = crate::NodeId(voter);
        if decisions
            .cluster_control()
            .nodes()
            .get(&node_id)
            .is_none_or(|descriptor| descriptor.state != NodeState::Active)
        {
            return Err(ApplyError::VoterNotActive { node_id });
        }
    }
    Ok(())
}

pub(crate) fn map_client_write_error(
    error: RaftError<u64, ClientWriteError<u64, BasicNode>>,
) -> DecisionRaftError {
    if let Some(forward) = error.forward_to_leader::<BasicNode>() {
        DecisionRaftError::ForwardToLeader {
            leader_id: forward.leader_id,
            leader_address: forward.leader_node.as_ref().map(|node| node.addr.clone()),
        }
    } else {
        DecisionRaftError::Unavailable(error.to_string())
    }
}

fn storage_error(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    error: impl ToString,
) -> StorageError<u64> {
    StorageIOError::new(subject, verb, AnyError::error(error)).into()
}

fn read_error(error: DurableStorageError) -> StorageError<u64> {
    storage_error(ErrorSubject::Store, ErrorVerb::Read, error)
}

fn write_error(error: DurableStorageError) -> StorageError<u64> {
    storage_error(ErrorSubject::Store, ErrorVerb::Write, error)
}

#[cfg(test)]
mod snapshot_compatibility_tests {
    use std::collections::BTreeSet;

    use openraft::Membership;

    use super::*;

    #[test]
    fn snapshot_wire_migrates_raw_v050_and_enveloped_pre_cluster_control_state() {
        let config = StorageConfig {
            max_commit_entries: 4,
            max_commit_bytes: 64 * 1024,
        };
        let empty_invocations = BTreeMap::new();
        let legacy = LegacyMachineStateV050 {
            decisions: LegacyStateMachineV050 {
                max_commit_entries: config.max_commit_entries,
                max_commit_bytes: config.max_commit_bytes,
                executor: None,
                committed_invocation_bytes: codec::encoded_len(&empty_invocations).unwrap(),
                committed_invocations: empty_invocations,
                last_commit_cursor: None,
                finalized_through: None,
            },
            last_applied_log_id: None,
            membership: StoredMembership::default(),
            snapshot_generation: 7,
        };

        let legacy_raw = codec::encode(&legacy).unwrap();
        assert_eq!(codec::record_payload(&legacy_raw).unwrap(), legacy_raw);
        let migrated = decode_machine_snapshot(&legacy_raw).unwrap();
        assert_eq!(migrated.snapshot_generation, 7);
        assert_eq!(migrated.decisions.cluster_id(), None);
        assert_eq!(
            migrated.decisions.system_bootstrap(),
            crate::SystemBootstrapState::Missing
        );
        assert_eq!(
            migrated
                .decisions
                .cluster_control()
                .active_placement_log_id(),
            None
        );

        let pre_cluster_control = LegacyMachineStatePreClusterControl {
            decisions: LegacyStateMachinePreClusterControl {
                max_commit_entries: config.max_commit_entries,
                max_commit_bytes: config.max_commit_bytes,
                cluster_id: Some(crate::ClusterId([9; 16])),
                system_bootstrap: crate::SystemBootstrapState::Missing,
                executor: None,
                committed_invocations: BTreeMap::new(),
                committed_invocation_bytes: codec::encoded_len(
                    &BTreeMap::<u64, CommittedInvocation>::new(),
                )
                .unwrap(),
                last_commit_cursor: None,
                finalized_through: None,
            },
            last_applied_log_id: None,
            membership: StoredMembership::default(),
            snapshot_generation: 8,
        };
        let pre_cluster_control_record = codec::encode_record(&pre_cluster_control).unwrap();
        let migrated = decode_machine_snapshot(&pre_cluster_control_record).unwrap();
        assert_eq!(migrated.snapshot_generation, 8);
        assert_eq!(
            migrated.decisions.cluster_id(),
            Some(crate::ClusterId([9; 16]))
        );
        assert!(migrated.decisions.cluster_control().nodes().is_empty());
        assert_eq!(
            migrated
                .decisions
                .cluster_control()
                .active_placement_log_id(),
            None
        );

        let v2_last_applied = LogId::new(openraft::CommittedLeaderId::new(3, 1), 73);
        let v2 = LegacyMachineStateV2 {
            decisions: LegacyStateMachineV2 {
                max_commit_entries: config.max_commit_entries,
                max_commit_bytes: config.max_commit_bytes,
                cluster_id: Some(crate::ClusterId([7; 16])),
                system_bootstrap: crate::SystemBootstrapState::Missing,
                cluster_control: LegacyClusterControlStateV2 {
                    nodes: BTreeMap::new(),
                    used_node_ids: crate::UsedNodeIds::default(),
                    transition: None,
                    jwt_signing_key_fingerprint: None,
                    erasure_code_profile: None,
                },
                executor: None,
                committed_invocations: BTreeMap::new(),
                committed_invocation_bytes: codec::encoded_len(
                    &BTreeMap::<u64, CommittedInvocation>::new(),
                )
                .unwrap(),
                last_commit_cursor: None,
                finalized_through: None,
            },
            last_applied_log_id: Some(v2_last_applied),
            membership: StoredMembership::default(),
            snapshot_generation: 9,
        };
        let v2_record =
            codec::encode_record_at_version(&v2, codec::SNAPSHOT_RECORD_FORMAT_V2).unwrap();
        let migrated = decode_machine_snapshot(&v2_record).unwrap();
        assert_eq!(migrated.last_applied_log_id, Some(v2_last_applied));
        assert_eq!(
            migrated
                .decisions
                .cluster_control()
                .active_placement_log_id(),
            None,
            "v2 migration must not invent placement lineage from last_applied"
        );

        let mut current = MachineState::new(config).unwrap();
        current.decisions.cluster_control.active_placement_log_id = Some(41);
        let current_record =
            codec::encode_record_at_version(&current, codec::SNAPSHOT_RECORD_FORMAT_V3).unwrap();
        assert_eq!(decode_machine_snapshot(&current_record).unwrap(), current);

        let unsupported =
            codec::encode_record_at_version(&current, codec::SNAPSHOT_RECORD_FORMAT_V3 + 1)
                .unwrap();
        assert_eq!(
            decode_machine_snapshot(&unsupported),
            Err(codec::CodecError::UnsupportedRecordVersion(
                codec::SNAPSHOT_RECORD_FORMAT_V3 + 1
            ))
        );

        let mut malformed_current = current_record;
        malformed_current.pop();
        assert!(matches!(
            decode_machine_snapshot(&malformed_current),
            Err(codec::CodecError::InvalidRecord(_))
        ));
    }

    #[test]
    fn snapshot_state_cross_validates_descriptors_against_openraft_membership() {
        let config = StorageConfig {
            max_commit_entries: 4,
            max_commit_bytes: 64 * 1024,
        };
        let mut state = MachineState::new(config).unwrap();
        state
            .decisions
            .apply(
                0,
                &Command::InitializeCluster {
                    cluster_id: crate::ClusterId([1; 16]),
                },
            )
            .unwrap();
        state
            .decisions
            .apply(
                1,
                &Command::BeginAddNode {
                    format_version: crate::CLUSTER_CONTROL_COMMAND_VERSION,
                    descriptor: crate::NodeDescriptor {
                        node_id: crate::NodeId(1),
                        peer_address: crate::PeerAddress("memory://1".into()),
                        storage_weight_millionths: 1_000_000,
                        state: NodeState::Joining,
                        current_peer_spki_sha256: crate::PeerSpkiSha256([1; 32]),
                        overlap_peer_spki_sha256: None,
                        join_capability_hash: Some(crate::JoinCapabilityHash([2; 32])),
                        supported_protocol: crate::CapabilityRange { min: 1, max: 1 },
                        supported_storage_format: crate::CapabilityRange { min: 1, max: 1 },
                    },
                },
            )
            .unwrap();
        for log_index in [2, 3] {
            state
                .decisions
                .apply(
                    log_index,
                    &Command::CompleteMembershipTransition {
                        format_version: crate::CLUSTER_CONTROL_COMMAND_VERSION,
                        started_log_index: 1,
                    },
                )
                .unwrap();
        }
        state.membership = StoredMembership::new(
            None,
            Membership::new(
                vec![BTreeSet::from([1])],
                BTreeMap::from([(1, BasicNode::new("memory://1"))]),
            ),
        );
        assert_eq!(state.validate_cluster_membership(), Ok(()));

        state.membership = StoredMembership::new(
            None,
            Membership::new(
                vec![BTreeSet::from([1])],
                BTreeMap::from([(1, BasicNode::new("memory://wrong"))]),
            ),
        );
        assert!(
            state
                .validate_cluster_membership()
                .unwrap_err()
                .contains("address does not match")
        );

        state.membership = StoredMembership::default();
        assert!(
            state
                .validate_cluster_membership()
                .unwrap_err()
                .contains("not present in OpenRaft membership")
        );
    }
}
