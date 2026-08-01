use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    io::Cursor,
    ops::RangeBounds,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use openraft::{
    AnyError, BasicNode, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend,
    RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote,
    error::{
        ClientWriteError, InitializeError, InstallSnapshotError, NetworkError, RPCError, RaftError,
        RemoteError, Unreachable,
    },
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::{LogFlushed, RaftLogStorage, RaftStateMachine},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{
    ApplyError, ApplyResult, Command, StateMachine, codec,
    raft_storage::{DurableSnapshot, DurableStorageError, DurableStore, RaftEntry, StorageConfig},
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
}

/// Address attached to an OpenRaft membership node.
///
/// Interpretation and connection management belong to the injected transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerNode {
    pub address: String,
}

impl PeerNode {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerRpcKind {
    AppendEntries,
    Vote,
    InstallSnapshot,
}

/// Opaque OpenRaft protocol envelope carried by the application's transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRpc {
    pub schema_version: u16,
    pub kind: PeerRpcKind,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PeerTransportError {
    #[error("peer is unreachable: {0}")]
    Unreachable(String),
    #[error("peer protocol failed: {0}")]
    Protocol(String),
}

/// Injectable transport for opaque peer protocol messages.
#[async_trait]
pub trait PeerTransport: Send + Sync + 'static {
    async fn send(
        &self,
        target: u64,
        node: &PeerNode,
        rpc: PeerRpc,
    ) -> Result<Vec<u8>, PeerTransportError>;
}

/// Useful for a single-node cluster, where no peer call should occur.
#[derive(Debug, Default)]
pub struct NoPeerTransport;

#[async_trait]
impl PeerTransport for NoPeerTransport {
    async fn send(
        &self,
        target: u64,
        _node: &PeerNode,
        _rpc: PeerRpc,
    ) -> Result<Vec<u8>, PeerTransportError> {
        Err(PeerTransportError::Unreachable(format!(
            "single-node transport has no peer {target}"
        )))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PeerRpcError {
    #[error("unsupported peer RPC schema {0}")]
    UnsupportedSchema(u16),
    #[error("peer RPC payload exceeds the compact consensus limit")]
    PayloadTooLarge,
    #[error("peer RPC codec error: {0}")]
    Codec(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DecisionRaftError {
    #[error("Raft node identity must be non-zero")]
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
struct NetworkFactoryAdapter {
    transport: Arc<dyn PeerTransport>,
}

struct NetworkAdapter {
    transport: Arc<dyn PeerTransport>,
    target: u64,
    node: BasicNode,
}

impl RaftNetworkFactory<DecisionRaftConfig> for NetworkFactoryAdapter {
    type Network = NetworkAdapter;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        NetworkAdapter {
            transport: self.transport.clone(),
            target,
            node: node.clone(),
        }
    }
}

impl NetworkAdapter {
    async fn call<Req, Resp, AppError>(
        &mut self,
        kind: PeerRpcKind,
        request: &Req,
    ) -> Result<Resp, RPCError<u64, BasicNode, RaftError<u64, AppError>>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        AppError: std::error::Error + Serialize + DeserializeOwned,
    {
        let payload =
            codec::encode(request).map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        let response = self
            .transport
            .send(
                self.target,
                &PeerNode::new(self.node.addr.clone()),
                PeerRpc {
                    schema_version: 1,
                    kind,
                    payload,
                },
            )
            .await
            .map_err(|error| match error {
                PeerTransportError::Unreachable(_) => {
                    RPCError::Unreachable(Unreachable::new(&error))
                }
                PeerTransportError::Protocol(_) => RPCError::Network(NetworkError::new(&error)),
            })?;
        let remote: Result<Resp, RaftError<u64, AppError>> = codec::decode(&response)
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

impl RaftNetwork<DecisionRaftConfig> for NetworkAdapter {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<DecisionRaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.call(PeerRpcKind::AppendEntries, &rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<DecisionRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        self.call(PeerRpcKind::InstallSnapshot, &rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.call(PeerRpcKind::Vote, &rpc).await
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
        let data = codec::encode(&snapshot_state)
            .map_err(|error| storage_error(ErrorSubject::StateMachine, ErrorVerb::Read, error))?;
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
        let state: MachineState = codec::decode(&data).map_err(|error| {
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
    node_id: u64,
    raft: openraft::Raft<DecisionRaftConfig>,
    store: DurableStore,
    machine: Arc<Mutex<MachineState>>,
}

impl DecisionRaft {
    pub async fn open(
        path: impl AsRef<Path>,
        node_id: u64,
        max_commit_entries: u32,
        max_commit_bytes: u64,
        transport: Arc<dyn PeerTransport>,
    ) -> Result<Self, DecisionRaftError> {
        if node_id == 0 {
            return Err(DecisionRaftError::InvalidNodeId);
        }
        StateMachine::new(max_commit_entries, max_commit_bytes)
            .map_err(DecisionRaftError::Rejected)?;
        let storage_config = StorageConfig {
            max_commit_entries,
            max_commit_bytes,
        };
        let store = DurableStore::open(path, storage_config)?;
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
            NetworkFactoryAdapter { transport },
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

    pub async fn bootstrap_one_node(&self, node: PeerNode) -> Result<(), DecisionRaftError> {
        self.ensure_one_node(node).await
    }

    /// Initialize a pristine single-node cluster, or leave an already initialized
    /// cluster untouched. This is safe to call on every process start.
    pub async fn ensure_one_node(&self, node: PeerNode) -> Result<(), DecisionRaftError> {
        if self
            .raft
            .is_initialized()
            .await
            .map_err(|error| DecisionRaftError::Unavailable(error.to_string()))?
        {
            return Ok(());
        }

        let members = BTreeMap::from([(self.node_id, BasicNode::new(node.address))]);
        match self.raft.initialize(members).await {
            Ok(()) | Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            Err(error) => Err(DecisionRaftError::Unavailable(error.to_string())),
        }
    }

    pub async fn initialize(
        &self,
        members: BTreeMap<u64, PeerNode>,
    ) -> Result<(), DecisionRaftError> {
        let members = members
            .into_iter()
            .map(|(node_id, node)| (node_id, BasicNode::new(node.address)))
            .collect::<BTreeMap<_, _>>();
        self.raft
            .initialize(members)
            .await
            .map_err(|error| DecisionRaftError::Unavailable(error.to_string()))
    }

    pub async fn add_learner(
        &self,
        node_id: u64,
        node: PeerNode,
        blocking: bool,
    ) -> Result<(), DecisionRaftError> {
        self.raft
            .add_learner(node_id, BasicNode::new(node.address), blocking)
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
    }

    pub async fn change_membership(
        &self,
        voters: BTreeSet<u64>,
        retain_removed_as_learners: bool,
    ) -> Result<(), DecisionRaftError> {
        self.raft
            .change_membership(voters, retain_removed_as_learners)
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
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

    pub async fn handle_peer_rpc(&self, rpc: PeerRpc) -> Result<Vec<u8>, PeerRpcError> {
        if rpc.schema_version != 1 {
            return Err(PeerRpcError::UnsupportedSchema(rpc.schema_version));
        }
        if rpc.payload.len() > codec::MAX_ENCODED_BYTES {
            return Err(PeerRpcError::PayloadTooLarge);
        }
        match rpc.kind {
            PeerRpcKind::AppendEntries => {
                let request: AppendEntriesRequest<DecisionRaftConfig> = decode_peer(&rpc.payload)?;
                encode_peer(&self.raft.append_entries(request).await)
            }
            PeerRpcKind::Vote => {
                let request: VoteRequest<u64> = decode_peer(&rpc.payload)?;
                encode_peer(&self.raft.vote(request).await)
            }
            PeerRpcKind::InstallSnapshot => {
                let request: InstallSnapshotRequest<DecisionRaftConfig> =
                    decode_peer(&rpc.payload)?;
                encode_peer(&self.raft.install_snapshot(request).await)
            }
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
            let state: MachineState = codec::decode(&snapshot.data)?;
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
    Ok(state)
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
            match validate_membership_command(&state.membership, command)
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
    command: &Command,
) -> Result<(), ApplyError> {
    let executor = match command {
        Command::NominateExecutor { executor } => Some(*executor),
        Command::CommitBatch(batch) => Some(batch.executor),
        Command::FinalizedThrough { executor, .. } => Some(*executor),
    };
    if let Some(executor) = executor
        && membership.membership().get_node(&executor.0).is_none()
    {
        return Err(ApplyError::ExecutorNotCurrentMember { executor });
    }
    Ok(())
}

fn decode_peer<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, PeerRpcError> {
    codec::decode(bytes).map_err(|error| PeerRpcError::Codec(error.to_string()))
}

fn encode_peer<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, PeerRpcError> {
    codec::encode(value).map_err(|error| PeerRpcError::Codec(error.to_string()))
}

fn map_client_write_error(
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
