//! The only module permitted to name concrete OpenRaft application types.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    io::Cursor,
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
    AppliedDecision, CertificationResult, CertificationState, CertifyTransaction,
    ClusterControlState, CommitVersion, CommittedBundleDecision, Consensus, ConsensusCommand,
    ConsensusError, ControlApplyResult, NodeId, NodeIncarnation, RaftStorageError, RocksRaftStore,
    storage::{KEY_LAST_PURGED_LOG_ID, KEY_OPENRAFT_STATE},
};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusNode {
    pub address: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusRpcKind {
    AppendEntries,
    Vote,
    InstallSnapshot,
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

/// Concrete OpenRaft V2 log store backed by the existing RocksDB.
#[derive(Clone)]
pub(crate) struct OpenRaftLogStore {
    store: RocksRaftStore,
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
}

impl OpenRaftConsensus {
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

    async fn apply_control(
        &self,
        command: ConsensusCommand,
    ) -> Result<ControlApplyResult, ConsensusError> {
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

    pub async fn install_node(
        &self,
        cluster_id_hash: [u8; 32],
        node: NodeIncarnation,
    ) -> Result<ControlApplyResult, ConsensusError> {
        self.apply_control(ConsensusCommand::InstallNode {
            cluster_id_hash,
            node,
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
        let config = openraft::Config {
            cluster_name: cluster_name.into(),
            ..Default::default()
        }
        .validate()
        .map_err(|error| ConsensusError::Rejected(error.to_string()))?;
        let (log_store, state_machine) = stores(store, cluster_id_hash)
            .map_err(|error| ConsensusError::Storage(error.to_string()))?;
        let runtime_store = state_machine.store.clone();
        let raft = openraft::Raft::new(
            node_id.0,
            Arc::new(config),
            NetworkFactoryAdapter { inner: network },
            log_store,
            state_machine,
        )
        .await
        .map_err(|error| ConsensusError::Storage(error.to_string()))?;
        Ok(Self {
            raft,
            store: runtime_store,
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

    async fn linearized_read_barrier(&self) -> Result<CommitVersion, ConsensusError> {
        self.raft
            .ensure_linearizable()
            .await
            .map(|log_id| CommitVersion(log_id.map_or(0, |id| id.index)))
            .map_err(map_raft_error)
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
        match self.store.append_logs(&encoded) {
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
            let response = match entry.payload {
                EntryPayload::Blank => RaftApplyResult::Noop,
                EntryPayload::Membership(membership) => {
                    state.membership = StoredMembership::new(Some(log_id), membership);
                    RaftApplyResult::Noop
                }
                EntryPayload::Normal(ConsensusCommand::Certify(command)) => {
                    let result = state
                        .certification
                        .apply(CommitVersion(log_id.index), &command);
                    match result {
                        Ok(result) => {
                            if matches!(result, CertificationResult::Committed { .. }) {
                                committed_bundle = Some(CommittedBundleDecision {
                                    cluster_id_hash: command.cluster_id_hash,
                                    bundle_hash: command.bundle_hash,
                                    bundle_length: command.bundle_length,
                                });
                            }
                            RaftApplyResult::Certification(result)
                        }
                        Err(error) => RaftApplyResult::Rejected(error.to_string()),
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
                        match state.control.apply(&command) {
                            Ok(result) => {
                                if let ControlApplyResult::GcWatermarkAdvanced(watermark) = &result
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
    use std::time::Duration;

    use openraft::{CommittedLeaderId, RaftTypeConfig, storage::RaftLogStorage};
    use tempfile::TempDir;

    use super::*;
    use crate::{BundleHash, DurabilityLevel, NodeId, NodeIncarnation, TransactionId};

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
    async fn duplicate_transaction_advances_raft_applied_id_but_keeps_commit_version() {
        let directory = TempDir::new().unwrap();
        let (_, mut machine) =
            stores(RocksRaftStore::open(directory.path(), 0).unwrap(), [1; 32]).unwrap();
        let command = CertifyTransaction {
            cluster_id_hash: [1; 32],
            transaction_id: TransactionId([1; 16]),
            snapshot_version: CommitVersion(0),
            point_observations: vec![],
            range_observations: vec![],
            written_point_keys: vec![],
            advanced_range_stamps: vec![],
            bundle_hash: BundleHash([2; 32]),
            bundle_length: 1,
            durability: DurabilityLevel::Local,
            durable_holders: vec![NodeIncarnation {
                node_id: NodeId(1),
                incarnation: 1,
            }],
        };
        let log_id_1 = LogId::new(CommittedLeaderId::new(1, 1), 1);
        let log_id_9 = LogId::new(CommittedLeaderId::new(1, 1), 9);
        let first = machine
            .apply([Entry {
                log_id: log_id_1,
                payload: EntryPayload::Normal(ConsensusCommand::Certify(command.clone())),
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
                    commit_version: CommitVersion(1),
                    ..
                }
            )]
        ));
        assert_eq!(machine.applied_state().await.unwrap().0, Some(log_id_9));
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
        let result = runtime.certify(test_command(8)).await.unwrap();
        let committed = match result {
            CertificationResult::Committed { commit_version, .. } => commit_version,
            other => panic!("unexpected result: {other:?}"),
        };
        assert_eq!(
            runtime.linearized_read_barrier().await.unwrap(),
            runtime.observed_commit_version()
        );
        assert!(runtime.observed_commit_version() >= committed);
        runtime.shutdown().await.unwrap();
    }

    fn test_command(id: u8) -> CertifyTransaction {
        CertifyTransaction {
            cluster_id_hash: [1; 32],
            transaction_id: TransactionId([id; 16]),
            snapshot_version: CommitVersion(0),
            point_observations: vec![],
            range_observations: vec![],
            written_point_keys: vec![],
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
