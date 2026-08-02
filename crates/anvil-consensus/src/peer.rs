//! Transport-neutral peer networking for the compact decision Raft group.
//!
//! This module owns only Raft RPC transport and membership mutation. TLS,
//! admission, cluster descriptors, storage routing, and public APIs belong to
//! higher layers.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use openraft::BasicNode;
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::codec;
use crate::raft::{DecisionRaft, DecisionRaftConfig, DecisionRaftError, map_client_write_error};
use crate::types::MAX_RAFT_NODE_ID;

const PEER_RPC_SCHEMA_VERSION: u16 = 1;

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

/// One object-safe transport call without requiring an async-trait macro.
pub type PeerTransportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<u8>, PeerTransportError>> + Send + 'a>>;

/// Transport used by OpenRaft to reach one registered peer.
///
/// Implementations are responsible only for carrying the bounded envelope.
/// Authentication and encryption are intentionally outside this foundation.
pub trait PeerTransport: Send + Sync + 'static {
    fn send<'a>(&'a self, target: u64, node: &'a PeerNode, rpc: PeerRpc)
    -> PeerTransportFuture<'a>;
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

#[derive(Clone)]
pub(crate) struct PeerNetworkFactory {
    pub(crate) transport: Arc<dyn PeerTransport>,
}

pub(crate) struct PeerNetwork {
    transport: Arc<dyn PeerTransport>,
    target: u64,
    node: BasicNode,
}

impl RaftNetworkFactory<DecisionRaftConfig> for PeerNetworkFactory {
    type Network = PeerNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        PeerNetwork {
            transport: self.transport.clone(),
            target,
            node: node.clone(),
        }
    }
}

impl PeerNetwork {
    async fn call<Req, Resp, AppError>(
        &mut self,
        kind: PeerRpcKind,
        request: &Req,
        option: RPCOption,
    ) -> Result<Resp, RPCError<u64, BasicNode, RaftError<u64, AppError>>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        AppError: std::error::Error + Serialize + DeserializeOwned,
    {
        let payload =
            codec::encode(request).map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        let node = PeerNode::new(self.node.addr.clone());
        let call = self.transport.send(
            self.target,
            &node,
            PeerRpc {
                schema_version: PEER_RPC_SCHEMA_VERSION,
                kind,
                payload,
            },
        );
        let response = tokio::time::timeout(option.hard_ttl(), call)
            .await
            .map_err(|_| {
                let error = PeerTransportError::Unreachable(format!(
                    "peer {} did not respond before the Raft RPC deadline",
                    self.target
                ));
                RPCError::Unreachable(Unreachable::new(&error))
            })?
            .map_err(map_transport_error)?;
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

fn map_transport_error<AppError>(
    error: PeerTransportError,
) -> RPCError<u64, BasicNode, RaftError<u64, AppError>>
where
    AppError: std::error::Error,
{
    match &error {
        PeerTransportError::Unreachable(_) => RPCError::Unreachable(Unreachable::new(&error)),
        PeerTransportError::Protocol(_) => RPCError::Network(NetworkError::new(&error)),
    }
}

impl RaftNetwork<DecisionRaftConfig> for PeerNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<DecisionRaftConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.call(PeerRpcKind::AppendEntries, &rpc, option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<DecisionRaftConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        self.call(PeerRpcKind::InstallSnapshot, &rpc, option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.call(PeerRpcKind::Vote, &rpc, option).await
    }
}

impl DecisionRaft {
    /// Initialize a pristine Raft group with an explicit genesis membership.
    pub async fn initialize_genesis(
        &self,
        members: BTreeMap<u64, PeerNode>,
    ) -> Result<(), DecisionRaftError> {
        validate_members(&members)?;
        if !members.contains_key(&self.node_id) {
            return Err(DecisionRaftError::Configuration(format!(
                "genesis membership does not contain local node {}",
                self.node_id
            )));
        }
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
        validate_node(node_id, &node)?;
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
        if voters.is_empty() {
            return Err(DecisionRaftError::Configuration(
                "Raft voter membership must not be empty".into(),
            ));
        }
        if let Some(node_id) = voters
            .iter()
            .copied()
            .find(|node_id| !(1..=MAX_RAFT_NODE_ID).contains(node_id))
        {
            return Err(invalid_node_id(node_id));
        }
        self.raft
            .change_membership(voters, retain_removed_as_learners)
            .await
            .map(|_| ())
            .map_err(map_client_write_error)
    }

    pub async fn handle_peer_rpc(&self, rpc: PeerRpc) -> Result<Vec<u8>, PeerRpcError> {
        if rpc.schema_version != PEER_RPC_SCHEMA_VERSION {
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
}

fn validate_members(members: &BTreeMap<u64, PeerNode>) -> Result<(), DecisionRaftError> {
    if members.is_empty() {
        return Err(DecisionRaftError::Configuration(
            "genesis membership must not be empty".into(),
        ));
    }
    for (node_id, node) in members {
        validate_node(*node_id, node)?;
    }
    Ok(())
}

fn validate_node(node_id: u64, node: &PeerNode) -> Result<(), DecisionRaftError> {
    if !(1..=MAX_RAFT_NODE_ID).contains(&node_id) {
        return Err(invalid_node_id(node_id));
    }
    if node.address.is_empty() {
        return Err(DecisionRaftError::Configuration(format!(
            "peer node {node_id} has an empty address"
        )));
    }
    Ok(())
}

fn invalid_node_id(node_id: u64) -> DecisionRaftError {
    DecisionRaftError::Configuration(format!(
        "peer node id {node_id} is outside the supported range 1..={MAX_RAFT_NODE_ID}"
    ))
}

fn decode_peer<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, PeerRpcError> {
    codec::decode(bytes).map_err(|error| PeerRpcError::Codec(error.to_string()))
}

fn encode_peer<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, PeerRpcError> {
    codec::encode(value).map_err(|error| PeerRpcError::Codec(error.to_string()))
}

/// Bounded, process-local transport for exercising the real OpenRaft network
/// path without inventing a production wire protocol.
#[derive(Clone, Default)]
pub struct InMemoryPeerTransport {
    peers: Arc<RwLock<BTreeMap<u64, DecisionRaft>>>,
}

impl InMemoryPeerTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, node_id: u64, raft: DecisionRaft) -> Result<(), PeerTransportError> {
        let mut peers = self
            .peers
            .write()
            .map_err(|_| PeerTransportError::Protocol("peer registry lock poisoned".into()))?;
        if peers.contains_key(&node_id) {
            return Err(PeerTransportError::Protocol(format!(
                "peer {node_id} is already registered"
            )));
        }
        peers.insert(node_id, raft);
        Ok(())
    }

    pub fn unregister(&self, node_id: u64) -> Result<(), PeerTransportError> {
        self.peers
            .write()
            .map_err(|_| PeerTransportError::Protocol("peer registry lock poisoned".into()))?
            .remove(&node_id);
        Ok(())
    }
}

impl PeerTransport for InMemoryPeerTransport {
    fn send<'a>(
        &'a self,
        target: u64,
        _node: &'a PeerNode,
        rpc: PeerRpc,
    ) -> PeerTransportFuture<'a> {
        Box::pin(async move {
            let peer = self
                .peers
                .read()
                .map_err(|_| PeerTransportError::Protocol("peer registry lock poisoned".into()))?
                .get(&target)
                .cloned()
                .ok_or_else(|| {
                    PeerTransportError::Unreachable(format!("peer {target} is not registered"))
                })?;
            peer.handle_peer_rpc(rpc)
                .await
                .map_err(|error| PeerTransportError::Protocol(error.to_string()))
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct UnreachablePeerTransport;

impl PeerTransport for UnreachablePeerTransport {
    fn send<'a>(
        &'a self,
        target: u64,
        _node: &'a PeerNode,
        _rpc: PeerRpc,
    ) -> PeerTransportFuture<'a> {
        Box::pin(async move {
            Err(PeerTransportError::Unreachable(format!(
                "peer {target} has no configured transport"
            )))
        })
    }
}
