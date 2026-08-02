//! Deferred distributed-consensus capability.
//!
//! This module is intentionally not compiled by Anvil 0.5.0. The base release
//! has exactly one Raft member and a private network implementation that can
//! only report an unreachable peer. Keeping the peer protocol, transport,
//! inbound handlers, and membership mutation here prevents those later
//! capabilities from leaking into the base release surface.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use openraft::{BasicNode, RaftError};
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::codec;
use crate::raft::{DecisionRaft, DecisionRaftConfig, DecisionRaftError, map_client_write_error};

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

#[async_trait]
pub trait PeerTransport: Send + Sync + 'static {
    async fn send(
        &self,
        target: u64,
        node: &PeerNode,
        rpc: PeerRpc,
    ) -> Result<Vec<u8>, PeerTransportError>;
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

impl RaftNetwork<DecisionRaftConfig> for PeerNetwork {
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

impl DecisionRaft {
    pub async fn initialize_peers(
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
}

fn decode_peer<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, PeerRpcError> {
    codec::decode(bytes).map_err(|error| PeerRpcError::Codec(error.to_string()))
}

fn encode_peer<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, PeerRpcError> {
    codec::encode(value).map_err(|error| PeerRpcError::Codec(error.to_string()))
}
