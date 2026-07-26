use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use anvil_mvcc_consensus::{
    ConsensusNode, ConsensusRpc, ConsensusRpcClient, ConsensusRpcError, ConsensusRpcFactory,
    ConsensusRpcKind, NodeId, OpenRaftConsensus,
};
use async_trait::async_trait;
use futures_core::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Response, Status,
    metadata::{AsciiMetadataValue, MetadataMap},
    transport::{Channel, Endpoint},
};
use uuid::Uuid;

use crate::anvil_api::{
    ConsensusRpcFrame, ConsensusRpcReply, ConsensusSessionOpen, ConsensusStreamRequest,
    ConsensusStreamResponse, consensus_stream_request, consensus_stream_response,
    consensus_transport_client::ConsensusTransportClient,
    consensus_transport_server::ConsensusTransport,
};

const NODE_TOKEN_HEADER: &str = "x-anvil-node-token";

#[async_trait]
pub trait ConsensusConnectionAuthorizer: Send + Sync + 'static {
    /// Validate the node token and Zanzibar node relationship once for this
    /// stream. Successful return authorizes subsequent RPC frames.
    async fn authorize(
        &self,
        metadata: &MetadataMap,
        open: &ConsensusSessionOpen,
    ) -> Result<(), Status>;
}

pub struct ConsensusTransportService<A> {
    runtime: Arc<OpenRaftConsensus>,
    authorizer: Arc<A>,
}

impl<A> Clone for ConsensusTransportService<A> {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            authorizer: self.authorizer.clone(),
        }
    }
}

impl<A> ConsensusTransportService<A> {
    pub fn new(runtime: Arc<OpenRaftConsensus>, authorizer: A) -> Self {
        Self {
            runtime,
            authorizer: Arc::new(authorizer),
        }
    }
}

#[async_trait]
impl<A: ConsensusConnectionAuthorizer> ConsensusTransport for ConsensusTransportService<A> {
    type ExchangeStream =
        Pin<Box<dyn Stream<Item = Result<ConsensusStreamResponse, Status>> + Send + 'static>>;

    async fn exchange(
        &self,
        request: Request<tonic::Streaming<ConsensusStreamRequest>>,
    ) -> Result<Response<Self::ExchangeStream>, Status> {
        let metadata = request.metadata().clone();
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("consensus stream requires session open"))?;
        let open = match first.message {
            Some(consensus_stream_request::Message::Open(open)) => open,
            _ => {
                return Err(Status::invalid_argument(
                    "first consensus stream frame must open a session",
                ));
            }
        };
        if open.node_id == 0 || open.node_incarnation == 0 {
            return Err(Status::invalid_argument(
                "node ID and incarnation must be non-zero",
            ));
        }
        self.authorizer.authorize(&metadata, &open).await?;

        let (output, receiver) = mpsc::channel(32);
        let runtime = self.runtime.clone();
        tokio::spawn(async move {
            if output
                .send(Ok(ConsensusStreamResponse {
                    message: Some(consensus_stream_response::Message::AcceptedSessionId(
                        Uuid::new_v4().to_string(),
                    )),
                }))
                .await
                .is_err()
            {
                return;
            }
            loop {
                let request = match inbound.message().await {
                    Ok(Some(request)) => request,
                    Ok(None) => return,
                    Err(error) => {
                        let _ = output.send(Err(error)).await;
                        return;
                    }
                };
                let frame = match request.message {
                    Some(consensus_stream_request::Message::Rpc(frame)) => frame,
                    Some(consensus_stream_request::Message::Open(_)) => {
                        let _ = output
                            .send(Err(Status::failed_precondition(
                                "reconnect requires a new consensus stream",
                            )))
                            .await;
                        return;
                    }
                    None => {
                        let _ = output
                            .send(Err(Status::invalid_argument(
                                "empty consensus stream frame",
                            )))
                            .await;
                        return;
                    }
                };
                let reply = dispatch(&runtime, frame).await;
                if output
                    .send(Ok(ConsensusStreamResponse {
                        message: Some(consensus_stream_response::Message::Reply(reply)),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

async fn dispatch(runtime: &OpenRaftConsensus, frame: ConsensusRpcFrame) -> ConsensusRpcReply {
    let kind = match frame.kind {
        1 => ConsensusRpcKind::AppendEntries,
        2 => ConsensusRpcKind::Vote,
        3 => ConsensusRpcKind::InstallSnapshot,
        other => {
            return ConsensusRpcReply {
                request_id: frame.request_id,
                error: format!("unknown consensus RPC kind {other}"),
                ..Default::default()
            };
        }
    };
    match runtime
        .handle_rpc(ConsensusRpc {
            schema_version: frame.schema_version as u16,
            kind,
            payload: frame.payload,
        })
        .await
    {
        Ok(payload) => ConsensusRpcReply {
            request_id: frame.request_id,
            payload,
            error: String::new(),
        },
        Err(error) => ConsensusRpcReply {
            request_id: frame.request_id,
            payload: Vec::new(),
            error: error.to_string(),
        },
    }
}

#[derive(Clone)]
pub struct TonicConsensusRpcFactory {
    local_node_id: NodeId,
    local_incarnation: u64,
    node_token: Arc<str>,
    request_timeout: Duration,
    channels: Arc<Mutex<HashMap<String, Channel>>>,
}

impl TonicConsensusRpcFactory {
    pub fn new(
        local_node_id: NodeId,
        local_incarnation: u64,
        node_token: impl Into<Arc<str>>,
        request_timeout: Duration,
    ) -> Self {
        Self {
            local_node_id,
            local_incarnation,
            node_token: node_token.into(),
            request_timeout,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn channel(&self, address: &str) -> Result<Channel, String> {
        let mut channels = self
            .channels
            .lock()
            .map_err(|_| "consensus channel cache lock poisoned".to_string())?;
        if let Some(channel) = channels.get(address) {
            return Ok(channel.clone());
        }
        let channel = Endpoint::from_shared(address.to_string())
            .map_err(|error| error.to_string())?
            .connect_timeout(self.request_timeout)
            .timeout(self.request_timeout)
            .connect_lazy();
        channels.insert(address.to_string(), channel.clone());
        Ok(channel)
    }
}

impl ConsensusRpcFactory for TonicConsensusRpcFactory {
    fn client(&self, _target: NodeId, node: &ConsensusNode) -> Box<dyn ConsensusRpcClient> {
        Box::new(TonicConsensusRpcClient {
            channel: self.channel(&node.address),
            local_node_id: self.local_node_id,
            local_incarnation: self.local_incarnation,
            node_token: self.node_token.clone(),
            request_timeout: self.request_timeout,
            next_request_id: 1,
            session: None,
        })
    }
}

struct ConnectedSession {
    output: mpsc::Sender<ConsensusStreamRequest>,
    input: tonic::Streaming<ConsensusStreamResponse>,
}

struct TonicConsensusRpcClient {
    channel: Result<Channel, String>,
    local_node_id: NodeId,
    local_incarnation: u64,
    node_token: Arc<str>,
    request_timeout: Duration,
    next_request_id: u64,
    session: Option<ConnectedSession>,
}

impl TonicConsensusRpcClient {
    async fn connect(&self) -> Result<ConnectedSession, ConsensusRpcError> {
        let channel = self
            .channel
            .as_ref()
            .map_err(|error| ConsensusRpcError::Unreachable(error.clone()))?
            .clone();
        let (output, receiver) = mpsc::channel(32);
        output
            .send(ConsensusStreamRequest {
                message: Some(consensus_stream_request::Message::Open(
                    ConsensusSessionOpen {
                        node_id: self.local_node_id.0,
                        node_incarnation: self.local_incarnation,
                    },
                )),
            })
            .await
            .map_err(|_| ConsensusRpcError::Unreachable("open stream closed".into()))?;
        let mut request = Request::new(ReceiverStream::new(receiver));
        let token: AsciiMetadataValue = self
            .node_token
            .parse()
            .map_err(|error| ConsensusRpcError::Protocol(format!("invalid node token: {error}")))?;
        request.metadata_mut().insert(NODE_TOKEN_HEADER, token);
        let mut input = ConsensusTransportClient::new(channel)
            .exchange(request)
            .await
            .map_err(|error| ConsensusRpcError::Unreachable(error.to_string()))?
            .into_inner();
        match input
            .message()
            .await
            .map_err(|error| ConsensusRpcError::Unreachable(error.to_string()))?
            .and_then(|message| message.message)
        {
            Some(consensus_stream_response::Message::AcceptedSessionId(_)) => {
                Ok(ConnectedSession { output, input })
            }
            _ => Err(ConsensusRpcError::Protocol(
                "peer did not accept consensus session".into(),
            )),
        }
    }

    async fn request_once(&mut self, rpc: ConsensusRpc) -> Result<Vec<u8>, ConsensusRpcError> {
        if self.session.is_none() {
            self.session = Some(self.connect().await?);
        }
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        let session = self.session.as_mut().expect("session established");
        session
            .output
            .send(ConsensusStreamRequest {
                message: Some(consensus_stream_request::Message::Rpc(ConsensusRpcFrame {
                    request_id,
                    schema_version: u32::from(rpc.schema_version),
                    kind: match rpc.kind {
                        ConsensusRpcKind::AppendEntries => 1,
                        ConsensusRpcKind::Vote => 2,
                        ConsensusRpcKind::InstallSnapshot => 3,
                    },
                    payload: rpc.payload,
                })),
            })
            .await
            .map_err(|_| ConsensusRpcError::Unreachable("consensus stream closed".into()))?;
        let response = session
            .input
            .message()
            .await
            .map_err(|error| ConsensusRpcError::Unreachable(error.to_string()))?
            .ok_or_else(|| ConsensusRpcError::Unreachable("consensus stream ended".into()))?;
        let Some(consensus_stream_response::Message::Reply(reply)) = response.message else {
            return Err(ConsensusRpcError::Protocol(
                "unexpected consensus stream response".into(),
            ));
        };
        if reply.request_id != request_id {
            return Err(ConsensusRpcError::Protocol(format!(
                "consensus response ID mismatch: expected {request_id}, received {}",
                reply.request_id
            )));
        }
        if !reply.error.is_empty() {
            return Err(ConsensusRpcError::Protocol(reply.error));
        }
        Ok(reply.payload)
    }
}

#[async_trait]
impl ConsensusRpcClient for TonicConsensusRpcClient {
    async fn request(&mut self, rpc: ConsensusRpc) -> Result<Vec<u8>, ConsensusRpcError> {
        let first =
            tokio::time::timeout(self.request_timeout, self.request_once(rpc.clone())).await;
        match first {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(ConsensusRpcError::Protocol(error))) => Err(ConsensusRpcError::Protocol(error)),
            Ok(Err(ConsensusRpcError::Unreachable(_))) | Err(_) => {
                self.session = None;
                tokio::time::timeout(self.request_timeout, self.request_once(rpc))
                    .await
                    .map_err(|_| ConsensusRpcError::Unreachable("consensus RPC timed out".into()))?
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anvil_mvcc_consensus::RocksRaftStore;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::*;
    use crate::anvil_api::consensus_transport_server::ConsensusTransportServer;

    struct UnusedNetwork;

    impl ConsensusRpcFactory for UnusedNetwork {
        fn client(&self, _target: NodeId, _node: &ConsensusNode) -> Box<dyn ConsensusRpcClient> {
            panic!("the transport test does not initialize a Raft cluster")
        }
    }

    struct CountingAuthorizer {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ConsensusConnectionAuthorizer for CountingAuthorizer {
        async fn authorize(
            &self,
            metadata: &MetadataMap,
            _open: &ConsensusSessionOpen,
        ) -> Result<(), Status> {
            assert_eq!(
                metadata
                    .get(NODE_TOKEN_HEADER)
                    .and_then(|value| value.to_str().ok()),
                Some("test-node-token")
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn authenticates_once_and_reuses_the_bidirectional_stream() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = Arc::new(
            OpenRaftConsensus::new(
                NodeId(1),
                RocksRaftStore::open(directory.path(), 1).unwrap(),
                "transport-test",
                Arc::new(UnusedNetwork),
            )
            .await
            .unwrap(),
        );
        let authorization_calls = Arc::new(AtomicUsize::new(0));
        let service = ConsensusTransportService::new(
            runtime,
            CountingAuthorizer {
                calls: authorization_calls.clone(),
            },
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(
            Server::builder()
                .add_service(ConsensusTransportServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );

        let factory =
            TonicConsensusRpcFactory::new(NodeId(2), 1, "test-node-token", Duration::from_secs(5));
        let mut client = factory.client(
            NodeId(1),
            &ConsensusNode {
                address: format!("http://{address}"),
            },
        );
        let invalid_rpc = ConsensusRpc {
            schema_version: 99,
            kind: ConsensusRpcKind::Vote,
            payload: Vec::new(),
        };

        assert!(matches!(
            client.request(invalid_rpc.clone()).await,
            Err(ConsensusRpcError::Protocol(_))
        ));
        assert!(matches!(
            client.request(invalid_rpc).await,
            Err(ConsensusRpcError::Protocol(_))
        ));
        assert_eq!(authorization_calls.load(Ordering::SeqCst), 1);

        server.abort();
    }
}
