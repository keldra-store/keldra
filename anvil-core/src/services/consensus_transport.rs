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
        if open.node_id == 0 || open.node_incarnation == 0 || open.cluster_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "cluster ID and non-zero node identity are required",
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
    let snapshot_started_at =
        (kind == ConsensusRpcKind::InstallSnapshot).then(std::time::Instant::now);
    let result = runtime
        .handle_rpc(ConsensusRpc {
            schema_version: frame.schema_version as u16,
            kind,
            payload: frame.payload,
        })
        .await;
    if let Some(started_at) = snapshot_started_at {
        crate::perf::record_consensus_phase(
            "snapshot",
            if result.is_ok() { "ok" } else { "error" },
            started_at.elapsed(),
        );
        tracing::debug!(
            operation = "consensus.snapshot",
            status = if result.is_ok() { "ok" } else { "error" },
            "processed Raft snapshot installation"
        );
    }
    match result {
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
    cluster_id: Arc<str>,
    local_node_id: NodeId,
    local_incarnation: u64,
    node_token: Arc<str>,
    request_timeout: Duration,
    channels: Arc<Mutex<HashMap<String, Channel>>>,
}

impl TonicConsensusRpcFactory {
    pub fn new(
        cluster_id: impl Into<Arc<str>>,
        local_node_id: NodeId,
        local_incarnation: u64,
        node_token: impl Into<Arc<str>>,
        request_timeout: Duration,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
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
            cluster_id: self.cluster_id.clone(),
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
    cluster_id: Arc<str>,
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
                        cluster_id: self.cluster_id.to_string(),
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

    use anvil_mvcc_consensus::{
        BundleHash, CertificationResult, CertifyTransaction, CommitVersion, Consensus,
        DurabilityLevel, LogicalKeyHash, NodeIncarnation, PointObservation, RocksRaftStore,
        TransactionId,
    };
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
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

    #[derive(Clone)]
    struct ClusterAuthorizer {
        cluster_id: &'static str,
        token: &'static str,
    }

    #[async_trait]
    impl ConsensusConnectionAuthorizer for ClusterAuthorizer {
        async fn authorize(
            &self,
            metadata: &MetadataMap,
            open: &ConsensusSessionOpen,
        ) -> Result<(), Status> {
            if open.cluster_id != self.cluster_id {
                return Err(Status::permission_denied("wrong cluster"));
            }
            if metadata
                .get(NODE_TOKEN_HEADER)
                .and_then(|value| value.to_str().ok())
                != Some(self.token)
            {
                return Err(Status::unauthenticated("wrong token"));
            }
            Ok(())
        }
    }

    async fn serve(
        listener: TcpListener,
        runtime: Arc<OpenRaftConsensus>,
        cluster_id: &'static str,
    ) -> JoinHandle<Result<(), tonic::transport::Error>> {
        tokio::spawn(
            Server::builder()
                .add_service(ConsensusTransportServer::new(
                    ConsensusTransportService::new(
                        runtime,
                        ClusterAuthorizer {
                            cluster_id,
                            token: "cluster-token",
                        },
                    ),
                ))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        )
    }

    async fn node(
        id: u64,
        store: RocksRaftStore,
        cluster_id: &'static str,
        cluster_hash: [u8; 32],
    ) -> Arc<OpenRaftConsensus> {
        Arc::new(
            OpenRaftConsensus::new(
                NodeId(id),
                store,
                cluster_hash,
                cluster_id,
                Arc::new(TonicConsensusRpcFactory::new(
                    cluster_id,
                    NodeId(id),
                    1,
                    "cluster-token",
                    Duration::from_secs(2),
                )),
            )
            .await
            .unwrap(),
        )
    }

    fn command(id: u8, cluster_hash: [u8; 32]) -> CertifyTransaction {
        let key = LogicalKeyHash([9; 32]);
        CertifyTransaction {
            cluster_id_hash: cluster_hash,
            transaction_id: TransactionId([id; 16]),
            snapshot_version: CommitVersion(0),
            point_observations: vec![PointObservation {
                key,
                observed_version: None,
            }],
            range_observations: Vec::new(),
            predicates: Vec::new(),
            assignment_predicates: Vec::new(),
            written_point_keys: vec![key],
            written_points: vec![anvil_mvcc_consensus::WrittenPoint {
                key,
                value_hash: Some([id; 32]),
            }],
            advanced_range_stamps: Vec::new(),
            bundle_hash: BundleHash([id; 32]),
            bundle_length: 1,
            durability: DurabilityLevel::Local,
            durable_holders: vec![NodeIncarnation {
                node_id: NodeId(1),
                incarnation: 1,
            }],
        }
    }

    async fn wait_until(description: &str, mut condition: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !condition() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {description}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
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
                [1; 32],
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

        let factory = TonicConsensusRpcFactory::new(
            "transport-test",
            NodeId(2),
            1,
            "test-node-token",
            Duration::from_secs(5),
        );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_nodes_certify_one_conflict_converge_and_rejoin_after_restart() {
        const CLUSTER: &str = "cluster-three";
        const HASH: [u8; 32] = [3; 32];
        let directories = [
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        ];
        let listeners = [
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
        ];
        let mut addresses = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap())
            .collect::<Vec<_>>();
        let stores = [
            RocksRaftStore::open(directories[0].path(), 1).unwrap(),
            RocksRaftStore::open(directories[1].path(), 1).unwrap(),
            RocksRaftStore::open(directories[2].path(), 1).unwrap(),
        ];
        let third_db = stores[2].database().clone();
        let first = node(1, stores[0].clone(), CLUSTER, HASH).await;
        let second = node(2, stores[1].clone(), CLUSTER, HASH).await;
        let third = node(3, stores[2].clone(), CLUSTER, HASH).await;
        let mut servers = Vec::new();
        for (listener, runtime) in
            listeners
                .into_iter()
                .zip([first.clone(), second.clone(), third.clone()])
        {
            servers.push(serve(listener, runtime, CLUSTER).await);
        }
        let membership = addresses
            .iter()
            .enumerate()
            .map(|(index, address)| {
                (
                    NodeId(index as u64 + 1),
                    ConsensusNode {
                        address: format!("http://{address}"),
                    },
                )
            })
            .collect();
        first.initialize(membership).await.unwrap();
        let leadership_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if tokio::time::timeout(Duration::from_millis(250), first.linearized_read_barrier())
                .await
                .is_ok_and(|result| result.is_ok())
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < leadership_deadline,
                "timed out waiting for node one leadership"
            );
        }

        let (left, right) = tokio::join!(
            first.certify(command(1, HASH)),
            first.certify(command(2, HASH))
        );
        let outcomes = [left.unwrap(), right.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| matches!(result, CertificationResult::Committed { .. }))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| matches!(result, CertificationResult::Aborted { .. }))
                .count(),
            1
        );
        let applied = first.observed_commit_version();
        wait_until("followers to converge", || {
            second.observed_commit_version() >= applied
                && third.observed_commit_version() >= applied
        })
        .await;

        servers[2].abort();
        let _ = (&mut servers[2]).await;
        third.shutdown().await.unwrap();
        drop(third);
        first
            .change_membership([NodeId(1), NodeId(2)].into_iter().collect(), false)
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses[2] = listener.local_addr().unwrap();
        let restarted = node(
            3,
            RocksRaftStore::from_db(third_db, 1).unwrap(),
            CLUSTER,
            HASH,
        )
        .await;
        servers[2] = serve(listener, restarted.clone(), CLUSTER).await;
        first
            .add_learner(
                NodeId(3),
                ConsensusNode {
                    address: format!("http://{}", addresses[2]),
                },
                true,
            )
            .await
            .unwrap();
        first.certify(command(3, HASH)).await.unwrap();
        let latest = first.observed_commit_version();
        wait_until("restarted follower catch-up", || {
            restarted.observed_commit_version() >= latest
        })
        .await;
        first
            .change_membership(
                [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect(),
                false,
            )
            .await
            .unwrap();

        for server in servers {
            server.abort();
        }
    }

    #[tokio::test]
    async fn consensus_stream_rejects_another_cluster_before_dispatch() {
        let directory = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let runtime = Arc::new(
            OpenRaftConsensus::new(
                NodeId(1),
                RocksRaftStore::open(directory.path(), 1).unwrap(),
                [1; 32],
                "cluster-a",
                Arc::new(UnusedNetwork),
            )
            .await
            .unwrap(),
        );
        let server = serve(listener, runtime, "cluster-a").await;
        let factory = TonicConsensusRpcFactory::new(
            "cluster-b",
            NodeId(2),
            1,
            "cluster-token",
            Duration::from_secs(2),
        );
        let mut client = factory.client(
            NodeId(1),
            &ConsensusNode {
                address: format!("http://{address}"),
            },
        );

        assert!(matches!(
            client
                .request(ConsensusRpc {
                    schema_version: 1,
                    kind: ConsensusRpcKind::Vote,
                    payload: Vec::new(),
                })
                .await,
            Err(ConsensusRpcError::Unreachable(error)) if error.contains("wrong cluster")
        ));
        server.abort();
    }
}
