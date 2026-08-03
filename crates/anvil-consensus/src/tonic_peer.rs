//! Tonic adapter for OpenRaft traffic and the transient serving-lease RPC.
//!
//! The caller supplies the already-configured mandatory-mTLS connector and
//! yields [`crate::AcceptedPeerTls`] streams to Tonic on the server. Tonic puts
//! each stream's presented SPKI pin in request extensions; the handler checks
//! that pin and claimed node against freshly read committed state on every RPC.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use hyper_util::rt::TokioIo;
use tonic::codegen::Service;
use tonic::codegen::http::Uri;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use crate::codec;
use crate::peer::{
    PEER_RPC_SCHEMA_VERSION, PeerNode, PeerRpc, PeerRpcError, PeerRpcKind, PeerTransport,
    PeerTransportError, PeerTransportFuture,
};
use crate::peer_tls::{
    CommittedPeerPinProvider, PeerTlsConnector, PeerTlsError, authorize_peer_rpc,
};
use crate::types::{ClusterId, MAX_RAFT_NODE_ID, NodeId, PeerSpkiSha256};
use crate::{
    DecisionRaft, ServingLeaseError, ServingLeaseGrant, ServingLeaseIssuer, ServingLeaseRequest,
};

mod wire {
    tonic::include_proto!("anvil.peer.v1");
}

const WIRE_SCHEMA_VERSION: u32 = PEER_RPC_SCHEMA_VERSION as u32;
const MAX_WIRE_MESSAGE_BYTES: usize = codec::MAX_ENCODED_BYTES + 128;

/// Server-side implementation of the private Raft peer protocol.
#[derive(Clone)]
pub struct TonicRaftPeerService {
    raft: DecisionRaft,
    pins: Arc<dyn CommittedPeerPinProvider>,
    serving_leases: ServingLeaseIssuer,
}

/// Generated Tonic service with Anvil's explicit message bounds applied.
pub type TonicRaftPeerServer = wire::raft_peer_server::RaftPeerServer<TonicRaftPeerService>;

impl TonicRaftPeerService {
    pub fn new(raft: DecisionRaft, pins: Arc<dyn CommittedPeerPinProvider>) -> Self {
        Self::with_serving_lease_issuer(raft, pins, ServingLeaseIssuer::new())
    }

    pub fn with_serving_lease_issuer(
        raft: DecisionRaft,
        pins: Arc<dyn CommittedPeerPinProvider>,
        serving_leases: ServingLeaseIssuer,
    ) -> Self {
        Self {
            raft,
            pins,
            serving_leases,
        }
    }

    pub fn into_server(self) -> TonicRaftPeerServer {
        TonicRaftPeerServer::new(self)
            .max_decoding_message_size(MAX_WIRE_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_WIRE_MESSAGE_BYTES)
    }

    async fn handle(
        &self,
        mut request: Request<wire::RaftRequest>,
        kind: PeerRpcKind,
    ) -> Result<Response<wire::RaftResponse>, Status> {
        let presented_pin = request
            .extensions()
            .get::<PeerSpkiSha256>()
            .copied()
            .ok_or_else(|| Status::unauthenticated("peer mTLS identity is missing"))?;
        let cluster_id = parse_cluster_id(&request.get_ref().cluster_id)?;
        let source_node_id = NodeId(request.get_ref().source_node_id);
        let authenticated = authorize_peer_rpc(
            self.pins.as_ref(),
            cluster_id,
            source_node_id,
            kind,
            presented_pin,
        )
        .map_err(map_peer_authorization_error)?;
        request.extensions_mut().insert(authenticated);

        let envelope = request.into_inner();
        if envelope.schema_version != WIRE_SCHEMA_VERSION {
            return Err(Status::failed_precondition(format!(
                "unsupported Raft peer schema {}",
                envelope.schema_version
            )));
        }
        if envelope.payload.len() > codec::MAX_ENCODED_BYTES {
            return Err(Status::resource_exhausted(
                "Raft peer payload exceeds the consensus limit",
            ));
        }
        let payload = self
            .raft
            .handle_peer_rpc(PeerRpc {
                schema_version: PEER_RPC_SCHEMA_VERSION,
                kind,
                payload: envelope.payload,
            })
            .await
            .map_err(map_peer_rpc_error)?;
        Ok(Response::new(wire::RaftResponse {
            schema_version: WIRE_SCHEMA_VERSION,
            payload,
        }))
    }

    async fn issue_serving_lease(
        &self,
        mut request: Request<wire::RaftRequest>,
    ) -> Result<Response<wire::RaftResponse>, Status> {
        let presented_pin = request
            .extensions()
            .get::<PeerSpkiSha256>()
            .copied()
            .ok_or_else(|| Status::unauthenticated("peer mTLS identity is missing"))?;
        let cluster_id = parse_cluster_id(&request.get_ref().cluster_id)?;
        let source_node_id = NodeId(request.get_ref().source_node_id);
        let authenticated = authorize_peer_rpc(
            self.pins.as_ref(),
            cluster_id,
            source_node_id,
            PeerRpcKind::ServingLease,
            presented_pin,
        )
        .map_err(map_peer_authorization_error)?;
        request.extensions_mut().insert(authenticated);

        let envelope = request.into_inner();
        validate_wire_request(&envelope)?;
        let lease_request: ServingLeaseRequest = codec::decode(&envelope.payload)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if lease_request.cluster_id != cluster_id {
            return Err(Status::invalid_argument(
                "serving lease payload cluster does not match its peer envelope",
            ));
        }
        let grant = self
            .serving_leases
            .grant(&self.raft, lease_request)
            .await
            .map_err(map_serving_lease_error)?;
        let payload = codec::encode(&grant).map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(wire::RaftResponse {
            schema_version: WIRE_SCHEMA_VERSION,
            payload,
        }))
    }
}

#[tonic::async_trait]
impl wire::raft_peer_server::RaftPeer for TonicRaftPeerService {
    async fn append_entries(
        &self,
        request: Request<wire::RaftRequest>,
    ) -> Result<Response<wire::RaftResponse>, Status> {
        self.handle(request, PeerRpcKind::AppendEntries).await
    }

    async fn vote(
        &self,
        request: Request<wire::RaftRequest>,
    ) -> Result<Response<wire::RaftResponse>, Status> {
        self.handle(request, PeerRpcKind::Vote).await
    }

    async fn install_snapshot(
        &self,
        request: Request<wire::RaftRequest>,
    ) -> Result<Response<wire::RaftResponse>, Status> {
        self.handle(request, PeerRpcKind::InstallSnapshot).await
    }

    async fn grant_serving_lease(
        &self,
        request: Request<wire::RaftRequest>,
    ) -> Result<Response<wire::RaftResponse>, Status> {
        self.issue_serving_lease(request).await
    }
}

/// Production [`PeerTransport`] backed by cached Tonic HTTP/2 channels.
#[derive(Clone)]
pub struct TonicPeerTransport {
    cluster_id: ClusterId,
    source_node_id: NodeId,
    tls: PeerTlsConnector,
    channels: Arc<Mutex<BTreeMap<u64, (String, Channel)>>>,
}

impl TonicPeerTransport {
    pub fn new(
        cluster_id: ClusterId,
        source_node_id: NodeId,
        tls: PeerTlsConnector,
    ) -> Result<Self, PeerTransportError> {
        if cluster_id.0 == [0; 16] {
            return Err(PeerTransportError::Protocol(
                "cluster id must not be all zero".into(),
            ));
        }
        if !(1..=MAX_RAFT_NODE_ID).contains(&source_node_id.0) {
            return Err(PeerTransportError::Protocol(format!(
                "source node id {} is outside the supported range 1..={MAX_RAFT_NODE_ID}",
                source_node_id.0
            )));
        }
        Ok(Self {
            cluster_id,
            source_node_id,
            tls,
            channels: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    fn channel(&self, target: u64, address: &str) -> Result<Channel, PeerTransportError> {
        if !(1..=MAX_RAFT_NODE_ID).contains(&target) {
            return Err(PeerTransportError::Protocol(format!(
                "target node id {target} is outside the supported range 1..={MAX_RAFT_NODE_ID}"
            )));
        }
        if address.is_empty() {
            return Err(PeerTransportError::Protocol(
                "target peer address is empty".into(),
            ));
        }
        let mut channels = self
            .channels
            .lock()
            .map_err(|_| PeerTransportError::Protocol("peer channel lock poisoned".into()))?;
        if let Some((cached_address, channel)) = channels.get(&target)
            && cached_address == address
        {
            return Ok(channel.clone());
        }
        let connector = PeerChannelConnector {
            tls: self.tls.clone(),
            target: NodeId(target),
            address: address.to_owned(),
        };
        let channel = Endpoint::from_static("http://anvil-peer.invalid")
            .connect_with_connector_lazy(connector);
        channels.insert(target, (address.to_owned(), channel.clone()));
        Ok(channel)
    }

    async fn send_rpc(
        &self,
        target: u64,
        node: &PeerNode,
        rpc: PeerRpc,
    ) -> Result<Vec<u8>, PeerTransportError> {
        if rpc.schema_version != PEER_RPC_SCHEMA_VERSION {
            return Err(PeerTransportError::Protocol(format!(
                "unsupported peer RPC schema {}",
                rpc.schema_version
            )));
        }
        if rpc.payload.len() > codec::MAX_ENCODED_BYTES {
            return Err(PeerTransportError::Protocol(
                "peer RPC payload exceeds the consensus limit".into(),
            ));
        }
        let channel = self.channel(target, &node.address)?;
        let mut client = wire::raft_peer_client::RaftPeerClient::new(channel)
            .max_decoding_message_size(MAX_WIRE_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_WIRE_MESSAGE_BYTES);
        let request = wire::RaftRequest {
            schema_version: WIRE_SCHEMA_VERSION,
            cluster_id: self.cluster_id.into_bytes().to_vec(),
            source_node_id: self.source_node_id.0,
            payload: rpc.payload,
        };
        let response = match rpc.kind {
            PeerRpcKind::AppendEntries => client.append_entries(request).await,
            PeerRpcKind::Vote => client.vote(request).await,
            PeerRpcKind::InstallSnapshot => client.install_snapshot(request).await,
            PeerRpcKind::ServingLease | PeerRpcKind::DataPlane | PeerRpcKind::StateTransfer => {
                return Err(PeerTransportError::Protocol(
                    "this RPC class requires a typed peer method".into(),
                ));
            }
        }
        .map_err(map_tonic_error)?
        .into_inner();
        if response.schema_version != WIRE_SCHEMA_VERSION {
            return Err(PeerTransportError::Protocol(format!(
                "peer returned unsupported Raft schema {}",
                response.schema_version
            )));
        }
        if response.payload.len() > codec::MAX_ENCODED_BYTES {
            return Err(PeerTransportError::Protocol(
                "peer response exceeds the consensus limit".into(),
            ));
        }
        Ok(response.payload)
    }

    /// Request one transient serving grant from the current leader over the
    /// same cached mandatory-mTLS channel used by Raft.
    pub async fn request_serving_lease(
        &self,
        target: u64,
        node: &PeerNode,
        request: ServingLeaseRequest,
    ) -> Result<ServingLeaseGrant, PeerTransportError> {
        if request.cluster_id != self.cluster_id {
            return Err(PeerTransportError::Protocol(
                "serving lease request belongs to another cluster".into(),
            ));
        }
        let payload = codec::encode(&request)
            .map_err(|error| PeerTransportError::Protocol(error.to_string()))?;
        let channel = self.channel(target, &node.address)?;
        let mut client = wire::raft_peer_client::RaftPeerClient::new(channel)
            .max_decoding_message_size(MAX_WIRE_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_WIRE_MESSAGE_BYTES);
        let response = client
            .grant_serving_lease(wire::RaftRequest {
                schema_version: WIRE_SCHEMA_VERSION,
                cluster_id: self.cluster_id.into_bytes().to_vec(),
                source_node_id: self.source_node_id.0,
                payload,
            })
            .await
            .map_err(map_tonic_error)?
            .into_inner();
        validate_wire_response(&response)?;
        codec::decode(&response.payload)
            .map_err(|error| PeerTransportError::Protocol(error.to_string()))
    }
}

impl PeerTransport for TonicPeerTransport {
    fn send<'a>(
        &'a self,
        target: u64,
        node: &'a PeerNode,
        rpc: PeerRpc,
    ) -> PeerTransportFuture<'a> {
        Box::pin(self.send_rpc(target, node, rpc))
    }
}

#[derive(Clone)]
struct PeerChannelConnector {
    tls: PeerTlsConnector,
    target: NodeId,
    address: String,
}

impl Service<Uri> for PeerChannelConnector {
    type Response = TokioIo<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>;
    type Error = PeerTlsError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: Uri) -> Self::Future {
        let tls = self.tls.clone();
        let target = self.target;
        let address = self.address.clone();
        Box::pin(async move {
            tls.connect(target, &address)
                .await
                .map(|peer| TokioIo::new(peer.stream))
        })
    }
}

fn map_peer_authorization_error(_error: PeerTlsError) -> Status {
    Status::permission_denied("peer RPC is not authorized")
}

fn parse_cluster_id(bytes: &[u8]) -> Result<ClusterId, Status> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("cluster_id must contain exactly 16 bytes"))?;
    if bytes == [0; 16] {
        return Err(Status::invalid_argument("cluster_id must not be all zero"));
    }
    Ok(ClusterId(bytes))
}

fn map_peer_rpc_error(error: PeerRpcError) -> Status {
    match error {
        PeerRpcError::UnsupportedSchema(version) => {
            Status::failed_precondition(format!("unsupported Raft peer schema {version}"))
        }
        PeerRpcError::PayloadTooLarge => {
            Status::resource_exhausted("Raft peer payload exceeds the consensus limit")
        }
        PeerRpcError::Codec(message) => Status::invalid_argument(message),
    }
}

fn validate_wire_request(request: &wire::RaftRequest) -> Result<(), Status> {
    if request.schema_version != WIRE_SCHEMA_VERSION {
        return Err(Status::failed_precondition(format!(
            "unsupported peer schema {}",
            request.schema_version
        )));
    }
    if request.payload.len() > codec::MAX_ENCODED_BYTES {
        return Err(Status::resource_exhausted(
            "peer payload exceeds the consensus limit",
        ));
    }
    Ok(())
}

fn validate_wire_response(response: &wire::RaftResponse) -> Result<(), PeerTransportError> {
    if response.schema_version != WIRE_SCHEMA_VERSION {
        return Err(PeerTransportError::Protocol(format!(
            "peer returned unsupported schema {}",
            response.schema_version
        )));
    }
    if response.payload.len() > codec::MAX_ENCODED_BYTES {
        return Err(PeerTransportError::Protocol(
            "peer response exceeds the consensus limit".into(),
        ));
    }
    Ok(())
}

fn map_serving_lease_error(error: ServingLeaseError) -> Status {
    match error {
        ServingLeaseError::CutoverInProgress
        | ServingLeaseError::LeaderQuorumProofStale
        | ServingLeaseError::Consensus(_) => Status::unavailable(error.to_string()),
        ServingLeaseError::ClusterNotInitialized
        | ServingLeaseError::ActivePlacementUnavailable
        | ServingLeaseError::ClusterMismatch { .. }
        | ServingLeaseError::ActivePlacementMismatch { .. }
        | ServingLeaseError::GrantLifetimeTooLong { .. }
        | ServingLeaseError::RaftTermRegressed { .. }
        | ServingLeaseError::GrantArrivedAfterExpiry
        | ServingLeaseError::RequestSuperseded
        | ServingLeaseError::ClockRangeExceeded => Status::failed_precondition(error.to_string()),
    }
}

fn map_tonic_error(error: Status) -> PeerTransportError {
    match error.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            PeerTransportError::Unreachable(error.to_string())
        }
        _ => PeerTransportError::Protocol(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::RwLock;

    use openraft::Vote;
    use openraft::error::RaftError;
    use openraft::raft::{VoteRequest, VoteResponse};
    use tonic::codegen::tokio_stream::StreamExt;
    use tonic::transport::Server;
    use tonic::transport::server::TcpIncoming;

    use super::*;
    use crate::{
        CLUSTER_CONTROL_COMMAND_VERSION, CapabilityRange, Command, CommittedPeerPins,
        JoinCapabilityHash, NodeDescriptor, NodeState, PeerAddress, PeerTlsAcceptor, PeerTlsConfig,
        PeerTlsIdentity,
    };

    const CERT_ONE: &[u8] = include_bytes!("../tests/fixtures/peer-one.cert.pem");
    const KEY_ONE: &[u8] = include_bytes!("../tests/fixtures/peer-one.key.pem");
    const CERT_TWO: &[u8] = include_bytes!("../tests/fixtures/peer-two.cert.pem");
    const KEY_TWO: &[u8] = include_bytes!("../tests/fixtures/peer-two.key.pem");
    const TEST_CLUSTER_ID: ClusterId = ClusterId([9; 16]);
    const OTHER_CLUSTER_ID: ClusterId = ClusterId([8; 16]);

    #[derive(Default)]
    struct TestPins {
        entries: RwLock<BTreeMap<NodeId, CommittedPeerPins>>,
        authorized: RwLock<bool>,
        seen: Mutex<Vec<PeerRpcKind>>,
    }

    impl TestPins {
        fn insert(&self, node_id: NodeId, pin: PeerSpkiSha256) {
            self.entries.write().unwrap().insert(
                node_id,
                CommittedPeerPins {
                    current: pin,
                    overlap: None,
                },
            );
        }

        fn set_authorized(&self, authorized: bool) {
            *self.authorized.write().unwrap() = authorized;
        }

        fn take_seen(&self) -> Vec<PeerRpcKind> {
            std::mem::take(&mut *self.seen.lock().unwrap())
        }
    }

    impl CommittedPeerPinProvider for TestPins {
        fn connection_pins(&self, node_id: NodeId) -> Option<CommittedPeerPins> {
            self.entries.read().unwrap().get(&node_id).copied()
        }

        fn authorized_rpc_pins(
            &self,
            cluster_id: ClusterId,
            node_id: NodeId,
            kind: PeerRpcKind,
        ) -> Option<CommittedPeerPins> {
            self.seen.lock().unwrap().push(kind);
            if cluster_id != TEST_CLUSTER_ID || !*self.authorized.read().unwrap() {
                return None;
            }
            self.entries.read().unwrap().get(&node_id).copied()
        }
    }

    struct RunningPeer {
        address: String,
        raft: DecisionRaft,
        stop: tokio::sync::oneshot::Sender<()>,
        task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        _directory: tempfile::TempDir,
    }

    impl RunningPeer {
        async fn stop(self) {
            let _ = self.stop.send(());
            self.task.await.unwrap().unwrap();
            self.raft.shutdown().await.unwrap();
        }
    }

    fn identity(certificate: &[u8], key: &[u8]) -> Arc<PeerTlsIdentity> {
        Arc::new(PeerTlsIdentity::from_pem(certificate, key).unwrap())
    }

    async fn start_peer(pins: Arc<TestPins>) -> RunningPeer {
        let directory = tempfile::tempdir().unwrap();
        let raft = DecisionRaft::open(directory.path(), 2, 4, 64 * 1024)
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let acceptor =
            PeerTlsAcceptor::new(&identity(CERT_TWO, KEY_TWO), PeerTlsConfig::default()).unwrap();
        let incoming = TcpIncoming::from(listener).then(move |stream| {
            let acceptor = acceptor.clone();
            async move {
                let stream = stream.map_err(PeerTlsError::Io)?;
                acceptor.accept(stream).await
            }
        });
        let service = TonicRaftPeerService::new(raft.clone(), pins).into_server();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = stopped.await;
                })
                .await
        });
        RunningPeer {
            address,
            raft,
            stop,
            task,
            _directory: directory,
        }
    }

    fn transport_for(cluster_id: ClusterId, pins: Arc<TestPins>) -> TonicPeerTransport {
        let connector =
            PeerTlsConnector::new(identity(CERT_ONE, KEY_ONE), pins, PeerTlsConfig::default())
                .unwrap();
        TonicPeerTransport::new(cluster_id, NodeId(1), connector).unwrap()
    }

    fn transport(pins: Arc<TestPins>) -> TonicPeerTransport {
        transport_for(TEST_CLUSTER_ID, pins)
    }

    fn configured_pins() -> Arc<TestPins> {
        let pins = Arc::new(TestPins::default());
        pins.insert(NodeId(1), identity(CERT_ONE, KEY_ONE).spki_sha256());
        pins.insert(NodeId(2), identity(CERT_TWO, KEY_TWO).spki_sha256());
        pins.set_authorized(true);
        pins
    }

    async fn initialize_serving_state(raft: &DecisionRaft) -> ServingLeaseRequest {
        raft.ensure_one_node().await.unwrap();
        raft.wait_for_leader(std::time::Duration::from_secs(5))
            .await
            .unwrap();
        raft.submit(Command::InitializeCluster {
            cluster_id: TEST_CLUSTER_ID,
        })
        .await
        .unwrap();
        let begun = raft
            .submit(Command::BeginAddNode {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                descriptor: NodeDescriptor {
                    node_id: NodeId(2),
                    peer_address: PeerAddress("anvil-local://2".into()),
                    storage_weight_millionths: 1_000_000,
                    state: NodeState::Joining,
                    current_peer_spki_sha256: PeerSpkiSha256([2; 32]),
                    overlap_peer_spki_sha256: None,
                    join_capability_hash: Some(JoinCapabilityHash([3; 32])),
                    supported_protocol: CapabilityRange { min: 1, max: 1 },
                    supported_storage_format: CapabilityRange { min: 1, max: 1 },
                },
            })
            .await
            .unwrap();
        raft.submit(Command::CompleteMembershipTransition {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            started_log_index: begun.log_index,
        })
        .await
        .unwrap();
        ServingLeaseRequest {
            cluster_id: TEST_CLUSTER_ID,
            active_placement_log_id: raft
                .state()
                .unwrap()
                .cluster_control()
                .active_placement_log_id()
                .unwrap(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn vote_crosses_real_mtls_and_authority_is_rechecked_on_the_cached_connection() {
        let pins = configured_pins();
        let peer = start_peer(pins.clone()).await;
        let transport = transport(pins.clone());
        let node = PeerNode::new(peer.address.clone());
        let request = VoteRequest::new(Vote::new(1, 1), None);
        let encoded_request = codec::encode(&request).unwrap();
        assert_eq!(
            codec::decode::<VoteRequest<u64>>(&encoded_request).unwrap(),
            request
        );
        let response = transport
            .send(
                2,
                &node,
                PeerRpc {
                    schema_version: PEER_RPC_SCHEMA_VERSION,
                    kind: PeerRpcKind::Vote,
                    payload: encoded_request.clone(),
                },
            )
            .await
            .unwrap();
        let response: Result<VoteResponse<u64>, RaftError<u64>> = codec::decode(&response).unwrap();
        assert!(response.unwrap().vote_granted);

        pins.set_authorized(false);
        let denied = transport
            .send(
                2,
                &node,
                PeerRpc {
                    schema_version: PEER_RPC_SCHEMA_VERSION,
                    kind: PeerRpcKind::Vote,
                    payload: encoded_request,
                },
            )
            .await;
        assert!(matches!(denied, Err(PeerTransportError::Protocol(_))));
        assert_eq!(pins.take_seen(), vec![PeerRpcKind::Vote, PeerRpcKind::Vote]);
        drop(transport);
        peer.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mismatched_cluster_is_rejected_on_the_cached_mtls_connection() {
        let pins = configured_pins();
        let peer = start_peer(pins.clone()).await;
        let transport = transport(pins.clone());
        let channel = transport.channel(2, &peer.address).unwrap();
        let mut client = wire::raft_peer_client::RaftPeerClient::new(channel)
            .max_decoding_message_size(MAX_WIRE_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_WIRE_MESSAGE_BYTES);
        let authorized = client
            .vote(wire::RaftRequest {
                schema_version: WIRE_SCHEMA_VERSION,
                cluster_id: TEST_CLUSTER_ID.into_bytes().to_vec(),
                source_node_id: 1,
                payload: Vec::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(authorized.code(), tonic::Code::InvalidArgument);
        let rejected = client
            .vote(wire::RaftRequest {
                schema_version: WIRE_SCHEMA_VERSION,
                cluster_id: OTHER_CLUSTER_ID.into_bytes().to_vec(),
                source_node_id: 1,
                payload: Vec::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);

        drop(transport);
        peer.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn three_named_methods_apply_their_own_rpc_authorization_kind() {
        let pins = configured_pins();
        let peer = start_peer(pins.clone()).await;
        let transport = transport(pins.clone());
        let node = PeerNode::new(peer.address.clone());
        for kind in [
            PeerRpcKind::AppendEntries,
            PeerRpcKind::Vote,
            PeerRpcKind::InstallSnapshot,
        ] {
            let result = transport
                .send(
                    2,
                    &node,
                    PeerRpc {
                        schema_version: PEER_RPC_SCHEMA_VERSION,
                        kind,
                        payload: Vec::new(),
                    },
                )
                .await;
            assert!(matches!(result, Err(PeerTransportError::Protocol(_))));
        }
        assert_eq!(
            pins.take_seen(),
            vec![
                PeerRpcKind::AppendEntries,
                PeerRpcKind::Vote,
                PeerRpcKind::InstallSnapshot,
            ]
        );
        drop(transport);
        peer.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serving_grant_rpc_crosses_mtls_and_starts_the_fixed_cutover() {
        let pins = configured_pins();
        let peer = start_peer(pins.clone()).await;
        let request = initialize_serving_state(&peer.raft).await;
        let transport = transport(pins.clone());
        let node = PeerNode::new(peer.address.clone());

        assert!(matches!(
            transport.request_serving_lease(2, &node, request).await,
            Err(PeerTransportError::Unreachable(_))
        ));
        assert_eq!(pins.take_seen(), vec![PeerRpcKind::ServingLease]);
        drop(transport);
        peer.stop().await;
    }

    #[tokio::test]
    async fn oversized_payload_is_rejected_before_connecting() {
        let pins = configured_pins();
        let transport = transport(pins);
        let error = transport
            .send(
                2,
                &PeerNode::new("127.0.0.1:1"),
                PeerRpc {
                    schema_version: PEER_RPC_SCHEMA_VERSION,
                    kind: PeerRpcKind::Vote,
                    payload: vec![0; codec::MAX_ENCODED_BYTES + 1],
                },
            )
            .await;
        assert!(matches!(error, Err(PeerTransportError::Protocol(_))));
    }
}
