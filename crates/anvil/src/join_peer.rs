//! Bounded seed discovery and learner admission over the mandatory-mTLS peer
//! listener.
//!
//! This module deliberately does not move storage records. Activation calls a
//! fail-closed handoff gate; the concrete distributed handoff must prove typed
//! ownership transfer before the Raft membership boundary can advance.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use anvil_consensus::{
    ApplyResult, AuthenticatedPeer, CLUSTER_CONTROL_COMMAND_VERSION, ClusterId, CommittedPeerPins,
    DecisionRaft, DecisionRaftError, MAX_PEER_ADDRESS_BYTES, MembershipTransition,
    MembershipTransitionKind, NodeDescriptor, NodeId, NodeState, PeerAddress, PeerRpcKind,
    PeerSpkiSha256, PeerTlsConnector, PeerTlsError, authorize_peer_rpc,
};
use hyper_util::rt::TokioIo;
use tonic::codegen::Service;
use tonic::codegen::http::Uri;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use crate::join_bundle::hash_capability;
use crate::node_identity::{PendingJoinIdentity, PendingJoinSeed};

mod handoff;
pub(crate) use handoff::TypedAddHandoff;

pub(crate) mod wire {
    tonic::include_proto!("anvil.join_peer.v1");
}

const JOIN_PEER_SCHEMA_VERSION: u32 = 1;
const MAX_JOIN_PEER_MESSAGE_BYTES: usize = 4 * 1024;

/// Temporary trust copied from the operator bundle. It is consulted only
/// while the local Raft state has not yet installed committed descriptors.
pub(crate) struct JoinBootstrapPins {
    cluster_id: ClusterId,
    seeds: RwLock<BTreeMap<NodeId, CommittedPeerPins>>,
}

impl JoinBootstrapPins {
    pub(crate) fn new(cluster_id: ClusterId, seeds: &[PendingJoinSeed]) -> Self {
        Self {
            cluster_id,
            seeds: RwLock::new(
                seeds
                    .iter()
                    .map(|seed| {
                        (
                            seed.node_id,
                            CommittedPeerPins {
                                current: seed.current_peer_spki_sha256,
                                overlap: seed.overlap_peer_spki_sha256,
                            },
                        )
                    })
                    .collect(),
            ),
        }
    }

    pub(crate) fn connection_pins(&self, node_id: NodeId) -> Option<CommittedPeerPins> {
        self.seeds.read().ok()?.get(&node_id).copied()
    }

    pub(crate) fn authorized_catch_up_pins(
        &self,
        cluster_id: ClusterId,
        node_id: NodeId,
        kind: PeerRpcKind,
    ) -> Option<CommittedPeerPins> {
        if cluster_id != self.cluster_id
            || !matches!(
                kind,
                PeerRpcKind::AppendEntries
                    | PeerRpcKind::InstallSnapshot
                    | PeerRpcKind::JoinControl
            )
        {
            return None;
        }
        self.connection_pins(node_id)
    }

    fn install_redirect(&self, seed: &PendingJoinSeed) -> Result<(), Status> {
        if seed.node_id.0 == 0
            || seed.current_peer_spki_sha256.0 == [0; 32]
            || seed.overlap_peer_spki_sha256 == Some(seed.current_peer_spki_sha256)
            || seed
                .overlap_peer_spki_sha256
                .is_some_and(|pin| pin.0 == [0; 32])
        {
            return Err(Status::failed_precondition(
                "seed returned an invalid leader identity",
            ));
        }
        self.seeds
            .write()
            .map_err(|_| Status::internal("join bootstrap pin lock is poisoned"))?
            .insert(
                seed.node_id,
                CommittedPeerPins {
                    current: seed.current_peer_spki_sha256,
                    overlap: seed.overlap_peer_spki_sha256,
                },
            );
        Ok(())
    }

    pub(crate) fn clear(&self) {
        if let Ok(mut seeds) = self.seeds.write() {
            seeds.clear();
        }
    }
}

#[tonic::async_trait]
pub(crate) trait JoinActivationGate: Send + Sync + 'static {
    async fn ensure_handoff_complete(
        &self,
        descriptor: &NodeDescriptor,
        transition: &MembershipTransition,
    ) -> Result<JoinActivationPermit, Status>;
}

/// Keeps old-placement lease grants paused across the Raft activation append.
pub(crate) struct JoinActivationPermit {
    _lease_pause: Option<anvil_consensus::ServingLeaseGrantPause>,
    _program_quiescence: Option<crate::programs::ProgramQuiescenceGuard>,
}

impl std::fmt::Debug for JoinActivationPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JoinActivationPermit")
            .field("lease_pause", &self._lease_pause.is_some())
            .field("program_quiescence", &self._program_quiescence.is_some())
            .finish()
    }
}

impl JoinActivationPermit {
    pub(crate) fn after_handoff(
        pause: anvil_consensus::ServingLeaseGrantPause,
        program_quiescence: crate::programs::ProgramQuiescenceGuard,
    ) -> Self {
        Self {
            _lease_pause: Some(pause),
            _program_quiescence: Some(program_quiescence),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_only() -> Self {
        Self {
            _lease_pause: None,
            _program_quiescence: None,
        }
    }
}

pub(crate) struct RejectIncompleteHandoff;

#[tonic::async_trait]
impl JoinActivationGate for RejectIncompleteHandoff {
    async fn ensure_handoff_complete(
        &self,
        _descriptor: &NodeDescriptor,
        _transition: &MembershipTransition,
    ) -> Result<JoinActivationPermit, Status> {
        Err(Status::failed_precondition(
            "typed ownership handoff has not completed",
        ))
    }
}

#[derive(Clone)]
pub(crate) struct JoinPeerService {
    decisions: DecisionRaft,
    local_node_id: NodeId,
    pins: Arc<dyn anvil_consensus::CommittedPeerPinProvider>,
    activation_gate: Arc<dyn JoinActivationGate>,
}

pub(crate) type JoinPeerServer = wire::join_peer_server::JoinPeerServer<JoinPeerService>;

impl JoinPeerService {
    pub(crate) fn new(
        decisions: DecisionRaft,
        local_node_id: NodeId,
        pins: Arc<dyn anvil_consensus::CommittedPeerPinProvider>,
        activation_gate: Arc<dyn JoinActivationGate>,
    ) -> Self {
        Self {
            decisions,
            local_node_id,
            pins,
            activation_gate,
        }
    }

    pub(crate) fn into_server(self) -> JoinPeerServer {
        JoinPeerServer::new(self)
            .max_decoding_message_size(MAX_JOIN_PEER_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_JOIN_PEER_MESSAGE_BYTES)
    }

    fn authorize(
        &self,
        request: &mut Request<wire::JoinRequest>,
    ) -> Result<(AuthenticatedPeer, [u8; 32]), Status> {
        let presented_pin = request
            .extensions()
            .get::<PeerSpkiSha256>()
            .copied()
            .ok_or_else(|| Status::unauthenticated("peer mTLS identity is missing"))?;
        let envelope = request.get_ref();
        if envelope.schema_version != JOIN_PEER_SCHEMA_VERSION {
            return Err(Status::failed_precondition(format!(
                "unsupported join-peer schema {}",
                envelope.schema_version
            )));
        }
        let cluster_id = parse_cluster_id(&envelope.cluster_id)?;
        let peer = authorize_peer_rpc(
            self.pins.as_ref(),
            cluster_id,
            NodeId(envelope.source_node_id),
            PeerRpcKind::JoinControl,
            presented_pin,
        )
        .map_err(|_| Status::permission_denied("joining peer is not authorized"))?;
        let capability: [u8; 32] = envelope
            .join_capability
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("join capability must contain 32 bytes"))?;
        request.extensions_mut().insert(peer);
        Ok((peer, capability))
    }

    fn validate_admission(
        &self,
        peer: AuthenticatedPeer,
        capability: [u8; 32],
    ) -> Result<(NodeDescriptor, Option<MembershipTransition>), Status> {
        let state = self
            .decisions
            .state()
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if state.cluster_id() != Some(peer.cluster_id) {
            return Err(Status::permission_denied(
                "joining peer belongs to another cluster",
            ));
        }
        let descriptor = state
            .cluster_control()
            .nodes()
            .get(&peer.node_id)
            .cloned()
            .ok_or_else(|| Status::permission_denied("joining node is not admitted"))?;
        let transition = state.cluster_control().transition().cloned();
        validate_descriptor_admission(&descriptor, transition.as_ref(), peer, capability)?;
        Ok((descriptor, transition))
    }

    fn redirect(&self) -> Result<Option<wire::JoinResponse>, Status> {
        let Some(leader_id) = self.decisions.current_leader().map(NodeId) else {
            return Err(Status::unavailable("cluster leader is not known"));
        };
        if leader_id == self.local_node_id {
            return Ok(None);
        }
        let state = self
            .decisions
            .state()
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let descriptor = state
            .cluster_control()
            .nodes()
            .get(&leader_id)
            .ok_or_else(|| Status::unavailable("leader has no committed descriptor"))?;
        if descriptor.state != NodeState::Active {
            return Err(Status::unavailable("leader is not ACTIVE"));
        }
        Ok(Some(response(
            wire::JoinState::Redirect,
            0,
            Some(descriptor),
        )))
    }
}

fn validate_descriptor_admission(
    descriptor: &NodeDescriptor,
    transition: Option<&MembershipTransition>,
    peer: AuthenticatedPeer,
    capability: [u8; 32],
) -> Result<(), Status> {
    if descriptor.node_id != peer.node_id
        || (descriptor.current_peer_spki_sha256 != peer.spki_sha256
            && descriptor.overlap_peer_spki_sha256 != Some(peer.spki_sha256))
    {
        return Err(Status::permission_denied(
            "joining certificate does not match its descriptor",
        ));
    }
    if descriptor.state != NodeState::Joining {
        return Ok(());
    }
    if descriptor.join_capability_hash != Some(hash_capability(capability)) {
        return Err(Status::permission_denied("join capability is invalid"));
    }
    let current = transition
        .ok_or_else(|| Status::failed_precondition("joining descriptor has no ADD transition"))?;
    if current.kind != MembershipTransitionKind::Add || current.node_id != descriptor.node_id {
        return Err(Status::failed_precondition(
            "joining descriptor is not the current ADD transition",
        ));
    }
    Ok(())
}

#[tonic::async_trait]
impl wire::join_peer_server::JoinPeer for JoinPeerService {
    async fn catch_up(
        &self,
        mut request: Request<wire::JoinRequest>,
    ) -> Result<Response<wire::JoinResponse>, Status> {
        let (peer, capability) = self.authorize(&mut request)?;
        let (descriptor, transition) = self.validate_admission(peer, capability)?;
        if descriptor.state == NodeState::Active {
            return Ok(Response::new(response(wire::JoinState::Active, 0, None)));
        }
        if let Some(redirect) = self.redirect()? {
            return Ok(Response::new(redirect));
        }
        self.decisions
            .confirm_leadership()
            .await
            .map_err(map_consensus_status)?;
        let transition = transition.expect("JOINING admission validation requires a transition");
        self.decisions
            .catch_up_joining_learner(transition.started_log_index)
            .await
            .map_err(map_consensus_status)?;
        let state = self.decisions.state().map_err(map_consensus_status)?;
        let leader = state.cluster_control().nodes().get(&self.local_node_id);
        Ok(Response::new(response(
            wire::JoinState::HandoffRequired,
            transition.started_log_index,
            leader,
        )))
    }

    async fn activate(
        &self,
        mut request: Request<wire::JoinRequest>,
    ) -> Result<Response<wire::JoinResponse>, Status> {
        let (peer, capability) = self.authorize(&mut request)?;
        let (descriptor, transition) = self.validate_admission(peer, capability)?;
        if descriptor.state == NodeState::Active && transition.is_none() {
            return Ok(Response::new(response(wire::JoinState::Active, 0, None)));
        }
        if let Some(redirect) = self.redirect()? {
            return Ok(Response::new(redirect));
        }
        self.decisions
            .confirm_leadership()
            .await
            .map_err(map_consensus_status)?;
        let transition = transition.ok_or_else(|| {
            Status::failed_precondition("joining activation has no ADD transition")
        })?;
        if transition.kind != MembershipTransitionKind::Add
            || transition.node_id != descriptor.node_id
        {
            return Err(Status::failed_precondition(
                "joining activation does not match the ADD transition",
            ));
        }
        if descriptor.state == NodeState::Joining {
            let activation_permit = self
                .activation_gate
                .ensure_handoff_complete(&descriptor, &transition)
                .await?;
            let advanced = self
                .decisions
                .submit(anvil_consensus::Command::CompleteMembershipTransition {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    started_log_index: transition.started_log_index,
                })
                .await
                .map_err(map_consensus_status)?;
            if !matches!(
                advanced.result,
                ApplyResult::MembershipTransitionAdvanced(_)
            ) {
                return Err(Status::internal(
                    "ADD activation returned an unexpected result",
                ));
            }
            // The applied placement has changed, so an old request can no
            // longer match a grant. Release the pause only after that durable
            // boundary; normal issuer cutover handles the new placement.
            drop(activation_permit);
        }
        self.decisions
            .apply_fixed_voters_for_transition(transition.started_log_index)
            .await
            .map_err(map_consensus_status)?;
        let finished = self
            .decisions
            .submit(anvil_consensus::Command::CompleteMembershipTransition {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                started_log_index: transition.started_log_index,
            })
            .await
            .map_err(map_consensus_status)?;
        if !matches!(
            finished.result,
            ApplyResult::MembershipTransitionFinished { .. }
        ) {
            return Err(Status::internal(
                "ADD completion returned an unexpected result",
            ));
        }
        Ok(Response::new(response(
            wire::JoinState::Active,
            transition.started_log_index,
            None,
        )))
    }
}

#[derive(Clone)]
pub(crate) struct JoinPeerTransport {
    cluster_id: ClusterId,
    source_node_id: NodeId,
    pending: PendingJoinIdentity,
    tls: PeerTlsConnector,
    bootstrap_pins: Arc<JoinBootstrapPins>,
    channels: Arc<Mutex<BTreeMap<NodeId, (String, Channel)>>>,
}

impl JoinPeerTransport {
    pub(crate) fn new(
        cluster_id: ClusterId,
        source_node_id: NodeId,
        pending: PendingJoinIdentity,
        tls: PeerTlsConnector,
        bootstrap_pins: Arc<JoinBootstrapPins>,
    ) -> Self {
        Self {
            cluster_id,
            source_node_id,
            pending,
            tls,
            bootstrap_pins,
            channels: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) async fn catch_up(
        &self,
        maximum_time: Duration,
    ) -> Result<MembershipTransition, Status> {
        let response = self
            .discover(maximum_time, |mut client, request| async move {
                client.catch_up(request).await
            })
            .await?;
        if response.state() == wire::JoinState::Active {
            return Err(Status::already_exists("node is already ACTIVE"));
        }
        if response.state() != wire::JoinState::HandoffRequired || response.started_log_index == 0 {
            return Err(Status::failed_precondition(
                "leader returned an invalid catch-up result",
            ));
        }
        Ok(MembershipTransition {
            kind: MembershipTransitionKind::Add,
            node_id: self.source_node_id,
            started_log_index: response.started_log_index,
            target_weight_millionths: Some(self.pending.storage_weight_millionths()),
        })
    }

    pub(crate) async fn activate(&self, maximum_time: Duration) -> Result<(), Status> {
        let response = self
            .discover(maximum_time, |mut client, request| async move {
                client.activate(request).await
            })
            .await?;
        if response.state() != wire::JoinState::Active {
            return Err(Status::failed_precondition(
                "leader did not activate the joining node",
            ));
        }
        Ok(())
    }

    pub(crate) fn clear_bootstrap_pins(&self) {
        self.bootstrap_pins.clear();
    }

    async fn discover<F, Fut>(
        &self,
        maximum_time: Duration,
        operation: F,
    ) -> Result<wire::JoinResponse, Status>
    where
        F: Fn(wire::join_peer_client::JoinPeerClient<Channel>, Request<wire::JoinRequest>) -> Fut,
        Fut: Future<Output = Result<Response<wire::JoinResponse>, Status>>,
    {
        if maximum_time.is_zero() {
            return Err(Status::invalid_argument(
                "join timeout must be greater than zero",
            ));
        }
        let deadline = tokio::time::Instant::now() + maximum_time;
        let mut queue = VecDeque::from(self.pending.seeds().to_vec());
        let mut attempted = BTreeMap::<NodeId, ()>::new();
        let mut last_error = None;
        while let Some(seed) = queue.pop_front() {
            if attempted.insert(seed.node_id, ()).is_some() {
                continue;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let client = match self.client(&seed) {
                Ok(client) => client,
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };
            let mut request = Request::new(self.request());
            request.set_timeout(remaining);
            match operation(client, request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    validate_response_schema(&response)?;
                    if response.state() != wire::JoinState::Redirect {
                        return Ok(response);
                    }
                    let leader = parse_seed(response.leader.as_ref().ok_or_else(|| {
                        Status::failed_precondition("join redirect omitted the leader")
                    })?)?;
                    self.bootstrap_pins.install_redirect(&leader)?;
                    queue.push_front(leader);
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| Status::unavailable("no join seed was reachable")))
    }

    fn request(&self) -> wire::JoinRequest {
        wire::JoinRequest {
            schema_version: JOIN_PEER_SCHEMA_VERSION,
            cluster_id: self.cluster_id.into_bytes().to_vec(),
            source_node_id: self.source_node_id.0,
            join_capability: self.pending.capability().to_vec(),
        }
    }

    fn client(
        &self,
        seed: &PendingJoinSeed,
    ) -> Result<wire::join_peer_client::JoinPeerClient<Channel>, Status> {
        let channel = self.channel(seed.node_id, &seed.peer_address.0)?;
        Ok(wire::join_peer_client::JoinPeerClient::new(channel)
            .max_decoding_message_size(MAX_JOIN_PEER_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_JOIN_PEER_MESSAGE_BYTES))
    }

    fn channel(&self, target: NodeId, address: &str) -> Result<Channel, Status> {
        let mut channels = self
            .channels
            .lock()
            .map_err(|_| Status::internal("join-peer channel lock is poisoned"))?;
        if let Some((cached_address, channel)) = channels.get(&target)
            && cached_address == address
        {
            return Ok(channel.clone());
        }
        let connector = JoinChannelConnector {
            tls: self.tls.clone(),
            target,
            address: address.to_owned(),
        };
        let channel = Endpoint::from_static("http://anvil-peer.invalid")
            .connect_with_connector_lazy(connector);
        channels.insert(target, (address.to_owned(), channel.clone()));
        Ok(channel)
    }
}

#[derive(Clone)]
struct JoinChannelConnector {
    tls: PeerTlsConnector,
    target: NodeId,
    address: String,
}

impl Service<Uri> for JoinChannelConnector {
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

fn response(
    state: wire::JoinState,
    started_log_index: u64,
    leader: Option<&NodeDescriptor>,
) -> wire::JoinResponse {
    wire::JoinResponse {
        schema_version: JOIN_PEER_SCHEMA_VERSION,
        state: state as i32,
        started_log_index,
        leader: leader.map(|descriptor| wire::JoinSeed {
            node_id: descriptor.node_id.0,
            peer_address: descriptor.peer_address.0.clone(),
            current_peer_spki_sha256: descriptor.current_peer_spki_sha256.0.to_vec(),
            overlap_peer_spki_sha256: descriptor
                .overlap_peer_spki_sha256
                .map(|pin| pin.0.to_vec()),
        }),
    }
}

fn parse_seed(seed: &wire::JoinSeed) -> Result<PendingJoinSeed, Status> {
    let current_peer_spki_sha256 = parse_pin(&seed.current_peer_spki_sha256)?;
    let overlap_peer_spki_sha256 = seed
        .overlap_peer_spki_sha256
        .as_deref()
        .map(parse_pin)
        .transpose()?;
    if !(1..=1_023).contains(&seed.node_id)
        || seed.peer_address.is_empty()
        || seed.peer_address.len() > MAX_PEER_ADDRESS_BYTES
        || seed
            .peer_address
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(Status::failed_precondition(
            "join redirect contains invalid leader fields",
        ));
    }
    Ok(PendingJoinSeed {
        node_id: NodeId(seed.node_id),
        peer_address: PeerAddress(seed.peer_address.clone()),
        current_peer_spki_sha256,
        overlap_peer_spki_sha256,
    })
}

fn parse_cluster_id(bytes: &[u8]) -> Result<ClusterId, Status> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Status::invalid_argument("cluster_id must contain 16 bytes"))?;
    if bytes == [0; 16] {
        return Err(Status::invalid_argument("cluster_id must not be zero"));
    }
    Ok(ClusterId(bytes))
}

fn parse_pin(bytes: &[u8]) -> Result<PeerSpkiSha256, Status> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| Status::failed_precondition("peer pin must contain 32 bytes"))?;
    if bytes == [0; 32] {
        return Err(Status::failed_precondition("peer pin must not be zero"));
    }
    Ok(PeerSpkiSha256(bytes))
}

fn validate_response_schema(response: &wire::JoinResponse) -> Result<(), Status> {
    if response.schema_version != JOIN_PEER_SCHEMA_VERSION {
        return Err(Status::failed_precondition(format!(
            "peer returned unsupported join schema {}",
            response.schema_version
        )));
    }
    if response.state() == wire::JoinState::Unspecified {
        return Err(Status::failed_precondition(
            "peer returned an unspecified join state",
        ));
    }
    Ok(())
}

fn map_consensus_status(error: DecisionRaftError) -> Status {
    match error {
        DecisionRaftError::ForwardToLeader { .. }
        | DecisionRaftError::Unavailable(_)
        | DecisionRaftError::LeaderTimeout => Status::unavailable(error.to_string()),
        DecisionRaftError::Rejected(_)
        | DecisionRaftError::Configuration(_)
        | DecisionRaftError::InvalidNodeId => Status::failed_precondition(error.to_string()),
        DecisionRaftError::Storage(_)
        | DecisionRaftError::SnapshotTimeout
        | DecisionRaftError::StatePoisoned => Status::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use anvil_consensus::CapabilityRange;

    use super::*;

    fn seed() -> PendingJoinSeed {
        PendingJoinSeed {
            node_id: NodeId(1),
            peer_address: PeerAddress("127.0.0.1:50052".into()),
            current_peer_spki_sha256: PeerSpkiSha256([7; 32]),
            overlap_peer_spki_sha256: None,
        }
    }

    fn joining_descriptor(capability: [u8; 32]) -> NodeDescriptor {
        NodeDescriptor {
            node_id: NodeId(2),
            peer_address: PeerAddress("127.0.0.1:50062".into()),
            storage_weight_millionths: 1_000_000,
            state: NodeState::Joining,
            current_peer_spki_sha256: PeerSpkiSha256([8; 32]),
            overlap_peer_spki_sha256: None,
            join_capability_hash: Some(hash_capability(capability)),
            supported_protocol: CapabilityRange { min: 1, max: 1 },
            supported_storage_format: CapabilityRange { min: 1, max: 1 },
        }
    }

    #[test]
    fn admission_rejects_wrong_capability_pin_and_transition() {
        let capability = [3; 32];
        let descriptor = joining_descriptor(capability);
        let transition = MembershipTransition {
            kind: MembershipTransitionKind::Add,
            node_id: NodeId(2),
            started_log_index: 7,
            target_weight_millionths: Some(1_000_000),
        };
        let peer = AuthenticatedPeer {
            cluster_id: ClusterId([4; 16]),
            node_id: NodeId(2),
            spki_sha256: PeerSpkiSha256([8; 32]),
        };
        assert!(
            validate_descriptor_admission(&descriptor, Some(&transition), peer, capability).is_ok()
        );
        assert_eq!(
            validate_descriptor_admission(&descriptor, Some(&transition), peer, [4; 32])
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let wrong_pin = AuthenticatedPeer {
            spki_sha256: PeerSpkiSha256([9; 32]),
            ..peer
        };
        assert_eq!(
            validate_descriptor_admission(&descriptor, Some(&transition), wrong_pin, capability)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let wrong_transition = MembershipTransition {
            node_id: NodeId(3),
            ..transition
        };
        assert_eq!(
            validate_descriptor_admission(&descriptor, Some(&wrong_transition), peer, capability)
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[test]
    fn bootstrap_pins_are_narrow_and_revocable() {
        let cluster_id = ClusterId([4; 16]);
        let pins = JoinBootstrapPins::new(cluster_id, &[seed()]);
        assert_eq!(
            pins.connection_pins(NodeId(1)).unwrap().current,
            PeerSpkiSha256([7; 32])
        );
        for kind in [
            PeerRpcKind::AppendEntries,
            PeerRpcKind::InstallSnapshot,
            PeerRpcKind::JoinControl,
        ] {
            assert!(
                pins.authorized_catch_up_pins(cluster_id, NodeId(1), kind)
                    .is_some()
            );
        }
        for kind in [
            PeerRpcKind::Vote,
            PeerRpcKind::ServingLease,
            PeerRpcKind::DataPlane,
            PeerRpcKind::StateTransfer,
        ] {
            assert!(
                pins.authorized_catch_up_pins(cluster_id, NodeId(1), kind)
                    .is_none()
            );
        }
        assert!(
            pins.authorized_catch_up_pins(ClusterId([5; 16]), NodeId(1), PeerRpcKind::JoinControl)
                .is_none()
        );
        pins.clear();
        assert!(pins.connection_pins(NodeId(1)).is_none());
    }

    #[test]
    fn redirected_leader_is_bounded_before_trust_is_installed() {
        let invalid = wire::JoinSeed {
            node_id: 2,
            peer_address: "leader host:50052".into(),
            current_peer_spki_sha256: vec![8; 32],
            overlap_peer_spki_sha256: None,
        };
        assert!(parse_seed(&invalid).is_err());

        let valid = wire::JoinSeed {
            node_id: 2,
            peer_address: "leader.internal:50052".into(),
            current_peer_spki_sha256: vec![8; 32],
            overlap_peer_spki_sha256: Some(vec![9; 32]),
        };
        let parsed = parse_seed(&valid).unwrap();
        assert_eq!(parsed.node_id, NodeId(2));
        assert_eq!(parsed.current_peer_spki_sha256, PeerSpkiSha256([8; 32]));
        assert_eq!(
            parsed.overlap_peer_spki_sha256,
            Some(PeerSpkiSha256([9; 32]))
        );
    }

    #[tokio::test]
    async fn activation_gate_fails_closed_until_typed_handoff_exists() {
        let descriptor = joining_descriptor([9; 32]);
        let transition = MembershipTransition {
            kind: MembershipTransitionKind::Add,
            node_id: NodeId(2),
            started_log_index: 7,
            target_weight_millionths: Some(1_000_000),
        };
        let error = RejectIncompleteHandoff
            .ensure_handoff_complete(&descriptor, &transition)
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }
}
