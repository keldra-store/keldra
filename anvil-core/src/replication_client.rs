//! Persistent client-side replication streams with application-level ACKs.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request,
    metadata::AsciiMetadataValue,
    transport::{Channel, Endpoint},
};
use uuid::Uuid;

use crate::{
    anvil_api::{
        ReplicationAckStatus, ReplicationApplicationAck, ReplicationDataFrame,
        ReplicationReadRequest, ReplicationSessionOpen, ReplicationStreamRequest,
        ReplicationStreamResponse, ReplicationTransferKind, ReplicationTransferWatermark,
        replication_service_client::ReplicationServiceClient, replication_stream_request,
        replication_stream_response,
    },
    bundle_replication::{BundleTarget, BundleTargetStream},
    mvcc_transaction::{BundleIdentity, NodeIncarnation},
    replication::{AckStatus, ReplicationAck, ReplicationFrame},
    shard_placement::{ShardTarget, ShardTargetStream},
    streaming_erasure::EncodedShard,
};

const NODE_TOKEN_HEADER: &str = "x-anvil-node-token";

fn require_secure_endpoint(endpoint: &str, allow_insecure_for_tests: bool) -> Result<()> {
    if endpoint.starts_with("https://") || allow_insecure_for_tests {
        return Ok(());
    }
    bail!("replication endpoint requires TLS (https://)");
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationPeer {
    pub cluster_id: String,
    pub node: NodeIncarnation,
    pub endpoint: String,
}

#[derive(Clone, Copy, Debug)]
pub struct ReplicationStreamOptions {
    pub operation_timeout: Duration,
    pub frame_bytes: usize,
    /// Number of replacement sessions after the initial session fails.
    pub reconnect_attempts: usize,
    pub queue_capacity: usize,
    pub heartbeat_interval: Duration,
    pub progress_timeout: Duration,
    pub allow_insecure_transport_for_tests: bool,
}

impl Default for ReplicationStreamOptions {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(10),
            frame_bytes: 256 * 1024,
            reconnect_attempts: 2,
            queue_capacity: 8,
            heartbeat_interval: Duration::from_secs(5),
            progress_timeout: Duration::from_secs(10),
            allow_insecure_transport_for_tests: false,
        }
    }
}

struct ConnectedStream {
    session_id: Uuid,
    next_sequence: u64,
    output: mpsc::Sender<ReplicationStreamRequest>,
    input: tonic::Streaming<ReplicationStreamResponse>,
    last_progress: tokio::time::Instant,
    last_acknowledged_sequence: u64,
}

struct PeerState {
    channel: Channel,
    session: Option<ConnectedStream>,
}

/// One manager may be shared by bundle and shard replication. Operations for a
/// target are serialized through its bounded stream, while different targets
/// progress independently.
#[derive(Clone)]
pub struct TonicReplicationStreamManager {
    cluster_id: Arc<str>,
    local_node: NodeIncarnation,
    node_token: Arc<str>,
    options: ReplicationStreamOptions,
    peers: Arc<BTreeMap<(String, NodeIncarnation), Arc<AsyncMutex<PeerState>>>>,
}

impl std::fmt::Debug for TonicReplicationStreamManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TonicReplicationStreamManager")
            .field("cluster_id", &self.cluster_id)
            .field("local_node", &self.local_node)
            .field("peer_count", &self.peers.len())
            .finish_non_exhaustive()
    }
}

impl TonicReplicationStreamManager {
    pub fn new(
        cluster_id: impl Into<Arc<str>>,
        local_node: NodeIncarnation,
        node_token: impl Into<Arc<str>>,
        peers: impl IntoIterator<Item = ReplicationPeer>,
        options: ReplicationStreamOptions,
    ) -> Result<Self> {
        let cluster_id = cluster_id.into();
        if cluster_id.trim().is_empty()
            || local_node.node_id.trim().is_empty()
            || local_node.incarnation == 0
        {
            bail!("replication client requires a valid local node incarnation");
        }
        if options.operation_timeout.is_zero()
            || options.frame_bytes == 0
            || options.queue_capacity == 0
            || options.heartbeat_interval.is_zero()
            || options.progress_timeout.is_zero()
        {
            bail!("replication stream timeouts, frame size, and queue capacity must be non-zero");
        }
        let mut states = BTreeMap::new();
        for peer in peers {
            if peer.cluster_id.trim().is_empty()
                || peer.node.node_id.trim().is_empty()
                || peer.node.incarnation == 0
                || peer.endpoint.trim().is_empty()
            {
                bail!("replication peers require a valid node incarnation and endpoint");
            }
            require_secure_endpoint(&peer.endpoint, options.allow_insecure_transport_for_tests)?;
            let channel = Endpoint::from_shared(peer.endpoint.clone())?
                .connect_timeout(options.operation_timeout)
                .connect_lazy();
            if states
                .insert(
                    (peer.cluster_id, peer.node),
                    Arc::new(AsyncMutex::new(PeerState {
                        channel,
                        session: None,
                    })),
                )
                .is_some()
            {
                bail!("duplicate replication peer node incarnation");
            }
        }
        Ok(Self {
            cluster_id,
            local_node,
            node_token: node_token.into(),
            options,
            peers: Arc::new(states),
        })
    }

    /// Replaces the transport endpoint for one configured peer incarnation.
    ///
    /// The peer mutex serializes this topology transition with transfers. Any
    /// authenticated stream bound to the previous channel is discarded, so
    /// the next operation performs a fresh node-to-node authentication
    /// handshake against the replacement endpoint.
    pub async fn replace_peer_endpoint(
        &self,
        cluster_id: &str,
        node: &NodeIncarnation,
        endpoint: impl Into<String>,
    ) -> Result<()> {
        if cluster_id != &*self.cluster_id {
            bail!("cross-cluster replication endpoint replacement is forbidden");
        }
        let endpoint = endpoint.into();
        if endpoint.trim().is_empty() {
            bail!("replacement replication endpoint must be non-empty");
        }
        require_secure_endpoint(&endpoint, self.options.allow_insecure_transport_for_tests)?;
        let channel = Endpoint::from_shared(endpoint)?
            .connect_timeout(self.options.operation_timeout)
            .connect_lazy();
        let peer = self
            .peers
            .get(&(cluster_id.to_string(), node.clone()))
            .with_context(|| format!("no replication peer for {cluster_id}/{node:?}"))?
            .clone();
        let mut peer = peer.lock().await;
        peer.channel = channel;
        peer.session = None;
        Ok(())
    }

    async fn transfer(
        &self,
        target_cluster_id: &str,
        target: &NodeIncarnation,
        partition: String,
        kind: ReplicationTransferKind,
        transfer_id: Uuid,
        bytes: &[u8],
        final_hash: [u8; 32],
    ) -> Result<ReplicationAck> {
        #[cfg(feature = "test-cluster-transport-faults")]
        {
            if !crate::cluster_transport_fault::link_available(
                target_cluster_id,
                &self.local_node.node_id,
                &target.node_id,
            ) {
                bail!("replication link is partitioned by fixture");
            }
        }
        let peer = self
            .peers
            .get(&(target_cluster_id.to_string(), target.clone()))
            .with_context(|| format!("no replication endpoint for {target_cluster_id}/{target:?}"))?
            .clone();
        if target_cluster_id != &*self.cluster_id {
            bail!("cross-cluster replication cannot provide transaction durability");
        }
        let mut peer = peer.lock().await;
        let mut last_error = None;
        for _ in 0..=self.options.reconnect_attempts {
            if peer.session.is_none() {
                match self.connect(&peer.channel).await {
                    Ok(session) => peer.session = Some(session),
                    Err(error) => {
                        last_error = Some(error);
                        continue;
                    }
                }
            }
            if let Err(error) = self
                .heartbeat_if_due(peer.session.as_mut().expect("session established"))
                .await
            {
                crate::perf::record_replication_heartbeat("expired");
                crate::perf::record_replication_reconnect("heartbeat_expired");
                tracing::warn!(
                    cluster_id = target_cluster_id,
                    node_id = %target.node_id,
                    incarnation = target.incarnation,
                    %error,
                    "replication heartbeat progress expired; reconnecting"
                );
                last_error = Some(error);
                peer.session = None;
                continue;
            }
            let result = self
                .transfer_on_session(
                    peer.session.as_mut().expect("session established"),
                    &partition,
                    kind,
                    transfer_id,
                    bytes,
                    final_hash,
                )
                .await;
            match result {
                Ok(ack) => return Ok(ack),
                Err(error) => {
                    crate::perf::record_replication_reconnect("transfer_progress_expired");
                    tracing::warn!(
                        cluster_id = target_cluster_id,
                        node_id = %target.node_id,
                        incarnation = target.incarnation,
                        %error,
                        "replication transfer lost ACK progress; reconnecting from durable watermark"
                    );
                    last_error = Some(error);
                    peer.session = None;
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("replication transfer failed")))
    }

    pub async fn read_complete_transfer(
        &self,
        cluster_id: &str,
        target: &NodeIncarnation,
        transfer_id: Uuid,
        expected_length: u64,
        expected_hash: [u8; 32],
    ) -> Result<Vec<u8>> {
        let peer = self
            .peers
            .get(&(cluster_id.to_string(), target.clone()))
            .with_context(|| format!("no replication endpoint for {cluster_id}/{target:?}"))?
            .clone();
        if cluster_id != &*self.cluster_id {
            bail!("cross-cluster replication reads require a separate replication boundary");
        }
        let mut peer = peer.lock().await;
        let mut last_error = None;
        for _ in 0..=self.options.reconnect_attempts {
            if peer.session.is_none() {
                match self.connect(&peer.channel).await {
                    Ok(session) => peer.session = Some(session),
                    Err(error) => {
                        last_error = Some(error);
                        continue;
                    }
                }
            }
            if let Err(error) = self
                .heartbeat_if_due(peer.session.as_mut().expect("session established"))
                .await
            {
                crate::perf::record_replication_heartbeat("expired");
                crate::perf::record_replication_reconnect("heartbeat_expired");
                tracing::warn!(
                    cluster_id,
                    node_id = %target.node_id,
                    incarnation = target.incarnation,
                    %error,
                    "replication read heartbeat expired; reconnecting"
                );
                last_error = Some(error);
                peer.session = None;
                continue;
            }
            match self
                .read_on_session(
                    peer.session.as_mut().expect("session established"),
                    transfer_id,
                    expected_length,
                    expected_hash,
                )
                .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(error) => {
                    crate::perf::record_replication_reconnect("read_progress_expired");
                    tracing::warn!(
                        cluster_id,
                        node_id = %target.node_id,
                        incarnation = target.incarnation,
                        %error,
                        "replication read lost progress; reconnecting"
                    );
                    last_error = Some(error);
                    peer.session = None;
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("replication read failed")))
    }

    async fn read_on_session(
        &self,
        session: &mut ConnectedStream,
        transfer_id: Uuid,
        expected_length: u64,
        expected_hash: [u8; 32],
    ) -> Result<Vec<u8>> {
        let capacity =
            usize::try_from(expected_length).context("replication read exceeds address space")?;
        let mut bytes = Vec::with_capacity(capacity);
        loop {
            send_timeout(
                self.options.operation_timeout,
                &session.output,
                request(replication_stream_request::Message::Read(
                    ReplicationReadRequest {
                        transfer_id: transfer_id.to_string(),
                        offset: bytes.len() as u64,
                        max_bytes: self.options.frame_bytes as u64,
                    },
                )),
            )
            .await?;
            let response = timeout_message(self.options.progress_timeout, &mut session.input)
                .await?
                .context("replication stream ended while awaiting read chunk")?;
            let Some(replication_stream_response::Message::Read(chunk)) = response.message else {
                bail!("unexpected replication response while awaiting read chunk");
            };
            if chunk.transfer_id != transfer_id.to_string()
                || chunk.offset != bytes.len() as u64
                || chunk.total_length != expected_length
                || parse_optional_hash(&chunk.completed_hash)? != Some(expected_hash)
                || chunk.payload_checksum != ReplicationFrame::checksum(&chunk.payload)
            {
                bail!("replication read chunk failed immutable metadata verification");
            }
            bytes.extend_from_slice(&chunk.payload);
            if bytes.len() as u64 > expected_length {
                bail!("replication read exceeded immutable length");
            }
            if chunk.finish {
                if bytes.len() as u64 != expected_length {
                    bail!("replication read finished before immutable length");
                }
                return Ok(bytes);
            }
            if chunk.payload.is_empty() {
                bail!("replication read made no progress");
            }
        }
    }

    async fn connect(&self, channel: &Channel) -> Result<ConnectedStream> {
        let (output, receiver) = mpsc::channel(self.options.queue_capacity);
        output
            .send(request(replication_stream_request::Message::Open(
                ReplicationSessionOpen {
                    node_id: self.local_node.node_id.clone(),
                    node_incarnation: self.local_node.incarnation,
                    requested_session_id: Uuid::new_v4().to_string(),
                    cluster_id: self.cluster_id.to_string(),
                },
            )))
            .await
            .context("replication request stream closed before session open")?;
        let mut request = Request::new(ReceiverStream::new(receiver));
        let token: AsciiMetadataValue = self
            .node_token
            .parse()
            .context("invalid replication node token")?;
        request.metadata_mut().insert(NODE_TOKEN_HEADER, token);
        let response = tokio::time::timeout(
            self.options.progress_timeout,
            ReplicationServiceClient::new(channel.clone()).replicate(request),
        )
        .await
        .context("replication session open timed out")??;
        let mut input = response.into_inner();
        let first = timeout_message(self.options.operation_timeout, &mut input)
            .await?
            .context("replication peer ended stream before accepting session")?;
        let Some(replication_stream_response::Message::Accepted(accepted)) = first.message else {
            bail!("replication peer did not accept session");
        };
        let session_id =
            Uuid::parse_str(&accepted.session_id).context("peer returned invalid session ID")?;
        if accepted.peer_node_id != self.local_node.node_id
            || accepted.peer_node_incarnation != self.local_node.incarnation
        {
            bail!("replication peer accepted session for a different authenticated node");
        }
        crate::perf::record_replication_stream_connected(true);
        tracing::info!(
            operation = "replication.stream",
            session_id = %session_id,
            peer_node_id = %accepted.peer_node_id,
            peer_incarnation = accepted.peer_node_incarnation,
            "persistent replication stream authenticated"
        );
        Ok(ConnectedStream {
            session_id,
            next_sequence: 1,
            output,
            input,
            last_progress: tokio::time::Instant::now(),
            last_acknowledged_sequence: 0,
        })
    }

    async fn heartbeat_if_due(&self, session: &mut ConnectedStream) -> Result<()> {
        if session.last_progress.elapsed() < self.options.heartbeat_interval {
            return Ok(());
        }
        let sequence = session.next_sequence;
        session.next_sequence = session
            .next_sequence
            .checked_add(1)
            .context("replication heartbeat sequence exhausted")?;
        send_timeout(
            self.options.progress_timeout,
            &session.output,
            request(replication_stream_request::Message::Heartbeat(
                crate::anvil_api::ReplicationHeartbeat {
                    session_id: session.session_id.to_string(),
                    sequence,
                    last_acknowledged_sequence: session.last_acknowledged_sequence,
                },
            )),
        )
        .await?;
        let response = timeout_message(self.options.progress_timeout, &mut session.input)
            .await?
            .context("replication stream ended while awaiting heartbeat")?;
        let Some(replication_stream_response::Message::Heartbeat(heartbeat)) = response.message
        else {
            bail!("unexpected replication response while awaiting heartbeat");
        };
        if heartbeat.session_id != session.session_id.to_string()
            || heartbeat.sequence != sequence
            || heartbeat.last_acknowledged_sequence != session.last_acknowledged_sequence
        {
            bail!("replication heartbeat response does not match session progress");
        }
        session.last_progress = tokio::time::Instant::now();
        crate::perf::record_replication_heartbeat("ok");
        tracing::debug!(
            session_id = %session.session_id,
            sequence,
            last_acknowledged_sequence = session.last_acknowledged_sequence,
            "replication heartbeat acknowledged"
        );
        Ok(())
    }

    async fn transfer_on_session(
        &self,
        session: &mut ConnectedStream,
        partition: &str,
        kind: ReplicationTransferKind,
        transfer_id: Uuid,
        bytes: &[u8],
        final_hash: [u8; 32],
    ) -> Result<ReplicationAck> {
        send_timeout(
            self.options.operation_timeout,
            &session.output,
            request(replication_stream_request::Message::Watermark(
                ReplicationTransferWatermark {
                    transfer_id: transfer_id.to_string(),
                    persisted_through: 0,
                    complete: false,
                    completed_hash: Vec::new(),
                },
            )),
        )
        .await?;
        let watermark = receive_watermark(
            self.options.progress_timeout,
            &mut session.input,
            transfer_id,
        )
        .await?;
        if watermark.persisted_through > bytes.len() as u64 {
            bail!("peer watermark exceeds immutable transfer length");
        }
        if watermark.persisted_through > 0 {
            crate::perf::record_replication_resume_bytes(watermark.persisted_through);
        }
        if watermark.complete {
            let completed_hash = parse_optional_hash(&watermark.completed_hash)?
                .context("complete watermark omitted completed hash")?;
            if watermark.persisted_through != bytes.len() as u64 || completed_hash != final_hash {
                bail!("peer complete watermark differs from immutable transfer");
            }
            return Ok(ReplicationAck {
                session_id: session.session_id,
                acknowledged_sequence: session.next_sequence.saturating_sub(1),
                transfer_id,
                persisted_through: watermark.persisted_through,
                completed_hash: Some(completed_hash),
                status: AckStatus::Complete,
            });
        }

        let mut offset = usize::try_from(watermark.persisted_through)
            .context("peer watermark does not fit client address space")?;
        if offset > 0 && bytes.get(..offset).is_none() {
            bail!("peer watermark is outside transfer bytes");
        }
        // An empty transfer still needs a finishing frame.
        loop {
            let end = offset
                .saturating_add(self.options.frame_bytes)
                .min(bytes.len());
            let payload = bytes[offset..end].to_vec();
            let finish = end == bytes.len();
            let sequence = session.next_sequence;
            session.next_sequence = session
                .next_sequence
                .checked_add(1)
                .context("replication session sequence exhausted")?;
            let ack_started_at = std::time::Instant::now();
            crate::perf::record_replication_unacked_bytes(
                if kind == ReplicationTransferKind::TransactionBundle {
                    "bundle"
                } else {
                    "shard"
                },
                payload.len() as u64,
            );
            send_timeout(
                self.options.operation_timeout,
                &session.output,
                request(replication_stream_request::Message::Frame(
                    ReplicationDataFrame {
                        session_id: session.session_id.to_string(),
                        sequence,
                        partition: partition.to_string(),
                        transfer_id: transfer_id.to_string(),
                        kind: kind as i32,
                        offset: offset as u64,
                        payload_checksum: ReplicationFrame::checksum(&payload).to_vec(),
                        payload,
                        total_length: bytes.len() as u64,
                        final_hash: final_hash.to_vec(),
                        finish,
                        cluster_id: self.cluster_id.to_string(),
                    },
                )),
            )
            .await?;
            let ack = receive_ack(
                self.options.progress_timeout,
                &mut session.input,
                session.session_id,
            )
            .await?;
            crate::perf::record_replication_ack_latency("received", ack_started_at.elapsed());
            crate::perf::record_replication_unacked_bytes(
                if kind == ReplicationTransferKind::TransactionBundle {
                    "bundle"
                } else {
                    "shard"
                },
                0,
            );
            tracing::debug!(
                operation = "replication.persist_ack",
                session_id = %session.session_id,
                transfer_id = %transfer_id,
                sequence,
                persisted_through = ack.persisted_through,
                "replication frame received durable application ACK"
            );
            if ack.transfer_id != transfer_id || ack.acknowledged_sequence != sequence {
                bail!("replication ACK does not correlate to the outstanding frame");
            }
            session.last_progress = tokio::time::Instant::now();
            session.last_acknowledged_sequence = sequence;
            if ack.persisted_through < end as u64 || ack.persisted_through > bytes.len() as u64 {
                bail!("replication ACK contains an invalid durable watermark");
            }
            if ack.status == AckStatus::Rejected {
                bail!("replication peer rejected transfer");
            }
            if finish {
                if ack.status != AckStatus::Complete
                    || ack.persisted_through != bytes.len() as u64
                    || ack.completed_hash != Some(final_hash)
                {
                    bail!("final replication frame did not receive matching Complete ACK");
                }
                return Ok(ack);
            }
            offset = usize::try_from(ack.persisted_through)
                .context("ACK watermark does not fit client address space")?;
        }
    }
}

#[async_trait]
impl BundleTargetStream for TonicReplicationStreamManager {
    async fn send_bundle(
        &self,
        target: &BundleTarget,
        identity: &BundleIdentity,
        bytes: &[u8],
    ) -> Result<ReplicationAck> {
        let final_hash = parse_identity_hash(&identity.hash)?;
        if identity.length != bytes.len() as u64 {
            bail!("bundle length differs from immutable identity");
        }
        let transfer_id = deterministic_transfer_id(
            ReplicationTransferKind::TransactionBundle,
            identity.hash.as_bytes(),
            final_hash,
            identity.length,
        );
        self.transfer(
            &target.cluster_id,
            &target.node,
            format!("bundle/{}", identity.hash),
            ReplicationTransferKind::TransactionBundle,
            transfer_id,
            bytes,
            final_hash,
        )
        .await
    }
}

pub fn bundle_transfer_id(identity: &BundleIdentity) -> Result<Uuid> {
    let final_hash = parse_identity_hash(&identity.hash)?;
    Ok(deterministic_transfer_id(
        ReplicationTransferKind::TransactionBundle,
        identity.hash.as_bytes(),
        final_hash,
        identity.length,
    ))
}

pub fn object_shard_transfer_id(
    object_identity: Uuid,
    encoding_generation: u64,
    stripe_ordinal: u64,
    shard_ordinal: u16,
    payload_hash: [u8; 32],
    payload_length: u64,
) -> Uuid {
    let partition = format!(
        "object/{object_identity}/generation/{encoding_generation}/stripe/{stripe_ordinal}/shard/{shard_ordinal}"
    );
    deterministic_transfer_id(
        ReplicationTransferKind::ObjectShard,
        partition.as_bytes(),
        payload_hash,
        payload_length,
    )
}

#[async_trait]
impl ShardTargetStream for TonicReplicationStreamManager {
    async fn send(&self, target: &ShardTarget, shard: &EncodedShard<'_>) -> Result<ReplicationAck> {
        let partition = format!(
            "object/{}/generation/{}/stripe/{}/shard/{}",
            shard.object_identity,
            shard.encoding_generation,
            shard.stripe_ordinal,
            shard.shard_ordinal
        );
        let transfer_id = object_shard_transfer_id(
            shard.object_identity,
            shard.encoding_generation,
            shard.stripe_ordinal,
            shard.shard_ordinal,
            shard.payload_hash,
            shard.payload.len() as u64,
        );
        self.transfer(
            &target.cluster_id,
            &target.node,
            partition,
            ReplicationTransferKind::ObjectShard,
            transfer_id,
            shard.payload,
            shard.payload_hash,
        )
        .await
    }
}

fn deterministic_transfer_id(
    kind: ReplicationTransferKind,
    identity: &[u8],
    hash: [u8; 32],
    length: u64,
) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.replication.transfer.v1");
    hasher.update(&(kind as i32).to_be_bytes());
    hasher.update(&(identity.len() as u64).to_be_bytes());
    hasher.update(identity);
    hasher.update(&hash);
    hasher.update(&length.to_be_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

fn parse_identity_hash(value: &str) -> Result<[u8; 32]> {
    hex::decode(
        value
            .strip_prefix("sha256:")
            .context("bundle identity must use sha256")?,
    )?
    .try_into()
    .map_err(|_| anyhow!("bundle identity hash must contain 32 bytes"))
}

fn request(message: replication_stream_request::Message) -> ReplicationStreamRequest {
    ReplicationStreamRequest {
        message: Some(message),
    }
}

async fn send_timeout(
    timeout: Duration,
    output: &mpsc::Sender<ReplicationStreamRequest>,
    message: ReplicationStreamRequest,
) -> Result<()> {
    tokio::time::timeout(timeout, output.send(message))
        .await
        .context("replication stream backpressure timed out")?
        .context("replication request stream closed")
}

async fn timeout_message(
    timeout: Duration,
    input: &mut tonic::Streaming<ReplicationStreamResponse>,
) -> Result<Option<ReplicationStreamResponse>> {
    tokio::time::timeout(timeout, input.message())
        .await
        .context("replication response timed out")?
        .map_err(Into::into)
}

async fn receive_watermark(
    timeout: Duration,
    input: &mut tonic::Streaming<ReplicationStreamResponse>,
    transfer_id: Uuid,
) -> Result<ReplicationTransferWatermark> {
    let response = timeout_message(timeout, input)
        .await?
        .context("replication stream ended while awaiting watermark")?;
    let Some(replication_stream_response::Message::Watermark(watermark)) = response.message else {
        bail!("unexpected replication response while awaiting watermark");
    };
    if watermark.transfer_id != transfer_id.to_string() {
        bail!("replication watermark does not correlate to requested transfer");
    }
    Ok(watermark)
}

async fn receive_ack(
    timeout: Duration,
    input: &mut tonic::Streaming<ReplicationStreamResponse>,
    session_id: Uuid,
) -> Result<ReplicationAck> {
    let response = timeout_message(timeout, input)
        .await?
        .context("replication stream ended while awaiting application ACK")?;
    let Some(replication_stream_response::Message::Ack(ack)) = response.message else {
        bail!("unexpected replication response while awaiting application ACK");
    };
    decode_ack(ack, session_id)
}

fn decode_ack(ack: ReplicationApplicationAck, session_id: Uuid) -> Result<ReplicationAck> {
    if ack.session_id != session_id.to_string() {
        bail!("replication ACK belongs to another connection session");
    }
    let status = match ReplicationAckStatus::try_from(ack.status) {
        Ok(ReplicationAckStatus::Received) => AckStatus::Received,
        Ok(ReplicationAckStatus::Persisted) => AckStatus::Persisted,
        Ok(ReplicationAckStatus::Complete) => AckStatus::Complete,
        Ok(ReplicationAckStatus::Applied) => AckStatus::Applied,
        Ok(ReplicationAckStatus::Rejected) => AckStatus::Rejected,
        _ => bail!("replication ACK contains invalid status"),
    };
    if status == AckStatus::Rejected && !ack.rejection_reason.is_empty() {
        bail!(
            "replication peer rejected transfer: {}",
            ack.rejection_reason
        );
    }
    Ok(ReplicationAck {
        session_id,
        acknowledged_sequence: ack.acknowledged_sequence,
        transfer_id: Uuid::parse_str(&ack.transfer_id)
            .context("replication ACK contains invalid transfer ID")?,
        persisted_through: ack.persisted_through,
        completed_hash: parse_optional_hash(&ack.completed_hash)?,
        status,
    })
}

fn parse_optional_hash(bytes: &[u8]) -> Result<Option<[u8; 32]>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    Ok(Some(bytes.try_into().map_err(|_| {
        anyhow!("replication hash must contain 32 bytes")
    })?))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use sha2::{Digest, Sha256};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Status, metadata::MetadataMap, transport::Server};

    use super::*;
    use crate::{
        mvcc_fault_injection::{FrameAction, FrameFaultPlan},
        replication::AuthenticatedPeer,
        services::replication::{ReplicationConnectionAuthorizer, ReplicationServiceImpl},
    };

    struct Authorizer {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ReplicationConnectionAuthorizer for Authorizer {
        async fn authorize(
            &self,
            metadata: &MetadataMap,
            open: &ReplicationSessionOpen,
        ) -> std::result::Result<AuthenticatedPeer, Status> {
            if metadata
                .get(NODE_TOKEN_HEADER)
                .and_then(|value| value.to_str().ok())
                != Some("test-token")
            {
                return Err(Status::unauthenticated("missing token"));
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            AuthenticatedPeer::new(open.node_id.clone(), open.node_incarnation)
                .map_err(|error| Status::permission_denied(error.to_string()))
        }
    }

    fn bundle_identity(bytes: &[u8]) -> BundleIdentity {
        let mut hash = Sha256::new();
        hash.update(b"anvil.mvcc.transaction-bundle.v1");
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
        BundleIdentity {
            hash: format!("sha256:{}", hex::encode(hash.finalize())),
            length: bytes.len() as u64,
        }
    }

    #[tokio::test]
    async fn reuses_authenticated_stream_and_complete_watermark() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let directory = tempfile::tempdir().unwrap();
        let service = ReplicationServiceImpl::open(
            Authorizer {
                calls: calls.clone(),
            },
            directory.path(),
        )
        .unwrap();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(
                    crate::anvil_api::replication_service_server::ReplicationServiceServer::new(
                        service,
                    ),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let local = NodeIncarnation {
            node_id: "node-a".into(),
            incarnation: 1,
        };
        let remote = NodeIncarnation {
            node_id: "node-b".into(),
            incarnation: 1,
        };
        let manager = TonicReplicationStreamManager::new(
            "cluster-a",
            local,
            "test-token",
            [ReplicationPeer {
                cluster_id: "cluster-a".into(),
                node: remote.clone(),
                endpoint: format!("http://{address}"),
            }],
            ReplicationStreamOptions {
                operation_timeout: Duration::from_secs(2),
                frame_bytes: 3,
                reconnect_attempts: 1,
                queue_capacity: 1,
                heartbeat_interval: Duration::from_millis(25),
                progress_timeout: Duration::from_secs(2),
                allow_insecure_transport_for_tests: true,
            },
        )
        .unwrap();
        let target = BundleTarget {
            cluster_id: "cluster-a".into(),
            node: remote,
            failure_domain: "zone-b".into(),
            voter: true,
        };
        let bytes = b"persistent-stream-bundle";
        let identity = bundle_identity(bytes);
        let first = manager
            .send_bundle(&target, &identity, bytes)
            .await
            .unwrap();
        assert_eq!(first.status, AckStatus::Complete);
        assert_eq!(
            manager
                .read_complete_transfer(
                    "cluster-a",
                    &target.node,
                    first.transfer_id,
                    identity.length,
                    parse_identity_hash(&identity.hash).unwrap(),
                )
                .await
                .unwrap(),
            bytes
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        let second = manager
            .send_bundle(&target, &identity, bytes)
            .await
            .unwrap();
        assert_eq!(second.status, AckStatus::Complete);
        assert_eq!(second.completed_hash, first.completed_hash);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test]
    async fn dropped_frame_and_complete_ack_reconnect_at_persisted_watermark() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let directory = tempfile::tempdir().unwrap();
        let service = ReplicationServiceImpl::open(
            Authorizer {
                calls: calls.clone(),
            },
            directory.path(),
        )
        .unwrap()
        .with_frame_fault_plan(FrameFaultPlan::new([
            FrameAction::Drop,
            FrameAction::Duplicate,
        ]))
        .with_dropped_complete_acks(1);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(
                    crate::anvil_api::replication_service_server::ReplicationServiceServer::new(
                        service,
                    ),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let remote = NodeIncarnation {
            node_id: "node-b".into(),
            incarnation: 1,
        };
        let manager = TonicReplicationStreamManager::new(
            "cluster-a",
            NodeIncarnation {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            "test-token",
            [ReplicationPeer {
                cluster_id: "cluster-a".into(),
                node: remote.clone(),
                endpoint: format!("http://{address}"),
            }],
            ReplicationStreamOptions {
                operation_timeout: Duration::from_secs(1),
                frame_bytes: 64,
                reconnect_attempts: 3,
                queue_capacity: 2,
                heartbeat_interval: Duration::from_secs(1),
                progress_timeout: Duration::from_millis(50),
                allow_insecure_transport_for_tests: true,
            },
        )
        .unwrap();
        let bytes = b"durable-watermark-resume";
        let identity = bundle_identity(bytes);
        let target = BundleTarget {
            cluster_id: "cluster-a".into(),
            node: remote,
            failure_domain: "zone-b".into(),
            voter: true,
        };
        let ack = manager
            .send_bundle(&target, &identity, bytes)
            .await
            .unwrap();

        assert_eq!(ack.status, AckStatus::Complete);
        assert_eq!(
            manager
                .read_complete_transfer(
                    "cluster-a",
                    &target.node,
                    ack.transfer_id,
                    identity.length,
                    parse_identity_hash(&identity.hash).unwrap(),
                )
                .await
                .unwrap(),
            bytes
        );
        // One reconnect follows the silently dropped frame and another follows
        // the Complete ACK dropped after durable rename.
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[tokio::test]
    async fn silent_half_open_expires_progress_and_reconnects() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let directory = tempfile::tempdir().unwrap();
        let service = ReplicationServiceImpl::open(
            Authorizer {
                calls: calls.clone(),
            },
            directory.path(),
        )
        .unwrap()
        .with_frame_fault_plan(FrameFaultPlan::new([
            FrameAction::HalfOpen,
            FrameAction::Deliver,
        ]));
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(
                    crate::anvil_api::replication_service_server::ReplicationServiceServer::new(
                        service,
                    ),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let remote = NodeIncarnation {
            node_id: "node-b".into(),
            incarnation: 1,
        };
        let manager = TonicReplicationStreamManager::new(
            "cluster-a",
            NodeIncarnation {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            "test-token",
            [ReplicationPeer {
                cluster_id: "cluster-a".into(),
                node: remote.clone(),
                endpoint: format!("http://{address}"),
            }],
            ReplicationStreamOptions {
                operation_timeout: Duration::from_secs(1),
                frame_bytes: 64,
                reconnect_attempts: 2,
                queue_capacity: 2,
                heartbeat_interval: Duration::from_secs(1),
                progress_timeout: Duration::from_millis(50),
                allow_insecure_transport_for_tests: true,
            },
        )
        .unwrap();
        let bytes = b"half-open-retry";
        let ack = manager
            .send_bundle(
                &BundleTarget {
                    cluster_id: "cluster-a".into(),
                    node: remote,
                    failure_domain: "zone-b".into(),
                    voter: true,
                },
                &bundle_identity(bytes),
                bytes,
            )
            .await
            .unwrap();

        assert_eq!(ack.status, AckStatus::Complete);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }

    #[tokio::test]
    async fn rejects_cross_cluster_target_before_opening_stream() {
        let remote = NodeIncarnation {
            node_id: "node-b".into(),
            incarnation: 1,
        };
        let manager = TonicReplicationStreamManager::new(
            "cluster-a",
            NodeIncarnation {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            "test-token",
            [ReplicationPeer {
                cluster_id: "cluster-b".into(),
                node: remote.clone(),
                endpoint: "http://127.0.0.1:9".into(),
            }],
            ReplicationStreamOptions {
                allow_insecure_transport_for_tests: true,
                ..ReplicationStreamOptions::default()
            },
        )
        .unwrap();
        let bytes = b"bundle";
        let error = manager
            .send_bundle(
                &BundleTarget {
                    cluster_id: "cluster-b".into(),
                    node: remote,
                    failure_domain: "zone-b".into(),
                    voter: true,
                },
                &bundle_identity(bytes),
                bytes,
            )
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cross-cluster replication cannot provide transaction durability")
        );
    }

    #[tokio::test]
    async fn endpoint_replacement_rejects_plaintext_without_explicit_test_mode() {
        let remote = NodeIncarnation {
            node_id: "node-b".into(),
            incarnation: 1,
        };
        let manager = TonicReplicationStreamManager::new(
            "cluster-a",
            NodeIncarnation {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            "test-token",
            [ReplicationPeer {
                cluster_id: "cluster-a".into(),
                node: remote.clone(),
                endpoint: "https://node-b.example".into(),
            }],
            ReplicationStreamOptions::default(),
        )
        .unwrap();
        let error = manager
            .replace_peer_endpoint("cluster-a", &remote, "http://node-b.example")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("requires TLS"));
    }
}
