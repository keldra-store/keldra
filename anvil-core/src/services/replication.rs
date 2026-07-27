use std::{path::Path, pin::Pin, sync::Arc};

use anyhow::Context as _;
use async_trait::async_trait;
use futures_core::Stream;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, metadata::MetadataMap};
use uuid::Uuid;

#[cfg(test)]
use prost::Message as _;

use crate::{
    anvil_api::{
        ReplicationAckStatus, ReplicationApplicationAck, ReplicationDataFrame,
        ReplicationReadChunk, ReplicationSessionAccepted, ReplicationSessionOpen,
        ReplicationStreamRequest, ReplicationStreamResponse, ReplicationTransferKind,
        ReplicationTransferWatermark, replication_service_server::ReplicationService,
        replication_stream_request, replication_stream_response,
    },
    replication::{
        AckStatus, AuthenticatedPeer, CompleteTransferChunk, ConnectionSession, ReplicationFrame,
        TransferKind, TransferReceiver,
    },
    replication_client::bundle_transfer_id,
};

#[async_trait]
pub trait ReplicationConnectionAuthorizer: Send + Sync + 'static {
    /// Validates the node token and Zanzibar connection permission.
    ///
    /// This is called exactly once for each `Replicate` RPC, after its opening
    /// message and before any data frame is accepted.
    async fn authorize(
        &self,
        metadata: &MetadataMap,
        open: &ReplicationSessionOpen,
    ) -> Result<AuthenticatedPeer, Status>;

    /// Revalidate only the applied incarnation fence before accepting another
    /// frame on an authenticated stream.
    fn authorize_incarnation(&self, _node_id: &str, _incarnation: u64) -> Result<(), Status> {
        Ok(())
    }
}

pub struct ReplicationServiceImpl<A> {
    authorizer: Arc<A>,
    receiver: Arc<std::sync::Mutex<TransferReceiver>>,
    prepared_bundles: Option<crate::bundle_replication::AppendOnlyPreparedBundleStore>,
    #[cfg(test)]
    frame_faults: Option<Arc<std::sync::Mutex<crate::mvcc_fault_injection::FrameFaultPlan>>>,
    #[cfg(test)]
    dropped_complete_acks: Arc<std::sync::atomic::AtomicUsize>,
}

impl<A> Clone for ReplicationServiceImpl<A> {
    fn clone(&self) -> Self {
        Self {
            authorizer: self.authorizer.clone(),
            receiver: self.receiver.clone(),
            prepared_bundles: self.prepared_bundles.clone(),
            #[cfg(test)]
            frame_faults: self.frame_faults.clone(),
            #[cfg(test)]
            dropped_complete_acks: self.dropped_complete_acks.clone(),
        }
    }
}

impl<A: ReplicationConnectionAuthorizer> ReplicationServiceImpl<A> {
    pub fn open(authorizer: A, directory: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            authorizer: Arc::new(authorizer),
            receiver: Arc::new(std::sync::Mutex::new(TransferReceiver::open(directory)?)),
            prepared_bundles: None,
            #[cfg(test)]
            frame_faults: None,
            #[cfg(test)]
            dropped_complete_acks: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }

    pub(crate) fn receiver(&self) -> Arc<std::sync::Mutex<TransferReceiver>> {
        self.receiver.clone()
    }

    pub(crate) fn with_prepared_bundles(
        mut self,
        prepared_bundles: crate::bundle_replication::AppendOnlyPreparedBundleStore,
    ) -> Self {
        self.prepared_bundles = Some(prepared_bundles);
        self
    }

    #[cfg(test)]
    pub fn with_frame_fault_plan(
        mut self,
        plan: crate::mvcc_fault_injection::FrameFaultPlan,
    ) -> Self {
        self.frame_faults = Some(Arc::new(std::sync::Mutex::new(plan)));
        self
    }

    /// Drops Complete ACKs after the receiver has durably completed the
    /// transfer. The stream remains open, modelling a silent half-open socket.
    #[cfg(test)]
    pub fn with_dropped_complete_acks(self, count: usize) -> Self {
        self.dropped_complete_acks
            .store(count, std::sync::atomic::Ordering::SeqCst);
        self
    }

    #[cfg(test)]
    fn apply_frame_faults(
        &self,
        frame: ReplicationDataFrame,
    ) -> Result<Vec<ReplicationDataFrame>, Status> {
        use crate::mvcc_fault_injection::{FaultPoint, hit};

        if hit(FaultPoint::ReplicationFrame).is_err() {
            return Ok(Vec::new());
        }
        let Some(plan) = &self.frame_faults else {
            return Ok(vec![frame]);
        };
        let mut encoded = Vec::new();
        frame
            .encode(&mut encoded)
            .map_err(|error| Status::internal(error.to_string()))?;
        let mut plan = plan.lock().expect("frame fault plan poisoned");
        let mut delayed = plan.flush_reordered();
        let mut outputs = match plan.apply(encoded) {
            Ok(outputs) => outputs,
            Err(_) => {
                let _ = hit(FaultPoint::ReplicationHalfOpen);
                return Ok(Vec::new());
            }
        };
        // Current frames precede previously held frames, making reordering
        // deterministic so protocol validation can reject gaps or stale data.
        outputs.append(&mut delayed);
        outputs
            .into_iter()
            .map(|bytes| {
                ReplicationDataFrame::decode(bytes.as_slice())
                    .map_err(|error| Status::invalid_argument(error.to_string()))
            })
            .collect()
    }

    #[cfg(not(test))]
    fn apply_frame_faults(
        &self,
        frame: ReplicationDataFrame,
    ) -> Result<Vec<ReplicationDataFrame>, Status> {
        Ok(vec![frame])
    }

    #[cfg(test)]
    fn should_drop_complete_ack(&self, status: AckStatus) -> bool {
        use std::sync::atomic::Ordering;

        if status != AckStatus::Complete {
            return false;
        }
        self.dropped_complete_acks
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
    }

    #[cfg(not(test))]
    fn should_drop_complete_ack(&self, _status: AckStatus) -> bool {
        false
    }

    async fn serve<S>(
        &self,
        metadata: MetadataMap,
        mut inbound: S,
        output: mpsc::Sender<Result<ReplicationStreamResponse, Status>>,
    ) where
        S: Stream<Item = Result<ReplicationStreamRequest, Status>> + Unpin + Send + 'static,
    {
        let Some(first) = inbound.next().await else {
            send_error(
                &output,
                Status::invalid_argument("replication stream requires open"),
            )
            .await;
            return;
        };
        let first = match first {
            Ok(first) => first,
            Err(error) => {
                send_error(&output, error).await;
                return;
            }
        };
        let Some(replication_stream_request::Message::Open(open)) = first.message else {
            send_error(
                &output,
                Status::invalid_argument("first replication message must open a session"),
            )
            .await;
            return;
        };
        let peer = match self.authorizer.authorize(&metadata, &open).await {
            Ok(peer) => peer,
            Err(error) => {
                send_error(&output, error).await;
                return;
            }
        };
        let mut session = match ConnectionSession::establish(open.cluster_id, peer) {
            Ok(session) => session,
            Err(error) => {
                send_error(&output, Status::invalid_argument(error.to_string())).await;
                return;
            }
        };
        if output
            .send(Ok(response(
                replication_stream_response::Message::Accepted(ReplicationSessionAccepted {
                    session_id: session.id().to_string(),
                    peer_node_id: session.peer().node_id.clone(),
                    peer_node_incarnation: session.peer().incarnation,
                }),
            )))
            .await
            .is_err()
        {
            return;
        }

        while let Some(message) = inbound.next().await {
            if let Err(error) = self
                .authorizer
                .authorize_incarnation(&session.peer().node_id, session.peer().incarnation)
            {
                send_error(&output, error).await;
                return;
            }
            let request = match message {
                Ok(request) => request,
                Err(error) => {
                    send_error(&output, error).await;
                    return;
                }
            };
            let frames = match request.message {
                Some(replication_stream_request::Message::Frame(frame)) => {
                    let frames = match self.apply_frame_faults(frame) {
                        Ok(frames) => frames,
                        Err(error) => {
                            send_error(&output, error).await;
                            return;
                        }
                    };
                    let mut decoded = Vec::with_capacity(frames.len());
                    for frame in frames {
                        match decode_frame(frame) {
                            Ok(frame) => decoded.push(frame),
                            Err(error) => {
                                send_error(&output, error).await;
                                return;
                            }
                        }
                    }
                    decoded
                }
                Some(replication_stream_request::Message::Heartbeat(heartbeat)) => {
                    if heartbeat.session_id != session.id().to_string() {
                        send_error(
                            &output,
                            Status::failed_precondition(
                                "heartbeat belongs to a different replication session",
                            ),
                        )
                        .await;
                        return;
                    }
                    if output
                        .send(Ok(response(
                            replication_stream_response::Message::Heartbeat(heartbeat),
                        )))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                Some(replication_stream_request::Message::Watermark(watermark)) => {
                    let transfer_id = match parse_uuid("transfer_id", &watermark.transfer_id) {
                        Ok(transfer_id) => transfer_id,
                        Err(error) => {
                            send_error(&output, error).await;
                            return;
                        }
                    };
                    let receiver = self.receiver.clone();
                    let result = tokio::task::spawn_blocking(move || {
                        receiver
                            .lock()
                            .map_err(|_| anyhow::anyhow!("replication receiver lock poisoned"))
                            .and_then(|receiver| receiver.watermark(transfer_id))
                    })
                    .await;
                    let watermark = match result {
                        Ok(Ok(Some(watermark))) => watermark,
                        Ok(Ok(None)) => crate::replication::TransferWatermark {
                            persisted_through: 0,
                            complete: false,
                            completed_hash: None,
                        },
                        Ok(Err(error)) => {
                            send_error(&output, Status::internal(error.to_string())).await;
                            return;
                        }
                        Err(error) => {
                            send_error(
                                &output,
                                Status::internal(format!(
                                    "replication watermark task failed: {error}"
                                )),
                            )
                            .await;
                            return;
                        }
                    };
                    if output
                        .send(Ok(response(
                            replication_stream_response::Message::Watermark(
                                ReplicationTransferWatermark {
                                    transfer_id: transfer_id.to_string(),
                                    persisted_through: watermark.persisted_through,
                                    complete: watermark.complete,
                                    completed_hash: watermark
                                        .completed_hash
                                        .map(Vec::from)
                                        .unwrap_or_default(),
                                },
                            ),
                        )))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                Some(replication_stream_request::Message::Read(read)) => {
                    let transfer_id = match parse_uuid("transfer_id", &read.transfer_id) {
                        Ok(transfer_id) => transfer_id,
                        Err(error) => {
                            send_error(&output, error).await;
                            return;
                        }
                    };
                    let max_bytes = match usize::try_from(read.max_bytes) {
                        Ok(max_bytes) if max_bytes > 0 => max_bytes,
                        _ => {
                            send_error(
                                &output,
                                Status::invalid_argument(
                                    "replication read max_bytes must fit usize and be non-zero",
                                ),
                            )
                            .await;
                            return;
                        }
                    };
                    let receiver = self.receiver.clone();
                    let prepared_bundles = self.prepared_bundles.clone();
                    let result = tokio::task::spawn_blocking(move || {
                        let inbox_result = receiver
                            .lock()
                            .map_err(|_| anyhow::anyhow!("replication receiver lock poisoned"))?
                            .read_complete_chunk(transfer_id, read.offset, max_bytes);
                        match inbox_result {
                            Ok(chunk) => Ok(chunk),
                            Err(inbox_error) => {
                                let Some(prepared_bundles) = prepared_bundles else {
                                    return Err(inbox_error);
                                };
                                read_prepared_bundle_chunk(
                                    &prepared_bundles,
                                    transfer_id,
                                    read.offset,
                                    max_bytes,
                                )?
                                .ok_or(inbox_error)
                            }
                        }
                    })
                    .await;
                    let chunk = match result {
                        Ok(Ok(chunk)) => chunk,
                        Ok(Err(error)) => {
                            send_error(&output, Status::not_found(error.to_string())).await;
                            return;
                        }
                        Err(error) => {
                            send_error(&output, Status::internal(error.to_string())).await;
                            return;
                        }
                    };
                    if output
                        .send(Ok(response(replication_stream_response::Message::Read(
                            ReplicationReadChunk {
                                transfer_id: transfer_id.to_string(),
                                offset: chunk.offset,
                                payload_checksum: ReplicationFrame::checksum(&chunk.payload)
                                    .to_vec(),
                                payload: chunk.payload,
                                total_length: chunk.total_length,
                                completed_hash: chunk.completed_hash.to_vec(),
                                finish: chunk.finish,
                            },
                        ))))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                Some(replication_stream_request::Message::Open(_)) => {
                    send_error(
                        &output,
                        Status::failed_precondition("reconnect requires a new replication stream"),
                    )
                    .await;
                    return;
                }
                None => {
                    send_error(
                        &output,
                        Status::invalid_argument("empty replication message"),
                    )
                    .await;
                    return;
                }
            };

            // A dropped or half-open frame deliberately produces no
            // application response. The client progress deadline must expire,
            // discard this session, and resume from the durable watermark.
            if frames.is_empty() {
                continue;
            }

            let persist_started_at = std::time::Instant::now();
            let receiver = self.receiver.clone();
            let result = tokio::task::spawn_blocking(move || {
                let result = receiver
                    .lock()
                    .map_err(|_| anyhow::anyhow!("replication receiver lock poisoned"))
                    .and_then(|mut receiver| {
                        let mut last_ack = None;
                        for frame in &frames {
                            last_ack = Some(receiver.receive(&mut session, frame)?);
                        }
                        last_ack.context("frame fault plan emitted no frames")
                    });
                (session, result)
            })
            .await;
            let (returned_session, ack) = match result {
                Ok(result) => result,
                Err(error) => {
                    send_error(
                        &output,
                        Status::internal(format!("replication persistence task failed: {error}")),
                    )
                    .await;
                    return;
                }
            };
            session = returned_session;
            let ack = match ack {
                Ok(ack) => {
                    crate::perf::record_replication_persist_latency(
                        "ok",
                        persist_started_at.elapsed(),
                    );
                    tracing::debug!(
                        operation = "replication.persist_ack",
                        session_id = %ack.session_id,
                        transfer_id = %ack.transfer_id,
                        sequence = ack.acknowledged_sequence,
                        persisted_through = ack.persisted_through,
                        "persisted replication frame before ACK"
                    );
                    ack
                }
                Err(error) => {
                    crate::perf::record_replication_persist_latency(
                        "error",
                        persist_started_at.elapsed(),
                    );
                    send_error(&output, Status::data_loss(error.to_string())).await;
                    return;
                }
            };
            if self.should_drop_complete_ack(ack.status) {
                tracing::debug!(
                    operation = "replication.persist_ack",
                    transfer_id = %ack.transfer_id,
                    persisted_through = ack.persisted_through,
                    "fault injection dropped Complete ACK after durable persistence"
                );
                continue;
            }
            if output
                .send(Ok(response(replication_stream_response::Message::Ack(
                    ReplicationApplicationAck {
                        session_id: ack.session_id.to_string(),
                        acknowledged_sequence: ack.acknowledged_sequence,
                        transfer_id: ack.transfer_id.to_string(),
                        persisted_through: ack.persisted_through,
                        completed_hash: ack.completed_hash.map(Vec::from).unwrap_or_default(),
                        status: encode_ack_status(ack.status) as i32,
                        rejection_reason: String::new(),
                    },
                ))))
                .await
                .is_err()
            {
                return;
            }
        }
    }
}

#[tonic::async_trait]
impl<A: ReplicationConnectionAuthorizer> ReplicationService for ReplicationServiceImpl<A> {
    type ReplicateStream =
        Pin<Box<dyn Stream<Item = Result<ReplicationStreamResponse, Status>> + Send>>;

    async fn replicate(
        &self,
        request: Request<tonic::Streaming<ReplicationStreamRequest>>,
    ) -> Result<Response<Self::ReplicateStream>, Status> {
        let metadata = request.metadata().clone();
        let inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(32);
        let service = self.clone();
        tokio::spawn(async move { service.serve(metadata, inbound, tx).await });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn decode_frame(frame: ReplicationDataFrame) -> Result<ReplicationFrame, Status> {
    let session_id = parse_uuid("session_id", &frame.session_id)?;
    let transfer_id = parse_uuid("transfer_id", &frame.transfer_id)?;
    let payload_checksum = parse_hash("payload_checksum", &frame.payload_checksum)?;
    let final_hash = parse_hash("final_hash", &frame.final_hash)?;
    let kind = match ReplicationTransferKind::try_from(frame.kind) {
        Ok(ReplicationTransferKind::TransactionBundle) => TransferKind::TransactionBundle,
        Ok(ReplicationTransferKind::ObjectShard) => TransferKind::ObjectShard,
        Ok(ReplicationTransferKind::MvccCatchUp) => TransferKind::MvccCatchUp,
        Ok(ReplicationTransferKind::ConsensusSnapshot) => TransferKind::ConsensusSnapshot,
        Ok(ReplicationTransferKind::Repair) => TransferKind::Repair,
        _ => {
            return Err(Status::invalid_argument(
                "invalid replication transfer kind",
            ));
        }
    };
    Ok(ReplicationFrame {
        session_id,
        cluster_id: frame.cluster_id,
        sequence: frame.sequence,
        partition: frame.partition,
        transfer_id,
        kind,
        offset: frame.offset,
        payload: frame.payload,
        payload_checksum,
        total_length: frame.total_length,
        final_hash,
        finish: frame.finish,
        transaction_id: frame.transaction_id,
        prepared_snapshot_version: frame.prepared_snapshot_version,
        prepared_at_unix_ms: frame.prepared_at_unix_ms,
        provisional: frame.provisional,
    })
}

fn parse_uuid(label: &str, value: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(value).map_err(|_| Status::invalid_argument(format!("{label} must be a UUID")))
}

fn parse_hash(label: &str, value: &[u8]) -> Result<[u8; 32], Status> {
    value
        .try_into()
        .map_err(|_| Status::invalid_argument(format!("{label} must contain 32 bytes")))
}

fn encode_ack_status(status: AckStatus) -> ReplicationAckStatus {
    match status {
        AckStatus::Received => ReplicationAckStatus::Received,
        AckStatus::Persisted => ReplicationAckStatus::Persisted,
        AckStatus::Complete => ReplicationAckStatus::Complete,
        AckStatus::Applied => ReplicationAckStatus::Applied,
        AckStatus::Rejected => ReplicationAckStatus::Rejected,
    }
}

fn read_prepared_bundle_chunk(
    prepared_bundles: &crate::bundle_replication::AppendOnlyPreparedBundleStore,
    transfer_id: Uuid,
    offset: u64,
    max_bytes: usize,
) -> anyhow::Result<Option<CompleteTransferChunk>> {
    for identity in prepared_bundles.identities()? {
        if bundle_transfer_id(&identity)? != transfer_id {
            continue;
        }
        let Some(bytes) = prepared_bundles.read(&identity)? else {
            continue;
        };
        if offset > identity.length {
            anyhow::bail!("replication read offset exceeds prepared bundle length");
        }
        let start =
            usize::try_from(offset).context("replication read offset exceeds address space")?;
        let end = start.saturating_add(max_bytes).min(bytes.len());
        let completed_hash: [u8; 32] = hex::decode(
            identity
                .hash
                .strip_prefix("sha256:")
                .context("prepared bundle identity must use sha256")?,
        )?
        .try_into()
        .map_err(|_| anyhow::anyhow!("prepared bundle hash must contain 32 bytes"))?;
        return Ok(Some(CompleteTransferChunk {
            offset,
            payload: bytes[start..end].to_vec(),
            total_length: identity.length,
            completed_hash,
            finish: end == bytes.len(),
        }));
    }
    Ok(None)
}

fn response(message: replication_stream_response::Message) -> ReplicationStreamResponse {
    ReplicationStreamResponse {
        message: Some(message),
    }
}

async fn send_error(
    output: &mpsc::Sender<Result<ReplicationStreamResponse, Status>>,
    error: Status,
) {
    let _ = output.send(Err(error)).await;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use sha2::{Digest, Sha256};

    use super::*;
    use crate::{
        bundle_replication::AppendOnlyPreparedBundleStore,
        mvcc_fault_injection::{FrameAction, FrameFaultPlan},
        mvcc_transaction::{BundleIdentity, NodeIncarnation, PreparedBundleStore},
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
        ) -> Result<AuthenticatedPeer, Status> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if metadata.get("authorization").is_none() {
                return Err(Status::unauthenticated("node token required"));
            }
            AuthenticatedPeer::new(open.node_id.clone(), open.node_incarnation)
                .map_err(|error| Status::permission_denied(error.to_string()))
        }
    }

    fn request(message: replication_stream_request::Message) -> ReplicationStreamRequest {
        ReplicationStreamRequest {
            message: Some(message),
        }
    }

    fn data_frame(sequence: u64) -> ReplicationDataFrame {
        let payload = vec![sequence as u8];
        ReplicationDataFrame {
            session_id: Uuid::new_v4().to_string(),
            cluster_id: "cluster-a".into(),
            sequence,
            partition: "partition-a".into(),
            transfer_id: Uuid::new_v4().to_string(),
            kind: ReplicationTransferKind::ObjectShard as i32,
            offset: sequence - 1,
            payload_checksum: blake3::hash(&payload).as_bytes().to_vec(),
            final_hash: blake3::hash(&payload).as_bytes().to_vec(),
            total_length: 2,
            payload,
            finish: false,
            transaction_id: "tx".into(),
            prepared_snapshot_version: 1,
            prepared_at_unix_ms: 1,
            provisional: true,
        }
    }

    #[tokio::test]
    async fn prepared_bundle_holder_serves_bundle_without_replication_inbox_copy() {
        let prepared_directory = tempfile::tempdir().unwrap();
        let prepared = AppendOnlyPreparedBundleStore::open(
            prepared_directory.path(),
            "cluster-a",
            NodeIncarnation {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            "zone-a",
        )
        .unwrap();
        let bytes = b"coordinator-local-canonical-bundle";
        let mut hash = Sha256::new();
        hash.update(b"anvil.mvcc.transaction-bundle.v1");
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
        let identity = BundleIdentity {
            hash: format!("sha256:{}", hex::encode(hash.finalize())),
            length: bytes.len() as u64,
        };
        prepared.persist(&identity, bytes).await.unwrap();
        let transfer_id = bundle_transfer_id(&identity).unwrap();

        let first = read_prepared_bundle_chunk(&prepared, transfer_id, 0, 7)
            .unwrap()
            .expect("prepared holder must resolve deterministic transfer identity");
        assert_eq!(first.payload, &bytes[..7]);
        assert!(!first.finish);
        let rest = read_prepared_bundle_chunk(&prepared, transfer_id, 7, bytes.len())
            .unwrap()
            .expect("prepared holder must serve subsequent chunks");
        assert_eq!(rest.payload, &bytes[7..]);
        assert!(rest.finish);
    }

    #[test]
    fn operational_frame_hook_reorders_delayed_frames_deterministically() {
        let service = ReplicationServiceImpl::open(
            Authorizer {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            tempfile::tempdir().unwrap().path(),
        )
        .unwrap()
        .with_frame_fault_plan(FrameFaultPlan::new([
            FrameAction::Hold,
            FrameAction::Deliver,
        ]));

        assert!(
            service
                .apply_frame_faults(data_frame(1))
                .unwrap()
                .is_empty()
        );
        let reordered = service.apply_frame_faults(data_frame(2)).unwrap();
        assert_eq!(
            reordered
                .iter()
                .map(|frame| frame.sequence)
                .collect::<Vec<_>>(),
            [2, 1]
        );
    }

    #[tokio::test]
    async fn authorizes_once_then_persists_and_completes_frames() {
        let calls = Arc::new(AtomicUsize::new(0));
        let directory = tempfile::tempdir().unwrap();
        let service = ReplicationServiceImpl::open(
            Authorizer {
                calls: calls.clone(),
            },
            directory.path(),
        )
        .unwrap();
        let mut metadata = MetadataMap::new();
        metadata.insert("authorization", "Bearer node-token".parse().unwrap());
        let (tx, mut rx) = mpsc::channel(8);
        let (input_tx, input_rx) = mpsc::channel(8);
        let task = tokio::spawn(async move {
            service
                .serve(metadata, ReceiverStream::new(input_rx), tx)
                .await
        });
        input_tx
            .send(Ok(request(replication_stream_request::Message::Open(
                ReplicationSessionOpen {
                    node_id: "node-b".into(),
                    node_incarnation: 2,
                    requested_session_id: String::new(),
                    cluster_id: "cluster-a".into(),
                },
            ))))
            .await
            .unwrap();
        let accepted = rx.recv().await.unwrap().unwrap();
        let session_id = match accepted.message.unwrap() {
            replication_stream_response::Message::Accepted(accepted) => accepted.session_id,
            _ => panic!("expected accepted session"),
        };
        let payload = b"object-shard".to_vec();
        let transfer_id = Uuid::new_v4();
        input_tx
            .send(Ok(request(replication_stream_request::Message::Frame(
                ReplicationDataFrame {
                    session_id: session_id.clone(),
                    cluster_id: "cluster-a".into(),
                    sequence: 1,
                    partition: "partition-a".into(),
                    transfer_id: transfer_id.to_string(),
                    kind: ReplicationTransferKind::ObjectShard as i32,
                    offset: 0,
                    payload_checksum: blake3::hash(&payload).as_bytes().to_vec(),
                    final_hash: blake3::hash(&payload).as_bytes().to_vec(),
                    total_length: payload.len() as u64,
                    payload,
                    finish: true,
                    transaction_id: "tx".into(),
                    prepared_snapshot_version: 1,
                    prepared_at_unix_ms: 1,
                    provisional: true,
                },
            ))))
            .await
            .unwrap();
        let ack = rx.recv().await.unwrap().unwrap();
        match ack.message.unwrap() {
            replication_stream_response::Message::Ack(ack) => {
                assert_eq!(ack.status, ReplicationAckStatus::Complete as i32);
                assert_eq!(ack.persisted_through, 12);
            }
            _ => panic!("expected application acknowledgement"),
        }
        input_tx
            .send(Ok(request(replication_stream_request::Message::Watermark(
                ReplicationTransferWatermark {
                    transfer_id: transfer_id.to_string(),
                    ..Default::default()
                },
            ))))
            .await
            .unwrap();
        let watermark = rx.recv().await.unwrap().unwrap();
        match watermark.message.unwrap() {
            replication_stream_response::Message::Watermark(watermark) => {
                assert!(watermark.complete);
                assert_eq!(watermark.persisted_through, 12);
                assert_eq!(watermark.completed_hash.len(), 32);
            }
            _ => panic!("expected transfer watermark"),
        }
        input_tx
            .send(Ok(request(replication_stream_request::Message::Heartbeat(
                crate::anvil_api::ReplicationHeartbeat {
                    session_id,
                    sequence: 2,
                    last_acknowledged_sequence: 1,
                },
            ))))
            .await
            .unwrap();
        assert!(matches!(
            rx.recv().await.unwrap().unwrap().message,
            Some(replication_stream_response::Message::Heartbeat(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(input_tx);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_data_before_open_without_authorizing() {
        let calls = Arc::new(AtomicUsize::new(0));
        let service = ReplicationServiceImpl::open(
            Authorizer {
                calls: calls.clone(),
            },
            tempfile::tempdir().unwrap().path(),
        )
        .unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        service
            .serve(
                MetadataMap::new(),
                tokio_stream::iter(vec![Ok(request(
                    replication_stream_request::Message::Heartbeat(Default::default()),
                ))]),
                tx,
            )
            .await;
        assert_eq!(
            rx.recv().await.unwrap().unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
