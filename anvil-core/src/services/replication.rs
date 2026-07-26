use std::{path::Path, pin::Pin, sync::Arc};

use async_trait::async_trait;
use futures_core::Stream;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, metadata::MetadataMap};
use uuid::Uuid;

use crate::{
    anvil_api::{
        ReplicationAckStatus, ReplicationApplicationAck, ReplicationDataFrame,
        ReplicationReadChunk, ReplicationSessionAccepted, ReplicationSessionOpen,
        ReplicationStreamRequest, ReplicationStreamResponse, ReplicationTransferKind,
        ReplicationTransferWatermark, replication_service_server::ReplicationService,
        replication_stream_request, replication_stream_response,
    },
    replication::{
        AckStatus, AuthenticatedPeer, ConnectionSession, ReplicationFrame, TransferKind,
        TransferReceiver,
    },
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
}

pub struct ReplicationServiceImpl<A> {
    authorizer: Arc<A>,
    receiver: Arc<std::sync::Mutex<TransferReceiver>>,
}

impl<A> Clone for ReplicationServiceImpl<A> {
    fn clone(&self) -> Self {
        Self {
            authorizer: self.authorizer.clone(),
            receiver: self.receiver.clone(),
        }
    }
}

impl<A: ReplicationConnectionAuthorizer> ReplicationServiceImpl<A> {
    pub fn open(authorizer: A, directory: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            authorizer: Arc::new(authorizer),
            receiver: Arc::new(std::sync::Mutex::new(TransferReceiver::open(directory)?)),
        })
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
            let request = match message {
                Ok(request) => request,
                Err(error) => {
                    send_error(&output, error).await;
                    return;
                }
            };
            let frame = match request.message {
                Some(replication_stream_request::Message::Frame(frame)) => {
                    match decode_frame(frame) {
                        Ok(frame) => frame,
                        Err(error) => {
                            send_error(&output, error).await;
                            return;
                        }
                    }
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
                    let result = tokio::task::spawn_blocking(move || {
                        receiver
                            .lock()
                            .map_err(|_| anyhow::anyhow!("replication receiver lock poisoned"))?
                            .read_complete_chunk(transfer_id, read.offset, max_bytes)
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

            let receiver = self.receiver.clone();
            let result = tokio::task::spawn_blocking(move || {
                let result = receiver
                    .lock()
                    .map_err(|_| anyhow::anyhow!("replication receiver lock poisoned"))
                    .and_then(|mut receiver| receiver.receive(&mut session, &frame));
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
                Ok(ack) => ack,
                Err(error) => {
                    send_error(&output, Status::data_loss(error.to_string())).await;
                    return;
                }
            };
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

    use super::*;

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
