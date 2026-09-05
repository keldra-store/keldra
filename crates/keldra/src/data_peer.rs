//! Typed storage operations on Keldra's mandatory-mTLS private peer listener.
//!
use std::collections::BTreeMap;
use std::future::Future;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hyper_util::rt::TokioIo;
use keldra_consensus::{
    AuthenticatedPeer, ClusterId, CommittedPeerPinProvider, NodeId, PeerRpcKind, PeerSpkiSha256,
    PeerTlsConnector, PeerTlsError, authorize_peer_rpc,
};
use keldra_store::{
    AuthzRealmMutation, BlobRef, CompleteCopySealOutcome, CurrentObjectSnapshot, ErasureCodec,
    ErasureProfile, LocalChange, MAX_LOCAL_INVALIDATION_SCAN_RECORDS, MutationError, ObjectKey,
    ObjectMutation, ObjectPathSnapshot, ObjectSnapshotApplied, ObjectSnapshotError,
    PayloadStoreError, ReferenceDeltaApplied, ReferenceDeltaBatch,
    ReplicaAuthzRealmMutationApplied, ReplicaObjectMutationApplied, RetainedVersionDeleteMutation,
    ShardIdentity, ShardSealOutcome, ShardStoreError, SourceId, Store, WatchJournalStatus,
};
use tokio::io::AsyncWriteExt;
use tonic::codegen::Service;
use tonic::codegen::http::Uri;
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status, Streaming};

mod cutover;
mod definition_coordination;
mod derived_consumer;
mod errors;
mod handoff;
mod handoff_scope;
mod mutation_admission;
mod object_mutation;
mod object_mutation_batch;
mod object_snapshot;
mod protocol;
mod retained_version_delete;
mod source_journal;
mod stream;
mod timeout;
mod transport;
mod typed_json;
mod wire;
mod wire_value;

use errors::{map_mutation_error, map_payload_error, map_shard_error};
use handoff_scope::{HandoffAuthority, HandoffTarget};
use mutation_admission::MutationAdmission;
use object_snapshot::{
    encode_object_snapshot, map_object_snapshot_error, require_object_snapshot_bound,
};
use protocol::{
    AuthzRealmStream, ContentStream, MAX_DATA_PEER_MESSAGE_BYTES, MAX_OBJECT_SNAPSHOT_BYTES,
    MAX_TYPED_MUTATION_BYTES,
};
pub(crate) use protocol::{
    DATA_PEER_FRAME_BYTES, DATA_PEER_SCHEMA_VERSION, MAX_OBJECT_MUTATION_BATCH_BYTES,
    MAX_OBJECT_MUTATION_BATCH_ITEMS,
};
use stream::{next_stream_message, require_large_blob, stream_blob, validate_stream_frame};
use timeout::effective_timeout;
pub(crate) use transport::{DataPeerTransport, RemoteMutationDrain};
use typed_json::{decode_typed, encode_page, encode_typed, require_typed_bound};
use wire_value::{
    content_end, content_frame, parse_blob, parse_cluster_id, parse_shard, parse_small_blob,
    require_response_schema, wire_blob, wire_shard,
};

#[derive(Clone)]
pub(crate) struct DataPeerService {
    store: Store,
    pins: Arc<dyn CommittedPeerPinProvider>,
    codec: Arc<ErasureCodec>,
    handoff: HandoffAuthority,
    mutation_admission: MutationAdmission,
    cutover_admission: crate::mutation_admission::MutationAdmission,
    maximum_unary_time: Duration,
    max_blob_bytes: u64,
}

pub(crate) type DataPeerServer = wire::data_peer_server::DataPeerServer<DataPeerService>;

impl DataPeerService {
    pub(crate) fn new(
        store: Store,
        pins: Arc<dyn CommittedPeerPinProvider>,
        decisions: keldra_consensus::DecisionRaft,
        local_node: NodeId,
        profile: ErasureProfile,
        maximum_unary_time: Duration,
        max_blob_bytes: u64,
        cutover_admission: crate::mutation_admission::MutationAdmission,
    ) -> Result<Self, anyhow::Error> {
        Self::validate_and_build(
            store,
            pins,
            HandoffAuthority::raft(decisions.clone(), local_node),
            MutationAdmission::raft(decisions, local_node),
            profile,
            maximum_unary_time,
            max_blob_bytes,
            cutover_admission,
        )
    }

    #[cfg(test)]
    fn new_test(
        store: Store,
        pins: Arc<dyn CommittedPeerPinProvider>,
        cluster_id: ClusterId,
        local_node: NodeId,
        active_nodes: impl IntoIterator<Item = NodeId>,
        profile: ErasureProfile,
        maximum_unary_time: Duration,
        max_blob_bytes: u64,
    ) -> Result<Self, anyhow::Error> {
        Self::validate_and_build(
            store,
            pins,
            HandoffAuthority::reject(),
            MutationAdmission::fixed(cluster_id, local_node, active_nodes),
            profile,
            maximum_unary_time,
            max_blob_bytes,
            crate::mutation_admission::MutationAdmission::new(),
        )
    }

    fn validate_and_build(
        store: Store,
        pins: Arc<dyn CommittedPeerPinProvider>,
        handoff: HandoffAuthority,
        mutation_admission: MutationAdmission,
        profile: ErasureProfile,
        maximum_unary_time: Duration,
        max_blob_bytes: u64,
        cutover_admission: crate::mutation_admission::MutationAdmission,
    ) -> Result<Self, anyhow::Error> {
        anyhow::ensure!(
            !maximum_unary_time.is_zero()
                && tokio::time::Instant::now()
                    .checked_add(maximum_unary_time)
                    .is_some(),
            "private peer maximum unary time must be positive and fit the server clock"
        );
        anyhow::ensure!(
            max_blob_bytes > keldra_store::SMALL_BLOB_MAX_BYTES as u64,
            "private peer maximum blob bytes must permit a large object"
        );
        let codec = ErasureCodec::new(profile)?;
        Ok(Self {
            store,
            pins,
            codec: Arc::new(codec),
            handoff,
            mutation_admission,
            cutover_admission,
            maximum_unary_time,
            max_blob_bytes,
        })
    }

    pub(crate) fn into_server(self) -> DataPeerServer {
        DataPeerServer::new(self)
            .max_decoding_message_size(MAX_DATA_PEER_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_DATA_PEER_MESSAGE_BYTES)
    }

    fn authorize<T>(
        &self,
        request: &mut Request<T>,
        context: Option<&wire::PeerContext>,
        kind: PeerRpcKind,
    ) -> Result<AuthenticatedPeer, Status> {
        let pin = request
            .extensions()
            .get::<PeerSpkiSha256>()
            .copied()
            .ok_or_else(|| Status::unauthenticated("peer mTLS identity is missing"))?;
        let authenticated = self.authorize_context(context, pin, kind)?;
        request.extensions_mut().insert(authenticated);
        Ok(authenticated)
    }

    fn authorize_context(
        &self,
        context: Option<&wire::PeerContext>,
        pin: PeerSpkiSha256,
        kind: PeerRpcKind,
    ) -> Result<AuthenticatedPeer, Status> {
        let context =
            context.ok_or_else(|| Status::invalid_argument("peer context is required"))?;
        if context.schema_version != DATA_PEER_SCHEMA_VERSION {
            return Err(Status::failed_precondition(format!(
                "unsupported data-peer schema {}",
                context.schema_version
            )));
        }
        let cluster_id = parse_cluster_id(&context.cluster_id)?;
        authorize_peer_rpc(
            self.pins.as_ref(),
            cluster_id,
            NodeId(context.source_node_id),
            kind,
            pin,
        )
        .map_err(|_| Status::permission_denied("peer is not authorized for this RPC class"))
    }

    fn validate_handoff(
        &self,
        caller: AuthenticatedPeer,
        scope: Option<&wire::HandoffScope>,
        target: HandoffTarget,
    ) -> Result<(), Status> {
        self.handoff.validate(caller, scope, target)
    }

    async fn bounded<T>(
        &self,
        metadata: &MetadataMap,
        operation: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        let timeout = effective_timeout(metadata, self.maximum_unary_time);
        tokio::time::timeout(timeout, operation)
            .await
            .map_err(|_| Status::deadline_exceeded("private peer operation deadline exceeded"))?
    }
}

#[tonic::async_trait]
impl wire::data_peer_server::DataPeer for DataPeerService {
    async fn drain_mutations(
        &self,
        request: Request<wire::MutationDrainRequest>,
    ) -> Result<Response<wire::MutationDrained>, Status> {
        cutover::drain_mutations(self, request).await
    }
    async fn release_mutation_drain(
        &self,
        request: Request<wire::MutationDrainRequest>,
    ) -> Result<Response<wire::MutationDrained>, Status> {
        cutover::release_mutation_drain(self, request)
    }
    type GetSmallContentStream = ContentStream;
    type GetCompleteSourceStream = ContentStream;
    type GetShardStream = ContentStream;
    type GetAuthzRealmStream = AuthzRealmStream;
    async fn apply_object_mutation(
        &self,
        request: Request<wire::TypedMutationRequest>,
    ) -> Result<Response<wire::ObjectMutationApplied>, Status> {
        self.apply_object_mutation_call(request).await
    }
    async fn apply_object_mutation_batch(
        &self,
        request: Request<wire::TypedMutationBatchRequest>,
    ) -> Result<Response<wire::ObjectMutationBatchApplied>, Status> {
        self.apply_object_mutation_batch_call(request).await
    }
    async fn apply_retained_version_delete(
        &self,
        request: Request<wire::TypedMutationRequest>,
    ) -> Result<Response<wire::RetainedVersionDeleteApplied>, Status> {
        self.apply_retained_version_delete_call(request).await
    }
    async fn read_object_path_snapshot(
        &self,
        request: Request<wire::ObjectPathSnapshotRequest>,
    ) -> Result<Response<wire::ObjectPathSnapshotResponse>, Status> {
        self.read_object_path_snapshot_call(request).await
    }
    async fn read_object_path_snapshots(
        &self,
        request: Request<wire::ObjectPathSnapshotBatchRequest>,
    ) -> Result<Response<wire::ObjectPathSnapshotBatchResponse>, Status> {
        self.read_object_path_snapshots_call(request).await
    }
    async fn read_current_object_snapshot(
        &self,
        request: Request<wire::ObjectPathSnapshotRequest>,
    ) -> Result<Response<wire::CurrentObjectSnapshotResponse>, Status> {
        self.read_current_object_snapshot_call(request).await
    }
    async fn read_current_object_snapshots(
        &self,
        request: Request<wire::CurrentObjectSnapshotBatchRequest>,
    ) -> Result<Response<wire::CurrentObjectSnapshotBatchResponse>, Status> {
        self.read_current_object_snapshots_call(request).await
    }
    async fn read_exact_object_versions(
        &self,
        request: Request<wire::ExactObjectVersionBatchRequest>,
    ) -> Result<Response<wire::ExactObjectVersionBatchResponse>, Status> {
        self.read_exact_object_versions_call(request).await
    }
    async fn repair_object_path_snapshot(
        &self,
        request: Request<wire::RepairObjectPathSnapshotRequest>,
    ) -> Result<Response<wire::ObjectPathSnapshotApplied>, Status> {
        self.repair_object_path_snapshot_call(request).await
    }
    async fn apply_authz_realm_mutation(
        &self,
        mut request: Request<wire::TypedMutationRequest>,
    ) -> Result<Response<wire::AuthzRealmMutationApplied>, Status> {
        let _permit = self.cutover_admission.enter_continuation()?;
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        require_typed_bound(&request.get_ref().mutation_json)?;
        let mutation: AuthzRealmMutation = decode_typed(&request.get_ref().mutation_json)?;
        let metadata = request.metadata().clone();
        let repository = self.store.authz();
        let applied = self
            .bounded(&metadata, async move {
                tokio::task::spawn_blocking(move || {
                    repository.apply_authz_realm_mutation_replica(&mutation)
                })
                .await
                .map_err(|error| Status::internal(format!("join authorization apply: {error}")))?
                .map_err(|error| Status::failed_precondition(error.to_string()))
            })
            .await?;
        Ok(Response::new(wire::AuthzRealmMutationApplied {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            revision: applied.revision.0,
            replayed: applied.replayed,
        }))
    }
    async fn apply_reference_deltas(
        &self,
        mut request: Request<wire::TypedMutationRequest>,
    ) -> Result<Response<wire::ReferenceDeltaApplied>, Status> {
        let _permit = self.cutover_admission.enter_continuation()?;
        let peer = request.get_ref().peer.clone();
        let peer = self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        require_typed_bound(&request.get_ref().mutation_json)?;
        let mutation: ReferenceDeltaBatch = decode_typed(&request.get_ref().mutation_json)?;
        let placement_fence = self
            .mutation_admission
            .reference_deltas(peer, mutation.source)?;
        let metadata = request.metadata().clone();
        let store = self.store.clone();
        let admission = self.mutation_admission.clone();
        let applied = self
            .bounded(&metadata, async move {
                admission.require_fence(placement_fence)?;
                let applied = store
                    .apply_reference_deltas_progress(mutation)
                    .await
                    .map_err(|error| Status::failed_precondition(error.to_string()))?;
                admission.require_fence(placement_fence)?;
                Ok(applied)
            })
            .await?;
        Ok(Response::new(wire::ReferenceDeltaApplied {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            through: applied.through,
            replayed: applied.replayed,
        }))
    }
    async fn get_reference_delta_status(
        &self,
        mut request: Request<wire::ReferenceDeltaStatusRequest>,
    ) -> Result<Response<wire::ReferenceDeltaStatus>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
        let source: SourceId = decode_typed(&request.get_ref().source_id_json)?;
        let metadata = request.metadata().clone();
        let store = self.store.clone();
        let through = self
            .bounded(&metadata, async move {
                tokio::task::spawn_blocking(move || store.reference_delta_cursor(source))
                    .await
                    .map_err(|error| Status::internal(format!("join reference status: {error}")))?
                    .map_err(|error| Status::failed_precondition(error.to_string()))
            })
            .await?;
        Ok(Response::new(wire::ReferenceDeltaStatus {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            through,
        }))
    }
    async fn get_source_journal_status(
        &self,
        request: Request<wire::SourceJournalStatusRequest>,
    ) -> Result<Response<wire::SourceJournalStatus>, Status> {
        source_journal::status(self, request).await
    }
    async fn read_source_journal(
        &self,
        request: Request<wire::SourceJournalReadRequest>,
    ) -> Result<Response<wire::SourceJournalPage>, Status> {
        source_journal::read(self, request).await
    }
    async fn read_routed_source_journal(
        &self,
        request: Request<wire::RoutedSourceJournalReadRequest>,
    ) -> Result<Response<wire::RoutedSourceJournalPage>, Status> {
        definition_coordination::read_routed_source_journal(self, request).await
    }
    async fn apply_derived_consumer_checkpoint(
        &self,
        request: Request<wire::ApplyDerivedConsumerCheckpointRequest>,
    ) -> Result<Response<wire::DerivedConsumerCheckpointApplied>, Status> {
        derived_consumer::apply(self, request).await
    }
    async fn apply_definition_assignment_page(
        &self,
        request: Request<wire::ApplyDefinitionAssignmentPageRequest>,
    ) -> Result<Response<wire::DefinitionAssignmentPageApplied>, Status> {
        definition_coordination::apply_definition_assignment_page(self, request).await
    }
    async fn get_definition_checkpoint(
        &self,
        request: Request<wire::DefinitionCheckpointRequest>,
    ) -> Result<Response<wire::DefinitionCheckpointState>, Status> {
        definition_coordination::get_definition_checkpoint(self, request).await
    }
    async fn apply_definition_assignments(
        &self,
        request: Request<wire::ApplyDefinitionAssignmentsRequest>,
    ) -> Result<Response<wire::DefinitionAssignmentPageApplied>, Status> {
        definition_coordination::apply_definition_assignments(self, request).await
    }
    async fn scan_definition_locators_by_bucket(
        &self,
        request: Request<wire::DefinitionLocatorScanRequest>,
    ) -> Result<Response<wire::DefinitionLocatorScanPage>, Status> {
        definition_coordination::scan_definition_locators_by_bucket(self, request).await
    }
    async fn scan_definition_locators_by_kind(
        &self,
        request: Request<wire::DefinitionLocatorKindScanRequest>,
    ) -> Result<Response<wire::DefinitionLocatorScanPage>, Status> {
        definition_coordination::scan_definition_locators_by_kind(self, request).await
    }
    async fn scan_definition_assignments_by_kind(
        &self,
        request: Request<wire::DefinitionAssignmentScanRequest>,
    ) -> Result<Response<wire::DefinitionAssignmentScanPage>, Status> {
        definition_coordination::scan_definition_assignments_by_kind(self, request).await
    }
    async fn small_content_exists(
        &self,
        mut request: Request<wire::ContentRequest>,
    ) -> Result<Response<wire::ExistsResponse>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
        let reference = parse_small_blob(request.get_ref().blob.as_ref())?;
        let metadata = request.metadata().clone();
        let store = self.store.clone();
        let exists = self
            .bounded(&metadata, async move {
                match store.open_blob(&reference).await {
                    Ok(_) => Ok(true),
                    Err(MutationError::BlobNotFound) => Ok(false),
                    Err(error) => Err(map_mutation_error(error)),
                }
            })
            .await?;
        Ok(Response::new(wire::ExistsResponse {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            exists,
        }))
    }
    async fn get_small_content(
        &self,
        mut request: Request<wire::ContentRequest>,
    ) -> Result<Response<Self::GetSmallContentStream>, Status> {
        let peer = request.get_ref().peer.clone();
        // JOINING coordinators proxy immutable reads to ACTIVE owners.
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        let reference = parse_small_blob(request.get_ref().blob.as_ref())?;
        let metadata = request.metadata().clone();
        let store = self.store.clone();
        let mut reader = self
            .bounded(&metadata, async move {
                store
                    .open_blob(&reference)
                    .await
                    .map_err(map_mutation_error)
            })
            .await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        tokio::spawn(async move {
            let mut offset = 0_u64;
            let mut buffer = vec![0_u8; DATA_PEER_FRAME_BYTES];
            loop {
                match reader.read(&mut buffer).await {
                    Ok(0) => {
                        let _ = sender.send(Ok(content_end(offset))).await;
                        break;
                    }
                    Ok(read) => {
                        let frame = content_frame(offset, buffer[..read].to_vec());
                        offset += read as u64;
                        if sender.send(Ok(frame)).await.is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(Err(Status::data_loss(error.to_string()))).await;
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        )))
    }
    async fn put_small_content(
        &self,
        request: Request<Streaming<wire::SmallContentPutFrame>>,
    ) -> Result<Response<wire::ContentStored>, Status> {
        let pin = request
            .extensions()
            .get::<PeerSpkiSha256>()
            .copied()
            .ok_or_else(|| Status::unauthenticated("peer mTLS identity is missing"))?;
        let timeout = effective_timeout(request.metadata(), self.maximum_unary_time);
        let mut stream = request.into_inner();
        let mut identity: Option<BlobRef> = None;
        let mut bytes = Vec::new();
        loop {
            let frame = tokio::time::timeout(timeout, stream.message())
                .await
                .map_err(|_| Status::deadline_exceeded("content stream made no progress"))??
                .ok_or_else(|| {
                    Status::invalid_argument("content stream ended without end frame")
                })?;
            self.authorize_context(frame.peer.as_ref(), pin, PeerRpcKind::DataPlane)?;
            if frame.content.len() > DATA_PEER_FRAME_BYTES {
                return Err(Status::resource_exhausted("content frame exceeds 64 KiB"));
            }
            let frame_identity = parse_small_blob(frame.blob.as_ref())?;
            if let Some(expected) = &identity {
                if expected != &frame_identity {
                    return Err(Status::invalid_argument(
                        "content identity changed within stream",
                    ));
                }
            } else {
                identity = Some(frame_identity);
            }
            if frame.offset != bytes.len() as u64 {
                return Err(Status::invalid_argument(
                    "content frame offset is not contiguous",
                ));
            }
            let next = bytes
                .len()
                .checked_add(frame.content.len())
                .filter(|length| *length <= keldra_store::SMALL_BLOB_MAX_BYTES)
                .ok_or_else(|| Status::resource_exhausted("small content exceeds 64 KiB"))?;
            bytes.reserve(next - bytes.len());
            bytes.extend_from_slice(&frame.content);
            if !frame.end {
                continue;
            }
            let expected = identity.expect("an accepted frame installed its identity");
            if expected.length != bytes.len() as u64
                || expected.hash != *blake3::hash(&bytes).as_bytes()
            {
                return Err(Status::data_loss(
                    "content does not match its immutable identity",
                ));
            }
            tokio::time::timeout(
                timeout,
                self.store.seal_replica_small_copy(&expected, &bytes),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("content store deadline exceeded"))?
            .map_err(map_payload_error)?;
            return Ok(Response::new(wire::ContentStored {
                schema_version: DATA_PEER_SCHEMA_VERSION,
            }));
        }
    }
    async fn get_complete_source(
        &self,
        mut request: Request<wire::ContentRequest>,
    ) -> Result<Response<Self::GetCompleteSourceStream>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        let reference = parse_blob(request.get_ref().blob.as_ref())?;
        require_large_blob(&reference, self.max_blob_bytes)?;
        let metadata = request.metadata().clone();
        let store = self.store.clone();
        let reader = self
            .bounded(&metadata, async move {
                store
                    .open_blob(&reference)
                    .await
                    .map_err(map_mutation_error)
            })
            .await?;
        Ok(Response::new(stream_blob(reader)))
    }
    async fn put_complete_source(
        &self,
        request: Request<Streaming<wire::CompleteSourcePutFrame>>,
    ) -> Result<Response<wire::CompleteSourceStored>, Status> {
        let pin = request
            .extensions()
            .get::<PeerSpkiSha256>()
            .copied()
            .ok_or_else(|| Status::unauthenticated("peer mTLS identity is missing"))?;
        let idle = effective_timeout(request.metadata(), self.maximum_unary_time);
        let mut stream = request.into_inner();
        let first = next_stream_message(&mut stream, idle, "complete-source stream").await?;
        self.authorize_context(first.peer.as_ref(), pin, PeerRpcKind::DataPlane)?;
        let expected_peer = first.peer.clone();
        let expected_blob = first.blob.clone();
        let expected = parse_blob(expected_blob.as_ref())?;
        require_large_blob(&expected, self.max_blob_bytes)?;
        let mut upload = tokio::time::timeout(idle, self.store.begin_blob_upload())
            .await
            .map_err(|_| Status::deadline_exceeded("complete-source staging made no progress"))?
            .map_err(map_mutation_error)?;
        let mut offset = 0_u64;
        let mut current = Some(first);
        loop {
            let frame = match current.take() {
                Some(frame) => frame,
                None => next_stream_message(&mut stream, idle, "complete-source stream").await?,
            };
            self.authorize_context(frame.peer.as_ref(), pin, PeerRpcKind::DataPlane)?;
            if frame.peer != expected_peer || frame.blob != expected_blob {
                return Err(Status::invalid_argument(
                    "complete-source identity changed within stream",
                ));
            }
            validate_stream_frame(offset, &frame.content, frame.offset, frame.end)?;
            offset = offset
                .checked_add(frame.content.len() as u64)
                .filter(|offset| *offset <= expected.length)
                .ok_or_else(|| {
                    Status::resource_exhausted("complete-source bytes exceed declared length")
                })?;
            tokio::time::timeout(idle, upload.write(&frame.content))
                .await
                .map_err(|_| Status::deadline_exceeded("complete-source staging made no progress"))?
                .map_err(|error| Status::internal(error.to_string()))?;
            if !frame.end {
                continue;
            }
            if offset != expected.length {
                return Err(Status::data_loss(
                    "complete-source stream ended before its declared length",
                ));
            }
            let outcome = tokio::time::timeout(
                idle,
                self.store
                    .seal_replica_complete_source_upload(&expected, upload),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("complete-source seal made no progress"))?
            .map_err(map_payload_error)?;
            return Ok(Response::new(wire::CompleteSourceStored {
                schema_version: DATA_PEER_SCHEMA_VERSION,
                already_present: outcome == CompleteCopySealOutcome::AlreadyPresent,
            }));
        }
    }
    async fn shard_exists(
        &self,
        mut request: Request<wire::ShardRequest>,
    ) -> Result<Response<wire::ExistsResponse>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
        let metadata = request.metadata().clone();
        let identity = parse_shard(&request.into_inner())?;
        require_large_blob(identity.blob(), self.max_blob_bytes)?;
        let store = self.store.clone();
        let codec = self.codec.clone();
        let exists = self
            .bounded(&metadata, async move {
                tokio::task::spawn_blocking(move || match store.get_shard(&codec, &identity) {
                    Ok(_) => Ok(true),
                    Err(ShardStoreError::NotFound) => Ok(false),
                    Err(error) => Err(map_shard_error(error)),
                })
                .await
                .map_err(|error| Status::internal(format!("join shard existence check: {error}")))?
            })
            .await?;
        Ok(Response::new(wire::ExistsResponse {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            exists,
        }))
    }
    async fn get_shard(
        &self,
        mut request: Request<wire::ShardRequest>,
    ) -> Result<Response<Self::GetShardStream>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        let metadata = request.metadata().clone();
        let identity = parse_shard(&request.into_inner())?;
        require_large_blob(identity.blob(), self.max_blob_bytes)?;
        let store = self.store.clone();
        let codec = self.codec.clone();
        let mut reader = self
            .bounded(&metadata, async move {
                tokio::task::spawn_blocking(move || store.get_shard(&codec, &identity))
                    .await
                    .map_err(|error| Status::internal(format!("join shard open: {error}")))?
                    .map_err(map_shard_error)
            })
            .await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        tokio::task::spawn_blocking(move || {
            let mut offset = 0_u64;
            let mut buffer = vec![0_u8; DATA_PEER_FRAME_BYTES];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => {
                        let _ = sender.blocking_send(Ok(content_end(offset)));
                        break;
                    }
                    Ok(read) => {
                        let frame = content_frame(offset, buffer[..read].to_vec());
                        offset += read as u64;
                        if sender.blocking_send(Ok(frame)).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = sender.blocking_send(Err(Status::data_loss(error.to_string())));
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        )))
    }

    async fn put_shard(
        &self,
        request: Request<Streaming<wire::ShardPutFrame>>,
    ) -> Result<Response<wire::ShardStored>, Status> {
        let pin = request
            .extensions()
            .get::<PeerSpkiSha256>()
            .copied()
            .ok_or_else(|| Status::unauthenticated("peer mTLS identity is missing"))?;
        let idle = effective_timeout(request.metadata(), self.maximum_unary_time);
        let mut stream = request.into_inner();
        let first = next_stream_message(&mut stream, idle, "shard stream").await?;
        let expected_request = first
            .shard
            .clone()
            .ok_or_else(|| Status::invalid_argument("shard identity is required"))?;
        self.authorize_context(expected_request.peer.as_ref(), pin, PeerRpcKind::DataPlane)?;
        let identity = parse_shard(&expected_request)?;
        require_large_blob(identity.blob(), self.max_blob_bytes)?;
        let expected_length = self
            .codec
            .encoded_shard_length(identity.blob(), identity.ordinal())
            .map_err(|error| map_shard_error(error.into()))?;

        let (mut sender, receiver) = tokio::io::duplex(DATA_PEER_FRAME_BYTES * 2);
        let store = self.store.clone();
        let codec = self.codec.clone();
        let seal_identity = identity.clone();
        let seal = tokio::spawn(async move {
            store
                .seal_replica_shard_stream(&codec, &seal_identity, receiver)
                .await
        });

        let transfer = async {
            let mut offset = 0_u64;
            let mut current = Some(first);
            loop {
                let frame = match current.take() {
                    Some(frame) => frame,
                    None => next_stream_message(&mut stream, idle, "shard stream").await?,
                };
                let shard = frame
                    .shard
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("shard identity is required"))?;
                self.authorize_context(shard.peer.as_ref(), pin, PeerRpcKind::DataPlane)?;
                if shard != &expected_request {
                    return Err(Status::invalid_argument(
                        "shard identity changed within stream",
                    ));
                }
                validate_stream_frame(offset, &frame.content, frame.offset, frame.end)?;
                offset = offset
                    .checked_add(frame.content.len() as u64)
                    .filter(|offset| *offset <= expected_length)
                    .ok_or_else(|| {
                        Status::resource_exhausted("shard bytes exceed their encoded length")
                    })?;
                if !frame.content.is_empty() {
                    tokio::time::timeout(idle, sender.write_all(&frame.content))
                        .await
                        .map_err(|_| Status::deadline_exceeded("shard staging made no progress"))?
                        .map_err(|error| {
                            Status::internal(format!("shard staging stopped unexpectedly: {error}"))
                        })?;
                }
                if !frame.end {
                    continue;
                }
                if offset != expected_length {
                    return Err(Status::data_loss(
                        "shard stream ended before its encoded length",
                    ));
                }
                tokio::time::timeout(idle, sender.shutdown())
                    .await
                    .map_err(|_| Status::deadline_exceeded("shard staging made no progress"))?
                    .map_err(|error| {
                        Status::internal(format!("shard staging stopped unexpectedly: {error}"))
                    })?;
                return Ok(());
            }
        }
        .await;

        if let Err(status) = transfer {
            drop(sender);
            let _ = seal.await;
            return Err(status);
        }
        drop(sender);
        let outcome = seal
            .await
            .map_err(|error| Status::internal(format!("join shard seal: {error}")))?
            .map_err(map_shard_error)?;
        Ok(Response::new(wire::ShardStored {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            already_present: outcome == ShardSealOutcome::AlreadyPresent,
        }))
    }

    async fn export_object_records(
        &self,
        request: Request<wire::HandoffPageRequest>,
    ) -> Result<Response<wire::HandoffPage>, Status> {
        handoff::export_object_records(self, request).await
    }

    async fn read_handoff_object_path_snapshot(
        &self,
        request: Request<wire::HandoffObjectPathSnapshotRequest>,
    ) -> Result<Response<wire::ObjectPathSnapshotResponse>, Status> {
        handoff::read_object_path_snapshot(self, request).await
    }

    async fn repair_handoff_object_path_snapshot(
        &self,
        request: Request<wire::RepairHandoffObjectPathSnapshotRequest>,
    ) -> Result<Response<wire::ObjectPathSnapshotApplied>, Status> {
        handoff::repair_object_path_snapshot(self, request).await
    }

    async fn get_handoff_source_journal_status(
        &self,
        request: Request<wire::HandoffSourceJournalStatusRequest>,
    ) -> Result<Response<wire::SourceJournalStatus>, Status> {
        handoff::source_journal_status(self, request).await
    }

    async fn complete_system_bootstrap_handoff(
        &self,
        request: Request<wire::CompleteSystemBootstrapHandoffRequest>,
    ) -> Result<Response<wire::HandoffRecordApplied>, Status> {
        handoff::complete_system_bootstrap(self, request).await
    }

    async fn read_handoff_source_journal(
        &self,
        request: Request<wire::HandoffSourceJournalReadRequest>,
    ) -> Result<Response<wire::SourceJournalPage>, Status> {
        handoff::read_source_journal(self, request).await
    }

    async fn get_handoff_reference_cursor(
        &self,
        request: Request<wire::HandoffReferenceCursorRequest>,
    ) -> Result<Response<wire::ReferenceDeltaStatus>, Status> {
        handoff::reference_cursor(self, request).await
    }

    async fn advance_handoff_reference_cursor(
        &self,
        request: Request<wire::HandoffReferenceCursorAdvanceRequest>,
    ) -> Result<Response<wire::ReferenceDeltaApplied>, Status> {
        handoff::advance_reference_cursor(self, request).await
    }

    async fn install_object_record(
        &self,
        request: Request<wire::HandoffRecordRequest>,
    ) -> Result<Response<wire::HandoffRecordApplied>, Status> {
        handoff::install_object_record(self, request).await
    }

    async fn export_logical_records(
        &self,
        request: Request<wire::HandoffPageRequest>,
    ) -> Result<Response<wire::HandoffPage>, Status> {
        handoff::export_logical_records(self, request).await
    }

    async fn install_logical_record(
        &self,
        request: Request<wire::HandoffRecordRequest>,
    ) -> Result<Response<wire::HandoffRecordApplied>, Status> {
        handoff::install_logical_record(self, request).await
    }

    async fn read_logical_record(
        &self,
        request: Request<wire::LogicalRecordRequest>,
    ) -> Result<Response<wire::LogicalRecordResponse>, Status> {
        handoff::read_logical_record(self, request).await
    }

    async fn repair_logical_record(
        &self,
        request: Request<wire::RepairLogicalRecordRequest>,
    ) -> Result<Response<wire::HandoffRecordApplied>, Status> {
        handoff::repair_logical_record(self, request).await
    }

    async fn export_authz_realm_keys(
        &self,
        request: Request<wire::HandoffPageRequest>,
    ) -> Result<Response<wire::HandoffPage>, Status> {
        handoff::export_authz_realm_keys(self, request).await
    }

    async fn read_authz_schema_catalogue(
        &self,
        request: Request<wire::AuthzSchemaCatalogueRequest>,
    ) -> Result<Response<wire::AuthzSchemaCatalogueResponse>, Status> {
        handoff::read_authz_schema_catalogue(self, request).await
    }

    async fn repair_authz_schema_catalogue(
        &self,
        request: Request<wire::RepairAuthzSchemaCatalogueRequest>,
    ) -> Result<Response<wire::HandoffRecordApplied>, Status> {
        handoff::repair_authz_schema_catalogue(self, request).await
    }

    async fn read_authz_realm_manifest(
        &self,
        request: Request<wire::AuthzRealmRequest>,
    ) -> Result<Response<wire::AuthzRealmManifest>, Status> {
        handoff::read_authz_realm_manifest(self, request).await
    }

    async fn repair_authz_realm_absence(
        &self,
        request: Request<wire::AuthzRealmRequest>,
    ) -> Result<Response<wire::HandoffRecordApplied>, Status> {
        handoff::repair_authz_realm_absence(self, request).await
    }

    async fn get_authz_realm(
        &self,
        request: Request<wire::AuthzRealmRequest>,
    ) -> Result<Response<Self::GetAuthzRealmStream>, Status> {
        handoff::get_authz_realm(self, request).await
    }

    async fn put_authz_realm(
        &self,
        request: Request<Streaming<wire::AuthzRealmPutFrame>>,
    ) -> Result<Response<wire::HandoffRecordApplied>, Status> {
        handoff::put_authz_realm(self, request).await
    }

    async fn export_payload_artifacts(
        &self,
        request: Request<wire::HandoffPageRequest>,
    ) -> Result<Response<wire::HandoffPage>, Status> {
        handoff::export_payload_artifacts(self, request).await
    }

    async fn install_payload_lifecycle(
        &self,
        request: Request<wire::HandoffRecordRequest>,
    ) -> Result<Response<wire::HandoffRecordApplied>, Status> {
        handoff::install_payload_lifecycle(self, request).await
    }
}
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::sync::{Arc, RwLock};

    use keldra_consensus::{
        CommittedPeerPins, NodeState, PeerTlsAcceptor, PeerTlsConfig, PeerTlsIdentity,
    };
    use keldra_store::StoreOptions;
    use tonic::Code;
    use tonic::codegen::tokio_stream::StreamExt;
    use tonic::transport::Server;
    use tonic::transport::server::TcpIncoming;

    use super::*;
    use crate::node_identity;

    struct TestPins {
        cluster_id: ClusterId,
        nodes: RwLock<BTreeMap<NodeId, (CommittedPeerPins, NodeState)>>,
    }

    impl TestPins {
        fn new(cluster_id: ClusterId) -> Self {
            Self {
                cluster_id,
                nodes: RwLock::new(BTreeMap::new()),
            }
        }

        fn install(&self, node_id: NodeId, pin: PeerSpkiSha256, state: NodeState) {
            self.nodes.write().unwrap().insert(
                node_id,
                (
                    CommittedPeerPins {
                        current: pin,
                        overlap: None,
                    },
                    state,
                ),
            );
        }

        fn set_state(&self, node_id: NodeId, state: NodeState) {
            self.nodes.write().unwrap().get_mut(&node_id).unwrap().1 = state;
        }

        fn remove(&self, node_id: NodeId) {
            self.nodes.write().unwrap().remove(&node_id);
        }
    }

    impl CommittedPeerPinProvider for TestPins {
        fn connection_pins(&self, node_id: NodeId) -> Option<CommittedPeerPins> {
            self.nodes.read().ok()?.get(&node_id).map(|(pins, _)| *pins)
        }

        fn authorized_rpc_pins(
            &self,
            cluster_id: ClusterId,
            node_id: NodeId,
            kind: PeerRpcKind,
        ) -> Option<CommittedPeerPins> {
            if cluster_id != self.cluster_id {
                return None;
            }
            let nodes = self.nodes.read().ok()?;
            let (pins, state) = nodes.get(&node_id)?;
            let allowed = match kind {
                PeerRpcKind::JoinControl => matches!(state, NodeState::Active | NodeState::Joining),
                _ => *state == NodeState::Active,
            };
            allowed.then_some(*pins)
        }
    }

    fn identity(cluster_id: ClusterId, node_id: NodeId) -> Arc<PeerTlsIdentity> {
        let identity = node_identity::generate(cluster_id, node_id).unwrap();
        let peer = identity.presented_peer_identity();
        Arc::new(
            PeerTlsIdentity::from_pem(
                peer.certificate_pem().as_bytes(),
                peer.private_key_pem().as_bytes(),
            )
            .unwrap(),
        )
    }

    async fn start_server(
        identity: Arc<PeerTlsIdentity>,
        pins: Arc<TestPins>,
        store: Store,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = PeerTlsAcceptor::new(&identity, PeerTlsConfig::default()).unwrap();
        let incoming = TcpIncoming::from(listener)
            .then(move |stream| {
                let acceptor = acceptor.clone();
                async move {
                    let stream = stream.map_err(PeerTlsError::Io)?;
                    acceptor.accept(stream).await
                }
            })
            .filter_map(|result| result.ok().map(Ok::<_, std::io::Error>));
        let service = DataPeerService::new_test(
            store,
            pins.clone(),
            pins.cluster_id,
            NodeId(1),
            [NodeId(1), NodeId(2)],
            ErasureProfile::default(),
            Duration::from_secs(30),
            16 * 1024 * 1024,
        )
        .unwrap()
        .into_server();
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        (address, shutdown, task)
    }

    async fn collect_content(mut stream: Streaming<wire::ContentFrame>) -> Vec<u8> {
        let mut bytes = Vec::new();
        while let Some(frame) = stream.message().await.unwrap() {
            assert_eq!(frame.schema_version, DATA_PEER_SCHEMA_VERSION);
            assert_eq!(frame.offset, bytes.len() as u64);
            bytes.extend_from_slice(&frame.content);
            if frame.end {
                return bytes;
            }
        }
        panic!("peer content stream ended without an end frame");
    }

    fn shard_frame(
        transport: &DataPeerTransport,
        identity: &ShardIdentity,
        offset: u64,
        content: &[u8],
        end: bool,
    ) -> wire::ShardPutFrame {
        wire::ShardPutFrame {
            shard: Some(wire_shard(transport.context(), identity)),
            offset,
            content: content.to_vec(),
            end,
        }
    }

    #[test]
    fn joining_callers_have_only_join_control_authority() {
        let cluster_id = ClusterId(*b"join-control-tst");
        let pins = TestPins::new(cluster_id);
        pins.install(NodeId(2), PeerSpkiSha256([7; 32]), NodeState::Joining);
        assert!(
            pins.authorized_rpc_pins(cluster_id, NodeId(2), PeerRpcKind::JoinControl)
                .is_some()
        );
        for denied in [
            PeerRpcKind::AppendEntries,
            PeerRpcKind::Vote,
            PeerRpcKind::InstallSnapshot,
            PeerRpcKind::ServingLease,
            PeerRpcKind::DataPlane,
            PeerRpcKind::StateTransfer,
        ] {
            assert!(
                pins.authorized_rpc_pins(cluster_id, NodeId(2), denied)
                    .is_none(),
                "JOINING caller received {denied:?} authority"
            );
        }
    }

    async fn assert_joining_denied_by_every_rpc(transport: &DataPeerTransport, address: &str) {
        let mut client = transport.client(NodeId(1), address).unwrap();
        let peer = transport.context();
        let typed = || wire::TypedMutationRequest {
            peer: Some(peer.clone()),
            mutation_json: Vec::new(),
        };
        let page = || wire::HandoffPageRequest {
            peer: Some(peer.clone()),
            cursor_json: Vec::new(),
            max_records: 0,
            max_bytes: 0,
            handoff: None,
        };
        let record = || wire::HandoffRecordRequest {
            peer: Some(peer.clone()),
            record_json: Vec::new(),
            handoff: None,
        };
        let content = || wire::ContentRequest {
            peer: Some(peer.clone()),
            blob: None,
        };
        let shard = || wire::ShardRequest {
            peer: Some(peer.clone()),
            fragment_format_version: 0,
            blob: None,
            ordinal: 0,
        };
        let realm = || wire::AuthzRealmRequest {
            peer: Some(peer.clone()),
            scope_json: Vec::new(),
            handoff: None,
        };
        let catalogue = || wire::AuthzSchemaCatalogueRequest {
            peer: Some(peer.clone()),
            storage_tenant: "tenant".into(),
            handoff: None,
        };
        let mut denied = 0_usize;
        macro_rules! require_denied {
            ($operation:expr, $name:literal) => {
                match $operation.await {
                    Err(status) => {
                        assert_eq!(
                            status.code(),
                            Code::PermissionDenied,
                            "{} returned {status}",
                            $name
                        );
                        denied += 1;
                    }
                    Ok(_) => panic!("{} accepted a JOINING caller", $name),
                }
            };
        }

        let d = || wire::MutationDrainRequest {
            peer: Some(peer.clone()),
            handoff: None,
        };
        require_denied!(client.drain_mutations(d()), "DrainMutations");
        require_denied!(client.release_mutation_drain(d()), "ReleaseMutationDrain");
        require_denied!(client.apply_object_mutation(typed()), "ApplyObjectMutation");
        require_denied!(
            client.read_object_path_snapshot(wire::ObjectPathSnapshotRequest {
                peer: Some(peer.clone()),
                tenant_id: 0,
                bucket_id: 0,
                exact_path: String::new(),
            }),
            "ReadObjectPathSnapshot"
        );
        require_denied!(
            client.read_object_path_snapshots(wire::ObjectPathSnapshotBatchRequest {
                peer: Some(peer.clone()),
                tenant_id: 0,
                bucket_id: 0,
                exact_paths: Vec::new(),
            }),
            "ReadObjectPathSnapshots"
        );
        require_denied!(
            client.read_current_object_snapshot(wire::ObjectPathSnapshotRequest {
                peer: Some(peer.clone()),
                tenant_id: 0,
                bucket_id: 0,
                exact_path: String::new(),
            }),
            "ReadCurrentObjectSnapshot"
        );
        require_denied!(
            client.read_current_object_snapshots(wire::CurrentObjectSnapshotBatchRequest {
                peer: Some(peer.clone()),
                tenant_id: 0,
                bucket_id: 0,
                exact_paths: Vec::new(),
            }),
            "ReadCurrentObjectSnapshots"
        );
        require_denied!(
            client.repair_object_path_snapshot(wire::RepairObjectPathSnapshotRequest {
                peer: Some(peer.clone()),
                tenant_id: 0,
                bucket_id: 0,
                exact_path: String::new(),
                expected_snapshot_json: Vec::new(),
                selected_snapshot_json: Vec::new(),
                placement_fence_term: 0,
                placement_fence_index: 0,
            }),
            "RepairObjectPathSnapshot"
        );
        require_denied!(
            client.apply_authz_realm_mutation(typed()),
            "ApplyAuthzRealmMutation"
        );
        require_denied!(
            client.apply_reference_deltas(typed()),
            "ApplyReferenceDeltas"
        );
        require_denied!(
            client.get_reference_delta_status(wire::ReferenceDeltaStatusRequest {
                peer: Some(peer.clone()),
                source_id_json: Vec::new(),
            }),
            "GetReferenceDeltaStatus"
        );
        require_denied!(
            client.get_source_journal_status(wire::SourceJournalStatusRequest {
                peer: Some(peer.clone()),
            }),
            "GetSourceJournalStatus"
        );
        require_denied!(
            client.read_source_journal(wire::SourceJournalReadRequest {
                peer: Some(peer.clone()),
                after_offset: 0,
                limit: 0,
                max_bytes: 1,
            }),
            "ReadSourceJournal"
        );
        definition_coordination::denied_test_calls!(client, peer, require_denied);
        derived_consumer::denied_test_call!(client, peer, require_denied);
        require_denied!(client.small_content_exists(content()), "SmallContentExists");
        require_denied!(client.get_small_content(content()), "GetSmallContent");
        require_denied!(
            client.put_small_content(tokio_stream::iter([wire::SmallContentPutFrame {
                peer: Some(peer.clone()),
                blob: None,
                offset: 0,
                content: Vec::new(),
                end: true,
            }])),
            "PutSmallContent"
        );
        require_denied!(client.get_complete_source(content()), "GetCompleteSource");
        require_denied!(
            client.put_complete_source(tokio_stream::iter([wire::CompleteSourcePutFrame {
                peer: Some(peer.clone()),
                blob: None,
                offset: 0,
                content: Vec::new(),
                end: true,
            }])),
            "PutCompleteSource"
        );
        require_denied!(client.shard_exists(shard()), "ShardExists");
        require_denied!(client.get_shard(shard()), "GetShard");
        require_denied!(
            client.put_shard(tokio_stream::iter([wire::ShardPutFrame {
                shard: Some(shard()),
                offset: 0,
                content: Vec::new(),
                end: true,
            }])),
            "PutShard"
        );
        require_denied!(client.export_object_records(page()), "ExportObjectRecords");
        require_denied!(
            client.install_object_record(record()),
            "InstallObjectRecord"
        );
        require_denied!(
            client.read_handoff_object_path_snapshot(wire::HandoffObjectPathSnapshotRequest {
                peer: Some(peer.clone()),
                handoff: None,
                tenant_id: 0,
                bucket_id: 0,
                exact_path: String::new(),
            }),
            "ReadHandoffObjectPathSnapshot"
        );
        require_denied!(
            client.repair_handoff_object_path_snapshot(
                wire::RepairHandoffObjectPathSnapshotRequest {
                    peer: Some(peer.clone()),
                    handoff: None,
                    tenant_id: 0,
                    bucket_id: 0,
                    exact_path: String::new(),
                    expected_snapshot_json: Vec::new(),
                    selected_snapshot_json: Vec::new(),
                }
            ),
            "RepairHandoffObjectPathSnapshot"
        );
        require_denied!(
            client.get_handoff_source_journal_status(wire::HandoffSourceJournalStatusRequest {
                peer: Some(peer.clone()),
                handoff: None,
            }),
            "GetHandoffSourceJournalStatus"
        );
        require_denied!(
            client.complete_system_bootstrap_handoff(wire::CompleteSystemBootstrapHandoffRequest {
                peer: Some(peer.clone()),
                handoff: None,
            }),
            "CompleteSystemBootstrapHandoff"
        );
        require_denied!(
            client.read_handoff_source_journal(wire::HandoffSourceJournalReadRequest {
                peer: Some(peer.clone()),
                handoff: None,
                after_offset: 0,
                limit: 0,
                max_bytes: 1,
            }),
            "ReadHandoffSourceJournal"
        );
        require_denied!(
            client.get_handoff_reference_cursor(wire::HandoffReferenceCursorRequest {
                peer: Some(peer.clone()),
                handoff: None,
                source_id_json: Vec::new(),
            }),
            "GetHandoffReferenceCursor"
        );
        require_denied!(
            client.advance_handoff_reference_cursor(wire::HandoffReferenceCursorAdvanceRequest {
                peer: Some(peer.clone()),
                handoff: None,
                source_id_json: Vec::new(),
                through: 0,
            }),
            "AdvanceHandoffReferenceCursor"
        );
        require_denied!(
            client.export_logical_records(page()),
            "ExportLogicalRecords"
        );
        require_denied!(
            client.install_logical_record(record()),
            "InstallLogicalRecord"
        );
        require_denied!(
            client.read_logical_record(wire::LogicalRecordRequest {
                peer: Some(peer.clone()),
                id_json: Vec::new(),
                handoff: None,
            }),
            "ReadLogicalRecord"
        );
        require_denied!(
            client.repair_logical_record(wire::RepairLogicalRecordRequest {
                peer: Some(peer.clone()),
                id_json: Vec::new(),
                present: false,
                candidate_json: Vec::new(),
                handoff: None,
            }),
            "RepairLogicalRecord"
        );
        require_denied!(
            client.export_authz_realm_keys(page()),
            "ExportAuthzRealmKeys"
        );
        require_denied!(
            client.read_authz_schema_catalogue(catalogue()),
            "ReadAuthzSchemaCatalogue"
        );
        require_denied!(
            client.repair_authz_schema_catalogue(wire::RepairAuthzSchemaCatalogueRequest {
                peer: Some(peer.clone()),
                storage_tenant: "tenant".into(),
                present: false,
                catalogue_json: Vec::new(),
                handoff: None,
            }),
            "RepairAuthzSchemaCatalogue"
        );
        require_denied!(
            client.read_authz_realm_manifest(realm()),
            "ReadAuthzRealmManifest"
        );
        require_denied!(
            client.repair_authz_realm_absence(realm()),
            "RepairAuthzRealmAbsence"
        );
        require_denied!(client.get_authz_realm(realm()), "GetAuthzRealm");
        require_denied!(
            client.put_authz_realm(tokio_stream::iter([wire::AuthzRealmPutFrame {
                peer: Some(peer.clone()),
                offset: 0,
                content: Vec::new(),
                end: true,
                manifest_json: Vec::new(),
                handoff: None,
            }])),
            "PutAuthzRealm"
        );
        require_denied!(
            client.export_payload_artifacts(page()),
            "ExportPayloadArtifacts"
        );
        require_denied!(
            client.install_payload_lifecycle(record()),
            "InstallPayloadLifecycle"
        );
        assert_eq!(
            denied, 51,
            "the DataPeer RPC list changed without updating this test"
        );
    }

    #[tokio::test]
    async fn real_mtls_binds_claimed_node_and_rechecks_rpc_class_and_membership() {
        let cluster_id = ClusterId(*b"data-peer-test01");
        let server_id = identity(cluster_id, NodeId(1));
        let joining_id = identity(cluster_id, NodeId(2));
        let wrong_id = identity(cluster_id, NodeId(3));
        let pins = Arc::new(TestPins::new(cluster_id));
        pins.install(NodeId(1), server_id.spki_sha256(), NodeState::Active);
        pins.install(NodeId(2), joining_id.spki_sha256(), NodeState::Joining);

        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        let (address, shutdown, server) =
            start_server(server_id, pins.clone(), store.clone()).await;
        let address = address.to_string();
        let joining = DataPeerTransport::new(
            cluster_id,
            NodeId(2),
            PeerTlsConnector::new(joining_id.clone(), pins.clone(), PeerTlsConfig::default())
                .unwrap(),
        )
        .unwrap();

        assert_joining_denied_by_every_rpc(&joining, &address).await;

        pins.set_state(NodeId(2), NodeState::Active);
        let status = joining
            .source_journal_status(NodeId(1), &address)
            .await
            .unwrap();
        assert_eq!(status.source_id.node_id, 1);
        let spoofed = ReferenceDeltaBatch {
            source: status.source_id,
            after: 0,
            through: 0,
            deltas: Vec::new(),
        };
        let denied = joining
            .apply_reference_deltas(NodeId(1), &address, &spoofed)
            .await
            .unwrap_err();
        assert_eq!(denied.code(), Code::PermissionDenied);
        let client_source = SourceId {
            node_id: 2,
            source_epoch: [2; 32],
        };
        let empty = ReferenceDeltaBatch {
            source: client_source,
            after: 0,
            through: 0,
            deltas: Vec::new(),
        };
        let applied = joining
            .apply_reference_deltas(NodeId(1), &address, &empty)
            .await
            .unwrap();
        assert_eq!(applied.through, 0);
        assert_eq!(
            joining
                .reference_delta_status(NodeId(1), &address, client_source)
                .await
                .unwrap(),
            0
        );
        let journal = joining
            .source_journal_status(NodeId(1), &address)
            .await
            .unwrap();
        assert!(
            joining
                .read_source_journal(
                    NodeId(1),
                    &address,
                    journal.source_id,
                    0,
                    16,
                    MAX_TYPED_MUTATION_BYTES as u64,
                )
                .await
                .unwrap()
                .changes
                .is_empty()
        );

        let mut raw_active = joining.client(NodeId(1), &address).unwrap();
        let invalid = raw_active
            .export_object_records(wire::HandoffPageRequest {
                peer: Some(joining.context()),
                cursor_json: Vec::new(),
                max_records: 0,
                max_bytes: 1,
                handoff: None,
            })
            .await
            .unwrap_err();
        assert_eq!(invalid.code(), Code::InvalidArgument);
        let oversized = raw_active
            .install_object_record(wire::HandoffRecordRequest {
                peer: Some(joining.context()),
                record_json: vec![0; MAX_TYPED_MUTATION_BYTES + 1],
                handoff: None,
            })
            .await
            .unwrap_err();
        assert_eq!(oversized.code(), Code::InvalidArgument);

        let tenant_id = 11;
        let bucket_id = 22;
        let exact_path = "peer-snapshot";
        let placement_fence = keldra_store::PlacementLogId { term: 1, index: 1 };
        let snapshot = Some(ObjectPathSnapshot {
            tenant_id,
            bucket_id,
            exact_path: exact_path.into(),
            head: keldra_store::Head {
                version: keldra_store::VersionId(1),
                deleted: true,
                mutation_stamp: None,
            },
            versions: vec![keldra_store::Version {
                id: keldra_store::VersionId(1),
                blob: None,
                content_type: None,
                deleted: true,
                committed_at_unix_millis: 1,
                protected_link_descriptor: false,
            }],
            journal_pending_versions: Vec::new(),
            journal_released_versions: Vec::new(),
            definition_locator: None,
            alias_registry: None,
            alias_registry_transition: None,
        });
        store
            .repair_object_path_snapshot(tenant_id, bucket_id, exact_path, None, snapshot.as_ref())
            .await
            .unwrap();
        assert_eq!(
            joining
                .read_object_path_snapshot(NodeId(1), &address, tenant_id, bucket_id, exact_path,)
                .await
                .unwrap(),
            snapshot
        );
        let current = joining
            .read_current_object_snapshot(NodeId(1), &address, tenant_id, bucket_id, exact_path)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.exact_path, exact_path);
        assert_eq!(current.head, snapshot.as_ref().unwrap().head);
        assert_eq!(current.version, snapshot.as_ref().unwrap().versions[0]);
        let stale = joining
            .repair_object_path_snapshot(
                NodeId(1),
                &address,
                keldra_store::PlacementLogId { term: 1, index: 0 },
                tenant_id,
                bucket_id,
                exact_path,
                snapshot.as_ref(),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(stale.code(), Code::Unavailable);
        joining
            .repair_object_path_snapshot(
                NodeId(1),
                &address,
                placement_fence,
                tenant_id,
                bucket_id,
                exact_path,
                snapshot.as_ref(),
                None,
            )
            .await
            .unwrap();
        assert!(
            store
                .export_object_path_record(tenant_id, bucket_id, exact_path)
                .unwrap()
                .is_none()
        );
        joining
            .repair_object_path_snapshot(
                NodeId(1),
                &address,
                placement_fence,
                tenant_id,
                bucket_id,
                exact_path,
                None,
                snapshot.as_ref(),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .export_object_path_record(tenant_id, bucket_id, exact_path)
                .unwrap(),
            snapshot
        );

        let bytes = b"typed data peer over real mutual TLS";
        let reference = BlobRef {
            hash: *blake3::hash(bytes).as_bytes(),
            length: bytes.len() as u64,
        };
        joining
            .put_small_content(NodeId(1), &address, &reference, bytes)
            .await
            .unwrap();
        assert!(
            joining
                .small_content_exists(NodeId(1), &address, &reference)
                .await
                .unwrap()
        );
        assert_eq!(
            joining
                .get_small_content(NodeId(1), &address, &reference)
                .await
                .unwrap(),
            bytes
        );
        let error = joining
            .shard_exists(
                NodeId(1),
                &address,
                &ShardIdentity::new(reference.clone(), 0),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);

        let mismatched = DataPeerTransport::new(
            cluster_id,
            NodeId(2),
            PeerTlsConnector::new(wrong_id, pins.clone(), PeerTlsConfig::default()).unwrap(),
        )
        .unwrap();
        let denied = mismatched
            .source_journal_status(NodeId(1), &address)
            .await
            .unwrap_err();
        assert_eq!(denied.code(), Code::PermissionDenied);

        let wrong_cluster = DataPeerTransport::new(
            ClusterId(*b"other-cluster-id"),
            NodeId(2),
            PeerTlsConnector::new(joining_id.clone(), pins.clone(), PeerTlsConfig::default())
                .unwrap(),
        )
        .unwrap();
        let denied = wrong_cluster
            .source_journal_status(NodeId(1), &address)
            .await
            .unwrap_err();
        assert_eq!(denied.code(), Code::PermissionDenied);

        let wrong_node = DataPeerTransport::new(
            cluster_id,
            NodeId(99),
            PeerTlsConnector::new(joining_id, pins.clone(), PeerTlsConfig::default()).unwrap(),
        )
        .unwrap();
        let denied = wrong_node
            .source_journal_status(NodeId(1), &address)
            .await
            .unwrap_err();
        assert_eq!(denied.code(), Code::PermissionDenied);

        let mut wrong_schema = joining.client(NodeId(1), &address).unwrap();
        let denied = wrong_schema
            .get_source_journal_status(wire::SourceJournalStatusRequest {
                peer: Some(wire::PeerContext {
                    schema_version: DATA_PEER_SCHEMA_VERSION + 1,
                    ..joining.context()
                }),
            })
            .await
            .unwrap_err();
        assert_eq!(denied.code(), Code::FailedPrecondition);

        pins.remove(NodeId(2));
        let denied = joining
            .source_journal_status(NodeId(1), &address)
            .await
            .unwrap_err();
        assert_eq!(denied.code(), Code::PermissionDenied);

        let _ = shutdown.send(());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn real_mtls_large_source_and_shard_streams_are_exact_and_restart_safe() {
        let cluster_id = ClusterId(*b"peer-payload-tst");
        let server_id = identity(cluster_id, NodeId(1));
        let client_id = identity(cluster_id, NodeId(2));
        let pins = Arc::new(TestPins::new(cluster_id));
        pins.install(NodeId(1), server_id.spki_sha256(), NodeState::Active);
        pins.install(NodeId(2), client_id.spki_sha256(), NodeState::Active);

        let source_directory = tempfile::tempdir().unwrap();
        let destination_directory = tempfile::tempdir().unwrap();
        let destination_root = destination_directory.path().join("store");
        let source_store = Store::open(StoreOptions::new(source_directory.path(), 2))
            .await
            .unwrap();
        let destination = Store::open(StoreOptions::new(&destination_root, 1))
            .await
            .unwrap();
        let source = (0..2 * DATA_PEER_FRAME_BYTES + 333)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let reference = source_store.stage_blob(&source).await.unwrap();
        let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
        let mut shards = vec![Vec::new(); usize::from(codec.profile().total_shards())];
        source_store
            .encode_sealed_source(&codec, &reference, &mut shards)
            .await
            .unwrap();

        let (source_address, source_shutdown, source_server) =
            start_server(client_id.clone(), pins.clone(), source_store.clone()).await;
        let source_address = source_address.to_string();
        let (address, shutdown, server) =
            start_server(server_id, pins.clone(), destination.clone()).await;
        let address = address.to_string();
        let transport = DataPeerTransport::new(
            cluster_id,
            NodeId(2),
            PeerTlsConnector::new(client_id, pins, PeerTlsConfig::default()).unwrap(),
        )
        .unwrap();

        assert_eq!(
            transport
                .copy_complete_source(NodeId(2), &source_address, NodeId(1), &address, &reference,)
                .await
                .unwrap(),
            CompleteCopySealOutcome::Created
        );
        assert_eq!(
            transport
                .copy_complete_source(NodeId(2), &source_address, NodeId(1), &address, &reference,)
                .await
                .unwrap(),
            CompleteCopySealOutcome::AlreadyPresent
        );

        let source_reader = source_store.open_blob(&reference).await.unwrap();
        assert_eq!(
            transport
                .put_complete_source(NodeId(1), &address, &reference, source_reader)
                .await
                .unwrap(),
            CompleteCopySealOutcome::AlreadyPresent
        );
        let retry_reader = source_store.open_blob(&reference).await.unwrap();
        assert_eq!(
            transport
                .put_complete_source(NodeId(1), &address, &reference, retry_reader)
                .await
                .unwrap(),
            CompleteCopySealOutcome::AlreadyPresent
        );
        assert_eq!(
            collect_content(
                transport
                    .get_complete_source(NodeId(1), &address, &reference)
                    .await
                    .unwrap()
            )
            .await,
            source
        );

        let first = ShardIdentity::new(reference.clone(), 0);
        assert_eq!(
            transport
                .put_shard(NodeId(1), &address, &first, Cursor::new(shards[0].clone()),)
                .await
                .unwrap(),
            ShardSealOutcome::Created
        );
        assert_eq!(
            transport
                .put_shard(NodeId(1), &address, &first, Cursor::new(shards[0].clone()),)
                .await
                .unwrap(),
            ShardSealOutcome::AlreadyPresent
        );
        assert_eq!(
            collect_content(
                transport
                    .get_shard(NodeId(1), &address, &first)
                    .await
                    .unwrap()
            )
            .await,
            shards[0]
        );

        let second = ShardIdentity::new(reference.clone(), 1);
        let mut corrupt = shards[1].clone();
        *corrupt.last_mut().unwrap() ^= 0xff;
        let error = transport
            .put_shard(NodeId(1), &address, &second, Cursor::new(corrupt))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(
            !transport
                .shard_exists(NodeId(1), &address, &second)
                .await
                .unwrap()
        );

        let third = ShardIdentity::new(reference.clone(), 2);
        let truncated = shards[2][..shards[2].len() - 1].to_vec();
        let error = transport
            .put_shard(NodeId(1), &address, &third, Cursor::new(truncated))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::DataLoss);
        assert!(
            !transport
                .shard_exists(NodeId(1), &address, &third)
                .await
                .unwrap()
        );

        let error = transport
            .client(NodeId(1), &address)
            .unwrap()
            .put_shard(tokio_stream::iter([
                shard_frame(&transport, &second, 0, b"a", false),
                shard_frame(&transport, &second, 2, b"b", false),
            ]))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);

        let error = transport
            .client(NodeId(1), &address)
            .unwrap()
            .put_shard(tokio_stream::iter([
                shard_frame(&transport, &second, 0, b"a", false),
                shard_frame(&transport, &third, 1, b"b", false),
            ]))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);

        let _ = shutdown.send(());
        server.await.unwrap();
        let _ = source_shutdown.send(());
        source_server.await.unwrap();
        drop(destination);
        let reopened = Store::open(StoreOptions::new(&destination_root, 1))
            .await
            .unwrap();
        let mut reader = reopened.open_blob(&reference).await.unwrap();
        let mut recovered = Vec::new();
        let mut buffer = vec![0_u8; DATA_PEER_FRAME_BYTES];
        loop {
            let read = reader.read(&mut buffer).await.unwrap();
            if read == 0 {
                break;
            }
            recovered.extend_from_slice(&buffer[..read]);
        }
        assert_eq!(recovered, source);
        assert!(reopened.get_shard(&codec, &first).is_ok());
    }
}
