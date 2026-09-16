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
mod payload_ranges;
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
    async fn get_payload_range(
        &self,
        request: Request<wire::PayloadRangeRequest>,
    ) -> Result<Response<wire::PayloadRangeResponse>, Status> {
        self.serve_payload_range(request).await
    }

    async fn get_complete_source_state(
        &self,
        mut request: Request<wire::CompleteSourceStateRequest>,
    ) -> Result<Response<wire::CompleteSourceState>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
        let reference = parse_blob(request.get_ref().blob.as_ref())?;
        require_large_blob(&reference, self.max_blob_bytes)?;
        let fence = keldra_store::PlacementLogId {
            term: request.get_ref().placement_fence_term,
            index: request.get_ref().placement_fence_index,
        };
        self.mutation_admission.require_fence(fence)?;
        let metadata = request.metadata().clone();
        let store = self.store.clone();
        let state = self
            .bounded(&metadata, async move {
                store
                    .complete_copy_state(&reference)
                    .await
                    .map_err(map_payload_error)
            })
            .await?;
        self.mutation_admission.require_fence(fence)?;
        let state = match state {
            keldra_store::PayloadArtifactState::Missing => wire::CompleteCopyState::Missing,
            keldra_store::PayloadArtifactState::Valid => wire::CompleteCopyState::Valid,
            keldra_store::PayloadArtifactState::Corrupt => wire::CompleteCopyState::Corrupt,
        };
        Ok(Response::new(wire::CompleteSourceState {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            state: state as i32,
        }))
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
#[path = "data_peer/tests.rs"]
mod tests;
