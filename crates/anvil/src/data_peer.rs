//! Typed storage operations on Anvil's mandatory-mTLS private peer listener.
//!
//! This is deliberately not a RocksDB endpoint. Each method decodes one
//! versioned logical store type and invokes the corresponding storage-kernel
//! boundary. Operations without such a boundary are absent from the protocol.

use std::collections::BTreeMap;
use std::future::Future;
use std::io::Read;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use anvil_consensus::{
    AuthenticatedPeer, ClusterId, CommittedPeerPinProvider, NodeId, PeerRpcKind, PeerSpkiSha256,
    PeerTlsConnector, PeerTlsError, authorize_peer_rpc,
};
use anvil_store::{
    AuthzRealmMutation, BlobRef, ErasureCodec, ErasureProfile, FRAGMENT_FORMAT_VERSION,
    LocalChange, MAX_LOCAL_INVALIDATION_SCAN_RECORDS, MutationError, ObjectMutation,
    ReferenceDeltaApplied, ReferenceDeltaBatch, ReplicaAuthzRealmMutationApplied,
    ReplicaObjectMutationApplied, ShardIdentity, ShardStoreError, SourceId, Store,
    WatchJournalStatus,
};
use hyper_util::rt::TokioIo;
use tonic::codegen::Service;
use tonic::codegen::http::Uri;
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status, Streaming};

pub(crate) mod wire {
    tonic::include_proto!("anvil.data_peer.v1");
}

pub(crate) const DATA_PEER_SCHEMA_VERSION: u32 = 1;
pub(crate) const DATA_PEER_FRAME_BYTES: usize = 64 * 1024;
const MAX_TYPED_MUTATION_BYTES: usize = 16 * 1024 * 1024;
const MAX_DATA_PEER_MESSAGE_BYTES: usize = MAX_TYPED_MUTATION_BYTES + 1024;

#[derive(Clone)]
pub(crate) struct DataPeerService {
    store: Store,
    pins: Arc<dyn CommittedPeerPinProvider>,
    codec: Arc<ErasureCodec>,
    maximum_unary_time: Duration,
}

pub(crate) type DataPeerServer = wire::data_peer_server::DataPeerServer<DataPeerService>;
type ContentStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<wire::ContentFrame, Status>> + Send>>;

impl DataPeerService {
    pub(crate) fn new(
        store: Store,
        pins: Arc<dyn CommittedPeerPinProvider>,
        profile: ErasureProfile,
        maximum_unary_time: Duration,
    ) -> Result<Self, anyhow::Error> {
        anyhow::ensure!(
            !maximum_unary_time.is_zero()
                && tokio::time::Instant::now()
                    .checked_add(maximum_unary_time)
                    .is_some(),
            "private peer maximum unary time must be positive and fit the server clock"
        );
        let codec = ErasureCodec::new(profile)?;
        Ok(Self {
            store,
            pins,
            codec: Arc::new(codec),
            maximum_unary_time,
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
    type GetSmallContentStream = ContentStream;
    type GetShardStream = ContentStream;

    async fn apply_object_mutation(
        &self,
        mut request: Request<wire::TypedMutationRequest>,
    ) -> Result<Response<wire::ObjectMutationApplied>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        require_typed_bound(&request.get_ref().mutation_json)?;
        let mutation: ObjectMutation = decode_typed(&request.get_ref().mutation_json)?;
        let metadata = request.metadata().clone();
        let store = self.store.clone();
        let applied = self
            .bounded(&metadata, async move {
                store
                    .apply_object_mutation_replica(&mutation)
                    .await
                    .map_err(map_mutation_error)
            })
            .await?;
        Ok(Response::new(wire::ObjectMutationApplied {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            version: applied.version.0,
            replayed: applied.replayed,
        }))
    }

    async fn apply_authz_realm_mutation(
        &self,
        mut request: Request<wire::TypedMutationRequest>,
    ) -> Result<Response<wire::AuthzRealmMutationApplied>, Status> {
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
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        require_typed_bound(&request.get_ref().mutation_json)?;
        let mutation: ReferenceDeltaBatch = decode_typed(&request.get_ref().mutation_json)?;
        let metadata = request.metadata().clone();
        let store = self.store.clone();
        let applied = self
            .bounded(&metadata, async move {
                store
                    .apply_reference_deltas(mutation)
                    .await
                    .map_err(|error| Status::failed_precondition(error.to_string()))
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
        mut request: Request<wire::SourceJournalStatusRequest>,
    ) -> Result<Response<wire::SourceJournalStatus>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
        let metadata = request.metadata().clone();
        let store = self.store.clone();
        let status = self
            .bounded(&metadata, async move {
                tokio::task::spawn_blocking(move || store.local_watch_status())
                    .await
                    .map_err(|error| Status::internal(format!("join journal status: {error}")))?
                    .map_err(|error| Status::failed_precondition(error.to_string()))
            })
            .await?;
        Ok(Response::new(wire::SourceJournalStatus {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            source_id_json: encode_typed(&status.source_id)?,
            tail: status.tail,
            retention_floor: status.retention_floor,
            retained_entries: status.retained_entries,
            retained_bytes: status.retained_bytes,
        }))
    }

    async fn read_source_journal(
        &self,
        mut request: Request<wire::SourceJournalReadRequest>,
    ) -> Result<Response<wire::SourceJournalPage>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
        let after = request.get_ref().after_offset;
        let limit = usize::try_from(request.get_ref().limit)
            .unwrap_or(usize::MAX)
            .min(MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
        let metadata = request.metadata().clone();
        let store = self.store.clone();
        let changes = self
            .bounded(&metadata, async move {
                tokio::task::spawn_blocking(move || store.scan_local_changes(after, limit))
                    .await
                    .map_err(|error| Status::internal(format!("join journal read: {error}")))?
                    .map_err(map_mutation_error)
            })
            .await?;
        let changes_json = encode_page(changes)?;
        Ok(Response::new(wire::SourceJournalPage {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            changes_json,
        }))
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
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
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
                .filter(|length| *length <= anvil_store::SMALL_BLOB_MAX_BYTES)
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
            let stored = tokio::time::timeout(timeout, self.store.stage_blob(&bytes))
                .await
                .map_err(|_| Status::deadline_exceeded("content store deadline exceeded"))?
                .map_err(map_mutation_error)?;
            if stored != expected {
                return Err(Status::data_loss("stored content identity changed"));
            }
            return Ok(Response::new(wire::ContentStored {
                schema_version: DATA_PEER_SCHEMA_VERSION,
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
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::StateTransfer)?;
        let metadata = request.metadata().clone();
        let identity = parse_shard(&request.into_inner())?;
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
}

/// Cached mandatory-mTLS client for typed data-plane operations.
#[derive(Clone)]
#[allow(
    dead_code,
    reason = "the distributed coordinators consume this transport in the immediately following integration slice"
)]
pub(crate) struct DataPeerTransport {
    cluster_id: ClusterId,
    source_node_id: NodeId,
    tls: PeerTlsConnector,
    channels: Arc<Mutex<BTreeMap<u64, (String, Channel)>>>,
}

#[allow(
    dead_code,
    reason = "the distributed coordinators consume this transport in the immediately following integration slice"
)]
impl DataPeerTransport {
    pub(crate) fn new(
        cluster_id: ClusterId,
        source_node_id: NodeId,
        tls: PeerTlsConnector,
    ) -> Result<Self, anyhow::Error> {
        anyhow::ensure!(cluster_id.0 != [0; 16], "cluster id must not be all zero");
        anyhow::ensure!(source_node_id.0 != 0, "source node id must not be zero");
        Ok(Self {
            cluster_id,
            source_node_id,
            tls,
            channels: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    fn context(&self) -> wire::PeerContext {
        wire::PeerContext {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            cluster_id: self.cluster_id.into_bytes().to_vec(),
            source_node_id: self.source_node_id.0,
        }
    }

    fn channel(&self, target: NodeId, address: &str) -> Result<Channel, Status> {
        if target.0 == 0 {
            return Err(Status::invalid_argument("target node id must not be zero"));
        }
        if address.is_empty() {
            return Err(Status::invalid_argument("target peer address is empty"));
        }
        let mut channels = self
            .channels
            .lock()
            .map_err(|_| Status::internal("data-peer channel lock is poisoned"))?;
        if let Some((cached_address, channel)) = channels.get(&target.0)
            && cached_address == address
        {
            return Ok(channel.clone());
        }
        let connector = DataPeerChannelConnector {
            tls: self.tls.clone(),
            target,
            address: address.to_owned(),
        };
        let channel = Endpoint::from_static("http://anvil-peer.invalid")
            .connect_with_connector_lazy(connector);
        channels.insert(target.0, (address.to_owned(), channel.clone()));
        Ok(channel)
    }

    fn client(
        &self,
        target: NodeId,
        address: &str,
    ) -> Result<wire::data_peer_client::DataPeerClient<Channel>, Status> {
        Ok(
            wire::data_peer_client::DataPeerClient::new(self.channel(target, address)?)
                .max_decoding_message_size(MAX_DATA_PEER_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_DATA_PEER_MESSAGE_BYTES),
        )
    }

    pub(crate) async fn apply_object_mutation(
        &self,
        target: NodeId,
        address: &str,
        mutation: &ObjectMutation,
    ) -> Result<ReplicaObjectMutationApplied, Status> {
        let response = self
            .client(target, address)?
            .apply_object_mutation(wire::TypedMutationRequest {
                peer: Some(self.context()),
                mutation_json: encode_typed(mutation)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(ReplicaObjectMutationApplied {
            version: anvil_store::VersionId(response.version),
            replayed: response.replayed,
        })
    }

    pub(crate) async fn apply_authz_realm_mutation(
        &self,
        target: NodeId,
        address: &str,
        mutation: &AuthzRealmMutation,
    ) -> Result<ReplicaAuthzRealmMutationApplied, Status> {
        let response = self
            .client(target, address)?
            .apply_authz_realm_mutation(wire::TypedMutationRequest {
                peer: Some(self.context()),
                mutation_json: encode_typed(mutation)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(ReplicaAuthzRealmMutationApplied {
            revision: anvil_store::AuthzRevision(response.revision),
            replayed: response.replayed,
        })
    }

    pub(crate) async fn apply_reference_deltas(
        &self,
        target: NodeId,
        address: &str,
        mutation: &ReferenceDeltaBatch,
    ) -> Result<ReferenceDeltaApplied, Status> {
        let response = self
            .client(target, address)?
            .apply_reference_deltas(wire::TypedMutationRequest {
                peer: Some(self.context()),
                mutation_json: encode_typed(mutation)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(ReferenceDeltaApplied {
            through: response.through,
            replayed: response.replayed,
        })
    }

    pub(crate) async fn reference_delta_status(
        &self,
        target: NodeId,
        address: &str,
        source: SourceId,
    ) -> Result<u64, Status> {
        let response = self
            .client(target, address)?
            .get_reference_delta_status(wire::ReferenceDeltaStatusRequest {
                peer: Some(self.context()),
                source_id_json: encode_typed(&source)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.through)
    }

    pub(crate) async fn source_journal_status(
        &self,
        target: NodeId,
        address: &str,
    ) -> Result<WatchJournalStatus, Status> {
        let response = self
            .client(target, address)?
            .get_source_journal_status(wire::SourceJournalStatusRequest {
                peer: Some(self.context()),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(WatchJournalStatus {
            source_id: decode_typed(&response.source_id_json)?,
            tail: response.tail,
            retention_floor: response.retention_floor,
            retained_entries: response.retained_entries,
            retained_bytes: response.retained_bytes,
        })
    }

    pub(crate) async fn read_source_journal(
        &self,
        target: NodeId,
        address: &str,
        after_offset: u64,
        limit: usize,
    ) -> Result<Vec<LocalChange>, Status> {
        let limit = u32::try_from(limit.min(MAX_LOCAL_INVALIDATION_SCAN_RECORDS))
            .expect("source journal limit fits u32");
        let response = self
            .client(target, address)?
            .read_source_journal(wire::SourceJournalReadRequest {
                peer: Some(self.context()),
                after_offset,
                limit,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        response
            .changes_json
            .iter()
            .map(|encoded| decode_typed(encoded))
            .collect()
    }

    pub(crate) async fn small_content_exists(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
    ) -> Result<bool, Status> {
        let response = self
            .client(target, address)?
            .small_content_exists(wire::ContentRequest {
                peer: Some(self.context()),
                blob: Some(wire_blob(reference)),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.exists)
    }

    pub(crate) async fn put_small_content(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        bytes: &[u8],
    ) -> Result<(), Status> {
        if bytes.len() > anvil_store::SMALL_BLOB_MAX_BYTES
            || bytes.len() as u64 != reference.length
            || blake3::hash(bytes).as_bytes() != &reference.hash
        {
            return Err(Status::invalid_argument(
                "small content does not match its immutable identity",
            ));
        }
        let mut frames = Vec::new();
        if bytes.is_empty() {
            frames.push(wire::SmallContentPutFrame {
                peer: Some(self.context()),
                blob: Some(wire_blob(reference)),
                offset: 0,
                content: Vec::new(),
                end: true,
            });
        } else {
            for (index, content) in bytes.chunks(DATA_PEER_FRAME_BYTES).enumerate() {
                let offset = index * DATA_PEER_FRAME_BYTES;
                frames.push(wire::SmallContentPutFrame {
                    peer: Some(self.context()),
                    blob: Some(wire_blob(reference)),
                    offset: offset as u64,
                    content: content.to_vec(),
                    end: offset + content.len() == bytes.len(),
                });
            }
        }
        let response = self
            .client(target, address)?
            .put_small_content(tokio_stream::iter(frames))
            .await?
            .into_inner();
        require_response_schema(response.schema_version)
    }

    pub(crate) async fn get_small_content(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
    ) -> Result<Vec<u8>, Status> {
        if reference.length > anvil_store::SMALL_BLOB_MAX_BYTES as u64 {
            return Err(Status::invalid_argument(
                "content identity is not a small blob",
            ));
        }
        let mut stream = self
            .client(target, address)?
            .get_small_content(wire::ContentRequest {
                peer: Some(self.context()),
                blob: Some(wire_blob(reference)),
            })
            .await?
            .into_inner();
        let mut bytes = Vec::with_capacity(reference.length as usize);
        while let Some(frame) = stream.message().await? {
            require_response_schema(frame.schema_version)?;
            if frame.offset != bytes.len() as u64 || frame.content.len() > DATA_PEER_FRAME_BYTES {
                return Err(Status::data_loss("small-content stream is not contiguous"));
            }
            bytes.extend_from_slice(&frame.content);
            if bytes.len() > anvil_store::SMALL_BLOB_MAX_BYTES {
                return Err(Status::resource_exhausted(
                    "small-content response is too large",
                ));
            }
            if frame.end {
                if bytes.len() as u64 != reference.length
                    || blake3::hash(&bytes).as_bytes() != &reference.hash
                {
                    return Err(Status::data_loss(
                        "small-content response failed identity verification",
                    ));
                }
                return Ok(bytes);
            }
        }
        Err(Status::data_loss(
            "small-content stream ended without an end frame",
        ))
    }

    pub(crate) async fn shard_exists(
        &self,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
    ) -> Result<bool, Status> {
        let response = self
            .client(target, address)?
            .shard_exists(wire_shard(self.context(), identity))
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.exists)
    }

    pub(crate) async fn get_shard(
        &self,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
    ) -> Result<Streaming<wire::ContentFrame>, Status> {
        self.client(target, address)?
            .get_shard(wire_shard(self.context(), identity))
            .await
            .map(Response::into_inner)
    }
}

#[derive(Clone)]
struct DataPeerChannelConnector {
    tls: PeerTlsConnector,
    target: NodeId,
    address: String,
}

impl Service<Uri> for DataPeerChannelConnector {
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

fn parse_cluster_id(encoded: &[u8]) -> Result<ClusterId, Status> {
    let bytes = encoded
        .try_into()
        .map_err(|_| Status::invalid_argument("cluster id must contain exactly 16 bytes"))?;
    Ok(ClusterId(bytes))
}

fn require_response_schema(schema_version: u32) -> Result<(), Status> {
    if schema_version != DATA_PEER_SCHEMA_VERSION {
        return Err(Status::failed_precondition(format!(
            "peer returned unsupported data-peer schema {schema_version}"
        )));
    }
    Ok(())
}

fn wire_blob(reference: &BlobRef) -> wire::BlobIdentity {
    wire::BlobIdentity {
        blake3: reference.hash.to_vec(),
        length: reference.length,
    }
}

#[allow(
    dead_code,
    reason = "used by the typed shard client when distributed payload orchestration is connected"
)]
fn wire_shard(context: wire::PeerContext, identity: &ShardIdentity) -> wire::ShardRequest {
    wire::ShardRequest {
        peer: Some(context),
        fragment_format_version: u32::from(identity.fragment_format_version()),
        blob: Some(wire_blob(identity.blob())),
        ordinal: u32::from(identity.ordinal()),
    }
}

fn parse_blob(value: Option<&wire::BlobIdentity>) -> Result<BlobRef, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("blob identity is required"))?;
    let hash =
        value.blake3.as_slice().try_into().map_err(|_| {
            Status::invalid_argument("BLAKE3 identity must contain exactly 32 bytes")
        })?;
    Ok(BlobRef {
        hash,
        length: value.length,
    })
}

fn parse_small_blob(value: Option<&wire::BlobIdentity>) -> Result<BlobRef, Status> {
    let reference = parse_blob(value)?;
    if reference.length > anvil_store::SMALL_BLOB_MAX_BYTES as u64 {
        return Err(Status::invalid_argument(
            "content identity is not a small blob",
        ));
    }
    Ok(reference)
}

fn parse_shard(value: &wire::ShardRequest) -> Result<ShardIdentity, Status> {
    let fragment_format_version = u16::try_from(value.fragment_format_version)
        .map_err(|_| Status::invalid_argument("fragment format does not fit u16"))?;
    if fragment_format_version != FRAGMENT_FORMAT_VERSION {
        return Err(Status::failed_precondition(format!(
            "unsupported fragment format {fragment_format_version}"
        )));
    }
    let ordinal = u16::try_from(value.ordinal)
        .map_err(|_| Status::invalid_argument("shard ordinal does not fit u16"))?;
    Ok(ShardIdentity::new(
        parse_blob(value.blob.as_ref())?,
        ordinal,
    ))
}

fn require_typed_bound(encoded: &[u8]) -> Result<(), Status> {
    if encoded.len() > MAX_TYPED_MUTATION_BYTES {
        return Err(Status::resource_exhausted(
            "typed mutation exceeds the private peer limit",
        ));
    }
    Ok(())
}

fn decode_typed<T: serde::de::DeserializeOwned>(encoded: &[u8]) -> Result<T, Status> {
    serde_json::from_slice(encoded)
        .map_err(|error| Status::invalid_argument(format!("invalid typed peer payload: {error}")))
}

fn encode_typed<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, Status> {
    serde_json::to_vec(value)
        .map_err(|error| Status::internal(format!("encode typed peer payload: {error}")))
}

fn encode_page(changes: Vec<LocalChange>) -> Result<Vec<Vec<u8>>, Status> {
    let mut encoded = Vec::with_capacity(changes.len());
    let mut total = 0_usize;
    for change in changes {
        let item = encode_typed(&change)?;
        total = total
            .checked_add(item.len())
            .filter(|total| *total <= MAX_TYPED_MUTATION_BYTES)
            .ok_or_else(|| Status::resource_exhausted("source journal page exceeds peer limit"))?;
        encoded.push(item);
    }
    Ok(encoded)
}

fn content_frame(offset: u64, content: Vec<u8>) -> wire::ContentFrame {
    wire::ContentFrame {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        offset,
        content,
        end: false,
    }
}

fn content_end(offset: u64) -> wire::ContentFrame {
    wire::ContentFrame {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        offset,
        content: Vec::new(),
        end: true,
    }
}

fn map_mutation_error(error: MutationError) -> Status {
    match error {
        MutationError::BlobNotFound => Status::not_found(error.to_string()),
        MutationError::InvalidObjectMutation(_) | MutationError::InvalidCommandId => {
            Status::invalid_argument(error.to_string())
        }
        MutationError::Storage(_) => Status::internal(error.to_string()),
        _ => Status::failed_precondition(error.to_string()),
    }
}

fn map_shard_error(error: ShardStoreError) -> Status {
    match error {
        ShardStoreError::NotFound => Status::not_found(error.to_string()),
        ShardStoreError::MalformedIdentity => Status::invalid_argument(error.to_string()),
        ShardStoreError::Storage(_) => Status::internal(error.to_string()),
        _ => Status::failed_precondition(error.to_string()),
    }
}

fn effective_timeout(metadata: &MetadataMap, server_maximum: Duration) -> Duration {
    client_grpc_timeout(metadata).map_or(server_maximum, |client| client.min(server_maximum))
}

fn client_grpc_timeout(metadata: &MetadataMap) -> Option<Duration> {
    let encoded = metadata.get("grpc-timeout")?.to_str().ok()?;
    if encoded.is_empty() {
        return None;
    }
    let (value, unit) = encoded.split_at(encoded.len() - 1);
    if value.is_empty() || value.len() > 8 {
        return None;
    }
    let value = value.parse::<u64>().ok()?;
    match unit {
        "H" => Some(Duration::from_secs(value.checked_mul(60 * 60)?)),
        "M" => Some(Duration::from_secs(value.checked_mul(60)?)),
        "S" => Some(Duration::from_secs(value)),
        "m" => Some(Duration::from_millis(value)),
        "u" => Some(Duration::from_micros(value)),
        "n" => Some(Duration::from_nanos(value)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, RwLock};

    use anvil_consensus::{
        CommittedPeerPins, NodeState, PeerTlsAcceptor, PeerTlsConfig, PeerTlsIdentity,
    };
    use anvil_store::StoreOptions;
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
                PeerRpcKind::StateTransfer => {
                    matches!(state, NodeState::Active | NodeState::Joining)
                }
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
        let service = DataPeerService::new(
            store,
            pins,
            ErasureProfile::default(),
            Duration::from_secs(30),
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
            PeerTlsConnector::new(joining_id, pins.clone(), PeerTlsConfig::default()).unwrap(),
        )
        .unwrap();

        let status = joining
            .source_journal_status(NodeId(1), &address)
            .await
            .unwrap();
        assert_eq!(status.source_id.node_id, 1);

        let empty = ReferenceDeltaBatch {
            source: status.source_id,
            after: 0,
            through: 0,
            deltas: Vec::new(),
        };
        let denied = joining
            .apply_reference_deltas(NodeId(1), &address, &empty)
            .await
            .unwrap_err();
        assert_eq!(denied.code(), Code::PermissionDenied);

        pins.set_state(NodeId(2), NodeState::Active);
        let applied = joining
            .apply_reference_deltas(NodeId(1), &address, &empty)
            .await
            .unwrap();
        assert_eq!(applied.through, 0);
        assert_eq!(
            joining
                .reference_delta_status(NodeId(1), &address, status.source_id)
                .await
                .unwrap(),
            0
        );
        assert!(
            joining
                .read_source_journal(NodeId(1), &address, 0, 16)
                .await
                .unwrap()
                .is_empty()
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
        assert!(
            !joining
                .shard_exists(
                    NodeId(1),
                    &address,
                    &ShardIdentity::new(reference.clone(), 0),
                )
                .await
                .unwrap()
        );

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

        pins.remove(NodeId(2));
        let denied = joining
            .source_journal_status(NodeId(1), &address)
            .await
            .unwrap_err();
        assert_eq!(denied.code(), Code::PermissionDenied);

        let _ = shutdown.send(());
        server.await.unwrap();
    }
}
