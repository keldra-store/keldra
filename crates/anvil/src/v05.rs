use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use anvil_api::v1::batch_get_outcome::Outcome as BatchGetResult;
use anvil_api::v1::object_chunk::Value as ObjectChunkValue;
use anvil_api::v1::object_head::State as ObjectState;
use anvil_api::v1::object_service_server::ObjectService;
use anvil_api::v1::object_version::State as ObjectVersionState;
use anvil_api::v1::put_header::Operation as ApiPutOperation;
use anvil_api::v1::watch_prefix_request::Start as WatchStartValue;
use anvil_api::v1::{
    BatchGetObject, BatchGetOutcome, BatchGetRequest, BatchGetResponse, BucketPolicy,
    BulkOperation, BulkOutcome, BulkPutIfVersionRequest, BulkPutRequest, BulkWriteRequest,
    BulkWriteResponse, DeleteIfVersionRequest, DeleteRequest as ApiDeleteRequest,
    DeleteVersionRequest, DeleteVersionResponse, DeletedObject, Durability as ApiDurability,
    GetObjectRequest, HeadObjectRequest, InvokeProgramRequest, InvokeProgramResponse,
    ListObjectVersionsRequest, ListObjectsRequest, ListObjectsResponse, MutationFailure,
    MutationFailureCode, MutationReceipt as ApiMutationReceipt, NeverExisted, ObjectAddress,
    ObjectChunk, ObjectHead, ObjectVersion, PresentObject, PutHeader, PutRequest as ApiPutRequest,
    PutToken, ReadFailure, ReadFailureCode, SetBucketPolicyRequest, WatchInvalidation,
    WatchPrefixRequest, WatchStateHint,
};
use anvil_atomic_program::{ExpandedProgramPath, MAX_OBJECT_PATH_BYTES};
use anvil_store::{
    AuthzStoreError, BatchOperation, BlobRef, BlobUpload, DeleteRequest as StoreDeleteRequest,
    DeleteRetainedVersionOutcome, Durability as StoreDurability, InvalidationStateHint,
    LocalInvalidation, MAX_LIST_OBJECTS, MutationError, MutationReceipt, ObjectKey,
    ObjectVersioning as StoreObjectVersioning, Precondition, PublishRequest, PutMode,
    PutRequest as StorePutRequest, Store, Version, VersionId, WatchError, WatchScope,
};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use tokio_stream::Stream;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};

use crate::authentication::{Caller, JwtManager, PUT_TOKEN_LIFETIME};
use crate::authoritative_system::AuthoritativeSystemAuthorization;
use crate::authorization::{ObjectPermission, SystemAuthorization, SystemAuthorizer};
use crate::bucket_governance::{
    BucketGovernance, require_versioning_enabled as require_governance_versioning_enabled,
};
use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::ClusterPeerTransport;
use crate::distributed_list::{DistributedObjectLister, OriginalBearer};
use crate::distributed_watch::{DistributedWatch, DistributedWatchScope};
use crate::logical_name_resolution::LogicalNameResolver;
use crate::object_distribution::ObjectDistribution;
use crate::programs::ProgramCoordinator;

#[cfg(test)]
use anvil_store::WatchJournalStatus;

mod atomic_program;
mod batch_get;
mod bulk;
mod distributed_reads;
mod distributed_watch_stream;
mod routed_writes;

const OBJECT_CHUNK_BYTES: usize = 64 * 1024;
const MAX_BULK_ITEMS: usize = 1_000;
const MAX_BULK_BYTES: usize = 64 * 1024 * 1024;
const MAX_BATCH_GET_ITEMS: usize = 1_000;
const MAX_BATCH_GET_BYTES: usize = 64 * 1024 * 1024;
const MAX_CONTENT_TYPE_BYTES: usize = 512;
const DEFAULT_LIST_OBJECTS_LIMIT: usize = 100;
const PUT_TOKEN_FORMAT_VERSION: u8 = 2;

#[derive(Clone)]
pub struct ObjectServiceImpl {
    store: Store,
    system_authorizer: SystemAuthorizer,
    programs: ProgramCoordinator,
    distribution: ObjectDistribution,
    reader: ClusterObjectReader,
    cluster_peers: ClusterPeerTransport,
    lister: DistributedObjectLister,
    watch: Arc<DistributedWatch>,
    name_resolver: LogicalNameResolver,
    authoritative_system: AuthoritativeSystemAuthorization,
    bucket_governance: BucketGovernance,
    jwt_manager: JwtManager,
    max_blob_bytes: u64,
    atomic_program_timeout: Duration,
}

impl ObjectServiceImpl {
    pub(crate) fn new(
        store: Store,
        programs: ProgramCoordinator,
        distribution: ObjectDistribution,
        reader: ClusterObjectReader,
        cluster_peers: ClusterPeerTransport,
        lister: DistributedObjectLister,
        watch: Arc<DistributedWatch>,
        name_resolver: LogicalNameResolver,
        authoritative_system: AuthoritativeSystemAuthorization,
        bucket_governance: BucketGovernance,
        jwt_manager: JwtManager,
        max_blob_bytes: u64,
        atomic_program_timeout: Duration,
    ) -> Self {
        Self {
            system_authorizer: SystemAuthorizer::new(store.authz()),
            store,
            programs,
            distribution,
            reader,
            cluster_peers,
            lister,
            watch,
            name_resolver,
            authoritative_system,
            bucket_governance,
            jwt_manager,
            max_blob_bytes,
            atomic_program_timeout,
        }
    }

    async fn system_authorization(&self) -> Result<SystemAuthorization, Status> {
        let authorizer = self.system_authorizer.clone();
        tokio::task::spawn_blocking(move || authorizer.load())
            .await
            .map_err(|error| internal(format!("authorization worker failed: {error}")))?
            .map_err(authorization_store_status)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalPutHeader {
    tenant: String,
    bucket: String,
    path: String,
    content_type: Option<String>,
    command_id: String,
    durability: TokenDurability,
    operation: TokenPutOperation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalPutCapability {
    format_version: u8,
    phase: PutTokenPhase,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PutTokenPhase {
    Upload(UploadCapability),
    Ready(ReadyCapability),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadCapability {
    header: CanonicalPutHeader,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadyCapability {
    header: CanonicalPutHeader,
    blob_hash: [u8; 32],
    blob_length: u64,
    upload_source_node_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TokenDurability {
    Local,
    Replicated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TokenPutOperation {
    Put,
    PutIfAbsent,
    PutIfVersion { expected_version: u64 },
    PutImmutable,
}

#[derive(Clone, Debug)]
struct PutMetadata {
    key: ObjectKey,
    content_type: Option<String>,
    command_id: String,
    durability: StoreDurability,
    mode: PutMode,
}

#[derive(Debug)]
struct ListObjectsQuery {
    tenant: String,
    bucket: String,
    prefix: String,
    start_after: Option<String>,
    limit: usize,
}

type GetObjectStream =
    Pin<Box<dyn Stream<Item = Result<ObjectChunk, Status>> + Send + Sync + 'static>>;
type ListObjectVersionsStream =
    Pin<Box<dyn Stream<Item = Result<ObjectVersion, Status>> + Send + Sync + 'static>>;
type WatchPrefixStream = distributed_watch_stream::ClusterWatchStream;

#[tonic::async_trait]
impl ObjectService for ObjectServiceImpl {
    async fn start_put(&self, request: Request<PutHeader>) -> Result<Response<PutToken>, Status> {
        let caller = authenticated_caller(&request)?;
        let metadata = put_metadata(request.into_inner())?;
        self.authorize_object(&caller, &metadata.key, ObjectPermission::Put)
            .await?;
        self.issue_upload_token(&caller, &metadata)
            .map(Response::new)
    }

    async fn put(
        &self,
        request: Request<Streaming<ApiPutRequest>>,
    ) -> Result<Response<PutToken>, Status> {
        let caller = authenticated_caller(&request)?;
        let mut stream = request.into_inner();
        let first = tokio::time::timeout(PUT_TOKEN_LIFETIME, stream.message())
            .await
            .map_err(|_| Status::deadline_exceeded("put stream inactivity lease expired"))??
            .ok_or_else(|| Status::invalid_argument("put stream is empty"))?;
        let token = required_put_token(first.token)?;
        let capability = self.verify_put_token(&caller, &token)?;
        let header = require_upload_phase(capability)?;
        let metadata = header.to_metadata()?;
        self.authorize_object(&caller, &metadata.key, ObjectPermission::Put)
            .await?;

        let mut upload = self.store.begin_blob_upload().await.map_err(status)?;
        let mut length = 0_u64;
        write_upload_chunk(&mut upload, &mut length, &first.chunk, self.max_blob_bytes).await?;
        loop {
            let frame = tokio::time::timeout(PUT_TOKEN_LIFETIME, stream.message())
                .await
                .map_err(|_| Status::deadline_exceeded("put stream inactivity lease expired"))??;
            let Some(frame) = frame else {
                break;
            };
            let frame_token = required_put_token(frame.token)?;
            if frame_token != token {
                return Err(Status::invalid_argument(
                    "put stream contains a missing or different upload token",
                ));
            }
            write_upload_chunk(&mut upload, &mut length, &frame.chunk, self.max_blob_bytes).await?;
        }
        let blob = self.store.seal_blob_upload(upload).await.map_err(status)?;
        self.issue_ready_token(&caller, header, &blob)
            .map(Response::new)
    }

    async fn put_end(
        &self,
        request: Request<PutToken>,
    ) -> Result<Response<ApiMutationReceipt>, Status> {
        let peer_routed = request
            .extensions()
            .get::<routed_writes::RoutedDestination>()
            .is_some();
        let caller = authenticated_caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let remaining =
            effective_atomic_program_timeout(request.metadata(), self.atomic_program_timeout);
        let token = required_put_token(Some(request.into_inner()))?;
        let capability = self.verify_put_token(&caller, &token)?;
        let ready = require_ready_phase(capability)?;
        let metadata = ready.header.to_metadata()?;
        self.authorize_object(&caller, &metadata.key, ObjectPermission::Put)
            .await?;
        let publish = PublishRequest {
            key: metadata.key,
            blob: BlobRef {
                hash: ready.blob_hash,
                length: ready.blob_length,
            },
            content_type: metadata.content_type,
            mode: metadata.mode,
            command_id: Some(metadata.command_id),
            durability: metadata.durability,
        };
        let receipt = match self.distribution.routing_target(&publish.key)? {
            Some(_) if peer_routed => {
                return Err(Status::failed_precondition(
                    "a routed PutEnd reached a node that is not its coordinator",
                ));
            }
            Some((target, address)) => {
                self.cluster_peers
                    .route_put_end(target, &address, bearer.signed_token(), token, remaining)
                    .await?
            }
            None => {
                let governance = self
                    .bucket_governance
                    .resolve(publish.key.tenant(), publish.key.bucket())
                    .await?;
                api_receipt(
                    self.distribution
                        .publish_from_source_with_governance(
                            publish,
                            anvil_consensus::NodeId(ready.upload_source_node_id),
                            governance,
                        )
                        .await?,
                )
            }
        };
        Ok(Response::new(receipt))
    }

    async fn delete(
        &self,
        request: Request<ApiDeleteRequest>,
    ) -> Result<Response<ApiMutationReceipt>, Status> {
        let peer_routed = request
            .extensions()
            .get::<routed_writes::RoutedDestination>()
            .is_some();
        let caller = authenticated_caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let remaining =
            effective_atomic_program_timeout(request.metadata(), self.atomic_program_timeout);
        let api_request = request.into_inner();
        let mutation = delete_request(api_request.clone(), Precondition::Any)?;
        self.authorize_object(&caller, &mutation.key, ObjectPermission::Delete)
            .await?;
        let receipt = match self.distribution.routing_target(&mutation.key)? {
            Some(_) if peer_routed => {
                return Err(Status::failed_precondition(
                    "a routed Delete reached a node that is not its coordinator",
                ));
            }
            Some((target, address)) => {
                self.cluster_peers
                    .route_delete(
                        target,
                        &address,
                        bearer.signed_token(),
                        api_request,
                        remaining,
                    )
                    .await?
            }
            None => api_receipt(
                self.distribution
                    .mutate(BatchOperation::Delete(mutation))
                    .await?,
            ),
        };
        Ok(Response::new(receipt))
    }

    async fn delete_if_version(
        &self,
        request: Request<DeleteIfVersionRequest>,
    ) -> Result<Response<ApiMutationReceipt>, Status> {
        let peer_routed = request
            .extensions()
            .get::<routed_writes::RoutedDestination>()
            .is_some();
        let caller = authenticated_caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let remaining =
            effective_atomic_program_timeout(request.metadata(), self.atomic_program_timeout);
        let api_request = request.into_inner();
        let precondition = Precondition::Version(VersionId(api_request.expected_version));
        let mutation = delete_if_version_request(api_request.clone(), precondition)?;
        self.authorize_object(&caller, &mutation.key, ObjectPermission::Delete)
            .await?;
        let receipt = match self.distribution.routing_target(&mutation.key)? {
            Some(_) if peer_routed => {
                return Err(Status::failed_precondition(
                    "a routed DeleteIfVersion reached a node that is not its coordinator",
                ));
            }
            Some((target, address)) => {
                self.cluster_peers
                    .route_delete_if_version(
                        target,
                        &address,
                        bearer.signed_token(),
                        api_request,
                        remaining,
                    )
                    .await?
            }
            None => api_receipt(
                self.distribution
                    .mutate(BatchOperation::Delete(mutation))
                    .await?,
            ),
        };
        Ok(Response::new(receipt))
    }

    async fn delete_version(
        &self,
        request: Request<DeleteVersionRequest>,
    ) -> Result<Response<DeleteVersionResponse>, Status> {
        let peer_routed = request
            .extensions()
            .get::<routed_writes::RoutedDestination>()
            .is_some();
        let caller = authenticated_caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let remaining =
            effective_atomic_program_timeout(request.metadata(), self.atomic_program_timeout);
        let api_request = request.into_inner();
        let _durability = durability(api_request.durability)?;
        let key = object_key(api_request.address.clone())?;
        self.authorize_object(&caller, &key, ObjectPermission::Delete)
            .await?;
        let governance = self
            .bucket_governance
            .resolve(key.tenant(), key.bucket())
            .await?;
        require_governance_versioning_enabled(&governance)?;
        let outcome = match self.distribution.routing_target_stable(
            &key,
            governance.tenant_id,
            governance.bucket_id,
        )? {
            Some(_) if peer_routed => {
                return Err(Status::failed_precondition(
                    "a routed DeleteVersion reached a node that is not its coordinator",
                ));
            }
            Some((target, address)) => {
                return self
                    .cluster_peers
                    .route_delete_version(
                        target,
                        &address,
                        bearer.signed_token(),
                        api_request,
                        remaining,
                    )
                    .await
                    .map(Response::new);
            }
            None => {
                self.distribution
                    .delete_retained_version_with_governance(
                        &key,
                        VersionId(api_request.version),
                        governance,
                    )
                    .await?
            }
        };
        Ok(Response::new(api_delete_version_outcome(outcome)))
    }

    async fn head_object(
        &self,
        request: Request<HeadObjectRequest>,
    ) -> Result<Response<ObjectHead>, Status> {
        let deadline = request_deadline(request.metadata(), self.atomic_program_timeout)?;
        let caller = authenticated_caller(&request)?;
        let key = object_key(request.into_inner().address)?;
        self.authorize_object(&caller, &key, ObjectPermission::Get)
            .await?;
        loop {
            let (version, cursor) = self.reader.head_with_program_cursor(&key).await?;
            match cursor {
                Some(cursor) if !self.programs.cursor_is_visible(cursor)? => {
                    self.programs
                        .wait_for_cursor(cursor, deadline_remaining(deadline)?)
                        .await?;
                    continue;
                }
                _ => {
                    return version.map_or_else(
                        || Ok(Response::new(never_existed())),
                        |version| api_head(&version).map(Response::new),
                    );
                }
            }
        }
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let caller = authenticated_caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let query = list_objects_query(request.into_inner())?;
        if caller.storage_tenant().as_str() != query.tenant.as_str() {
            return Err(Status::permission_denied(
                "object list does not belong to the authenticated tenant",
            ));
        }
        let (tenant_id, bucket_id) = self
            .name_resolver
            .resolve_bucket_ids(&query.tenant, &query.bucket)
            .await?;
        let page = self
            .lister
            .list_objects(
                bearer,
                &query.tenant,
                &query.bucket,
                tenant_id,
                bucket_id,
                &query.prefix,
                query.start_after.as_deref(),
                query.limit,
            )
            .await?;
        Ok(Response::new(ListObjectsResponse {
            paths: page.paths,
            has_more: page.has_more,
        }))
    }

    type GetObjectStream = GetObjectStream;

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        let deadline = request_deadline(request.metadata(), self.atomic_program_timeout)?;
        let caller = authenticated_caller(&request)?;
        let request = request.into_inner();
        let key = object_key(request.address)?;
        self.authorize_object(&caller, &key, ObjectPermission::Get)
            .await?;
        if request.version.is_some() {
            require_versioning_enabled(&self.store, &key)?;
        }
        let requested_version = request.version.map(VersionId);
        loop {
            let selected = self.reader.open(&key, requested_version).await?;
            let cursor = selected
                .as_ref()
                .and_then(|object| object.program_commit_cursor);
            match cursor {
                Some(cursor) if !self.programs.cursor_is_visible(cursor)? => {
                    self.programs
                        .wait_for_cursor(cursor, deadline_remaining(deadline)?)
                        .await?;
                }
                _ => {
                    return distributed_reads::get_object_response(
                        selected,
                        requested_version.is_some(),
                    );
                }
            }
        }
    }

    type ListObjectVersionsStream = ListObjectVersionsStream;

    async fn list_object_versions(
        &self,
        request: Request<ListObjectVersionsRequest>,
    ) -> Result<Response<Self::ListObjectVersionsStream>, Status> {
        let deadline = request_deadline(request.metadata(), self.atomic_program_timeout)?;
        let caller = authenticated_caller(&request)?;
        let key = object_key(request.into_inner().address)?;
        self.authorize_object(&caller, &key, ObjectPermission::Get)
            .await?;
        let governance = self
            .bucket_governance
            .resolve(key.tenant(), key.bucket())
            .await?;
        require_governance_versioning_enabled(&governance)?;

        loop {
            let snapshot = self.distribution.reconciled_object_snapshot(&key).await?;
            let cursor = snapshot.as_ref().and_then(|snapshot| {
                snapshot
                    .head
                    .mutation_stamp
                    .and_then(|stamp| stamp.program_commit_cursor)
            });
            match cursor {
                Some(cursor) if !self.programs.cursor_is_visible(cursor)? => {
                    self.programs
                        .wait_for_cursor(cursor, deadline_remaining(deadline)?)
                        .await?;
                }
                _ => return distributed_reads::list_object_versions_response(snapshot, &key),
            }
        }
    }

    async fn bulk_write(
        &self,
        request: Request<BulkWriteRequest>,
    ) -> Result<Response<BulkWriteResponse>, Status> {
        let peer_routed = request
            .extensions()
            .get::<routed_writes::RoutedDestination>()
            .is_some();
        let started = Instant::now();
        let operation_count = request.get_ref().operations.len() as u64;
        let encoded_bytes = request.get_ref().encoded_len() as u64;
        let result = async {
            let caller = authenticated_caller(&request)?;
            let bearer = OriginalBearer::from_metadata(request.metadata())?;
            let route_budget =
                effective_atomic_program_timeout(request.metadata(), self.atomic_program_timeout);
            let operations = request.into_inner().operations;
            validate_bulk_limits(&operations)?;

            let mut local = Vec::with_capacity(operations.len());
            let mut remote =
                BTreeMap::<anvil_consensus::NodeId, (String, Vec<(usize, BulkOperation)>)>::new();
            let mut outcomes = Vec::new();
            let mut pending = Vec::with_capacity(operations.len());
            for (index, operation) in operations.into_iter().enumerate() {
                let forwarded = operation.clone();
                match batch_operation(operation, self.max_blob_bytes) {
                    Ok(mutation) => {
                        match require_caller_tenant(&caller, batch_operation_key(&mutation)) {
                            Ok(()) => pending.push((index, forwarded, mutation)),
                            Err(error) => outcomes.push(bulk_authorization_failure(index, &error)),
                        }
                    }
                    Err(error) => outcomes.push(BulkOutcome {
                        index: index as u32,
                        outcome: Some(anvil_api::v1::bulk_outcome::Outcome::Failure(
                            api_request_failure(error),
                        )),
                    }),
                }
            }
            let authorization_requests = pending
                .iter()
                .map(|(_, _, mutation)| {
                    (
                        batch_operation_key(mutation).clone(),
                        batch_operation_permission(mutation),
                    )
                })
                .collect::<Vec<_>>();
            let allowed = if authorization_requests.is_empty() {
                Vec::new()
            } else {
                self.authoritative_system
                    .allows_objects(&caller, &authorization_requests)
                    .await?
            };
            for ((index, forwarded, mutation), allowed) in pending.into_iter().zip(allowed) {
                if !allowed {
                    outcomes.push(bulk_authorization_failure(
                        index,
                        &Status::permission_denied("bulk object operation is not authorized"),
                    ));
                    continue;
                }
                match self
                    .distribution
                    .routing_target(batch_operation_key(&mutation))?
                {
                    Some((target, address)) => remote
                        .entry(target)
                        .or_insert_with(|| (address, Vec::new()))
                        .1
                        .push((index, forwarded)),
                    None => local.push((index, mutation)),
                }
            }
            let local_indices = local.iter().map(|(index, _)| *index).collect::<Vec<_>>();
            let local_operations = local.into_iter().map(|(_, operation)| operation).collect();
            if peer_routed && !remote.is_empty() {
                return Err(Status::failed_precondition(
                    "a routed bulk reached a node that is not every item's coordinator",
                ));
            }
            outcomes.extend(
                bulk::execute_coordinator_groups(
                    self.distribution.clone(),
                    self.cluster_peers.clone(),
                    local_indices,
                    local_operations,
                    remote,
                    bearer.signed_token().to_owned(),
                    started,
                    route_budget,
                )
                .await?,
            );
            outcomes.sort_unstable_by_key(|outcome| outcome.index);
            Ok(Response::new(BulkWriteResponse { outcomes }))
        }
        .await;
        record_bulk_write_metrics(operation_count, encoded_bytes, started.elapsed(), &result);
        result
    }

    type WatchPrefixStream = WatchPrefixStream;

    async fn watch_prefix(
        &self,
        request: Request<WatchPrefixRequest>,
    ) -> Result<Response<Self::WatchPrefixStream>, Status> {
        let caller = authenticated_caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let request = request.into_inner();
        let prefix = request
            .prefix
            .ok_or_else(|| Status::invalid_argument("watch prefix is required"))?;
        let scope =
            WatchScope::new(prefix.tenant, prefix.bucket, prefix.path).map_err(watch_status)?;
        if caller.storage_tenant().as_str() != scope.tenant() {
            return Err(Status::permission_denied(
                "watch prefix does not belong to the authenticated tenant",
            ));
        }
        let start = match request.start {
            Some(WatchStartValue::Now(_)) => distributed_watch_stream::ClusterWatchStart::Now,
            Some(WatchStartValue::RetainedBeginning(_)) => {
                distributed_watch_stream::ClusterWatchStart::RetainedBeginning
            }
            Some(WatchStartValue::ResumeToken(token)) if !token.is_empty() => {
                distributed_watch_stream::ClusterWatchStart::Resume(token)
            }
            Some(WatchStartValue::ResumeToken(_)) => {
                return Err(Status::invalid_argument(
                    "watch resume token must not be empty",
                ));
            }
            None => {
                return Err(Status::invalid_argument(
                    "watch start must be NOW, retained beginning, or a resume token",
                ));
            }
        };
        let (tenant_id, bucket_id) = self
            .name_resolver
            .resolve_bucket_ids(scope.tenant(), scope.bucket())
            .await?;
        let distributed_scope = DistributedWatchScope::new(&scope, tenant_id, bucket_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        distributed_watch_stream::response(self.watch.clone(), distributed_scope, bearer, start)
            .await
    }

    async fn batch_get(
        &self,
        request: Request<BatchGetRequest>,
    ) -> Result<Response<BatchGetResponse>, Status> {
        let deadline = request_deadline(request.metadata(), self.atomic_program_timeout)?;
        let caller = authenticated_caller(&request)?;
        let objects = request.into_inner().objects;
        if objects.len() > MAX_BATCH_GET_ITEMS {
            return Err(Status::resource_exhausted(format!(
                "batch read contains more than {MAX_BATCH_GET_ITEMS} items"
            )));
        }
        let mut accepted = Vec::with_capacity(objects.len());
        let mut outcomes = Vec::new();
        let mut pending = Vec::with_capacity(objects.len());
        for (index, request) in objects.into_iter().enumerate() {
            let address = request.address.clone();
            let key = match object_key(request.address) {
                Ok(key) => key,
                Err(error) => {
                    outcomes.push(BatchGetOutcome {
                        index: index as u32,
                        address,
                        outcome: Some(BatchGetResult::Failure(ReadFailure {
                            code: ReadFailureCode::Invalid as i32,
                            message: error.message().to_owned(),
                        })),
                    });
                    continue;
                }
            };
            match require_caller_tenant(&caller, &key) {
                Ok(()) => pending.push((index, key, request.version.map(VersionId))),
                Err(error) if error.code() == tonic::Code::PermissionDenied => {
                    outcomes.push(batch_get_authorization_failure(index, &key, &error));
                }
                Err(error) => return Err(error),
            }
        }
        let authorization_requests = pending
            .iter()
            .map(|(_, key, _)| (key.clone(), ObjectPermission::Get))
            .collect::<Vec<_>>();
        let allowed = if authorization_requests.is_empty() {
            Vec::new()
        } else {
            self.authoritative_system
                .allows_objects(&caller, &authorization_requests)
                .await?
        };
        for ((index, key, requested_version), allowed) in pending.into_iter().zip(allowed) {
            if !allowed {
                outcomes.push(batch_get_authorization_failure(
                    index,
                    &key,
                    &Status::permission_denied("object read is not authorized"),
                ));
                continue;
            }
            if requested_version.is_some()
                && !bucket_versioning_enabled(&self.store, &key).map_err(status)?
            {
                outcomes.push(BatchGetOutcome {
                    index: index as u32,
                    address: Some(api_address(&key)),
                    outcome: Some(BatchGetResult::Failure(ReadFailure {
                        code: ReadFailureCode::VersioningDisabled as i32,
                        message: "exact-version reads require bucket versioning to be enabled"
                            .into(),
                    })),
                });
                continue;
            }
            accepted.push((index, key, requested_version));
        }
        loop {
            let (read_outcomes, cursor) = batch_get::read_accepted(
                self.distribution.clone(),
                self.reader.clone(),
                accepted.clone(),
                MAX_BATCH_GET_BYTES as u64,
            )
            .await?;
            if let Some(cursor) = cursor
                && !self.programs.cursor_is_visible(cursor)?
            {
                self.programs
                    .wait_for_cursor(cursor, deadline_remaining(deadline)?)
                    .await?;
                continue;
            }
            outcomes.extend(read_outcomes);
            outcomes.sort_unstable_by_key(|outcome| outcome.index);
            return Ok(Response::new(BatchGetResponse { outcomes }));
        }
    }

    async fn set_bucket_policy(
        &self,
        request: Request<SetBucketPolicyRequest>,
    ) -> Result<Response<BucketPolicy>, Status> {
        let peer_routed = request
            .extensions()
            .get::<routed_writes::RoutedDestination>()
            .is_some();
        let caller = authenticated_caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let remaining =
            effective_atomic_program_timeout(request.metadata(), self.atomic_program_timeout);
        let api_request = request.into_inner();
        let policy = api_request
            .policy
            .clone()
            .ok_or_else(|| Status::invalid_argument("policy is required"))?;
        let key = ObjectKey::new(&api_request.tenant, &api_request.bucket, "_anvil/policy")
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        require_caller_tenant(&caller, &key)?;
        require_authorized(
            self.authoritative_system
                .allows_bucket_policy(&caller, &api_request.tenant, &api_request.bucket)
                .await?,
            "bucket policy mutation is not authorized",
        )?;
        let (tenant_id, bucket_id) = self
            .name_resolver
            .resolve_bucket_ids(&api_request.tenant, &api_request.bucket)
            .await?;
        match self.bucket_governance.policy_target(tenant_id, bucket_id)? {
            Some(_) if peer_routed => {
                return Err(Status::failed_precondition(
                    "a routed SetBucketPolicy reached a node that is not its coordinator",
                ));
            }
            Some(target) => {
                return self
                    .cluster_peers
                    .route_set_bucket_policy(
                        target.node_id,
                        &target.address,
                        bearer.signed_token(),
                        api_request,
                        remaining,
                    )
                    .await
                    .map(Response::new);
            }
            None => {
                self.bucket_governance
                    .set_policy_local(
                        tenant_id,
                        bucket_id,
                        anvil_store::BucketPolicy {
                            immutable_prefixes: policy.immutable_path_prefixes.clone(),
                            program_only_prefixes: policy.program_only_path_prefixes.clone(),
                        },
                    )
                    .await?;
            }
        }
        Ok(Response::new(policy))
    }

    async fn invoke_program(
        &self,
        request: Request<InvokeProgramRequest>,
    ) -> Result<Response<InvokeProgramResponse>, Status> {
        atomic_program::invoke(self, request).await
    }
}

fn effective_atomic_program_timeout(metadata: &MetadataMap, server_maximum: Duration) -> Duration {
    client_grpc_timeout(metadata).map_or(server_maximum, |client| client.min(server_maximum))
}

fn request_deadline(
    metadata: &MetadataMap,
    server_maximum: Duration,
) -> Result<tokio::time::Instant, Status> {
    tokio::time::Instant::now()
        .checked_add(effective_atomic_program_timeout(metadata, server_maximum))
        .ok_or_else(|| Status::internal("configured request timeout exceeds clock"))
}

fn deadline_remaining(deadline: tokio::time::Instant) -> Result<Duration, Status> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        Err(Status::deadline_exceeded("request deadline exceeded"))
    } else {
        Ok(remaining)
    }
}

// Tonic enforces the same grpc-timeout grammar at the transport boundary. We
// parse it here as well so InvokeProgram has one explicit absolute budget that
// can be shorter than, but never longer than, the configured server maximum.
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
        "H" => Some(Duration::from_secs(value * 60 * 60)),
        "M" => Some(Duration::from_secs(value * 60)),
        "S" => Some(Duration::from_secs(value)),
        "m" => Some(Duration::from_millis(value)),
        "u" => Some(Duration::from_micros(value)),
        "n" => Some(Duration::from_nanos(value)),
        _ => None,
    }
}

async fn run_atomic_program_until<T, F>(
    deadline: tokio::time::Instant,
    invocation: F,
) -> Result<T, Status>
where
    F: Future<Output = Result<T, Status>>,
{
    tokio::time::timeout_at(deadline, invocation)
        .await
        .map_err(|_| Status::deadline_exceeded("atomic program execution deadline exceeded"))?
}

impl ObjectServiceImpl {
    async fn authorize_object(
        &self,
        caller: &Caller,
        key: &ObjectKey,
        permission: ObjectPermission,
    ) -> Result<(), Status> {
        require_caller_tenant(caller, key)?;
        require_authorized(
            self.authoritative_system
                .allows_object(caller, key, permission)
                .await?,
            "object operation is not authorized",
        )
    }

    fn issue_upload_token(
        &self,
        caller: &Caller,
        metadata: &PutMetadata,
    ) -> Result<PutToken, Status> {
        let operation = match metadata.mode {
            PutMode::Put => TokenPutOperation::Put,
            PutMode::PutIfAbsent => TokenPutOperation::PutIfAbsent,
            PutMode::PutIfVersion(version) => TokenPutOperation::PutIfVersion {
                expected_version: version.0,
            },
            PutMode::PutImmutable => TokenPutOperation::PutImmutable,
        };
        let header = CanonicalPutHeader {
            tenant: metadata.key.tenant().to_owned(),
            bucket: metadata.key.bucket().to_owned(),
            path: metadata.key.path().to_owned(),
            content_type: metadata.content_type.clone(),
            command_id: metadata.command_id.clone(),
            durability: token_durability(metadata.durability)?,
            operation,
        };
        self.issue_put_capability(
            caller,
            CanonicalPutCapability {
                format_version: PUT_TOKEN_FORMAT_VERSION,
                phase: PutTokenPhase::Upload(UploadCapability { header }),
            },
        )
    }

    fn issue_ready_token(
        &self,
        caller: &Caller,
        header: CanonicalPutHeader,
        blob: &BlobRef,
    ) -> Result<PutToken, Status> {
        self.issue_put_capability(
            caller,
            CanonicalPutCapability {
                format_version: PUT_TOKEN_FORMAT_VERSION,
                phase: PutTokenPhase::Ready(ReadyCapability {
                    header,
                    blob_hash: blob.hash,
                    blob_length: blob.length,
                    upload_source_node_id: self.distribution.local_node().0,
                }),
            },
        )
    }

    fn issue_put_capability(
        &self,
        caller: &Caller,
        capability: CanonicalPutCapability,
    ) -> Result<PutToken, Status> {
        let capability = serde_json::to_vec(&capability)
            .map_err(|error| internal(format!("encode put capability: {error}")))?;
        let (value, expires_at_unix_seconds) = self
            .jwt_manager
            .mint_put_token(caller, &capability, PUT_TOKEN_LIFETIME)
            .map_err(|_| Status::internal("could not issue put token"))?;
        let expires_at = UNIX_EPOCH
            .checked_add(Duration::from_secs(expires_at_unix_seconds))
            .ok_or_else(|| Status::internal("put token expiry is out of range"))?;
        Ok(PutToken {
            value: value.into_bytes(),
            expires_at: Some(expires_at.into()),
        })
    }

    fn verify_put_token(
        &self,
        caller: &Caller,
        token: &PutToken,
    ) -> Result<CanonicalPutCapability, Status> {
        let value = std::str::from_utf8(&token.value)
            .map_err(|_| Status::invalid_argument("put token is malformed"))?;
        let claims = self
            .jwt_manager
            .verify_put_token(value)
            .map_err(|_| Status::unauthenticated("put token is invalid or expired"))?;
        if !claims.belongs_to(caller) {
            return Err(Status::permission_denied(
                "put token belongs to a different authenticated caller",
            ));
        }
        let payload: CanonicalPutCapability = serde_json::from_slice(&claims.header)
            .map_err(|_| Status::invalid_argument("put token capability is malformed"))?;
        if payload.format_version != PUT_TOKEN_FORMAT_VERSION {
            return Err(Status::invalid_argument("put token format is unsupported"));
        }
        let expires_at = token
            .expires_at
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("put token expiry is missing"))?;
        if expires_at.seconds < 0
            || expires_at.nanos != 0
            || expires_at.seconds as u64 != claims.expires_at_unix_seconds
        {
            return Err(Status::invalid_argument("put token expiry was modified"));
        }
        Ok(payload)
    }
}

impl CanonicalPutHeader {
    fn to_metadata(&self) -> Result<PutMetadata, Status> {
        let mode = match self.operation {
            TokenPutOperation::Put => PutMode::Put,
            TokenPutOperation::PutIfAbsent => PutMode::PutIfAbsent,
            TokenPutOperation::PutIfVersion { expected_version } => {
                PutMode::PutIfVersion(VersionId(expected_version))
            }
            TokenPutOperation::PutImmutable => PutMode::PutImmutable,
        };
        let durability = match self.durability {
            TokenDurability::Local => StoreDurability::Local,
            TokenDurability::Replicated => StoreDurability::Replicated,
        };
        Ok(PutMetadata {
            key: ObjectKey::new(&self.tenant, &self.bucket, &self.path)
                .map_err(|error| Status::invalid_argument(error.to_string()))?,
            content_type: self.content_type.clone(),
            command_id: required_command_id(self.command_id.clone())?,
            durability,
            mode,
        })
    }
}

fn authenticated_caller<T>(request: &Request<T>) -> Result<Caller, Status> {
    request
        .extensions()
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("authenticated caller identity is missing"))
}

fn require_caller_tenant(caller: &Caller, key: &ObjectKey) -> Result<(), Status> {
    if caller.storage_tenant().as_str() == key.tenant() {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "object address does not belong to the authenticated tenant",
        ))
    }
}

fn require_authorized(allowed: bool, message: &'static str) -> Result<(), Status> {
    if allowed {
        Ok(())
    } else {
        Err(Status::permission_denied(message))
    }
}

fn batch_operation_permission(operation: &BatchOperation) -> ObjectPermission {
    match operation {
        BatchOperation::Put(_) | BatchOperation::Publish(_) => ObjectPermission::Put,
        BatchOperation::Delete(_) => ObjectPermission::Delete,
    }
}

fn batch_operation_key(operation: &BatchOperation) -> &ObjectKey {
    match operation {
        BatchOperation::Put(request) => &request.key,
        BatchOperation::Publish(request) => &request.key,
        BatchOperation::Delete(request) => &request.key,
    }
}

fn bulk_authorization_failure(index: usize, error: &Status) -> BulkOutcome {
    BulkOutcome {
        index: index as u32,
        outcome: Some(anvil_api::v1::bulk_outcome::Outcome::Failure(
            MutationFailure {
                code: MutationFailureCode::AuthorizationDenied as i32,
                message: error.message().to_owned(),
                current_version: None,
            },
        )),
    }
}

fn batch_get_authorization_failure(
    index: usize,
    key: &ObjectKey,
    error: &Status,
) -> BatchGetOutcome {
    BatchGetOutcome {
        index: index as u32,
        address: Some(api_address(key)),
        outcome: Some(BatchGetResult::Failure(ReadFailure {
            code: ReadFailureCode::AuthorizationDenied as i32,
            message: error.message().to_owned(),
        })),
    }
}

fn authorize_program_dependency(
    authorization: &SystemAuthorization,
    caller: &Caller,
    dependency: &ExpandedProgramPath,
) -> Result<(), Status> {
    let key = ObjectKey::new(
        &dependency.path.tenant,
        &dependency.path.bucket,
        &dependency.path.path,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))?;
    require_caller_tenant(caller, &key)?;
    for (required, permission, message) in [
        (
            dependency.intent.get,
            ObjectPermission::Get,
            "atomic program dependency read is not authorized",
        ),
        (
            dependency.intent.put,
            ObjectPermission::Put,
            "atomic program dependency put is not authorized",
        ),
        (
            dependency.intent.delete,
            ObjectPermission::Delete,
            "atomic program dependency delete is not authorized",
        ),
    ] {
        if required {
            require_authorized(
                authorization
                    .allows_object(caller.subject(), &key, permission)
                    .map_err(crate::authz_api::authz_status)?,
                message,
            )?;
        }
    }
    Ok(())
}

async fn authorize_program_dependencies_authoritatively(
    authorization: &AuthoritativeSystemAuthorization,
    governance: &BucketGovernance,
    caller: &Caller,
    dependencies: Vec<ExpandedProgramPath>,
) -> Result<(), Status> {
    let mut requests = Vec::new();
    for dependency in dependencies {
        let key = ObjectKey::new(
            &dependency.path.tenant,
            &dependency.path.bucket,
            &dependency.path.path,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        require_caller_tenant(caller, &key)?;
        let policy = governance.resolve(key.tenant(), key.bucket()).await?;
        if !policy.policy.is_program_only(key.path()) {
            return Err(Status::failed_precondition(
                "PROGRAM_CONCURRENCY_VIOLATION: every atomic program dependency must be PROGRAM_ONLY",
            ));
        }
        for (required, permission) in [
            (dependency.intent.get, ObjectPermission::Get),
            (dependency.intent.put, ObjectPermission::Put),
            (dependency.intent.delete, ObjectPermission::Delete),
        ] {
            if required {
                requests.push((key.clone(), permission));
            }
        }
    }
    let allowed = authorization.allows_objects(caller, &requests).await?;
    if allowed.into_iter().all(|allowed| allowed) {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "atomic program dependency is not authorized",
        ))
    }
}

fn authorization_store_status(error: AuthzStoreError) -> Status {
    match error {
        AuthzStoreError::MissingBinding(_, _) | AuthzStoreError::SchemaNotFound(_, _) => {
            Status::failed_precondition(error.to_string())
        }
        AuthzStoreError::Authorization(error) => crate::authz_api::authz_status(error),
        AuthzStoreError::InvalidInput(_) => Status::invalid_argument(error.to_string()),
        AuthzStoreError::RevisionConflict { .. }
        | AuthzStoreError::BindingGenerationConflict { .. }
        | AuthzStoreError::RevisionNotAvailable { .. }
        | AuthzStoreError::RevisionExpired { .. }
        | AuthzStoreError::OperationMismatch => Status::failed_precondition(error.to_string()),
        AuthzStoreError::ReceiptCapacity => Status::resource_exhausted(error.to_string()),
        AuthzStoreError::RealmMutationLineageGap { .. }
        | AuthzStoreError::RealmMutationStale { .. }
        | AuthzStoreError::RealmMutationSibling { .. }
        | AuthzStoreError::RealmMutationConflict => {
            Status::unavailable("authorization realm replica is not current")
        }
        AuthzStoreError::InvalidRealmMutation(_) => {
            Status::internal("authorization replication input was invalid")
        }
        AuthzStoreError::Storage(_) => Status::internal(error.to_string()),
    }
}

fn api_watch_invalidation(invalidation: LocalInvalidation) -> WatchInvalidation {
    let state_hint = match invalidation.state_hint {
        InvalidationStateHint::Present => WatchStateHint::Present,
        InvalidationStateHint::Deleted => WatchStateHint::Deleted,
    };
    WatchInvalidation {
        address: Some(api_address(&invalidation.key)),
        minimum_path_version: invalidation.minimum_path_version.0,
        state_hint: state_hint as i32,
    }
}

fn watch_status(error: WatchError) -> Status {
    match &error {
        WatchError::InvalidConfiguration(_) | WatchError::InvalidScope(_) => {
            Status::invalid_argument(error.to_string())
        }
        WatchError::InvalidResumeToken => Status::invalid_argument(error.to_string()),
        WatchError::ResumeExpired => Status::failed_precondition("RESUME_EXPIRED"),
        WatchError::Storage(_) => Status::internal(error.to_string()),
    }
}

fn validate_bulk_limits(operations: &[BulkOperation]) -> Result<(), Status> {
    if operations.len() > MAX_BULK_ITEMS {
        return Err(Status::resource_exhausted(format!(
            "bulk contains more than {MAX_BULK_ITEMS} items"
        )));
    }
    enforce_bulk_encoded_limit(bulk_encoded_len(operations)?)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BulkMetricCounts {
    successful: u64,
    failed: u64,
    replayed: u64,
}

fn bulk_metric_counts(
    operation_count: u64,
    result: &Result<Response<BulkWriteResponse>, Status>,
) -> BulkMetricCounts {
    let Ok(response) = result else {
        return BulkMetricCounts {
            failed: operation_count,
            ..Default::default()
        };
    };

    let mut counts = BulkMetricCounts::default();
    for outcome in &response.get_ref().outcomes {
        match outcome.outcome.as_ref() {
            Some(anvil_api::v1::bulk_outcome::Outcome::Receipt(receipt)) if receipt.replayed => {
                counts.replayed += 1;
            }
            Some(anvil_api::v1::bulk_outcome::Outcome::Receipt(_)) => counts.successful += 1,
            Some(anvil_api::v1::bulk_outcome::Outcome::Failure(_)) | None => counts.failed += 1,
        }
    }
    let reported = counts.successful + counts.failed + counts.replayed;
    counts.failed += operation_count.saturating_sub(reported);
    counts
}

fn record_bulk_write_metrics(
    operation_count: u64,
    encoded_bytes: u64,
    duration: Duration,
    result: &Result<Response<BulkWriteResponse>, Status>,
) {
    let counts = bulk_metric_counts(operation_count, result);
    tracing::info!(
        monotonic_counter.anvil_bulk_operations_total = operation_count,
        monotonic_counter.anvil_bulk_encoded_bytes_total = encoded_bytes,
        monotonic_counter.anvil_bulk_successful_operations_total = counts.successful,
        monotonic_counter.anvil_bulk_failed_operations_total = counts.failed,
        monotonic_counter.anvil_bulk_replayed_operations_total = counts.replayed,
        histogram.anvil_bulk_request_duration_seconds = duration.as_secs_f64(),
        operation_count,
        encoded_bytes,
        successful = counts.successful,
        failed = counts.failed,
        replayed = counts.replayed,
        "bulk write request completed"
    );
}

#[cfg(test)]
fn watch_consumer_lag(status: &WatchJournalStatus, cursor_offset: u64) -> Option<u64> {
    status.tail.checked_sub(cursor_offset)
}

fn bulk_encoded_len(operations: &[BulkOperation]) -> Result<usize, Status> {
    let mut encoded_bytes = 0_usize;
    for operation in operations {
        let operation_bytes = operation.encoded_len();
        encoded_bytes = encoded_bytes
            .checked_add(1)
            .and_then(|total| total.checked_add(protobuf_varint_len(operation_bytes)))
            .and_then(|total| total.checked_add(operation_bytes))
            .ok_or_else(|| Status::resource_exhausted("bulk encoded size overflow"))?;
    }
    Ok(encoded_bytes)
}

fn enforce_bulk_encoded_limit(encoded_bytes: usize) -> Result<(), Status> {
    if encoded_bytes > MAX_BULK_BYTES {
        return Err(Status::resource_exhausted(format!(
            "bulk encoded request exceeds {MAX_BULK_BYTES} bytes"
        )));
    }
    Ok(())
}

fn protobuf_varint_len(mut value: usize) -> usize {
    let mut bytes = 1;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

fn put_metadata(request: PutHeader) -> Result<PutMetadata, Status> {
    let mode = match request.operation {
        Some(ApiPutOperation::Put(_)) => PutMode::Put,
        Some(ApiPutOperation::PutIfAbsent(_)) => PutMode::PutIfAbsent,
        Some(ApiPutOperation::PutIfVersion(request)) => {
            PutMode::PutIfVersion(VersionId(request.expected_version))
        }
        Some(ApiPutOperation::PutImmutable(_)) => PutMode::PutImmutable,
        None => return Err(Status::invalid_argument("put operation is required")),
    };
    Ok(PutMetadata {
        key: object_key(request.address)?,
        content_type: content_type(request.content_type)?,
        command_id: required_command_id(request.command_id)?,
        durability: durability(request.durability)?,
        mode,
    })
}

fn bulk_put_request(request: BulkPutRequest, mode: PutMode) -> Result<StorePutRequest, Status> {
    Ok(StorePutRequest {
        key: object_key(request.address)?,
        bytes: request.bytes,
        content_type: content_type(request.content_type)?,
        mode,
        command_id: Some(required_command_id(request.command_id)?),
        durability: durability(request.durability)?,
    })
}

fn bulk_put_if_version_request(
    request: BulkPutIfVersionRequest,
) -> Result<StorePutRequest, Status> {
    let mode = PutMode::PutIfVersion(VersionId(request.expected_version));
    Ok(StorePutRequest {
        key: object_key(request.address)?,
        bytes: request.bytes,
        content_type: content_type(request.content_type)?,
        mode,
        command_id: Some(required_command_id(request.command_id)?),
        durability: durability(request.durability)?,
    })
}

fn delete_request(
    request: ApiDeleteRequest,
    precondition: Precondition,
) -> Result<StoreDeleteRequest, Status> {
    Ok(StoreDeleteRequest {
        key: object_key(request.address)?,
        precondition,
        command_id: Some(required_command_id(request.command_id)?),
        durability: durability(request.durability)?,
    })
}

fn delete_if_version_request(
    request: DeleteIfVersionRequest,
    precondition: Precondition,
) -> Result<StoreDeleteRequest, Status> {
    Ok(StoreDeleteRequest {
        key: object_key(request.address)?,
        precondition,
        command_id: Some(required_command_id(request.command_id)?),
        durability: durability(request.durability)?,
    })
}

fn batch_operation(
    operation: BulkOperation,
    max_blob_bytes: u64,
) -> Result<BatchOperation, Status> {
    let operation = match operation.operation {
        Some(anvil_api::v1::bulk_operation::Operation::Put(request)) => {
            BatchOperation::Put(bulk_put_request(request, PutMode::Put)?)
        }
        Some(anvil_api::v1::bulk_operation::Operation::PutIfAbsent(request)) => {
            BatchOperation::Put(bulk_put_request(request, PutMode::PutIfAbsent)?)
        }
        Some(anvil_api::v1::bulk_operation::Operation::PutIfVersion(request)) => {
            BatchOperation::Put(bulk_put_if_version_request(request)?)
        }
        Some(anvil_api::v1::bulk_operation::Operation::PutImmutable(request)) => {
            BatchOperation::Put(bulk_put_request(request, PutMode::PutImmutable)?)
        }
        Some(anvil_api::v1::bulk_operation::Operation::Delete(request)) => {
            BatchOperation::Delete(delete_request(request, Precondition::Any)?)
        }
        Some(anvil_api::v1::bulk_operation::Operation::DeleteIfVersion(request)) => {
            let version = VersionId(request.expected_version);
            BatchOperation::Delete(delete_if_version_request(
                request,
                Precondition::Version(version),
            )?)
        }
        None => return Err(Status::invalid_argument("bulk operation is required")),
    };
    match &operation {
        BatchOperation::Put(request) => {
            if request.bytes.len() as u64 > max_blob_bytes {
                return Err(Status::resource_exhausted(
                    "bulk put item exceeds the object-size limit",
                ));
            }
        }
        BatchOperation::Publish(_) | BatchOperation::Delete(_) => {}
    }
    Ok(operation)
}

fn api_request_failure(error: Status) -> MutationFailure {
    let code = match error.code() {
        tonic::Code::ResourceExhausted => MutationFailureCode::ResourceLimit,
        tonic::Code::Unavailable => MutationFailureCode::DurabilityUnavailable,
        _ => MutationFailureCode::Invalid,
    };
    MutationFailure {
        code: code as i32,
        message: error.message().to_owned(),
        current_version: None,
    }
}

fn object_key(address: Option<ObjectAddress>) -> Result<ObjectKey, Status> {
    let address = address.ok_or_else(|| Status::invalid_argument("object address is required"))?;
    ObjectKey::new(address.tenant, address.bucket, address.path)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn list_objects_query(request: ListObjectsRequest) -> Result<ListObjectsQuery, Status> {
    if request.prefix.len() > MAX_OBJECT_PATH_BYTES {
        return Err(Status::invalid_argument(format!(
            "list prefix exceeds {MAX_OBJECT_PATH_BYTES} UTF-8 bytes"
        )));
    }
    let validation_path = request.start_after.as_deref().unwrap_or("_list");
    ObjectKey::new(&request.tenant, &request.bucket, validation_path)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let limit = match request.limit as usize {
        0 => DEFAULT_LIST_OBJECTS_LIMIT,
        limit if limit <= MAX_LIST_OBJECTS => limit,
        _ => {
            return Err(Status::invalid_argument(format!(
                "list limit must not exceed {MAX_LIST_OBJECTS}"
            )));
        }
    };
    Ok(ListObjectsQuery {
        tenant: request.tenant,
        bucket: request.bucket,
        prefix: request.prefix,
        start_after: request.start_after,
        limit,
    })
}

fn required_hash(value: &[u8], name: &'static str) -> Result<[u8; 32], Status> {
    let hash: [u8; 32] = value
        .try_into()
        .map_err(|_| Status::invalid_argument(format!("{name} must contain 32 bytes")))?;
    if hash == [0; 32] {
        return Err(Status::invalid_argument(format!(
            "{name} must not be all zeroes"
        )));
    }
    Ok(hash)
}

fn api_head(version: &Version) -> Result<ObjectHead, Status> {
    let state = if version.deleted {
        ObjectState::Deleted(DeletedObject {
            version: version.id.0,
        })
    } else {
        let blob = version
            .blob
            .as_ref()
            .ok_or_else(|| Status::data_loss("live version has no payload reference"))?;
        ObjectState::Present(PresentObject {
            version: version.id.0,
            content_hash: blob.hash.to_vec(),
            content_length: blob.length,
            content_type: version.content_type.clone().unwrap_or_default(),
        })
    };
    Ok(ObjectHead { state: Some(state) })
}

fn api_object_version(version: &Version) -> Result<ObjectVersion, Status> {
    let state = match api_head(version)?.state {
        Some(ObjectState::Present(present)) => ObjectVersionState::Present(present),
        Some(ObjectState::Deleted(deleted)) => ObjectVersionState::Deleted(deleted),
        Some(ObjectState::NeverExisted(_)) | None => {
            return Err(Status::data_loss(
                "stored version cannot have a never-existed state",
            ));
        }
    };
    Ok(ObjectVersion { state: Some(state) })
}

fn api_delete_version_outcome(outcome: DeleteRetainedVersionOutcome) -> DeleteVersionResponse {
    match outcome {
        DeleteRetainedVersionOutcome::NotFound => DeleteVersionResponse {
            deleted: false,
            replacement_tombstone_version: None,
        },
        DeleteRetainedVersionOutcome::DeletedNonCurrent => DeleteVersionResponse {
            deleted: true,
            replacement_tombstone_version: None,
        },
        DeleteRetainedVersionOutcome::ReplacedCurrentWithTombstone { version } => {
            DeleteVersionResponse {
                deleted: true,
                replacement_tombstone_version: Some(version.0),
            }
        }
    }
}

fn bucket_versioning_enabled(store: &Store, key: &ObjectKey) -> Result<bool, MutationError> {
    store
        .bucket_versioning(key.tenant(), key.bucket())
        .map(|versioning| versioning == StoreObjectVersioning::Enabled)
}

fn require_versioning_enabled(store: &Store, key: &ObjectKey) -> Result<(), Status> {
    if bucket_versioning_enabled(store, key).map_err(status)? {
        Ok(())
    } else {
        Err(Status::failed_precondition(
            "bucket versioning is not enabled",
        ))
    }
}

fn never_existed() -> ObjectHead {
    ObjectHead {
        state: Some(ObjectState::NeverExisted(NeverExisted {})),
    }
}

fn api_receipt(receipt: MutationReceipt) -> ApiMutationReceipt {
    let replay_guarantee_expires_at = UNIX_EPOCH
        .checked_add(Duration::from_millis(
            receipt.replay_guarantee_expires_at_unix_millis,
        ))
        .map(Into::into);
    ApiMutationReceipt {
        command_id: receipt.command_id.unwrap_or_default(),
        version: receipt.version.0,
        deleted: receipt.deleted,
        replayed: receipt.replayed,
        replay_guarantee_expires_at,
    }
}

fn api_address(key: &ObjectKey) -> ObjectAddress {
    ObjectAddress {
        tenant: key.tenant().into(),
        bucket: key.bucket().into(),
        path: key.path().into(),
    }
}

fn api_failure(error: MutationError) -> MutationFailure {
    let (code, current_version) = match &error {
        MutationError::PreconditionFailed { current } => (
            MutationFailureCode::ConditionFailed,
            current.map(|value| value.0),
        ),
        MutationError::Immutable => (MutationFailureCode::Immutable, None),
        MutationError::ImmutablePolicyRequired => {
            (MutationFailureCode::ImmutablePolicyRequired, None)
        }
        MutationError::ProgramConcurrencyViolation => {
            (MutationFailureCode::ProgramConcurrencyViolation, None)
        }
        MutationError::CurrentTombstoneCannotBeDeleted => {
            (MutationFailureCode::ConditionFailed, None)
        }
        MutationError::ObjectVersioningNotEnabled => (MutationFailureCode::ConditionFailed, None),
        MutationError::IdempotencyConflict => (MutationFailureCode::IdempotencyInputMismatch, None),
        MutationError::InvalidCommandId
        | MutationError::InvalidPolicy(_)
        | MutationError::InvalidObjectMutation(_)
        | MutationError::BlobNotFound => (MutationFailureCode::Invalid, None),
        MutationError::DurabilityUnavailable => (MutationFailureCode::DurabilityUnavailable, None),
        MutationError::ReceiptCapacity | MutationError::SourceJournalCapacity => {
            (MutationFailureCode::ResourceLimit, None)
        }
        MutationError::ObjectMutationLineageGap { .. }
        | MutationError::ObjectMutationSibling { .. }
        | MutationError::ObjectMutationConflict => (MutationFailureCode::Internal, None),
        MutationError::Storage(_) => (MutationFailureCode::Internal, None),
    };
    MutationFailure {
        code: code as i32,
        message: error.to_string(),
        current_version,
    }
}

fn status(error: MutationError) -> Status {
    match error {
        MutationError::ProgramConcurrencyViolation => {
            Status::failed_precondition(format!("PROGRAM_CONCURRENCY_VIOLATION: {error}"))
        }
        MutationError::PreconditionFailed { .. }
        | MutationError::Immutable
        | MutationError::ImmutablePolicyRequired
        | MutationError::ObjectVersioningNotEnabled => {
            Status::failed_precondition(error.to_string())
        }
        MutationError::CurrentTombstoneCannotBeDeleted => Status::failed_precondition(format!(
            "CURRENT_TOMBSTONE_VERSION_CANNOT_BE_DELETED: {error}"
        )),
        MutationError::IdempotencyConflict => Status::already_exists(error.to_string()),
        MutationError::InvalidCommandId
        | MutationError::InvalidPolicy(_)
        | MutationError::InvalidObjectMutation(_) => Status::invalid_argument(error.to_string()),
        MutationError::BlobNotFound => Status::not_found(error.to_string()),
        MutationError::DurabilityUnavailable => {
            Status::unavailable(format!("DURABILITY_UNAVAILABLE: {error}"))
        }
        MutationError::ReceiptCapacity | MutationError::SourceJournalCapacity => {
            Status::resource_exhausted(error.to_string())
        }
        MutationError::ObjectMutationLineageGap { .. }
        | MutationError::ObjectMutationSibling { .. }
        | MutationError::ObjectMutationConflict => {
            Status::unavailable(format!("MUTATION_REPLICA_UNAVAILABLE: {error}"))
        }
        MutationError::Storage(_) => Status::internal(error.to_string()),
    }
}

fn durability(value: i32) -> Result<StoreDurability, Status> {
    match ApiDurability::try_from(value) {
        Ok(ApiDurability::Local) => Ok(StoreDurability::Local),
        Ok(ApiDurability::Replicated) => Ok(StoreDurability::Replicated),
        Err(_) => Err(Status::invalid_argument("durability is unknown")),
    }
}

fn token_durability(value: StoreDurability) -> Result<TokenDurability, Status> {
    match value {
        StoreDurability::Local => Ok(TokenDurability::Local),
        StoreDurability::Replicated => Ok(TokenDurability::Replicated),
    }
}

fn durability_name(value: StoreDurability) -> &'static str {
    match value {
        StoreDurability::Local => "local",
        StoreDurability::Replicated => "replicated",
    }
}

fn required_put_token(value: Option<PutToken>) -> Result<PutToken, Status> {
    match value {
        Some(token) if !token.value.is_empty() => Ok(token),
        _ => Err(Status::invalid_argument("put token is required")),
    }
}

fn require_upload_phase(capability: CanonicalPutCapability) -> Result<CanonicalPutHeader, Status> {
    match capability.phase {
        PutTokenPhase::Upload(upload) => Ok(upload.header),
        PutTokenPhase::Ready(_) => Err(Status::failed_precondition(
            "READY put token cannot start an upload",
        )),
    }
}

fn require_ready_phase(capability: CanonicalPutCapability) -> Result<ReadyCapability, Status> {
    match capability.phase {
        PutTokenPhase::Ready(ready) => Ok(ready),
        PutTokenPhase::Upload(_) => Err(Status::failed_precondition(
            "UPLOAD put token cannot publish an object",
        )),
    }
}

async fn write_upload_chunk(
    upload: &mut BlobUpload,
    length: &mut u64,
    bytes: &[u8],
    max_blob_bytes: u64,
) -> Result<(), Status> {
    *length = length
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| Status::resource_exhausted("object length overflow"))?;
    if *length > max_blob_bytes {
        return Err(Status::resource_exhausted("object exceeds server limit"));
    }
    upload.write(bytes).await.map_err(internal)
}

fn enforce_batch_get_payload_limit(declared_payload_bytes: u64) -> Result<(), Status> {
    if declared_payload_bytes > MAX_BATCH_GET_BYTES as u64 {
        return Err(Status::resource_exhausted(format!(
            "batch response exceeds {MAX_BATCH_GET_BYTES} bytes"
        )));
    }
    Ok(())
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

fn content_type(value: String) -> Result<Option<String>, Status> {
    if value.len() > MAX_CONTENT_TYPE_BYTES {
        Err(Status::invalid_argument(format!(
            "content_type exceeds {MAX_CONTENT_TYPE_BYTES} UTF-8 bytes"
        )))
    } else {
        Ok((!value.is_empty()).then_some(value))
    }
}

fn required_command_id(value: String) -> Result<String, Status> {
    if value.is_empty() || value.len() > 256 || value.contains('\0') {
        Err(Status::invalid_argument(
            "command_id must contain 1 to 256 bytes and no NUL",
        ))
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests;
