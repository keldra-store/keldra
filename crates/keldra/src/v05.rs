use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use keldra_api::v1::batch_get_outcome::Outcome as BatchGetResult;
use keldra_api::v1::object_chunk::Value as ObjectChunkValue;
use keldra_api::v1::object_head::State as ObjectState;
use keldra_api::v1::object_service_server::ObjectService;
use keldra_api::v1::object_version::State as ObjectVersionState;
use keldra_api::v1::put_header::Operation as ApiPutOperation;
use keldra_api::v1::watch_prefix_request::Start as WatchStartValue;
use keldra_api::v1::{
    BatchGetObject, BatchGetOutcome, BatchGetRequest, BatchGetResponse, BucketPolicy,
    BulkOperation, BulkOutcome, BulkPutIfVersionRequest, BulkPutRequest, BulkWriteRequest,
    BulkWriteResponse, DeleteIfVersionRequest, DeleteRequest as ApiDeleteRequest,
    DeleteVersionRequest, DeleteVersionResponse, DeletedObject, Durability as ApiDurability,
    GetObjectRequest, HeadObjectRequest, InvokeProgramRequest, InvokeProgramResponse,
    LinkObjectRequest, ListObjectVersionsRequest, ListObjectsRequest, ListObjectsResponse,
    MutationFailure, MutationFailureCode, MutationReceipt as ApiMutationReceipt, NeverExisted,
    ObjectAddress, ObjectChunk, ObjectHead, ObjectVersion, PresentObject, PutHeader,
    PutRequest as ApiPutRequest, PutToken, ReadFailure, ReadFailureCode, SetBucketPolicyRequest,
    UnlinkObjectRequest, WatchInvalidation, WatchPrefixRequest, WatchStateHint,
};
use keldra_atomic_program::ExpandedProgramPath;
use keldra_store::{
    AuthzStoreError, BatchOperation, BlobRef, BlobUpload, DeleteRequest as StoreDeleteRequest,
    DeleteRetainedVersionOutcome, Durability as StoreDurability, InvalidationStateHint,
    LocalInvalidation, MutationError, MutationReceipt, ObjectKey,
    ObjectVersioning as StoreObjectVersioning, Precondition, PublishRequest, PutMode,
    PutRequest as StorePutRequest, Store, Version, VersionId, WatchError, WatchScope,
};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use tokio_stream::Stream;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};

use crate::accounting::AccountingTraffic;
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
use crate::object_path_access;
use crate::programs::ProgramCoordinator;

#[cfg(test)]
use keldra_store::WatchJournalStatus;

mod accounting_traffic;
mod atomic_program;
mod batch_get;
mod bulk;
mod bulk_alias;
mod clone;
mod delete_version;
mod distributed_reads;
mod distributed_watch_stream;
mod gateway;
mod list_query;
mod mutation_failures;
mod object_link;
mod object_read;
mod read_identity;
mod request_auth;
mod routed_writes;
mod upload;

use list_query::{list_objects_query, list_objects_scoped};
#[cfg(test)]
use mutation_failures::api_failure;
use mutation_failures::{api_mutation_failure, api_request_failure};
use read_identity::ObjectReadIdentity;
use request_auth::{
    authenticated_caller, content_type, plugin_object_scope, reject_plugin_token,
    require_authorized, require_caller_tenant, require_plugin_key_scope, require_plugin_list_scope,
    required_command_id,
};

pub(crate) use gateway::{GatewayIdentity, GatewayObjectAdapter, GatewayPutMode};

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
    accounting_traffic: AccountingTraffic,
    max_blob_bytes: u64,
    atomic_program_timeout: Duration,
    bulk_write_timeout: Duration,
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
        accounting_traffic: AccountingTraffic,
        max_blob_bytes: u64,
        atomic_program_timeout: Duration,
        bulk_write_timeout: Duration,
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
            accounting_traffic,
            max_blob_bytes,
            atomic_program_timeout,
            bulk_write_timeout,
        }
    }

    pub(crate) fn is_single_node(&self) -> Result<bool, Status> {
        self.distribution.is_single_node()
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
    #[serde(default)]
    link: Option<CanonicalLinkBinding>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalLinkBinding {
    path: String,
    descriptor_version: u64,
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
    link: Option<keldra_store::ResolvedObjectLink>,
    content_type: Option<String>,
    command_id: String,
    durability: StoreDurability,
    mode: PutMode,
}

pub(crate) type GetObjectStream =
    Pin<Box<dyn Stream<Item = Result<ObjectChunk, Status>> + Send + Sync + 'static>>;
type ListObjectVersionsStream =
    Pin<Box<dyn Stream<Item = Result<ObjectVersion, Status>> + Send + Sync + 'static>>;
type WatchPrefixStream = distributed_watch_stream::ClusterWatchStream;

#[tonic::async_trait]
impl ObjectService for ObjectServiceImpl {
    async fn start_put(&self, request: Request<PutHeader>) -> Result<Response<PutToken>, Status> {
        upload::start_put(self, request).await
    }

    async fn put(
        &self,
        request: Request<Streaming<ApiPutRequest>>,
    ) -> Result<Response<PutToken>, Status> {
        upload::put(self, request).await
    }

    async fn put_end(
        &self,
        request: Request<PutToken>,
    ) -> Result<Response<ApiMutationReceipt>, Status> {
        upload::put_end(self, request).await
    }

    async fn clone_object(
        &self,
        request: Request<keldra_api::v1::CloneObjectRequest>,
    ) -> Result<Response<ApiMutationReceipt>, Status> {
        clone::clone_object(self, request).await
    }

    async fn link_object(
        &self,
        request: Request<LinkObjectRequest>,
    ) -> Result<Response<ApiMutationReceipt>, Status> {
        object_link::link_object(self, request).await
    }

    async fn unlink_object(
        &self,
        request: Request<UnlinkObjectRequest>,
    ) -> Result<Response<ApiMutationReceipt>, Status> {
        object_link::unlink_object(self, request).await
    }

    async fn delete(
        &self,
        request: Request<ApiDeleteRequest>,
    ) -> Result<Response<ApiMutationReceipt>, Status> {
        let plugin_scope = plugin_object_scope(&request);
        let peer_routed = routed_writes::is_routed(&request);
        let replay_marker = routed_writes::atomic_executor_replay_marker(&request);
        let caller = authenticated_caller(&request)?;
        let path_access = object_path_access::access_for(&request);
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let deadline = request_deadline(request.metadata(), self.atomic_program_timeout)?;
        let api_request = request.get_ref().clone();
        let mutation = delete_request(api_request.clone(), Precondition::Any)?;
        object_path_access::require_key(&path_access, &mutation.key)?;
        require_plugin_key_scope(plugin_scope.as_ref(), &mutation.key)?;
        match object_link::route_or_replay_delete(
            self,
            &caller,
            &path_access,
            plugin_scope.as_ref(),
            peer_routed,
            replay_marker,
            bearer.signed_token(),
            &api_request,
            &mutation,
            deadline_remaining(deadline)?,
        )
        .await?
        {
            object_link::DeleteReplayCheck::Response(response) => return Ok(response),
            object_link::DeleteReplayCheck::Checked => {}
        }
        if let object_link::ResolvedAddress::Link(_) =
            object_link::resolve_current(self, mutation.key.clone()).await?
        {
            return object_link::unlink_object(
                self,
                request.map(|request| UnlinkObjectRequest {
                    link: request.address,
                    command_id: request.command_id,
                    durability: request.durability,
                }),
            )
            .await;
        }
        self.authorize_object(&caller, &mutation.key, ObjectPermission::Delete)
            .await?;
        object_link::require_no_inbound_links(self, &mutation.key).await?;
        let receipt = match self.distribution.routing_target(&mutation.key)? {
            Some((target, address)) => {
                self.cluster_peers
                    .route_delete(
                        target,
                        &address,
                        bearer.signed_token(),
                        api_request,
                        true,
                        deadline_remaining(deadline)?,
                    )
                    .await?
            }
            None => api_receipt(
                run_request_until(
                    deadline,
                    self.distribution.mutate(BatchOperation::Delete(mutation)),
                    "delete deadline exceeded",
                )
                .await?,
            ),
        };
        Ok(Response::new(receipt))
    }

    async fn delete_if_version(
        &self,
        request: Request<DeleteIfVersionRequest>,
    ) -> Result<Response<ApiMutationReceipt>, Status> {
        let plugin_scope = plugin_object_scope(&request);
        let peer_routed = routed_writes::is_routed(&request);
        let replay_marker = routed_writes::atomic_executor_replay_marker(&request);
        let caller = authenticated_caller(&request)?;
        let path_access = object_path_access::access_for(&request);
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let deadline = request_deadline(request.metadata(), self.atomic_program_timeout)?;
        let api_request = request.get_ref().clone();
        let precondition = Precondition::Version(VersionId(api_request.expected_version));
        let mutation = delete_if_version_request(api_request.clone(), precondition)?;
        object_path_access::require_key(&path_access, &mutation.key)?;
        require_plugin_key_scope(plugin_scope.as_ref(), &mutation.key)?;
        match object_link::route_or_replay_conditional_delete(
            self,
            &caller,
            &path_access,
            plugin_scope.as_ref(),
            peer_routed,
            replay_marker,
            object_path_access::is_internal(&path_access),
            bearer.signed_token(),
            &api_request,
            &mutation,
            deadline_remaining(deadline)?,
        )
        .await?
        {
            object_link::DeleteReplayCheck::Response(response) => return Ok(response),
            object_link::DeleteReplayCheck::Checked => {}
        }
        if let object_link::ResolvedAddress::Link(_) =
            object_link::resolve_current(self, mutation.key.clone()).await?
        {
            return object_link::delete_if_version_link(self, request).await;
        }
        self.authorize_object(&caller, &mutation.key, ObjectPermission::Delete)
            .await?;
        object_link::require_no_inbound_links(self, &mutation.key).await?;
        let receipt = match self.distribution.routing_target(&mutation.key)? {
            Some((target, address)) => {
                if object_path_access::is_internal(&path_access) {
                    self.cluster_peers
                        .route_internal_delete_if_version(
                            target,
                            &address,
                            bearer.signed_token(),
                            api_request,
                            true,
                            deadline_remaining(deadline)?,
                        )
                        .await?
                } else {
                    self.cluster_peers
                        .route_delete_if_version(
                            target,
                            &address,
                            bearer.signed_token(),
                            api_request,
                            true,
                            deadline_remaining(deadline)?,
                        )
                        .await?
                }
            }
            None => api_receipt(
                run_request_until(
                    deadline,
                    self.distribution.mutate(BatchOperation::Delete(mutation)),
                    "conditional delete deadline exceeded",
                )
                .await?,
            ),
        };
        Ok(Response::new(receipt))
    }

    async fn delete_version(
        &self,
        request: Request<DeleteVersionRequest>,
    ) -> Result<Response<DeleteVersionResponse>, Status> {
        delete_version::delete_version(self, request).await
    }

    async fn head_object(
        &self,
        request: Request<HeadObjectRequest>,
    ) -> Result<Response<ObjectHead>, Status> {
        object_read::head_object(self, request).await
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let plugin_scope = plugin_object_scope(&request);
        let identity = ObjectReadIdentity::from_request(&request)?;
        let bearer = identity.original_bearer(request.metadata())?;
        let query = list_objects_query(request.into_inner())?;
        require_plugin_list_scope(
            plugin_scope.as_ref(),
            &query.tenant,
            &query.bucket,
            &query.prefix,
        )?;
        let caller = identity.caller_for_tenant(&query.tenant)?;
        if caller.storage_tenant().as_str() != query.tenant.as_str() {
            return Err(Status::permission_denied(
                "object list does not belong to the authenticated tenant",
            ));
        }
        let (tenant_id, bucket_id) = self
            .name_resolver
            .resolve_bucket_ids(&query.tenant, &query.bucket)
            .await?;
        Ok(Response::new(
            list_objects_scoped(
                self,
                bearer,
                tenant_id,
                bucket_id,
                query,
                plugin_scope.as_ref(),
            )
            .await?,
        ))
    }

    type GetObjectStream = GetObjectStream;

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        object_read::get_object(self, request).await
    }

    type ListObjectVersionsStream = ListObjectVersionsStream;

    async fn list_object_versions(
        &self,
        request: Request<ListObjectVersionsRequest>,
    ) -> Result<Response<Self::ListObjectVersionsStream>, Status> {
        object_read::list_object_versions(self, request).await
    }

    async fn bulk_write(
        &self,
        request: Request<BulkWriteRequest>,
    ) -> Result<Response<BulkWriteResponse>, Status> {
        let plugin_scope = plugin_object_scope(&request);
        let peer_routed = request
            .extensions()
            .get::<routed_writes::RoutedDestination>()
            .is_some();
        let started = Instant::now();
        let operation_count = request.get_ref().operations.len() as u64;
        let encoded_bytes = request.get_ref().encoded_len() as u64;
        let result = async {
            let caller = authenticated_caller(&request)?;
            let path_access = object_path_access::access_for(&request);
            let meter_public = !peer_routed && !object_path_access::is_internal(&path_access);
            let bearer = OriginalBearer::from_metadata(request.metadata())?;
            let deadline = request_deadline(request.metadata(), self.bulk_write_timeout)?;
            let route_budget =
                effective_request_timeout(request.metadata(), self.bulk_write_timeout);
            let operations = request.into_inner().operations;
            validate_bulk_limits(&operations)?;
            if operations.iter().any(bulk::requests_replicated_durability) {
                self.distribution.wait_for_joining_replica(deadline).await?;
            }
            object_path_access::validate_definition_intents(&path_access, operations.len())?;
            let mut local = Vec::with_capacity(operations.len());
            let mut remote = BTreeMap::<
                Vec<u64>,
                (
                    keldra_consensus::NodeId,
                    String,
                    Vec<(
                        usize,
                        BulkOperation,
                        Option<keldra_store::DefinitionMutationIntent>,
                    )>,
                ),
            >::new();
            let prepared = bulk_alias::prepare_before_live_dispatch(
                self,
                &caller,
                &path_access,
                plugin_scope.as_ref(),
                operations,
                peer_routed,
                deadline,
            )
            .await?;
            let validation_duration = prepared.validation_duration;
            let authorization_duration = prepared.authorization_duration;
            let replay_identity_duration = prepared.identity_resolution_duration;
            let mut outcomes = prepared.outcomes;
            let prepared_items = prepared.items;
            let stable_buckets = prepared.stable_buckets;
            let placement = self.distribution.current_program_placement()?;
            let single_node = placement.active_node_ids().len() == 1;
            let identity_duration = replay_identity_duration;
            let routing_started = Instant::now();
            let mut accounting_inbound = Vec::<(u64, u64, String, u64)>::new();
            let mut alias_items = Vec::new();
            for item in prepared_items {
                let index = item.index;
                let key = item.requested;
                if let Some(receipt) = item.replay_receipt {
                    if meter_public && item.inbound_bytes != 0 {
                        self.record_accounting_inbound(&key, item.inbound_bytes);
                    }
                    outcomes.push(BulkOutcome {
                        index: index as u32,
                        outcome: Some(keldra_api::v1::bulk_outcome::Outcome::Receipt(receipt)),
                    });
                    continue;
                }
                let operation = item
                    .operation
                    .ok_or_else(|| Status::internal("prepared bulk operation is absent"))?;
                let definition_intent = item.definition_intent;
                let resolution = item
                    .resolution
                    .ok_or_else(|| Status::internal("prepared bulk resolution is absent"))?;
                if let object_link::ResolvedAddress::Link(link) = resolution {
                    alias_items.push(bulk_alias::AliasBulkItem {
                        index,
                        operation,
                        link,
                    });
                    continue;
                }
                let (tenant_id, bucket_id) = stable_buckets
                    .get(key.tenant())
                    .and_then(|tenant| tenant.get(key.bucket()))
                    .copied()
                    .ok_or_else(|| Status::internal("bulk stable bucket identity is missing"))?;
                let target = if single_node {
                    None
                } else {
                    let group = self
                        .distribution
                        .object_replica_group_stable(&placement, &key, tenant_id, bucket_id)?;
                    let coordinator = group.coordinator();
                    (coordinator != self.distribution.local_node())
                        .then(|| {
                            let address = placement.address(coordinator).ok_or_else(|| {
                                Status::unavailable(format!(
                                    "ACTIVE object coordinator {} has no peer address",
                                    coordinator.0
                                ))
                            })?;
                            Ok::<_, Status>((
                                group
                                    .replicas()
                                    .iter()
                                    .map(|node| node.0)
                                    .collect::<Vec<_>>(),
                                coordinator,
                                address.0.clone(),
                            ))
                        })
                        .transpose()?
                };
                match target {
                    Some((group_key, target, address)) => {
                        if meter_public {
                            let bytes = bulk::operation_inbound_bytes(&operation);
                            if bytes != 0 {
                                accounting_inbound.push((
                                    tenant_id,
                                    bucket_id,
                                    key.path().to_owned(),
                                    bytes,
                                ));
                            }
                        }
                        remote
                            .entry(group_key)
                            .or_insert_with(|| (target, address, Vec::new()))
                            .2
                            .push((index, operation, definition_intent));
                    }
                    None => match batch_operation(operation, self.max_blob_bytes) {
                        Ok(mutation) => {
                            if meter_public && let BatchOperation::Put(put) = &mutation {
                                accounting_inbound.push((
                                    tenant_id,
                                    bucket_id,
                                    put.key.path().to_owned(),
                                    put.bytes.len() as u64,
                                ));
                            }
                            local.push((index, mutation, definition_intent));
                        }
                        Err(error) => outcomes.push(BulkOutcome {
                            index: index as u32,
                            outcome: Some(keldra_api::v1::bulk_outcome::Outcome::Failure(
                                api_request_failure(error),
                            )),
                        }),
                    },
                }
            }
            let routing_duration = routing_started.elapsed();
            self.accounting_traffic
                .record_inbound_batch(accounting_inbound.iter().map(
                    |(tenant_id, bucket_id, path, bytes)| {
                        (*tenant_id, *bucket_id, path.as_str(), *bytes)
                    },
                ));
            let local_indices = local.iter().map(|(index, _, _)| *index).collect::<Vec<_>>();
            let local_operations = local
                .into_iter()
                .map(|(_, operation, definition_intent)| (operation, definition_intent))
                .collect();
            if peer_routed && !remote.is_empty() {
                return Err(Status::failed_precondition(
                    "a routed bulk reached a node that is not every item's coordinator",
                ));
            }
            let dispatch_started = Instant::now();
            let bearer_token = bearer.signed_token().to_owned();
            let dispatch_operation_count = local_indices.len()
                + remote
                    .values()
                    .map(|(_, _, items)| items.len())
                    .sum::<usize>();
            let dispatched = run_request_until(
                deadline,
                bulk::execute_coordinator_groups(
                    self.distribution.clone(),
                    self.cluster_peers.clone(),
                    local_indices,
                    local_operations,
                    remote,
                    bearer_token.clone(),
                    object_path_access::is_internal(&path_access),
                    started,
                    route_budget,
                ),
                "bulk write deadline exceeded",
            )
            .await;
            if let Err(error) = &dispatched {
                bulk::record_dispatch_interruption(
                    error,
                    dispatch_operation_count,
                    encoded_bytes,
                    dispatch_started.elapsed(),
                );
            }
            outcomes.extend(dispatched?);
            outcomes.extend(
                bulk_alias::execute(
                    self.clone(),
                    caller.clone(),
                    bearer_token,
                    alias_items,
                    deadline,
                    meter_public,
                )
                .await?,
            );
            bulk::record_phase_metrics(
                validation_duration,
                authorization_duration,
                identity_duration,
                routing_duration,
                dispatch_started.elapsed(),
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
        reject_plugin_token(&request, "WatchPrefix")?;
        let caller = authenticated_caller(&request)?;
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
        distributed_watch_stream::response(self.watch.clone(), distributed_scope, caller, start)
            .await
    }

    async fn batch_get(
        &self,
        request: Request<BatchGetRequest>,
    ) -> Result<Response<BatchGetResponse>, Status> {
        let plugin_scope = plugin_object_scope(&request);
        let deadline = request_deadline(request.metadata(), self.atomic_program_timeout)?;
        let identity = ObjectReadIdentity::from_request(&request)?;
        let path_access = object_path_access::access_for(&request);
        let meter_public = !object_path_access::is_internal(&path_access);
        let objects = request.into_inner().objects;
        if objects.len() > MAX_BATCH_GET_ITEMS {
            return Err(Status::resource_exhausted(format!(
                "batch read contains more than {MAX_BATCH_GET_ITEMS} items"
            )));
        }
        let mut accepted = Vec::with_capacity(objects.len());
        let mut outcomes = Vec::new();
        let mut pending = Vec::with_capacity(objects.len());
        let mut caller = None::<Caller>;
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
            if let Err(error) = object_path_access::require_key(&path_access, &key) {
                outcomes.push(batch_get_authorization_failure(index, &key, &error));
                continue;
            }
            if let Err(error) = require_plugin_key_scope(plugin_scope.as_ref(), &key) {
                outcomes.push(batch_get_authorization_failure(index, &key, &error));
                continue;
            }
            let candidate = identity.caller_for_tenant(key.tenant())?;
            let caller = caller.get_or_insert(candidate);
            match require_caller_tenant(caller, &key) {
                Ok(()) => {
                    let resolution = object_link::resolve_current(self, key.clone()).await?;
                    let canonical = resolution.canonical();
                    if let Err(error) = object_path_access::require_key(&path_access, canonical)
                        .and_then(|()| require_plugin_key_scope(plugin_scope.as_ref(), canonical))
                    {
                        outcomes.push(batch_get_authorization_failure(index, &key, &error));
                        continue;
                    }
                    pending.push((index, key, resolution, request.version.map(VersionId)));
                }
                Err(error) if error.code() == tonic::Code::PermissionDenied => {
                    outcomes.push(batch_get_authorization_failure(index, &key, &error));
                }
                Err(error) => return Err(error),
            }
        }
        let authorization_requests = pending
            .iter()
            .map(|(_, _, resolution, _)| (resolution.canonical().clone(), ObjectPermission::Get))
            .collect::<Vec<_>>();
        let allowed = if authorization_requests.is_empty() {
            Vec::new()
        } else {
            self.authoritative_system
                .allows_objects(
                    caller
                        .as_ref()
                        .ok_or_else(|| Status::internal("batch read caller is missing"))?,
                    &authorization_requests,
                )
                .await?
        };
        let mut link_bindings = Vec::new();
        for ((index, key, resolution, requested_version), allowed) in
            pending.into_iter().zip(allowed)
        {
            if !allowed {
                outcomes.push(batch_get_authorization_failure(
                    index,
                    &key,
                    &Status::permission_denied("object read is not authorized"),
                ));
                continue;
            }
            if requested_version.is_some()
                && !bucket_versioning_enabled(&self.store, resolution.canonical())
                    .map_err(status)?
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
            if matches!(resolution, object_link::ResolvedAddress::Link(_)) {
                link_bindings.push(resolution.clone());
            }
            accepted.push((
                index,
                key,
                resolution.canonical().clone(),
                requested_version,
            ));
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
            for binding in &link_bindings {
                if !object_link::revalidate(self, binding).await? {
                    return Err(Status::aborted(
                        "object-link binding changed during BatchGet",
                    ));
                }
            }
            outcomes.extend(read_outcomes);
            outcomes.sort_unstable_by_key(|outcome| outcome.index);
            if meter_public {
                for outcome in &outcomes {
                    let (Some(address), Some(BatchGetResult::Object(object))) =
                        (&outcome.address, &outcome.outcome)
                    else {
                        continue;
                    };
                    self.record_accounting_traffic(
                        &address.tenant,
                        &address.bucket,
                        &address.path,
                        0,
                        object.bytes.len() as u64,
                    );
                }
            }
            return Ok(Response::new(BatchGetResponse { outcomes }));
        }
    }

    async fn set_bucket_policy(
        &self,
        request: Request<SetBucketPolicyRequest>,
    ) -> Result<Response<BucketPolicy>, Status> {
        reject_plugin_token(&request, "SetBucketPolicy")?;
        let peer_routed = request
            .extensions()
            .get::<routed_writes::RoutedDestination>()
            .is_some();
        let caller = authenticated_caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let remaining = effective_request_timeout(request.metadata(), self.atomic_program_timeout);
        let api_request = request.into_inner();
        let policy = api_request
            .policy
            .clone()
            .ok_or_else(|| Status::invalid_argument("policy is required"))?;
        let key = ObjectKey::new(&api_request.tenant, &api_request.bucket, "_keldra/policy")
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
                        keldra_store::BucketPolicy {
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
        reject_plugin_token(&request, "InvokeProgram")?;
        atomic_program::invoke(self, request).await
    }
}

fn effective_request_timeout(metadata: &MetadataMap, server_maximum: Duration) -> Duration {
    client_grpc_timeout(metadata).map_or(server_maximum, |client| client.min(server_maximum))
}

pub(crate) fn request_deadline(
    metadata: &MetadataMap,
    server_maximum: Duration,
) -> Result<tokio::time::Instant, Status> {
    tokio::time::Instant::now()
        .checked_add(effective_request_timeout(metadata, server_maximum))
        .ok_or_else(|| Status::internal("configured request timeout exceeds clock"))
}

pub(crate) fn deadline_remaining(deadline: tokio::time::Instant) -> Result<Duration, Status> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        Err(Status::deadline_exceeded("request deadline exceeded"))
    } else {
        Ok(remaining)
    }
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
    run_request_until(
        deadline,
        invocation,
        "atomic program execution deadline exceeded",
    )
    .await
}

pub(crate) async fn run_request_until<T, F>(
    deadline: tokio::time::Instant,
    invocation: F,
    timeout_message: &'static str,
) -> Result<T, Status>
where
    F: Future<Output = Result<T, Status>>,
{
    tokio::time::timeout_at(deadline, invocation)
        .await
        .map_err(|_| Status::deadline_exceeded(timeout_message))?
}

impl ObjectServiceImpl {
    async fn authorize_object(
        &self,
        caller: &Caller,
        key: &ObjectKey,
        permission: ObjectPermission,
    ) -> Result<(), Status> {
        require_caller_tenant(caller, key)?;
        if object_path_access::is_plugin_binding(key.path()) {
            return require_authorized(
                self.authoritative_system
                    .allows_bucket_policy(caller, key.tenant(), key.bucket())
                    .await?,
                "plugin binding access requires bucket policy management",
            );
        }
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
            link: metadata.link.as_ref().map(|link| CanonicalLinkBinding {
                path: link.link.path().to_owned(),
                descriptor_version: link.descriptor_version.0,
            }),
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
            link: self
                .link
                .as_ref()
                .map(|binding| {
                    if binding.descriptor_version == 0 {
                        return Err(Status::invalid_argument(
                            "put token link descriptor version is zero",
                        ));
                    }
                    let target = ObjectKey::new(&self.tenant, &self.bucket, &self.path)
                        .map_err(|error| Status::invalid_argument(error.to_string()))?;
                    let link = ObjectKey::new(&self.tenant, &self.bucket, &binding.path)
                        .map_err(|error| Status::invalid_argument(error.to_string()))?;
                    Ok(keldra_store::ResolvedObjectLink {
                        link,
                        descriptor_version: VersionId(binding.descriptor_version),
                        target,
                    })
                })
                .transpose()?,
            content_type: content_type(self.content_type.clone().unwrap_or_default())?,
            command_id: required_command_id(self.command_id.clone())?,
            durability,
            mode,
        })
    }
}

fn batch_operation_permission(operation: &BatchOperation) -> ObjectPermission {
    match operation {
        BatchOperation::Put(_) | BatchOperation::Publish(_) | BatchOperation::Clone(_) => {
            ObjectPermission::Put
        }
        BatchOperation::Delete(_) => ObjectPermission::Delete,
    }
}

fn batch_operation_key(operation: &BatchOperation) -> &ObjectKey {
    match operation {
        BatchOperation::Put(request) => &request.key,
        BatchOperation::Publish(request) => &request.key,
        BatchOperation::Clone(request) => &request.destination,
        BatchOperation::Delete(request) => &request.key,
    }
}

fn bulk_authorization_failure(index: usize, error: &Status) -> BulkOutcome {
    BulkOutcome {
        index: index as u32,
        outcome: Some(keldra_api::v1::bulk_outcome::Outcome::Failure(
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
        AuthzStoreError::ReceiptCapacity | AuthzStoreError::SourceJournalCapacity => {
            Status::resource_exhausted(error.to_string())
        }
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
            Some(keldra_api::v1::bulk_outcome::Outcome::Receipt(receipt)) if receipt.replayed => {
                counts.replayed += 1;
            }
            Some(keldra_api::v1::bulk_outcome::Outcome::Receipt(_)) => counts.successful += 1,
            Some(keldra_api::v1::bulk_outcome::Outcome::Failure(_)) | None => counts.failed += 1,
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
        monotonic_counter.keldra_bulk_operations_total = operation_count,
        monotonic_counter.keldra_bulk_encoded_bytes_total = encoded_bytes,
        monotonic_counter.keldra_bulk_successful_operations_total = counts.successful,
        monotonic_counter.keldra_bulk_failed_operations_total = counts.failed,
        monotonic_counter.keldra_bulk_replayed_operations_total = counts.replayed,
        histogram.keldra_bulk_request_duration_seconds = duration.as_secs_f64(),
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
    status.settled_through.checked_sub(cursor_offset)
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
        link: None,
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
        Some(keldra_api::v1::bulk_operation::Operation::Put(request)) => {
            BatchOperation::Put(bulk_put_request(request, PutMode::Put)?)
        }
        Some(keldra_api::v1::bulk_operation::Operation::PutIfAbsent(request)) => {
            BatchOperation::Put(bulk_put_request(request, PutMode::PutIfAbsent)?)
        }
        Some(keldra_api::v1::bulk_operation::Operation::PutIfVersion(request)) => {
            BatchOperation::Put(bulk_put_if_version_request(request)?)
        }
        Some(keldra_api::v1::bulk_operation::Operation::PutImmutable(request)) => {
            BatchOperation::Put(bulk_put_request(request, PutMode::PutImmutable)?)
        }
        Some(keldra_api::v1::bulk_operation::Operation::Delete(request)) => {
            BatchOperation::Delete(delete_request(request, Precondition::Any)?)
        }
        Some(keldra_api::v1::bulk_operation::Operation::DeleteIfVersion(request)) => {
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
        BatchOperation::Publish(_) | BatchOperation::Clone(_) | BatchOperation::Delete(_) => {}
    }
    Ok(operation)
}

fn validate_command_id(value: &str) -> Result<(), Status> {
    if value.is_empty() || value.len() > 256 || value.contains('\0') {
        Err(Status::invalid_argument(
            "command_id must contain 1 to 256 bytes and no NUL",
        ))
    } else {
        Ok(())
    }
}

fn object_key(address: Option<ObjectAddress>) -> Result<ObjectKey, Status> {
    let address = address.ok_or_else(|| Status::invalid_argument("object address is required"))?;
    ObjectKey::new(address.tenant, address.bucket, address.path)
        .map_err(|error| Status::invalid_argument(error.to_string()))
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

fn status(error: MutationError) -> Status {
    match error {
        MutationError::ProgramConcurrencyViolation => {
            Status::failed_precondition(format!("PROGRAM_CONCURRENCY_VIOLATION: {error}"))
        }
        MutationError::PreconditionFailed { .. }
        | MutationError::AtomicReservationConflict { .. }
        | MutationError::Immutable
        | MutationError::ImmutablePolicyRequired
        | MutationError::ObjectVersioningNotEnabled
        | MutationError::ObjectHasInboundAliases => Status::failed_precondition(error.to_string()),
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
        MutationError::ReceiptTooLarge { .. }
        | MutationError::SourceJournalRecordTooLarge { .. }
        | MutationError::SourceJournalTransitionTooLarge { .. } => {
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

#[cfg(test)]
mod tests;
