//! Public index lifecycle and query RPC implementation.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anvil_api::v1::bulk_operation::Operation as BulkOperationValue;
use anvil_api::v1::bulk_outcome::Outcome as BulkOutcomeValue;
use anvil_api::v1::index_query::Query as IndexQueryValue;
use anvil_api::v1::index_service_server::IndexService as IndexServiceRpc;
use anvil_api::v1::object_chunk::Value as ObjectChunkValue;
use anvil_api::v1::object_head::State as ObjectState;
use anvil_api::v1::object_service_server::ObjectService;
use anvil_api::v1::{
    BatchGetRequest, BulkOperation, BulkPutIfVersionRequest, BulkPutRequest, BulkWriteRequest,
    BulkWriteResponse, CreateIndexRequest, DeleteIfVersionRequest, DeleteIndexRequest,
    DeleteIndexResponse, Durability, GetIndexRequest, GetObjectRequest, IndexDefinition, IndexKind,
    IndexQuery, ListIndexesRequest, ListIndexesResponse, MutationFailure, MutationFailureCode,
    MutationReceipt, ObjectAddress, QueryIndexRequest, QueryIndexResponse, ReadFailureCode,
    RebuildIndexRequest, UpdateIndexRequest,
};
use anvil_store::{
    DefinitionKind, DefinitionMutationIntent, DefinitionOperation, ObjectKey, StorageTenantId,
};
use prost::Message;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use crate::authentication::{AnonymousIndexRequest, Caller};
use crate::authorization::ObjectPermission;
use crate::distributed_list::OriginalBearer;
use crate::logical_name_resolution::LogicalNameResolver;
use crate::object_path_access;
use crate::v05::{ObjectServiceImpl, request_deadline, run_request_until};

use super::boundary::{
    ExecuteIndexQuery, IndexAuthorizationEvidence, IndexDefinitionScan, IndexDefinitionScanPage,
    IndexPageCursor, IndexPageTokenBinding, IndexRequestContext, IndexServiceDependencies,
};
use super::{
    AuthorizedCurrentCandidates, IndexCandidateVisibility, StoredIndexDefinition, definition_path,
    derive_index_id, validate_command_id, validate_create_definition, validate_update_definition,
};

const DEFINITION_CONTENT_TYPE: &str = "application/vnd.anvil.index-definition+json";
const DEFAULT_PAGE_LIMIT: usize = 100;
const MAX_PAGE_LIMIT: usize = 1_000;
const QUERY_HASH_CONTEXT: &[u8] = b"anvil.index/query/v1";
const EXPLICIT_REBUILD_INTERVAL_MILLIS: u64 = 60 * 60 * 1_000;

#[derive(Clone)]
pub(crate) struct IndexServiceImpl {
    objects: ObjectServiceImpl,
    names: LogicalNameResolver,
    dependencies: IndexServiceDependencies,
    timeouts: IndexRequestTimeouts,
}

#[derive(Clone, Copy)]
struct IndexRequestTimeouts {
    ordinary: Duration,
    query: Duration,
}

#[derive(Clone, Copy)]
enum IndexRequestClass {
    Ordinary,
    Query,
}

impl IndexRequestTimeouts {
    fn deadline(
        self,
        metadata: &tonic::metadata::MetadataMap,
        class: IndexRequestClass,
    ) -> Result<tokio::time::Instant, Status> {
        let maximum = match class {
            IndexRequestClass::Ordinary => self.ordinary,
            IndexRequestClass::Query => self.query,
        };
        request_deadline(metadata, maximum)
    }
}

impl IndexServiceImpl {
    pub(crate) fn new(
        objects: ObjectServiceImpl,
        names: LogicalNameResolver,
        dependencies: IndexServiceDependencies,
        request_timeout: Duration,
        query_timeout: Duration,
    ) -> Self {
        Self {
            objects,
            names,
            dependencies,
            timeouts: IndexRequestTimeouts {
                ordinary: request_timeout,
                query: query_timeout,
            },
        }
    }

    async fn load_definition(
        &self,
        context: &IndexRequestContext,
        bucket: &str,
        name: &str,
    ) -> Result<LoadedDefinition, Status> {
        let key = definition_key(context.caller(), bucket, name)?;
        let response = ObjectService::get_object(
            &self.objects,
            forwarded_request(
                context,
                GetObjectRequest {
                    address: Some(api_address(&key)),
                    version: None,
                },
            )?,
        )
        .await?;
        let mut stream = response.into_inner();
        let first = stream
            .next()
            .await
            .transpose()?
            .ok_or_else(|| Status::data_loss("index definition object returned no head"))?;
        let present = match first.value {
            Some(ObjectChunkValue::Head(head)) => match head.state {
                Some(ObjectState::Present(present)) => present,
                Some(ObjectState::Deleted(_)) | Some(ObjectState::NeverExisted(_)) => {
                    return Err(Status::not_found("index definition does not exist"));
                }
                None => return Err(Status::data_loss("index definition head has no state")),
            },
            Some(ObjectChunkValue::Bytes(_)) | None => {
                return Err(Status::data_loss(
                    "index definition object did not begin with a head",
                ));
            }
        };
        if present.version == 0 {
            return Err(Status::data_loss(
                "index definition has a zero object version",
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            match chunk?.value {
                Some(ObjectChunkValue::Bytes(chunk)) => bytes.extend_from_slice(&chunk),
                Some(ObjectChunkValue::Head(_)) | None => {
                    return Err(Status::data_loss(
                        "index definition object stream contains an invalid frame",
                    ));
                }
            }
        }
        if u64::try_from(bytes.len()).ok() != Some(present.content_length) {
            return Err(Status::data_loss(
                "index definition payload length does not match its head",
            ));
        }
        let stored = StoredIndexDefinition::decode(&bytes)?;
        require_definition_identity(&stored, context.caller(), bucket, name)?;
        let api = stored.to_api(present.version)?;
        Ok(LoadedDefinition { key, stored, api })
    }

    async fn write_definition(
        &self,
        context: &IndexRequestContext,
        operation: BulkOperationValue,
        intent: DefinitionMutationIntent,
    ) -> Result<MutationReceipt, Status> {
        let mut request = forwarded_request(
            context,
            BulkWriteRequest {
                operations: vec![BulkOperation {
                    operation: Some(operation),
                }],
            },
        )?;
        object_path_access::mark_index_definition(&mut request, 0, intent);
        let response = ObjectService::bulk_write(&self.objects, request)
            .await?
            .into_inner();
        single_bulk_receipt(response)
    }

    async fn load_listed_definitions(
        &self,
        context: &IndexRequestContext,
        bucket: &str,
        page: IndexDefinitionScanPage,
        after: Option<&str>,
    ) -> Result<Vec<IndexDefinition>, Status> {
        validate_scan_page(&page, after)?;
        if page.definitions.is_empty() {
            return Ok(Vec::new());
        }
        let requests = page
            .definitions
            .iter()
            .map(|definition| {
                let key = definition_key(context.caller(), bucket, &definition.name)?;
                Ok(GetObjectRequest {
                    address: Some(api_address(&key)),
                    version: None,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let outcomes = ObjectService::batch_get(
            &self.objects,
            forwarded_request(context, BatchGetRequest { objects: requests })?,
        )
        .await?
        .into_inner()
        .outcomes;
        if outcomes.len() != page.definitions.len() {
            return Err(Status::data_loss(
                "definition batch read returned an invalid outcome count",
            ));
        }
        let mut visible = Vec::new();
        for (expected_index, (entry, outcome)) in
            page.definitions.into_iter().zip(outcomes).enumerate()
        {
            if outcome.index as usize != expected_index {
                return Err(Status::data_loss(
                    "definition batch read returned an invalid outcome index",
                ));
            }
            let Some(outcome) = outcome.outcome else {
                return Err(Status::data_loss(
                    "definition batch read returned no outcome",
                ));
            };
            let object = match outcome {
                anvil_api::v1::batch_get_outcome::Outcome::Object(object) => object,
                anvil_api::v1::batch_get_outcome::Outcome::Failure(failure)
                    if ReadFailureCode::try_from(failure.code)
                        == Ok(ReadFailureCode::AuthorizationDenied) =>
                {
                    continue;
                }
                anvil_api::v1::batch_get_outcome::Outcome::Failure(failure) => {
                    return Err(batch_read_failure_status(failure.code, failure.message));
                }
            };
            let Some(head) = object.head else {
                return Err(Status::data_loss("definition batch object has no head"));
            };
            let present = match head.state {
                Some(ObjectState::Present(present)) => present,
                Some(ObjectState::Deleted(_)) | Some(ObjectState::NeverExisted(_)) => continue,
                None => return Err(Status::data_loss("definition batch head has no state")),
            };
            if present.version == 0 || present.content_length != object.bytes.len() as u64 {
                return Err(Status::data_loss(
                    "definition batch object has invalid version or length evidence",
                ));
            }
            let stored = StoredIndexDefinition::decode(&object.bytes)?;
            require_definition_identity(&stored, context.caller(), bucket, &entry.name)?;
            visible.push(stored.to_api(present.version)?);
        }
        Ok(visible)
    }
}

#[tonic::async_trait]
impl IndexServiceRpc for IndexServiceImpl {
    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<IndexDefinition>, Status> {
        let deadline = self
            .timeouts
            .deadline(request.metadata(), IndexRequestClass::Ordinary)?;
        run_request_until(
            deadline,
            async {
                let context = request_context(&request, deadline)?;
                let request = request.into_inner();
                validate_create_definition(&request)?;
                let definition_key =
                    definition_key(context.caller(), &request.bucket, &request.name)?;
                authorize_definition_access(
                    self.dependencies.authorization.as_ref(),
                    context.caller(),
                    &definition_key,
                    ObjectPermission::Put,
                    "index definition create is not authorized",
                )
                .await?;
                let tenant = context.caller().storage_tenant().as_str().to_owned();
                let (tenant_id, bucket_id) = self
                    .names
                    .resolve_bucket_ids(&tenant, &request.bucket)
                    .await?;
                let index_id =
                    derive_index_id(tenant_id, bucket_id, &request.name, &request.command_id)?;
                let stored =
                    StoredIndexDefinition::create(tenant.clone(), request.clone(), index_id)?;
                let receipt = self
                    .write_definition(
                        &context,
                        BulkOperationValue::PutIfAbsent(BulkPutRequest {
                            address: Some(ObjectAddress {
                                tenant,
                                bucket: request.bucket,
                                path: definition_path(&request.name)?,
                            }),
                            bytes: stored.encode()?,
                            content_type: DEFINITION_CONTENT_TYPE.into(),
                            command_id: request.command_id,
                            durability: Durability::Local as i32,
                        }),
                        DefinitionMutationIntent::new(DefinitionKind::Index, index_id)
                            .map_err(|error| Status::internal(error.to_string()))?,
                    )
                    .await?;
                stored.to_api(receipt.version).map(Response::new)
            },
            "index request deadline exceeded",
        )
        .await
    }

    async fn update_index(
        &self,
        request: Request<UpdateIndexRequest>,
    ) -> Result<Response<IndexDefinition>, Status> {
        let deadline = self
            .timeouts
            .deadline(request.metadata(), IndexRequestClass::Ordinary)?;
        run_request_until(
            deadline,
            async {
                let context = request_context(&request, deadline)?;
                let request = request.into_inner();
                validate_update_definition(&request)?;
                let definition_key =
                    definition_key(context.caller(), &request.bucket, &request.name)?;
                authorize_definition_access(
                    self.dependencies.authorization.as_ref(),
                    context.caller(),
                    &definition_key,
                    ObjectPermission::Put,
                    "index definition update is not authorized",
                )
                .await?;
                let current = self
                    .load_definition(&context, &request.bucket, &request.name)
                    .await?;
                let updated = current.stored.updated(request.clone())?;
                let receipt = self
                    .write_definition(
                        &context,
                        BulkOperationValue::PutIfVersion(BulkPutIfVersionRequest {
                            address: Some(api_address(&current.key)),
                            bytes: updated.encode()?,
                            content_type: DEFINITION_CONTENT_TYPE.into(),
                            command_id: request.command_id,
                            durability: Durability::Local as i32,
                            expected_version: request.expected_version,
                        }),
                        DefinitionMutationIntent::new(
                            DefinitionKind::Index,
                            current.stored.index_id,
                        )
                        .map_err(|error| Status::data_loss(error.to_string()))?,
                    )
                    .await?;
                updated.to_api(receipt.version).map(Response::new)
            },
            "index request deadline exceeded",
        )
        .await
    }

    async fn get_index(
        &self,
        request: Request<GetIndexRequest>,
    ) -> Result<Response<IndexDefinition>, Status> {
        let deadline = self
            .timeouts
            .deadline(request.metadata(), IndexRequestClass::Ordinary)?;
        run_request_until(
            deadline,
            async {
                let context = request_context(&request, deadline)?;
                let request = request.into_inner();
                let definition_key =
                    definition_key(context.caller(), &request.bucket, &request.name)?;
                authorize_definition_access(
                    self.dependencies.authorization.as_ref(),
                    context.caller(),
                    &definition_key,
                    ObjectPermission::Get,
                    "index definition read is not authorized",
                )
                .await?;
                self.load_definition(&context, &request.bucket, &request.name)
                    .await
                    .map(|loaded| Response::new(loaded.api))
            },
            "index request deadline exceeded",
        )
        .await
    }

    async fn list_indexes(
        &self,
        request: Request<ListIndexesRequest>,
    ) -> Result<Response<ListIndexesResponse>, Status> {
        let deadline = self
            .timeouts
            .deadline(request.metadata(), IndexRequestClass::Ordinary)?;
        run_request_until(
            deadline,
            async {
                let context = request_context(&request, deadline)?;
                let request = request.into_inner();
                if let Some(after) = request.start_after_name.as_deref() {
                    definition_path(after)?;
                }
                let public_limit = page_limit(request.limit)?;
                let tenant = context.caller().storage_tenant().as_str().to_owned();
                let (tenant_id, bucket_id) = self
                    .names
                    .resolve_bucket_ids(&tenant, &request.bucket)
                    .await?;
                let mut after = request.start_after_name;
                let mut visible = Vec::with_capacity(public_limit + 1);
                while visible.len() <= public_limit {
                    let page = self
                        .dependencies
                        .definitions
                        .scan(IndexDefinitionScan {
                            bearer: context.bearer(),
                            tenant: tenant.clone(),
                            bucket: request.bucket.clone(),
                            tenant_id,
                            bucket_id,
                            start_after_name: after.clone(),
                            limit: MAX_PAGE_LIMIT,
                        })
                        .await?;
                    if page.definitions.is_empty() {
                        if page.has_more {
                            return Err(Status::data_loss(
                                "index definition scan did not advance its cursor",
                            ));
                        }
                        break;
                    }
                    let next_after = page
                        .definitions
                        .last()
                        .map(|definition| definition.name.clone())
                        .ok_or_else(|| Status::data_loss("index definition scan page is empty"))?;
                    let has_more = page.has_more;
                    visible.extend(
                        self.load_listed_definitions(
                            &context,
                            &request.bucket,
                            page,
                            after.as_deref(),
                        )
                        .await?,
                    );
                    if visible.len() > public_limit || !has_more {
                        break;
                    }
                    after = Some(next_after);
                }
                let has_more = visible.len() > public_limit;
                visible.truncate(public_limit);
                Ok(Response::new(ListIndexesResponse {
                    indexes: visible,
                    has_more,
                }))
            },
            "index request deadline exceeded",
        )
        .await
    }

    async fn rebuild_index(
        &self,
        request: Request<RebuildIndexRequest>,
    ) -> Result<Response<IndexDefinition>, Status> {
        let deadline = self
            .timeouts
            .deadline(request.metadata(), IndexRequestClass::Ordinary)?;
        run_request_until(
            deadline,
            async {
                let context = request_context(&request, deadline)?;
                let request = request.into_inner();
                if request.expected_version == 0 {
                    return Err(Status::invalid_argument(
                        "expected index definition version must be non-zero",
                    ));
                }
                validate_command_id(&request.command_id)?;
                let definition_key =
                    definition_key(context.caller(), &request.bucket, &request.name)?;
                authorize_rebuild_access(
                    self.dependencies.authorization.as_ref(),
                    context.caller(),
                    &definition_key,
                )
                .await?;
                let current = self
                    .load_definition(&context, &request.bucket, &request.name)
                    .await?;
                if current.api.version != request.expected_version {
                    let receipt = self
                        .write_definition(
                            &context,
                            BulkOperationValue::PutIfVersion(BulkPutIfVersionRequest {
                                address: Some(api_address(&current.key)),
                                bytes: current.stored.encode()?,
                                content_type: DEFINITION_CONTENT_TYPE.into(),
                                command_id: request.command_id,
                                durability: Durability::Local as i32,
                                expected_version: request.expected_version,
                            }),
                            DefinitionMutationIntent::new(
                                DefinitionKind::Index,
                                current.stored.index_id,
                            )
                            .map_err(|error| Status::data_loss(error.to_string()))?,
                        )
                        .await?;
                    return current.stored.to_api(receipt.version).map(Response::new);
                }
                let accepted_at_unix_millis = current_unix_millis()?;
                enforce_explicit_rebuild_interval(
                    current.stored.last_explicit_rebuild_at_unix_millis(),
                    accepted_at_unix_millis,
                )?;
                let rebuilt = current
                    .stored
                    .with_explicit_rebuild(accepted_at_unix_millis)?;
                let receipt = self
                    .write_definition(
                        &context,
                        BulkOperationValue::PutIfVersion(BulkPutIfVersionRequest {
                            address: Some(api_address(&current.key)),
                            bytes: rebuilt.encode()?,
                            content_type: DEFINITION_CONTENT_TYPE.into(),
                            command_id: request.command_id,
                            durability: Durability::Local as i32,
                            expected_version: request.expected_version,
                        }),
                        DefinitionMutationIntent::new(
                            DefinitionKind::Index,
                            current.stored.index_id,
                        )
                        .map_err(|error| Status::data_loss(error.to_string()))?,
                    )
                    .await?;
                rebuilt.to_api(receipt.version).map(Response::new)
            },
            "index request deadline exceeded",
        )
        .await
    }

    async fn delete_index(
        &self,
        request: Request<DeleteIndexRequest>,
    ) -> Result<Response<DeleteIndexResponse>, Status> {
        let deadline = self
            .timeouts
            .deadline(request.metadata(), IndexRequestClass::Ordinary)?;
        run_request_until(
            deadline,
            async {
                let context = request_context(&request, deadline)?;
                let request = request.into_inner();
                if request.expected_version == 0 {
                    return Err(Status::invalid_argument(
                        "expected index definition version must be non-zero",
                    ));
                }
                validate_command_id(&request.command_id)?;
                let key = definition_key(context.caller(), &request.bucket, &request.name)?;
                let tenant = context.caller().storage_tenant().as_str();
                let (tenant_id, bucket_id) = self
                    .names
                    .resolve_bucket_ids(tenant, &request.bucket)
                    .await?;
                let snapshot = authorized_definition_snapshot(
                    self.dependencies.authorization.as_ref(),
                    self.dependencies.definition_reader.as_ref(),
                    context.caller(),
                    &key,
                    tenant_id,
                    bucket_id,
                )
                .await?
                .ok_or_else(|| Status::not_found("index definition does not exist"))?;
                if snapshot.head.deleted {
                    return Err(Status::not_found("index definition does not exist"));
                }
                let locator = snapshot
                    .definition_locator
                    .ok_or_else(|| Status::data_loss("index definition has no typed locator"))?;
                if locator.kind != DefinitionKind::Index
                    || locator.operation != DefinitionOperation::Upsert
                    || locator.tenant_id != tenant_id
                    || locator.bucket_id != bucket_id
                    || locator.path != key.path()
                    || locator.object_version != snapshot.head.version
                {
                    return Err(Status::data_loss(
                        "index definition locator disagrees with its object head",
                    ));
                }
                let receipt = self
                    .write_definition(
                        &context,
                        BulkOperationValue::DeleteIfVersion(DeleteIfVersionRequest {
                            address: Some(api_address(&key)),
                            command_id: request.command_id,
                            durability: Durability::Local as i32,
                            expected_version: request.expected_version,
                        }),
                        DefinitionMutationIntent::new(DefinitionKind::Index, locator.definition_id)
                            .map_err(|error| Status::data_loss(error.to_string()))?,
                    )
                    .await?;
                if !receipt.deleted {
                    return Err(Status::data_loss(
                        "index definition delete returned a non-delete receipt",
                    ));
                }
                Ok(Response::new(DeleteIndexResponse { deleted: true }))
            },
            "index request deadline exceeded",
        )
        .await
    }

    async fn query_index(
        &self,
        request: Request<QueryIndexRequest>,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        let started = std::time::Instant::now();
        let deadline = self
            .timeouts
            .deadline(request.metadata(), IndexRequestClass::Query)?;
        let result = run_request_until(
            deadline,
            async {
                let context =
                    query_request_context(&request, request.get_ref().tenant.as_str(), deadline)?;
                let request = request.into_inner();
                let query = request
                    .query
                    .ok_or_else(|| Status::invalid_argument("index query is required"))?;
                let limit = page_limit(request.limit)?;
                let definition_key =
                    definition_key(context.caller(), &request.bucket, &request.index_name)?;
                let admission = authorize_definition_access_with_evidence(
                    self.dependencies.authorization.as_ref(),
                    context.caller(),
                    &definition_key,
                    ObjectPermission::Get,
                    "index definition query is not authorized",
                )
                .await?;
                let loaded = self
                    .load_definition(&context, &request.bucket, &request.index_name)
                    .await?;
                validate_query_kind(&loaded.api, &query)?;
                let tenant = context.caller().storage_tenant().as_str();
                let (tenant_id, bucket_id) = self
                    .names
                    .resolve_bucket_ids(tenant, &request.bucket)
                    .await?;
                let binding = IndexPageTokenBinding {
                    tenant_id,
                    bucket_id,
                    index_id: loaded.api.index_id,
                    definition_version: loaded.api.version,
                    query_hash: canonical_query_hash(&query),
                };
                let resume = if request.page_token.is_empty() {
                    None
                } else {
                    let cursor = self.dependencies.page_tokens.decode(
                        context.caller(),
                        &request.page_token,
                        binding,
                    )?;
                    validate_page_cursor(&cursor)?;
                    Some(cursor)
                };
                if resume
                    .as_ref()
                    .is_some_and(|cursor| cursor.authorization_revision != admission.revision)
                {
                    return Err(Status::failed_precondition(
                        "page token authorization revision is no longer current",
                    ));
                }
                let kind = IndexKind::try_from(loaded.api.kind)
                    .map_err(|_| Status::data_loss("index definition has an unknown kind"))?;
                let candidate_visibility: Arc<dyn IndexCandidateVisibility> =
                    Arc::new(AuthorizedCurrentCandidates::new(
                        context.caller().clone(),
                        admission.revision,
                        loaded.stored.bucket.clone(),
                        loaded.stored.path_prefix.clone(),
                        kind,
                        tenant_id,
                        bucket_id,
                        deadline,
                        self.dependencies.authorization.clone(),
                        self.dependencies.live_versions.clone(),
                    ));
                let executed = self
                    .dependencies
                    .queries
                    .execute(ExecuteIndexQuery {
                        context: context.clone(),
                        tenant_id,
                        bucket_id,
                        definition: loaded.api.clone(),
                        query,
                        limit,
                        candidate_visibility,
                        authorization_revision: admission.revision,
                        resume: resume.clone(),
                    })
                    .await?;
                validate_execution(&executed, resume.as_ref(), limit)?;
                let authorization_revision = executed.freshness.authorization_revision;
                let next_page_token = match executed.next_position {
                    Some(last_position) => self.dependencies.page_tokens.encode(
                        context.caller(),
                        binding,
                        &IndexPageCursor {
                            generation: executed.freshness.generation,
                            last_position,
                            authorization_revision,
                        },
                    )?,
                    None => Vec::new(),
                };
                Ok(Response::new(QueryIndexResponse {
                    hits: executed.hits,
                    next_page_token,
                    freshness: Some(executed.freshness),
                }))
            },
            "index request deadline exceeded",
        )
        .await;
        let (outcome, status_code, failed, deadline_exceeded) = match &result {
            Ok(_) => ("completed", "ok", false, false),
            Err(status) if status.code() == tonic::Code::DeadlineExceeded => {
                ("deadline_exceeded", "deadline_exceeded", true, true)
            }
            Err(status) => ("failed", grpc_status_code_name(status.code()), true, false),
        };
        tracing::info!(
            operation = "query_index",
            query.outcome = outcome,
            grpc_status_code = status_code,
            monotonic_counter.anvil_index_query_requests_total = 1_u64,
            monotonic_counter.anvil_index_query_request_failures_total = u64::from(failed),
            monotonic_counter.anvil_index_query_deadlines_exceeded_total =
                u64::from(deadline_exceeded),
            histogram.anvil_index_query_request_duration_seconds = started.elapsed().as_secs_f64(),
            "public index query reached a terminal outcome"
        );
        result
    }
}

fn grpc_status_code_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "ok",
        tonic::Code::Cancelled => "cancelled",
        tonic::Code::Unknown => "unknown",
        tonic::Code::InvalidArgument => "invalid_argument",
        tonic::Code::DeadlineExceeded => "deadline_exceeded",
        tonic::Code::NotFound => "not_found",
        tonic::Code::AlreadyExists => "already_exists",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::ResourceExhausted => "resource_exhausted",
        tonic::Code::FailedPrecondition => "failed_precondition",
        tonic::Code::Aborted => "aborted",
        tonic::Code::OutOfRange => "out_of_range",
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Internal => "internal",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::DataLoss => "data_loss",
        tonic::Code::Unauthenticated => "unauthenticated",
    }
}

fn current_unix_millis() -> Result<u64, Status> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::internal("system clock is before the Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| Status::internal("system clock exceeds index timestamp range"))
}

fn enforce_explicit_rebuild_interval(
    previous_accepted_at_unix_millis: Option<u64>,
    now_unix_millis: u64,
) -> Result<(), Status> {
    let Some(previous) = previous_accepted_at_unix_millis else {
        return Ok(());
    };
    let retry_at = previous
        .checked_add(EXPLICIT_REBUILD_INTERVAL_MILLIS)
        .ok_or_else(|| Status::data_loss("stored explicit rebuild timestamp overflows"))?;
    if now_unix_millis < retry_at {
        Err(Status::resource_exhausted(format!(
            "index rebuild is rate limited; retry after Unix millisecond {retry_at}"
        )))
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct LoadedDefinition {
    key: ObjectKey,
    stored: StoredIndexDefinition,
    api: IndexDefinition,
}

fn request_context<T>(
    request: &Request<T>,
    deadline: tokio::time::Instant,
) -> Result<IndexRequestContext, Status> {
    let caller = request
        .extensions()
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("authenticated caller identity is missing"))?;
    let bearer = OriginalBearer::from_metadata(request.metadata())?;
    Ok(IndexRequestContext::new(
        caller,
        bearer,
        request.metadata().clone(),
        deadline,
    ))
}

fn query_request_context<T>(
    request: &Request<T>,
    requested_tenant: &str,
    deadline: tokio::time::Instant,
) -> Result<IndexRequestContext, Status> {
    let (caller, bearer) = match (
        request.extensions().get::<Caller>(),
        request.extensions().get::<AnonymousIndexRequest>(),
    ) {
        (Some(caller), None) => {
            if !requested_tenant.is_empty() && requested_tenant != caller.storage_tenant().as_str()
            {
                return Err(Status::permission_denied(
                    "index query does not belong to the authenticated tenant",
                ));
            }
            (
                caller.clone(),
                OriginalBearer::from_metadata(request.metadata())?,
            )
        }
        (None, Some(_)) => {
            if request.metadata().get("authorization").is_some() {
                return Err(Status::unauthenticated(
                    "anonymous index request unexpectedly supplied authorization",
                ));
            }
            if requested_tenant.is_empty() {
                return Err(Status::invalid_argument(
                    "tenant is required for an anonymous index query",
                ));
            }
            let tenant = StorageTenantId::parse(requested_tenant)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            (Caller::from_anonymous(tenant), OriginalBearer::anonymous())
        }
        (Some(_), Some(_)) => {
            return Err(Status::internal(
                "index request contains contradictory caller identities",
            ));
        }
        (None, None) => {
            return Err(Status::unauthenticated("request identity is missing"));
        }
    };
    Ok(IndexRequestContext::new(
        caller,
        bearer,
        request.metadata().clone(),
        deadline,
    ))
}

fn forwarded_request<T>(context: &IndexRequestContext, value: T) -> Result<Request<T>, Status> {
    let mut request = Request::new(value);
    *request.metadata_mut() = context.metadata().clone();
    request.set_timeout(context.remaining()?);
    request.extensions_mut().insert(context.caller().clone());
    object_path_access::mark_index(&mut request);
    Ok(request)
}

fn definition_key(caller: &Caller, bucket: &str, name: &str) -> Result<ObjectKey, Status> {
    ObjectKey::new(
        caller.storage_tenant().as_str(),
        bucket,
        definition_path(name)?,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn api_address(key: &ObjectKey) -> ObjectAddress {
    ObjectAddress {
        tenant: key.tenant().into(),
        bucket: key.bucket().into(),
        path: key.path().into(),
    }
}

fn require_definition_identity(
    stored: &StoredIndexDefinition,
    caller: &Caller,
    bucket: &str,
    name: &str,
) -> Result<(), Status> {
    if stored.tenant != caller.storage_tenant().as_str()
        || stored.bucket != bucket
        || stored.name != name
    {
        Err(Status::data_loss(
            "index definition payload does not match its object address",
        ))
    } else {
        Ok(())
    }
}

fn single_bulk_receipt(response: BulkWriteResponse) -> Result<MutationReceipt, Status> {
    let [outcome] = response.outcomes.as_slice() else {
        return Err(Status::data_loss(
            "index definition write returned an invalid outcome count",
        ));
    };
    if outcome.index != 0 {
        return Err(Status::data_loss(
            "index definition write returned an invalid outcome index",
        ));
    }
    match outcome.outcome.clone() {
        Some(BulkOutcomeValue::Receipt(receipt)) if receipt.version != 0 => Ok(receipt),
        Some(BulkOutcomeValue::Receipt(_)) => Err(Status::data_loss(
            "index definition write returned a zero version",
        )),
        Some(BulkOutcomeValue::Failure(failure)) => Err(bulk_failure_status(failure)),
        None => Err(Status::data_loss(
            "index definition write returned no outcome",
        )),
    }
}

fn bulk_failure_status(failure: MutationFailure) -> Status {
    let message = failure.message;
    match MutationFailureCode::try_from(failure.code) {
        Ok(MutationFailureCode::ConditionFailed)
        | Ok(MutationFailureCode::Immutable)
        | Ok(MutationFailureCode::ImmutablePolicyRequired)
        | Ok(MutationFailureCode::ProgramConcurrencyViolation) => {
            Status::failed_precondition(message)
        }
        Ok(MutationFailureCode::IdempotencyInputMismatch) => Status::already_exists(message),
        Ok(MutationFailureCode::Invalid) | Ok(MutationFailureCode::Unspecified) => {
            Status::invalid_argument(message)
        }
        Ok(MutationFailureCode::DurabilityUnavailable) => Status::unavailable(message),
        Ok(MutationFailureCode::ResourceLimit) => Status::resource_exhausted(message),
        Ok(MutationFailureCode::AuthorizationDenied) => Status::permission_denied(message),
        Err(_) | Ok(MutationFailureCode::Internal) => Status::internal(message),
    }
}

fn batch_read_failure_status(code: i32, message: String) -> Status {
    match ReadFailureCode::try_from(code) {
        Ok(ReadFailureCode::Invalid) => Status::invalid_argument(message),
        Ok(ReadFailureCode::AuthorizationDenied) => Status::permission_denied(message),
        Ok(ReadFailureCode::VersionNotFound) => Status::not_found(message),
        Ok(ReadFailureCode::ResourceLimit) => Status::resource_exhausted(message),
        Ok(ReadFailureCode::DataLoss) => Status::data_loss(message),
        Ok(ReadFailureCode::VersioningDisabled) => Status::failed_precondition(message),
        Ok(ReadFailureCode::Internal) | Ok(ReadFailureCode::Unspecified) | Err(_) => {
            Status::internal(message)
        }
    }
}

fn page_limit(value: u32) -> Result<usize, Status> {
    match value {
        0 => Ok(DEFAULT_PAGE_LIMIT),
        1..=1_000 => Ok(value as usize),
        _ => Err(Status::invalid_argument(format!(
            "index page limit exceeds {MAX_PAGE_LIMIT}"
        ))),
    }
}

fn validate_scan_page(page: &IndexDefinitionScanPage, after: Option<&str>) -> Result<(), Status> {
    if page.definitions.len() > MAX_PAGE_LIMIT {
        return Err(Status::data_loss(
            "index definition scan exceeded its requested limit",
        ));
    }
    let mut previous = after;
    for definition in &page.definitions {
        definition_path(&definition.name)
            .map_err(|_| Status::data_loss("index definition scan returned an invalid name"))?;
        if previous.is_some_and(|previous| definition.name.as_str() <= previous) {
            return Err(Status::data_loss(
                "index definition scan is not strictly ordered",
            ));
        }
        previous = Some(definition.name.as_str());
    }
    Ok(())
}

fn validate_authorization_evidence(
    evidence: &IndexAuthorizationEvidence,
    expected: usize,
) -> Result<(), Status> {
    if evidence.revision == 0 || evidence.allowed.len() != expected {
        Err(Status::data_loss(
            "Zanzibar returned invalid index authorization evidence",
        ))
    } else {
        Ok(())
    }
}

fn canonical_query_hash(query: &IndexQuery) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(QUERY_HASH_CONTEXT);
    hasher.update(&query.encode_to_vec());
    *hasher.finalize().as_bytes()
}

fn validate_page_cursor(cursor: &IndexPageCursor) -> Result<(), Status> {
    if cursor.generation == 0
        || cursor.last_position.is_empty()
        || cursor.authorization_revision == 0
    {
        Err(Status::invalid_argument(
            "index page token contains an invalid continuation",
        ))
    } else {
        Ok(())
    }
}

fn validate_query_kind(definition: &IndexDefinition, query: &IndexQuery) -> Result<(), Status> {
    let kind = IndexKind::try_from(definition.kind)
        .map_err(|_| Status::data_loss("index definition has an unknown kind"))?;
    let matches = matches!(
        (kind, query.query.as_ref()),
        (IndexKind::Path, Some(IndexQueryValue::Path(_)))
            | (
                IndexKind::MetadataFilter,
                Some(IndexQueryValue::MetadataFilter(_))
            )
            | (IndexKind::TypedJson, Some(IndexQueryValue::TypedJson(_)))
            | (IndexKind::FullText, Some(IndexQueryValue::FullText(_)))
            | (IndexKind::Vector, Some(IndexQueryValue::Vector(_)))
            | (IndexKind::Hybrid, Some(IndexQueryValue::Hybrid(_)))
            | (IndexKind::GitSource, Some(IndexQueryValue::GitSource(_)))
            | (IndexKind::Tensor, Some(IndexQueryValue::Tensor(_)))
    );
    if matches {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "query type does not match the index definition",
        ))
    }
}

fn validate_execution(
    execution: &super::boundary::ExecutedIndexQuery,
    resume: Option<&IndexPageCursor>,
    limit: usize,
) -> Result<(), Status> {
    if execution.hits.len() > limit {
        return Err(Status::data_loss(
            "index executor returned more hits than requested",
        ));
    }
    if execution.next_position.as_ref().is_some_and(Vec::is_empty) {
        return Err(Status::data_loss(
            "index executor returned an empty continuation position",
        ));
    }
    if execution.next_position.is_some() && execution.freshness.generation == 0 {
        return Err(Status::data_loss(
            "index executor returned a continuation without a generation",
        ));
    }
    if execution.freshness.authorization_revision == 0 {
        return Err(Status::data_loss(
            "index executor returned no Zanzibar authorization revision",
        ));
    }
    if let Some(resume) = resume {
        if execution.freshness.generation != resume.generation {
            return Err(Status::failed_precondition(
                "requested index generation is no longer available",
            ));
        }
        if execution.freshness.authorization_revision != resume.authorization_revision {
            return Err(Status::failed_precondition(
                "authorization revision changed during index execution",
            ));
        }
    }
    Ok(())
}

async fn authorized_definition_snapshot(
    authorization: &dyn super::boundary::IndexAuthorization,
    reader: &dyn super::boundary::IndexDefinitionReader,
    caller: &Caller,
    key: &ObjectKey,
    tenant_id: u64,
    bucket_id: u64,
) -> Result<Option<anvil_store::ObjectPathSnapshot>, Status> {
    authorize_definition_access(
        authorization,
        caller,
        key,
        ObjectPermission::Delete,
        "index definition delete is not authorized",
    )
    .await?;
    reader.current_snapshot(key, tenant_id, bucket_id).await
}

async fn authorize_definition_access(
    authorization: &dyn super::boundary::IndexAuthorization,
    caller: &Caller,
    key: &ObjectKey,
    permission: ObjectPermission,
    denied_message: &'static str,
) -> Result<(), Status> {
    authorize_definition_access_with_evidence(
        authorization,
        caller,
        key,
        permission,
        denied_message,
    )
    .await
    .map(|_| ())
}

async fn authorize_definition_access_with_evidence(
    authorization: &dyn super::boundary::IndexAuthorization,
    caller: &Caller,
    key: &ObjectKey,
    permission: ObjectPermission,
    denied_message: &'static str,
) -> Result<IndexAuthorizationEvidence, Status> {
    let evidence = authorization
        .allows_objects_with_evidence(caller, &[(key.clone(), permission)])
        .await?;
    validate_authorization_evidence(&evidence, 1)?;
    if evidence.allowed[0] {
        Ok(evidence)
    } else {
        Err(Status::permission_denied(denied_message))
    }
}

async fn authorize_rebuild_access(
    authorization: &dyn super::boundary::IndexAuthorization,
    caller: &Caller,
    key: &ObjectKey,
) -> Result<(), Status> {
    authorize_definition_access(
        authorization,
        caller,
        key,
        ObjectPermission::Put,
        "index rebuild is not authorized",
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anvil_api::v1::{
        IndexFreshness, IndexSourceFreshness, IndexSpecification, PathIndexQuery, PathIndexSpec,
        TensorIndexQuery, TensorIndexSpec, index_query, index_specification,
    };
    use anvil_store::StorageTenantId;
    use tonic::metadata::MetadataValue;

    use super::*;

    fn caller() -> Caller {
        Caller::from_authenticated_application(
            StorageTenantId::parse("tenant").unwrap(),
            "application",
        )
        .unwrap()
    }

    fn path_query(prefix: &str) -> IndexQuery {
        IndexQuery {
            query: Some(index_query::Query::Path(PathIndexQuery {
                prefix: prefix.into(),
                start_after: None,
            })),
        }
    }

    struct FakeAuthorization {
        allowed: Vec<bool>,
        revision: u64,
        seen: Mutex<Vec<(ObjectKey, ObjectPermission)>>,
    }

    struct CountingDefinitionReader {
        reads: AtomicUsize,
    }

    #[tonic::async_trait]
    impl super::super::boundary::IndexDefinitionReader for CountingDefinitionReader {
        async fn current_snapshot(
            &self,
            _key: &ObjectKey,
            _tenant_id: u64,
            _bucket_id: u64,
        ) -> Result<Option<anvil_store::ObjectPathSnapshot>, Status> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
    }

    #[tonic::async_trait]
    impl super::super::boundary::IndexAuthorization for FakeAuthorization {
        async fn allows_objects_with_evidence(
            &self,
            _caller: &Caller,
            requests: &[(ObjectKey, ObjectPermission)],
        ) -> Result<IndexAuthorizationEvidence, Status> {
            *self.seen.lock().unwrap() = requests.to_vec();
            Ok(IndexAuthorizationEvidence {
                allowed: self.allowed.clone(),
                revision: self.revision,
            })
        }
    }

    #[tokio::test]
    async fn denied_definition_delete_does_not_read_internal_definition_state() {
        let authorization = FakeAuthorization {
            allowed: vec![false],
            revision: 3,
            seen: Mutex::new(Vec::new()),
        };
        let reader = CountingDefinitionReader {
            reads: AtomicUsize::new(0),
        };
        let key = ObjectKey::new("tenant", "objects", definition_path("by-path").unwrap()).unwrap();
        let error =
            authorized_definition_snapshot(&authorization, &reader, &caller(), &key, 11, 12)
                .await
                .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert_eq!(reader.reads.load(Ordering::Relaxed), 0);
        assert_eq!(
            authorization.seen.lock().unwrap().as_slice(),
            &[(key, ObjectPermission::Delete)]
        );
    }

    #[tokio::test]
    async fn rebuild_authorization_checks_definition_put_permission() {
        let authorization = FakeAuthorization {
            allowed: vec![false],
            revision: 3,
            seen: Mutex::new(Vec::new()),
        };
        let key = ObjectKey::new("tenant", "objects", definition_path("by-path").unwrap()).unwrap();
        let error = authorize_rebuild_access(&authorization, &caller(), &key)
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert_eq!(
            authorization.seen.lock().unwrap().as_slice(),
            &[(key, ObjectPermission::Put)]
        );
    }

    #[test]
    fn explicit_rebuild_interval_is_one_full_hour() {
        assert!(enforce_explicit_rebuild_interval(None, 1).is_ok());
        let previous = 10_000;
        let before_boundary = enforce_explicit_rebuild_interval(
            Some(previous),
            previous + EXPLICIT_REBUILD_INTERVAL_MILLIS - 1,
        )
        .unwrap_err();
        assert_eq!(before_boundary.code(), tonic::Code::ResourceExhausted);
        assert!(
            before_boundary
                .message()
                .contains("retry after Unix millisecond 3610000")
        );
        assert!(
            enforce_explicit_rebuild_interval(
                Some(previous),
                previous + EXPLICIT_REBUILD_INTERVAL_MILLIS,
            )
            .is_ok()
        );
    }

    #[test]
    fn index_query_has_a_separate_maximum_but_client_deadlines_still_win() {
        let timeouts = IndexRequestTimeouts {
            ordinary: Duration::from_secs(30),
            query: Duration::from_secs(300),
        };
        let metadata = tonic::metadata::MetadataMap::new();
        let ordinary_started = tokio::time::Instant::now();
        let ordinary = timeouts
            .deadline(&metadata, IndexRequestClass::Ordinary)
            .unwrap();
        let ordinary_finished = tokio::time::Instant::now();
        let query_started = tokio::time::Instant::now();
        let query = timeouts
            .deadline(&metadata, IndexRequestClass::Query)
            .unwrap();
        let query_finished = tokio::time::Instant::now();
        assert!(ordinary >= ordinary_started + Duration::from_secs(30));
        assert!(ordinary <= ordinary_finished + Duration::from_secs(30));
        assert!(query >= query_started + Duration::from_secs(300));
        assert!(query <= query_finished + Duration::from_secs(300));

        let mut client_limited = tonic::metadata::MetadataMap::new();
        client_limited.insert("grpc-timeout", MetadataValue::from_static("2S"));
        for class in [IndexRequestClass::Ordinary, IndexRequestClass::Query] {
            let started = tokio::time::Instant::now();
            let deadline = timeouts.deadline(&client_limited, class).unwrap();
            let finished = tokio::time::Instant::now();
            assert!(deadline >= started + Duration::from_secs(2));
            assert!(deadline <= finished + Duration::from_secs(2));
        }
    }

    #[test]
    fn forwarded_requests_keep_the_original_bearer_and_caller() {
        let caller = caller();
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer signed-token").unwrap(),
        );
        request.extensions_mut().insert(caller.clone());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let context = request_context(&request, deadline).unwrap();
        let forwarded = forwarded_request(&context, 7_u64).unwrap();

        assert_eq!(
            forwarded.metadata().get("authorization"),
            request.metadata().get("authorization")
        );
        assert_eq!(forwarded.extensions().get::<Caller>(), Some(&caller));
        assert_eq!(context.routed_bearer(), "signed-token");
    }

    #[test]
    fn authenticated_query_tenant_is_optional_but_cannot_cross_tenants() {
        let caller = caller();
        let mut request = Request::new(());
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer signed-token").unwrap(),
        );
        request.extensions_mut().insert(caller.clone());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);

        let omitted = query_request_context(&request, "", deadline).unwrap();
        assert_eq!(omitted.caller(), &caller);
        let explicit = query_request_context(&request, "tenant", deadline).unwrap();
        assert_eq!(explicit.caller(), &caller);
        assert_eq!(
            query_request_context(&request, "another", deadline)
                .err()
                .unwrap()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn anonymous_query_requires_and_binds_an_explicit_tenant() {
        let mut request = Request::new(());
        request.extensions_mut().insert(AnonymousIndexRequest);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);

        let context = query_request_context(&request, "tenant", deadline).unwrap();
        assert_eq!(context.caller().storage_tenant().as_str(), "tenant");
        assert_eq!(
            context.caller().subject(),
            &anvil_authz::ObjectRef::anonymous()
        );
        assert_eq!(context.routed_bearer(), anvil_authz::ANONYMOUS_SUBJECT_ID);
        assert_eq!(
            query_request_context(&request, "", deadline)
                .err()
                .unwrap()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            request_context(&request, deadline).err().unwrap().code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn canonical_query_hash_changes_with_query_semantics() {
        assert_eq!(
            canonical_query_hash(&path_query("a/")),
            canonical_query_hash(&path_query("a/"))
        );
        assert_ne!(
            canonical_query_hash(&path_query("a/")),
            canonical_query_hash(&path_query("b/"))
        );
    }

    #[test]
    fn continuations_require_generation_position_and_authorization_revision() {
        assert!(
            validate_page_cursor(&IndexPageCursor {
                generation: 4,
                last_position: b"after".to_vec(),
                authorization_revision: 8,
            })
            .is_ok()
        );
        for cursor in [
            IndexPageCursor {
                generation: 0,
                last_position: b"after".to_vec(),
                authorization_revision: 8,
            },
            IndexPageCursor {
                generation: 4,
                last_position: Vec::new(),
                authorization_revision: 8,
            },
            IndexPageCursor {
                generation: 4,
                last_position: b"after".to_vec(),
                authorization_revision: 0,
            },
        ] {
            assert!(validate_page_cursor(&cursor).is_err());
        }
    }

    #[test]
    fn lag_is_returned_as_freshness_evidence_never_an_execution_error() {
        let execution = super::super::boundary::ExecutedIndexQuery {
            hits: Vec::new(),
            freshness: IndexFreshness {
                generation: 7,
                sources: vec![IndexSourceFreshness {
                    node_id: 2,
                    source_epoch: vec![3; 16],
                    indexed_next_offset: 10,
                    observed_tail: Some(100),
                    lag_hint: 90,
                }],
                initial_build_complete: false,
                rebuilding: true,
                authorization_revision: 9,
                ..Default::default()
            },
            next_position: None,
        };

        assert!(validate_execution(&execution, None, 100).is_ok());
    }

    #[test]
    fn zero_hit_execution_still_requires_and_preserves_the_zanzibar_revision() {
        let resume = IndexPageCursor {
            generation: 7,
            last_position: b"after".to_vec(),
            authorization_revision: 9,
        };
        let mut execution = super::super::boundary::ExecutedIndexQuery {
            hits: Vec::new(),
            freshness: IndexFreshness {
                generation: 7,
                authorization_revision: 9,
                ..Default::default()
            },
            next_position: None,
        };
        validate_execution(&execution, Some(&resume), 100).unwrap();

        execution.freshness.authorization_revision = 0;
        assert_eq!(
            validate_execution(&execution, None, 100)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
        execution.freshness.authorization_revision = 10;
        assert_eq!(
            validate_execution(&execution, Some(&resume), 100)
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[test]
    fn tensor_query_kind_matches_only_tensor_definitions() {
        let definition = IndexDefinition {
            index_id: 1,
            bucket: "objects".into(),
            name: "model-tensors".into(),
            path_prefix: "models/".into(),
            content_type: String::new(),
            kind: IndexKind::Tensor as i32,
            specification: Some(IndexSpecification {
                specification: Some(index_specification::Specification::Tensor(
                    TensorIndexSpec {
                        model_id: "encoder-v1".into(),
                    },
                )),
            }),
            version: 1,
        };
        let tensor = IndexQuery {
            query: Some(index_query::Query::Tensor(TensorIndexQuery {
                tensor_name: "encoder.weight".into(),
            })),
        };
        assert!(validate_query_kind(&definition, &tensor).is_ok());

        let path = IndexQuery {
            query: Some(index_query::Query::Path(PathIndexQuery {
                prefix: String::new(),
                start_after: None,
            })),
        };
        assert_eq!(
            validate_query_kind(&definition, &path).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let mut path_definition = definition;
        path_definition.kind = IndexKind::Path as i32;
        path_definition.specification = Some(IndexSpecification {
            specification: Some(index_specification::Specification::Path(PathIndexSpec {})),
        });
        assert_eq!(
            validate_query_kind(&path_definition, &tensor)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}
