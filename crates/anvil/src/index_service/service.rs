//! Public index lifecycle and query RPC implementation.

use std::time::Duration;

use anvil_api::v1::bulk_operation::Operation as BulkOperationValue;
use anvil_api::v1::bulk_outcome::Outcome as BulkOutcomeValue;
use anvil_api::v1::index_query::Query as IndexQueryValue;
use anvil_api::v1::index_service_server::IndexService as IndexServiceRpc;
use anvil_api::v1::object_chunk::Value as ObjectChunkValue;
use anvil_api::v1::object_head::State as ObjectState;
use anvil_api::v1::object_service_server::ObjectService;
use anvil_api::v1::{
    BulkOperation, BulkPutIfVersionRequest, BulkPutRequest, BulkWriteRequest, BulkWriteResponse,
    CreateIndexRequest, DeleteIfVersionRequest, DeleteIndexRequest, DeleteIndexResponse,
    Durability, GetIndexRequest, GetObjectRequest, IndexDefinition, IndexKind, IndexQuery,
    IndexQueryHit, ListIndexesRequest, ListIndexesResponse, MutationFailure, MutationFailureCode,
    MutationReceipt, ObjectAddress, QueryIndexRequest, QueryIndexResponse, UpdateIndexRequest,
};
use anvil_store::ObjectKey;
use prost::Message;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use crate::authentication::Caller;
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
    StoredIndexDefinition, collect_authorized_page, definition_path, derive_index_id,
    path_matches_prefix, validate_command_id, validate_create_definition,
    validate_update_definition,
};

const DEFINITION_CONTENT_TYPE: &str = "application/vnd.anvil.index-definition+json";
const DEFAULT_PAGE_LIMIT: usize = 100;
const MAX_PAGE_LIMIT: usize = 1_000;
const QUERY_HASH_CONTEXT: &[u8] = b"anvil.index/query/v1";

#[derive(Clone)]
pub(crate) struct IndexServiceImpl {
    objects: ObjectServiceImpl,
    names: LogicalNameResolver,
    dependencies: IndexServiceDependencies,
    request_timeout: Duration,
}

impl IndexServiceImpl {
    pub(crate) fn new(
        objects: ObjectServiceImpl,
        names: LogicalNameResolver,
        dependencies: IndexServiceDependencies,
        request_timeout: Duration,
    ) -> Self {
        Self {
            objects,
            names,
            dependencies,
            request_timeout,
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
    ) -> Result<MutationReceipt, Status> {
        let response = ObjectService::bulk_write(
            &self.objects,
            forwarded_request(
                context,
                BulkWriteRequest {
                    operations: vec![BulkOperation {
                        operation: Some(operation),
                    }],
                },
            )?,
        )
        .await?
        .into_inner();
        single_bulk_receipt(response)
    }

    async fn authorize_listed_definitions(
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
        let mut keys = Vec::with_capacity(page.definitions.len());
        for definition in &page.definitions {
            keys.push(definition_key(context.caller(), bucket, &definition.name)?);
        }
        let requests = keys
            .iter()
            .cloned()
            .map(|key| (key, ObjectPermission::Get))
            .collect::<Vec<_>>();
        let evidence = self
            .dependencies
            .authorization
            .allows_objects_with_evidence(context.caller(), &requests)
            .await?;
        validate_authorization_evidence(&evidence, page.definitions.len())?;

        let mut visible = Vec::new();
        for ((entry, allowed), key) in page.definitions.into_iter().zip(evidence.allowed).zip(keys)
        {
            if !allowed {
                continue;
            }
            if entry.version == 0 {
                return Err(Status::data_loss(
                    "listed index definition has a zero version",
                ));
            }
            let stored = StoredIndexDefinition::decode(&entry.bytes)?;
            require_definition_identity(&stored, context.caller(), bucket, &entry.name)?;
            if key.path() != definition_path(&entry.name)?.as_str() {
                return Err(Status::data_loss(
                    "listed index definition has the wrong object path",
                ));
            }
            visible.push(stored.to_api(entry.version)?);
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
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
        run_request_until(
            deadline,
            async {
                let context = request_context(&request, deadline)?;
                let request = request.into_inner();
                validate_create_definition(&request)?;
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
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
        run_request_until(
            deadline,
            async {
                let context = request_context(&request, deadline)?;
                let request = request.into_inner();
                validate_update_definition(&request)?;
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
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
        run_request_until(
            deadline,
            async {
                let context = request_context(&request, deadline)?;
                let request = request.into_inner();
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
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
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
                        self.authorize_listed_definitions(
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

    async fn delete_index(
        &self,
        request: Request<DeleteIndexRequest>,
    ) -> Result<Response<DeleteIndexResponse>, Status> {
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
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
                let receipt = ObjectService::delete_if_version(
                    &self.objects,
                    forwarded_request(
                        &context,
                        DeleteIfVersionRequest {
                            address: Some(api_address(&key)),
                            command_id: request.command_id,
                            durability: Durability::Local as i32,
                            expected_version: request.expected_version,
                        },
                    )?,
                )
                .await?
                .into_inner();
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
        let deadline = request_deadline(request.metadata(), self.request_timeout)?;
        run_request_until(
            deadline,
            async {
                let context = request_context(&request, deadline)?;
                let request = request.into_inner();
                let query = request
                    .query
                    .ok_or_else(|| Status::invalid_argument("index query is required"))?;
                let limit = page_limit(request.limit)?;
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
                let queries = self.dependencies.queries.clone();
                let execute_context = context.clone();
                let execute_definition = loaded.api.clone();
                let execute_query = query.clone();
                let authorization = self.dependencies.authorization.clone();
                let authorization_caller = context.caller().clone();
                let authorization_definition = loaded.clone();
                let executed = collect_authorized_page(
                    limit,
                    resume,
                    None,
                    move |resume, execute_limit| {
                        let queries = queries.clone();
                        let context = execute_context.clone();
                        let definition = execute_definition.clone();
                        let query = execute_query.clone();
                        async move {
                            let resumed = resume.clone();
                            let result = queries
                                .execute(ExecuteIndexQuery {
                                    context,
                                    tenant_id,
                                    bucket_id,
                                    definition,
                                    query,
                                    limit: execute_limit,
                                    resume,
                                })
                                .await?;
                            validate_execution(&result, resumed.as_ref(), execute_limit)?;
                            Ok(result)
                        }
                    },
                    move |hits| {
                        let authorization = authorization.clone();
                        let caller = authorization_caller.clone();
                        let definition = authorization_definition.clone();
                        async move {
                            authorize_query_hits_with(
                                authorization.as_ref(),
                                &caller,
                                &definition,
                                hits,
                            )
                            .await
                        }
                    },
                )
                .await?;
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
        .await
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
    if let Some(resume) = resume {
        if execution.freshness.generation != resume.generation {
            return Err(Status::failed_precondition(
                "requested index generation is no longer available",
            ));
        }
    }
    Ok(())
}

fn validate_query_hit(
    caller: &Caller,
    definition: &StoredIndexDefinition,
    hit: &IndexQueryHit,
) -> Result<ObjectKey, Status> {
    let address = hit
        .address
        .as_ref()
        .ok_or_else(|| Status::data_loss("index hit has no object address"))?;
    if hit.object_version == 0 || hit.score.is_some_and(|score| !score.is_finite()) {
        return Err(Status::data_loss("index hit contains invalid result data"));
    }
    if address.tenant != caller.storage_tenant().as_str()
        || address.bucket != definition.bucket
        || !path_matches_prefix(&address.path, &definition.path_prefix)
        || contains_reserved_segment(&address.path)
    {
        return Err(Status::data_loss(
            "index hit is outside the definition's object scope",
        ));
    }
    ObjectKey::new(&address.tenant, &address.bucket, &address.path)
        .map_err(|_| Status::data_loss("index hit has an invalid object address"))
}

async fn authorize_query_hits_with(
    authorization: &dyn super::boundary::IndexAuthorization,
    caller: &Caller,
    definition: &LoadedDefinition,
    hits: Vec<IndexQueryHit>,
) -> Result<(Vec<IndexQueryHit>, u64), Status> {
    let mut keys = Vec::with_capacity(hits.len() + 1);
    keys.push(definition.key.clone());
    for hit in &hits {
        keys.push(validate_query_hit(caller, &definition.stored, hit)?);
    }
    let requests = keys
        .into_iter()
        .map(|key| (key, ObjectPermission::Get))
        .collect::<Vec<_>>();
    let evidence = authorization
        .allows_objects_with_evidence(caller, &requests)
        .await?;
    validate_authorization_evidence(&evidence, hits.len() + 1)?;
    if !evidence.allowed[0] {
        return Err(Status::permission_denied(
            "index definition read is no longer authorized",
        ));
    }
    let visible = hits
        .into_iter()
        .zip(evidence.allowed.into_iter().skip(1))
        .filter_map(|(hit, allowed)| allowed.then_some(hit))
        .collect();
    Ok((visible, evidence.revision))
}

fn contains_reserved_segment(path: &str) -> bool {
    path.split('/').any(|segment| segment == "_anvil")
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use anvil_api::v1::{
        CreateIndexRequest, IndexFreshness, IndexSourceFreshness, IndexSpecification,
        PathIndexQuery, PathIndexSpec, TensorIndexQuery, TensorIndexSpec, index_query,
        index_specification,
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

    fn loaded_definition(path_prefix: &str) -> LoadedDefinition {
        let stored = StoredIndexDefinition::create(
            "tenant".into(),
            CreateIndexRequest {
                bucket: "objects".into(),
                name: "by-path".into(),
                path_prefix: path_prefix.into(),
                content_type: String::new(),
                specification: Some(IndexSpecification {
                    specification: Some(index_specification::Specification::Path(PathIndexSpec {})),
                }),
                command_id: "create-by-path".into(),
            },
            17,
        )
        .unwrap();
        LoadedDefinition {
            key: ObjectKey::new("tenant", "objects", definition_path("by-path").unwrap()).unwrap(),
            api: stored.to_api(3).unwrap(),
            stored,
        }
    }

    fn hit(path: &str, version: u64) -> IndexQueryHit {
        IndexQueryHit {
            address: Some(ObjectAddress {
                tenant: "tenant".into(),
                bucket: "objects".into(),
                path: path.into(),
            }),
            object_version: version,
            score: None,
            fields_json: Vec::new(),
        }
    }

    struct FakeAuthorization {
        allowed: Vec<bool>,
        revision: u64,
        seen: Mutex<Vec<(ObjectKey, ObjectPermission)>>,
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
        assert_eq!(context.signed_bearer(), "signed-token");
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
    fn reserved_internal_paths_cannot_escape_through_query_hits() {
        assert!(contains_reserved_segment("_anvil/indexes/1/current"));
        assert!(contains_reserved_segment("prefix/_anvil/meta.json"));
        assert!(!contains_reserved_segment("prefix/_anvilish/value"));
    }

    #[tokio::test]
    async fn query_authorization_checks_definition_and_every_hit_then_filters() {
        let authorization = FakeAuthorization {
            allowed: vec![true, true, false],
            revision: 29,
            seen: Mutex::new(Vec::new()),
        };
        let definition = loaded_definition("models");
        let (visible, revision) = authorize_query_hits_with(
            &authorization,
            &caller(),
            &definition,
            vec![hit("models/one", 1), hit("models/two", 2)],
        )
        .await
        .unwrap();

        assert_eq!(visible, vec![hit("models/one", 1)]);
        assert_eq!(revision, 29);
        let seen = authorization.seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].0.path(), definition_path("by-path").unwrap());
        assert_eq!(seen[1].0.path(), "models/one");
        assert_eq!(seen[2].0.path(), "models/two");
        assert!(
            seen.iter()
                .all(|(_, permission)| *permission == ObjectPermission::Get)
        );
    }

    #[tokio::test]
    async fn query_authorization_fails_closed_for_definition_or_scope() {
        let denied = FakeAuthorization {
            allowed: vec![false, true],
            revision: 31,
            seen: Mutex::new(Vec::new()),
        };
        let definition = loaded_definition("models");
        assert_eq!(
            authorize_query_hits_with(&denied, &caller(), &definition, vec![hit("models/one", 1)],)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let out_of_scope = FakeAuthorization {
            allowed: vec![true, true],
            revision: 33,
            seen: Mutex::new(Vec::new()),
        };
        assert_eq!(
            authorize_query_hits_with(
                &out_of_scope,
                &caller(),
                &definition,
                vec![hit("models-neighbour/one", 1)],
            )
            .await
            .unwrap_err()
            .code(),
            tonic::Code::DataLoss
        );
        assert!(out_of_scope.seen.lock().unwrap().is_empty());
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
                ..Default::default()
            },
            next_position: None,
        };

        assert!(validate_execution(&execution, None, 100).is_ok());
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
