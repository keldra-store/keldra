//! One-hop execution on a weighted-HRW index query replica.
//!
//! The peer protocol carries the original signed bearer or fixed anonymous
//! marker plus raw query inputs. It never carries a serialized caller or an
//! authorization decision.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use keldra_api::v1::index_query::Query as IndexQueryValue;
use keldra_api::v1::{IndexAggregateOperation, IndexDefinition, IndexKind, IndexQuery};
use keldra_consensus::NodeId;
use keldra_store::{ObjectKey, PlacementLogId, StorageTenantId};
use tonic::{Request, Response, Status};

use super::transport::add_bearer_and_timeout_with_limit;
use super::{
    CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, ClusterPeerTransport, require_response_schema,
    wire,
};
use crate::authentication::JwtManager;
use crate::authorization::ObjectPermission;
use crate::cluster_placement::ClusterPlacement;
use crate::distributed_list::OriginalBearer;
use crate::index_runtime::placement::{IndexIdentity, IndexPlacement};
use crate::index_service::{
    AuthorizedCurrentCandidates, ExecutedIndexQuery, IndexAuthorization, IndexCandidateVisibility,
    IndexFreshnessRequirement, IndexLiveVersionReader, IndexPageCursor,
    RequiredIndexSourceCheckpoint, definition_path,
};
use crate::logical_name_resolution::LogicalNameResolver;

const MAX_QUERY_HITS: usize = 1_000;
const MAX_QUERY_COMPUTATIONS: usize = 32;
const MAX_FACET_BUCKETS: usize = 1_000;
/// The peer context represents remaining time as milliseconds in a `u32`.
/// Public ingress already clamps this to the configured QueryIndex maximum;
/// this is only the lossless ceiling of the existing one-hop wire field.
const MAX_ROUTED_INDEX_QUERY_TIME: Duration = Duration::from_millis(u32::MAX as u64);

#[derive(Clone, Debug)]
pub(crate) struct RoutedIndexQueryRequest {
    pub(crate) storage_tenant: String,
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) definition: IndexDefinition,
    pub(crate) query: IndexQuery,
    pub(crate) limit: usize,
    pub(crate) resume: Option<IndexPageCursor>,
    pub(crate) required_freshness: Option<IndexFreshnessRequirement>,
}

#[derive(Clone)]
pub(crate) struct LocalIndexQueryRequest {
    /// Verified tenant name used only for constructing ordinary object addresses.
    /// The destination binds it to the signed bearer or fixed anonymous marker,
    /// then verifies that its current stable ID matches `tenant_id`.
    pub(crate) storage_tenant: String,
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) definition: IndexDefinition,
    pub(crate) query: IndexQuery,
    pub(crate) limit: usize,
    pub(crate) resume: Option<IndexPageCursor>,
    /// Mandatory in-loop candidate authorization and exact-current check. It
    /// is process-local and is never serialized onto the peer protocol.
    pub(crate) candidate_visibility: Arc<dyn IndexCandidateVisibility>,
    /// Definition-admission Zanzibar revision which the engine must retain in
    /// freshness even when the query finds no candidates.
    pub(crate) authorization_revision: u64,
    pub(crate) required_freshness: Option<IndexFreshnessRequirement>,
    pub(crate) deadline: tokio::time::Instant,
}

#[tonic::async_trait]
pub(crate) trait LocalIndexQueryExecutor: Send + Sync + 'static {
    async fn execute_local(
        &self,
        request: LocalIndexQueryRequest,
    ) -> Result<ExecutedIndexQuery, Status>;
}

pub(crate) struct RoutedIndexQueryCall {
    bearer: Arc<str>,
    placement: ClusterPlacement,
    deadline: tokio::time::Instant,
    request: RoutedIndexQueryRequest,
}

#[tonic::async_trait]
pub(crate) trait RoutedIndexQueryHandler: Send + Sync + 'static {
    async fn execute(&self, call: RoutedIndexQueryCall) -> Result<ExecutedIndexQuery, Status>;
}

#[derive(Clone, Default)]
pub(crate) struct RoutedIndexQueryHandlers {
    inner: Arc<OnceLock<Arc<dyn RoutedIndexQueryHandler>>>,
}

impl RoutedIndexQueryHandlers {
    pub(crate) fn install(
        &self,
        handler: Arc<dyn RoutedIndexQueryHandler>,
    ) -> Result<(), Arc<dyn RoutedIndexQueryHandler>> {
        self.inner.set(handler)
    }

    fn get(&self) -> Result<Arc<dyn RoutedIndexQueryHandler>, Status> {
        self.inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("routed index query handler is not ready"))
    }
}

/// Destination-side security wrapper around a storage-neutral local engine.
#[derive(Clone)]
pub(crate) struct AuthorizedIndexQueryHandler {
    local_node: NodeId,
    tokens: JwtManager,
    names: LogicalNameResolver,
    authorization: Arc<dyn IndexAuthorization>,
    live_versions: Arc<dyn IndexLiveVersionReader>,
    executor: Arc<dyn LocalIndexQueryExecutor>,
}

impl AuthorizedIndexQueryHandler {
    pub(crate) fn new(
        local_node: NodeId,
        tokens: JwtManager,
        names: LogicalNameResolver,
        authorization: Arc<dyn IndexAuthorization>,
        live_versions: Arc<dyn IndexLiveVersionReader>,
        executor: Arc<dyn LocalIndexQueryExecutor>,
    ) -> Self {
        Self {
            local_node,
            tokens,
            names,
            authorization,
            live_versions,
            executor,
        }
    }
}

#[tonic::async_trait]
impl RoutedIndexQueryHandler for AuthorizedIndexQueryHandler {
    async fn execute(&self, call: RoutedIndexQueryCall) -> Result<ExecutedIndexQuery, Status> {
        validate_request(&call.request)?;
        require_query_replica(
            self.local_node,
            &call.placement,
            call.request.tenant_id,
            call.request.bucket_id,
            call.request.definition.index_id,
        )?;
        let (caller, plugin_scope) =
            routed_caller(&self.tokens, &call.bearer, &call.request.storage_tenant)?;
        let tenant = caller.storage_tenant().as_str();
        let resolved = self
            .names
            .resolve_bucket_ids(tenant, &call.request.definition.bucket)
            .await?;
        if resolved != (call.request.tenant_id, call.request.bucket_id) {
            return Err(Status::failed_precondition(
                "routed index stable IDs no longer match mutable names",
            ));
        }
        let definition_key = definition_object_key(&caller, &call.request.definition)?;
        let before = self
            .authorization
            .allows_objects_with_evidence(
                &caller,
                &[(definition_key.clone(), ObjectPermission::Get)],
            )
            .await?;
        require_authorization_evidence(&before, 1)?;
        if !before.allowed[0] {
            return Err(Status::permission_denied(
                "index definition read is not authorized",
            ));
        }
        if call
            .request
            .resume
            .as_ref()
            .is_some_and(|resume| resume.authorization_revision != before.revision)
        {
            return Err(Status::failed_precondition(
                "page token authorization revision is no longer current",
            ));
        }

        let request = call.request;
        let query_shape = request.query.clone();
        let result_tenant = caller.storage_tenant().as_str().to_owned();
        let result_bucket = request.definition.bucket.clone();
        let authorization_revision = before.revision;
        let resume = request.resume.clone();
        let kind = IndexKind::try_from(request.definition.kind)
            .map_err(|_| Status::data_loss("routed index definition has an unknown kind"))?;
        let candidate_visibility: Arc<dyn IndexCandidateVisibility> =
            Arc::new(AuthorizedCurrentCandidates::new(
                caller.clone(),
                authorization_revision,
                request.definition.bucket.clone(),
                request.definition.path_prefix.clone(),
                kind,
                request.tenant_id,
                request.bucket_id,
                call.deadline,
                plugin_scope,
                self.authorization.clone(),
                self.live_versions.clone(),
            ));
        let result = self
            .executor
            .execute_local(LocalIndexQueryRequest {
                storage_tenant: caller.storage_tenant().as_str().to_owned(),
                tenant_id: request.tenant_id,
                bucket_id: request.bucket_id,
                definition: request.definition,
                query: request.query,
                limit: request.limit,
                resume,
                candidate_visibility,
                authorization_revision,
                required_freshness: request.required_freshness,
                deadline: call.deadline,
            })
            .await?;
        validate_result(&result, request.resume.as_ref(), request.limit)?;
        require_result_scope(&result, &result_tenant, &result_bucket)?;
        require_computation_shape(&result, &query_shape)?;
        require_result_authorization_revision(&result, authorization_revision)?;
        Ok(result)
    }
}

impl ClusterPeerService {
    pub(super) async fn route_index_query_call(
        &self,
        request: Request<wire::RouteIndexQueryRequest>,
    ) -> Result<Response<wire::RoutedIndexQueryResponse>, Status> {
        let admitted = self.admit_with_timeout_limit(
            &request,
            request.get_ref().peer.as_ref(),
            1,
            MAX_ROUTED_INDEX_QUERY_TIME,
        )?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let value = request_from_wire(request.get_ref())?;
        require_query_replica(
            self.local_node,
            &admitted.placement,
            value.tenant_id,
            value.bucket_id,
            value.definition.index_id,
        )?;
        let fence = admitted.placement.fence();
        let deadline = tokio::time::Instant::now()
            .checked_add(admitted.timeout)
            .ok_or_else(|| Status::invalid_argument("routed index query deadline overflowed"))?;
        let result = tokio::time::timeout(
            admitted.timeout,
            self.routed_index_queries
                .get()?
                .execute(RoutedIndexQueryCall {
                    bearer: Arc::from(bearer.signed_token()),
                    placement: admitted.placement,
                    deadline,
                    request: value,
                }),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed index query deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response_to_wire(result, fence)?))
    }
}

impl ClusterPeerTransport {
    pub(crate) async fn route_index_query(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: RoutedIndexQueryRequest,
        remaining: Duration,
    ) -> Result<ExecutedIndexQuery, Status> {
        validate_request(&value)?;
        let placement = self.placement()?;
        require_query_replica(
            target,
            &placement,
            value.tenant_id,
            value.bucket_id,
            value.definition.index_id,
        )?;
        let fence = placement.fence();
        let limit = value.limit;
        let resumed = value.resume.clone();
        let query_shape = value.query.clone();
        let result_tenant = value.storage_tenant.clone();
        let result_bucket = value.definition.bucket.clone();
        let remaining = remaining.min(MAX_ROUTED_INDEX_QUERY_TIME);
        let mut request = Request::new(request_to_wire(
            self.context_with_timeout_limit(fence, 1, remaining, MAX_ROUTED_INDEX_QUERY_TIME)?,
            value,
        )?);
        add_bearer_and_timeout_with_limit(
            &mut request,
            bearer,
            remaining,
            MAX_ROUTED_INDEX_QUERY_TIME,
        )?;
        let response = self
            .client(target, address)?
            .route_index_query(request)
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        let response_fence = PlacementLogId {
            term: response.routing_placement_term,
            index: response.routing_placement_index,
        };
        let result = response_from_wire(response)?;
        validate_result(&result, resumed.as_ref(), limit)?;
        require_result_scope(&result, &result_tenant, &result_bucket)?;
        require_computation_shape(&result, &query_shape)?;
        if response_fence != fence {
            return Err(Status::data_loss(
                "routed index response carries another placement fence",
            ));
        }
        Ok(result)
    }
}

fn request_to_wire(
    peer: wire::PeerContext,
    request: RoutedIndexQueryRequest,
) -> Result<wire::RouteIndexQueryRequest, Status> {
    let limit = u32::try_from(request.limit)
        .map_err(|_| Status::invalid_argument("index query limit does not fit u32"))?;
    Ok(wire::RouteIndexQueryRequest {
        peer: Some(peer),
        storage_tenant: request.storage_tenant,
        tenant_id: request.tenant_id,
        bucket_id: request.bucket_id,
        definition: Some(request.definition),
        query: Some(request.query),
        limit,
        resume: request.resume.map(|resume| wire::RoutedIndexQueryResume {
            commit_revision: resume.commit_revision,
            last_position: resume.last_position,
            authorization_revision: resume.authorization_revision,
        }),
        required_freshness: request.required_freshness.map(freshness_to_wire),
    })
}

fn request_from_wire(
    request: &wire::RouteIndexQueryRequest,
) -> Result<RoutedIndexQueryRequest, Status> {
    let value = RoutedIndexQueryRequest {
        storage_tenant: request.storage_tenant.clone(),
        tenant_id: request.tenant_id,
        bucket_id: request.bucket_id,
        definition: request
            .definition
            .clone()
            .ok_or_else(|| Status::invalid_argument("routed index definition is required"))?,
        query: request
            .query
            .clone()
            .ok_or_else(|| Status::invalid_argument("routed index query is required"))?,
        limit: usize::try_from(request.limit)
            .map_err(|_| Status::invalid_argument("routed index limit does not fit this node"))?,
        resume: request.resume.as_ref().map(|resume| IndexPageCursor {
            commit_revision: resume.commit_revision,
            last_position: resume.last_position.clone(),
            authorization_revision: resume.authorization_revision,
        }),
        required_freshness: request
            .required_freshness
            .as_ref()
            .map(freshness_from_wire)
            .transpose()?,
    };
    validate_request(&value)?;
    Ok(value)
}

fn response_to_wire(
    result: ExecutedIndexQuery,
    routing_fence: PlacementLogId,
) -> Result<wire::RoutedIndexQueryResponse, Status> {
    validate_result(&result, None, MAX_QUERY_HITS)?;
    Ok(wire::RoutedIndexQueryResponse {
        schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        hits: result.hits,
        freshness: Some(result.freshness),
        next_position: result.next_position,
        routing_placement_term: routing_fence.term,
        routing_placement_index: routing_fence.index,
        facet_results: result.facet_results,
        aggregate_results: result.aggregate_results,
    })
}

fn response_from_wire(
    response: wire::RoutedIndexQueryResponse,
) -> Result<ExecutedIndexQuery, Status> {
    let result = ExecutedIndexQuery {
        hits: response.hits,
        facet_results: response.facet_results,
        aggregate_results: response.aggregate_results,
        freshness: response
            .freshness
            .ok_or_else(|| Status::data_loss("routed index response has no freshness"))?,
        next_position: response.next_position,
    };
    validate_result(&result, None, MAX_QUERY_HITS)?;
    Ok(result)
}

fn validate_request(request: &RoutedIndexQueryRequest) -> Result<(), Status> {
    StorageTenantId::parse(&request.storage_tenant)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if request.tenant_id == 0
        || request.bucket_id == 0
        || request.definition.index_id == 0
        || request.definition.version == 0
        || request.limit == 0
        || request.limit > MAX_QUERY_HITS
        || request.definition.specification.is_none()
    {
        return Err(Status::invalid_argument(
            "routed index identity, definition, and limit must be valid",
        ));
    }
    definition_path(&request.definition.name)?;
    ObjectKey::new("validation", &request.definition.bucket, "validation")
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    validate_query_kind(&request.definition, &request.query)?;
    if let Some(resume) = request.resume.as_ref() {
        if resume.commit_revision == 0
            || resume.authorization_revision == 0
            || resume.last_position.is_empty()
        {
            return Err(Status::invalid_argument(
                "routed index continuation is invalid",
            ));
        }
    }
    if request.resume.is_some() && request.required_freshness.is_some() {
        return Err(Status::invalid_argument(
            "routed required freshness cannot be combined with a continuation",
        ));
    }
    Ok(())
}

fn freshness_to_wire(
    requirement: IndexFreshnessRequirement,
) -> wire::RoutedIndexFreshnessRequirement {
    wire::RoutedIndexFreshnessRequirement {
        sources: requirement
            .sources
            .into_iter()
            .map(|source| wire::RoutedIndexRequiredSourceCheckpoint {
                node_id: source.node_id,
                source_epoch: source.source_epoch.to_vec(),
                next_offset: source.next_offset,
            })
            .collect(),
        atomic_through: requirement.atomic_through,
    }
}

fn freshness_from_wire(
    requirement: &wire::RoutedIndexFreshnessRequirement,
) -> Result<IndexFreshnessRequirement, Status> {
    if requirement.sources.is_empty() && requirement.atomic_through.is_none() {
        return Err(Status::invalid_argument(
            "routed required freshness has no checkpoint",
        ));
    }
    let mut sources = requirement
        .sources
        .iter()
        .map(|source| {
            let node_id = u16::try_from(source.node_id).map_err(|_| {
                Status::invalid_argument("routed freshness source node ID is invalid")
            })?;
            let source_epoch = source.source_epoch.as_slice().try_into().map_err(|_| {
                Status::invalid_argument("routed freshness source epoch is invalid")
            })?;
            Ok(RequiredIndexSourceCheckpoint {
                node_id: u64::from(node_id),
                source_epoch,
                next_offset: source.next_offset,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    sources.sort_by_key(|source| source.node_id);
    if sources.len() > 1_024
        || sources
            .windows(2)
            .any(|pair| pair[0].node_id == pair[1].node_id)
    {
        return Err(Status::invalid_argument(
            "routed required freshness source vector is invalid",
        ));
    }
    Ok(IndexFreshnessRequirement {
        sources,
        atomic_through: requirement.atomic_through,
    })
}

fn routed_caller(
    tokens: &JwtManager,
    bearer: &str,
    storage_tenant: &str,
) -> Result<
    (
        crate::authentication::Caller,
        Option<crate::authentication::PluginObjectScope>,
    ),
    Status,
> {
    let routed = OriginalBearer::from_signed_token(bearer);
    let (caller, plugin_scope) = if routed.is_anonymous() {
        let tenant = StorageTenantId::parse(storage_tenant)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        (crate::authentication::Caller::from_anonymous(tenant), None)
    } else {
        tokens
            .verify_object_bearer(routed.signed_token())
            .map_err(|_| Status::unauthenticated("the routed bearer token is invalid or expired"))?
    };
    if caller.storage_tenant().as_str() != storage_tenant {
        return Err(Status::permission_denied(
            "routed index query does not belong to the authenticated tenant",
        ));
    }
    Ok((caller, plugin_scope))
}

fn validate_result(
    result: &ExecutedIndexQuery,
    resume: Option<&IndexPageCursor>,
    limit: usize,
) -> Result<(), Status> {
    if result.hits.len() > limit
        || result
            .facet_results
            .len()
            .saturating_add(result.aggregate_results.len())
            > MAX_QUERY_COMPUTATIONS
        || result
            .facet_results
            .iter()
            .any(|facet| facet.buckets.len() > MAX_FACET_BUCKETS)
        || result
            .facet_results
            .iter()
            .any(|facet| !valid_facet_result(facet))
        || result
            .aggregate_results
            .iter()
            .any(|aggregate| !valid_aggregate_result(aggregate))
        || result.freshness.index_id == 0
        || result.freshness.definition_version == 0
        || result.freshness.authorization_revision == 0
        || result
            .freshness
            .sources
            .iter()
            .any(|source| source.node_id == 0 || source.source_epoch.len() != 32)
        || result
            .freshness
            .sources
            .windows(2)
            .any(|pair| pair[0].node_id >= pair[1].node_id)
        || (result.freshness.commit_revision == 0
            && (result.freshness.published_at.is_some()
                || result.freshness.placement_term != 0
                || result.freshness.placement_index != 0))
        || (result.freshness.commit_revision != 0
            && (result.freshness.published_at.is_none()
                || result.freshness.placement_term == 0
                || result.freshness.placement_index == 0))
        || result.next_position.as_ref().is_some_and(Vec::is_empty)
        || (result.next_position.is_some() && result.freshness.commit_revision == 0)
    {
        return Err(Status::data_loss("routed index result is invalid"));
    }
    if resume.is_some_and(|resume| resume.commit_revision != result.freshness.commit_revision) {
        return Err(Status::failed_precondition(
            "requested index commit revision is no longer available",
        ));
    }
    Ok(())
}

fn require_result_scope(
    result: &ExecutedIndexQuery,
    tenant: &str,
    bucket: &str,
) -> Result<(), Status> {
    for hit in &result.hits {
        let address = hit
            .address
            .as_ref()
            .ok_or_else(|| Status::data_loss("routed index hit has no object address"))?;
        if hit.object_version == 0
            || hit.score.is_some_and(|score| !score.is_finite())
            || address.tenant != tenant
            || address.bucket != bucket
            || address.path.split('/').any(|segment| segment == "_keldra")
            || ObjectKey::new(&address.tenant, &address.bucket, &address.path).is_err()
        {
            return Err(Status::data_loss(
                "routed index hit is invalid or outside the requested scope",
            ));
        }
    }
    Ok(())
}

fn valid_facet_result(result: &keldra_api::v1::IndexFacetResult) -> bool {
    result
        .buckets
        .iter()
        .all(|bucket| bucket.count != 0 && canonical_scalar(&bucket.value_json).is_some())
        && result.buckets.windows(2).all(|pair| {
            pair[0].count > pair[1].count
                || pair[0].count == pair[1].count
                    && pair[0].value_json.as_slice() < pair[1].value_json.as_slice()
        })
}

fn valid_aggregate_result(result: &keldra_api::v1::IndexAggregateResult) -> bool {
    let Ok(operation) = IndexAggregateOperation::try_from(result.operation) else {
        return false;
    };
    let value = result.value_json.as_deref().and_then(canonical_scalar);
    match operation {
        IndexAggregateOperation::Count => {
            value.as_ref().and_then(serde_json::Value::as_u64) == Some(result.contributing_count)
        }
        IndexAggregateOperation::Minimum
        | IndexAggregateOperation::Maximum
        | IndexAggregateOperation::Sum
        | IndexAggregateOperation::Average => {
            if result.contributing_count == 0 {
                result.value_json.is_none()
            } else {
                value.is_some_and(|value| value.is_number())
            }
        }
    }
}

fn canonical_scalar(encoded: &[u8]) -> Option<serde_json::Value> {
    let value = serde_json::from_slice::<serde_json::Value>(encoded).ok()?;
    if matches!(
        value,
        serde_json::Value::Array(_) | serde_json::Value::Object(_)
    ) || serde_json::to_vec(&value).ok()?.as_slice() != encoded
    {
        None
    } else {
        Some(value)
    }
}

fn require_computation_shape(
    result: &ExecutedIndexQuery,
    query: &IndexQuery,
) -> Result<(), Status> {
    let Some(IndexQueryValue::TypedJson(query)) = query.query.as_ref() else {
        if result.facet_results.is_empty() && result.aggregate_results.is_empty() {
            return Ok(());
        }
        return Err(Status::data_loss(
            "non-Typed-JSON routed query returned computation results",
        ));
    };
    let facets_match = result.facet_results.len() == query.facets.len()
        && result
            .facet_results
            .iter()
            .zip(&query.facets)
            .all(|(result, request)| {
                result.field == request.field && result.buckets.len() <= request.limit as usize
            });
    let aggregates_match = result.aggregate_results.len() == query.aggregates.len()
        && result
            .aggregate_results
            .iter()
            .zip(&query.aggregates)
            .all(|(result, request)| {
                result.field == request.field && result.operation == request.operation
            });
    if facets_match && aggregates_match {
        Ok(())
    } else {
        Err(Status::data_loss(
            "routed query computation results do not match the request",
        ))
    }
}

fn require_result_authorization_revision(
    result: &ExecutedIndexQuery,
    required: u64,
) -> Result<(), Status> {
    if required == 0 || result.freshness.authorization_revision == 0 {
        return Err(Status::data_loss(
            "routed index result has no Zanzibar authorization revision",
        ));
    }
    if result.freshness.authorization_revision != required {
        return Err(Status::failed_precondition(
            "authorization revision changed during index execution",
        ));
    }
    Ok(())
}

fn validate_query_kind(definition: &IndexDefinition, query: &IndexQuery) -> Result<(), Status> {
    let kind = IndexKind::try_from(definition.kind)
        .map_err(|_| Status::invalid_argument("routed index kind is unknown"))?;
    let matches = matches!(
        (kind, query.query.as_ref()),
        (IndexKind::TypedJson, Some(IndexQueryValue::TypedJson(_)))
    );
    if matches {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "routed query type does not match the index definition",
        ))
    }
}

fn require_query_replica(
    node: NodeId,
    placement: &ClusterPlacement,
    tenant_id: u64,
    bucket_id: u64,
    _index_id: u64,
) -> Result<(), Status> {
    let identity = IndexIdentity::projection_partition(tenant_id, bucket_id)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let indices = IndexPlacement::derive(identity, placement)
        .map_err(|error| Status::unavailable(error.to_string()))?;
    if indices.fence() != placement.fence() || !indices.query_replicas().contains(&node) {
        Err(Status::failed_precondition(
            "node is not a current weighted-HRW index query replica",
        ))
    } else {
        Ok(())
    }
}

fn definition_object_key(
    caller: &crate::authentication::Caller,
    definition: &IndexDefinition,
) -> Result<ObjectKey, Status> {
    ObjectKey::new(
        caller.storage_tenant().as_str(),
        &definition.bucket,
        definition_path(&definition.name)?,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn require_authorization_evidence(
    evidence: &crate::index_service::IndexAuthorizationEvidence,
    expected: usize,
) -> Result<(), Status> {
    if evidence.revision == 0 || evidence.allowed.len() != expected {
        Err(Status::data_loss(
            "Zanzibar returned invalid routed index authorization evidence",
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use keldra_api::v1::{
        IndexAggregateOperation, IndexAggregateRequest, IndexAggregateResult, IndexFacetBucket,
        IndexFacetRequest, IndexFacetResult, IndexSpecification, PathIndexQuery, PathIndexSpec,
        TypedJsonIndexQuery, index_query, index_specification,
    };

    use super::*;

    fn request() -> RoutedIndexQueryRequest {
        RoutedIndexQueryRequest {
            storage_tenant: "tenant".into(),
            tenant_id: 7,
            bucket_id: 9,
            definition: IndexDefinition {
                index_id: 11,
                bucket: "objects".into(),
                name: "typed-json".into(),
                path_prefix: "docs/".into(),
                content_type: String::new(),
                kind: IndexKind::TypedJson as i32,
                specification: Some(IndexSpecification {
                    specification: Some(index_specification::Specification::TypedJson(
                        Default::default(),
                    )),
                }),
                version: 13,
            },
            query: IndexQuery {
                query: Some(index_query::Query::TypedJson(Default::default())),
            },
            limit: 100,
            resume: Some(IndexPageCursor {
                commit_revision: 17,
                last_position: b"docs/a".to_vec(),
                authorization_revision: 19,
            }),
            required_freshness: None,
        }
    }

    fn published_freshness() -> keldra_api::v1::IndexFreshness {
        keldra_api::v1::IndexFreshness {
            commit_revision: 1,
            published_at: Some(Default::default()),
            authorization_revision: 19,
            placement_term: 1,
            placement_index: 1,
            index_id: 11,
            definition_version: 13,
            ..Default::default()
        }
    }

    #[test]
    fn peer_wire_round_trip_contains_no_caller_or_allow_decision() {
        let value = request();
        let wire = request_to_wire(
            wire::PeerContext {
                schema_version: 1,
                cluster_id: vec![1; 16],
                source_node_id: 2,
                placement_term: 3,
                placement_index: 4,
                hop_count: 1,
                remaining_deadline_millis: 1000,
            },
            value.clone(),
        )
        .unwrap();
        let decoded = request_from_wire(&wire).unwrap();

        assert_eq!(decoded.tenant_id, value.tenant_id);
        assert_eq!(decoded.storage_tenant, value.storage_tenant);
        assert_eq!(decoded.bucket_id, value.bucket_id);
        assert_eq!(decoded.definition, value.definition);
        assert_eq!(decoded.query, value.query);
        assert_eq!(decoded.resume, value.resume);
    }

    #[test]
    fn malformed_resume_and_mismatched_query_fail_before_execution() {
        let mut invalid = request();
        invalid.resume.as_mut().unwrap().last_position.clear();
        assert!(validate_request(&invalid).is_err());

        invalid = request();
        invalid.query.query = Some(index_query::Query::Path(PathIndexQuery {
            prefix: "docs/".into(),
            start_after: None,
        }));
        invalid.definition.kind = IndexKind::Vector as i32;
        assert!(validate_request(&invalid).is_err());

        invalid = request();
        invalid.storage_tenant.clear();
        assert!(validate_request(&invalid).is_err());
    }

    #[test]
    fn routed_anonymous_marker_reconstructs_only_the_named_tenant() {
        let tokens = JwtManager::new(b"anonymous-index-route-secret-0123456789").unwrap();
        let (anonymous, plugin_scope) =
            routed_caller(&tokens, keldra_authz::ANONYMOUS_SUBJECT_ID, "tenant").unwrap();
        assert!(plugin_scope.is_none());
        assert_eq!(anonymous.storage_tenant().as_str(), "tenant");
        assert_eq!(anonymous.subject(), &keldra_authz::ObjectRef::anonymous());

        let token = tokens
            .mint(StorageTenantId::parse("tenant").unwrap(), "reader")
            .unwrap();
        assert_eq!(
            routed_caller(&tokens, &token, "another")
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[test]
    fn routed_plugin_bearer_preserves_its_object_scope() {
        let tokens = JwtManager::new(b"plugin-index-route-secret-012345678901").unwrap();
        let token = tokens
            .mint_plugin_token(
                StorageTenantId::parse("tenant").unwrap(),
                "plugin",
                "bucket",
                "allowed/root",
            )
            .unwrap();
        let (caller, plugin_scope) = routed_caller(&tokens, &token, "tenant").unwrap();

        assert_eq!(caller.storage_tenant().as_str(), "tenant");
        let plugin_scope = plugin_scope.expect("plugin bearer must retain its scope");
        assert!(plugin_scope.allows("tenant", "bucket", "allowed/root/object"));
        assert!(!plugin_scope.allows("tenant", "bucket", "another/object"));
    }

    #[test]
    fn routing_fence_does_not_replace_committed_view_freshness_fence() {
        let response = response_to_wire(
            ExecutedIndexQuery {
                hits: Vec::new(),
                facet_results: Vec::new(),
                aggregate_results: Vec::new(),
                freshness: keldra_api::v1::IndexFreshness {
                    placement_term: 31,
                    placement_index: 32,
                    ..published_freshness()
                },
                next_position: None,
            },
            PlacementLogId { term: 7, index: 8 },
        )
        .unwrap();

        assert_eq!(response.routing_placement_term, 7);
        assert_eq!(response.routing_placement_index, 8);
        let freshness = response.freshness.unwrap();
        assert_eq!(freshness.placement_term, 31);
        assert_eq!(freshness.placement_index, 32);
    }

    #[test]
    fn routed_results_preserve_facets_and_aggregates() {
        let expected_facets = vec![IndexFacetResult {
            field: "ecosystem".into(),
            buckets: vec![IndexFacetBucket {
                value_json: br#""cargo""#.to_vec(),
                count: 7,
            }],
        }];
        let expected_aggregates = vec![IndexAggregateResult {
            field: "severity".into(),
            operation: IndexAggregateOperation::Average as i32,
            value_json: Some(b"6.5".to_vec()),
            contributing_count: 4,
        }];
        let wire = response_to_wire(
            ExecutedIndexQuery {
                hits: Vec::new(),
                facet_results: expected_facets.clone(),
                aggregate_results: expected_aggregates.clone(),
                freshness: published_freshness(),
                next_position: None,
            },
            PlacementLogId { term: 7, index: 8 },
        )
        .unwrap();
        let decoded = response_from_wire(wire).unwrap();

        assert_eq!(decoded.facet_results, expected_facets);
        assert_eq!(decoded.aggregate_results, expected_aggregates);
    }

    #[test]
    fn routed_computations_must_match_the_request_shape() {
        let query = IndexQuery {
            query: Some(index_query::Query::TypedJson(TypedJsonIndexQuery {
                predicate: None,
                order: Vec::new(),
                facets: vec![IndexFacetRequest {
                    field: "ecosystem".into(),
                    limit: 1,
                }],
                aggregates: vec![IndexAggregateRequest {
                    field: "severity".into(),
                    operation: IndexAggregateOperation::Maximum as i32,
                }],
            })),
        };
        let mut result = ExecutedIndexQuery {
            hits: Vec::new(),
            facet_results: vec![IndexFacetResult {
                field: "ecosystem".into(),
                buckets: vec![IndexFacetBucket {
                    value_json: br#""cargo""#.to_vec(),
                    count: 7,
                }],
            }],
            aggregate_results: vec![IndexAggregateResult {
                field: "severity".into(),
                operation: IndexAggregateOperation::Maximum as i32,
                value_json: Some(b"9".to_vec()),
                contributing_count: 4,
            }],
            freshness: Default::default(),
            next_position: None,
        };

        require_computation_shape(&result, &query).unwrap();

        result.facet_results[0].field = "state".into();
        assert_eq!(
            require_computation_shape(&result, &query)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
        result.facet_results[0].field = "ecosystem".into();
        result.aggregate_results[0].operation = IndexAggregateOperation::Minimum as i32;
        assert_eq!(
            require_computation_shape(&result, &query)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
        result.aggregate_results[0].operation = IndexAggregateOperation::Maximum as i32;
        result.facet_results[0].buckets.push(IndexFacetBucket {
            value_json: br#""npm""#.to_vec(),
            count: 3,
        });
        assert_eq!(
            require_computation_shape(&result, &query)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
    }

    #[test]
    fn routed_hits_remain_in_the_authenticated_tenant_and_bucket() {
        let mut result = ExecutedIndexQuery {
            hits: vec![keldra_api::v1::IndexQueryHit {
                address: Some(keldra_api::v1::ObjectAddress {
                    tenant: "tenant".into(),
                    bucket: "objects".into(),
                    path: "docs/a".into(),
                }),
                object_version: 3,
                score: Some(1.0),
            }],
            facet_results: Vec::new(),
            aggregate_results: Vec::new(),
            freshness: published_freshness(),
            next_position: None,
        };
        require_result_scope(&result, "tenant", "objects").unwrap();

        result.hits[0].address.as_mut().unwrap().tenant = "another".into();
        assert_eq!(
            require_result_scope(&result, "tenant", "objects")
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
    }

    #[test]
    fn routed_computation_payloads_are_canonical_and_ordered() {
        let mut facet = IndexFacetResult {
            field: "ecosystem".into(),
            buckets: vec![
                IndexFacetBucket {
                    value_json: br#""cargo""#.to_vec(),
                    count: 2,
                },
                IndexFacetBucket {
                    value_json: br#""npm""#.to_vec(),
                    count: 1,
                },
            ],
        };
        assert!(valid_facet_result(&facet));
        facet.buckets.swap(0, 1);
        assert!(!valid_facet_result(&facet));

        let mut aggregate = IndexAggregateResult {
            field: "severity".into(),
            operation: IndexAggregateOperation::Count as i32,
            value_json: Some(b"4".to_vec()),
            contributing_count: 4,
        };
        assert!(valid_aggregate_result(&aggregate));
        aggregate.value_json = Some(b"04".to_vec());
        assert!(!valid_aggregate_result(&aggregate));
    }

    #[test]
    fn even_zero_hit_results_must_retain_the_admission_revision() {
        let mut result = ExecutedIndexQuery {
            hits: Vec::new(),
            facet_results: Vec::new(),
            aggregate_results: Vec::new(),
            freshness: keldra_api::v1::IndexFreshness {
                authorization_revision: 19,
                ..Default::default()
            },
            next_position: None,
        };
        require_result_authorization_revision(&result, 19).unwrap();

        result.freshness.authorization_revision = 0;
        assert_eq!(
            require_result_authorization_revision(&result, 19)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
        result.freshness.authorization_revision = 20;
        assert_eq!(
            require_result_authorization_revision(&result, 19)
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[test]
    fn routed_query_preserves_long_and_short_public_deadlines() {
        for remaining in [Duration::from_secs(120), Duration::from_secs(2)] {
            let mut request = Request::new(());
            add_bearer_and_timeout_with_limit(
                &mut request,
                "signed-token",
                remaining,
                MAX_ROUTED_INDEX_QUERY_TIME,
            )
            .unwrap();
            assert_eq!(grpc_timeout(&request), remaining);

            let context = wire::PeerContext {
                schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                cluster_id: vec![1; 16],
                source_node_id: 2,
                placement_term: 3,
                placement_index: 4,
                hop_count: 1,
                remaining_deadline_millis: u32::try_from(remaining.as_millis()).unwrap(),
            };
            super::super::admission::validate_context_with_timeout_limit(
                &context,
                1,
                MAX_ROUTED_INDEX_QUERY_TIME,
            )
            .unwrap();
        }

        let long = wire::PeerContext {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            cluster_id: vec![1; 16],
            source_node_id: 2,
            placement_term: 3,
            placement_index: 4,
            hop_count: 1,
            remaining_deadline_millis: 120_000,
        };
        assert!(super::super::admission::validate_context(&long, 1).is_err());
    }

    fn grpc_timeout<T>(request: &Request<T>) -> Duration {
        let encoded = request
            .metadata()
            .get("grpc-timeout")
            .unwrap()
            .to_str()
            .unwrap();
        let (value, unit) = encoded.split_at(encoded.len() - 1);
        let value = value.parse::<u64>().unwrap();
        match unit {
            "H" => Duration::from_secs(value * 60 * 60),
            "M" => Duration::from_secs(value * 60),
            "S" => Duration::from_secs(value),
            "m" => Duration::from_millis(value),
            "u" => Duration::from_micros(value),
            "n" => Duration::from_nanos(value),
            _ => panic!("unexpected grpc-timeout unit"),
        }
    }
}
