//! One-hop execution on a weighted-HRW index query replica.
//!
//! The peer protocol carries the original signed bearer or fixed anonymous
//! marker plus raw query inputs. It never carries a serialized caller or an
//! authorization decision.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anvil_api::v1::index_query::Query as IndexQueryValue;
use anvil_api::v1::{IndexDefinition, IndexKind, IndexQuery, IndexQueryHit};
use anvil_consensus::NodeId;
use anvil_store::{ObjectKey, PlacementLogId, StorageTenantId};
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
    ExecutedIndexQuery, IndexAuthorization, IndexLiveVersionReader, IndexPageCursor,
    collect_authorized_page, definition_path, path_matches_prefix, retain_live_query_hits,
};
use crate::logical_name_resolution::LogicalNameResolver;

const MAX_QUERY_HITS: usize = 1_000;
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
}

#[derive(Clone, Debug)]
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
        let caller = routed_caller(&self.tokens, &call.bearer, &call.request.storage_tenant)?;
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
        let deadline = call.deadline;
        let limit = request.limit;
        let tenant_id = request.tenant_id;
        let bucket_id = request.bucket_id;
        let resume = request.resume;
        let executor = self.executor.clone();
        let storage_tenant = caller.storage_tenant().as_str().to_owned();
        let execute_definition = request.definition.clone();
        let execute_query = request.query.clone();
        let authorization = self.authorization.clone();
        let live_versions = self.live_versions.clone();
        let authorization_caller = caller.clone();
        let authorization_definition = request.definition.clone();
        collect_authorized_page(
            limit,
            resume,
            Some(before.revision),
            move |resume, execute_limit| {
                let executor = executor.clone();
                let storage_tenant = storage_tenant.clone();
                let definition = execute_definition.clone();
                let query = execute_query.clone();
                async move {
                    let resumed = resume.clone();
                    let result = executor
                        .execute_local(LocalIndexQueryRequest {
                            storage_tenant,
                            tenant_id,
                            bucket_id,
                            definition,
                            query,
                            limit: execute_limit,
                            resume,
                        })
                        .await?;
                    validate_result(&result, resumed.as_ref(), execute_limit)?;
                    Ok(result)
                }
            },
            move |hits| {
                let authorization = authorization.clone();
                let live_versions = live_versions.clone();
                let caller = authorization_caller.clone();
                let definition = authorization_definition.clone();
                let definition_key = definition_key.clone();
                async move {
                    let mut keys = Vec::with_capacity(hits.len() + 1);
                    keys.push(definition_key);
                    for hit in &hits {
                        keys.push(validate_hit(&caller, &definition, hit)?);
                    }
                    let checks = keys
                        .into_iter()
                        .map(|key| (key, ObjectPermission::Get))
                        .collect::<Vec<_>>();
                    let evidence = authorization
                        .allows_objects_with_evidence(&caller, &checks)
                        .await?;
                    require_authorization_evidence(&evidence, hits.len() + 1)?;
                    if !evidence.allowed[0] {
                        return Err(Status::permission_denied(
                            "index definition read is no longer authorized",
                        ));
                    }
                    let authorized = hits
                        .into_iter()
                        .zip(evidence.allowed.into_iter().skip(1))
                        .filter_map(|(hit, allowed)| allowed.then_some(hit))
                        .collect();
                    let visible = retain_live_query_hits(
                        live_versions.as_ref(),
                        tenant_id,
                        bucket_id,
                        authorized,
                        crate::v05::deadline_remaining(deadline)?,
                    )
                    .await?;
                    Ok((visible, evidence.revision))
                }
            },
        )
        .await
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
            generation: resume.generation,
            last_position: resume.last_position,
            authorization_revision: resume.authorization_revision,
        }),
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
            generation: resume.generation,
            last_position: resume.last_position.clone(),
            authorization_revision: resume.authorization_revision,
        }),
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
    })
}

fn response_from_wire(
    response: wire::RoutedIndexQueryResponse,
) -> Result<ExecutedIndexQuery, Status> {
    let result = ExecutedIndexQuery {
        hits: response.hits,
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
        if resume.generation == 0
            || resume.authorization_revision == 0
            || resume.last_position.is_empty()
        {
            return Err(Status::invalid_argument(
                "routed index continuation is invalid",
            ));
        }
    }
    Ok(())
}

fn routed_caller(
    tokens: &JwtManager,
    bearer: &str,
    storage_tenant: &str,
) -> Result<crate::authentication::Caller, Status> {
    let routed = OriginalBearer::from_signed_token(bearer);
    let caller = if routed.is_anonymous() {
        let tenant = StorageTenantId::parse(storage_tenant)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        crate::authentication::Caller::from_anonymous(tenant)
    } else {
        tokens
            .verify(routed.signed_token())
            .map_err(|_| Status::unauthenticated("the routed bearer token is invalid or expired"))?
    };
    if caller.storage_tenant().as_str() != storage_tenant {
        return Err(Status::permission_denied(
            "routed index query does not belong to the authenticated tenant",
        ));
    }
    Ok(caller)
}

fn validate_result(
    result: &ExecutedIndexQuery,
    resume: Option<&IndexPageCursor>,
    limit: usize,
) -> Result<(), Status> {
    if result.hits.len() > limit
        || result.next_position.as_ref().is_some_and(Vec::is_empty)
        || (result.next_position.is_some() && result.freshness.generation == 0)
    {
        return Err(Status::data_loss("routed index result is invalid"));
    }
    if resume.is_some_and(|resume| resume.generation != result.freshness.generation) {
        return Err(Status::failed_precondition(
            "requested index generation is no longer available",
        ));
    }
    Ok(())
}

fn validate_query_kind(definition: &IndexDefinition, query: &IndexQuery) -> Result<(), Status> {
    let kind = IndexKind::try_from(definition.kind)
        .map_err(|_| Status::invalid_argument("routed index kind is unknown"))?;
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
            "routed query type does not match the index definition",
        ))
    }
}

fn require_query_replica(
    node: NodeId,
    placement: &ClusterPlacement,
    tenant_id: u64,
    bucket_id: u64,
    index_id: u64,
) -> Result<(), Status> {
    let identity = IndexIdentity::new(tenant_id, bucket_id, index_id)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let indexes = IndexPlacement::derive(identity, placement)
        .map_err(|error| Status::unavailable(error.to_string()))?;
    if indexes.fence() != placement.fence() || !indexes.query_replicas().contains(&node) {
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

fn validate_hit(
    caller: &crate::authentication::Caller,
    definition: &IndexDefinition,
    hit: &IndexQueryHit,
) -> Result<ObjectKey, Status> {
    let address = hit
        .address
        .as_ref()
        .ok_or_else(|| Status::data_loss("routed index hit has no object address"))?;
    let kind = IndexKind::try_from(definition.kind)
        .map_err(|_| Status::data_loss("routed index definition has an unknown kind"))?;
    let references_another_object = matches!(kind, IndexKind::GitSource | IndexKind::Tensor);
    if hit.object_version == 0
        || hit.score.is_some_and(|score| !score.is_finite())
        || address.tenant != caller.storage_tenant().as_str()
        || address.bucket != definition.bucket
        || (!references_another_object
            && !path_matches_prefix(&address.path, &definition.path_prefix))
        || address.path.split('/').any(|segment| segment == "_anvil")
    {
        return Err(Status::data_loss(
            "routed index hit is invalid or outside the definition scope",
        ));
    }
    ObjectKey::new(&address.tenant, &address.bucket, &address.path)
        .map_err(|_| Status::data_loss("routed index hit has an invalid object address"))
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
    use anvil_api::v1::{
        IndexSpecification, ObjectAddress, PathIndexQuery, PathIndexSpec, index_query,
        index_specification,
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
                name: "by-path".into(),
                path_prefix: "docs/".into(),
                content_type: String::new(),
                kind: IndexKind::Path as i32,
                specification: Some(IndexSpecification {
                    specification: Some(index_specification::Specification::Path(PathIndexSpec {})),
                }),
                version: 13,
            },
            query: IndexQuery {
                query: Some(index_query::Query::Path(PathIndexQuery {
                    prefix: "docs/".into(),
                    start_after: None,
                })),
            },
            limit: 100,
            resume: Some(IndexPageCursor {
                generation: 17,
                last_position: b"docs/a".to_vec(),
                authorization_revision: 19,
            }),
        }
    }

    fn caller() -> crate::authentication::Caller {
        crate::authentication::Caller::from_authenticated_application(
            StorageTenantId::parse("tenant").unwrap(),
            "application",
        )
        .unwrap()
    }

    fn hit(path: &str) -> IndexQueryHit {
        IndexQueryHit {
            address: Some(ObjectAddress {
                tenant: "tenant".into(),
                bucket: "objects".into(),
                path: path.into(),
            }),
            object_version: 23,
            score: None,
            fields_json: Vec::new(),
        }
    }

    #[test]
    fn git_and_tensor_references_may_leave_the_manifest_prefix() {
        let caller = caller();
        for (kind, path) in [
            (IndexKind::GitSource, "packs/repository.pack"),
            (IndexKind::Tensor, "tensors/model.bin"),
        ] {
            let mut definition = request().definition;
            definition.kind = kind as i32;

            let key = validate_hit(&caller, &definition, &hit(path)).unwrap();
            assert_eq!(key.path(), path);
        }
    }

    #[test]
    fn ordinary_results_remain_inside_the_manifest_prefix() {
        let definition = request().definition;
        let error =
            validate_hit(&caller(), &definition, &hit("packs/repository.pack")).unwrap_err();

        assert_eq!(error.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn referenced_results_remain_tenant_bucket_and_namespace_scoped() {
        let mut definition = request().definition;
        definition.kind = IndexKind::GitSource as i32;
        let mut wrong_tenant = hit("packs/repository.pack");
        wrong_tenant.address.as_mut().unwrap().tenant = "another".into();
        let mut wrong_bucket = hit("packs/repository.pack");
        wrong_bucket.address.as_mut().unwrap().bucket = "another".into();

        for invalid in [wrong_tenant, wrong_bucket, hit("packs/_anvil/private.pack")] {
            assert_eq!(
                validate_hit(&caller(), &definition, &invalid)
                    .unwrap_err()
                    .code(),
                tonic::Code::DataLoss
            );
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
        let anonymous =
            routed_caller(&tokens, anvil_authz::ANONYMOUS_SUBJECT_ID, "tenant").unwrap();
        assert_eq!(anonymous.storage_tenant().as_str(), "tenant");
        assert_eq!(anonymous.subject(), &anvil_authz::ObjectRef::anonymous());

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
    fn routing_fence_does_not_replace_generation_freshness_fence() {
        let response = response_to_wire(
            ExecutedIndexQuery {
                hits: Vec::new(),
                freshness: anvil_api::v1::IndexFreshness {
                    generation: 1,
                    placement_term: 31,
                    placement_index: 32,
                    ..Default::default()
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
