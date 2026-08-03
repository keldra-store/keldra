//! Cluster-transparent public Zanzibar API.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use anvil_api::v1 as api;
use anvil_api::v1::authz_service_server::AuthzService as PublicAuthzService;
use anvil_authz::{Authorization, AuthorizationCheck};
use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{
    AuthzRevision, AuthzScope, BindSchemaRequest, CoordinatedAuthzRealmResult,
    ProtectedRealmOwnership, PublishSchemaRequest, SchemaId, Store, TupleBatchRequest,
    TupleMutation, TupleMutationKind,
};
use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use super::{
    MAX_CHECKS, PageToken, authz_store_status, binding_to_api, caller, decode_page_token,
    encode_page_token, normalize_page_size, page_fingerprint, require_public_storage_tenant,
    tuple_matches_filter,
};
use crate::authentication::{Caller, JwtManager};
use crate::authoritative_system::AuthoritativeSystemAuthorization;
use crate::authorization::{
    RealmPermission, StorageTenantPermission, realm_authorization_check,
    storage_tenant_authorization_check,
};
use crate::authz_api::{
    DomainTupleMutation, check_from_api, consistency_from_api, public_scope_from_api,
    schema_from_api, schema_ref_from_api, schema_ref_to_api, schema_to_api, tuple_filter_from_api,
    tuple_mutation_from_api, tuple_to_api,
};
use crate::authz_distribution::ZanzibarDistribution;
use crate::cluster_peer::{ClusterPeerTransport, RoutedAuthzHandler, RoutedCall};
use crate::cluster_placement::ClusterPlacement;
use crate::distributed_list::OriginalBearer;
use crate::logical_name_resolution::{LogicalNameResolution, LogicalNameResolver};
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::PlacementKind;

const ROUTE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug)]
pub(crate) struct RoutedAuthzDestination;

#[derive(Clone)]
pub(crate) struct DistributedAuthzService {
    local_node: NodeId,
    store: Store,
    decisions: DecisionRaft,
    zanzibar: Arc<ZanzibarDistribution>,
    peers: ClusterPeerTransport,
    names: LogicalNameResolver,
    system: AuthoritativeSystemAuthorization,
    tokens: JwtManager,
}

impl DistributedAuthzService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        local_node: NodeId,
        store: Store,
        decisions: DecisionRaft,
        zanzibar: Arc<ZanzibarDistribution>,
        peers: ClusterPeerTransport,
        names: LogicalNameResolver,
        system: AuthoritativeSystemAuthorization,
        tokens: JwtManager,
    ) -> Self {
        Self {
            local_node,
            store,
            decisions,
            zanzibar,
            peers,
            names,
            system,
            tokens,
        }
    }

    pub(crate) async fn put_schema(
        &self,
        request: Request<api::PutSchemaRequest>,
    ) -> Result<api::PutSchemaResponse, Status> {
        let (caller, bearer, routed, request) = self.request_parts(request)?;
        require_public_storage_tenant(&caller)?;
        let tenant_id = self.tenant_id(&caller).await?;
        if let Some(target) = self.route_target(tenant_id, routed)? {
            return self
                .peers
                .route_authz_put_schema(
                    target.node,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    ROUTE_TIMEOUT,
                )
                .await;
        }
        self.authorize_tenant(&caller, StorageTenantPermission::ManageAuthz)
            .await?;
        let schema_id = SchemaId::parse(request.schema_id).map_err(authz_store_status)?;
        let schema = schema_from_api(
            request.namespaces,
            self.zanzibar.repository().limits().evaluator,
        )?;
        let result = self
            .zanzibar
            .publish_schema_journaled(
                tenant_id,
                &self.store,
                PublishSchemaRequest {
                    storage_tenant: caller.storage_tenant().clone(),
                    schema_id,
                    schema,
                    expected_revision: None,
                },
            )
            .await?;
        Ok(api::PutSchemaResponse {
            schema_ref: Some(schema_ref_to_api(&result.result.schema_ref)),
            revision: result.result.authz_revision.0,
            replayed: result.result.replayed,
        })
    }

    pub(crate) async fn bind_schema(
        &self,
        request: Request<api::BindSchemaRequest>,
    ) -> Result<api::BindSchemaResponse, Status> {
        let (caller, bearer, routed, request) = self.request_parts(request)?;
        let tenant_id = self.tenant_id(&caller).await?;
        if let Some(target) = self.route_target(tenant_id, routed)? {
            return self
                .peers
                .route_authz_bind_schema(
                    target.node,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    ROUTE_TIMEOUT,
                )
                .await;
        }
        let scope = public_scope_from_api(request.scope, caller.storage_tenant().as_str())?;
        let schema_ref = schema_ref_from_api(request.schema_ref)?;
        if self
            .zanzibar
            .repository()
            .get_binding(&scope)
            .map_err(authz_store_status)?
            .is_none()
        {
            let check = storage_tenant_authorization_check(
                caller.subject(),
                caller.storage_tenant().as_str(),
                StorageTenantPermission::ManageAuthz,
            )
            .map_err(crate::authz_api::authz_status)?;
            let system = self.system.fresh_system_check(check).await?;
            if !system.allowed[0] {
                return Err(Status::permission_denied(
                    "first realm binding is not authorized",
                ));
            }
            require_single_node_first_bind(self.placement()?.active_node_ids().len())?;
            let repository = self.zanzibar.repository().clone();
            let principal = caller.subject().clone();
            let bound = super::run_authz(move || {
                repository
                    .bind_schema_with_protected_owner(
                        BindSchemaRequest {
                            scope,
                            schema_ref,
                            expected_generation: request.expected_binding_generation,
                            expected_revision: None,
                        },
                        ProtectedRealmOwnership {
                            principal,
                            expected_revision: system.revision,
                            expected_binding_generation: system.binding_generation,
                        },
                    )
                    .map(|result| result.realm)
                    .map_err(super::AuthzServiceError::from)
            })
            .await?;
            return Ok(api::BindSchemaResponse {
                binding: Some(binding_to_api(&bound.binding)),
                revision: bound.binding.authz_revision.0,
            });
        }
        self.authorize_realm(&caller, &scope, RealmPermission::BindSchema)
            .await?;
        let coordinated = self
            .zanzibar
            .bind_schema_journaled(
                tenant_id,
                &self.store,
                BindSchemaRequest {
                    scope,
                    schema_ref,
                    expected_generation: request.expected_binding_generation,
                    expected_revision: None,
                },
            )
            .await?;
        let CoordinatedAuthzRealmResult::Bound(bound) = coordinated.result else {
            return Err(Status::internal("schema binding returned tuple result"));
        };
        Ok(api::BindSchemaResponse {
            binding: Some(binding_to_api(&bound.binding)),
            revision: bound.binding.authz_revision.0,
        })
    }

    pub(crate) async fn get_binding(
        &self,
        request: Request<api::GetBindingRequest>,
    ) -> Result<api::GetBindingResponse, Status> {
        let (caller, bearer, routed, request) = self.request_parts(request)?;
        let tenant_id = self.tenant_id(&caller).await?;
        if let Some(target) = self.route_target(tenant_id, routed)? {
            return self
                .peers
                .route_authz_get_binding(
                    target.node,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    ROUTE_TIMEOUT,
                )
                .await;
        }
        let scope = public_scope_from_api(request.scope, caller.storage_tenant().as_str())?;
        self.authorize_realm(&caller, &scope, RealmPermission::List)
            .await?;
        self.zanzibar.reconcile_realm(tenant_id, &scope).await?;
        let binding = self
            .zanzibar
            .repository()
            .get_binding(&scope)
            .map_err(authz_store_status)?
            .ok_or_else(|| Status::not_found("authorization realm has no schema binding"))?;
        Ok(api::GetBindingResponse {
            binding: Some(binding_to_api(&binding)),
        })
    }

    pub(crate) async fn get_schema(
        &self,
        request: Request<api::GetSchemaRequest>,
    ) -> Result<api::GetSchemaResponse, Status> {
        let (caller, bearer, routed, request) = self.request_parts(request)?;
        require_public_storage_tenant(&caller)?;
        let tenant_id = self.tenant_id(&caller).await?;
        if let Some(target) = self.route_target(tenant_id, routed)? {
            return self
                .peers
                .route_authz_get_schema(
                    target.node,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    ROUTE_TIMEOUT,
                )
                .await;
        }
        self.authorize_tenant(&caller, StorageTenantPermission::Read)
            .await?;
        let schema_ref = schema_ref_from_api(request.schema_ref)?;
        let schema = self
            .zanzibar
            .repository()
            .get_schema(caller.storage_tenant(), &schema_ref)
            .map_err(authz_store_status)?
            .ok_or_else(|| Status::not_found("authorization schema was not found"))?;
        Ok(api::GetSchemaResponse {
            schema_ref: Some(schema_ref_to_api(&schema_ref)),
            namespaces: schema_to_api(&schema),
        })
    }

    pub(crate) async fn mutate_tuples(
        &self,
        request: Request<api::MutateTuplesRequest>,
    ) -> Result<api::MutateTuplesResponse, Status> {
        let (caller, bearer, routed, request) = self.request_parts(request)?;
        let tenant_id = self.tenant_id(&caller).await?;
        if let Some(target) = self.route_target(tenant_id, routed)? {
            return self
                .peers
                .route_authz_mutate_tuples(
                    target.node,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    ROUTE_TIMEOUT,
                )
                .await;
        }
        let scope = public_scope_from_api(request.scope, caller.storage_tenant().as_str())?;
        self.authorize_realm(&caller, &scope, RealmPermission::WriteTuples)
            .await?;
        self.zanzibar.reconcile_realm(tenant_id, &scope).await?;
        let binding = self
            .zanzibar
            .repository()
            .get_binding(&scope)
            .map_err(authz_store_status)?
            .ok_or_else(|| Status::failed_precondition("authorization realm has no binding"))?;
        let mutations = request
            .mutations
            .into_iter()
            .map(tuple_mutation_from_api)
            .map(|mutation| match mutation? {
                DomainTupleMutation::Add(tuple) => Ok(TupleMutation {
                    kind: TupleMutationKind::Add,
                    tuple,
                }),
                DomainTupleMutation::Remove(tuple) => Ok(TupleMutation {
                    kind: TupleMutationKind::Remove,
                    tuple,
                }),
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let coordinated = self
            .zanzibar
            .mutate_tuples_journaled(
                tenant_id,
                &self.store,
                TupleBatchRequest {
                    scope,
                    principal: caller.subject().clone(),
                    expected_revision: request.expected_revision.map(AuthzRevision),
                    expected_binding_generation: binding.generation,
                    operation_id: Some(request.operation_id),
                    mutations,
                },
            )
            .await?;
        let CoordinatedAuthzRealmResult::Tuples(receipt) = coordinated.result else {
            return Err(Status::internal("tuple mutation returned binding result"));
        };
        Ok(api::MutateTuplesResponse {
            revision: receipt.authz_revision.0,
            replayed: receipt.replayed,
            replay_guarantee_expires_at: Some(
                (UNIX_EPOCH
                    + Duration::from_millis(receipt.replay_guarantee_expires_at_unix_millis))
                .into(),
            ),
        })
    }

    pub(crate) async fn read_tuples(
        &self,
        request: Request<api::ReadTuplesRequest>,
    ) -> Result<api::ReadTuplesResponse, Status> {
        let (caller, bearer, routed, request) = self.request_parts(request)?;
        let tenant_id = self.tenant_id(&caller).await?;
        if let Some(target) = self.route_target(tenant_id, routed)? {
            return self
                .peers
                .route_authz_read_tuples(
                    target.node,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    ROUTE_TIMEOUT,
                )
                .await;
        }
        let scope = public_scope_from_api(request.scope, caller.storage_tenant().as_str())?;
        self.authorize_realm(&caller, &scope, RealmPermission::List)
            .await?;
        let filter = tuple_filter_from_api(request.filter)?;
        let consistency = consistency_from_api(request.consistency)?;
        let page_size = normalize_page_size(request.page_size)?;
        let fingerprint = page_fingerprint(&caller, &scope, &filter, page_size)?;
        let page_token = decode_page_token(&request.page_token)?;
        if page_token
            .as_ref()
            .is_some_and(|token| token.fingerprint != fingerprint)
        {
            return Err(Status::invalid_argument(
                "authorization page token does not match this request",
            ));
        }
        self.zanzibar.reconcile_realm(tenant_id, &scope).await?;
        let snapshot = self
            .zanzibar
            .repository()
            .realm_snapshot(&scope, consistency)
            .map_err(authz_store_status)?;
        Authorization::new(
            scope.realm.clone(),
            snapshot.schema,
            snapshot.tuples.iter().cloned(),
            self.zanzibar.repository().limits().evaluator,
        )
        .map_err(crate::authz_api::authz_status)?;
        let offset = match page_token {
            Some(token) if token.revision != snapshot.revision => {
                return Err(Status::failed_precondition(
                    "AUTHZ_REVISION_EXPIRED: authorization tuple page revision is no longer current",
                ));
            }
            Some(token) => token.offset,
            None => 0,
        };
        let tuples = snapshot
            .tuples
            .into_iter()
            .filter(|tuple| tuple_matches_filter(tuple, &filter))
            .collect::<Vec<_>>();
        if offset > tuples.len() {
            return Err(Status::invalid_argument(
                "authorization page token position is invalid",
            ));
        }
        let end = offset.saturating_add(page_size).min(tuples.len());
        let next_page_token = if end < tuples.len() {
            encode_page_token(&PageToken {
                revision: snapshot.revision,
                offset: end,
                fingerprint,
            })?
        } else {
            String::new()
        };
        Ok(api::ReadTuplesResponse {
            tuples: tuples[offset..end].iter().map(tuple_to_api).collect(),
            revision: snapshot.revision.0,
            next_page_token,
        })
    }

    pub(crate) async fn check_permission(
        &self,
        request: Request<api::CheckPermissionRequest>,
    ) -> Result<api::CheckPermissionResponse, Status> {
        let (caller, bearer, routed, request) = self.request_parts(request)?;
        let tenant_id = self.tenant_id(&caller).await?;
        if let Some(target) = self.route_target(tenant_id, routed)? {
            return self
                .peers
                .route_authz_check_permission(
                    target.node,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    ROUTE_TIMEOUT,
                )
                .await;
        }
        let scope = public_scope_from_api(request.scope, caller.storage_tenant().as_str())?;
        self.authorize_realm(&caller, &scope, RealmPermission::Check)
            .await?;
        let check = check_from_api(
            request
                .check
                .ok_or_else(|| Status::invalid_argument("permission check is required"))?,
        )?;
        let (allowed, revision) = self
            .zanzibar
            .fresh_check(
                tenant_id,
                scope,
                consistency_from_api(request.consistency)?,
                check,
            )
            .await?;
        Ok(api::CheckPermissionResponse {
            allowed,
            revision: revision.0,
        })
    }

    pub(crate) async fn check_permissions(
        &self,
        request: Request<api::CheckPermissionsRequest>,
    ) -> Result<api::CheckPermissionsResponse, Status> {
        let (caller, bearer, routed, request) = self.request_parts(request)?;
        let tenant_id = self.tenant_id(&caller).await?;
        if let Some(target) = self.route_target(tenant_id, routed)? {
            return self
                .peers
                .route_authz_check_permissions(
                    target.node,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    ROUTE_TIMEOUT,
                )
                .await;
        }
        if request.checks.is_empty() || request.checks.len() > MAX_CHECKS {
            return Err(Status::resource_exhausted(
                "checks must contain 1..=1000 items",
            ));
        }
        let scope = public_scope_from_api(request.scope, caller.storage_tenant().as_str())?;
        self.authorize_realm(&caller, &scope, RealmPermission::Check)
            .await?;
        let checks = request
            .checks
            .into_iter()
            .map(check_from_api)
            .collect::<Result<Vec<_>, _>>()?;
        let (allowed, revision, _) = self
            .zanzibar
            .fresh_checks_with_generation(
                tenant_id,
                scope,
                consistency_from_api(request.consistency)?,
                checks,
            )
            .await?;
        Ok(api::CheckPermissionsResponse {
            results: allowed
                .into_iter()
                .map(|allowed| api::PermissionResult { allowed })
                .collect(),
            revision: revision.0,
        })
    }

    pub(crate) fn verify_routed_caller(&self, bearer: &str) -> Result<Caller, Status> {
        self.tokens
            .verify(bearer)
            .map_err(|_| Status::unauthenticated("the bearer token is invalid or expired"))
    }

    async fn tenant_id(&self, caller: &Caller) -> Result<u64, Status> {
        self.names
            .resolve_tenant_id(caller.storage_tenant())
            .await?
            .ok_or_else(|| Status::not_found("authenticated storage tenant does not exist"))
    }

    async fn authorize_tenant(
        &self,
        caller: &Caller,
        permission: StorageTenantPermission,
    ) -> Result<(), Status> {
        let check = storage_tenant_authorization_check(
            caller.subject(),
            caller.storage_tenant().as_str(),
            permission,
        )
        .map_err(crate::authz_api::authz_status)?;
        self.require_system_allowed(check, "storage tenant operation is not authorized")
            .await
    }

    async fn authorize_realm(
        &self,
        caller: &Caller,
        scope: &AuthzScope,
        permission: RealmPermission,
    ) -> Result<(), Status> {
        let check = realm_authorization_check(
            caller.subject(),
            caller.storage_tenant().as_str(),
            &scope.realm,
            permission,
        )
        .map_err(crate::authz_api::authz_status)?;
        self.require_system_allowed(check, "authorization realm operation is not authorized")
            .await
    }

    async fn require_system_allowed(
        &self,
        check: AuthorizationCheck,
        message: &'static str,
    ) -> Result<(), Status> {
        if self.system.fresh_system_check(check).await?.allowed[0] {
            Ok(())
        } else {
            Err(Status::permission_denied(message))
        }
    }

    fn request_parts<T>(
        &self,
        request: Request<T>,
    ) -> Result<(Caller, OriginalBearer, bool, T), Status> {
        let caller = caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let routed = request
            .extensions()
            .get::<RoutedAuthzDestination>()
            .is_some();
        Ok((caller, bearer, routed, request.into_inner()))
    }

    fn route_target(&self, stable_tenant_id: u64, routed: bool) -> Result<Option<Target>, Status> {
        let placement = self.placement()?;
        let group = MutableRecordReplicaGroup::select(
            PlacementKind::ZanzibarRealm,
            placement.cluster_id(),
            &stable_tenant_id.to_be_bytes(),
            placement.placement_nodes(),
        )
        .ok_or_else(|| Status::unavailable("cluster has no Zanzibar replica"))?;
        let coordinator = group.coordinator();
        if coordinator == self.local_node {
            return Ok(None);
        }
        if routed {
            return Err(Status::failed_precondition(
                "routed authorization request did not reach its current coordinator",
            ));
        }
        let address = placement.address(coordinator).ok_or_else(|| {
            Status::unavailable("Zanzibar coordinator has no current peer address")
        })?;
        Ok(Some(Target {
            node: coordinator,
            address: address.0.clone(),
        }))
    }

    fn placement(&self) -> Result<ClusterPlacement, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}

struct Target {
    node: NodeId,
    address: String,
}

fn require_single_node_first_bind(active_nodes: usize) -> Result<(), Status> {
    if active_nodes == 1 {
        Ok(())
    } else {
        Err(Status::unavailable(
            "first realm binding is unavailable in a multi-node cluster",
        ))
    }
}

#[derive(Clone)]
struct RoutedDistributedAuthz {
    service: super::AuthzServiceImpl,
}

impl super::AuthzServiceImpl {
    pub(crate) fn routed_authz_handler(&self) -> Arc<dyn RoutedAuthzHandler> {
        Arc::new(RoutedDistributedAuthz {
            service: self.clone(),
        })
    }
}

impl RoutedDistributedAuthz {
    fn authenticated_request<T>(&self, call: RoutedCall<T>) -> Result<Request<T>, Status> {
        let distributed =
            self.service.distributed.as_ref().ok_or_else(|| {
                Status::unavailable("distributed authorization service is not ready")
            })?;
        let bearer = call.bearer().to_owned();
        let caller = distributed
            .tokens
            .verify(&bearer)
            .map_err(|_| Status::unauthenticated("the bearer token is invalid or expired"))?;
        let authorization = format!("Bearer {bearer}")
            .parse::<MetadataValue<_>>()
            .map_err(|_| Status::unauthenticated("the bearer token is malformed"))?;
        let mut request = Request::new(call.into_request());
        request
            .metadata_mut()
            .insert("authorization", authorization);
        request.extensions_mut().insert(caller);
        request.extensions_mut().insert(RoutedAuthzDestination);
        Ok(request)
    }
}

#[tonic::async_trait]
impl RoutedAuthzHandler for RoutedDistributedAuthz {
    async fn put_schema(
        &self,
        call: RoutedCall<api::PutSchemaRequest>,
    ) -> Result<api::PutSchemaResponse, Status> {
        Ok(
            PublicAuthzService::put_schema(&self.service, self.authenticated_request(call)?)
                .await?
                .into_inner(),
        )
    }

    async fn bind_schema(
        &self,
        call: RoutedCall<api::BindSchemaRequest>,
    ) -> Result<api::BindSchemaResponse, Status> {
        Ok(
            PublicAuthzService::bind_schema(&self.service, self.authenticated_request(call)?)
                .await?
                .into_inner(),
        )
    }

    async fn get_binding(
        &self,
        call: RoutedCall<api::GetBindingRequest>,
    ) -> Result<api::GetBindingResponse, Status> {
        Ok(
            PublicAuthzService::get_binding(&self.service, self.authenticated_request(call)?)
                .await?
                .into_inner(),
        )
    }

    async fn get_schema(
        &self,
        call: RoutedCall<api::GetSchemaRequest>,
    ) -> Result<api::GetSchemaResponse, Status> {
        Ok(
            PublicAuthzService::get_schema(&self.service, self.authenticated_request(call)?)
                .await?
                .into_inner(),
        )
    }

    async fn mutate_tuples(
        &self,
        call: RoutedCall<api::MutateTuplesRequest>,
    ) -> Result<api::MutateTuplesResponse, Status> {
        Ok(
            PublicAuthzService::mutate_tuples(&self.service, self.authenticated_request(call)?)
                .await?
                .into_inner(),
        )
    }

    async fn read_tuples(
        &self,
        call: RoutedCall<api::ReadTuplesRequest>,
    ) -> Result<api::ReadTuplesResponse, Status> {
        Ok(
            PublicAuthzService::read_tuples(&self.service, self.authenticated_request(call)?)
                .await?
                .into_inner(),
        )
    }

    async fn check_permission(
        &self,
        call: RoutedCall<api::CheckPermissionRequest>,
    ) -> Result<api::CheckPermissionResponse, Status> {
        Ok(
            PublicAuthzService::check_permission(&self.service, self.authenticated_request(call)?)
                .await?
                .into_inner(),
        )
    }

    async fn check_permissions(
        &self,
        call: RoutedCall<api::CheckPermissionsRequest>,
    ) -> Result<api::CheckPermissionsResponse, Status> {
        Ok(
            PublicAuthzService::check_permissions(&self.service, self.authenticated_request(call)?)
                .await?
                .into_inner(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_bind_keeps_the_atomic_local_path_on_one_node() {
        require_single_node_first_bind(1).unwrap();
    }

    #[test]
    fn first_bind_fails_closed_before_cross_zanzibar_multi_node_work() {
        let status = require_single_node_first_bind(3).unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }
}
