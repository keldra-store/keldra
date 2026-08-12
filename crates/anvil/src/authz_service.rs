//! Public authorization RPCs backed by the same repository Anvil consults.

use anvil_api::v1 as api;
use anvil_api::v1::authz_service_server::AuthzService;
use anvil_authz::{Authorization, ObjectRef, Tuple, TupleSubject};
use anvil_store::{
    AuthzRepository, AuthzRevision, AuthzStoreError, BindSchemaRequest, ProtectedRealmOwnership,
    PublishSchemaRequest, SchemaId, StorageTenantId, TupleBatchRequest, TupleMutation,
    TupleMutationKind,
};
use serde::{Deserialize, Serialize};
use std::time::{Duration, UNIX_EPOCH};
use tonic::{Request, Response, Status};

use crate::authentication::Caller;
use crate::authorization::{RealmPermission, StorageTenantPermission, SystemAuthorizer};
use crate::authz_api::{
    DomainObjectFilter, DomainTupleFilter, DomainTupleMutation, check_from_api,
    consistency_from_api, public_scope_from_api, schema_from_api, schema_ref_from_api,
    schema_ref_to_api, schema_to_api, tuple_filter_from_api, tuple_mutation_from_api, tuple_to_api,
};

mod distributed;
pub(crate) use distributed::DistributedAuthzService;

const DEFAULT_PAGE_SIZE: usize = 100;
const MAX_PAGE_SIZE: usize = 1_000;
const MAX_PAGE_TOKEN_BYTES: usize = 128 * 1024;
const MAX_CHECKS: usize = 1_000;

#[derive(Clone)]
pub struct AuthzServiceImpl {
    repository: AuthzRepository,
    system_authorizer: SystemAuthorizer,
    distributed: Option<std::sync::Arc<DistributedAuthzService>>,
}

impl AuthzServiceImpl {
    pub(crate) fn new(repository: AuthzRepository) -> Self {
        Self {
            system_authorizer: SystemAuthorizer::new(repository.clone()),
            repository,
            distributed: None,
        }
    }

    pub(crate) fn with_distributed(mut self, distributed: DistributedAuthzService) -> Self {
        self.distributed = Some(std::sync::Arc::new(distributed));
        self
    }
}

#[tonic::async_trait]
impl AuthzService for AuthzServiceImpl {
    async fn put_schema(
        &self,
        request: Request<api::PutSchemaRequest>,
    ) -> Result<Response<api::PutSchemaResponse>, Status> {
        if let Some(distributed) = self.distributed.as_ref() {
            return distributed.put_schema(request).await.map(Response::new);
        }
        let caller = caller(&request)?;
        require_public_storage_tenant(&caller)?;
        let request = request.into_inner();
        let schema_id = SchemaId::parse(request.schema_id).map_err(authz_store_status)?;
        let limits = self.repository.limits().evaluator;
        let schema = schema_from_api(request.namespaces, limits)?;
        let repository = self.repository.clone();
        let system_authorizer = self.system_authorizer.clone();
        let result = run_authz(move || {
            let system = system_authorizer.load()?;
            require_allowed(
                system.allows_storage_tenant(
                    caller.subject(),
                    caller.storage_tenant().as_str(),
                    StorageTenantPermission::ManageAuthz,
                )?,
                "schema publication is not authorized",
            )?;
            repository
                .publish_schema(PublishSchemaRequest {
                    storage_tenant: caller.storage_tenant().clone(),
                    schema_id,
                    schema,
                    expected_revision: None,
                })
                .map_err(AuthzServiceError::from)
        })
        .await?;
        Ok(Response::new(api::PutSchemaResponse {
            schema_ref: Some(schema_ref_to_api(&result.schema_ref)),
            revision: result.authz_revision.0,
            replayed: result.replayed,
        }))
    }

    async fn bind_schema(
        &self,
        request: Request<api::BindSchemaRequest>,
    ) -> Result<Response<api::BindSchemaResponse>, Status> {
        if let Some(distributed) = self.distributed.as_ref() {
            return distributed.bind_schema(request).await.map(Response::new);
        }
        let caller = caller(&request)?;
        let request = request.into_inner();
        let scope = public_scope_from_api(request.scope, caller.storage_tenant().as_str())?;
        let schema_ref = schema_ref_from_api(request.schema_ref)?;
        let expected_generation = request.expected_binding_generation;
        let repository = self.repository.clone();
        let system_authorizer = self.system_authorizer.clone();
        let bound = run_authz(move || {
            let system = system_authorizer.load()?;
            let existing = repository.get_binding(&scope)?;
            if existing.is_none() {
                require_allowed(
                    system.allows_storage_tenant(
                        caller.subject(),
                        caller.storage_tenant().as_str(),
                        StorageTenantPermission::ManageAuthz,
                    )?,
                    "first realm binding is not authorized",
                )?;
                let result = repository.bind_schema_with_protected_owner(
                    BindSchemaRequest {
                        scope,
                        schema_ref,
                        expected_generation,
                        expected_revision: None,
                    },
                    ProtectedRealmOwnership {
                        principal: caller.subject().clone(),
                        expected_revision: system.revision,
                        expected_binding_generation: system.binding_generation,
                    },
                )?;
                Ok(result.realm)
            } else {
                require_allowed(
                    system.allows_realm(
                        caller.subject(),
                        caller.storage_tenant().as_str(),
                        &scope.realm,
                        RealmPermission::BindSchema,
                    )?,
                    "realm schema binding is not authorized",
                )?;
                repository
                    .bind_schema(BindSchemaRequest {
                        scope,
                        schema_ref,
                        expected_generation,
                        expected_revision: None,
                    })
                    .map_err(AuthzServiceError::from)
            }
        })
        .await?;
        Ok(Response::new(api::BindSchemaResponse {
            binding: Some(binding_to_api(&bound.binding)),
            revision: bound.binding.authz_revision.0,
        }))
    }

    async fn get_binding(
        &self,
        request: Request<api::GetBindingRequest>,
    ) -> Result<Response<api::GetBindingResponse>, Status> {
        if let Some(distributed) = self.distributed.as_ref() {
            return distributed.get_binding(request).await.map(Response::new);
        }
        let caller = caller(&request)?;
        let scope =
            public_scope_from_api(request.into_inner().scope, caller.storage_tenant().as_str())?;
        let repository = self.repository.clone();
        let system_authorizer = self.system_authorizer.clone();
        let binding = run_authz(move || {
            let system = system_authorizer.load()?;
            require_allowed(
                system.allows_realm(
                    caller.subject(),
                    caller.storage_tenant().as_str(),
                    &scope.realm,
                    RealmPermission::List,
                )?,
                "realm binding read is not authorized",
            )?;
            Ok(repository
                .get_binding(&scope)?
                .ok_or_else(|| Status::not_found("authorization realm has no schema binding"))?)
        })
        .await?;
        Ok(Response::new(api::GetBindingResponse {
            binding: Some(binding_to_api(&binding)),
        }))
    }

    async fn get_schema(
        &self,
        request: Request<api::GetSchemaRequest>,
    ) -> Result<Response<api::GetSchemaResponse>, Status> {
        if let Some(distributed) = self.distributed.as_ref() {
            return distributed.get_schema(request).await.map(Response::new);
        }
        let caller = caller(&request)?;
        require_public_storage_tenant(&caller)?;
        let schema_ref = schema_ref_from_api(request.into_inner().schema_ref)?;
        let repository = self.repository.clone();
        let system_authorizer = self.system_authorizer.clone();
        let response_ref = schema_ref.clone();
        let schema = run_authz(move || {
            let system = system_authorizer.load()?;
            require_allowed(
                system.allows_storage_tenant(
                    caller.subject(),
                    caller.storage_tenant().as_str(),
                    StorageTenantPermission::Read,
                )?,
                "schema read is not authorized",
            )?;
            Ok(repository
                .get_schema(caller.storage_tenant(), &schema_ref)?
                .ok_or_else(|| Status::not_found("authorization schema was not found"))?)
        })
        .await?;
        Ok(Response::new(api::GetSchemaResponse {
            schema_ref: Some(schema_ref_to_api(&response_ref)),
            namespaces: schema_to_api(&schema),
        }))
    }

    async fn mutate_tuples(
        &self,
        request: Request<api::MutateTuplesRequest>,
    ) -> Result<Response<api::MutateTuplesResponse>, Status> {
        if let Some(distributed) = self.distributed.as_ref() {
            return distributed.mutate_tuples(request).await.map(Response::new);
        }
        let caller = caller(&request)?;
        let request = request.into_inner();
        let scope = public_scope_from_api(request.scope, caller.storage_tenant().as_str())?;
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
        let repository = self.repository.clone();
        let system_authorizer = self.system_authorizer.clone();
        let receipt = run_authz(move || {
            let system = system_authorizer.load()?;
            require_allowed(
                system.allows_realm(
                    caller.subject(),
                    caller.storage_tenant().as_str(),
                    &scope.realm,
                    RealmPermission::WriteTuples,
                )?,
                "tuple mutation is not authorized",
            )?;
            // The server observes the binding immediately before mutation and
            // passes that generation into the repository CAS. A concurrent
            // rebind therefore rejects the complete tuple batch.
            let binding = repository.get_binding(&scope)?.ok_or_else(|| {
                Status::failed_precondition("authorization realm has no schema binding")
            })?;
            repository
                .mutate_tuples(TupleBatchRequest {
                    scope,
                    principal: caller.subject().clone(),
                    expected_revision: request.expected_revision.map(AuthzRevision),
                    expected_binding_generation: binding.generation,
                    operation_id: Some(request.operation_id),
                    mutations,
                })
                .map_err(AuthzServiceError::from)
        })
        .await?;
        Ok(Response::new(api::MutateTuplesResponse {
            revision: receipt.authz_revision.0,
            replayed: receipt.replayed,
            replay_guarantee_expires_at: Some(
                (UNIX_EPOCH
                    + Duration::from_millis(receipt.replay_guarantee_expires_at_unix_millis))
                .into(),
            ),
        }))
    }

    async fn read_tuples(
        &self,
        request: Request<api::ReadTuplesRequest>,
    ) -> Result<Response<api::ReadTuplesResponse>, Status> {
        if let Some(distributed) = self.distributed.as_ref() {
            return distributed.read_tuples(request).await.map(Response::new);
        }
        let caller = caller(&request)?;
        let request = request.into_inner();
        let scope = public_scope_from_api(request.scope, caller.storage_tenant().as_str())?;
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
        let repository = self.repository.clone();
        let system_authorizer = self.system_authorizer.clone();
        let (tuples, revision) = run_authz(move || {
            let system = system_authorizer.load()?;
            require_allowed(
                system.allows_realm(
                    caller.subject(),
                    caller.storage_tenant().as_str(),
                    &scope.realm,
                    RealmPermission::List,
                )?,
                "tuple read is not authorized",
            )?;
            let snapshot = repository.realm_snapshot(&scope, consistency)?;
            Authorization::new(
                scope.realm.clone(),
                snapshot.schema.clone(),
                snapshot.tuples.iter().cloned(),
                repository.limits().evaluator,
            )?;
            Ok((snapshot.tuples, snapshot.revision))
        })
        .await?;
        let offset = match page_token {
            Some(token) if token.revision != revision => {
                return Err(Status::failed_precondition(
                    "AUTHZ_REVISION_EXPIRED: authorization tuple page revision is no longer current",
                ));
            }
            Some(token) => token.offset,
            None => 0,
        };
        let filtered = tuples
            .into_iter()
            .filter(|tuple| tuple_matches_filter(tuple, &filter))
            .collect::<Vec<_>>();
        if offset > filtered.len() {
            return Err(Status::invalid_argument(
                "authorization page token position is invalid",
            ));
        }
        let end = offset.saturating_add(page_size).min(filtered.len());
        let next_page_token = if end < filtered.len() {
            encode_page_token(&PageToken {
                revision,
                offset: end,
                fingerprint,
            })?
        } else {
            String::new()
        };
        Ok(Response::new(api::ReadTuplesResponse {
            tuples: filtered[offset..end].iter().map(tuple_to_api).collect(),
            revision: revision.0,
            next_page_token,
        }))
    }

    async fn check_permission(
        &self,
        request: Request<api::CheckPermissionRequest>,
    ) -> Result<Response<api::CheckPermissionResponse>, Status> {
        if let Some(distributed) = self.distributed.as_ref() {
            return distributed
                .check_permission(request)
                .await
                .map(Response::new);
        }
        let caller = caller(&request)?;
        let request = request.into_inner();
        let scope = public_scope_from_api(request.scope, caller.storage_tenant().as_str())?;
        let check = check_from_api(
            request
                .check
                .ok_or_else(|| Status::invalid_argument("permission check is required"))?,
        )?;
        let consistency = consistency_from_api(request.consistency)?;
        let repository = self.repository.clone();
        let system_authorizer = self.system_authorizer.clone();
        let (allowed, revision) = run_authz(move || {
            let system = system_authorizer.load()?;
            require_allowed(
                system.allows_realm(
                    caller.subject(),
                    caller.storage_tenant().as_str(),
                    &scope.realm,
                    RealmPermission::Check,
                )?,
                "permission evaluation is not authorized",
            )?;
            repository
                .check(&scope, consistency, &check)
                .map_err(AuthzServiceError::from)
        })
        .await?;
        Ok(Response::new(api::CheckPermissionResponse {
            allowed,
            revision: revision.0,
        }))
    }

    async fn check_permissions(
        &self,
        request: Request<api::CheckPermissionsRequest>,
    ) -> Result<Response<api::CheckPermissionsResponse>, Status> {
        if let Some(distributed) = self.distributed.as_ref() {
            return distributed
                .check_permissions(request)
                .await
                .map(Response::new);
        }
        let caller = caller(&request)?;
        let request = request.into_inner();
        if request.checks.is_empty() {
            return Err(Status::invalid_argument("checks must not be empty"));
        }
        if request.checks.len() > MAX_CHECKS {
            return Err(Status::resource_exhausted(format!(
                "checks exceeds the {MAX_CHECKS} item limit"
            )));
        }
        let scope = public_scope_from_api(request.scope, caller.storage_tenant().as_str())?;
        let checks = request
            .checks
            .into_iter()
            .map(check_from_api)
            .collect::<Result<Vec<_>, _>>()?;
        let consistency = consistency_from_api(request.consistency)?;
        let repository = self.repository.clone();
        let system_authorizer = self.system_authorizer.clone();
        let result = run_authz(move || {
            let system = system_authorizer.load()?;
            require_allowed(
                system.allows_realm(
                    caller.subject(),
                    caller.storage_tenant().as_str(),
                    &scope.realm,
                    RealmPermission::Check,
                )?,
                "permission evaluation is not authorized",
            )?;
            repository
                .batch_check(&scope, consistency, &checks)
                .map_err(AuthzServiceError::from)
        })
        .await?;
        Ok(Response::new(api::CheckPermissionsResponse {
            results: result
                .allowed
                .into_iter()
                .map(|allowed| api::PermissionResult { allowed })
                .collect(),
            revision: result.revision.0,
        }))
    }
}

pub(super) fn caller<T>(request: &Request<T>) -> Result<Caller, Status> {
    request
        .extensions()
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("authenticated caller identity is missing"))
}

pub(super) fn require_public_storage_tenant(caller: &Caller) -> Result<(), Status> {
    if caller.storage_tenant().is_system() {
        Err(Status::permission_denied(
            "the protected system authorization tenant is not public",
        ))
    } else {
        Ok(())
    }
}

async fn run_authz<T, F>(operation: F) -> Result<T, Status>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AuthzServiceError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| Status::internal(format!("authorization worker failed: {error}")))?
        .map_err(authz_service_status)
}

fn require_allowed(allowed: bool, message: &'static str) -> Result<(), AuthzServiceError> {
    if allowed {
        Ok(())
    } else {
        Err(AuthzServiceError::Denied(message))
    }
}

#[derive(Debug)]
enum AuthzServiceError {
    Denied(&'static str),
    Status(Status),
    Store(AuthzStoreError),
    Evaluation(anvil_authz::AuthorizationError),
}

impl From<Status> for AuthzServiceError {
    fn from(value: Status) -> Self {
        Self::Status(value)
    }
}

impl From<AuthzStoreError> for AuthzServiceError {
    fn from(value: AuthzStoreError) -> Self {
        Self::Store(value)
    }
}

impl From<anvil_authz::AuthorizationError> for AuthzServiceError {
    fn from(value: anvil_authz::AuthorizationError) -> Self {
        Self::Evaluation(value)
    }
}

fn authz_service_status(error: AuthzServiceError) -> Status {
    match error {
        AuthzServiceError::Denied(message) => Status::permission_denied(message),
        AuthzServiceError::Status(status) => status,
        AuthzServiceError::Store(error) => authz_store_status(error),
        AuthzServiceError::Evaluation(error) => crate::authz_api::authz_status(error),
    }
}

pub(super) fn binding_to_api(binding: &anvil_store::RealmBinding) -> api::SchemaBinding {
    api::SchemaBinding {
        scope: Some(crate::authz_api::scope_to_api(&binding.scope)),
        schema_ref: Some(schema_ref_to_api(&binding.schema_ref)),
        generation: binding.generation,
    }
}

pub(super) fn authz_store_status(error: AuthzStoreError) -> Status {
    match error {
        AuthzStoreError::InvalidInput(_) | AuthzStoreError::Authorization(_) => {
            Status::invalid_argument(error.to_string())
        }
        AuthzStoreError::MissingBinding(_, _) | AuthzStoreError::SchemaNotFound(_, _) => {
            Status::not_found(error.to_string())
        }
        AuthzStoreError::RevisionConflict { .. }
        | AuthzStoreError::BindingGenerationConflict { .. }
        | AuthzStoreError::OperationMismatch => Status::aborted(error.to_string()),
        AuthzStoreError::RevisionNotAvailable { .. } => {
            Status::failed_precondition(error.to_string())
        }
        AuthzStoreError::RevisionExpired { .. } => Status::failed_precondition(error.to_string()),
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

pub(super) fn normalize_page_size(value: u32) -> Result<usize, Status> {
    let value = if value == 0 {
        DEFAULT_PAGE_SIZE
    } else {
        value as usize
    };
    if value > MAX_PAGE_SIZE {
        return Err(Status::resource_exhausted(format!(
            "page size exceeds the {MAX_PAGE_SIZE} item limit"
        )));
    }
    Ok(value)
}

pub(super) fn tuple_matches_filter(tuple: &Tuple, filter: &DomainTupleFilter) -> bool {
    let object_matches = match filter.object.as_ref() {
        None => true,
        Some(DomainObjectFilter::Namespace(namespace)) => tuple.object.namespace == *namespace,
        Some(DomainObjectFilter::Exact(object)) => tuple.object == *object,
    };
    object_matches
        && filter
            .relation
            .as_ref()
            .is_none_or(|relation| tuple.relation == *relation)
        && filter
            .subject
            .as_ref()
            .is_none_or(|subject| tuple.subject == *subject)
}

#[derive(Debug, Serialize)]
struct PageFingerprint<'a> {
    storage_tenant: &'a StorageTenantId,
    realm: &'a anvil_authz::RealmId,
    caller: &'a ObjectRef,
    object: PageObjectFilter<'a>,
    relation: Option<&'a str>,
    subject: Option<&'a TupleSubject>,
    page_size: usize,
}

#[derive(Debug, Serialize)]
enum PageObjectFilter<'a> {
    Any,
    Namespace(&'a str),
    Exact(&'a ObjectRef),
}

pub(super) fn page_fingerprint(
    caller: &Caller,
    scope: &anvil_store::AuthzScope,
    filter: &DomainTupleFilter,
    page_size: usize,
) -> Result<[u8; 32], Status> {
    let object = match filter.object.as_ref() {
        None => PageObjectFilter::Any,
        Some(DomainObjectFilter::Namespace(namespace)) => PageObjectFilter::Namespace(namespace),
        Some(DomainObjectFilter::Exact(object)) => PageObjectFilter::Exact(object),
    };
    let canonical = serde_json::to_vec(&PageFingerprint {
        storage_tenant: &scope.storage_tenant,
        realm: &scope.realm,
        caller: caller.subject(),
        object,
        relation: filter.relation.as_deref(),
        subject: filter.subject.as_ref(),
        page_size,
    })
    .map_err(|error| Status::internal(format!("encode page token fingerprint: {error}")))?;
    Ok(*blake3::hash(&canonical).as_bytes())
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct PageToken {
    revision: AuthzRevision,
    offset: usize,
    fingerprint: [u8; 32],
}

pub(super) fn encode_page_token(value: &PageToken) -> Result<String, Status> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| Status::internal(format!("encode authorization page token: {error}")))?;
    Ok(hex::encode(bytes))
}

pub(super) fn decode_page_token(value: &str) -> Result<Option<PageToken>, Status> {
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() > MAX_PAGE_TOKEN_BYTES {
        return Err(Status::resource_exhausted(
            "authorization page token exceeds the server limit",
        ));
    }
    let bytes = hex::decode(value)
        .map_err(|_| Status::invalid_argument("authorization page token is malformed"))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| Status::invalid_argument("authorization page token is malformed"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authorization::storage_tenant_resource;
    use anvil_authz::{RealmId, TupleSubject};
    use anvil_store::{AuthzScope, Store, StoreOptions, SystemBootstrapRequest};

    fn test_caller() -> Caller {
        let manager =
            crate::authentication::JwtManager::new(b"0123456789abcdef0123456789abcdef").unwrap();
        let token = manager
            .mint(StorageTenantId::parse("acme").unwrap(), "alice")
            .unwrap();
        manager.verify(&token).unwrap()
    }

    fn request<T>(caller: &Caller, value: T) -> Request<T> {
        let mut request = Request::new(value);
        request.extensions_mut().insert(caller.clone());
        request
    }

    fn document_schema() -> Vec<api::NamespaceDefinition> {
        vec![api::NamespaceDefinition {
            name: "document".into(),
            relations: vec![
                api::RelationDefinition {
                    name: "viewer".into(),
                    kind: Some(api::relation_definition::Kind::Direct(
                        api::DirectRelation {
                            allowed_subjects: vec![api::SubjectSelector {
                                selector: Some(api::subject_selector::Selector::AnyObject(
                                    api::AnyObjectSelector {
                                        namespace: "app".into(),
                                    },
                                )),
                            }],
                        },
                    )),
                },
                api::RelationDefinition {
                    name: "view".into(),
                    kind: Some(api::relation_definition::Kind::Permission(
                        api::Permission {
                            rules: vec![api::PermissionRule {
                                rule: Some(api::permission_rule::Rule::Inherit(api::InheritRule {
                                    relation: "viewer".into(),
                                })),
                            }],
                        },
                    )),
                },
            ],
        }]
    }

    #[test]
    fn page_tokens_are_bound_to_caller_scope_filter_and_size() {
        let caller = test_caller();
        let scope = AuthzScope::new(
            StorageTenantId::parse("acme").unwrap(),
            RealmId::default_realm(),
        )
        .unwrap();
        let filter = DomainTupleFilter {
            object: Some(DomainObjectFilter::Namespace("document".into())),
            relation: Some("viewer".into()),
            subject: Some(TupleSubject::Object(
                ObjectRef::opaque("app", "bob").unwrap(),
            )),
        };
        let fingerprint = page_fingerprint(&caller, &scope, &filter, 100).unwrap();
        let encoded = encode_page_token(&PageToken {
            revision: AuthzRevision(9),
            offset: 100,
            fingerprint,
        })
        .unwrap();
        let decoded = decode_page_token(&encoded).unwrap().unwrap();
        assert_eq!(decoded.revision, AuthzRevision(9));
        assert_eq!(decoded.offset, 100);
        assert_eq!(decoded.fingerprint, fingerprint);
        assert_ne!(
            fingerprint,
            page_fingerprint(&caller, &scope, &filter, 101).unwrap()
        );
    }

    #[test]
    fn tuple_filter_matches_all_declared_dimensions() {
        let tuple = Tuple::new(
            ObjectRef::opaque("document", "one").unwrap(),
            "viewer",
            ObjectRef::opaque("app", "alice").unwrap(),
        );
        assert!(tuple_matches_filter(&tuple, &DomainTupleFilter::default()));
        assert!(tuple_matches_filter(
            &tuple,
            &DomainTupleFilter {
                object: Some(DomainObjectFilter::Namespace("document".into())),
                relation: Some("viewer".into()),
                subject: Some(TupleSubject::Object(
                    ObjectRef::opaque("app", "alice").unwrap()
                )),
            }
        ));
        assert!(!tuple_matches_filter(
            &tuple,
            &DomainTupleFilter {
                relation: Some("owner".into()),
                ..Default::default()
            }
        ));
    }

    #[tokio::test]
    async fn every_public_authz_rpc_requires_an_installed_caller() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 3))
            .await
            .unwrap();
        let service = AuthzServiceImpl::new(store.authz());

        let put = service
            .put_schema(Request::new(api::PutSchemaRequest::default()))
            .await
            .unwrap_err();
        let bind = service
            .bind_schema(Request::new(api::BindSchemaRequest::default()))
            .await
            .unwrap_err();
        let get_binding = service
            .get_binding(Request::new(api::GetBindingRequest::default()))
            .await
            .unwrap_err();
        let get_schema = service
            .get_schema(Request::new(api::GetSchemaRequest::default()))
            .await
            .unwrap_err();
        let mutate = service
            .mutate_tuples(Request::new(api::MutateTuplesRequest::default()))
            .await
            .unwrap_err();
        let read = service
            .read_tuples(Request::new(api::ReadTuplesRequest::default()))
            .await
            .unwrap_err();
        let check = service
            .check_permission(Request::new(api::CheckPermissionRequest::default()))
            .await
            .unwrap_err();
        let checks = service
            .check_permissions(Request::new(api::CheckPermissionsRequest::default()))
            .await
            .unwrap_err();

        for status in [
            put,
            bind,
            get_binding,
            get_schema,
            mutate,
            read,
            check,
            checks,
        ] {
            assert_eq!(status.code(), tonic::Code::Unauthenticated);
        }
    }

    #[tokio::test]
    async fn public_realm_lifecycle_uses_system_authority_and_one_shared_evaluator() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 3))
            .await
            .unwrap();
        store
            .bootstrap_system(SystemBootstrapRequest {
                app_id: "bootstrap-app".into(),
                client_id: "bootstrap-client".into(),
                client_secret: "bootstrap-secret-with-at-least-32-bytes".into(),
            })
            .unwrap();
        let repository = store.authz();
        let system = SystemAuthorizer::new(repository.clone()).load().unwrap();
        let caller = test_caller();
        repository
            .mutate_tuples(TupleBatchRequest {
                scope: AuthzScope::system(),
                principal: ObjectRef::opaque("app", "bootstrap-app").unwrap(),
                expected_revision: Some(system.revision),
                expected_binding_generation: system.binding_generation,
                operation_id: Some("seed-acme-owner".into()),
                mutations: vec![TupleMutation {
                    kind: TupleMutationKind::Add,
                    tuple: Tuple::new(
                        storage_tenant_resource("acme").unwrap(),
                        "owner",
                        caller.subject().clone(),
                    ),
                }],
            })
            .unwrap();
        let service = AuthzServiceImpl::new(repository.clone());

        let system_caller =
            Caller::from_authenticated_application(StorageTenantId::system(), "bootstrap-app")
                .unwrap();
        let protected_publication = service
            .put_schema(request(
                &system_caller,
                api::PutSchemaRequest {
                    schema_id: "forbidden".into(),
                    namespaces: document_schema(),
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(protected_publication.code(), tonic::Code::PermissionDenied);

        let publication = service
            .put_schema(request(
                &caller,
                api::PutSchemaRequest {
                    schema_id: "documents".into(),
                    namespaces: document_schema(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(publication.revision, 1);
        let scope = api::AuthzScope {
            storage_tenant: "acme".into(),
            realm: "default".into(),
        };
        let binding = service
            .bind_schema(request(
                &caller,
                api::BindSchemaRequest {
                    scope: Some(scope.clone()),
                    schema_ref: publication.schema_ref,
                    expected_binding_generation: Some(0),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(binding.revision, 2);
        assert_eq!(binding.binding.unwrap().generation, 1);

        let document = api::ObjectRef {
            namespace: "document".into(),
            id: Some(api::object_ref::Id::OpaqueId("osv-1".into())),
        };
        let bob = api::Subject {
            kind: Some(api::subject::Kind::Object(api::ObjectRef {
                namespace: "app".into(),
                id: Some(api::object_ref::Id::OpaqueId("bob".into())),
            })),
        };
        let mutation = service
            .mutate_tuples(request(
                &caller,
                api::MutateTuplesRequest {
                    scope: Some(scope.clone()),
                    operation_id: "grant-bob".into(),
                    expected_revision: Some(2),
                    mutations: vec![api::TupleMutation {
                        operation: Some(api::tuple_mutation::Operation::Add(api::RelationTuple {
                            object: Some(document.clone()),
                            relation: "viewer".into(),
                            subject: Some(bob.clone()),
                        })),
                    }],
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(mutation.revision, 3);

        let checked = service
            .check_permission(request(
                &caller,
                api::CheckPermissionRequest {
                    scope: Some(scope),
                    check: Some(api::PermissionCheck {
                        subject: Some(bob),
                        object: Some(document),
                        relation: "view".into(),
                    }),
                    consistency: Some(api::AuthzConsistency {
                        requirement: Some(api::authz_consistency::Requirement::AtLeast(
                            api::AtLeastRevision { revision: 3 },
                        )),
                    }),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(checked.allowed);
        assert_eq!(checked.revision, 3);

        let protected = service
            .get_binding(request(
                &caller,
                api::GetBindingRequest {
                    scope: Some(api::AuthzScope {
                        storage_tenant: anvil_store::SYSTEM_STORAGE_TENANT_ID.into(),
                        realm: anvil_authz::SYSTEM_REALM_ID.into(),
                    }),
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(protected.code(), tonic::Code::PermissionDenied);
    }
}
