//! Typed administration backed by the protected Zanzibar system realm.

use anvil_api::v1 as api;
use anvil_api::v1::administration_service_server::AdministrationService;
use anvil_store::{
    ApplicationCredentialRequest, ApplicationRoleTarget, AuthzStoreError, BucketApplicationRole,
    CreateBucketRequest, CredentialMutationReceipt, CredentialRepositoryError,
    ObjectVersioning as StoreObjectVersioning, ProvisionTenantRequest, SetApplicationRoleRequest,
    StorageTenantId, Store, SystemApplicationRole, TenantApplicationRole,
};
use tonic::{Request, Response, Status};

use crate::authentication::Caller;
use crate::authorization::{StorageTenantPermission, SystemAuthorizer};

#[derive(Clone, Debug)]
pub(crate) struct AdministrationServiceImpl {
    store: Store,
    system_authorizer: SystemAuthorizer,
}

impl AdministrationServiceImpl {
    pub(crate) fn new(store: Store) -> Self {
        Self {
            system_authorizer: SystemAuthorizer::new(store.authz()),
            store,
        }
    }
}

#[tonic::async_trait]
impl AdministrationService for AdministrationServiceImpl {
    async fn provision_tenant(
        &self,
        request: Request<api::ProvisionTenantRequest>,
    ) -> Result<Response<api::ProvisionTenantResponse>, Status> {
        let caller = caller(&request)?;
        let request = request.into_inner();
        let storage_tenant =
            StorageTenantId::parse(request.storage_tenant).map_err(authz_status)?;
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_allowed(
                system
                    .allows_manage_system(caller.subject())
                    .map_err(authz_evaluation_status)?,
                "tenant provisioning is not authorized",
            )?;
            store
                .provision_tenant(ProvisionTenantRequest {
                    storage_tenant,
                    owner_app_id: request.owner_app_id,
                    owner_client_id: request.owner_client_id,
                    owner_client_secret: request.owner_client_secret,
                    principal: caller.subject().clone(),
                    expected_authorization_revision: system.revision,
                    expected_binding_generation: system.binding_generation,
                })
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(api::ProvisionTenantResponse {
            credential: Some(credential_to_api(receipt.credential, receipt.replayed)),
            authorization_revision: receipt.authorization_revision.0,
            replayed: receipt.replayed,
        }))
    }

    async fn create_application(
        &self,
        request: Request<api::CreateApplicationRequest>,
    ) -> Result<Response<api::ApplicationCredential>, Status> {
        let caller = caller(&request)?;
        let request = request.into_inner();
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_manage_tenant_or_system(&system, &caller)?;
            store
                .create_application(
                    ApplicationCredentialRequest {
                        storage_tenant: caller.storage_tenant().clone(),
                        app_id: request.app_id,
                        client_id: request.client_id,
                        client_secret: request.client_secret,
                    },
                    system.revision,
                )
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(credential_to_api(
            receipt.credential,
            receipt.replayed,
        )))
    }

    async fn rotate_application_credential(
        &self,
        request: Request<api::RotateApplicationCredentialRequest>,
    ) -> Result<Response<api::ApplicationCredential>, Status> {
        let caller = caller(&request)?;
        let request = request.into_inner();
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_manage_tenant_or_system(&system, &caller)?;
            store
                .rotate_application_credential(
                    ApplicationCredentialRequest {
                        storage_tenant: caller.storage_tenant().clone(),
                        app_id: request.app_id,
                        client_id: request.client_id,
                        client_secret: request.client_secret,
                    },
                    system.revision,
                )
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(credential_to_api(
            receipt.credential,
            receipt.replayed,
        )))
    }

    async fn disable_application_credential(
        &self,
        request: Request<api::DisableApplicationCredentialRequest>,
    ) -> Result<Response<api::ApplicationCredentialState>, Status> {
        let caller = caller(&request)?;
        let request = request.into_inner();
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_manage_tenant_or_system(&system, &caller)?;
            store
                .disable_application_credential(
                    caller.storage_tenant().clone(),
                    request.app_id,
                    request.client_id,
                    system.revision,
                )
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(credential_state_to_api(receipt)))
    }

    async fn create_bucket(
        &self,
        request: Request<api::CreateBucketRequest>,
    ) -> Result<Response<api::CreateBucketResponse>, Status> {
        let caller = caller(&request)?;
        if caller.storage_tenant().is_system() {
            return Err(Status::invalid_argument(
                "buckets cannot be created in the protected system tenant",
            ));
        }
        let request = request.into_inner();
        let versioning = versioning_from_api(request.versioning)?;
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_allowed(
                system
                    .allows_storage_tenant(
                        caller.subject(),
                        caller.storage_tenant().as_str(),
                        StorageTenantPermission::ManageBuckets,
                    )
                    .map_err(authz_evaluation_status)?,
                "bucket creation is not authorized",
            )?;
            store
                .create_bucket(CreateBucketRequest {
                    storage_tenant: caller.storage_tenant().clone(),
                    bucket: request.bucket,
                    versioning,
                    owner: caller.subject().clone(),
                    principal: caller.subject().clone(),
                    expected_authorization_revision: system.revision,
                    expected_binding_generation: system.binding_generation,
                })
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(api::CreateBucketResponse {
            storage_tenant: receipt.storage_tenant.to_string(),
            bucket: receipt.bucket,
            authorization_revision: receipt.authorization_revision.0,
            replayed: receipt.replayed,
            versioning: versioning_to_api(receipt.versioning) as i32,
        }))
    }

    async fn set_bucket_versioning(
        &self,
        request: Request<api::SetBucketVersioningRequest>,
    ) -> Result<Response<api::SetBucketVersioningResponse>, Status> {
        let caller = caller(&request)?;
        if caller.storage_tenant().is_system() {
            return Err(Status::invalid_argument(
                "buckets cannot exist in the protected system tenant",
            ));
        }
        let request = request.into_inner();
        let requested = versioning_from_api(request.versioning)?;
        if requested != StoreObjectVersioning::Enabled {
            return Err(Status::invalid_argument(
                "SetBucketVersioning accepts only ENABLED; bucket versioning cannot be disabled",
            ));
        }
        let storage_tenant = caller.storage_tenant().clone();
        let bucket = request.bucket;
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let response_tenant = storage_tenant.clone();
        let response_bucket = bucket.clone();
        let authorization_tenant = storage_tenant.clone();
        let authorization_bucket = bucket.clone();
        run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_allowed(
                system
                    .allows_bucket_policy(
                        caller.subject(),
                        authorization_tenant.as_str(),
                        &authorization_bucket,
                    )
                    .map_err(authz_evaluation_status)?,
                "bucket versioning management is not authorized",
            )
        })
        .await?;
        let changed = store
            .enable_bucket_versioning(storage_tenant.as_str(), &bucket)
            .await
            .map_err(|_| Status::internal("bucket versioning metadata could not be updated"))?;
        Ok(Response::new(api::SetBucketVersioningResponse {
            storage_tenant: response_tenant.to_string(),
            bucket: response_bucket,
            versioning: api::ObjectVersioning::Enabled as i32,
            changed,
        }))
    }

    async fn grant_application_role(
        &self,
        request: Request<api::ApplicationRoleRequest>,
    ) -> Result<Response<api::ApplicationRoleResponse>, Status> {
        self.change_application_role(request, true).await
    }

    async fn revoke_application_role(
        &self,
        request: Request<api::ApplicationRoleRequest>,
    ) -> Result<Response<api::ApplicationRoleResponse>, Status> {
        self.change_application_role(request, false).await
    }
}

impl AdministrationServiceImpl {
    async fn change_application_role(
        &self,
        request: Request<api::ApplicationRoleRequest>,
        granted: bool,
    ) -> Result<Response<api::ApplicationRoleResponse>, Status> {
        let caller = caller(&request)?;
        let request = request.into_inner();
        let target = role_target_from_api(request.target)?;
        let store = self.store.clone();
        let authorizer = self.system_authorizer.clone();
        let receipt = run(move || {
            let system = authorizer.load().map_err(authz_status)?;
            require_role_management(&system, &caller, &target)?;
            store
                .set_application_role(SetApplicationRoleRequest {
                    storage_tenant: caller.storage_tenant().clone(),
                    app_id: request.app_id,
                    target,
                    granted,
                    principal: caller.subject().clone(),
                    expected_authorization_revision: system.revision,
                    expected_binding_generation: system.binding_generation,
                })
                .map_err(credential_store_status)
        })
        .await?;
        Ok(Response::new(api::ApplicationRoleResponse {
            authorization_revision: receipt.authorization_revision.0,
            replayed: receipt.replayed,
        }))
    }
}

fn require_manage_tenant_or_system(
    system: &crate::authorization::SystemAuthorization,
    caller: &Caller,
) -> Result<(), Status> {
    let allowed = if caller.storage_tenant().is_system() {
        system
            .allows_manage_system(caller.subject())
            .map_err(authz_evaluation_status)?
    } else {
        system
            .allows_storage_tenant(
                caller.subject(),
                caller.storage_tenant().as_str(),
                StorageTenantPermission::ManageTenant,
            )
            .map_err(authz_evaluation_status)?
    };
    require_allowed(
        allowed,
        "application credential management is not authorized",
    )
}

fn require_role_management(
    system: &crate::authorization::SystemAuthorization,
    caller: &Caller,
    target: &ApplicationRoleTarget,
) -> Result<(), Status> {
    let allowed = match target {
        ApplicationRoleTarget::System(_) => {
            if !caller.storage_tenant().is_system() {
                false
            } else {
                system
                    .allows_manage_system(caller.subject())
                    .map_err(authz_evaluation_status)?
            }
        }
        ApplicationRoleTarget::Tenant(_) => {
            if caller.storage_tenant().is_system() {
                false
            } else {
                system
                    .allows_storage_tenant(
                        caller.subject(),
                        caller.storage_tenant().as_str(),
                        StorageTenantPermission::ManageTenant,
                    )
                    .map_err(authz_evaluation_status)?
            }
        }
        ApplicationRoleTarget::Bucket { bucket, .. } => {
            if caller.storage_tenant().is_system() {
                false
            } else {
                system
                    .allows_bucket_policy(
                        caller.subject(),
                        caller.storage_tenant().as_str(),
                        bucket,
                    )
                    .map_err(authz_evaluation_status)?
            }
        }
    };
    require_allowed(allowed, "application role management is not authorized")
}

fn role_target_from_api(
    target: Option<api::application_role_request::Target>,
) -> Result<ApplicationRoleTarget, Status> {
    match target.ok_or_else(|| Status::invalid_argument("role target is required"))? {
        api::application_role_request::Target::System(target) => {
            let role = api::SystemApplicationRole::try_from(target.role)
                .map_err(|_| Status::invalid_argument("system application role is invalid"))?;
            match role {
                api::SystemApplicationRole::Admin => {
                    Ok(ApplicationRoleTarget::System(SystemApplicationRole::Admin))
                }
                api::SystemApplicationRole::Unspecified => Err(Status::invalid_argument(
                    "system application role must be specified",
                )),
            }
        }
        api::application_role_request::Target::Tenant(target) => {
            let role = api::TenantApplicationRole::try_from(target.role)
                .map_err(|_| Status::invalid_argument("tenant application role is invalid"))?;
            let role = match role {
                api::TenantApplicationRole::Owner => TenantApplicationRole::Owner,
                api::TenantApplicationRole::Admin => TenantApplicationRole::Admin,
                api::TenantApplicationRole::Reader => TenantApplicationRole::Reader,
                api::TenantApplicationRole::ManageTenant => TenantApplicationRole::ManageTenant,
                api::TenantApplicationRole::ReadTenant => TenantApplicationRole::ReadTenant,
                api::TenantApplicationRole::ManageBuckets => TenantApplicationRole::ManageBuckets,
                api::TenantApplicationRole::ManageAuthz => TenantApplicationRole::ManageAuthz,
                api::TenantApplicationRole::Unspecified => {
                    return Err(Status::invalid_argument(
                        "tenant application role must be specified",
                    ));
                }
            };
            Ok(ApplicationRoleTarget::Tenant(role))
        }
        api::application_role_request::Target::Bucket(target) => {
            let role = api::BucketApplicationRole::try_from(target.role)
                .map_err(|_| Status::invalid_argument("bucket application role is invalid"))?;
            let role = match role {
                api::BucketApplicationRole::Owner => BucketApplicationRole::Owner,
                api::BucketApplicationRole::Admin => BucketApplicationRole::Admin,
                api::BucketApplicationRole::Reader => BucketApplicationRole::Reader,
                api::BucketApplicationRole::Writer => BucketApplicationRole::Writer,
                api::BucketApplicationRole::GetObject => BucketApplicationRole::GetObject,
                api::BucketApplicationRole::PutObject => BucketApplicationRole::PutObject,
                api::BucketApplicationRole::DeleteObject => BucketApplicationRole::DeleteObject,
                api::BucketApplicationRole::ManagePolicy => BucketApplicationRole::ManagePolicy,
                api::BucketApplicationRole::Unspecified => {
                    return Err(Status::invalid_argument(
                        "bucket application role must be specified",
                    ));
                }
            };
            Ok(ApplicationRoleTarget::Bucket {
                bucket: target.bucket,
                role,
            })
        }
    }
}

fn credential_to_api(
    credential: anvil_store::ApplicationCredential,
    replayed: bool,
) -> api::ApplicationCredential {
    api::ApplicationCredential {
        storage_tenant: credential.storage_tenant.to_string(),
        app_id: credential.app_id,
        client_id: credential.client_id,
        active: credential.active,
        replayed,
    }
}

fn credential_state_to_api(receipt: CredentialMutationReceipt) -> api::ApplicationCredentialState {
    api::ApplicationCredentialState {
        storage_tenant: receipt.credential.storage_tenant.to_string(),
        app_id: receipt.credential.app_id,
        client_id: receipt.credential.client_id,
        active: receipt.credential.active,
        replayed: receipt.replayed,
    }
}

fn versioning_from_api(value: i32) -> Result<StoreObjectVersioning, Status> {
    match api::ObjectVersioning::try_from(value) {
        Ok(api::ObjectVersioning::Unversioned) => Ok(StoreObjectVersioning::Unversioned),
        Ok(api::ObjectVersioning::Enabled) => Ok(StoreObjectVersioning::Enabled),
        Err(_) => Err(Status::invalid_argument(
            "object versioning mode is unknown",
        )),
    }
}

fn versioning_to_api(value: StoreObjectVersioning) -> api::ObjectVersioning {
    match value {
        StoreObjectVersioning::Unversioned => api::ObjectVersioning::Unversioned,
        StoreObjectVersioning::Enabled => api::ObjectVersioning::Enabled,
    }
}

fn caller<T>(request: &Request<T>) -> Result<Caller, Status> {
    request
        .extensions()
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("authenticated caller identity is missing"))
}

fn require_allowed(allowed: bool, message: &'static str) -> Result<(), Status> {
    if allowed {
        Ok(())
    } else {
        Err(Status::permission_denied(message))
    }
}

async fn run<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|_| Status::internal("administration task failed"))?
}

fn credential_store_status(error: CredentialRepositoryError) -> Status {
    match error {
        CredentialRepositoryError::InvalidInput(message) => Status::invalid_argument(message),
        CredentialRepositoryError::AlreadyExists(message) => Status::already_exists(message),
        CredentialRepositoryError::NotFound(message) => Status::not_found(message),
        CredentialRepositoryError::Conflict(message) => Status::failed_precondition(message),
        CredentialRepositoryError::AlreadyBootstrapped => {
            Status::failed_precondition("system bootstrap has already completed")
        }
        CredentialRepositoryError::Authorization(error) => authz_status(error),
        CredentialRepositoryError::Entropy(_) | CredentialRepositoryError::Storage(_) => {
            Status::internal("credential or provisioning storage failed")
        }
    }
}

fn authz_status(error: AuthzStoreError) -> Status {
    let message = error.to_string();
    match error {
        AuthzStoreError::InvalidInput(message) => Status::invalid_argument(message),
        AuthzStoreError::RevisionConflict { .. }
        | AuthzStoreError::BindingGenerationConflict { .. } => {
            Status::aborted("authorization state changed; retry the request")
        }
        AuthzStoreError::MissingBinding(_, _) | AuthzStoreError::SchemaNotFound(_, _) => {
            Status::failed_precondition(message)
        }
        AuthzStoreError::RevisionNotAvailable { .. } => Status::unavailable(message),
        AuthzStoreError::RevisionExpired { .. } => Status::failed_precondition(message),
        AuthzStoreError::ReceiptCapacity => Status::resource_exhausted(message),
        AuthzStoreError::OperationMismatch => Status::already_exists(message),
        AuthzStoreError::Authorization(_) | AuthzStoreError::Storage(_) => {
            Status::internal("authorization state could not be evaluated")
        }
    }
}

fn authz_evaluation_status(_error: anvil_authz::AuthorizationError) -> Status {
    Status::internal("authorization state could not be evaluated")
}

#[cfg(test)]
mod tests {
    use anvil_api::v1::administration_service_server::AdministrationService;
    use anvil_store::{StoreOptions, SystemBootstrapRequest};

    use super::*;

    const SECRET: &str = "secret-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    async fn service() -> (tempfile::TempDir, Store, AdministrationServiceImpl) {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        store
            .bootstrap_system(SystemBootstrapRequest {
                app_id: "bootstrap-app".into(),
                client_id: "bootstrap-client".into(),
                client_secret: SECRET.into(),
            })
            .unwrap();
        let service = AdministrationServiceImpl::new(store.clone());
        (directory, store, service)
    }

    fn authenticated<T>(tenant: StorageTenantId, app_id: &str, body: T) -> Request<T> {
        let mut request = Request::new(body);
        request
            .extensions_mut()
            .insert(Caller::from_authenticated_application(tenant, app_id).unwrap());
        request
    }

    #[tokio::test]
    async fn bootstrap_admin_provisions_tenant_and_owner_can_manage_its_resources() {
        let (_directory, store, service) = service().await;
        let provisioned = service
            .provision_tenant(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                api::ProvisionTenantRequest {
                    storage_tenant: "acme".into(),
                    owner_app_id: "acme-owner".into(),
                    owner_client_id: "acme-owner-client".into(),
                    owner_client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(provisioned.authorization_revision, 4);
        assert_eq!(provisioned.credential.unwrap().storage_tenant, "acme");

        let acme = StorageTenantId::parse("acme").unwrap();
        let worker = service
            .create_application(authenticated(
                acme.clone(),
                "acme-owner",
                api::CreateApplicationRequest {
                    app_id: "worker".into(),
                    client_id: "worker-client".into(),
                    client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(worker.app_id, "worker");
        assert!(worker.active);
        let bucket = service
            .create_bucket(authenticated(
                acme.clone(),
                "acme-owner",
                api::CreateBucketRequest {
                    bucket: "objects".into(),
                    versioning: api::ObjectVersioning::Unversioned as i32,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(bucket.authorization_revision, 5);
        assert_eq!(bucket.versioning, api::ObjectVersioning::Unversioned as i32);
        let unauthorized = service
            .set_bucket_versioning(authenticated(
                acme.clone(),
                "worker",
                api::SetBucketVersioningRequest {
                    bucket: "objects".into(),
                    versioning: api::ObjectVersioning::Enabled as i32,
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(unauthorized.code(), tonic::Code::PermissionDenied);
        let versioning = service
            .set_bucket_versioning(authenticated(
                acme.clone(),
                "acme-owner",
                api::SetBucketVersioningRequest {
                    bucket: "objects".into(),
                    versioning: api::ObjectVersioning::Enabled as i32,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(versioning.changed);
        assert_eq!(versioning.storage_tenant, "acme");
        assert_eq!(versioning.bucket, "objects");
        assert_eq!(versioning.versioning, api::ObjectVersioning::Enabled as i32);
        let replay = service
            .set_bucket_versioning(authenticated(
                acme.clone(),
                "acme-owner",
                api::SetBucketVersioningRequest {
                    bucket: "objects".into(),
                    versioning: api::ObjectVersioning::Enabled as i32,
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(!replay.changed);
        let disable = service
            .set_bucket_versioning(authenticated(
                acme.clone(),
                "acme-owner",
                api::SetBucketVersioningRequest {
                    bucket: "objects".into(),
                    versioning: api::ObjectVersioning::Unversioned as i32,
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(disable.code(), tonic::Code::InvalidArgument);
        let role = service
            .grant_application_role(authenticated(
                acme,
                "acme-owner",
                api::ApplicationRoleRequest {
                    app_id: "worker".into(),
                    target: Some(api::application_role_request::Target::Bucket(
                        api::BucketApplicationRoleTarget {
                            bucket: "objects".into(),
                            role: api::BucketApplicationRole::Writer.into(),
                        },
                    )),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(role.authorization_revision, 6);
        let revoked = service
            .revoke_application_role(authenticated(
                StorageTenantId::parse("acme").unwrap(),
                "acme-owner",
                api::ApplicationRoleRequest {
                    app_id: "worker".into(),
                    target: Some(api::application_role_request::Target::Bucket(
                        api::BucketApplicationRoleTarget {
                            bucket: "objects".into(),
                            role: api::BucketApplicationRole::Writer.into(),
                        },
                    )),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(revoked.authorization_revision, 7);
        assert!(!revoked.replayed);
        assert!(
            store
                .application(&StorageTenantId::parse("acme").unwrap(), "worker")
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn requests_without_identity_and_unprivileged_apps_fail_closed() {
        let (_directory, _store, service) = service().await;
        let missing = service
            .provision_tenant(Request::new(api::ProvisionTenantRequest {
                storage_tenant: "acme".into(),
                owner_app_id: "owner".into(),
                owner_client_id: "owner-client".into(),
                owner_client_secret: SECRET.into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::Unauthenticated);

        let create_application = service
            .create_application(Request::new(api::CreateApplicationRequest::default()))
            .await
            .unwrap_err();
        let rotate = service
            .rotate_application_credential(Request::new(
                api::RotateApplicationCredentialRequest::default(),
            ))
            .await
            .unwrap_err();
        let disable = service
            .disable_application_credential(Request::new(
                api::DisableApplicationCredentialRequest::default(),
            ))
            .await
            .unwrap_err();
        let bucket = service
            .create_bucket(Request::new(api::CreateBucketRequest::default()))
            .await
            .unwrap_err();
        let versioning = service
            .set_bucket_versioning(Request::new(api::SetBucketVersioningRequest::default()))
            .await
            .unwrap_err();
        let grant = service
            .grant_application_role(Request::new(api::ApplicationRoleRequest::default()))
            .await
            .unwrap_err();
        let revoke = service
            .revoke_application_role(Request::new(api::ApplicationRoleRequest::default()))
            .await
            .unwrap_err();
        for status in [
            create_application,
            rotate,
            disable,
            bucket,
            versioning,
            grant,
            revoke,
        ] {
            assert_eq!(status.code(), tonic::Code::Unauthenticated);
        }

        let denied = service
            .provision_tenant(authenticated(
                StorageTenantId::system(),
                "not-an-admin",
                api::ProvisionTenantRequest {
                    storage_tenant: "acme".into(),
                    owner_app_id: "owner".into(),
                    owner_client_id: "owner-client".into(),
                    owner_client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn application_rotation_disable_and_system_role_assignment_are_typed_and_authorized() {
        let (_directory, store, service) = service().await;
        let system_app = service
            .create_application(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                api::CreateApplicationRequest {
                    app_id: "system-admin".into(),
                    client_id: "system-admin-client".into(),
                    client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(system_app.storage_tenant, "_anvil");

        let role = service
            .grant_application_role(authenticated(
                StorageTenantId::system(),
                "bootstrap-app",
                api::ApplicationRoleRequest {
                    app_id: "system-admin".into(),
                    target: Some(api::application_role_request::Target::System(
                        api::SystemApplicationRoleTarget {
                            role: api::SystemApplicationRole::Admin.into(),
                        },
                    )),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(role.authorization_revision, 4);

        service
            .provision_tenant(authenticated(
                StorageTenantId::system(),
                "system-admin",
                api::ProvisionTenantRequest {
                    storage_tenant: "acme".into(),
                    owner_app_id: "acme-owner".into(),
                    owner_client_id: "acme-owner-client".into(),
                    owner_client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap();
        let acme = StorageTenantId::parse("acme").unwrap();
        service
            .create_application(authenticated(
                acme.clone(),
                "acme-owner",
                api::CreateApplicationRequest {
                    app_id: "worker".into(),
                    client_id: "worker-client".into(),
                    client_secret: SECRET.into(),
                },
            ))
            .await
            .unwrap();

        let replacement = "replacement-0123456789abcdef0123456789abcdef0123456789abcdef";
        let rotated = service
            .rotate_application_credential(authenticated(
                acme.clone(),
                "acme-owner",
                api::RotateApplicationCredentialRequest {
                    app_id: "worker".into(),
                    client_id: "worker-client".into(),
                    client_secret: replacement.into(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(rotated.active);
        assert!(
            store
                .verify_credential("worker-client", replacement)
                .unwrap()
                .is_some()
        );
        let disabled = service
            .disable_application_credential(authenticated(
                acme.clone(),
                "acme-owner",
                api::DisableApplicationCredentialRequest {
                    app_id: "worker".into(),
                    client_id: "worker-client".into(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(!disabled.active);
        assert!(
            store
                .verify_credential("worker-client", replacement)
                .unwrap()
                .is_none()
        );

        let denied = service
            .grant_application_role(authenticated(
                acme,
                "acme-owner",
                api::ApplicationRoleRequest {
                    app_id: "system-admin".into(),
                    target: Some(api::application_role_request::Target::System(
                        api::SystemApplicationRoleTarget {
                            role: api::SystemApplicationRole::Admin.into(),
                        },
                    )),
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn unspecified_and_unknown_roles_are_rejected_before_storage() {
        for role in [0, 999] {
            let error = role_target_from_api(Some(api::application_role_request::Target::System(
                api::SystemApplicationRoleTarget { role },
            )))
            .unwrap_err();
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
        }
    }
}
