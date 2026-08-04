//! Bounded distributed administration and credential exchange.
//!
//! Public administration is routed once to the Raft-nominated executor. The
//! executor coordinates complete logical records through their independent
//! HRW coordinators and applies authorization grants only after those record
//! quorums are durable. Credential verification is routed by client ID and
//! performs Argon2 only on that coordinator.

use std::time::Duration;

use anvil_api::v1 as api;
use anvil_authz::{AuthorizationCheck, ObjectRef};
use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{
    ApplicationCredentialRequest, ApplicationRoleTarget, AuthzConsistency, AuthzRevision,
    AuthzScope, CoordinatedAuthzRealmResult, CreateBucketRequest, LogicalApplicationRecord,
    LogicalCredentialRecord, LogicalRecordId, LogicalRecordValue, ObjectVersioning, PlacementLogId,
    ProvisionTenantRequest, SetApplicationRoleRequest, SetBucketPublicReadRequest, StorageTenantId,
    Store, TupleBatchReceipt, TupleBatchRequest,
};
use tonic::Status;

use crate::authentication::{ACCESS_TOKEN_LIFETIME, Caller, JwtManager};
use crate::authorization::{SYSTEM_STABLE_TENANT_ID, bucket_resource, storage_tenant_resource};
use crate::authz_distribution::ZanzibarDistribution;
use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_placement::ClusterPlacement;
use crate::distributed_list::OriginalBearer;
use crate::logical_record_distribution::LogicalRecordDistribution;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::PlacementKind;
use crate::serving_fence::ServingAuthority;

const CONTROL_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(crate) struct DistributedControlPlane {
    local_node: NodeId,
    store: Store,
    decisions: DecisionRaft,
    serving: ServingAuthority,
    logical: LogicalRecordDistribution,
    zanzibar: std::sync::Arc<ZanzibarDistribution>,
    peers: ClusterPeerTransport,
    tokens: JwtManager,
    administration_serial: std::sync::Arc<tokio::sync::Mutex<()>>,
}

impl DistributedControlPlane {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        local_node: NodeId,
        store: Store,
        decisions: DecisionRaft,
        serving: ServingAuthority,
        logical: LogicalRecordDistribution,
        zanzibar: std::sync::Arc<ZanzibarDistribution>,
        peers: ClusterPeerTransport,
        tokens: JwtManager,
    ) -> Self {
        Self {
            local_node,
            store,
            decisions,
            serving,
            logical,
            zanzibar,
            peers,
            tokens,
            administration_serial: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(crate) async fn provision_tenant(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        request: api::ProvisionTenantRequest,
    ) -> Result<api::ProvisionTenantResponse, Status> {
        if let Some(target) = self.executor_target()? {
            return self
                .peers
                .route_provision_tenant(
                    target.node_id,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    CONTROL_OPERATION_TIMEOUT,
                )
                .await;
        }
        self.execute_provision_tenant(caller, request).await
    }

    pub(crate) async fn execute_routed_provision_tenant(
        &self,
        bearer: &str,
        request: api::ProvisionTenantRequest,
    ) -> Result<api::ProvisionTenantResponse, Status> {
        let caller = self
            .tokens
            .verify(bearer)
            .map_err(|_| Status::unauthenticated("the bearer token is invalid"))?;
        self.require_local_executor()?;
        self.execute_provision_tenant(caller, request).await
    }

    pub(crate) async fn create_bucket(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        request: api::CreateBucketRequest,
    ) -> Result<api::CreateBucketResponse, Status> {
        if let Some(target) = self.executor_target()? {
            return self
                .peers
                .route_create_bucket(
                    target.node_id,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    CONTROL_OPERATION_TIMEOUT,
                )
                .await;
        }
        self.execute_create_bucket(caller, request).await
    }

    pub(crate) async fn execute_routed_create_bucket(
        &self,
        bearer: &str,
        request: api::CreateBucketRequest,
    ) -> Result<api::CreateBucketResponse, Status> {
        let caller = self
            .tokens
            .verify(bearer)
            .map_err(|_| Status::unauthenticated("the bearer token is invalid"))?;
        self.require_local_executor()?;
        self.execute_create_bucket(caller, request).await
    }

    pub(crate) async fn create_application(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        request: api::CreateApplicationRequest,
    ) -> Result<api::ApplicationCredential, Status> {
        if let Some(target) = self.executor_target()? {
            return self
                .peers
                .route_admin_create_application(
                    target.node_id,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    CONTROL_OPERATION_TIMEOUT,
                )
                .await;
        }
        self.execute_create_application(caller, request).await
    }

    pub(crate) async fn execute_routed_create_application(
        &self,
        bearer: &str,
        request: api::CreateApplicationRequest,
    ) -> Result<api::ApplicationCredential, Status> {
        let caller = self.verify_routed_bearer(bearer)?;
        self.require_local_executor()?;
        self.execute_create_application(caller, request).await
    }

    pub(crate) async fn rotate_application_credential(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        request: api::RotateApplicationCredentialRequest,
    ) -> Result<api::ApplicationCredential, Status> {
        if let Some(target) = self.executor_target()? {
            return self
                .peers
                .route_admin_rotate_credential(
                    target.node_id,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    CONTROL_OPERATION_TIMEOUT,
                )
                .await;
        }
        self.execute_rotate_credential(caller, request).await
    }

    pub(crate) async fn execute_routed_rotate_credential(
        &self,
        bearer: &str,
        request: api::RotateApplicationCredentialRequest,
    ) -> Result<api::ApplicationCredential, Status> {
        let caller = self.verify_routed_bearer(bearer)?;
        self.require_local_executor()?;
        self.execute_rotate_credential(caller, request).await
    }

    pub(crate) async fn disable_application_credential(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        request: api::DisableApplicationCredentialRequest,
    ) -> Result<api::ApplicationCredentialState, Status> {
        if let Some(target) = self.executor_target()? {
            return self
                .peers
                .route_admin_disable_credential(
                    target.node_id,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    CONTROL_OPERATION_TIMEOUT,
                )
                .await;
        }
        self.execute_disable_credential(caller, request).await
    }

    pub(crate) async fn execute_routed_disable_credential(
        &self,
        bearer: &str,
        request: api::DisableApplicationCredentialRequest,
    ) -> Result<api::ApplicationCredentialState, Status> {
        let caller = self.verify_routed_bearer(bearer)?;
        self.require_local_executor()?;
        self.execute_disable_credential(caller, request).await
    }

    pub(crate) async fn set_bucket_versioning(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        request: api::SetBucketVersioningRequest,
    ) -> Result<api::SetBucketVersioningResponse, Status> {
        if let Some(target) = self.executor_target()? {
            return self
                .peers
                .route_admin_set_bucket_versioning(
                    target.node_id,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    CONTROL_OPERATION_TIMEOUT,
                )
                .await;
        }
        self.execute_set_bucket_versioning(caller, request).await
    }

    pub(crate) async fn execute_routed_set_bucket_versioning(
        &self,
        bearer: &str,
        request: api::SetBucketVersioningRequest,
    ) -> Result<api::SetBucketVersioningResponse, Status> {
        let caller = self.verify_routed_bearer(bearer)?;
        self.require_local_executor()?;
        self.execute_set_bucket_versioning(caller, request).await
    }

    pub(crate) async fn set_bucket_public_read(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        request: api::SetBucketPublicReadRequest,
    ) -> Result<api::SetBucketPublicReadResponse, Status> {
        if let Some(target) = self.executor_target()? {
            return self
                .peers
                .route_admin_set_bucket_public_read(
                    target.node_id,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    CONTROL_OPERATION_TIMEOUT,
                )
                .await;
        }
        self.execute_set_bucket_public_read(caller, request).await
    }

    pub(crate) async fn execute_routed_set_bucket_public_read(
        &self,
        bearer: &str,
        request: api::SetBucketPublicReadRequest,
    ) -> Result<api::SetBucketPublicReadResponse, Status> {
        let caller = self.verify_routed_bearer(bearer)?;
        self.require_local_executor()?;
        self.execute_set_bucket_public_read(caller, request).await
    }

    pub(crate) async fn change_application_role(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        request: api::ApplicationRoleRequest,
        granted: bool,
    ) -> Result<api::ApplicationRoleResponse, Status> {
        if let Some(target) = self.executor_target()? {
            return self
                .peers
                .route_admin_change_application_role(
                    target.node_id,
                    &target.address,
                    bearer.signed_token(),
                    request,
                    granted,
                    CONTROL_OPERATION_TIMEOUT,
                )
                .await;
        }
        self.execute_change_application_role(caller, request, granted)
            .await
    }

    pub(crate) async fn execute_routed_change_application_role(
        &self,
        bearer: &str,
        request: api::ApplicationRoleRequest,
        granted: bool,
    ) -> Result<api::ApplicationRoleResponse, Status> {
        let caller = self.verify_routed_bearer(bearer)?;
        self.require_local_executor()?;
        self.execute_change_application_role(caller, request, granted)
            .await
    }

    /// PrepareNode remains on the contacted Raft leader because its response
    /// names a mode-0600 file on that node. Authorization is nevertheless read
    /// freshly from the distributed protected realm.
    pub(crate) async fn authorize_node_preparation(&self, caller: &Caller) -> Result<(), Status> {
        self.authorize_system(
            caller.subject(),
            ObjectRef::opaque("system", anvil_store::SYSTEM_STORAGE_TENANT_ID)
                .map_err(authz_evaluation_status)?,
            "manage_system",
            "node preparation is not authorized",
        )
        .await
        .map(|_| ())
    }

    pub(crate) async fn exchange_client_credentials(
        &self,
        request: api::ExchangeClientCredentialsRequest,
    ) -> Result<api::AccessToken, Status> {
        if let Some(target) = self.credential_target(&request.client_id)? {
            return self
                .peers
                .route_credential_exchange(
                    target.node_id,
                    &target.address,
                    request,
                    CONTROL_OPERATION_TIMEOUT,
                )
                .await;
        }
        self.execute_credential_exchange(request).await
    }

    pub(crate) async fn execute_routed_credential_exchange(
        &self,
        request: api::ExchangeClientCredentialsRequest,
    ) -> Result<api::AccessToken, Status> {
        if self.credential_target(&request.client_id)?.is_some() {
            return Err(Status::failed_precondition(
                "credential exchange did not reach its current coordinator",
            ));
        }
        self.execute_credential_exchange(request).await
    }

    pub(crate) async fn coordinate_logical_record(
        &self,
        value: LogicalRecordValue,
    ) -> Result<(), Status> {
        self.require_local_logical_coordinator(&value.id())?;
        self.logical.mutate(value).await?;
        Ok(())
    }

    pub(crate) async fn read_logical_record(
        &self,
        id: LogicalRecordId,
    ) -> Result<Option<LogicalRecordValue>, Status> {
        self.require_local_logical_coordinator(&id)?;
        self.logical.read(&id).await
    }

    pub(crate) async fn coordinate_system_grant(
        &self,
        request: TupleBatchRequest,
    ) -> Result<TupleBatchReceipt, Status> {
        self.require_system_realm_coordinator()?;
        let coordinated = self
            .zanzibar
            .mutate_tuples_journaled(SYSTEM_STABLE_TENANT_ID, &self.store, request)
            .await?;
        match coordinated.result {
            CoordinatedAuthzRealmResult::Tuples(receipt) => Ok(receipt),
            CoordinatedAuthzRealmResult::Bound(_) => Err(Status::internal(
                "administration grant returned a schema-binding result",
            )),
        }
    }

    async fn execute_provision_tenant(
        &self,
        caller: Caller,
        request: api::ProvisionTenantRequest,
    ) -> Result<api::ProvisionTenantResponse, Status> {
        self.require_local_executor()?;
        let _serial = self.administration_serial.lock().await;
        self.require_local_executor()?;
        let system = self
            .authorize_system(
                caller.subject(),
                ObjectRef::opaque("system", anvil_store::SYSTEM_STORAGE_TENANT_ID)
                    .map_err(authz_evaluation_status)?,
                "manage_system",
                "tenant provisioning is not authorized",
            )
            .await?;
        let storage_tenant = StorageTenantId::parse(request.storage_tenant)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let name_id = LogicalRecordId::TenantNameClaim {
            storage_tenant: storage_tenant.clone(),
        };
        let existing = self.read_record(&name_id).await?;
        let prepared = if let Some(existing) = existing {
            let LogicalRecordValue::TenantNameClaim { tenant_id, .. } = existing else {
                return Err(Status::data_loss(
                    "tenant-name claim has the wrong typed value",
                ));
            };
            let tenant = self
                .read_record(&LogicalRecordId::TenantRecord { tenant_id })
                .await?;
            let expected = match tenant {
                Some(LogicalRecordValue::TenantRecord(tenant_record)) => {
                    if tenant_record.storage_tenant != storage_tenant
                        || tenant_record.owner_app_id != request.owner_app_id
                        || tenant_record.owner_client_id != request.owner_client_id
                    {
                        return Err(Status::already_exists("storage tenant is already claimed"));
                    }
                    AuthzRevision(
                        tenant_record
                            .authorization_revision
                            .0
                            .checked_sub(1)
                            .ok_or_else(|| {
                                Status::data_loss("tenant authorization revision is invalid")
                            })?,
                    )
                }
                Some(_) => {
                    return Err(Status::data_loss("tenant record has the wrong typed value"));
                }
                None => system.revision,
            };
            self.store
                .prepare_tenant_provisioning(
                    ProvisionTenantRequest {
                        storage_tenant: storage_tenant.clone(),
                        owner_app_id: request.owner_app_id.clone(),
                        owner_client_id: request.owner_client_id.clone(),
                        owner_client_secret: request.owner_client_secret.clone(),
                        principal: caller.subject().clone(),
                        expected_authorization_revision: expected,
                        expected_binding_generation: system.binding_generation,
                    },
                    tenant_id,
                )
                .map_err(credential_status)?
        } else {
            self.require_identity_absent(&request.owner_app_id, &request.owner_client_id)
                .await?;
            let tenant_id = self
                .store
                .allocate_logical_record_version()
                .map_err(|error| Status::internal(error.to_string()))?
                .0;
            self.store
                .prepare_tenant_provisioning(
                    ProvisionTenantRequest {
                        storage_tenant: storage_tenant.clone(),
                        owner_app_id: request.owner_app_id.clone(),
                        owner_client_id: request.owner_client_id.clone(),
                        owner_client_secret: request.owner_client_secret.clone(),
                        principal: caller.subject().clone(),
                        expected_authorization_revision: system.revision,
                        expected_binding_generation: system.binding_generation,
                    },
                    tenant_id,
                )
                .map_err(credential_status)?
        };

        for value in prepared.logical_records.iter().cloned() {
            self.ensure_administration_record(value, Some(&request.owner_client_secret))
                .await?;
        }
        let credential = self
            .read_and_verify_credential(
                &request.owner_client_id,
                &request.owner_app_id,
                &storage_tenant,
                &request.owner_client_secret,
            )
            .await?;
        let grant = self.apply_system_grant(prepared.grant).await?;
        self.require_local_executor()?;
        let replayed = grant.replayed;
        Ok(api::ProvisionTenantResponse {
            credential: Some(api::ApplicationCredential {
                storage_tenant: credential.storage_tenant.to_string(),
                app_id: credential.app_id,
                client_id: credential.client_id,
                active: credential.active,
                replayed,
            }),
            authorization_revision: grant.authz_revision.0,
            replayed,
        })
    }

    async fn execute_create_bucket(
        &self,
        caller: Caller,
        request: api::CreateBucketRequest,
    ) -> Result<api::CreateBucketResponse, Status> {
        self.require_local_executor()?;
        let _serial = self.administration_serial.lock().await;
        self.require_local_executor()?;
        if caller.storage_tenant().is_system() {
            return Err(Status::invalid_argument(
                "buckets cannot be created in the protected system tenant",
            ));
        }
        let versioning = versioning_from_api(request.versioning)?;
        let storage_tenant = caller.storage_tenant().clone();
        let tenant_id = match self
            .read_record(&LogicalRecordId::TenantNameClaim {
                storage_tenant: storage_tenant.clone(),
            })
            .await?
        {
            Some(LogicalRecordValue::TenantNameClaim { tenant_id, .. }) => tenant_id,
            Some(_) => return Err(Status::data_loss("tenant-name claim has the wrong type")),
            None => return Err(Status::not_found("storage tenant does not exist")),
        };
        let system = self
            .authorize_system(
                caller.subject(),
                storage_tenant_resource(storage_tenant.as_str())
                    .map_err(authz_evaluation_status)?,
                "manage_buckets",
                "bucket creation is not authorized",
            )
            .await?;
        let name_id = LogicalRecordId::BucketNameClaim {
            tenant_id,
            bucket: request.bucket.clone(),
        };
        let prepared = if let Some(existing) = self.read_record(&name_id).await? {
            let LogicalRecordValue::BucketNameClaim { bucket_id, .. } = existing else {
                return Err(Status::data_loss(
                    "bucket-name claim has the wrong typed value",
                ));
            };
            let record = self
                .read_record(&LogicalRecordId::BucketRecord {
                    tenant_id,
                    bucket_id,
                })
                .await?;
            let expected = match record {
                Some(LogicalRecordValue::BucketRecord(record)) => {
                    if record.storage_tenant != storage_tenant
                        || record.bucket != request.bucket
                        || record.owner != *caller.subject()
                    {
                        return Err(Status::already_exists("bucket is already claimed"));
                    }
                    AuthzRevision(record.authorization_revision.0.checked_sub(1).ok_or_else(
                        || Status::data_loss("bucket authorization revision is invalid"),
                    )?)
                }
                Some(_) => {
                    return Err(Status::data_loss("bucket record has the wrong typed value"));
                }
                None => system.revision,
            };
            self.store
                .prepare_bucket_creation(
                    CreateBucketRequest {
                        storage_tenant: storage_tenant.clone(),
                        bucket: request.bucket.clone(),
                        owner: caller.subject().clone(),
                        principal: caller.subject().clone(),
                        expected_authorization_revision: expected,
                        expected_binding_generation: system.binding_generation,
                        versioning,
                    },
                    tenant_id,
                    bucket_id,
                )
                .map_err(credential_status)?
        } else {
            let bucket_id = self
                .store
                .allocate_logical_record_version()
                .map_err(|error| Status::internal(error.to_string()))?
                .0;
            self.store
                .prepare_bucket_creation(
                    CreateBucketRequest {
                        storage_tenant: storage_tenant.clone(),
                        bucket: request.bucket.clone(),
                        owner: caller.subject().clone(),
                        principal: caller.subject().clone(),
                        expected_authorization_revision: system.revision,
                        expected_binding_generation: system.binding_generation,
                        versioning,
                    },
                    tenant_id,
                    bucket_id,
                )
                .map_err(credential_status)?
        };
        for value in prepared.logical_records.iter().cloned() {
            self.ensure_administration_record(value, None).await?;
        }
        let grant = self.apply_system_grant(prepared.grant).await?;
        self.require_local_executor()?;
        let replayed = grant.replayed;
        Ok(api::CreateBucketResponse {
            storage_tenant: storage_tenant.to_string(),
            bucket: request.bucket,
            authorization_revision: grant.authz_revision.0,
            replayed,
            versioning: versioning_to_api(versioning) as i32,
        })
    }

    async fn execute_create_application(
        &self,
        caller: Caller,
        request: api::CreateApplicationRequest,
    ) -> Result<api::ApplicationCredential, Status> {
        self.require_local_executor()?;
        let _serial = self.administration_serial.lock().await;
        self.require_local_executor()?;
        self.authorize_application_management(&caller).await?;
        let storage_tenant = caller.storage_tenant().clone();
        let application_id = LogicalRecordId::Application {
            app_id: request.app_id.clone(),
        };
        let credential_id = LogicalRecordId::Credential {
            client_id: request.client_id.clone(),
        };
        let existing_application = self.read_record(&application_id).await?;
        let existing_credential = self.read_record(&credential_id).await?;
        if let Some(value) = existing_application.as_ref() {
            require_application_value(value, &storage_tenant, &request.app_id, &request.client_id)?;
        }
        let replay_credential = if let Some(value) = existing_credential.as_ref() {
            let credential = require_credential_value(
                value,
                &storage_tenant,
                &request.app_id,
                &request.client_id,
            )?
            .verify_secret(&request.client_secret)
            .map_err(credential_status)?
            .ok_or_else(|| Status::already_exists("client identity is already claimed"))?;
            Some(credential)
        } else {
            None
        };
        if existing_application.is_some()
            && let Some(credential) = replay_credential
        {
            return Ok(credential_to_api(credential, true));
        }
        let prepared = self
            .store
            .prepare_application_creation(ApplicationCredentialRequest {
                storage_tenant: storage_tenant.clone(),
                app_id: request.app_id.clone(),
                client_id: request.client_id.clone(),
                client_secret: request.client_secret.clone(),
            })
            .map_err(credential_status)?;
        for value in prepared.logical_records {
            self.ensure_administration_record(value, Some(&request.client_secret))
                .await?;
        }
        let credential = self
            .read_and_verify_credential(
                &request.client_id,
                &request.app_id,
                &storage_tenant,
                &request.client_secret,
            )
            .await?;
        self.require_local_executor()?;
        Ok(credential_to_api(
            credential,
            existing_application.is_some() && existing_credential.is_some(),
        ))
    }

    async fn execute_rotate_credential(
        &self,
        caller: Caller,
        request: api::RotateApplicationCredentialRequest,
    ) -> Result<api::ApplicationCredential, Status> {
        self.require_local_executor()?;
        let _serial = self.administration_serial.lock().await;
        self.require_local_executor()?;
        self.authorize_application_management(&caller).await?;
        let storage_tenant = caller.storage_tenant().clone();
        let application = self
            .read_record(&LogicalRecordId::Application {
                app_id: request.app_id.clone(),
            })
            .await?
            .ok_or_else(|| Status::not_found("application does not exist"))?;
        require_application_value(
            &application,
            &storage_tenant,
            &request.app_id,
            &request.client_id,
        )?;
        let credential_id = LogicalRecordId::Credential {
            client_id: request.client_id.clone(),
        };
        let current = self
            .read_record(&credential_id)
            .await?
            .ok_or_else(|| Status::not_found("application credential does not exist"))?;
        let current = require_credential_value(
            &current,
            &storage_tenant,
            &request.app_id,
            &request.client_id,
        )?;
        if let Some(credential) = current
            .verify_secret(&request.client_secret)
            .map_err(credential_status)?
        {
            return Ok(credential_to_api(credential, true));
        }
        let prepared = self
            .store
            .prepare_credential_rotation(ApplicationCredentialRequest {
                storage_tenant: storage_tenant.clone(),
                app_id: request.app_id.clone(),
                client_id: request.client_id.clone(),
                client_secret: request.client_secret.clone(),
            })
            .map_err(credential_status)?;
        let value =
            prepared.logical_records.into_iter().next().ok_or_else(|| {
                Status::internal("credential rotation produced no logical record")
            })?;
        self.write_record(value).await?;
        let credential = self
            .read_and_verify_credential(
                &request.client_id,
                &request.app_id,
                &storage_tenant,
                &request.client_secret,
            )
            .await?;
        self.require_local_executor()?;
        Ok(credential_to_api(credential, false))
    }

    async fn execute_disable_credential(
        &self,
        caller: Caller,
        request: api::DisableApplicationCredentialRequest,
    ) -> Result<api::ApplicationCredentialState, Status> {
        self.require_local_executor()?;
        let _serial = self.administration_serial.lock().await;
        self.require_local_executor()?;
        self.authorize_application_management(&caller).await?;
        let storage_tenant = caller.storage_tenant().clone();
        let application = self
            .read_record(&LogicalRecordId::Application {
                app_id: request.app_id.clone(),
            })
            .await?
            .ok_or_else(|| Status::not_found("application does not exist"))?;
        require_application_value(
            &application,
            &storage_tenant,
            &request.app_id,
            &request.client_id,
        )?;
        let value = self
            .read_record(&LogicalRecordId::Credential {
                client_id: request.client_id.clone(),
            })
            .await?
            .ok_or_else(|| Status::not_found("application credential does not exist"))?;
        let current =
            require_credential_value(&value, &storage_tenant, &request.app_id, &request.client_id)?;
        let replayed = !current.active();
        let prepared = self
            .store
            .prepare_credential_disable(current.clone())
            .map_err(credential_status)?;
        if !replayed {
            let value =
                prepared.logical_records.into_iter().next().ok_or_else(|| {
                    Status::internal("credential disable produced no logical record")
                })?;
            self.write_record(value).await?;
        }
        self.require_local_executor()?;
        Ok(credential_state_to_api(prepared.credential, replayed))
    }

    async fn execute_set_bucket_versioning(
        &self,
        caller: Caller,
        request: api::SetBucketVersioningRequest,
    ) -> Result<api::SetBucketVersioningResponse, Status> {
        self.require_local_executor()?;
        let _serial = self.administration_serial.lock().await;
        self.require_local_executor()?;
        if caller.storage_tenant().is_system() {
            return Err(Status::invalid_argument(
                "buckets cannot exist in the protected system tenant",
            ));
        }
        if versioning_from_api(request.versioning)? != ObjectVersioning::Enabled {
            return Err(Status::invalid_argument(
                "SetBucketVersioning accepts only ENABLED; bucket versioning cannot be disabled",
            ));
        }
        let storage_tenant = caller.storage_tenant().clone();
        self.authorize_system(
            caller.subject(),
            bucket_resource(storage_tenant.as_str(), &request.bucket)
                .map_err(authz_evaluation_status)?,
            "manage_policy",
            "bucket versioning management is not authorized",
        )
        .await?;
        let (tenant_id, bucket_id) = self
            .require_bucket_identity(&storage_tenant, &request.bucket)
            .await?;
        let id = LogicalRecordId::BucketOptions {
            tenant_id,
            bucket_id,
        };
        let changed = match self.read_record(&id).await? {
            Some(LogicalRecordValue::BucketOptions { versioning, .. })
                if versioning == ObjectVersioning::Enabled =>
            {
                false
            }
            Some(LogicalRecordValue::BucketOptions { .. }) | None => {
                self.write_record(LogicalRecordValue::BucketOptions {
                    tenant_id,
                    bucket_id,
                    versioning: ObjectVersioning::Enabled,
                })
                .await?;
                true
            }
            Some(_) => return Err(Status::data_loss("bucket options have the wrong type")),
        };
        self.require_local_executor()?;
        Ok(api::SetBucketVersioningResponse {
            storage_tenant: storage_tenant.to_string(),
            bucket: request.bucket,
            versioning: api::ObjectVersioning::Enabled as i32,
            changed,
        })
    }

    async fn execute_set_bucket_public_read(
        &self,
        caller: Caller,
        request: api::SetBucketPublicReadRequest,
    ) -> Result<api::SetBucketPublicReadResponse, Status> {
        self.require_local_executor()?;
        let _serial = self.administration_serial.lock().await;
        self.require_local_executor()?;
        if caller.storage_tenant().is_system() {
            return Err(Status::invalid_argument(
                "buckets cannot exist in the protected system tenant",
            ));
        }
        let storage_tenant = caller.storage_tenant().clone();
        let authorization = self
            .authorize_system(
                caller.subject(),
                bucket_resource(storage_tenant.as_str(), &request.bucket)
                    .map_err(authz_evaluation_status)?,
                "manage_policy",
                "public bucket policy management is not authorized",
            )
            .await?;
        self.require_bucket_identity(&storage_tenant, &request.bucket)
            .await?;
        let grant = self
            .store
            .prepare_bucket_public_read_change(SetBucketPublicReadRequest {
                storage_tenant: storage_tenant.clone(),
                bucket: request.bucket.clone(),
                enabled: request.enabled,
                principal: caller.subject().clone(),
                expected_authorization_revision: authorization.revision,
                expected_binding_generation: authorization.binding_generation,
            })
            .map_err(credential_status)?;
        let receipt = self.apply_system_grant(grant).await?;
        self.require_local_executor()?;
        Ok(api::SetBucketPublicReadResponse {
            storage_tenant: storage_tenant.to_string(),
            bucket: request.bucket,
            enabled: request.enabled,
            authorization_revision: receipt.authz_revision.0,
            replayed: receipt.replayed,
        })
    }

    async fn execute_change_application_role(
        &self,
        caller: Caller,
        request: api::ApplicationRoleRequest,
        granted: bool,
    ) -> Result<api::ApplicationRoleResponse, Status> {
        self.require_local_executor()?;
        let _serial = self.administration_serial.lock().await;
        self.require_local_executor()?;
        let target = crate::administration_service::role_target_from_api(request.target)?;
        let authorization = self.authorize_role_management(&caller, &target).await?;
        let application = self
            .read_record(&LogicalRecordId::Application {
                app_id: request.app_id.clone(),
            })
            .await?
            .ok_or_else(|| Status::not_found("application does not exist"))?;
        let LogicalRecordValue::Application(application) = application else {
            return Err(Status::data_loss("application record has the wrong type"));
        };
        if application.storage_tenant != *caller.storage_tenant() {
            return Err(Status::not_found("application does not exist"));
        }
        if let ApplicationRoleTarget::Bucket { bucket, .. } = &target {
            self.require_bucket_identity(caller.storage_tenant(), bucket)
                .await?;
        }
        let grant = self
            .store
            .prepare_application_role_change(SetApplicationRoleRequest {
                storage_tenant: caller.storage_tenant().clone(),
                app_id: request.app_id,
                target,
                granted,
                principal: caller.subject().clone(),
                expected_authorization_revision: authorization.revision,
                expected_binding_generation: authorization.binding_generation,
            })
            .map_err(credential_status)?;
        let receipt = self.apply_system_grant(grant).await?;
        self.require_local_executor()?;
        Ok(api::ApplicationRoleResponse {
            authorization_revision: receipt.authz_revision.0,
            replayed: receipt.replayed,
        })
    }

    async fn execute_credential_exchange(
        &self,
        request: api::ExchangeClientCredentialsRequest,
    ) -> Result<api::AccessToken, Status> {
        let credential_id = LogicalRecordId::Credential {
            client_id: request.client_id.clone(),
        };
        let credential = match self.read_record(&credential_id).await? {
            Some(LogicalRecordValue::Credential(record)) => record,
            Some(_) => return Err(Status::data_loss("credential record has the wrong type")),
            None => return Err(invalid_credentials()),
        };
        let verified = credential
            .verify_secret(&request.client_secret)
            .map_err(credential_status)?
            .ok_or_else(invalid_credentials)?;
        let application = self
            .read_record(&LogicalRecordId::Application {
                app_id: verified.app_id.clone(),
            })
            .await?;
        match application {
            Some(LogicalRecordValue::Application(LogicalApplicationRecord {
                app_id,
                client_id,
                storage_tenant,
            })) if app_id == verified.app_id
                && client_id == verified.client_id
                && storage_tenant == verified.storage_tenant => {}
            Some(_) => return Err(Status::data_loss("application and credential disagree")),
            None => return Err(invalid_credentials()),
        }
        if self.credential_target(&request.client_id)?.is_some() {
            return Err(Status::unavailable(
                "credential placement changed during verification",
            ));
        }
        let access_token = self
            .tokens
            .mint(verified.storage_tenant, verified.app_id)
            .map_err(|_| Status::internal("access token could not be minted"))?;
        Ok(api::AccessToken {
            access_token,
            token_type: "Bearer".into(),
            expires_in_seconds: ACCESS_TOKEN_LIFETIME.as_secs(),
        })
    }

    async fn require_identity_absent(&self, app_id: &str, client_id: &str) -> Result<(), Status> {
        if self
            .read_record(&LogicalRecordId::Application {
                app_id: app_id.to_owned(),
            })
            .await?
            .is_some()
            || self
                .read_record(&LogicalRecordId::Credential {
                    client_id: client_id.to_owned(),
                })
                .await?
                .is_some()
        {
            return Err(Status::already_exists(
                "application or client identity is already claimed",
            ));
        }
        Ok(())
    }

    async fn read_and_verify_credential(
        &self,
        client_id: &str,
        app_id: &str,
        storage_tenant: &StorageTenantId,
        secret: &str,
    ) -> Result<anvil_store::ApplicationCredential, Status> {
        let value = self
            .read_record(&LogicalRecordId::Credential {
                client_id: client_id.to_owned(),
            })
            .await?
            .ok_or_else(|| Status::unavailable("credential replication is incomplete"))?;
        let LogicalRecordValue::Credential(record) = value else {
            return Err(Status::data_loss("credential record has the wrong type"));
        };
        let credential = record
            .verify_secret(secret)
            .map_err(credential_status)?
            .ok_or_else(|| Status::already_exists("credential does not match the request"))?;
        if credential.app_id != app_id || credential.storage_tenant != *storage_tenant {
            return Err(Status::already_exists(
                "credential does not match the request",
            ));
        }
        Ok(credential)
    }

    async fn read_record(
        &self,
        id: &LogicalRecordId,
    ) -> Result<Option<LogicalRecordValue>, Status> {
        let Some(target) = self.logical.read_target(id)? else {
            return self.logical.read(id).await;
        };
        let value = self
            .peers
            .read_coordinated_logical_record(
                target.node_id,
                &target.address,
                id,
                CONTROL_OPERATION_TIMEOUT,
            )
            .await?;
        self.logical.require_read_target(id, &target)?;
        Ok(value)
    }

    async fn ensure_administration_record(
        &self,
        expected: LogicalRecordValue,
        credential_secret: Option<&str>,
    ) -> Result<(), Status> {
        let id = expected.id();
        let Some(current) = self.read_record(&id).await? else {
            return self.write_record(expected).await;
        };
        if current == expected {
            return Ok(());
        }
        if let (
            LogicalRecordValue::Credential(current),
            LogicalRecordValue::Credential(expected),
            Some(secret),
        ) = (&current, &expected, credential_secret)
        {
            let verified = current
                .verify_secret(secret)
                .map_err(credential_status)?
                .ok_or_else(|| Status::already_exists("client identity is already claimed"))?;
            if verified.app_id == expected.app_id()
                && verified.client_id == expected.client_id()
                && verified.storage_tenant == *expected.storage_tenant()
                && verified.active == expected.active()
            {
                // Credential verifiers contain random salt. A matching durable
                // verifier is the exact replay authority and must not rotate.
                return Ok(());
            }
        }
        Err(Status::already_exists(
            "administration record conflicts with the existing resource",
        ))
    }

    async fn write_record(&self, value: LogicalRecordValue) -> Result<(), Status> {
        let id = value.id();
        let Some(target) = self.logical.read_target(&id)? else {
            self.logical.mutate(value).await?;
            return Ok(());
        };
        self.peers
            .coordinate_logical_record(
                target.node_id,
                &target.address,
                &value,
                CONTROL_OPERATION_TIMEOUT,
            )
            .await?;
        self.logical.require_read_target(&id, &target)
    }

    async fn authorize_system(
        &self,
        subject: &ObjectRef,
        object: ObjectRef,
        permission: &'static str,
        denied_message: &'static str,
    ) -> Result<AdministrationAuthorization, Status> {
        let scope = AuthzScope::system();
        let check = AuthorizationCheck::new(subject.clone(), object, permission);
        let (allowed, revision, binding_generation) =
            if let Some(target) = self.system_realm_target()? {
                let result = self
                    .peers
                    .fresh_authorization_check(
                        target.node_id,
                        &target.address,
                        SYSTEM_STABLE_TENANT_ID,
                        &scope,
                        AuthzConsistency::Latest,
                        &check,
                        None,
                        target.placement_fence,
                    )
                    .await?;
                self.require_same_system_realm_target(&target)?;
                result
            } else {
                let result = self
                    .zanzibar
                    .fresh_check_with_generation(
                        SYSTEM_STABLE_TENANT_ID,
                        scope,
                        AuthzConsistency::Latest,
                        check,
                    )
                    .await?;
                self.require_system_realm_coordinator()?;
                result
            };
        require_allowed(allowed, denied_message)?;
        Ok(AdministrationAuthorization {
            revision,
            binding_generation,
        })
    }

    async fn apply_system_grant(
        &self,
        request: TupleBatchRequest,
    ) -> Result<TupleBatchReceipt, Status> {
        let Some(target) = self.system_realm_target()? else {
            return self.coordinate_system_grant(request).await;
        };
        let receipt = self
            .peers
            .coordinate_system_grant(
                target.node_id,
                &target.address,
                &request,
                CONTROL_OPERATION_TIMEOUT,
            )
            .await?;
        self.require_same_system_realm_target(&target)?;
        Ok(receipt)
    }

    async fn authorize_application_management(
        &self,
        caller: &Caller,
    ) -> Result<AdministrationAuthorization, Status> {
        if caller.storage_tenant().is_system() {
            self.authorize_system(
                caller.subject(),
                ObjectRef::opaque("system", anvil_store::SYSTEM_STORAGE_TENANT_ID)
                    .map_err(authz_evaluation_status)?,
                "manage_system",
                "application credential management is not authorized",
            )
            .await
        } else {
            self.authorize_system(
                caller.subject(),
                storage_tenant_resource(caller.storage_tenant().as_str())
                    .map_err(authz_evaluation_status)?,
                "manage_tenant",
                "application credential management is not authorized",
            )
            .await
        }
    }

    async fn authorize_role_management(
        &self,
        caller: &Caller,
        target: &ApplicationRoleTarget,
    ) -> Result<AdministrationAuthorization, Status> {
        match target {
            ApplicationRoleTarget::System(_) if caller.storage_tenant().is_system() => {
                self.authorize_system(
                    caller.subject(),
                    ObjectRef::opaque("system", anvil_store::SYSTEM_STORAGE_TENANT_ID)
                        .map_err(authz_evaluation_status)?,
                    "manage_system",
                    "application role management is not authorized",
                )
                .await
            }
            ApplicationRoleTarget::Tenant(_) if !caller.storage_tenant().is_system() => {
                self.authorize_system(
                    caller.subject(),
                    storage_tenant_resource(caller.storage_tenant().as_str())
                        .map_err(authz_evaluation_status)?,
                    "manage_tenant",
                    "application role management is not authorized",
                )
                .await
            }
            ApplicationRoleTarget::Bucket { bucket, .. }
                if !caller.storage_tenant().is_system() =>
            {
                self.authorize_system(
                    caller.subject(),
                    bucket_resource(caller.storage_tenant().as_str(), bucket)
                        .map_err(authz_evaluation_status)?,
                    "manage_policy",
                    "application role management is not authorized",
                )
                .await
            }
            _ => Err(Status::permission_denied(
                "application role management is not authorized",
            )),
        }
    }

    async fn require_bucket_identity(
        &self,
        storage_tenant: &StorageTenantId,
        bucket: &str,
    ) -> Result<(u64, u64), Status> {
        let tenant_id = match self
            .read_record(&LogicalRecordId::TenantNameClaim {
                storage_tenant: storage_tenant.clone(),
            })
            .await?
        {
            Some(LogicalRecordValue::TenantNameClaim { tenant_id, .. }) => tenant_id,
            Some(_) => return Err(Status::data_loss("tenant-name claim has the wrong type")),
            None => return Err(Status::not_found("storage tenant does not exist")),
        };
        let bucket_id = match self
            .read_record(&LogicalRecordId::BucketNameClaim {
                tenant_id,
                bucket: bucket.to_owned(),
            })
            .await?
        {
            Some(LogicalRecordValue::BucketNameClaim { bucket_id, .. }) => bucket_id,
            Some(_) => return Err(Status::data_loss("bucket-name claim has the wrong type")),
            None => return Err(Status::not_found("bucket does not exist")),
        };
        match self
            .read_record(&LogicalRecordId::BucketRecord {
                tenant_id,
                bucket_id,
            })
            .await?
        {
            Some(LogicalRecordValue::BucketRecord(record))
                if record.storage_tenant == *storage_tenant && record.bucket == bucket =>
            {
                Ok((tenant_id, bucket_id))
            }
            Some(_) => Err(Status::data_loss(
                "bucket record disagrees with its name claim",
            )),
            None => Err(Status::unavailable("bucket replication is incomplete")),
        }
    }

    fn verify_routed_bearer(&self, bearer: &str) -> Result<Caller, Status> {
        self.tokens
            .verify(bearer)
            .map_err(|_| Status::unauthenticated("the bearer token is invalid"))
    }

    fn executor_target(&self) -> Result<Option<ControlTarget>, Status> {
        let placement = self.placement()?;
        let nomination = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("atomic executor state is unavailable"))?
            .executor()
            .ok_or_else(|| Status::unavailable("no atomic executor is nominated"))?;
        if !placement.active_node_ids().contains(&nomination.executor) {
            return Err(Status::unavailable("nominated executor is not ACTIVE"));
        }
        if nomination.executor == self.local_node {
            return Ok(None);
        }
        let address = placement
            .address(nomination.executor)
            .ok_or_else(|| Status::unavailable("nominated executor has no peer address"))?;
        Ok(Some(ControlTarget {
            node_id: nomination.executor,
            address: address.0.clone(),
            placement_fence: placement.fence(),
        }))
    }

    fn require_local_executor(&self) -> Result<(), Status> {
        if self.executor_target()?.is_none() {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "administration request did not reach the nominated executor",
            ))
        }
    }

    fn credential_target(&self, client_id: &str) -> Result<Option<ControlTarget>, Status> {
        if client_id.is_empty() {
            return Err(Status::unauthenticated(
                "the client credentials are invalid",
            ));
        }
        let placement = self.placement()?;
        let node = placement
            .rank(PlacementKind::Credential, client_id.as_bytes())
            .into_iter()
            .next()
            .ok_or_else(|| Status::unavailable("cluster has no credential coordinator"))?;
        if node == self.local_node {
            return Ok(None);
        }
        let address = placement
            .address(node)
            .ok_or_else(|| Status::unavailable("credential coordinator has no peer address"))?;
        Ok(Some(ControlTarget {
            node_id: node,
            address: address.0.clone(),
            placement_fence: placement.fence(),
        }))
    }

    fn system_realm_target(&self) -> Result<Option<ControlTarget>, Status> {
        let placement = self.placement()?;
        let group = MutableRecordReplicaGroup::select(
            PlacementKind::ZanzibarRealm,
            placement.cluster_id(),
            &SYSTEM_STABLE_TENANT_ID.to_be_bytes(),
            placement.placement_nodes(),
        )
        .ok_or_else(|| Status::unavailable("cluster has no system Zanzibar replica"))?;
        let node = group.coordinator();
        if node == self.local_node {
            return Ok(None);
        }
        let address = placement
            .address(node)
            .ok_or_else(|| Status::unavailable("system Zanzibar coordinator has no address"))?;
        Ok(Some(ControlTarget {
            node_id: node,
            address: address.0.clone(),
            placement_fence: placement.fence(),
        }))
    }

    fn require_system_realm_coordinator(&self) -> Result<(), Status> {
        if self.system_realm_target()?.is_none() {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "administration grant did not reach the system Zanzibar coordinator",
            ))
        }
    }

    fn require_same_system_realm_target(&self, expected: &ControlTarget) -> Result<(), Status> {
        if self.system_realm_target()?.as_ref() == Some(expected) {
            Ok(())
        } else {
            Err(Status::unavailable(
                "system Zanzibar placement changed during administration",
            ))
        }
    }

    fn require_local_logical_coordinator(&self, id: &LogicalRecordId) -> Result<(), Status> {
        if self.logical.read_target(id)?.is_none() {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "logical record did not reach its current coordinator",
            ))
        }
    }

    fn placement(&self) -> Result<ClusterPlacement, Status> {
        self.serving.mutation_context()?;
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ControlTarget {
    node_id: NodeId,
    address: String,
    placement_fence: PlacementLogId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdministrationAuthorization {
    revision: AuthzRevision,
    binding_generation: u64,
}

fn require_allowed(allowed: bool, message: &'static str) -> Result<(), Status> {
    if allowed {
        Ok(())
    } else {
        Err(Status::permission_denied(message))
    }
}

fn authz_status(error: anvil_store::AuthzStoreError) -> Status {
    Status::unavailable(error.to_string())
}

fn authz_evaluation_status(error: anvil_authz::AuthorizationError) -> Status {
    Status::internal(error.to_string())
}

fn credential_status(error: anvil_store::CredentialRepositoryError) -> Status {
    use anvil_store::CredentialRepositoryError::*;
    match error {
        InvalidInput(message) => Status::invalid_argument(message),
        AlreadyExists(message) => Status::already_exists(message),
        NotFound(message) => Status::not_found(message),
        Conflict(message) => Status::failed_precondition(message),
        Authorization(error) => authz_status(error),
        AlreadyBootstrapped => Status::already_exists("system bootstrap has already completed"),
        Entropy(_) | Storage(_) => Status::internal(error.to_string()),
    }
}

fn invalid_credentials() -> Status {
    Status::unauthenticated("the client credentials are invalid")
}

fn require_application_value(
    value: &LogicalRecordValue,
    storage_tenant: &StorageTenantId,
    app_id: &str,
    client_id: &str,
) -> Result<(), Status> {
    match value {
        LogicalRecordValue::Application(record)
            if record.storage_tenant == *storage_tenant
                && record.app_id == app_id
                && record.client_id == client_id =>
        {
            Ok(())
        }
        LogicalRecordValue::Application(_) => Err(Status::already_exists(
            "application identity is already claimed",
        )),
        _ => Err(Status::data_loss("application record has the wrong type")),
    }
}

fn require_credential_value<'a>(
    value: &'a LogicalRecordValue,
    storage_tenant: &StorageTenantId,
    app_id: &str,
    client_id: &str,
) -> Result<&'a LogicalCredentialRecord, Status> {
    match value {
        LogicalRecordValue::Credential(record)
            if record.storage_tenant() == storage_tenant
                && record.app_id() == app_id
                && record.client_id() == client_id =>
        {
            Ok(record)
        }
        LogicalRecordValue::Credential(_) => {
            Err(Status::already_exists("client identity is already claimed"))
        }
        _ => Err(Status::data_loss("credential record has the wrong type")),
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

fn credential_state_to_api(
    credential: anvil_store::ApplicationCredential,
    replayed: bool,
) -> api::ApplicationCredentialState {
    api::ApplicationCredentialState {
        storage_tenant: credential.storage_tenant.to_string(),
        app_id: credential.app_id,
        client_id: credential.client_id,
        active: credential.active,
        replayed,
    }
}

fn versioning_from_api(value: i32) -> Result<ObjectVersioning, Status> {
    match api::ObjectVersioning::try_from(value) {
        Ok(api::ObjectVersioning::Unversioned) => Ok(ObjectVersioning::Unversioned),
        Ok(api::ObjectVersioning::Enabled) => Ok(ObjectVersioning::Enabled),
        Err(_) => Err(Status::invalid_argument(
            "object versioning mode is unknown",
        )),
    }
}

fn versioning_to_api(value: ObjectVersioning) -> api::ObjectVersioning {
    match value {
        ObjectVersioning::Unversioned => api::ObjectVersioning::Unversioned,
        ObjectVersioning::Enabled => api::ObjectVersioning::Enabled,
    }
}
