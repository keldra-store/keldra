use super::admin_cursor::{self, AdminCursorBinding};
use crate::admin_audit::{self, AdminAuditEvent, AuditEventFilter};
use crate::anvil_api::admin_service_server::AdminService;
use crate::anvil_api::*;
use crate::mesh_lifecycle::{
    self, CreateHostAliasDescriptor, CreateRegionDescriptor, LifecycleError,
    LifecycleState as CoreLifecycleState, NodeCapability as CoreNodeCapability,
    NodeDrainDescriptor, RegisterCellDescriptor, RegisterNodeDescriptor,
};
use crate::persistence;
use crate::repair_finding::{RepairFinding, RepairSubjectRef};
use crate::routing::{
    self, HostAliasDescriptor as CoreHostAliasDescriptor, HostAliasState as CoreHostAliasState,
    RoutingConfig,
};
use crate::system_realm::{AdminPrincipal, SystemAdminRelation};
use crate::{
    AppState, auth, authz_repair, directory_repair, index_repair, mesh_control_stream,
    mesh_directory,
    persistence::Bucket,
    personaldb_repair,
    personaldb_signing_store::{
        PersonalDbSigningKeyAuditMetadata, PersonalDbSigningKeyImport,
        PersonalDbSigningKeyPublicRecord, PersonalDbSigningKeyStatusUpdate, SensitiveBytes,
    },
};
use chrono::Utc;
use personaldb_protocol::{
    DatabaseId, Ed25519PublicKey, KeyGeneration, KeyTrustPolicy, PublicKeyStatus,
    PublicKeyTrustRecord, SignaturePurpose,
};
use serde_json::json;
use tonic::{Request, Response, Status};

mod rpc_mapping;
pub use rpc_mapping::admin_rpc_relation_mapping;

#[tonic::async_trait]
impl AdminService for AppState {
    async fn create_tenant(
        &self,
        request: Request<CreateTenantRequest>,
    ) -> Result<Response<TenantAdminResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageTenants).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let home_region = if req.home_region.trim().is_empty() {
            self.config.region.clone()
        } else {
            req.home_region.clone()
        };
        let tenant = self
            .persistence
            .create_tenant(&req.name, "admin-created")
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.tenant.create",
            &format!("tenant:{}", tenant.id),
            json!({
                "resource_kind": "tenant",
                "tenant_id": tenant.id,
                "tenant_name": &tenant.name,
                "home_region": &home_region,
            }),
        )
        .await?;
        Ok(Response::new(TenantAdminResponse {
            request_id: context.request_id.clone(),
            tenant: Some(TenantAdminDescriptor {
                tenant_id: tenant.id.to_string(),
                name: tenant.name,
                home_region,
            }),
            audit_event_id,
        }))
    }

    async fn create_application(
        &self,
        request: Request<CreateApplicationRequest>,
    ) -> Result<Response<ApplicationSecretResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageApps).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let tenant_id = resolve_tenant_id(self, &req.tenant_id).await?;
        let client_id = generated_client_id();
        let client_secret = generated_client_secret();
        let encrypted_secret = encrypt_admin_client_secret(self, &client_secret)?;
        let app = self
            .persistence
            .create_app(tenant_id, &req.app_name, &client_id, &encrypted_secret)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.app.create",
            &format!("app:{}", app.client_id),
            json!({
                "resource_kind": "application",
                "tenant_id": tenant_id,
                "app_id": app.id,
                "app_name": &app.name,
                "client_id": &app.client_id,
            }),
        )
        .await?;
        Ok(Response::new(ApplicationSecretResponse {
            request_id: context.request_id.clone(),
            tenant_id: tenant_id.to_string(),
            app_name: app.name,
            client_id: app.client_id,
            client_secret,
            audit_event_id,
            app_id: app.id.to_string(),
        }))
    }

    async fn rotate_application_secret(
        &self,
        request: Request<RotateApplicationSecretRequest>,
    ) -> Result<Response<ApplicationSecretResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageApps).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let tenant_id = resolve_tenant_id(self, &req.tenant_id).await?;
        let app = self
            .persistence
            .get_app_by_tenant_name(tenant_id, &req.app_name)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .ok_or_else(|| Status::not_found("Application not found"))?;
        let client_secret = generated_client_secret();
        let encrypted_secret = encrypt_admin_client_secret(self, &client_secret)?;
        self.persistence
            .update_app_secret(app.id, &encrypted_secret)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.app.secret.rotate",
            &format!("app:{}", app.client_id),
            json!({
                "resource_kind": "application",
                "tenant_id": tenant_id,
                "app_id": app.id,
                "app_name": &app.name,
                "client_id": &app.client_id,
            }),
        )
        .await?;
        Ok(Response::new(ApplicationSecretResponse {
            request_id: context.request_id.clone(),
            tenant_id: tenant_id.to_string(),
            app_name: app.name,
            client_id: app.client_id,
            client_secret,
            audit_event_id,
            app_id: app.id.to_string(),
        }))
    }

    async fn grant_application_policy(
        &self,
        request: Request<GrantApplicationPolicyRequest>,
    ) -> Result<Response<ApplicationPolicyResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManagePolicies).await?;
        let req = request.into_inner();
        let context = require_admin_action_context(req.context.as_ref())?;
        let tenant_id = resolve_tenant_id(self, &req.tenant_id).await?;
        let app = resolve_tenant_app(self, tenant_id, &req.app_name).await?;
        validate_policy_parts(&req.action, &req.resource)?;
        let delegated_action = req
            .action
            .parse::<crate::permissions::AnvilAction>()
            .map_err(|_| Status::invalid_argument("Invalid delegated action"))?;
        crate::access_control::write_delegated_action_tuple(
            &self.storage,
            &self.persistence,
            tenant_id,
            &app.id.to_string(),
            delegated_action,
            &req.resource,
            "add",
            &principal.principal_id,
            "admin access grant",
        )
        .await?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.app.policy.grant",
            &app_resource_id(tenant_id, &app.name),
            json!({
                "resource_kind": "application_policy",
                "tenant_id": tenant_id,
                "app_id": app.id,
                "app_name": &app.name,
                "client_id": &app.client_id,
                "action": &req.action,
                "resource": &req.resource,
            }),
        )
        .await?;
        Ok(Response::new(ApplicationPolicyResponse {
            request_id: context.request_id.clone(),
            tenant_id: tenant_id.to_string(),
            app_name: app.name,
            action: req.action,
            resource: req.resource,
            audit_event_id,
        }))
    }

    async fn revoke_application_policy(
        &self,
        request: Request<RevokeApplicationPolicyRequest>,
    ) -> Result<Response<ApplicationPolicyResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManagePolicies).await?;
        let req = request.into_inner();
        let context = require_admin_action_context(req.context.as_ref())?;
        let tenant_id = resolve_tenant_id(self, &req.tenant_id).await?;
        let app = resolve_tenant_app(self, tenant_id, &req.app_name).await?;
        validate_policy_parts(&req.action, &req.resource)?;
        let delegated_action = req
            .action
            .parse::<crate::permissions::AnvilAction>()
            .map_err(|_| Status::invalid_argument("Invalid delegated action"))?;
        crate::access_control::write_delegated_action_tuple(
            &self.storage,
            &self.persistence,
            tenant_id,
            &app.id.to_string(),
            delegated_action,
            &req.resource,
            "remove",
            &principal.principal_id,
            "admin access revoke",
        )
        .await?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.app.policy.revoke",
            &app_resource_id(tenant_id, &app.name),
            json!({
                "resource_kind": "application_policy",
                "tenant_id": tenant_id,
                "app_id": app.id,
                "app_name": &app.name,
                "client_id": &app.client_id,
                "action": &req.action,
                "resource": &req.resource,
            }),
        )
        .await?;
        Ok(Response::new(ApplicationPolicyResponse {
            request_id: context.request_id.clone(),
            tenant_id: tenant_id.to_string(),
            app_name: app.name,
            action: req.action,
            resource: req.resource,
            audit_event_id,
        }))
    }

    async fn grant_application_policies(
        &self,
        request: Request<ApplicationPoliciesRequest>,
    ) -> Result<Response<ApplicationPoliciesResponse>, Status> {
        mutate_application_policy_batch(
            self,
            request,
            "add",
            "admin.app.policy.batch_grant",
            "admin access batch grant",
        )
        .await
    }

    async fn revoke_application_policies(
        &self,
        request: Request<ApplicationPoliciesRequest>,
    ) -> Result<Response<ApplicationPoliciesResponse>, Status> {
        mutate_application_policy_batch(
            self,
            request,
            "remove",
            "admin.app.policy.batch_revoke",
            "admin access batch revoke",
        )
        .await
    }

    async fn rotate_secret_encryption_key(
        &self,
        request: Request<RotateSecretEncryptionKeyRequest>,
    ) -> Result<Response<SecretEncryptionKeyRotationResponse>, Status> {
        let principal = require_admin(
            &request,
            self,
            SystemAdminRelation::ManageSecretEncryptionKeys,
        )
        .await?;
        let req = request.into_inner();
        let context = require_admin_action_context(req.context.as_ref())?;
        let mut stats = SecretEncryptionRotationStats::default();

        rotate_application_secret_envelopes(self, req.dry_run, &mut stats).await?;
        rotate_hf_secret_envelopes(self, req.dry_run, &mut stats).await?;

        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.secret_encryption_key.rotate",
            "secret_encryption_key",
            json!({
                "resource_kind": "secret_encryption_key",
                "active_key_id": self.secret_keyring.active_key_id(),
                "dry_run": req.dry_run,
                "app_secrets_examined": stats.app_secrets_examined,
                "app_secrets_rotated": stats.app_secrets_rotated,
                "hf_keys_examined": stats.hf_keys_examined,
                "hf_keys_rotated": stats.hf_keys_rotated,
                "already_active": stats.already_active,
            }),
        )
        .await?;

        Ok(Response::new(SecretEncryptionKeyRotationResponse {
            request_id: context.request_id.clone(),
            active_key_id: self.secret_keyring.active_key_id().to_string(),
            dry_run: req.dry_run,
            app_secrets_examined: stats.app_secrets_examined,
            app_secrets_rotated: stats.app_secrets_rotated,
            hf_keys_examined: stats.hf_keys_examined,
            hf_keys_rotated: stats.hf_keys_rotated,
            already_active: stats.already_active,
            audit_event_id,
        }))
    }

    async fn import_personal_db_signing_key(
        &self,
        request: Request<ImportPersonalDbSigningKeyRequest>,
    ) -> Result<Response<PersonalDbSigningKeyResponse>, Status> {
        let principal = require_admin(
            &request,
            self,
            SystemAdminRelation::ManagePersonalDbSigningKeys,
        )
        .await?;
        let mut req = request.into_inner();
        let private_key_pkcs8_der =
            SensitiveBytes::new(std::mem::take(&mut req.private_key_pkcs8_der));
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let purpose = req
            .purpose
            .parse::<SignaturePurpose>()
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let status = parse_personaldb_key_status(&req.status, true)?;
        let public_key = Ed25519PublicKey::try_from(req.public_key.as_slice())
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let policy = KeyTrustPolicy {
            key_generation: KeyGeneration::new(req.key_generation)
                .map_err(|err| Status::invalid_argument(err.to_string()))?,
            purpose,
            database_scopes: req
                .database_scopes
                .into_iter()
                .map(DatabaseId::new)
                .collect(),
            group_scopes: req.group_scopes,
            valid_from_log_index: req.valid_from_log_index,
            valid_until_log_index: req.valid_until_log_index,
            status,
        };
        let trust_record = PublicKeyTrustRecord::new(public_key, policy);
        let import_result = self
            .personaldb_signing_key_store
            .import_key(PersonalDbSigningKeyImport {
                trust_record,
                private_key_pkcs8_der: private_key_pkcs8_der.as_slice(),
                audit: PersonalDbSigningKeyAuditMetadata::new(
                    &principal.principal_id,
                    &context.request_id,
                )
                .with_reason(&context.audit_reason),
            })
            .await;
        let record = import_result.map_err(|err| Status::failed_precondition(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.personaldb_signing_key.import",
            &format!("personaldb_signing_key:{}", record.trust_record.key_id),
            json!({
                "resource_kind": "personaldb_signing_key",
                "key_id": record.trust_record.key_id.as_str(),
                "key_generation": record.trust_record.key_generation.get(),
                "purpose": record.trust_record.purpose.as_str(),
                "status": record.trust_record.status.as_str(),
                "record_revision": record.record_revision,
            }),
        )
        .await?;
        Ok(Response::new(PersonalDbSigningKeyResponse {
            request_id: context.request_id.clone(),
            key: Some(personaldb_signing_key_to_proto(record)),
            audit_event_id,
            runtime_reload_required: true,
        }))
    }

    async fn list_personal_db_signing_keys(
        &self,
        request: Request<ListPersonalDbSigningKeysRequest>,
    ) -> Result<Response<ListPersonalDbSigningKeysResponse>, Status> {
        let principal = require_admin(
            &request,
            self,
            SystemAdminRelation::ManagePersonalDbSigningKeys,
        )
        .await?;
        let req = request.into_inner();
        let page_size = crate::services::collection_cursor::page_size(req.page.as_ref())?;
        let revision = self
            .personaldb_signing_key_store
            .current_collection_revision()
            .map_err(|err| Status::internal(err.to_string()))?;
        let principal_scope = format!(
            "admin:{}/tenant:{}",
            principal.principal_id, principal.tenant_id
        );
        let revision_string = revision.to_string();
        let binding = crate::services::collection_cursor::CollectionCursorBinding {
            service_method: "anvil.AdminService/ListPersonalDbSigningKeys",
            filters: &[],
            principal_scope: &principal_scope,
            page_size,
            revision: &revision_string,
            sort: "key_id.asc",
        };
        let position = crate::services::collection_cursor::decode_page_token(
            req.page.as_ref(),
            &binding,
            self.config.jwt_secret.as_bytes(),
        )?;
        let after_tuple_key =
            crate::services::collection_cursor::decode_binary_position(position.as_deref())?;
        let page = self
            .personaldb_signing_key_store
            .page_public_records(revision, after_tuple_key.as_deref(), page_size)
            .map_err(|err| Status::aborted(err.to_string()))?;
        let next_page_token = page
            .next_tuple_key
            .as_deref()
            .map(crate::services::collection_cursor::encode_binary_position)
            .transpose()?
            .map(|position| {
                crate::services::collection_cursor::encode_next_page_token(
                    &position,
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })
            .transpose()?
            .unwrap_or_default();
        Ok(Response::new(ListPersonalDbSigningKeysResponse {
            keys: page
                .records
                .into_iter()
                .map(personaldb_signing_key_to_proto)
                .collect(),
            page: Some(PageResponse { next_page_token }),
        }))
    }

    async fn set_personal_db_signing_key_status(
        &self,
        request: Request<SetPersonalDbSigningKeyStatusRequest>,
    ) -> Result<Response<PersonalDbSigningKeyResponse>, Status> {
        let principal = require_admin(
            &request,
            self,
            SystemAdminRelation::ManagePersonalDbSigningKeys,
        )
        .await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let key_id = personaldb_protocol::KeyId::new(req.key_id)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let status = parse_personaldb_key_status(&req.status, false)?;
        let current = self
            .personaldb_signing_key_store
            .get_public_record(&key_id)
            .map_err(|err| Status::internal(err.to_string()))?
            .ok_or_else(|| Status::not_found("PersonalDB signing key not found"))?;
        if current.record_revision != context.expected_generation {
            return Err(Status::aborted(format!(
                "PersonalDB signing key record revision mismatch: expected {}, current {}",
                context.expected_generation, current.record_revision
            )));
        }
        let record = self
            .personaldb_signing_key_store
            .set_status(PersonalDbSigningKeyStatusUpdate {
                key_id,
                expected_record_revision: context.expected_generation,
                status,
                valid_until_log_index: req.valid_until_log_index,
                audit: PersonalDbSigningKeyAuditMetadata::new(
                    &principal.principal_id,
                    &context.request_id,
                )
                .with_reason(&context.audit_reason),
            })
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.personaldb_signing_key.status_set",
            &format!("personaldb_signing_key:{}", record.trust_record.key_id),
            json!({
                "resource_kind": "personaldb_signing_key",
                "key_id": record.trust_record.key_id.as_str(),
                "key_generation": record.trust_record.key_generation.get(),
                "purpose": record.trust_record.purpose.as_str(),
                "status": record.trust_record.status.as_str(),
                "valid_until_log_index": record.trust_record.valid_until_log_index,
                "record_revision": record.record_revision,
            }),
        )
        .await?;
        Ok(Response::new(PersonalDbSigningKeyResponse {
            request_id: context.request_id.clone(),
            key: Some(personaldb_signing_key_to_proto(record)),
            audit_event_id,
            runtime_reload_required: true,
        }))
    }

    async fn create_bucket_admin(
        &self,
        request: Request<CreateBucketAdminRequest>,
    ) -> Result<Response<BucketAdminResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageBuckets).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let tenant_id = resolve_tenant_id(self, &req.tenant_id).await?;
        let bucket = self
            .persistence
            .create_bucket(tenant_id, &req.bucket_name, &req.region)
            .await?;
        crate::access_control::grant_bucket_defaults(
            &self.persistence,
            &bucket,
            &principal.principal_id,
            &principal.principal_id,
            "admin bucket create",
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.bucket.create",
            &bucket_resource_id(tenant_id, &bucket.name),
            json!({
                "resource_kind": "bucket",
                "tenant_id": tenant_id,
                "bucket_id": bucket.id,
                "bucket_name": &bucket.name,
                "region": &bucket.region,
                "is_public_read": bucket.is_public_read,
            }),
        )
        .await?;
        Ok(Response::new(BucketAdminResponse {
            request_id: context.request_id.clone(),
            bucket: Some(bucket_to_proto(bucket)),
            audit_event_id,
        }))
    }

    async fn set_bucket_public_access_admin(
        &self,
        request: Request<SetBucketPublicAccessAdminRequest>,
    ) -> Result<Response<BucketAdminResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageBuckets).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let tenant_id = resolve_tenant_id(self, &req.tenant_id).await?;
        self.persistence
            .set_bucket_public_access(tenant_id, &req.bucket_name, req.allow_public_read)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let bucket = self
            .persistence
            .get_bucket_by_name(tenant_id, &req.bucket_name)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.bucket.public_access.set",
            &bucket_resource_id(tenant_id, &bucket.name),
            json!({
                "resource_kind": "bucket",
                "tenant_id": tenant_id,
                "bucket_id": bucket.id,
                "bucket_name": &bucket.name,
                "region": &bucket.region,
                "allow_public_read": req.allow_public_read,
                "is_public_read": bucket.is_public_read,
            }),
        )
        .await?;
        Ok(Response::new(BucketAdminResponse {
            request_id: context.request_id.clone(),
            bucket: Some(bucket_to_proto(bucket)),
            audit_event_id,
        }))
    }

    async fn create_host_alias(
        &self,
        request: Request<CreateHostAliasAdminRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        let principal =
            require_admin(&request, self, SystemAdminRelation::ManageHostAliases).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let request_id = context.request_id.clone();
        let routing_config = routing_config_for_region(self, &req.region).await?;
        let host_alias = self
            .persistence
            .create_host_alias_descriptor(
                &routing_config,
                CreateHostAliasDescriptor {
                    hostname: req.hostname,
                    tenant_id: req.tenant_id,
                    bucket_name: req.bucket_name,
                    region: req.region,
                    prefix: req.prefix,
                },
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.host_alias.create",
            &format!("host_alias:{}", host_alias.hostname),
            host_alias_audit_details(&host_alias),
        )
        .await?;

        Ok(Response::new(HostAliasResponse {
            request_id,
            host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
            audit_event_id,
        }))
    }

    async fn activate_host_alias(
        &self,
        request: Request<ActivateHostAliasRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        let principal =
            require_admin(&request, self, SystemAdminRelation::ManageHostAliases).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let host_alias = self
            .persistence
            .transition_host_alias_descriptor(
                &req.hostname,
                context.expected_generation,
                CoreHostAliasState::Active,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.host_alias.activate",
            &format!("host_alias:{}", host_alias.hostname),
            host_alias_audit_details(&host_alias),
        )
        .await?;

        Ok(Response::new(HostAliasResponse {
            request_id: context.request_id.clone(),
            host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
            audit_event_id,
        }))
    }

    async fn suspend_host_alias(
        &self,
        request: Request<SuspendHostAliasRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        let principal =
            require_admin(&request, self, SystemAdminRelation::ManageHostAliases).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let host_alias = self
            .persistence
            .transition_host_alias_descriptor(
                &req.hostname,
                context.expected_generation,
                CoreHostAliasState::Suspended,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.host_alias.suspend",
            &format!("host_alias:{}", host_alias.hostname),
            host_alias_audit_details(&host_alias),
        )
        .await?;

        Ok(Response::new(HostAliasResponse {
            request_id: context.request_id.clone(),
            host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
            audit_event_id,
        }))
    }

    async fn delete_host_alias(
        &self,
        request: Request<DeleteHostAliasAdminRequest>,
    ) -> Result<Response<AdminMutationResponse>, Status> {
        let principal =
            require_admin(&request, self, SystemAdminRelation::ManageHostAliases).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let host_alias = self
            .persistence
            .transition_host_alias_descriptor(
                &req.hostname,
                context.expected_generation,
                CoreHostAliasState::Deleted,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.host_alias.delete",
            &format!("host_alias:{}", host_alias.hostname),
            host_alias_audit_details(&host_alias),
        )
        .await?;

        Ok(Response::new(AdminMutationResponse {
            request_id: context.request_id.clone(),
            resource_id: host_alias.hostname,
            generation: host_alias.generation,
            audit_event_id,
            idempotent_replay: false,
        }))
    }

    async fn read_host_alias(
        &self,
        request: Request<ReadHostAliasRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        require_admin(&request, self, SystemAdminRelation::ManageHostAliases).await?;
        let req = request.into_inner();
        let host_alias = self
            .persistence
            .get_host_alias_descriptor(&req.hostname)
            .await
            .map_err(lifecycle_status)?
            .ok_or_else(|| Status::not_found("Host alias not found"))?;

        Ok(Response::new(HostAliasResponse {
            request_id: req.request_id,
            host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
            audit_event_id: String::new(),
        }))
    }

    async fn list_host_aliases(
        &self,
        request: Request<ListHostAliasesRequest>,
    ) -> Result<Response<ListHostAliasesResponse>, Status> {
        let principal =
            require_admin(&request, self, SystemAdminRelation::ManageHostAliases).await?;
        let req = request.into_inner();
        let page = req.page.as_ref();
        let limit = page_limit(page)?;
        let mut host_aliases = self
            .persistence
            .list_host_alias_descriptors(none_if_empty(&req.region))
            .await
            .map_err(lifecycle_status)?;
        host_aliases.sort_by(|left, right| left.hostname.cmp(&right.hostname));
        let revision = admin_cursor::collection_revision(
            host_aliases
                .iter()
                .map(|alias| (alias.hostname.as_str(), alias.generation)),
        );
        let filters = [("region", req.region.as_str())];
        let binding = AdminCursorBinding {
            scope: "admin.list_host_aliases.v1",
            filters: &filters,
            principal: &principal,
            limit,
            revision: &revision,
            sort: "hostname.asc",
        };
        let cursor =
            admin_cursor::decode_page_cursor(page, &binding, self.config.jwt_secret.as_bytes())?;
        let mut host_aliases = host_aliases
            .into_iter()
            .filter(|alias| {
                cursor
                    .as_deref()
                    .is_none_or(|cursor| alias.hostname.as_str() > cursor)
            })
            .take(limit + 1)
            .collect::<Vec<_>>();
        let has_more = host_aliases.len() > limit;
        if has_more {
            host_aliases.truncate(limit);
        }
        let next_cursor = if has_more {
            host_aliases.last().map_or(Ok(String::new()), |alias| {
                admin_cursor::encode_next_cursor(
                    &alias.hostname,
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })?
        } else {
            String::new()
        };

        Ok(Response::new(ListHostAliasesResponse {
            page: Some(PageResponse {
                next_page_token: next_cursor,
            }),
            host_aliases: host_aliases
                .into_iter()
                .map(host_alias_descriptor_to_proto)
                .collect(),
        }))
    }

    async fn create_region(
        &self,
        request: Request<CreateRegionRequest>,
    ) -> Result<Response<RegionResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let region = self
            .persistence
            .create_region_descriptor(CreateRegionDescriptor {
                mesh_id: self.config.mesh_id.clone(),
                region: req.region,
                public_base_url: req.public_base_url,
                virtual_host_suffix: req.virtual_host_suffix,
                placement_weight: req.placement_weight,
                default_cell: empty_to_none(req.default_cell),
            })
            .await
            .map_err(lifecycle_status)?;
        crate::access_control::grant_region_defaults(
            &self.persistence,
            &region.region,
            &principal.principal_id,
            "admin region create",
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.region.create",
            &format!("region:{}", region.region),
            region_audit_details(&region),
        )
        .await?;
        Ok(Response::new(RegionResponse {
            request_id: context.request_id.clone(),
            region: Some(region_descriptor_to_proto(region)),
            audit_event_id,
        }))
    }

    async fn activate_region(
        &self,
        request: Request<ActivateRegionRequest>,
    ) -> Result<Response<RegionResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let checkpoint =
            mesh_lifecycle::parse_activation_checkpoint_json(&req.activation_checkpoint_json)
                .map_err(lifecycle_status)?;
        let region = self
            .persistence
            .activate_region_descriptor(&req.region, context.expected_generation, &checkpoint)
            .await
            .map_err(lifecycle_status)?;
        let mut details = region_audit_details(&region);
        add_audit_detail(&mut details, "activation_checkpoint", json!(&checkpoint));
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.region.activate",
            &format!("region:{}", region.region),
            details,
        )
        .await?;
        Ok(Response::new(RegionResponse {
            request_id: context.request_id.clone(),
            region: Some(region_descriptor_to_proto(region)),
            audit_event_id,
        }))
    }

    async fn set_region_read_only(
        &self,
        request: Request<SetRegionReadOnlyRequest>,
    ) -> Result<Response<RegionResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let region = self
            .persistence
            .transition_region_descriptor(
                &req.region,
                context.expected_generation,
                CoreLifecycleState::ReadOnly,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.region.read_only.set",
            &format!("region:{}", region.region),
            region_audit_details(&region),
        )
        .await?;
        Ok(Response::new(RegionResponse {
            request_id: context.request_id.clone(),
            region: Some(region_descriptor_to_proto(region)),
            audit_event_id,
        }))
    }

    async fn drain_region(
        &self,
        request: Request<DrainRegionRequest>,
    ) -> Result<Response<DrainOperationResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let default_disposition =
            region_drain_disposition_from_proto(req.default_disposition, true)?;
        let bucket_overrides = req
            .bucket_overrides
            .iter()
            .map(bucket_drain_override_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let region = self
            .persistence
            .transition_region_descriptor(
                &req.region,
                context.expected_generation,
                CoreLifecycleState::Draining,
            )
            .await
            .map_err(lifecycle_status)?;
        let drain_report = self
            .persistence
            .apply_region_drain_plan(&region.region, default_disposition, bucket_overrides)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))?;
        let mut details = region_audit_details(&region);
        add_audit_detail(
            &mut details,
            "default_disposition",
            json!(default_disposition.as_str()),
        );
        add_audit_detail(
            &mut details,
            "default_disposition_code",
            json!(req.default_disposition),
        );
        add_audit_detail(
            &mut details,
            "bucket_overrides",
            json!(bucket_drain_overrides_details(&req.bucket_overrides)),
        );
        add_audit_detail(
            &mut details,
            "bucket_disposition_decisions",
            region_drain_plan_details(&drain_report),
        );
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.region.drain",
            &format!("region:{}", region.region),
            details,
        )
        .await?;
        for decision in &drain_report.decisions {
            record_admin_audit_event_with_suffix(
                self,
                &principal,
                context,
                "admin.region.bucket_disposition",
                &format!(
                    "tenant:{}:bucket:{}:region:{}",
                    decision.tenant_id, decision.bucket_name, drain_report.region
                ),
                json!({
                    "region": &drain_report.region,
                    "tenant_id": &decision.tenant_id,
                    "bucket_name": &decision.bucket_name,
                    "disposition": decision.disposition.as_str(),
                    "reason": &decision.reason,
                    "expires_at": decision.expires_at.as_deref(),
                    "status_before": format!("{:?}", decision.status_before),
                    "status_after": format!("{:?}", decision.status_after),
                    "bucket_locator_generation_before": decision.bucket_locator_generation_before,
                    "bucket_locator_generation_after": decision.bucket_locator_generation_after,
                    "exception_written": decision.exception_written,
                    "locator_updated": decision.locator_updated,
                }),
                &format!(
                    "bucket-disposition-{}-{}",
                    decision.tenant_id, decision.bucket_name
                ),
            )
            .await?;
        }
        Ok(Response::new(DrainOperationResponse {
            request_id: context.request_id.clone(),
            resource_id: region.region,
            state: lifecycle_state_to_proto(region.state),
            generation: region.generation,
            audit_event_id,
        }))
    }

    async fn remove_region(
        &self,
        request: Request<RemoveRegionRequest>,
    ) -> Result<Response<AdminMutationResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let region = self
            .persistence
            .transition_region_descriptor(
                &req.region,
                context.expected_generation,
                CoreLifecycleState::Removed,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.region.remove",
            &format!("region:{}", region.region),
            region_audit_details(&region),
        )
        .await?;
        Ok(Response::new(AdminMutationResponse {
            request_id: context.request_id.clone(),
            resource_id: region.region,
            generation: region.generation,
            audit_event_id,
            idempotent_replay: false,
        }))
    }

    async fn list_regions(
        &self,
        request: Request<ListRegionsRequest>,
    ) -> Result<Response<ListRegionsResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let req = request.into_inner();
        let page = req.page.as_ref();
        let limit = page_limit(page)?;
        let mut regions = self
            .persistence
            .list_region_descriptors()
            .await
            .map_err(lifecycle_status)?;
        regions.sort_by(|left, right| left.region.cmp(&right.region));
        let revision = admin_cursor::collection_revision(
            regions
                .iter()
                .map(|region| (region.region.as_str(), region.generation)),
        );
        let binding = AdminCursorBinding {
            scope: "admin.list_regions.v1",
            filters: &[],
            principal: &principal,
            limit,
            revision: &revision,
            sort: "region.asc",
        };
        let cursor =
            admin_cursor::decode_page_cursor(page, &binding, self.config.jwt_secret.as_bytes())?;
        let mut regions = regions
            .into_iter()
            .filter(|region| {
                cursor
                    .as_deref()
                    .is_none_or(|cursor| region.region.as_str() > cursor)
            })
            .take(limit + 1)
            .map(region_descriptor_to_proto)
            .collect::<Vec<_>>();
        let has_more = regions.len() > limit;
        if has_more {
            regions.truncate(limit);
        }
        let next_cursor = if has_more {
            regions.last().map_or(Ok(String::new()), |region| {
                admin_cursor::encode_next_cursor(
                    &region.region,
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })?
        } else {
            String::new()
        };
        Ok(Response::new(ListRegionsResponse {
            page: Some(PageResponse {
                next_page_token: next_cursor,
            }),
            regions,
        }))
    }

    async fn register_cell(
        &self,
        request: Request<RegisterCellRequest>,
    ) -> Result<Response<CellResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let cell = self
            .persistence
            .register_cell_descriptor(RegisterCellDescriptor {
                mesh_id: self.config.mesh_id.clone(),
                region: req.region,
                cell_id: req.cell_id,
                placement_weight: req.placement_weight,
                failure_domain: req.failure_domain,
            })
            .await
            .map_err(lifecycle_status)?;
        crate::access_control::grant_cell_defaults(
            &self.persistence,
            &cell.region,
            &cell.cell_id,
            &principal.principal_id,
            "admin cell register",
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.cell.register",
            &cell_resource_id(&cell.region, &cell.cell_id),
            cell_audit_details(&cell),
        )
        .await?;
        Ok(Response::new(CellResponse {
            request_id: context.request_id.clone(),
            cell: Some(cell_descriptor_to_proto(cell)),
            audit_event_id,
        }))
    }

    async fn activate_cell(
        &self,
        request: Request<ActivateCellRequest>,
    ) -> Result<Response<CellResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let cell = self
            .persistence
            .transition_cell_descriptor(
                &req.region,
                &req.cell_id,
                context.expected_generation,
                CoreLifecycleState::Active,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.cell.activate",
            &cell_resource_id(&cell.region, &cell.cell_id),
            cell_audit_details(&cell),
        )
        .await?;
        Ok(Response::new(CellResponse {
            request_id: context.request_id.clone(),
            cell: Some(cell_descriptor_to_proto(cell)),
            audit_event_id,
        }))
    }

    async fn drain_cell(
        &self,
        request: Request<DrainCellRequest>,
    ) -> Result<Response<DrainOperationResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let cell = self
            .persistence
            .transition_cell_descriptor(
                &req.region,
                &req.cell_id,
                context.expected_generation,
                CoreLifecycleState::Draining,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.cell.drain",
            &cell_resource_id(&cell.region, &cell.cell_id),
            cell_audit_details(&cell),
        )
        .await?;
        Ok(Response::new(DrainOperationResponse {
            request_id: context.request_id.clone(),
            resource_id: cell.cell_id,
            state: lifecycle_state_to_proto(cell.state),
            generation: cell.generation,
            audit_event_id,
        }))
    }

    async fn remove_cell(
        &self,
        request: Request<RemoveCellRequest>,
    ) -> Result<Response<AdminMutationResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let cell = self
            .persistence
            .transition_cell_descriptor(
                &req.region,
                &req.cell_id,
                context.expected_generation,
                CoreLifecycleState::Removed,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.cell.remove",
            &cell_resource_id(&cell.region, &cell.cell_id),
            cell_audit_details(&cell),
        )
        .await?;
        Ok(Response::new(AdminMutationResponse {
            request_id: context.request_id.clone(),
            resource_id: cell.cell_id,
            generation: cell.generation,
            audit_event_id,
            idempotent_replay: false,
        }))
    }

    async fn list_cells(
        &self,
        request: Request<ListCellsRequest>,
    ) -> Result<Response<ListCellsResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageRegions).await?;
        let req = request.into_inner();
        let region_filter = none_if_empty(&req.region);
        let page = req.page.as_ref();
        let limit = page_limit(page)?;
        let mut cells = self
            .persistence
            .list_cell_descriptors(region_filter)
            .await
            .map_err(lifecycle_status)?;
        cells.sort_by(|left, right| {
            left.region
                .cmp(&right.region)
                .then(left.cell_id.cmp(&right.cell_id))
        });
        let revision_keys = cells
            .iter()
            .map(|cell| (format!("{}/{}", cell.region, cell.cell_id), cell.generation))
            .collect::<Vec<_>>();
        let revision = admin_cursor::collection_revision(
            revision_keys
                .iter()
                .map(|(key, generation)| (key.as_str(), *generation)),
        );
        let filters = [("region", req.region.as_str())];
        let binding = AdminCursorBinding {
            scope: "admin.list_cells.v1",
            filters: &filters,
            principal: &principal,
            limit,
            revision: &revision,
            sort: "region_cell.asc",
        };
        let cursor =
            admin_cursor::decode_page_cursor(page, &binding, self.config.jwt_secret.as_bytes())?;
        let mut cells = cells
            .into_iter()
            .filter(|cell| {
                cursor.as_deref().is_none_or(|cursor| {
                    format!("{}/{}", cell.region, cell.cell_id).as_str() > cursor
                })
            })
            .take(limit + 1)
            .map(cell_descriptor_to_proto)
            .collect::<Vec<_>>();
        let has_more = cells.len() > limit;
        if has_more {
            cells.truncate(limit);
        }
        let next_cursor = if has_more {
            cells.last().map_or(Ok(String::new()), |cell| {
                admin_cursor::encode_next_cursor(
                    &format!("{}/{}", cell.region, cell.cell_id),
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })?
        } else {
            String::new()
        };
        Ok(Response::new(ListCellsResponse {
            page: Some(PageResponse {
                next_page_token: next_cursor,
            }),
            cells,
        }))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<NodeResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageNodes).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let capabilities = req
            .capabilities
            .into_iter()
            .map(node_capability_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let node = self
            .persistence
            .register_node_descriptor(RegisterNodeDescriptor {
                mesh_id: self.config.mesh_id.clone(),
                node_id: req.node_id,
                region: req.region,
                cell_id: req.cell_id,
                receipt_signing_public_key: req.receipt_signing_public_key,
                public_api_addr: req.public_api_addr,
                capabilities,
                capacity_json: req.capacity_json,
            })
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.node.register",
            &format!("node:{}", node.node_id),
            node_audit_details(&node),
        )
        .await?;
        Ok(Response::new(NodeResponse {
            request_id: context.request_id.clone(),
            node: Some(node_descriptor_to_proto(node)),
            audit_event_id,
        }))
    }

    async fn get_local_node_descriptor(
        &self,
        request: Request<GetLocalNodeDescriptorRequest>,
    ) -> Result<Response<NodeResponse>, Status> {
        require_admin(&request, self, SystemAdminRelation::ManageNodes).await?;
        let request_id = request
            .metadata()
            .get(crate::middleware::ANVIL_REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // This RPC is used by the Docker topology bootstrap before lifecycle
        // projections and control streams are initialised. Build the descriptor
        // from local configuration and signing state instead of reading mesh
        // lifecycle storage, otherwise early boot can recurse through CoreStore/
        // control-stream bootstrap while the node is still trying to join the mesh.
        let now = Utc::now().to_rfc3339();
        let node = mesh_lifecycle::NodeDescriptor {
            schema: mesh_lifecycle::NODE_DESCRIPTOR_SCHEMA.to_string(),
            mesh_id: self.config.mesh_id.clone(),
            node_id: self.config.node_id.clone(),
            region: self.config.region.clone(),
            cell_id: self.config.cell_id.clone(),
            receipt_signing_public_key: self.core_store.local_receipt_signing_public_key(),
            public_api_addr: self.config.public_api_addr.clone(),
            capabilities: vec![
                CoreNodeCapability::Object,
                CoreNodeCapability::Index,
                CoreNodeCapability::PersonalDb,
                CoreNodeCapability::Metadata,
                CoreNodeCapability::Gateway,
                CoreNodeCapability::Admin,
            ],
            capacity_json_hash: mesh_lifecycle::capacity_json_hash("{}")
                .map_err(lifecycle_status)?,
            state: CoreLifecycleState::Joining,
            drain: None,
            last_heartbeat_at: None,
            created_at: now.clone(),
            updated_at: now,
            generation: 0,
        };
        Ok(Response::new(NodeResponse {
            request_id,
            node: Some(node_descriptor_to_proto(node)),
            audit_event_id: String::new(),
        }))
    }

    async fn activate_node(
        &self,
        request: Request<ActivateNodeRequest>,
    ) -> Result<Response<NodeResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageNodes).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let node = self
            .persistence
            .transition_node_descriptor(
                &req.node_id,
                context.expected_generation,
                CoreLifecycleState::Active,
                None,
            )
            .await
            .map_err(lifecycle_status)?;
        crate::access_control::grant_node_defaults(
            &self.persistence,
            &node.region,
            &node.cell_id,
            &node.node_id,
            &principal.principal_id,
            "admin node activate",
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
        crate::access_control::grant_internal_node_system_access(
            &self.persistence,
            &node.node_id,
            &principal.principal_id,
            "admin node activate",
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.node.activate",
            &format!("node:{}", node.node_id),
            node_audit_details(&node),
        )
        .await?;
        Ok(Response::new(NodeResponse {
            request_id: context.request_id.clone(),
            node: Some(node_descriptor_to_proto(node)),
            audit_event_id,
        }))
    }

    async fn drain_node(
        &self,
        request: Request<DrainNodeRequest>,
    ) -> Result<Response<DrainOperationResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageNodes).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let node = self
            .persistence
            .transition_node_descriptor(
                &req.node_id,
                context.expected_generation,
                CoreLifecycleState::Draining,
                Some(NodeDrainDescriptor {
                    started_at: chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    graceful_timeout_ms: req.graceful_timeout_ms,
                    force_after_timeout: req.force_after_timeout,
                }),
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.node.drain",
            &format!("node:{}", node.node_id),
            node_audit_details(&node),
        )
        .await?;
        Ok(Response::new(DrainOperationResponse {
            request_id: context.request_id.clone(),
            resource_id: node.node_id,
            state: lifecycle_state_to_proto(node.state),
            generation: node.generation,
            audit_event_id,
        }))
    }

    async fn force_offline_node(
        &self,
        request: Request<ForceOfflineNodeRequest>,
    ) -> Result<Response<NodeResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageNodes).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let node = self
            .persistence
            .transition_node_descriptor(
                &req.node_id,
                context.expected_generation,
                CoreLifecycleState::Offline,
                None,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.node.force_offline",
            &format!("node:{}", node.node_id),
            node_audit_details(&node),
        )
        .await?;
        Ok(Response::new(NodeResponse {
            request_id: context.request_id.clone(),
            node: Some(node_descriptor_to_proto(node)),
            audit_event_id,
        }))
    }

    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<AdminMutationResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageNodes).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let node = self
            .persistence
            .transition_node_descriptor(
                &req.node_id,
                context.expected_generation,
                CoreLifecycleState::Removed,
                None,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.node.remove",
            &format!("node:{}", node.node_id),
            node_audit_details(&node),
        )
        .await?;
        Ok(Response::new(AdminMutationResponse {
            request_id: context.request_id.clone(),
            resource_id: node.node_id,
            generation: node.generation,
            audit_event_id,
            idempotent_replay: false,
        }))
    }

    async fn list_nodes(
        &self,
        request: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageNodes).await?;
        let req = request.into_inner();
        let page = req.page.as_ref();
        let limit = page_limit(page)?;
        let mut nodes = self
            .persistence
            .list_node_descriptors(none_if_empty(&req.region), none_if_empty(&req.cell_id))
            .await
            .map_err(lifecycle_status)?;
        nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        let revision = admin_cursor::collection_revision(
            nodes
                .iter()
                .map(|node| (node.node_id.as_str(), node.generation)),
        );
        let filters = [
            ("region", req.region.as_str()),
            ("cell_id", req.cell_id.as_str()),
        ];
        let binding = AdminCursorBinding {
            scope: "admin.list_nodes.v1",
            filters: &filters,
            principal: &principal,
            limit,
            revision: &revision,
            sort: "node_id.asc",
        };
        let cursor =
            admin_cursor::decode_page_cursor(page, &binding, self.config.jwt_secret.as_bytes())?;
        let mut nodes = nodes
            .into_iter()
            .filter(|node| {
                cursor
                    .as_deref()
                    .is_none_or(|cursor| node.node_id.as_str() > cursor)
            })
            .take(limit + 1)
            .map(node_descriptor_to_proto)
            .collect::<Vec<_>>();
        let has_more = nodes.len() > limit;
        if has_more {
            nodes.truncate(limit);
        }
        let next_cursor = if has_more {
            nodes.last().map_or(Ok(String::new()), |node| {
                admin_cursor::encode_next_cursor(
                    &node.node_id,
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })?
        } else {
            String::new()
        };
        Ok(Response::new(ListNodesResponse {
            page: Some(PageResponse {
                next_page_token: next_cursor,
            }),
            nodes,
        }))
    }

    async fn list_routing_records(
        &self,
        request: Request<ListRoutingRecordsRequest>,
    ) -> Result<Response<ListRoutingRecordsResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageRouting).await?;
        let req = request.into_inner();
        let family = routing_record_family_from_proto(req.family)?;
        let page = req.page.as_ref();
        let limit = page_limit(page)?;
        let mut records = self
            .persistence
            .list_mesh_routing_records(family)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        records.sort_by(|left, right| left.descriptor_key.cmp(&right.descriptor_key));
        let revision = admin_cursor::collection_revision(
            records
                .iter()
                .map(|record| (record.descriptor_key.as_str(), record.generation)),
        );
        let family_filter = req.family.to_string();
        let filters = [("family", family_filter.as_str())];
        let binding = AdminCursorBinding {
            scope: "admin.list_routing_records.v1",
            filters: &filters,
            principal: &principal,
            limit,
            revision: &revision,
            sort: "descriptor_key.asc",
        };
        let cursor =
            admin_cursor::decode_page_cursor(page, &binding, self.config.jwt_secret.as_bytes())?;
        let mut records = records
            .into_iter()
            .filter(|record| {
                cursor
                    .as_deref()
                    .is_none_or(|cursor| record.descriptor_key.as_str() > cursor)
            })
            .take(limit + 1)
            .map(routing_record_descriptor_to_proto)
            .collect::<Vec<_>>();
        let has_more = records.len() > limit;
        if has_more {
            records.truncate(limit);
        }
        let next_cursor = if has_more {
            records.last().map_or(Ok(String::new()), |record| {
                admin_cursor::encode_next_cursor(
                    &record.descriptor_key,
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })?
        } else {
            String::new()
        };

        Ok(Response::new(ListRoutingRecordsResponse {
            page: Some(PageResponse {
                next_page_token: next_cursor,
            }),
            records,
        }))
    }

    async fn repair_routing_record(
        &self,
        request: Request<RepairRoutingRecordRequest>,
    ) -> Result<Response<AdminMutationResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageRouting).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let family = routing_record_family_from_proto(req.family)?
            .ok_or_else(|| Status::invalid_argument("routing record family is required"))?;
        let record = self
            .persistence
            .repair_mesh_routing_record(family, &req.record_key)
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))?;
        let audit_event_id = record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.routing_record.repair",
            &format!("routing_record:{}", record.descriptor_key),
            json!({
                "resource_kind": "routing_record",
                "family": record.family,
                "record_key": &record.record_key,
                "partition": &record.partition,
                "descriptor_key": &record.descriptor_key,
                "generation": record.generation,
                "payload_json": &record.payload_json,
            }),
        )
        .await?;

        Ok(Response::new(AdminMutationResponse {
            request_id: context.request_id.clone(),
            resource_id: record.descriptor_key,
            generation: record.generation,
            audit_event_id,
            idempotent_replay: false,
        }))
    }

    async fn run_repair(
        &self,
        request: Request<RunRepairRequest>,
    ) -> Result<Response<RepairTaskResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::RunRepair).await?;
        let req = request.into_inner();
        let context = require_admin_action_context(req.context.as_ref())?;
        let request_id = context.request_id.clone();
        let audit_event_id = audit_event_id(&principal, context);

        let response = match req.repair_kind {
            1 => run_index_repair(self, &request_id, &audit_event_id, &req).await?,
            2 => run_directory_index_repair(self, &request_id, &audit_event_id, &req).await?,
            3 => run_authz_derived_index_repair(self, &request_id, &audit_event_id, &req).await?,
            4 => run_personaldb_log_chain_repair(self, &request_id, &audit_event_id, &req).await?,
            5 => run_mesh_routing_projection_repair(self, &request_id, &audit_event_id).await?,
            _ => {
                return Err(Status::invalid_argument(
                    "repair_kind must select a supported repair backend",
                ));
            }
        };
        record_admin_audit_event(
            self,
            &principal,
            context,
            "admin.repair.run",
            &response.repair_task_id,
            json!({
                "repair_kind": req.repair_kind,
                "scope_kind": &response.scope_kind,
                "scope_id": &response.scope_id,
                "status": &response.status,
                "repair_task_details_json": &response.details_json,
            }),
        )
        .await?;

        Ok(Response::new(response))
    }

    async fn list_diagnostics(
        &self,
        request: Request<ListDiagnosticsRequest>,
    ) -> Result<Response<DiagnosticsResponse>, Status> {
        read_handlers::list_diagnostics(self, request).await
    }

    async fn list_audit_events(
        &self,
        request: Request<ListAuditEventsRequest>,
    ) -> Result<Response<AuditEventsResponse>, Status> {
        read_handlers::list_audit_events(self, request).await
    }

    async fn list_storage_classes(
        &self,
        request: Request<ListStorageClassesRequest>,
    ) -> Result<Response<ListStorageClassesResponse>, Status> {
        read_handlers::list_storage_classes(self, request).await
    }

    async fn get_storage_class(
        &self,
        request: Request<GetStorageClassRequest>,
    ) -> Result<Response<StorageClassResponse>, Status> {
        read_handlers::get_storage_class(self, request).await
    }
}

mod helpers;
mod read_handlers;
use helpers::*;
