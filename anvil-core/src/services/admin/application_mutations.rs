use super::*;

impl AppState {
    pub(super) async fn create_tenant_admin(
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
        let audit_event = build_admin_audit_event(
            &principal,
            context,
            "admin.tenant.create",
            &format!("tenant-name:{}", req.name),
            json!({
                "resource_kind": "tenant",
                "tenant_name": &req.name,
                "home_region": &home_region,
            }),
        )?;
        let audit_event_id = audit_event.audit_event_id.clone();
        let transaction =
            begin_admin_product_transaction(self, &principal, context, "tenant-create").await?;
        let input_hash = admin_tenant_input_hash(&req.name, &home_region);
        let now = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
        let tenant = if transaction.replayed {
            stage_or_verify_admin_tenant_result(self, &transaction, &input_hash, now)?;
            self.persistence
                .get_tenant_by_name(&req.name)
                .await
                .map_err(|err| Status::internal(err.to_string()))?
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "committed admin tenant transaction is missing its tenant",
                    )
                })?
        } else {
            let tenant = match crate::control_journal::read_tenant_by_name_in_transaction(
                &self.mvcc,
                &transaction.transaction_id,
                &transaction.principal,
                &req.name,
            )
            .map_err(|err| Status::internal(err.to_string()))?
            {
                Some(tenant) => tenant,
                None => crate::control_journal::plan_create_tenant_in_transaction(
                    &self.mvcc,
                    &transaction.transaction_id,
                    &transaction.principal,
                    &req.name,
                    &audit_event,
                )
                .map_err(|err| Status::internal(err.to_string()))?
                .stage(
                    &self.mvcc,
                    &transaction.transaction_id,
                    &transaction.principal,
                    now,
                )
                .await
                .map_err(|err| Status::failed_precondition(err.to_string()))?,
            };
            let locator_job =
                crate::tenant_locator_finalization_job::TenantLocatorFinalizationJob {
                    cluster_id: self.mvcc.cluster_id().to_string(),
                    transaction_id: transaction.transaction_id.clone(),
                    tenant: tenant.clone(),
                    idempotency_key: context.idempotency_key.trim().to_string(),
                    home_region: home_region.clone(),
                };
            self.mvcc
                .stage_product_mutations(
                    &transaction.transaction_id,
                    &transaction.principal,
                    vec![
                        locator_job
                            .mutation()
                            .map_err(|err| Status::internal(err.to_string()))?,
                    ],
                    now,
                )
                .map_err(|err| Status::failed_precondition(err.to_string()))?;
            stage_or_verify_admin_tenant_result(self, &transaction, &input_hash, now)?;
            commit_admin_product_transaction(self, &transaction).await?;
            tenant
        };
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

    pub(super) async fn create_application_admin(
        &self,
        request: Request<CreateApplicationRequest>,
    ) -> Result<Response<ApplicationSecretResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageApps).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), true)?;
        let tenant_id = resolve_tenant_id(self, &req.tenant_id).await?;
        let transaction =
            begin_admin_product_transaction(self, &principal, context, "application-create")
                .await?;
        let input_hash =
            admin_application_input_hash("create", tenant_id, &req.app_name, &context.request_id);
        if transaction.replayed {
            let result = replay_admin_application_result(self, &transaction, &input_hash)?
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "committed admin application transaction is missing its response record",
                    )
                })?;
            return admin_application_response(self, result).map(Response::new);
        }
        let prior_result = replay_admin_application_result(self, &transaction, &input_hash)?;
        let client_id = prior_result
            .as_ref()
            .map(|result| result.client_id.clone())
            .unwrap_or_else(|| {
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"anvil.admin.application-client-id.v1");
                hasher.update(transaction.transaction_id.as_bytes());
                format!("app_{}", hex::encode(&hasher.finalize().as_bytes()[..16]))
            });
        let existing_app = crate::control_journal::read_app_by_tenant_name_in_transaction(
            &self.mvcc,
            &transaction.transaction_id,
            &transaction.principal,
            tenant_id,
            &req.app_name,
        )
        .map_err(|err| Status::internal(err.to_string()))?;
        let encrypted_secret = match prior_result.as_ref() {
            Some(result) => result.encrypted_secret.clone(),
            None if existing_app.is_some() => existing_app
                .as_ref()
                .expect("checked above")
                .client_secret_encrypted
                .clone(),
            None => encrypt_admin_client_secret(self, &generated_client_secret())?,
        };
        let audit_event = build_admin_audit_event(
            &principal,
            context,
            "admin.app.create",
            &format!("app:{client_id}"),
            json!({
                "resource_kind": "application",
                "tenant_id": tenant_id,
                "app_name": &req.app_name,
                "client_id": &client_id,
            }),
        )?;
        let audit_event_id = audit_event.audit_event_id.clone();
        let now = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
        let app = if let Some(existing) = existing_app {
            if existing.app.client_id != client_id
                || existing.client_secret_encrypted != encrypted_secret
            {
                return Err(Status::already_exists(
                    "admin application transaction contains different staged input",
                ));
            }
            existing.app
        } else {
            crate::control_journal::plan_create_app_in_transaction(
                &self.mvcc,
                &transaction.transaction_id,
                &transaction.principal,
                tenant_id,
                &req.app_name,
                &client_id,
                &encrypted_secret,
                None,
            )
            .and_then(|plan| plan.with_admin_audit(&audit_event, &transaction.transaction_id))
            .map_err(|err| Status::internal(err.to_string()))?
            .stage(
                &self.mvcc,
                &transaction.transaction_id,
                &transaction.principal,
                now,
            )
            .await
            .map_err(|err| Status::failed_precondition(err.to_string()))?
        };
        let result = prior_result.unwrap_or(AdminApplicationMutationResult {
            input_hash,
            request_id: context.request_id.clone(),
            tenant_id,
            app_id: app.id,
            app_name: app.name,
            client_id: app.client_id,
            encrypted_secret,
            audit_event_id,
        });
        stage_admin_application_result(self, &transaction, &result, now)?;
        commit_admin_product_transaction(self, &transaction).await?;
        admin_application_response(self, result).map(Response::new)
    }

    pub(super) async fn rotate_application_secret_admin(
        &self,
        request: Request<RotateApplicationSecretRequest>,
    ) -> Result<Response<ApplicationSecretResponse>, Status> {
        let principal = require_admin(&request, self, SystemAdminRelation::ManageApps).await?;
        let req = request.into_inner();
        let context = require_mutation_context(req.context.as_ref(), false)?;
        let tenant_id = resolve_tenant_id(self, &req.tenant_id).await?;
        let transaction =
            begin_admin_product_transaction(self, &principal, context, "application-secret-rotate")
                .await?;
        let input_hash =
            admin_application_input_hash("rotate", tenant_id, &req.app_name, &context.request_id);
        if transaction.replayed {
            let result = replay_admin_application_result(self, &transaction, &input_hash)?
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "committed admin secret rotation is missing its response record",
                    )
                })?;
            return admin_application_response(self, result).map(Response::new);
        }
        let prior_result = replay_admin_application_result(self, &transaction, &input_hash)?;
        let app = crate::control_journal::read_app_by_tenant_name_in_transaction(
            &self.mvcc,
            &transaction.transaction_id,
            &transaction.principal,
            tenant_id,
            &req.app_name,
        )
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::not_found("Application not found"))?
        .app;
        let encrypted_secret = match prior_result.as_ref() {
            Some(result) => result.encrypted_secret.clone(),
            None => encrypt_admin_client_secret(self, &generated_client_secret())?,
        };
        let audit_event = build_admin_audit_event(
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
        )?;
        let audit_event_id = audit_event.audit_event_id.clone();
        let now = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
        let app = crate::control_journal::plan_update_app_secret_in_transaction(
            &self.mvcc,
            &transaction.transaction_id,
            &transaction.principal,
            app.id,
            &encrypted_secret,
            None,
        )
        .and_then(|plan| plan.with_admin_audit(&audit_event, &transaction.transaction_id))
        .map_err(|err| Status::internal(err.to_string()))?
        .stage(
            &self.mvcc,
            &transaction.transaction_id,
            &transaction.principal,
            now,
        )
        .await
        .map_err(|err| Status::failed_precondition(err.to_string()))?;
        let result = prior_result.unwrap_or(AdminApplicationMutationResult {
            input_hash,
            request_id: context.request_id.clone(),
            tenant_id,
            app_id: app.id,
            app_name: app.name,
            client_id: app.client_id,
            encrypted_secret,
            audit_event_id,
        });
        stage_admin_application_result(self, &transaction, &result, now)?;
        commit_admin_product_transaction(self, &transaction).await?;
        admin_application_response(self, result).map(Response::new)
    }

    pub(super) async fn grant_application_policy_admin(
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
        let audit_event = build_admin_audit_event(
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
        )?;
        let audit_event_id = audit_event.audit_event_id.clone();
        let policies = vec![(delegated_action, req.resource.clone())];
        let input_hash = admin_policy_input_hash("add", tenant_id, &app.name, &policies);
        let transaction =
            begin_admin_product_transaction(self, &principal, context, "application-policy-grant")
                .await?;
        let now = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
        if transaction.replayed {
            stage_or_verify_admin_policy_result(self, &transaction, &input_hash, now)?;
        } else {
            crate::access_control::stage_delegated_action_tuple_batch_with_admin_audit(
                &self.storage,
                &self.persistence,
                tenant_id,
                &app.id.to_string(),
                &policies,
                "add",
                &principal.principal_id,
                "admin access grant",
                &audit_event,
                &transaction.transaction_id,
                &transaction.principal,
            )
            .await?;
            stage_or_verify_admin_policy_result(self, &transaction, &input_hash, now)?;
            commit_admin_product_transaction(self, &transaction).await?;
        }
        Ok(Response::new(ApplicationPolicyResponse {
            request_id: context.request_id.clone(),
            tenant_id: tenant_id.to_string(),
            app_name: app.name,
            action: req.action,
            resource: req.resource,
            audit_event_id,
        }))
    }

    pub(super) async fn revoke_application_policy_admin(
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
        let audit_event = build_admin_audit_event(
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
        )?;
        let audit_event_id = audit_event.audit_event_id.clone();
        let policies = vec![(delegated_action, req.resource.clone())];
        let input_hash = admin_policy_input_hash("remove", tenant_id, &app.name, &policies);
        let transaction =
            begin_admin_product_transaction(self, &principal, context, "application-policy-revoke")
                .await?;
        let now = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
        if transaction.replayed {
            stage_or_verify_admin_policy_result(self, &transaction, &input_hash, now)?;
        } else {
            crate::access_control::stage_delegated_action_tuple_batch_with_admin_audit(
                &self.storage,
                &self.persistence,
                tenant_id,
                &app.id.to_string(),
                &policies,
                "remove",
                &principal.principal_id,
                "admin access revoke",
                &audit_event,
                &transaction.transaction_id,
                &transaction.principal,
            )
            .await?;
            stage_or_verify_admin_policy_result(self, &transaction, &input_hash, now)?;
            commit_admin_product_transaction(self, &transaction).await?;
        }
        Ok(Response::new(ApplicationPolicyResponse {
            request_id: context.request_id.clone(),
            tenant_id: tenant_id.to_string(),
            app_name: app.name,
            action: req.action,
            resource: req.resource,
            audit_event_id,
        }))
    }
}
