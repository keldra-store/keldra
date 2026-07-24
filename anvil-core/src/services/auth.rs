use crate::anvil_api::auth_service_server::AuthService;
use crate::anvil_api::*;
use crate::{
    AppState, access_control, auth, authz_derived_lag_watch, authz_journal, authz_namespace_watch,
    authz_realm_schema, authz_schema,
    authz_scope::{
        DEFAULT_AUTHZ_REALM_ID, decode_realm_namespace, decode_userset_subject_realm,
        encode_optional_realm_namespace, encode_realm_namespace, encode_userset_subject_realm,
        parse_userset_subject,
    },
    bucket_journal, control_journal,
    formats::hash32,
    permissions::AnvilAction,
    services::watch_envelope::{self, WatchEnvelopeParts},
    system_realm::{SYSTEM_REALM_ID, SYSTEM_STORAGE_TENANT_ID},
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

async fn public_access_grant_record(
    state: &AppState,
    app: &crate::persistence::App,
    grant: crate::persistence::AuthzTupleRecord,
) -> Result<AccessGrantRecord, Status> {
    let (action, resource) = public_action_resource_for_system_tuple(state, &grant)
        .await
        .unwrap_or_else(|| {
            (
                grant.relation.clone(),
                format!("{}:{}", grant.namespace, grant.object_id),
            )
        });
    Ok(AccessGrantRecord {
        app_id: app.id.to_string(),
        app_name: app.name.clone(),
        action,
        resource,
    })
}

async fn public_action_resource_for_system_tuple(
    state: &AppState,
    grant: &crate::persistence::AuthzTupleRecord,
) -> Option<(String, String)> {
    let namespace = decode_realm_namespace(SYSTEM_REALM_ID, &grant.namespace)?;
    match namespace {
        crate::system_realm::SYSTEM_STORAGE_TENANT_NAMESPACE => {
            let action = match grant.relation.as_str() {
                "create_bucket" => "bucket:create",
                "list_buckets" => "bucket:list",
                "read_tenant" => "app:read",
                "grant_access" => "policy:grant",
                "revoke_access" => "policy:revoke",
                "read_access_grants" => "policy:read",
                "lease_read" => "coordination:lease_read",
                "lease_write" => "coordination:lease_write",
                "lease_admin" => "coordination:lease_admin",
                "manage_tenant" | "owner" | "admin" => "tenant:manage",
                _ => return None,
            };
            Some((action.to_string(), format!("tenant:{}", grant.object_id)))
        }
        crate::system_realm::SYSTEM_BUCKET_NAMESPACE => {
            let bucket_id = grant.object_id.parse::<i64>().ok()?;
            let bucket = bucket_journal::read_current_bucket_by_id(&state.storage, bucket_id)
                .await
                .ok()
                .flatten()?;
            let action = match grant.relation.as_str() {
                "list_objects" | "reader" => "bucket:read",
                "manage_bucket" | "owner" | "admin" => "bucket:write",
                "get_object" => "object:read",
                "put_object" | "writer" => "object:write",
                "delete_object" => "object:delete",
                "manage_links" => "object:write",
                "manage_indexes" => "index:create",
                "query_indexes" => "index:read",
                _ => return None,
            };
            Some((action.to_string(), bucket.name))
        }
        crate::system_realm::SYSTEM_OBJECT_NAMESPACE => {
            let (bucket_id, key) = grant.object_id.split_once('/')?;
            let bucket = bucket_journal::read_current_bucket_by_id(
                &state.storage,
                bucket_id.parse::<i64>().ok()?,
            )
            .await
            .ok()
            .flatten()?;
            let action = match grant.relation.as_str() {
                "get" | "reader" => "object:read",
                "put" | "writer" => "object:write",
                "delete" => "object:delete",
                "link" => "object:write",
                _ => return None,
            };
            Some((action.to_string(), format!("{}/{}", bucket.name, key)))
        }
        crate::system_realm::SYSTEM_INDEX_NAMESPACE => {
            let (bucket_id, index) = grant.object_id.split_once('/')?;
            let bucket = bucket_journal::read_current_bucket_by_id(
                &state.storage,
                bucket_id.parse::<i64>().ok()?,
            )
            .await
            .ok()
            .flatten()?;
            let action = match grant.relation.as_str() {
                "define" | "owner" | "writer" => "index:create",
                "query" | "reader" => "index:read",
                "repair" => "index:update",
                _ => return None,
            };
            Some((action.to_string(), format!("{}/{}", bucket.name, index)))
        }
        crate::system_realm::SYSTEM_STREAM_NAMESPACE => {
            let (bucket_id, stream_key) = grant.object_id.split_once('/')?;
            let bucket = bucket_journal::read_current_bucket_by_id(
                &state.storage,
                bucket_id.parse::<i64>().ok()?,
            )
            .await
            .ok()
            .flatten()?;
            let action = match grant.relation.as_str() {
                "append" | "producer" => "stream:append",
                "read" | "consumer" => "stream:read",
                "seal_segment" => "stream:seal_segment",
                "owner" => "stream:create",
                _ => return None,
            };
            Some((
                action.to_string(),
                format!("{}/{}", bucket.name, stream_key),
            ))
        }
        crate::system_realm::SYSTEM_AUTHZ_REALM_NAMESPACE => {
            let action = match grant.relation.as_str() {
                "tuple_writer" | "write_tuples" => "authz:tuple_write",
                "checker" | "check" => "authz:check",
                "auditor" | "list" => "authz:tuple_read",
                "schema_admin" | "put_schema" | "bind_schema" => "authz:schema_write",
                _ => return None,
            };
            Some((action.to_string(), grant.object_id.clone()))
        }
        crate::system_realm::SYSTEM_PERSONALDB_GROUP_NAMESPACE => {
            let action = match grant.relation.as_str() {
                "get_snapshot" => "personaldb:read",
                "watch" => "personaldb:watch",
                "apply_changeset" => "personaldb:commit",
                "owner" => "personaldb:create",
                _ => return None,
            };
            Some((action.to_string(), grant.object_id.clone()))
        }
        crate::system_realm::SYSTEM_REGISTRY_NAMESPACE => {
            let action = match grant.relation.as_str() {
                "publish" => "registry:version_write",
                "read" => "registry:read",
                _ => return None,
            };
            Some((action.to_string(), grant.object_id.clone()))
        }
        _ => None,
    }
}

#[tonic::async_trait]
impl AuthService for AppState {
    type WatchAuthzTupleLogStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchAuthzTupleLogResponse, Status>> + Send>,
    >;
    type WatchAuthzNamespaceStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchAuthzNamespaceResponse, Status>> + Send>,
    >;
    type WatchAuthzDerivedLagStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchAuthzDerivedLagResponse, Status>> + Send>,
    >;

    async fn get_access_token(
        &self,
        request: Request<GetAccessTokenRequest>,
    ) -> Result<Response<GetAccessTokenResponse>, Status> {
        let req = request.into_inner();

        // 1. Verify credentials
        let app_details = self
            .persistence
            .get_app_by_client_id(&req.client_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::unauthenticated("Invalid client ID"))?;

        let decrypted_secret = self
            .secret_keyring
            .decrypt(&app_details.client_secret_encrypted)
            .map_err(|_| Status::unauthenticated("Invalid client secret"))?;

        if !constant_time_eq::constant_time_eq(
            decrypted_secret.as_slice(),
            req.client_secret.as_bytes(),
        ) {
            return Err(Status::unauthenticated("Invalid client secret"));
        }

        // Tokens identify the principal and Anvil storage tenant. Authorisation
        // is resolved from Zanzibar relations at request time, not token scopes.
        let token = self
            .jwt_manager
            .mint_token(app_details.id.to_string(), app_details.tenant_id)
            .map_err(|e| Status::internal(e.to_string()))?;
        tracing::info!(
            "[AuthService] Returning access token for app_id={}",
            app_details.id
        );
        Ok(Response::new(GetAccessTokenResponse {
            access_token: token,
            expires_in: 3600,
        }))
    }

    async fn create_application_credential(
        &self,
        request: Request<CreateApplicationCredentialRequest>,
    ) -> Result<Response<ApplicationSecretResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        require_app_management_permission(self, &claims, AnvilAction::AppCreate).await?;
        validate_public_app_request(&req.app_name, &req.request_id, &req.idempotency_key)?;

        let client_id = format!("app_{}", uuid::Uuid::new_v4().simple());
        let client_secret = format!("secret_{}", uuid::Uuid::new_v4().simple());
        let encrypted_secret = self
            .secret_keyring
            .encrypt(client_secret.as_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        let app = self
            .persistence
            .create_app(
                claims.tenant_id,
                &req.app_name,
                &client_id,
                &encrypted_secret,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let audit_event_id = crate::services::audit::record_tenant_audit_event(
            self,
            &claims,
            &req.request_id,
            format!("app:{}", app.name),
            "app.create",
            serde_json::json!({ "app_id": app.id, "client_id": client_id }),
        )
        .await?;

        Ok(Response::new(ApplicationSecretResponse {
            request_id: req.request_id,
            tenant_id: claims.tenant_id.to_string(),
            app_name: app.name,
            client_id,
            client_secret,
            audit_event_id,
            app_id: app.id.to_string(),
        }))
    }

    async fn rotate_application_credential_secret(
        &self,
        request: Request<RotateApplicationCredentialSecretRequest>,
    ) -> Result<Response<ApplicationSecretResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        require_app_management_permission(self, &claims, AnvilAction::AppRotateSecret).await?;
        validate_public_app_request(&req.app_name, &req.request_id, &req.idempotency_key)?;
        let app = app_in_claims_tenant(self, claims.tenant_id, &req.app_name).await?;

        let client_secret = format!("secret_{}", uuid::Uuid::new_v4().simple());
        let encrypted_secret = self
            .secret_keyring
            .encrypt(client_secret.as_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        self.persistence
            .update_app_secret(app.id, &encrypted_secret)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let audit_event_id = crate::services::audit::record_tenant_audit_event(
            self,
            &claims,
            &req.request_id,
            format!("app:{}", app.name),
            "app.rotate_secret",
            serde_json::json!({ "app_id": app.id, "client_id": app.client_id }),
        )
        .await?;

        Ok(Response::new(ApplicationSecretResponse {
            request_id: req.request_id,
            tenant_id: claims.tenant_id.to_string(),
            app_name: app.name,
            client_id: app.client_id,
            client_secret,
            audit_event_id,
            app_id: app.id.to_string(),
        }))
    }

    async fn delete_application_credential(
        &self,
        request: Request<DeleteApplicationCredentialRequest>,
    ) -> Result<Response<DeleteApplicationCredentialResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        require_app_management_permission(self, &claims, AnvilAction::AppDelete).await?;
        validate_public_app_request(&req.app_name, &req.request_id, &req.idempotency_key)?;
        let app = app_in_claims_tenant(self, claims.tenant_id, &req.app_name).await?;

        self.persistence
            .delete_app(app.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        crate::services::audit::record_tenant_audit_event(
            self,
            &claims,
            &req.request_id,
            format!("app:{}", app.name),
            "app.delete",
            serde_json::json!({ "app_id": app.id }),
        )
        .await?;

        Ok(Response::new(DeleteApplicationCredentialResponse {
            request_id: req.request_id,
            app_id: app.id.to_string(),
        }))
    }

    async fn list_applications(
        &self,
        request: Request<ListApplicationsRequest>,
    ) -> Result<Response<ListApplicationsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        require_app_management_permission(self, &claims, AnvilAction::AppRead).await?;
        let page_size = crate::services::collection_cursor::page_size(req.page.as_ref())?;
        let revision = control_journal::current_control_collection_revision(&self.storage)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        let principal_scope = format!("tenant:{}/subject:{}", claims.tenant_id, claims.sub);
        let binding = crate::services::collection_cursor::CollectionCursorBinding {
            service_method: "anvil.AuthService/ListApplications",
            filters: &[],
            principal_scope: &principal_scope,
            page_size,
            revision: &revision,
            sort: "app_name.asc",
        };
        let position = crate::services::collection_cursor::decode_page_token(
            req.page.as_ref(),
            &binding,
            self.config.jwt_secret.as_bytes(),
        )?;
        let after_tuple_key =
            crate::services::collection_cursor::decode_binary_position(position.as_deref())?;
        let app_page = self
            .persistence
            .page_apps_for_tenant(
                claims.tenant_id,
                &revision,
                after_tuple_key.as_deref(),
                page_size,
            )
            .await
            .map_err(|error| Status::aborted(error.to_string()))?;
        let next_page_token = app_page
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
        let applications = app_page
            .apps
            .into_iter()
            .map(|app| ApplicationDescriptor {
                tenant_id: claims.tenant_id.to_string(),
                app_id: app.id.to_string(),
                app_name: app.name,
                client_id: app.client_id,
            })
            .collect();
        Ok(Response::new(ListApplicationsResponse {
            applications,
            page: Some(PageResponse { next_page_token }),
        }))
    }

    async fn grant_access(
        &self,
        request: Request<GrantAccessRequest>,
    ) -> Result<Response<GrantAccessResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        validate_public_delegation_resource(claims, &req.resource)?;
        if req.action.trim() == "*"
            || req.action.trim().ends_with(":*")
            || req.resource.trim() == "*"
        {
            return Err(Status::permission_denied(
                "Public policy delegation cannot grant wildcard authority",
            ));
        }
        let delegated_action = req
            .action
            .parse::<AnvilAction>()
            .map_err(|_| Status::invalid_argument("Invalid delegated action"))?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::PolicyGrant,
            &req.resource,
        )
        .await?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            delegated_action.clone(),
            &req.resource,
        )
        .await?;

        let app = app_in_claims_tenant(self, claims.tenant_id, &req.grantee_app_id).await?;
        access_control::write_delegated_action_tuple(
            &self.storage,
            &self.persistence,
            claims.tenant_id,
            &app.id.to_string(),
            delegated_action,
            &req.resource,
            "add",
            &claims.sub,
            "tenant access grant",
        )
        .await?;
        crate::services::audit::record_tenant_audit_event(
            self,
            claims,
            "policy-grant",
            &req.resource,
            "policy.grant",
            serde_json::json!({ "grantee_app_id": app.id, "action": req.action }),
        )
        .await?;

        Ok(Response::new(GrantAccessResponse {}))
    }

    async fn revoke_access(
        &self,
        request: Request<RevokeAccessRequest>,
    ) -> Result<Response<RevokeAccessResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        validate_public_delegation_resource(claims, &req.resource)?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::PolicyRevoke,
            &req.resource,
        )
        .await?;

        let app = app_in_claims_tenant(self, claims.tenant_id, &req.grantee_app_id).await?;

        let delegated_action = req
            .action
            .parse::<AnvilAction>()
            .map_err(|_| Status::invalid_argument("Invalid delegated action"))?;
        access_control::write_delegated_action_tuple(
            &self.storage,
            &self.persistence,
            claims.tenant_id,
            &app.id.to_string(),
            delegated_action,
            &req.resource,
            "remove",
            &claims.sub,
            "tenant access revoke",
        )
        .await?;
        crate::services::audit::record_tenant_audit_event(
            self,
            claims,
            "policy-revoke",
            &req.resource,
            "policy.revoke",
            serde_json::json!({ "grantee_app_id": app.id, "action": req.action }),
        )
        .await?;

        Ok(Response::new(RevokeAccessResponse {}))
    }

    async fn list_access_grants(
        &self,
        request: Request<ListAccessGrantsRequest>,
    ) -> Result<Response<ListAccessGrantsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        require_app_management_permission(self, &claims, AnvilAction::PolicyRead).await?;
        let app = app_in_claims_tenant(self, claims.tenant_id, &req.app).await?;
        let page_size = crate::services::collection_cursor::page_size(req.page.as_ref())?;
        let supplied_page_token = req
            .page
            .as_ref()
            .map(|page| page.page_token.as_str())
            .unwrap_or_default();
        let revision = match authz_page_token_revision(supplied_page_token)? {
            Some(revision) => revision,
            None => authz_journal::latest_authz_revision(
                &self.storage,
                crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
            )
            .await
            .map_err(|error| Status::internal(error.to_string()))?,
        };
        let filter_hash =
            authz_page_filter_hash("list_access_grants", &[req.app.as_str(), "subject_order"]);
        let binding = AuthzPageBinding {
            tenant_id: claims.tenant_id,
            principal_id: &claims.sub,
            revision,
            filter_hash: &filter_hash,
            page_size,
        };
        let token = parse_authz_page_token(
            supplied_page_token,
            &binding,
            self.config.jwt_secret.as_bytes(),
        )?;
        require_current_authz_list_revision(&self.storage, SYSTEM_STORAGE_TENANT_ID, revision)
            .await?;
        let after_tuple_key =
            decode_authz_page_position(token.as_ref().map(|token| token.position.as_str()))?;
        let grant_page = authz_journal::page_current_authz_tuples(
            &self.storage,
            SYSTEM_STORAGE_TENANT_ID,
            &authz_journal::AuthzTupleFilter {
                realm_id: Some(SYSTEM_REALM_ID.to_string()),
                subject_kind: Some(access_control::APP_SUBJECT_KIND.to_string()),
                subject_id: Some(app.id.to_string()),
                caveat_hash: Some(String::new()),
                ..authz_journal::AuthzTupleFilter::default()
            },
            revision,
            after_tuple_key.as_deref(),
            page_size,
        )
        .await
        .map_err(authz_projection_page_status)?;
        let mut grants = Vec::with_capacity(grant_page.records.len());
        for grant in grant_page.records {
            grants.push(public_access_grant_record(self, &app, grant).await?);
        }
        let next_page_token = grant_page
            .next_tuple_key
            .as_deref()
            .map(encode_authz_page_position)
            .transpose()?
            .as_deref()
            .map(|position| {
                encode_authz_page_token(&binding, position, self.config.jwt_secret.as_bytes())
            })
            .transpose()?
            .unwrap_or_default();
        Ok(Response::new(ListAccessGrantsResponse {
            grants,
            page: Some(PageResponse { next_page_token }),
        }))
    }

    async fn set_public_access(
        &self,
        request: Request<SetPublicAccessRequest>,
    ) -> Result<Response<SetPublicAccessResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::BucketWrite,
            &req.bucket,
        )
        .await?;

        let bucket = self
            .persistence
            .set_bucket_public_access(claims.tenant_id, &req.bucket, req.allow_public_read)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        access_control::write_bucket_public_read_tuple(
            &self.persistence,
            &bucket,
            req.allow_public_read,
            &claims.sub,
            "bucket public-read policy update",
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(SetPublicAccessResponse {}))
    }

    async fn write_authz_tuple(
        &self,
        request: Request<WriteAuthzTupleRequest>,
    ) -> Result<Response<WriteAuthzTupleResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let record = write_authz_tuple_record(
            self,
            &claims,
            AuthzTupleMutation {
                namespace: req.namespace,
                object_id: req.object_id,
                relation: req.relation,
                subject_kind: req.subject_kind,
                subject_id: req.subject_id,
                caveat_hash: req.caveat_hash,
                operation: req.operation,
                reason: req.reason,
                scope: req.scope,
            },
        )
        .await?;

        Ok(Response::new(write_authz_tuple_response(&record)?))
    }

    async fn write_authz_tuples(
        &self,
        request: Request<WriteAuthzTuplesRequest>,
    ) -> Result<Response<WriteAuthzTuplesResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if req.mutations.is_empty() {
            return Err(Status::invalid_argument(
                "mutations must contain at least one tuple",
            ));
        }
        if req.mutations.len() > 1000 {
            return Err(Status::invalid_argument(
                "mutations must contain no more than 1000 tuples",
            ));
        }
        for mutation in &req.mutations {
            validate_authz_tuple_mutation_shape(mutation)?;
        }
        let scope = resolve_batch_scope(&claims, req.scope.as_ref(), &req.mutations)?;
        validate_authz_batch_operation_id(req.operation_id.as_deref())?;
        let expected_revision = optional_expected_authz_revision(req.expected_revision)?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzTupleWrite,
            &scope.authz_realm_id,
        )
        .await?;

        let mutations = req
            .mutations
            .iter()
            .map(|mutation| crate::persistence::AuthzTupleBatchMutation {
                namespace: encode_realm_namespace(&scope.authz_realm_id, &mutation.namespace),
                object_id: mutation.object_id.clone(),
                relation: mutation.relation.clone(),
                subject_id: encode_userset_subject_realm(
                    &scope.authz_realm_id,
                    &mutation.subject_kind,
                    &mutation.subject_id,
                ),
                subject_kind: mutation.subject_kind.clone(),
                caveat_hash: mutation.caveat_hash.clone(),
                operation: mutation.operation.clone(),
                reason: mutation.reason.clone(),
            })
            .collect::<Vec<_>>();
        let options = crate::persistence::AuthzTupleBatchWriteOptions {
            authz_realm_id: scope.authz_realm_id.clone(),
            operation_id: req.operation_id,
            expected_revision,
            schema_binding_precondition: None,
        };
        if let Some(replay) = self
            .persistence
            .replay_authz_tuple_batch(claims.tenant_id, &mutations, &claims.sub, &options)
            .await
            .map_err(authz_tuple_batch_write_status)?
        {
            return Ok(Response::new(write_authz_tuple_batch_response(
                &replay.records,
            )?));
        }

        let outcome = self
            .persistence
            .write_authz_tuple_batch_conditionally(
                claims.tenant_id,
                mutations,
                &claims.sub,
                &options,
            )
            .await
            .map_err(authz_tuple_batch_write_status)?;
        Ok(Response::new(write_authz_tuple_batch_response(
            &outcome.records,
        )?))
    }

    async fn read_authz_tuples(
        &self,
        request: Request<ReadAuthzTuplesRequest>,
    ) -> Result<Response<ReadAuthzTuplesResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_optional_public_authz_namespace(&req.namespace)?;
        validate_optional_tuple_field("object_id", &req.object_id)?;
        validate_optional_tuple_component("relation", &req.relation)?;
        validate_optional_tuple_component("subject_kind", &req.subject_kind)?;
        validate_optional_tuple_field("subject_id", &req.subject_id)?;
        validate_caveat_hash(&req.caveat_hash)?;
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;

        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzTupleRead,
            &scope.authz_realm_id,
        )
        .await?;
        let filter_hash = authz_page_filter_hash(
            "read_tuples",
            &[
                &scope.authz_realm_id,
                &req.namespace,
                &req.object_id,
                &req.relation,
                &req.subject_kind,
                &req.subject_id,
                &req.caveat_hash,
                if req.subject_kind.is_empty() {
                    "object_order"
                } else {
                    "subject_order"
                },
            ],
        );
        let page_size = normalize_page_size(req.page_size)?;
        let response_revision = match authz_page_token_revision(&req.page_token)? {
            Some(revision) => revision,
            None => {
                let consistency = AuthzConsistency::from_request(&req.consistency, &req.zookie)?;
                resolve_authz_response_revision(&self.storage, claims.tenant_id, consistency)
                    .await?
            }
        };
        let page_binding = AuthzPageBinding {
            tenant_id: claims.tenant_id,
            principal_id: &claims.sub,
            revision: response_revision,
            filter_hash: &filter_hash,
            page_size,
        };
        let page_token = parse_authz_page_token(
            &req.page_token,
            &page_binding,
            self.config.jwt_secret.as_bytes(),
        )?;
        require_current_authz_list_revision(&self.storage, claims.tenant_id, response_revision)
            .await?;
        let after_tuple_key =
            decode_authz_page_position(page_token.as_ref().map(|token| token.position.as_str()))?;
        let page = authz_journal::page_current_authz_tuples(
            &self.storage,
            claims.tenant_id,
            &authz_journal::AuthzTupleFilter {
                realm_id: Some(scope.authz_realm_id.clone()),
                namespace: optional_filter_value(encode_optional_realm_namespace(
                    &scope.authz_realm_id,
                    &req.namespace,
                )),
                object_id: optional_filter_value(req.object_id),
                relation: optional_filter_value(req.relation),
                subject_id: optional_filter_value(encode_userset_subject_realm(
                    &scope.authz_realm_id,
                    &req.subject_kind,
                    &req.subject_id,
                )),
                subject_kind: optional_filter_value(req.subject_kind),
                caveat_hash: optional_filter_value(req.caveat_hash),
            },
            response_revision,
            after_tuple_key.as_deref(),
            page_size,
        )
        .await
        .map_err(authz_projection_page_status)?;
        let next_page_token = page
            .next_tuple_key
            .as_deref()
            .map(encode_authz_page_position)
            .transpose()?
            .as_deref()
            .map(|position| {
                encode_authz_page_token(&page_binding, position, self.config.jwt_secret.as_bytes())
            })
            .transpose()?
            .unwrap_or_default();

        Ok(Response::new(ReadAuthzTuplesResponse {
            tuples: page
                .records
                .into_iter()
                .map(|record| authz_tuple_response_for_realm(&record, &scope.authz_realm_id))
                .collect::<Result<Vec<_>, _>>()?,
            revision: revision_to_u64(response_revision)?,
            zookie: zookie(response_revision),
            next_page_token,
        }))
    }

    async fn check_permission(
        &self,
        request: Request<CheckPermissionRequest>,
    ) -> Result<Response<CheckPermissionResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        Ok(Response::new(
            check_permission_response(self, &claims, req).await?,
        ))
    }

    async fn check_permissions(
        &self,
        request: Request<CheckPermissionsRequest>,
    ) -> Result<Response<CheckPermissionsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if req.checks.is_empty() {
            return Err(Status::invalid_argument(
                "checks must contain at least one request",
            ));
        }
        if req.checks.len() > 1000 {
            return Err(Status::invalid_argument(
                "checks must contain no more than 1000 requests",
            ));
        }

        let mut results = Vec::with_capacity(req.checks.len());
        let mut latest_revision = 0;
        for check in req.checks {
            let response = check_permission_response(self, &claims, check).await?;
            latest_revision = latest_revision.max(response.revision);
            results.push(response);
        }

        Ok(Response::new(CheckPermissionsResponse {
            results,
            revision: latest_revision,
            zookie: format!("authz:{latest_revision}"),
        }))
    }

    async fn list_authz_objects(
        &self,
        request: Request<ListAuthzObjectsRequest>,
    ) -> Result<Response<ListAuthzObjectsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_public_authz_namespace(&req.namespace)?;
        validate_tuple_component("relation", &req.relation)?;
        validate_tuple_component("subject_kind", &req.subject_kind)?;
        validate_tuple_field("subject_id", &req.subject_id)?;
        validate_caveat_hash(&req.caveat_hash)?;
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzTupleRead,
            &scope.authz_realm_id,
        )
        .await?;
        let filter_hash = authz_page_filter_hash(
            "list_objects",
            &[
                &scope.authz_realm_id,
                &req.namespace,
                &req.relation,
                &req.subject_kind,
                &req.subject_id,
                &req.caveat_hash,
            ],
        );
        let page_size = normalize_page_size(req.page_size)?;
        let response_revision = match authz_page_token_revision(&req.page_token)? {
            Some(revision) => revision,
            None => {
                let consistency = AuthzConsistency::from_request(&req.consistency, &req.zookie)?;
                resolve_authz_response_revision(&self.storage, claims.tenant_id, consistency)
                    .await?
            }
        };
        let page_binding = AuthzPageBinding {
            tenant_id: claims.tenant_id,
            principal_id: &claims.sub,
            revision: response_revision,
            filter_hash: &filter_hash,
            page_size,
        };
        let page_token = parse_authz_page_token(
            &req.page_token,
            &page_binding,
            self.config.jwt_secret.as_bytes(),
        )?;
        require_current_authz_list_revision(&self.storage, claims.tenant_id, response_revision)
            .await?;
        let page = authz_journal::list_current_authz_objects_page(
            &self.storage,
            claims.tenant_id,
            &encode_realm_namespace(&scope.authz_realm_id, &req.namespace),
            &req.relation,
            &req.subject_kind,
            &req.subject_id,
            &req.caveat_hash,
            response_revision,
            page_token.as_ref().map(|token| token.position.as_str()),
            page_size,
        )
        .await
        .map_err(crate::services::authz_status::consistency_status)?;
        let next_page_token = page
            .next_object_id
            .as_deref()
            .map(|position| {
                encode_authz_page_token(&page_binding, position, self.config.jwt_secret.as_bytes())
            })
            .transpose()?
            .unwrap_or_default();

        Ok(Response::new(ListAuthzObjectsResponse {
            object_ids: page.object_ids,
            revision: revision_to_u64(response_revision)?,
            zookie: zookie(response_revision),
            next_page_token,
        }))
    }

    async fn list_authz_subjects(
        &self,
        request: Request<ListAuthzSubjectsRequest>,
    ) -> Result<Response<ListAuthzSubjectsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_public_authz_namespace(&req.namespace)?;
        validate_tuple_field("object_id", &req.object_id)?;
        validate_tuple_component("relation", &req.relation)?;
        validate_optional_tuple_component("subject_kind", &req.subject_kind)?;
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzTupleRead,
            &scope.authz_realm_id,
        )
        .await?;
        let filter_hash = authz_page_filter_hash(
            "list_subjects",
            &[
                &scope.authz_realm_id,
                &req.namespace,
                &req.object_id,
                &req.relation,
                &req.subject_kind,
            ],
        );
        let page_size = normalize_page_size(req.page_size)?;
        let response_revision = match authz_page_token_revision(&req.page_token)? {
            Some(revision) => revision,
            None => {
                let consistency = AuthzConsistency::from_request(&req.consistency, &req.zookie)?;
                resolve_authz_response_revision(&self.storage, claims.tenant_id, consistency)
                    .await?
            }
        };
        let page_binding = AuthzPageBinding {
            tenant_id: claims.tenant_id,
            principal_id: &claims.sub,
            revision: response_revision,
            filter_hash: &filter_hash,
            page_size,
        };
        let page_token = parse_authz_page_token(
            &req.page_token,
            &page_binding,
            self.config.jwt_secret.as_bytes(),
        )?;
        require_current_authz_list_revision(&self.storage, claims.tenant_id, response_revision)
            .await?;
        let page = authz_journal::list_current_authz_subjects_page(
            &self.storage,
            claims.tenant_id,
            &encode_realm_namespace(&scope.authz_realm_id, &req.namespace),
            &req.object_id,
            &req.relation,
            optional_str(req.subject_kind.as_str()),
            response_revision,
            page_token.as_ref().map(|token| token.position.as_str()),
            page_size,
        )
        .await
        .map_err(crate::services::authz_status::consistency_status)?;
        let next_page_token = page
            .next_subject_position
            .as_deref()
            .map(|position| {
                encode_authz_page_token(&page_binding, position, self.config.jwt_secret.as_bytes())
            })
            .transpose()?
            .unwrap_or_default();

        Ok(Response::new(ListAuthzSubjectsResponse {
            subjects: page
                .subjects
                .into_iter()
                .map(|subject| AuthzSubject {
                    subject_id: decode_userset_subject_realm(
                        &scope.authz_realm_id,
                        &subject.subject_kind,
                        &subject.subject_id,
                    ),
                    subject_kind: subject.subject_kind,
                    caveat_hash: subject.caveat_hash,
                })
                .collect(),
            revision: revision_to_u64(response_revision)?,
            zookie: zookie(response_revision),
            next_page_token,
        }))
    }

    async fn put_authz_schema(
        &self,
        request: Request<PutAuthzSchemaRequest>,
    ) -> Result<Response<PutAuthzSchemaResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_storage_tenant(&claims, &req.anvil_storage_tenant_id)?;
        validate_tuple_component("schema_id", &req.schema_id)?;
        if req.namespaces.is_empty() {
            return Err(Status::invalid_argument(
                "namespaces must contain at least one schema",
            ));
        }
        for namespace in &req.namespaces {
            validate_public_authz_namespace(&namespace.namespace)?;
        }
        crate::authz_schema_contract::validate_schema_set(&req.namespaces)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzSchemaWrite,
            &format!("schema:{}", req.schema_id),
        )
        .await?;
        let record = authz_realm_schema::put_schema_revision(
            &self.storage,
            claims.tenant_id,
            &req.schema_id,
            req.namespaces,
            &claims.sub,
            &req.reason,
        )
        .await
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let authz_revision = record.authz_revision;
        Ok(Response::new(PutAuthzSchemaResponse {
            schema_ref: Some(schema_ref_response(&record.schema_ref)),
            authz_revision,
            zookie: zookie(u64_to_i64(authz_revision)?),
        }))
    }

    async fn bind_authz_schema(
        &self,
        request: Request<BindAuthzSchemaRequest>,
    ) -> Result<Response<BindAuthzSchemaResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;
        let schema_ref = req
            .schema_ref
            .ok_or_else(|| Status::invalid_argument("schema_ref is required"))?;
        validate_tuple_component("schema_id", &schema_ref.schema_id)?;
        // Creating or rebinding a tenant authz realm is controlled by the
        // owning storage-tenant relation first. The realm row may not exist yet,
        // so checking the realm relation before seeding its parent_tenant tuple
        // would make first bind impossible without a non-Zanzibar bypass.
        access_control::require_storage_tenant_permission(&self.storage, &claims, "manage_tenant")
            .await?;
        access_control::grant_authz_realm_defaults(
            &self.persistence,
            claims.tenant_id,
            &scope.authz_realm_id,
            &claims.sub,
            &claims.sub,
            "grant creator authz realm owner",
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        access_control::require_system_realm_permission(
            &self.storage,
            &claims,
            crate::system_realm::SYSTEM_AUTHZ_REALM_NAMESPACE,
            &access_control::authz_realm_object_id(claims.tenant_id, &scope.authz_realm_id),
            "bind_schema",
        )
        .await?;
        let binding = authz_realm_schema::bind_schema(
            &self.storage,
            claims.tenant_id,
            &scope.authz_realm_id,
            authz_realm_schema::StoredSchemaRef {
                schema_id: schema_ref.schema_id,
                schema_revision: schema_ref.schema_revision,
                schema_digest: schema_ref.schema_digest,
            },
            req.expected_binding_generation,
            &claims.sub,
            &req.reason,
        )
        .await
        .map_err(|e| Status::failed_precondition(e.to_string()))?;
        let authz_revision = binding.authz_revision;
        Ok(Response::new(BindAuthzSchemaResponse {
            scope: Some(scope),
            schema_ref: Some(schema_ref_response(&binding.schema_ref)),
            binding_generation: binding.binding_generation,
            authz_revision,
            zookie: zookie(u64_to_i64(authz_revision)?),
        }))
    }

    async fn get_authz_schema_binding(
        &self,
        request: Request<GetAuthzSchemaBindingRequest>,
    ) -> Result<Response<GetAuthzSchemaBindingResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzSchemaRead,
            &scope.authz_realm_id,
        )
        .await?;
        let binding = authz_realm_schema::read_schema_binding(
            &self.storage,
            claims.tenant_id,
            &scope.authz_realm_id,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found("schema binding not found"))?;
        Ok(Response::new(GetAuthzSchemaBindingResponse {
            scope: Some(scope),
            schema_ref: Some(schema_ref_response(&binding.schema_ref)),
            binding_generation: binding.binding_generation,
        }))
    }

    async fn apply_authz_schema(
        &self,
        request: Request<ApplyAuthzSchemaRequest>,
    ) -> Result<Response<ApplyAuthzSchemaResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if req.namespaces.is_empty() {
            return Err(Status::invalid_argument(
                "namespaces must contain at least one schema",
            ));
        }
        for namespace in &req.namespaces {
            validate_public_authz_namespace(&namespace.namespace)?;
        }
        crate::authz_schema_contract::validate_schema_set(&req.namespaces)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzSchemaWrite,
            DEFAULT_AUTHZ_REALM_ID,
        )
        .await?;

        access_control::grant_authz_realm_defaults(
            &self.persistence,
            claims.tenant_id,
            DEFAULT_AUTHZ_REALM_ID,
            &claims.sub,
            &claims.sub,
            "grant default authz realm owner",
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let mut records = Vec::with_capacity(req.namespaces.len());
        let authz_revision = authz_journal::latest_authz_revision(&self.storage, claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))
            .and_then(revision_to_u64)?
            .max(1);
        for namespace in req.namespaces {
            let record = authz_schema::write_authz_namespace_schema(
                &self.storage,
                claims.tenant_id,
                namespace,
                authz_revision,
                &claims.sub,
                &req.reason,
            )
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
            authz_namespace_watch::append_authz_namespace_watch_record(
                &self.storage,
                claims.tenant_id,
                mutation_id_from_record_hash(&record.record_hash),
                authz_namespace_watch::AuthzNamespaceWatchPayload {
                    namespace: record.namespace.clone(),
                    event_type: "schema_changed".to_string(),
                    authz_revision,
                    schema_hash: record.schema_hash.clone(),
                    invalidates_derived_usersets: true,
                    emitted_at: record.applied_at.clone(),
                },
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
            records.push(record);
        }
        let schema_version = records
            .iter()
            .map(|record| record.schema_version)
            .max()
            .unwrap_or(0);
        Ok(Response::new(ApplyAuthzSchemaResponse {
            namespaces: records.iter().map(authz_schema::schema_response).collect(),
            schema_version,
        }))
    }

    async fn get_authz_schema(
        &self,
        request: Request<GetAuthzSchemaRequest>,
    ) -> Result<Response<GetAuthzSchemaResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if !req.schema_id.is_empty() {
            validate_storage_tenant(&claims, &req.anvil_storage_tenant_id)?;
            validate_tuple_component("schema_id", &req.schema_id)?;
            access_control::require_action(
                &self.storage,
                &self.persistence,
                &claims,
                AnvilAction::AuthzSchemaRead,
                &format!("schema:{}", req.schema_id),
            )
            .await?;
            let record = authz_realm_schema::read_schema_revision(
                &self.storage,
                claims.tenant_id,
                &req.schema_id,
                req.schema_revision,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("schema not found"))?;
            return Ok(Response::new(GetAuthzSchemaResponse {
                namespaces: record.namespaces,
                schema_version: record.schema_ref.schema_revision,
                schema_ref: Some(schema_ref_response(&record.schema_ref)),
            }));
        }
        if !req.namespace.is_empty() {
            validate_public_authz_namespace(&req.namespace)?;
        }
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzSchemaRead,
            DEFAULT_AUTHZ_REALM_ID,
        )
        .await?;
        let records = if req.namespace.is_empty() {
            let page = authz_schema::page_authz_namespace_schemas(
                &self.storage,
                claims.tenant_id,
                None,
                authz_schema::AUTHZ_NAMESPACE_SCHEMA_PAGE_MAX,
            )
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
            if page.next_tuple_key.is_some() {
                return Err(Status::resource_exhausted(format!(
                    "authorization schema contains more than {} namespaces; request one namespace",
                    authz_schema::AUTHZ_NAMESPACE_SCHEMA_PAGE_MAX
                )));
            }
            page.records
        } else {
            authz_schema::read_authz_namespace_schema(
                &self.storage,
                claims.tenant_id,
                &req.namespace,
            )
            .await
            .map(|record| record.into_iter().collect())
            .map_err(|error| Status::internal(error.to_string()))?
        };
        let schema_version = records
            .iter()
            .map(|record| record.schema_version)
            .max()
            .unwrap_or(0);
        Ok(Response::new(GetAuthzSchemaResponse {
            namespaces: records.iter().map(authz_schema::schema_response).collect(),
            schema_version,
            schema_ref: None,
        }))
    }

    async fn watch_authz_tuple_log(
        &self,
        request: Request<WatchAuthzTupleLogRequest>,
    ) -> Result<Response<Self::WatchAuthzTupleLogStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzWatch,
            &scope.authz_realm_id,
        )
        .await?;
        let after_revision = i64::try_from(req.after_revision)
            .map_err(|_| Status::invalid_argument("after_revision exceeds supported range"))?;
        let stream_id = authz_journal::authz_tuple_stream_id(claims.tenant_id);
        let mut live = self.storage.subscribe_stream(&stream_id);
        let storage = self.storage.clone();
        let tenant_id = claims.tenant_id;
        let namespace = encode_optional_realm_namespace(&scope.authz_realm_id, &req.namespace);

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_revision = after_revision;
            loop {
                loop {
                    let page = match authz_journal::list_authz_tuple_log_page(
                        &storage,
                        tenant_id,
                        last_revision,
                        &namespace,
                        256,
                    )
                    .await
                    {
                        Ok(page) => page,
                        Err(error) => {
                            let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                            return;
                        }
                    };
                    let previous_revision = last_revision;
                    for record in page.records {
                        if tx
                            .send(Ok(authz_tuple_log_response_for_realm(
                                &record,
                                &scope.authz_realm_id,
                            )))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    last_revision = page.next_revision;
                    if !page.has_more || last_revision == previous_revision {
                        break;
                    }
                }

                match live.recv().await {
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchAuthzTupleLogStream
        ))
    }

    async fn watch_authz_namespace(
        &self,
        request: Request<WatchAuthzNamespaceRequest>,
    ) -> Result<Response<Self::WatchAuthzNamespaceStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_public_authz_namespace(&req.namespace)?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzWatch,
            DEFAULT_AUTHZ_REALM_ID,
        )
        .await?;

        let after_cursor = join_u128(req.after_cursor_low, req.after_cursor_high);
        let stream_id = authz_namespace_watch::authz_namespace_watch_stream_id(
            claims.tenant_id,
            &req.namespace,
        );
        let mut live = self.storage.subscribe_stream(&stream_id);
        let storage = self.storage.clone();
        let namespace = req.namespace;
        let tenant_id = claims.tenant_id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            loop {
                loop {
                    let page = match authz_namespace_watch::list_authz_namespace_watch_event_page(
                        &storage,
                        tenant_id,
                        &namespace,
                        last_cursor,
                        256,
                    )
                    .await
                    {
                        Ok(page) => page,
                        Err(error) => {
                            let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                            return;
                        }
                    };
                    let previous_cursor = last_cursor;
                    for event in page.events {
                        if tx
                            .send(Ok(authz_namespace_watch_response(event)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    last_cursor = page.next_cursor;
                    if !page.has_more || last_cursor == previous_cursor {
                        break;
                    }
                }
                match live.recv().await {
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchAuthzNamespaceStream
        ))
    }

    async fn watch_authz_derived_lag(
        &self,
        request: Request<WatchAuthzDerivedLagRequest>,
    ) -> Result<Response<Self::WatchAuthzDerivedLagStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_watch_component("derived_index_id", &req.derived_index_id)?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzWatch,
            DEFAULT_AUTHZ_REALM_ID,
        )
        .await?;

        let after_cursor = join_u128(req.after_cursor_low, req.after_cursor_high);
        let stream_id = authz_derived_lag_watch::authz_derived_lag_watch_stream_id(
            claims.tenant_id,
            &req.derived_index_id,
        );
        let mut live = self.storage.subscribe_stream(&stream_id);
        let storage = self.storage.clone();
        let derived_index_id = req.derived_index_id;
        let tenant_id = claims.tenant_id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            loop {
                loop {
                    let page =
                        match authz_derived_lag_watch::list_authz_derived_lag_watch_event_page(
                            &storage,
                            tenant_id,
                            &derived_index_id,
                            last_cursor,
                            256,
                        )
                        .await
                        {
                            Ok(page) => page,
                            Err(error) => {
                                let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                                return;
                            }
                        };
                    let previous_cursor = last_cursor;
                    for event in page.events {
                        if tx
                            .send(Ok(authz_derived_lag_watch_response(event)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    last_cursor = page.next_cursor;
                    if !page.has_more || last_cursor == previous_cursor {
                        break;
                    }
                }
                match live.recv().await {
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchAuthzDerivedLagStream
        ))
    }
}

mod helpers;
use helpers::*;

#[cfg(test)]
mod tests;
