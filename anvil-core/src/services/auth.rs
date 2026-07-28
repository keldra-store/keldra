use crate::anvil_api::auth_service_server::AuthService;
use crate::anvil_api::*;
use crate::{
    AppState, access_control, auth, authz_derived_lag_watch, authz_journal, authz_namespace_watch,
    authz_realm_schema,
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
use prost::Message;
use sha2::Sha256;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

struct ImplicitAuthTransaction {
    transaction_id: String,
    principal: String,
    replayed: bool,
}

impl AppState {
    async fn begin_implicit_auth_transaction(
        &self,
        claims: &auth::Claims,
        context: Option<&PublicMutationContext>,
        operation: &str,
    ) -> Result<ImplicitAuthTransaction, Status> {
        let principal = crate::object_manager::transaction_principal_from_claims(claims);
        let supplied = context
            .map(|context| context.idempotency_key.trim())
            .filter(|key| !key.is_empty());
        let idempotency_key = supplied
            .map(|key| format!("auth:{}:{}:{operation}:{key}", claims.tenant_id, claims.sub))
            .unwrap_or_else(|| {
                format!(
                    "auth:{}:{}:{operation}:{}",
                    claims.tenant_id,
                    claims.sub,
                    uuid::Uuid::new_v4()
                )
            });
        let now = u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| Status::internal("authorization mutation predates Unix epoch"))?;
        let handle = self
            .mvcc
            .open_transactions
            .begin(
                self.mvcc.runtime.as_ref(),
                self.mvcc.cluster_id(),
                &principal,
                &idempotency_key,
                std::time::Duration::from_secs(300),
                crate::mvcc_transaction::DurabilityLevel::Quorum,
                crate::mvcc_transaction::ReadConsistency::Linearized,
                now,
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let status = self
            .mvcc
            .open_transactions
            .status(&handle.transaction_id, &principal, now)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if status.state == "committing" {
            self.commit_implicit_auth_transaction(&ImplicitAuthTransaction {
                transaction_id: handle.transaction_id.clone(),
                principal: principal.clone(),
                replayed: false,
            })
            .await?;
        } else if status.state == "aborted" {
            return Err(Status::aborted(
                "implicit authorization transaction previously aborted",
            ));
        }
        Ok(ImplicitAuthTransaction {
            transaction_id: handle.transaction_id,
            principal,
            replayed: matches!(status.state, "committed" | "committing"),
        })
    }

    async fn commit_implicit_auth_transaction(
        &self,
        transaction: &ImplicitAuthTransaction,
    ) -> Result<(), Status> {
        let outcome = self
            .mvcc
            .open_transactions
            .commit(
                self.mvcc.runtime.as_ref(),
                &transaction.transaction_id,
                &transaction.principal,
                u64::try_from(chrono::Utc::now().timestamp_millis())
                    .map_err(|_| Status::internal("authorization commit predates Unix epoch"))?,
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        match outcome.certification {
            crate::mvcc_transaction::CertificationResult::Committed { .. } => Ok(()),
            crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
                Err(Status::aborted(format!(
                    "implicit authorization transaction aborted: {reason:?}"
                )))
            }
        }
    }
}

const CREDENTIAL_IDEMPOTENCY_NAMESPACE: &str = "auth.application-credential.v1";
const CREDENTIAL_IMPLICIT_RESULT_KEY: &str = "result";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CredentialMutationResult {
    input_hash: String,
    operation: String,
    request_id: String,
    tenant_id: i64,
    app_id: i64,
    app_name: String,
    client_id: String,
    encrypted_secret: Vec<u8>,
    audit_event_id: String,
}

fn credential_input_hash(
    operation: &str,
    claims: &auth::Claims,
    app_name: &str,
    request_id: &str,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.auth.application-credential.input.v1");
    for component in [
        operation,
        &claims.tenant_id.to_string(),
        &claims.sub,
        app_name,
        request_id,
    ] {
        hasher.update(&(component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    hex::encode(hasher.finalize().as_bytes())
}

fn control_journal_credential_identifier(transaction_id: &str, purpose: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.auth.application-credential.identifier.v1");
    hasher.update(transaction_id.as_bytes());
    hasher.update(&[0]);
    hasher.update(purpose.as_bytes());
    hex::encode(&hasher.finalize().as_bytes()[..16])
}

fn credential_transaction_id(options: Option<&WriteOptions>) -> Result<Option<&str>, Status> {
    crate::services::transaction_context::write_options_transaction_id(options)
}

fn credential_implicit_context(request_id: &str, idempotency_key: &str) -> PublicMutationContext {
    PublicMutationContext {
        request_id: request_id.to_string(),
        idempotency_key: idempotency_key.to_string(),
        expected_generation: 0,
        transaction_id: None,
    }
}

fn stage_credential_result(
    state: &AppState,
    transaction_id: &str,
    principal: &str,
    result_key: &str,
    result: &CredentialMutationResult,
    now_unix_ms: u64,
) -> Result<(), Status> {
    state
        .mvcc
        .open_transactions
        .add_idempotency_result(
            transaction_id,
            principal,
            crate::mvcc_transaction::IdempotencyResult {
                namespace: CREDENTIAL_IDEMPOTENCY_NAMESPACE.to_string(),
                key: result_key.to_string(),
                payload: serde_json::to_vec(result)
                    .map_err(|error| Status::internal(error.to_string()))?,
            },
            now_unix_ms,
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))
}

fn replayed_credential_result(
    state: &AppState,
    transaction: &ImplicitAuthTransaction,
    result_key: &str,
    expected_input_hash: &str,
) -> Result<CredentialMutationResult, Status> {
    let result = state
        .mvcc
        .open_transactions
        .resolved_idempotency_result(
            &transaction.transaction_id,
            &transaction.principal,
            CREDENTIAL_IDEMPOTENCY_NAMESPACE,
            result_key,
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?
        .ok_or_else(|| {
            Status::failed_precondition(
                "committed credential transaction is missing its response record",
            )
        })?;
    let result: CredentialMutationResult = serde_json::from_slice(&result.payload)
        .map_err(|error| Status::internal(error.to_string()))?;
    if result.input_hash != expected_input_hash {
        return Err(Status::already_exists(
            "credential idempotency key was already used for different input",
        ));
    }
    Ok(result)
}

fn application_secret_response(
    state: &AppState,
    result: CredentialMutationResult,
    write_state: WriteState,
) -> Result<ApplicationSecretResponse, Status> {
    let client_secret = state
        .secret_keyring
        .decrypt(&result.encrypted_secret)
        .map_err(|error| Status::internal(error.to_string()))
        .and_then(|secret| {
            String::from_utf8(secret)
                .map_err(|_| Status::internal("stored application credential secret is not UTF-8"))
        })?;
    Ok(ApplicationSecretResponse {
        request_id: result.request_id,
        tenant_id: result.tenant_id.to_string(),
        app_name: result.app_name,
        client_id: result.client_id,
        client_secret,
        audit_event_id: result.audit_event_id,
        app_id: result.app_id.to_string(),
        write_state: write_state as i32,
    })
}

const AUTH_MUTATION_IDEMPOTENCY_NAMESPACE: &str = "auth.public-mutation.v1";
const AUTH_MUTATION_IMPLICIT_RESULT_KEY: &str = "result";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct AuthMutationResult {
    input_hash: String,
    response: Vec<u8>,
}

fn auth_mutation_input_hash<M: Message>(
    operation: &str,
    claims: &auth::Claims,
    request: &M,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.auth.public-mutation.input.v1");
    for component in [operation, &claims.tenant_id.to_string(), &claims.sub] {
        hasher.update(&(component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    hasher.update(&request.encode_to_vec());
    hex::encode(hasher.finalize().as_bytes())
}

fn stage_auth_mutation_response<M: Message>(
    state: &AppState,
    transaction_id: &str,
    principal: &str,
    result_key: &str,
    input_hash: String,
    response: &M,
    now_unix_ms: u64,
) -> Result<(), Status> {
    let result = AuthMutationResult {
        input_hash,
        response: response.encode_to_vec(),
    };
    state
        .mvcc
        .open_transactions
        .add_idempotency_result(
            transaction_id,
            principal,
            crate::mvcc_transaction::IdempotencyResult {
                namespace: AUTH_MUTATION_IDEMPOTENCY_NAMESPACE.to_string(),
                key: result_key.to_string(),
                payload: serde_json::to_vec(&result)
                    .map_err(|error| Status::internal(error.to_string()))?,
            },
            now_unix_ms,
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))
}

fn replay_auth_mutation_response<M: Message + Default>(
    state: &AppState,
    transaction: &ImplicitAuthTransaction,
    expected_input_hash: &str,
) -> Result<M, Status> {
    let result = state
        .mvcc
        .open_transactions
        .resolved_idempotency_result(
            &transaction.transaction_id,
            &transaction.principal,
            AUTH_MUTATION_IDEMPOTENCY_NAMESPACE,
            AUTH_MUTATION_IMPLICIT_RESULT_KEY,
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?
        .ok_or_else(|| {
            Status::failed_precondition(
                "committed authorization transaction is missing its response record",
            )
        })?;
    let result: AuthMutationResult = serde_json::from_slice(&result.payload)
        .map_err(|error| Status::internal(error.to_string()))?;
    if result.input_hash != expected_input_hash {
        return Err(Status::already_exists(
            "authorization idempotency key was already used for different input",
        ));
    }
    M::decode(result.response.as_slice()).map_err(|error| Status::internal(error.to_string()))
}

fn stage_tenant_audit_in_transaction(
    state: &AppState,
    transaction_id: &str,
    principal: &str,
    event: &crate::tenant_audit::TenantAuditEvent,
    generation: u64,
    now_unix_ms: u64,
) -> Result<(), Status> {
    let plan = crate::tenant_audit::tenant_audit_mvcc_plan(event, generation, transaction_id)
        .map_err(|error| Status::internal(error.to_string()))?;
    state
        .mvcc
        .stage_product_mutations(transaction_id, principal, plan.mutations, now_unix_ms)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    for (key, predicate) in plan.predicates {
        state
            .mvcc
            .stage_predicate(transaction_id, principal, key, predicate, now_unix_ms)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
    }
    for event in plan.outbox_events {
        state
            .mvcc
            .open_transactions
            .add_stream_event(transaction_id, event, now_unix_ms)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
    }
    Ok(())
}

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
            let bucket = bucket_journal::read_current_bucket_by_id_mvcc(&state.mvcc, bucket_id)
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
            let bucket = bucket_journal::read_current_bucket_by_id_mvcc(
                &state.mvcc,
                bucket_id.parse::<i64>().ok()?,
            )
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
            let bucket = bucket_journal::read_current_bucket_by_id_mvcc(
                &state.mvcc,
                bucket_id.parse::<i64>().ok()?,
            )
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
            let bucket = bucket_journal::read_current_bucket_by_id_mvcc(
                &state.mvcc,
                bucket_id.parse::<i64>().ok()?,
            )
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

mod operations;

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
        let supplied_transaction_id = credential_transaction_id(req.options.as_ref())?;
        let implicit_context = credential_implicit_context(&req.request_id, &req.idempotency_key);
        let implicit = if supplied_transaction_id.is_none() {
            Some(
                self.begin_implicit_auth_transaction(
                    &claims,
                    Some(&implicit_context),
                    "credential-create",
                )
                .await?,
            )
        } else {
            None
        };
        let transaction_id = supplied_transaction_id
            .or_else(|| {
                implicit
                    .as_ref()
                    .map(|transaction| transaction.transaction_id.as_str())
            })
            .ok_or_else(|| Status::internal("credential transaction was not established"))?;
        let principal = crate::object_manager::transaction_principal_from_claims(&claims);
        let input_hash = credential_input_hash("create", &claims, &req.app_name, &req.request_id);
        let explicit_result_key;
        let result_key = if implicit.is_some() {
            CREDENTIAL_IMPLICIT_RESULT_KEY
        } else {
            explicit_result_key = format!("create:{}", req.request_id);
            &explicit_result_key
        };
        if let Some(transaction) = implicit.as_ref().filter(|transaction| transaction.replayed) {
            let result = replayed_credential_result(self, transaction, result_key, &input_hash)?;
            return application_secret_response(self, result, WriteState::Committed)
                .map(Response::new);
        }

        let client_id = format!(
            "app_{}",
            control_journal_credential_identifier(transaction_id, "client")
        );
        let client_secret = format!("secret_{}", uuid::Uuid::new_v4().simple());
        let encrypted_secret = self
            .secret_keyring
            .encrypt(client_secret.as_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        let audit_event = crate::services::audit::build_tenant_audit_event(
            &claims,
            &req.request_id,
            format!("app:{}", req.app_name),
            "app.create",
            serde_json::json!({ "client_id": client_id }),
        )?;
        let audit_event_id = audit_event.audit_event_id.clone();
        let now = u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| Status::internal("credential mutation predates Unix epoch"))?;
        let app = control_journal::plan_create_app_in_transaction(
            &self.mvcc,
            transaction_id,
            &principal,
            claims.tenant_id,
            &req.app_name,
            &client_id,
            &encrypted_secret,
            Some(&audit_event),
        )
        .map_err(|e| Status::internal(e.to_string()))?;
        let app = app
            .stage(&self.mvcc, transaction_id, &principal, now)
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let result = CredentialMutationResult {
            input_hash,
            operation: "create".to_string(),
            request_id: req.request_id,
            tenant_id: claims.tenant_id,
            app_id: app.id,
            app_name: app.name,
            client_id: app.client_id,
            encrypted_secret,
            audit_event_id,
        };
        stage_credential_result(self, transaction_id, &principal, result_key, &result, now)?;
        if let Some(transaction) = &implicit {
            self.commit_implicit_auth_transaction(transaction).await?;
        }
        application_secret_response(
            self,
            result,
            if implicit.is_some() {
                WriteState::Committed
            } else {
                WriteState::Staged
            },
        )
        .map(Response::new)
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
        let supplied_transaction_id = credential_transaction_id(req.options.as_ref())?;
        let implicit_context = credential_implicit_context(&req.request_id, &req.idempotency_key);
        let implicit = if supplied_transaction_id.is_none() {
            Some(
                self.begin_implicit_auth_transaction(
                    &claims,
                    Some(&implicit_context),
                    "credential-rotate",
                )
                .await?,
            )
        } else {
            None
        };
        let transaction_id = supplied_transaction_id
            .or_else(|| {
                implicit
                    .as_ref()
                    .map(|transaction| transaction.transaction_id.as_str())
            })
            .ok_or_else(|| Status::internal("credential transaction was not established"))?;
        let principal = crate::object_manager::transaction_principal_from_claims(&claims);
        let input_hash = credential_input_hash("rotate", &claims, &req.app_name, &req.request_id);
        let explicit_result_key;
        let result_key = if implicit.is_some() {
            CREDENTIAL_IMPLICIT_RESULT_KEY
        } else {
            explicit_result_key = format!("rotate:{}", req.request_id);
            &explicit_result_key
        };
        if let Some(transaction) = implicit.as_ref().filter(|transaction| transaction.replayed) {
            let result = replayed_credential_result(self, transaction, result_key, &input_hash)?;
            return application_secret_response(self, result, WriteState::Committed)
                .map(Response::new);
        }
        let details = control_journal::read_app_by_tenant_name_in_transaction(
            &self.mvcc,
            transaction_id,
            &principal,
            claims.tenant_id,
            &req.app_name,
        )
        .map_err(|error| Status::internal(error.to_string()))?
        .ok_or_else(|| Status::not_found("Application not found"))?;
        let app = details.app;

        let client_secret = format!("secret_{}", uuid::Uuid::new_v4().simple());
        let encrypted_secret = self
            .secret_keyring
            .encrypt(client_secret.as_bytes())
            .map_err(|e| Status::internal(e.to_string()))?;
        let audit_event = crate::services::audit::build_tenant_audit_event(
            &claims,
            &req.request_id,
            format!("app:{}", app.name),
            "app.rotate_secret",
            serde_json::json!({ "app_id": app.id, "client_id": app.client_id }),
        )?;
        let audit_event_id = audit_event.audit_event_id.clone();
        let now = u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| Status::internal("credential mutation predates Unix epoch"))?;
        let app = control_journal::plan_update_app_secret_in_transaction(
            &self.mvcc,
            transaction_id,
            &principal,
            app.id,
            &encrypted_secret,
            Some(&audit_event),
        )
        .map_err(|error| Status::internal(error.to_string()))?
        .stage(&self.mvcc, transaction_id, &principal, now)
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let result = CredentialMutationResult {
            input_hash,
            operation: "rotate".to_string(),
            request_id: req.request_id,
            tenant_id: claims.tenant_id,
            app_id: app.id,
            app_name: app.name,
            client_id: app.client_id,
            encrypted_secret,
            audit_event_id,
        };
        stage_credential_result(self, transaction_id, &principal, result_key, &result, now)?;
        if let Some(transaction) = &implicit {
            self.commit_implicit_auth_transaction(transaction).await?;
        }
        application_secret_response(
            self,
            result,
            if implicit.is_some() {
                WriteState::Committed
            } else {
                WriteState::Staged
            },
        )
        .map(Response::new)
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
        let supplied_transaction_id = credential_transaction_id(req.options.as_ref())?;
        let implicit_context = credential_implicit_context(&req.request_id, &req.idempotency_key);
        let implicit = if supplied_transaction_id.is_none() {
            Some(
                self.begin_implicit_auth_transaction(
                    &claims,
                    Some(&implicit_context),
                    "credential-delete",
                )
                .await?,
            )
        } else {
            None
        };
        let transaction_id = supplied_transaction_id
            .or_else(|| {
                implicit
                    .as_ref()
                    .map(|transaction| transaction.transaction_id.as_str())
            })
            .ok_or_else(|| Status::internal("credential transaction was not established"))?;
        let principal = crate::object_manager::transaction_principal_from_claims(&claims);
        let input_hash = credential_input_hash("delete", &claims, &req.app_name, &req.request_id);
        let explicit_result_key;
        let result_key = if implicit.is_some() {
            CREDENTIAL_IMPLICIT_RESULT_KEY
        } else {
            explicit_result_key = format!("delete:{}", req.request_id);
            &explicit_result_key
        };
        if let Some(transaction) = implicit.as_ref().filter(|transaction| transaction.replayed) {
            let result = replayed_credential_result(self, transaction, result_key, &input_hash)?;
            return Ok(Response::new(DeleteApplicationCredentialResponse {
                request_id: result.request_id,
                app_id: result.app_id.to_string(),
                write_state: WriteState::Committed as i32,
            }));
        }
        let app = control_journal::read_app_by_tenant_name_in_transaction(
            &self.mvcc,
            transaction_id,
            &principal,
            claims.tenant_id,
            &req.app_name,
        )
        .map_err(|error| Status::internal(error.to_string()))?
        .ok_or_else(|| Status::not_found("Application not found"))?
        .app;

        let audit_event = crate::services::audit::build_tenant_audit_event(
            &claims,
            &req.request_id,
            format!("app:{}", app.name),
            "app.delete",
            serde_json::json!({ "app_id": app.id }),
        )?;
        let audit_event_id = audit_event.audit_event_id.clone();
        let now = u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| Status::internal("credential mutation predates Unix epoch"))?;
        let app = control_journal::plan_delete_app_in_transaction(
            &self.mvcc,
            transaction_id,
            &principal,
            app.id,
            Some(&audit_event),
        )
        .map_err(|error| Status::internal(error.to_string()))?
        .stage(&self.mvcc, transaction_id, &principal, now)
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let result = CredentialMutationResult {
            input_hash,
            operation: "delete".to_string(),
            request_id: req.request_id.clone(),
            tenant_id: claims.tenant_id,
            app_id: app.id,
            app_name: app.name,
            client_id: app.client_id,
            encrypted_secret: Vec::new(),
            audit_event_id,
        };
        stage_credential_result(self, transaction_id, &principal, result_key, &result, now)?;
        if let Some(transaction) = &implicit {
            self.commit_implicit_auth_transaction(transaction).await?;
        }
        Ok(Response::new(DeleteApplicationCredentialResponse {
            request_id: req.request_id,
            app_id: app.id.to_string(),
            write_state: if implicit.is_some() {
                WriteState::Committed as i32
            } else {
                WriteState::Staged as i32
            },
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
        let revision = control_journal::current_control_collection_revision_mvcc(&self.mvcc)
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
        let transaction_id = req
            .context
            .as_ref()
            .map(crate::services::transaction_context::public_context_transaction_id)
            .transpose()?
            .flatten();
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
        let audit_event = crate::services::audit::build_tenant_audit_event(
            claims,
            "policy-grant",
            &req.resource,
            "policy.grant",
            serde_json::json!({ "grantee_app_id": app.id, "action": req.action }),
        )?;
        let implicit = if transaction_id.is_none() {
            Some(
                self.begin_implicit_auth_transaction(claims, req.context.as_ref(), "grant-access")
                    .await?,
            )
        } else {
            None
        };
        let effective_transaction_id = transaction_id
            .or_else(|| {
                implicit
                    .as_ref()
                    .map(|transaction| transaction.transaction_id.as_str())
            })
            .ok_or_else(|| Status::internal("authorization transaction is missing"))?;
        if !implicit
            .as_ref()
            .is_some_and(|transaction| transaction.replayed)
        {
            access_control::stage_delegated_action_tuple_with_tenant_audit(
                &self.storage,
                &self.persistence,
                claims.tenant_id,
                &app.id.to_string(),
                delegated_action,
                &req.resource,
                "add",
                &claims.sub,
                "tenant access grant",
                effective_transaction_id,
                &crate::object_manager::transaction_principal_from_claims(claims),
                implicit.as_ref().map(|_| &audit_event),
            )
            .await?;
        }
        if let Some(transaction) = implicit.as_ref()
            && !transaction.replayed
        {
            self.commit_implicit_auth_transaction(transaction).await?;
        }

        Ok(Response::new(GrantAccessResponse {
            write_state: if implicit.is_none() {
                WriteState::Staged as i32
            } else {
                WriteState::Committed as i32
            },
        }))
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
        let transaction_id = req
            .context
            .as_ref()
            .map(crate::services::transaction_context::public_context_transaction_id)
            .transpose()?
            .flatten();

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
        let audit_event = crate::services::audit::build_tenant_audit_event(
            claims,
            "policy-revoke",
            &req.resource,
            "policy.revoke",
            serde_json::json!({ "grantee_app_id": app.id, "action": req.action }),
        )?;
        let implicit = if transaction_id.is_none() {
            Some(
                self.begin_implicit_auth_transaction(claims, req.context.as_ref(), "revoke-access")
                    .await?,
            )
        } else {
            None
        };
        let effective_transaction_id = transaction_id
            .or_else(|| {
                implicit
                    .as_ref()
                    .map(|transaction| transaction.transaction_id.as_str())
            })
            .ok_or_else(|| Status::internal("authorization transaction is missing"))?;
        if !implicit
            .as_ref()
            .is_some_and(|transaction| transaction.replayed)
        {
            access_control::stage_delegated_action_tuple_with_tenant_audit(
                &self.storage,
                &self.persistence,
                claims.tenant_id,
                &app.id.to_string(),
                delegated_action,
                &req.resource,
                "remove",
                &claims.sub,
                "tenant access revoke",
                effective_transaction_id,
                &crate::object_manager::transaction_principal_from_claims(claims),
                implicit.as_ref().map(|_| &audit_event),
            )
            .await?;
        }
        if let Some(transaction) = implicit.as_ref()
            && !transaction.replayed
        {
            self.commit_implicit_auth_transaction(transaction).await?;
        }

        Ok(Response::new(RevokeAccessResponse {
            write_state: if implicit.is_none() {
                WriteState::Staged as i32
            } else {
                WriteState::Committed as i32
            },
        }))
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
                &self.mvcc,
                crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
            )
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
        require_current_authz_list_revision(&self.mvcc, SYSTEM_STORAGE_TENANT_ID, revision).await?;
        let after_tuple_key =
            decode_authz_page_position(token.as_ref().map(|token| token.position.as_str()))?;
        let grant_page = authz_journal::page_current_authz_tuples(
            &self.mvcc,
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
        let transaction_id = req
            .context
            .as_ref()
            .map(crate::services::transaction_context::public_context_transaction_id)
            .transpose()?
            .flatten();
        let transaction_principal =
            crate::object_manager::transaction_principal_from_claims(claims);

        access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::BucketWrite,
            &req.bucket,
        )
        .await?;

        let implicit = if transaction_id.is_none() {
            Some(
                self.begin_implicit_auth_transaction(
                    claims,
                    req.context.as_ref(),
                    "set-public-access",
                )
                .await?,
            )
        } else {
            None
        };
        let effective_transaction_id = transaction_id
            .or_else(|| {
                implicit
                    .as_ref()
                    .map(|transaction| transaction.transaction_id.as_str())
            })
            .ok_or_else(|| Status::internal("authorization transaction is missing"))?;
        if let Some(transaction) = implicit.as_ref()
            && transaction.replayed
        {
            let bucket =
                bucket_journal::read_current_bucket_mvcc(&self.mvcc, claims.tenant_id, &req.bucket)
                    .map_err(|error| Status::internal(error.to_string()))?
                    .filter(|bucket| bucket.is_public_read == req.allow_public_read)
                    .ok_or_else(|| {
                        Status::already_exists(
                            "public-access idempotency key was already used for different input",
                        )
                    })?;
            let _ = bucket;
        } else {
            let bucket = self
                .persistence
                .stage_bucket_public_access(
                    claims.tenant_id,
                    &req.bucket,
                    req.allow_public_read,
                    effective_transaction_id,
                    &transaction_principal,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            access_control::stage_bucket_public_read_tuple(
                &self.persistence,
                &bucket,
                req.allow_public_read,
                &claims.sub,
                "bucket public-read policy update",
                effective_transaction_id,
                &transaction_principal,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        }
        if let Some(transaction) = implicit.as_ref()
            && !transaction.replayed
        {
            self.commit_implicit_auth_transaction(transaction).await?;
        }

        Ok(Response::new(SetPublicAccessResponse {
            write_state: if implicit.is_none() {
                WriteState::Staged as i32
            } else {
                WriteState::Committed as i32
            },
        }))
    }

    async fn write_authz_tuple(
        &self,
        request: Request<WriteAuthzTupleRequest>,
    ) -> Result<Response<WriteAuthzTupleResponse>, Status> {
        self.write_authz_tuple_impl(request).await
    }

    async fn write_authz_tuples(
        &self,
        request: Request<WriteAuthzTuplesRequest>,
    ) -> Result<Response<WriteAuthzTuplesResponse>, Status> {
        self.write_authz_tuples_impl(request).await
    }

    async fn read_authz_tuples(
        &self,
        request: Request<ReadAuthzTuplesRequest>,
    ) -> Result<Response<ReadAuthzTuplesResponse>, Status> {
        self.read_authz_tuples_impl(request).await
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
                resolve_authz_response_revision(&self.mvcc, claims.tenant_id, consistency).await?
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
        require_current_authz_list_revision(&self.mvcc, claims.tenant_id, response_revision)
            .await?;
        let page = authz_journal::list_current_authz_objects_page(
            &self.storage,
            &self.mvcc,
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
                resolve_authz_response_revision(&self.mvcc, claims.tenant_id, consistency).await?
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
        require_current_authz_list_revision(&self.mvcc, claims.tenant_id, response_revision)
            .await?;
        let page = authz_journal::list_current_authz_subjects_page(
            &self.storage,
            &self.mvcc,
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
        self.put_authz_schema_impl(request).await
    }

    async fn bind_authz_schema(
        &self,
        request: Request<BindAuthzSchemaRequest>,
    ) -> Result<Response<BindAuthzSchemaResponse>, Status> {
        self.bind_authz_schema_impl(request).await
    }

    async fn get_authz_schema_binding(
        &self,
        request: Request<GetAuthzSchemaBindingRequest>,
    ) -> Result<Response<GetAuthzSchemaBindingResponse>, Status> {
        self.get_authz_schema_binding_impl(request).await
    }

    async fn apply_authz_schema(
        &self,
        request: Request<ApplyAuthzSchemaRequest>,
    ) -> Result<Response<ApplyAuthzSchemaResponse>, Status> {
        self.apply_authz_schema_impl(request).await
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
                &self.mvcc,
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
        let binding = authz_realm_schema::read_schema_binding(
            &self.storage,
            &self.mvcc,
            claims.tenant_id,
            DEFAULT_AUTHZ_REALM_ID,
        )
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .ok_or_else(|| Status::not_found("default authorization schema is not bound"))?;
        let schema = authz_realm_schema::read_schema_revision(
            &self.storage,
            &self.mvcc,
            claims.tenant_id,
            &binding.schema_ref.schema_id,
            Some(binding.schema_ref.schema_revision),
        )
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .ok_or_else(|| Status::not_found("bound authorization schema revision not found"))?;
        if schema.schema_ref != binding.schema_ref {
            return Err(Status::data_loss(
                "bound authorization schema reference does not match its revision",
            ));
        }
        let namespaces = if req.namespace.is_empty() {
            schema.namespaces
        } else {
            schema
                .namespaces
                .into_iter()
                .filter(|record| record.namespace == req.namespace)
                .collect()
        };
        Ok(Response::new(GetAuthzSchemaResponse {
            namespaces,
            schema_version: binding.schema_ref.schema_revision,
            schema_ref: Some(schema_ref_response(&binding.schema_ref)),
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
        let mvcc = self.mvcc.clone();
        let tenant_id = claims.tenant_id;
        let namespace = encode_optional_realm_namespace(&scope.authz_realm_id, &req.namespace);

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_revision = after_revision;
            let mut poll = tokio::time::interval(std::time::Duration::from_millis(100));
            loop {
                loop {
                    let page = match authz_journal::list_authz_tuple_log_page(
                        &mvcc,
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

                poll.tick().await;
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
        let mvcc = self.mvcc.clone();
        let namespace = req.namespace;
        let tenant_id = claims.tenant_id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            loop {
                loop {
                    let page = match authz_namespace_watch::list_authz_namespace_watch_event_page(
                        &mvcc,
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
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
        let mvcc = self.mvcc.clone();
        let derived_index_id = req.derived_index_id;
        let tenant_id = claims.tenant_id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            loop {
                loop {
                    let page =
                        match authz_derived_lag_watch::list_authz_derived_lag_watch_event_page(
                            &mvcc,
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
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
