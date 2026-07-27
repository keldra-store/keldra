use crate::anvil_api::registry_service_server::RegistryService;
use crate::anvil_api::*;
use crate::{AppState, access_control, auth, gateway_store, middleware, permissions::AnvilAction};
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};

struct RegistryMutationTransaction {
    id: String,
    principal: String,
    internal: bool,
    replayed: bool,
}

#[tonic::async_trait]
impl RegistryService for AppState {
    async fn put_package_blob(
        &self,
        request: Request<PutPackageBlobRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let request_id = request_id(&request);
        let claims = registry_claims(&request)?;
        let req = request.into_inner();
        enforce_registry_scope(
            self,
            &claims,
            AnvilAction::RegistryBlobWrite,
            &registry_resource(&req.registry_kind, &req.namespace, None),
        )
        .await?;
        let transaction =
            begin_registry_mutation(self, &claims, req.options.as_ref(), "blob").await?;
        let expected_digest = req.digest.clone();
        if transaction.replayed {
            return Ok(Response::new(write_response(
                request_id,
                expected_digest,
                req.options.as_ref(),
            )));
        }
        gateway_store::put_registry_blob(
            &self.storage,
            self.mvcc.as_ref(),
            claims.tenant_id,
            &req.registry_kind,
            &req.namespace,
            &req.digest,
            &req.media_type,
            &req.inline_body,
            &claims.sub,
            Some(&transaction.id),
        )
        .await
        .map_err(registry_status)?;
        stage_registry_namespace_defaults(
            self,
            &claims,
            &req.registry_kind,
            &req.namespace,
            &transaction,
        )
        .await
        .map_err(registry_status)?;
        commit_registry_mutation(self, &transaction).await?;
        Ok(Response::new(write_response(
            request_id,
            expected_digest,
            req.options.as_ref(),
        )))
    }

    async fn put_package_version(
        &self,
        request: Request<PutPackageVersionRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let request_id = request_id(&request);
        let claims = registry_claims(&request)?;
        let req = request.into_inner();
        enforce_registry_scope(
            self,
            &claims,
            AnvilAction::RegistryVersionWrite,
            &registry_resource(&req.registry_kind, &req.namespace, Some(&req.package_name)),
        )
        .await?;
        let transaction =
            begin_registry_mutation(self, &claims, req.options.as_ref(), "version").await?;
        let manifest_digest = digest_bytes(req.manifest_json.as_bytes());
        if transaction.replayed {
            return Ok(Response::new(write_response(
                request_id,
                manifest_digest,
                req.options.as_ref(),
            )));
        }
        gateway_store::put_package_version(
            &self.storage,
            self.mvcc.as_ref(),
            claims.tenant_id,
            &req.registry_kind,
            &req.namespace,
            &req.package_name,
            &req.version,
            &req.manifest_json,
            &req.blob_digests,
            &claims.sub,
            None,
            Some(&transaction.id),
        )
        .await
        .map_err(registry_status)?;
        stage_registry_namespace_defaults(
            self,
            &claims,
            &req.registry_kind,
            &req.namespace,
            &transaction,
        )
        .await
        .map_err(registry_status)?;
        commit_registry_mutation(self, &transaction).await?;
        Ok(Response::new(write_response(
            request_id,
            manifest_digest,
            req.options.as_ref(),
        )))
    }

    async fn put_registry_ref(
        &self,
        request: Request<PutRegistryRefRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let request_id = request_id(&request);
        let claims = registry_claims(&request)?;
        let req = request.into_inner();
        enforce_registry_scope(
            self,
            &claims,
            AnvilAction::RegistryRefWrite,
            &registry_resource(&req.registry_kind, &req.namespace, Some(&req.package_name)),
        )
        .await?;
        let transaction =
            begin_registry_mutation(self, &claims, req.options.as_ref(), "ref").await?;
        if transaction.replayed {
            let target = gateway_store::get_package_version(
                &self.storage,
                self.mvcc.as_ref(),
                claims.tenant_id,
                &req.registry_kind,
                &req.namespace,
                &req.package_name,
                &req.target_version,
            )
            .await
            .map_err(registry_status)?
            .ok_or_else(|| {
                Status::already_exists(
                    "registry idempotency key was already used for different input",
                )
            })?;
            return Ok(Response::new(write_response(
                request_id,
                target.manifest_ref,
                req.options.as_ref(),
            )));
        }
        let receipt = gateway_store::put_registry_ref(
            &self.storage,
            self.mvcc.as_ref(),
            claims.tenant_id,
            &req.registry_kind,
            &req.namespace,
            &req.package_name,
            &req.ref_name,
            &req.target_version,
            &claims.sub,
            None,
            Some(&transaction.id),
        )
        .await
        .map_err(registry_status)?;
        stage_registry_namespace_defaults(
            self,
            &claims,
            &req.registry_kind,
            &req.namespace,
            &transaction,
        )
        .await
        .map_err(registry_status)?;
        commit_registry_mutation(self, &transaction).await?;
        Ok(Response::new(write_response(
            request_id,
            receipt.record.target_digest,
            req.options.as_ref(),
        )))
    }

    async fn get_package_version(
        &self,
        request: Request<GetPackageVersionRequest>,
    ) -> Result<Response<PackageVersion>, Status> {
        let claims = registry_claims(&request)?;
        let req = request.into_inner();
        enforce_registry_scope(
            self,
            &claims,
            AnvilAction::RegistryRead,
            &registry_resource(&req.registry_kind, &req.namespace, Some(&req.package_name)),
        )
        .await?;
        let version = gateway_store::get_package_version(
            &self.storage,
            self.mvcc.as_ref(),
            claims.tenant_id,
            &req.registry_kind,
            &req.namespace,
            &req.package_name,
            &req.version,
        )
        .await
        .map_err(registry_status)?
        .ok_or_else(|| Status::not_found("registry package version not found"))?;
        Ok(Response::new(package_version(version)))
    }

    async fn list_package_versions(
        &self,
        request: Request<ListPackageVersionsRequest>,
    ) -> Result<Response<ListPackageVersionsResponse>, Status> {
        let claims = registry_claims(&request)?;
        let req = request.into_inner();
        enforce_registry_scope(
            self,
            &claims,
            AnvilAction::RegistryList,
            &registry_resource(&req.registry_kind, &req.namespace, Some(&req.package_name)),
        )
        .await?;
        let (versions, next_page_token) = gateway_store::list_package_versions(
            self.mvcc.as_ref(),
            claims.tenant_id,
            &req.registry_kind,
            &req.namespace,
            &req.package_name,
            usize::try_from(req.limit).unwrap_or(1000),
            &req.page_token,
        )
        .await
        .map_err(registry_status)?;
        Ok(Response::new(ListPackageVersionsResponse {
            versions: versions.into_iter().map(package_version).collect(),
            next_page_token: next_page_token.unwrap_or_default(),
        }))
    }
}

fn registry_claims<T>(request: &Request<T>) -> Result<auth::Claims, Status> {
    request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))
}

async fn enforce_registry_scope(
    state: &AppState,
    claims: &auth::Claims,
    action: AnvilAction,
    resource: &str,
) -> Result<(), Status> {
    access_control::require_action(&state.storage, &state.persistence, claims, action, resource)
        .await
        .map_err(|status| {
            if status.code() == tonic::Code::PermissionDenied {
                Status::permission_denied("registry access denied")
            } else {
                status
            }
        })
}

fn registry_resource(registry_kind: &str, namespace: &str, package_name: Option<&str>) -> String {
    match package_name {
        Some(package_name) => format!(
            "{}/{}",
            registry_namespace_resource(registry_kind, namespace),
            package_name
        ),
        None => registry_namespace_resource(registry_kind, namespace),
    }
}

fn registry_namespace_resource(registry_kind: &str, namespace: &str) -> String {
    format!("registry/{registry_kind}/{namespace}")
}

async fn begin_registry_mutation(
    state: &AppState,
    claims: &auth::Claims,
    options: Option<&WriteOptions>,
    operation: &str,
) -> Result<RegistryMutationTransaction, Status> {
    let principal = crate::object_manager::transaction_principal_from_claims(claims);
    if let Some(transaction_id) = registry_transaction_id(options)? {
        state
            .mvcc
            .open_transactions
            .binding(transaction_id, &principal)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        return Ok(RegistryMutationTransaction {
            id: transaction_id.to_string(),
            principal,
            internal: false,
            replayed: false,
        });
    }
    let supplied = options
        .map(|options| options.idempotency_key.trim())
        .filter(|key| !key.is_empty());
    let idempotency_key = supplied.map_or_else(
        || {
            format!(
                "registry:{}:{}:{operation}:{}",
                claims.tenant_id,
                claims.sub,
                uuid::Uuid::new_v4()
            )
        },
        |key| format!("registry:{}:{}:{key}", claims.tenant_id, claims.sub),
    );
    let now = u64::try_from(chrono::Utc::now().timestamp_millis())
        .map_err(|_| Status::internal("registry mutation timestamp predates Unix epoch"))?;
    let handle = state
        .mvcc
        .open_transactions
        .begin(
            state.mvcc.runtime.as_ref(),
            state.mvcc.cluster_id().to_string(),
            principal.clone(),
            idempotency_key,
            std::time::Duration::from_secs(300),
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            crate::mvcc_transaction::ReadConsistency::Linearized,
            now,
        )
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let status = state
        .mvcc
        .open_transactions
        .status(&handle.transaction_id, &principal, now)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    if status.state == "committing" {
        let outcome = state
            .mvcc
            .open_transactions
            .commit(
                state.mvcc.runtime.as_ref(),
                &handle.transaction_id,
                &principal,
                now,
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if let crate::mvcc_transaction::CertificationResult::Aborted { reason } =
            outcome.certification
        {
            return Err(Status::aborted(format!(
                "implicit registry transaction aborted: {reason:?}"
            )));
        }
    } else if status.state == "aborted" {
        return Err(Status::aborted(
            "implicit registry transaction previously aborted",
        ));
    }
    Ok(RegistryMutationTransaction {
        id: handle.transaction_id,
        principal,
        internal: true,
        replayed: matches!(status.state, "committed" | "committing"),
    })
}

async fn stage_registry_namespace_defaults(
    state: &AppState,
    claims: &auth::Claims,
    registry_kind: &str,
    namespace: &str,
    transaction: &RegistryMutationTransaction,
) -> anyhow::Result<()> {
    let namespace = registry_namespace_resource(registry_kind, namespace);
    let object_id =
        access_control::registry_namespace_object_id(claims.tenant_id, &namespace);
    state
        .persistence
        .stage_authz_tuple_batch(
            crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
            vec![
                crate::persistence::AuthzTupleBatchMutation {
                    namespace: access_control::system_realm_namespace(
                        crate::system_realm::SYSTEM_REGISTRY_NAMESPACE,
                    ),
                    object_id: object_id.clone(),
                    relation: "parent_tenant".to_string(),
                    subject_kind:
                        crate::system_realm::SYSTEM_STORAGE_TENANT_NAMESPACE.to_string(),
                    subject_id:
                        access_control::storage_tenant_object_id(claims.tenant_id),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: "stage registry namespace owner".to_string(),
                },
                crate::persistence::AuthzTupleBatchMutation {
                    namespace: access_control::system_realm_namespace(
                        crate::system_realm::SYSTEM_REGISTRY_NAMESPACE,
                    ),
                    object_id,
                    relation: "owner".to_string(),
                    subject_kind: access_control::APP_SUBJECT_KIND.to_string(),
                    subject_id: claims.sub.clone(),
                    caveat_hash: String::new(),
                    operation: "add".to_string(),
                    reason: "stage registry namespace owner".to_string(),
                },
            ],
            &claims.sub,
            &transaction.id,
            &transaction.principal,
            None,
        )
        .await?;
    Ok(())
}

async fn commit_registry_mutation(
    state: &AppState,
    transaction: &RegistryMutationTransaction,
) -> Result<(), Status> {
    if !transaction.internal {
        return Ok(());
    }
    let outcome = state
        .mvcc
        .open_transactions
        .commit(
            state.mvcc.runtime.as_ref(),
            &transaction.id,
            &transaction.principal,
            u64::try_from(chrono::Utc::now().timestamp_millis())
                .map_err(|_| Status::internal("registry commit predates Unix epoch"))?,
        )
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    match outcome.certification {
        crate::mvcc_transaction::CertificationResult::Committed { .. } => Ok(()),
        crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
            Err(Status::aborted(format!(
                "implicit registry transaction aborted: {reason:?}"
            )))
        }
    }
}

fn registry_transaction_id(options: Option<&WriteOptions>) -> Result<Option<&str>, Status> {
    crate::services::transaction_context::write_options_transaction_id(options)
}

fn write_response(
    request_id: String,
    mutation_id: String,
    options: Option<&WriteOptions>,
) -> WriteResponse {
    let state = if crate::services::transaction_context::write_options_is_transactional(options) {
        WriteState::Staged
    } else if options
        .map(|options| {
            options.wait_for_finalization
                || options.consistency == ConsistencyMode::Finalised as i32
        })
        .unwrap_or(true)
    {
        WriteState::Finalised
    } else {
        WriteState::Committed
    };
    WriteResponse {
        request_id,
        mutation_id,
        state: state as i32,
        root_generation: None,
        transaction_manifest_ref: None,
        idempotency_outcome: "accepted".to_string(),
        retry_after_hint: None,
        finalisation_error: None,
    }
}

fn package_version(version: gateway_store::GatewayPackageVersionRecord) -> PackageVersion {
    PackageVersion {
        registry_kind: version.registry_kind,
        namespace: version.namespace,
        package_name: version.package_name,
        version: version.version,
        manifest_ref: version.manifest_ref,
    }
}

fn digest_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

fn request_id<T>(request: &Request<T>) -> String {
    request
        .extensions()
        .get::<middleware::AnvilRequestId>()
        .map(|request_id| request_id.0.clone())
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string())
}

fn registry_status(error: anyhow::Error) -> Status {
    let message = error.to_string();
    if message.contains("not found") || message.contains("missing") {
        Status::not_found(message)
    } else if message.contains("invalid")
        || message.contains("must")
        || message.contains("mismatch")
    {
        Status::invalid_argument(message)
    } else {
        Status::internal(message)
    }
}
