use crate::anvil_api::registry_service_server::RegistryService;
use crate::anvil_api::*;
use crate::{AppState, access_control, auth, gateway_store, middleware, permissions::AnvilAction};
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};

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
        let transaction_id = registry_transaction_id(req.options.as_ref())?;
        let expected_digest = req.digest.clone();
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
            transaction_id,
        )
        .await
        .map_err(registry_status)?;
        access_control::grant_registry_namespace_defaults(
            &self.persistence,
            claims.tenant_id,
            &registry_namespace_resource(&req.registry_kind, &req.namespace),
            &claims.sub,
            &claims.sub,
            "grant registry namespace owner",
        )
        .await
        .map_err(registry_status)?;
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
        let transaction_id = registry_transaction_id(req.options.as_ref())?;
        let manifest_digest = digest_bytes(req.manifest_json.as_bytes());
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
            transaction_id,
        )
        .await
        .map_err(registry_status)?;
        access_control::grant_registry_namespace_defaults(
            &self.persistence,
            claims.tenant_id,
            &registry_namespace_resource(&req.registry_kind, &req.namespace),
            &claims.sub,
            &claims.sub,
            "grant registry namespace owner",
        )
        .await
        .map_err(registry_status)?;
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
        let transaction_id = registry_transaction_id(req.options.as_ref())?;
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
            transaction_id,
        )
        .await
        .map_err(registry_status)?;
        access_control::grant_registry_namespace_defaults(
            &self.persistence,
            claims.tenant_id,
            &registry_namespace_resource(&req.registry_kind, &req.namespace),
            &claims.sub,
            &claims.sub,
            "grant registry namespace owner",
        )
        .await
        .map_err(registry_status)?;
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
