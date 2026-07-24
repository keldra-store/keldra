use super::*;

pub(super) fn public_link_context(
    context: Option<&PublicMutationContext>,
    create: bool,
) -> Result<&PublicMutationContext, Status> {
    let context = context.ok_or_else(|| Status::invalid_argument("Missing mutation context"))?;
    if context.request_id.trim().is_empty() {
        return Err(Status::invalid_argument("request_id is required"));
    }
    if context.idempotency_key.trim().is_empty() {
        return Err(Status::invalid_argument("idempotency_key is required"));
    }
    if !create && context.expected_generation == 0 {
        return Err(Status::invalid_argument("expected_generation is required"));
    }
    crate::services::saga_reserved::reject_public_saga_context(context)?;
    Ok(context)
}

pub(super) fn public_context_transaction_id(
    context: &PublicMutationContext,
) -> Result<Option<&str>, Status> {
    crate::services::saga_reserved::public_context_transaction_id(context)
}

pub(super) fn validate_public_tenant_locator(
    claims: &auth::Claims,
    tenant_id: &str,
) -> Result<(), Status> {
    let tenant_id = tenant_id.trim();
    if tenant_id.is_empty() || tenant_id == claims.tenant_id.to_string() {
        return Ok(());
    }
    Err(Status::permission_denied(
        "Request tenant_id does not match authenticated tenant",
    ))
}

pub(super) async fn public_link_bucket(
    state: &AppState,
    claims: &auth::Claims,
    bucket_name: &str,
) -> Result<crate::persistence::Bucket, Status> {
    if bucket_name.trim().is_empty() {
        return Err(Status::invalid_argument("bucket_name is required"));
    }
    state
        .persistence
        .get_bucket_by_name(claims.tenant_id, bucket_name)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::not_found("Bucket not found"))
}

pub(super) async fn require_object_link_scope(
    state: &AppState,
    claims: &auth::Claims,
    bucket_name: &str,
    link_key: &str,
    action: AnvilAction,
) -> Result<(), Status> {
    if crate::validation::is_reserved_internal_key(link_key) {
        return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
    }
    crate::access_control::require_action(
        &state.storage,
        &state.persistence,
        claims,
        action,
        &format!("{bucket_name}/{link_key}"),
    )
    .await
}

pub(super) async fn public_host_alias_bucket(
    state: &AppState,
    claims: &auth::Claims,
    bucket_name: &str,
) -> Result<crate::persistence::Bucket, Status> {
    public_link_bucket(state, claims, bucket_name).await
}

pub(super) async fn public_host_alias_descriptor(
    state: &AppState,
    claims: &auth::Claims,
    hostname: &str,
) -> Result<CoreHostAliasDescriptor, Status> {
    let descriptor = state
        .persistence
        .get_host_alias_descriptor(hostname)
        .await
        .map_err(lifecycle_status)?
        .ok_or_else(|| Status::not_found("Host alias not found"))?;
    if descriptor.tenant_id != claims.tenant_id.to_string() {
        return Err(Status::not_found("Host alias not found"));
    }
    Ok(descriptor)
}

pub(super) async fn require_bucket_scope(
    state: &AppState,
    claims: &auth::Claims,
    bucket_name: &str,
    action: AnvilAction,
) -> Result<(), Status> {
    crate::access_control::require_action(
        &state.storage,
        &state.persistence,
        claims,
        action,
        bucket_name,
    )
    .await
}

pub(super) async fn public_routing_config_for_region(
    state: &AppState,
    region_name: &str,
) -> Result<RoutingConfig, Status> {
    let region_name = region_name.trim();
    if region_name.is_empty() {
        return Err(Status::invalid_argument("region is required"));
    }
    let region = state
        .persistence
        .list_region_descriptors()
        .await
        .map_err(lifecycle_status)?
        .into_iter()
        .find(|region| region.region == region_name)
        .ok_or_else(|| Status::not_found("Region not found"))?;
    let base_domain =
        public_base_domain_from_region_suffix(&region.region, &region.virtual_host_suffix)?;
    RoutingConfig::new(base_domain).map_err(|err| Status::invalid_argument(err.to_string()))
}

pub(super) fn public_base_domain_from_region_suffix(
    region: &str,
    virtual_host_suffix: &str,
) -> Result<String, Status> {
    let suffix = routing::normalize_alias_hostname(virtual_host_suffix)
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
    let region_prefix = format!(
        "{}.",
        region.trim().trim_end_matches('.').to_ascii_lowercase()
    );
    Ok(suffix
        .strip_prefix(&region_prefix)
        .unwrap_or(&suffix)
        .to_string())
}

pub(super) fn parse_optional_uuid(
    field_name: &'static str,
    value: String,
) -> Result<Option<uuid::Uuid>, Status> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    value
        .parse::<uuid::Uuid>()
        .map(Some)
        .map_err(|_| Status::invalid_argument(format!("Invalid {field_name}")))
}

pub(super) fn object_link_status(err: object_links::ObjectLinkError) -> Status {
    match err {
        object_links::ObjectLinkError::InvalidLinkKey
        | object_links::ObjectLinkError::InvalidTargetKey
        | object_links::ObjectLinkError::MissingExpectedGeneration => {
            Status::invalid_argument(err.to_string())
        }
        object_links::ObjectLinkError::AlreadyExists => Status::already_exists(err.to_string()),
        object_links::ObjectLinkError::BucketNotFound | object_links::ObjectLinkError::NotFound => {
            Status::not_found(err.to_string())
        }
        object_links::ObjectLinkError::BucketTenantMismatch => {
            Status::not_found("Bucket not found")
        }
        object_links::ObjectLinkError::GenerationConflict { .. } => {
            Status::aborted(err.to_string())
        }
        object_links::ObjectLinkError::ExistingObjectIsNotLink
        | object_links::ObjectLinkError::DanglingObjectLink
        | object_links::ObjectLinkError::TargetNotBlob
        | object_links::ObjectLinkError::LinkLoop
        | object_links::ObjectLinkError::LinkDepthExceeded => {
            Status::failed_precondition(err.to_string())
        }
        object_links::ObjectLinkError::Internal(message) => transaction_core_store_status(&message)
            .unwrap_or_else(|| Status::internal(format!("internal object-link error: {message}"))),
    }
}

pub(super) fn transaction_core_store_status(message: &str) -> Option<Status> {
    if message.contains("TransactionNotFound") {
        Some(Status::not_found("TransactionNotFound"))
    } else if message.contains("TransactionPrincipalMismatch") {
        Some(Status::permission_denied("TransactionPrincipalMismatch"))
    } else if message.contains("TransactionScopeMismatch") {
        Some(Status::failed_precondition("TransactionScopeMismatch"))
    } else if message.contains("TransactionExpired")
        || message.contains("TransactionRolledBack")
        || message.contains("TransactionAlreadyCommitted")
        || message.contains("TransactionNotOpen")
        || message.contains("TransactionNotCommittable")
    {
        Some(Status::failed_precondition(message.to_string()))
    } else if message.contains("TransactionConflict") {
        Some(Status::aborted("TransactionConflict"))
    } else if message.contains("idempotency conflict") {
        Some(Status::already_exists("TransactionConflict"))
    } else {
        None
    }
}

pub(super) fn object_link_resolution_from_proto(
    value: i32,
) -> Result<object_links::ObjectLinkResolution, Status> {
    match value {
        1 => Ok(object_links::ObjectLinkResolution::Follow),
        2 => Ok(object_links::ObjectLinkResolution::Redirect),
        _ => Err(Status::invalid_argument("Invalid object link resolution")),
    }
}

pub(super) fn object_link_resolution_to_proto(value: object_links::ObjectLinkResolution) -> i32 {
    match value {
        object_links::ObjectLinkResolution::Follow => 1,
        object_links::ObjectLinkResolution::Redirect => 2,
    }
}

pub(super) fn object_link_descriptor_to_proto(
    value: object_links::ObjectLinkDescriptor,
) -> crate::anvil_api::ObjectLinkDescriptor {
    crate::anvil_api::ObjectLinkDescriptor {
        schema: value.schema,
        tenant_id: value.tenant_id,
        bucket_name: value.bucket_name,
        link_key: value.link_key,
        target_key: value.target_key,
        target_version: value.target_version.unwrap_or_default(),
        resolution: object_link_resolution_to_proto(value.resolution),
        created_at: value
            .created_at
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        updated_at: value
            .updated_at
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        created_by: value.created_by,
        generation: value.generation,
    }
}

pub(super) fn lifecycle_status(err: LifecycleError) -> Status {
    match err {
        LifecycleError::InvalidArgument(message) => Status::invalid_argument(message),
        LifecycleError::AlreadyExists { .. } => Status::already_exists(err.to_string()),
        LifecycleError::NotFound { .. } => Status::not_found(err.to_string()),
        LifecycleError::GenerationConflict { .. } => Status::aborted(err.to_string()),
        LifecycleError::LifecycleTransitionDenied { .. }
        | LifecycleError::ActivationCheckpointNotReached { .. } => {
            Status::failed_precondition(err.to_string())
        }
        LifecycleError::Io(_) | LifecycleError::Json(_) | LifecycleError::Other(_) => {
            Status::internal(err.to_string())
        }
    }
}

pub(super) fn host_alias_state_to_proto(value: CoreHostAliasState) -> i32 {
    match value {
        CoreHostAliasState::PendingVerification => 1,
        CoreHostAliasState::Active => 2,
        CoreHostAliasState::Suspended => 3,
        CoreHostAliasState::Deleted => 4,
    }
}

pub(super) fn host_alias_descriptor_to_proto(
    value: CoreHostAliasDescriptor,
) -> crate::anvil_api::HostAliasDescriptor {
    let verification_challenge = host_alias_verification_challenge(&value);
    crate::anvil_api::HostAliasDescriptor {
        schema: value.schema,
        hostname: value.hostname,
        tenant_id: value.tenant_id,
        bucket_name: value.bucket_name,
        region: value.region,
        prefix: value.prefix,
        state: host_alias_state_to_proto(value.state),
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
        verification_challenge,
    }
}

pub(super) fn host_alias_verification_challenge(value: &CoreHostAliasDescriptor) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(value.hostname.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.tenant_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.bucket_name.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.region.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.prefix.as_bytes());
    format!("anvil-host-alias={}", hasher.finalize().to_hex())
}

pub(super) fn none_if_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() { None } else { Some(value) }
}
