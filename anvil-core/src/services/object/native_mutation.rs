use super::*;

pub(super) struct NativeMutationAttempt<'a> {
    context: &'a NativeMutationContext,
    _idempotency_guard: OwnedMutexGuard<()>,
    _target_guard: OwnedMutexGuard<()>,
}

pub(super) async fn begin_native_mutation<'a, T>(
    state: &AppState,
    context: Option<&'a NativeMutationContext>,
    target: &NativeIdempotencyTarget,
    claims: &auth::Claims,
    action: AnvilAction,
) -> Result<(NativeMutationAttempt<'a>, Option<T>), Status>
where
    T: DeserializeOwned,
{
    let context =
        context.ok_or_else(|| Status::invalid_argument("Missing native mutation context"))?;
    validate_native_mutation_target_authorization(state, claims, target, action).await?;
    let idempotency_guard = acquire_native_mutation_lock(state, context).await?;
    let target_guard = acquire_native_target_lock(state, context, target).await?;
    let replay = native_idempotency::load_response(&state.storage, context, target).await?;
    Ok((
        NativeMutationAttempt {
            context,
            _idempotency_guard: idempotency_guard,
            _target_guard: target_guard,
        },
        replay,
    ))
}

pub(super) async fn validate_native_mutation_target_authorization(
    state: &AppState,
    claims: &auth::Claims,
    target: &NativeIdempotencyTarget,
    action: AnvilAction,
) -> Result<(), Status> {
    if !crate::validation::is_valid_bucket_name(&target.bucket_name) {
        return Err(Status::invalid_argument("Invalid bucket name"));
    }
    if crate::validation::is_reserved_internal_key(&target.object_key) {
        return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
    }
    if !crate::validation::is_valid_object_key(&target.object_key) {
        return Err(Status::invalid_argument("Invalid object key"));
    }
    crate::access_control::require_action(
        &state.storage,
        &state.persistence,
        claims,
        action,
        &format!("{}/{}", target.bucket_name, target.object_key),
    )
    .await
}

pub(super) async fn complete_native_mutation<T>(
    state: &AppState,
    attempt: &NativeMutationAttempt<'_>,
    target: &NativeIdempotencyTarget,
    response: &T,
) -> Result<(), Status>
where
    T: Serialize,
{
    native_idempotency::store_response(&state.storage, attempt.context, target, response).await
}

pub(super) async fn acquire_native_mutation_lock(
    state: &AppState,
    context: &NativeMutationContext,
) -> Result<OwnedMutexGuard<()>, Status> {
    acquire_native_lock_key(state, native_mutation_lock_key(context)).await
}

pub(super) async fn acquire_native_target_lock(
    state: &AppState,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
) -> Result<OwnedMutexGuard<()>, Status> {
    acquire_native_lock_key(
        state,
        native_target_lock_key(context.tenant_id, &target.bucket_name, &target.object_key),
    )
    .await
}

pub(super) async fn acquire_native_lock_key(
    state: &AppState,
    lock_key: String,
) -> Result<OwnedMutexGuard<()>, Status> {
    let lock = {
        let mut locks = state.native_mutation_locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(&lock_key).and_then(std::sync::Weak::upgrade) {
            lock
        } else {
            let lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
            locks.insert(lock_key, std::sync::Arc::downgrade(&lock));
            lock
        }
    };
    Ok(lock.lock_owned().await)
}

pub(super) fn native_mutation_lock_key(context: &NativeMutationContext) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&context.tenant_id.to_le_bytes());
    hasher.update(&context.bucket_id.to_le_bytes());
    hasher.update(context.principal.as_bytes());
    hasher.update(&[0]);
    hasher.update(context.idempotency_key.as_bytes());
    hasher.finalize().to_hex().to_string()
}

pub(super) fn native_target_lock_key(
    tenant_id: i64,
    bucket_name: &str,
    object_key: &str,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"native-target");
    hasher.update(&tenant_id.to_le_bytes());
    hasher.update(bucket_name.as_bytes());
    hasher.update(&[0]);
    hasher.update(object_key.as_bytes());
    hasher.finalize().to_hex().to_string()
}

pub(super) async fn validate_native_mutation_context(
    state: &AppState,
    claims: &auth::Claims,
    bucket_name: &str,
    context: Option<&NativeMutationContext>,
) -> Result<(), Status> {
    let context =
        context.ok_or_else(|| Status::invalid_argument("Missing native mutation context"))?;
    if context.tenant_id != claims.tenant_id {
        return Err(Status::permission_denied("Native mutation tenant mismatch"));
    }
    if context.principal != claims.sub {
        return Err(Status::permission_denied(
            "Native mutation principal mismatch",
        ));
    }
    require_native_context_field("request_id", &context.request_id)?;
    require_native_context_field("precondition", &context.precondition)?;
    require_native_context_field("idempotency_key", &context.idempotency_key)?;
    let bucket = bucket_journal::read_current_bucket(&state.storage, claims.tenant_id, bucket_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found("BucketNotFound"))?;
    if context.bucket_id > 0 && bucket.id != context.bucket_id {
        return Err(Status::permission_denied("Native mutation bucket mismatch"));
    }

    if let Some(required_revision) = parse_authz_zookie(&context.authz_zookie_optional)? {
        let latest = authz_journal::latest_authz_revision(&state.storage, claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if latest < required_revision {
            return Err(Status::failed_precondition("AuthzRevisionUnavailable"));
        }
    }

    Ok(())
}

pub(super) enum NativeMutationPrecondition<'a> {
    None,
    Exists,
    NotExists,
    Version(uuid::Uuid),
    Etag(&'a str),
}

pub(super) async fn enforce_native_mutation_precondition(
    state: &AppState,
    claims: &auth::Claims,
    bucket_name: &str,
    object_key: &str,
    context: Option<&NativeMutationContext>,
    action: AnvilAction,
) -> Result<(), Status> {
    prepare_native_mutation_precondition(
        state,
        claims,
        bucket_name,
        object_key,
        context,
        action,
        None,
    )
    .await
    .map(|_| ())
}

pub(super) async fn prepare_native_mutation_precondition(
    state: &AppState,
    claims: &auth::Claims,
    bucket_name: &str,
    object_key: &str,
    context: Option<&NativeMutationContext>,
    action: AnvilAction,
    transaction: Option<&crate::core_store::CoreTransaction>,
) -> Result<Option<crate::core_store::CoreMutationPrecondition>, Status> {
    let context =
        context.ok_or_else(|| Status::invalid_argument("Missing native mutation context"))?;
    let precondition = parse_native_mutation_precondition(&context.precondition)?;
    if matches!(precondition, NativeMutationPrecondition::None) {
        return Ok(None);
    }

    let snapshot = state
        .object_manager
        .object_mutation_precondition_snapshot(claims, bucket_name, object_key, action, transaction)
        .await?;
    let current = snapshot
        .object
        .as_ref()
        .filter(|object| object.deleted_at.is_none());

    let satisfied = match precondition {
        NativeMutationPrecondition::None => true,
        NativeMutationPrecondition::Exists => current.is_some(),
        NativeMutationPrecondition::NotExists => current.is_none(),
        NativeMutationPrecondition::Version(expected) => current
            .map(|object| object.version_id == expected)
            .unwrap_or(false),
        NativeMutationPrecondition::Etag(expected) => current
            .map(|object| etag_matches(&object.etag, expected))
            .unwrap_or(false),
    };
    if !satisfied {
        return Err(Status::failed_precondition(
            "Native mutation precondition failed",
        ));
    }
    Ok(Some(snapshot.precondition))
}

pub(super) fn parse_native_mutation_precondition(
    value: &str,
) -> Result<NativeMutationPrecondition<'_>, Status> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("none") {
        return Ok(NativeMutationPrecondition::None);
    }
    if value.eq_ignore_ascii_case("exists") {
        return Ok(NativeMutationPrecondition::Exists);
    }
    if value.eq_ignore_ascii_case("not_exists")
        || value.eq_ignore_ascii_case("not-exists")
        || value.eq_ignore_ascii_case("absent")
    {
        return Ok(NativeMutationPrecondition::NotExists);
    }
    if let Some(version) = value.strip_prefix("version:") {
        let version = uuid::Uuid::parse_str(version.trim()).map_err(|_| {
            Status::invalid_argument("Invalid native mutation version precondition")
        })?;
        return Ok(NativeMutationPrecondition::Version(version));
    }
    if let Some(etag) = value.strip_prefix("etag:") {
        let etag = etag.trim();
        if etag.is_empty() {
            return Err(Status::invalid_argument(
                "Invalid native mutation etag precondition",
            ));
        }
        return Ok(NativeMutationPrecondition::Etag(etag));
    }
    Err(Status::invalid_argument(
        "Unsupported native mutation precondition",
    ))
}

pub(super) fn etag_matches(actual: &str, expected: &str) -> bool {
    actual == expected || trim_etag_quotes(actual) == trim_etag_quotes(expected)
}

pub(super) fn trim_etag_quotes(value: &str) -> &str {
    value.trim().trim_matches('"')
}

pub(super) fn require_native_context_field(name: &str, value: &str) -> Result<(), Status> {
    if value.trim().is_empty() {
        return Err(Status::invalid_argument(format!(
            "Native mutation {name} is required"
        )));
    }
    Ok(())
}

pub(super) fn parse_authz_zookie(value: &str) -> Result<Option<i64>, Status> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let revision = value
        .strip_prefix("authz:")
        .unwrap_or(value)
        .parse::<i64>()
        .map_err(|_| Status::invalid_argument("Invalid authz_zookie_optional"))?;
    if revision < 0 {
        return Err(Status::invalid_argument("Invalid authz_zookie_optional"));
    }
    Ok(Some(revision))
}
