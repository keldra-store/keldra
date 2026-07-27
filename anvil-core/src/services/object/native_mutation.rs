use super::*;

pub(super) struct NativeMutationAttempt<'a> {
    context: &'a NativeMutationContext,
    _idempotency_guard: OwnedMutexGuard<()>,
    _target_guard: OwnedMutexGuard<()>,
}

pub(super) struct ImplicitNativeTransaction {
    pub transaction_id: String,
    pub principal: String,
}

pub(super) async fn begin_implicit_native_transaction(
    state: &AppState,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
    claims: &auth::Claims,
) -> Result<ImplicitNativeTransaction, Status> {
    if context.transaction_id.is_some() {
        return Err(Status::invalid_argument(
            "implicit native transaction requires no caller transaction",
        ));
    }
    let principal = crate::object_manager::transaction_principal_from_claims(claims);
    let idempotency_key = implicit_native_transaction_key(context, target)?;
    let handle = state
        .mvcc
        .open_transactions
        .begin(
            state.mvcc.runtime.as_ref(),
            state.config.mvcc_cluster_id.clone(),
            principal.clone(),
            idempotency_key,
            std::time::Duration::from_secs(300),
            super::native_put_rpc::configured_default_durability(
                &state.config.mvcc_default_durability,
            )?,
            crate::mvcc_transaction::ReadConsistency::Linearized,
            native_mutation_unix_ms()?,
        )
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    Ok(ImplicitNativeTransaction {
        transaction_id: handle.transaction_id,
        principal,
    })
}

fn implicit_native_transaction_key(
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
) -> Result<String, Status> {
    let target_bytes = serde_json::to_vec(target)
        .map_err(|error| Status::internal(format!("serialize mutation target: {error}")))?;
    let mut identity = blake3::Hasher::new();
    for value in [
        b"implicit-native-transaction-v1".as_slice(),
        context.idempotency_key.as_bytes(),
        target_bytes.as_slice(),
    ] {
        identity.update(&(value.len() as u64).to_be_bytes());
        identity.update(value);
    }
    Ok(format!("implicit-native:{}", identity.finalize().to_hex()))
}

pub(super) async fn stage_implicit_native_response<T>(
    state: &AppState,
    attempt: &NativeMutationAttempt<'_>,
    target: &NativeIdempotencyTarget,
    response: &T,
    transaction: &ImplicitNativeTransaction,
) -> Result<(), Status>
where
    T: Serialize,
{
    let plan = native_idempotency::prepare_response_for_implicit_batch(
        &state.mvcc,
        attempt.context,
        target,
        response,
    )
    .await?;
    state
        .mvcc
        .stage_product_mutations(
            &transaction.transaction_id,
            &transaction.principal,
            plan.mutations,
            native_mutation_unix_ms()?,
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    for (key, predicate) in plan.predicates {
        state
            .mvcc
            .stage_predicate(
                &transaction.transaction_id,
                &transaction.principal,
                key,
                predicate,
                native_mutation_unix_ms()?,
            )
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
    }
    Ok(())
}

pub(super) async fn commit_implicit_native_transaction(
    state: &AppState,
    transaction: &ImplicitNativeTransaction,
) -> Result<(), Status> {
    let outcome = state
        .mvcc
        .open_transactions
        .commit(
            state.mvcc.runtime.as_ref(),
            &transaction.transaction_id,
            &transaction.principal,
            native_mutation_unix_ms()?,
        )
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    if let crate::mvcc_transaction::CertificationResult::Aborted { reason } =
        outcome.certification
    {
        return Err(Status::aborted(format!(
            "implicit MVCC transaction aborted: {reason:?}"
        )));
    }
    Ok(())
}

fn native_mutation_unix_ms() -> Result<u64, Status> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Status::internal("system clock precedes Unix epoch"))?;
    u64::try_from(elapsed.as_millis()).map_err(|_| Status::internal("system time exceeds u64"))
}

#[cfg(test)]
mod implicit_transaction_tests {
    use super::*;

    fn context(key: &str) -> NativeMutationContext {
        NativeMutationContext {
            tenant_id: 7,
            bucket_id: 11,
            principal: "alice".to_string(),
            request_id: "request".to_string(),
            precondition: "none".to_string(),
            idempotency_key: key.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn implicit_transaction_identity_is_stable_and_binds_all_target_parameters() {
        let first = NativeIdempotencyTarget::new("UploadPart", "bucket", "object")
            .with_parameters(serde_json::json!({"upload_id":"one","part_number":1}));
        let changed = NativeIdempotencyTarget::new("UploadPart", "bucket", "object")
            .with_parameters(serde_json::json!({"upload_id":"one","part_number":2}));
        assert_eq!(
            implicit_native_transaction_key(&context("retry"), &first).unwrap(),
            implicit_native_transaction_key(&context("retry"), &first).unwrap()
        );
        assert_ne!(
            implicit_native_transaction_key(&context("retry"), &first).unwrap(),
            implicit_native_transaction_key(&context("retry"), &changed).unwrap()
        );
        assert_ne!(
            implicit_native_transaction_key(&context("retry"), &first).unwrap(),
            implicit_native_transaction_key(&context("other"), &first).unwrap()
        );
    }
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
    let replay = native_idempotency::load_response(&state.mvcc, context, target).await?;
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
    native_idempotency::store_response(&state.mvcc, attempt.context, target, response).await
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
    let bucket =
        bucket_journal::read_current_bucket_mvcc(&state.mvcc, claims.tenant_id, bucket_name)
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("BucketNotFound"))?;
    if context.bucket_id > 0 && bucket.id != context.bucket_id {
        return Err(Status::permission_denied("Native mutation bucket mismatch"));
    }

    if let Some(required_revision) = parse_authz_zookie(&context.authz_zookie_optional)? {
        let latest = authz_journal::latest_authz_revision(&state.mvcc, claims.tenant_id)
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
    transaction: Option<(&str, &str)>,
) -> Result<
    Option<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    Status,
> {
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
