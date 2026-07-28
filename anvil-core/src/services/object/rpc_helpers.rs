use super::*;
use crate::object_manager;

pub(super) fn request_id<T>(request: &Request<T>) -> String {
    request
        .metadata()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
}

pub(in crate::services::object) fn native_transaction_id(
    context: Option<&NativeMutationContext>,
) -> Result<Option<&str>, Status> {
    crate::services::transaction_context::native_context_transaction_id(context)
}

pub(super) async fn begin_implicit_native_transaction(
    state: &AppState,
    claims: &auth::Claims,
    context: Option<&NativeMutationContext>,
    target: &NativeIdempotencyTarget,
) -> Result<Option<super::super::native_mutation::ImplicitNativeTransaction>, Status> {
    let context =
        context.ok_or_else(|| Status::invalid_argument("Missing native mutation context"))?;
    if context.transaction_id.is_some() {
        return Ok(None);
    }
    super::super::native_mutation::begin_implicit_native_transaction(state, context, target, claims)
        .await
        .map(Some)
}

pub(super) async fn commit_implicit_native_response<T: serde::Serialize>(
    state: &AppState,
    claims: &auth::Claims,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
    response: &T,
    handle: &super::super::native_mutation::ImplicitNativeTransaction,
) -> Result<(), Status> {
    let principal = object_manager::transaction_principal_from_claims(claims);
    let plan = crate::native_idempotency::prepare_response_for_implicit_batch(
        &state.mvcc,
        context,
        target,
        response,
    )
    .await?;
    let now = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default();
    state
        .mvcc
        .stage_product_mutations(&handle.transaction_id, &principal, plan.mutations, now)
        .map_err(|error| Status::internal(error.to_string()))?;
    for (key, predicate) in plan.predicates {
        state
            .mvcc
            .stage_predicate(&handle.transaction_id, &principal, key, predicate, now)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
    }
    crate::native_idempotency::stage_generic_result(
        &state.mvcc,
        &handle.transaction_id,
        &principal,
        context,
        target,
        response,
    )?;
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
    match outcome.certification {
        crate::mvcc_transaction::CertificationResult::Committed { .. } => Ok(()),
        crate::mvcc_transaction::CertificationResult::Aborted { reason } => Err(Status::aborted(
            format!("implicit native transaction aborted: {reason:?}"),
        )),
    }
}

pub(super) fn native_route_tenant_id(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<Option<i64>, Status> {
    let Some(raw) = metadata.get("x-anvil-tenant-id") else {
        return Ok(None);
    };
    let value = raw
        .to_str()
        .map_err(|_| Status::invalid_argument("Invalid x-anvil-tenant-id route metadata"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument(
            "Empty x-anvil-tenant-id route metadata",
        ));
    }
    trimmed
        .parse::<i64>()
        .map(Some)
        .map_err(|_| Status::invalid_argument("Invalid x-anvil-tenant-id route metadata"))
}

pub(super) fn object_storage_class(object: &crate::persistence::Object) -> String {
    object.storage_class.clone().unwrap_or_default()
}

pub(in crate::services::object) fn write_state_for_transaction(
    transaction_id: Option<&str>,
) -> i32 {
    if transaction_id.is_some() {
        WriteState::Staged as i32
    } else {
        WriteState::Finalised as i32
    }
}

pub(super) fn promotion_target(
    value: i32,
) -> Result<crate::mvcc_transaction::DurabilityLevel, Status> {
    match MvccDurability::try_from(value).unwrap_or(MvccDurability::Unspecified) {
        MvccDurability::Quorum => Ok(crate::mvcc_transaction::DurabilityLevel::Quorum),
        MvccDurability::Erasure => Ok(crate::mvcc_transaction::DurabilityLevel::Erasure),
        MvccDurability::Unspecified | MvccDurability::Local => Err(Status::invalid_argument(
            "promotion target must be quorum or erasure",
        )),
    }
}

pub(super) fn object_promotion_status(
    request_id: String,
    promotion_id: String,
    bucket_name: &str,
    object_key: &str,
    version_id: uuid::Uuid,
    record: &crate::mvcc_local_durability_upgrade::LocalDurabilityUpgradeRecord,
    replicated_complete: bool,
) -> ObjectDurabilityPromotionStatus {
    use crate::mvcc_local_durability_upgrade::LocalDurabilityUpgradeState;

    let target_durability = match record.job.target {
        crate::mvcc_transaction::DurabilityLevel::Local => MvccDurability::Local,
        crate::mvcc_transaction::DurabilityLevel::Quorum => MvccDurability::Quorum,
        crate::mvcc_transaction::DurabilityLevel::Erasure => MvccDurability::Erasure,
    };
    let state = if replicated_complete {
        "complete"
    } else {
        match record.state {
            LocalDurabilityUpgradeState::Pending => "pending",
            LocalDurabilityUpgradeState::Running => "running",
            LocalDurabilityUpgradeState::Complete => "complete",
        }
    };
    ObjectDurabilityPromotionStatus {
        request_id,
        promotion_id,
        bucket_name: bucket_name.to_string(),
        object_key: object_key.to_string(),
        version_id: version_id.to_string(),
        target_durability: target_durability as i32,
        state: state.to_string(),
        attempts: record.attempts,
        requested_at_unix_ms: record.job.requested_at_unix_ms,
        next_attempt_unix_ms: record.next_attempt_unix_ms,
        last_error: record.last_error.clone(),
    }
}

pub(in crate::services::object) fn object_write_visibility(
    context: Option<&NativeMutationContext>,
) -> Result<ObjectWriteVisibility, Status> {
    let Some(options) = context.and_then(|context| context.write_visibility.as_ref()) else {
        return Ok(ObjectWriteVisibility::default());
    };
    Ok(ObjectWriteVisibility {
        indexes: match options.indexes {
            0 => IndexMaintenanceVisibility::Deferred,
            1 => IndexMaintenanceVisibility::Enqueued,
            2 => IndexMaintenanceVisibility::CaughtUp,
            _ => return Err(Status::invalid_argument("Invalid index maintenance mode")),
        },
        watches: match options.watches {
            0 => WatchVisibility::Deferred,
            1 => WatchVisibility::Published,
            _ => return Err(Status::invalid_argument("Invalid watch visibility mode")),
        },
        authz_materialization: match options.authz_materialization {
            0 => AuthzMaterializationVisibility::InheritedOk,
            1 => AuthzMaterializationVisibility::Materialized,
            _ => {
                return Err(Status::invalid_argument(
                    "Invalid authz materialization mode",
                ));
            }
        },
        boundary_extraction: match options.boundary_extraction {
            0 => BoundaryExtractionVisibility::HintsOnly,
            1 => BoundaryExtractionVisibility::PayloadNow,
            _ => return Err(Status::invalid_argument("Invalid boundary extraction mode")),
        },
        index_policy_snapshot: match options.index_policy_snapshot {
            0 => IndexPolicySnapshotVisibility::Cached,
            1 => IndexPolicySnapshotVisibility::Exact,
            _ => {
                return Err(Status::invalid_argument(
                    "Invalid index policy snapshot mode",
                ));
            }
        },
        authz_revision: match options.authz_revision {
            0 => AuthzRevisionVisibility::CurrentKnown,
            1 => AuthzRevisionVisibility::FenceExact,
            _ => return Err(Status::invalid_argument("Invalid authz revision mode")),
        },
    })
}
