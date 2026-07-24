use super::*;
use crate::core_store::{CoreMutationPrecondition, CoreTransaction, CoreTransactionState};

const MAX_MUTATION_BATCH_OPERATIONS: usize = 256;

pub(crate) async fn enforce_write_precondition(
    state: &AppState,
    claims: &auth::Claims,
    precondition: Option<&WritePrecondition>,
) -> Result<(), Status> {
    prepare_write_preconditions(state, claims, precondition, None)
        .await
        .map(|_| ())
}

pub(super) async fn prepare_write_preconditions(
    state: &AppState,
    claims: &auth::Claims,
    precondition: Option<&WritePrecondition>,
    transaction: Option<&CoreTransaction>,
) -> Result<Vec<CoreMutationPrecondition>, Status> {
    let Some(precondition) = precondition else {
        return Ok(Vec::new());
    };
    let object_checks = precondition
        .object_versions
        .iter()
        .map(|object_precondition| async move {
            if object_precondition.bucket_name.trim().is_empty()
                || object_precondition.object_key.trim().is_empty()
            {
                return Err(Status::invalid_argument(
                    "ObjectVersionPrecondition requires bucket_name and object_key",
                ));
            }
            let expected_version_id =
                parse_optional_version_id(object_precondition.expected_version_id.as_deref())?;
            let snapshot = state
                .object_manager
                .object_mutation_precondition_snapshot(
                    claims,
                    &object_precondition.bucket_name,
                    &object_precondition.object_key,
                    AnvilAction::ObjectRead,
                    transaction,
                )
                .await;
            match (
                object_precondition.must_not_exist,
                expected_version_id,
                snapshot,
            ) {
                (true, _, Ok(snapshot)) if snapshot.object.is_none() => Ok(snapshot.precondition),
                (true, _, Ok(_)) => Err(Status::failed_precondition(
                    "ObjectVersionPreconditionFailed",
                )),
                (true, _, Err(status)) => Err(status),
                (false, Some(expected), Ok(snapshot))
                    if snapshot
                        .object
                        .as_ref()
                        .is_some_and(|object| object.version_id == expected) =>
                {
                    Ok(snapshot.precondition)
                }
                (false, Some(_), Ok(_)) => Err(Status::failed_precondition(
                    "ObjectVersionPreconditionFailed",
                )),
                (false, Some(_), Err(status)) => Err(status),
                (false, None, _) => Err(Status::invalid_argument(
                    "ObjectVersionPrecondition requires expected_version_id or must_not_exist",
                )),
            }
        });
    let mut durable_preconditions = Vec::with_capacity(precondition.object_versions.len() + 1);
    for result in futures_util::future::join_all(object_checks).await {
        durable_preconditions.push(result?);
    }

    if let Some(lease_fence) = precondition.lease_fence.as_ref() {
        validate_task_lease_id(&lease_fence.task_id)?;
        let lease = state
            .persistence
            .read_named_task_lease(claims.tenant_id, &lease_fence.task_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::failed_precondition(task_lease::LEASE_EXPIRED))?;
        let owner = lease_owner_from_claims(claims);
        if !lease.owner.same_security_owner(&owner) {
            return Err(Status::permission_denied(task_lease::LEASE_OWNER_MISMATCH));
        }
        if lease.fence_token != lease_fence.fence_token {
            return Err(Status::failed_precondition(task_lease::STALE_FENCE));
        }
        if lease.expires_at_nanos <= current_time_nanos()? {
            return Err(Status::failed_precondition(task_lease::LEASE_EXPIRED));
        }
        durable_preconditions.push(
            state
                .persistence
                .named_task_lease_fenced_precondition(&lease, current_time_nanos()?)
                .await
                .map_err(lease_error_status)?,
        );
    }

    Ok(durable_preconditions)
}

pub(super) async fn mutation_precondition_transaction(
    state: &AppState,
    claims: &auth::Claims,
    transaction_id: Option<&str>,
) -> Result<Option<CoreTransaction>, Status> {
    let Some(transaction_id) = transaction_id else {
        return Ok(None);
    };
    let principal = crate::object_manager::transaction_principal_from_claims(claims);
    let transaction = state
        .core_store
        .read_explicit_transaction_for_principal(transaction_id, &principal)
        .await
        .map_err(|error| {
            transaction_core_store_status(&error.to_string())
                .unwrap_or_else(|| Status::internal(error.to_string()))
        })?;
    match transaction.state {
        CoreTransactionState::Open => Ok(Some(transaction)),
        CoreTransactionState::Expired => Err(Status::failed_precondition("TransactionExpired")),
        CoreTransactionState::RolledBack | CoreTransactionState::Aborted => {
            Err(Status::failed_precondition("TransactionRolledBack"))
        }
        CoreTransactionState::Committed => {
            Err(Status::failed_precondition("TransactionAlreadyCommitted"))
        }
        _ => Err(Status::failed_precondition("TransactionNotOpen")),
    }
}

pub(super) fn validate_mutation_batch_operations(req: &MutationBatchRequest) -> Result<(), Status> {
    if req.operations.is_empty() {
        return Err(Status::invalid_argument(
            "MutationBatch requires at least one operation",
        ));
    }
    if req.operations.len() > MAX_MUTATION_BATCH_OPERATIONS {
        return Err(Status::invalid_argument(format!(
            "MutationBatch supports at most {MAX_MUTATION_BATCH_OPERATIONS} operations"
        )));
    }
    for operation in &req.operations {
        let Some(op) = operation.op.as_ref() else {
            return Err(Status::invalid_argument(
                "MutationBatch operation is missing op",
            ));
        };
        match op {
            mutation_batch_operation::Op::PutObject(op) if op.object_key.trim().is_empty() => {
                return Err(Status::invalid_argument(
                    "put_object.object_key is required",
                ));
            }
            mutation_batch_operation::Op::PutObject(op)
                if crate::validation::is_reserved_internal_key(&op.object_key) =>
            {
                return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
            }
            mutation_batch_operation::Op::PatchJsonObject(op)
                if op.object_key.trim().is_empty() =>
            {
                return Err(Status::invalid_argument(
                    "patch_json_object.object_key is required",
                ));
            }
            mutation_batch_operation::Op::PatchJsonObject(op)
                if crate::validation::is_reserved_internal_key(&op.object_key) =>
            {
                return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
            }
            mutation_batch_operation::Op::DeleteObject(op) if op.object_key.trim().is_empty() => {
                return Err(Status::invalid_argument(
                    "delete_object.object_key is required",
                ));
            }
            mutation_batch_operation::Op::DeleteObject(op)
                if crate::validation::is_reserved_internal_key(&op.object_key) =>
            {
                return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
            }
            mutation_batch_operation::Op::AppendStreamRecord(op)
                if op.stream_key.trim().is_empty() || op.stream_id.trim().is_empty() =>
            {
                return Err(Status::invalid_argument(
                    "append_stream_record stream_key and stream_id are required",
                ));
            }
            mutation_batch_operation::Op::AppendStreamRecord(op)
                if crate::validation::is_reserved_internal_key(&op.stream_key) =>
            {
                return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
            }
            mutation_batch_operation::Op::CheckpointTaskLease(op)
                if op.task_id.trim().is_empty() || op.fence_token == 0 =>
            {
                return Err(Status::invalid_argument(
                    "task lease batch operation requires task_id and fence_token",
                ));
            }
            mutation_batch_operation::Op::CommitTaskLease(op)
                if op.task_id.trim().is_empty() || op.fence_token == 0 =>
            {
                return Err(Status::invalid_argument(
                    "task lease batch operation requires task_id and fence_token",
                ));
            }
            mutation_batch_operation::Op::CompareAndSwapManifest(op)
                if op.manifest_key.trim().is_empty() =>
            {
                return Err(Status::invalid_argument(
                    "compare_and_swap_manifest.manifest_key is required",
                ));
            }
            mutation_batch_operation::Op::CompareAndSwapManifest(op)
                if crate::validation::is_reserved_internal_key(&op.manifest_key) =>
            {
                return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
            }
            _ => {}
        }
    }
    Ok(())
}

pub(super) async fn validate_mutation_batch_authorization(
    state: &AppState,
    claims: &auth::Claims,
    req: &MutationBatchRequest,
) -> Result<(), Status> {
    for operation in &req.operations {
        let Some(op) = operation.op.as_ref() else {
            continue;
        };
        match op {
            mutation_batch_operation::Op::CheckpointTaskLease(op) => {
                crate::access_control::require_action(
                    &state.storage,
                    &state.persistence,
                    claims,
                    AnvilAction::CoordinationLeaseWrite,
                    &task_lease_resource(&op.task_id),
                )
                .await?;
            }
            mutation_batch_operation::Op::CommitTaskLease(op) => {
                crate::access_control::require_action(
                    &state.storage,
                    &state.persistence,
                    claims,
                    AnvilAction::CoordinationLeaseWrite,
                    &task_lease_resource(&op.task_id),
                )
                .await?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub(super) async fn prepare_mutation_batch_native_preconditions(
    state: &AppState,
    claims: &auth::Claims,
    req: &MutationBatchRequest,
    transaction: Option<&CoreTransaction>,
) -> Result<Vec<CoreMutationPrecondition>, Status> {
    let mut preconditions = Vec::new();
    for operation in &req.operations {
        let Some(op) = operation.op.as_ref() else {
            continue;
        };
        let (object_key, action) = match op {
            mutation_batch_operation::Op::PutObject(op) => {
                (op.object_key.as_str(), AnvilAction::ObjectWrite)
            }
            mutation_batch_operation::Op::PatchJsonObject(op) => {
                (op.object_key.as_str(), AnvilAction::ObjectWrite)
            }
            mutation_batch_operation::Op::DeleteObject(op) => {
                (op.object_key.as_str(), AnvilAction::ObjectDelete)
            }
            mutation_batch_operation::Op::AppendStreamRecord(op) => {
                (op.stream_key.as_str(), AnvilAction::StreamAppend)
            }
            mutation_batch_operation::Op::CompareAndSwapManifest(op) => {
                (op.manifest_key.as_str(), AnvilAction::ObjectWrite)
            }
            mutation_batch_operation::Op::CheckpointTaskLease(_)
            | mutation_batch_operation::Op::CommitTaskLease(_) => continue,
        };
        if let Some(precondition) = prepare_native_mutation_precondition(
            state,
            claims,
            &req.bucket_name,
            object_key,
            req.mutation_context.as_ref(),
            action,
            transaction,
        )
        .await?
        {
            preconditions.push(precondition);
        }
    }
    preconditions.dedup();
    Ok(preconditions)
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct MutationBatchDigestInputProto {
    #[prost(message, optional, tag = "1")]
    precondition: Option<WritePrecondition>,
    #[prost(message, repeated, tag = "2")]
    operations: Vec<MutationBatchOperation>,
}

pub(super) fn mutation_batch_digest(req: &MutationBatchRequest) -> Result<String, Status> {
    let input = MutationBatchDigestInputProto {
        precondition: req.precondition.clone(),
        operations: req.operations.clone(),
    };
    Ok(
        blake3::hash(&crate::core_store::encode_deterministic_proto(&input))
            .to_hex()
            .to_string(),
    )
}

pub(super) fn task_lease_resource(task_id: &str) -> String {
    format!("task_lease/{task_id}")
}

pub(super) async fn acquire_mutation_batch_operation_locks(
    state: &AppState,
    tenant_id: i64,
    req: &MutationBatchRequest,
) -> Result<Vec<OwnedMutexGuard<()>>, Status> {
    let mut keys = Vec::new();
    for operation in &req.operations {
        let Some(op) = operation.op.as_ref() else {
            continue;
        };
        let key = match op {
            mutation_batch_operation::Op::PutObject(op) => op.object_key.as_str(),
            mutation_batch_operation::Op::PatchJsonObject(op) => op.object_key.as_str(),
            mutation_batch_operation::Op::DeleteObject(op) => op.object_key.as_str(),
            mutation_batch_operation::Op::AppendStreamRecord(op) => op.stream_key.as_str(),
            mutation_batch_operation::Op::CompareAndSwapManifest(op) => op.manifest_key.as_str(),
            mutation_batch_operation::Op::CheckpointTaskLease(op) => {
                keys.push(native_target_lock_key(
                    tenant_id,
                    &req.bucket_name,
                    &format!("_task_lease/{}", op.task_id),
                ));
                continue;
            }
            mutation_batch_operation::Op::CommitTaskLease(op) => {
                keys.push(native_target_lock_key(
                    tenant_id,
                    &req.bucket_name,
                    &format!("_task_lease/{}", op.task_id),
                ));
                continue;
            }
        };
        keys.push(native_target_lock_key(tenant_id, &req.bucket_name, key));
    }
    keys.sort();
    keys.dedup();

    let mut guards = Vec::with_capacity(keys.len());
    for key in keys {
        guards.push(acquire_native_lock_key(state, key).await?);
    }
    Ok(guards)
}

pub(super) fn lease_owner_from_claims(claims: &auth::Claims) -> task_lease::TaskLeaseOwner {
    let actor_instance_id = claims.jti.clone().unwrap_or_else(|| claims.sub.clone());
    task_lease::TaskLeaseOwner {
        tenant_id: claims.tenant_id,
        principal_kind: "app".to_string(),
        principal_id: claims.sub.clone(),
        actor_instance_id,
        display_name: claims.sub.clone(),
    }
}

pub(super) fn current_time_nanos() -> Result<i64, Status> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| Status::internal("current time exceeds supported range"))
}

pub(super) fn join_u128(low: u64, high: u64) -> u128 {
    ((high as u128) << 64) | low as u128
}

pub(super) fn lease_error_status(error: anyhow::Error) -> Status {
    let message = error.to_string();
    if message.contains(task_lease::LEASE_HELD) {
        Status::failed_precondition(task_lease::LEASE_HELD)
    } else if message.contains(task_lease::LEASE_EXPIRED) {
        Status::failed_precondition(task_lease::LEASE_EXPIRED)
    } else if message.contains(task_lease::STALE_FENCE) {
        Status::failed_precondition(task_lease::STALE_FENCE)
    } else if message.contains(task_lease::LEASE_OWNER_MISMATCH) {
        Status::permission_denied(task_lease::LEASE_OWNER_MISMATCH)
    } else if message.contains(task_lease::LEASE_CAS_CONFLICT) {
        Status::aborted(task_lease::LEASE_CAS_CONFLICT)
    } else {
        Status::failed_precondition(message)
    }
}

pub(super) fn validate_task_lease_id(value: &str) -> Result<(), Status> {
    if value.trim().is_empty()
        || value.len() > 256
        || value.chars().any(|ch| ch.is_control())
        || value.contains("..")
        || value.starts_with('/')
    {
        return Err(Status::invalid_argument("Invalid task_id"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_with(operation: mutation_batch_operation::Op) -> MutationBatchRequest {
        MutationBatchRequest {
            bucket_name: "docs".to_string(),
            mutation_context: None,
            precondition: None,
            operations: vec![MutationBatchOperation {
                op: Some(operation),
            }],
        }
    }

    #[test]
    fn mutation_batch_rejects_reserved_object_keys_before_execution() {
        let cases = [
            mutation_batch_operation::Op::PutObject(MutationBatchPutObject {
                object_key: "_anvil/authz/tuple".to_string(),
                payload: Vec::new(),
                content_type: None,
                user_metadata_json: "{}".to_string(),
                storage_class: None,
            }),
            mutation_batch_operation::Op::PatchJsonObject(MutationBatchPatchJsonObject {
                object_key: "_anvil/meta/head".to_string(),
                base_version_id: None,
                merge_patch_json: "{}".to_string(),
            }),
            mutation_batch_operation::Op::DeleteObject(MutationBatchDeleteObject {
                object_key: "_anvil/personaldb/group".to_string(),
                version_id: None,
            }),
            mutation_batch_operation::Op::AppendStreamRecord(MutationBatchAppendStreamRecord {
                stream_key: "_anvil/watch/internal".to_string(),
                stream_id: uuid::Uuid::new_v4().to_string(),
                payload: Vec::new(),
                content_type: None,
                user_metadata_json: "{}".to_string(),
            }),
            mutation_batch_operation::Op::CompareAndSwapManifest(
                MutationBatchCompareAndSwapManifest {
                    manifest_key: "_anvil/index/manifest".to_string(),
                    expected_revision: 0,
                    manifest_json: "{}".to_string(),
                },
            ),
        ];

        for op in cases {
            let err = validate_mutation_batch_operations(&request_with(op)).unwrap_err();
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
            assert_eq!(err.message(), "UnauthorizedReservedNamespace");
        }
    }

    #[test]
    fn mutation_batch_rejects_empty_and_oversized_requests() {
        let empty = MutationBatchRequest {
            bucket_name: "docs".to_string(),
            mutation_context: None,
            precondition: None,
            operations: Vec::new(),
        };
        assert_eq!(
            validate_mutation_batch_operations(&empty)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );

        let operation = MutationBatchOperation {
            op: Some(mutation_batch_operation::Op::DeleteObject(
                MutationBatchDeleteObject {
                    object_key: "document".to_string(),
                    version_id: None,
                },
            )),
        };
        let oversized = MutationBatchRequest {
            operations: vec![operation; MAX_MUTATION_BATCH_OPERATIONS + 1],
            ..empty
        };
        assert_eq!(
            validate_mutation_batch_operations(&oversized)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}
