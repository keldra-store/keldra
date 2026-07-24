use super::rpc::{native_transaction_id, object_write_visibility, write_state_for_transaction};
use super::*;
use crate::core_store::{
    CoreMutationBatchAdditions, CoreMutationPrecondition, CoreTransactionState,
};
use crate::object_manager;

fn put_mutation_batch_response(
    operation_digest: &str,
    request_id: &str,
    transaction_id: Option<&str>,
    objects: &[crate::persistence::Object],
) -> MutationBatchResponse {
    MutationBatchResponse {
        batch_id: operation_digest.to_string(),
        operation_receipts: objects
            .iter()
            .map(|object| MutationBatchOperationReceipt {
                operation: "put_object".to_string(),
                object_key: object.key.clone(),
                version_id: object.version_id.to_string(),
                mutation_id: object.mutation_id.to_string(),
                payload_hash: object.content_hash.clone(),
                record_hash: object.record_hash.clone(),
                append_record_sequence: 0,
                manifest_revision: 0,
                lease_fence_token: 0,
            })
            .collect(),
        watch_cursor: 0,
        mutation_id: request_id.to_string(),
        write_state: write_state_for_transaction(transaction_id),
    }
}

async fn hydrate_put_batch_watch_cursor(
    state: &AppState,
    context: &NativeMutationContext,
    write_visibility: crate::object_manager::ObjectWriteVisibility,
    response: &mut MutationBatchResponse,
) -> Result<(), Status> {
    if context.transaction_id.is_some()
        || !write_visibility.requires_watch_visible()
        || response.watch_cursor != 0
    {
        return Ok(());
    }

    let mut max_watch_cursor = 0_u64;
    for receipt in &response.operation_receipts {
        if receipt.operation != "put_object" {
            continue;
        }
        let version_id = uuid::Uuid::parse_str(&receipt.version_id)
            .map_err(|_| Status::internal("Invalid idempotent object version"))?;
        let mutation_id = uuid::Uuid::parse_str(&receipt.mutation_id)
            .map_err(|_| Status::internal("Invalid idempotent object mutation"))?;
        let cursor = crate::watch_log::exact_object_watch_cursor(
            &state.storage,
            context.tenant_id,
            context.bucket_id,
            version_id,
            mutation_id,
        )
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .ok_or_else(|| Status::internal("Object mutation watch event not found"))?;
        let cursor =
            u64::try_from(cursor).map_err(|_| Status::internal("Invalid object watch cursor"))?;
        max_watch_cursor = max_watch_cursor.max(cursor);
    }
    response.watch_cursor = max_watch_cursor;
    Ok(())
}

fn ensure_mutation_batch_operations_supported(
    operations: &[MutationBatchOperation],
) -> Result<(), Status> {
    for operation in operations {
        let Some(op) = operation.op.as_ref() else {
            return Err(Status::invalid_argument(
                "MutationBatch operation is missing op",
            ));
        };
        let unsupported = match op {
            mutation_batch_operation::Op::PutObject(_)
            | mutation_batch_operation::Op::DeleteObject(_)
            | mutation_batch_operation::Op::CompareAndSwapManifest(_)
            | mutation_batch_operation::Op::PatchJsonObject(_)
            | mutation_batch_operation::Op::AppendStreamRecord(_) => None,
            mutation_batch_operation::Op::CheckpointTaskLease(_) => Some("checkpoint_task_lease"),
            mutation_batch_operation::Op::CommitTaskLease(_) => Some("commit_task_lease"),
        };
        if let Some(operation) = unsupported {
            return Err(Status::failed_precondition(format!(
                "{operation} is a coordination-plane operation; use CoordinationService so lease state is never published separately from MutationBatch idempotency"
            )));
        }
    }
    Ok(())
}

fn implicit_batch_transaction_id(
    context: &NativeMutationContext,
    operation_digest: &str,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.mutation-batch.implicit.v1");
    hasher.update(&context.tenant_id.to_le_bytes());
    hasher.update(&context.bucket_id.to_le_bytes());
    hasher.update(context.principal.as_bytes());
    hasher.update(context.idempotency_key.as_bytes());
    hasher.update(operation_digest.as_bytes());
    format!("mutation-batch:{}", hasher.finalize().to_hex())
}

fn is_object_data_operation(operation: &MutationBatchOperation) -> bool {
    matches!(
        operation.op.as_ref(),
        Some(
            mutation_batch_operation::Op::PutObject(_)
                | mutation_batch_operation::Op::DeleteObject(_)
                | mutation_batch_operation::Op::CompareAndSwapManifest(_)
                | mutation_batch_operation::Op::PatchJsonObject(_)
                | mutation_batch_operation::Op::AppendStreamRecord(_)
        )
    )
}

fn deduplicate_preconditions(preconditions: &mut Vec<CoreMutationPrecondition>) {
    let mut unique = Vec::with_capacity(preconditions.len());
    for precondition in preconditions.drain(..) {
        if !unique.contains(&precondition) {
            unique.push(precondition);
        }
    }
    *preconditions = unique;
}

async fn prepare_put_batch_additions(
    storage: crate::storage::Storage,
    context: NativeMutationContext,
    target: NativeIdempotencyTarget,
    response: MutationBatchResponse,
    mut durable_preconditions: Vec<CoreMutationPrecondition>,
) -> Result<CoreMutationBatchAdditions, Status> {
    let mut additions = if context.transaction_id.is_some() {
        native_idempotency::prepare_response_in_transaction(&storage, &context, &target, &response)
            .await?
    } else {
        let publication_root_anchor =
            hex::encode(crate::metadata_journal::object_metadata_partition_id(
                context.tenant_id,
                context.bucket_id,
            ));
        native_idempotency::prepare_response_for_implicit_batch(
            &storage,
            &context,
            &target,
            &response,
            &publication_root_anchor,
        )
        .await?
    };
    additions.preconditions.append(&mut durable_preconditions);
    deduplicate_preconditions(&mut additions.preconditions);
    Ok(additions)
}

pub(super) async fn execute_mutation_batch(
    state: &AppState,
    request: Request<MutationBatchRequest>,
) -> Result<Response<MutationBatchResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    validate_native_mutation_context(
        state,
        &claims,
        &req.bucket_name,
        req.mutation_context.as_ref(),
    )
    .await?;
    if req.operations.is_empty() {
        return Err(Status::invalid_argument(
            "MutationBatch requires at least one operation",
        ));
    }
    validate_mutation_batch_operations(&req)?;
    ensure_mutation_batch_operations_supported(&req.operations)?;
    validate_mutation_batch_authorization(state, &claims, &req).await?;
    let operation_digest = mutation_batch_digest(&req)?;
    let context = req
        .mutation_context
        .clone()
        .ok_or_else(|| Status::invalid_argument("Missing native mutation context"))?;
    let transaction_id = native_transaction_id(Some(&context))?;
    let write_visibility = object_write_visibility(Some(&context))?;
    let target = NativeIdempotencyTarget::new("MutationBatch", &req.bucket_name, "mutation_batch")
        .with_parameters(serde_json::json!({ "request_digest": operation_digest }));
    let _idempotency_guard = acquire_native_mutation_lock(state, &context).await?;
    let replay = native_idempotency::load_response::<MutationBatchResponse>(
        &state.storage,
        &context,
        &target,
    )
    .await?;
    if let Some(mut response) = replay {
        // The durable idempotency row is committed in the same root as the
        // object metadata. The exact cursor projection is part of that commit,
        // so it can be hydrated without widening the publication boundary.
        hydrate_put_batch_watch_cursor(state, &context, write_visibility, &mut response).await?;
        return Ok(Response::new(response));
    }
    let _operation_guards =
        acquire_mutation_batch_operation_locks(state, claims.tenant_id, &req).await?;
    let put_only_batch = !write_visibility.requires_payload_boundary_extraction()
        && req.operations.iter().all(|operation| {
            matches!(
                operation.op.as_ref(),
                Some(mutation_batch_operation::Op::PutObject(_))
            )
        });
    if transaction_id.is_none()
        && !put_only_batch
        && req.operations.iter().any(is_object_data_operation)
    {
        return Err(Status::failed_precondition(
            "ExplicitTransactionRequiredForNonPutMutationBatch",
        ));
    }
    let precondition_transaction =
        mutation_precondition_transaction(state, &claims, transaction_id).await?;
    let mut durable_preconditions = prepare_mutation_batch_native_preconditions(
        state,
        &claims,
        &req,
        precondition_transaction.as_ref(),
    )
    .await?;
    durable_preconditions.extend(
        prepare_write_preconditions(
            state,
            &claims,
            req.precondition.as_ref(),
            precondition_transaction.as_ref(),
        )
        .await?,
    );
    deduplicate_preconditions(&mut durable_preconditions);
    if put_only_batch {
        let idempotency_context = context.clone();
        let idempotency_target = target.clone();
        let idempotency_storage = state.storage.clone();
        let operation_digest_for_additions = operation_digest.clone();
        let inputs = req
            .operations
            .into_iter()
            .map(|operation| {
                let Some(mutation_batch_operation::Op::PutObject(operation)) = operation.op else {
                    unreachable!("put-only mutation batch was checked");
                };
                Ok(object_manager::ObjectBatchPut {
                    object_key: operation.object_key,
                    payload: operation.payload,
                    content_type: operation.content_type,
                    user_metadata: parse_user_metadata_json(&operation.user_metadata_json)?,
                    storage_class_id: operation.storage_class,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let objects = if let Some(transaction_id) = transaction_id {
            state
                .object_manager
                .put_objects_batch_in_transaction(
                    &claims,
                    &req.bucket_name,
                    inputs,
                    transaction_id,
                    &object_manager::transaction_principal_from_claims(&claims),
                    write_visibility,
                    move |objects| {
                        let response = put_mutation_batch_response(
                            &operation_digest_for_additions,
                            &idempotency_context.request_id,
                            idempotency_context.transaction_id.as_deref(),
                            objects,
                        );
                        prepare_put_batch_additions(
                            idempotency_storage,
                            idempotency_context,
                            idempotency_target,
                            response,
                            durable_preconditions,
                        )
                    },
                )
                .await?
        } else {
            let publication_transaction_id =
                implicit_batch_transaction_id(&idempotency_context, &operation_digest);
            state
                .object_manager
                .put_objects_batch(
                    &claims,
                    &req.bucket_name,
                    inputs,
                    &publication_transaction_id,
                    write_visibility,
                    move |objects| {
                        let response = put_mutation_batch_response(
                            &operation_digest_for_additions,
                            &idempotency_context.request_id,
                            idempotency_context.transaction_id.as_deref(),
                            objects,
                        );
                        prepare_put_batch_additions(
                            idempotency_storage,
                            idempotency_context,
                            idempotency_target,
                            response,
                            durable_preconditions,
                        )
                    },
                )
                .await?
        };
        let mut response = put_mutation_batch_response(
            &operation_digest,
            &context.request_id,
            transaction_id,
            &objects,
        );
        hydrate_put_batch_watch_cursor(state, &context, write_visibility, &mut response).await?;
        return Ok(Response::new(response));
    }

    let mut receipts = Vec::with_capacity(req.operations.len());
    let mut max_watch_cursor = 0_u64;
    for operation in req.operations {
        let Some(op) = operation.op else {
            return Err(Status::invalid_argument(
                "MutationBatch operation is missing op",
            ));
        };
        match op {
            mutation_batch_operation::Op::PutObject(op) => {
                let object = state
                    .object_manager
                    .put_object(
                        &claims,
                        &req.bucket_name,
                        &op.object_key,
                        futures_util::stream::iter(vec![Ok(op.payload)]),
                        ObjectWriteOptions {
                            content_type: op.content_type,
                            user_metadata: parse_user_metadata_json(&op.user_metadata_json)?,
                            transaction_id: transaction_id.map(ToOwned::to_owned),
                            transaction_principal: transaction_id.map(|_| {
                                crate::object_manager::transaction_principal_from_claims(&claims)
                            }),
                            storage_class_id: op.storage_class,
                            visibility: write_visibility,
                        },
                    )
                    .await?;
                let watch_cursor =
                    if transaction_id.is_some() || !write_visibility.requires_watch_visible() {
                        0
                    } else {
                        object_watch_cursor(state, &object).await?
                    };
                max_watch_cursor = max_watch_cursor.max(watch_cursor);
                receipts.push(MutationBatchOperationReceipt {
                    operation: "put_object".to_string(),
                    object_key: object.key,
                    version_id: object.version_id.to_string(),
                    mutation_id: object.mutation_id.to_string(),
                    payload_hash: object.content_hash,
                    record_hash: object.record_hash,
                    append_record_sequence: 0,
                    manifest_revision: 0,
                    lease_fence_token: 0,
                });
            }
            mutation_batch_operation::Op::PatchJsonObject(op) => {
                let object = state
                    .object_manager
                    .patch_json_object(
                        claims.clone(),
                        &req.bucket_name,
                        &op.object_key,
                        parse_optional_version_id(op.base_version_id.as_deref())?,
                        &op.merge_patch_json,
                        transaction_id,
                    )
                    .await?;
                if transaction_id.is_none() {
                    let watch_cursor = object_watch_cursor(state, &object).await?;
                    max_watch_cursor = max_watch_cursor.max(watch_cursor);
                }
                receipts.push(MutationBatchOperationReceipt {
                    operation: "patch_json_object".to_string(),
                    object_key: object.key,
                    version_id: object.version_id.to_string(),
                    mutation_id: object.mutation_id.to_string(),
                    payload_hash: object.content_hash,
                    record_hash: object.record_hash,
                    append_record_sequence: 0,
                    manifest_revision: 0,
                    lease_fence_token: 0,
                });
            }
            mutation_batch_operation::Op::DeleteObject(op) => {
                let transaction_principal = transaction_id
                    .map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
                let deleted = if let Some(version_id) =
                    parse_optional_version_id(op.version_id.as_deref())?
                {
                    state
                        .object_manager
                        .delete_object_version(
                            &claims,
                            &req.bucket_name,
                            &op.object_key,
                            version_id,
                            transaction_id,
                            transaction_principal.as_deref(),
                            write_visibility,
                        )
                        .await?
                } else {
                    state
                        .object_manager
                        .delete_object(
                            &claims,
                            &req.bucket_name,
                            &op.object_key,
                            transaction_id,
                            transaction_principal.as_deref(),
                            write_visibility,
                        )
                        .await?
                };
                let watch_cursor =
                    if transaction_id.is_some() || !write_visibility.requires_watch_visible() {
                        0
                    } else {
                        object_watch_cursor(state, &deleted).await?
                    };
                max_watch_cursor = max_watch_cursor.max(watch_cursor);
                receipts.push(MutationBatchOperationReceipt {
                    operation: "delete_object".to_string(),
                    object_key: deleted.key,
                    version_id: deleted.version_id.to_string(),
                    mutation_id: deleted.mutation_id.to_string(),
                    payload_hash: deleted.content_hash,
                    record_hash: deleted.record_hash,
                    append_record_sequence: 0,
                    manifest_revision: 0,
                    lease_fence_token: 0,
                });
            }
            mutation_batch_operation::Op::AppendStreamRecord(op) => {
                let stream_id = uuid::Uuid::parse_str(&op.stream_id)
                    .map_err(|_| Status::invalid_argument("Invalid stream_id"))?;
                let record = state
                    .object_manager
                    .append_stream_record(
                        &claims,
                        &req.bucket_name,
                        &op.stream_key,
                        stream_id,
                        op.payload,
                        op.content_type,
                        parse_user_metadata_json(&op.user_metadata_json)?,
                        transaction_id,
                    )
                    .await?;
                if transaction_id.is_none() {
                    max_watch_cursor = max_watch_cursor.max(record.receipt.watch_cursor);
                }
                receipts.push(MutationBatchOperationReceipt {
                    operation: "append_stream_record".to_string(),
                    object_key: op.stream_key,
                    version_id: record.record_sequence.to_string(),
                    mutation_id: record.receipt.mutation_id.to_string(),
                    payload_hash: record.payload_hash,
                    record_hash: record.receipt.record_hash,
                    append_record_sequence: record.record_sequence,
                    manifest_revision: 0,
                    lease_fence_token: 0,
                });
            }
            mutation_batch_operation::Op::CheckpointTaskLease(_)
            | mutation_batch_operation::Op::CommitTaskLease(_) => {
                unreachable!("coordination-plane operations are rejected before batch execution")
            }
            mutation_batch_operation::Op::CompareAndSwapManifest(op) => {
                let transaction_principal = transaction_id
                    .map(|_| object_manager::transaction_principal_from_claims(&claims));
                let result = state
                    .object_manager
                    .compare_and_swap_manifest(
                        &claims,
                        &req.bucket_name,
                        &op.manifest_key,
                        op.expected_revision,
                        &op.manifest_json,
                        transaction_id,
                        transaction_principal.as_deref(),
                    )
                    .await?;
                if transaction_id.is_none() {
                    max_watch_cursor = max_watch_cursor.max(result.receipt.watch_cursor);
                }
                receipts.push(MutationBatchOperationReceipt {
                    operation: "compare_and_swap_manifest".to_string(),
                    object_key: op.manifest_key,
                    version_id: result.revision.to_string(),
                    mutation_id: result.receipt.mutation_id.to_string(),
                    payload_hash: result.manifest_hash,
                    record_hash: result.receipt.record_hash,
                    append_record_sequence: 0,
                    manifest_revision: result.revision,
                    lease_fence_token: 0,
                });
            }
        }
    }

    let response = MutationBatchResponse {
        batch_id: operation_digest,
        operation_receipts: receipts,
        watch_cursor: max_watch_cursor,
        mutation_id: context.request_id.clone(),
        write_state: write_state_for_transaction(transaction_id),
    };
    if let Some(transaction_id) = transaction_id {
        let mut additions = native_idempotency::prepare_response_in_transaction(
            &state.storage,
            &context,
            &target,
            &response,
        )
        .await?;
        additions.preconditions.extend(durable_preconditions);
        deduplicate_preconditions(&mut additions.preconditions);
        let receipt = state
            .core_store
            .stage_mutation_additions_in_transaction(
                transaction_id,
                &object_manager::transaction_principal_from_claims(&claims),
                additions,
            )
            .await
            .map_err(|error| {
                transaction_core_store_status(&error.to_string())
                    .unwrap_or_else(|| Status::internal(error.to_string()))
            })?;
        if receipt.state != CoreTransactionState::Open {
            return Err(Status::failed_precondition(
                "MutationBatchTransactionNotOpen",
            ));
        }
    } else {
        native_idempotency::store_response(&state.storage, &context, &target, &response).await?;
    }
    Ok(Response::new(response))
}
