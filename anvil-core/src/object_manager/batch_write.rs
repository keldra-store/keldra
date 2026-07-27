use super::*;
use crate::{mvcc_product::ProductMutationPlan, persistence::ObjectBatchCreateInput};
use futures_util::future::try_join_all;
use std::collections::BTreeSet;
use std::future::Future;

pub(crate) struct ObjectBatchPut {
    pub object_key: String,
    pub payload: Vec<u8>,
    pub content_type: Option<String>,
    pub user_metadata: Option<JsonValue>,
    pub storage_class_id: Option<String>,
}

struct ResolvedBatchPut {
    input: ObjectBatchPut,
    storage_class_id: String,
}

struct PreparedBatchPut {
    object_key: String,
    payload: Vec<u8>,
    size: i64,
    content_type: Option<String>,
    user_metadata: Option<JsonValue>,
    storage_class_id: String,
}

struct PreparedObjectBatch {
    bucket: Bucket,
    inputs: Vec<PreparedBatchPut>,
}

impl ObjectManager {
    pub(crate) async fn put_objects_batch_in_transaction<F, Fut>(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        inputs: Vec<ObjectBatchPut>,
        transaction_id: &str,
        transaction_principal: &str,
        visibility: ObjectWriteVisibility,
        build_additions: F,
    ) -> Result<Vec<Object>, Status>
    where
        F: FnOnce(&[Object]) -> Fut + Send,
        Fut: Future<Output = Result<ProductMutationPlan, Status>> + Send,
    {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        let prepared = self
            .prepare_object_batch(claims, bucket_name, inputs, visibility)
            .await?;

        // Reject a stale, terminal, foreign, or incorrectly scoped transaction before
        // CoreStore publishes any payload representation.
        self.preflight_object_transaction(&prepared.bucket, transaction_id, transaction_principal)
            .await?;

        let create_inputs = self
            .stage_payloads_for_mvcc_transaction(
                prepared.inputs,
                transaction_id,
                transaction_principal,
                &prepared.bucket.name,
            )
            .await?;

        let prepared_objects = self
            .persistence
            .prepare_objects_with_storage_class_in_transaction(
                claims.tenant_id,
                prepared.bucket.id,
                create_inputs,
                transaction_id,
                transaction_principal,
                visibility.persistence_options(),
            )
            .await
            .map_err(transaction_preflight_status)?;
        let additions = build_additions(&prepared_objects.objects).await?;
        self.persistence
            .stage_prepared_objects_in_transaction(
                prepared_objects,
                transaction_id,
                transaction_principal,
                additions,
            )
            .await
            .map_err(transaction_preflight_status)
    }

    async fn prepare_object_batch(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        inputs: Vec<ObjectBatchPut>,
        visibility: ObjectWriteVisibility,
    ) -> Result<PreparedObjectBatch, Status> {
        if matches!(visibility.indexes, IndexMaintenanceVisibility::CaughtUp) {
            return Err(Status::unimplemented(
                "INDEX_MAINTENANCE_CAUGHT_UP is reserved but not yet available for object writes",
            ));
        }
        if visibility.requires_payload_boundary_extraction() {
            return Err(Status::failed_precondition(
                "payload boundary extraction requires the single-object write path",
            ));
        }
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        for input in &inputs {
            if validation::is_reserved_internal_key(&input.object_key) {
                self.record_reserved_namespace_rejection("put_objects_batch");
                return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
            }
            if !validation::is_valid_object_key(&input.object_key) {
                return Err(Status::invalid_argument("Invalid object key"));
            }
        }

        let bucket = self
            .get_tenant_bucket(claims.tenant_id, bucket_name)
            .await?;
        let unique_keys = inputs
            .iter()
            .map(|input| input.object_key.as_str())
            .collect::<BTreeSet<_>>();
        let mvcc = self.installed_mvcc()?;
        try_join_all(unique_keys.into_iter().map(|object_key| {
            access_control::require_object_permission(
                &self.storage,
                mvcc,
                claims,
                &bucket,
                object_key,
                "put",
            )
        }))
        .await?;

        let resolved = self.resolve_batch_storage(inputs)?;
        let inputs = self.prepare_batch_payloads(resolved)?;
        Ok(PreparedObjectBatch { bucket, inputs })
    }

    fn resolve_batch_storage(
        &self,
        inputs: Vec<ObjectBatchPut>,
    ) -> Result<Vec<ResolvedBatchPut>, Status> {
        inputs
            .into_iter()
            .map(|input| {
                let storage_class_id = self
                    .core_store
                    .resolve_storage_class_id(input.storage_class_id.as_deref())
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                self.core_store
                    .get_storage_class(&storage_class_id)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                Ok(ResolvedBatchPut {
                    input,
                    storage_class_id,
                })
            })
            .collect()
    }

    fn prepare_batch_payloads(
        &self,
        inputs: Vec<ResolvedBatchPut>,
    ) -> Result<Vec<PreparedBatchPut>, Status> {
        let mut prepared = Vec::with_capacity(inputs.len());
        for resolved in inputs {
            let ObjectBatchPut {
                object_key,
                payload,
                content_type,
                user_metadata,
                storage_class_id: _,
            } = resolved.input;
            let payload_len = u64::try_from(payload.len())
                .map_err(|_| Status::invalid_argument("Object payload is too large"))?;
            let size = i64::try_from(payload_len)
                .map_err(|_| Status::invalid_argument("Object payload is too large"))?;
            prepared.push(PreparedBatchPut {
                object_key,
                payload,
                size,
                content_type,
                user_metadata,
                storage_class_id: resolved.storage_class_id,
            });
        }
        Ok(prepared)
    }

    async fn stage_payloads_for_mvcc_transaction(
        &self,
        inputs: Vec<PreparedBatchPut>,
        transaction_id: &str,
        transaction_principal: &str,
        bucket_name: &str,
    ) -> Result<Vec<ObjectBatchCreateInput>, Status> {
        let mut create_inputs = Vec::with_capacity(inputs.len());
        for input in inputs {
            let PreparedBatchPut {
                object_key,
                payload,
                size,
                content_type,
                user_metadata,
                storage_class_id,
            } = input;
            // The batch payload already arrived in memory from the unary RPC.
            // Feed it straight into the same bounded-stripe MVCC ingest used by
            // streaming puts; never copy it through a complete scratch file.
            let prepared = self
                .prepare_mvcc_object_ingest(
                    futures_util::stream::iter(vec![Ok(payload)]),
                    transaction_id,
                    transaction_principal,
                    bucket_name,
                    &object_key,
                    None,
                )
                .await?;
            let target = object_data_target_from_shard_map(&prepared.shard_map)
                .map_err(|error| Status::internal(error.to_string()))?;
            let content_hash = prepared.object_hash;
            create_inputs.push(object_batch_create_input(
                (
                    object_key,
                    size,
                    content_type,
                    user_metadata,
                    storage_class_id,
                ),
                content_hash,
                target,
            )?);
        }
        Ok(create_inputs)
    }

    async fn preflight_object_transaction(
        &self,
        bucket: &Bucket,
        transaction_id: &str,
        transaction_principal: &str,
    ) -> Result<(), Status> {
        let _ = bucket;
        self.persistence
            .mvcc()
            .map_err(transaction_preflight_status)?
            .open_transactions
            .binding(transaction_id, transaction_principal)
            .map_err(transaction_preflight_status)?;
        Ok(())
    }
}

fn object_batch_create_input(
    metadata: (String, i64, Option<String>, Option<JsonValue>, String),
    content_hash: String,
    target: ObjectDataTarget,
) -> Result<ObjectBatchCreateInput, Status> {
    let (object_key, size, content_type, user_metadata, storage_class_id) = metadata;
    let shard_map = object_data_target_to_shard_map(&target)
        .map_err(|error| Status::internal(error.to_string()))?;
    Ok(ObjectBatchCreateInput {
        key: object_key,
        content_hash: content_hash.clone(),
        size,
        etag: content_hash,
        content_type,
        user_meta: user_metadata,
        shard_map,
        storage_class: storage_class_id,
    })
}

fn transaction_preflight_status(error: anyhow::Error) -> Status {
    let message = error.to_string();
    if message.contains("TransactionNotFound") || message.contains("unknown transaction") {
        Status::not_found("TransactionNotFound")
    } else if message.contains("TransactionPrincipalMismatch")
        || message.contains("another principal")
    {
        Status::permission_denied("TransactionPrincipalMismatch")
    } else if message.contains("TransactionExpired")
        || message.contains("TransactionRolledBack")
        || message.contains("TransactionAlreadyCommitted")
        || message.contains("TransactionNotOpen")
        || message.contains("TransactionNotCommittable")
        || message.contains("no longer accept staged data")
        || message.contains("transaction has expired")
    {
        Status::failed_precondition(message)
    } else if message.contains("TransactionConflict") {
        Status::aborted("TransactionConflict")
    } else {
        core_store_status(error)
    }
}
