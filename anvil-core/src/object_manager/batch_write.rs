use super::*;
use crate::{
    core_store::{CoreMutationBatchAdditions, CoreTransactionState},
    metadata_journal::object_metadata_partition_id,
    persistence::ObjectBatchCreateInput,
};
use futures_util::future::try_join_all;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashSet};
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
    inline_eligible: bool,
}

struct PreparedBatchPut {
    object_key: String,
    payload: Vec<u8>,
    size: i64,
    content_type: Option<String>,
    user_metadata: Option<JsonValue>,
    storage_class_id: String,
    inline_eligible: bool,
    logical_name: String,
    boundary_values: Vec<CoreBoundaryValue>,
    mutation_id: String,
}

struct PreparedObjectBatch {
    bucket: Bucket,
    inputs: Vec<PreparedBatchPut>,
    payload_preparation: BatchPayloadPreparation,
}

#[derive(Clone, Copy)]
enum BatchPayloadPreparation {
    PhysicalInline,
    EstablishedPerObject,
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
        Fut: Future<Output = Result<CoreMutationBatchAdditions, Status>> + Send,
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

        let create_inputs = match prepared.payload_preparation {
            BatchPayloadPreparation::PhysicalInline => {
                self.stage_physical_inline_batch(prepared.inputs).await?
            }
            BatchPayloadPreparation::EstablishedPerObject => {
                self.stage_payloads_through_established_paths(prepared.inputs)
                    .await?
            }
        };

        let prepared_objects = self
            .persistence
            .prepare_objects_with_storage_class_in_transaction(
                claims.tenant_id,
                prepared.bucket.id,
                create_inputs,
                transaction_id,
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

    pub(crate) async fn put_objects_batch<F, Fut>(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        inputs: Vec<ObjectBatchPut>,
        publication_transaction_id: &str,
        visibility: ObjectWriteVisibility,
        build_additions: F,
    ) -> Result<Vec<Object>, Status>
    where
        F: FnOnce(&[Object]) -> Fut + Send,
        Fut: Future<Output = Result<CoreMutationBatchAdditions, Status>> + Send,
    {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        let prepared = self
            .prepare_object_batch(claims, bucket_name, inputs, visibility)
            .await?;
        let create_inputs = match prepared.payload_preparation {
            BatchPayloadPreparation::PhysicalInline => {
                self.stage_physical_inline_batch(prepared.inputs).await?
            }
            BatchPayloadPreparation::EstablishedPerObject => {
                self.stage_payloads_through_established_paths(prepared.inputs)
                    .await?
            }
        };
        let prepared_objects = self
            .persistence
            .prepare_objects_with_storage_class(
                claims.tenant_id,
                prepared.bucket.id,
                create_inputs,
                visibility.persistence_options(),
            )
            .await
            .map_err(transaction_preflight_status)?;
        let additions = build_additions(&prepared_objects.objects).await?;
        let objects = self
            .persistence
            .commit_prepared_objects(prepared_objects, publication_transaction_id, additions)
            .await
            .map_err(transaction_preflight_status)?;

        if visibility.defers_write_maintenance() {
            for object in &objects {
                self.schedule_deferred_object_maintenance(prepared.bucket.clone(), &object.key);
            }
        }
        if visibility.requires_authz_materialization() {
            access_control::grant_object_defaults_batch(
                &self.persistence,
                objects
                    .iter()
                    .map(|object| (&prepared.bucket, object.key.as_str())),
                "grant object parent bucket relations",
            )
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        }
        Ok(objects)
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
        try_join_all(unique_keys.into_iter().map(|object_key| {
            access_control::require_object_permission(
                &self.storage,
                claims,
                &bucket,
                object_key,
                "put",
            )
        }))
        .await?;

        let resolved = self.resolve_batch_storage(inputs)?;
        let payload_preparation = if resolved.iter().all(|input| input.inline_eligible)
            && payload_digests_are_unique(&resolved)
        {
            BatchPayloadPreparation::PhysicalInline
        } else {
            BatchPayloadPreparation::EstablishedPerObject
        };
        let inputs = self
            .prepare_batch_payloads(claims, &bucket, resolved)
            .await?;
        Ok(PreparedObjectBatch {
            bucket,
            inputs,
            payload_preparation,
        })
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
                let storage_class = self
                    .core_store
                    .get_storage_class(&storage_class_id)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                let payload_len = u64::try_from(input.payload.len())
                    .map_err(|_| Status::invalid_argument("Object payload is too large"))?;
                let inline_eligible = storage_class.inline_payload_policy.enabled
                    && payload_len
                        <= storage_class
                            .inline_payload_policy
                            .effective_raw_payload_cap_bytes();
                Ok(ResolvedBatchPut {
                    input,
                    storage_class_id,
                    inline_eligible,
                })
            })
            .collect()
    }

    async fn prepare_batch_payloads(
        &self,
        claims: &auth::Claims,
        bucket: &Bucket,
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
            let boundary_values = self
                .object_write_boundary_values_from_hints(
                    claims.tenant_id,
                    &bucket.name,
                    &object_key,
                    content_type.as_deref(),
                    user_metadata.as_ref(),
                    payload_len,
                )
                .await?;
            let logical_name = format!(
                "tenant:{}/bucket:{}/object:{}",
                claims.tenant_id, bucket.name, object_key
            );
            prepared.push(PreparedBatchPut {
                object_key,
                payload,
                size,
                content_type,
                user_metadata,
                storage_class_id: resolved.storage_class_id,
                inline_eligible: resolved.inline_eligible,
                logical_name,
                boundary_values,
                mutation_id: uuid::Uuid::new_v4().to_string(),
            });
        }
        Ok(prepared)
    }

    async fn stage_physical_inline_batch(
        &self,
        inputs: Vec<PreparedBatchPut>,
    ) -> Result<Vec<ObjectBatchCreateInput>, Status> {
        let mut metadata = Vec::with_capacity(inputs.len());
        let mut blobs = Vec::with_capacity(inputs.len());
        for input in inputs {
            let storage_class_id = input.storage_class_id.clone();
            blobs.push((
                PutBlob {
                    logical_name: input.logical_name,
                    bytes: input.payload,
                    boundary_values: input.boundary_values,
                    region_id: self.region.clone(),
                    mutation_id: input.mutation_id,
                },
                Some(storage_class_id),
            ));
            metadata.push((
                input.object_key,
                input.size,
                input.content_type,
                input.user_metadata,
                input.storage_class_id,
            ));
        }

        let object_refs = self
            .core_store
            .put_blobs_with_storage_classes(blobs)
            .await
            .map_err(core_store_status)?;
        if object_refs.len() != metadata.len() {
            return Err(Status::internal(
                "CoreStore returned an incomplete physical object batch",
            ));
        }

        metadata
            .into_iter()
            .zip(object_refs)
            .map(|(metadata, object_ref)| {
                let content_hash = object_ref.hash.clone();
                object_batch_create_input(
                    metadata,
                    content_hash,
                    ObjectDataTarget::ObjectRef(object_ref),
                )
            })
            .collect()
    }

    async fn stage_payloads_through_established_paths(
        &self,
        inputs: Vec<PreparedBatchPut>,
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
                inline_eligible,
                logical_name,
                boundary_values,
                mutation_id,
            } = input;
            let (content_hash, target) = if inline_eligible {
                let object_ref = self
                    .core_store
                    .put_blob_with_storage_class(
                        PutBlob {
                            logical_name,
                            bytes: payload,
                            boundary_values,
                            region_id: self.region.clone(),
                            mutation_id,
                        },
                        Some(&storage_class_id),
                    )
                    .await
                    .map_err(core_store_status)?;
                (
                    object_ref.hash.clone(),
                    ObjectDataTarget::ObjectRef(object_ref),
                )
            } else {
                let pipeline_policy = self
                    .core_store
                    .pipeline_policy_for_storage_class(Some(&storage_class_id))
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                let logical_write = self
                    .core_store
                    .write_logical_file_with_locator(WriteLogicalFileRequest {
                        writer_family: WriterFamily::ObjectBlob.as_str().to_string(),
                        generation: 0,
                        logical_file_id: logical_name,
                        source: payload,
                        range_hints: Vec::new(),
                        pipeline_policy,
                        trace_context: Default::default(),
                        boundary_values,
                        mutation_id,
                        region_id: self.region.clone(),
                    })
                    .await
                    .map_err(core_store_status)?;
                (
                    logical_write.manifest.content_hash,
                    ObjectDataTarget::LogicalFile(logical_write.locator),
                )
            };
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
        let transaction = self
            .core_store
            .read_explicit_transaction_for_principal(transaction_id, transaction_principal)
            .await
            .map_err(transaction_preflight_status)?;
        if transaction.state != CoreTransactionState::Open {
            return Err(transaction_state_status(transaction.state));
        }

        let expected_scope = hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
        let expected_root_hash = CoreStore::root_key_hash_for_anchor(&expected_scope);
        if transaction.root_anchor_key != expected_scope
            || transaction.scope_partition != expected_scope
            || transaction.root_key_hash != expected_root_hash
        {
            return Err(Status::failed_precondition("TransactionScopeMismatch"));
        }
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

fn payload_digests_are_unique(inputs: &[ResolvedBatchPut]) -> bool {
    let mut digests = HashSet::with_capacity(inputs.len());
    inputs.iter().all(|input| {
        let digest: [u8; 32] = Sha256::digest(&input.input.payload).into();
        digests.insert(digest)
    })
}

fn transaction_preflight_status(error: anyhow::Error) -> Status {
    let message = error.to_string();
    if message.contains("TransactionNotFound") {
        Status::not_found("TransactionNotFound")
    } else if message.contains("TransactionPrincipalMismatch") {
        Status::permission_denied("TransactionPrincipalMismatch")
    } else if message.contains("TransactionScopeMismatch") {
        Status::failed_precondition("TransactionScopeMismatch")
    } else if message.contains("TransactionExpired")
        || message.contains("TransactionRolledBack")
        || message.contains("TransactionAlreadyCommitted")
        || message.contains("TransactionNotOpen")
        || message.contains("TransactionNotCommittable")
    {
        Status::failed_precondition(message)
    } else if message.contains("TransactionConflict") {
        Status::aborted("TransactionConflict")
    } else {
        core_store_status(error)
    }
}

fn transaction_state_status(state: CoreTransactionState) -> Status {
    let message = match state {
        CoreTransactionState::Expired => "TransactionExpired",
        CoreTransactionState::RolledBack => "TransactionRolledBack",
        CoreTransactionState::Committed => "TransactionAlreadyCommitted",
        _ => "TransactionNotOpen",
    };
    Status::failed_precondition(message)
}
