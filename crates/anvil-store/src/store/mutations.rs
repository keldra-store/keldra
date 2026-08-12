use super::*;
use crate::model::{
    CoordinatedObjectMutation, MUTATION_STAMP_FORMAT, MutationStamp, OBJECT_MUTATION_FORMAT,
    ObjectMutation, ObjectMutationContext, ObjectMutationGovernance, ReplicaObjectMutationApplied,
};
use crate::{
    DefinitionMutationIntent, DefinitionOperation, DefinitionStateError, DefinitionTransition,
};

const MAX_EXPIRED_RECEIPTS_PRUNED_PER_PASS: usize = 1_024;
const MAX_EXPIRED_RECEIPT_BYTES_PRUNED_PER_PASS: u64 = 4 * 1024 * 1024;

#[derive(Clone, Copy)]
pub(super) struct DistributedEvaluationContext {
    pub(super) mutation: ObjectMutationContext,
    pub(super) source_id: SourceId,
    pub(super) source_journal_position: u64,
}

pub(super) struct EvaluatedOperation {
    pub(super) receipt: MutationReceipt,
    pub(super) mutation: Option<ObjectMutation>,
    pub(super) reference_deltas: Vec<ReferenceDelta>,
    pub(super) accounting_transition: Option<AccountingHeadTransition>,
    pub(super) definition_transition: Option<DefinitionTransition>,
}

fn is_mutation_capacity(error: &MutationError) -> bool {
    mutation_capacity_kind(error).is_some()
}

fn mutation_capacity_kind(error: &MutationError) -> Option<&'static str> {
    match error {
        MutationError::ReceiptCapacity => Some("receipt"),
        MutationError::SourceJournalCapacity => Some("source_journal"),
        _ => None,
    }
}

fn fail_unresolved_prepared(
    results: &mut BTreeMap<usize, Result<MutationReceipt, MutationError>>,
    prepared: &[(usize, PreparedOperation)],
    error: MutationError,
) {
    for (index, _) in prepared {
        let result = results.entry(*index).or_insert_with(|| Err(error.clone()));
        if result.is_ok() || result.as_ref().is_err_and(is_mutation_capacity) {
            *result = Err(error.clone());
        }
    }
}

impl Store {
    pub async fn put(&self, request: PutRequest) -> Result<MutationReceipt, MutationError> {
        self.bulk_write(vec![BatchOperation::Put(request)])
            .await
            .pop()
            .expect("one operation has one outcome")
            .result
    }

    pub async fn publish(&self, request: PublishRequest) -> Result<MutationReceipt, MutationError> {
        self.bulk_write(vec![BatchOperation::Publish(request)])
            .await
            .pop()
            .expect("one operation has one outcome")
            .result
    }

    pub async fn delete(&self, request: DeleteRequest) -> Result<MutationReceipt, MutationError> {
        self.bulk_write(vec![BatchOperation::Delete(request)])
            .await
            .pop()
            .expect("one operation has one outcome")
            .result
    }

    /// Preserve the one-node physical WriteBatch path while evaluating the
    /// coordinator-reconciled bucket options supplied by the cluster layer.
    pub async fn mutate_with_governance(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
    ) -> Result<MutationReceipt, MutationError> {
        governance.validate()?;
        self.bulk_write_inner(vec![operation], Some(governance), None, false)
            .await
            .pop()
            .expect("one operation has one outcome")
            .result
    }

    pub async fn mutate_with_governance_and_backpressure(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
    ) -> Result<MutationReceipt, MutationError> {
        governance.validate()?;
        self.bulk_write_inner(vec![operation], Some(governance), None, true)
            .await
            .pop()
            .expect("one operation has one outcome")
            .result
    }

    /// Trusted single-node definition mutation. The typed intent is converted
    /// to a version-bound transition inside the same commit batch.
    pub async fn mutate_definition_with_governance(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
        intent: DefinitionMutationIntent,
    ) -> Result<MutationReceipt, MutationError> {
        governance.validate()?;
        intent.validate().map_err(definition_mutation_error)?;
        self.bulk_write_inner(vec![operation], Some(governance), Some(intent), false)
            .await
            .pop()
            .expect("one operation has one outcome")
            .result
    }

    pub async fn mutate_definition_with_governance_and_backpressure(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
        intent: DefinitionMutationIntent,
    ) -> Result<MutationReceipt, MutationError> {
        governance.validate()?;
        intent.validate().map_err(definition_mutation_error)?;
        self.bulk_write_inner(vec![operation], Some(governance), Some(intent), true)
            .await
            .pop()
            .expect("one operation has one outcome")
            .result
    }

    /// Evaluates independent operations in request order and persists all
    /// successful outcomes with one physical RocksDB write. A failed
    /// precondition is an item result, not a reason to retry the whole bulk.
    pub async fn bulk_write(&self, operations: Vec<BatchOperation>) -> Vec<BatchOutcome> {
        self.bulk_write_inner(operations, None, None, false).await
    }

    /// Applies one public/internal coordinator batch with capacity
    /// backpressure. The original prepared payloads remain owned by this call
    /// while source-journal or receipt capacity catches up, so retrying does
    /// not clone request bytes and command IDs retain their replay contract.
    pub async fn bulk_write_with_backpressure(
        &self,
        operations: Vec<BatchOperation>,
    ) -> Vec<BatchOutcome> {
        self.bulk_write_inner(operations, None, None, true).await
    }

    async fn bulk_write_inner(
        &self,
        operations: Vec<BatchOperation>,
        governance: Option<ObjectMutationGovernance>,
        definition_intent: Option<DefinitionMutationIntent>,
        backpressure: bool,
    ) -> Vec<BatchOutcome> {
        if definition_intent.is_some() && operations.len() != 1 {
            return operations
                .into_iter()
                .enumerate()
                .map(|(index, _)| BatchOutcome {
                    index,
                    result: Err(MutationError::InvalidObjectMutation(
                        "one typed definition intent must describe exactly one operation".into(),
                    )),
                })
                .collect();
        }
        let prepare_started = std::time::Instant::now();
        let mut prepared = Vec::with_capacity(operations.len());
        let mut early = BTreeMap::new();
        let mut identity_cache =
            BTreeMap::<(String, String), Result<BucketIdentity, MutationError>>::new();
        for (index, operation) in operations.into_iter().enumerate() {
            let logical_key = match &operation {
                BatchOperation::Put(request) => &request.key,
                BatchOperation::Publish(request) => &request.key,
                BatchOperation::Delete(request) => &request.key,
            };
            let identity = governance.as_ref().map_or_else(
                || {
                    let cache_key = (
                        logical_key.tenant().to_owned(),
                        logical_key.bucket().to_owned(),
                    );
                    identity_cache
                        .entry(cache_key)
                        .or_insert_with(|| {
                            self.resolve_bucket_identity(logical_key.tenant(), logical_key.bucket())
                        })
                        .clone()
                },
                |governance| {
                    Ok(BucketIdentity {
                        tenant_id: TenantId(governance.tenant_id),
                        bucket_id: BucketId(governance.bucket_id),
                    })
                },
            );
            let result = match identity {
                Ok(identity) => self.prepare(operation, identity, false).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(operation) => prepared.push((index, operation)),
                Err(error) => {
                    early.insert(index, error);
                }
            }
        }
        let prepare_duration = prepare_started.elapsed();
        tracing::info!(
            histogram.anvil_store_bulk_prepare_duration_seconds = prepare_duration.as_secs_f64(),
            operation_count = prepared.len(),
            "object storage bulk preparation completed"
        );
        let mut completed = BTreeMap::<usize, Result<MutationReceipt, MutationError>>::new();

        loop {
            let _policy_guard = self.policy_gate.read().await;
            let lock_started = std::time::Instant::now();
            let _guards = self
                .ordinary_locks
                .acquire(
                    &prepared
                        .iter()
                        .map(|(_, operation)| object_path(operation.key()))
                        .collect::<Vec<_>>(),
                )
                .await;
            let _commit_guard = self.commit_lock.lock().await;
            let lock_duration = lock_started.elapsed();
            let mut batch = WriteBatch::default();
            let now = match now_unix_millis() {
                Ok(now) => now,
                Err(error) => {
                    return fail_prepared_operations(completed, early, prepared, error);
                }
            };
            let mut receipt_status = match self.mutation_receipt_status() {
                Ok(status) => status,
                Err(error) => {
                    return fail_prepared_operations(completed, early, prepared, error);
                }
            };
            let initial_receipt_status = receipt_status;
            let pruned_receipts =
                match self.stage_expired_mutation_receipts(&mut batch, now, &mut receipt_status) {
                    Ok(pruned) => pruned,
                    Err(error) => {
                        return fail_prepared_operations(completed, early, prepared, error);
                    }
                };
            let mut pending_heads = BTreeMap::<Vec<u8>, Head>::new();
            let mut pending_versions = BTreeMap::<Vec<u8>, Version>::new();
            let mut pending_receipts = BTreeMap::<Vec<u8>, StoredReceipt>::new();
            let mut pending_blob_references = PendingBlobReferences::new();
            let mut pending_small_blobs = BTreeSet::<Vec<u8>>::new();
            let mut policy_cache = BTreeMap::<Vec<u8>, Result<BucketPolicy, MutationError>>::new();
            let mut versioning_cache =
                BTreeMap::<Vec<u8>, Result<ObjectVersioning, MutationError>>::new();
            if let Some(governance) = governance.as_ref() {
                let identity = BucketIdentity {
                    tenant_id: TenantId(governance.tenant_id),
                    bucket_id: BucketId(governance.bucket_id),
                }
                .encode()
                .to_vec();
                policy_cache.insert(identity.clone(), Ok(governance.policy.clone()));
                versioning_cache.insert(identity, Ok(governance.versioning));
            }
            let mut results = BTreeMap::<usize, Result<MutationReceipt, MutationError>>::new();
            let mut batch_high_watermark = None;
            let mut pending_changes = Vec::new();
            let mut receipt_capacity_at = None;
            let evaluate_started = std::time::Instant::now();
            for (prepared_index, (index, operation)) in prepared.iter().enumerate() {
                let outcome = self
                    .evaluate_operation(
                        &operation,
                        &mut batch,
                        &mut pending_heads,
                        &mut pending_versions,
                        &mut pending_receipts,
                        &mut pending_blob_references,
                        &mut pending_small_blobs,
                        &mut policy_cache,
                        &mut versioning_cache,
                        &pruned_receipts,
                        &mut receipt_status,
                        now,
                        None,
                        definition_intent,
                    )
                    .await;
                if backpressure
                    && outcome
                        .as_ref()
                        .is_err_and(|error| matches!(error, MutationError::ReceiptCapacity))
                {
                    results.insert(*index, outcome.map(|evaluated| evaluated.receipt));
                    receipt_capacity_at = Some(prepared_index);
                    break;
                }
                if let Ok(evaluated) = &outcome
                    && !evaluated.receipt.replayed
                {
                    batch_high_watermark = Some(
                        batch_high_watermark
                            .map_or(evaluated.receipt.version, |current: VersionId| {
                                current.max(evaluated.receipt.version)
                            }),
                    );
                    pending_changes.push(PendingLocalChange::ObjectHead {
                        identity: operation.identity(),
                        exact_path: operation.key().path().to_owned(),
                        path_version: evaluated.receipt.version,
                        deleted: evaluated.receipt.deleted,
                        reference_deltas: evaluated.reference_deltas.clone(),
                        accounting_transition: evaluated.accounting_transition,
                        definition_transition: evaluated.definition_transition.clone(),
                    });
                }
                results.insert(*index, outcome.map(|evaluated| evaluated.receipt));
            }
            let evaluate_duration = evaluate_started.elapsed();

            let persistence_started = std::time::Instant::now();
            let persistence = (|| {
                if receipt_status != initial_receipt_status {
                    self.stage_mutation_receipt_status(&mut batch, receipt_status)?;
                }
                self.stage_local_changes(
                    &mut batch,
                    &pending_changes,
                    LocalReferenceEffects::AppliedInline,
                )?;
                if let Some(high_watermark) = batch_high_watermark {
                    batch.put_cf(
                        self.cf(CF_METADATA)?,
                        VERSION_HIGH_WATERMARK_KEY,
                        serde_json::to_vec(&high_watermark).map_err(storage_error)?,
                    );
                }
                if batch.is_empty() {
                    return Ok(());
                }
                let mut options = WriteOptions::default();
                options.set_sync(self.sync_writes);
                self.db.write_opt(batch, &options).map_err(storage_error)
            })();
            let persistence_duration = persistence_started.elapsed();
            tracing::info!(
                histogram.anvil_store_bulk_lock_duration_seconds = lock_duration.as_secs_f64(),
                histogram.anvil_store_bulk_evaluate_duration_seconds =
                    evaluate_duration.as_secs_f64(),
                histogram.anvil_store_bulk_persist_duration_seconds =
                    persistence_duration.as_secs_f64(),
                operation_count = prepared.len(),
                "object storage bulk phases completed"
            );
            match persistence {
                Ok(()) => {
                    if !pruned_receipts.is_empty() {
                        self.mutation_capacity_notify.notify_waiters();
                    }
                    if !pending_changes.is_empty() {
                        if let Err(error) = self.settle_inline_source_changes() {
                            fail_unresolved_prepared(&mut results, &prepared, error);
                            completed.extend(results);
                            return completed
                                .into_iter()
                                .chain(early.into_iter().map(|(index, error)| (index, Err(error))))
                                .map(|(index, result)| BatchOutcome { index, result })
                                .collect();
                        }
                        self.notify_local_invalidations();
                    }
                }
                Err(error) if backpressure && is_mutation_capacity(&error) => {
                    let capacity =
                        mutation_capacity_kind(&error).expect("capacity error was matched");
                    drop(_commit_guard);
                    drop(_guards);
                    drop(_policy_guard);
                    self.wait_for_capacity_with_metrics(capacity).await;
                    continue;
                }
                Err(error) => {
                    fail_unresolved_prepared(&mut results, &prepared, error);
                    completed.extend(results);
                    completed.extend(early.into_iter().map(|(index, error)| (index, Err(error))));
                    return completed
                        .into_iter()
                        .map(|(index, result)| BatchOutcome { index, result })
                        .collect();
                }
            }
            if backpressure && let Some(retry_from) = receipt_capacity_at {
                let retry = prepared.split_off(retry_from);
                let capacity_index = retry
                    .first()
                    .map(|(index, _)| *index)
                    .expect("receipt-capacity retry retains its first operation");
                for (index, result) in results {
                    if index != capacity_index {
                        completed.insert(index, result);
                    }
                }
                prepared = retry;
                drop(_commit_guard);
                drop(_guards);
                drop(_policy_guard);
                self.wait_for_capacity_with_metrics("receipt").await;
                continue;
            }
            completed.extend(results);
            completed.extend(early.into_iter().map(|(index, error)| (index, Err(error))));
            return completed
                .into_iter()
                .map(|(index, result)| BatchOutcome { index, result })
                .collect();
        }
    }

    async fn wait_for_capacity_with_metrics(&self, capacity: &'static str) {
        let wait = super::MutationBackpressureWait::start(capacity);
        self.wait_for_mutation_capacity().await;
        wait.complete();
    }

    /// Evaluates and durably applies one exact-path mutation on its current
    /// coordinator, returning the complete bounded result peers must apply.
    /// Network routing, replica selection, and acknowledgement policy remain
    /// outside the storage kernel.
    pub async fn coordinate_object_mutation(
        &self,
        operation: BatchOperation,
        context: ObjectMutationContext,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        let _policy_guard = self.policy_gate.read().await;
        let logical_key = match &operation {
            BatchOperation::Put(request) => &request.key,
            BatchOperation::Publish(request) => &request.key,
            BatchOperation::Delete(request) => &request.key,
        };
        let identity = self.resolve_bucket_identity(logical_key.tenant(), logical_key.bucket())?;
        let governance = ObjectMutationGovernance {
            tenant_id: identity.tenant_id.0,
            bucket_id: identity.bucket_id.0,
            versioning: self.bucket_versioning_by_key(&identity.encode())?,
            policy: self
                .bucket_policy_by_key(&identity.encode())?
                .unwrap_or_default(),
        };
        self.coordinate_object_mutation_with_governance(operation, governance, context)
            .await
    }

    pub async fn coordinate_object_mutation_with_governance(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        governance.validate()?;
        let identity = BucketIdentity {
            tenant_id: TenantId(governance.tenant_id),
            bucket_id: BucketId(governance.bucket_id),
        };
        let prepared = self.prepare(operation, identity, true).await?;
        self.coordinate_prepared_object_mutation(prepared, context, governance, None)
            .await
    }

    pub async fn coordinate_definition_object_mutation_with_governance(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
        intent: DefinitionMutationIntent,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        governance.validate()?;
        intent.validate().map_err(definition_mutation_error)?;
        let identity = BucketIdentity {
            tenant_id: TenantId(governance.tenant_id),
            bucket_id: BucketId(governance.bucket_id),
        };
        let prepared = self.prepare(operation, identity, true).await?;
        self.coordinate_prepared_object_mutation(prepared, context, governance, Some(intent))
            .await
    }

    /// Coordinates a distributed publish whose payload evidence was verified
    /// by the cluster layer on the current path coordinator.
    ///
    /// Unlike [`Store::coordinate_object_mutation`], this exact boundary does
    /// not require the complete payload source to be present on the metadata
    /// coordinator. Ordinary local publishes and every other operation retain
    /// their existing local-byte check.
    pub async fn coordinate_distributed_publish(
        &self,
        request: PublishRequest,
        context: ObjectMutationContext,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        let _policy_guard = self.policy_gate.read().await;
        let identity = self.resolve_bucket_identity(request.key.tenant(), request.key.bucket())?;
        let governance = ObjectMutationGovernance {
            tenant_id: identity.tenant_id.0,
            bucket_id: identity.bucket_id.0,
            versioning: self.bucket_versioning_by_key(&identity.encode())?,
            policy: self
                .bucket_policy_by_key(&identity.encode())?
                .unwrap_or_default(),
        };
        self.coordinate_distributed_publish_with_governance(request, governance, context)
            .await
    }

    pub async fn coordinate_distributed_publish_with_governance(
        &self,
        request: PublishRequest,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        governance.validate()?;
        let identity = BucketIdentity {
            tenant_id: TenantId(governance.tenant_id),
            bucket_id: BucketId(governance.bucket_id),
        };
        let prepared = self.prepare_verified_distributed_publish(request, identity)?;
        self.coordinate_prepared_object_mutation(prepared, context, governance, None)
            .await
    }

    pub async fn coordinate_distributed_definition_publish_with_governance(
        &self,
        request: PublishRequest,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
        intent: DefinitionMutationIntent,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        governance.validate()?;
        intent.validate().map_err(definition_mutation_error)?;
        let identity = BucketIdentity {
            tenant_id: TenantId(governance.tenant_id),
            bucket_id: BucketId(governance.bucket_id),
        };
        let prepared = self.prepare_verified_distributed_publish(request, identity)?;
        self.coordinate_prepared_object_mutation(prepared, context, governance, Some(intent))
            .await
    }

    pub(super) fn prepare_verified_distributed_publish(
        &self,
        request: PublishRequest,
        identity: BucketIdentity,
    ) -> Result<PreparedOperation, MutationError> {
        validate_command_id(request.command_id.as_deref())?;
        let fingerprint = publish_fingerprint(&request, identity);
        Ok(PreparedOperation::Publish {
            request,
            identity,
            fingerprint,
        })
    }

    async fn coordinate_prepared_object_mutation(
        &self,
        prepared: PreparedOperation,
        context: ObjectMutationContext,
        governance: ObjectMutationGovernance,
        definition_intent: Option<DefinitionMutationIntent>,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if prepared.command_id().is_none() {
            return Err(MutationError::InvalidCommandId);
        }
        let identity = prepared.identity();

        let _path_guard = self
            .ordinary_locks
            .acquire(&[object_path(prepared.key())])
            .await;
        let _commit_guard = self.commit_lock.lock().await;
        let source = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let source_journal_position = source.tail.checked_add(1).ok_or_else(|| {
            MutationError::Storage("local invalidation offset is exhausted".into())
        })?;
        let now = now_unix_millis()?;
        let mut batch = WriteBatch::default();
        let mut receipt_status = self.mutation_receipt_status()?;
        let initial_receipt_status = receipt_status;
        let pruned_receipts =
            self.stage_expired_mutation_receipts(&mut batch, now, &mut receipt_status)?;
        let mut pending_heads = BTreeMap::new();
        let mut pending_versions = BTreeMap::new();
        let mut pending_receipts = BTreeMap::new();
        let mut pending_blob_references = PendingBlobReferences::new();
        let mut pending_small_blobs = BTreeSet::new();
        let encoded_bucket = prepared.identity().encode().to_vec();
        let mut policy_cache = BTreeMap::from([(encoded_bucket.clone(), Ok(governance.policy))]);
        let mut versioning_cache = BTreeMap::from([(encoded_bucket, Ok(governance.versioning))]);
        let evaluated = self
            .evaluate_operation(
                &prepared,
                &mut batch,
                &mut pending_heads,
                &mut pending_versions,
                &mut pending_receipts,
                &mut pending_blob_references,
                &mut pending_small_blobs,
                &mut policy_cache,
                &mut versioning_cache,
                &pruned_receipts,
                &mut receipt_status,
                now,
                Some(DistributedEvaluationContext {
                    mutation: context,
                    source_id: source.source_id,
                    source_journal_position,
                }),
                definition_intent,
            )
            .await?;

        let created = !evaluated.receipt.replayed;
        if created {
            let mutation = evaluated.mutation.as_ref().ok_or_else(|| {
                MutationError::Storage("distributed mutation result is missing".into())
            })?;
            if mutation.stamp.source_journal_position != source_journal_position {
                return Err(MutationError::Storage(
                    "distributed mutation source position changed during evaluation".into(),
                ));
            }
            self.stage_local_changes(
                &mut batch,
                &[PendingLocalChange::ObjectHead {
                    identity,
                    exact_path: prepared.key().path().to_owned(),
                    path_version: evaluated.receipt.version,
                    deleted: evaluated.receipt.deleted,
                    reference_deltas: evaluated.reference_deltas.clone(),
                    accounting_transition: evaluated.accounting_transition,
                    definition_transition: evaluated.definition_transition.clone(),
                }],
                LocalReferenceEffects::Deferred,
            )?;
            batch.put_cf(
                self.cf(CF_METADATA)?,
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&evaluated.receipt.version).map_err(storage_error)?,
            );
        }
        if let Some(mutation) = evaluated.mutation.as_ref() {
            self.stage_object_mutation_reference_proof(&mut batch, mutation)?;
        }
        if receipt_status != initial_receipt_status {
            self.stage_mutation_receipt_status(&mut batch, receipt_status)?;
        }
        if !batch.is_empty() {
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
        }
        if !pruned_receipts.is_empty() {
            self.mutation_capacity_notify.notify_waiters();
        }
        if created {
            self.notify_local_invalidations();
        }
        Ok(CoordinatedObjectMutation {
            receipt: evaluated.receipt,
            mutation: evaluated.mutation,
        })
    }

    /// Applies one coordinator-produced exact-path result to a complete
    /// metadata replica. Content reference counts are deliberately not
    /// changed here; their actual owners consume the ordered source journal.
    pub async fn apply_object_mutation_replica(
        &self,
        mutation: &ObjectMutation,
    ) -> Result<ReplicaObjectMutationApplied, MutationError> {
        mutation.validate()?;
        let identity = BucketIdentity {
            tenant_id: TenantId(mutation.tenant_id),
            bucket_id: BucketId(mutation.bucket_id),
        };
        let encoded_head_key = identity.head_key(&mutation.exact_path);
        let encoded_version_key =
            exact_version_key(identity, &mutation.exact_path, mutation.version.id);
        let primary_receipt_key = receipt_key(identity, &mutation.command_id);
        let _commit_guard = self.commit_lock.lock().await;
        let now = now_unix_millis()?;

        let retained_identical_receipt = if let Some(existing) =
            self.read_json::<StoredReceipt>(CF_RECEIPTS, &primary_receipt_key)?
            && existing.expires_at_unix_millis > now
        {
            if existing.fingerprint != mutation.input_fingerprint
                || existing.version != mutation.version.id
                || existing.deleted != mutation.version.deleted
                || existing.expires_at_unix_millis != mutation.receipt_expires_at_unix_millis
                || existing.object_mutation.as_ref() != Some(mutation)
            {
                return Err(MutationError::ObjectMutationConflict);
            }
            true
        } else {
            false
        };

        let mut batch = WriteBatch::default();
        let proof_staged = self.stage_object_mutation_reference_proof(&mut batch, mutation)?;
        let current = self.head_by_storage_key(&encoded_head_key)?;
        let locator_applied = mutation
            .definition_transition
            .as_ref()
            .map(|transition| self.definition_transition_is_applied(transition))
            .transpose()?
            .unwrap_or(true);
        let mut already_applied = false;
        match current.as_ref() {
            None if mutation.stamp.predecessor_version.is_some() => {
                return Err(MutationError::ObjectMutationLineageGap {
                    current: None,
                    predecessor: mutation.stamp.predecessor_version,
                });
            }
            None => {}
            Some(head) if head.version == mutation.version.id => {
                let descriptor = self
                    .read_json::<Version>(CF_VERSIONS, &encoded_version_key)?
                    .ok_or_else(|| {
                        MutationError::Storage(
                            "replicated head references a missing version descriptor".into(),
                        )
                    })?;
                if head.deleted == mutation.version.deleted
                    && head.mutation_stamp == Some(mutation.stamp)
                    && descriptor == mutation.version
                {
                    if retained_identical_receipt && !proof_staged && locator_applied {
                        return Ok(ReplicaObjectMutationApplied {
                            version: mutation.version.id,
                            replayed: true,
                        });
                    }
                    already_applied = true;
                } else if head.mutation_stamp.is_some_and(|stamp| {
                    stamp.predecessor_version == mutation.stamp.predecessor_version
                }) {
                    return Err(MutationError::ObjectMutationSibling {
                        predecessor: mutation.stamp.predecessor_version,
                    });
                } else {
                    return Err(MutationError::ObjectMutationConflict);
                }
            }
            Some(head) if Some(head.version) == mutation.stamp.predecessor_version => {}
            Some(head)
                if retained_identical_receipt
                    && head.mutation_stamp.is_some_and(|stamp| {
                        stamp.predecessor_version == Some(mutation.version.id)
                    }) =>
            {
                if !proof_staged {
                    return Ok(ReplicaObjectMutationApplied {
                        version: mutation.version.id,
                        replayed: true,
                    });
                }
                already_applied = true;
            }
            Some(head)
                if head.mutation_stamp.is_some_and(|stamp| {
                    stamp.predecessor_version == mutation.stamp.predecessor_version
                }) =>
            {
                return Err(MutationError::ObjectMutationSibling {
                    predecessor: mutation.stamp.predecessor_version,
                });
            }
            Some(head) => {
                return Err(MutationError::ObjectMutationLineageGap {
                    current: Some(head.version),
                    predecessor: mutation.stamp.predecessor_version,
                });
            }
        }

        if let Some(existing) = self.read_json::<Version>(CF_VERSIONS, &encoded_version_key)?
            && existing != mutation.version
        {
            return Err(MutationError::ObjectMutationConflict);
        }

        let mut receipt_status = self.mutation_receipt_status()?;
        let initial_receipt_status = receipt_status;
        let pruned = self.stage_expired_mutation_receipts(&mut batch, now, &mut receipt_status)?;
        if !retained_identical_receipt
            && !pruned.contains(&primary_receipt_key)
            && self
                .read_json::<StoredReceipt>(CF_RECEIPTS, &primary_receipt_key)?
                .is_some()
        {
            return Err(MutationError::ObjectMutationConflict);
        }

        if !already_applied {
            if mutation.retire_predecessor {
                let predecessor = mutation.stamp.predecessor_version.ok_or_else(|| {
                    MutationError::InvalidObjectMutation(
                        "retired predecessor is missing from mutation lineage".into(),
                    )
                })?;
                batch.delete_cf(
                    self.cf(CF_VERSIONS)?,
                    exact_version_key(identity, &mutation.exact_path, predecessor),
                );
            }
            batch.put_cf(
                self.cf(CF_VERSIONS)?,
                &encoded_version_key,
                serde_json::to_vec(&mutation.version).map_err(storage_error)?,
            );
            batch.put_cf(
                self.cf(CF_HEADS)?,
                &encoded_head_key,
                serde_json::to_vec(&Head {
                    version: mutation.version.id,
                    deleted: mutation.version.deleted,
                    mutation_stamp: Some(mutation.stamp),
                })
                .map_err(storage_error)?,
            );
        }
        if !retained_identical_receipt && mutation.receipt_expires_at_unix_millis > now {
            self.stage_stored_mutation_receipt(
                &mut batch,
                primary_receipt_key,
                StoredReceipt {
                    fingerprint: mutation.input_fingerprint,
                    version: mutation.version.id,
                    deleted: mutation.version.deleted,
                    expires_at_unix_millis: mutation.receipt_expires_at_unix_millis,
                    object_mutation: Some(mutation.clone()),
                    definition_transition: mutation.definition_transition.clone(),
                },
                &mut receipt_status,
                &mut BTreeMap::new(),
            )?;
        }
        if receipt_status != initial_receipt_status {
            self.stage_mutation_receipt_status(&mut batch, receipt_status)?;
        }
        let high_watermark = self
            .read_json::<VersionId>(CF_METADATA, VERSION_HIGH_WATERMARK_KEY)?
            .map_or(mutation.version.id, |current| {
                current.max(mutation.version.id)
            });
        batch.put_cf(
            self.cf(CF_METADATA)?,
            VERSION_HIGH_WATERMARK_KEY,
            serde_json::to_vec(&high_watermark).map_err(storage_error)?,
        );
        let mutation_is_current = !already_applied
            || current
                .as_ref()
                .is_some_and(|head| head.version == mutation.version.id);
        if mutation_is_current && let Some(transition) = mutation.definition_transition.as_ref() {
            self.stage_definition_transition(&mut batch, transition)
                .map_err(definition_mutation_error)?;
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
        if !pruned.is_empty() {
            self.mutation_capacity_notify.notify_waiters();
        }
        self.clock.observe(mutation.version.id);
        Ok(ReplicaObjectMutationApplied {
            version: mutation.version.id,
            replayed: already_applied,
        })
    }

    pub(crate) fn stage_local_changes(
        &self,
        batch: &mut WriteBatch,
        changes: &[PendingLocalChange],
        reference_effects: LocalReferenceEffects,
    ) -> Result<(), MutationError> {
        if changes.is_empty() {
            return Ok(());
        }

        let journal = self.cf(CF_LOCAL_INVALIDATIONS)?;
        let metadata = self.cf(CF_METADATA)?;
        let mut status = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let old_tail = status.tail;
        let cursor = self
            .reference_delta_cursor(status.source_id)
            .map_err(|error| {
                MutationError::Storage(format!(
                    "cannot read local reference cursor before source-journal append: {error}"
                ))
            })?;
        if cursor > old_tail {
            return Err(MutationError::Storage(format!(
                "local reference cursor {cursor} is ahead of source-journal tail {old_tail}"
            )));
        }
        let local_reference_cursor = match reference_effects {
            LocalReferenceEffects::AppliedInline => {
                if cursor != old_tail {
                    return Err(MutationError::Storage(format!(
                        "local reference cursor {cursor} does not match source-journal tail {old_tail}"
                    )));
                }
                Some(status.source_id)
            }
            LocalReferenceEffects::NoReferenceEffects => {
                if changes
                    .iter()
                    .any(PendingLocalChange::has_reference_effects)
                {
                    return Err(MutationError::Storage(
                        "source-journal append declared no reference effects but carried a reference delta"
                            .into(),
                    ));
                }
                (cursor == old_tail).then_some(status.source_id)
            }
            LocalReferenceEffects::Deferred => None,
        };
        let reference_safe_through = self
            .source_journal_reference_safe_through
            .load(std::sync::atomic::Ordering::Acquire);
        let (index_safe_through, accounting_safe_through) = self
            .derived_consumer_safe_through(status)
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let safe_through = reference_safe_through
            .min(index_safe_through)
            .min(accounting_safe_through);
        let mut appended = VecDeque::new();
        for pending in changes {
            status.tail = status.tail.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation offset is exhausted".into())
            })?;
            let change = match pending {
                PendingLocalChange::ObjectHead {
                    identity,
                    exact_path,
                    path_version,
                    deleted,
                    reference_deltas,
                    accounting_transition,
                    definition_transition,
                } => LocalChange::object_head(
                    status.tail,
                    identity.tenant_id.0,
                    identity.bucket_id.0,
                    exact_path.clone(),
                    *path_version,
                    *deleted,
                    reference_deltas.clone(),
                    *accounting_transition,
                    definition_transition.clone(),
                ),
                PendingLocalChange::RetainedVersionDeleted {
                    identity,
                    exact_path,
                    deleted_version,
                    resulting_head_version,
                    reference_deltas,
                    accounting_transition,
                } => LocalChange::retained_version_deleted(
                    status.tail,
                    identity.tenant_id.0,
                    identity.bucket_id.0,
                    exact_path.clone(),
                    *deleted_version,
                    *resulting_head_version,
                    reference_deltas.clone(),
                    *accounting_transition,
                ),
                PendingLocalChange::AggregateChanged {
                    aggregate_kind,
                    aggregate_key,
                    revision,
                } => LocalChange::aggregate_changed(
                    status.tail,
                    *aggregate_kind,
                    aggregate_key.clone(),
                    *revision,
                ),
                PendingLocalChange::ContentLifecycleChanged {
                    blob_identity,
                    revision,
                    reference_deltas,
                } => LocalChange::content_lifecycle_changed(
                    status.tail,
                    blob_identity.clone(),
                    *revision,
                    reference_deltas.clone(),
                ),
            };
            let encoded = encode_local_change(&change).map_err(storage_error)?;
            let logical_bytes = invalidation_record_bytes(encoded.len())
                .saturating_add(super::journal_routes::journal_route_logical_bytes(&change));
            if logical_bytes > self.watch_retention.max_bytes {
                return Err(MutationError::SourceJournalRecordTooLarge {
                    bytes: logical_bytes,
                    maximum: self.watch_retention.max_bytes,
                });
            }
            self.stage_journal_routes(batch, status.source_id.source_epoch, &change)?;
            status.retained_entries = status.retained_entries.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation entry count is exhausted".into())
            })?;
            status.retained_bytes = status
                .retained_bytes
                .checked_add(logical_bytes)
                .ok_or_else(|| {
                    MutationError::Storage("local invalidation byte count is exhausted".into())
                })?;
            appended.push_back((status.tail, encoded));
        }

        let exceeds_retention = status.retained_entries > self.watch_retention.max_entries
            || status.retained_bytes > self.watch_retention.max_bytes;
        let first_old_key = invalidation_key(status.retention_floor.saturating_add(1));
        let mut old_entries = exceeds_retention.then(|| {
            self.db.iterator_cf(
                journal,
                IteratorMode::From(&first_old_key, Direction::Forward),
            )
        });
        while status.retained_entries > self.watch_retention.max_entries
            || status.retained_bytes > self.watch_retention.max_bytes
        {
            let pruned = status.retention_floor.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation retention floor is exhausted".into())
            })?;
            if pruned > old_tail || pruned > safe_through || pruned > status.settled_through {
                return Err(MutationError::SourceJournalCapacity);
            }
            let (stored_key, encoded) = old_entries
                .as_mut()
                .expect("retention iterator exists while the journal is over capacity")
                .next()
                .ok_or_else(|| {
                    MutationError::Storage(format!(
                        "retained local invalidation offset {pruned} is missing"
                    ))
                })?
                .map_err(storage_error)?;
            if offset_from_key(&stored_key) != Some(pruned) {
                return Err(MutationError::Storage(format!(
                    "retained local invalidation offset {pruned} is missing"
                )));
            }
            let pruned_change = self.decode_local_change_record(&encoded)?;
            if pruned_change.offset() != pruned {
                return Err(MutationError::Storage(
                    "local change key does not match its stored offset".into(),
                ));
            }
            self.stage_journal_route_removal(batch, status.source_id.source_epoch, &pruned_change)?;
            batch.delete_cf(journal, invalidation_key(pruned));
            status.retention_floor = pruned;
            status.retained_entries -= 1;
            status.retained_bytes = status
                .retained_bytes
                .checked_sub(invalidation_record_bytes(encoded.len()).saturating_add(
                    super::journal_routes::journal_route_logical_bytes(&pruned_change),
                ))
                .ok_or_else(|| {
                    MutationError::Storage(
                        "local invalidation byte accounting is inconsistent".into(),
                    )
                })?;
        }
        for (offset, encoded) in appended {
            batch.put_cf(journal, invalidation_key(offset), encoded);
        }
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_OFFSET_KEY,
            status.tail.to_be_bytes(),
        );
        if reference_effects != LocalReferenceEffects::Deferred
            && status.settled_through == old_tail
        {
            batch.put_cf(
                metadata,
                LOCAL_INVALIDATION_SETTLED_KEY,
                status.tail.to_be_bytes(),
            );
        }
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_FLOOR_KEY,
            status.retention_floor.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_COUNT_KEY,
            status.retained_entries.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_BYTES_KEY,
            status.retained_bytes.to_be_bytes(),
        );
        if let Some(source) = local_reference_cursor {
            self.stage_reference_delta_cursor(batch, source, status.tail)?;
        }
        Ok(())
    }

    pub(crate) fn notify_local_invalidations(&self) {
        self.watch_notify.send_replace(());
    }

    pub(super) fn enforce_local_watch_retention(&self) -> Result<(), WatchError> {
        let journal = self
            .cf(CF_LOCAL_INVALIDATIONS)
            .map_err(|error| WatchError::Storage(error.to_string()))?;
        let metadata = self
            .cf(CF_METADATA)
            .map_err(|error| WatchError::Storage(error.to_string()))?;
        let mut status = self.local_watch_status()?;
        if status.retained_entries <= self.watch_retention.max_entries
            && status.retained_bytes <= self.watch_retention.max_bytes
        {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        let reference_safe_through = self
            .source_journal_reference_safe_through
            .load(std::sync::atomic::Ordering::Acquire)
            .min(status.settled_through);
        let (index_safe_through, accounting_safe_through) = self
            .derived_consumer_safe_through(status)
            .map_err(|error| WatchError::Storage(error.to_string()))?;
        let safe_through = reference_safe_through
            .min(index_safe_through)
            .min(accounting_safe_through);
        while (status.retained_entries > self.watch_retention.max_entries
            || status.retained_bytes > self.watch_retention.max_bytes)
            && status.retention_floor < safe_through
        {
            let offset = status.retention_floor.checked_add(1).ok_or_else(|| {
                WatchError::Storage("local invalidation retention floor is exhausted".into())
            })?;
            let encoded = self
                .db
                .get_cf(journal, invalidation_key(offset))
                .map_err(|error| WatchError::Storage(error.to_string()))?
                .ok_or_else(|| {
                    WatchError::Storage(format!(
                        "retained local invalidation offset {offset} is missing"
                    ))
                })?;
            let pruned_change = self
                .decode_local_change_record(&encoded)
                .map_err(|error| WatchError::Storage(error.to_string()))?;
            if pruned_change.offset() != offset {
                return Err(WatchError::Storage(
                    "local change key does not match its stored offset".into(),
                ));
            }
            self.stage_journal_route_removal(
                &mut batch,
                status.source_id.source_epoch,
                &pruned_change,
            )
            .map_err(|error| WatchError::Storage(error.to_string()))?;
            batch.delete_cf(journal, invalidation_key(offset));
            status.retention_floor = offset;
            status.retained_entries -= 1;
            status.retained_bytes = status
                .retained_bytes
                .checked_sub(invalidation_record_bytes(encoded.len()).saturating_add(
                    super::journal_routes::journal_route_logical_bytes(&pruned_change),
                ))
                .ok_or_else(|| {
                    WatchError::Storage("local invalidation byte accounting is inconsistent".into())
                })?;
        }
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_FLOOR_KEY,
            status.retention_floor.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_COUNT_KEY,
            status.retained_entries.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_BYTES_KEY,
            status.retained_bytes.to_be_bytes(),
        );
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .write_opt(batch, &options)
            .map_err(|error| WatchError::Storage(error.to_string()))
    }

    pub(super) async fn prepare(
        &self,
        operation: BatchOperation,
        identity: BucketIdentity,
        distributed_coordination: bool,
    ) -> Result<PreparedOperation, MutationError> {
        match operation {
            BatchOperation::Put(mut request) => {
                validate_command_id(request.command_id.as_deref())?;
                if !distributed_coordination {
                    require_local_durability(request.durability)?;
                }
                let bytes = std::mem::take(&mut request.bytes);
                let payload = if distributed_coordination {
                    PreparedPayload::Sealed(self.stage_blob(&bytes).await?)
                } else if bytes.len() <= SMALL_BLOB_MAX_BYTES {
                    let reference = blob_reference_for_bytes(&bytes);
                    PreparedPayload::Small { reference, bytes }
                } else {
                    PreparedPayload::Large(self.blobs.put(&bytes).await.map_err(storage_error)?)
                };
                let fingerprint = put_fingerprint(
                    &identity.head_key(request.key.path()),
                    request.mode,
                    request.content_type.as_deref(),
                    request.durability,
                    payload.reference(),
                );
                Ok(PreparedOperation::Put {
                    request,
                    identity,
                    payload,
                    fingerprint,
                })
            }
            BatchOperation::Publish(request) => {
                validate_command_id(request.command_id.as_deref())?;
                if !distributed_coordination {
                    require_local_durability(request.durability)?;
                }
                if !self.contains_blob(&request.blob).await? {
                    return Err(MutationError::BlobNotFound);
                }
                let fingerprint = publish_fingerprint(&request, identity);
                Ok(PreparedOperation::Publish {
                    request,
                    identity,
                    fingerprint,
                })
            }
            BatchOperation::Delete(request) => {
                validate_command_id(request.command_id.as_deref())?;
                if !distributed_coordination {
                    require_local_durability(request.durability)?;
                }
                let fingerprint = delete_fingerprint(&request, identity);
                Ok(PreparedOperation::Delete {
                    request,
                    identity,
                    fingerprint,
                })
            }
        }
    }

    pub(super) fn mutation_receipt_status(&self) -> Result<MutationReceiptStatus, MutationError> {
        let metadata = self.cf(CF_METADATA)?;
        let read = |key: &[u8]| {
            self.db
                .get_cf(metadata, key)
                .map_err(storage_error)?
                .ok_or_else(|| {
                    MutationError::Storage("mutation receipt metadata is missing".into())
                })
                .and_then(|encoded| decode_offset(&encoded))
        };
        Ok(MutationReceiptStatus {
            entries: read(MUTATION_RECEIPT_COUNT_KEY)?,
            bytes: read(MUTATION_RECEIPT_BYTES_KEY)?,
        })
    }

    pub(super) fn stage_expired_mutation_receipts(
        &self,
        batch: &mut WriteBatch,
        now_unix_millis: u64,
        status: &mut MutationReceiptStatus,
    ) -> Result<BTreeSet<Vec<u8>>, MutationError> {
        if status.entries == 0 {
            return Ok(BTreeSet::new());
        }
        let receipts = self.cf(CF_RECEIPTS)?;
        let mut pruned = BTreeSet::new();
        let mut pruned_bytes = 0_u64;
        let iterator = self.db.iterator_cf(
            receipts,
            IteratorMode::From(
                &[STORAGE_KEY_FORMAT_VERSION, RECEIPT_EXPIRY_PREFIX],
                Direction::Forward,
            ),
        );
        for entry in iterator {
            let (index_key, _) = entry.map_err(storage_error)?;
            let Some((expires_at, primary_key)) = parse_receipt_expiry_key(&index_key)? else {
                break;
            };
            if expires_at > now_unix_millis {
                break;
            }
            if pruned.contains(&primary_key) {
                return Err(MutationError::Storage(
                    "mutation receipt has duplicate expiry indexes".into(),
                ));
            }
            let encoded = self
                .db
                .get_cf(receipts, &primary_key)
                .map_err(storage_error)?
                .ok_or_else(|| {
                    MutationError::Storage(
                        "mutation receipt expiry index references a missing receipt".into(),
                    )
                })?;
            let receipt =
                serde_json::from_slice::<StoredReceipt>(&encoded).map_err(storage_error)?;
            if receipt.expires_at_unix_millis != expires_at {
                return Err(MutationError::Storage(
                    "mutation receipt expiry index disagrees with its receipt".into(),
                ));
            }
            let logical_bytes =
                mutation_receipt_logical_bytes(primary_key.len(), encoded.len(), index_key.len());
            if pruned.len() >= MAX_EXPIRED_RECEIPTS_PRUNED_PER_PASS
                || (!pruned.is_empty()
                    && pruned_bytes.saturating_add(logical_bytes)
                        > MAX_EXPIRED_RECEIPT_BYTES_PRUNED_PER_PASS)
            {
                break;
            }
            status.entries = status.entries.checked_sub(1).ok_or_else(|| {
                MutationError::Storage("mutation receipt count is inconsistent".into())
            })?;
            status.bytes = status.bytes.checked_sub(logical_bytes).ok_or_else(|| {
                MutationError::Storage("mutation receipt byte accounting is inconsistent".into())
            })?;
            batch.delete_cf(receipts, &primary_key);
            batch.delete_cf(receipts, &index_key);
            pruned.insert(primary_key);
            pruned_bytes = pruned_bytes.saturating_add(logical_bytes);
        }
        Ok(pruned)
    }

    /// Persist one bounded receipt-expiry maintenance pass while a writer is
    /// waiting for capacity. Keeping this separate from the rejected mutation
    /// guarantees progress even when that mutation's WriteBatch must be
    /// discarded atomically.
    pub(super) async fn prune_expired_receipts_for_capacity(&self) -> Result<bool, MutationError> {
        let _commit_guard = self.commit_lock.lock().await;
        let now = now_unix_millis()?;
        let mut status = self.mutation_receipt_status()?;
        let initial = status;
        let mut batch = WriteBatch::default();
        let pruned = self.stage_expired_mutation_receipts(&mut batch, now, &mut status)?;
        if pruned.is_empty() {
            return Ok(false);
        }
        if status != initial {
            self.stage_mutation_receipt_status(&mut batch, status)?;
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
        self.mutation_capacity_notify.notify_waiters();
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn stage_mutation_receipt(
        &self,
        batch: &mut WriteBatch,
        primary_key: Option<Vec<u8>>,
        fingerprint: [u8; 32],
        version: VersionId,
        deleted: bool,
        object_mutation: Option<ObjectMutation>,
        definition_transition: Option<DefinitionTransition>,
        now_unix_millis: u64,
        status: &mut MutationReceiptStatus,
        pending_receipts: &mut BTreeMap<Vec<u8>, StoredReceipt>,
    ) -> Result<u64, MutationError> {
        let Some(primary_key) = primary_key else {
            return Ok(0);
        };
        let expires_at_unix_millis = now_unix_millis
            .checked_add(self.mutation_receipt_retention.retention_millis())
            .ok_or_else(|| MutationError::Storage("mutation receipt expiry overflow".into()))?;
        let stored = StoredReceipt {
            fingerprint,
            version,
            deleted,
            expires_at_unix_millis,
            object_mutation,
            definition_transition,
        };
        self.stage_stored_mutation_receipt(batch, primary_key, stored, status, pending_receipts)?;
        Ok(expires_at_unix_millis)
    }

    pub(super) fn stage_stored_mutation_receipt(
        &self,
        batch: &mut WriteBatch,
        primary_key: Vec<u8>,
        stored: StoredReceipt,
        status: &mut MutationReceiptStatus,
        pending_receipts: &mut BTreeMap<Vec<u8>, StoredReceipt>,
    ) -> Result<(), MutationError> {
        let encoded = serde_json::to_vec(&stored).map_err(storage_error)?;
        let expiry_key = receipt_expiry_key(stored.expires_at_unix_millis, &primary_key)?;
        let logical_bytes =
            mutation_receipt_logical_bytes(primary_key.len(), encoded.len(), expiry_key.len());
        if logical_bytes > self.mutation_receipt_retention.max_bytes {
            return Err(MutationError::ReceiptTooLarge {
                bytes: logical_bytes,
                maximum: self.mutation_receipt_retention.max_bytes,
            });
        }
        let next_entries = status
            .entries
            .checked_add(1)
            .ok_or_else(|| MutationError::Storage("mutation receipt count is exhausted".into()))?;
        let next_bytes = status.bytes.checked_add(logical_bytes).ok_or_else(|| {
            MutationError::Storage("mutation receipt byte accounting is exhausted".into())
        })?;
        if next_entries > self.mutation_receipt_retention.max_entries
            || next_bytes > self.mutation_receipt_retention.max_bytes
        {
            return Err(MutationError::ReceiptCapacity);
        }
        batch.put_cf(self.cf(CF_RECEIPTS)?, &primary_key, encoded);
        batch.put_cf(self.cf(CF_RECEIPTS)?, expiry_key, []);
        pending_receipts.insert(primary_key, stored);
        status.entries = next_entries;
        status.bytes = next_bytes;
        Ok(())
    }

    pub(super) fn stage_mutation_receipt_status(
        &self,
        batch: &mut WriteBatch,
        status: MutationReceiptStatus,
    ) -> Result<(), MutationError> {
        let metadata = self.cf(CF_METADATA)?;
        batch.put_cf(
            metadata,
            MUTATION_RECEIPT_COUNT_KEY,
            status.entries.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            MUTATION_RECEIPT_BYTES_KEY,
            status.bytes.to_be_bytes(),
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn evaluate_operation(
        &self,
        operation: &PreparedOperation,
        batch: &mut WriteBatch,
        pending_heads: &mut BTreeMap<Vec<u8>, Head>,
        pending_versions: &mut BTreeMap<Vec<u8>, Version>,
        pending_receipts: &mut BTreeMap<Vec<u8>, StoredReceipt>,
        pending_blob_references: &mut PendingBlobReferences,
        pending_small_blobs: &mut BTreeSet<Vec<u8>>,
        policy_cache: &mut BTreeMap<Vec<u8>, Result<BucketPolicy, MutationError>>,
        versioning_cache: &mut BTreeMap<Vec<u8>, Result<ObjectVersioning, MutationError>>,
        pruned_receipts: &BTreeSet<Vec<u8>>,
        receipt_status: &mut MutationReceiptStatus,
        now_unix_millis: u64,
        distributed: Option<DistributedEvaluationContext>,
        definition_intent: Option<DefinitionMutationIntent>,
    ) -> Result<EvaluatedOperation, MutationError> {
        let key = operation.key();
        let encoded_key = operation.encoded_head_key();
        let receipt_key = operation
            .command_id()
            .map(|command_id| receipt_key(operation.identity(), command_id));
        if let Some(receipt_key) = receipt_key.as_ref() {
            let existing = match pending_receipts.get(receipt_key) {
                Some(receipt) => Some(receipt.clone()),
                None if pruned_receipts.contains(receipt_key) => None,
                None => self.read_json(CF_RECEIPTS, receipt_key)?,
            };
            if let Some(existing) = existing {
                if existing.expires_at_unix_millis <= now_unix_millis {
                    return Err(MutationError::Storage(
                        "expired mutation receipt escaped pruning".into(),
                    ));
                }
                if existing.fingerprint != operation.fingerprint() {
                    return Err(MutationError::IdempotencyConflict);
                }
                if !definition_receipt_matches_intent(
                    existing.definition_transition.as_ref(),
                    definition_intent,
                    operation,
                ) {
                    return Err(MutationError::IdempotencyConflict);
                }
                return Ok(EvaluatedOperation {
                    receipt: MutationReceipt {
                        command_id: operation.command_id().map(str::to_owned),
                        fingerprint: existing.fingerprint,
                        version: existing.version,
                        deleted: existing.deleted,
                        replayed: true,
                        replay_guarantee_expires_at_unix_millis: existing.expires_at_unix_millis,
                    },
                    mutation: existing.object_mutation,
                    reference_deltas: Vec::new(),
                    accounting_transition: None,
                    definition_transition: existing.definition_transition,
                });
            }
        }

        let current = match pending_heads.get(&encoded_key) {
            Some(head) => Some(head.clone()),
            None => self.head_by_storage_key(&encoded_key)?,
        };
        let current_version = match current.as_ref() {
            Some(head) => match pending_versions.get(&encoded_key) {
                Some(version) => Some(version.clone()),
                None => Some(
                    self.version_metadata_by_identity(operation.identity(), key, head.version)?
                        .ok_or_else(|| {
                            MutationError::Storage("head references a missing version".into())
                        })?,
                ),
            },
            None => None,
        };
        if current_version
            .as_ref()
            .zip(current.as_ref())
            .is_some_and(|(version, head)| {
                version.id != head.version || version.deleted != head.deleted
            })
        {
            return Err(MutationError::Storage(
                "head and current version descriptor disagree".into(),
            ));
        }
        let encoded_bucket = operation.identity().encode().to_vec();
        let policy = policy_cache
            .entry(encoded_bucket.clone())
            .or_insert_with(|| {
                self.bucket_policy_by_key(&encoded_bucket)
                    .map(Option::unwrap_or_default)
            })
            .as_ref()
            .map_err(Clone::clone)?;
        let versioning = *versioning_cache
            .entry(encoded_bucket)
            .or_insert_with(|| self.bucket_versioning_by_key(&operation.identity().encode()))
            .as_ref()
            .map_err(Clone::clone)?;
        let program_definition = is_program_definition_path(key.path());
        if policy.is_program_only(key.path()) && !program_definition {
            return Err(MutationError::ProgramConcurrencyViolation);
        }
        let immutable_path = policy.is_immutable(key.path()) || program_definition;
        match operation.put_mode() {
            Some(PutMode::PutImmutable) if !immutable_path => {
                return Err(MutationError::ImmutablePolicyRequired);
            }
            Some(PutMode::PutImmutable) => {
                // Handled below: publish once or return an identical-content
                // semantic replay without advancing the path version.
            }
            Some(_) | None if immutable_path => {
                return Err(MutationError::Immutable);
            }
            Some(_) | None => {}
        }
        if matches!(operation.put_mode(), Some(PutMode::PutImmutable))
            && let Some(current) = current.as_ref()
        {
            let existing = current_version.as_ref().ok_or_else(|| {
                MutationError::Storage("head references a missing version".into())
            })?;
            let requested_payload = match operation {
                PreparedOperation::Put { payload, .. } => payload.reference().clone(),
                PreparedOperation::Publish { request, .. } => request.blob.clone(),
                PreparedOperation::Delete { .. } => unreachable!(),
            };
            let requested_content_type = match operation {
                PreparedOperation::Put { request, .. } => request.content_type.as_ref(),
                PreparedOperation::Publish { request, .. } => request.content_type.as_ref(),
                PreparedOperation::Delete { .. } => unreachable!(),
            };
            if !current.deleted
                && version_blob_reference(existing)?.as_ref() == Some(&requested_payload)
                && existing.content_type.as_ref() == requested_content_type
            {
                let fingerprint = operation.fingerprint();
                let definition_transition = definition_intent.map(|intent| DefinitionTransition {
                    kind: intent.kind,
                    tenant_id: operation.identity().tenant_id.0,
                    bucket_id: operation.identity().bucket_id.0,
                    definition_id: intent.definition_id,
                    path: key.path().to_owned(),
                    object_version: current.version,
                    operation: DefinitionOperation::Upsert,
                });
                let expires_at = self.stage_mutation_receipt(
                    batch,
                    receipt_key,
                    fingerprint,
                    current.version,
                    false,
                    None,
                    definition_transition.clone(),
                    now_unix_millis,
                    receipt_status,
                    pending_receipts,
                )?;
                if let Some(transition) = definition_transition.as_ref() {
                    self.stage_definition_transition(batch, transition)
                        .map_err(definition_mutation_error)?;
                }
                return Ok(EvaluatedOperation {
                    receipt: MutationReceipt {
                        command_id: operation.command_id().map(str::to_owned),
                        fingerprint,
                        version: current.version,
                        deleted: false,
                        replayed: true,
                        replay_guarantee_expires_at_unix_millis: expires_at,
                    },
                    mutation: None,
                    reference_deltas: Vec::new(),
                    accounting_transition: None,
                    definition_transition,
                });
            }
            return Err(MutationError::Immutable);
        }
        check_precondition(operation.precondition(), current.as_ref())?;

        let id = self.clock.next().map_err(storage_error)?;
        let deleted = matches!(operation, PreparedOperation::Delete { .. });
        let new_blob = match operation {
            PreparedOperation::Put { payload, .. } => Some(payload.reference().clone()),
            PreparedOperation::Publish { request, .. } => Some(request.blob.clone()),
            PreparedOperation::Delete { .. } => None,
        };
        if let PreparedOperation::Put { payload, .. } = operation
            && payload.small_bytes().is_none()
            && !self.contains_blob(payload.reference()).await?
        {
            return Err(MutationError::BlobNotFound);
        }
        let version = Version {
            id,
            blob: new_blob.clone(),
            content_type: match operation {
                PreparedOperation::Put { request, .. } => request.content_type.clone(),
                PreparedOperation::Publish { request, .. } => request.content_type.clone(),
                PreparedOperation::Delete { .. } => None,
            },
            deleted,
            committed_at_unix_millis: now_unix_millis,
        };
        let accounting_transition = AccountingHeadTransition::new(
            current_version.as_ref().and_then(live_version_length),
            live_version_length(&version),
        );
        let definition_transition = definition_intent.map(|intent| DefinitionTransition {
            kind: intent.kind,
            tenant_id: operation.identity().tenant_id.0,
            bucket_id: operation.identity().bucket_id.0,
            definition_id: intent.definition_id,
            path: key.path().to_owned(),
            object_version: id,
            operation: if deleted {
                DefinitionOperation::Delete
            } else {
                DefinitionOperation::Upsert
            },
        });
        if let Some(transition) = definition_transition.as_ref() {
            transition.validate().map_err(definition_mutation_error)?;
        }
        let fingerprint = operation.fingerprint();
        let apply_content_lifecycle = distributed.is_none();
        let old_blob = current_version
            .as_ref()
            .map(version_blob_reference)
            .transpose()?
            .flatten();
        let references_changed = old_blob.as_ref() != new_blob.as_ref();
        let mut reference_deltas = Vec::with_capacity(2);
        if versioning == ObjectVersioning::Unversioned
            && references_changed
            && let Some(reference) = old_blob.as_ref()
        {
            reference_deltas.push(ReferenceDelta {
                blob: reference.clone(),
                change: -1,
            });
        }
        if let Some(reference) = new_blob.as_ref()
            && (versioning == ObjectVersioning::Enabled || references_changed)
        {
            reference_deltas.push(ReferenceDelta {
                blob: reference.clone(),
                change: 1,
            });
        }
        let receipt_expires_at_unix_millis = if receipt_key.is_some() {
            now_unix_millis
                .checked_add(self.mutation_receipt_retention.retention_millis())
                .ok_or_else(|| MutationError::Storage("mutation receipt expiry overflow".into()))?
        } else {
            0
        };
        let object_mutation = distributed
            .map(|distributed| {
                let command_id = operation
                    .command_id()
                    .ok_or(MutationError::InvalidCommandId)?;
                let mut mutation = ObjectMutation {
                    format: OBJECT_MUTATION_FORMAT,
                    tenant_id: operation.identity().tenant_id.0,
                    bucket_id: operation.identity().bucket_id.0,
                    exact_path: key.path().to_owned(),
                    command_id: command_id.to_owned(),
                    input_fingerprint: fingerprint,
                    version: version.clone(),
                    retire_predecessor: versioning == ObjectVersioning::Unversioned
                        && current.is_some(),
                    receipt_expires_at_unix_millis,
                    stamp: MutationStamp {
                        format: MUTATION_STAMP_FORMAT,
                        predecessor_version: current.as_ref().map(|head| head.version),
                        program_commit_cursor: None,
                        mutation_fingerprint: [0; 32],
                        active_placement_log_id: distributed.mutation.active_placement_log_id,
                        serving_fence_term: distributed.mutation.serving_fence_term,
                        source_id: distributed.source_id,
                        source_journal_position: distributed.source_journal_position,
                    },
                    reference_deltas: reference_deltas.clone(),
                    accounting_transition: Some(accounting_transition),
                    definition_transition: definition_transition.clone(),
                };
                mutation.set_computed_fingerprint();
                mutation.validate()?;
                Ok(mutation)
            })
            .transpose()?;
        let head = Head {
            version: id,
            deleted,
            mutation_stamp: object_mutation.as_ref().map(|mutation| mutation.stamp),
        };
        let encoded_version = serde_json::to_vec(&version).map_err(storage_error)?;
        let encoded_head = serde_json::to_vec(&head).map_err(storage_error)?;
        let versions = self.cf(CF_VERSIONS)?;
        let heads = self.cf(CF_HEADS)?;
        let encoded_version_key = version_key(operation.identity(), key, id);
        let mut blob_reference_updates = Vec::with_capacity(2);
        if apply_content_lifecycle
            && versioning == ObjectVersioning::Unversioned
            && references_changed
            && let Some(reference) = old_blob.as_ref()
        {
            blob_reference_updates.push(self.prepare_blob_reference_retirement(
                reference,
                pending_blob_references,
                now_unix_millis,
            )?);
        }
        let small_blob_value = if apply_content_lifecycle {
            match operation {
                PreparedOperation::Put { payload, .. } => match payload.small_bytes() {
                    Some(bytes) => self.prepare_hashed_small_blob_value(
                        payload.reference(),
                        bytes,
                        pending_small_blobs,
                    )?,
                    None => None,
                },
                PreparedOperation::Publish { .. } | PreparedOperation::Delete { .. } => None,
            }
        } else {
            None
        };
        if apply_content_lifecycle
            && let Some(reference) = new_blob.as_ref()
            && (versioning == ObjectVersioning::Enabled || references_changed)
        {
            let update = match operation {
                PreparedOperation::Put { .. } => self.prepare_materialized_blob_publication(
                    reference,
                    pending_blob_references,
                    now_unix_millis,
                )?,
                PreparedOperation::Publish { .. } => self.prepare_blob_reference_publication(
                    reference,
                    pending_blob_references,
                    now_unix_millis,
                )?,
                PreparedOperation::Delete { .. } => unreachable!(),
            };
            blob_reference_updates.push(update);
        }
        let expires_at = self.stage_mutation_receipt(
            batch,
            receipt_key,
            fingerprint,
            id,
            deleted,
            object_mutation.clone(),
            definition_transition.clone(),
            now_unix_millis,
            receipt_status,
            pending_receipts,
        )?;
        if let Some((key, bytes)) = small_blob_value {
            batch.put_cf(self.cf(CF_SMALL_BLOBS)?, &key, bytes);
            pending_small_blobs.insert(key);
        }
        for (key, state) in blob_reference_updates {
            self.stage_blob_reference_update(batch, pending_blob_references, key, state)?;
        }
        if versioning == ObjectVersioning::Unversioned
            && let Some(previous) = current_version.as_ref()
        {
            batch.delete_cf(
                versions,
                version_key(operation.identity(), key, previous.id),
            );
        }
        batch.put_cf(versions, encoded_version_key, encoded_version);
        batch.put_cf(heads, &encoded_key, encoded_head);
        if let Some(transition) = definition_transition.as_ref() {
            self.stage_definition_transition(batch, transition)
                .map_err(definition_mutation_error)?;
        }
        pending_heads.insert(encoded_key.clone(), head);
        pending_versions.insert(encoded_key, version);
        Ok(EvaluatedOperation {
            receipt: MutationReceipt {
                command_id: operation.command_id().map(str::to_owned),
                fingerprint,
                version: id,
                deleted,
                replayed: false,
                replay_guarantee_expires_at_unix_millis: expires_at,
            },
            mutation: object_mutation,
            reference_deltas,
            accounting_transition: Some(accounting_transition),
            definition_transition,
        })
    }
}

fn live_version_length(version: &Version) -> Option<u64> {
    (!version.deleted)
        .then(|| version.blob.as_ref().map(|blob| blob.length))
        .flatten()
}

fn definition_receipt_matches_intent(
    stored: Option<&DefinitionTransition>,
    intent: Option<DefinitionMutationIntent>,
    operation: &PreparedOperation,
) -> bool {
    match (stored, intent) {
        (None, None) => true,
        (Some(stored), Some(intent)) => {
            stored.kind == intent.kind
                && stored.definition_id == intent.definition_id
                && stored.tenant_id == operation.identity().tenant_id.0
                && stored.bucket_id == operation.identity().bucket_id.0
                && stored.path == operation.key().path()
        }
        (Some(_), None) | (None, Some(_)) => false,
    }
}

fn definition_mutation_error(error: DefinitionStateError) -> MutationError {
    MutationError::InvalidObjectMutation(error.to_string())
}

fn exact_version_key(identity: BucketIdentity, exact_path: &str, version: VersionId) -> Vec<u8> {
    let mut encoded = identity.head_key(exact_path);
    encoded.push(0);
    encoded.extend_from_slice(&version.0.to_be_bytes());
    encoded
}
