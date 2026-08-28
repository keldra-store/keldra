use super::journal_capacity::SourceJournalAdmission;
use super::mutation_helpers::{
    definition_mutation_error, definition_receipt_matches_intent, exact_version_key,
    fail_unresolved_prepared, is_mutation_capacity, live_version_length, mutation_capacity_kind,
};
use super::mutation_prefetch::MutationReadCache;
use super::mutation_types::{DistributedEvaluationContext, EvaluatedOperation};
use super::*;
use crate::model::{
    CoordinatedObjectMutation, MUTATION_STAMP_FORMAT, MutationStamp, OBJECT_MUTATION_FORMAT,
    ObjectMutation, ObjectMutationContext, ObjectMutationGovernance, ReplicaObjectMutationApplied,
};
use crate::{DefinitionMutationIntent, DefinitionOperation, DefinitionTransition};

const MAX_EXPIRED_RECEIPTS_PRUNED_PER_PASS: usize = 1_024;
const MAX_EXPIRED_RECEIPT_BYTES_PRUNED_PER_PASS: u64 = 4 * 1024 * 1024;

impl Store {
    /// Preserve the one-node physical WriteBatch path while evaluating the
    /// coordinator-reconciled bucket options supplied by the cluster layer.
    pub async fn mutate_with_governance(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
    ) -> Result<MutationReceipt, MutationError> {
        governance.validate()?;
        self.bulk_write_inner(
            vec![operation],
            Some(governance),
            None,
            false,
            SourceJournalAdmission::Bounded,
        )
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
        self.bulk_write_inner(
            vec![operation],
            Some(governance),
            None,
            true,
            SourceJournalAdmission::Bounded,
        )
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
        self.bulk_write_inner(
            vec![operation],
            Some(governance),
            Some(intent),
            false,
            SourceJournalAdmission::Bounded,
        )
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
        self.bulk_write_inner(
            vec![operation],
            Some(governance),
            Some(intent),
            true,
            SourceJournalAdmission::Bounded,
        )
        .await
        .pop()
        .expect("one operation has one outcome")
        .result
    }

    /// Evaluates independent operations in request order and persists all
    /// successful outcomes with one physical RocksDB write. A failed
    /// precondition is an item result, not a reason to retry the whole bulk.
    pub async fn bulk_write(&self, operations: Vec<BatchOperation>) -> Vec<BatchOutcome> {
        self.bulk_write_inner(
            operations,
            None,
            None,
            false,
            SourceJournalAdmission::Bounded,
        )
        .await
    }

    /// Applies one coordinator batch with capacity backpressure while retaining
    /// the original payloads and command replay contract across retries.
    pub async fn bulk_write_with_backpressure(
        &self,
        operations: Vec<BatchOperation>,
    ) -> Vec<BatchOutcome> {
        self.bulk_write_inner(
            operations,
            None,
            None,
            true,
            SourceJournalAdmission::Bounded,
        )
        .await
    }

    pub(super) async fn bulk_write_inner(
        &self,
        operations: Vec<BatchOperation>,
        governance: Option<ObjectMutationGovernance>,
        definition_intent: Option<DefinitionMutationIntent>,
        backpressure: bool,
        source_journal_admission: SourceJournalAdmission,
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
                BatchOperation::Clone(request) => &request.destination,
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
            histogram.keldra_store_bulk_prepare_duration_seconds = prepare_duration.as_secs_f64(),
            operation_count = prepared.len(),
            "object storage bulk preparation completed"
        );
        let mut completed = BTreeMap::<usize, Result<MutationReceipt, MutationError>>::new();
        loop {
            let _policy_guard = self.policy_gate.read().await;
            let lock_started = std::time::Instant::now();
            let path_lock_started = lock_started;
            let _guards = self
                .ordinary_locks
                .acquire(
                    &prepared
                        .iter()
                        .flat_map(|(_, operation)| operation.lock_paths())
                        .collect::<Vec<_>>(),
                )
                .await;
            let path_lock_wait = path_lock_started.elapsed();
            let _commit_guard = self.lock_commit("bulk_mutation").await;
            let commit_lock_wait = _commit_guard.wait_duration();
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
            let read_cache = match MutationReadCache::load(
                self,
                &prepared
                    .iter()
                    .map(|(_, operation)| operation)
                    .collect::<Vec<_>>(),
            ) {
                Ok(read_cache) => read_cache,
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
            read_cache.seed_bucket_settings(&mut policy_cache, &mut versioning_cache);
            let mut results = BTreeMap::<usize, Result<MutationReceipt, MutationError>>::new();
            let mut batch_high_watermark = None;
            let mut pending_changes = Vec::new();
            let mut receipt_capacity_at = None;
            let evaluate_started = std::time::Instant::now();
            for (prepared_index, (index, operation)) in prepared.iter().enumerate() {
                if let Some(error) = operation.lock_paths().iter().find_map(|path| {
                    self.require_unreserved_object_locked(operation.identity(), &path.path, None)
                        .err()
                }) {
                    results.insert(*index, Err(error));
                    continue;
                }
                let outcome = self
                    .evaluate_operation(
                        &operation,
                        &mut batch,
                        &mut pending_heads,
                        &mut pending_versions,
                        &mut pending_receipts,
                        &mut pending_blob_references,
                        &mut pending_small_blobs,
                        &read_cache,
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
                    pending_changes.extend(
                        evaluated
                            .pending_head_changes(operation.identity(), operation.key().path()),
                    );
                }
                results.insert(*index, outcome.map(|evaluated| evaluated.receipt));
            }
            let evaluate_duration = evaluate_started.elapsed();
            let persistence_started = std::time::Instant::now();
            let persistence = (|| {
                if receipt_status != initial_receipt_status {
                    self.stage_mutation_receipt_status(&mut batch, receipt_status)?;
                }
                self.stage_local_changes_with_admission(
                    &mut batch,
                    &pending_changes,
                    LocalReferenceEffects::AppliedInline,
                    source_journal_admission,
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
                histogram.keldra_store_bulk_lock_duration_seconds = lock_duration.as_secs_f64(),
                histogram.keldra_store_bulk_ordinary_path_lock_wait_duration_seconds =
                    path_lock_wait.as_secs_f64(),
                histogram.keldra_store_bulk_commit_lock_wait_duration_seconds =
                    commit_lock_wait.as_secs_f64(),
                histogram.keldra_store_bulk_evaluate_duration_seconds =
                    evaluate_duration.as_secs_f64(),
                histogram.keldra_store_bulk_persist_duration_seconds =
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
            BatchOperation::Clone(request) => &request.destination,
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
        if matches!(&operation, BatchOperation::Clone(_)) {
            return Err(MutationError::InvalidObjectMutation(
                "distributed clone requires an exact retained-version atomic precondition".into(),
            ));
        }
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
        self.coordinate_prepared_object_mutation(
            prepared,
            context,
            governance,
            None,
            SourceJournalAdmission::Bounded,
        )
        .await
    }

    pub async fn coordinate_definition_object_mutation_with_governance(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
        intent: DefinitionMutationIntent,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if matches!(&operation, BatchOperation::Clone(_)) {
            return Err(MutationError::InvalidObjectMutation(
                "clone is not a definition mutation".into(),
            ));
        }
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
        self.coordinate_prepared_object_mutation(
            prepared,
            context,
            governance,
            Some(intent),
            SourceJournalAdmission::Bounded,
        )
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
        self.coordinate_prepared_object_mutation(
            prepared,
            context,
            governance,
            Some(intent),
            SourceJournalAdmission::Bounded,
        )
        .await
    }

    pub(super) async fn coordinate_prepared_object_mutation(
        &self,
        prepared: PreparedOperation,
        context: ObjectMutationContext,
        governance: ObjectMutationGovernance,
        definition_intent: Option<DefinitionMutationIntent>,
        source_journal_admission: SourceJournalAdmission,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if prepared.command_id().is_none() {
            return Err(MutationError::InvalidCommandId);
        }
        let identity = prepared.identity();

        let _path_guard = self.ordinary_locks.acquire(&prepared.lock_paths()).await;
        let _commit_guard = self.lock_commit("coordinated_object_mutation").await;
        for path in prepared.lock_paths() {
            self.require_unreserved_object_locked(identity, &path.path, None)?;
        }
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
        let read_cache = MutationReadCache::default();
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
                &read_cache,
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
            self.stage_local_changes_with_admission(
                &mut batch,
                &evaluated.pending_head_changes(identity, prepared.key().path()),
                LocalReferenceEffects::Deferred,
                source_journal_admission,
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
        let _commit_guard = self.lock_commit("object_mutation_replica").await;
        self.require_unreserved_object_locked(identity, &mutation.exact_path, None)?;
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
                    .stored_version_by_key(&encoded_version_key)?
                    .map(|stored| stored.version)
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

        if !already_applied {
            let predecessor = match mutation.stamp.predecessor_version {
                Some(version) => Some(
                    self.stored_version_by_key(&exact_version_key(
                        identity,
                        &mutation.exact_path,
                        version,
                    ))?
                    .map(|stored| stored.version)
                    .ok_or_else(|| {
                        MutationError::Storage(
                            "replicated predecessor references a missing version descriptor".into(),
                        )
                    })?,
                ),
                None => None,
            };
            if predecessor
                .as_ref()
                .is_some_and(|version| version.protected_link_descriptor)
            {
                return Err(MutationError::InvalidObjectMutation(
                    "protected alias descriptors must be mutated through sealed link authority"
                        .into(),
                ));
            }
            if self.alias_registry_locked(identity, &mutation.exact_path)?
                != mutation
                    .alias_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.registry.clone())
            {
                return Err(MutationError::ObjectMutationConflict);
            }
            if let Some(snapshot) = mutation.alias_snapshot.as_ref() {
                if predecessor.as_ref() != Some(&snapshot.canonical_version) {
                    return Err(MutationError::ObjectMutationConflict);
                }
            }
        }

        if let Some(existing) = self.stored_version_by_key(&encoded_version_key)?
            && existing.version != mutation.version
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
            let retention = self.version_retention_for_bucket(identity)?;
            if let Some(predecessor) = mutation.stamp.predecessor_version {
                let predecessor_key =
                    exact_version_key(identity, &mutation.exact_path, predecessor);
                if let Some(stored) = self.stored_version_by_key(&predecessor_key)? {
                    match stored.retention {
                        StoredVersionRetention::JournalPending
                            if retention == StoredVersionRetention::UserRetained =>
                        {
                            batch.put_cf(
                                self.cf(CF_VERSIONS)?,
                                predecessor_key,
                                serde_json::to_vec(&StoredVersion::new(
                                    stored.version,
                                    StoredVersionRetention::UserRetained,
                                ))
                                .map_err(storage_error)?,
                            );
                        }
                        StoredVersionRetention::JournalReleased
                            if retention == StoredVersionRetention::UserRetained =>
                        {
                            batch.put_cf(
                                self.cf(CF_VERSIONS)?,
                                predecessor_key,
                                serde_json::to_vec(&StoredVersion::new(
                                    stored.version,
                                    StoredVersionRetention::UserRetained,
                                ))
                                .map_err(storage_error)?,
                            );
                        }
                        StoredVersionRetention::JournalReleased => {
                            batch.delete_cf(self.cf(CF_VERSIONS)?, predecessor_key);
                        }
                        StoredVersionRetention::JournalPending
                        | StoredVersionRetention::UserRetained => {}
                    }
                }
            }
            batch.put_cf(
                self.cf(CF_VERSIONS)?,
                &encoded_version_key,
                serde_json::to_vec(&StoredVersion::new(mutation.version.clone(), retention))
                    .map_err(storage_error)?,
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
        self.stage_local_changes_with_admission(
            batch,
            changes,
            reference_effects,
            SourceJournalAdmission::Bounded,
        )
    }

    pub(super) fn stage_local_changes_with_admission(
        &self,
        batch: &mut WriteBatch,
        changes: &[PendingLocalChange],
        reference_effects: LocalReferenceEffects,
        admission: SourceJournalAdmission,
    ) -> Result<(), MutationError> {
        if changes.is_empty() {
            return Ok(());
        }

        let journal = self.cf(CF_LOCAL_INVALIDATIONS)?;
        let metadata = self.cf(CF_METADATA)?;
        let mut status = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let retained_entries_before = status.retained_entries;
        let retained_bytes_before = status.retained_bytes;
        if admission == SourceJournalAdmission::Bounded
            && (status.retained_entries > self.watch_retention.max_entries
                || status.retained_bytes > self.watch_retention.max_bytes)
        {
            // Repay publication debt before an ordinary append retries.
            return Err(MutationError::SourceJournalCapacity);
        }
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
        let mut appended = VecDeque::new();
        for pending in changes {
            status.tail = status.tail.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation offset is exhausted".into())
            })?;
            let change = pending.at_offset(status.tail);
            let encoded = encode_local_change(&change).map_err(storage_error)?;
            let logical_bytes = invalidation_record_bytes(encoded.len())
                .saturating_add(super::journal_routes::journal_route_logical_bytes(&change));
            if admission == SourceJournalAdmission::Bounded
                && logical_bytes > self.watch_retention.max_bytes
            {
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

        if admission == SourceJournalAdmission::Bounded {
            let appended_entries = status
                .retained_entries
                .saturating_sub(retained_entries_before);
            let appended_bytes = status.retained_bytes.saturating_sub(retained_bytes_before);
            if appended_entries > self.watch_retention.max_entries
                || appended_bytes > self.watch_retention.max_bytes
            {
                return Err(MutationError::SourceJournalTransitionTooLarge {
                    entries: appended_entries,
                    bytes: appended_bytes,
                    maximum_entries: self.watch_retention.max_entries,
                    maximum_bytes: self.watch_retention.max_bytes,
                });
            }
            if status.retained_entries > self.watch_retention.max_entries
                || status.retained_bytes > self.watch_retention.max_bytes
            {
                // Keep capacity retirement in a separate committed prune.
                return Err(MutationError::SourceJournalCapacity);
            }
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
        self.observe_source_journal_progress_debt();
        self.watch_notify.send_replace(());
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
                reject_reserved_object_link_content_type(request.content_type.as_deref())?;
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
                    PreparedPayload::Large(self.stage_blob(&bytes).await?)
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
                reject_reserved_object_link_content_type(request.content_type.as_deref())?;
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
            BatchOperation::Clone(request) => {
                validate_clone_request(&request)?;
                reject_reserved_object_link_content_type(request.content_type.as_deref())?;
                if !distributed_coordination {
                    require_local_durability(request.durability)?;
                    if !self.contains_blob(&request.blob).await? {
                        return Err(MutationError::BlobNotFound);
                    }
                }
                let fingerprint = clone_fingerprint(&request, identity);
                Ok(PreparedOperation::Clone {
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
        let _commit_guard = self.lock_commit("receipt_pruning").await;
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
        read_cache: &MutationReadCache,
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
                None => match read_cache.receipt(receipt_key) {
                    Some(cached) => cached?,
                    None => self.read_json(CF_RECEIPTS, receipt_key)?,
                },
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
                let alias_snapshot = existing
                    .object_mutation
                    .as_ref()
                    .and_then(|mutation| mutation.alias_snapshot.clone());
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
                    alias_snapshot,
                });
            }
        }

        if let PreparedOperation::Clone { request, .. } = operation {
            let source = self.user_retained_version(
                operation.identity(),
                &request.source,
                request.source_version,
            )?;
            if !source.as_ref().is_some_and(|source| {
                !source.deleted
                    && source.id == request.source_version
                    && source.blob.as_ref() == Some(&request.blob)
                    && source.content_type == request.content_type
            }) {
                return Err(MutationError::InvalidObjectMutation(
                    "clone source exact version is no longer live or no longer matches its content identity"
                        .into(),
                ));
            }
        }

        let current = match pending_heads.get(&encoded_key) {
            Some(head) => Some(head.clone()),
            None => match read_cache.head(&encoded_key) {
                Some(cached) => cached?,
                None => self.head_by_storage_key(&encoded_key)?,
            },
        };
        let current_version = match current.as_ref() {
            Some(head) => match pending_versions.get(&encoded_key) {
                Some(version) => Some(version.clone()),
                None => Some(
                    match read_cache.version(&encoded_key) {
                        Some(cached) => cached?,
                        None => self.version_metadata_by_identity(
                            operation.identity(),
                            key,
                            head.version,
                        )?,
                    }
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
        if current_version
            .as_ref()
            .is_some_and(|version| version.protected_link_descriptor)
        {
            return Err(MutationError::InvalidObjectMutation(
                "protected alias descriptors must be mutated through sealed link authority".into(),
            ));
        }
        let alias_registry = self.alias_registry_locked(operation.identity(), key.path())?;
        let alias_snapshot = match alias_registry {
            Some(registry) => {
                if matches!(operation, PreparedOperation::Delete { .. }) {
                    return Err(MutationError::ObjectHasInboundAliases);
                }
                let canonical_version = current_version
                    .clone()
                    .filter(|version| !version.deleted)
                    .ok_or_else(|| {
                        MutationError::Storage(
                            "alias registry exists without a live canonical target".into(),
                        )
                    })?;
                Some(crate::ObjectAliasSnapshot {
                    registry,
                    canonical_version,
                })
            }
            None => None,
        };
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
                PreparedOperation::Clone { request, .. } => request.blob.clone(),
                PreparedOperation::Delete { .. } => unreachable!(),
            };
            let requested_content_type = match operation {
                PreparedOperation::Put { request, .. } => request.content_type.as_ref(),
                PreparedOperation::Publish { request, .. } => request.content_type.as_ref(),
                PreparedOperation::Clone { request, .. } => request.content_type.as_ref(),
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
                    alias_snapshot: None,
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
            PreparedOperation::Clone { request, .. } => Some(request.blob.clone()),
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
                PreparedOperation::Clone { request, .. } => request.content_type.clone(),
                PreparedOperation::Delete { .. } => None,
            },
            deleted,
            committed_at_unix_millis: now_unix_millis,
            protected_link_descriptor: false,
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
        let released_predecessor = (versioning == ObjectVersioning::Unversioned)
            .then_some(current_version.as_ref())
            .flatten()
            .map(|previous| {
                self.stored_version_by_key(&version_key(operation.identity(), key, previous.id))
                    .map(|stored| {
                        stored.filter(|stored| {
                            stored.retention == StoredVersionRetention::JournalReleased
                        })
                    })
            })
            .transpose()?
            .flatten();
        let mut reference_deltas = Vec::with_capacity(2);
        if let Some(reference) = new_blob.as_ref() {
            reference_deltas.push(ReferenceDelta {
                blob: reference.clone(),
                change: 1,
            });
        }
        if let Some(reference) = released_predecessor
            .as_ref()
            .and_then(|stored| stored.version.blob.clone())
        {
            reference_deltas.push(ReferenceDelta {
                blob: reference,
                change: -1,
            });
        }
        if reference_deltas.len() == 2 && reference_deltas[0].blob == reference_deltas[1].blob {
            reference_deltas.clear();
        }
        let released_same_as_new = released_predecessor
            .as_ref()
            .and_then(|stored| stored.version.blob.as_ref())
            .zip(new_blob.as_ref())
            .is_some_and(|(old, new)| old == new);
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
                    format: if alias_snapshot.is_some() {
                        OBJECT_MUTATION_FORMAT
                    } else {
                        crate::LEGACY_OBJECT_MUTATION_FORMAT
                    },
                    tenant_id: operation.identity().tenant_id.0,
                    bucket_id: operation.identity().bucket_id.0,
                    exact_path: key.path().to_owned(),
                    command_id: command_id.to_owned(),
                    input_fingerprint: fingerprint,
                    version: version.clone(),
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
                    alias_snapshot: alias_snapshot.clone(),
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
        let retention = match versioning {
            ObjectVersioning::Unversioned => StoredVersionRetention::JournalPending,
            ObjectVersioning::Enabled => StoredVersionRetention::UserRetained,
        };
        let encoded_version = serde_json::to_vec(&StoredVersion::new(version.clone(), retention))
            .map_err(storage_error)?;
        let encoded_head = serde_json::to_vec(&head).map_err(storage_error)?;
        let versions = self.cf(CF_VERSIONS)?;
        let heads = self.cf(CF_HEADS)?;
        let encoded_version_key = version_key(operation.identity(), key, id);
        let mut blob_reference_updates = Vec::with_capacity(2);
        let small_blob_value = if apply_content_lifecycle {
            match operation {
                PreparedOperation::Put { payload, .. } => match payload.small_bytes() {
                    Some(bytes) => self.prepare_hashed_small_blob_value_cached(
                        payload.reference(),
                        bytes,
                        pending_small_blobs,
                        read_cache.small_blob(payload.reference()),
                    )?,
                    None => None,
                },
                PreparedOperation::Publish { .. }
                | PreparedOperation::Clone { .. }
                | PreparedOperation::Delete { .. } => None,
            }
        } else {
            None
        };
        if apply_content_lifecycle
            && !released_same_as_new
            && let Some(reference) = new_blob.as_ref()
        {
            let update = match operation {
                PreparedOperation::Put { .. } => self.prepare_materialized_blob_publication(
                    reference,
                    pending_blob_references,
                    read_cache.blob_reference(reference),
                    now_unix_millis,
                )?,
                PreparedOperation::Publish { .. } => self
                    .prepare_blob_reference_publication_cached(
                        reference,
                        pending_blob_references,
                        read_cache.blob_reference(reference),
                        now_unix_millis,
                    )?,
                PreparedOperation::Clone { .. } => self.prepare_blob_reference_publication_cached(
                    reference,
                    pending_blob_references,
                    read_cache.blob_reference(reference),
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
            let prefetched = read_cache.blob_reference_by_key(&key);
            self.stage_blob_reference_update_cached(
                batch,
                pending_blob_references,
                key,
                state,
                prefetched,
            )?;
        }
        if apply_content_lifecycle
            && !released_same_as_new
            && let Some(reference) = released_predecessor
                .as_ref()
                .and_then(|stored| stored.version.blob.as_ref())
        {
            let (key, state) = self.prepare_blob_reference_retirement_cached(
                reference,
                pending_blob_references,
                read_cache.blob_reference(reference),
                now_unix_millis,
            )?;
            self.stage_blob_reference_update(batch, pending_blob_references, key, state)?;
        }
        if let Some(previous) = current_version.as_ref() {
            let previous_key = version_key(operation.identity(), key, previous.id);
            if let Some(stored) = self.stored_version_by_key(&previous_key)? {
                match stored.retention {
                    StoredVersionRetention::JournalPending
                        if versioning == ObjectVersioning::Enabled =>
                    {
                        batch.put_cf(
                            versions,
                            previous_key,
                            serde_json::to_vec(&StoredVersion::new(
                                stored.version,
                                StoredVersionRetention::UserRetained,
                            ))
                            .map_err(storage_error)?,
                        );
                    }
                    StoredVersionRetention::JournalReleased
                        if versioning == ObjectVersioning::Enabled =>
                    {
                        batch.put_cf(
                            versions,
                            previous_key,
                            serde_json::to_vec(&StoredVersion::new(
                                stored.version,
                                StoredVersionRetention::UserRetained,
                            ))
                            .map_err(storage_error)?,
                        );
                    }
                    StoredVersionRetention::JournalReleased => {
                        batch.delete_cf(versions, previous_key);
                    }
                    StoredVersionRetention::JournalPending
                    | StoredVersionRetention::UserRetained => {}
                }
            } else if pending_versions
                .get(&encoded_key)
                .is_none_or(|pending| pending.id != previous.id)
            {
                return Err(MutationError::Storage(
                    "current predecessor descriptor is missing".into(),
                ));
            }
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
            alias_snapshot,
        })
    }
}

fn reject_reserved_object_link_content_type(
    content_type: Option<&str>,
) -> Result<(), MutationError> {
    if content_type.is_some_and(crate::is_object_link_content_type) {
        return Err(MutationError::InvalidObjectMutation(
            "object-link content types require sealed built-in transaction authority".into(),
        ));
    }
    Ok(())
}
