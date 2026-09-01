//! One physical coordinator batch for independently receipted distributed publishes.

use super::journal_capacity::SourceJournalAdmission;
use super::mutation_prefetch::MutationReadCache;
use super::mutation_types::DistributedEvaluationContext;
use super::single_node_group_commit::{SingleNodeOperations, SingleNodeOutcomes};
use super::*;
use crate::model::{CoordinatedObjectMutation, ObjectMutationContext, ObjectMutationGovernance};
use crate::{BatchOperation, DefinitionMutationIntent};

struct PreparedDistributedMutation {
    index: usize,
    operation: PreparedOperation,
    definition_intent: Option<DefinitionMutationIntent>,
}

struct CoordinatedBatchEvaluation {
    outcomes: Vec<Result<CoordinatedObjectMutation, MutationError>>,
    receipt_capacity_at: Option<usize>,
    metrics: CoordinatorBatchMetrics,
}

#[derive(Clone, Copy, Default)]
pub(super) struct CoordinatorBatchMetrics {
    pub(super) prepare: std::time::Duration,
    pub(super) policy_wait: std::time::Duration,
    pub(super) path_wait: std::time::Duration,
    pub(super) commit_wait: std::time::Duration,
    pub(super) locked_setup: std::time::Duration,
    pub(super) locked_prefetch: std::time::Duration,
    pub(super) evaluate: std::time::Duration,
    pub(super) stage: std::time::Duration,
    pub(super) persist: std::time::Duration,
    pub(super) settle: std::time::Duration,
    pub(super) commit_hold: std::time::Duration,
    pub(super) total: std::time::Duration,
    pub(super) write_batch_entries: u64,
    pub(super) write_batch_bytes: u64,
    pub(super) physical_commit: bool,
}

#[derive(Clone, Copy)]
enum CoordinatorBatchPayloadPreparation {
    Distributed,
    SingleNode,
}

impl Store {
    /// Evaluate independently receipted operations for one metadata replica
    /// group in request order and commit their successful coordinator state
    /// with one physical RocksDB batch.
    pub async fn coordinate_distributed_mutation_batch(
        &self,
        operations: Vec<(
            BatchOperation,
            ObjectMutationGovernance,
            Option<DefinitionMutationIntent>,
        )>,
        context: ObjectMutationContext,
    ) -> Result<Vec<Result<CoordinatedObjectMutation, MutationError>>, MutationError> {
        let evaluated = self
            .coordinate_mutation_batch(
                operations,
                context,
                CoordinatorBatchPayloadPreparation::Distributed,
            )
            .await?;
        if evaluated.receipt_capacity_at.is_some() {
            Err(MutationError::ReceiptCapacity)
        } else {
            Ok(evaluated.outcomes)
        }
    }

    /// Coordinate one independently receipted batch when the serving topology
    /// has exactly one active node.
    ///
    /// The cluster layer owns and fences that topology decision. Small local
    /// `Put` payloads stay in memory until this method folds their content,
    /// metadata, mutation stamps and reference proofs into the final atomic
    /// RocksDB batch. Replicated durability remains unavailable in a one-node
    /// topology. Other operations retain the ordinary distributed preparation
    /// rules.
    pub async fn coordinate_single_node_mutation_batch(
        &self,
        operations: SingleNodeOperations,
        context: ObjectMutationContext,
    ) -> Result<Vec<Result<CoordinatedObjectMutation, MutationError>>, MutationError> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }
        self.single_node_group_commit
            .submit(self.clone(), operations, context)
            .await
    }

    pub(super) async fn coordinate_single_node_mutation_group(
        &self,
        operations: SingleNodeOperations,
        context: ObjectMutationContext,
        request_operation_counts: &[usize],
    ) -> (Vec<SingleNodeOutcomes>, Option<CoordinatorBatchMetrics>) {
        let total = operations.len();
        let evaluated = self
            .coordinate_mutation_batch(
                operations,
                context,
                CoordinatorBatchPayloadPreparation::SingleNode,
            )
            .await;
        let mut evaluated = match evaluated {
            Ok(evaluated) => evaluated,
            Err(error) => {
                return (
                    request_operation_counts
                        .iter()
                        .map(|_| Err(error.clone()))
                        .collect(),
                    None,
                );
            }
        };
        if request_operation_counts.iter().sum::<usize>() != total {
            return (
                request_operation_counts
                    .iter()
                    .map(|_| {
                        Err(MutationError::Storage(
                            "single-node group boundary is inconsistent".into(),
                        ))
                    })
                    .collect(),
                None,
            );
        }
        let mut responses = Vec::with_capacity(request_operation_counts.len());
        let mut start = 0;
        for count in request_operation_counts {
            let end = start + count;
            let outcomes = evaluated.outcomes.drain(..*count).collect();
            if evaluated
                .receipt_capacity_at
                .is_some_and(|capacity| capacity < end)
            {
                responses.push(Err(MutationError::ReceiptCapacity));
            } else {
                responses.push(Ok(outcomes));
            }
            start = end;
        }
        (responses, Some(evaluated.metrics))
    }

    async fn coordinate_mutation_batch(
        &self,
        operations: Vec<(
            BatchOperation,
            ObjectMutationGovernance,
            Option<DefinitionMutationIntent>,
        )>,
        context: ObjectMutationContext,
        payload_preparation: CoordinatorBatchPayloadPreparation,
    ) -> Result<CoordinatedBatchEvaluation, MutationError> {
        let total_started = std::time::Instant::now();
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        if operations.is_empty() {
            return Ok(CoordinatedBatchEvaluation {
                outcomes: Vec::new(),
                receipt_capacity_at: None,
                metrics: CoordinatorBatchMetrics::default(),
            });
        }

        let total = operations.len();
        let mut prepared = Vec::with_capacity(total);
        let mut early = BTreeMap::new();
        let mut bucket_governance = BTreeMap::<Vec<u8>, ObjectMutationGovernance>::new();
        let prepare_started = std::time::Instant::now();
        for (index, (operation, governance, definition_intent)) in
            operations.into_iter().enumerate()
        {
            let key = match &operation {
                BatchOperation::Put(request) => &request.key,
                BatchOperation::Publish(request) => &request.key,
                BatchOperation::Clone(request) => &request.destination,
                BatchOperation::Delete(request) => &request.key,
            };
            let identity = BucketIdentity {
                tenant_id: TenantId(governance.tenant_id),
                bucket_id: BucketId(governance.bucket_id),
            };
            let validation = governance.validate().and_then(|()| {
                definition_intent
                    .map(DefinitionMutationIntent::validate)
                    .transpose()
                    .map_err(|error| MutationError::InvalidObjectMutation(error.to_string()))?;
                if let Some(existing) = bucket_governance.get(&identity.encode().to_vec())
                    && existing != &governance
                {
                    return Err(MutationError::InvalidPolicy(
                        "one distributed batch supplied contradictory bucket governance".into(),
                    ));
                }
                if key.tenant().is_empty() || key.bucket().is_empty() {
                    return Err(MutationError::InvalidObjectMutation(
                        "distributed batch object identity is empty".into(),
                    ));
                }
                Ok(())
            });
            if let Err(error) = validation {
                early.insert(index, error);
                continue;
            }
            bucket_governance.insert(identity.encode().to_vec(), governance.clone());
            let operation = match payload_preparation {
                CoordinatorBatchPayloadPreparation::Distributed => {
                    self.prepare(operation, identity, true).await
                }
                CoordinatorBatchPayloadPreparation::SingleNode => {
                    self.prepare_single_node_coordinated(operation, identity)
                        .await
                }
            };
            match operation {
                Ok(operation) => prepared.push(PreparedDistributedMutation {
                    index,
                    operation,
                    definition_intent,
                }),
                Err(error) => {
                    early.insert(index, error);
                }
            }
        }
        let prepare_duration = prepare_started.elapsed();

        let policy_wait_started = std::time::Instant::now();
        let _policy_guard = self.policy_gate.read().await;
        let policy_wait_duration = policy_wait_started.elapsed();
        let path_wait_started = std::time::Instant::now();
        let _path_guards = self
            .ordinary_locks
            .acquire(
                &prepared
                    .iter()
                    .map(|item| object_path(item.operation.key()))
                    .collect::<Vec<_>>(),
            )
            .await;
        let path_wait_duration = path_wait_started.elapsed();
        let commit_wait_started = std::time::Instant::now();
        let _commit_guard = self.lock_commit("distributed_publish").await;
        let commit_wait_duration = commit_wait_started.elapsed();
        let commit_hold_started = std::time::Instant::now();
        let locked_setup_started = std::time::Instant::now();
        let mut reserved = BTreeMap::new();
        for item in &prepared {
            if let Err(error) = self.require_unreserved_object_locked(
                item.operation.identity(),
                item.operation.key().path(),
                None,
            ) {
                reserved.insert(item.index, error);
            }
        }
        if !reserved.is_empty() {
            prepared.retain(|item| !reserved.contains_key(&item.index));
            early.extend(reserved);
        }
        let source = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let reference_effects = match payload_preparation {
            CoordinatorBatchPayloadPreparation::Distributed => LocalReferenceEffects::Deferred,
            CoordinatorBatchPayloadPreparation::SingleNode => {
                let cursor = self.reference_delta_cursor(source.source_id).map_err(|error| {
                    MutationError::Storage(format!(
                        "cannot read local reference cursor before single-node coordination: {error}"
                    ))
                })?;
                if cursor > source.tail {
                    return Err(MutationError::Storage(format!(
                        "local reference cursor {cursor} is ahead of source-journal tail {}",
                        source.tail
                    )));
                }
                if cursor == source.tail {
                    LocalReferenceEffects::AppliedInline
                } else {
                    // A derived or distributed publication may have appended
                    // reference work that the delivery runtime has not yet
                    // consumed. Do not advance across that gap or apply this
                    // group's effects out of order; append them to the same
                    // authoritative journal for contiguous delivery instead.
                    LocalReferenceEffects::Deferred
                }
            }
        };
        let mut next_source_position = source.tail.checked_add(1).ok_or_else(|| {
            MutationError::Storage("local invalidation offset is exhausted".into())
        })?;
        let now = now_unix_millis()?;
        let mut batch = WriteBatch::default();
        let mut receipt_status = self.mutation_receipt_status()?;
        let initial_receipt_status = receipt_status;
        let pruned_receipts =
            self.stage_expired_mutation_receipts(&mut batch, now, &mut receipt_status)?;
        let locked_setup_duration = locked_setup_started.elapsed();
        let locked_prefetch_started = std::time::Instant::now();
        let read_cache = MutationReadCache::load(
            self,
            &prepared
                .iter()
                .map(|item| &item.operation)
                .collect::<Vec<_>>(),
        )?;
        let locked_prefetch_duration = locked_prefetch_started.elapsed();
        let mut pending_heads = BTreeMap::new();
        let mut pending_versions = BTreeMap::new();
        let mut pending_receipts = BTreeMap::new();
        let mut pending_blob_references = PendingBlobReferences::new();
        let mut pending_inline_payloads = BTreeSet::new();
        let mut policy_cache = bucket_governance
            .iter()
            .map(|(identity, governance)| (identity.clone(), Ok(governance.policy.clone())))
            .collect();
        let mut versioning_cache = bucket_governance
            .iter()
            .map(|(identity, governance)| (identity.clone(), Ok(governance.versioning)))
            .collect();
        let mut pending_changes = Vec::new();
        let mut high_watermark = None;
        let mut evaluated = BTreeMap::new();
        let mut receipt_capacity_exhausted = false;
        let mut receipt_capacity_at = None;

        let evaluate_started = std::time::Instant::now();
        for item in &prepared {
            let outcome = self
                .evaluate_operation(
                    &item.operation,
                    &mut batch,
                    &mut pending_heads,
                    &mut pending_versions,
                    &mut pending_receipts,
                    &mut pending_blob_references,
                    &mut pending_inline_payloads,
                    &read_cache,
                    &mut policy_cache,
                    &mut versioning_cache,
                    &pruned_receipts,
                    &mut receipt_status,
                    now,
                    Some(DistributedEvaluationContext {
                        mutation: context,
                        source_id: source.source_id,
                        source_journal_position: next_source_position,
                        reference_effects,
                        materialize_inline_payload: matches!(
                            payload_preparation,
                            CoordinatorBatchPayloadPreparation::SingleNode
                        ),
                    }),
                    item.definition_intent,
                )
                .await;
            if outcome
                .as_ref()
                .is_err_and(|error| matches!(error, MutationError::ReceiptCapacity))
            {
                receipt_capacity_exhausted = true;
                receipt_capacity_at = Some(item.index);
                break;
            }
            if let Ok(value) = &outcome {
                if !value.receipt.replayed {
                    let mutation = value.mutation.as_ref().ok_or_else(|| {
                        MutationError::Storage(
                            "distributed batch mutation result is missing".into(),
                        )
                    })?;
                    if mutation.stamp.source_journal_position != next_source_position {
                        return Err(MutationError::Storage(
                            "distributed batch source position changed during evaluation".into(),
                        ));
                    }
                    next_source_position = next_source_position
                        .checked_add(
                            1 + mutation
                                .alias_snapshot
                                .as_ref()
                                .map_or(0, |snapshot| snapshot.registry.aliases.len() as u64),
                        )
                        .ok_or_else(|| {
                            MutationError::Storage("local invalidation offset is exhausted".into())
                        })?;
                    high_watermark = Some(
                        high_watermark.map_or(value.receipt.version, |current: VersionId| {
                            current.max(value.receipt.version)
                        }),
                    );
                    pending_changes.extend(value.pending_head_changes(
                        item.operation.identity(),
                        item.operation.key().path(),
                    ));
                }
            }
            evaluated.insert(
                item.index,
                outcome.map(|value| CoordinatedObjectMutation {
                    receipt: value.receipt,
                    mutation: value.mutation,
                }),
            );
        }
        let proof_mutations = evaluated
            .values()
            .filter_map(|outcome| outcome.as_ref().ok()?.mutation.as_ref())
            .collect::<Vec<_>>();
        self.stage_object_mutation_reference_proofs(&mut batch, &proof_mutations)?;
        let evaluate_duration = evaluate_started.elapsed();

        let stage_started = std::time::Instant::now();
        if receipt_status != initial_receipt_status {
            self.stage_mutation_receipt_status(&mut batch, receipt_status)?;
        }
        self.stage_local_changes(&mut batch, &pending_changes, reference_effects)?;
        if let Some(high_watermark) = high_watermark {
            batch.put_cf(
                self.cf(CF_METADATA)?,
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&high_watermark).map_err(storage_error)?,
            );
        }
        let stage_duration = stage_started.elapsed();
        let persist_started = std::time::Instant::now();
        let physical_commit = !batch.is_empty();
        let write_batch_entries = u64::try_from(batch.len()).unwrap_or(u64::MAX);
        let write_batch_bytes = u64::try_from(batch.size_in_bytes()).unwrap_or(u64::MAX);
        if physical_commit {
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
        }
        let persist_duration = persist_started.elapsed();
        let settle_started = std::time::Instant::now();
        if !pruned_receipts.is_empty() {
            self.mutation_capacity_notify.notify_waiters();
        }
        if !pending_changes.is_empty() {
            if reference_effects == LocalReferenceEffects::AppliedInline {
                self.settle_inline_source_changes()?;
            }
            self.notify_local_invalidations();
        }
        let settle_duration = settle_started.elapsed();
        let mut outcomes = Vec::with_capacity(total);
        for index in 0..total {
            outcomes.push(if let Some(outcome) = evaluated.remove(&index) {
                outcome
            } else if let Some(error) = early.remove(&index) {
                Err(error)
            } else if receipt_capacity_exhausted {
                Err(MutationError::ReceiptCapacity)
            } else {
                return Err(MutationError::Storage(
                    "distributed batch outcome index is inconsistent".into(),
                ));
            });
        }
        let commit_hold_duration = commit_hold_started.elapsed();
        let outcome = CoordinatedBatchEvaluation {
            outcomes,
            receipt_capacity_at,
            metrics: CoordinatorBatchMetrics {
                prepare: prepare_duration,
                policy_wait: policy_wait_duration,
                path_wait: path_wait_duration,
                commit_wait: commit_wait_duration,
                locked_setup: locked_setup_duration,
                locked_prefetch: locked_prefetch_duration,
                evaluate: evaluate_duration,
                stage: stage_duration,
                persist: persist_duration,
                settle: settle_duration,
                commit_hold: commit_hold_duration,
                total: total_started.elapsed(),
                write_batch_entries,
                write_batch_bytes,
                physical_commit,
            },
        };
        Ok(outcome)
    }

    async fn prepare_single_node_coordinated(
        &self,
        operation: BatchOperation,
        identity: BucketIdentity,
    ) -> Result<PreparedOperation, MutationError> {
        let durability = match &operation {
            BatchOperation::Put(request) => request.durability,
            BatchOperation::Publish(request) => request.durability,
            BatchOperation::Clone(request) => request.durability,
            BatchOperation::Delete(request) => request.durability,
        };
        require_local_durability(durability)?;
        let mut request = match operation {
            BatchOperation::Put(request) => request,
            operation => return self.prepare(operation, identity, true).await,
        };
        validate_command_id(request.command_id.as_deref())?;
        let bytes = std::mem::take(&mut request.bytes);
        let payload = if bytes.len() <= PAYLOAD_ARTIFACT_CHUNK_BYTES {
            let reference = blob_reference_for_bytes(&bytes);
            PreparedPayload::Inline { reference, bytes }
        } else {
            PreparedPayload::Sealed(self.stage_blob(&bytes).await?)
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

    /// Evaluate independent, already payload-verified publishes in request order
    /// and commit every successful metadata mutation with one RocksDB batch.
    ///
    /// The cluster layer remains responsible for exact replica-group routing,
    /// payload evidence, replica acknowledgement and per-item durability. A
    /// A capacity error commits the successful request prefix and bounded
    /// expiry pruning before it is returned. The caller's ordinary idempotent
    /// retry then replays that prefix and continues without rebuilding bytes.
    pub async fn coordinate_distributed_publish_batch_with_governance(
        &self,
        requests: Vec<PublishRequest>,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
    ) -> Result<Vec<Result<CoordinatedObjectMutation, MutationError>>, MutationError> {
        self.coordinate_distributed_publish_batch_with_admission(
            requests,
            governance,
            context,
            SourceJournalAdmission::Bounded,
        )
        .await
    }

    /// Trusted grouped derived publication. The cluster layer validates that
    /// every request is an immutable artifact needed to publish progress.
    #[doc(hidden)]
    pub async fn coordinate_derived_progress_publish_batch_with_governance(
        &self,
        requests: Vec<PublishRequest>,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
    ) -> Result<Vec<Result<CoordinatedObjectMutation, MutationError>>, MutationError> {
        self.coordinate_distributed_publish_batch_with_admission(
            requests,
            governance,
            context,
            SourceJournalAdmission::DerivedProgress,
        )
        .await
    }

    async fn coordinate_distributed_publish_batch_with_admission(
        &self,
        requests: Vec<PublishRequest>,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
        source_journal_admission: SourceJournalAdmission,
    ) -> Result<Vec<Result<CoordinatedObjectMutation, MutationError>>, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        governance.validate()?;
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let identity = BucketIdentity {
            tenant_id: TenantId(governance.tenant_id),
            bucket_id: BucketId(governance.bucket_id),
        };
        let mut prepared = Vec::with_capacity(requests.len());
        let mut early = BTreeMap::new();
        for (index, request) in requests.into_iter().enumerate() {
            match self.prepare_verified_distributed_publish(request, identity) {
                Ok(operation) => prepared.push((index, operation)),
                Err(error) => {
                    early.insert(index, error);
                }
            }
        }

        let _policy_guard = self.policy_gate.read().await;
        let _path_guards = self
            .ordinary_locks
            .acquire(
                &prepared
                    .iter()
                    .map(|(_, operation)| object_path(operation.key()))
                    .collect::<Vec<_>>(),
            )
            .await;
        let _commit_guard = self.lock_commit("distributed_publish").await;
        let source = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let mut next_source_position = source.tail.checked_add(1).ok_or_else(|| {
            MutationError::Storage("local invalidation offset is exhausted".into())
        })?;
        let now = now_unix_millis()?;
        let mut batch = WriteBatch::default();
        let mut receipt_status = self.mutation_receipt_status()?;
        let initial_receipt_status = receipt_status;
        let pruned_receipts =
            self.stage_expired_mutation_receipts(&mut batch, now, &mut receipt_status)?;
        let read_cache = MutationReadCache::load(
            self,
            &prepared
                .iter()
                .map(|(_, operation)| operation)
                .collect::<Vec<_>>(),
        )?;
        let mut pending_heads = BTreeMap::new();
        let mut pending_versions = BTreeMap::new();
        let mut pending_receipts = BTreeMap::new();
        let mut pending_blob_references = PendingBlobReferences::new();
        let mut pending_inline_payloads = BTreeSet::new();
        let encoded_bucket = identity.encode().to_vec();
        let mut policy_cache =
            BTreeMap::from([(encoded_bucket.clone(), Ok(governance.policy.clone()))]);
        let mut versioning_cache = BTreeMap::from([(encoded_bucket, Ok(governance.versioning))]);
        let mut pending_changes = Vec::new();
        let mut high_watermark = None;
        let mut evaluated = BTreeMap::new();
        let mut receipt_capacity_exhausted = false;

        for (index, operation) in &prepared {
            let outcome = self
                .evaluate_operation(
                    operation,
                    &mut batch,
                    &mut pending_heads,
                    &mut pending_versions,
                    &mut pending_receipts,
                    &mut pending_blob_references,
                    &mut pending_inline_payloads,
                    &read_cache,
                    &mut policy_cache,
                    &mut versioning_cache,
                    &pruned_receipts,
                    &mut receipt_status,
                    now,
                    Some(DistributedEvaluationContext {
                        mutation: context,
                        source_id: source.source_id,
                        source_journal_position: next_source_position,
                        reference_effects: LocalReferenceEffects::Deferred,
                        materialize_inline_payload: false,
                    }),
                    None,
                )
                .await;
            if outcome
                .as_ref()
                .is_err_and(|error| matches!(error, MutationError::ReceiptCapacity))
            {
                // Receipt creation is the first physical staging step for a
                // new mutation. Capacity therefore leaves none of this
                // failing item in `batch`; stop before evaluating any suffix.
                receipt_capacity_exhausted = true;
                break;
            }
            if let Ok(value) = &outcome {
                if !value.receipt.replayed {
                    let mutation = value.mutation.as_ref().ok_or_else(|| {
                        MutationError::Storage(
                            "distributed batch mutation result is missing".into(),
                        )
                    })?;
                    if mutation.stamp.source_journal_position != next_source_position {
                        return Err(MutationError::Storage(
                            "distributed batch source position changed during evaluation".into(),
                        ));
                    }
                    next_source_position = next_source_position
                        .checked_add(
                            1 + mutation
                                .alias_snapshot
                                .as_ref()
                                .map_or(0, |snapshot| snapshot.registry.aliases.len() as u64),
                        )
                        .ok_or_else(|| {
                            MutationError::Storage("local invalidation offset is exhausted".into())
                        })?;
                    high_watermark = Some(
                        high_watermark.map_or(value.receipt.version, |current: VersionId| {
                            current.max(value.receipt.version)
                        }),
                    );
                    pending_changes
                        .extend(value.pending_head_changes(identity, operation.key().path()));
                }
                if let Some(mutation) = value.mutation.as_ref() {
                    self.stage_object_mutation_reference_proof(&mut batch, mutation)?;
                }
            }
            evaluated.insert(
                *index,
                outcome.map(|value| CoordinatedObjectMutation {
                    receipt: value.receipt,
                    mutation: value.mutation,
                }),
            );
        }

        if receipt_status != initial_receipt_status {
            self.stage_mutation_receipt_status(&mut batch, receipt_status)?;
        }
        self.stage_local_changes_with_admission(
            &mut batch,
            &pending_changes,
            LocalReferenceEffects::Deferred,
            source_journal_admission,
        )?;
        if let Some(high_watermark) = high_watermark {
            batch.put_cf(
                self.cf(CF_METADATA)?,
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&high_watermark).map_err(storage_error)?,
            );
        }
        if !batch.is_empty() {
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
        }
        if !pruned_receipts.is_empty() {
            self.mutation_capacity_notify.notify_waiters();
        }
        if !pending_changes.is_empty() {
            self.notify_local_invalidations();
        }
        if receipt_capacity_exhausted {
            return Err(MutationError::ReceiptCapacity);
        }

        let mut outcomes = Vec::with_capacity(prepared.len() + early.len());
        for index in 0..prepared.len() + early.len() {
            outcomes.push(match evaluated.remove(&index) {
                Some(outcome) => outcome,
                None => Err(early.remove(&index).ok_or_else(|| {
                    MutationError::Storage("distributed batch outcome index is inconsistent".into())
                })?),
            });
        }
        Ok(outcomes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PlacementLogId;

    fn request(path: &str, command: &str, blob: BlobRef) -> PublishRequest {
        PublishRequest {
            key: ObjectKey::new("tenant", "bucket", path).unwrap(),
            blob,
            content_type: Some("application/octet-stream".into()),
            mode: PutMode::PutIfAbsent,
            command_id: Some(command.into()),
            durability: Durability::Local,
        }
    }

    fn put_request(path: &str, command: &str, bytes: &[u8], durability: Durability) -> PutRequest {
        PutRequest {
            key: ObjectKey::new("tenant", "bucket", path).unwrap(),
            bytes: bytes.to_vec(),
            content_type: Some("application/octet-stream".into()),
            mode: PutMode::PutIfAbsent,
            command_id: Some(command.into()),
            durability,
        }
    }

    #[tokio::test]
    async fn single_node_inline_put_batch_has_one_physical_commit_and_readable_stamped_payloads() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
            policy: store.bucket_policy("tenant", "bucket").unwrap(),
        };
        let context = ObjectMutationContext {
            active_placement_log_id: PlacementLogId { term: 3, index: 7 },
            serving_fence_term: 3,
        };
        let payloads = [
            ("objects/0", b"zero".as_slice()),
            ("objects/1", b"one".as_slice()),
            ("objects/2", b"two".as_slice()),
        ];
        let operations = payloads
            .iter()
            .enumerate()
            .map(|(index, (path, bytes))| {
                (
                    BatchOperation::Put(put_request(
                        path,
                        &format!("put-{index}"),
                        bytes,
                        Durability::Local,
                    )),
                    governance.clone(),
                    None,
                )
            })
            .collect();
        let before = store.db.latest_sequence_number();

        let outcomes = store
            .coordinate_single_node_mutation_batch(operations, context)
            .await
            .unwrap();

        assert_eq!(outcomes.len(), payloads.len());
        let mut source = None;
        let mut source_positions = Vec::new();
        for (outcome, (path, bytes)) in outcomes.iter().zip(payloads) {
            let coordinated = outcome.as_ref().unwrap();
            let mutation = coordinated
                .mutation
                .as_ref()
                .expect("new coordinated put must carry its replica mutation");
            source.get_or_insert(mutation.stamp.source_id);
            assert_eq!(source, Some(mutation.stamp.source_id));
            source_positions.push(mutation.stamp.source_journal_position);
            assert!(
                store
                    .read_reference_proof(
                        mutation.stamp.source_id,
                        mutation.stamp.source_journal_position,
                    )
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                mutation.stamp.active_placement_log_id,
                context.active_placement_log_id
            );
            assert_eq!(
                mutation.stamp.serving_fence_term,
                context.serving_fence_term
            );
            let key = ObjectKey::new("tenant", "bucket", path).unwrap();
            let object = store
                .get(&key)
                .await
                .unwrap()
                .expect("committed inline payload must be readable");
            assert_eq!(object.bytes, bytes);
            let reference = mutation.version.blob.as_ref().unwrap();
            let reference_state = store.blob_reference_state(reference).unwrap().unwrap();
            assert_eq!((reference_state.ref_count, reference_state.flags), (1, 0));
            assert_eq!(
                store.head(&key).unwrap().unwrap().mutation_stamp,
                Some(mutation.stamp)
            );
        }
        let journal = store.local_watch_status().unwrap();
        assert_eq!(journal.settled_through, journal.tail);
        assert_eq!(
            store.reference_delta_cursor(journal.source_id).unwrap(),
            journal.tail
        );
        assert_eq!(source_positions.last().copied(), Some(journal.tail));
        assert!(
            store
                .settle_source_journal_positions_if_contiguous(
                    source.expect("batch must have a source"),
                    &source_positions,
                )
                .await
                .unwrap()
                .is_none(),
            "the production local acknowledgement must not need another durable settlement"
        );
        assert_eq!(
            store
                .db
                .get_updates_since(before)
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
                .len(),
            1
        );

        let sequence_before_replicated = store.db.latest_sequence_number();
        let replicated = store
            .coordinate_single_node_mutation_batch(
                vec![(
                    BatchOperation::Put(put_request(
                        "objects/replicated",
                        "put-replicated",
                        b"not locally satisfiable",
                        Durability::Replicated,
                    )),
                    governance,
                    None,
                )],
                context,
            )
            .await
            .unwrap();
        assert!(matches!(
            replicated.as_slice(),
            [Err(MutationError::DurabilityUnavailable)]
        ));
        assert_eq!(
            store.db.latest_sequence_number(),
            sequence_before_replicated
        );
    }

    #[tokio::test]
    async fn multiple_distributed_publishes_use_one_physical_metadata_batch() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let first = store.stage_blob(b"first pack").await.unwrap();
        let second = store.stage_blob(b"second pack").await.unwrap();
        let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
            policy: store.bucket_policy("tenant", "bucket").unwrap(),
        };
        let before = store.db.latest_sequence_number();
        let outcomes = store
            .coordinate_distributed_publish_batch_with_governance(
                vec![
                    request("packs/0", "pack-0", first),
                    request("packs/1", "pack-1", second),
                ],
                governance,
                ObjectMutationContext {
                    active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                    serving_fence_term: 1,
                },
            )
            .await
            .unwrap();

        assert_eq!(outcomes.len(), 2);
        assert!(outcomes.iter().all(std::result::Result::is_ok));
        assert!(
            store
                .head(&ObjectKey::new("tenant", "bucket", "packs/0").unwrap())
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .head(&ObjectKey::new("tenant", "bucket", "packs/1").unwrap())
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store
                .db
                .get_updates_since(before)
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn receipt_capacity_commits_only_the_successful_prefix_before_retry() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_mutation_receipt_retention(
                MutationReceiptRetention::new(60, 1, 1024 * 1024).unwrap(),
            ),
        )
        .await
        .unwrap();
        let first = store.stage_blob(b"first pack").await.unwrap();
        let second = store.stage_blob(b"second pack").await.unwrap();
        let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
            policy: store.bucket_policy("tenant", "bucket").unwrap(),
        };
        let before = store.db.latest_sequence_number();

        let error = store
            .coordinate_distributed_publish_batch_with_governance(
                vec![
                    request("packs/0", "pack-0", first),
                    request("packs/1", "pack-1", second),
                ],
                governance,
                ObjectMutationContext {
                    active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                    serving_fence_term: 1,
                },
            )
            .await
            .unwrap_err();

        assert_eq!(error, MutationError::ReceiptCapacity);
        assert!(
            store
                .head(&ObjectKey::new("tenant", "bucket", "packs/0").unwrap())
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .head(&ObjectKey::new("tenant", "bucket", "packs/1").unwrap())
                .unwrap()
                .is_none()
        );
        assert_eq!(store.mutation_receipt_status().unwrap().entries, 1);
        assert_eq!(
            store
                .db
                .get_updates_since(before)
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
                .len(),
            1
        );
    }
}
