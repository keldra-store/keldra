//! One physical coordinator batch for independently receipted distributed publishes.

use super::evaluation_telemetry::EvaluationSubphaseMetrics;
use super::journal_capacity::SourceJournalAdmission;
use super::mutation_prefetch::MutationReadCache;
use super::mutation_types::DistributedEvaluationContext;
use super::mutations::StagedLocalChanges;
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

struct MutationBatchAttempt {
    batch: WriteBatch,
    evaluated: BTreeMap<usize, Result<CoordinatedObjectMutation, MutationError>>,
    receipt_capacity_at: Option<usize>,
    receipt_status: MutationReceiptStatus,
    pruned_receipts: BTreeSet<Vec<u8>>,
    pending_changes: Vec<PendingLocalChange>,
    high_watermark: Option<VersionId>,
    staged_local_changes: Option<StagedLocalChanges>,
    evaluate_duration: std::time::Duration,
    evaluation_subphases: EvaluationSubphaseMetrics,
    stage_duration: std::time::Duration,
}

#[derive(Clone, Copy)]
struct LaneAuthoritySnapshot {
    watch: WatchJournalStatus,
    receipts: MutationReceiptStatus,
}

impl LaneAuthoritySnapshot {
    /// The exact path, receipt, blob and definition guards keep every object
    /// fact used by the attempt stable. Only these source/receipt allocation
    /// authorities can advance while evaluation runs without the sequence
    /// mutex. `settled_through` is projection progress rather than reservation
    /// authority and is refreshed immediately before reservation.
    fn still_current(self, runtime: &super::mutation_commit_lanes::LaneRuntime) -> bool {
        self.watch.source_id == runtime.reserved_watch.source_id
            && self.watch.tail == runtime.reserved_watch.tail
            && self.watch.retention_floor == runtime.reserved_watch.retention_floor
            && self.watch.retained_entries == runtime.reserved_watch.retained_entries
            && self.watch.retained_bytes == runtime.reserved_watch.retained_bytes
            && self.receipts == runtime.reserved_receipts
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct CoordinatorBatchMetrics {
    pub(super) prepare: std::time::Duration,
    pub(super) policy_wait: std::time::Duration,
    pub(super) path_wait: std::time::Duration,
    /// Legacy commit-mutex wait, or the first lane-sequence acquisition wait.
    pub(super) commit_wait: std::time::Duration,
    pub(super) lane_fence_wait: std::time::Duration,
    pub(super) lane_conflict_wait: std::time::Duration,
    pub(super) physical_slot_wait: std::time::Duration,
    pub(super) physical_slots_active_at_acquire: usize,
    pub(super) physical_slots_active_before_release: usize,
    pub(super) physical_slots_peak_since_start_at_acquire: usize,
    pub(super) physical_slots_peak_since_start_before_release: usize,
    pub(super) physical_slot_count: usize,
    /// Wait and hold for the first authority snapshot only.
    pub(super) first_sequence_wait: std::time::Duration,
    pub(super) first_sequence_hold: std::time::Duration,
    /// Cumulative wait and hold for replacement snapshots after failed
    /// authority comparisons.
    pub(super) authority_retry_snapshot_sequence_wait: std::time::Duration,
    pub(super) authority_retry_snapshot_sequence_hold: std::time::Duration,
    /// Cumulative wait and hold for compare-and-reserve acquisitions.
    pub(super) reservation_sequence_wait: std::time::Duration,
    pub(super) reservation_sequence_hold: std::time::Duration,
    pub(super) locked_setup: std::time::Duration,
    pub(super) baseline_prefetch: std::time::Duration,
    pub(super) baseline_revalidation_retries: u64,
    pub(super) lane_authority_revalidation_retries: u64,
    pub(super) evaluate: std::time::Duration,
    pub(super) evaluation_subphases: EvaluationSubphaseMetrics,
    pub(super) stage: std::time::Duration,
    /// Composite primary persistence plus ordered lane settlement retained for
    /// comparison with historical qualification evidence.
    pub(super) persist: std::time::Duration,
    pub(super) primary_db_write: std::time::Duration,
    pub(super) completion_sequence_wait: std::time::Duration,
    pub(super) prior_retry_projection_db_write: std::time::Duration,
    pub(super) completion_projection_db_write: std::time::Duration,
    pub(super) ordered_frontier_wait: std::time::Duration,
    pub(super) completion_reorder_depth: usize,
    pub(super) completion_ticket_lag: u64,
    pub(super) prior_retry_projection_completions: usize,
    pub(super) completion_projection_completions: usize,
    pub(super) settle: std::time::Duration,
    /// Composite path from post-commit-lock acquisition through outcomes,
    /// retained for comparison with historical qualification evidence.
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
    fn stage_single_node_local_changes(
        &self,
        batch: &mut WriteBatch,
        changes: &[PendingLocalChange],
        reference_effects: LocalReferenceEffects,
        status: WatchJournalStatus,
        reference_cursor: u64,
    ) -> Result<Option<StagedLocalChanges>, MutationError> {
        if changes.is_empty() {
            return Ok(None);
        }
        self.stage_local_changes_from_status(
            batch,
            changes,
            reference_effects,
            SourceJournalAdmission::Bounded,
            status,
            reference_cursor,
            false,
            false,
        )
        .map(Some)
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_mutation_batch_attempt(
        &self,
        prepared: &[PreparedDistributedMutation],
        bucket_governance: &BTreeMap<Vec<u8>, ObjectMutationGovernance>,
        read_cache: &MutationReadCache,
        context: ObjectMutationContext,
        payload_preparation: CoordinatorBatchPayloadPreparation,
        source: WatchJournalStatus,
        initial_receipt_status: MutationReceiptStatus,
    ) -> Result<MutationBatchAttempt, MutationError> {
        let (reference_effects, reference_cursor) = match payload_preparation {
            CoordinatorBatchPayloadPreparation::Distributed => {
                (LocalReferenceEffects::Deferred, None)
            }
            CoordinatorBatchPayloadPreparation::SingleNode => {
                // Lane primary batches apply reference effects inline but do
                // not publish the durable reference cursor. The ordered lane
                // projector owns that cursor, and `reserved_watch` is the
                // in-memory source-position authority while projection is in
                // flight.
                (LocalReferenceEffects::AppliedInlineLane, Some(source.tail))
            }
        };
        let mut next_source_position = source.tail.checked_add(1).ok_or_else(|| {
            MutationError::Storage("local invalidation offset is exhausted".into())
        })?;
        let now = now_unix_millis()?;
        let mut batch = WriteBatch::default();
        let mut receipt_status = initial_receipt_status;
        let pruned_receipts = if matches!(
            payload_preparation,
            CoordinatorBatchPayloadPreparation::SingleNode
        ) {
            BTreeSet::new()
        } else {
            self.stage_expired_mutation_receipts(&mut batch, now, &mut receipt_status)?
        };
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
        let mut receipt_capacity_at = None;
        let mut evaluation_subphases = match payload_preparation {
            CoordinatorBatchPayloadPreparation::SingleNode => {
                EvaluationSubphaseMetrics::single_node_group()
            }
            CoordinatorBatchPayloadPreparation::Distributed => EvaluationSubphaseMetrics::default(),
        };

        let evaluate_started = std::time::Instant::now();
        for item in prepared {
            let outcome = self
                .evaluate_operation(
                    &item.operation,
                    &mut batch,
                    &mut pending_heads,
                    &mut pending_versions,
                    &mut pending_receipts,
                    &mut pending_blob_references,
                    &mut pending_inline_payloads,
                    read_cache,
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
                    &mut evaluation_subphases,
                )
                .await;
            let coordinator_bookkeeping_started = evaluation_subphases.start();
            if outcome
                .as_ref()
                .is_err_and(|error| matches!(error, MutationError::ReceiptCapacity))
            {
                receipt_capacity_at = Some(item.index);
                break;
            }
            if let Ok(value) = &outcome
                && !value.receipt.replayed
            {
                let mutation = value.mutation.as_ref().ok_or_else(|| {
                    MutationError::Storage("distributed batch mutation result is missing".into())
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
                pending_changes.extend(
                    value.pending_head_changes(
                        item.operation.identity(),
                        item.operation.key().path(),
                    ),
                );
            }
            evaluated.insert(
                item.index,
                outcome.map(|value| CoordinatedObjectMutation {
                    receipt: value.receipt,
                    mutation: value.mutation,
                }),
            );
            evaluation_subphases.record_since(
                super::evaluation_telemetry::EvaluationSubphase::Coordinator,
                coordinator_bookkeeping_started,
            );
        }
        let proof_mutations = evaluation_subphases.measure(
            super::evaluation_telemetry::EvaluationSubphase::Coordinator,
            || {
                evaluated
                    .values()
                    .filter_map(|outcome| outcome.as_ref().ok()?.mutation.as_ref())
                    .collect::<Vec<_>>()
            },
        );
        self.stage_object_mutation_reference_proofs(
            &mut batch,
            &proof_mutations,
            &mut evaluation_subphases,
        )?;
        let evaluate_duration = evaluate_started.elapsed();

        let stage_started = std::time::Instant::now();
        if matches!(
            payload_preparation,
            CoordinatorBatchPayloadPreparation::Distributed
        ) && receipt_status != initial_receipt_status
        {
            self.stage_mutation_receipt_status(&mut batch, receipt_status)?;
        }
        let staged_local_changes = match payload_preparation {
            CoordinatorBatchPayloadPreparation::SingleNode => self
                .stage_single_node_local_changes(
                    &mut batch,
                    &pending_changes,
                    reference_effects,
                    source,
                    reference_cursor.expect("single-node reference cursor was read"),
                )?,
            CoordinatorBatchPayloadPreparation::Distributed => {
                self.stage_local_changes(&mut batch, &pending_changes, reference_effects)?;
                None
            }
        };
        if matches!(
            payload_preparation,
            CoordinatorBatchPayloadPreparation::Distributed
        ) && let Some(high_watermark) = high_watermark
        {
            batch.put_cf(
                self.cf(CF_METADATA)?,
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&high_watermark).map_err(storage_error)?,
            );
        }
        let stage_duration = stage_started.elapsed();
        Ok(MutationBatchAttempt {
            batch,
            evaluated,
            receipt_capacity_at,
            receipt_status,
            pruned_receipts,
            pending_changes,
            high_watermark,
            staged_local_changes,
            evaluate_duration,
            evaluation_subphases,
            stage_duration,
        })
    }

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
        self.coordinate_single_node_mutation_batch_with_settlement(operations, context)
            .await
            .map(|batch| batch.outcomes)
    }

    /// Internal cross-crate variant that makes post-coordination source
    /// settlement ownership explicit to the distribution layer.
    #[doc(hidden)]
    pub async fn coordinate_single_node_mutation_batch_with_settlement(
        &self,
        operations: SingleNodeOperations,
        context: ObjectMutationContext,
    ) -> Result<SingleNodeMutationBatch, MutationError> {
        if operations.is_empty() {
            return Ok(SingleNodeMutationBatch {
                outcomes: Vec::new(),
                source_journal_settlement: SourceJournalSettlement::CompletedByCoordinator,
            });
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
                responses.push(Ok(SingleNodeMutationBatch {
                    outcomes,
                    source_journal_settlement: SourceJournalSettlement::CompletedByCoordinator,
                }));
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
                    .flat_map(|item| item.operation.lock_paths())
                    .collect::<Vec<_>>(),
            )
            .await;
        let path_wait_duration = path_wait_started.elapsed();
        let lane_mode = matches!(
            payload_preparation,
            CoordinatorBatchPayloadPreparation::SingleNode
        );
        let prepared_operations = prepared
            .iter()
            .map(|item| &item.operation)
            .collect::<Vec<_>>();
        let mut baseline_prefetch_duration = std::time::Duration::ZERO;
        // The fence must precede the discovery snapshot. Ordinary path locks do
        // not exclude legacy exclusive writers, and predecessor blob stripes
        // can only be known from a head/version snapshot protected from them.
        let mut lane_fence_wait_duration = std::time::Duration::ZERO;
        let lane_fence = if lane_mode {
            let (fence, wait) = self.mutation_commit_lanes.acquire_fence_measured().await;
            lane_fence_wait_duration = wait;
            Some(fence)
        } else {
            None
        };
        let lane_read_cache = if lane_mode {
            let prefetch_started = std::time::Instant::now();
            let read_cache = MutationReadCache::load(self, &prepared_operations)?;
            baseline_prefetch_duration = prefetch_started.elapsed();
            Some(read_cache)
        } else {
            None
        };
        let mut mutation_lane = if lane_mode {
            let mut lane_resources = prepared
                .iter()
                .flat_map(|item| {
                    super::mutation_commit_lanes::conflict_resources(
                        &item.operation,
                        item.definition_intent,
                    )
                })
                .collect::<Vec<_>>();
            lane_resources.extend(
                lane_read_cache
                    .as_ref()
                    .expect("lane read cache was loaded")
                    .predecessor_blob_conflict_resources(),
            );
            Some(
                self.mutation_commit_lanes
                    .acquire_with_fence(
                        lane_fence.expect("lane fence was acquired before cache discovery"),
                        lane_resources,
                    )
                    .await,
            )
        } else {
            None
        };
        let lane_conflict_wait_duration = mutation_lane
            .as_ref()
            .map_or(std::time::Duration::ZERO, |lane| lane.conflict_wait());
        let physical_slot_wait_duration = mutation_lane
            .as_ref()
            .map_or(std::time::Duration::ZERO, |lane| lane.physical_slot_wait());
        let physical_slots_active_at_acquire = mutation_lane
            .as_ref()
            .map_or(0, |lane| lane.physical_slots_active_at_acquire());
        let physical_slots_peak_since_start_at_acquire = mutation_lane
            .as_ref()
            .map_or(0, |lane| lane.physical_slots_peak_since_start_at_acquire());
        let physical_slot_count = mutation_lane
            .as_ref()
            .map_or(0, |lane| lane.physical_slot_count());
        let mut commit_wait_duration = std::time::Duration::ZERO;
        let mut first_sequence_wait_duration = std::time::Duration::ZERO;
        let mut prior_projection_metrics =
            super::mutation_commit_lanes::LaneProjectionMetrics::default();
        let mut baseline_revalidation_retries = 0_u64;
        let mut lane_authority_revalidation_retries = 0_u64;
        let (read_cache, mut commit_guard) = if lane_mode {
            // The first snapshot discovered predecessor blob identities while
            // the lane fence excluded legacy writers. Reload stripe-protected
            // values so concurrent lanes cannot change them between the cached
            // baseline and this lane's atomic write.
            let mut read_cache = lane_read_cache.expect("lane read cache was loaded");
            let prefetch_started = std::time::Instant::now();
            read_cache.refresh_conflict_values(self)?;
            baseline_prefetch_duration =
                baseline_prefetch_duration.saturating_add(prefetch_started.elapsed());
            (read_cache, None)
        } else {
            let (read_cache, guard) = loop {
                let prefetch_started = std::time::Instant::now();
                let read_cache = MutationReadCache::load(self, &prepared_operations)?;
                baseline_prefetch_duration =
                    baseline_prefetch_duration.saturating_add(prefetch_started.elapsed());

                let commit_wait_started = std::time::Instant::now();
                let commit_guard = self.lock_commit("distributed_publish").await;
                commit_wait_duration =
                    commit_wait_duration.saturating_add(commit_wait_started.elapsed());
                if read_cache.is_current(self) {
                    break (read_cache, commit_guard);
                }
                drop(commit_guard);
                baseline_revalidation_retries = baseline_revalidation_retries.saturating_add(1);
                tokio::task::yield_now().await;
            };
            (read_cache, Some(guard))
        };
        drop(prepared_operations);
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
        let locked_setup_duration = locked_setup_started.elapsed();
        if lane_mode && self.mutation_commit_lanes.projection_retry_needed() {
            // Retry a prior physical commit's buffered projection without
            // retaining the global reservation sequence during RocksDB I/O.
            prior_projection_metrics = self.project_lane_completions().await?;
        }
        let mut first_sequence_hold_duration = std::time::Duration::ZERO;
        let mut authority_retry_snapshot_sequence_wait_duration = std::time::Duration::ZERO;
        let mut authority_retry_snapshot_sequence_hold_duration = std::time::Duration::ZERO;
        let mut reservation_sequence_wait_duration = std::time::Duration::ZERO;
        let mut reservation_sequence_hold_duration = std::time::Duration::ZERO;
        let mut authority_snapshot_count = 0_u64;
        let mut retry_evaluate_duration = std::time::Duration::ZERO;
        let mut retry_stage_duration = std::time::Duration::ZERO;
        let mut retry_evaluation_subphases = EvaluationSubphaseMetrics::default();
        let (mut attempt, lane_completion) = loop {
            // Take a short optimistic authority snapshot. Object reads,
            // planning, encoding and proof construction then run concurrently
            // under their exact conflict guards. If another independent lane
            // allocates source/receipt authority first, discard the uncommitted
            // attempt and rebuild it from the new authority rather than
            // weakening offsets, receipts or capacity accounting.
            let authority = if lane_mode {
                let wait_started = std::time::Instant::now();
                let mut guard = self.mutation_commit_lanes.sequence().await;
                let wait_duration = wait_started.elapsed();
                let hold_started = std::time::Instant::now();
                let runtime = guard.as_mut().ok_or_else(|| {
                    MutationError::Storage("mutation lane runtime is not initialized".into())
                })?;
                self.refresh_stale_lane_runtime(runtime)?;
                let reference_cursor = self
                    .reference_delta_cursor(runtime.projected_watch.source_id)
                    .map_err(|error| MutationError::Storage(error.to_string()))?;
                self.rearm_caught_up_reference_frontiers(runtime, reference_cursor)?;
                if reference_cursor < runtime.projected_watch.tail {
                    return Err(MutationError::SourceJournalCapacity);
                }
                let snapshot = LaneAuthoritySnapshot {
                    watch: runtime.reserved_watch,
                    receipts: runtime.reserved_receipts,
                };
                let hold_duration = hold_started.elapsed();
                if authority_snapshot_count == 0 {
                    first_sequence_wait_duration = wait_duration;
                    first_sequence_hold_duration = hold_duration;
                } else {
                    authority_retry_snapshot_sequence_wait_duration =
                        authority_retry_snapshot_sequence_wait_duration
                            .saturating_add(wait_duration);
                    authority_retry_snapshot_sequence_hold_duration =
                        authority_retry_snapshot_sequence_hold_duration
                            .saturating_add(hold_duration);
                }
                authority_snapshot_count = authority_snapshot_count.saturating_add(1);
                drop(guard);
                #[cfg(test)]
                self.mutation_commit_lanes
                    .pause_lane_evaluation_after_snapshot()
                    .await;
                Some(snapshot)
            } else {
                None
            };
            let source = authority.map_or_else(
                || {
                    self.local_watch_status()
                        .map_err(|error| MutationError::Storage(error.to_string()))
                },
                |snapshot| Ok(snapshot.watch),
            )?;
            let receipts = authority.map_or_else(
                || self.mutation_receipt_status(),
                |snapshot| Ok(snapshot.receipts),
            )?;
            let built = self
                .build_mutation_batch_attempt(
                    &prepared,
                    &bucket_governance,
                    &read_cache,
                    context,
                    payload_preparation,
                    source,
                    receipts,
                )
                .await;
            let mut built = match built {
                Ok(built) => built,
                Err(error) if lane_mode => {
                    // Optimistic evaluation can observe a source-position
                    // proof committed after its authority snapshot. Compare
                    // authority before reporting any evaluation error: stale
                    // work must be discarded and rebuilt from the new source
                    // and receipt frontier.
                    let wait_started = std::time::Instant::now();
                    let mut guard = self.mutation_commit_lanes.sequence().await;
                    reservation_sequence_wait_duration =
                        reservation_sequence_wait_duration.saturating_add(wait_started.elapsed());
                    let hold_started = std::time::Instant::now();
                    let runtime = guard.as_mut().ok_or_else(|| {
                        MutationError::Storage("mutation lane runtime is not initialized".into())
                    })?;
                    let authority = authority.expect("lane authority was captured");
                    if authority.still_current(runtime) {
                        return Err(error);
                    }
                    reservation_sequence_hold_duration =
                        reservation_sequence_hold_duration.saturating_add(hold_started.elapsed());
                    lane_authority_revalidation_retries =
                        lane_authority_revalidation_retries.saturating_add(1);
                    drop(guard);
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if !lane_mode || built.batch.is_empty() {
                break (built, None);
            }

            let wait_started = std::time::Instant::now();
            let mut guard = self.mutation_commit_lanes.sequence().await;
            reservation_sequence_wait_duration =
                reservation_sequence_wait_duration.saturating_add(wait_started.elapsed());
            let hold_started = std::time::Instant::now();
            let runtime = guard.as_mut().ok_or_else(|| {
                MutationError::Storage("mutation lane runtime is not initialized".into())
            })?;
            let authority = authority.expect("lane authority was captured");
            if !authority.still_current(runtime) {
                reservation_sequence_hold_duration =
                    reservation_sequence_hold_duration.saturating_add(hold_started.elapsed());
                lane_authority_revalidation_retries =
                    lane_authority_revalidation_retries.saturating_add(1);
                drop(guard);
                retry_evaluate_duration =
                    retry_evaluate_duration.saturating_add(built.evaluate_duration);
                retry_stage_duration = retry_stage_duration.saturating_add(built.stage_duration);
                retry_evaluation_subphases.accumulate(built.evaluation_subphases);
                tokio::task::yield_now().await;
                continue;
            }
            if let Some(staged) = built.staged_local_changes.as_mut() {
                staged.status.settled_through = runtime.reserved_watch.settled_through;
            }
            let watch = built
                .staged_local_changes
                .as_ref()
                .map_or(authority.watch, |staged| staged.status);
            let completion = runtime.reserve(watch, built.receipt_status, built.high_watermark)?;
            self.stage_lane_completion(&mut built.batch, completion)?;
            reservation_sequence_hold_duration =
                reservation_sequence_hold_duration.saturating_add(hold_started.elapsed());
            drop(guard);
            break (built, Some(completion));
        };
        if lane_mode {
            commit_wait_duration = first_sequence_wait_duration;
            drop(commit_guard.take());
        }
        let receipt_capacity_at = attempt.receipt_capacity_at;
        let pruned_receipts = std::mem::take(&mut attempt.pruned_receipts);
        let pending_changes = std::mem::take(&mut attempt.pending_changes);
        let staged_local_changes = attempt.staged_local_changes.take();
        let evaluate_duration = attempt
            .evaluate_duration
            .saturating_add(retry_evaluate_duration);
        let mut evaluation_subphases = attempt.evaluation_subphases;
        evaluation_subphases.accumulate(retry_evaluation_subphases);
        let stage_duration = attempt.stage_duration.saturating_add(retry_stage_duration);
        let mut evaluated = std::mem::take(&mut attempt.evaluated);
        let batch = attempt.batch;
        let persist_started = std::time::Instant::now();
        let physical_commit = !batch.is_empty();
        let write_batch_entries = u64::try_from(batch.len()).unwrap_or(u64::MAX);
        let write_batch_bytes = u64::try_from(batch.size_in_bytes()).unwrap_or(u64::MAX);
        let mut primary_db_write_duration = std::time::Duration::ZERO;
        let persistence = if physical_commit {
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            let write_started = std::time::Instant::now();
            let result = self.db.write_opt(batch, &options).map_err(storage_error);
            primary_db_write_duration = write_started.elapsed();
            result
        } else {
            Ok(())
        };
        let physical_slots_active_before_release = mutation_lane
            .as_ref()
            .map_or(0, |lane| lane.physical_slots_active());
        let physical_slots_peak_since_start_before_release = mutation_lane
            .as_ref()
            .map_or(0, |lane| lane.physical_slots_peak_since_start());
        if let Some(lane) = mutation_lane.as_mut() {
            lane.release_physical_slot();
        }
        let mut lane_settlement_metrics =
            super::mutation_commit_lanes::LaneSettlementMetrics::default();
        if let Some(completion) = lane_completion {
            let committed = match &persistence {
                Ok(()) => true,
                Err(_) => self
                    .db
                    .get_cf(self.cf(CF_METADATA)?, completion.key())
                    .map_err(storage_error)?
                    .is_some(),
            };
            lane_settlement_metrics = self.finish_lane_commit(completion, committed).await?;
        }
        persistence?;
        let persist_duration = persist_started.elapsed();
        let settle_started = std::time::Instant::now();
        if !lane_mode && !pruned_receipts.is_empty() {
            self.mutation_capacity_notify.notify_waiters();
        }
        if !lane_mode && !pending_changes.is_empty() {
            // Distributed coordination always stages deferred local reference
            // effects; the single-node lane path owns inline settlement above.
            debug_assert!(staged_local_changes.is_none());
            self.notify_local_invalidations();
        }
        let settle_duration = settle_started.elapsed();
        let mut outcomes = Vec::with_capacity(total);
        for index in 0..total {
            outcomes.push(if let Some(outcome) = evaluated.remove(&index) {
                outcome
            } else if let Some(error) = early.remove(&index) {
                Err(error)
            } else if receipt_capacity_at.is_some() {
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
                lane_fence_wait: lane_fence_wait_duration,
                lane_conflict_wait: lane_conflict_wait_duration,
                physical_slot_wait: physical_slot_wait_duration,
                physical_slots_active_at_acquire,
                physical_slots_active_before_release,
                physical_slots_peak_since_start_at_acquire,
                physical_slots_peak_since_start_before_release,
                physical_slot_count,
                first_sequence_wait: first_sequence_wait_duration,
                first_sequence_hold: first_sequence_hold_duration,
                authority_retry_snapshot_sequence_wait:
                    authority_retry_snapshot_sequence_wait_duration,
                authority_retry_snapshot_sequence_hold:
                    authority_retry_snapshot_sequence_hold_duration,
                reservation_sequence_wait: reservation_sequence_wait_duration,
                reservation_sequence_hold: reservation_sequence_hold_duration,
                locked_setup: locked_setup_duration,
                baseline_prefetch: baseline_prefetch_duration,
                baseline_revalidation_retries,
                lane_authority_revalidation_retries,
                evaluate: evaluate_duration,
                evaluation_subphases,
                stage: stage_duration,
                persist: persist_duration,
                primary_db_write: primary_db_write_duration,
                completion_sequence_wait: lane_settlement_metrics.completion_sequence_wait,
                prior_retry_projection_db_write: prior_projection_metrics.write,
                completion_projection_db_write: lane_settlement_metrics.projection_write,
                ordered_frontier_wait: lane_settlement_metrics.ordered_frontier_wait,
                completion_reorder_depth: lane_settlement_metrics.completion_reorder_depth,
                completion_ticket_lag: lane_settlement_metrics.completion_ticket_lag,
                prior_retry_projection_completions: prior_projection_metrics.completions,
                completion_projection_completions: lane_settlement_metrics
                    .contiguous_projection_completions,
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
                    &mut EvaluationSubphaseMetrics::default(),
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

    fn governed_put(
        path: &str,
        command: &str,
        bytes: &[u8],
        governance: ObjectMutationGovernance,
    ) -> (
        BatchOperation,
        ObjectMutationGovernance,
        Option<DefinitionMutationIntent>,
    ) {
        (
            BatchOperation::Put(put_request(path, command, bytes, Durability::Local)),
            governance,
            None,
        )
    }

    async fn conflict_resources_for_put(
        store: &Store,
        governance: &ObjectMutationGovernance,
        path: &str,
        command: &str,
        bytes: &[u8],
    ) -> BTreeSet<Vec<u8>> {
        let identity = BucketIdentity {
            tenant_id: TenantId(governance.tenant_id),
            bucket_id: BucketId(governance.bucket_id),
        };
        let prepared = store
            .prepare_single_node_coordinated(
                BatchOperation::Put(put_request(path, command, bytes, Durability::Local)),
                identity,
            )
            .await
            .unwrap();
        store.mutation_commit_lanes.conflict_resources_for_test(
            super::super::mutation_commit_lanes::conflict_resources(&prepared, None),
        )
    }

    #[tokio::test]
    async fn independent_lane_evaluation_retries_without_holding_sequence_authority() {
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
            active_placement_log_id: PlacementLogId { term: 7, index: 9 },
            serving_fence_term: 7,
        };
        let first_path = "objects/paused";
        let first_resources = conflict_resources_for_put(
            &store,
            &governance,
            first_path,
            "paused-command",
            b"paused",
        )
        .await;
        let mut second = None;
        for candidate in 0..256 {
            let path = format!("objects/independent-{candidate}");
            let command = format!("independent-command-{candidate}");
            let bytes = format!("independent-{candidate}").into_bytes();
            let resources =
                conflict_resources_for_put(&store, &governance, &path, &command, &bytes).await;
            if first_resources.is_disjoint(&resources) {
                second = Some((path, command, bytes));
                break;
            }
        }
        let (second_path, second_command, second_bytes) =
            second.expect("the exact conflict resource set has an independent candidate");
        let before = store.local_watch_status().unwrap().tail;

        store.mutation_commit_lanes.pause_next_lane_evaluation();
        let paused = tokio::spawn({
            let store = store.clone();
            let governance = governance.clone();
            async move {
                store
                    .coordinate_mutation_batch(
                        vec![governed_put(
                            first_path,
                            "paused-command",
                            b"paused",
                            governance,
                        )],
                        context,
                        CoordinatorBatchPayloadPreparation::SingleNode,
                    )
                    .await
            }
        });
        store
            .mutation_commit_lanes
            .wait_for_paused_lane_evaluation()
            .await;
        let sequence = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            store.mutation_commit_lanes.sequence(),
        )
        .await
        .expect("evaluation must not retain the sequence authority");
        drop(sequence);

        let independent = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.coordinate_mutation_batch(
                vec![governed_put(
                    &second_path,
                    &second_command,
                    &second_bytes,
                    governance.clone(),
                )],
                context,
                CoordinatorBatchPayloadPreparation::SingleNode,
            ),
        )
        .await
        .expect("independent lane must commit while the first evaluation is paused")
        .unwrap();
        store.mutation_commit_lanes.resume_paused_lane_evaluation();
        let paused = paused.await.unwrap().unwrap();

        assert_eq!(independent.metrics.lane_authority_revalidation_retries, 0);
        assert_eq!(paused.metrics.lane_authority_revalidation_retries, 1);
        let independent_mutation = independent.outcomes[0]
            .as_ref()
            .unwrap()
            .mutation
            .as_ref()
            .unwrap();
        let paused_mutation = paused.outcomes[0]
            .as_ref()
            .unwrap()
            .mutation
            .as_ref()
            .unwrap();
        assert_eq!(
            independent_mutation.stamp.source_journal_position,
            before + 1
        );
        assert_eq!(paused_mutation.stamp.source_journal_position, before + 2);
        assert_eq!(store.local_watch_status().unwrap().tail, before + 2);

        let replay_tail = store.local_watch_status().unwrap().tail;
        let replays: [(&str, &str, &[u8]); 2] = [
            (first_path, "paused-command", b"paused".as_slice()),
            (&second_path, &second_command, second_bytes.as_slice()),
        ];
        for (path, command, bytes) in replays {
            let replay = store
                .coordinate_mutation_batch(
                    vec![governed_put(path, command, bytes, governance.clone())],
                    context,
                    CoordinatorBatchPayloadPreparation::SingleNode,
                )
                .await
                .unwrap();
            assert!(replay.outcomes[0].as_ref().unwrap().receipt.replayed);
        }
        assert_eq!(store.local_watch_status().unwrap().tail, replay_tail);
    }

    #[tokio::test]
    async fn failed_lane_evaluation_does_not_reserve_sequence_authority() {
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
            active_placement_log_id: PlacementLogId { term: 8, index: 1 },
            serving_fence_term: 8,
        };
        store
            .coordinate_mutation_batch(
                vec![governed_put(
                    "objects/existing",
                    "create-existing",
                    b"existing",
                    governance.clone(),
                )],
                context,
                CoordinatorBatchPayloadPreparation::SingleNode,
            )
            .await
            .unwrap();
        let before_watch = store.local_watch_status().unwrap();
        let before_ticket = store
            .mutation_commit_lanes
            .sequence()
            .await
            .as_ref()
            .unwrap()
            .next_ticket;

        let failed = store
            .coordinate_mutation_batch(
                vec![governed_put(
                    "objects/existing",
                    "must-not-reserve",
                    b"replacement",
                    governance,
                )],
                context,
                CoordinatorBatchPayloadPreparation::SingleNode,
            )
            .await
            .unwrap();

        assert!(failed.outcomes[0].is_err());
        assert!(!failed.metrics.physical_commit);
        assert_eq!(store.local_watch_status().unwrap(), before_watch);
        assert_eq!(
            store
                .mutation_commit_lanes
                .sequence()
                .await
                .as_ref()
                .unwrap()
                .next_ticket,
            before_ticket
        );
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
        assert_eq!(source, Some(journal.source_id));
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
    async fn single_node_cache_is_loaded_after_legacy_exclusive_writer_finishes() {
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
            active_placement_log_id: PlacementLogId { term: 1, index: 1 },
            serving_fence_term: 1,
        };
        let exclusive = store.mutation_commit_lanes.acquire_exclusive().await;
        let mutation = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .coordinate_single_node_mutation_batch(
                        vec![(
                            BatchOperation::Put(put_request(
                                "objects/raced",
                                "put-raced",
                                b"lane value",
                                Durability::Local,
                            )),
                            governance,
                            None,
                        )],
                        context,
                    )
                    .await
            }
        });
        while store.mutation_commit_lanes.waiting_fence_readers() == 0 {
            tokio::task::yield_now().await;
        }

        let identity = BucketIdentity {
            tenant_id: TenantId(tenant_id),
            bucket_id: BucketId(bucket_id),
        };
        let head_key = identity.head_key("objects/raced");
        store
            .db
            .put_cf(
                store.cf(CF_HEADS).unwrap(),
                &head_key,
                encode_head(&Head {
                    version: VersionId(u64::MAX),
                    deleted: false,
                    mutation_stamp: None,
                })
                .unwrap(),
            )
            .unwrap();
        drop(exclusive);

        let outcomes = mutation.await.unwrap().unwrap();
        assert!(outcomes[0].is_err());
        assert_eq!(
            store
                .head_by_storage_key(&head_key)
                .unwrap()
                .unwrap()
                .version,
            VersionId(u64::MAX)
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
