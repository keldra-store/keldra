//! One physical coordinator batch for independently receipted distributed publishes.

use super::evaluation_telemetry::EvaluationSubphaseMetrics;
use super::journal_capacity::SourceJournalAdmission;
use super::mutation_prefetch::MutationReadCache;
use super::mutation_types::DistributedEvaluationContext;
use super::mutations::StagedLocalChanges;
use super::single_node_group_commit::{
    GroupConflictRegistrationHandoff, MutationGroupMode, SingleNodeOperations, SingleNodeOutcomes,
};
use super::*;
use crate::model::{CoordinatedObjectMutation, ObjectMutationContext, ObjectMutationGovernance};
use crate::{BatchOperation, DefinitionMutationIntent, MutationReceipt, PlacementLogId};
use tokio::sync::Semaphore;

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
    pub(super) execution_slot_wait: std::time::Duration,
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
    Distributed {
        source_journal_admission: SourceJournalAdmission,
        verified_publish: bool,
    },
    SingleNode {
        source_journal_admission: SourceJournalAdmission,
    },
    /// Direct local `Store` API surface.
    /// It shares the universal conflict-lane and ordered journal authorities,
    /// while deliberately retaining the historical local-head representation
    /// (no peer mutation stamp).
    DirectLocal {
        source_journal_admission: SourceJournalAdmission,
        /// True for the trusted governance variants; false for the convenience
        /// APIs whose settings must be loaded while holding the policy gate.
        governance_supplied: bool,
    },
}

impl Store {
    fn stage_single_node_local_changes(
        &self,
        batch: &mut WriteBatch,
        changes: &[PendingLocalChange],
        reference_effects: LocalReferenceEffects,
        status: WatchJournalStatus,
        reference_cursor: u64,
        source_journal_admission: SourceJournalAdmission,
    ) -> Result<Option<StagedLocalChanges>, MutationError> {
        if changes.is_empty() {
            return Ok(None);
        }
        self.stage_local_changes_from_status(
            batch,
            changes,
            reference_effects,
            source_journal_admission,
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
            CoordinatorBatchPayloadPreparation::Distributed { .. } => {
                (LocalReferenceEffects::Deferred, None)
            }
            CoordinatorBatchPayloadPreparation::SingleNode { .. }
            | CoordinatorBatchPayloadPreparation::DirectLocal { .. } => {
                // Lane primary batches apply reference effects inline but do
                // not publish the durable reference cursor. The ordered lane
                // projector owns that cursor, and `reserved_watch` is the
                // in-memory source-position authority while projection is in
                // flight.
                (LocalReferenceEffects::AppliedInlineLane, Some(source.tail))
            }
        };
        let source_journal_admission = match payload_preparation {
            CoordinatorBatchPayloadPreparation::Distributed {
                source_journal_admission,
                ..
            }
            | CoordinatorBatchPayloadPreparation::SingleNode {
                source_journal_admission,
            }
            | CoordinatorBatchPayloadPreparation::DirectLocal {
                source_journal_admission,
                ..
            } => source_journal_admission,
        };
        let mut next_source_position = source.tail.checked_add(1).ok_or_else(|| {
            MutationError::Storage("local invalidation offset is exhausted".into())
        })?;
        let now = now_unix_millis()?;
        let mut batch = WriteBatch::default();
        let mut receipt_status = initial_receipt_status;
        // Expiry pruning is a separate bounded maintenance authority. Lane
        // attempts must not independently rediscover and debit the same stale
        // receipt while earlier physical commits are still being projected.
        let pruned_receipts = BTreeSet::new();
        let mut pending_heads = BTreeMap::new();
        let mut pending_versions = BTreeMap::new();
        let mut pending_receipts = BTreeMap::new();
        let mut pending_blob_references = PendingBlobReferences::new();
        let mut pending_inline_payloads = BTreeSet::new();
        let settings_are_supplied = !matches!(
            payload_preparation,
            CoordinatorBatchPayloadPreparation::DirectLocal {
                governance_supplied: false,
                ..
            }
        );
        let mut policy_cache = settings_are_supplied
            .then(|| {
                bucket_governance
                    .iter()
                    .map(|(identity, governance)| (identity.clone(), Ok(governance.policy.clone())))
                    .collect()
            })
            .unwrap_or_default();
        let mut versioning_cache = settings_are_supplied
            .then(|| {
                bucket_governance
                    .iter()
                    .map(|(identity, governance)| (identity.clone(), Ok(governance.versioning)))
                    .collect()
            })
            .unwrap_or_default();
        read_cache.seed_bucket_settings(&mut policy_cache, &mut versioning_cache);
        let mut pending_changes = Vec::new();
        let mut high_watermark = None;
        let mut evaluated = BTreeMap::new();
        let mut receipt_capacity_at = None;
        let mut evaluation_subphases = match payload_preparation {
            CoordinatorBatchPayloadPreparation::SingleNode { .. } => {
                EvaluationSubphaseMetrics::single_node_group()
            }
            CoordinatorBatchPayloadPreparation::Distributed { .. }
            | CoordinatorBatchPayloadPreparation::DirectLocal { .. } => {
                EvaluationSubphaseMetrics::default()
            }
        };

        let evaluate_started = std::time::Instant::now();
        for item in prepared {
            let distributed_context = match payload_preparation {
                CoordinatorBatchPayloadPreparation::Distributed { .. }
                | CoordinatorBatchPayloadPreparation::SingleNode { .. } => {
                    Some(DistributedEvaluationContext {
                        mutation: context,
                        source_id: source.source_id,
                        source_journal_position: next_source_position,
                        reference_effects,
                        materialize_inline_payload: matches!(
                            payload_preparation,
                            CoordinatorBatchPayloadPreparation::SingleNode { .. }
                        ),
                        source_journal_admission,
                    })
                }
                CoordinatorBatchPayloadPreparation::DirectLocal { .. } => None,
            };
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
                    distributed_context,
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
                if let Some(mutation) = value.mutation.as_ref() {
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
                } else if !matches!(
                    payload_preparation,
                    CoordinatorBatchPayloadPreparation::DirectLocal { .. }
                ) {
                    return Err(MutationError::Storage(
                        "distributed batch mutation result is missing".into(),
                    ));
                }
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
        // Receipt totals are projected once from ordered completion deltas;
        // physical lanes persist only the receipt rows and their completion
        // marker, never an out-of-order aggregate snapshot.
        let staged_local_changes = match payload_preparation {
            CoordinatorBatchPayloadPreparation::SingleNode {
                source_journal_admission,
            }
            | CoordinatorBatchPayloadPreparation::DirectLocal {
                source_journal_admission,
                ..
            } => self.stage_single_node_local_changes(
                &mut batch,
                &pending_changes,
                reference_effects,
                source,
                reference_cursor.expect("single-node reference cursor was read"),
                source_journal_admission,
            )?,
            CoordinatorBatchPayloadPreparation::Distributed {
                source_journal_admission,
                ..
            } => (!pending_changes.is_empty())
                .then(|| {
                    self.stage_local_changes_from_status(
                        &mut batch,
                        &pending_changes,
                        reference_effects,
                        source_journal_admission,
                        source,
                        source.tail,
                        false,
                        false,
                    )
                })
                .transpose()?,
        };
        let stage_duration = stage_started.elapsed();
        Ok(MutationBatchAttempt {
            batch,
            evaluated,
            receipt_capacity_at,
            receipt_status,
            pruned_receipts,
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
        self.coordinate_distributed_mutation_batch_with_admission(
            operations,
            context,
            SourceJournalAdmission::Bounded,
        )
        .await
    }

    pub(super) async fn coordinate_distributed_mutation_batch_with_admission(
        &self,
        operations: Vec<(
            BatchOperation,
            ObjectMutationGovernance,
            Option<DefinitionMutationIntent>,
        )>,
        context: ObjectMutationContext,
        source_journal_admission: SourceJournalAdmission,
    ) -> Result<Vec<Result<CoordinatedObjectMutation, MutationError>>, MutationError> {
        self.single_node_group_commit
            .submit_distributed(self.clone(), operations, context, source_journal_admission)
            .await
            .map(|batch| batch.outcomes)
    }

    pub(super) async fn coordinate_verified_distributed_publish_operations_with_admission(
        &self,
        publishes: Vec<(
            PublishRequest,
            ObjectMutationGovernance,
            Option<DefinitionMutationIntent>,
        )>,
        context: ObjectMutationContext,
        source_journal_admission: SourceJournalAdmission,
    ) -> Result<Vec<Result<CoordinatedObjectMutation, MutationError>>, MutationError> {
        let operations = publishes
            .into_iter()
            .map(|(request, governance, intent)| {
                (BatchOperation::Publish(request), governance, intent)
            })
            .collect();
        self.single_node_group_commit
            .submit_verified_distributed_publish(
                self.clone(),
                operations,
                context,
                source_journal_admission,
            )
            .await
            .map(|batch| batch.outcomes)
    }

    /// Runs the direct local Store compatibility API through the same
    /// conflict-scoped physical lanes and ordered source/receipt projector as
    /// coordinator traffic. The zero context is never encoded: DirectLocal
    /// intentionally evaluates without a peer mutation stamp.
    pub(super) async fn commit_direct_local_mutation_batch(
        &self,
        operations: Vec<(
            BatchOperation,
            ObjectMutationGovernance,
            Option<DefinitionMutationIntent>,
        )>,
        source_journal_admission: SourceJournalAdmission,
        governance_supplied: bool,
    ) -> Result<(Vec<Result<MutationReceipt, MutationError>>, Option<usize>), MutationError> {
        let evaluated = self
            .coordinate_mutation_batch(
                operations,
                ObjectMutationContext {
                    active_placement_log_id: PlacementLogId { term: 0, index: 0 },
                    serving_fence_term: 0,
                },
                CoordinatorBatchPayloadPreparation::DirectLocal {
                    source_journal_admission,
                    governance_supplied,
                },
            )
            .await?;
        Ok((
            evaluated
                .outcomes
                .into_iter()
                .map(|outcome| outcome.map(|coordinated| coordinated.receipt))
                .collect(),
            evaluated.receipt_capacity_at,
        ))
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

    pub(super) async fn coordinate_mutation_group(
        &self,
        operations: SingleNodeOperations,
        context: ObjectMutationContext,
        request_operation_counts: &[usize],
        source_journal_admission: SourceJournalAdmission,
        mode: MutationGroupMode,
        registration_handoff: GroupConflictRegistrationHandoff,
        execution_slots: Arc<Semaphore>,
    ) -> (Vec<SingleNodeOutcomes>, Option<CoordinatorBatchMetrics>) {
        let total = operations.len();
        let payload_preparation = match mode {
            MutationGroupMode::SingleNode => CoordinatorBatchPayloadPreparation::SingleNode {
                source_journal_admission,
            },
            MutationGroupMode::Distributed => CoordinatorBatchPayloadPreparation::Distributed {
                source_journal_admission,
                verified_publish: false,
            },
            MutationGroupMode::VerifiedDistributedPublish => {
                CoordinatorBatchPayloadPreparation::Distributed {
                    source_journal_admission,
                    verified_publish: true,
                }
            }
        };
        let evaluated = self
            .coordinate_mutation_batch_grouped(
                operations,
                context,
                payload_preparation,
                registration_handoff,
                execution_slots,
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
                            "mutation group boundary is inconsistent".into(),
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
                    source_journal_settlement: match mode {
                        MutationGroupMode::SingleNode => {
                            SourceJournalSettlement::CompletedByCoordinator
                        }
                        MutationGroupMode::Distributed
                        | MutationGroupMode::VerifiedDistributedPublish => {
                            SourceJournalSettlement::RequiredAfterQuorum
                        }
                    },
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
        self.coordinate_mutation_batch_inner(operations, context, payload_preparation, None, None)
            .await
    }

    async fn coordinate_mutation_batch_grouped(
        &self,
        operations: Vec<(
            BatchOperation,
            ObjectMutationGovernance,
            Option<DefinitionMutationIntent>,
        )>,
        context: ObjectMutationContext,
        payload_preparation: CoordinatorBatchPayloadPreparation,
        registration_handoff: GroupConflictRegistrationHandoff,
        execution_slots: Arc<Semaphore>,
    ) -> Result<CoordinatedBatchEvaluation, MutationError> {
        self.coordinate_mutation_batch_inner(
            operations,
            context,
            payload_preparation,
            Some(registration_handoff),
            Some(execution_slots),
        )
        .await
    }

    async fn coordinate_mutation_batch_inner(
        &self,
        operations: Vec<(
            BatchOperation,
            ObjectMutationGovernance,
            Option<DefinitionMutationIntent>,
        )>,
        context: ObjectMutationContext,
        payload_preparation: CoordinatorBatchPayloadPreparation,
        mut registration_handoff: Option<GroupConflictRegistrationHandoff>,
        execution_slots: Option<Arc<Semaphore>>,
    ) -> Result<CoordinatedBatchEvaluation, MutationError> {
        let total_started = std::time::Instant::now();
        if context.serving_fence_term == 0
            && !matches!(
                payload_preparation,
                CoordinatorBatchPayloadPreparation::DirectLocal { .. }
            )
        {
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
                CoordinatorBatchPayloadPreparation::Distributed {
                    verified_publish: true,
                    ..
                } => match operation {
                    BatchOperation::Publish(request) => {
                        self.prepare_verified_distributed_publish(request, identity)
                    }
                    operation => self.prepare(operation, identity, true).await,
                },
                CoordinatorBatchPayloadPreparation::Distributed {
                    verified_publish: false,
                    ..
                } => self.prepare(operation, identity, true).await,
                CoordinatorBatchPayloadPreparation::SingleNode { .. } => {
                    self.prepare_single_node_coordinated(operation, identity)
                        .await
                }
                CoordinatorBatchPayloadPreparation::DirectLocal { .. } => {
                    self.prepare(operation, identity, false).await
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

        // Preserve caller order before taking any guard that the predecessor
        // may still need. In particular, holding a policy read guard here while
        // an earlier group is queued behind a fair policy writer would form a
        // cycle: writer -> this read guard -> predecessor -> writer.
        if let Some(handoff) = registration_handoff.as_mut() {
            handoff.await_predecessor().await;
        }
        let policy_wait_started = std::time::Instant::now();
        let _policy_guard = self.policy_gate.read().await;
        let policy_wait_duration = policy_wait_started.elapsed();
        // Conflict lanes are the mutation authority for every topology. The
        // payload/reference settlement policy differs between local and
        // replicated durability, but neither requires a process-wide commit
        // mutex around unrelated object paths.
        let prepared_operations = prepared
            .iter()
            .map(|item| &item.operation)
            .collect::<Vec<_>>();
        // The fence excludes legacy writers during discovery. Ordinary lanes
        // may still change heads while this full resource set waits, so reload
        // and validate the entire discovery after admission below.
        let (lane_fence, lane_fence_wait_duration) =
            self.mutation_commit_lanes.acquire_fence_measured().await;
        let prefetch_started = std::time::Instant::now();
        let mut lane_read_cache = MutationReadCache::load(self, &prepared_operations)?;
        let mut baseline_prefetch_duration = prefetch_started.elapsed();
        let mut lane_resources = prepared
            .iter()
            .flat_map(|item| {
                super::mutation_commit_lanes::conflict_resources(
                    &item.operation,
                    item.definition_intent,
                )
            })
            .collect::<BTreeSet<_>>();
        lane_resources.extend(lane_read_cache.predecessor_blob_conflict_resources());
        let lane_registration = self
            .mutation_commit_lanes
            .register_with_fence(lane_fence, lane_resources.iter().cloned());
        if let Some(handoff) = registration_handoff.as_mut() {
            handoff.complete();
        }
        let mut lane_admission = lane_registration.acquire_conflicts().await;
        let mut path_wait_duration = std::time::Duration::ZERO;
        let mut baseline_revalidation_retries = 0_u64;
        let (_path_guards, lane_admission) = loop {
            // Register before any exact-path wait so a blocked successor cannot
            // pin the registration handoff of an unrelated later group.
            let started = std::time::Instant::now();
            let path_guards = self
                .ordinary_locks
                .acquire(
                    &prepared
                        .iter()
                        .flat_map(|item| item.operation.lock_paths())
                        .collect::<Vec<_>>(),
                )
                .await;
            path_wait_duration = path_wait_duration.saturating_add(started.elapsed());
            if lane_read_cache.is_current(self) {
                break (path_guards, lane_admission);
            }
            let started = std::time::Instant::now();
            let refreshed = MutationReadCache::load(self, &prepared_operations)?;
            baseline_prefetch_duration =
                baseline_prefetch_duration.saturating_add(started.elapsed());
            baseline_revalidation_retries = baseline_revalidation_retries.saturating_add(1);
            let predecessors = refreshed.predecessor_blob_conflict_resources();
            if predecessors
                .iter()
                .all(|resource| lane_resources.contains(resource))
            {
                lane_read_cache = refreshed;
                break (path_guards, lane_admission);
            }
            lane_resources.extend(predecessors);
            drop(path_guards);
            // Requeue all-or-none at the original scheduler ticket: preserve
            // object/receipt ordering without retaining partial conflict guards.
            lane_admission = lane_admission
                .rediscover(lane_resources.iter().cloned())
                .await;
        };
        // Preparation and complete conflict registration must not consume this
        // bounded execution capacity. Otherwise later groups can fill every
        // permit while waiting for an earlier group that cannot obtain one to
        // register its resources. Conflict admission also establishes that any
        // permit holder can make progress rather than waiting on a predecessor
        // in the ordered registration chain.
        let (_execution_slot, execution_slot_wait_duration) = match execution_slots {
            Some(slots) => {
                let started = std::time::Instant::now();
                let permit = slots
                    .acquire_owned()
                    .await
                    .expect("mutation group execution semaphore remains open");
                (Some(permit), started.elapsed())
            }
            None => (None, std::time::Duration::ZERO),
        };
        let mut mutation_lane = Some(lane_admission.acquire_physical().await);
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
        let mut first_sequence_wait_duration = std::time::Duration::ZERO;
        let mut prior_projection_metrics =
            super::mutation_commit_lanes::LaneProjectionMetrics::default();
        let mut lane_authority_revalidation_retries = 0_u64;
        // Reload stripe-protected values so concurrent lanes cannot change
        // them between the discovery snapshot and this lane's atomic write.
        let prefetch_started = std::time::Instant::now();
        lane_read_cache.refresh_conflict_values(self)?;
        baseline_prefetch_duration =
            baseline_prefetch_duration.saturating_add(prefetch_started.elapsed());
        let read_cache = lane_read_cache;
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
        if self.mutation_commit_lanes.projection_retry_needed() {
            // Retry a prior physical commit's buffered projection without
            // retaining the global reservation sequence during RocksDB I/O.
            prior_projection_metrics = self.request_lane_projection().await?;
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
        let (mut attempt, lane_completion, inline_lane_projection) = loop {
            // Take a short optimistic authority snapshot. Object reads,
            // planning, encoding and proof construction then run concurrently
            // under their exact conflict guards. If another independent lane
            // allocates source/receipt authority first, discard the uncommitted
            // attempt and rebuild it from the new authority rather than
            // weakening offsets, receipts or capacity accounting.
            let authority = {
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
                Err(error) => {
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
            };
            if built.batch.is_empty() {
                break (built, None, None);
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
            let (reference_safe, inline_reference_safe, visibility_settled) =
                match payload_preparation {
                    CoordinatorBatchPayloadPreparation::SingleNode { .. }
                    | CoordinatorBatchPayloadPreparation::DirectLocal { .. } => (true, true, true),
                    CoordinatorBatchPayloadPreparation::Distributed { .. } => (false, false, false),
                };
            let completion = runtime.reserve_with_reference_settlement(
                watch,
                built.receipt_status,
                built.high_watermark,
                reference_safe,
                inline_reference_safe,
                visibility_settled,
            )?;
            let inline_projection =
                self.stage_inline_lane_projection(&mut built.batch, runtime, completion)?;
            if inline_projection.is_none() {
                self.stage_lane_completion(&mut built.batch, completion)?;
            }
            reservation_sequence_hold_duration =
                reservation_sequence_hold_duration.saturating_add(hold_started.elapsed());
            drop(guard);
            break (built, Some(completion), inline_projection);
        };
        let commit_wait_duration = first_sequence_wait_duration;
        let receipt_capacity_at = attempt.receipt_capacity_at;
        let pruned_receipts = std::mem::take(&mut attempt.pruned_receipts);
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
            lane_settlement_metrics = self
                .finish_lane_commit_cancellation_safe_with_inline_projection(
                    completion,
                    inline_lane_projection,
                    persistence.is_ok(),
                    mutation_lane
                        .as_ref()
                        .expect("lane completion requires its admitted mutation lane")
                        .settlement_fence_lease(),
                )
                .await?;
        }
        persistence?;
        let persist_duration = persist_started.elapsed();
        let settle_started = std::time::Instant::now();
        if !pruned_receipts.is_empty() {
            self.mutation_capacity_notify.notify_waiters();
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
                execution_slot_wait: execution_slot_wait_duration,
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

    /// Coordinate trusted derived-progress publication for a topology with one
    /// active node. Reference deltas are applied in the same atomic lane commit
    /// as the object metadata, so there is no quorum settlement or deferred
    /// source-journal debt for the distribution layer to complete.
    #[doc(hidden)]
    pub async fn coordinate_single_node_derived_progress_publish_batch_with_governance(
        &self,
        requests: Vec<PublishRequest>,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
    ) -> Result<SingleNodeMutationBatch, MutationError> {
        if requests.is_empty() {
            return Ok(SingleNodeMutationBatch {
                outcomes: Vec::new(),
                source_journal_settlement: SourceJournalSettlement::CompletedByCoordinator,
            });
        }
        let operations = requests
            .into_iter()
            .map(|request| (BatchOperation::Publish(request), governance.clone(), None))
            .collect();
        self.single_node_group_commit
            .submit_with_admission(
                self.clone(),
                operations,
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
        let publishes = requests
            .into_iter()
            .map(|request| (request, governance.clone(), None))
            .collect();
        self.coordinate_verified_distributed_publish_operations_with_admission(
            publishes,
            context,
            source_journal_admission,
        )
        .await
    }
}

#[cfg(test)]
#[path = "distributed_publish_batch_tests.rs"]
mod tests;
