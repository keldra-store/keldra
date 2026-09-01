//! Bounded cancellation-safe group commit for single-node coordinator calls.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, Semaphore, oneshot};

use super::Store;
use crate::{
    BatchOperation, CoordinatedObjectMutation, DefinitionMutationIntent, MutationError,
    ObjectMutationContext, ObjectMutationGovernance,
};

const DEFAULT_MAX_GROUP_REQUESTS: usize = 5;
const DEFAULT_MAX_GROUP_OPERATIONS: usize = 5_000;
const DEFAULT_MAX_GROUP_INLINE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_QUEUED_REQUESTS: usize = 64;
const DEFAULT_MAX_QUEUED_OPERATIONS: usize = 8_000;
const DEFAULT_MAX_QUEUED_INLINE_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_MAX_GROUP_DWELL: Duration = Duration::from_micros(250);

/// Validated bounds and dwell time for single-node mutation group commit.
///
/// Queue capacities must cover the largest admitted group. Construction also
/// rejects values that Tokio's semaphores cannot represent, so a successfully
/// constructed configuration is safe to install directly in a [`Store`](super::Store).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SingleNodeGroupCommitConfig {
    max_group_requests: usize,
    max_group_operations: usize,
    max_group_inline_bytes: usize,
    max_queued_requests: usize,
    max_queued_operations: usize,
    max_queued_inline_bytes: usize,
    max_group_dwell: Duration,
}

impl SingleNodeGroupCommitConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_group_requests: usize,
        max_group_operations: usize,
        max_group_inline_bytes: usize,
        max_queued_requests: usize,
        max_queued_operations: usize,
        max_queued_inline_bytes: usize,
        max_group_dwell: Duration,
    ) -> anyhow::Result<Self> {
        for (name, value) in [
            ("maximum group requests", max_group_requests),
            ("maximum group operations", max_group_operations),
            ("maximum group inline bytes", max_group_inline_bytes),
            ("maximum queued requests", max_queued_requests),
            ("maximum queued operations", max_queued_operations),
            ("maximum queued inline bytes", max_queued_inline_bytes),
        ] {
            anyhow::ensure!(value != 0, "{name} must be non-zero");
        }
        anyhow::ensure!(
            !max_group_dwell.is_zero(),
            "maximum group dwell must be non-zero"
        );
        anyhow::ensure!(
            max_group_requests <= max_queued_requests,
            "maximum group requests must not exceed maximum queued requests"
        );
        anyhow::ensure!(
            max_group_operations <= max_queued_operations,
            "maximum group operations must not exceed maximum queued operations"
        );
        anyhow::ensure!(
            max_group_inline_bytes <= max_queued_inline_bytes,
            "maximum group inline bytes must not exceed maximum queued inline bytes"
        );
        anyhow::ensure!(
            max_queued_requests <= Semaphore::MAX_PERMITS,
            "maximum queued requests exceeds the runtime semaphore limit"
        );
        for (name, value) in [
            ("maximum queued operations", max_queued_operations),
            ("maximum queued inline bytes", max_queued_inline_bytes),
        ] {
            anyhow::ensure!(
                value <= Semaphore::MAX_PERMITS && u32::try_from(value).is_ok(),
                "{name} exceeds the runtime weighted semaphore limit"
            );
        }
        Ok(Self {
            max_group_requests,
            max_group_operations,
            max_group_inline_bytes,
            max_queued_requests,
            max_queued_operations,
            max_queued_inline_bytes,
            max_group_dwell,
        })
    }

    pub fn max_group_requests(&self) -> usize {
        self.max_group_requests
    }

    pub fn max_group_operations(&self) -> usize {
        self.max_group_operations
    }

    pub fn max_group_inline_bytes(&self) -> usize {
        self.max_group_inline_bytes
    }

    pub fn max_queued_requests(&self) -> usize {
        self.max_queued_requests
    }

    pub fn max_queued_operations(&self) -> usize {
        self.max_queued_operations
    }

    pub fn max_queued_inline_bytes(&self) -> usize {
        self.max_queued_inline_bytes
    }

    pub fn max_group_dwell(&self) -> Duration {
        self.max_group_dwell
    }
}

impl Default for SingleNodeGroupCommitConfig {
    fn default() -> Self {
        Self::new(
            DEFAULT_MAX_GROUP_REQUESTS,
            DEFAULT_MAX_GROUP_OPERATIONS,
            DEFAULT_MAX_GROUP_INLINE_BYTES,
            DEFAULT_MAX_QUEUED_REQUESTS,
            DEFAULT_MAX_QUEUED_OPERATIONS,
            DEFAULT_MAX_QUEUED_INLINE_BYTES,
            DEFAULT_MAX_GROUP_DWELL,
        )
        .expect("default single-node group commit configuration is valid")
    }
}

pub(super) type SingleNodeOperations = Vec<(
    BatchOperation,
    ObjectMutationGovernance,
    Option<DefinitionMutationIntent>,
)>;

/// States whether the one-node coordinator has already attempted settlement
/// for every newly committed source-journal position in this physical group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum SourceJournalSettlement {
    CompletedByCoordinator,
    RequiredAfterQuorum,
}

/// Independently receipted results from one logical one-node request, together
/// with the settlement responsibility required of the distribution layer.
#[derive(Debug)]
#[doc(hidden)]
pub struct SingleNodeMutationBatch {
    pub outcomes: Vec<Result<CoordinatedObjectMutation, MutationError>>,
    pub source_journal_settlement: SourceJournalSettlement,
}

pub(super) type SingleNodeOutcomes = Result<SingleNodeMutationBatch, MutationError>;

pub(super) struct SingleNodeCommitRequest {
    pub(super) operations: SingleNodeOperations,
    pub(super) context: ObjectMutationContext,
    response: oneshot::Sender<SingleNodeOutcomes>,
    _queue_permits: QueuePermits,
}

struct QueuePermits {
    _request: tokio::sync::OwnedSemaphorePermit,
    _operations: tokio::sync::OwnedSemaphorePermit,
    _inline_bytes: tokio::sync::OwnedSemaphorePermit,
}

impl SingleNodeCommitRequest {
    fn operation_count(&self) -> usize {
        self.operations.len()
    }

    fn inline_bytes(&self) -> usize {
        self.operations
            .iter()
            .fold(0_usize, |total, (operation, _, _)| {
                total.saturating_add(match operation {
                    BatchOperation::Put(request) => request.bytes.len(),
                    BatchOperation::Publish(_)
                    | BatchOperation::Clone(_)
                    | BatchOperation::Delete(_) => 0,
                })
            })
    }

    fn consistent_governance(&self) -> Option<BTreeMap<(u64, u64), ObjectMutationGovernance>> {
        let mut collected = BTreeMap::new();
        for (_, governance, _) in &self.operations {
            let identity = (governance.tenant_id, governance.bucket_id);
            if collected
                .get(&identity)
                .is_some_and(|existing| existing != governance)
            {
                return None;
            }
            collected
                .entry(identity)
                .or_insert_with(|| governance.clone());
        }
        Some(collected)
    }
}

#[derive(Default)]
struct QueueState {
    requests: VecDeque<SingleNodeCommitRequest>,
    worker_running: bool,
}

#[derive(Clone)]
pub(super) struct SingleNodeGroupCommit {
    config: SingleNodeGroupCommitConfig,
    state: Arc<Mutex<QueueState>>,
    queue_slots: Arc<Semaphore>,
    operation_slots: Arc<Semaphore>,
    inline_byte_slots: Arc<Semaphore>,
    #[cfg(test)]
    settlement_attempts: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    fail_next_settlement: Arc<std::sync::atomic::AtomicBool>,
}

impl SingleNodeGroupCommit {
    pub(super) fn new(config: SingleNodeGroupCommitConfig) -> Self {
        Self {
            queue_slots: Arc::new(Semaphore::new(config.max_queued_requests)),
            operation_slots: Arc::new(Semaphore::new(config.max_queued_operations)),
            inline_byte_slots: Arc::new(Semaphore::new(config.max_queued_inline_bytes)),
            config,
            state: Arc::new(Mutex::new(QueueState::default())),
            #[cfg(test)]
            settlement_attempts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            fail_next_settlement: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    #[cfg(test)]
    pub(super) fn record_settlement_attempt(&self) {
        self.settlement_attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn settlement_attempts(&self) -> usize {
        self.settlement_attempts
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    async fn wait_until_idle(&self) {
        loop {
            if !self.state.lock().await.worker_running {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    #[cfg(test)]
    fn fail_next_settlement(&self) {
        self.fail_next_settlement
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(super) fn take_injected_settlement_failure(&self) -> bool {
        self.fail_next_settlement
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(not(test))]
    pub(super) fn take_injected_settlement_failure(&self) -> bool {
        false
    }

    pub(super) async fn submit(
        &self,
        store: Store,
        operations: SingleNodeOperations,
        context: ObjectMutationContext,
    ) -> SingleNodeOutcomes {
        let operation_count = operations.len();
        let inline_bytes = operations.iter().fold(0_usize, |total, (operation, _, _)| {
            total.saturating_add(match operation {
                BatchOperation::Put(request) => request.bytes.len(),
                BatchOperation::Publish(_)
                | BatchOperation::Clone(_)
                | BatchOperation::Delete(_) => 0,
            })
        });
        if operation_count > self.config.max_group_operations
            || inline_bytes > self.config.max_group_inline_bytes
        {
            return Err(MutationError::InvalidObjectMutation(
                "single-node commit request exceeds its bounded group admission".into(),
            ));
        }
        let request_permit = self
            .queue_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MutationError::Storage("single-node commit queue closed".into()))?;
        let operation_permit = self
            .operation_slots
            .clone()
            .acquire_many_owned(operation_count as u32)
            .await
            .map_err(|_| {
                MutationError::Storage("single-node commit operation queue closed".into())
            })?;
        let inline_byte_permit = self
            .inline_byte_slots
            .clone()
            .acquire_many_owned(inline_bytes as u32)
            .await
            .map_err(|_| MutationError::Storage("single-node commit byte queue closed".into()))?;
        let (response, received) = oneshot::channel();
        let start_worker = {
            let mut state = self.state.lock().await;
            state.requests.push_back(SingleNodeCommitRequest {
                operations,
                context,
                response,
                _queue_permits: QueuePermits {
                    _request: request_permit,
                    _operations: operation_permit,
                    _inline_bytes: inline_byte_permit,
                },
            });
            if state.worker_running {
                false
            } else {
                state.worker_running = true;
                true
            }
        };
        if start_worker {
            let queue = self.clone();
            tokio::spawn(async move { queue.run(store).await });
        }
        received.await.unwrap_or_else(|_| {
            Err(MutationError::Storage(
                "single-node commit worker stopped before replying".into(),
            ))
        })
    }

    async fn run(self, store: Store) {
        loop {
            let dwell_started = std::time::Instant::now();
            tokio::time::sleep(self.config.max_group_dwell).await;
            let dwell_duration = dwell_started.elapsed();
            let (requests, queued_requests, stop_reason) = {
                let mut state = self.state.lock().await;
                let Some(first) = state.requests.pop_front() else {
                    state.worker_running = false;
                    return;
                };
                let mut group = vec![first];
                let mut operations = group[0].operation_count();
                let mut inline_bytes = group[0].inline_bytes();
                let context = group[0].context;
                let mut governance = group[0].consistent_governance();
                let mut stop_reason = "max_requests";
                while group.len() < self.config.max_group_requests {
                    let Some(candidate) = state.requests.front() else {
                        stop_reason = "queue_empty";
                        break;
                    };
                    let (Some(group_governance), Some(candidate_governance)) =
                        (governance.as_ref(), candidate.consistent_governance())
                    else {
                        stop_reason = "inconsistent_governance";
                        break;
                    };
                    let candidate_operations = candidate.operation_count();
                    let candidate_bytes = candidate.inline_bytes();
                    let compatible_governance =
                        candidate_governance.iter().all(|(identity, value)| {
                            group_governance
                                .get(&identity)
                                .is_none_or(|existing| existing == value)
                        });
                    if candidate.context != context {
                        stop_reason = "context";
                        break;
                    }
                    if !compatible_governance {
                        stop_reason = "governance";
                        break;
                    }
                    if operations.saturating_add(candidate_operations)
                        > self.config.max_group_operations
                    {
                        stop_reason = "operations";
                        break;
                    }
                    if inline_bytes.saturating_add(candidate_bytes)
                        > self.config.max_group_inline_bytes
                    {
                        stop_reason = "inline_bytes";
                        break;
                    }
                    let candidate = state.requests.pop_front().expect("front exists");
                    operations = operations.saturating_add(candidate_operations);
                    inline_bytes = inline_bytes.saturating_add(candidate_bytes);
                    for (identity, value) in candidate_governance {
                        governance
                            .as_mut()
                            .expect("compatible group governance exists")
                            .entry(identity)
                            .or_insert(value);
                    }
                    group.push(candidate);
                }
                (group, state.requests.len(), stop_reason)
            };

            let request_count = requests.len();
            let operation_counts = requests
                .iter()
                .map(SingleNodeCommitRequest::operation_count)
                .collect::<Vec<_>>();
            let context = requests[0].context;
            let mut operations = Vec::with_capacity(operation_counts.iter().sum());
            let mut replies = Vec::with_capacity(requests.len());
            let inline_bytes = requests.iter().fold(0_usize, |total, request| {
                total.saturating_add(request.inline_bytes())
            });
            for request in requests {
                operations.extend(request.operations);
                replies.push((request.response, request._queue_permits));
            }
            let operation_count = operations.len();
            let execute_started = std::time::Instant::now();
            let (results, metrics) = store
                .coordinate_single_node_mutation_group(operations, context, &operation_counts)
                .await;
            let execute_duration = execute_started.elapsed();
            let failed_requests = results.iter().filter(|result| result.is_err()).count();
            let metrics = metrics.unwrap_or_default();
            let evaluation_uncategorized = metrics
                .evaluate
                .saturating_sub(metrics.evaluation_subphases.categorized());
            tracing::info!(
                target: "keldra_store::single_node_group_commit_phases",
                attempts = 1_u64,
                physical_commits = metrics.physical_commit as u64,
                request_count,
                operation_count,
                inline_bytes,
                failed_requests,
                dwell_seconds = dwell_duration.as_secs_f64(),
                execute_seconds = execute_duration.as_secs_f64(),
                prepare_seconds = metrics.prepare.as_secs_f64(),
                policy_wait_seconds = metrics.policy_wait.as_secs_f64(),
                path_wait_seconds = metrics.path_wait.as_secs_f64(),
                commit_wait_seconds = metrics.commit_wait.as_secs_f64(),
                locked_setup_seconds = metrics.locked_setup.as_secs_f64(),
                locked_prefetch_seconds = metrics.locked_prefetch.as_secs_f64(),
                evaluate_seconds = metrics.evaluate.as_secs_f64(),
                evaluation_current_precondition_governance_seconds = metrics
                    .evaluation_subphases
                    .current_precondition_governance
                    .as_secs_f64(),
                evaluation_mutation_planning_seconds = metrics
                    .evaluation_subphases
                    .mutation_planning
                    .as_secs_f64(),
                evaluation_mutation_construction_validation_seconds = metrics
                    .evaluation_subphases
                    .mutation_construction_validation
                    .as_secs_f64(),
                evaluation_mutation_construction_validation_operations = metrics
                    .evaluation_subphases
                    .mutation_construction_validation_operations,
                evaluation_durable_record_encoding_seconds = metrics
                    .evaluation_subphases
                    .durable_record_encoding
                    .as_secs_f64(),
                evaluation_inline_payload_receipt_stage_seconds = metrics
                    .evaluation_subphases
                    .inline_payload_receipt_stage
                    .as_secs_f64(),
                evaluation_inline_payload_receipt_stage_operations = metrics
                    .evaluation_subphases
                    .inline_payload_receipt_stage_operations,
                evaluation_blob_lifecycle_stage_seconds = metrics
                    .evaluation_subphases
                    .blob_lifecycle_stage
                    .as_secs_f64(),
                evaluation_object_state_stage_seconds = metrics
                    .evaluation_subphases
                    .object_state_stage
                    .as_secs_f64(),
                evaluation_coordinator_bookkeeping_seconds = metrics
                    .evaluation_subphases
                    .coordinator_bookkeeping
                    .as_secs_f64(),
                evaluation_proof_construction_seconds = metrics
                    .evaluation_subphases
                    .proof_construction
                    .as_secs_f64(),
                evaluation_proof_construction_proofs =
                    metrics.evaluation_subphases.proof_construction_proofs,
                evaluation_proof_multi_get_lookup_seconds = metrics
                    .evaluation_subphases
                    .proof_multi_get_lookup
                    .as_secs_f64(),
                evaluation_proof_multi_get_lookup_proofs =
                    metrics.evaluation_subphases.proof_multi_get_lookup_proofs,
                evaluation_proof_validate_encode_stage_seconds = metrics
                    .evaluation_subphases
                    .proof_validate_encode_stage
                    .as_secs_f64(),
                evaluation_proof_validate_encode_stage_proofs = metrics
                    .evaluation_subphases
                    .proof_validate_encode_stage_proofs,
                evaluation_proof_bookkeeping_seconds = metrics
                    .evaluation_subphases
                    .proof_bookkeeping
                    .as_secs_f64(),
                evaluation_uncategorized_seconds = evaluation_uncategorized.as_secs_f64(),
                stage_seconds = metrics.stage.as_secs_f64(),
                db_write_sync_seconds = metrics.persist.as_secs_f64(),
                settle_seconds = metrics.settle.as_secs_f64(),
                commit_hold_seconds = metrics.commit_hold.as_secs_f64(),
                store_seconds = metrics.total.as_secs_f64(),
                write_batch_entries = metrics.write_batch_entries,
                write_batch_bytes = metrics.write_batch_bytes,
                total_seconds = dwell_duration.saturating_add(execute_duration).as_secs_f64(),
                queued_requests,
                stop_reason,
                phase_complete = metrics.total != std::time::Duration::ZERO,
                physical_commit = metrics.physical_commit,
                "single-node mutation group completed"
            );
            for ((response, _permits), result) in replies.into_iter().zip(results) {
                let _ = response.send(result);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BucketPolicy, Durability, ObjectKey, PlacementLogId, PutMode, PutRequest, StoreOptions,
    };

    fn context(term: u64) -> ObjectMutationContext {
        ObjectMutationContext {
            active_placement_log_id: PlacementLogId { term, index: term },
            serving_fence_term: term,
        }
    }

    fn governance(store: &Store) -> ObjectMutationGovernance {
        let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
            policy: store.bucket_policy("tenant", "bucket").unwrap(),
        }
    }

    fn request(
        path: &str,
        command: &str,
        governance: ObjectMutationGovernance,
    ) -> SingleNodeOperations {
        vec![(
            BatchOperation::Put(PutRequest {
                key: ObjectKey::new("tenant", "bucket", path).unwrap(),
                bytes: command.as_bytes().to_vec(),
                content_type: Some("application/octet-stream".into()),
                mode: PutMode::PutIfAbsent,
                command_id: Some(command.into()),
                durability: Durability::Local,
            }),
            governance,
            None,
        )]
    }

    fn physical_commits_since(store: &Store, sequence: u64) -> usize {
        store
            .db
            .get_updates_since(sequence)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
            .len()
    }

    #[tokio::test]
    async fn five_requests_share_one_commit_and_one_group_settlement_attempt() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let governance = governance(&store);
        let before = store.db.latest_sequence_number();
        let settlements_before = store.single_node_group_commit.settlement_attempts();
        let (a, b, c, d, e) = tokio::join!(
            store.coordinate_single_node_mutation_batch(
                request("objects/five-a", "five-a", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/five-b", "five-b", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/five-c", "five-c", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/five-d", "five-d", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/five-e", "five-e", governance),
                context(1),
            ),
        );

        for result in [a, b, c, d, e] {
            assert!(matches!(result.as_deref(), Ok([Ok(_)])));
        }
        assert_eq!(physical_commits_since(&store, before), 1);
        assert_eq!(
            store.single_node_group_commit.settlement_attempts() - settlements_before,
            1
        );
        let status = store.local_watch_status().unwrap();
        assert_eq!(status.settled_through, status.tail);
    }

    #[tokio::test]
    async fn ten_compatible_requests_share_one_physical_commit() {
        let temporary = tempfile::tempdir().unwrap();
        let config = SingleNodeGroupCommitConfig::new(
            10,
            DEFAULT_MAX_GROUP_OPERATIONS,
            DEFAULT_MAX_GROUP_INLINE_BYTES,
            DEFAULT_MAX_QUEUED_REQUESTS,
            DEFAULT_MAX_QUEUED_OPERATIONS,
            DEFAULT_MAX_QUEUED_INLINE_BYTES,
            DEFAULT_MAX_GROUP_DWELL,
        )
        .unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_single_node_group_commit(config),
        )
        .await
        .unwrap();
        let governance = governance(&store);
        let before = store.db.latest_sequence_number();
        let settlements_before = store.single_node_group_commit.settlement_attempts();
        let (a, b, c, d, e, f, g, h, i, j) = tokio::join!(
            store.coordinate_single_node_mutation_batch(
                request("objects/a", "a", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/b", "b", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/c", "c", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/d", "d", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/e", "e", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/f", "f", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/g", "g", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/h", "h", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/i", "i", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/j", "j", governance),
                context(1),
            ),
        );

        for result in [a, b, c, d, e, f, g, h, i, j] {
            assert!(matches!(result.as_deref(), Ok([Ok(_)])));
        }
        assert_eq!(physical_commits_since(&store, before), 1);
        assert_eq!(
            store.single_node_group_commit.settlement_attempts() - settlements_before,
            1
        );
        let status = store.local_watch_status().unwrap();
        assert_eq!(
            store.reference_delta_cursor(status.source_id).unwrap(),
            status.tail
        );
    }

    #[test]
    fn group_commit_config_rejects_limits_not_covered_by_the_queue() {
        let error = SingleNodeGroupCommitConfig::new(
            65,
            DEFAULT_MAX_GROUP_OPERATIONS,
            DEFAULT_MAX_GROUP_INLINE_BYTES,
            64,
            DEFAULT_MAX_QUEUED_OPERATIONS,
            DEFAULT_MAX_QUEUED_INLINE_BYTES,
            DEFAULT_MAX_GROUP_DWELL,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("maximum group requests must not exceed maximum queued requests")
        );
    }

    #[test]
    fn group_commit_config_rejects_zero_limits_and_dwell() {
        let zero_requests = SingleNodeGroupCommitConfig::new(
            0,
            DEFAULT_MAX_GROUP_OPERATIONS,
            DEFAULT_MAX_GROUP_INLINE_BYTES,
            DEFAULT_MAX_QUEUED_REQUESTS,
            DEFAULT_MAX_QUEUED_OPERATIONS,
            DEFAULT_MAX_QUEUED_INLINE_BYTES,
            DEFAULT_MAX_GROUP_DWELL,
        )
        .unwrap_err();
        assert!(zero_requests.to_string().contains("must be non-zero"));

        let zero_dwell = SingleNodeGroupCommitConfig::new(
            DEFAULT_MAX_GROUP_REQUESTS,
            DEFAULT_MAX_GROUP_OPERATIONS,
            DEFAULT_MAX_GROUP_INLINE_BYTES,
            DEFAULT_MAX_QUEUED_REQUESTS,
            DEFAULT_MAX_QUEUED_OPERATIONS,
            DEFAULT_MAX_QUEUED_INLINE_BYTES,
            Duration::ZERO,
        )
        .unwrap_err();
        assert!(zero_dwell.to_string().contains("dwell must be non-zero"));
    }

    #[tokio::test]
    async fn deferred_reference_backlog_does_not_reject_single_node_group() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let governance = governance(&store);
        let deferred = store
            .coordinate_distributed_mutation_batch(
                request("objects/deferred", "deferred", governance.clone()),
                context(1),
            )
            .await
            .unwrap();
        assert!(matches!(deferred.as_slice(), [Ok(_)]));
        let deferred_mutation = deferred[0]
            .as_ref()
            .unwrap()
            .mutation
            .as_ref()
            .expect("new deferred operation must carry its mutation");
        store
            .settle_source_journal_positions_if_contiguous(
                deferred_mutation.stamp.source_id,
                &[deferred_mutation.stamp.source_journal_position],
            )
            .await
            .unwrap();
        let before = store.local_watch_status().unwrap();
        let cursor = store.reference_delta_cursor(before.source_id).unwrap();
        assert!(cursor < before.tail);
        assert_eq!(before.settled_through, before.tail);
        let before_sequence = store.db.latest_sequence_number();
        let settlements_before = store.single_node_group_commit.settlement_attempts();

        let (first, second) = tokio::join!(
            store.coordinate_single_node_mutation_batch(
                request(
                    "objects/first-after-gap",
                    "first-after-gap",
                    governance.clone()
                ),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/second-after-gap", "second-after-gap", governance),
                context(1),
            ),
        );

        assert!(matches!(first.as_deref(), Ok([Ok(_)])), "first={first:?}");
        assert!(
            matches!(second.as_deref(), Ok([Ok(_)])),
            "second={second:?}"
        );
        assert_eq!(physical_commits_since(&store, before_sequence), 2);
        assert_eq!(
            store.single_node_group_commit.settlement_attempts() - settlements_before,
            1
        );
        for (path, expected) in [
            ("objects/first-after-gap", b"first-after-gap".as_slice()),
            ("objects/second-after-gap", b"second-after-gap".as_slice()),
        ] {
            let object = store
                .get(&ObjectKey::new("tenant", "bucket", path).unwrap())
                .await
                .unwrap()
                .expect("post-gap single-node object must remain readable");
            assert_eq!(object.bytes, expected);
            let reference = object
                .version
                .blob
                .expect("put must retain its blob identity");
            let state = store
                .blob_reference_state(&reference)
                .unwrap()
                .expect("inline payload must retain lifecycle authority");
            assert_eq!(state.ref_count, 1);
            assert_eq!(
                store.complete_copy_state(&reference).await.unwrap(),
                crate::PayloadArtifactState::Valid
            );
        }
        let after = store.local_watch_status().unwrap();
        assert!(after.tail > before.tail);
        assert_eq!(after.settled_through, after.tail);
        assert_eq!(
            store.reference_delta_cursor(after.source_id).unwrap(),
            cursor
        );
        store.single_node_group_commit.wait_until_idle().await;
        drop(store);
        let reopened = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let reopened_status = reopened.local_watch_status().unwrap();
        assert_eq!(reopened_status.source_id, after.source_id);
        assert_eq!(reopened_status.tail, after.tail);
        assert_eq!(reopened_status.settled_through, after.settled_through);
    }

    #[tokio::test]
    async fn settlement_failure_preserves_receipt_and_requires_quorum_fallback() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let governance = governance(&store);
        let deferred = store
            .coordinate_distributed_mutation_batch(
                request(
                    "objects/unsettled-prefix",
                    "unsettled-prefix",
                    governance.clone(),
                ),
                context(1),
            )
            .await
            .unwrap();
        assert!(matches!(deferred.as_slice(), [Ok(_)]));
        let before = store.local_watch_status().unwrap();
        assert!(before.settled_through < before.tail);

        let operation = request("objects/settlement-retry", "settlement-retry", governance);
        store.single_node_group_commit.fail_next_settlement();
        let committed = store
            .coordinate_single_node_mutation_batch_with_settlement(operation.clone(), context(1))
            .await
            .unwrap();
        assert_eq!(
            committed.source_journal_settlement,
            SourceJournalSettlement::RequiredAfterQuorum
        );
        let receipt = committed.outcomes[0].as_ref().unwrap();
        assert!(!receipt.receipt.replayed);
        let committed_tail = store.local_watch_status().unwrap().tail;
        assert!(committed_tail > before.tail);

        let replay = store
            .coordinate_single_node_mutation_batch_with_settlement(operation, context(1))
            .await
            .unwrap();
        assert_eq!(
            replay.source_journal_settlement,
            SourceJournalSettlement::CompletedByCoordinator
        );
        assert!(replay.outcomes[0].as_ref().unwrap().receipt.replayed);
        let unsettled = store.local_watch_status().unwrap();
        assert_eq!(unsettled.settled_through, before.settled_through);

        let positions = (unsettled.settled_through + 1..=unsettled.tail).collect::<Vec<_>>();
        store
            .settle_source_journal_positions_if_contiguous(unsettled.source_id, &positions)
            .await
            .unwrap();
        let recovered = store.local_watch_status().unwrap();
        assert_eq!(recovered.settled_through, recovered.tail);
        drop(store);

        let reopened = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let reopened_status = reopened.local_watch_status().unwrap();
        assert_eq!(reopened_status.tail, recovered.tail);
        assert_eq!(reopened_status.settled_through, recovered.tail);
    }

    #[tokio::test]
    async fn fifo_evaluation_preserves_same_path_cas() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let governance = governance(&store);
        let settlements_before = store.single_node_group_commit.settlement_attempts();
        let (first, second) = tokio::join!(
            store.coordinate_single_node_mutation_batch(
                request("objects/same", "first", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/same", "second", governance),
                context(1),
            ),
        );

        assert!(matches!(first.as_deref(), Ok([Ok(_)])));
        assert!(matches!(
            second.as_deref(),
            Ok([Err(MutationError::PreconditionFailed { .. })])
        ));
        assert_eq!(
            store
                .get(&ObjectKey::new("tenant", "bucket", "objects/same").unwrap())
                .await
                .unwrap()
                .unwrap()
                .bytes,
            b"first"
        );
        assert_eq!(
            store.single_node_group_commit.settlement_attempts() - settlements_before,
            1
        );
        let status = store.local_watch_status().unwrap();
        assert_eq!(status.settled_through, status.tail);
    }

    #[tokio::test]
    async fn conflicting_governance_splits_physical_commits() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let first = governance(&store);
        let mut second = first.clone();
        second.policy = BucketPolicy {
            immutable_prefixes: vec!["immutable".into()],
            ..BucketPolicy::default()
        };
        let before = store.db.latest_sequence_number();
        let (first, second) = tokio::join!(
            store.coordinate_single_node_mutation_batch(
                request("objects/first", "first", first),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/second", "second", second),
                context(1),
            ),
        );

        assert!(matches!(first.as_deref(), Ok([Ok(_)])), "first={first:?}");
        assert!(
            matches!(second.as_deref(), Ok([Ok(_)])),
            "second={second:?}"
        );
        assert_eq!(physical_commits_since(&store, before), 2);
    }

    #[tokio::test]
    async fn incompatible_context_splits_physical_commits() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let governance = governance(&store);
        let before = store.db.latest_sequence_number();
        let (first, second) = tokio::join!(
            store.coordinate_single_node_mutation_batch(
                request("objects/context-first", "context-first", governance.clone()),
                context(1),
            ),
            store.coordinate_single_node_mutation_batch(
                request("objects/context-second", "context-second", governance),
                context(2),
            ),
        );

        assert!(matches!(first.as_deref(), Ok([Ok(_)])));
        assert!(matches!(second.as_deref(), Ok([Ok(_)])));
        assert_eq!(physical_commits_since(&store, before), 2);
    }

    #[tokio::test]
    async fn dropped_receiver_does_not_cancel_an_admitted_commit() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let queue = store.single_node_group_commit.clone();
        let settlements_before = queue.settlement_attempts();
        let operations = request("objects/detached", "detached", governance(&store));
        let request_permit = queue.queue_slots.clone().acquire_owned().await.unwrap();
        let operation_permit = queue
            .operation_slots
            .clone()
            .acquire_many_owned(1)
            .await
            .unwrap();
        let inline_byte_permit = queue
            .inline_byte_slots
            .clone()
            .acquire_many_owned("detached".len() as u32)
            .await
            .unwrap();
        let (response, received) = oneshot::channel();
        drop(received);
        {
            let mut state = queue.state.lock().await;
            state.requests.push_back(SingleNodeCommitRequest {
                operations,
                context: context(1),
                response,
                _queue_permits: QueuePermits {
                    _request: request_permit,
                    _operations: operation_permit,
                    _inline_bytes: inline_byte_permit,
                },
            });
            state.worker_running = true;
        }

        queue.clone().run(store.clone()).await;

        assert!(
            store
                .get(&ObjectKey::new("tenant", "bucket", "objects/detached").unwrap())
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(queue.settlement_attempts() - settlements_before, 1);
        let status = store.local_watch_status().unwrap();
        assert_eq!(status.settled_through, status.tail);
    }
}
