//! Bounded cancellation-safe group commit for single-node coordinator calls.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{Mutex, Notify, Semaphore, oneshot};
use tokio::time::Instant;

use super::Store;
use crate::{
    BatchOperation, CoordinatedObjectMutation, DefinitionMutationIntent, MutationError,
    ObjectMutationContext, ObjectMutationGovernance,
};

const DEFAULT_MAX_GROUP_REQUESTS: usize = 16;
const DEFAULT_MAX_GROUP_OPERATIONS: usize = 5_000;
const DEFAULT_MAX_GROUP_INLINE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_QUEUED_REQUESTS: usize = 64;
const DEFAULT_MAX_QUEUED_OPERATIONS: usize = 8_000;
const DEFAULT_MAX_QUEUED_INLINE_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_MAX_GROUP_DWELL: Duration = Duration::from_micros(250);
const DEFAULT_COMMIT_LANES: usize = 4;
const MAX_COMMIT_LANES: usize = 256;

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
    commit_lanes: usize,
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
            commit_lanes: DEFAULT_COMMIT_LANES,
        })
    }

    /// Sets the bounded number of independent local commit queues. Requests
    /// for the same primary exact path are always assigned to the same lane.
    pub fn with_commit_lanes(mut self, commit_lanes: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(commit_lanes != 0, "commit lanes must be non-zero");
        anyhow::ensure!(
            commit_lanes <= MAX_COMMIT_LANES,
            "commit lanes exceed the supported maximum of {MAX_COMMIT_LANES}"
        );
        self.commit_lanes = commit_lanes;
        Ok(self)
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

    pub fn commit_lanes(&self) -> usize {
        self.commit_lanes
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
    enqueued_at: Instant,
    admission_wait: Duration,
    request_slot_wait: Duration,
    operation_slot_wait: Duration,
    inline_byte_slot_wait: Duration,
    enqueue_lock_wait: Duration,
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
    peak_depth: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GroupPlan {
    request_count: usize,
    stop_reason: &'static str,
}

enum QueueAction {
    Empty,
    Group {
        requests: Vec<SingleNodeCommitRequest>,
        lane_queued_requests: usize,
        lane_peak_queued_requests: usize,
        stop_reason: &'static str,
    },
    WaitUntil(Instant),
}

#[derive(Clone)]
pub(super) struct SingleNodeGroupCommit {
    config: SingleNodeGroupCommitConfig,
    states: Arc<Vec<Mutex<QueueState>>>,
    queue_changed: Arc<Vec<Notify>>,
    queue_slots: Arc<Semaphore>,
    operation_slots: Arc<Semaphore>,
    inline_byte_slots: Arc<Semaphore>,
    queued_requests_total: Arc<AtomicUsize>,
    queued_requests_peak: Arc<AtomicUsize>,
}

impl SingleNodeGroupCommit {
    fn record_enqueued_request(&self) -> usize {
        let total = self.queued_requests_total.fetch_add(1, Ordering::AcqRel) + 1;
        self.queued_requests_peak.fetch_max(total, Ordering::AcqRel);
        total
    }

    fn record_dequeued_requests(&self, count: usize) -> usize {
        let mut current = self.queued_requests_total.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_sub(count)
                .expect("dequeued request count must not exceed queued request count");
            match self.queued_requests_total.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return next,
                Err(observed) => current = observed,
            }
        }
    }

    pub(super) fn new(config: SingleNodeGroupCommitConfig) -> Self {
        let states = (0..config.commit_lanes)
            .map(|_| Mutex::new(QueueState::default()))
            .collect();
        let queue_changed = (0..config.commit_lanes).map(|_| Notify::new()).collect();
        Self {
            queue_slots: Arc::new(Semaphore::new(config.max_queued_requests)),
            operation_slots: Arc::new(Semaphore::new(config.max_queued_operations)),
            inline_byte_slots: Arc::new(Semaphore::new(config.max_queued_inline_bytes)),
            queued_requests_total: Arc::new(AtomicUsize::new(0)),
            queued_requests_peak: Arc::new(AtomicUsize::new(0)),
            config,
            states: Arc::new(states),
            queue_changed: Arc::new(queue_changed),
        }
    }

    #[cfg(test)]
    async fn wait_until_idle(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let mut active = false;
                for state in self.states.iter() {
                    active |= state.lock().await.worker_running;
                }
                if !active {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("single-node group worker must become idle");
    }

    fn lane_for(&self, operations: &SingleNodeOperations) -> usize {
        let Some((operation, governance, _)) = operations.first() else {
            return 0;
        };
        let path = match operation {
            BatchOperation::Put(request) => request.key.path(),
            BatchOperation::Publish(request) => request.key.path(),
            BatchOperation::Clone(request) => request.destination.path(),
            BatchOperation::Delete(request) => request.key.path(),
        };
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in governance
            .tenant_id
            .to_be_bytes()
            .into_iter()
            .chain(governance.bucket_id.to_be_bytes())
            .chain(path.bytes())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        let lane_count = u64::try_from(self.states.len()).expect("lane count fits u64");
        usize::try_from(hash % lane_count).expect("lane index fits usize")
    }

    fn plan_group(&self, requests: &VecDeque<SingleNodeCommitRequest>) -> Option<GroupPlan> {
        let first = requests.front()?;
        let mut request_count = 1;
        let mut operations = first.operation_count();
        let mut inline_bytes = first.inline_bytes();
        let context = first.context;
        let Some(mut governance) = first.consistent_governance() else {
            return Some(GroupPlan {
                request_count,
                stop_reason: "inconsistent_governance",
            });
        };
        let stop_reason = loop {
            if request_count >= self.config.max_group_requests {
                break "max_requests";
            }
            if operations >= self.config.max_group_operations {
                break "operations";
            }
            if inline_bytes >= self.config.max_group_inline_bytes {
                break "inline_bytes";
            }
            let Some(candidate) = requests.get(request_count) else {
                break "queue_empty";
            };
            let Some(candidate_governance) = candidate.consistent_governance() else {
                break "inconsistent_governance";
            };
            let candidate_operations = candidate.operation_count();
            let candidate_bytes = candidate.inline_bytes();
            let compatible_governance = candidate_governance.iter().all(|(identity, value)| {
                governance
                    .get(identity)
                    .is_none_or(|existing| existing == value)
            });
            if candidate.context != context {
                break "context";
            }
            if !compatible_governance {
                break "governance";
            }
            if operations.saturating_add(candidate_operations) > self.config.max_group_operations {
                break "operations";
            }
            if inline_bytes.saturating_add(candidate_bytes) > self.config.max_group_inline_bytes {
                break "inline_bytes";
            }
            operations = operations.saturating_add(candidate_operations);
            inline_bytes = inline_bytes.saturating_add(candidate_bytes);
            for (identity, value) in candidate_governance {
                governance.entry(identity).or_insert(value);
            }
            request_count += 1;
        };
        Some(GroupPlan {
            request_count,
            stop_reason,
        })
    }

    fn next_queue_action(&self, state: &mut QueueState, now: Instant) -> QueueAction {
        let Some(plan) = self.plan_group(&state.requests) else {
            state.worker_running = false;
            return QueueAction::Empty;
        };
        if plan.stop_reason == "queue_empty" {
            let deadline = state.requests[0]
                .enqueued_at
                .checked_add(self.config.max_group_dwell)
                .unwrap_or(now);
            if now < deadline {
                return QueueAction::WaitUntil(deadline);
            }
        }
        let requests = state
            .requests
            .drain(..plan.request_count)
            .collect::<Vec<_>>();
        QueueAction::Group {
            requests,
            lane_queued_requests: state.requests.len(),
            lane_peak_queued_requests: state.peak_depth,
            stop_reason: plan.stop_reason,
        }
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
        let admission_started = Instant::now();
        let request_slot_started = Instant::now();
        let request_permit = self
            .queue_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MutationError::Storage("single-node commit queue closed".into()))?;
        let request_slot_wait = request_slot_started.elapsed();
        let operation_slot_started = Instant::now();
        let operation_permit = self
            .operation_slots
            .clone()
            .acquire_many_owned(operation_count as u32)
            .await
            .map_err(|_| {
                MutationError::Storage("single-node commit operation queue closed".into())
            })?;
        let operation_slot_wait = operation_slot_started.elapsed();
        let inline_byte_slot_started = Instant::now();
        let inline_byte_permit = self
            .inline_byte_slots
            .clone()
            .acquire_many_owned(inline_bytes as u32)
            .await
            .map_err(|_| MutationError::Storage("single-node commit byte queue closed".into()))?;
        let inline_byte_slot_wait = inline_byte_slot_started.elapsed();
        let admission_wait = admission_started.elapsed();
        let (response, received) = oneshot::channel();
        let lane = self.lane_for(&operations);
        let enqueue_lock_started = Instant::now();
        let start_worker = {
            let mut state = self.states[lane].lock().await;
            let enqueue_lock_wait = enqueue_lock_started.elapsed();
            state.requests.push_back(SingleNodeCommitRequest {
                operations,
                context,
                enqueued_at: Instant::now(),
                admission_wait,
                request_slot_wait,
                operation_slot_wait,
                inline_byte_slot_wait,
                enqueue_lock_wait,
                response,
                _queue_permits: QueuePermits {
                    _request: request_permit,
                    _operations: operation_permit,
                    _inline_bytes: inline_byte_permit,
                },
            });
            state.peak_depth = state.peak_depth.max(state.requests.len());
            self.record_enqueued_request();
            if state.worker_running {
                false
            } else {
                state.worker_running = true;
                true
            }
        };
        if start_worker {
            let queue = self.clone();
            tokio::spawn(async move { queue.run(store, lane).await });
        } else {
            self.queue_changed[lane].notify_one();
        }
        received.await.unwrap_or_else(|_| {
            Err(MutationError::Storage(
                "single-node commit worker stopped before replying".into(),
            ))
        })
    }

    async fn run(self, store: Store, lane: usize) {
        loop {
            let dwell_started = Instant::now();
            let (requests, lane_queued_requests, lane_peak_queued_requests, stop_reason) = loop {
                let notified = self.queue_changed[lane].notified();
                let action = {
                    let mut state = self.states[lane].lock().await;
                    self.next_queue_action(&mut state, Instant::now())
                };
                match action {
                    QueueAction::Empty => return,
                    QueueAction::Group {
                        requests,
                        lane_queued_requests,
                        lane_peak_queued_requests,
                        stop_reason,
                    } => {
                        break (
                            requests,
                            lane_queued_requests,
                            lane_peak_queued_requests,
                            stop_reason,
                        );
                    }
                    QueueAction::WaitUntil(deadline) => {
                        tokio::select! {
                            () = notified => {}
                            () = tokio::time::sleep_until(deadline) => {}
                        }
                    }
                }
            };
            let total_queued_requests = self.record_dequeued_requests(requests.len());
            let total_peak_queued_requests = self.queued_requests_peak.load(Ordering::Acquire);
            let dwell_duration = dwell_started.elapsed();

            let request_count = requests.len();
            let admission_wait_seconds = requests
                .iter()
                .map(|request| request.admission_wait.as_secs_f64())
                .sum::<f64>();
            let admission_wait_max_seconds = requests
                .iter()
                .map(|request| request.admission_wait)
                .max()
                .unwrap_or_default()
                .as_secs_f64();
            let request_slot_wait_seconds = requests
                .iter()
                .map(|request| request.request_slot_wait.as_secs_f64())
                .sum::<f64>();
            let operation_slot_wait_seconds = requests
                .iter()
                .map(|request| request.operation_slot_wait.as_secs_f64())
                .sum::<f64>();
            let inline_byte_slot_wait_seconds = requests
                .iter()
                .map(|request| request.inline_byte_slot_wait.as_secs_f64())
                .sum::<f64>();
            let enqueue_lock_wait_seconds = requests
                .iter()
                .map(|request| request.enqueue_lock_wait.as_secs_f64())
                .sum::<f64>();
            let enqueue_to_group_seconds = requests
                .iter()
                .map(|request| request.enqueued_at.elapsed().as_secs_f64())
                .sum::<f64>();
            let enqueue_to_group_max_seconds = requests
                .iter()
                .map(|request| request.enqueued_at.elapsed())
                .max()
                .unwrap_or_default()
                .as_secs_f64();
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
            let group_execute_started_epoch_milliseconds = unix_milliseconds();
            let execute_started = std::time::Instant::now();
            let (results, metrics) = store
                .coordinate_single_node_mutation_group(operations, context, &operation_counts)
                .await;
            let execute_duration = execute_started.elapsed();
            let group_execute_ended_epoch_milliseconds = unix_milliseconds();
            let failed_requests = results.iter().filter(|result| result.is_err()).count();
            let metrics = metrics.unwrap_or_default();
            let evaluation_uncategorized = metrics
                .evaluate
                .saturating_sub(metrics.evaluation_subphases.categorized());
            tracing::info!(
                target: "keldra_store::single_node_group_commit_phases",
                attempts = 1_u64,
                physical_commits = metrics.physical_commit as u64,
                commit_lane = lane,
                request_count,
                operation_count,
                inline_bytes,
                failed_requests,
                group_execute_started_epoch_milliseconds,
                group_execute_ended_epoch_milliseconds,
                admission_wait_sum_seconds = admission_wait_seconds,
                admission_wait_max_seconds,
                request_slot_wait_sum_seconds = request_slot_wait_seconds,
                operation_slot_wait_sum_seconds = operation_slot_wait_seconds,
                inline_byte_slot_wait_sum_seconds = inline_byte_slot_wait_seconds,
                enqueue_lock_wait_sum_seconds = enqueue_lock_wait_seconds,
                enqueue_to_group_sum_seconds = enqueue_to_group_seconds,
                enqueue_to_group_max_seconds,
                dwell_seconds = dwell_duration.as_secs_f64(),
                execute_seconds = execute_duration.as_secs_f64(),
                prepare_seconds = metrics.prepare.as_secs_f64(),
                policy_wait_seconds = metrics.policy_wait.as_secs_f64(),
                path_wait_seconds = metrics.path_wait.as_secs_f64(),
                commit_wait_seconds = metrics.commit_wait.as_secs_f64(),
                lane_fence_wait_seconds = metrics.lane_fence_wait.as_secs_f64(),
                lane_conflict_lock_wait_seconds = metrics.lane_conflict_wait.as_secs_f64(),
                physical_slot_wait_seconds = metrics.physical_slot_wait.as_secs_f64(),
                physical_slots_active_at_acquire = metrics.physical_slots_active_at_acquire,
                physical_slots_active_before_release = metrics.physical_slots_active_before_release,
                physical_slots_peak_since_start_at_acquire = metrics
                    .physical_slots_peak_since_start_at_acquire,
                physical_slots_peak_since_start_before_release = metrics
                    .physical_slots_peak_since_start_before_release,
                physical_slot_count = metrics.physical_slot_count,
                first_sequence_wait_seconds = metrics.first_sequence_wait.as_secs_f64(),
                first_sequence_hold_seconds = metrics.first_sequence_hold.as_secs_f64(),
                locked_setup_seconds = metrics.locked_setup.as_secs_f64(),
                baseline_prefetch_seconds = metrics.baseline_prefetch.as_secs_f64(),
                baseline_revalidation_retries = metrics.baseline_revalidation_retries,
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
                db_write_sync_seconds = metrics.primary_db_write.as_secs_f64(),
                persistence_and_ordered_settlement_seconds = metrics.persist.as_secs_f64(),
                primary_db_write_seconds = metrics.primary_db_write.as_secs_f64(),
                completion_sequence_wait_seconds = metrics.completion_sequence_wait.as_secs_f64(),
                prior_retry_projection_db_write_seconds = metrics
                    .prior_retry_projection_db_write
                    .as_secs_f64(),
                completion_projection_db_write_seconds = metrics
                    .completion_projection_db_write
                    .as_secs_f64(),
                ordered_frontier_wait_seconds = metrics.ordered_frontier_wait.as_secs_f64(),
                completion_reorder_depth = metrics.completion_reorder_depth,
                completion_ticket_lag = metrics.completion_ticket_lag,
                prior_retry_projection_completions = metrics.prior_retry_projection_completions,
                completion_projection_completions = metrics.completion_projection_completions,
                settlement_measured_component_sum_seconds = metrics
                    .completion_sequence_wait
                    .saturating_add(metrics.completion_projection_db_write)
                    .saturating_add(metrics.ordered_frontier_wait)
                    .as_secs_f64(),
                settle_seconds = metrics.settle.as_secs_f64(),
                commit_hold_seconds = metrics.commit_hold.as_secs_f64(),
                commit_path_composite_seconds = metrics.commit_hold.as_secs_f64(),
                store_seconds = metrics.total.as_secs_f64(),
                write_batch_entries = metrics.write_batch_entries,
                write_batch_bytes = metrics.write_batch_bytes,
                total_seconds = dwell_duration.saturating_add(execute_duration).as_secs_f64(),
                queued_requests = lane_queued_requests,
                lane_queued_requests,
                lane_peak_queued_requests_since_start = lane_peak_queued_requests,
                total_queued_requests,
                total_peak_queued_requests_since_start = total_peak_queued_requests,
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

fn unix_milliseconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::{
        BucketPolicy, DestinationReferenceArtifact, DestinationReferenceDelta, Durability,
        MutationReceiptRetention, ObjectKey, PlacementLogId, PutMode, PutRequest,
        ReferenceDeltaBatch, StoreOptions,
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

    fn governance_stub() -> ObjectMutationGovernance {
        ObjectMutationGovernance {
            tenant_id: 1,
            bucket_id: 1,
            versioning: Default::default(),
            policy: Default::default(),
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

    fn queued_request(
        queue: &SingleNodeGroupCommit,
        operations: SingleNodeOperations,
        context: ObjectMutationContext,
        enqueued_at: Instant,
    ) -> SingleNodeCommitRequest {
        let operation_count = u32::try_from(operations.len()).unwrap();
        let inline_bytes = operations
            .iter()
            .map(|(operation, _, _)| match operation {
                BatchOperation::Put(request) => request.bytes.len(),
                BatchOperation::Publish(_)
                | BatchOperation::Clone(_)
                | BatchOperation::Delete(_) => 0,
            })
            .sum::<usize>();
        let (response, received) = oneshot::channel();
        drop(received);
        SingleNodeCommitRequest {
            operations,
            context,
            enqueued_at,
            admission_wait: Duration::ZERO,
            request_slot_wait: Duration::ZERO,
            operation_slot_wait: Duration::ZERO,
            inline_byte_slot_wait: Duration::ZERO,
            enqueue_lock_wait: Duration::ZERO,
            response,
            _queue_permits: QueuePermits {
                _request: queue.queue_slots.clone().try_acquire_owned().unwrap(),
                _operations: queue
                    .operation_slots
                    .clone()
                    .try_acquire_many_owned(operation_count)
                    .unwrap(),
                _inline_bytes: queue
                    .inline_byte_slots
                    .clone()
                    .try_acquire_many_owned(u32::try_from(inline_bytes).unwrap())
                    .unwrap(),
            },
        }
    }

    #[tokio::test]
    async fn five_requests_share_one_commit_with_primary_settlement() {
        let temporary = tempfile::tempdir().unwrap();
        let config = SingleNodeGroupCommitConfig::default()
            .with_commit_lanes(1)
            .unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_single_node_group_commit(config),
        )
        .await
        .unwrap();
        let governance = governance(&store);
        let before = store.db.latest_sequence_number();
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
        .unwrap()
        .with_commit_lanes(1)
        .unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_single_node_group_commit(config),
        )
        .await
        .unwrap();
        let governance = governance(&store);
        let before = store.db.latest_sequence_number();
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
        let status = store.local_watch_status().unwrap();
        assert_eq!(
            store.reference_delta_cursor(status.source_id).unwrap(),
            status.tail
        );
    }

    #[tokio::test]
    async fn full_compatible_group_skips_its_dwell_deadline() {
        let temporary = tempfile::tempdir().unwrap();
        let config = SingleNodeGroupCommitConfig::new(
            2,
            DEFAULT_MAX_GROUP_OPERATIONS,
            DEFAULT_MAX_GROUP_INLINE_BYTES,
            DEFAULT_MAX_QUEUED_REQUESTS,
            DEFAULT_MAX_QUEUED_OPERATIONS,
            DEFAULT_MAX_QUEUED_INLINE_BYTES,
            Duration::from_secs(30),
        )
        .unwrap()
        .with_commit_lanes(1)
        .unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_single_node_group_commit(config),
        )
        .await
        .unwrap();
        let governance = governance(&store);
        let before = store.db.latest_sequence_number();

        let (first, second) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                store.coordinate_single_node_mutation_batch(
                    request("objects/full-a", "full-a", governance.clone()),
                    context(1),
                ),
                store.coordinate_single_node_mutation_batch(
                    request("objects/full-b", "full-b", governance),
                    context(1),
                ),
            )
        })
        .await
        .expect("a full compatible group must not wait for its dwell deadline");

        assert!(matches!(first.as_deref(), Ok([Ok(_)])));
        assert!(matches!(second.as_deref(), Ok([Ok(_)])));
        assert_eq!(physical_commits_since(&store, before), 1);
    }

    #[test]
    fn underfilled_group_waits_until_the_first_request_deadline() {
        let queue = SingleNodeGroupCommit::new(SingleNodeGroupCommitConfig::default());
        let enqueued_at = Instant::now();
        let deadline = enqueued_at + queue.config.max_group_dwell;
        let mut state = QueueState::default();
        state.requests.push_back(queued_request(
            &queue,
            request("objects/underfilled", "underfilled", governance_stub()),
            context(1),
            enqueued_at,
        ));

        for now in [enqueued_at, deadline - Duration::from_nanos(1)] {
            assert_eq!(
                match queue.next_queue_action(&mut state, now) {
                    QueueAction::WaitUntil(planned) => planned,
                    _ => panic!("an underfilled group must wait for another compatible request"),
                },
                deadline,
            );
        }
        assert!(matches!(
            queue.next_queue_action(&mut state, deadline),
            QueueAction::Group { requests, .. } if requests.len() == 1
        ));
    }

    #[test]
    fn group_planning_keeps_operation_and_inline_byte_bounds() {
        fn assert_limit(config: SingleNodeGroupCommitConfig, commands: [&str; 2], reason: &str) {
            let queue = SingleNodeGroupCommit::new(config);
            let enqueued_at = Instant::now();
            let mut state = QueueState::default();
            for (index, command) in commands.into_iter().enumerate() {
                state.requests.push_back(queued_request(
                    &queue,
                    request(
                        &format!("objects/bounded-{index}"),
                        command,
                        governance_stub(),
                    ),
                    context(1),
                    enqueued_at,
                ));
            }
            match queue.next_queue_action(&mut state, enqueued_at) {
                QueueAction::Group {
                    requests,
                    stop_reason,
                    ..
                } => {
                    assert_eq!(requests.len(), 1);
                    assert_eq!(stop_reason, reason);
                }
                _ => panic!("a bounded compatible prefix must proceed immediately"),
            }
            assert_eq!(state.requests.len(), 1);
        }

        assert_limit(
            SingleNodeGroupCommitConfig::new(
                16,
                1,
                DEFAULT_MAX_GROUP_INLINE_BYTES,
                DEFAULT_MAX_QUEUED_REQUESTS,
                DEFAULT_MAX_QUEUED_OPERATIONS,
                DEFAULT_MAX_QUEUED_INLINE_BYTES,
                Duration::from_secs(30),
            )
            .unwrap(),
            ["operation-a", "operation-b"],
            "operations",
        );
        assert_limit(
            SingleNodeGroupCommitConfig::new(
                16,
                DEFAULT_MAX_GROUP_OPERATIONS,
                3,
                DEFAULT_MAX_QUEUED_REQUESTS,
                DEFAULT_MAX_QUEUED_OPERATIONS,
                DEFAULT_MAX_QUEUED_INLINE_BYTES,
                Duration::from_secs(30),
            )
            .unwrap(),
            ["aa", "bb"],
            "inline_bytes",
        );
    }

    #[tokio::test]
    async fn oversized_requests_are_rejected_before_queueing_or_writing() {
        let temporary = tempfile::tempdir().unwrap();
        let config = SingleNodeGroupCommitConfig::new(
            16,
            1,
            3,
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
        let mut too_many = request("objects/too-many-a", "a", governance.clone());
        too_many.extend(request("objects/too-many-b", "b", governance.clone()));

        for result in [
            store
                .coordinate_single_node_mutation_batch(too_many, context(1))
                .await,
            store
                .coordinate_single_node_mutation_batch(
                    request("objects/too-large", "four", governance),
                    context(1),
                )
                .await,
        ] {
            assert!(matches!(
                result,
                Err(MutationError::InvalidObjectMutation(_))
            ));
        }
        assert_eq!(store.db.latest_sequence_number(), before);
        for state in store.single_node_group_commit.states.iter() {
            let state = state.lock().await;
            assert!(state.requests.is_empty());
            assert!(!state.worker_running);
        }
        assert_eq!(
            store
                .single_node_group_commit
                .queued_requests_total
                .load(Ordering::Acquire),
            0
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

    #[tokio::test]
    async fn exact_primary_path_selects_one_stable_bounded_lane() {
        let queue = SingleNodeGroupCommit::new(
            SingleNodeGroupCommitConfig::default()
                .with_commit_lanes(4)
                .unwrap(),
        );
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let governance = governance(&store);
        let first = request("objects/stable", "first", governance.clone());
        let second = request("objects/stable", "second", governance.clone());
        assert_eq!(queue.lane_for(&first), queue.lane_for(&second));

        let lanes = ["objects/a", "objects/b", "objects/c", "objects/d"]
            .into_iter()
            .map(|path| queue.lane_for(&request(path, path, governance.clone())))
            .collect::<BTreeSet<_>>();
        assert!(lanes.len() > 1);
        assert!(lanes.iter().all(|lane| *lane < queue.states.len()));
    }

    #[test]
    fn group_commit_config_rejects_invalid_lane_counts() {
        assert!(
            SingleNodeGroupCommitConfig::default()
                .with_commit_lanes(0)
                .is_err()
        );
        assert!(
            SingleNodeGroupCommitConfig::default()
                .with_commit_lanes(MAX_COMMIT_LANES + 1)
                .is_err()
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
    async fn deferred_reference_backlog_backpressures_inline_lanes() {
        let temporary = tempfile::tempdir().unwrap();
        let config = SingleNodeGroupCommitConfig::default()
            .with_commit_lanes(1)
            .unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_single_node_group_commit(config),
        )
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

        assert!(matches!(first, Err(MutationError::SourceJournalCapacity)));
        assert!(matches!(second, Err(MutationError::SourceJournalCapacity)));
        assert_eq!(physical_commits_since(&store, before_sequence), 0);
        let after = store.local_watch_status().unwrap();
        assert_eq!(after, before);
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
    async fn settlement_gap_remains_hidden_without_a_second_coordinator_write() {
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

        let gap_operation = request(
            "objects/settlement-gap",
            "settlement-gap",
            governance.clone(),
        );
        let gap_error = store
            .coordinate_single_node_mutation_batch_with_settlement(gap_operation, context(1))
            .await
            .unwrap_err();
        assert!(matches!(gap_error, MutationError::SourceJournalCapacity));
        let after_gap = store.local_watch_status().unwrap();
        assert_eq!(after_gap, before);

        let operation = request("objects/settlement-retry", "settlement-retry", governance);
        let committed = store
            .coordinate_single_node_mutation_batch_with_settlement(operation.clone(), context(1))
            .await
            .unwrap_err();
        assert!(matches!(committed, MutationError::SourceJournalCapacity));
        let committed_tail = store.local_watch_status().unwrap().tail;
        assert_eq!(committed_tail, after_gap.tail);

        let replay = store
            .coordinate_single_node_mutation_batch_with_settlement(operation, context(1))
            .await
            .unwrap_err();
        assert!(matches!(replay, MutationError::SourceJournalCapacity));
        let unsettled = store.local_watch_status().unwrap();
        assert_eq!(unsettled.settled_through, before.settled_through);

        let positions = (unsettled.settled_through + 1..=unsettled.tail).collect::<Vec<_>>();
        store
            .settle_source_journal_positions_if_contiguous(unsettled.source_id, &positions)
            .await
            .unwrap();
        let recovered = store.local_watch_status().unwrap();
        assert_eq!(recovered.settled_through, recovered.tail);
        store.single_node_group_commit.wait_until_idle().await;
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
        let status = store.local_watch_status().unwrap();
        assert_eq!(status.settled_through, status.tail);
    }

    #[tokio::test]
    async fn idle_settlement_is_not_regressed_by_the_next_lane_projection() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let governance = governance(&store);
        let first = store
            .coordinate_single_node_mutation_batch(
                request(
                    "objects/frontier-first",
                    "frontier-first",
                    governance.clone(),
                ),
                context(1),
            )
            .await
            .unwrap();
        assert!(matches!(first.as_slice(), [Ok(_)]));
        let first_status = store.local_watch_status().unwrap();
        let first_position = first_status.tail;
        assert_eq!(first_status.settled_through, first_position);

        let second = store
            .coordinate_single_node_mutation_batch(
                request("objects/frontier-second", "frontier-second", governance),
                context(1),
            )
            .await
            .unwrap();
        assert!(matches!(second.as_slice(), [Ok(_)]));
        let status = store.local_watch_status().unwrap();
        assert!(status.tail > first_position);
        assert_eq!(status.settled_through, status.tail);
    }

    #[tokio::test]
    async fn legacy_journal_authority_is_refreshed_before_the_next_lane() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let governance = governance(&store);
        let legacy = store
            .coordinate_distributed_mutation_batch(
                request("objects/legacy", "legacy", governance.clone()),
                context(1),
            )
            .await
            .unwrap();
        assert!(matches!(legacy.as_slice(), [Ok(_)]));
        let legacy_mutation = legacy[0]
            .as_ref()
            .unwrap()
            .mutation
            .as_ref()
            .expect("new distributed operation must carry its mutation");
        let legacy_status = store.local_watch_status().unwrap();
        let legacy_receipts = store.mutation_receipt_status().unwrap();
        assert!(legacy_status.settled_through < legacy_status.tail);
        let reference_after = store
            .reference_delta_cursor(legacy_status.source_id)
            .unwrap();
        store
            .apply_reference_deltas(ReferenceDeltaBatch {
                source: legacy_status.source_id,
                after: reference_after,
                through: legacy_status.tail,
                deltas: legacy_mutation
                    .reference_deltas
                    .iter()
                    .map(|delta| DestinationReferenceDelta {
                        artifact: DestinationReferenceArtifact::CompleteBlob(delta.blob.clone()),
                        change: delta.change,
                    })
                    .collect(),
            })
            .await
            .unwrap();
        let after_effects = store.local_watch_status().unwrap();
        let after_cursor = store
            .reference_delta_cursor(after_effects.source_id)
            .unwrap();
        store
            .apply_reference_deltas(ReferenceDeltaBatch {
                source: after_effects.source_id,
                after: after_cursor,
                through: after_effects.tail,
                deltas: Vec::new(),
            })
            .await
            .unwrap();
        let positions =
            (after_effects.settled_through + 1..=after_effects.tail).collect::<Vec<_>>();
        assert_eq!(
            store
                .settle_source_journal_positions_if_contiguous(after_effects.source_id, &positions)
                .await
                .unwrap(),
            Some(after_effects.tail)
        );

        let lane = store
            .coordinate_single_node_mutation_batch(
                request("objects/lane", "lane", governance),
                context(1),
            )
            .await
            .unwrap();
        assert!(matches!(lane.as_slice(), [Ok(_)]));
        let status = store.local_watch_status().unwrap();
        assert!(status.tail > legacy_status.tail);
        assert!(status.retained_entries > legacy_status.retained_entries);
        assert_eq!(
            store.mutation_receipt_status().unwrap().entries,
            legacy_receipts.entries + 1
        );
    }

    #[test]
    fn conflicting_governance_stops_the_current_group() {
        let queue = SingleNodeGroupCommit::new(SingleNodeGroupCommitConfig::default());
        let enqueued_at = Instant::now();
        let mut state = QueueState::default();
        let first = governance_stub();
        let mut second = first.clone();
        second.policy = BucketPolicy {
            immutable_prefixes: vec!["immutable".into()],
            ..BucketPolicy::default()
        };
        state.requests.push_back(queued_request(
            &queue,
            request("objects/first", "first", first),
            context(1),
            enqueued_at,
        ));
        state.requests.push_back(queued_request(
            &queue,
            request("objects/second", "second", second),
            context(1),
            enqueued_at,
        ));
        assert!(matches!(
            queue.next_queue_action(&mut state, enqueued_at),
            QueueAction::Group { requests, stop_reason: "governance", .. }
                if requests.len() == 1
        ));
        assert_eq!(state.requests.len(), 1);
    }

    #[test]
    fn incompatible_context_stops_the_current_group() {
        let queue = SingleNodeGroupCommit::new(SingleNodeGroupCommitConfig::default());
        let enqueued_at = Instant::now();
        let mut state = QueueState::default();
        let governance = governance_stub();
        state.requests.push_back(queued_request(
            &queue,
            request("objects/context-first", "context-first", governance.clone()),
            context(1),
            enqueued_at,
        ));
        state.requests.push_back(queued_request(
            &queue,
            request("objects/context-second", "context-second", governance),
            context(2),
            enqueued_at,
        ));
        assert!(matches!(
            queue.next_queue_action(&mut state, enqueued_at),
            QueueAction::Group { requests, stop_reason: "context", .. }
                if requests.len() == 1
        ));
        assert_eq!(state.requests.len(), 1);
    }

    #[test]
    fn queue_accounting_combines_lanes_without_wrapping() {
        let queue = SingleNodeGroupCommit::new(
            SingleNodeGroupCommitConfig::default()
                .with_commit_lanes(2)
                .unwrap(),
        );
        assert_eq!(queue.record_enqueued_request(), 1);
        assert_eq!(queue.record_enqueued_request(), 2);
        assert_eq!(queue.record_dequeued_requests(1), 1);
        assert_eq!(queue.record_dequeued_requests(1), 0);
        assert_eq!(queue.queued_requests_peak.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn dropped_receiver_does_not_cancel_an_admitted_commit() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let queue = store.single_node_group_commit.clone();
        let operations = request("objects/detached", "detached", governance(&store));
        let lane = queue.lane_for(&operations);
        let enqueued_at = Instant::now();
        {
            let mut state = queue.states[lane].lock().await;
            state
                .requests
                .push_back(queued_request(&queue, operations, context(1), enqueued_at));
            state.worker_running = true;
            queue.record_enqueued_request();
        }

        queue.clone().run(store.clone(), lane).await;

        assert!(
            store
                .get(&ObjectKey::new("tenant", "bucket", "objects/detached").unwrap())
                .await
                .unwrap()
                .is_some()
        );
        let status = store.local_watch_status().unwrap();
        assert_eq!(status.settled_through, status.tail);
        assert_eq!(queue.queued_requests_total.load(Ordering::Acquire), 0);
        assert_eq!(queue.queued_requests_peak.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn exclusive_receipt_pruning_refreshes_lane_capacity() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_mutation_receipt_retention(
                MutationReceiptRetention::new(1, 1, 1024 * 1024).unwrap(),
            ),
        )
        .await
        .unwrap();
        let governance = governance(&store);
        let first = store
            .coordinate_single_node_mutation_batch(
                request("objects/receipt-first", "receipt-first", governance.clone()),
                context(1),
            )
            .await
            .unwrap();
        assert!(matches!(first.as_slice(), [Ok(_)]));

        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert!(store.prune_expired_receipts_for_capacity().await.unwrap());

        let second = store
            .coordinate_single_node_mutation_batch(
                request("objects/receipt-second", "receipt-second", governance),
                context(1),
            )
            .await
            .unwrap();
        assert!(matches!(second.as_slice(), [Ok(_)]), "second={second:?}");
        assert_eq!(store.mutation_receipt_status().unwrap().entries, 1);
    }
}
