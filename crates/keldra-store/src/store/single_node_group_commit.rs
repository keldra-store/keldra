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

const MAX_GROUP_REQUESTS: usize = 5;
const MAX_GROUP_OPERATIONS: usize = 5_000;
const MAX_GROUP_INLINE_BYTES: usize = 64 * 1024 * 1024;
const MAX_QUEUED_REQUESTS: usize = 64;
const MAX_QUEUED_OPERATIONS: usize = 8_000;
const MAX_QUEUED_INLINE_BYTES: usize = 128 * 1024 * 1024;
const MAX_GROUP_DWELL: Duration = Duration::from_micros(250);

pub(super) type SingleNodeOperations = Vec<(
    BatchOperation,
    ObjectMutationGovernance,
    Option<DefinitionMutationIntent>,
)>;
pub(super) type SingleNodeOutcomes =
    Result<Vec<Result<CoordinatedObjectMutation, MutationError>>, MutationError>;

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
    state: Arc<Mutex<QueueState>>,
    queue_slots: Arc<Semaphore>,
    operation_slots: Arc<Semaphore>,
    inline_byte_slots: Arc<Semaphore>,
}

impl Default for SingleNodeGroupCommit {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(QueueState::default())),
            queue_slots: Arc::new(Semaphore::new(MAX_QUEUED_REQUESTS)),
            operation_slots: Arc::new(Semaphore::new(MAX_QUEUED_OPERATIONS)),
            inline_byte_slots: Arc::new(Semaphore::new(MAX_QUEUED_INLINE_BYTES)),
        }
    }
}

impl SingleNodeGroupCommit {
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
        if operation_count > MAX_GROUP_OPERATIONS || inline_bytes > MAX_GROUP_INLINE_BYTES {
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
            tokio::time::sleep(MAX_GROUP_DWELL).await;
            let requests = {
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
                while group.len() < MAX_GROUP_REQUESTS {
                    let Some(candidate) = state.requests.front() else {
                        break;
                    };
                    let (Some(group_governance), Some(candidate_governance)) =
                        (governance.as_ref(), candidate.consistent_governance())
                    else {
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
                    if candidate.context != context
                        || !compatible_governance
                        || operations.saturating_add(candidate_operations) > MAX_GROUP_OPERATIONS
                        || inline_bytes.saturating_add(candidate_bytes) > MAX_GROUP_INLINE_BYTES
                    {
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
                group
            };

            let operation_counts = requests
                .iter()
                .map(SingleNodeCommitRequest::operation_count)
                .collect::<Vec<_>>();
            let context = requests[0].context;
            let mut operations = Vec::with_capacity(operation_counts.iter().sum());
            let mut replies = Vec::with_capacity(requests.len());
            for request in requests {
                operations.extend(request.operations);
                replies.push((request.response, request._queue_permits));
            }
            let results = store
                .coordinate_single_node_mutation_group(operations, context, &operation_counts)
                .await;
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
    async fn five_compatible_requests_share_one_physical_commit() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let governance = governance(&store);
        let before = store.db.latest_sequence_number();
        let (a, b, c, d, e) = tokio::join!(
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
                request("objects/e", "e", governance),
                context(1),
            ),
        );

        for result in [a, b, c, d, e] {
            assert!(matches!(result.as_deref(), Ok([Ok(_)])));
        }
        assert_eq!(physical_commits_since(&store, before), 1);
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
            immutable_prefixes: vec!["immutable/".into()],
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

        assert!(matches!(first.as_deref(), Ok([Ok(_)])));
        assert!(matches!(second.as_deref(), Ok([Ok(_)])));
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

        queue.run(store.clone()).await;

        assert!(
            store
                .get(&ObjectKey::new("tenant", "bucket", "objects/detached").unwrap())
                .await
                .unwrap()
                .is_some()
        );
    }
}
