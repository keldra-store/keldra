use super::*;
use crate::{Durability, ObjectKey, PlacementLogId, PutMode, PutRequest};

impl SingleNodeGroupCommit {
    pub(in crate::store) fn pause_next_group_registration(&self) {
        let ticket = self.next_arrival_ticket.load(Ordering::Acquire) + 1;
        self.pause_registration_ticket
            .store(ticket, Ordering::Release);
    }

    pub(in crate::store) async fn wait_for_paused_group_registration(&self) {
        self.registration_paused.acquire().await.unwrap().forget();
    }

    pub(in crate::store) fn resume_group_registration(&self) {
        self.registration_continue.add_permits(1);
    }

    pub(in crate::store) async fn wait_for_registration_predecessor(&self) {
        self.registration_predecessor_waiting
            .acquire()
            .await
            .unwrap()
            .forget();
    }

    pub(in crate::store) async fn reserve_all_execution_slots(
        &self,
    ) -> tokio::sync::OwnedSemaphorePermit {
        self.execution_slots
            .clone()
            .acquire_many_owned(self.config.commit_lanes as u32)
            .await
            .unwrap()
    }
}

pub(super) fn context(term: u64) -> ObjectMutationContext {
    ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term, index: term },
        serving_fence_term: term,
    }
}

pub(super) fn governance(store: &Store) -> ObjectMutationGovernance {
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    ObjectMutationGovernance {
        tenant_id,
        bucket_id,
        versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
        policy: store.bucket_policy("tenant", "bucket").unwrap(),
    }
}

pub(super) fn governance_stub() -> ObjectMutationGovernance {
    ObjectMutationGovernance {
        tenant_id: 1,
        bucket_id: 1,
        versioning: Default::default(),
        policy: Default::default(),
    }
}

pub(super) fn request(
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

pub(super) fn physical_commits_since(store: &Store, sequence: u64) -> usize {
    store
        .db
        .get_updates_since(sequence)
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
        .len()
}

pub(super) fn queued_request(
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
            BatchOperation::Publish(_) | BatchOperation::Clone(_) | BatchOperation::Delete(_) => 0,
        })
        .sum::<usize>();
    let (response, received) = oneshot::channel();
    drop(received);
    SingleNodeCommitRequest {
        operations,
        context,
        source_journal_admission: SourceJournalAdmission::Bounded,
        mode: MutationGroupMode::SingleNode,
        arrival_ticket: queue.next_arrival_ticket.fetch_add(1, Ordering::AcqRel) + 1,
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
