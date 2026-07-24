use super::*;
use crate::{
    core_store::{
        CF_LEASES_FENCES, CoreMetaStore, CoreMutationBatchReceipt, CoreStore, CoreTransactionState,
        TABLE_TASK_CURRENT_ROW, core_meta_record_tuple_key,
    },
    partition_fence::partition_write_precondition,
    persistence::TaskRecord,
    storage::Storage,
    task_lease::STALE_FENCE,
    tasks::{TaskStatus, TaskType},
};
use chrono::Utc;
use serde_json::json;
use tempfile::tempdir;

async fn list_tasks(storage: &Storage) -> anyhow::Result<Vec<TaskRecord>> {
    let mut tasks = Vec::new();
    let mut after_tuple_key = None;
    loop {
        let page = list_tasks_page(storage, after_tuple_key.as_deref(), 128).await?;
        tasks.extend(page.tasks);
        let Some(next) = page.next_tuple_key else {
            break;
        };
        after_tuple_key = Some(next);
    }
    Ok(tasks)
}

#[tokio::test]
async fn task_pages_are_bounded_and_continue_by_physical_key() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for index in 0..5 {
        enqueue_task(
            &storage,
            TaskType::DeleteObject,
            json!({ "object_id": index }),
            0,
        )
        .await
        .unwrap();
    }

    let first = list_tasks_page(&storage, None, 2).await.unwrap();
    assert_eq!(first.tasks.len(), 2);
    let second = list_tasks_page(&storage, first.next_tuple_key.as_deref(), 2)
        .await
        .unwrap();
    assert_eq!(second.tasks.len(), 2);
    let third = list_tasks_page(&storage, second.next_tuple_key.as_deref(), 2)
        .await
        .unwrap();
    assert_eq!(third.tasks.len(), 1);
    assert!(third.next_tuple_key.is_none());

    let ids = first
        .tasks
        .into_iter()
        .chain(second.tasks)
        .chain(third.tasks)
        .map(|task| task.id)
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    assert!(list_tasks_page(&storage, None, 0).await.is_err());
    assert!(list_tasks_page(&storage, None, 1_001).await.is_err());
}

#[test]
fn failed_task_mutation_receipt_is_not_acknowledged() {
    let mut receipt = CoreMutationBatchReceipt {
        transaction_id: "task-receipt-failure".to_string(),
        scope_partition: "task_queue/global".to_string(),
        state: CoreTransactionState::FinalisationFailed,
        visible_updates: Vec::new(),
        finalisation_error: Some("CoreMetaPublicationTerminal: stale fence".to_string()),
    };
    let error = super::store::require_committed_task_mutation(&receipt).unwrap_err();
    assert!(format!("{error:#}").contains("CoreMetaPublicationTerminal: stale fence"));

    receipt.state = CoreTransactionState::Committed;
    receipt.finalisation_error = None;
    super::store::require_committed_task_mutation(&receipt).unwrap();
}

#[test]
fn failed_task_row_payload_cas_is_retryable() {
    let receipt = CoreMutationBatchReceipt {
        transaction_id: "task-receipt-conflict".to_string(),
        scope_partition: "task_queue/global".to_string(),
        state: CoreTransactionState::FinalisationFailed,
        visible_updates: Vec::new(),
        finalisation_error: Some(format!(
            "CoreMeta row {CF_LEASES_FENCES}/{TABLE_TASK_CURRENT_ROW:#06x}/01 precondition failed: payload hash mismatch: expected blake3:old, got blake3:new"
        )),
    };
    let error = super::store::require_committed_task_mutation(&receipt).unwrap_err();
    assert!(super::store::is_queue_cas_conflict(&error));

    let unrelated = anyhow::anyhow!(
        "CoreMeta row {CF_LEASES_FENCES}/0x0001/01 precondition failed: payload hash mismatch"
    );
    assert!(!super::store::is_queue_cas_conflict(&unrelated));
}

const KEY: &[u8] = b"task queue partition owner key";

#[tokio::test]
async fn pending_projection_orders_by_due_time_then_priority() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();

    enqueue_task(
        &storage,
        TaskType::DeleteBucket,
        json!({"bucket_id": 7}),
        100,
    )
    .await
    .unwrap();
    enqueue_task(
        &storage,
        TaskType::DeleteObject,
        json!({"object_id": 9}),
        10,
    )
    .await
    .unwrap();
    enqueue_task(
        &storage,
        TaskType::DeleteObject,
        json!({"object_id": 10}),
        1,
    )
    .await
    .unwrap();
    enqueue_task(
        &storage,
        TaskType::DeleteObject,
        json!({"object_id": 11}),
        1_000,
    )
    .await
    .unwrap();

    let due_at = Utc::now() - chrono::Duration::seconds(1);
    force_task_schedule_for_test(&storage, 1, due_at)
        .await
        .unwrap();
    force_task_schedule_for_test(&storage, 2, due_at)
        .await
        .unwrap();
    force_task_schedule_for_test(&storage, 3, Utc::now() + chrono::Duration::hours(1))
        .await
        .unwrap();
    force_task_schedule_for_test(&storage, 4, due_at - chrono::Duration::seconds(1))
        .await
        .unwrap();

    let claimed = claim_pending_tasks(&storage, 4).await.unwrap();
    assert_eq!(claimed.len(), 3);
    assert_eq!(
        claimed.iter().map(|task| task.id).collect::<Vec<_>>(),
        vec![4, 2, 1]
    );
    assert!(
        claimed
            .iter()
            .all(|task| task.status == TaskStatus::Running)
    );
}

#[tokio::test]
async fn each_task_transition_uses_one_post_generation_for_its_root_rows() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    enqueue_task(
        &storage,
        TaskType::DeleteObject,
        json!({"object_id": 7}),
        25,
    )
    .await
    .unwrap();

    let (enqueued, enqueued_generation) = task_entry_and_generation(&storage, 1);
    let pending = queue_row(
        &storage,
        &model::pending_key(&model::TaskOrder::from_task(&enqueued.task).unwrap()).unwrap(),
    );
    assert_eq!(pending.publication_generation, enqueued_generation);
    assert_eq!(
        task_journal_generations(&storage, 1),
        vec![enqueued_generation]
    );

    claim_pending_tasks(&storage, 1).await.unwrap();
    let (running, running_generation) = task_entry_and_generation(&storage, 1);
    assert_eq!(running_generation, enqueued_generation + 1);
    let running_projection = queue_row(&storage, &model::running_key(&running).unwrap());
    assert_eq!(
        running_projection.publication_generation,
        running_generation
    );
    assert_eq!(
        task_journal_generations(&storage, 1),
        vec![enqueued_generation, running_generation]
    );

    update_task_status(&storage, 1, TaskStatus::Completed)
        .await
        .unwrap();
    let (_, completed_generation) = task_entry_and_generation(&storage, 1);
    assert_eq!(completed_generation, running_generation + 1);
    assert_eq!(
        task_journal_generations(&storage, 1),
        vec![
            enqueued_generation,
            running_generation,
            completed_generation
        ]
    );
}

#[tokio::test]
async fn future_high_priority_task_does_not_block_due_lower_priority_work() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    enqueue_task(&storage, TaskType::DeleteObject, json!({"object_id": 1}), 1)
        .await
        .unwrap();
    force_task_schedule_for_test(&storage, 1, Utc::now() + chrono::Duration::hours(1))
        .await
        .unwrap();
    enqueue_task(
        &storage,
        TaskType::DeleteObject,
        json!({"object_id": 2}),
        50,
    )
    .await
    .unwrap();

    let claimed = claim_pending_tasks(&storage, 1).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, 2);
}

#[tokio::test]
async fn exact_live_task_head_is_atomic_and_released_on_completion() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let permit = owner.write_permit().unwrap();
    let payload = json!({"bucket_id": 7});

    let first = enqueue_task_if_absent_with_permit(
        &storage,
        TaskType::ObjectMetadataCompaction,
        payload.clone(),
        50,
        &permit,
        KEY,
    );
    let second = enqueue_task_if_absent_with_permit(
        &storage,
        TaskType::ObjectMetadataCompaction,
        payload.clone(),
        50,
        &permit,
        KEY,
    );
    let (first, second) = tokio::join!(first, second);
    assert_eq!(
        usize::from(first.unwrap()) + usize::from(second.unwrap()),
        1
    );

    let task = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
        .await
        .unwrap()
        .pop()
        .unwrap();
    update_task_status_with_permit(&storage, task.id, TaskStatus::Completed, &permit, KEY)
        .await
        .unwrap();
    assert!(
        enqueue_task_if_absent_with_permit(
            &storage,
            TaskType::ObjectMetadataCompaction,
            payload,
            50,
            &permit,
            KEY,
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn grouped_tasks_supersede_pending_work_and_serialize_followups() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let permit = owner.write_permit().unwrap();

    assert!(
        enqueue_index_build_task_with_permit(&storage, index_payload(2), 40, &permit, KEY,)
            .await
            .unwrap()
    );
    assert!(
        enqueue_index_build_task_with_permit(&storage, index_payload(5), 40, &permit, KEY,)
            .await
            .unwrap()
    );
    assert!(
        !enqueue_index_build_task_with_permit(&storage, index_payload(4), 40, &permit, KEY,)
            .await
            .unwrap()
    );

    let tasks = list_tasks(&storage).await.unwrap();
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].status, TaskStatus::Completed);
    assert_eq!(tasks[1].payload["source_cursor"], json!(5));

    let running = claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
        .await
        .unwrap();
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].payload["source_cursor"], json!(5));

    assert!(
        enqueue_index_build_task_with_permit(&storage, index_payload(8), 40, &permit, KEY,)
            .await
            .unwrap()
    );
    assert!(
        claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
            .await
            .unwrap()
            .is_empty(),
        "the grouped follow-up stays parked while its predecessor is running"
    );
    update_task_status_with_permit(&storage, running[0].id, TaskStatus::Completed, &permit, KEY)
        .await
        .unwrap();
    let (parked, parked_generation) = task_entry_and_generation(&storage, 3);
    let parked_projection = queue_row(
        &storage,
        &model::pending_key(&model::TaskOrder::from_task(&parked.task).unwrap()).unwrap(),
    );
    assert_ne!(
        parked_generation, parked_projection.publication_generation,
        "a grouped task's domain state may span independently published CoreMeta rows"
    );
    let followup = claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
        .await
        .unwrap();
    assert_eq!(followup.len(), 1);
    assert_eq!(followup[0].payload["source_cursor"], json!(8));
}

#[tokio::test]
async fn failed_task_retry_is_delayed_and_failure_is_idempotent() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    enqueue_task(
        &storage,
        TaskType::DeleteBucket,
        json!({"bucket_id": 7}),
        100,
    )
    .await
    .unwrap();
    let task = claim_pending_tasks(&storage, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();

    fail_task(&storage, task.id, "try again").await.unwrap();
    fail_task(&storage, task.id, "try again").await.unwrap();
    let failed = list_tasks(&storage).await.unwrap().pop().unwrap();
    assert_eq!(
        failed.attempts, 1,
        "a retried response must not double-fail"
    );
    assert!(claim_pending_tasks(&storage, 1).await.unwrap().is_empty());

    force_task_schedule_for_test(&storage, task.id, Utc::now() - chrono::Duration::seconds(1))
        .await
        .unwrap();
    let retried = claim_pending_tasks(&storage, 1).await.unwrap();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].id, task.id);
    assert_eq!(retried[0].attempts, 1);
}

#[tokio::test]
async fn completed_history_does_not_increase_enqueue_or_claim_row_visits() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();

    for object_id in 0..32 {
        enqueue_task(
            &storage,
            TaskType::DeleteObject,
            json!({"object_id": object_id}),
            50,
        )
        .await
        .unwrap();
        let task = claim_pending_tasks(&storage, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        update_task_status(&storage, task.id, TaskStatus::Completed)
            .await
            .unwrap();
    }

    reset_task_row_visits_for_test();
    enqueue_task(
        &storage,
        TaskType::DeleteObject,
        json!({"object_id": "bounded"}),
        10,
    )
    .await
    .unwrap();
    let enqueue_visits = task_row_visits_for_test();
    assert!(
        enqueue_visits <= 8,
        "enqueue visited {enqueue_visits} rows after retained history"
    );

    reset_task_row_visits_for_test();
    let claimed = claim_pending_tasks(&storage, 1).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let claim_visits = task_row_visits_for_test();
    assert!(
        claim_visits <= 12,
        "single-task claim visited {claim_visits} rows after retained history"
    );
}

#[tokio::test]
async fn next_due_seek_is_constant_with_retained_tasks_and_pending_priorities() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let mut next_task_id = 1_i64;

    for object_id in 0..8 {
        enqueue_task(
            &storage,
            TaskType::DeleteObject,
            json!({"object_id": format!("retained-{object_id}")}),
            50,
        )
        .await
        .unwrap();
        let task = claim_pending_tasks(&storage, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(task.id, next_task_id);
        update_task_status(&storage, task.id, TaskStatus::Completed)
            .await
            .unwrap();
        next_task_id += 1;
    }

    let future = Utc::now() + chrono::Duration::hours(1);
    for priority in 0..24 {
        enqueue_task(
            &storage,
            TaskType::DeleteObject,
            json!({"object_id": format!("future-{priority}")}),
            priority,
        )
        .await
        .unwrap();
        force_task_schedule_for_test(&storage, next_task_id, future)
            .await
            .unwrap();
        next_task_id += 1;
    }

    enqueue_task(
        &storage,
        TaskType::DeleteObject,
        json!({"object_id": "due"}),
        10_000,
    )
    .await
    .unwrap();

    reset_task_row_visits_for_test();
    let core_store = CoreStore::new(storage.clone()).await.unwrap();
    let due = store::QueueStore::open(&storage)
        .unwrap()
        .first_due_task(&core_store, Utc::now())
        .unwrap()
        .unwrap();
    assert_eq!(due.task.id, next_task_id);
    assert_eq!(
        task_row_visits_for_test(),
        2,
        "next-due selection must read one pending row and one current task row"
    );
}

#[tokio::test]
async fn concurrent_claims_of_one_pending_task_have_one_winner() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    enqueue_task(
        &storage,
        TaskType::DeleteObject,
        json!({"object_id": "single-winner"}),
        50,
    )
    .await
    .unwrap();

    let first = claim_pending_tasks(&storage, 1);
    let second = claim_pending_tasks(&storage, 1);
    let (first, second) = tokio::join!(first, second);
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.len() + second.len(), 1);
    assert_eq!(
        first
            .into_iter()
            .chain(second)
            .map(|task| task.id)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert!(claim_pending_tasks(&storage, 1).await.unwrap().is_empty());
}

#[tokio::test]
async fn independent_running_tasks_complete_concurrently_without_queue_lock() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for object_id in 0..12 {
        enqueue_task(
            &storage,
            TaskType::DeleteObject,
            json!({"object_id": object_id}),
            object_id,
        )
        .await
        .unwrap();
    }
    let running = claim_pending_tasks(&storage, 12).await.unwrap();
    let completions = running.into_iter().map(|task| {
        let storage = storage.clone();
        tokio::spawn(
            async move { update_task_status(&storage, task.id, TaskStatus::Completed).await },
        )
    });
    for completion in completions {
        completion.await.unwrap().unwrap();
    }
    assert!(
        list_tasks(&storage)
            .await
            .unwrap()
            .iter()
            .all(|task| task.status == TaskStatus::Completed)
    );
}

#[tokio::test]
async fn fenced_task_mutations_reject_stale_owner_and_record_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let permit = owner.write_permit().unwrap();
    enqueue_task_with_permit(
        &storage,
        TaskType::DeleteBucket,
        json!({"bucket_id": 7}),
        100,
        &permit,
        KEY,
    )
    .await
    .unwrap();
    let claimed = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
        .await
        .unwrap();
    update_task_status_with_permit(&storage, claimed[0].id, TaskStatus::Completed, &permit, KEY)
        .await
        .unwrap();
    let fences = read_task_frame_fences_for_test(&storage).await.unwrap();
    assert_eq!(fences, vec![permit.fence_token; 3]);

    let stale = permit;
    let newer = ready_owner(&storage, "node-b").await;
    assert!(newer.fence_token > stale.fence_token);
    let error = enqueue_task_with_permit(
        &storage,
        TaskType::DeleteBucket,
        json!({"bucket_id": 8}),
        100,
        &stale,
        KEY,
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("write permit owner is not current")
    );
}

#[tokio::test]
async fn stale_partition_precondition_cannot_commit_task_transition() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let stale = owner.write_permit().unwrap();
    enqueue_task_with_permit(
        &storage,
        TaskType::DeleteBucket,
        json!({"bucket_id": 7}),
        100,
        &stale,
        KEY,
    )
    .await
    .unwrap();
    let stale_precondition = partition_write_precondition(&storage, &stale, KEY)
        .await
        .unwrap();
    let newer = ready_owner(&storage, "node-b").await;
    assert!(newer.fence_token > stale.fence_token);

    let mut mutation =
        store::TaskMutation::new(&storage, stale.fence_token, Some(stale_precondition)).unwrap();
    let mut entry = mutation.read_task(1).unwrap().unwrap();
    entry.task.status = TaskStatus::Completed;
    entry.task.updated_at = Utc::now();
    mutation
        .put(
            model::current_key(1).unwrap(),
            model::TaskQueueRow::Task(entry),
        )
        .unwrap();
    let error = mutation.commit().await.unwrap_err();
    assert!(
        error.to_string().contains("target mismatch")
            || error.to_string().contains("generation mismatch")
            || error.to_string().contains("precondition failed")
    );
}

#[tokio::test]
async fn stale_execution_attempt_cannot_fail_a_newly_claimed_retry() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let permit = owner.write_permit().unwrap();
    enqueue_task_with_permit(
        &storage,
        TaskType::DeleteBucket,
        json!({"bucket_id": 7}),
        100,
        &permit,
        KEY,
    )
    .await
    .unwrap();
    let first = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(first.attempts, 0);

    let first_guard = partition_write_precondition(&storage, &permit, KEY)
        .await
        .unwrap();
    fail_task_with_execution_guard(
        &storage,
        first.id,
        first.attempts,
        "first attempt expired",
        &permit,
        KEY,
        first_guard,
    )
    .await
    .unwrap();
    force_task_schedule_for_test(&storage, first.id, Utc::now())
        .await
        .unwrap();
    let retry = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(retry.attempts, 1);

    let stale_guard = partition_write_precondition(&storage, &permit, KEY)
        .await
        .unwrap();
    let error = fail_task_with_execution_guard(
        &storage,
        first.id,
        first.attempts,
        "late first-attempt recovery",
        &permit,
        KEY,
        stale_guard,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains(STALE_FENCE));

    let current = list_tasks(&storage).await.unwrap().pop().unwrap();
    assert_eq!(current.status, TaskStatus::Running);
    assert_eq!(current.attempts, retry.attempts);
}

fn queue_row(storage: &Storage, key: &[u8]) -> model::DecodedTaskQueueRow {
    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    let payload = meta
        .get(CF_LEASES_FENCES, TABLE_TASK_CURRENT_ROW, key)
        .unwrap()
        .expect("task queue row");
    model::decode_queue_row(&payload).unwrap()
}

fn task_entry_and_generation(storage: &Storage, task_id: i64) -> (model::TaskEntry, u64) {
    let decoded = queue_row(storage, &model::current_key(task_id).unwrap());
    let model::TaskQueueRow::Task(entry) = decoded.row else {
        panic!("task current key must contain a task row");
    };
    (entry, decoded.publication_generation)
}

fn task_journal_generations(storage: &Storage, task_id: i64) -> Vec<u64> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    let prefix = model::journal_prefix().unwrap();
    let mut cursor = None;
    let mut generations = Vec::new();
    loop {
        let rows = meta
            .scan_prefix_page(
                CF_LEASES_FENCES,
                TABLE_TASK_CURRENT_ROW,
                &prefix,
                cursor.as_deref(),
                128,
            )
            .unwrap();
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            let tuple_key = core_meta_record_tuple_key(&row.key).unwrap();
            let decoded = model::decode_queue_row(&row.payload).unwrap();
            let model::TaskQueueRow::Journal(entry) = decoded.row else {
                panic!("task journal prefix must contain journal rows");
            };
            assert_eq!(
                tuple_key,
                model::journal_key(entry.task_id, &entry.mutation_id, entry.ordinal).unwrap()
            );
            if entry.task_id == task_id {
                generations.push(decoded.publication_generation);
            }
        }
        if rows.len() < 128 {
            break;
        }
        cursor = Some(
            core_meta_record_tuple_key(&rows.last().unwrap().key)
                .unwrap()
                .to_vec(),
        );
    }
    generations.sort_unstable();
    generations
}

fn index_payload(cursor: u64) -> serde_json::Value {
    json!({
        "tenant_id": 1,
        "bucket_id": 7,
        "index_id": 9,
        "index_version": 3,
        "source_cursor": cursor,
    })
}

async fn ready_owner(
    storage: &Storage,
    owner_node_id: &str,
) -> crate::partition_fence::PartitionOwnerState {
    crate::partition_fence::ready_partition_owner_for_test(
        storage,
        "task_queue".to_string(),
        hex::encode(task_queue_partition_id()),
        owner_node_id,
        0,
        hex::encode([0; 32]),
        hex::encode([1; 32]),
        KEY,
    )
    .await
}
