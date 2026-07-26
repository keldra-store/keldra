use super::{
    model::{
        LiveDedupeHead, PendingProjection, RunningProjection, TaskAllocator, TaskAuditEvent,
        TaskEntry, TaskGroupHead, TaskGroupIdentity, TaskOrder, TaskQueueRow, allocator_key,
        current_key, dedupe_key, group_key, pending_key, running_key, task_group_identity,
        task_identity_hash,
    },
    store::{QueueStore, TaskMutation, is_queue_cas_conflict, max_queue_cas_attempts},
};
use crate::{
    mvcc_bootstrap::MvccSubsystem,
    mvcc_transaction::{LogicalKey, PredicateKind},
    partition_fence::PartitionWritePermit,
    persistence::TaskRecord,
    task_lease::STALE_FENCE,
    tasks::{TaskStatus, TaskType},
};
use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

const MAX_CLAIM_PAGE: usize = 4096;

pub(crate) async fn enqueue_task_with_permit(
    mvcc: &MvccSubsystem,
    task_type: TaskType,
    payload: JsonValue,
    priority: i32,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_task_queue_permit(mvcc, permit)?;
    enqueue_generic(
        mvcc,
        task_type,
        payload,
        priority,
        false,
        permit.fence_token,
    )
    .await
    .map(|_| ())
}

pub(crate) async fn enqueue_task_if_absent_with_permit(
    mvcc: &MvccSubsystem,
    task_type: TaskType,
    payload: JsonValue,
    priority: i32,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<bool> {
    require_task_queue_permit(mvcc, permit)?;
    enqueue_generic(mvcc, task_type, payload, priority, true, permit.fence_token).await
}

pub(crate) async fn enqueue_index_build_task_with_permit(
    mvcc: &MvccSubsystem,
    payload: JsonValue,
    priority: i32,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<bool> {
    enqueue_grouped_with_permit(
        mvcc,
        TaskType::IndexBuild,
        payload,
        priority,
        permit,
        partition_owner_signing_key,
    )
    .await
}

pub(crate) async fn enqueue_authz_materialization_task_with_permit(
    mvcc: &MvccSubsystem,
    payload: JsonValue,
    priority: i32,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<bool> {
    enqueue_grouped_with_permit(
        mvcc,
        TaskType::AuthzMaterialization,
        payload,
        priority,
        permit,
        partition_owner_signing_key,
    )
    .await
}

async fn enqueue_grouped_with_permit(
    mvcc: &MvccSubsystem,
    task_type: TaskType,
    payload: JsonValue,
    priority: i32,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<bool> {
    require_task_queue_permit(mvcc, permit)?;
    enqueue_grouped(mvcc, task_type, payload, priority, permit.fence_token).await
}

async fn enqueue_generic(
    mvcc: &MvccSubsystem,
    task_type: TaskType,
    payload: JsonValue,
    priority: i32,
    deduplicate: bool,
    fence_token: u64,
) -> Result<bool> {
    let dedupe_hash = deduplicate
        .then(|| task_identity_hash(task_type, &payload))
        .transpose()?;
    for attempt in 0..max_queue_cas_attempts() {
        let mut mutation = TaskMutation::new(mvcc, fence_token)?;
        if let Some(hash) = dedupe_hash.as_ref()
            && live_dedupe_task(&mut mutation, hash)?.is_some()
        {
            return Ok(false);
        }
        let task_id = reserve_task_id(&mut mutation)?;
        let now = Utc::now();
        let entry = TaskEntry {
            task: TaskRecord {
                id: task_id,
                task_type,
                payload: payload.clone(),
                priority,
                status: TaskStatus::Pending,
                attempts: 0,
                last_error: None,
                scheduled_at: now,
                created_at: now,
                updated_at: now,
            },
            dedupe_hash: dedupe_hash.clone(),
            group: None,
        };
        mutation.put(current_key(task_id)?, TaskQueueRow::Task(entry.clone()))?;
        add_pending_projection(&mut mutation, &entry)?;
        if let Some(hash) = dedupe_hash.as_ref() {
            mutation.put(
                dedupe_key(hash)?,
                TaskQueueRow::Dedupe(LiveDedupeHead {
                    dedupe_hash: hash.clone(),
                    task_id,
                }),
            )?;
        }
        mutation.audit(TaskAuditEvent::Enqueued {
            task: entry.task.clone(),
        });
        match mutation.commit().await {
            Ok(()) => return Ok(true),
            Err(error)
                if attempt + 1 < max_queue_cas_attempts() && is_queue_cas_conflict(&error) =>
            {
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("bounded task enqueue loop returns on its final attempt")
}

async fn enqueue_grouped(
    mvcc: &MvccSubsystem,
    task_type: TaskType,
    payload: JsonValue,
    priority: i32,
    fence_token: u64,
) -> Result<bool> {
    let group = task_group_identity(task_type, &payload)?
        .ok_or_else(|| anyhow!("grouped task type has no group identity"))?;
    for attempt in 0..max_queue_cas_attempts() {
        let mut mutation = TaskMutation::new(mvcc, fence_token)?;
        let mut head = read_group_head(&mut mutation, &group)?.unwrap_or(TaskGroupHead {
            kind: group.kind.clone(),
            group_hash: group.hash.clone(),
            running_task_id: None,
            pending_task_id: None,
        });

        if let Some(running_id) = head.running_task_id {
            let running = mutation
                .read_task(running_id)?
                .ok_or_else(|| anyhow!("task group references a missing running task"))?;
            if running.task.priority == priority
                && running
                    .group
                    .as_ref()
                    .is_some_and(|existing| existing.cursor >= group.cursor)
            {
                return Ok(false);
            }
        }
        if let Some(pending_id) = head.pending_task_id {
            let pending = mutation
                .read_task(pending_id)?
                .ok_or_else(|| anyhow!("task group references a missing pending task"))?;
            if pending.task.priority == priority
                && pending
                    .group
                    .as_ref()
                    .is_some_and(|existing| existing.cursor >= group.cursor)
            {
                return Ok(false);
            }
            if head.running_task_id.is_none() {
                remove_pending_projection(&mut mutation, &pending)?;
            }
            retire_superseded_task(&mut mutation, pending)?;
            head.pending_task_id = None;
        }

        let task_id = reserve_task_id(&mut mutation)?;
        let now = Utc::now();
        let entry = TaskEntry {
            task: TaskRecord {
                id: task_id,
                task_type,
                payload: payload.clone(),
                priority,
                status: TaskStatus::Pending,
                attempts: 0,
                last_error: None,
                scheduled_at: now,
                created_at: now,
                updated_at: now,
            },
            dedupe_hash: None,
            group: Some(group.clone()),
        };
        head.pending_task_id = Some(task_id);
        mutation.put(current_key(task_id)?, TaskQueueRow::Task(entry.clone()))?;
        if head.running_task_id.is_none() {
            add_pending_projection(&mut mutation, &entry)?;
        }
        save_group_head(&mut mutation, &group, &head)?;
        mutation.audit(TaskAuditEvent::Enqueued {
            task: entry.task.clone(),
        });
        match mutation.commit().await {
            Ok(()) => return Ok(true),
            Err(error)
                if attempt + 1 < max_queue_cas_attempts() && is_queue_cas_conflict(&error) =>
            {
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("bounded grouped task enqueue loop returns on its final attempt")
}

pub(crate) async fn claim_pending_tasks_with_permit(
    mvcc: &MvccSubsystem,
    limit: i64,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<Vec<TaskRecord>> {
    require_task_queue_permit(mvcc, permit)?;
    claim_pending_tasks_inner(mvcc, limit, permit.fence_token).await
}

async fn claim_pending_tasks_inner(
    mvcc: &MvccSubsystem,
    limit: i64,
    fence_token: u64,
) -> Result<Vec<TaskRecord>> {
    let limit = usize::try_from(limit.max(0)).map_err(|_| anyhow!("task claim limit overflow"))?;
    if limit > MAX_CLAIM_PAGE {
        bail!("task claim limit exceeds {MAX_CLAIM_PAGE}");
    }
    let mut claimed = Vec::with_capacity(limit);
    while claimed.len() < limit {
        let mut completed_slot = false;
        for attempt in 0..max_queue_cas_attempts() {
            let Some(candidate) = QueueStore::open(mvcc)?.first_due_task(Utc::now())? else {
                return Ok(claimed);
            };
            let mut mutation = TaskMutation::new(mvcc, fence_token)?;
            let Some(mut entry) = mutation.read_task(candidate.task.id)? else {
                continue;
            };
            if !matches!(entry.task.status, TaskStatus::Pending | TaskStatus::Failed)
                || TaskOrder::from_task(&entry.task)? != TaskOrder::from_task(&candidate.task)?
            {
                continue;
            }
            remove_pending_projection(&mut mutation, &entry)?;
            if let Some(group) = entry.group.as_ref() {
                let mut head = read_group_head(&mut mutation, group)?
                    .ok_or_else(|| anyhow!("claimable grouped task has no group head"))?;
                if head.running_task_id.is_some() || head.pending_task_id != Some(entry.task.id) {
                    bail!("claimable grouped task is inconsistent with its group head");
                }
                head.pending_task_id = None;
                head.running_task_id = Some(entry.task.id);
                save_group_head(&mut mutation, group, &head)?;
            }
            add_running_projection(&mut mutation, &entry)?;
            let now = Utc::now();
            entry.task.status = TaskStatus::Running;
            entry.task.updated_at = now;
            mutation.put(
                current_key(entry.task.id)?,
                TaskQueueRow::Task(entry.clone()),
            )?;
            mutation.audit(TaskAuditEvent::Claimed {
                task_id: entry.task.id,
                updated_at: now,
            });
            match mutation.commit().await {
                Ok(()) => {
                    claimed.push(entry.task);
                    completed_slot = true;
                    break;
                }
                Err(error)
                    if attempt + 1 < max_queue_cas_attempts() && is_queue_cas_conflict(&error) =>
                {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        if !completed_slot {
            bail!("task claim CAS retries exhausted");
        }
    }
    Ok(claimed)
}

pub(crate) async fn list_tasks_page(
    mvcc: &MvccSubsystem,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<crate::persistence::TaskPage> {
    QueueStore::open(mvcc)?.list_tasks_page(after_tuple_key, page_size)
}

pub(crate) async fn has_due_tasks(mvcc: &MvccSubsystem) -> Result<bool> {
    Ok(QueueStore::open(mvcc)?
        .first_due_task(Utc::now())?
        .is_some())
}

pub(crate) async fn update_task_status_with_permit(
    mvcc: &MvccSubsystem,
    task_id: i64,
    status: TaskStatus,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_task_queue_permit(mvcc, permit)?;
    update_task_status_inner(mvcc, task_id, status, permit.fence_token, None).await
}

pub(crate) async fn update_task_status_with_execution_guard(
    mvcc: &MvccSubsystem,
    task_id: i64,
    expected_attempts: i32,
    status: TaskStatus,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
    lease_predicate: (LogicalKey, PredicateKind),
) -> Result<()> {
    require_task_queue_permit(mvcc, permit)?;
    update_task_status_inner(
        mvcc,
        task_id,
        status,
        permit.fence_token,
        Some(lease_predicate),
        Some(expected_attempts),
    )
    .await
}

async fn update_task_status_inner(
    mvcc: &MvccSubsystem,
    task_id: i64,
    status: TaskStatus,
    fence_token: u64,
    additional_predicate: Option<(LogicalKey, PredicateKind)>,
    expected_running_attempts: Option<i32>,
) -> Result<()> {
    for attempt in 0..max_queue_cas_attempts() {
        let mut mutation = TaskMutation::new(mvcc, fence_token)?;
        if let Some(predicate) = additional_predicate.clone() {
            mutation.add_predicate(predicate);
        }
        let Some(mut entry) = mutation.read_task(task_id)? else {
            return Ok(());
        };
        require_running_attempt(&entry, expected_running_attempts)?;
        if entry.task.status == status {
            return Ok(());
        }
        let now = Utc::now();
        match status {
            TaskStatus::Completed => complete_entry(&mut mutation, &mut entry, now)?,
            TaskStatus::Pending | TaskStatus::Failed => {
                requeue_entry(&mut mutation, &mut entry, status, now)?
            }
            TaskStatus::Running => run_entry(&mut mutation, &mut entry, now)?,
        }
        mutation.audit(TaskAuditEvent::StatusUpdated {
            task_id,
            status: entry.task.status,
            updated_at: now,
        });
        match mutation.commit().await {
            Ok(()) => return Ok(()),
            Err(error)
                if attempt + 1 < max_queue_cas_attempts() && is_queue_cas_conflict(&error) =>
            {
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("bounded task status loop returns on its final attempt")
}

pub(crate) async fn fail_task_with_permit(
    mvcc: &MvccSubsystem,
    task_id: i64,
    error: &str,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_task_queue_permit(mvcc, permit)?;
    fail_task_inner(mvcc, task_id, error, permit.fence_token, None).await
}

pub(crate) async fn fail_task_with_execution_guard(
    mvcc: &MvccSubsystem,
    task_id: i64,
    expected_attempts: i32,
    error: &str,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
    lease_predicate: (LogicalKey, PredicateKind),
) -> Result<()> {
    require_task_queue_permit(mvcc, permit)?;
    fail_task_inner(
        mvcc,
        task_id,
        error,
        permit.fence_token,
        Some(lease_predicate),
        Some(expected_attempts),
    )
    .await
}

async fn fail_task_inner(
    mvcc: &MvccSubsystem,
    task_id: i64,
    error: &str,
    fence_token: u64,
    additional_predicate: Option<(LogicalKey, PredicateKind)>,
    expected_running_attempts: Option<i32>,
) -> Result<()> {
    for attempt in 0..max_queue_cas_attempts() {
        let mut mutation = TaskMutation::new(mvcc, fence_token)?;
        if let Some(predicate) = additional_predicate.clone() {
            mutation.add_predicate(predicate);
        }
        let Some(mut entry) = mutation.read_task(task_id)? else {
            return Ok(());
        };
        require_running_attempt(&entry, expected_running_attempts)?;
        if entry.task.status == TaskStatus::Completed {
            return Ok(());
        }
        if entry.task.status == TaskStatus::Failed
            && entry.task.last_error.as_deref() == Some(error)
        {
            return Ok(());
        }
        detach_active_projection(&mut mutation, &entry)?;
        let now = Utc::now();
        entry.task.attempts = entry.task.attempts.saturating_add(1);
        entry.task.last_error = Some(error.to_string());
        entry.task.status = TaskStatus::Failed;
        entry.task.updated_at = now;
        entry.task.scheduled_at = now
            + chrono::Duration::seconds(i64::from(
                entry
                    .task
                    .attempts
                    .saturating_mul(entry.task.attempts)
                    .saturating_mul(10),
            ));

        if let Some(group) = entry.group.as_ref() {
            let mut head = read_group_head(&mut mutation, group)?
                .ok_or_else(|| anyhow!("failed grouped task has no group head"))?;
            if head.running_task_id == Some(task_id) {
                head.running_task_id = None;
            }
            if head.pending_task_id.is_none() || head.pending_task_id == Some(task_id) {
                head.pending_task_id = Some(task_id);
                if head.running_task_id.is_none() {
                    add_pending_projection(&mut mutation, &entry)?;
                }
            } else if head.running_task_id.is_none() {
                activate_group_pending(&mut mutation, &head)?;
            }
            save_group_head(&mut mutation, group, &head)?;
        } else {
            add_pending_projection(&mut mutation, &entry)?;
        }
        mutation.put(current_key(task_id)?, TaskQueueRow::Task(entry.clone()))?;
        mutation.audit(TaskAuditEvent::Failed {
            task_id,
            error: error.to_string(),
            attempts: entry.task.attempts,
            scheduled_at: entry.task.scheduled_at,
            updated_at: now,
        });
        match mutation.commit().await {
            Ok(()) => return Ok(()),
            Err(cause)
                if attempt + 1 < max_queue_cas_attempts() && is_queue_cas_conflict(&cause) =>
            {
                tokio::task::yield_now().await;
            }
            Err(cause) => return Err(cause),
        }
    }
    unreachable!("bounded task failure loop returns on its final attempt")
}

fn require_running_attempt(entry: &TaskEntry, expected_attempts: Option<i32>) -> Result<()> {
    let Some(expected_attempts) = expected_attempts else {
        return Ok(());
    };
    if entry.task.status != TaskStatus::Running || entry.task.attempts != expected_attempts {
        bail!(
            "{STALE_FENCE}: task execution attempt changed (expected attempts={expected_attempts}, actual status={:?}, actual attempts={})",
            entry.task.status,
            entry.task.attempts
        );
    }
    Ok(())
}

fn complete_entry(
    mutation: &mut TaskMutation,
    entry: &mut TaskEntry,
    now: DateTime<Utc>,
) -> Result<()> {
    detach_active_projection(mutation, entry)?;
    if let Some(group) = entry.group.as_ref() {
        let mut head = read_group_head(mutation, group)?
            .ok_or_else(|| anyhow!("completed grouped task has no group head"))?;
        if head.running_task_id == Some(entry.task.id) {
            head.running_task_id = None;
        }
        if head.pending_task_id == Some(entry.task.id) {
            head.pending_task_id = None;
        }
        if head.running_task_id.is_none() && head.pending_task_id.is_some() {
            activate_group_pending(mutation, &head)?;
        }
        save_group_head(mutation, group, &head)?;
    }
    release_dedupe(mutation, entry)?;
    entry.task.status = TaskStatus::Completed;
    entry.task.updated_at = now;
    mutation.put(
        current_key(entry.task.id)?,
        TaskQueueRow::Task(entry.clone()),
    )
}

fn requeue_entry(
    mutation: &mut TaskMutation,
    entry: &mut TaskEntry,
    status: TaskStatus,
    now: DateTime<Utc>,
) -> Result<()> {
    detach_active_projection(mutation, entry)?;
    if let Some(group) = entry.group.as_ref() {
        let mut head = read_group_head(mutation, group)?
            .ok_or_else(|| anyhow!("requeued grouped task has no group head"))?;
        if head.running_task_id == Some(entry.task.id) {
            head.running_task_id = None;
        }
        if head.pending_task_id.is_some() && head.pending_task_id != Some(entry.task.id) {
            bail!("cannot requeue a grouped task while its successor is pending");
        }
        head.pending_task_id = Some(entry.task.id);
        entry.task.status = status;
        entry.task.updated_at = now;
        if head.running_task_id.is_none() {
            add_pending_projection(mutation, entry)?;
        }
        save_group_head(mutation, group, &head)?;
    } else {
        entry.task.status = status;
        entry.task.updated_at = now;
        add_pending_projection(mutation, entry)?;
    }
    mutation.put(
        current_key(entry.task.id)?,
        TaskQueueRow::Task(entry.clone()),
    )
}

fn run_entry(mutation: &mut TaskMutation, entry: &mut TaskEntry, now: DateTime<Utc>) -> Result<()> {
    detach_active_projection(mutation, entry)?;
    if let Some(group) = entry.group.as_ref() {
        let mut head = read_group_head(mutation, group)?
            .ok_or_else(|| anyhow!("running grouped task has no group head"))?;
        if head.running_task_id.is_some() && head.running_task_id != Some(entry.task.id) {
            bail!("another task in the group is already running");
        }
        if head.pending_task_id == Some(entry.task.id) {
            head.pending_task_id = None;
        }
        head.running_task_id = Some(entry.task.id);
        save_group_head(mutation, group, &head)?;
    }
    add_running_projection(mutation, entry)?;
    entry.task.status = TaskStatus::Running;
    entry.task.updated_at = now;
    mutation.put(
        current_key(entry.task.id)?,
        TaskQueueRow::Task(entry.clone()),
    )
}

fn reserve_task_id(mutation: &mut TaskMutation) -> Result<i64> {
    let key = allocator_key()?;
    let current = match mutation.read(&key)? {
        None => 0,
        Some(TaskQueueRow::Allocator(allocator)) => allocator.max_task_id,
        Some(_) => bail!("task allocator key contains another row kind"),
    };
    let next = current
        .checked_add(1)
        .ok_or_else(|| anyhow!("task id overflow"))?;
    mutation.put(
        key,
        TaskQueueRow::Allocator(TaskAllocator { max_task_id: next }),
    )?;
    Ok(next)
}

fn live_dedupe_task(mutation: &mut TaskMutation, hash: &str) -> Result<Option<TaskEntry>> {
    let key = dedupe_key(hash)?;
    let Some(row) = mutation.read(&key)? else {
        return Ok(None);
    };
    let TaskQueueRow::Dedupe(head) = row else {
        bail!("task dedupe key contains another row kind");
    };
    if head.dedupe_hash != hash {
        bail!("task dedupe head hash does not match its key");
    }
    let Some(entry) = mutation.read_task(head.task_id)? else {
        bail!("task dedupe head references a missing task");
    };
    if entry.task.status == TaskStatus::Completed {
        mutation.delete(key)?;
        return Ok(None);
    }
    Ok(Some(entry))
}

fn add_pending_projection(mutation: &mut TaskMutation, entry: &TaskEntry) -> Result<()> {
    let order = TaskOrder::from_task(&entry.task)?;
    mutation.put(
        pending_key(&order)?,
        TaskQueueRow::Pending(PendingProjection { order }),
    )
}

fn remove_pending_projection(mutation: &mut TaskMutation, entry: &TaskEntry) -> Result<()> {
    let order = TaskOrder::from_task(&entry.task)?;
    let projection_key = pending_key(&order)?;
    match mutation.read(&projection_key)? {
        None => return Ok(()),
        Some(TaskQueueRow::Pending(projection)) if projection.order == order => {}
        Some(_) => bail!("task pending projection does not match current task"),
    }
    mutation.delete(projection_key)
}

fn add_running_projection(mutation: &mut TaskMutation, entry: &TaskEntry) -> Result<()> {
    mutation.put(
        running_key(entry)?,
        TaskQueueRow::Running(RunningProjection {
            task_id: entry.task.id,
            task_type: entry.task.task_type,
            group_hash: entry.group.as_ref().map(|group| group.hash.clone()),
        }),
    )
}

fn remove_running_projection(mutation: &mut TaskMutation, entry: &TaskEntry) -> Result<()> {
    let key = running_key(entry)?;
    match mutation.read(&key)? {
        None => Ok(()),
        Some(TaskQueueRow::Running(projection)) if projection.task_id == entry.task.id => {
            mutation.delete(key)
        }
        Some(_) => bail!("task running projection does not match current task"),
    }
}

fn detach_active_projection(mutation: &mut TaskMutation, entry: &TaskEntry) -> Result<()> {
    match entry.task.status {
        TaskStatus::Pending | TaskStatus::Failed => remove_pending_projection(mutation, entry),
        TaskStatus::Running => remove_running_projection(mutation, entry),
        TaskStatus::Completed => Ok(()),
    }
}

fn read_group_head(
    mutation: &mut TaskMutation,
    group: &TaskGroupIdentity,
) -> Result<Option<TaskGroupHead>> {
    match mutation.read(&group_key(group)?)? {
        None => Ok(None),
        Some(TaskQueueRow::Group(head))
            if head.kind == group.kind && head.group_hash == group.hash =>
        {
            Ok(Some(head))
        }
        Some(_) => bail!("task group row does not match its key"),
    }
}

fn save_group_head(
    mutation: &mut TaskMutation,
    group: &TaskGroupIdentity,
    head: &TaskGroupHead,
) -> Result<()> {
    let key = group_key(group)?;
    if head.running_task_id.is_none() && head.pending_task_id.is_none() {
        mutation.delete(key)
    } else {
        mutation.put(key, TaskQueueRow::Group(head.clone()))
    }
}

fn activate_group_pending(mutation: &mut TaskMutation, head: &TaskGroupHead) -> Result<()> {
    let Some(task_id) = head.pending_task_id else {
        return Ok(());
    };
    let entry = mutation
        .read_task(task_id)?
        .ok_or_else(|| anyhow!("task group pending head references a missing task"))?;
    if !matches!(entry.task.status, TaskStatus::Pending | TaskStatus::Failed) {
        bail!("task group pending head references a non-pending task");
    }
    add_pending_projection(mutation, &entry)
}

fn retire_superseded_task(mutation: &mut TaskMutation, mut entry: TaskEntry) -> Result<()> {
    let now = Utc::now();
    release_dedupe(mutation, &entry)?;
    entry.task.status = TaskStatus::Completed;
    entry.task.updated_at = now;
    mutation.put(
        current_key(entry.task.id)?,
        TaskQueueRow::Task(entry.clone()),
    )?;
    mutation.audit(TaskAuditEvent::StatusUpdated {
        task_id: entry.task.id,
        status: TaskStatus::Completed,
        updated_at: now,
    });
    Ok(())
}

fn release_dedupe(mutation: &mut TaskMutation, entry: &TaskEntry) -> Result<()> {
    let Some(hash) = entry.dedupe_hash.as_ref() else {
        return Ok(());
    };
    let key = dedupe_key(hash)?;
    match mutation.read(&key)? {
        None => Ok(()),
        Some(TaskQueueRow::Dedupe(head)) if head.task_id == entry.task.id => mutation.delete(key),
        Some(TaskQueueRow::Dedupe(_)) => Ok(()),
        Some(_) => bail!("task dedupe key contains another row kind"),
    }
}

fn require_task_queue_permit(mvcc: &MvccSubsystem, permit: &PartitionWritePermit) -> Result<()> {
    if permit.partition_family != "task_queue"
        || permit.partition_id != hex::encode(super::task_queue_partition_id())
        || permit.owner_node_id != mvcc.local_node.node_id
    {
        bail!("task queue write permit targets a different partition");
    }
    Ok(())
}
