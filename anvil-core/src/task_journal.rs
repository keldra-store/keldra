mod model;
mod queue;
mod store;

use crate::formats::{Hash32, hash32};

pub(crate) use queue::{
    claim_pending_tasks_with_permit, enqueue_authz_materialization_task_with_permit,
    enqueue_index_build_task_with_permit, enqueue_repair_run_with_permit,
    enqueue_task_if_absent_with_permit, enqueue_task_with_permit, fail_task_with_execution_guard,
    fail_task_with_permit, get_task, has_due_tasks, list_tasks_page,
    update_task_status_with_execution_guard, update_task_status_with_permit,
};

pub fn task_queue_partition_id() -> Hash32 {
    hash32(b"task_queue/global")
}

fn task_queue_partition_principal() -> String {
    "partition-owner:task_queue:global".to_string()
}
