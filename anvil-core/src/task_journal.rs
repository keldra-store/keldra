mod model;
mod queue;
mod store;

use crate::formats::{Hash32, hash32};

pub(crate) use queue::{
    claim_pending_tasks_with_permit, enqueue_authz_materialization_task_with_permit,
    enqueue_index_build_task_with_permit, enqueue_task_if_absent_with_permit,
    enqueue_task_with_permit, fail_task_with_execution_guard, fail_task_with_permit, has_due_tasks,
    list_tasks_page, update_task_status_with_execution_guard, update_task_status_with_permit,
};

pub fn task_queue_partition_id() -> Hash32 {
    hash32(b"task_queue/global")
}

fn task_queue_partition_principal() -> String {
    "partition-owner:task_queue:global".to_string()
}

#[cfg(test)]
pub(crate) use queue::{
    claim_pending_tasks, enqueue_task, fail_task, force_task_schedule_for_test, update_task_status,
};
#[cfg(test)]
pub(crate) use store::{
    read_task_frame_fences_for_test, reset_task_row_visits_for_test, task_row_visits_for_test,
};

#[cfg(test)]
mod tests;
