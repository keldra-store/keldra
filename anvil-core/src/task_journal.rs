mod model;
mod queue;
mod store;

use crate::formats::{Hash32, hash32};

/// Non-owning admission proof for an ordinary task producer.
///
/// This is deliberately distinct from [`crate::partition_fence::PartitionWritePermit`]:
/// enqueue is serialized by MVCC predicates and does not confer authority to
/// claim or advance task work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskQueueProducerPermit {
    producer_node_id: String,
}

impl TaskQueueProducerPermit {
    pub(crate) fn for_node(producer_node_id: impl Into<String>) -> Self {
        Self {
            producer_node_id: producer_node_id.into(),
        }
    }

    fn producer_node_id(&self) -> &str {
        &self.producer_node_id
    }
}

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
