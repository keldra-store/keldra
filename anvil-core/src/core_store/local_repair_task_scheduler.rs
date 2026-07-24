use super::*;
use crate::{persistence::Persistence, tasks::RebalanceShardTaskPayload};
use tokio::sync::{mpsc, oneshot};

const REPAIR_TASK_QUEUE_CAPACITY: usize = 256;

#[derive(Debug)]
enum SchedulerRequest {
    CheckOwnership {
        completion: oneshot::Sender<std::result::Result<bool, String>>,
    },
    Schedule {
        payload: RebalanceShardTaskPayload,
        priority: i32,
        completion: oneshot::Sender<std::result::Result<bool, String>>,
    },
}

#[derive(Debug, Clone)]
pub(super) struct RepairTaskScheduler {
    sender: mpsc::Sender<SchedulerRequest>,
}

impl RepairTaskScheduler {
    fn spawn(persistence: Persistence) -> Self {
        let (sender, mut receiver) = mpsc::channel::<SchedulerRequest>(REPAIR_TASK_QUEUE_CAPACITY);
        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                match request {
                    SchedulerRequest::CheckOwnership { completion } => {
                        let result = persistence
                            .owns_rebalance_shard_scheduler()
                            .await
                            .map_err(|error| format!("{error:#}"));
                        let _ = completion.send(result);
                    }
                    SchedulerRequest::Schedule {
                        payload,
                        priority,
                        completion,
                    } => {
                        let result = persistence
                            .enqueue_rebalance_shard_task(&payload, priority)
                            .await
                            .map_err(|error| format!("{error:#}"));
                        let _ = completion.send(result);
                    }
                }
            }
        });
        Self { sender }
    }

    async fn owns_queue(&self) -> Result<bool> {
        let (completion, result) = oneshot::channel();
        self.sender
            .send(SchedulerRequest::CheckOwnership { completion })
            .await
            .map_err(|_| anyhow!("CoreStore shard repair task scheduler has stopped"))?;
        result
            .await
            .map_err(|_| anyhow!("CoreStore shard repair task scheduler dropped its response"))?
            .map_err(|error| anyhow!(error))
    }

    async fn schedule(&self, payload: RebalanceShardTaskPayload, priority: i32) -> Result<bool> {
        payload.validate()?;
        let (completion, result) = oneshot::channel();
        self.sender
            .send(SchedulerRequest::Schedule {
                payload,
                priority,
                completion,
            })
            .await
            .map_err(|_| anyhow!("CoreStore shard repair task scheduler has stopped"))?;
        result
            .await
            .map_err(|_| anyhow!("CoreStore shard repair task scheduler dropped its response"))?
            .map_err(|error| anyhow!(error))
    }
}

impl CoreStore {
    pub(crate) fn install_repair_task_scheduler(&self, persistence: Persistence) -> Result<()> {
        if self.repair_task_scheduler.get().is_some() {
            return Ok(());
        }
        self.repair_task_scheduler
            .set(RepairTaskScheduler::spawn(persistence))
            .map_err(|_| anyhow!("CoreStore shard repair task scheduler was installed twice"))
    }

    pub(super) async fn schedule_rebalance_shard_task(
        &self,
        payload: RebalanceShardTaskPayload,
        priority: i32,
    ) -> Result<bool> {
        self.repair_task_scheduler
            .get()
            .ok_or_else(|| anyhow!("CoreStore shard repair task scheduler is not installed"))?
            .schedule(payload, priority)
            .await
    }

    pub(super) async fn owns_rebalance_shard_scheduler(&self) -> Result<bool> {
        self.repair_task_scheduler
            .get()
            .ok_or_else(|| anyhow!("CoreStore shard repair task scheduler is not installed"))?
            .owns_queue()
            .await
    }
}
