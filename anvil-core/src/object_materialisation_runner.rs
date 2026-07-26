use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::watch;

use crate::{mvcc_store::LocalMvccStore, object_materialisation::ObjectMaterialisationJob};

#[async_trait]
pub trait ObjectMaterialisationExecutor: Send + Sync + 'static {
    async fn execute(&self, job_id: &str, job: &ObjectMaterialisationJob) -> Result<()>;
}

pub struct ObjectMaterialisationRunner<E> {
    store: LocalMvccStore,
    executor: Arc<E>,
    worker_id: String,
    lease_ms: u64,
    idle: Duration,
}

impl<E: ObjectMaterialisationExecutor> ObjectMaterialisationRunner<E> {
    pub fn new(store: LocalMvccStore, executor: Arc<E>, worker_id: String) -> Result<Self> {
        anyhow::ensure!(!worker_id.trim().is_empty(), "worker ID is required");
        Ok(Self {
            store,
            executor,
            worker_id,
            lease_ms: 30_000,
            idle: Duration::from_millis(250),
        })
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            if let Err(error) = self.run_once(now_unix_ms()).await {
                tracing::warn!(%error, worker_id = %self.worker_id, "object materialisation attempt failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(self.idle) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    pub async fn run_once(&self, now_unix_ms: u64) -> Result<bool> {
        let Some((job_id, record)) =
            self.store
                .claim_object_materialisation(&self.worker_id, now_unix_ms, self.lease_ms)?
        else {
            return Ok(false);
        };
        match self.executor.execute(&job_id, &record.job).await {
            Ok(()) => self
                .store
                .complete_object_materialisation(&job_id, &self.worker_id)?,
            Err(error) => {
                let shift = record.attempts.saturating_sub(1).min(10);
                let delay = 250_u64.saturating_mul(1_u64 << shift);
                self.store.retry_object_materialisation(
                    &job_id,
                    &self.worker_id,
                    now_unix_ms.saturating_add(delay),
                    &error.to_string(),
                )?;
                return Err(error);
            }
        }
        Ok(true)
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}
