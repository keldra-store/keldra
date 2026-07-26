use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::watch;

use crate::{
    mvcc_bootstrap::MvccSubsystem,
    mvcc_product::ProductMutation,
    mvcc_transaction::{CertificationResult, DurabilityLevel, ReadConsistency},
    object_materialisation::{ObjectMaterialisationResult, ObjectMaterialisationState},
};
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

pub struct MvccMaterialisationPublisher {
    mvcc: Arc<MvccSubsystem>,
}

impl MvccMaterialisationPublisher {
    pub fn new(mvcc: Arc<MvccSubsystem>) -> Self {
        Self { mvcc }
    }

    pub async fn publish(&self, mut result: ObjectMaterialisationResult) -> Result<()> {
        anyhow::ensure!(
            result.cluster_id == self.mvcc.cluster_id(),
            "materialisation result belongs to another cluster"
        );
        result.state = ObjectMaterialisationState::Complete;
        let result_key = result.result_key()?;
        let status_key = result.status_key()?;
        if let Some(existing) = self.mvcc.runtime.local_store().read_latest(&status_key)?
            && existing.value == result.canonical_bytes()?
        {
            return Ok(());
        }
        let principal = "system/object-materialisation";
        let now = now_unix_ms();
        let handle = self
            .mvcc
            .open_transactions
            .begin(
                self.mvcc.runtime.as_ref(),
                result.cluster_id.clone(),
                principal,
                format!("object-materialisation:{}", result.job_id),
                Duration::from_secs(300),
                DurabilityLevel::Quorum,
                ReadConsistency::Linearized,
                now,
            )
            .await?;
        let bytes = result.canonical_bytes()?;
        self.mvcc.stage_product_mutations(
            &handle.transaction_id,
            principal,
            vec![
                ProductMutation::put(result_key, bytes.clone()),
                ProductMutation::put(status_key, bytes),
            ],
            now,
        )?;
        let outcome = self
            .mvcc
            .open_transactions
            .commit(
                self.mvcc.runtime.as_ref(),
                &handle.transaction_id,
                principal,
                now_unix_ms(),
            )
            .await?;
        anyhow::ensure!(
            matches!(outcome.certification, CertificationResult::Committed { .. }),
            "materialisation result transaction conflicted"
        );
        Ok(())
    }
}
