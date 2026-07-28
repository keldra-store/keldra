use crate::task_lease::{self, LEASE_EXPIRED, TaskLease};
use anyhow::{Context, Result, anyhow};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Coordinates one in-process execution against its exact durable task lease.
///
/// Renewal, checkpointing, validation, and authoritative publication all lock
/// the same lease version. A publication permit therefore prevents this
/// process from renewing the lease while that version protects an in-flight
/// MVCC mutation. Certification rejects publication if the exact lease value
/// changed before commit.
#[derive(Clone)]
pub(crate) struct TaskExecutionGuard {
    lease: Arc<Mutex<TaskLease>>,
    mvcc: Arc<crate::mvcc_bootstrap::MvccSubsystem>,
    signing_key: Arc<[u8]>,
    ttl_nanos: i64,
    assignment: crate::mvcc_worker_authority::AssignmentGuard,
}

impl TaskExecutionGuard {
    pub(crate) fn mvcc(&self) -> Result<&crate::mvcc_bootstrap::MvccSubsystem> {
        Ok(self.mvcc.as_ref())
    }

    pub(crate) fn new_mvcc(
        mvcc: Arc<crate::mvcc_bootstrap::MvccSubsystem>,
        signing_key: Vec<u8>,
        lease: TaskLease,
    ) -> Result<Self> {
        let ttl_nanos = lease
            .expires_at_nanos
            .checked_sub(lease.acquired_at_nanos)
            .filter(|ttl| *ttl > 0)
            .ok_or_else(|| anyhow!("task lease has no positive renewal window"))?;
        lease.verify(&signing_key)?;
        let assignment = task_lease::claim_task_lease_assignment(&mvcc, &lease)
            .context("local node no longer owns the task execution assignment")?;

        Ok(Self {
            lease: Arc::new(Mutex::new(lease)),
            mvcc,
            signing_key: Arc::from(signing_key),
            ttl_nanos,
            assignment,
        })
    }

    pub(crate) fn assignment(&self) -> &crate::mvcc_worker_authority::AssignmentGuard {
        &self.assignment
    }

    /// Returns the exact in-process lease version without changing it.
    pub(crate) async fn snapshot(&self) -> TaskLease {
        self.lease.lock().await.clone()
    }

    /// Confirms that the guarded version is still current and unexpired.
    pub(crate) async fn check(&self) -> Result<TaskLease> {
        let mut lease = self.lease.lock().await;
        let now = current_time_nanos()?;
        let checked =
            task_lease::check_task_lease_mvcc(&self.mvcc, &lease, now, &self.signing_key)?;
        *lease = checked.clone();
        Ok(checked)
    }

    /// Renews the current version while excluding publication and checkpointing.
    pub(crate) async fn renew(&self) -> Result<TaskLease> {
        let mut lease = self.lease.lock().await;
        let now = current_time_nanos()?;
        let renewed = task_lease::renew_task_lease_mvcc(
            &self.mvcc,
            &lease,
            now,
            self.ttl_nanos,
            &self.signing_key,
        )
        .await?;
        *lease = renewed.clone();
        Ok(renewed)
    }

    /// Persists progress against the exact current lease version.
    pub(crate) async fn checkpoint(&self, checkpoint_cursor: u128) -> Result<TaskLease> {
        let mut lease = self.lease.lock().await;
        let now = current_time_nanos()?;
        let checkpointed = task_lease::checkpoint_task_lease_mvcc(
            &self.mvcc,
            &lease,
            checkpoint_cursor,
            now,
            &self.signing_key,
        )
        .await?;
        *lease = checkpointed.clone();
        Ok(checkpointed)
    }

    /// Returns the delay before the next renewal attempt for the current version.
    pub(crate) async fn renewal_delay(&self) -> Result<Duration> {
        let lease = self.lease.lock().await;
        renewal_delay(&lease, current_time_nanos()?)
    }

    /// Retains the exact MVCC lease version while an atomic publication is
    /// staged and certified.
    pub(crate) async fn publish_mvcc_with<T, F, Fut>(&self, publication: F) -> Result<T>
    where
        F: FnOnce(
            (
                crate::mvcc_transaction::LogicalKey,
                crate::mvcc_transaction::PredicateKind,
            ),
        ) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let lease = self.lease.clone().lock_owned().await;
        let predicate = task_lease::task_lease_mvcc_predicate(&lease)?;
        let result = publication(predicate).await;
        drop(lease);
        result
    }
}

fn current_time_nanos() -> Result<i64> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp cannot be represented in nanoseconds"))
}

fn renewal_delay(lease: &TaskLease, now_nanos: i64) -> Result<Duration> {
    let remaining = lease.expires_at_nanos.saturating_sub(now_nanos);
    if remaining <= 0 {
        return Err(anyhow!("{LEASE_EXPIRED}: task lease expired"));
    }
    let delay_nanos = (remaining / 3).max(1);
    Ok(Duration::from_nanos(
        u64::try_from(delay_nanos).map_err(|_| anyhow!("task lease delay exceeds u64"))?,
    ))
}
