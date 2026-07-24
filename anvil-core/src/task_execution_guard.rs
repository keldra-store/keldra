use crate::core_store::CoreMutationPrecondition;
use crate::storage::Storage;
use crate::task_lease::{self, LEASE_EXPIRED, TaskLease};
use anyhow::{Result, anyhow};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OwnedMutexGuard};

/// Coordinates one in-process execution against its exact durable task lease.
///
/// Renewal, checkpointing, validation, and authoritative publication all lock
/// the same lease version. A publication permit therefore prevents this
/// process from renewing the lease while that version protects an in-flight
/// mutation. Another process can still take over an expired lease; CoreStore's
/// final precondition validation must reject that stale publication.
#[derive(Clone)]
pub(crate) struct TaskExecutionGuard {
    lease: Arc<Mutex<TaskLease>>,
    storage: Storage,
    signing_key: Arc<[u8]>,
    ttl_nanos: i64,
}

/// Exclusive authority to publish using one exact temporal task-lease version.
///
/// Call [`TaskPublicationPermit::publish_with`] so the lease-version lock is
/// retained across the complete authoritative publication future.
pub(crate) struct TaskPublicationPermit {
    lease: OwnedMutexGuard<TaskLease>,
    precondition: CoreMutationPrecondition,
}

impl TaskExecutionGuard {
    pub(crate) fn new(storage: Storage, signing_key: Vec<u8>, lease: TaskLease) -> Result<Self> {
        let ttl_nanos = lease
            .expires_at_nanos
            .checked_sub(lease.acquired_at_nanos)
            .filter(|ttl| *ttl > 0)
            .ok_or_else(|| anyhow!("task lease has no positive renewal window"))?;
        lease.verify(&signing_key)?;

        Ok(Self {
            lease: Arc::new(Mutex::new(lease)),
            storage,
            signing_key: Arc::from(signing_key),
            ttl_nanos,
        })
    }

    /// Returns the exact in-process lease version without changing it.
    pub(crate) async fn snapshot(&self) -> TaskLease {
        self.lease.lock().await.clone()
    }

    /// Confirms that the guarded version is still current and unexpired.
    pub(crate) async fn check(&self) -> Result<TaskLease> {
        let mut lease = self.lease.lock().await;
        let checked = task_lease::check_task_lease(
            &self.storage,
            &lease,
            current_time_nanos()?,
            &self.signing_key,
        )
        .await?;
        *lease = checked.clone();
        Ok(checked)
    }

    /// Renews the current version while excluding publication and checkpointing.
    pub(crate) async fn renew(&self) -> Result<TaskLease> {
        let mut lease = self.lease.lock().await;
        let renewed = task_lease::renew_task_lease(
            &self.storage,
            &lease,
            current_time_nanos()?,
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
        let checkpointed = task_lease::checkpoint_task_lease(
            &self.storage,
            &lease,
            checkpoint_cursor,
            current_time_nanos()?,
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

    /// Locks the lease version and derives its exact temporal publication fence.
    pub(crate) async fn publication_permit(&self) -> Result<TaskPublicationPermit> {
        let lease = self.lease.clone().lock_owned().await;
        let precondition = task_lease::task_lease_fenced_precondition(
            &self.storage,
            &lease,
            current_time_nanos()?,
            &self.signing_key,
        )
        .await?;
        Ok(TaskPublicationPermit {
            lease,
            precondition,
        })
    }
}

impl TaskPublicationPermit {
    /// Runs an authoritative publication while retaining the exact lease lock.
    pub(crate) async fn publish_with<T, E, F, Fut>(
        self,
        publication: F,
    ) -> std::result::Result<T, E>
    where
        F: FnOnce(CoreMutationPrecondition) -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
    {
        let Self {
            lease,
            precondition,
        } = self;
        let result = publication(precondition).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_lease::{TaskLeaseAcquire, TaskLeaseOwner, acquire_task_lease};
    use tempfile::{TempDir, tempdir};
    use tokio::{sync::oneshot, time::timeout};

    const KEY: &[u8] = b"task execution guard signing key";
    const TEST_TTL_NANOS: i64 = 60_000_000_000;

    #[tokio::test]
    async fn publication_permit_blocks_local_renewal_and_exposes_temporal_fence() {
        let (_temp, guard, original) = acquired_guard().await;
        let permit = guard.publication_permit().await.unwrap();
        let renewal_guard = guard.clone();
        let (renewal_started_tx, renewal_started_rx) = oneshot::channel();
        let renewal = tokio::spawn(async move {
            renewal_started_tx
                .send(())
                .expect("renewal start receiver remains open");
            renewal_guard.renew().await
        });
        renewal_started_rx
            .await
            .expect("renewal reports its lock attempt");
        assert!(
            !renewal.is_finished(),
            "renewal must wait for the publication permit"
        );

        let precondition = permit
            .publish_with(|precondition| async move { Ok::<_, ()>(precondition) })
            .await
            .unwrap();
        let CoreMutationPrecondition::CoreMetaLease {
            expected_payload_hash,
            expires_at_unix_nanos,
            ..
        } = precondition
        else {
            panic!("publication permit must expose a temporal CoreMeta lease fence");
        };
        assert!(!expected_payload_hash.is_empty());
        assert_eq!(
            expires_at_unix_nanos,
            u64::try_from(original.expires_at_nanos).unwrap()
        );

        let renewed = timeout(Duration::from_secs(60), renewal)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(renewed.fence_token, original.fence_token);
        assert_eq!(renewed.lease_epoch, original.lease_epoch + 1);
        assert_eq!(guard.snapshot().await, renewed);
    }

    #[tokio::test]
    async fn checkpoint_and_check_update_the_shared_exact_version() {
        let (_temp, guard, original) = acquired_guard().await;
        let checkpointed = guard.checkpoint(original.source_cursor + 5).await.unwrap();

        assert_eq!(checkpointed.root_generation, original.root_generation + 1);
        assert_eq!(checkpointed.checkpoint_cursor, original.source_cursor + 5);
        assert_eq!(guard.snapshot().await, checkpointed);
        assert_eq!(guard.check().await.unwrap(), checkpointed);
    }

    #[tokio::test]
    async fn publication_permit_and_checkpoint_share_one_version_lock() {
        let (_temp, guard, original) = acquired_guard().await;
        let permit = guard.publication_permit().await.unwrap();
        let checkpoint_guard = guard.clone();
        let (checkpoint_started_tx, checkpoint_started_rx) = oneshot::channel();
        let checkpoint = tokio::spawn(async move {
            checkpoint_started_tx
                .send(())
                .expect("checkpoint start receiver remains open");
            checkpoint_guard
                .checkpoint(original.source_cursor + 7)
                .await
        });
        checkpoint_started_rx
            .await
            .expect("checkpoint reports its lock attempt");
        assert!(
            !checkpoint.is_finished(),
            "checkpoint must wait for the publication permit"
        );
        permit
            .publish_with(|_| async move { Ok::<_, ()>(()) })
            .await
            .unwrap();

        let checkpointed = timeout(Duration::from_secs(60), checkpoint)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(guard.snapshot().await, checkpointed);
    }

    async fn acquired_guard() -> (TempDir, TaskExecutionGuard, TaskLease) {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let now_nanos = current_time_nanos().unwrap();
        let lease = acquire_task_lease(
            &storage,
            TaskLeaseAcquire {
                task_id: format!("guard-test-{}", uuid::Uuid::new_v4()),
                task_kind: "test".to_string(),
                partition_family: "test".to_string(),
                partition_id: hex::encode([7; 32]),
                owner: TaskLeaseOwner::node("node-a"),
                source_cursor: 10,
                now_nanos,
                ttl_nanos: TEST_TTL_NANOS,
            },
            KEY,
        )
        .await
        .unwrap();
        let guard = TaskExecutionGuard::new(storage, KEY.to_vec(), lease.clone()).unwrap();
        (temp, guard, lease)
    }
}
