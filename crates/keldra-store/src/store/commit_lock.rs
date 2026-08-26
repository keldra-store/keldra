use std::time::{Duration, Instant};

use super::Store;

pub(crate) struct CommitLockGuard<'a> {
    guard: Option<tokio::sync::MutexGuard<'a, ()>>,
    owner: &'static str,
    wait_duration: Duration,
    acquired_at: Instant,
}

impl CommitLockGuard<'_> {
    pub(crate) fn wait_duration(&self) -> Duration {
        self.wait_duration
    }
}

impl Drop for CommitLockGuard<'_> {
    fn drop(&mut self) {
        let hold_duration = self.acquired_at.elapsed();
        drop(self.guard.take());
        record_release(self.owner, self.wait_duration, hold_duration);
    }
}

pub(crate) struct OwnedCommitLockGuard {
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    owner: &'static str,
    wait_duration: Duration,
    acquired_at: Instant,
}

impl Drop for OwnedCommitLockGuard {
    fn drop(&mut self) {
        let hold_duration = self.acquired_at.elapsed();
        drop(self.guard.take());
        record_release(self.owner, self.wait_duration, hold_duration);
    }
}

impl Store {
    pub(crate) async fn lock_commit(&self, owner: &'static str) -> CommitLockGuard<'_> {
        let started = Instant::now();
        let guard = self.commit_lock.lock().await;
        CommitLockGuard {
            guard: Some(guard),
            owner,
            wait_duration: started.elapsed(),
            acquired_at: Instant::now(),
        }
    }

    pub(crate) async fn lock_commit_owned(&self, owner: &'static str) -> OwnedCommitLockGuard {
        let started = Instant::now();
        let guard = self.commit_lock.clone().lock_owned().await;
        OwnedCommitLockGuard {
            guard: Some(guard),
            owner,
            wait_duration: started.elapsed(),
            acquired_at: Instant::now(),
        }
    }

    pub(crate) fn try_lock_commit(
        &self,
        owner: &'static str,
    ) -> Result<CommitLockGuard<'_>, tokio::sync::TryLockError> {
        let started = Instant::now();
        let guard = self.commit_lock.try_lock()?;
        Ok(CommitLockGuard {
            guard: Some(guard),
            owner,
            wait_duration: started.elapsed(),
            acquired_at: Instant::now(),
        })
    }

    pub(crate) fn blocking_lock_commit(&self, owner: &'static str) -> CommitLockGuard<'_> {
        let started = Instant::now();
        let guard = self.commit_lock.blocking_lock();
        CommitLockGuard {
            guard: Some(guard),
            owner,
            wait_duration: started.elapsed(),
            acquired_at: Instant::now(),
        }
    }
}

fn record_release(owner: &'static str, wait_duration: Duration, hold_duration: Duration) {
    tracing::debug!(
        commit_lock.owner = owner,
        histogram.keldra_store_commit_lock_wait_duration_seconds = wait_duration.as_secs_f64(),
        histogram.keldra_store_commit_lock_hold_duration_seconds = hold_duration.as_secs_f64(),
        "Store commit lock released"
    );
}
