//! The one process-owned CPU pool for non-async index projection work.

use std::sync::Arc;

use thiserror::Error;

#[derive(Clone)]
pub(crate) struct IndexCpuPool {
    inner: Arc<rayon::ThreadPool>,
}

impl IndexCpuPool {
    pub(crate) fn new(workers: u32) -> Result<Self, IndexCpuPoolError> {
        if workers == 0 {
            return Err(IndexCpuPoolError::ZeroWorkers);
        }
        let workers = usize::try_from(workers).map_err(|_| IndexCpuPoolError::WorkerOverflow)?;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|index| format!("anvil-index-{index}"))
            .build()
            .map_err(|error| IndexCpuPoolError::Build(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(pool),
        })
    }

    /// Run CPU work inside Anvil's pool, never Rayon's global registry.
    pub(crate) async fn install<F, T>(&self, work: F) -> Result<T, IndexCpuPoolError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let pool = self.inner.clone();
        tokio::task::spawn_blocking(move || pool.install(work))
            .await
            .map_err(|error| IndexCpuPoolError::Task(error.to_string()))
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum IndexCpuPoolError {
    #[error("index Rayon worker count must be positive")]
    ZeroWorkers,
    #[error("index Rayon worker count exceeds this platform")]
    WorkerOverflow,
    #[error("create index Rayon pool: {0}")]
    Build(String),
    #[error("index CPU task failed: {0}")]
    Task(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn work_runs_inside_the_owned_pool() {
        let pool = IndexCpuPool::new(1).unwrap();
        let name = pool
            .install(|| std::thread::current().name().unwrap_or_default().to_owned())
            .await
            .unwrap();
        assert!(name.starts_with("anvil-index-"));
    }
}
