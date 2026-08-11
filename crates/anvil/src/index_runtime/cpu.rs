//! The one process-owned CPU pool for non-async index projection work.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anvil_index::IndexError;
use anvil_index::compaction::{CompactionExecutor, CompactionTaskFuture, CompactionTaskHandle};
use thiserror::Error;

#[derive(Clone)]
pub(crate) struct IndexCpuPool {
    inner: Arc<rayon::ThreadPool>,
    workers: usize,
}

/// Runtime bridge used by storage-neutral parallel index compaction.
#[derive(Clone)]
pub(crate) struct IndexCompactionExecutor {
    cpu: IndexCpuPool,
}

impl IndexCompactionExecutor {
    pub(crate) fn new(cpu: IndexCpuPool) -> Self {
        Self { cpu }
    }
}

pub(crate) struct IndexCompactionTask {
    inner: tokio::task::JoinHandle<Result<(), IndexError>>,
}

impl Drop for IndexCompactionTask {
    fn drop(&mut self) {
        self.inner.abort();
    }
}

impl Future for IndexCompactionTask {
    type Output = Result<(), IndexError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.inner).poll(context) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(error)) => Poll::Ready(Err(IndexError::Io(format!(
                "parallel compaction task failed: {error}"
            )))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl CompactionTaskHandle for IndexCompactionTask {
    fn abort(&self) {
        self.inner.abort();
    }
}

impl CompactionExecutor for IndexCompactionExecutor {
    type Task = IndexCompactionTask;

    fn spawn_io(&self, task: CompactionTaskFuture) -> Self::Task {
        IndexCompactionTask {
            inner: tokio::spawn(task),
        }
    }

    async fn run_cpu<T, F>(&self, work: F) -> Result<T, IndexError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, IndexError> + Send + 'static,
    {
        self.cpu
            .install(work)
            .await
            .map_err(|error| IndexError::Io(error.to_string()))?
    }
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
            workers,
        })
    }

    pub(crate) fn workers(&self) -> usize {
        self.workers
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
        assert_eq!(pool.workers(), 1);
        let name = pool
            .install(|| std::thread::current().name().unwrap_or_default().to_owned())
            .await
            .unwrap();
        assert!(name.starts_with("anvil-index-"));
    }
}
