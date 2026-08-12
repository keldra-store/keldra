//! The one process-owned CPU pool for non-async index projection work.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use anvil_index::compaction::{CompactionExecutor, CompactionTaskFuture, CompactionTaskHandle};
use anvil_index::{IndexError, IndexKind};
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

struct QueryCpuActiveGuard {
    kind: IndexKind,
    span: tracing::Span,
}

impl Drop for QueryCpuActiveGuard {
    fn drop(&mut self) {
        self.span.in_scope(|| {
            tracing::info!(
                index.kind = ?self.kind,
                counter.anvil_index_query_cpu_active = -1_i64,
                "index query CPU chunk released"
            );
        });
    }
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

    /// Execute one already-materialized query CPU chunk on the process-owned
    /// Rayon pool. Async artifact I/O happens before this boundary.
    pub(crate) async fn query_chunk<F, T>(&self, kind: IndexKind, work: F) -> Result<T, IndexError>
    where
        F: FnOnce() -> Result<T, IndexError> + Send + 'static,
        T: Send + 'static,
    {
        let enqueued = std::time::Instant::now();
        let started = Arc::new(AtomicBool::new(false));
        let worker_started = Arc::clone(&started);
        let span = tracing::info_span!(
            "anvil.index.query.cpu",
            index.kind = ?kind,
            query.cpu_queue_seconds = tracing::field::Empty,
            query.cpu_seconds = tracing::field::Empty,
            query.outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        span.in_scope(|| {
            tracing::info!(
                index.kind = ?kind,
                counter.anvil_index_query_cpu_waiting = 1_i64,
                "index query CPU chunk queued"
            );
        });
        let worker_span = span.clone();
        let execution = self
            .install(move || {
                worker_started.store(true, Ordering::Release);
                let queue_seconds = enqueued.elapsed().as_secs_f64();
                worker_span.record("query.cpu_queue_seconds", queue_seconds);
                worker_span.in_scope(|| {
                    tracing::info!(
                        index.kind = ?kind,
                        counter.anvil_index_query_cpu_waiting = -1_i64,
                        "index query CPU queue wait released"
                    );
                    tracing::info!(
                        index.kind = ?kind,
                        counter.anvil_index_query_cpu_active = 1_i64,
                        "index query CPU chunk started"
                    );
                });
                let _active = QueryCpuActiveGuard {
                    kind,
                    span: worker_span.clone(),
                };
                let cpu_started = std::time::Instant::now();
                let result = work();
                let cpu_seconds = cpu_started.elapsed().as_secs_f64();
                worker_span.record("query.cpu_seconds", cpu_seconds);
                (result, queue_seconds, cpu_seconds)
            })
            .await;
        let (result, queue_seconds, cpu_seconds) = match execution {
            Ok(execution) => execution,
            Err(error) => {
                if !started.load(Ordering::Acquire) {
                    span.in_scope(|| {
                        tracing::info!(
                            index.kind = ?kind,
                            counter.anvil_index_query_cpu_waiting = -1_i64,
                            "index query CPU queue wait released after task failure"
                        );
                    });
                }
                span.record("query.outcome", "failed");
                span.record("otel.status_code", "error");
                span.in_scope(|| {
                    tracing::warn!(
                        index.kind = ?kind,
                        query.outcome = "failed",
                        monotonic_counter.anvil_index_query_cpu_chunks_total = 1_u64,
                        monotonic_counter.anvil_index_query_cpu_failures_total = 1_u64,
                        %error,
                        "index query CPU task failed"
                    );
                });
                return Err(IndexError::Io(error.to_string()));
            }
        };
        let failed = result.is_err();
        span.record("query.outcome", if failed { "failed" } else { "completed" });
        span.record("otel.status_code", if failed { "error" } else { "ok" });
        span.in_scope(|| {
            tracing::info!(
                index.kind = ?kind,
                query.outcome = if failed { "failed" } else { "completed" },
                monotonic_counter.anvil_index_query_cpu_chunks_total = 1_u64,
                monotonic_counter.anvil_index_query_cpu_failures_total = u64::from(failed),
                histogram.anvil_index_query_cpu_queue_seconds = queue_seconds,
                histogram.anvil_index_query_cpu_seconds = cpu_seconds,
                "index query CPU chunk completed"
            );
        });
        result
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

    #[tokio::test]
    async fn query_chunks_run_inside_the_owned_pool() {
        let pool = IndexCpuPool::new(1).unwrap();
        let name = pool
            .query_chunk(IndexKind::FullText, || {
                Ok(std::thread::current().name().unwrap_or_default().to_owned())
            })
            .await
            .unwrap();
        assert!(name.starts_with("anvil-index-"));
    }
}
