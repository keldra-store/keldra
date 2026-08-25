//! The one process-owned CPU pool for non-async index projection work.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use keldra_index::compaction::{CompactionExecutor, CompactionTaskFuture, CompactionTaskHandle};
use keldra_index::{IndexError, IndexKind};
use thiserror::Error;

#[derive(Clone)]
pub(crate) struct IndexCpuPool {
    background: Arc<rayon::ThreadPool>,
    query: Arc<rayon::ThreadPool>,
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
            tracing::debug!(
                index.kind = ?self.kind,
                counter.keldra_index_query_cpu_active = -1_i64,
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
        let background_workers = workers.saturating_sub(1).max(1);
        let background = rayon::ThreadPoolBuilder::new()
            .num_threads(background_workers)
            .thread_name(|index| format!("keldra-index-background-{index}"))
            .build()
            .map_err(|error| IndexCpuPoolError::Build(error.to_string()))?;
        let query = if workers == 1 {
            None
        } else {
            Some(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(1)
                    .thread_name(|index| format!("keldra-index-query-{index}"))
                    .build()
                    .map_err(|error| IndexCpuPoolError::Build(error.to_string()))?,
            )
        };
        let background = Arc::new(background);
        Ok(Self {
            query: query.map_or_else(|| background.clone(), Arc::new),
            background,
            workers: background_workers,
        })
    }

    pub(crate) fn workers(&self) -> usize {
        self.workers
    }

    /// Run CPU work inside Keldra's pool, never Rayon's global registry.
    pub(crate) async fn install<F, T>(&self, work: F) -> Result<T, IndexCpuPoolError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let pool = self.background.clone();
        tokio::task::spawn_blocking(move || pool.install(work))
            .await
            .map_err(|error| IndexCpuPoolError::Task(error.to_string()))
    }

    /// Submit one finite CPU unit without occupying a Tokio blocking thread
    /// while it waits for a Rayon worker.
    ///
    /// Projection lanes use this boundary one source at a time. A completed
    /// result can therefore wait on async consumer backpressure without
    /// retaining the Rayon worker needed by nested index work such as an
    /// external-sort spill.
    pub(crate) async fn submit<F, T>(&self, work: F) -> Result<T, IndexCpuPoolError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.background.spawn(move || {
            let outcome = catch_unwind(AssertUnwindSafe(work));
            let _ = sender.send(outcome);
        });
        match receiver.await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_)) => Err(IndexCpuPoolError::Task(
                "index CPU task panicked".to_owned(),
            )),
            Err(error) => Err(IndexCpuPoolError::Task(error.to_string())),
        }
    }

    /// Execute one already-materialized query CPU chunk on the process-owned
    /// Rayon pool. Async artifact I/O happens before this boundary.
    pub(crate) async fn query_chunk<F, T>(&self, kind: IndexKind, work: F) -> Result<T, IndexError>
    where
        F: FnOnce() -> Result<T, IndexError> + Send + 'static,
        T: Send + 'static,
    {
        // These events carry the OTLP counters and histograms for every
        // bounded CPU chunk. The metrics and OpenTelemetry layers are
        // intentionally unfiltered, so DEBUG preserves those signals while
        // keeping the default INFO console log bounded by query-level events.
        let enqueued = std::time::Instant::now();
        let started = Arc::new(AtomicBool::new(false));
        let worker_started = Arc::clone(&started);
        // One public query can execute millions of bounded CPU chunks. Keep
        // their metrics on the enclosing query span instead of creating one
        // exported trace span per chunk.
        let span = tracing::Span::current();
        span.in_scope(|| {
            tracing::debug!(
                index.kind = ?kind,
                counter.keldra_index_query_cpu_waiting = 1_i64,
                "index query CPU chunk queued"
            );
        });
        let worker_span = span.clone();
        let pool = self.query.clone();
        let execution = tokio::task::spawn_blocking(move || {
            pool.install(move || {
                worker_started.store(true, Ordering::Release);
                let queue_seconds = enqueued.elapsed().as_secs_f64();
                worker_span.in_scope(|| {
                    tracing::debug!(
                        index.kind = ?kind,
                        counter.keldra_index_query_cpu_waiting = -1_i64,
                        "index query CPU queue wait released"
                    );
                    tracing::debug!(
                        index.kind = ?kind,
                        counter.keldra_index_query_cpu_active = 1_i64,
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
                (result, queue_seconds, cpu_seconds)
            })
        })
        .await
        .map_err(|error| IndexCpuPoolError::Task(error.to_string()));
        let (result, queue_seconds, cpu_seconds) = match execution {
            Ok(execution) => execution,
            Err(error) => {
                if !started.load(Ordering::Acquire) {
                    span.in_scope(|| {
                        tracing::debug!(
                            index.kind = ?kind,
                            counter.keldra_index_query_cpu_waiting = -1_i64,
                            "index query CPU queue wait released after task failure"
                        );
                    });
                }
                span.in_scope(|| {
                    tracing::warn!(
                        index.kind = ?kind,
                        query.outcome = "failed",
                        monotonic_counter.keldra_index_query_cpu_chunks_total = 1_u64,
                        monotonic_counter.keldra_index_query_cpu_failures_total = 1_u64,
                        %error,
                        "index query CPU task failed"
                    );
                });
                return Err(IndexError::Io(error.to_string()));
            }
        };
        let failed = result.is_err();
        span.in_scope(|| {
            if failed {
                tracing::warn!(
                    index.kind = ?kind,
                    query.outcome = "failed",
                    monotonic_counter.keldra_index_query_cpu_chunks_total = 1_u64,
                    monotonic_counter.keldra_index_query_cpu_failures_total = 1_u64,
                    histogram.keldra_index_query_cpu_queue_seconds = queue_seconds,
                    histogram.keldra_index_query_cpu_seconds = cpu_seconds,
                    "index query CPU chunk failed"
                );
            } else {
                tracing::debug!(
                    index.kind = ?kind,
                    query.outcome = "completed",
                    monotonic_counter.keldra_index_query_cpu_chunks_total = 1_u64,
                    monotonic_counter.keldra_index_query_cpu_failures_total = 0_u64,
                    histogram.keldra_index_query_cpu_queue_seconds = queue_seconds,
                    histogram.keldra_index_query_cpu_seconds = cpu_seconds,
                    "index query CPU chunk completed"
                );
            }
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
        assert!(name.starts_with("keldra-index-"));
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
        assert!(name.starts_with("keldra-index-"));
    }

    #[tokio::test]
    async fn submitted_work_runs_inside_the_owned_pool() {
        let pool = IndexCpuPool::new(1).unwrap();
        let name = pool
            .submit(|| std::thread::current().name().unwrap_or_default().to_owned())
            .await
            .unwrap();
        assert!(name.starts_with("keldra-index-"));
    }

    #[tokio::test]
    async fn query_cpu_has_reserved_capacity_when_multiple_workers_are_configured() {
        let pool = IndexCpuPool::new(2).unwrap();
        let background_name = pool
            .submit(|| std::thread::current().name().unwrap_or_default().to_owned())
            .await
            .unwrap();
        let query_name = pool
            .query_chunk(IndexKind::FullText, || {
                Ok(std::thread::current().name().unwrap_or_default().to_owned())
            })
            .await
            .unwrap();

        assert!(background_name.starts_with("keldra-index-background-"));
        assert!(query_name.starts_with("keldra-index-query-"));
        assert_eq!(pool.workers(), 1);
    }
}
