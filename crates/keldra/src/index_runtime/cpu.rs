//! The one process-owned CPU pool for non-async index and query work.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use keldra_index::IndexError;
use keldra_index::compaction::{CompactionExecutor, CompactionTaskFuture, CompactionTaskHandle};
use thiserror::Error;

#[derive(Clone)]
pub(crate) struct IndexCpuPool {
    pool: Arc<rayon::ThreadPool>,
    workers: usize,
}

/// Cloneable query scheduler over the one configured index CPU pool.
///
/// Its lane bound controls independent async partition work as well as CPU
/// submissions. It never creates threads or admits more CPU lanes than the
/// process-wide indexing worker count.
#[derive(Clone)]
pub(crate) struct IndexQueryScheduler {
    cpu: IndexCpuPool,
    maximum_parallelism: usize,
    partition_permits: Arc<tokio::sync::Semaphore>,
}

/// Runtime bridge used by storage-neutral parallel index compaction.
#[derive(Clone)]
pub(crate) struct IndexCompactionExecutor {
    cpu: IndexCpuPool,
}

struct CancelQueuedCpuWork(Option<Arc<AtomicBool>>);

impl CancelQueuedCpuWork {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for CancelQueuedCpuWork {
    fn drop(&mut self) {
        if let Some(cancelled) = self.0.take() {
            cancelled.store(true, Ordering::Release);
        }
    }
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
    span: tracing::Span,
}

impl Drop for QueryCpuActiveGuard {
    fn drop(&mut self) {
        self.span.in_scope(|| {
            tracing::debug!(
                index.kind = "typed_json",
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
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .thread_name(|index| format!("keldra-index-worker-{index}"))
            .build()
            .map_err(|error| IndexCpuPoolError::Build(error.to_string()))?;
        Ok(Self {
            pool: Arc::new(pool),
            workers,
        })
    }

    pub(crate) fn workers(&self) -> usize {
        self.workers
    }

    pub(crate) fn query_scheduler(&self, maximum_parallelism: usize) -> IndexQueryScheduler {
        let maximum_parallelism = maximum_parallelism.clamp(1, self.workers);
        IndexQueryScheduler {
            cpu: self.clone(),
            maximum_parallelism,
            partition_permits: Arc::new(tokio::sync::Semaphore::new(maximum_parallelism)),
        }
    }

    /// Run CPU work inside Keldra's pool, never Rayon's global registry.
    pub(crate) async fn install<F, T>(&self, work: F) -> Result<T, IndexCpuPoolError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let pool = self.pool.clone();
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
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = Arc::clone(&cancelled);
        let mut cancel_on_drop = CancelQueuedCpuWork(Some(cancelled));
        self.pool.spawn(move || {
            if worker_cancelled.load(Ordering::Acquire) {
                return;
            }
            let outcome = catch_unwind(AssertUnwindSafe(work));
            let _ = sender.send(outcome);
        });
        let result = match receiver.await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_)) => Err(IndexCpuPoolError::Task(
                "index CPU task panicked".to_owned(),
            )),
            Err(error) => Err(IndexCpuPoolError::Task(error.to_string())),
        };
        // The work has either completed or the pool has closed its sender.
        // Prevent the cancellation guard from changing a completed outcome.
        cancel_on_drop.disarm();
        result
    }

    /// Execute one already-materialized query CPU chunk on the process-owned
    /// Rayon pool. Async artifact I/O happens before this boundary.
    pub(crate) async fn query_chunk<F, T>(&self, work: F) -> Result<T, IndexError>
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
                index.kind = "typed_json",
                counter.keldra_index_query_cpu_waiting = 1_i64,
                "index query CPU chunk queued"
            );
        });
        let worker_span = span.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = Arc::clone(&cancelled);
        let mut cancel_on_drop = CancelQueuedCpuWork(Some(cancelled));
        self.pool.spawn(move || {
            if worker_cancelled.load(Ordering::Acquire) {
                worker_span.in_scope(|| {
                    tracing::debug!(
                        index.kind = "typed_json",
                        counter.keldra_index_query_cpu_waiting = -1_i64,
                        "cancelled index query CPU queue wait released"
                    );
                });
                return;
            }
            worker_started.store(true, Ordering::Release);
            let queue_seconds = enqueued.elapsed().as_secs_f64();
            worker_span.in_scope(|| {
                tracing::debug!(
                    index.kind = "typed_json",
                    counter.keldra_index_query_cpu_waiting = -1_i64,
                    "index query CPU queue wait released"
                );
                tracing::debug!(
                    index.kind = "typed_json",
                    counter.keldra_index_query_cpu_active = 1_i64,
                    "index query CPU chunk started"
                );
            });
            let _active = QueryCpuActiveGuard {
                span: worker_span.clone(),
            };
            let cpu_started = std::time::Instant::now();
            let result = catch_unwind(AssertUnwindSafe(work));
            let cpu_seconds = cpu_started.elapsed().as_secs_f64();
            let _ = sender.send((result, queue_seconds, cpu_seconds));
        });
        let execution = receiver.await;
        cancel_on_drop.disarm();
        let (result, queue_seconds, cpu_seconds) = match execution {
            Ok((Ok(result), queue_seconds, cpu_seconds)) => (result, queue_seconds, cpu_seconds),
            Ok((Err(_), queue_seconds, cpu_seconds)) => (
                Err(IndexError::Io("index query CPU task panicked".to_owned())),
                queue_seconds,
                cpu_seconds,
            ),
            Err(error) => {
                if !started.load(Ordering::Acquire) {
                    span.in_scope(|| {
                        tracing::debug!(
                            index.kind = "typed_json",
                            counter.keldra_index_query_cpu_waiting = -1_i64,
                            "index query CPU queue wait released after task failure"
                        );
                    });
                }
                span.in_scope(|| {
                    tracing::warn!(
                        index.kind = "typed_json",
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
                    index.kind = "typed_json",
                    query.outcome = "failed",
                    monotonic_counter.keldra_index_query_cpu_chunks_total = 1_u64,
                    monotonic_counter.keldra_index_query_cpu_failures_total = 1_u64,
                    histogram.keldra_index_query_cpu_queue_seconds = queue_seconds,
                    histogram.keldra_index_query_cpu_seconds = cpu_seconds,
                    "index query CPU chunk failed"
                );
            } else {
                tracing::debug!(
                    index.kind = "typed_json",
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

impl IndexQueryScheduler {
    pub(crate) fn maximum_parallelism(&self) -> usize {
        self.maximum_parallelism
    }

    /// Hold one scheduler-wide query-partition lane for the complete async job.
    /// All clones of a configured scheduler share this admission bound.
    pub(crate) async fn run_partition<O, F, Fut>(&self, operation: F) -> O
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = O>,
    {
        let permit = self
            .partition_permits
            .clone()
            .acquire_owned()
            .await
            .expect("the query scheduler's partition semaphore is never closed");
        let output = operation().await;
        drop(permit);
        output
    }

    /// Run one already materialized query job on the shared index CPU pool.
    /// Bounded partition orchestration lives in `v1_parallel` so the generic
    /// pool remains independent of query result ordering and join policy.
    pub(crate) async fn run_cpu<O, F>(&self, operation: F) -> Result<O, IndexError>
    where
        O: Send + 'static,
        F: FnOnce() -> Result<O, IndexError> + Send + 'static,
    {
        self.cpu.query_chunk(operation).await
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
            .query_chunk(|| Ok(std::thread::current().name().unwrap_or_default().to_owned()))
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
    async fn cancelling_a_queued_submission_skips_obsolete_cpu_work() {
        use std::sync::atomic::AtomicUsize;

        let pool = IndexCpuPool::new(1).unwrap();
        let blocker_started = Arc::new(AtomicBool::new(false));
        let blocker_release = Arc::new(AtomicBool::new(false));
        let started = Arc::clone(&blocker_started);
        let release = Arc::clone(&blocker_release);
        let blocker = tokio::spawn({
            let pool = pool.clone();
            async move {
                pool.submit(move || {
                    started.store(true, Ordering::Release);
                    while !release.load(Ordering::Acquire) {
                        std::thread::yield_now();
                    }
                })
                .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !blocker_started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first CPU submission should occupy the only worker");

        let executions = Arc::new(AtomicUsize::new(0));
        let executions_by_work = Arc::clone(&executions);
        let obsolete = tokio::spawn({
            let pool = pool.clone();
            async move {
                pool.submit(move || {
                    executions_by_work.fetch_add(1, Ordering::Release);
                })
                .await
            }
        });
        tokio::task::yield_now().await;
        obsolete.abort();
        let _ = obsolete.await;
        blocker_release.store(true, Ordering::Release);
        blocker.await.unwrap().unwrap();

        // A final submission is a barrier proving that the cancelled work's
        // queue position has been consumed by the Rayon worker.
        pool.submit(|| ()).await.unwrap();
        assert_eq!(executions.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn cancelling_a_queued_query_chunk_skips_obsolete_cpu_work() {
        use std::sync::atomic::AtomicUsize;

        let pool = IndexCpuPool::new(1).unwrap();
        let blocker_started = Arc::new(AtomicBool::new(false));
        let blocker_release = Arc::new(AtomicBool::new(false));
        let started = Arc::clone(&blocker_started);
        let release = Arc::clone(&blocker_release);
        let blocker = tokio::spawn({
            let pool = pool.clone();
            async move {
                pool.submit(move || {
                    started.store(true, Ordering::Release);
                    while !release.load(Ordering::Acquire) {
                        std::thread::yield_now();
                    }
                })
                .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !blocker_started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first CPU submission should occupy the only worker");

        let executions = Arc::new(AtomicUsize::new(0));
        let executions_by_work = Arc::clone(&executions);
        let queued = Arc::new(AtomicBool::new(false));
        let queued_by_task = Arc::clone(&queued);
        let obsolete = tokio::spawn({
            let pool = pool.clone();
            async move {
                queued_by_task.store(true, Ordering::Release);
                pool.query_chunk(move || {
                    executions_by_work.fetch_add(1, Ordering::Release);
                    Ok(())
                })
                .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !queued.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the obsolete query chunk should reach the pool queue");
        obsolete.abort();
        let _ = obsolete.await;
        blocker_release.store(true, Ordering::Release);
        blocker.await.unwrap().unwrap();

        pool.submit(|| ()).await.unwrap();
        assert_eq!(executions.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn cancelling_partition_work_releases_its_scheduler_lane() {
        let scheduler = IndexCpuPool::new(1).unwrap().query_scheduler(1);
        let entered = Arc::new(tokio::sync::Notify::new());
        let entered_by_task = Arc::clone(&entered);
        let running = tokio::spawn({
            let scheduler = scheduler.clone();
            async move {
                scheduler
                    .run_partition(move || async move {
                        entered_by_task.notify_one();
                        std::future::pending::<()>().await;
                    })
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
            .await
            .expect("the first partition should acquire the only scheduler lane");

        running.abort();
        let _ = running.await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            scheduler.run_partition(|| async {}).await;
        })
        .await
        .expect("cancelling partition work should release its scheduler lane");
    }

    #[tokio::test]
    async fn query_scheduler_shares_the_configured_pool_without_oversubscription() {
        let pool = IndexCpuPool::new(3).unwrap();
        let background_name = pool
            .submit(|| std::thread::current().name().unwrap_or_default().to_owned())
            .await
            .unwrap();
        let scheduler = pool.query_scheduler(usize::MAX);
        let query_name = scheduler
            .run_cpu(|| Ok(std::thread::current().name().unwrap_or_default().to_owned()))
            .await
            .unwrap();

        assert!(background_name.starts_with("keldra-index-worker-"));
        assert_eq!(pool.workers(), 3);
        assert_eq!(scheduler.maximum_parallelism(), 3);
        assert!(query_name.starts_with("keldra-index-worker-"));
    }
}
