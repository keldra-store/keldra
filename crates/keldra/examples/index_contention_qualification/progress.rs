use crate::metrics::{Latencies, LatencyReport};
use anyhow::{Context, Result};
use serde::Serialize;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, watch},
    time::Duration,
};

#[derive(Default)]
pub struct Counters {
    pub scheduled: AtomicU64,
    pub completed: AtomicU64,
    pub dropped: AtomicU64,
    pub errors: AtomicU64,
    pub timeouts: AtomicU64,
    pub mutations: AtomicU64,
    pub mutation_errors: AtomicU64,
    pub pagination_attempts: AtomicU64,
    pub pagination_attempts_completed: AtomicU64,
    pub pagination_attempt_failures: AtomicU64,
    pub pagination_pages_completed: AtomicU64,
    pub pagination_page_failures: AtomicU64,
    pagination_furthest: StdMutex<(u64, u64)>,
    pub pagination_last_page_microseconds: AtomicU64,
    pub pagination_maximum_page_microseconds: AtomicU64,
    pub pagination_last_attempt_microseconds: AtomicU64,
    pub latest_commit_revision: AtomicU64,
    pub latest_source_lag_hint: AtomicU64,
    pub maximum_source_lag_hint: AtomicU64,
    phase_generation: AtomicU64,
    query_latencies: Mutex<Option<Latencies>>,
    phase: Mutex<String>,
}

#[derive(Serialize)]
struct Snapshot {
    schema: &'static str,
    timestamp_unix_milliseconds: u128,
    elapsed_seconds: f64,
    phase_elapsed_seconds: f64,
    phase: String,
    scheduled_queries: u64,
    completed_queries: u64,
    dropped_queries: u64,
    query_errors: u64,
    query_timeouts: u64,
    query_latency: LatencyReport,
    accepted_mutations: u64,
    mutation_errors: u64,
    pagination_attempts: u64,
    pagination_attempts_completed: u64,
    pagination_attempt_failures: u64,
    pagination_pages_completed: u64,
    pagination_page_failures: u64,
    pagination_furthest_page: u64,
    pagination_furthest_hits: u64,
    pagination_last_page_milliseconds: f64,
    pagination_maximum_page_milliseconds: f64,
    pagination_last_attempt_milliseconds: f64,
    latest_observed_commit_revision: u64,
    latest_source_lag_hint: u64,
    maximum_source_lag_hint: u64,
}

impl Counters {
    pub async fn new() -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            query_latencies: Mutex::new(Some(Latencies::new()?)),
            ..Self::default()
        }))
    }

    pub async fn phase(&self, value: &str) {
        *self.phase.lock().await = value.to_owned();
        *self.query_latencies.lock().await = Latencies::new().ok();
        self.latest_source_lag_hint.store(0, Ordering::Relaxed);
        self.maximum_source_lag_hint.store(0, Ordering::Relaxed);
        self.phase_generation.fetch_add(1, Ordering::Relaxed);
    }
    pub async fn query_completed(&self, elapsed: Duration, revision: u64, lag_hint: u64) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.latest_commit_revision
            .fetch_max(revision, Ordering::Relaxed);
        self.latest_source_lag_hint
            .store(lag_hint, Ordering::Relaxed);
        self.maximum_source_lag_hint
            .fetch_max(lag_hint, Ordering::Relaxed);
        if let Some(histogram) = self.query_latencies.lock().await.as_mut() {
            let _ = histogram.record(elapsed);
        }
    }

    pub fn pagination_attempt_started(&self) {
        self.pagination_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn pagination_attempt_completed(&self, elapsed: Duration) {
        self.pagination_attempts_completed
            .fetch_add(1, Ordering::Relaxed);
        self.record_pagination_attempt_elapsed(elapsed);
    }

    pub fn pagination_attempt_failed(&self, elapsed: Duration) {
        self.pagination_attempt_failures
            .fetch_add(1, Ordering::Relaxed);
        self.record_pagination_attempt_elapsed(elapsed);
    }

    pub fn pagination_page_completed(
        &self,
        page: usize,
        accumulated_hits: usize,
        elapsed: Duration,
    ) {
        self.pagination_pages_completed
            .fetch_add(1, Ordering::Relaxed);
        let observation = (page as u64, accumulated_hits as u64);
        let mut furthest = self
            .pagination_furthest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if observation > *furthest {
            *furthest = observation;
        }
        drop(furthest);
        self.record_pagination_page_elapsed(elapsed);
    }

    pub fn pagination_page_failed(&self, elapsed: Duration) {
        self.pagination_page_failures
            .fetch_add(1, Ordering::Relaxed);
        self.record_pagination_page_elapsed(elapsed);
    }

    fn record_pagination_page_elapsed(&self, elapsed: Duration) {
        let elapsed = duration_microseconds(elapsed);
        self.pagination_last_page_microseconds
            .store(elapsed, Ordering::Relaxed);
        self.pagination_maximum_page_microseconds
            .fetch_max(elapsed, Ordering::Relaxed);
    }

    fn record_pagination_attempt_elapsed(&self, elapsed: Duration) {
        self.pagination_last_attempt_microseconds
            .store(duration_microseconds(elapsed), Ordering::Relaxed);
    }

    async fn snapshot(&self, started: Instant, phase_elapsed: Duration) -> Snapshot {
        let latency = self
            .query_latencies
            .lock()
            .await
            .as_ref()
            .map(Latencies::report)
            .unwrap_or_default();
        let pagination_furthest = *self
            .pagination_furthest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Snapshot {
            schema: "keldra.index-contention.progress.v1",
            timestamp_unix_milliseconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            elapsed_seconds: started.elapsed().as_secs_f64(),
            phase_elapsed_seconds: phase_elapsed.as_secs_f64(),
            phase: self.phase.lock().await.clone(),
            scheduled_queries: self.scheduled.load(Ordering::Relaxed),
            completed_queries: self.completed.load(Ordering::Relaxed),
            dropped_queries: self.dropped.load(Ordering::Relaxed),
            query_errors: self.errors.load(Ordering::Relaxed),
            query_timeouts: self.timeouts.load(Ordering::Relaxed),
            query_latency: latency,
            accepted_mutations: self.mutations.load(Ordering::Relaxed),
            mutation_errors: self.mutation_errors.load(Ordering::Relaxed),
            pagination_attempts: self.pagination_attempts.load(Ordering::Relaxed),
            pagination_attempts_completed: self
                .pagination_attempts_completed
                .load(Ordering::Relaxed),
            pagination_attempt_failures: self.pagination_attempt_failures.load(Ordering::Relaxed),
            pagination_pages_completed: self.pagination_pages_completed.load(Ordering::Relaxed),
            pagination_page_failures: self.pagination_page_failures.load(Ordering::Relaxed),
            pagination_furthest_page: pagination_furthest.0,
            pagination_furthest_hits: pagination_furthest.1,
            pagination_last_page_milliseconds: self
                .pagination_last_page_microseconds
                .load(Ordering::Relaxed) as f64
                / 1_000.0,
            pagination_maximum_page_milliseconds: self
                .pagination_maximum_page_microseconds
                .load(Ordering::Relaxed) as f64
                / 1_000.0,
            pagination_last_attempt_milliseconds: self
                .pagination_last_attempt_microseconds
                .load(Ordering::Relaxed) as f64
                / 1_000.0,
            latest_observed_commit_revision: self.latest_commit_revision.load(Ordering::Relaxed),
            latest_source_lag_hint: self.latest_source_lag_hint.load(Ordering::Relaxed),
            maximum_source_lag_hint: self.maximum_source_lag_hint.load(Ordering::Relaxed),
        }
    }
}

fn duration_microseconds(elapsed: Duration) -> u64 {
    elapsed.as_micros().min(u64::MAX as u128) as u64
}

pub fn start(
    path: Option<PathBuf>,
    counters: Arc<Counters>,
    mut stop: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let Some(path) = path else {
            return Ok(());
        };
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("open progress output {}", path.display()))?;
        let started = Instant::now();
        let mut phase_started = Instant::now();
        let mut phase_generation = counters.phase_generation.load(Ordering::Relaxed);
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    let current_generation = counters.phase_generation.load(Ordering::Relaxed);
                    if current_generation != phase_generation { phase_generation = current_generation; phase_started = Instant::now(); }
                    let mut encoded = serde_json::to_vec(&counters.snapshot(started, phase_started.elapsed()).await)?;
                    encoded.push(b'\n'); file.write_all(&encoded).await?; file.flush().await?;
                }
                changed = stop.changed() => { if changed.is_err() || *stop.borrow() { break; } }
            }
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pagination_counters_distinguish_attempts_pages_and_failures() {
        let counters = Counters::default();
        counters.pagination_attempt_started();
        counters.pagination_page_completed(1, 1_000, Duration::from_millis(12));
        counters.pagination_page_completed(2, 1_750, Duration::from_millis(18));
        counters.pagination_attempt_completed(Duration::from_millis(31));
        counters.pagination_attempt_started();
        counters.pagination_page_completed(1, 900, Duration::from_millis(8));
        counters.pagination_page_failed(Duration::from_millis(25));
        counters.pagination_attempt_failed(Duration::from_millis(34));

        assert_eq!(counters.pagination_attempts.load(Ordering::Relaxed), 2);
        assert_eq!(
            counters
                .pagination_attempts_completed
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            counters.pagination_attempt_failures.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            counters.pagination_pages_completed.load(Ordering::Relaxed),
            3
        );
        assert_eq!(counters.pagination_page_failures.load(Ordering::Relaxed), 1);
        assert_eq!(
            *counters
                .pagination_furthest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            (2, 1_750)
        );
        assert_eq!(
            counters
                .pagination_last_page_microseconds
                .load(Ordering::Relaxed),
            25_000
        );
        assert_eq!(
            counters
                .pagination_maximum_page_microseconds
                .load(Ordering::Relaxed),
            25_000
        );
        assert_eq!(
            counters
                .pagination_last_attempt_microseconds
                .load(Ordering::Relaxed),
            34_000
        );
    }

    #[test]
    fn pagination_furthest_page_and_hits_remain_one_observation() {
        let counters = Counters::default();

        counters.pagination_page_completed(5, 10, Duration::ZERO);
        counters.pagination_page_completed(4, 10_000, Duration::ZERO);
        assert_eq!(
            *counters
                .pagination_furthest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            (5, 10)
        );

        counters.pagination_page_completed(5, 20, Duration::ZERO);
        assert_eq!(
            *counters
                .pagination_furthest
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            (5, 20)
        );
    }
}
