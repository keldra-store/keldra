use crate::metrics::{Latencies, LatencyReport};
use anyhow::{Context, Result};
use serde::Serialize;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
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

    async fn snapshot(&self, started: Instant, phase_elapsed: Duration) -> Snapshot {
        let latency = self
            .query_latencies
            .lock()
            .await
            .as_ref()
            .map(Latencies::report)
            .unwrap_or_default();
        Snapshot {
            schema: "keldra.index-contention.progress.v1",
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
            latest_observed_commit_revision: self.latest_commit_revision.load(Ordering::Relaxed),
            latest_source_lag_hint: self.latest_source_lag_hint.load(Ordering::Relaxed),
            maximum_source_lag_hint: self.maximum_source_lag_hint.load(Ordering::Relaxed),
        }
    }
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
