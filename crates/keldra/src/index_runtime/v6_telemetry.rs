//! Low-cardinality cumulative telemetry for the partition-owned v6 pipeline.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

macro_rules! counters {
    ($($name:ident),+ $(,)?) => {
        pub(crate) struct V6PipelineTelemetry {
            started: Instant,
            $(pub(crate) $name: AtomicU64,)+
        }

        impl Default for V6PipelineTelemetry {
            fn default() -> Self {
                Self {
                    started: Instant::now(),
                    $($name: AtomicU64::new(0),)+
                }
            }
        }
    };
}

counters!(
    source_rows,
    source_bytes,
    hot_raw_hits,
    hot_prepared_hits,
    hot_misses,
    hot_evictions,
    payload_parsed_bytes,
    selected_bytes,
    extracted_bytes,
    prepared_rows,
    prepared_bytes,
    projected_rows,
    projected_bytes,
    sealed_bytes,
    published_source_rows,
    published_source_bytes,
    checkpointed_source_rows,
    checkpointed_source_bytes,
    catalog_directory_publications,
    catalog_activations,
    stage_cpu_nanos,
    stage_queue_wait_nanos,
    stage_resident_bytes,
    stage_limit_bytes,
    local_next_offset,
    local_tail,
    lag_entries,
    lag_oldest_age_millis,
);

static TELEMETRY: OnceLock<Arc<V6PipelineTelemetry>> = OnceLock::new();

pub(crate) fn global() -> &'static Arc<V6PipelineTelemetry> {
    TELEMETRY.get_or_init(|| Arc::new(V6PipelineTelemetry::default()))
}

pub(crate) fn start_summary_task() -> tokio::task::JoinHandle<()> {
    let telemetry = global().clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            telemetry.emit_summary();
        }
    })
}

impl V6PipelineTelemetry {
    pub(crate) fn add(counter: &AtomicU64, value: u64) {
        counter.fetch_add(value, Ordering::Relaxed);
    }

    pub(crate) fn set(gauge: &AtomicU64, value: u64) {
        gauge.store(value, Ordering::Relaxed);
    }

    fn load(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    fn emit_summary(&self) {
        tracing::info!(
            target: "keldra::index_runtime::v6_summary",
            keldra_index_v6_summary_elapsed_milliseconds = self.started.elapsed().as_millis() as u64,
            keldra_index_v6_source_rows_total = Self::load(&self.source_rows),
            keldra_index_v6_source_bytes_total = Self::load(&self.source_bytes),
            keldra_index_v6_hot_raw_hits_total = Self::load(&self.hot_raw_hits),
            keldra_index_v6_hot_prepared_hits_total = Self::load(&self.hot_prepared_hits),
            keldra_index_v6_hot_misses_total = Self::load(&self.hot_misses),
            keldra_index_v6_hot_evictions_total = Self::load(&self.hot_evictions),
            keldra_index_v6_payload_parsed_bytes_total = Self::load(&self.payload_parsed_bytes),
            keldra_index_v6_selected_bytes_total = Self::load(&self.selected_bytes),
            keldra_index_v6_extracted_bytes_total = Self::load(&self.extracted_bytes),
            keldra_index_v6_prepared_rows_total = Self::load(&self.prepared_rows),
            keldra_index_v6_prepared_bytes_total = Self::load(&self.prepared_bytes),
            keldra_index_v6_projected_rows_total = Self::load(&self.projected_rows),
            keldra_index_v6_projected_bytes_total = Self::load(&self.projected_bytes),
            keldra_index_v6_sealed_bytes_total = Self::load(&self.sealed_bytes),
            keldra_index_v6_published_source_rows_total = Self::load(&self.published_source_rows),
            keldra_index_v6_published_source_bytes_total = Self::load(&self.published_source_bytes),
            keldra_index_v6_checkpointed_source_rows_total = Self::load(&self.checkpointed_source_rows),
            keldra_index_v6_checkpointed_source_bytes_total = Self::load(&self.checkpointed_source_bytes),
            keldra_index_v6_catalog_directory_publications_total = Self::load(&self.catalog_directory_publications),
            keldra_index_v6_catalog_activations_total = Self::load(&self.catalog_activations),
            keldra_index_v6_stage_cpu_nanoseconds_total = Self::load(&self.stage_cpu_nanos),
            keldra_index_v6_stage_queue_wait_nanoseconds_total = Self::load(&self.stage_queue_wait_nanos),
            keldra_index_v6_stage_resident_bytes = Self::load(&self.stage_resident_bytes),
            keldra_index_v6_stage_limit_bytes = Self::load(&self.stage_limit_bytes),
            keldra_index_v6_local_next_offset = Self::load(&self.local_next_offset),
            keldra_index_v6_local_tail = Self::load(&self.local_tail),
            keldra_index_v6_lag_entries = Self::load(&self.lag_entries),
            keldra_index_v6_lag_oldest_age_milliseconds = Self::load(&self.lag_oldest_age_millis),
            "keldra_index_v6_summary"
        );
    }
}
