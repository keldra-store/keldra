//! Low-cardinality cumulative telemetry for the partition-owned v1 pipeline.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

macro_rules! counters {
    ($($name:ident),+ $(,)?) => {
        pub(crate) struct V1PipelineTelemetry {
            started: Instant,
            next_preparation_id: AtomicU64,
            in_flight_preparations: Mutex<BTreeMap<u64, InFlightPreparation>>,
            $(pub(crate) $name: AtomicU64,)+
        }

        impl Default for V1PipelineTelemetry {
            fn default() -> Self {
                Self {
                    started: Instant::now(),
                    next_preparation_id: AtomicU64::new(1),
                    in_flight_preparations: Mutex::new(BTreeMap::new()),
                    $($name: AtomicU64::new(0),)+
                }
            }
        }
    };
}

counters!(
    hot_raw_hits,
    hot_prepared_hits,
    hot_misses,
    hot_evictions,
    hot_admissions,
    hot_superseded,
    hot_stale_preparations,
    payload_parsed_bytes,
    selected_bytes,
    extracted_bytes,
    prepared_rows,
    prepared_bytes,
    projected_rows,
    projected_bytes,
    sealed_bytes,
    checkpointed_source_positions,
    checkpointed_source_payload_bytes,
    catalog_checkpointed_source_positions,
    catalog_checkpointed_source_payload_bytes,
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
    oldest_no_progress_age_millis,
    stalled_partitions,
    retrying_partitions,
    halted_partitions,
);

static TELEMETRY: OnceLock<Arc<V1PipelineTelemetry>> = OnceLock::new();

#[derive(Clone, Copy)]
struct InFlightPreparation {
    started_at: Instant,
    stall_after: Duration,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct InFlightPreparationSnapshot {
    count: u64,
    oldest_age_milliseconds: u64,
    stalled: u64,
}

pub(crate) struct InFlightPreparationGuard {
    telemetry: Arc<V1PipelineTelemetry>,
    id: u64,
}

impl Drop for InFlightPreparationGuard {
    fn drop(&mut self) {
        self.telemetry
            .in_flight_preparations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.id);
    }
}

pub(crate) fn global() -> &'static Arc<V1PipelineTelemetry> {
    TELEMETRY.get_or_init(|| Arc::new(V1PipelineTelemetry::default()))
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

impl V1PipelineTelemetry {
    pub(crate) fn add(counter: &AtomicU64, value: u64) {
        counter.fetch_add(value, Ordering::Relaxed);
    }

    pub(crate) fn set(gauge: &AtomicU64, value: u64) {
        gauge.store(value, Ordering::Relaxed);
    }

    fn load(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    pub(crate) fn begin_in_flight_preparation(
        self: &Arc<Self>,
        stall_after: Duration,
    ) -> InFlightPreparationGuard {
        self.begin_in_flight_preparation_at(Instant::now(), stall_after)
    }

    fn begin_in_flight_preparation_at(
        self: &Arc<Self>,
        started_at: Instant,
        stall_after: Duration,
    ) -> InFlightPreparationGuard {
        let id = self.next_preparation_id.fetch_add(1, Ordering::Relaxed);
        self.in_flight_preparations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                id,
                InFlightPreparation {
                    started_at,
                    stall_after,
                },
            );
        InFlightPreparationGuard {
            telemetry: self.clone(),
            id,
        }
    }

    fn in_flight_preparation_snapshot(&self, now: Instant) -> InFlightPreparationSnapshot {
        let preparations = self
            .in_flight_preparations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        preparations.values().fold(
            InFlightPreparationSnapshot::default(),
            |mut snapshot, preparation| {
                let age = now.saturating_duration_since(preparation.started_at);
                snapshot.count = snapshot.count.saturating_add(1);
                snapshot.oldest_age_milliseconds = snapshot
                    .oldest_age_milliseconds
                    .max(age.as_millis().min(u128::from(u64::MAX)) as u64);
                snapshot.stalled = snapshot
                    .stalled
                    .saturating_add(u64::from(age >= preparation.stall_after));
                snapshot
            },
        )
    }

    fn no_progress_summary(&self, now: Instant) -> (InFlightPreparationSnapshot, u64, u64) {
        let preparation = self.in_flight_preparation_snapshot(now);
        let oldest_no_progress = Self::load(&self.oldest_no_progress_age_millis)
            .max(preparation.oldest_age_milliseconds);
        let stalled_partitions = Self::load(&self.stalled_partitions).max(preparation.stalled);
        (preparation, oldest_no_progress, stalled_partitions)
    }

    fn emit_summary(&self) {
        let (preparation, oldest_no_progress, stalled_partitions) =
            self.no_progress_summary(Instant::now());
        tracing::info!(
            target: "keldra::index_runtime::v1_summary",
            keldra_index_v1_summary_elapsed_milliseconds = self.started.elapsed().as_millis() as u64,
            keldra_index_v1_hot_raw_hits_total = Self::load(&self.hot_raw_hits),
            keldra_index_v1_hot_prepared_hits_total = Self::load(&self.hot_prepared_hits),
            keldra_index_v1_hot_misses_total = Self::load(&self.hot_misses),
            keldra_index_v1_hot_evictions_total = Self::load(&self.hot_evictions),
            keldra_index_v1_hot_admissions_total = Self::load(&self.hot_admissions),
            keldra_index_v1_hot_superseded_total = Self::load(&self.hot_superseded),
            keldra_index_v1_hot_stale_preparations_total = Self::load(&self.hot_stale_preparations),
            keldra_index_v1_payload_parsed_bytes_total = Self::load(&self.payload_parsed_bytes),
            keldra_index_v1_selected_bytes_total = Self::load(&self.selected_bytes),
            keldra_index_v1_extracted_bytes_total = Self::load(&self.extracted_bytes),
            keldra_index_v1_prepared_rows_total = Self::load(&self.prepared_rows),
            keldra_index_v1_prepared_bytes_total = Self::load(&self.prepared_bytes),
            keldra_index_v1_projected_rows_total = Self::load(&self.projected_rows),
            keldra_index_v1_projected_bytes_total = Self::load(&self.projected_bytes),
            keldra_index_v1_sealed_bytes_total = Self::load(&self.sealed_bytes),
            keldra_index_v1_checkpointed_source_positions_total = Self::load(&self.checkpointed_source_positions),
            keldra_index_v1_checkpointed_source_payload_bytes_total = Self::load(&self.checkpointed_source_payload_bytes),
            keldra_index_v1_catalog_checkpointed_source_positions_total = Self::load(&self.catalog_checkpointed_source_positions),
            keldra_index_v1_catalog_checkpointed_source_payload_bytes_total = Self::load(&self.catalog_checkpointed_source_payload_bytes),
            keldra_index_v1_catalog_directory_publications_total = Self::load(&self.catalog_directory_publications),
            keldra_index_v1_catalog_activations_total = Self::load(&self.catalog_activations),
            keldra_index_v1_stage_cpu_nanoseconds_total = Self::load(&self.stage_cpu_nanos),
            keldra_index_v1_stage_queue_wait_nanoseconds_total = Self::load(&self.stage_queue_wait_nanos),
            keldra_index_v1_stage_resident_bytes = Self::load(&self.stage_resident_bytes),
            keldra_index_v1_stage_limit_bytes = Self::load(&self.stage_limit_bytes),
            keldra_index_v1_local_next_offset = Self::load(&self.local_next_offset),
            keldra_index_v1_local_tail = Self::load(&self.local_tail),
            keldra_index_v1_lag_entries = Self::load(&self.lag_entries),
            keldra_index_v1_lag_oldest_age_milliseconds = Self::load(&self.lag_oldest_age_millis),
            keldra_index_v1_oldest_no_progress_age_milliseconds = oldest_no_progress,
            keldra_index_v1_stalled_partitions = stalled_partitions,
            keldra_index_v1_retrying_partitions = Self::load(&self.retrying_partitions),
            keldra_index_v1_halted_partitions = Self::load(&self.halted_partitions),
            keldra_index_v1_in_flight_preparing_partitions = preparation.count,
            keldra_index_v1_oldest_in_flight_preparation_age_milliseconds = preparation.oldest_age_milliseconds,
            keldra_index_v1_in_flight_preparation_stalled_partitions = preparation.stalled,
            "keldra_index_v1_summary"
        );
        for snapshot in keldra_index::hash_profile_snapshots() {
            tracing::info!(
                target: "keldra::hash_profile",
                hash_site = snapshot.site,
                hash_calls_total = snapshot.calls,
                hash_bytes_total = snapshot.bytes,
                hash_nanoseconds_total = snapshot.nanoseconds,
                "keldra_hash_profile"
            );
        }
    }

    pub(crate) fn record_catalog_checkpoint(&self, positions: u64, payload_bytes: u64) {
        Self::add(&self.catalog_checkpointed_source_positions, positions);
        Self::add(
            &self.catalog_checkpointed_source_payload_bytes,
            payload_bytes,
        );
    }

    /// An empty cursor-advance batch retains control metadata but prepares no
    /// object row. Keep those bytes out of indexed-object throughput.
    pub(crate) const fn indexed_prepared_bytes(source_rows: u64, resident_bytes: u64) -> u64 {
        if source_rows == 0 { 0 } else { resident_bytes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_checkpoint_does_not_claim_projection_progress() {
        let telemetry = V1PipelineTelemetry::default();
        telemetry.record_catalog_checkpoint(7, 4096);

        assert_eq!(
            V1PipelineTelemetry::load(&telemetry.checkpointed_source_positions),
            0
        );
        assert_eq!(
            V1PipelineTelemetry::load(&telemetry.checkpointed_source_payload_bytes),
            0
        );
        assert_eq!(
            V1PipelineTelemetry::load(&telemetry.catalog_checkpointed_source_positions),
            7
        );
        assert_eq!(
            V1PipelineTelemetry::load(&telemetry.catalog_checkpointed_source_payload_bytes),
            4096
        );
    }

    #[test]
    fn empty_control_batch_has_no_indexed_prepared_bytes() {
        assert_eq!(V1PipelineTelemetry::indexed_prepared_bytes(0, 6096), 0);
        assert_eq!(V1PipelineTelemetry::indexed_prepared_bytes(1, 6096), 6096);
    }

    #[test]
    fn in_flight_preparation_reports_live_age_and_is_removed_on_drop() {
        let telemetry = Arc::new(V1PipelineTelemetry::default());
        let now = Instant::now();
        let guard = telemetry.begin_in_flight_preparation_at(
            now.checked_sub(Duration::from_secs(31)).unwrap(),
            Duration::from_secs(30),
        );

        let (preparation, oldest_no_progress, stalled) = telemetry.no_progress_summary(now);
        assert_eq!(preparation.count, 1);
        assert_eq!(preparation.oldest_age_milliseconds, 31_000);
        assert_eq!(preparation.stalled, 1);
        assert_eq!(oldest_no_progress, 31_000);
        assert_eq!(stalled, 1);

        drop(guard);
        assert_eq!(
            telemetry.in_flight_preparation_snapshot(now),
            InFlightPreparationSnapshot::default()
        );
    }

    #[test]
    fn live_preparation_is_combined_conservatively_with_reconciled_gauges() {
        let telemetry = Arc::new(V1PipelineTelemetry::default());
        V1PipelineTelemetry::set(&telemetry.oldest_no_progress_age_millis, 45_000);
        V1PipelineTelemetry::set(&telemetry.stalled_partitions, 3);
        let now = Instant::now();
        let _guard = telemetry.begin_in_flight_preparation_at(
            now.checked_sub(Duration::from_secs(31)).unwrap(),
            Duration::from_secs(30),
        );

        let (_, oldest_no_progress, stalled) = telemetry.no_progress_summary(now);
        assert_eq!(oldest_no_progress, 45_000);
        assert_eq!(stalled, 3);
    }
}
