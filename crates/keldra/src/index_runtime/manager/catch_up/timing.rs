//! Phase attribution for bounded catch-up work.

use std::time::{Duration, Instant};

use crate::index_runtime::catalog::CatalogDefinition;

const SLOW_PHASE: Duration = Duration::from_secs(5);

pub(super) fn complete(
    definition: &CatalogDefinition,
    name: &'static str,
    records: usize,
    started: Instant,
) {
    let elapsed = started.elapsed();
    tracing::debug!(
        index.id = definition.physical_index_id(),
        index.phase = name,
        index.records = records as u64,
        histogram.keldra_index_catch_up_phase_duration_seconds = elapsed.as_secs_f64(),
        "index catch-up phase completed"
    );
    if elapsed >= SLOW_PHASE {
        tracing::info!(
            index.id = definition.physical_index_id(),
            index.phase = name,
            index.records = records as u64,
            index.phase_duration_seconds = elapsed.as_secs_f64(),
            "slow index catch-up phase completed"
        );
    }
}
