//! Buffer lifecycle telemetry kept out of the builder control flow.

use std::time::Duration;

use keldra_index::IndexKind;

pub(super) fn emit_active_buffer(
    index_id: u64,
    kind: IndexKind,
    bytes: u64,
    mutations: u64,
    wall_age: Duration,
    runnable_age: Duration,
) {
    tracing::debug!(
        index.id = index_id,
        index.kind = ?kind,
        gauge.keldra_index_active_buffer_count = u64::from(mutations != 0),
        gauge.keldra_index_active_buffer_bytes = bytes,
        gauge.keldra_index_active_buffer_mutations = mutations,
        gauge.keldra_index_active_buffer_wall_age_seconds = wall_age.as_secs_f64(),
        gauge.keldra_index_active_buffer_runnable_age_seconds = runnable_age.as_secs_f64(),
        "index active mutation buffer state"
    );
}

pub(super) fn emit_frozen_buffer(
    index_id: u64,
    kind: IndexKind,
    count: u64,
    bytes: u64,
    reason: &'static str,
    wall_age: Duration,
    runnable_age: Duration,
) {
    tracing::debug!(
        index.id = index_id,
        index.kind = ?kind,
        flush.reason = reason,
        flush.wall_age_seconds = wall_age.as_secs_f64(),
        flush.runnable_age_seconds = runnable_age.as_secs_f64(),
        gauge.keldra_index_frozen_buffer_count = count,
        gauge.keldra_index_frozen_buffer_bytes = bytes,
        "index frozen mutation buffer state"
    );
}
