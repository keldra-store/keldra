//! Low-cardinality progress telemetry for long-running index work.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use keldra_index::compaction::{
    CompactionParallelism, CompactionProgress, CompactionProgressSnapshot,
};
use keldra_index::{IndexError, IndexKind};
use tracing::Instrument;

pub(crate) const PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CompactionDebtSnapshot {
    tiers: u64,
    segments: u64,
    bytes: u64,
}

fn compaction_debt(
    segments: &[keldra_index::v4::SegmentDescriptor],
    maximum_segments_per_tier: usize,
    maximum_unmerged_bytes_per_tier: u64,
) -> (CompactionDebtSnapshot, BTreeMap<u8, CompactionTierDebt>) {
    compaction_debt_summaries(
        segments.iter().map(|segment| {
            (
                segment.encoded_bytes.max(1).ilog2() as u8,
                segment.encoded_bytes,
            )
        }),
        maximum_segments_per_tier,
        maximum_unmerged_bytes_per_tier,
    )
}

fn compaction_debt_summaries(
    segments: impl IntoIterator<Item = (u8, u64)>,
    maximum_segments_per_tier: usize,
    maximum_unmerged_bytes_per_tier: u64,
) -> (CompactionDebtSnapshot, BTreeMap<u8, CompactionTierDebt>) {
    let mut tiers = BTreeMap::<u8, CompactionTierDebt>::new();
    for (tier, encoded_bytes) in segments {
        let tier = tiers.entry(tier).or_default();
        tier.segments = tier.segments.saturating_add(1);
        tier.bytes = tier.bytes.saturating_add(encoded_bytes);
    }
    let debt = tiers
        .values()
        .filter(|tier| {
            tier.segments > maximum_segments_per_tier as u64
                || tier.bytes > maximum_unmerged_bytes_per_tier
        })
        .fold(CompactionDebtSnapshot::default(), |mut debt, tier| {
            debt.tiers = debt.tiers.saturating_add(1);
            debt.segments = debt.segments.saturating_add(tier.segments);
            debt.bytes = debt.bytes.saturating_add(tier.bytes);
            debt
        });
    (debt, tiers)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CompactionTierDebt {
    segments: u64,
    bytes: u64,
}

pub(crate) fn emit_compaction_debt(
    kind: IndexKind,
    segments: &[keldra_index::v4::SegmentDescriptor],
    maximum_segments_per_tier: usize,
    maximum_unmerged_bytes_per_tier: u64,
    trigger: &'static str,
) {
    let (debt, tiers) = compaction_debt(
        segments,
        maximum_segments_per_tier,
        maximum_unmerged_bytes_per_tier,
    );
    tracing::debug!(
        index.kind = ?kind,
        compaction.trigger = trigger,
        gauge.keldra_index_compaction_debt_tiers = debt.tiers,
        gauge.keldra_index_compaction_debt_segments = debt.segments,
        gauge.keldra_index_compaction_debt_bytes = debt.bytes,
        gauge.keldra_index_compaction_debt_segment_limit = maximum_segments_per_tier as u64,
        gauge.keldra_index_compaction_debt_byte_limit = maximum_unmerged_bytes_per_tier,
        "index compaction debt observed"
    );
    for (tier, current) in tiers {
        let over_limit = current.segments > maximum_segments_per_tier as u64
            || current.bytes > maximum_unmerged_bytes_per_tier;
        tracing::debug!(
            index.kind = ?kind,
            index.tier = tier,
            compaction.trigger = trigger,
            gauge.keldra_index_compaction_tier_debt_segments =
                if over_limit { current.segments } else { 0 },
            gauge.keldra_index_compaction_tier_debt_bytes =
                if over_limit { current.bytes } else { 0 },
            "index compaction tier debt observed"
        );
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BuilderProgressPhase {
    Rebuild,
    CatchUp,
}

impl BuilderProgressPhase {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Rebuild => "rebuild",
            Self::CatchUp => "catch_up",
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct IndexTelemetryIdentity {
    pub(crate) index_id: u64,
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) kind: IndexKind,
}

#[derive(Clone)]
pub(crate) struct BuilderProgress {
    identity: IndexTelemetryIdentity,
    phase: BuilderProgressPhase,
    inner: Arc<Mutex<BuilderProgressState>>,
}

struct BuilderProgressState {
    started: Instant,
    last_progress: Instant,
    last_emit: Instant,
    records: u64,
    bytes: u64,
    units: u64,
    emitted_records: u64,
    emitted_bytes: u64,
    emitted_units: u64,
    span: Option<tracing::Span>,
    finished: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct BuilderProgressSnapshot {
    pub(crate) records: u64,
    pub(crate) bytes: u64,
    pub(crate) units: u64,
    pub(crate) elapsed_seconds: f64,
    pub(crate) last_progress_age_seconds: f64,
}

#[derive(Clone, Copy)]
struct BuilderProgressEmission {
    snapshot: BuilderProgressSnapshot,
    records: u64,
    bytes: u64,
    units: u64,
    interval_seconds: f64,
}

impl BuilderProgress {
    pub(crate) fn start(identity: IndexTelemetryIdentity, phase: BuilderProgressPhase) -> Self {
        let now = Instant::now();
        let span = tracing::info_span!(
            "keldra.index.builder",
            index.id = identity.index_id,
            tenant.id = identity.tenant_id,
            bucket.id = identity.bucket_id,
            index.kind = ?identity.kind,
            builder.phase = phase.as_str(),
            progress.records = tracing::field::Empty,
            progress.bytes = tracing::field::Empty,
            progress.units = tracing::field::Empty,
            progress.elapsed_seconds = tracing::field::Empty,
            progress.last_progress_age_seconds = tracing::field::Empty,
            builder.outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let value = Self {
            identity,
            phase,
            inner: Arc::new(Mutex::new(BuilderProgressState {
                started: now,
                last_progress: now,
                last_emit: now,
                records: 0,
                bytes: 0,
                units: 0,
                emitted_records: 0,
                emitted_bytes: 0,
                emitted_units: 0,
                span: Some(span.clone()),
                finished: false,
            })),
        };
        span.in_scope(|| {
            match phase {
                BuilderProgressPhase::Rebuild => tracing::info!(
                    index.kind = ?identity.kind,
                    counter.keldra_index_rebuild_active = 1_i64,
                    monotonic_counter.keldra_index_rebuilds_total = 1_u64,
                    gauge.keldra_index_rebuild_elapsed_seconds = 0_f64,
                    gauge.keldra_index_rebuild_last_progress_age_seconds = 0_f64,
                    "index rebuild admitted"
                ),
                BuilderProgressPhase::CatchUp => tracing::info!(
                    index.kind = ?identity.kind,
                    counter.keldra_index_catch_up_active = 1_i64,
                    monotonic_counter.keldra_index_catch_up_turns_total = 1_u64,
                    gauge.keldra_index_catch_up_elapsed_seconds = 0_f64,
                    gauge.keldra_index_catch_up_last_progress_age_seconds = 0_f64,
                    "index catch-up admitted"
                ),
            }
            tracing::info!("index builder phase started");
        });
        value
    }

    pub(crate) fn advance(&self, records: u64, bytes: u64) {
        let now = Instant::now();
        {
            let mut state = self.lock();
            if state.finished {
                return;
            }
            state.records = state.records.saturating_add(records);
            state.bytes = state.bytes.saturating_add(bytes);
            state.units = state.units.saturating_add(1);
            state.last_progress = now;
        }
        self.emit_if_due(now, false);
    }

    pub(crate) fn snapshot(&self) -> BuilderProgressSnapshot {
        self.snapshot_at(Instant::now())
    }

    fn snapshot_at(&self, now: Instant) -> BuilderProgressSnapshot {
        let state = self.lock();
        snapshot(&state, now)
    }

    pub(crate) fn heartbeat(&self) {
        self.emit_if_due(Instant::now(), true);
    }

    pub(crate) fn until_heartbeat(&self) -> Duration {
        let state = self.lock();
        PROGRESS_HEARTBEAT_INTERVAL
            .saturating_sub(Instant::now().saturating_duration_since(state.last_emit))
    }

    pub(crate) fn complete(&self) {
        self.finish(false);
    }

    fn emit_if_due(&self, now: Instant, heartbeat: bool) {
        let emission = {
            let mut state = self.lock();
            if state.finished
                || now.saturating_duration_since(state.last_emit) < PROGRESS_HEARTBEAT_INTERVAL
            {
                return;
            }
            take_emission(&mut state, now)
        };
        self.record_span(emission.snapshot);
        self.emit_progress(emission, heartbeat);
    }

    fn finish(&self, failed: bool) {
        let (emission, span) = {
            let mut state = self.lock();
            if state.finished {
                return;
            }
            state.finished = true;
            let emission = take_emission(&mut state, Instant::now());
            (emission, state.span.take())
        };
        self.record_span_with(emission.snapshot, span.as_ref());
        if let Some(span) = span {
            span.record(
                "builder.outcome",
                if failed { "failed" } else { "completed" },
            );
            span.record("otel.status_code", if failed { "error" } else { "ok" });
            span.in_scope(|| {
                self.emit_terminal(emission, failed);
                tracing::info!(
                    records = emission.snapshot.records,
                    bytes = emission.snapshot.bytes,
                    work.units = emission.snapshot.units,
                    elapsed.seconds = emission.snapshot.elapsed_seconds,
                    failed,
                    "index builder phase finished"
                );
            });
        } else {
            self.emit_terminal(emission, failed);
        }
    }

    fn record_span(&self, snapshot: BuilderProgressSnapshot) {
        let state = self.lock();
        self.record_span_with(snapshot, state.span.as_ref());
    }

    fn record_span_with(&self, snapshot: BuilderProgressSnapshot, span: Option<&tracing::Span>) {
        let Some(span) = span else {
            return;
        };
        span.record("progress.records", snapshot.records);
        span.record("progress.bytes", snapshot.bytes);
        span.record("progress.units", snapshot.units);
        span.record("progress.elapsed_seconds", snapshot.elapsed_seconds);
        span.record(
            "progress.last_progress_age_seconds",
            snapshot.last_progress_age_seconds,
        );
    }

    fn emit_progress(&self, emission: BuilderProgressEmission, heartbeat: bool) {
        let record_rate = emission.records as f64 / emission.interval_seconds.max(0.001);
        let byte_rate = emission.bytes as f64 / emission.interval_seconds.max(0.001);
        let emit = || match self.phase {
            BuilderProgressPhase::Rebuild => tracing::debug!(
                index.kind = ?self.identity.kind,
                monotonic_counter.keldra_index_rebuild_records_total = emission.records,
                monotonic_counter.keldra_index_rebuild_bytes_total = emission.bytes,
                monotonic_counter.keldra_index_rebuild_frames_total = emission.units,
                monotonic_counter.keldra_index_rebuild_progress_heartbeats_total =
                    u64::from(heartbeat),
                gauge.keldra_index_rebuild_elapsed_seconds =
                    emission.snapshot.elapsed_seconds,
                gauge.keldra_index_rebuild_last_progress_age_seconds =
                    emission.snapshot.last_progress_age_seconds,
                gauge.keldra_index_rebuild_records_per_second = record_rate,
                gauge.keldra_index_rebuild_bytes_per_second = byte_rate,
                "index rebuild progress"
            ),
            BuilderProgressPhase::CatchUp => tracing::debug!(
                index.kind = ?self.identity.kind,
                monotonic_counter.keldra_index_catch_up_records_total = emission.records,
                monotonic_counter.keldra_index_catch_up_bytes_total = emission.bytes,
                monotonic_counter.keldra_index_catch_up_pages_total = emission.units,
                monotonic_counter.keldra_index_catch_up_progress_heartbeats_total =
                    u64::from(heartbeat),
                gauge.keldra_index_catch_up_elapsed_seconds =
                    emission.snapshot.elapsed_seconds,
                gauge.keldra_index_catch_up_last_progress_age_seconds =
                    emission.snapshot.last_progress_age_seconds,
                gauge.keldra_index_catch_up_records_per_second = record_rate,
                gauge.keldra_index_catch_up_bytes_per_second = byte_rate,
                "index catch-up progress"
            ),
        };
        let state = self.lock();
        if let Some(span) = state.span.as_ref() {
            span.in_scope(emit);
        } else {
            emit();
        }
    }

    fn emit_terminal(&self, emission: BuilderProgressEmission, failed: bool) {
        match self.phase {
            BuilderProgressPhase::Rebuild => tracing::info!(
                index.kind = ?self.identity.kind,
                counter.keldra_index_rebuild_active = -1_i64,
                monotonic_counter.keldra_index_rebuild_records_total = emission.records,
                monotonic_counter.keldra_index_rebuild_bytes_total = emission.bytes,
                monotonic_counter.keldra_index_rebuild_frames_total = emission.units,
                monotonic_counter.keldra_index_rebuild_failures_total = u64::from(failed),
                gauge.keldra_index_rebuild_elapsed_seconds = emission.snapshot.elapsed_seconds,
                gauge.keldra_index_rebuild_last_progress_age_seconds =
                    emission.snapshot.last_progress_age_seconds,
                histogram.keldra_index_rebuild_records = emission.snapshot.records,
                histogram.keldra_index_rebuild_bytes = emission.snapshot.bytes,
                histogram.keldra_index_rebuild_frames = emission.snapshot.units,
                histogram.keldra_index_rebuild_duration_seconds =
                    emission.snapshot.elapsed_seconds,
                "index rebuild finished"
            ),
            BuilderProgressPhase::CatchUp => tracing::info!(
                index.kind = ?self.identity.kind,
                counter.keldra_index_catch_up_active = -1_i64,
                monotonic_counter.keldra_index_catch_up_records_total = emission.records,
                monotonic_counter.keldra_index_catch_up_bytes_total = emission.bytes,
                monotonic_counter.keldra_index_catch_up_pages_total = emission.units,
                monotonic_counter.keldra_index_catch_up_failures_total = u64::from(failed),
                gauge.keldra_index_catch_up_elapsed_seconds = emission.snapshot.elapsed_seconds,
                gauge.keldra_index_catch_up_last_progress_age_seconds =
                    emission.snapshot.last_progress_age_seconds,
                histogram.keldra_index_catch_up_records = emission.snapshot.records,
                histogram.keldra_index_catch_up_bytes = emission.snapshot.bytes,
                histogram.keldra_index_catch_up_pages = emission.snapshot.units,
                histogram.keldra_index_catch_up_duration_seconds =
                    emission.snapshot.elapsed_seconds,
                "index catch-up finished"
            ),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BuilderProgressState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn span(&self) -> tracing::Span {
        self.lock().span.clone().unwrap_or_else(tracing::Span::none)
    }
}

impl Drop for BuilderProgress {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 && !self.lock().finished {
            self.finish(true);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CompactionInputTotals {
    pub(crate) segments: u64,
    pub(crate) documents: u64,
    pub(crate) bytes: u64,
}

impl CompactionInputTotals {
    pub(crate) fn from_segments(
        segments: &[keldra_index::v4::SegmentDescriptor],
    ) -> Result<Self, IndexError> {
        let mut documents = 0_u64;
        let mut bytes = 0_u64;
        for segment in segments {
            documents = documents
                .checked_add(u64::from(segment.document_count))
                .ok_or(IndexError::OffsetOverflow)?;
            bytes = bytes
                .checked_add(segment.encoded_bytes)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        Ok(Self {
            segments: u64::try_from(segments.len()).map_err(|_| IndexError::OffsetOverflow)?,
            documents,
            bytes,
        })
    }
}

pub(crate) struct CompactionTelemetry {
    identity: IndexTelemetryIdentity,
    input: CompactionInputTotals,
    parallelism: CompactionParallelism,
    progress: CompactionProgress,
    inner: Mutex<CompactionTelemetryState>,
}

struct CompactionTelemetryState {
    started: Instant,
    last_progress: Instant,
    last_emit: Instant,
    last_snapshot: CompactionProgressSnapshot,
    emitted_snapshot: CompactionProgressSnapshot,
    span: Option<tracing::Span>,
    finished: bool,
}

#[derive(Clone, Copy)]
struct CompactionEmission {
    snapshot: CompactionProgressSnapshot,
    delta: CompactionProgressSnapshot,
    elapsed_seconds: f64,
    last_progress_age_seconds: f64,
    interval_seconds: f64,
}

impl CompactionTelemetry {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        identity: IndexTelemetryIdentity,
        input_tier: u8,
        output_tier: u8,
        input: CompactionInputTotals,
        parallelism: CompactionParallelism,
        leased_bytes: u64,
        progress: CompactionProgress,
    ) -> Result<Self, IndexError> {
        let now = Instant::now();
        let admitted_bytes = parallelism.admitted_bytes()? as u64;
        let span = tracing::info_span!(
            "keldra.index.compaction",
            index.id = identity.index_id,
            tenant.id = identity.tenant_id,
            bucket.id = identity.bucket_id,
            index.kind = ?identity.kind,
            compaction.input_tier = input_tier,
            compaction.output_tier = output_tier,
            compaction.input_segments = input.segments,
            compaction.input_documents = input.documents,
            compaction.input_bytes = input.bytes,
            compaction.configured_lanes = parallelism.configured_lanes(),
            compaction.effective_lanes = tracing::field::Empty,
            compaction.lane_limit_reason = tracing::field::Empty,
            compaction.range_limit = tracing::field::Empty,
            compaction.ranges_total = tracing::field::Empty,
            compaction.ranges_completed = tracing::field::Empty,
            compaction.peak_active_lanes = tracing::field::Empty,
            compaction.input_component_rows = tracing::field::Empty,
            compaction.actual_input_bytes = tracing::field::Empty,
            compaction.input_blocks = tracing::field::Empty,
            compaction.output_component_rows = tracing::field::Empty,
            compaction.output_bytes = tracing::field::Empty,
            compaction.output_blocks = tracing::field::Empty,
            compaction.sort_chunks = tracing::field::Empty,
            compaction.sort_merge_passes = tracing::field::Empty,
            compaction.sort_peak_workspace_bytes = tracing::field::Empty,
            compaction.elapsed_seconds = tracing::field::Empty,
            compaction.last_progress_age_seconds = tracing::field::Empty,
            compaction.outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let value = Self {
            identity,
            input,
            parallelism,
            progress,
            inner: Mutex::new(CompactionTelemetryState {
                started: now,
                last_progress: now,
                last_emit: now,
                last_snapshot: CompactionProgressSnapshot::default(),
                emitted_snapshot: CompactionProgressSnapshot::default(),
                span: Some(span.clone()),
                finished: false,
            }),
        };
        span.in_scope(|| {
            tracing::info!(
                index.kind = ?identity.kind,
                counter.keldra_index_compaction_active = 1_i64,
                monotonic_counter.keldra_index_compaction_attempts_total = 1_u64,
                gauge.keldra_index_compaction_configured_lanes =
                    parallelism.configured_lanes() as u64,
                gauge.keldra_index_compaction_worker_limit = parallelism.worker_limit() as u64,
                gauge.keldra_index_compaction_budget_limit = parallelism.budget_limit() as u64,
                gauge.keldra_index_compaction_active_lanes = 0_u64,
                gauge.keldra_index_compaction_waiting_lanes = 0_u64,
                gauge.keldra_index_compaction_shared_workspace_bytes =
                    parallelism.shared_workspace_bytes() as u64,
                gauge.keldra_index_compaction_incremental_lane_workspace_bytes =
                    parallelism.incremental_lane_workspace_bytes() as u64,
                gauge.keldra_index_compaction_admitted_workspace_bytes = admitted_bytes,
                gauge.keldra_index_compaction_leased_bytes = leased_bytes,
                gauge.keldra_index_compaction_input_tier = u64::from(input_tier),
                gauge.keldra_index_compaction_output_tier = u64::from(output_tier),
                gauge.keldra_index_compaction_selected_input_segments = input.segments,
                gauge.keldra_index_compaction_selected_input_documents = input.documents,
                gauge.keldra_index_compaction_selected_input_bytes = input.bytes,
                gauge.keldra_index_compaction_elapsed_seconds = 0_f64,
                gauge.keldra_index_compaction_last_progress_age_seconds = 0_f64,
                "index compaction admitted"
            );
            tracing::info!("index compaction started");
        });
        Ok(value)
    }

    pub(crate) fn until_heartbeat(&self) -> Duration {
        let state = self.lock();
        PROGRESS_HEARTBEAT_INTERVAL
            .saturating_sub(Instant::now().saturating_duration_since(state.last_emit))
    }

    pub(crate) fn heartbeat(&self) {
        let now = Instant::now();
        let emission = {
            let mut state = self.lock();
            if state.finished
                || now.saturating_duration_since(state.last_emit) < PROGRESS_HEARTBEAT_INTERVAL
            {
                return;
            }
            take_compaction_emission(&mut state, self.progress.snapshot(), now)
        };
        self.record_span(emission, None);
        self.emit_progress(emission, true);
    }

    pub(crate) fn complete(&self) {
        self.finish(false);
    }

    pub(crate) fn failed(&self) {
        self.finish(true);
    }

    fn finish(&self, failed: bool) {
        let now = Instant::now();
        let (emission, span) = {
            let mut state = self.lock();
            if state.finished {
                return;
            }
            state.finished = true;
            let emission = take_compaction_emission(&mut state, self.progress.snapshot(), now);
            (emission, state.span.take())
        };
        self.record_span(emission, span.as_ref());
        if let Some(span) = span {
            span.record(
                "compaction.outcome",
                if failed { "failed" } else { "completed" },
            );
            span.record("otel.status_code", if failed { "error" } else { "ok" });
            span.in_scope(|| {
                self.emit_terminal(emission, failed);
                tracing::info!(
                    ranges.total = emission.snapshot.ranges_total,
                    ranges.completed = emission.snapshot.ranges_completed,
                    input.component_rows = emission.snapshot.input_records,
                    input.bytes = emission.snapshot.input_bytes,
                    input.blocks = emission.snapshot.input_blocks,
                    output.component_rows = emission.snapshot.output_records,
                    output.bytes = emission.snapshot.output_bytes,
                    output.blocks = emission.snapshot.output_blocks,
                    elapsed.seconds = emission.elapsed_seconds,
                    failed,
                    "format-v4 index segment compaction finished"
                );
                if !failed {
                    tracing::info!("format-v4 index segments compacted");
                }
            });
        } else {
            self.emit_terminal(emission, failed);
        }
    }

    fn record_span(&self, emission: CompactionEmission, span: Option<&tracing::Span>) {
        let lanes = effective_lanes(
            self.parallelism,
            emission.snapshot.range_limit,
            emission.snapshot.effective_lanes,
        );
        let state;
        let span = match span {
            Some(span) => span,
            None => {
                state = self.lock();
                let Some(span) = state.span.as_ref() else {
                    return;
                };
                span
            }
        };
        span.record("compaction.effective_lanes", lanes.count);
        span.record("compaction.lane_limit_reason", lanes.reason);
        span.record("compaction.range_limit", emission.snapshot.range_limit);
        span.record("compaction.ranges_total", emission.snapshot.ranges_total);
        span.record(
            "compaction.ranges_completed",
            emission.snapshot.ranges_completed,
        );
        span.record(
            "compaction.peak_active_lanes",
            emission.snapshot.peak_active_lanes,
        );
        span.record(
            "compaction.input_component_rows",
            emission.snapshot.input_records,
        );
        span.record(
            "compaction.actual_input_bytes",
            emission.snapshot.input_bytes,
        );
        span.record("compaction.input_blocks", emission.snapshot.input_blocks);
        span.record(
            "compaction.output_component_rows",
            emission.snapshot.output_records,
        );
        span.record("compaction.output_bytes", emission.snapshot.output_bytes);
        span.record("compaction.output_blocks", emission.snapshot.output_blocks);
        span.record("compaction.sort_chunks", emission.snapshot.sort_chunks);
        span.record(
            "compaction.sort_merge_passes",
            emission.snapshot.sort_merge_passes,
        );
        span.record(
            "compaction.sort_peak_workspace_bytes",
            emission.snapshot.sort_peak_workspace_bytes,
        );
        span.record("compaction.elapsed_seconds", emission.elapsed_seconds);
        span.record(
            "compaction.last_progress_age_seconds",
            emission.last_progress_age_seconds,
        );
    }

    fn emit_progress(&self, emission: CompactionEmission, heartbeat: bool) {
        let lanes = effective_lanes(
            self.parallelism,
            emission.snapshot.range_limit,
            emission.snapshot.effective_lanes,
        );
        let output_rate = emission.delta.output_bytes as f64 / emission.interval_seconds.max(0.001);
        let input_rate = emission.delta.input_bytes as f64 / emission.interval_seconds.max(0.001);
        let emit = || {
            tracing::debug!(
                index.kind = ?self.identity.kind,
                monotonic_counter.keldra_index_compaction_ranges_completed_total =
                    emission.delta.ranges_completed,
                monotonic_counter.keldra_index_compaction_input_component_rows_total =
                    emission.delta.input_records,
                monotonic_counter.keldra_index_compaction_input_read_bytes_total =
                    emission.delta.input_bytes,
                monotonic_counter.keldra_index_compaction_input_blocks_total =
                    emission.delta.input_blocks,
                monotonic_counter.keldra_index_compaction_output_component_rows_total =
                    emission.delta.output_records,
                monotonic_counter.keldra_index_compaction_output_bytes_total =
                    emission.delta.output_bytes,
                monotonic_counter.keldra_index_compaction_output_blocks_total =
                    emission.delta.output_blocks,
                monotonic_counter.keldra_index_compaction_sort_chunks_total =
                    emission.delta.sort_chunks,
                monotonic_counter.keldra_index_compaction_sort_merge_passes_total =
                    emission.delta.sort_merge_passes,
                monotonic_counter.keldra_index_compaction_progress_heartbeats_total =
                    u64::from(heartbeat),
                gauge.keldra_index_compaction_range_limit = emission.snapshot.range_limit,
                gauge.keldra_index_compaction_ranges_total = emission.snapshot.ranges_total,
                gauge.keldra_index_compaction_ranges_completed = emission.snapshot.ranges_completed,
                gauge.keldra_index_compaction_active_lanes = emission.snapshot.active_lanes,
                gauge.keldra_index_compaction_peak_active_lanes =
                    emission.snapshot.peak_active_lanes,
                gauge.keldra_index_compaction_waiting_lanes = emission.snapshot.waiting_lanes,
                gauge.keldra_index_compaction_current_input_component_rows =
                    emission.snapshot.input_records,
                gauge.keldra_index_compaction_current_input_read_bytes = emission.snapshot.input_bytes,
                gauge.keldra_index_compaction_current_input_blocks = emission.snapshot.input_blocks,
                gauge.keldra_index_compaction_current_output_component_rows =
                    emission.snapshot.output_records,
                gauge.keldra_index_compaction_current_output_bytes = emission.snapshot.output_bytes,
                gauge.keldra_index_compaction_current_output_blocks = emission.snapshot.output_blocks,
                gauge.keldra_index_compaction_sort_peak_workspace_bytes =
                    emission.snapshot.sort_peak_workspace_bytes,
                gauge.keldra_index_compaction_input_bytes_per_second = input_rate,
                gauge.keldra_index_compaction_output_bytes_per_second = output_rate,
                gauge.keldra_index_compaction_elapsed_seconds = emission.elapsed_seconds,
                gauge.keldra_index_compaction_last_progress_age_seconds =
                    emission.last_progress_age_seconds,
                "index compaction progress"
            );
            // Keep the reason label off unrelated instruments. A tracing event
            // applies all of its attributes to every metric it contains.
            tracing::debug!(
                index.kind = ?self.identity.kind,
                compaction.lane_limit_reason = lanes.reason,
                gauge.keldra_index_compaction_effective_lanes = lanes.count,
                "index compaction effective lanes"
            );
        };
        let state = self.lock();
        if let Some(span) = state.span.as_ref() {
            span.in_scope(emit);
        } else {
            emit();
        }
    }

    fn emit_terminal(&self, emission: CompactionEmission, failed: bool) {
        let lanes = effective_lanes(
            self.parallelism,
            emission.snapshot.range_limit,
            emission.snapshot.effective_lanes,
        );
        let output_rate = emission.delta.output_bytes as f64 / emission.interval_seconds.max(0.001);
        let input_rate = emission.delta.input_bytes as f64 / emission.interval_seconds.max(0.001);
        // Active has exactly the same attribute set on admission and release,
        // so its signed measurements aggregate into one time series.
        tracing::info!(
            index.kind = ?self.identity.kind,
            counter.keldra_index_compaction_active = -1_i64,
            "index compaction released"
        );
        tracing::info!(
            index.kind = ?self.identity.kind,
            monotonic_counter.keldra_index_compaction_ranges_completed_total =
                emission.delta.ranges_completed,
            monotonic_counter.keldra_index_compaction_input_component_rows_total =
                emission.delta.input_records,
            monotonic_counter.keldra_index_compaction_input_read_bytes_total =
                emission.delta.input_bytes,
            monotonic_counter.keldra_index_compaction_input_blocks_total =
                emission.delta.input_blocks,
            monotonic_counter.keldra_index_compaction_output_component_rows_total =
                emission.delta.output_records,
            monotonic_counter.keldra_index_compaction_output_bytes_total =
                emission.delta.output_bytes,
            monotonic_counter.keldra_index_compaction_output_blocks_total =
                emission.delta.output_blocks,
            monotonic_counter.keldra_index_compaction_sort_chunks_total =
                emission.delta.sort_chunks,
            monotonic_counter.keldra_index_compaction_sort_merge_passes_total =
                emission.delta.sort_merge_passes,
            monotonic_counter.keldra_index_compaction_failures_total = u64::from(failed),
            gauge.keldra_index_compaction_configured_lanes =
                self.parallelism.configured_lanes() as u64,
            gauge.keldra_index_compaction_worker_limit = self.parallelism.worker_limit() as u64,
            gauge.keldra_index_compaction_budget_limit = self.parallelism.budget_limit() as u64,
            gauge.keldra_index_compaction_range_limit = emission.snapshot.range_limit,
            gauge.keldra_index_compaction_active_lanes = 0_u64,
            gauge.keldra_index_compaction_peak_active_lanes =
                emission.snapshot.peak_active_lanes,
            gauge.keldra_index_compaction_waiting_lanes = 0_u64,
            gauge.keldra_index_compaction_ranges_total = emission.snapshot.ranges_total,
            gauge.keldra_index_compaction_ranges_completed = emission.snapshot.ranges_completed,
            gauge.keldra_index_compaction_current_input_component_rows =
                emission.snapshot.input_records,
            gauge.keldra_index_compaction_current_input_read_bytes = emission.snapshot.input_bytes,
            gauge.keldra_index_compaction_current_input_blocks = emission.snapshot.input_blocks,
            gauge.keldra_index_compaction_current_output_component_rows =
                emission.snapshot.output_records,
            gauge.keldra_index_compaction_current_output_bytes = emission.snapshot.output_bytes,
            gauge.keldra_index_compaction_current_output_blocks = emission.snapshot.output_blocks,
            gauge.keldra_index_compaction_sort_peak_workspace_bytes =
                emission.snapshot.sort_peak_workspace_bytes,
            gauge.keldra_index_compaction_input_bytes_per_second = input_rate,
            gauge.keldra_index_compaction_output_bytes_per_second = output_rate,
            gauge.keldra_index_compaction_elapsed_seconds = emission.elapsed_seconds,
            gauge.keldra_index_compaction_last_progress_age_seconds =
                emission.last_progress_age_seconds,
            histogram.keldra_index_compaction_input_segments = self.input.segments,
            histogram.keldra_index_compaction_input_documents = self.input.documents,
            histogram.keldra_index_compaction_input_bytes = self.input.bytes,
            histogram.keldra_index_compaction_output_component_rows = emission.snapshot.output_records,
            histogram.keldra_index_compaction_output_bytes = emission.snapshot.output_bytes,
            histogram.keldra_index_compaction_output_blocks = emission.snapshot.output_blocks,
            histogram.keldra_index_compaction_merge_ranges = emission.snapshot.ranges_total,
            histogram.keldra_index_compaction_duration_seconds = emission.elapsed_seconds,
            "index compaction terminal metrics"
        );
        tracing::info!(
            index.kind = ?self.identity.kind,
            compaction.lane_limit_reason = lanes.reason,
            gauge.keldra_index_compaction_effective_lanes = lanes.count,
            "index compaction effective lanes"
        );
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CompactionTelemetryState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn span(&self) -> tracing::Span {
        self.lock().span.clone().unwrap_or_else(tracing::Span::none)
    }
}

impl Drop for CompactionTelemetry {
    fn drop(&mut self) {
        let finished = self.lock().finished;
        if !finished {
            self.finish(true);
        }
    }
}

fn take_compaction_emission(
    state: &mut CompactionTelemetryState,
    current: CompactionProgressSnapshot,
    now: Instant,
) -> CompactionEmission {
    if current != state.last_snapshot {
        state.last_progress = now;
        state.last_snapshot = current;
    }
    let emission = CompactionEmission {
        snapshot: current,
        delta: subtract_compaction_progress(current, state.emitted_snapshot),
        elapsed_seconds: now.saturating_duration_since(state.started).as_secs_f64(),
        last_progress_age_seconds: now
            .saturating_duration_since(state.last_progress)
            .as_secs_f64(),
        interval_seconds: now.saturating_duration_since(state.last_emit).as_secs_f64(),
    };
    state.emitted_snapshot = current;
    state.last_emit = now;
    emission
}

fn subtract_compaction_progress(
    current: CompactionProgressSnapshot,
    previous: CompactionProgressSnapshot,
) -> CompactionProgressSnapshot {
    CompactionProgressSnapshot {
        ranges_total: current.ranges_total.saturating_sub(previous.ranges_total),
        ranges_completed: current
            .ranges_completed
            .saturating_sub(previous.ranges_completed),
        input_records: current.input_records.saturating_sub(previous.input_records),
        input_bytes: current.input_bytes.saturating_sub(previous.input_bytes),
        input_blocks: current.input_blocks.saturating_sub(previous.input_blocks),
        output_records: current
            .output_records
            .saturating_sub(previous.output_records),
        output_bytes: current.output_bytes.saturating_sub(previous.output_bytes),
        output_blocks: current.output_blocks.saturating_sub(previous.output_blocks),
        range_limit: current.range_limit,
        effective_lanes: current.effective_lanes,
        active_lanes: current.active_lanes,
        peak_active_lanes: current.peak_active_lanes,
        waiting_lanes: current.waiting_lanes,
        sort_chunks: current.sort_chunks.saturating_sub(previous.sort_chunks),
        sort_merge_passes: current
            .sort_merge_passes
            .saturating_sub(previous.sort_merge_passes),
        sort_peak_workspace_bytes: current.sort_peak_workspace_bytes,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EffectiveLanes {
    count: u64,
    reason: &'static str,
}

fn effective_lanes(
    parallelism: CompactionParallelism,
    range_limit: u64,
    engine_effective: u64,
) -> EffectiveLanes {
    let configured = parallelism.configured_lanes() as u64;
    let workers = parallelism.worker_limit() as u64;
    let budget = parallelism.budget_limit() as u64;
    let count = configured.min(workers).min(budget).min(range_limit);
    // Range availability wins ties because it is the final, runtime-only cap;
    // the remaining order keeps the bounded reason deterministic.
    let reason = if range_limit == count {
        "ranges"
    } else if budget == count {
        "budget"
    } else if workers == count {
        "workers"
    } else {
        "configured"
    };
    EffectiveLanes {
        count: engine_effective,
        reason,
    }
}

pub(crate) async fn await_with_compaction_heartbeats<F: Future>(
    telemetry: &CompactionTelemetry,
    future: F,
) -> F::Output {
    let future = future.instrument(telemetry.span());
    tokio::pin!(future);
    loop {
        let delay = tokio::time::sleep(telemetry.until_heartbeat());
        tokio::pin!(delay);
        tokio::select! {
            result = &mut future => return result,
            _ = &mut delay => telemetry.heartbeat(),
        }
    }
}

fn snapshot(state: &BuilderProgressState, now: Instant) -> BuilderProgressSnapshot {
    BuilderProgressSnapshot {
        records: state.records,
        bytes: state.bytes,
        units: state.units,
        elapsed_seconds: now.saturating_duration_since(state.started).as_secs_f64(),
        last_progress_age_seconds: now
            .saturating_duration_since(state.last_progress)
            .as_secs_f64(),
    }
}

fn take_emission(state: &mut BuilderProgressState, now: Instant) -> BuilderProgressEmission {
    let emission = BuilderProgressEmission {
        snapshot: snapshot(state, now),
        records: state.records.saturating_sub(state.emitted_records),
        bytes: state.bytes.saturating_sub(state.emitted_bytes),
        units: state.units.saturating_sub(state.emitted_units),
        interval_seconds: now.saturating_duration_since(state.last_emit).as_secs_f64(),
    };
    state.emitted_records = state.records;
    state.emitted_bytes = state.bytes;
    state.emitted_units = state.units;
    state.last_emit = now;
    emission
}

pub(crate) async fn await_with_builder_heartbeats<F: Future>(
    progress: &BuilderProgress,
    future: F,
) -> F::Output {
    let future = future.instrument(progress.span());
    tokio::pin!(future);
    loop {
        let delay = tokio::time::sleep(progress.until_heartbeat());
        tokio::pin!(delay);
        tokio::select! {
            result = &mut future => return result,
            _ = &mut delay => progress.heartbeat(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> IndexTelemetryIdentity {
        IndexTelemetryIdentity {
            index_id: 7,
            tenant_id: 8,
            bucket_id: 9,
            kind: IndexKind::Path,
        }
    }

    #[test]
    fn builder_progress_accumulates_without_emitting_each_unit() {
        let progress = BuilderProgress::start(identity(), BuilderProgressPhase::Rebuild);
        progress.advance(3, 40);
        progress.advance(5, 60);
        let snapshot = progress.snapshot();
        assert_eq!(snapshot.records, 8);
        assert_eq!(snapshot.bytes, 100);
        assert_eq!(snapshot.units, 2);
        assert_eq!(progress.lock().emitted_units, 0);
        progress.complete();
    }

    #[test]
    fn debt_counts_only_tiers_over_the_bound() {
        let mut segments = vec![(0, 10); 5];
        segments.extend(vec![(1, 10); 4]);

        let (debt, tiers) = compaction_debt_summaries(segments.clone(), 4, u64::MAX);
        assert_eq!(
            debt,
            CompactionDebtSnapshot {
                tiers: 1,
                segments: 5,
                bytes: 50,
            }
        );
        assert_eq!(tiers[&0].segments, 5);
        assert_eq!(
            compaction_debt_summaries(segments.clone(), 5, u64::MAX).0,
            CompactionDebtSnapshot::default()
        );
        assert_eq!(compaction_debt_summaries(segments, 5, 49).0, debt);
    }

    #[tokio::test]
    async fn heartbeat_wait_preserves_the_wrapped_result() {
        let progress = BuilderProgress::start(identity(), BuilderProgressPhase::CatchUp);
        let result = await_with_builder_heartbeats(&progress, async { 17 }).await;
        assert_eq!(result, 17);
        progress.complete();
    }

    #[test]
    fn range_availability_caps_effective_lanes_and_wins_ties() {
        let parallelism = CompactionParallelism::new(
            4,
            keldra_index::compaction::COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES,
        )
        .unwrap();

        assert_eq!(
            effective_lanes(parallelism, 2, 2),
            EffectiveLanes {
                count: 2,
                reason: "ranges",
            }
        );
        assert_eq!(
            effective_lanes(parallelism, 4, 4),
            EffectiveLanes {
                count: 4,
                reason: "ranges",
            }
        );
    }

    #[test]
    fn fixed_lane_caps_have_a_deterministic_reason() {
        let ample_budget = (keldra_index::compaction::COMPACTION_SHARED_WORKSPACE_BYTES
            + 8 * keldra_index::compaction::COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES)
            as u64;
        let configured = CompactionParallelism::for_budget(2, 4, ample_budget).unwrap();
        let workers = CompactionParallelism::for_budget(4, 2, ample_budget).unwrap();
        let budget = CompactionParallelism::for_budget(
            4,
            4,
            keldra_index::compaction::COMPACTION_SHARED_WORKSPACE_BYTES as u64,
        )
        .unwrap();

        assert_eq!(
            effective_lanes(configured, 8, 2),
            EffectiveLanes {
                count: 2,
                reason: "configured",
            }
        );
        assert_eq!(
            effective_lanes(workers, 8, 2),
            EffectiveLanes {
                count: 2,
                reason: "workers",
            }
        );
        assert_eq!(
            effective_lanes(budget, 8, 1),
            EffectiveLanes {
                count: 1,
                reason: "budget",
            }
        );
    }
}
