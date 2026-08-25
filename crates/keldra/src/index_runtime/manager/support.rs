//! Shared admission, placement, telemetry, and error conversion helpers.

use super::*;

pub(super) fn source_wire_limit(limit: u64) -> u64 {
    let fixed = FIXED_INDEX_SEAL_WORKSPACE_BYTES as u64;
    let remaining = limit.saturating_sub(fixed);
    let builder_reserve = remaining / 2;
    let safe = remaining
        .saturating_sub(builder_reserve)
        .saturating_sub(256);
    MAX_SOURCE_WIRE_BYTES.min(safe.max(64 * 1024))
}

pub(super) fn work_plan_for_limit(
    limit: u64,
    source_resident_bytes: u64,
    segment_flush_bytes: u64,
) -> Result<SegmentMemoryPlan, Status> {
    let total = usize::try_from(limit)
        .map_err(|_| Status::resource_exhausted("index construction budget exceeds platform"))?;
    let source_resident = usize::try_from(source_resident_bytes)
        .map_err(|_| Status::resource_exhausted("index source frame exceeds platform"))?;
    let available = total.checked_sub(source_resident).ok_or_else(|| {
        Status::resource_exhausted("resident index source frame exhausts its kind budget")
    })?;
    let configured = SegmentMemoryPlan::new(total).map_err(index_status)?;
    let segment_flush_bytes = usize::try_from(segment_flush_bytes)
        .map_err(|_| Status::resource_exhausted("index segment flush target exceeds platform"))?;
    let max_resident_bytes = configured.max_resident_bytes.min(segment_flush_bytes);
    let max_source_projection_bytes = available
        .checked_sub(FIXED_INDEX_SEAL_WORKSPACE_BYTES)
        .and_then(|bytes| bytes.checked_sub(max_resident_bytes))
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| {
            Status::resource_exhausted("index source frame leaves no bounded projection workspace")
        })?;
    Ok(SegmentMemoryPlan {
        total_bytes: total,
        max_resident_bytes,
        max_source_projection_bytes,
    })
}

pub(super) fn emit_source_lag(kind: IndexKind, from: &IndexBarrier, target: &IndexBarrier) {
    let lag = from.sources.iter().fold(0_u64, |total, (node, cursor)| {
        total.saturating_add(target.sources.get(node).map_or(0, |latest| {
            latest.next_offset.saturating_sub(cursor.next_offset)
        }))
    });
    tracing::info!(
        index.kind = ?kind,
        gauge.keldra_index_source_lag = lag,
        gauge.keldra_index_publication_fresh = u64::from(lag == 0),
        "index source lag observed"
    );
}

pub(super) fn emit_publication_age(kind: IndexKind, current: Option<&CommittedIndexView>) {
    let Some(current) = current else {
        tracing::info!(
            index.kind = ?kind,
            gauge.keldra_index_publication_present = 0_u64,
            gauge.keldra_index_publication_age_seconds = 0_f64,
            gauge.keldra_index_publication_fresh = 0_u64,
            "index has no published committed view"
        );
        return;
    };
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u64, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        });
    let age_seconds = now_millis.saturating_sub(current.pointer.current.published_at_unix_millis)
        as f64
        / 1_000.0;
    tracing::info!(
        index.kind = ?kind,
        gauge.keldra_index_publication_present = 1_u64,
        gauge.keldra_index_publication_age_seconds = age_seconds,
        "index publication age observed"
    );
}

pub(super) fn current_placement(decisions: &DecisionRaft) -> Result<ClusterPlacement, Status> {
    let state = decisions
        .state()
        .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
    ClusterPlacement::from_applied(&state).map_err(|error| Status::unavailable(error.to_string()))
}

pub(super) fn budget_status(error: IndexBudgetError) -> Status {
    Status::resource_exhausted(error.to_string())
}

pub(super) fn cpu_status(error: IndexCpuPoolError) -> Status {
    Status::internal(error.to_string())
}

pub(super) fn index_status(error: IndexError) -> Status {
    match error {
        IndexError::ResourceLimit { .. } => Status::resource_exhausted(error.to_string()),
        IndexError::Io(_) => Status::unavailable(error.to_string()),
        _ => Status::data_loss(error.to_string()),
    }
}

pub(super) fn event_status(error: IndexEventError) -> Status {
    match error {
        IndexEventError::Placement(_)
        | IndexEventError::AtomicProgramInProgress
        | IndexEventError::Source { .. }
        | IndexEventError::Task(_) => Status::unavailable(error.to_string()),
        IndexEventError::BarrierChanged => Status::aborted(error.to_string()),
        IndexEventError::CheckpointMismatch(_)
        | IndexEventError::SourceEpochChanged(_)
        | IndexEventError::SourceHistoryGap(_)
        | IndexEventError::IncompleteSources => Status::failed_precondition(error.to_string()),
        IndexEventError::PageBytesExceeded { .. } => Status::resource_exhausted(error.to_string()),
        IndexEventError::ZeroPageByteLimit => Status::invalid_argument(error.to_string()),
        IndexEventError::InvalidSourceStatus(_)
        | IndexEventError::InvalidAtomicBatch(_)
        | IndexEventError::NonContiguousSource(_)
        | IndexEventError::OffsetOverflow(_)
        | IndexEventError::PageLengthOverflow
        | IndexEventError::PageLengthMismatch { .. }
        | IndexEventError::Encode(_) => Status::data_loss(error.to_string()),
    }
}

pub(super) fn commit_view_status(error: super::super::committed_view::CommitViewError) -> Status {
    Status::data_loss(error.to_string())
}
