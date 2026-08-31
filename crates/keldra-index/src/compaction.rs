//! Format-neutral execution, admission, and progress primitives for native
//! index construction and compaction.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::IndexError;

/// Bounded decode workspace for one native projection component.
const INDEX_DECODE_BYTES: usize = 4 * 1024 * 1024;
/// Bounded immutable projection pack emitted by one sealing lane.
const INDEX_ARTIFACT_PACK_BYTES: usize = 16 * 1024 * 1024;

/// A four-input range merge may retain twelve maximum decoded input leaves.
/// Its four former output slots are one incremental 16 MiB artifact pack, so
/// the complete lane remains exactly 64 MiB without hiding publication memory.
const DECODED_INPUT_COMPONENTS_PER_LANE: usize = 12;
const COMPLETE_LANE_WORKSPACE_BYTES: usize =
    DECODED_INPUT_COMPONENTS_PER_LANE * INDEX_DECODE_BYTES + INDEX_ARTIFACT_PACK_BYTES;

/// Workspace charged before the first compaction lane starts.
pub const COMPACTION_SHARED_WORKSPACE_BYTES: usize = COMPLETE_LANE_WORKSPACE_BYTES;
/// Additional workspace charged for every compaction lane after the first.
pub const COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES: usize = COMPLETE_LANE_WORKSPACE_BYTES;

/// One incremental artifact pack, one bounded block/codec workspace, and one
/// bounded streaming-routing workspace held while a native writer seals
/// through the ordinary-object sink. The two non-pack workspaces overlap: a
/// data block is encoded while previously published routing levels remain
/// resident.
pub const FIXED_INDEX_SEAL_WORKSPACE_BYTES: usize =
    INDEX_ARTIFACT_PACK_BYTES + 2 * INDEX_DECODE_BYTES;
/// Smallest supported per-kind construction pool.
pub const MIN_INDEX_KIND_MEMORY_BYTES: usize = 64 * 1024 * 1024;

/// Conservative division of one process-wide per-kind construction budget.
///
/// Native writers receive the complete admitted total. The two subordinate
/// values bound caller-retained source projections and mutable writer state;
/// together with [`FIXED_INDEX_SEAL_WORKSPACE_BYTES`] they exactly partition
/// the pool, so no phase relies on an uncharged codec or artifact-pack buffer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentMemoryPlan {
    pub total_bytes: usize,
    pub max_resident_bytes: usize,
    pub max_source_projection_bytes: usize,
}

impl SegmentMemoryPlan {
    pub fn new(total_bytes: usize) -> Result<Self, IndexError> {
        if total_bytes < MIN_INDEX_KIND_MEMORY_BYTES
            || total_bytes <= FIXED_INDEX_SEAL_WORKSPACE_BYTES
        {
            return Err(IndexError::ResourceLimit {
                needed: MIN_INDEX_KIND_MEMORY_BYTES,
                limit: total_bytes,
            });
        }
        let max_resident_bytes = (total_bytes - FIXED_INDEX_SEAL_WORKSPACE_BYTES) / 2;
        let max_source_projection_bytes = total_bytes
            .checked_sub(FIXED_INDEX_SEAL_WORKSPACE_BYTES)
            .and_then(|bytes| bytes.checked_sub(max_resident_bytes))
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(Self {
            total_bytes,
            max_resident_bytes,
            max_source_projection_bytes,
        })
    }

    pub fn seal_workspace_bytes(self, resident_bytes: usize) -> Result<usize, IndexError> {
        if resident_bytes > self.max_resident_bytes {
            return Err(IndexError::ResourceLimit {
                needed: resident_bytes,
                limit: self.max_resident_bytes,
            });
        }
        resident_bytes
            .checked_add(FIXED_INDEX_SEAL_WORKSPACE_BYTES)
            .ok_or(IndexError::OffsetOverflow)
    }
}

/// Validated per-compaction parallelism admitted by the process-wide kind
/// pool. It is execution policy only and is never persisted in index bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionParallelism {
    configured_lanes: usize,
    worker_limit: usize,
    budget_limit: usize,
    max_lanes: usize,
    workspace_bytes_per_lane: usize,
}

impl CompactionParallelism {
    pub const fn serial() -> Self {
        Self {
            configured_lanes: 1,
            worker_limit: 1,
            budget_limit: 1,
            max_lanes: 1,
            workspace_bytes_per_lane: COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES,
        }
    }

    pub fn new(max_lanes: usize, workspace_bytes_per_lane: usize) -> Result<Self, IndexError> {
        if max_lanes == 0 {
            return Err(IndexError::InvalidDefinition(
                "compaction lane count must be greater than zero".into(),
            ));
        }
        if workspace_bytes_per_lane < COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES,
                limit: workspace_bytes_per_lane,
            });
        }
        max_lanes
            .saturating_sub(1)
            .checked_mul(workspace_bytes_per_lane)
            .and_then(|additional| additional.checked_add(COMPACTION_SHARED_WORKSPACE_BYTES))
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(Self {
            configured_lanes: max_lanes,
            worker_limit: max_lanes,
            budget_limit: max_lanes,
            max_lanes,
            workspace_bytes_per_lane,
        })
    }

    pub fn for_budget(
        requested_lanes: usize,
        rayon_workers: usize,
        budget_bytes: u64,
    ) -> Result<Self, IndexError> {
        if requested_lanes == 0 || rayon_workers == 0 {
            return Err(IndexError::InvalidDefinition(
                "compaction lanes and Rayon workers must be greater than zero".into(),
            ));
        }
        let budget = usize::try_from(budget_bytes).map_err(|_| IndexError::OffsetOverflow)?;
        if budget < COMPACTION_SHARED_WORKSPACE_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: COMPACTION_SHARED_WORKSPACE_BYTES,
                limit: budget,
            });
        }
        let admitted_by_memory = 1usize.saturating_add(
            (budget - COMPACTION_SHARED_WORKSPACE_BYTES)
                / COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES,
        );
        let mut value = Self::new(
            requested_lanes.min(rayon_workers).min(admitted_by_memory),
            COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES,
        )?;
        value.configured_lanes = requested_lanes;
        value.worker_limit = rayon_workers;
        value.budget_limit = admitted_by_memory;
        Ok(value)
    }

    pub const fn configured_lanes(self) -> usize {
        self.configured_lanes
    }

    pub const fn worker_limit(self) -> usize {
        self.worker_limit
    }

    pub const fn budget_limit(self) -> usize {
        self.budget_limit
    }

    pub const fn max_lanes(self) -> usize {
        self.max_lanes
    }

    pub const fn workspace_bytes_per_lane(self) -> usize {
        self.workspace_bytes_per_lane
    }

    pub const fn shared_workspace_bytes(self) -> usize {
        COMPACTION_SHARED_WORKSPACE_BYTES
    }

    pub const fn incremental_lane_workspace_bytes(self) -> usize {
        self.workspace_bytes_per_lane
    }

    pub fn admitted_bytes(self) -> Result<usize, IndexError> {
        self.max_lanes
            .saturating_sub(1)
            .checked_mul(self.workspace_bytes_per_lane)
            .and_then(|additional| additional.checked_add(COMPACTION_SHARED_WORKSPACE_BYTES))
            .ok_or(IndexError::OffsetOverflow)
    }
}

impl Default for CompactionParallelism {
    fn default() -> Self {
        Self::serial()
    }
}

/// Boxed asynchronous task accepted by the runtime-owned executor.
pub type CompactionTaskFuture =
    Pin<Box<dyn Future<Output = Result<(), IndexError>> + Send + 'static>>;

pub trait CompactionTaskHandle:
    Future<Output = Result<(), IndexError>> + Send + Unpin + 'static
{
    fn abort(&self);
}

/// Storage-neutral boundary between async orchestration and finite CPU work.
pub trait CompactionExecutor: Clone + Send + Sync + 'static {
    type Task: CompactionTaskHandle;

    fn spawn_io(&self, task: CompactionTaskFuture) -> Self::Task;

    fn run_cpu<T, F>(&self, work: F) -> impl Future<Output = Result<T, IndexError>> + Send
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, IndexError> + Send + 'static;
}

/// Lock-free, low-cardinality aggregate counters for long-running native
/// construction and compaction work.
#[derive(Clone, Default)]
pub struct CompactionProgress {
    inner: Arc<ProgressCounters>,
}

#[derive(Default)]
struct ProgressCounters {
    ranges_total: AtomicU64,
    ranges_completed: AtomicU64,
    input_records: AtomicU64,
    input_bytes: AtomicU64,
    input_blocks: AtomicU64,
    output_records: AtomicU64,
    output_bytes: AtomicU64,
    output_blocks: AtomicU64,
    effective_lanes: AtomicU64,
    range_limit: AtomicU64,
    active_lanes: AtomicU64,
    peak_active_lanes: AtomicU64,
    waiting_lanes: AtomicU64,
    sort_chunks: AtomicU64,
    sort_merge_passes: AtomicU64,
    sort_peak_workspace_bytes: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompactionProgressSnapshot {
    pub ranges_total: u64,
    pub ranges_completed: u64,
    pub input_records: u64,
    pub input_bytes: u64,
    pub input_blocks: u64,
    pub output_records: u64,
    pub output_bytes: u64,
    pub output_blocks: u64,
    pub effective_lanes: u64,
    pub range_limit: u64,
    pub active_lanes: u64,
    pub peak_active_lanes: u64,
    pub waiting_lanes: u64,
    pub sort_chunks: u64,
    pub sort_merge_passes: u64,
    pub sort_peak_workspace_bytes: u64,
}

impl CompactionProgress {
    pub fn record_input(&self, records: u64, bytes: u64, blocks: u64) {
        self.inner
            .input_records
            .fetch_add(records, Ordering::Relaxed);
        self.inner.input_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.inner.input_blocks.fetch_add(blocks, Ordering::Relaxed);
    }

    pub fn record_output(&self, records: u64, bytes: u64, blocks: u64) {
        self.inner
            .output_records
            .fetch_add(records, Ordering::Relaxed);
        self.inner.output_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.inner
            .output_blocks
            .fetch_add(blocks, Ordering::Relaxed);
    }

    pub fn record_range_limit(&self, range_limit: usize) -> Result<(), IndexError> {
        let range_limit = u64::try_from(range_limit).map_err(|_| IndexError::OffsetOverflow)?;
        self.inner
            .range_limit
            .fetch_max(range_limit, Ordering::Relaxed);
        Ok(())
    }

    pub fn add_ranges(&self, count: usize) -> Result<(), IndexError> {
        let count = u64::try_from(count).map_err(|_| IndexError::OffsetOverflow)?;
        self.inner.ranges_total.fetch_add(count, Ordering::Relaxed);
        self.inner.waiting_lanes.fetch_add(count, Ordering::Relaxed);
        Ok(())
    }

    pub fn record_effective_lanes(&self, lanes: usize) -> Result<(), IndexError> {
        let lanes = u64::try_from(lanes).map_err(|_| IndexError::OffsetOverflow)?;
        self.inner
            .effective_lanes
            .fetch_max(lanes, Ordering::Relaxed);
        Ok(())
    }

    pub fn start_range(&self) -> ActiveCompactionRange {
        self.inner.waiting_lanes.fetch_sub(1, Ordering::Relaxed);
        let active = self
            .inner
            .active_lanes
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.inner
            .peak_active_lanes
            .fetch_max(active, Ordering::Relaxed);
        ActiveCompactionRange {
            progress: self.clone(),
        }
    }

    pub fn record_sort_chunk(&self, workspace_bytes: usize) -> Result<(), IndexError> {
        let workspace_bytes =
            u64::try_from(workspace_bytes).map_err(|_| IndexError::OffsetOverflow)?;
        self.inner.sort_chunks.fetch_add(1, Ordering::Relaxed);
        self.inner
            .sort_peak_workspace_bytes
            .fetch_max(workspace_bytes, Ordering::Relaxed);
        Ok(())
    }

    pub fn record_sort_merge_pass(&self) {
        self.inner.sort_merge_passes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CompactionProgressSnapshot {
        CompactionProgressSnapshot {
            ranges_total: self.inner.ranges_total.load(Ordering::Relaxed),
            ranges_completed: self.inner.ranges_completed.load(Ordering::Relaxed),
            input_records: self.inner.input_records.load(Ordering::Relaxed),
            input_bytes: self.inner.input_bytes.load(Ordering::Relaxed),
            input_blocks: self.inner.input_blocks.load(Ordering::Relaxed),
            output_records: self.inner.output_records.load(Ordering::Relaxed),
            output_bytes: self.inner.output_bytes.load(Ordering::Relaxed),
            output_blocks: self.inner.output_blocks.load(Ordering::Relaxed),
            effective_lanes: self.inner.effective_lanes.load(Ordering::Relaxed),
            range_limit: self.inner.range_limit.load(Ordering::Relaxed),
            active_lanes: self.inner.active_lanes.load(Ordering::Relaxed),
            peak_active_lanes: self.inner.peak_active_lanes.load(Ordering::Relaxed),
            waiting_lanes: self.inner.waiting_lanes.load(Ordering::Relaxed),
            sort_chunks: self.inner.sort_chunks.load(Ordering::Relaxed),
            sort_merge_passes: self.inner.sort_merge_passes.load(Ordering::Relaxed),
            sort_peak_workspace_bytes: self.inner.sort_peak_workspace_bytes.load(Ordering::Relaxed),
        }
    }
}

pub struct ActiveCompactionRange {
    progress: CompactionProgress,
}

impl ActiveCompactionRange {
    pub fn complete(self) {
        self.progress
            .inner
            .ranges_completed
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for ActiveCompactionRange {
    fn drop(&mut self) {
        self.progress
            .inner
            .active_lanes
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_pool_admits_four_complete_lanes() {
        assert_eq!(COMPLETE_LANE_WORKSPACE_BYTES, 64 * 1024 * 1024);
        assert_eq!(FIXED_INDEX_SEAL_WORKSPACE_BYTES, 24 * 1024 * 1024);
        let parallelism = CompactionParallelism::for_budget(4, 4, 256 * 1024 * 1024).unwrap();
        assert_eq!(parallelism.max_lanes(), 4);
        assert_eq!(parallelism.admitted_bytes().unwrap(), 256 * 1024 * 1024);
    }

    #[test]
    fn native_memory_plan_is_bounded() {
        let plan = SegmentMemoryPlan::new(MIN_INDEX_KIND_MEMORY_BYTES).unwrap();
        assert_eq!(
            plan.max_resident_bytes
                + plan.max_source_projection_bytes
                + FIXED_INDEX_SEAL_WORKSPACE_BYTES,
            plan.total_bytes
        );
        assert!(plan.seal_workspace_bytes(plan.max_resident_bytes).unwrap() <= plan.total_bytes);
    }

    #[test]
    fn progress_tracks_ranges_and_component_work() {
        let progress = CompactionProgress::default();
        progress.add_ranges(1).unwrap();
        let range = progress.start_range();
        progress.record_input(7, 100, 2);
        progress.record_output(5, 80, 1);
        range.complete();
        let snapshot = progress.snapshot();
        assert_eq!(snapshot.ranges_completed, 1);
        assert_eq!(snapshot.active_lanes, 0);
        assert_eq!(snapshot.input_records, 7);
        assert_eq!(snapshot.output_records, 5);
    }
}
