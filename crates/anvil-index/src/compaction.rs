//! Storage-neutral bounds and progress for deterministic range compaction.
//!
//! Engine mergers keep canonical row assembly ordered even when disjoint input
//! ranges are prepared concurrently.  These types contain no runtime, storage,
//! or telemetry dependency: Anvil may snapshot the counters and export them
//! through its existing tracing/OTLP bridge.

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};

use crate::full_text::MAX_TEXT_POSTING_KEY_BYTES;
use crate::model::{
    MAX_RUN_ROOT_WORKSPACE_BYTES, MAX_RUN_VIEW_WORKSPACE_BYTES, routing_descriptor_workspace_bytes,
    routing_page_workspace_bytes, routing_traversal_workspace_bytes,
};
use crate::{
    BlockDescriptor, IndexError, MAX_INDEX_BLOCK_BYTES, MAX_INDEX_DECODED_BLOCK_BYTES,
    MAX_INDEX_ROUTING_KEY_BYTES,
};

pub(crate) const MAX_COMPACTION_INPUT_RUNS: usize = 4;
/// One external-sort chunk retained by a compaction lane before it is sealed
/// as an ordinary staged component.
pub(crate) const MAX_EXTERNAL_SORT_CHUNK_RESIDENT_BYTES: usize = 4 * 1024 * 1024;
const ORDINAL_ROUTING_KEY_BYTES: usize = 8;
const COMPACTION_CODEC_AND_OUTPUT_BUFFERS: usize = 5;
const SOURCE_PHASE_RETAINED_DECODED_LEAVES: usize = 12;
const TEXT_PHASE_RETAINED_DECODED_LEAVES: usize = 13;
const TEXT_SELECTION_RETAINED_DECODED_LEAVES: usize = 2 * MAX_COMPACTION_INPUT_RUNS + 2;
const CACHED_LEAF_METADATA_OVERHEAD_BYTES: usize = 128;
const KEY_RANGE_METADATA_OVERHEAD_BYTES: usize = 128;
const MAX_LANE_KEY_RANGE_COPIES: usize = MAX_COMPACTION_INPUT_RUNS * 2 + 4;
const MAX_ROOT_DESCRIPTOR_COPIES: usize = MAX_COMPACTION_INPUT_RUNS * 4;
const MAX_CURRENT_ROW_COPIES: usize = MAX_COMPACTION_INPUT_RUNS * 4;
const CONTAINER_AND_TASK_OVERHEAD_BYTES: usize = 512 * 1024;

const ENUMERATED_COMPACTION_METADATA_BYTES: usize = MAX_COMPACTION_INPUT_RUNS
    * MAX_RUN_VIEW_WORKSPACE_BYTES
    + TEXT_PHASE_RETAINED_DECODED_LEAVES
        * (routing_descriptor_workspace_bytes(MAX_INDEX_ROUTING_KEY_BYTES)
            + CACHED_LEAF_METADATA_OVERHEAD_BYTES)
    + MAX_LANE_KEY_RANGE_COPIES
        * (2 * MAX_INDEX_ROUTING_KEY_BYTES + KEY_RANGE_METADATA_OVERHEAD_BYTES)
    + MAX_ROOT_DESCRIPTOR_COPIES * routing_descriptor_workspace_bytes(MAX_INDEX_ROUTING_KEY_BYTES)
    + MAX_CURRENT_ROW_COPIES * (MAX_INDEX_ROUTING_KEY_BYTES + 128)
    + CONTAINER_AND_TASK_OVERHEAD_BYTES;

// Covers all four shared input RunViews using resident BTreeMap/descriptor
// sizes, cached-leaf descriptors, range/root/current-row copies, and bounded
// Vec/Arc/task bookkeeping. Charging it to every lane is conservative because
// the RunViews and range plan are shared by the whole compaction.
const COMPACTION_METADATA_WORKSPACE_BYTES: usize = 2 * 1024 * 1024;
const _: () = assert!(ENUMERATED_COMPACTION_METADATA_BYTES <= COMPACTION_METADATA_WORKSPACE_BYTES);

const fn retained_leaf_workspace_bytes(decoded_leaves: usize) -> usize {
    decoded_leaves * MAX_INDEX_DECODED_BLOCK_BYTES + MAX_INDEX_BLOCK_BYTES
}

const COMMON_COMPACTION_WORKSPACE_BYTES: usize =
    COMPACTION_CODEC_AND_OUTPUT_BUFFERS * MAX_INDEX_BLOCK_BYTES + MAX_RUN_ROOT_WORKSPACE_BYTES;
const COMPLETE_COMMON_COMPACTION_WORKSPACE_BYTES: usize =
    COMMON_COMPACTION_WORKSPACE_BYTES + COMPACTION_METADATA_WORKSPACE_BYTES;

// Path/document/payload phases retain four path cursors while streaming one
// path-keyed output tree and two ordinal-keyed output trees.
const SOURCE_PHASE_WORKSPACE_BYTES: usize = COMPLETE_COMMON_COMPACTION_WORKSPACE_BYTES
    + retained_leaf_workspace_bytes(SOURCE_PHASE_RETAINED_DECODED_LEAVES)
    + MAX_COMPACTION_INPUT_RUNS * routing_traversal_workspace_bytes(MAX_INDEX_ROUTING_KEY_BYTES)
    + routing_traversal_workspace_bytes(MAX_INDEX_ROUTING_KEY_BYTES)
    + 2 * routing_traversal_workspace_bytes(ORDINAL_ROUTING_KEY_BYTES)
    + routing_page_workspace_bytes(MAX_INDEX_ROUTING_KEY_BYTES)
    + 2 * routing_page_workspace_bytes(ORDINAL_ROUTING_KEY_BYTES);

// Routed phases retain four maximum-key cursors and one maximum-key output
// tree. Point reads traverse at most one additional decoded routing page.
const ROUTED_PHASE_WORKSPACE_BYTES: usize = COMPLETE_COMMON_COMPACTION_WORKSPACE_BYTES
    + retained_leaf_workspace_bytes(SOURCE_PHASE_RETAINED_DECODED_LEAVES)
    + (MAX_COMPACTION_INPUT_RUNS + 1)
        * routing_traversal_workspace_bytes(MAX_INDEX_ROUTING_KEY_BYTES)
    + routing_page_workspace_bytes(MAX_INDEX_ROUTING_KEY_BYTES);

// Full-text and hybrid posting phases retain thirteen decoded data leaves, but
// their canonical posting routing keys are much smaller than object paths.
const TEXT_PHASE_WORKSPACE_BYTES: usize = COMPLETE_COMMON_COMPACTION_WORKSPACE_BYTES
    + retained_leaf_workspace_bytes(TEXT_PHASE_RETAINED_DECODED_LEAVES)
    + (MAX_COMPACTION_INPUT_RUNS + 1)
        * routing_traversal_workspace_bytes(MAX_TEXT_POSTING_KEY_BYTES)
    + routing_page_workspace_bytes(MAX_INDEX_ROUTING_KEY_BYTES);

// Range-striped text selection retains four input path leaves, one staged
// final-path leaf, four source-ordinal posting leaves, and one selected-row
// writer. It precedes the final external sort, so no sort chunk overlaps it.
const TEXT_SELECTION_PHASE_WORKSPACE_BYTES: usize = COMPLETE_COMMON_COMPACTION_WORKSPACE_BYTES
    + retained_leaf_workspace_bytes(TEXT_SELECTION_RETAINED_DECODED_LEAVES)
    + (MAX_COMPACTION_INPUT_RUNS + 1)
        * routing_traversal_workspace_bytes(MAX_INDEX_ROUTING_KEY_BYTES)
    + MAX_COMPACTION_INPUT_RUNS * routing_traversal_workspace_bytes(MAX_TEXT_POSTING_KEY_BYTES)
    + routing_page_workspace_bytes(MAX_TEXT_POSTING_KEY_BYTES);

// The external-sort phase is deliberately disjoint from source selection. It
// retains one byte-bounded row chunk while scanning one staged payload leaf,
// or up to four routed input leaves while merging spills. An online spill merge
// still overlaps that caller leaf, so six leaves cover caller, merge inputs,
// and output; the row chunk has already been consumed when a merge begins.
const EXTERNAL_SORT_PHASE_WORKSPACE_BYTES: usize = COMPLETE_COMMON_COMPACTION_WORKSPACE_BYTES
    + MAX_EXTERNAL_SORT_CHUNK_RESIDENT_BYTES
    + retained_leaf_workspace_bytes(MAX_COMPACTION_INPUT_RUNS + 2)
    + (MAX_COMPACTION_INPUT_RUNS + 1)
        * routing_traversal_workspace_bytes(MAX_INDEX_ROUTING_KEY_BYTES)
    + routing_page_workspace_bytes(MAX_INDEX_ROUTING_KEY_BYTES);

const MAX_LEGACY_COMPACTION_PHASE_WORKSPACE_BYTES: usize =
    if SOURCE_PHASE_WORKSPACE_BYTES > ROUTED_PHASE_WORKSPACE_BYTES {
        if SOURCE_PHASE_WORKSPACE_BYTES > TEXT_PHASE_WORKSPACE_BYTES {
            SOURCE_PHASE_WORKSPACE_BYTES
        } else {
            TEXT_PHASE_WORKSPACE_BYTES
        }
    } else if ROUTED_PHASE_WORKSPACE_BYTES > TEXT_PHASE_WORKSPACE_BYTES {
        ROUTED_PHASE_WORKSPACE_BYTES
    } else {
        TEXT_PHASE_WORKSPACE_BYTES
    };
const MAX_COMPACTION_PHASE_WORKSPACE_BYTES: usize =
    if MAX_LEGACY_COMPACTION_PHASE_WORKSPACE_BYTES > EXTERNAL_SORT_PHASE_WORKSPACE_BYTES {
        if MAX_LEGACY_COMPACTION_PHASE_WORKSPACE_BYTES > TEXT_SELECTION_PHASE_WORKSPACE_BYTES {
            MAX_LEGACY_COMPACTION_PHASE_WORKSPACE_BYTES
        } else {
            TEXT_SELECTION_PHASE_WORKSPACE_BYTES
        }
    } else if EXTERNAL_SORT_PHASE_WORKSPACE_BYTES > TEXT_SELECTION_PHASE_WORKSPACE_BYTES {
        EXTERNAL_SORT_PHASE_WORKSPACE_BYTES
    } else {
        TEXT_SELECTION_PHASE_WORKSPACE_BYTES
    };

/// One complete lane is hard-capped at 64 MiB. Four lanes therefore fit
/// exactly within the approved 256 MiB per-kind construction budget.
const COMPLETE_COMPACTION_LANE_WORKSPACE_BYTES: usize = 64 * 1024 * 1024;
const _: () =
    assert!(MAX_COMPACTION_PHASE_WORKSPACE_BYTES <= COMPLETE_COMPACTION_LANE_WORKSPACE_BYTES);

/// Shared root assembly, run metadata, and one complete first-lane workspace.
pub const COMPACTION_SHARED_WORKSPACE_BYTES: usize = COMPLETE_COMPACTION_LANE_WORKSPACE_BYTES;
/// Conservative workspace added for every lane after the first.
///
/// Every additional lane is charged the same complete phase maximum.
pub const COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES: usize =
    COMPLETE_COMPACTION_LANE_WORKSPACE_BYTES;

/// Reject fan-in that exceeds the resident-set proof used by every parallel
/// merger. The runtime selects at most four runs, but the engine entrypoints
/// enforce the same boundary so direct callers cannot silently exceed it.
pub(crate) fn validate_parallel_compaction_fan_in(run_count: usize) -> Result<(), IndexError> {
    if run_count == 0 {
        return Err(IndexError::InvalidDefinition(
            "parallel compaction requires at least one input run".into(),
        ));
    }
    if run_count > MAX_COMPACTION_INPUT_RUNS {
        return Err(IndexError::ResourceLimit {
            needed: run_count,
            limit: MAX_COMPACTION_INPUT_RUNS,
        });
    }
    Ok(())
}

/// Validated per-compaction parallelism admitted by the process-wide kind pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionParallelism {
    configured_lanes: usize,
    worker_limit: usize,
    budget_limit: usize,
    max_lanes: usize,
    workspace_bytes_per_lane: usize,
}

impl CompactionParallelism {
    /// Preserve the historical single-writer execution profile.
    pub const fn serial() -> Self {
        Self {
            configured_lanes: 1,
            worker_limit: 1,
            budget_limit: 1,
            max_lanes: 1,
            workspace_bytes_per_lane: COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES,
        }
    }

    /// Validate an explicitly admitted lane count and lane charge.
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

    /// Cap requested lanes by CPU workers and the already-held per-kind bytes.
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

/// Boxed asynchronous range task accepted by an injected runtime executor.
pub type CompactionTaskFuture =
    Pin<Box<dyn Future<Output = Result<(), IndexError>> + Send + 'static>>;

/// Join handle whose drop/abort behavior keeps failed compactions bounded.
pub trait CompactionTaskHandle:
    Future<Output = Result<(), IndexError>> + Send + Unpin + 'static
{
    fn abort(&self);
}

/// Storage-neutral execution boundary for range I/O and bounded codec CPU.
///
/// Anvil implements `spawn_io` on its async runtime and `run_cpu` on the one
/// process-owned index Rayon pool. Engine code never creates a private pool or
/// uses Rayon's global registry.
pub trait CompactionExecutor: Clone + Send + Sync + 'static {
    type Task: CompactionTaskHandle;

    fn spawn_io(&self, task: CompactionTaskFuture) -> Self::Task;

    fn run_cpu<T, F>(&self, work: F) -> impl Future<Output = Result<T, IndexError>> + Send
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, IndexError> + Send + 'static;
}

/// Lock-free aggregate counters suitable for periodic operational snapshots.
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
    /// Maximum lanes available in any one component phase, not the sum of
    /// sequential ranges across path, routed, text, or projection phases.
    pub effective_lanes: u64,
    /// Maximum observed deterministic range capacity, capped at one above the
    /// admitted lane count. This distinguishes an exact range cap/tie from a
    /// worker or budget cap without planning an unbounded number of stripes.
    pub range_limit: u64,
    pub active_lanes: u64,
    pub peak_active_lanes: u64,
    pub waiting_lanes: u64,
    pub sort_chunks: u64,
    pub sort_merge_passes: u64,
    pub sort_peak_workspace_bytes: u64,
}

impl CompactionProgress {
    /// Add actual input work observed by an engine cursor or directory wrapper.
    pub fn record_input(&self, records: u64, bytes: u64, blocks: u64) {
        self.inner
            .input_records
            .fetch_add(records, Ordering::Relaxed);
        self.inner.input_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.inner.input_blocks.fetch_add(blocks, Ordering::Relaxed);
    }

    /// Add output work after it has been accepted by the canonical writer or
    /// durably staged by the runtime. Engine code records rows; storage code
    /// records encoded bytes and blocks so neither layer double-counts.
    pub fn record_output(&self, records: u64, bytes: u64, blocks: u64) {
        self.inner
            .output_records
            .fetch_add(records, Ordering::Relaxed);
        self.inner.output_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.inner
            .output_blocks
            .fetch_add(blocks, Ordering::Relaxed);
    }

    /// Record the planner's bounded range-capacity probe before lanes start.
    pub fn record_range_limit(&self, range_limit: usize) -> Result<(), IndexError> {
        let range_limit = u64::try_from(range_limit).map_err(|_| IndexError::OffsetOverflow)?;
        self.inner
            .range_limit
            .fetch_max(range_limit, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn record_sort_chunk(&self, workspace_bytes: usize) -> Result<(), IndexError> {
        let workspace_bytes =
            u64::try_from(workspace_bytes).map_err(|_| IndexError::OffsetOverflow)?;
        self.inner.sort_chunks.fetch_add(1, Ordering::Relaxed);
        self.inner
            .sort_peak_workspace_bytes
            .fetch_max(workspace_bytes, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn record_sort_merge_pass(&self) {
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

    pub(crate) fn add_ranges(&self, count: usize) -> Result<(), IndexError> {
        let count = u64::try_from(count).map_err(|_| IndexError::OffsetOverflow)?;
        self.inner.ranges_total.fetch_add(count, Ordering::Relaxed);
        self.inner
            .effective_lanes
            .fetch_max(count, Ordering::Relaxed);
        self.inner.range_limit.fetch_max(count, Ordering::Relaxed);
        self.inner.waiting_lanes.fetch_add(count, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn start_range(&self) -> ActiveCompactionRange {
        self.inner.waiting_lanes.fetch_sub(1, Ordering::Relaxed);
        let active_lanes = self
            .inner
            .active_lanes
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.inner
            .peak_active_lanes
            .fetch_max(active_lanes, Ordering::Relaxed);
        ActiveCompactionRange {
            progress: self.clone(),
        }
    }

    fn pending_range(&self) -> PendingCompactionRange {
        PendingCompactionRange {
            progress: self.clone(),
            started: false,
        }
    }
}

struct PendingCompactionRange {
    progress: CompactionProgress,
    started: bool,
}

impl PendingCompactionRange {
    fn start(mut self) -> ActiveCompactionRange {
        self.started = true;
        self.progress.start_range()
    }
}

impl Drop for PendingCompactionRange {
    fn drop(&mut self) {
        if !self.started {
            self.progress
                .inner
                .waiting_lanes
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub(crate) struct ActiveCompactionRange {
    progress: CompactionProgress,
}

impl ActiveCompactionRange {
    pub(crate) fn complete(self) {
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

/// One half-open byte-lexical stripe. The first/last stripe is unbounded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct KeyRange {
    pub(crate) lower: Option<Vec<u8>>,
    pub(crate) upper: Option<Vec<u8>>,
}

impl KeyRange {
    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        self.lower.as_deref().is_none_or(|lower| key >= lower)
            && self.upper.as_deref().is_none_or(|upper| key < upper)
    }

    pub(crate) fn intersects(&self, descriptor: &BlockDescriptor) -> bool {
        self.lower
            .as_deref()
            .is_none_or(|lower| descriptor.maximum_key.as_slice() >= lower)
            && self
                .upper
                .as_deref()
                .is_none_or(|upper| descriptor.minimum_key.as_slice() < upper)
    }
}

/// Produce deterministic, disjoint ranges spanning the complete key space.
///
/// Root bounds select byte-lexical split points without scanning leaves. When
/// the observed interval is too narrow for all requested lanes, fewer lanes are
/// returned rather than emitting empty or overlapping work.
pub(crate) fn deterministic_key_ranges(
    roots: impl IntoIterator<Item = BlockDescriptor>,
    max_lanes: usize,
) -> Vec<KeyRange> {
    deterministic_key_range_plan(roots, max_lanes).ranges
}

pub(crate) struct DeterministicRangePlan {
    pub(crate) ranges: Vec<KeyRange>,
    pub(crate) range_limit: usize,
}

pub(crate) fn deterministic_key_range_plan(
    roots: impl IntoIterator<Item = BlockDescriptor>,
    max_lanes: usize,
) -> DeterministicRangePlan {
    deterministic_range_plan_from_bounds(
        roots
            .into_iter()
            .map(|root| (root.minimum_key, root.maximum_key)),
        max_lanes,
    )
}

/// Plan by a routed row's primary key, never its ordinal/position suffix.
pub(crate) fn deterministic_suffix_key_range_plan(
    roots: impl IntoIterator<Item = BlockDescriptor>,
    suffix_bytes: usize,
    max_lanes: usize,
) -> Result<DeterministicRangePlan, IndexError> {
    let mut bounds = Vec::new();
    for root in roots {
        if root.minimum_key.len() <= suffix_bytes || root.maximum_key.len() <= suffix_bytes {
            return Err(IndexError::InvalidFormat("range key suffix"));
        }
        bounds.push((
            root.minimum_key[..root.minimum_key.len() - suffix_bytes].to_vec(),
            root.maximum_key[..root.maximum_key.len() - suffix_bytes].to_vec(),
        ));
    }
    Ok(deterministic_range_plan_from_bounds(bounds, max_lanes))
}

/// Plan by the prefix before a delimiter so all rows in a logical group stay
/// in one lane even when the encoded key carries an ordinal/field suffix.
pub(crate) fn deterministic_delimited_key_range_plan(
    roots: impl IntoIterator<Item = BlockDescriptor>,
    delimiter: u8,
    max_lanes: usize,
) -> Result<DeterministicRangePlan, IndexError> {
    let mut bounds = Vec::new();
    for root in roots {
        let minimum = root
            .minimum_key
            .iter()
            .position(|byte| *byte == delimiter)
            .ok_or(IndexError::InvalidFormat("range key delimiter"))?;
        let maximum = root
            .maximum_key
            .iter()
            .position(|byte| *byte == delimiter)
            .ok_or(IndexError::InvalidFormat("range key delimiter"))?;
        bounds.push((
            root.minimum_key[..minimum].to_vec(),
            root.maximum_key[..maximum].to_vec(),
        ));
    }
    Ok(deterministic_range_plan_from_bounds(bounds, max_lanes))
}

fn deterministic_range_plan_from_bounds(
    bounds: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    max_lanes: usize,
) -> DeterministicRangePlan {
    let bounds = bounds.into_iter().collect::<Vec<_>>();
    if bounds.is_empty() {
        return DeterministicRangePlan {
            ranges: vec![KeyRange {
                lower: None,
                upper: None,
            }],
            range_limit: 1,
        };
    }
    let minimum = bounds
        .iter()
        .map(|(minimum, _)| minimum.as_slice())
        .min()
        .unwrap();
    let maximum = bounds
        .iter()
        .map(|(_, maximum)| maximum.as_slice())
        .max()
        .unwrap();
    let (boundaries, range_limit) = interpolated_boundaries(minimum, maximum, max_lanes);
    if boundaries.is_empty() {
        return DeterministicRangePlan {
            ranges: vec![KeyRange {
                lower: None,
                upper: None,
            }],
            range_limit,
        };
    }
    let mut ranges = Vec::with_capacity(boundaries.len() + 1);
    let mut lower = None;
    for boundary in boundaries {
        ranges.push(KeyRange {
            lower,
            upper: Some(boundary.clone()),
        });
        lower = Some(boundary);
    }
    ranges.push(KeyRange { lower, upper: None });
    DeterministicRangePlan {
        ranges,
        range_limit,
    }
}

/// Winner stream for one path-key stripe. Input runs are newest first, which
/// remains the final tie-break after document version exactly as in serial
/// compaction. Ordinals are intentionally left untouched for ordered assembly.
pub(crate) struct PathWinnerCursor<'a, D, E> {
    cursors: Vec<crate::segment::PathRunCursor<'a, D>>,
    current: Vec<Option<crate::segment::PathChange>>,
    executor: E,
    progress: CompactionProgress,
}

impl<'a, D, E> PathWinnerCursor<'a, D, E>
where
    D: crate::IndexDirectoryRead,
    E: CompactionExecutor,
{
    pub(crate) async fn open(
        runs: &'a [D],
        roots: &[BlockDescriptor],
        range: KeyRange,
        executor: E,
        progress: CompactionProgress,
    ) -> Result<Self, IndexError> {
        if runs.len() != roots.len() {
            return Err(IndexError::InvalidDefinition(
                "compaction path roots must match input runs".into(),
            ));
        }
        let mut cursors = Vec::with_capacity(runs.len());
        for (run, root) in runs.iter().zip(roots) {
            cursors.push(crate::segment::PathRunCursor::in_range(
                run,
                root.clone(),
                range.clone(),
            ));
        }
        let mut current = Vec::with_capacity(cursors.len());
        for cursor in &mut cursors {
            let row = cursor.next_parallel(&executor, &progress).await?;
            current.push(row);
        }
        Ok(Self {
            cursors,
            current,
            executor,
            progress,
        })
    }

    pub(crate) async fn next(
        &mut self,
    ) -> Result<Option<(usize, crate::segment::PathChange)>, IndexError> {
        let Some(path) = self
            .current
            .iter()
            .flatten()
            .map(|row| row.document.path.as_str())
            .min()
            .map(str::to_owned)
        else {
            return Ok(None);
        };
        let mut winner = None::<(usize, crate::segment::PathChange)>;
        for (run_index, row) in self.current.iter().enumerate() {
            let Some(row) = row.as_ref().filter(|row| row.document.path == path) else {
                continue;
            };
            if winner.as_ref().is_none_or(|(current_index, current)| {
                row.document.version > current.document.version
                    || (row.document.version == current.document.version
                        && run_index < *current_index)
            }) {
                winner = Some((run_index, row.clone()));
            }
        }
        for (run_index, row) in self.current.iter_mut().enumerate() {
            if row.as_ref().is_some_and(|row| row.document.path == path) {
                let next = self.cursors[run_index]
                    .next_parallel(&self.executor, &self.progress)
                    .await?;
                *row = next;
            }
        }
        Ok(winner)
    }
}

pub(crate) type LaneProducer<T> =
    Box<dyn FnOnce(LaneSender<T>) -> CompactionTaskFuture + Send + 'static>;

pub(crate) type LaneResultFuture<T> =
    Pin<Box<dyn Future<Output = Result<T, IndexError>> + Send + 'static>>;
pub(crate) type LaneResultProducer<T> = Box<dyn FnOnce() -> LaneResultFuture<T> + Send + 'static>;

/// Collect exactly one small result from each concurrent lane in lane order.
pub(crate) async fn collect_ordered_lanes<E, T>(
    executor: &E,
    producers: Vec<LaneResultProducer<T>>,
    progress: &CompactionProgress,
) -> Result<Vec<T>, IndexError>
where
    E: CompactionExecutor,
    T: Send + 'static,
{
    let producers = producers
        .into_iter()
        .map(|producer| {
            Box::new(move |sender: LaneSender<T>| {
                Box::pin(async move {
                    sender.send(producer().await?).await?;
                    Ok(())
                }) as CompactionTaskFuture
            }) as LaneProducer<T>
        })
        .collect();
    let mut results = Vec::new();
    run_ordered_lanes(
        executor,
        producers,
        progress,
        &mut results,
        |results, value| {
            Box::pin(async move {
                results.push(value);
                Ok(())
            })
        },
    )
    .await?;
    Ok(results)
}

/// Run bounded producers concurrently and drain their rows strictly by range.
/// Later ranges may prepare one row while the canonical writer drains an
/// earlier range; channel capacity is fixed at one and charged per lane.
pub(crate) async fn run_ordered_lanes<E, T, State, Consume>(
    executor: &E,
    producers: Vec<LaneProducer<T>>,
    progress: &CompactionProgress,
    state: &mut State,
    mut consume: Consume,
) -> Result<(), IndexError>
where
    E: CompactionExecutor,
    T: Send + 'static,
    Consume: for<'a> FnMut(
        &'a mut State,
        T,
    )
        -> Pin<Box<dyn Future<Output = Result<(), IndexError>> + Send + 'a>>,
{
    if producers.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "parallel compaction requires at least one range".into(),
        ));
    }
    progress.add_ranges(producers.len())?;
    let mut receivers = Vec::with_capacity(producers.len());
    let mut tasks = Vec::with_capacity(producers.len());
    for producer in producers {
        let (sender, receiver) = lane_channel(1);
        let task_sender = sender.clone();
        let pending_range = progress.pending_range();
        let task = Box::pin(async move {
            let active = pending_range.start();
            match producer(task_sender).await {
                Ok(()) => {
                    sender.finish(None);
                    active.complete();
                }
                Err(error) => sender.finish(Some(error)),
            }
            Ok(())
        });
        receivers.push(receiver);
        tasks.push(executor.spawn_io(task));
    }

    let mut consume_result = Ok(());
    'ranges: for receiver in &mut receivers {
        loop {
            match receiver.recv().await {
                Ok(Some(row)) => {
                    if let Err(error) = consume(state, row).await {
                        consume_result = Err(error);
                        break 'ranges;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    consume_result = Err(error);
                    break 'ranges;
                }
            }
        }
    }
    if consume_result.is_err() {
        for task in &tasks {
            task.abort();
        }
    }
    drop(receivers);
    let mut task_result = Ok(());
    for task in tasks {
        if let Err(error) = task.await
            && task_result.is_ok()
        {
            task_result = Err(error);
        }
    }
    consume_result.and(task_result)
}

pub(crate) struct LaneSender<T> {
    inner: Arc<LaneChannel<T>>,
}

impl<T> Clone for LaneSender<T> {
    fn clone(&self) -> Self {
        {
            let mut state = lock_channel(&self.inner);
            state.sender_count = state.sender_count.saturating_add(1);
        }
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for LaneSender<T> {
    fn drop(&mut self) {
        let mut state = lock_channel(&self.inner);
        state.sender_count = state.sender_count.saturating_sub(1);
        if state.sender_count == 0 && !state.done {
            state.done = true;
            state.error = Some(IndexError::Io(
                "ordered compaction lane ended without finishing".into(),
            ));
            if let Some(waker) = state.receiver_waker.take() {
                waker.wake();
            }
        }
    }
}

impl<T> LaneSender<T> {
    pub(crate) async fn send(&self, value: T) -> Result<(), IndexError> {
        let mut value = Some(value);
        poll_fn(|cx| {
            let mut state = lock_channel(&self.inner);
            if state.receiver_closed {
                return Poll::Ready(Err(IndexError::Io(
                    "ordered compaction lane receiver closed".into(),
                )));
            }
            if state.done {
                return Poll::Ready(Err(IndexError::Io(
                    "ordered compaction lane already finished".into(),
                )));
            }
            if state.queue.len() < self.inner.capacity {
                state.queue.push_back(value.take().unwrap());
                if let Some(waker) = state.receiver_waker.take() {
                    waker.wake();
                }
                Poll::Ready(Ok(()))
            } else {
                replace_waker(&mut state.sender_waker, cx.waker());
                Poll::Pending
            }
        })
        .await
    }

    fn finish(&self, error: Option<IndexError>) {
        let mut state = lock_channel(&self.inner);
        state.done = true;
        state.error = error;
        if let Some(waker) = state.receiver_waker.take() {
            waker.wake();
        }
    }
}

struct LaneReceiver<T> {
    inner: Arc<LaneChannel<T>>,
}

impl<T> LaneReceiver<T> {
    async fn recv(&mut self) -> Result<Option<T>, IndexError> {
        poll_fn(|cx| {
            let mut state = lock_channel(&self.inner);
            if let Some(value) = state.queue.pop_front() {
                if let Some(waker) = state.sender_waker.take() {
                    waker.wake();
                }
                return Poll::Ready(Ok(Some(value)));
            }
            if state.done {
                return Poll::Ready(match state.error.take() {
                    Some(error) => Err(error),
                    None => Ok(None),
                });
            }
            replace_waker(&mut state.receiver_waker, cx.waker());
            Poll::Pending
        })
        .await
    }
}

impl<T> Drop for LaneReceiver<T> {
    fn drop(&mut self) {
        let mut state = lock_channel(&self.inner);
        state.receiver_closed = true;
        state.queue.clear();
        if let Some(waker) = state.sender_waker.take() {
            waker.wake();
        }
    }
}

struct LaneChannel<T> {
    capacity: usize,
    state: Mutex<LaneChannelState<T>>,
}

struct LaneChannelState<T> {
    queue: VecDeque<T>,
    sender_count: usize,
    done: bool,
    error: Option<IndexError>,
    receiver_closed: bool,
    sender_waker: Option<Waker>,
    receiver_waker: Option<Waker>,
}

fn lane_channel<T>(capacity: usize) -> (LaneSender<T>, LaneReceiver<T>) {
    let inner = Arc::new(LaneChannel {
        capacity: capacity.max(1),
        state: Mutex::new(LaneChannelState {
            queue: VecDeque::new(),
            sender_count: 1,
            done: false,
            error: None,
            receiver_closed: false,
            sender_waker: None,
            receiver_waker: None,
        }),
    });
    (
        LaneSender {
            inner: inner.clone(),
        },
        LaneReceiver { inner },
    )
}

fn lock_channel<T>(channel: &LaneChannel<T>) -> std::sync::MutexGuard<'_, LaneChannelState<T>> {
    channel
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn replace_waker(slot: &mut Option<Waker>, waker: &Waker) {
    if slot
        .as_ref()
        .is_none_or(|current| !current.will_wake(waker))
    {
        *slot = Some(waker.clone());
    }
}

const INTERPOLATION_BYTES: usize = 16;

fn interpolated_boundaries(minimum: &[u8], maximum: &[u8], lanes: usize) -> (Vec<Vec<u8>>, usize) {
    if minimum >= maximum {
        return (Vec::new(), 1);
    }
    let common = minimum
        .iter()
        .zip(maximum)
        .take_while(|(left, right)| left == right)
        .count();
    let mut left = [0_u8; INTERPOLATION_BYTES];
    let mut right = [0_u8; INTERPOLATION_BYTES];
    for (target, source) in left.iter_mut().zip(&minimum[common..]) {
        *target = *source;
    }
    for (target, source) in right.iter_mut().zip(&maximum[common..]) {
        *target = *source;
    }
    let left = u128::from_be_bytes(left);
    let right = u128::from_be_bytes(right);
    let width = right.saturating_sub(left);
    if width < 2 {
        return (Vec::new(), 1);
    }
    let range_limit = usize::try_from(width).unwrap_or(usize::MAX);
    let lanes = lanes.max(1).min(range_limit);
    let mut boundaries = Vec::with_capacity(lanes.saturating_sub(1));
    for lane in 1..lanes {
        let lane = lane as u128;
        let lanes = lanes as u128;
        let offset = (width / lanes)
            .checked_mul(lane)
            .and_then(|base| base.checked_add((width % lanes) * lane / lanes))
            .unwrap_or(width);
        let value = left.saturating_add(offset);
        let mut boundary = minimum[..common].to_vec();
        boundary.extend_from_slice(&value.to_be_bytes());
        if boundary.as_slice() > minimum
            && boundary.as_slice() < maximum
            && boundaries.last().is_none_or(|last| last < &boundary)
        {
            boundaries.push(boundary);
        }
    }
    (boundaries, range_limit)
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::task::{Context, Poll};

    use super::*;

    pub(crate) struct TokioTask(tokio::task::JoinHandle<Result<(), IndexError>>);

    impl Future for TokioTask {
        type Output = Result<(), IndexError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match Pin::new(&mut self.0).poll(cx) {
                Poll::Ready(Ok(result)) => Poll::Ready(result),
                Poll::Ready(Err(error)) => Poll::Ready(Err(IndexError::Io(format!(
                    "test compaction lane task failed: {error}"
                )))),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl CompactionTaskHandle for TokioTask {
        fn abort(&self) {
            self.0.abort();
        }
    }

    #[derive(Clone, Default)]
    pub(crate) struct TokioExecutor {
        fail_cpu: bool,
    }

    impl TokioExecutor {
        pub(crate) fn failing_cpu() -> Self {
            Self { fail_cpu: true }
        }
    }

    impl CompactionExecutor for TokioExecutor {
        type Task = TokioTask;

        fn spawn_io(&self, task: CompactionTaskFuture) -> Self::Task {
            TokioTask(tokio::spawn(task))
        }

        async fn run_cpu<T, F>(&self, work: F) -> Result<T, IndexError>
        where
            T: Send + 'static,
            F: FnOnce() -> Result<T, IndexError> + Send + 'static,
        {
            if self.fail_cpu {
                Err(IndexError::Io("injected compaction CPU failure".into()))
            } else {
                work()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::TokioExecutor;
    use super::*;
    use crate::{ComponentCodec, IndexKind};

    fn root(minimum: &[u8], maximum: &[u8]) -> BlockDescriptor {
        BlockDescriptor {
            kind: IndexKind::Path,
            component_tag: 1,
            codec: ComponentCodec::FixedRows,
            routing_height: 1,
            minimum_key: minimum.to_vec(),
            maximum_key: maximum.to_vec(),
            element_count: 100,
            encoded_bytes: 100,
            hash: [7; 32],
            pack_id: 0,
            pack_offset: 0,
        }
    }

    #[test]
    fn budget_caps_lanes_by_workers_and_complete_workspace() {
        let shared = COMPACTION_SHARED_WORKSPACE_BYTES as u64;
        let lane = COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES as u64;
        assert_eq!(
            CompactionParallelism::for_budget(8, 6, shared + lane * 2)
                .unwrap()
                .max_lanes(),
            3
        );
        assert!(CompactionParallelism::for_budget(1, 1, shared - 1).is_err());
        assert!(CompactionParallelism::for_budget(0, 1, shared).is_err());
    }

    #[test]
    fn parallel_compaction_fan_in_matches_the_resident_set_proof() {
        assert!(validate_parallel_compaction_fan_in(0).is_err());
        assert!(validate_parallel_compaction_fan_in(1).is_ok());
        assert!(validate_parallel_compaction_fan_in(4).is_ok());
        assert!(matches!(
            validate_parallel_compaction_fan_in(5),
            Err(IndexError::ResourceLimit {
                needed: 5,
                limit: 4
            })
        ));
    }

    #[test]
    fn every_phase_fits_the_complete_lane_workspace() {
        assert!(ENUMERATED_COMPACTION_METADATA_BYTES <= COMPACTION_METADATA_WORKSPACE_BYTES);
        assert!(SOURCE_PHASE_WORKSPACE_BYTES <= COMPLETE_COMPACTION_LANE_WORKSPACE_BYTES);
        assert!(ROUTED_PHASE_WORKSPACE_BYTES <= COMPLETE_COMPACTION_LANE_WORKSPACE_BYTES);
        assert!(TEXT_PHASE_WORKSPACE_BYTES <= COMPLETE_COMPACTION_LANE_WORKSPACE_BYTES);
        assert!(TEXT_SELECTION_PHASE_WORKSPACE_BYTES <= COMPLETE_COMPACTION_LANE_WORKSPACE_BYTES);
        assert!(EXTERNAL_SORT_PHASE_WORKSPACE_BYTES <= COMPLETE_COMPACTION_LANE_WORKSPACE_BYTES);
        assert_eq!(
            MAX_COMPACTION_PHASE_WORKSPACE_BYTES,
            SOURCE_PHASE_WORKSPACE_BYTES
        );
        assert_eq!(
            COMPACTION_SHARED_WORKSPACE_BYTES,
            COMPLETE_COMPACTION_LANE_WORKSPACE_BYTES
        );
        assert_eq!(
            COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES,
            COMPLETE_COMPACTION_LANE_WORKSPACE_BYTES
        );
    }

    #[test]
    fn default_budget_admits_four_complete_lanes_at_the_exact_boundary() {
        let default_budget = 256 * 1024 * 1024;
        let admitted = CompactionParallelism::for_budget(4, 4, default_budget).unwrap();
        assert_eq!(admitted.max_lanes(), 4);
        let required = admitted.admitted_bytes().unwrap();
        assert_eq!(required, default_budget as usize);
        assert_eq!(
            CompactionParallelism::for_budget(4, 4, required as u64)
                .unwrap()
                .max_lanes(),
            4
        );
        assert_eq!(
            CompactionParallelism::for_budget(4, 4, required as u64 - 1)
                .unwrap()
                .max_lanes(),
            3
        );
    }

    #[test]
    fn ranges_are_stable_disjoint_and_cover_observed_keys() {
        let roots = [
            root(b"records/000000", b"records/999999"),
            root(b"records/100000", b"records/800000"),
        ];
        let first = deterministic_key_ranges(roots.clone(), 4);
        let second = deterministic_key_ranges(roots, 4);
        assert_eq!(first, second);
        assert_eq!(first.len(), 4);
        for pair in first.windows(2) {
            assert_eq!(pair[0].upper, pair[1].lower);
        }
        for key in [
            b"records/000000".as_slice(),
            b"records/555555",
            b"records/999999",
        ] {
            assert_eq!(first.iter().filter(|range| range.contains(key)).count(), 1);
        }
    }

    #[test]
    fn routed_and_delimited_plans_never_split_logical_groups() {
        let mut routed = root(b"alpha", b"omega");
        routed.minimum_key.extend_from_slice(&[0; 12]);
        routed.maximum_key.extend_from_slice(&[u8::MAX; 12]);
        let routed_plan = deterministic_suffix_key_range_plan([routed], 12, 4).unwrap();
        assert_eq!(routed_plan.ranges.len(), 4);
        assert!(routed_plan.range_limit > routed_plan.ranges.len());
        for primary in [b"alpha".as_slice(), b"middle", b"omega"] {
            assert_eq!(
                routed_plan
                    .ranges
                    .iter()
                    .filter(|range| range.contains(primary))
                    .count(),
                1
            );
        }

        let text = root(b"apple\0\0\0", b"zebra\0\xff\xff");
        let text_plan = deterministic_delimited_key_range_plan([text], 0, 4).unwrap();
        assert_eq!(text_plan.ranges.len(), 4);
        for term in [b"apple".as_slice(), b"middle", b"zebra"] {
            assert_eq!(
                text_plan
                    .ranges
                    .iter()
                    .filter(|range| range.contains(term))
                    .count(),
                1
            );
        }
    }

    #[test]
    fn progress_snapshots_aggregate_without_per_record_events() {
        let progress = CompactionProgress::default();
        progress.add_ranges(2).unwrap();
        progress.record_range_limit(9).unwrap();
        let first = progress.start_range();
        progress.record_input(7, 101, 2);
        progress.record_output(5, 89, 1);
        progress.record_sort_chunk(64).unwrap();
        progress.record_sort_chunk(32).unwrap();
        progress.record_sort_merge_pass();
        assert_eq!(
            progress.snapshot(),
            CompactionProgressSnapshot {
                ranges_total: 2,
                ranges_completed: 0,
                input_records: 7,
                input_bytes: 101,
                input_blocks: 2,
                output_records: 5,
                output_bytes: 89,
                output_blocks: 1,
                effective_lanes: 2,
                range_limit: 9,
                active_lanes: 1,
                peak_active_lanes: 1,
                waiting_lanes: 1,
                sort_chunks: 2,
                sort_merge_passes: 1,
                sort_peak_workspace_bytes: 64,
            }
        );
        let second = progress.start_range();
        assert_eq!(progress.snapshot().active_lanes, 2);
        assert_eq!(progress.snapshot().peak_active_lanes, 2);
        first.complete();
        assert_eq!(progress.snapshot().ranges_completed, 1);
        assert_eq!(progress.snapshot().active_lanes, 1);
        second.complete();
        assert_eq!(progress.snapshot().ranges_completed, 2);
        assert_eq!(progress.snapshot().active_lanes, 0);
        assert_eq!(progress.snapshot().peak_active_lanes, 2);
    }

    #[tokio::test]
    async fn ordered_lanes_drain_by_range_not_completion_order() {
        let progress = CompactionProgress::default();
        let producers: Vec<LaneProducer<u32>> = vec![
            Box::new(|sender| {
                Box::pin(async move {
                    tokio::task::yield_now().await;
                    sender.send(0).await?;
                    sender.send(1).await
                })
            }),
            Box::new(|sender| {
                Box::pin(async move {
                    sender.send(10).await?;
                    sender.send(11).await
                })
            }),
            Box::new(|sender| Box::pin(async move { sender.send(20).await })),
        ];
        let mut rows = Vec::new();
        run_ordered_lanes(
            &TokioExecutor::default(),
            producers,
            &progress,
            &mut rows,
            |rows, row| {
                Box::pin(async move {
                    rows.push(row);
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
        assert_eq!(rows, [0, 1, 10, 11, 20]);
        assert_eq!(progress.snapshot().ranges_completed, 3);
        assert_eq!(progress.snapshot().active_lanes, 0);
    }

    #[tokio::test]
    async fn lane_results_are_collected_in_range_order() {
        let progress = CompactionProgress::default();
        let producers: Vec<LaneResultProducer<u32>> = vec![
            Box::new(|| {
                Box::pin(async move {
                    tokio::task::yield_now().await;
                    Ok(1)
                })
            }),
            Box::new(|| Box::pin(async move { Ok(2) })),
            Box::new(|| Box::pin(async move { Ok(3) })),
        ];
        let results = collect_ordered_lanes(&TokioExecutor::default(), producers, &progress)
            .await
            .unwrap();
        assert_eq!(results, [1, 2, 3]);
        assert_eq!(progress.snapshot().ranges_completed, 3);
        assert_eq!(progress.snapshot().active_lanes, 0);
    }

    #[tokio::test]
    async fn lane_result_producers_make_concurrent_progress() {
        let progress = CompactionProgress::default();
        let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let producers = (0_u32..3)
            .map(|value| {
                let started = started.clone();
                Box::new(move || {
                    Box::pin(async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        for _ in 0..10_000 {
                            if started.load(Ordering::SeqCst) == 3 {
                                return Ok(value);
                            }
                            tokio::task::yield_now().await;
                        }
                        Err(IndexError::Io(
                            "lane-local producers did not make concurrent progress".into(),
                        ))
                    }) as LaneResultFuture<u32>
                }) as LaneResultProducer<u32>
            })
            .collect();
        let results = collect_ordered_lanes(&TokioExecutor::default(), producers, &progress)
            .await
            .unwrap();

        assert_eq!(results, [0, 1, 2]);
        assert_eq!(progress.snapshot().ranges_completed, 3);
    }

    #[tokio::test]
    async fn panicked_lane_closes_and_wakes_ordered_receiver() {
        let progress = CompactionProgress::default();
        let producers: Vec<LaneProducer<u32>> = vec![Box::new(|_sender| {
            Box::pin(async move {
                panic!("injected compaction lane panic");
                #[allow(unreachable_code)]
                Ok(())
            })
        })];
        let mut rows = Vec::new();
        let error = run_ordered_lanes(
            &TokioExecutor::default(),
            producers,
            &progress,
            &mut rows,
            |rows, row| {
                Box::pin(async move {
                    rows.push(row);
                    Ok(())
                })
            },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("ended without finishing"));
        assert_eq!(progress.snapshot().active_lanes, 0);
    }
}
