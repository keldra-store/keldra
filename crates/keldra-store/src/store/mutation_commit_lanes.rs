//! Stable conflict lanes for independently prepared object mutations.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{
    Mutex, MutexGuard, Notify, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard,
    RwLock, RwLockWriteGuard, Semaphore, mpsc, oneshot,
};

use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions};

use super::mutation_conflict_scheduler::{
    MutationConflictAcquisition, MutationConflictGuard, MutationConflictScheduler,
};
use super::{
    CF_LOCAL_INVALIDATIONS, CF_METADATA, LOCAL_INVALIDATION_STATUS_KEY,
    MUTATION_RECEIPT_STATUS_KEY, MutationReceiptStatus, PreparedOperation, Store,
    VERSION_HIGH_WATERMARK_KEY, encode_mutation_receipt_status, receipt_key, storage_error,
};
use crate::DefinitionMutationIntent;
use crate::watch::{
    encode_local_change, encode_watch_journal_status, invalidation_key, invalidation_record_bytes,
};
use crate::{LocalChange, MutationError, VersionId, WatchJournalStatus};

pub(super) const LANE_FRONTIER_KEY: &[u8] = b"mutation_lane_frontier_current_v1";
pub(super) const LANE_COMPLETION_PREFIX: &[u8] = b"mutation_lane_completion_v1/";
const LANE_COMPLETION_FORMAT: u8 = 1;
const LANE_COMPLETION_BYTES: usize = 1 + 8 * 7 + 1 + 8 + 3;
const LANE_PROJECTION_QUEUE_CAPACITY: usize = 1_024;

#[path = "mutation_commit_lane_codec.rs"]
mod completion_codec;
#[cfg(test)]
use completion_codec::lane_completion_key;
use completion_codec::lane_completion_ticket_from_key;

struct LaneProjectionRequest {
    store: Store,
    reply: oneshot::Sender<Result<LaneProjectionMetrics, LaneProjectionFailure>>,
}

#[derive(Clone)]
enum LaneProjectionFailure {
    Write(MutationError),
    Invariant(MutationError),
}

impl LaneProjectionFailure {
    fn retryable_write_kind(kind: rocksdb::ErrorKind) -> bool {
        matches!(
            kind,
            rocksdb::ErrorKind::IOError
                | rocksdb::ErrorKind::TimedOut
                | rocksdb::ErrorKind::Aborted
                | rocksdb::ErrorKind::Busy
                | rocksdb::ErrorKind::TryAgain
        )
    }

    fn from_write_error(error: rocksdb::Error) -> Self {
        let retryable = Self::retryable_write_kind(error.kind());
        let error = storage_error(error);
        if retryable {
            Self::Write(error)
        } else {
            Self::Invariant(error)
        }
    }

    fn into_error(self) -> MutationError {
        match self {
            Self::Write(error) | Self::Invariant(error) => error,
        }
    }
}

impl From<MutationError> for LaneProjectionFailure {
    fn from(error: MutationError) -> Self {
        Self::Invariant(error)
    }
}

#[derive(Clone)]
pub(super) struct MutationCommitLanes {
    fence: Arc<RwLock<()>>,
    conflicts: MutationConflictScheduler,
    physical_slots: Arc<Semaphore>,
    physical_slots_active: Arc<AtomicUsize>,
    physical_slots_peak: Arc<AtomicUsize>,
    physical_slot_count: usize,
    sequence: Arc<Mutex<Option<LaneRuntime>>>,
    projection: Arc<Mutex<()>>,
    projection_tx: mpsc::Sender<LaneProjectionRequest>,
    projection_rx: Arc<Mutex<Option<mpsc::Receiver<LaneProjectionRequest>>>>,
    projection_retry_needed: Arc<AtomicBool>,
    frontier_notify: Arc<Notify>,
    authorities_stale: Arc<AtomicBool>,
    #[cfg(test)]
    fail_next_projection: Arc<AtomicBool>,
    #[cfg(test)]
    fail_next_completion_disambiguation: Arc<AtomicBool>,
    #[cfg(test)]
    pub(super) pause_next_projection: Arc<AtomicBool>,
    #[cfg(test)]
    pub(super) projection_write_completed: Arc<Semaphore>,
    #[cfg(test)]
    pub(super) projection_publish_continue: Arc<Semaphore>,
    #[cfg(test)]
    pause_next_evaluation: Arc<AtomicBool>,
    #[cfg(test)]
    evaluation_snapshot_taken: Arc<Semaphore>,
    #[cfg(test)]
    evaluation_continue: Arc<Semaphore>,
    #[cfg(test)]
    fence_waiters: Arc<AtomicUsize>,
    #[cfg(test)]
    pause_next_cancellation_safe_settlement: Arc<AtomicBool>,
    #[cfg(test)]
    cancellation_safe_settlement_started: Arc<Semaphore>,
    #[cfg(test)]
    cancellation_safe_settlement_continue: Arc<Semaphore>,
}

pub(super) struct MutationLaneGuard {
    _fence: Arc<OwnedRwLockReadGuard<()>>,
    _conflicts: MutationConflictGuard,
    physical_slot: Option<tokio::sync::OwnedSemaphorePermit>,
    physical_slots_active: Arc<AtomicUsize>,
    physical_slots_peak_since_start: Arc<AtomicUsize>,
    conflict_wait: Duration,
    physical_slot_wait: Duration,
    physical_slots_active_at_acquire: usize,
    physical_slots_peak_since_start_at_acquire: usize,
    physical_slot_count: usize,
}

pub(super) struct MutationLaneRegistration {
    lanes: MutationCommitLanes,
    fence: OwnedRwLockReadGuard<()>,
    conflicts: MutationConflictAcquisition,
    conflict_started: Instant,
}

pub(super) struct MutationLaneAdmission {
    lanes: MutationCommitLanes,
    fence: OwnedRwLockReadGuard<()>,
    conflicts: MutationConflictGuard,
    conflict_wait: Duration,
}

impl Drop for MutationLaneGuard {
    fn drop(&mut self) {
        self.release_physical_slot();
    }
}

impl MutationLaneGuard {
    /// Shares the existing fence with independently owned settlement. This
    /// does not acquire another reader behind a queued exclusive writer.
    pub(super) fn settlement_fence_lease(&self) -> Arc<OwnedRwLockReadGuard<()>> {
        self._fence.clone()
    }

    /// Releases only the bounded physical-commit admission slot. The lane
    /// fence and conflict guards remain held until this guard is dropped, so
    /// ordered settlement cannot overlap a legacy writer or a conflicting
    /// mutation even after the primary RocksDB write has finished.
    pub(super) fn release_physical_slot(&mut self) {
        if self.physical_slot.take().is_some() {
            self.physical_slots_active.fetch_sub(1, Ordering::AcqRel);
        }
    }

    pub(super) fn conflict_wait(&self) -> Duration {
        self.conflict_wait
    }

    pub(super) fn physical_slot_wait(&self) -> Duration {
        self.physical_slot_wait
    }

    pub(super) fn physical_slots_active_at_acquire(&self) -> usize {
        self.physical_slots_active_at_acquire
    }

    pub(super) fn physical_slots_active(&self) -> usize {
        self.physical_slots_active.load(Ordering::Acquire)
    }

    pub(super) fn physical_slots_peak_since_start_at_acquire(&self) -> usize {
        self.physical_slots_peak_since_start_at_acquire
    }

    pub(super) fn physical_slots_peak_since_start(&self) -> usize {
        self.physical_slots_peak_since_start.load(Ordering::Acquire)
    }

    pub(super) fn physical_slot_count(&self) -> usize {
        self.physical_slot_count
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct LaneSettlementMetrics {
    pub(super) completion_sequence_wait: Duration,
    pub(super) projection_write: Duration,
    pub(super) ordered_frontier_wait: Duration,
    /// Number of tickets between this arrival and the next projected ticket.
    pub(super) completion_reorder_depth: usize,
    /// This arrival's ticket minus the already projected frontier.
    pub(super) completion_ticket_lag: u64,
    pub(super) contiguous_projection_completions: usize,
}

#[derive(Clone, Copy, Default)]
pub(super) struct LaneProjectionMetrics {
    pub(super) write: Duration,
    pub(super) completions: usize,
}

struct LaneProjectionPlan {
    base_projected_ticket: u64,
    prospective: LaneRuntime,
    completions: Vec<(u64, LaneCompletionState)>,
    batch: WriteBatch,
    requires_sync: bool,
    inline_reference_safe_through: Option<u64>,
}

enum ExclusiveFence<'a> {
    Owned { _guard: OwnedRwLockWriteGuard<()> },
    Borrowed { _guard: RwLockWriteGuard<'a, ()> },
}

enum ExclusiveProjection<'a> {
    Owned { _guard: OwnedMutexGuard<()> },
    Borrowed { _guard: MutexGuard<'a, ()> },
}

pub(super) struct ExclusiveMutationGuard<'a> {
    // Release projection before the lane fence, after Drop marks authority stale.
    _projection: ExclusiveProjection<'a>,
    _fence: ExclusiveFence<'a>,
    authorities_stale: Arc<AtomicBool>,
}

impl Drop for ExclusiveMutationGuard<'_> {
    fn drop(&mut self) {
        self.authorities_stale.store(true, Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LaneCompletion {
    pub(super) ticket: u64,
    pub(super) first_offset: u64,
    pub(super) last_offset: u64,
    pub(super) journal_entries: u64,
    pub(super) journal_bytes: u64,
    pub(super) receipt_entries: u64,
    pub(super) receipt_bytes: u64,
    pub(super) high_version: Option<VersionId>,
    pub(super) reference_cursor_advanced: bool,
    pub(super) inline_reference_safe: bool,
    pub(super) visibility_settled: bool,
}

/// A contiguous lane completion whose durable authorities were folded into the
/// primary mutation batch. Those authority snapshots also disambiguate an
/// ambiguous RocksDB result without adding a completion marker to the common
/// one-WAL path.
pub(super) struct InlineLaneProjection {
    base_projected_ticket: u64,
    projected_ticket: u64,
    projected_watch: WatchJournalStatus,
    projected_receipts: MutationReceiptStatus,
    projected_high_version: Option<VersionId>,
    inline_reference_safe_through: Option<u64>,
}

#[derive(Clone)]
pub(super) struct LaneRuntime {
    pub(super) next_ticket: u64,
    pub(super) projected_ticket: u64,
    pub(super) reserved_watch: WatchJournalStatus,
    pub(super) reserved_receipts: MutationReceiptStatus,
    pub(super) projected_watch: WatchJournalStatus,
    pub(super) projected_receipts: MutationReceiptStatus,
    pub(super) projected_high_version: Option<VersionId>,
    /// True when the durable reference cursor may advance through every
    /// reserved source entry.
    reserved_reference_cursor_safe: bool,
    /// True when the process-local retention frontier may advance through
    /// every reserved source entry without a derived-consumer checkpoint.
    reserved_inline_reference_safe: bool,
    pub(super) completions: BTreeMap<u64, LaneCompletionState>,
    /// Quorum-proven source positions not yet consumed by the contiguous
    /// visibility frontier. These are process-local hints; recovery re-proves
    /// them from authoritative replica evidence after restart.
    pub(super) visibility_proofs: BTreeSet<u64>,
    /// Recovery may prove an entire contiguous prefix without materializing
    /// one in-memory set entry per journal position.
    pub(super) visibility_prefix_proof: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LaneCompletionState {
    Committed(LaneCompletion),
    Abandoned(LaneCompletion),
}

impl MutationCommitLanes {
    pub(super) fn new(commit_lanes: usize) -> Self {
        let (projection_tx, projection_rx) = mpsc::channel(LANE_PROJECTION_QUEUE_CAPACITY);
        Self {
            fence: Arc::new(RwLock::new(())),
            conflicts: MutationConflictScheduler::default(),
            physical_slots: Arc::new(Semaphore::new(commit_lanes)),
            physical_slots_active: Arc::new(AtomicUsize::new(0)),
            physical_slots_peak: Arc::new(AtomicUsize::new(0)),
            physical_slot_count: commit_lanes,
            sequence: Arc::new(Mutex::new(None)),
            projection: Arc::new(Mutex::new(())),
            projection_tx,
            projection_rx: Arc::new(Mutex::new(Some(projection_rx))),
            projection_retry_needed: Arc::new(AtomicBool::new(false)),
            frontier_notify: Arc::new(Notify::new()),
            authorities_stale: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_next_projection: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_next_completion_disambiguation: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            pause_next_projection: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            projection_write_completed: Arc::new(Semaphore::new(0)),
            #[cfg(test)]
            projection_publish_continue: Arc::new(Semaphore::new(0)),
            #[cfg(test)]
            pause_next_evaluation: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            evaluation_snapshot_taken: Arc::new(Semaphore::new(0)),
            #[cfg(test)]
            evaluation_continue: Arc::new(Semaphore::new(0)),
            #[cfg(test)]
            fence_waiters: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            pause_next_cancellation_safe_settlement: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            cancellation_safe_settlement_started: Arc::new(Semaphore::new(0)),
            #[cfg(test)]
            cancellation_safe_settlement_continue: Arc::new(Semaphore::new(0)),
        }
    }

    pub(super) async fn acquire_fence(&self) -> OwnedRwLockReadGuard<()> {
        #[cfg(test)]
        self.fence_waiters.fetch_add(1, Ordering::AcqRel);
        let guard = self.fence.clone().read_owned().await;
        #[cfg(test)]
        self.fence_waiters.fetch_sub(1, Ordering::AcqRel);
        guard
    }

    pub(super) async fn acquire_fence_measured(&self) -> (OwnedRwLockReadGuard<()>, Duration) {
        let started = Instant::now();
        let fence = self.acquire_fence().await;
        (fence, started.elapsed())
    }

    pub(super) async fn acquire_with_fence(
        &self,
        fence: OwnedRwLockReadGuard<()>,
        resources: impl IntoIterator<Item = Vec<u8>>,
    ) -> MutationLaneGuard {
        self.register_with_fence(fence, resources).acquire().await
    }

    pub(super) fn register_with_fence(
        &self,
        fence: OwnedRwLockReadGuard<()>,
        resources: impl IntoIterator<Item = Vec<u8>>,
    ) -> MutationLaneRegistration {
        MutationLaneRegistration {
            lanes: self.clone(),
            fence,
            conflicts: self.conflicts.register(resources),
            conflict_started: Instant::now(),
        }
    }

    async fn finish_registered_acquisition(
        &self,
        fence: OwnedRwLockReadGuard<()>,
        conflicts: MutationConflictGuard,
        conflict_wait: Duration,
    ) -> MutationLaneGuard {
        let physical_slot_started = Instant::now();
        let physical_slot = self
            .physical_slots
            .clone()
            .acquire_owned()
            .await
            .expect("mutation commit lane semaphore remains open");
        let physical_slot_wait = physical_slot_started.elapsed();
        let physical_slots_active_at_acquire =
            self.physical_slots_active.fetch_add(1, Ordering::AcqRel) + 1;
        self.physical_slots_peak
            .fetch_max(physical_slots_active_at_acquire, Ordering::AcqRel);
        let physical_slots_peak_since_start_at_acquire =
            self.physical_slots_peak.load(Ordering::Acquire);
        MutationLaneGuard {
            _fence: Arc::new(fence),
            _conflicts: conflicts,
            physical_slot: Some(physical_slot),
            physical_slots_active: self.physical_slots_active.clone(),
            physical_slots_peak_since_start: self.physical_slots_peak.clone(),
            conflict_wait,
            physical_slot_wait,
            physical_slots_active_at_acquire,
            physical_slots_peak_since_start_at_acquire,
            physical_slot_count: self.physical_slot_count,
        }
    }

    pub(super) async fn acquire(
        &self,
        resources: impl IntoIterator<Item = Vec<u8>>,
    ) -> MutationLaneGuard {
        let fence = self.acquire_fence().await;
        self.acquire_with_fence(fence, resources).await
    }

    pub(super) fn has_active_conflict(&self, resources: impl IntoIterator<Item = Vec<u8>>) -> bool {
        self.conflicts.has_active_conflict(resources)
    }

    pub(super) async fn acquire_exclusive(&self) -> ExclusiveMutationGuard<'static> {
        let fence = self.fence.clone().write_owned().await;
        let projection = self.projection.clone().lock_owned().await;
        ExclusiveMutationGuard {
            _projection: ExclusiveProjection::Owned { _guard: projection },
            _fence: ExclusiveFence::Owned { _guard: fence },
            authorities_stale: self.authorities_stale.clone(),
        }
    }

    pub(super) fn try_acquire_exclusive(
        &self,
    ) -> Result<ExclusiveMutationGuard<'static>, tokio::sync::TryLockError> {
        let fence = self.fence.clone().try_write_owned()?;
        let projection = self.projection.clone().try_lock_owned()?;
        Ok(ExclusiveMutationGuard {
            _projection: ExclusiveProjection::Owned { _guard: projection },
            _fence: ExclusiveFence::Owned { _guard: fence },
            authorities_stale: self.authorities_stale.clone(),
        })
    }

    pub(super) fn blocking_acquire_exclusive(&self) -> ExclusiveMutationGuard<'_> {
        let fence = self.fence.blocking_write();
        let projection = self.projection.blocking_lock();
        ExclusiveMutationGuard {
            _projection: ExclusiveProjection::Borrowed { _guard: projection },
            _fence: ExclusiveFence::Borrowed { _guard: fence },
            authorities_stale: self.authorities_stale.clone(),
        }
    }

    pub(super) async fn sequence(&self) -> tokio::sync::MutexGuard<'_, Option<LaneRuntime>> {
        self.sequence.lock().await
    }

    pub(super) fn projection_retry_needed(&self) -> bool {
        self.projection_retry_needed.load(Ordering::Acquire)
    }

    async fn take_projection_receiver(&self) -> Option<mpsc::Receiver<LaneProjectionRequest>> {
        self.projection_rx.lock().await.take()
    }

    #[cfg(test)]
    pub(super) async fn pause_lane_evaluation_after_snapshot(&self) {
        if self.pause_next_evaluation.swap(false, Ordering::AcqRel) {
            self.evaluation_snapshot_taken.add_permits(1);
            self.evaluation_continue
                .acquire()
                .await
                .expect("lane evaluation test gate remains open")
                .forget();
        }
    }

    #[cfg(test)]
    pub(super) fn pause_next_lane_evaluation(&self) {
        self.pause_next_evaluation.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) async fn wait_for_paused_lane_evaluation(&self) {
        self.evaluation_snapshot_taken
            .acquire()
            .await
            .expect("lane evaluation test gate remains open")
            .forget();
    }

    #[cfg(test)]
    pub(super) fn resume_paused_lane_evaluation(&self) {
        self.evaluation_continue.add_permits(1);
    }

    #[cfg(test)]
    pub(super) fn conflict_resources_for_test(
        &self,
        resources: impl IntoIterator<Item = Vec<u8>>,
    ) -> BTreeSet<Vec<u8>> {
        resources.into_iter().collect()
    }

    #[cfg(test)]
    pub(super) fn waiting_fence_readers(&self) -> usize {
        self.fence_waiters.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn pause_next_cancellation_safe_settlement(&self) {
        self.pause_next_cancellation_safe_settlement
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) async fn wait_for_cancellation_safe_settlement(&self) {
        self.cancellation_safe_settlement_started
            .acquire()
            .await
            .expect("cancellation-safe settlement test gate remains open")
            .forget();
    }

    #[cfg(test)]
    pub(super) fn resume_cancellation_safe_settlement(&self) {
        self.cancellation_safe_settlement_continue.add_permits(1);
    }
}

impl MutationLaneRegistration {
    pub(super) async fn acquire(self) -> MutationLaneGuard {
        self.acquire_conflicts().await.acquire_physical().await
    }

    pub(super) async fn acquire_conflicts(self) -> MutationLaneAdmission {
        let Self {
            lanes,
            fence,
            conflicts,
            conflict_started,
        } = self;
        let conflicts = conflicts.acquire().await;
        MutationLaneAdmission {
            lanes,
            fence,
            conflicts,
            conflict_wait: conflict_started.elapsed(),
        }
    }
}

impl MutationLaneAdmission {
    pub(super) async fn rediscover(self, resources: impl IntoIterator<Item = Vec<u8>>) -> Self {
        let Self {
            lanes,
            fence,
            conflicts,
            conflict_wait,
        } = self;
        let started = Instant::now();
        let conflicts = conflicts.rediscover(resources).acquire().await;
        Self {
            lanes,
            fence,
            conflicts,
            conflict_wait: conflict_wait + started.elapsed(),
        }
    }

    pub(super) async fn acquire_physical(self) -> MutationLaneGuard {
        let Self {
            lanes,
            fence,
            conflicts,
            conflict_wait,
        } = self;
        lanes
            .finish_registered_acquisition(fence, conflicts, conflict_wait)
            .await
    }
}

impl LaneRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn reserve(
        &mut self,
        watch: WatchJournalStatus,
        receipts: MutationReceiptStatus,
        high_version: Option<VersionId>,
    ) -> Result<LaneCompletion, MutationError> {
        self.reserve_with_reference_settlement(watch, receipts, high_version, true, true, true)
    }

    pub(super) fn reserve_with_reference_settlement(
        &mut self,
        watch: WatchJournalStatus,
        receipts: MutationReceiptStatus,
        high_version: Option<VersionId>,
        own_reference_cursor_safe: bool,
        own_inline_reference_safe: bool,
        visibility_settled: bool,
    ) -> Result<LaneCompletion, MutationError> {
        let ticket = self
            .next_ticket
            .checked_add(1)
            .ok_or_else(|| MutationError::Storage("mutation lane ticket is exhausted".into()))?;
        let journal_entries = watch
            .retained_entries
            .checked_sub(self.reserved_watch.retained_entries)
            .ok_or_else(|| MutationError::Storage("lane journal reservation regressed".into()))?;
        let journal_bytes = watch
            .retained_bytes
            .checked_sub(self.reserved_watch.retained_bytes)
            .ok_or_else(|| MutationError::Storage("lane journal bytes regressed".into()))?;
        let receipt_entries = receipts
            .entries
            .checked_sub(self.reserved_receipts.entries)
            .ok_or_else(|| MutationError::Storage("lane receipt reservation regressed".into()))?;
        let receipt_bytes = receipts
            .bytes
            .checked_sub(self.reserved_receipts.bytes)
            .ok_or_else(|| MutationError::Storage("lane receipt bytes regressed".into()))?;
        let first_offset = if journal_entries == 0 {
            0
        } else {
            self.reserved_watch
                .tail
                .checked_add(1)
                .ok_or_else(|| MutationError::Storage("lane source offset is exhausted".into()))?
        };
        let reference_cursor_safe =
            self.reserved_reference_cursor_safe && own_reference_cursor_safe;
        let inline_reference_safe =
            self.reserved_inline_reference_safe && own_inline_reference_safe;
        let completion = LaneCompletion {
            ticket,
            first_offset,
            last_offset: if journal_entries == 0 { 0 } else { watch.tail },
            journal_entries,
            journal_bytes,
            receipt_entries,
            receipt_bytes,
            high_version,
            reference_cursor_advanced: reference_cursor_safe,
            inline_reference_safe,
            visibility_settled,
        };
        completion.validate().map_err(MutationError::Storage)?;
        self.next_ticket = ticket;
        self.reserved_watch = watch;
        self.reserved_receipts = receipts;
        if journal_entries != 0 {
            self.reserved_reference_cursor_safe = reference_cursor_safe;
            self.reserved_inline_reference_safe = inline_reference_safe;
        }
        Ok(completion)
    }
}

impl Store {
    pub(super) fn rearm_caught_up_reference_frontiers(
        &self,
        runtime: &mut LaneRuntime,
        reference_cursor: u64,
    ) -> Result<(), MutationError> {
        if reference_cursor > runtime.reserved_watch.tail {
            return Err(MutationError::Storage(format!(
                "reference cursor {reference_cursor} is beyond reserved source-journal tail {}",
                runtime.reserved_watch.tail
            )));
        }
        if reference_cursor == runtime.reserved_watch.tail {
            runtime.reserved_reference_cursor_safe = true;
        }
        let inline_reference_safe = self
            .source_journal_reference_safe_through
            .load(Ordering::Acquire);
        if inline_reference_safe > runtime.reserved_watch.tail {
            return Err(MutationError::Storage(format!(
                "inline reference-safe cursor {inline_reference_safe} is beyond reserved source-journal tail {}",
                runtime.reserved_watch.tail
            )));
        }
        if inline_reference_safe == runtime.reserved_watch.tail {
            runtime.reserved_inline_reference_safe = true;
        }
        Ok(())
    }

    pub(super) fn refresh_stale_lane_runtime(
        &self,
        runtime: &mut LaneRuntime,
    ) -> Result<(), MutationError> {
        if !self
            .mutation_commit_lanes
            .authorities_stale
            .load(Ordering::Acquire)
        {
            return Ok(());
        }
        if runtime.next_ticket != runtime.projected_ticket
            || !runtime.completions.is_empty()
            || runtime.reserved_watch != runtime.projected_watch
            || runtime.reserved_receipts != runtime.projected_receipts
        {
            return Err(MutationError::Storage(
                "exclusive mutation authority overlapped active commit lanes".into(),
            ));
        }
        let watch = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let receipts = self.mutation_receipt_status()?;
        let high_version = self
            .db
            .get_cf(self.cf(CF_METADATA)?, VERSION_HIGH_WATERMARK_KEY)
            .map_err(storage_error)?
            .map(|encoded| serde_json::from_slice::<VersionId>(&encoded))
            .transpose()
            .map_err(storage_error)?;
        runtime.reserved_watch = watch;
        runtime.projected_watch = watch;
        runtime.reserved_receipts = receipts;
        runtime.projected_receipts = receipts;
        runtime.projected_high_version = high_version;
        // Quorum proofs are disposable recovery hints, not authority. An
        // exclusive legacy writer invalidates their snapshot; the recovery
        // worker will re-prove any still-unsettled positions.
        runtime.visibility_proofs.clear();
        runtime.visibility_prefix_proof = None;
        let reference_cursor = self
            .reference_delta_cursor(watch.source_id)
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        if reference_cursor > watch.tail {
            return Err(MutationError::Storage(format!(
                "reference cursor {reference_cursor} is beyond source-journal tail {} while refreshing mutation lanes",
                watch.tail
            )));
        }
        runtime.reserved_reference_cursor_safe = reference_cursor == watch.tail;
        let inline_reference_safe = self
            .source_journal_reference_safe_through
            .load(Ordering::Acquire);
        if inline_reference_safe > watch.tail {
            return Err(MutationError::Storage(format!(
                "inline reference-safe cursor {inline_reference_safe} is beyond source-journal tail {} while refreshing mutation lanes",
                watch.tail
            )));
        }
        runtime.reserved_inline_reference_safe = inline_reference_safe == watch.tail;
        self.mutation_commit_lanes
            .authorities_stale
            .store(false, Ordering::Release);
        Ok(())
    }

    pub(super) async fn initialize_mutation_lane_runtime(
        &self,
        existing_database: bool,
    ) -> anyhow::Result<()> {
        let metadata = self.cf(CF_METADATA)?;
        let frontier = self.db.get_cf(metadata, LANE_FRONTIER_KEY)?;
        let initialize_frontier = frontier.is_none();
        let mut projected_ticket = match frontier.as_deref() {
            Some(encoded) => u64::from_be_bytes(
                encoded
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("mutation lane frontier is malformed"))?,
            ),
            None if existing_database => {
                anyhow::bail!("existing Keldra volume has no mutation lane frontier")
            }
            None => 0,
        };
        let mut watch = self.local_watch_status()?;
        let mut receipts = self.mutation_receipt_status()?;
        let mut high_version = self
            .db
            .get_cf(metadata, VERSION_HIGH_WATERMARK_KEY)?
            .map(|encoded| serde_json::from_slice::<VersionId>(&encoded))
            .transpose()?;
        let mut batch = WriteBatch::default();
        let mut recovered = initialize_frontier;
        let iterator = self.db.iterator_cf(
            metadata,
            IteratorMode::From(LANE_COMPLETION_PREFIX, Direction::Forward),
        );
        for entry in iterator {
            let (key, encoded) = entry?;
            if !key.starts_with(LANE_COMPLETION_PREFIX) {
                break;
            }
            let key_ticket = lane_completion_ticket_from_key(&key)?;
            let completion = LaneCompletion::decode(&encoded).map_err(anyhow::Error::msg)?;
            if completion.ticket != key_ticket {
                anyhow::bail!("mutation lane completion key disagrees with its value");
            }
            if completion.ticket <= projected_ticket {
                anyhow::bail!("mutation lane completion is behind its durable frontier");
            }
            self.verify_lane_journal_range(completion)?;
            self.apply_committed_lane_completion(
                &mut batch,
                &mut watch,
                &mut receipts,
                &mut high_version,
                completion,
            )?;
            batch.delete_cf(metadata, &key);
            projected_ticket = completion.ticket;
            recovered = true;
        }
        if recovered {
            self.stage_lane_frontier_projection(
                &mut batch,
                &LaneRuntime {
                    next_ticket: projected_ticket,
                    projected_ticket,
                    reserved_watch: watch,
                    reserved_receipts: receipts,
                    projected_watch: watch,
                    projected_receipts: receipts,
                    projected_high_version: high_version,
                    reserved_reference_cursor_safe: false,
                    reserved_inline_reference_safe: false,
                    completions: BTreeMap::new(),
                    visibility_proofs: BTreeSet::new(),
                    visibility_prefix_proof: None,
                },
            )?;
            let options = WriteOptions::default();
            // Recovery projects WAL-backed completion markers. Its deletion
            // and frontier must also enter the WAL: a later inline mutation
            // can persist a newer frontier before this memtable is flushed.
            self.db.write_opt(batch, &options)?;
        }
        let reference_cursor = self.reference_delta_cursor(watch.source_id)?;
        if reference_cursor > watch.tail {
            anyhow::bail!(
                "reference cursor {reference_cursor} is beyond source-journal tail {} while initializing mutation lanes",
                watch.tail
            );
        }
        let runtime = LaneRuntime {
            next_ticket: projected_ticket,
            projected_ticket,
            reserved_watch: watch,
            reserved_receipts: receipts,
            projected_watch: watch,
            projected_receipts: receipts,
            projected_high_version: high_version,
            reserved_reference_cursor_safe: reference_cursor == watch.tail,
            reserved_inline_reference_safe: watch.retention_floor == watch.tail,
            completions: BTreeMap::new(),
            visibility_proofs: BTreeSet::new(),
            visibility_prefix_proof: None,
        };
        if let Some(version) = high_version {
            self.clock.observe(version);
        }
        let mut slot = self.mutation_commit_lanes.sequence.lock().await;
        if slot.replace(runtime).is_some() {
            anyhow::bail!("mutation lane runtime was initialized twice");
        }
        Ok(())
    }

    /// Starts the one bounded projector for this Store. Physical mutation
    /// lanes submit only a wake/reply handle; the worker drains all requests
    /// already admitted and projects the largest contiguous completion and
    /// quorum-proof prefix with one RocksDB batch.
    pub(super) async fn start_mutation_lane_projector(&self) -> anyhow::Result<()> {
        let mut receiver = self
            .mutation_commit_lanes
            .take_projection_receiver()
            .await
            .ok_or_else(|| anyhow::anyhow!("mutation lane projector was started twice"))?;
        tokio::spawn(async move {
            while let Some(first) = receiver.recv().await {
                let mut requests = Vec::with_capacity(LANE_PROJECTION_QUEUE_CAPACITY.min(64));
                requests.push(first);
                while requests.len() < LANE_PROJECTION_QUEUE_CAPACITY {
                    match receiver.try_recv() {
                        Ok(request) => requests.push(request),
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }
                let store = requests[0].store.clone();
                let result = store.project_lane_completions_classified().await;
                for (index, request) in requests.into_iter().enumerate() {
                    let response = match &result {
                        Ok(metrics) if index == 0 => Ok(*metrics),
                        Ok(_) => Ok(LaneProjectionMetrics::default()),
                        Err(error) => Err(error.clone()),
                    };
                    let _ = request.reply.send(response);
                }
            }
        });
        Ok(())
    }

    pub(super) async fn request_lane_projection(
        &self,
    ) -> Result<LaneProjectionMetrics, MutationError> {
        self.request_lane_projection_classified()
            .await
            .map_err(LaneProjectionFailure::into_error)
    }

    async fn request_lane_projection_classified(
        &self,
    ) -> Result<LaneProjectionMetrics, LaneProjectionFailure> {
        let (reply, response) = oneshot::channel();
        self.mutation_commit_lanes
            .projection_tx
            .send(LaneProjectionRequest {
                store: self.clone(),
                reply,
            })
            .await
            .map_err(|_| MutationError::Storage("mutation lane projector stopped".into()))?;
        response
            .await
            .map_err(|_| MutationError::Storage("mutation lane projector dropped a reply".into()))?
    }

    async fn request_lane_projection_retrying_writes(
        &self,
    ) -> Result<LaneProjectionMetrics, MutationError> {
        let mut failures = 0_u64;
        loop {
            match self.request_lane_projection_classified().await {
                Ok(metrics) => return Ok(metrics),
                Err(LaneProjectionFailure::Invariant(error)) => return Err(error),
                Err(LaneProjectionFailure::Write(error)) => {
                    failures = failures.saturating_add(1);
                    if failures == 1 || failures % 100 == 0 {
                        tracing::warn!(error = %error, failures,
                            "retrying physical mutation lane projection write");
                    }
                    // The independently owned settlement retains its existing
                    // fence during this wait. No new ticket is inserted.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    fn verify_lane_journal_range(&self, completion: LaneCompletion) -> anyhow::Result<()> {
        if completion.first_offset == 0 {
            return Ok(());
        }
        let journal = self.cf(CF_LOCAL_INVALIDATIONS)?;
        for offset in completion.first_offset..=completion.last_offset {
            if self.db.get_cf(journal, invalidation_key(offset))?.is_none() {
                anyhow::bail!("committed mutation lane journal range has a missing record")
            }
        }
        Ok(())
    }

    pub(super) fn stage_lane_completion(
        &self,
        batch: &mut WriteBatch,
        completion: LaneCompletion,
    ) -> Result<(), MutationError> {
        batch.put_cf(self.cf(CF_METADATA)?, completion.key(), completion.encode());
        Ok(())
    }

    /// Folds the ordered projection into the primary batch when this
    /// completion has no unprojected predecessor. This preserves one durable
    /// RocksDB commit for the common group-commit path while later independent
    /// lanes still use the reorder-safe projector.
    pub(super) fn stage_inline_lane_projection(
        &self,
        batch: &mut WriteBatch,
        runtime: &LaneRuntime,
        completion: LaneCompletion,
    ) -> Result<Option<InlineLaneProjection>, MutationError> {
        if completion.ticket != runtime.projected_ticket.saturating_add(1)
            || !runtime.completions.is_empty()
        {
            return Ok(None);
        }

        let base_projected_ticket = runtime.projected_ticket;
        let mut projected_watch = runtime.projected_watch;
        let mut projected_receipts = runtime.projected_receipts;
        let mut projected_high_version = runtime.projected_high_version;
        self.apply_committed_lane_completion(
            batch,
            &mut projected_watch,
            &mut projected_receipts,
            &mut projected_high_version,
            completion,
        )?;
        // A completion which changes no projected authority has no existing
        // durable value that can prove whether an ambiguous write committed.
        // Keep that uncommon case on the explicit completion-marker path.
        if projected_watch == runtime.projected_watch
            && projected_receipts == runtime.projected_receipts
            && projected_high_version == runtime.projected_high_version
        {
            return Ok(None);
        }
        let projected = LaneRuntime {
            next_ticket: completion.ticket,
            projected_ticket: completion.ticket,
            reserved_watch: runtime.reserved_watch,
            reserved_receipts: runtime.reserved_receipts,
            projected_watch,
            projected_receipts,
            projected_high_version,
            reserved_reference_cursor_safe: runtime.reserved_reference_cursor_safe,
            reserved_inline_reference_safe: runtime.reserved_inline_reference_safe,
            completions: BTreeMap::new(),
            visibility_proofs: BTreeSet::new(),
            visibility_prefix_proof: None,
        };
        self.stage_lane_frontier_projection(batch, &projected)?;
        Ok(Some(InlineLaneProjection {
            base_projected_ticket,
            projected_ticket: completion.ticket,
            projected_watch,
            projected_receipts,
            projected_high_version,
            inline_reference_safe_through: (completion.inline_reference_safe
                && completion.first_offset != 0)
                .then_some(completion.last_offset),
        }))
    }

    #[cfg(test)]
    pub(super) async fn finish_lane_commit(
        &self,
        completion: LaneCompletion,
        committed: bool,
    ) -> Result<LaneSettlementMetrics, MutationError> {
        self.finish_lane_commit_inner(completion, committed, false)
            .await
    }

    async fn finish_lane_commit_inner(
        &self,
        completion: LaneCompletion,
        committed: bool,
        retry_projection_writes: bool,
    ) -> Result<LaneSettlementMetrics, MutationError> {
        let completion_sequence_started = Instant::now();
        let mut metrics = LaneSettlementMetrics::default();
        {
            let mut runtime = self.mutation_commit_lanes.sequence().await;
            metrics.completion_sequence_wait = completion_sequence_started.elapsed();
            let runtime = runtime.as_mut().ok_or_else(|| {
                MutationError::Storage("mutation lane runtime is not initialized".into())
            })?;
            let state = if committed {
                LaneCompletionState::Committed(completion)
            } else {
                runtime.reserved_receipts.entries = runtime
                    .reserved_receipts
                    .entries
                    .checked_sub(completion.receipt_entries)
                    .ok_or_else(|| {
                        MutationError::Storage("lane receipt rollback underflowed".into())
                    })?;
                runtime.reserved_receipts.bytes = runtime
                    .reserved_receipts
                    .bytes
                    .checked_sub(completion.receipt_bytes)
                    .ok_or_else(|| {
                        MutationError::Storage("lane receipt rollback underflowed".into())
                    })?;
                let gap_bytes = self.sequence_gap_bytes(completion)?;
                runtime.reserved_watch.retained_bytes = runtime
                    .reserved_watch
                    .retained_bytes
                    .checked_sub(completion.journal_bytes)
                    .and_then(|bytes| bytes.checked_add(gap_bytes))
                    .ok_or_else(|| {
                        MutationError::Storage("lane journal rollback overflowed".into())
                    })?;
                LaneCompletionState::Abandoned(completion)
            };
            if runtime
                .completions
                .insert(completion.ticket, state)
                .is_some()
            {
                return Err(MutationError::Storage(
                    "mutation lane completion was reported twice".into(),
                ));
            }
            let expected_ticket = runtime.projected_ticket.saturating_add(1);
            metrics.completion_reorder_depth =
                usize::try_from(completion.ticket.saturating_sub(expected_ticket))
                    .unwrap_or(usize::MAX);
            metrics.completion_ticket_lag =
                completion.ticket.saturating_sub(runtime.projected_ticket);
        }
        let projection = if retry_projection_writes {
            self.request_lane_projection_retrying_writes().await?
        } else {
            self.request_lane_projection().await?
        };
        metrics.projection_write = projection.write;
        metrics.contiguous_projection_completions = projection.completions;
        let frontier_wait_started = Instant::now();
        loop {
            let notified = self.mutation_commit_lanes.frontier_notify.notified();
            {
                let runtime = self.mutation_commit_lanes.sequence().await;
                let runtime = runtime.as_ref().ok_or_else(|| {
                    MutationError::Storage("mutation lane runtime is not initialized".into())
                })?;
                if runtime.projected_ticket >= completion.ticket {
                    metrics.ordered_frontier_wait = frontier_wait_started.elapsed();
                    return Ok(metrics);
                }
            }
            notified.await;
        }
    }

    pub(super) async fn settle_lane_source_journal_positions(
        &self,
        source: crate::SourceId,
        offsets: &[u64],
    ) -> Result<Option<u64>, MutationError> {
        if offsets.is_empty() {
            return Ok(None);
        }
        // Exclude the remaining genuinely exclusive administrative writers
        // while refreshing the lane authority, but do not exclude any other
        // ordinary or derived mutation lane.
        let _fence = self.mutation_commit_lanes.acquire_fence().await;
        let before = {
            let mut runtime = self.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_mut().ok_or_else(|| {
                MutationError::Storage("mutation lane runtime is not initialized".into())
            })?;
            self.refresh_stale_lane_runtime(runtime)?;
            if source != runtime.reserved_watch.source_id {
                return Err(MutationError::Storage(format!(
                    "source journal identity {source:?} does not match local source {:?}",
                    runtime.reserved_watch.source_id
                )));
            }
            if offsets
                .iter()
                .any(|offset| *offset > runtime.reserved_watch.tail)
            {
                return Err(MutationError::Storage(format!(
                    "source journal settled cursor is beyond reserved tail {}",
                    runtime.reserved_watch.tail
                )));
            }
            let before = runtime.projected_watch.settled_through;
            runtime
                .visibility_proofs
                .extend(offsets.iter().copied().filter(|offset| *offset > before));
            before
        };
        self.request_lane_projection().await?;
        let runtime = self.mutation_commit_lanes.sequence().await;
        let runtime = runtime.as_ref().ok_or_else(|| {
            MutationError::Storage("mutation lane runtime is not initialized".into())
        })?;
        Ok((runtime.projected_watch.settled_through > before)
            .then_some(runtime.projected_watch.settled_through))
    }

    pub(super) async fn settle_lane_source_journal_through(
        &self,
        source: crate::SourceId,
        offset: u64,
    ) -> Result<(), MutationError> {
        let _fence = self.mutation_commit_lanes.acquire_fence().await;
        {
            let mut sequence = self.mutation_commit_lanes.sequence().await;
            let runtime = sequence.as_mut().ok_or_else(|| {
                MutationError::Storage("mutation lane runtime is not initialized".into())
            })?;
            self.refresh_stale_lane_runtime(runtime)?;
            if source != runtime.reserved_watch.source_id {
                return Err(MutationError::Storage(format!(
                    "source journal identity {source:?} does not match local source {:?}",
                    runtime.reserved_watch.source_id
                )));
            }
            if offset > runtime.reserved_watch.tail {
                return Err(MutationError::Storage(format!(
                    "source journal settled cursor {offset} is beyond reserved tail {}",
                    runtime.reserved_watch.tail
                )));
            }
            if offset <= runtime.projected_watch.settled_through {
                return Ok(());
            }
            runtime.visibility_prefix_proof = Some(
                runtime
                    .visibility_prefix_proof
                    .map_or(offset, |current| current.max(offset)),
            );
        }
        self.request_lane_projection().await?;
        Ok(())
    }

    /// Settles a reserved ticket in an independently owned task.
    ///
    /// Once the primary batch has run, dropping the request future must not
    /// cancel settlement and leave a permanent hole in the ordered frontier.
    /// A failed ambiguity read is retried because only the atomically persisted
    /// completion record can distinguish a committed batch from an abandoned
    /// reservation without weakening mutation correctness.
    pub(super) async fn finish_lane_commit_cancellation_safe(
        &self,
        completion: LaneCompletion,
        primary_write_succeeded: bool,
        fence_lease: Arc<OwnedRwLockReadGuard<()>>,
    ) -> Result<LaneSettlementMetrics, MutationError> {
        self.finish_lane_commit_cancellation_safe_with_inline_projection(
            completion,
            None,
            primary_write_succeeded,
            fence_lease,
        )
        .await
    }

    pub(super) async fn finish_lane_commit_cancellation_safe_with_inline_projection(
        &self,
        completion: LaneCompletion,
        inline_projection: Option<InlineLaneProjection>,
        primary_write_succeeded: bool,
        fence_lease: Arc<OwnedRwLockReadGuard<()>>,
    ) -> Result<LaneSettlementMetrics, MutationError> {
        let store = self.clone();
        tokio::spawn(async move {
            // Caller cancellation may release its lane guard. Keep its exact
            // existing read fence until durable completion and volatile
            // frontier publication both finish.
            let _fence_lease = fence_lease;
            #[cfg(test)]
            if store
                .mutation_commit_lanes
                .pause_next_cancellation_safe_settlement
                .swap(false, Ordering::AcqRel)
            {
                store
                    .mutation_commit_lanes
                    .cancellation_safe_settlement_started
                    .add_permits(1);
                store
                    .mutation_commit_lanes
                    .cancellation_safe_settlement_continue
                    .acquire()
                    .await
                    .expect("cancellation-safe settlement test gate remains open")
                    .forget();
            }
            let committed = if primary_write_succeeded {
                true
            } else {
                let mut read_failures = 0_u64;
                loop {
                    let persisted = match inline_projection.as_ref() {
                        Some(projection) => {
                            store.persisted_inline_lane_projection_exists(projection)
                        }
                        None => store.persisted_lane_completion_exists(completion),
                    };
                    match persisted {
                        Ok(committed) => break committed,
                        Err(error) => {
                            read_failures = read_failures.saturating_add(1);
                            if read_failures == 1 || read_failures % 100 == 0 {
                                tracing::warn!(
                                    error = %error,
                                    lane_completion_ticket = completion.ticket,
                                    read_failures,
                                    "retrying mutation lane completion disambiguation"
                                );
                            }
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                }
            };
            if committed && let Some(projection) = inline_projection {
                store.publish_inline_lane_projection(projection).await
            } else {
                store
                    .finish_lane_commit_inner(completion, committed, true)
                    .await
            }
        })
        .await
        .map_err(|error| {
            MutationError::Storage(format!("mutation lane settlement task failed: {error}"))
        })?
    }

    async fn publish_inline_lane_projection(
        &self,
        projection: InlineLaneProjection,
    ) -> Result<LaneSettlementMetrics, MutationError> {
        let has_waiting_completions = {
            let mut runtime = self.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_mut().ok_or_else(|| {
                MutationError::Storage("mutation lane runtime is not initialized".into())
            })?;
            if runtime.projected_ticket != projection.base_projected_ticket {
                return Err(MutationError::Storage(
                    "inline mutation lane projection frontier changed before publication".into(),
                ));
            }
            runtime.projected_ticket = projection.projected_ticket;
            runtime.projected_watch = projection.projected_watch;
            runtime.projected_receipts = projection.projected_receipts;
            runtime.projected_high_version = projection.projected_high_version;
            runtime.reserved_watch.settled_through = projection.projected_watch.settled_through;
            !runtime.completions.is_empty()
        };
        if let Some(reference_safe_through) = projection.inline_reference_safe_through {
            self.settle_inline_source_changes_through_from_status(
                projection.projected_watch,
                reference_safe_through,
            )?;
        }
        self.mutation_commit_lanes.frontier_notify.notify_waiters();
        self.mutation_capacity_notify.notify_waiters();
        self.notify_local_invalidations_from_status(projection.projected_watch);
        if has_waiting_completions {
            self.request_lane_projection_retrying_writes().await?;
        }
        Ok(LaneSettlementMetrics {
            contiguous_projection_completions: 1,
            ..LaneSettlementMetrics::default()
        })
    }

    fn persisted_lane_completion_exists(
        &self,
        completion: LaneCompletion,
    ) -> Result<bool, MutationError> {
        #[cfg(test)]
        if self
            .mutation_commit_lanes
            .fail_next_completion_disambiguation
            .swap(false, Ordering::AcqRel)
        {
            return Err(MutationError::Storage(
                "injected mutation lane completion disambiguation failure".into(),
            ));
        }
        self.db
            .get_cf(self.cf(CF_METADATA)?, completion.key())
            .map(|value| value.is_some())
            .map_err(storage_error)
    }

    fn persisted_inline_lane_projection_exists(
        &self,
        projection: &InlineLaneProjection,
    ) -> Result<bool, MutationError> {
        let frontier = self
            .db
            .get_cf(self.cf(CF_METADATA)?, LANE_FRONTIER_KEY)
            .map_err(storage_error)?
            .map(|encoded| {
                let encoded: [u8; 8] = encoded.as_slice().try_into().map_err(|_| {
                    MutationError::Storage("mutation lane frontier is malformed".into())
                })?;
                Ok(u64::from_be_bytes(encoded))
            })
            .transpose()?;
        let watch = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let receipts = self.mutation_receipt_status()?;
        let high_version = self
            .db
            .get_cf(self.cf(CF_METADATA)?, VERSION_HIGH_WATERMARK_KEY)
            .map_err(storage_error)?
            .map(|encoded| serde_json::from_slice::<VersionId>(&encoded))
            .transpose()
            .map_err(storage_error)?;
        Ok(frontier == Some(projection.projected_ticket)
            && watch == projection.projected_watch
            && receipts == projection.projected_receipts
            && high_version == projection.projected_high_version)
    }

    #[cfg(test)]
    pub(super) async fn project_lane_completions(
        &self,
    ) -> Result<LaneProjectionMetrics, MutationError> {
        self.project_lane_completions_classified()
            .await
            .map_err(LaneProjectionFailure::into_error)
    }

    async fn project_lane_completions_classified(
        &self,
    ) -> Result<LaneProjectionMetrics, LaneProjectionFailure> {
        let projection = self.mutation_commit_lanes.projection.lock().await;
        let result = self.project_lane_completions_inner().await;
        self.mutation_commit_lanes
            .projection_retry_needed
            .store(result.is_err(), Ordering::Release);
        drop(projection);
        result
    }

    async fn project_lane_completions_inner(
        &self,
    ) -> Result<LaneProjectionMetrics, LaneProjectionFailure> {
        let plan = {
            let mut runtime = self.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_mut().ok_or_else(|| {
                MutationError::Storage("mutation lane runtime is not initialized".into())
            })?;
            // The projection lock excludes direct authority changes. Rebind
            // cached frontiers after an exclusive writer before preparing any
            // status write, including a visibility-only projection.
            self.refresh_stale_lane_runtime(runtime)?;
            self.plan_lane_completions(runtime)?
        };
        let Some(mut plan) = plan else {
            return Ok(LaneProjectionMetrics::default());
        };
        let completion_count = plan.completions.len();
        let projected_watch = plan.prospective.projected_watch;
        let mut options = WriteOptions::default();
        if plan.requires_sync {
            options.set_sync(self.sync_writes);
        }
        // Completion markers originate in primary WAL-backed mutation batches.
        // WAL-less deletion is not replay-safe: a later inline commit can log
        // a newer frontier while restart resurrects the old marker. Keep this
        // atomic projection in WAL even when no additional fsync is required.
        #[cfg(test)]
        if self
            .mutation_commit_lanes
            .fail_next_projection
            .swap(false, Ordering::AcqRel)
        {
            return Err(LaneProjectionFailure::Write(MutationError::Storage(
                "injected mutation lane projection failure".into(),
            )));
        }
        let write_started = Instant::now();
        self.db
            .write_opt(std::mem::take(&mut plan.batch), &options)
            .map_err(LaneProjectionFailure::from_write_error)?;
        let write = write_started.elapsed();
        #[cfg(test)]
        if self
            .mutation_commit_lanes
            .pause_next_projection
            .swap(false, Ordering::AcqRel)
        {
            self.mutation_commit_lanes
                .projection_write_completed
                .add_permits(1);
            self.mutation_commit_lanes
                .projection_publish_continue
                .acquire()
                .await
                .expect("projection test gate remains open")
                .forget();
        }
        {
            let mut runtime = self.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_mut().ok_or_else(|| {
                MutationError::Storage("mutation lane runtime is not initialized".into())
            })?;
            self.publish_lane_projection(runtime, &plan)?;
        }
        if let Some(reference_safe_through) = plan.inline_reference_safe_through {
            self.settle_inline_source_changes_through_from_status(
                projected_watch,
                reference_safe_through,
            )?;
        }
        self.mutation_commit_lanes.frontier_notify.notify_waiters();
        self.mutation_capacity_notify.notify_waiters();
        self.notify_local_invalidations_from_status(projected_watch);
        Ok(LaneProjectionMetrics {
            write,
            completions: completion_count,
        })
    }

    fn plan_lane_completions(
        &self,
        runtime: &LaneRuntime,
    ) -> Result<Option<LaneProjectionPlan>, MutationError> {
        let mut prospective = runtime.clone();
        let mut batch = WriteBatch::default();
        let mut requires_sync = false;
        let mut completions = Vec::new();
        let mut inline_reference_safe_through = None;
        let base_settled_through = prospective.projected_watch.settled_through;
        loop {
            let next = prospective.projected_ticket.checked_add(1).ok_or_else(|| {
                MutationError::Storage("mutation lane frontier is exhausted".into())
            })?;
            let Some(completion) = prospective.completions.remove(&next) else {
                break;
            };
            completions.push((next, completion));
            match completion {
                LaneCompletionState::Committed(completion) => {
                    self.apply_committed_lane_completion(
                        &mut batch,
                        &mut prospective.projected_watch,
                        &mut prospective.projected_receipts,
                        &mut prospective.projected_high_version,
                        completion,
                    )?;
                    if completion.inline_reference_safe && completion.first_offset != 0 {
                        inline_reference_safe_through = Some(completion.last_offset);
                    }
                    batch.delete_cf(self.cf(CF_METADATA)?, completion.key());
                }
                LaneCompletionState::Abandoned(completion) => {
                    requires_sync = true;
                    self.stage_abandoned_lane_range(
                        &mut batch,
                        &mut prospective.projected_watch,
                        completion,
                    )?;
                    if completion.inline_reference_safe && completion.first_offset != 0 {
                        inline_reference_safe_through = Some(completion.last_offset);
                    }
                }
            }
            prospective.projected_ticket = next;
        }
        while prospective.projected_watch.settled_through < prospective.projected_watch.tail {
            let next = prospective
                .projected_watch
                .settled_through
                .checked_add(1)
                .ok_or_else(|| {
                    MutationError::Storage("source settlement frontier is exhausted".into())
                })?;
            let prefix_proven = prospective
                .visibility_prefix_proof
                .is_some_and(|through| next <= through);
            if !prefix_proven && !prospective.visibility_proofs.remove(&next) {
                break;
            }
            prospective.projected_watch.settled_through = next;
        }
        prospective
            .visibility_proofs
            .retain(|offset| *offset > prospective.projected_watch.settled_through);
        if prospective
            .visibility_prefix_proof
            .is_some_and(|through| through <= prospective.projected_watch.settled_through)
        {
            prospective.visibility_prefix_proof = None;
        }
        if prospective.projected_watch.settled_through != base_settled_through {
            requires_sync = true;
        }
        if completions.is_empty()
            && prospective.projected_watch.settled_through == base_settled_through
        {
            return Ok(None);
        }
        prospective.reserved_watch.settled_through = prospective.projected_watch.settled_through;
        self.stage_lane_frontier_projection(&mut batch, &prospective)?;
        Ok(Some(LaneProjectionPlan {
            base_projected_ticket: runtime.projected_ticket,
            prospective,
            completions,
            batch,
            requires_sync,
            inline_reference_safe_through,
        }))
    }

    fn publish_lane_projection(
        &self,
        runtime: &mut LaneRuntime,
        plan: &LaneProjectionPlan,
    ) -> Result<(), MutationError> {
        if runtime.projected_ticket != plan.base_projected_ticket {
            return Err(MutationError::Storage(
                "mutation lane projection frontier changed during its durable write".into(),
            ));
        }
        for (ticket, expected) in &plan.completions {
            if runtime.completions.get(ticket) != Some(expected) {
                return Err(MutationError::Storage(
                    "mutation lane completion changed during its durable projection".into(),
                ));
            }
        }
        for (ticket, _) in &plan.completions {
            runtime.completions.remove(ticket);
        }
        runtime.projected_ticket = plan.prospective.projected_ticket;
        runtime.projected_watch = plan.prospective.projected_watch;
        runtime.projected_receipts = plan.prospective.projected_receipts;
        runtime.projected_high_version = plan.prospective.projected_high_version;
        runtime
            .visibility_proofs
            .retain(|offset| *offset > runtime.projected_watch.settled_through);
        if runtime
            .visibility_prefix_proof
            .is_some_and(|through| through <= runtime.projected_watch.settled_through)
        {
            runtime.visibility_prefix_proof = None;
        }
        runtime.reserved_watch.settled_through = runtime.projected_watch.settled_through;
        Ok(())
    }

    fn apply_committed_lane_completion(
        &self,
        batch: &mut WriteBatch,
        watch: &mut WatchJournalStatus,
        receipts: &mut MutationReceiptStatus,
        high_version: &mut Option<VersionId>,
        completion: LaneCompletion,
    ) -> Result<(), MutationError> {
        if completion.first_offset != 0 {
            let expected = watch.tail.checked_add(1).ok_or_else(|| {
                MutationError::Storage("lane source frontier is exhausted".into())
            })?;
            let visibility_contiguous = completion.visibility_settled
                && watch.settled_through.checked_add(1) == Some(expected);
            if completion.first_offset > expected {
                self.stage_sequence_gaps(batch, watch, expected, completion.first_offset - 1)?;
            } else if completion.first_offset < expected {
                return Err(MutationError::Storage(
                    "lane completion overlaps the projected source frontier".into(),
                ));
            }
            watch.tail = completion.last_offset;
            watch.retained_entries = watch
                .retained_entries
                .checked_add(completion.journal_entries)
                .ok_or_else(|| MutationError::Storage("lane journal count is exhausted".into()))?;
            watch.retained_bytes = watch
                .retained_bytes
                .checked_add(completion.journal_bytes)
                .ok_or_else(|| MutationError::Storage("lane journal bytes are exhausted".into()))?;
            if completion.reference_cursor_advanced {
                self.stage_reference_delta_cursor(batch, watch.source_id, watch.tail)?;
            }
            if visibility_contiguous {
                watch.settled_through = watch.tail;
            }
        }
        receipts.entries = receipts
            .entries
            .checked_add(completion.receipt_entries)
            .ok_or_else(|| MutationError::Storage("lane receipt count is exhausted".into()))?;
        receipts.bytes = receipts
            .bytes
            .checked_add(completion.receipt_bytes)
            .ok_or_else(|| MutationError::Storage("lane receipt bytes are exhausted".into()))?;
        if let Some(version) = completion.high_version {
            *high_version = Some(high_version.map_or(version, |current| current.max(version)));
        }
        Ok(())
    }

    fn stage_abandoned_lane_range(
        &self,
        batch: &mut WriteBatch,
        watch: &mut WatchJournalStatus,
        completion: LaneCompletion,
    ) -> Result<(), MutationError> {
        if completion.first_offset != 0 {
            let expected = watch.tail.checked_add(1).ok_or_else(|| {
                MutationError::Storage("lane source frontier is exhausted".into())
            })?;
            let visibility_contiguous = completion.visibility_settled
                && watch.settled_through.checked_add(1) == Some(expected);
            if completion.first_offset != expected {
                return Err(MutationError::Storage(
                    "abandoned lane range is not contiguous".into(),
                ));
            }
            self.stage_sequence_gaps(
                batch,
                watch,
                completion.first_offset,
                completion.last_offset,
            )?;
            if completion.reference_cursor_advanced {
                self.stage_reference_delta_cursor(batch, watch.source_id, watch.tail)?;
            }
            if visibility_contiguous {
                watch.settled_through = watch.tail;
            }
        }
        Ok(())
    }

    fn stage_sequence_gaps(
        &self,
        batch: &mut WriteBatch,
        watch: &mut WatchJournalStatus,
        first: u64,
        last: u64,
    ) -> Result<(), MutationError> {
        for offset in first..=last {
            let encoded =
                encode_local_change(&LocalChange::sequence_gap(offset)).map_err(storage_error)?;
            let bytes = invalidation_record_bytes(encoded.len());
            batch.put_cf(
                self.cf(CF_LOCAL_INVALIDATIONS)?,
                invalidation_key(offset),
                encoded,
            );
            watch.retained_entries = watch
                .retained_entries
                .checked_add(1)
                .ok_or_else(|| MutationError::Storage("lane gap count is exhausted".into()))?;
            watch.retained_bytes = watch
                .retained_bytes
                .checked_add(bytes)
                .ok_or_else(|| MutationError::Storage("lane gap bytes are exhausted".into()))?;
            watch.tail = offset;
        }
        Ok(())
    }

    fn sequence_gap_bytes(&self, completion: LaneCompletion) -> Result<u64, MutationError> {
        if completion.first_offset == 0 {
            return Ok(0);
        }
        let mut bytes = 0_u64;
        for offset in completion.first_offset..=completion.last_offset {
            let encoded =
                encode_local_change(&LocalChange::sequence_gap(offset)).map_err(storage_error)?;
            bytes = bytes
                .checked_add(invalidation_record_bytes(encoded.len()))
                .ok_or_else(|| MutationError::Storage("lane gap bytes are exhausted".into()))?;
        }
        Ok(bytes)
    }

    fn stage_lane_frontier_projection(
        &self,
        batch: &mut WriteBatch,
        runtime: &LaneRuntime,
    ) -> Result<(), MutationError> {
        let metadata = self.cf(CF_METADATA)?;
        batch.put_cf(
            metadata,
            LANE_FRONTIER_KEY,
            runtime.projected_ticket.to_be_bytes(),
        );
        self.stage_lane_authority_projection(batch, runtime)
    }

    fn stage_lane_authority_projection(
        &self,
        batch: &mut WriteBatch,
        runtime: &LaneRuntime,
    ) -> Result<(), MutationError> {
        let metadata = self.cf(CF_METADATA)?;
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_STATUS_KEY,
            encode_watch_journal_status(runtime.projected_watch),
        );
        batch.put_cf(
            metadata,
            MUTATION_RECEIPT_STATUS_KEY,
            encode_mutation_receipt_status(runtime.projected_receipts),
        );
        if let Some(version) = runtime.projected_high_version {
            batch.put_cf(
                metadata,
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&version).map_err(storage_error)?,
            );
        }
        Ok(())
    }
}

pub(super) fn conflict_resources(
    operation: &PreparedOperation,
    definition_intent: Option<DefinitionMutationIntent>,
) -> Vec<Vec<u8>> {
    let mut resources = operation
        .lock_paths()
        .into_iter()
        .map(|path| object_path_conflict_resource(operation.identity(), &path.path))
        .collect::<Vec<_>>();
    if let Some(command_id) = operation.command_id() {
        resources.push(tagged_resource(
            2,
            [receipt_key(operation.identity(), command_id).as_slice()],
        ));
    }
    if let Some(reference) = operation.payload_reference() {
        resources.push(blob_conflict_resource(reference));
    }
    if let Some(intent) = definition_intent {
        resources.push(tagged_resource(
            4,
            [
                operation.identity().encode().as_slice(),
                &[intent.kind as u8],
                intent.definition_id.to_be_bytes().as_slice(),
            ],
        ));
    }
    resources
}

pub(super) fn object_path_conflict_resource(
    identity: crate::key::BucketIdentity,
    exact_path: &str,
) -> Vec<u8> {
    tagged_resource(1, [identity.encode().as_slice(), exact_path.as_bytes()])
}

pub(super) fn replica_conflict_resources(mutation: &crate::ObjectMutation) -> Vec<Vec<u8>> {
    let identity = crate::store::BucketIdentity {
        tenant_id: crate::store::TenantId(mutation.tenant_id),
        bucket_id: crate::store::BucketId(mutation.bucket_id),
    };
    let mut resources = vec![
        tagged_resource(
            1,
            [identity.encode().as_slice(), mutation.exact_path.as_bytes()],
        ),
        tagged_resource(2, [receipt_key(identity, &mutation.command_id).as_slice()]),
    ];
    if let Some(transition) = mutation.definition_transition.as_ref() {
        resources.push(tagged_resource(
            4,
            [
                identity.encode().as_slice(),
                &[transition.kind as u8],
                transition.definition_id.to_be_bytes().as_slice(),
            ],
        ));
    }
    resources
}

pub(super) fn blob_conflict_resource(reference: &crate::BlobRef) -> Vec<u8> {
    tagged_resource(
        super::mutation_conflict_scheduler::MUTEX_ONLY_BLOB_RESOURCE_TAG,
        [
            reference.hash.as_slice(),
            reference.length.to_be_bytes().as_slice(),
        ],
    )
}

pub(super) fn artifact_conflict_resource(identity: &[u8]) -> Vec<u8> {
    tagged_resource(5, [identity])
}

/// One local source journal has one Raft-fenced derived-consumer membership
/// and checkpoint namespace. Updates within that namespace must be ordered,
/// but they do not conflict with object paths, receipts, blobs, definitions,
/// or immutable artifacts.
pub(super) fn derived_consumer_conflict_resource() -> Vec<u8> {
    tagged_resource(6, std::iter::empty::<&[u8]>())
}

fn tagged_resource<'a>(tag: u8, parts: impl IntoIterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut resource = vec![tag];
    for part in parts {
        resource.extend_from_slice(
            &u64::try_from(part.len())
                .expect("mutation resource component length fits u64")
                .to_be_bytes(),
        );
        resource.extend_from_slice(part);
    }
    resource
}

#[cfg(test)]
#[path = "mutation_commit_lanes_tests.rs"]
pub(super) mod tests;
