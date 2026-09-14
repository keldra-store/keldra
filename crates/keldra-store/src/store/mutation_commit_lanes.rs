//! Stable conflict lanes for independently prepared object mutations.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{
    Mutex, Notify, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock,
    RwLockWriteGuard, Semaphore,
};

use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions};

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

#[derive(Clone)]
pub(super) struct MutationCommitLanes {
    fence: Arc<RwLock<()>>,
    conflicts: Arc<std::sync::Mutex<BTreeMap<Vec<u8>, std::sync::Weak<tokio::sync::Mutex<()>>>>>,
    physical_slots: Arc<Semaphore>,
    physical_slots_active: Arc<AtomicUsize>,
    physical_slots_peak: Arc<AtomicUsize>,
    physical_slot_count: usize,
    sequence: Arc<Mutex<Option<LaneRuntime>>>,
    projection: Arc<Mutex<()>>,
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
}

pub(super) struct MutationLaneGuard {
    _fence: OwnedRwLockReadGuard<()>,
    conflicts: Vec<OwnedMutexGuard<()>>,
    conflict_keys: Vec<Vec<u8>>,
    conflict_registry:
        Arc<std::sync::Mutex<BTreeMap<Vec<u8>, std::sync::Weak<tokio::sync::Mutex<()>>>>>,
    physical_slot: Option<tokio::sync::OwnedSemaphorePermit>,
    physical_slots_active: Arc<AtomicUsize>,
    physical_slots_peak_since_start: Arc<AtomicUsize>,
    conflict_wait: Duration,
    physical_slot_wait: Duration,
    physical_slots_active_at_acquire: usize,
    physical_slots_peak_since_start_at_acquire: usize,
    physical_slot_count: usize,
}

impl Drop for MutationLaneGuard {
    fn drop(&mut self) {
        self.release_physical_slot();
        self.conflicts.clear();
        let mut registry = self
            .conflict_registry
            .lock()
            .expect("mutation conflict registry lock is not poisoned");
        for key in &self.conflict_keys {
            if registry
                .get(key)
                .is_some_and(|entry| entry.strong_count() == 0)
            {
                registry.remove(key);
            }
        }
    }
}

impl MutationLaneGuard {
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

pub(super) struct ExclusiveMutationGuard<'a> {
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

#[derive(Clone)]
pub(super) struct LaneRuntime {
    pub(super) next_ticket: u64,
    pub(super) projected_ticket: u64,
    pub(super) reserved_watch: WatchJournalStatus,
    pub(super) reserved_receipts: MutationReceiptStatus,
    pub(super) projected_watch: WatchJournalStatus,
    pub(super) projected_receipts: MutationReceiptStatus,
    pub(super) projected_high_version: Option<VersionId>,
    pub(super) completions: BTreeMap<u64, LaneCompletionState>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LaneCompletionState {
    Committed(LaneCompletion),
    Abandoned(LaneCompletion),
}

impl MutationCommitLanes {
    pub(super) fn new(commit_lanes: usize) -> Self {
        Self {
            fence: Arc::new(RwLock::new(())),
            conflicts: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            physical_slots: Arc::new(Semaphore::new(commit_lanes)),
            physical_slots_active: Arc::new(AtomicUsize::new(0)),
            physical_slots_peak: Arc::new(AtomicUsize::new(0)),
            physical_slot_count: commit_lanes,
            sequence: Arc::new(Mutex::new(None)),
            projection: Arc::new(Mutex::new(())),
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
        let conflict_keys = resources.into_iter().collect::<BTreeSet<_>>();
        let conflict_locks = {
            let mut registry = self
                .conflicts
                .lock()
                .expect("mutation conflict registry lock is not poisoned");
            conflict_keys
                .iter()
                .map(|key| {
                    if let Some(lock) = registry.get(key).and_then(std::sync::Weak::upgrade) {
                        lock
                    } else {
                        let lock = Arc::new(Mutex::new(()));
                        registry.insert(key.clone(), Arc::downgrade(&lock));
                        lock
                    }
                })
                .collect::<Vec<_>>()
        };
        let conflict_started = Instant::now();
        let mut conflicts = Vec::with_capacity(conflict_locks.len());
        for lock in conflict_locks {
            conflicts.push(lock.lock_owned().await);
        }
        let conflict_wait = conflict_started.elapsed();
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
            _fence: fence,
            conflicts,
            conflict_keys: conflict_keys.into_iter().collect(),
            conflict_registry: self.conflicts.clone(),
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

    pub(super) async fn acquire_exclusive(&self) -> ExclusiveMutationGuard<'static> {
        ExclusiveMutationGuard {
            _fence: ExclusiveFence::Owned {
                _guard: self.fence.clone().write_owned().await,
            },
            authorities_stale: self.authorities_stale.clone(),
        }
    }

    pub(super) fn try_acquire_exclusive(
        &self,
    ) -> Result<ExclusiveMutationGuard<'static>, tokio::sync::TryLockError> {
        Ok(ExclusiveMutationGuard {
            _fence: ExclusiveFence::Owned {
                _guard: self.fence.clone().try_write_owned()?,
            },
            authorities_stale: self.authorities_stale.clone(),
        })
    }

    pub(super) fn blocking_acquire_exclusive(&self) -> ExclusiveMutationGuard<'_> {
        ExclusiveMutationGuard {
            _fence: ExclusiveFence::Borrowed {
                _guard: self.fence.blocking_write(),
            },
            authorities_stale: self.authorities_stale.clone(),
        }
    }

    pub(super) async fn sequence(&self) -> tokio::sync::MutexGuard<'_, Option<LaneRuntime>> {
        self.sequence.lock().await
    }

    pub(super) fn projection_retry_needed(&self) -> bool {
        self.projection_retry_needed.load(Ordering::Acquire)
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
        reference_cursor_advanced: bool,
        inline_reference_safe: bool,
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
        let completion = LaneCompletion {
            ticket,
            first_offset,
            last_offset: if journal_entries == 0 { 0 } else { watch.tail },
            journal_entries,
            journal_bytes,
            receipt_entries,
            receipt_bytes,
            high_version,
            reference_cursor_advanced,
            inline_reference_safe,
            visibility_settled,
        };
        completion.validate().map_err(MutationError::Storage)?;
        self.next_ticket = ticket;
        self.reserved_watch = watch;
        self.reserved_receipts = receipts;
        Ok(completion)
    }
}

impl Store {
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
        let runtime = LaneRuntime {
            next_ticket: projected_ticket,
            projected_ticket,
            reserved_watch: watch,
            reserved_receipts: receipts,
            projected_watch: watch,
            projected_receipts: receipts,
            projected_high_version: high_version,
            completions: BTreeMap::new(),
        };
        if recovered {
            self.stage_lane_frontier_projection(&mut batch, &runtime)?;
            let mut options = WriteOptions::default();
            options.disable_wal(true);
            self.db.write_opt(batch, &options)?;
        }
        if let Some(version) = high_version {
            self.clock.observe(version);
        }
        let mut slot = self.mutation_commit_lanes.sequence.lock().await;
        if slot.replace(runtime).is_some() {
            anyhow::bail!("mutation lane runtime was initialized twice");
        }
        Ok(())
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

    pub(super) async fn finish_lane_commit(
        &self,
        completion: LaneCompletion,
        committed: bool,
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
        let projection = self.project_lane_completions().await?;
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
    ) -> Result<LaneSettlementMetrics, MutationError> {
        let store = self.clone();
        tokio::spawn(async move {
            let committed = if primary_write_succeeded {
                true
            } else {
                let mut read_failures = 0_u64;
                loop {
                    match store.persisted_lane_completion_exists(completion) {
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
            store.finish_lane_commit(completion, committed).await
        })
        .await
        .map_err(|error| {
            MutationError::Storage(format!("mutation lane settlement task failed: {error}"))
        })?
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

    pub(super) async fn project_lane_completions(
        &self,
    ) -> Result<LaneProjectionMetrics, MutationError> {
        let projection = self.mutation_commit_lanes.projection.lock().await;
        let result = self.project_lane_completions_inner().await;
        self.mutation_commit_lanes
            .projection_retry_needed
            .store(result.is_err(), Ordering::Release);
        drop(projection);
        result
    }

    async fn project_lane_completions_inner(&self) -> Result<LaneProjectionMetrics, MutationError> {
        let plan = {
            let runtime = self.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_ref().ok_or_else(|| {
                MutationError::Storage("mutation lane runtime is not initialized".into())
            })?;
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
        } else {
            options.disable_wal(true);
        }
        #[cfg(test)]
        if self
            .mutation_commit_lanes
            .fail_next_projection
            .swap(false, Ordering::AcqRel)
        {
            return Err(MutationError::Storage(
                "injected mutation lane projection failure".into(),
            ));
        }
        let write_started = Instant::now();
        self.db
            .write_opt(std::mem::take(&mut plan.batch), &options)
            .map_err(storage_error)?;
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
        if completions.is_empty() {
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
            if completion.visibility_settled {
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
            if completion.visibility_settled {
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

fn lane_completion_ticket_from_key(key: &[u8]) -> anyhow::Result<u64> {
    let suffix = key
        .strip_prefix(LANE_COMPLETION_PREFIX)
        .ok_or_else(|| anyhow::anyhow!("mutation lane completion key has the wrong prefix"))?;
    Ok(u64::from_be_bytes(suffix.try_into().map_err(|_| {
        anyhow::anyhow!("mutation lane completion key is malformed")
    })?))
}

impl LaneCompletion {
    pub(super) fn key(self) -> Vec<u8> {
        let mut key = Vec::with_capacity(LANE_COMPLETION_PREFIX.len() + 8);
        key.extend_from_slice(LANE_COMPLETION_PREFIX);
        key.extend_from_slice(&self.ticket.to_be_bytes());
        key
    }

    pub(super) fn encode(self) -> [u8; LANE_COMPLETION_BYTES] {
        let mut encoded = [0_u8; LANE_COMPLETION_BYTES];
        encoded[0] = LANE_COMPLETION_FORMAT;
        let values = [
            self.ticket,
            self.first_offset,
            self.last_offset,
            self.journal_entries,
            self.journal_bytes,
            self.receipt_entries,
            self.receipt_bytes,
        ];
        for (index, value) in values.into_iter().enumerate() {
            let start = 1 + index * 8;
            encoded[start..start + 8].copy_from_slice(&value.to_be_bytes());
        }
        let option = 1 + values.len() * 8;
        if let Some(version) = self.high_version {
            encoded[option] = 1;
            encoded[option + 1..option + 9].copy_from_slice(&version.0.to_be_bytes());
        }
        encoded[option + 9] = u8::from(self.reference_cursor_advanced);
        encoded[option + 10] = u8::from(self.inline_reference_safe);
        encoded[option + 11] = u8::from(self.visibility_settled);
        encoded
    }

    pub(super) fn decode(encoded: &[u8]) -> Result<Self, String> {
        let encoded: &[u8; LANE_COMPLETION_BYTES] = encoded
            .try_into()
            .map_err(|_| "lane completion length is invalid".to_owned())?;
        if encoded[0] != LANE_COMPLETION_FORMAT {
            return Err("lane completion format is unsupported".into());
        }
        let read = |start: usize| {
            u64::from_be_bytes(encoded[start..start + 8].try_into().expect("fixed slice"))
        };
        let option = 1 + 7 * 8;
        let high_version = match encoded[option] {
            0 => None,
            1 => Some(VersionId(read(option + 1))),
            _ => return Err("lane completion version marker is invalid".into()),
        };
        let reference_cursor_advanced = match encoded[option + 9] {
            0 => false,
            1 => true,
            _ => return Err("lane completion reference-settlement marker is invalid".into()),
        };
        let inline_reference_safe = match encoded[option + 10] {
            0 => false,
            1 => true,
            _ => return Err("lane completion inline-reference marker is invalid".into()),
        };
        let visibility_settled = match encoded[option + 11] {
            0 => false,
            1 => true,
            _ => return Err("lane completion visibility marker is invalid".into()),
        };
        let completion = Self {
            ticket: read(1),
            first_offset: read(9),
            last_offset: read(17),
            journal_entries: read(25),
            journal_bytes: read(33),
            receipt_entries: read(41),
            receipt_bytes: read(49),
            high_version,
            reference_cursor_advanced,
            inline_reference_safe,
            visibility_settled,
        };
        completion.validate()?;
        Ok(completion)
    }

    fn validate(self) -> Result<(), String> {
        if self.ticket == 0 {
            return Err("lane completion ticket is zero".into());
        }
        let range_entries = if self.first_offset == 0 && self.last_offset == 0 {
            0
        } else if self.first_offset == 0 || self.last_offset < self.first_offset {
            return Err("lane completion source range is invalid".into());
        } else {
            self.last_offset - self.first_offset + 1
        };
        if range_entries != self.journal_entries {
            return Err("lane completion source range disagrees with its entry count".into());
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
        .map(|path| {
            tagged_resource(
                1,
                [
                    path.tenant.as_bytes(),
                    path.bucket.as_bytes(),
                    path.path.as_bytes(),
                ],
            )
        })
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

pub(super) fn blob_conflict_resource(reference: &crate::BlobRef) -> Vec<u8> {
    tagged_resource(
        3,
        [
            reference.hash.as_slice(),
            reference.length.to_be_bytes().as_slice(),
        ],
    )
}

pub(super) fn artifact_conflict_resource(identity: &[u8]) -> Vec<u8> {
    tagged_resource(5, [identity])
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
mod tests {
    use super::*;
    use crate::{
        BatchOperation, BlobRef, Durability, ObjectKey, ObjectMutationContext,
        ObjectMutationGovernance, PlacementLogId, PutMode, PutRequest, ReferenceDelta,
        StoreOptions, WatchCursor, WatchScope,
    };

    fn status(tail: u64, entries: u64, bytes: u64) -> WatchJournalStatus {
        WatchJournalStatus {
            source_id: crate::SourceId {
                node_id: 1,
                source_epoch: [7; 32],
            },
            tail,
            settled_through: tail,
            retention_floor: 0,
            retained_entries: entries,
            retained_bytes: bytes,
        }
    }

    #[test]
    fn completion_encoding_is_fixed_and_rejects_range_mismatch() {
        let completion = LaneCompletion {
            ticket: 9,
            first_offset: 11,
            last_offset: 13,
            journal_entries: 3,
            journal_bytes: 707,
            receipt_entries: 2,
            receipt_bytes: 808,
            high_version: Some(VersionId(91)),
            reference_cursor_advanced: true,
            inline_reference_safe: true,
            visibility_settled: true,
        };
        assert_eq!(
            LaneCompletion::decode(&completion.encode()).unwrap(),
            completion
        );
        let mut malformed = completion.encode();
        malformed[25..33].copy_from_slice(&2_u64.to_be_bytes());
        assert!(LaneCompletion::decode(&malformed).is_err());
    }

    #[test]
    fn reservation_assigns_monotonic_tickets_and_exact_source_ranges() {
        let initial = status(4, 4, 400);
        let mut runtime = LaneRuntime {
            next_ticket: 7,
            projected_ticket: 7,
            reserved_watch: initial,
            reserved_receipts: MutationReceiptStatus {
                entries: 2,
                bytes: 20,
            },
            projected_watch: initial,
            projected_receipts: MutationReceiptStatus {
                entries: 2,
                bytes: 20,
            },
            projected_high_version: Some(VersionId(10)),
            completions: BTreeMap::new(),
        };
        let completion = runtime
            .reserve(
                status(6, 6, 650),
                MutationReceiptStatus {
                    entries: 3,
                    bytes: 35,
                },
                Some(VersionId(12)),
            )
            .unwrap();
        assert_eq!(completion.ticket, 8);
        assert_eq!((completion.first_offset, completion.last_offset), (5, 6));
        assert_eq!(
            (completion.journal_entries, completion.journal_bytes),
            (2, 250)
        );
        assert_eq!(
            (completion.receipt_entries, completion.receipt_bytes),
            (1, 15)
        );
    }

    #[tokio::test]
    async fn out_of_order_completion_does_not_advance_across_live_ticket() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let (first, second) = {
            let mut runtime = store.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_mut().unwrap();
            let watch = runtime.reserved_watch;
            let receipts = runtime.reserved_receipts;
            (
                runtime.reserve(watch, receipts, None).unwrap(),
                runtime.reserve(watch, receipts, None).unwrap(),
            )
        };
        for completion in [first, second] {
            let mut batch = WriteBatch::default();
            store.stage_lane_completion(&mut batch, completion).unwrap();
            store.db.write(batch).unwrap();
        }

        let later_finish = tokio::spawn({
            let store = store.clone();
            async move { store.finish_lane_commit(second, true).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !later_finish.is_finished(),
            "a later response must wait for the contiguous durable frontier"
        );
        let encoded = store
            .db
            .get_cf(store.cf(CF_METADATA).unwrap(), LANE_FRONTIER_KEY)
            .unwrap()
            .unwrap();
        assert_eq!(
            u64::from_be_bytes(encoded.as_slice().try_into().unwrap()),
            0
        );

        let first_metrics = store.finish_lane_commit(first, true).await.unwrap();
        let later_metrics = later_finish.await.unwrap().unwrap();
        assert_eq!(first_metrics.completion_reorder_depth, 0);
        assert_eq!(first_metrics.completion_ticket_lag, 1);
        assert_eq!(first_metrics.contiguous_projection_completions, 2);
        assert!(first_metrics.projection_write > Duration::ZERO);
        assert_eq!(later_metrics.completion_reorder_depth, 1);
        assert_eq!(later_metrics.completion_ticket_lag, 2);
        assert_eq!(later_metrics.contiguous_projection_completions, 0);
        assert!(later_metrics.ordered_frontier_wait > Duration::ZERO);
        let encoded = store
            .db
            .get_cf(store.cf(CF_METADATA).unwrap(), LANE_FRONTIER_KEY)
            .unwrap()
            .unwrap();
        assert_eq!(
            u64::from_be_bytes(encoded.as_slice().try_into().unwrap()),
            2
        );
    }

    #[tokio::test]
    async fn out_of_order_reference_completion_does_not_cross_visibility_frontier() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        let make_change = |offset| {
            LocalChange::object_head(
                offset,
                tenant_id,
                bucket_id,
                format!("objects/{offset}"),
                VersionId(offset),
                false,
                vec![ReferenceDelta {
                    blob: BlobRef {
                        hash: [offset as u8; 32],
                        length: 8,
                    },
                    change: 1,
                }],
                None,
                None,
            )
        };
        let mut completions = Vec::new();
        for offset in [1, 2] {
            let change = make_change(offset);
            let encoded = encode_local_change(&change).unwrap();
            let completion = LaneCompletion {
                ticket: offset,
                first_offset: offset,
                last_offset: offset,
                journal_entries: 1,
                journal_bytes: invalidation_record_bytes(encoded.len()),
                receipt_entries: 0,
                receipt_bytes: 0,
                high_version: Some(VersionId(offset)),
                reference_cursor_advanced: true,
                inline_reference_safe: true,
                visibility_settled: true,
            };
            let mut batch = WriteBatch::default();
            batch.put_cf(
                store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
                invalidation_key(offset),
                encoded,
            );
            store.stage_lane_completion(&mut batch, completion).unwrap();
            store.db.write(batch).unwrap();
            completions.push(completion);
        }

        let later_finish = tokio::spawn({
            let store = store.clone();
            let completion = completions[1];
            async move { store.finish_lane_commit(completion, true).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !later_finish.is_finished(),
            "reference visibility must wait for the contiguous frontier"
        );
        assert_eq!(store.local_watch_status().unwrap().tail, 0);
        store
            .finish_lane_commit(completions[0], true)
            .await
            .unwrap();
        later_finish.await.unwrap().unwrap();

        let status = store.local_watch_status().unwrap();
        assert_eq!((status.tail, status.settled_through), (2, 2));
        let page = store
            .scan_watch_page(
                &WatchScope::new("tenant", "bucket", "").unwrap(),
                WatchCursor::new(0),
                8,
            )
            .await
            .unwrap();
        assert_eq!(page.invalidations.len(), 2);
        assert_eq!(page.checkpoint.offset(), 2);
    }

    #[tokio::test]
    async fn projection_failure_preserves_runtime_and_retry_advances_retention_authority() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let change = LocalChange::sequence_gap(1);
        let encoded = encode_local_change(&change).unwrap();
        let completion = {
            let mut runtime = store.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_mut().unwrap();
            let mut watch = runtime.reserved_watch;
            watch.tail = 1;
            watch.retained_entries = 1;
            watch.retained_bytes = invalidation_record_bytes(encoded.len());
            let receipts = runtime.reserved_receipts;
            runtime.reserve(watch, receipts, None).unwrap()
        };
        let mut batch = WriteBatch::default();
        batch.put_cf(
            store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
            invalidation_key(1),
            encoded,
        );
        store.stage_lane_completion(&mut batch, completion).unwrap();
        store.db.write(batch).unwrap();

        store
            .mutation_commit_lanes
            .fail_next_projection
            .store(true, Ordering::Release);
        assert!(store.finish_lane_commit(completion, true).await.is_err());
        {
            let runtime = store.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_ref().unwrap();
            assert_eq!(runtime.projected_ticket, 0);
            assert!(runtime.completions.contains_key(&completion.ticket));
        }
        assert_eq!(store.local_watch_status().unwrap().tail, 0);
        assert!(
            store
                .db
                .get_cf(store.cf(CF_METADATA).unwrap(), completion.key())
                .unwrap()
                .is_some()
        );

        let retry = store.project_lane_completions().await.unwrap();
        assert_eq!(retry.completions, 1);
        assert!(retry.write > Duration::ZERO);
        assert_eq!(store.local_watch_status().unwrap().tail, 1);
        assert_eq!(
            store
                .source_journal_reference_safe_through
                .load(Ordering::Acquire),
            1
        );
        assert!(
            store
                .db
                .get_cf(store.cf(CF_METADATA).unwrap(), completion.key())
                .unwrap()
                .is_none()
        );
        assert!(store.prune_source_journal_for_capacity().await.unwrap());
        assert_eq!(store.local_watch_status().unwrap().retention_floor, 1);
    }

    #[tokio::test]
    async fn completion_disambiguation_read_failure_retries_without_losing_the_ticket() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let completion = {
            let mut runtime = store.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_mut().unwrap();
            let watch = runtime.reserved_watch;
            let receipts = runtime.reserved_receipts;
            runtime.reserve(watch, receipts, None).unwrap()
        };
        let mut batch = WriteBatch::default();
        store.stage_lane_completion(&mut batch, completion).unwrap();
        store.db.write(batch).unwrap();
        store
            .mutation_commit_lanes
            .fail_next_completion_disambiguation
            .store(true, Ordering::Release);

        store
            .finish_lane_commit_cancellation_safe(completion, false)
            .await
            .unwrap();

        let runtime = store.mutation_commit_lanes.sequence().await;
        let runtime = runtime.as_ref().unwrap();
        assert_eq!(runtime.projected_ticket, completion.ticket);
        assert!(!runtime.completions.contains_key(&completion.ticket));
        assert!(
            store
                .db
                .get_cf(store.cf(CF_METADATA).unwrap(), completion.key())
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn projection_write_releases_sequence_and_merges_newer_runtime_state() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let first = {
            let mut runtime = store.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_mut().unwrap();
            let watch = runtime.reserved_watch;
            runtime
                .reserve(
                    watch,
                    MutationReceiptStatus {
                        entries: 1,
                        bytes: 10,
                    },
                    Some(VersionId(1)),
                )
                .unwrap()
        };
        let mut batch = WriteBatch::default();
        store.stage_lane_completion(&mut batch, first).unwrap();
        store.db.write(batch).unwrap();
        store
            .mutation_commit_lanes
            .pause_next_projection
            .store(true, Ordering::Release);
        let finishing_first = tokio::spawn({
            let store = store.clone();
            async move { store.finish_lane_commit(first, true).await }
        });
        store
            .mutation_commit_lanes
            .projection_write_completed
            .acquire()
            .await
            .unwrap()
            .forget();

        let second = {
            let mut runtime = store.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_mut().unwrap();
            let watch = runtime.reserved_watch;
            let completion = runtime
                .reserve(
                    watch,
                    MutationReceiptStatus {
                        entries: 2,
                        bytes: 30,
                    },
                    Some(VersionId(2)),
                )
                .unwrap();
            assert!(
                runtime
                    .completions
                    .insert(
                        completion.ticket,
                        LaneCompletionState::Committed(completion)
                    )
                    .is_none()
            );
            completion
        };
        let mut batch = WriteBatch::default();
        store.stage_lane_completion(&mut batch, second).unwrap();
        store.db.write(batch).unwrap();
        store
            .mutation_commit_lanes
            .projection_publish_continue
            .add_permits(1);
        finishing_first.await.unwrap().unwrap();

        {
            let runtime = store.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_ref().unwrap();
            assert_eq!((runtime.projected_ticket, runtime.next_ticket), (1, 2));
            assert_eq!(
                runtime.projected_receipts,
                MutationReceiptStatus {
                    entries: 1,
                    bytes: 10,
                }
            );
            assert_eq!(
                runtime.reserved_receipts,
                MutationReceiptStatus {
                    entries: 2,
                    bytes: 30,
                }
            );
            assert_eq!(
                runtime.completions.get(&second.ticket),
                Some(&LaneCompletionState::Committed(second))
            );
        }

        let projection = store.project_lane_completions().await.unwrap();
        assert_eq!(projection.completions, 1);
        let runtime = store.mutation_commit_lanes.sequence().await;
        let runtime = runtime.as_ref().unwrap();
        assert_eq!((runtime.projected_ticket, runtime.next_ticket), (2, 2));
        assert_eq!(runtime.projected_receipts, runtime.reserved_receipts);
        assert!(runtime.completions.is_empty());
    }

    #[tokio::test]
    async fn durable_projection_ahead_of_runtime_does_not_reject_a_lane_reservation() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
            policy: store.bucket_policy("tenant", "bucket").unwrap(),
        };
        let change = LocalChange::sequence_gap(1);
        let encoded = encode_local_change(&change).unwrap();
        let (completion, source_id) = {
            let mut runtime = store.mutation_commit_lanes.sequence().await;
            let runtime = runtime.as_mut().unwrap();
            let mut watch = runtime.reserved_watch;
            let source_id = watch.source_id;
            watch.tail = 1;
            watch.retained_entries = 1;
            watch.retained_bytes = invalidation_record_bytes(encoded.len());
            let receipts = runtime.reserved_receipts;
            (runtime.reserve(watch, receipts, None).unwrap(), source_id)
        };
        let mut batch = WriteBatch::default();
        batch.put_cf(
            store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
            invalidation_key(1),
            encoded,
        );
        store.stage_lane_completion(&mut batch, completion).unwrap();
        store.db.write(batch).unwrap();
        store
            .mutation_commit_lanes
            .pause_next_projection
            .store(true, Ordering::Release);
        let finishing_projection = tokio::spawn({
            let store = store.clone();
            async move { store.finish_lane_commit(completion, true).await }
        });
        store
            .mutation_commit_lanes
            .projection_write_completed
            .acquire()
            .await
            .unwrap()
            .forget();
        assert_eq!(store.reference_delta_cursor(source_id).unwrap(), 1);
        {
            let runtime = store.mutation_commit_lanes.sequence().await;
            assert_eq!(runtime.as_ref().unwrap().projected_watch.tail, 0);
        }

        let group = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .coordinate_single_node_mutation_batch(
                        vec![(
                            BatchOperation::Put(PutRequest {
                                key: ObjectKey::new("tenant", "bucket", "objects/next").unwrap(),
                                bytes: b"next".to_vec(),
                                content_type: Some("application/octet-stream".into()),
                                mode: PutMode::PutIfAbsent,
                                command_id: Some("next-command".into()),
                                durability: Durability::Local,
                            }),
                            governance,
                            None,
                        )],
                        ObjectMutationContext {
                            active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                            serving_fence_term: 1,
                        },
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let next_ticket = store
                    .mutation_commit_lanes
                    .sequence()
                    .await
                    .as_ref()
                    .unwrap()
                    .next_ticket;
                if next_ticket == 2 {
                    break;
                }
                assert!(
                    !group.is_finished(),
                    "the lane group failed before reserving its source range"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the lane group reserves while volatile projection publication is paused");
        assert!(
            !group.is_finished(),
            "the lane group still waits for ordered projection acknowledgement"
        );

        store
            .mutation_commit_lanes
            .projection_publish_continue
            .add_permits(1);
        finishing_projection.await.unwrap().unwrap();
        let outcomes = group.await.unwrap().unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].is_ok());
    }

    #[tokio::test]
    async fn restart_closes_abandoned_source_gap_before_later_completion() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let later = LocalChange::sequence_gap(2);
        let encoded = encode_local_change(&later).unwrap();
        let completion = LaneCompletion {
            ticket: 2,
            first_offset: 2,
            last_offset: 2,
            journal_entries: 1,
            journal_bytes: invalidation_record_bytes(encoded.len()),
            receipt_entries: 0,
            receipt_bytes: 0,
            high_version: None,
            reference_cursor_advanced: true,
            inline_reference_safe: true,
            visibility_settled: true,
        };
        let mut batch = WriteBatch::default();
        batch.put_cf(
            store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
            invalidation_key(2),
            encoded,
        );
        store.stage_lane_completion(&mut batch, completion).unwrap();
        store.db.write(batch).unwrap();
        drop(store);

        let recovered = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let status = recovered.local_watch_status().unwrap();
        assert_eq!((status.tail, status.settled_through), (2, 2));
        assert_eq!(
            recovered.reference_delta_cursor(status.source_id).unwrap(),
            2
        );
        let gap = recovered
            .db
            .get_cf(
                recovered.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
                invalidation_key(1),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            crate::watch::decode_local_change(&gap).unwrap(),
            LocalChange::sequence_gap(1)
        );
    }

    #[tokio::test]
    async fn shared_resource_excludes_a_second_lane() {
        let lanes = MutationCommitLanes::new(4);
        let first = lanes.acquire([b"path:a".to_vec()]).await;
        let waiting = tokio::spawn({
            let lanes = lanes.clone();
            async move { lanes.acquire([b"path:a".to_vec()]).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(first);
        waiting.await.unwrap();
    }

    #[tokio::test]
    async fn physical_slot_metrics_track_current_and_peak_utilization() {
        let lanes = MutationCommitLanes::new(2);
        let first = lanes.acquire([b"path:a".to_vec()]).await;
        assert_eq!(first.physical_slots_active_at_acquire(), 1);
        assert_eq!(first.physical_slots_peak_since_start_at_acquire(), 1);
        assert_eq!(first.physical_slot_count(), 2);

        let second = lanes.acquire([b"path:b".to_vec()]).await;
        assert_eq!(second.physical_slots_active_at_acquire(), 2);
        assert_eq!(second.physical_slots_peak_since_start_at_acquire(), 2);
        assert_eq!(first.physical_slots_active(), 2);
        assert_eq!(first.physical_slots_peak_since_start(), 2);
        drop(second);
        assert_eq!(first.physical_slots_active(), 1);
        drop(first);
        assert_eq!(lanes.physical_slots_active.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn physical_slot_can_be_released_without_releasing_conflict_guard() {
        let lanes = MutationCommitLanes::new(1);
        let mut first = lanes.acquire([b"path:a".to_vec()]).await;
        let conflicting = tokio::spawn({
            let lanes = lanes.clone();
            async move { lanes.acquire([b"path:a".to_vec()]).await }
        });
        let independent = tokio::spawn({
            let lanes = lanes.clone();
            async move { lanes.acquire([b"path:b".to_vec()]).await }
        });
        tokio::task::yield_now().await;
        assert!(!conflicting.is_finished());
        assert!(!independent.is_finished());

        first.release_physical_slot();
        let independent = tokio::time::timeout(Duration::from_secs(1), independent)
            .await
            .expect("an independent lane can use the released physical slot")
            .unwrap();
        assert!(
            !conflicting.is_finished(),
            "releasing the physical slot must retain the conflict guard"
        );

        drop(first);
        drop(independent);
        tokio::time::timeout(Duration::from_secs(1), conflicting)
            .await
            .expect("the conflict guard is released when the lane guard drops")
            .unwrap();
        assert_eq!(lanes.physical_slots_active.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn exclusive_fence_waits_for_lane_and_blocks_new_lanes() {
        let lanes = MutationCommitLanes::new(2);
        let first = lanes.acquire([b"path:a".to_vec()]).await;
        let exclusive = tokio::spawn({
            let lanes = lanes.clone();
            async move { lanes.acquire_exclusive().await }
        });
        tokio::task::yield_now().await;
        assert!(!exclusive.is_finished());
        drop(first);
        let exclusive = exclusive.await.unwrap();
        let waiting = tokio::spawn({
            let lanes = lanes.clone();
            async move { lanes.acquire([b"path:b".to_vec()]).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(exclusive);
        waiting.await.unwrap();
    }
}
