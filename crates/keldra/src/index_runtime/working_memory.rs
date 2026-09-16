//! One fair hard ceiling for accounted index heap memory.
//!
//! Pipeline stages charge actual owned allocations through the existing credit
//! backend. Queries use FIFO admission; disposable caches borrow idle capacity
//! and are reclaimed before mandatory admission. Every live allocation owner
//! retains its permit after cache eviction.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};

use thiserror::Error;
use tokio::sync::Notify;

use crate::index_config::IndexRuntimeConfig;

const ACCOUNT_COUNT: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkingMemoryAccount {
    Query,
    IndexingPipeline,
    ReusableCache,
    Sealing,
}

impl WorkingMemoryAccount {
    const fn slot(self) -> usize {
        match self {
            Self::Query => 0,
            Self::IndexingPipeline => 1,
            Self::ReusableCache => 2,
            Self::Sealing => 3,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::IndexingPipeline => "indexing_pipeline",
            Self::ReusableCache => "reusable_cache",
            Self::Sealing => "sealing",
        }
    }
}

#[derive(Clone)]
pub(crate) struct IndexWorkingMemory {
    inner: Arc<WorkingMemoryInner>,
}

struct WorkingMemoryInner {
    hard_limit: u64,
    shares: [u64; ACCOUNT_COUNT],
    state: Mutex<WorkingMemoryState>,
    changed: Notify,
    reclaimers: Mutex<Vec<Weak<dyn WorkingMemoryReclaimer>>>,
}

pub(crate) trait WorkingMemoryReclaimer: Send + Sync {
    fn reclaim(&self, needed_bytes: u64);
}

#[derive(Default)]
struct WorkingMemoryState {
    sealing_promised: u64,
    used: u64,
    peak: u64,
    account_used: [u64; ACCOUNT_COUNT],
    account_peak: [u64; ACCOUNT_COUNT],
    next_ticket: u64,
    waiters: VecDeque<WorkingMemoryWaiter>,
}

#[derive(Clone, Copy)]
struct WorkingMemoryWaiter {
    ticket: u64,
    account: WorkingMemoryAccount,
    minimum: u64,
}

impl IndexWorkingMemory {
    pub(crate) fn from_config(config: IndexRuntimeConfig) -> Result<Self, WorkingMemoryError> {
        let shares = [config.query_memory_bytes(), config.pipeline_memory_bytes()];
        Self::new(
            config
                .working_memory_bytes()
                .map_err(|error| WorkingMemoryError::InvalidConfig(error.to_string()))?,
            shares,
        )
    }

    pub(crate) fn new(hard_limit: u64, shares: [u64; 2]) -> Result<Self, WorkingMemoryError> {
        if hard_limit == 0 || shares.contains(&0) {
            return Err(WorkingMemoryError::ZeroLimit);
        }
        let largest_share = shares.iter().copied().max().unwrap_or(0);
        if largest_share > hard_limit {
            return Err(WorkingMemoryError::ShareExceedsLimit {
                share: largest_share,
                limit: hard_limit,
            });
        }
        Ok(Self {
            inner: Arc::new(WorkingMemoryInner {
                hard_limit,
                shares: [shares[0], shares[1], 0, 0],
                state: Mutex::new(WorkingMemoryState::default()),
                changed: Notify::new(),
                reclaimers: Mutex::new(Vec::new()),
            }),
        })
    }

    pub(crate) fn hard_limit(&self) -> u64 {
        self.inner.hard_limit
    }

    /// Range-attempt buffers use the same aggregate authority as their caller.
    /// The lease may travel into a blocking local read, retaining its charge
    /// even after the requesting async task has been cancelled.
    pub(crate) fn payload_range_memory(
        &self,
        account: WorkingMemoryAccount,
    ) -> Arc<dyn crate::payload_read::PayloadRangeMemory> {
        Arc::new(SharedPayloadRangeMemory {
            memory: self.clone(),
            account,
        })
    }

    pub(crate) fn share(&self, account: WorkingMemoryAccount) -> u64 {
        self.inner.shares[account.slot()]
    }

    pub(crate) fn free_bytes(&self) -> u64 {
        self.inner
            .hard_limit
            .saturating_sub(lock_state(&self.inner).used)
    }

    pub(crate) fn register_reclaimer(&self, reclaimer: Weak<dyn WorkingMemoryReclaimer>) {
        self.inner
            .reclaimers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(reclaimer);
    }

    fn reclaim_for_account(&self, account: WorkingMemoryAccount, minimum: u64) {
        let needed = {
            let state = lock_state(&self.inner);
            state.account_used[WorkingMemoryAccount::ReusableCache.slot()] != 0
                && available_for(&self.inner, &state, account) < minimum
        };
        if needed {
            let reclaimers = self
                .inner
                .reclaimers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            for reclaimer in reclaimers {
                let shortfall = {
                    let state = lock_state(&self.inner);
                    minimum.saturating_sub(available_for(&self.inner, &state, account))
                };
                if shortfall == 0 {
                    break;
                }
                reclaimer.reclaim(shortfall);
            }
        }
    }

    /// Nonblocking admission for allocations whose owner already has bounded
    /// replay/eviction semantics. The stage permit is the RAII owner; this
    /// method and `release_charge` are its storage-neutral backend bridge.
    pub(crate) fn try_charge(&self, account: WorkingMemoryAccount, bytes: u64) -> Result<(), u64> {
        if account != WorkingMemoryAccount::ReusableCache {
            self.reclaim_for_account(account, bytes);
        }
        let mut state = lock_state(&self.inner);
        let available = available_for(&self.inner, &state, account);
        if bytes > available
            || (account != WorkingMemoryAccount::Sealing
                && state
                    .waiters
                    .iter()
                    .any(|waiter| waiter.account == WorkingMemoryAccount::Query))
        {
            return Err(available);
        }
        state.used += bytes;
        state.peak = state.peak.max(state.used);
        state.account_used[account.slot()] += bytes;
        state.account_peak[account.slot()] =
            state.account_peak[account.slot()].max(state.account_used[account.slot()]);
        emit_state(&self.inner, &state, account);
        Ok(())
    }

    pub(crate) fn release_charge(&self, account: WorkingMemoryAccount, bytes: u64) {
        release_charge(&self.inner, account, bytes);
    }

    fn try_reserve_progress(&self, bytes: u64) -> Result<(), u64> {
        self.reclaim_for_account(WorkingMemoryAccount::IndexingPipeline, bytes);
        let mut state = lock_state(&self.inner);
        let available = available_for(&self.inner, &state, WorkingMemoryAccount::IndexingPipeline)
            .saturating_add(
                state.account_used[WorkingMemoryAccount::Sealing.slot()]
                    .saturating_sub(state.sealing_promised),
            );
        if bytes > available {
            return Err(available);
        }
        state.sealing_promised = state.sealing_promised.checked_add(bytes).ok_or(available)?;
        emit_state(&self.inner, &state, WorkingMemoryAccount::Sealing);
        Ok(())
    }

    fn release_progress(&self, bytes: u64) {
        let mut state = lock_state(&self.inner);
        debug_assert!(state.sealing_promised >= bytes);
        state.sealing_promised = state.sealing_promised.saturating_sub(bytes);
        emit_state(&self.inner, &state, WorkingMemoryAccount::Sealing);
        drop(state);
        self.inner.changed.notify_waiters();
    }

    fn transfer_charge(
        &self,
        source: WorkingMemoryAccount,
        destination: WorkingMemoryAccount,
        bytes: u64,
    ) -> Result<(), u64> {
        if source == destination {
            return Ok(());
        }
        let mut state = lock_state(&self.inner);
        let previous = state
            .sealing_promised
            .saturating_sub(state.account_used[WorkingMemoryAccount::Sealing.slot()]);
        let after_sealing = if source == WorkingMemoryAccount::Sealing {
            state.account_used[WorkingMemoryAccount::Sealing.slot()].saturating_sub(bytes)
        } else if destination == WorkingMemoryAccount::Sealing {
            state.account_used[WorkingMemoryAccount::Sealing.slot()].saturating_add(bytes)
        } else {
            state.account_used[WorkingMemoryAccount::Sealing.slot()]
        };
        let after = state.sealing_promised.saturating_sub(after_sealing);
        let additional = after.saturating_sub(previous);
        let available = self
            .inner
            .hard_limit
            .saturating_sub(state.used)
            .saturating_sub(previous);
        if additional > available {
            return Err(available);
        }
        debug_assert!(state.account_used[source.slot()] >= bytes);
        state.account_used[source.slot()] = state.account_used[source.slot()].saturating_sub(bytes);
        state.account_used[destination.slot()] += bytes;
        state.account_peak[destination.slot()] =
            state.account_peak[destination.slot()].max(state.account_used[destination.slot()]);
        emit_state(&self.inner, &state, destination);
        Ok(())
    }

    pub(crate) fn try_acquire(
        &self,
        account: WorkingMemoryAccount,
        bytes: u64,
    ) -> Option<WorkingMemoryPermit> {
        self.try_charge(account, bytes).ok()?;
        Some(WorkingMemoryPermit {
            inner: self.inner.clone(),
            account,
            bytes,
        })
    }

    /// Wait for `minimum` bytes with query priority and per-class FIFO, then
    /// take immediately idle bytes up to `preferred`. Builders cannot consume
    /// the reserved query share.
    pub(crate) async fn acquire_up_to(
        &self,
        account: WorkingMemoryAccount,
        minimum: u64,
        preferred: u64,
    ) -> Result<WorkingMemoryPermit, WorkingMemoryError> {
        if minimum == 0 || minimum > preferred || minimum > self.inner.hard_limit {
            return Err(WorkingMemoryError::MandatoryRequestExceedsLimit {
                requested: minimum,
                limit: self.inner.hard_limit,
            });
        }
        let preferred = preferred.min(self.inner.hard_limit);
        let ticket = {
            let mut state = lock_state(&self.inner);
            let ticket = state.next_ticket;
            state.next_ticket = state.next_ticket.wrapping_add(1);
            state.waiters.push_back(WorkingMemoryWaiter {
                ticket,
                account,
                minimum,
            });
            emit_state(&self.inner, &state, account);
            ticket
        };
        let mut queued = QueuedRequest {
            inner: self.inner.clone(),
            ticket: Some(ticket),
            account,
        };
        loop {
            let changed = self.inner.changed.notified();
            if account == WorkingMemoryAccount::Query {
                self.reclaim_for_account(account, minimum);
            }
            let granted = {
                let mut state = lock_state(&self.inner);
                let waiter_index = state
                    .waiters
                    .iter()
                    .position(|waiter| waiter.ticket == ticket);
                let eligible = waiter_index.is_some_and(|index| match account {
                    // Queries retain FIFO ordering with each other but bypass
                    // queued background work so an idle query reservation can
                    // never be head-of-line blocked by a builder.
                    WorkingMemoryAccount::Query => !state
                        .waiters
                        .iter()
                        .take(index)
                        .any(|waiter| waiter.account == WorkingMemoryAccount::Query),
                    WorkingMemoryAccount::IndexingPipeline => {
                        index == 0
                            && !state
                                .waiters
                                .iter()
                                .any(|waiter| waiter.account == WorkingMemoryAccount::Query)
                    }
                    WorkingMemoryAccount::ReusableCache => false,
                    WorkingMemoryAccount::Sealing => index == 0,
                });
                let available = available_for(&self.inner, &state, account);
                if eligible && available >= minimum {
                    // Existing mandatory waiters get priority over optional
                    // borrowing. If this is the only waiter, all currently idle
                    // bytes are safely available until this bounded permit ends.
                    let no_other_waiter = state.waiters.len() == 1;
                    let bytes = if no_other_waiter {
                        preferred.min(available).max(minimum)
                    } else {
                        minimum
                    };
                    state
                        .waiters
                        .remove(waiter_index.expect("eligible waiter remains queued"));
                    state.used += bytes;
                    state.peak = state.peak.max(state.used);
                    let account_slot = account.slot();
                    state.account_used[account_slot] += bytes;
                    let account_used = state.account_used[account_slot];
                    state.account_peak[account_slot] =
                        state.account_peak[account_slot].max(account_used);
                    emit_state(&self.inner, &state, account);
                    Some(bytes)
                } else {
                    None
                }
            };
            if let Some(bytes) = granted {
                queued.ticket = None;
                self.inner.changed.notify_waiters();
                return Ok(WorkingMemoryPermit {
                    inner: self.inner.clone(),
                    account,
                    bytes,
                });
            }
            changed.await;
        }
    }

    #[cfg(test)]
    fn used(&self) -> u64 {
        lock_state(&self.inner).used
    }

    #[cfg(test)]
    pub(crate) fn available(&self) -> u64 {
        self.inner.hard_limit.saturating_sub(self.used())
    }

    #[cfg(test)]
    pub(crate) fn waiting(&self, account: WorkingMemoryAccount) -> usize {
        lock_state(&self.inner)
            .waiters
            .iter()
            .filter(|waiter| waiter.account == account)
            .count()
    }
}

fn available_for(
    inner: &WorkingMemoryInner,
    state: &WorkingMemoryState,
    account: WorkingMemoryAccount,
) -> u64 {
    let free = inner.hard_limit.saturating_sub(state.used);
    let promised = state
        .sealing_promised
        .saturating_sub(state.account_used[WorkingMemoryAccount::Sealing.slot()]);
    match account {
        WorkingMemoryAccount::Query => free.saturating_sub(promised),
        WorkingMemoryAccount::IndexingPipeline => free.saturating_sub(promised).saturating_sub(
            inner.shares[WorkingMemoryAccount::Query.slot()]
                .saturating_sub(state.account_used[WorkingMemoryAccount::Query.slot()]),
        ),
        // Reusable entries borrow idle bytes; mandatory admissions reclaim
        // them first. Active readers keep their charge after eviction.
        WorkingMemoryAccount::ReusableCache => free.saturating_sub(promised),
        WorkingMemoryAccount::Sealing => free.saturating_sub(
            inner.shares[WorkingMemoryAccount::Query.slot()]
                .saturating_sub(state.account_used[WorkingMemoryAccount::Query.slot()]),
        ),
    }
}

fn release_charge(inner: &WorkingMemoryInner, account: WorkingMemoryAccount, bytes: u64) {
    let mut state = lock_state(inner);
    debug_assert!(state.account_used[account.slot()] >= bytes);
    state.used = state.used.saturating_sub(bytes);
    state.account_used[account.slot()] = state.account_used[account.slot()].saturating_sub(bytes);
    emit_state(inner, &state, account);
    drop(state);
    inner.changed.notify_waiters();
}

#[derive(Clone)]
pub(crate) struct SharedIndexingMemoryBackend(pub(crate) IndexWorkingMemory);

struct SharedPayloadRangeMemory {
    memory: IndexWorkingMemory,
    account: WorkingMemoryAccount,
}

impl crate::payload_read::PayloadRangeMemory for SharedPayloadRangeMemory {
    fn try_reserve(&self, bytes: usize) -> Result<Box<dyn Send + Sync>, tonic::Status> {
        let bytes = u64::try_from(bytes).map_err(|_| {
            tonic::Status::resource_exhausted("payload range scratch exceeds platform capacity")
        })?;
        let permit = self
            .memory
            .try_acquire(self.account, bytes)
            .ok_or_else(|| {
                tonic::Status::resource_exhausted(
                    "payload range scratch exceeds available shared working memory",
                )
            })?;
        Ok(Box::new(permit))
    }
}

impl std::fmt::Debug for SharedIndexingMemoryBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedIndexingMemoryBackend")
            .field("hard_limit", &self.0.hard_limit())
            .finish()
    }
}

impl keldra_index::v1::IndexingMemoryBackend for SharedIndexingMemoryBackend {
    fn try_reserve(&self, bytes: usize) -> Result<(), usize> {
        self.0
            .try_charge(WorkingMemoryAccount::IndexingPipeline, bytes as u64)
            .map_err(|available| usize::try_from(available).unwrap_or(usize::MAX))
    }
    fn release(&self, bytes: usize) {
        self.0
            .release_charge(WorkingMemoryAccount::IndexingPipeline, bytes as u64);
    }
    fn try_reserve_stage(
        &self,
        stage: keldra_index::v1::IndexingMemoryStage,
        bytes: usize,
    ) -> Result<(), usize> {
        self.0
            .try_charge(stage_account(stage), bytes as u64)
            .map_err(|available| usize::try_from(available).unwrap_or(usize::MAX))
    }
    fn release_stage(&self, stage: keldra_index::v1::IndexingMemoryStage, bytes: usize) {
        self.0.release_charge(stage_account(stage), bytes as u64);
    }
    fn transfer_stage(
        &self,
        source: keldra_index::v1::IndexingMemoryStage,
        destination: keldra_index::v1::IndexingMemoryStage,
        bytes: usize,
    ) -> Result<(), usize> {
        self.0
            .transfer_charge(
                stage_account(source),
                stage_account(destination),
                bytes as u64,
            )
            .map_err(|available| usize::try_from(available).unwrap_or(usize::MAX))
    }
    fn try_reserve_progress(&self, bytes: usize) -> Result<(), usize> {
        self.0
            .try_reserve_progress(bytes as u64)
            .map_err(|available| usize::try_from(available).unwrap_or(usize::MAX))
    }
    fn release_progress(&self, bytes: usize) {
        self.0.release_progress(bytes as u64);
    }
}

fn stage_account(stage: keldra_index::v1::IndexingMemoryStage) -> WorkingMemoryAccount {
    if stage == keldra_index::v1::IndexingMemoryStage::SealScratch {
        WorkingMemoryAccount::Sealing
    } else {
        WorkingMemoryAccount::IndexingPipeline
    }
}

fn lock_state(inner: &WorkingMemoryInner) -> std::sync::MutexGuard<'_, WorkingMemoryState> {
    inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct QueuedRequest {
    inner: Arc<WorkingMemoryInner>,
    ticket: Option<u64>,
    account: WorkingMemoryAccount,
}

impl Drop for QueuedRequest {
    fn drop(&mut self) {
        let Some(ticket) = self.ticket else {
            return;
        };
        let mut state = lock_state(&self.inner);
        if let Some(index) = state
            .waiters
            .iter()
            .position(|waiter| waiter.ticket == ticket)
        {
            state.waiters.remove(index);
        }
        emit_state(&self.inner, &state, self.account);
        drop(state);
        self.inner.changed.notify_waiters();
    }
}

pub(crate) struct WorkingMemoryPermit {
    inner: Arc<WorkingMemoryInner>,
    account: WorkingMemoryAccount,
    bytes: u64,
}

impl WorkingMemoryPermit {
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for WorkingMemoryPermit {
    fn drop(&mut self) {
        release_charge(&self.inner, self.account, self.bytes);
    }
}

fn emit_state(
    inner: &WorkingMemoryInner,
    state: &WorkingMemoryState,
    account: WorkingMemoryAccount,
) {
    let account_used = state.account_used[account.slot()];
    let account_share = inner.shares[account.slot()];
    let waiting = state
        .waiters
        .iter()
        .filter(|waiter| waiter.account == account)
        .count() as u64;
    let waiting_bytes = state
        .waiters
        .iter()
        .filter(|waiter| waiter.account == account)
        .fold(0_u64, |sum, waiter| sum.saturating_add(waiter.minimum));
    let borrowed = account_used.saturating_sub(account_share);
    tracing::debug!(
        memory.class = account.label(),
        gauge.keldra_index_working_memory_configured_bytes = inner.hard_limit,
        gauge.keldra_index_working_memory_used_bytes = state.used,
        gauge.keldra_index_working_memory_peak_bytes = state.peak,
        gauge.keldra_index_working_memory_share_bytes = account_share,
        gauge.keldra_index_working_memory_class_used_bytes = account_used,
        gauge.keldra_index_working_memory_borrowed_bytes = borrowed,
        gauge.keldra_index_working_memory_waiting = waiting,
        gauge.keldra_index_working_memory_waiting_bytes = waiting_bytes,
        gauge.keldra_index_working_memory_sealing_promised_bytes = state.sealing_promised,
        gauge.keldra_index_working_memory_sealing_headroom_bytes = state
            .sealing_promised
            .saturating_sub(state.account_used[WorkingMemoryAccount::Sealing.slot()]),
        "index working-memory budget state"
    );
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum WorkingMemoryError {
    #[error("index working-memory limits must be greater than zero")]
    ZeroLimit,
    #[error("index working-memory fair share {share} exceeds hard limit {limit}")]
    ShareExceedsLimit { share: u64, limit: u64 },
    #[error("index working-memory mandatory request {requested} exceeds hard limit {limit}")]
    MandatoryRequestExceedsLimit { requested: u64, limit: u64 },
    #[error("invalid index working-memory configuration: {0}")]
    InvalidConfig(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(limit: u64, query_share: u64, pipeline_share: u64) -> IndexWorkingMemory {
        IndexWorkingMemory::new(limit, [query_share, pipeline_share]).unwrap()
    }

    #[test]
    fn range_scratch_is_charged_until_last_attempt_owner_drops() {
        let memory = pool(100, 20, 80);
        let ranges = memory.payload_range_memory(WorkingMemoryAccount::Query);
        let lease = Arc::new(ranges.try_reserve(60).unwrap());
        let blocking_owner = lease.clone();
        assert_eq!(memory.free_bytes(), 40);
        assert!(ranges.try_reserve(41).is_err());
        drop(lease);
        assert_eq!(memory.free_bytes(), 40);
        drop(blocking_owner);
        assert_eq!(memory.free_bytes(), 100);
    }

    #[test]
    fn producer_range_scratch_preserves_query_and_sealing_headroom() {
        let memory = pool(100, 20, 80);
        memory.try_reserve_progress(30).unwrap();
        let ranges = memory.payload_range_memory(WorkingMemoryAccount::IndexingPipeline);
        assert!(ranges.try_reserve(51).is_err());
        let lease = ranges.try_reserve(50).unwrap();
        assert_eq!(memory.free_bytes(), 50);
        drop(lease);
        memory.release_progress(30);
    }

    #[test]
    fn retained_cache_borrows_idle_shares_without_crossing_the_hard_ceiling() {
        let memory = pool(100, 20, 30);
        let cache = memory
            .try_acquire(WorkingMemoryAccount::ReusableCache, 50)
            .unwrap();
        assert!(
            memory
                .try_acquire(WorkingMemoryAccount::ReusableCache, 1)
                .is_some()
        );
        let builder = memory
            .try_acquire(WorkingMemoryAccount::IndexingPipeline, 30)
            .unwrap();
        let query = memory.try_acquire(WorkingMemoryAccount::Query, 20).unwrap();
        assert_eq!(memory.used(), 100);
        drop((cache, builder, query));
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn pipeline_backend_reserves_actual_allocations_not_its_lifetime_share() {
        use keldra_index::v1::IndexingMemoryBackend;
        let memory = pool(100, 20, 60);
        let backend = SharedIndexingMemoryBackend(memory.clone());
        backend.try_reserve(10).unwrap();
        assert_eq!(memory.used(), 10);
        assert!(backend.try_reserve(71).is_err());
        assert_eq!(memory.used(), 10);
        backend.release(10);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn promised_seal_headroom_is_reusable_by_sealing_not_other_allocations() {
        let memory = pool(100, 20, 60);
        memory.try_reserve_progress(30).unwrap();
        assert_eq!(
            memory.used(),
            0,
            "future headroom is not preallocated memory"
        );
        assert!(
            memory
                .try_acquire(WorkingMemoryAccount::ReusableCache, 71)
                .is_none()
        );
        assert!(
            memory
                .try_acquire(WorkingMemoryAccount::IndexingPipeline, 51)
                .is_none()
        );
        let seal = memory
            .try_acquire(WorkingMemoryAccount::Sealing, 30)
            .unwrap();
        let builder = memory
            .try_acquire(WorkingMemoryAccount::IndexingPipeline, 50)
            .unwrap();
        let query = memory.try_acquire(WorkingMemoryAccount::Query, 20).unwrap();
        assert_eq!(memory.used(), 100);
        drop((seal, builder, query));
        memory.release_progress(30);
        assert_eq!(memory.used(), 0);
    }

    #[tokio::test]
    async fn optional_work_borrows_idle_capacity_without_crossing_hard_ceiling() {
        let memory = pool(100, 20, 10);
        let permit = memory
            .acquire_up_to(WorkingMemoryAccount::Query, 20, 100)
            .await
            .unwrap();
        assert_eq!(permit.bytes(), 100);
        assert_eq!(memory.used(), 100);
        drop(permit);
        assert_eq!(memory.used(), 0);
    }

    #[tokio::test]
    async fn query_bypasses_a_blocked_pipeline_waiter_without_exceeding_the_ceiling() {
        let memory = pool(12, 4, 8);
        let held = memory
            .acquire_up_to(WorkingMemoryAccount::IndexingPipeline, 8, 8)
            .await
            .unwrap();
        let pipeline_memory = memory.clone();
        let pipeline = tokio::spawn(async move {
            pipeline_memory
                .acquire_up_to(WorkingMemoryAccount::IndexingPipeline, 8, 8)
                .await
                .unwrap()
        });
        tokio::task::yield_now().await;
        assert!(!pipeline.is_finished());
        let query_memory = memory.clone();
        let query = tokio::spawn(async move {
            query_memory
                .acquire_up_to(WorkingMemoryAccount::Query, 4, 4)
                .await
                .unwrap()
        });
        tokio::task::yield_now().await;
        let query = query.await.unwrap();
        assert_eq!(query.bytes(), 4);
        assert_eq!(memory.used(), 12);
        assert!(!pipeline.is_finished());
        drop(query);
        assert!(!pipeline.is_finished());
        drop(held);
        let pipeline = pipeline.await.unwrap();
        assert_eq!(pipeline.bytes(), 8);
        assert_eq!(memory.used(), 8);
    }

    #[tokio::test]
    async fn pipeline_cannot_consume_the_idle_query_reservation() {
        let memory = pool(12, 4, 8);
        let permit = memory
            .acquire_up_to(WorkingMemoryAccount::IndexingPipeline, 8, 12)
            .await
            .unwrap();
        assert_eq!(permit.bytes(), 8);
        assert_eq!(memory.available(), 4);
    }

    #[tokio::test]
    async fn permanent_projection_residency_remains_in_the_hard_parent() {
        let memory = pool(40, 10, 10);
        let projection = memory
            .acquire_up_to(WorkingMemoryAccount::IndexingPipeline, 10, 10)
            .await
            .unwrap();

        assert_eq!(projection.bytes(), 10);
        assert_eq!(memory.used(), 10);
        assert_eq!(memory.available(), 30);
    }

    #[tokio::test]
    async fn mandatory_overflow_can_borrow_beyond_its_class_share() {
        let memory = pool(100, 20, 10);
        let permit = memory
            .acquire_up_to(WorkingMemoryAccount::Query, 30, 30)
            .await
            .unwrap();
        assert_eq!(permit.bytes(), 30);
        assert_eq!(memory.used(), 30);
    }

    #[tokio::test]
    async fn cancelled_front_waiter_does_not_block_fifo_progress() {
        let memory = pool(10, 10, 10);
        let held = memory
            .acquire_up_to(WorkingMemoryAccount::Query, 10, 10)
            .await
            .unwrap();
        let first_memory = memory.clone();
        let first = tokio::spawn(async move {
            first_memory
                .acquire_up_to(WorkingMemoryAccount::Query, 10, 10)
                .await
                .unwrap()
        });
        let second_memory = memory.clone();
        let second = tokio::spawn(async move {
            second_memory
                .acquire_up_to(WorkingMemoryAccount::Query, 1, 1)
                .await
                .unwrap()
        });
        tokio::task::yield_now().await;
        first.abort();
        let _ = first.await;
        drop(held);
        assert_eq!(second.await.unwrap().bytes(), 1);
    }
}
