//! One fair hard ceiling for accounted index heap memory.
//!
//! The v6 pipeline holds its configured share for its lifetime. Queries use
//! the remaining capacity with FIFO admission. The process-wide ceiling is
//! never exceeded.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use thiserror::Error;
use tokio::sync::Notify;

use crate::index_config::IndexRuntimeConfig;

const ACCOUNT_COUNT: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkingMemoryAccount {
    Query,
    IndexingPipeline,
}

impl WorkingMemoryAccount {
    const fn slot(self) -> usize {
        match self {
            Self::Query => 0,
            Self::IndexingPipeline => 1,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::IndexingPipeline => "indexing_pipeline",
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
}

#[derive(Default)]
struct WorkingMemoryState {
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

    pub(crate) fn new(
        hard_limit: u64,
        shares: [u64; ACCOUNT_COUNT],
    ) -> Result<Self, WorkingMemoryError> {
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
                shares,
                state: Mutex::new(WorkingMemoryState::default()),
                changed: Notify::new(),
            }),
        })
    }

    pub(crate) fn hard_limit(&self) -> u64 {
        self.inner.hard_limit
    }

    pub(crate) fn share(&self, account: WorkingMemoryAccount) -> u64 {
        self.inner.shares[account.slot()]
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
                });
                let free = self.inner.hard_limit.saturating_sub(state.used);
                let available = match account {
                    WorkingMemoryAccount::Query => free,
                    WorkingMemoryAccount::IndexingPipeline => free.saturating_sub(
                        self.inner.shares[WorkingMemoryAccount::Query.slot()]
                            .saturating_sub(state.account_used[WorkingMemoryAccount::Query.slot()]),
                    ),
                };
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
        let mut state = lock_state(&self.inner);
        debug_assert!(state.used >= self.bytes);
        debug_assert!(state.account_used[self.account.slot()] >= self.bytes);
        state.used = state.used.saturating_sub(self.bytes);
        state.account_used[self.account.slot()] =
            state.account_used[self.account.slot()].saturating_sub(self.bytes);
        emit_state(&self.inner, &state, self.account);
        drop(state);
        self.inner.changed.notify_waiters();
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
