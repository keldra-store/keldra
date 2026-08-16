//! One fair hard ceiling for accounted index heap memory.
//!
//! Query and construction settings remain fair-share planning targets. Idle
//! shares may be borrowed for optional work, but queued mandatory work always
//! has FIFO priority and the process-wide ceiling is never exceeded. This pool
//! intentionally excludes mmap cache, tmpfs pages, RocksDB and ordinary runtime
//! allocations.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use anvil_index::IndexKind;
use thiserror::Error;
use tokio::sync::Notify;

use crate::index_config::IndexRuntimeConfig;

const ACCOUNT_COUNT: usize = 9;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkingMemoryAccount {
    Query,
    Builder(IndexKind),
}

impl WorkingMemoryAccount {
    const fn slot(self) -> usize {
        match self {
            Self::Query => 0,
            Self::Builder(kind) => kind as u8 as usize,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Builder(IndexKind::Path) => "path",
            Self::Builder(IndexKind::MetadataFilter) => "metadata_filter",
            Self::Builder(IndexKind::TypedJson) => "typed_json",
            Self::Builder(IndexKind::FullText) => "full_text",
            Self::Builder(IndexKind::Vector) => "vector",
            Self::Builder(IndexKind::Hybrid) => "hybrid",
            Self::Builder(IndexKind::GitSource) => "git_source",
            Self::Builder(IndexKind::Tensor) => "tensor",
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
        let mut shares = [0; ACCOUNT_COUNT];
        shares[WorkingMemoryAccount::Query.slot()] = config.query_memory_bytes();
        for kind in all_kinds() {
            shares[WorkingMemoryAccount::Builder(kind).slot()] = config.builder_memory_bytes(kind);
        }
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

    /// Wait for `minimum` bytes in global FIFO order, then take immediately
    /// idle bytes up to `preferred`. Optional bytes are never granted while a
    /// mandatory request is already queued.
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
                let first = state
                    .waiters
                    .front()
                    .is_some_and(|waiter| waiter.ticket == ticket);
                let free = self.inner.hard_limit.saturating_sub(state.used);
                if first && free >= minimum {
                    // Existing mandatory waiters get priority over optional
                    // borrowing. If this is the only waiter, all currently idle
                    // bytes are safely available until this bounded permit ends.
                    let no_other_waiter = state.waiters.len() == 1;
                    let bytes = if no_other_waiter {
                        preferred.min(free).max(minimum)
                    } else {
                        minimum
                    };
                    state.waiters.pop_front();
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
    tracing::info!(
        memory.class = account.label(),
        gauge.anvil_index_working_memory_configured_bytes = inner.hard_limit,
        gauge.anvil_index_working_memory_used_bytes = state.used,
        gauge.anvil_index_working_memory_peak_bytes = state.peak,
        gauge.anvil_index_working_memory_share_bytes = account_share,
        gauge.anvil_index_working_memory_class_used_bytes = account_used,
        gauge.anvil_index_working_memory_borrowed_bytes = borrowed,
        gauge.anvil_index_working_memory_waiting = waiting,
        gauge.anvil_index_working_memory_waiting_bytes = waiting_bytes,
        "index working-memory budget state"
    );
    if let WorkingMemoryAccount::Builder(kind) = account {
        tracing::info!(
            index.kind = ?kind,
            gauge.anvil_index_construction_configured_bytes = account_share,
            gauge.anvil_index_construction_leased_bytes = account_used,
            gauge.anvil_index_construction_peak_leased_bytes = state.account_peak[account.slot()],
            gauge.anvil_index_construction_waiting = waiting,
            "index construction budget state"
        );
    }
}

const fn all_kinds() -> [IndexKind; 8] {
    [
        IndexKind::Path,
        IndexKind::MetadataFilter,
        IndexKind::TypedJson,
        IndexKind::FullText,
        IndexKind::Vector,
        IndexKind::Hybrid,
        IndexKind::GitSource,
        IndexKind::Tensor,
    ]
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

    fn pool(limit: u64, query_share: u64, builder_share: u64) -> IndexWorkingMemory {
        let mut shares = [builder_share; ACCOUNT_COUNT];
        shares[WorkingMemoryAccount::Query.slot()] = query_share;
        IndexWorkingMemory::new(limit, shares).unwrap()
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
    async fn queued_mandatory_work_is_not_bypassed_by_optional_borrowing() {
        let memory = pool(100, 50, 40);
        let held = memory
            .acquire_up_to(WorkingMemoryAccount::Query, 50, 90)
            .await
            .unwrap();
        let builder_memory = memory.clone();
        let builder = tokio::spawn(async move {
            builder_memory
                .acquire_up_to(WorkingMemoryAccount::Builder(IndexKind::Path), 40, 40)
                .await
                .unwrap()
        });
        tokio::task::yield_now().await;
        assert!(!builder.is_finished());
        let query_memory = memory.clone();
        let query = tokio::spawn(async move {
            query_memory
                .acquire_up_to(WorkingMemoryAccount::Query, 20, 60)
                .await
                .unwrap()
        });
        tokio::task::yield_now().await;
        assert!(!query.is_finished());
        drop(held);
        let builder = builder.await.unwrap();
        assert_eq!(builder.bytes(), 40);
        let query = query.await.unwrap();
        assert_eq!(query.bytes(), 60);
        assert_eq!(memory.used(), 100);
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
