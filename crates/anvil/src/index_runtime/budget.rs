//! Fair hard byte admission for index building and compaction.
//!
//! Each public index kind owns one process-wide pool. Builders are sequential
//! clients of the pool, so strict FIFO admission gives every definition and
//! compaction pass a turn without retaining an in-memory work queue.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use anvil_index::{IndexKind, MIN_INDEX_KIND_MEMORY_BYTES, SegmentMemoryPlan};
use thiserror::Error;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

/// A small fixed number of pull-driven rebuild streams may remain open per
/// kind. Each can retain only transport backpressure state; every source frame
/// still waits in the shared FIFO byte queue before it is pulled. Three lets
/// the release qualification prove round-robin progress for three same-kind
/// definitions without making open streams scale with definition count.
const MAX_OPEN_SNAPSHOTS_PER_KIND: usize = 3;

#[derive(Clone)]
pub(crate) struct IndexMemoryBudgets {
    path: IndexMemoryBudget,
    metadata_filter: IndexMemoryBudget,
    typed_json: IndexMemoryBudget,
    full_text: IndexMemoryBudget,
    vector: IndexMemoryBudget,
    hybrid: IndexMemoryBudget,
    git_source: IndexMemoryBudget,
    tensor: IndexMemoryBudget,
}

impl IndexMemoryBudgets {
    pub(crate) fn new(bytes_per_kind: u64) -> Result<Self, IndexBudgetError> {
        let plan_bytes = usize::try_from(bytes_per_kind)
            .map_err(|_| IndexBudgetError::LimitExceedsPlatform(bytes_per_kind))?;
        SegmentMemoryPlan::new(plan_bytes).map_err(|_| IndexBudgetError::BelowMinimum {
            configured: bytes_per_kind,
            minimum: MIN_INDEX_KIND_MEMORY_BYTES as u64,
        })?;
        Ok(Self {
            path: IndexMemoryBudget::new(IndexKind::Path, bytes_per_kind)?,
            metadata_filter: IndexMemoryBudget::new(IndexKind::MetadataFilter, bytes_per_kind)?,
            typed_json: IndexMemoryBudget::new(IndexKind::TypedJson, bytes_per_kind)?,
            full_text: IndexMemoryBudget::new(IndexKind::FullText, bytes_per_kind)?,
            vector: IndexMemoryBudget::new(IndexKind::Vector, bytes_per_kind)?,
            hybrid: IndexMemoryBudget::new(IndexKind::Hybrid, bytes_per_kind)?,
            git_source: IndexMemoryBudget::new(IndexKind::GitSource, bytes_per_kind)?,
            tensor: IndexMemoryBudget::new(IndexKind::Tensor, bytes_per_kind)?,
        })
    }

    pub(crate) fn for_kind(&self, kind: IndexKind) -> &IndexMemoryBudget {
        match kind {
            IndexKind::Path => &self.path,
            IndexKind::MetadataFilter => &self.metadata_filter,
            IndexKind::TypedJson => &self.typed_json,
            IndexKind::FullText => &self.full_text,
            IndexKind::Vector => &self.vector,
            IndexKind::Hybrid => &self.hybrid,
            IndexKind::GitSource => &self.git_source,
            IndexKind::Tensor => &self.tensor,
        }
    }
}

#[derive(Clone)]
pub(crate) struct IndexMemoryBudget {
    inner: Arc<BudgetInner>,
    snapshot_slot: Arc<Semaphore>,
}

struct BudgetInner {
    kind: IndexKind,
    limit: u64,
    state: Mutex<BudgetState>,
    changed: Notify,
}

#[derive(Default)]
struct BudgetState {
    used: u64,
    peak: u64,
    next_ticket: u64,
    waiters: VecDeque<BudgetWaiter>,
}

#[derive(Clone, Copy)]
struct BudgetWaiter {
    ticket: u64,
}

impl IndexMemoryBudget {
    fn new(kind: IndexKind, limit: u64) -> Result<Self, IndexBudgetError> {
        if limit == 0 {
            return Err(IndexBudgetError::ZeroLimit);
        }
        Ok(Self {
            inner: Arc::new(BudgetInner {
                kind,
                limit,
                state: Mutex::new(BudgetState::default()),
                changed: Notify::new(),
            }),
            snapshot_slot: Arc::new(Semaphore::new(MAX_OPEN_SNAPSHOTS_PER_KIND)),
        })
    }

    pub(crate) fn limit(&self) -> u64 {
        self.inner.limit
    }

    pub(crate) fn memory_plan(&self) -> SegmentMemoryPlan {
        let bytes = usize::try_from(self.inner.limit)
            .expect("validated index memory budget fits this platform");
        SegmentMemoryPlan::new(bytes).expect("validated index memory budget has a usable plan")
    }

    /// Admit one of the bounded pull-driven rebuild sessions for this kind.
    /// The permit is held for the stream lifetime, while its byte permit is
    /// released after every frame so other admitted sessions take FIFO turns.
    pub(crate) async fn acquire_snapshot_slot(
        &self,
    ) -> Result<OwnedSemaphorePermit, IndexBudgetError> {
        self.snapshot_slot
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| IndexBudgetError::SnapshotPoolClosed)
    }

    /// Wait in FIFO order for an exact byte reservation.
    ///
    /// Callers must not fetch a source page or payload until this returns.
    /// Dropping the future removes its queue entry synchronously.
    pub(crate) async fn acquire(&self, bytes: u64) -> Result<IndexMemoryPermit, IndexBudgetError> {
        if bytes == 0 {
            return Ok(IndexMemoryPermit {
                inner: self.inner.clone(),
                bytes: 0,
            });
        }
        if bytes > self.inner.limit {
            return Err(IndexBudgetError::RequestExceedsLimit {
                requested: bytes,
                limit: self.inner.limit,
            });
        }

        let ticket = {
            let mut state = lock_state(&self.inner);
            let ticket = state.next_ticket;
            state.next_ticket = state.next_ticket.wrapping_add(1);
            state.waiters.push_back(BudgetWaiter { ticket });
            emit_budget_state(&self.inner, &state);
            ticket
        };
        let mut queued = QueuedRequest {
            inner: self.inner.clone(),
            ticket: Some(ticket),
        };
        loop {
            // Register before testing the condition so a release between the
            // test and await cannot be missed.
            let changed = self.inner.changed.notified();
            let admitted = {
                let mut state = lock_state(&self.inner);
                let first = state
                    .waiters
                    .front()
                    .is_some_and(|waiter| waiter.ticket == ticket);
                if first && state.used <= self.inner.limit.saturating_sub(bytes) {
                    state.waiters.pop_front();
                    state.used += bytes;
                    state.peak = state.peak.max(state.used);
                    emit_budget_state(&self.inner, &state);
                    true
                } else {
                    false
                }
            };
            if admitted {
                queued.ticket = None;
                self.inner.changed.notify_waiters();
                return Ok(IndexMemoryPermit {
                    inner: self.inner.clone(),
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
}

fn lock_state(inner: &BudgetInner) -> std::sync::MutexGuard<'_, BudgetState> {
    inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct QueuedRequest {
    inner: Arc<BudgetInner>,
    ticket: Option<u64>,
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
        emit_budget_state(&self.inner, &state);
        drop(state);
        self.inner.changed.notify_waiters();
    }
}

pub(crate) struct IndexMemoryPermit {
    inner: Arc<BudgetInner>,
    bytes: u64,
}

impl IndexMemoryPermit {
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for IndexMemoryPermit {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        let mut state = lock_state(&self.inner);
        debug_assert!(state.used >= self.bytes);
        state.used = state.used.saturating_sub(self.bytes);
        emit_budget_state(&self.inner, &state);
        drop(state);
        self.inner.changed.notify_waiters();
    }
}

fn emit_budget_state(inner: &BudgetInner, state: &BudgetState) {
    tracing::info!(
        index.kind = ?inner.kind,
        gauge.anvil_index_construction_configured_bytes = inner.limit,
        gauge.anvil_index_construction_used_bytes = state.used,
        gauge.anvil_index_construction_peak_bytes = state.peak,
        gauge.anvil_index_construction_waiting = state.waiters.len() as u64,
        "index construction budget state"
    );
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum IndexBudgetError {
    #[error("index memory budget must be greater than zero")]
    ZeroLimit,
    #[error("index work requires {requested} bytes but its kind is capped at {limit} bytes")]
    RequestExceedsLimit { requested: u64, limit: u64 },
    #[error("index snapshot admission is closed")]
    SnapshotPoolClosed,
    #[error("index memory budget {configured} is below the format minimum {minimum}")]
    BelowMinimum { configured: u64, minimum: u64 },
    #[error("index memory budget {0} exceeds this platform's addressable size")]
    LimitExceedsPlatform(u64),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn aggregate_use_never_exceeds_the_hard_limit() {
        let budget = IndexMemoryBudget::new(IndexKind::Path, 10).unwrap();
        let first = budget.acquire(7).await.unwrap();
        let waiting_budget = budget.clone();
        let waiting = tokio::spawn(async move { waiting_budget.acquire(4).await.unwrap() });
        tokio::task::yield_now().await;
        assert_eq!(budget.used(), 7);
        assert!(!waiting.is_finished());
        drop(first);
        let second = waiting.await.unwrap();
        assert_eq!(second.bytes(), 4);
        assert_eq!(budget.used(), 4);
    }

    #[tokio::test]
    async fn cancelled_front_waiter_does_not_block_the_queue() {
        let budget = IndexMemoryBudget::new(IndexKind::Path, 10).unwrap();
        let held = budget.acquire(10).await.unwrap();
        let first_budget = budget.clone();
        let first = tokio::spawn(async move { first_budget.acquire(10).await.unwrap() });
        let second_budget = budget.clone();
        let second = tokio::spawn(async move { second_budget.acquire(1).await.unwrap() });
        tokio::task::yield_now().await;
        first.abort();
        let _ = first.await;
        drop(held);
        assert_eq!(second.await.unwrap().bytes(), 1);
    }

    #[test]
    fn every_kind_has_an_independent_pool() {
        let limit = MIN_INDEX_KIND_MEMORY_BYTES as u64;
        let budgets = IndexMemoryBudgets::new(limit).unwrap();
        assert_eq!(budgets.for_kind(IndexKind::Path).limit(), limit);
        assert_eq!(
            budgets.for_kind(IndexKind::Path).memory_plan().total_bytes,
            MIN_INDEX_KIND_MEMORY_BYTES
        );
        assert!(!Arc::ptr_eq(
            &budgets.for_kind(IndexKind::Path).inner,
            &budgets.for_kind(IndexKind::Vector).inner,
        ));
        assert!(!Arc::ptr_eq(
            &budgets.for_kind(IndexKind::Path).snapshot_slot,
            &budgets.for_kind(IndexKind::Vector).snapshot_slot,
        ));
    }

    #[test]
    fn aggregate_pool_rejects_a_budget_below_the_engine_minimum() {
        assert!(matches!(
            IndexMemoryBudgets::new(MIN_INDEX_KIND_MEMORY_BYTES as u64 - 1),
            Err(IndexBudgetError::BelowMinimum { .. })
        ));
    }

    #[tokio::test]
    async fn snapshot_sessions_are_bounded_without_serializing_three_rebuilds() {
        let budget = IndexMemoryBudget::new(IndexKind::Path, 10).unwrap();
        let first = budget.acquire_snapshot_slot().await.unwrap();
        let second = budget.acquire_snapshot_slot().await.unwrap();
        let third = budget.acquire_snapshot_slot().await.unwrap();
        let waiting_budget = budget.clone();
        let waiting =
            tokio::spawn(async move { waiting_budget.acquire_snapshot_slot().await.unwrap() });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(first);
        drop(waiting.await.unwrap());
        drop(second);
        drop(third);
    }

    #[tokio::test]
    async fn three_same_kind_full_quanta_make_fifo_progress() {
        let budget = IndexMemoryBudget::new(IndexKind::Path, 10).unwrap();
        let held = budget.acquire(10).await.unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for id in 1..=3 {
            let budget = budget.clone();
            let order = order.clone();
            tasks.push(tokio::spawn(async move {
                let permit = budget.acquire(10).await.unwrap();
                order.lock().unwrap().push(id);
                drop(permit);
            }));
            tokio::task::yield_now().await;
        }
        drop(held);
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
    }
}
