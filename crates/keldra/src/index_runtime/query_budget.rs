//! Fair process-wide byte admission for index query working memory.

use std::time::Instant;

use thiserror::Error;

use super::working_memory::{
    IndexWorkingMemory, WorkingMemoryAccount, WorkingMemoryError, WorkingMemoryPermit,
};

#[derive(Clone)]
pub(crate) struct IndexQueryMemoryBudget {
    memory: IndexWorkingMemory,
    fair_share_bytes: u64,
}

impl IndexQueryMemoryBudget {
    pub(crate) fn new(limit_bytes: u64) -> Result<Self, QueryBudgetError> {
        if limit_bytes == 0 {
            return Err(QueryBudgetError::ZeroLimit);
        }
        let memory = IndexWorkingMemory::new(limit_bytes, [limit_bytes; 2])
            .map_err(QueryBudgetError::WorkingMemory)?;
        Ok(Self::from_shared(memory))
    }

    pub(crate) fn from_shared(memory: IndexWorkingMemory) -> Self {
        let fair_share_bytes = memory.share(WorkingMemoryAccount::Query);
        Self {
            memory,
            fair_share_bytes,
        }
    }

    /// Acquire a conservative reservation before decoded blocks, candidate
    /// batches, or top-K state are allocated.
    pub(crate) async fn acquire(
        &self,
        requested_bytes: u64,
    ) -> Result<IndexQueryMemoryPermit, QueryBudgetError> {
        self.acquire_up_to(requested_bytes, requested_bytes).await
    }

    /// Queue for an exact per-query execution lease. The request may borrow
    /// aggregate headroom beyond the query share, but cannot exceed either the
    /// per-query bound or capacity left after the permanent pipeline lease.
    pub(crate) async fn acquire_bounded(
        &self,
        requested_bytes: u64,
        maximum_bytes: u64,
    ) -> Result<IndexQueryMemoryPermit, QueryBudgetError> {
        let attainable_bytes = self.maximum_bounded_lease(maximum_bytes);
        if requested_bytes == 0 || requested_bytes > attainable_bytes {
            return Err(QueryBudgetError::RequestExceedsLimit {
                requested: requested_bytes,
                limit: attainable_bytes,
            });
        }
        self.acquire(requested_bytes).await
    }

    pub(crate) fn maximum_bounded_lease(&self, maximum_bytes: u64) -> u64 {
        maximum_bytes.min(self.fair_share_bytes.max(
            self.memory
                .hard_limit()
                .saturating_sub(self.memory.share(WorkingMemoryAccount::IndexingPipeline)),
        ))
    }

    /// Wait for the mandatory query reservation, then borrow any permits which
    /// are immediately idle up to the preferred amount. The optional portion
    /// never waits behind active queries and remains covered by the same hard
    /// process-wide ceiling.
    pub(crate) async fn acquire_up_to(
        &self,
        minimum_bytes: u64,
        preferred_bytes: u64,
    ) -> Result<IndexQueryMemoryPermit, QueryBudgetError> {
        if minimum_bytes == 0
            || minimum_bytes > preferred_bytes
            || minimum_bytes > self.memory.hard_limit()
        {
            return Err(QueryBudgetError::RequestExceedsLimit {
                requested: minimum_bytes,
                limit: self.memory.hard_limit(),
            });
        }
        let minimum_charged = minimum_bytes;
        let preferred_charged = preferred_bytes.min(self.memory.hard_limit());
        let started = Instant::now();
        tracing::debug!(
            gauge.keldra_index_query_memory_configured_bytes = self.fair_share_bytes,
            counter.keldra_index_query_memory_waiting_bytes = minimum_charged as i64,
            "index query is waiting for working-memory admission"
        );
        let permit = self
            .memory
            .acquire_up_to(
                WorkingMemoryAccount::Query,
                minimum_charged,
                preferred_charged,
            )
            .await
            .map_err(QueryBudgetError::WorkingMemory)?;
        let granted = permit.bytes();
        tracing::debug!(
            counter.keldra_index_query_memory_waiting_bytes = -(minimum_charged as i64),
            counter.keldra_index_query_memory_leased_bytes = granted as i64,
            histogram.keldra_index_query_memory_wait_seconds = started.elapsed().as_secs_f64(),
            "index query working memory admitted"
        );
        Ok(IndexQueryMemoryPermit {
            bytes: granted,
            _permit: permit,
        })
    }

    #[cfg(test)]
    fn available_permits(&self) -> usize {
        usize::try_from(self.memory.available()).unwrap_or(usize::MAX)
    }

    #[cfg(test)]
    fn waiting_queries(&self) -> usize {
        self.memory.waiting(WorkingMemoryAccount::Query)
    }
}

pub(crate) struct IndexQueryMemoryPermit {
    bytes: u64,
    _permit: WorkingMemoryPermit,
}

impl IndexQueryMemoryPermit {
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for IndexQueryMemoryPermit {
    fn drop(&mut self) {
        tracing::debug!(
            counter.keldra_index_query_memory_leased_bytes = -(self.bytes as i64),
            "index query working memory released"
        );
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum QueryBudgetError {
    #[error("index query memory budget must be greater than zero")]
    ZeroLimit,
    #[error("index query memory request is {requested} bytes but the global limit is {limit}")]
    RequestExceedsLimit { requested: u64, limit: u64 },
    #[error(transparent)]
    WorkingMemory(#[from] WorkingMemoryError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reservations_are_fair_and_never_exceed_the_ceiling() {
        let budget = IndexQueryMemoryBudget::new(8 * 1024).unwrap();
        let first = budget.acquire(8 * 1024).await.unwrap();
        let waiting_budget = budget.clone();
        let waiting = tokio::spawn(async move { waiting_budget.acquire(1).await.unwrap() });
        tokio::task::yield_now().await;
        assert_eq!(budget.available_permits(), 0);
        assert!(!waiting.is_finished());
        drop(first);
        let second = waiting.await.unwrap();
        assert_eq!(second.charged_bytes(), 1);
        assert_eq!(budget.available_permits(), 8 * 1024 - 1);
    }

    #[tokio::test]
    async fn zero_oversized_and_unrepresentable_requests_fail() {
        let budget = IndexQueryMemoryBudget::new(4 * 1024).unwrap();
        assert!(matches!(
            budget.acquire(0).await,
            Err(QueryBudgetError::RequestExceedsLimit { .. })
        ));
        assert!(matches!(
            budget.acquire(4 * 1024 + 1).await,
            Err(QueryBudgetError::RequestExceedsLimit { .. })
        ));
        assert_eq!(
            IndexQueryMemoryBudget::new(0).err(),
            Some(QueryBudgetError::ZeroLimit)
        );
    }

    #[tokio::test]
    async fn elastic_reservation_uses_idle_capacity_without_exceeding_the_ceiling() {
        let budget = IndexQueryMemoryBudget::new(16 * 1024).unwrap();
        let occupied = budget.acquire(4 * 1024).await.unwrap();

        let elastic = budget.acquire_up_to(4 * 1024, 32 * 1024).await.unwrap();

        assert_eq!(elastic.charged_bytes(), 12 * 1024);
        assert_eq!(budget.available_permits(), 0);
        drop(elastic);
        drop(occupied);
        assert_eq!(budget.available_permits(), 16 * 1024);
    }

    #[tokio::test]
    async fn bounded_exact_waits_for_temporary_pressure_instead_of_failing() {
        let budget = IndexQueryMemoryBudget::new(16 * 1024).unwrap();
        let occupied = budget.acquire(8 * 1024).await.unwrap();
        let waiting_budget = budget.clone();
        let waiting = tokio::spawn(async move {
            waiting_budget
                .acquire_bounded(16 * 1024, 16 * 1024)
                .await
                .unwrap()
        });
        tokio::task::yield_now().await;

        assert!(!waiting.is_finished());
        assert_eq!(budget.available_permits(), 8 * 1024);
        drop(occupied);

        let admitted = waiting.await.unwrap();
        assert_eq!(admitted.charged_bytes(), 16 * 1024);
        assert_eq!(budget.available_permits(), 0);
        drop(admitted);
        assert_eq!(budget.available_permits(), 16 * 1024);
    }

    #[tokio::test]
    async fn bounded_exact_preserves_fifo_under_concurrent_pressure() {
        let budget = IndexQueryMemoryBudget::new(16 * 1024).unwrap();
        let occupied = budget.acquire(16 * 1024).await.unwrap();

        let first_budget = budget.clone();
        let first = tokio::spawn(async move {
            first_budget
                .acquire_bounded(16 * 1024, 16 * 1024)
                .await
                .unwrap()
        });
        while budget.waiting_queries() != 1 {
            tokio::task::yield_now().await;
        }
        let second_budget = budget.clone();
        let second = tokio::spawn(async move {
            second_budget
                .acquire_bounded(16 * 1024, 16 * 1024)
                .await
                .unwrap()
        });
        while budget.waiting_queries() != 2 {
            tokio::task::yield_now().await;
        }

        drop(occupied);
        let first_admitted = first.await.unwrap();
        assert!(!second.is_finished());
        drop(first_admitted);
        let second_admitted = second.await.unwrap();
        assert_eq!(second_admitted.charged_bytes(), 16 * 1024);
    }

    #[tokio::test]
    async fn bounded_exact_rejects_a_request_above_the_hard_ceiling() {
        let budget = IndexQueryMemoryBudget::new(12 * 1024).unwrap();

        assert_eq!(
            budget.acquire_bounded(16 * 1024, 16 * 1024).await.err(),
            Some(QueryBudgetError::RequestExceedsLimit {
                requested: 16 * 1024,
                limit: 12 * 1024,
            })
        );
        assert_eq!(budget.available_permits(), 12 * 1024);
    }

    #[tokio::test]
    async fn bounded_exact_borrows_only_attainable_headroom_with_pipeline_held() {
        let memory = IndexWorkingMemory::new(32 * 1024, [8 * 1024, 16 * 1024]).unwrap();
        let pipeline = memory
            .acquire_up_to(WorkingMemoryAccount::IndexingPipeline, 16 * 1024, 16 * 1024)
            .await
            .unwrap();
        let budget = IndexQueryMemoryBudget::from_shared(memory);

        assert!(matches!(
            budget.acquire_bounded(24 * 1024, 24 * 1024).await,
            Err(QueryBudgetError::RequestExceedsLimit { .. })
        ));
        let admitted = budget.acquire_bounded(16 * 1024, 24 * 1024).await.unwrap();

        assert_eq!(admitted.charged_bytes(), 16 * 1024);
        assert_eq!(budget.available_permits(), 0);
        drop(admitted);
        drop(pipeline);
        assert_eq!(budget.available_permits(), 32 * 1024);
    }

    #[tokio::test]
    async fn bounded_exact_rejects_a_request_above_attainable_capacity() {
        let memory = IndexWorkingMemory::new(8 * 1024, [2 * 1024, 6 * 1024]).unwrap();
        let budget = IndexQueryMemoryBudget::from_shared(memory);

        assert_eq!(budget.maximum_bounded_lease(8 * 1024), 2 * 1024);
        assert_eq!(
            budget.acquire_bounded(4 * 1024, 8 * 1024).await.err(),
            Some(QueryBudgetError::RequestExceedsLimit {
                requested: 4 * 1024,
                limit: 2 * 1024,
            })
        );
    }

    #[tokio::test]
    async fn bounded_exact_keeps_small_queries_concurrent() {
        let budget = IndexQueryMemoryBudget::new(32 * 1024).unwrap();
        let mut admitted = Vec::new();
        for _ in 0..4 {
            admitted.push(budget.acquire_bounded(8 * 1024, 32 * 1024).await.unwrap());
        }

        assert!(
            admitted
                .iter()
                .all(|permit| permit.charged_bytes() == 8 * 1024)
        );
        assert_eq!(budget.available_permits(), 0);
    }
}
