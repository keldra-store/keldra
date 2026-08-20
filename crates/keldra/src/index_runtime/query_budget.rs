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
        let memory = IndexWorkingMemory::new(limit_bytes, [limit_bytes; 9])
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
        tracing::info!(
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
        tracing::info!(
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
        tracing::info!(
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
}
