//! Fair process-wide byte admission for index query working memory.

use std::sync::Arc;
use std::time::Instant;

use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const PERMIT_BYTES: u64 = 4 * 1024;

#[derive(Clone)]
pub(crate) struct IndexQueryMemoryBudget {
    inner: Arc<QueryBudgetInner>,
}

struct QueryBudgetInner {
    semaphore: Arc<Semaphore>,
    limit_bytes: u64,
}

impl IndexQueryMemoryBudget {
    pub(crate) fn new(limit_bytes: u64) -> Result<Self, QueryBudgetError> {
        if limit_bytes == 0 {
            return Err(QueryBudgetError::ZeroLimit);
        }
        let permits = permits_for(limit_bytes)?;
        Ok(Self {
            inner: Arc::new(QueryBudgetInner {
                semaphore: Arc::new(Semaphore::new(permits as usize)),
                limit_bytes,
            }),
        })
    }

    pub(crate) fn limit_bytes(&self) -> u64 {
        self.inner.limit_bytes
    }

    /// Acquire a conservative reservation before decoded blocks, candidate
    /// batches, or top-K state are allocated. Four-KiB permit rounding can
    /// only under-utilize the configured byte ceiling; it cannot exceed it.
    pub(crate) async fn acquire(
        &self,
        requested_bytes: u64,
    ) -> Result<IndexQueryMemoryPermit, QueryBudgetError> {
        if requested_bytes == 0 || requested_bytes > self.inner.limit_bytes {
            return Err(QueryBudgetError::RequestExceedsLimit {
                requested: requested_bytes,
                limit: self.inner.limit_bytes,
            });
        }
        let permits = permits_for(requested_bytes)?;
        let started = Instant::now();
        tracing::info!(
            gauge.anvil_index_query_memory_configured_bytes = self.inner.limit_bytes,
            counter.anvil_index_query_memory_waiting_bytes = charged_bytes(permits) as i64,
            "index query is waiting for working-memory admission"
        );
        let permit = self
            .inner
            .semaphore
            .clone()
            .acquire_many_owned(permits)
            .await
            .map_err(|_| QueryBudgetError::Closed)?;
        tracing::info!(
            counter.anvil_index_query_memory_waiting_bytes = -(charged_bytes(permits) as i64),
            counter.anvil_index_query_memory_leased_bytes = charged_bytes(permits) as i64,
            histogram.anvil_index_query_memory_wait_seconds = started.elapsed().as_secs_f64(),
            "index query working memory admitted"
        );
        Ok(IndexQueryMemoryPermit {
            permits,
            _permit: permit,
        })
    }

    #[cfg(test)]
    fn available_permits(&self) -> usize {
        self.inner.semaphore.available_permits()
    }
}

pub(crate) struct IndexQueryMemoryPermit {
    permits: u32,
    _permit: OwnedSemaphorePermit,
}

impl IndexQueryMemoryPermit {
    pub(crate) fn charged_bytes(&self) -> u64 {
        charged_bytes(self.permits)
    }
}

impl Drop for IndexQueryMemoryPermit {
    fn drop(&mut self) {
        tracing::info!(
            counter.anvil_index_query_memory_leased_bytes = -(charged_bytes(self.permits) as i64),
            "index query working memory released"
        );
    }
}

fn permits_for(bytes: u64) -> Result<u32, QueryBudgetError> {
    bytes
        .div_ceil(PERMIT_BYTES)
        .try_into()
        .map_err(|_| QueryBudgetError::LimitExceedsPlatform(bytes))
}

fn charged_bytes(permits: u32) -> u64 {
    u64::from(permits) * PERMIT_BYTES
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum QueryBudgetError {
    #[error("index query memory budget must be greater than zero")]
    ZeroLimit,
    #[error("index query memory request is {requested} bytes but the global limit is {limit}")]
    RequestExceedsLimit { requested: u64, limit: u64 },
    #[error("index query memory budget {0} exceeds the supported platform range")]
    LimitExceedsPlatform(u64),
    #[error("index query memory admission is closed")]
    Closed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reservations_are_fair_and_never_exceed_the_ceiling() {
        let budget = IndexQueryMemoryBudget::new(2 * PERMIT_BYTES).unwrap();
        let first = budget.acquire(2 * PERMIT_BYTES).await.unwrap();
        let waiting_budget = budget.clone();
        let waiting = tokio::spawn(async move { waiting_budget.acquire(1).await.unwrap() });
        tokio::task::yield_now().await;
        assert_eq!(budget.available_permits(), 0);
        assert!(!waiting.is_finished());
        drop(first);
        let second = waiting.await.unwrap();
        assert_eq!(second.charged_bytes(), PERMIT_BYTES);
        assert_eq!(budget.available_permits(), 1);
    }

    #[tokio::test]
    async fn zero_oversized_and_unrepresentable_requests_fail() {
        let budget = IndexQueryMemoryBudget::new(PERMIT_BYTES).unwrap();
        assert!(matches!(
            budget.acquire(0).await,
            Err(QueryBudgetError::RequestExceedsLimit { .. })
        ));
        assert!(matches!(
            budget.acquire(PERMIT_BYTES + 1).await,
            Err(QueryBudgetError::RequestExceedsLimit { .. })
        ));
        assert_eq!(
            IndexQueryMemoryBudget::new(0).err(),
            Some(QueryBudgetError::ZeroLimit)
        );
        let unsupported = (u64::from(u32::MAX) + 1) * PERMIT_BYTES;
        assert_eq!(
            permits_for(unsupported),
            Err(QueryBudgetError::LimitExceedsPlatform(unsupported))
        );
    }
}
