//! Shared logical-work and heap admission for one query execution.

use std::sync::{Arc, Mutex, MutexGuard};

use crate::IndexError;

use super::super::query_credits::QueryCreditReservation;
use super::{QueryArtifactKind, QueryBlockCredits, QueryExecutionLimits, QueryLoadEvidence};

pub(super) struct Budget {
    ledger: Arc<Mutex<BudgetLedger>>,
}

struct BudgetLedger {
    limits: QueryExecutionLimits,
    evidence: QueryLoadEvidence,
    heap_bytes: usize,
}

/// A logical heap charge paired with its exact byte reservation.
///
/// Acquisition and release take the logical ledger before the exact credit
/// ledger, and neither lock is held across user code or an await.
#[must_use = "dropping the reservation immediately returns its logical and exact bytes"]
pub(super) struct BudgetReservation {
    budget: Budget,
    credit: Option<QueryCreditReservation>,
    bytes: usize,
}

impl Clone for Budget {
    fn clone(&self) -> Self {
        Self {
            ledger: Arc::clone(&self.ledger),
        }
    }
}

fn lock_ledger(ledger: &Mutex<BudgetLedger>) -> MutexGuard<'_, BudgetLedger> {
    ledger
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl Budget {
    pub(super) fn new(limits: QueryExecutionLimits) -> Self {
        Self {
            ledger: Arc::new(Mutex::new(BudgetLedger {
                limits,
                evidence: QueryLoadEvidence::default(),
                heap_bytes: 0,
            })),
        }
    }

    pub(super) fn limits(&self) -> QueryExecutionLimits {
        lock_ledger(&self.ledger).limits
    }

    pub(super) fn evidence(&self) -> QueryLoadEvidence {
        lock_ledger(&self.ledger).evidence
    }

    /// Fork both query authorities as one operation for an independent task.
    /// The returned handles retain the same logical ledger and exact root
    /// permit; no bytes or work limits are divided between tasks.
    #[allow(dead_code)]
    pub(super) fn try_fork_query(
        &self,
        credits: &QueryBlockCredits,
    ) -> Result<(Self, QueryBlockCredits), IndexError> {
        Ok((self.clone(), credits.try_fork_query()?))
    }

    #[cfg(test)]
    pub(super) fn heap_bytes(&self) -> usize {
        lock_ledger(&self.ledger).heap_bytes
    }

    pub(super) fn load(&mut self, kind: QueryArtifactKind, bytes: usize) -> Result<(), IndexError> {
        let mut ledger = lock_ledger(&self.ledger);
        let (current, limit) = match kind {
            QueryArtifactKind::Page => (ledger.evidence.pages, ledger.limits.maximum_page_loads),
            QueryArtifactKind::Run => (ledger.evidence.runs, ledger.limits.maximum_run_loads),
            QueryArtifactKind::Block => (ledger.evidence.blocks, ledger.limits.maximum_block_loads),
        };
        let next = current.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        if next > limit {
            return resource(next, limit);
        }
        let loaded_bytes = ledger
            .evidence
            .bytes
            .checked_add(bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        if loaded_bytes > ledger.limits.maximum_loaded_bytes {
            return resource(loaded_bytes, ledger.limits.maximum_loaded_bytes);
        }
        match kind {
            QueryArtifactKind::Page => ledger.evidence.pages = next,
            QueryArtifactKind::Run => ledger.evidence.runs = next,
            QueryArtifactKind::Block => ledger.evidence.blocks = next,
        }
        ledger.evidence.bytes = loaded_bytes;
        Ok(())
    }

    pub(super) fn candidates(&self, count: usize) -> Result<(), IndexError> {
        let limit = lock_ledger(&self.ledger).limits.maximum_candidates;
        if count > limit {
            resource(count, limit)
        } else {
            Ok(())
        }
    }

    pub(super) fn reserve_heap(
        &mut self,
        credits: &mut QueryBlockCredits,
        bytes: usize,
    ) -> Result<(), IndexError> {
        let mut ledger = lock_ledger(&self.ledger);
        let next = ledger
            .heap_bytes
            .checked_add(bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        if next > ledger.limits.maximum_heap_bytes {
            return resource(next, ledger.limits.maximum_heap_bytes);
        }
        // Fixed lock order: logical Budget, then exact QueryBlockCredits. A
        // refused exact reservation leaves the logical ledger unchanged.
        credits.reserve(bytes)?;
        ledger.heap_bytes = next;
        Ok(())
    }

    #[allow(dead_code)]
    pub(super) fn reserve_heap_scoped(
        &mut self,
        credits: &mut QueryBlockCredits,
        bytes: usize,
    ) -> Result<BudgetReservation, IndexError> {
        let mut ledger = lock_ledger(&self.ledger);
        let next = ledger
            .heap_bytes
            .checked_add(bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        if next > ledger.limits.maximum_heap_bytes {
            return resource(next, ledger.limits.maximum_heap_bytes);
        }
        let credit = credits.reserve_scoped(bytes)?;
        ledger.heap_bytes = next;
        drop(ledger);
        Ok(BudgetReservation {
            budget: self.clone(),
            credit: Some(credit),
            bytes,
        })
    }

    pub(super) fn release_heap(
        &mut self,
        credits: &mut QueryBlockCredits,
        bytes: usize,
    ) -> Result<(), IndexError> {
        let mut ledger = lock_ledger(&self.ledger);
        let next = ledger
            .heap_bytes
            .checked_sub(bytes)
            .ok_or(IndexError::Integrity)?;
        // Preserve the same lock order as reserve. A refused exact release
        // leaves the logical ledger unchanged.
        credits.release(bytes)?;
        ledger.heap_bytes = next;
        Ok(())
    }
}

impl BudgetReservation {
    #[allow(dead_code)]
    pub(super) fn release(mut self) -> Result<(), IndexError> {
        self.release_both()
    }

    fn release_both(&mut self) -> Result<(), IndexError> {
        let mut ledger = lock_ledger(&self.budget.ledger);
        let next = ledger
            .heap_bytes
            .checked_sub(self.bytes)
            .ok_or(IndexError::Integrity)?;
        // Fixed release order mirrors acquisition. If the exact release is
        // refused, the logical charge remains intact.
        self.credit
            .as_mut()
            .ok_or(IndexError::Integrity)?
            .release_in_place()?;
        self.credit.take();
        ledger.heap_bytes = next;
        Ok(())
    }
}

impl Drop for BudgetReservation {
    fn drop(&mut self) {
        if self.credit.is_some() {
            let outcome = self.release_both();
            debug_assert!(outcome.is_ok(), "scoped query-budget release must balance");
        }
    }
}

fn resource<T>(needed: usize, limit: usize) -> Result<T, IndexError> {
    Err(IndexError::ResourceLimit { needed, limit })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::QueryMemoryPermit;

    struct Permit(usize);

    impl QueryMemoryPermit for Permit {
        fn admitted_bytes(&self) -> usize {
            self.0
        }
    }

    fn credits(bytes: usize) -> QueryBlockCredits {
        QueryBlockCredits::from_query_permit(Box::new(Permit(bytes))).unwrap()
    }

    #[test]
    fn forked_budget_and_credits_share_exact_limits() {
        let mut budget = Budget::new(QueryExecutionLimits {
            maximum_heap_bytes: 16,
            ..QueryExecutionLimits::default_for_memory()
        });
        let mut credits = credits(16);
        let (mut forked_budget, mut forked_credits) = budget.try_fork_query(&credits).unwrap();

        budget.reserve_heap(&mut credits, 10).unwrap();
        assert_eq!(
            forked_budget.reserve_heap(&mut forked_credits, 7),
            Err(IndexError::ResourceLimit {
                needed: 17,
                limit: 16,
            })
        );
        assert_eq!(credits.remaining(), 6);
        forked_budget.release_heap(&mut forked_credits, 10).unwrap();
        assert_eq!(budget.heap_bytes(), 0);
        assert_eq!(credits.remaining(), 16);
    }

    #[test]
    fn scoped_heap_reservation_releases_both_ledgers() {
        let mut budget = Budget::new(QueryExecutionLimits::default_for_memory());
        let mut credits = credits(32);
        {
            let _reservation = budget.reserve_heap_scoped(&mut credits, 12).unwrap();
            assert_eq!(budget.heap_bytes(), 12);
            assert_eq!(credits.remaining(), 20);
        }
        assert_eq!(budget.heap_bytes(), 0);
        assert_eq!(credits.remaining(), 32);
    }

    #[test]
    fn refused_load_rolls_back_all_logical_evidence() {
        let mut budget = Budget::new(QueryExecutionLimits {
            maximum_loaded_bytes: 8,
            ..QueryExecutionLimits::default_for_memory()
        });

        assert_eq!(
            budget.load(QueryArtifactKind::Block, 9),
            Err(IndexError::ResourceLimit {
                needed: 9,
                limit: 8,
            })
        );
        assert_eq!(budget.evidence(), QueryLoadEvidence::default());
    }
}
