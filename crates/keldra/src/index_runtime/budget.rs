//! Fair-share admission for index building and compaction.
//!
//! Kind settings remain planning targets, while every actual allocation is
//! charged to the shared hard index working-memory ceiling.

use keldra_index::{IndexKind, MIN_INDEX_KIND_MEMORY_BYTES, SegmentMemoryPlan};
use thiserror::Error;

use crate::index_config::IndexRuntimeConfig;

use super::working_memory::{
    IndexWorkingMemory, WorkingMemoryAccount, WorkingMemoryError, WorkingMemoryPermit,
};

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
    #[cfg(test)]
    pub(crate) fn new(bytes_per_kind: u64) -> Result<Self, IndexBudgetError> {
        let aggregate = bytes_per_kind
            .checked_mul(9)
            .ok_or(IndexBudgetError::LimitExceedsPlatform(bytes_per_kind))?;
        let memory = IndexWorkingMemory::new(aggregate, [bytes_per_kind; 9])?;
        Self::from_limits(memory, |_| bytes_per_kind)
    }

    pub(crate) fn from_config(
        config: IndexRuntimeConfig,
        memory: IndexWorkingMemory,
    ) -> Result<Self, IndexBudgetError> {
        Self::from_limits(memory, |kind| config.builder_memory_bytes(kind))
    }

    fn from_limits(
        memory: IndexWorkingMemory,
        mut limit: impl FnMut(IndexKind) -> u64,
    ) -> Result<Self, IndexBudgetError> {
        let path = limit(IndexKind::Path);
        let metadata_filter = limit(IndexKind::MetadataFilter);
        let typed_json = limit(IndexKind::TypedJson);
        let full_text = limit(IndexKind::FullText);
        let vector = limit(IndexKind::Vector);
        let hybrid = limit(IndexKind::Hybrid);
        let git_source = limit(IndexKind::GitSource);
        let tensor = limit(IndexKind::Tensor);
        for bytes in [
            path,
            metadata_filter,
            typed_json,
            full_text,
            vector,
            hybrid,
            git_source,
            tensor,
        ] {
            validate_limit(bytes)?;
        }
        Ok(Self {
            path: IndexMemoryBudget::new(IndexKind::Path, path, memory.clone())?,
            metadata_filter: IndexMemoryBudget::new(
                IndexKind::MetadataFilter,
                metadata_filter,
                memory.clone(),
            )?,
            typed_json: IndexMemoryBudget::new(IndexKind::TypedJson, typed_json, memory.clone())?,
            full_text: IndexMemoryBudget::new(IndexKind::FullText, full_text, memory.clone())?,
            vector: IndexMemoryBudget::new(IndexKind::Vector, vector, memory.clone())?,
            hybrid: IndexMemoryBudget::new(IndexKind::Hybrid, hybrid, memory.clone())?,
            git_source: IndexMemoryBudget::new(IndexKind::GitSource, git_source, memory.clone())?,
            tensor: IndexMemoryBudget::new(IndexKind::Tensor, tensor, memory)?,
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

fn validate_limit(bytes: u64) -> Result<(), IndexBudgetError> {
    let plan_bytes =
        usize::try_from(bytes).map_err(|_| IndexBudgetError::LimitExceedsPlatform(bytes))?;
    SegmentMemoryPlan::new(plan_bytes).map_err(|_| IndexBudgetError::BelowMinimum {
        configured: bytes,
        minimum: MIN_INDEX_KIND_MEMORY_BYTES as u64,
    })?;
    Ok(())
}

#[derive(Clone)]
pub(crate) struct IndexMemoryBudget {
    kind: IndexKind,
    fair_share: u64,
    memory: IndexWorkingMemory,
}

impl IndexMemoryBudget {
    fn new(
        kind: IndexKind,
        fair_share: u64,
        memory: IndexWorkingMemory,
    ) -> Result<Self, IndexBudgetError> {
        if fair_share == 0 {
            return Err(IndexBudgetError::ZeroLimit);
        }
        Ok(Self {
            kind,
            fair_share,
            memory,
        })
    }

    /// Configured planning target for this kind, not a separate hard pool.
    pub(crate) fn limit(&self) -> u64 {
        self.fair_share
    }

    pub(crate) fn working_memory_limit(&self) -> u64 {
        self.memory.hard_limit()
    }

    pub(crate) fn memory_plan(&self) -> SegmentMemoryPlan {
        let bytes = usize::try_from(self.fair_share)
            .expect("validated index memory fair share fits this platform");
        SegmentMemoryPlan::new(bytes).expect("validated index memory share has a usable plan")
    }

    /// Wait in global FIFO order for one exact reservation.
    pub(crate) async fn acquire(&self, bytes: u64) -> Result<IndexMemoryPermit, IndexBudgetError> {
        self.acquire_up_to(bytes, bytes).await
    }

    /// Admit mandatory bytes fairly, then borrow idle aggregate capacity up to
    /// the preferred amount. Callers must derive their memory plan from the
    /// returned permit rather than the configured fair share.
    pub(crate) async fn acquire_up_to(
        &self,
        minimum: u64,
        preferred: u64,
    ) -> Result<IndexMemoryPermit, IndexBudgetError> {
        tracing::info!(
            index.kind = ?self.kind,
            gauge.keldra_index_construction_configured_bytes = self.fair_share,
            gauge.keldra_index_construction_working_memory_bytes = self.memory.hard_limit(),
            "index construction is waiting for working-memory admission"
        );
        let permit = self
            .memory
            .acquire_up_to(WorkingMemoryAccount::Builder(self.kind), minimum, preferred)
            .await?;
        let bytes = permit.bytes();
        tracing::info!(
            index.kind = ?self.kind,
            gauge.keldra_index_construction_configured_bytes = self.fair_share,
            histogram.keldra_index_construction_minimum_bytes = minimum,
            histogram.keldra_index_construction_desired_bytes = preferred,
            histogram.keldra_index_construction_granted_bytes = bytes,
            histogram.keldra_index_construction_borrowed_bytes = bytes.saturating_sub(self.fair_share),
            "index construction working memory admitted"
        );
        Ok(IndexMemoryPermit {
            bytes,
            _permit: permit,
        })
    }
}

pub(crate) struct IndexMemoryPermit {
    bytes: u64,
    _permit: WorkingMemoryPermit,
}

impl IndexMemoryPermit {
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum IndexBudgetError {
    #[error("index memory budget must be greater than zero")]
    ZeroLimit,
    #[error("index memory budget {configured} is below the format minimum {minimum}")]
    BelowMinimum { configured: u64, minimum: u64 },
    #[error("index memory budget {0} exceeds this platform's addressable size")]
    LimitExceedsPlatform(u64),
    #[error(transparent)]
    WorkingMemory(#[from] WorkingMemoryError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_kind_overrides_remain_distinct_fair_shares() {
        let baseline = MIN_INDEX_KIND_MEMORY_BYTES as u64;
        let config = IndexRuntimeConfig::new(1, 1, baseline, 1, 1, 1, 1)
            .unwrap()
            .with_kind_limits(IndexKind::Vector, baseline * 2, 2)
            .unwrap();
        let memory = IndexWorkingMemory::from_config(config).unwrap();
        let budgets = IndexMemoryBudgets::from_config(config, memory).unwrap();
        assert_eq!(budgets.for_kind(IndexKind::Path).limit(), baseline);
        assert_eq!(budgets.for_kind(IndexKind::Vector).limit(), baseline * 2);
    }

    #[test]
    fn aggregate_pool_rejects_a_budget_below_the_engine_minimum() {
        assert!(matches!(
            IndexMemoryBudgets::new(MIN_INDEX_KIND_MEMORY_BYTES as u64 - 1),
            Err(IndexBudgetError::BelowMinimum { .. })
        ));
    }

    #[tokio::test]
    async fn completed_full_share_turn_leaves_the_next_turn_runnable() {
        let share = MIN_INDEX_KIND_MEMORY_BYTES as u64;
        let budgets = IndexMemoryBudgets::new(share).unwrap();
        let budget = budgets.for_kind(IndexKind::TypedJson);
        let rebuild_turn = budget.acquire(share).await.unwrap();
        assert_eq!(rebuild_turn.bytes(), share);
        drop(rebuild_turn);

        let catch_up_turn =
            tokio::time::timeout(std::time::Duration::from_secs(1), budget.acquire(share))
                .await
                .expect("a yielded rebuild turn must not pin working memory")
                .unwrap();
        assert_eq!(catch_up_turn.bytes(), share);
    }

    #[tokio::test]
    async fn one_kind_can_borrow_idle_capacity_from_the_shared_parent() {
        let share = MIN_INDEX_KIND_MEMORY_BYTES as u64;
        let budgets = IndexMemoryBudgets::new(share).unwrap();
        let budget = budgets.for_kind(IndexKind::TypedJson);
        let permit = budget
            .acquire_up_to(share, share.checked_mul(2).unwrap())
            .await
            .unwrap();
        assert_eq!(permit.bytes(), share * 2);
    }
}
