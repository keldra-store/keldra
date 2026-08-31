//! Startup configuration for the format-v6 TypedJson index runtime.
//!
//! The v6 runtime has one bounded, partition-owned pipeline. A logical
//! definition neither owns a worker nor changes any of these budgets.

use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexRuntimeConfig {
    indexing_cores: NonZeroU32,
    pipeline_memory_bytes: NonZeroU64,
    query_memory_bytes: NonZeroU64,
    working_memory_bytes: Option<NonZeroU64>,
    flush_bytes: NonZeroU64,
    flush_max_age_millis: NonZeroU64,
    flush_max_operations: NonZeroU64,
    lsm_max_runs_per_level: NonZeroU32,
    lsm_max_unmerged_bytes_per_level: NonZeroU64,
}

impl IndexRuntimeConfig {
    pub const DEFAULT_INDEXING_CORES: u32 = 4;
    pub const DEFAULT_PIPELINE_MEMORY_BYTES_PER_CORE: u64 = 256 * 1024 * 1024;
    pub const DEFAULT_QUERY_MEMORY_BYTES: u64 = 512 * 1024 * 1024;
    pub const DEFAULT_FLUSH_BYTES: u64 = 16 * 1024 * 1024;
    pub const DEFAULT_FLUSH_MAX_AGE_MILLIS: u64 = 1_000;
    pub const DEFAULT_FLUSH_MAX_OPERATIONS: u64 = 65_536;
    pub const DEFAULT_LSM_MAX_RUNS_PER_LEVEL: u32 = 64;
    pub const DEFAULT_LSM_MAX_UNMERGED_BYTES_PER_LEVEL: u64 = 1024 * 1024 * 1024;

    pub fn new(indexing_cores: u32) -> Result<Self, IndexRuntimeConfigError> {
        let indexing_cores =
            NonZeroU32::new(indexing_cores).ok_or(IndexRuntimeConfigError::ZeroIndexingCores)?;
        let pipeline_memory_bytes = Self::DEFAULT_PIPELINE_MEMORY_BYTES_PER_CORE
            .checked_mul(u64::from(indexing_cores.get()))
            .and_then(NonZeroU64::new)
            .ok_or(IndexRuntimeConfigError::PipelineMemoryBytesOverflow)?;
        Ok(Self {
            indexing_cores,
            pipeline_memory_bytes,
            query_memory_bytes: NonZeroU64::new(Self::DEFAULT_QUERY_MEMORY_BYTES)
                .expect("default query memory is positive"),
            working_memory_bytes: None,
            flush_bytes: NonZeroU64::new(Self::DEFAULT_FLUSH_BYTES)
                .expect("default flush bytes are positive"),
            flush_max_age_millis: NonZeroU64::new(Self::DEFAULT_FLUSH_MAX_AGE_MILLIS)
                .expect("default flush age is positive"),
            flush_max_operations: NonZeroU64::new(Self::DEFAULT_FLUSH_MAX_OPERATIONS)
                .expect("default flush operation count is positive"),
            lsm_max_runs_per_level: NonZeroU32::new(Self::DEFAULT_LSM_MAX_RUNS_PER_LEVEL)
                .expect("default LSM run limit is positive"),
            lsm_max_unmerged_bytes_per_level: NonZeroU64::new(
                Self::DEFAULT_LSM_MAX_UNMERGED_BYTES_PER_LEVEL,
            )
            .expect("default LSM byte limit is positive"),
        })
    }

    pub const fn indexing_cores(self) -> u32 {
        self.indexing_cores.get()
    }
    pub const fn query_memory_bytes(self) -> u64 {
        self.query_memory_bytes.get()
    }
    pub const fn pipeline_memory_bytes(self) -> u64 {
        self.pipeline_memory_bytes.get()
    }
    pub const fn flush_bytes(self) -> u64 {
        self.flush_bytes.get()
    }
    pub fn flush_max_age(self) -> Duration {
        Duration::from_millis(self.flush_max_age_millis.get())
    }
    pub const fn flush_max_operations(self) -> u64 {
        self.flush_max_operations.get()
    }
    pub const fn lsm_max_runs_per_level(self) -> u32 {
        self.lsm_max_runs_per_level.get()
    }
    pub const fn lsm_max_unmerged_bytes_per_level(self) -> u64 {
        self.lsm_max_unmerged_bytes_per_level.get()
    }

    pub fn with_pipeline_memory_bytes(
        mut self,
        bytes: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.pipeline_memory_bytes =
            positive_u64(bytes, IndexRuntimeConfigError::ZeroPipelineMemoryBytes)?;
        Ok(self)
    }

    pub fn with_query_memory_bytes(mut self, value: u64) -> Result<Self, IndexRuntimeConfigError> {
        self.query_memory_bytes =
            positive_u64(value, IndexRuntimeConfigError::ZeroQueryMemoryBytes)?;
        Ok(self)
    }

    pub fn with_working_memory_bytes(
        mut self,
        value: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.working_memory_bytes = Some(positive_u64(
            value,
            IndexRuntimeConfigError::ZeroWorkingMemoryBytes,
        )?);
        self.validate_working_memory()?;
        Ok(self)
    }

    pub fn with_flush_boundaries(
        mut self,
        bytes: u64,
        max_age_millis: u64,
        max_operations: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.flush_bytes = positive_u64(bytes, IndexRuntimeConfigError::ZeroFlushBytes)?;
        self.flush_max_age_millis = positive_u64(
            max_age_millis,
            IndexRuntimeConfigError::ZeroFlushMaxAgeMillis,
        )?;
        self.flush_max_operations =
            positive_u64(max_operations, IndexRuntimeConfigError::ZeroFlushOperations)?;
        Ok(self)
    }

    pub fn with_lsm_limits(
        mut self,
        runs: u32,
        bytes: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.lsm_max_runs_per_level =
            NonZeroU32::new(runs).ok_or(IndexRuntimeConfigError::ZeroLsmRunsPerLevel)?;
        self.lsm_max_unmerged_bytes_per_level =
            positive_u64(bytes, IndexRuntimeConfigError::ZeroLsmUnmergedBytesPerLevel)?;
        Ok(self)
    }

    /// The single hard aggregate must always admit the pipeline and one query.
    pub fn working_memory_bytes(self) -> Result<u64, IndexRuntimeConfigError> {
        self.validate_working_memory()?;
        self.working_memory_bytes.map_or_else(
            || {
                self.pipeline_memory_bytes()
                    .checked_add(self.query_memory_bytes())
                    .ok_or(IndexRuntimeConfigError::WorkingMemoryBytesOverflow)
            },
            |configured| Ok(configured.get()),
        )
    }

    fn validate_working_memory(self) -> Result<(), IndexRuntimeConfigError> {
        let minimum = self
            .pipeline_memory_bytes()
            .checked_add(self.query_memory_bytes())
            .ok_or(IndexRuntimeConfigError::WorkingMemoryBytesOverflow)?;
        if let Some(configured) = self.working_memory_bytes
            && configured.get() < minimum
        {
            return Err(
                IndexRuntimeConfigError::WorkingMemoryBelowMandatoryMinimum {
                    configured: configured.get(),
                    minimum,
                },
            );
        }
        Ok(())
    }
}

fn positive_u64(
    value: u64,
    error: IndexRuntimeConfigError,
) -> Result<NonZeroU64, IndexRuntimeConfigError> {
    NonZeroU64::new(value).ok_or(error)
}

impl Default for IndexRuntimeConfig {
    fn default() -> Self {
        Self::new(Self::DEFAULT_INDEXING_CORES)
            .expect("the built-in v6 index configuration is valid")
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum IndexRuntimeConfigError {
    #[error("indexing core count must be greater than zero")]
    ZeroIndexingCores,
    #[error("default v6 pipeline memory calculation overflows u64")]
    PipelineMemoryBytesOverflow,
    #[error("v6 pipeline memory bytes must be greater than zero")]
    ZeroPipelineMemoryBytes,
    #[error("index query memory bytes must be greater than zero")]
    ZeroQueryMemoryBytes,
    #[error("aggregate index working memory bytes must be greater than zero")]
    ZeroWorkingMemoryBytes,
    #[error("aggregate index working memory {configured} cannot admit mandatory request {minimum}")]
    WorkingMemoryBelowMandatoryMinimum { configured: u64, minimum: u64 },
    #[error("aggregate index working-memory sum overflows u64")]
    WorkingMemoryBytesOverflow,
    #[error("v6 segment flush byte target must be greater than zero")]
    ZeroFlushBytes,
    #[error("v6 segment flush maximum age milliseconds must be greater than zero")]
    ZeroFlushMaxAgeMillis,
    #[error("v6 segment flush operation limit must be greater than zero")]
    ZeroFlushOperations,
    #[error("v6 LSM maximum runs per level must be greater than zero")]
    ZeroLsmRunsPerLevel,
    #[error("v6 LSM maximum unmerged bytes per level must be greater than zero")]
    ZeroLsmUnmergedBytesPerLevel,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_bind_one_v6_pipeline_per_configured_core() {
        let config = IndexRuntimeConfig::default();
        assert_eq!(config.indexing_cores(), 4);
        assert_eq!(config.pipeline_memory_bytes(), 1024 * 1024 * 1024);
        assert_eq!(config.working_memory_bytes().unwrap(), 1536 * 1024 * 1024);
    }

    #[test]
    fn aggregate_memory_cannot_starve_v6_pipeline() {
        assert!(matches!(
            IndexRuntimeConfig::default().with_working_memory_bytes(1024),
            Err(IndexRuntimeConfigError::WorkingMemoryBelowMandatoryMinimum { .. })
        ));
    }
}
