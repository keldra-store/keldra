use std::num::{NonZeroU8, NonZeroU32, NonZeroU64};

use anvil_index::IndexKind;
use thiserror::Error;

/// Startup-only budgets and retention bounds shared by every index on a node.
///
/// Authoritative index bytes remain ordinary Anvil objects. The disk and
/// memory values here only bound disposable local materialisations. Retention
/// bounds apply per index and always preserve its current generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexRuntimeConfig {
    disk_cache_bytes: NonZeroU64,
    memory_percent: NonZeroU8,
    builder_memory_bytes_per_kind: NonZeroU64,
    builder_memory_bytes: [NonZeroU64; 8],
    projection_max_lanes: [NonZeroU32; 8],
    source_quantum_bytes: [NonZeroU64; 8],
    external_sort_chunk_bytes: [NonZeroU64; 8],
    compaction_max_lanes: [NonZeroU32; 8],
    max_runs_per_level: [NonZeroU32; 8],
    max_uncompacted_bytes_per_level: [NonZeroU64; 8],
    auto_rebuild_lag_entries: NonZeroU64,
    auto_rebuild_lag_bytes: NonZeroU64,
    auto_rebuild_lag_uses_source_journal_defaults: bool,
    rayon_workers: NonZeroU32,
    query_max_concurrency: NonZeroU32,
    query_work_quantum_bytes: NonZeroU64,
    max_retained_generations: NonZeroU32,
    max_generation_age_hours: NonZeroU64,
    max_retained_generation_bytes: NonZeroU64,
}

impl IndexRuntimeConfig {
    pub const DEFAULT_DISK_CACHE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
    pub const DEFAULT_MEMORY_PERCENT: u8 = 10;
    pub const DEFAULT_BUILDER_MEMORY_BYTES_PER_KIND: u64 = 256 * 1024 * 1024;
    pub const DEFAULT_PROJECTION_MAX_LANES: u32 = 4;
    pub const DEFAULT_SOURCE_QUANTUM_BYTES: u64 = 16 * 1024 * 1024;
    pub const DEFAULT_EXTERNAL_SORT_CHUNK_BYTES: u64 = 16 * 1024 * 1024;
    /// Four lanes fit the default per-kind construction budget. Setting one
    /// explicitly preserves the byte-for-byte sequential compaction path.
    pub const DEFAULT_COMPACTION_MAX_LANES: u32 = 4;
    pub const DEFAULT_RAYON_WORKERS: u32 = 4;
    pub const DEFAULT_QUERY_MAX_CONCURRENCY: u32 = 64;
    pub const DEFAULT_QUERY_WORK_QUANTUM_BYTES: u64 = 4 * 1024 * 1024;
    pub const DEFAULT_MAX_RUNS_PER_LEVEL: u32 = 64;
    pub const DEFAULT_MAX_UNCOMPACTED_BYTES_PER_LEVEL: u64 = 1024 * 1024 * 1024;
    pub const DEFAULT_AUTO_REBUILD_LAG_ENTRIES: u64 = anvil_store::DEFAULT_WATCH_MAX_ENTRIES / 2;
    pub const DEFAULT_AUTO_REBUILD_LAG_BYTES: u64 = anvil_store::DEFAULT_WATCH_MAX_BYTES / 2;
    pub const DEFAULT_MAX_RETAINED_GENERATIONS: u32 = 3;
    pub const DEFAULT_MAX_GENERATION_AGE_HOURS: u64 = 24;
    pub const DEFAULT_MAX_RETAINED_GENERATION_BYTES: u64 = 50 * 1024 * 1024 * 1024;

    pub fn new(
        disk_cache_bytes: u64,
        memory_percent: u8,
        builder_memory_bytes_per_kind: u64,
        rayon_workers: u32,
        max_retained_generations: u32,
        max_generation_age_hours: u64,
        max_retained_generation_bytes: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        let disk_cache_bytes =
            NonZeroU64::new(disk_cache_bytes).ok_or(IndexRuntimeConfigError::ZeroDiskCacheBytes)?;
        let memory_percent = NonZeroU8::new(memory_percent).ok_or(
            IndexRuntimeConfigError::InvalidMemoryPercent(memory_percent),
        )?;
        if memory_percent.get() > 100 {
            return Err(IndexRuntimeConfigError::InvalidMemoryPercent(
                memory_percent.get(),
            ));
        }
        let builder_memory_bytes_per_kind = NonZeroU64::new(builder_memory_bytes_per_kind)
            .ok_or(IndexRuntimeConfigError::ZeroBuilderMemoryBytesPerKind)?;
        let rayon_workers =
            NonZeroU32::new(rayon_workers).ok_or(IndexRuntimeConfigError::ZeroRayonWorkers)?;
        let max_retained_generations = NonZeroU32::new(max_retained_generations)
            .ok_or(IndexRuntimeConfigError::ZeroRetainedGenerations)?;
        let max_generation_age_hours = NonZeroU64::new(max_generation_age_hours)
            .ok_or(IndexRuntimeConfigError::ZeroGenerationAgeHours)?;
        let max_retained_generation_bytes = NonZeroU64::new(max_retained_generation_bytes)
            .ok_or(IndexRuntimeConfigError::ZeroRetainedGenerationBytes)?;

        Ok(Self {
            disk_cache_bytes,
            memory_percent,
            builder_memory_bytes_per_kind,
            builder_memory_bytes: [builder_memory_bytes_per_kind; 8],
            projection_max_lanes: [NonZeroU32::new(Self::DEFAULT_PROJECTION_MAX_LANES)
                .expect("the default projection lane limit is positive");
                8],
            source_quantum_bytes: [NonZeroU64::new(Self::DEFAULT_SOURCE_QUANTUM_BYTES)
                .expect("the default source quantum is positive");
                8],
            external_sort_chunk_bytes: [NonZeroU64::new(Self::DEFAULT_EXTERNAL_SORT_CHUNK_BYTES)
                .expect("the default external-sort chunk is positive");
                8],
            compaction_max_lanes: [NonZeroU32::new(Self::DEFAULT_COMPACTION_MAX_LANES)
                .expect("the default compaction lane limit is positive");
                8],
            max_runs_per_level: [NonZeroU32::new(Self::DEFAULT_MAX_RUNS_PER_LEVEL)
                .expect("the default run-debt bound is positive");
                8],
            max_uncompacted_bytes_per_level: [NonZeroU64::new(
                Self::DEFAULT_MAX_UNCOMPACTED_BYTES_PER_LEVEL,
            )
            .expect("the default byte-debt bound is positive");
                8],
            auto_rebuild_lag_entries: NonZeroU64::new(Self::DEFAULT_AUTO_REBUILD_LAG_ENTRIES)
                .expect("the default automatic rebuild entry threshold is positive"),
            auto_rebuild_lag_bytes: NonZeroU64::new(Self::DEFAULT_AUTO_REBUILD_LAG_BYTES)
                .expect("the default automatic rebuild byte threshold is positive"),
            auto_rebuild_lag_uses_source_journal_defaults: true,
            rayon_workers,
            query_max_concurrency: NonZeroU32::new(Self::DEFAULT_QUERY_MAX_CONCURRENCY)
                .expect("the default query concurrency is positive"),
            query_work_quantum_bytes: NonZeroU64::new(Self::DEFAULT_QUERY_WORK_QUANTUM_BYTES)
                .expect("the default query work quantum is positive"),
            max_retained_generations,
            max_generation_age_hours,
            max_retained_generation_bytes,
        })
    }

    pub fn disk_cache_bytes(self) -> u64 {
        self.disk_cache_bytes.get()
    }

    pub fn memory_percent(self) -> u8 {
        self.memory_percent.get()
    }

    /// Hard aggregate build-and-compaction heap budget for each index kind.
    ///
    /// Every definition of the same kind shares this one process-wide pool.
    pub fn builder_memory_bytes_per_kind(self) -> u64 {
        self.builder_memory_bytes_per_kind.get()
    }

    /// Override construction memory for one kind.
    pub fn with_kind_builder_memory_bytes(
        mut self,
        kind: IndexKind,
        builder_memory_bytes: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.builder_memory_bytes[kind_slot(kind)] = NonZeroU64::new(builder_memory_bytes)
            .ok_or(IndexRuntimeConfigError::ZeroBuilderMemoryBytesForKind(kind))?;
        Ok(self)
    }

    pub fn with_kind_projection_max_lanes(
        mut self,
        kind: IndexKind,
        lanes: u32,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.projection_max_lanes[kind_slot(kind)] = NonZeroU32::new(lanes)
            .ok_or(IndexRuntimeConfigError::ZeroProjectionLanesForKind(kind))?;
        Ok(self)
    }

    pub fn with_kind_source_quantum_bytes(
        mut self,
        kind: IndexKind,
        bytes: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.source_quantum_bytes[kind_slot(kind)] = NonZeroU64::new(bytes)
            .ok_or(IndexRuntimeConfigError::ZeroSourceQuantumForKind(kind))?;
        Ok(self)
    }

    pub fn with_kind_external_sort_chunk_bytes(
        mut self,
        kind: IndexKind,
        bytes: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.external_sort_chunk_bytes[kind_slot(kind)] = NonZeroU64::new(bytes)
            .ok_or(IndexRuntimeConfigError::ZeroExternalSortChunkForKind(kind))?;
        Ok(self)
    }

    /// Override compaction parallelism for one kind.
    ///
    /// A lane limit of one selects the original sequential merge.
    pub fn with_kind_compaction_max_lanes(
        mut self,
        kind: IndexKind,
        compaction_max_lanes: u32,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.compaction_max_lanes[kind_slot(kind)] = NonZeroU32::new(compaction_max_lanes)
            .ok_or(IndexRuntimeConfigError::ZeroCompactionLanesForKind(kind))?;
        Ok(self)
    }

    /// Override both construction limits for one kind.
    pub fn with_kind_limits(
        self,
        kind: IndexKind,
        builder_memory_bytes: u64,
        compaction_max_lanes: u32,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.with_kind_builder_memory_bytes(kind, builder_memory_bytes)?
            .with_kind_compaction_max_lanes(kind, compaction_max_lanes)
    }

    /// Hard aggregate build-and-compaction heap budget for `kind`.
    pub fn builder_memory_bytes(self, kind: IndexKind) -> u64 {
        self.builder_memory_bytes[kind_slot(kind)].get()
    }

    pub fn projection_max_lanes(self, kind: IndexKind) -> u32 {
        self.projection_max_lanes[kind_slot(kind)].get()
    }

    pub fn source_quantum_bytes(self, kind: IndexKind) -> u64 {
        self.source_quantum_bytes[kind_slot(kind)].get()
    }

    pub fn external_sort_chunk_bytes(self, kind: IndexKind) -> u64 {
        self.external_sort_chunk_bytes[kind_slot(kind)].get()
    }

    /// Operator ceiling for parallel compaction lanes of `kind`.
    pub fn compaction_max_lanes(self, kind: IndexKind) -> u32 {
        self.compaction_max_lanes[kind_slot(kind)].get()
    }

    pub fn rayon_workers(self) -> u32 {
        self.rayon_workers.get()
    }

    pub fn with_query_max_concurrency(
        mut self,
        concurrency: u32,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.query_max_concurrency =
            NonZeroU32::new(concurrency).ok_or(IndexRuntimeConfigError::ZeroQueryConcurrency)?;
        Ok(self)
    }

    pub fn query_max_concurrency(self) -> u32 {
        self.query_max_concurrency.get()
    }

    pub fn with_query_work_quantum_bytes(
        mut self,
        bytes: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.query_work_quantum_bytes =
            NonZeroU64::new(bytes).ok_or(IndexRuntimeConfigError::ZeroQueryWorkQuantumBytes)?;
        Ok(self)
    }

    pub fn query_work_quantum_bytes(self) -> u64 {
        self.query_work_quantum_bytes.get()
    }

    /// Maximum source-complete immutable runs retained at any one level
    /// before the builder compacts instead of accepting more source work.
    pub fn max_runs_per_level(self, kind: IndexKind) -> u32 {
        self.max_runs_per_level[kind_slot(kind)].get()
    }

    pub fn with_kind_compaction_debt_limits(
        mut self,
        kind: IndexKind,
        max_runs_per_level: u32,
        max_uncompacted_bytes_per_level: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.max_runs_per_level[kind_slot(kind)] = NonZeroU32::new(max_runs_per_level)
            .ok_or(IndexRuntimeConfigError::ZeroRunsPerLevel(kind))?;
        if max_runs_per_level as usize > crate::index_runtime::generation::MAX_RUNS_PER_LEVEL {
            return Err(IndexRuntimeConfigError::TooManyRunsPerLevel {
                configured: max_runs_per_level,
                maximum: crate::index_runtime::generation::MAX_RUNS_PER_LEVEL as u32,
            });
        }
        self.max_uncompacted_bytes_per_level[kind_slot(kind)] =
            NonZeroU64::new(max_uncompacted_bytes_per_level)
                .ok_or(IndexRuntimeConfigError::ZeroUncompactedBytesPerLevel(kind))?;
        Ok(self)
    }

    pub fn max_uncompacted_bytes_per_level(self, kind: IndexKind) -> u64 {
        self.max_uncompacted_bytes_per_level[kind_slot(kind)].get()
    }

    /// Bucket-routed lag at which an incremental builder switches to a scoped
    /// snapshot rebuild. Either non-zero threshold is sufficient.
    pub fn with_auto_rebuild_lag_thresholds(
        mut self,
        entries: u64,
        bytes: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.auto_rebuild_lag_entries =
            NonZeroU64::new(entries).ok_or(IndexRuntimeConfigError::ZeroAutoRebuildLagEntries)?;
        self.auto_rebuild_lag_bytes =
            NonZeroU64::new(bytes).ok_or(IndexRuntimeConfigError::ZeroAutoRebuildLagBytes)?;
        self.auto_rebuild_lag_uses_source_journal_defaults = false;
        Ok(self)
    }

    /// Resolve default automatic-rebuild thresholds from this deployment's
    /// source-journal bounds. Explicit threshold overrides remain unchanged.
    pub fn with_source_journal_rebuild_defaults(
        mut self,
        journal_entries: u64,
        journal_bytes: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        if self.auto_rebuild_lag_uses_source_journal_defaults {
            self.auto_rebuild_lag_entries = NonZeroU64::new(half_nonzero_bound(journal_entries))
                .ok_or(IndexRuntimeConfigError::ZeroAutoRebuildLagEntries)?;
            self.auto_rebuild_lag_bytes = NonZeroU64::new(half_nonzero_bound(journal_bytes))
                .ok_or(IndexRuntimeConfigError::ZeroAutoRebuildLagBytes)?;
        }
        Ok(self)
    }

    pub fn auto_rebuild_lag_entries(self) -> u64 {
        self.auto_rebuild_lag_entries.get()
    }

    pub fn auto_rebuild_lag_bytes(self) -> u64 {
        self.auto_rebuild_lag_bytes.get()
    }

    pub fn max_retained_generations(self) -> u32 {
        self.max_retained_generations.get()
    }

    pub fn max_generation_age_hours(self) -> u64 {
        self.max_generation_age_hours.get()
    }

    pub fn max_retained_generation_bytes(self) -> u64 {
        self.max_retained_generation_bytes.get()
    }
}

const fn kind_slot(kind: IndexKind) -> usize {
    kind as u8 as usize - 1
}

const fn half_nonzero_bound(bound: u64) -> u64 {
    bound / 2 + bound % 2
}

impl Default for IndexRuntimeConfig {
    fn default() -> Self {
        Self::new(
            Self::DEFAULT_DISK_CACHE_BYTES,
            Self::DEFAULT_MEMORY_PERCENT,
            Self::DEFAULT_BUILDER_MEMORY_BYTES_PER_KIND,
            Self::DEFAULT_RAYON_WORKERS,
            Self::DEFAULT_MAX_RETAINED_GENERATIONS,
            Self::DEFAULT_MAX_GENERATION_AGE_HOURS,
            Self::DEFAULT_MAX_RETAINED_GENERATION_BYTES,
        )
        .expect("the built-in index runtime configuration must be valid")
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum IndexRuntimeConfigError {
    #[error("index disk cache byte budget must be greater than zero")]
    ZeroDiskCacheBytes,
    #[error("index memory percentage must be between 1 and 100, got {0}")]
    InvalidMemoryPercent(u8),
    #[error("index builder memory byte budget per kind must be greater than zero")]
    ZeroBuilderMemoryBytesPerKind,
    #[error("index builder memory byte budget for {0:?} must be greater than zero")]
    ZeroBuilderMemoryBytesForKind(IndexKind),
    #[error("index compaction lane limit for {0:?} must be greater than zero")]
    ZeroCompactionLanesForKind(IndexKind),
    #[error("index Rayon worker count must be greater than zero")]
    ZeroRayonWorkers,
    #[error("maximum concurrent index queries must be greater than zero")]
    ZeroQueryConcurrency,
    #[error("index query work quantum bytes must be greater than zero")]
    ZeroQueryWorkQuantumBytes,
    #[error("index projection lanes for {0:?} must be greater than zero")]
    ZeroProjectionLanesForKind(IndexKind),
    #[error("index source quantum for {0:?} must be greater than zero")]
    ZeroSourceQuantumForKind(IndexKind),
    #[error("index external-sort chunk for {0:?} must be greater than zero")]
    ZeroExternalSortChunkForKind(IndexKind),
    #[error("maximum {0:?} index runs per level must be greater than zero")]
    ZeroRunsPerLevel(IndexKind),
    #[error("maximum uncompacted bytes per {0:?} index level must be greater than zero")]
    ZeroUncompactedBytesPerLevel(IndexKind),
    #[error("maximum index runs per level {configured} exceeds format bound {maximum}")]
    TooManyRunsPerLevel { configured: u32, maximum: u32 },
    #[error("automatic index rebuild lag entry threshold must be greater than zero")]
    ZeroAutoRebuildLagEntries,
    #[error("automatic index rebuild lag byte threshold must be greater than zero")]
    ZeroAutoRebuildLagBytes,
    #[error("maximum retained index generations must be greater than zero")]
    ZeroRetainedGenerations,
    #[error("maximum index generation age hours must be greater than zero")]
    ZeroGenerationAgeHours,
    #[error("maximum retained index generation bytes must be greater than zero")]
    ZeroRetainedGenerationBytes,
}

#[cfg(test)]
mod tests {
    use super::*;

    const KINDS: [IndexKind; 8] = [
        IndexKind::Path,
        IndexKind::MetadataFilter,
        IndexKind::TypedJson,
        IndexKind::FullText,
        IndexKind::Vector,
        IndexKind::Hybrid,
        IndexKind::GitSource,
        IndexKind::Tensor,
    ];

    #[test]
    fn defaults_are_conservative_and_valid() {
        let config = IndexRuntimeConfig::default();
        assert_eq!(config.disk_cache_bytes(), 10 * 1024 * 1024 * 1024);
        assert_eq!(config.memory_percent(), 10);
        assert_eq!(config.builder_memory_bytes_per_kind(), 256 * 1024 * 1024);
        assert_eq!(config.rayon_workers(), 4);
        assert_eq!(config.query_max_concurrency(), 64);
        assert_eq!(config.query_work_quantum_bytes(), 4 * 1024 * 1024);
        assert_eq!(config.auto_rebuild_lag_entries(), 500_000);
        assert_eq!(config.auto_rebuild_lag_bytes(), 256 * 1024 * 1024);
        for kind in KINDS {
            assert_eq!(config.builder_memory_bytes(kind), 256 * 1024 * 1024);
            assert_eq!(config.projection_max_lanes(kind), 4);
            assert_eq!(config.source_quantum_bytes(kind), 16 * 1024 * 1024);
            assert_eq!(config.external_sort_chunk_bytes(kind), 16 * 1024 * 1024);
            assert_eq!(config.compaction_max_lanes(kind), 4);
            assert_eq!(config.max_runs_per_level(kind), 64);
            assert_eq!(
                config.max_uncompacted_bytes_per_level(kind),
                1024 * 1024 * 1024
            );
        }
        assert_eq!(config.max_retained_generations(), 3);
        assert_eq!(config.max_generation_age_hours(), 24);
        assert_eq!(
            config.max_retained_generation_bytes(),
            50 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn every_zero_limit_is_rejected() {
        let defaults = IndexRuntimeConfig::default();
        assert_eq!(
            IndexRuntimeConfig::new(
                0,
                defaults.memory_percent(),
                defaults.builder_memory_bytes_per_kind(),
                defaults.rayon_workers(),
                defaults.max_retained_generations(),
                defaults.max_generation_age_hours(),
                defaults.max_retained_generation_bytes(),
            ),
            Err(IndexRuntimeConfigError::ZeroDiskCacheBytes)
        );
        assert_eq!(
            IndexRuntimeConfig::new(
                defaults.disk_cache_bytes(),
                0,
                defaults.builder_memory_bytes_per_kind(),
                defaults.rayon_workers(),
                defaults.max_retained_generations(),
                defaults.max_generation_age_hours(),
                defaults.max_retained_generation_bytes(),
            ),
            Err(IndexRuntimeConfigError::InvalidMemoryPercent(0))
        );
        assert_eq!(
            IndexRuntimeConfig::new(
                defaults.disk_cache_bytes(),
                defaults.memory_percent(),
                0,
                defaults.rayon_workers(),
                defaults.max_retained_generations(),
                defaults.max_generation_age_hours(),
                defaults.max_retained_generation_bytes(),
            ),
            Err(IndexRuntimeConfigError::ZeroBuilderMemoryBytesPerKind)
        );
        assert_eq!(
            IndexRuntimeConfig::new(
                defaults.disk_cache_bytes(),
                defaults.memory_percent(),
                defaults.builder_memory_bytes_per_kind(),
                0,
                defaults.max_retained_generations(),
                defaults.max_generation_age_hours(),
                defaults.max_retained_generation_bytes(),
            ),
            Err(IndexRuntimeConfigError::ZeroRayonWorkers)
        );
        assert_eq!(
            IndexRuntimeConfig::new(
                defaults.disk_cache_bytes(),
                defaults.memory_percent(),
                defaults.builder_memory_bytes_per_kind(),
                defaults.rayon_workers(),
                0,
                defaults.max_generation_age_hours(),
                defaults.max_retained_generation_bytes(),
            ),
            Err(IndexRuntimeConfigError::ZeroRetainedGenerations)
        );
        assert_eq!(
            IndexRuntimeConfig::new(
                defaults.disk_cache_bytes(),
                defaults.memory_percent(),
                defaults.builder_memory_bytes_per_kind(),
                defaults.rayon_workers(),
                defaults.max_retained_generations(),
                0,
                defaults.max_retained_generation_bytes(),
            ),
            Err(IndexRuntimeConfigError::ZeroGenerationAgeHours)
        );
        assert_eq!(
            IndexRuntimeConfig::new(
                defaults.disk_cache_bytes(),
                defaults.memory_percent(),
                defaults.builder_memory_bytes_per_kind(),
                defaults.rayon_workers(),
                defaults.max_retained_generations(),
                defaults.max_generation_age_hours(),
                0,
            ),
            Err(IndexRuntimeConfigError::ZeroRetainedGenerationBytes)
        );
    }

    #[test]
    fn memory_percentage_cannot_exceed_one_hundred() {
        assert_eq!(
            IndexRuntimeConfig::new(1, 101, 1, 1, 1, 1, 1),
            Err(IndexRuntimeConfigError::InvalidMemoryPercent(101))
        );
        assert!(IndexRuntimeConfig::new(1, 100, 1, 1, 1, 1, 1).is_ok());
    }

    #[test]
    fn kind_limits_are_independent_and_keep_the_common_fallback() {
        let configured = IndexRuntimeConfig::default()
            .with_kind_builder_memory_bytes(IndexKind::Path, 96 * 1024 * 1024)
            .unwrap()
            .with_kind_compaction_max_lanes(IndexKind::Path, 2)
            .unwrap()
            .with_kind_limits(IndexKind::Tensor, 192 * 1024 * 1024, 7)
            .unwrap();

        assert_eq!(
            configured.builder_memory_bytes(IndexKind::Path),
            96 * 1024 * 1024
        );
        assert_eq!(configured.compaction_max_lanes(IndexKind::Path), 2);
        assert_eq!(
            configured.builder_memory_bytes(IndexKind::Tensor),
            192 * 1024 * 1024
        );
        assert_eq!(configured.compaction_max_lanes(IndexKind::Tensor), 7);
        assert_eq!(
            configured.builder_memory_bytes(IndexKind::TypedJson),
            configured.builder_memory_bytes_per_kind()
        );
        assert_eq!(configured.compaction_max_lanes(IndexKind::TypedJson), 4);
    }

    #[test]
    fn zero_kind_limits_are_rejected_without_changing_other_kinds() {
        let defaults = IndexRuntimeConfig::default();
        assert_eq!(
            defaults.with_kind_limits(IndexKind::Vector, 0, 1),
            Err(IndexRuntimeConfigError::ZeroBuilderMemoryBytesForKind(
                IndexKind::Vector
            ))
        );
        assert_eq!(
            defaults.with_kind_limits(IndexKind::Vector, 1, 0),
            Err(IndexRuntimeConfigError::ZeroCompactionLanesForKind(
                IndexKind::Vector
            ))
        );
        assert_eq!(
            defaults.with_kind_compaction_debt_limits(IndexKind::Vector, 0, 1),
            Err(IndexRuntimeConfigError::ZeroRunsPerLevel(IndexKind::Vector))
        );
        assert_eq!(
            defaults.with_kind_compaction_debt_limits(IndexKind::Vector, 4, 0),
            Err(IndexRuntimeConfigError::ZeroUncompactedBytesPerLevel(
                IndexKind::Vector
            ))
        );
    }

    #[test]
    fn compaction_debt_limits_are_independent_by_kind() {
        let configured = IndexRuntimeConfig::default()
            .with_kind_compaction_debt_limits(IndexKind::TypedJson, 12, 99)
            .unwrap();
        assert_eq!(configured.max_runs_per_level(IndexKind::TypedJson), 12);
        assert_eq!(
            configured.max_uncompacted_bytes_per_level(IndexKind::TypedJson),
            99
        );
        assert_eq!(configured.max_runs_per_level(IndexKind::Path), 64);
        assert_eq!(
            configured.max_uncompacted_bytes_per_level(IndexKind::Path),
            1024 * 1024 * 1024
        );
    }

    #[test]
    fn a_single_run_limit_is_valid() {
        let configured = IndexRuntimeConfig::default()
            .with_kind_compaction_debt_limits(IndexKind::TypedJson, 1, 99)
            .unwrap();
        assert_eq!(configured.max_runs_per_level(IndexKind::TypedJson), 1);
    }

    #[test]
    fn automatic_rebuild_thresholds_are_nonzero_and_independent() {
        let defaults = IndexRuntimeConfig::default();
        assert_eq!(
            defaults.with_auto_rebuild_lag_thresholds(0, 1),
            Err(IndexRuntimeConfigError::ZeroAutoRebuildLagEntries)
        );
        assert_eq!(
            defaults.with_auto_rebuild_lag_thresholds(1, 0),
            Err(IndexRuntimeConfigError::ZeroAutoRebuildLagBytes)
        );

        let configured = defaults.with_auto_rebuild_lag_thresholds(123, 456).unwrap();
        assert_eq!(configured.auto_rebuild_lag_entries(), 123);
        assert_eq!(configured.auto_rebuild_lag_bytes(), 456);
    }

    #[test]
    fn source_journal_defaults_are_dynamic_but_do_not_replace_overrides() {
        let derived = IndexRuntimeConfig::default()
            .with_source_journal_rebuild_defaults(101, 1001)
            .unwrap();
        assert_eq!(derived.auto_rebuild_lag_entries(), 51);
        assert_eq!(derived.auto_rebuild_lag_bytes(), 501);

        let explicit = IndexRuntimeConfig::default()
            .with_auto_rebuild_lag_thresholds(7, 9)
            .unwrap()
            .with_source_journal_rebuild_defaults(101, 1001)
            .unwrap();
        assert_eq!(explicit.auto_rebuild_lag_entries(), 7);
        assert_eq!(explicit.auto_rebuild_lag_bytes(), 9);
    }
}
