use std::num::{NonZeroU8, NonZeroU32, NonZeroU64};
use std::time::Duration;

use keldra_index::IndexKind;
use thiserror::Error;

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

/// Startup-only budgets and retention bounds shared by every index on a node.
///
/// Authoritative index bytes remain ordinary Keldra objects. The disk and
/// memory values here only bound disposable local materialisations. Retention
/// bounds apply per index and always preserve its current committed view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexRuntimeConfig {
    disk_cache_bytes: NonZeroU64,
    memory_percent: NonZeroU8,
    builder_memory_bytes_per_kind: NonZeroU64,
    builder_memory_bytes: [NonZeroU64; 8],
    projection_max_lanes: [NonZeroU32; 8],
    source_quantum_bytes: [NonZeroU64; 8],
    segment_flush_bytes: [NonZeroU64; 8],
    segment_flush_max_age_millis: NonZeroU64,
    segment_flush_max_operations: [NonZeroU64; 8],
    external_sort_chunk_bytes: [NonZeroU64; 8],
    compaction_max_lanes: [NonZeroU32; 8],
    max_segments_per_tier: [NonZeroU32; 8],
    max_unmerged_bytes_per_tier: [NonZeroU64; 8],
    rayon_workers: NonZeroU32,
    query_max_concurrency: NonZeroU32,
    query_work_quantum_bytes: NonZeroU64,
    query_memory_bytes: NonZeroU64,
    shared_projection_memory_bytes: NonZeroU64,
    working_memory_bytes: Option<NonZeroU64>,
    max_retained_commit_revisions: NonZeroU32,
    max_commit_revision_age_hours: NonZeroU64,
    max_retained_commit_bytes: NonZeroU64,
}

impl IndexRuntimeConfig {
    pub const MAX_RETAINED_COMMIT_REVISIONS: u32 = 64;
    pub const DEFAULT_DISK_CACHE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
    pub const DEFAULT_MEMORY_PERCENT: u8 = 10;
    pub const DEFAULT_BUILDER_MEMORY_BYTES_PER_KIND: u64 = 256 * 1024 * 1024;
    pub const DEFAULT_PROJECTION_MAX_LANES: u32 = 4;
    pub const DEFAULT_SOURCE_QUANTUM_BYTES: u64 = 16 * 1024 * 1024;
    pub const DEFAULT_SEGMENT_FLUSH_BYTES: u64 = 16 * 1024 * 1024;
    pub const DEFAULT_SEGMENT_FLUSH_MAX_AGE_MILLIS: u64 = 1_000;
    pub const DEFAULT_SEGMENT_FLUSH_MAX_OPERATIONS: u64 = 65_536;
    pub const DEFAULT_EXTERNAL_SORT_CHUNK_BYTES: u64 = 16 * 1024 * 1024;
    /// Four lanes fit the default per-kind construction budget. Setting one
    /// explicitly preserves the byte-for-byte sequential compaction path.
    pub const DEFAULT_COMPACTION_MAX_LANES: u32 = 4;
    pub const DEFAULT_RAYON_WORKERS: u32 = 4;
    pub const DEFAULT_QUERY_MAX_CONCURRENCY: u32 = 64;
    pub const DEFAULT_QUERY_WORK_QUANTUM_BYTES: u64 = 4 * 1024 * 1024;
    pub const DEFAULT_QUERY_MEMORY_BYTES: u64 = 512 * 1024 * 1024;
    pub const DEFAULT_SHARED_PROJECTION_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
    pub const DEFAULT_MAX_SEGMENTS_PER_TIER: u32 = 64;
    pub const DEFAULT_MAX_UNMERGED_BYTES_PER_TIER: u64 = 1024 * 1024 * 1024;
    pub const DEFAULT_MAX_RETAINED_COMMIT_REVISIONS: u32 = 3;
    pub const DEFAULT_MAX_COMMIT_REVISION_AGE_HOURS: u64 = 24;
    pub const DEFAULT_MAX_RETAINED_COMMIT_BYTES: u64 = 50 * 1024 * 1024 * 1024;

    pub fn new(
        disk_cache_bytes: u64,
        memory_percent: u8,
        builder_memory_bytes_per_kind: u64,
        rayon_workers: u32,
        max_retained_commit_revisions: u32,
        max_commit_revision_age_hours: u64,
        max_retained_commit_bytes: u64,
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
        let max_retained_commit_revisions = NonZeroU32::new(max_retained_commit_revisions)
            .ok_or(IndexRuntimeConfigError::ZeroRetainedRevisions)?;
        if max_retained_commit_revisions.get() > Self::MAX_RETAINED_COMMIT_REVISIONS {
            return Err(IndexRuntimeConfigError::TooManyRetainedRevisions {
                configured: max_retained_commit_revisions.get(),
                maximum: Self::MAX_RETAINED_COMMIT_REVISIONS,
            });
        }
        let max_commit_revision_age_hours = NonZeroU64::new(max_commit_revision_age_hours)
            .ok_or(IndexRuntimeConfigError::ZeroRevisionAgeHours)?;
        let max_retained_commit_bytes = NonZeroU64::new(max_retained_commit_bytes)
            .ok_or(IndexRuntimeConfigError::ZeroRetainedRevisionBytes)?;

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
            segment_flush_bytes: [NonZeroU64::new(Self::DEFAULT_SEGMENT_FLUSH_BYTES)
                .expect("the default segment flush byte target is positive");
                8],
            segment_flush_max_age_millis: NonZeroU64::new(
                Self::DEFAULT_SEGMENT_FLUSH_MAX_AGE_MILLIS,
            )
            .expect("the default segment flush maximum age is positive"),
            segment_flush_max_operations: [NonZeroU64::new(
                Self::DEFAULT_SEGMENT_FLUSH_MAX_OPERATIONS,
            )
            .expect("the default segment flush operation limit is positive");
                8],
            external_sort_chunk_bytes: [NonZeroU64::new(Self::DEFAULT_EXTERNAL_SORT_CHUNK_BYTES)
                .expect("the default external-sort chunk is positive");
                8],
            compaction_max_lanes: [NonZeroU32::new(Self::DEFAULT_COMPACTION_MAX_LANES)
                .expect("the default compaction lane limit is positive");
                8],
            max_segments_per_tier: [NonZeroU32::new(Self::DEFAULT_MAX_SEGMENTS_PER_TIER)
                .expect("the default segment-debt bound is positive");
                8],
            max_unmerged_bytes_per_tier: [NonZeroU64::new(
                Self::DEFAULT_MAX_UNMERGED_BYTES_PER_TIER,
            )
            .expect("the default byte-debt bound is positive");
                8],
            rayon_workers,
            query_max_concurrency: NonZeroU32::new(Self::DEFAULT_QUERY_MAX_CONCURRENCY)
                .expect("the default query concurrency is positive"),
            query_work_quantum_bytes: NonZeroU64::new(Self::DEFAULT_QUERY_WORK_QUANTUM_BYTES)
                .expect("the default query work quantum is positive"),
            query_memory_bytes: NonZeroU64::new(Self::DEFAULT_QUERY_MEMORY_BYTES)
                .expect("the default query memory budget is positive"),
            shared_projection_memory_bytes: NonZeroU64::new(
                Self::DEFAULT_SHARED_PROJECTION_MEMORY_BYTES,
            )
            .expect("the default shared projection memory budget is positive"),
            working_memory_bytes: None,
            max_retained_commit_revisions,
            max_commit_revision_age_hours,
            max_retained_commit_bytes,
        })
    }

    pub fn disk_cache_bytes(self) -> u64 {
        self.disk_cache_bytes.get()
    }

    pub fn memory_percent(self) -> u8 {
        self.memory_percent.get()
    }

    /// Build-and-compaction fair share for each index kind.
    ///
    /// Every definition of the same kind shares this planning target and may
    /// borrow idle bytes under the aggregate working-memory ceiling.
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

    /// Build-and-compaction fair-share planning target for `kind`.
    pub fn builder_memory_bytes(self, kind: IndexKind) -> u64 {
        self.builder_memory_bytes[kind_slot(kind)].get()
    }

    pub fn projection_max_lanes(self, kind: IndexKind) -> u32 {
        self.projection_max_lanes[kind_slot(kind)].get()
    }

    pub fn source_quantum_bytes(self, kind: IndexKind) -> u64 {
        self.source_quantum_bytes[kind_slot(kind)].get()
    }

    /// Accounted active-builder RAM which freezes one non-empty segment.
    pub fn segment_flush_bytes(self, kind: IndexKind) -> u64 {
        self.segment_flush_bytes[kind_slot(kind)].get()
    }

    /// Maximum age of the oldest mutation in a non-empty active segment.
    pub fn segment_flush_max_age(self) -> Duration {
        Duration::from_millis(self.segment_flush_max_age_millis.get())
    }

    /// Maximum complete source mutation units accumulated in one segment.
    pub fn segment_flush_max_operations(self, kind: IndexKind) -> u64 {
        self.segment_flush_max_operations[kind_slot(kind)].get()
    }

    pub fn with_segment_flush_boundaries(
        mut self,
        bytes: u64,
        max_age_millis: u64,
        max_operations: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.segment_flush_bytes =
            [NonZeroU64::new(bytes).ok_or(IndexRuntimeConfigError::ZeroSegmentFlushBytes)?; 8];
        self.segment_flush_max_age_millis = NonZeroU64::new(max_age_millis)
            .ok_or(IndexRuntimeConfigError::ZeroSegmentFlushMaxAgeMillis)?;
        self.segment_flush_max_operations = [NonZeroU64::new(max_operations)
            .ok_or(IndexRuntimeConfigError::ZeroSegmentFlushOperations)?;
            8];
        Ok(self)
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

    pub fn with_query_memory_bytes(mut self, bytes: u64) -> Result<Self, IndexRuntimeConfigError> {
        self.query_memory_bytes =
            NonZeroU64::new(bytes).ok_or(IndexRuntimeConfigError::ZeroQueryMemoryBytes)?;
        Ok(self)
    }

    /// Fair-share planning target shared by every index query.
    pub fn query_memory_bytes(self) -> u64 {
        self.query_memory_bytes.get()
    }

    pub fn with_shared_projection_memory_bytes(
        mut self,
        bytes: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.shared_projection_memory_bytes = NonZeroU64::new(bytes)
            .ok_or(IndexRuntimeConfigError::ZeroSharedProjectionMemoryBytes)?;
        Ok(self)
    }

    /// Accounted process-local cache and mapping workspace shared by all
    /// assigned index definitions.
    pub fn shared_projection_memory_bytes(self) -> u64 {
        self.shared_projection_memory_bytes.get()
    }

    /// Override the hard aggregate heap ceiling shared by index queries,
    /// builders and compaction. When absent, the checked sum of the query and
    /// per-kind fair shares preserves the former theoretical process maximum.
    pub fn with_working_memory_bytes(
        mut self,
        bytes: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.working_memory_bytes =
            Some(NonZeroU64::new(bytes).ok_or(IndexRuntimeConfigError::ZeroWorkingMemoryBytes)?);
        Ok(self)
    }

    pub fn working_memory_bytes(self) -> Result<u64, IndexRuntimeConfigError> {
        // Shared projection is a permanent accounted resident. Preserve the
        // query reservation and enough room for one complete builder share so
        // an explicit parent cannot validate and then deadlock at startup or
        // make all construction inadmissible.
        let required = self
            .query_memory_bytes()
            .checked_add(self.shared_projection_memory_bytes())
            .and_then(|bytes| {
                bytes.checked_add(
                    KINDS
                        .iter()
                        .map(|kind| self.builder_memory_bytes(*kind))
                        .max()
                        .unwrap_or(0),
                )
            })
            .ok_or(IndexRuntimeConfigError::WorkingMemoryBytesOverflow)?;
        if let Some(configured) = self.working_memory_bytes {
            if configured.get() < required {
                return Err(
                    IndexRuntimeConfigError::WorkingMemoryBelowMandatoryMinimum {
                        configured: configured.get(),
                        minimum: required,
                    },
                );
            }
            return Ok(configured.get());
        }
        KINDS.iter().try_fold(
            self.query_memory_bytes()
                .checked_add(self.shared_projection_memory_bytes())
                .ok_or(IndexRuntimeConfigError::WorkingMemoryBytesOverflow)?,
            |total, kind| {
                total
                    .checked_add(self.builder_memory_bytes(*kind))
                    .ok_or(IndexRuntimeConfigError::WorkingMemoryBytesOverflow)
            },
        )
    }

    /// Maximum source-complete immutable segments retained in one deterministic
    /// size tier before the builder merges instead of adding more debt.
    pub fn max_segments_per_tier(self, kind: IndexKind) -> u32 {
        self.max_segments_per_tier[kind_slot(kind)].get()
    }

    pub fn with_kind_compaction_debt_limits(
        mut self,
        kind: IndexKind,
        max_segments_per_tier: u32,
        max_unmerged_bytes_per_tier: u64,
    ) -> Result<Self, IndexRuntimeConfigError> {
        self.max_segments_per_tier[kind_slot(kind)] = NonZeroU32::new(max_segments_per_tier)
            .ok_or(IndexRuntimeConfigError::ZeroSegmentsPerTier(kind))?;
        if max_segments_per_tier as usize
            > crate::index_runtime::committed_view::MAX_SEGMENTS_PER_COMMIT
        {
            return Err(IndexRuntimeConfigError::TooManySegmentsPerTier {
                configured: max_segments_per_tier,
                maximum: crate::index_runtime::committed_view::MAX_SEGMENTS_PER_COMMIT as u32,
            });
        }
        self.max_unmerged_bytes_per_tier[kind_slot(kind)] =
            NonZeroU64::new(max_unmerged_bytes_per_tier)
                .ok_or(IndexRuntimeConfigError::ZeroUnmergedBytesPerTier(kind))?;
        Ok(self)
    }

    pub fn max_unmerged_bytes_per_tier(self, kind: IndexKind) -> u64 {
        self.max_unmerged_bytes_per_tier[kind_slot(kind)].get()
    }

    pub fn max_retained_commit_revisions(self) -> u32 {
        self.max_retained_commit_revisions.get()
    }

    pub fn max_commit_revision_age_hours(self) -> u64 {
        self.max_commit_revision_age_hours.get()
    }

    pub fn max_retained_commit_bytes(self) -> u64 {
        self.max_retained_commit_bytes.get()
    }
}

const fn kind_slot(kind: IndexKind) -> usize {
    kind as u8 as usize - 1
}

impl Default for IndexRuntimeConfig {
    fn default() -> Self {
        Self::new(
            Self::DEFAULT_DISK_CACHE_BYTES,
            Self::DEFAULT_MEMORY_PERCENT,
            Self::DEFAULT_BUILDER_MEMORY_BYTES_PER_KIND,
            Self::DEFAULT_RAYON_WORKERS,
            Self::DEFAULT_MAX_RETAINED_COMMIT_REVISIONS,
            Self::DEFAULT_MAX_COMMIT_REVISION_AGE_HOURS,
            Self::DEFAULT_MAX_RETAINED_COMMIT_BYTES,
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
    #[error("global index query memory bytes must be greater than zero")]
    ZeroQueryMemoryBytes,
    #[error("shared index projection memory bytes must be greater than zero")]
    ZeroSharedProjectionMemoryBytes,
    #[error("aggregate index working memory bytes must be greater than zero")]
    ZeroWorkingMemoryBytes,
    #[error("aggregate index working memory {configured} cannot admit mandatory request {minimum}")]
    WorkingMemoryBelowMandatoryMinimum { configured: u64, minimum: u64 },
    #[error("default aggregate index working memory sum overflows u64")]
    WorkingMemoryBytesOverflow,
    #[error("index projection lanes for {0:?} must be greater than zero")]
    ZeroProjectionLanesForKind(IndexKind),
    #[error("index source quantum for {0:?} must be greater than zero")]
    ZeroSourceQuantumForKind(IndexKind),
    #[error("index segment flush byte target must be greater than zero")]
    ZeroSegmentFlushBytes,
    #[error("index segment flush maximum age milliseconds must be greater than zero")]
    ZeroSegmentFlushMaxAgeMillis,
    #[error("index segment flush operation limit must be greater than zero")]
    ZeroSegmentFlushOperations,
    #[error("index external-sort chunk for {0:?} must be greater than zero")]
    ZeroExternalSortChunkForKind(IndexKind),
    #[error("maximum {0:?} index segments per size tier must be greater than zero")]
    ZeroSegmentsPerTier(IndexKind),
    #[error("maximum unmerged bytes per {0:?} index size tier must be greater than zero")]
    ZeroUnmergedBytesPerTier(IndexKind),
    #[error("maximum index segments per size tier {configured} exceeds format bound {maximum}")]
    TooManySegmentsPerTier { configured: u32, maximum: u32 },
    #[error("maximum retained index commit revisions must be greater than zero")]
    ZeroRetainedRevisions,
    #[error("maximum retained index commit revisions {configured} exceeds format bound {maximum}")]
    TooManyRetainedRevisions { configured: u32, maximum: u32 },
    #[error("maximum index revision age hours must be greater than zero")]
    ZeroRevisionAgeHours,
    #[error("maximum retained index revision bytes must be greater than zero")]
    ZeroRetainedRevisionBytes,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_conservative_and_valid() {
        let config = IndexRuntimeConfig::default();
        assert_eq!(config.disk_cache_bytes(), 10 * 1024 * 1024 * 1024);
        assert_eq!(config.memory_percent(), 10);
        assert_eq!(config.builder_memory_bytes_per_kind(), 256 * 1024 * 1024);
        assert_eq!(config.rayon_workers(), 4);
        assert_eq!(config.query_max_concurrency(), 64);
        assert_eq!(config.query_work_quantum_bytes(), 4 * 1024 * 1024);
        assert_eq!(config.query_memory_bytes(), 512 * 1024 * 1024);
        assert_eq!(config.shared_projection_memory_bytes(), 256 * 1024 * 1024);
        assert_eq!(
            config.working_memory_bytes().unwrap(),
            512 * 1024 * 1024 + 256 * 1024 * 1024 + 8 * 256 * 1024 * 1024
        );
        for kind in KINDS {
            assert_eq!(config.builder_memory_bytes(kind), 256 * 1024 * 1024);
            assert_eq!(config.projection_max_lanes(kind), 4);
            assert_eq!(config.source_quantum_bytes(kind), 16 * 1024 * 1024);
            assert_eq!(config.external_sort_chunk_bytes(kind), 16 * 1024 * 1024);
            assert_eq!(config.compaction_max_lanes(kind), 4);
            assert_eq!(config.max_segments_per_tier(kind), 64);
            assert_eq!(config.max_unmerged_bytes_per_tier(kind), 1024 * 1024 * 1024);
        }
        for kind in KINDS {
            assert_eq!(config.segment_flush_bytes(kind), 16 * 1024 * 1024);
            assert_eq!(config.segment_flush_max_operations(kind), 65_536);
        }
        assert_eq!(config.segment_flush_max_age(), Duration::from_secs(1));
        assert_eq!(config.max_retained_commit_revisions(), 3);
        assert_eq!(config.max_commit_revision_age_hours(), 24);
        assert_eq!(config.max_retained_commit_bytes(), 50 * 1024 * 1024 * 1024);
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
                defaults.max_retained_commit_revisions(),
                defaults.max_commit_revision_age_hours(),
                defaults.max_retained_commit_bytes(),
            ),
            Err(IndexRuntimeConfigError::ZeroDiskCacheBytes)
        );
        assert_eq!(
            IndexRuntimeConfig::new(
                defaults.disk_cache_bytes(),
                0,
                defaults.builder_memory_bytes_per_kind(),
                defaults.rayon_workers(),
                defaults.max_retained_commit_revisions(),
                defaults.max_commit_revision_age_hours(),
                defaults.max_retained_commit_bytes(),
            ),
            Err(IndexRuntimeConfigError::InvalidMemoryPercent(0))
        );
        assert_eq!(
            IndexRuntimeConfig::new(
                defaults.disk_cache_bytes(),
                defaults.memory_percent(),
                0,
                defaults.rayon_workers(),
                defaults.max_retained_commit_revisions(),
                defaults.max_commit_revision_age_hours(),
                defaults.max_retained_commit_bytes(),
            ),
            Err(IndexRuntimeConfigError::ZeroBuilderMemoryBytesPerKind)
        );
        assert_eq!(
            IndexRuntimeConfig::new(
                defaults.disk_cache_bytes(),
                defaults.memory_percent(),
                defaults.builder_memory_bytes_per_kind(),
                0,
                defaults.max_retained_commit_revisions(),
                defaults.max_commit_revision_age_hours(),
                defaults.max_retained_commit_bytes(),
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
                defaults.max_commit_revision_age_hours(),
                defaults.max_retained_commit_bytes(),
            ),
            Err(IndexRuntimeConfigError::ZeroRetainedRevisions)
        );
        assert_eq!(
            IndexRuntimeConfig::new(
                defaults.disk_cache_bytes(),
                defaults.memory_percent(),
                defaults.builder_memory_bytes_per_kind(),
                defaults.rayon_workers(),
                defaults.max_retained_commit_revisions(),
                0,
                defaults.max_retained_commit_bytes(),
            ),
            Err(IndexRuntimeConfigError::ZeroRevisionAgeHours)
        );
        assert_eq!(
            IndexRuntimeConfig::new(
                defaults.disk_cache_bytes(),
                defaults.memory_percent(),
                defaults.builder_memory_bytes_per_kind(),
                defaults.rayon_workers(),
                defaults.max_retained_commit_revisions(),
                defaults.max_commit_revision_age_hours(),
                0,
            ),
            Err(IndexRuntimeConfigError::ZeroRetainedRevisionBytes)
        );
    }

    #[test]
    fn segment_flush_boundaries_are_explicit_and_nonzero() {
        let config = IndexRuntimeConfig::default()
            .with_segment_flush_boundaries(8 * 1024 * 1024, 250, 4_096)
            .unwrap();
        assert_eq!(config.segment_flush_bytes(IndexKind::Path), 8 * 1024 * 1024);
        assert_eq!(config.segment_flush_max_age(), Duration::from_millis(250));
        assert_eq!(config.segment_flush_max_operations(IndexKind::Path), 4_096);

        assert_eq!(
            IndexRuntimeConfig::default().with_segment_flush_boundaries(0, 1, 1),
            Err(IndexRuntimeConfigError::ZeroSegmentFlushBytes)
        );
        assert_eq!(
            IndexRuntimeConfig::default().with_segment_flush_boundaries(1, 0, 1),
            Err(IndexRuntimeConfigError::ZeroSegmentFlushMaxAgeMillis)
        );
        assert_eq!(
            IndexRuntimeConfig::default().with_segment_flush_boundaries(1, 1, 0),
            Err(IndexRuntimeConfigError::ZeroSegmentFlushOperations)
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
    fn explicit_working_memory_is_a_hard_parent_not_a_replacement_for_shares() {
        let config = IndexRuntimeConfig::default()
            .with_working_memory_bytes(1024 * 1024 * 1024)
            .unwrap();
        assert_eq!(config.working_memory_bytes().unwrap(), 1024 * 1024 * 1024);
        assert_eq!(config.query_memory_bytes(), 512 * 1024 * 1024);
        assert_eq!(
            config.builder_memory_bytes(IndexKind::TypedJson),
            256 * 1024 * 1024
        );
    }

    #[test]
    fn explicit_working_memory_must_admit_each_configured_share() {
        let config = IndexRuntimeConfig::default()
            .with_working_memory_bytes(256 * 1024 * 1024)
            .unwrap();
        assert_eq!(
            config.working_memory_bytes(),
            Err(
                IndexRuntimeConfigError::WorkingMemoryBelowMandatoryMinimum {
                    configured: 256 * 1024 * 1024,
                    minimum: 1024 * 1024 * 1024,
                }
            )
        );
        assert_eq!(
            IndexRuntimeConfig::default().with_working_memory_bytes(0),
            Err(IndexRuntimeConfigError::ZeroWorkingMemoryBytes)
        );
        assert_eq!(
            IndexRuntimeConfig::default().with_shared_projection_memory_bytes(0),
            Err(IndexRuntimeConfigError::ZeroSharedProjectionMemoryBytes)
        );
    }

    #[test]
    fn retained_generation_count_cannot_exceed_the_scratch_rank_format() {
        assert_eq!(
            IndexRuntimeConfig::new(
                1,
                10,
                256 * 1024 * 1024,
                4,
                IndexRuntimeConfig::MAX_RETAINED_COMMIT_REVISIONS + 1,
                24,
                50 * 1024 * 1024 * 1024,
            ),
            Err(IndexRuntimeConfigError::TooManyRetainedRevisions {
                configured: IndexRuntimeConfig::MAX_RETAINED_COMMIT_REVISIONS + 1,
                maximum: IndexRuntimeConfig::MAX_RETAINED_COMMIT_REVISIONS,
            })
        );
        assert!(
            IndexRuntimeConfig::new(
                1,
                10,
                256 * 1024 * 1024,
                4,
                IndexRuntimeConfig::MAX_RETAINED_COMMIT_REVISIONS,
                24,
                50 * 1024 * 1024 * 1024,
            )
            .is_ok()
        );
    }

    #[test]
    fn global_query_memory_is_nonzero_and_independent() {
        let defaults = IndexRuntimeConfig::default();
        assert_eq!(
            defaults.with_query_memory_bytes(0),
            Err(IndexRuntimeConfigError::ZeroQueryMemoryBytes)
        );
        let configured = defaults.with_query_memory_bytes(123_456).unwrap();
        assert_eq!(configured.query_memory_bytes(), 123_456);
        assert_eq!(
            configured.builder_memory_bytes(IndexKind::TypedJson),
            defaults.builder_memory_bytes(IndexKind::TypedJson)
        );
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
            Err(IndexRuntimeConfigError::ZeroSegmentsPerTier(
                IndexKind::Vector
            ))
        );
        assert_eq!(
            defaults.with_kind_compaction_debt_limits(IndexKind::Vector, 4, 0),
            Err(IndexRuntimeConfigError::ZeroUnmergedBytesPerTier(
                IndexKind::Vector
            ))
        );
    }

    #[test]
    fn compaction_debt_limits_are_independent_by_kind() {
        let configured = IndexRuntimeConfig::default()
            .with_kind_compaction_debt_limits(IndexKind::TypedJson, 12, 99)
            .unwrap();
        assert_eq!(configured.max_segments_per_tier(IndexKind::TypedJson), 12);
        assert_eq!(
            configured.max_unmerged_bytes_per_tier(IndexKind::TypedJson),
            99
        );
        assert_eq!(configured.max_segments_per_tier(IndexKind::Path), 64);
        assert_eq!(
            configured.max_unmerged_bytes_per_tier(IndexKind::Path),
            1024 * 1024 * 1024
        );
    }

    #[test]
    fn a_single_segment_limit_is_valid() {
        let configured = IndexRuntimeConfig::default()
            .with_kind_compaction_debt_limits(IndexKind::TypedJson, 1, 99)
            .unwrap();
        assert_eq!(configured.max_segments_per_tier(IndexKind::TypedJson), 1);
    }
}
