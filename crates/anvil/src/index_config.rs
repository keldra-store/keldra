use std::num::{NonZeroU8, NonZeroU32, NonZeroU64};

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
    rayon_workers: NonZeroU32,
    max_retained_generations: NonZeroU32,
    max_generation_age_hours: NonZeroU64,
    max_retained_generation_bytes: NonZeroU64,
}

impl IndexRuntimeConfig {
    pub const DEFAULT_DISK_CACHE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
    pub const DEFAULT_MEMORY_PERCENT: u8 = 10;
    pub const DEFAULT_BUILDER_MEMORY_BYTES_PER_KIND: u64 = 64 * 1024 * 1024;
    pub const DEFAULT_RAYON_WORKERS: u32 = 4;
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
            rayon_workers,
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

    pub fn rayon_workers(self) -> u32 {
        self.rayon_workers.get()
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
    #[error("index Rayon worker count must be greater than zero")]
    ZeroRayonWorkers,
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

    #[test]
    fn defaults_are_conservative_and_valid() {
        let config = IndexRuntimeConfig::default();
        assert_eq!(config.disk_cache_bytes(), 10 * 1024 * 1024 * 1024);
        assert_eq!(config.memory_percent(), 10);
        assert_eq!(config.builder_memory_bytes_per_kind(), 64 * 1024 * 1024);
        assert_eq!(config.rayon_workers(), 4);
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
}
