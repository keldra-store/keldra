//! Direct bounded construction of one L1 base run from canonical source order.
//!
//! Primary path and range-local identity components stream straight to the
//! caller's ordinary block sink. Only secondary keys use the sink's disposable
//! scratch lanes for bounded external sorting.

mod full_text;
mod ordered;
mod projections;
#[cfg(test)]
mod tests;
mod typed_json;
mod vector;

pub use full_text::{FullTextBulkBuilder, HybridBulkBuilder};
pub use ordered::PathBulkBuilder;
pub use projections::{GitSourceBulkBuilder, TensorBulkBuilder};
pub use typed_json::{MetadataBulkBuilder, TypedJsonBulkBuilder};
pub use vector::VectorBulkBuilder;

use crate::run::RunStatistics;
use crate::{IndexError, MAX_INDEX_BLOCK_BYTES};

/// Direct builders finish one source-work quantum as one deterministic path
/// range. A range can expand to at most three routing levels under the fixed
/// 512 KiB block bound; the final streaming assembler has five further levels
/// available for millions of ranges without retaining their roots.
pub(crate) const BULK_RANGE_ROOT_HEIGHT: u8 = 3;
pub(crate) const BULK_OUTPUT_LEVEL: u8 = 1;
pub(crate) const BULK_TARGET_BLOCK_BYTES: usize = MAX_INDEX_BLOCK_BYTES - 64 * 1024;
const LOCAL_ORDINAL_BITS: u32 = 32;
const MAX_LOCAL_ORDINAL: u64 = (1u64 << LOCAL_ORDINAL_BITS) - 1;
const MAX_RANGE_ID: u64 = (1u64 << (u64::BITS - LOCAL_ORDINAL_BITS)) - 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BulkBuildOptions {
    /// Maximum resident bytes in each secondary-key external-sort chunk.
    pub max_sort_chunk_bytes: usize,
    /// Maximum deterministic final-rewrite stripes for secondary keys.
    pub max_rewrite_lanes: usize,
}

impl BulkBuildOptions {
    pub fn new(max_sort_chunk_bytes: usize, max_rewrite_lanes: usize) -> Result<Self, IndexError> {
        if max_sort_chunk_bytes == 0 || max_rewrite_lanes == 0 {
            return Err(IndexError::InvalidDefinition(
                "bulk external-sort chunk and rewrite lanes must be nonzero".into(),
            ));
        }
        Ok(Self {
            max_sort_chunk_bytes,
            max_rewrite_lanes,
        })
    }
}

pub(crate) fn range_ordinal_base(range_id: u64) -> Result<u64, IndexError> {
    if range_id > MAX_RANGE_ID {
        return Err(IndexError::ResourceLimit {
            needed: usize::try_from(range_id).unwrap_or(usize::MAX),
            limit: MAX_RANGE_ID as usize,
        });
    }
    Ok(range_id << LOCAL_ORDINAL_BITS)
}

pub(crate) fn range_local_ordinal(base: u64, local: u64) -> Result<u64, IndexError> {
    if local > MAX_LOCAL_ORDINAL || base & MAX_LOCAL_ORDINAL != 0 {
        return Err(IndexError::OffsetOverflow);
    }
    base.checked_add(local).ok_or(IndexError::OffsetOverflow)
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BulkStatistics {
    mutation_count: u64,
    live_document_count: u64,
    minimum_version: u64,
    maximum_version: u64,
}

impl Default for BulkStatistics {
    fn default() -> Self {
        Self {
            mutation_count: 0,
            live_document_count: 0,
            minimum_version: u64::MAX,
            maximum_version: 0,
        }
    }
}

impl BulkStatistics {
    pub(crate) fn record(&mut self, version: u64, live: bool) -> Result<(), IndexError> {
        if version == 0 {
            return Err(IndexError::InvalidDefinition(
                "bulk source version must be nonzero".into(),
            ));
        }
        self.mutation_count = self
            .mutation_count
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        if live {
            self.live_document_count = self
                .live_document_count
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        self.minimum_version = self.minimum_version.min(version);
        self.maximum_version = self.maximum_version.max(version);
        Ok(())
    }

    pub(crate) fn is_empty(self) -> bool {
        self.mutation_count == 0
    }

    pub(crate) fn finish(self) -> Result<RunStatistics, IndexError> {
        if self.is_empty() {
            return Err(IndexError::InvalidDefinition(
                "an empty bulk builder has no run statistics".into(),
            ));
        }
        Ok(RunStatistics {
            mutation_count: self.mutation_count,
            live_document_count: self.live_document_count,
            minimum_version: self.minimum_version,
            maximum_version: self.maximum_version,
        })
    }
}
