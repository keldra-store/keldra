use serde::{Deserialize, Serialize};

use crate::IndexError;

/// Fixed format-2 leaf/routing object ceiling, including its component
/// envelope. Writers split before crossing this boundary.
pub const MAX_INDEX_BLOCK_BYTES: usize = 512 * 1024;
/// Maximum decoded allocation charged while reconstructing one encoded block.
/// Four-way compaction therefore retains at most 16 MiB of decoded input blocks.
pub const MAX_INDEX_DECODED_BLOCK_BYTES: usize = 4 * 1024 * 1024;
/// Object paths are the largest routing keys in format 2.
pub const MAX_INDEX_ROUTING_KEY_BYTES: usize = 4096;
pub const INDEX_ROUTING_FANOUT: usize = 32;
pub const MAX_INDEX_ROUTING_HEIGHT: usize = 8;
pub const MAX_RUN_COMPONENTS: usize = 16;

const COMPONENT_HEADER_BYTES: usize = 54;
const DESCRIPTOR_FIXED_BYTES: usize = 4 + 8 + 16 + 32;
const MAX_DESCRIPTOR_ENCODED_BYTES: usize =
    DESCRIPTOR_FIXED_BYTES + 2 * MAX_INDEX_ROUTING_KEY_BYTES;
const MAX_DESCRIPTOR_RESIDENT_BYTES: usize = 256 + 2 * MAX_INDEX_ROUTING_KEY_BYTES;

/// Largest canonical routing object at the fixed format-2 fanout.
pub const MAX_INDEX_ROUTING_BLOCK_BYTES: usize =
    COMPONENT_HEADER_BYTES + 1 + 4 + INDEX_ROUTING_FANOUT * MAX_DESCRIPTOR_ENCODED_BYTES;
/// Fixed routing-descriptor and run-root workspace retained while streaming a
/// component. This uses the maximum supported height and key size.
pub const MAX_INDEX_ROUTING_WORKSPACE_BYTES: usize =
    MAX_INDEX_ROUTING_HEIGHT * INDEX_ROUTING_FANOUT * MAX_DESCRIPTOR_RESIDENT_BYTES
        + COMPONENT_HEADER_BYTES
        + 1
        + 4 * 8
        + 4
        + MAX_RUN_COMPONENTS * (8 + MAX_DESCRIPTOR_ENCODED_BYTES);
/// One bounded row batch, codec input/output arrays, one move-only emitted
/// object, and the nine encoded-plus-decoded leaves retained by four-way
/// compaction: four sequential cursor leaves plus four point-read input leaves
/// and one staged-output point leaf. Engine-specific derived state is charged
/// separately at no more than its admitted resident mutation bytes.
pub const FIXED_INDEX_SEAL_WORKSPACE_BYTES: usize = MAX_INDEX_ROUTING_WORKSPACE_BYTES
    + 5 * MAX_INDEX_BLOCK_BYTES
    + 9 * (MAX_INDEX_BLOCK_BYTES + MAX_INDEX_DECODED_BLOCK_BYTES);
pub const MIN_INDEX_KIND_MEMORY_BYTES: usize = 64 * 1024 * 1024;

/// Conservative partition of one process-wide per-kind construction budget.
/// At seal time the resident mutations remain live, derived engine state may
/// use the same charged amount once, and fixed block/routing work uses the
/// exported constant. The sum therefore never exceeds `total_bytes`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentMemoryPlan {
    pub total_bytes: usize,
    pub max_resident_bytes: usize,
    pub max_source_projection_bytes: usize,
}

impl SegmentMemoryPlan {
    pub fn new(total_bytes: usize) -> Result<Self, IndexError> {
        if total_bytes < MIN_INDEX_KIND_MEMORY_BYTES
            || total_bytes <= FIXED_INDEX_SEAL_WORKSPACE_BYTES
        {
            return Err(IndexError::ResourceLimit {
                needed: MIN_INDEX_KIND_MEMORY_BYTES,
                limit: total_bytes,
            });
        }
        let max_resident_bytes = (total_bytes - FIXED_INDEX_SEAL_WORKSPACE_BYTES) / 2;
        Ok(Self {
            total_bytes,
            max_resident_bytes,
            max_source_projection_bytes: total_bytes - max_resident_bytes,
        })
    }

    pub fn seal_workspace_bytes(self, resident_bytes: usize) -> Result<usize, IndexError> {
        if resident_bytes > self.max_resident_bytes {
            return Err(IndexError::ResourceLimit {
                needed: resident_bytes,
                limit: self.max_resident_bytes,
            });
        }
        resident_bytes
            .checked_add(FIXED_INDEX_SEAL_WORKSPACE_BYTES)
            .ok_or(IndexError::OffsetOverflow)
    }

    pub fn options(self) -> Result<SegmentBuildOptions, IndexError> {
        SegmentBuildOptions::new(self.max_resident_bytes)
    }
}

/// The eight index capabilities supported by Anvil 0.7.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum IndexKind {
    Path = 1,
    MetadataFilter = 2,
    TypedJson = 3,
    FullText = 4,
    Vector = 5,
    Hybrid = 6,
    GitSource = 7,
    Tensor = 8,
}

impl IndexKind {
    pub(crate) fn from_tag(tag: u8) -> Result<Self, IndexError> {
        match tag {
            1 => Ok(Self::Path),
            2 => Ok(Self::MetadataFilter),
            3 => Ok(Self::TypedJson),
            4 => Ok(Self::FullText),
            5 => Ok(Self::Vector),
            6 => Ok(Self::Hybrid),
            7 => Ok(Self::GitSource),
            8 => Ok(Self::Tensor),
            _ => Err(IndexError::InvalidFormat("unknown index kind")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ComponentCodec {
    FixedRows = 1,
    GapPostings = 2,
    PrefixEliasFano = 3,
    QuasiSuccinctPostings = 4,
    FixedVectors = 5,
}

impl ComponentCodec {
    pub(crate) fn from_tag(tag: u8) -> Result<Self, IndexError> {
        match tag {
            1 => Ok(Self::FixedRows),
            2 => Ok(Self::GapPostings),
            3 => Ok(Self::PrefixEliasFano),
            4 => Ok(Self::QuasiSuccinctPostings),
            5 => Ok(Self::FixedVectors),
            _ => Err(IndexError::InvalidFormat("unknown component codec")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DocumentRef {
    pub path: String,
    pub version: u64,
}

impl DocumentRef {
    pub(crate) fn validate(&self) -> Result<(), IndexError> {
        if self.version == 0
            || self.path.is_empty()
            || self.path.contains('\0')
            || self.path.len() > MAX_INDEX_ROUTING_KEY_BYTES
        {
            return Err(IndexError::InvalidDefinition(
                "indexed object version must be non-zero and its path must be 1..=4096 bytes without NUL".into(),
            ));
        }
        Ok(())
    }
}

/// One ordered source-object change consumed by an index builder.
///
/// A removal is retained as a tombstone in the common document component so
/// it shadows an older upsert in another immutable segment.
#[derive(Clone, Debug, PartialEq)]
pub enum IndexMutation<T> {
    Upsert(T),
    Remove(DocumentRef),
}

impl<T> IndexMutation<T> {
    pub fn removed(document: DocumentRef) -> Self {
        Self::Remove(document)
    }
}

/// Result of bounded builder admission. `Full` returns the untouched mutation
/// so the caller can seal the current segment, release it, and retry.
#[derive(Clone, Debug, PartialEq)]
pub enum SegmentPush<T> {
    Accepted,
    Full(IndexMutation<T>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentBuildOptions {
    pub max_resident_bytes: usize,
    pub level: u8,
}

impl SegmentBuildOptions {
    pub fn new(max_resident_bytes: usize) -> Result<Self, IndexError> {
        if max_resident_bytes == 0 {
            return Err(IndexError::InvalidDefinition(
                "segment memory limit must be greater than zero".into(),
            ));
        }
        Ok(Self {
            max_resident_bytes,
            level: 0,
        })
    }

    pub fn for_level(max_resident_bytes: usize, level: u8) -> Result<Self, IndexError> {
        let mut options = Self::new(max_resident_bytes)?;
        options.level = level;
        Ok(options)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryHit {
    pub document: DocumentRef,
    pub score: Option<f32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_plan_accounts_for_ingest_and_seal_peaks() {
        let total = 64 * 1024 * 1024;
        let plan = SegmentMemoryPlan::new(total).unwrap();
        assert_eq!(
            plan.max_resident_bytes + plan.max_source_projection_bytes,
            total
        );
        let seal = plan.seal_workspace_bytes(plan.max_resident_bytes).unwrap();
        assert!(plan.max_resident_bytes + seal <= total);
        assert!(total - (plan.max_resident_bytes + seal) <= 1);
    }

    #[test]
    fn undersized_kind_budget_fails_at_startup_planning() {
        assert!(SegmentMemoryPlan::new(MIN_INDEX_KIND_MEMORY_BYTES - 1).is_err());
        assert!(SegmentMemoryPlan::new(MIN_INDEX_KIND_MEMORY_BYTES).is_ok());
    }

    #[test]
    fn fixed_routing_objects_fit_the_block_ceiling() {
        assert!(MAX_INDEX_ROUTING_BLOCK_BYTES <= MAX_INDEX_BLOCK_BYTES);
        assert!(FIXED_INDEX_SEAL_WORKSPACE_BYTES < MIN_INDEX_KIND_MEMORY_BYTES);
        assert_eq!(9 * MAX_INDEX_DECODED_BLOCK_BYTES, 36 * 1024 * 1024);
    }

    #[test]
    fn document_references_require_a_real_object_version() {
        assert!(
            DocumentRef {
                path: "objects/example".into(),
                version: 0,
            }
            .validate()
            .is_err()
        );
    }
}
