//! Portable native index components and storage-neutral query execution for
//! Anvil.
//!
//! The crate owns format-v4 codecs and algorithms. RocksDB, erasure coding,
//! placement, authorization, cache policy, and ordinary-object publication
//! remain Anvil runtime responsibilities.

pub mod compaction;
mod error;
mod io;
pub mod v4;

pub use compaction::{
    FIXED_INDEX_SEAL_WORKSPACE_BYTES, MIN_INDEX_KIND_MEMORY_BYTES, SegmentMemoryPlan,
};
pub use error::IndexError;
pub use io::IndexFileRead;
pub use v4::{INDEX_FORMAT_VERSION, IndexKind};
