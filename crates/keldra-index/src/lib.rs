//! Portable Typed JSON vocabulary and native index components for Keldra.
//!
//! The crate owns the storage-neutral Typed JSON contract and the clean-break
//! format-v6 partition projection codecs. RocksDB, erasure coding, placement,
//! authorization, cache policy, query materialization, and ordinary-object
//! publication remain Keldra runtime responsibilities.

pub mod compaction;
mod error;
mod io;
pub mod typed_json;
pub mod v6;

pub use compaction::{
    FIXED_INDEX_SEAL_WORKSPACE_BYTES, MIN_INDEX_KIND_MEMORY_BYTES, SegmentMemoryPlan,
};
pub use error::IndexError;
pub use io::IndexFileRead;
pub use v6::INDEX_FORMAT_VERSION;
