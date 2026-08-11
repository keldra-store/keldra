//! Bounded immutable index segments and lazy multi-segment query engines.
//!
//! The crate owns portable version-2 component encodings. It deliberately
//! knows nothing about RocksDB, erasure coding, placement, manifests, or cache
//! policy. Anvil publishes every sealed component through its ordinary object
//! path and supplies materialized bytes through [`IndexDirectoryRead`].

mod artifact;
mod codec;
pub mod compaction;
mod error;
mod io;
mod model;
mod query_bounds;
mod routed;
mod routed_sort;
mod run;
mod segment;
mod succinct;

pub mod full_text;
pub mod hybrid;
pub mod ordered;
pub mod projections;
pub mod typed_json;
pub mod vector;

pub use artifact::{BlockDescriptor, GeneratedBlock, RunDescriptor, SealedRun};
pub use error::IndexError;
pub use io::{IndexBlockSink, IndexDirectoryRead, IndexFileRead};
pub use model::{
    ComponentCodec, DocumentRef, FIXED_INDEX_SEAL_WORKSPACE_BYTES, INDEX_ROUTING_FANOUT, IndexKind,
    IndexMutation, MAX_INDEX_BLOCK_BYTES, MAX_INDEX_DECODED_BLOCK_BYTES,
    MAX_INDEX_ROUTING_BLOCK_BYTES, MAX_INDEX_ROUTING_HEIGHT, MAX_INDEX_ROUTING_KEY_BYTES,
    MAX_INDEX_ROUTING_WORKSPACE_BYTES, MAX_RUN_COMPONENTS, MIN_INDEX_KIND_MEMORY_BYTES, QueryHit,
    SegmentBuildOptions, SegmentMemoryPlan, SegmentPush,
};
pub use run::RunBlockWalker;

pub const INDEX_FORMAT_VERSION: u16 = 2;
