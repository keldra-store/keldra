//! Versioned index formats and query engines.
//!
//! This crate deliberately knows nothing about RocksDB, erasure coding, node
//! placement, or the local cache. Engines read immutable named files through
//! [`IndexDirectoryRead`]. The server supplies those files from its shared
//! distributed index cache and publishes generated files through the ordinary
//! Anvil object path.

mod artifact;
mod error;
mod io;
mod key;
mod model;
mod paged_map;

pub mod full_text;
pub mod hybrid;
pub mod ordered;
pub mod projections;
pub mod typed_json;
pub mod vector;

pub use artifact::{GeneratedFile, IndexArtifacts};
pub use error::IndexError;
pub use io::{IndexDirectoryRead, IndexFileRead};
pub use model::{DocumentRef, IndexKind, QueryHit};
pub use paged_map::{DEFAULT_PAGE_BYTES, MapRecord, PagedMap, PagedMapBuilder};
