use std::future::Future;

use crate::IndexError;

/// An immutable byte source supplied by Anvil's cache manager.
///
/// A returned slice owns or pins its backing bytes. Reads may return fewer
/// bytes than requested at a cache-segment boundary and return an empty slice
/// only at EOF.
pub trait IndexFileRead: Send + Sync {
    type Slice: AsRef<[u8]> + Send + Sync + 'static;

    fn read_at(
        &self,
        offset: u64,
        max_length: usize,
    ) -> impl Future<Output = Result<Self::Slice, IndexError>> + Send;
}
