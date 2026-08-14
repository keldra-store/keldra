use std::future::Future;

use crate::IndexError;

/// One restart-disposable random-access file used while merging immutable
/// format-v4 segments.
///
/// Scratch bytes are never referenced by a segment, generation manifest,
/// Raft record, or ordinary object.  A caller owns their lifetime and may
/// discard the complete workspace after success, failure, or restart.
pub trait MergeScratchFile: Clone + Send + Sync + 'static {
    /// Grow the file to `length` bytes. Newly exposed bytes must read as zero.
    fn resize_zeroed(&self, length: u64) -> impl Future<Output = Result<(), IndexError>> + Send;

    /// Replace exactly `bytes.len()` bytes at `offset`.
    fn write_all_at(
        &self,
        offset: u64,
        bytes: Vec<u8>,
    ) -> impl Future<Output = Result<(), IndexError>> + Send;

    /// Append one complete record and return its starting offset.
    fn append(&self, bytes: Vec<u8>) -> impl Future<Output = Result<u64, IndexError>> + Send;

    /// Read exactly `length` bytes. Short reads are corruption of this merge
    /// attempt rather than an authoritative-store failure.
    fn read_exact_at(
        &self,
        offset: u64,
        length: usize,
    ) -> impl Future<Output = Result<Vec<u8>, IndexError>> + Send;

    fn len(&self) -> impl Future<Output = Result<u64, IndexError>> + Send;

    fn is_empty(&self) -> impl Future<Output = Result<bool, IndexError>> + Send {
        async move { Ok(self.len().await? == 0) }
    }
}

/// Caller-provided disposable workspace. Every returned file belongs only to
/// the current merge attempt. Implementations must use collision-safe
/// create-new semantics and clean the workspace on drop or process restart.
pub trait MergeScratchSpace: Clone + Send + Sync + 'static {
    type File: MergeScratchFile;

    fn create_file(&self) -> impl Future<Output = Result<Self::File, IndexError>> + Send;
}
