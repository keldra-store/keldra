//! Lazy cache-backed directory for one immutable v2 logical run.

use std::sync::atomic::{AtomicBool, Ordering};

use anvil_index::compaction::CompactionProgress;
use anvil_index::{BlockDescriptor, IndexDirectoryRead, IndexError};

use super::cache::{IndexCache, IndexFile, IndexSegmentId, IndexSlice};
use super::generation::ManifestRun;

#[derive(Clone)]
pub(crate) struct ManifestIndexDirectory {
    cache: IndexCache,
    root: IndexSegmentId,
    progress: Option<CompactionProgress>,
}

impl ManifestIndexDirectory {
    pub(crate) fn open(cache: IndexCache, run: &ManifestRun) -> Result<Self, IndexError> {
        let root = IndexSegmentId::new(run.root_blob.hash, run.root_blob.length)
            .map_err(|error| IndexError::InvalidDefinition(error.to_string()))?;
        Ok(Self {
            cache,
            root,
            progress: None,
        })
    }

    pub(crate) fn open_observed(
        cache: IndexCache,
        run: &ManifestRun,
        progress: CompactionProgress,
    ) -> Result<Self, IndexError> {
        let mut directory = Self::open(cache, run)?;
        directory.progress = Some(progress);
        Ok(directory)
    }
}

impl IndexDirectoryRead for ManifestIndexDirectory {
    type File = ManifestIndexFile;

    async fn open_root(&self) -> Result<Self::File, IndexError> {
        Ok(ManifestIndexFile::new(
            self.cache.open(self.root),
            self.progress.clone(),
        ))
    }

    async fn open_block(&self, descriptor: &BlockDescriptor) -> Result<Self::File, IndexError> {
        let id = IndexSegmentId::new(descriptor.hash, descriptor.encoded_bytes)
            .map_err(|error| IndexError::InvalidDefinition(error.to_string()))?;
        Ok(ManifestIndexFile::new(
            self.cache.open(id),
            self.progress.clone(),
        ))
    }
}

pub(crate) struct ManifestIndexFile {
    inner: IndexFile,
    progress: Option<CompactionProgress>,
    observed: AtomicBool,
}

impl ManifestIndexFile {
    fn new(inner: IndexFile, progress: Option<CompactionProgress>) -> Self {
        Self {
            inner,
            progress,
            observed: AtomicBool::new(false),
        }
    }
}

impl anvil_index::IndexFileRead for ManifestIndexFile {
    type Slice = IndexSlice;

    async fn read_at(&self, offset: u64, max_length: usize) -> Result<Self::Slice, IndexError> {
        let slice = anvil_index::IndexFileRead::read_at(&self.inner, offset, max_length).await?;
        let bytes = slice.as_ref().len() as u64;
        if bytes != 0
            && let Some(progress) = &self.progress
        {
            let blocks = u64::from(!self.observed.swap(true, Ordering::Relaxed));
            progress.record_input(0, bytes, blocks);
        }
        Ok(slice)
    }
}
