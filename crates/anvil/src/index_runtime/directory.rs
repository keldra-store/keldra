//! Lazy cache-backed directory for one immutable v3 logical run.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anvil_index::compaction::CompactionProgress;
use anvil_index::{BlockDescriptor, IndexDirectoryRead, IndexError};

use super::cache::{IndexCache, IndexFile, IndexSegmentId, IndexSlice};
use super::generation::ManifestRun;

#[derive(Clone)]
pub(crate) struct ManifestIndexDirectory {
    cache: IndexCache,
    root: IndexSegmentId,
    packs: Arc<BTreeMap<u32, IndexSegmentId>>,
    progress: Option<CompactionProgress>,
}

impl ManifestIndexDirectory {
    pub(crate) fn open(cache: IndexCache, run: &ManifestRun) -> Result<Self, IndexError> {
        let root = IndexSegmentId::new(run.root_blob.hash, run.root_blob.length)
            .map_err(|error| IndexError::InvalidDefinition(error.to_string()))?;
        let packs = run
            .packs
            .iter()
            .map(|pack| {
                IndexSegmentId::new(pack.blob.hash, pack.blob.length)
                    .map(|segment| (pack.id, segment))
                    .map_err(|error| IndexError::InvalidDefinition(error.to_string()))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(Self {
            cache,
            root,
            packs: Arc::new(packs),
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
            0,
            self.root.length,
            self.progress.clone(),
        ))
    }

    async fn open_block(&self, descriptor: &BlockDescriptor) -> Result<Self::File, IndexError> {
        let id = *self
            .packs
            .get(&descriptor.pack_id)
            .ok_or_else(|| IndexError::FileNotFound(descriptor.logical_name()))?;
        let end = descriptor
            .pack_offset
            .checked_add(descriptor.encoded_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        if end > id.length {
            return Err(IndexError::InvalidFormat("index block pack bounds"));
        }
        Ok(ManifestIndexFile::new(
            self.cache.open(id),
            descriptor.pack_offset,
            descriptor.encoded_bytes,
            self.progress.clone(),
        ))
    }
}

pub(crate) struct ManifestIndexFile {
    inner: IndexFile,
    start: u64,
    length: u64,
    progress: Option<CompactionProgress>,
    observed: AtomicBool,
}

impl ManifestIndexFile {
    fn new(
        inner: IndexFile,
        start: u64,
        length: u64,
        progress: Option<CompactionProgress>,
    ) -> Self {
        Self {
            inner,
            start,
            length,
            progress,
            observed: AtomicBool::new(false),
        }
    }
}

impl anvil_index::IndexFileRead for ManifestIndexFile {
    type Slice = IndexSlice;

    async fn read_at(&self, offset: u64, max_length: usize) -> Result<Self::Slice, IndexError> {
        if offset >= self.length || max_length == 0 {
            return anvil_index::IndexFileRead::read_at(&self.inner, 0, 0).await;
        }
        let remaining = usize::try_from(self.length - offset).unwrap_or(usize::MAX);
        let physical = self
            .start
            .checked_add(offset)
            .ok_or(IndexError::OffsetOverflow)?;
        let slice =
            anvil_index::IndexFileRead::read_at(&self.inner, physical, max_length.min(remaining))
                .await?;
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
