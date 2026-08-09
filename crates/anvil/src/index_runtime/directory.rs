//! Lazy cache-backed directory for one immutable v2 logical run.

use anvil_index::{BlockDescriptor, IndexDirectoryRead, IndexError};

use super::cache::{IndexCache, IndexFile, IndexSegmentId};
use super::generation::ManifestRun;

#[derive(Clone)]
pub(crate) struct ManifestIndexDirectory {
    cache: IndexCache,
    root: IndexSegmentId,
}

impl ManifestIndexDirectory {
    pub(crate) fn open(cache: IndexCache, run: &ManifestRun) -> Result<Self, IndexError> {
        let root = IndexSegmentId::new(run.root_blob.hash, run.root_blob.length)
            .map_err(|error| IndexError::InvalidDefinition(error.to_string()))?;
        Ok(Self { cache, root })
    }
}

impl IndexDirectoryRead for ManifestIndexDirectory {
    type File = IndexFile;

    async fn open_root(&self) -> Result<Self::File, IndexError> {
        Ok(self.cache.open(self.root))
    }

    async fn open_block(&self, descriptor: &BlockDescriptor) -> Result<Self::File, IndexError> {
        let id = IndexSegmentId::new(descriptor.hash, descriptor.encoded_bytes)
            .map_err(|error| IndexError::InvalidDefinition(error.to_string()))?;
        Ok(self.cache.open(id))
    }
}
