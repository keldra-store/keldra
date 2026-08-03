//! Engine directory view over one validated immutable generation manifest.

use std::collections::BTreeMap;

use anvil_index::{IndexDirectoryRead, IndexError};

use super::cache::{IndexCache, IndexFile, IndexFileLayout, IndexSegment, IndexSegmentId};
use super::generation::IndexGenerationManifest;

#[derive(Clone)]
pub(crate) struct ManifestIndexDirectory {
    files: BTreeMap<String, IndexFile>,
}

impl ManifestIndexDirectory {
    pub(crate) fn open(
        cache: IndexCache,
        manifest: &IndexGenerationManifest,
    ) -> Result<Self, IndexError> {
        manifest
            .validate()
            .map_err(|error| IndexError::InvalidDefinition(error.to_string()))?;
        let mut files = BTreeMap::new();
        for file in &manifest.files {
            let segments = file
                .segments
                .iter()
                .map(|segment| {
                    let id = IndexSegmentId::new(segment.blob.hash, segment.blob.length)
                        .map_err(|error| IndexError::InvalidDefinition(error.to_string()))?;
                    Ok(IndexSegment {
                        logical_offset: segment.logical_offset,
                        id,
                    })
                })
                .collect::<Result<Vec<_>, IndexError>>()?;
            let layout = IndexFileLayout::new(segments)
                .map_err(|error| IndexError::InvalidDefinition(error.to_string()))?;
            let opened = cache.open(layout);
            if files.insert(file.name.clone(), opened).is_some() {
                return Err(IndexError::InvalidDefinition(
                    "generation repeats an index file name".into(),
                ));
            }
        }
        Ok(Self { files })
    }
}

impl IndexDirectoryRead for ManifestIndexDirectory {
    type File = IndexFile;

    async fn open_file(&self, name: &str) -> Result<Self::File, IndexError> {
        self.files
            .get(name)
            .cloned()
            .ok_or_else(|| IndexError::FileNotFound(name.to_owned()))
    }
}
