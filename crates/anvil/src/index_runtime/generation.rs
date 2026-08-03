//! Immutable generation manifests over ordinary Anvil object identities.

use std::time::{SystemTime, UNIX_EPOCH};

use anvil_store::{BlobRef, PlacementLogId, SourceId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::engine::{BuiltIndexGeneration, IndexBuildDiagnostics};
use super::events::IndexBarrier;
use super::publication::{generation_manifest_path, generation_segment_path};

const GENERATION_MANIFEST_FORMAT: u16 = 1;
const CURRENT_POINTER_FORMAT: u16 = 1;
/// Fixed minimum-product logical cache segment size. This is unrelated to an
/// erasure-code shard or stripe and can change in a later file generation.
pub(crate) const DEFAULT_INDEX_SEGMENT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ManifestSourceCheckpoint {
    pub node_id: u64,
    pub source: SourceId,
    /// First source-local journal offset not represented by this generation.
    pub next_offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ManifestSegment {
    pub logical_offset: u64,
    pub blob: BlobRef,
    pub object_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ManifestFile {
    pub name: String,
    pub file_blake3: [u8; 32],
    pub logical_length: u64,
    pub segments: Vec<ManifestSegment>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct IndexGenerationManifest {
    format: u16,
    pub index_id: u64,
    pub generation: u64,
    pub definition_version: u64,
    pub placement_fence: PlacementLogId,
    pub atomic_finalized_through: Option<u64>,
    pub sources: Vec<ManifestSourceCheckpoint>,
    pub files: Vec<ManifestFile>,
    pub accepted_objects: u64,
    pub skipped_objects: u64,
    pub authoritative_bytes: u64,
}

impl IndexGenerationManifest {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, GenerationError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| GenerationError::Encode(error.to_string()))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, GenerationError> {
        let manifest: Self = serde_json::from_slice(bytes)
            .map_err(|error| GenerationError::Decode(error.to_string()))?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub(crate) fn validate(&self) -> Result<(), GenerationError> {
        if self.format != GENERATION_MANIFEST_FORMAT
            || self.index_id == 0
            || self.generation == 0
            || self.definition_version == 0
            || self.placement_fence.term == 0
            || self.placement_fence.index == 0
        {
            return Err(GenerationError::InvalidManifest(
                "generation identity or fence is invalid".into(),
            ));
        }
        let mut previous_node = None;
        for source in &self.sources {
            if source.node_id == 0
                || u64::from(source.source.node_id) != source.node_id
                || previous_node.is_some_and(|previous| previous >= source.node_id)
            {
                return Err(GenerationError::InvalidManifest(
                    "source checkpoint vector is not strictly node ordered".into(),
                ));
            }
            previous_node = Some(source.node_id);
        }
        let mut total = 0_u64;
        for file in &self.files {
            if file.name.is_empty() || file.logical_length == 0 || file.segments.is_empty() {
                return Err(GenerationError::InvalidManifest(
                    "generation file is empty".into(),
                ));
            }
            let mut expected = 0_u64;
            for segment in &file.segments {
                if segment.logical_offset != expected || segment.blob.length == 0 {
                    return Err(GenerationError::InvalidManifest(
                        "generation file segments are not contiguous".into(),
                    ));
                }
                expected = expected
                    .checked_add(segment.blob.length)
                    .ok_or(GenerationError::LengthOverflow)?;
                total = total
                    .checked_add(segment.blob.length)
                    .ok_or(GenerationError::LengthOverflow)?;
            }
            if expected != file.logical_length {
                return Err(GenerationError::InvalidManifest(
                    "generation file length differs from its segments".into(),
                ));
            }
        }
        if total != self.authoritative_bytes {
            return Err(GenerationError::InvalidManifest(
                "generation authoritative byte count is invalid".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct IndexCurrentPointer {
    format: u16,
    pub index_id: u64,
    pub generation: u64,
    pub definition_version: u64,
    pub manifest_path: String,
    pub manifest_blob: BlobRef,
    pub published_at_unix_millis: u64,
}

impl IndexCurrentPointer {
    pub(crate) fn new(
        manifest: &IndexGenerationManifest,
        manifest_blob: BlobRef,
        published_at: SystemTime,
    ) -> Result<Self, GenerationError> {
        let published_at_unix_millis = u64::try_from(
            published_at
                .duration_since(UNIX_EPOCH)
                .map_err(|_| GenerationError::ClockBeforeEpoch)?
                .as_millis(),
        )
        .map_err(|_| GenerationError::TimestampOverflow)?;
        let value = Self {
            format: CURRENT_POINTER_FORMAT,
            index_id: manifest.index_id,
            generation: manifest.generation,
            definition_version: manifest.definition_version,
            manifest_path: generation_manifest_path(manifest.index_id, manifest.generation),
            manifest_blob,
            published_at_unix_millis,
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, GenerationError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| GenerationError::Encode(error.to_string()))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, GenerationError> {
        let pointer: Self = serde_json::from_slice(bytes)
            .map_err(|error| GenerationError::Decode(error.to_string()))?;
        pointer.validate()?;
        Ok(pointer)
    }

    fn validate(&self) -> Result<(), GenerationError> {
        if self.format != CURRENT_POINTER_FORMAT
            || self.index_id == 0
            || self.generation == 0
            || self.definition_version == 0
            || self.manifest_blob.length == 0
            || self.published_at_unix_millis == 0
            || self.manifest_path != generation_manifest_path(self.index_id, self.generation)
        {
            return Err(GenerationError::InvalidPointer);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct PreparedIndexGeneration {
    pub manifest: IndexGenerationManifest,
    /// Segment bytes in the same file/segment order as `manifest`.
    pub segment_bytes: Vec<Vec<Vec<u8>>>,
}

impl PreparedIndexGeneration {
    pub(crate) fn prepare(
        index_id: u64,
        generation: u64,
        definition_version: u64,
        barrier: &IndexBarrier,
        built: BuiltIndexGeneration,
        segment_bytes: usize,
    ) -> Result<Self, GenerationError> {
        if segment_bytes == 0 {
            return Err(GenerationError::ZeroSegmentBytes);
        }
        let diagnostics = built.diagnostics;
        let mut files = Vec::new();
        let mut all_bytes = Vec::new();
        let mut authoritative_bytes = 0_u64;
        for generated in built.artifacts.into_files() {
            if generated.bytes.is_empty() {
                return Err(GenerationError::EmptyGeneratedFile(generated.name));
            }
            let file_blake3 = *blake3::hash(&generated.bytes).as_bytes();
            let logical_length = generated.bytes.len() as u64;
            let mut offset = 0_u64;
            let mut descriptors = Vec::new();
            let mut bytes = Vec::new();
            for (index, segment) in generated.bytes.chunks(segment_bytes).enumerate() {
                let blob = BlobRef {
                    hash: *blake3::hash(segment).as_bytes(),
                    length: segment.len() as u64,
                };
                let ordinal = u64::try_from(index)
                    .map_err(|_| GenerationError::LengthOverflow)?
                    .checked_add(1)
                    .ok_or(GenerationError::LengthOverflow)?;
                descriptors.push(ManifestSegment {
                    logical_offset: offset,
                    object_path: generation_segment_path(
                        index_id,
                        generation,
                        file_blake3,
                        ordinal,
                    ),
                    blob: blob.clone(),
                });
                offset = offset
                    .checked_add(blob.length)
                    .ok_or(GenerationError::LengthOverflow)?;
                authoritative_bytes = authoritative_bytes
                    .checked_add(blob.length)
                    .ok_or(GenerationError::LengthOverflow)?;
                bytes.push(segment.to_vec());
            }
            files.push(ManifestFile {
                name: generated.name,
                file_blake3,
                logical_length,
                segments: descriptors,
            });
            all_bytes.push(bytes);
        }
        files.sort_by(|left, right| left.name.cmp(&right.name));
        // `all_bytes` was produced in engine artifact order, which is already
        // BTreeMap order. Assert the invariant before publication relies on it.
        debug_assert!(files.windows(2).all(|pair| pair[0].name < pair[1].name));
        let sources = barrier
            .sources
            .iter()
            .map(|(node, cursor)| ManifestSourceCheckpoint {
                node_id: node.0,
                source: cursor.source,
                next_offset: cursor.next_offset,
            })
            .collect();
        let manifest = IndexGenerationManifest {
            format: GENERATION_MANIFEST_FORMAT,
            index_id,
            generation,
            definition_version,
            placement_fence: barrier.fence,
            atomic_finalized_through: barrier.atomic.finalized_through(),
            sources,
            files,
            accepted_objects: diagnostics.accepted_objects,
            skipped_objects: diagnostics.skipped_objects,
            authoritative_bytes,
        };
        manifest.validate()?;
        Ok(Self {
            manifest,
            segment_bytes: all_bytes,
        })
    }

    pub(crate) fn diagnostics(&self) -> IndexBuildDiagnostics {
        IndexBuildDiagnostics {
            accepted_objects: self.manifest.accepted_objects,
            skipped_objects: self.manifest.skipped_objects,
        }
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum GenerationError {
    #[error("index generation manifest is invalid: {0}")]
    InvalidManifest(String),
    #[error("index current pointer is invalid")]
    InvalidPointer,
    #[error("index generation contains an empty file `{0}`")]
    EmptyGeneratedFile(String),
    #[error("index logical segment byte target must be positive")]
    ZeroSegmentBytes,
    #[error("index generation length overflow")]
    LengthOverflow,
    #[error("system clock predates the Unix epoch")]
    ClockBeforeEpoch,
    #[error("index publication timestamp overflow")]
    TimestampOverflow,
    #[error("encode index generation: {0}")]
    Encode(String),
    #[error("decode index generation: {0}")]
    Decode(String),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anvil_consensus::NodeId;
    use anvil_index::IndexArtifacts;

    use super::*;
    use crate::index_runtime::events::{AtomicProgramWatermark, IndexSourceCursor};

    #[test]
    fn files_are_split_into_independently_addressable_logical_segments() {
        let mut artifacts = IndexArtifacts::default();
        artifacts
            .insert("path/entries.map", vec![1, 2, 3, 4, 5])
            .unwrap();
        let barrier = IndexBarrier {
            fence: PlacementLogId { term: 2, index: 7 },
            atomic: AtomicProgramWatermark::new(Some(9), Some(9), 0),
            sources: BTreeMap::from([(
                NodeId(1),
                IndexSourceCursor {
                    source: SourceId {
                        node_id: 1,
                        source_epoch: [3; 32],
                    },
                    next_offset: 12,
                },
            )]),
        };
        let prepared = PreparedIndexGeneration::prepare(
            4,
            5,
            6,
            &barrier,
            BuiltIndexGeneration {
                artifacts,
                diagnostics: IndexBuildDiagnostics {
                    accepted_objects: 1,
                    skipped_objects: 0,
                },
            },
            2,
        )
        .unwrap();
        assert_eq!(prepared.manifest.files[0].segments.len(), 3);
        assert_eq!(prepared.manifest.authoritative_bytes, 5);
        assert_eq!(prepared.segment_bytes[0], [vec![1, 2], vec![3, 4], vec![5]]);
        assert_eq!(
            IndexGenerationManifest::decode(&prepared.manifest.encode().unwrap()).unwrap(),
            prepared.manifest
        );
    }
}
