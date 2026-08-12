//! Version-3 immutable logical-run and generation manifests.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anvil_index::{IndexKind, RunDescriptor};
use anvil_store::{BlobRef, PlacementLogId, SourceId, VersionId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::events::IndexBarrier;
use super::publication::{manifest_path, run_root_path};

pub(crate) const INDEX_MANIFEST_FORMAT: u16 = 3;
pub(crate) const INDEX_CURRENT_FORMAT: u16 = 3;
/// Corruption guard for one published level. Runtime admission uses a lower,
/// startup-configured debt bound and compacts before crossing it.
pub(crate) const MAX_RUNS_PER_LEVEL: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestSourceCheckpoint {
    pub node_id: u64,
    pub source: SourceId,
    /// First source-local journal offset not represented by this generation.
    pub next_offset: u64,
}

/// One logical run. Its root recursively names logical blocks by deterministic
/// pack ID; this manifest resolves those IDs to ordinary immutable objects.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestPack {
    /// Deterministic writer-lane/local-pack identity carried by logical block
    /// descriptors. It is not an object version or placement decision.
    pub id: u32,
    pub blob: BlobRef,
    pub object_version: VersionId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestRun {
    /// Monotonic source-state order. Larger values are newer.
    pub sequence: u64,
    /// Durable creation time of this immutable run, used only for retention
    /// and exact compaction-debt age observability.
    pub created_at_unix_millis: u64,
    pub level: u8,
    pub root_path: String,
    pub root_blob: BlobRef,
    pub root_object_version: VersionId,
    /// Physical ordinary-object packs addressed by every logical block
    /// descriptor below the standalone run root.
    pub packs: Vec<ManifestPack>,
    pub mutation_count: u64,
    pub live_document_count: u64,
    pub minimum_version: u64,
    pub maximum_version: u64,
    /// Root plus every recursively referenced block in this run.
    pub authoritative_bytes: u64,
}

impl ManifestRun {
    pub(crate) fn from_descriptor(
        index_id: u64,
        sequence: u64,
        descriptor: &RunDescriptor,
        root_blob: BlobRef,
        root_object_version: VersionId,
        packs: Vec<ManifestPack>,
    ) -> Result<Self, GenerationError> {
        let value = Self {
            sequence,
            created_at_unix_millis: unix_millis(SystemTime::now())?,
            level: descriptor.level,
            root_path: run_root_path(index_id, descriptor.hash),
            root_blob,
            root_object_version,
            packs,
            mutation_count: descriptor.mutation_count,
            live_document_count: descriptor.live_document_count,
            minimum_version: descriptor.minimum_version,
            maximum_version: descriptor.maximum_version,
            authoritative_bytes: descriptor.encoded_bytes,
        };
        value.validate(index_id)?;
        Ok(value)
    }

    fn validate(&self, index_id: u64) -> Result<(), GenerationError> {
        let mut previous_pack_id = None;
        let packed_bytes = self.packs.iter().try_fold(0_u64, |total, pack| {
            if pack.id == u32::MAX
                || previous_pack_id.is_some_and(|previous| previous >= pack.id)
                || pack.blob.length == 0
                || pack.object_version.0 == 0
            {
                return Err(GenerationError::InvalidRun(
                    "logical run contains invalid artifact pack metadata".into(),
                ));
            }
            previous_pack_id = Some(pack.id);
            total
                .checked_add(pack.blob.length)
                .ok_or(GenerationError::LengthOverflow)
        })?;
        if self.sequence == 0
            || self.created_at_unix_millis == 0
            || self.root_blob.length == 0
            || self.root_object_version.0 == 0
            || self.mutation_count == 0
            || self.live_document_count > self.mutation_count
            || self.maximum_version < self.minimum_version
            || packed_bytes
                .checked_add(self.root_blob.length)
                .ok_or(GenerationError::LengthOverflow)?
                != self.authoritative_bytes
            || self.root_path != run_root_path(index_id, self.root_blob.hash)
        {
            return Err(GenerationError::InvalidRun(
                "logical run identity, statistics, or root reference is invalid".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestReference {
    pub generation: u64,
    pub definition_version: u64,
    pub path: String,
    pub blob: BlobRef,
    pub object_version: VersionId,
    pub published_at_unix_millis: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexGenerationManifest {
    format: u16,
    pub index_id: u64,
    pub generation: u64,
    pub definition_version: u64,
    pub kind: IndexKind,
    pub placement_fence: PlacementLogId,
    pub atomic_finalized_through: Option<u64>,
    pub sources: Vec<ManifestSourceCheckpoint>,
    /// Strictly increasing source-state order. Queries visit this in reverse.
    pub runs: Vec<ManifestRun>,
    pub previous: Option<ManifestReference>,
    pub accepted_objects: u64,
    pub skipped_objects: u64,
    pub authoritative_bytes: u64,
}

impl IndexGenerationManifest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        index_id: u64,
        generation: u64,
        definition_version: u64,
        kind: IndexKind,
        barrier: &IndexBarrier,
        runs: Vec<ManifestRun>,
        previous: Option<ManifestReference>,
        accepted_objects: u64,
        skipped_objects: u64,
    ) -> Result<Self, GenerationError> {
        let authoritative_bytes = runs.iter().try_fold(0_u64, |total, run| {
            total
                .checked_add(run.authoritative_bytes)
                .ok_or(GenerationError::LengthOverflow)
        })?;
        let value = Self {
            format: INDEX_MANIFEST_FORMAT,
            index_id,
            generation,
            definition_version,
            kind,
            placement_fence: barrier.fence,
            atomic_finalized_through: barrier.atomic.finalized_through(),
            sources: barrier
                .sources
                .iter()
                .map(|(node, cursor)| ManifestSourceCheckpoint {
                    node_id: node.0,
                    source: cursor.source,
                    next_offset: cursor.next_offset,
                })
                .collect(),
            runs,
            previous,
            accepted_objects,
            skipped_objects,
            authoritative_bytes,
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, GenerationError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|error| GenerationError::Encode(error.to_string()))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, GenerationError> {
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|error| GenerationError::Decode(error.to_string()))?;
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn validate(&self) -> Result<(), GenerationError> {
        if self.format != INDEX_MANIFEST_FORMAT
            || self.index_id == 0
            || self.generation == 0
            || self.definition_version == 0
            || self.placement_fence.term == 0
            || self.placement_fence.index == 0
        {
            return Err(GenerationError::InvalidManifest(
                "manifest identity or placement fence is invalid".into(),
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
        let mut levels = BTreeMap::<u8, usize>::new();
        let mut previous_sequence = None;
        let mut total = 0_u64;
        for run in &self.runs {
            run.validate(self.index_id)?;
            if previous_sequence.is_some_and(|previous| previous >= run.sequence) {
                return Err(GenerationError::InvalidManifest(
                    "logical runs are not strictly source ordered".into(),
                ));
            }
            let count = levels.entry(run.level).or_default();
            *count += 1;
            if *count > MAX_RUNS_PER_LEVEL {
                return Err(GenerationError::InvalidManifest(
                    "one logical run level exceeds the format debt bound".into(),
                ));
            }
            total = total
                .checked_add(run.authoritative_bytes)
                .ok_or(GenerationError::LengthOverflow)?;
            previous_sequence = Some(run.sequence);
        }
        if total != self.authoritative_bytes {
            return Err(GenerationError::InvalidManifest(
                "manifest authoritative byte count is invalid".into(),
            ));
        }
        if let Some(previous) = &self.previous {
            if previous.generation == 0
                || previous.generation >= self.generation
                || previous.definition_version == 0
                || previous.blob.length == 0
                || previous.object_version.0 == 0
                || previous.published_at_unix_millis == 0
                || previous.path != manifest_path(self.index_id, previous.blob.hash)
            {
                return Err(GenerationError::InvalidManifest(
                    "manifest predecessor is invalid".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn barrier(&self) -> Result<IndexBarrier, GenerationError> {
        let sources = self
            .sources
            .iter()
            .map(|source| {
                (
                    anvil_consensus::NodeId(source.node_id),
                    super::events::IndexSourceCursor {
                        source: source.source,
                        next_offset: source.next_offset,
                    },
                )
            })
            .collect();
        Ok(IndexBarrier {
            fence: self.placement_fence,
            atomic: super::events::AtomicProgramWatermark::new(
                self.atomic_finalized_through,
                self.atomic_finalized_through,
                0,
            ),
            sources,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexCurrentPointer {
    format: u16,
    pub index_id: u64,
    pub generation: u64,
    pub definition_version: u64,
    pub manifest_path: String,
    pub manifest_blob: BlobRef,
    pub manifest_object_version: VersionId,
    pub published_at_unix_millis: u64,
}

impl IndexCurrentPointer {
    pub(crate) fn new(
        manifest: &IndexGenerationManifest,
        manifest_blob: BlobRef,
        manifest_object_version: VersionId,
        published_at: SystemTime,
    ) -> Result<Self, GenerationError> {
        let published_at_unix_millis = unix_millis(published_at)?;
        let value = Self {
            format: INDEX_CURRENT_FORMAT,
            index_id: manifest.index_id,
            generation: manifest.generation,
            definition_version: manifest.definition_version,
            manifest_path: manifest_path(manifest.index_id, manifest_blob.hash),
            manifest_blob,
            manifest_object_version,
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
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|error| GenerationError::Decode(error.to_string()))?;
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn as_manifest_reference(&self) -> ManifestReference {
        ManifestReference {
            generation: self.generation,
            definition_version: self.definition_version,
            path: self.manifest_path.clone(),
            blob: self.manifest_blob.clone(),
            object_version: self.manifest_object_version,
            published_at_unix_millis: self.published_at_unix_millis,
        }
    }

    fn validate(&self) -> Result<(), GenerationError> {
        if self.format != INDEX_CURRENT_FORMAT
            || self.index_id == 0
            || self.generation == 0
            || self.definition_version == 0
            || self.manifest_blob.length == 0
            || self.manifest_object_version.0 == 0
            || self.published_at_unix_millis == 0
            || self.manifest_path != manifest_path(self.index_id, self.manifest_blob.hash)
        {
            return Err(GenerationError::InvalidPointer);
        }
        Ok(())
    }
}

fn unix_millis(time: SystemTime) -> Result<u64, GenerationError> {
    u64::try_from(
        time.duration_since(UNIX_EPOCH)
            .map_err(|_| GenerationError::ClockBeforeEpoch)?
            .as_millis(),
    )
    .map_err(|_| GenerationError::TimestampOverflow)
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum GenerationError {
    #[error("index logical run is invalid: {0}")]
    InvalidRun(String),
    #[error("index generation manifest is invalid: {0}")]
    InvalidManifest(String),
    #[error("index current pointer is invalid or uses an unsupported format")]
    InvalidPointer,
    #[error("index generation length overflow")]
    LengthOverflow,
    #[error("system clock predates the Unix epoch")]
    ClockBeforeEpoch,
    #[error("index timestamp overflow")]
    TimestampOverflow,
    #[error("encode index v3 object: {0}")]
    Encode(String),
    #[error("decode index v3 object: {0}")]
    Decode(String),
}

#[cfg(test)]
mod tests {
    use anvil_consensus::NodeId;

    use super::*;
    use crate::index_runtime::events::{AtomicProgramWatermark, IndexSourceCursor};

    fn barrier() -> IndexBarrier {
        IndexBarrier {
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
        }
    }

    fn run(sequence: u64, level: u8) -> ManifestRun {
        let digest = [sequence as u8; 32];
        ManifestRun {
            sequence,
            created_at_unix_millis: 1_000,
            level,
            root_path: run_root_path(4, digest),
            root_blob: BlobRef {
                hash: digest,
                length: 10,
            },
            root_object_version: VersionId(sequence + 5),
            packs: vec![ManifestPack {
                id: 0,
                blob: BlobRef {
                    hash: [(sequence as u8).wrapping_add(1); 32],
                    length: 10,
                },
                object_version: VersionId(sequence + 6),
            }],
            mutation_count: 2,
            live_document_count: 2,
            minimum_version: 1,
            maximum_version: 2,
            authoritative_bytes: 20,
        }
    }

    #[test]
    fn v3_pointer_and_manifest_round_trip() {
        let manifest = IndexGenerationManifest::new(
            4,
            1,
            8,
            IndexKind::Path,
            &barrier(),
            vec![run(1, 0)],
            None,
            2,
            0,
        )
        .unwrap();
        let encoded = manifest.encode().unwrap();
        assert_eq!(IndexGenerationManifest::decode(&encoded).unwrap(), manifest);
        let blob = BlobRef {
            hash: *blake3::hash(&encoded).as_bytes(),
            length: encoded.len() as u64,
        };
        let pointer = IndexCurrentPointer::new(
            &manifest,
            blob,
            VersionId(9),
            UNIX_EPOCH + std::time::Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(
            IndexCurrentPointer::decode(&pointer.encode().unwrap()).unwrap(),
            pointer
        );
    }

    #[test]
    fn fan_in_is_bounded_per_level_not_for_the_whole_index() {
        let runs = (1..=8)
            .map(|sequence| run(sequence, u8::from(sequence > 4)))
            .collect();
        assert!(
            IndexGenerationManifest::new(4, 1, 8, IndexKind::Path, &barrier(), runs, None, 8, 0,)
                .is_ok()
        );
        let too_many = (1..=MAX_RUNS_PER_LEVEL as u64 + 1)
            .map(|sequence| run(sequence, 0))
            .collect();
        assert!(
            IndexGenerationManifest::new(
                4,
                1,
                8,
                IndexKind::Path,
                &barrier(),
                too_many,
                None,
                MAX_RUNS_PER_LEVEL as u64 + 1,
                0,
            )
            .is_err()
        );
    }

    #[test]
    fn run_creation_time_is_required() {
        let mut invalid = run(1, 0);
        invalid.created_at_unix_millis = 0;
        assert!(
            IndexGenerationManifest::new(
                4,
                1,
                8,
                IndexKind::Path,
                &barrier(),
                vec![invalid],
                None,
                1,
                0,
            )
            .is_err()
        );
    }

    #[test]
    fn v1_values_are_rejected_not_interpreted() {
        let old = br#"{"format":1,"index_id":1,"generation":1}"#;
        assert!(IndexGenerationManifest::decode(old).is_err());
        assert!(IndexCurrentPointer::decode(old).is_err());
    }
}
