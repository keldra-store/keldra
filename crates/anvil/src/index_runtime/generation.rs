//! Portable format-v4 index generation and current-pointer records.
//!
//! These records contain only ordinary-object references. Component bytes,
//! manifests, and the mutable current pointer remain ordinary Anvil objects;
//! no index state is stored in Raft or a side plane.

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use anvil_index::v4::{
    ArtifactDescriptor, ArtifactPackReference, COMPONENT_HEADER_BYTES, ComponentKind, FieldId,
    INDEX_COMPONENT_BYTES, INDEX_DECODE_BYTES, INDEX_FORMAT_VERSION, INDEX_GENERATION_SEGMENTS,
    INDEX_ROUTING_KEY_BYTES, IndexKind, LocatorStreamRoot, SegmentDescriptor,
};
use anvil_store::{BlobRef, PlacementLogId, SourceId, VersionId};
use thiserror::Error;

use super::events::IndexBarrier;
use super::publication::manifest_path;

pub(crate) const INDEX_MANIFEST_FORMAT: u16 = INDEX_FORMAT_VERSION;
pub(crate) const INDEX_CURRENT_FORMAT: u16 = INDEX_FORMAT_VERSION;
pub(crate) const MAX_RETAINED_GENERATIONS: usize = 64;

const COMPONENT_MAGIC: &[u8; 8] = b"ANVLIDX4";
const CURRENT_MAGIC: &[u8; 8] = b"ANVLCUR4";
const MANIFEST_CODEC_VERSION: u16 = 1;
pub(crate) const MAX_SEGMENTS_PER_GENERATION: usize = INDEX_GENERATION_SEGMENTS;
const MAX_SEGMENT_COMPONENTS: usize = 4_096;
pub(crate) const MAX_LOCATOR_ROOTS_PER_GENERATION: usize = 4_096;
// Cluster node IDs are capped at 1,023; one spare slot keeps decode bounds
// explicit without imposing a smaller index-specific topology limit.
const MAX_SOURCE_CHECKPOINTS: usize = 1_024;
const MAX_PHYSICAL_ORDER_FIELDS: usize = INDEX_COMPONENT_BYTES / 5;
const MIN_ENCODED_SOURCE_CHECKPOINT_BYTES: usize = 8 + 2 + 32 + 8;
const MIN_ENCODED_PHYSICAL_ORDER_BYTES: usize = 4 + 1;
const MIN_ENCODED_SEGMENT_BYTES: usize = 8 + 8 + 32 + 8 + 4 + 4 + 4 + 4 + 8 + 8;
// Four path-length bytes plus the fixed fields after the path. A canonical
// path is non-empty, so this deliberately underestimates every real record.
const MIN_ENCODED_PACK_BYTES: usize = 4 + 8 + 32 + 8;
const MIN_ENCODED_ARTIFACT_BYTES: usize = 4 + 8 + 8 + 8 + 2 + 2 + 32;
const MIN_ENCODED_SEGMENT_COMPONENT_BYTES: usize = 2 + 1 + 4 + MIN_ENCODED_ARTIFACT_BYTES;
const MIN_ENCODED_LOCATOR_ROOT_BYTES: usize =
    8 + 8 + 8 + 32 + 8 + MIN_ENCODED_ARTIFACT_BYTES + 1 + 8 + 8;
const MAX_PACKS_PER_OWNER: usize = INDEX_COMPONENT_BYTES / MIN_ENCODED_PACK_BYTES;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestSourceCheckpoint {
    pub node_id: u64,
    pub source: SourceId,
    /// First source-local journal offset not represented by this generation.
    pub next_offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocatorRoot {
    /// Strictly increasing source-state order; larger values are newer.
    pub sequence: u64,
    /// Exact envelope identity needed to validate a detached locator tree.
    pub identity: anvil_index::v4::SegmentIdentity,
    pub artifact: ArtifactDescriptor,
    pub pack_ownership: LocatorPackOwnership,
    /// Complete recursively referenced component bytes, including envelopes.
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LocatorPackOwnership {
    /// The exact matching segment descriptor owns the locator's pack table.
    Segment,
    /// A locator which outlives or was built without a segment owns its table.
    Standalone(Vec<ArtifactPackReference>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ManifestPhysicalOrder {
    pub field_id: FieldId,
    pub descending: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ManifestReference {
    pub generation: u64,
    pub definition_version: u64,
    pub schema_fingerprint: [u8; 32],
    pub path: String,
    pub blob: BlobRef,
    pub object_version: VersionId,
    pub published_at_unix_millis: u64,
}

impl ManifestReference {
    pub(crate) fn new(
        manifest: &IndexGenerationManifest,
        blob: BlobRef,
        object_version: VersionId,
        published_at: SystemTime,
    ) -> Result<Self, GenerationError> {
        let encoded = manifest.encode()?;
        if blob.length != encoded.len() as u64 || blob.hash != *blake3::hash(&encoded).as_bytes() {
            return Err(GenerationError::InvalidManifestReference);
        }
        let value = Self {
            generation: manifest.generation,
            definition_version: manifest.definition_version,
            schema_fingerprint: manifest.schema_fingerprint,
            path: manifest_path(manifest.index_id, blob.hash),
            blob,
            object_version,
            published_at_unix_millis: unix_millis(published_at)?,
        };
        value.validate(manifest.index_id)?;
        Ok(value)
    }

    pub(crate) fn validate(&self, index_id: u64) -> Result<(), GenerationError> {
        if index_id == 0
            || self.generation == 0
            || self.definition_version == 0
            || self.blob.length < COMPONENT_HEADER_BYTES as u64
            || self.blob.length > INDEX_COMPONENT_BYTES as u64
            || self.object_version.0 == 0
            || self.published_at_unix_millis == 0
            || self.path.len() > INDEX_ROUTING_KEY_BYTES
            || self.path != manifest_path(index_id, self.blob.hash)
        {
            return Err(GenerationError::InvalidManifestReference);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexGenerationManifest {
    format: u16,
    pub index_id: u64,
    pub generation: u64,
    pub definition_version: u64,
    pub kind: IndexKind,
    pub schema_fingerprint: [u8; 32],
    pub placement_fence: PlacementLogId,
    pub atomic_finalized_through: Option<u64>,
    pub sources: Vec<ManifestSourceCheckpoint>,
    pub physical_order: Vec<ManifestPhysicalOrder>,
    pub segments: Vec<SegmentDescriptor>,
    pub locator_roots: Vec<LocatorRoot>,
    pub artifact_encoded_bytes: u64,
    pub artifact_logical_bytes: u64,
}

impl IndexGenerationManifest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        index_id: u64,
        generation: u64,
        definition_version: u64,
        kind: IndexKind,
        schema_fingerprint: [u8; 32],
        barrier: &IndexBarrier,
        physical_order: Vec<ManifestPhysicalOrder>,
        segments: Vec<SegmentDescriptor>,
        locator_roots: Vec<LocatorRoot>,
        artifact_encoded_bytes: u64,
        artifact_logical_bytes: u64,
    ) -> Result<Self, GenerationError> {
        let value = Self {
            format: INDEX_MANIFEST_FORMAT,
            index_id,
            generation,
            definition_version,
            kind,
            schema_fingerprint,
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
            physical_order,
            segments,
            locator_roots,
            artifact_encoded_bytes,
            artifact_logical_bytes,
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, GenerationError> {
        self.validate()?;
        let mut payload = Encoder::default();
        payload.u16(self.format);
        payload.u64(self.generation);
        payload.u8(self.kind as u8);
        payload.u64(self.placement_fence.term);
        payload.u64(self.placement_fence.index);
        payload.option_u64(self.atomic_finalized_through);
        payload.count(self.sources.len())?;
        for source in &self.sources {
            payload.u64(source.node_id);
            payload.u16(source.source.node_id);
            payload.fixed(&source.source.source_epoch);
            payload.u64(source.next_offset);
        }
        payload.count(self.physical_order.len())?;
        for order in &self.physical_order {
            payload.u32(order.field_id.get());
            payload.u8(u8::from(order.descending));
        }
        payload.count(self.segments.len())?;
        for segment in &self.segments {
            encode_segment(&mut payload, segment)?;
        }
        payload.count(self.locator_roots.len())?;
        for locator in &self.locator_roots {
            payload.u64(locator.sequence);
            payload.u64(locator.identity.index_id);
            payload.u64(locator.identity.definition_version);
            payload.fixed(&locator.identity.schema_fingerprint);
            payload.u64(locator.identity.segment_id);
            encode_artifact(&mut payload, &locator.artifact)?;
            match &locator.pack_ownership {
                LocatorPackOwnership::Segment => payload.u8(0),
                LocatorPackOwnership::Standalone(packs) => {
                    payload.u8(1);
                    encode_packs(&mut payload, packs)?;
                }
            }
            payload.u64(locator.encoded_bytes);
            payload.u64(locator.logical_bytes);
        }
        payload.u64(self.artifact_encoded_bytes);
        payload.u64(self.artifact_logical_bytes);
        encode_manifest_envelope(self, payload.finish())
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, GenerationError> {
        let envelope = decode_manifest_envelope(bytes)?;
        let mut payload = Decoder::new(envelope.payload);
        let format = payload.u16()?;
        let generation = payload.u64()?;
        let kind = index_kind(payload.u8()?)?;
        let placement_fence = PlacementLogId {
            term: payload.u64()?,
            index: payload.u64()?,
        };
        let atomic_finalized_through = payload.option_u64()?;
        let source_count = payload
            .collection_count(MAX_SOURCE_CHECKPOINTS, MIN_ENCODED_SOURCE_CHECKPOINT_BYTES)?;
        let mut sources = Vec::with_capacity(source_count);
        for _ in 0..source_count {
            sources.push(ManifestSourceCheckpoint {
                node_id: payload.u64()?,
                source: SourceId {
                    node_id: payload.u16()?,
                    source_epoch: payload.array_32()?,
                },
                next_offset: payload.u64()?,
            });
        }
        let order_count = payload
            .collection_count(MAX_PHYSICAL_ORDER_FIELDS, MIN_ENCODED_PHYSICAL_ORDER_BYTES)?;
        let mut physical_order = Vec::with_capacity(order_count);
        for _ in 0..order_count {
            physical_order.push(ManifestPhysicalOrder {
                field_id: FieldId::new(payload.u32()?),
                descending: payload.boolean()?,
            });
        }
        let segment_count =
            payload.collection_count(MAX_SEGMENTS_PER_GENERATION, MIN_ENCODED_SEGMENT_BYTES)?;
        let mut segments = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            segments.push(decode_segment(&mut payload)?);
        }
        let locator_count = payload.collection_count(
            MAX_LOCATOR_ROOTS_PER_GENERATION,
            MIN_ENCODED_LOCATOR_ROOT_BYTES,
        )?;
        let mut locator_roots = Vec::with_capacity(locator_count);
        for _ in 0..locator_count {
            let sequence = payload.u64()?;
            let identity = anvil_index::v4::SegmentIdentity {
                index_id: payload.u64()?,
                definition_version: payload.u64()?,
                schema_fingerprint: payload.array_32()?,
                segment_id: payload.u64()?,
            };
            let artifact = decode_artifact(&mut payload)?;
            let pack_ownership = match payload.u8()? {
                0 => LocatorPackOwnership::Segment,
                1 => LocatorPackOwnership::Standalone(decode_packs(&mut payload)?),
                _ => {
                    return Err(GenerationError::Decode(
                        "invalid locator pack ownership".into(),
                    ));
                }
            };
            locator_roots.push(LocatorRoot {
                sequence,
                identity,
                artifact,
                pack_ownership,
                encoded_bytes: payload.u64()?,
                logical_bytes: payload.u64()?,
            });
        }
        let artifact_encoded_bytes = payload.u64()?;
        let artifact_logical_bytes = payload.u64()?;
        payload.finish()?;
        let value = Self {
            format,
            index_id: envelope.index_id,
            generation,
            definition_version: envelope.definition_version,
            kind,
            schema_fingerprint: envelope.schema_fingerprint,
            placement_fence,
            atomic_finalized_through,
            sources,
            physical_order,
            segments,
            locator_roots,
            artifact_encoded_bytes,
            artifact_logical_bytes,
        };
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
            || self.atomic_finalized_through == Some(0)
            || self.sources.len() > MAX_SOURCE_CHECKPOINTS
            || self.physical_order.len() > MAX_PHYSICAL_ORDER_FIELDS
            || self.segments.len() > MAX_SEGMENTS_PER_GENERATION
            || self.locator_roots.len() > MAX_LOCATOR_ROOTS_PER_GENERATION
        {
            return Err(GenerationError::InvalidManifest(
                "manifest identity, fence, schema, or collection bounds are invalid".into(),
            ));
        }
        let mut previous_node = None;
        for source in &self.sources {
            if source.node_id == 0
                || u64::from(source.source.node_id) != source.node_id
                || source.source.source_epoch == [0; 32]
                || source.next_offset == 0
                || previous_node.is_some_and(|previous| previous >= source.node_id)
            {
                return Err(GenerationError::InvalidManifest(
                    "source checkpoints are not in unique node order".into(),
                ));
            }
            previous_node = Some(source.node_id);
        }
        let mut order_fields = BTreeSet::new();
        if self
            .physical_order
            .iter()
            .any(|order| !order_fields.insert(order.field_id))
        {
            return Err(GenerationError::InvalidManifest(
                "physical-order fields are not unique".into(),
            ));
        }
        let mut previous_segment = None;
        for segment in &self.segments {
            segment
                .validate()
                .map_err(|error| GenerationError::InvalidSegment(error.to_string()))?;
            if segment.identity.index_id != self.index_id
                || segment.identity.definition_version != self.definition_version
                || segment.identity.schema_fingerprint != self.schema_fingerprint
                || segment.packs.len() > MAX_PACKS_PER_OWNER
                || segment.components.len() > MAX_SEGMENT_COMPONENTS
            {
                return Err(GenerationError::InvalidSegment(
                    "segment identity differs from its generation".into(),
                ));
            }
            let mut previous_component = None;
            for component in &segment.components {
                let key = (component.role, component.field_id, component.ordinal);
                if previous_component.is_some_and(|previous| previous >= key) {
                    return Err(GenerationError::InvalidSegment(
                        "segment components are not in unique canonical order".into(),
                    ));
                }
                previous_component = Some(key);
            }
            if previous_segment.is_some_and(|previous| previous >= segment.identity.segment_id) {
                return Err(GenerationError::InvalidManifest(
                    "segments are not in unique segment-ID order".into(),
                ));
            }
            previous_segment = Some(segment.identity.segment_id);
        }
        let mut previous_locator = None;
        for locator in &self.locator_roots {
            locator
                .identity
                .validate()
                .map_err(|error| GenerationError::InvalidSegment(error.to_string()))?;
            let segment = self
                .segments
                .binary_search_by_key(&locator.identity.segment_id, |segment| {
                    segment.identity.segment_id
                })
                .ok()
                .map(|position| &self.segments[position]);
            let packs = match (&locator.pack_ownership, segment) {
                (LocatorPackOwnership::Segment, Some(segment))
                    if segment.identity == locator.identity =>
                {
                    &segment.packs
                }
                (LocatorPackOwnership::Standalone(packs), None) if !packs.is_empty() => packs,
                _ => {
                    return Err(GenerationError::InvalidManifest(
                        "locator pack ownership does not match the generation segments".into(),
                    ));
                }
            };
            if packs.len() > MAX_PACKS_PER_OWNER {
                return Err(GenerationError::InvalidManifest(
                    "locator pack table exceeds its manifest bound".into(),
                ));
            }
            for pack in packs {
                pack.validate(self.index_id)
                    .map_err(|error| GenerationError::InvalidArtifact(error.to_string()))?;
            }
            locator
                .artifact
                .pack(self.index_id, packs)
                .map_err(|error| GenerationError::InvalidArtifact(error.to_string()))?;
            if locator.sequence == 0
                || locator.identity.index_id != self.index_id
                || locator.identity.definition_version != self.definition_version
                || locator.identity.schema_fingerprint != self.schema_fingerprint
                || locator.artifact.component_kind != ComponentKind::ROUTING_NODE
                || locator.encoded_bytes < locator.artifact.encoded_length
                || locator.logical_bytes < locator.artifact.logical_length
                || previous_locator.is_some_and(|previous| previous >= locator.sequence)
            {
                return Err(GenerationError::InvalidManifest(
                    "locator roots are not in unique source order".into(),
                ));
            }
            previous_locator = Some(locator.sequence);
        }
        let encoded_bytes = self
            .segments
            .iter()
            .map(|segment| segment.encoded_bytes)
            .chain(
                self.locator_roots
                    .iter()
                    .map(|locator| locator.encoded_bytes),
            )
            .try_fold(0_u64, |total, bytes| {
                total
                    .checked_add(bytes)
                    .ok_or(GenerationError::LengthOverflow)
            })?;
        let logical_bytes = self
            .segments
            .iter()
            .map(|segment| segment.logical_bytes)
            .chain(
                self.locator_roots
                    .iter()
                    .map(|locator| locator.logical_bytes),
            )
            .try_fold(0_u64, |total, bytes| {
                total
                    .checked_add(bytes)
                    .ok_or(GenerationError::LengthOverflow)
            })?;
        if self.artifact_encoded_bytes != encoded_bytes
            || self.artifact_logical_bytes != logical_bytes
        {
            return Err(GenerationError::InvalidManifest(
                "manifest artifact byte totals are invalid".into(),
            ));
        }
        Ok(())
    }

    /// Resolve every locator's generation-local pack ownership into the
    /// storage-neutral roots consumed by lookup and compaction.
    pub(crate) fn locator_stream_roots(&self) -> Result<Vec<LocatorStreamRoot>, GenerationError> {
        self.validate()?;
        self.locator_roots
            .iter()
            .map(|locator| {
                let packs = match &locator.pack_ownership {
                    LocatorPackOwnership::Segment => {
                        let position = self
                            .segments
                            .binary_search_by_key(&locator.identity.segment_id, |segment| {
                                segment.identity.segment_id
                            })
                            .map_err(|_| {
                                GenerationError::InvalidManifest(
                                    "segment-owned locator has no segment".into(),
                                )
                            })?;
                        self.segments[position].packs.clone()
                    }
                    LocatorPackOwnership::Standalone(packs) => packs.clone(),
                };
                Ok(LocatorStreamRoot {
                    sequence: locator.sequence,
                    identity: locator.identity,
                    packs,
                    artifact: locator.artifact.clone(),
                })
            })
            .collect()
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

/// One mutable publication root. `current` is queryable and `retained` is in
/// strictly descending generation order. The complete set is bounded by the
/// format and contains no predecessor links.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexCurrentPointer {
    format: u16,
    pub index_id: u64,
    pub current: ManifestReference,
    pub retained: Vec<ManifestReference>,
}

impl IndexCurrentPointer {
    pub(crate) fn new(
        index_id: u64,
        current: ManifestReference,
        retained: Vec<ManifestReference>,
    ) -> Result<Self, GenerationError> {
        let value = Self {
            format: INDEX_CURRENT_FORMAT,
            index_id,
            current,
            retained,
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, GenerationError> {
        self.validate()?;
        let mut encoder = Encoder::default();
        encoder.fixed(CURRENT_MAGIC);
        encoder.u16(self.format);
        encoder.u16(0);
        encoder.u64(self.index_id);
        encode_manifest_reference(&mut encoder, &self.current)?;
        encoder.count(self.retained.len())?;
        for reference in &self.retained {
            encode_manifest_reference(&mut encoder, reference)?;
        }
        let bytes = encoder.finish();
        if bytes.len() > INDEX_COMPONENT_BYTES {
            return Err(GenerationError::LengthOverflow);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, GenerationError> {
        if bytes.len() > INDEX_COMPONENT_BYTES {
            return Err(GenerationError::InvalidPointer);
        }
        let mut decoder = Decoder::new(bytes);
        if decoder.fixed(8)? != CURRENT_MAGIC {
            return Err(GenerationError::InvalidPointer);
        }
        let format = decoder.u16()?;
        if decoder.u16()? != 0 {
            return Err(GenerationError::InvalidPointer);
        }
        let index_id = decoder.u64()?;
        let current = decode_manifest_reference(&mut decoder)?;
        let retained_count = decoder.count(MAX_RETAINED_GENERATIONS.saturating_sub(1))?;
        let mut retained = Vec::with_capacity(retained_count);
        for _ in 0..retained_count {
            retained.push(decode_manifest_reference(&mut decoder)?);
        }
        decoder.finish()?;
        let value = Self {
            format,
            index_id,
            current,
            retained,
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn validate(&self) -> Result<(), GenerationError> {
        if self.format != INDEX_CURRENT_FORMAT
            || self.index_id == 0
            || self.retained.len().saturating_add(1) > MAX_RETAINED_GENERATIONS
        {
            return Err(GenerationError::InvalidPointer);
        }
        self.current.validate(self.index_id)?;
        let mut previous_generation = self.current.generation;
        let mut paths = BTreeSet::from([self.current.path.as_str()]);
        for reference in &self.retained {
            reference.validate(self.index_id)?;
            if reference.generation >= previous_generation || !paths.insert(&reference.path) {
                return Err(GenerationError::InvalidPointer);
            }
            previous_generation = reference.generation;
        }
        Ok(())
    }

    pub(crate) fn generation(&self, generation: u64) -> Option<&ManifestReference> {
        if self.current.generation == generation {
            Some(&self.current)
        } else {
            self.retained
                .iter()
                .find(|reference| reference.generation == generation)
        }
    }
}

struct ManifestEnvelope<'a> {
    index_id: u64,
    definition_version: u64,
    schema_fingerprint: [u8; 32],
    payload: &'a [u8],
}

fn encode_manifest_envelope(
    manifest: &IndexGenerationManifest,
    payload: Vec<u8>,
) -> Result<Vec<u8>, GenerationError> {
    let encoded_length =
        u64::try_from(payload.len()).map_err(|_| GenerationError::LengthOverflow)?;
    let total = payload
        .len()
        .checked_add(COMPONENT_HEADER_BYTES)
        .ok_or(GenerationError::LengthOverflow)?;
    if total > INDEX_COMPONENT_BYTES {
        return Err(GenerationError::LengthOverflow);
    }
    let checksum = blake3::hash(&payload);
    let mut encoder = Encoder::default();
    encoder.fixed(COMPONENT_MAGIC);
    encoder.u16(ComponentKind::GENERATION_MANIFEST.get());
    encoder.u16(MANIFEST_CODEC_VERSION);
    encoder.u32(0);
    encoder.u64(manifest.index_id);
    encoder.u64(manifest.definition_version);
    encoder.fixed(&manifest.schema_fingerprint);
    encoder.u64(0);
    encoder.u64(encoded_length);
    encoder.u64(encoded_length);
    encoder.fixed(checksum.as_bytes());
    encoder.fixed(&payload);
    Ok(encoder.finish())
}

fn decode_manifest_envelope(bytes: &[u8]) -> Result<ManifestEnvelope<'_>, GenerationError> {
    if bytes.len() < COMPONENT_HEADER_BYTES || bytes.len() > INDEX_COMPONENT_BYTES {
        return Err(GenerationError::InvalidManifest(
            "manifest envelope length is invalid".into(),
        ));
    }
    let mut decoder = Decoder::new(bytes);
    if decoder.fixed(8)? != COMPONENT_MAGIC
        || decoder.u16()? != ComponentKind::GENERATION_MANIFEST.get()
        || decoder.u16()? != MANIFEST_CODEC_VERSION
        || decoder.u32()? != 0
    {
        return Err(GenerationError::InvalidManifest(
            "manifest envelope identity is invalid".into(),
        ));
    }
    let index_id = decoder.u64()?;
    let definition_version = decoder.u64()?;
    let schema_fingerprint = decoder.array_32()?;
    if decoder.u64()? != 0 {
        return Err(GenerationError::InvalidManifest(
            "manifest envelope has a segment identity".into(),
        ));
    }
    let logical_length = decoder.u64()?;
    let encoded_length = decoder.u64()?;
    let checksum = decoder.array_32()?;
    if logical_length != encoded_length
        || logical_length > INDEX_DECODE_BYTES as u64
        || encoded_length > (INDEX_COMPONENT_BYTES - COMPONENT_HEADER_BYTES) as u64
    {
        return Err(GenerationError::InvalidManifest(
            "manifest envelope lengths or codec are invalid".into(),
        ));
    }
    let payload_length =
        usize::try_from(encoded_length).map_err(|_| GenerationError::LengthOverflow)?;
    let payload = decoder.fixed(payload_length)?;
    decoder.finish()?;
    if blake3::hash(payload).as_bytes() != &checksum {
        return Err(GenerationError::InvalidManifest(
            "manifest payload checksum differs".into(),
        ));
    }
    Ok(ManifestEnvelope {
        index_id,
        definition_version,
        schema_fingerprint,
        payload,
    })
}

fn encode_segment(
    encoder: &mut Encoder,
    segment: &SegmentDescriptor,
) -> Result<(), GenerationError> {
    encoder.u64(segment.identity.index_id);
    encoder.u64(segment.identity.definition_version);
    encoder.fixed(&segment.identity.schema_fingerprint);
    encoder.u64(segment.identity.segment_id);
    encoder.u32(segment.document_count);
    encoder.u32(segment.live_document_count);
    encode_packs(encoder, &segment.packs)?;
    encoder.count(segment.components.len())?;
    for component in &segment.components {
        encoder.u16(component.role.get());
        match component.field_id {
            Some(field_id) => {
                encoder.u8(1);
                encoder.u32(field_id.get());
            }
            None => encoder.u8(0),
        }
        encoder.u32(component.ordinal);
        encode_artifact(encoder, &component.artifact)?;
    }
    encoder.u64(segment.encoded_bytes);
    encoder.u64(segment.logical_bytes);
    Ok(())
}

fn decode_segment(decoder: &mut Decoder<'_>) -> Result<SegmentDescriptor, GenerationError> {
    let identity = anvil_index::v4::SegmentIdentity {
        index_id: decoder.u64()?,
        definition_version: decoder.u64()?,
        schema_fingerprint: decoder.array_32()?,
        segment_id: decoder.u64()?,
    };
    let document_count = decoder.u32()?;
    let live_document_count = decoder.u32()?;
    let packs = decode_packs(decoder)?;
    let component_count =
        decoder.collection_count(MAX_SEGMENT_COMPONENTS, MIN_ENCODED_SEGMENT_COMPONENT_BYTES)?;
    let mut components = Vec::with_capacity(component_count);
    for _ in 0..component_count {
        let role = ComponentKind::new(decoder.u16()?)
            .map_err(|error| GenerationError::InvalidSegment(error.to_string()))?;
        let field_id = match decoder.u8()? {
            0 => None,
            1 => Some(FieldId::new(decoder.u32()?)),
            _ => return Err(GenerationError::Decode("invalid optional field ID".into())),
        };
        components.push(anvil_index::v4::SegmentComponent {
            role,
            field_id,
            ordinal: decoder.u32()?,
            artifact: decode_artifact(decoder)?,
        });
    }
    Ok(SegmentDescriptor {
        identity,
        document_count,
        live_document_count,
        packs,
        components,
        encoded_bytes: decoder.u64()?,
        logical_bytes: decoder.u64()?,
    })
}

fn encode_artifact(
    encoder: &mut Encoder,
    artifact: &ArtifactDescriptor,
) -> Result<(), GenerationError> {
    encoder.u32(artifact.pack_ordinal);
    encoder.u64(artifact.offset);
    encoder.u64(artifact.encoded_length);
    encoder.u64(artifact.logical_length);
    encoder.u16(artifact.component_kind.get());
    encoder.u16(artifact.codec_version);
    encoder.fixed(&artifact.checksum);
    Ok(())
}

fn decode_artifact(decoder: &mut Decoder<'_>) -> Result<ArtifactDescriptor, GenerationError> {
    let pack_ordinal = decoder.u32()?;
    let offset = decoder.u64()?;
    let encoded_length = decoder.u64()?;
    let logical_length = decoder.u64()?;
    let component_kind = ComponentKind::new(decoder.u16()?)
        .map_err(|error| GenerationError::InvalidArtifact(error.to_string()))?;
    let codec_version = decoder.u16()?;
    let checksum = decoder.array_32()?;
    Ok(ArtifactDescriptor {
        pack_ordinal,
        offset,
        encoded_length,
        logical_length,
        component_kind,
        codec_version,
        checksum,
    })
}

fn encode_packs(
    encoder: &mut Encoder,
    packs: &[ArtifactPackReference],
) -> Result<(), GenerationError> {
    encoder.count(packs.len())?;
    for pack in packs {
        encoder.string(&pack.path)?;
        encoder.u64(pack.object_version);
        encoder.fixed(&pack.object_content_hash);
        encoder.u64(pack.object_length);
    }
    Ok(())
}

fn decode_packs(decoder: &mut Decoder<'_>) -> Result<Vec<ArtifactPackReference>, GenerationError> {
    let count = decoder.collection_count(MAX_PACKS_PER_OWNER, MIN_ENCODED_PACK_BYTES)?;
    let mut packs = Vec::with_capacity(count);
    for _ in 0..count {
        packs.push(ArtifactPackReference {
            path: decoder.string(INDEX_ROUTING_KEY_BYTES)?,
            object_version: decoder.u64()?,
            object_content_hash: decoder.array_32()?,
            object_length: decoder.u64()?,
        });
    }
    Ok(packs)
}

fn encode_manifest_reference(
    encoder: &mut Encoder,
    reference: &ManifestReference,
) -> Result<(), GenerationError> {
    encoder.u64(reference.generation);
    encoder.u64(reference.definition_version);
    encoder.fixed(&reference.schema_fingerprint);
    encoder.string(&reference.path)?;
    encoder.fixed(&reference.blob.hash);
    encoder.u64(reference.blob.length);
    encoder.u64(reference.object_version.0);
    encoder.u64(reference.published_at_unix_millis);
    Ok(())
}

fn decode_manifest_reference(
    decoder: &mut Decoder<'_>,
) -> Result<ManifestReference, GenerationError> {
    Ok(ManifestReference {
        generation: decoder.u64()?,
        definition_version: decoder.u64()?,
        schema_fingerprint: decoder.array_32()?,
        path: decoder.string(INDEX_ROUTING_KEY_BYTES)?,
        blob: BlobRef {
            hash: decoder.array_32()?,
            length: decoder.u64()?,
        },
        object_version: VersionId(decoder.u64()?),
        published_at_unix_millis: decoder.u64()?,
    })
}

fn index_kind(tag: u8) -> Result<IndexKind, GenerationError> {
    match tag {
        1 => Ok(IndexKind::Path),
        2 => Ok(IndexKind::MetadataFilter),
        3 => Ok(IndexKind::TypedJson),
        4 => Ok(IndexKind::FullText),
        5 => Ok(IndexKind::Vector),
        6 => Ok(IndexKind::Hybrid),
        7 => Ok(IndexKind::GitSource),
        8 => Ok(IndexKind::Tensor),
        _ => Err(GenerationError::Decode("unknown index kind".into())),
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

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn option_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.u64(value);
            }
            None => self.u8(0),
        }
    }

    fn count(&mut self, value: usize) -> Result<(), GenerationError> {
        self.u32(u32::try_from(value).map_err(|_| GenerationError::LengthOverflow)?);
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<(), GenerationError> {
        self.count(value.len())?;
        self.fixed(value.as_bytes());
        Ok(())
    }

    fn fixed(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn fixed(&mut self, length: usize) -> Result<&'a [u8], GenerationError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(GenerationError::LengthOverflow)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| GenerationError::Decode("truncated record".into()))?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, GenerationError> {
        Ok(self.fixed(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, GenerationError> {
        Ok(u16::from_le_bytes(
            self.fixed(2)?.try_into().expect("fixed length"),
        ))
    }

    fn u32(&mut self) -> Result<u32, GenerationError> {
        Ok(u32::from_le_bytes(
            self.fixed(4)?.try_into().expect("fixed length"),
        ))
    }

    fn u64(&mut self) -> Result<u64, GenerationError> {
        Ok(u64::from_le_bytes(
            self.fixed(8)?.try_into().expect("fixed length"),
        ))
    }

    fn array_32(&mut self) -> Result<[u8; 32], GenerationError> {
        Ok(self.fixed(32)?.try_into().expect("fixed length"))
    }

    fn boolean(&mut self) -> Result<bool, GenerationError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(GenerationError::Decode("invalid Boolean".into())),
        }
    }

    fn option_u64(&mut self) -> Result<Option<u64>, GenerationError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            _ => Err(GenerationError::Decode("invalid optional integer".into())),
        }
    }

    fn count(&mut self, maximum: usize) -> Result<usize, GenerationError> {
        self.collection_count(maximum, 1)
    }

    fn collection_count(
        &mut self,
        maximum: usize,
        minimum_encoded_bytes: usize,
    ) -> Result<usize, GenerationError> {
        let value = usize::try_from(self.u32()?).map_err(|_| GenerationError::LengthOverflow)?;
        let remaining = self.bytes.len().saturating_sub(self.offset);
        if minimum_encoded_bytes == 0
            || value > maximum
            || value > remaining / minimum_encoded_bytes
        {
            return Err(GenerationError::Decode(
                "collection bound is invalid".into(),
            ));
        }
        Ok(value)
    }

    fn string(&mut self, maximum: usize) -> Result<String, GenerationError> {
        let length = self.count(maximum)?;
        let value = self.fixed(length)?;
        std::str::from_utf8(value)
            .map(str::to_owned)
            .map_err(|_| GenerationError::Decode("string is not UTF-8".into()))
    }

    fn finish(self) -> Result<(), GenerationError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(GenerationError::Decode("record has trailing bytes".into()))
        }
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum GenerationError {
    #[error("index artifact reference is invalid: {0}")]
    InvalidArtifact(String),
    #[error("index segment descriptor is invalid: {0}")]
    InvalidSegment(String),
    #[error("index generation manifest is invalid: {0}")]
    InvalidManifest(String),
    #[error("index manifest reference is invalid")]
    InvalidManifestReference,
    #[error("index current pointer is invalid or uses an unsupported format")]
    InvalidPointer,
    #[error("index generation length overflow")]
    LengthOverflow,
    #[error("system clock predates the Unix epoch")]
    ClockBeforeEpoch,
    #[error("index timestamp overflow")]
    TimestampOverflow,
    #[error("decode index v4 object: {0}")]
    Decode(String),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use anvil_consensus::NodeId;

    use super::*;
    use crate::index_runtime::events::{AtomicProgramWatermark, IndexSourceCursor};
    use crate::index_runtime::publication::artifact_path;

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
                    next_offset: 17,
                },
            )]),
        }
    }

    fn artifact(index_id: u64, seed: u8, component_kind: ComponentKind) -> ArtifactDescriptor {
        ArtifactDescriptor::new(
            index_id,
            u32::from(seed - 1),
            16,
            128,
            8,
            component_kind,
            1,
            [seed.wrapping_add(1); 32],
        )
        .unwrap()
    }

    fn pack(index_id: u64, seed: u8) -> ArtifactPackReference {
        let object_hash = [seed; 32];
        ArtifactPackReference::new(
            index_id,
            artifact_path(index_id, object_hash),
            u64::from(seed) + 1,
            object_hash,
            512,
        )
        .unwrap()
    }

    fn manifest(generation: u64) -> IndexGenerationManifest {
        manifest_with_schema(generation, [7; 32])
    }

    fn manifest_with_schema(
        generation: u64,
        schema_fingerprint: [u8; 32],
    ) -> IndexGenerationManifest {
        let identity = anvil_index::v4::SegmentIdentity::new(4, 8, schema_fingerprint, 91).unwrap();
        let segment = SegmentDescriptor::new(
            identity,
            7,
            6,
            (1..=4).map(|seed| pack(4, seed)).collect(),
            vec![
                anvil_index::v4::SegmentComponent {
                    role: ComponentKind::IDENTITY_TABLE,
                    field_id: None,
                    ordinal: 0,
                    artifact: artifact(4, 1, ComponentKind::ROUTING_NODE),
                },
                anvil_index::v4::SegmentComponent {
                    role: ComponentKind::LIVE_MASK,
                    field_id: None,
                    ordinal: 0,
                    artifact: artifact(4, 2, ComponentKind::ROUTING_NODE),
                },
                anvil_index::v4::SegmentComponent {
                    role: ComponentKind::SCORING_STATISTICS,
                    field_id: None,
                    ordinal: 0,
                    artifact: artifact(4, 3, ComponentKind::ROUTING_NODE),
                },
            ],
            384,
            24,
        )
        .unwrap();
        IndexGenerationManifest::new(
            4,
            generation,
            8,
            IndexKind::TypedJson,
            schema_fingerprint,
            &barrier(),
            vec![ManifestPhysicalOrder {
                field_id: FieldId::new(0),
                descending: true,
            }],
            vec![segment],
            vec![LocatorRoot {
                sequence: 1,
                identity,
                artifact: artifact(4, 4, ComponentKind::ROUTING_NODE),
                pack_ownership: LocatorPackOwnership::Segment,
                encoded_bytes: 128,
                logical_bytes: 8,
            }],
            512,
            32,
        )
        .unwrap()
    }

    fn manifest_blob(manifest: &IndexGenerationManifest) -> BlobRef {
        let encoded = manifest.encode().unwrap();
        BlobRef {
            hash: *blake3::hash(&encoded).as_bytes(),
            length: encoded.len() as u64,
        }
    }

    #[test]
    fn v4_manifest_round_trip_uses_checked_envelope() {
        let manifest = manifest(3);
        let encoded = manifest.encode().unwrap();
        assert_eq!(&encoded[..8], COMPONENT_MAGIC);
        assert_eq!(IndexGenerationManifest::decode(&encoded).unwrap(), manifest);

        let mut corrupt = encoded;
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(matches!(
            IndexGenerationManifest::decode(&corrupt),
            Err(GenerationError::InvalidManifest(_))
        ));
    }

    #[test]
    fn schema_fingerprint_is_an_opaque_32_byte_value() {
        let manifest = manifest_with_schema(3, [0xff; 32]);
        let encoded = manifest.encode().unwrap();
        assert_eq!(IndexGenerationManifest::decode(&encoded).unwrap(), manifest);
    }

    #[test]
    fn detached_locator_root_is_bound_to_identity_and_complete_subtree_totals() {
        let mut value = manifest(3);
        value.locator_roots[0].identity.segment_id += 1;
        value.locator_roots[0].artifact.pack_ordinal = 0;
        value.locator_roots[0].pack_ownership = LocatorPackOwnership::Standalone(vec![pack(4, 4)]);
        assert!(value.validate().is_ok());

        let encoded = value.encode().unwrap();
        let decoded = IndexGenerationManifest::decode(&encoded).unwrap();
        assert_eq!(decoded, value);
        let roots = decoded.locator_stream_roots().unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].packs, vec![pack(4, 4)]);

        value.locator_roots[0].identity.definition_version += 1;
        assert!(matches!(
            value.validate(),
            Err(GenerationError::InvalidManifest(_))
        ));

        let mut value = manifest(3);
        value.locator_roots[0].encoded_bytes = 127;
        assert!(matches!(
            value.validate(),
            Err(GenerationError::InvalidManifest(_))
        ));
    }

    #[test]
    fn segment_owned_locator_resolves_its_segment_pack_table() {
        let value = manifest(3);
        let roots = value.locator_stream_roots().unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].identity, value.segments[0].identity);
        assert_eq!(roots[0].packs, value.segments[0].packs);
        assert_eq!(roots[0].artifact, value.locator_roots[0].artifact);
    }

    #[test]
    fn locator_pack_ownership_is_unambiguous() {
        let mut missing_segment = manifest(3);
        missing_segment.locator_roots[0].identity.segment_id += 1;
        assert!(matches!(
            missing_segment.validate(),
            Err(GenerationError::InvalidManifest(_))
        ));

        let mut duplicate_owner = manifest(3);
        duplicate_owner.locator_roots[0].pack_ownership =
            LocatorPackOwnership::Standalone(vec![pack(4, 4)]);
        duplicate_owner.locator_roots[0].artifact.pack_ordinal = 0;
        assert!(matches!(
            duplicate_owner.validate(),
            Err(GenerationError::InvalidManifest(_))
        ));
    }

    #[test]
    fn current_pointer_carries_a_bounded_root_set_without_predecessors() {
        let current_manifest = manifest(9);
        let old_manifest = manifest(8);
        let published = UNIX_EPOCH + Duration::from_secs(10);
        let current = ManifestReference::new(
            &current_manifest,
            manifest_blob(&current_manifest),
            VersionId(20),
            published,
        )
        .unwrap();
        let retained = ManifestReference::new(
            &old_manifest,
            manifest_blob(&old_manifest),
            VersionId(19),
            published,
        )
        .unwrap();
        let pointer = IndexCurrentPointer::new(4, current, vec![retained]).unwrap();
        let encoded = pointer.encode().unwrap();
        assert_eq!(IndexCurrentPointer::decode(&encoded).unwrap(), pointer);
        assert_eq!(pointer.generation(8).unwrap().generation, 8);
    }

    #[test]
    fn current_pointer_rejects_duplicate_or_unordered_generations() {
        let value = manifest(9);
        let published = UNIX_EPOCH + Duration::from_secs(10);
        let reference =
            ManifestReference::new(&value, manifest_blob(&value), VersionId(20), published)
                .unwrap();
        assert_eq!(
            IndexCurrentPointer::new(4, reference.clone(), vec![reference]).unwrap_err(),
            GenerationError::InvalidPointer
        );
    }

    #[test]
    fn manifest_reference_is_bound_to_exact_manifest_bytes() {
        let value = manifest(9);
        let mut blob = manifest_blob(&value);
        blob.hash[0] ^= 1;
        assert_eq!(
            ManifestReference::new(
                &value,
                blob,
                VersionId(20),
                UNIX_EPOCH + Duration::from_secs(10),
            )
            .unwrap_err(),
            GenerationError::InvalidManifestReference
        );
    }

    #[test]
    fn artifact_reference_requires_a_valid_pack_ordinal_and_range() {
        let mut object = pack(4, 1);
        object.path.push_str("/extra");
        assert!(object.validate(4).is_err());

        let reference = artifact(4, 2, ComponentKind::IDENTITY_TABLE);
        assert!(reference.pack(4, &[pack(4, 1)]).is_err());

        let mut reference = artifact(4, 1, ComponentKind::IDENTITY_TABLE);
        reference.offset = 500;
        assert!(reference.pack(4, &[pack(4, 1)]).is_err());
    }

    #[test]
    fn old_json_is_not_a_compatibility_input() {
        let old = br#"{"format":3,"index_id":4,"generation":1}"#;
        assert!(IndexGenerationManifest::decode(old).is_err());
        assert!(IndexCurrentPointer::decode(old).is_err());
    }
}
