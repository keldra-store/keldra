//! Portable format-v4 index revision and current-pointer records.
//!
//! These records contain only ordinary-object references. Component bytes,
//! manifests, and the mutable current pointer remain ordinary Keldra objects;
//! no index state is stored in Raft or a side plane.

use std::collections::BTreeSet;
use std::mem::size_of;
use std::time::{SystemTime, UNIX_EPOCH};

use keldra_index::v4::{
    ArtifactDescriptor, ArtifactPackReference, ComponentKind, FieldId, INDEX_COMMIT_SEGMENTS,
    INDEX_COMPONENT_BYTES, INDEX_FORMAT_VERSION, INDEX_ROUTING_KEY_BYTES, IndexKind,
    LocatorStreamRoot, SegmentDescriptor,
};
use keldra_store::{BlobRef, PlacementLogId, SourceId, VersionId};
use thiserror::Error;

use super::events::IndexBarrier;
use super::publication::manifest_path;

pub(crate) const INDEX_MANIFEST_FORMAT: u16 = INDEX_FORMAT_VERSION;
pub(crate) const INDEX_CURRENT_FORMAT: u16 = INDEX_FORMAT_VERSION;
pub(crate) const MAX_RETAINED_COMMIT_REVISIONS: usize = 64;
// A canonical maximum-width manifest reference plus release timestamp encodes
// to 238 bytes. 2,048 roots leave room for the current reference, all 63
// retained references, and the pointer envelope beneath the 512 KiB codec
// bound. Reaching this queue fails publication closed instead of losing a root.
pub(crate) const MAX_RELEASING_COMMIT_REVISIONS: usize = 2_048;
// About 2.5 MiB of fixed-width identity at the ceiling. Hitting this bound
// fails catch-up closed and lets the already bounded source journal apply its
// normal upstream backpressure rather than growing commit metadata without
// limit while another source vector is unavailable.
pub(crate) const MAX_PENDING_ATOMIC_BATCHES: usize = 65_536;

const MANIFEST_MAGIC: &[u8; 8] = b"ANVLMNF4";
const CURRENT_MAGIC: &[u8; 8] = b"ANVLCUR4";
// KELDRA-0016 is a clean persistence break. Manifest version 4 adds bounded
// pending-atomic identity plus non-owning predecessor evidence; pointer version
// 3 adds exact releasing roots. Older transitional records are rejected.
const MANIFEST_CODEC_VERSION: u16 = 4;
const CURRENT_POINTER_CODEC_VERSION: u16 = 3;
const MANIFEST_HEADER_BYTES: usize =
    MANIFEST_MAGIC.len() + size_of::<u16>() * 2 + size_of::<u64>() * 3 + 32;
pub(crate) const MAX_SEGMENTS_PER_COMMIT: usize = INDEX_COMMIT_SEGMENTS;
const MAX_SEGMENT_COMPONENTS: usize = 4_096;
pub(crate) const MAX_LOCATOR_ROOTS_PER_COMMIT: usize = 4_096;
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
    /// First source-local journal offset not represented by this revision.
    pub next_offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LocatorRoot {
    /// Strictly increasing source-state order; larger values are newer.
    pub sequence: u64,
    /// Exact envelope identity needed to validate a detached locator tree.
    pub identity: keldra_index::v4::SegmentIdentity,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingAtomicBatch {
    pub cursor: u64,
    pub bundle_hash: keldra_store::PreparedBundleHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitManifestReference {
    pub revision: u64,
    pub definition_version: u64,
    pub schema_fingerprint: [u8; 32],
    pub path: String,
    pub blob: BlobRef,
    pub object_version: VersionId,
    pub published_at_unix_millis: u64,
    /// Conservative bytes retained by this root: the manifest object plus all
    /// segment/locator artifacts named by it. Shared artifacts may be counted
    /// more than once, which trims earlier but can never exceed the bound.
    pub retained_bytes: u64,
}

impl CommitManifestReference {
    pub(crate) fn new(
        manifest: &IndexCommitManifest,
        blob: BlobRef,
        object_version: VersionId,
        published_at: SystemTime,
    ) -> Result<Self, CommitViewError> {
        let retained_bytes = manifest
            .artifact_encoded_bytes
            .checked_add(blob.length)
            .ok_or(CommitViewError::SizeLimit)?;
        let value = Self {
            revision: manifest.revision,
            definition_version: manifest.definition_version,
            schema_fingerprint: manifest.schema_fingerprint,
            path: manifest_path(manifest.index_id, blob.hash),
            blob,
            object_version,
            published_at_unix_millis: unix_millis(published_at)?,
            retained_bytes,
        };
        value.validate(manifest.index_id)?;
        Ok(value)
    }

    pub(crate) fn validate(&self, index_id: u64) -> Result<(), CommitViewError> {
        if index_id == 0
            || self.revision == 0
            || self.definition_version == 0
            || self.blob.length < MANIFEST_HEADER_BYTES as u64
            || self.object_version.0 == 0
            || self.published_at_unix_millis == 0
            || self.retained_bytes < self.blob.length
            || self.path.len() > INDEX_ROUTING_KEY_BYTES
            || self.path != manifest_path(index_id, self.blob.hash)
        {
            return Err(CommitViewError::InvalidCommitManifestReference);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexCommitManifest {
    format: u16,
    pub index_id: u64,
    pub revision: u64,
    pub definition_version: u64,
    pub kind: IndexKind,
    pub schema_fingerprint: [u8; 32],
    pub placement_fence: PlacementLogId,
    pub atomic_through: Option<u64>,
    /// Atomic publication cursors already materialized in this revision but
    /// not yet covered by the complete global atomic watermark. Persisting
    /// this bounded set makes retry idempotence independent of executor
    /// identity while preserving partial-vector progress.
    pub pending_atomic_batches: Vec<PendingAtomicBatch>,
    /// Non-owning lineage evidence only. This hash never retains, resolves, or
    /// authorizes traversal to a predecessor manifest; retention authority is
    /// exclusively the bounded current-pointer root sets.
    pub previous_manifest_hash: Option<[u8; 32]>,
    pub sources: Vec<ManifestSourceCheckpoint>,
    pub physical_order: Vec<ManifestPhysicalOrder>,
    /// Each descriptor is the complete immutable segment root set. Its own
    /// `validate` contract requires exactly one canonical LIVE_MASK component,
    /// so a separate manifest-level `live_document_views` vector would create
    /// a conflicting liveness authority and is deliberately absent.
    pub segments: Vec<SegmentDescriptor>,
    pub locator_roots: Vec<LocatorRoot>,
    pub artifact_encoded_bytes: u64,
    pub artifact_logical_bytes: u64,
}

impl IndexCommitManifest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        index_id: u64,
        revision: u64,
        definition_version: u64,
        kind: IndexKind,
        schema_fingerprint: [u8; 32],
        barrier: &IndexBarrier,
        pending_atomic_batches: Vec<PendingAtomicBatch>,
        previous_manifest_hash: Option<[u8; 32]>,
        physical_order: Vec<ManifestPhysicalOrder>,
        segments: Vec<SegmentDescriptor>,
        locator_roots: Vec<LocatorRoot>,
        artifact_encoded_bytes: u64,
        artifact_logical_bytes: u64,
    ) -> Result<Self, CommitViewError> {
        let value = Self {
            format: INDEX_MANIFEST_FORMAT,
            index_id,
            revision,
            definition_version,
            kind,
            schema_fingerprint,
            placement_fence: barrier.fence,
            atomic_through: barrier.atomic.finalized_through(),
            pending_atomic_batches,
            previous_manifest_hash,
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

    pub(crate) fn encode(&self) -> Result<Vec<u8>, CommitViewError> {
        self.validate()?;
        let mut payload = Encoder::default();
        payload.u16(self.format);
        payload.u64(self.revision);
        payload.u8(self.kind as u8);
        payload.u64(self.placement_fence.term);
        payload.u64(self.placement_fence.index);
        payload.option_u64(self.atomic_through);
        payload.count(self.pending_atomic_batches.len())?;
        for pending in &self.pending_atomic_batches {
            payload.u64(pending.cursor);
            payload.fixed(&pending.bundle_hash.0);
        }
        match self.previous_manifest_hash {
            Some(hash) => {
                payload.u8(1);
                payload.fixed(&hash);
            }
            None => payload.u8(0),
        }
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

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, CommitViewError> {
        let envelope = decode_manifest_envelope(bytes)?;
        let mut payload = Decoder::new(envelope.payload);
        let format = payload.u16()?;
        let revision = payload.u64()?;
        let kind = index_kind(payload.u8()?)?;
        let placement_fence = PlacementLogId {
            term: payload.u64()?,
            index: payload.u64()?,
        };
        let atomic_through = payload.option_u64()?;
        let pending_atomic_count =
            payload.collection_count(MAX_PENDING_ATOMIC_BATCHES, size_of::<u64>() + 32)?;
        let mut pending_atomic_batches = Vec::with_capacity(pending_atomic_count);
        for _ in 0..pending_atomic_count {
            pending_atomic_batches.push(PendingAtomicBatch {
                cursor: payload.u64()?,
                bundle_hash: keldra_store::PreparedBundleHash(payload.array_32()?),
            });
        }
        let previous_manifest_hash = match payload.u8()? {
            0 => None,
            1 => Some(payload.array_32()?),
            _ => {
                return Err(CommitViewError::InvalidManifest(
                    "manifest predecessor option is invalid".into(),
                ));
            }
        };
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
            payload.collection_count(MAX_SEGMENTS_PER_COMMIT, MIN_ENCODED_SEGMENT_BYTES)?;
        let mut segments = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            segments.push(decode_segment(&mut payload)?);
        }
        let locator_count = payload
            .collection_count(MAX_LOCATOR_ROOTS_PER_COMMIT, MIN_ENCODED_LOCATOR_ROOT_BYTES)?;
        let mut locator_roots = Vec::with_capacity(locator_count);
        for _ in 0..locator_count {
            let sequence = payload.u64()?;
            let identity = keldra_index::v4::SegmentIdentity {
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
                    return Err(CommitViewError::Decode(
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
            revision,
            definition_version: envelope.definition_version,
            kind,
            schema_fingerprint: envelope.schema_fingerprint,
            placement_fence,
            atomic_through,
            pending_atomic_batches,
            previous_manifest_hash,
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

    pub(crate) fn validate(&self) -> Result<(), CommitViewError> {
        if self.format != INDEX_MANIFEST_FORMAT
            || self.index_id == 0
            || self.revision == 0
            || self.definition_version == 0
            || self.placement_fence.term == 0
            || self.placement_fence.index == 0
            || self.atomic_through == Some(0)
            || self.pending_atomic_batches.len() > MAX_PENDING_ATOMIC_BATCHES
            || self.previous_manifest_hash == Some([0; 32])
            || self.sources.len() > MAX_SOURCE_CHECKPOINTS
            || self.physical_order.len() > MAX_PHYSICAL_ORDER_FIELDS
            || self.segments.len() > MAX_SEGMENTS_PER_COMMIT
            || self.locator_roots.len() > MAX_LOCATOR_ROOTS_PER_COMMIT
        {
            return Err(CommitViewError::InvalidManifest(
                "manifest identity, fence, schema, or collection bounds are invalid".into(),
            ));
        }
        let mut previous_atomic_cursor = self.atomic_through;
        for pending in &self.pending_atomic_batches {
            if pending.cursor == 0
                || pending.bundle_hash.0 == [0; 32]
                || previous_atomic_cursor.is_some_and(|previous| previous >= pending.cursor)
            {
                return Err(CommitViewError::InvalidManifest(
                    "pending atomic cursors are not above the watermark in unique order".into(),
                ));
            }
            previous_atomic_cursor = Some(pending.cursor);
        }
        let mut previous_node = None;
        for source in &self.sources {
            if source.node_id == 0
                || u64::from(source.source.node_id) != source.node_id
                || source.source.source_epoch == [0; 32]
                || source.next_offset == 0
                || previous_node.is_some_and(|previous| previous >= source.node_id)
            {
                return Err(CommitViewError::InvalidManifest(
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
            return Err(CommitViewError::InvalidManifest(
                "physical-order fields are not unique".into(),
            ));
        }
        let mut previous_segment = None;
        for segment in &self.segments {
            segment
                .validate()
                .map_err(|error| CommitViewError::InvalidSegment(error.to_string()))?;
            if segment.identity.index_id != self.index_id
                || segment.identity.definition_version != self.definition_version
                || segment.identity.schema_fingerprint != self.schema_fingerprint
                || segment.packs.len() > MAX_PACKS_PER_OWNER
                || segment.components.len() > MAX_SEGMENT_COMPONENTS
            {
                return Err(CommitViewError::InvalidSegment(
                    "segment identity differs from its revision".into(),
                ));
            }
            let mut previous_component = None;
            for component in &segment.components {
                let key = (component.role, component.field_id, component.ordinal);
                if previous_component.is_some_and(|previous| previous >= key) {
                    return Err(CommitViewError::InvalidSegment(
                        "segment components are not in unique canonical order".into(),
                    ));
                }
                previous_component = Some(key);
            }
            if previous_segment.is_some_and(|previous| previous >= segment.identity.segment_id) {
                return Err(CommitViewError::InvalidManifest(
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
                .map_err(|error| CommitViewError::InvalidSegment(error.to_string()))?;
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
                    return Err(CommitViewError::InvalidManifest(
                        "locator pack ownership does not match the revision segments".into(),
                    ));
                }
            };
            if packs.len() > MAX_PACKS_PER_OWNER {
                return Err(CommitViewError::InvalidManifest(
                    "locator pack table exceeds its manifest bound".into(),
                ));
            }
            for pack in packs {
                pack.validate(self.index_id)
                    .map_err(|error| CommitViewError::InvalidArtifact(error.to_string()))?;
            }
            locator
                .artifact
                .pack(self.index_id, packs)
                .map_err(|error| CommitViewError::InvalidArtifact(error.to_string()))?;
            if locator.sequence == 0
                || locator.identity.index_id != self.index_id
                || locator.identity.definition_version != self.definition_version
                || locator.identity.schema_fingerprint != self.schema_fingerprint
                || locator.artifact.component_kind != ComponentKind::ROUTING_NODE
                || locator.encoded_bytes < locator.artifact.encoded_length
                || locator.logical_bytes < locator.artifact.logical_length
                || previous_locator.is_some_and(|previous| previous >= locator.sequence)
            {
                return Err(CommitViewError::InvalidManifest(
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
                total.checked_add(bytes).ok_or(CommitViewError::SizeLimit)
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
                total.checked_add(bytes).ok_or(CommitViewError::SizeLimit)
            })?;
        if self.artifact_encoded_bytes != encoded_bytes
            || self.artifact_logical_bytes != logical_bytes
        {
            return Err(CommitViewError::InvalidManifest(
                "manifest artifact byte totals are invalid".into(),
            ));
        }
        Ok(())
    }

    /// Resolve every locator's revision-local pack ownership into the
    /// storage-neutral roots consumed by lookup and compaction.
    pub(crate) fn locator_stream_roots(&self) -> Result<Vec<LocatorStreamRoot>, CommitViewError> {
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
                                CommitViewError::InvalidManifest(
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

    pub(crate) fn barrier(&self) -> Result<IndexBarrier, CommitViewError> {
        let sources = self
            .sources
            .iter()
            .map(|source| {
                (
                    keldra_consensus::NodeId(source.node_id),
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
                self.atomic_through,
                self.atomic_through,
                0,
            ),
            sources,
        })
    }
}

/// A manifest root removed from reader retention but still durably named until
/// its exact object graph has been deleted. The release timestamp records when
/// that exact cleanup obligation was created; known-unshared objects do not
/// wait for the independent uncertain-orphan safety age.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReleasingManifestReference {
    pub manifest: CommitManifestReference,
    pub released_at_unix_millis: u64,
}

impl ReleasingManifestReference {
    pub(crate) fn new(
        manifest: CommitManifestReference,
        released_at: SystemTime,
    ) -> Result<Self, CommitViewError> {
        let value = Self {
            manifest,
            released_at_unix_millis: unix_millis(released_at)?,
        };
        if value.released_at_unix_millis == 0 {
            return Err(CommitViewError::InvalidPointer);
        }
        Ok(value)
    }
}

/// One mutable publication root. `current` is queryable, `retained` is in
/// strictly descending revision order, and `releasing` is deliberately not
/// reader-visible. The latter makes asynchronous cleanup exact and
/// restart-safe without a namespace scan. Every set is format-bounded and
/// contains no predecessor links.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexCurrentPointer {
    format: u16,
    pub index_id: u64,
    pub current: CommitManifestReference,
    pub retained: Vec<CommitManifestReference>,
    pub releasing: Vec<ReleasingManifestReference>,
}

impl IndexCurrentPointer {
    pub(crate) fn new(
        index_id: u64,
        current: CommitManifestReference,
        retained: Vec<CommitManifestReference>,
        releasing: Vec<ReleasingManifestReference>,
    ) -> Result<Self, CommitViewError> {
        let value = Self {
            format: INDEX_CURRENT_FORMAT,
            index_id,
            current,
            retained,
            releasing,
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, CommitViewError> {
        self.validate()?;
        let mut encoder = Encoder::default();
        encoder.fixed(CURRENT_MAGIC);
        encoder.u16(self.format);
        encoder.u16(CURRENT_POINTER_CODEC_VERSION);
        encoder.u64(self.index_id);
        encode_manifest_reference(&mut encoder, &self.current)?;
        encoder.count(self.retained.len())?;
        for reference in &self.retained {
            encode_manifest_reference(&mut encoder, reference)?;
        }
        encoder.count(self.releasing.len())?;
        for reference in &self.releasing {
            encode_manifest_reference(&mut encoder, &reference.manifest)?;
            encoder.u64(reference.released_at_unix_millis);
        }
        let bytes = encoder.finish();
        if bytes.len() > INDEX_COMPONENT_BYTES {
            return Err(CommitViewError::SizeLimit);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, CommitViewError> {
        if bytes.len() > INDEX_COMPONENT_BYTES {
            return Err(CommitViewError::InvalidPointer);
        }
        let mut decoder = Decoder::new(bytes);
        if decoder.fixed(8)? != CURRENT_MAGIC {
            return Err(CommitViewError::InvalidPointer);
        }
        let format = decoder.u16()?;
        if decoder.u16()? != CURRENT_POINTER_CODEC_VERSION {
            return Err(CommitViewError::InvalidPointer);
        }
        let index_id = decoder.u64()?;
        let current = decode_manifest_reference(&mut decoder)?;
        let retained_count = decoder.count(MAX_RETAINED_COMMIT_REVISIONS.saturating_sub(1))?;
        let mut retained = Vec::with_capacity(retained_count);
        for _ in 0..retained_count {
            retained.push(decode_manifest_reference(&mut decoder)?);
        }
        let releasing_count = decoder.count(MAX_RELEASING_COMMIT_REVISIONS)?;
        let mut releasing = Vec::with_capacity(releasing_count);
        for _ in 0..releasing_count {
            releasing.push(ReleasingManifestReference {
                manifest: decode_manifest_reference(&mut decoder)?,
                released_at_unix_millis: decoder.u64()?,
            });
        }
        decoder.finish()?;
        let value = Self {
            format,
            index_id,
            current,
            retained,
            releasing,
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn validate(&self) -> Result<(), CommitViewError> {
        if self.format != INDEX_CURRENT_FORMAT
            || self.index_id == 0
            || self.retained.len().saturating_add(1) > MAX_RETAINED_COMMIT_REVISIONS
            || self.releasing.len() > MAX_RELEASING_COMMIT_REVISIONS
        {
            return Err(CommitViewError::InvalidPointer);
        }
        self.current.validate(self.index_id)?;
        let mut previous_revision = self.current.revision;
        let mut paths = BTreeSet::from([self.current.path.as_str()]);
        let mut revisions = BTreeSet::from([self.current.revision]);
        for reference in &self.retained {
            reference.validate(self.index_id)?;
            if reference.revision >= previous_revision
                || !paths.insert(&reference.path)
                || !revisions.insert(reference.revision)
            {
                return Err(CommitViewError::InvalidPointer);
            }
            previous_revision = reference.revision;
        }
        for reference in &self.releasing {
            reference.manifest.validate(self.index_id)?;
            if reference.released_at_unix_millis == 0
                || !paths.insert(&reference.manifest.path)
                || !revisions.insert(reference.manifest.revision)
            {
                return Err(CommitViewError::InvalidPointer);
            }
        }
        Ok(())
    }

    pub(crate) fn revision(&self, revision: u64) -> Option<&CommitManifestReference> {
        if self.current.revision == revision {
            Some(&self.current)
        } else {
            self.retained
                .iter()
                .find(|reference| reference.revision == revision)
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
    manifest: &IndexCommitManifest,
    payload: Vec<u8>,
) -> Result<Vec<u8>, CommitViewError> {
    let encoded_length = u64::try_from(payload.len()).map_err(|_| CommitViewError::SizeLimit)?;
    let mut encoder = Encoder::default();
    encoder.fixed(MANIFEST_MAGIC);
    encoder.u16(MANIFEST_CODEC_VERSION);
    encoder.u16(0);
    encoder.u64(manifest.index_id);
    encoder.u64(manifest.definition_version);
    encoder.fixed(&manifest.schema_fingerprint);
    encoder.u64(encoded_length);
    encoder.fixed(&payload);
    Ok(encoder.finish())
}

fn decode_manifest_envelope(bytes: &[u8]) -> Result<ManifestEnvelope<'_>, CommitViewError> {
    if bytes.len() < MANIFEST_HEADER_BYTES {
        return Err(CommitViewError::InvalidManifest(
            "manifest envelope length is invalid".into(),
        ));
    }
    let mut decoder = Decoder::new(bytes);
    if decoder.fixed(8)? != MANIFEST_MAGIC
        || decoder.u16()? != MANIFEST_CODEC_VERSION
        || decoder.u16()? != 0
    {
        return Err(CommitViewError::InvalidManifest(
            "manifest envelope identity is invalid".into(),
        ));
    }
    let index_id = decoder.u64()?;
    let definition_version = decoder.u64()?;
    let schema_fingerprint = decoder.array_32()?;
    let encoded_length = decoder.u64()?;
    let payload_length = usize::try_from(encoded_length).map_err(|_| CommitViewError::SizeLimit)?;
    let payload = decoder.fixed(payload_length)?;
    decoder.finish()?;
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
) -> Result<(), CommitViewError> {
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

fn decode_segment(decoder: &mut Decoder<'_>) -> Result<SegmentDescriptor, CommitViewError> {
    let identity = keldra_index::v4::SegmentIdentity {
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
            .map_err(|error| CommitViewError::InvalidSegment(error.to_string()))?;
        let field_id = match decoder.u8()? {
            0 => None,
            1 => Some(FieldId::new(decoder.u32()?)),
            _ => return Err(CommitViewError::Decode("invalid optional field ID".into())),
        };
        components.push(keldra_index::v4::SegmentComponent {
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
) -> Result<(), CommitViewError> {
    encoder.u32(artifact.pack_ordinal);
    encoder.u64(artifact.offset);
    encoder.u64(artifact.encoded_length);
    encoder.u64(artifact.logical_length);
    encoder.u16(artifact.component_kind.get());
    encoder.u16(artifact.codec_version);
    encoder.fixed(&artifact.checksum);
    Ok(())
}

fn decode_artifact(decoder: &mut Decoder<'_>) -> Result<ArtifactDescriptor, CommitViewError> {
    let pack_ordinal = decoder.u32()?;
    let offset = decoder.u64()?;
    let encoded_length = decoder.u64()?;
    let logical_length = decoder.u64()?;
    let component_kind = ComponentKind::new(decoder.u16()?)
        .map_err(|error| CommitViewError::InvalidArtifact(error.to_string()))?;
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
) -> Result<(), CommitViewError> {
    encoder.count(packs.len())?;
    for pack in packs {
        encoder.string(&pack.path)?;
        encoder.u64(pack.object_version);
        encoder.fixed(&pack.object_content_hash);
        encoder.u64(pack.object_length);
    }
    Ok(())
}

fn decode_packs(decoder: &mut Decoder<'_>) -> Result<Vec<ArtifactPackReference>, CommitViewError> {
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
    reference: &CommitManifestReference,
) -> Result<(), CommitViewError> {
    encoder.u64(reference.revision);
    encoder.u64(reference.definition_version);
    encoder.fixed(&reference.schema_fingerprint);
    encoder.string(&reference.path)?;
    encoder.fixed(&reference.blob.hash);
    encoder.u64(reference.blob.length);
    encoder.u64(reference.object_version.0);
    encoder.u64(reference.published_at_unix_millis);
    encoder.u64(reference.retained_bytes);
    Ok(())
}

fn decode_manifest_reference(
    decoder: &mut Decoder<'_>,
) -> Result<CommitManifestReference, CommitViewError> {
    Ok(CommitManifestReference {
        revision: decoder.u64()?,
        definition_version: decoder.u64()?,
        schema_fingerprint: decoder.array_32()?,
        path: decoder.string(INDEX_ROUTING_KEY_BYTES)?,
        blob: BlobRef {
            hash: decoder.array_32()?,
            length: decoder.u64()?,
        },
        object_version: VersionId(decoder.u64()?),
        published_at_unix_millis: decoder.u64()?,
        retained_bytes: decoder.u64()?,
    })
}

fn index_kind(tag: u8) -> Result<IndexKind, CommitViewError> {
    match tag {
        1 => Ok(IndexKind::Path),
        2 => Ok(IndexKind::MetadataFilter),
        3 => Ok(IndexKind::TypedJson),
        4 => Ok(IndexKind::FullText),
        5 => Ok(IndexKind::Vector),
        6 => Ok(IndexKind::Hybrid),
        7 => Ok(IndexKind::GitSource),
        8 => Ok(IndexKind::Tensor),
        _ => Err(CommitViewError::Decode("unknown index kind".into())),
    }
}

fn unix_millis(time: SystemTime) -> Result<u64, CommitViewError> {
    u64::try_from(
        time.duration_since(UNIX_EPOCH)
            .map_err(|_| CommitViewError::ClockBeforeEpoch)?
            .as_millis(),
    )
    .map_err(|_| CommitViewError::TimestampOverflow)
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

    fn count(&mut self, value: usize) -> Result<(), CommitViewError> {
        self.u32(u32::try_from(value).map_err(|_| CommitViewError::SizeLimit)?);
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<(), CommitViewError> {
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

    fn fixed(&mut self, length: usize) -> Result<&'a [u8], CommitViewError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CommitViewError::SizeLimit)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| CommitViewError::Decode("truncated record".into()))?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, CommitViewError> {
        Ok(self.fixed(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, CommitViewError> {
        Ok(u16::from_le_bytes(
            self.fixed(2)?.try_into().expect("fixed length"),
        ))
    }

    fn u32(&mut self) -> Result<u32, CommitViewError> {
        Ok(u32::from_le_bytes(
            self.fixed(4)?.try_into().expect("fixed length"),
        ))
    }

    fn u64(&mut self) -> Result<u64, CommitViewError> {
        Ok(u64::from_le_bytes(
            self.fixed(8)?.try_into().expect("fixed length"),
        ))
    }

    fn array_32(&mut self) -> Result<[u8; 32], CommitViewError> {
        Ok(self.fixed(32)?.try_into().expect("fixed length"))
    }

    fn boolean(&mut self) -> Result<bool, CommitViewError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(CommitViewError::Decode("invalid Boolean".into())),
        }
    }

    fn option_u64(&mut self) -> Result<Option<u64>, CommitViewError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            _ => Err(CommitViewError::Decode("invalid optional integer".into())),
        }
    }

    fn count(&mut self, maximum: usize) -> Result<usize, CommitViewError> {
        self.collection_count(maximum, 1)
    }

    fn collection_count(
        &mut self,
        maximum: usize,
        minimum_encoded_bytes: usize,
    ) -> Result<usize, CommitViewError> {
        let value = usize::try_from(self.u32()?).map_err(|_| CommitViewError::SizeLimit)?;
        let remaining = self.bytes.len().saturating_sub(self.offset);
        if minimum_encoded_bytes == 0
            || value > maximum
            || value > remaining / minimum_encoded_bytes
        {
            return Err(CommitViewError::Decode(
                "collection bound is invalid".into(),
            ));
        }
        Ok(value)
    }

    fn string(&mut self, maximum: usize) -> Result<String, CommitViewError> {
        let length = self.count(maximum)?;
        let value = self.fixed(length)?;
        std::str::from_utf8(value)
            .map(str::to_owned)
            .map_err(|_| CommitViewError::Decode("string is not UTF-8".into()))
    }

    fn finish(self) -> Result<(), CommitViewError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(CommitViewError::Decode("record has trailing bytes".into()))
        }
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum CommitViewError {
    #[error("index artifact reference is invalid: {0}")]
    InvalidArtifact(String),
    #[error("index segment descriptor is invalid: {0}")]
    InvalidSegment(String),
    #[error("index revision manifest is invalid: {0}")]
    InvalidManifest(String),
    #[error("index manifest reference is invalid")]
    InvalidCommitManifestReference,
    #[error("index current pointer is invalid or uses an unsupported format")]
    InvalidPointer,
    #[error("index revision exceeds an encoded integer or platform size limit")]
    SizeLimit,
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

    use keldra_consensus::NodeId;

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

    fn manifest(revision: u64) -> IndexCommitManifest {
        manifest_with_schema(revision, [7; 32])
    }

    fn manifest_with_schema(revision: u64, schema_fingerprint: [u8; 32]) -> IndexCommitManifest {
        let identity =
            keldra_index::v4::SegmentIdentity::new(4, 8, schema_fingerprint, 91).unwrap();
        let segment = SegmentDescriptor::new(
            identity,
            7,
            6,
            (1..=4).map(|seed| pack(4, seed)).collect(),
            vec![
                keldra_index::v4::SegmentComponent {
                    role: ComponentKind::IDENTITY_TABLE,
                    field_id: None,
                    ordinal: 0,
                    artifact: artifact(4, 1, ComponentKind::ROUTING_NODE),
                },
                keldra_index::v4::SegmentComponent {
                    role: ComponentKind::LIVE_MASK,
                    field_id: None,
                    ordinal: 0,
                    artifact: artifact(4, 2, ComponentKind::ROUTING_NODE),
                },
                keldra_index::v4::SegmentComponent {
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
        IndexCommitManifest::new(
            4,
            revision,
            8,
            IndexKind::TypedJson,
            schema_fingerprint,
            &barrier(),
            Vec::new(),
            None,
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

    fn manifest_blob(manifest: &IndexCommitManifest) -> BlobRef {
        let encoded = manifest.encode().unwrap();
        BlobRef {
            hash: *blake3::hash(&encoded).as_bytes(),
            length: encoded.len() as u64,
        }
    }

    #[test]
    fn v4_manifest_round_trip_uses_checked_envelope() {
        let mut manifest = manifest(3);
        manifest.previous_manifest_hash = Some([4; 32]);
        let encoded = manifest.encode().unwrap();
        assert_eq!(&encoded[..8], MANIFEST_MAGIC);
        assert_eq!(IndexCommitManifest::decode(&encoded).unwrap(), manifest);

        let mut corrupt = encoded;
        corrupt.push(0);
        assert!(matches!(
            IndexCommitManifest::decode(&corrupt),
            Err(CommitViewError::Decode(_))
        ));
        manifest.previous_manifest_hash = Some([0; 32]);
        assert!(matches!(
            manifest.validate(),
            Err(CommitViewError::InvalidManifest(_))
        ));
    }

    #[test]
    fn manifest_persists_pending_atomic_identity_and_rejects_conflicts() {
        let mut value = manifest(3);
        value.pending_atomic_batches = vec![
            PendingAtomicBatch {
                cursor: 10,
                bundle_hash: keldra_store::PreparedBundleHash([1; 32]),
            },
            PendingAtomicBatch {
                cursor: 12,
                bundle_hash: keldra_store::PreparedBundleHash([2; 32]),
            },
        ];
        let encoded = value.encode().unwrap();
        assert_eq!(IndexCommitManifest::decode(&encoded).unwrap(), value);

        value.pending_atomic_batches[1].cursor = 10;
        assert!(matches!(
            value.validate(),
            Err(CommitViewError::InvalidManifest(_))
        ));
        value.pending_atomic_batches[1].cursor = 8;
        assert!(matches!(
            value.validate(),
            Err(CommitViewError::InvalidManifest(_))
        ));
        value.pending_atomic_batches = vec![PendingAtomicBatch {
            cursor: 10,
            bundle_hash: keldra_store::PreparedBundleHash([0; 32]),
        }];
        assert!(matches!(
            value.validate(),
            Err(CommitViewError::InvalidManifest(_))
        ));
    }

    #[test]
    fn obsolete_candidate_codec_is_rejected() {
        let manifest = manifest(3);
        let mut encoded = manifest.encode().unwrap();
        encoded[8..10].copy_from_slice(&1_u16.to_be_bytes());
        assert!(IndexCommitManifest::decode(&encoded).is_err());

        let blob = manifest_blob(&manifest);
        let reference = CommitManifestReference::new(
            &manifest,
            blob,
            VersionId(20),
            UNIX_EPOCH + Duration::from_secs(10),
        )
        .unwrap();
        let pointer = IndexCurrentPointer::new(4, reference, Vec::new(), Vec::new()).unwrap();
        let mut encoded = pointer.encode().unwrap();
        encoded[10..12].copy_from_slice(&1_u16.to_be_bytes());
        assert!(IndexCurrentPointer::decode(&encoded).is_err());
    }

    #[test]
    fn manifest_larger_than_one_component_round_trips_as_an_ordinary_object() {
        let template = manifest(3);
        let mut segments = Vec::new();
        let mut locator_roots = Vec::new();
        for ordinal in 0_u64..800 {
            let mut segment = template.segments[0].clone();
            segment.identity.segment_id = 1_000 + ordinal;
            let mut locator = template.locator_roots[0].clone();
            locator.sequence = ordinal + 1;
            locator.identity = segment.identity;
            segments.push(segment);
            locator_roots.push(locator);
        }
        let value = IndexCommitManifest::new(
            template.index_id,
            template.revision,
            template.definition_version,
            template.kind,
            template.schema_fingerprint,
            &barrier(),
            Vec::new(),
            None,
            template.physical_order,
            segments,
            locator_roots,
            512 * 800,
            32 * 800,
        )
        .unwrap();

        let encoded = value.encode().unwrap();

        assert!(encoded.len() > INDEX_COMPONENT_BYTES);
        assert_eq!(IndexCommitManifest::decode(&encoded).unwrap(), value);
    }

    #[test]
    fn schema_fingerprint_is_an_opaque_32_byte_value() {
        let manifest = manifest_with_schema(3, [0xff; 32]);
        let encoded = manifest.encode().unwrap();
        assert_eq!(IndexCommitManifest::decode(&encoded).unwrap(), manifest);
    }

    #[test]
    fn detached_locator_root_is_bound_to_identity_and_complete_subtree_totals() {
        let mut value = manifest(3);
        value.locator_roots[0].identity.segment_id += 1;
        value.locator_roots[0].artifact.pack_ordinal = 0;
        value.locator_roots[0].pack_ownership = LocatorPackOwnership::Standalone(vec![pack(4, 4)]);
        assert!(value.validate().is_ok());

        let encoded = value.encode().unwrap();
        let decoded = IndexCommitManifest::decode(&encoded).unwrap();
        assert_eq!(decoded, value);
        let roots = decoded.locator_stream_roots().unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].packs, vec![pack(4, 4)]);

        value.locator_roots[0].identity.definition_version += 1;
        assert!(matches!(
            value.validate(),
            Err(CommitViewError::InvalidManifest(_))
        ));

        let mut value = manifest(3);
        value.locator_roots[0].encoded_bytes = 127;
        assert!(matches!(
            value.validate(),
            Err(CommitViewError::InvalidManifest(_))
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
            Err(CommitViewError::InvalidManifest(_))
        ));

        let mut duplicate_owner = manifest(3);
        duplicate_owner.locator_roots[0].pack_ownership =
            LocatorPackOwnership::Standalone(vec![pack(4, 4)]);
        duplicate_owner.locator_roots[0].artifact.pack_ordinal = 0;
        assert!(matches!(
            duplicate_owner.validate(),
            Err(CommitViewError::InvalidManifest(_))
        ));
    }

    #[test]
    fn current_pointer_carries_a_bounded_root_set_without_predecessors() {
        let current_manifest = manifest(9);
        let old_manifest = manifest(8);
        let published = UNIX_EPOCH + Duration::from_secs(10);
        let current_blob = manifest_blob(&current_manifest);
        let current = CommitManifestReference::new(
            &current_manifest,
            current_blob.clone(),
            VersionId(20),
            published,
        )
        .unwrap();
        let old_blob = manifest_blob(&old_manifest);
        let retained =
            CommitManifestReference::new(&old_manifest, old_blob.clone(), VersionId(19), published)
                .unwrap();
        let pointer = IndexCurrentPointer::new(4, current, vec![retained], Vec::new()).unwrap();
        let encoded = pointer.encode().unwrap();
        assert_eq!(IndexCurrentPointer::decode(&encoded).unwrap(), pointer);
        assert_eq!(pointer.revision(8).unwrap().revision, 8);
    }

    #[test]
    fn releasing_manifest_is_durable_but_not_reader_visible() {
        let current_manifest = manifest(9);
        let released_manifest = manifest(8);
        let published = UNIX_EPOCH + Duration::from_secs(10);
        let current = CommitManifestReference::new(
            &current_manifest,
            manifest_blob(&current_manifest),
            VersionId(20),
            published,
        )
        .unwrap();
        let released = CommitManifestReference::new(
            &released_manifest,
            manifest_blob(&released_manifest),
            VersionId(19),
            published,
        )
        .unwrap();
        let pointer = IndexCurrentPointer::new(
            4,
            current,
            Vec::new(),
            vec![
                ReleasingManifestReference::new(released, published + Duration::from_secs(5))
                    .unwrap(),
            ],
        )
        .unwrap();
        let encoded = pointer.encode().unwrap();
        assert_eq!(IndexCurrentPointer::decode(&encoded).unwrap(), pointer);
        assert!(pointer.revision(8).is_none());
        assert_eq!(pointer.releasing[0].manifest.revision, 8);
        assert_eq!(pointer.releasing[0].released_at_unix_millis, 15_000);
    }

    #[test]
    fn maximum_releasing_root_bound_fits_the_current_pointer_codec() {
        fn reference(revision: u64) -> CommitManifestReference {
            let hash = *blake3::hash(&revision.to_be_bytes()).as_bytes();
            CommitManifestReference {
                revision,
                definition_version: 1,
                schema_fingerprint: [1; 32],
                path: manifest_path(u64::MAX, hash),
                blob: BlobRef {
                    hash,
                    length: MANIFEST_HEADER_BYTES as u64,
                },
                object_version: VersionId(revision),
                published_at_unix_millis: 1,
                retained_bytes: MANIFEST_HEADER_BYTES as u64,
            }
        }

        let releasing = (1..=MAX_RELEASING_COMMIT_REVISIONS as u64)
            .map(|revision| ReleasingManifestReference {
                manifest: reference(revision),
                released_at_unix_millis: 1,
            })
            .collect::<Vec<_>>();
        let retained = (0..MAX_RETAINED_COMMIT_REVISIONS - 1)
            .map(|offset| reference(9_999 - offset as u64))
            .collect::<Vec<_>>();
        let pointer = IndexCurrentPointer::new(
            u64::MAX,
            reference(10_000),
            retained.clone(),
            releasing.clone(),
        )
        .unwrap();
        assert!(pointer.encode().unwrap().len() <= INDEX_COMPONENT_BYTES);

        let mut too_many = releasing;
        too_many.push(ReleasingManifestReference {
            manifest: reference(3_000),
            released_at_unix_millis: 1,
        });
        assert_eq!(
            IndexCurrentPointer::new(u64::MAX, reference(10_000), retained, too_many,).unwrap_err(),
            CommitViewError::InvalidPointer,
        );
    }

    #[test]
    fn current_pointer_rejects_duplicate_or_unordered_commit_revisions() {
        let value = manifest(9);
        let published = UNIX_EPOCH + Duration::from_secs(10);
        let blob = manifest_blob(&value);
        let reference =
            CommitManifestReference::new(&value, blob.clone(), VersionId(20), published).unwrap();
        assert_eq!(
            IndexCurrentPointer::new(4, reference.clone(), vec![reference], Vec::new())
                .unwrap_err(),
            CommitViewError::InvalidPointer
        );
    }

    #[test]
    fn manifest_reference_uses_the_already_staged_blob_identity() {
        let value = manifest(9);
        let blob = manifest_blob(&value);
        let reference = CommitManifestReference::new(
            &value,
            blob.clone(),
            VersionId(20),
            UNIX_EPOCH + Duration::from_secs(10),
        )
        .unwrap();

        assert_eq!(reference.blob, blob);
        assert_eq!(reference.path, manifest_path(value.index_id, blob.hash));
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
        let old = br#"{"format":3,"index_id":4,"revision":1}"#;
        assert!(IndexCommitManifest::decode(old).is_err());
        assert!(IndexCurrentPointer::decode(old).is_err());
    }
}
