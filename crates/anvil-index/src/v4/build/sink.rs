use std::collections::BTreeMap;
use std::future::Future;

use crate::IndexError;

use super::super::codec::{
    COMPONENT_HEADER_BYTES, decode_component_header, prepare_component_payload,
};
use super::super::{
    ArtifactDescriptor, ArtifactPackReference, ComponentKind, ComponentStatistics, FieldId,
    GeneratedComponent, INDEX_ARTIFACT_PACK_BYTES, INDEX_DECODE_BYTES, INDEX_ROUTING_FANOUT,
    INDEX_ROUTING_HEIGHT, RoutingEntry, RoutingNode, SegmentIdentity, artifact_path,
    encode_component,
};

// A checked component always contains at least its fixed envelope. The byte
// ceiling is authoritative; deriving the count ceiling from the largest legal
// component made 32 tiny components look like a full 16 MiB pack and caused
// tens of thousands of mostly-empty ordinary-object publications.
const INDEX_ARTIFACT_PACK_COMPONENTS: usize = INDEX_ARTIFACT_PACK_BYTES / COMPONENT_HEADER_BYTES;
// Accumulate enough child groups to fill one pack even when every routing
// component reaches the format maximum. Shorter routing components may share
// the same pack until the byte ceiling, but this threshold keeps streaming
// state bounded independently of the input size.
const ROUTING_COMPONENTS_PER_BATCH: usize =
    INDEX_ARTIFACT_PACK_BYTES / super::super::INDEX_COMPONENT_BYTES;
/// Two worst-case 32,772-byte term boundaries plus one descriptor fit seven
/// times within a 512 KiB routing component. Short-key streams retain the
/// format-wide fanout of 32.
const LONG_TERM_ROUTING_FANOUT: usize = 7;

fn routing_fanout(logical_kind: ComponentKind) -> usize {
    if logical_kind == ComponentKind::TERM_DICTIONARY {
        LONG_TERM_ROUTING_FANOUT
    } else {
        INDEX_ROUTING_FANOUT
    }
}

fn validate_stream_routing_key(logical_kind: ComponentKind, key: &[u8]) -> Result<(), IndexError> {
    super::super::routing::validate_logical_routing_key(logical_kind, key)
}

/// Segment-scoped artifact staging and grouped publication.
///
/// A sink assigns pack ordinals and checked component ranges while keeping at
/// most one incomplete 16 MiB pack resident. Completed packs may be spooled,
/// but no ordinary-object reference becomes authoritative until
/// `finalize_segment` publishes the complete pack set.
pub trait ComponentBatchSink: Send {
    fn begin_segment(
        &mut self,
        identity: SegmentIdentity,
        base_packs: &[ArtifactPackReference],
    ) -> Result<(), IndexError>;

    fn stage_component(
        &mut self,
        component: GeneratedComponent,
    ) -> impl Future<Output = Result<ArtifactDescriptor, IndexError>> + Send;

    fn finalize_segment(
        &mut self,
        identity: SegmentIdentity,
    ) -> impl Future<Output = Result<Vec<ArtifactPackReference>, IndexError>> + Send;
}

/// One move-only ordinary-object pack assembled by the format layer.
///
/// Component boundaries are recovered from their checked fixed envelopes.
/// Keeping a second, per-component layout vector would make resident memory
/// depend on the number of tiny components in a pack.
#[derive(Debug)]
pub struct ComponentPack {
    identity: SegmentIdentity,
    bytes: Vec<u8>,
}

impl ComponentPack {
    pub fn identity(&self) -> SegmentIdentity {
        self.identity
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn encoded_bytes(&self) -> u64 {
        self.bytes.len() as u64
    }

    pub fn component_count(&self) -> Result<u64, IndexError> {
        let mut offset = 0usize;
        let mut count = 0u64;
        while offset < self.bytes.len() {
            let (_, length) = self.component_at(offset)?;
            offset = offset
                .checked_add(length)
                .ok_or(IndexError::OffsetOverflow)?;
            count = count.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
            if count > INDEX_ARTIFACT_PACK_COMPONENTS as u64 {
                return Err(IndexError::InvalidFormat(
                    "format-v4 artifact pack has too many components",
                ));
            }
        }
        Ok(count)
    }

    pub fn reference(
        &self,
        path: String,
        object_version: u64,
        object_content_hash: [u8; 32],
    ) -> Result<ArtifactPackReference, IndexError> {
        if object_content_hash != *blake3::hash(&self.bytes).as_bytes() {
            return Err(IndexError::Integrity);
        }
        let object_length =
            u64::try_from(self.bytes.len()).map_err(|_| IndexError::OffsetOverflow)?;
        self.component_count()?;
        ArtifactPackReference::new(
            self.identity.index_id,
            path,
            object_version,
            object_content_hash,
            object_length,
        )
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    fn component_at(
        &self,
        offset: usize,
    ) -> Result<(super::super::ComponentHeader, usize), IndexError> {
        if self.bytes.is_empty() || self.bytes.len() > INDEX_ARTIFACT_PACK_BYTES {
            return Err(IndexError::InvalidFormat("format-v4 component pack length"));
        }
        let remaining = self.bytes.get(offset..).ok_or(IndexError::OffsetOverflow)?;
        let header = decode_component_header(remaining)?;
        if header.identity != self.identity {
            return Err(IndexError::InvalidFormat(
                "one component pack crossed segment identities",
            ));
        }
        let payload =
            usize::try_from(header.encoded_length).map_err(|_| IndexError::OffsetOverflow)?;
        let length = COMPONENT_HEADER_BYTES
            .checked_add(payload)
            .ok_or(IndexError::OffsetOverflow)?;
        if offset
            .checked_add(length)
            .is_none_or(|end| end > self.bytes.len())
        {
            return Err(IndexError::InvalidFormat(
                "component extends beyond its artifact pack",
            ));
        }
        Ok((header, length))
    }
}

#[derive(Debug)]
struct ComponentPackBuilder {
    identity: Option<SegmentIdentity>,
    bytes: Vec<u8>,
    component_count: usize,
}

impl ComponentPackBuilder {
    fn new() -> Self {
        Self {
            identity: None,
            bytes: Vec::with_capacity(INDEX_ARTIFACT_PACK_BYTES),
            component_count: 0,
        }
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn is_full(&self) -> bool {
        self.component_count == INDEX_ARTIFACT_PACK_COMPONENTS
    }

    fn accepts(&self, encoded: usize) -> bool {
        !self.is_full()
            && self
                .bytes
                .len()
                .checked_add(encoded)
                .is_some_and(|needed| needed <= INDEX_ARTIFACT_PACK_BYTES)
    }

    fn push(&mut self, component: GeneratedComponent) -> Result<(), IndexError> {
        let header = component.header();
        if self
            .identity
            .is_some_and(|identity| identity != header.identity)
        {
            return Err(IndexError::InvalidDefinition(
                "one component pack cannot cross segment identities".into(),
            ));
        }
        let encoded = component.bytes().len();
        let needed = self
            .bytes
            .len()
            .checked_add(encoded)
            .ok_or(IndexError::OffsetOverflow)?;
        if needed > INDEX_ARTIFACT_PACK_BYTES {
            return Err(IndexError::ResourceLimit {
                needed,
                limit: INDEX_ARTIFACT_PACK_BYTES,
            });
        }
        if self.component_count == INDEX_ARTIFACT_PACK_COMPONENTS {
            return Err(IndexError::ResourceLimit {
                needed: self.component_count.saturating_add(1),
                limit: INDEX_ARTIFACT_PACK_COMPONENTS,
            });
        }
        self.identity = Some(header.identity);
        self.bytes.extend_from_slice(component.bytes());
        self.component_count = self
            .component_count
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(())
    }

    fn finish(self) -> Result<ComponentPack, IndexError> {
        Ok(ComponentPack {
            identity: self.identity.ok_or_else(|| {
                IndexError::InvalidDefinition("component pack must not be empty".into())
            })?,
            bytes: self.bytes,
        })
    }
}

async fn publish_single_component<S: ComponentBatchSink>(
    sink: &mut S,
    component: GeneratedComponent,
) -> Result<ArtifactDescriptor, IndexError> {
    sink.stage_component(component).await
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedObject {
    pub path: String,
    pub object_version: u64,
    pub bytes: Vec<u8>,
}

/// Exact in-memory ordinary-object pack sink used by format tests.
#[derive(Debug)]
pub struct ExactMemorySink {
    objects: BTreeMap<String, PublishedObject>,
    next_object_version: u64,
    publish_calls: usize,
    active: Option<MemorySegmentPacks>,
}

#[derive(Debug)]
struct MemorySegmentPacks {
    identity: SegmentIdentity,
    base_packs: Vec<ArtifactPackReference>,
    completed: Vec<ComponentPack>,
    pending: ComponentPackBuilder,
}

impl Default for ExactMemorySink {
    fn default() -> Self {
        Self::new()
    }
}

impl ExactMemorySink {
    pub fn new() -> Self {
        Self {
            objects: BTreeMap::new(),
            next_object_version: 1,
            publish_calls: 0,
            active: None,
        }
    }

    pub fn objects(&self) -> &BTreeMap<String, PublishedObject> {
        &self.objects
    }

    pub fn publish_calls(&self) -> usize {
        self.publish_calls
    }

    pub fn component_bytes<'a>(
        &'a self,
        packs: &[ArtifactPackReference],
        descriptor: &ArtifactDescriptor,
    ) -> Result<&'a [u8], IndexError> {
        let pack = packs
            .get(descriptor.pack_ordinal as usize)
            .ok_or(IndexError::InvalidFormat("artifact pack ordinal"))?;
        let object = self
            .objects
            .get(&pack.path)
            .ok_or_else(|| IndexError::FileNotFound(pack.path.clone()))?;
        if object.object_version != pack.object_version {
            return Err(IndexError::Integrity);
        }
        if object.bytes.len() as u64 != pack.object_length
            || *blake3::hash(&object.bytes).as_bytes() != pack.object_content_hash
        {
            return Err(IndexError::Integrity);
        }
        let start = usize::try_from(descriptor.offset).map_err(|_| IndexError::OffsetOverflow)?;
        let length =
            usize::try_from(descriptor.encoded_length).map_err(|_| IndexError::OffsetOverflow)?;
        let end = start
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        object
            .bytes
            .get(start..end)
            .ok_or(IndexError::InvalidFormat(
                "component range outside memory pack",
            ))
    }

    fn publish_memory_pack(
        &mut self,
        pack: ComponentPack,
    ) -> Result<ArtifactPackReference, IndexError> {
        let hash = *blake3::hash(pack.bytes()).as_bytes();
        let path = artifact_path(pack.identity().index_id, hash);
        let existing_version = if let Some(existing) = self.objects.get(&path) {
            if existing.bytes != pack.bytes() {
                return Err(IndexError::Integrity);
            }
            Some(existing.object_version)
        } else {
            None
        };
        let object_version = match existing_version {
            Some(version) => version,
            None => {
                let version = self.next_object_version;
                self.next_object_version = self
                    .next_object_version
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                version
            }
        };
        let reference = pack.reference(path.clone(), object_version, hash)?;
        if existing_version.is_none() {
            self.objects.insert(
                path.clone(),
                PublishedObject {
                    path: path.clone(),
                    object_version,
                    bytes: pack.into_bytes(),
                },
            );
        }
        Ok(reference)
    }
}

impl ComponentBatchSink for ExactMemorySink {
    fn begin_segment(
        &mut self,
        identity: SegmentIdentity,
        base_packs: &[ArtifactPackReference],
    ) -> Result<(), IndexError> {
        identity.validate()?;
        if self.active.is_some() {
            return Err(IndexError::InvalidDefinition(
                "component sink already has an active segment".into(),
            ));
        }
        for pack in base_packs {
            pack.validate(identity.index_id)?;
        }
        self.active = Some(MemorySegmentPacks {
            identity,
            base_packs: base_packs.to_vec(),
            completed: Vec::new(),
            pending: ComponentPackBuilder::new(),
        });
        Ok(())
    }

    fn stage_component(
        &mut self,
        component: GeneratedComponent,
    ) -> impl Future<Output = Result<ArtifactDescriptor, IndexError>> + Send {
        std::future::ready((|| {
            let identity = component.header().identity;
            let active = self.active.as_mut().ok_or(IndexError::InvalidFormat(
                "component sink has no active segment",
            ))?;
            if active.identity != identity {
                return Err(IndexError::InvalidDefinition(
                    "component sink cannot cross segment identities".into(),
                ));
            }
            let encoded = component.bytes().len();
            if active.pending.identity.is_some() && !active.pending.accepts(encoded) {
                let pending = std::mem::replace(&mut active.pending, ComponentPackBuilder::new());
                active.completed.push(pending.finish()?);
            }
            let pack_ordinal = u32::try_from(
                active
                    .base_packs
                    .len()
                    .checked_add(active.completed.len())
                    .ok_or(IndexError::OffsetOverflow)?,
            )
            .map_err(|_| IndexError::OffsetOverflow)?;
            let offset =
                u64::try_from(active.pending.len()).map_err(|_| IndexError::OffsetOverflow)?;
            let descriptor = component.placed(pack_ordinal, offset)?;
            active.pending.push(component)?;
            if active.pending.is_full() || active.pending.len() == INDEX_ARTIFACT_PACK_BYTES {
                let pending = std::mem::replace(&mut active.pending, ComponentPackBuilder::new());
                active.completed.push(pending.finish()?);
            }
            Ok(descriptor)
        })())
    }

    fn finalize_segment(
        &mut self,
        identity: SegmentIdentity,
    ) -> impl Future<Output = Result<Vec<ArtifactPackReference>, IndexError>> + Send {
        std::future::ready((|| {
            let mut active = self.active.take().ok_or(IndexError::InvalidFormat(
                "component sink has no active segment",
            ))?;
            if active.identity != identity {
                return Err(IndexError::InvalidDefinition(
                    "component sink finalized another segment identity".into(),
                ));
            }
            if active.pending.identity.is_some() {
                active.completed.push(active.pending.finish()?);
            }
            let mut references = active.base_packs;
            references.reserve(active.completed.len());
            for pack in active.completed {
                self.publish_calls = self
                    .publish_calls
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                references.push(self.publish_memory_pack(pack)?);
            }
            Ok(references)
        })())
    }
}

pub struct ComponentLeaf {
    pub minimum_key: Vec<u8>,
    pub maximum_key: Vec<u8>,
    pub element_count: u64,
    pub component: GeneratedComponent,
}

/// One already-published data leaf and the exact routing evidence needed to
/// reuse it in a replacement immutable stream. This is deliberately distinct
/// from [`ComponentLeaf`]: rebuilding a routing root must not republish an
/// unchanged data component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescriptorLeaf {
    pub minimum_key: Vec<u8>,
    pub maximum_key: Vec<u8>,
    pub element_count: u64,
    pub descriptor: ArtifactDescriptor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedStream {
    pub root: ArtifactDescriptor,
    pub minimum_key: Vec<u8>,
    pub maximum_key: Vec<u8>,
    pub element_count: u64,
    pub routing_height: u8,
    /// Complete recursively referenced component bytes, including envelopes.
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub leaf_count: u64,
    pub component_count: u64,
}

impl PublishedStream {
    pub(crate) fn statistics(
        &self,
        role: ComponentKind,
        field_id: Option<FieldId>,
    ) -> Result<Option<ComponentStatistics>, IndexError> {
        if !super::super::text::tracks_component_statistics(role) {
            return Ok(None);
        }
        Ok(Some(ComponentStatistics {
            role,
            field_id,
            leaf_count: self.leaf_count,
            component_count: self.component_count,
            encoded_bytes: self.encoded_bytes,
            logical_bytes: self.logical_bytes,
            decoded_bytes_upper_bound: self
                .component_count
                .checked_mul(INDEX_DECODE_BYTES as u64)
                .ok_or(IndexError::OffsetOverflow)?,
        }))
    }
}

struct PublishedNode {
    minimum_key: Vec<u8>,
    maximum_key: Vec<u8>,
    element_count: u64,
    descriptor: ArtifactDescriptor,
}

/// Fixed-memory publisher for one routed component stream.
///
/// Data leaves are staged into the segment sink's ordinary-object packs. The
/// sink owns the one incomplete pack shared by every logical stream; this
/// publisher retains only one incomplete fanout group at each routing height.
pub struct StreamingComponentPublisher<'a, S> {
    sink: &'a mut S,
    identity: SegmentIdentity,
    logical_kind: ComponentKind,
    leaf_codec_version: u16,
    routing_codec_version: u16,
    levels: Vec<Vec<PublishedNode>>,
    first_minimum_key: Option<Vec<u8>>,
    previous_maximum_key: Option<Vec<u8>>,
    element_count: u64,
    encoded_bytes: u64,
    logical_bytes: u64,
    component_count: u64,
    leaf_count: u64,
}

impl<'a, S: ComponentBatchSink> StreamingComponentPublisher<'a, S> {
    pub fn new(
        sink: &'a mut S,
        identity: SegmentIdentity,
        logical_kind: ComponentKind,
        leaf_codec_version: u16,
        routing_codec_version: u16,
    ) -> Result<Self, IndexError> {
        identity.validate()?;
        if logical_kind == ComponentKind::ROUTING_NODE
            || leaf_codec_version == 0
            || routing_codec_version == 0
        {
            return Err(IndexError::InvalidDefinition(
                "streaming component publisher requires data and routing codecs".into(),
            ));
        }
        Ok(Self {
            sink,
            identity,
            logical_kind,
            leaf_codec_version,
            routing_codec_version,
            levels: vec![Vec::new()],
            first_minimum_key: None,
            previous_maximum_key: None,
            element_count: 0,
            encoded_bytes: 0,
            logical_bytes: 0,
            component_count: 0,
            leaf_count: 0,
        })
    }

    pub fn leaf_count(&self) -> u64 {
        self.leaf_count
    }

    pub async fn push_payload(
        &mut self,
        minimum_key: Vec<u8>,
        maximum_key: Vec<u8>,
        element_count: u64,
        payload: Vec<u8>,
    ) -> Result<(), IndexError> {
        let (flags, logical_length, payload) =
            prepare_component_payload(self.logical_kind, self.leaf_codec_version, payload)?;
        let component = encode_component(
            self.identity,
            self.logical_kind,
            self.leaf_codec_version,
            flags,
            logical_length,
            payload,
        )?;
        self.push_leaf(ComponentLeaf {
            minimum_key,
            maximum_key,
            element_count,
            component,
        })
        .await
    }

    pub async fn push_leaf(&mut self, leaf: ComponentLeaf) -> Result<(), IndexError> {
        self.validate_leaf_range(&leaf.minimum_key, &leaf.maximum_key, leaf.element_count)?;
        let header = leaf.component.header();
        if header.identity != self.identity
            || header.component_kind != self.logical_kind
            || header.codec_version != self.leaf_codec_version
        {
            return Err(IndexError::InvalidDefinition(
                "streaming component leaves must have ordered disjoint ranges and one identity"
                    .into(),
            ));
        }
        let encoded = leaf.component.bytes().len();
        if encoded > INDEX_ARTIFACT_PACK_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: encoded,
                limit: INDEX_ARTIFACT_PACK_BYTES,
            });
        }
        let descriptor = self.sink.stage_component(leaf.component).await?;
        validate_placed_component(&descriptor, header, encoded)?;
        self.push_published_leaf(DescriptorLeaf {
            minimum_key: leaf.minimum_key,
            maximum_key: leaf.maximum_key,
            element_count: leaf.element_count,
            descriptor,
        })
        .await
    }

    /// Add one already-published leaf without opening or publishing its data
    /// component again. The descriptor must have been obtained by traversing
    /// this publisher's exact `SegmentIdentity`; descriptors deliberately do
    /// not duplicate the segment identity carried by their checked envelope.
    pub async fn push_descriptor_leaf(&mut self, leaf: DescriptorLeaf) -> Result<(), IndexError> {
        self.validate_leaf_range(&leaf.minimum_key, &leaf.maximum_key, leaf.element_count)?;
        leaf.descriptor.validate(self.identity.index_id)?;
        if leaf.descriptor.component_kind != self.logical_kind
            || leaf.descriptor.codec_version != self.leaf_codec_version
        {
            return Err(IndexError::InvalidDefinition(
                "reused component leaf differs from the publisher stream".into(),
            ));
        }
        self.push_published_leaf(leaf).await
    }

    async fn push_published_leaf(&mut self, leaf: DescriptorLeaf) -> Result<(), IndexError> {
        self.record_descriptor(&leaf.descriptor)?;
        if self.first_minimum_key.is_none() {
            self.first_minimum_key = Some(leaf.minimum_key.clone());
        }
        self.previous_maximum_key = Some(leaf.maximum_key.clone());
        self.element_count = self
            .element_count
            .checked_add(leaf.element_count)
            .ok_or(IndexError::OffsetOverflow)?;
        self.leaf_count = self
            .leaf_count
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        self.levels[0].push(PublishedNode {
            minimum_key: leaf.minimum_key,
            maximum_key: leaf.maximum_key,
            element_count: leaf.element_count,
            descriptor: leaf.descriptor,
        });
        self.cascade_full_levels(0).await
    }

    fn validate_leaf_range(
        &self,
        minimum_key: &[u8],
        maximum_key: &[u8],
        element_count: u64,
    ) -> Result<(), IndexError> {
        validate_stream_routing_key(self.logical_kind, minimum_key)?;
        validate_stream_routing_key(self.logical_kind, maximum_key)?;
        let previous_maximum_key = self.previous_maximum_key.as_ref();
        if element_count == 0
            || minimum_key.is_empty()
            || minimum_key > maximum_key
            || previous_maximum_key.is_some_and(|previous| previous.as_slice() >= minimum_key)
        {
            return Err(IndexError::InvalidDefinition(
                "streaming component leaves must have ordered disjoint ranges and one identity"
                    .into(),
            ));
        }
        Ok(())
    }

    pub async fn finish(mut self) -> Result<PublishedStream, IndexError> {
        if self.leaf_count == 0 {
            return Err(IndexError::InvalidDefinition(
                "component stream requires at least one leaf".into(),
            ));
        }
        loop {
            let occupied = self
                .levels
                .iter()
                .enumerate()
                .filter(|(_, nodes)| !nodes.is_empty())
                .collect::<Vec<_>>();
            if occupied.len() == 1 && occupied[0].1.len() == 1 && occupied[0].0 > 0 {
                let root_level = occupied[0].0;
                let root = self.levels[root_level]
                    .pop()
                    .expect("one routing root")
                    .descriptor;
                return Ok(PublishedStream {
                    root,
                    minimum_key: self
                        .first_minimum_key
                        .expect("nonempty stream has a minimum key"),
                    maximum_key: self
                        .previous_maximum_key
                        .expect("nonempty stream has a maximum key"),
                    element_count: self.element_count,
                    routing_height: u8::try_from(root_level)
                        .map_err(|_| IndexError::OffsetOverflow)?,
                    encoded_bytes: self.encoded_bytes,
                    logical_bytes: self.logical_bytes,
                    leaf_count: self.leaf_count,
                    component_count: self.component_count,
                });
            }
            let level = self
                .levels
                .iter()
                .position(|nodes| !nodes.is_empty())
                .expect("a nonempty stream has an occupied level");
            self.publish_routing_groups(level, true).await?;
            self.cascade_full_levels(level + 1).await?;
        }
    }

    async fn cascade_full_levels(&mut self, mut level: usize) -> Result<(), IndexError> {
        let buffered_children = routing_fanout(self.logical_kind)
            .checked_mul(ROUTING_COMPONENTS_PER_BATCH)
            .ok_or(IndexError::OffsetOverflow)?;
        while self
            .levels
            .get(level)
            .is_some_and(|nodes| nodes.len() >= buffered_children)
        {
            self.publish_routing_groups(level, false).await?;
            level += 1;
        }
        Ok(())
    }

    async fn publish_routing_groups(
        &mut self,
        level: usize,
        include_partial_group: bool,
    ) -> Result<(), IndexError> {
        let height = u8::try_from(level + 1).map_err(|_| IndexError::OffsetOverflow)?;
        if height > INDEX_ROUTING_HEIGHT {
            return Err(IndexError::ResourceLimit {
                needed: height as usize,
                limit: INDEX_ROUTING_HEIGHT as usize,
            });
        }
        let mut children = std::mem::take(
            self.levels
                .get_mut(level)
                .ok_or(IndexError::InvalidFormat("streaming routing level"))?,
        );
        if children.is_empty() {
            return Err(IndexError::InvalidFormat(
                "streaming routing fanout is invalid",
            ));
        }
        let fanout = routing_fanout(self.logical_kind);
        let complete_groups = children.len() / fanout;
        let groups = if include_partial_group {
            children.len().div_ceil(fanout)
        } else {
            complete_groups - (complete_groups % ROUTING_COMPONENTS_PER_BATCH)
        };
        if groups == 0 {
            self.levels[level] = children;
            return Ok(());
        }
        let consumed = if include_partial_group {
            children.len()
        } else {
            groups
                .checked_mul(fanout)
                .ok_or(IndexError::OffsetOverflow)?
        };
        let remainder = children.split_off(consumed);
        let mut children = children.into_iter();
        for _ in 0..groups {
            let group = children.by_ref().take(fanout).collect::<Vec<_>>();
            if group.is_empty() || (!include_partial_group && group.len() != fanout) {
                return Err(IndexError::InvalidFormat(
                    "streaming routing fanout is invalid",
                ));
            }
            let minimum_key = group.first().unwrap().minimum_key.clone();
            let maximum_key = group.last().unwrap().maximum_key.clone();
            let element_count = group.iter().try_fold(0u64, |sum, child| {
                sum.checked_add(child.element_count)
                    .ok_or(IndexError::OffsetOverflow)
            })?;
            let entries = group
                .into_iter()
                .map(|child| RoutingEntry {
                    minimum_key: child.minimum_key,
                    maximum_key: child.maximum_key,
                    element_count: child.element_count,
                    child: child.descriptor,
                })
                .collect();
            let routing = RoutingNode::new_for_kind(
                self.identity.index_id,
                height,
                self.logical_kind,
                entries,
            )?;
            let payload = routing.encode_payload()?;
            let component = encode_component(
                self.identity,
                ComponentKind::ROUTING_NODE,
                self.routing_codec_version,
                0,
                u64::try_from(payload.len()).map_err(|_| IndexError::OffsetOverflow)?,
                payload,
            )?;
            let encoded_length = component.bytes().len();
            let header = component.header();
            let descriptor = self.sink.stage_component(component).await?;
            validate_placed_component(&descriptor, header, encoded_length)?;
            if self.levels.len() <= level + 1 {
                self.levels.resize_with(level + 2, Vec::new);
            }
            self.record_descriptor(&descriptor)?;
            self.levels[level + 1].push(PublishedNode {
                minimum_key,
                maximum_key,
                element_count,
                descriptor,
            });
        }
        self.levels[level] = remainder;
        Ok(())
    }

    fn record_descriptor(&mut self, descriptor: &ArtifactDescriptor) -> Result<(), IndexError> {
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(descriptor.encoded_length)
            .ok_or(IndexError::OffsetOverflow)?;
        self.logical_bytes = self
            .logical_bytes
            .checked_add(descriptor.logical_length)
            .ok_or(IndexError::OffsetOverflow)?;
        self.component_count = self
            .component_count
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(())
    }
}

fn validate_placed_component(
    descriptor: &ArtifactDescriptor,
    header: super::super::ComponentHeader,
    encoded: usize,
) -> Result<(), IndexError> {
    if descriptor.component_kind != header.component_kind
        || descriptor.codec_version != header.codec_version
        || descriptor.logical_length != header.logical_length
        || descriptor.encoded_length
            != u64::try_from(encoded).map_err(|_| IndexError::OffsetOverflow)?
        || descriptor.checksum != header.payload_checksum
    {
        return Err(IndexError::InvalidFormat(
            "component sink changed component identity",
        ));
    }
    Ok(())
}

/// Publish data leaves and then each required parent routing layer. No parent
/// can contain a provisional location because a layer is complete before the
/// next one is encoded.
pub async fn publish_stream<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    routing_codec_version: u16,
    leaves: Vec<ComponentLeaf>,
) -> Result<PublishedStream, IndexError> {
    let mut leaves = leaves.into_iter();
    let first = leaves.next().ok_or_else(|| {
        IndexError::InvalidDefinition("component stream requires leaves and a routing codec".into())
    })?;
    if routing_codec_version == 0 {
        return Err(IndexError::InvalidDefinition(
            "component stream requires leaves and a routing codec".into(),
        ));
    }
    let header = first.component.header();
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        header.component_kind,
        header.codec_version,
        routing_codec_version,
    )?;
    publisher.push_leaf(first).await?;
    for leaf in leaves {
        publisher.push_leaf(leaf).await?;
    }
    publisher.finish().await
}

/// Publish a new routing tree over ordered existing or newly published data
/// leaves. Only routing parents are written. Returned totals include every
/// referenced data leaf as well as the new routing nodes, so callers can use
/// them directly in segment and generation accounting.
pub async fn publish_descriptor_stream<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    logical_kind: ComponentKind,
    routing_codec_version: u16,
    leaves: Vec<DescriptorLeaf>,
) -> Result<PublishedStream, IndexError> {
    let leaf_codec_version = leaves
        .first()
        .map(|leaf| leaf.descriptor.codec_version)
        .ok_or_else(|| {
            IndexError::InvalidDefinition(
                "descriptor stream requires data leaves and a routing codec".into(),
            )
        })?;
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        logical_kind,
        leaf_codec_version,
        routing_codec_version,
    )?;
    for leaf in leaves {
        publisher.push_descriptor_leaf(leaf).await?;
    }
    publisher.finish().await
}

/// Assemble independently published, non-overlapping range subtrees into one
/// routed stream. Lane completion order is irrelevant: the coordinator sorts
/// and validates exact key ranges, promotes shorter roots, and writes only the
/// bounded routing layers required to join them.
pub async fn combine_published_streams<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    logical_kind: ComponentKind,
    routing_codec_version: u16,
    mut streams: Vec<PublishedStream>,
) -> Result<PublishedStream, IndexError> {
    identity.validate()?;
    if streams.is_empty()
        || logical_kind == ComponentKind::ROUTING_NODE
        || routing_codec_version == 0
    {
        return Err(IndexError::InvalidDefinition(
            "range subtree assembly requires at least one logical stream".into(),
        ));
    }
    streams.sort_by(|left, right| left.minimum_key.cmp(&right.minimum_key));
    let mut previous = None::<Vec<u8>>;
    for stream in &streams {
        validate_stream_routing_key(logical_kind, &stream.minimum_key)?;
        validate_stream_routing_key(logical_kind, &stream.maximum_key)?;
        stream.root.validate(identity.index_id)?;
        if stream.minimum_key > stream.maximum_key
            || stream.element_count == 0
            || stream.routing_height == 0
            || stream.routing_height > INDEX_ROUTING_HEIGHT
            || stream.root.component_kind != ComponentKind::ROUTING_NODE
            || stream.root.codec_version != routing_codec_version
            || previous
                .as_ref()
                .is_some_and(|value| value.as_slice() >= stream.minimum_key.as_slice())
        {
            return Err(IndexError::InvalidFormat(
                "range subtrees are not ordered, disjoint routing trees",
            ));
        }
        previous = Some(stream.maximum_key.clone());
    }
    if streams.len() == 1 {
        return Ok(streams.pop().expect("one checked subtree"));
    }
    let target_height = streams
        .iter()
        .map(|stream| stream.routing_height)
        .max()
        .expect("nonempty checked subtrees");
    let mut promoted = Vec::with_capacity(streams.len());
    for stream in streams {
        promoted.push(
            promote_stream(
                sink,
                identity,
                logical_kind,
                routing_codec_version,
                stream,
                target_height,
            )
            .await?,
        );
    }
    let minimum_key = promoted.first().unwrap().minimum_key.clone();
    let maximum_key = promoted.last().unwrap().maximum_key.clone();
    let element_count = promoted.iter().try_fold(0u64, |sum, stream| {
        sum.checked_add(stream.element_count)
            .ok_or(IndexError::OffsetOverflow)
    })?;
    let mut encoded_bytes = promoted.iter().try_fold(0u64, |sum, stream| {
        sum.checked_add(stream.encoded_bytes)
            .ok_or(IndexError::OffsetOverflow)
    })?;
    let mut logical_bytes = promoted.iter().try_fold(0u64, |sum, stream| {
        sum.checked_add(stream.logical_bytes)
            .ok_or(IndexError::OffsetOverflow)
    })?;
    let mut component_count = promoted.iter().try_fold(0u64, |sum, stream| {
        sum.checked_add(stream.component_count)
            .ok_or(IndexError::OffsetOverflow)
    })?;
    let leaf_count = promoted.iter().try_fold(0u64, |sum, stream| {
        sum.checked_add(stream.leaf_count)
            .ok_or(IndexError::OffsetOverflow)
    })?;
    let mut nodes = promoted
        .into_iter()
        .map(|stream| PublishedNode {
            minimum_key: stream.minimum_key,
            maximum_key: stream.maximum_key,
            element_count: stream.element_count,
            descriptor: stream.root,
        })
        .collect::<Vec<_>>();
    let mut height = target_height;
    while nodes.len() != 1 {
        height = height.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        if height > INDEX_ROUTING_HEIGHT {
            return Err(IndexError::ResourceLimit {
                needed: height as usize,
                limit: INDEX_ROUTING_HEIGHT as usize,
            });
        }
        let fanout = routing_fanout(logical_kind);
        let mut parents = Vec::with_capacity(nodes.len().div_ceil(fanout));
        for children in nodes.chunks(fanout) {
            let node = RoutingNode::new_for_kind(
                identity.index_id,
                height,
                logical_kind,
                children
                    .iter()
                    .map(|child| RoutingEntry {
                        minimum_key: child.minimum_key.clone(),
                        maximum_key: child.maximum_key.clone(),
                        element_count: child.element_count,
                        child: child.descriptor.clone(),
                    })
                    .collect(),
            )?;
            let payload = node.encode_payload()?;
            let component = encode_component(
                identity,
                ComponentKind::ROUTING_NODE,
                routing_codec_version,
                0,
                payload.len() as u64,
                payload,
            )?;
            let minimum_key = children.first().unwrap().minimum_key.clone();
            let maximum_key = children.last().unwrap().maximum_key.clone();
            let element_count = children.iter().try_fold(0u64, |sum, child| {
                sum.checked_add(child.element_count)
                    .ok_or(IndexError::OffsetOverflow)
            })?;
            let header = component.header();
            let encoded = component.bytes().len();
            let descriptor = publish_single_component(sink, component).await?;
            validate_placed_component(&descriptor, header, encoded)?;
            encoded_bytes = encoded_bytes
                .checked_add(descriptor.encoded_length)
                .ok_or(IndexError::OffsetOverflow)?;
            logical_bytes = logical_bytes
                .checked_add(descriptor.logical_length)
                .ok_or(IndexError::OffsetOverflow)?;
            component_count = component_count
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
            parents.push(PublishedNode {
                minimum_key,
                maximum_key,
                element_count,
                descriptor,
            });
        }
        nodes = parents;
    }
    Ok(PublishedStream {
        root: nodes.pop().expect("one assembled routing root").descriptor,
        minimum_key,
        maximum_key,
        element_count,
        routing_height: height,
        encoded_bytes,
        logical_bytes,
        leaf_count,
        component_count,
    })
}

async fn promote_stream<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    logical_kind: ComponentKind,
    routing_codec_version: u16,
    mut stream: PublishedStream,
    target_height: u8,
) -> Result<PublishedStream, IndexError> {
    while stream.routing_height < target_height {
        let height = stream
            .routing_height
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        let node = RoutingNode::new_for_kind(
            identity.index_id,
            height,
            logical_kind,
            vec![RoutingEntry {
                minimum_key: stream.minimum_key.clone(),
                maximum_key: stream.maximum_key.clone(),
                element_count: stream.element_count,
                child: stream.root,
            }],
        )?;
        let payload = node.encode_payload()?;
        let component = encode_component(
            identity,
            ComponentKind::ROUTING_NODE,
            routing_codec_version,
            0,
            payload.len() as u64,
            payload,
        )?;
        let header = component.header();
        let encoded = component.bytes().len();
        let descriptor = publish_single_component(sink, component).await?;
        validate_placed_component(&descriptor, header, encoded)?;
        stream.encoded_bytes = stream
            .encoded_bytes
            .checked_add(descriptor.encoded_length)
            .ok_or(IndexError::OffsetOverflow)?;
        stream.logical_bytes = stream
            .logical_bytes
            .checked_add(descriptor.logical_length)
            .ok_or(IndexError::OffsetOverflow)?;
        stream.component_count = stream
            .component_count
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        stream.root = descriptor;
        stream.routing_height = height;
    }
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn completed_components_share_one_pack_and_preserve_ranges() {
        let identity = SegmentIdentity::new(1, 2, [3; 32], 4).unwrap();
        let first =
            encode_component(identity, ComponentKind::POSTINGS, 1, 0, 3, vec![1, 2, 3]).unwrap();
        let first_length = first.bytes().len() as u64;
        let second =
            encode_component(identity, ComponentKind::POSTINGS, 1, 0, 2, vec![4, 5]).unwrap();
        let mut sink = ExactMemorySink::new();
        sink.begin_segment(identity, &[]).unwrap();
        let first = sink.stage_component(first).await.unwrap();
        let second = sink.stage_component(second).await.unwrap();
        let packs = sink.finalize_segment(identity).await.unwrap();
        assert_eq!(first.offset, 0);
        assert_eq!(second.offset, first_length);
        assert_eq!(first.pack_ordinal, second.pack_ordinal);
        assert_eq!(packs.len(), 1);
        assert!(sink.component_bytes(&packs, &first).is_ok());
        assert!(sink.component_bytes(&packs, &second).is_ok());
    }

    #[tokio::test]
    async fn complete_components_never_straddle_pack_boundaries() {
        let identity = SegmentIdentity::new(1, 2, [3; 32], 4).unwrap();
        let payload_bytes = super::super::super::INDEX_COMPONENT_BYTES - COMPONENT_HEADER_BYTES;
        let mut sink = ExactMemorySink::new();
        sink.begin_segment(identity, &[]).unwrap();
        let mut descriptors = Vec::new();
        for value in 0..33_u8 {
            let component = encode_component(
                identity,
                ComponentKind::POSTINGS,
                1,
                0,
                payload_bytes as u64,
                vec![value; payload_bytes],
            )
            .unwrap();
            descriptors.push(sink.stage_component(component).await.unwrap());
        }
        let packs = sink.finalize_segment(identity).await.unwrap();
        assert_eq!(packs.len(), 2);
        assert_eq!(descriptors[31].pack_ordinal, 0);
        assert_eq!(descriptors[32].pack_ordinal, 1);
        assert_eq!(descriptors[32].offset, 0);
        for descriptor in &descriptors {
            descriptor.pack(identity.index_id, &packs).unwrap();
        }
    }

    #[tokio::test]
    async fn child_layers_are_exact_before_parent_publication() {
        let identity = SegmentIdentity::new(1, 2, [3; 32], 4).unwrap();
        let mut leaves = Vec::new();
        for value in 0..33u32 {
            let payload = value.to_le_bytes().to_vec();
            leaves.push(ComponentLeaf {
                minimum_key: value.to_be_bytes().to_vec(),
                maximum_key: value.to_be_bytes().to_vec(),
                element_count: 1,
                component: encode_component(
                    identity,
                    ComponentKind::POSTINGS,
                    1,
                    0,
                    payload.len() as u64,
                    payload,
                )
                .unwrap(),
            });
        }
        let mut sink = ExactMemorySink::new();
        sink.begin_segment(identity, &[]).unwrap();
        let stream = publish_stream(&mut sink, identity, 1, leaves)
            .await
            .unwrap();
        let packs = sink.finalize_segment(identity).await.unwrap();
        assert_eq!(sink.publish_calls(), 1);
        assert_eq!(sink.objects().len(), 1);
        assert_eq!(stream.component_count, 36); // 33 leaves, 2 parents, 1 root.
        assert_eq!(stream.root.component_kind, ComponentKind::ROUTING_NODE);
        assert!(sink.component_bytes(&packs, &stream.root).is_ok());
    }

    #[tokio::test]
    async fn descriptor_rewrite_reuses_data_and_counts_complete_stream() {
        let identity = SegmentIdentity::new(1, 2, [3; 32], 4).unwrap();
        let generated = (0..2u32)
            .map(|value| {
                encode_component(
                    identity,
                    ComponentKind::LIVE_MASK,
                    1,
                    0,
                    4,
                    value.to_le_bytes().to_vec(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let mut sink = ExactMemorySink::new();
        sink.begin_segment(identity, &[]).unwrap();
        let mut descriptors = Vec::new();
        for component in generated {
            descriptors.push(sink.stage_component(component).await.unwrap());
        }
        let leaves = descriptors
            .iter()
            .enumerate()
            .map(|(offset, descriptor)| DescriptorLeaf {
                minimum_key: (offset as u32).to_be_bytes().to_vec(),
                maximum_key: (offset as u32).to_be_bytes().to_vec(),
                element_count: 1,
                descriptor: descriptor.clone(),
            })
            .collect();
        let stream =
            publish_descriptor_stream(&mut sink, identity, ComponentKind::LIVE_MASK, 1, leaves)
                .await
                .unwrap();
        let packs = sink.finalize_segment(identity).await.unwrap();
        assert_eq!(stream.component_count, 3);
        assert_eq!(
            stream.encoded_bytes,
            descriptors
                .iter()
                .map(|value| value.encoded_length)
                .sum::<u64>()
                + stream.root.encoded_length
        );
        assert_eq!(sink.objects().len(), 1);
        assert!(sink.component_bytes(&packs, &stream.root).is_ok());
    }

    #[tokio::test]
    async fn streaming_small_leaves_share_bounded_ordinary_packs() {
        let identity = SegmentIdentity::new(1, 2, [3; 32], 4).unwrap();
        let mut sink = ExactMemorySink::new();
        sink.begin_segment(identity, &[]).unwrap();
        let mut publisher =
            StreamingComponentPublisher::new(&mut sink, identity, ComponentKind::POSTINGS, 1, 1)
                .unwrap();
        for value in 0..40_u32 {
            publisher
                .push_payload(
                    value.to_be_bytes().to_vec(),
                    value.to_be_bytes().to_vec(),
                    1,
                    value.to_le_bytes().to_vec(),
                )
                .await
                .unwrap();
        }
        let stream = publisher.finish().await.unwrap();
        let packs = sink.finalize_segment(identity).await.unwrap();

        assert_eq!(stream.component_count, 43); // 40 leaves, 2 parents, 1 root.
        assert_eq!(sink.publish_calls(), 1);
        assert_eq!(sink.objects().len(), 1);
        assert!(sink.component_bytes(&packs, &stream.root).is_ok());
    }

    #[tokio::test]
    async fn routing_layers_share_byte_bounded_ordinary_packs() {
        let identity = SegmentIdentity::new(1, 2, [3; 32], 4).unwrap();
        let mut sink = ExactMemorySink::new();
        sink.begin_segment(identity, &[]).unwrap();
        let mut publisher =
            StreamingComponentPublisher::new(&mut sink, identity, ComponentKind::POSTINGS, 1, 1)
                .unwrap();
        for value in 0..1_024_u32 {
            publisher
                .push_payload(
                    value.to_be_bytes().to_vec(),
                    value.to_be_bytes().to_vec(),
                    1,
                    value.to_le_bytes().to_vec(),
                )
                .await
                .unwrap();
        }
        let stream = publisher.finish().await.unwrap();
        let packs = sink.finalize_segment(identity).await.unwrap();

        assert_eq!(stream.component_count, 1_057); // 1,024 leaves, 32 parents, 1 root.
        assert_eq!(sink.publish_calls(), 1);
        assert_eq!(sink.objects().len(), 1);
        assert!(sink.component_bytes(&packs, &stream.root).is_ok());
    }

    #[test]
    fn tiny_components_are_bounded_by_pack_bytes_not_worst_case_count() {
        let identity = SegmentIdentity::new(1, 2, [3; 32], 4).unwrap();
        let mut builder = ComponentPackBuilder::new();
        for value in 0..1_024_u32 {
            builder
                .push(
                    encode_component(
                        identity,
                        ComponentKind::POSTINGS,
                        1,
                        0,
                        4,
                        value.to_le_bytes().to_vec(),
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let pack = builder.finish().unwrap();
        assert_eq!(pack.component_count().unwrap(), 1_024);
        assert!(pack.bytes().len() < INDEX_ARTIFACT_PACK_BYTES);
    }

    #[tokio::test]
    async fn maximum_keyword_boundaries_use_bounded_term_fanout() {
        let identity = SegmentIdentity::new(1, 2, [3; 32], 4).unwrap();
        let mut sink = ExactMemorySink::new();
        sink.begin_segment(identity, &[]).unwrap();
        let mut publisher = StreamingComponentPublisher::new(
            &mut sink,
            identity,
            ComponentKind::TERM_DICTIONARY,
            1,
            1,
        )
        .unwrap();
        for discriminator in b'a'..=b'h' {
            let mut key = vec![0, 0, 0, 1, super::super::super::TERM_TYPE_STRING, 0];
            key.push(discriminator);
            key.extend(vec![b'x'; super::super::super::INDEX_TERM_BYTES - 1]);
            publisher
                .push_payload(key.clone(), key, 1, vec![discriminator])
                .await
                .unwrap();
        }
        let stream = publisher.finish().await.unwrap();
        let packs = sink.finalize_segment(identity).await.unwrap();
        assert_eq!(stream.routing_height, 2);
        assert_eq!(stream.leaf_count, 8);
        assert_eq!(stream.component_count, 11); // Eight leaves, two parents, one root.
        let root = sink.component_bytes(&packs, &stream.root).unwrap();
        assert!(root.len() <= super::super::super::INDEX_COMPONENT_BYTES);
    }

    #[tokio::test]
    async fn range_subtree_assembly_is_completion_order_independent() {
        let identity = SegmentIdentity::new(1, 2, [3; 32], 4).unwrap();
        let mut sink = ExactMemorySink::new();
        sink.begin_segment(identity, &[]).unwrap();
        let mut left =
            StreamingComponentPublisher::new(&mut sink, identity, ComponentKind::POSTINGS, 1, 1)
                .unwrap();
        left.push_payload(vec![0], vec![0], 1, vec![10])
            .await
            .unwrap();
        let left = left.finish().await.unwrap();
        let mut right =
            StreamingComponentPublisher::new(&mut sink, identity, ComponentKind::POSTINGS, 1, 1)
                .unwrap();
        right
            .push_payload(vec![1], vec![1], 1, vec![11])
            .await
            .unwrap();
        let right = right.finish().await.unwrap();

        let reversed = combine_published_streams(
            &mut sink,
            identity,
            ComponentKind::POSTINGS,
            1,
            vec![right.clone(), left.clone()],
        )
        .await
        .unwrap();
        let ordered = combine_published_streams(
            &mut sink,
            identity,
            ComponentKind::POSTINGS,
            1,
            vec![left, right],
        )
        .await
        .unwrap();
        let packs = sink.finalize_segment(identity).await.unwrap();
        assert_eq!(packs.len(), 1, "all logical streams share the segment pack");
        assert_eq!(reversed.root.checksum, ordered.root.checksum);
        assert_eq!(reversed.root.encoded_length, ordered.root.encoded_length);
        assert_eq!(reversed.root.logical_length, ordered.root.logical_length);
        assert_eq!(
            sink.component_bytes(&packs, &reversed.root).unwrap(),
            sink.component_bytes(&packs, &ordered.root).unwrap(),
        );
        assert_eq!(reversed.minimum_key, vec![0]);
        assert_eq!(reversed.maximum_key, vec![1]);
        assert_eq!(reversed.element_count, 2);
    }
}
