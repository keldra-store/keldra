use super::buffer::{ComponentDeltaCursor, seal_component};
#[cfg(test)]
use super::pack_component_deltas;
use super::{
    ComponentIdentity, ComponentRoot, PackedComponentDelta, RecipeIdentity, SealedComponentDelta,
    StableDocumentKey, decode_component_delta_segment,
};
use crate::IndexError;
use std::collections::{BTreeMap, BTreeSet};
const PAGE_MAGIC: &[u8; 8] = b"K6CSTR01";
const PAGE_FORMAT: u16 = 2;
const LEAF_PAGE: u8 = 1;
const BRANCH_PAGE: u8 = 2;
pub const COMPONENT_STREAM_DIRECTORY_FANOUT: usize = 128;
const MAX_COMPONENT_STREAM_SEGMENTS: usize = 1_000_000;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentSegmentDescriptor {
    pub sequence: u64,
    pub level: u8,
    pub minimum_key: StableDocumentKey,
    pub maximum_key: StableDocumentKey,
    pub source_start_offset: u64,
    pub next_offset: u64,
    pub through_atomic_position: u64,
    pub pack_hash: [u8; 32],
    pub pack_offset: u64,
    pub segment_hash: [u8; 32],
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub records: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ComponentRecordLookup {
    Missing,
    Tombstone,
    Value(Vec<u8>),
}
impl ComponentSegmentDescriptor {
    fn validate(&self) -> Result<(), IndexError> {
        if self.sequence == 0
            || self.level > 63
            || self.minimum_key > self.maximum_key
            || self.source_start_offset >= self.next_offset
            || self.pack_hash == [0; 32]
            || self.segment_hash == [0; 32]
            || self.encoded_bytes == 0
            || self.logical_bytes == 0
            || self.records == 0
        {
            return Err(IndexError::InvalidDefinition(
                "component stream segment descriptor is invalid".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedComponentStreamPage {
    pub hash: [u8; 32],
    pub bytes: Vec<u8>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentStreamDirectory {
    pub component: ComponentIdentity,
    pub root_hash: [u8; 32],
    pub segment_count: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub directory_bytes: u64,
    pub pages: Vec<EncodedComponentStreamPage>,
}
impl ComponentStreamDirectory {
    pub fn component_root(&self) -> Result<ComponentRoot, IndexError> {
        decode_component_stream(self)?;
        self.root().component_root()
    }

    pub fn root(&self) -> ComponentStreamRoot {
        ComponentStreamRoot {
            component: self.component,
            root_hash: self.root_hash,
            segment_count: self.segment_count,
            first_sequence: self.first_sequence,
            last_sequence: self.last_sequence,
            encoded_bytes: self.encoded_bytes,
            logical_bytes: self.logical_bytes,
            directory_bytes: self.directory_bytes,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ComponentStreamRoot {
    pub component: ComponentIdentity,
    pub root_hash: [u8; 32],
    pub segment_count: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub directory_bytes: u64,
}

impl ComponentStreamRoot {
    pub fn from_component_root(root: &ComponentRoot) -> Result<Self, IndexError> {
        root.validate()?;
        Ok(Self {
            component: root.component,
            root_hash: root.stream_root_hash,
            segment_count: root.segment_count,
            first_sequence: root.first_sequence,
            last_sequence: root.last_sequence,
            encoded_bytes: root
                .encoded_bytes
                .checked_sub(root.directory_bytes)
                .ok_or(IndexError::OffsetOverflow)?,
            logical_bytes: root.logical_bytes,
            directory_bytes: root.directory_bytes,
        })
    }

    pub fn component_root(self) -> Result<ComponentRoot, IndexError> {
        validate_root(self)?;
        ComponentRoot::with_sequences(
            self.component,
            self.root_hash,
            self.segment_count,
            self.first_sequence,
            self.last_sequence,
            self.encoded_bytes
                .checked_add(self.directory_bytes)
                .ok_or(IndexError::OffsetOverflow)?,
            self.logical_bytes,
            self.directory_bytes,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentStreamAppend {
    pub root: ComponentStreamRoot,
    pub new_pages: Vec<EncodedComponentStreamPage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ComponentStreamReverseStep {
    LoadPage { hash: [u8; 32] },
    Segment(ComponentSegmentDescriptor),
    Complete,
}

#[derive(Debug)]
pub struct ComponentStreamReverseCursor {
    component: ComponentIdentity,
    pending_pages: Vec<Child>,
    awaiting_page: Option<Child>,
    leaf_segments: Vec<ComponentSegmentDescriptor>,
}

impl ComponentStreamReverseCursor {
    pub fn new(root: ComponentStreamRoot) -> Result<Self, IndexError> {
        validate_root(root)?;
        Ok(Self {
            component: root.component,
            pending_pages: vec![Child {
                first_sequence: root.first_sequence,
                last_sequence: root.last_sequence,
                hash: root.root_hash,
                segment_count: root.segment_count,
                encoded_bytes: root.encoded_bytes,
                logical_bytes: root.logical_bytes,
                directory_bytes: root.directory_bytes,
                minimum_key: StableDocumentKey::from_bytes([1; 32])?,
                maximum_key: StableDocumentKey::from_bytes([u8::MAX; 32])?,
                minimum_source_offset: 0,
                maximum_next_offset: u64::MAX,
                through_atomic_position: u64::MAX,
                summary_exact: false,
            }],
            awaiting_page: None,
            leaf_segments: Vec::new(),
        })
    }

    pub fn next(&mut self) -> Result<ComponentStreamReverseStep, IndexError> {
        if let Some(descriptor) = self.leaf_segments.pop() {
            return Ok(ComponentStreamReverseStep::Segment(descriptor));
        }
        if let Some(expected) = &self.awaiting_page {
            return Ok(ComponentStreamReverseStep::LoadPage {
                hash: expected.hash,
            });
        }
        let Some(expected) = self.pending_pages.pop() else {
            return Ok(ComponentStreamReverseStep::Complete);
        };
        let hash = expected.hash;
        self.awaiting_page = Some(expected);
        Ok(ComponentStreamReverseStep::LoadPage { hash })
    }

    pub fn provide_page(&mut self, hash: [u8; 32], bytes: &[u8]) -> Result<(), IndexError> {
        let expected = self.awaiting_page.take().ok_or_else(|| {
            IndexError::InvalidDefinition("component cursor did not request a page".into())
        })?;
        if hash != expected.hash || hash != *blake3::hash(bytes).as_bytes() {
            return Err(IndexError::Integrity);
        }
        match decode_page(self.component, bytes)? {
            Page::Leaf(segments) => {
                let actual = Child::from_segments(&segments, hash, bytes.len() as u64)?;
                if !child_matches(&actual, &expected) {
                    return Err(IndexError::Integrity);
                }
                self.leaf_segments = segments;
            }
            Page::Branch(children) => {
                let actual = Child::from_children(&children, hash, bytes.len() as u64)?;
                if !child_matches(&actual, &expected) {
                    return Err(IndexError::Integrity);
                }
                self.pending_pages.extend(children);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Page {
    Leaf(Vec<ComponentSegmentDescriptor>),
    Branch(Vec<Child>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Child {
    first_sequence: u64,
    last_sequence: u64,
    hash: [u8; 32],
    segment_count: u64,
    encoded_bytes: u64,
    logical_bytes: u64,
    directory_bytes: u64,
    minimum_key: StableDocumentKey,
    maximum_key: StableDocumentKey,
    minimum_source_offset: u64,
    maximum_next_offset: u64,
    through_atomic_position: u64,
    summary_exact: bool,
}

fn child_matches(actual: &Child, expected: &Child) -> bool {
    actual.first_sequence == expected.first_sequence
        && actual.last_sequence == expected.last_sequence
        && actual.hash == expected.hash
        && actual.segment_count == expected.segment_count
        && actual.encoded_bytes == expected.encoded_bytes
        && actual.logical_bytes == expected.logical_bytes
        && actual.directory_bytes == expected.directory_bytes
        && (!expected.summary_exact
            || (actual.minimum_key == expected.minimum_key
                && actual.maximum_key == expected.maximum_key
                && actual.minimum_source_offset == expected.minimum_source_offset
                && actual.maximum_next_offset == expected.maximum_next_offset
                && actual.through_atomic_position == expected.through_atomic_position))
}

pub fn build_component_stream(
    component: ComponentIdentity,
    segments: &[ComponentSegmentDescriptor],
) -> Result<ComponentStreamDirectory, IndexError> {
    validate_segments(segments)?;
    let mut pages = Vec::new();
    let mut level = Vec::new();
    for chunk in segments.chunks(COMPONENT_STREAM_DIRECTORY_FANOUT) {
        let encoded = encode_page(component, &Page::Leaf(chunk.to_vec()))?;
        level.push(Child::from_segments(
            chunk,
            encoded.hash,
            encoded.bytes.len() as u64,
        )?);
        pages.push(encoded);
    }
    while level.len() > 1 {
        let mut parent = Vec::new();
        for chunk in level.chunks(COMPONENT_STREAM_DIRECTORY_FANOUT) {
            let encoded = encode_page(component, &Page::Branch(chunk.to_vec()))?;
            parent.push(Child::from_children(
                chunk,
                encoded.hash,
                encoded.bytes.len() as u64,
            )?);
            pages.push(encoded);
        }
        level = parent;
    }
    let root = level
        .into_iter()
        .next()
        .expect("validated component stream has one root");
    Ok(ComponentStreamDirectory {
        component,
        root_hash: root.hash,
        segment_count: root.segment_count,
        first_sequence: root.first_sequence,
        last_sequence: root.last_sequence,
        encoded_bytes: root.encoded_bytes,
        logical_bytes: root.logical_bytes,
        directory_bytes: root.directory_bytes,
        pages,
    })
}

pub fn append_component_stream(
    previous: Option<ComponentStreamRoot>,
    mut load_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    delta: &PackedComponentDelta,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
) -> Result<ComponentStreamAppend, IndexError> {
    let next_sequence = match previous {
        Some(root) => {
            validate_root(root)?;
            if root.component != delta.component {
                return Err(IndexError::InvalidDefinition(
                    "component delta was appended to a different stream".into(),
                ));
            }
            root.last_sequence
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?
        }
        None => 1,
    };
    if previous.is_some_and(|root| root.segment_count >= MAX_COMPONENT_STREAM_SEGMENTS as u64) {
        return Err(IndexError::ResourceLimit {
            needed: MAX_COMPONENT_STREAM_SEGMENTS + 1,
            limit: MAX_COMPONENT_STREAM_SEGMENTS,
        });
    }
    let descriptor = descriptor(
        next_sequence,
        0,
        source_start_offset,
        next_offset,
        through_atomic_position,
        delta,
    )?;
    let mut new_pages = Vec::new();
    let children = match previous {
        Some(root) => append_subtree(
            root.component,
            root.root_hash,
            &descriptor,
            &mut load_page,
            &mut new_pages,
        )?,
        None => {
            let encoded = encode_page(delta.component, &Page::Leaf(vec![descriptor]))?;
            let child = Child::from_segments(
                match decode_page(delta.component, &encoded.bytes)? {
                    Page::Leaf(ref segments) => segments,
                    Page::Branch(_) => unreachable!(),
                },
                encoded.hash,
                encoded.bytes.len() as u64,
            )?;
            new_pages.push(encoded);
            vec![child]
        }
    };
    let root_child = if children.len() == 1 {
        children[0].clone()
    } else {
        let encoded = encode_page(delta.component, &Page::Branch(children.clone()))?;
        let child = Child::from_children(&children, encoded.hash, encoded.bytes.len() as u64)?;
        new_pages.push(encoded);
        child
    };
    if root_child.last_sequence != next_sequence
        || root_child.segment_count
            != previous.map_or(1, |root| root.segment_count.saturating_add(1))
    {
        return Err(IndexError::Integrity);
    }
    Ok(ComponentStreamAppend {
        root: ComponentStreamRoot {
            component: delta.component,
            root_hash: root_child.hash,
            segment_count: root_child.segment_count,
            first_sequence: root_child.first_sequence,
            last_sequence: root_child.last_sequence,
            encoded_bytes: root_child.encoded_bytes,
            logical_bytes: root_child.logical_bytes,
            directory_bytes: root_child.directory_bytes,
        },
        new_pages,
    })
}

pub fn append_component_delta(
    previous: Option<&ComponentStreamDirectory>,
    delta: &PackedComponentDelta,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
) -> Result<ComponentStreamDirectory, IndexError> {
    let mut segments = match previous {
        Some(directory) => {
            if directory.component != delta.component {
                return Err(IndexError::InvalidDefinition(
                    "component delta was appended to a different stream".into(),
                ));
            }
            decode_component_stream(directory)?
        }
        None => Vec::new(),
    };
    let sequence = segments.last().map_or(Ok(1), |segment| {
        segment
            .sequence
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)
    })?;
    segments.push(descriptor(
        sequence,
        0,
        source_start_offset,
        next_offset,
        through_atomic_position,
        delta,
    )?);
    build_component_stream(delta.component, &segments)
}

pub fn decode_component_stream(
    directory: &ComponentStreamDirectory,
) -> Result<Vec<ComponentSegmentDescriptor>, IndexError> {
    if directory.root_hash == [0; 32]
        || directory.segment_count == 0
        || directory.segment_count > MAX_COMPONENT_STREAM_SEGMENTS as u64
        || directory.first_sequence == 0
        || directory.first_sequence > directory.last_sequence
        || directory.encoded_bytes == 0
        || directory.logical_bytes == 0
        || directory.directory_bytes == 0
    {
        return Err(IndexError::InvalidFormat(
            "component stream directory root is invalid",
        ));
    }
    let mut pages = BTreeMap::new();
    for page in &directory.pages {
        if page.hash == [0; 32]
            || page.hash != *blake3::hash(&page.bytes).as_bytes()
            || pages.insert(page.hash, page.bytes.as_slice()).is_some()
        {
            return Err(IndexError::Integrity);
        }
    }
    let mut segments = Vec::new();
    let mut visited = BTreeSet::new();
    let totals = decode_subtree(
        directory.component,
        directory.root_hash,
        &pages,
        &mut visited,
        &mut segments,
    )?;
    if totals.segment_count != directory.segment_count
        || totals.first_sequence != directory.first_sequence
        || totals.last_sequence != directory.last_sequence
        || totals.encoded_bytes != directory.encoded_bytes
        || totals.logical_bytes != directory.logical_bytes
        || totals.directory_bytes != directory.directory_bytes
        || visited.len() != pages.len()
    {
        return Err(IndexError::Integrity);
    }
    validate_segments(&segments)?;
    Ok(segments)
}

pub fn component_stream_child_hashes(
    component: ComponentIdentity,
    bytes: &[u8],
) -> Result<Vec<[u8; 32]>, IndexError> {
    Ok(match decode_page(component, bytes)? {
        Page::Leaf(_) => Vec::new(),
        Page::Branch(children) => children.into_iter().map(|child| child.hash).collect(),
    })
}

pub fn resolve_component_record(
    directory: &ComponentStreamDirectory,
    artifacts: &BTreeMap<[u8; 32], Vec<u8>>,
    stable_key: StableDocumentKey,
) -> Result<Option<Vec<u8>>, IndexError> {
    let segments = decode_component_stream(directory)?;
    for descriptor in segments.iter().rev().filter(|run| run.level == 0) {
        let pack = artifacts
            .get(&descriptor.pack_hash)
            .ok_or(IndexError::Integrity)?;
        match lookup_component_record_in_pack(directory.component, descriptor, pack, stable_key)? {
            ComponentRecordLookup::Missing => {}
            ComponentRecordLookup::Tombstone => return Ok(None),
            ComponentRecordLookup::Value(value) => return Ok(Some(value)),
        }
    }
    let levels = segments
        .iter()
        .filter(|run| run.level > 0)
        .map(|run| run.level)
        .collect::<BTreeSet<_>>();
    for level in levels {
        let Some(descriptor) = segments.iter().find(|run| {
            run.level == level && run.minimum_key <= stable_key && stable_key <= run.maximum_key
        }) else {
            continue;
        };
        let pack = artifacts
            .get(&descriptor.pack_hash)
            .ok_or(IndexError::Integrity)?;
        match lookup_component_record_in_pack(directory.component, descriptor, pack, stable_key)? {
            ComponentRecordLookup::Missing => {}
            ComponentRecordLookup::Tombstone => return Ok(None),
            ComponentRecordLookup::Value(value) => return Ok(Some(value)),
        }
    }
    Ok(None)
}

pub fn lookup_component_record_in_pack(
    component: ComponentIdentity,
    descriptor: &ComponentSegmentDescriptor,
    pack: &[u8],
    stable_key: StableDocumentKey,
) -> Result<ComponentRecordLookup, IndexError> {
    let bytes = validate_artifact(component, descriptor, pack)?;
    let decoded = decode_component_delta_segment(bytes)?;
    let Ok(index) = decoded
        .records
        .binary_search_by_key(&stable_key, |record| record.stable_key)
    else {
        return Ok(ComponentRecordLookup::Missing);
    };
    Ok(match decoded.records[index].replacement.clone() {
        Some(value) => ComponentRecordLookup::Value(value),
        None => ComponentRecordLookup::Tombstone,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ComponentCompactionLimits {
    pub l0_trigger: usize,
    pub maximum_input_runs: usize,
    pub maximum_loaded_pack_bytes: usize,
    pub maximum_output_run_bytes: usize,
}

impl ComponentCompactionLimits {
    pub fn validate(self) -> Result<Self, IndexError> {
        if self.l0_trigger < 2
            || self.maximum_input_runs < self.l0_trigger
            || self.maximum_input_runs > COMPONENT_STREAM_DIRECTORY_FANOUT
            || self.maximum_loaded_pack_bytes == 0
            || self.maximum_output_run_bytes < 1024
        {
            return Err(IndexError::InvalidDefinition(
                "projection compaction limits are invalid".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TombstoneCompactionPolicy {
    Retain,
    DropWhenOldestHistoryCovered,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentCompactionPlan {
    stream_root_hash: [u8; 32],
    component: ComponentIdentity,
    inputs: Vec<ComponentSegmentDescriptor>,
    target_level: u8,
    covers_oldest_history: bool,
    minimum_key: StableDocumentKey,
    maximum_key: StableDocumentKey,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
}

impl ComponentCompactionPlan {
    pub const fn component(&self) -> ComponentIdentity {
        self.component
    }

    pub const fn target_level(&self) -> u8 {
        self.target_level
    }

    pub const fn input_count(&self) -> usize {
        self.inputs.len()
    }
}
pub fn select_component_compaction(
    previous: ComponentStreamRoot,
    mut load_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    limits: ComponentCompactionLimits,
) -> Result<Option<ComponentCompactionPlan>, IndexError> {
    let mut cursor = ComponentStreamReverseCursor::new(previous)?;
    let mut segments = Vec::new();
    loop {
        match cursor.next()? {
            ComponentStreamReverseStep::LoadPage { hash } => {
                let page = load_page(hash)?;
                cursor.provide_page(hash, &page)?;
            }
            ComponentStreamReverseStep::Segment(segment) => segments.push(segment),
            ComponentStreamReverseStep::Complete => break,
        }
    }
    segments.reverse();
    select_component_compaction_from_segments(previous, &segments, limits)
}

fn select_component_compaction_from_segments(
    previous: ComponentStreamRoot,
    segments: &[ComponentSegmentDescriptor],
    limits: ComponentCompactionLimits,
) -> Result<Option<ComponentCompactionPlan>, IndexError> {
    let limits = limits.validate()?;
    validate_root(previous)?;
    validate_segments(segments)?;
    let source_level = (0_u8..63).find(|level| {
        segments
            .iter()
            .filter(|run| run.level == *level)
            .take(limits.l0_trigger)
            .count()
            == limits.l0_trigger
    });
    let Some(source_level) = source_level else {
        return Ok(None);
    };
    let target_level = source_level
        .checked_add(1)
        .ok_or(IndexError::OffsetOverflow)?;
    let source = segments
        .iter()
        .filter(|run| run.level == source_level)
        .take(limits.l0_trigger)
        .cloned()
        .collect::<Vec<_>>();
    let minimum_key = source.iter().map(|run| run.minimum_key).min().unwrap();
    let maximum_key = source.iter().map(|run| run.maximum_key).max().unwrap();
    let mut inputs = source;
    inputs.extend(
        segments
            .iter()
            .filter(|run| {
                run.level == target_level
                    && run.minimum_key <= maximum_key
                    && minimum_key <= run.maximum_key
            })
            .cloned(),
    );
    if inputs.len() > limits.maximum_input_runs {
        return Ok(None);
    }
    inputs.sort_unstable_by_key(|run| run.sequence);
    let selected = inputs
        .iter()
        .map(|run| run.sequence)
        .collect::<BTreeSet<_>>();
    let covers_oldest_history = !segments.iter().any(|run| {
        !selected.contains(&run.sequence)
            && run.sequence < inputs.last().unwrap().sequence
            && run.minimum_key <= maximum_key
            && minimum_key <= run.maximum_key
    });
    let plan = ComponentCompactionPlan {
        stream_root_hash: previous.root_hash,
        component: previous.component,
        target_level,
        minimum_key,
        maximum_key,
        source_start_offset: inputs
            .iter()
            .map(|run| run.source_start_offset)
            .min()
            .unwrap(),
        next_offset: inputs.iter().map(|run| run.next_offset).max().unwrap(),
        through_atomic_position: inputs
            .iter()
            .map(|run| run.through_atomic_position)
            .max()
            .unwrap(),
        inputs,
        covers_oldest_history,
    };
    Ok(Some(plan))
}
/// Streaming k-way newest-wins merge with de-duplicated, byte-charged input packs.
pub fn compact_component_runs(
    plan: &ComponentCompactionPlan,
    limits: ComponentCompactionLimits,
    policy: TombstoneCompactionPolicy,
    mut load_pack: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<Vec<SealedComponentDelta>, IndexError> {
    let limits = limits.validate()?;
    if plan.inputs.len() < 2 || plan.inputs.len() > limits.maximum_input_runs {
        return Err(IndexError::InvalidDefinition(
            "projection compaction plan has invalid fan-in".into(),
        ));
    }
    if policy == TombstoneCompactionPolicy::DropWhenOldestHistoryCovered
        && !plan.covers_oldest_history
    {
        return Err(IndexError::InvalidDefinition(
            "projection compaction cannot discard an unproven tombstone".into(),
        ));
    }
    let mut packs = BTreeMap::new();
    let mut loaded = 0_usize;
    for run in &plan.inputs {
        if packs.contains_key(&run.pack_hash) {
            continue;
        }
        let pack = load_pack(run.pack_hash)?;
        loaded = loaded
            .checked_add(pack.len())
            .ok_or(IndexError::OffsetOverflow)?;
        if loaded > limits.maximum_loaded_pack_bytes {
            return Err(IndexError::ResourceLimit {
                needed: loaded,
                limit: limits.maximum_loaded_pack_bytes,
            });
        }
        if *blake3::hash(&pack).as_bytes() != run.pack_hash {
            return Err(IndexError::Integrity);
        }
        packs.insert(run.pack_hash, pack);
    }
    let slices = plan
        .inputs
        .iter()
        .map(|run| {
            let pack = packs.get(&run.pack_hash).ok_or(IndexError::Integrity)?;
            let start = usize::try_from(run.pack_offset).map_err(|_| IndexError::OffsetOverflow)?;
            let length =
                usize::try_from(run.encoded_bytes).map_err(|_| IndexError::OffsetOverflow)?;
            let end = start
                .checked_add(length)
                .ok_or(IndexError::OffsetOverflow)?;
            let bytes = pack.get(start..end).ok_or(IndexError::Integrity)?;
            if *blake3::hash(bytes).as_bytes() != run.segment_hash {
                return Err(IndexError::Integrity);
            }
            Ok(bytes)
        })
        .collect::<Result<Vec<_>, IndexError>>()?;
    let mut cursors = slices
        .iter()
        .map(|bytes| ComponentDeltaCursor::new(bytes))
        .collect::<Result<Vec<_>, _>>()?;
    if cursors
        .iter()
        .any(|cursor| cursor.component() != plan.component)
    {
        return Err(IndexError::Integrity);
    }
    let mut heads = cursors
        .iter_mut()
        .map(ComponentDeltaCursor::next_record)
        .collect::<Result<Vec<_>, _>>()?;
    let mut output = Vec::new();
    let mut records = BTreeMap::new();
    let mut resident = 0_usize;
    loop {
        let Some(key) = heads.iter().flatten().map(|record| record.stable_key).min() else {
            break;
        };
        let winner = heads
            .iter()
            .enumerate()
            .filter(|(_, record)| record.is_some_and(|record| record.stable_key == key))
            .max_by_key(|(index, _)| plan.inputs[*index].sequence)
            .and_then(|(index, record)| record.map(|record| (index, record)))
            .ok_or(IndexError::Integrity)?;
        let replacement = winner.1.replacement.map(<[u8]>::to_vec);
        let record_bytes = 160_usize
            .checked_add(replacement.as_ref().map_or(0, Vec::len))
            .ok_or(IndexError::OffsetOverflow)?;
        if record_bytes > limits.maximum_output_run_bytes {
            return Err(IndexError::ResourceLimit {
                needed: record_bytes,
                limit: limits.maximum_output_run_bytes,
            });
        }
        if resident > 0 && resident.saturating_add(record_bytes) > limits.maximum_output_run_bytes {
            output.push(seal_component(
                plan.component,
                std::mem::take(&mut records),
            )?);
            resident = 0;
        }
        if replacement.is_some()
            || policy == TombstoneCompactionPolicy::Retain
            || !plan.covers_oldest_history
        {
            resident = resident
                .checked_add(record_bytes)
                .ok_or(IndexError::OffsetOverflow)?;
            records.insert(key, replacement);
        }
        for index in 0..heads.len() {
            if heads[index].is_some_and(|record| record.stable_key == key) {
                heads[index] = cursors[index].next_record()?;
            }
        }
    }
    if !records.is_empty() {
        output.push(seal_component(plan.component, records)?);
    }
    if output.is_empty() && policy == TombstoneCompactionPolicy::Retain {
        return Err(IndexError::Integrity);
    }
    Ok(output)
}
pub fn splice_compacted_component_runs(
    previous: ComponentStreamRoot,
    plan: &ComponentCompactionPlan,
    output: &[PackedComponentDelta],
    mut load_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<ComponentStreamAppend, IndexError> {
    validate_root(previous)?;
    validate_compaction_plan(plan)?;
    if previous.root_hash != plan.stream_root_hash || previous.component != plan.component {
        return Err(IndexError::Integrity);
    }
    if output.len() > plan.inputs.len() {
        return Err(IndexError::ResourceLimit {
            needed: output.len(),
            limit: plan.inputs.len(),
        });
    }
    let selected = plan
        .inputs
        .iter()
        .map(|run| (run.sequence, run))
        .collect::<BTreeMap<_, _>>();
    if selected.len() != plan.inputs.len() {
        return Err(IndexError::InvalidDefinition(
            "projection compaction input sequences are not unique".into(),
        ));
    }
    let slots = plan
        .inputs
        .iter()
        .map(|run| run.sequence)
        .rev()
        .take(output.len())
        .collect::<Vec<_>>();
    let mut replacements = BTreeMap::new();
    for (slot, delta) in slots.into_iter().rev().zip(output) {
        if delta.component != plan.component {
            return Err(IndexError::InvalidDefinition(
                "compaction output belongs to another component".into(),
            ));
        }
        if delta.minimum_key < plan.minimum_key || delta.maximum_key > plan.maximum_key {
            return Err(IndexError::InvalidDefinition(
                "compaction output escapes its selected key coverage".into(),
            ));
        }
        replacements.insert(
            slot,
            descriptor(
                slot,
                plan.target_level,
                plan.source_start_offset,
                plan.next_offset,
                plan.through_atomic_position,
                delta,
            )?,
        );
    }
    let mut new_pages = Vec::new();
    let mut matched = 0_usize;
    let rewritten = splice_subtree(
        previous.component,
        previous.root_hash,
        &selected,
        &replacements,
        &mut load_page,
        &mut new_pages,
        &mut matched,
    )?;
    if matched != selected.len() {
        return Err(IndexError::Integrity);
    }
    let root = rewritten
        .into_iter()
        .next()
        .ok_or(IndexError::InvalidDefinition(
            "compaction removed every component run".into(),
        ))?;
    if root.segment_count > MAX_COMPONENT_STREAM_SEGMENTS as u64 {
        return Err(IndexError::ResourceLimit {
            needed: usize::try_from(root.segment_count).unwrap_or(usize::MAX),
            limit: MAX_COMPONENT_STREAM_SEGMENTS,
        });
    }
    Ok(ComponentStreamAppend {
        root: ComponentStreamRoot {
            component: previous.component,
            root_hash: root.hash,
            segment_count: root.segment_count,
            first_sequence: root.first_sequence,
            last_sequence: root.last_sequence,
            encoded_bytes: root.encoded_bytes,
            logical_bytes: root.logical_bytes,
            directory_bytes: root.directory_bytes,
        },
        new_pages,
    })
}
fn validate_compaction_plan(plan: &ComponentCompactionPlan) -> Result<(), IndexError> {
    if plan.inputs.len() < 2
        || plan.inputs.len() > COMPONENT_STREAM_DIRECTORY_FANOUT
        || plan.target_level == 0
        || plan.inputs.iter().any(|run| run.validate().is_err())
        || plan
            .inputs
            .windows(2)
            .any(|pair| pair[0].sequence >= pair[1].sequence)
        || plan.minimum_key
            != plan
                .inputs
                .iter()
                .map(|run| run.minimum_key)
                .min()
                .ok_or(IndexError::Integrity)?
        || plan.maximum_key
            != plan
                .inputs
                .iter()
                .map(|run| run.maximum_key)
                .max()
                .ok_or(IndexError::Integrity)?
        || plan.source_start_offset
            != plan
                .inputs
                .iter()
                .map(|run| run.source_start_offset)
                .min()
                .ok_or(IndexError::Integrity)?
        || plan.next_offset
            != plan
                .inputs
                .iter()
                .map(|run| run.next_offset)
                .max()
                .ok_or(IndexError::Integrity)?
        || plan.through_atomic_position
            != plan
                .inputs
                .iter()
                .map(|run| run.through_atomic_position)
                .max()
                .ok_or(IndexError::Integrity)?
    {
        return Err(IndexError::InvalidDefinition(
            "projection compaction plan coverage is invalid".into(),
        ));
    }
    Ok(())
}
fn splice_subtree(
    component: ComponentIdentity,
    hash: [u8; 32],
    selected: &BTreeMap<u64, &ComponentSegmentDescriptor>,
    replacements: &BTreeMap<u64, ComponentSegmentDescriptor>,
    load_page: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    new_pages: &mut Vec<EncodedComponentStreamPage>,
    matched: &mut usize,
) -> Result<Vec<Child>, IndexError> {
    let bytes = load_page(hash)?;
    if *blake3::hash(&bytes).as_bytes() != hash {
        return Err(IndexError::Integrity);
    }
    match decode_page(component, &bytes)? {
        Page::Leaf(segments) => {
            let mut next = Vec::with_capacity(segments.len());
            for segment in segments {
                match selected.get(&segment.sequence) {
                    None => next.push(segment),
                    Some(expected) if *expected == &segment => {
                        *matched = matched.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
                        if let Some(replacement) = replacements.get(&segment.sequence) {
                            next.push(replacement.clone());
                        }
                    }
                    Some(_) => return Err(IndexError::Integrity),
                }
            }
            if next.is_empty() {
                return Ok(Vec::new());
            }
            next.sort_unstable_by_key(|segment| segment.sequence);
            validate_segments(&next)?;
            let encoded = encode_page(component, &Page::Leaf(next.clone()))?;
            let child = Child::from_segments(&next, encoded.hash, encoded.bytes.len() as u64)?;
            new_pages.push(encoded);
            Ok(vec![child])
        }
        Page::Branch(children) => {
            let mut next = Vec::with_capacity(children.len());
            for child in children {
                let selected_here = selected
                    .range(child.first_sequence..=child.last_sequence)
                    .next()
                    .is_some();
                if selected_here {
                    next.extend(splice_subtree(
                        component,
                        child.hash,
                        selected,
                        replacements,
                        load_page,
                        new_pages,
                        matched,
                    )?);
                } else {
                    next.push(child);
                }
            }
            if next.is_empty() {
                return Ok(Vec::new());
            }
            validate_children(&next)?;
            let encoded = encode_page(component, &Page::Branch(next.clone()))?;
            let child = Child::from_children(&next, encoded.hash, encoded.bytes.len() as u64)?;
            new_pages.push(encoded);
            Ok(vec![child])
        }
    }
}
fn descriptor(
    sequence: u64,
    level: u8,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
    delta: &PackedComponentDelta,
) -> Result<ComponentSegmentDescriptor, IndexError> {
    let descriptor = ComponentSegmentDescriptor {
        sequence,
        level,
        minimum_key: delta.minimum_key,
        maximum_key: delta.maximum_key,
        source_start_offset,
        next_offset,
        through_atomic_position,
        pack_hash: delta.pack_hash,
        pack_offset: delta.offset,
        segment_hash: delta.segment_hash,
        encoded_bytes: delta.encoded_bytes,
        logical_bytes: delta.logical_bytes,
        records: delta.records,
    };
    descriptor.validate()?;
    Ok(descriptor)
}

fn validate_root(root: ComponentStreamRoot) -> Result<(), IndexError> {
    if root.root_hash == [0; 32]
        || root.segment_count == 0
        || root.segment_count > MAX_COMPONENT_STREAM_SEGMENTS as u64
        || root.first_sequence == 0
        || root.first_sequence > root.last_sequence
        || root.encoded_bytes == 0
        || root.logical_bytes == 0
        || root.directory_bytes == 0
    {
        return Err(IndexError::InvalidDefinition(
            "component stream root is invalid".into(),
        ));
    }
    Ok(())
}

fn append_subtree(
    component: ComponentIdentity,
    hash: [u8; 32],
    descriptor: &ComponentSegmentDescriptor,
    load_page: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    new_pages: &mut Vec<EncodedComponentStreamPage>,
) -> Result<Vec<Child>, IndexError> {
    let bytes = load_page(hash)?;
    if hash != *blake3::hash(&bytes).as_bytes() {
        return Err(IndexError::Integrity);
    }
    match decode_page(component, &bytes)? {
        Page::Leaf(mut segments) => {
            if segments
                .last()
                .and_then(|last| last.sequence.checked_add(1))
                != Some(descriptor.sequence)
            {
                return Err(IndexError::Integrity);
            }
            if segments.len() < COMPONENT_STREAM_DIRECTORY_FANOUT {
                segments.push(descriptor.clone());
                let encoded = encode_page(component, &Page::Leaf(segments.clone()))?;
                let child =
                    Child::from_segments(&segments, encoded.hash, encoded.bytes.len() as u64)?;
                new_pages.push(encoded);
                Ok(vec![child])
            } else {
                let existing = Child::from_segments(&segments, hash, bytes.len() as u64)?;
                let encoded = encode_page(component, &Page::Leaf(vec![descriptor.clone()]))?;
                let appended = Child::from_segments(
                    std::slice::from_ref(descriptor),
                    encoded.hash,
                    encoded.bytes.len() as u64,
                )?;
                new_pages.push(encoded);
                Ok(vec![existing, appended])
            }
        }
        Page::Branch(mut children) => {
            let previous = children.pop().ok_or(IndexError::Integrity)?;
            if previous.last_sequence.checked_add(1) != Some(descriptor.sequence) {
                return Err(IndexError::Integrity);
            }
            children.extend(append_subtree(
                component,
                previous.hash,
                descriptor,
                load_page,
                new_pages,
            )?);
            if children.len() <= COMPONENT_STREAM_DIRECTORY_FANOUT {
                let encoded = encode_page(component, &Page::Branch(children.clone()))?;
                let child =
                    Child::from_children(&children, encoded.hash, encoded.bytes.len() as u64)?;
                if encoded.hash != hash {
                    new_pages.push(encoded);
                }
                Ok(vec![child])
            } else {
                let right = children.split_off(COMPONENT_STREAM_DIRECTORY_FANOUT);
                let mut result = Vec::with_capacity(2);
                for half in [children, right] {
                    let encoded = encode_page(component, &Page::Branch(half.clone()))?;
                    let child =
                        Child::from_children(&half, encoded.hash, encoded.bytes.len() as u64)?;
                    if encoded.hash != hash {
                        new_pages.push(encoded);
                    }
                    result.push(child);
                }
                Ok(result)
            }
        }
    }
}

fn validate_artifact<'a>(
    component: ComponentIdentity,
    descriptor: &ComponentSegmentDescriptor,
    pack: &'a [u8],
) -> Result<&'a [u8], IndexError> {
    if *blake3::hash(pack).as_bytes() != descriptor.pack_hash {
        return Err(IndexError::Integrity);
    }
    let start = usize::try_from(descriptor.pack_offset).map_err(|_| IndexError::OffsetOverflow)?;
    let length =
        usize::try_from(descriptor.encoded_bytes).map_err(|_| IndexError::OffsetOverflow)?;
    let end = start
        .checked_add(length)
        .ok_or(IndexError::OffsetOverflow)?;
    let bytes = pack.get(start..end).ok_or(IndexError::Integrity)?;
    if *blake3::hash(bytes).as_bytes() != descriptor.segment_hash {
        return Err(IndexError::Integrity);
    }
    let decoded = decode_component_delta_segment(bytes)?;
    if decoded.component != component || decoded.records.len() as u64 != descriptor.records {
        return Err(IndexError::Integrity);
    }
    Ok(bytes)
}

fn validate_segments(segments: &[ComponentSegmentDescriptor]) -> Result<(), IndexError> {
    if segments.is_empty()
        || segments.len() > MAX_COMPONENT_STREAM_SEGMENTS
        || segments.iter().any(|segment| segment.validate().is_err())
        || segments
            .windows(2)
            .any(|pair| pair[0].sequence >= pair[1].sequence)
    {
        return Err(IndexError::InvalidDefinition(
            "component stream is empty, unbounded, or non-canonical".into(),
        ));
    }
    let mut levels = BTreeMap::<u8, Vec<&ComponentSegmentDescriptor>>::new();
    for segment in segments.iter().filter(|segment| segment.level > 0) {
        levels.entry(segment.level).or_default().push(segment);
    }
    for level in levels.values_mut() {
        level.sort_unstable_by_key(|segment| segment.minimum_key);
        if level
            .windows(2)
            .any(|pair| pair[0].maximum_key >= pair[1].minimum_key)
        {
            return Err(IndexError::InvalidDefinition(
                "nonzero projection LSM level contains overlapping key ranges".into(),
            ));
        }
    }
    Ok(())
}

impl Child {
    fn from_segments(
        segments: &[ComponentSegmentDescriptor],
        hash: [u8; 32],
        page_bytes: u64,
    ) -> Result<Self, IndexError> {
        let first = segments.first().expect("nonempty leaf");
        let last = segments.last().expect("nonempty leaf");
        Ok(Self {
            first_sequence: first.sequence,
            last_sequence: last.sequence,
            hash,
            segment_count: segments.len() as u64,
            encoded_bytes: sum_segments(segments, |segment| segment.encoded_bytes)?,
            logical_bytes: sum_segments(segments, |segment| segment.logical_bytes)?,
            directory_bytes: page_bytes,
            minimum_key: segments
                .iter()
                .map(|segment| segment.minimum_key)
                .min()
                .expect("nonempty leaf"),
            maximum_key: segments
                .iter()
                .map(|segment| segment.maximum_key)
                .max()
                .expect("nonempty leaf"),
            minimum_source_offset: segments
                .iter()
                .map(|segment| segment.source_start_offset)
                .min()
                .expect("nonempty leaf"),
            maximum_next_offset: segments
                .iter()
                .map(|segment| segment.next_offset)
                .max()
                .expect("nonempty leaf"),
            through_atomic_position: segments
                .iter()
                .map(|segment| segment.through_atomic_position)
                .max()
                .expect("nonempty leaf"),
            summary_exact: true,
        })
    }

    fn from_children(
        children: &[Child],
        hash: [u8; 32],
        page_bytes: u64,
    ) -> Result<Self, IndexError> {
        let first = children.first().expect("nonempty branch");
        let last = children.last().expect("nonempty branch");
        Ok(Self {
            first_sequence: first.first_sequence,
            last_sequence: last.last_sequence,
            hash,
            segment_count: sum_children(children, |child| child.segment_count)?,
            encoded_bytes: sum_children(children, |child| child.encoded_bytes)?,
            logical_bytes: sum_children(children, |child| child.logical_bytes)?,
            directory_bytes: sum_children(children, |child| child.directory_bytes)?
                .checked_add(page_bytes)
                .ok_or(IndexError::OffsetOverflow)?,
            minimum_key: children
                .iter()
                .map(|child| child.minimum_key)
                .min()
                .expect("nonempty branch"),
            maximum_key: children
                .iter()
                .map(|child| child.maximum_key)
                .max()
                .expect("nonempty branch"),
            minimum_source_offset: children
                .iter()
                .map(|child| child.minimum_source_offset)
                .min()
                .expect("nonempty branch"),
            maximum_next_offset: children
                .iter()
                .map(|child| child.maximum_next_offset)
                .max()
                .expect("nonempty branch"),
            through_atomic_position: children
                .iter()
                .map(|child| child.through_atomic_position)
                .max()
                .expect("nonempty branch"),
            summary_exact: true,
        })
    }
}

fn sum_segments(
    segments: &[ComponentSegmentDescriptor],
    value: impl Fn(&ComponentSegmentDescriptor) -> u64,
) -> Result<u64, IndexError> {
    segments.iter().try_fold(0_u64, |total, segment| {
        total
            .checked_add(value(segment))
            .ok_or(IndexError::OffsetOverflow)
    })
}

fn sum_children(children: &[Child], value: impl Fn(&Child) -> u64) -> Result<u64, IndexError> {
    children.iter().try_fold(0_u64, |total, child| {
        total
            .checked_add(value(child))
            .ok_or(IndexError::OffsetOverflow)
    })
}

fn encode_page(
    component: ComponentIdentity,
    page: &Page,
) -> Result<EncodedComponentStreamPage, IndexError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(PAGE_MAGIC);
    put_u16(&mut bytes, PAGE_FORMAT);
    put_component(&mut bytes, component);
    match page {
        Page::Leaf(segments) => {
            if segments.is_empty() || segments.len() > COMPONENT_STREAM_DIRECTORY_FANOUT {
                return Err(IndexError::InvalidDefinition(
                    "component stream leaf fanout is invalid".into(),
                ));
            }
            bytes.push(LEAF_PAGE);
            put_u16(&mut bytes, segments.len() as u16);
            for segment in segments {
                segment.validate()?;
                put_u64(&mut bytes, segment.sequence);
                bytes.push(segment.level);
                bytes.extend_from_slice(&segment.minimum_key.bytes());
                bytes.extend_from_slice(&segment.maximum_key.bytes());
                put_u64(&mut bytes, segment.source_start_offset);
                put_u64(&mut bytes, segment.next_offset);
                put_u64(&mut bytes, segment.through_atomic_position);
                bytes.extend_from_slice(&segment.pack_hash);
                put_u64(&mut bytes, segment.pack_offset);
                bytes.extend_from_slice(&segment.segment_hash);
                put_u64(&mut bytes, segment.encoded_bytes);
                put_u64(&mut bytes, segment.logical_bytes);
                put_u64(&mut bytes, segment.records);
            }
        }
        Page::Branch(children) => {
            validate_children(children)?;
            bytes.push(BRANCH_PAGE);
            put_u16(&mut bytes, children.len() as u16);
            for child in children {
                put_u64(&mut bytes, child.first_sequence);
                put_u64(&mut bytes, child.last_sequence);
                bytes.extend_from_slice(&child.hash);
                put_u64(&mut bytes, child.segment_count);
                put_u64(&mut bytes, child.encoded_bytes);
                put_u64(&mut bytes, child.logical_bytes);
                put_u64(&mut bytes, child.directory_bytes);
                bytes.extend_from_slice(&child.minimum_key.bytes());
                bytes.extend_from_slice(&child.maximum_key.bytes());
                put_u64(&mut bytes, child.minimum_source_offset);
                put_u64(&mut bytes, child.maximum_next_offset);
                put_u64(&mut bytes, child.through_atomic_position);
            }
        }
    }
    append_integrity(&mut bytes);
    Ok(EncodedComponentStreamPage {
        hash: *blake3::hash(&bytes).as_bytes(),
        bytes,
    })
}

fn decode_subtree(
    component: ComponentIdentity,
    hash: [u8; 32],
    pages: &BTreeMap<[u8; 32], &[u8]>,
    visited: &mut BTreeSet<[u8; 32]>,
    segments: &mut Vec<ComponentSegmentDescriptor>,
) -> Result<Child, IndexError> {
    if !visited.insert(hash) {
        return Err(IndexError::Integrity);
    }
    let bytes = pages.get(&hash).ok_or(IndexError::Integrity)?;
    match decode_page(component, bytes)? {
        Page::Leaf(page_segments) => {
            let child = Child::from_segments(&page_segments, hash, bytes.len() as u64)?;
            segments.extend(page_segments);
            Ok(child)
        }
        Page::Branch(children) => {
            let before = segments.len();
            for expected in &children {
                let actual = decode_subtree(component, expected.hash, pages, visited, segments)?;
                if &actual != expected {
                    return Err(IndexError::Integrity);
                }
            }
            if segments.len() == before {
                return Err(IndexError::Integrity);
            }
            Child::from_children(&children, hash, bytes.len() as u64)
        }
    }
}

fn decode_page(component: ComponentIdentity, bytes: &[u8]) -> Result<Page, IndexError> {
    let payload = verify_integrity(bytes)?;
    let mut input = Decoder::new(payload);
    input.expect(PAGE_MAGIC)?;
    if input.u16()? != PAGE_FORMAT || input.component()? != component {
        return Err(IndexError::InvalidFormat(
            "component stream page identity is invalid",
        ));
    }
    let kind = input.byte()?;
    let count = input.u16()? as usize;
    if count == 0 || count > COMPONENT_STREAM_DIRECTORY_FANOUT {
        return Err(IndexError::InvalidFormat(
            "component stream page fanout is invalid",
        ));
    }
    let page = match kind {
        LEAF_PAGE => {
            let mut segments = Vec::with_capacity(count);
            for _ in 0..count {
                segments.push(ComponentSegmentDescriptor {
                    sequence: input.u64()?,
                    level: input.byte()?,
                    minimum_key: StableDocumentKey::from_bytes(input.array_32()?)?,
                    maximum_key: StableDocumentKey::from_bytes(input.array_32()?)?,
                    source_start_offset: input.u64()?,
                    next_offset: input.u64()?,
                    through_atomic_position: input.u64()?,
                    pack_hash: input.array_32()?,
                    pack_offset: input.u64()?,
                    segment_hash: input.array_32()?,
                    encoded_bytes: input.u64()?,
                    logical_bytes: input.u64()?,
                    records: input.u64()?,
                });
            }
            Page::Leaf(segments)
        }
        BRANCH_PAGE => {
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(Child {
                    first_sequence: input.u64()?,
                    last_sequence: input.u64()?,
                    hash: input.array_32()?,
                    segment_count: input.u64()?,
                    encoded_bytes: input.u64()?,
                    logical_bytes: input.u64()?,
                    directory_bytes: input.u64()?,
                    minimum_key: StableDocumentKey::from_bytes(input.array_32()?)?,
                    maximum_key: StableDocumentKey::from_bytes(input.array_32()?)?,
                    minimum_source_offset: input.u64()?,
                    maximum_next_offset: input.u64()?,
                    through_atomic_position: input.u64()?,
                    summary_exact: true,
                });
            }
            validate_children(&children)?;
            Page::Branch(children)
        }
        _ => {
            return Err(IndexError::InvalidFormat(
                "component stream page kind is invalid",
            ));
        }
    };
    input.finish()?;
    Ok(page)
}

fn validate_children(children: &[Child]) -> Result<(), IndexError> {
    if children.is_empty()
        || children.len() > COMPONENT_STREAM_DIRECTORY_FANOUT
        || children.iter().any(|child| {
            child.first_sequence == 0
                || child.first_sequence > child.last_sequence
                || child.hash == [0; 32]
                || child.segment_count == 0
                || child.encoded_bytes == 0
                || child.logical_bytes == 0
                || child.directory_bytes == 0
                || child.minimum_key > child.maximum_key
                || child.minimum_source_offset >= child.maximum_next_offset
        })
        || children
            .windows(2)
            .any(|pair| pair[0].last_sequence >= pair[1].first_sequence)
    {
        return Err(IndexError::InvalidDefinition(
            "component stream branch is invalid".into(),
        ));
    }
    Ok(())
}

fn put_component(out: &mut Vec<u8>, component: ComponentIdentity) {
    match component {
        ComponentIdentity::DocumentHead => out.push(1),
        ComponentIdentity::SourceRecords => out.push(6),
        ComponentIdentity::Membership(recipe) => {
            out.push(2);
            out.extend_from_slice(&recipe.bytes());
        }
        ComponentIdentity::Field(recipe) => {
            out.push(3);
            out.extend_from_slice(&recipe.bytes());
        }
        ComponentIdentity::Order(recipe) => {
            out.push(4);
            out.extend_from_slice(&recipe.bytes());
        }
    }
}

fn append_integrity(bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(blake3::hash(bytes).as_bytes());
}

fn verify_integrity(bytes: &[u8]) -> Result<&[u8], IndexError> {
    let split = bytes.len().checked_sub(32).ok_or(IndexError::Integrity)?;
    let (payload, integrity) = bytes.split_at(split);
    if blake3::hash(payload).as_bytes() != integrity {
        return Err(IndexError::Integrity);
    }
    Ok(payload)
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], IndexError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(IndexError::UnexpectedEof {
                expected: end as u64,
                actual: self.bytes.len() as u64,
            })?;
        self.offset = end;
        Ok(value)
    }
    fn expect(&mut self, expected: &[u8]) -> Result<(), IndexError> {
        if self.take(expected.len())? == expected {
            Ok(())
        } else {
            Err(IndexError::InvalidFormat(
                "component stream page magic is invalid",
            ))
        }
    }
    fn byte(&mut self) -> Result<u8, IndexError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, IndexError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, IndexError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn array_32(&mut self) -> Result<[u8; 32], IndexError> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn component(&mut self) -> Result<ComponentIdentity, IndexError> {
        match self.byte()? {
            1 => Ok(ComponentIdentity::DocumentHead),
            6 => Ok(ComponentIdentity::SourceRecords),
            2 => Ok(ComponentIdentity::Membership(RecipeIdentity::new(
                self.array_32()?,
            )?)),
            3 => Ok(ComponentIdentity::Field(RecipeIdentity::new(
                self.array_32()?,
            )?)),
            4 => Ok(ComponentIdentity::Order(RecipeIdentity::new(
                self.array_32()?,
            )?)),
            _ => Err(IndexError::Decode(
                "component stream identity is unknown".into(),
            )),
        }
    }
    fn finish(self) -> Result<(), IndexError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(IndexError::Decode(
                "component stream page has trailing bytes".into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v6::pack::test_pack_credits;
    fn key(byte: u8) -> StableDocumentKey {
        StableDocumentKey::from_bytes([byte; 32]).unwrap()
    }

    fn sealed(
        component: ComponentIdentity,
        records: &[(u8, Option<&[u8]>)],
    ) -> SealedComponentDelta {
        seal_component(
            component,
            records
                .iter()
                .map(|(key_byte, value)| (key(*key_byte), value.map(<[u8]>::to_vec)))
                .collect(),
        )
        .unwrap()
    }
    fn packed(delta: SealedComponentDelta) -> (PackedComponentDelta, Vec<u8>) {
        let bytes = delta.bytes.len();
        let pack = pack_component_deltas(vec![delta], test_pack_credits(bytes))
            .unwrap()
            .packs
            .remove(0);
        (pack.deltas[0].clone(), pack.bytes)
    }
    fn run(sequence: u64) -> ComponentSegmentDescriptor {
        ComponentSegmentDescriptor {
            sequence,
            level: 0,
            minimum_key: key(1),
            maximum_key: key(u8::MAX),
            source_start_offset: sequence - 1,
            next_offset: sequence,
            through_atomic_position: sequence,
            pack_hash: *blake3::hash(&sequence.to_le_bytes()).as_bytes(),
            pack_offset: 0,
            segment_hash: *blake3::hash(&sequence.to_be_bytes()).as_bytes(),
            encoded_bytes: 100,
            logical_bytes: 80,
            records: 1,
        }
    }
    fn reachable_pages(
        component: ComponentIdentity,
        hash: [u8; 32],
        pages: &BTreeMap<[u8; 32], Vec<u8>>,
        reachable: &mut Vec<EncodedComponentStreamPage>,
    ) {
        let bytes = pages.get(&hash).unwrap().clone();
        if let Page::Branch(children) = decode_page(component, &bytes).unwrap() {
            for child in children {
                reachable_pages(component, child.hash, pages, reachable);
            }
        }
        reachable.push(EncodedComponentStreamPage { hash, bytes });
    }
    #[test]
    fn newest_delta_wins_and_tombstones_hide_older_values() {
        let component = ComponentIdentity::Field(RecipeIdentity::new([7; 32]).unwrap());
        let (first, first_pack) =
            packed(sealed(component, &[(1, Some(b"old")), (2, Some(b"kept"))]));
        let (second, second_pack) = packed(sealed(component, &[(1, Some(b"new")), (2, None)]));
        let one = append_component_delta(None, &first, 0, 1, 1).unwrap();
        let two = append_component_delta(Some(&one), &second, 1, 2, 2).unwrap();
        let artifacts = [
            (first.pack_hash, first_pack),
            (second.pack_hash, second_pack),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            resolve_component_record(&two, &artifacts, key(1)).unwrap(),
            Some(b"new".to_vec())
        );
        assert_eq!(
            resolve_component_record(&two, &artifacts, key(2)).unwrap(),
            None
        );
        let newest = decode_component_stream(&two).unwrap().pop().unwrap();
        let newest_pack = artifacts.get(&newest.pack_hash).unwrap();
        assert_eq!(
            lookup_component_record_in_pack(component, &newest, newest_pack, key(2)).unwrap(),
            ComponentRecordLookup::Tombstone
        );
        assert_eq!(
            lookup_component_record_in_pack(component, &newest, newest_pack, key(3)).unwrap(),
            ComponentRecordLookup::Missing
        );
    }

    #[test]
    fn compaction_preserves_the_exact_newest_view() {
        let component = ComponentIdentity::Membership(RecipeIdentity::new([8; 32]).unwrap());
        let (first, first_pack) =
            packed(sealed(component, &[(1, Some(b"one")), (2, Some(b"two"))]));
        let (second, second_pack) = packed(sealed(component, &[(1, None), (3, Some(b"three"))]));
        let one = append_component_delta(None, &first, 0, 1, 1).unwrap();
        let two = append_component_delta(Some(&one), &second, 1, 2, 2).unwrap();
        let artifacts = [
            (first.pack_hash, first_pack),
            (second.pack_hash, second_pack),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let pages = two
            .pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone()))
            .collect::<BTreeMap<_, _>>();
        let plan = select_component_compaction(
            two.root(),
            |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
            ComponentCompactionLimits {
                l0_trigger: 2,
                maximum_input_runs: 4,
                maximum_loaded_pack_bytes: 4096,
                maximum_output_run_bytes: 1024,
            },
        )
        .unwrap()
        .unwrap();
        let compacted = compact_component_runs(
            &plan,
            ComponentCompactionLimits {
                l0_trigger: 2,
                maximum_input_runs: 4,
                maximum_loaded_pack_bytes: 4096,
                maximum_output_run_bytes: 1024,
            },
            TombstoneCompactionPolicy::Retain,
            |hash| artifacts.get(&hash).cloned().ok_or(IndexError::Integrity),
        )
        .unwrap();
        assert_eq!(compacted.len(), 1);
        let (delta, bytes) = packed(compacted.into_iter().next().unwrap());
        let spliced =
            splice_compacted_component_runs(two.root(), &plan, &[delta.clone()], |hash| {
                pages.get(&hash).cloned().ok_or(IndexError::Integrity)
            })
            .unwrap();
        assert!(
            spliced.new_pages.len() <= 1,
            "one-leaf compaction rewrites one page"
        );
        let mut page_store = pages;
        page_store.extend(
            spliced
                .new_pages
                .iter()
                .map(|page| (page.hash, page.bytes.clone())),
        );
        let mut reachable = Vec::new();
        reachable_pages(
            component,
            spliced.root.root_hash,
            &page_store,
            &mut reachable,
        );
        let compacted = ComponentStreamDirectory {
            component,
            root_hash: spliced.root.root_hash,
            segment_count: spliced.root.segment_count,
            first_sequence: spliced.root.first_sequence,
            last_sequence: spliced.root.last_sequence,
            encoded_bytes: spliced.root.encoded_bytes,
            logical_bytes: spliced.root.logical_bytes,
            directory_bytes: spliced.root.directory_bytes,
            pages: reachable,
        };
        let compacted_artifacts = [(delta.pack_hash, bytes)].into_iter().collect();
        for stable_key in [key(1), key(2), key(3), key(4)] {
            assert_eq!(
                resolve_component_record(&two, &artifacts, stable_key).unwrap(),
                resolve_component_record(&compacted, &compacted_artifacts, stable_key).unwrap()
            );
        }
    }

    #[test]
    fn splice_rewrites_only_the_affected_page_path_and_refuses_unrepresentable_output() {
        let component = ComponentIdentity::DocumentHead;
        let segments = (1_u64..=300).map(run).collect::<Vec<_>>();
        let directory = build_component_stream(component, &segments).unwrap();
        let plan = ComponentCompactionPlan {
            stream_root_hash: directory.root_hash,
            component,
            inputs: segments[..2].to_vec(),
            target_level: 1,
            covers_oldest_history: false,
            minimum_key: key(1),
            maximum_key: key(u8::MAX),
            source_start_offset: 0,
            next_offset: 2,
            through_atomic_position: 2,
        };
        let (delta, _) = packed(sealed(component, &[(1, Some(b"new"))]));
        let pages = directory
            .pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone()))
            .collect::<BTreeMap<_, _>>();
        let spliced =
            splice_compacted_component_runs(directory.root(), &plan, &[delta.clone()], |hash| {
                pages.get(&hash).cloned().ok_or(IndexError::Integrity)
            })
            .unwrap();
        assert_eq!(spliced.root.segment_count, 299);
        assert_eq!(spliced.new_pages.len(), 2, "leaf plus its root path");
        assert!(spliced.new_pages.len() < directory.pages.len());
        assert!(matches!(
            splice_compacted_component_runs(
                directory.root(),
                &plan,
                &[delta.clone(), delta.clone(), delta.clone()],
                |_| Err(IndexError::Integrity)
            ),
            Err(IndexError::ResourceLimit { .. })
        ));
        let mut wrong_root = directory.root();
        wrong_root.root_hash = [42; 32];
        assert_eq!(
            splice_compacted_component_runs(wrong_root, &plan, &[delta], |_| {
                Err(IndexError::Integrity)
            }),
            Err(IndexError::Integrity)
        );
    }

    #[test]
    fn directory_fanout_bounds_pages_for_seventy_thousand_segments() {
        let segments = (1_u64..=70_000).map(run).collect::<Vec<_>>();
        let directory = build_component_stream(ComponentIdentity::DocumentHead, &segments).unwrap();
        assert_eq!(decode_component_stream(&directory).unwrap(), segments);
        assert!(
            directory
                .pages
                .iter()
                .all(|page| page.bytes.len() < 32 * 1024)
        );
        assert!(directory.pages.len() < 600);
        let pages = directory
            .pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone()))
            .collect::<BTreeMap<_, _>>();
        let root_children = component_stream_child_hashes(
            directory.component,
            pages.get(&directory.root_hash).expect("root page"),
        )
        .unwrap();
        assert!(!root_children.is_empty());
        assert!(root_children.iter().all(|hash| pages.contains_key(hash)));
    }

    #[test]
    fn reverse_cursor_reaches_newest_segment_without_opening_all_pages() {
        let segments = (1_u64..=300).map(run).collect::<Vec<_>>();
        let directory = build_component_stream(ComponentIdentity::DocumentHead, &segments).unwrap();
        let pages = directory
            .pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut cursor = ComponentStreamReverseCursor::new(directory.root()).unwrap();
        let mut page_loads = 0;
        let newest = loop {
            match cursor.next().unwrap() {
                ComponentStreamReverseStep::LoadPage { hash } => {
                    page_loads += 1;
                    cursor
                        .provide_page(hash, pages.get(&hash).unwrap())
                        .unwrap();
                }
                ComponentStreamReverseStep::Segment(descriptor) => break descriptor,
                ComponentStreamReverseStep::Complete => panic!("stream unexpectedly empty"),
            }
        };
        assert_eq!(newest.sequence, 300);
        assert_eq!(page_loads, 2, "only the root and newest leaf are opened");
        assert!(page_loads < pages.len());
    }

    #[test]
    fn append_path_copies_only_the_logarithmic_right_spine() {
        let component = ComponentIdentity::DocumentHead;
        let segments = (1_u64..=65_536).map(run).collect::<Vec<_>>();
        let previous = build_component_stream(component, &segments).unwrap();
        let pages = previous
            .pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone()))
            .collect::<BTreeMap<_, _>>();
        let persisted_root = previous.component_root().unwrap();
        let reopened_root = ComponentStreamRoot::from_component_root(&persisted_root).unwrap();
        assert_eq!(reopened_root, previous.root());
        let (delta, _) = packed(sealed(component, &[(1, Some(b"next"))]));

        let appended = append_component_stream(
            Some(reopened_root),
            |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
            &delta,
            65_536,
            65_537,
            65_537,
        )
        .unwrap();

        assert_eq!(appended.root.segment_count, 65_537);
        assert_eq!(appended.new_pages.len(), 3);
        assert!(appended.new_pages.len() < previous.pages.len() / 50);
        let mut page_store = pages;
        page_store.extend(
            appended
                .new_pages
                .iter()
                .map(|page| (page.hash, page.bytes.clone())),
        );
        let mut all_pages = Vec::new();
        reachable_pages(
            component,
            appended.root.root_hash,
            &page_store,
            &mut all_pages,
        );
        let complete = ComponentStreamDirectory {
            component,
            root_hash: appended.root.root_hash,
            segment_count: appended.root.segment_count,
            first_sequence: appended.root.first_sequence,
            last_sequence: appended.root.last_sequence,
            encoded_bytes: appended.root.encoded_bytes,
            logical_bytes: appended.root.logical_bytes,
            directory_bytes: appended.root.directory_bytes,
            pages: all_pages,
        };
        let decoded = decode_component_stream(&complete).unwrap();
        assert_eq!(decoded.len(), 65_537);
        assert_eq!(decoded.last().unwrap().pack_hash, delta.pack_hash);
    }

    #[test]
    fn artifact_identity_is_verified_before_reading() {
        let component = ComponentIdentity::DocumentHead;
        let (segment, mut corrupted) = packed(sealed(component, &[(1, Some(b"state"))]));
        let directory = append_component_delta(None, &segment, 0, 1, 1).unwrap();
        corrupted[0] ^= 1;
        let artifacts = [(segment.pack_hash, corrupted)].into_iter().collect();
        assert!(matches!(
            resolve_component_record(&directory, &artifacts, key(1)),
            Err(IndexError::Integrity)
        ));
    }
}
