use std::collections::{BTreeMap, BTreeSet};

use crate::IndexError;

use super::buffer::seal_component;
use super::{
    ComponentIdentity, ComponentRoot, PackedComponentDelta, RecipeIdentity, SealedComponentDelta,
    StableDocumentKey, decode_component_delta_segment, pack_component_deltas,
};

const PAGE_MAGIC: &[u8; 8] = b"K5CSTR01";
const PAGE_FORMAT: u16 = 1;
const LEAF_PAGE: u8 = 1;
const BRANCH_PAGE: u8 = 2;
pub const COMPONENT_STREAM_DIRECTORY_FANOUT: usize = 256;
const MAX_COMPONENT_STREAM_SEGMENTS: usize = 1_000_000;

/// One immutable delta segment in chronological component-stream order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentSegmentDescriptor {
    pub sequence: u64,
    pub pack_hash: [u8; 32],
    pub pack_offset: u64,
    pub segment_hash: [u8; 32],
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub records: u64,
}

impl ComponentSegmentDescriptor {
    fn validate(&self) -> Result<(), IndexError> {
        if self.sequence == 0
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

/// Bounded-fanout content-addressed directory for one complete component
/// stream. Pages and delta artifacts are immutable; a generation atomically
/// installs only this root identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentStreamDirectory {
    pub component: ComponentIdentity,
    pub root_hash: [u8; 32],
    pub segment_count: u64,
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub directory_bytes: u64,
    pub pages: Vec<EncodedComponentStreamPage>,
}

impl ComponentStreamDirectory {
    pub fn component_root(&self) -> Result<ComponentRoot, IndexError> {
        decode_component_stream(self)?;
        ComponentRoot::new(
            self.component,
            self.root_hash,
            self.encoded_bytes
                .checked_add(self.directory_bytes)
                .ok_or(IndexError::OffsetOverflow)?,
            self.logical_bytes,
        )
    }

    pub fn root(&self) -> ComponentStreamRoot {
        ComponentStreamRoot {
            component: self.component,
            root_hash: self.root_hash,
            segment_count: self.segment_count,
            encoded_bytes: self.encoded_bytes,
            logical_bytes: self.logical_bytes,
            directory_bytes: self.directory_bytes,
        }
    }
}

/// The small generation-owned identity of a component stream. Immutable pages
/// are loaded independently by hash; appending never copies the full page set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ComponentStreamRoot {
    pub component: ComponentIdentity,
    pub root_hash: [u8; 32],
    pub segment_count: u64,
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub directory_bytes: u64,
}

/// A path-copy append result. Only these pages need publication before the new
/// root is installed; every other reachable page is shared with the prior root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentStreamAppend {
    pub root: ComponentStreamRoot,
    pub new_pages: Vec<EncodedComponentStreamPage>,
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
        encoded_bytes: root.encoded_bytes,
        logical_bytes: root.logical_bytes,
        directory_bytes: root.directory_bytes,
        pages,
    })
}

/// Append one immutable delta by copying only the rightmost tree path. The
/// loader is called once per existing level, so the work is O(log_256 N).
pub fn append_component_stream(
    previous: Option<ComponentStreamRoot>,
    mut load_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    delta: &PackedComponentDelta,
) -> Result<ComponentStreamAppend, IndexError> {
    let next_sequence = match previous {
        Some(root) => {
            validate_root(root)?;
            if root.component != delta.component {
                return Err(IndexError::InvalidDefinition(
                    "component delta was appended to a different stream".into(),
                ));
            }
            root.segment_count
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?
        }
        None => 1,
    };
    if next_sequence > MAX_COMPONENT_STREAM_SEGMENTS as u64 {
        return Err(IndexError::ResourceLimit {
            needed: next_sequence as usize,
            limit: MAX_COMPONENT_STREAM_SEGMENTS,
        });
    }
    let descriptor = descriptor(next_sequence, delta)?;
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
    if root_child.segment_count != next_sequence {
        return Err(IndexError::Integrity);
    }
    Ok(ComponentStreamAppend {
        root: ComponentStreamRoot {
            component: delta.component,
            root_hash: root_child.hash,
            segment_count: root_child.segment_count,
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
    let sequence = (segments.len() as u64)
        .checked_add(1)
        .ok_or(IndexError::OffsetOverflow)?;
    segments.push(descriptor(sequence, delta)?);
    build_component_stream(delta.component, &segments)
}

pub fn decode_component_stream(
    directory: &ComponentStreamDirectory,
) -> Result<Vec<ComponentSegmentDescriptor>, IndexError> {
    if directory.root_hash == [0; 32]
        || directory.segment_count == 0
        || directory.segment_count > MAX_COMPONENT_STREAM_SEGMENTS as u64
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

/// Resolve the newest value for one stable document key. `None` means either
/// the key never existed or the newest record is a tombstone.
pub fn resolve_component_record(
    directory: &ComponentStreamDirectory,
    artifacts: &BTreeMap<[u8; 32], Vec<u8>>,
    stable_key: StableDocumentKey,
) -> Result<Option<Vec<u8>>, IndexError> {
    let segments = decode_component_stream(directory)?;
    for descriptor in segments.iter().rev() {
        let pack = artifacts
            .get(&descriptor.pack_hash)
            .ok_or(IndexError::Integrity)?;
        let bytes = validate_artifact(directory.component, descriptor, pack)?;
        let decoded = decode_component_delta_segment(bytes)?;
        if let Ok(index) = decoded
            .records
            .binary_search_by_key(&stable_key, |record| record.stable_key)
        {
            return Ok(decoded.records[index].replacement.clone());
        }
    }
    Ok(None)
}

/// Fold every historical delta into one self-contained segment. Tombstones are
/// retained so a compacted component remains an exact replacement even when a
/// reader still holds an older generation concurrently.
pub fn compact_component_stream(
    directory: &ComponentStreamDirectory,
    artifacts: &BTreeMap<[u8; 32], Vec<u8>>,
) -> Result<(ComponentStreamDirectory, SealedComponentDelta), IndexError> {
    let segments = decode_component_stream(directory)?;
    let mut records = BTreeMap::new();
    for descriptor in &segments {
        let pack = artifacts
            .get(&descriptor.pack_hash)
            .ok_or(IndexError::Integrity)?;
        let bytes = validate_artifact(directory.component, descriptor, pack)?;
        for record in decode_component_delta_segment(bytes)?.records {
            records.insert(record.stable_key, record.replacement);
        }
    }
    let sealed = seal_component(directory.component, records)?;
    let pack = pack_component_deltas(vec![sealed])?
        .pop()
        .expect("one compacted segment produces one pack");
    let compacted = append_component_delta(None, &pack.deltas[0])?;
    let segment = SealedComponentDelta {
        root: ComponentRoot::new(
            pack.deltas[0].component,
            pack.deltas[0].segment_hash,
            pack.deltas[0].encoded_bytes,
            pack.deltas[0].logical_bytes,
        )?,
        bytes: pack.bytes,
        records: pack.deltas[0].records,
    };
    Ok((compacted, segment))
}

fn descriptor(
    sequence: u64,
    delta: &PackedComponentDelta,
) -> Result<ComponentSegmentDescriptor, IndexError> {
    let descriptor = ComponentSegmentDescriptor {
        sequence,
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
        || segments.iter().enumerate().any(|(index, segment)| {
            segment.validate().is_err() || segment.sequence != index as u64 + 1
        })
    {
        return Err(IndexError::InvalidDefinition(
            "component stream is empty, unbounded, or non-canonical".into(),
        ));
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
        })
        || children
            .windows(2)
            .any(|pair| pair[0].last_sequence.checked_add(1) != Some(pair[1].first_sequence))
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
        ComponentIdentity::ProjectedState => out.push(5),
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
            5 => Ok(ComponentIdentity::ProjectedState),
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
        let pack = pack_component_deltas(vec![delta]).unwrap().remove(0);
        (pack.deltas[0].clone(), pack.bytes)
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
        let one = append_component_delta(None, &first).unwrap();
        let two = append_component_delta(Some(&one), &second).unwrap();
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
    }

    #[test]
    fn compaction_preserves_the_exact_newest_view() {
        let component = ComponentIdentity::Membership(RecipeIdentity::new([8; 32]).unwrap());
        let (first, first_pack) =
            packed(sealed(component, &[(1, Some(b"one")), (2, Some(b"two"))]));
        let (second, second_pack) = packed(sealed(component, &[(1, None), (3, Some(b"three"))]));
        let one = append_component_delta(None, &first).unwrap();
        let two = append_component_delta(Some(&one), &second).unwrap();
        let artifacts = [
            (first.pack_hash, first_pack),
            (second.pack_hash, second_pack),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let (compacted, segment) = compact_component_stream(&two, &artifacts).unwrap();
        assert_eq!(compacted.segment_count, 1);
        let compacted_pack_hash = decode_component_stream(&compacted).unwrap()[0].pack_hash;
        let compacted_artifacts = [(compacted_pack_hash, segment.bytes)].into_iter().collect();
        for stable_key in [key(1), key(2), key(3), key(4)] {
            assert_eq!(
                resolve_component_record(&two, &artifacts, stable_key).unwrap(),
                resolve_component_record(&compacted, &compacted_artifacts, stable_key).unwrap()
            );
        }
    }

    #[test]
    fn directory_fanout_bounds_pages_for_seventy_thousand_segments() {
        let segments = (1_u64..=70_000)
            .map(|sequence| ComponentSegmentDescriptor {
                sequence,
                pack_hash: *blake3::hash(&sequence.to_le_bytes()).as_bytes(),
                pack_offset: 0,
                segment_hash: *blake3::hash(&sequence.to_be_bytes()).as_bytes(),
                encoded_bytes: 100,
                logical_bytes: 80,
                records: 1,
            })
            .collect::<Vec<_>>();
        let directory = build_component_stream(ComponentIdentity::DocumentHead, &segments).unwrap();
        assert_eq!(decode_component_stream(&directory).unwrap(), segments);
        assert!(
            directory
                .pages
                .iter()
                .all(|page| page.bytes.len() < 32 * 1024)
        );
        assert!(directory.pages.len() < 300);
    }

    #[test]
    fn append_path_copies_only_the_logarithmic_right_spine() {
        let component = ComponentIdentity::DocumentHead;
        let segments = (1_u64..=65_536)
            .map(|sequence| ComponentSegmentDescriptor {
                sequence,
                pack_hash: *blake3::hash(&sequence.to_le_bytes()).as_bytes(),
                pack_offset: 0,
                segment_hash: *blake3::hash(&sequence.to_be_bytes()).as_bytes(),
                encoded_bytes: 100,
                logical_bytes: 80,
                records: 1,
            })
            .collect::<Vec<_>>();
        let previous = build_component_stream(component, &segments).unwrap();
        let pages = previous
            .pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone()))
            .collect::<BTreeMap<_, _>>();
        let (delta, _) = packed(sealed(component, &[(1, Some(b"next"))]));

        let appended = append_component_stream(
            Some(previous.root()),
            |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
            &delta,
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
        let component = ComponentIdentity::ProjectedState;
        let (segment, mut corrupted) = packed(sealed(component, &[(1, Some(b"state"))]));
        let directory = append_component_delta(None, &segment).unwrap();
        corrupted[0] ^= 1;
        let artifacts = [(segment.pack_hash, corrupted)].into_iter().collect();
        assert!(matches!(
            resolve_component_record(&directory, &artifacts, key(1)),
            Err(IndexError::Integrity)
        ));
    }
}
