use std::collections::BTreeMap;

use crate::IndexError;

use super::buffer::seal_component;
use super::{
    ComponentIdentity, ComponentRoot, RecipeIdentity, SealedComponentDelta, StableDocumentKey,
    decode_component_delta_segment,
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
    pub artifact_hash: [u8; 32],
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub records: u64,
}

impl ComponentSegmentDescriptor {
    fn validate(&self) -> Result<(), IndexError> {
        if self.sequence == 0
            || self.artifact_hash == [0; 32]
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
    pub pages: Vec<EncodedComponentStreamPage>,
}

impl ComponentStreamDirectory {
    pub fn component_root(&self) -> Result<ComponentRoot, IndexError> {
        decode_component_stream(self)?;
        let directory_bytes = self.pages.iter().try_fold(0_u64, |total, page| {
            total
                .checked_add(page.bytes.len() as u64)
                .ok_or(IndexError::OffsetOverflow)
        })?;
        ComponentRoot::new(
            self.component,
            self.root_hash,
            self.encoded_bytes
                .checked_add(directory_bytes)
                .ok_or(IndexError::OffsetOverflow)?,
            self.logical_bytes,
        )
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
        level.push(Child::from_segments(chunk, encoded.hash)?);
        pages.push(encoded);
    }
    while level.len() > 1 {
        let mut parent = Vec::new();
        for chunk in level.chunks(COMPONENT_STREAM_DIRECTORY_FANOUT) {
            let encoded = encode_page(component, &Page::Branch(chunk.to_vec()))?;
            parent.push(Child::from_children(chunk, encoded.hash)?);
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
        pages,
    })
}

pub fn append_component_delta(
    previous: Option<&ComponentStreamDirectory>,
    delta: &SealedComponentDelta,
) -> Result<ComponentStreamDirectory, IndexError> {
    let decoded = decode_component_delta_segment(&delta.bytes)?;
    if decoded.component != delta.root.component
        || delta.root.artifact_hash != *blake3::hash(&delta.bytes).as_bytes()
        || delta.root.encoded_bytes != delta.bytes.len() as u64
        || delta.records != decoded.records.len() as u64
    {
        return Err(IndexError::Integrity);
    }
    let mut segments = match previous {
        Some(directory) => {
            if directory.component != delta.root.component {
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
    segments.push(ComponentSegmentDescriptor {
        sequence,
        artifact_hash: delta.root.artifact_hash,
        encoded_bytes: delta.root.encoded_bytes,
        logical_bytes: delta.root.logical_bytes,
        records: delta.records,
    });
    build_component_stream(delta.root.component, &segments)
}

pub fn decode_component_stream(
    directory: &ComponentStreamDirectory,
) -> Result<Vec<ComponentSegmentDescriptor>, IndexError> {
    if directory.root_hash == [0; 32]
        || directory.segment_count == 0
        || directory.segment_count > MAX_COMPONENT_STREAM_SEGMENTS as u64
        || directory.encoded_bytes == 0
        || directory.logical_bytes == 0
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
    let totals = decode_subtree(
        directory.component,
        directory.root_hash,
        &pages,
        &mut segments,
    )?;
    if totals.segment_count != directory.segment_count
        || totals.encoded_bytes != directory.encoded_bytes
        || totals.logical_bytes != directory.logical_bytes
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
        let bytes = artifacts
            .get(&descriptor.artifact_hash)
            .ok_or(IndexError::Integrity)?;
        validate_artifact(directory.component, descriptor, bytes)?;
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
        let bytes = artifacts
            .get(&descriptor.artifact_hash)
            .ok_or(IndexError::Integrity)?;
        validate_artifact(directory.component, descriptor, bytes)?;
        for record in decode_component_delta_segment(bytes)?.records {
            records.insert(record.stable_key, record.replacement);
        }
    }
    let sealed = seal_component(directory.component, records)?;
    let compacted = append_component_delta(None, &sealed)?;
    Ok((compacted, sealed))
}

fn validate_artifact(
    component: ComponentIdentity,
    descriptor: &ComponentSegmentDescriptor,
    bytes: &[u8],
) -> Result<(), IndexError> {
    if *blake3::hash(bytes).as_bytes() != descriptor.artifact_hash
        || bytes.len() as u64 != descriptor.encoded_bytes
    {
        return Err(IndexError::Integrity);
    }
    let decoded = decode_component_delta_segment(bytes)?;
    if decoded.component != component || decoded.records.len() as u64 != descriptor.records {
        return Err(IndexError::Integrity);
    }
    Ok(())
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
        })
    }

    fn from_children(children: &[Child], hash: [u8; 32]) -> Result<Self, IndexError> {
        let first = children.first().expect("nonempty branch");
        let last = children.last().expect("nonempty branch");
        Ok(Self {
            first_sequence: first.first_sequence,
            last_sequence: last.last_sequence,
            hash,
            segment_count: sum_children(children, |child| child.segment_count)?,
            encoded_bytes: sum_children(children, |child| child.encoded_bytes)?,
            logical_bytes: sum_children(children, |child| child.logical_bytes)?,
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
                bytes.extend_from_slice(&segment.artifact_hash);
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
    segments: &mut Vec<ComponentSegmentDescriptor>,
) -> Result<Child, IndexError> {
    let bytes = pages.get(&hash).ok_or(IndexError::Integrity)?;
    match decode_page(component, bytes)? {
        Page::Leaf(page_segments) => {
            let child = Child::from_segments(&page_segments, hash)?;
            segments.extend(page_segments);
            Ok(child)
        }
        Page::Branch(children) => {
            let before = segments.len();
            for expected in &children {
                let actual = decode_subtree(component, expected.hash, pages, segments)?;
                if &actual != expected {
                    return Err(IndexError::Integrity);
                }
            }
            if segments.len() == before {
                return Err(IndexError::Integrity);
            }
            Child::from_children(&children, hash)
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
                    artifact_hash: input.array_32()?,
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

    #[test]
    fn newest_delta_wins_and_tombstones_hide_older_values() {
        let component = ComponentIdentity::Field(RecipeIdentity::new([7; 32]).unwrap());
        let first = sealed(component, &[(1, Some(b"old")), (2, Some(b"kept"))]);
        let second = sealed(component, &[(1, Some(b"new")), (2, None)]);
        let one = append_component_delta(None, &first).unwrap();
        let two = append_component_delta(Some(&one), &second).unwrap();
        let artifacts = [
            (first.root.artifact_hash, first.bytes.clone()),
            (second.root.artifact_hash, second.bytes.clone()),
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
        let first = sealed(component, &[(1, Some(b"one")), (2, Some(b"two"))]);
        let second = sealed(component, &[(1, None), (3, Some(b"three"))]);
        let one = append_component_delta(None, &first).unwrap();
        let two = append_component_delta(Some(&one), &second).unwrap();
        let artifacts = [
            (first.root.artifact_hash, first.bytes.clone()),
            (second.root.artifact_hash, second.bytes.clone()),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let (compacted, segment) = compact_component_stream(&two, &artifacts).unwrap();
        assert_eq!(compacted.segment_count, 1);
        let compacted_artifacts = [(segment.root.artifact_hash, segment.bytes)]
            .into_iter()
            .collect();
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
                artifact_hash: *blake3::hash(&sequence.to_le_bytes()).as_bytes(),
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
    fn artifact_identity_is_verified_before_reading() {
        let component = ComponentIdentity::ProjectedState;
        let segment = sealed(component, &[(1, Some(b"state"))]);
        let directory = append_component_delta(None, &segment).unwrap();
        let mut corrupted = segment.bytes;
        corrupted[0] ^= 1;
        let artifacts = [(segment.root.artifact_hash, corrupted)]
            .into_iter()
            .collect();
        assert!(matches!(
            resolve_component_record(&directory, &artifacts, key(1)),
            Err(IndexError::Integrity)
        ));
    }
}
