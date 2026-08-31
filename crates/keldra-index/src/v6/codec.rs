use std::collections::BTreeMap;

use crate::IndexError;

use super::{
    ComponentIdentity, ComponentRoot, LogicalFieldBinding, LogicalProjectionBinding,
    ProjectionCurrent, ProjectionGeneration, ProjectionPartitionIdentity,
    ProjectionQueryStreamRoot, RecipeIdentity,
};

const DIRECTORY_MAGIC: &[u8; 8] = b"K6CDIR01";
const GENERATION_MAGIC: &[u8; 8] = b"K6PGEN01";
const EMPTY_COMPONENT_DIRECTORY_DOMAIN: &[u8] = b"keldra.index.v6.empty-component-directory/v1";
const CURRENT_MAGIC: &[u8; 8] = b"K6PCUR01";
const BINDING_MAGIC: &[u8; 8] = b"K6BIND01";
const DIRECTORY_FORMAT: u16 = 1;
const GENERATION_FORMAT: u16 = 2;
const CURRENT_FORMAT: u16 = 1;
const BINDING_FORMAT: u16 = 1;
pub const COMPONENT_DIRECTORY_FANOUT: usize = 256;
pub const MAX_LOGICAL_BINDING_FIELDS: usize = 65_536;
pub const MAX_INHERITED_PROJECTION_PARTITIONS: usize = 4_096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedComponentDirectoryPage {
    pub hash: [u8; 32],
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentDirectory {
    pub root_hash: [u8; 32],
    pub root_count: u64,
    pub pages: Vec<EncodedComponentDirectoryPage>,
}

/// One content-addressed generation record and the independently publishable
/// bounded directory pages it names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedProjectionGeneration {
    pub hash: [u8; 32],
    pub bytes: Vec<u8>,
    pub component_directory: ComponentDirectory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionGenerationHeader {
    pub partition: ProjectionPartitionIdentity,
    pub physical_catalog_generation: [u8; 32],
    pub revision: u64,
    pub next_offset: u64,
    pub through_atomic_position: u64,
    pub query_stream_root: ProjectionQueryStreamRoot,
    pub inherited_partitions: Vec<super::ProjectionGenerationReference>,
    pub component_directory_root_hash: [u8; 32],
    pub component_root_count: u64,
    pub previous_generation_hash: Option<[u8; 32]>,
}

pub fn encode_projection_current(current: ProjectionCurrent) -> Result<Vec<u8>, IndexError> {
    current.partition.validate()?;
    if current.generation_hash == [0; 32] || current.generation_revision == 0 {
        return Err(IndexError::InvalidDefinition(
            "projection current is invalid".into(),
        ));
    }
    let mut out = Vec::new();
    out.extend_from_slice(CURRENT_MAGIC);
    put_u16(&mut out, CURRENT_FORMAT);
    put_partition(&mut out, current.partition);
    out.extend_from_slice(&current.physical_catalog_generation);
    out.extend_from_slice(&current.generation_hash);
    put_u64(&mut out, current.generation_revision);
    put_u64(&mut out, current.next_offset);
    put_u64(&mut out, current.through_atomic_position);
    append_integrity(&mut out);
    Ok(out)
}

pub fn decode_projection_current(bytes: &[u8]) -> Result<ProjectionCurrent, IndexError> {
    let payload = verify_integrity(bytes)?;
    let mut input = Decoder::new(payload);
    input.expect(CURRENT_MAGIC)?;
    if input.u16()? != CURRENT_FORMAT {
        return Err(IndexError::InvalidFormat(
            "projection current format is unsupported",
        ));
    }
    let current = ProjectionCurrent {
        partition: input.partition()?,
        physical_catalog_generation: input.array_32()?,
        generation_hash: input.array_32()?,
        generation_revision: input.u64()?,
        next_offset: input.u64()?,
        through_atomic_position: input.u64()?,
    };
    input.finish()?;
    encode_projection_current(current)?;
    Ok(current)
}

pub fn encode_projection_generation(
    generation: &ProjectionGeneration,
) -> Result<EncodedProjectionGeneration, IndexError> {
    generation.validate()?;
    let component_directory = build_component_directory(&generation.roots)?;
    let mut out = Vec::new();
    out.extend_from_slice(GENERATION_MAGIC);
    put_u16(&mut out, GENERATION_FORMAT);
    put_partition(&mut out, generation.partition);
    out.extend_from_slice(&generation.physical_catalog_generation);
    put_u64(&mut out, generation.revision);
    put_u64(&mut out, generation.next_offset);
    put_u64(&mut out, generation.through_atomic_position);
    put_query_stream_root(&mut out, generation.query_stream_root);
    put_u32(
        &mut out,
        u32::try_from(generation.inherited_partitions.len())
            .map_err(|_| IndexError::OffsetOverflow)?,
    );
    for inherited in &generation.inherited_partitions {
        put_generation_reference(&mut out, *inherited);
    }
    out.extend_from_slice(&component_directory.root_hash);
    put_u64(&mut out, component_directory.root_count);
    put_optional_hash(&mut out, generation.previous_generation_hash);
    append_integrity(&mut out);
    Ok(EncodedProjectionGeneration {
        hash: *blake3::hash(&out).as_bytes(),
        bytes: out,
        component_directory,
    })
}

pub fn decode_projection_generation(
    bytes: &[u8],
    component_directory: &ComponentDirectory,
) -> Result<ProjectionGeneration, IndexError> {
    let header = decode_projection_generation_header(bytes)?;
    if header.component_directory_root_hash != component_directory.root_hash
        || header.component_root_count != component_directory.root_count
    {
        return Err(IndexError::Integrity);
    }
    let generation = ProjectionGeneration {
        partition: header.partition,
        physical_catalog_generation: header.physical_catalog_generation,
        revision: header.revision,
        next_offset: header.next_offset,
        through_atomic_position: header.through_atomic_position,
        query_stream_root: header.query_stream_root,
        roots: decode_component_directory(component_directory)?,
        inherited_partitions: header.inherited_partitions,
        previous_generation_hash: header.previous_generation_hash,
    };
    generation.validate()?;
    Ok(generation)
}

pub fn decode_projection_generation_header(
    bytes: &[u8],
) -> Result<ProjectionGenerationHeader, IndexError> {
    let payload = verify_integrity(bytes)?;
    let mut input = Decoder::new(payload);
    input.expect(GENERATION_MAGIC)?;
    if input.u16()? != GENERATION_FORMAT {
        return Err(IndexError::InvalidFormat(
            "projection generation format is unsupported",
        ));
    }
    let partition = input.partition()?;
    let physical_catalog_generation = input.array_32()?;
    let revision = input.u64()?;
    let next_offset = input.u64()?;
    let through_atomic_position = input.u64()?;
    let query_stream_root = input.query_stream_root()?;
    let inherited_count = input.u32()? as usize;
    if inherited_count > MAX_INHERITED_PROJECTION_PARTITIONS {
        return Err(IndexError::InvalidFormat(
            "projection generation predecessor count is unbounded",
        ));
    }
    let mut inherited_partitions = Vec::with_capacity(inherited_count);
    for _ in 0..inherited_count {
        inherited_partitions.push(input.generation_reference()?);
    }
    let directory_hash = input.array_32()?;
    let directory_count = input.u64()?;
    let previous_generation_hash = input.optional_hash()?;
    input.finish()?;
    if directory_hash == [0; 32]
        || (directory_count == 0 && directory_hash != empty_component_directory_hash())
    {
        return Err(IndexError::InvalidFormat(
            "projection generation header contains a zero identity",
        ));
    }
    query_stream_root.validate_at(next_offset, through_atomic_position)?;
    Ok(ProjectionGenerationHeader {
        partition,
        physical_catalog_generation,
        revision,
        next_offset,
        through_atomic_position,
        query_stream_root,
        inherited_partitions,
        component_directory_root_hash: directory_hash,
        component_root_count: directory_count,
        previous_generation_hash,
    })
}

/// Open the exact generation named by one already pinned partition current
/// pointer without loading its component-directory pages or stream artifacts.
/// The returned directory root/count can be resolved lazily per component.
pub fn decode_current_projection_generation_header(
    current: ProjectionCurrent,
    bytes: &[u8],
) -> Result<ProjectionGenerationHeader, IndexError> {
    if *blake3::hash(bytes).as_bytes() != current.generation_hash {
        return Err(IndexError::Integrity);
    }
    let header = decode_projection_generation_header(bytes)?;
    if header.partition != current.partition
        || header.physical_catalog_generation != current.physical_catalog_generation
        || header.revision != current.generation_revision
        || header.next_offset != current.next_offset
        || header.through_atomic_position != current.through_atomic_position
    {
        return Err(IndexError::Integrity);
    }
    Ok(header)
}

pub fn resolve_component_root(
    root_hash: [u8; 32],
    root_count: u64,
    component: ComponentIdentity,
    mut load_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<Option<ComponentRoot>, IndexError> {
    if root_count == 0 && root_hash == empty_component_directory_hash() {
        return Ok(None);
    }
    if root_hash == [0; 32] || root_count == 0 {
        return Err(IndexError::InvalidDefinition(
            "component directory lookup root is invalid".into(),
        ));
    }
    resolve_component_subtree(root_hash, root_count, component, &mut load_page)
}

pub fn empty_component_directory_hash() -> [u8; 32] {
    *blake3::hash(EMPTY_COMPONENT_DIRECTORY_DOMAIN).as_bytes()
}

/// Child page hashes named by one verified component-directory page.
pub fn component_directory_child_hashes(bytes: &[u8]) -> Result<Vec<[u8; 32]>, IndexError> {
    Ok(match decode_directory_page(bytes)? {
        DirectoryPage::Leaf(_) => Vec::new(),
        DirectoryPage::Branch(children) => children.into_iter().map(|child| child.hash).collect(),
    })
}

fn resolve_component_subtree(
    hash: [u8; 32],
    root_count: u64,
    component: ComponentIdentity,
    load_page: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<Option<ComponentRoot>, IndexError> {
    let bytes = load_page(hash)?;
    if hash != *blake3::hash(&bytes).as_bytes() {
        return Err(IndexError::Integrity);
    }
    match decode_directory_page(&bytes)? {
        DirectoryPage::Leaf(roots) => {
            if roots.len() as u64 != root_count {
                return Err(IndexError::Integrity);
            }
            Ok(roots
                .binary_search_by_key(&component, |root| root.component)
                .ok()
                .map(|index| roots[index].clone()))
        }
        DirectoryPage::Branch(children) => {
            if children
                .iter()
                .try_fold(0_u64, |total, child| total.checked_add(child.root_count))
                != Some(root_count)
            {
                return Err(IndexError::Integrity);
            }
            let Some(child) = children
                .iter()
                .find(|child| child.minimum <= component && component <= child.maximum)
            else {
                return Ok(None);
            };
            resolve_component_subtree(child.hash, child.root_count, component, load_page)
        }
    }
}

pub fn encode_logical_projection_binding(
    binding: &LogicalProjectionBinding,
    generation: &ProjectionGeneration,
) -> Result<Vec<u8>, IndexError> {
    binding.validate_against(generation)?;
    if binding.fields.len() > MAX_LOGICAL_BINDING_FIELDS {
        return Err(IndexError::InvalidDefinition(
            "logical projection binding has too many fields".into(),
        ));
    }
    let mut out = Vec::new();
    out.extend_from_slice(BINDING_MAGIC);
    put_u16(&mut out, BINDING_FORMAT);
    put_u64(&mut out, binding.logical_index_id);
    put_u64(&mut out, binding.logical_definition_version);
    out.extend_from_slice(&binding.family_id);
    out.extend_from_slice(&binding.physical_catalog_generation);
    out.extend_from_slice(&binding.membership.bytes());
    put_u32(&mut out, binding.fields.len() as u32);
    for field in &binding.fields {
        put_u32(&mut out, field.public_field_id);
        put_bytes(&mut out, field.public_name.as_bytes())?;
        out.extend_from_slice(&field.recipe.bytes());
    }
    append_integrity(&mut out);
    Ok(out)
}

pub fn decode_logical_projection_binding(
    bytes: &[u8],
    generation: &ProjectionGeneration,
) -> Result<LogicalProjectionBinding, IndexError> {
    let payload = verify_integrity(bytes)?;
    let mut input = Decoder::new(payload);
    input.expect(BINDING_MAGIC)?;
    if input.u16()? != BINDING_FORMAT {
        return Err(IndexError::InvalidFormat(
            "logical projection binding format is unsupported",
        ));
    }
    let logical_index_id = input.u64()?;
    let logical_definition_version = input.u64()?;
    let family_id = input.array_32()?;
    let physical_catalog_generation = input.array_32()?;
    let membership = RecipeIdentity::new(input.array_32()?)?;
    let field_count = input.u32()? as usize;
    if field_count > MAX_LOGICAL_BINDING_FIELDS {
        return Err(IndexError::InvalidFormat(
            "logical projection binding has too many fields",
        ));
    }
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        fields.push(LogicalFieldBinding {
            public_field_id: input.u32()?,
            public_name: input.string()?,
            recipe: RecipeIdentity::new(input.array_32()?)?,
        });
    }
    input.finish()?;
    let binding = LogicalProjectionBinding {
        logical_index_id,
        logical_definition_version,
        family_id,
        physical_catalog_generation,
        membership,
        fields,
    };
    binding.validate_against(generation)?;
    Ok(binding)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DirectoryPage {
    Leaf(Vec<ComponentRoot>),
    Branch(Vec<DirectoryChild>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirectoryChild {
    minimum: ComponentIdentity,
    maximum: ComponentIdentity,
    hash: [u8; 32],
    root_count: u64,
}

pub fn build_component_directory(
    roots: &[ComponentRoot],
) -> Result<ComponentDirectory, IndexError> {
    if roots.is_empty() {
        return Ok(ComponentDirectory {
            root_hash: empty_component_directory_hash(),
            root_count: 0,
            pages: Vec::new(),
        });
    }
    validate_roots(roots)?;
    let mut pages = Vec::new();
    let mut level = Vec::new();
    for chunk in roots.chunks(COMPONENT_DIRECTORY_FANOUT) {
        let page = DirectoryPage::Leaf(chunk.to_vec());
        let encoded = encode_directory_page(&page)?;
        level.push(DirectoryChild {
            minimum: chunk.first().expect("nonempty leaf").component,
            maximum: chunk.last().expect("nonempty leaf").component,
            hash: encoded.hash,
            root_count: chunk.len() as u64,
        });
        pages.push(encoded);
    }
    while level.len() > 1 {
        let mut parent = Vec::new();
        for chunk in level.chunks(COMPONENT_DIRECTORY_FANOUT) {
            let page = DirectoryPage::Branch(chunk.to_vec());
            let encoded = encode_directory_page(&page)?;
            parent.push(DirectoryChild {
                minimum: chunk.first().expect("nonempty branch").minimum,
                maximum: chunk.last().expect("nonempty branch").maximum,
                hash: encoded.hash,
                root_count: chunk.iter().try_fold(0_u64, |total, child| {
                    total
                        .checked_add(child.root_count)
                        .ok_or(IndexError::OffsetOverflow)
                })?,
            });
            pages.push(encoded);
        }
        level = parent;
    }
    let root = level
        .into_iter()
        .next()
        .expect("validated roots are nonempty");
    Ok(ComponentDirectory {
        root_hash: root.hash,
        root_count: root.root_count,
        pages,
    })
}

pub fn decode_component_directory(
    directory: &ComponentDirectory,
) -> Result<Vec<ComponentRoot>, IndexError> {
    if directory.root_count == 0 {
        return if directory.root_hash == empty_component_directory_hash()
            && directory.pages.is_empty()
        {
            Ok(Vec::new())
        } else {
            Err(IndexError::Integrity)
        };
    }
    if directory.root_hash == [0; 32] || directory.root_count == 0 {
        return Err(IndexError::InvalidFormat(
            "component directory root is invalid",
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
    let mut roots = Vec::new();
    decode_directory_subtree(directory.root_hash, &pages, &mut roots)?;
    if roots.len() as u64 != directory.root_count {
        return Err(IndexError::Integrity);
    }
    validate_roots(&roots)?;
    Ok(roots)
}

fn decode_directory_subtree(
    hash: [u8; 32],
    pages: &BTreeMap<[u8; 32], &[u8]>,
    roots: &mut Vec<ComponentRoot>,
) -> Result<(), IndexError> {
    let bytes = pages.get(&hash).ok_or(IndexError::Integrity)?;
    match decode_directory_page(bytes)? {
        DirectoryPage::Leaf(mut page_roots) => roots.append(&mut page_roots),
        DirectoryPage::Branch(children) => {
            for child in children {
                let before = roots.len();
                decode_directory_subtree(child.hash, pages, roots)?;
                let decoded = &roots[before..];
                if decoded.len() as u64 != child.root_count
                    || decoded.first().map(|root| root.component) != Some(child.minimum)
                    || decoded.last().map(|root| root.component) != Some(child.maximum)
                {
                    return Err(IndexError::Integrity);
                }
            }
        }
    }
    Ok(())
}

fn encode_directory_page(
    page: &DirectoryPage,
) -> Result<EncodedComponentDirectoryPage, IndexError> {
    let mut out = Vec::new();
    out.extend_from_slice(DIRECTORY_MAGIC);
    put_u16(&mut out, DIRECTORY_FORMAT);
    match page {
        DirectoryPage::Leaf(roots) => {
            validate_roots(roots)?;
            out.push(1);
            put_u32(&mut out, roots.len() as u32);
            for root in roots {
                put_component(&mut out, root.component);
                out.extend_from_slice(&root.stream_root_hash);
                put_u64(&mut out, root.segment_count);
                put_u64(&mut out, root.first_sequence);
                put_u64(&mut out, root.last_sequence);
                put_u64(&mut out, root.encoded_bytes);
                put_u64(&mut out, root.logical_bytes);
                put_u64(&mut out, root.directory_bytes);
            }
        }
        DirectoryPage::Branch(children) => {
            validate_children(children)?;
            out.push(2);
            put_u32(&mut out, children.len() as u32);
            for child in children {
                put_component(&mut out, child.minimum);
                put_component(&mut out, child.maximum);
                out.extend_from_slice(&child.hash);
                put_u64(&mut out, child.root_count);
            }
        }
    }
    append_integrity(&mut out);
    Ok(EncodedComponentDirectoryPage {
        hash: *blake3::hash(&out).as_bytes(),
        bytes: out,
    })
}

fn decode_directory_page(bytes: &[u8]) -> Result<DirectoryPage, IndexError> {
    let payload = verify_integrity(bytes)?;
    let mut input = Decoder::new(payload);
    input.expect(DIRECTORY_MAGIC)?;
    if input.u16()? != DIRECTORY_FORMAT {
        return Err(IndexError::InvalidFormat(
            "component directory format is unsupported",
        ));
    }
    let kind = input.byte()?;
    let count = input.u32()? as usize;
    if count == 0 || count > COMPONENT_DIRECTORY_FANOUT {
        return Err(IndexError::InvalidFormat(
            "component directory page is unbounded",
        ));
    }
    let page = match kind {
        1 => {
            let mut roots = Vec::with_capacity(count);
            for _ in 0..count {
                roots.push(ComponentRoot::with_sequences(
                    input.component()?,
                    input.array_32()?,
                    input.u64()?,
                    input.u64()?,
                    input.u64()?,
                    input.u64()?,
                    input.u64()?,
                    input.u64()?,
                )?);
            }
            validate_roots(&roots)?;
            DirectoryPage::Leaf(roots)
        }
        2 => {
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(DirectoryChild {
                    minimum: input.component()?,
                    maximum: input.component()?,
                    hash: input.array_32()?,
                    root_count: input.u64()?,
                });
            }
            validate_children(&children)?;
            DirectoryPage::Branch(children)
        }
        _ => {
            return Err(IndexError::InvalidFormat(
                "component directory page kind is unknown",
            ));
        }
    };
    input.finish()?;
    Ok(page)
}

fn validate_roots(roots: &[ComponentRoot]) -> Result<(), IndexError> {
    if roots.len() > u32::MAX as usize
        || roots
            .windows(2)
            .any(|pair| pair[0].component >= pair[1].component)
    {
        return Err(IndexError::InvalidDefinition(
            "component roots must be nonempty, unique, and canonical".into(),
        ));
    }
    for root in roots {
        ComponentRoot::with_sequences(
            root.component,
            root.stream_root_hash,
            root.segment_count,
            root.first_sequence,
            root.last_sequence,
            root.encoded_bytes,
            root.logical_bytes,
            root.directory_bytes,
        )?;
    }
    Ok(())
}

fn validate_children(children: &[DirectoryChild]) -> Result<(), IndexError> {
    if children.is_empty()
        || children.len() > COMPONENT_DIRECTORY_FANOUT
        || children.iter().any(|child| {
            child.minimum > child.maximum || child.hash == [0; 32] || child.root_count == 0
        })
        || children
            .windows(2)
            .any(|pair| pair[0].maximum >= pair[1].minimum)
    {
        return Err(IndexError::InvalidDefinition(
            "component directory children are invalid".into(),
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

fn append_integrity(out: &mut Vec<u8>) {
    let hash = *blake3::hash(out).as_bytes();
    out.extend_from_slice(&hash);
}

fn verify_integrity(bytes: &[u8]) -> Result<&[u8], IndexError> {
    let split = bytes
        .len()
        .checked_sub(32)
        .ok_or(IndexError::UnexpectedEof {
            expected: 32,
            actual: bytes.len() as u64,
        })?;
    let (payload, expected) = bytes.split_at(split);
    if blake3::hash(payload).as_bytes() != expected {
        return Err(IndexError::Integrity);
    }
    Ok(payload)
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), IndexError> {
    put_u32(
        out,
        u32::try_from(bytes.len()).map_err(|_| IndexError::OffsetOverflow)?,
    );
    out.extend_from_slice(bytes);
    Ok(())
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn put_partition(out: &mut Vec<u8>, partition: ProjectionPartitionIdentity) {
    out.extend_from_slice(&partition.family_id);
    put_u64(out, partition.source_node);
    out.extend_from_slice(&partition.source_epoch);
    put_u64(out, partition.producer_node);
    put_u64(out, partition.placement_term);
    put_u64(out, partition.placement_index);
}
fn put_query_stream_root(out: &mut Vec<u8>, root: ProjectionQueryStreamRoot) {
    out.extend_from_slice(&root.stream_root_hash);
    put_u64(out, root.run_count);
    put_u64(out, root.first_sequence);
    put_u64(out, root.last_sequence);
    put_u64(out, root.source_start_offset);
    put_u64(out, root.next_offset);
    put_u64(out, root.through_atomic_position);
}
fn put_generation_reference(out: &mut Vec<u8>, reference: super::ProjectionGenerationReference) {
    put_partition(out, reference.partition);
    out.extend_from_slice(&reference.physical_catalog_generation);
    out.extend_from_slice(&reference.generation_hash);
    put_u64(out, reference.generation_revision);
    put_u64(out, reference.next_offset);
    put_u64(out, reference.through_atomic_position);
}
fn put_optional_hash(out: &mut Vec<u8>, value: Option<[u8; 32]>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value);
        }
        None => out.push(0),
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
        if self.take(expected.len())? != expected {
            Err(IndexError::InvalidFormat("index magic is invalid"))
        } else {
            Ok(())
        }
    }
    fn byte(&mut self) -> Result<u8, IndexError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, IndexError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, IndexError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, IndexError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn array_32(&mut self) -> Result<[u8; 32], IndexError> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn partition(&mut self) -> Result<ProjectionPartitionIdentity, IndexError> {
        ProjectionPartitionIdentity::new(
            self.array_32()?,
            self.u64()?,
            self.array_32()?,
            self.u64()?,
            self.u64()?,
            self.u64()?,
        )
    }
    fn query_stream_root(&mut self) -> Result<ProjectionQueryStreamRoot, IndexError> {
        Ok(ProjectionQueryStreamRoot {
            stream_root_hash: self.array_32()?,
            run_count: self.u64()?,
            first_sequence: self.u64()?,
            last_sequence: self.u64()?,
            source_start_offset: self.u64()?,
            next_offset: self.u64()?,
            through_atomic_position: self.u64()?,
        })
    }
    fn generation_reference(&mut self) -> Result<super::ProjectionGenerationReference, IndexError> {
        let reference = super::ProjectionGenerationReference {
            partition: self.partition()?,
            physical_catalog_generation: self.array_32()?,
            generation_hash: self.array_32()?,
            generation_revision: self.u64()?,
            next_offset: self.u64()?,
            through_atomic_position: self.u64()?,
        };
        reference.validate()?;
        Ok(reference)
    }
    fn optional_hash(&mut self) -> Result<Option<[u8; 32]>, IndexError> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.array_32()?)),
            _ => Err(IndexError::Decode("optional hash is invalid".into())),
        }
    }
    fn string(&mut self) -> Result<String, IndexError> {
        let length = self.u32()? as usize;
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|_| IndexError::Decode("string is not UTF-8".into()))
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
            _ => Err(IndexError::Decode("component identity is unknown".into())),
        }
    }
    fn finish(self) -> Result<(), IndexError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(IndexError::Decode("trailing index bytes".into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recipe(byte: u8) -> RecipeIdentity {
        RecipeIdentity::new([byte; 32]).unwrap()
    }

    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([9; 32], 1, [2; 32], 1, 3, 4).unwrap()
    }

    fn generation() -> ProjectionGeneration {
        ProjectionGeneration::initial(
            partition(),
            [7; 32],
            7,
            6,
            vec![
                ComponentRoot::new(ComponentIdentity::DocumentHead, [1; 32], 1, 10, 9, 1).unwrap(),
                ComponentRoot::new(ComponentIdentity::SourceRecords, [4; 32], 1, 10, 9, 1).unwrap(),
                ComponentRoot::new(
                    ComponentIdentity::Membership(recipe(2)),
                    [2; 32],
                    1,
                    10,
                    9,
                    1,
                )
                .unwrap(),
                ComponentRoot::new(ComponentIdentity::Field(recipe(3)), [3; 32], 1, 10, 9, 1)
                    .unwrap(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn large_component_catalog_uses_bounded_merkle_pages() {
        let roots = (1_u32..=70_000)
            .map(|ordinal| {
                let mut identity = [0_u8; 32];
                identity[..4].copy_from_slice(&ordinal.to_be_bytes());
                let recipe = RecipeIdentity::new(identity).unwrap();
                ComponentRoot::new(
                    ComponentIdentity::Field(recipe),
                    *blake3::hash(&identity).as_bytes(),
                    1,
                    64,
                    48,
                    16,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let directory = build_component_directory(&roots).unwrap();
        assert!(directory.pages.len() > roots.len() / COMPONENT_DIRECTORY_FANOUT);
        assert!(
            directory
                .pages
                .iter()
                .all(|page| page.bytes.len() < 32 * 1024)
        );
        assert_eq!(decode_component_directory(&directory).unwrap(), roots);
        let pages = directory
            .pages
            .iter()
            .map(|page| (page.hash, page.bytes.clone()))
            .collect::<BTreeMap<_, _>>();
        let root_children =
            component_directory_child_hashes(pages.get(&directory.root_hash).expect("root page"))
                .unwrap();
        assert!(!root_children.is_empty());
        assert!(root_children.iter().all(|hash| pages.contains_key(hash)));
        let target = roots[63_777].component;
        assert_eq!(
            resolve_component_root(directory.root_hash, directory.root_count, target, |hash| {
                pages.get(&hash).cloned().ok_or(IndexError::Integrity)
            },)
            .unwrap(),
            Some(roots[63_777].clone())
        );
    }

    #[test]
    fn directory_rejects_missing_or_corrupt_pages() {
        let roots = [
            ComponentRoot::new(ComponentIdentity::DocumentHead, [1; 32], 1, 10, 10, 1).unwrap(),
            ComponentRoot::new(ComponentIdentity::Field(recipe(2)), [2; 32], 1, 10, 10, 1).unwrap(),
        ];
        let mut directory = build_component_directory(&roots).unwrap();
        directory.pages[0].bytes[4] ^= 1;
        assert_eq!(
            decode_component_directory(&directory),
            Err(IndexError::Integrity)
        );
    }

    #[test]
    fn generation_record_round_trips_through_its_exact_directory() {
        let generation = generation();
        let encoded = encode_projection_generation(&generation).unwrap();
        assert_eq!(
            decode_projection_generation(&encoded.bytes, &encoded.component_directory).unwrap(),
            generation
        );
        let header = decode_projection_generation_header(&encoded.bytes).unwrap();
        assert_eq!(header.partition, generation.partition);
        assert_eq!(header.revision, generation.revision);
        assert_eq!(
            header.component_directory_root_hash,
            encoded.component_directory.root_hash
        );
        assert_eq!(
            header.component_root_count,
            encoded.component_directory.root_count
        );

        let mut wrong_directory = encoded.component_directory.clone();
        wrong_directory.root_count += 1;
        assert_eq!(
            decode_projection_generation(&encoded.bytes, &wrong_directory),
            Err(IndexError::Integrity)
        );
    }

    #[test]
    fn current_pointer_round_trips_and_binds_one_exact_generation() {
        let generation = generation();
        let encoded_generation = encode_projection_generation(&generation).unwrap();
        let current = ProjectionCurrent::new(encoded_generation.hash, &generation).unwrap();
        let bytes = encode_projection_current(current).unwrap();
        let decoded = decode_projection_current(&bytes).unwrap();
        assert_eq!(decoded, current);
        decoded.validate_against(&generation).unwrap();
        let header =
            decode_current_projection_generation_header(decoded, &encoded_generation.bytes)
                .unwrap();
        assert_eq!(header.component_root_count, generation.roots.len() as u64);

        let mut wrong_generation_bytes = encoded_generation.bytes.clone();
        wrong_generation_bytes[20] ^= 1;
        assert!(matches!(
            decode_current_projection_generation_header(decoded, &wrong_generation_bytes),
            Err(IndexError::Integrity)
        ));

        let advanced = generation
            .advance(encoded_generation.hash, [7; 32], 8, 7, Vec::new())
            .unwrap();
        assert!(decoded.validate_against(&advanced).is_err());

        let mut corrupt = bytes;
        corrupt[20] ^= 1;
        assert_eq!(
            decode_projection_current(&corrupt),
            Err(IndexError::Integrity)
        );
    }

    #[test]
    fn generation_record_size_does_not_grow_with_component_count() {
        let mut roots = vec![
            ComponentRoot::new(ComponentIdentity::DocumentHead, [8; 32], 1, 64, 48, 16).unwrap(),
            ComponentRoot::new(ComponentIdentity::SourceRecords, [9; 32], 1, 64, 48, 16).unwrap(),
        ];
        let baseline = encode_projection_generation(
            &ProjectionGeneration::initial(partition(), [7; 32], 7, 6, roots.clone()).unwrap(),
        )
        .unwrap();
        roots.extend((1_u32..=70_000).map(|ordinal| {
            let mut identity = [0_u8; 32];
            identity[..4].copy_from_slice(&ordinal.to_be_bytes());
            ComponentRoot::new(
                ComponentIdentity::Field(RecipeIdentity::new(identity).unwrap()),
                *blake3::hash(&identity).as_bytes(),
                1,
                64,
                48,
                16,
            )
            .unwrap()
        }));
        let generation = ProjectionGeneration::initial(partition(), [7; 32], 7, 6, roots).unwrap();
        let encoded = encode_projection_generation(&generation).unwrap();
        assert_eq!(encoded.bytes.len(), baseline.bytes.len());
        assert!(encoded.bytes.len() < 512);
        assert_eq!(encoded.component_directory.root_count, 70_002);
    }

    #[test]
    fn logical_binding_round_trips_and_follows_only_ready_family_generations() {
        let generation = generation();
        let binding = LogicalProjectionBinding {
            logical_index_id: 44,
            logical_definition_version: 8,
            family_id: generation.partition.family_id,
            physical_catalog_generation: generation.physical_catalog_generation,
            membership: recipe(2),
            fields: vec![LogicalFieldBinding {
                public_field_id: 7,
                public_name: "renamed".into(),
                recipe: recipe(3),
            }],
        };
        let bytes = encode_logical_projection_binding(&binding, &generation).unwrap();
        assert_eq!(
            decode_logical_projection_binding(&bytes, &generation).unwrap(),
            binding
        );

        let advanced = generation
            .advance([7; 32], [7; 32], 8, 7, Vec::new())
            .unwrap();
        assert_eq!(
            decode_logical_projection_binding(&bytes, &advanced).unwrap(),
            binding
        );

        let mut wrong_family = advanced.clone();
        wrong_family.partition.family_id = [42; 32];
        assert!(decode_logical_projection_binding(&bytes, &wrong_family).is_err());

        let mut wrong_catalog = binding;
        wrong_catalog.physical_catalog_generation = [42; 32];
        assert!(encode_logical_projection_binding(&wrong_catalog, &advanced).is_err());
    }
}
