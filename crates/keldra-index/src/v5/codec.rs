use std::collections::BTreeMap;

use crate::IndexError;

use super::{
    CanonicalRecipeState, ComponentIdentity, ComponentRoot, DocumentHead, LogicalFieldBinding,
    LogicalProjectionBinding, ProjectedDocumentState, ProjectionBarrier, ProjectionCurrent,
    ProjectionGeneration, RecipeIdentity, StableDocumentKey,
};

const STATE_MAGIC: &[u8; 8] = b"K5STATE1";
const DIRECTORY_MAGIC: &[u8; 8] = b"K5CDIR01";
const GENERATION_MAGIC: &[u8; 8] = b"K5GEN001";
const EMPTY_COMPONENT_DIRECTORY_DOMAIN: &[u8] = b"keldra.index.v5.empty-component-directory/v1";
const CURRENT_MAGIC: &[u8; 8] = b"K5CUR001";
const BINDING_MAGIC: &[u8; 8] = b"K5BIND01";
const STATE_FORMAT: u16 = 1;
const DIRECTORY_FORMAT: u16 = 1;
const GENERATION_FORMAT: u16 = 1;
const CURRENT_FORMAT: u16 = 1;
const BINDING_FORMAT: u16 = 1;
pub const COMPONENT_DIRECTORY_FANOUT: usize = 256;
pub const MAX_PROJECTION_SOURCES: usize = 4_096;
pub const MAX_LOGICAL_BINDING_FIELDS: usize = 65_536;

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
    pub family_id: [u8; 32],
    pub revision: u64,
    pub barrier: ProjectionBarrier,
    pub component_directory_root_hash: [u8; 32],
    pub component_root_count: u64,
    pub previous_generation_hash: Option<[u8; 32]>,
}

pub fn encode_projection_current(current: ProjectionCurrent) -> Result<Vec<u8>, IndexError> {
    if current.family_id == [0; 32]
        || current.generation_hash == [0; 32]
        || current.generation_revision == 0
    {
        return Err(IndexError::InvalidDefinition(
            "projection current is invalid".into(),
        ));
    }
    let mut out = Vec::new();
    out.extend_from_slice(CURRENT_MAGIC);
    put_u16(&mut out, CURRENT_FORMAT);
    out.extend_from_slice(&current.family_id);
    out.extend_from_slice(&current.generation_hash);
    put_u64(&mut out, current.generation_revision);
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
        family_id: input.array_32()?,
        generation_hash: input.array_32()?,
        generation_revision: input.u64()?,
    };
    input.finish()?;
    encode_projection_current(current)?;
    Ok(current)
}

pub fn encode_projection_generation(
    generation: &ProjectionGeneration,
) -> Result<EncodedProjectionGeneration, IndexError> {
    generation.validate()?;
    if generation.barrier.source_offsets.len() > MAX_PROJECTION_SOURCES {
        return Err(IndexError::InvalidDefinition(
            "projection generation source barrier is unbounded".into(),
        ));
    }
    let component_directory = build_component_directory(&generation.roots)?;
    let mut out = Vec::new();
    out.extend_from_slice(GENERATION_MAGIC);
    put_u16(&mut out, GENERATION_FORMAT);
    out.extend_from_slice(&generation.family_id);
    put_u64(&mut out, generation.revision);
    put_u32(&mut out, generation.barrier.source_offsets.len() as u32);
    for (node, epoch, offset) in &generation.barrier.source_offsets {
        put_u64(&mut out, *node);
        out.extend_from_slice(epoch);
        put_u64(&mut out, *offset);
    }
    put_optional_u64(&mut out, generation.barrier.atomic_through);
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
        family_id: header.family_id,
        revision: header.revision,
        barrier: header.barrier,
        roots: decode_component_directory(component_directory)?,
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
    let family_id = input.array_32()?;
    let revision = input.u64()?;
    let source_count = input.u32()? as usize;
    if source_count == 0 || source_count > MAX_PROJECTION_SOURCES {
        return Err(IndexError::InvalidFormat(
            "projection generation source barrier is unbounded",
        ));
    }
    let mut source_offsets = Vec::with_capacity(source_count);
    for _ in 0..source_count {
        source_offsets.push((input.u64()?, input.array_32()?, input.u64()?));
    }
    let atomic_through = input.optional_u64()?;
    let directory_hash = input.array_32()?;
    let directory_count = input.u64()?;
    let previous_generation_hash = input.optional_hash()?;
    input.finish()?;
    if family_id == [0; 32]
        || directory_hash == [0; 32]
        || (directory_count == 0 && directory_hash != empty_component_directory_hash())
    {
        return Err(IndexError::InvalidFormat(
            "projection generation header contains a zero identity",
        ));
    }
    Ok(ProjectionGenerationHeader {
        family_id,
        revision,
        barrier: ProjectionBarrier::new(source_offsets, atomic_through)?,
        component_directory_root_hash: directory_hash,
        component_root_count: directory_count,
        previous_generation_hash,
    })
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
    put_u64(&mut out, binding.ready_from_revision);
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
    let ready_from_revision = input.u64()?;
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
        ready_from_revision,
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

pub fn encode_projected_document_state(
    state: &ProjectedDocumentState,
) -> Result<Vec<u8>, IndexError> {
    state.validate()?;
    let mut out = Vec::new();
    out.extend_from_slice(STATE_MAGIC);
    put_u16(&mut out, STATE_FORMAT);
    out.extend_from_slice(&state.source_scope);
    out.extend_from_slice(&state.head.stable_key.bytes());
    put_bytes(&mut out, state.head.source_path.as_bytes())?;
    put_u32(&mut out, state.head.source_record);
    put_u64(&mut out, state.head.source_version);
    out.push(u8::from(state.head.live));
    match &state.head.result {
        Some(result) => {
            out.push(1);
            put_bytes(&mut out, result.path.as_bytes())?;
            put_u64(&mut out, result.version);
        }
        None => out.push(0),
    }
    put_recipe_states(&mut out, &state.memberships)?;
    put_recipe_states(&mut out, &state.fields)?;
    append_integrity(&mut out);
    Ok(out)
}

pub fn decode_projected_document_state(bytes: &[u8]) -> Result<ProjectedDocumentState, IndexError> {
    let payload = verify_integrity(bytes)?;
    let mut input = Decoder::new(payload);
    input.expect(STATE_MAGIC)?;
    if input.u16()? != STATE_FORMAT {
        return Err(IndexError::InvalidFormat(
            "projected state format is unsupported",
        ));
    }
    let source_scope = input.array_32()?;
    let encoded_key = StableDocumentKey::from_bytes(input.array_32()?)?;
    let source_path = input.string()?;
    let source_record = input.u32()?;
    let source_version = input.u64()?;
    let live = input.boolean()?;
    let result = match input.byte()? {
        0 => None,
        1 => Some(crate::v4::ObjectIdentity {
            path: input.string()?,
            version: input.u64()?,
        }),
        _ => {
            return Err(IndexError::Decode(
                "projected result presence is invalid".into(),
            ));
        }
    };
    let head = DocumentHead::new(
        source_scope,
        source_path,
        source_record,
        source_version,
        result,
        live,
    )?;
    if head.stable_key != encoded_key {
        return Err(IndexError::Integrity);
    }
    let memberships = input.recipe_states()?;
    let fields = input.recipe_states()?;
    input.finish()?;
    ProjectedDocumentState::new(source_scope, head, memberships, fields)
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
                roots.push(ComponentRoot::new(
                    input.component()?,
                    input.array_32()?,
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
        ComponentRoot::new(
            root.component,
            root.stream_root_hash,
            root.segment_count,
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

fn put_recipe_states(out: &mut Vec<u8>, states: &[CanonicalRecipeState]) -> Result<(), IndexError> {
    put_u32(
        out,
        u32::try_from(states.len()).map_err(|_| IndexError::OffsetOverflow)?,
    );
    for state in states {
        out.extend_from_slice(&state.recipe.bytes());
        out.extend_from_slice(&state.digest);
        put_bytes_u64(out, &state.value)?;
    }
    Ok(())
}

fn put_component(out: &mut Vec<u8>, component: ComponentIdentity) {
    match component {
        ComponentIdentity::DocumentHead => out.push(1),
        ComponentIdentity::ProjectedState => out.push(5),
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

fn put_bytes_u64(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), IndexError> {
    put_u64(
        out,
        u64::try_from(bytes.len()).map_err(|_| IndexError::OffsetOverflow)?,
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
fn put_optional_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            out.push(1);
            put_u64(out, value);
        }
        None => out.push(0),
    }
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
    fn boolean(&mut self) -> Result<bool, IndexError> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(IndexError::Decode("boolean is invalid".into())),
        }
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
    fn optional_u64(&mut self) -> Result<Option<u64>, IndexError> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            _ => Err(IndexError::Decode("optional u64 is invalid".into())),
        }
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
    fn recipe_states(&mut self) -> Result<Vec<CanonicalRecipeState>, IndexError> {
        let count = self.u32()? as usize;
        let mut states = Vec::with_capacity(count);
        for _ in 0..count {
            let recipe = RecipeIdentity::new(self.array_32()?)?;
            let digest = self.array_32()?;
            let length = usize::try_from(self.u64()?).map_err(|_| IndexError::OffsetOverflow)?;
            let value = self.take(length)?.to_vec();
            let state = CanonicalRecipeState::new(recipe, value)?;
            if state.digest != digest {
                return Err(IndexError::Integrity);
            }
            states.push(state);
        }
        Ok(states)
    }
    fn component(&mut self) -> Result<ComponentIdentity, IndexError> {
        match self.byte()? {
            1 => Ok(ComponentIdentity::DocumentHead),
            5 => Ok(ComponentIdentity::ProjectedState),
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

    fn generation() -> ProjectionGeneration {
        ProjectionGeneration::initial(
            [9; 32],
            ProjectionBarrier::new(vec![(1, [1; 32], 7), (2, [2; 32], 11)], Some(5)).unwrap(),
            vec![
                ComponentRoot::new(ComponentIdentity::DocumentHead, [1; 32], 1, 10, 9, 1).unwrap(),
                ComponentRoot::new(ComponentIdentity::ProjectedState, [4; 32], 1, 10, 9, 1)
                    .unwrap(),
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
    fn projected_state_round_trips_and_detects_corruption() {
        let scope = [7; 32];
        let state = ProjectedDocumentState::new(
            scope,
            DocumentHead::new(scope, "objects/a".into(), 4, 9, None, true).unwrap(),
            vec![CanonicalRecipeState::new(recipe(1), vec![1]).unwrap()],
            vec![CanonicalRecipeState::new(recipe(2), b"stable".to_vec()).unwrap()],
        )
        .unwrap();
        let encoded = encode_projected_document_state(&state).unwrap();
        assert_eq!(decode_projected_document_state(&encoded).unwrap(), state);
        let mut corrupt = encoded;
        corrupt[20] ^= 1;
        assert_eq!(
            decode_projected_document_state(&corrupt),
            Err(IndexError::Integrity)
        );
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
        assert_eq!(header.family_id, generation.family_id);
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

        let advanced = generation
            .advance(
                encoded_generation.hash,
                ProjectionBarrier::new(vec![(1, [1; 32], 8), (2, [2; 32], 11)], Some(5)).unwrap(),
                Vec::new(),
            )
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
            ComponentRoot::new(ComponentIdentity::ProjectedState, [9; 32], 1, 64, 48, 16).unwrap(),
        ];
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
        let generation = ProjectionGeneration::initial(
            [9; 32],
            ProjectionBarrier::new(vec![(1, [1; 32], 7)], None).unwrap(),
            roots,
        )
        .unwrap();
        let encoded = encode_projection_generation(&generation).unwrap();
        assert!(encoded.bytes.len() < 256);
        assert_eq!(encoded.component_directory.root_count, 70_002);
    }

    #[test]
    fn logical_binding_round_trips_and_follows_only_ready_family_generations() {
        let generation = generation();
        let binding = LogicalProjectionBinding {
            logical_index_id: 44,
            logical_definition_version: 8,
            family_id: generation.family_id,
            ready_from_revision: generation.revision,
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
            .advance(
                [7; 32],
                ProjectionBarrier::new(vec![(1, [1; 32], 8), (2, [2; 32], 11)], Some(5)).unwrap(),
                Vec::new(),
            )
            .unwrap();
        assert_eq!(
            decode_logical_projection_binding(&bytes, &advanced).unwrap(),
            binding
        );

        let mut wrong_family = advanced.clone();
        wrong_family.family_id = [42; 32];
        assert!(decode_logical_projection_binding(&bytes, &wrong_family).is_err());

        let mut not_ready = binding;
        not_ready.ready_from_revision = advanced.revision + 1;
        assert!(encode_logical_projection_binding(&not_ready, &advanced).is_err());
    }
}
