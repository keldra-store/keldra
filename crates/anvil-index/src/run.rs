use std::borrow::Borrow;
use std::collections::BTreeMap;

use crate::codec::{
    Decoder, Encoder, decode_component_bytes, encode_component, read_component_file,
};
use crate::{
    BlockDescriptor, ComponentCodec, GeneratedBlock, INDEX_ROUTING_FANOUT, IndexBlockSink,
    IndexDirectoryRead, IndexError, IndexKind, MAX_INDEX_BLOCK_BYTES, MAX_INDEX_ROUTING_HEIGHT,
    MAX_RUN_COMPONENTS, SealedRun,
};

#[cfg(test)]
pub(crate) const RUN_ROOT_FILE: &str = "run/root.v2";
pub(crate) const RUN_ROOT_TAG: u8 = 254;
const ROUTING_TAG: u8 = 253;
const ROUTING_FANOUT: usize = INDEX_ROUTING_FANOUT;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RunStatistics {
    pub(crate) mutation_count: u64,
    pub(crate) live_document_count: u64,
    pub(crate) minimum_version: u64,
    pub(crate) maximum_version: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ComponentTree {
    pub(crate) root: BlockDescriptor,
    pub(crate) encoded_bytes: u64,
}

/// Rolls ordered leaf descriptors into fixed-fanout recursive Merkle routing
/// pages. Memory is O(fanout * tree height), independent of component size.
pub(crate) struct RoutingTreeBuilder {
    kind: IndexKind,
    component_tag: u8,
    levels: Vec<Vec<BlockDescriptor>>,
    encoded_bytes: u64,
    last_maximum: Option<Vec<u8>>,
}

impl RoutingTreeBuilder {
    pub(crate) fn new(kind: IndexKind, component_tag: u8) -> Self {
        Self {
            kind,
            component_tag,
            levels: vec![Vec::with_capacity(ROUTING_FANOUT)],
            encoded_bytes: 0,
            last_maximum: None,
        }
    }

    pub(crate) async fn emit_leaf<S: IndexBlockSink>(
        &mut self,
        block: GeneratedBlock,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        let descriptor = block.descriptor().clone();
        if descriptor.component_tag != self.component_tag || descriptor.routing_height != 0 {
            return Err(IndexError::InvalidDefinition(
                "routing tree received the wrong leaf component".into(),
            ));
        }
        if self
            .last_maximum
            .as_ref()
            .is_some_and(|maximum| maximum >= &descriptor.minimum_key)
        {
            return Err(IndexError::InvalidDefinition(
                "component leaf ranges must be strictly ordered".into(),
            ));
        }
        self.last_maximum = Some(descriptor.maximum_key.clone());
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(descriptor.encoded_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        sink.emit(block).await?;
        self.add_descriptor(0, descriptor, sink).await
    }

    async fn add_descriptor<S: IndexBlockSink>(
        &mut self,
        mut level: usize,
        mut descriptor: BlockDescriptor,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        loop {
            if self.levels.len() <= level {
                self.levels
                    .resize_with(level + 1, || Vec::with_capacity(ROUTING_FANOUT));
            }
            self.levels[level].push(descriptor);
            if self.levels[level].len() < ROUTING_FANOUT {
                return Ok(());
            }
            let children =
                std::mem::replace(&mut self.levels[level], Vec::with_capacity(ROUTING_FANOUT));
            let page = encode_routing_page(self.kind, self.component_tag, level + 1, &children)?;
            descriptor = page.descriptor().clone();
            self.encoded_bytes = self
                .encoded_bytes
                .checked_add(descriptor.encoded_bytes)
                .ok_or(IndexError::OffsetOverflow)?;
            sink.emit(page).await?;
            level += 1;
        }
    }

    pub(crate) async fn finish<S: IndexBlockSink>(
        mut self,
        sink: &mut S,
    ) -> Result<ComponentTree, IndexError> {
        loop {
            let populated = self
                .levels
                .iter()
                .enumerate()
                .filter(|(_, entries)| !entries.is_empty())
                .collect::<Vec<_>>();
            if populated.len() == 1 && populated[0].1.len() == 1 {
                return Ok(ComponentTree {
                    root: populated[0].1[0].clone(),
                    encoded_bytes: self.encoded_bytes,
                });
            }
            let Some(level) = self.levels.iter().position(|entries| !entries.is_empty()) else {
                return Err(IndexError::InvalidDefinition(
                    "a component tree requires at least one leaf".into(),
                ));
            };
            let children =
                std::mem::replace(&mut self.levels[level], Vec::with_capacity(ROUTING_FANOUT));
            let page = encode_routing_page(self.kind, self.component_tag, level + 1, &children)?;
            let descriptor = page.descriptor().clone();
            self.encoded_bytes = self
                .encoded_bytes
                .checked_add(descriptor.encoded_bytes)
                .ok_or(IndexError::OffsetOverflow)?;
            sink.emit(page).await?;
            self.add_descriptor(level + 1, descriptor, sink).await?;
        }
    }

    async fn emit_subtree<S: IndexBlockSink>(
        &mut self,
        tree: ComponentTree,
        target_height: u8,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        if tree.root.kind != self.kind
            || tree.root.component_tag != self.component_tag
            || tree.root.routing_height > target_height
        {
            return Err(IndexError::InvalidDefinition(
                "component range has the wrong kind, tag, or height".into(),
            ));
        }
        if self
            .last_maximum
            .as_ref()
            .is_some_and(|maximum| maximum >= &tree.root.minimum_key)
        {
            return Err(IndexError::InvalidDefinition(
                "component ranges must be strictly ordered".into(),
            ));
        }
        self.last_maximum = Some(tree.root.maximum_key.clone());
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(tree.encoded_bytes)
            .ok_or(IndexError::OffsetOverflow)?;

        let mut descriptor = tree.root;
        while descriptor.routing_height < target_height {
            let next_height = descriptor
                .routing_height
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
            let page = encode_routing_page(
                self.kind,
                self.component_tag,
                usize::from(next_height),
                std::slice::from_ref(&descriptor),
            )?;
            descriptor = page.descriptor().clone();
            self.encoded_bytes = self
                .encoded_bytes
                .checked_add(descriptor.encoded_bytes)
                .ok_or(IndexError::OffsetOverflow)?;
            sink.emit(page).await?;
        }
        self.add_descriptor(usize::from(target_height), descriptor, sink)
            .await
    }
}

/// Stitch independently written, strictly ordered component subtrees into one
/// Merkle tree. Shorter subtrees are promoted with single-child routing pages;
/// lane blocks remain ordinary immutable component blocks.
pub(crate) async fn assemble_component_ranges<S, I, T>(
    kind: IndexKind,
    component_tag: u8,
    trees: I,
    sink: &mut S,
) -> Result<ComponentTree, IndexError>
where
    S: IndexBlockSink,
    I: IntoIterator<Item = T>,
    T: Borrow<ComponentTree>,
{
    let trees = trees
        .into_iter()
        .map(|tree| tree.borrow().clone())
        .collect::<Vec<_>>();
    let target_height = trees
        .iter()
        .map(|tree| tree.root.routing_height)
        .max()
        .ok_or_else(|| {
            IndexError::InvalidDefinition(
                "component assembly requires at least one non-empty range".into(),
            )
        })?;
    let mut builder = RoutingTreeBuilder::new(kind, component_tag);
    for tree in trees {
        builder.emit_subtree(tree, target_height, sink).await?;
    }
    builder.finish(sink).await
}

fn encode_routing_page(
    kind: IndexKind,
    component_tag: u8,
    routing_height: usize,
    children: &[BlockDescriptor],
) -> Result<GeneratedBlock, IndexError> {
    if routing_height == 0 || routing_height > MAX_INDEX_ROUTING_HEIGHT {
        return Err(IndexError::ResourceLimit {
            needed: routing_height,
            limit: MAX_INDEX_ROUTING_HEIGHT,
        });
    }
    let first = children
        .first()
        .ok_or(IndexError::InvalidDefinition("empty routing page".into()))?;
    let last = children.last().unwrap();
    let element_count = children.iter().try_fold(0u64, |count, child| {
        count
            .checked_add(child.element_count)
            .ok_or(IndexError::OffsetOverflow)
    })?;
    if children.windows(2).any(|pair| {
        pair[0].maximum_key >= pair[1].minimum_key
            || pair[0].routing_height != pair[1].routing_height
            || pair[0].component_tag != component_tag
            || pair[1].component_tag != component_tag
    }) {
        return Err(IndexError::InvalidDefinition(
            "non-canonical routing children".into(),
        ));
    }
    let mut body = Encoder::default();
    body.u8(component_tag);
    body.u32(children.len())?;
    for child in children {
        encode_descriptor(&mut body, child)?;
    }
    let bytes = encode_component(kind, ROUTING_TAG, ComponentCodec::FixedRows, body.finish())?;
    GeneratedBlock::new(
        kind,
        component_tag,
        ComponentCodec::FixedRows,
        u8::try_from(routing_height).map_err(|_| IndexError::OffsetOverflow)?,
        first.minimum_key.clone(),
        last.maximum_key.clone(),
        element_count,
        bytes,
    )
}

pub(crate) fn seal_run_root(
    kind: IndexKind,
    level: u8,
    statistics: RunStatistics,
    components: impl IntoIterator<Item = ComponentTree>,
) -> Result<SealedRun, IndexError> {
    let mut components = components
        .into_iter()
        .map(|tree| (tree.root.component_tag, tree))
        .collect::<BTreeMap<_, _>>();
    if components.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "a run requires at least one component".into(),
        ));
    }
    if components.len() > MAX_RUN_COMPONENTS {
        return Err(IndexError::ResourceLimit {
            needed: components.len(),
            limit: MAX_RUN_COMPONENTS,
        });
    }
    let encoded_children = components.values().try_fold(0u64, |bytes, tree| {
        bytes
            .checked_add(tree.encoded_bytes)
            .ok_or(IndexError::OffsetOverflow)
    })?;
    let minimum_key = components
        .values()
        .map(|tree| tree.root.minimum_key.as_slice())
        .min()
        .unwrap()
        .to_vec();
    let maximum_key = components
        .values()
        .map(|tree| tree.root.maximum_key.as_slice())
        .max()
        .unwrap()
        .to_vec();
    let mut body = Encoder::default();
    body.u8(level);
    body.u64(statistics.mutation_count);
    body.u64(statistics.live_document_count);
    body.u64(statistics.minimum_version);
    body.u64(statistics.maximum_version);
    body.u32(components.len())?;
    for tree in components.values() {
        body.u64(tree.encoded_bytes);
        encode_descriptor(&mut body, &tree.root)?;
    }
    let bytes = encode_component(kind, RUN_ROOT_TAG, ComponentCodec::FixedRows, body.finish())?;
    let root = GeneratedBlock::new(
        kind,
        RUN_ROOT_TAG,
        ComponentCodec::FixedRows,
        0,
        minimum_key,
        maximum_key,
        statistics.mutation_count.max(1),
        bytes,
    )?;
    let encoded_bytes = encoded_children
        .checked_add(root.descriptor().encoded_bytes)
        .ok_or(IndexError::OffsetOverflow)?;
    components.clear();
    Ok(SealedRun::new(
        kind,
        level,
        statistics.mutation_count,
        statistics.live_document_count,
        statistics.minimum_version,
        statistics.maximum_version,
        encoded_bytes,
        root,
    ))
}

#[derive(Clone, Debug)]
pub(crate) struct RunView {
    components: BTreeMap<u8, (u64, BlockDescriptor)>,
}

impl RunView {
    pub(crate) fn component(&self, tag: u8) -> Result<&BlockDescriptor, IndexError> {
        self.components
            .get(&tag)
            .map(|(_, descriptor)| descriptor)
            .ok_or(IndexError::InvalidFormat("missing run component"))
    }

    pub(crate) fn component_optional(&self, tag: u8) -> Option<&BlockDescriptor> {
        self.components.get(&tag).map(|(_, descriptor)| descriptor)
    }
}

pub(crate) async fn open_run<D: IndexDirectoryRead>(
    directory: &D,
    expected_kind: IndexKind,
) -> Result<RunView, IndexError> {
    let file = directory.open_root().await?;
    let component = read_component_file(
        &file,
        expected_kind,
        RUN_ROOT_TAG,
        &[ComponentCodec::FixedRows],
    )
    .await?;
    decode_run_root(
        component.body.as_slice(),
        expected_kind,
        component.encoded_bytes,
        None,
    )
}

fn decode_run_root(
    body: &[u8],
    expected_kind: IndexKind,
    root_encoded_bytes: u64,
    root: Option<&BlockDescriptor>,
) -> Result<RunView, IndexError> {
    let mut decoder = Decoder::new(body);
    let _level = decoder.u8()?;
    let mutation_count = decoder.u64()?;
    let live_document_count = decoder.u64()?;
    let minimum_version = decoder.u64()?;
    let maximum_version = decoder.u64()?;
    let count = decoder.u32()? as usize;
    if count == 0
        || count > MAX_RUN_COMPONENTS
        || mutation_count == 0
        || live_document_count > mutation_count
    {
        return Err(IndexError::InvalidFormat("run root counts"));
    }
    if minimum_version > maximum_version {
        return Err(IndexError::InvalidFormat("run root versions"));
    }
    let mut components = BTreeMap::new();
    let mut previous_tag = None;
    for _ in 0..count {
        let component_bytes = decoder.u64()?;
        let descriptor = decode_descriptor(&mut decoder)?;
        if descriptor.kind != expected_kind
            || descriptor.component_tag == RUN_ROOT_TAG
            || descriptor.component_tag == ROUTING_TAG
            || component_bytes < descriptor.encoded_bytes
            || previous_tag.is_some_and(|previous| previous >= descriptor.component_tag)
        {
            return Err(IndexError::InvalidFormat("run component descriptor"));
        }
        previous_tag = Some(descriptor.component_tag);
        if components
            .insert(descriptor.component_tag, (component_bytes, descriptor))
            .is_some()
        {
            return Err(IndexError::InvalidFormat("duplicate run component"));
        }
    }
    decoder.finish()?;
    validate_required_components(expected_kind, live_document_count, &components)?;
    if let Some(root) = root {
        let minimum_key = components
            .values()
            .map(|(_, descriptor)| descriptor.minimum_key.as_slice())
            .min()
            .unwrap();
        let maximum_key = components
            .values()
            .map(|(_, descriptor)| descriptor.maximum_key.as_slice())
            .max()
            .unwrap();
        if root.kind != expected_kind
            || root.component_tag != RUN_ROOT_TAG
            || root.codec != ComponentCodec::FixedRows
            || root.routing_height != 0
            || root.element_count != mutation_count.max(1)
            || root.minimum_key.as_slice() != minimum_key
            || root.maximum_key.as_slice() != maximum_key
            || root.encoded_bytes != root_encoded_bytes
        {
            return Err(IndexError::InvalidFormat("run root descriptor"));
        }
    }
    Ok(RunView { components })
}

fn validate_required_components(
    kind: IndexKind,
    live_document_count: u64,
    components: &BTreeMap<u8, (u64, BlockDescriptor)>,
) -> Result<(), IndexError> {
    use crate::segment::{DOCUMENTS_TAG, PATH_CHANGES_TAG};

    let mut required = vec![PATH_CHANGES_TAG];
    if live_document_count > 0 {
        match kind {
            IndexKind::Path => {}
            IndexKind::MetadataFilter | IndexKind::TypedJson => {
                required.extend([DOCUMENTS_TAG, crate::typed_json::ROWS_TAG]);
            }
            IndexKind::FullText => required.push(DOCUMENTS_TAG),
            IndexKind::Vector => {
                required.extend([DOCUMENTS_TAG, crate::vector::VECTORS_TAG]);
            }
            IndexKind::Hybrid => {
                required.extend([DOCUMENTS_TAG, crate::hybrid::HYBRID_VECTOR_TAG]);
            }
            IndexKind::GitSource | IndexKind::Tensor => {
                required.extend([DOCUMENTS_TAG, crate::projections::RECORDS_TAG]);
            }
        }
    }
    if required.iter().any(|tag| !components.contains_key(tag)) {
        return Err(IndexError::InvalidFormat("missing required run component"));
    }
    Ok(())
}

pub(crate) async fn open_views<D: IndexDirectoryRead>(
    runs: &[D],
    expected_kind: IndexKind,
) -> Result<Vec<RunView>, IndexError> {
    let mut views = Vec::with_capacity(runs.len());
    for run in runs {
        views.push(open_run(run, expected_kind).await?);
    }
    Ok(views)
}

pub(crate) async fn find_leaf<D: IndexDirectoryRead>(
    directory: &D,
    root: &BlockDescriptor,
    key: &[u8],
) -> Result<Option<BlockDescriptor>, IndexError> {
    if key < root.minimum_key.as_slice() || key > root.maximum_key.as_slice() {
        return Ok(None);
    }
    let mut current = root.clone();
    while current.routing_height > 0 {
        let children = read_routing_page(directory, &current).await?;
        let Some(child) = children.into_iter().find(|child| {
            key >= child.minimum_key.as_slice() && key <= child.maximum_key.as_slice()
        }) else {
            return Ok(None);
        };
        current = child;
    }
    Ok(Some(current))
}

pub(crate) struct LeafCursor<'a, D> {
    directory: &'a D,
    pending: Option<BlockDescriptor>,
    stack: Vec<(Vec<BlockDescriptor>, usize)>,
    range: Option<crate::compaction::KeyRange>,
}

/// One validated leaf retaining the original encoded allocation. Consumers
/// borrow its body without allocating and copying the complete block again.
pub(crate) struct LeafBlock<S> {
    encoded: crate::io::ReadBuffer<S>,
    body_start: usize,
}

impl<S: AsRef<[u8]>> LeafBlock<S> {
    pub(crate) fn body(&self) -> &[u8] {
        &self.encoded.as_ref()[self.body_start..]
    }
}

/// Bounded deterministic traversal of every non-root block referenced by one
/// staged run. The run root is exposed separately so a publisher can make it
/// visible last.
///
/// Canonical disjoint ranges and strictly decreasing routing heights prove the
/// structure is a tree: duplicate references, cross-links and cycles fail
/// validation without retaining a corpus-sized visited set. The DFS stack is
/// bounded by routing fanout times tree height.
pub struct RunBlockWalker<'a, D> {
    directory: &'a D,
    root: BlockDescriptor,
    pending: Vec<BlockDescriptor>,
}

impl<'a, D: IndexDirectoryRead> RunBlockWalker<'a, D> {
    pub async fn open(directory: &'a D, root: BlockDescriptor) -> Result<Self, IndexError> {
        let bytes = read_descriptor_bytes(directory, &root).await?;
        let component = decode_component_bytes(
            bytes.as_ref(),
            root.kind,
            RUN_ROOT_TAG,
            &[ComponentCodec::FixedRows],
        )?;
        let view = decode_run_root(
            component.body,
            root.kind,
            component.encoded_bytes,
            Some(&root),
        )?;
        let mut pending = view
            .components
            .into_values()
            .map(|(_, descriptor)| descriptor)
            .collect::<Vec<_>>();
        pending.reverse();
        Ok(Self {
            directory,
            root,
            pending,
        })
    }

    pub fn root_descriptor(&self) -> &BlockDescriptor {
        &self.root
    }

    pub async fn next(&mut self) -> Result<Option<BlockDescriptor>, IndexError> {
        let Some(descriptor) = self.pending.pop() else {
            return Ok(None);
        };
        if descriptor.routing_height == 0 {
            read_leaf(self.directory, &descriptor).await?;
        } else {
            let children = read_routing_page(self.directory, &descriptor).await?;
            self.pending.extend(children.into_iter().rev());
        }
        Ok(Some(descriptor))
    }
}

impl<'a, D: IndexDirectoryRead> LeafCursor<'a, D> {
    pub(crate) fn new(directory: &'a D, root: BlockDescriptor) -> Self {
        Self {
            directory,
            pending: Some(root),
            stack: Vec::new(),
            range: None,
        }
    }

    pub(crate) fn in_range(
        directory: &'a D,
        root: BlockDescriptor,
        range: crate::compaction::KeyRange,
    ) -> Self {
        Self {
            directory,
            pending: Some(root),
            stack: Vec::new(),
            range: Some(range),
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<BlockDescriptor>, IndexError> {
        loop {
            if let Some(current) = self.pending.take() {
                if self
                    .range
                    .as_ref()
                    .is_some_and(|range| !range.intersects(&current))
                {
                    continue;
                }
                if current.routing_height == 0 {
                    return Ok(Some(current));
                }
                let mut children = read_routing_page(self.directory, &current).await?;
                if let Some(range) = &self.range {
                    children.retain(|child| range.intersects(child));
                }
                let Some(first) = children.first().cloned() else {
                    continue;
                };
                self.stack.push((children, 1));
                self.pending = Some(first);
                continue;
            }
            let Some((children, next)) = self.stack.last_mut() else {
                return Ok(None);
            };
            if *next < children.len() {
                self.pending = Some(children[*next].clone());
                *next += 1;
            } else {
                self.stack.pop();
            }
        }
    }
}

pub(crate) async fn read_leaf<D: IndexDirectoryRead>(
    directory: &D,
    descriptor: &BlockDescriptor,
) -> Result<LeafBlock<<D::File as crate::IndexFileRead>::Slice>, IndexError> {
    if descriptor.routing_height != 0 {
        return Err(IndexError::InvalidFormat("expected data leaf"));
    }
    let bytes = read_descriptor_bytes(directory, descriptor).await?;
    let component = decode_component_bytes(
        bytes.as_ref(),
        descriptor.kind,
        descriptor.component_tag,
        &[descriptor.codec],
    )?;
    let body_start = bytes
        .as_ref()
        .len()
        .checked_sub(component.body.len())
        .ok_or(IndexError::OffsetOverflow)?;
    Ok(LeafBlock {
        encoded: bytes,
        body_start,
    })
}

async fn read_routing_page<D: IndexDirectoryRead>(
    directory: &D,
    descriptor: &BlockDescriptor,
) -> Result<Vec<BlockDescriptor>, IndexError> {
    if descriptor.routing_height == 0 || descriptor.codec != ComponentCodec::FixedRows {
        return Err(IndexError::InvalidFormat("routing descriptor"));
    }
    let bytes = read_descriptor_bytes(directory, descriptor).await?;
    let component = decode_component_bytes(
        bytes.as_ref(),
        descriptor.kind,
        ROUTING_TAG,
        &[ComponentCodec::FixedRows],
    )?;
    let mut decoder = Decoder::new(component.body);
    if decoder.u8()? != descriptor.component_tag {
        return Err(IndexError::InvalidFormat("routing component tag"));
    }
    let count = decoder.u32()? as usize;
    if count == 0 || count > ROUTING_FANOUT {
        return Err(IndexError::InvalidFormat("routing page fanout"));
    }
    let mut children = Vec::with_capacity(count);
    for _ in 0..count {
        children.push(decode_descriptor(&mut decoder)?);
    }
    decoder.finish()?;
    let element_count = children.iter().try_fold(0u64, |count, child| {
        count
            .checked_add(child.element_count)
            .ok_or(IndexError::OffsetOverflow)
    })?;
    if element_count != descriptor.element_count
        || children.iter().any(|child| {
            child.routing_height.checked_add(1) != Some(descriptor.routing_height)
                || child.component_tag != descriptor.component_tag
                || child.kind != descriptor.kind
        })
        || children
            .windows(2)
            .any(|pair| pair[0].maximum_key >= pair[1].minimum_key)
        || children.first().map(|child| &child.minimum_key) != Some(&descriptor.minimum_key)
        || children.last().map(|child| &child.maximum_key) != Some(&descriptor.maximum_key)
    {
        return Err(IndexError::InvalidFormat("routing page bounds"));
    }
    Ok(children)
}

async fn read_descriptor_bytes<D: IndexDirectoryRead>(
    directory: &D,
    descriptor: &BlockDescriptor,
) -> Result<crate::io::ReadBuffer<<D::File as crate::IndexFileRead>::Slice>, IndexError> {
    use crate::IndexFileRead;
    use crate::io::read_exact_at;

    let file = directory.open_block(descriptor).await?;
    let length =
        usize::try_from(descriptor.encoded_bytes).map_err(|_| IndexError::OffsetOverflow)?;
    if length > MAX_INDEX_BLOCK_BYTES {
        return Err(IndexError::InvalidFormat("block descriptor size"));
    }
    let bytes = read_exact_at(&file, 0, length).await?;
    if !file
        .read_at(descriptor.encoded_bytes, 1)
        .await?
        .as_ref()
        .is_empty()
    {
        return Err(IndexError::InvalidFormat("trailing block bytes"));
    }
    if blake3::hash(bytes.as_ref()).as_bytes() != &descriptor.hash {
        return Err(IndexError::Integrity);
    }
    Ok(bytes)
}

pub(crate) fn encode_descriptor(
    output: &mut Encoder,
    descriptor: &BlockDescriptor,
) -> Result<(), IndexError> {
    output.u8(descriptor.component_tag);
    output.u8(descriptor.kind as u8);
    output.u8(descriptor.codec as u8);
    output.u8(descriptor.routing_height);
    output.bytes(&descriptor.minimum_key)?;
    output.bytes(&descriptor.maximum_key)?;
    output.u64(descriptor.element_count);
    output.u64(descriptor.encoded_bytes);
    output.raw_bytes(&descriptor.hash);
    Ok(())
}

pub(crate) fn decode_descriptor(decoder: &mut Decoder<'_>) -> Result<BlockDescriptor, IndexError> {
    let component_tag = decoder.u8()?;
    let kind = IndexKind::from_tag(decoder.u8()?)?;
    let codec = ComponentCodec::from_tag(decoder.u8()?)?;
    let routing_height = decoder.u8()?;
    let minimum_key = decoder.bytes()?.to_vec();
    let maximum_key = decoder.bytes()?.to_vec();
    let element_count = decoder.u64()?;
    let encoded_bytes = decoder.u64()?;
    let hash = decoder.fixed(32)?.try_into().unwrap();
    if element_count == 0
        || encoded_bytes == 0
        || encoded_bytes > MAX_INDEX_BLOCK_BYTES as u64
        || usize::from(routing_height) > MAX_INDEX_ROUTING_HEIGHT
        || minimum_key.len() > crate::MAX_INDEX_ROUTING_KEY_BYTES
        || maximum_key.len() > crate::MAX_INDEX_ROUTING_KEY_BYTES
        || minimum_key > maximum_key
    {
        return Err(IndexError::InvalidFormat("block descriptor bounds"));
    }
    Ok(BlockDescriptor {
        kind,
        component_tag,
        codec,
        routing_height,
        minimum_key,
        maximum_key,
        element_count,
        encoded_bytes,
        hash,
    })
}

#[cfg(test)]
mod tests {
    use crate::io::tests::MemoryBlockSink;

    use super::*;

    #[test]
    fn live_runs_require_their_payload_component() {
        let mut components = BTreeMap::new();
        let descriptor = BlockDescriptor {
            kind: IndexKind::Vector,
            component_tag: crate::segment::PATH_CHANGES_TAG,
            codec: ComponentCodec::FixedRows,
            routing_height: 0,
            minimum_key: b"a".to_vec(),
            maximum_key: b"a".to_vec(),
            element_count: 1,
            encoded_bytes: 1,
            hash: [0; 32],
        };
        components.insert(crate::segment::PATH_CHANGES_TAG, (1, descriptor));
        assert_eq!(
            validate_required_components(IndexKind::Vector, 1, &components).unwrap_err(),
            IndexError::InvalidFormat("missing required run component")
        );
    }

    #[test]
    fn oversized_block_descriptor_is_rejected() {
        let mut encoded = Encoder::default();
        encoded.u8(crate::segment::PATH_CHANGES_TAG);
        encoded.u8(IndexKind::Path as u8);
        encoded.u8(ComponentCodec::FixedRows as u8);
        encoded.u8(0);
        encoded.bytes(b"a").unwrap();
        encoded.bytes(b"a").unwrap();
        encoded.u64(1);
        encoded.u64((MAX_INDEX_BLOCK_BYTES + 1) as u64);
        encoded.raw_bytes(&[0; 32]);
        let bytes = encoded.finish();
        assert_eq!(
            decode_descriptor(&mut Decoder::new(&bytes)).unwrap_err(),
            IndexError::InvalidFormat("block descriptor bounds")
        );
    }

    #[tokio::test]
    async fn recursive_routing_keeps_only_bounded_fanout() {
        let mut sink = MemoryBlockSink::default();
        let mut tree = RoutingTreeBuilder::new(IndexKind::Path, 1);
        for index in 0..(ROUTING_FANOUT * ROUTING_FANOUT + 3) {
            let key = format!("{index:08}").into_bytes();
            let bytes =
                encode_component(IndexKind::Path, 1, ComponentCodec::FixedRows, key.clone())
                    .unwrap();
            tree.emit_leaf(
                GeneratedBlock::new(
                    IndexKind::Path,
                    1,
                    ComponentCodec::FixedRows,
                    0,
                    key.clone(),
                    key,
                    1,
                    bytes,
                )
                .unwrap(),
                &mut sink,
            )
            .await
            .unwrap();
        }
        let tree = tree.finish(&mut sink).await.unwrap();
        assert!(tree.root.routing_height >= 2);
        assert!(sink.len() > ROUTING_FANOUT * ROUTING_FANOUT);

        let directory = sink.directory();
        let mut cursor = LeafCursor::new(&directory, tree.root);
        let mut keys = Vec::new();
        while let Some(descriptor) = cursor.next().await.unwrap() {
            keys.push(descriptor.minimum_key);
        }
        assert_eq!(keys.len(), ROUTING_FANOUT * ROUTING_FANOUT + 3);
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[tokio::test]
    async fn independently_written_ranges_assemble_across_tree_heights() {
        let mut sink = MemoryBlockSink::default();
        let mut first = RoutingTreeBuilder::new(IndexKind::Path, 1);
        for index in 0..=ROUTING_FANOUT {
            let key = format!("a{index:08}").into_bytes();
            let bytes =
                encode_component(IndexKind::Path, 1, ComponentCodec::FixedRows, key.clone())
                    .unwrap();
            first
                .emit_leaf(
                    GeneratedBlock::new(
                        IndexKind::Path,
                        1,
                        ComponentCodec::FixedRows,
                        0,
                        key.clone(),
                        key,
                        1,
                        bytes,
                    )
                    .unwrap(),
                    &mut sink,
                )
                .await
                .unwrap();
        }
        let first = first.finish(&mut sink).await.unwrap();
        assert_eq!(first.root.routing_height, 2);

        let mut second_sink = sink.clone();
        let mut second = RoutingTreeBuilder::new(IndexKind::Path, 1);
        let key = b"z00000000".to_vec();
        let bytes =
            encode_component(IndexKind::Path, 1, ComponentCodec::FixedRows, key.clone()).unwrap();
        second
            .emit_leaf(
                GeneratedBlock::new(
                    IndexKind::Path,
                    1,
                    ComponentCodec::FixedRows,
                    0,
                    key.clone(),
                    key,
                    1,
                    bytes,
                )
                .unwrap(),
                &mut second_sink,
            )
            .await
            .unwrap();
        let second = second.finish(&mut second_sink).await.unwrap();
        assert_eq!(second.root.routing_height, 0);

        let assembled = assemble_component_ranges(IndexKind::Path, 1, [&first, &second], &mut sink)
            .await
            .unwrap();
        let directory = sink.directory();
        let mut cursor = LeafCursor::new(&directory, assembled.root);
        let mut keys = Vec::new();
        while let Some(descriptor) = cursor.next().await.unwrap() {
            keys.push(descriptor.minimum_key);
        }
        assert_eq!(keys.len(), ROUTING_FANOUT + 2);
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[tokio::test]
    async fn component_range_assembly_rejects_out_of_order_subtrees() {
        let mut sink = MemoryBlockSink::default();
        let mut trees = Vec::new();
        for key in [b"a".as_slice(), b"b".as_slice()] {
            let mut builder = RoutingTreeBuilder::new(IndexKind::Path, 1);
            let bytes =
                encode_component(IndexKind::Path, 1, ComponentCodec::FixedRows, key.to_vec())
                    .unwrap();
            builder
                .emit_leaf(
                    GeneratedBlock::new(
                        IndexKind::Path,
                        1,
                        ComponentCodec::FixedRows,
                        0,
                        key.to_vec(),
                        key.to_vec(),
                        1,
                        bytes,
                    )
                    .unwrap(),
                    &mut sink,
                )
                .await
                .unwrap();
            trees.push(builder.finish(&mut sink).await.unwrap());
        }
        trees.reverse();
        assert!(
            assemble_component_ranges(IndexKind::Path, 1, trees, &mut sink)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn identical_block_hash_is_idempotent() {
        let mut sink = MemoryBlockSink::default();
        for _ in 0..2 {
            let bytes = encode_component(
                IndexKind::Path,
                1,
                ComponentCodec::FixedRows,
                b"same".to_vec(),
            )
            .unwrap();
            sink.emit(
                GeneratedBlock::new(
                    IndexKind::Path,
                    1,
                    ComponentCodec::FixedRows,
                    0,
                    b"a".to_vec(),
                    b"a".to_vec(),
                    1,
                    bytes,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        }
        assert_eq!(sink.len(), 1);
    }

    #[tokio::test]
    async fn corrupt_routing_descriptor_fails_closed() {
        let mut sink = MemoryBlockSink::default();
        let mut tree = RoutingTreeBuilder::new(IndexKind::Path, 1);
        for index in 0..(ROUTING_FANOUT + 1) {
            let key = format!("{index:08}").into_bytes();
            let bytes =
                encode_component(IndexKind::Path, 1, ComponentCodec::FixedRows, key.clone())
                    .unwrap();
            tree.emit_leaf(
                GeneratedBlock::new(
                    IndexKind::Path,
                    1,
                    ComponentCodec::FixedRows,
                    0,
                    key.clone(),
                    key,
                    1,
                    bytes,
                )
                .unwrap(),
                &mut sink,
            )
            .await
            .unwrap();
        }
        let mut root = tree.finish(&mut sink).await.unwrap().root;
        root.maximum_key = b"00000000".to_vec();
        let directory = sink.directory();
        let mut cursor = LeafCursor::new(&directory, root);
        assert!(cursor.next().await.is_err());
    }

    #[tokio::test]
    async fn run_walker_visits_each_staged_block_once_and_root_last() {
        let mut sink = MemoryBlockSink::default();
        let mut tree = RoutingTreeBuilder::new(IndexKind::Path, 1);
        for index in 0..(ROUTING_FANOUT + 3) {
            let key = format!("{index:08}").into_bytes();
            let bytes =
                encode_component(IndexKind::Path, 1, ComponentCodec::FixedRows, key.clone())
                    .unwrap();
            tree.emit_leaf(
                GeneratedBlock::new(
                    IndexKind::Path,
                    1,
                    ComponentCodec::FixedRows,
                    0,
                    key.clone(),
                    key,
                    1,
                    bytes,
                )
                .unwrap(),
                &mut sink,
            )
            .await
            .unwrap();
        }
        let tree = tree.finish(&mut sink).await.unwrap();
        let expected_non_root = sink.len();
        let run = seal_run_root(
            IndexKind::Path,
            0,
            RunStatistics {
                mutation_count: ROUTING_FANOUT as u64 + 3,
                live_document_count: ROUTING_FANOUT as u64 + 3,
                minimum_version: 1,
                maximum_version: 1,
            },
            [tree],
        )
        .unwrap();
        let root = run.into_root();
        let root_descriptor = root.descriptor().clone();
        let directory = sink.directory_with_root(root);
        let mut walker = RunBlockWalker::open(&directory, root_descriptor.clone())
            .await
            .unwrap();
        assert_eq!(walker.root_descriptor(), &root_descriptor);
        let mut visited = Vec::new();
        while let Some(descriptor) = walker.next().await.unwrap() {
            visited.push(descriptor.hash);
        }
        let unique = visited
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(visited.len(), expected_non_root);
        assert_eq!(unique.len(), visited.len());
        assert!(!unique.contains(&root_descriptor.hash));
    }

    #[test]
    fn routing_page_rejects_duplicate_child_ranges() {
        let child = BlockDescriptor {
            kind: IndexKind::Path,
            component_tag: 1,
            codec: ComponentCodec::FixedRows,
            routing_height: 0,
            minimum_key: b"a".to_vec(),
            maximum_key: b"a".to_vec(),
            element_count: 1,
            encoded_bytes: 1,
            hash: [1; 32],
        };
        assert!(encode_routing_page(IndexKind::Path, 1, 1, &[child.clone(), child]).is_err());
    }
}
