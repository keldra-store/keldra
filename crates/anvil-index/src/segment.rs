use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::codec::{Decoder, Encoder, encode_component};
use crate::run::{ComponentTree, RoutingTreeBuilder};
use crate::succinct::{decode_prefix_dictionary_with_budget, encode_prefix_dictionary};
use crate::{
    ComponentCodec, DocumentRef, GeneratedBlock, IndexBlockSink, IndexDirectoryRead, IndexError,
    IndexKind, IndexMutation, SegmentBuildOptions, SegmentPush,
};

pub(crate) const PATH_CHANGES_TAG: u8 = 1;
pub(crate) const DOCUMENTS_TAG: u8 = 2;
pub(crate) const DEFAULT_COMPONENT_BLOCK_BYTES: usize = crate::MAX_INDEX_BLOCK_BYTES - 64 * 1024;
const ENTRY_OVERHEAD: usize = 128;
const LATEST_LIVE_PROBE_CACHE_ENTRIES: usize = 256;

#[derive(Debug)]
pub(crate) struct PendingMutation<T> {
    pub(crate) mutation: IndexMutation<T>,
    resident_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct MutationBuffer<T> {
    options: SegmentBuildOptions,
    resident_bytes: usize,
    entries: BTreeMap<String, PendingMutation<T>>,
}

impl<T> MutationBuffer<T> {
    pub(crate) fn new(options: SegmentBuildOptions) -> Result<Self, IndexError> {
        SegmentBuildOptions::new(options.max_resident_bytes)?;
        Ok(Self {
            options,
            resident_bytes: 0,
            entries: BTreeMap::new(),
        })
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    pub(crate) fn seal_workspace_bytes(&self) -> Result<usize, IndexError> {
        self.resident_bytes
            .checked_add(crate::FIXED_INDEX_SEAL_WORKSPACE_BYTES)
            .ok_or(IndexError::OffsetOverflow)
    }

    pub(crate) fn level(&self) -> u8 {
        self.options.level
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn try_push(
        &mut self,
        mutation: IndexMutation<T>,
        payload_bytes: usize,
        upsert_document: impl Fn(&T) -> &DocumentRef,
    ) -> Result<SegmentPush<T>, IndexError> {
        let document = mutation_document(&mutation, &upsert_document);
        document.validate()?;
        let entry_bytes = payload_bytes
            .checked_add(document.path.len())
            .and_then(|bytes| bytes.checked_add(ENTRY_OVERHEAD))
            .ok_or(IndexError::OffsetOverflow)?;
        if entry_bytes > self.options.max_resident_bytes {
            return Err(IndexError::ResourceLimit {
                needed: entry_bytes,
                limit: self.options.max_resident_bytes,
            });
        }
        let old = self.entries.get(&document.path);
        if old.is_some_and(|old| {
            mutation_document(&old.mutation, &upsert_document).version > document.version
        }) {
            return Ok(SegmentPush::Accepted);
        }
        let old_bytes = old.map_or(0, |entry| entry.resident_bytes);
        let projected = self
            .resident_bytes
            .checked_sub(old_bytes)
            .and_then(|bytes| bytes.checked_add(entry_bytes))
            .ok_or(IndexError::OffsetOverflow)?;
        if projected > self.options.max_resident_bytes {
            return Ok(SegmentPush::Full(mutation));
        }
        let path = document.path.clone();
        self.entries.insert(
            path,
            PendingMutation {
                mutation,
                resident_bytes: entry_bytes,
            },
        );
        self.resident_bytes = projected;
        Ok(SegmentPush::Accepted)
    }

    pub(crate) fn into_entries(self) -> BTreeMap<String, PendingMutation<T>> {
        self.entries
    }
}

pub(crate) fn mutation_document<'a, T>(
    mutation: &'a IndexMutation<T>,
    upsert_document: impl Fn(&'a T) -> &'a DocumentRef,
) -> &'a DocumentRef {
    match mutation {
        IndexMutation::Upsert(value) => upsert_document(value),
        IndexMutation::Remove(document) => document,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DocumentState {
    Removed,
    Live,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PathChange {
    pub(crate) document: DocumentRef,
    pub(crate) state: DocumentState,
    pub(crate) document_ordinal: Option<u64>,
}

impl PathChange {
    pub(crate) fn from_mutation<T>(
        mutation: &IndexMutation<T>,
        upsert_document: impl Fn(&T) -> &DocumentRef,
        document_ordinal: Option<u64>,
    ) -> Self {
        match mutation {
            IndexMutation::Upsert(value) => Self {
                document: upsert_document(value).clone(),
                state: DocumentState::Live,
                document_ordinal,
            },
            IndexMutation::Remove(document) => Self {
                document: document.clone(),
                state: DocumentState::Removed,
                document_ordinal: None,
            },
        }
    }
}

pub(crate) struct PathComponentWriter {
    kind: IndexKind,
    level: u8,
    target_bytes: usize,
    rows: Vec<PathChange>,
    estimated_bytes: usize,
    tree: RoutingTreeBuilder,
}

impl PathComponentWriter {
    pub(crate) fn new(kind: IndexKind, level: u8, target_bytes: usize) -> Self {
        Self {
            kind,
            level,
            target_bytes: target_bytes.max(256),
            rows: Vec::new(),
            estimated_bytes: 0,
            tree: RoutingTreeBuilder::new(kind, PATH_CHANGES_TAG),
        }
    }

    pub(crate) async fn push<S: IndexBlockSink>(
        &mut self,
        row: PathChange,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        let encoded_row_bytes = row.document.path.len().saturating_add(32);
        let resident_row_bytes = row
            .document
            .path
            .capacity()
            .saturating_add(2 * std::mem::size_of::<PathChange>());
        let row_bytes = encoded_row_bytes.max(resident_row_bytes);
        if !self.rows.is_empty()
            && self.estimated_bytes.saturating_add(row_bytes) > self.target_bytes
        {
            self.flush(sink).await?;
        }
        if self
            .rows
            .last()
            .is_some_and(|previous| previous.document.path >= row.document.path)
        {
            return Err(IndexError::InvalidDefinition(
                "path changes must be unique and sorted".into(),
            ));
        }
        self.estimated_bytes = self.estimated_bytes.saturating_add(row_bytes);
        self.rows.push(row);
        Ok(())
    }

    async fn flush<S: IndexBlockSink>(&mut self, sink: &mut S) -> Result<(), IndexError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        self.estimated_bytes = 0;
        let first = rows.first().unwrap().document.path.as_bytes().to_vec();
        let last = rows.last().unwrap().document.path.as_bytes().to_vec();
        let codec = if self.level == 0 {
            ComponentCodec::FixedRows
        } else {
            ComponentCodec::PrefixEliasFano
        };
        let body = if self.level == 0 {
            encode_path_rows_fixed(&rows)?
        } else {
            encode_path_rows_succinct(&rows)?
        };
        let bytes = encode_component(self.kind, PATH_CHANGES_TAG, codec, body)?;
        self.tree
            .emit_leaf(
                GeneratedBlock::new(
                    self.kind,
                    PATH_CHANGES_TAG,
                    codec,
                    0,
                    first,
                    last,
                    rows.len() as u64,
                    bytes,
                )?,
                sink,
            )
            .await
    }

    pub(crate) async fn finish<S: IndexBlockSink>(
        mut self,
        sink: &mut S,
    ) -> Result<ComponentTree, IndexError> {
        self.flush(sink).await?;
        self.tree.finish(sink).await
    }
}

fn encode_path_rows_fixed(rows: &[PathChange]) -> Result<Vec<u8>, IndexError> {
    let mut output = Encoder::default();
    output.u32(rows.len())?;
    for row in rows {
        output.string(&row.document.path)?;
        output.u64(row.document.version);
        output.u8(u8::from(row.state == DocumentState::Live));
        output.u64(row.document_ordinal.unwrap_or(u64::MAX));
    }
    Ok(output.finish())
}

fn encode_path_rows_succinct(rows: &[PathChange]) -> Result<Vec<u8>, IndexError> {
    let dictionary = encode_prefix_dictionary(
        &rows
            .iter()
            .map(|row| row.document.path.clone())
            .collect::<Vec<_>>(),
    )?;
    let mut output = Encoder::default();
    output.u32(rows.len())?;
    output.bytes(&dictionary)?;
    let mut states = vec![0u64; rows.len().div_ceil(64)];
    for (index, row) in rows.iter().enumerate() {
        output.u64(row.document.version);
        if row.state == DocumentState::Live {
            states[index / 64] |= 1u64 << (index % 64);
        }
    }
    output.u32(states.len())?;
    for word in states {
        output.u64(word);
    }
    for row in rows {
        output.u64(row.document_ordinal.unwrap_or(u64::MAX));
    }
    Ok(output.finish())
}

pub(crate) async fn read_path_block<D: IndexDirectoryRead>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
) -> Result<Vec<PathChange>, IndexError> {
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let descriptor = descriptor.clone();
    directory
        .run_query_cpu(move || {
            let rows = match descriptor.codec {
                ComponentCodec::FixedRows => decode_path_rows_fixed(block.body())?,
                ComponentCodec::PrefixEliasFano => decode_path_rows_succinct(block.body())?,
                _ => return Err(IndexError::InvalidFormat("path block codec")),
            };
            validate_path_block(rows, &descriptor)
        })
        .await
}

pub(crate) async fn read_path_block_parallel<D, E>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
    executor: &E,
    progress: &crate::compaction::CompactionProgress,
) -> Result<Vec<PathChange>, IndexError>
where
    D: IndexDirectoryRead,
    E: crate::compaction::CompactionExecutor,
{
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let descriptor = descriptor.clone();
    let rows = executor
        .run_cpu(move || {
            let rows = match descriptor.codec {
                ComponentCodec::FixedRows => decode_path_rows_fixed(block.body())?,
                ComponentCodec::PrefixEliasFano => decode_path_rows_succinct(block.body())?,
                _ => return Err(IndexError::InvalidFormat("path block codec")),
            };
            validate_path_block(rows, &descriptor)
        })
        .await?;
    progress.record_input(rows.len() as u64, 0, 0);
    Ok(rows)
}

fn validate_path_block(
    rows: Vec<PathChange>,
    descriptor: &crate::BlockDescriptor,
) -> Result<Vec<PathChange>, IndexError> {
    if rows.first().map(|row| row.document.path.as_bytes())
        != Some(descriptor.minimum_key.as_slice())
        || rows.last().map(|row| row.document.path.as_bytes())
            != Some(descriptor.maximum_key.as_slice())
        || rows.len() as u64 != descriptor.element_count
    {
        return Err(IndexError::InvalidFormat("path block descriptor"));
    }
    Ok(rows)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DocumentRecord {
    pub(crate) ordinal: u64,
    pub(crate) document: DocumentRef,
}

/// Streams the common ordinal-to-document component. Ordinals are assigned in
/// path order while sealing a run and are never repeated in engine blocks.
pub(crate) struct DocumentComponentWriter {
    kind: IndexKind,
    level: u8,
    target_bytes: usize,
    rows: Vec<DocumentRecord>,
    estimated_bytes: usize,
    next_ordinal: u64,
    tree: RoutingTreeBuilder,
}

impl DocumentComponentWriter {
    pub(crate) fn new(kind: IndexKind, level: u8, target_bytes: usize) -> Self {
        Self::with_ordinal_base(kind, level, target_bytes, 0)
    }

    pub(crate) fn with_ordinal_base(
        kind: IndexKind,
        level: u8,
        target_bytes: usize,
        ordinal_base: u64,
    ) -> Self {
        Self {
            kind,
            level,
            target_bytes: target_bytes.max(256),
            rows: Vec::new(),
            estimated_bytes: 0,
            next_ordinal: ordinal_base,
            tree: RoutingTreeBuilder::new(kind, DOCUMENTS_TAG),
        }
    }

    pub(crate) async fn push<S: IndexBlockSink>(
        &mut self,
        row: DocumentRecord,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        row.document.validate()?;
        if row.ordinal != self.next_ordinal {
            return Err(IndexError::InvalidDefinition(
                "document ordinals must be contiguous from the writer base".into(),
            ));
        }
        let encoded_row_bytes = row.document.path.len().saturating_add(24);
        let resident_row_bytes = row
            .document
            .path
            .capacity()
            .saturating_add(2 * std::mem::size_of::<DocumentRecord>());
        let row_bytes = encoded_row_bytes.max(resident_row_bytes);
        if !self.rows.is_empty()
            && self.estimated_bytes.saturating_add(row_bytes) > self.target_bytes
        {
            self.flush(sink).await?;
        }
        self.estimated_bytes = self.estimated_bytes.saturating_add(row_bytes);
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        self.rows.push(row);
        Ok(())
    }

    async fn flush<S: IndexBlockSink>(&mut self, sink: &mut S) -> Result<(), IndexError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        self.estimated_bytes = 0;
        let first = ordinal_key(rows.first().unwrap().ordinal);
        let last = ordinal_key(rows.last().unwrap().ordinal);
        let codec = if self.level == 0 {
            ComponentCodec::FixedRows
        } else {
            ComponentCodec::PrefixEliasFano
        };
        let body = if self.level == 0 {
            encode_document_rows_fixed(&rows)?
        } else {
            encode_document_rows_succinct(&rows)?
        };
        let bytes = encode_component(self.kind, DOCUMENTS_TAG, codec, body)?;
        self.tree
            .emit_leaf(
                GeneratedBlock::new(
                    self.kind,
                    DOCUMENTS_TAG,
                    codec,
                    0,
                    first,
                    last,
                    rows.len() as u64,
                    bytes,
                )?,
                sink,
            )
            .await
    }

    pub(crate) async fn finish<S: IndexBlockSink>(
        mut self,
        sink: &mut S,
    ) -> Result<ComponentTree, IndexError> {
        self.flush(sink).await?;
        self.tree.finish(sink).await
    }
}

fn ordinal_key(ordinal: u64) -> Vec<u8> {
    ordinal.to_be_bytes().to_vec()
}

fn encode_document_rows_fixed(rows: &[DocumentRecord]) -> Result<Vec<u8>, IndexError> {
    let mut output = Encoder::default();
    output.u32(rows.len())?;
    for row in rows {
        output.u64(row.ordinal);
        output.string(&row.document.path)?;
        output.u64(row.document.version);
    }
    Ok(output.finish())
}

fn encode_document_rows_succinct(rows: &[DocumentRecord]) -> Result<Vec<u8>, IndexError> {
    let ordinals = rows.iter().map(|row| row.ordinal).collect::<Vec<_>>();
    let paths = rows
        .iter()
        .map(|row| row.document.path.clone())
        .collect::<Vec<_>>();
    let mut output = Encoder::default();
    output.u32(rows.len())?;
    output.bytes(&crate::succinct::encode_elias_fano(&ordinals)?)?;
    output.bytes(&encode_prefix_dictionary(&paths)?)?;
    for row in rows {
        output.u64(row.document.version);
    }
    Ok(output.finish())
}

pub(crate) async fn read_document_block<D: IndexDirectoryRead>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
) -> Result<Vec<DocumentRecord>, IndexError> {
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let descriptor = descriptor.clone();
    directory
        .run_query_cpu(move || {
            let rows = match descriptor.codec {
                ComponentCodec::FixedRows => decode_document_rows_fixed(block.body())?,
                ComponentCodec::PrefixEliasFano => decode_document_rows_succinct(block.body())?,
                _ => return Err(IndexError::InvalidFormat("document block codec")),
            };
            validate_document_block(rows, &descriptor)
        })
        .await
}

pub(crate) async fn read_document_block_parallel<D, E>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
    executor: &E,
    progress: &crate::compaction::CompactionProgress,
) -> Result<Vec<DocumentRecord>, IndexError>
where
    D: IndexDirectoryRead,
    E: crate::compaction::CompactionExecutor,
{
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let descriptor = descriptor.clone();
    let rows = executor
        .run_cpu(move || {
            let rows = match descriptor.codec {
                ComponentCodec::FixedRows => decode_document_rows_fixed(block.body())?,
                ComponentCodec::PrefixEliasFano => decode_document_rows_succinct(block.body())?,
                _ => return Err(IndexError::InvalidFormat("document block codec")),
            };
            validate_document_block(rows, &descriptor)
        })
        .await?;
    progress.record_input(rows.len() as u64, 0, 0);
    Ok(rows)
}

fn validate_document_block(
    rows: Vec<DocumentRecord>,
    descriptor: &crate::BlockDescriptor,
) -> Result<Vec<DocumentRecord>, IndexError> {
    if rows.first().map(|row| ordinal_key(row.ordinal)) != Some(descriptor.minimum_key.clone())
        || rows.last().map(|row| ordinal_key(row.ordinal)) != Some(descriptor.maximum_key.clone())
        || rows.len() as u64 != descriptor.element_count
    {
        return Err(IndexError::InvalidFormat("document block descriptor"));
    }
    Ok(rows)
}

fn decode_document_rows_fixed(bytes: &[u8]) -> Result<Vec<DocumentRecord>, IndexError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u32()? as usize;
    decoder.guard_count::<DocumentRecord>(count, 20)?;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        rows.push(DocumentRecord {
            ordinal: decoder.u64()?,
            document: DocumentRef {
                path: decoder.string()?,
                version: decoder.u64()?,
            },
        });
    }
    decoder.finish()?;
    validate_document_rows(rows)
}

fn decode_document_rows_succinct(bytes: &[u8]) -> Result<Vec<DocumentRecord>, IndexError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u32()? as usize;
    decoder.guard_count::<DocumentRecord>(count, 0)?;
    let ordinals_budget = decoder.budget();
    let ordinals =
        crate::succinct::decode_elias_fano_with_budget(decoder.bytes()?, ordinals_budget)?;
    let paths_budget = decoder.budget();
    let paths = decode_prefix_dictionary_with_budget(decoder.bytes()?, paths_budget)?;
    if ordinals.len() != count || paths.len() != count {
        return Err(IndexError::InvalidFormat("document column count"));
    }
    let mut rows = Vec::with_capacity(count);
    for index in 0..count {
        let path = paths.get(index)?;
        decoder.charge(path.len())?;
        rows.push(DocumentRecord {
            ordinal: ordinals.get(index)?,
            document: DocumentRef {
                path,
                version: decoder.u64()?,
            },
        });
    }
    decoder.finish()?;
    validate_document_rows(rows)
}

fn validate_document_rows(rows: Vec<DocumentRecord>) -> Result<Vec<DocumentRecord>, IndexError> {
    if rows.is_empty()
        || rows.windows(2).any(|pair| {
            pair[0].ordinal.checked_add(1) != Some(pair[1].ordinal)
                || pair[0].document.path >= pair[1].document.path
        })
        || rows.iter().any(|row| row.document.validate().is_err())
    {
        return Err(IndexError::InvalidFormat("canonical document rows"));
    }
    Ok(rows)
}

pub(crate) async fn document_by_ordinal<D: IndexDirectoryRead>(
    directory: &D,
    view: &crate::run::RunView,
    ordinal: u64,
) -> Result<DocumentRef, IndexError> {
    let root = view.component(DOCUMENTS_TAG)?;
    let key = ordinal_key(ordinal);
    let descriptor = crate::run::find_leaf(directory, root, &key)
        .await?
        .ok_or(IndexError::InvalidFormat("document ordinal outside run"))?;
    let rows = read_document_block(directory, &descriptor).await?;
    let index = rows
        .binary_search_by_key(&ordinal, |row| row.ordinal)
        .map_err(|_| IndexError::InvalidFormat("missing document ordinal"))?;
    Ok(rows[index].document.clone())
}

pub(crate) async fn latest_path_change<D: IndexDirectoryRead>(
    runs: &[D],
    views: &[crate::run::RunView],
    path: &str,
) -> Result<Option<(usize, PathChange)>, IndexError> {
    if runs.len() != views.len() {
        return Err(IndexError::InvalidDefinition(
            "run readers and descriptors must have equal length".into(),
        ));
    }
    let mut winner = None::<(usize, PathChange)>;
    for (run_index, (run, view)) in runs.iter().zip(views).enumerate() {
        let root = view.component(PATH_CHANGES_TAG)?;
        let Some(candidate) = path_change_in_tree(run, root, path).await? else {
            continue;
        };
        if winner.as_ref().is_none_or(|(current_index, current)| {
            candidate.document.version > current.document.version
                || (candidate.document.version == current.document.version
                    && run_index < *current_index)
        }) {
            winner = Some((run_index, candidate));
        }
    }
    Ok(winner)
}

pub(crate) async fn path_change_in_tree<D: IndexDirectoryRead>(
    directory: &D,
    root: &crate::BlockDescriptor,
    path: &str,
) -> Result<Option<PathChange>, IndexError> {
    let Some(descriptor) = crate::run::find_leaf(directory, root, path.as_bytes()).await? else {
        return Ok(None);
    };
    let rows = read_path_block(directory, &descriptor).await?;
    let Ok(index) = rows.binary_search_by(|row| row.document.path.as_str().cmp(path)) else {
        return Ok(None);
    };
    Ok(Some(rows[index].clone()))
}

/// Small per-query reuse of exact latest-version decisions. Immutable runs
/// make a cached answer authoritative for the lifetime of one query, while a
/// fixed FIFO bound prevents broad scans from retaining one entry per object.
pub(crate) struct LatestLiveProbe {
    entries: HashMap<String, Option<CachedPathChange>>,
    insertion_order: VecDeque<String>,
}

#[derive(Clone, Copy)]
struct CachedPathChange {
    run_index: usize,
    version: u64,
    state: DocumentState,
    document_ordinal: Option<u64>,
}

impl LatestLiveProbe {
    pub(crate) fn new() -> Self {
        Self {
            entries: HashMap::with_capacity(LATEST_LIVE_PROBE_CACHE_ENTRIES),
            insertion_order: VecDeque::with_capacity(LATEST_LIVE_PROBE_CACHE_ENTRIES),
        }
    }

    pub(crate) async fn latest_change<D: IndexDirectoryRead>(
        &mut self,
        runs: &[D],
        views: &[crate::run::RunView],
        path: &str,
    ) -> Result<Option<(usize, PathChange)>, IndexError> {
        if let Some(cached) = self.entries.get(path) {
            return Ok(cached.map(|change| change.materialize(path)));
        }
        let latest = latest_path_change(runs, views, path).await?;
        let cached = latest.as_ref().map(|(run_index, change)| CachedPathChange {
            run_index: *run_index,
            version: change.document.version,
            state: change.state,
            document_ordinal: change.document_ordinal,
        });
        self.remember(path.to_owned(), cached);
        Ok(latest)
    }

    pub(crate) async fn is_latest_live<D: IndexDirectoryRead>(
        &mut self,
        runs: &[D],
        views: &[crate::run::RunView],
        candidate: &DocumentRef,
    ) -> Result<bool, IndexError> {
        Ok(self
            .latest_change(runs, views, &candidate.path)
            .await?
            .is_some_and(|(_, change)| {
                change.state == DocumentState::Live && change.document.version == candidate.version
            }))
    }

    fn remember(&mut self, path: String, change: Option<CachedPathChange>) {
        if self.entries.contains_key(&path) {
            return;
        }
        if self.entries.len() == LATEST_LIVE_PROBE_CACHE_ENTRIES
            && let Some(oldest) = self.insertion_order.pop_front()
        {
            self.entries.remove(&oldest);
        }
        self.insertion_order.push_back(path.clone());
        self.entries.insert(path, change);
    }
}

impl CachedPathChange {
    fn materialize(self, path: &str) -> (usize, PathChange) {
        (
            self.run_index,
            PathChange {
                document: DocumentRef {
                    path: path.to_owned(),
                    version: self.version,
                },
                state: self.state,
                document_ordinal: self.document_ordinal,
            },
        )
    }
}

pub(crate) struct PathRunCursor<'a, D> {
    directory: &'a D,
    leaves: crate::run::LeafCursor<'a, D>,
    rows: Vec<PathChange>,
    next_row: usize,
    range: Option<crate::compaction::KeyRange>,
}

impl<'a, D: IndexDirectoryRead> PathRunCursor<'a, D> {
    pub(crate) fn new(directory: &'a D, root: crate::BlockDescriptor) -> Self {
        Self {
            directory,
            leaves: crate::run::LeafCursor::new(directory, root),
            rows: Vec::new(),
            next_row: 0,
            range: None,
        }
    }

    pub(crate) fn in_range(
        directory: &'a D,
        root: crate::BlockDescriptor,
        range: crate::compaction::KeyRange,
    ) -> Self {
        Self {
            directory,
            leaves: crate::run::LeafCursor::in_range(directory, root, range.clone()),
            rows: Vec::new(),
            next_row: 0,
            range: Some(range),
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<PathChange>, IndexError> {
        loop {
            if let Some(row) = self.rows.get(self.next_row).cloned() {
                self.next_row += 1;
                if self
                    .range
                    .as_ref()
                    .is_none_or(|range| range.contains(row.document.path.as_bytes()))
                {
                    return Ok(Some(row));
                }
                continue;
            }
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            self.rows = read_path_block(self.directory, &descriptor).await?;
            self.next_row = 0;
        }
    }

    pub(crate) async fn next_parallel<E: crate::compaction::CompactionExecutor>(
        &mut self,
        executor: &E,
        progress: &crate::compaction::CompactionProgress,
    ) -> Result<Option<PathChange>, IndexError> {
        loop {
            if let Some(row) = self.rows.get(self.next_row).cloned() {
                self.next_row += 1;
                if self
                    .range
                    .as_ref()
                    .is_none_or(|range| range.contains(row.document.path.as_bytes()))
                {
                    return Ok(Some(row));
                }
                continue;
            }
            // Release the exhausted decoded leaf before fetching/decoding its
            // replacement so the lane never retains both at once.
            self.rows = Vec::new();
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            self.rows =
                read_path_block_parallel(self.directory, &descriptor, executor, progress).await?;
            self.next_row = 0;
        }
    }
}

fn decode_path_rows_fixed(bytes: &[u8]) -> Result<Vec<PathChange>, IndexError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u32()? as usize;
    decoder.guard_count::<PathChange>(count, 21)?;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let path = decoder.string()?;
        let version = decoder.u64()?;
        let state = if decoder.bool()? {
            DocumentState::Live
        } else {
            DocumentState::Removed
        };
        let ordinal = decoder.u64()?;
        rows.push(PathChange {
            document: DocumentRef { path, version },
            state,
            document_ordinal: (ordinal != u64::MAX).then_some(ordinal),
        });
    }
    decoder.finish()?;
    validate_path_rows(rows)
}

fn decode_path_rows_succinct(bytes: &[u8]) -> Result<Vec<PathChange>, IndexError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u32()? as usize;
    decoder.guard_count::<PathChange>(count, 0)?;
    let paths_budget = decoder.budget();
    let paths = decode_prefix_dictionary_with_budget(decoder.bytes()?, paths_budget)?;
    if paths.len() != count {
        return Err(IndexError::InvalidFormat("path dictionary count"));
    }
    decoder.guard_count::<u64>(count, 8)?;
    let mut versions = Vec::with_capacity(count);
    for _ in 0..count {
        versions.push(decoder.u64()?);
    }
    let state_count = decoder.u32()? as usize;
    if state_count != count.div_ceil(64) {
        return Err(IndexError::InvalidFormat("path state words"));
    }
    decoder.guard_count::<u64>(state_count, 8)?;
    let mut states = Vec::with_capacity(state_count);
    for _ in 0..state_count {
        states.push(decoder.u64()?);
    }
    if count % 64 != 0 && states.last().is_some_and(|word| *word >> (count % 64) != 0) {
        return Err(IndexError::InvalidFormat("path state padding"));
    }
    decoder.guard_count::<u64>(count, 8)?;
    let mut ordinals = Vec::with_capacity(count);
    for _ in 0..count {
        ordinals.push(decoder.u64()?);
    }
    decoder.finish()?;
    let mut rows = Vec::with_capacity(count);
    for index in 0..count {
        let state = if states[index / 64] & (1u64 << (index % 64)) != 0 {
            DocumentState::Live
        } else {
            DocumentState::Removed
        };
        let path = paths.get(index)?;
        decoder.charge(path.len())?;
        rows.push(PathChange {
            document: DocumentRef {
                path,
                version: versions[index],
            },
            state,
            document_ordinal: (ordinals[index] != u64::MAX).then_some(ordinals[index]),
        });
    }
    validate_path_rows(rows)
}

fn validate_path_rows(rows: Vec<PathChange>) -> Result<Vec<PathChange>, IndexError> {
    if rows.is_empty()
        || rows
            .windows(2)
            .any(|pair| pair[0].document.path >= pair[1].document.path)
        || rows.iter().any(|row| {
            row.document.path.is_empty()
                || row.document.path.contains('\0')
                || (row.state == DocumentState::Removed && row.document_ordinal.is_some())
                || (row.state == DocumentState::Live && row.document_ordinal.is_none())
        })
    {
        return Err(IndexError::InvalidFormat("canonical path rows"));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use crate::io::tests::{MemoryBlockSink, MemoryDirectory};
    use crate::run::{RunStatistics, open_run, open_views, seal_run_root};

    use super::*;

    async fn common_run(
        level: u8,
        changes: &[(&str, u64, DocumentState)],
    ) -> (MemoryBlockSink, crate::SealedRun) {
        let mut sink = MemoryBlockSink::default();
        let mut paths = PathComponentWriter::new(IndexKind::FullText, level, 96);
        let mut documents = DocumentComponentWriter::new(IndexKind::FullText, level, 96);
        let mut ordinal = 0u64;
        let mut live = 0u64;
        for (path, version, state) in changes {
            let document = DocumentRef {
                path: (*path).into(),
                version: *version,
            };
            let document_ordinal = (*state == DocumentState::Live).then_some(ordinal);
            paths
                .push(
                    PathChange {
                        document: document.clone(),
                        state: *state,
                        document_ordinal,
                    },
                    &mut sink,
                )
                .await
                .unwrap();
            if let Some(document_ordinal) = document_ordinal {
                documents
                    .push(
                        DocumentRecord {
                            ordinal: document_ordinal,
                            document,
                        },
                        &mut sink,
                    )
                    .await
                    .unwrap();
                ordinal += 1;
                live += 1;
            }
        }
        let path_tree = paths.finish(&mut sink).await.unwrap();
        let document_tree = documents.finish(&mut sink).await.unwrap();
        let run = seal_run_root(
            IndexKind::FullText,
            level,
            RunStatistics {
                mutation_count: changes.len() as u64,
                live_document_count: live,
                minimum_version: changes
                    .iter()
                    .map(|(_, version, _)| *version)
                    .min()
                    .unwrap(),
                maximum_version: changes
                    .iter()
                    .map(|(_, version, _)| *version)
                    .max()
                    .unwrap(),
            },
            [path_tree, document_tree],
        )
        .unwrap();
        (sink, run)
    }

    fn directory(sink: &MemoryBlockSink, run: crate::SealedRun) -> MemoryDirectory {
        sink.directory_with_root(run.into_root())
    }

    #[tokio::test]
    async fn document_lookup_is_lazy_across_fixed_and_succinct_blocks() {
        let changes = (0..80)
            .map(|index| {
                (
                    format!("/objects/{index:04}"),
                    index + 1,
                    DocumentState::Live,
                )
            })
            .collect::<Vec<_>>();
        let borrowed = changes
            .iter()
            .map(|(path, version, state)| (path.as_str(), *version, *state))
            .collect::<Vec<_>>();
        for level in [0, 1] {
            let (sink, run) = common_run(level, &borrowed).await;
            assert!(sink.len() > 4);
            let directory = directory(&sink, run);
            let view = open_run(&directory, IndexKind::FullText).await.unwrap();
            assert_eq!(
                document_by_ordinal(&directory, &view, 67).await.unwrap(),
                DocumentRef {
                    path: "/objects/0067".into(),
                    version: 68,
                }
            );
        }
    }

    #[tokio::test]
    async fn latest_exact_path_version_and_tombstone_control_visibility() {
        let (old_sink, old) = common_run(
            0,
            &[
                ("/a", 1, DocumentState::Live),
                ("/b", 1, DocumentState::Live),
            ],
        )
        .await;
        let old = directory(&old_sink, old);
        let (new_sink, new) = common_run(
            0,
            &[
                ("/a", 2, DocumentState::Live),
                ("/b", 2, DocumentState::Removed),
            ],
        )
        .await;
        let new = directory(&new_sink, new);
        let runs = [new, old];
        let views = open_views(&runs, IndexKind::FullText).await.unwrap();
        let mut probe = LatestLiveProbe::new();
        assert!(
            probe
                .is_latest_live(
                    &runs,
                    &views,
                    &DocumentRef {
                        path: "/a".into(),
                        version: 2,
                    },
                )
                .await
                .unwrap()
        );
        assert!(
            !probe
                .is_latest_live(
                    &runs,
                    &views,
                    &DocumentRef {
                        path: "/a".into(),
                        version: 1,
                    },
                )
                .await
                .unwrap()
        );
        assert!(
            !probe
                .is_latest_live(
                    &runs,
                    &views,
                    &DocumentRef {
                        path: "/b".into(),
                        version: 1,
                    },
                )
                .await
                .unwrap()
        );
        assert_eq!(probe.entries.len(), 2);
    }

    #[test]
    fn latest_live_probe_cache_has_a_fixed_fifo_bound() {
        let mut probe = LatestLiveProbe::new();
        for index in 0..LATEST_LIVE_PROBE_CACHE_ENTRIES + 8 {
            probe.remember(format!("/objects/{index}"), None);
        }
        assert_eq!(probe.entries.len(), LATEST_LIVE_PROBE_CACHE_ENTRIES);
        assert!(!probe.entries.contains_key("/objects/0"));
        assert!(
            probe
                .entries
                .contains_key(&format!("/objects/{}", LATEST_LIVE_PROBE_CACHE_ENTRIES + 7))
        );
    }
}
