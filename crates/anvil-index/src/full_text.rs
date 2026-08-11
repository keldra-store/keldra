//! Bounded immutable positional full-text runs.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::codec::{Decoder, Encoder, encode_component};
use crate::run::{
    ComponentTree, LeafCursor, RoutingTreeBuilder, RunStatistics, RunView, open_views,
    seal_run_root,
};
use crate::segment::{
    DEFAULT_COMPONENT_BLOCK_BYTES, DocumentComponentWriter, DocumentRecord, DocumentState,
    MutationBuffer, PATH_CHANGES_TAG, PathChange, PathComponentWriter, PathRunCursor,
    document_by_ordinal, is_latest_live, latest_path_change, path_change_in_tree,
};
use crate::succinct::{
    decode_elias_fano_with_budget, decode_prefix_dictionary_with_budget, encode_elias_fano,
    encode_prefix_dictionary,
};
use crate::{
    BlockDescriptor, ComponentCodec, DocumentRef, GeneratedBlock, IndexBlockSink,
    IndexDirectoryRead, IndexError, IndexKind, IndexMutation, SealedRun, SegmentBuildOptions,
    SegmentPush,
};

pub(crate) const FULL_TEXT_POSTINGS_TAG: u8 = 30;
const MAX_TOKEN_CHARS: usize = 128;
const MAX_FIELD_BYTES: usize = 256;
const POSTING_CHARGE_BYTES: usize = 256;
const MAX_PHRASE_POSITION_BYTES: usize = crate::MAX_INDEX_BLOCK_BYTES;
pub(crate) const MAX_QUERY_TERM_CURSORS: usize = crate::INDEX_ROUTING_FANOUT;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullTextDocument {
    pub document: DocumentRef,
    pub fields: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FullTextQuery<'a> {
    pub text: &'a str,
    pub fields: &'a [String],
    pub phrase: bool,
    pub match_all_terms: bool,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FullTextHit {
    pub document: DocumentRef,
    pub score: f32,
    pub matched_terms: u32,
}

/// Exclusive continuation key in full-text result order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FullTextQueryCursor {
    pub score: f32,
    pub document: DocumentRef,
}

impl FullTextQueryCursor {
    pub fn from_hit(hit: &FullTextHit) -> Self {
        Self {
            score: hit.score,
            document: hit.document.clone(),
        }
    }
}

pub struct FullTextSegmentBuilder {
    buffer: MutationBuffer<FullTextDocument>,
}

impl FullTextSegmentBuilder {
    pub fn new(options: SegmentBuildOptions) -> Result<Self, IndexError> {
        Ok(Self {
            buffer: MutationBuffer::new(options)?,
        })
    }

    pub fn estimate_mutation(mutation: &IndexMutation<FullTextDocument>) -> usize {
        match mutation {
            IndexMutation::Remove(document) => document.path.len(),
            IndexMutation::Upsert(document) => document
                .document
                .path
                .len()
                .saturating_add(estimate_text_fields(&document.fields)),
        }
    }

    pub fn try_push(
        &mut self,
        mutation: IndexMutation<FullTextDocument>,
    ) -> Result<SegmentPush<FullTextDocument>, IndexError> {
        if let IndexMutation::Upsert(document) = &mutation {
            validate_fields(&document.fields)?;
        }
        let estimate = Self::estimate_mutation(&mutation);
        self.buffer
            .try_push(mutation, estimate, |document| &document.document)
    }

    pub fn resident_bytes(&self) -> usize {
        self.buffer.resident_bytes()
    }

    pub fn seal_workspace_bytes(&self) -> Result<usize, IndexError> {
        self.buffer.seal_workspace_bytes()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub async fn seal<S: IndexBlockSink>(
        self,
        sink: &mut S,
    ) -> Result<Option<SealedRun>, IndexError> {
        self.seal_with_target(sink, DEFAULT_COMPONENT_BLOCK_BYTES)
            .await
    }

    async fn seal_with_target<S: IndexBlockSink>(
        self,
        sink: &mut S,
        target_bytes: usize,
    ) -> Result<Option<SealedRun>, IndexError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        let level = self.buffer.level();
        let entries = self.buffer.into_entries();
        let postings = collect_postings(&entries)?;
        let mut paths = PathComponentWriter::new(IndexKind::FullText, level, target_bytes);
        let mut documents = DocumentComponentWriter::new(IndexKind::FullText, level, target_bytes);
        let mut live = 0u64;
        let mut minimum_version = u64::MAX;
        let mut maximum_version = 0u64;
        for entry in entries.values() {
            let ordinal = matches!(entry.mutation, IndexMutation::Upsert(_)).then_some(live);
            let change =
                PathChange::from_mutation(&entry.mutation, |document| &document.document, ordinal);
            minimum_version = minimum_version.min(change.document.version);
            maximum_version = maximum_version.max(change.document.version);
            paths.push(change.clone(), sink).await?;
            if let Some(ordinal) = ordinal {
                documents
                    .push(
                        DocumentRecord {
                            ordinal,
                            document: change.document,
                        },
                        sink,
                    )
                    .await?;
                live += 1;
            }
        }
        let mutation_count = entries.len() as u64;
        let mut components = vec![paths.finish(sink).await?];
        if live > 0 {
            components.push(documents.finish(sink).await?);
        }
        if !postings.is_empty() {
            let mut writer = TextComponentWriter::new(
                IndexKind::FullText,
                FULL_TEXT_POSTINGS_TAG,
                level,
                target_bytes,
            );
            for (term, rows) in postings {
                for row in rows {
                    writer.push(&term, row, sink).await?;
                }
            }
            components.push(writer.finish(sink).await?);
        }
        Ok(Some(seal_run_root(
            IndexKind::FullText,
            level,
            RunStatistics {
                mutation_count,
                live_document_count: live,
                minimum_version,
                maximum_version,
            },
            components,
        )?))
    }
}

pub struct FullTextEngine;

impl FullTextEngine {
    pub fn builder(options: SegmentBuildOptions) -> Result<FullTextSegmentBuilder, IndexError> {
        FullTextSegmentBuilder::new(options)
    }

    pub async fn query<D: IndexDirectoryRead>(
        runs: &[D],
        query: FullTextQuery<'_>,
    ) -> Result<Vec<FullTextHit>, IndexError> {
        Self::query_after(runs, query, None).await
    }

    pub async fn query_after<D: IndexDirectoryRead>(
        runs: &[D],
        query: FullTextQuery<'_>,
        after: Option<&FullTextQueryCursor>,
    ) -> Result<Vec<FullTextHit>, IndexError> {
        validate_score_cursor(after.map(|cursor| (cursor.score, &cursor.document)))?;
        if query.limit == 0 || runs.is_empty() {
            return Ok(Vec::new());
        }
        let (phrase_terms, unique_terms) = query_terms(query.text, query.phrase)?;
        if phrase_terms.is_empty() {
            return Err(IndexError::InvalidQuery(
                "full-text query contains no indexable terms".into(),
            ));
        }
        let selected_fields = Arc::new(query.fields.iter().cloned().collect::<BTreeSet<_>>());
        let views = open_views(runs, IndexKind::FullText).await?;
        let mut hits = Vec::with_capacity(query.limit.min(128));
        for (run, view) in runs.iter().zip(&views) {
            let Some(root) = view.component_optional(FULL_TEXT_POSTINGS_TAG) else {
                continue;
            };
            let mut cursors = Vec::with_capacity(unique_terms.len());
            for term in &unique_terms {
                cursors.push(TermCursor::new(
                    run,
                    root.clone(),
                    term.clone(),
                    Arc::clone(&selected_fields),
                    query.phrase,
                ));
            }
            let mut candidates = RunCandidateCursor::new(
                cursors,
                phrase_terms.clone(),
                unique_terms.clone(),
                query.phrase,
                query.match_all_terms,
            )
            .await?;
            while let Some(candidate) = candidates.next().await? {
                let document = document_by_ordinal(run, view, candidate.ordinal).await?;
                if !is_latest_live(runs, &views, &document).await? {
                    continue;
                }
                let hit = FullTextHit {
                    document,
                    score: candidate.score,
                    matched_terms: candidate.matched_terms,
                };
                if after
                    .is_some_and(|cursor| compare_score_cursor(&hit, cursor) != Ordering::Greater)
                {
                    continue;
                }
                insert_bounded(&mut hits, hit, query.limit);
            }
        }
        sort_hits(&mut hits);
        Ok(hits)
    }

    pub async fn merge_runs<D, S>(
        runs: &[D],
        output_level: u8,
        sink: &mut S,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead,
        S: IndexBlockSink + IndexDirectoryRead,
    {
        Self::merge_with_target(runs, output_level, DEFAULT_COMPONENT_BLOCK_BYTES, sink).await
    }

    async fn merge_with_target<D, S>(
        runs: &[D],
        output_level: u8,
        target_bytes: usize,
        sink: &mut S,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead,
        S: IndexBlockSink + IndexDirectoryRead,
    {
        if runs.is_empty() || output_level == 0 {
            return Err(IndexError::InvalidDefinition(
                "full-text compaction requires input runs and an L1+ output level".into(),
            ));
        }
        let views = open_views(runs, IndexKind::FullText).await?;
        let (path_tree, document_tree, statistics) =
            merge_common_components(runs, &views, output_level, target_bytes, sink).await?;
        let text_tree = merge_text_component(
            IndexKind::FullText,
            FULL_TEXT_POSTINGS_TAG,
            runs,
            &views,
            &path_tree,
            output_level,
            target_bytes,
            sink,
        )
        .await?;
        let mut components = vec![path_tree];
        if let Some(tree) = document_tree {
            components.push(tree);
        }
        if let Some(tree) = text_tree {
            components.push(tree);
        }
        seal_run_root(IndexKind::FullText, output_level, statistics, components)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TextPosting {
    pub(crate) ordinal: u64,
    pub(crate) field: String,
    pub(crate) positions: Vec<u32>,
    pub(crate) field_length: u32,
}

fn collect_postings(
    entries: &BTreeMap<String, crate::segment::PendingMutation<FullTextDocument>>,
) -> Result<BTreeMap<String, Vec<TextPosting>>, IndexError> {
    let mut terms = BTreeMap::<String, Vec<TextPosting>>::new();
    let mut ordinal = 0u64;
    for entry in entries.values() {
        let IndexMutation::Upsert(document) = &entry.mutation else {
            continue;
        };
        validate_fields(&document.fields)?;
        for (field, text) in &document.fields {
            let tokens = tokenize(text);
            let field_length = u32::try_from(tokens.len()).unwrap_or(u32::MAX);
            let mut by_term = BTreeMap::<String, Vec<u32>>::new();
            for (term, position) in tokens {
                by_term.entry(term).or_default().push(position);
            }
            for (term, positions) in by_term {
                terms.entry(term).or_default().push(TextPosting {
                    ordinal,
                    field: field.clone(),
                    positions,
                    field_length,
                });
            }
        }
        ordinal = ordinal.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
    }
    Ok(terms)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TextPostingRow {
    pub(crate) term: String,
    pub(crate) ordinal: u64,
    pub(crate) field: String,
    pub(crate) field_length: u32,
    pub(crate) part: u32,
    pub(crate) positions: Vec<u32>,
}

pub(crate) struct TextComponentWriter {
    kind: IndexKind,
    component_tag: u8,
    level: u8,
    target_bytes: usize,
    rows: Vec<TextPostingRow>,
    estimated_bytes: usize,
    last_key: Option<Vec<u8>>,
    tree: RoutingTreeBuilder,
}

impl TextComponentWriter {
    pub(crate) fn new(kind: IndexKind, component_tag: u8, level: u8, target_bytes: usize) -> Self {
        Self {
            kind,
            component_tag,
            level,
            target_bytes: target_bytes.max(1024),
            rows: Vec::new(),
            estimated_bytes: 0,
            last_key: None,
            tree: RoutingTreeBuilder::new(kind, component_tag),
        }
    }

    pub(crate) async fn push<S: IndexBlockSink>(
        &mut self,
        term: &str,
        posting: TextPosting,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        validate_term(term)?;
        validate_field(&posting.field)?;
        if posting.positions.is_empty()
            || posting.positions.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(IndexError::InvalidDefinition(
                "full-text positions must be non-empty and strictly ordered".into(),
            ));
        }
        let row_overhead = term
            .len()
            .saturating_mul(2)
            .saturating_add(posting.field.len().saturating_mul(2))
            .saturating_add(128);
        if row_overhead.saturating_add(5) > self.target_bytes {
            return Err(IndexError::ResourceLimit {
                needed: row_overhead.saturating_add(5),
                limit: self.target_bytes,
            });
        }
        let maximum_positions = (self.target_bytes - row_overhead) / 5;
        for (part, positions) in posting.positions.chunks(maximum_positions).enumerate() {
            self.push_row(
                TextPostingRow {
                    term: term.to_owned(),
                    ordinal: posting.ordinal,
                    field: posting.field.clone(),
                    field_length: posting.field_length,
                    part: u32::try_from(part).map_err(|_| IndexError::OffsetOverflow)?,
                    positions: positions.to_vec(),
                },
                sink,
            )
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn push_row<S: IndexBlockSink>(
        &mut self,
        row: TextPostingRow,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        let key = posting_key(&row)?;
        if self.last_key.as_ref().is_some_and(|last| last >= &key) {
            return Err(IndexError::UnsortedRecords);
        }
        let row_bytes = posting_estimate(&row);
        if row_bytes > self.target_bytes {
            return Err(IndexError::ResourceLimit {
                needed: row_bytes,
                limit: self.target_bytes,
            });
        }
        if !self.rows.is_empty()
            && self.estimated_bytes.saturating_add(row_bytes) > self.target_bytes
        {
            self.flush(sink).await?;
        }
        self.last_key = Some(key);
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
        let minimum = posting_key(rows.first().unwrap())?;
        let maximum = posting_key(rows.last().unwrap())?;
        let codec = if self.level == 0 {
            ComponentCodec::GapPostings
        } else {
            ComponentCodec::QuasiSuccinctPostings
        };
        let body = if self.level == 0 {
            encode_text_rows_fixed(&rows)?
        } else {
            encode_text_rows_succinct(&rows)?
        };
        let bytes = encode_component(self.kind, self.component_tag, codec, body)?;
        self.tree
            .emit_leaf(
                GeneratedBlock::new(
                    self.kind,
                    self.component_tag,
                    codec,
                    0,
                    minimum,
                    maximum,
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

fn posting_estimate(row: &TextPostingRow) -> usize {
    row.term
        .len()
        .saturating_mul(2)
        .saturating_add(row.field.len().saturating_mul(2))
        .saturating_add(row.positions.len().saturating_mul(5))
        .saturating_add(128)
}

fn posting_key(row: &TextPostingRow) -> Result<Vec<u8>, IndexError> {
    validate_term(&row.term)?;
    validate_field(&row.field)?;
    let mut key = Vec::with_capacity(row.term.len() + row.field.len() + 14);
    key.extend_from_slice(row.term.as_bytes());
    key.push(0);
    key.extend_from_slice(&row.ordinal.to_be_bytes());
    key.extend_from_slice(row.field.as_bytes());
    key.push(0);
    key.extend_from_slice(&row.part.to_be_bytes());
    Ok(key)
}

fn term_lower(term: &str) -> Vec<u8> {
    let mut key = term.as_bytes().to_vec();
    key.push(0);
    key
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    for index in (0..successor.len()).rev() {
        if successor[index] != u8::MAX {
            successor[index] += 1;
            successor.truncate(index + 1);
            return Some(successor);
        }
    }
    None
}

fn encode_text_rows_fixed(rows: &[TextPostingRow]) -> Result<Vec<u8>, IndexError> {
    let mut output = Encoder::default();
    output.u32(rows.len())?;
    for row in rows {
        output.string(&row.term)?;
        output.u64(row.ordinal);
        output.string(&row.field)?;
        output.raw_u32(row.field_length);
        output.raw_u32(row.part);
        output.u32(row.positions.len())?;
        let mut previous = 0u32;
        for position in &row.positions {
            encode_varint(u64::from(position - previous), &mut output);
            previous = *position;
        }
    }
    Ok(output.finish())
}

fn encode_text_rows_succinct(rows: &[TextPostingRow]) -> Result<Vec<u8>, IndexError> {
    let terms = rows
        .iter()
        .map(|row| row.term.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let fields = rows
        .iter()
        .map(|row| row.field.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let field_ids = fields
        .iter()
        .enumerate()
        .map(|(index, field)| (field.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let mut output = Encoder::default();
    output.bytes(&encode_prefix_dictionary(&terms)?)?;
    output.bytes(&encode_prefix_dictionary(&fields)?)?;
    output.u32(terms.len())?;
    for term in &terms {
        let selected = rows
            .iter()
            .filter(|row| &row.term == term)
            .collect::<Vec<_>>();
        output.u32(selected.len())?;
        output.bytes(&encode_elias_fano(
            &selected.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
        )?)?;
        for row in selected {
            output.u32(field_ids[&row.field.as_str()])?;
            output.raw_u32(row.field_length);
            output.raw_u32(row.part);
            output.u32(row.positions.len())?;
            let mut previous = 0u32;
            for position in &row.positions {
                encode_varint(u64::from(position - previous), &mut output);
                previous = *position;
            }
        }
    }
    Ok(output.finish())
}

pub(crate) async fn read_text_block<D: IndexDirectoryRead>(
    directory: &D,
    descriptor: &BlockDescriptor,
) -> Result<Vec<TextPostingRow>, IndexError> {
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let rows = match descriptor.codec {
        ComponentCodec::GapPostings => decode_text_rows_fixed(block.body())?,
        ComponentCodec::QuasiSuccinctPostings => decode_text_rows_succinct(block.body())?,
        _ => return Err(IndexError::InvalidFormat("full-text block codec")),
    };
    if rows.first().map(posting_key).transpose()? != Some(descriptor.minimum_key.clone())
        || rows.last().map(posting_key).transpose()? != Some(descriptor.maximum_key.clone())
        || rows.len() as u64 != descriptor.element_count
    {
        return Err(IndexError::InvalidFormat("full-text block descriptor"));
    }
    Ok(rows)
}

fn decode_text_rows_fixed(bytes: &[u8]) -> Result<Vec<TextPostingRow>, IndexError> {
    let mut decoder = Decoder::new(bytes);
    let count = decoder.u32()? as usize;
    decoder.guard_count::<TextPostingRow>(count, 28)?;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        rows.push(decode_text_row(
            decoder.string()?,
            decoder.u64()?,
            decoder.string()?,
            decoder.u32()?,
            decoder.u32()?,
            &mut decoder,
        )?);
    }
    decoder.finish()?;
    validate_text_rows(rows)
}

fn decode_text_rows_succinct(bytes: &[u8]) -> Result<Vec<TextPostingRow>, IndexError> {
    let mut decoder = Decoder::new(bytes);
    let terms_budget = decoder.budget();
    let terms = decode_prefix_dictionary_with_budget(decoder.bytes()?, terms_budget)?;
    let fields_budget = decoder.budget();
    let fields = decode_prefix_dictionary_with_budget(decoder.bytes()?, fields_budget)?;
    let term_count = decoder.u32()? as usize;
    if term_count != terms.len() {
        return Err(IndexError::InvalidFormat("full-text term count"));
    }
    let mut rows = Vec::new();
    for term_index in 0..term_count {
        let count = decoder.u32()? as usize;
        decoder.guard_count::<TextPostingRow>(count, 0)?;
        let ordinals_budget = decoder.budget();
        let ordinals = decode_elias_fano_with_budget(decoder.bytes()?, ordinals_budget)?;
        if ordinals.len() != count {
            return Err(IndexError::InvalidFormat("full-text posting count"));
        }
        for index in 0..count {
            let field = fields.get(decoder.u32()? as usize)?;
            let term = terms.get(term_index)?;
            decoder.charge(
                term.len()
                    .checked_add(field.len())
                    .ok_or(IndexError::OffsetOverflow)?,
            )?;
            rows.push(decode_text_row(
                term,
                ordinals.get(index)?,
                field,
                decoder.u32()?,
                decoder.u32()?,
                &mut decoder,
            )?);
        }
    }
    decoder.finish()?;
    validate_text_rows(rows)
}

fn decode_text_row(
    term: String,
    ordinal: u64,
    field: String,
    field_length: u32,
    part: u32,
    decoder: &mut Decoder<'_>,
) -> Result<TextPostingRow, IndexError> {
    let count = decoder.u32()? as usize;
    decoder.guard_count::<u32>(count, 1)?;
    let mut positions = Vec::with_capacity(count);
    let mut position = 0u32;
    for _ in 0..count {
        let gap = u32::try_from(decode_varint(decoder)?)
            .map_err(|_| IndexError::InvalidFormat("full-text position gap"))?;
        position = position
            .checked_add(gap)
            .ok_or(IndexError::InvalidFormat("full-text position overflow"))?;
        positions.push(position);
    }
    Ok(TextPostingRow {
        term,
        ordinal,
        field,
        field_length,
        part,
        positions,
    })
}

fn validate_text_rows(rows: Vec<TextPostingRow>) -> Result<Vec<TextPostingRow>, IndexError> {
    if rows.is_empty()
        || rows.iter().any(|row| {
            validate_term(&row.term).is_err()
                || validate_field(&row.field).is_err()
                || row.positions.is_empty()
                || row.positions.windows(2).any(|pair| pair[0] >= pair[1])
        })
        || rows.windows(2).any(|pair| {
            posting_key(&pair[0])
                .ok()
                .zip(posting_key(&pair[1]).ok())
                .is_none_or(|(left, right)| left >= right)
        })
    {
        return Err(IndexError::InvalidFormat("canonical full-text rows"));
    }
    Ok(rows)
}

pub(crate) struct TextRowCursor<'a, D> {
    directory: &'a D,
    leaves: LeafCursor<'a, D>,
    rows: Vec<TextPostingRow>,
    next_row: usize,
}

impl<'a, D: IndexDirectoryRead> TextRowCursor<'a, D> {
    pub(crate) fn new(directory: &'a D, root: BlockDescriptor) -> Self {
        Self {
            directory,
            leaves: LeafCursor::new(directory, root),
            rows: Vec::new(),
            next_row: 0,
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<TextPostingRow>, IndexError> {
        loop {
            if let Some(row) = self.rows.get(self.next_row).cloned() {
                self.next_row += 1;
                return Ok(Some(row));
            }
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            self.rows = read_text_block(self.directory, &descriptor).await?;
            self.next_row = 0;
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TermDocumentMatch {
    pub(crate) ordinal: u64,
    pub(crate) fields: BTreeMap<String, FieldMatch>,
}

#[derive(Clone, Debug)]
pub(crate) struct FieldMatch {
    pub(crate) length: u32,
    pub(crate) frequency: usize,
    pub(crate) positions: Vec<u32>,
}

pub(crate) struct TermCursor<'a, D> {
    directory: &'a D,
    leaves: LeafCursor<'a, D>,
    term: String,
    lower: Vec<u8>,
    upper: Option<Vec<u8>>,
    fields: Arc<BTreeSet<String>>,
    collect_positions: bool,
    rows: Vec<TextPostingRow>,
    next_row: usize,
    pending: Option<TextPostingRow>,
}

impl<'a, D: IndexDirectoryRead> TermCursor<'a, D> {
    pub(crate) fn new(
        directory: &'a D,
        root: BlockDescriptor,
        term: String,
        fields: Arc<BTreeSet<String>>,
        collect_positions: bool,
    ) -> Self {
        let lower = term_lower(&term);
        let upper = prefix_successor(&lower);
        Self {
            directory,
            leaves: LeafCursor::new(directory, root),
            term,
            lower,
            upper,
            fields,
            collect_positions,
            rows: Vec::new(),
            next_row: 0,
            pending: None,
        }
    }

    async fn next_matching_row(&mut self) -> Result<Option<TextPostingRow>, IndexError> {
        loop {
            while let Some(row) = self.rows.get(self.next_row).cloned() {
                self.next_row += 1;
                if row.term == self.term
                    && (self.fields.is_empty() || self.fields.contains(&row.field))
                {
                    return Ok(Some(row));
                }
            }
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            if self
                .upper
                .as_ref()
                .is_some_and(|upper| descriptor.minimum_key.as_slice() >= upper.as_slice())
            {
                return Ok(None);
            }
            if descriptor.maximum_key.as_slice() < self.lower.as_slice() {
                continue;
            }
            self.rows = read_text_block(self.directory, &descriptor).await?;
            self.next_row = 0;
        }
    }

    pub(crate) async fn next_document(&mut self) -> Result<Option<TermDocumentMatch>, IndexError> {
        let first = match self.pending.take() {
            Some(row) => row,
            None => match self.next_matching_row().await? {
                Some(row) => row,
                None => return Ok(None),
            },
        };
        let ordinal = first.ordinal;
        let mut fields = BTreeMap::<String, FieldMatch>::new();
        let mut position_bytes = 0usize;
        append_field_match(
            &mut fields,
            first,
            self.collect_positions,
            &mut position_bytes,
        )?;
        while let Some(row) = self.next_matching_row().await? {
            if row.ordinal != ordinal {
                self.pending = Some(row);
                break;
            }
            append_field_match(
                &mut fields,
                row,
                self.collect_positions,
                &mut position_bytes,
            )?;
        }
        Ok(Some(TermDocumentMatch { ordinal, fields }))
    }
}

fn append_field_match(
    fields: &mut BTreeMap<String, FieldMatch>,
    row: TextPostingRow,
    collect_positions: bool,
    position_bytes: &mut usize,
) -> Result<(), IndexError> {
    let field = fields.entry(row.field).or_insert_with(|| FieldMatch {
        length: row.field_length,
        frequency: 0,
        positions: Vec::new(),
    });
    if field.length != row.field_length
        || (collect_positions
            && field
                .positions
                .last()
                .is_some_and(|last| *last >= row.positions[0]))
    {
        return Err(IndexError::InvalidFormat("full-text posting parts"));
    }
    field.frequency = field
        .frequency
        .checked_add(row.positions.len())
        .ok_or(IndexError::OffsetOverflow)?;
    if collect_positions {
        let additional = row
            .positions
            .len()
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or(IndexError::OffsetOverflow)?;
        let needed = position_bytes
            .checked_add(additional)
            .ok_or(IndexError::OffsetOverflow)?;
        if needed > MAX_PHRASE_POSITION_BYTES {
            return Err(IndexError::ResourceLimit {
                needed,
                limit: MAX_PHRASE_POSITION_BYTES,
            });
        }
        *position_bytes = needed;
        field.positions.extend(row.positions);
    }
    Ok(())
}

pub(crate) struct Candidate {
    pub(crate) ordinal: u64,
    pub(crate) score: f32,
    pub(crate) matched_terms: u32,
}

pub(crate) struct RunCandidateCursor<'a, D> {
    cursors: Vec<TermCursor<'a, D>>,
    current: Vec<Option<TermDocumentMatch>>,
    phrase_terms: Vec<String>,
    unique_terms: Vec<String>,
    phrase: bool,
    match_all_terms: bool,
}

impl<'a, D: IndexDirectoryRead> RunCandidateCursor<'a, D> {
    pub(crate) async fn new(
        mut cursors: Vec<TermCursor<'a, D>>,
        phrase_terms: Vec<String>,
        unique_terms: Vec<String>,
        phrase: bool,
        match_all_terms: bool,
    ) -> Result<Self, IndexError> {
        let mut current = Vec::with_capacity(cursors.len());
        for cursor in &mut cursors {
            current.push(cursor.next_document().await?);
        }
        Ok(Self {
            cursors,
            current,
            phrase_terms,
            unique_terms,
            phrase,
            match_all_terms,
        })
    }

    pub(crate) async fn next(&mut self) -> Result<Option<Candidate>, IndexError> {
        loop {
            let Some(ordinal) = self
                .current
                .iter()
                .flatten()
                .map(|entry| entry.ordinal)
                .min()
            else {
                return Ok(None);
            };
            let mut matched = BTreeMap::<String, TermDocumentMatch>::new();
            for (index, entry) in self.current.iter_mut().enumerate() {
                if entry.as_ref().is_some_and(|entry| entry.ordinal == ordinal) {
                    matched.insert(self.unique_terms[index].clone(), entry.take().unwrap());
                    *entry = self.cursors[index].next_document().await?;
                }
            }
            let accepted = if self.phrase {
                phrase_matches(&self.phrase_terms, &matched)
            } else if self.match_all_terms {
                matched.len() == self.unique_terms.len()
            } else {
                !matched.is_empty()
            };
            if !accepted {
                continue;
            }
            let score = matched
                .values()
                .flat_map(|entry| entry.fields.values())
                .map(|field| {
                    let frequency = field.frequency as f32;
                    frequency * 2.2
                        / (frequency + 1.2 * (0.25 + 0.75 * field.length as f32 / 100.0))
                })
                .sum();
            return Ok(Some(Candidate {
                ordinal,
                score,
                matched_terms: u32::try_from(matched.len()).unwrap_or(u32::MAX),
            }));
        }
    }
}

fn phrase_matches(terms: &[String], matches: &BTreeMap<String, TermDocumentMatch>) -> bool {
    let Some(first) = terms.first().and_then(|term| matches.get(term)) else {
        return false;
    };
    first.fields.iter().any(|(field, first_match)| {
        first_match.positions.iter().any(|start| {
            terms.iter().enumerate().skip(1).all(|(offset, term)| {
                matches
                    .get(term)
                    .and_then(|entry| entry.fields.get(field))
                    .is_some_and(|entry| {
                        entry
                            .positions
                            .binary_search(&start.saturating_add(offset as u32))
                            .is_ok()
                    })
            })
        })
    })
}

async fn merge_common_components<D, S>(
    runs: &[D],
    views: &[RunView],
    output_level: u8,
    target_bytes: usize,
    sink: &mut S,
) -> Result<(ComponentTree, Option<ComponentTree>, RunStatistics), IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink,
{
    let mut cursors = Vec::with_capacity(runs.len());
    for (run, view) in runs.iter().zip(views) {
        cursors.push(PathRunCursor::new(
            run,
            view.component(PATH_CHANGES_TAG)?.clone(),
        ));
    }
    let mut current = Vec::with_capacity(cursors.len());
    for cursor in &mut cursors {
        current.push(cursor.next().await?);
    }
    let mut paths = PathComponentWriter::new(IndexKind::FullText, output_level, target_bytes);
    let mut documents =
        DocumentComponentWriter::new(IndexKind::FullText, output_level, target_bytes);
    let mut mutations = 0u64;
    let mut live = 0u64;
    let mut minimum_version = u64::MAX;
    let mut maximum_version = 0u64;
    loop {
        let Some(path) = current
            .iter()
            .flatten()
            .map(|change| change.document.path.as_str())
            .min()
            .map(str::to_owned)
        else {
            break;
        };
        let mut winner = None::<(usize, PathChange)>;
        for (run_index, row) in current.iter().enumerate() {
            let Some(row) = row.as_ref().filter(|row| row.document.path == path) else {
                continue;
            };
            if winner.as_ref().is_none_or(|(current_index, current)| {
                row.document.version > current.document.version
                    || (row.document.version == current.document.version
                        && run_index < *current_index)
            }) {
                winner = Some((run_index, row.clone()));
            }
        }
        for (run_index, row) in current.iter_mut().enumerate() {
            if row.as_ref().is_some_and(|row| row.document.path == path) {
                *row = cursors[run_index].next().await?;
            }
        }
        let mut change = winner.unwrap().1;
        if change.state == DocumentState::Live {
            change.document_ordinal = Some(live);
            documents
                .push(
                    DocumentRecord {
                        ordinal: live,
                        document: change.document.clone(),
                    },
                    sink,
                )
                .await?;
            live += 1;
        } else {
            change.document_ordinal = None;
        }
        minimum_version = minimum_version.min(change.document.version);
        maximum_version = maximum_version.max(change.document.version);
        mutations += 1;
        paths.push(change, sink).await?;
    }
    if mutations == 0 {
        return Err(IndexError::InvalidDefinition(
            "full-text compaction produced no changes".into(),
        ));
    }
    let path_tree = paths.finish(sink).await?;
    let document_tree = if live == 0 {
        None
    } else {
        Some(documents.finish(sink).await?)
    };
    Ok((
        path_tree,
        document_tree,
        RunStatistics {
            mutation_count: mutations,
            live_document_count: live,
            minimum_version,
            maximum_version,
        },
    ))
}

struct ResolvedTextRow {
    run_index: usize,
    document: DocumentRef,
    row: TextPostingRow,
}

async fn next_resolved<'a, D: IndexDirectoryRead>(
    run_index: usize,
    cursor: &mut TextRowCursor<'a, D>,
    run: &'a D,
    view: &RunView,
) -> Result<Option<ResolvedTextRow>, IndexError> {
    let Some(row) = cursor.next().await? else {
        return Ok(None);
    };
    let document = document_by_ordinal(run, view, row.ordinal).await?;
    Ok(Some(ResolvedTextRow {
        run_index,
        document,
        row,
    }))
}

pub(crate) async fn merge_text_component<D, S>(
    kind: IndexKind,
    component_tag: u8,
    runs: &[D],
    views: &[RunView],
    output_paths: &ComponentTree,
    output_level: u8,
    target_bytes: usize,
    sink: &mut S,
) -> Result<Option<ComponentTree>, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink + IndexDirectoryRead,
{
    let mut run_indices = Vec::new();
    let mut cursors = Vec::new();
    for (run_index, (run, view)) in runs.iter().zip(views).enumerate() {
        if let Some(root) = view.component_optional(component_tag) {
            run_indices.push(run_index);
            cursors.push(TextRowCursor::new(run, root.clone()));
        }
    }
    let mut current = Vec::with_capacity(cursors.len());
    for (cursor_index, cursor) in cursors.iter_mut().enumerate() {
        let run_index = run_indices[cursor_index];
        current.push(next_resolved(run_index, cursor, &runs[run_index], &views[run_index]).await?);
    }
    let mut writer = TextComponentWriter::new(kind, component_tag, output_level, target_bytes);
    let mut wrote = false;
    loop {
        let Some(selected) = current
            .iter()
            .enumerate()
            .filter_map(|(index, value)| {
                value.as_ref().map(|value| {
                    (
                        index,
                        &value.row.term,
                        &value.document.path,
                        &value.row.field,
                        value.row.part,
                    )
                })
            })
            .min_by(|left, right| {
                left.1
                    .cmp(right.1)
                    .then(left.2.cmp(right.2))
                    .then(left.3.cmp(right.3))
                    .then(left.4.cmp(&right.4))
            })
            .map(|value| value.0)
        else {
            break;
        };
        let candidate = current[selected].take().unwrap();
        let cursor_run_index = run_indices[selected];
        current[selected] = next_resolved(
            cursor_run_index,
            &mut cursors[selected],
            &runs[cursor_run_index],
            &views[cursor_run_index],
        )
        .await?;
        let Some((winner_index, winner)) =
            latest_path_change(runs, views, &candidate.document.path).await?
        else {
            continue;
        };
        if winner_index != candidate.run_index
            || winner.state != DocumentState::Live
            || winner.document.version != candidate.document.version
        {
            continue;
        }
        let output = path_change_in_tree(sink, &output_paths.root, &candidate.document.path)
            .await?
            .ok_or(IndexError::InvalidFormat("missing compacted path"))?;
        let mut row = candidate.row;
        row.ordinal = output.document_ordinal.ok_or(IndexError::InvalidFormat(
            "missing compacted document ordinal",
        ))?;
        writer.push_row(row, sink).await?;
        wrote = true;
    }
    if wrote {
        Ok(Some(writer.finish(sink).await?))
    } else {
        Ok(None)
    }
}

pub(crate) fn tokenize(text: &str) -> Vec<(String, u32)> {
    let mut output = Vec::new();
    let mut token = String::new();
    let mut position = 0u32;
    for character in text.chars().chain(std::iter::once(' ')) {
        if character.is_alphanumeric() && token.chars().count() < MAX_TOKEN_CHARS {
            for lower in character.to_lowercase() {
                token.push(lower);
            }
        } else if !token.is_empty() {
            output.push((std::mem::take(&mut token), position));
            position = position.saturating_add(1);
        }
    }
    output
}

/// Tokenizes query text while bounding both phrase state and the number of
/// independently decoded posting cursors. Non-phrase repetition does not
/// create another cursor; phrase repetition does retain sequence position.
pub(crate) fn query_terms(
    text: &str,
    phrase: bool,
) -> Result<(Vec<String>, Vec<String>), IndexError> {
    let mut phrase_terms = Vec::new();
    let mut unique_terms = BTreeSet::new();
    let mut token = String::new();
    for character in text.chars().chain(std::iter::once(' ')) {
        if character.is_alphanumeric() && token.chars().count() < MAX_TOKEN_CHARS {
            token.extend(character.to_lowercase());
        } else if !token.is_empty() {
            if phrase && phrase_terms.len() == MAX_QUERY_TERM_CURSORS {
                return Err(query_term_limit(MAX_QUERY_TERM_CURSORS + 1));
            }
            if phrase {
                phrase_terms.push(token.clone());
            }
            unique_terms.insert(std::mem::take(&mut token));
            if unique_terms.len() > MAX_QUERY_TERM_CURSORS {
                return Err(query_term_limit(unique_terms.len()));
            }
        }
    }
    let unique_terms = unique_terms.into_iter().collect::<Vec<_>>();
    if !phrase {
        phrase_terms.clone_from(&unique_terms);
    }
    Ok((phrase_terms, unique_terms))
}

fn query_term_limit(terms: usize) -> IndexError {
    IndexError::ResourceLimit {
        needed: terms.saturating_mul(crate::MAX_INDEX_DECODED_BLOCK_BYTES),
        limit: MAX_QUERY_TERM_CURSORS.saturating_mul(crate::MAX_INDEX_DECODED_BLOCK_BYTES),
    }
}

pub(crate) fn estimate_text_fields(fields: &BTreeMap<String, String>) -> usize {
    fields.iter().fold(0usize, |bytes, (field, text)| {
        let (token_count, token_bytes) = estimate_tokens(text);
        bytes
            .saturating_add(field.len())
            .saturating_add(text.len())
            .saturating_add(token_bytes)
            .saturating_add(
                token_count.saturating_mul(field.len().saturating_add(POSTING_CHARGE_BYTES)),
            )
    })
}

/// Mirrors `tokenize` without retaining token strings or a corpus-sized token
/// vector. The byte count includes Unicode lowercase expansion.
fn estimate_tokens(text: &str) -> (usize, usize) {
    let mut count = 0usize;
    let mut token_chars = 0usize;
    let mut token_bytes = 0usize;
    let mut total_token_bytes = 0usize;
    for character in text.chars().chain(std::iter::once(' ')) {
        if character.is_alphanumeric() && token_chars < MAX_TOKEN_CHARS {
            for lower in character.to_lowercase() {
                token_chars = token_chars.saturating_add(1);
                token_bytes = token_bytes.saturating_add(lower.len_utf8());
            }
        } else if token_chars != 0 {
            count = count.saturating_add(1);
            total_token_bytes = total_token_bytes.saturating_add(token_bytes);
            token_chars = 0;
            token_bytes = 0;
        }
    }
    (count, total_token_bytes)
}

pub(crate) fn validate_fields(fields: &BTreeMap<String, String>) -> Result<(), IndexError> {
    for field in fields.keys() {
        validate_field(field)?;
    }
    Ok(())
}

fn validate_field(field: &str) -> Result<(), IndexError> {
    if field.is_empty() || field.contains('\0') || field.len() > MAX_FIELD_BYTES {
        return Err(IndexError::InvalidDefinition(
            "full-text field names must be 1..=256 bytes and contain no NUL".into(),
        ));
    }
    Ok(())
}

fn validate_term(term: &str) -> Result<(), IndexError> {
    if term.is_empty() || term.contains('\0') || term.chars().count() > MAX_TOKEN_CHARS {
        return Err(IndexError::InvalidDefinition(
            "full-text term is outside canonical bounds".into(),
        ));
    }
    Ok(())
}

fn encode_varint(mut value: u64, output: &mut Encoder) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.u8(byte);
        if value == 0 {
            return;
        }
    }
}

fn decode_varint(decoder: &mut Decoder<'_>) -> Result<u64, IndexError> {
    let mut value = 0u64;
    for shift in (0..=63).step_by(7) {
        let byte = decoder.u8()?;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(IndexError::InvalidFormat("unterminated posting varint"))
}

fn insert_bounded(hits: &mut Vec<FullTextHit>, hit: FullTextHit, limit: usize) {
    if hits
        .iter()
        .any(|existing| existing.document == hit.document)
    {
        return;
    }
    hits.push(hit);
    sort_hits(hits);
    hits.truncate(limit);
}

fn compare_score_cursor(hit: &FullTextHit, cursor: &FullTextQueryCursor) -> Ordering {
    cursor
        .score
        .partial_cmp(&hit.score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| hit.document.cmp(&cursor.document))
}

fn validate_score_cursor(cursor: Option<(f32, &DocumentRef)>) -> Result<(), IndexError> {
    if cursor.is_some_and(|(score, document)| !score.is_finite() || document.validate().is_err()) {
        return Err(IndexError::InvalidQuery(
            "invalid full-text query continuation".into(),
        ));
    }
    Ok(())
}

fn sort_hits(hits: &mut [FullTextHit]) {
    hits.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.document.cmp(&right.document))
    });
}

#[cfg(test)]
mod tests {
    use crate::io::tests::{MemoryBlockSink, MemoryDirectory};

    use super::*;

    fn upsert(path: &str, version: u64, text: &str) -> IndexMutation<FullTextDocument> {
        IndexMutation::Upsert(FullTextDocument {
            document: DocumentRef {
                path: path.into(),
                version,
            },
            fields: BTreeMap::from([("body".into(), text.into())]),
        })
    }

    async fn build(
        mutations: impl IntoIterator<Item = IndexMutation<FullTextDocument>>,
        level: u8,
        target: usize,
    ) -> (MemoryBlockSink, SealedRun) {
        let mut builder =
            FullTextSegmentBuilder::new(SegmentBuildOptions::for_level(256 * 1024, level).unwrap())
                .unwrap();
        for mutation in mutations {
            assert!(matches!(
                builder.try_push(mutation).unwrap(),
                SegmentPush::Accepted
            ));
        }
        let mut sink = MemoryBlockSink::default();
        let run = builder
            .seal_with_target(&mut sink, target)
            .await
            .unwrap()
            .unwrap();
        (sink, run)
    }

    fn directory(sink: &MemoryBlockSink, run: SealedRun) -> MemoryDirectory {
        sink.directory_with_root(run.into_root())
    }

    #[tokio::test]
    async fn phrase_update_delete_and_streaming_compaction_are_equivalent() {
        let (old_sink, old) = build(
            [
                upsert("/a", 1, "rust storage engine"),
                upsert("/b", 1, "rust storage engine"),
                upsert("/c", 1, "other text"),
            ],
            0,
            256,
        )
        .await;
        let old = directory(&old_sink, old);
        let (new_sink, new) = build(
            [
                upsert("/a", 2, "fast rust storage"),
                IndexMutation::Remove(DocumentRef {
                    path: "/b".into(),
                    version: 2,
                }),
            ],
            0,
            256,
        )
        .await;
        let new = directory(&new_sink, new);
        let runs = [new, old];
        let fields = Vec::new();
        let query = FullTextQuery {
            text: "rust storage",
            fields: &fields,
            phrase: true,
            match_all_terms: true,
            limit: 10,
        };
        let expected = FullTextEngine::query(&runs, query.clone()).await.unwrap();
        assert_eq!(expected.len(), 1);
        assert_eq!(expected[0].document.path, "/a");
        assert_eq!(expected[0].document.version, 2);

        let mut merged_sink = MemoryBlockSink::default();
        let merged = FullTextEngine::merge_with_target(&runs, 1, 256, &mut merged_sink)
            .await
            .unwrap();
        let merged = [directory(&merged_sink, merged)];
        assert_eq!(
            FullTextEngine::query(&merged, query).await.unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn output_is_deterministic_and_uses_multiple_blocks() {
        let mutations = (0..120)
            .map(|index| {
                upsert(
                    &format!("/docs/{index:04}"),
                    1,
                    &format!("common term value{index:04}"),
                )
            })
            .collect::<Vec<_>>();
        let (first_sink, first) = build(mutations.clone(), 1, 256).await;
        let (second_sink, second) = build(mutations, 1, 256).await;
        assert_eq!(first.descriptor().hash, second.descriptor().hash);
        assert_eq!(first_sink.len(), second_sink.len());
        assert!(first_sink.len() > 8);
        let directory = directory(&first_sink, first);
        let fields = Vec::new();
        let query = FullTextQuery {
            text: "common",
            fields: &fields,
            phrase: false,
            match_all_terms: false,
            limit: 3,
        };
        let first_page = FullTextEngine::query(&[directory.clone()], query.clone())
            .await
            .unwrap();
        let cursor = FullTextQueryCursor::from_hit(&first_page[0]);
        let second_page = FullTextEngine::query_after(&[directory], query, Some(&cursor))
            .await
            .unwrap();
        assert_eq!(second_page[0].document.path, first_page[1].document.path);
    }

    #[tokio::test]
    async fn hot_term_is_split_without_crossing_the_builder_cap() {
        let text = "hot ".repeat(8_000);
        let mutation = upsert("/hot", 1, &text);
        let estimated = FullTextSegmentBuilder::estimate_mutation(&mutation);
        let options = SegmentBuildOptions::new(estimated + 256).unwrap();
        let mut builder = FullTextSegmentBuilder::new(options).unwrap();
        assert!(matches!(
            builder.try_push(mutation).unwrap(),
            SegmentPush::Accepted
        ));
        assert!(builder.resident_bytes() <= options.max_resident_bytes);
        let mut sink = MemoryBlockSink::default();
        let run = builder
            .seal_with_target(&mut sink, 1024)
            .await
            .unwrap()
            .unwrap();
        assert!(sink.len() > 8);
        let directory = directory(&sink, run);
        let fields = Vec::new();
        let hits = FullTextEngine::query(
            &[directory],
            FullTextQuery {
                text: "hot",
                fields: &fields,
                phrase: false,
                match_all_terms: false,
                limit: 1,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn admission_accounts_for_derived_postings_and_returns_full_mutation() {
        let options = SegmentBuildOptions::new(1_000).unwrap();
        let mut builder = FullTextSegmentBuilder::new(options).unwrap();
        assert!(matches!(
            builder.try_push(upsert("/a", 1, "one two")).unwrap(),
            SegmentPush::Accepted
        ));
        assert!(matches!(
            builder.try_push(upsert("/b", 1, "three four")).unwrap(),
            SegmentPush::Full(_)
        ));
        assert!(builder.resident_bytes() <= options.max_resident_bytes);
    }

    #[test]
    fn non_phrase_matches_count_positions_without_retaining_them() {
        let mut fields = BTreeMap::new();
        let mut position_bytes = 0;
        append_field_match(
            &mut fields,
            TextPostingRow {
                term: "hot".into(),
                ordinal: 0,
                field: "body".into(),
                field_length: 3,
                part: 0,
                positions: vec![0, 1, 2],
            },
            false,
            &mut position_bytes,
        )
        .unwrap();
        assert_eq!(fields["body"].frequency, 3);
        assert!(fields["body"].positions.is_empty());
        assert_eq!(position_bytes, 0);
    }

    #[test]
    fn phrase_position_accumulation_is_hard_bounded() {
        let mut fields = BTreeMap::new();
        let mut position_bytes = 0;
        let count = MAX_PHRASE_POSITION_BYTES / std::mem::size_of::<u32>() + 1;
        let error = append_field_match(
            &mut fields,
            TextPostingRow {
                term: "hot".into(),
                ordinal: 0,
                field: "body".into(),
                field_length: count as u32,
                part: 0,
                positions: (0..count as u32).collect(),
            },
            true,
            &mut position_bytes,
        )
        .unwrap_err();
        assert!(matches!(error, IndexError::ResourceLimit { .. }));
    }

    #[test]
    fn bounded_results_keep_one_copy_of_a_document() {
        let document = DocumentRef {
            path: "/same".into(),
            version: 1,
        };
        let mut hits = Vec::new();
        insert_bounded(
            &mut hits,
            FullTextHit {
                document: document.clone(),
                score: 1.0,
                matched_terms: 1,
            },
            10,
        );
        insert_bounded(
            &mut hits,
            FullTextHit {
                document,
                score: 0.5,
                matched_terms: 1,
            },
            10,
        );
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].score, 1.0);
    }

    include!("full_text/query_bounds_tests.rs");
}
