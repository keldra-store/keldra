//! Hybrid full-text and exact-vector runs with one shared document table.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::full_text::{
    Candidate, RunCandidateCursor, TermCursor, TextComponentWriter, TextPosting,
    estimate_text_fields, merge_text_component, query_terms, tokenize, validate_fields,
};
use crate::run::{ComponentTree, LeafCursor, RunStatistics, RunView, open_views, seal_run_root};
use crate::segment::{
    DEFAULT_COMPONENT_BLOCK_BYTES, DocumentComponentWriter, DocumentRecord, DocumentState,
    MutationBuffer, PATH_CHANGES_TAG, PathChange, PathComponentWriter, PathRunCursor,
    document_by_ordinal, is_latest_live,
};
use crate::vector::{
    VectorComponentWriter, VectorDefinition, VectorRow, read_vector_block, similarity,
    validate_vector, vector_by_ordinal,
};
use crate::{
    BlockDescriptor, DocumentRef, IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind,
    IndexMutation, SealedRun, SegmentBuildOptions, SegmentPush,
};

const HYBRID_TEXT_TAG: u8 = 50;
pub(crate) const HYBRID_VECTOR_TAG: u8 = 51;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HybridDocument {
    pub document: DocumentRef,
    pub text_fields: BTreeMap<String, String>,
    pub vector: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HybridDefinition {
    pub vector: VectorDefinition,
    pub text_weight: f32,
    pub vector_weight: f32,
}

impl HybridDefinition {
    pub fn validate(&self) -> Result<(), IndexError> {
        self.vector.validate()?;
        if !self.text_weight.is_finite()
            || !self.vector_weight.is_finite()
            || self.text_weight < 0.0
            || self.vector_weight < 0.0
            || self.text_weight + self.vector_weight <= 0.0
        {
            return Err(IndexError::InvalidDefinition(
                "hybrid weights must be finite, non-negative and not both zero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct HybridQuery<'a> {
    pub text: &'a str,
    pub vector: &'a [f32],
    pub fields: &'a [String],
    pub phrase: bool,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HybridHit {
    pub document: DocumentRef,
    pub score: f32,
    pub text_score: Option<f32>,
    pub vector_score: Option<f32>,
}

/// Exclusive continuation key in hybrid result order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HybridQueryCursor {
    pub score: f32,
    pub document: DocumentRef,
}

impl HybridQueryCursor {
    pub fn from_hit(hit: &HybridHit) -> Self {
        Self {
            score: hit.score,
            document: hit.document.clone(),
        }
    }
}

pub struct HybridSegmentBuilder {
    definition: HybridDefinition,
    buffer: MutationBuffer<HybridDocument>,
}

impl HybridSegmentBuilder {
    pub fn new(
        definition: HybridDefinition,
        options: SegmentBuildOptions,
    ) -> Result<Self, IndexError> {
        definition.validate()?;
        Ok(Self {
            definition,
            buffer: MutationBuffer::new(options)?,
        })
    }

    pub fn estimate_mutation(mutation: &IndexMutation<HybridDocument>) -> usize {
        match mutation {
            IndexMutation::Remove(document) => document.path.len(),
            IndexMutation::Upsert(document) => document
                .document
                .path
                .len()
                .saturating_add(estimate_text_fields(&document.text_fields))
                .saturating_add(document.vector.len().saturating_mul(4)),
        }
    }

    pub fn try_push(
        &mut self,
        mutation: IndexMutation<HybridDocument>,
    ) -> Result<SegmentPush<HybridDocument>, IndexError> {
        if let IndexMutation::Upsert(document) = &mutation {
            validate_fields(&document.text_fields)?;
            validate_vector(&document.vector, self.definition.vector.dimension)?;
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
        let mutation_count = entries.len() as u64;
        let mut paths = PathComponentWriter::new(IndexKind::Hybrid, level, target_bytes);
        let mut documents = DocumentComponentWriter::new(IndexKind::Hybrid, level, target_bytes);
        let mut vectors = VectorComponentWriter::new(
            IndexKind::Hybrid,
            HYBRID_VECTOR_TAG,
            level,
            self.definition.vector.dimension,
            target_bytes,
        );
        let mut live = 0u64;
        let mut minimum_version = u64::MAX;
        let mut maximum_version = 0u64;
        for entry in entries.into_values() {
            match entry.mutation {
                IndexMutation::Upsert(value) => {
                    let ordinal = live;
                    live += 1;
                    minimum_version = minimum_version.min(value.document.version);
                    maximum_version = maximum_version.max(value.document.version);
                    paths
                        .push(
                            PathChange {
                                document: value.document.clone(),
                                state: DocumentState::Live,
                                document_ordinal: Some(ordinal),
                            },
                            sink,
                        )
                        .await?;
                    documents
                        .push(
                            DocumentRecord {
                                ordinal,
                                document: value.document,
                            },
                            sink,
                        )
                        .await?;
                    vectors
                        .push(
                            VectorRow {
                                ordinal,
                                values: value.vector,
                            },
                            sink,
                        )
                        .await?;
                }
                IndexMutation::Remove(document) => {
                    minimum_version = minimum_version.min(document.version);
                    maximum_version = maximum_version.max(document.version);
                    paths
                        .push(
                            PathChange {
                                document,
                                state: DocumentState::Removed,
                                document_ordinal: None,
                            },
                            sink,
                        )
                        .await?;
                }
            }
        }
        let mut components = vec![paths.finish(sink).await?];
        if live > 0 {
            components.push(documents.finish(sink).await?);
            components.push(vectors.finish(sink).await?);
        }
        if !postings.is_empty() {
            let mut text =
                TextComponentWriter::new(IndexKind::Hybrid, HYBRID_TEXT_TAG, level, target_bytes);
            for (term, rows) in postings {
                for row in rows {
                    text.push(&term, row, sink).await?;
                }
            }
            components.push(text.finish(sink).await?);
        }
        Ok(Some(seal_run_root(
            IndexKind::Hybrid,
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

pub struct HybridEngine;

impl HybridEngine {
    pub fn builder(
        definition: HybridDefinition,
        options: SegmentBuildOptions,
    ) -> Result<HybridSegmentBuilder, IndexError> {
        HybridSegmentBuilder::new(definition, options)
    }

    pub async fn query<D: IndexDirectoryRead>(
        runs: &[D],
        definition: &HybridDefinition,
        query: HybridQuery<'_>,
    ) -> Result<Vec<HybridHit>, IndexError> {
        Self::query_after(runs, definition, query, None).await
    }

    pub async fn query_after<D: IndexDirectoryRead>(
        runs: &[D],
        definition: &HybridDefinition,
        query: HybridQuery<'_>,
        after: Option<&HybridQueryCursor>,
    ) -> Result<Vec<HybridHit>, IndexError> {
        definition.validate()?;
        validate_query_cursor(after)?;
        if query.limit == 0 || runs.is_empty() {
            return Ok(Vec::new());
        }
        if query.text.trim().is_empty() && query.vector.is_empty() {
            return Err(IndexError::InvalidQuery(
                "hybrid query needs text, a vector, or both".into(),
            ));
        }
        if !query.vector.is_empty() {
            validate_vector(query.vector, definition.vector.dimension).map_err(|_| {
                IndexError::InvalidQuery("hybrid query vector has the wrong dimension".into())
            })?;
        }
        let (phrase_terms, unique_terms) = query_terms(query.text, query.phrase)?;
        if !query.text.trim().is_empty() && phrase_terms.is_empty() && query.vector.is_empty() {
            return Err(IndexError::InvalidQuery(
                "hybrid text contains no indexable terms".into(),
            ));
        }
        let selected_fields = Arc::new(query.fields.iter().cloned().collect::<BTreeSet<_>>());
        let views = open_views(runs, IndexKind::Hybrid).await?;
        let mut maximum_text = 0.0f32;
        let mut vector_range = None::<(f32, f32)>;
        for (run, view) in runs.iter().zip(&views) {
            if !query.vector.is_empty() && view.component_optional(HYBRID_VECTOR_TAG).is_none() {
                continue;
            }
            let mut cursor = HybridScoreCursor::open(
                run,
                view,
                definition,
                &phrase_terms,
                &unique_terms,
                Arc::clone(&selected_fields),
                query.phrase,
                query.vector,
            )
            .await?;
            while let Some(raw) = cursor.next().await? {
                let document = document_by_ordinal(run, view, raw.ordinal).await?;
                if !is_latest_live(runs, &views, &document).await? {
                    continue;
                }
                if let Some(score) = raw.text {
                    maximum_text = maximum_text.max(score);
                }
                if let Some(score) = raw.vector {
                    vector_range = Some(match vector_range {
                        None => (score, score),
                        Some((minimum, maximum)) => (minimum.min(score), maximum.max(score)),
                    });
                }
            }
        }

        let text_requested = !unique_terms.is_empty();
        let vector_requested = !query.vector.is_empty();
        let active_weight = if text_requested {
            definition.text_weight
        } else {
            0.0
        } + if vector_requested {
            definition.vector_weight
        } else {
            0.0
        };
        if active_weight <= 0.0 {
            return Err(IndexError::InvalidQuery(
                "hybrid query selects only a zero-weight modality".into(),
            ));
        }
        let mut hits = Vec::with_capacity(query.limit.min(128));
        for (run, view) in runs.iter().zip(&views) {
            if !query.vector.is_empty() && view.component_optional(HYBRID_VECTOR_TAG).is_none() {
                continue;
            }
            let mut cursor = HybridScoreCursor::open(
                run,
                view,
                definition,
                &phrase_terms,
                &unique_terms,
                Arc::clone(&selected_fields),
                query.phrase,
                query.vector,
            )
            .await?;
            while let Some(raw) = cursor.next().await? {
                if (!vector_requested && raw.text.is_none())
                    || (!text_requested && raw.vector.is_none())
                {
                    continue;
                }
                let document = document_by_ordinal(run, view, raw.ordinal).await?;
                if !is_latest_live(runs, &views, &document).await? {
                    continue;
                }
                let text_score = raw.text.map(|score| {
                    if maximum_text > 0.0 {
                        score / maximum_text
                    } else {
                        0.0
                    }
                });
                let vector_score = raw
                    .vector
                    .map(|score| normalize_vector_score(score, vector_range));
                let score = (text_score.unwrap_or(0.0) * definition.text_weight
                    + vector_score.unwrap_or(0.0) * definition.vector_weight)
                    / active_weight;
                let hit = HybridHit {
                    document,
                    score,
                    text_score,
                    vector_score,
                };
                if after
                    .is_some_and(|cursor| compare_hit_to_cursor(&hit, cursor) != Ordering::Greater)
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
        definition: &HybridDefinition,
        output_level: u8,
        sink: &mut S,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead,
        S: IndexBlockSink + IndexDirectoryRead,
    {
        definition.validate()?;
        Self::merge_with_target(
            runs,
            definition,
            output_level,
            DEFAULT_COMPONENT_BLOCK_BYTES,
            sink,
        )
        .await
    }

    async fn merge_with_target<D, S>(
        runs: &[D],
        definition: &HybridDefinition,
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
                "hybrid compaction requires input runs and an L1+ output level".into(),
            ));
        }
        let views = open_views(runs, IndexKind::Hybrid).await?;
        let (path_tree, document_tree, vector_tree, statistics) =
            merge_common_vectors(runs, &views, definition, output_level, target_bytes, sink)
                .await?;
        let text_tree = merge_text_component(
            IndexKind::Hybrid,
            HYBRID_TEXT_TAG,
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
        if let Some(tree) = vector_tree {
            components.push(tree);
        }
        if let Some(tree) = text_tree {
            components.push(tree);
        }
        seal_run_root(IndexKind::Hybrid, output_level, statistics, components)
    }
}

fn collect_postings(
    entries: &BTreeMap<String, crate::segment::PendingMutation<HybridDocument>>,
) -> Result<BTreeMap<String, Vec<TextPosting>>, IndexError> {
    let mut terms = BTreeMap::<String, Vec<TextPosting>>::new();
    let mut ordinal = 0u64;
    for entry in entries.values() {
        let IndexMutation::Upsert(document) = &entry.mutation else {
            continue;
        };
        validate_fields(&document.text_fields)?;
        for (field, text) in &document.text_fields {
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

struct VectorRowCursor<'a, D> {
    directory: &'a D,
    definition: &'a VectorDefinition,
    leaves: LeafCursor<'a, D>,
    rows: Vec<VectorRow>,
    next_row: usize,
}

impl<'a, D: IndexDirectoryRead> VectorRowCursor<'a, D> {
    fn new(directory: &'a D, definition: &'a VectorDefinition, root: BlockDescriptor) -> Self {
        Self {
            directory,
            definition,
            leaves: LeafCursor::new(directory, root),
            rows: Vec::new(),
            next_row: 0,
        }
    }

    async fn next(&mut self) -> Result<Option<VectorRow>, IndexError> {
        loop {
            if let Some(row) = self.rows.get(self.next_row).cloned() {
                self.next_row += 1;
                return Ok(Some(row));
            }
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            self.rows = read_vector_block(self.directory, &descriptor, self.definition).await?;
            self.next_row = 0;
        }
    }
}

struct RawHybridScore {
    ordinal: u64,
    text: Option<f32>,
    vector: Option<f32>,
}

struct HybridScoreCursor<'a, D> {
    vectors: Option<VectorRowCursor<'a, D>>,
    text: Option<RunCandidateCursor<'a, D>>,
    current_text: Option<Candidate>,
    query_vector: &'a [f32],
    definition: &'a HybridDefinition,
}

impl<'a, D: IndexDirectoryRead> HybridScoreCursor<'a, D> {
    #[allow(clippy::too_many_arguments)]
    async fn open(
        run: &'a D,
        view: &RunView,
        definition: &'a HybridDefinition,
        phrase_terms: &[String],
        unique_terms: &[String],
        selected_fields: Arc<BTreeSet<String>>,
        phrase: bool,
        query_vector: &'a [f32],
    ) -> Result<Self, IndexError> {
        let vectors = if query_vector.is_empty() {
            None
        } else {
            Some(VectorRowCursor::new(
                run,
                &definition.vector,
                view.component(HYBRID_VECTOR_TAG)?.clone(),
            ))
        };
        let mut text = if unique_terms.is_empty() {
            None
        } else if let Some(root) = view.component_optional(HYBRID_TEXT_TAG) {
            let cursors = unique_terms
                .iter()
                .map(|term| {
                    TermCursor::new(
                        run,
                        root.clone(),
                        term.clone(),
                        Arc::clone(&selected_fields),
                        phrase,
                    )
                })
                .collect::<Vec<_>>();
            Some(
                RunCandidateCursor::new(
                    cursors,
                    phrase_terms.to_vec(),
                    unique_terms.to_vec(),
                    phrase,
                    false,
                )
                .await?,
            )
        } else {
            None
        };
        let current_text = match text.as_mut() {
            Some(cursor) => cursor.next().await?,
            None => None,
        };
        Ok(Self {
            vectors,
            text,
            current_text,
            query_vector,
            definition,
        })
    }

    async fn next(&mut self) -> Result<Option<RawHybridScore>, IndexError> {
        if self.vectors.is_none() {
            let candidate = match self.current_text.take() {
                Some(candidate) => candidate,
                None => return Ok(None),
            };
            self.current_text = match self.text.as_mut() {
                Some(cursor) => cursor.next().await?,
                None => None,
            };
            return Ok(Some(RawHybridScore {
                ordinal: candidate.ordinal,
                text: Some(candidate.score),
                vector: None,
            }));
        }
        let Some(vector) = self.vectors.as_mut().unwrap().next().await? else {
            return Ok(None);
        };
        while self
            .current_text
            .as_ref()
            .is_some_and(|text| text.ordinal < vector.ordinal)
        {
            self.current_text = match self.text.as_mut() {
                Some(cursor) => cursor.next().await?,
                None => None,
            };
        }
        let text = if self
            .current_text
            .as_ref()
            .is_some_and(|text| text.ordinal == vector.ordinal)
        {
            let score = self.current_text.take().unwrap().score;
            self.current_text = match self.text.as_mut() {
                Some(cursor) => cursor.next().await?,
                None => None,
            };
            Some(score)
        } else {
            None
        };
        let vector_score = (!self.query_vector.is_empty()).then(|| {
            similarity(
                self.query_vector,
                &vector.values,
                self.definition.vector.metric,
            )
        });
        Ok(Some(RawHybridScore {
            ordinal: vector.ordinal,
            text,
            vector: vector_score,
        }))
    }
}

async fn merge_common_vectors<D, S>(
    runs: &[D],
    views: &[RunView],
    definition: &HybridDefinition,
    output_level: u8,
    target_bytes: usize,
    sink: &mut S,
) -> Result<
    (
        ComponentTree,
        Option<ComponentTree>,
        Option<ComponentTree>,
        RunStatistics,
    ),
    IndexError,
>
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
    let mut paths = PathComponentWriter::new(IndexKind::Hybrid, output_level, target_bytes);
    let mut documents = DocumentComponentWriter::new(IndexKind::Hybrid, output_level, target_bytes);
    let mut vectors = VectorComponentWriter::new(
        IndexKind::Hybrid,
        HYBRID_VECTOR_TAG,
        output_level,
        definition.vector.dimension,
        target_bytes,
    );
    let mut mutation_count = 0u64;
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
        let (winner_run, mut change) = winner.unwrap();
        if change.state == DocumentState::Live {
            let input_ordinal = change.document_ordinal.ok_or(IndexError::InvalidFormat(
                "live hybrid document has no ordinal",
            ))?;
            let source_document =
                document_by_ordinal(&runs[winner_run], &views[winner_run], input_ordinal).await?;
            if source_document != change.document {
                return Err(IndexError::InvalidFormat("hybrid document mismatch"));
            }
            let values = vector_by_ordinal(
                &runs[winner_run],
                &views[winner_run],
                &definition.vector,
                HYBRID_VECTOR_TAG,
                input_ordinal,
            )
            .await?;
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
            vectors
                .push(
                    VectorRow {
                        ordinal: live,
                        values,
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
        mutation_count += 1;
        paths.push(change, sink).await?;
    }
    if mutation_count == 0 {
        return Err(IndexError::InvalidDefinition(
            "hybrid compaction produced no changes".into(),
        ));
    }
    let path_tree = paths.finish(sink).await?;
    let (document_tree, vector_tree) = if live == 0 {
        (None, None)
    } else {
        (
            Some(documents.finish(sink).await?),
            Some(vectors.finish(sink).await?),
        )
    };
    Ok((
        path_tree,
        document_tree,
        vector_tree,
        RunStatistics {
            mutation_count,
            live_document_count: live,
            minimum_version,
            maximum_version,
        },
    ))
}

fn normalize_vector_score(score: f32, range: Option<(f32, f32)>) -> f32 {
    match range {
        Some((minimum, maximum)) if maximum > minimum => (score - minimum) / (maximum - minimum),
        Some(_) => 1.0,
        None => 0.0,
    }
}

fn insert_bounded(hits: &mut Vec<HybridHit>, hit: HybridHit, limit: usize) {
    if hits.iter().any(|current| current.document == hit.document) {
        return;
    }
    hits.push(hit);
    sort_hits(hits);
    hits.truncate(limit);
}

fn compare_hit_to_cursor(hit: &HybridHit, cursor: &HybridQueryCursor) -> Ordering {
    cursor
        .score
        .partial_cmp(&hit.score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| hit.document.cmp(&cursor.document))
}

fn validate_query_cursor(cursor: Option<&HybridQueryCursor>) -> Result<(), IndexError> {
    if cursor.is_some_and(|cursor| !cursor.score.is_finite() || cursor.document.validate().is_err())
    {
        return Err(IndexError::InvalidQuery(
            "invalid hybrid query continuation".into(),
        ));
    }
    Ok(())
}

fn sort_hits(hits: &mut [HybridHit]) {
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
    use crate::vector::VectorMetric;

    use super::*;

    fn definition() -> HybridDefinition {
        HybridDefinition {
            vector: VectorDefinition {
                dimension: 2,
                metric: VectorMetric::Cosine,
            },
            text_weight: 0.6,
            vector_weight: 0.4,
        }
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
            HybridHit {
                document: document.clone(),
                score: 1.0,
                text_score: Some(1.0),
                vector_score: Some(1.0),
            },
            10,
        );
        insert_bounded(
            &mut hits,
            HybridHit {
                document,
                score: 0.5,
                text_score: Some(0.5),
                vector_score: Some(0.5),
            },
            10,
        );
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].score, 1.0);
    }

    fn upsert(
        path: &str,
        version: u64,
        text: &str,
        vector: &[f32],
    ) -> IndexMutation<HybridDocument> {
        IndexMutation::Upsert(HybridDocument {
            document: DocumentRef {
                path: path.into(),
                version,
            },
            text_fields: BTreeMap::from([("body".into(), text.into())]),
            vector: vector.to_vec(),
        })
    }

    async fn build(
        definition: &HybridDefinition,
        mutations: impl IntoIterator<Item = IndexMutation<HybridDocument>>,
        level: u8,
        target: usize,
        resident: usize,
    ) -> (MemoryBlockSink, SealedRun) {
        let mut builder = HybridSegmentBuilder::new(
            definition.clone(),
            SegmentBuildOptions::for_level(resident, level).unwrap(),
        )
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
    async fn fusion_observes_updates_deletes_and_streaming_compaction() {
        let definition = definition();
        let (old_sink, old) = build(
            &definition,
            [
                upsert("/a", 1, "rust storage", &[1.0, 0.0]),
                upsert("/b", 1, "rust storage", &[1.0, 0.0]),
                upsert("/c", 1, "other", &[0.0, 1.0]),
            ],
            0,
            256,
            64 * 1024,
        )
        .await;
        let old = directory(&old_sink, old);
        let (new_sink, new) = build(
            &definition,
            [
                upsert("/a", 2, "application", &[0.0, 1.0]),
                IndexMutation::Remove(DocumentRef {
                    path: "/b".into(),
                    version: 2,
                }),
            ],
            0,
            256,
            64 * 1024,
        )
        .await;
        let new = directory(&new_sink, new);
        let runs = [new, old];
        let fields = Vec::new();
        let query = HybridQuery {
            text: "application",
            vector: &[0.0, 1.0],
            fields: &fields,
            phrase: false,
            limit: 10,
        };
        let expected = HybridEngine::query(&runs, &definition, query.clone())
            .await
            .unwrap();
        assert!(expected.iter().all(|hit| hit.document.path != "/b"));
        assert_eq!(expected[0].document.path, "/a");
        assert_eq!(expected[0].document.version, 2);
        let cursor = HybridQueryCursor::from_hit(&expected[0]);
        assert_eq!(
            HybridEngine::query_after(&runs, &definition, query.clone(), Some(&cursor))
                .await
                .unwrap(),
            expected[1..]
        );

        let mut merged_sink = MemoryBlockSink::default();
        let merged = HybridEngine::merge_with_target(&runs, &definition, 1, 256, &mut merged_sink)
            .await
            .unwrap();
        let merged = [directory(&merged_sink, merged)];
        assert_eq!(
            HybridEngine::query(&merged, &definition, query)
                .await
                .unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn hot_term_is_split_without_crossing_the_builder_cap() {
        let definition = definition();
        let text = "hot ".repeat(8_000);
        let mutation = upsert("/hot", 1, &text, &[1.0, 0.0]);
        let estimated = HybridSegmentBuilder::estimate_mutation(&mutation);
        let (sink, run) = build(&definition, [mutation], 0, 1024, estimated + 256).await;
        assert!(sink.len() > 8);
        let directory = directory(&sink, run);
        let fields = Vec::new();
        let hits = HybridEngine::query(
            &[directory],
            &definition,
            HybridQuery {
                text: "hot",
                vector: &[],
                fields: &fields,
                phrase: false,
                limit: 1,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[tokio::test]
    async fn output_is_deterministic_and_multi_block() {
        let definition = definition();
        let mutations = (0..100)
            .map(|index| {
                upsert(
                    &format!("/hybrid/{index:04}"),
                    1,
                    &format!("shared term value{index:04}"),
                    &[index as f32, 1.0],
                )
            })
            .collect::<Vec<_>>();
        let (first_sink, first) = build(&definition, mutations.clone(), 1, 256, 512 * 1024).await;
        let (second_sink, second) = build(&definition, mutations, 1, 256, 512 * 1024).await;
        assert_eq!(first.descriptor().hash, second.descriptor().hash);
        assert_eq!(first_sink.len(), second_sink.len());
        assert!(first_sink.len() > 10);
    }

    include!("hybrid/query_bounds_tests.rs");
}
