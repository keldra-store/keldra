//! Exact vector search over bounded immutable runs.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::codec::{Decoder, Encoder, encode_component};
use crate::run::{
    ComponentTree, LeafCursor, RoutingTreeBuilder, RunStatistics, RunView, find_leaf, open_views,
    seal_run_root,
};
use crate::segment::{
    DEFAULT_COMPONENT_BLOCK_BYTES, DocumentComponentWriter, DocumentRecord, DocumentState,
    MutationBuffer, PATH_CHANGES_TAG, PathChange, PathComponentWriter, document_by_ordinal,
    is_latest_live, read_path_block,
};
use crate::succinct::{decode_elias_fano_with_budget, encode_elias_fano};
use crate::{
    ComponentCodec, DocumentRef, GeneratedBlock, IndexBlockSink, IndexDirectoryRead, IndexError,
    IndexKind, IndexMutation, SealedRun, SegmentBuildOptions, SegmentPush,
};

pub(crate) const VECTORS_TAG: u8 = 40;
const VECTOR_ROW_OVERHEAD: usize = 24;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorMetric {
    Cosine,
    DotProduct,
    Euclidean,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorDefinition {
    pub dimension: usize,
    pub metric: VectorMetric,
}

impl VectorDefinition {
    pub fn validate(&self) -> Result<(), IndexError> {
        let maximum = DEFAULT_COMPONENT_BLOCK_BYTES
            .saturating_sub(VECTOR_ROW_OVERHEAD)
            .saturating_div(4);
        if self.dimension == 0 || self.dimension > maximum {
            return Err(IndexError::InvalidDefinition(format!(
                "vector dimension must be between 1 and {maximum}"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorDocument {
    pub document: DocumentRef,
    pub values: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorHit {
    pub document: DocumentRef,
    pub score: f32,
}

/// Exclusive continuation key in vector result order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorQueryCursor {
    pub score: f32,
    pub document: DocumentRef,
}

impl VectorQueryCursor {
    pub fn from_hit(hit: &VectorHit) -> Self {
        Self {
            score: hit.score,
            document: hit.document.clone(),
        }
    }
}

pub struct VectorSegmentBuilder {
    definition: VectorDefinition,
    buffer: MutationBuffer<VectorDocument>,
}

impl VectorSegmentBuilder {
    pub fn new(
        definition: VectorDefinition,
        options: SegmentBuildOptions,
    ) -> Result<Self, IndexError> {
        definition.validate()?;
        Ok(Self {
            definition,
            buffer: MutationBuffer::new(options)?,
        })
    }

    pub fn estimate_mutation(mutation: &IndexMutation<VectorDocument>) -> usize {
        match mutation {
            IndexMutation::Upsert(document) => document.values.len().saturating_mul(4),
            IndexMutation::Remove(document) => document.path.len(),
        }
    }

    pub fn try_push(
        &mut self,
        mutation: IndexMutation<VectorDocument>,
    ) -> Result<SegmentPush<VectorDocument>, IndexError> {
        if let IndexMutation::Upsert(document) = &mutation {
            validate_vector(&document.values, self.definition.dimension)?;
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
        target_block_bytes: usize,
    ) -> Result<Option<SealedRun>, IndexError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        let level = self.buffer.level();
        let entries = self.buffer.into_entries();
        let mutation_count = entries.len() as u64;
        let mut paths = PathComponentWriter::new(IndexKind::Vector, level, target_block_bytes);
        let mut documents =
            DocumentComponentWriter::new(IndexKind::Vector, level, target_block_bytes);
        let mut vectors = VectorComponentWriter::new(
            IndexKind::Vector,
            VECTORS_TAG,
            level,
            self.definition.dimension,
            target_block_bytes,
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
                                values: value.values,
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
        Ok(Some(seal_run_root(
            IndexKind::Vector,
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

pub struct VectorEngine;

impl VectorEngine {
    pub fn builder(
        definition: VectorDefinition,
        options: SegmentBuildOptions,
    ) -> Result<VectorSegmentBuilder, IndexError> {
        VectorSegmentBuilder::new(definition, options)
    }

    pub async fn query<D: IndexDirectoryRead>(
        runs: &[D],
        definition: &VectorDefinition,
        query: &[f32],
        limit: usize,
    ) -> Result<Vec<VectorHit>, IndexError> {
        Self::query_after(runs, definition, query, limit, None).await
    }

    pub async fn query_after<D: IndexDirectoryRead>(
        runs: &[D],
        definition: &VectorDefinition,
        query: &[f32],
        limit: usize,
        after: Option<&VectorQueryCursor>,
    ) -> Result<Vec<VectorHit>, IndexError> {
        definition.validate()?;
        validate_vector(query, definition.dimension)
            .map_err(|_| IndexError::InvalidQuery("query vector has the wrong dimension".into()))?;
        validate_query_cursor(after)?;
        if limit == 0 || runs.is_empty() {
            return Ok(Vec::new());
        }
        let views = open_views(runs, IndexKind::Vector).await?;
        let mut hits = Vec::with_capacity(limit.min(128));
        for (run, view) in runs.iter().zip(&views) {
            let Some(root) = view.component_optional(VECTORS_TAG) else {
                continue;
            };
            let mut leaves = LeafCursor::new(run, root.clone());
            while let Some(descriptor) = leaves.next().await? {
                for row in read_vector_block(run, &descriptor, definition).await? {
                    let document = document_by_ordinal(run, view, row.ordinal).await?;
                    if !is_latest_live(runs, &views, &document).await? {
                        continue;
                    }
                    let hit = VectorHit {
                        document,
                        score: similarity(query, &row.values, definition.metric),
                    };
                    if after.is_some_and(|cursor| {
                        compare_hit_to_cursor(&hit, cursor) != Ordering::Greater
                    }) {
                        continue;
                    }
                    insert_bounded(&mut hits, hit, limit);
                }
            }
        }
        sort_hits(&mut hits);
        Ok(hits)
    }

    pub async fn merge_runs<D, S>(
        runs: &[D],
        definition: &VectorDefinition,
        output_level: u8,
        sink: &mut S,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead,
        S: IndexBlockSink,
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
        definition: &VectorDefinition,
        output_level: u8,
        target_block_bytes: usize,
        sink: &mut S,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead,
        S: IndexBlockSink,
    {
        if runs.is_empty() || output_level == 0 {
            return Err(IndexError::InvalidDefinition(
                "vector compaction requires input runs and an L1+ output level".into(),
            ));
        }
        let views = open_views(runs, IndexKind::Vector).await?;
        let mut cursors = Vec::with_capacity(runs.len());
        for (run, view) in runs.iter().zip(&views) {
            cursors.push(PathRunCursor::new(
                run,
                view.component(PATH_CHANGES_TAG)?.clone(),
            ));
        }
        let mut current = Vec::with_capacity(cursors.len());
        for cursor in &mut cursors {
            current.push(cursor.next().await?);
        }
        let mut paths =
            PathComponentWriter::new(IndexKind::Vector, output_level, target_block_bytes);
        let mut documents =
            DocumentComponentWriter::new(IndexKind::Vector, output_level, target_block_bytes);
        let mut vectors = VectorComponentWriter::new(
            IndexKind::Vector,
            VECTORS_TAG,
            output_level,
            definition.dimension,
            target_block_bytes,
        );
        let mut mutation_count = 0u64;
        let mut live = 0u64;
        let mut minimum_version = u64::MAX;
        let mut maximum_version = 0u64;
        loop {
            let Some(path) = current
                .iter()
                .flatten()
                .map(|row| row.document.path.as_str())
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
            let (winner_run, mut winner) = winner.unwrap();
            if winner.state == DocumentState::Live {
                let input_ordinal = winner
                    .document_ordinal
                    .ok_or(IndexError::InvalidFormat("live vector has no ordinal"))?;
                let source_document =
                    document_by_ordinal(&runs[winner_run], &views[winner_run], input_ordinal)
                        .await?;
                if source_document != winner.document {
                    return Err(IndexError::InvalidFormat("vector document mismatch"));
                }
                let values = vector_by_ordinal(
                    &runs[winner_run],
                    &views[winner_run],
                    definition,
                    VECTORS_TAG,
                    input_ordinal,
                )
                .await?;
                let ordinal = live;
                live += 1;
                winner.document_ordinal = Some(ordinal);
                documents
                    .push(
                        DocumentRecord {
                            ordinal,
                            document: winner.document.clone(),
                        },
                        sink,
                    )
                    .await?;
                vectors.push(VectorRow { ordinal, values }, sink).await?;
            } else {
                winner.document_ordinal = None;
            }
            minimum_version = minimum_version.min(winner.document.version);
            maximum_version = maximum_version.max(winner.document.version);
            mutation_count += 1;
            paths.push(winner, sink).await?;
        }
        if mutation_count == 0 {
            return Err(IndexError::InvalidDefinition(
                "vector compaction produced no changes".into(),
            ));
        }
        let mut components = vec![paths.finish(sink).await?];
        if live > 0 {
            components.push(documents.finish(sink).await?);
            components.push(vectors.finish(sink).await?);
        }
        seal_run_root(
            IndexKind::Vector,
            output_level,
            RunStatistics {
                mutation_count,
                live_document_count: live,
                minimum_version,
                maximum_version,
            },
            components,
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct VectorRow {
    pub(crate) ordinal: u64,
    pub(crate) values: Vec<f32>,
}

pub(crate) struct VectorComponentWriter {
    kind: IndexKind,
    component_tag: u8,
    level: u8,
    dimension: usize,
    target_bytes: usize,
    estimated_bytes: usize,
    rows: Vec<VectorRow>,
    tree: RoutingTreeBuilder,
}

impl VectorComponentWriter {
    pub(crate) fn new(
        kind: IndexKind,
        component_tag: u8,
        level: u8,
        dimension: usize,
        target_bytes: usize,
    ) -> Self {
        Self {
            kind,
            component_tag,
            level,
            dimension,
            target_bytes: target_bytes.max(256),
            estimated_bytes: 0,
            rows: Vec::new(),
            tree: RoutingTreeBuilder::new(kind, component_tag),
        }
    }

    pub(crate) async fn push<S: IndexBlockSink>(
        &mut self,
        row: VectorRow,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        validate_vector(&row.values, self.dimension)?;
        if self
            .rows
            .last()
            .is_some_and(|previous| previous.ordinal >= row.ordinal)
        {
            return Err(IndexError::UnsortedRecords);
        }
        let row_bytes = row.values.len().saturating_mul(4).saturating_add(8);
        if !self.rows.is_empty()
            && self.estimated_bytes.saturating_add(row_bytes) > self.target_bytes
        {
            self.flush(sink).await?;
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
        let first = ordinal_key(rows.first().unwrap().ordinal);
        let last = ordinal_key(rows.last().unwrap().ordinal);
        let body = encode_vector_rows(&rows, self.dimension, self.level > 0)?;
        let bytes = encode_component(
            self.kind,
            self.component_tag,
            ComponentCodec::FixedVectors,
            body,
        )?;
        self.tree
            .emit_leaf(
                GeneratedBlock::new(
                    self.kind,
                    self.component_tag,
                    ComponentCodec::FixedVectors,
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

fn encode_vector_rows(
    rows: &[VectorRow],
    dimension: usize,
    succinct: bool,
) -> Result<Vec<u8>, IndexError> {
    let mut output = Encoder::default();
    output.u32(dimension)?;
    output.u32(rows.len())?;
    output.bool(succinct);
    if succinct {
        output.bytes(&encode_elias_fano(
            &rows.iter().map(|row| row.ordinal).collect::<Vec<_>>(),
        )?)?;
    }
    for row in rows {
        if !succinct {
            output.u64(row.ordinal);
        }
        for value in &row.values {
            output.f32(*value);
        }
    }
    Ok(output.finish())
}

pub(crate) async fn read_vector_block<D: IndexDirectoryRead>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
    definition: &VectorDefinition,
) -> Result<Vec<VectorRow>, IndexError> {
    if descriptor.codec != ComponentCodec::FixedVectors {
        return Err(IndexError::InvalidFormat("vector block codec"));
    }
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let rows = decode_vector_rows(block.body(), definition)?;
    if rows.first().map(|row| ordinal_key(row.ordinal)) != Some(descriptor.minimum_key.clone())
        || rows.last().map(|row| ordinal_key(row.ordinal)) != Some(descriptor.maximum_key.clone())
        || rows.len() as u64 != descriptor.element_count
    {
        return Err(IndexError::InvalidFormat("vector block descriptor"));
    }
    Ok(rows)
}

fn decode_vector_rows(
    bytes: &[u8],
    definition: &VectorDefinition,
) -> Result<Vec<VectorRow>, IndexError> {
    let mut decoder = Decoder::new(bytes);
    if decoder.u32()? as usize != definition.dimension {
        return Err(IndexError::InvalidFormat("vector run dimension"));
    }
    let count = decoder.u32()? as usize;
    let succinct = decoder.bool()?;
    let ordinals = if succinct {
        let budget = decoder.budget();
        let values = decode_elias_fano_with_budget(decoder.bytes()?, budget)?;
        if values.len() != count {
            return Err(IndexError::InvalidFormat("vector ordinal count"));
        }
        Some(values)
    } else {
        None
    };
    let scalar_count = count
        .checked_mul(definition.dimension)
        .ok_or(IndexError::OffsetOverflow)?;
    decoder.guard_count::<f32>(scalar_count, 4)?;
    decoder.guard_count::<VectorRow>(count, 0)?;
    let mut rows = Vec::with_capacity(count);
    for index in 0..count {
        let ordinal = match &ordinals {
            Some(values) => values.get(index)?,
            None => decoder.u64()?,
        };
        let mut values = Vec::with_capacity(definition.dimension);
        for _ in 0..definition.dimension {
            values.push(decoder.f32()?);
        }
        if values.iter().any(|value| !value.is_finite()) {
            return Err(IndexError::InvalidFormat("non-finite vector value"));
        }
        rows.push(VectorRow { ordinal, values });
    }
    decoder.finish()?;
    if rows.is_empty()
        || rows
            .windows(2)
            .any(|pair| pair[0].ordinal >= pair[1].ordinal)
    {
        return Err(IndexError::InvalidFormat("vector ordinal order"));
    }
    Ok(rows)
}

pub(crate) async fn vector_by_ordinal<D: IndexDirectoryRead>(
    run: &D,
    view: &RunView,
    definition: &VectorDefinition,
    component_tag: u8,
    ordinal: u64,
) -> Result<Vec<f32>, IndexError> {
    let root = view.component(component_tag)?;
    let descriptor = find_leaf(run, root, &ordinal_key(ordinal))
        .await?
        .ok_or(IndexError::InvalidFormat("missing vector ordinal"))?;
    let rows = read_vector_block(run, &descriptor, definition).await?;
    let index = rows
        .binary_search_by_key(&ordinal, |row| row.ordinal)
        .map_err(|_| IndexError::InvalidFormat("missing vector ordinal"))?;
    Ok(rows.into_iter().nth(index).unwrap().values)
}

struct PathRunCursor<'a, D> {
    directory: &'a D,
    leaves: LeafCursor<'a, D>,
    rows: Vec<PathChange>,
    next: usize,
}

impl<'a, D: IndexDirectoryRead> PathRunCursor<'a, D> {
    fn new(directory: &'a D, root: crate::BlockDescriptor) -> Self {
        Self {
            directory,
            leaves: LeafCursor::new(directory, root),
            rows: Vec::new(),
            next: 0,
        }
    }

    async fn next(&mut self) -> Result<Option<PathChange>, IndexError> {
        loop {
            if let Some(row) = self.rows.get(self.next).cloned() {
                self.next += 1;
                return Ok(Some(row));
            }
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            self.rows = read_path_block(self.directory, &descriptor).await?;
            self.next = 0;
        }
    }
}

pub(crate) fn validate_vector(values: &[f32], dimension: usize) -> Result<(), IndexError> {
    if values.len() != dimension || values.iter().any(|value| !value.is_finite()) {
        return Err(IndexError::InvalidDefinition(
            "vector values must be finite and match the configured dimension".into(),
        ));
    }
    Ok(())
}

pub(crate) fn similarity(left: &[f32], right: &[f32], metric: VectorMetric) -> f32 {
    match metric {
        VectorMetric::DotProduct => finite_score(
            left.iter()
                .zip(right)
                .map(|(left, right)| f64::from(*left) * f64::from(*right))
                .sum(),
        ),
        VectorMetric::Euclidean => finite_score(
            -left
                .iter()
                .zip(right)
                .map(|(left, right)| {
                    let difference = f64::from(*left) - f64::from(*right);
                    difference * difference
                })
                .sum::<f64>()
                .sqrt(),
        ),
        VectorMetric::Cosine => {
            let dot = left
                .iter()
                .zip(right)
                .map(|(left, right)| f64::from(*left) * f64::from(*right))
                .sum::<f64>();
            let left_norm = left
                .iter()
                .map(|value| f64::from(*value) * f64::from(*value))
                .sum::<f64>()
                .sqrt();
            let right_norm = right
                .iter()
                .map(|value| f64::from(*value) * f64::from(*value))
                .sum::<f64>()
                .sqrt();
            if left_norm == 0.0 || right_norm == 0.0 {
                0.0
            } else {
                finite_score(dot / (left_norm * right_norm))
            }
        }
    }
}

fn finite_score(value: f64) -> f32 {
    value.clamp(f64::from(f32::MIN), f64::from(f32::MAX)) as f32
}

fn ordinal_key(ordinal: u64) -> Vec<u8> {
    ordinal.to_be_bytes().to_vec()
}

fn insert_bounded(hits: &mut Vec<VectorHit>, hit: VectorHit, limit: usize) {
    if hits.iter().any(|current| current.document == hit.document) {
        return;
    }
    hits.push(hit);
    sort_hits(hits);
    hits.truncate(limit);
}

fn compare_hit_to_cursor(hit: &VectorHit, cursor: &VectorQueryCursor) -> Ordering {
    cursor
        .score
        .partial_cmp(&hit.score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| hit.document.cmp(&cursor.document))
}

fn validate_query_cursor(cursor: Option<&VectorQueryCursor>) -> Result<(), IndexError> {
    if cursor.is_some_and(|cursor| !cursor.score.is_finite() || cursor.document.validate().is_err())
    {
        return Err(IndexError::InvalidQuery(
            "invalid vector query continuation".into(),
        ));
    }
    Ok(())
}

fn sort_hits(hits: &mut [VectorHit]) {
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

    fn upsert(path: &str, version: u64, values: &[f32]) -> IndexMutation<VectorDocument> {
        IndexMutation::Upsert(VectorDocument {
            document: DocumentRef {
                path: path.into(),
                version,
            },
            values: values.to_vec(),
        })
    }

    async fn build(
        definition: &VectorDefinition,
        mutations: impl IntoIterator<Item = IndexMutation<VectorDocument>>,
        level: u8,
        target: usize,
    ) -> (MemoryBlockSink, SealedRun) {
        let mut builder = VectorSegmentBuilder::new(
            definition.clone(),
            SegmentBuildOptions::for_level(64 * 1024, level).unwrap(),
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
    async fn exact_scan_observes_updates_deletes_and_compaction() {
        let definition = VectorDefinition {
            dimension: 2,
            metric: VectorMetric::Cosine,
        };
        let (old_sink, old_run) = build(
            &definition,
            [
                upsert("/a", 1, &[1.0, 0.0]),
                upsert("/b", 1, &[1.0, 0.0]),
                upsert("/c", 1, &[0.0, 1.0]),
            ],
            0,
            96,
        )
        .await;
        let old = directory(&old_sink, old_run);
        let (new_sink, new_run) = build(
            &definition,
            [
                upsert("/a", 2, &[0.0, 1.0]),
                IndexMutation::Remove(DocumentRef {
                    path: "/b".into(),
                    version: 2,
                }),
            ],
            0,
            96,
        )
        .await;
        let new = directory(&new_sink, new_run);
        let runs = [new, old];
        let expected = VectorEngine::query(&runs, &definition, &[1.0, 0.0], 10)
            .await
            .unwrap();
        assert_eq!(expected.len(), 2);
        assert!(expected.iter().all(|hit| hit.document.path != "/b"));
        let cursor = VectorQueryCursor::from_hit(&expected[0]);
        assert_eq!(
            VectorEngine::query_after(&runs, &definition, &[1.0, 0.0], 10, Some(&cursor),)
                .await
                .unwrap(),
            expected[1..]
        );

        let mut merged_sink = MemoryBlockSink::default();
        let merged = VectorEngine::merge_with_target(&runs, &definition, 1, 96, &mut merged_sink)
            .await
            .unwrap();
        let merged = [directory(&merged_sink, merged)];
        assert_eq!(
            VectorEngine::query(&merged, &definition, &[1.0, 0.0], 10)
                .await
                .unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn output_is_deterministic_and_split_into_lazy_blocks() {
        let definition = VectorDefinition {
            dimension: 4,
            metric: VectorMetric::DotProduct,
        };
        let mutations = (0..100)
            .map(|index| {
                upsert(
                    &format!("/vectors/{index:04}"),
                    1,
                    &[index as f32, 1.0, 2.0, 3.0],
                )
            })
            .collect::<Vec<_>>();
        let (first_sink, first) = build(&definition, mutations.clone(), 1, 128).await;
        let (second_sink, second) = build(&definition, mutations, 1, 128).await;
        assert_eq!(first.descriptor().hash, second.descriptor().hash);
        assert_eq!(first_sink.len(), second_sink.len());
        assert!(first_sink.len() > 4);
    }

    #[test]
    fn invalid_vectors_fail_before_admission() {
        let definition = VectorDefinition {
            dimension: 2,
            metric: VectorMetric::Cosine,
        };
        let mut builder =
            VectorSegmentBuilder::new(definition, SegmentBuildOptions::new(1024).unwrap()).unwrap();
        assert!(builder.try_push(upsert("/a", 1, &[1.0])).is_err());
        assert!(builder.try_push(upsert("/a", 1, &[f32::NAN, 0.0])).is_err());
    }

    #[test]
    fn finite_inputs_always_produce_finite_scores() {
        let left = [f32::MAX, f32::MAX];
        let right = [f32::MAX, -f32::MAX];
        for metric in [
            VectorMetric::Cosine,
            VectorMetric::DotProduct,
            VectorMetric::Euclidean,
        ] {
            assert!(similarity(&left, &right, metric).is_finite());
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
            VectorHit {
                document: document.clone(),
                score: 1.0,
            },
            10,
        );
        insert_bounded(
            &mut hits,
            VectorHit {
                document,
                score: 0.5,
            },
            10,
        );
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].score, 1.0);
    }
}
