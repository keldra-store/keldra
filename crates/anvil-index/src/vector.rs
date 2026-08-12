//! Exact vector search over bounded immutable runs.

#[path = "vector_compaction_cache.rs"]
mod compaction_cache;

use std::cmp::Ordering;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::codec::{Decoder, Encoder, encode_component};
use crate::compaction::{
    CompactionExecutor, CompactionParallelism, CompactionProgress, LaneResultProducer,
    PathWinnerCursor, collect_ordered_lanes, deterministic_key_range_plan,
};
use crate::run::{
    ComponentTree, LeafCursor, RoutingTreeBuilder, RunStatistics, RunView,
    assemble_component_ranges, find_leaf, open_views, seal_run_root,
};
use crate::segment::{
    DEFAULT_COMPONENT_BLOCK_BYTES, DocumentComponentWriter, DocumentRecord, DocumentState,
    LatestLiveProbe, MutationBuffer, PATH_CHANGES_TAG, PathChange, PathComponentWriter,
    PathRunCursor, document_by_ordinal,
};
use crate::succinct::{decode_elias_fano_with_budget, encode_elias_fano};
use crate::{
    ComponentCodec, DocumentRef, GeneratedBlock, IndexBlockSink, IndexDirectoryRead, IndexError,
    IndexKind, IndexMutation, SealedRun, SegmentBuildOptions, SegmentPush,
};

use compaction_cache::VectorCompactionPointCache;

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
        let mut live_probe = LatestLiveProbe::new();
        let mut hits = Vec::with_capacity(limit.min(128));
        let mut hit_chunk = Vec::with_capacity(128);
        for (run, view) in runs.iter().zip(&views) {
            let Some(root) = view.component_optional(VECTORS_TAG) else {
                continue;
            };
            let mut leaves = LeafCursor::new(run, root.clone());
            while let Some(descriptor) = leaves.next().await? {
                let rows = read_vector_block(run, &descriptor, definition).await?;
                let query_values = query.to_vec();
                let metric = definition.metric;
                let scored = run
                    .run_query_cpu(move || {
                        Ok(rows
                            .into_iter()
                            .map(|row| {
                                let score = similarity(&query_values, &row.values, metric);
                                (row, score)
                            })
                            .collect::<Vec<_>>())
                    })
                    .await?;
                for (row, score) in scored {
                    let document = document_by_ordinal(run, view, row.ordinal).await?;
                    if !live_probe.is_latest_live(runs, &views, &document).await? {
                        continue;
                    }
                    let hit = VectorHit { document, score };
                    if after.is_some_and(|cursor| {
                        compare_hit_to_cursor(&hit, cursor) != Ordering::Greater
                    }) {
                        continue;
                    }
                    hit_chunk.push(hit);
                    if hit_chunk.len() == 128 {
                        hits = run
                            .run_query_cpu({
                                let retained = std::mem::take(&mut hits);
                                let candidates = std::mem::take(&mut hit_chunk);
                                move || Ok(merge_vector_hits(retained, candidates, limit))
                            })
                            .await?;
                    }
                }
            }
        }
        if !hit_chunk.is_empty() {
            hits = runs[0]
                .run_query_cpu(move || Ok(merge_vector_hits(hits, hit_chunk, limit)))
                .await?;
        }
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

    pub async fn merge_runs_parallel<D, S, E>(
        runs: &[D],
        definition: &VectorDefinition,
        output_level: u8,
        sink: &mut S,
        parallelism: CompactionParallelism,
        progress: CompactionProgress,
        executor: E,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead + Clone + 'static,
        S: IndexBlockSink + Clone + 'static,
        E: CompactionExecutor,
    {
        definition.validate()?;
        Self::merge_parallel_with_target(
            runs,
            definition,
            output_level,
            DEFAULT_COMPONENT_BLOCK_BYTES,
            sink,
            parallelism,
            progress,
            executor,
        )
        .await
    }

    async fn merge_parallel_with_target<D, S, E>(
        runs: &[D],
        definition: &VectorDefinition,
        output_level: u8,
        target_block_bytes: usize,
        sink: &mut S,
        parallelism: CompactionParallelism,
        progress: CompactionProgress,
        executor: E,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead + Clone + 'static,
        S: IndexBlockSink + Clone + 'static,
        E: CompactionExecutor,
    {
        if runs.is_empty() || output_level == 0 {
            return Err(IndexError::InvalidDefinition(
                "vector compaction requires input runs and an L1+ output level".into(),
            ));
        }
        crate::compaction::validate_parallel_compaction_fan_in(runs.len())?;
        let (path_tree, document_tree, vector_tree, statistics) = merge_vector_components_parallel(
            runs,
            IndexKind::Vector,
            definition,
            VECTORS_TAG,
            output_level,
            target_block_bytes,
            sink,
            parallelism,
            progress,
            executor,
        )
        .await?;
        let mut components = vec![path_tree];
        if let Some(tree) = document_tree {
            components.push(tree);
        }
        if let Some(tree) = vector_tree {
            components.push(tree);
        }
        seal_run_root(IndexKind::Vector, output_level, statistics, components)
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn merge_vector_components_parallel<D, S, E>(
    runs: &[D],
    kind: IndexKind,
    definition: &VectorDefinition,
    vector_tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    sink: &mut S,
    parallelism: CompactionParallelism,
    progress: CompactionProgress,
    executor: E,
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
    D: IndexDirectoryRead + Clone + 'static,
    S: IndexBlockSink + Clone + 'static,
    E: CompactionExecutor,
{
    let views = open_views(runs, kind).await?;
    let roots = views
        .iter()
        .map(|view| view.component(PATH_CHANGES_TAG).cloned())
        .collect::<Result<Vec<_>, _>>()?;
    let plan = deterministic_key_range_plan(roots.iter().cloned(), parallelism.max_lanes());
    progress.record_range_limit(plan.range_limit)?;
    let ranges = plan.ranges;
    let runs = Arc::new(runs.to_vec());
    let views = Arc::new(views);
    let roots = Arc::new(roots);
    let definition = Arc::new(definition.clone());

    let mut write_producers =
        Vec::<LaneResultProducer<Option<VectorLaneComponents>>>::with_capacity(ranges.len());
    for (range_id, range) in ranges.into_iter().enumerate() {
        let range_id = u64::try_from(range_id).map_err(|_| IndexError::OffsetOverflow)?;
        let ordinal_base = crate::bulk::range_ordinal_base(range_id)?;
        let runs = runs.clone();
        let views = views.clone();
        let roots = roots.clone();
        let definition = definition.clone();
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        let mut lane_sink = sink.fork()?;
        write_producers.push(Box::new(move || {
            Box::pin(async move {
                let mut paths = PathComponentWriter::new(kind, output_level, target_block_bytes);
                let mut documents = DocumentComponentWriter::with_ordinal_base(
                    kind,
                    output_level,
                    target_block_bytes,
                    ordinal_base,
                );
                let mut vectors = VectorComponentWriter::new(
                    kind,
                    vector_tag,
                    output_level,
                    definition.dimension,
                    target_block_bytes,
                );
                let mut point_cache = VectorCompactionPointCache::default();
                let mut summary = VectorLaneSummary::default();
                let mut cursor = PathWinnerCursor::open(
                    runs.as_slice(),
                    roots.as_slice(),
                    range,
                    lane_executor.clone(),
                    lane_progress.clone(),
                )
                .await?;
                while let Some((winner_run, mut winner)) = cursor.next().await? {
                    let local_ordinal = summary.live_count;
                    summary.observe(&winner)?;
                    if winner.state == DocumentState::Live {
                        let input_ordinal = winner
                            .document_ordinal
                            .ok_or(IndexError::InvalidFormat("live vector has no ordinal"))?;
                        let source_document = point_cache
                            .document(
                                &runs[winner_run],
                                &views[winner_run],
                                input_ordinal,
                                &lane_executor,
                                &lane_progress,
                            )
                            .await?;
                        if source_document != winner.document {
                            return Err(IndexError::InvalidFormat("vector document mismatch"));
                        }
                        let values = point_cache
                            .vector(
                                &runs[winner_run],
                                &views[winner_run],
                                &definition,
                                vector_tag,
                                input_ordinal,
                                &lane_executor,
                                &lane_progress,
                            )
                            .await?;
                        let ordinal =
                            crate::bulk::range_local_ordinal(ordinal_base, local_ordinal)?;
                        winner.document_ordinal = Some(ordinal);
                        documents
                            .push(
                                DocumentRecord {
                                    ordinal,
                                    document: winner.document.clone(),
                                },
                                &mut lane_sink,
                            )
                            .await?;
                        vectors
                            .push(VectorRow { ordinal, values }, &mut lane_sink)
                            .await?;
                    } else {
                        winner.document_ordinal = None;
                    }
                    paths.push(winner, &mut lane_sink).await?;
                    lane_progress.record_output(1, 0, 0);
                }
                if summary.mutation_count == 0 {
                    return Ok(None);
                }
                Ok(Some(VectorLaneComponents {
                    paths: paths.finish(&mut lane_sink).await?,
                    documents: if summary.live_count == 0 {
                        None
                    } else {
                        Some(documents.finish(&mut lane_sink).await?)
                    },
                    vectors: if summary.live_count == 0 {
                        None
                    } else {
                        Some(vectors.finish(&mut lane_sink).await?)
                    },
                    summary,
                }))
            })
        }));
    }
    let lane_components = collect_ordered_lanes(&executor, write_producers, &progress)
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let mutation_count = lane_components.iter().try_fold(0u64, |total, lane| {
        total
            .checked_add(lane.summary.mutation_count)
            .ok_or(IndexError::OffsetOverflow)
    })?;
    if mutation_count == 0 {
        return Err(IndexError::InvalidDefinition(
            "vector compaction produced no changes".into(),
        ));
    }
    let live_document_count = lane_components.iter().try_fold(0u64, |total, lane| {
        total
            .checked_add(lane.summary.live_count)
            .ok_or(IndexError::OffsetOverflow)
    })?;
    let minimum_version = lane_components
        .iter()
        .map(|lane| lane.summary.minimum_version)
        .min()
        .expect("nonempty vector compaction has one lane");
    let maximum_version = lane_components
        .iter()
        .map(|lane| lane.summary.maximum_version)
        .max()
        .expect("nonempty vector compaction has one lane");
    let path_tree = assemble_component_ranges(
        kind,
        PATH_CHANGES_TAG,
        lane_components.iter().map(|lane| lane.paths.clone()),
        sink,
    )
    .await?;
    let (document_tree, vector_tree) = if live_document_count == 0 {
        (None, None)
    } else {
        (
            Some(
                assemble_component_ranges(
                    kind,
                    crate::segment::DOCUMENTS_TAG,
                    lane_components
                        .iter()
                        .filter_map(|lane| lane.documents.clone()),
                    sink,
                )
                .await?,
            ),
            Some(
                assemble_component_ranges(
                    kind,
                    vector_tag,
                    lane_components
                        .iter()
                        .filter_map(|lane| lane.vectors.clone()),
                    sink,
                )
                .await?,
            ),
        )
    };
    Ok((
        path_tree,
        document_tree,
        vector_tree,
        RunStatistics {
            mutation_count,
            live_document_count,
            minimum_version,
            maximum_version,
        },
    ))
}

#[derive(Clone, Copy, Debug)]
struct VectorLaneSummary {
    mutation_count: u64,
    live_count: u64,
    minimum_version: u64,
    maximum_version: u64,
}

impl Default for VectorLaneSummary {
    fn default() -> Self {
        Self {
            mutation_count: 0,
            live_count: 0,
            minimum_version: u64::MAX,
            maximum_version: 0,
        }
    }
}

impl VectorLaneSummary {
    fn observe(&mut self, winner: &PathChange) -> Result<(), IndexError> {
        self.mutation_count = self
            .mutation_count
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        if winner.state == DocumentState::Live {
            self.live_count = self
                .live_count
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        self.minimum_version = self.minimum_version.min(winner.document.version);
        self.maximum_version = self.maximum_version.max(winner.document.version);
        Ok(())
    }
}

struct VectorLaneComponents {
    paths: ComponentTree,
    documents: Option<ComponentTree>,
    vectors: Option<ComponentTree>,
    summary: VectorLaneSummary,
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
    resident_bytes: usize,
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
            resident_bytes: 0,
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
        let row_resident_bytes = row
            .values
            .capacity()
            .saturating_mul(std::mem::size_of::<f32>())
            .saturating_add(2 * std::mem::size_of::<VectorRow>());
        if row_resident_bytes > crate::MAX_INDEX_DECODED_BLOCK_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: row_resident_bytes,
                limit: crate::MAX_INDEX_DECODED_BLOCK_BYTES,
            });
        }
        if !self.rows.is_empty()
            && (self.estimated_bytes.saturating_add(row_bytes) > self.target_bytes
                || self.resident_bytes.saturating_add(row_resident_bytes)
                    > crate::MAX_INDEX_DECODED_BLOCK_BYTES)
        {
            self.flush(sink).await?;
        }
        self.estimated_bytes = self.estimated_bytes.saturating_add(row_bytes);
        self.resident_bytes = self.resident_bytes.saturating_add(row_resident_bytes);
        self.rows.push(row);
        Ok(())
    }

    async fn flush<S: IndexBlockSink>(&mut self, sink: &mut S) -> Result<(), IndexError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        self.estimated_bytes = 0;
        self.resident_bytes = 0;
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
    let descriptor = descriptor.clone();
    let definition = definition.clone();
    directory
        .run_query_cpu(move || {
            let rows = decode_vector_rows(block.body(), &definition)?;
            validate_vector_block(rows, &descriptor)
        })
        .await
}

async fn read_vector_block_parallel<D, E>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
    definition: &VectorDefinition,
    executor: &E,
    progress: &CompactionProgress,
) -> Result<Vec<VectorRow>, IndexError>
where
    D: IndexDirectoryRead,
    E: CompactionExecutor,
{
    if descriptor.codec != ComponentCodec::FixedVectors {
        return Err(IndexError::InvalidFormat("vector block codec"));
    }
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let descriptor = descriptor.clone();
    let definition = definition.clone();
    let rows = executor
        .run_cpu(move || {
            let rows = decode_vector_rows(block.body(), &definition)?;
            validate_vector_block(rows, &descriptor)
        })
        .await?;
    progress.record_input(rows.len() as u64, 0, 0);
    Ok(rows)
}

fn validate_vector_block(
    rows: Vec<VectorRow>,
    descriptor: &crate::BlockDescriptor,
) -> Result<Vec<VectorRow>, IndexError> {
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

fn merge_vector_hits(
    mut retained: Vec<VectorHit>,
    candidates: Vec<VectorHit>,
    limit: usize,
) -> Vec<VectorHit> {
    for hit in candidates {
        insert_bounded(&mut retained, hit, limit);
    }
    sort_hits(&mut retained);
    retained
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
    use crate::compaction::COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES;
    use crate::compaction::test_support::TokioExecutor;
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

    #[tokio::test]
    async fn parallel_ranges_are_deterministic_and_query_equivalent() {
        let definition = VectorDefinition {
            dimension: 3,
            metric: VectorMetric::DotProduct,
        };
        let old_mutations = (0..72)
            .map(|index| {
                upsert(
                    &format!("/{:02x}/vector/{index:04}", index % 24),
                    1,
                    &[index as f32, 1.0, 2.0],
                )
            })
            .collect::<Vec<_>>();
        let (old_sink, old_run) = build(&definition, old_mutations, 0, 96).await;
        let old = directory(&old_sink, old_run);
        let new_mutations = (0..36)
            .map(|index| {
                let path = format!("/{:02x}/vector/{index:04}", index % 24);
                if index % 7 == 0 {
                    IndexMutation::Remove(DocumentRef { path, version: 2 })
                } else {
                    upsert(&path, 2, &[2.0, index as f32, 3.0])
                }
            })
            .collect::<Vec<_>>();
        let (new_sink, new_run) = build(&definition, new_mutations, 0, 96).await;
        let runs = [directory(&new_sink, new_run), old];

        let one_lane_progress = CompactionProgress::default();
        let mut one_lane_sink = MemoryBlockSink::default();
        let one_lane = VectorEngine::merge_parallel_with_target(
            &runs,
            &definition,
            1,
            96,
            &mut one_lane_sink,
            CompactionParallelism::serial(),
            one_lane_progress.clone(),
            TokioExecutor::default(),
        )
        .await
        .unwrap();
        let progress = CompactionProgress::default();
        let mut parallel_sink = MemoryBlockSink::default();
        let parallel = VectorEngine::merge_parallel_with_target(
            &runs,
            &definition,
            1,
            96,
            &mut parallel_sink,
            CompactionParallelism::new(4, COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES).unwrap(),
            progress.clone(),
            TokioExecutor::default(),
        )
        .await
        .unwrap();

        let mut repeated_sink = MemoryBlockSink::default();
        let repeated = VectorEngine::merge_parallel_with_target(
            &runs,
            &definition,
            1,
            96,
            &mut repeated_sink,
            CompactionParallelism::new(4, COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES).unwrap(),
            CompactionProgress::default(),
            TokioExecutor::default(),
        )
        .await
        .unwrap();

        assert_eq!(parallel, repeated);
        assert_eq!(parallel_sink.len(), repeated_sink.len());
        assert_eq!(
            parallel.descriptor().mutation_count,
            one_lane.descriptor().mutation_count
        );
        assert_eq!(
            parallel.descriptor().live_document_count,
            one_lane.descriptor().live_document_count
        );
        assert_eq!(
            parallel.descriptor().minimum_version,
            one_lane.descriptor().minimum_version
        );
        assert_eq!(
            parallel.descriptor().maximum_version,
            one_lane.descriptor().maximum_version
        );
        let parallel_mutation_count = parallel.descriptor().mutation_count;
        let one_lane = [directory(&one_lane_sink, one_lane)];
        let parallel_directory = [directory(&parallel_sink, parallel)];
        for query in [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [2.0, 3.0, 1.0]] {
            assert_eq!(
                VectorEngine::query(&parallel_directory, &definition, &query, 128)
                    .await
                    .unwrap(),
                VectorEngine::query(&one_lane, &definition, &query, 128)
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(one_lane_progress.snapshot().effective_lanes, 1);
        let snapshot = progress.snapshot();
        assert!(snapshot.effective_lanes > 1 && snapshot.effective_lanes <= 4);
        assert_eq!(snapshot.ranges_completed, snapshot.ranges_total);
        assert_eq!(snapshot.output_records, parallel_mutation_count);
        assert!(snapshot.input_records >= parallel_mutation_count);
    }

    #[tokio::test]
    async fn parallel_cpu_failure_joins_all_vector_ranges() {
        let definition = VectorDefinition {
            dimension: 2,
            metric: VectorMetric::Cosine,
        };
        let (sink, run) = build(
            &definition,
            [upsert("/a", 1, &[1.0, 0.0]), upsert("/z", 1, &[0.0, 1.0])],
            0,
            64,
        )
        .await;
        let runs = [directory(&sink, run)];
        let progress = CompactionProgress::default();
        let mut output = MemoryBlockSink::default();
        let error = VectorEngine::merge_parallel_with_target(
            &runs,
            &definition,
            1,
            64,
            &mut output,
            CompactionParallelism::new(4, COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES).unwrap(),
            progress.clone(),
            TokioExecutor::failing_cpu(),
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("injected compaction CPU failure")
        );
        let snapshot = progress.snapshot();
        assert_eq!(snapshot.active_lanes, 0);
        assert_eq!(snapshot.waiting_lanes, 0);
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
