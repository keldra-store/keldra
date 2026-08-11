//! Bounded deterministic range compaction for Git-source and tensor runs.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::compaction::{
    CompactionExecutor, CompactionParallelism, CompactionProgress, KeyRange, LaneResultProducer,
    PathWinnerCursor, collect_ordered_lanes, dense_ordinal_bases, deterministic_key_range_plan,
};
use crate::routed_sort::{
    MAX_EXTERNAL_SORT_CHUNK_RESIDENT_BYTES, RoutedExternalSorter, merge_routed_component_trees,
};
use crate::run::{
    ComponentTree, LeafCursor, RunStatistics, RunView, assemble_component_ranges, seal_run_root,
};
use crate::segment::{
    DOCUMENTS_TAG, DocumentComponentWriter, DocumentRecord, DocumentState, PATH_CHANGES_TAG,
    PathComponentWriter,
};
use crate::{IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind, SealedRun};

use super::compaction_cache::ProjectionPointCache;
use super::{
    OrdinalComponentWriter, OrdinalRow, ProjectionPayload, decode_ordinal_rows, open_views,
    ordinal_key,
};

#[allow(clippy::too_many_arguments)]
pub(super) async fn merge_projection_parallel<D, S, T, E>(
    runs: &[D],
    kind: IndexKind,
    output_level: u8,
    target_block_bytes: usize,
    sink: &mut S,
    parallelism: CompactionParallelism,
    progress: CompactionProgress,
    executor: E,
) -> Result<SealedRun, IndexError>
where
    D: IndexDirectoryRead + Clone + 'static,
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    T: ProjectionPayload + Clone + Send + 'static,
    E: CompactionExecutor,
{
    if runs.is_empty() || output_level == 0 {
        return Err(IndexError::InvalidDefinition(
            "projection compaction requires input runs and an L1+ output level".into(),
        ));
    }
    crate::compaction::validate_parallel_compaction_fan_in(runs.len())?;
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
    let expected_live = if ranges.len() == 1 {
        None
    } else {
        let mut producers =
            Vec::<LaneResultProducer<ProjectionRangeCount>>::with_capacity(ranges.len());
        for range in ranges.iter().cloned() {
            let runs = runs.clone();
            let roots = roots.clone();
            let lane_executor = executor.clone();
            let lane_progress = progress.clone();
            producers.push(Box::new(move || {
                Box::pin(count_projection_range(
                    runs,
                    roots,
                    range,
                    lane_executor,
                    lane_progress,
                ))
            }));
        }
        Some(collect_ordered_lanes(&executor, producers, &progress).await?)
    };
    let (ordinal_bases, expected_total_live) = match &expected_live {
        Some(counts) => {
            dense_ordinal_bases(&counts.iter().map(|count| count.live).collect::<Vec<_>>())?
        }
        None => (vec![0], 0),
    };
    let mut producers =
        Vec::<LaneResultProducer<Option<ProjectionRangeOutput>>>::with_capacity(ranges.len());
    for (range, ordinal_base) in ranges.into_iter().zip(ordinal_bases) {
        let runs = runs.clone();
        let views = views.clone();
        let roots = roots.clone();
        let lane_sink = sink.clone();
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        producers.push(Box::new(move || {
            Box::pin(build_projection_range::<D, S, T, E>(
                runs,
                views,
                roots,
                kind,
                output_level,
                target_block_bytes,
                range,
                ordinal_base,
                lane_sink,
                lane_executor,
                lane_progress,
            ))
        }));
    }
    let outputs = collect_ordered_lanes(&executor, producers, &progress).await?;
    if let Some(expected) = &expected_live {
        for (expected, output) in expected.iter().zip(&outputs) {
            let actual = output
                .as_ref()
                .map_or(0, |output| output.statistics.live_document_count);
            if actual != expected.live {
                return Err(IndexError::InvalidFormat(
                    "projection range live count changed",
                ));
            }
        }
    }
    let outputs = outputs.into_iter().flatten().collect::<Vec<_>>();
    let statistics = aggregate_projection_statistics(&outputs)?;
    if expected_live.is_some() && statistics.live_document_count != expected_total_live {
        return Err(IndexError::InvalidFormat(
            "projection range live count changed",
        ));
    }
    let path_tree = assemble_component_ranges(
        kind,
        PATH_CHANGES_TAG,
        outputs.iter().map(|output| &output.paths),
        sink,
    )
    .await?;
    let documents = if statistics.live_document_count == 0 {
        None
    } else {
        Some(
            assemble_component_ranges(
                kind,
                DOCUMENTS_TAG,
                outputs
                    .iter()
                    .filter_map(|output| output.documents.as_ref()),
                sink,
            )
            .await?,
        )
    };
    let projections = if statistics.live_document_count == 0 {
        None
    } else {
        Some(
            assemble_component_ranges(
                kind,
                super::RECORDS_TAG,
                outputs
                    .iter()
                    .filter_map(|output| output.projections.as_ref()),
                sink,
            )
            .await?,
        )
    };
    // Rebuild routed query components only after every source range has
    // finished. This keeps source path/payload caches and routed external-sort
    // merges in disjoint resident-memory phases.
    let routed = if statistics.live_document_count == 0 {
        Vec::new()
    } else {
        rebuild_projection_routes::<S, T, E>(
            kind,
            output_level,
            target_block_bytes,
            outputs
                .iter()
                .filter_map(|output| output.projections.clone())
                .collect(),
            sink,
            executor.clone(),
            progress.clone(),
        )
        .await?
    };
    let mut components = vec![path_tree];
    if statistics.live_document_count > 0 {
        components.push(documents.ok_or(IndexError::InvalidFormat(
            "missing compacted projection documents",
        ))?);
        components.push(projections.ok_or(IndexError::InvalidFormat(
            "missing compacted projection records",
        ))?);
        for (_, tree) in routed {
            components.push(tree);
        }
    }
    seal_run_root(kind, output_level, statistics, components)
}

#[derive(Clone, Copy)]
struct ProjectionRangeCount {
    live: u64,
}

struct ProjectionRangeOutput {
    paths: ComponentTree,
    documents: Option<ComponentTree>,
    projections: Option<ComponentTree>,
    statistics: RunStatistics,
}

async fn count_projection_range<D, E>(
    runs: Arc<Vec<D>>,
    roots: Arc<Vec<crate::BlockDescriptor>>,
    range: KeyRange,
    executor: E,
    progress: CompactionProgress,
) -> Result<ProjectionRangeCount, IndexError>
where
    D: IndexDirectoryRead,
    E: CompactionExecutor,
{
    let mut winners =
        PathWinnerCursor::open(runs.as_slice(), roots.as_slice(), range, executor, progress)
            .await?;
    let mut live = 0u64;
    while let Some((_, winner)) = winners.next().await? {
        if winner.state == DocumentState::Live {
            live = live.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        }
    }
    Ok(ProjectionRangeCount { live })
}

#[allow(clippy::too_many_arguments)]
async fn build_projection_range<D, S, T, E>(
    runs: Arc<Vec<D>>,
    views: Arc<Vec<RunView>>,
    roots: Arc<Vec<crate::BlockDescriptor>>,
    kind: IndexKind,
    output_level: u8,
    target_block_bytes: usize,
    range: KeyRange,
    ordinal_base: u64,
    mut sink: S,
    executor: E,
    progress: CompactionProgress,
) -> Result<Option<ProjectionRangeOutput>, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink,
    T: ProjectionPayload + Clone + Send + 'static,
    E: CompactionExecutor,
{
    let mut winners = PathWinnerCursor::open(
        runs.as_slice(),
        roots.as_slice(),
        range,
        executor.clone(),
        progress.clone(),
    )
    .await?;
    let mut point_cache = ProjectionPointCache::<T>::default();
    let mut paths = PathComponentWriter::new(kind, output_level, target_block_bytes);
    let mut documents = DocumentComponentWriter::with_ordinal_base(
        kind,
        output_level,
        target_block_bytes,
        ordinal_base,
    );
    let mut projections = OrdinalComponentWriter::new(kind, output_level, target_block_bytes);
    let mut mutation_count = 0u64;
    let mut live = 0u64;
    let mut minimum_version = u64::MAX;
    let mut maximum_version = 0u64;
    while let Some((winner_run, mut winner)) = winners.next().await? {
        if winner.state == DocumentState::Live {
            let old_ordinal = winner
                .document_ordinal
                .ok_or(IndexError::InvalidFormat("live projection has no ordinal"))?;
            let source_document = point_cache
                .document(
                    &runs[winner_run],
                    &views[winner_run],
                    old_ordinal,
                    &executor,
                    &progress,
                )
                .await?;
            if source_document != winner.document {
                return Err(IndexError::InvalidFormat("projection document mismatch"));
            }
            let payload = point_cache
                .projection(
                    &runs[winner_run],
                    &views[winner_run],
                    old_ordinal,
                    &executor,
                    &progress,
                )
                .await?;
            let ordinal = ordinal_base
                .checked_add(live)
                .ok_or(IndexError::OffsetOverflow)?;
            live = live.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
            winner.document_ordinal = Some(ordinal);
            documents
                .push(
                    DocumentRecord {
                        ordinal,
                        document: winner.document.clone(),
                    },
                    &mut sink,
                )
                .await?;
            projections
                .push(OrdinalRow { ordinal, payload }, &mut sink)
                .await?;
        } else {
            winner.document_ordinal = None;
        }
        minimum_version = minimum_version.min(winner.document.version);
        maximum_version = maximum_version.max(winner.document.version);
        mutation_count = mutation_count
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        paths.push(winner, &mut sink).await?;
        progress.record_output(1, 0, 0);
    }
    if mutation_count == 0 {
        return Ok(None);
    }
    let paths = paths.finish(&mut sink).await?;
    let (documents, projections) = if live == 0 {
        (None, None)
    } else {
        (
            Some(documents.finish(&mut sink).await?),
            Some(projections.finish(&mut sink).await?),
        )
    };
    Ok(Some(ProjectionRangeOutput {
        paths,
        documents,
        projections,
        statistics: RunStatistics {
            mutation_count,
            live_document_count: live,
            minimum_version,
            maximum_version,
        },
    }))
}

#[allow(clippy::too_many_arguments)]
async fn rebuild_projection_routes<S, T, E>(
    kind: IndexKind,
    output_level: u8,
    target_block_bytes: usize,
    projection_ranges: Vec<ComponentTree>,
    sink: &mut S,
    executor: E,
    progress: CompactionProgress,
) -> Result<Vec<(u8, ComponentTree)>, IndexError>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    T: ProjectionPayload + Send + 'static,
    E: CompactionExecutor,
{
    if T::key_tags().is_empty() {
        return Err(IndexError::InvalidFormat(
            "projection payload has no routed components",
        ));
    }
    let mut producers =
        Vec::<LaneResultProducer<Vec<(u8, ComponentTree)>>>::with_capacity(projection_ranges.len());
    for projection in projection_ranges {
        let lane_sink = sink.clone();
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        producers.push(Box::new(move || {
            Box::pin(rebuild_projection_route_range::<S, T, E>(
                kind,
                output_level,
                target_block_bytes,
                projection,
                lane_sink,
                lane_executor,
                lane_progress,
            ))
        }));
    }
    let ranges = collect_ordered_lanes(&executor, producers, &progress).await?;
    let mut routed = Vec::with_capacity(T::key_tags().len());
    for tag in T::key_tags() {
        let trees = ranges
            .iter()
            .filter_map(|range| {
                range
                    .iter()
                    .find_map(|(candidate, tree)| (candidate == tag).then_some(tree))
            })
            .cloned()
            .collect();
        if let Some(tree) = merge_routed_component_trees(
            kind,
            *tag,
            output_level,
            target_block_bytes,
            trees,
            sink,
            &executor,
            &progress,
        )
        .await?
        {
            routed.push((*tag, tree));
        }
    }
    Ok(routed)
}

#[allow(clippy::too_many_arguments)]
async fn rebuild_projection_route_range<S, T, E>(
    kind: IndexKind,
    output_level: u8,
    target_block_bytes: usize,
    projection: ComponentTree,
    sink: S,
    executor: E,
    progress: CompactionProgress,
) -> Result<Vec<(u8, ComponentTree)>, IndexError>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone,
    T: ProjectionPayload + Send + 'static,
    E: CompactionExecutor,
{
    let sorter_count = T::key_tags().len();
    let sorter_chunk_bytes = MAX_EXTERNAL_SORT_CHUNK_RESIDENT_BYTES
        .checked_div(sorter_count)
        .filter(|bytes| *bytes > 0)
        .ok_or(IndexError::OffsetOverflow)?;
    let directory = sink.clone();
    let mut cursor = StagedProjectionCursor::<_, T>::new(&directory, projection.root);
    let mut sorters = Vec::with_capacity(sorter_count);
    for tag in T::key_tags() {
        sorters.push((
            *tag,
            RoutedExternalSorter::new(
                kind,
                *tag,
                output_level,
                target_block_bytes,
                sorter_chunk_bytes,
                sink.clone(),
                executor.clone(),
                progress.clone(),
            )?,
        ));
    }
    while let Some(OrdinalRow { ordinal, payload }) = cursor.next(&executor, &progress).await? {
        for (tag, row) in payload.key_rows(ordinal)? {
            let sorter = sorters
                .iter_mut()
                .find_map(|(candidate, sorter)| (candidate == &tag).then_some(sorter))
                .ok_or(IndexError::InvalidFormat(
                    "projection payload emitted an unknown routed component",
                ))?;
            sorter.push(row).await?;
        }
    }
    let mut routed = Vec::with_capacity(sorters.len());
    for (tag, sorter) in sorters {
        if let Some(tree) = sorter.finish().await? {
            routed.push((tag, tree));
        }
    }
    Ok(routed)
}

struct StagedProjectionCursor<'a, D, T> {
    directory: &'a D,
    leaves: LeafCursor<'a, D>,
    rows: VecDeque<OrdinalRow<T>>,
}

impl<'a, D, T> StagedProjectionCursor<'a, D, T>
where
    D: IndexDirectoryRead,
    T: ProjectionPayload + Send + 'static,
{
    fn new(directory: &'a D, root: crate::BlockDescriptor) -> Self {
        Self {
            directory,
            leaves: LeafCursor::new(directory, root),
            rows: VecDeque::new(),
        }
    }

    async fn next<E: CompactionExecutor>(
        &mut self,
        executor: &E,
        progress: &CompactionProgress,
    ) -> Result<Option<OrdinalRow<T>>, IndexError> {
        loop {
            if let Some(row) = self.rows.pop_front() {
                return Ok(Some(row));
            }
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            progress.record_input(0, descriptor.encoded_bytes, 1);
            self.rows =
                read_projection_block_parallel(self.directory, &descriptor, executor, progress)
                    .await?
                    .into();
        }
    }
}

fn aggregate_projection_statistics(
    outputs: &[ProjectionRangeOutput],
) -> Result<RunStatistics, IndexError> {
    if outputs.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "projection compaction produced no changes".into(),
        ));
    }
    let mut statistics = RunStatistics {
        mutation_count: 0,
        live_document_count: 0,
        minimum_version: u64::MAX,
        maximum_version: 0,
    };
    for output in outputs {
        statistics.mutation_count = statistics
            .mutation_count
            .checked_add(output.statistics.mutation_count)
            .ok_or(IndexError::OffsetOverflow)?;
        statistics.live_document_count = statistics
            .live_document_count
            .checked_add(output.statistics.live_document_count)
            .ok_or(IndexError::OffsetOverflow)?;
        statistics.minimum_version = statistics
            .minimum_version
            .min(output.statistics.minimum_version);
        statistics.maximum_version = statistics
            .maximum_version
            .max(output.statistics.maximum_version);
    }
    Ok(statistics)
}

pub(super) async fn read_projection_block_parallel<D, T, E>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
    executor: &E,
    progress: &CompactionProgress,
) -> Result<Vec<OrdinalRow<T>>, IndexError>
where
    D: IndexDirectoryRead,
    T: ProjectionPayload + Send + 'static,
    E: CompactionExecutor,
{
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let descriptor = descriptor.clone();
    let rows = executor
        .run_cpu(move || {
            let rows = decode_ordinal_rows(block.body(), descriptor.codec)?;
            if rows.first().map(|row| ordinal_key(row.ordinal))
                != Some(descriptor.minimum_key.clone())
                || rows.last().map(|row| ordinal_key(row.ordinal))
                    != Some(descriptor.maximum_key.clone())
                || rows.len() as u64 != descriptor.element_count
            {
                return Err(IndexError::InvalidFormat("projection block descriptor"));
            }
            Ok(rows)
        })
        .await?;
    progress.record_input(rows.len() as u64, 0, 0);
    Ok(rows)
}
