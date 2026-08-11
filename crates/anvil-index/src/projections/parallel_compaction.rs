//! Bounded deterministic range compaction for Git-source and tensor runs.

use std::sync::Arc;

use crate::compaction::{
    CompactionExecutor, CompactionParallelism, CompactionProgress, KeyRange, LaneResultProducer,
    PathWinnerCursor, collect_ordered_lanes, dense_ordinal_bases, deterministic_key_range_plan,
    deterministic_suffix_key_range_plan,
};
use crate::routed::{RoutedComponentWriter, RoutedCursor};
use crate::run::{ComponentTree, RunStatistics, RunView, assemble_component_ranges, seal_run_root};
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

const ROUTED_SUFFIX_BYTES: usize = 12;

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
    let output_path_root = path_tree.root.clone();
    let mut components = vec![path_tree];
    if statistics.live_document_count > 0 {
        components.push(documents.ok_or(IndexError::InvalidFormat(
            "missing compacted projection documents",
        ))?);
        components.push(projections.ok_or(IndexError::InvalidFormat(
            "missing compacted projection records",
        ))?);
        for tag in T::key_tags() {
            if let Some(tree) = merge_routed_component_parallel::<D, S, T, E>(
                runs.as_slice(),
                views.as_slice(),
                kind,
                *tag,
                output_level,
                target_block_bytes,
                &output_path_root,
                sink,
                parallelism,
                progress.clone(),
                executor.clone(),
            )
            .await?
            {
                components.push(tree);
            }
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

#[allow(clippy::too_many_arguments)]
async fn merge_routed_component_parallel<D, S, T, E>(
    runs: &[D],
    views: &[RunView],
    kind: IndexKind,
    tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    output_path_root: &crate::BlockDescriptor,
    sink: &mut S,
    parallelism: CompactionParallelism,
    progress: CompactionProgress,
    executor: E,
) -> Result<Option<ComponentTree>, IndexError>
where
    D: IndexDirectoryRead + Clone + 'static,
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    T: ProjectionPayload + Clone + Send + 'static,
    E: CompactionExecutor,
{
    let roots = views
        .iter()
        .map(|view| view.component_optional(tag).cloned())
        .collect::<Vec<_>>();
    let present_roots = roots.iter().flatten().cloned().collect::<Vec<_>>();
    if present_roots.is_empty() {
        return Ok(None);
    }
    let plan = deterministic_suffix_key_range_plan(
        present_roots.iter().cloned(),
        ROUTED_SUFFIX_BYTES,
        parallelism.max_lanes(),
    )?;
    progress.record_range_limit(plan.range_limit)?;
    let runs = Arc::new(runs.to_vec());
    let views = Arc::new(views.to_vec());
    let roots = Arc::new(roots);
    let mut producers =
        Vec::<LaneResultProducer<Option<ComponentTree>>>::with_capacity(plan.ranges.len());
    for range in plan.ranges {
        let runs = runs.clone();
        let views = views.clone();
        let roots = roots.clone();
        let output_path_root = output_path_root.clone();
        let lane_sink = sink.clone();
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        producers.push(Box::new(move || {
            Box::pin(build_projection_keys_range::<D, S, T, E>(
                runs,
                views,
                roots,
                kind,
                tag,
                output_level,
                target_block_bytes,
                output_path_root,
                range,
                lane_sink,
                lane_executor,
                lane_progress,
            ))
        }));
    }
    let trees = collect_ordered_lanes(&executor, producers, &progress)
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if trees.is_empty() {
        Ok(None)
    } else {
        Ok(Some(
            assemble_component_ranges(kind, tag, trees.iter(), sink).await?,
        ))
    }
}

#[allow(clippy::too_many_arguments)]
async fn build_projection_keys_range<D, S, T, E>(
    runs: Arc<Vec<D>>,
    views: Arc<Vec<RunView>>,
    roots: Arc<Vec<Option<crate::BlockDescriptor>>>,
    kind: IndexKind,
    tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    output_path_root: crate::BlockDescriptor,
    range: KeyRange,
    mut sink: S,
    executor: E,
    progress: CompactionProgress,
) -> Result<Option<ComponentTree>, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink + IndexDirectoryRead,
    T: ProjectionPayload + Clone + Send + 'static,
    E: CompactionExecutor,
{
    let mut cursors = runs
        .iter()
        .zip(roots.iter())
        .map(|(run, root)| {
            root.clone()
                .map(|root| RoutedCursor::in_range(run, root, range.clone()))
        })
        .collect::<Vec<_>>();
    let mut current = Vec::with_capacity(cursors.len());
    for cursor in &mut cursors {
        current.push(match cursor {
            Some(cursor) => cursor.next_parallel(&executor, &progress).await?,
            None => None,
        });
    }
    let mut point_cache = ProjectionPointCache::<T>::input_documents();
    let mut output_paths = ProjectionPointCache::<T>::staged_output_paths();
    let mut writer = RoutedComponentWriter::new(kind, tag, output_level, target_block_bytes);
    loop {
        let Some(primary) = current
            .iter()
            .flatten()
            .map(|row| row.primary.as_slice())
            .min()
            .map(<[u8]>::to_vec)
        else {
            return writer.finish(&mut sink).await;
        };
        while current.iter().flatten().any(|row| row.primary == primary) {
            let mut documents = vec![None; current.len()];
            for run_index in 0..current.len() {
                let Some(row) = current[run_index]
                    .as_ref()
                    .filter(|row| row.primary == primary)
                else {
                    continue;
                };
                documents[run_index] = Some(
                    point_cache
                        .document(
                            &runs[run_index],
                            &views[run_index],
                            row.ordinal,
                            &executor,
                            &progress,
                        )
                        .await?,
                );
            }
            let path = documents
                .iter()
                .flatten()
                .map(|document| document.path.as_str())
                .min()
                .ok_or(IndexError::InvalidFormat("projection key without document"))?
                .to_owned();
            let mut winner = None::<usize>;
            for (run_index, document) in documents.iter().enumerate() {
                let Some(document) = document.as_ref().filter(|document| document.path == path)
                else {
                    continue;
                };
                if winner.is_none_or(|current_index| {
                    let current = documents[current_index].as_ref().unwrap();
                    document.version > current.version
                        || (document.version == current.version && run_index < current_index)
                }) {
                    winner = Some(run_index);
                }
            }
            let winner_index = winner.expect("one current projection row supplied the path");
            let document = documents[winner_index].as_ref().unwrap().clone();
            let row = current[winner_index].as_ref().unwrap().clone();
            for run_index in 0..current.len() {
                if documents[run_index]
                    .as_ref()
                    .is_some_and(|document| document.path == path)
                {
                    current[run_index] = cursors[run_index]
                        .as_mut()
                        .expect("a current row always has a cursor")
                        .next_parallel(&executor, &progress)
                        .await?;
                }
            }
            let Some(output) = output_paths
                .path(
                    &sink,
                    &output_path_root,
                    &document.path,
                    &executor,
                    &progress,
                )
                .await?
            else {
                return Err(IndexError::InvalidFormat(
                    "compacted path missing from staged output",
                ));
            };
            if output.state == DocumentState::Live && output.document.version == document.version {
                let ordinal = output.document_ordinal.ok_or(IndexError::InvalidFormat(
                    "compacted live path has no ordinal",
                ))?;
                writer.push(row.with_ordinal(ordinal), &mut sink).await?;
            }
        }
    }
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
