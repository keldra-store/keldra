//! Deterministic bounded range compaction for full-text path and posting data.

use std::sync::Arc;

use crate::compaction::{
    CompactionExecutor, CompactionParallelism, CompactionProgress, KeyRange, LaneResultProducer,
    PathWinnerCursor, collect_ordered_lanes, dense_ordinal_bases,
    deterministic_delimited_key_range_plan, deterministic_key_range_plan,
};
use crate::run::{
    ComponentTree, RunStatistics, RunView, assemble_component_ranges, open_views, seal_run_root,
};
use crate::segment::{
    DocumentComponentWriter, DocumentRecord, DocumentState, PATH_CHANGES_TAG, PathComponentWriter,
};
use crate::{
    BlockDescriptor, DocumentRef, IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind,
    SealedRun,
};

use super::compaction_cache::FullTextPointCache;
use super::{
    FULL_TEXT_POSTINGS_TAG, TextComponentWriter, TextPostingRow, TextRowCursor, posting_key,
};

pub(super) async fn merge_full_text_parallel<D, S, E>(
    runs: &[D],
    output_level: u8,
    target_bytes: usize,
    sink: &mut S,
    parallelism: CompactionParallelism,
    progress: CompactionProgress,
    executor: E,
) -> Result<SealedRun, IndexError>
where
    D: IndexDirectoryRead + Clone + 'static,
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    if runs.is_empty() || output_level == 0 {
        return Err(IndexError::InvalidDefinition(
            "full-text compaction requires input runs and an L1+ output level".into(),
        ));
    }
    let views = open_views(runs, IndexKind::FullText).await?;
    let (path_tree, document_tree, statistics) = merge_common_components_parallel(
        runs,
        &views,
        output_level,
        target_bytes,
        sink,
        parallelism,
        progress.clone(),
        executor.clone(),
    )
    .await?;
    let text_tree = if statistics.live_document_count == 0 {
        None
    } else {
        merge_text_component_parallel(
            runs,
            &views,
            &path_tree.root,
            output_level,
            target_bytes,
            sink,
            parallelism,
            progress,
            executor,
        )
        .await?
    };
    let mut components = vec![path_tree];
    if let Some(tree) = document_tree {
        components.push(tree);
    }
    if let Some(tree) = text_tree {
        components.push(tree);
    }
    seal_run_root(IndexKind::FullText, output_level, statistics, components)
}

#[allow(clippy::too_many_arguments)]
async fn merge_common_components_parallel<D, S, E>(
    runs: &[D],
    views: &[RunView],
    output_level: u8,
    target_bytes: usize,
    sink: &mut S,
    parallelism: CompactionParallelism,
    progress: CompactionProgress,
    executor: E,
) -> Result<(ComponentTree, Option<ComponentTree>, RunStatistics), IndexError>
where
    D: IndexDirectoryRead + Clone + 'static,
    S: IndexBlockSink + Clone + 'static,
    E: CompactionExecutor,
{
    let roots = views
        .iter()
        .map(|view| view.component(PATH_CHANGES_TAG).cloned())
        .collect::<Result<Vec<_>, _>>()?;
    let plan = deterministic_key_range_plan(roots.iter().cloned(), parallelism.max_lanes());
    progress.record_range_limit(plan.range_limit)?;
    let ranges = plan.ranges;
    let runs = Arc::new(runs.to_vec());
    let roots = Arc::new(roots);
    let mut count_producers = Vec::<LaneResultProducer<u64>>::with_capacity(ranges.len());
    for range in ranges.iter().cloned() {
        let runs = runs.clone();
        let roots = roots.clone();
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        count_producers.push(Box::new(move || {
            Box::pin(count_live_range(
                runs,
                roots,
                range,
                lane_executor,
                lane_progress,
            ))
        }));
    }
    let live_counts = collect_ordered_lanes(&executor, count_producers, &progress).await?;
    let (ordinal_bases, total_live) = dense_ordinal_bases(&live_counts)?;

    let mut write_producers =
        Vec::<LaneResultProducer<CommonLaneOutput>>::with_capacity(ranges.len());
    for ((range, ordinal_base), expected_live) in
        ranges.into_iter().zip(ordinal_bases).zip(live_counts)
    {
        let runs = runs.clone();
        let roots = roots.clone();
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        let lane_sink = sink.clone();
        write_producers.push(Box::new(move || {
            Box::pin(build_common_range(
                runs,
                roots,
                range,
                ordinal_base,
                expected_live,
                output_level,
                target_bytes,
                lane_sink,
                lane_executor,
                lane_progress,
            ))
        }));
    }
    let lane_outputs = collect_ordered_lanes(&executor, write_producers, &progress).await?;

    let mut mutation_count = 0u64;
    let mut observed_live = 0u64;
    let mut minimum_version = u64::MAX;
    let mut maximum_version = 0u64;
    let mut path_trees = Vec::new();
    let mut document_trees = Vec::new();
    for lane in lane_outputs {
        mutation_count = mutation_count
            .checked_add(lane.mutation_count)
            .ok_or(IndexError::OffsetOverflow)?;
        observed_live = observed_live
            .checked_add(lane.live_document_count)
            .ok_or(IndexError::OffsetOverflow)?;
        if lane.mutation_count != 0 {
            minimum_version = minimum_version.min(lane.minimum_version);
            maximum_version = maximum_version.max(lane.maximum_version);
        }
        if let Some(tree) = lane.paths {
            path_trees.push(tree);
        }
        if let Some(tree) = lane.documents {
            document_trees.push(tree);
        }
    }
    if mutation_count == 0 {
        return Err(IndexError::InvalidDefinition(
            "full-text compaction produced no changes".into(),
        ));
    }
    if observed_live != total_live {
        return Err(IndexError::InvalidFormat(
            "full-text range count changed while compacting",
        ));
    }
    let paths =
        assemble_component_ranges(IndexKind::FullText, PATH_CHANGES_TAG, path_trees, sink).await?;
    let documents = if total_live == 0 {
        None
    } else {
        Some(
            assemble_component_ranges(
                IndexKind::FullText,
                crate::segment::DOCUMENTS_TAG,
                document_trees,
                sink,
            )
            .await?,
        )
    };
    Ok((
        paths,
        documents,
        RunStatistics {
            mutation_count,
            live_document_count: total_live,
            minimum_version,
            maximum_version,
        },
    ))
}

async fn count_live_range<D, E>(
    runs: Arc<Vec<D>>,
    roots: Arc<Vec<BlockDescriptor>>,
    range: KeyRange,
    executor: E,
    progress: CompactionProgress,
) -> Result<u64, IndexError>
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
    Ok(live)
}

struct CommonLaneOutput {
    paths: Option<ComponentTree>,
    documents: Option<ComponentTree>,
    mutation_count: u64,
    live_document_count: u64,
    minimum_version: u64,
    maximum_version: u64,
}

#[allow(clippy::too_many_arguments)]
async fn build_common_range<D, S, E>(
    runs: Arc<Vec<D>>,
    roots: Arc<Vec<BlockDescriptor>>,
    range: KeyRange,
    ordinal_base: u64,
    expected_live: u64,
    output_level: u8,
    target_bytes: usize,
    mut sink: S,
    executor: E,
    progress: CompactionProgress,
) -> Result<CommonLaneOutput, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink,
    E: CompactionExecutor,
{
    let mut winners = PathWinnerCursor::open(
        runs.as_slice(),
        roots.as_slice(),
        range,
        executor,
        progress.clone(),
    )
    .await?;
    let mut paths = PathComponentWriter::new(IndexKind::FullText, output_level, target_bytes);
    let mut documents = DocumentComponentWriter::with_ordinal_base(
        IndexKind::FullText,
        output_level,
        target_bytes,
        ordinal_base,
    );
    let mut mutation_count = 0u64;
    let mut live = 0u64;
    let mut minimum_version = u64::MAX;
    let mut maximum_version = 0u64;
    while let Some((_, mut winner)) = winners.next().await? {
        if winner.state == DocumentState::Live {
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
    if live != expected_live {
        return Err(IndexError::InvalidFormat(
            "full-text range count changed while compacting",
        ));
    }
    if mutation_count == 0 {
        Ok(CommonLaneOutput {
            paths: None,
            documents: None,
            mutation_count,
            live_document_count: 0,
            minimum_version,
            maximum_version,
        })
    } else {
        let paths = Some(paths.finish(&mut sink).await?);
        let documents = if live == 0 {
            None
        } else {
            Some(documents.finish(&mut sink).await?)
        };
        Ok(CommonLaneOutput {
            paths,
            documents,
            mutation_count,
            live_document_count: live,
            minimum_version,
            maximum_version,
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn merge_text_component_parallel<D, S, E>(
    runs: &[D],
    views: &[RunView],
    output_path_root: &BlockDescriptor,
    output_level: u8,
    target_bytes: usize,
    sink: &mut S,
    parallelism: CompactionParallelism,
    progress: CompactionProgress,
    executor: E,
) -> Result<Option<ComponentTree>, IndexError>
where
    D: IndexDirectoryRead + Clone + 'static,
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    let roots = views
        .iter()
        .map(|view| view.component_optional(FULL_TEXT_POSTINGS_TAG).cloned())
        .collect::<Vec<_>>();
    let present_roots = roots.iter().flatten().cloned().collect::<Vec<_>>();
    if present_roots.is_empty() {
        return Ok(None);
    }
    let plan = deterministic_delimited_key_range_plan(present_roots, 0, parallelism.max_lanes())?;
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
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        let lane_sink = sink.clone();
        let output_path_root = output_path_root.clone();
        producers.push(Box::new(move || {
            Box::pin(build_text_range(
                runs,
                views,
                roots,
                range,
                output_path_root,
                output_level,
                target_bytes,
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
            assemble_component_ranges(IndexKind::FullText, FULL_TEXT_POSTINGS_TAG, trees, sink)
                .await?,
        ))
    }
}

struct ResolvedTextRow {
    run_index: usize,
    document: DocumentRef,
    row: TextPostingRow,
}

async fn next_resolved_parallel<'a, D, E>(
    run_index: usize,
    cursor: &mut TextRowCursor<'a, D>,
    run: &'a D,
    view: &RunView,
    executor: &E,
    progress: &CompactionProgress,
    point_cache: &mut FullTextPointCache,
) -> Result<Option<ResolvedTextRow>, IndexError>
where
    D: IndexDirectoryRead,
    E: CompactionExecutor,
{
    let Some(row) = cursor.next_parallel(executor, progress).await? else {
        return Ok(None);
    };
    let document = point_cache
        .document(run, view, row.ordinal, executor, progress)
        .await?;
    Ok(Some(ResolvedTextRow {
        run_index,
        document,
        row,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn build_text_range<D, S, E>(
    runs: Arc<Vec<D>>,
    views: Arc<Vec<RunView>>,
    roots: Arc<Vec<Option<BlockDescriptor>>>,
    range: KeyRange,
    output_path_root: BlockDescriptor,
    output_level: u8,
    target_bytes: usize,
    mut sink: S,
    executor: E,
    progress: CompactionProgress,
) -> Result<Option<ComponentTree>, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink + IndexDirectoryRead,
    E: CompactionExecutor,
{
    let mut cursors = runs
        .iter()
        .zip(roots.iter())
        .map(|(run, root)| {
            root.clone()
                .map(|root| TextRowCursor::in_range(run, root, range.clone()))
        })
        .collect::<Vec<_>>();
    let mut current = Vec::with_capacity(cursors.len());
    let mut input_cache = FullTextPointCache::input();
    let mut output_cache = FullTextPointCache::staged_output();
    let mut writer = TextComponentWriter::new(
        IndexKind::FullText,
        FULL_TEXT_POSTINGS_TAG,
        output_level,
        target_bytes,
    );
    let mut wrote = false;
    for run_index in 0..cursors.len() {
        current.push(match &mut cursors[run_index] {
            Some(cursor) => {
                next_resolved_parallel(
                    run_index,
                    cursor,
                    &runs[run_index],
                    &views[run_index],
                    &executor,
                    &progress,
                    &mut input_cache,
                )
                .await?
            }
            None => None,
        });
    }
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
        current[selected] = next_resolved_parallel(
            selected,
            cursors[selected].as_mut().unwrap(),
            &runs[selected],
            &views[selected],
            &executor,
            &progress,
            &mut input_cache,
        )
        .await?;
        let Some((winner_index, winner)) = input_cache
            .latest_path(
                runs.as_slice(),
                views.as_slice(),
                &candidate.document.path,
                &executor,
                &progress,
            )
            .await?
        else {
            continue;
        };
        if winner_index == candidate.run_index
            && winner.state == DocumentState::Live
            && winner.document.version == candidate.document.version
        {
            let output = output_cache
                .path(
                    &sink,
                    &output_path_root,
                    &candidate.document.path,
                    &executor,
                    &progress,
                )
                .await?
                .ok_or(IndexError::InvalidFormat("missing compacted path"))?;
            let mut row = candidate.row;
            row.ordinal = output.document_ordinal.ok_or(IndexError::InvalidFormat(
                "missing compacted document ordinal",
            ))?;
            let _ = posting_key(&row)?;
            writer.push_row(row, &mut sink).await?;
            wrote = true;
        }
    }
    if wrote {
        Ok(Some(writer.finish(&mut sink).await?))
    } else {
        Ok(None)
    }
}
