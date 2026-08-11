//! Deterministic bounded range compaction for full-text path and posting data.

use std::sync::Arc;

use crate::compaction::{
    CompactionExecutor, CompactionParallelism, CompactionProgress, KeyRange, LaneResultProducer,
    PathWinnerCursor, collect_ordered_lanes, dense_ordinal_bases, deterministic_key_range_plan,
};
use crate::routed_sort::MAX_EXTERNAL_SORT_CHUNK_RESIDENT_BYTES;
use crate::run::{
    ComponentTree, RunStatistics, RunView, assemble_component_ranges, open_views, seal_run_root,
};
use crate::segment::{
    DocumentComponentWriter, DocumentRecord, DocumentState, PATH_CHANGES_TAG, PathComponentWriter,
    PathRunCursor,
};
use crate::{
    BlockDescriptor, IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind, SealedRun,
};

use super::{
    FULL_TEXT_POSTINGS_TAG, TextRowCursor,
    text_sort::{
        SourceTextComponentWriter, SpillTextCursor, TextExternalSorter, TextSortOrder,
        merge_text_component_trees,
    },
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
    crate::compaction::validate_parallel_compaction_fan_in(runs.len())?;
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
        rebuild_selected_text_component_parallel(
            runs,
            &views,
            &path_tree.root,
            IndexKind::FullText,
            FULL_TEXT_POSTINGS_TAG,
            statistics.live_document_count,
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
pub(crate) async fn rebuild_selected_text_component_parallel<D, S, E>(
    runs: &[D],
    views: &[RunView],
    output_path_root: &BlockDescriptor,
    kind: IndexKind,
    component_tag: u8,
    expected_live: u64,
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
    crate::compaction::validate_parallel_compaction_fan_in(runs.len())?;
    if runs.len() != views.len() {
        return Err(IndexError::InvalidDefinition(
            "run readers and descriptors must have equal length".into(),
        ));
    }
    let mut source_trees = Vec::with_capacity(runs.len());
    for (run, view) in runs.iter().zip(views) {
        let Some(root) = view.component_optional(component_tag) else {
            source_trees.push(None);
            continue;
        };
        let document_count = view.component(crate::segment::DOCUMENTS_TAG)?.element_count;
        let mut sorter = TextExternalSorter::new(
            kind,
            component_tag,
            output_level,
            target_bytes,
            MAX_EXTERNAL_SORT_CHUNK_RESIDENT_BYTES,
            TextSortOrder::SourceOrdinal,
            sink.clone(),
            executor.clone(),
            progress.clone(),
        )?;
        let mut cursor = TextRowCursor::new(run, root.clone());
        while let Some(row) = cursor.next_parallel(&executor, &progress).await? {
            if row.ordinal >= document_count {
                return Err(IndexError::InvalidFormat(
                    "text posting ordinal outside run",
                ));
            }
            sorter.push(row).await?;
        }
        drop(cursor);
        source_trees.push(sorter.finish().await?);
    }
    let path_roots = views
        .iter()
        .map(|view| view.component(PATH_CHANGES_TAG).cloned())
        .collect::<Result<Vec<_>, _>>()?;
    let plan = deterministic_key_range_plan(path_roots.iter().cloned(), parallelism.max_lanes());
    progress.record_range_limit(plan.range_limit)?;
    let runs = Arc::new(runs.to_vec());
    let views = Arc::new(views.to_vec());
    let path_roots = Arc::new(path_roots);
    let source_trees = Arc::new(source_trees);
    let mut producers =
        Vec::<LaneResultProducer<SelectedTextRange>>::with_capacity(plan.ranges.len());
    for range in plan.ranges {
        let runs = runs.clone();
        let views = views.clone();
        let path_roots = path_roots.clone();
        let source_trees = source_trees.clone();
        let output_path_root = output_path_root.clone();
        let lane_sink = sink.clone();
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        producers.push(Box::new(move || {
            Box::pin(rebuild_selected_text_range(
                runs,
                views,
                path_roots,
                source_trees,
                output_path_root,
                range,
                kind,
                component_tag,
                output_level,
                target_bytes,
                lane_sink,
                lane_executor,
                lane_progress,
            ))
        }));
    }
    let ranges = collect_ordered_lanes(&executor, producers, &progress).await?;
    let live = ranges.iter().try_fold(0u64, |total, range| {
        total
            .checked_add(range.live)
            .ok_or(IndexError::OffsetOverflow)
    })?;
    if live != expected_live {
        return Err(IndexError::InvalidFormat(
            "text winner replay changed live count",
        ));
    }
    let trees = ranges
        .into_iter()
        .filter_map(|range| range.tree)
        .collect::<Vec<_>>();
    if trees.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        merge_text_component_trees(
            kind,
            component_tag,
            output_level,
            target_bytes,
            TextSortOrder::FinalPosting,
            trees,
            sink,
            &executor,
            &progress,
        )
        .await?,
    ))
}

struct SelectedTextRange {
    tree: Option<ComponentTree>,
    live: u64,
}

#[allow(clippy::too_many_arguments)]
async fn rebuild_selected_text_range<D, S, E>(
    runs: Arc<Vec<D>>,
    views: Arc<Vec<RunView>>,
    path_roots: Arc<Vec<BlockDescriptor>>,
    source_trees: Arc<Vec<Option<ComponentTree>>>,
    output_path_root: BlockDescriptor,
    range: KeyRange,
    kind: IndexKind,
    component_tag: u8,
    output_level: u8,
    target_bytes: usize,
    mut sink: S,
    executor: E,
    progress: CompactionProgress,
) -> Result<SelectedTextRange, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink + IndexDirectoryRead + Clone,
    E: CompactionExecutor,
{
    let mut winners = PathWinnerCursor::open(
        runs.as_slice(),
        path_roots.as_slice(),
        range.clone(),
        executor.clone(),
        progress.clone(),
    )
    .await?;
    let output_directory = sink.clone();
    let mut output_paths = PathRunCursor::in_range(&output_directory, output_path_root, range);
    let source_directory = sink.clone();
    let mut source_cursors = (0..runs.len()).map(|_| None).collect::<Vec<_>>();
    let mut current = (0..runs.len()).map(|_| None).collect::<Vec<_>>();
    let mut selected = SourceTextComponentWriter::new(kind, component_tag, target_bytes);
    let mut wrote = false;
    let mut live = 0u64;

    loop {
        let winner = winners.next().await?;
        let output = output_paths.next_parallel(&executor, &progress).await?;
        let (winner_run, winner, output) = match (winner, output) {
            (Some((winner_run, winner)), Some(output)) => (winner_run, winner, output),
            (None, None) => break,
            _ => {
                return Err(IndexError::InvalidFormat(
                    "compacted text path range changed",
                ));
            }
        };
        if winner.document != output.document || winner.state != output.state {
            return Err(IndexError::InvalidFormat("compacted text winner mismatch"));
        }
        if winner.state != DocumentState::Live {
            if output.document_ordinal.is_some() {
                return Err(IndexError::InvalidFormat(
                    "removed compacted text path has an ordinal",
                ));
            }
            continue;
        }
        let old_ordinal = winner.document_ordinal.ok_or(IndexError::InvalidFormat(
            "live text path has no source ordinal",
        ))?;
        let output_ordinal = output.document_ordinal.ok_or(IndexError::InvalidFormat(
            "live text path has no output ordinal",
        ))?;
        let document_count = views[winner_run]
            .component(crate::segment::DOCUMENTS_TAG)?
            .element_count;
        if old_ordinal >= document_count {
            return Err(IndexError::InvalidFormat("text path ordinal outside run"));
        }
        live = live.checked_add(1).ok_or(IndexError::OffsetOverflow)?;

        if source_cursors[winner_run].is_none()
            && let Some(tree) = source_trees[winner_run].clone()
        {
            let mut cursor =
                SpillTextCursor::source_from_ordinal(&source_directory, tree, old_ordinal);
            current[winner_run] = cursor.next_parallel(&executor, &progress).await?;
            source_cursors[winner_run] = Some(cursor);
        }
        if let Some(cursor) = source_cursors[winner_run].as_mut() {
            while current[winner_run]
                .as_ref()
                .is_some_and(|row| row.ordinal < old_ordinal)
            {
                current[winner_run] = cursor.next_parallel(&executor, &progress).await?;
            }
            while current[winner_run]
                .as_ref()
                .is_some_and(|row| row.ordinal == old_ordinal)
            {
                let mut row = current[winner_run].take().unwrap();
                current[winner_run] = cursor.next_parallel(&executor, &progress).await?;
                row.ordinal = output_ordinal;
                selected.push(row, &mut sink).await?;
                wrote = true;
            }
        }
    }

    drop(output_paths);
    drop(winners);
    drop(current);
    drop(source_cursors);
    drop(source_directory);
    drop(output_directory);
    if !wrote {
        return Ok(SelectedTextRange { tree: None, live });
    }
    let selected_tree = selected.finish(&mut sink).await?;
    let selected_directory = sink.clone();
    let mut cursor = SpillTextCursor::new(
        &selected_directory,
        selected_tree,
        TextSortOrder::SourceOrdinal,
    );
    let mut sorter = TextExternalSorter::new(
        kind,
        component_tag,
        output_level,
        target_bytes,
        MAX_EXTERNAL_SORT_CHUNK_RESIDENT_BYTES,
        TextSortOrder::FinalPosting,
        sink,
        executor.clone(),
        progress.clone(),
    )?;
    while let Some(row) = cursor.next_parallel(&executor, &progress).await? {
        sorter.push(row).await?;
    }
    drop(cursor);
    Ok(SelectedTextRange {
        tree: sorter.finish().await?,
        live,
    })
}
