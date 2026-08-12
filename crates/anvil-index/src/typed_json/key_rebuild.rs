//! Linear routed-key reconstruction from already-selected final typed rows.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::compaction::{
    CompactionExecutor, CompactionProgress, KeyRange, LaneResultProducer,
    MAX_COMPACTION_INPUT_RUNS, collect_ordered_lanes, deterministic_suffix_key_range_plan,
    validate_parallel_compaction_fan_in,
};
use crate::routed::{RoutedCursor, RoutedRow};
use crate::routed_sort::{
    DEFAULT_ROUTED_SORT_CHUNK_RESIDENT_BYTES, RoutedExternalSorter, merge_routed_range_once,
};
use crate::run::{ComponentTree, LeafCursor, assemble_component_ranges, discard_component_tree};
use crate::{IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind};

use super::{
    KEYS_TAG, identity::TypedRow, parallel_compaction::read_typed_block_parallel,
    postings::PostingComponentWriter, typed_primary,
};

#[allow(clippy::too_many_arguments)]
pub(super) async fn rebuild_keys_parallel<S, E>(
    kind: IndexKind,
    output_level: u8,
    target_block_bytes: usize,
    typed_ranges: Vec<ComponentTree>,
    sink: &mut S,
    max_lanes: usize,
    executor: E,
    progress: CompactionProgress,
) -> Result<Option<ComponentTree>, IndexError>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    if typed_ranges.is_empty() {
        return Ok(None);
    }
    let mut producers =
        Vec::<LaneResultProducer<Option<ComponentTree>>>::with_capacity(typed_ranges.len());
    for tree in typed_ranges {
        let lane_sink = sink.fork()?;
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        producers.push(Box::new(move || {
            Box::pin(rebuild_range(
                kind,
                output_level,
                target_block_bytes,
                tree,
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
    merge_posting_trees_parallel(
        kind,
        output_level,
        target_block_bytes,
        trees,
        sink,
        max_lanes,
        executor,
        progress,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn rebuild_range<S, E>(
    kind: IndexKind,
    output_level: u8,
    target_block_bytes: usize,
    typed: ComponentTree,
    sink: S,
    executor: E,
    progress: CompactionProgress,
) -> Result<Option<ComponentTree>, IndexError>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone,
    E: CompactionExecutor,
{
    let directory = sink.clone();
    let mut cursor = StagedTypedCursor::new(&directory, typed.root);
    let scratch = sink.fork_scratch()?;
    let mut sorter = RoutedExternalSorter::new(
        kind,
        KEYS_TAG,
        output_level,
        target_block_bytes,
        DEFAULT_ROUTED_SORT_CHUNK_RESIDENT_BYTES,
        scratch,
        executor.clone(),
        progress.clone(),
    )?;
    while let Some(TypedRow { ordinal, payload }) = cursor.next(&executor, &progress).await? {
        let mut position = 0u32;
        for (field, values) in &payload.fields {
            sorter
                .push(super::typed_exists_row(field, ordinal)?)
                .await?;
            for value in values {
                sorter
                    .push(RoutedRow::new(
                        typed_primary(field, value)?,
                        ordinal,
                        position,
                    )?)
                    .await?;
                position = position.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
            }
        }
    }
    sorter.finish().await
}

#[allow(clippy::too_many_arguments)]
async fn merge_posting_trees_parallel<S, E>(
    kind: IndexKind,
    output_level: u8,
    target_block_bytes: usize,
    trees: Vec<ComponentTree>,
    sink: &mut S,
    max_lanes: usize,
    executor: E,
    progress: CompactionProgress,
) -> Result<Option<ComponentTree>, IndexError>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    if trees.is_empty() {
        return Ok(None);
    }
    let plan = deterministic_suffix_key_range_plan(
        trees.iter().map(|tree| tree.root.clone()),
        std::mem::size_of::<u64>() + std::mem::size_of::<u32>(),
        max_lanes,
    )?;
    progress.record_range_limit(plan.range_limit)?;
    let directory = sink.clone();
    let trees = Arc::new(trees);
    let mut producers = Vec::<LaneResultProducer<Option<ComponentTree>>>::new();
    for range in plan.ranges {
        let trees = trees.clone();
        let lane_directory = directory.clone();
        let lane_sink = sink.fork()?;
        let lane_scratch = sink.fork_scratch()?;
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        producers.push(Box::new(move || {
            Box::pin(build_posting_range_bounded(
                kind,
                output_level,
                target_block_bytes,
                trees,
                range,
                lane_directory,
                lane_sink,
                lane_scratch,
                lane_executor,
                lane_progress,
            ))
        }));
    }
    let ranges = collect_ordered_lanes(&executor, producers, &progress)
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    for tree in trees.iter() {
        discard_component_tree(&directory, tree, sink).await?;
    }
    if ranges.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        assemble_component_ranges(kind, KEYS_TAG, ranges, sink).await?,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn build_posting_range_bounded<S, E>(
    kind: IndexKind,
    output_level: u8,
    target_block_bytes: usize,
    source_trees: Arc<Vec<ComponentTree>>,
    range: KeyRange,
    directory: S,
    mut sink: S,
    mut scratch: S,
    executor: E,
    progress: CompactionProgress,
) -> Result<Option<ComponentTree>, IndexError>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone,
    E: CompactionExecutor,
{
    let mut trees = source_trees.as_ref().clone();
    let mut trees_are_scratch = false;
    while trees.len() > MAX_COMPACTION_INPUT_RUNS {
        let mut reduced = Vec::with_capacity(trees.len().div_ceil(MAX_COMPACTION_INPUT_RUNS));
        for group in trees.chunks(MAX_COMPACTION_INPUT_RUNS) {
            if let Some(tree) = merge_routed_range_once(
                kind,
                KEYS_TAG,
                output_level,
                target_block_bytes,
                group.to_vec(),
                range.clone(),
                &directory,
                &mut scratch,
                &executor,
                &progress,
            )
            .await?
            {
                reduced.push(tree);
            }
        }
        if trees_are_scratch {
            for tree in &trees {
                discard_component_tree(&directory, tree, &mut scratch).await?;
            }
        }
        trees = reduced;
        trees_are_scratch = true;
        if trees.is_empty() {
            return Ok(None);
        }
    }
    let output = write_posting_range(
        kind,
        target_block_bytes,
        trees.clone(),
        range,
        &directory,
        &mut sink,
        &executor,
        &progress,
    )
    .await?;
    if trees_are_scratch {
        for tree in &trees {
            discard_component_tree(&directory, tree, &mut scratch).await?;
        }
    }
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
async fn write_posting_range<D, S, E>(
    kind: IndexKind,
    target_block_bytes: usize,
    trees: Vec<ComponentTree>,
    range: KeyRange,
    directory: &D,
    sink: &mut S,
    executor: &E,
    progress: &CompactionProgress,
) -> Result<Option<ComponentTree>, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink,
    E: CompactionExecutor,
{
    validate_parallel_compaction_fan_in(trees.len())?;
    let merged_multiple_inputs = trees.len() > 1;
    let mut cursors = trees
        .into_iter()
        .map(|tree| RoutedCursor::in_range(directory, tree.root, range.clone()))
        .collect::<Vec<_>>();
    let mut current = Vec::with_capacity(cursors.len());
    for cursor in &mut cursors {
        current.push(cursor.next_parallel(executor, progress).await?);
    }
    let mut writer = PostingComponentWriter::new(kind, target_block_bytes);
    loop {
        let Some(selected) = current
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.as_ref().map(|row| (index, row)))
            .min_by(|(_, left), (_, right)| left.compare(right))
            .map(|(index, _)| index)
        else {
            break;
        };
        let row = current[selected]
            .take()
            .expect("the selected typed posting cursor has one row");
        if current
            .iter()
            .flatten()
            .any(|candidate| candidate.compare(&row).is_eq())
        {
            return Err(IndexError::InvalidDefinition(
                "derived typed posting keys must be unique".into(),
            ));
        }
        writer.push(row, sink).await?;
        current[selected] = cursors[selected].next_parallel(executor, progress).await?;
    }
    if merged_multiple_inputs {
        progress.record_sort_merge_pass();
    }
    writer.finish(sink).await
}

struct StagedTypedCursor<'a, D> {
    directory: &'a D,
    leaves: LeafCursor<'a, D>,
    rows: VecDeque<TypedRow>,
}

impl<'a, D: IndexDirectoryRead> StagedTypedCursor<'a, D> {
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
    ) -> Result<Option<TypedRow>, IndexError> {
        loop {
            if let Some(row) = self.rows.pop_front() {
                return Ok(Some(row));
            }
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            progress.record_input(0, descriptor.encoded_bytes, 1);
            self.rows = read_typed_block_parallel(self.directory, &descriptor, executor, progress)
                .await?
                .into();
        }
    }
}
