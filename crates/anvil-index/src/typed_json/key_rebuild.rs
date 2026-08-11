//! Linear routed-key reconstruction from already-selected final typed rows.

use std::collections::VecDeque;

use crate::compaction::{
    CompactionExecutor, CompactionProgress, LaneResultProducer, collect_ordered_lanes,
};
use crate::routed::RoutedRow;
use crate::routed_sort::{
    DEFAULT_ROUTED_SORT_CHUNK_RESIDENT_BYTES, RoutedExternalSorter, merge_routed_component_trees,
};
use crate::run::{ComponentTree, LeafCursor};
use crate::{IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind};

use super::{KEYS_TAG, TypedRow, parallel_compaction::read_typed_block_parallel, typed_primary};

#[allow(clippy::too_many_arguments)]
pub(super) async fn rebuild_keys_parallel<S, E>(
    kind: IndexKind,
    output_level: u8,
    target_block_bytes: usize,
    typed_ranges: Vec<ComponentTree>,
    sink: &mut S,
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
        let lane_sink = sink.clone();
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
    merge_routed_component_trees(
        kind,
        KEYS_TAG,
        output_level,
        target_block_bytes,
        trees,
        sink,
        &executor,
        &progress,
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
    let mut sorter = RoutedExternalSorter::new(
        kind,
        KEYS_TAG,
        output_level,
        target_block_bytes,
        DEFAULT_ROUTED_SORT_CHUNK_RESIDENT_BYTES,
        sink,
        executor.clone(),
        progress.clone(),
    )?;
    while let Some(TypedRow { ordinal, payload }) = cursor.next(&executor, &progress).await? {
        let mut position = 0u32;
        for (field, values) in &payload.fields {
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
