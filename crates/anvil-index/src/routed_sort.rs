//! Bounded external sorting for routed rows whose final document ordinal is known.

use crate::compaction::{
    CompactionExecutor, CompactionProgress, KeyRange, LaneResultProducer,
    MAX_COMPACTION_INPUT_RUNS,
    MAX_EXTERNAL_SORT_CHUNK_RESIDENT_BYTES as COMPACTION_EXTERNAL_SORT_CHUNK_BYTES,
    collect_ordered_lanes, deterministic_suffix_key_range_plan,
    validate_parallel_compaction_fan_in,
};
use crate::routed::{
    ROUTED_ROW_RESIDENT_OVERHEAD_BYTES, RoutedComponentWriter, RoutedCursor, RoutedRow,
};
use crate::run::{ComponentTree, assemble_component_ranges, discard_component_tree};
use crate::{IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind};

/// One bounded in-memory sort chunk. Spill components and their later merges use
/// ordinary staged index blocks, so corpus growth never grows this allocation.
pub(crate) const MAX_EXTERNAL_SORT_CHUNK_RESIDENT_BYTES: usize =
    COMPACTION_EXTERNAL_SORT_CHUNK_BYTES;
pub(crate) const DEFAULT_ROUTED_SORT_CHUNK_RESIDENT_BYTES: usize =
    MAX_EXTERNAL_SORT_CHUNK_RESIDENT_BYTES;

/// External sorter for already-final routed rows. Each level retains at most
/// four immutable component trees; a fifth tree merges the four oldest into the
/// next level while leaving the newest available for subsequent input.
pub(crate) struct RoutedExternalSorter<S, E> {
    kind: IndexKind,
    tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    max_chunk_resident_bytes: usize,
    chunk_resident_bytes: usize,
    chunk: Vec<RoutedRow>,
    levels: Vec<Vec<ComponentTree>>,
    sink: S,
    executor: E,
    progress: CompactionProgress,
    #[cfg(test)]
    maximum_observed_fan_in: usize,
}

impl<S, E> RoutedExternalSorter<S, E>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone,
    E: CompactionExecutor,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kind: IndexKind,
        tag: u8,
        output_level: u8,
        target_block_bytes: usize,
        max_chunk_resident_bytes: usize,
        sink: S,
        executor: E,
        progress: CompactionProgress,
    ) -> Result<Self, IndexError> {
        if output_level == 0 || max_chunk_resident_bytes == 0 {
            return Err(IndexError::InvalidDefinition(
                "routed external sort requires an L1+ output and nonzero chunk".into(),
            ));
        }
        Ok(Self {
            kind,
            tag,
            output_level,
            target_block_bytes,
            max_chunk_resident_bytes,
            chunk_resident_bytes: 0,
            chunk: Vec::new(),
            levels: Vec::new(),
            sink,
            executor,
            progress,
            #[cfg(test)]
            maximum_observed_fan_in: 0,
        })
    }

    pub(crate) async fn push(&mut self, row: RoutedRow) -> Result<(), IndexError> {
        let row_bytes = row
            .primary
            .len()
            .checked_add(ROUTED_ROW_RESIDENT_OVERHEAD_BYTES)
            .ok_or(IndexError::OffsetOverflow)?;
        if row_bytes > self.max_chunk_resident_bytes {
            return Err(IndexError::ResourceLimit {
                needed: row_bytes,
                limit: self.max_chunk_resident_bytes,
            });
        }
        if !self.chunk.is_empty()
            && self
                .chunk_resident_bytes
                .checked_add(row_bytes)
                .ok_or(IndexError::OffsetOverflow)?
                > self.max_chunk_resident_bytes
        {
            self.flush_chunk().await?;
        }
        self.chunk_resident_bytes = self
            .chunk_resident_bytes
            .checked_add(row_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        self.chunk.push(row);
        Ok(())
    }

    pub(crate) async fn finish(mut self) -> Result<Option<ComponentTree>, IndexError> {
        self.flush_chunk().await?;
        loop {
            let total = self.levels.iter().map(Vec::len).sum::<usize>();
            if total == 0 {
                return Ok(None);
            }
            if total == 1 {
                return Ok(self.levels.iter_mut().find_map(Vec::pop));
            }
            let level = self
                .levels
                .iter()
                .position(|runs| !runs.is_empty())
                .expect("a nonzero tree count has one occupied level");
            let inputs = if self.levels[level].len() == 1 {
                vec![self.levels[level].pop().unwrap()]
            } else {
                self.levels[level].drain(..).collect::<Vec<_>>()
            };
            let tree = if inputs.len() == 1 {
                inputs.into_iter().next().unwrap()
            } else {
                self.merge(inputs).await?
            };
            self.insert_tree(level + 1, tree).await?;
        }
    }

    /// End one fair source quantum without retaining its row payloads in
    /// memory. The immutable spill remains disposable scratch and the sorter
    /// can accept the next path range after the caller yields.
    pub(crate) async fn checkpoint(&mut self) -> Result<(), IndexError> {
        self.flush_chunk().await
    }

    async fn flush_chunk(&mut self) -> Result<(), IndexError> {
        if self.chunk.is_empty() {
            return Ok(());
        }
        let chunk_workspace_bytes = self.chunk_resident_bytes;
        let mut rows = std::mem::take(&mut self.chunk);
        self.chunk_resident_bytes = 0;
        rows = self
            .executor
            .run_cpu(move || {
                rows.sort_by(RoutedRow::compare);
                if rows
                    .windows(2)
                    .any(|pair| pair[0].compare(&pair[1]).is_ge())
                {
                    return Err(IndexError::InvalidDefinition(
                        "derived routed keys must be unique".into(),
                    ));
                }
                Ok(rows)
            })
            .await?;
        let mut writer = RoutedComponentWriter::new(
            self.kind,
            self.tag,
            self.output_level,
            self.target_block_bytes,
        );
        for row in rows {
            writer.push(row, &mut self.sink).await?;
        }
        let tree = writer
            .finish(&mut self.sink)
            .await?
            .ok_or(IndexError::InvalidFormat("nonempty routed sort chunk"))?;
        self.insert_tree(0, tree).await?;
        self.progress.record_sort_chunk(chunk_workspace_bytes)
    }

    async fn insert_tree(
        &mut self,
        mut level: usize,
        mut tree: ComponentTree,
    ) -> Result<(), IndexError> {
        loop {
            if self.levels.len() <= level {
                self.levels.resize_with(level + 1, Vec::new);
            }
            self.levels[level].push(tree);
            if self.levels[level].len() <= MAX_COMPACTION_INPUT_RUNS {
                return Ok(());
            }
            let inputs = self.levels[level]
                .drain(..MAX_COMPACTION_INPUT_RUNS)
                .collect::<Vec<_>>();
            tree = self.merge(inputs).await?;
            level += 1;
        }
    }

    async fn merge(&mut self, inputs: Vec<ComponentTree>) -> Result<ComponentTree, IndexError> {
        #[cfg(test)]
        {
            self.maximum_observed_fan_in = self.maximum_observed_fan_in.max(inputs.len());
        }
        let consumed = inputs.clone();
        let output = merge_routed_component_trees_once(
            self.kind,
            self.tag,
            self.output_level,
            self.target_block_bytes,
            inputs,
            &mut self.sink,
            &self.executor,
            &self.progress,
        )
        .await?
        .ok_or(IndexError::InvalidFormat("nonempty routed spill merge"))?;
        let directory = self.sink.clone();
        for tree in consumed {
            discard_component_tree(&directory, &tree, &mut self.sink).await?;
        }
        Ok(output)
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn merge_routed_component_trees<S, E>(
    kind: IndexKind,
    tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    trees: Vec<ComponentTree>,
    sink: &mut S,
    executor: &E,
    progress: &CompactionProgress,
) -> Result<Option<ComponentTree>, IndexError>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone,
    E: CompactionExecutor,
{
    if trees.is_empty() {
        return Ok(None);
    }
    let mut trees = trees;
    while trees.len() > MAX_COMPACTION_INPUT_RUNS {
        let mut inputs = trees.into_iter();
        let mut merged = Vec::new();
        while let Some(first) = inputs.next() {
            let mut group = vec![first];
            for _ in 1..MAX_COMPACTION_INPUT_RUNS {
                let Some(tree) = inputs.next() else {
                    break;
                };
                group.push(tree);
            }
            merged.push(
                merge_routed_component_trees_once(
                    kind,
                    tag,
                    output_level,
                    target_block_bytes,
                    group,
                    sink,
                    executor,
                    progress,
                )
                .await?
                .expect("a nonempty routed merge produces one tree"),
            );
        }
        trees = merged;
    }
    merge_routed_component_trees_once(
        kind,
        tag,
        output_level,
        target_block_bytes,
        trees,
        sink,
        executor,
        progress,
    )
    .await
}

/// Merge independently sorted routed trees directly through deterministic
/// primary-key stripes. Each stripe performs any required bounded fan-in
/// reduction in disposable scratch, then writes its final rows once into an
/// ordinary authoritative lane.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn merge_routed_component_trees_parallel<S, E>(
    kind: IndexKind,
    tag: u8,
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
    let trees = std::sync::Arc::new(trees);
    let mut producers = Vec::<LaneResultProducer<Option<ComponentTree>>>::new();
    for range in plan.ranges {
        let trees = trees.clone();
        let lane_directory = directory.clone();
        let lane_sink = sink.fork()?;
        let lane_scratch = sink.fork_scratch()?;
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        producers.push(Box::new(move || {
            Box::pin(merge_routed_range_bounded(
                kind,
                tag,
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
        assemble_component_ranges(kind, tag, ranges, sink).await?,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn merge_routed_range_bounded<S, E>(
    kind: IndexKind,
    tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    source_trees: std::sync::Arc<Vec<ComponentTree>>,
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
                tag,
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
    let output = merge_routed_range_once(
        kind,
        tag,
        output_level,
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
pub(crate) async fn merge_routed_range_once<D, S, E>(
    kind: IndexKind,
    tag: u8,
    output_level: u8,
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
    let mut writer = RoutedComponentWriter::new(kind, tag, output_level, target_block_bytes);
    let mut wrote = false;
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
            .expect("the selected routed cursor has one row");
        if current
            .iter()
            .flatten()
            .any(|candidate| candidate.compare(&row).is_eq())
        {
            return Err(IndexError::InvalidDefinition(
                "derived routed keys must be unique".into(),
            ));
        }
        writer.push(row, sink).await?;
        wrote = true;
        current[selected] = cursors[selected].next_parallel(executor, progress).await?;
    }
    if merged_multiple_inputs {
        progress.record_sort_merge_pass();
    }
    if wrote {
        writer.finish(sink).await
    } else {
        Ok(None)
    }
}

/// Rewrite one sorted disposable component into the authoritative output
/// lane. External-sort spill and merge blocks can therefore remain local
/// scratch while the generation retains only this final component tree.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn rewrite_routed_component_tree<D, S, E>(
    kind: IndexKind,
    tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    tree: ComponentTree,
    directory: &D,
    sink: &mut S,
    executor: &E,
    progress: &CompactionProgress,
) -> Result<ComponentTree, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink,
    E: CompactionExecutor,
{
    let mut cursor = RoutedCursor::new(directory, tree.root, None);
    let mut writer = RoutedComponentWriter::new(kind, tag, output_level, target_block_bytes);
    while let Some(row) = cursor.next_parallel(executor, progress).await? {
        writer.push(row, sink).await?;
    }
    writer
        .finish(sink)
        .await?
        .ok_or(IndexError::InvalidFormat("nonempty routed scratch tree"))
}

/// Rewrite one disposable routed tree through deterministic primary-key
/// stripes. Each stripe writes an independent ordinary lane; only their small
/// ordered roots return to the coordinator.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn rewrite_routed_component_tree_parallel<D, S, E>(
    kind: IndexKind,
    tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    tree: ComponentTree,
    directory: D,
    sink: &mut S,
    max_lanes: usize,
    executor: E,
    progress: CompactionProgress,
) -> Result<ComponentTree, IndexError>
where
    D: IndexDirectoryRead + Clone + Send + Sync + 'static,
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    let plan = deterministic_suffix_key_range_plan(
        [tree.root.clone()],
        std::mem::size_of::<u64>() + std::mem::size_of::<u32>(),
        max_lanes,
    )?;
    progress.record_range_limit(plan.range_limit)?;
    let mut producers = Vec::<LaneResultProducer<Option<ComponentTree>>>::new();
    for range in plan.ranges {
        let root = tree.root.clone();
        let directory = directory.clone();
        let lane_sink = sink.fork()?;
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        producers.push(Box::new(move || {
            Box::pin(rewrite_routed_range(
                kind,
                tag,
                output_level,
                target_block_bytes,
                root,
                range,
                directory,
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
    let output = assemble_component_ranges(kind, tag, &trees, sink).await?;
    discard_component_tree(&directory, &tree, sink).await?;
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
async fn rewrite_routed_range<D, S, E>(
    kind: IndexKind,
    tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    root: crate::BlockDescriptor,
    range: KeyRange,
    directory: D,
    mut sink: S,
    executor: E,
    progress: CompactionProgress,
) -> Result<Option<ComponentTree>, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink,
    E: CompactionExecutor,
{
    let mut cursor = RoutedCursor::in_range(&directory, root, range);
    let mut writer = RoutedComponentWriter::new(kind, tag, output_level, target_block_bytes);
    let mut wrote = false;
    while let Some(row) = cursor.next_parallel(&executor, &progress).await? {
        writer.push(row, &mut sink).await?;
        wrote = true;
    }
    if wrote {
        writer.finish(&mut sink).await
    } else {
        Ok(None)
    }
}

#[allow(clippy::too_many_arguments)]
async fn merge_routed_component_trees_once<S, E>(
    kind: IndexKind,
    tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    trees: Vec<ComponentTree>,
    sink: &mut S,
    executor: &E,
    progress: &CompactionProgress,
) -> Result<Option<ComponentTree>, IndexError>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone,
    E: CompactionExecutor,
{
    validate_parallel_compaction_fan_in(trees.len())?;
    if trees.len() == 1 {
        return Ok(trees.into_iter().next());
    }
    let directory = sink.clone();
    let mut cursors = trees
        .iter()
        .map(|tree| RoutedCursor::new(&directory, tree.root.clone(), None))
        .collect::<Vec<_>>();
    let mut current = Vec::with_capacity(cursors.len());
    for cursor in &mut cursors {
        current.push(cursor.next_parallel(executor, progress).await?);
    }
    let mut writer = RoutedComponentWriter::new(kind, tag, output_level, target_block_bytes);
    let output = loop {
        let Some(selected) = current
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.as_ref().map(|row| (index, row)))
            .min_by(|(_, left), (_, right)| left.compare(right))
            .map(|(index, _)| index)
        else {
            break writer.finish(sink).await?;
        };
        let row = current[selected]
            .take()
            .expect("the selected routed cursor has one row");
        if current
            .iter()
            .flatten()
            .any(|candidate| candidate.compare(&row).is_eq())
        {
            return Err(IndexError::InvalidDefinition(
                "derived routed keys must be unique".into(),
            ));
        }
        writer.push(row, sink).await?;
        current[selected] = cursors[selected].next_parallel(executor, progress).await?;
    };
    progress.record_sort_merge_pass();
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::test_support::TokioExecutor;
    use crate::io::tests::MemoryBlockSink;

    #[tokio::test]
    async fn spills_are_byte_bounded_sorted_and_merged_with_at_most_four_inputs() {
        let sink = MemoryBlockSink::default();
        let progress = CompactionProgress::default();
        let mut sorter = RoutedExternalSorter::new(
            IndexKind::TypedJson,
            20,
            1,
            96,
            ROUTED_ROW_RESIDENT_OVERHEAD_BYTES + 4,
            sink.clone(),
            TokioExecutor::default(),
            progress.clone(),
        )
        .unwrap();
        for ordinal in (0..37_u64).rev() {
            sorter
                .push(RoutedRow::new(format!("k{ordinal:03}").into_bytes(), ordinal, 0).unwrap())
                .await
                .unwrap();
            assert!(sorter.chunk_resident_bytes <= sorter.max_chunk_resident_bytes);
            assert!(
                sorter
                    .levels
                    .iter()
                    .all(|runs| runs.len() <= MAX_COMPACTION_INPUT_RUNS)
            );
        }
        assert_eq!(sorter.maximum_observed_fan_in, MAX_COMPACTION_INPUT_RUNS);
        let tree = sorter.finish().await.unwrap().unwrap();
        let directory = sink.directory();
        let mut cursor = RoutedCursor::new(&directory, tree.root, None);
        let mut ordinals = Vec::new();
        while let Some(row) = cursor.next().await.unwrap() {
            ordinals.push(row.ordinal);
        }
        assert_eq!(ordinals, (0..37_u64).collect::<Vec<_>>());
        let progress = progress.snapshot();
        assert!(progress.input_records > 0);
        assert!(progress.input_bytes > 0);
        assert!(progress.input_blocks > 0);
        assert!(progress.sort_chunks > 1);
        assert!(progress.sort_merge_passes > 0);
        assert!(progress.sort_peak_workspace_bytes > 0);
    }

    #[tokio::test]
    async fn one_row_larger_than_the_chunk_fails_before_allocation_growth() {
        let sink = MemoryBlockSink::default();
        let mut sorter = RoutedExternalSorter::new(
            IndexKind::TypedJson,
            20,
            1,
            96,
            ROUTED_ROW_RESIDENT_OVERHEAD_BYTES,
            sink,
            TokioExecutor::default(),
            CompactionProgress::default(),
        )
        .unwrap();
        assert!(matches!(
            sorter
                .push(RoutedRow::new(b"too-large".to_vec(), 1, 0).unwrap())
                .await,
            Err(IndexError::ResourceLimit { .. })
        ));
    }

    #[tokio::test]
    async fn coordinator_reduces_more_than_four_lane_trees_with_bounded_fan_in() {
        let mut sink = MemoryBlockSink::default();
        let mut trees = Vec::new();
        for ordinal in (0..9_u64).rev() {
            let mut writer = RoutedComponentWriter::new(IndexKind::TypedJson, 20, 1, 96);
            writer
                .push(
                    RoutedRow::new(format!("k{ordinal:03}").into_bytes(), ordinal, 0).unwrap(),
                    &mut sink,
                )
                .await
                .unwrap();
            trees.push(writer.finish(&mut sink).await.unwrap().unwrap());
        }

        let tree = merge_routed_component_trees(
            IndexKind::TypedJson,
            20,
            1,
            96,
            trees,
            &mut sink,
            &TokioExecutor::default(),
            &CompactionProgress::default(),
        )
        .await
        .unwrap()
        .unwrap();
        let directory = sink.directory();
        let mut cursor = RoutedCursor::new(&directory, tree.root, None);
        let mut ordinals = Vec::new();
        while let Some(row) = cursor.next().await.unwrap() {
            ordinals.push(row.ordinal);
        }
        assert_eq!(ordinals, (0..9_u64).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn final_rewrite_uses_multiple_ranges_when_admitted() {
        let mut sink = MemoryBlockSink::default();
        let executor = TokioExecutor::default();
        let mut sorter = RoutedExternalSorter::new(
            IndexKind::GitSource,
            61,
            1,
            1024,
            1024,
            sink.clone(),
            executor.clone(),
            CompactionProgress::default(),
        )
        .unwrap();
        for value in 0..64_u64 {
            sorter
                .push(RoutedRow::new(vec![value as u8, b'k'], value, 0).unwrap())
                .await
                .unwrap();
        }
        let tree = sorter.finish().await.unwrap().unwrap();
        let progress = CompactionProgress::default();
        let rewritten = rewrite_routed_component_tree_parallel(
            IndexKind::GitSource,
            61,
            1,
            1024,
            tree,
            sink.clone(),
            &mut sink,
            4,
            executor,
            progress.clone(),
        )
        .await
        .unwrap();
        assert_eq!(rewritten.root.element_count, 64);
        assert!(progress.snapshot().effective_lanes > 1);
        assert!(progress.snapshot().ranges_completed > 1);
    }

    #[tokio::test]
    async fn final_multi_tree_merge_is_striped_without_a_whole_tree_rewrite() {
        let mut sink = MemoryBlockSink::default();
        let mut trees = Vec::new();
        for tree_index in 0..4_u64 {
            let mut writer = RoutedComponentWriter::new(IndexKind::GitSource, 61, 1, 1024);
            for key in 0..64_u64 {
                writer
                    .push(
                        RoutedRow::new(
                            vec![u8::try_from(key).unwrap(), b'k'],
                            tree_index * 64 + key,
                            0,
                        )
                        .unwrap(),
                        &mut sink,
                    )
                    .await
                    .unwrap();
            }
            trees.push(writer.finish(&mut sink).await.unwrap().unwrap());
        }
        let progress = CompactionProgress::default();
        let tree = merge_routed_component_trees_parallel(
            IndexKind::GitSource,
            61,
            1,
            1024,
            trees,
            &mut sink,
            4,
            TokioExecutor::default(),
            progress.clone(),
        )
        .await
        .unwrap()
        .unwrap();
        let directory = sink.directory();
        let mut cursor = RoutedCursor::new(&directory, tree.root, None);
        let mut previous = None;
        let mut count = 0;
        while let Some(row) = cursor.next().await.unwrap() {
            assert!(
                previous
                    .as_ref()
                    .is_none_or(|previous: &RoutedRow| previous.compare(&row).is_lt())
            );
            previous = Some(row);
            count += 1;
        }
        assert_eq!(count, 256);
        let snapshot = progress.snapshot();
        assert!(snapshot.effective_lanes > 1);
        assert_eq!(snapshot.ranges_completed, snapshot.ranges_total);
    }
}
