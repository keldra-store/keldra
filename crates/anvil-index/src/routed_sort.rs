//! Bounded external sorting for routed rows whose final document ordinal is known.

use crate::compaction::{
    CompactionExecutor, CompactionProgress, MAX_COMPACTION_INPUT_RUNS,
    MAX_EXTERNAL_SORT_CHUNK_RESIDENT_BYTES as COMPACTION_EXTERNAL_SORT_CHUNK_BYTES,
    validate_parallel_compaction_fan_in,
};
use crate::routed::{
    ROUTED_ROW_RESIDENT_OVERHEAD_BYTES, RoutedComponentWriter, RoutedCursor, RoutedRow,
};
use crate::run::ComponentTree;
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

    async fn flush_chunk(&mut self) -> Result<(), IndexError> {
        if self.chunk.is_empty() {
            return Ok(());
        }
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
        self.insert_tree(0, tree).await
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
        merge_routed_component_trees_once(
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
        .ok_or(IndexError::InvalidFormat("nonempty routed spill merge"))
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
    loop {
        let Some(selected) = current
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.as_ref().map(|row| (index, row)))
            .min_by(|(_, left), (_, right)| left.compare(right))
            .map(|(index, _)| index)
        else {
            return writer.finish(sink).await;
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
    }
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
}
