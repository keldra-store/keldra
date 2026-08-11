//! Bounded temporary sorting for final full-text posting rows.

use std::cmp::Ordering;

use crate::compaction::{
    CompactionExecutor, CompactionProgress, MAX_COMPACTION_INPUT_RUNS,
    validate_parallel_compaction_fan_in,
};
use crate::run::{ComponentTree, LeafCursor, RoutingTreeBuilder};
use crate::{
    ComponentCodec, GeneratedBlock, IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind,
};

use super::{
    TextComponentWriter, TextPostingRow, decode_text_rows_fixed_unvalidated,
    encode_text_rows_fixed, posting_estimate, posting_key,
};

// Four-way leveling needs at most 32 slots before it exceeds every addressable
// byte count represented by the format's u64 lengths.
const MAX_SORT_LEVELS: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TextSortOrder {
    SourceOrdinal,
    FinalPosting,
}

/// A bounded external sorter. At most three immutable runs are retained at
/// each four-way level; the fourth is merged immediately into the next level.
pub(crate) struct TextExternalSorter<S, E> {
    kind: IndexKind,
    component_tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    max_chunk_resident_bytes: usize,
    chunk_resident_bytes: usize,
    rows: Vec<TextPostingRow>,
    levels: Vec<Vec<ComponentTree>>,
    order: TextSortOrder,
    sink: S,
    executor: E,
    progress: CompactionProgress,
}

impl<S, E> TextExternalSorter<S, E>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone,
    E: CompactionExecutor,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kind: IndexKind,
        component_tag: u8,
        output_level: u8,
        target_block_bytes: usize,
        max_chunk_resident_bytes: usize,
        order: TextSortOrder,
        sink: S,
        executor: E,
        progress: CompactionProgress,
    ) -> Result<Self, IndexError> {
        if output_level == 0 || max_chunk_resident_bytes == 0 {
            return Err(IndexError::InvalidDefinition(
                "external text sort requires an L1+ output and nonzero chunk".into(),
            ));
        }
        Ok(Self {
            kind,
            component_tag,
            output_level,
            target_block_bytes,
            max_chunk_resident_bytes,
            chunk_resident_bytes: 0,
            rows: Vec::new(),
            levels: (0..MAX_SORT_LEVELS).map(|_| Vec::new()).collect(),
            order,
            sink,
            executor,
            progress,
        })
    }

    pub(crate) async fn push(&mut self, row: TextPostingRow) -> Result<(), IndexError> {
        // Validate public posting constraints before comparator calls become
        // infallible in the CPU sort.
        let _ = posting_key(&row)?;
        // `posting_estimate` deliberately overcharges row metadata and owned
        // buffers, including spare Vec capacity in the sorter chunk.
        let resident_bytes = posting_estimate(&row);
        if resident_bytes > self.max_chunk_resident_bytes {
            return Err(IndexError::ResourceLimit {
                needed: resident_bytes,
                limit: self.max_chunk_resident_bytes,
            });
        }
        if !self.rows.is_empty()
            && self.chunk_resident_bytes.saturating_add(resident_bytes)
                > self.max_chunk_resident_bytes
        {
            self.spill_chunk().await?;
        }
        self.chunk_resident_bytes = self.chunk_resident_bytes.saturating_add(resident_bytes);
        self.rows.push(row);
        Ok(())
    }

    pub(crate) async fn finish(mut self) -> Result<Option<ComponentTree>, IndexError> {
        self.spill_chunk().await?;
        let mut carry = None;
        for level in 0..self.levels.len() {
            let mut trees = std::mem::take(&mut self.levels[level]);
            if let Some(tree) = carry.take() {
                trees.push(tree);
            }
            carry = match trees.len() {
                0 => None,
                1 => trees.pop(),
                _ => Some(
                    merge_text_component_trees(
                        self.kind,
                        self.component_tag,
                        self.output_level,
                        self.target_block_bytes,
                        self.order,
                        trees,
                        &mut self.sink,
                        &self.executor,
                        &self.progress,
                    )
                    .await?,
                ),
            };
        }
        Ok(carry)
    }

    async fn spill_chunk(&mut self) -> Result<(), IndexError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let mut rows = std::mem::take(&mut self.rows);
        self.chunk_resident_bytes = 0;
        let order = self.order;
        rows = self
            .executor
            .run_cpu(move || {
                rows.sort_unstable_by(|left, right| compare_rows(order, left, right));
                if rows
                    .windows(2)
                    .any(|pair| compare_rows(order, &pair[0], &pair[1]) != Ordering::Less)
                {
                    return Err(IndexError::InvalidFormat(
                        "duplicate external text sort key",
                    ));
                }
                Ok(rows)
            })
            .await?;
        let tree = write_sorted_rows(
            self.kind,
            self.component_tag,
            self.output_level,
            self.target_block_bytes,
            self.order,
            rows,
            &mut self.sink,
        )
        .await?;
        self.add_run(0, tree).await
    }

    async fn add_run(
        &mut self,
        mut level: usize,
        mut tree: ComponentTree,
    ) -> Result<(), IndexError> {
        loop {
            let Some(runs) = self.levels.get_mut(level) else {
                return Err(IndexError::OffsetOverflow);
            };
            runs.push(tree);
            if runs.len() < MAX_COMPACTION_INPUT_RUNS {
                return Ok(());
            }
            let trees = std::mem::take(runs);
            tree = merge_text_component_trees(
                self.kind,
                self.component_tag,
                self.output_level,
                self.target_block_bytes,
                self.order,
                trees,
                &mut self.sink,
                &self.executor,
                &self.progress,
            )
            .await?;
            level = level.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn write_sorted_rows<S: IndexBlockSink>(
    kind: IndexKind,
    component_tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    order: TextSortOrder,
    rows: Vec<TextPostingRow>,
    sink: &mut S,
) -> Result<ComponentTree, IndexError> {
    let mut writer =
        SpillTextWriter::new(kind, component_tag, output_level, target_block_bytes, order);
    for row in rows {
        writer.push(row, sink).await?;
    }
    writer.finish(sink).await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn merge_text_component_trees<S, E>(
    kind: IndexKind,
    component_tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    order: TextSortOrder,
    trees: Vec<ComponentTree>,
    sink: &mut S,
    executor: &E,
    progress: &CompactionProgress,
) -> Result<ComponentTree, IndexError>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone,
    E: CompactionExecutor,
{
    if trees.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "external text merge requires input trees".into(),
        ));
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
                merge_text_component_trees_once(
                    kind,
                    component_tag,
                    output_level,
                    target_block_bytes,
                    order,
                    group,
                    sink,
                    executor,
                    progress,
                )
                .await?,
            );
        }
        trees = merged;
    }
    merge_text_component_trees_once(
        kind,
        component_tag,
        output_level,
        target_block_bytes,
        order,
        trees,
        sink,
        executor,
        progress,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn merge_text_component_trees_once<S, E>(
    kind: IndexKind,
    component_tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    order: TextSortOrder,
    trees: Vec<ComponentTree>,
    sink: &mut S,
    executor: &E,
    progress: &CompactionProgress,
) -> Result<ComponentTree, IndexError>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone,
    E: CompactionExecutor,
{
    validate_parallel_compaction_fan_in(trees.len())?;
    let directory = sink.clone();
    let mut cursors = trees
        .into_iter()
        .map(|tree| SpillTextCursor::new(&directory, tree, order))
        .collect::<Vec<_>>();
    let mut current = Vec::with_capacity(cursors.len());
    for cursor in &mut cursors {
        current.push(cursor.next_parallel(executor, progress).await?);
    }
    let mut writer =
        SpillTextWriter::new(kind, component_tag, output_level, target_block_bytes, order);
    loop {
        let Some(selected) = current
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.as_ref().map(|row| (index, row)))
            .min_by(|left, right| compare_rows(order, left.1, right.1))
            .map(|(index, _)| index)
        else {
            break;
        };
        let row = current[selected].take().unwrap();
        current[selected] = cursors[selected].next_parallel(executor, progress).await?;
        writer.push(row, sink).await?;
    }
    writer.finish(sink).await
}

enum SpillTextWriter {
    Source(SourceTextComponentWriter),
    Final(TextComponentWriter),
}

impl SpillTextWriter {
    fn new(
        kind: IndexKind,
        component_tag: u8,
        output_level: u8,
        target_block_bytes: usize,
        order: TextSortOrder,
    ) -> Self {
        match order {
            TextSortOrder::SourceOrdinal => Self::Source(SourceTextComponentWriter::new(
                kind,
                component_tag,
                target_block_bytes,
            )),
            TextSortOrder::FinalPosting => Self::Final(TextComponentWriter::new(
                kind,
                component_tag,
                output_level,
                target_block_bytes,
            )),
        }
    }

    async fn push<S: IndexBlockSink>(
        &mut self,
        row: TextPostingRow,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        match self {
            Self::Source(writer) => writer.push(row, sink).await,
            Self::Final(writer) => writer.push_row(row, sink).await,
        }
    }

    async fn finish<S: IndexBlockSink>(self, sink: &mut S) -> Result<ComponentTree, IndexError> {
        match self {
            Self::Source(writer) => writer.finish(sink).await,
            Self::Final(writer) => writer.finish(sink).await,
        }
    }
}

pub(crate) struct SourceTextComponentWriter {
    kind: IndexKind,
    component_tag: u8,
    target_bytes: usize,
    rows: Vec<TextPostingRow>,
    estimated_bytes: usize,
    last_key: Option<Vec<u8>>,
    tree: RoutingTreeBuilder,
}

impl SourceTextComponentWriter {
    pub(crate) fn new(kind: IndexKind, component_tag: u8, target_bytes: usize) -> Self {
        Self {
            kind,
            component_tag,
            target_bytes: target_bytes.max(1024),
            rows: Vec::new(),
            estimated_bytes: 0,
            last_key: None,
            tree: RoutingTreeBuilder::new(kind, component_tag),
        }
    }

    pub(crate) async fn push<S: IndexBlockSink>(
        &mut self,
        row: TextPostingRow,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        let key = source_key(&row)?;
        if self.last_key.as_ref().is_some_and(|last| last >= &key) {
            return Err(IndexError::UnsortedRecords);
        }
        let row_bytes = posting_estimate(&row);
        if row_bytes > self.target_bytes {
            return Err(IndexError::ResourceLimit {
                needed: row_bytes,
                limit: self.target_bytes,
            });
        }
        if !self.rows.is_empty()
            && self.estimated_bytes.saturating_add(row_bytes) > self.target_bytes
        {
            self.flush(sink).await?;
        }
        self.last_key = Some(key);
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
        let minimum = source_key(rows.first().unwrap())?;
        let maximum = source_key(rows.last().unwrap())?;
        let body = encode_text_rows_fixed(&rows)?;
        let bytes = crate::codec::encode_component(
            self.kind,
            self.component_tag,
            ComponentCodec::GapPostings,
            body,
        )?;
        self.tree
            .emit_leaf(
                GeneratedBlock::new(
                    self.kind,
                    self.component_tag,
                    ComponentCodec::GapPostings,
                    0,
                    minimum,
                    maximum,
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

pub(crate) struct SpillTextCursor<'a, D> {
    directory: &'a D,
    leaves: LeafCursor<'a, D>,
    rows: std::vec::IntoIter<TextPostingRow>,
    order: TextSortOrder,
    minimum_source_ordinal: Option<u64>,
}

impl<'a, D: IndexDirectoryRead> SpillTextCursor<'a, D> {
    pub(crate) fn new(directory: &'a D, tree: ComponentTree, order: TextSortOrder) -> Self {
        Self {
            directory,
            leaves: LeafCursor::new(directory, tree.root),
            rows: Vec::new().into_iter(),
            order,
            minimum_source_ordinal: None,
        }
    }

    pub(crate) fn source_from_ordinal(directory: &'a D, tree: ComponentTree, ordinal: u64) -> Self {
        Self {
            directory,
            leaves: LeafCursor::in_range(
                directory,
                tree.root,
                crate::compaction::KeyRange {
                    lower: Some(ordinal.to_be_bytes().to_vec()),
                    upper: None,
                },
            ),
            rows: Vec::new().into_iter(),
            order: TextSortOrder::SourceOrdinal,
            minimum_source_ordinal: Some(ordinal),
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<TextPostingRow>, IndexError> {
        loop {
            if let Some(row) = self.rows.next() {
                if self
                    .minimum_source_ordinal
                    .is_none_or(|minimum| row.ordinal >= minimum)
                {
                    return Ok(Some(row));
                }
                continue;
            }
            self.rows = Vec::new().into_iter();
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            let rows = match self.order {
                TextSortOrder::SourceOrdinal => {
                    read_source_text_block(self.directory, &descriptor).await?
                }
                TextSortOrder::FinalPosting => {
                    super::read_text_block(self.directory, &descriptor).await?
                }
            };
            self.rows = rows.into_iter();
        }
    }

    pub(crate) async fn next_parallel<E: CompactionExecutor>(
        &mut self,
        executor: &E,
        progress: &CompactionProgress,
    ) -> Result<Option<TextPostingRow>, IndexError> {
        loop {
            if let Some(row) = self.rows.next() {
                if self
                    .minimum_source_ordinal
                    .is_none_or(|minimum| row.ordinal >= minimum)
                {
                    return Ok(Some(row));
                }
                continue;
            }
            self.rows = Vec::new().into_iter();
            let Some(descriptor) = self.leaves.next().await? else {
                return Ok(None);
            };
            let rows = match self.order {
                TextSortOrder::SourceOrdinal => {
                    read_source_text_block_parallel(self.directory, &descriptor, executor, progress)
                        .await?
                }
                TextSortOrder::FinalPosting => {
                    let rows = super::read_text_block_parallel(
                        self.directory,
                        &descriptor,
                        executor,
                        progress,
                    )
                    .await?;
                    progress.record_input(0, descriptor.encoded_bytes, 1);
                    rows
                }
            };
            self.rows = rows.into_iter();
        }
    }
}

async fn read_source_text_block<D: IndexDirectoryRead>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
) -> Result<Vec<TextPostingRow>, IndexError> {
    if descriptor.codec != ComponentCodec::GapPostings {
        return Err(IndexError::InvalidFormat("external text spill codec"));
    }
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let rows = decode_text_rows_fixed_unvalidated(block.body())?;
    if rows.first().map(source_key).transpose()? != Some(descriptor.minimum_key.clone())
        || rows.last().map(source_key).transpose()? != Some(descriptor.maximum_key.clone())
        || rows.len() as u64 != descriptor.element_count
        || rows
            .windows(2)
            .any(|pair| source_cmp(&pair[0], &pair[1]) != Ordering::Less)
    {
        return Err(IndexError::InvalidFormat("external text spill rows"));
    }
    Ok(rows)
}

async fn read_source_text_block_parallel<D, E>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
    executor: &E,
    progress: &CompactionProgress,
) -> Result<Vec<TextPostingRow>, IndexError>
where
    D: IndexDirectoryRead,
    E: CompactionExecutor,
{
    if descriptor.codec != ComponentCodec::GapPostings {
        return Err(IndexError::InvalidFormat("external text spill codec"));
    }
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let encoded_bytes = descriptor.encoded_bytes;
    let descriptor = descriptor.clone();
    let rows = executor
        .run_cpu(move || {
            let rows = decode_text_rows_fixed_unvalidated(block.body())?;
            if rows.first().map(source_key).transpose()? != Some(descriptor.minimum_key.clone())
                || rows.last().map(source_key).transpose()? != Some(descriptor.maximum_key.clone())
                || rows.len() as u64 != descriptor.element_count
                || rows
                    .windows(2)
                    .any(|pair| source_cmp(&pair[0], &pair[1]) != Ordering::Less)
            {
                return Err(IndexError::InvalidFormat("external text spill rows"));
            }
            Ok(rows)
        })
        .await?;
    progress.record_input(rows.len() as u64, encoded_bytes, 1);
    Ok(rows)
}

fn compare_rows(order: TextSortOrder, left: &TextPostingRow, right: &TextPostingRow) -> Ordering {
    match order {
        TextSortOrder::SourceOrdinal => source_cmp(left, right),
        TextSortOrder::FinalPosting => final_cmp(left, right),
    }
}

fn source_cmp(left: &TextPostingRow, right: &TextPostingRow) -> Ordering {
    left.ordinal
        .cmp(&right.ordinal)
        .then_with(|| left.term.cmp(&right.term))
        .then_with(|| left.field.cmp(&right.field))
        .then_with(|| left.part.cmp(&right.part))
}

fn final_cmp(left: &TextPostingRow, right: &TextPostingRow) -> Ordering {
    left.term
        .cmp(&right.term)
        .then_with(|| left.ordinal.cmp(&right.ordinal))
        .then_with(|| left.field.cmp(&right.field))
        .then_with(|| left.part.cmp(&right.part))
}

fn source_key(row: &TextPostingRow) -> Result<Vec<u8>, IndexError> {
    let _ = posting_key(row)?;
    let mut key = Vec::with_capacity(row.term.len().saturating_add(row.field.len()) + 14);
    key.extend_from_slice(&row.ordinal.to_be_bytes());
    key.extend_from_slice(row.term.as_bytes());
    key.push(0);
    key.extend_from_slice(row.field.as_bytes());
    key.push(0);
    key.extend_from_slice(&row.part.to_be_bytes());
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::test_support::TokioExecutor;
    use crate::io::tests::MemoryBlockSink;

    #[tokio::test]
    async fn tiny_chunks_force_bounded_multilevel_spills_in_both_orders() {
        let rows = (0..33u64)
            .rev()
            .map(|ordinal| TextPostingRow {
                term: format!("term-{:02}", (ordinal * 7) % 33),
                ordinal,
                field: "body".into(),
                field_length: 1,
                part: 0,
                positions: vec![u32::try_from(ordinal).unwrap()],
            })
            .collect::<Vec<_>>();
        let one_row_chunk = rows.iter().map(posting_estimate).max().unwrap();

        for order in [TextSortOrder::SourceOrdinal, TextSortOrder::FinalPosting] {
            let sink = MemoryBlockSink::default();
            let mut sorter = TextExternalSorter::new(
                IndexKind::FullText,
                super::super::FULL_TEXT_POSTINGS_TAG,
                1,
                1024,
                one_row_chunk,
                order,
                sink.clone(),
                TokioExecutor::default(),
                CompactionProgress::default(),
            )
            .unwrap();
            for row in rows.iter().cloned() {
                sorter.push(row).await.unwrap();
            }
            let tree = sorter.finish().await.unwrap().unwrap();
            let mut cursor = SpillTextCursor::new(&sink, tree, order);
            let mut sorted = Vec::new();
            while let Some(row) = cursor.next().await.unwrap() {
                sorted.push(row);
            }

            assert_eq!(sorted.len(), rows.len());
            assert!(
                sorted
                    .windows(2)
                    .all(|pair| compare_rows(order, &pair[0], &pair[1]).is_lt())
            );
            let mut expected = rows.clone();
            expected.sort_unstable_by(|left, right| compare_rows(order, left, right));
            assert_eq!(sorted, expected);
        }
    }

    #[tokio::test]
    async fn more_than_four_lane_trees_reduce_in_bounded_merge_rounds() {
        let mut sink = MemoryBlockSink::default();
        let executor = TokioExecutor::default();
        let progress = CompactionProgress::default();
        let mut trees = Vec::new();
        let mut expected = Vec::new();
        for index in (0..9u64).rev() {
            let row = TextPostingRow {
                term: format!("term-{index:02}"),
                ordinal: index,
                field: "body".into(),
                field_length: 1,
                part: 0,
                positions: vec![0],
            };
            expected.push(row.clone());
            trees.push(
                write_sorted_rows(
                    IndexKind::FullText,
                    super::super::FULL_TEXT_POSTINGS_TAG,
                    1,
                    1024,
                    TextSortOrder::FinalPosting,
                    vec![row],
                    &mut sink,
                )
                .await
                .unwrap(),
            );
        }

        let tree = merge_text_component_trees(
            IndexKind::FullText,
            super::super::FULL_TEXT_POSTINGS_TAG,
            1,
            1024,
            TextSortOrder::FinalPosting,
            trees,
            &mut sink,
            &executor,
            &progress,
        )
        .await
        .unwrap();
        let mut cursor = SpillTextCursor::new(&sink, tree, TextSortOrder::FinalPosting);
        let mut actual = Vec::new();
        while let Some(row) = cursor.next().await.unwrap() {
            actual.push(row);
        }
        expected
            .sort_unstable_by(|left, right| compare_rows(TextSortOrder::FinalPosting, left, right));
        assert_eq!(actual, expected);
    }
}
