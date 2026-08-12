//! Bounded temporary sorting for final full-text posting rows.

use std::cmp::Ordering;

use crate::compaction::{
    CompactionExecutor, CompactionProgress, KeyRange, LaneResultProducer,
    MAX_COMPACTION_INPUT_RUNS, collect_ordered_lanes, deterministic_delimited_key_range_plan,
    validate_parallel_compaction_fan_in,
};
use crate::run::{
    ComponentTree, LeafCursor, RoutingTreeBuilder, assemble_component_ranges,
    discard_component_tree,
};
use crate::{
    ComponentCodec, GeneratedBlock, IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind,
};

use super::{
    TextComponentWriter, TextPostingRow, decode_text_rows_fixed_unvalidated,
    encode_text_rows_fixed, posting_estimate, posting_key, posting_positions_per_row,
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
                _ => {
                    let consumed = trees.clone();
                    let merged = merge_text_component_trees(
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
                    self.discard_trees(&consumed).await?;
                    Some(merged)
                }
            };
        }
        Ok(carry)
    }

    /// Spill the current byte-bounded source quantum before its owner yields.
    /// Only routing descriptors remain resident between scheduler turns.
    pub(crate) async fn checkpoint(&mut self) -> Result<(), IndexError> {
        self.spill_chunk().await
    }

    async fn spill_chunk(&mut self) -> Result<(), IndexError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let chunk_workspace_bytes = self.chunk_resident_bytes;
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
        self.add_run(0, tree).await?;
        self.progress.record_sort_chunk(chunk_workspace_bytes)
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
            let consumed = trees.clone();
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
            self.discard_trees(&consumed).await?;
            level = level.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        }
    }

    async fn discard_trees(&mut self, trees: &[ComponentTree]) -> Result<(), IndexError> {
        let directory = self.sink.clone();
        for tree in trees {
            discard_component_tree(&directory, tree, &mut self.sink).await?;
        }
        Ok(())
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

/// Merge final-posting scratch trees directly through deterministic term
/// stripes. Every stripe performs bounded fan-in reduction locally, then
/// canonicalizes occurrences once into its authoritative output lane.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn merge_final_text_component_trees_parallel<S, E>(
    kind: IndexKind,
    component_tag: u8,
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
    let plan = deterministic_delimited_key_range_plan(
        trees.iter().map(|tree| tree.root.clone()),
        0,
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
            Box::pin(merge_final_text_range_bounded(
                kind,
                component_tag,
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
        assemble_component_ranges(kind, component_tag, ranges, sink).await?,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn merge_final_text_range_bounded<S, E>(
    kind: IndexKind,
    component_tag: u8,
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
            if let Some(tree) = merge_final_text_range_once(
                kind,
                component_tag,
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
    let output = write_canonical_text_range(
        kind,
        component_tag,
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
async fn merge_final_text_range_once<D, S, E>(
    kind: IndexKind,
    component_tag: u8,
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
        .map(|tree| SpillTextCursor::final_in_range(directory, tree, range.clone()))
        .collect::<Vec<_>>();
    let mut current = Vec::with_capacity(cursors.len());
    for cursor in &mut cursors {
        current.push(cursor.next_parallel(executor, progress).await?);
    }
    let mut writer = SpillTextWriter::new(
        kind,
        component_tag,
        output_level,
        target_block_bytes,
        TextSortOrder::FinalPosting,
    );
    let mut wrote = false;
    loop {
        let Some(selected) = current
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.as_ref().map(|row| (index, row)))
            .min_by(|left, right| compare_rows(TextSortOrder::FinalPosting, left.1, right.1))
            .map(|(index, _)| index)
        else {
            break;
        };
        let row = current[selected]
            .take()
            .expect("the selected text cursor has one row");
        writer.push(row, sink).await?;
        wrote = true;
        current[selected] = cursors[selected].next_parallel(executor, progress).await?;
    }
    if merged_multiple_inputs {
        progress.record_sort_merge_pass();
    }
    if wrote {
        Ok(Some(writer.finish(sink).await?))
    } else {
        Ok(None)
    }
}

#[allow(clippy::too_many_arguments)]
async fn write_canonical_text_range<D, S, E>(
    kind: IndexKind,
    component_tag: u8,
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
        .map(|tree| SpillTextCursor::final_in_range(directory, tree, range.clone()))
        .collect::<Vec<_>>();
    let mut current = Vec::with_capacity(cursors.len());
    for cursor in &mut cursors {
        current.push(cursor.next_parallel(executor, progress).await?);
    }
    let mut writer =
        CanonicalPostingWriter::new(kind, component_tag, output_level, target_block_bytes);
    let mut wrote = false;
    loop {
        let Some(selected) = current
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.as_ref().map(|row| (index, row)))
            .min_by(|left, right| compare_rows(TextSortOrder::FinalPosting, left.1, right.1))
            .map(|(index, _)| index)
        else {
            break;
        };
        let row = current[selected]
            .take()
            .expect("the selected canonical text cursor has one row");
        writer.push_occurrence(row, sink).await?;
        wrote = true;
        current[selected] = cursors[selected].next_parallel(executor, progress).await?;
    }
    if merged_multiple_inputs {
        progress.record_sort_merge_pass();
    }
    if wrote {
        Ok(Some(writer.finish(sink).await?))
    } else {
        Ok(None)
    }
}

/// Rewrite one sorted disposable text tree into an authoritative output lane.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn rewrite_text_component_tree<D, S, E>(
    kind: IndexKind,
    component_tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    order: TextSortOrder,
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
    let mut cursor = SpillTextCursor::new(directory, tree, order);
    let mut writer =
        SpillTextWriter::new(kind, component_tag, output_level, target_block_bytes, order);
    while let Some(row) = cursor.next_parallel(executor, progress).await? {
        writer.push(row, sink).await?;
    }
    writer.finish(sink).await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn rewrite_text_component_tree_parallel<D, S, E>(
    kind: IndexKind,
    component_tag: u8,
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
    let plan = deterministic_delimited_key_range_plan([tree.root.clone()], 0, max_lanes)?;
    progress.record_range_limit(plan.range_limit)?;
    let mut producers = Vec::<LaneResultProducer<Option<ComponentTree>>>::new();
    for range in plan.ranges {
        let tree = tree.clone();
        let directory = directory.clone();
        let lane_sink = sink.fork()?;
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        producers.push(Box::new(move || {
            Box::pin(rewrite_text_range(
                kind,
                component_tag,
                output_level,
                target_block_bytes,
                tree,
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
    let output = assemble_component_ranges(kind, component_tag, &trees, sink).await?;
    discard_component_tree(&directory, &tree, sink).await?;
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
async fn rewrite_text_range<D, S, E>(
    kind: IndexKind,
    component_tag: u8,
    output_level: u8,
    target_block_bytes: usize,
    tree: ComponentTree,
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
    let mut cursor = SpillTextCursor::final_in_range(&directory, tree, range);
    let mut writer =
        CanonicalPostingWriter::new(kind, component_tag, output_level, target_block_bytes);
    let mut wrote = false;
    while let Some(row) = cursor.next_parallel(&executor, &progress).await? {
        writer.push_occurrence(row, &mut sink).await?;
        wrote = true;
    }
    if wrote {
        Ok(Some(writer.finish(&mut sink).await?))
    } else {
        Ok(None)
    }
}

struct PendingPosting {
    term: String,
    ordinal: u64,
    field: String,
    field_length: u32,
    next_part: u32,
    maximum_positions: usize,
    last_position: Option<u32>,
    positions: Vec<u32>,
}

impl PendingPosting {
    fn matches(&self, row: &TextPostingRow) -> bool {
        self.term == row.term && self.ordinal == row.ordinal && self.field == row.field
    }
}

/// Coalesces occurrence-sort rows into the canonical bounded posting parts
/// used by authoritative components. At most one output part is resident.
struct CanonicalPostingWriter {
    target_block_bytes: usize,
    writer: TextComponentWriter,
    pending: Option<PendingPosting>,
}

impl CanonicalPostingWriter {
    fn new(
        kind: IndexKind,
        component_tag: u8,
        output_level: u8,
        target_block_bytes: usize,
    ) -> Self {
        // `TextComponentWriter` applies the same floor. Canonical part sizing
        // must use the writer's effective block size rather than rejecting a
        // deliberately tiny test/configured target that the writer can safely
        // raise to its format minimum.
        let target_block_bytes = target_block_bytes.max(1024);
        Self {
            target_block_bytes,
            writer: TextComponentWriter::new(kind, component_tag, output_level, target_block_bytes),
            pending: None,
        }
    }

    async fn push_occurrence<S: IndexBlockSink>(
        &mut self,
        row: TextPostingRow,
        sink: &mut S,
    ) -> Result<(), IndexError> {
        if row.positions.is_empty() || row.positions.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(IndexError::InvalidFormat("external text posting positions"));
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| !pending.matches(&row))
        {
            self.finish_pending(sink).await?;
        }
        if self.pending.is_none() {
            self.pending = Some(PendingPosting {
                maximum_positions: posting_positions_per_row(
                    &row.term,
                    &row.field,
                    self.target_block_bytes,
                )?,
                term: row.term.clone(),
                ordinal: row.ordinal,
                field: row.field.clone(),
                field_length: row.field_length,
                next_part: 0,
                last_position: None,
                positions: Vec::new(),
            });
        }
        let field_length = row.field_length;
        for position in row.positions {
            let flush = {
                let pending = self.pending.as_mut().expect("posting group was opened");
                if pending.field_length != field_length
                    || pending
                        .last_position
                        .is_some_and(|previous| previous >= position)
                {
                    return Err(IndexError::InvalidFormat("external text posting order"));
                }
                pending.last_position = Some(position);
                pending.positions.push(position);
                pending.positions.len() == pending.maximum_positions
            };
            if flush {
                self.flush_part(sink).await?;
            }
        }
        Ok(())
    }

    async fn flush_part<S: IndexBlockSink>(&mut self, sink: &mut S) -> Result<(), IndexError> {
        let mut pending = self.pending.take().expect("posting group is present");
        if pending.positions.is_empty() {
            self.pending = Some(pending);
            return Ok(());
        }
        let row = TextPostingRow {
            term: pending.term.clone(),
            ordinal: pending.ordinal,
            field: pending.field.clone(),
            field_length: pending.field_length,
            part: pending.next_part,
            positions: std::mem::take(&mut pending.positions),
        };
        pending.next_part = pending
            .next_part
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        self.writer.push_row(row, sink).await?;
        self.pending = Some(pending);
        Ok(())
    }

    async fn finish_pending<S: IndexBlockSink>(&mut self, sink: &mut S) -> Result<(), IndexError> {
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| !pending.positions.is_empty())
        {
            self.flush_part(sink).await?;
        }
        self.pending = None;
        Ok(())
    }

    async fn finish<S: IndexBlockSink>(
        mut self,
        sink: &mut S,
    ) -> Result<ComponentTree, IndexError> {
        self.finish_pending(sink).await?;
        self.writer.finish(sink).await
    }
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
    let merged_multiple_inputs = trees.len() > 1;
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
    let output = writer.finish(sink).await?;
    if merged_multiple_inputs {
        progress.record_sort_merge_pass();
    }
    Ok(output)
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
    range: Option<KeyRange>,
}

impl<'a, D: IndexDirectoryRead> SpillTextCursor<'a, D> {
    pub(crate) fn new(directory: &'a D, tree: ComponentTree, order: TextSortOrder) -> Self {
        Self {
            directory,
            leaves: LeafCursor::new(directory, tree.root),
            rows: Vec::new().into_iter(),
            order,
            minimum_source_ordinal: None,
            range: None,
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
            range: None,
        }
    }

    pub(crate) fn final_in_range(directory: &'a D, tree: ComponentTree, range: KeyRange) -> Self {
        Self {
            directory,
            leaves: LeafCursor::in_range(directory, tree.root, range.clone()),
            rows: Vec::new().into_iter(),
            order: TextSortOrder::FinalPosting,
            minimum_source_ordinal: None,
            range: Some(range),
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<TextPostingRow>, IndexError> {
        loop {
            if let Some(row) = self.rows.next() {
                if self
                    .minimum_source_ordinal
                    .is_none_or(|minimum| row.ordinal >= minimum)
                    && self
                        .range
                        .as_ref()
                        .is_none_or(|range| range.contains(row.term.as_bytes()))
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
                    && self
                        .range
                        .as_ref()
                        .is_none_or(|range| range.contains(row.term.as_bytes()))
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
            let progress = CompactionProgress::default();
            let mut sorter = TextExternalSorter::new(
                IndexKind::FullText,
                super::super::FULL_TEXT_POSTINGS_TAG,
                1,
                1024,
                one_row_chunk,
                order,
                sink.clone(),
                TokioExecutor::default(),
                progress.clone(),
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
            let progress = progress.snapshot();
            assert!(progress.sort_chunks > 1);
            assert!(progress.sort_merge_passes > 0);
            assert!(progress.sort_peak_workspace_bytes > 0);
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

    #[tokio::test]
    async fn final_multi_tree_merge_is_term_striped() {
        let mut sink = MemoryBlockSink::default();
        let mut trees = Vec::new();
        for tree_index in 0..4_u64 {
            let rows = (0..64_u64)
                .map(|index| TextPostingRow {
                    term: format!("term-{index:03}"),
                    ordinal: tree_index * 64 + index,
                    field: "body".into(),
                    field_length: 1,
                    part: 0,
                    positions: vec![0],
                })
                .collect::<Vec<_>>();
            trees.push(
                write_sorted_rows(
                    IndexKind::FullText,
                    super::super::FULL_TEXT_POSTINGS_TAG,
                    1,
                    1024,
                    TextSortOrder::FinalPosting,
                    rows,
                    &mut sink,
                )
                .await
                .unwrap(),
            );
        }
        let progress = CompactionProgress::default();
        let tree = merge_final_text_component_trees_parallel(
            IndexKind::FullText,
            super::super::FULL_TEXT_POSTINGS_TAG,
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
        let mut cursor = SpillTextCursor::new(&sink, tree, TextSortOrder::FinalPosting);
        let mut previous = None;
        let mut count = 0;
        while let Some(row) = cursor.next().await.unwrap() {
            assert!(
                previous
                    .as_ref()
                    .is_none_or(|previous: &TextPostingRow| { final_cmp(previous, &row).is_lt() })
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
