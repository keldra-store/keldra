//! Deterministic bounded range compaction shared by typed JSON and metadata.

use std::sync::Arc;

use crate::compaction::{
    CompactionExecutor, CompactionParallelism, CompactionProgress, KeyRange, LaneResultProducer,
    PathWinnerCursor, collect_ordered_lanes, deterministic_key_range_plan,
};
use crate::run::{ComponentTree, RunStatistics, RunView, assemble_component_ranges, seal_run_root};
use crate::segment::{
    DOCUMENTS_TAG, DocumentComponentWriter, DocumentRecord, DocumentState, PATH_CHANGES_TAG,
    PathComponentWriter,
};
use crate::{IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind, SealedRun};

use super::{
    CompactionPointCache,
    identity::{
        TypedComponentWriter, TypedRow, decode_typed_rows, range_local_ordinal, range_ordinal_base,
        validate_typed_rows,
    },
    key_rebuild, open_views,
};

#[allow(clippy::too_many_arguments)]
pub(super) async fn merge_typed_parallel<D, S, E>(
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
    E: CompactionExecutor,
{
    if runs.is_empty() || output_level == 0 {
        return Err(IndexError::InvalidDefinition(
            "typed compaction requires input runs and an L1+ output level".into(),
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
    let mut producers =
        Vec::<LaneResultProducer<Option<TypedRangeOutput>>>::with_capacity(ranges.len());
    for (range_id, range) in ranges.into_iter().enumerate() {
        let ordinal_base = range_ordinal_base(range_id)?;
        let runs = runs.clone();
        let views = views.clone();
        let roots = roots.clone();
        let lane_sink = sink.fork()?;
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        producers.push(Box::new(move || {
            Box::pin(build_typed_range(
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
    let outputs = outputs.into_iter().flatten().collect::<Vec<_>>();
    let statistics = aggregate_typed_statistics(&outputs)?;
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
    let typed = if statistics.live_document_count == 0 {
        None
    } else {
        Some(
            assemble_component_ranges(
                kind,
                super::ROWS_TAG,
                outputs.iter().filter_map(|output| output.typed.as_ref()),
                sink,
            )
            .await?,
        )
    };
    let keys = if statistics.live_document_count == 0 {
        None
    } else {
        key_rebuild::rebuild_keys_parallel(
            kind,
            output_level,
            target_block_bytes,
            outputs
                .iter()
                .filter_map(|output| output.typed.clone())
                .collect(),
            sink,
            parallelism.max_lanes(),
            executor,
            progress,
        )
        .await?
    };
    let mut components = vec![path_tree];
    if statistics.live_document_count > 0 {
        components.push(documents.ok_or(IndexError::InvalidFormat(
            "missing compacted typed documents",
        ))?);
        components.push(typed.ok_or(IndexError::InvalidFormat("missing compacted typed rows"))?);
        if let Some(keys) = keys {
            components.push(keys);
        }
    }
    seal_run_root(kind, output_level, statistics, components)
}

struct TypedRangeOutput {
    paths: ComponentTree,
    documents: Option<ComponentTree>,
    typed: Option<ComponentTree>,
    statistics: RunStatistics,
}

#[allow(clippy::too_many_arguments)]
async fn build_typed_range<D, S, E>(
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
) -> Result<Option<TypedRangeOutput>, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink,
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
    let mut point_cache = CompactionPointCache::default();
    let mut paths = PathComponentWriter::new(kind, output_level, target_block_bytes);
    let mut documents = DocumentComponentWriter::with_ordinal_base(
        kind,
        output_level,
        target_block_bytes,
        ordinal_base,
    );
    let mut typed = TypedComponentWriter::new(kind, output_level, target_block_bytes);
    let mut mutation_count = 0u64;
    let mut live = 0u64;
    let mut minimum_version = u64::MAX;
    let mut maximum_version = 0u64;
    while let Some((winner_run, mut winner)) = winners.next().await? {
        if winner.state == DocumentState::Live {
            let old_ordinal = winner
                .document_ordinal
                .ok_or(IndexError::InvalidFormat("live typed row has no ordinal"))?;
            let source_document = point_cache
                .document_parallel(
                    &runs[winner_run],
                    &views[winner_run],
                    old_ordinal,
                    &executor,
                    &progress,
                )
                .await?;
            if source_document != winner.document {
                return Err(IndexError::InvalidFormat("typed document mismatch"));
            }
            let payload = point_cache
                .typed_parallel(
                    &runs[winner_run],
                    &views[winner_run],
                    old_ordinal,
                    &executor,
                    &progress,
                )
                .await?
                .payload;
            let ordinal = range_local_ordinal(ordinal_base, live)?;
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
            typed.push(TypedRow { ordinal, payload }, &mut sink).await?;
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
    let (documents, typed) = if live == 0 {
        (None, None)
    } else {
        (
            Some(documents.finish(&mut sink).await?),
            Some(typed.finish(&mut sink).await?),
        )
    };
    Ok(Some(TypedRangeOutput {
        paths,
        documents,
        typed,
        statistics: RunStatistics {
            mutation_count,
            live_document_count: live,
            minimum_version,
            maximum_version,
        },
    }))
}

fn aggregate_typed_statistics(outputs: &[TypedRangeOutput]) -> Result<RunStatistics, IndexError> {
    if outputs.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "typed compaction produced no changes".into(),
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

pub(super) async fn read_typed_block_parallel<D, E>(
    directory: &D,
    descriptor: &crate::BlockDescriptor,
    executor: &E,
    progress: &CompactionProgress,
) -> Result<Vec<TypedRow>, IndexError>
where
    D: IndexDirectoryRead,
    E: CompactionExecutor,
{
    let block = crate::run::read_leaf(directory, descriptor).await?;
    let descriptor = descriptor.clone();
    let rows = executor
        .run_cpu(move || {
            validate_typed_rows(
                decode_typed_rows(block.body(), descriptor.codec)?,
                &descriptor,
            )
        })
        .await?;
    progress.record_input(rows.len() as u64, 0, 0);
    Ok(rows)
}
