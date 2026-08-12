//! Ordered path runs.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::compaction::{
    CompactionExecutor, CompactionParallelism, CompactionProgress, KeyRange, LaneResultProducer,
    PathWinnerCursor, collect_ordered_lanes, deterministic_key_range_plan,
};
use crate::run::{
    ComponentTree, LeafCursor, RunStatistics, assemble_component_ranges, open_views, seal_run_root,
};
use crate::segment::{
    DEFAULT_COMPONENT_BLOCK_BYTES, DocumentState, LatestLiveProbe, MutationBuffer,
    PATH_CHANGES_TAG, PathChange, PathComponentWriter, PathRunCursor, read_path_block,
};
use crate::{
    DocumentRef, IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind, IndexMutation,
    MAX_INDEX_ROUTING_KEY_BYTES, SealedRun, SegmentBuildOptions, SegmentPush,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathDocument {
    pub document: DocumentRef,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathQuery<'a> {
    pub prefix: &'a str,
    pub after_path: Option<&'a str>,
    pub limit: usize,
}

pub struct PathSegmentBuilder {
    buffer: MutationBuffer<PathDocument>,
}

impl PathSegmentBuilder {
    pub fn new(options: SegmentBuildOptions) -> Result<Self, IndexError> {
        Ok(Self {
            buffer: MutationBuffer::new(options)?,
        })
    }

    pub fn estimate_mutation(mutation: &IndexMutation<PathDocument>) -> usize {
        match mutation {
            IndexMutation::Upsert(document) => document.document.path.len(),
            IndexMutation::Remove(document) => document.path.len(),
        }
    }

    pub fn try_push(
        &mut self,
        mutation: IndexMutation<PathDocument>,
    ) -> Result<SegmentPush<PathDocument>, IndexError> {
        let estimate = Self::estimate_mutation(&mutation);
        self.buffer
            .try_push(mutation, estimate, |document| &document.document)
    }

    pub fn resident_bytes(&self) -> usize {
        self.buffer.resident_bytes()
    }

    pub fn seal_workspace_bytes(&self) -> Result<usize, IndexError> {
        self.buffer.seal_workspace_bytes()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub async fn seal<S: IndexBlockSink>(
        self,
        sink: &mut S,
    ) -> Result<Option<SealedRun>, IndexError> {
        self.seal_with_target(sink, DEFAULT_COMPONENT_BLOCK_BYTES)
            .await
    }

    async fn seal_with_target<S: IndexBlockSink>(
        self,
        sink: &mut S,
        target_block_bytes: usize,
    ) -> Result<Option<SealedRun>, IndexError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        let level = self.buffer.level();
        let entries = self.buffer.into_entries();
        let mut writer = PathComponentWriter::new(IndexKind::Path, level, target_block_bytes);
        let mut live = 0u64;
        let mut minimum_version = u64::MAX;
        let mut maximum_version = 0u64;
        for entry in entries.values() {
            let ordinal = if matches!(entry.mutation, IndexMutation::Upsert(_)) {
                let ordinal = live;
                live += 1;
                Some(ordinal)
            } else {
                None
            };
            let row =
                PathChange::from_mutation(&entry.mutation, |document| &document.document, ordinal);
            minimum_version = minimum_version.min(row.document.version);
            maximum_version = maximum_version.max(row.document.version);
            writer.push(row, sink).await?;
        }
        let mutation_count = entries.len() as u64;
        let path_tree = writer.finish(sink).await?;
        Ok(Some(seal_run_root(
            IndexKind::Path,
            level,
            RunStatistics {
                mutation_count,
                live_document_count: live,
                minimum_version,
                maximum_version,
            },
            [path_tree],
        )?))
    }
}

pub struct PathEngine;

impl PathEngine {
    pub fn builder(options: SegmentBuildOptions) -> Result<PathSegmentBuilder, IndexError> {
        PathSegmentBuilder::new(options)
    }

    pub async fn query<D: IndexDirectoryRead>(
        runs: &[D],
        query: PathQuery<'_>,
    ) -> Result<Vec<DocumentRef>, IndexError> {
        validate_prefix(query.prefix)?;
        if let Some(after) = query.after_path {
            validate_path(after)?;
        }
        if query.limit == 0 || runs.is_empty() {
            return Ok(Vec::new());
        }
        let views = open_views(runs, IndexKind::Path).await?;
        let upper = prefix_successor(query.prefix.as_bytes());
        let range = KeyRange {
            lower: Some(query.prefix.as_bytes().to_vec()),
            upper: upper.clone(),
        };
        let mut live_probe = LatestLiveProbe::new();
        let mut selected = BTreeMap::<String, DocumentRef>::new();
        for (run_index, run) in runs.iter().enumerate() {
            let root = views[run_index].component(PATH_CHANGES_TAG)?.clone();
            let mut cursor = LeafCursor::in_range(run, root, range.clone());
            while let Some(descriptor) = cursor.next().await? {
                if descriptor.maximum_key.as_slice() < query.prefix.as_bytes()
                    || upper
                        .as_ref()
                        .is_some_and(|upper| descriptor.minimum_key.as_slice() >= upper.as_slice())
                {
                    continue;
                }
                let rows = read_path_block(run, &descriptor).await?;
                let prefix = query.prefix.to_owned();
                let after = query.after_path.map(str::to_owned);
                let candidates = run
                    .run_query_cpu(move || {
                        Ok(rows
                            .into_iter()
                            .filter(|candidate| {
                                candidate.document.path.starts_with(&prefix)
                                    && after.as_ref().is_none_or(|after| {
                                        candidate.document.path.as_str() > after.as_str()
                                    })
                            })
                            .collect::<Vec<_>>())
                    })
                    .await?;
                let mut live = Vec::with_capacity(candidates.len());
                for candidate in candidates {
                    let Some((_, latest)) = live_probe
                        .latest_change(runs, &views, &candidate.document.path)
                        .await?
                    else {
                        continue;
                    };
                    if latest.state == DocumentState::Live
                        && latest.document.version == candidate.document.version
                    {
                        live.push(latest.document);
                    }
                }
                if !live.is_empty() {
                    let limit = query.limit;
                    selected = run
                        .run_query_cpu(move || {
                            for document in live {
                                selected.entry(document.path.clone()).or_insert(document);
                                if selected.len() > limit {
                                    selected.pop_last();
                                }
                            }
                            Ok(selected)
                        })
                        .await?;
                }
            }
        }
        Ok(selected.into_values().collect())
    }

    pub async fn merge_runs<D, S>(
        runs: &[D],
        output_level: u8,
        sink: &mut S,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead,
        S: IndexBlockSink,
    {
        Self::merge_with_target(runs, output_level, DEFAULT_COMPONENT_BLOCK_BYTES, sink).await
    }

    /// Compact deterministic path-key stripes with lane-local immutable output
    /// staging and one ordered root assembly.
    pub async fn merge_runs_parallel<D, S, E>(
        runs: &[D],
        output_level: u8,
        sink: &mut S,
        parallelism: CompactionParallelism,
        progress: CompactionProgress,
        executor: E,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead + Clone + 'static,
        S: IndexBlockSink + Clone + 'static,
        E: CompactionExecutor,
    {
        Self::merge_parallel_with_target(
            runs,
            output_level,
            DEFAULT_COMPONENT_BLOCK_BYTES,
            sink,
            parallelism,
            progress,
            executor,
        )
        .await
    }

    async fn merge_parallel_with_target<D, S, E>(
        runs: &[D],
        output_level: u8,
        target_block_bytes: usize,
        sink: &mut S,
        parallelism: CompactionParallelism,
        progress: CompactionProgress,
        executor: E,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead + Clone + 'static,
        S: IndexBlockSink + Clone + 'static,
        E: CompactionExecutor,
    {
        if runs.is_empty() || output_level == 0 {
            return Err(IndexError::InvalidDefinition(
                "path compaction requires input runs and an L1+ output level".into(),
            ));
        }
        crate::compaction::validate_parallel_compaction_fan_in(runs.len())?;
        let views = open_views(runs, IndexKind::Path).await?;
        let roots = views
            .iter()
            .map(|view| view.component(PATH_CHANGES_TAG).cloned())
            .collect::<Result<Vec<_>, _>>()?;
        let plan = deterministic_key_range_plan(roots.iter().cloned(), parallelism.max_lanes());
        progress.record_range_limit(plan.range_limit)?;
        let ranges = plan.ranges;
        let runs = Arc::new(runs.to_vec());
        let roots = Arc::new(roots);
        let mut write_producers =
            Vec::<LaneResultProducer<PathRangeOutput>>::with_capacity(ranges.len());
        for (range_id, range) in ranges.into_iter().enumerate() {
            let range_id = u64::try_from(range_id).map_err(|_| IndexError::OffsetOverflow)?;
            let ordinal_base = crate::bulk::range_ordinal_base(range_id)?;
            let runs = runs.clone();
            let roots = roots.clone();
            let lane_executor = executor.clone();
            let lane_progress = progress.clone();
            let mut lane_sink = sink.fork()?;
            write_producers.push(Box::new(move || {
                Box::pin(async move {
                    let mut cursor = PathWinnerCursor::open(
                        runs.as_slice(),
                        roots.as_slice(),
                        range,
                        lane_executor,
                        lane_progress.clone(),
                    )
                    .await?;
                    let mut writer = None::<PathComponentWriter>;
                    let mut summary = PathRangeSummary::default();
                    let mut local_live_count = 0u64;
                    while let Some((_, mut winner)) = cursor.next().await? {
                        summary.record(&winner)?;
                        winner.document_ordinal = if winner.state == DocumentState::Live {
                            let ordinal =
                                crate::bulk::range_local_ordinal(ordinal_base, local_live_count)?;
                            local_live_count = local_live_count
                                .checked_add(1)
                                .ok_or(IndexError::OffsetOverflow)?;
                            Some(ordinal)
                        } else {
                            None
                        };
                        let writer = writer.get_or_insert_with(|| {
                            PathComponentWriter::new(
                                IndexKind::Path,
                                output_level,
                                target_block_bytes,
                            )
                        });
                        writer.push(winner, &mut lane_sink).await?;
                        lane_progress.record_output(1, 0, 0);
                    }
                    let tree = match writer {
                        Some(writer) => Some(writer.finish(&mut lane_sink).await?),
                        None => None,
                    };
                    Ok(PathRangeOutput { tree, summary })
                })
            }));
        }
        let outputs = collect_ordered_lanes(&executor, write_producers, &progress).await?;
        let statistics = PathRangeSummary::combine(
            &outputs
                .iter()
                .map(|output| output.summary)
                .collect::<Vec<_>>(),
        )?;
        if statistics.mutation_count == 0 {
            return Err(IndexError::InvalidDefinition(
                "path compaction produced no changes".into(),
            ));
        }
        let tree = assemble_component_ranges(
            IndexKind::Path,
            PATH_CHANGES_TAG,
            outputs.into_iter().filter_map(|output| output.tree),
            sink,
        )
        .await?;
        seal_run_root(
            IndexKind::Path,
            output_level,
            RunStatistics {
                mutation_count: statistics.mutation_count,
                live_document_count: statistics.live_count,
                minimum_version: statistics.minimum_version,
                maximum_version: statistics.maximum_version,
            },
            [tree],
        )
    }

    async fn merge_with_target<D, S>(
        runs: &[D],
        output_level: u8,
        target_block_bytes: usize,
        sink: &mut S,
    ) -> Result<SealedRun, IndexError>
    where
        D: IndexDirectoryRead,
        S: IndexBlockSink,
    {
        if runs.is_empty() || output_level == 0 {
            return Err(IndexError::InvalidDefinition(
                "path compaction requires input runs and an L1+ output level".into(),
            ));
        }
        let views = open_views(runs, IndexKind::Path).await?;
        let mut cursors = Vec::with_capacity(runs.len());
        for (run, view) in runs.iter().zip(&views) {
            cursors.push(PathRunCursor::new(
                run,
                view.component(PATH_CHANGES_TAG)?.clone(),
            ));
        }
        let mut current = Vec::with_capacity(cursors.len());
        for cursor in &mut cursors {
            current.push(cursor.next().await?);
        }
        let mut writer =
            PathComponentWriter::new(IndexKind::Path, output_level, target_block_bytes);
        let mut mutation_count = 0u64;
        let mut live_count = 0u64;
        let mut minimum_version = u64::MAX;
        let mut maximum_version = 0u64;
        loop {
            let Some(path) = current
                .iter()
                .flatten()
                .map(|row| row.document.path.as_str())
                .min()
                .map(str::to_owned)
            else {
                break;
            };
            let mut winner = None::<(usize, PathChange)>;
            for (run_index, row) in current.iter().enumerate() {
                let Some(row) = row.as_ref().filter(|row| row.document.path == path) else {
                    continue;
                };
                if winner.as_ref().is_none_or(|(current_index, current)| {
                    row.document.version > current.document.version
                        || (row.document.version == current.document.version
                            && run_index < *current_index)
                }) {
                    winner = Some((run_index, row.clone()));
                }
            }
            for (run_index, row) in current.iter_mut().enumerate() {
                if row.as_ref().is_some_and(|row| row.document.path == path) {
                    *row = cursors[run_index].next().await?;
                }
            }
            let mut winner = winner.unwrap().1;
            winner.document_ordinal = if winner.state == DocumentState::Live {
                let ordinal = live_count;
                live_count += 1;
                Some(ordinal)
            } else {
                None
            };
            minimum_version = minimum_version.min(winner.document.version);
            maximum_version = maximum_version.max(winner.document.version);
            mutation_count += 1;
            writer.push(winner, sink).await?;
        }
        if mutation_count == 0 {
            return Err(IndexError::InvalidDefinition(
                "path compaction produced no changes".into(),
            ));
        }
        let tree = writer.finish(sink).await?;
        seal_run_root(
            IndexKind::Path,
            output_level,
            RunStatistics {
                mutation_count,
                live_document_count: live_count,
                minimum_version,
                maximum_version,
            },
            [tree],
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PathRangeSummary {
    mutation_count: u64,
    live_count: u64,
    minimum_version: u64,
    maximum_version: u64,
}

impl Default for PathRangeSummary {
    fn default() -> Self {
        Self {
            mutation_count: 0,
            live_count: 0,
            minimum_version: u64::MAX,
            maximum_version: 0,
        }
    }
}

impl PathRangeSummary {
    fn record(&mut self, winner: &PathChange) -> Result<(), IndexError> {
        self.mutation_count = self
            .mutation_count
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        if winner.state == DocumentState::Live {
            self.live_count = self
                .live_count
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        self.minimum_version = self.minimum_version.min(winner.document.version);
        self.maximum_version = self.maximum_version.max(winner.document.version);
        Ok(())
    }

    fn combine(ranges: &[Self]) -> Result<Self, IndexError> {
        let mut combined = Self::default();
        for range in ranges {
            combined.mutation_count = combined
                .mutation_count
                .checked_add(range.mutation_count)
                .ok_or(IndexError::OffsetOverflow)?;
            combined.live_count = combined
                .live_count
                .checked_add(range.live_count)
                .ok_or(IndexError::OffsetOverflow)?;
            if range.mutation_count != 0 {
                combined.minimum_version = combined.minimum_version.min(range.minimum_version);
                combined.maximum_version = combined.maximum_version.max(range.maximum_version);
            }
        }
        Ok(combined)
    }
}

struct PathRangeOutput {
    tree: Option<ComponentTree>,
    summary: PathRangeSummary,
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    for index in (0..successor.len()).rev() {
        if successor[index] != u8::MAX {
            successor[index] += 1;
            successor.truncate(index + 1);
            return Some(successor);
        }
    }
    None
}

fn validate_path(path: &str) -> Result<(), IndexError> {
    if path.is_empty() || path.contains('\0') || path.len() > MAX_INDEX_ROUTING_KEY_BYTES {
        return Err(IndexError::InvalidQuery(
            "path must be 1..=4096 bytes without NUL".into(),
        ));
    }
    Ok(())
}

fn validate_prefix(prefix: &str) -> Result<(), IndexError> {
    if prefix.contains('\0') || prefix.len() > MAX_INDEX_ROUTING_KEY_BYTES {
        return Err(IndexError::InvalidQuery(
            "path prefix must be at most 4096 bytes and contain no NUL".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::compaction::COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES;
    use crate::compaction::test_support::TokioExecutor;
    use crate::io::tests::{MemoryBlockSink, MemoryDirectory};

    use super::*;

    fn upsert(path: &str, version: u64) -> IndexMutation<PathDocument> {
        IndexMutation::Upsert(PathDocument {
            document: DocumentRef {
                path: path.into(),
                version,
            },
        })
    }

    async fn build_run(
        mutations: impl IntoIterator<Item = IndexMutation<PathDocument>>,
        level: u8,
        target: usize,
    ) -> (MemoryBlockSink, SealedRun) {
        let mut builder =
            PathSegmentBuilder::new(SegmentBuildOptions::for_level(64 * 1024, level).unwrap())
                .unwrap();
        for mutation in mutations {
            assert!(matches!(
                builder.try_push(mutation).unwrap(),
                SegmentPush::Accepted
            ));
        }
        let mut sink = MemoryBlockSink::default();
        let run = builder
            .seal_with_target(&mut sink, target)
            .await
            .unwrap()
            .unwrap();
        (sink, run)
    }

    #[tokio::test]
    async fn path_pagination_accepts_a_real_after_path() {
        let (sink, run) = build_run([upsert("objects/a", 1), upsert("objects/b", 1)], 0, 128).await;
        let directory = sink.directory_with_root(run.into_root());

        let selected = PathEngine::query(
            &[directory],
            PathQuery {
                prefix: "objects/",
                after_path: Some("objects/a"),
                limit: 10,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            selected,
            vec![DocumentRef {
                path: "objects/b".into(),
                version: 1,
            }]
        );
    }

    fn directory(sink: &MemoryBlockSink, run: SealedRun) -> MemoryDirectory {
        sink.directory_with_root(run.into_root())
    }

    #[tokio::test]
    async fn tiny_blocks_update_delete_and_compact_equivalently() {
        let (old_sink, old_run) =
            build_run([upsert("/a", 1), upsert("/b", 1), upsert("/c", 1)], 0, 64).await;
        let old = directory(&old_sink, old_run);
        let (new_sink, new_run) = build_run(
            [
                upsert("/a", 2),
                IndexMutation::Remove(DocumentRef {
                    path: "/b".into(),
                    version: 2,
                }),
            ],
            0,
            64,
        )
        .await;
        let new = directory(&new_sink, new_run);
        let runs = [new, old];
        let query = PathQuery {
            prefix: "/",
            after_path: None,
            limit: 10,
        };
        let expected = PathEngine::query(&runs, query.clone()).await.unwrap();
        assert_eq!(
            expected,
            [
                DocumentRef {
                    path: "/a".into(),
                    version: 2
                },
                DocumentRef {
                    path: "/c".into(),
                    version: 1
                }
            ]
        );

        let mut merged_sink = MemoryBlockSink::default();
        let merged = PathEngine::merge_with_target(&runs, 1, 64, &mut merged_sink)
            .await
            .unwrap();
        assert_eq!(merged.descriptor().level, 1);
        let merged = [directory(&merged_sink, merged)];
        assert_eq!(PathEngine::query(&merged, query).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn output_is_deterministic_and_flushes_multiple_blocks() {
        let mutations = (0..100)
            .map(|index| upsert(&format!("/common/prefix/{index:04}"), 1))
            .collect::<Vec<_>>();
        let (first_sink, first) = build_run(mutations.clone(), 1, 128).await;
        let (second_sink, second) = build_run(mutations, 1, 128).await;
        assert_eq!(first.descriptor().hash, second.descriptor().hash);
        assert_eq!(
            first.descriptor().encoded_bytes,
            second.descriptor().encoded_bytes
        );
        assert!(first_sink.len() > 2);
        assert_eq!(first_sink.len(), second_sink.len());
    }

    #[tokio::test]
    async fn one_and_four_lane_compaction_are_query_equivalent() {
        let old_mutations = (0..80)
            .map(|index| upsert(&format!("/{:02x}/object/{index:04}", index % 32), 1))
            .collect::<Vec<_>>();
        let (old_sink, old_run) = build_run(old_mutations, 0, 96).await;
        let old = directory(&old_sink, old_run);
        let new_mutations = (0..40)
            .map(|index| {
                if index % 5 == 0 {
                    IndexMutation::Remove(DocumentRef {
                        path: format!("/{:02x}/object/{index:04}", index % 32),
                        version: 2,
                    })
                } else {
                    upsert(&format!("/{:02x}/object/{index:04}", index % 32), 2)
                }
            })
            .collect::<Vec<_>>();
        let (new_sink, new_run) = build_run(new_mutations, 0, 96).await;
        let new = directory(&new_sink, new_run);
        let runs = [new, old];

        let one_lane_progress = CompactionProgress::default();
        let mut one_lane_sink = MemoryBlockSink::default();
        let one_lane = PathEngine::merge_parallel_with_target(
            &runs,
            1,
            96,
            &mut one_lane_sink,
            CompactionParallelism::serial(),
            one_lane_progress.clone(),
            TokioExecutor::default(),
        )
        .await
        .unwrap();
        let progress = CompactionProgress::default();
        let mut first_parallel_sink = MemoryBlockSink::default();
        let first_parallel = PathEngine::merge_parallel_with_target(
            &runs,
            1,
            96,
            &mut first_parallel_sink,
            CompactionParallelism::new(4, COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES).unwrap(),
            progress.clone(),
            TokioExecutor::default(),
        )
        .await
        .unwrap();
        let mut second_parallel_sink = MemoryBlockSink::default();
        let second_parallel = PathEngine::merge_parallel_with_target(
            &runs,
            1,
            96,
            &mut second_parallel_sink,
            CompactionParallelism::new(4, COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES).unwrap(),
            CompactionProgress::default(),
            TokioExecutor::default(),
        )
        .await
        .unwrap();

        assert_eq!(first_parallel, second_parallel);
        assert_eq!(first_parallel_sink.len(), second_parallel_sink.len());
        let parallel_mutation_count = first_parallel.descriptor().mutation_count;
        let query = PathQuery {
            prefix: "/",
            after_path: None,
            limit: 200,
        };
        let one_lane = [directory(&one_lane_sink, one_lane)];
        let parallel = [directory(&first_parallel_sink, first_parallel)];
        assert_eq!(
            PathEngine::query(&parallel, query.clone()).await.unwrap(),
            PathEngine::query(&one_lane, query).await.unwrap()
        );
        assert_eq!(one_lane_progress.snapshot().effective_lanes, 1);
        let snapshot = progress.snapshot();
        assert_eq!(snapshot.ranges_total, snapshot.effective_lanes);
        assert!(snapshot.effective_lanes > 1 && snapshot.effective_lanes <= 4);
        assert_eq!(snapshot.ranges_completed, snapshot.ranges_total);
        assert_eq!(snapshot.output_records, parallel_mutation_count);
        assert!(snapshot.input_records >= parallel_mutation_count);
    }

    #[tokio::test]
    async fn parallel_cpu_failure_joins_all_ranges() {
        let (sink, run) = build_run([upsert("/a", 1), upsert("/z", 1)], 0, 64).await;
        let runs = [directory(&sink, run)];
        let progress = CompactionProgress::default();
        let mut output = MemoryBlockSink::default();
        let error = PathEngine::merge_parallel_with_target(
            &runs,
            1,
            64,
            &mut output,
            CompactionParallelism::new(4, COMPACTION_INCREMENTAL_LANE_WORKSPACE_BYTES).unwrap(),
            progress.clone(),
            TokioExecutor::failing_cpu(),
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("injected compaction CPU failure")
        );
        assert_eq!(progress.snapshot().active_lanes, 0);
        assert_eq!(progress.snapshot().waiting_lanes, 0);
    }

    #[test]
    fn full_builder_returns_the_unadmitted_mutation_and_never_crosses_cap() {
        let options = SegmentBuildOptions::new(140).unwrap();
        let mut builder = PathSegmentBuilder::new(options).unwrap();
        assert!(matches!(
            builder.try_push(upsert("/a", 1)).unwrap(),
            SegmentPush::Accepted
        ));
        assert!(matches!(
            builder.try_push(upsert("/b", 1)).unwrap(),
            SegmentPush::Full(_)
        ));
        assert!(builder.resident_bytes() <= options.max_resident_bytes);
    }
}
