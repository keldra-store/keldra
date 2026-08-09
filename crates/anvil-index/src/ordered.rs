//! Ordered path runs.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::run::{LeafCursor, RunStatistics, open_views, seal_run_root};
use crate::segment::{
    DEFAULT_COMPONENT_BLOCK_BYTES, DocumentState, MutationBuffer, PATH_CHANGES_TAG, PathChange,
    PathComponentWriter, PathRunCursor, latest_path_change, read_path_block,
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
        let mut selected = BTreeMap::<String, DocumentRef>::new();
        for (run_index, run) in runs.iter().enumerate() {
            let root = views[run_index].component(PATH_CHANGES_TAG)?.clone();
            let mut cursor = LeafCursor::new(run, root);
            while let Some(descriptor) = cursor.next().await? {
                if descriptor.maximum_key.as_slice() < query.prefix.as_bytes()
                    || upper
                        .as_ref()
                        .is_some_and(|upper| descriptor.minimum_key.as_slice() >= upper.as_slice())
                {
                    continue;
                }
                for candidate in read_path_block(run, &descriptor).await? {
                    if !candidate.document.path.starts_with(query.prefix)
                        || query
                            .after_path
                            .is_some_and(|after| candidate.document.path.as_str() <= after)
                    {
                        continue;
                    }
                    let Some((_, latest)) =
                        latest_path_change(runs, &views, &candidate.document.path).await?
                    else {
                        continue;
                    };
                    if latest.state == DocumentState::Live
                        && latest.document.version == candidate.document.version
                    {
                        selected
                            .entry(latest.document.path.clone())
                            .or_insert(latest.document);
                        if selected.len() > query.limit {
                            selected.pop_last();
                        }
                    }
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
