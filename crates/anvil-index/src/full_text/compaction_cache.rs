//! Bounded decoded-leaf reuse for full-text compaction point lookups.

use std::collections::VecDeque;

use crate::compaction::{CompactionExecutor, CompactionProgress};
use crate::run::{RunView, find_leaf};
use crate::segment::{
    DOCUMENTS_TAG, DocumentRecord, PATH_CHANGES_TAG, PathChange, read_document_block_parallel,
    read_path_block_parallel,
};
use crate::{BlockDescriptor, DocumentRef, IndexDirectoryRead, IndexError};

/// The posting phase can retain one document and one source-path leaf for each
/// of four inputs. Its separate output cache retains one staged path leaf at
/// the same time. Streaming posting cursors own their separately charged
/// leaves.
const MAX_INPUT_LEAVES: usize = 8;
const MAX_STAGED_OUTPUT_LEAVES: usize = 1;

enum CachedRows {
    Documents(Vec<DocumentRecord>),
    Paths(Vec<PathChange>),
}

struct CachedLeaf {
    root_hash: [u8; 32],
    descriptor: BlockDescriptor,
    rows: CachedRows,
}

/// A compaction-lane-local LRU discarded with the lane.
pub(super) struct FullTextPointCache {
    max_leaves: usize,
    leaves: VecDeque<CachedLeaf>,
}

impl FullTextPointCache {
    pub(super) fn input() -> Self {
        Self::with_limit(MAX_INPUT_LEAVES)
    }

    pub(super) fn staged_output() -> Self {
        Self::with_limit(MAX_STAGED_OUTPUT_LEAVES)
    }

    fn with_limit(max_leaves: usize) -> Self {
        Self {
            max_leaves,
            leaves: VecDeque::with_capacity(max_leaves),
        }
    }

    #[cfg(test)]
    pub(super) fn cached_leaf_count(&self) -> usize {
        self.leaves.len()
    }

    pub(super) async fn document<D, E>(
        &mut self,
        directory: &D,
        view: &RunView,
        ordinal: u64,
        executor: &E,
        progress: &CompactionProgress,
    ) -> Result<DocumentRef, IndexError>
    where
        D: IndexDirectoryRead,
        E: CompactionExecutor,
    {
        let root = view.component(DOCUMENTS_TAG)?;
        let key = ordinal.to_be_bytes();
        if let Some(index) =
            self.cached_leaf(root, &key, |rows| matches!(rows, CachedRows::Documents(_)))
        {
            let CachedRows::Documents(rows) = &self.touch(index).rows else {
                unreachable!("cache variant was checked")
            };
            return document_in_rows(rows, ordinal);
        }

        let descriptor = find_leaf(directory, root, &key)
            .await?
            .ok_or(IndexError::InvalidFormat("document ordinal outside run"))?;
        self.reserve_miss_slot();
        let rows = read_document_block_parallel(directory, &descriptor, executor, progress).await?;
        let document = document_in_rows(&rows, ordinal)?;
        self.insert(CachedLeaf {
            root_hash: root.hash,
            descriptor,
            rows: CachedRows::Documents(rows),
        });
        Ok(document)
    }

    pub(super) async fn path<D, E>(
        &mut self,
        directory: &D,
        root: &BlockDescriptor,
        path: &str,
        executor: &E,
        progress: &CompactionProgress,
    ) -> Result<Option<PathChange>, IndexError>
    where
        D: IndexDirectoryRead,
        E: CompactionExecutor,
    {
        let key = path.as_bytes();
        if let Some(index) =
            self.cached_leaf(root, key, |rows| matches!(rows, CachedRows::Paths(_)))
        {
            let CachedRows::Paths(rows) = &self.touch(index).rows else {
                unreachable!("cache variant was checked")
            };
            return Ok(path_in_rows(rows, path));
        }

        let Some(descriptor) = find_leaf(directory, root, key).await? else {
            return Ok(None);
        };
        self.reserve_miss_slot();
        let rows = read_path_block_parallel(directory, &descriptor, executor, progress).await?;
        let change = path_in_rows(&rows, path);
        self.insert(CachedLeaf {
            root_hash: root.hash,
            descriptor,
            rows: CachedRows::Paths(rows),
        });
        Ok(change)
    }

    pub(super) async fn latest_path<D, E>(
        &mut self,
        runs: &[D],
        views: &[RunView],
        path: &str,
        executor: &E,
        progress: &CompactionProgress,
    ) -> Result<Option<(usize, PathChange)>, IndexError>
    where
        D: IndexDirectoryRead,
        E: CompactionExecutor,
    {
        if runs.len() != views.len() {
            return Err(IndexError::InvalidDefinition(
                "run readers and descriptors must have equal length".into(),
            ));
        }
        let mut winner = None::<(usize, PathChange)>;
        for (run_index, (run, view)) in runs.iter().zip(views).enumerate() {
            let root = view.component(PATH_CHANGES_TAG)?;
            let Some(candidate) = self.path(run, root, path, executor, progress).await? else {
                continue;
            };
            if winner.as_ref().is_none_or(|(current_index, current)| {
                candidate.document.version > current.document.version
                    || (candidate.document.version == current.document.version
                        && run_index < *current_index)
            }) {
                winner = Some((run_index, candidate));
            }
        }
        Ok(winner)
    }

    fn cached_leaf(
        &self,
        root: &BlockDescriptor,
        key: &[u8],
        expected: impl Fn(&CachedRows) -> bool,
    ) -> Option<usize> {
        self.leaves.iter().position(|leaf| {
            leaf.root_hash == root.hash
                && key >= leaf.descriptor.minimum_key.as_slice()
                && key <= leaf.descriptor.maximum_key.as_slice()
                && expected(&leaf.rows)
        })
    }

    fn touch(&mut self, index: usize) -> &CachedLeaf {
        let leaf = self
            .leaves
            .remove(index)
            .expect("cache index came from this deque");
        self.leaves.push_back(leaf);
        self.leaves.back().expect("touched leaf was reinserted")
    }

    fn insert(&mut self, leaf: CachedLeaf) {
        assert!(
            self.leaves.len() < self.max_leaves,
            "a point-cache miss must reserve capacity before decoding"
        );
        self.leaves.push_back(leaf);
    }

    fn reserve_miss_slot(&mut self) {
        if self.leaves.len() == self.max_leaves {
            self.leaves.pop_front();
        }
    }
}

fn document_in_rows(rows: &[DocumentRecord], ordinal: u64) -> Result<DocumentRef, IndexError> {
    let index = rows
        .binary_search_by_key(&ordinal, |row| row.ordinal)
        .map_err(|_| IndexError::InvalidFormat("missing document ordinal"))?;
    Ok(rows[index].document.clone())
}

fn path_in_rows(rows: &[PathChange], path: &str) -> Option<PathChange> {
    let index = rows
        .binary_search_by(|row| row.document.path.as_str().cmp(path))
        .ok()?;
    Some(rows[index].clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ComponentCodec, IndexKind};

    #[test]
    fn point_cache_never_retains_more_than_its_charged_leaf_count() {
        assert_eq!(MAX_INPUT_LEAVES, 8);
        assert_eq!(MAX_STAGED_OUTPUT_LEAVES, 1);
        assert_cache_limit(FullTextPointCache::input(), MAX_INPUT_LEAVES);
        assert_cache_limit(
            FullTextPointCache::staged_output(),
            MAX_STAGED_OUTPUT_LEAVES,
        );
    }

    fn assert_cache_limit(mut cache: FullTextPointCache, limit: usize) {
        for value in 0..(limit + 3) {
            cache.reserve_miss_slot();
            cache.insert(CachedLeaf {
                root_hash: [value as u8; 32],
                descriptor: descriptor(value as u8),
                rows: CachedRows::Documents(vec![DocumentRecord {
                    ordinal: value as u64,
                    document: DocumentRef {
                        path: format!("/{value}"),
                        version: 1,
                    },
                }]),
            });
            assert!(cache.cached_leaf_count() <= limit);
        }
        assert_eq!(cache.cached_leaf_count(), limit);
        assert_eq!(cache.leaves.front().unwrap().root_hash, [3; 32]);
    }

    fn descriptor(value: u8) -> BlockDescriptor {
        BlockDescriptor {
            kind: IndexKind::FullText,
            component_tag: DOCUMENTS_TAG,
            codec: ComponentCodec::FixedRows,
            routing_height: 0,
            minimum_key: vec![value],
            maximum_key: vec![value],
            element_count: 1,
            encoded_bytes: 1,
            hash: [value; 32],
        }
    }
}
