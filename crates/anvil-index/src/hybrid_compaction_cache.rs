//! Bounded decoded-leaf reuse for hybrid text compaction.

use std::collections::VecDeque;

use crate::compaction::{CompactionExecutor, CompactionProgress};
use crate::run::{RunView, find_leaf};
use crate::segment::{
    DOCUMENTS_TAG, DocumentRecord, PathChange, read_document_block_parallel,
    read_path_block_parallel,
};
use crate::{BlockDescriptor, DocumentRef, IndexDirectoryRead, IndexError};

// Four text cursors retain their current decoded leaves. Bound the shared
// document/input-path/output-path reuse set to five more leaves so one lane
// never retains more than the nine leaves covered by its workspace charge.
const MAX_CACHED_LEAVES: usize = 5;

enum CachedRows {
    Documents(Vec<DocumentRecord>),
    Paths(Vec<PathChange>),
}

struct CachedLeaf {
    root_hash: [u8; 32],
    descriptor: BlockDescriptor,
    rows: CachedRows,
}

#[derive(Default)]
pub(super) struct HybridCompactionPointCache {
    leaves: VecDeque<CachedLeaf>,
}

impl HybridCompactionPointCache {
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

        self.reserve_miss_slot();
        let descriptor = find_leaf(directory, root, &key)
            .await?
            .ok_or(IndexError::InvalidFormat("document ordinal outside run"))?;
        let rows = read_document_block_parallel(directory, &descriptor, executor, progress).await?;
        let document = document_in_rows(&rows, ordinal)?;
        self.insert(CachedLeaf {
            root_hash: root.hash,
            descriptor,
            rows: CachedRows::Documents(rows),
        });
        Ok(document)
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
        let mut winner = None::<(usize, PathChange)>;
        for (run_index, (run, view)) in runs.iter().zip(views).enumerate() {
            let root = view.component(crate::segment::PATH_CHANGES_TAG)?;
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

        self.reserve_miss_slot();
        let Some(descriptor) = find_leaf(directory, root, key).await? else {
            return Ok(None);
        };
        let rows = read_path_block_parallel(directory, &descriptor, executor, progress).await?;
        let candidate = path_in_rows(&rows, path);
        self.insert(CachedLeaf {
            root_hash: root.hash,
            descriptor,
            rows: CachedRows::Paths(rows),
        });
        Ok(candidate)
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

    fn reserve_miss_slot(&mut self) {
        if self.leaves.len() == MAX_CACHED_LEAVES {
            self.leaves.pop_front();
        }
    }

    fn insert(&mut self, leaf: CachedLeaf) {
        debug_assert!(self.leaves.len() < MAX_CACHED_LEAVES);
        self.leaves.push_back(leaf);
    }
}

fn document_in_rows(rows: &[DocumentRecord], ordinal: u64) -> Result<DocumentRef, IndexError> {
    rows.binary_search_by_key(&ordinal, |row| row.ordinal)
        .map(|index| rows[index].document.clone())
        .map_err(|_| IndexError::InvalidFormat("missing document ordinal"))
}

fn path_in_rows(rows: &[PathChange], path: &str) -> Option<PathChange> {
    rows.binary_search_by(|row| row.document.path.as_str().cmp(path))
        .ok()
        .map(|index| rows[index].clone())
}
