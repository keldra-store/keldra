//! Bounded decoded-leaf reuse for projection compaction point lookups.

use std::collections::VecDeque;

use crate::run::{RunView, find_leaf};
use crate::segment::{
    DOCUMENTS_TAG, DocumentRecord, PathChange, read_document_block_parallel,
    read_path_block_parallel,
};
use crate::{BlockDescriptor, DocumentRef, IndexDirectoryRead, IndexError};

use super::parallel_compaction::read_projection_block_parallel;
use super::{OrdinalRow, ProjectionPayload, RECORDS_TAG, ordinal_key};

const MAX_SOURCE_PAYLOAD_LEAVES: usize = 5;
const MAX_INPUT_DOCUMENT_LEAVES: usize = 4;
const MAX_STAGED_OUTPUT_PATH_LEAVES: usize = 1;

enum CachedRows<T> {
    Documents(Vec<DocumentRecord>),
    Paths(Vec<PathChange>),
    Projections(Vec<OrdinalRow<T>>),
}

struct CachedLeaf<T> {
    root_hash: [u8; 32],
    descriptor: BlockDescriptor,
    rows: CachedRows<T>,
}

pub(super) struct ProjectionPointCache<T> {
    max_leaves: usize,
    leaves: VecDeque<CachedLeaf<T>>,
}

impl<T> Default for ProjectionPointCache<T> {
    fn default() -> Self {
        Self {
            max_leaves: MAX_SOURCE_PAYLOAD_LEAVES,
            leaves: VecDeque::with_capacity(MAX_SOURCE_PAYLOAD_LEAVES),
        }
    }
}

impl<T> ProjectionPointCache<T>
where
    T: ProjectionPayload + Clone + Send + 'static,
{
    pub(super) fn input_documents() -> Self {
        Self::with_limit(MAX_INPUT_DOCUMENT_LEAVES)
    }

    pub(super) fn staged_output_paths() -> Self {
        Self::with_limit(MAX_STAGED_OUTPUT_PATH_LEAVES)
    }

    fn with_limit(max_leaves: usize) -> Self {
        Self {
            max_leaves,
            leaves: VecDeque::with_capacity(max_leaves),
        }
    }

    pub(super) async fn document<D, E>(
        &mut self,
        directory: &D,
        view: &RunView,
        ordinal: u64,
        executor: &E,
        progress: &crate::compaction::CompactionProgress,
    ) -> Result<DocumentRef, IndexError>
    where
        D: IndexDirectoryRead,
        E: crate::compaction::CompactionExecutor,
    {
        let root = view.component(DOCUMENTS_TAG)?;
        let key = ordinal_key(ordinal);
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

    pub(super) async fn projection<D, E>(
        &mut self,
        directory: &D,
        view: &RunView,
        ordinal: u64,
        executor: &E,
        progress: &crate::compaction::CompactionProgress,
    ) -> Result<T, IndexError>
    where
        D: IndexDirectoryRead,
        E: crate::compaction::CompactionExecutor,
    {
        let root = view
            .component_optional(RECORDS_TAG)
            .ok_or(IndexError::InvalidFormat("missing projection component"))?;
        let key = ordinal_key(ordinal);
        if let Some(index) = self.cached_leaf(root, &key, |rows| {
            matches!(rows, CachedRows::Projections(_))
        }) {
            let CachedRows::Projections(rows) = &self.touch(index).rows else {
                unreachable!("cache variant was checked")
            };
            return projection_in_rows(rows, ordinal);
        }
        self.reserve_miss_slot();
        let descriptor = find_leaf(directory, root, &key)
            .await?
            .ok_or(IndexError::InvalidFormat("missing projection ordinal"))?;
        let rows =
            read_projection_block_parallel(directory, &descriptor, executor, progress).await?;
        let projection = projection_in_rows(&rows, ordinal)?;
        self.insert(CachedLeaf {
            root_hash: root.hash,
            descriptor,
            rows: CachedRows::Projections(rows),
        });
        Ok(projection)
    }

    pub(super) async fn path<D, E>(
        &mut self,
        directory: &D,
        root: &BlockDescriptor,
        path: &str,
        executor: &E,
        progress: &crate::compaction::CompactionProgress,
    ) -> Result<Option<PathChange>, IndexError>
    where
        D: IndexDirectoryRead,
        E: crate::compaction::CompactionExecutor,
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
        let change = path_in_rows(&rows, path);
        self.insert(CachedLeaf {
            root_hash: root.hash,
            descriptor,
            rows: CachedRows::Paths(rows),
        });
        Ok(change)
    }

    fn cached_leaf(
        &self,
        root: &BlockDescriptor,
        key: &[u8],
        expected: impl Fn(&CachedRows<T>) -> bool,
    ) -> Option<usize> {
        self.leaves.iter().position(|leaf| {
            leaf.root_hash == root.hash
                && key >= leaf.descriptor.minimum_key.as_slice()
                && key <= leaf.descriptor.maximum_key.as_slice()
                && expected(&leaf.rows)
        })
    }

    fn touch(&mut self, index: usize) -> &CachedLeaf<T> {
        let leaf = self
            .leaves
            .remove(index)
            .expect("cache index came from this deque");
        self.leaves.push_back(leaf);
        self.leaves.back().expect("touched leaf was reinserted")
    }

    fn insert(&mut self, leaf: CachedLeaf<T>) {
        debug_assert!(self.leaves.len() < self.max_leaves);
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

fn projection_in_rows<T: Clone>(rows: &[OrdinalRow<T>], ordinal: u64) -> Result<T, IndexError> {
    let index = rows
        .binary_search_by_key(&ordinal, |row| row.ordinal)
        .map_err(|_| IndexError::InvalidFormat("missing projection ordinal"))?;
    Ok(rows[index].payload.clone())
}

fn path_in_rows(rows: &[PathChange], path: &str) -> Option<PathChange> {
    let index = rows
        .binary_search_by(|row| row.document.path.as_str().cmp(path))
        .ok()?;
    Some(rows[index].clone())
}
