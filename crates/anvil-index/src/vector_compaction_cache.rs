//! Bounded decoded-leaf reuse for vector compaction point lookups.

use std::collections::VecDeque;

use crate::compaction::{CompactionExecutor, CompactionProgress};
use crate::run::{RunView, find_leaf};
use crate::segment::{DOCUMENTS_TAG, DocumentRecord, read_document_block_parallel};
use crate::{BlockDescriptor, DocumentRef, IndexDirectoryRead, IndexError};

use super::{VectorDefinition, VectorRow, ordinal_key, read_vector_block_parallel};

// Path cursors already retain one decoded leaf for each of four inputs. Five
// shared document/vector point leaves keep the complete lane at nine retained
// decoded leaves; misses remain correct and merely evict the least-recent leaf.
const MAX_CACHED_LEAVES: usize = 5;

enum CachedRows {
    Documents(Vec<DocumentRecord>),
    Vectors(Vec<VectorRow>),
}

struct CachedLeaf {
    root_hash: [u8; 32],
    descriptor: BlockDescriptor,
    rows: CachedRows,
}

#[derive(Default)]
pub(super) struct VectorCompactionPointCache {
    leaves: VecDeque<CachedLeaf>,
}

impl VectorCompactionPointCache {
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

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn vector<D, E>(
        &mut self,
        directory: &D,
        view: &RunView,
        definition: &VectorDefinition,
        component_tag: u8,
        ordinal: u64,
        executor: &E,
        progress: &CompactionProgress,
    ) -> Result<Vec<f32>, IndexError>
    where
        D: IndexDirectoryRead,
        E: CompactionExecutor,
    {
        let root = view.component(component_tag)?;
        let key = ordinal_key(ordinal);
        if let Some(index) =
            self.cached_leaf(root, &key, |rows| matches!(rows, CachedRows::Vectors(_)))
        {
            let CachedRows::Vectors(rows) = &self.touch(index).rows else {
                unreachable!("cache variant was checked")
            };
            return vector_in_rows(rows, ordinal);
        }

        self.reserve_miss_slot();
        let descriptor = find_leaf(directory, root, &key)
            .await?
            .ok_or(IndexError::InvalidFormat("missing vector ordinal"))?;
        let rows =
            read_vector_block_parallel(directory, &descriptor, definition, executor, progress)
                .await?;
        let values = vector_in_rows(&rows, ordinal)?;
        self.insert(CachedLeaf {
            root_hash: root.hash,
            descriptor,
            rows: CachedRows::Vectors(rows),
        });
        Ok(values)
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

fn vector_in_rows(rows: &[VectorRow], ordinal: u64) -> Result<Vec<f32>, IndexError> {
    rows.binary_search_by_key(&ordinal, |row| row.ordinal)
        .map(|index| rows[index].values.clone())
        .map_err(|_| IndexError::InvalidFormat("missing vector ordinal"))
}
