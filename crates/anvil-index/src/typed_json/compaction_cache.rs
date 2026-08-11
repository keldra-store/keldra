//! Bounded decoded-leaf reuse for typed compaction point lookups.

use std::collections::VecDeque;

use crate::run::{RunView, find_leaf};
use crate::segment::{
    DOCUMENTS_TAG, DocumentRecord, PathChange, read_document_block, read_path_block,
};
use crate::{BlockDescriptor, DocumentRef, IndexDirectoryRead, IndexError};

use super::{ROWS_TAG, TypedRow, ordinal_key, read_typed_block};

/// Four document leaves plus the staged output-path leaf are the complete
/// point-lookup working set for one four-way key-merge step.
const MAX_CACHED_LEAVES: usize = 5;

enum CachedRows {
    Documents(Vec<DocumentRecord>),
    Paths(Vec<PathChange>),
    Typed(Vec<TypedRow>),
}

struct CachedLeaf {
    root_hash: [u8; 32],
    descriptor: BlockDescriptor,
    rows: CachedRows,
}

/// A compaction-local LRU. A hit reuses both the resolved leaf descriptor and
/// its decoded rows, avoiding another routing-tree descent and whole-leaf
/// decode. It is discarded with the compaction quantum.
#[derive(Default)]
pub(super) struct CompactionPointCache {
    leaves: VecDeque<CachedLeaf>,
}

impl CompactionPointCache {
    #[cfg(test)]
    pub(super) fn cached_leaf_count(&self) -> usize {
        self.leaves.len()
    }

    pub(super) async fn document<D: IndexDirectoryRead>(
        &mut self,
        directory: &D,
        view: &RunView,
        ordinal: u64,
    ) -> Result<DocumentRef, IndexError> {
        let root = view.component(DOCUMENTS_TAG)?;
        let key = ordinal_key(ordinal);
        if let Some(index) =
            self.cached_leaf(root, &key, |rows| matches!(rows, CachedRows::Documents(_)))
        {
            let leaf = self.touch(index);
            let CachedRows::Documents(rows) = &leaf.rows else {
                unreachable!("cache variant was checked")
            };
            return document_in_rows(rows, ordinal);
        }

        self.reserve_miss_slot();
        let descriptor = find_leaf(directory, root, &key)
            .await?
            .ok_or(IndexError::InvalidFormat("document ordinal outside run"))?;
        let rows = read_document_block(directory, &descriptor).await?;
        let document = document_in_rows(&rows, ordinal)?;
        self.insert(CachedLeaf {
            root_hash: root.hash,
            descriptor,
            rows: CachedRows::Documents(rows),
        });
        Ok(document)
    }

    pub(super) async fn typed<D: IndexDirectoryRead>(
        &mut self,
        directory: &D,
        view: &RunView,
        ordinal: u64,
    ) -> Result<TypedRow, IndexError> {
        let root = view
            .component_optional(ROWS_TAG)
            .ok_or(IndexError::InvalidFormat("missing typed component"))?;
        let key = ordinal_key(ordinal);
        if let Some(index) =
            self.cached_leaf(root, &key, |rows| matches!(rows, CachedRows::Typed(_)))
        {
            let leaf = self.touch(index);
            let CachedRows::Typed(rows) = &leaf.rows else {
                unreachable!("cache variant was checked")
            };
            return typed_in_rows(rows, ordinal);
        }

        self.reserve_miss_slot();
        let descriptor = find_leaf(directory, root, &key)
            .await?
            .ok_or(IndexError::InvalidFormat("missing typed ordinal"))?;
        let rows = read_typed_block(directory, &descriptor).await?;
        let row = typed_in_rows(&rows, ordinal)?;
        self.insert(CachedLeaf {
            root_hash: root.hash,
            descriptor,
            rows: CachedRows::Typed(rows),
        });
        Ok(row)
    }

    pub(super) async fn path<D: IndexDirectoryRead>(
        &mut self,
        directory: &D,
        root: &BlockDescriptor,
        path: &str,
    ) -> Result<Option<PathChange>, IndexError> {
        let key = path.as_bytes();
        if let Some(index) =
            self.cached_leaf(root, key, |rows| matches!(rows, CachedRows::Paths(_)))
        {
            let leaf = self.touch(index);
            let CachedRows::Paths(rows) = &leaf.rows else {
                unreachable!("cache variant was checked")
            };
            return Ok(path_in_rows(rows, path));
        }

        self.reserve_miss_slot();
        let Some(descriptor) = find_leaf(directory, root, key).await? else {
            return Ok(None);
        };
        let rows = read_path_block(directory, &descriptor).await?;
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
        self.leaves.back().expect("the touched leaf was reinserted")
    }

    fn insert(&mut self, leaf: CachedLeaf) {
        assert!(
            self.leaves.len() < MAX_CACHED_LEAVES,
            "a point-cache miss must reserve capacity before decoding"
        );
        self.leaves.push_back(leaf);
    }

    fn reserve_miss_slot(&mut self) {
        if self.leaves.len() == MAX_CACHED_LEAVES {
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

fn typed_in_rows(rows: &[TypedRow], ordinal: u64) -> Result<TypedRow, IndexError> {
    let index = rows
        .binary_search_by_key(&ordinal, |row| row.ordinal)
        .map_err(|_| IndexError::InvalidFormat("missing typed ordinal"))?;
    Ok(rows[index].clone())
}

fn path_in_rows(rows: &[PathChange], path: &str) -> Option<PathChange> {
    let index = rows
        .binary_search_by(|row| row.document.path.as_str().cmp(path))
        .ok()?;
    Some(rows[index].clone())
}
