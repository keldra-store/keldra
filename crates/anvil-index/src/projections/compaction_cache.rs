//! Bounded decoded-leaf reuse for projection compaction point lookups.

use std::collections::VecDeque;

use crate::run::{RunView, find_leaf};
use crate::segment::{DOCUMENTS_TAG, DocumentRecord, read_document_block_parallel};
use crate::{BlockDescriptor, DocumentRef, IndexDirectoryRead, IndexError};

use super::parallel_compaction::read_projection_block_parallel;
use super::{OrdinalRow, ProjectionPayload, RECORDS_TAG, ordinal_key};

// Six source leaves plus four path cursors leave two decoded-allocation slots
// for the writer's retained output batch and an incoming moved row during a
// flush.
const MAX_SOURCE_PAYLOAD_LEAVES: usize = 6;

enum CachedRows<T> {
    Documents(Vec<DocumentRecord>),
    Projections(Vec<CachedProjectionRow<T>>),
}

struct CachedProjectionRow<T> {
    ordinal: u64,
    payload: Option<T>,
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
    T: ProjectionPayload + Send + 'static,
{
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
            let CachedRows::Projections(rows) = &mut self.touch(index).rows else {
                unreachable!("cache variant was checked")
            };
            return take_projection_in_rows(rows, ordinal);
        }
        self.reserve_miss_slot();
        let descriptor = find_leaf(directory, root, &key)
            .await?
            .ok_or(IndexError::InvalidFormat("missing projection ordinal"))?;
        let mut rows = cache_projection_rows(
            read_projection_block_parallel(directory, &descriptor, executor, progress).await?,
        );
        let projection = take_projection_in_rows(&mut rows, ordinal)?;
        self.insert(CachedLeaf {
            root_hash: root.hash,
            descriptor,
            rows: CachedRows::Projections(rows),
        });
        Ok(projection)
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

    fn touch(&mut self, index: usize) -> &mut CachedLeaf<T> {
        let leaf = self
            .leaves
            .remove(index)
            .expect("cache index came from this deque");
        self.leaves.push_back(leaf);
        self.leaves.back_mut().expect("touched leaf was reinserted")
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

fn cache_projection_rows<T>(rows: Vec<OrdinalRow<T>>) -> Vec<CachedProjectionRow<T>> {
    rows.into_iter()
        .map(|row| CachedProjectionRow {
            ordinal: row.ordinal,
            payload: Some(row.payload),
        })
        .collect()
}

fn take_projection_in_rows<T>(
    rows: &mut [CachedProjectionRow<T>],
    ordinal: u64,
) -> Result<T, IndexError> {
    let index = rows
        .binary_search_by_key(&ordinal, |row| row.ordinal)
        .map_err(|_| IndexError::InvalidFormat("missing projection ordinal"))?;
    rows[index].payload.take().ok_or(IndexError::InvalidFormat(
        "projection ordinal already consumed",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_cache_covers_the_selected_source_payload_working_set() {
        let source = ProjectionPointCache::<super::super::GitPayload>::default();

        assert_eq!(source.max_leaves, 6);
    }

    #[test]
    fn projection_payload_is_consumed_once_without_a_clone_bound() {
        struct NonClonePayload(Vec<u8>);

        let expected = vec![1, 2, 3];
        let expected_allocation = expected.as_ptr();
        let mut rows = vec![CachedProjectionRow {
            ordinal: 9,
            payload: Some(NonClonePayload(expected)),
        }];

        let payload = take_projection_in_rows(&mut rows, 9).unwrap();
        assert_eq!(payload.0.as_ptr(), expected_allocation);
        assert!(matches!(
            take_projection_in_rows(&mut rows, 9),
            Err(IndexError::InvalidFormat(
                "projection ordinal already consumed"
            ))
        ));
    }
}
