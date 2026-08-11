//! Bounded decoded-leaf reuse for vector compaction point lookups.

use std::collections::VecDeque;

use crate::compaction::{CompactionExecutor, CompactionProgress};
use crate::run::{RunView, find_leaf};
use crate::segment::{DOCUMENTS_TAG, DocumentRecord, read_document_block_parallel};
use crate::{BlockDescriptor, DocumentRef, IndexDirectoryRead, IndexError};

use super::{VectorDefinition, VectorRow, ordinal_key, read_vector_block_parallel};

// Six source leaves plus four path cursors leave two decoded-allocation slots
// for the writer's retained output batch and an incoming moved row during a
// flush.
const MAX_CACHED_LEAVES: usize = 6;

enum CachedRows {
    Documents(Vec<DocumentRecord>),
    Vectors(Vec<CachedVectorRow>),
}

struct CachedVectorRow {
    ordinal: u64,
    values: Option<Vec<f32>>,
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
            let CachedRows::Vectors(rows) = &mut self.touch(index).rows else {
                unreachable!("cache variant was checked")
            };
            return take_vector_in_rows(rows, ordinal);
        }

        self.reserve_miss_slot();
        let descriptor = find_leaf(directory, root, &key)
            .await?
            .ok_or(IndexError::InvalidFormat("missing vector ordinal"))?;
        let mut rows = cache_vector_rows(
            read_vector_block_parallel(directory, &descriptor, definition, executor, progress)
                .await?,
        );
        let values = take_vector_in_rows(&mut rows, ordinal)?;
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

    fn touch(&mut self, index: usize) -> &mut CachedLeaf {
        let leaf = self
            .leaves
            .remove(index)
            .expect("cache index came from this deque");
        self.leaves.push_back(leaf);
        self.leaves.back_mut().expect("touched leaf was reinserted")
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

fn cache_vector_rows(rows: Vec<VectorRow>) -> Vec<CachedVectorRow> {
    rows.into_iter()
        .map(|row| CachedVectorRow {
            ordinal: row.ordinal,
            values: Some(row.values),
        })
        .collect()
}

fn take_vector_in_rows(rows: &mut [CachedVectorRow], ordinal: u64) -> Result<Vec<f32>, IndexError> {
    let index = rows
        .binary_search_by_key(&ordinal, |row| row.ordinal)
        .map_err(|_| IndexError::InvalidFormat("missing vector ordinal"))?;
    rows[index]
        .values
        .take()
        .ok_or(IndexError::InvalidFormat("vector ordinal already consumed"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ComponentCodec, IndexKind};

    #[test]
    fn point_cache_reserves_two_moved_payload_slots() {
        let mut cache = VectorCompactionPointCache::default();
        for value in 0..(MAX_CACHED_LEAVES + 3) {
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
            assert!(cache.leaves.len() <= MAX_CACHED_LEAVES);
        }
        assert_eq!(MAX_CACHED_LEAVES, 6);
        assert_eq!(cache.leaves.len(), 6);
        assert_eq!(cache.leaves.front().unwrap().root_hash, [3; 32]);
    }

    #[test]
    fn vector_payload_is_consumed_exactly_once_without_cloning() {
        let expected = vec![1.0, 2.0, 3.0];
        let expected_allocation = expected.as_ptr();
        let mut rows = cache_vector_rows(vec![VectorRow {
            ordinal: 7,
            values: expected,
        }]);

        let values = take_vector_in_rows(&mut rows, 7).unwrap();
        assert_eq!(values.as_ptr(), expected_allocation);
        assert!(matches!(
            take_vector_in_rows(&mut rows, 7),
            Err(IndexError::InvalidFormat("vector ordinal already consumed"))
        ));
    }

    fn descriptor(value: u8) -> BlockDescriptor {
        BlockDescriptor {
            kind: IndexKind::Vector,
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
