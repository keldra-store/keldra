//! Immutable, segment-local document identities. These are derived metadata,
//! never a second authority for the source object or its version.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::IndexError;

use super::StableDocumentKey;

/// Disposable allocation ownership is deliberately excluded from semantic
/// equality. Clones share the lease and release it only with the last owner.
#[derive(Clone, Debug, Default)]
pub struct SegmentMemoryLease(Arc<std::sync::OnceLock<Arc<dyn Send + Sync + std::fmt::Debug>>>);

impl PartialEq for SegmentMemoryLease {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for SegmentMemoryLease {}

impl SegmentMemoryLease {
    pub fn attach(&self, lease: Arc<dyn Send + Sync + std::fmt::Debug>) -> bool {
        self.0.set(lease).is_ok()
    }
    pub fn is_attached(&self) -> bool {
        self.0.get().is_some()
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SegmentDocumentId(pub u32);

#[derive(Debug, Eq, PartialEq)]
pub struct SegmentDocumentTable {
    documents: Vec<[u8; 32]>,
    material_versions: Vec<u64>,
    version_bound: bool,
    identity: [u8; 32],
    memory_lease: SegmentMemoryLease,
}

impl Clone for SegmentDocumentTable {
    fn clone(&self) -> Self {
        Self {
            documents: self.documents.clone(),
            material_versions: self.material_versions.clone(),
            version_bound: self.version_bound,
            identity: self.identity,
            memory_lease: SegmentMemoryLease::default(),
        }
    }
}

impl Default for SegmentDocumentTable {
    fn default() -> Self {
        Self::new([]).expect("empty document table")
    }
}

impl SegmentDocumentTable {
    pub fn new(documents: impl IntoIterator<Item = StableDocumentKey>) -> Result<Self, IndexError> {
        Self::new_with_versions(documents.into_iter().map(|document| (document, 0)))
    }

    pub fn new_with_versions(
        documents: impl IntoIterator<Item = (StableDocumentKey, u64)>,
    ) -> Result<Self, IndexError> {
        let mut entries: Vec<_> = documents
            .into_iter()
            .map(|(key, version)| (key.bytes(), version))
            .collect();
        entries.sort_unstable();
        entries.dedup();
        if entries.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(IndexError::IntegrityViolation(
                "segment table contains multiple material versions for one object".into(),
            ));
        }
        let (documents, material_versions): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
        Self::from_sorted_columns(documents, material_versions)
    }

    /// Wire readers already receive canonical columns. Validate them directly;
    /// do not recollect tuples, sort, or allocate a second representation.
    pub(crate) fn from_sorted_columns(
        documents: Vec<[u8; 32]>,
        material_versions: Vec<u64>,
    ) -> Result<Self, IndexError> {
        if documents.len() != material_versions.len() {
            return Err(IndexError::Integrity);
        }
        if documents.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(IndexError::UnsortedRecords);
        }
        u32::try_from(documents.len()).map_err(|_| IndexError::OffsetOverflow)?;
        let mut identity = blake3::Hasher::new();
        identity.update(b"keldra-v1-segment-document-table\0");
        for (document, version) in documents.iter().zip(&material_versions) {
            identity.update(document);
            identity.update(&version.to_be_bytes());
        }
        let version_bound = material_versions.iter().all(|version| *version != 0);
        Ok(Self {
            documents,
            material_versions,
            version_bound,
            identity: *identity.finalize().as_bytes(),
            memory_lease: SegmentMemoryLease::default(),
        })
    }

    pub fn documents(&self) -> &[[u8; 32]] {
        &self.documents
    }
    pub fn identity(&self) -> [u8; 32] {
        self.identity
    }
    pub fn is_version_bound(&self) -> bool {
        self.version_bound
    }
    pub fn material_version(&self, id: SegmentDocumentId) -> Result<u64, IndexError> {
        self.material_versions
            .get(id.0 as usize)
            .copied()
            .ok_or(IndexError::Integrity)
    }
    pub fn has_memory_lease(&self) -> bool {
        self.memory_lease.is_attached()
    }
    pub fn attach_memory_lease(&self, lease: Arc<dyn Send + Sync + std::fmt::Debug>) -> bool {
        self.memory_lease.attach(lease)
    }

    pub fn id(&self, document: StableDocumentKey) -> Result<SegmentDocumentId, IndexError> {
        let index = self
            .documents
            .binary_search(&document.bytes())
            .map_err(|_| IndexError::Integrity)?;
        Ok(SegmentDocumentId(
            u32::try_from(index).map_err(|_| IndexError::OffsetOverflow)?,
        ))
    }

    pub fn lower_bound(&self, document: StableDocumentKey) -> SegmentDocumentId {
        SegmentDocumentId(
            self.documents
                .partition_point(|key| key < &document.bytes()) as u32,
        )
    }
    pub fn upper_bound(&self, document: StableDocumentKey) -> SegmentDocumentId {
        SegmentDocumentId(
            self.documents
                .partition_point(|key| key <= &document.bytes()) as u32,
        )
    }

    pub fn document(&self, id: SegmentDocumentId) -> Result<StableDocumentKey, IndexError> {
        StableDocumentKey::from_bytes(
            self.documents
                .get(id.0 as usize)
                .copied()
                .ok_or(IndexError::Integrity)?,
        )
    }

    pub(crate) fn key(&self, id: u32) -> Result<&[u8], IndexError> {
        self.documents
            .get(id as usize)
            .map(|key| key.as_slice())
            .ok_or(IndexError::Integrity)
    }

    pub fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.documents.capacity() * std::mem::size_of::<StableDocumentKey>()
            + self.material_versions.capacity() * 8
    }
}

/// A generation-specific visibility view over an immutable document table.
/// Publishing a replacement changes this mask, not immutable postings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentLiveDocuments {
    documents: Arc<SegmentDocumentTable>,
    words: LiveWords,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LiveWords {
    Dense(Vec<u64>),
    Sparse(BTreeMap<u32, u64>),
}

impl SegmentLiveDocuments {
    pub fn document_table_identity(&self) -> [u8; 32] {
        self.documents.identity()
    }
    pub fn none_live(documents: Arc<SegmentDocumentTable>) -> Self {
        let words = LiveWords::Sparse(BTreeMap::new());
        Self { documents, words }
    }

    pub fn set_live(&mut self, id: SegmentDocumentId) -> Result<(), IndexError> {
        self.documents.document(id)?;
        match &mut self.words {
            LiveWords::Dense(words) => words[id.0 as usize / 64] |= 1 << (id.0 % 64),
            LiveWords::Sparse(words) => *words.entry(id.0 / 64).or_default() |= 1 << (id.0 % 64),
        }
        Ok(())
    }
    pub fn all_live(documents: Arc<SegmentDocumentTable>) -> Self {
        let count = documents.documents.len();
        let mut words = vec![u64::MAX; count.div_ceil(64)];
        if count % 64 != 0 {
            if let Some(last) = words.last_mut() {
                *last = (1u64 << (count % 64)) - 1;
            }
        }
        Self {
            documents,
            words: LiveWords::Dense(words),
        }
    }

    pub fn is_live(&self, id: SegmentDocumentId) -> bool {
        self.word(id.0 / 64) & (1 << (id.0 % 64)) != 0
    }

    pub fn intersect(&mut self, other: &Self) -> Result<(), IndexError> {
        if self.document_table_identity() != other.document_table_identity() {
            return Err(IndexError::Integrity);
        }
        match &mut self.words {
            LiveWords::Dense(words) => {
                for (index, word) in words.iter_mut().enumerate() {
                    *word &= other.word(index as u32);
                }
            }
            LiveWords::Sparse(words) => words.retain(|index, word| {
                *word &= other.word(*index);
                *word != 0
            }),
        }
        Ok(())
    }

    pub fn union(&mut self, other: &Self) -> Result<(), IndexError> {
        if self.document_table_identity() != other.document_table_identity() {
            return Err(IndexError::Integrity);
        }
        match (&mut self.words, &other.words) {
            (LiveWords::Dense(words), _) => {
                for (index, word) in words.iter_mut().enumerate() {
                    *word |= other.word(index as u32);
                }
            }
            (LiveWords::Sparse(words), LiveWords::Sparse(other)) => {
                for (index, word) in other {
                    *words.entry(*index).or_default() |= word;
                }
            }
            (LiveWords::Sparse(words), LiveWords::Dense(other)) => {
                let mut dense = other.clone();
                for (index, word) in words.iter() {
                    dense[*index as usize] |= word;
                }
                self.words = LiveWords::Dense(dense);
            }
        }
        Ok(())
    }

    pub fn remove(&mut self, document: StableDocumentKey) -> Result<(), IndexError> {
        let id = self.documents.id(document)?;
        match &mut self.words {
            LiveWords::Dense(words) => words[id.0 as usize / 64] &= !(1 << (id.0 % 64)),
            LiveWords::Sparse(words) => {
                if let Some(word) = words.get_mut(&(id.0 / 64)) {
                    *word &= !(1 << (id.0 % 64));
                }
            }
        }
        Ok(())
    }

    fn word(&self, index: u32) -> u64 {
        match &self.words {
            LiveWords::Dense(words) => words.get(index as usize).copied().unwrap_or_default(),
            LiveWords::Sparse(words) => words.get(&index).copied().unwrap_or_default(),
        }
    }

    pub fn insertion_bytes(&self, id: SegmentDocumentId) -> Result<usize, IndexError> {
        self.documents.document(id)?;
        Ok(match &self.words {
            LiveWords::Sparse(words) if !words.contains_key(&(id.0 / 64)) => 512,
            _ => 0,
        })
    }

    pub fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match &self.words {
                LiveWords::Dense(words) => words.capacity() * 8,
                LiveWords::Sparse(words) => words.len() * 512,
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorted_columns_match_general_constructor_without_reallocation() {
        let identities = vec![[1; 32], [2; 32]];
        let versions = vec![7, 9];
        let identity_pointer = identities.as_ptr();
        let version_pointer = versions.as_ptr();
        let direct = SegmentDocumentTable::from_sorted_columns(identities, versions).unwrap();
        let general = SegmentDocumentTable::new_with_versions([
            (StableDocumentKey::from_bytes([2; 32]).unwrap(), 9),
            (StableDocumentKey::from_bytes([1; 32]).unwrap(), 7),
        ])
        .unwrap();
        assert_eq!(direct.identity(), general.identity());
        assert_eq!(direct.documents.as_ptr(), identity_pointer);
        assert_eq!(direct.material_versions.as_ptr(), version_pointer);
    }

    #[test]
    fn sorted_columns_reject_mismatch_duplicates_and_reordered_identities() {
        assert!(SegmentDocumentTable::from_sorted_columns(vec![[1; 32]], vec![]).is_err());
        assert!(
            SegmentDocumentTable::from_sorted_columns(vec![[1; 32], [1; 32]], vec![7, 7]).is_err()
        );
        assert!(
            SegmentDocumentTable::from_sorted_columns(vec![[2; 32], [1; 32]], vec![7, 7]).is_err()
        );
    }

    #[test]
    fn table_memory_lease_follows_final_arc_owner_and_not_deep_clone() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Debug)]
        struct Lease(Arc<AtomicUsize>);
        impl Drop for Lease {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicUsize::new(0));
        let table =
            Arc::new(SegmentDocumentTable::from_sorted_columns(vec![[1; 32]], vec![7]).unwrap());
        assert!(table.attach_memory_lease(Arc::new(Lease(dropped.clone()))));
        let shared = table.clone();
        let deep = table.as_ref().clone();
        assert!(shared.has_memory_lease());
        assert!(
            !deep.has_memory_lease(),
            "new column allocations need their own charge"
        );
        drop(table);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(shared);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        drop(deep);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
}
