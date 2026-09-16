//! Segment-local candidates retain their immutable reader identity until the
//! bounded cross-segment join and pinned-current admission boundary.

use std::ops::Deref;

use super::super::query_blocks::DenseSegmentPoint;
use super::super::{DenseSegmentPosting, SegmentDocumentTable, SegmentLiveDocuments};
use super::*;

pub(super) struct DenseQueryPoint {
    pub(super) documents: Arc<SegmentDocumentTable>,
    pub(super) point: DenseSegmentPoint,
}

#[derive(Clone, Debug)]
pub(super) struct DenseQueryPosting {
    pub(super) documents: Arc<SegmentDocumentTable>,
    pub(super) posting: DenseSegmentPosting,
}

pub(super) trait SegmentIdentity {
    fn documents(&self) -> &Arc<SegmentDocumentTable>;
    fn local_id(&self) -> super::super::SegmentDocumentId;
}

impl SegmentIdentity for DenseQueryPosting {
    fn documents(&self) -> &Arc<SegmentDocumentTable> {
        &self.documents
    }
    fn local_id(&self) -> super::super::SegmentDocumentId {
        self.posting.document
    }
}

#[derive(Clone)]
pub(super) struct SegmentCandidateIdentity {
    pub(super) documents: Arc<SegmentDocumentTable>,
    pub(super) local_id: super::super::SegmentDocumentId,
}

impl SegmentIdentity for SegmentCandidateIdentity {
    fn documents(&self) -> &Arc<SegmentDocumentTable> {
        &self.documents
    }
    fn local_id(&self) -> super::super::SegmentDocumentId {
        self.local_id
    }
}

impl Deref for DenseQueryPosting {
    type Target = DenseSegmentPosting;
    fn deref(&self) -> &Self::Target {
        &self.posting
    }
}

impl DenseQueryPosting {
    pub(super) fn document(&self) -> Result<StableDocumentKey, IndexError> {
        self.documents.document(self.posting.document)
    }
}

/// Masks combine directly within an immutable segment. Different segments
/// require an exact stable-identity AND material-version join: the same object
/// revision can legitimately occur in different shared-recipe backfills.
pub(super) struct DenseBooleanMask {
    segments: BTreeMap<[u8; 32], SegmentLiveDocuments>,
    charged_bytes: usize,
}

impl DenseBooleanMask {
    pub(super) fn collect<'a, P: SegmentIdentity + 'a>(
        postings: impl IntoIterator<Item = &'a P>,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<Self, IndexError> {
        let mut segments = BTreeMap::new();
        let mut charged_bytes = 0usize;
        for posting in postings {
            let identity = posting.documents().identity();
            if !segments.contains_key(&identity) {
                let bytes = std::mem::size_of::<([u8; 32], SegmentLiveDocuments)>()
                    .checked_add(4096)
                    .ok_or(IndexError::OffsetOverflow)?;
                budget.reserve_heap(credits, bytes)?;
                charged_bytes = charged_bytes
                    .checked_add(bytes)
                    .ok_or(IndexError::OffsetOverflow)?;
                segments.insert(
                    identity,
                    SegmentLiveDocuments::none_live(posting.documents().clone()),
                );
            }
            let mask = segments.get_mut(&identity).ok_or(IndexError::Integrity)?;
            let bytes = mask.insertion_bytes(posting.local_id())?;
            budget.reserve_heap(credits, bytes)?;
            charged_bytes = charged_bytes
                .checked_add(bytes)
                .ok_or(IndexError::OffsetOverflow)?;
            mask.set_live(posting.local_id())?;
        }
        Ok(Self {
            segments,
            charged_bytes,
        })
    }

    pub(super) fn contains<P: SegmentIdentity>(&self, posting: &P) -> bool {
        self.segments
            .get(&posting.documents().identity())
            .is_some_and(|mask| mask.is_live(posting.local_id()))
    }

    pub(super) fn release(
        self,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<(), IndexError> {
        let bytes = self.charged_bytes;
        drop(self);
        budget.release_heap(credits, bytes)
    }
}

#[derive(Clone)]
pub(super) struct QueryMaterialCandidate {
    pub(super) material_source_version: u64,
    pub(super) segment: Option<SegmentCandidateIdentity>,
}

impl From<DenseQueryPosting> for QueryMaterialCandidate {
    fn from(posting: DenseQueryPosting) -> Self {
        Self {
            material_source_version: posting.material_source_version,
            segment: Some(SegmentCandidateIdentity {
                documents: posting.documents,
                local_id: posting.posting.document,
            }),
        }
    }
}

impl QueryMaterialCandidate {
    pub(super) fn detached(version: u64) -> Self {
        Self {
            material_source_version: version,
            segment: None,
        }
    }
}

#[derive(Default)]
pub(super) struct DenseCandidateSet {
    pub(super) entries: BTreeMap<StableDocumentKey, QueryMaterialCandidate>,
}

pub(super) const fn candidate_set_entry_bytes() -> usize {
    2 * std::mem::size_of::<(StableDocumentKey, QueryMaterialCandidate)>() + 96
}
pub(super) fn candidate_set_bytes(count: usize) -> Result<usize, IndexError> {
    count
        .checked_mul(candidate_set_entry_bytes())
        .and_then(|bytes| bytes.checked_add(if count == 0 { 0 } else { 2048 }))
        .ok_or(IndexError::OffsetOverflow)
}
pub(super) fn document_key_set_bytes(count: usize) -> Result<usize, IndexError> {
    count
        .checked_mul(80)
        .and_then(|bytes| bytes.checked_add(if count == 0 { 0 } else { 512 }))
        .ok_or(IndexError::OffsetOverflow)
}

impl FromIterator<StableDocumentKey> for DenseCandidateSet {
    fn from_iter<T: IntoIterator<Item = StableDocumentKey>>(iter: T) -> Self {
        Self {
            entries: iter
                .into_iter()
                .map(|key| (key, QueryMaterialCandidate::detached(0)))
                .collect(),
        }
    }
}

impl DenseCandidateSet {
    pub(super) fn new() -> Self {
        Self::default()
    }
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }
    pub(super) fn contains(&self, document: &StableDocumentKey) -> bool {
        self.entries.contains_key(document)
    }

    pub(super) fn intersect(
        &mut self,
        other: Self,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<(), IndexError> {
        let before = self.len();
        let masks = DenseBooleanMask::collect(
            other
                .entries
                .values()
                .filter_map(|candidate| candidate.segment.as_ref()),
            credits,
            budget,
        )?;
        self.entries.retain(|document, candidate| {
            candidate
                .segment
                .as_ref()
                .is_some_and(|segment| masks.contains(segment))
                || other.entries.get(document).is_some_and(|other| {
                    candidate.material_source_version == 0
                        || other.material_source_version == 0
                        || candidate.material_source_version == other.material_source_version
                })
        });
        masks.release(credits, budget)?;
        other.release(credits, budget)?;
        if self.entries.is_empty() {
            self.entries.clear();
        }
        budget.release_heap(
            credits,
            candidate_set_bytes(before)?
                .checked_sub(candidate_set_bytes(self.len())?)
                .ok_or(IndexError::Integrity)?,
        )
    }

    pub(super) fn union(
        &mut self,
        other: Self,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<usize, IndexError> {
        let consumed_bytes = candidate_set_bytes(other.len())?;
        let masks = DenseBooleanMask::collect(
            self.entries
                .values()
                .filter_map(|candidate| candidate.segment.as_ref()),
            credits,
            budget,
        )?;
        let mut added = 0usize;
        for (document, candidate) in other.entries {
            if candidate
                .segment
                .as_ref()
                .is_some_and(|segment| masks.contains(segment))
            {
                continue;
            }
            let empty = self.entries.is_empty();
            match self.entries.entry(document) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    budget.reserve_heap(
                        credits,
                        candidate_set_entry_bytes() + if empty { 2048 } else { 0 },
                    )?;
                    entry.insert(candidate);
                    added += 1;
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if candidate.material_source_version > entry.get().material_source_version {
                        entry.insert(candidate);
                    }
                }
            }
        }
        masks.release(credits, budget)?;
        budget.release_heap(credits, consumed_bytes)?;
        Ok(added)
    }

    pub(super) fn into_document_keys(
        self,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<BTreeSet<StableDocumentKey>, IndexError> {
        budget.reserve_heap(credits, document_key_set_bytes(self.len())?)?;
        let bytes = candidate_set_bytes(self.len())?;
        let keys = self.entries.into_keys().collect();
        budget.release_heap(credits, bytes)?;
        Ok(keys)
    }

    pub(super) fn release(
        self,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<(), IndexError> {
        let bytes = candidate_set_bytes(self.len())?;
        drop(self);
        budget.release_heap(credits, bytes)
    }
}

pub(super) fn intersect_term_candidates(
    postings: &TermPostingMap,
    terms: &[ScalarValue],
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<BTreeMap<StableDocumentKey, QueryMaterialCandidate>, IndexError> {
    let Some(first) = terms.first() else {
        return Ok(BTreeMap::new());
    };
    let first_count = postings.get(first).map_or(0, BTreeMap::len);
    budget.reserve_heap(credits, candidate_set_bytes(first_count)?)?;
    let mut found = postings
        .get(first)
        .into_iter()
        .flatten()
        .filter_map(|(document, (_, posting))| {
            posting
                .live
                .then_some((*document, QueryMaterialCandidate::from(posting.clone())))
        })
        .collect::<BTreeMap<_, _>>();
    budget.release_heap(
        credits,
        candidate_set_bytes(first_count)?
            .checked_sub(candidate_set_bytes(found.len())?)
            .ok_or(IndexError::Integrity)?,
    )?;
    for term in &terms[1..] {
        let next = postings.get(term);
        let masks = DenseBooleanMask::collect(
            next.into_iter().flat_map(|entries| {
                entries
                    .values()
                    .filter_map(|(_, posting)| posting.live.then_some(posting))
            }),
            credits,
            budget,
        )?;
        let before = found.len();
        found.retain(|document, candidate| {
            let Some(segment) = &candidate.segment else {
                return false;
            };
            // Same-table membership is an exact local-ID/material-version
            // intersection. Only cross-table aliases require the stable join.
            masks.contains(segment)
                || next
                    .and_then(|entries| entries.get(document))
                    .is_some_and(|(_, posting)| {
                        posting.live
                            && posting.material_source_version == candidate.material_source_version
                    })
        });
        if found.is_empty() {
            found.clear();
        }
        budget.release_heap(
            credits,
            candidate_set_bytes(before)?
                .checked_sub(candidate_set_bytes(found.len())?)
                .ok_or(IndexError::Integrity)?,
        )?;
        masks.release(credits, budget)?;
        if found.is_empty() {
            break;
        }
    }
    Ok(found)
}

#[cfg(test)]
#[path = "query_executor_dense_tests.rs"]
mod tests;
