//! One unpublished immutable-segment manifest candidate.

use super::*;
use crate::index_runtime::committed_view::{
    IndexCommitManifest, MAX_PENDING_ATOMIC_BATCHES, ManifestPhysicalOrder, PendingAtomicBatch,
};

pub(super) fn runtime_kind(kind: keldra_index::v4::IndexKind) -> IndexKind {
    match kind {
        keldra_index::v4::IndexKind::Path => IndexKind::Path,
        keldra_index::v4::IndexKind::MetadataFilter => IndexKind::MetadataFilter,
        keldra_index::v4::IndexKind::TypedJson => IndexKind::TypedJson,
        keldra_index::v4::IndexKind::FullText => IndexKind::FullText,
        keldra_index::v4::IndexKind::Vector => IndexKind::Vector,
        keldra_index::v4::IndexKind::Hybrid => IndexKind::Hybrid,
        keldra_index::v4::IndexKind::GitSource => IndexKind::GitSource,
        keldra_index::v4::IndexKind::Tensor => IndexKind::Tensor,
    }
}

pub(super) fn manifest_physical_order(schema: &Schema) -> Vec<ManifestPhysicalOrder> {
    schema
        .physical_order
        .iter()
        .map(|order| ManifestPhysicalOrder {
            field_id: order.field_id,
            descending: matches!(
                order.direction,
                keldra_index::v4::OrderDirection::Descending
            ),
        })
        .collect()
}

pub(super) struct CandidateCommit {
    pub(super) segments: Vec<SegmentDescriptor>,
    pub(super) locator_roots: Vec<LocatorRoot>,
    pub(super) pending_atomic_batches: Vec<PendingAtomicBatch>,
    pending_live_mask_invalidations: Vec<PendingLiveMaskInvalidation>,
    pub(super) next_sequence: u64,
    pub(super) diagnostics: IndexBuildDiagnostics,
}

impl Clone for CandidateCommit {
    fn clone(&self) -> Self {
        let mut pending_live_mask_invalidations = Vec::with_capacity(MAX_SEGMENTS_PER_COMMIT);
        pending_live_mask_invalidations
            .extend(self.pending_live_mask_invalidations.iter().cloned());
        Self {
            segments: self.segments.clone(),
            locator_roots: self.locator_roots.clone(),
            pending_atomic_batches: self.pending_atomic_batches.clone(),
            pending_live_mask_invalidations,
            next_sequence: self.next_sequence,
            diagnostics: self.diagnostics.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PendingLiveMaskInvalidation {
    pub(super) identity: SegmentIdentity,
    pub(super) ranges: Box<[DocIdRange]>,
}

impl CandidateCommit {
    pub(super) fn clone_resident_bytes(&self) -> Result<usize, Status> {
        let mut bytes = std::mem::size_of::<Self>()
            .checked_add(
                self.segments
                    .len()
                    .checked_mul(std::mem::size_of::<SegmentDescriptor>())
                    .ok_or_else(|| Status::resource_exhausted("candidate clone size overflow"))?,
            )
            .and_then(|bytes| {
                bytes.checked_add(
                    self.locator_roots
                        .len()
                        .checked_mul(std::mem::size_of::<LocatorRoot>())?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    self.pending_atomic_batches
                        .len()
                        .checked_mul(std::mem::size_of::<PendingAtomicBatch>())?,
                )
            })
            .and_then(|bytes| bytes.checked_add(self.pending_invalidation_resident_bytes().ok()?))
            .ok_or_else(|| Status::resource_exhausted("candidate clone size overflow"))?;
        for segment in &self.segments {
            bytes = add_pack_clone_bytes(bytes, &segment.packs)?;
            bytes = bytes
                .checked_add(
                    segment
                        .components
                        .len()
                        .checked_mul(std::mem::size_of::<keldra_index::v4::SegmentComponent>())
                        .ok_or_else(|| {
                            Status::resource_exhausted("candidate clone size overflow")
                        })?,
                )
                .ok_or_else(|| Status::resource_exhausted("candidate clone size overflow"))?;
        }
        for root in &self.locator_roots {
            if let LocatorPackOwnership::Standalone(packs) = &root.pack_ownership {
                bytes = add_pack_clone_bytes(bytes, packs)?;
            }
        }
        Ok(bytes)
    }

    pub(super) fn rebuild() -> Self {
        Self {
            segments: Vec::new(),
            locator_roots: Vec::new(),
            pending_atomic_batches: Vec::new(),
            pending_live_mask_invalidations: Vec::with_capacity(MAX_SEGMENTS_PER_COMMIT),
            next_sequence: 1,
            diagnostics: IndexBuildDiagnostics::default(),
        }
    }

    pub(super) fn incremental(current: &CommittedIndexView) -> Self {
        let next_sequence = current
            .manifest
            .locator_roots
            .last()
            .map_or(1, |root| root.sequence.saturating_add(1));
        Self {
            segments: current.manifest.segments.clone(),
            locator_roots: current.manifest.locator_roots.clone(),
            pending_atomic_batches: current.manifest.pending_atomic_batches.clone(),
            pending_live_mask_invalidations: Vec::with_capacity(MAX_SEGMENTS_PER_COMMIT),
            next_sequence,
            diagnostics: IndexBuildDiagnostics::default(),
        }
    }

    pub(super) fn from_rebuild_manifest(manifest: &IndexCommitManifest) -> Self {
        let next_sequence = manifest
            .locator_roots
            .last()
            .map_or(1, |root| root.sequence.saturating_add(1));
        Self {
            segments: manifest.segments.clone(),
            locator_roots: manifest.locator_roots.clone(),
            pending_atomic_batches: manifest.pending_atomic_batches.clone(),
            pending_live_mask_invalidations: Vec::with_capacity(MAX_SEGMENTS_PER_COMMIT),
            next_sequence,
            diagnostics: IndexBuildDiagnostics::default(),
        }
    }

    pub(super) fn contains_atomic_batch(
        &self,
        cursor: u64,
        bundle_hash: keldra_store::PreparedBundleHash,
    ) -> Result<bool, Status> {
        match self
            .pending_atomic_batches
            .binary_search_by_key(&cursor, |pending| pending.cursor)
        {
            Ok(index) if self.pending_atomic_batches[index].bundle_hash == bundle_hash => Ok(true),
            Ok(_) => Err(Status::data_loss(
                "one atomic cursor was observed with conflicting bundle hashes",
            )),
            Err(_) => Ok(false),
        }
    }

    pub(super) fn record_atomic_batch(
        &mut self,
        cursor: u64,
        bundle_hash: keldra_store::PreparedBundleHash,
    ) -> Result<(), Status> {
        if self.contains_atomic_batch(cursor, bundle_hash)? {
            return Ok(());
        }
        if self.pending_atomic_batches.len() >= MAX_PENDING_ATOMIC_BATCHES {
            return Err(Status::resource_exhausted(
                "pending atomic batch identity bound reached",
            ));
        }
        let index = self
            .pending_atomic_batches
            .binary_search_by_key(&cursor, |pending| pending.cursor)
            .expect_err("missing atomic cursor has an insertion position");
        self.pending_atomic_batches.insert(
            index,
            PendingAtomicBatch {
                cursor,
                bundle_hash,
            },
        );
        Ok(())
    }

    pub(super) fn prune_finalized_atomic_batches(&mut self, finalized_through: Option<u64>) {
        let Some(finalized_through) = finalized_through else {
            return;
        };
        self.pending_atomic_batches
            .retain(|pending| pending.cursor > finalized_through);
    }

    pub(super) fn allocate_sequence(&mut self) -> Result<u64, Status> {
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("path-locator sequence exhausted"))?;
        Ok(sequence)
    }

    pub(super) fn record_live_mask_invalidation(
        &mut self,
        segment_id: u64,
        ranges: &[DocIdRange],
        maximum_peak_bytes: usize,
    ) -> Result<(), Status> {
        let (identity, document_count) = self
            .segments
            .binary_search_by_key(&segment_id, |segment| segment.identity.segment_id)
            .ok()
            .map(|position| {
                let segment = &self.segments[position];
                (segment.identity, segment.document_count)
            })
            .ok_or_else(|| {
                Status::data_loss("live-mask invalidation names a missing candidate segment")
            })?;
        validate_invalidation_range_bounds(segment_id, document_count, ranges)?;
        let (_, peak_bytes) = self.live_mask_invalidation_peak_bytes(segment_id, ranges)?;
        if peak_bytes > maximum_peak_bytes {
            return Err(Status::resource_exhausted(
                "pending live-mask invalidations exhausted the builder workspace",
            ));
        }
        let existing_position = self
            .pending_live_mask_invalidations
            .binary_search_by_key(&segment_identity_key(identity), |pending| {
                segment_identity_key(pending.identity)
            })
            .ok();
        let existing = existing_position.map(|position| {
            self.pending_live_mask_invalidations[position]
                .ranges
                .as_ref()
        });
        let merged_capacity = existing
            .map_or(0, |ranges| ranges.len())
            .checked_add(ranges.len())
            .ok_or_else(|| {
                Status::resource_exhausted("pending live-mask invalidation count overflow")
            })?;
        if let Some(existing) = existing {
            let mut merged = Vec::with_capacity(merged_capacity);
            merged.extend_from_slice(existing);
            merged.extend_from_slice(ranges);
            // A retained page can replay a range already accumulated by an
            // earlier attempt. Overlap across those two validated sets is
            // idempotent; overlap within either locator result is corruption.
            normalize_invalidation_ranges(segment_id, document_count, &mut merged, true)?;
            self.pending_live_mask_invalidations[existing_position.expect("existing position")]
                .ranges = merged.into_boxed_slice();
        } else {
            if self.pending_live_mask_invalidations.len() >= MAX_SEGMENTS_PER_COMMIT {
                return Err(Status::resource_exhausted(
                    "pending live-mask invalidation segment bound reached",
                ));
            }
            let mut normalized = ranges.to_vec();
            normalize_invalidation_ranges(segment_id, document_count, &mut normalized, false)?;
            let position = self
                .pending_live_mask_invalidations
                .binary_search_by_key(&segment_identity_key(identity), |pending| {
                    segment_identity_key(pending.identity)
                })
                .expect_err("missing invalidation identity has an insertion position");
            self.pending_live_mask_invalidations.insert(
                position,
                PendingLiveMaskInvalidation {
                    identity,
                    ranges: normalized.into_boxed_slice(),
                },
            );
        }
        Ok(())
    }

    pub(super) fn live_mask_invalidation_peak_bytes(
        &self,
        segment_id: u64,
        ranges: &[DocIdRange],
    ) -> Result<(SegmentIdentity, usize), Status> {
        let identity = self
            .segments
            .binary_search_by_key(&segment_id, |segment| segment.identity.segment_id)
            .ok()
            .map(|position| self.segments[position].identity)
            .ok_or_else(|| {
                Status::data_loss("live-mask invalidation names a missing candidate segment")
            })?;
        let current_bytes = self.pending_invalidation_resident_bytes()?;
        let existing = self
            .pending_live_mask_invalidations
            .binary_search_by_key(&segment_identity_key(identity), |pending| {
                segment_identity_key(pending.identity)
            })
            .ok()
            .map(|position| {
                self.pending_live_mask_invalidations[position]
                    .ranges
                    .as_ref()
            });
        let merged_capacity = existing
            .map_or(0, |ranges| ranges.len())
            .checked_add(ranges.len())
            .ok_or_else(|| {
                Status::resource_exhausted("pending live-mask invalidation count overflow")
            })?;
        // Every candidate reserves the authoritative maximum segment count
        // up front, so insertion never has allocator-dependent outer-vector
        // growth. Charge two full inner destinations because exact-length box
        // conversion may temporarily retain the merge Vec while shrinking.
        let allocation = merged_capacity
            .checked_mul(std::mem::size_of::<DocIdRange>())
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or_else(|| {
                Status::resource_exhausted("pending live-mask invalidation size overflow")
            })?;
        let peak_bytes = current_bytes.checked_add(allocation).ok_or_else(|| {
            Status::resource_exhausted("pending live-mask invalidation size overflow")
        })?;
        Ok((identity, peak_bytes))
    }

    pub(super) fn pending_invalidation_resident_bytes(&self) -> Result<usize, Status> {
        self.pending_live_mask_invalidations.iter().try_fold(
            self.pending_live_mask_invalidations
                .capacity()
                .checked_mul(std::mem::size_of::<PendingLiveMaskInvalidation>())
                .ok_or_else(|| {
                    Status::resource_exhausted("pending live-mask invalidation size overflow")
                })?,
            |bytes, pending| {
                let range_bytes = pending
                    .ranges
                    .len()
                    .checked_mul(std::mem::size_of::<DocIdRange>())
                    .ok_or_else(|| {
                        Status::resource_exhausted("pending live-mask invalidation size overflow")
                    })?;
                bytes.checked_add(range_bytes).ok_or_else(|| {
                    Status::resource_exhausted("pending live-mask invalidation size overflow")
                })
            },
        )
    }

    pub(super) fn has_pending_live_mask_invalidations(&self) -> bool {
        !self.pending_live_mask_invalidations.is_empty()
    }

    pub(super) fn take_live_mask_invalidations(&mut self) -> Vec<PendingLiveMaskInvalidation> {
        std::mem::take(&mut self.pending_live_mask_invalidations)
    }

    pub(super) fn restore_live_mask_invalidations(
        &mut self,
        invalidations: Vec<PendingLiveMaskInvalidation>,
    ) {
        debug_assert!(self.pending_live_mask_invalidations.is_empty());
        self.pending_live_mask_invalidations = invalidations;
    }

    pub(super) fn locator_stream_roots(&self) -> Result<Vec<LocatorStreamRoot>, Status> {
        self.locator_roots
            .iter()
            .map(|root| self.locator_stream_root(root))
            .collect()
    }

    pub(super) fn locator_stream_root(
        &self,
        root: &LocatorRoot,
    ) -> Result<LocatorStreamRoot, Status> {
        let segment = self
            .segments
            .binary_search_by_key(&root.identity.segment_id, |segment| {
                segment.identity.segment_id
            })
            .ok()
            .map(|position| &self.segments[position]);
        let packs = match (&root.pack_ownership, segment) {
            (LocatorPackOwnership::Segment, Some(segment)) if segment.identity == root.identity => {
                segment.packs.clone()
            }
            (LocatorPackOwnership::Standalone(packs), None) if !packs.is_empty() => packs.clone(),
            (LocatorPackOwnership::Segment, None) => {
                return Err(Status::data_loss(
                    "segment-owned locator has no committed-view segment",
                ));
            }
            (LocatorPackOwnership::Segment, Some(_)) => {
                return Err(Status::data_loss(
                    "segment-owned locator identity differs from its committed-view segment",
                ));
            }
            (LocatorPackOwnership::Standalone(_), Some(_)) => {
                return Err(Status::data_loss(
                    "standalone locator duplicates a committed-view segment owner",
                ));
            }
            (LocatorPackOwnership::Standalone(_), None) => {
                return Err(Status::data_loss(
                    "standalone locator has no artifact pack table",
                ));
            }
        };
        Ok(LocatorStreamRoot {
            sequence: root.sequence,
            identity: root.identity,
            packs,
            artifact: root.artifact.clone(),
        })
    }
}

fn normalize_invalidation_ranges(
    segment_id: u64,
    document_count: u32,
    ranges: &mut Vec<DocIdRange>,
    allow_overlap: bool,
) -> Result<(), Status> {
    ranges.sort_by_key(|range| range.first_doc_id.get());
    let mut write = 0usize;
    for read in 0..ranges.len() {
        let current = ranges[read];
        if current.segment_id != segment_id || current.count == 0 {
            return Err(Status::data_loss(
                "path locator returned an invalid live DocId range",
            ));
        }
        let current_end = current
            .first_doc_id
            .get()
            .checked_add(current.count)
            .ok_or_else(|| Status::data_loss("path locator DocId range overflow"))?;
        if current_end > document_count {
            return Err(Status::data_loss(
                "path locator live DocId range exceeds its segment document count",
            ));
        }
        if write != 0 {
            let previous = &mut ranges[write - 1];
            let previous_end = previous
                .first_doc_id
                .get()
                .checked_add(previous.count)
                .ok_or_else(|| Status::data_loss("path locator DocId range overflow"))?;
            if current.first_doc_id.get() < previous_end && !allow_overlap {
                return Err(Status::data_loss(
                    "path locator returned overlapping live DocId ranges",
                ));
            }
            if current.first_doc_id.get() <= previous_end {
                previous.count = previous_end
                    .max(current_end)
                    .checked_sub(previous.first_doc_id.get())
                    .ok_or_else(|| Status::data_loss("path locator DocId range underflow"))?;
                continue;
            }
        }
        ranges[write] = current;
        write += 1;
    }
    ranges.truncate(write);
    Ok(())
}

fn segment_identity_key(identity: SegmentIdentity) -> (u64, u64, [u8; 32], u64) {
    (
        identity.index_id,
        identity.definition_version,
        identity.schema_fingerprint,
        identity.segment_id,
    )
}

fn validate_invalidation_range_bounds(
    segment_id: u64,
    document_count: u32,
    ranges: &[DocIdRange],
) -> Result<(), Status> {
    let mut previous_end = None;
    for range in ranges {
        if range.segment_id != segment_id || range.count == 0 {
            return Err(Status::data_loss(
                "path locator returned an invalid live DocId range",
            ));
        }
        let end = range
            .first_doc_id
            .get()
            .checked_add(range.count)
            .ok_or_else(|| Status::data_loss("path locator DocId range overflow"))?;
        if end > document_count {
            return Err(Status::data_loss(
                "path locator live DocId range exceeds its segment document count",
            ));
        }
        if previous_end.is_some_and(|previous| range.first_doc_id.get() < previous) {
            return Err(Status::data_loss(
                "path locator returned unsorted or overlapping live DocId ranges",
            ));
        }
        previous_end = Some(end);
    }
    Ok(())
}

fn add_pack_clone_bytes(
    initial: usize,
    packs: &[keldra_index::v4::ArtifactPackReference],
) -> Result<usize, Status> {
    packs.iter().try_fold(
        initial
            .checked_add(
                packs
                    .len()
                    .checked_mul(std::mem::size_of::<keldra_index::v4::ArtifactPackReference>())
                    .ok_or_else(|| Status::resource_exhausted("candidate clone size overflow"))?,
            )
            .ok_or_else(|| Status::resource_exhausted("candidate clone size overflow"))?,
        |bytes, pack| {
            bytes
                .checked_add(pack.path.len())
                .ok_or_else(|| Status::resource_exhausted("candidate clone size overflow"))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(segment_id: u64, first: u32, count: u32) -> DocIdRange {
        DocIdRange {
            segment_id,
            first_doc_id: keldra_index::v4::DocId::new(first),
            count,
        }
    }

    fn candidate(identity: SegmentIdentity) -> CandidateCommit {
        let mut candidate = CandidateCommit::rebuild();
        candidate.segments.push(SegmentDescriptor {
            identity,
            document_count: 32,
            live_document_count: 32,
            packs: Vec::new(),
            components: Vec::new(),
            encoded_bytes: 0,
            logical_bytes: 0,
        });
        candidate
    }

    #[test]
    fn repeated_same_segment_invalidations_form_one_exact_rewrite_unit() {
        let identity = SegmentIdentity::new(3, 5, [7; 32], 11).unwrap();
        let mut candidate = candidate(identity);
        candidate
            .record_live_mask_invalidation(
                identity.segment_id,
                &[
                    range(identity.segment_id, 2, 3),
                    range(identity.segment_id, 8, 2),
                ],
                usize::MAX,
            )
            .unwrap();
        candidate
            .record_live_mask_invalidation(
                identity.segment_id,
                &[
                    range(identity.segment_id, 2, 3),
                    range(identity.segment_id, 5, 4),
                ],
                usize::MAX,
            )
            .unwrap();

        let pending = candidate.take_live_mask_invalidations();
        assert_eq!(pending.len(), 1, "one segment produces one rewrite pack");
        assert_eq!(pending[0].identity, identity);
        assert_eq!(
            pending[0].ranges.as_ref(),
            &[range(identity.segment_id, 2, 8)]
        );
        assert!(candidate.take_live_mask_invalidations().is_empty());
    }

    #[test]
    fn retained_candidate_clone_replays_the_exact_pending_invalidation() {
        let identity = SegmentIdentity::new(3, 5, [7; 32], 11).unwrap();
        let mut candidate = candidate(identity);
        candidate
            .record_live_mask_invalidation(
                identity.segment_id,
                &[range(identity.segment_id, 4, 2)],
                usize::MAX,
            )
            .unwrap();
        let mut retained_retry = candidate.clone();

        let first = candidate.take_live_mask_invalidations();
        let replay = retained_retry.take_live_mask_invalidations();
        assert_eq!(first, replay);
        assert_eq!(replay[0].identity, identity);
        assert_eq!(
            replay[0].ranges.as_ref(),
            &[range(identity.segment_id, 4, 2)]
        );
    }

    #[test]
    fn pending_invalidation_rejects_allocation_beyond_the_builder_charge() {
        let identity = SegmentIdentity::new(3, 5, [7; 32], 11).unwrap();
        let mut candidate = candidate(identity);
        let error = candidate
            .record_live_mask_invalidation(
                identity.segment_id,
                &[range(identity.segment_id, 4, 2)],
                0,
            )
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(!candidate.has_pending_live_mask_invalidations());
    }

    #[test]
    fn one_locator_result_cannot_hide_overlapping_live_ranges() {
        let identity = SegmentIdentity::new(3, 5, [7; 32], 11).unwrap();
        let mut candidate = candidate(identity);
        let error = candidate
            .record_live_mask_invalidation(
                identity.segment_id,
                &[
                    range(identity.segment_id, 4, 4),
                    range(identity.segment_id, 6, 3),
                ],
                usize::MAX,
            )
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
        assert!(!candidate.has_pending_live_mask_invalidations());
    }

    #[test]
    fn live_range_cannot_exceed_segment_document_count() {
        let identity = SegmentIdentity::new(3, 5, [7; 32], 11).unwrap();
        let mut candidate = candidate(identity);
        let error = candidate
            .record_live_mask_invalidation(
                identity.segment_id,
                &[range(identity.segment_id, 31, 2)],
                usize::MAX,
            )
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
        assert!(!candidate.has_pending_live_mask_invalidations());
    }

    #[test]
    fn post_drain_ceiling_excludes_the_materialized_pending_charge() {
        let identity = SegmentIdentity::new(3, 5, [7; 32], 11).unwrap();
        let mut candidate = candidate(identity);
        candidate
            .record_live_mask_invalidation(
                identity.segment_id,
                &[range(identity.segment_id, 4, 2)],
                usize::MAX,
            )
            .unwrap();
        let workspace = std::mem::size_of::<DocIdRange>();
        let stale_limit = candidate
            .pending_invalidation_resident_bytes()
            .unwrap()
            .checked_add(workspace)
            .unwrap();
        let original_capacity = candidate.pending_live_mask_invalidations.capacity();
        let mut materialized = candidate.take_live_mask_invalidations();
        materialized.clear();
        candidate.restore_live_mask_invalidations(materialized);
        let post_drain_limit = candidate
            .pending_invalidation_resident_bytes()
            .unwrap()
            .checked_add(workspace)
            .unwrap();

        assert!(post_drain_limit < stale_limit);
        assert_eq!(
            candidate.pending_live_mask_invalidations.capacity(),
            original_capacity,
            "successful drain reuses the fixed outer allocation"
        );
    }
}
