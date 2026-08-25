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

#[derive(Clone)]
pub(super) struct CandidateCommit {
    pub(super) segments: Vec<SegmentDescriptor>,
    pub(super) locator_roots: Vec<LocatorRoot>,
    pub(super) pending_atomic_batches: Vec<PendingAtomicBatch>,
    pub(super) next_sequence: u64,
    pub(super) diagnostics: IndexBuildDiagnostics,
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
