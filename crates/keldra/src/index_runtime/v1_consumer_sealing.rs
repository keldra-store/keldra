//! Writer-owned promises derived before retaining the next replacement batch.
use super::*;

// Existing right-spine loads are bounded to this exact public-page read size.
const STREAM_PAGE_BYTES: usize = 32 * 1024;

/// Append trees split at half-full fanout; each level was created by at least
/// two children. Historical sequence, unlike live run count, remains a safe
/// height bound after compaction removes entries. Include one possible split.
fn spine_pages(sequence: u64) -> usize {
    if sequence == 0 {
        0
    } else {
        (u64::BITS - sequence.leading_zeros()) as usize + 1
    }
}

// Greedy bounded children have adjacent encoded sizes whose sum exceeds the
// pack capacity. Resident entry admission also bounds their encoded headers,
// so at most two extra children per capacity can be emitted. Each extra append
// path-copies at most two pages per level, including a possible root split.
pub(super) fn split_component_metadata_bound(
    resident_bytes: usize,
    historical_sequence: u64,
) -> Result<usize, Status> {
    let extra = resident_bytes / (keldra_index::v1::ARTIFACT_PACK_MAX_BYTES / 2);
    if extra == 0 {
        return Ok(0);
    }
    let sequence =
        historical_sequence
            .checked_add(u64::try_from(extra).map_err(|_| {
                Status::resource_exhausted("v1 split child count admission overflow")
            })?)
            .and_then(|sequence| sequence.checked_add(1))
            .ok_or_else(|| Status::resource_exhausted("v1 split sequence admission overflow"))?;
    let pages = extra
        .checked_mul(spine_pages(sequence))
        .and_then(|pages| pages.checked_mul(2 * STREAM_PAGE_BYTES))
        .and_then(|bytes| bytes.checked_mul(3))
        .ok_or_else(|| Status::resource_exhausted("v1 split metadata admission overflow"))?;
    // Extra native descriptors, pack references and their serialized paths
    // coexist with the encoded pages. Production pack paths have fixed widths.
    let pack_path = "_keldra/index-projections/v1/".len() + 64 + "/artifacts/packs/".len() + 64;
    let descriptor = std::mem::size_of::<keldra_index::v1::PackedComponentDelta>()
        + std::mem::size_of::<keldra_index::v1::ComponentSegmentDescriptor>()
        + std::mem::size_of::<keldra_index::v1::ArtifactPackReference>()
        + std::mem::size_of::<keldra_index::v1::SealedComponentDelta>()
        + 2 * pack_path
        + 256;
    extra
        .checked_mul(descriptor)
        .and_then(|bytes| pages.checked_add(bytes))
        .ok_or_else(|| Status::resource_exhausted("v1 split descriptor admission overflow"))
}

pub(super) fn successor_component_sequence_bound(writer: &Writer) -> Result<u64, Status> {
    let sequence = writer.current.as_ref().map_or(0, |current| {
        current
            .generation
            .roots
            .iter()
            .map(|root| root.last_sequence)
            .max()
            .unwrap_or(0)
    });
    let extra = writer.pending_projected_encoded_bytes
        / (keldra_index::v1::ARTIFACT_PACK_MAX_BYTES as u64 / 2);
    sequence
        .checked_add(extra)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| Status::resource_exhausted("v1 successor split sequence overflow"))
}

pub(super) fn spine_preload_bound(
    current: Option<&LoadedV1ProjectionGeneration>,
) -> Result<usize, Status> {
    let Some(current) = current else {
        return Ok(1);
    };
    let pages = current.generation.roots.iter().try_fold(
        spine_pages(current.generation.query_stream_root.last_sequence),
        |pages, root| {
            pages
                .checked_add(spine_pages(root.last_sequence))
                .ok_or_else(|| Status::resource_exhausted("v1 spine height admission overflow"))
        },
    )?;
    pages
        .checked_mul(STREAM_PAGE_BYTES)
        .map(|bytes| bytes.max(1))
        .ok_or_else(|| Status::resource_exhausted("v1 spine byte admission overflow"))
}

// A writer has one immutable physical recipe. Extraction emits this recipe's
// membership only; native field columns are query blocks, not canonical Field
// streams. Consequently this is the exact union across every document in a cut.
fn catalog_components(
    recipe: &PhysicalCatalogRecipe,
) -> Result<[keldra_index::v1::ComponentIdentity; 3], Status> {
    use keldra_index::v1::ComponentIdentity;
    Ok([
        ComponentIdentity::DocumentHead,
        ComponentIdentity::SourceRecords,
        ComponentIdentity::Membership(
            keldra_index::v1::RecipeIdentity::new(recipe.membership_recipe)
                .map_err(index_status)?,
        ),
    ])
}

pub(super) fn publication_metadata_bound(
    writer: &Writer,
    successor: bool,
) -> Result<usize, Status> {
    let roots = writer
        .current
        .as_ref()
        .map(|current| current.generation.roots.as_slice())
        .unwrap_or(&[]);
    let components = catalog_components(&writer.recipe)?;
    let missing = components
        .iter()
        .filter(|component| !roots.iter().any(|root| &root.component == *component))
        .count();
    let fresh = roots
        .len()
        .checked_add(missing)
        .and_then(|count| count.checked_add(1))
        .and_then(|count| count.checked_mul(2 * STREAM_PAGE_BYTES))
        .ok_or_else(|| Status::resource_exhausted("v1 fresh page admission overflow"))?;
    let preload = if successor {
        let query_sequence = writer.current.as_ref().map_or(0, |current| {
            current.generation.query_stream_root.last_sequence
        });
        let successor_sequence = successor_component_sequence_bound(writer)?
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("v1 successor append sequence overflow"))?;
        let pages = roots.iter().try_fold(
            spine_pages(query_sequence.saturating_add(2)),
            |pages, _root| {
                pages
                    .checked_add(spine_pages(successor_sequence))
                    .ok_or_else(|| {
                        Status::resource_exhausted("v1 successor spine admission overflow")
                    })
            },
        )?;
        pages
            .checked_add(missing * spine_pages(successor_sequence))
            .and_then(|pages| pages.checked_mul(STREAM_PAGE_BYTES))
            .ok_or_else(|| Status::resource_exhausted("v1 successor spine admission overflow"))?
    } else {
        spine_preload_bound(writer.current.as_ref())?
    };
    preload
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(fresh))
        .ok_or_else(|| Status::resource_exhausted("v1 metadata peak admission overflow"))
}

pub(super) fn reserve_sealing_progress<'a>(
    writer: &'a mut Writer,
    incoming: impl IntoIterator<Item = &'a PreparedQueryMutationBatch>,
    sources: impl IntoIterator<Item = (&'a str, &'a [keldra_index::v1::ProjectedDocumentState])>,
    credits: &IndexingMemoryCredits,
) -> Result<(), Status> {
    let mut components = 0usize;
    for (path, states) in sources {
        components = components
            .checked_add(
                keldra_index::v1::ProjectionMutationBuffer::source_replacement_admission_bytes(
                    path, states,
                )
                .map_err(index_status)?,
            )
            .ok_or_else(|| Status::resource_exhausted("v1 component admission overflow"))?;
    }
    let pending = writer
        .accumulator
        .buffered_bytes()
        .checked_add(components)
        .ok_or_else(|| Status::resource_exhausted("v1 component seal admission overflow"))?;
    let query = keldra_index::v1::query_batches_seal_peak_bytes(
        std::iter::once(&writer.query).chain(incoming),
        keldra_index::v1::QueryBlockLimits::default_for_memory(),
    )
    .map_err(index_status)?;
    let historical_sequence = writer.current.as_ref().map_or(0, |current| {
        current
            .generation
            .roots
            .iter()
            .map(|root| root.last_sequence)
            .max()
            .unwrap_or(0)
    });
    let metadata = publication_metadata_bound(writer, false)?
        .checked_add(split_component_metadata_bound(
            pending,
            historical_sequence,
        )?)
        .ok_or_else(|| Status::resource_exhausted("v1 split publication metadata overflow"))?;
    // Three component representations overlap: seal source lease, encoded
    // segments, packed artifact copy. Their wire overhead is below the buffer's
    // explicit 160-byte entry/192-byte component admission. Preloaded pages can
    // coexist with decoded path-copy pages and finalized publication pages.
    // This permit changes stage before construction; the promise therefore
    // includes its already-admitted bytes, not only additional builder space.
    let retained_query = writer.query_credits.admitted_bytes();
    let peak = pending
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(query))
        .and_then(|bytes| bytes.checked_add(retained_query))
        .and_then(|bytes| bytes.checked_add(metadata))
        .ok_or_else(|| Status::resource_exhausted("v1 publication peak admission overflow"))?;
    if let Some(promise) = &mut writer.sealing_progress {
        promise.grow_to(peak).map_err(|_| {
            Status::resource_exhausted("v1 future sealing progress memory unavailable")
        })?;
    } else {
        writer.sealing_progress = Some(credits.reserve_progress(peak).map_err(|_| {
            Status::resource_exhausted("v1 future sealing progress memory unavailable")
        })?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spine_bound_tracks_historical_append_depth_not_memory_fraction() {
        assert_eq!(spine_pages(0), 0);
        assert_eq!(spine_pages(1), 2);
        assert_eq!(spine_pages(128), 9);
        assert_eq!(spine_pages(u64::MAX), 65);
    }

    #[test]
    fn split_metadata_admits_each_extra_child_without_a_new_memory_cap() {
        let half = keldra_index::v1::ARTIFACT_PACK_MAX_BYTES / 2;
        assert_eq!(split_component_metadata_bound(half - 1, 0).unwrap(), 0);
        assert!(
            split_component_metadata_bound(half, 0).unwrap()
                > 2 * STREAM_PAGE_BYTES * 3 * spine_pages(2)
        );
        assert!(
            split_component_metadata_bound(4 * half, 128).unwrap()
                >= 4 * 2 * STREAM_PAGE_BYTES * 3 * spine_pages(133)
        );
        assert!(split_component_metadata_bound(half, u64::MAX).is_err());
    }
}
