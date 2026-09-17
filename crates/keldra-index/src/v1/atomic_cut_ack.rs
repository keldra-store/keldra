//! Same-format acknowledgement of replayed atomic control metadata.
use super::{
    EncodedProjectionGeneration, EncodedProjectionQueryRun, EncodedQueryRunPage, ProjectionCurrent,
    ProjectionGeneration, ProjectionQueryStreamRoot, QueryBlockCredits, QueryBlockLimits,
    QueryRunReference, decode_projection_query_run, encode_projection_current,
    encode_projection_generation, encode_projection_query_run, replace_latest_query_run_path_copy,
};
use crate::IndexError;

/// Immutable metadata and its existing admission travel together into the
/// normal durable publication path. No object data or component delta changes.
#[derive(Debug)]
pub struct PreparedAtomicCutAcknowledgement {
    pub query_run: Option<EncodedProjectionQueryRun>,
    pub query_stream_pages: Vec<EncodedQueryRunPage>,
    pub generation: EncodedProjectionGeneration,
    pub current: Vec<u8>,
    pub query_credits: QueryBlockCredits,
}

#[cfg(test)]
#[path = "atomic_cut_ack_tests.rs"]
mod tests;

/// The caller must prove complete all-source replay and durable owned data
/// before acknowledging this atomic cut. This helper preserves all native
/// metadata integrity checks and never invents an empty source interval.
pub fn prepare_atomic_cut_acknowledgement<PageBytes: AsRef<[u8]>>(
    previous: &ProjectionGeneration,
    previous_hash: [u8; 32],
    through_atomic: u64,
    latest: Option<(QueryRunReference, &[u8])>,
    limits: QueryBlockLimits,
    mut credits: QueryBlockCredits,
    mut load_query_page: impl FnMut([u8; 32]) -> Result<PageBytes, IndexError>,
) -> Result<PreparedAtomicCutAcknowledgement, IndexError> {
    previous.validate()?;
    // Native component roots and inherited references are fixed-size records.
    // Cover successor encodings, directory nodes, advancement's
    // temporary ordered map and retained header allocations before creating any.
    let metadata_bytes = previous
        .roots
        .len()
        .checked_add(previous.inherited_partitions.len())
        .and_then(|count| count.checked_mul(4096))
        .and_then(|bytes| bytes.checked_add(16 * 1024))
        .ok_or(IndexError::OffsetOverflow)?;
    credits.reserve(metadata_bytes)?;
    // The ordinary immutable loader binds the predecessor to this hash before
    // entering preparation; retain that authority rather than encode it again.
    if previous_hash == [0; 32] || through_atomic <= previous.through_atomic_position {
        return Err(IndexError::Integrity);
    }
    let (root, query_run, pages) = if previous.query_stream_root.run_count == 0 {
        if latest.is_some() {
            return Err(IndexError::Integrity);
        }
        (
            ProjectionQueryStreamRoot::empty(
                previous.partition,
                previous.physical_catalog_generation,
                previous.next_offset,
                through_atomic,
            )?,
            None,
            Vec::new(),
        )
    } else {
        let (reference, bytes) = latest.ok_or(IndexError::Integrity)?;
        if bytes.len() as u64 != reference.encoded_bytes
            || *crate::profiled_blake3_hash!(bytes).as_bytes() != reference.hash
        {
            return Err(IndexError::Integrity);
        }
        let mut descriptor = decode_projection_query_run(bytes, limits, &mut credits)?;
        if descriptor.partition != previous.partition
            || descriptor.physical_catalog_generation != previous.physical_catalog_generation
            || descriptor.sequence != reference.sequence
            || descriptor.source_start_offset != reference.source_start_offset
            || descriptor.next_offset != reference.next_offset
            || descriptor.through_atomic_position != reference.through_atomic_position
        {
            return Err(IndexError::Integrity);
        }
        // Only this scalar changes. Shared tables, exact object versions,
        // block hashes, ranges and physical pack locators remain untouched.
        descriptor.through_atomic_position = through_atomic;
        let encoded = encode_projection_query_run(&descriptor, limits, &mut credits)?;
        let replacement = QueryRunReference {
            hash: encoded.hash,
            encoded_bytes: u64::try_from(encoded.bytes.len())
                .map_err(|_| IndexError::OffsetOverflow)?,
            through_atomic_position: through_atomic,
            ..reference
        };
        let copied = replace_latest_query_run_path_copy(
            previous.query_stream_root,
            reference,
            replacement,
            |hash| {
                // One bounded page's loaded, decoded and newly encoded owners.
                credits.reserve(3 * 32 * 1024)?;
                load_query_page(hash)
            },
        )?;
        (copied.root, Some(encoded), copied.pages)
    };
    let native_generation = previous
        .advance(
            previous_hash,
            previous.physical_catalog_generation,
            previous.next_offset,
            through_atomic,
            Vec::new(),
        )?
        .with_query_stream_root(root)?;
    let generation = encode_projection_generation(&native_generation)?;
    let current =
        encode_projection_current(ProjectionCurrent::new(generation.hash, &native_generation)?)?;
    Ok(PreparedAtomicCutAcknowledgement {
        query_run,
        query_stream_pages: pages,
        generation,
        current,
        query_credits: credits,
    })
}
