//! Bounded encoded-shard ranges over existing artifact chunks.
use super::*;

impl Store {
    /// Read a sealed payload replica without requiring this node to own the
    /// canonical metadata reference. Authenticated callers must pin placement
    /// and verify the child's published identity before using returned bytes.
    pub fn read_complete_copy_range(
        &self,
        reference: &BlobRef,
        offset: u64,
        length: u64,
        maximum_bytes: usize,
    ) -> Result<Vec<u8>, MutationError> {
        self.read_complete_artifact_range_for_reference(reference, offset, length, maximum_bytes)?
            .ok_or(MutationError::BlobNotFound)
    }

    pub(in crate::store) fn read_encoded_shard_range(
        &self,
        identity: &ShardIdentity,
        offset: u64,
        length: u64,
        maximum_bytes: usize,
    ) -> Result<Vec<u8>, MutationError> {
        let manifest = self
            .read_shard_manifest(identity)?
            .ok_or_else(|| artifact_storage("shard is absent"))?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| artifact_storage("shard range overflow"))?;
        let count = usize::try_from(length)
            .map_err(|_| artifact_storage("shard range exceeds platform"))?;
        if end > manifest.encoded_length || count > maximum_bytes {
            return Err(artifact_storage("shard range exceeds admitted bound"));
        }
        let cf = self.cf(CF_PAYLOAD_ARTIFACTS)?;
        let options = trusted_local_payload_read_options();
        match manifest.layout {
            ArtifactLayout::Inline => {
                let bytes = self
                    .db
                    .get_pinned_cf_opt(
                        cf,
                        tagged_identity(SHARD_INLINE_TAG, &manifest.storage_id),
                        &options,
                    )
                    .map_err(storage_error)?
                    .ok_or_else(|| artifact_storage("shard value is absent"))?;
                if bytes.len() as u64 != manifest.encoded_length {
                    return Err(artifact_storage("shard value is truncated"));
                }
                Ok(bytes[offset as usize..end as usize].to_vec())
            }
            ArtifactLayout::Chunked { chunk_count } => {
                if length == 0 {
                    return Ok(Vec::new());
                }
                let unit = PAYLOAD_ARTIFACT_CHUNK_BYTES as u64;
                let first = u32::try_from(offset / unit)
                    .map_err(|_| artifact_storage("shard ordinal overflow"))?;
                let last = u32::try_from((end - 1) / unit)
                    .map_err(|_| artifact_storage("shard ordinal overflow"))?;
                if last >= chunk_count {
                    return Err(artifact_storage("shard range exceeds layout"));
                }
                let mut output = Vec::with_capacity(count);
                for ordinal in first..=last {
                    let start = u64::from(ordinal) * unit;
                    let expected = (manifest.encoded_length - start).min(unit);
                    let bytes = self
                        .db
                        .get_pinned_cf_opt(
                            cf,
                            chunk_key(SHARD_CHUNK_TAG, &manifest.storage_id, ordinal),
                            &options,
                        )
                        .map_err(storage_error)?
                        .ok_or_else(|| artifact_storage("shard chunk is absent"))?;
                    if bytes.len() as u64 != expected {
                        return Err(artifact_storage("shard chunk is truncated"));
                    }
                    let from = offset.saturating_sub(start) as usize;
                    let through = (end - start).min(expected) as usize;
                    output.extend_from_slice(&bytes[from..through]);
                }
                Ok(output)
            }
        }
    }
}
