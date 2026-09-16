//! Exact local ranges over the existing immutable payload layout.
use super::*;

impl Store {
    pub(in crate::store) fn read_complete_artifact_range_for_reference(
        &self,
        reference: &BlobRef,
        offset: u64,
        length: u64,
        maximum_bytes: usize,
    ) -> Result<Option<Vec<u8>>, MutationError> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| artifact_storage("payload range offset overflow"))?;
        if offset > reference.length || end > reference.length {
            return Err(artifact_storage(
                "payload range exceeds the exact blob length",
            ));
        }
        let count = usize::try_from(length)
            .map_err(|_| artifact_storage("payload range length exceeds platform capacity"))?;
        if count > maximum_bytes {
            return Err(artifact_storage(
                "payload range exceeds its admitted byte bound",
            ));
        }
        let derived = ArtifactManifest::complete(reference)?;
        let cf = self.cf(CF_PAYLOAD_ARTIFACTS)?;
        let options = trusted_local_payload_read_options();
        if derived.layout == ArtifactLayout::Inline {
            if let Some(bytes) = self
                .db
                .get_pinned_cf_opt(
                    cf,
                    tagged_identity(COMPLETE_INLINE_TAG, &derived.storage_id),
                    &options,
                )
                .map_err(storage_error)?
            {
                if bytes.len() as u64 != reference.length {
                    return Err(artifact_storage(
                        "payload artifact value has the wrong encoded length",
                    ));
                }
                return Ok(Some(bytes[offset as usize..end as usize].to_vec()));
            }
        }
        let Some(manifest) = self.read_complete_manifest(reference)? else {
            return Ok(None);
        };
        if count == 0 {
            return Ok(Some(Vec::new()));
        }
        match manifest.layout {
            ArtifactLayout::Inline => {
                let bytes = self
                    .db
                    .get_pinned_cf_opt(
                        cf,
                        tagged_identity(COMPLETE_INLINE_TAG, &manifest.storage_id),
                        &options,
                    )
                    .map_err(storage_error)?
                    .ok_or_else(|| artifact_storage("payload artifact value is missing"))?;
                if bytes.len() as u64 != manifest.encoded_length {
                    return Err(artifact_storage(
                        "payload artifact value has the wrong encoded length",
                    ));
                }
                Ok(Some(bytes[offset as usize..end as usize].to_vec()))
            }
            ArtifactLayout::Chunked { chunk_count } => {
                let chunk_bytes = PAYLOAD_ARTIFACT_CHUNK_BYTES as u64;
                let first = u32::try_from(offset / chunk_bytes)
                    .map_err(|_| artifact_storage("payload range chunk ordinal overflow"))?;
                let last = u32::try_from((end - 1) / chunk_bytes)
                    .map_err(|_| artifact_storage("payload range chunk ordinal overflow"))?;
                if last >= chunk_count {
                    return Err(artifact_storage("payload range exceeds its chunk layout"));
                }
                let mut output = Vec::with_capacity(count);
                for ordinal in first..=last {
                    let start = u64::from(ordinal) * chunk_bytes;
                    let expected = (manifest.encoded_length - start).min(chunk_bytes);
                    let chunk = self
                        .db
                        .get_pinned_cf_opt(
                            cf,
                            chunk_key(COMPLETE_CHUNK_TAG, &manifest.storage_id, ordinal),
                            &options,
                        )
                        .map_err(storage_error)?
                        .ok_or_else(|| artifact_storage("payload artifact chunk is missing"))?;
                    if chunk.len() as u64 != expected {
                        return Err(artifact_storage(
                            "payload artifact chunk has the wrong encoded length",
                        ));
                    }
                    let from = offset.saturating_sub(start) as usize;
                    let through = (end - start).min(expected) as usize;
                    output.extend_from_slice(&chunk[from..through]);
                }
                if output.len() != count {
                    return Err(artifact_storage("payload range is truncated"));
                }
                Ok(Some(output))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> (tempfile::TempDir, Store) {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        (directory, store)
    }

    #[tokio::test]
    async fn inline_ranges_are_exact_and_empty_eof_is_allowed() {
        let (_directory, store) = store().await;
        let bytes = b"0123456789";
        let reference = store.stage_blob(bytes).await.unwrap();
        for (offset, length) in [(0, 10), (3, 4), (9, 1), (10, 0), (0, 0)] {
            assert_eq!(
                store
                    .read_complete_artifact_range_for_reference(
                        &reference,
                        offset,
                        length,
                        length as usize,
                    )
                    .unwrap()
                    .unwrap(),
                bytes[offset as usize..(offset + length) as usize]
            );
        }
        for (offset, length, bound) in [(10, 1, 1), (11, 0, 0), (u64::MAX, 1, 1), (0, 3, 2)] {
            assert!(
                store
                    .read_complete_artifact_range_for_reference(&reference, offset, length, bound,)
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn chunked_ranges_load_only_intersecting_chunks_and_reject_truncation() {
        let (_directory, store) = store().await;
        let chunk_bytes = PAYLOAD_ARTIFACT_CHUNK_BYTES;
        let reference = BlobRef {
            hash: [0x84; 32],
            length: (chunk_bytes * 2 + 3) as u64,
        };
        let manifest = ArtifactManifest::complete(&reference).unwrap();
        store
            .db
            .put_cf(
                store.cf(CF_PAYLOAD_MANIFESTS).unwrap(),
                manifest_key(&complete_identity(&reference)),
                manifest.encode(),
            )
            .unwrap();
        let cf = store.cf(CF_PAYLOAD_ARTIFACTS).unwrap();
        store
            .db
            .put_cf(
                cf,
                chunk_key(COMPLETE_CHUNK_TAG, &manifest.storage_id, 1),
                vec![7; chunk_bytes],
            )
            .unwrap();
        store
            .db
            .put_cf(
                cf,
                chunk_key(COMPLETE_CHUNK_TAG, &manifest.storage_id, 2),
                [8, 9, 10],
            )
            .unwrap();
        // Chunk zero is deliberately absent: an unrelated range must not
        // retrieve it or silently require the complete object in memory.
        assert_eq!(
            store
                .read_complete_artifact_range_for_reference(
                    &reference,
                    (chunk_bytes * 2 - 2) as u64,
                    5,
                    5,
                )
                .unwrap()
                .unwrap(),
            [7, 7, 8, 9, 10]
        );
        assert!(
            store
                .read_complete_artifact_range_for_reference(&reference, 0, 1, 1)
                .is_err()
        );
        store
            .db
            .put_cf(
                cf,
                chunk_key(COMPLETE_CHUNK_TAG, &manifest.storage_id, 2),
                [8, 9],
            )
            .unwrap();
        assert!(
            store
                .read_complete_artifact_range_for_reference(
                    &reference,
                    (chunk_bytes * 2) as u64,
                    1,
                    1,
                )
                .unwrap_err()
                .to_string()
                .contains("wrong encoded length")
        );
    }

    #[tokio::test]
    async fn streamed_inline_length_retains_upload_chunk_identity() {
        let (_directory, store) = store().await;
        let reference = BlobRef {
            hash: [0x85; 32],
            length: 5,
        };
        let manifest = ArtifactManifest::uploaded_complete(&reference, [0x86; 32]).unwrap();
        store
            .db
            .put_cf(
                store.cf(CF_PAYLOAD_MANIFESTS).unwrap(),
                manifest_key(&complete_identity(&reference)),
                manifest.encode(),
            )
            .unwrap();
        store
            .db
            .put_cf(
                store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(),
                chunk_key(COMPLETE_CHUNK_TAG, &manifest.storage_id, 0),
                b"abcde",
            )
            .unwrap();
        assert_eq!(
            store
                .read_complete_artifact_range_for_reference(&reference, 1, 3, 3,)
                .unwrap()
                .unwrap(),
            b"bcd"
        );
    }

    #[tokio::test]
    async fn public_ranges_do_not_bypass_blob_liveness() {
        let (_directory, store) = store().await;
        let reference = store
            .stage_blob(b"payload whose reference is gone")
            .await
            .unwrap();
        assert_eq!(
            store.read_blob_range(&reference, 0, 7, 7).await.unwrap(),
            b"payload"
        );
        store
            .db
            .delete_cf(
                store.cf(CF_BLOB_REFERENCES).unwrap(),
                blob_reference_key(&reference),
            )
            .unwrap();
        assert!(matches!(
            store.read_blob_range(&reference, 0, 1, 1).await,
            Err(MutationError::BlobNotFound)
        ));
    }
}
