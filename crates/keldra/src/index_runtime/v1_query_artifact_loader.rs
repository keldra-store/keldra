use std::sync::Arc;

use keldra_index::IndexError;
use keldra_index::v1::{
    ProjectionQueryRunDescriptor, QueryArtifactKind, QueryArtifactLoad, QueryArtifactLoader,
    QueryPopulation,
};
use keldra_store::BlobRef;

use super::super::v1_artifact_cache::DecodePopulation;
use super::{IndexQueryScheduler, V1ProjectionPublisher};

pub(super) struct RuntimeArtifactLoader {
    projections: V1ProjectionPublisher,
    storage_tenant: String,
    bucket: String,
    tenant_id: u64,
    bucket_id: u64,
    query_scheduler: IndexQueryScheduler,
}

impl RuntimeArtifactLoader {
    pub(super) fn new(
        projections: V1ProjectionPublisher,
        storage_tenant: String,
        bucket: String,
        tenant_id: u64,
        bucket_id: u64,
        query_scheduler: IndexQueryScheduler,
    ) -> Self {
        Self {
            projections,
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            query_scheduler,
        }
    }
}

impl QueryArtifactLoader for RuntimeArtifactLoader {
    fn load_query_artifact(
        &mut self,
        request: QueryArtifactLoad,
    ) -> impl std::future::Future<Output = Result<bytes::Bytes, IndexError>> + Send {
        async move {
            if let Some(pack) = &request.pack {
                if request.kind != QueryArtifactKind::Block {
                    return Err(IndexError::Integrity);
                }
                let pack_bytes = self
                    .projections
                    .read_exact_artifact_pack(
                        &self.storage_tenant,
                        &self.bucket,
                        self.tenant_id,
                        self.bucket_id,
                        pack,
                    )
                    .await
                    .map_err(|error| IndexError::Io(error.to_string()))?;
                let start =
                    usize::try_from(request.pack_offset).map_err(|_| IndexError::OffsetOverflow)?;
                let end = start
                    .checked_add(request.encoded_bytes)
                    .ok_or(IndexError::OffsetOverflow)?;
                let expected_hash = request.hash;
                return self
                    .query_scheduler
                    .run_cpu(move || {
                        right_sized_verified_child(&pack_bytes, start, end, expected_hash)
                    })
                    .await;
            }
            if request.kind == QueryArtifactKind::Block {
                return Err(IndexError::Integrity);
            }
            let blob = BlobRef {
                hash: request.hash,
                length: request.encoded_bytes as u64,
            };
            self.projections
                .read_blob_local_first(&blob, request.encoded_bytes)
                .await
                .map_err(|error| IndexError::Io(error.to_string()))
        }
    }

    fn cached_projection_query_run(
        &self,
        request: QueryArtifactLoad,
    ) -> Result<Option<Arc<ProjectionQueryRunDescriptor>>, IndexError> {
        if request.kind != QueryArtifactKind::Run || request.pack.is_some() {
            return Ok(None);
        }
        let cached = self
            .projections
            .cached_query_run(&logical_blob(&request), request.encoded_bytes)
            .map_err(|error| IndexError::Io(error.to_string()))?;
        tracing::debug!(
            index.kind = "typed_json",
            query.artifact = "run",
            query.cache = if cached.is_some() { "hit" } else { "miss" },
            monotonic_counter.keldra_index_query_run_cache_lookups_total = 1_u64,
            monotonic_counter.keldra_index_query_run_cache_hits_total = u64::from(cached.is_some()),
            "v1 query run cache lookup"
        );
        Ok(cached)
    }

    fn cache_projection_query_run(
        &mut self,
        request: QueryArtifactLoad,
        descriptor: Arc<ProjectionQueryRunDescriptor>,
    ) {
        if request.kind == QueryArtifactKind::Run && request.pack.is_none() {
            self.projections
                .cache_query_run(&logical_blob(&request), descriptor);
        }
    }

    fn cached_query_block(
        &self,
        generation: [u8; 32],
        request: QueryArtifactLoad,
    ) -> Result<Option<Arc<keldra_index::v1::DecodedQueryBlock>>, IndexError> {
        if request.kind != QueryArtifactKind::Block || request.pack.is_none() {
            return Ok(None);
        }
        let cached = self
            .projections
            .cached_query_block(&logical_blob(&request), generation, request.encoded_bytes)
            .map_err(|error| IndexError::Io(error.to_string()))?;
        tracing::debug!(
            index.kind = "typed_json",
            query.artifact = "block",
            query.cache = if cached.is_some() { "hit" } else { "miss" },
            monotonic_counter.keldra_index_query_block_cache_lookups_total = 1_u64,
            monotonic_counter.keldra_index_query_block_cache_hits_total =
                u64::from(cached.is_some()),
            "v1 query block cache lookup"
        );
        Ok(cached)
    }

    fn cache_query_block(
        &mut self,
        generation: [u8; 32],
        request: QueryArtifactLoad,
        block: Arc<keldra_index::v1::DecodedQueryBlock>,
    ) {
        if request.kind == QueryArtifactKind::Block && request.pack.is_some() {
            self.projections
                .cache_query_block(&logical_blob(&request), generation, block);
        }
    }

    fn coordinate_query_run_population(
        &mut self,
        request: QueryArtifactLoad,
    ) -> impl std::future::Future<Output = Result<QueryPopulation, IndexError>> + Send {
        async move {
            match self
                .projections
                .immutable_cache()
                .coordinate_query_run(&logical_blob(&request))
                .await
                .map_err(|error| IndexError::Io(error.to_string()))?
            {
                DecodePopulation::Completed => Ok(QueryPopulation::Completed),
                DecodePopulation::Lead(guard) => Ok(QueryPopulation::Lead(Box::new(guard))),
            }
        }
    }

    fn coordinate_query_block_population(
        &mut self,
        generation: [u8; 32],
        request: QueryArtifactLoad,
    ) -> impl std::future::Future<Output = Result<QueryPopulation, IndexError>> + Send {
        async move {
            match self
                .projections
                .immutable_cache()
                .coordinate_query_block(&logical_blob(&request), generation)
                .await
                .map_err(|error| IndexError::Io(error.to_string()))?
            {
                DecodePopulation::Completed => Ok(QueryPopulation::Completed),
                DecodePopulation::Lead(guard) => Ok(QueryPopulation::Lead(Box::new(guard))),
            }
        }
    }

    fn try_fork_query_loader(&self) -> Result<Option<Self>, IndexError> {
        Ok(Some(Self::new(
            self.projections.clone(),
            self.storage_tenant.clone(),
            self.bucket.clone(),
            self.tenant_id,
            self.bucket_id,
            self.query_scheduler.clone(),
        )))
    }
}

fn logical_blob(request: &QueryArtifactLoad) -> BlobRef {
    BlobRef {
        hash: request.hash,
        length: request.encoded_bytes as u64,
    }
}

fn right_sized_verified_child(
    pack: &bytes::Bytes,
    start: usize,
    end: usize,
    expected_hash: [u8; 32],
) -> Result<bytes::Bytes, IndexError> {
    let encoded = pack.get(start..end).ok_or(IndexError::Integrity)?;
    if *keldra_index::profiled_blake3_hash!(encoded).as_bytes() != expected_hash {
        return Err(IndexError::Integrity);
    }
    // The decoded cache charges this exact allocation plus its lookup index;
    // it never pins or relies on the containing pack's eviction lifetime.
    Ok(bytes::Bytes::copy_from_slice(encoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_child_is_right_sized_and_does_not_pin_the_pack_allocation() {
        let pack = bytes::Bytes::from(vec![7_u8; 1024 * 1024]);
        let expected = *keldra_index::profiled_blake3_hash!(&pack[41..57]).as_bytes();

        let child = right_sized_verified_child(&pack, 41, 57, expected).unwrap();

        assert_eq!(child.len(), 16);
        assert_eq!(child.as_ref(), &pack[41..57]);
        assert_ne!(child.as_ptr(), pack[41..57].as_ptr());
    }

    #[test]
    fn packed_child_rejects_a_mismatched_identity_before_cache_population() {
        let pack = bytes::Bytes::from_static(b"whole physical pack");
        assert_eq!(
            right_sized_verified_child(&pack, 6, 14, [9; 32]),
            Err(IndexError::Integrity)
        );
    }
}
