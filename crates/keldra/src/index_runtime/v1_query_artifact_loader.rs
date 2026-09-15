use std::sync::Arc;

use keldra_index::IndexError;
use keldra_index::v1::{
    ProjectionQueryRunDescriptor, QueryArtifactKind, QueryArtifactLoad, QueryArtifactLoader,
};
use keldra_store::BlobRef;

use super::V1ProjectionPublisher;

pub(super) struct RuntimeArtifactLoader {
    projections: V1ProjectionPublisher,
    storage_tenant: String,
    bucket: String,
    tenant_id: u64,
    bucket_id: u64,
}

impl RuntimeArtifactLoader {
    pub(super) fn new(
        projections: V1ProjectionPublisher,
        storage_tenant: String,
        bucket: String,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Self {
        Self {
            projections,
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
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
                let encoded = pack_bytes.get(start..end).ok_or(IndexError::Integrity)?;
                if *keldra_index::profiled_blake3_hash!(encoded).as_bytes() != request.hash {
                    return Err(IndexError::Integrity);
                }
                return Ok(pack_bytes.slice(start..end));
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
        self.projections
            .cached_query_run(&logical_blob(&request), request.encoded_bytes)
            .map_err(|error| IndexError::Io(error.to_string()))
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
        self.projections
            .cached_query_block(&logical_blob(&request), generation, request.encoded_bytes)
            .map_err(|error| IndexError::Io(error.to_string()))
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

    fn try_fork_query_loader(&self) -> Result<Option<Self>, IndexError> {
        Ok(Some(Self::new(
            self.projections.clone(),
            self.storage_tenant.clone(),
            self.bucket.clone(),
            self.tenant_id,
            self.bucket_id,
        )))
    }
}

fn logical_blob(request: &QueryArtifactLoad) -> BlobRef {
    BlobRef {
        hash: request.hash,
        length: request.encoded_bytes as u64,
    }
}
