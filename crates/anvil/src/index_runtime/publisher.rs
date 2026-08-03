//! Construction and ordinary-object publication of immutable index generations.

use std::io::Read;
use std::time::SystemTime;

use anvil_store::{ObjectKey, Store, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::IndexHeadScanScope;
use crate::index_service::StoredIndexDefinition;

use super::engine::{IndexBuildObject, build_generation};
use super::events::IndexBarrier;
use super::generation::{
    DEFAULT_INDEX_SEGMENT_BYTES, IndexCurrentPointer, PreparedIndexGeneration,
};
use super::publication::{
    IndexArtifactPublish, IndexArtifactRouter, current_path, generation_manifest_path,
};
use super::scanner::ClusterIndexScanner;

#[derive(Clone)]
pub(crate) struct IndexGenerationPublisher {
    store: Store,
    reader: ClusterObjectReader,
    scanner: ClusterIndexScanner,
    artifacts: IndexArtifactRouter,
}

#[derive(Clone, Debug)]
pub(crate) struct PublishedGeneration {
    pub(crate) pointer: IndexCurrentPointer,
    pub(crate) current_object_version: VersionId,
}

impl IndexGenerationPublisher {
    pub(crate) fn new(
        store: Store,
        reader: ClusterObjectReader,
        scanner: ClusterIndexScanner,
        artifacts: IndexArtifactRouter,
    ) -> Self {
        Self {
            store,
            reader,
            scanner,
            artifacts,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn build_and_publish(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        definition_version: u64,
        barrier: IndexBarrier,
        objects: Vec<IndexBuildObject>,
    ) -> Result<PublishedGeneration, Status> {
        let current = self.load_current(definition, tenant_id, bucket_id).await?;
        let generation = self
            .next_generation(tenant_id, bucket_id, definition.index_id)
            .await?;
        let specification = definition.specification()?;
        let index_id = definition.index_id;
        let prepared = tokio::task::spawn_blocking(move || {
            let built = build_generation(&specification, objects)
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            PreparedIndexGeneration::prepare(
                index_id,
                generation,
                definition_version,
                &barrier,
                built,
                DEFAULT_INDEX_SEGMENT_BYTES,
            )
            .map_err(|error| Status::internal(error.to_string()))
        })
        .await
        .map_err(|error| Status::internal(format!("index build worker failed: {error}")))??;
        self.publish_prepared(
            definition,
            tenant_id,
            bucket_id,
            current.as_ref().map(|value| value.current_object_version),
            prepared,
        )
        .await
    }

    async fn publish_prepared(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        expected_current: Option<VersionId>,
        prepared: PreparedIndexGeneration,
    ) -> Result<PublishedGeneration, Status> {
        for (file, segments) in prepared.manifest.files.iter().zip(&prepared.segment_bytes) {
            for (segment, bytes) in file.segments.iter().zip(segments) {
                let blob = self.store.stage_blob(bytes).await.map_err(store_status)?;
                if blob != segment.blob {
                    return Err(Status::data_loss(
                        "staged index segment identity differs from its manifest",
                    ));
                }
                self.artifacts
                    .publish(IndexArtifactPublish {
                        storage_tenant: definition.tenant.clone(),
                        bucket: definition.bucket.clone(),
                        tenant_id,
                        bucket_id,
                        index_id: definition.index_id,
                        exact_path: segment.object_path.clone(),
                        blob,
                        expected_version: None,
                        command_id: artifact_command(
                            definition.index_id,
                            prepared.manifest.generation,
                            &segment.object_path,
                        ),
                    })
                    .await?;
            }
        }

        let manifest_bytes = prepared
            .manifest
            .encode()
            .map_err(|error| Status::internal(error.to_string()))?;
        let manifest_blob = self
            .store
            .stage_blob(&manifest_bytes)
            .await
            .map_err(store_status)?;
        let manifest_path =
            generation_manifest_path(definition.index_id, prepared.manifest.generation);
        self.artifacts
            .publish(IndexArtifactPublish {
                storage_tenant: definition.tenant.clone(),
                bucket: definition.bucket.clone(),
                tenant_id,
                bucket_id,
                index_id: definition.index_id,
                exact_path: manifest_path.clone(),
                blob: manifest_blob.clone(),
                expected_version: None,
                command_id: artifact_command(
                    definition.index_id,
                    prepared.manifest.generation,
                    &manifest_path,
                ),
            })
            .await?;

        let pointer =
            IndexCurrentPointer::new(&prepared.manifest, manifest_blob, SystemTime::now())
                .map_err(|error| Status::internal(error.to_string()))?;
        let pointer_bytes = pointer
            .encode()
            .map_err(|error| Status::internal(error.to_string()))?;
        let pointer_blob = self
            .store
            .stage_blob(&pointer_bytes)
            .await
            .map_err(store_status)?;
        let current_path = current_path(definition.index_id);
        let outcome = self
            .artifacts
            .publish(IndexArtifactPublish {
                storage_tenant: definition.tenant.clone(),
                bucket: definition.bucket.clone(),
                tenant_id,
                bucket_id,
                index_id: definition.index_id,
                exact_path: current_path.clone(),
                blob: pointer_blob,
                expected_version: expected_current,
                command_id: artifact_command(
                    definition.index_id,
                    prepared.manifest.generation,
                    &current_path,
                ),
            })
            .await?;
        Ok(PublishedGeneration {
            pointer,
            current_object_version: outcome.version,
        })
    }

    pub(crate) async fn load_current(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<PublishedGeneration>, Status> {
        let key = ObjectKey::new(
            &definition.tenant,
            &definition.bucket,
            &current_path(definition.index_id),
        )
        .map_err(|error| Status::internal(error.to_string()))?;
        let Some(opened) = self
            .reader
            .open_stable(&key, tenant_id, bucket_id, None)
            .await?
        else {
            return Ok(None);
        };
        if opened.version.deleted {
            return Err(Status::data_loss("current index pointer is deleted"));
        }
        let Some(mut payload) = opened.payload else {
            return Err(Status::data_loss("current index pointer has no payload"));
        };
        let mut bytes = Vec::new();
        payload
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read current index pointer: {error}")))?;
        let pointer = IndexCurrentPointer::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if pointer.index_id != definition.index_id {
            return Err(Status::data_loss(
                "current index pointer belongs to another index",
            ));
        }
        Ok(Some(PublishedGeneration {
            pointer,
            current_object_version: opened.version.id,
        }))
    }

    async fn next_generation(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    ) -> Result<u64, Status> {
        let heads = self
            .scanner
            .scan(IndexHeadScanScope::Generation {
                tenant_id,
                bucket_id,
                index_id,
            })
            .await?;
        heads
            .iter()
            .filter_map(|head| generation_from_path(index_id, &head.exact_path))
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("index generation number is exhausted"))
    }
}

fn generation_from_path(index_id: u64, path: &str) -> Option<u64> {
    let mut parts = path.split('/');
    if parts.next()? != "_anvil"
        || parts.next()? != "indexes"
        || parts.next()?.parse::<u64>().ok()? != index_id
        || parts.next()? != "generations"
    {
        return None;
    }
    let generation = parts.next()?.parse::<u64>().ok()?;
    (generation != 0).then_some(generation)
}

fn artifact_command(index_id: u64, generation: u64, path: &str) -> String {
    let digest = blake3::hash(path.as_bytes());
    format!(
        "index-{index_id}-{generation}-{}",
        &digest.to_hex().as_str()[..16]
    )
}

fn store_status(error: anvil_store::MutationError) -> Status {
    Status::internal(format!("stage index artifact: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_paths_are_parsed_without_accepting_other_indexes() {
        assert_eq!(
            generation_from_path(7, "_anvil/indexes/7/generations/9/manifest"),
            Some(9)
        );
        assert_eq!(
            generation_from_path(8, "_anvil/indexes/7/generations/9/manifest"),
            None
        );
        assert_eq!(generation_from_path(7, "ordinary/path"), None);
    }
}
