use std::io::Read;

use keldra_store::{DefinitionKind, ObjectKey, VersionId};
use tonic::Status;

use super::*;
use crate::index_runtime::publication::{IndexArtifactDelete, rebuild_path};
use crate::index_runtime::rebuild_root::{DurableRebuildRoot, MAX_REBUILD_ROOT_BYTES};

#[derive(Clone, Debug)]
pub(crate) struct LoadedRebuildRoot {
    pub root: DurableRebuildRoot,
    pub object_version: VersionId,
}

impl IndexCommitPublisher {
    /// Create or replace the sole non-serving rebuild checkpoint at its exact
    /// observed version. This object is never followed by query readers.
    pub(crate) async fn publish_rebuild_root(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        root: &DurableRebuildRoot,
        expected_version: Option<VersionId>,
    ) -> Result<LoadedRebuildRoot, Status> {
        if root.index_id != definition.index_id {
            return Err(Status::invalid_argument(
                "rebuild root belongs to another definition",
            ));
        }
        let bytes = root
            .encode()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let blob = stage_artifact_bytes(
            &self.store,
            &bytes,
            DerivedArtifactAdmission::PublicationProgress,
        )
        .await?;
        let path = rebuild_path(definition.index_id);
        let current_guard = self
            .artifacts
            .acquire_current_mutation(definition.index_id)
            .await?;
        let outcome = self
            .artifacts
            .publish_while_current_mutation_held(
                IndexArtifactPublish {
                    storage_tenant: definition.tenant.clone(),
                    bucket: definition.bucket.clone(),
                    tenant_id,
                    bucket_id,
                    index_id: definition.index_id,
                    exact_path: path.clone(),
                    blob: blob.clone(),
                    expected_version,
                    command_id: content_command(definition.index_id, &path, &blob),
                    definition_guard: Some(DefinitionVersionGuard {
                        kind: DefinitionKind::Index,
                        exact_path: definition_path(&definition.name)?,
                        expected_version: VersionId(root.definition_version),
                    }),
                    definition_intent: None,
                    admission: DerivedArtifactAdmission::PublicationProgress,
                },
                Some(&current_guard),
            )
            .await?;
        Ok(LoadedRebuildRoot {
            root: root.clone(),
            object_version: outcome.version,
        })
    }

    pub(crate) async fn load_rebuild_root(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<LoadedRebuildRoot>, Status> {
        let path = rebuild_path(definition.index_id);
        let key = ObjectKey::new(&definition.tenant, &definition.bucket, &path)
            .map_err(|error| Status::internal(error.to_string()))?;
        let Some(mut opened) = self
            .reader
            .open_stable(&key, tenant_id, bucket_id, None)
            .await?
        else {
            return Ok(None);
        };
        if opened.version.deleted {
            return Ok(None);
        }
        let payload = opened
            .payload
            .take()
            .ok_or_else(|| Status::data_loss("live rebuild root has no payload"))?;
        let mut bytes = Vec::new();
        payload
            .take(MAX_REBUILD_ROOT_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read rebuild root: {error}")))?;
        if bytes.len() > MAX_REBUILD_ROOT_BYTES {
            return Err(Status::data_loss("rebuild root exceeds its format bound"));
        }
        let root = DurableRebuildRoot::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if root.index_id != definition.index_id {
            return Err(Status::data_loss(
                "rebuild root belongs to another definition",
            ));
        }
        Ok(Some(LoadedRebuildRoot {
            root,
            object_version: opened.version.id,
        }))
    }

    pub(crate) async fn delete_rebuild_root(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        expected_version: VersionId,
    ) -> Result<(), Status> {
        let path = rebuild_path(definition.index_id);
        let current_guard = self
            .artifacts
            .acquire_current_mutation(definition.index_id)
            .await?;
        self.artifacts
            .delete_while_current_mutation_held(
                IndexArtifactDelete {
                    storage_tenant: definition.tenant.clone(),
                    bucket: definition.bucket.clone(),
                    tenant_id,
                    bucket_id,
                    index_id: definition.index_id,
                    exact_path: path.clone(),
                    expected_version,
                    command_id: format!(
                        "index-rebuild-root-delete-{}-{}",
                        definition.index_id, expected_version.0
                    ),
                    definition_intent: None,
                },
                &current_guard,
            )
            .await
            .map(|_| ())
    }
}
