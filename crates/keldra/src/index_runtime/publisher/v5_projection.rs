use std::collections::BTreeMap;

use keldra_index::v5::{
    PreparedProjectionGeneration, projection_component_page_path, projection_current_path,
    projection_generation_path, projection_pack_path, projection_routing_id,
    projection_stream_page_path,
};
use keldra_store::{BlobRef, VersionId};
use tonic::Status;

use super::{IndexCommitPublisher, publish_command, stage_index_bytes_with_retry};
use crate::index_runtime::manager::publication_cohort::PublicationCohortClass;
use crate::index_runtime::publication::{DerivedArtifactAdmission, IndexArtifactPublish};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProjectionArtifactReference {
    pub(crate) exact_path: String,
    pub(crate) blob: BlobRef,
    pub(crate) version: VersionId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublishedProjectionGeneration {
    pub(crate) family_id: [u8; 32],
    pub(crate) generation_hash: [u8; 32],
    pub(crate) artifacts: Vec<ProjectionArtifactReference>,
    pub(crate) current_version: VersionId,
}

struct ArtifactBytes {
    path: String,
    hash: [u8; 32],
    bytes: Vec<u8>,
}

impl IndexCommitPublisher {
    /// Durably publishes one prepared v5 generation, making it visible only by
    /// the final exact-version current-pointer mutation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_projection_generation(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        family_id: [u8; 32],
        expected_current: Option<VersionId>,
        prepared: PreparedProjectionGeneration,
        admission: DerivedArtifactAdmission,
    ) -> Result<PublishedProjectionGeneration, Status> {
        let routing_id = projection_routing_id(family_id);
        let generation_hash = prepared.generation.hash;
        let artifacts = collect_artifacts(family_id, prepared)?;
        let mut staged = Vec::with_capacity(artifacts.len());
        for artifact in artifacts {
            let blob = stage_index_bytes_with_retry(&self.store, &artifact.bytes, admission)
                .await
                .map_err(index_status)?;
            if blob.hash != artifact.hash || blob.length != artifact.bytes.len() as u64 {
                return Err(Status::data_loss(
                    "staged v5 projection artifact changed content identity",
                ));
            }
            staged.push((artifact.path, blob));
        }

        let current_path = projection_current_path(family_id);
        let current = staged
            .pop()
            .ok_or_else(|| Status::internal("prepared projection has no current pointer"))?;
        if current.0 != current_path {
            return Err(Status::internal(
                "prepared projection current pointer was not ordered last",
            ));
        }

        let requests = staged
            .iter()
            .map(|(path, blob)| {
                projection_request(
                    storage_tenant,
                    bucket,
                    tenant_id,
                    bucket_id,
                    routing_id,
                    path,
                    blob.clone(),
                    None,
                    admission,
                )
            })
            .collect::<Vec<_>>();
        let outcomes = self
            .cohorts
            .publish_packs(requests, PublicationCohortClass::Incremental)
            .await?;
        if outcomes.len() != staged.len() {
            return Err(Status::data_loss(
                "v5 immutable publication returned an unaligned outcome set",
            ));
        }
        let mut references = Vec::with_capacity(staged.len());
        for ((path, blob), outcome) in staged.into_iter().zip(outcomes) {
            let outcome = outcome?;
            references.push(ProjectionArtifactReference {
                exact_path: path,
                blob,
                version: outcome.version,
            });
        }

        let current_request = projection_request(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            routing_id,
            &current.0,
            current.1,
            expected_current,
            admission,
        );
        let current_outcome = self.artifacts.publish(current_request).await?;
        Ok(PublishedProjectionGeneration {
            family_id,
            generation_hash,
            artifacts: references,
            current_version: current_outcome.version,
        })
    }
}

fn collect_artifacts(
    family_id: [u8; 32],
    prepared: PreparedProjectionGeneration,
) -> Result<Vec<ArtifactBytes>, Status> {
    let mut artifacts = BTreeMap::<String, ArtifactBytes>::new();
    for pack in prepared.packs {
        insert_artifact(
            &mut artifacts,
            projection_pack_path(family_id, pack.hash),
            pack.hash,
            pack.bytes,
        )?;
    }
    for page in prepared.stream_pages {
        insert_artifact(
            &mut artifacts,
            projection_stream_page_path(family_id, page.hash),
            page.hash,
            page.bytes,
        )?;
    }
    for page in prepared.generation.component_directory.pages {
        insert_artifact(
            &mut artifacts,
            projection_component_page_path(family_id, page.hash),
            page.hash,
            page.bytes,
        )?;
    }
    insert_artifact(
        &mut artifacts,
        projection_generation_path(family_id, prepared.generation.hash),
        prepared.generation.hash,
        prepared.generation.bytes,
    )?;
    let current_hash = *blake3::hash(&prepared.current).as_bytes();
    let mut ordered = artifacts.into_values().collect::<Vec<_>>();
    ordered.push(ArtifactBytes {
        path: projection_current_path(family_id),
        hash: current_hash,
        bytes: prepared.current,
    });
    Ok(ordered)
}

fn insert_artifact(
    artifacts: &mut BTreeMap<String, ArtifactBytes>,
    path: String,
    hash: [u8; 32],
    bytes: Vec<u8>,
) -> Result<(), Status> {
    if hash != *blake3::hash(&bytes).as_bytes() {
        return Err(Status::data_loss(
            "prepared v5 projection artifact has the wrong content hash",
        ));
    }
    match artifacts.entry(path.clone()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(ArtifactBytes { path, hash, bytes });
        }
        std::collections::btree_map::Entry::Occupied(entry)
            if entry.get().hash == hash && entry.get().bytes == bytes => {}
        std::collections::btree_map::Entry::Occupied(_) => {
            return Err(Status::data_loss(
                "v5 projection path named conflicting immutable bytes",
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn projection_request(
    storage_tenant: &str,
    bucket: &str,
    tenant_id: u64,
    bucket_id: u64,
    routing_id: u64,
    path: &str,
    blob: BlobRef,
    expected_version: Option<VersionId>,
    admission: DerivedArtifactAdmission,
) -> IndexArtifactPublish {
    IndexArtifactPublish {
        storage_tenant: storage_tenant.into(),
        bucket: bucket.into(),
        tenant_id,
        bucket_id,
        index_id: routing_id,
        exact_path: path.into(),
        command_id: publish_command(routing_id, path, &blob, expected_version),
        blob,
        expected_version,
        definition_guard: None,
        definition_intent: None,
        admission,
    }
}

fn index_status(error: keldra_index::IndexError) -> Status {
    match error {
        keldra_index::IndexError::ResourceLimit { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        keldra_index::IndexError::Io(_) => Status::unavailable(error.to_string()),
        _ => Status::data_loss(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use keldra_index::v5::{
        ComponentDirectory, EncodedComponentDirectoryPage, EncodedComponentStreamPage,
        EncodedProjectionGeneration,
    };

    fn hash(bytes: &[u8]) -> [u8; 32] {
        *blake3::hash(bytes).as_bytes()
    }

    #[test]
    fn prepared_generation_flattens_to_deduplicated_immutable_paths_then_current() {
        let family = [9; 32];
        let stream_bytes = b"stream-page".to_vec();
        let component_bytes = b"component-page".to_vec();
        let generation_bytes = b"generation".to_vec();
        let prepared = PreparedProjectionGeneration {
            packs: Vec::new(),
            stream_pages: vec![
                EncodedComponentStreamPage {
                    hash: hash(&stream_bytes),
                    bytes: stream_bytes.clone(),
                },
                EncodedComponentStreamPage {
                    hash: hash(&stream_bytes),
                    bytes: stream_bytes,
                },
            ],
            generation: EncodedProjectionGeneration {
                hash: hash(&generation_bytes),
                bytes: generation_bytes,
                component_directory: ComponentDirectory {
                    root_hash: hash(&component_bytes),
                    root_count: 1,
                    pages: vec![EncodedComponentDirectoryPage {
                        hash: hash(&component_bytes),
                        bytes: component_bytes,
                    }],
                },
            },
            current: b"current".to_vec(),
        };
        let artifacts = collect_artifacts(family, prepared).unwrap();
        assert_eq!(artifacts.len(), 4);
        assert_eq!(
            artifacts.last().unwrap().path,
            projection_current_path(family)
        );
        assert_eq!(
            artifacts
                .iter()
                .filter(|artifact| artifact.path.contains("/stream-pages/"))
                .count(),
            1
        );
    }

    #[test]
    fn prepared_artifact_hash_mismatch_fails_before_staging() {
        let mut artifacts = BTreeMap::new();
        assert!(insert_artifact(&mut artifacts, "path".into(), [1; 32], vec![2]).is_err());
        assert!(artifacts.is_empty());
    }
}
