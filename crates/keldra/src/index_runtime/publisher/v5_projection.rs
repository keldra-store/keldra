use std::collections::BTreeMap;
use std::collections::{BTreeSet, VecDeque};
use std::io::Read;

use keldra_index::v4::INDEX_ARTIFACT_PACK_BYTES;
use keldra_index::v5::{
    ComponentDirectory, ComponentIdentity, ComponentRecordLookup, ComponentRoot,
    ComponentStreamDirectory, ComponentStreamRoot, EncodedComponentDirectoryPage,
    EncodedComponentStreamPage, PreparedProjectionGeneration, ProjectionBarrier, ProjectionCurrent,
    ProjectionGeneration, SealedComponentDelta, StableDocumentKey,
    component_directory_child_hashes, component_stream_child_hashes, decode_component_stream,
    decode_projected_document_state, decode_projection_current, decode_projection_generation,
    decode_projection_generation_header, decode_source_records, empty_component_directory_hash,
    lookup_component_record_in_pack, prepare_projection_generation, projection_component_page_path,
    projection_current_path, projection_generation_path, projection_pack_path,
    projection_routing_id, projection_stream_page_path,
};
use keldra_store::{BlobRef, ObjectKey, VersionId};
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

pub(crate) struct PublishedProjectionArtifacts {
    pub(crate) family_id: [u8; 32],
    pub(crate) generation_hash: [u8; 32],
    pub(crate) artifacts: Vec<ProjectionArtifactReference>,
    current: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct LoadedProjectionGeneration {
    pub(crate) current: ProjectionCurrent,
    pub(crate) current_object_version: VersionId,
    pub(crate) generation: ProjectionGeneration,
}

struct ArtifactBytes {
    path: String,
    hash: [u8; 32],
    bytes: Vec<u8>,
}

impl IndexCommitPublisher {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn advance_projection_generation(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        family_id: [u8; 32],
        previous: Option<&LoadedProjectionGeneration>,
        barrier: ProjectionBarrier,
        deltas: Vec<SealedComponentDelta>,
        admission: DerivedArtifactAdmission,
    ) -> Result<PublishedProjectionGeneration, Status> {
        let mut pages = BTreeMap::new();
        if let Some(previous) = previous {
            if previous.current.family_id != family_id {
                return Err(Status::data_loss(
                    "projection predecessor belongs to another family",
                ));
            }
            for component in deltas.iter().map(|delta| delta.component) {
                let Some(root) = previous.generation.root(component) else {
                    continue;
                };
                let directory = self
                    .load_component_stream(
                        storage_tenant,
                        bucket,
                        tenant_id,
                        bucket_id,
                        family_id,
                        root,
                    )
                    .await?;
                for page in directory.pages {
                    match pages.entry(page.hash) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(page.bytes);
                        }
                        std::collections::btree_map::Entry::Occupied(entry)
                            if entry.get() == &page.bytes => {}
                        std::collections::btree_map::Entry::Occupied(_) => {
                            return Err(Status::data_loss(
                                "projection stream page hash names conflicting bytes",
                            ));
                        }
                    }
                }
            }
        }
        let prepared = prepare_projection_generation(
            family_id,
            previous.map(|loaded| (&loaded.generation, loaded.current.generation_hash)),
            barrier,
            deltas,
            |hash| {
                pages
                    .get(&hash)
                    .cloned()
                    .ok_or(keldra_index::IndexError::Integrity)
            },
        )
        .map_err(index_status)?;
        self.publish_projection_generation(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            family_id,
            previous.map(|loaded| loaded.current_object_version),
            prepared,
            admission,
        )
        .await
    }

    pub(crate) async fn load_projection_generation(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        family_id: [u8; 32],
    ) -> Result<Option<LoadedProjectionGeneration>, Status> {
        let current_path = projection_current_path(family_id);
        let Some((current_bytes, current_object_version)) = self
            .read_projection_object(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                &current_path,
                None,
                1024,
            )
            .await?
        else {
            return Ok(None);
        };
        let current = decode_projection_current(&current_bytes).map_err(index_status)?;
        if current.family_id != family_id {
            return Err(Status::data_loss(
                "projection current pointer belongs to another family",
            ));
        }
        let generation_path = projection_generation_path(family_id, current.generation_hash);
        let (generation_bytes, _) = self
            .read_projection_object(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                &generation_path,
                Some(current.generation_hash),
                256 * 1024,
            )
            .await?
            .ok_or_else(|| Status::data_loss("projection generation object is absent"))?;
        let header =
            decode_projection_generation_header(&generation_bytes).map_err(index_status)?;
        if header.family_id != family_id || header.revision != current.generation_revision {
            return Err(Status::data_loss(
                "projection generation header does not match current",
            ));
        }
        let pages = self
            .load_component_directory(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                family_id,
                header.component_directory_root_hash,
                header.component_root_count,
            )
            .await?;
        let directory = ComponentDirectory {
            root_hash: header.component_directory_root_hash,
            root_count: header.component_root_count,
            pages,
        };
        let generation =
            decode_projection_generation(&generation_bytes, &directory).map_err(index_status)?;
        current
            .validate_against(&generation)
            .map_err(index_status)?;
        Ok(Some(LoadedProjectionGeneration {
            current,
            current_object_version,
            generation,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_projection_component_record(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        loaded: &LoadedProjectionGeneration,
        component: ComponentIdentity,
        stable_key: StableDocumentKey,
    ) -> Result<Option<Vec<u8>>, Status> {
        let mut records = self
            .load_projection_component_records(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                loaded,
                component,
                &[stable_key],
            )
            .await?;
        Ok(records.remove(&stable_key).flatten())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_projection_source_states(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        loaded: &LoadedProjectionGeneration,
        source_scope: [u8; 32],
        source_path: &str,
    ) -> Result<Vec<keldra_index::v5::ProjectedDocumentState>, Status> {
        let locator_key =
            StableDocumentKey::derive(source_scope, source_path, 0).map_err(index_status)?;
        let Some(locator) = self
            .load_projection_component_record(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                loaded,
                ComponentIdentity::SourceRecords,
                locator_key,
            )
            .await?
        else {
            return Ok(Vec::new());
        };
        let stable_keys =
            decode_source_records(source_scope, source_path, &locator).map_err(index_status)?;
        let mut encoded = self
            .load_projection_component_records(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                loaded,
                ComponentIdentity::ProjectedState,
                &stable_keys,
            )
            .await?;
        let mut states = Vec::with_capacity(stable_keys.len());
        for stable_key in stable_keys {
            let bytes = encoded.remove(&stable_key).flatten().ok_or_else(|| {
                Status::data_loss("projection source locator names absent projected state")
            })?;
            let state = decode_projected_document_state(&bytes).map_err(index_status)?;
            if state.source_scope != source_scope
                || state.head.stable_key != stable_key
                || state.head.source_path != source_path
                || !state.head.live
            {
                return Err(Status::data_loss(
                    "projection source locator names inconsistent projected state",
                ));
            }
            states.push(state);
        }
        Ok(states)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_projection_component_records(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        loaded: &LoadedProjectionGeneration,
        component: ComponentIdentity,
        stable_keys: &[StableDocumentKey],
    ) -> Result<BTreeMap<StableDocumentKey, Option<Vec<u8>>>, Status> {
        if stable_keys.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Status::invalid_argument(
                "projection record lookup keys are not sorted and unique",
            ));
        }
        let Some(root) = loaded.generation.root(component) else {
            return Ok(stable_keys.iter().map(|key| (*key, None)).collect());
        };
        let directory = self
            .load_component_stream(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                loaded.current.family_id,
                root,
            )
            .await?;
        let mut pending = stable_keys.iter().copied().collect::<BTreeSet<_>>();
        let mut resolved = BTreeMap::new();
        for descriptor in decode_component_stream(&directory)
            .map_err(index_status)?
            .iter()
            .rev()
        {
            let path = projection_pack_path(loaded.current.family_id, descriptor.pack_hash);
            let (pack, _) = self
                .read_projection_object(
                    storage_tenant,
                    bucket,
                    tenant_id,
                    bucket_id,
                    &path,
                    Some(descriptor.pack_hash),
                    INDEX_ARTIFACT_PACK_BYTES,
                )
                .await?
                .ok_or_else(|| Status::data_loss("projection delta pack is absent"))?;
            let keys = pending.iter().copied().collect::<Vec<_>>();
            for stable_key in keys {
                match lookup_component_record_in_pack(component, descriptor, &pack, stable_key)
                    .map_err(index_status)?
                {
                    ComponentRecordLookup::Missing => {}
                    ComponentRecordLookup::Tombstone => {
                        pending.remove(&stable_key);
                        resolved.insert(stable_key, None);
                    }
                    ComponentRecordLookup::Value(value) => {
                        pending.remove(&stable_key);
                        resolved.insert(stable_key, Some(value));
                    }
                }
            }
            if pending.is_empty() {
                break;
            }
        }
        resolved.extend(pending.into_iter().map(|key| (key, None)));
        Ok(resolved)
    }

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
        let published = self
            .publish_projection_artifacts(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                family_id,
                prepared,
                admission,
            )
            .await?;
        self.install_projection_current(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            expected_current,
            published,
            admission,
        )
        .await
    }

    /// Publish a complete immutable generation without making it visible.
    /// Rebuilds use this after each bounded frame, retaining only the newest
    /// returned installation token. A failed or superseded attempt leaves
    /// content-addressed artifacts for the ordinary orphan collector.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_projection_artifacts(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        family_id: [u8; 32],
        prepared: PreparedProjectionGeneration,
        admission: DerivedArtifactAdmission,
    ) -> Result<PublishedProjectionArtifacts, Status> {
        let routing_id = projection_routing_id(family_id);
        let generation_hash = prepared.generation.hash;
        let (artifacts, current) = collect_artifacts(family_id, prepared)?;
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

        Ok(PublishedProjectionArtifacts {
            family_id,
            generation_hash,
            artifacts: references,
            current,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn install_projection_current(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        expected_current: Option<VersionId>,
        published: PublishedProjectionArtifacts,
        admission: DerivedArtifactAdmission,
    ) -> Result<PublishedProjectionGeneration, Status> {
        let current_path = projection_current_path(published.family_id);
        let current_blob = stage_index_bytes_with_retry(&self.store, &published.current, admission)
            .await
            .map_err(index_status)?;
        let current_request = projection_request(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            projection_routing_id(published.family_id),
            &current_path,
            current_blob,
            expected_current,
            admission,
        );
        let current_outcome = self.artifacts.publish(current_request).await?;
        Ok(PublishedProjectionGeneration {
            family_id: published.family_id,
            generation_hash: published.generation_hash,
            artifacts: published.artifacts,
            current_version: current_outcome.version,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_component_directory(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        family_id: [u8; 32],
        root_hash: [u8; 32],
        root_count: u64,
    ) -> Result<Vec<EncodedComponentDirectoryPage>, Status> {
        if root_count == 0 && root_hash == empty_component_directory_hash() {
            return Ok(Vec::new());
        }
        let maximum_pages = usize::try_from(root_count)
            .map_err(|_| Status::resource_exhausted("projection root count is unbounded"))?
            .saturating_mul(2)
            .max(1);
        let mut pending = VecDeque::from([root_hash]);
        let mut visited = BTreeSet::new();
        let mut pages = Vec::new();
        let mut resident = 0_usize;
        while let Some(hash) = pending.pop_front() {
            if !visited.insert(hash) {
                return Err(Status::data_loss(
                    "projection component directory contains a cycle or duplicate page",
                ));
            }
            if visited.len() > maximum_pages {
                return Err(Status::resource_exhausted(
                    "projection component directory exceeds its root-count bound",
                ));
            }
            let path = projection_component_page_path(family_id, hash);
            let (bytes, _) = self
                .read_projection_object(
                    storage_tenant,
                    bucket,
                    tenant_id,
                    bucket_id,
                    &path,
                    Some(hash),
                    32 * 1024,
                )
                .await?
                .ok_or_else(|| Status::data_loss("projection component page is absent"))?;
            resident = resident
                .checked_add(bytes.len())
                .ok_or_else(|| Status::resource_exhausted("projection directory bytes overflow"))?;
            if resident > 64 * 1024 * 1024 {
                return Err(Status::resource_exhausted(
                    "projection component directory exceeds its runtime read bound",
                ));
            }
            pending.extend(component_directory_child_hashes(&bytes).map_err(index_status)?);
            pages.push(EncodedComponentDirectoryPage { hash, bytes });
        }
        Ok(pages)
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_component_stream(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        family_id: [u8; 32],
        root: &ComponentRoot,
    ) -> Result<ComponentStreamDirectory, Status> {
        let stream = ComponentStreamRoot::from_component_root(root).map_err(index_status)?;
        let maximum_pages = usize::try_from(stream.segment_count)
            .map_err(|_| Status::resource_exhausted("projection segment count is unbounded"))?
            .saturating_mul(2)
            .max(1);
        let maximum_directory_bytes = usize::try_from(stream.directory_bytes)
            .map_err(|_| Status::resource_exhausted("projection directory bytes are unbounded"))?;
        if maximum_directory_bytes > 64 * 1024 * 1024 {
            return Err(Status::resource_exhausted(
                "projection stream directory exceeds its runtime read bound",
            ));
        }
        let mut pending = VecDeque::from([stream.root_hash]);
        let mut visited = BTreeSet::new();
        let mut pages = Vec::new();
        let mut resident = 0_usize;
        while let Some(hash) = pending.pop_front() {
            if !visited.insert(hash) {
                return Err(Status::data_loss(
                    "projection stream directory contains a cycle or duplicate page",
                ));
            }
            if visited.len() > maximum_pages {
                return Err(Status::resource_exhausted(
                    "projection stream directory exceeds its segment-count bound",
                ));
            }
            let path = projection_stream_page_path(family_id, hash);
            let (bytes, _) = self
                .read_projection_object(
                    storage_tenant,
                    bucket,
                    tenant_id,
                    bucket_id,
                    &path,
                    Some(hash),
                    32 * 1024,
                )
                .await?
                .ok_or_else(|| Status::data_loss("projection stream page is absent"))?;
            resident = resident.checked_add(bytes.len()).ok_or_else(|| {
                Status::resource_exhausted("projection stream directory bytes overflow")
            })?;
            if resident > maximum_directory_bytes {
                return Err(Status::data_loss(
                    "projection stream directory exceeds its committed byte count",
                ));
            }
            pending.extend(
                component_stream_child_hashes(stream.component, &bytes).map_err(index_status)?,
            );
            pages.push(EncodedComponentStreamPage { hash, bytes });
        }
        if resident != maximum_directory_bytes {
            return Err(Status::data_loss(
                "projection stream directory does not match its committed byte count",
            ));
        }
        let directory = ComponentStreamDirectory {
            component: stream.component,
            root_hash: stream.root_hash,
            segment_count: stream.segment_count,
            encoded_bytes: stream.encoded_bytes,
            logical_bytes: stream.logical_bytes,
            directory_bytes: stream.directory_bytes,
            pages,
        };
        decode_component_stream(&directory).map_err(index_status)?;
        Ok(directory)
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_projection_object(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        expected_hash: Option<[u8; 32]>,
        maximum_bytes: usize,
    ) -> Result<Option<(Vec<u8>, VersionId)>, Status> {
        let key = ObjectKey::new(storage_tenant, bucket, path)
            .map_err(|error| Status::internal(error.to_string()))?;
        let Some(mut opened) = self
            .reader
            .open_stable(&key, tenant_id, bucket_id, None)
            .await?
        else {
            return Ok(None);
        };
        if opened.version.deleted {
            return Err(Status::data_loss("projection artifact is deleted"));
        }
        let blob = opened
            .version
            .blob
            .as_ref()
            .ok_or_else(|| Status::data_loss("projection artifact has no payload identity"))?;
        if expected_hash.is_some_and(|hash| blob.hash != hash) {
            return Err(Status::data_loss(
                "projection artifact path and payload hash differ",
            ));
        }
        let mut payload = opened
            .payload
            .take()
            .ok_or_else(|| Status::data_loss("projection artifact has no payload"))?;
        let mut bytes = Vec::new();
        payload
            .by_ref()
            .take(maximum_bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read projection artifact: {error}")))?;
        if bytes.len() > maximum_bytes || bytes.len() as u64 != blob.length {
            return Err(Status::data_loss(
                "projection artifact violates its exact byte bound",
            ));
        }
        Ok(Some((bytes, opened.version.id)))
    }
}

fn collect_artifacts(
    family_id: [u8; 32],
    prepared: PreparedProjectionGeneration,
) -> Result<(Vec<ArtifactBytes>, Vec<u8>), Status> {
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
    Ok((artifacts.into_values().collect(), prepared.current))
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
    fn prepared_generation_separates_deduplicated_immutable_paths_from_current() {
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
        let (artifacts, current) = collect_artifacts(family, prepared).unwrap();
        assert_eq!(artifacts.len(), 3);
        assert_eq!(current, b"current");
        assert!(
            artifacts
                .iter()
                .all(|artifact| artifact.path != projection_current_path(family))
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
