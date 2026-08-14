//! Format-v4 index publication through ordinary Anvil objects.

use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};

use anvil_index::IndexError;
use anvil_index::compaction::CompactionProgress;
use anvil_index::v4::build::{ComponentBatchSink, ComponentPack};
use anvil_index::v4::{ArtifactDescriptor, INDEX_COMPONENT_BYTES, IndexKind, SegmentDescriptor};
use anvil_store::{BlobRef, DefinitionKind, ObjectKey, Store, VersionId};
use tonic::Status;
use tracing::Instrument;

use crate::cluster_object_read::ClusterObjectReader;
use crate::index_config::IndexRuntimeConfig;
use crate::index_service::{StoredIndexDefinition, definition_path};

use super::events::IndexBarrier;
use super::generation::{
    IndexCurrentPointer, IndexGenerationManifest, LocatorRoot, ManifestPhysicalOrder,
    ManifestReference,
};
use super::publication::{
    DefinitionVersionGuard, DerivedArtifactAdmission, IndexArtifactPublish, IndexArtifactRouter,
    artifact_path, current_path, manifest_path,
};

#[derive(Clone)]
pub(crate) struct IndexGenerationPublisher {
    store: Store,
    reader: ClusterObjectReader,
    artifacts: IndexArtifactRouter,
    config: IndexRuntimeConfig,
}

#[derive(Clone, Debug)]
pub(crate) struct PublishedGeneration {
    pub(crate) pointer: IndexCurrentPointer,
    pub(crate) current_object_version: VersionId,
    pub(crate) manifest: IndexGenerationManifest,
}

#[derive(Clone, Debug)]
pub(crate) struct SelectedPublishedGeneration {
    pub(crate) pointer: IndexCurrentPointer,
    pub(crate) current_object_version: VersionId,
    pub(crate) reference: ManifestReference,
    pub(crate) manifest: IndexGenerationManifest,
}

impl IndexGenerationPublisher {
    pub(crate) fn new(
        store: Store,
        reader: ClusterObjectReader,
        artifacts: IndexArtifactRouter,
        config: IndexRuntimeConfig,
    ) -> Self {
        Self {
            store,
            reader,
            artifacts,
            config,
        }
    }

    pub(crate) fn component_sink(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        admission: DerivedArtifactAdmission,
    ) -> IndexComponentBatchSink {
        IndexComponentBatchSink {
            store: self.store.clone(),
            artifacts: self.artifacts.clone(),
            definition: definition.clone(),
            tenant_id,
            bucket_id,
            admission,
            progress: None,
        }
    }

    pub(crate) fn observed_component_sink(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        admission: DerivedArtifactAdmission,
        progress: CompactionProgress,
    ) -> IndexComponentBatchSink {
        let mut sink = self.component_sink(definition, tenant_id, bucket_id, admission);
        sink.progress = Some(progress);
        sink
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_manifest(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        definition_version: u64,
        kind: IndexKind,
        schema_fingerprint: [u8; 32],
        barrier: IndexBarrier,
        physical_order: Vec<ManifestPhysicalOrder>,
        mut segments: Vec<SegmentDescriptor>,
        mut locator_roots: Vec<LocatorRoot>,
        current: Option<&PublishedGeneration>,
        admission: DerivedArtifactAdmission,
    ) -> Result<PublishedGeneration, Status> {
        segments.sort_by_key(|segment| segment.identity.segment_id);
        locator_roots.sort_by_key(|locator| locator.sequence);
        let generation = current
            .map(|value| value.pointer.current.generation)
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("index generation revision overflow"))?;
        let (artifact_encoded_bytes, artifact_logical_bytes) =
            generation_artifact_totals(&segments, &locator_roots)?;
        let manifest = IndexGenerationManifest::new(
            definition.index_id,
            generation,
            definition_version,
            kind,
            schema_fingerprint,
            &barrier,
            physical_order,
            segments,
            locator_roots,
            artifact_encoded_bytes,
            artifact_logical_bytes,
        )
        .map_err(generation_status)?;

        let manifest_bytes = manifest.encode().map_err(generation_status)?;
        let manifest_length = manifest_bytes.len() as u64;
        let manifest_span = tracing::info_span!(
            "anvil.index.manifest_publish",
            index.id = definition.index_id,
            tenant.id = tenant_id,
            bucket.id = bucket_id,
            index.kind = ?kind,
            generation,
            manifest.bytes = manifest_length,
        );
        let manifest_started = std::time::Instant::now();
        let manifest_result = async {
            let blob = stage_artifact_bytes(&self.store, &manifest_bytes, admission).await?;
            let path = manifest_path(definition.index_id, blob.hash);
            let version = self
                .publish_immutable(
                    definition,
                    tenant_id,
                    bucket_id,
                    &path,
                    blob.clone(),
                    admission,
                )
                .await?;
            Ok::<_, Status>((blob, version))
        }
        .instrument(manifest_span.clone())
        .await;
        let manifest_failed = manifest_result.is_err();
        manifest_span.in_scope(|| {
            tracing::info!(
                index.kind = ?kind,
                publish.phase = "manifest",
                publish.outcome = if manifest_failed { "failed" } else { "completed" },
                monotonic_counter.anvil_index_manifests_published_total =
                    u64::from(!manifest_failed),
                monotonic_counter.anvil_index_manifest_publish_failures_total =
                    u64::from(manifest_failed),
                histogram.anvil_index_manifest_bytes = manifest_length,
                histogram.anvil_index_manifest_publish_duration_seconds =
                    manifest_started.elapsed().as_secs_f64(),
                "format-v4 index generation manifest publication finished"
            );
        });
        let (manifest_blob, manifest_object_version) = manifest_result?;
        let published_at = SystemTime::now();
        let current_reference = ManifestReference::new(
            &manifest,
            manifest_blob,
            manifest_object_version,
            published_at,
        )
        .map_err(generation_status)?;
        // Publication stays O(pointer references). Exact distinct-object byte
        // enforcement is deliberately performed later by bounded retention
        // maintenance, never while making a new generation visible.
        let retained = select_retained_metadata(
            self.config,
            current.into_iter().flat_map(|previous| {
                std::iter::once(&previous.pointer.current).chain(previous.pointer.retained.iter())
            }),
            published_at,
        )?;
        let pointer = IndexCurrentPointer::new(definition.index_id, current_reference, retained)
            .map_err(generation_status)?;
        let pointer_bytes = pointer.encode().map_err(generation_status)?;
        let pointer_length = pointer_bytes.len() as u64;
        let path = current_path(definition.index_id);
        let current_span = tracing::info_span!(
            "anvil.index.current_pointer_cas",
            index.id = definition.index_id,
            tenant.id = tenant_id,
            bucket.id = bucket_id,
            index.kind = ?kind,
            generation,
            current.bytes = pointer_length,
            retained.generations = pointer.retained.len() as u64,
        );
        let current_started = std::time::Instant::now();
        let current_result = async {
            let blob = stage_artifact_bytes(&self.store, &pointer_bytes, admission).await?;
            self.artifacts
                .publish(IndexArtifactPublish {
                    storage_tenant: definition.tenant.clone(),
                    bucket: definition.bucket.clone(),
                    tenant_id,
                    bucket_id,
                    index_id: definition.index_id,
                    exact_path: path.clone(),
                    blob: blob.clone(),
                    expected_version: current.map(|value| value.current_object_version),
                    command_id: content_command(definition.index_id, &path, &blob),
                    definition_guard: Some(DefinitionVersionGuard {
                        kind: DefinitionKind::Index,
                        exact_path: definition_path(&definition.name)?,
                        expected_version: VersionId(definition_version),
                    }),
                    definition_intent: None,
                    admission,
                })
                .await
        }
        .instrument(current_span.clone())
        .await;
        let current_failed = current_result.is_err();
        current_span.in_scope(|| {
            tracing::info!(
                index.kind = ?kind,
                publish.phase = "current_pointer_cas",
                publish.outcome = if current_failed { "failed" } else { "completed" },
                monotonic_counter.anvil_index_current_pointer_cas_attempts_total = 1_u64,
                monotonic_counter.anvil_index_current_pointer_cas_successes_total =
                    u64::from(!current_failed),
                monotonic_counter.anvil_index_current_pointer_cas_failures_total =
                    u64::from(current_failed),
                histogram.anvil_index_current_pointer_bytes = pointer_length,
                histogram.anvil_index_current_pointer_cas_duration_seconds =
                    current_started.elapsed().as_secs_f64(),
                "format-v4 index current-pointer CAS finished"
            );
        });
        let outcome = current_result?;
        Ok(PublishedGeneration {
            pointer,
            current_object_version: outcome.version,
            manifest,
        })
    }

    /// CAS a strictly smaller retained suffix while preserving the exact
    /// current generation. The caller owns the complete retention proof; this
    /// operation intentionally performs no manifest or artifact traversal.
    pub(crate) async fn trim_retained(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        current: &PublishedGeneration,
        retained: Vec<ManifestReference>,
    ) -> Result<PublishedGeneration, Status> {
        validate_manifest_reference(
            &current.pointer.current,
            &current.manifest,
            definition.index_id,
        )?;
        if retained.len() > current.pointer.retained.len()
            || retained
                .iter()
                .zip(&current.pointer.retained)
                .any(|(selected, existing)| selected != existing)
        {
            return Err(Status::invalid_argument(
                "retention may only remove an oldest suffix from the current pointer",
            ));
        }
        if retained == current.pointer.retained {
            return Ok(current.clone());
        }

        let pointer = IndexCurrentPointer::new(
            definition.index_id,
            current.pointer.current.clone(),
            retained,
        )
        .map_err(generation_status)?;
        let pointer_bytes = pointer.encode().map_err(generation_status)?;
        let blob = stage_artifact_bytes(
            &self.store,
            &pointer_bytes,
            DerivedArtifactAdmission::Bounded,
        )
        .await?;
        let path = current_path(definition.index_id);
        let outcome = self
            .artifacts
            .publish(IndexArtifactPublish {
                storage_tenant: definition.tenant.clone(),
                bucket: definition.bucket.clone(),
                tenant_id,
                bucket_id,
                index_id: definition.index_id,
                exact_path: path.clone(),
                blob: blob.clone(),
                expected_version: Some(current.current_object_version),
                command_id: content_command(definition.index_id, &path, &blob),
                definition_guard: Some(DefinitionVersionGuard {
                    kind: DefinitionKind::Index,
                    exact_path: definition_path(&definition.name)?,
                    expected_version: VersionId(current.manifest.definition_version),
                }),
                definition_intent: None,
                admission: DerivedArtifactAdmission::Bounded,
            })
            .await?;
        Ok(PublishedGeneration {
            pointer,
            current_object_version: outcome.version,
            manifest: current.manifest.clone(),
        })
    }

    pub(crate) fn metadata_retained(
        &self,
        current: &PublishedGeneration,
        now: SystemTime,
    ) -> Result<Vec<ManifestReference>, Status> {
        select_retained_metadata(self.config, current.pointer.retained.iter(), now)
    }

    pub(crate) async fn load_current(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<PublishedGeneration>, Status> {
        Ok(self
            .load_generation(
                &definition.tenant,
                &definition.bucket,
                tenant_id,
                bucket_id,
                definition.index_id,
                None,
            )
            .await?
            .map(|selected| PublishedGeneration {
                pointer: selected.pointer,
                current_object_version: selected.current_object_version,
                manifest: selected.manifest,
            }))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_generation(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
        exact_generation: Option<u64>,
    ) -> Result<Option<SelectedPublishedGeneration>, Status> {
        let path = current_path(index_id);
        let key = ObjectKey::new(storage_tenant, bucket, &path)
            .map_err(|error| Status::internal(error.to_string()))?;
        let Some(mut opened) = self
            .reader
            .open_stable(&key, tenant_id, bucket_id, None)
            .await?
        else {
            return Ok(None);
        };
        if opened.version.deleted {
            return Err(Status::data_loss("current index pointer is deleted"));
        }
        let mut payload = opened
            .payload
            .take()
            .ok_or_else(|| Status::data_loss("current index pointer has no payload"))?;
        let mut bytes = Vec::new();
        payload
            .by_ref()
            .take(INDEX_COMPONENT_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read current index pointer: {error}")))?;
        if bytes.len() > INDEX_COMPONENT_BYTES {
            return Err(Status::data_loss(
                "current index pointer exceeds the format-v4 bound",
            ));
        }
        let pointer = IndexCurrentPointer::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if pointer.index_id != index_id {
            return Err(Status::data_loss(
                "current index pointer belongs to another index",
            ));
        }
        let requested = exact_generation.unwrap_or(pointer.current.generation);
        if requested > pointer.current.generation {
            return Err(Status::failed_precondition(
                "requested index generation was never published",
            ));
        }
        let reference = pointer.generation(requested).cloned().ok_or_else(|| {
            Status::failed_precondition("requested index generation is no longer retained")
        })?;
        let manifest = self
            .load_manifest_reference(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                index_id,
                &reference,
            )
            .await?;
        Ok(Some(SelectedPublishedGeneration {
            pointer,
            current_object_version: opened.version.id,
            reference,
            manifest,
        }))
    }

    async fn load_manifest_reference(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
        reference: &ManifestReference,
    ) -> Result<IndexGenerationManifest, Status> {
        reference.validate(index_id).map_err(generation_status)?;
        let key = ObjectKey::new(storage_tenant, bucket, &reference.path)
            .map_err(|error| Status::internal(error.to_string()))?;
        let Some(mut opened) = self
            .reader
            .open_stable(&key, tenant_id, bucket_id, Some(reference.object_version))
            .await?
        else {
            return Err(Status::data_loss(
                "format-v4 generation manifest object is absent",
            ));
        };
        if opened.version.id != reference.object_version
            || opened.version.deleted
            || opened.version.blob.as_ref() != Some(&reference.blob)
        {
            return Err(Status::data_loss(
                "format-v4 generation manifest differs from its exact reference",
            ));
        }
        let mut payload = opened
            .payload
            .take()
            .ok_or_else(|| Status::data_loss("format-v4 generation manifest has no payload"))?;
        let mut bytes = Vec::new();
        payload
            .by_ref()
            .take(INDEX_COMPONENT_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read format-v4 manifest: {error}")))?;
        if bytes.len() > INDEX_COMPONENT_BYTES
            || bytes.len() as u64 != reference.blob.length
            || blake3::hash(&bytes).as_bytes() != &reference.blob.hash
        {
            return Err(Status::data_loss(
                "format-v4 generation manifest bytes differ from their reference",
            ));
        }
        let manifest = IndexGenerationManifest::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        validate_manifest_reference(reference, &manifest, index_id)?;
        Ok(manifest)
    }

    async fn publish_immutable(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        blob: BlobRef,
        admission: DerivedArtifactAdmission,
    ) -> Result<VersionId, Status> {
        let outcome = self
            .artifacts
            .publish(IndexArtifactPublish {
                storage_tenant: definition.tenant.clone(),
                bucket: definition.bucket.clone(),
                tenant_id,
                bucket_id,
                index_id: definition.index_id,
                exact_path: path.to_owned(),
                command_id: content_command(definition.index_id, path, &blob),
                blob,
                expected_version: None,
                definition_guard: None,
                definition_intent: None,
                admission,
            })
            .await?;
        Ok(outcome.version)
    }
}

#[derive(Clone)]
pub(crate) struct IndexComponentBatchSink {
    store: Store,
    artifacts: IndexArtifactRouter,
    definition: StoredIndexDefinition,
    tenant_id: u64,
    bucket_id: u64,
    admission: DerivedArtifactAdmission,
    progress: Option<CompactionProgress>,
}

impl ComponentBatchSink for IndexComponentBatchSink {
    fn publish_pack(
        &mut self,
        pack: ComponentPack,
    ) -> impl std::future::Future<Output = Result<Vec<ArtifactDescriptor>, IndexError>> + Send {
        async move { self.publish_component_pack(pack).await }
    }
}

impl IndexComponentBatchSink {
    async fn publish_component_pack(
        &self,
        pack: ComponentPack,
    ) -> Result<Vec<ArtifactDescriptor>, IndexError> {
        if pack.identity().index_id != self.definition.index_id {
            return Err(IndexError::InvalidDefinition(
                "component publication reached the wrong index publisher".into(),
            ));
        }
        let encoded_bytes = pack.encoded_bytes();
        let component_count = pack.component_count()?;
        let span = tracing::info_span!(
            "anvil.index.v4_component_publish",
            index.id = self.definition.index_id,
            tenant.id = self.tenant_id,
            bucket.id = self.bucket_id,
            component.count = component_count,
            component.bytes = encoded_bytes,
            pack.count = 1_u64,
        );
        let started = std::time::Instant::now();
        let result = self.publish_one_pack(pack).instrument(span.clone()).await;
        let failed = result.is_err();
        span.in_scope(|| {
            tracing::info!(
                publish.outcome = if failed { "failed" } else { "completed" },
                monotonic_counter.anvil_index_v4_components_published_total =
                    if failed { 0 } else { component_count },
                monotonic_counter.anvil_index_v4_component_publish_failures_total =
                    u64::from(failed),
                monotonic_counter.anvil_index_v4_component_bytes_total =
                    if failed { 0 } else { encoded_bytes },
                histogram.anvil_index_v4_component_publish_duration_seconds =
                    started.elapsed().as_secs_f64(),
                "format-v4 index components publication finished"
            );
        });
        if !failed {
            if let Some(progress) = &self.progress {
                progress.record_output(0, encoded_bytes, component_count);
            }
        }
        result
    }

    async fn publish_one_pack(
        &self,
        pack: ComponentPack,
    ) -> Result<Vec<ArtifactDescriptor>, IndexError> {
        let blob = stage_index_bytes(&self.store, pack.bytes(), self.admission).await?;
        if blob.length != pack.encoded_bytes()
            || blob.hash != *blake3::hash(pack.bytes()).as_bytes()
        {
            return Err(IndexError::Integrity);
        }
        let path = artifact_path(self.definition.index_id, blob.hash);
        let outcome = self
            .artifacts
            .publish(IndexArtifactPublish {
                storage_tenant: self.definition.tenant.clone(),
                bucket: self.definition.bucket.clone(),
                tenant_id: self.tenant_id,
                bucket_id: self.bucket_id,
                index_id: self.definition.index_id,
                exact_path: path.clone(),
                blob: blob.clone(),
                expected_version: None,
                command_id: content_command(self.definition.index_id, &path, &blob),
                definition_guard: None,
                definition_intent: None,
                admission: self.admission,
            })
            .await
            .map_err(|error| IndexError::Io(error.to_string()))?;
        pack.descriptors(path, outcome.version.0, blob.hash)
    }
}

fn generation_artifact_totals(
    segments: &[SegmentDescriptor],
    locator_roots: &[LocatorRoot],
) -> Result<(u64, u64), Status> {
    let encoded = segments
        .iter()
        .map(|segment| segment.encoded_bytes)
        .chain(locator_roots.iter().map(|locator| locator.encoded_bytes))
        .try_fold(0_u64, |total, bytes| total.checked_add(bytes))
        .ok_or_else(|| Status::resource_exhausted("index artifact byte total overflowed"))?;
    let logical = segments
        .iter()
        .map(|segment| segment.logical_bytes)
        .chain(locator_roots.iter().map(|locator| locator.logical_bytes))
        .try_fold(0_u64, |total, bytes| total.checked_add(bytes))
        .ok_or_else(|| Status::resource_exhausted("index artifact byte total overflowed"))?;
    Ok((encoded, logical))
}

fn validate_manifest_reference(
    reference: &ManifestReference,
    manifest: &IndexGenerationManifest,
    index_id: u64,
) -> Result<(), Status> {
    reference.validate(index_id).map_err(generation_status)?;
    if manifest.index_id != index_id
        || manifest.generation != reference.generation
        || manifest.definition_version != reference.definition_version
        || manifest.schema_fingerprint != reference.schema_fingerprint
    {
        return Err(Status::data_loss(
            "format-v4 manifest identity differs from its current-pointer reference",
        ));
    }
    let bytes = manifest.encode().map_err(generation_status)?;
    if bytes.len() as u64 != reference.blob.length
        || blake3::hash(&bytes).as_bytes() != &reference.blob.hash
    {
        return Err(Status::data_loss(
            "format-v4 manifest bytes differ from their current-pointer reference",
        ));
    }
    Ok(())
}

fn unix_millis(time: SystemTime) -> Result<u64, Status> {
    u64::try_from(
        time.duration_since(UNIX_EPOCH)
            .map_err(|_| Status::internal("system clock predates the Unix epoch"))?
            .as_millis(),
    )
    .map_err(|_| Status::resource_exhausted("system timestamp exceeds u64"))
}

fn select_retained_metadata<'a>(
    config: IndexRuntimeConfig,
    candidates: impl IntoIterator<Item = &'a ManifestReference>,
    now: SystemTime,
) -> Result<Vec<ManifestReference>, Status> {
    let now_millis = unix_millis(now)?;
    let maximum_age_millis = config
        .max_generation_age_hours()
        .saturating_mul(60 * 60 * 1_000);
    let maximum_count = config.max_retained_generations() as usize;
    let mut retained = Vec::new();
    for reference in candidates {
        // The new/current generation occupies the first retained-count slot.
        if retained.len().saturating_add(1) >= maximum_count
            || now_millis.saturating_sub(reference.published_at_unix_millis) > maximum_age_millis
        {
            break;
        }
        retained.push(reference.clone());
    }
    Ok(retained)
}

async fn stage_index_bytes(
    store: &Store,
    bytes: &[u8],
    admission: DerivedArtifactAdmission,
) -> Result<BlobRef, IndexError> {
    match admission {
        DerivedArtifactAdmission::Bounded => store.stage_blob(bytes).await,
        DerivedArtifactAdmission::PublicationProgress => {
            store.stage_derived_progress_blob(bytes).await
        }
    }
    .map_err(|error| IndexError::Io(error.to_string()))
}

async fn stage_artifact_bytes(
    store: &Store,
    bytes: &[u8],
    admission: DerivedArtifactAdmission,
) -> Result<BlobRef, Status> {
    stage_index_bytes(store, bytes, admission)
        .await
        .map_err(index_status)
}

fn content_command(index_id: u64, path: &str, blob: &BlobRef) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(path.as_bytes());
    hasher.update(&blob.hash);
    hasher.update(&blob.length.to_be_bytes());
    let digest = hasher.finalize();
    format!("index-v4-{index_id}-{}", &digest.to_hex().as_str()[..24])
}

fn generation_status(error: super::generation::GenerationError) -> Status {
    Status::data_loss(error.to_string())
}

fn index_status(error: IndexError) -> Status {
    match error {
        IndexError::ResourceLimit { .. } => Status::resource_exhausted(error.to_string()),
        IndexError::Io(_) => Status::unavailable(error.to_string()),
        _ => Status::data_loss(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn manifest_reference(generation: u64, published_at: u64) -> ManifestReference {
        let hash = [generation as u8; 32];
        ManifestReference {
            generation,
            definition_version: 1,
            schema_fingerprint: [1; 32],
            path: manifest_path(7, hash),
            blob: BlobRef { hash, length: 120 },
            object_version: VersionId(generation + 10),
            published_at_unix_millis: published_at,
        }
    }

    #[test]
    fn publication_retention_selection_uses_only_bounded_pointer_metadata() {
        let hour = 60 * 60 * 1_000;
        let now_millis = 100 * hour;
        let candidates = [
            manifest_reference(3, now_millis - hour),
            manifest_reference(2, now_millis - 2 * hour),
            manifest_reference(1, now_millis - 3 * hour),
        ];
        let selected = select_retained_metadata(
            IndexRuntimeConfig::default(),
            candidates.iter(),
            UNIX_EPOCH + Duration::from_millis(now_millis),
        )
        .unwrap();
        assert_eq!(selected.len(), 2);

        let old = [manifest_reference(1, now_millis - 25 * hour)];
        assert!(
            select_retained_metadata(
                IndexRuntimeConfig::default(),
                old.iter(),
                UNIX_EPOCH + Duration::from_millis(now_millis),
            )
            .unwrap()
            .is_empty()
        );
    }
}
