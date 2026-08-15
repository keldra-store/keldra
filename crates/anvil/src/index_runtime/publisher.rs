//! Format-v4 index publication through ordinary Anvil objects.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use anvil_index::IndexError;
use anvil_index::compaction::CompactionProgress;
use anvil_index::v4::build::ComponentBatchSink;
use anvil_index::v4::{
    ArtifactDescriptor, ArtifactPackReference, GeneratedComponent, INDEX_ARTIFACT_PACK_BYTES,
    INDEX_COMPONENT_BYTES, IndexKind, SegmentDescriptor, SegmentIdentity,
};
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
            active: Arc::new(Mutex::new(None)),
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
    active: Arc<Mutex<Option<PendingSegmentPacks>>>,
}

struct PendingSegmentPacks {
    identity: SegmentIdentity,
    base_packs: Vec<ArtifactPackReference>,
    staged: Vec<StagedIndexPackSlot>,
    pending_bytes: Vec<u8>,
    pending_components: u64,
    finalizing: bool,
}

struct CompletedSegmentPacks {
    base_packs: Vec<ArtifactPackReference>,
    staged: Vec<StagedIndexPack>,
}

struct StagedIndexPack {
    blob: BlobRef,
    component_count: u64,
}

enum StagedIndexPackSlot {
    Pending,
    Ready(StagedIndexPack),
    Failed,
}

struct ReservedIndexPack {
    identity: SegmentIdentity,
    slot: usize,
    bytes: Vec<u8>,
    component_count: u64,
}

impl PendingSegmentPacks {
    fn must_seal_before(&self, encoded: usize) -> Result<bool, IndexError> {
        Ok(!self.pending_bytes.is_empty()
            && self
                .pending_bytes
                .len()
                .checked_add(encoded)
                .ok_or(IndexError::OffsetOverflow)?
                > INDEX_ARTIFACT_PACK_BYTES)
    }

    fn next_component_location(&self) -> Result<(u32, u64), IndexError> {
        let pack_ordinal = u32::try_from(
            self.base_packs
                .len()
                .checked_add(self.staged.len())
                .ok_or(IndexError::OffsetOverflow)?,
        )
        .map_err(|_| IndexError::OffsetOverflow)?;
        let offset =
            u64::try_from(self.pending_bytes.len()).map_err(|_| IndexError::OffsetOverflow)?;
        Ok((pack_ordinal, offset))
    }

    fn reserve_pending_pack(&mut self) -> Result<Option<ReservedIndexPack>, IndexError> {
        if self.pending_bytes.is_empty() {
            return Ok(None);
        }
        let slot = self.staged.len();
        let bytes = std::mem::take(&mut self.pending_bytes);
        let component_count = std::mem::take(&mut self.pending_components);
        if component_count == 0 {
            return Err(IndexError::InvalidFormat(
                "non-empty index pack has no components",
            ));
        }
        self.staged.push(StagedIndexPackSlot::Pending);
        Ok(Some(ReservedIndexPack {
            identity: self.identity,
            slot,
            bytes,
            component_count,
        }))
    }

    fn reserve_component(
        &mut self,
        component: GeneratedComponent,
    ) -> Result<(ArtifactDescriptor, Vec<ReservedIndexPack>), IndexError> {
        if self.finalizing {
            return Err(IndexError::InvalidDefinition(
                "component sink is finalizing its active segment".into(),
            ));
        }
        let identity = component.header().identity;
        if self.identity != identity {
            return Err(IndexError::InvalidDefinition(
                "component sink cannot cross segment identities".into(),
            ));
        }
        let encoded = component.bytes().len();
        if encoded > INDEX_ARTIFACT_PACK_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: encoded,
                limit: INDEX_ARTIFACT_PACK_BYTES,
            });
        }
        let mut reserved = Vec::new();
        if self.must_seal_before(encoded)?
            && let Some(pack) = self.reserve_pending_pack()?
        {
            reserved.push(pack);
        }
        let (pack_ordinal, offset) = self.next_component_location()?;
        let descriptor = component.placed(pack_ordinal, offset)?;
        let component_bytes = component.into_bytes();
        if self.pending_bytes.is_empty() {
            self.pending_bytes = component_bytes;
        } else {
            self.pending_bytes.extend_from_slice(&component_bytes);
        }
        self.pending_components = self
            .pending_components
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        if self.pending_bytes.len() == INDEX_ARTIFACT_PACK_BYTES
            && let Some(pack) = self.reserve_pending_pack()?
        {
            reserved.push(pack);
        }
        Ok((descriptor, reserved))
    }

    fn complete(self) -> Result<CompletedSegmentPacks, IndexError> {
        let mut staged = Vec::with_capacity(self.staged.len());
        for slot in self.staged {
            match slot {
                StagedIndexPackSlot::Ready(pack) => staged.push(pack),
                StagedIndexPackSlot::Pending => {
                    return Err(IndexError::InvalidFormat(
                        "index pack staging slot is unresolved",
                    ));
                }
                StagedIndexPackSlot::Failed => {
                    return Err(IndexError::Io("index pack staging failed".into()));
                }
            }
        }
        Ok(CompletedSegmentPacks {
            base_packs: self.base_packs,
            staged,
        })
    }
}

fn deduplicate_staged_packs(
    packs: &[StagedIndexPack],
) -> Result<(Vec<usize>, Vec<usize>), IndexError> {
    let mut unique_by_hash = BTreeMap::<[u8; 32], usize>::new();
    let mut unique = Vec::<usize>::new();
    let mut outcomes = Vec::with_capacity(packs.len());
    for (pack_index, pack) in packs.iter().enumerate() {
        if let Some(&ordinal) = unique_by_hash.get(&pack.blob.hash) {
            if packs[unique[ordinal]].blob.length != pack.blob.length {
                return Err(IndexError::Integrity);
            }
            outcomes.push(ordinal);
        } else {
            let ordinal = unique.len();
            unique_by_hash.insert(pack.blob.hash, ordinal);
            unique.push(pack_index);
            outcomes.push(ordinal);
        }
    }
    Ok((unique, outcomes))
}

impl ComponentBatchSink for IndexComponentBatchSink {
    fn begin_segment(
        &mut self,
        identity: SegmentIdentity,
        base_packs: &[ArtifactPackReference],
    ) -> Result<(), IndexError> {
        identity.validate()?;
        let mut shared = self.lock_active()?;
        if shared.is_some() {
            return Err(IndexError::InvalidDefinition(
                "component sink already has an active segment".into(),
            ));
        }
        for pack in base_packs {
            pack.validate(identity.index_id)?;
        }
        *shared = Some(PendingSegmentPacks {
            identity,
            base_packs: base_packs.to_vec(),
            staged: Vec::new(),
            pending_bytes: Vec::new(),
            pending_components: 0,
            finalizing: false,
        });
        Ok(())
    }

    fn stage_component(
        &mut self,
        component: GeneratedComponent,
    ) -> impl std::future::Future<Output = Result<ArtifactDescriptor, IndexError>> + Send {
        async move { self.stage_component_inner(component).await }
    }

    fn finalize_segment(
        &mut self,
        identity: SegmentIdentity,
    ) -> impl std::future::Future<Output = Result<Vec<ArtifactPackReference>, IndexError>> + Send
    {
        async move { self.finalize_segment_inner(identity).await }
    }
}

impl IndexComponentBatchSink {
    fn lock_active(&self) -> Result<MutexGuard<'_, Option<PendingSegmentPacks>>, IndexError> {
        self.active
            .lock()
            .map_err(|_| IndexError::InvalidFormat("component sink mutex is poisoned"))
    }

    async fn stage_component_inner(
        &mut self,
        component: GeneratedComponent,
    ) -> Result<ArtifactDescriptor, IndexError> {
        let identity = component.header().identity;
        if identity.index_id != self.definition.index_id {
            return Err(IndexError::InvalidDefinition(
                "component publication reached the wrong index publisher".into(),
            ));
        }
        let (descriptor, reserved) = {
            let mut shared = self.lock_active()?;
            let active = shared.as_mut().ok_or(IndexError::InvalidFormat(
                "component sink has no active segment",
            ))?;
            active.reserve_component(component)?
        };
        for pack in reserved {
            self.stage_reserved_pack(pack).await?;
        }
        Ok(descriptor)
    }

    async fn stage_reserved_pack(&self, pack: ReservedIndexPack) -> Result<(), IndexError> {
        let result = stage_index_bytes(&self.store, &pack.bytes, self.admission).await;
        // Store staging is the content-address authority and has already
        // computed the BLAKE3 identity while writing these exact bytes.
        let result = match result {
            Ok(blob) if blob.length == pack.bytes.len() as u64 => Ok(StagedIndexPack {
                blob,
                component_count: pack.component_count,
            }),
            Ok(_) => Err(IndexError::Integrity),
            Err(error) => Err(error),
        };
        let mut shared = self.lock_active()?;
        let active = shared.as_mut().ok_or(IndexError::InvalidFormat(
            "component sink has no active segment",
        ))?;
        if active.identity != pack.identity {
            return Err(IndexError::InvalidDefinition(
                "component sink changed segment while staging a pack".into(),
            ));
        }
        let slot = active
            .staged
            .get_mut(pack.slot)
            .ok_or(IndexError::InvalidFormat(
                "index pack staging slot is missing",
            ))?;
        if !matches!(slot, StagedIndexPackSlot::Pending) {
            return Err(IndexError::InvalidFormat(
                "index pack staging slot resolved more than once",
            ));
        }
        match result {
            Ok(staged) => {
                *slot = StagedIndexPackSlot::Ready(staged);
                Ok(())
            }
            Err(error) => {
                *slot = StagedIndexPackSlot::Failed;
                Err(error)
            }
        }
    }

    async fn finalize_segment_inner(
        &mut self,
        identity: SegmentIdentity,
    ) -> Result<Vec<ArtifactPackReference>, IndexError> {
        let tail = {
            let mut shared = self.lock_active()?;
            let active = shared.as_mut().ok_or(IndexError::InvalidFormat(
                "component sink has no active segment",
            ))?;
            if active.identity != identity {
                return Err(IndexError::InvalidDefinition(
                    "component sink finalized another segment identity".into(),
                ));
            }
            if active.finalizing {
                return Err(IndexError::InvalidDefinition(
                    "component sink is already finalizing".into(),
                ));
            }
            active.finalizing = true;
            active.reserve_pending_pack()?
        };
        if let Some(pack) = tail {
            self.stage_reserved_pack(pack).await?;
        }
        let active = self
            .lock_active()?
            .take()
            .ok_or(IndexError::InvalidFormat(
                "component sink has no active segment",
            ))?
            .complete()?;
        let pack_count = active.staged.len() as u64;
        let encoded_bytes = active
            .staged
            .iter()
            .try_fold(0_u64, |total, pack| total.checked_add(pack.blob.length))
            .ok_or(IndexError::OffsetOverflow)?;
        let component_count = active
            .staged
            .iter()
            .try_fold(0_u64, |total, pack| total.checked_add(pack.component_count))
            .ok_or(IndexError::OffsetOverflow)?;
        let span = tracing::info_span!(
            "anvil.index.v4_component_publish",
            index.id = self.definition.index_id,
            tenant.id = self.tenant_id,
            bucket.id = self.bucket_id,
            component.count = component_count,
            component.bytes = encoded_bytes,
            pack.count = pack_count,
        );
        let started = std::time::Instant::now();
        let result = self
            .publish_staged_packs(active)
            .instrument(span.clone())
            .await;
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
                monotonic_counter.anvil_index_v4_packs_published_total =
                    if failed { 0 } else { pack_count },
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

    async fn publish_staged_packs(
        &self,
        active: CompletedSegmentPacks,
    ) -> Result<Vec<ArtifactPackReference>, IndexError> {
        let (unique_pack_indices, pack_outcomes) = deduplicate_staged_packs(&active.staged)?;
        let mut requests = Vec::with_capacity(unique_pack_indices.len());
        for pack_index in unique_pack_indices {
            let pack = &active.staged[pack_index];
            let path = artifact_path(self.definition.index_id, pack.blob.hash);
            requests.push(IndexArtifactPublish {
                storage_tenant: self.definition.tenant.clone(),
                bucket: self.definition.bucket.clone(),
                tenant_id: self.tenant_id,
                bucket_id: self.bucket_id,
                index_id: self.definition.index_id,
                exact_path: path.clone(),
                blob: pack.blob.clone(),
                expected_version: None,
                command_id: content_command(self.definition.index_id, &path, &pack.blob),
                definition_guard: None,
                definition_intent: None,
                admission: self.admission,
            });
        }
        let published_pack_count = requests.len();
        let outcomes = self
            .artifacts
            .publish_many(requests)
            .await
            .map_err(|error| IndexError::Io(error.to_string()))?;
        if outcomes.len() != published_pack_count {
            return Err(IndexError::InvalidFormat(
                "grouped pack outcome count differs from staged pack count",
            ));
        }
        let mut references = active.base_packs;
        references.reserve(active.staged.len());
        for (pack, outcome_ordinal) in active.staged.into_iter().zip(pack_outcomes) {
            let outcome = outcomes
                .get(outcome_ordinal)
                .ok_or(IndexError::InvalidFormat(
                    "grouped pack outcome ordinal is missing",
                ))?;
            let path = artifact_path(self.definition.index_id, pack.blob.hash);
            references.push(ArtifactPackReference::new(
                self.definition.index_id,
                path,
                outcome.version.0,
                pack.blob.hash,
                pack.blob.length,
            )?);
        }
        Ok(references)
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

    fn staged_pack(hash: u8, length: u64) -> StagedIndexPack {
        StagedIndexPack {
            blob: BlobRef {
                hash: [hash; 32],
                length,
            },
            component_count: 1,
        }
    }

    #[test]
    fn segment_pack_locations_follow_base_table_and_never_straddle() {
        let identity = SegmentIdentity::new(7, 3, [4; 32], 9).unwrap();
        let base = (0_u8..2)
            .map(|value| {
                let hash = [value; 32];
                ArtifactPackReference::new(
                    7,
                    artifact_path(7, hash),
                    u64::from(value) + 1,
                    hash,
                    128,
                )
                .unwrap()
            })
            .collect();
        let state = PendingSegmentPacks {
            identity,
            base_packs: base,
            staged: vec![StagedIndexPackSlot::Ready(staged_pack(3, 128))],
            pending_bytes: vec![0; INDEX_ARTIFACT_PACK_BYTES - 64],
            pending_components: 1,
            finalizing: false,
        };

        assert_eq!(
            state.next_component_location().unwrap(),
            (3, (INDEX_ARTIFACT_PACK_BYTES - 64) as u64)
        );
        assert!(!state.must_seal_before(64).unwrap());
        assert!(state.must_seal_before(65).unwrap());
    }

    #[test]
    fn cloned_lane_accumulator_reserves_nonoverlapping_pack_ranges() {
        let identity = SegmentIdentity::new(7, 3, [4; 32], 9).unwrap();
        let shared = Arc::new(Mutex::new(Some(PendingSegmentPacks {
            identity,
            base_packs: Vec::new(),
            staged: Vec::new(),
            pending_bytes: Vec::new(),
            pending_components: 0,
            finalizing: false,
        })));
        let mut lanes = Vec::new();
        for lane in 0..8_u8 {
            let shared = shared.clone();
            lanes.push(std::thread::spawn(move || {
                let mut descriptors = Vec::new();
                for ordinal in 0..80_u8 {
                    let payload = vec![lane ^ ordinal; 32 * 1024];
                    let component = anvil_index::v4::encode_component(
                        identity,
                        anvil_index::v4::ComponentKind::POSTINGS,
                        1,
                        0,
                        payload.len() as u64,
                        payload,
                    )
                    .unwrap();
                    let (descriptor, reserved) = shared
                        .lock()
                        .unwrap()
                        .as_mut()
                        .unwrap()
                        .reserve_component(component)
                        .unwrap();
                    drop(reserved);
                    descriptors.push(descriptor);
                }
                descriptors
            }));
        }
        let mut descriptors = lanes
            .into_iter()
            .flat_map(|lane| lane.join().unwrap())
            .collect::<Vec<_>>();
        let mut shared = shared.lock().unwrap();
        let state = shared.as_mut().unwrap();
        drop(state.reserve_pending_pack().unwrap());
        assert!(state.staged.len() >= 2);

        descriptors.sort_by_key(|descriptor| (descriptor.pack_ordinal, descriptor.offset));
        assert_eq!(descriptors.len(), 640);
        for descriptor in &descriptors {
            assert!(
                descriptor.offset + descriptor.encoded_length <= INDEX_ARTIFACT_PACK_BYTES as u64
            );
        }
        for pair in descriptors.windows(2) {
            if pair[0].pack_ordinal == pair[1].pack_ordinal {
                assert_eq!(pair[0].offset + pair[0].encoded_length, pair[1].offset);
            } else {
                assert!(pair[0].pack_ordinal < pair[1].pack_ordinal);
            }
        }
    }

    #[test]
    fn grouped_pack_publication_deduplicates_content_without_losing_ordinals() {
        let packs = [
            staged_pack(1, 100),
            staged_pack(1, 100),
            staged_pack(2, 200),
            staged_pack(1, 100),
        ];
        let (unique, outcomes) = deduplicate_staged_packs(&packs).unwrap();

        assert_eq!(unique, vec![0, 2]);
        assert_eq!(outcomes, vec![0, 0, 1, 0]);

        let inconsistent = [staged_pack(1, 100), staged_pack(1, 101)];
        assert!(matches!(
            deduplicate_staged_packs(&inconsistent),
            Err(IndexError::Integrity)
        ));
    }

    #[test]
    fn finalization_fails_closed_on_an_unresolved_staging_slot() {
        let identity = SegmentIdentity::new(7, 3, [4; 32], 9).unwrap();
        let state = PendingSegmentPacks {
            identity,
            base_packs: Vec::new(),
            staged: vec![StagedIndexPackSlot::Pending],
            pending_bytes: Vec::new(),
            pending_components: 0,
            finalizing: true,
        };

        assert!(matches!(
            state.complete(),
            Err(IndexError::InvalidFormat(
                "index pack staging slot is unresolved"
            ))
        ));
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
