//! Streaming v3 run and generation publication through ordinary objects.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};

use anvil_index::compaction::CompactionProgress;
use anvil_index::{
    BlockDescriptor, GeneratedBlock, IndexBlockSink, IndexDirectoryRead, IndexError, IndexFileRead,
    IndexKind, MAX_INDEX_ARTIFACT_PACK_BYTES, SealedRun,
};
use anvil_store::{BlobRef, BlobUpload, DefinitionKind, ObjectKey, Store, VersionId};
use tokio::io::AsyncWriteExt;
use tonic::Status;
use tracing::Instrument;

use crate::cluster_object_read::ClusterObjectReader;
use crate::index_service::{StoredIndexDefinition, definition_path};

use super::engine::IndexBuildDiagnostics;
use super::events::IndexBarrier;
use super::generation::{IndexCurrentPointer, IndexGenerationManifest, ManifestPack, ManifestRun};
use super::publication::{
    DefinitionVersionGuard, DerivedArtifactAdmission, IndexArtifactPublish, IndexArtifactRouter,
    current_path, manifest_path, run_pack_path, run_root_path,
};

const STAGED_READ_BUFFER_BYTES: usize = 64 * 1024;
static NEXT_INDEX_SCRATCH_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub(crate) struct IndexGenerationPublisher {
    store: Store,
    reader: ClusterObjectReader,
    artifacts: IndexArtifactRouter,
    scratch_root: Arc<PathBuf>,
}

#[derive(Clone, Debug)]
pub(crate) struct PublishedGeneration {
    pub(crate) pointer: IndexCurrentPointer,
    pub(crate) current_object_version: VersionId,
    pub(crate) manifest: IndexGenerationManifest,
}

#[derive(Clone, Debug)]
pub(crate) struct PublishedRun {
    pub(crate) manifest: ManifestRun,
}

impl IndexGenerationPublisher {
    pub(crate) fn new(
        store: Store,
        reader: ClusterObjectReader,
        artifacts: IndexArtifactRouter,
        scratch_root: PathBuf,
    ) -> Result<Self, std::io::Error> {
        match std::fs::remove_dir_all(&scratch_root) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        std::fs::create_dir_all(&scratch_root)?;
        Ok(Self {
            store,
            reader,
            artifacts,
            scratch_root: Arc::new(scratch_root),
        })
    }

    pub(crate) fn staging_sink(&self) -> IndexBlockStagingSink {
        self.staging_sink_with_admission(DerivedArtifactAdmission::PublicationProgress)
    }

    fn staging_sink_with_admission(
        &self,
        admission: DerivedArtifactAdmission,
    ) -> IndexBlockStagingSink {
        IndexBlockStagingSink::new(
            self.store.clone(),
            None,
            admission,
            self.scratch_root.as_ref(),
        )
    }

    pub(crate) fn observed_staging_sink(
        &self,
        progress: CompactionProgress,
        admission: DerivedArtifactAdmission,
    ) -> IndexBlockStagingSink {
        IndexBlockStagingSink::new(
            self.store.clone(),
            Some(progress),
            admission,
            self.scratch_root.as_ref(),
        )
    }

    /// Seal and publish every deterministic lane-local pack before publishing
    /// the standalone run root.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_run(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        sequence: u64,
        sealed: SealedRun,
        sink: IndexBlockStagingSink,
    ) -> Result<PublishedRun, Status> {
        self.publish_run_with_progress(
            definition, tenant_id, bucket_id, sequence, sealed, sink, None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_run_observed(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        sequence: u64,
        sealed: SealedRun,
        sink: IndexBlockStagingSink,
        progress: CompactionProgress,
    ) -> Result<PublishedRun, Status> {
        self.publish_run_with_progress(
            definition,
            tenant_id,
            bucket_id,
            sequence,
            sealed,
            sink,
            Some(progress),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_run_with_progress(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        sequence: u64,
        sealed: SealedRun,
        mut sink: IndexBlockStagingSink,
        progress: Option<CompactionProgress>,
    ) -> Result<PublishedRun, Status> {
        if sequence == 0 {
            return Err(Status::internal("index run sequence is zero"));
        }
        let descriptor = sealed.descriptor().clone();
        let root = sealed.into_root();
        let root_descriptor = root.descriptor().clone();
        if descriptor.hash != root_descriptor.hash
            || descriptor.kind != root_descriptor.kind
            || descriptor.encoded_bytes < root_descriptor.encoded_bytes
        {
            return Err(Status::data_loss(
                "sealed index run and root descriptor disagree",
            ));
        }
        let pack_span = tracing::info_span!(
            "anvil.index.pack",
            index.id = definition.index_id,
            tenant.id = tenant_id,
            bucket.id = bucket_id,
            index.kind = ?descriptor.kind,
            run.sequence = sequence,
            pack.phase = "seal",
        );
        let pack_started = Instant::now();
        let finished = sink.finish().instrument(pack_span.clone()).await;
        let finished = match finished {
            Ok(finished) => finished,
            Err(error) => {
                pack_span.in_scope(|| {
                    tracing::info!(
                        index.kind = ?descriptor.kind,
                        pack.phase = "seal",
                        publish.outcome = "failed",
                        monotonic_counter.anvil_index_pack_seal_failures_total = 1_u64,
                        histogram.anvil_index_pack_seal_duration_seconds =
                            pack_started.elapsed().as_secs_f64(),
                        "index artifact pack sealing failed"
                    );
                });
                return Err(index_status(error));
            }
        };
        let packs = finished.packs;
        let packed_bytes = packs.iter().map(|pack| pack.blob.length).sum::<u64>();
        let pack_count = packs.len() as u64;
        let fill_ratio =
            packed_bytes as f64 / pack_count.max(1) as f64 / MAX_INDEX_ARTIFACT_PACK_BYTES as f64;
        pack_span.in_scope(|| {
            tracing::info!(
                index.kind = ?descriptor.kind,
                pack.phase = "seal",
                monotonic_counter.anvil_index_pack_blocks_total =
                    finished.summary.authoritative_blocks,
                monotonic_counter.anvil_index_pack_bytes_total =
                    finished.summary.authoritative_bytes,
                monotonic_counter.anvil_index_sort_scratch_blocks_total =
                    finished.summary.scratch_blocks,
                monotonic_counter.anvil_index_sort_scratch_bytes_total =
                    finished.summary.scratch_bytes,
                monotonic_counter.anvil_index_packs_total = pack_count,
                histogram.anvil_index_pack_fill_ratio = fill_ratio,
                histogram.anvil_index_pack_seal_duration_seconds =
                    pack_started.elapsed().as_secs_f64(),
                "index artifact packs sealed"
            );
        });
        let pack_requests = pack_publication_requests(
            definition,
            tenant_id,
            bucket_id,
            descriptor.hash,
            &packs,
            sink.admission,
        );
        let publish_span = tracing::info_span!(
            "anvil.index.pack_publish",
            index.id = definition.index_id,
            tenant.id = tenant_id,
            bucket.id = bucket_id,
            index.kind = ?descriptor.kind,
            run.sequence = sequence,
            pack.count = pack_count,
            pack.bytes = packed_bytes,
        );
        let publish_started = Instant::now();
        let pack_outcomes = self
            .artifacts
            .publish_many(pack_requests)
            .instrument(publish_span.clone())
            .await;
        let pack_publish_failed = pack_outcomes.is_err();
        publish_span.in_scope(|| {
            tracing::info!(
                index.kind = ?descriptor.kind,
                publish.phase = "packs",
                publish.outcome = if pack_publish_failed { "failed" } else { "completed" },
                monotonic_counter.anvil_index_pack_publish_failures_total =
                    u64::from(pack_publish_failed),
                histogram.anvil_index_pack_publish_duration_seconds =
                    publish_started.elapsed().as_secs_f64(),
                histogram.anvil_index_pack_durability_seconds =
                    publish_started.elapsed().as_secs_f64(),
                "index artifact pack publication finished"
            );
        });
        let pack_outcomes = pack_outcomes?;
        let published_packs = manifest_packs_from_outcomes(packs, pack_outcomes)?;
        let root_started = Instant::now();
        let root_span = tracing::info_span!(
            "anvil.index.run_root_publish",
            index.id = definition.index_id,
            tenant.id = tenant_id,
            bucket.id = bucket_id,
            index.kind = ?descriptor.kind,
            run.sequence = sequence,
        );
        let root_blob = stage_generated_block(&self.store, root, sink.admission)
            .instrument(root_span.clone())
            .await;
        let root_blob = match root_blob {
            Ok(root_blob) => root_blob,
            Err(error) => {
                root_span.in_scope(|| {
                    tracing::info!(
                        index.kind = ?descriptor.kind,
                        publish.phase = "run_root",
                        publish.outcome = "failed",
                        monotonic_counter.anvil_index_run_root_publish_failures_total = 1_u64,
                        histogram.anvil_index_run_root_publish_duration_seconds =
                            root_started.elapsed().as_secs_f64(),
                        "index run root staging failed"
                    );
                });
                return Err(index_status(error));
            }
        };
        if let Some(progress) = &progress {
            progress.record_output(0, root_descriptor.encoded_bytes, 1);
        }
        let root_path = run_root_path(definition.index_id, descriptor.hash);
        let root_object_version = self
            .publish_immutable(
                definition,
                tenant_id,
                bucket_id,
                &root_path,
                root_blob.clone(),
                sink.admission,
            )
            .instrument(root_span.clone())
            .await;
        let root_failed = root_object_version.is_err();
        root_span.in_scope(|| {
            tracing::info!(
                index.kind = ?descriptor.kind,
                publish.phase = "run_root",
                publish.outcome = if root_failed { "failed" } else { "completed" },
                monotonic_counter.anvil_index_run_roots_published_total =
                    u64::from(!root_failed),
                monotonic_counter.anvil_index_run_root_publish_failures_total =
                    u64::from(root_failed),
                histogram.anvil_index_run_root_publish_duration_seconds =
                    root_started.elapsed().as_secs_f64(),
                histogram.anvil_index_run_root_bytes = root_descriptor.encoded_bytes,
                "index run root publication finished"
            );
        });
        let root_object_version = root_object_version?;
        let manifest = ManifestRun::from_descriptor(
            definition.index_id,
            sequence,
            &descriptor,
            root_blob,
            root_object_version,
            published_packs,
        )
        .map_err(generation_status)?;
        Ok(PublishedRun { manifest })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_manifest(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        definition_version: u64,
        kind: IndexKind,
        barrier: IndexBarrier,
        mut runs: Vec<ManifestRun>,
        diagnostics: IndexBuildDiagnostics,
        current: Option<&PublishedGeneration>,
        admission: DerivedArtifactAdmission,
    ) -> Result<PublishedGeneration, Status> {
        runs.sort_by_key(|run| run.sequence);
        let generation =
            match current {
                Some(current) => current.pointer.generation.checked_add(1).ok_or_else(|| {
                    Status::resource_exhausted("index generation revision overflow")
                })?,
                None => 1,
            };
        let previous = current.map(|current| current.pointer.as_manifest_reference());
        let manifest = IndexGenerationManifest::new(
            definition.index_id,
            generation,
            definition_version,
            kind,
            &barrier,
            runs,
            previous,
            diagnostics.accepted_objects,
            diagnostics.skipped_objects,
        )
        .map_err(generation_status)?;
        let manifest_bytes = manifest.encode().map_err(generation_status)?;
        let manifest_length = manifest_bytes.len() as u64;
        let manifest_started = Instant::now();
        let manifest_span = tracing::info_span!(
            "anvil.index.manifest_publish",
            index.id = definition.index_id,
            tenant.id = tenant_id,
            bucket.id = bucket_id,
            index.kind = ?kind,
            generation,
            manifest.bytes = manifest_length,
        );
        let manifest_result = async {
            let manifest_blob =
                stage_artifact_bytes(&self.store, &manifest_bytes, admission).await?;
            let path = manifest_path(definition.index_id, manifest_blob.hash);
            let manifest_object_version = self
                .publish_immutable(
                    definition,
                    tenant_id,
                    bucket_id,
                    &path,
                    manifest_blob.clone(),
                    admission,
                )
                .await?;
            Ok::<_, Status>((manifest_blob, manifest_object_version))
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
                "index generation manifest publication finished"
            );
        });
        let (manifest_blob, manifest_object_version) = manifest_result?;

        let pointer = IndexCurrentPointer::new(
            &manifest,
            manifest_blob,
            manifest_object_version,
            SystemTime::now(),
        )
        .map_err(generation_status)?;
        let pointer_bytes = pointer.encode().map_err(generation_status)?;
        let pointer_length = pointer_bytes.len() as u64;
        let current_path = current_path(definition.index_id);
        let current_started = Instant::now();
        let current_span = tracing::info_span!(
            "anvil.index.current_pointer_cas",
            index.id = definition.index_id,
            tenant.id = tenant_id,
            bucket.id = bucket_id,
            index.kind = ?kind,
            generation,
            current.bytes = pointer_length,
        );
        let current_result = async {
            let pointer_blob = stage_artifact_bytes(&self.store, &pointer_bytes, admission).await?;
            self.artifacts
                .publish(IndexArtifactPublish {
                    storage_tenant: definition.tenant.clone(),
                    bucket: definition.bucket.clone(),
                    tenant_id,
                    bucket_id,
                    index_id: definition.index_id,
                    exact_path: current_path.clone(),
                    blob: pointer_blob.clone(),
                    expected_version: current.map(|value| value.current_object_version),
                    command_id: content_command(definition.index_id, &current_path, &pointer_blob),
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
                "index current-pointer CAS finished"
            );
        });
        let outcome = current_result?;
        Ok(PublishedGeneration {
            pointer,
            current_object_version: outcome.version,
            manifest,
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
            current_path(definition.index_id),
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
        std::io::Read::read_to_end(&mut payload, &mut bytes)
            .map_err(|error| Status::internal(format!("read current index pointer: {error}")))?;
        let pointer = IndexCurrentPointer::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if pointer.index_id != definition.index_id {
            return Err(Status::data_loss(
                "current index pointer belongs to another index",
            ));
        }
        let manifest = self.load_manifest(&pointer).await?;
        if manifest.index_id != pointer.index_id
            || manifest.generation != pointer.generation
            || manifest.definition_version != pointer.definition_version
        {
            return Err(Status::data_loss(
                "current index pointer and manifest identity differ",
            ));
        }
        Ok(Some(PublishedGeneration {
            pointer,
            current_object_version: opened.version.id,
            manifest,
        }))
    }

    pub(crate) async fn load_manifest(
        &self,
        pointer: &IndexCurrentPointer,
    ) -> Result<IndexGenerationManifest, Status> {
        let bytes = self.reader.read_blob_bytes(&pointer.manifest_blob).await?;
        IndexGenerationManifest::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))
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

/// Move-only block sink used during build/merge. Plain clones share one
/// writer lane so a clone can act as a read directory. Explicit `fork()` calls
/// create deterministic lane-local pack streams for parallel writers.
pub(crate) struct IndexBlockStagingSink {
    store: Store,
    progress: Option<CompactionProgress>,
    admission: DerivedArtifactAdmission,
    registry: Arc<Mutex<IndexPackRegistry>>,
    lane: Arc<tokio::sync::Mutex<IndexPackLane>>,
    seal_permits: Arc<tokio::sync::Semaphore>,
    scratch_directory: Arc<PathBuf>,
}

#[derive(Default)]
struct IndexPackLane {
    id: u32,
    scratch: bool,
    current: Option<BlobUpload>,
    current_bytes: u64,
    packs: Vec<PackSealState>,
    /// Scratch bytes are disposable files addressed by this monotone bound.
    /// Keeping their descriptors here would make builder memory O(corpus).
    scratch_block_count: u32,
    scratch_bytes: u64,
    authoritative_blocks: u64,
    authoritative_bytes: u64,
}

impl IndexPackLane {
    fn record_written_scratch_block(&mut self, local_id: u32) -> Result<(), IndexError> {
        if local_id != self.scratch_block_count {
            return Err(IndexError::Integrity);
        }
        self.scratch_block_count = self
            .scratch_block_count
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(())
    }
}

struct IndexPackRegistry {
    next_lane_id: u32,
    lanes: BTreeMap<u32, Arc<tokio::sync::Mutex<IndexPackLane>>>,
    finished: bool,
}

const PACK_LANE_LIMIT: u32 = (1 << 12) - 1;
const PACKS_PER_LANE: u32 = 1 << 20;
const MAX_CONCURRENT_PACK_SEALS: usize = 4;

enum PackSealState {
    Sealing(PendingPackSeal),
    Sealed(BlobRef),
    Failed(PackSealFailure),
}

struct PendingPackSeal {
    task: tokio::task::JoinHandle<Result<BlobRef, PackSealFailure>>,
}

impl Drop for PendingPackSeal {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
enum PackSealFailure {
    Io(String),
    Integrity,
}

impl PackSealFailure {
    fn into_index_error(self) -> IndexError {
        match self {
            Self::Io(message) => IndexError::Io(message),
            Self::Integrity => IndexError::Integrity,
        }
    }
}

impl Clone for IndexBlockStagingSink {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            progress: self.progress.clone(),
            admission: self.admission,
            registry: self.registry.clone(),
            lane: self.lane.clone(),
            seal_permits: self.seal_permits.clone(),
            scratch_directory: self.scratch_directory.clone(),
        }
    }
}

impl IndexBlockStagingSink {
    fn new(
        store: Store,
        progress: Option<CompactionProgress>,
        admission: DerivedArtifactAdmission,
        scratch_root: &std::path::Path,
    ) -> Self {
        let scratch_id = NEXT_INDEX_SCRATCH_ID.fetch_add(1, Ordering::Relaxed);
        let scratch_directory =
            Arc::new(scratch_root.join(format!("candidate-{}-{scratch_id}", std::process::id())));
        let lane = Arc::new(tokio::sync::Mutex::new(IndexPackLane::default()));
        let mut lanes = BTreeMap::new();
        lanes.insert(0, lane.clone());
        Self {
            store,
            progress,
            admission,
            registry: Arc::new(Mutex::new(IndexPackRegistry {
                next_lane_id: 1,
                lanes,
                finished: false,
            })),
            lane,
            seal_permits: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_PACK_SEALS)),
            scratch_directory,
        }
    }

    async fn seal_current(&self, lane: &mut IndexPackLane) -> Result<(), IndexError> {
        let Some(upload) = lane.current.take() else {
            return Ok(());
        };
        if lane.current_bytes == 0 {
            return Err(IndexError::InvalidFormat("empty index artifact pack"));
        }
        let expected_length = lane.current_bytes;
        let local_id = u32::try_from(lane.packs.len()).map_err(|_| IndexError::OffsetOverflow)?;
        pack_id(lane.id, local_id)?;
        let permit = self
            .seal_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| IndexError::Io("index pack seal admission closed".into()))?;
        let store = self.store.clone();
        let admission = self.admission;
        let task = tokio::spawn(async move {
            let _permit = permit;
            let blob = seal_artifact_upload(&store, upload, admission)
                .await
                .map_err(|error| PackSealFailure::Io(error.to_string()))?;
            if blob.length != expected_length || blob.length > MAX_INDEX_ARTIFACT_PACK_BYTES as u64
            {
                return Err(PackSealFailure::Integrity);
            }
            Ok(blob)
        });
        lane.packs
            .push(PackSealState::Sealing(PendingPackSeal { task }));
        lane.current_bytes = 0;
        Ok(())
    }

    async fn resolve_pack(
        &self,
        lane: &mut IndexPackLane,
        local_id: usize,
    ) -> Result<BlobRef, IndexError> {
        let result = match lane
            .packs
            .get_mut(local_id)
            .ok_or_else(|| IndexError::FileNotFound(format!("staged pack {local_id}")))?
        {
            PackSealState::Sealing(pending) => match (&mut pending.task).await {
                Ok(result) => result,
                Err(error) => Err(PackSealFailure::Io(format!(
                    "index pack seal task failed: {error}"
                ))),
            },
            PackSealState::Sealed(blob) => return Ok(blob.clone()),
            PackSealState::Failed(error) => return Err(error.clone().into_index_error()),
        };
        let slot = lane
            .packs
            .get_mut(local_id)
            .expect("the resolved pack slot remains present");
        match result {
            Ok(blob) => {
                *slot = PackSealState::Sealed(blob.clone());
                Ok(blob)
            }
            Err(error) => {
                *slot = PackSealState::Failed(error.clone());
                Err(error.into_index_error())
            }
        }
    }

    async fn finish(&mut self) -> Result<FinishedPacks, IndexError> {
        let lanes = {
            let mut registry = self
                .registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if registry.finished {
                return Err(IndexError::InvalidDefinition(
                    "index block sink was finished more than once".into(),
                ));
            }
            registry.finished = true;
            registry
                .lanes
                .iter()
                .map(|(id, lane)| (*id, lane.clone()))
                .collect::<Vec<_>>()
        };
        let mut packs = Vec::new();
        let mut summary = StagingSummary::default();
        for (lane_id, lane) in lanes {
            let mut lane = lane.lock().await;
            if lane.scratch {
                lane.current.take();
                summary.scratch_blocks = summary
                    .scratch_blocks
                    .saturating_add(u64::from(lane.scratch_block_count));
                summary.scratch_bytes = summary.scratch_bytes.saturating_add(lane.scratch_bytes);
                continue;
            }
            self.seal_current(&mut lane).await?;
            summary.authoritative_blocks = summary
                .authoritative_blocks
                .saturating_add(lane.authoritative_blocks);
            summary.authoritative_bytes = summary
                .authoritative_bytes
                .saturating_add(lane.authoritative_bytes);
            for local_id in 0..lane.packs.len() {
                let blob = self.resolve_pack(&mut lane, local_id).await?;
                let local_id = u32::try_from(local_id).map_err(|_| IndexError::OffsetOverflow)?;
                packs.push(StagedPack {
                    id: pack_id(lane_id, local_id)?,
                    blob,
                });
            }
        }
        match tokio::fs::remove_dir_all(self.scratch_directory.as_ref()).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(IndexError::Io(error.to_string())),
        }
        Ok(FinishedPacks { packs, summary })
    }
}

struct FinishedPacks {
    packs: Vec<StagedPack>,
    summary: StagingSummary,
}

#[derive(Default)]
struct StagingSummary {
    authoritative_blocks: u64,
    authoritative_bytes: u64,
    scratch_blocks: u64,
    scratch_bytes: u64,
}

struct StagedPack {
    id: u32,
    blob: BlobRef,
}

fn pack_publication_requests(
    definition: &StoredIndexDefinition,
    tenant_id: u64,
    bucket_id: u64,
    run_hash: [u8; 32],
    packs: &[StagedPack],
    admission: DerivedArtifactAdmission,
) -> Vec<IndexArtifactPublish> {
    packs
        .iter()
        .map(|pack| {
            let path = run_pack_path(definition.index_id, run_hash, pack.id);
            IndexArtifactPublish {
                storage_tenant: definition.tenant.clone(),
                bucket: definition.bucket.clone(),
                tenant_id,
                bucket_id,
                index_id: definition.index_id,
                exact_path: path.clone(),
                command_id: content_command(definition.index_id, &path, &pack.blob),
                blob: pack.blob.clone(),
                expected_version: None,
                definition_guard: None,
                definition_intent: None,
                admission,
            }
        })
        .collect()
}

fn manifest_packs_from_outcomes(
    packs: Vec<StagedPack>,
    outcomes: Vec<super::publication::IndexArtifactOutcome>,
) -> Result<Vec<ManifestPack>, Status> {
    if packs.len() != outcomes.len() {
        return Err(Status::data_loss(
            "published pack outcome count differs from staged packs",
        ));
    }
    Ok(packs
        .into_iter()
        .zip(outcomes)
        .map(|(pack, outcome)| ManifestPack {
            id: pack.id,
            blob: pack.blob,
            object_version: outcome.version,
        })
        .collect())
}

impl IndexBlockSink for IndexBlockStagingSink {
    fn fork(&self) -> Result<Self, IndexError> {
        self.fork_lane(false)
    }

    fn fork_scratch(&self) -> Result<Self, IndexError> {
        self.fork_lane(true)
    }

    async fn discard_scratch_block(
        &mut self,
        descriptor: &BlockDescriptor,
    ) -> Result<(), IndexError> {
        let (lane_id, local_id) = unpack_id(descriptor.pack_id)?;
        let Some(lane) = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lanes
            .get(&lane_id)
            .cloned()
        else {
            return Ok(());
        };
        let lane = lane.lock().await;
        if !lane.scratch {
            return Ok(());
        }
        if local_id >= lane.scratch_block_count as usize || descriptor.pack_offset != 0 {
            return Err(IndexError::Integrity);
        }
        let local_id = u32::try_from(local_id).map_err(|_| IndexError::OffsetOverflow)?;
        let path = scratch_block_path(self.scratch_directory.as_ref(), lane_id, local_id);
        drop(lane);
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(IndexError::Io(error.to_string())),
        }
    }

    async fn emit(&mut self, block: GeneratedBlock) -> Result<BlockDescriptor, IndexError> {
        let (mut descriptor, bytes) = block.into_parts();
        if bytes.len() as u64 != descriptor.encoded_bytes
            || blake3::hash(&bytes).as_bytes() != &descriptor.hash
        {
            return Err(IndexError::Integrity);
        }
        let encoded_bytes = descriptor.encoded_bytes;
        if self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .finished
        {
            return Err(IndexError::InvalidDefinition(
                "cannot emit into a finished index block sink".into(),
            ));
        }
        let mut lane = self.lane.lock().await;
        if lane.scratch {
            tokio::fs::create_dir_all(self.scratch_directory.as_ref())
                .await
                .map_err(|error| IndexError::Io(error.to_string()))?;
            let local_id = lane.scratch_block_count;
            descriptor.pack_id = pack_id(lane.id, local_id)?;
            descriptor.pack_offset = 0;
            let path = scratch_block_path(self.scratch_directory.as_ref(), lane.id, local_id);
            let mut file = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .await
                .map_err(|error| IndexError::Io(error.to_string()))?;
            file.write_all(&bytes)
                .await
                .map_err(|error| IndexError::Io(error.to_string()))?;
            file.flush()
                .await
                .map_err(|error| IndexError::Io(error.to_string()))?;
            drop(file);
            lane.record_written_scratch_block(local_id)?;
            lane.scratch_bytes = lane.scratch_bytes.saturating_add(encoded_bytes);
        } else {
            if lane.current_bytes != 0
                && lane.current_bytes.saturating_add(descriptor.encoded_bytes)
                    > MAX_INDEX_ARTIFACT_PACK_BYTES as u64
            {
                self.seal_current(&mut lane).await?;
            }
            if lane.current.is_none() {
                lane.current = Some(
                    self.store
                        .begin_blob_upload()
                        .await
                        .map_err(|error| IndexError::Io(error.to_string()))?,
                );
            }
            let local_id =
                u32::try_from(lane.packs.len()).map_err(|_| IndexError::OffsetOverflow)?;
            descriptor.pack_id = pack_id(lane.id, local_id)?;
            descriptor.pack_offset = lane.current_bytes;
            lane.current
                .as_mut()
                .expect("index pack upload was opened")
                .write(&bytes)
                .await
                .map_err(|error| IndexError::Io(error.to_string()))?;
            lane.current_bytes = lane
                .current_bytes
                .checked_add(descriptor.encoded_bytes)
                .ok_or(IndexError::OffsetOverflow)?;
            lane.authoritative_blocks = lane.authoritative_blocks.saturating_add(1);
            lane.authoritative_bytes = lane.authoritative_bytes.saturating_add(encoded_bytes);
        }
        drop(lane);
        if let Some(progress) = &self.progress {
            progress.record_output(0, encoded_bytes, 1);
        }
        Ok(descriptor)
    }
}

impl IndexBlockStagingSink {
    fn fork_lane(&self, scratch: bool) -> Result<Self, IndexError> {
        let lane = {
            let mut registry = self
                .registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if registry.finished || registry.next_lane_id >= PACK_LANE_LIMIT {
                return Err(IndexError::ResourceLimit {
                    needed: registry.next_lane_id as usize + 1,
                    limit: PACK_LANE_LIMIT as usize,
                });
            }
            let id = registry.next_lane_id;
            registry.next_lane_id += 1;
            let lane = Arc::new(tokio::sync::Mutex::new(IndexPackLane {
                id,
                scratch,
                ..IndexPackLane::default()
            }));
            registry.lanes.insert(id, lane.clone());
            lane
        };
        Ok(Self {
            store: self.store.clone(),
            progress: self.progress.clone(),
            admission: self.admission,
            registry: self.registry.clone(),
            lane,
            seal_permits: self.seal_permits.clone(),
            scratch_directory: self.scratch_directory.clone(),
        })
    }
}

// Some compactions must reread an already-emitted path/document block to
// build dense run-local ordinals. The sink exposes those exact staged BlobRefs
// through the same descriptor API; this is still the ordinary byte plane and
// creates no scratch namespace or second authority.
impl IndexDirectoryRead for IndexBlockStagingSink {
    type File = StagedIndexFile;

    async fn open_root(&self) -> Result<Self::File, IndexError> {
        Err(IndexError::FileNotFound("unsealed run root".into()))
    }

    async fn open_block(&self, descriptor: &BlockDescriptor) -> Result<Self::File, IndexError> {
        let (lane_id, local_id) = unpack_id(descriptor.pack_id)?;
        let lane = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lanes
            .get(&lane_id)
            .cloned()
            .ok_or_else(|| IndexError::FileNotFound(descriptor.logical_name()))?;
        let staged = {
            let mut lane = lane.lock().await;
            if lane.scratch {
                if local_id >= lane.scratch_block_count as usize {
                    return Err(IndexError::FileNotFound(descriptor.logical_name()));
                }
                if descriptor.pack_offset != 0 {
                    return Err(IndexError::Integrity);
                }
                Some(scratch_block_path(
                    self.scratch_directory.as_ref(),
                    lane_id,
                    local_id as u32,
                ))
            } else {
                if local_id == lane.packs.len() && lane.current.is_some() {
                    self.seal_current(&mut lane).await?;
                }
                None
            }
        };
        if let Some(path) = staged {
            return StagedIndexFile::open_scratch(&path, descriptor.encoded_bytes, descriptor.hash)
                .await;
        }
        let blob = {
            let mut lane = lane.lock().await;
            self.resolve_pack(&mut lane, local_id).await?
        };
        StagedIndexFile::open_window(
            &self.store,
            &blob,
            descriptor.pack_offset,
            descriptor.encoded_bytes,
            descriptor.hash,
        )
        .await
    }
}

fn scratch_block_path(root: &std::path::Path, lane_id: u32, local_id: u32) -> PathBuf {
    root.join(format!("lane-{lane_id:04}-block-{local_id:07}"))
}

impl Drop for IndexBlockStagingSink {
    fn drop(&mut self) {
        if Arc::strong_count(&self.registry) == 1
            && let Ok(registry) = self.registry.lock()
            && !registry.finished
        {
            let _ = std::fs::remove_dir_all(self.scratch_directory.as_ref());
            tracing::debug!(
                staged_lanes = registry.lanes.len(),
                "unfinished index pack upload will be removed"
            );
        }
    }
}

fn pack_id(lane_id: u32, local_id: u32) -> Result<u32, IndexError> {
    if lane_id >= PACK_LANE_LIMIT || local_id >= PACKS_PER_LANE {
        return Err(IndexError::OffsetOverflow);
    }
    let id = lane_id
        .checked_mul(PACKS_PER_LANE)
        .and_then(|base| base.checked_add(local_id))
        .ok_or(IndexError::OffsetOverflow)?;
    if id == u32::MAX {
        return Err(IndexError::OffsetOverflow);
    }
    Ok(id)
}

fn unpack_id(id: u32) -> Result<(u32, usize), IndexError> {
    if id == u32::MAX {
        return Err(IndexError::InvalidFormat("unplaced index artifact pack"));
    }
    let lane_id = id / PACKS_PER_LANE;
    if lane_id >= PACK_LANE_LIMIT {
        return Err(IndexError::InvalidFormat("index artifact pack lane"));
    }
    Ok((lane_id, (id % PACKS_PER_LANE) as usize))
}

async fn stage_generated_block(
    store: &Store,
    block: GeneratedBlock,
    admission: DerivedArtifactAdmission,
) -> Result<BlobRef, IndexError> {
    let descriptor = block.descriptor().clone();
    let (_, bytes) = block.into_parts();
    if bytes.len() as u64 != descriptor.encoded_bytes
        || blake3::hash(&bytes).as_bytes() != &descriptor.hash
    {
        return Err(IndexError::Integrity);
    }
    let blob = match admission {
        DerivedArtifactAdmission::Bounded => store.stage_blob(&bytes).await,
        DerivedArtifactAdmission::PublicationProgress => {
            store.stage_derived_progress_blob(&bytes).await
        }
    }
    .map_err(|error| IndexError::Io(error.to_string()))?;
    if blob.hash != descriptor.hash || blob.length != descriptor.encoded_bytes {
        return Err(IndexError::Integrity);
    }
    Ok(blob)
}

async fn stage_artifact_bytes(
    store: &Store,
    bytes: &[u8],
    admission: DerivedArtifactAdmission,
) -> Result<BlobRef, Status> {
    match admission {
        DerivedArtifactAdmission::Bounded => store.stage_blob(bytes).await,
        DerivedArtifactAdmission::PublicationProgress => {
            store.stage_derived_progress_blob(bytes).await
        }
    }
    .map_err(store_status)
}

async fn seal_artifact_upload(
    store: &Store,
    upload: BlobUpload,
    admission: DerivedArtifactAdmission,
) -> Result<BlobRef, anvil_store::MutationError> {
    match admission {
        DerivedArtifactAdmission::Bounded => store.seal_blob_upload(upload).await,
        DerivedArtifactAdmission::PublicationProgress => {
            store.seal_derived_progress_blob_upload(upload).await
        }
    }
}

pub(crate) struct StagedIndexFile {
    bytes: Arc<[u8]>,
    start: usize,
    end: usize,
}

impl StagedIndexFile {
    async fn open_scratch(
        path: &std::path::Path,
        logical_length: u64,
        logical_hash: [u8; 32],
    ) -> Result<Self, IndexError> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|error| IndexError::Io(error.to_string()))?;
        if bytes.len() as u64 != logical_length || blake3::hash(&bytes).as_bytes() != &logical_hash
        {
            return Err(IndexError::Integrity);
        }
        let end = bytes.len();
        Ok(Self {
            bytes: bytes.into(),
            start: 0,
            end,
        })
    }

    async fn open_window(
        store: &Store,
        blob: &BlobRef,
        offset: u64,
        logical_length: u64,
        logical_hash: [u8; 32],
    ) -> Result<Self, IndexError> {
        let length = usize::try_from(blob.length).map_err(|_| IndexError::OffsetOverflow)?;
        let mut reader = store
            .open_blob(blob)
            .await
            .map_err(|error| IndexError::Io(error.to_string()))?;
        let mut bytes = Vec::with_capacity(length);
        let mut buffer = vec![0_u8; STAGED_READ_BUFFER_BYTES.min(length.max(1))];
        while bytes.len() < length {
            let read = reader
                .read(&mut buffer)
                .await
                .map_err(|error| IndexError::Io(error.to_string()))?;
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
        }
        if bytes.len() != length || blake3::hash(&bytes).as_bytes() != &blob.hash {
            return Err(IndexError::Integrity);
        }
        let start = usize::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?;
        let logical_length =
            usize::try_from(logical_length).map_err(|_| IndexError::OffsetOverflow)?;
        let end = start
            .checked_add(logical_length)
            .ok_or(IndexError::OffsetOverflow)?;
        if end > bytes.len() || blake3::hash(&bytes[start..end]).as_bytes() != &logical_hash {
            return Err(IndexError::Integrity);
        }
        Ok(Self {
            bytes: bytes.into(),
            start,
            end,
        })
    }
}

impl IndexFileRead for StagedIndexFile {
    type Slice = StagedIndexSlice;

    async fn read_at(&self, offset: u64, max_length: usize) -> Result<Self::Slice, IndexError> {
        let relative = usize::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?;
        let length = self.end.saturating_sub(self.start);
        if max_length == 0 || relative >= length {
            return Ok(StagedIndexSlice {
                bytes: self.bytes.clone(),
                start: 0,
                end: 0,
            });
        }
        let start = self
            .start
            .checked_add(relative)
            .ok_or(IndexError::OffsetOverflow)?;
        let end = start.saturating_add(max_length).min(self.end);
        Ok(StagedIndexSlice {
            bytes: self.bytes.clone(),
            start,
            end,
        })
    }
}

pub(crate) struct StagedIndexSlice {
    bytes: Arc<[u8]>,
    start: usize,
    end: usize,
}

impl AsRef<[u8]> for StagedIndexSlice {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[self.start..self.end]
    }
}

fn content_command(index_id: u64, path: &str, blob: &BlobRef) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(path.as_bytes());
    hasher.update(&blob.hash);
    hasher.update(&blob.length.to_be_bytes());
    let digest = hasher.finalize();
    format!("index-v3-{index_id}-{}", &digest.to_hex().as_str()[..24])
}

fn store_status(error: anvil_store::MutationError) -> Status {
    Status::internal(error.to_string())
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
    use anvil_api::v1::{CreateIndexRequest, IndexSpecification, PathIndexSpec};

    use super::*;

    fn test_definition() -> StoredIndexDefinition {
        StoredIndexDefinition::create(
            "tenant".into(),
            CreateIndexRequest {
                bucket: "bucket".into(),
                name: "path".into(),
                path_prefix: String::new(),
                content_type: String::new(),
                specification: Some(IndexSpecification {
                    specification: Some(anvil_api::v1::index_specification::Specification::Path(
                        PathIndexSpec {},
                    )),
                }),
                command_id: "create-path-index".into(),
            },
            7,
        )
        .unwrap()
    }

    fn scratch_descriptor(lane_id: u32, local_id: u32, bytes: &[u8]) -> BlockDescriptor {
        BlockDescriptor {
            kind: IndexKind::Path,
            component_tag: 1,
            codec: anvil_index::ComponentCodec::FixedRows,
            routing_height: 0,
            minimum_key: vec![0],
            maximum_key: vec![0],
            element_count: 1,
            encoded_bytes: bytes.len() as u64,
            hash: *blake3::hash(bytes).as_bytes(),
            pack_id: pack_id(lane_id, local_id).unwrap(),
            pack_offset: 0,
        }
    }

    #[test]
    fn lane_local_pack_ids_are_deterministic_and_reversible() {
        for (lane, local) in [(0, 0), (0, 1_048_575), (1, 0), (7, 41), (4_094, 1_048_575)] {
            let id = pack_id(lane, local).unwrap();
            assert_ne!(id, u32::MAX);
            assert_eq!(unpack_id(id).unwrap(), (lane, local as usize));
        }
        assert!(pack_id(4_095, 0).is_err());
        assert!(pack_id(0, 1_048_576).is_err());
        assert!(unpack_id(4_095 << 20).is_err());
        assert!(unpack_id(u32::MAX).is_err());
    }

    #[tokio::test]
    async fn authoritative_pack_seals_share_one_bounded_pool_and_finish_in_id_order() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(anvil_store::StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let scratch_root = temporary.path().join("index-scratch");
        let mut sink = IndexBlockStagingSink::new(
            store.clone(),
            None,
            DerivedArtifactAdmission::Bounded,
            &scratch_root,
        );
        let fork = sink.fork().unwrap();
        assert!(Arc::ptr_eq(&sink.seal_permits, &fork.seal_permits));
        assert_eq!(
            sink.seal_permits.available_permits(),
            MAX_CONCURRENT_PACK_SEALS
        );

        let mut expected = Vec::new();
        for (writer, lane_id, payloads) in [
            (
                &sink,
                0_u32,
                [b"lane-zero-pack-zero".as_slice(), b"lane-zero-pack-one"],
            ),
            (
                &fork,
                1_u32,
                [b"lane-one-pack-zero".as_slice(), b"lane-one-pack-one"],
            ),
        ] {
            for (local_id, bytes) in payloads.into_iter().enumerate() {
                let mut upload = store.begin_blob_upload().await.unwrap();
                upload.write(bytes).await.unwrap();
                let mut lane = writer.lane.lock().await;
                lane.current = Some(upload);
                lane.current_bytes = bytes.len() as u64;
                writer.seal_current(&mut lane).await.unwrap();
                expected.push((
                    pack_id(lane_id, local_id as u32).unwrap(),
                    BlobRef {
                        hash: *blake3::hash(bytes).as_bytes(),
                        length: bytes.len() as u64,
                    },
                ));
            }
        }

        let finished = sink.finish().await.unwrap();
        assert_eq!(
            finished
                .packs
                .iter()
                .map(|pack| (pack.id, pack.blob.clone()))
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            sink.seal_permits.available_permits(),
            MAX_CONCURRENT_PACK_SEALS
        );
    }

    #[tokio::test]
    async fn staged_read_waits_for_an_in_flight_pack_seal() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(anvil_store::StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let scratch_root = temporary.path().join("index-scratch");
        let mut sink = IndexBlockStagingSink::new(
            store.clone(),
            None,
            DerivedArtifactAdmission::Bounded,
            &scratch_root,
        );
        let bytes = b"block read while its containing pack is sealing";
        let mut upload = store.begin_blob_upload().await.unwrap();
        upload.write(bytes).await.unwrap();
        {
            let mut lane = sink.lane.lock().await;
            lane.current = Some(upload);
            lane.current_bytes = bytes.len() as u64;
            sink.seal_current(&mut lane).await.unwrap();
        }

        let descriptor = scratch_descriptor(0, 0, bytes);
        let file = sink.open_block(&descriptor).await.unwrap();
        assert_eq!(file.read_at(0, bytes.len()).await.unwrap().as_ref(), bytes);
        let finished = sink.finish().await.unwrap();
        assert_eq!(finished.packs.len(), 1);
        assert_eq!(finished.packs[0].id, 0);
        assert_eq!(finished.packs[0].blob.hash, descriptor.hash);
    }

    #[tokio::test]
    async fn disposable_scratch_blocks_never_enter_object_writes_or_manifests() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(anvil_store::StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let published_blob = store.stage_blob(b"authoritative pack").await.unwrap();
        let scratch_root = temporary.path().join("index-scratch");
        let mut sink = IndexBlockStagingSink::new(
            store,
            None,
            DerivedArtifactAdmission::Bounded,
            &scratch_root,
        );
        sink.lane
            .lock()
            .await
            .packs
            .push(PackSealState::Sealed(published_blob.clone()));
        let scratch = sink.fork_scratch().unwrap();
        let bytes = b"disposable external-sort block";
        tokio::fs::create_dir_all(scratch.scratch_directory.as_ref())
            .await
            .unwrap();
        let lane_id = scratch.lane.lock().await.id;
        let path = scratch_block_path(scratch.scratch_directory.as_ref(), lane_id, 0);
        tokio::fs::write(&path, bytes).await.unwrap();
        scratch.lane.lock().await.scratch_block_count = 1;

        let finished = sink.finish().await.unwrap();
        assert_eq!(finished.packs.len(), 1);
        assert_eq!(finished.packs[0].blob, published_blob);
        assert_eq!(finished.summary.authoritative_bytes, 0);
        let staged = finished.packs;
        let definition = test_definition();
        let requests = pack_publication_requests(
            &definition,
            1,
            2,
            [9; 32],
            &staged,
            DerivedArtifactAdmission::Bounded,
        );
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].blob, published_blob);
        assert_ne!(requests[0].blob.hash, *blake3::hash(bytes).as_bytes());
        let manifest = manifest_packs_from_outcomes(
            staged,
            vec![super::super::publication::IndexArtifactOutcome {
                version: VersionId(11),
                replayed: false,
            }],
        )
        .unwrap();
        assert_eq!(manifest.len(), 1);
        assert_eq!(manifest[0].blob, published_blob);
        assert_ne!(manifest[0].blob.hash, *blake3::hash(bytes).as_bytes());
        assert!(!scratch.scratch_directory.exists());
    }

    #[tokio::test]
    async fn many_scratch_blocks_leave_only_a_monotone_count_in_lane_state() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(anvil_store::StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let sink = IndexBlockStagingSink::new(
            store,
            None,
            DerivedArtifactAdmission::Bounded,
            &temporary.path().join("index-scratch"),
        );
        let mut scratch = sink.fork_scratch().unwrap();
        let lane_id = scratch.lane.lock().await.id;
        let represented_blocks = 50_000_u32;
        {
            let mut lane = scratch.lane.lock().await;
            for local_id in 0..represented_blocks {
                lane.record_written_scratch_block(local_id).unwrap();
            }
        }

        let bytes = b"last scratch block";
        tokio::fs::create_dir_all(scratch.scratch_directory.as_ref())
            .await
            .unwrap();
        let local_id = represented_blocks - 1;
        let path = scratch_block_path(scratch.scratch_directory.as_ref(), lane_id, local_id);
        tokio::fs::write(&path, bytes).await.unwrap();
        let descriptor = scratch_descriptor(lane_id, local_id, bytes);
        let file = scratch.open_block(&descriptor).await.unwrap();
        assert_eq!(file.read_at(0, bytes.len()).await.unwrap().as_ref(), bytes);

        let mut corrupt = descriptor.clone();
        corrupt.hash = [0; 32];
        assert!(matches!(
            scratch.open_block(&corrupt).await,
            Err(IndexError::Integrity)
        ));
        scratch.discard_scratch_block(&descriptor).await.unwrap();
        assert!(!path.exists());
        let lane = scratch.lane.lock().await;
        assert_eq!(lane.scratch_block_count, represented_blocks);
        assert!(lane.packs.is_empty());
    }
}
