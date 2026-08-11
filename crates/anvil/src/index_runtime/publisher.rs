//! Streaming v2 run and generation publication through ordinary objects.

use std::io::Read;
use std::sync::Arc;
use std::time::SystemTime;

use anvil_index::compaction::CompactionProgress;
use anvil_index::{
    BlockDescriptor, GeneratedBlock, IndexBlockSink, IndexDirectoryRead, IndexError, IndexFileRead,
    IndexKind, RunBlockWalker, SealedRun,
};
use anvil_store::{BlobRef, DefinitionKind, ObjectKey, Store, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::index_service::{StoredIndexDefinition, definition_path};

use super::engine::IndexBuildDiagnostics;
use super::events::IndexBarrier;
use super::generation::{IndexCurrentPointer, IndexGenerationManifest, ManifestRun};
use super::publication::{
    DefinitionVersionGuard, IndexArtifactPublish, IndexArtifactRouter, current_path, manifest_path,
    run_block_path, run_root_path,
};

const STAGED_READ_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub(crate) struct IndexGenerationPublisher {
    store: Store,
    reader: ClusterObjectReader,
    artifacts: IndexArtifactRouter,
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
    ) -> Self {
        Self {
            store,
            reader,
            artifacts,
        }
    }

    pub(crate) fn staging_sink(&self) -> IndexBlockStagingSink {
        IndexBlockStagingSink {
            store: self.store.clone(),
            progress: None,
        }
    }

    pub(crate) fn observed_staging_sink(
        &self,
        progress: CompactionProgress,
    ) -> IndexBlockStagingSink {
        IndexBlockStagingSink {
            store: self.store.clone(),
            progress: Some(progress),
        }
    }

    /// Validate and publish every staged descendant before publishing the run
    /// root. The walker retains only one routing page and one block at a time.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_run(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        sequence: u64,
        sealed: SealedRun,
    ) -> Result<PublishedRun, Status> {
        self.publish_run_with_progress(definition, tenant_id, bucket_id, sequence, sealed, None)
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
        progress: CompactionProgress,
    ) -> Result<PublishedRun, Status> {
        self.publish_run_with_progress(
            definition,
            tenant_id,
            bucket_id,
            sequence,
            sealed,
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
        let root_blob = stage_generated_block(&self.store, root)
            .await
            .map_err(index_status)?;
        if let Some(progress) = &progress {
            progress.record_output(0, root_descriptor.encoded_bytes, 1);
        }
        let directory = StagedRunDirectory {
            store: self.store.clone(),
            root: root_blob.clone(),
        };
        let mut walker = RunBlockWalker::open(&directory, root_descriptor)
            .await
            .map_err(index_status)?;
        while let Some(block) = walker.next().await.map_err(index_status)? {
            let blob = BlobRef {
                hash: block.hash,
                length: block.encoded_bytes,
            };
            let path = run_block_path(definition.index_id, descriptor.hash, block.hash);
            self.publish_immutable(definition, tenant_id, bucket_id, &path, blob)
                .await?;
        }
        let root_path = run_root_path(definition.index_id, descriptor.hash);
        let root_object_version = self
            .publish_immutable(
                definition,
                tenant_id,
                bucket_id,
                &root_path,
                root_blob.clone(),
            )
            .await?;
        let manifest = ManifestRun::from_descriptor(
            definition.index_id,
            sequence,
            &descriptor,
            root_blob,
            root_object_version,
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
        let manifest_blob = self
            .store
            .stage_blob(&manifest_bytes)
            .await
            .map_err(store_status)?;
        let path = manifest_path(definition.index_id, manifest_blob.hash);
        let manifest_object_version = self
            .publish_immutable(
                definition,
                tenant_id,
                bucket_id,
                &path,
                manifest_blob.clone(),
            )
            .await?;

        let pointer = IndexCurrentPointer::new(
            &manifest,
            manifest_blob,
            manifest_object_version,
            SystemTime::now(),
        )
        .map_err(generation_status)?;
        let pointer_bytes = pointer.encode().map_err(generation_status)?;
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
                blob: pointer_blob.clone(),
                expected_version: current.map(|value| value.current_object_version),
                command_id: content_command(definition.index_id, &current_path, &pointer_blob),
                definition_guard: Some(DefinitionVersionGuard {
                    kind: DefinitionKind::Index,
                    exact_path: definition_path(&definition.name)?,
                    expected_version: VersionId(definition_version),
                }),
                definition_intent: None,
            })
            .await?;
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
            })
            .await?;
        Ok(outcome.version)
    }
}

/// Move-only block sink used during build/merge. It stages ordinary bytes and
/// retains no block list; the sealed root later drives bounded publication.
#[derive(Clone)]
pub(crate) struct IndexBlockStagingSink {
    store: Store,
    progress: Option<CompactionProgress>,
}

impl IndexBlockSink for IndexBlockStagingSink {
    async fn emit(&mut self, block: GeneratedBlock) -> Result<(), IndexError> {
        let encoded_bytes = block.descriptor().encoded_bytes;
        stage_generated_block(&self.store, block).await?;
        if let Some(progress) = &self.progress {
            progress.record_output(0, encoded_bytes, 1);
        }
        Ok(())
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
        StagedIndexFile::open(
            &self.store,
            &BlobRef {
                hash: descriptor.hash,
                length: descriptor.encoded_bytes,
            },
        )
        .await
    }
}

async fn stage_generated_block(
    store: &Store,
    block: GeneratedBlock,
) -> Result<BlobRef, IndexError> {
    let descriptor = block.descriptor().clone();
    let (_, bytes) = block.into_parts();
    if bytes.len() as u64 != descriptor.encoded_bytes
        || blake3::hash(&bytes).as_bytes() != &descriptor.hash
    {
        return Err(IndexError::Integrity);
    }
    let blob = store
        .stage_blob(&bytes)
        .await
        .map_err(|error| IndexError::Io(error.to_string()))?;
    if blob.hash != descriptor.hash || blob.length != descriptor.encoded_bytes {
        return Err(IndexError::Integrity);
    }
    Ok(blob)
}

struct StagedRunDirectory {
    store: Store,
    root: BlobRef,
}

impl IndexDirectoryRead for StagedRunDirectory {
    type File = StagedIndexFile;

    async fn open_root(&self) -> Result<Self::File, IndexError> {
        StagedIndexFile::open(&self.store, &self.root).await
    }

    async fn open_block(&self, descriptor: &BlockDescriptor) -> Result<Self::File, IndexError> {
        StagedIndexFile::open(
            &self.store,
            &BlobRef {
                hash: descriptor.hash,
                length: descriptor.encoded_bytes,
            },
        )
        .await
    }
}

pub(crate) struct StagedIndexFile {
    bytes: Arc<[u8]>,
}

impl StagedIndexFile {
    async fn open(store: &Store, blob: &BlobRef) -> Result<Self, IndexError> {
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
        Ok(Self {
            bytes: bytes.into(),
        })
    }
}

impl IndexFileRead for StagedIndexFile {
    type Slice = StagedIndexSlice;

    async fn read_at(&self, offset: u64, max_length: usize) -> Result<Self::Slice, IndexError> {
        let start = usize::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?;
        if max_length == 0 || start >= self.bytes.len() {
            return Ok(StagedIndexSlice {
                bytes: self.bytes.clone(),
                start: 0,
                end: 0,
            });
        }
        let end = start.saturating_add(max_length).min(self.bytes.len());
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
    format!("index-v2-{index_id}-{}", &digest.to_hex().as_str()[..24])
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
