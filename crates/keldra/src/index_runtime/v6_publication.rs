//! Format-v6 partition publication over ordinary Keldra objects.
//!
//! Immutable artifacts are family-scoped and content addressed. The only
//! partition-owned mutable object is `current`, installed after every object
//! reachable from the generation is durable.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Read;

use keldra_index::v6::{
    CanonicalRecipeState, ComponentIdentity, ComponentRecordLookup, ComponentRoot,
    ComponentStreamReverseCursor, ComponentStreamReverseStep, ComponentStreamRoot,
    PreparedAtomicProjectionGeneration, PreparedQueryMutationBatch, ProjectedDocumentState,
    ProjectionCatalogActivation, ProjectionCurrent, ProjectionFamilyPartitionDirectory,
    ProjectionGeneration, ProjectionGenerationReference, ProjectionPackCredits,
    ProjectionPartitionIdentity, QueryBlockCredits, QueryBlockLimits, QueryRunPage,
    StableDocumentKey, component_stream_child_hashes, decode_document_head,
    decode_projection_catalog_activation, decode_projection_current,
    decode_projection_family_directory, decode_projection_generation,
    decode_projection_generation_header, decode_query_run_page, decode_source_records,
    encode_projection_catalog_activation, encode_projection_family_directory,
    lookup_component_record_in_pack, prepare_atomic_projection_generation,
    projection_artifact_routing_id, projection_catalog_activation_path,
    projection_catalog_routing_id, projection_component_page_path, projection_current_path,
    projection_family_directory_path, projection_generation_path, projection_pack_path,
    projection_query_run_pack_path, projection_query_run_stream_page_path, projection_routing_id,
    projection_stream_page_path,
};
use keldra_store::{BlobRef, MutationError, ObjectKey, Store, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;

use super::publication::{DerivedArtifactAdmission, IndexArtifactPublish, IndexArtifactRouter};
use super::v6_compaction::{V6CompactionArtifacts, V6CompactionBase};

const MAX_STREAM_DIRECTORY_BYTES: usize = 64 * 1024 * 1024;
const MAX_STREAM_PAGE_BYTES: usize = 32 * 1024;
const MAX_GENERATION_BYTES: usize = 256 * 1024;
const MAX_FAMILY_DIRECTORY_BYTES: usize = 32 * 1024 * 1024;
const MAX_CATALOG_ACTIVATION_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct V6ProjectionPublisher {
    store: Store,
    reader: ClusterObjectReader,
    artifacts: IndexArtifactRouter,
}

#[derive(Clone, Debug)]
pub(crate) struct LoadedV6ProjectionGeneration {
    pub(crate) current: ProjectionCurrent,
    pub(crate) current_object_version: VersionId,
    pub(crate) generation: ProjectionGeneration,
}

#[derive(Debug)]
struct ArtifactBytes {
    path: String,
    kind: keldra_index::v6::ProjectionArtifactKind,
    hash: [u8; 32],
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct AtomicPublicationPlan {
    immutable: Vec<ArtifactBytes>,
    current_bytes: Vec<u8>,
    current: ProjectionCurrent,
    generation: ProjectionGeneration,
    sealed_bytes: u64,
    source_rows: u64,
}

impl V6ProjectionPublisher {
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn prepare_atomic_generation(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        physical_catalog_generation: [u8; 32],
        previous: Option<&LoadedV6ProjectionGeneration>,
        source_start_offset: u64,
        next_offset: u64,
        through_atomic_position: u64,
        deltas: Vec<keldra_index::v6::SealedComponentDelta>,
        query_batch: PreparedQueryMutationBatch,
        query_credits: QueryBlockCredits,
        pack_credits: ProjectionPackCredits,
        maximum_preload_bytes: usize,
    ) -> Result<PreparedAtomicProjectionGeneration, Status> {
        self.prepare_atomic_generation_inner(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            partition,
            physical_catalog_generation,
            previous,
            None,
            source_start_offset,
            next_offset,
            through_atomic_position,
            deltas,
            query_batch,
            query_credits,
            pack_credits,
            maximum_preload_bytes,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn prepare_atomic_generation_after_compaction(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        physical_catalog_generation: [u8; 32],
        previous: &LoadedV6ProjectionGeneration,
        compaction: &V6CompactionBase,
        source_start_offset: u64,
        next_offset: u64,
        through_atomic_position: u64,
        deltas: Vec<keldra_index::v6::SealedComponentDelta>,
        query_batch: PreparedQueryMutationBatch,
        query_credits: QueryBlockCredits,
        pack_credits: ProjectionPackCredits,
        maximum_preload_bytes: usize,
    ) -> Result<PreparedAtomicProjectionGeneration, Status> {
        self.prepare_atomic_generation_inner(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            partition,
            physical_catalog_generation,
            Some(previous),
            Some(compaction),
            source_start_offset,
            next_offset,
            through_atomic_position,
            deltas,
            query_batch,
            query_credits,
            pack_credits,
            maximum_preload_bytes,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_atomic_generation_inner(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        physical_catalog_generation: [u8; 32],
        previous: Option<&LoadedV6ProjectionGeneration>,
        compaction: Option<&V6CompactionBase>,
        source_start_offset: u64,
        next_offset: u64,
        through_atomic_position: u64,
        deltas: Vec<keldra_index::v6::SealedComponentDelta>,
        query_batch: PreparedQueryMutationBatch,
        query_credits: QueryBlockCredits,
        pack_credits: ProjectionPackCredits,
        maximum_preload_bytes: usize,
    ) -> Result<PreparedAtomicProjectionGeneration, Status> {
        let mut component_pages = BTreeMap::new();
        let mut query_pages = BTreeMap::new();
        let mut preloaded_bytes = 0usize;
        let changed = deltas
            .iter()
            .map(|delta| delta.component)
            .collect::<BTreeSet<_>>();
        if let Some(previous) = previous {
            let base = compaction.map_or(&previous.generation, |value| &value.predecessor);
            for root in base
                .roots
                .iter()
                .filter(|root| changed.contains(&root.component))
            {
                let mut pending = vec![root.stream_root_hash];
                while let Some(hash) = pending.pop() {
                    if component_pages.contains_key(&hash) {
                        continue;
                    }
                    let bytes = if let Some(bytes) =
                        compaction.and_then(|compaction| compaction.component_page(&hash))
                    {
                        bytes.clone()
                    } else {
                        let path = projection_stream_page_path(partition, hash);
                        self.read_object(
                            storage_tenant,
                            bucket,
                            tenant_id,
                            bucket_id,
                            &path,
                            Some(hash),
                            MAX_STREAM_PAGE_BYTES,
                        )
                        .await?
                        .map(|(bytes, _)| bytes)
                        .ok_or_else(|| Status::data_loss("v6 component stream page is absent"))?
                    };
                    preloaded_bytes = preloaded_bytes
                        .checked_add(bytes.len())
                        .filter(|bytes| *bytes <= maximum_preload_bytes)
                        .ok_or_else(|| {
                            Status::resource_exhausted(
                                "v6 previous-spine preload exceeds memory bound",
                            )
                        })?;
                    if let Some(child) = component_stream_child_hashes(root.component, &bytes)
                        .map_err(index_status)?
                        .last()
                        .copied()
                    {
                        pending.push(child);
                    }
                    component_pages.insert(hash, bytes);
                }
            }
            if base.query_stream_root.run_count > 0 {
                let mut pending = vec![base.query_stream_root.stream_root_hash];
                while let Some(hash) = pending.pop() {
                    if query_pages.contains_key(&hash) {
                        continue;
                    }
                    let bytes = if let Some(bytes) =
                        compaction.and_then(|compaction| compaction.query_page(&hash))
                    {
                        bytes.clone()
                    } else {
                        let path = projection_query_run_stream_page_path(partition, hash);
                        self.read_object(
                            storage_tenant,
                            bucket,
                            tenant_id,
                            bucket_id,
                            &path,
                            Some(hash),
                            MAX_STREAM_PAGE_BYTES,
                        )
                        .await?
                        .map(|(bytes, _)| bytes)
                        .ok_or_else(|| Status::data_loss("v6 query stream page is absent"))?
                    };
                    let page = decode_query_run_page(&bytes).map_err(index_status)?;
                    if let QueryRunPage::Branch(children) = page {
                        if let Some(child) = children.last() {
                            pending.push(child.hash);
                        }
                    }
                    preloaded_bytes = preloaded_bytes
                        .checked_add(bytes.len())
                        .filter(|bytes| *bytes <= maximum_preload_bytes)
                        .ok_or_else(|| {
                            Status::resource_exhausted(
                                "v6 previous-spine preload exceeds memory bound",
                            )
                        })?;
                    query_pages.insert(hash, bytes);
                }
            }
        }
        prepare_atomic_projection_generation(
            partition,
            physical_catalog_generation,
            previous.map(|previous| {
                (
                    compaction.map_or(&previous.generation, |value| &value.predecessor),
                    previous.current.generation_hash,
                )
            }),
            source_start_offset,
            next_offset,
            through_atomic_position,
            Vec::new(),
            deltas,
            query_batch,
            QueryBlockLimits::default_for_memory(),
            query_credits,
            pack_credits,
            |hash| {
                component_pages
                    .get(&hash)
                    .cloned()
                    .ok_or(keldra_index::IndexError::Integrity)
            },
            |hash| {
                query_pages
                    .get(&hash)
                    .cloned()
                    .ok_or(keldra_index::IndexError::Integrity)
            },
        )
        .map_err(index_status)
    }

    /// Load the one stable family lifecycle directory. This directory is not
    /// rewritten by ordinary partition publication; callers use it only for
    /// placement/handoff discovery.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_family_directory(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        family_id: [u8; 32],
    ) -> Result<Option<(ProjectionFamilyPartitionDirectory, VersionId)>, Status> {
        let Some((bytes, version)) = self
            .read_object(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                &projection_family_directory_path(family_id),
                None,
                MAX_FAMILY_DIRECTORY_BYTES,
            )
            .await?
        else {
            return Ok(None);
        };
        let directory = decode_projection_family_directory(&bytes).map_err(index_status)?;
        if directory.family_id != family_id {
            return Err(Status::data_loss(
                "v6 family directory belongs to a different projection family",
            ));
        }
        Ok(Some((directory, version)))
    }

    /// Publish a lifecycle transition through the one family directory CAS.
    /// Normal segment flushes must never call this method.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_family_directory(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        directory: &ProjectionFamilyPartitionDirectory,
        expected_version: Option<VersionId>,
    ) -> Result<VersionId, Status> {
        directory.validate().map_err(index_status)?;
        let bytes = encode_projection_family_directory(directory).map_err(index_status)?;
        if bytes.len() > MAX_FAMILY_DIRECTORY_BYTES {
            return Err(Status::resource_exhausted(
                "v6 family directory exceeds its encoded authority-object bound",
            ));
        }
        let blob = self.stage(&bytes).await?;
        let routing = projection_catalog_routing_id(directory.family_id, directory.family_id)
            .map_err(index_status)?;
        let outcome = self
            .artifacts
            .publish(request(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                routing,
                projection_family_directory_path(directory.family_id),
                blob,
                expected_version,
            ))
            .await?;
        Ok(outcome.version)
    }

    /// An activation is the only object that makes a physical catalog
    /// generation queryable. It pins the exact complete root set produced by
    /// directory-discovered partitions.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_activation(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        family_id: [u8; 32],
        physical_catalog_generation: [u8; 32],
    ) -> Result<Option<(ProjectionCatalogActivation, VersionId)>, Status> {
        let Some((bytes, version)) = self
            .read_object(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                &projection_catalog_activation_path(family_id, physical_catalog_generation),
                None,
                MAX_CATALOG_ACTIVATION_BYTES,
            )
            .await?
        else {
            return Ok(None);
        };
        let activation = decode_projection_catalog_activation(&bytes).map_err(index_status)?;
        if activation.family_id != family_id
            || activation.physical_catalog_generation != physical_catalog_generation
        {
            return Err(Status::data_loss(
                "v6 catalog activation path and payload disagree",
            ));
        }
        Ok(Some((activation, version)))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_activation(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        activation: &ProjectionCatalogActivation,
        expected_version: Option<VersionId>,
    ) -> Result<VersionId, Status> {
        activation.validate().map_err(index_status)?;
        let bytes = encode_projection_catalog_activation(activation).map_err(index_status)?;
        if bytes.len() > MAX_CATALOG_ACTIVATION_BYTES {
            return Err(Status::resource_exhausted(
                "v6 catalog activation exceeds its encoded authority-object bound",
            ));
        }
        let blob = self.stage(&bytes).await?;
        // Directory and activation share the one stable family lifecycle
        // authority. Physical generations are catalog state, not a routing
        // input: otherwise an activation could be sent to a different node
        // than the directory that proved it ready.
        let routing = projection_catalog_routing_id(activation.family_id, activation.family_id)
            .map_err(index_status)?;
        let outcome = self
            .artifacts
            .publish(request(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                routing,
                projection_catalog_activation_path(
                    activation.family_id,
                    activation.physical_catalog_generation,
                ),
                blob,
                expected_version,
            ))
            .await?;
        Ok(outcome.version)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_atomic_generation(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        previous: Option<&LoadedV6ProjectionGeneration>,
        prepared: PreparedAtomicProjectionGeneration,
        published_source_rows: u64,
        published_source_bytes: u64,
    ) -> Result<LoadedV6ProjectionGeneration, Status> {
        let plan = plan_atomic_publication(partition, previous, prepared)?;
        if published_source_rows != plan.source_rows {
            return Err(Status::data_loss(
                "v6 publication telemetry rows do not match the prepared source cut",
            ));
        }
        let sealed_bytes = plan.sealed_bytes;
        let next_offset = plan.current.next_offset;
        let generation_hash = plan.current.generation_hash;
        let mut publications = Vec::with_capacity(plan.immutable.len());
        for artifact in plan.immutable {
            let blob = self.stage(&artifact.bytes).await?;
            if blob.hash != artifact.hash || blob.length != artifact.bytes.len() as u64 {
                return Err(Status::data_loss(
                    "staged v6 immutable artifact changed its exact bytes",
                ));
            }
            let routing_id =
                projection_artifact_routing_id(partition.family_id, artifact.kind, artifact.hash)
                    .map_err(index_status)?;
            publications.push(request(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                routing_id,
                artifact.path,
                blob,
                None,
            ));
        }
        require_all_immutable_publications(
            self.artifacts.publish_immutable_many(publications).await?,
        )?;
        let current_blob = self.stage(&plan.current_bytes).await?;
        if current_blob.hash != *blake3::hash(&plan.current_bytes).as_bytes()
            || current_blob.length != plan.current_bytes.len() as u64
        {
            return Err(Status::data_loss(
                "staged v6 current changed its exact bytes",
            ));
        }
        let outcome = self
            .artifacts
            .publish(request(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                projection_routing_id(partition),
                projection_current_path(partition),
                current_blob,
                previous.map(|loaded| loaded.current_object_version),
            ))
            .await?;
        let loaded_generation = self
            .load_generation_by_hash(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                partition,
                generation_hash,
            )
            .await?;
        plan.current
            .validate_against(&loaded_generation)
            .map_err(index_status)?;
        if loaded_generation != plan.generation {
            return Err(Status::data_loss(
                "published v6 generation differs from the prepared generation",
            ));
        }
        // Publication and progress telemetry become true only after the
        // partition-current CAS committed. Failures above may leave safe,
        // unreachable immutable artifacts but never claim progress.
        super::v6_telemetry::V6PipelineTelemetry::set(
            &super::v6_telemetry::global().local_next_offset,
            next_offset,
        );
        super::v6_telemetry::V6PipelineTelemetry::add(
            &super::v6_telemetry::global().sealed_bytes,
            sealed_bytes,
        );
        super::v6_telemetry::V6PipelineTelemetry::add(
            &super::v6_telemetry::global().published_source_rows,
            published_source_rows,
        );
        super::v6_telemetry::V6PipelineTelemetry::add(
            &super::v6_telemetry::global().published_source_bytes,
            published_source_bytes,
        );
        super::v6_telemetry::V6PipelineTelemetry::add(
            &super::v6_telemetry::global().source_rows,
            published_source_rows,
        );
        super::v6_telemetry::V6PipelineTelemetry::add(
            &super::v6_telemetry::global().source_bytes,
            published_source_bytes,
        );
        super::v6_telemetry::V6PipelineTelemetry::add(
            &super::v6_telemetry::global().checkpointed_source_rows,
            published_source_rows,
        );
        super::v6_telemetry::V6PipelineTelemetry::add(
            &super::v6_telemetry::global().checkpointed_source_bytes,
            published_source_bytes,
        );
        Ok(LoadedV6ProjectionGeneration {
            current: plan.current,
            current_object_version: outcome.version,
            generation: loaded_generation,
        })
    }

    /// Make every immutable compaction output durable before the successor
    /// generation that references it reaches the partition Current CAS.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_compaction_artifacts(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        compaction: V6CompactionArtifacts,
    ) -> Result<(), Status> {
        let mut artifacts = BTreeMap::new();
        if let Some(component) = &compaction.component {
            for pack in &component.packs.packs {
                insert_artifact(
                    &mut artifacts,
                    projection_pack_path(partition, pack.hash),
                    keldra_index::v6::ProjectionArtifactKind::Pack,
                    pack.hash,
                    pack.bytes.clone(),
                )?;
            }
            for page in &component.pages {
                insert_artifact(
                    &mut artifacts,
                    projection_stream_page_path(partition, page.hash),
                    keldra_index::v6::ProjectionArtifactKind::StreamPage,
                    page.hash,
                    page.bytes.clone(),
                )?;
            }
        }
        if let Some(query) = &compaction.query {
            for block in &query.artifacts().blocks {
                insert_artifact(
                    &mut artifacts,
                    projection_query_run_pack_path(partition, block.descriptor.hash),
                    keldra_index::v6::ProjectionArtifactKind::QueryRunPack,
                    block.descriptor.hash,
                    block.bytes.clone(),
                )?;
            }
            let run = &query.artifacts().run;
            insert_artifact(
                &mut artifacts,
                projection_query_run_pack_path(partition, run.hash),
                keldra_index::v6::ProjectionArtifactKind::QueryRunPack,
                run.hash,
                run.bytes.clone(),
            )?;
            for page in &query.splice().pages {
                insert_artifact(
                    &mut artifacts,
                    projection_query_run_stream_page_path(partition, page.hash),
                    keldra_index::v6::ProjectionArtifactKind::QueryRunStreamPage,
                    page.hash,
                    page.bytes.clone(),
                )?;
            }
        }
        let mut publications = Vec::with_capacity(artifacts.len());
        for artifact in artifacts.into_values() {
            let blob = self.stage(&artifact.bytes).await?;
            if blob.hash != artifact.hash || blob.length != artifact.bytes.len() as u64 {
                return Err(Status::data_loss(
                    "staged v6 compaction artifact changed its exact bytes",
                ));
            }
            let routing_id =
                projection_artifact_routing_id(partition.family_id, artifact.kind, artifact.hash)
                    .map_err(index_status)?;
            publications.push(request(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                routing_id,
                artifact.path,
                blob,
                None,
            ));
        }
        require_all_immutable_publications(
            self.artifacts.publish_immutable_many(publications).await?,
        )?;
        // Keep the byte-credit guardians alive until every cloned publication
        // payload has left this future.
        drop(compaction);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_current(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
    ) -> Result<Option<LoadedV6ProjectionGeneration>, Status> {
        let Some((bytes, version)) = self
            .read_object(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                &projection_current_path(partition),
                None,
                1024,
            )
            .await?
        else {
            return Ok(None);
        };
        let current = decode_projection_current(&bytes).map_err(index_status)?;
        if current.partition != partition {
            return Err(Status::data_loss(
                "v6 current pointer belongs to another partition",
            ));
        }
        let generation = self
            .load_generation_by_hash(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                partition,
                current.generation_hash,
            )
            .await?;
        current
            .validate_against(&generation)
            .map_err(index_status)?;
        Ok(Some(LoadedV6ProjectionGeneration {
            current,
            current_object_version: version,
            generation,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_component_record(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        generation: &ProjectionGeneration,
        component: ComponentIdentity,
        key: StableDocumentKey,
    ) -> Result<Option<Vec<u8>>, Status> {
        let Some(root) = generation.root(component) else {
            return Ok(None);
        };
        let mut cursor = ComponentStreamReverseCursor::new(
            ComponentStreamRoot::from_component_root(root).map_err(index_status)?,
        )
        .map_err(index_status)?;
        loop {
            match cursor.next().map_err(index_status)? {
                ComponentStreamReverseStep::LoadPage { hash } => {
                    let path = projection_stream_page_path(generation.partition, hash);
                    let (bytes, _) = self
                        .read_object(
                            storage_tenant,
                            bucket,
                            tenant_id,
                            bucket_id,
                            &path,
                            Some(hash),
                            MAX_STREAM_PAGE_BYTES,
                        )
                        .await?
                        .ok_or_else(|| Status::data_loss("v6 stream page is absent"))?;
                    cursor.provide_page(hash, &bytes).map_err(index_status)?;
                }
                ComponentStreamReverseStep::Segment(descriptor) => {
                    if key < descriptor.minimum_key || key > descriptor.maximum_key {
                        continue;
                    }
                    let path = projection_pack_path(generation.partition, descriptor.pack_hash);
                    let maximum = 64 * 1024 * 1024;
                    let (bytes, _) = self
                        .read_object(
                            storage_tenant,
                            bucket,
                            tenant_id,
                            bucket_id,
                            &path,
                            Some(descriptor.pack_hash),
                            maximum,
                        )
                        .await?
                        .ok_or_else(|| Status::data_loss("v6 projection pack is absent"))?;
                    match lookup_component_record_in_pack(component, &descriptor, &bytes, key)
                        .map_err(index_status)?
                    {
                        ComponentRecordLookup::Missing => {}
                        ComponentRecordLookup::Tombstone => return Ok(None),
                        ComponentRecordLookup::Value(value) => return Ok(Some(value)),
                    }
                }
                ComponentStreamReverseStep::Complete => return Ok(None),
            }
        }
    }

    /// Reconstruct the exact prior expanded records for one source object
    /// from its compact locator, head and recipe components. This is the
    /// predecessor read used by the ordered v6 pipeline; it is deliberately
    /// not a historical projected-state stream or a second durable cache.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_source_states(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        generation: &ProjectionGeneration,
        source_scope: [u8; 32],
        source_path: &str,
    ) -> Result<Vec<ProjectedDocumentState>, Status> {
        let locator =
            StableDocumentKey::derive(source_scope, source_path, 0).map_err(index_status)?;
        let Some(encoded_records) = self
            .load_component_record(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                generation,
                ComponentIdentity::SourceRecords,
                locator,
            )
            .await?
        else {
            return Ok(Vec::new());
        };
        let records = decode_source_records(source_scope, source_path, &encoded_records)
            .map_err(index_status)?;
        let component_recipes = generation
            .roots
            .iter()
            .filter_map(|root| match root.component {
                ComponentIdentity::Membership(recipe) => Some((true, recipe)),
                ComponentIdentity::Field(recipe) => Some((false, recipe)),
                ComponentIdentity::DocumentHead
                | ComponentIdentity::SourceRecords
                | ComponentIdentity::Order(_) => None,
            })
            .collect::<Vec<_>>();
        let mut states = Vec::with_capacity(records.len());
        for key in records {
            let encoded_head = self
                .load_component_record(
                    storage_tenant,
                    bucket,
                    tenant_id,
                    bucket_id,
                    generation,
                    ComponentIdentity::DocumentHead,
                    key,
                )
                .await?
                .ok_or_else(|| {
                    Status::data_loss("v6 source locator names a missing document head")
                })?;
            let head =
                decode_document_head(source_scope, key, &encoded_head).map_err(index_status)?;
            if head.stable_key != key || head.source_path != source_path {
                return Err(Status::data_loss(
                    "v6 source locator, document head, and source scope disagree",
                ));
            }
            let mut memberships = Vec::new();
            let mut fields = Vec::new();
            for (membership, recipe) in &component_recipes {
                let component = if *membership {
                    ComponentIdentity::Membership(*recipe)
                } else {
                    ComponentIdentity::Field(*recipe)
                };
                let Some(value) = self
                    .load_component_record(
                        storage_tenant,
                        bucket,
                        tenant_id,
                        bucket_id,
                        generation,
                        component,
                        key,
                    )
                    .await?
                else {
                    continue;
                };
                let state = CanonicalRecipeState::new(*recipe, value).map_err(index_status)?;
                if *membership {
                    memberships.push(state);
                } else {
                    fields.push(state);
                }
            }
            memberships.sort_by_key(|state| state.recipe);
            fields.sort_by_key(|state| state.recipe);
            states.push(
                ProjectedDocumentState::new(source_scope, head, memberships, fields)
                    .map_err(index_status)?,
            );
        }
        states.sort_by_key(|state| state.head.source_record);
        Ok(states)
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_generation_by_hash(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        generation_hash: [u8; 32],
    ) -> Result<ProjectionGeneration, Status> {
        let path = projection_generation_path(partition, generation_hash);
        let (bytes, _) = self
            .read_object(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                &path,
                Some(generation_hash),
                MAX_GENERATION_BYTES,
            )
            .await?
            .ok_or_else(|| Status::data_loss("v6 generation object is absent"))?;
        let header = decode_projection_generation_header(&bytes).map_err(index_status)?;
        if header.partition != partition {
            return Err(Status::data_loss(
                "v6 generation header belongs to another partition",
            ));
        }
        let pages = self
            .load_component_directory(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                partition,
                header.component_directory_root_hash,
                header.component_root_count,
            )
            .await?;
        decode_projection_generation(&bytes, &pages).map_err(index_status)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_generation_reference(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        reference: ProjectionGenerationReference,
    ) -> Result<ProjectionGeneration, Status> {
        let generation = self
            .load_generation_by_hash(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                reference.partition,
                reference.generation_hash,
            )
            .await?;
        if generation
            .reference(reference.generation_hash)
            .map_err(index_status)?
            != reference
        {
            return Err(Status::data_loss(
                "v6 generation differs from its pinned reference",
            ));
        }
        Ok(generation)
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_component_directory(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        root_hash: [u8; 32],
        root_count: u64,
    ) -> Result<keldra_index::v6::ComponentDirectory, Status> {
        if root_count == 0 {
            return Ok(keldra_index::v6::ComponentDirectory {
                root_hash,
                root_count,
                pages: Vec::new(),
            });
        }
        let mut pending = VecDeque::from([root_hash]);
        let mut visited = BTreeSet::new();
        let mut pages = Vec::new();
        while let Some(hash) = pending.pop_front() {
            if !visited.insert(hash) || visited.len() > root_count as usize * 2 {
                return Err(Status::data_loss(
                    "v6 component directory contains a cycle or exceeds its bound",
                ));
            }
            let path = projection_component_page_path(partition, hash);
            let (bytes, _) = self
                .read_object(
                    storage_tenant,
                    bucket,
                    tenant_id,
                    bucket_id,
                    &path,
                    Some(hash),
                    MAX_STREAM_PAGE_BYTES,
                )
                .await?
                .ok_or_else(|| Status::data_loss("v6 component page is absent"))?;
            pending.extend(
                keldra_index::v6::component_directory_child_hashes(&bytes).map_err(index_status)?,
            );
            pages.push(keldra_index::v6::EncodedComponentDirectoryPage { hash, bytes });
        }
        Ok(keldra_index::v6::ComponentDirectory {
            root_hash,
            root_count,
            pages,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_component_append_spine(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        root: &ComponentRoot,
    ) -> Result<BTreeMap<[u8; 32], Vec<u8>>, Status> {
        let stream = ComponentStreamRoot::from_component_root(root).map_err(index_status)?;
        let maximum = usize::try_from(stream.directory_bytes)
            .map_err(|_| Status::resource_exhausted("v6 stream directory is unbounded"))?;
        if maximum > MAX_STREAM_DIRECTORY_BYTES {
            return Err(Status::resource_exhausted(
                "v6 stream directory exceeds its runtime read bound",
            ));
        }
        // `append_component_stream` rewrites only the rightmost branch/leaf
        // path.  Loading its complete historical directory here made every
        // normal flush proportional to all prior segments.  Keep exactly the
        // hashes that synchronous preparation will request instead.
        let mut hash = stream.root_hash;
        let mut visited = BTreeSet::new();
        let mut pages = BTreeMap::new();
        let mut resident = 0usize;
        loop {
            if !visited.insert(hash) || visited.len() > stream.segment_count as usize {
                return Err(Status::data_loss(
                    "v6 append spine contains a cycle or exceeds its segment bound",
                ));
            }
            let path = projection_stream_page_path(partition, hash);
            let (bytes, _) = self
                .read_object(
                    storage_tenant,
                    bucket,
                    tenant_id,
                    bucket_id,
                    &path,
                    Some(hash),
                    MAX_STREAM_PAGE_BYTES,
                )
                .await?
                .ok_or_else(|| Status::data_loss("v6 stream page is absent"))?;
            resident = resident
                .checked_add(bytes.len())
                .ok_or_else(|| Status::resource_exhausted("v6 stream bytes overflow"))?;
            if resident > maximum {
                return Err(Status::data_loss("v6 stream exceeds committed byte count"));
            }
            let children =
                component_stream_child_hashes(stream.component, &bytes).map_err(index_status)?;
            pages.insert(hash, bytes);
            let Some(rightmost) = children.last().copied() else {
                return Ok(pages);
            };
            hash = rightmost;
        }
    }

    async fn stage(&self, bytes: &[u8]) -> Result<BlobRef, Status> {
        self.store
            .stage_derived_progress_blob(bytes)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_object(
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
        let Some(version) = self.reader.head_stable(&key, tenant_id, bucket_id).await? else {
            return Ok(None);
        };
        if version.deleted {
            return Err(Status::data_loss("v6 projection artifact is deleted"));
        }
        let blob = version
            .blob
            .as_ref()
            .ok_or_else(|| Status::data_loss("v6 projection artifact has no blob"))?;
        if expected_hash.is_some_and(|hash| blob.hash != hash) {
            return Err(Status::data_loss(
                "v6 projection path and payload hash differ",
            ));
        }
        let bytes = self.read_blob_local_first(blob, maximum_bytes).await?;
        Ok(Some((bytes, version.id)))
    }

    /// Reads an immutable artifact from the local integrated blob store when
    /// present, reconstructing it from peers only when this node lacks it.
    pub(crate) async fn read_blob_local_first(
        &self,
        blob: &BlobRef,
        maximum_bytes: usize,
    ) -> Result<Vec<u8>, Status> {
        if blob.length > maximum_bytes as u64 {
            return Err(Status::data_loss(
                "v6 projection artifact violates its exact byte bound",
            ));
        }
        let read_limit = u64::try_from(maximum_bytes)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let mut bytes = Vec::with_capacity(blob.length as usize);
        match self.store.open_blob(blob).await {
            Ok(mut payload) => {
                let mut chunk = [0_u8; 8 * 1024];
                while bytes.len() < blob.length as usize {
                    let read = payload.read(&mut chunk).await.map_err(|error| {
                        Status::internal(format!("read local v6 projection artifact: {error}"))
                    })?;
                    if read == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..read]);
                }
            }
            Err(MutationError::BlobNotFound) => {
                let mut payload = self.reader.open_blob_payload(blob).await?;
                payload
                    .by_ref()
                    .take(read_limit)
                    .read_to_end(&mut bytes)
                    .map_err(|error| {
                        Status::internal(format!(
                            "read distributed v6 projection artifact: {error}"
                        ))
                    })?;
            }
            Err(error) => return Err(Status::unavailable(error.to_string())),
        }
        if bytes.len() > maximum_bytes || bytes.len() as u64 != blob.length {
            return Err(Status::data_loss(
                "v6 projection artifact violates its exact byte bound",
            ));
        }
        Ok(bytes)
    }
}

fn plan_atomic_publication(
    partition: ProjectionPartitionIdentity,
    previous: Option<&LoadedV6ProjectionGeneration>,
    prepared: PreparedAtomicProjectionGeneration,
) -> Result<AtomicPublicationPlan, Status> {
    let sealed_bytes = prepared
        .packs
        .iter()
        .flat_map(|pack| &pack.deltas)
        .try_fold(0_u64, |total, delta| {
            total
                .checked_add(delta.encoded_bytes)
                .ok_or_else(|| Status::resource_exhausted("v6 sealed delta bytes overflow"))
        })?;
    if prepared.generation.hash != *blake3::hash(&prepared.generation.bytes).as_bytes() {
        return Err(Status::data_loss(
            "prepared v6 generation has the wrong content hash",
        ));
    }
    let generation = decode_projection_generation(
        &prepared.generation.bytes,
        &prepared.generation.component_directory,
    )
    .map_err(index_status)?;
    if generation.partition != partition {
        return Err(Status::data_loss(
            "prepared v6 generation belongs to another partition",
        ));
    }
    let current = decode_projection_current(&prepared.current).map_err(index_status)?;
    current
        .validate_against(&generation)
        .map_err(index_status)?;
    if current.generation_hash != prepared.generation.hash {
        return Err(Status::data_loss(
            "prepared v6 current does not name the encoded generation",
        ));
    }
    match previous {
        Some(previous)
            if previous.generation.partition == partition
                && generation.previous_generation_hash
                    == Some(previous.current.generation_hash)
                && generation.revision == previous.generation.revision.saturating_add(1) => {}
        Some(_) => {
            return Err(Status::data_loss(
                "prepared v6 generation is not the exact predecessor successor",
            ));
        }
        None if generation.previous_generation_hash.is_none() && generation.revision == 1 => {}
        None => {
            return Err(Status::data_loss(
                "initial prepared v6 generation has a predecessor",
            ));
        }
    }

    let maximum_block_bytes = prepared
        .query_blocks
        .iter()
        .map(|block| block.bytes.len())
        .max()
        .unwrap_or(64)
        .max(64);
    let query_limits = keldra_index::v6::QueryBlockLimits {
        maximum_block_bytes,
        maximum_records: u32::MAX as usize,
        maximum_key_bytes: maximum_block_bytes,
        maximum_value_bytes: maximum_block_bytes,
        maximum_loaded_blocks: prepared.query_blocks.len().max(1),
        maximum_run_descriptor_bytes: prepared.query_run.bytes.len().max(256),
    };
    let mut validation_credits = keldra_index::v6::QueryBlockCredits::from_query_permit(Box::new(
        PublicationValidationPermit(prepared.query_run.bytes.len().max(1)),
    ))
    .map_err(index_status)?;
    let query_run = keldra_index::v6::decode_projection_query_run(
        &prepared.query_run.bytes,
        query_limits,
        &mut validation_credits,
    )
    .map_err(index_status)?;
    if prepared.query_run.hash != *blake3::hash(&prepared.query_run.bytes).as_bytes()
        || query_run.partition != partition
        || query_run.physical_catalog_generation != generation.physical_catalog_generation
        || previous
            .is_some_and(|previous| query_run.source_start_offset != previous.current.next_offset)
        || query_run.next_offset != generation.next_offset
        || query_run.through_atomic_position != generation.through_atomic_position
        || query_run.blocks
            != prepared
                .query_blocks
                .iter()
                .map(|block| block.descriptor.clone())
                .collect::<Vec<_>>()
    {
        return Err(Status::data_loss(
            "prepared v6 query run is not bound to its generation cut and blocks",
        ));
    }
    let newest = newest_prepared_query_run(
        generation.query_stream_root.stream_root_hash,
        &prepared.query_stream_pages,
    )?;
    if newest.hash != prepared.query_run.hash
        || newest.sequence != query_run.sequence
        || newest.source_start_offset != query_run.source_start_offset
        || newest.next_offset != query_run.next_offset
        || newest.through_atomic_position != query_run.through_atomic_position
        || generation.query_stream_root.next_offset != query_run.next_offset
        || generation.query_stream_root.through_atomic_position != query_run.through_atomic_position
    {
        return Err(Status::data_loss(
            "prepared v6 query stream does not name its exact newest run cut",
        ));
    }

    let mut artifacts = BTreeMap::new();
    for pack in prepared.packs {
        insert_artifact(
            &mut artifacts,
            projection_pack_path(partition, pack.hash),
            keldra_index::v6::ProjectionArtifactKind::Pack,
            pack.hash,
            pack.bytes,
        )?;
    }
    for page in prepared.stream_pages {
        insert_artifact(
            &mut artifacts,
            projection_stream_page_path(partition, page.hash),
            keldra_index::v6::ProjectionArtifactKind::StreamPage,
            page.hash,
            page.bytes,
        )?;
    }
    for block in prepared.query_blocks {
        insert_artifact(
            &mut artifacts,
            projection_query_run_pack_path(partition, block.descriptor.hash),
            keldra_index::v6::ProjectionArtifactKind::QueryRunPack,
            block.descriptor.hash,
            block.bytes,
        )?;
    }
    insert_artifact(
        &mut artifacts,
        projection_query_run_pack_path(partition, prepared.query_run.hash),
        keldra_index::v6::ProjectionArtifactKind::QueryRunPack,
        prepared.query_run.hash,
        prepared.query_run.bytes,
    )?;
    for page in prepared.query_stream_pages {
        insert_artifact(
            &mut artifacts,
            projection_query_run_stream_page_path(partition, page.hash),
            keldra_index::v6::ProjectionArtifactKind::QueryRunStreamPage,
            page.hash,
            page.bytes,
        )?;
    }
    for page in prepared.generation.component_directory.pages {
        insert_artifact(
            &mut artifacts,
            projection_component_page_path(partition, page.hash),
            keldra_index::v6::ProjectionArtifactKind::ComponentPage,
            page.hash,
            page.bytes,
        )?;
    }
    insert_artifact(
        &mut artifacts,
        projection_generation_path(partition, prepared.generation.hash),
        keldra_index::v6::ProjectionArtifactKind::Generation,
        prepared.generation.hash,
        prepared.generation.bytes,
    )?;
    let immutable = artifacts.into_values().collect::<Vec<_>>();
    Ok(AtomicPublicationPlan {
        immutable,
        current_bytes: prepared.current,
        current,
        generation,
        sealed_bytes,
        source_rows: query_run
            .next_offset
            .checked_sub(query_run.source_start_offset)
            .ok_or_else(|| Status::data_loss("v6 query run source cut moves backwards"))?,
    })
}

fn require_all_immutable_publications(
    outcomes: Vec<super::publication::IndexArtifactPublicationOutcome>,
) -> Result<(), Status> {
    for outcome in outcomes {
        outcome?;
    }
    Ok(())
}

struct PublicationValidationPermit(usize);

impl keldra_index::v6::QueryMemoryPermit for PublicationValidationPermit {
    fn admitted_bytes(&self) -> usize {
        self.0
    }
}

fn newest_prepared_query_run(
    mut hash: [u8; 32],
    pages: &[keldra_index::v6::EncodedQueryRunPage],
) -> Result<keldra_index::v6::QueryRunReference, Status> {
    let pages = pages
        .iter()
        .map(|page| (page.hash, page))
        .collect::<BTreeMap<_, _>>();
    let mut visited = BTreeSet::new();
    loop {
        if !visited.insert(hash) || visited.len() > pages.len() {
            return Err(Status::data_loss(
                "prepared v6 query stream right spine is cyclic or incomplete",
            ));
        }
        let page = pages.get(&hash).ok_or_else(|| {
            Status::data_loss("prepared v6 query stream omits its new right spine")
        })?;
        if page.hash != *blake3::hash(&page.bytes).as_bytes() {
            return Err(Status::data_loss(
                "prepared v6 query stream page has the wrong content hash",
            ));
        }
        match decode_query_run_page(&page.bytes).map_err(index_status)? {
            QueryRunPage::Leaf(runs) => {
                return runs
                    .last()
                    .copied()
                    .ok_or_else(|| Status::data_loss("prepared v6 query stream leaf is empty"));
            }
            QueryRunPage::Branch(children) => {
                hash = children
                    .last()
                    .ok_or_else(|| Status::data_loss("prepared v6 query stream branch is empty"))?
                    .hash;
            }
        }
    }
}

fn insert_artifact(
    artifacts: &mut BTreeMap<String, ArtifactBytes>,
    path: String,
    kind: keldra_index::v6::ProjectionArtifactKind,
    hash: [u8; 32],
    bytes: Vec<u8>,
) -> Result<(), Status> {
    if hash != *blake3::hash(&bytes).as_bytes() {
        return Err(Status::data_loss(
            "prepared v6 projection artifact has the wrong content hash",
        ));
    }
    match artifacts.entry(path.clone()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(ArtifactBytes {
                path,
                kind,
                hash,
                bytes,
            });
        }
        std::collections::btree_map::Entry::Occupied(entry)
            if entry.get().hash == hash && entry.get().bytes == bytes => {}
        std::collections::btree_map::Entry::Occupied(_) => {
            return Err(Status::data_loss(
                "v6 projection path names conflicting immutable bytes",
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn request(
    storage_tenant: &str,
    bucket: &str,
    tenant_id: u64,
    bucket_id: u64,
    routing_id: u64,
    exact_path: String,
    blob: BlobRef,
    expected_version: Option<VersionId>,
) -> IndexArtifactPublish {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keldra.index.v6.publish/v1");
    hasher.update(exact_path.as_bytes());
    hasher.update(&blob.hash);
    hasher.update(&blob.length.to_be_bytes());
    if let Some(version) = expected_version {
        hasher.update(&version.0.to_be_bytes());
    }
    IndexArtifactPublish {
        storage_tenant: storage_tenant.into(),
        bucket: bucket.into(),
        tenant_id,
        bucket_id,
        index_id: routing_id,
        exact_path,
        blob,
        expected_version,
        command_id: format!("index-v6-{}", &hasher.finalize().to_hex().as_str()[..24]),
        definition_guard: None,
        definition_intent: None,
        admission: DerivedArtifactAdmission::PublicationProgress,
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
    use keldra_index::v6::{
        IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage,
        PreparedQueryMembershipDelta, PreparedQueryMutationBatch, ProjectionPackCredits,
        QueryBlockCredits, QueryBlockLimits, QueryDocumentGate, RecipeIdentity, StableDocumentKey,
        prepare_atomic_projection_generation,
    };

    use super::*;
    use crate::index_runtime::publication::IndexArtifactOutcome;

    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([7; 32], 1, [8; 32], 2, 3, 4).unwrap()
    }

    fn query_credits() -> QueryBlockCredits {
        let bytes = 4 * 1024 * 1024;
        let memory = IndexingMemoryCredits::new(
            bytes,
            IndexingMemoryLimits {
                hot_payload_bytes: bytes,
                worker_scratch_bytes: bytes,
                prepared_rows_bytes: bytes,
                replay_input_bytes: bytes,
                projection_accumulator_bytes: bytes,
                seal_scratch_bytes: bytes,
                ordering_catalog_bytes: bytes,
            },
        )
        .unwrap();
        QueryBlockCredits::from_pipeline_permit(
            memory
                .acquire(IndexingMemoryStage::OrderingCatalog, bytes)
                .unwrap(),
        )
    }

    fn pack_credits() -> ProjectionPackCredits {
        let bytes = 4 * 1024 * 1024;
        let memory = IndexingMemoryCredits::new(
            bytes,
            IndexingMemoryLimits {
                hot_payload_bytes: bytes,
                worker_scratch_bytes: bytes,
                prepared_rows_bytes: bytes,
                replay_input_bytes: bytes,
                projection_accumulator_bytes: bytes,
                seal_scratch_bytes: bytes,
                ordering_catalog_bytes: bytes,
            },
        )
        .unwrap();
        ProjectionPackCredits::from_pipeline_permit(
            memory
                .acquire(IndexingMemoryStage::SealScratch, bytes)
                .unwrap(),
        )
    }

    fn prepared(
        next_offset: u64,
        through_atomic_position: u64,
    ) -> PreparedAtomicProjectionGeneration {
        prepare_atomic_projection_generation(
            partition(),
            [9; 32],
            None,
            0,
            next_offset,
            through_atomic_position,
            Vec::new(),
            Vec::new(),
            PreparedQueryMutationBatch {
                membership: Some(PreparedQueryMembershipDelta {
                    recipe: RecipeIdentity::new([10; 32]).unwrap(),
                    gates: vec![QueryDocumentGate {
                        document: StableDocumentKey::derive([11; 32], "objects/one", 0).unwrap(),
                        material_source_version: 1,
                        current_source_version: 1,
                        live: true,
                        source_path: Some("objects/one".into()),
                        result_path: Some("objects/one".into()),
                        result_version: 1,
                    }],
                }),
                fields: Vec::new(),
            },
            QueryBlockLimits::default_for_memory(),
            query_credits(),
            pack_credits(),
            |_| Err(keldra_index::IndexError::Integrity),
            |_| Err(keldra_index::IndexError::Integrity),
        )
        .unwrap()
    }

    fn artifact_fingerprints(plan: &AtomicPublicationPlan) -> Vec<(String, [u8; 32], Vec<u8>)> {
        plan.immutable
            .iter()
            .map(|artifact| (artifact.path.clone(), artifact.hash, artifact.bytes.clone()))
            .collect()
    }

    #[test]
    fn atomic_plan_contains_query_artifacts_and_generation_before_current_phase() {
        let plan = plan_atomic_publication(partition(), None, prepared(1, 11)).unwrap();
        let kinds = plan
            .immutable
            .iter()
            .map(|artifact| artifact.kind)
            .collect::<Vec<_>>();

        assert!(kinds.contains(&keldra_index::v6::ProjectionArtifactKind::QueryRunPack));
        assert!(kinds.contains(&keldra_index::v6::ProjectionArtifactKind::QueryRunStreamPage));
        assert!(kinds.contains(&keldra_index::v6::ProjectionArtifactKind::Generation));
        assert!(!kinds.contains(&keldra_index::v6::ProjectionArtifactKind::Current));
        assert_eq!(
            plan.immutable
                .iter()
                .filter(|artifact| {
                    artifact.kind == keldra_index::v6::ProjectionArtifactKind::QueryRunPack
                })
                .count(),
            2,
            "the gate block and its run descriptor must both be immutable"
        );
        assert_eq!(plan.current.next_offset, 1);
    }

    #[test]
    fn immutable_failure_cannot_reach_the_current_phase() {
        let outcomes = vec![
            Ok(IndexArtifactOutcome {
                version: VersionId(1),
                replayed: false,
            }),
            Err(Status::unavailable("injected immutable failure")),
        ];
        let mut current_attempted = false;
        if require_all_immutable_publications(outcomes).is_ok() {
            current_attempted = true;
        }

        assert!(!current_attempted);
    }

    #[test]
    fn replay_builds_identical_content_paths_and_commands() {
        let first = plan_atomic_publication(partition(), None, prepared(1, 11)).unwrap();
        let replay = plan_atomic_publication(partition(), None, prepared(1, 11)).unwrap();
        assert_eq!(
            artifact_fingerprints(&first),
            artifact_fingerprints(&replay)
        );

        let blob = BlobRef {
            hash: *blake3::hash(&first.current_bytes).as_bytes(),
            length: first.current_bytes.len() as u64,
        };
        let first_request = request(
            "tenant",
            "bucket",
            1,
            2,
            projection_routing_id(partition()),
            projection_current_path(partition()),
            blob.clone(),
            None,
        );
        let replay_request = request(
            "tenant",
            "bucket",
            1,
            2,
            projection_routing_id(partition()),
            projection_current_path(partition()),
            blob,
            None,
        );
        assert_eq!(first_request.command_id, replay_request.command_id);
    }

    #[test]
    fn current_and_query_cut_cannot_be_crossed() {
        let mut crossed = prepared(1, 11);
        crossed.current = prepared(2, 12).current;
        let error = plan_atomic_publication(partition(), None, crossed).unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }
}
