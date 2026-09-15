//! Format-v1 partition publication over ordinary Keldra objects. Immutable
//! artifacts are family-scoped and content addressed; `current` is installed
//! only after its complete generation is durable.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Read;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use keldra_index::v1::{
    ArtifactPackReference, ArtifactPackTable, AtomicProjectionPublicationCredits,
    CanonicalRecipeState, ComponentIdentity, ComponentRecordLookup, ComponentStreamReverseCursor,
    ComponentStreamReverseStep, ComponentStreamRoot, PreparedAtomicProjectionGeneration,
    PreparedQueryMutationBatch, ProjectedDocumentState, ProjectionCatalogActivation,
    ProjectionCurrent, ProjectionFamilyPartitionDirectory, ProjectionGeneration,
    ProjectionPackCredits, ProjectionPartitionIdentity, ProjectionQueryRunDescriptor,
    QueryBlockCredits, QueryBlockLimits, QueryRunPage, StableDocumentKey,
    component_stream_child_hashes, decode_component_delta_segment, decode_document_head,
    decode_projection_catalog_activation, decode_projection_current,
    decode_projection_family_directory, decode_projection_generation,
    decode_projection_generation_header, decode_query_run_page, decode_source_records,
    encode_projection_catalog_activation, encode_projection_family_directory,
    lookup_component_record_in_verified_pack, pack_component_deltas,
    prepare_atomic_projection_generation, prepare_projection_query_run,
    projection_artifact_routing_id, projection_catalog_activation_path,
    projection_catalog_routing_id, projection_component_page_path, projection_current_path,
    projection_family_directory_path, projection_generation_path, projection_pack_path,
    projection_query_run_pack_path, projection_query_run_stream_page_path, projection_routing_id,
    projection_stream_page_path,
};
use keldra_store::{
    BlobRef, MAX_DERIVED_PROGRESS_INLINE_BATCH_BYTES, MAX_DERIVED_PROGRESS_INLINE_BATCH_ITEMS,
    MutationError, ObjectKey, PAYLOAD_ARTIFACT_CHUNK_BYTES, Store, VersionId,
};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;

use super::publication::{DerivedArtifactAdmission, IndexArtifactPublish, IndexArtifactRouter};
use super::v1_artifact_cache::ImmutableArtifactCache;
use super::v1_compaction::{V1CompactionArtifacts, V1CompactionBase};
use super::v1_parallel::run_bounded_ordered;

mod immutable_staging;
mod physical_packs;
mod projection_cache_updates;
mod publication_types;
#[cfg(test)]
use immutable_staging::{immutable_stage_windows, immutable_stage_work, inline_window_fits};
use projection_cache_updates::ProjectionCacheAdvancer;
use publication_types::{
    ArtifactBytes, AtomicPublicationPlan, ComponentRecordRequest, ImmutableStageWindow,
    InlineArtifactIdentity, ObservedSourceProgress, StagedArtifact,
};
pub(crate) use publication_types::{
    LoadedV1ProjectionGeneration, PendingV1Publication, V1PostCasVerification,
    V1PublicationPredecessor,
};

const MAX_STREAM_PAGE_BYTES: usize = 32 * 1024;
const MAX_GENERATION_BYTES: usize = 256 * 1024;
const MAX_FAMILY_DIRECTORY_BYTES: usize = 32 * 1024 * 1024;
const MAX_CATALOG_ACTIVATION_BYTES: usize = 32 * 1024 * 1024;
const MAX_PARALLEL_IMMUTABLE_STAGE_WINDOWS: usize = 4;
const MAX_PARALLEL_DIRECTORY_READS: usize = 4;
#[derive(Clone)]
pub(crate) struct V1ProjectionPublisher {
    store: Store,
    reader: ClusterObjectReader,
    artifacts: IndexArtifactRouter,
    changes: tokio::sync::broadcast::Sender<()>,
    immutable_cache: ImmutableArtifactCache,
    observed_source_next: ObservedSourceProgress,
    projection_cache_updates: ProjectionCacheAdvancer,
}

impl V1ProjectionPublisher {
    pub(crate) fn new(
        store: Store,
        reader: ClusterObjectReader,
        artifacts: IndexArtifactRouter,
        projection_cache_bytes: usize,
    ) -> Self {
        let (changes, _) = tokio::sync::broadcast::channel(1_024);
        let projection_cache_updates =
            ProjectionCacheAdvancer::start(store.clone(), projection_cache_bytes);
        Self {
            store,
            reader,
            artifacts,
            changes,
            immutable_cache: ImmutableArtifactCache::default(),
            observed_source_next: ObservedSourceProgress::default(),
            projection_cache_updates,
        }
    }

    /// Replaces the disposable producer observation used to reject freshness
    /// against relevant source progress without scanning a journal on queries.
    pub(crate) fn replace_observed_source_next(
        &self,
        observations: &BTreeMap<ProjectionPartitionIdentity, u64>,
    ) {
        self.observed_source_next.replace(observations);
    }

    pub(crate) fn observed_source_next(
        &self,
        partition: ProjectionPartitionIdentity,
    ) -> Option<u64> {
        self.observed_source_next.get(partition)
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::broadcast::Receiver<()> {
        self.changes.subscribe()
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
        previous: Option<&LoadedV1ProjectionGeneration>,
        source_start_offset: u64,
        next_offset: u64,
        through_atomic_position: u64,
        deltas: Vec<keldra_index::v1::SealedComponentDelta>,
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
        previous: &LoadedV1ProjectionGeneration,
        compaction: &V1CompactionBase,
        source_start_offset: u64,
        next_offset: u64,
        through_atomic_position: u64,
        deltas: Vec<keldra_index::v1::SealedComponentDelta>,
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
        previous: Option<&LoadedV1ProjectionGeneration>,
        compaction: Option<&V1CompactionBase>,
        source_start_offset: u64,
        next_offset: u64,
        through_atomic_position: u64,
        deltas: Vec<keldra_index::v1::SealedComponentDelta>,
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
                        .map(|(bytes, _)| Bytes::from(bytes))
                        .ok_or_else(|| Status::data_loss("v1 component stream page is absent"))?
                    };
                    preloaded_bytes = preloaded_bytes
                        .checked_add(bytes.len())
                        .filter(|bytes| *bytes <= maximum_preload_bytes)
                        .ok_or_else(|| {
                            Status::resource_exhausted(
                                "v1 previous-spine preload exceeds memory bound",
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
                        .map(|(bytes, _)| Bytes::from(bytes))
                        .ok_or_else(|| Status::data_loss("v1 query stream page is absent"))?
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
                                "v1 previous-spine preload exceeds memory bound",
                            )
                        })?;
                    query_pages.insert(hash, bytes);
                }
            }
        }
        let base = previous
            .map(|previous| compaction.map_or(&previous.generation, |value| &value.predecessor));
        let query_sequence = base.map_or(1, |generation| {
            generation.query_stream_root.last_sequence.saturating_add(1)
        });
        let component_packs = pack_component_deltas(deltas, pack_credits).map_err(index_status)?;
        let query = prepare_projection_query_run(
            partition,
            physical_catalog_generation,
            query_sequence,
            source_start_offset,
            next_offset,
            through_atomic_position,
            query_batch,
            QueryBlockLimits::default_for_memory(),
            query_credits,
        )
        .map_err(index_status)?;
        let component_pack_table = self
            .publish_component_packs(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                partition,
                &component_packs.packs,
            )
            .await?;
        let query_pack_table = self
            .publish_query_packs(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                partition,
                query.packs(),
            )
            .await?;
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
            component_packs,
            component_pack_table,
            query,
            query_pack_table,
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
                "v1 family directory belongs to a different projection family",
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
                "v1 family directory exceeds its encoded authority-object bound",
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
        let _ = self.changes.send(());
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
                "v1 catalog activation path and payload disagree",
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
                "v1 catalog activation exceeds its encoded authority-object bound",
            ));
        }
        let blob = self.stage(&bytes).await?;
        // Directory and activation share one stable family routing authority;
        // the physical generation is catalog state, not a routing input.
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
        let _ = self.changes.send(());
        Ok(outcome.version)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_atomic_publication(
        &self,
        partition: ProjectionPartitionIdentity,
        predecessor: V1PublicationPredecessor<'_>,
        mut prepared: PreparedAtomicProjectionGeneration,
        compaction: Option<V1CompactionArtifacts>,
        checkpointed_source_positions: u64,
        checkpointed_source_payload_bytes: u64,
    ) -> Result<PendingV1Publication, Status> {
        let telemetry = super::v1_telemetry::global();
        let (previous, expected_current_version) = match predecessor {
            V1PublicationPredecessor::Initial => (None, None),
            V1PublicationPredecessor::Current(previous) => {
                (Some(previous), Some(previous.current_object_version))
            }
            V1PublicationPredecessor::CatalogRebuild(version) => (None, Some(version)),
        };
        if let Some(compaction) = &compaction {
            if let Some(component) = &compaction.component {
                prepared
                    .stream_pages
                    .extend(component.pages.iter().cloned());
            }
            if let Some(query) = &compaction.query {
                prepared
                    .query_stream_pages
                    .extend(query.splice().pages.iter().cloned());
            }
        }
        let publication_plan_timer = super::v1_telemetry::V1PipelineTelemetry::start_phase(
            &telemetry.publication_plan_nanos,
        );
        let plan = plan_atomic_publication(partition, previous, prepared)?;
        drop(publication_plan_timer);
        if checkpointed_source_positions != plan.source_positions {
            return Err(Status::data_loss(
                "v1 publication telemetry positions do not match the prepared source cut",
            ));
        }
        Ok(PendingV1Publication {
            plan,
            expected_current_version,
            previous_generation_hash: previous.map(|value| value.current.generation_hash),
            checkpointed_source_positions,
            checkpointed_source_payload_bytes,
            _compaction: compaction,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_atomic_generation(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        pending: &mut PendingV1Publication,
        predecessor_verification: &mut Option<V1PostCasVerification>,
    ) -> Result<(LoadedV1ProjectionGeneration, V1PostCasVerification), Status> {
        let telemetry = super::v1_telemetry::global();
        let _atomic_publication_timer = super::v1_telemetry::V1PipelineTelemetry::start_phase(
            &telemetry.atomic_publication_nanos,
        );
        let plan = &mut pending.plan;
        let sealed_bytes = plan.sealed_bytes;
        let next_offset = plan.current.next_offset;
        let generation_hash = plan.current.generation_hash;
        let immutable_artifact_count = plan.immutable.len();
        let staging_started = Instant::now();
        let staged_artifacts = self
            .stage_immutable_artifacts(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                plan.immutable.clone(),
            )
            .await?;
        let staging_duration = staging_started.elapsed();
        let mut publications = Vec::with_capacity(staged_artifacts.len());
        for artifact in staged_artifacts {
            if !artifact.needs_publication {
                continue;
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
                artifact.blob,
                None,
            ));
        }
        let immutable_publication_started = Instant::now();
        let immutable_publication_timer = super::v1_telemetry::V1PipelineTelemetry::start_phase(
            &telemetry.immutable_publication_nanos,
        );
        require_all_immutable_publications(if publications.is_empty() {
            Vec::new()
        } else {
            self.artifacts.publish_immutable_many(publications).await?
        })?;
        let immutable_publication_duration = immutable_publication_started.elapsed();
        drop(immutable_publication_timer);
        // Exact verification of the predecessor overlaps source reads,
        // extraction, generation construction, staging, and immutable
        // publication. Only the one ordered Current CAS chain waits for it.
        if let Some(verification) = predecessor_verification.take() {
            verification.finish().await?;
        }
        let current_staging_timer =
            super::v1_telemetry::V1PipelineTelemetry::start_phase(&telemetry.current_staging_nanos);
        let current_blob = self.stage(&plan.current_bytes).await?;
        drop(current_staging_timer);
        if current_blob.hash != *keldra_index::profiled_blake3_hash!(&plan.current_bytes).as_bytes()
            || current_blob.length != plan.current_bytes.len() as u64
        {
            return Err(Status::data_loss(
                "staged v1 current changed its exact bytes",
            ));
        }
        let current_cas_started = Instant::now();
        let current_cas_timer =
            super::v1_telemetry::V1PipelineTelemetry::start_phase(&telemetry.current_cas_nanos);
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
                pending.expected_current_version,
            ))
            .await;
        let (current_object_version, recovered_generation) = match outcome {
            Ok(outcome) => (outcome.version, None),
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::Aborted
                ) =>
            {
                // A routed/quorum response can be lost after the Current CAS
                // committed. Resolve authority before retrying so an ambiguous
                // success does not wedge on its now-stale expected version.
                match self
                    .load_current(storage_tenant, bucket, tenant_id, bucket_id, partition)
                    .await?
                {
                    Some(loaded) if loaded.current.generation_hash == generation_hash => {
                        (loaded.current_object_version, Some(loaded.generation))
                    }
                    Some(loaded)
                        if Some(loaded.current_object_version)
                            == pending.expected_current_version =>
                    {
                        return Err(error);
                    }
                    Some(_) => {
                        return Err(Status::aborted(
                            "v1 Current advanced to another generation during ambiguous publication",
                        ));
                    }
                    None => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        // `pending` retains prepared-byte admission through immutable
        // publication and the Current CAS. The caller drops it only after this
        // method reports the authoritative result.
        let current_cas_duration = current_cas_started.elapsed();
        drop(current_cas_timer);
        let cache_update = self.projection_cache_updates.admit(
            projection_state_partition_key(tenant_id, bucket_id, partition),
            pending.previous_generation_hash,
            generation_hash,
            std::mem::take(&mut plan.state_updates),
        );
        let cache_admitted = cache_update.is_some();
        tracing::info!(
            histogram.keldra_index_v1_artifact_staging_duration_seconds =
                staging_duration.as_secs_f64(),
            histogram.keldra_index_v1_immutable_publication_duration_seconds =
                immutable_publication_duration.as_secs_f64(),
            histogram.keldra_index_v1_current_cas_duration_seconds =
                current_cas_duration.as_secs_f64(),
            immutable_artifacts = immutable_artifact_count,
            cache_admitted,
            "keldra_index_v1_publication_phases"
        );
        let _ = self.changes.send(());
        // Publication and progress telemetry become true only after the
        // partition-current CAS committed. Failures above may leave safe,
        // unreachable immutable artifacts but never claim progress.
        super::v1_telemetry::V1PipelineTelemetry::set(
            &super::v1_telemetry::global().local_next_offset,
            next_offset,
        );
        super::v1_telemetry::V1PipelineTelemetry::add(
            &super::v1_telemetry::global().sealed_bytes,
            sealed_bytes,
        );
        super::v1_telemetry::V1PipelineTelemetry::add(
            &super::v1_telemetry::global().checkpointed_source_positions,
            pending.checkpointed_source_positions,
        );
        super::v1_telemetry::V1PipelineTelemetry::add(
            &super::v1_telemetry::global().checkpointed_source_payload_bytes,
            pending.checkpointed_source_payload_bytes,
        );
        let loaded = LoadedV1ProjectionGeneration {
            current: plan.current.clone(),
            current_object_version,
            generation: recovered_generation.unwrap_or_else(|| plan.generation.clone()),
        };
        let publisher = self.clone();
        let storage_tenant = storage_tenant.to_owned();
        let bucket = bucket.to_owned();
        let expected = loaded.clone();
        let task = tokio::spawn(async move {
            let result = async {
                let verification_started = Instant::now();
                let verification_timer = super::v1_telemetry::V1PipelineTelemetry::start_phase(
                    &super::v1_telemetry::global().post_cas_verification_nanos,
                );
                let observed = publisher
                    .load_generation_by_hash(
                        &storage_tenant,
                        &bucket,
                        tenant_id,
                        bucket_id,
                        partition,
                        generation_hash,
                    )
                    .await?;
                expected
                    .current
                    .validate_against(&observed)
                    .map_err(index_status)?;
                if observed != expected.generation {
                    return Err(Status::data_loss(
                        "published v1 generation differs from the prepared generation",
                    ));
                }
                let cache_scheduled = cache_update.is_some_and(|cache_update| {
                    publisher.projection_cache_updates.schedule(cache_update)
                });
                if !cache_scheduled {
                    tracing::info!(
                        counter.keldra_index_v1_projection_cache_updates_skipped = 1_u64,
                        "v1 disposable projection cache update skipped at its byte bound or shutdown"
                    );
                }
                let duration = verification_started.elapsed();
                drop(verification_timer);
                tracing::info!(
                    histogram.keldra_index_v1_post_cas_verification_duration_seconds =
                        duration.as_secs_f64(),
                    generation_hash = ?generation_hash,
                    cache_scheduled,
                    "keldra_index_v1_post_cas_verification"
                );
                Ok(())
            }
            .await;
            let _ = publisher.changes.send(());
            result
        });
        Ok((loaded, V1PostCasVerification { task: Some(task) }))
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
        compaction: &V1CompactionArtifacts,
    ) -> Result<(), Status> {
        let mut artifacts = BTreeMap::new();
        if let Some(component) = &compaction.component {
            for page in &component.pages {
                insert_artifact(
                    &mut artifacts,
                    projection_stream_page_path(partition, page.hash),
                    keldra_index::v1::ProjectionArtifactKind::StreamPage,
                    page.hash,
                    page.bytes.clone(),
                )?;
            }
        }
        if let Some(query) = &compaction.query {
            let run = &query.artifacts().run;
            insert_artifact(
                &mut artifacts,
                projection_query_run_pack_path(partition, run.hash),
                keldra_index::v1::ProjectionArtifactKind::QueryRunPack,
                run.hash,
                run.bytes.clone(),
            )?;
            for page in &query.splice().pages {
                insert_artifact(
                    &mut artifacts,
                    projection_query_run_stream_page_path(partition, page.hash),
                    keldra_index::v1::ProjectionArtifactKind::QueryRunStreamPage,
                    page.hash,
                    page.bytes.clone(),
                )?;
            }
        }
        let mut publications = Vec::with_capacity(artifacts.len());
        for artifact in self
            .stage_immutable_artifacts(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                artifacts.into_values().collect(),
            )
            .await?
        {
            if !artifact.needs_publication {
                continue;
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
                artifact.blob,
                None,
            ));
        }
        let telemetry = super::v1_telemetry::global();
        let immutable_publication_timer = super::v1_telemetry::V1PipelineTelemetry::start_phase(
            &telemetry.immutable_publication_nanos,
        );
        require_all_immutable_publications(if publications.is_empty() {
            Vec::new()
        } else {
            self.artifacts.publish_immutable_many(publications).await?
        })?;
        drop(immutable_publication_timer);
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
    ) -> Result<Option<LoadedV1ProjectionGeneration>, Status> {
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
                "v1 current pointer belongs to another partition",
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
        Ok(Some(LoadedV1ProjectionGeneration {
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
        let mut cursor = ComponentStreamReverseCursor::for_key(
            ComponentStreamRoot::from_component_root(root).map_err(index_status)?,
            key,
        )
        .map_err(index_status)?;
        loop {
            match cursor.next().map_err(index_status)? {
                ComponentStreamReverseStep::LoadPage { hash } => {
                    let path = projection_stream_page_path(generation.partition, hash);
                    let bytes = self
                        .read_immutable_object(
                            storage_tenant,
                            bucket,
                            tenant_id,
                            bucket_id,
                            &path,
                            hash,
                            MAX_STREAM_PAGE_BYTES,
                        )
                        .await?
                        .ok_or_else(|| Status::data_loss("v1 stream page is absent"))?;
                    cursor.provide_page(hash, &bytes).map_err(index_status)?;
                }
                ComponentStreamReverseStep::Segment(descriptor) => {
                    if key < descriptor.minimum_key || key > descriptor.maximum_key {
                        continue;
                    }
                    let pack = descriptor.pack_reference().map_err(index_status)?;
                    let bytes = self
                        .read_exact_artifact_pack(
                            storage_tenant,
                            bucket,
                            tenant_id,
                            bucket_id,
                            pack,
                        )
                        .await?;
                    match lookup_component_record_in_verified_pack(
                        component,
                        &descriptor,
                        &bytes,
                        key,
                    )
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

    /// Loads an ordered source-path phase through two generation-gated cache
    /// batches: source locators first, then all derived heads and components.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_source_states_batch(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        current: &LoadedV1ProjectionGeneration,
        source_scope: [u8; 32],
        source_paths: &[&str],
        fallback_parallelism: usize,
    ) -> Result<Vec<Vec<ProjectedDocumentState>>, Status> {
        if source_paths.is_empty() {
            return Ok(Vec::new());
        }
        let generation = &current.generation;
        let locator_requests = source_paths
            .iter()
            .map(|source_path| {
                Ok(ComponentRecordRequest {
                    component: ComponentIdentity::SourceRecords,
                    key: StableDocumentKey::derive(source_scope, source_path, 0)
                        .map_err(index_status)?,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let encoded_locators = self
            .load_cached_component_records(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                current,
                &locator_requests,
                fallback_parallelism,
            )
            .await?;
        let records = source_paths
            .iter()
            .zip(encoded_locators)
            .map(|(source_path, encoded)| match encoded {
                Some(encoded) => {
                    decode_source_records(source_scope, source_path, &encoded).map_err(index_status)
                }
                None => Ok(Vec::new()),
            })
            .collect::<Result<Vec<_>, Status>>()?;
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
        let documents = records
            .iter()
            .enumerate()
            .flat_map(|(source_index, records)| {
                records.iter().copied().map(move |key| (source_index, key))
            })
            .collect::<Vec<_>>();
        let detail_requests = documents
            .iter()
            .flat_map(|(_, key)| {
                std::iter::once(ComponentRecordRequest {
                    component: ComponentIdentity::DocumentHead,
                    key: *key,
                })
                .chain(component_recipes.iter().map(|(membership, recipe)| {
                    ComponentRecordRequest {
                        component: if *membership {
                            ComponentIdentity::Membership(*recipe)
                        } else {
                            ComponentIdentity::Field(*recipe)
                        },
                        key: *key,
                    }
                }))
            })
            .collect::<Vec<_>>();
        let details = self
            .load_cached_component_records(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                current,
                &detail_requests,
                fallback_parallelism,
            )
            .await?;
        let mut details = details.into_iter();
        let mut states = source_paths
            .iter()
            .map(|_| Vec::new())
            .collect::<Vec<Vec<ProjectedDocumentState>>>();
        for (source_index, key) in documents {
            let encoded_head = details.next().flatten().ok_or_else(|| {
                Status::data_loss("v1 source locator names a missing document head")
            })?;
            let head =
                decode_document_head(source_scope, key, &encoded_head).map_err(index_status)?;
            if head.stable_key != key || head.source_path != source_paths[source_index] {
                return Err(Status::data_loss(
                    "v1 source locator, document head, and source scope disagree",
                ));
            }
            let mut memberships = Vec::new();
            let mut fields = Vec::new();
            for (membership, recipe) in &component_recipes {
                let value = details.next().ok_or_else(|| {
                    Status::data_loss("v1 predecessor detail batch omitted a request")
                })?;
                let Some(value) = value else {
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
            states[source_index].push(
                ProjectedDocumentState::new(source_scope, head, memberships, fields)
                    .map_err(index_status)?,
            );
        }
        if details.next().is_some() {
            return Err(Status::data_loss(
                "v1 predecessor detail batch returned the wrong result count",
            ));
        }
        for states in &mut states {
            states.sort_by_key(|state| state.head.source_record);
        }
        Ok(states)
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_cached_component_records(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        current: &LoadedV1ProjectionGeneration,
        requests: &[ComponentRecordRequest],
        fallback_parallelism: usize,
    ) -> Result<Vec<Option<Vec<u8>>>, Status> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let partition_key =
            projection_state_partition_key(tenant_id, bucket_id, current.generation.partition);
        let record_keys = requests
            .iter()
            .map(|request| projection_state_record_key(request.component, request.key))
            .collect::<Vec<_>>();
        let record_key_refs = record_keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let cached = self
            .store
            .index_projection_states(
                &partition_key,
                current.current.generation_hash,
                &record_key_refs,
            )
            .unwrap_or_else(|_| {
                std::iter::repeat_with(|| None)
                    .take(requests.len())
                    .collect()
            });
        let mut values = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<_>>();
        let mut missed = Vec::new();
        for (index, (request, cached)) in requests.iter().copied().zip(cached).enumerate() {
            if let Some(value) = cached {
                values[index] = Some(value);
                continue;
            }
            missed.push((index, request));
        }
        let mut fills = Vec::new();
        let mut first_failure = None;
        for missed in missed.chunks(fallback_parallelism.max(1)) {
            let mut jobs = tokio::task::JoinSet::new();
            for (index, request) in missed.iter().copied() {
                let publisher = self.clone();
                let storage_tenant = storage_tenant.to_owned();
                let bucket = bucket.to_owned();
                let generation = current.generation.clone();
                jobs.spawn(async move {
                    let value = publisher
                        .load_component_record(
                            &storage_tenant,
                            &bucket,
                            tenant_id,
                            bucket_id,
                            &generation,
                            request.component,
                            request.key,
                        )
                        .await?;
                    Ok::<_, Status>((index, value))
                });
            }
            while let Some(result) = jobs.join_next().await {
                match result {
                    Ok(Ok((index, value))) => {
                        fills.push((index, value.clone()));
                        values[index] = Some(value);
                    }
                    Ok(Err(error)) => {
                        first_failure.get_or_insert(error);
                    }
                    Err(error) => {
                        first_failure.get_or_insert_with(|| {
                            Status::internal(format!("v1 component load task failed: {error}"))
                        });
                    }
                }
            }
        }
        fills.sort_by_key(|(index, _)| *index);
        let fill_values = fills
            .iter()
            .map(|(index, value)| (record_keys[*index].as_slice(), value.as_deref()))
            .collect::<Vec<_>>();
        let _ = self.store.cache_index_projection_states(
            &partition_key,
            current.current.generation_hash,
            &fill_values,
        );
        if let Some(error) = first_failure {
            return Err(error);
        }
        values
            .into_iter()
            .map(|value| {
                value.ok_or_else(|| Status::data_loss("v1 component batch omitted a request"))
            })
            .collect()
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
            .ok_or_else(|| Status::data_loss("v1 generation object is absent"))?;
        let header = decode_projection_generation_header(&bytes).map_err(index_status)?;
        if header.partition != partition {
            return Err(Status::data_loss(
                "v1 generation header belongs to another partition",
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
    async fn load_component_directory(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        root_hash: [u8; 32],
        root_count: u64,
    ) -> Result<keldra_index::v1::ComponentDirectory, Status> {
        if root_count == 0 {
            return Ok(keldra_index::v1::ComponentDirectory {
                root_hash,
                root_count,
                pages: Vec::new(),
            });
        }
        let mut pending = VecDeque::from([root_hash]);
        let mut visited = BTreeSet::new();
        let mut pages = Vec::new();
        while !pending.is_empty() {
            let mut work = Vec::new();
            while work.len() < MAX_PARALLEL_DIRECTORY_READS {
                let Some(hash) = pending.pop_front() else {
                    break;
                };
                if !visited.insert(hash) || visited.len() > root_count as usize * 2 {
                    return Err(Status::data_loss(
                        "v1 component directory contains a cycle or exceeds its bound",
                    ));
                }
                work.push((work.len(), hash));
            }
            let publisher = self.clone();
            let storage_tenant = storage_tenant.to_owned();
            let bucket = bucket.to_owned();
            let loaded = run_bounded_ordered(work, MAX_PARALLEL_DIRECTORY_READS, move |hash| {
                let publisher = publisher.clone();
                let storage_tenant = storage_tenant.clone();
                let bucket = bucket.clone();
                async move {
                    let path = projection_component_page_path(partition, hash);
                    let (bytes, _) = publisher
                        .read_object(
                            &storage_tenant,
                            &bucket,
                            tenant_id,
                            bucket_id,
                            &path,
                            Some(hash),
                            MAX_STREAM_PAGE_BYTES,
                        )
                        .await?
                        .ok_or_else(|| Status::data_loss("v1 component page is absent"))?;
                    Ok::<_, Status>((hash, bytes))
                }
            })
            .await?;
            for (_, loaded) in loaded {
                let (hash, bytes) = loaded?;
                pending.extend(
                    keldra_index::v1::component_directory_child_hashes(&bytes)
                        .map_err(index_status)?,
                );
                pages.push(keldra_index::v1::EncodedComponentDirectoryPage { hash, bytes });
            }
        }
        Ok(keldra_index::v1::ComponentDirectory {
            root_hash,
            root_count,
            pages,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_immutable_object(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        expected_hash: [u8; 32],
        maximum_bytes: usize,
    ) -> Result<Option<Bytes>, Status> {
        // Keep reuse inside the same authoritative object path. A content hash
        // alone must not allow another tenant, bucket, family, or artifact kind
        // to satisfy this read.
        self.immutable_cache
            .get_or_load(
                tenant_id,
                bucket_id,
                path,
                expected_hash,
                None,
                maximum_bytes,
                || async {
                    Ok(self
                        .read_object(
                            storage_tenant,
                            bucket,
                            tenant_id,
                            bucket_id,
                            path,
                            Some(expected_hash),
                            maximum_bytes,
                        )
                        .await?
                        .map(|(bytes, _)| Bytes::from(bytes)))
                },
            )
            .await
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
        tracing::debug!(
            projection.path = path,
            "v1 artifact read begins stable head selection"
        );
        let Some(snapshot) = self
            .reader
            .current_head_snapshot_stable(&key, tenant_id, bucket_id)
            .await?
        else {
            return Ok(None);
        };
        let version = snapshot.version;
        tracing::debug!(
            projection.path = path,
            "v1 artifact read selected its stable head"
        );
        if version.deleted {
            return Err(Status::data_loss("v1 projection artifact is deleted"));
        }
        let blob = version
            .blob
            .as_ref()
            .ok_or_else(|| Status::data_loss("v1 projection artifact has no blob"))?;
        if expected_hash.is_some_and(|hash| blob.hash != hash) {
            return Err(Status::data_loss(
                "v1 projection path and payload hash differ",
            ));
        }
        let bytes = self
            .read_blob_local_first_uncached(blob, maximum_bytes)
            .await?;
        Ok(Some((bytes, version.id)))
    }

    /// Reads an immutable artifact from the local integrated blob store when
    /// present, reconstructing it from peers only when this node lacks it.
    pub(crate) async fn read_blob_local_first(
        &self,
        blob: &BlobRef,
        maximum_bytes: usize,
    ) -> Result<Bytes, Status> {
        if blob.length > maximum_bytes as u64 {
            return Err(Status::data_loss(
                "v1 projection artifact violates its exact byte bound",
            ));
        }
        if let Some(bytes) = self.immutable_cache.get_blob(blob, maximum_bytes)? {
            return Ok(bytes);
        }
        let bytes = Bytes::from(
            self.read_blob_local_first_uncached(blob, maximum_bytes)
                .await?,
        );
        self.immutable_cache.insert_blob(blob, bytes.clone());
        Ok(bytes)
    }

    pub(crate) fn cached_query_run(
        &self,
        blob: &BlobRef,
        maximum_bytes: usize,
    ) -> Result<Option<Arc<ProjectionQueryRunDescriptor>>, Status> {
        self.immutable_cache.get_query_run(blob, maximum_bytes)
    }

    pub(crate) fn cache_query_run(
        &self,
        blob: &BlobRef,
        descriptor: Arc<ProjectionQueryRunDescriptor>,
    ) {
        self.immutable_cache.insert_query_run(blob, descriptor);
    }

    pub(crate) fn cached_query_block(
        &self,
        blob: &BlobRef,
        generation: [u8; 32],
        maximum_bytes: usize,
    ) -> Result<Option<Arc<keldra_index::v1::DecodedQueryBlock>>, Status> {
        self.immutable_cache
            .get_query_block(blob, generation, maximum_bytes)
    }

    pub(crate) fn cache_query_block(
        &self,
        blob: &BlobRef,
        generation: [u8; 32],
        block: Arc<keldra_index::v1::DecodedQueryBlock>,
    ) {
        self.immutable_cache
            .insert_query_block(blob, generation, block);
    }

    async fn read_blob_local_first_uncached(
        &self,
        blob: &BlobRef,
        maximum_bytes: usize,
    ) -> Result<Vec<u8>, Status> {
        if blob.length > maximum_bytes as u64 {
            return Err(Status::data_loss(
                "v1 projection artifact violates its exact byte bound",
            ));
        }
        let read_limit = u64::try_from(maximum_bytes)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let expected_length = usize::try_from(blob.length).map_err(|_| {
            Status::data_loss("v1 projection artifact violates its exact byte bound")
        })?;
        let bytes = match self.store.read_blob_bytes(blob).await {
            Ok(bytes) => {
                tracing::debug!("v1 artifact read opened its local integrated blob");
                bytes
            }
            Err(MutationError::BlobNotFound) => {
                tracing::debug!("v1 artifact read falls back to distributed reconstruction");
                let mut bytes = Vec::with_capacity(expected_length);
                let mut payload = self.reader.open_blob_payload(blob).await?;
                payload
                    .by_ref()
                    .take(read_limit)
                    .read_to_end(&mut bytes)
                    .map_err(|error| {
                        Status::internal(format!(
                            "read distributed v1 projection artifact: {error}"
                        ))
                    })?;
                bytes
            }
            Err(error) => return Err(Status::unavailable(error.to_string())),
        };
        if bytes.len() > maximum_bytes || bytes.len() as u64 != blob.length {
            return Err(Status::data_loss(
                "v1 projection artifact violates its exact byte bound",
            ));
        }
        Ok(bytes)
    }
}

fn plan_atomic_publication(
    partition: ProjectionPartitionIdentity,
    previous: Option<&LoadedV1ProjectionGeneration>,
    prepared: PreparedAtomicProjectionGeneration,
) -> Result<AtomicPublicationPlan, Status> {
    let (prepared, publication_credits) = prepared.into_publication_parts();
    let state_updates = projection_state_updates(&prepared.packs)?;
    let sealed_bytes = prepared
        .packs
        .iter()
        .flat_map(|pack| &pack.deltas)
        .try_fold(0_u64, |total, delta| {
            total
                .checked_add(delta.locator.encoded_bytes)
                .ok_or_else(|| Status::resource_exhausted("v1 sealed delta bytes overflow"))
        })?;
    if prepared.generation.hash
        != *keldra_index::profiled_blake3_hash!(&prepared.generation.bytes).as_bytes()
    {
        return Err(Status::data_loss(
            "prepared v1 generation has the wrong content hash",
        ));
    }
    let generation = decode_projection_generation(
        &prepared.generation.bytes,
        &prepared.generation.component_directory,
    )
    .map_err(index_status)?;
    if generation.partition != partition {
        return Err(Status::data_loss(
            "prepared v1 generation belongs to another partition",
        ));
    }
    let current = decode_projection_current(&prepared.current).map_err(index_status)?;
    current
        .validate_against(&generation)
        .map_err(index_status)?;
    if current.generation_hash != prepared.generation.hash {
        return Err(Status::data_loss(
            "prepared v1 current does not name the encoded generation",
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
                "prepared v1 generation is not the exact predecessor successor",
            ));
        }
        None if generation.previous_generation_hash.is_none() && generation.revision == 1 => {}
        None => {
            return Err(Status::data_loss(
                "initial prepared v1 generation has a predecessor",
            ));
        }
    }

    let maximum_block_bytes = QueryBlockLimits::default_for_memory().maximum_block_bytes;
    let query_limits = keldra_index::v1::QueryBlockLimits {
        maximum_block_bytes,
        maximum_records: u32::MAX as usize,
        maximum_key_bytes: maximum_block_bytes,
        maximum_value_bytes: maximum_block_bytes,
        maximum_loaded_blocks: QueryBlockLimits::default_for_memory().maximum_loaded_blocks,
        maximum_run_descriptor_bytes: prepared.query_run.bytes.len().max(256),
    };
    let mut validation_credits = keldra_index::v1::QueryBlockCredits::from_query_permit(Box::new(
        PublicationValidationPermit(prepared.query_run.bytes.len().max(1)),
    ))
    .map_err(index_status)?;
    let query_run = keldra_index::v1::decode_projection_query_run(
        &prepared.query_run.bytes,
        query_limits,
        &mut validation_credits,
    )
    .map_err(index_status)?;
    if prepared.query_run.hash
        != *keldra_index::profiled_blake3_hash!(&prepared.query_run.bytes).as_bytes()
        || query_run.partition != partition
        || query_run.physical_catalog_generation != generation.physical_catalog_generation
        || previous
            .is_some_and(|previous| query_run.source_start_offset != previous.current.next_offset)
        || query_run.next_offset != generation.next_offset
        || query_run.through_atomic_position != generation.through_atomic_position
    {
        return Err(Status::data_loss(
            "prepared v1 query run is not bound to its generation cut and blocks",
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
            "prepared v1 query stream does not name its exact newest run cut",
        ));
    }

    let mut artifacts = BTreeMap::new();
    // Physical packs were published first so their exact ordinary-object
    // versions could be encoded into the root-bound locator tables.
    for page in prepared.stream_pages {
        insert_artifact(
            &mut artifacts,
            projection_stream_page_path(partition, page.hash),
            keldra_index::v1::ProjectionArtifactKind::StreamPage,
            page.hash,
            page.bytes,
        )?;
    }
    drop(prepared.query_packs);
    insert_artifact(
        &mut artifacts,
        projection_query_run_pack_path(partition, prepared.query_run.hash),
        keldra_index::v1::ProjectionArtifactKind::QueryRunPack,
        prepared.query_run.hash,
        prepared.query_run.bytes,
    )?;
    for page in prepared.query_stream_pages {
        insert_artifact(
            &mut artifacts,
            projection_query_run_stream_page_path(partition, page.hash),
            keldra_index::v1::ProjectionArtifactKind::QueryRunStreamPage,
            page.hash,
            page.bytes,
        )?;
    }
    for page in prepared.generation.component_directory.pages {
        insert_artifact(
            &mut artifacts,
            projection_component_page_path(partition, page.hash),
            keldra_index::v1::ProjectionArtifactKind::ComponentPage,
            page.hash,
            page.bytes,
        )?;
    }
    insert_artifact(
        &mut artifacts,
        projection_generation_path(partition, prepared.generation.hash),
        keldra_index::v1::ProjectionArtifactKind::Generation,
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
        source_positions: query_run
            .next_offset
            .checked_sub(query_run.source_start_offset)
            .ok_or_else(|| Status::data_loss("v1 query run source cut moves backwards"))?,
        state_updates,
        _publication_credits: publication_credits,
    })
}

fn projection_state_updates(
    packs: &[keldra_index::v1::SealedProjectionDeltaPack],
) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>, Status> {
    let mut updates = BTreeMap::new();
    for pack in packs {
        for delta in &pack.deltas {
            let start = usize::try_from(delta.locator.offset)
                .map_err(|_| Status::data_loss("v1 cached delta offset is unbounded"))?;
            let length = usize::try_from(delta.locator.encoded_bytes)
                .map_err(|_| Status::data_loss("v1 cached delta length is unbounded"))?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| Status::data_loss("v1 cached delta range overflows"))?;
            let encoded = pack
                .bytes
                .get(start..end)
                .ok_or_else(|| Status::data_loss("v1 cached delta range is outside its pack"))?;
            let decoded = decode_component_delta_segment(encoded).map_err(index_status)?;
            if decoded.component != delta.component || decoded.records.len() as u64 != delta.records
            {
                return Err(Status::data_loss(
                    "v1 cached delta identity differs from its pack descriptor",
                ));
            }
            for record in decoded.records {
                let key = projection_state_record_key(delta.component, record.stable_key);
                if updates.insert(key, record.replacement).is_some() {
                    return Err(Status::data_loss(
                        "v1 publication repeats one cached component record",
                    ));
                }
            }
        }
    }
    Ok(updates.into_iter().collect())
}

fn projection_state_partition_key(
    tenant_id: u64,
    bucket_id: u64,
    partition: ProjectionPartitionIdentity,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(51);
    key.push(1);
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&bucket_id.to_be_bytes());
    key.extend_from_slice(&partition.family_id);
    key.extend_from_slice(&partition.source_node.to_be_bytes());
    key
}

fn projection_state_record_key(
    component: ComponentIdentity,
    stable_key: StableDocumentKey,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(65);
    match component {
        ComponentIdentity::DocumentHead => key.push(1),
        ComponentIdentity::Membership(recipe) => {
            key.push(2);
            key.extend_from_slice(&recipe.bytes());
        }
        ComponentIdentity::Field(recipe) => {
            key.push(3);
            key.extend_from_slice(&recipe.bytes());
        }
        ComponentIdentity::Order(recipe) => {
            key.push(4);
            key.extend_from_slice(&recipe.bytes());
        }
        ComponentIdentity::SourceRecords => key.push(6),
    }
    key.extend_from_slice(&stable_key.bytes());
    key
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

impl keldra_index::v1::QueryMemoryPermit for PublicationValidationPermit {
    fn admitted_bytes(&self) -> usize {
        self.0
    }
}

fn newest_prepared_query_run(
    mut hash: [u8; 32],
    pages: &[keldra_index::v1::EncodedQueryRunPage],
) -> Result<keldra_index::v1::QueryRunReference, Status> {
    let pages = pages
        .iter()
        .map(|page| (page.hash, page))
        .collect::<BTreeMap<_, _>>();
    let mut visited = BTreeSet::new();
    loop {
        if !visited.insert(hash) || visited.len() > pages.len() {
            return Err(Status::data_loss(
                "prepared v1 query stream right spine is cyclic or incomplete",
            ));
        }
        let page = pages.get(&hash).ok_or_else(|| {
            Status::data_loss("prepared v1 query stream omits its new right spine")
        })?;
        if page.hash != *keldra_index::profiled_blake3_hash!(&page.bytes).as_bytes() {
            return Err(Status::data_loss(
                "prepared v1 query stream page has the wrong content hash",
            ));
        }
        match decode_query_run_page(&page.bytes).map_err(index_status)? {
            QueryRunPage::Leaf(runs) => {
                return runs
                    .last()
                    .copied()
                    .ok_or_else(|| Status::data_loss("prepared v1 query stream leaf is empty"));
            }
            QueryRunPage::Branch(children) => {
                hash = children
                    .last()
                    .ok_or_else(|| Status::data_loss("prepared v1 query stream branch is empty"))?
                    .hash;
            }
        }
    }
}

fn insert_artifact(
    artifacts: &mut BTreeMap<String, ArtifactBytes>,
    path: String,
    kind: keldra_index::v1::ProjectionArtifactKind,
    hash: [u8; 32],
    bytes: Vec<u8>,
) -> Result<(), Status> {
    if hash != *keldra_index::profiled_blake3_hash!(&bytes).as_bytes() {
        return Err(Status::data_loss(
            "prepared v1 projection artifact has the wrong content hash",
        ));
    }
    match artifacts.entry(path.clone()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(ArtifactBytes {
                path,
                kind,
                hash,
                bytes: Bytes::from(bytes),
            });
        }
        std::collections::btree_map::Entry::Occupied(entry)
            if entry.get().hash == hash && entry.get().bytes == bytes => {}
        std::collections::btree_map::Entry::Occupied(_) => {
            return Err(Status::data_loss(
                "v1 projection path names conflicting immutable bytes",
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
    hasher.update(b"keldra.index.v1.publish/v1");
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
        command_id: format!("index-v1-{}", &hasher.finalize().to_hex().as_str()[..24]),
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
#[path = "v1_publication_tests.rs"]
mod tests;
