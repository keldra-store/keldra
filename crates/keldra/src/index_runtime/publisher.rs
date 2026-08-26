//! Format-v4 index publication through ordinary Keldra objects.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use keldra_index::IndexError;
use keldra_index::compaction::CompactionProgress;
use keldra_index::v4::build::ComponentBatchSink;
use keldra_index::v4::{
    ArtifactDescriptor, ArtifactPackReference, GeneratedComponent, INDEX_ARTIFACT_PACK_BYTES,
    INDEX_COMPONENT_BYTES, IndexKind, SegmentDescriptor, SegmentIdentity,
};
use keldra_store::{BlobRef, DefinitionKind, MutationError, ObjectKey, Store, VersionId};
use tonic::Status;
use tracing::Instrument;

#[path = "publisher/rebuild_checkpoint.rs"]
mod rebuild_checkpoint;
pub(crate) use rebuild_checkpoint::LoadedRebuildRoot;
#[path = "publisher/merge_rebase.rs"]
mod merge_rebase;
#[path = "publisher/prepared_publication.rs"]
mod prepared_publication;
#[path = "publisher/revalidation.rs"]
mod revalidation;

use crate::cluster_object_read::ClusterObjectReader;
use crate::index_config::IndexRuntimeConfig;
use crate::index_service::{StoredIndexDefinition, definition_path};

use super::committed_view::{
    CommitManifestReference, IndexCommitManifest, IndexCurrentPointer, LocatorRoot,
    MAX_RELEASING_COMMIT_REVISIONS, ManifestPhysicalOrder, PendingAtomicBatch,
    ReleasingManifestReference,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexPointerCasClass {
    Incremental,
    Merge,
    Retention,
    Rebuild,
}

impl IndexPointerCasClass {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Incremental => "incremental",
            Self::Merge => "merge",
            Self::Retention => "retention",
            Self::Rebuild => "rebuild",
        }
    }
}

fn cohort_class(class: IndexPointerCasClass) -> PublicationCohortClass {
    match class {
        IndexPointerCasClass::Incremental => PublicationCohortClass::Incremental,
        IndexPointerCasClass::Merge
        | IndexPointerCasClass::Retention
        | IndexPointerCasClass::Rebuild => PublicationCohortClass::Maintenance,
    }
}

fn emit_pointer_cas_metrics(
    index_id: u64,
    tenant_id: u64,
    bucket_id: u64,
    class: IndexPointerCasClass,
    pointer_bytes: u64,
    duration: Duration,
    failed: bool,
    lost: bool,
) {
    tracing::debug!(
        index.id = index_id,
        tenant.id = tenant_id,
        bucket.id = bucket_id,
        publication.cas_class = class.as_str(),
        publication.outcome = if lost {
            "lost"
        } else if failed {
            "failed"
        } else {
            "completed"
        },
        monotonic_counter.keldra_index_current_pointer_cas_attempts_total = 1_u64,
        monotonic_counter.keldra_index_current_pointer_cas_successes_total = u64::from(!failed),
        monotonic_counter.keldra_index_current_pointer_cas_failures_total = u64::from(failed),
        monotonic_counter.keldra_index_current_pointer_cas_losses_total = u64::from(lost),
        histogram.keldra_index_current_pointer_bytes = pointer_bytes,
        histogram.keldra_index_current_pointer_cas_duration_seconds = duration.as_secs_f64(),
        "index current-pointer CAS finished"
    );
}

fn emit_pointer_cas_result<T>(
    index_id: u64,
    tenant_id: u64,
    bucket_id: u64,
    class: IndexPointerCasClass,
    pointer_bytes: u64,
    duration: Duration,
    result: &Result<T, Status>,
) {
    let failed = result.is_err();
    let lost = result
        .as_ref()
        .is_err_and(|error| error.code() == tonic::Code::Aborted);
    emit_pointer_cas_metrics(
        index_id,
        tenant_id,
        bucket_id,
        class,
        pointer_bytes,
        duration,
        failed,
        lost,
    );
}
use super::events::IndexBarrier;
use super::manager::publication_cohort::{IndexPublicationCohorts, PublicationCohortClass};
use super::publication::{
    DefinitionVersionGuard, DerivedArtifactAdmission, GuardedIndexArtifactPublish,
    IndexArtifactOutcome, IndexArtifactPublish, IndexArtifactRouter, IndexCurrentMutationGuard,
    artifact_path, current_path, manifest_path,
};

pub(crate) use prepared_publication::{
    PreparedCurrentPointerPublication, PreparedManifestPublication, PreparedPackPublication,
    PublishedManifest,
};

#[derive(Clone)]
pub(crate) struct IndexCommitPublisher {
    store: Store,
    reader: ClusterObjectReader,
    artifacts: IndexArtifactRouter,
    cohorts: IndexPublicationCohorts,
    config: IndexRuntimeConfig,
}

#[derive(Clone, Debug)]
pub(crate) struct CommittedIndexView {
    pub(crate) pointer: IndexCurrentPointer,
    pub(crate) current_object_version: VersionId,
    pub(crate) manifest: IndexCommitManifest,
}

#[derive(Clone, Debug)]
pub(crate) struct SelectedCommittedIndexView {
    pub(crate) pointer: IndexCurrentPointer,
    pub(crate) current_object_version: VersionId,
    pub(crate) reference: CommitManifestReference,
    pub(crate) manifest: IndexCommitManifest,
}

impl IndexCommitPublisher {
    pub(crate) fn new(
        store: Store,
        reader: ClusterObjectReader,
        artifacts: IndexArtifactRouter,
        cohorts: IndexPublicationCohorts,
        config: IndexRuntimeConfig,
    ) -> Self {
        Self {
            store,
            reader,
            artifacts,
            cohorts,
            config,
        }
    }

    pub(crate) fn component_sink(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        admission: DerivedArtifactAdmission,
        cohort_class: PublicationCohortClass,
    ) -> IndexComponentBatchSink {
        IndexComponentBatchSink {
            store: self.store.clone(),
            cohorts: self.cohorts.clone(),
            definition: definition.clone(),
            tenant_id,
            bucket_id,
            admission,
            cohort_class,
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
        let mut sink = self.component_sink(
            definition,
            tenant_id,
            bucket_id,
            admission,
            PublicationCohortClass::Maintenance,
        );
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
        mut barrier: IndexBarrier,
        mut pending_atomic_batches: Vec<PendingAtomicBatch>,
        physical_order: Vec<ManifestPhysicalOrder>,
        mut segments: Vec<SegmentDescriptor>,
        mut locator_roots: Vec<LocatorRoot>,
        current: Option<&CommittedIndexView>,
        admission: DerivedArtifactAdmission,
        cas_class: IndexPointerCasClass,
    ) -> Result<CommittedIndexView, Status> {
        if cas_class == IndexPointerCasClass::Merge {
            let base = current.ok_or_else(|| {
                Status::failed_precondition("merge publication requires a committed base view")
            })?;
            if base.manifest.barrier().map_err(commit_view_status)? != barrier
                || base.manifest.pending_atomic_batches != pending_atomic_batches
            {
                return Err(Status::failed_precondition(
                    "merge publication must preserve its base source and atomic checkpoints",
                ));
            }
        }
        let mut expected_current = current.cloned();
        'publication: loop {
            let current = expected_current.as_ref();
            segments.sort_by_key(|segment| segment.identity.segment_id);
            locator_roots.sort_by_key(|locator| locator.sequence);
            // The candidate's exact pack versions must remain protected from
            // retention from the proof through manifest durability and the
            // current-pointer CAS. Builders for other indexes retain separate
            // gates and can join the same publication cohort.
            let current_guard = self
                .artifacts
                .acquire_current_mutation(definition.index_id)
                .await?;
            self.revalidate_candidate_while_current_mutation_held(
                definition,
                tenant_id,
                bucket_id,
                &segments,
                &locator_roots,
                current.map(|view| &view.manifest),
                None,
                cas_class,
                &current_guard,
            )
            .await?;
            let revision = current
                .map(|value| value.pointer.current.revision)
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("index commit revision overflow"))?;
            let (artifact_encoded_bytes, artifact_logical_bytes) =
                commit_artifact_totals(&segments, &locator_roots)?;
            let manifest = IndexCommitManifest::new(
                definition.index_id,
                revision,
                definition_version,
                kind,
                schema_fingerprint,
                &barrier,
                pending_atomic_batches.clone(),
                // A rebuild is a new physical lineage from source authority. It
                // deliberately does not name the superseded serving manifest;
                // incremental and merge publications retain non-owning ancestry
                // for diagnosis without turning it into retention authority.
                (cas_class != IndexPointerCasClass::Rebuild)
                    .then(|| current.map(|view| view.pointer.current.blob.hash))
                    .flatten(),
                physical_order.clone(),
                segments.clone(),
                locator_roots.clone(),
                artifact_encoded_bytes,
                artifact_logical_bytes,
            )
            .map_err(commit_view_status)?;

            let manifest_bytes = manifest.encode().map_err(commit_view_status)?;
            let manifest_length = manifest_bytes.len() as u64;
            let manifest_span = tracing::debug_span!(
                "keldra.index.manifest_publish",
                index.id = definition.index_id,
                tenant.id = tenant_id,
                bucket.id = bucket_id,
                index.kind = ?kind,
                revision,
                manifest.bytes = manifest_length,
            );
            let manifest_started = std::time::Instant::now();
            let manifest_result = async {
                let blob = stage_artifact_bytes(&self.store, &manifest_bytes, admission).await?;
                let prepared = PreparedManifestPublication::new(
                    definition, tenant_id, bucket_id, manifest, blob, admission,
                );
                let outcome = self
                    .cohorts
                    .publish_manifest(prepared.request().clone(), cohort_class(cas_class))
                    .await?;
                prepared.apply(outcome, SystemTime::now())
            }
            .instrument(manifest_span.clone())
            .await;
            let manifest_failed = manifest_result.is_err();
            manifest_span.in_scope(|| {
                tracing::debug!(
                    index.kind = ?kind,
                    publish.phase = "manifest",
                    publish.outcome = if manifest_failed { "failed" } else { "completed" },
                    monotonic_counter.keldra_index_manifests_published_total =
                        u64::from(!manifest_failed),
                    monotonic_counter.keldra_index_manifest_publish_failures_total =
                        u64::from(manifest_failed),
                    histogram.keldra_index_manifest_bytes = manifest_length,
                    histogram.keldra_index_manifest_publish_duration_seconds =
                        manifest_started.elapsed().as_secs_f64(),
                    "format-v4 index revision manifest publication finished"
                );
            });
            let PublishedManifest {
                manifest,
                reference: current_reference,
            } = manifest_result?;
            let published_at = SystemTime::now();
            let observed = self.load_current(definition, tenant_id, bucket_id).await?;
            self.revalidate_candidate_while_current_mutation_held(
                definition,
                tenant_id,
                bucket_id,
                &segments,
                &locator_roots,
                observed.as_ref().map(|view| &view.manifest),
                Some(&current_reference),
                cas_class,
                &current_guard,
            )
            .await?;
            let expected_revision = current.map(|value| value.pointer.current.revision);
            let observed_revision = observed
                .as_ref()
                .map(|value| value.pointer.current.revision);
            if observed_revision != expected_revision {
                if let Some(observed) = observed
                    && observed_revision > expected_revision
                {
                    if cas_class == IndexPointerCasClass::Incremental
                        && manifest_covers_barrier(&observed.manifest, &barrier)
                    {
                        return Ok(observed);
                    }
                    if cas_class == IndexPointerCasClass::Merge {
                        let base = current.ok_or_else(|| {
                            Status::failed_precondition(
                                "merge publication has no committed base view",
                            )
                        })?;
                        if !manifest_covers_barrier(&observed.manifest, &barrier)
                            || !manifest_preserves_pending_atomic_batches(
                                &observed.manifest,
                                &base.manifest.pending_atomic_batches,
                            )
                        {
                            return Err(Status::aborted(
                                "newer committed view does not cover the merge base checkpoint and atomic lineage",
                            ));
                        }
                        let rebased = merge_rebase::rebase_merge_candidate(
                            &base.manifest,
                            &segments,
                            &locator_roots,
                            &observed.manifest,
                        )?;
                        barrier = observed.manifest.barrier().map_err(commit_view_status)?;
                        pending_atomic_batches = observed.manifest.pending_atomic_batches.clone();
                        segments = rebased.segments;
                        locator_roots = rebased.locator_roots;
                        expected_current = Some(observed);
                        drop(current_guard);
                        continue 'publication;
                    }
                }
                emit_pointer_cas_metrics(
                    definition.index_id,
                    tenant_id,
                    bucket_id,
                    cas_class,
                    0,
                    Duration::ZERO,
                    true,
                    true,
                );
                return Err(Status::aborted(
                    "a newer committed index revision superseded this publication candidate",
                ));
            }
            if let (Some(expected), Some(observed)) = (current, observed.as_ref())
                && expected.pointer.current != observed.pointer.current
            {
                return Err(Status::data_loss(
                    "current index revision identity changed without advancing",
                ));
            }

            // Publication stays O(pointer references). Exact distinct-object byte
            // enforcement is deliberately performed later by bounded retention
            // maintenance, never while making a new revision visible.
            let retained = select_retained_metadata(
                self.config,
                current_reference.retained_bytes,
                observed.iter().flat_map(|previous| {
                    std::iter::once(&previous.pointer.current)
                        .chain(previous.pointer.retained.iter())
                }),
                published_at,
            )?;
            let releasing = publication_releasing_roots(
                observed.as_ref().map(|value| &value.pointer),
                retained.len(),
                published_at,
            )?;
            let pointer = IndexCurrentPointer::new(
                definition.index_id,
                current_reference,
                retained,
                releasing,
            )
            .map_err(commit_view_status)?;
            let candidate_reference = pointer.current.clone();
            let pointer_bytes = pointer.encode().map_err(commit_view_status)?;
            let pointer_length = pointer_bytes.len() as u64;
            let current_span = tracing::debug_span!(
                "keldra.index.current_pointer_cas",
                index.id = definition.index_id,
                tenant.id = tenant_id,
                bucket.id = bucket_id,
                index.kind = ?kind,
                revision,
                current.bytes = pointer_length,
                retained.revisions = pointer.retained.len() as u64,
            );
            let current_started = std::time::Instant::now();
            let current_result = async {
                let blob = stage_artifact_bytes(&self.store, &pointer_bytes, admission).await?;
                let prepared = PreparedCurrentPointerPublication::new(
                    definition,
                    tenant_id,
                    bucket_id,
                    definition_version,
                    pointer,
                    manifest,
                    blob,
                    observed.as_ref().map(|value| value.current_object_version),
                    admission,
                )?;
                let outcome = self
                    .cohorts
                    .publish_current(
                        GuardedIndexArtifactPublish {
                            request: prepared.request().clone(),
                            current_guard,
                        },
                        barrier.clone(),
                        cohort_class(cas_class),
                    )
                    .await?;
                Ok::<_, Status>((prepared, outcome))
            }
            .instrument(current_span.clone())
            .await;
            let current_failed = current_result.is_err();
            current_span.in_scope(|| {
                let cas_lost = current_result
                    .as_ref()
                    .is_err_and(|error| error.code() == tonic::Code::Aborted);
                tracing::debug!(
                    index.kind = ?kind,
                    publication.cas_class = cas_class.as_str(),
                    publish.phase = "current_pointer_cas",
                    publish.outcome = if current_failed { "failed" } else { "completed" },
                    monotonic_counter.keldra_index_current_pointer_cas_attempts_total = 1_u64,
                    monotonic_counter.keldra_index_current_pointer_cas_successes_total =
                        u64::from(!current_failed),
                    monotonic_counter.keldra_index_current_pointer_cas_failures_total =
                        u64::from(current_failed),
                    monotonic_counter.keldra_index_current_pointer_cas_losses_total =
                        u64::from(cas_lost),
                    histogram.keldra_index_current_pointer_bytes = pointer_length,
                    histogram.keldra_index_current_pointer_cas_duration_seconds =
                        current_started.elapsed().as_secs_f64(),
                    "format-v4 index current-pointer CAS finished"
                );
            });
            match current_result {
                Ok((prepared, outcome)) => return Ok(prepared.apply(outcome)),
                Err(error) if retryable_pack_publish_status(&error) => {
                    let resolved = self.load_current(definition, tenant_id, bucket_id).await?;
                    if let Some(resolved) = resolved
                        && lost_pointer_response_resolved(
                            &candidate_reference,
                            &resolved.pointer.current,
                        )
                    {
                        return Ok(resolved);
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// CAS a strictly smaller retained suffix while preserving the exact
    /// current committed view. The caller owns the complete retention proof; this
    /// operation intentionally performs no manifest or artifact traversal.
    pub(crate) async fn trim_retained(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        current: &CommittedIndexView,
        retained: Vec<CommitManifestReference>,
    ) -> Result<CommittedIndexView, Status> {
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

        let released_at = SystemTime::now();
        let mut releasing = current.pointer.releasing.clone();
        releasing.extend(
            current.pointer.retained[retained.len()..]
                .iter()
                .cloned()
                .map(|reference| ReleasingManifestReference::new(reference, released_at))
                .collect::<Result<Vec<_>, _>>()
                .map_err(commit_view_status)?,
        );
        if releasing.len() > MAX_RELEASING_COMMIT_REVISIONS {
            return Err(Status::resource_exhausted(
                "releasing manifest root queue reached its durable pointer bound",
            ));
        }
        let pointer = IndexCurrentPointer::new(
            definition.index_id,
            current.pointer.current.clone(),
            retained,
            releasing,
        )
        .map_err(commit_view_status)?;
        let pointer_bytes = pointer.encode().map_err(commit_view_status)?;
        let blob = stage_artifact_bytes(
            &self.store,
            &pointer_bytes,
            DerivedArtifactAdmission::Bounded,
        )
        .await?;
        let path = current_path(definition.index_id);
        let cas_started = std::time::Instant::now();
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
                command_id: publish_command(
                    definition.index_id,
                    &path,
                    &blob,
                    Some(current.current_object_version),
                ),
                definition_guard: Some(DefinitionVersionGuard {
                    kind: DefinitionKind::Index,
                    exact_path: definition_path(&definition.name)?,
                    expected_version: VersionId(current.manifest.definition_version),
                }),
                definition_intent: None,
                admission: DerivedArtifactAdmission::Bounded,
            })
            .await;
        emit_pointer_cas_result(
            definition.index_id,
            tenant_id,
            bucket_id,
            IndexPointerCasClass::Retention,
            pointer_bytes.len() as u64,
            cas_started.elapsed(),
            &outcome,
        );
        let outcome = outcome?;
        Ok(CommittedIndexView {
            pointer,
            current_object_version: outcome.version,
            manifest: current.manifest.clone(),
        })
    }

    /// Remove only releasing roots whose exact object graphs have completed
    /// cleanup. Pointer identity is protected by the ordinary exact CAS.
    pub(crate) async fn finish_releasing(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        completed: &[ReleasingManifestReference],
    ) -> Result<CommittedIndexView, Status> {
        let current_guard = self
            .artifacts
            .acquire_current_mutation(definition.index_id)
            .await?;
        let current = self
            .load_current(definition, tenant_id, bucket_id)
            .await?
            .ok_or_else(|| {
                Status::aborted("current pointer disappeared before releasing-root cleanup")
            })?;
        let (completed, releasing) =
            select_completed_releasing(&current.pointer.releasing, completed);
        if completed.is_empty() {
            return Ok(current);
        }
        let pointer = IndexCurrentPointer::new(
            definition.index_id,
            current.pointer.current.clone(),
            current.pointer.retained.clone(),
            releasing,
        )
        .map_err(commit_view_status)?;
        let pointer_bytes = pointer.encode().map_err(commit_view_status)?;
        let blob = stage_artifact_bytes(
            &self.store,
            &pointer_bytes,
            DerivedArtifactAdmission::Bounded,
        )
        .await?;
        let path = current_path(definition.index_id);
        let completed_roots = completed.len() as u64;
        let completed_bytes = completed.iter().fold(0_u64, |total, root| {
            total.saturating_add(root.manifest.retained_bytes)
        });
        let cas_started = std::time::Instant::now();
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
                    expected_version: Some(current.current_object_version),
                    command_id: publish_command(
                        definition.index_id,
                        &path,
                        &blob,
                        Some(current.current_object_version),
                    ),
                    definition_guard: Some(DefinitionVersionGuard {
                        kind: DefinitionKind::Index,
                        exact_path: definition_path(&definition.name)?,
                        expected_version: VersionId(current.manifest.definition_version),
                    }),
                    definition_intent: None,
                    admission: DerivedArtifactAdmission::Bounded,
                },
                Some(&current_guard),
            )
            .await;
        emit_pointer_cas_result(
            definition.index_id,
            tenant_id,
            bucket_id,
            IndexPointerCasClass::Retention,
            pointer_bytes.len() as u64,
            cas_started.elapsed(),
            &outcome,
        );
        let failed = outcome.is_err();
        tracing::debug!(
            index.id = definition.index_id,
            tenant.id = tenant_id,
            bucket.id = bucket_id,
            cleanup.outcome = if failed { "failed" } else { "completed" },
            monotonic_counter.keldra_index_releasing_root_cleanups_total =
                if failed { 0 } else { completed_roots },
            monotonic_counter.keldra_index_releasing_root_cleanup_failures_total =
                u64::from(failed),
            monotonic_counter.keldra_index_releasing_root_cleanup_bytes_total =
                if failed { 0 } else { completed_bytes },
            histogram.keldra_index_releasing_roots_per_cleanup = completed_roots,
            histogram.keldra_index_releasing_bytes_per_cleanup = completed_bytes,
            "exact released index roots finished cleanup"
        );
        let outcome = outcome?;
        Ok(CommittedIndexView {
            pointer,
            current_object_version: outcome.version,
            manifest: current.manifest.clone(),
        })
    }

    pub(crate) fn metadata_retained(
        &self,
        current: &CommittedIndexView,
        now: SystemTime,
    ) -> Result<Vec<CommitManifestReference>, Status> {
        select_retained_metadata(
            self.config,
            current.pointer.current.retained_bytes,
            current.pointer.retained.iter(),
            now,
        )
    }

    pub(crate) async fn load_current(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<CommittedIndexView>, Status> {
        Ok(self
            .load_committed_view(
                &definition.tenant,
                &definition.bucket,
                tenant_id,
                bucket_id,
                definition.index_id,
                None,
            )
            .await?
            .map(|selected| CommittedIndexView {
                pointer: selected.pointer,
                current_object_version: selected.current_object_version,
                manifest: selected.manifest,
            }))
    }

    /// Re-proves every exact immutable object named by a candidate while the
    /// caller excludes retention and current-pointer replacement for this
    /// index. A successful return is the prerequisite for making the candidate
    /// manifest reachable from the mutable current pointer.
    pub(crate) async fn revalidate_candidate_while_current_mutation_held(
        &self,
        definition: &StoredIndexDefinition,
        tenant_id: u64,
        bucket_id: u64,
        segments: &[SegmentDescriptor],
        locator_roots: &[LocatorRoot],
        rooted: Option<&IndexCommitManifest>,
        manifest: Option<&CommitManifestReference>,
        cas_class: IndexPointerCasClass,
        guard: &IndexCurrentMutationGuard,
    ) -> Result<(), Status> {
        revalidation::revalidate_candidate(
            &self.reader,
            definition,
            tenant_id,
            bucket_id,
            segments,
            locator_roots,
            rooted,
            manifest,
            cas_class,
            guard,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_committed_view(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
        exact_revision: Option<u64>,
    ) -> Result<Option<SelectedCommittedIndexView>, Status> {
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
        let requested = exact_revision.unwrap_or(pointer.current.revision);
        if requested > pointer.current.revision {
            return Err(Status::failed_precondition(
                "requested index revision was never published",
            ));
        }
        let reference = pointer.revision(requested).cloned().ok_or_else(|| {
            Status::failed_precondition("requested index revision is no longer retained")
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
        Ok(Some(SelectedCommittedIndexView {
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
        reference: &CommitManifestReference,
    ) -> Result<IndexCommitManifest, Status> {
        reference.validate(index_id).map_err(commit_view_status)?;
        let key = ObjectKey::new(storage_tenant, bucket, &reference.path)
            .map_err(|error| Status::internal(error.to_string()))?;
        let Some(mut opened) = self
            .reader
            .open_stable(&key, tenant_id, bucket_id, Some(reference.object_version))
            .await?
        else {
            return Err(Status::data_loss(
                "format-v4 revision manifest object is absent",
            ));
        };
        if opened.version.id != reference.object_version
            || opened.version.deleted
            || opened.version.blob.as_ref() != Some(&reference.blob)
        {
            return Err(Status::data_loss(
                "format-v4 revision manifest differs from its exact reference",
            ));
        }
        let mut payload = opened
            .payload
            .take()
            .ok_or_else(|| Status::data_loss("format-v4 revision manifest has no payload"))?;
        let mut bytes = Vec::new();
        let maximum =
            reference.blob.length.checked_add(1).ok_or_else(|| {
                Status::resource_exhausted("format-v4 manifest length exceeds u64")
            })?;
        payload
            .by_ref()
            .take(maximum)
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read format-v4 manifest: {error}")))?;
        if bytes.len() as u64 != reference.blob.length {
            return Err(Status::data_loss(
                "format-v4 revision manifest length differs from its verified object reference",
            ));
        }
        let manifest = IndexCommitManifest::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        validate_manifest_reference(reference, &manifest, index_id)?;
        Ok(manifest)
    }
}

#[derive(Clone)]
pub(crate) struct IndexComponentBatchSink {
    store: Store,
    cohorts: IndexPublicationCohorts,
    definition: StoredIndexDefinition,
    tenant_id: u64,
    bucket_id: u64,
    admission: DerivedArtifactAdmission,
    cohort_class: PublicationCohortClass,
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
        let result = stage_index_bytes_with_retry(&self.store, &pack.bytes, self.admission).await;
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
        let span = tracing::debug_span!(
            "keldra.index.v4_component_publish",
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
            tracing::debug!(
                publish.outcome = if failed { "failed" } else { "completed" },
                monotonic_counter.keldra_index_v4_components_published_total =
                    if failed { 0 } else { component_count },
                monotonic_counter.keldra_index_v4_component_publish_failures_total =
                    u64::from(failed),
                monotonic_counter.keldra_index_v4_component_bytes_total =
                    if failed { 0 } else { encoded_bytes },
                monotonic_counter.keldra_index_v4_packs_published_total =
                    if failed { 0 } else { pack_count },
                histogram.keldra_index_v4_component_publish_duration_seconds =
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
        let prepared = PreparedPackPublication::new(
            &self.definition,
            self.tenant_id,
            self.bucket_id,
            self.admission,
            active,
        )?;
        let mut outcomes = vec![None; prepared.requests().len()];
        let mut pending = (0..prepared.requests().len()).collect::<Vec<_>>();
        while !pending.is_empty() {
            let requests = pending
                .iter()
                .map(|&index| prepared.requests()[index].clone())
                .collect();
            let published = match self
                .cohorts
                .publish_packs(requests, self.cohort_class)
                .await
            {
                Err(error) if retryable_pack_publish_status(&error) => {
                    tracing::debug!(%error, "retrying retained immutable index pack cohort");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(error) => return Err(IndexError::Io(error.to_string())),
                Ok(published) if published.len() == pending.len() => published,
                Ok(_) => {
                    return Err(IndexError::InvalidFormat(
                        "grouped pack outcome count differs from submitted pack count",
                    ));
                }
            };
            let mut retry = Vec::new();
            for (index, result) in pending.into_iter().zip(published) {
                match result {
                    Ok(outcome) => outcomes[index] = Some(outcome),
                    Err(error) if retryable_pack_publish_status(&error) => retry.push(index),
                    Err(error) => return Err(IndexError::Io(error.to_string())),
                }
            }
            pending = retry;
            if !pending.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        let outcomes =
            outcomes
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or(IndexError::InvalidFormat(
                    "grouped pack publication left an unresolved receipt",
                ))?;
        prepared.apply(&outcomes)
    }
}

fn retryable_pack_publish_status(error: &Status) -> bool {
    matches!(
        error.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
    )
}

fn commit_artifact_totals(
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

fn manifest_covers_barrier(manifest: &IndexCommitManifest, required: &IndexBarrier) -> bool {
    manifest.placement_fence == required.fence
        && required.sources.iter().all(|(node, cursor)| {
            manifest.sources.iter().any(|indexed| {
                indexed.node_id == node.0
                    && indexed.source == cursor.source
                    && indexed.next_offset >= cursor.next_offset
            })
        })
        && required.atomic.finalized_through().is_none_or(|required| {
            manifest
                .atomic_through
                .is_some_and(|indexed| indexed >= required)
        })
}

fn manifest_preserves_pending_atomic_batches(
    manifest: &IndexCommitManifest,
    required: &[PendingAtomicBatch],
) -> bool {
    required.iter().all(|pending| {
        manifest
            .atomic_through
            .is_some_and(|finalized| finalized >= pending.cursor)
            || manifest
                .pending_atomic_batches
                .binary_search_by_key(&pending.cursor, |candidate| candidate.cursor)
                .ok()
                .is_some_and(|position| {
                    manifest.pending_atomic_batches[position].bundle_hash == pending.bundle_hash
                })
    })
}

fn lost_pointer_response_resolved(
    candidate: &CommitManifestReference,
    observed: &CommitManifestReference,
) -> bool {
    candidate == observed
}

fn validate_manifest_reference(
    reference: &CommitManifestReference,
    manifest: &IndexCommitManifest,
    index_id: u64,
) -> Result<(), Status> {
    reference.validate(index_id).map_err(commit_view_status)?;
    if manifest.index_id != index_id
        || manifest.revision != reference.revision
        || manifest.definition_version != reference.definition_version
        || manifest.schema_fingerprint != reference.schema_fingerprint
    {
        return Err(Status::data_loss(
            "format-v4 manifest identity differs from its current-pointer reference",
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
    current_retained_bytes: u64,
    candidates: impl IntoIterator<Item = &'a CommitManifestReference>,
    now: SystemTime,
) -> Result<Vec<CommitManifestReference>, Status> {
    let now_millis = unix_millis(now)?;
    let maximum_age_millis = config
        .max_commit_revision_age_hours()
        .saturating_mul(60 * 60 * 1_000);
    let maximum_count = config.max_retained_commit_revisions() as usize;
    let maximum_bytes = config.max_retained_commit_bytes();
    // The current manifest occupies the first count and byte slot. Candidates
    // are a newest-to-oldest prefix; stopping at the first exceeded bound keeps
    // the current pointer canonical and makes retention a bounded metadata CAS.
    let mut retained_bytes = current_retained_bytes;
    let mut retained = Vec::new();
    for reference in candidates {
        let candidate_bytes = retained_bytes.saturating_add(reference.retained_bytes);
        // The new/current revision occupies the first retained-count slot.
        if retained.len().saturating_add(1) >= maximum_count
            || now_millis.saturating_sub(reference.published_at_unix_millis) > maximum_age_millis
            || candidate_bytes > maximum_bytes
        {
            break;
        }
        retained_bytes = candidate_bytes;
        retained.push(reference.clone());
    }
    Ok(retained)
}

fn publication_releasing_roots(
    observed: Option<&IndexCurrentPointer>,
    retained_count: usize,
    released_at: SystemTime,
) -> Result<Vec<ReleasingManifestReference>, Status> {
    let Some(observed) = observed else {
        return Ok(Vec::new());
    };
    let previous = std::iter::once(&observed.current)
        .chain(observed.retained.iter())
        .collect::<Vec<_>>();
    if retained_count > previous.len() {
        return Err(Status::internal(
            "publication retained more roots than the observed pointer contains",
        ));
    }
    let mut releasing = observed.releasing.clone();
    releasing.extend(
        previous[retained_count..]
            .iter()
            .map(|reference| ReleasingManifestReference::new((*reference).clone(), released_at))
            .collect::<Result<Vec<_>, _>>()
            .map_err(commit_view_status)?,
    );
    if releasing.len() > MAX_RELEASING_COMMIT_REVISIONS {
        return Err(Status::resource_exhausted(
            "releasing manifest root queue reached its durable pointer bound",
        ));
    }
    Ok(releasing)
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

async fn stage_index_bytes_with_retry(
    store: &Store,
    bytes: &[u8],
    admission: DerivedArtifactAdmission,
) -> Result<BlobRef, IndexError> {
    loop {
        let result = match admission {
            DerivedArtifactAdmission::Bounded => store.stage_blob(bytes).await,
            DerivedArtifactAdmission::PublicationProgress => {
                store.stage_derived_progress_blob(bytes).await
            }
        };
        match result {
            Ok(blob) => return Ok(blob),
            Err(error) if retryable_pack_stage_error(&error) => {
                tracing::debug!(%error, "retrying retained immutable index pack staging");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(IndexError::Io(error.to_string())),
        }
    }
}

fn retryable_pack_stage_error(error: &MutationError) -> bool {
    matches!(
        error,
        MutationError::SourceJournalCapacity
            | MutationError::DurabilityUnavailable
            | MutationError::ReceiptCapacity
    )
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

fn publish_command(
    index_id: u64,
    path: &str,
    blob: &BlobRef,
    expected_version: Option<VersionId>,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keldra.index.publish.v1");
    hasher.update(path.as_bytes());
    hasher.update(&blob.hash);
    hasher.update(&blob.length.to_be_bytes());
    match expected_version {
        None => {
            hasher.update(&[0]);
        }
        Some(version) => {
            hasher.update(&[1]);
            hasher.update(&version.0.to_be_bytes());
        }
    }
    let digest = hasher.finalize();
    format!("index-v4-{index_id}-{}", &digest.to_hex().as_str()[..24])
}

fn select_completed_releasing(
    current: &[ReleasingManifestReference],
    completed: &[ReleasingManifestReference],
) -> (
    Vec<ReleasingManifestReference>,
    Vec<ReleasingManifestReference>,
) {
    let completed = completed
        .iter()
        .filter(|candidate| current.contains(candidate))
        .cloned()
        .collect::<Vec<_>>();
    let remaining = current
        .iter()
        .filter(|candidate| !completed.contains(candidate))
        .cloned()
        .collect();
    (completed, remaining)
}

fn commit_view_status(error: super::committed_view::CommitViewError) -> Status {
    match error {
        super::committed_view::CommitViewError::SizeLimit => {
            Status::resource_exhausted(error.to_string())
        }
        _ => Status::data_loss(error.to_string()),
    }
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

    fn manifest_reference(revision: u64, published_at: u64) -> CommitManifestReference {
        let hash = [revision as u8; 32];
        CommitManifestReference {
            revision,
            definition_version: 1,
            schema_fingerprint: [1; 32],
            path: manifest_path(7, hash),
            blob: BlobRef { hash, length: 120 },
            object_version: VersionId(revision + 10),
            published_at_unix_millis: published_at,
            retained_bytes: 1_024,
        }
    }

    fn releasing(revision: u64) -> ReleasingManifestReference {
        ReleasingManifestReference::new(
            manifest_reference(revision, revision * 100),
            UNIX_EPOCH + Duration::from_secs(revision),
        )
        .unwrap()
    }

    #[test]
    fn publish_command_binds_the_exact_cas_precondition() {
        let blob = BlobRef {
            hash: [7; 32],
            length: 91,
        };
        let absent = publish_command(11, "_keldra/index", &blob, None);
        let version_3 = publish_command(11, "_keldra/index", &blob, Some(VersionId(3)));

        assert_eq!(absent, publish_command(11, "_keldra/index", &blob, None));
        assert_eq!(
            version_3,
            publish_command(11, "_keldra/index", &blob, Some(VersionId(3)))
        );
        assert_ne!(absent, version_3);
        assert_ne!(
            version_3,
            publish_command(11, "_keldra/index", &blob, Some(VersionId(4)))
        );
    }

    #[test]
    fn lost_pointer_response_requires_the_exact_manifest_reference() {
        let candidate = manifest_reference(5, 1_000);
        assert!(lost_pointer_response_resolved(&candidate, &candidate));

        let mut same_revision_different_object = candidate.clone();
        same_revision_different_object.object_version = VersionId(99);
        assert!(!lost_pointer_response_resolved(
            &candidate,
            &same_revision_different_object
        ));
    }

    #[test]
    fn releasing_completion_is_idempotent_and_preserves_newer_roots() {
        let old = releasing(1);
        let already_unlinked = releasing(2);
        let newer = releasing(3);
        let (completed, remaining) = select_completed_releasing(
            &[old.clone(), newer.clone()],
            &[old.clone(), already_unlinked],
        );
        assert_eq!(completed, vec![old]);
        assert_eq!(remaining, vec![newer]);

        let (completed, remaining) = select_completed_releasing(&remaining, &completed);
        assert!(completed.is_empty(), "crash replay is an idempotent no-op");
        assert_eq!(remaining, vec![releasing(3)]);
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
                    let component = keldra_index::v4::encode_component(
                        identity,
                        keldra_index::v4::ComponentKind::POSTINGS,
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
            1_024,
            candidates.iter(),
            UNIX_EPOCH + Duration::from_millis(now_millis),
        )
        .unwrap();
        assert_eq!(selected.len(), 2);

        let old = [manifest_reference(1, now_millis - 25 * hour)];
        assert!(
            select_retained_metadata(
                IndexRuntimeConfig::default(),
                1_024,
                old.iter(),
                UNIX_EPOCH + Duration::from_millis(now_millis),
            )
            .unwrap()
            .is_empty()
        );

        let mut too_large = manifest_reference(1, now_millis - hour);
        too_large.retained_bytes = IndexRuntimeConfig::DEFAULT_MAX_RETAINED_COMMIT_BYTES;
        assert!(
            select_retained_metadata(
                IndexRuntimeConfig::default(),
                1,
                [&too_large],
                UNIX_EPOCH + Duration::from_millis(now_millis),
            )
            .unwrap()
            .is_empty()
        );

        let existing_release = releasing(2);
        let pointer = IndexCurrentPointer::new(
            7,
            manifest_reference(5, now_millis - hour),
            vec![
                manifest_reference(4, now_millis - 2 * hour),
                manifest_reference(3, now_millis - 3 * hour),
            ],
            vec![existing_release.clone()],
        )
        .unwrap();
        let released = publication_releasing_roots(
            Some(&pointer),
            1,
            UNIX_EPOCH + Duration::from_millis(now_millis),
        )
        .unwrap();
        assert_eq!(released[0], existing_release);
        assert_eq!(
            released
                .iter()
                .skip(1)
                .map(|root| root.manifest.revision)
                .collect::<Vec<_>>(),
            vec![4, 3],
            "normal publication must durably root every omitted suffix",
        );
    }

    #[test]
    fn immutable_pack_retries_only_typed_transient_failures() {
        for error in [
            MutationError::SourceJournalCapacity,
            MutationError::DurabilityUnavailable,
            MutationError::ReceiptCapacity,
        ] {
            assert!(retryable_pack_stage_error(&error));
        }
        assert!(!retryable_pack_stage_error(&MutationError::Storage(
            "corrupt".into()
        )));
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Cancelled,
            tonic::Code::Unknown,
        ] {
            assert!(retryable_pack_publish_status(&Status::new(code, "retry")));
        }
        assert!(!retryable_pack_publish_status(&Status::permission_denied(
            "permanent"
        )));
        assert!(!retryable_pack_publish_status(&Status::aborted("fence")));
    }
}
