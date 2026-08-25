//! Bounded publication of one durable committed index view.

use super::*;

const MAX_CONCURRENT_INCREMENTAL_PUBLICATIONS: usize = 2;
const MAX_CONCURRENT_MAINTENANCE_PUBLICATIONS: usize = 1;

/// Independent node-wide FIFO bounds keep merge and rebuild I/O from occupying
/// the capacity required to make incremental journal checkpoints durable.
#[derive(Clone)]
pub(crate) struct IndexPublicationSlots {
    incremental: std::sync::Arc<tokio::sync::Semaphore>,
    maintenance: std::sync::Arc<tokio::sync::Semaphore>,
}

impl Default for IndexPublicationSlots {
    fn default() -> Self {
        Self::new(
            MAX_CONCURRENT_INCREMENTAL_PUBLICATIONS,
            MAX_CONCURRENT_MAINTENANCE_PUBLICATIONS,
        )
    }
}

impl IndexPublicationSlots {
    pub(super) fn new(incremental_limit: usize, maintenance_limit: usize) -> Self {
        assert!(
            incremental_limit > 0,
            "incremental publication concurrency must be positive"
        );
        assert!(
            maintenance_limit > 0,
            "maintenance publication concurrency must be positive"
        );
        Self {
            incremental: std::sync::Arc::new(tokio::sync::Semaphore::new(incremental_limit)),
            maintenance: std::sync::Arc::new(tokio::sync::Semaphore::new(maintenance_limit)),
        }
    }

    pub(super) async fn acquire_incremental(
        &self,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        self.incremental
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("incremental index publication admission is closed"))
    }

    pub(super) async fn acquire_maintenance(
        &self,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        self.maintenance
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("maintenance index publication admission is closed"))
    }
}

pub(super) struct AbortOnDropTask<T> {
    task: Option<tokio::task::JoinHandle<T>>,
}

impl<T> AbortOnDropTask<T> {
    pub(super) fn new(task: tokio::task::JoinHandle<T>) -> Self {
        Self { task: Some(task) }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    pub(super) async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        self.task
            .take()
            .expect("owned task remains installed")
            .await
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub(super) fn start_candidate_publication(
    definition: CatalogDefinition,
    kind: IndexKind,
    barrier: IndexBarrier,
    candidate: CandidateCommit,
    current: Option<CommittedIndexView>,
    admission: DerivedArtifactAdmission,
    dependencies: IndexBuilderDependencies,
) -> AbortOnDropTask<Result<CommittedIndexView, Status>> {
    AbortOnDropTask::new(tokio::spawn(async move {
        let cas_class = publication_cas_class(&definition, &barrier, &candidate, current.as_ref())?;
        let _publication_slot = match cas_class {
            super::super::publisher::IndexPointerCasClass::Incremental => {
                dependencies.publication_slots.acquire_incremental().await?
            }
            super::super::publisher::IndexPointerCasClass::Merge
            | super::super::publisher::IndexPointerCasClass::Rebuild
            | super::super::publisher::IndexPointerCasClass::Retention => {
                dependencies.publication_slots.acquire_maintenance().await?
            }
        };
        publish_candidate(
            &definition,
            kind,
            barrier,
            candidate,
            current.as_ref(),
            admission,
            &dependencies,
        )
        .await
    }))
}

pub(super) async fn publish_candidate(
    definition: &CatalogDefinition,
    kind: IndexKind,
    barrier: IndexBarrier,
    candidate: CandidateCommit,
    current: Option<&CommittedIndexView>,
    admission: DerivedArtifactAdmission,
    dependencies: &IndexBuilderDependencies,
) -> Result<CommittedIndexView, Status> {
    let started = Instant::now();
    let cas_class = publication_cas_class(definition, &barrier, &candidate, current)?;
    let expected_revision =
        current.map_or(1, |value| value.pointer.current.revision.saturating_add(1));
    let segment_count = candidate.segments.len() as u64;
    let accepted_objects = candidate.diagnostics.accepted_objects;
    let skipped_objects = candidate.diagnostics.skipped_objects;
    let new_segment_bytes = candidate
        .segments
        .iter()
        .map(|segment| segment.encoded_bytes)
        .chain(
            candidate
                .locator_roots
                .iter()
                .map(|root| root.encoded_bytes),
        )
        .sum::<u64>();
    let span = tracing::debug_span!(
        "keldra.index.publication",
        index.id = definition.stored.index_id,
        tenant.id = definition.tenant_id,
        bucket.id = definition.bucket_id,
        index.kind = ?kind,
        revision = expected_revision,
        publication.segments = segment_count,
        publication.new_segment_bytes = new_segment_bytes,
        publication.accepted_objects = accepted_objects,
        publication.skipped_objects = skipped_objects,
        publication.fence_term = barrier.fence.term,
        publication.fence_index = barrier.fence.index,
        publication.source_count = barrier.sources.len(),
        publication.cas_class = cas_class.as_str(),
        publication.elapsed_seconds = tracing::field::Empty,
        publication.outcome = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    );
    let result = async {
        dependencies
            .journal
            .validate_publication_barrier(&barrier)
            .await
            .map_err(event_status)?;
        dependencies
            .publisher
            .publish_manifest(
                &definition.stored,
                definition.tenant_id,
                definition.bucket_id,
                definition.object_version,
                definition.schema.kind,
                definition.schema_fingerprint,
                barrier,
                candidate.pending_atomic_batches,
                manifest_physical_order(&definition.schema),
                candidate.segments,
                candidate.locator_roots,
                current,
                admission,
                cas_class,
            )
            .await
    }
    .instrument(span.clone())
    .await;
    let elapsed_seconds = started.elapsed().as_secs_f64();
    span.record("publication.elapsed_seconds", elapsed_seconds);
    span.record(
        "publication.outcome",
        if result.is_err() {
            "failed"
        } else {
            "completed"
        },
    );
    span.record(
        "otel.status_code",
        if result.is_err() { "error" } else { "ok" },
    );
    let published = match result {
        Ok(value) => value,
        Err(error) => {
            span.in_scope(|| {
                tracing::debug!(
                    index.kind = ?kind,
                    publication.outcome = "failed",
                    monotonic_counter.keldra_index_publication_failures_total = 1_u64,
                    histogram.keldra_index_publication_duration_seconds = elapsed_seconds,
                    "index commit publication failed"
                );
            });
            return Err(error);
        }
    };
    dependencies
        .derived_progress
        .report(
            derived_identity(definition),
            DerivedBarrierEvidence::Published(
                published.manifest.barrier().map_err(commit_view_status)?,
            ),
        )
        .await;
    span.in_scope(|| {
        tracing::debug!(
            revision = published.pointer.current.revision,
            "index commit published"
        );
        tracing::debug!(
            index.kind = ?kind,
            publication.outcome = "completed",
            gauge.keldra_index_commit_revision = published.pointer.current.revision,
            gauge.keldra_index_publication_present = 1_u64,
            gauge.keldra_index_publication_age_seconds = 0_f64,
            gauge.keldra_index_publication_fresh = 1_u64,
            gauge.keldra_index_source_lag = 0_u64,
            publication.committed_artifact_bytes = published.manifest.artifact_encoded_bytes,
            monotonic_counter.keldra_index_commit_revision_accepted_objects_total = accepted_objects,
            monotonic_counter.keldra_index_commit_revision_skipped_objects_total = skipped_objects,
            monotonic_counter.keldra_index_publications_total = 1_u64,
            histogram.keldra_index_publication_duration_seconds = elapsed_seconds,
            "index commit publication metrics"
        );
    });
    Ok(published)
}

pub(super) fn publication_cas_class(
    definition: &CatalogDefinition,
    barrier: &IndexBarrier,
    candidate: &CandidateCommit,
    current: Option<&CommittedIndexView>,
) -> Result<super::super::publisher::IndexPointerCasClass, Status> {
    let Some(current) = current else {
        return Ok(super::super::publisher::IndexPointerCasClass::Rebuild);
    };
    if current.manifest.definition_version != definition.object_version {
        return Ok(super::super::publisher::IndexPointerCasClass::Rebuild);
    }
    let current_barrier = current.manifest.barrier().map_err(commit_view_status)?;
    if &current_barrier == barrier
        && (candidate.segments != current.manifest.segments
            || candidate.locator_roots != current.manifest.locator_roots)
    {
        return Ok(super::super::publisher::IndexPointerCasClass::Merge);
    }
    Ok(super::super::publisher::IndexPointerCasClass::Incremental)
}
