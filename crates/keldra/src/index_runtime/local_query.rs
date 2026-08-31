//! Local execution against one pinned immutable format-v4 revision.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use keldra_api::v1::{
    IndexAggregateOperation, IndexAggregateResult, IndexFacetResult, IndexFreshness, IndexQueryHit,
    IndexSourceFreshness, ObjectAddress,
};
use keldra_atomic_program::{
    MAX_OBJECT_BUCKET_BYTES, MAX_OBJECT_PATH_BYTES, MAX_OBJECT_TENANT_BYTES,
};
use keldra_index::IndexError;
use keldra_index::v4::{
    AggregateOperation, ArtifactDirectoryRead, ArtifactPackReference, CandidateGate,
    CandidateGateEvidence, CandidateReference, FieldId, IndexKind, NativeQueryCursor,
    NativeQueryExecutionError, NativeQueryExecutor, NativeQueryLimits, NativeQueryPhase,
    NativeQueryRequest, NativeQueryStatisticsRecorder,
};
use keldra_store::{BlobRef, CurrentObjectSnapshot, MAX_CONTENT_TYPE_BYTES, ObjectKey};
use tonic::Status;
use tracing::Instrument;

use crate::authorization::ObjectPermission;
use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{LocalIndexQueryExecutor, LocalIndexQueryRequest};
use crate::index_service::{
    CandidateVisibilityEvidence, ExecutedIndexQuery, IndexCandidateIdentity,
    IndexCandidateVisibility,
};

use super::cache::{
    IndexCache, IndexCacheError, IndexDiskLease, IndexSegmentFetcher, IndexSegmentId, IndexSlice,
};
use super::catalog::CatalogDefinition;
use super::committed_view::{CommitManifestReference, IndexCommitManifest, ManifestPhysicalOrder};
use super::cpu::IndexCpuPool;
use super::directory::{ManifestArtifactDirectory, ManifestArtifactFile};
use super::events::IndexBarrier;
use super::projection_mapper::SharedProjectionMapper;
use super::publisher::{IndexCommitPublisher, SelectedCommittedIndexView};
use super::query_budget::IndexQueryMemoryBudget;
use super::query_response::{facet_result_to_api, scalar_json};
use super::v4_query::compile_query;
use super::v4_schema::compile_schema;

#[path = "local_query/opened_views.rs"]
mod opened_views;
use opened_views::{
    CommittedViewOpenReason, OpenedCommittedIndexView, OpenedCommittedViewKey,
    OpenedCommittedViewRegistry, opened_pack_charge,
};

#[path = "local_query/candidate_gate.rs"]
mod candidate_gate;
use candidate_gate::RuntimeCandidateGate;
#[cfg(test)]
use candidate_gate::runtime_gate_envelope_bytes;

#[derive(Clone)]
struct QueryReadObserver {
    inner: Arc<QueryReadObserverInner>,
}

struct QueryReadObserverInner {
    reads: AtomicU64,
    bytes: AtomicU64,
    bytes_since_yield: AtomicU64,
    work_quantum_bytes: u64,
    cooperative_yields: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct QueryReadSnapshot {
    reads: u64,
    bytes: u64,
    partial_quantum_bytes: u64,
    cooperative_yields: u64,
}

impl QueryReadObserver {
    fn new(work_quantum_bytes: u64) -> Self {
        debug_assert!(work_quantum_bytes > 0);
        Self {
            inner: Arc::new(QueryReadObserverInner {
                reads: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
                bytes_since_yield: AtomicU64::new(0),
                work_quantum_bytes,
                cooperative_yields: AtomicU64::new(0),
            }),
        }
    }

    async fn record_read_and_yield(&self, bytes: usize) {
        self.inner.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        let accumulated = self
            .inner
            .bytes_since_yield
            .fetch_add(bytes as u64, Ordering::Relaxed)
            .saturating_add(bytes as u64);
        if accumulated >= self.inner.work_quantum_bytes {
            self.inner.bytes_since_yield.store(0, Ordering::Relaxed);
            tokio::task::yield_now().await;
            self.inner
                .cooperative_yields
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> QueryReadSnapshot {
        QueryReadSnapshot {
            reads: self.inner.reads.load(Ordering::Relaxed),
            bytes: self.inner.bytes.load(Ordering::Relaxed),
            partial_quantum_bytes: self.inner.bytes_since_yield.load(Ordering::Relaxed),
            cooperative_yields: self.inner.cooperative_yields.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
struct QueryObservedDirectory {
    inner: ManifestArtifactDirectory,
    observer: QueryReadObserver,
    cpu: IndexCpuPool,
    kind: IndexKind,
}

impl QueryObservedDirectory {
    fn new(
        inner: ManifestArtifactDirectory,
        observer: QueryReadObserver,
        cpu: IndexCpuPool,
        kind: IndexKind,
    ) -> Self {
        Self {
            inner,
            observer,
            cpu,
            kind,
        }
    }
}

impl ArtifactDirectoryRead for QueryObservedDirectory {
    type File = QueryObservedFile;

    fn query_parallelism(&self) -> usize {
        self.cpu.workers().max(1)
    }

    async fn open_artifact(&self, pack: &ArtifactPackReference) -> Result<Self::File, IndexError> {
        Ok(QueryObservedFile {
            inner: self.inner.open(pack).await?,
            observer: self.observer.clone(),
        })
    }

    async fn run_query_cpu<T, F>(&self, work: F) -> Result<T, IndexError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, IndexError> + Send + 'static,
    {
        self.cpu.query_chunk(self.kind, work).await
    }
}

struct QueryObservedFile {
    inner: ManifestArtifactFile,
    observer: QueryReadObserver,
}

impl keldra_index::IndexFileRead for QueryObservedFile {
    type Slice = IndexSlice;

    async fn read_at(&self, offset: u64, max_length: usize) -> Result<Self::Slice, IndexError> {
        let slice = keldra_index::IndexFileRead::read_at(&self.inner, offset, max_length).await?;
        self.observer
            .record_read_and_yield(slice.as_ref().len())
            .await;
        Ok(slice)
    }
}

struct QueryActiveGuard {
    kind: Option<IndexKind>,
    span: tracing::Span,
    observer: QueryReadObserver,
    statistics: NativeQueryStatisticsRecorder,
    started: std::time::Instant,
    finished: bool,
}

struct QueryWaitingGuard {
    kind: Option<IndexKind>,
    span: tracing::Span,
    started: std::time::Instant,
    waiting: bool,
}

impl QueryWaitingGuard {
    fn start(kind: Option<IndexKind>, span: &tracing::Span) -> Self {
        span.in_scope(|| {
            tracing::debug!(
                index.kind = ?kind,
                counter.keldra_index_query_waiting = 1_i64,
                "local index query is waiting for admission"
            );
        });
        Self {
            kind,
            span: span.clone(),
            started: std::time::Instant::now(),
            waiting: true,
        }
    }

    fn admitted(mut self) -> f64 {
        self.waiting = false;
        let waiting_seconds = self.started.elapsed().as_secs_f64();
        self.span.in_scope(|| {
            tracing::debug!(
                index.kind = ?self.kind,
                counter.keldra_index_query_waiting = -1_i64,
                histogram.keldra_index_query_wait_duration_seconds = waiting_seconds,
                "local index query admitted"
            );
        });
        waiting_seconds
    }
}

impl Drop for QueryWaitingGuard {
    fn drop(&mut self) {
        if self.waiting {
            self.span.in_scope(|| {
                tracing::debug!(
                    index.kind = ?self.kind,
                    counter.keldra_index_query_waiting = -1_i64,
                    monotonic_counter.keldra_index_query_admission_cancellations_total = 1_u64,
                    histogram.keldra_index_query_wait_duration_seconds =
                        self.started.elapsed().as_secs_f64(),
                    "local index query admission was cancelled"
                );
            });
        }
    }
}

impl QueryActiveGuard {
    fn start(
        kind: Option<IndexKind>,
        span: &tracing::Span,
        observer: QueryReadObserver,
        statistics: NativeQueryStatisticsRecorder,
    ) -> Self {
        span.in_scope(|| {
            tracing::debug!(
                index.kind = ?kind,
                counter.keldra_index_query_active = 1_i64,
                monotonic_counter.keldra_index_query_runs_total = 1_u64,
                "local index query admitted"
            );
        });
        Self {
            kind,
            span: span.clone(),
            observer,
            statistics,
            started: std::time::Instant::now(),
            finished: false,
        }
    }

    fn finish(&mut self, result: &Result<ExecutedIndexQuery, Status>) {
        self.emit_terminal(
            if result.is_err() {
                "failed"
            } else {
                "completed"
            },
            result.is_err(),
            false,
        );
        self.finished = true;
    }

    fn emit_terminal(&self, outcome: &'static str, failed: bool, cancelled: bool) {
        let snapshot = self.observer.snapshot();
        let execution = self.statistics.snapshot();
        let tier = execution.tier.as_str();
        let elapsed = self.started.elapsed().as_secs_f64();
        self.span.record("query.read_ops", snapshot.reads);
        self.span.record("query.read_bytes", snapshot.bytes);
        self.span
            .record("query.cooperative_yields", snapshot.cooperative_yields);
        self.span.record("query.tier", tier);
        self.span
            .record("query.planner_conjunctions", execution.planner_conjunctions);
        self.span.record(
            "query.planner_reordered_conjunctions",
            execution.planner_reordered_conjunctions,
        );
        self.span.record(
            "query.planner_costed_children",
            execution.planner_costed_children,
        );
        self.span.record(
            "query.planner_child_cost_total",
            execution.planner_child_cost_total,
        );
        self.span.record(
            "query.planner_lead_cost_min",
            execution.planner_lead_cost_min,
        );
        self.span.record(
            "query.planner_lead_cost_max",
            execution.planner_lead_cost_max,
        );
        self.span.record("query.term_seeks", execution.term_seeks);
        self.span
            .record("query.enumerated_terms", execution.enumerated_terms);
        self.span.record(
            "query.posting_blocks_sought",
            execution.posting_blocks_sought,
        );
        self.span.record(
            "query.posting_blocks_decoded",
            execution.posting_blocks_decoded,
        );
        self.span.record(
            "query.posting_blocks_skipped",
            execution.posting_blocks_skipped,
        );
        self.span
            .record("query.posting_bytes_read", execution.posting_bytes_read);
        self.span.record(
            "query.posting_advance_calls",
            execution.posting_advance_calls,
        );
        self.span
            .record("query.conjunction_advances", execution.conjunction_advances);
        self.span
            .record("query.union_heap_pushes", execution.union_heap_pushes);
        self.span
            .record("query.union_heap_pops", execution.union_heap_pops);
        self.span.record(
            "query.two_phase_verifications",
            execution.two_phase_verifications,
        );
        self.span
            .record("query.candidate_doc_ids", execution.candidate_doc_ids);
        self.span.record(
            "query.live_mask_blocks_decoded",
            execution.live_mask_blocks_decoded,
        );
        self.span
            .record("query.live_mask_rejects", execution.live_mask_rejects);
        self.span
            .record("query.point_blocks_decoded", execution.point_blocks_decoded);
        self.span.record(
            "query.doc_value_blocks_decoded",
            execution.doc_value_blocks_decoded,
        );
        self.span.record(
            "query.facet_documents_processed",
            execution.facet_documents_processed,
        );
        self.span.record(
            "query.facet_values_processed",
            execution.facet_values_processed,
        );
        self.span.record(
            "query.aggregate_documents_processed",
            execution.aggregate_documents_processed,
        );
        self.span.record(
            "query.aggregate_values_processed",
            execution.aggregate_values_processed,
        );
        self.span
            .record("query.cursor_seeks", execution.cursor_seeks);
        self.span.record(
            "query.cursor_skipped_doc_ids",
            execution.cursor_skipped_doc_ids,
        );
        self.span.record(
            "query.physical_early_terminations",
            execution.physical_early_terminations,
        );
        self.span
            .record("query.top_k_inspected", execution.top_k_inspected);
        self.span.record(
            "query.candidate_gate_checked",
            execution.candidate_gate_checked,
        );
        self.span.record(
            "query.candidate_gate_batches",
            execution.candidate_gate_batches,
        );
        self.span.record(
            "query.candidate_gate_denied",
            execution.candidate_gate_denied,
        );
        self.span
            .record("query.candidate_gate_stale", execution.candidate_gate_stale);
        self.span.record(
            "query.candidate_gate_refills",
            execution.candidate_gate_refills,
        );
        self.span
            .record("query.returned_hits", execution.returned_hits);
        self.span.record(
            "query.memory_desired_bytes",
            execution.query_memory_desired_bytes,
        );
        self.span.record(
            "query.memory_granted_bytes",
            execution.query_memory_granted_bytes,
        );
        self.span.record(
            "query.resident_segment_slots",
            execution.resident_segment_slots,
        );
        self.span.record(
            "query.resident_segments_current",
            execution.resident_segments_current,
        );
        self.span.record(
            "query.resident_segments_peak",
            execution.resident_segments_peak,
        );
        self.span.record(
            "query.retained_decoded_charged_bytes",
            execution.retained_decoded_charged_bytes,
        );
        self.span.record(
            "query.retained_decoded_charged_peak_bytes",
            execution.retained_decoded_charged_peak_bytes,
        );
        self.span.record(
            "query.decoded_state_evictions",
            execution.decoded_state_evictions,
        );
        self.span.record(
            "query.decoded_state_reloads",
            execution.decoded_state_reloads,
        );
        self.span.record(
            "query.phase_plan_seconds",
            duration_seconds(execution.plan_duration_nanos),
        );
        self.span.record(
            "query.phase_continuation_seek_seconds",
            duration_seconds(execution.continuation_seek_duration_nanos),
        );
        self.span.record(
            "query.phase_head_initialization_seconds",
            duration_seconds(execution.head_initialization_duration_nanos),
        );
        self.span.record(
            "query.phase_physical_merge_advance_seconds",
            duration_seconds(execution.physical_merge_advance_duration_nanos),
        );
        self.span.record(
            "query.phase_candidate_visibility_seconds",
            duration_seconds(execution.candidate_visibility_duration_nanos),
        );
        self.span.record(
            "query.phase_response_materialization_seconds",
            duration_seconds(execution.response_materialization_duration_nanos),
        );
        self.span.record("query.elapsed_seconds", elapsed);
        self.span.record("query.outcome", outcome);
        self.span
            .record("otel.status_code", if failed { "error" } else { "ok" });
        let observed_quanta = snapshot
            .cooperative_yields
            .saturating_add(u64::from(snapshot.partial_quantum_bytes != 0));
        self.span.in_scope(|| {
            tracing::debug!(
                index.kind = ?self.kind,
                index.phase = "execute",
                index.tier = tier,
                monotonic_counter.keldra_index_query_read_ops_total = snapshot.reads,
                monotonic_counter.keldra_index_query_read_bytes_total = snapshot.bytes,
                monotonic_counter.keldra_index_query_cooperative_yields_total =
                    snapshot.cooperative_yields,
                monotonic_counter.keldra_index_query_failures_total = u64::from(failed),
                monotonic_counter.keldra_index_query_cancellations_total = u64::from(cancelled),
                monotonic_counter.keldra_index_query_planner_conjunctions_total =
                    execution.planner_conjunctions,
                monotonic_counter.keldra_index_query_planner_reordered_conjunctions_total =
                    execution.planner_reordered_conjunctions,
                monotonic_counter.keldra_index_query_planner_costed_children_total =
                    execution.planner_costed_children,
                monotonic_counter.keldra_index_query_planner_child_cost_total =
                    execution.planner_child_cost_total,
                monotonic_counter.keldra_index_query_term_seeks_total = execution.term_seeks,
                monotonic_counter.keldra_index_query_enumerated_terms_total =
                    execution.enumerated_terms,
                monotonic_counter.keldra_index_query_posting_blocks_decoded_total =
                    execution.posting_blocks_decoded,
                monotonic_counter.keldra_index_query_posting_blocks_sought_total =
                    execution.posting_blocks_sought,
                monotonic_counter.keldra_index_query_posting_blocks_skipped_total =
                    execution.posting_blocks_skipped,
                monotonic_counter.keldra_index_query_posting_bytes_read_total =
                    execution.posting_bytes_read,
                monotonic_counter.keldra_index_query_posting_advance_calls_total =
                    execution.posting_advance_calls,
                monotonic_counter.keldra_index_query_conjunction_advances_total =
                    execution.conjunction_advances,
                monotonic_counter.keldra_index_query_union_heap_pushes_total =
                    execution.union_heap_pushes,
                monotonic_counter.keldra_index_query_union_heap_pops_total =
                    execution.union_heap_pops,
                monotonic_counter.keldra_index_query_two_phase_verifications_total =
                    execution.two_phase_verifications,
                monotonic_counter.keldra_index_query_candidate_doc_ids_total =
                    execution.candidate_doc_ids,
                monotonic_counter.keldra_index_query_live_mask_blocks_decoded_total =
                    execution.live_mask_blocks_decoded,
                monotonic_counter.keldra_index_query_live_mask_rejects_total =
                    execution.live_mask_rejects,
                monotonic_counter.keldra_index_query_point_blocks_decoded_total =
                    execution.point_blocks_decoded,
                monotonic_counter.keldra_index_query_doc_value_blocks_decoded_total =
                    execution.doc_value_blocks_decoded,
                monotonic_counter.keldra_index_query_facet_documents_processed_total =
                    execution.facet_documents_processed,
                monotonic_counter.keldra_index_query_facet_values_processed_total =
                    execution.facet_values_processed,
                monotonic_counter.keldra_index_query_aggregate_documents_processed_total =
                    execution.aggregate_documents_processed,
                monotonic_counter.keldra_index_query_aggregate_values_processed_total =
                    execution.aggregate_values_processed,
                monotonic_counter.keldra_index_query_cursor_seeks_total = execution.cursor_seeks,
                monotonic_counter.keldra_index_query_cursor_skipped_doc_ids_total =
                    execution.cursor_skipped_doc_ids,
                monotonic_counter.keldra_index_query_physical_early_terminations_total =
                    execution.physical_early_terminations,
                monotonic_counter.keldra_index_query_top_k_inspected_total =
                    execution.top_k_inspected,
                monotonic_counter.keldra_index_query_candidate_gate_checked_total =
                    execution.candidate_gate_checked,
                monotonic_counter.keldra_index_query_candidate_gate_batches_total =
                    execution.candidate_gate_batches,
                monotonic_counter.keldra_index_query_candidate_gate_denied_total =
                    execution.candidate_gate_denied,
                monotonic_counter.keldra_index_query_candidate_gate_stale_total =
                    execution.candidate_gate_stale,
                monotonic_counter.keldra_index_query_candidate_gate_refills_total =
                    execution.candidate_gate_refills,
                monotonic_counter.keldra_index_query_decoded_state_evictions_total =
                    execution.decoded_state_evictions,
                monotonic_counter.keldra_index_query_decoded_state_reloads_total =
                    execution.decoded_state_reloads,
                histogram.keldra_index_query_duration_seconds = elapsed,
                histogram.keldra_index_query_returned_hits = execution.returned_hits,
                histogram.keldra_index_query_memory_desired_bytes =
                    execution.query_memory_desired_bytes,
                histogram.keldra_index_query_memory_granted_bytes =
                    execution.query_memory_granted_bytes,
                histogram.keldra_index_query_resident_segment_slots =
                    execution.resident_segment_slots,
                histogram.keldra_index_query_resident_segments_peak =
                    execution.resident_segments_peak,
                histogram.keldra_index_query_retained_decoded_charged_peak_bytes =
                    execution.retained_decoded_charged_peak_bytes,
                histogram.keldra_index_query_planner_lead_cost_min =
                    execution.planner_lead_cost_min,
                histogram.keldra_index_query_planner_lead_cost_max =
                    execution.planner_lead_cost_max,
                histogram.keldra_index_query_read_quantum_bytes =
                    snapshot.bytes as f64 / observed_quanta.max(1) as f64,
                "local index query reached a terminal outcome"
            );
        });
        record_query_phase(
            self.kind,
            NativeQueryPhase::Plan,
            execution.plan_duration_nanos,
        );
        record_query_phase(
            self.kind,
            NativeQueryPhase::ContinuationSeek,
            execution.continuation_seek_duration_nanos,
        );
        record_query_phase(
            self.kind,
            NativeQueryPhase::HeadInitialization,
            execution.head_initialization_duration_nanos,
        );
        record_query_phase(
            self.kind,
            NativeQueryPhase::PhysicalMergeAdvance,
            execution.physical_merge_advance_duration_nanos,
        );
        record_query_phase(
            self.kind,
            NativeQueryPhase::CandidateVisibility,
            execution.candidate_visibility_duration_nanos,
        );
        record_query_phase(
            self.kind,
            NativeQueryPhase::ResponseMaterialization,
            execution.response_materialization_duration_nanos,
        );
    }
}

fn duration_seconds(nanos: u64) -> f64 {
    Duration::from_nanos(nanos).as_secs_f64()
}

fn record_query_phase(kind: Option<IndexKind>, phase: NativeQueryPhase, nanos: u64) {
    if nanos == 0 {
        return;
    }
    tracing::debug!(
        index.kind = ?kind,
        index.phase = phase.as_str(),
        histogram.keldra_index_query_phase_duration_seconds = duration_seconds(nanos),
        "local index query phase completed"
    );
}

impl Drop for QueryActiveGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.emit_terminal("cancelled", true, true);
        }
        self.span.in_scope(|| {
            tracing::debug!(
                index.kind = ?self.kind,
                counter.keldra_index_query_active = -1_i64,
                "local index query released"
            );
        });
    }
}

#[derive(Clone)]
pub(crate) struct LocalRevisionQueryExecutor {
    reader: ClusterObjectReader,
    cache: IndexCache,
    publisher: IndexCommitPublisher,
    projection_mapper: SharedProjectionMapper,
    opened_views: OpenedCommittedViewRegistry,
    cpu: IndexCpuPool,
    query_budget: IndexQueryMemoryBudget,
    admission: Arc<tokio::sync::Semaphore>,
    work_quantum_bytes: u64,
}

impl LocalRevisionQueryExecutor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        reader: ClusterObjectReader,
        cache: IndexCache,
        publisher: IndexCommitPublisher,
        projection_mapper: SharedProjectionMapper,
        cpu: IndexCpuPool,
        query_budget: IndexQueryMemoryBudget,
        max_concurrency: u32,
        work_quantum_bytes: u64,
    ) -> Self {
        debug_assert!(work_quantum_bytes > 0);
        let disk_budget_bytes = cache.disk_budget_bytes();
        Self {
            reader,
            cache,
            publisher,
            projection_mapper,
            opened_views: OpenedCommittedViewRegistry::new(
                (max_concurrency as usize).saturating_mul(64).max(1_024),
                disk_budget_bytes,
            ),
            cpu,
            query_budget,
            admission: Arc::new(tokio::sync::Semaphore::new(max_concurrency as usize)),
            work_quantum_bytes,
        }
    }

    async fn select_committed_view(
        &self,
        request: &LocalIndexQueryRequest,
        exact_revision: Option<u64>,
    ) -> Result<Option<OpenedCommittedIndexView>, Status> {
        let key = OpenedCommittedViewKey {
            storage_tenant: request.storage_tenant.clone(),
            bucket: request.definition.bucket.clone(),
            tenant_id: request.tenant_id,
            bucket_id: request.bucket_id,
            index_id: request.definition.index_id,
            definition_version: request.definition.version,
        };
        if let Some(revision) = exact_revision {
            if let Some(Some(opened)) = self.opened_views.get(&key)
                && opened.selected.manifest.revision == revision
            {
                return Ok(Some(opened));
            }
            let selected = self
                .publisher
                .load_committed_view(
                    &key.storage_tenant,
                    &key.bucket,
                    key.tenant_id,
                    key.bucket_id,
                    key.index_id,
                    Some(revision),
                )
                .await?;
            return self
                .open_selected_view(request, selected, CommittedViewOpenReason::ExactRevision)
                .await;
        }

        if request.required_freshness.is_some() {
            return self.select_fresh_committed_view(request, key).await;
        }

        if let Some(opened) = self.opened_views.get(&key) {
            self.refresh_committed_view(key, Duration::from_secs(1));
            return Ok(opened);
        }

        let selected = self
            .publisher
            .load_committed_view(
                &key.storage_tenant,
                &key.bucket,
                key.tenant_id,
                key.bucket_id,
                key.index_id,
                None,
            )
            .await?;
        let opened = self
            .open_selected_view(request, selected, CommittedViewOpenReason::Initial)
            .await?;
        self.opened_views.install(key, opened.clone());
        Ok(opened)
    }

    async fn select_fresh_committed_view(
        &self,
        request: &LocalIndexQueryRequest,
        key: OpenedCommittedViewKey,
    ) -> Result<Option<OpenedCommittedIndexView>, Status> {
        let wait_started = std::time::Instant::now();
        let requirement = request
            .required_freshness
            .as_ref()
            .expect("freshness selection requires a checkpoint");
        loop {
            if let Some(Some(opened)) = self.opened_views.get(&key)
                && opened.selected.manifest.definition_version == request.definition.version
                && committed_view_covers(&opened, requirement)
            {
                emit_freshness_wait(&key, wait_started, "completed", false);
                return Ok(Some(opened));
            }
            if tokio::time::Instant::now() >= request.deadline {
                emit_freshness_wait(&key, wait_started, "deadline_exceeded", true);
                return Err(Status::deadline_exceeded(
                    "no committed local index view reached the required freshness checkpoint",
                ));
            }

            let changed = self.opened_views.changed();
            if self.opened_views.get(&key).is_none() {
                let selected = self
                    .publisher
                    .load_committed_view(
                        &key.storage_tenant,
                        &key.bucket,
                        key.tenant_id,
                        key.bucket_id,
                        key.index_id,
                        None,
                    )
                    .await?;
                let opened = self
                    .open_selected_view(request, selected, CommittedViewOpenReason::Freshness)
                    .await?;
                self.opened_views.install(key.clone(), opened);
            } else {
                self.refresh_committed_view(key.clone(), Duration::from_millis(25));
                tokio::select! {
                    _ = changed => {}
                    _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                    _ = tokio::time::sleep_until(request.deadline) => {
                        emit_freshness_wait(&key, wait_started, "deadline_exceeded", true);
                        return Err(Status::deadline_exceeded(
                            "no committed local index view reached the required freshness checkpoint",
                        ));
                    }
                }
            }
        }
    }

    async fn open_selected_view(
        &self,
        request: &LocalIndexQueryRequest,
        selected: Option<SelectedCommittedIndexView>,
        reason: CommittedViewOpenReason,
    ) -> Result<Option<OpenedCommittedIndexView>, Status> {
        self.open_selected_view_for_key(
            &OpenedCommittedViewKey {
                storage_tenant: request.storage_tenant.clone(),
                bucket: request.definition.bucket.clone(),
                tenant_id: request.tenant_id,
                bucket_id: request.bucket_id,
                index_id: request.definition.index_id,
                definition_version: request.definition.version,
            },
            selected,
            reason,
        )
        .await
    }

    fn refresh_committed_view(&self, key: OpenedCommittedViewKey, minimum_interval: Duration) {
        if !self.opened_views.begin_refresh(&key, minimum_interval) {
            return;
        }
        let publisher = self.publisher.clone();
        let executor = self.clone();
        let opened_views = self.opened_views.clone();
        tokio::spawn(async move {
            let result = publisher
                .load_committed_view(
                    &key.storage_tenant,
                    &key.bucket,
                    key.tenant_id,
                    key.bucket_id,
                    key.index_id,
                    None,
                )
                .await;
            match result {
                Ok(selected) => match executor
                    .open_selected_view_for_key(&key, selected, CommittedViewOpenReason::Background)
                    .await
                {
                    Ok(opened) => opened_views.finish_refresh(key, Some(opened)),
                    Err(error) => {
                        opened_views.finish_refresh(key.clone(), None);
                        tracing::debug!(index.id = key.index_id, %error, "background index committed-view open will retry");
                    }
                },
                Err(error) => {
                    opened_views.finish_refresh(key.clone(), None);
                    tracing::debug!(
                        index.id = key.index_id,
                        %error,
                        "background index committed-view reopen will retry"
                    );
                }
            }
        });
    }

    async fn open_selected_view_for_key(
        &self,
        key: &OpenedCommittedViewKey,
        selected: Option<SelectedCommittedIndexView>,
        reason: CommittedViewOpenReason,
    ) -> Result<Option<OpenedCommittedIndexView>, Status> {
        let started = std::time::Instant::now();
        let revision = selected
            .as_ref()
            .map_or(0, |selected| selected.manifest.revision);
        let result = self.materialize_selected_view_for_key(key, selected).await;
        let (pack_count, pack_bytes) = result
            .as_ref()
            .ok()
            .and_then(Option::as_ref)
            .map_or((0, 0), |opened| {
                committed_view_pack_totals(&opened.selected.manifest)
            });
        let failed = result.is_err();
        tracing::debug!(
            index.id = key.index_id,
            tenant.id = key.tenant_id,
            bucket.id = key.bucket_id,
            revision,
            reader.open_reason = reason.as_str(),
            reader.outcome = if failed { "failed" } else { "completed" },
            monotonic_counter.keldra_index_local_reader_reopens_total = u64::from(!failed),
            monotonic_counter.keldra_index_local_reader_reopen_failures_total = u64::from(failed),
            histogram.keldra_index_local_reader_materialized_packs = pack_count,
            histogram.keldra_index_local_reader_materialized_bytes = pack_bytes,
            histogram.keldra_index_local_reader_reopen_duration_seconds =
                started.elapsed().as_secs_f64(),
            "local committed index view open finished"
        );
        result
    }

    async fn materialize_selected_view_for_key(
        &self,
        key: &OpenedCommittedViewKey,
        selected: Option<SelectedCommittedIndexView>,
    ) -> Result<Option<OpenedCommittedIndexView>, Status> {
        let Some(selected) = selected else {
            return Ok(None);
        };
        let directory = ManifestArtifactDirectory::new(
            self.cache.clone(),
            self.reader.clone(),
            key.storage_tenant.clone(),
            key.bucket.clone(),
            key.tenant_id,
            key.bucket_id,
            key.index_id,
        )
        .map_err(index_status)?;
        let mut unique = BTreeMap::new();
        for pack in selected
            .manifest
            .segments
            .iter()
            .flat_map(|segment| &segment.packs)
        {
            unique
                .entry((pack.object_content_hash, pack.object_length))
                .or_insert(pack);
        }
        for locator in &selected.manifest.locator_roots {
            if let super::committed_view::LocatorPackOwnership::Standalone(packs) =
                &locator.pack_ownership
            {
                for pack in packs {
                    unique
                        .entry((pack.object_content_hash, pack.object_length))
                        .or_insert(pack);
                }
            }
        }
        let total_bytes = opened_pack_charge(unique.values().map(|pack| pack.object_length));
        if total_bytes > self.cache.disk_budget_bytes() {
            return Err(Status::resource_exhausted(
                "committed index view exceeds the local disk cache data and metadata budget",
            ));
        }
        let mut disk_leases = Vec::with_capacity(unique.len());
        for pack in unique.into_values() {
            // Resolve and exact-version verify the durable object before the
            // local immutable bytes may be selected by a reader.
            drop(directory.open(pack).await.map_err(index_status)?);
            let id = IndexSegmentId::new(pack.object_content_hash, pack.object_length)
                .map_err(cache_status)?;
            disk_leases.push(self.cache.lease_disk(id).await.map_err(cache_status)?);
        }
        Ok(Some(OpenedCommittedIndexView {
            selected,
            directory,
            disk_leases,
        }))
    }

    async fn execute(&self, request: LocalIndexQueryRequest) -> Result<ExecutedIndexQuery, Status> {
        let kind = request
            .definition
            .specification
            .as_ref()
            .and_then(|specification| {
                compile_schema(
                    &request.definition.path_prefix,
                    (!request.definition.content_type.is_empty())
                        .then_some(request.definition.content_type.as_str()),
                    specification,
                )
                .ok()
                .map(|schema| schema.kind)
            });
        let span = tracing::debug_span!(
            "keldra.index.query",
            index.id = request.definition.index_id,
            definition.version = request.definition.version,
            revision = tracing::field::Empty,
            tenant.id = request.tenant_id,
            bucket.id = request.bucket_id,
            index.kind = ?kind,
            query.work_quantum_bytes = self.work_quantum_bytes,
            query.admission_wait_seconds = tracing::field::Empty,
            query.read_ops = tracing::field::Empty,
            query.read_bytes = tracing::field::Empty,
            query.cooperative_yields = tracing::field::Empty,
            query.tier = tracing::field::Empty,
            query.plan_cost_model = "posting_document_frequency",
            query.planner_conjunctions = tracing::field::Empty,
            query.planner_reordered_conjunctions = tracing::field::Empty,
            query.planner_costed_children = tracing::field::Empty,
            query.planner_child_cost_total = tracing::field::Empty,
            query.planner_lead_cost_min = tracing::field::Empty,
            query.planner_lead_cost_max = tracing::field::Empty,
            query.term_seeks = tracing::field::Empty,
            query.enumerated_terms = tracing::field::Empty,
            query.posting_blocks_sought = tracing::field::Empty,
            query.posting_blocks_decoded = tracing::field::Empty,
            query.posting_blocks_skipped = tracing::field::Empty,
            query.posting_bytes_read = tracing::field::Empty,
            query.posting_advance_calls = tracing::field::Empty,
            query.conjunction_advances = tracing::field::Empty,
            query.union_heap_pushes = tracing::field::Empty,
            query.union_heap_pops = tracing::field::Empty,
            query.two_phase_verifications = tracing::field::Empty,
            query.candidate_doc_ids = tracing::field::Empty,
            query.live_mask_blocks_decoded = tracing::field::Empty,
            query.live_mask_rejects = tracing::field::Empty,
            query.point_blocks_decoded = tracing::field::Empty,
            query.doc_value_blocks_decoded = tracing::field::Empty,
            query.facet_documents_processed = tracing::field::Empty,
            query.facet_values_processed = tracing::field::Empty,
            query.aggregate_documents_processed = tracing::field::Empty,
            query.aggregate_values_processed = tracing::field::Empty,
            query.facet_computations_requested = tracing::field::Empty,
            query.aggregate_computations_requested = tracing::field::Empty,
            query.facet_computation_results = tracing::field::Empty,
            query.aggregate_computation_results = tracing::field::Empty,
            query.cursor_seeks = tracing::field::Empty,
            query.cursor_skipped_doc_ids = tracing::field::Empty,
            query.physical_early_terminations = tracing::field::Empty,
            query.top_k_inspected = tracing::field::Empty,
            query.candidate_gate_checked = tracing::field::Empty,
            query.candidate_gate_batches = tracing::field::Empty,
            query.candidate_gate_denied = tracing::field::Empty,
            query.candidate_gate_stale = tracing::field::Empty,
            query.candidate_gate_refills = tracing::field::Empty,
            query.returned_hits = tracing::field::Empty,
            query.memory_desired_bytes = tracing::field::Empty,
            query.memory_granted_bytes = tracing::field::Empty,
            query.resident_segment_slots = tracing::field::Empty,
            query.resident_segments_current = tracing::field::Empty,
            query.resident_segments_peak = tracing::field::Empty,
            query.retained_decoded_charged_bytes = tracing::field::Empty,
            query.retained_decoded_charged_peak_bytes = tracing::field::Empty,
            query.decoded_state_evictions = tracing::field::Empty,
            query.decoded_state_reloads = tracing::field::Empty,
            query.phase_plan_seconds = tracing::field::Empty,
            query.phase_continuation_seek_seconds = tracing::field::Empty,
            query.phase_head_initialization_seconds = tracing::field::Empty,
            query.phase_physical_merge_advance_seconds = tracing::field::Empty,
            query.phase_candidate_visibility_seconds = tracing::field::Empty,
            query.phase_response_materialization_seconds = tracing::field::Empty,
            query.elapsed_seconds = tracing::field::Empty,
            query.outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let waiting = QueryWaitingGuard::start(kind, &span);
        let permit = self
            .admission
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("index query admission is closed"))?;
        span.record("query.admission_wait_seconds", waiting.admitted());
        let observer = QueryReadObserver::new(self.work_quantum_bytes);
        let statistics = NativeQueryStatisticsRecorder::new();
        let mut active = QueryActiveGuard::start(kind, &span, observer.clone(), statistics.clone());
        let result = self
            .execute_inner(request, observer, statistics)
            .instrument(span.clone())
            .await;
        active.finish(&result);
        drop(permit);
        result
    }

    async fn execute_inner(
        &self,
        request: LocalIndexQueryRequest,
        observer: QueryReadObserver,
        statistics: NativeQueryStatisticsRecorder,
    ) -> Result<ExecutedIndexQuery, Status> {
        if request.authorization_revision == 0 {
            return Err(Status::data_loss(
                "local index query has no Zanzibar authorization revision",
            ));
        }
        let specification = request
            .definition
            .specification
            .as_ref()
            .ok_or_else(|| Status::data_loss("index definition has no specification"))?;
        let logical_schema = compile_schema(
            &request.definition.path_prefix,
            (!request.definition.content_type.is_empty())
                .then_some(request.definition.content_type.as_str()),
            specification,
        )
        .map_err(index_status)?;
        let projection_family = if logical_schema.kind == IndexKind::TypedJson {
            let family = CatalogDefinition::family_identity_for_schema(
                request.tenant_id,
                request.bucket_id,
                &logical_schema,
            )?;
            let source_scope = logical_schema
                .recipe_fingerprints()
                .map_err(index_status)?
                .membership;
            Some((family, source_scope))
        } else {
            None
        };
        let schema = if let Some((family, _)) = projection_family {
            self.projection_mapper
                .family_query_schema(family, &logical_schema)?
                .ok_or_else(|| {
                    Status::unavailable("projection family is not active on its builder")
                })?
        } else {
            logical_schema
        };
        let compiled =
            compile_query(&schema, specification, &request.query).map_err(index_status)?;
        record_computation_requests(
            schema.kind,
            compiled.facets.len(),
            compiled.aggregates.len(),
        )?;
        let after = request
            .resume
            .as_ref()
            .map(|resume| NativeQueryCursor::decode(&resume.last_position).map_err(index_status))
            .transpose()?;
        let field_names = schema
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        let requested_revision = request.resume.as_ref().map(|resume| resume.commit_revision);
        let selected = self
            .select_committed_view(&request, requested_revision)
            .await?;
        // Ordinary queries never inspect source journals. The pinned manifest
        // is complete freshness authority for this request; optional observed
        // tails are omitted until a background bucket-scoped monitor supplies
        // them without adding query admission work.
        let observed = None;
        let Some(selected) = selected else {
            let (facet_results, aggregate_results) =
                empty_computation_results(&field_names, &compiled.facets, &compiled.aggregates)?;
            record_computation_results(schema.kind, facet_results.len(), aggregate_results.len())?;
            return Ok(ExecutedIndexQuery {
                hits: Vec::new(),
                facet_results,
                aggregate_results,
                freshness: empty_freshness(
                    request.definition.index_id,
                    request.definition.version,
                    request.authorization_revision,
                    observed.as_ref(),
                ),
                next_position: None,
            });
        };
        let directory = selected.directory.clone();
        let loaded = LoadedCommittedView::new(selected.selected);
        tracing::Span::current().record("revision", loaded.manifest.revision);
        if loaded.manifest.definition_version != request.definition.version {
            if requested_revision.is_some() {
                return Err(Status::failed_precondition(
                    "requested revision belongs to another index definition version",
                ));
            }
            let (facet_results, aggregate_results) =
                empty_computation_results(&field_names, &compiled.facets, &compiled.aggregates)?;
            record_computation_results(schema.kind, facet_results.len(), aggregate_results.len())?;
            return Ok(ExecutedIndexQuery {
                hits: Vec::new(),
                facet_results,
                aggregate_results,
                freshness: query_freshness(
                    &loaded,
                    observed.as_ref(),
                    false,
                    request.authorization_revision,
                )?,
                next_position: None,
            });
        }
        require_manifest_schema(&loaded.manifest, &schema)?;
        let directory =
            QueryObservedDirectory::new(directory, observer, self.cpu.clone(), schema.kind);
        let native = NativeQueryRequest {
            schema,
            segments: loaded.manifest.segments.clone(),
            query: compiled.query,
            after,
            limit: u32::try_from(request.limit)
                .map_err(|_| Status::invalid_argument("index query limit does not fit u32"))?,
            authorization_revision: request.authorization_revision,
            facets: compiled.facets,
            aggregates: compiled.aggregates,
        };
        native.validate().map_err(index_status)?;
        let projection = if let Some((family, source_scope)) = projection_family {
            let required = loaded
                .manifest
                .barrier()
                .map_err(|error| Status::data_loss(error.to_string()))?;
            let required = super::projection_family_writer::projection_barrier(&required)?;
            let loaded_projection = self
                .publisher
                .load_projection_generation(
                    &request.storage_tenant,
                    &request.definition.bucket,
                    family.tenant_id,
                    family.bucket_id,
                    family.family_id,
                )
                .await?
                .ok_or_else(|| {
                    Status::data_loss("typed index cache has no canonical projection generation")
                })?;
            if !loaded_projection.generation.barrier.covers(&required) {
                return Err(Status::unavailable(
                    "canonical projection generation is behind the selected query cache",
                ));
            }
            Some(candidate_gate::ProjectionCandidateGate {
                publisher: self.publisher.clone(),
                generation: loaded_projection.generation,
                source_scope,
                tenant_id: family.tenant_id,
                bucket_id: family.bucket_id,
            })
        } else {
            None
        };
        let gate = RuntimeCandidateGate {
            storage_tenant: request.storage_tenant.clone(),
            bucket: request.definition.bucket.clone(),
            visibility: request.candidate_visibility,
            statistics: statistics.clone(),
            projection,
        };
        let mut limits = NativeQueryLimits::default();
        limits.candidate_gate_batch = gate.candidate_batch_limit(limits.candidate_gate_batch);
        let gate_memory_bytes = gate
            .working_memory_bytes(limits.candidate_gate_batch)
            .map_err(index_status)?;
        let page = execute_native_query(
            &directory,
            &gate,
            &self.query_budget,
            &native,
            limits,
            gate_memory_bytes,
            statistics,
        )
        .await?;
        if page.authorization_revision != request.authorization_revision {
            return Err(Status::failed_precondition(
                "authorization revision changed during index execution",
            ));
        }
        if page.facet_results.len() != native.facets.len()
            || page.aggregate_results.len() != native.aggregates.len()
        {
            return Err(Status::data_loss(
                "native query returned incomplete computation results",
            ));
        }
        record_computation_results(
            native.schema.kind,
            page.facet_results.len(),
            page.aggregate_results.len(),
        )?;
        let hits = page
            .hits
            .into_iter()
            .map(|hit| IndexQueryHit {
                address: Some(ObjectAddress {
                    tenant: request.storage_tenant.clone(),
                    bucket: request.definition.bucket.clone(),
                    path: hit.result.path,
                }),
                object_version: hit.result.version,
                score: hit.score,
            })
            .collect();
        let facet_results = page
            .facet_results
            .into_iter()
            .map(|result| facet_result_to_api(&native.schema.fields, result))
            .collect::<Result<Vec<_>, _>>()?;
        let aggregate_results = page
            .aggregate_results
            .into_iter()
            .map(|result| aggregate_result_to_api(&field_names, result))
            .collect::<Result<Vec<_>, _>>()?;
        let next_position = page
            .next
            .map(|position| position.encode().map_err(index_status))
            .transpose()?;
        Ok(ExecutedIndexQuery {
            hits,
            facet_results,
            aggregate_results,
            freshness: query_freshness(
                &loaded,
                observed.as_ref(),
                true,
                page.authorization_revision,
            )?,
            next_position,
        })
    }
}

fn emit_freshness_wait(
    key: &OpenedCommittedViewKey,
    started: std::time::Instant,
    outcome: &'static str,
    timed_out: bool,
) {
    tracing::debug!(
        index.id = key.index_id,
        tenant.id = key.tenant_id,
        bucket.id = key.bucket_id,
        freshness.outcome = outcome,
        monotonic_counter.keldra_index_freshness_waits_total = 1_u64,
        monotonic_counter.keldra_index_freshness_wait_timeouts_total = u64::from(timed_out),
        histogram.keldra_index_freshness_wait_duration_seconds = started.elapsed().as_secs_f64(),
        "required index freshness wait finished"
    );
}

fn committed_view_pack_totals(manifest: &IndexCommitManifest) -> (u64, u64) {
    let mut unique = BTreeMap::new();
    for pack in manifest.segments.iter().flat_map(|segment| &segment.packs) {
        unique
            .entry((pack.object_content_hash, pack.object_length))
            .or_insert(pack.object_length);
    }
    for locator in &manifest.locator_roots {
        if let super::committed_view::LocatorPackOwnership::Standalone(packs) =
            &locator.pack_ownership
        {
            for pack in packs {
                unique
                    .entry((pack.object_content_hash, pack.object_length))
                    .or_insert(pack.object_length);
            }
        }
    }
    (
        u64::try_from(unique.len()).unwrap_or(u64::MAX),
        unique
            .values()
            .fold(0_u64, |total, bytes| total.saturating_add(*bytes)),
    )
}

#[tonic::async_trait]
impl LocalIndexQueryExecutor for LocalRevisionQueryExecutor {
    async fn execute_local(
        &self,
        request: LocalIndexQueryRequest,
    ) -> Result<ExecutedIndexQuery, Status> {
        self.execute(request).await
    }
}

fn record_computation_requests(
    kind: IndexKind,
    facets: usize,
    aggregates: usize,
) -> Result<(), Status> {
    let facets = u64::try_from(facets)
        .map_err(|_| Status::resource_exhausted("facet request count exceeds u64"))?;
    let aggregates = u64::try_from(aggregates)
        .map_err(|_| Status::resource_exhausted("aggregate request count exceeds u64"))?;
    tracing::Span::current().record("query.facet_computations_requested", facets);
    tracing::Span::current().record("query.aggregate_computations_requested", aggregates);
    tracing::debug!(
        index.kind = ?kind,
        monotonic_counter.keldra_index_query_facet_computations_requested_total = facets,
        monotonic_counter.keldra_index_query_aggregate_computations_requested_total = aggregates,
        "native index computations admitted"
    );
    Ok(())
}

fn record_computation_results(
    kind: IndexKind,
    facets: usize,
    aggregates: usize,
) -> Result<(), Status> {
    let facets = u64::try_from(facets)
        .map_err(|_| Status::resource_exhausted("facet result count exceeds u64"))?;
    let aggregates = u64::try_from(aggregates)
        .map_err(|_| Status::resource_exhausted("aggregate result count exceeds u64"))?;
    tracing::Span::current().record("query.facet_computation_results", facets);
    tracing::Span::current().record("query.aggregate_computation_results", aggregates);
    tracing::debug!(
        index.kind = ?kind,
        monotonic_counter.keldra_index_query_facet_computation_results_total = facets,
        monotonic_counter.keldra_index_query_aggregate_computation_results_total = aggregates,
        "native index computations completed"
    );
    Ok(())
}

fn empty_computation_results(
    field_names: &[String],
    facets: &[keldra_index::v4::FacetRequest],
    aggregates: &[keldra_index::v4::AggregateRequest],
) -> Result<(Vec<IndexFacetResult>, Vec<IndexAggregateResult>), Status> {
    let facets = facets
        .iter()
        .map(|facet| {
            Ok(IndexFacetResult {
                field: field_name(field_names, facet.field_id)?.to_owned(),
                buckets: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    let aggregates = aggregates
        .iter()
        .map(|aggregate| {
            let operation = match aggregate.operation {
                AggregateOperation::Count => IndexAggregateOperation::Count,
                AggregateOperation::Minimum => IndexAggregateOperation::Minimum,
                AggregateOperation::Maximum => IndexAggregateOperation::Maximum,
                AggregateOperation::Sum => IndexAggregateOperation::Sum,
                AggregateOperation::Average => IndexAggregateOperation::Average,
            };
            Ok(IndexAggregateResult {
                field: field_name(field_names, aggregate.field_id)?.to_owned(),
                operation: operation as i32,
                value_json: (aggregate.operation == AggregateOperation::Count)
                    .then(|| b"0".to_vec()),
                contributing_count: 0,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok((facets, aggregates))
}

fn aggregate_result_to_api(
    field_names: &[String],
    result: keldra_index::v4::AggregateResult,
) -> Result<IndexAggregateResult, Status> {
    let operation = match result.operation {
        AggregateOperation::Count => IndexAggregateOperation::Count,
        AggregateOperation::Minimum => IndexAggregateOperation::Minimum,
        AggregateOperation::Maximum => IndexAggregateOperation::Maximum,
        AggregateOperation::Sum => IndexAggregateOperation::Sum,
        AggregateOperation::Average => IndexAggregateOperation::Average,
    };
    Ok(IndexAggregateResult {
        field: field_name(field_names, result.field_id)?.to_owned(),
        operation: operation as i32,
        value_json: result.value.as_ref().map(scalar_json).transpose()?,
        contributing_count: result.contributing_count,
    })
}

fn field_name(field_names: &[String], field_id: FieldId) -> Result<&str, Status> {
    field_names
        .get(field_id.get() as usize)
        .map(String::as_str)
        .ok_or_else(|| Status::data_loss("native query result names an unknown field"))
}

async fn execute_native_query<D, G>(
    directory: &D,
    gate: &G,
    budget: &IndexQueryMemoryBudget,
    request: &NativeQueryRequest,
    limits: NativeQueryLimits,
    runtime_gate_bytes: usize,
    statistics: NativeQueryStatisticsRecorder,
) -> Result<keldra_index::v4::NativeQueryPage, Status>
where
    D: ArtifactDirectoryRead,
    G: CandidateGate<Error = Status>,
{
    let executor = NativeQueryExecutor::new(directory, gate, limits).map_err(index_status)?;
    let estimate = executor.memory_estimate(request).map_err(index_status)?;
    let minimum = estimate
        .minimum_bytes()
        .checked_add(runtime_gate_bytes)
        .ok_or_else(|| Status::resource_exhausted("index query memory requirement overflow"))?;
    let preferred = estimate
        .preferred_bytes()
        .checked_add(runtime_gate_bytes)
        .ok_or_else(|| Status::resource_exhausted("index query memory preference overflow"))?;
    let minimum = u64::try_from(minimum)
        .map_err(|_| Status::resource_exhausted("index query memory requirement exceeds u64"))?;
    let preferred = u64::try_from(preferred)
        .map_err(|_| Status::resource_exhausted("index query memory preference exceeds u64"))?;
    let memory = budget
        .acquire_up_to(minimum, preferred)
        .await
        .map_err(|error| Status::resource_exhausted(error.to_string()))?;
    statistics.query_memory(preferred, memory.charged_bytes());
    let granted_native_bytes = usize::try_from(memory.charged_bytes())
        .unwrap_or(usize::MAX)
        .saturating_sub(runtime_gate_bytes);
    let resident_segments = estimate.resident_segments_for(granted_native_bytes);
    executor
        .execute_observed_with_resident_segments(request, statistics, resident_segments)
        .await
        .map_err(|error| match error {
            NativeQueryExecutionError::Index(error) => index_status(error),
            NativeQueryExecutionError::Gate(error) => error,
        })
}

#[derive(Clone)]
pub(crate) struct ClusterIndexSegmentFetcher {
    reader: ClusterObjectReader,
}

impl ClusterIndexSegmentFetcher {
    pub(crate) fn new(reader: ClusterObjectReader) -> Self {
        Self { reader }
    }
}

#[tonic::async_trait]
impl IndexSegmentFetcher for ClusterIndexSegmentFetcher {
    async fn fetch(
        &self,
        segment: IndexSegmentId,
    ) -> Result<Box<dyn std::io::Read + Send>, IndexCacheError> {
        self.reader
            .open_blob_payload(&BlobRef {
                hash: segment.blake3,
                length: segment.length,
            })
            .await
            .map(|payload| Box::new(payload) as Box<dyn std::io::Read + Send>)
            .map_err(|error| IndexCacheError::Fetch(error.to_string()))
    }
}

struct LoadedCommittedView {
    manifest: IndexCommitManifest,
    reference: CommitManifestReference,
}

impl LoadedCommittedView {
    fn new(selected: SelectedCommittedIndexView) -> Self {
        Self {
            manifest: selected.manifest,
            reference: selected.reference,
        }
    }
}

fn require_manifest_schema(
    manifest: &IndexCommitManifest,
    schema: &keldra_index::v4::Schema,
) -> Result<(), Status> {
    let fingerprint = schema.fingerprint().map_err(index_status)?;
    let physical_order = schema
        .physical_order
        .iter()
        .map(|order| ManifestPhysicalOrder {
            field_id: order.field_id,
            descending: matches!(
                order.direction,
                keldra_index::v4::OrderDirection::Descending
            ),
        })
        .collect::<Vec<_>>();
    if manifest.kind != schema.kind
        || manifest.schema_fingerprint != fingerprint
        || manifest.physical_order != physical_order
    {
        return Err(Status::data_loss(
            "format-v4 manifest schema differs from its definition",
        ));
    }
    Ok(())
}

fn freshness(
    revision: &LoadedCommittedView,
    observed: Option<&IndexBarrier>,
    initial_build_complete: bool,
    authorization_revision: u64,
) -> Result<IndexFreshness, Status> {
    let indexed = revision
        .manifest
        .sources
        .iter()
        .map(|source| (source.node_id, source))
        .collect::<std::collections::BTreeMap<_, _>>();
    let observed_sources = observed
        .map(|barrier| {
            barrier
                .sources
                .iter()
                .map(|(node, cursor)| (node.0, cursor))
                .collect::<std::collections::BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let mut node_ids = indexed.keys().copied().collect::<Vec<_>>();
    node_ids.extend(observed_sources.keys().copied());
    node_ids.sort_unstable();
    node_ids.dedup();
    let mut rebuilding =
        observed.is_some_and(|barrier| revision.manifest.placement_fence != barrier.fence);
    let sources = node_ids
        .into_iter()
        .map(
            |node_id| match (indexed.get(&node_id), observed_sources.get(&node_id)) {
                (Some(indexed), Some(observed)) if indexed.source == observed.source => {
                    IndexSourceFreshness {
                        node_id,
                        source_epoch: indexed.source.source_epoch.to_vec(),
                        indexed_next_offset: indexed.next_offset,
                        observed_tail: observed.next_offset.checked_sub(1),
                        lag_hint: observed.next_offset.saturating_sub(indexed.next_offset),
                    }
                }
                (Some(indexed), _) => {
                    rebuilding |= observed.is_some();
                    IndexSourceFreshness {
                        node_id,
                        source_epoch: indexed.source.source_epoch.to_vec(),
                        indexed_next_offset: indexed.next_offset,
                        observed_tail: None,
                        lag_hint: 0,
                    }
                }
                (None, Some(observed)) => {
                    rebuilding = true;
                    IndexSourceFreshness {
                        node_id,
                        source_epoch: observed.source.source_epoch.to_vec(),
                        indexed_next_offset: 0,
                        observed_tail: observed.next_offset.checked_sub(1),
                        lag_hint: observed.next_offset,
                    }
                }
                (None, None) => unreachable!("node ID came from one source map"),
            },
        )
        .collect();
    Ok(IndexFreshness {
        commit_revision: revision.manifest.revision,
        published_at: Some(publication_time(revision.reference.published_at_unix_millis)?.into()),
        sources,
        initial_build_complete,
        rebuilding,
        authorization_revision,
        placement_term: revision.manifest.placement_fence.term,
        placement_index: revision.manifest.placement_fence.index,
        index_id: revision.manifest.index_id,
        definition_version: revision.manifest.definition_version,
    })
}

fn query_freshness(
    revision: &LoadedCommittedView,
    observed: Option<&IndexBarrier>,
    initial_build_complete: bool,
    authorization_revision: u64,
) -> Result<IndexFreshness, Status> {
    freshness(
        revision,
        observed,
        initial_build_complete,
        authorization_revision,
    )
}

fn committed_view_covers(
    opened: &OpenedCommittedIndexView,
    requirement: &crate::index_service::IndexFreshnessRequirement,
) -> bool {
    requirement.sources.iter().all(|required| {
        opened.selected.manifest.sources.iter().any(|indexed| {
            indexed.node_id == required.node_id
                && indexed.source.source_epoch == required.source_epoch
                && indexed.next_offset >= required.next_offset
        })
    }) && requirement.atomic_through.is_none_or(|required| {
        opened
            .selected
            .manifest
            .atomic_through
            .is_some_and(|indexed| indexed >= required)
    })
}

fn empty_freshness(
    index_id: u64,
    definition_version: u64,
    authorization_revision: u64,
    observed: Option<&IndexBarrier>,
) -> IndexFreshness {
    let sources = observed
        .into_iter()
        .flat_map(|barrier| &barrier.sources)
        .map(|(node, cursor)| IndexSourceFreshness {
            node_id: node.0,
            source_epoch: cursor.source.source_epoch.to_vec(),
            indexed_next_offset: 0,
            observed_tail: cursor.next_offset.checked_sub(1),
            lag_hint: cursor.next_offset,
        })
        .collect();
    IndexFreshness {
        commit_revision: 0,
        published_at: None,
        sources,
        initial_build_complete: false,
        rebuilding: true,
        authorization_revision,
        placement_term: 0,
        placement_index: 0,
        index_id,
        definition_version,
    }
}

fn publication_time(unix_millis: u64) -> Result<std::time::SystemTime, Status> {
    std::time::UNIX_EPOCH
        .checked_add(Duration::from_millis(unix_millis))
        .ok_or_else(|| Status::data_loss("index publication timestamp exceeds the system clock"))
}

fn index_status(error: IndexError) -> Status {
    match error {
        IndexError::InvalidQuery(_) => Status::invalid_argument(error.to_string()),
        IndexError::ResourceLimit { .. } => Status::resource_exhausted(error.to_string()),
        IndexError::InvalidDefinition(_) => Status::failed_precondition(error.to_string()),
        IndexError::Io(_) => Status::unavailable(error.to_string()),
        IndexError::Encode(_) => Status::internal(error.to_string()),
        _ => Status::data_loss(error.to_string()),
    }
}

fn cache_status(error: IndexCacheError) -> Status {
    Status::unavailable(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn millisecond_timestamp_is_exact() {
        let value = publication_time(1_234)
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        assert_eq!(value, Duration::from_millis(1_234));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn observed_query_reads_yield_at_the_configured_byte_quantum() {
        let observer = QueryReadObserver::new(14);
        let peer_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_flag = Arc::clone(&peer_ran);
        let peer = tokio::spawn(async move {
            task_flag.store(true, Ordering::Relaxed);
        });
        for _ in 0..4 {
            observer.record_read_and_yield(7).await;
            if peer_ran.load(Ordering::Relaxed) {
                break;
            }
        }
        let peer_ran_during_reads = peer_ran.load(Ordering::Relaxed);
        peer.await.unwrap();
        assert!(peer_ran_during_reads);
        assert_eq!(observer.snapshot().bytes, 14);
    }

    #[test]
    fn empty_results_keep_authorization_evidence() {
        let freshness = empty_freshness(7, 2, 91, None);
        assert_eq!(freshness.authorization_revision, 91);
        assert_eq!(freshness.commit_revision, 0);
    }

    #[test]
    fn empty_commit_returns_one_result_per_requested_computation() {
        let (facets, aggregates) = empty_computation_results(
            &["ecosystem".into(), "severity".into()],
            &[keldra_index::v4::FacetRequest {
                field_id: FieldId::new(0),
                limit: 10,
            }],
            &[
                keldra_index::v4::AggregateRequest {
                    field_id: FieldId::new(1),
                    operation: AggregateOperation::Count,
                },
                keldra_index::v4::AggregateRequest {
                    field_id: FieldId::new(1),
                    operation: AggregateOperation::Average,
                },
            ],
        )
        .unwrap();

        assert_eq!(facets[0].field, "ecosystem");
        assert!(facets[0].buckets.is_empty());
        assert_eq!(aggregates[0].value_json.as_deref(), Some(b"0".as_slice()));
        assert_eq!(aggregates[1].value_json, None);
        assert!(
            aggregates
                .iter()
                .all(|aggregate| aggregate.contributing_count == 0)
        );
    }

    #[test]
    fn query_admission_charges_the_outer_candidate_gate_envelope() {
        let batch = NativeQueryLimits::default().candidate_gate_batch;
        let bytes = runtime_gate_envelope_bytes(
            batch,
            MAX_OBJECT_TENANT_BYTES,
            MAX_OBJECT_BUCKET_BYTES,
            false,
        )
        .unwrap();
        let retained_path_payload = batch * 4 * MAX_OBJECT_PATH_BYTES;

        assert!(bytes > retained_path_payload);
        assert!(
            runtime_gate_envelope_bytes(
                batch,
                MAX_OBJECT_TENANT_BYTES + 1,
                MAX_OBJECT_BUCKET_BYTES,
                false,
            )
            .is_err()
        );
    }
}
