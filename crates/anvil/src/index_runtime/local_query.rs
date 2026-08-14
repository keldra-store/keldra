//! Local execution against one pinned immutable format-v4 generation.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anvil_api::v1::{IndexFreshness, IndexQueryHit, IndexSourceFreshness, ObjectAddress};
use anvil_atomic_program::{
    MAX_OBJECT_BUCKET_BYTES, MAX_OBJECT_PATH_BYTES, MAX_OBJECT_TENANT_BYTES,
};
use anvil_index::IndexError;
use anvil_index::v4::{
    ArtifactDescriptor, ArtifactDirectoryRead, CandidateGate, CandidateGateEvidence,
    CandidateReference, IndexKind, NativeQueryCursor, NativeQueryExecutionError,
    NativeQueryExecutor, NativeQueryLimits, NativeQueryRequest, NativeQueryStatisticsRecorder,
};
use anvil_store::{BlobRef, CurrentObjectSnapshot, MAX_CONTENT_TYPE_BYTES, ObjectKey};
use tonic::Status;
use tracing::Instrument;

use crate::authorization::ObjectPermission;
use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{LocalIndexQueryExecutor, LocalIndexQueryRequest};
use crate::index_service::{
    CandidateVisibilityEvidence, ExecutedIndexQuery, IndexCandidateIdentity,
    IndexCandidateVisibility,
};

use super::cache::{IndexCache, IndexCacheError, IndexSegmentFetcher, IndexSegmentId, IndexSlice};
use super::cpu::IndexCpuPool;
use super::directory::{ManifestArtifactDirectory, ManifestArtifactFile};
use super::events::{IndexBarrier, IndexEventJournal};
use super::generation::{IndexGenerationManifest, ManifestPhysicalOrder, ManifestReference};
use super::publisher::{IndexGenerationPublisher, SelectedPublishedGeneration};
use super::query_budget::IndexQueryMemoryBudget;
use super::v4_query::compile_query;
use super::v4_schema::compile_schema;

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

    async fn open_artifact(
        &self,
        descriptor: &ArtifactDescriptor,
    ) -> Result<Self::File, IndexError> {
        Ok(QueryObservedFile {
            inner: self.inner.open(descriptor).await?,
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

impl anvil_index::IndexFileRead for QueryObservedFile {
    type Slice = IndexSlice;

    async fn read_at(&self, offset: u64, max_length: usize) -> Result<Self::Slice, IndexError> {
        let slice = anvil_index::IndexFileRead::read_at(&self.inner, offset, max_length).await?;
        self.observer
            .record_read_and_yield(slice.as_ref().len())
            .await;
        Ok(slice)
    }
}

struct RuntimeCandidateGate {
    storage_tenant: String,
    bucket: String,
    visibility: Arc<dyn IndexCandidateVisibility>,
}

impl RuntimeCandidateGate {
    fn working_memory_bytes(&self, batch: usize) -> Result<usize, IndexError> {
        runtime_gate_envelope_bytes(batch, self.storage_tenant.len(), self.bucket.len())
    }
}

/// Additional outer-runtime state retained while one native candidate batch is
/// authorized and checked against exact-current heads. The native executor
/// already charges its pending candidates and `CandidateReference`s; this
/// charge covers the concrete API candidate, object-key, evidence, and snapshot
/// representations created by Anvil around that boundary.
fn runtime_gate_envelope_bytes(
    batch: usize,
    tenant_bytes: usize,
    bucket_bytes: usize,
) -> Result<usize, IndexError> {
    if tenant_bytes > MAX_OBJECT_TENANT_BYTES || bucket_bytes > MAX_OBJECT_BUCKET_BYTES {
        return Err(IndexError::InvalidQuery(
            "candidate gate scope exceeds object-name bounds".into(),
        ));
    }
    let path_bytes = MAX_OBJECT_PATH_BYTES;
    let object_key_dynamic = tenant_bytes
        .checked_add(bucket_bytes)
        .and_then(|bytes| bytes.checked_add(path_bytes))
        .ok_or(IndexError::OffsetOverflow)?;
    let candidate = std::mem::size_of::<IndexCandidateIdentity>()
        .checked_add(
            path_bytes
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(tenant_bytes))
                .and_then(|bytes| bytes.checked_add(bucket_bytes))
                .ok_or(IndexError::OffsetOverflow)?,
        )
        .ok_or(IndexError::OffsetOverflow)?;
    let authorization_phase = std::mem::size_of::<ObjectKey>()
        .checked_add(std::mem::size_of::<(ObjectKey, ObjectPermission)>())
        .and_then(|bytes| bytes.checked_add(object_key_dynamic.checked_mul(2)?))
        .ok_or(IndexError::OffsetOverflow)?;
    let current_phase = std::mem::size_of::<ObjectKey>()
        .checked_add(std::mem::size_of::<usize>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Option<CurrentObjectSnapshot>>()))
        .and_then(|bytes| bytes.checked_add(object_key_dynamic))
        .and_then(|bytes| bytes.checked_add(path_bytes))
        .and_then(|bytes| bytes.checked_add(MAX_CONTENT_TYPE_BYTES))
        .ok_or(IndexError::OffsetOverflow)?;
    let per_candidate = candidate
        .checked_add(std::mem::size_of::<bool>())
        .and_then(|bytes| bytes.checked_add(authorization_phase.max(current_phase)))
        .ok_or(IndexError::OffsetOverflow)?;
    // Candidate/source/check/evidence in the authorization phase; candidate/
    // source/evidence/positions/snapshots in the exact-current phase.
    let vector_headers = 5usize
        .checked_mul(std::mem::size_of::<Vec<()>>())
        .ok_or(IndexError::OffsetOverflow)?;
    batch
        .checked_mul(per_candidate)
        .and_then(|bytes| bytes.checked_add(vector_headers))
        .ok_or(IndexError::OffsetOverflow)
}

impl CandidateGate for RuntimeCandidateGate {
    type Error = Status;

    fn evaluate(
        &self,
        candidates: &[CandidateReference],
    ) -> impl std::future::Future<Output = Result<CandidateGateEvidence, Self::Error>> + Send {
        async move {
            let candidates = candidates
                .iter()
                .map(|candidate| IndexCandidateIdentity {
                    source_path: candidate.source.path.clone(),
                    source_version: candidate.source.version,
                    result: IndexQueryHit {
                        address: Some(ObjectAddress {
                            tenant: self.storage_tenant.clone(),
                            bucket: self.bucket.clone(),
                            path: candidate.result.path.clone(),
                        }),
                        object_version: candidate.result.version,
                        score: None,
                    },
                })
                .collect::<Vec<_>>();
            let CandidateVisibilityEvidence {
                visible,
                authorization_revision,
                denied,
                stale,
            } = self.visibility.evaluate(&candidates).await?;
            Ok(CandidateGateEvidence {
                visible,
                authorization_revision,
                denied,
                stale,
            })
        }
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
            tracing::info!(
                index.kind = ?kind,
                counter.anvil_index_query_waiting = 1_i64,
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
            tracing::info!(
                index.kind = ?self.kind,
                counter.anvil_index_query_waiting = -1_i64,
                histogram.anvil_index_query_wait_duration_seconds = waiting_seconds,
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
                tracing::info!(
                    index.kind = ?self.kind,
                    counter.anvil_index_query_waiting = -1_i64,
                    monotonic_counter.anvil_index_query_admission_cancellations_total = 1_u64,
                    histogram.anvil_index_query_wait_duration_seconds =
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
            tracing::info!(
                index.kind = ?kind,
                counter.anvil_index_query_active = 1_i64,
                monotonic_counter.anvil_index_query_runs_total = 1_u64,
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
        self.span.record(
            "query.fast_column_blocks_decoded",
            execution.fast_column_blocks_decoded,
        );
        self.span.record(
            "query.stored_field_blocks_decoded",
            execution.stored_field_blocks_decoded,
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
        self.span.record("query.elapsed_seconds", elapsed);
        self.span.record("query.outcome", outcome);
        self.span
            .record("otel.status_code", if failed { "error" } else { "ok" });
        let observed_quanta = snapshot
            .cooperative_yields
            .saturating_add(u64::from(snapshot.partial_quantum_bytes != 0));
        self.span.in_scope(|| {
            tracing::info!(
                index.kind = ?self.kind,
                index.phase = "execute",
                index.tier = tier,
                monotonic_counter.anvil_index_query_read_ops_total = snapshot.reads,
                monotonic_counter.anvil_index_query_read_bytes_total = snapshot.bytes,
                monotonic_counter.anvil_index_query_cooperative_yields_total =
                    snapshot.cooperative_yields,
                monotonic_counter.anvil_index_query_failures_total = u64::from(failed),
                monotonic_counter.anvil_index_query_cancellations_total = u64::from(cancelled),
                monotonic_counter.anvil_index_query_planner_conjunctions_total =
                    execution.planner_conjunctions,
                monotonic_counter.anvil_index_query_planner_reordered_conjunctions_total =
                    execution.planner_reordered_conjunctions,
                monotonic_counter.anvil_index_query_planner_costed_children_total =
                    execution.planner_costed_children,
                monotonic_counter.anvil_index_query_planner_child_cost_total =
                    execution.planner_child_cost_total,
                monotonic_counter.anvil_index_query_term_seeks_total = execution.term_seeks,
                monotonic_counter.anvil_index_query_enumerated_terms_total =
                    execution.enumerated_terms,
                monotonic_counter.anvil_index_query_posting_blocks_decoded_total =
                    execution.posting_blocks_decoded,
                monotonic_counter.anvil_index_query_posting_blocks_sought_total =
                    execution.posting_blocks_sought,
                monotonic_counter.anvil_index_query_posting_blocks_skipped_total =
                    execution.posting_blocks_skipped,
                monotonic_counter.anvil_index_query_posting_bytes_read_total =
                    execution.posting_bytes_read,
                monotonic_counter.anvil_index_query_posting_advance_calls_total =
                    execution.posting_advance_calls,
                monotonic_counter.anvil_index_query_conjunction_advances_total =
                    execution.conjunction_advances,
                monotonic_counter.anvil_index_query_union_heap_pushes_total =
                    execution.union_heap_pushes,
                monotonic_counter.anvil_index_query_union_heap_pops_total =
                    execution.union_heap_pops,
                monotonic_counter.anvil_index_query_two_phase_verifications_total =
                    execution.two_phase_verifications,
                monotonic_counter.anvil_index_query_candidate_doc_ids_total =
                    execution.candidate_doc_ids,
                monotonic_counter.anvil_index_query_live_mask_blocks_decoded_total =
                    execution.live_mask_blocks_decoded,
                monotonic_counter.anvil_index_query_live_mask_rejects_total =
                    execution.live_mask_rejects,
                monotonic_counter.anvil_index_query_fast_column_blocks_decoded_total =
                    execution.fast_column_blocks_decoded,
                monotonic_counter.anvil_index_query_stored_field_blocks_decoded_total =
                    execution.stored_field_blocks_decoded,
                monotonic_counter.anvil_index_query_cursor_seeks_total = execution.cursor_seeks,
                monotonic_counter.anvil_index_query_cursor_skipped_doc_ids_total =
                    execution.cursor_skipped_doc_ids,
                monotonic_counter.anvil_index_query_physical_early_terminations_total =
                    execution.physical_early_terminations,
                monotonic_counter.anvil_index_query_top_k_inspected_total =
                    execution.top_k_inspected,
                monotonic_counter.anvil_index_query_candidate_gate_checked_total =
                    execution.candidate_gate_checked,
                monotonic_counter.anvil_index_query_candidate_gate_batches_total =
                    execution.candidate_gate_batches,
                monotonic_counter.anvil_index_query_candidate_gate_denied_total =
                    execution.candidate_gate_denied,
                monotonic_counter.anvil_index_query_candidate_gate_stale_total =
                    execution.candidate_gate_stale,
                monotonic_counter.anvil_index_query_candidate_gate_refills_total =
                    execution.candidate_gate_refills,
                histogram.anvil_index_query_duration_seconds = elapsed,
                histogram.anvil_index_query_returned_hits = execution.returned_hits,
                histogram.anvil_index_query_planner_lead_cost_min =
                    execution.planner_lead_cost_min,
                histogram.anvil_index_query_planner_lead_cost_max =
                    execution.planner_lead_cost_max,
                histogram.anvil_index_query_read_quantum_bytes =
                    snapshot.bytes as f64 / observed_quanta.max(1) as f64,
                "local index query reached a terminal outcome"
            );
        });
    }
}

impl Drop for QueryActiveGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.emit_terminal("cancelled", true, true);
        }
        self.span.in_scope(|| {
            tracing::info!(
                index.kind = ?self.kind,
                counter.anvil_index_query_active = -1_i64,
                "local index query released"
            );
        });
    }
}

#[derive(Clone)]
pub(crate) struct LocalGenerationQueryExecutor {
    reader: ClusterObjectReader,
    cache: IndexCache,
    events: Arc<IndexEventJournal>,
    publisher: IndexGenerationPublisher,
    cpu: IndexCpuPool,
    query_budget: IndexQueryMemoryBudget,
    admission: Arc<tokio::sync::Semaphore>,
    work_quantum_bytes: u64,
}

impl LocalGenerationQueryExecutor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        reader: ClusterObjectReader,
        cache: IndexCache,
        events: Arc<IndexEventJournal>,
        publisher: IndexGenerationPublisher,
        cpu: IndexCpuPool,
        query_budget: IndexQueryMemoryBudget,
        max_concurrency: u32,
        work_quantum_bytes: u64,
    ) -> Self {
        debug_assert!(work_quantum_bytes > 0);
        Self {
            reader,
            cache,
            events,
            publisher,
            cpu,
            query_budget,
            admission: Arc::new(tokio::sync::Semaphore::new(max_concurrency as usize)),
            work_quantum_bytes,
        }
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
        let span = tracing::info_span!(
            "anvil.index.query",
            index.id = request.definition.index_id,
            definition.version = request.definition.version,
            generation = tracing::field::Empty,
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
            query.fast_column_blocks_decoded = tracing::field::Empty,
            query.stored_field_blocks_decoded = tracing::field::Empty,
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
        let requested_generation = request.resume.as_ref().map(|resume| resume.generation);
        let selected = self
            .publisher
            .load_generation(
                &request.storage_tenant,
                &request.definition.bucket,
                request.tenant_id,
                request.bucket_id,
                request.definition.index_id,
                requested_generation,
            )
            .await?;
        let indexed = selected
            .as_ref()
            .map(|selected| {
                selected
                    .manifest
                    .barrier()
                    .map_err(|error| Status::data_loss(error.to_string()))
            })
            .transpose()?;
        // Freshness evidence is advisory and must never turn journal lag, a
        // temporarily unavailable source, or a retained-generation cursor into
        // query admission work. Builders and snapshot recovery update this
        // process-local observation opportunistically. If it does not cover the
        // pinned generation, omit the optional observed tails and serve the
        // complete generation with its authoritative published barrier.
        let observed =
            compatible_observed_barrier(indexed.as_ref(), self.events.last_observed_barrier());
        let Some(selected) = selected else {
            return Ok(ExecutedIndexQuery {
                hits: Vec::new(),
                freshness: empty_freshness(
                    request.definition.index_id,
                    request.definition.version,
                    request.authorization_revision,
                    observed.as_ref(),
                ),
                next_position: None,
            });
        };
        let loaded = LoadedGeneration::new(selected);
        tracing::Span::current().record("generation", loaded.manifest.generation);
        if loaded.manifest.definition_version != request.definition.version {
            if requested_generation.is_some() {
                return Err(Status::failed_precondition(
                    "requested generation belongs to another index definition version",
                ));
            }
            return Ok(ExecutedIndexQuery {
                hits: Vec::new(),
                freshness: query_freshness(
                    &loaded,
                    observed.as_ref(),
                    false,
                    request.authorization_revision,
                )?,
                next_position: None,
            });
        }
        let specification = request
            .definition
            .specification
            .as_ref()
            .ok_or_else(|| Status::data_loss("index definition has no specification"))?;
        let schema = compile_schema(
            &request.definition.path_prefix,
            (!request.definition.content_type.is_empty())
                .then_some(request.definition.content_type.as_str()),
            specification,
        )
        .map_err(index_status)?;
        require_manifest_schema(&loaded.manifest, &schema)?;
        let query = compile_query(&schema, specification, &request.query).map_err(index_status)?;
        let after = request
            .resume
            .as_ref()
            .map(|resume| NativeQueryCursor::decode(&resume.last_position).map_err(index_status))
            .transpose()?;
        if loaded.manifest.segments.is_empty() {
            return Ok(ExecutedIndexQuery {
                hits: Vec::new(),
                freshness: query_freshness(
                    &loaded,
                    observed.as_ref(),
                    true,
                    request.authorization_revision,
                )?,
                next_position: None,
            });
        }
        let directory = QueryObservedDirectory::new(
            ManifestArtifactDirectory::new(
                self.cache.clone(),
                self.reader.clone(),
                request.storage_tenant.clone(),
                request.definition.bucket.clone(),
                request.tenant_id,
                request.bucket_id,
                request.definition.index_id,
            )
            .map_err(index_status)?,
            observer,
            self.cpu.clone(),
            schema.kind,
        );
        let native = NativeQueryRequest {
            schema,
            segments: loaded.manifest.segments.clone(),
            query,
            after,
            limit: u32::try_from(request.limit)
                .map_err(|_| Status::invalid_argument("index query limit does not fit u32"))?,
            authorization_revision: request.authorization_revision,
        };
        native.validate().map_err(index_status)?;
        let gate = RuntimeCandidateGate {
            storage_tenant: request.storage_tenant.clone(),
            bucket: request.definition.bucket.clone(),
            visibility: request.candidate_visibility,
        };
        let limits = NativeQueryLimits::default();
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
        let next_position = page
            .next
            .map(|position| position.encode().map_err(index_status))
            .transpose()?;
        Ok(ExecutedIndexQuery {
            hits,
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

#[tonic::async_trait]
impl LocalIndexQueryExecutor for LocalGenerationQueryExecutor {
    async fn execute_local(
        &self,
        request: LocalIndexQueryRequest,
    ) -> Result<ExecutedIndexQuery, Status> {
        self.execute(request).await
    }
}

async fn execute_native_query<D, G>(
    directory: &D,
    gate: &G,
    budget: &IndexQueryMemoryBudget,
    request: &NativeQueryRequest,
    limits: NativeQueryLimits,
    runtime_gate_bytes: usize,
    statistics: NativeQueryStatisticsRecorder,
) -> Result<anvil_index::v4::NativeQueryPage, Status>
where
    D: ArtifactDirectoryRead,
    G: CandidateGate<Error = Status>,
{
    let executor = NativeQueryExecutor::new(directory, gate, limits).map_err(index_status)?;
    let requested = executor
        .working_memory_bytes(request)
        .map_err(index_status)?
        .checked_add(runtime_gate_bytes)
        .ok_or_else(|| Status::resource_exhausted("index query memory requirement overflow"))?;
    let requested = u64::try_from(requested)
        .map_err(|_| Status::resource_exhausted("index query memory requirement exceeds u64"))?;
    let _memory = budget
        .acquire(requested)
        .await
        .map_err(|error| Status::resource_exhausted(error.to_string()))?;
    executor
        .execute_observed(request, statistics)
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
    async fn fetch(&self, segment: IndexSegmentId) -> Result<Vec<u8>, IndexCacheError> {
        self.reader
            .read_blob_bytes(&BlobRef {
                hash: segment.blake3,
                length: segment.length,
            })
            .await
            .map_err(|error| IndexCacheError::Fetch(error.to_string()))
    }
}

struct LoadedGeneration {
    manifest: IndexGenerationManifest,
    reference: ManifestReference,
}

impl LoadedGeneration {
    fn new(selected: SelectedPublishedGeneration) -> Self {
        Self {
            manifest: selected.manifest,
            reference: selected.reference,
        }
    }
}

fn require_manifest_schema(
    manifest: &IndexGenerationManifest,
    schema: &anvil_index::v4::Schema,
) -> Result<(), Status> {
    let fingerprint = schema.fingerprint().map_err(index_status)?;
    let physical_order = schema
        .physical_order
        .iter()
        .map(|order| ManifestPhysicalOrder {
            field_id: order.field_id,
            descending: matches!(order.direction, anvil_index::v4::OrderDirection::Descending),
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
    generation: &LoadedGeneration,
    observed: Option<&IndexBarrier>,
    initial_build_complete: bool,
    authorization_revision: u64,
) -> Result<IndexFreshness, Status> {
    let indexed = generation
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
        observed.is_some_and(|barrier| generation.manifest.placement_fence != barrier.fence);
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
        generation: generation.manifest.generation,
        published_at: Some(publication_time(generation.reference.published_at_unix_millis)?.into()),
        sources,
        initial_build_complete,
        rebuilding,
        authorization_revision,
        placement_term: generation.manifest.placement_fence.term,
        placement_index: generation.manifest.placement_fence.index,
        index_id: generation.manifest.index_id,
        definition_version: generation.manifest.definition_version,
    })
}

fn query_freshness(
    generation: &LoadedGeneration,
    observed: Option<&IndexBarrier>,
    initial_build_complete: bool,
    authorization_revision: u64,
) -> Result<IndexFreshness, Status> {
    let mut value = freshness(
        generation,
        observed,
        initial_build_complete,
        authorization_revision,
    )?;
    if observed.is_none() {
        value.rebuilding = true;
    }
    Ok(value)
}

fn compatible_observed_barrier(
    indexed: Option<&IndexBarrier>,
    observed: Option<IndexBarrier>,
) -> Option<IndexBarrier> {
    let observed = observed?;
    let Some(indexed) = indexed else {
        return Some(observed);
    };
    if observed.fence != indexed.fence
        || indexed.sources.iter().any(|(node, indexed)| {
            observed.sources.get(node).is_none_or(|observed| {
                observed.source != indexed.source || observed.next_offset < indexed.next_offset
            })
        })
    {
        return None;
    }
    Some(observed)
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
        generation: 0,
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

    #[test]
    fn query_uses_only_compatible_already_observed_freshness() {
        use anvil_consensus::NodeId;
        use anvil_store::{PlacementLogId, SourceId};

        use crate::index_runtime::events::{AtomicProgramWatermark, IndexSourceCursor};

        fn barrier(fence_index: u64, next_offset: u64) -> IndexBarrier {
            IndexBarrier {
                fence: PlacementLogId {
                    term: 1,
                    index: fence_index,
                },
                atomic: AtomicProgramWatermark::new(None, None, 0),
                sources: [(
                    NodeId(1),
                    IndexSourceCursor {
                        source: SourceId {
                            node_id: 1,
                            source_epoch: [7; 32],
                        },
                        next_offset,
                    },
                )]
                .into_iter()
                .collect(),
            }
        }

        let indexed = barrier(4, 20);
        assert_eq!(
            compatible_observed_barrier(Some(&indexed), Some(barrier(4, 25)))
                .unwrap()
                .sources[&NodeId(1)]
                .next_offset,
            25
        );
        assert!(compatible_observed_barrier(Some(&indexed), Some(barrier(4, 19))).is_none());
        assert!(compatible_observed_barrier(Some(&indexed), Some(barrier(5, 25))).is_none());
        assert_eq!(
            compatible_observed_barrier(None, Some(barrier(5, 25)))
                .unwrap()
                .fence
                .index,
            5
        );
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
        assert_eq!(freshness.generation, 0);
    }

    #[test]
    fn query_admission_charges_the_outer_candidate_gate_envelope() {
        let batch = NativeQueryLimits::default().candidate_gate_batch;
        let bytes =
            runtime_gate_envelope_bytes(batch, MAX_OBJECT_TENANT_BYTES, MAX_OBJECT_BUCKET_BYTES)
                .unwrap();
        let retained_path_payload = batch * 4 * MAX_OBJECT_PATH_BYTES;

        assert!(bytes > retained_path_payload);
        assert!(
            runtime_gate_envelope_bytes(
                batch,
                MAX_OBJECT_TENANT_BYTES + 1,
                MAX_OBJECT_BUCKET_BYTES,
            )
            .is_err()
        );
    }
}
