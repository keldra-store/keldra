//! Local execution against one pinned immutable v3 manifest.

use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anvil_api::v1::{IndexFreshness, IndexQueryHit, IndexSourceFreshness, ObjectAddress};
use anvil_index::{BlockDescriptor, IndexDirectoryRead, IndexError, IndexFileRead};
use anvil_store::{BlobRef, ObjectKey};
use tonic::Status;
use tracing::Instrument;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{LocalIndexQueryExecutor, LocalIndexQueryRequest};
use crate::index_service::ExecutedIndexQuery;

use super::cache::{IndexCache, IndexCacheError, IndexSegmentFetcher, IndexSegmentId, IndexSlice};
use super::cpu::IndexCpuPool;
use super::directory::{ManifestIndexDirectory, ManifestIndexFile};
use super::engine::kind_for_specification;
use super::events::{IndexBarrier, IndexEventError, IndexEventJournal};
use super::generation::{
    IndexCurrentPointer, IndexGenerationManifest, ManifestReference, ManifestRun,
};
use super::publication::{current_path, manifest_path};
use super::query::{IndexQueryPosition, execute_query};

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
        // Cache hits can make every index read immediately ready. Each read is
        // already block-bounded; this configurable byte quantum prevents a
        // cached query loop from starving serving-fence renewal without
        // imposing one scheduler yield per small block.
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
    inner: ManifestIndexDirectory,
    observer: QueryReadObserver,
    cpu: IndexCpuPool,
    kind: anvil_index::IndexKind,
}

impl QueryObservedDirectory {
    fn new(
        inner: ManifestIndexDirectory,
        observer: QueryReadObserver,
        cpu: IndexCpuPool,
        kind: anvil_index::IndexKind,
    ) -> Self {
        Self {
            inner,
            observer,
            cpu,
            kind,
        }
    }
}

impl IndexDirectoryRead for QueryObservedDirectory {
    type File = QueryObservedFile;

    async fn open_root(&self) -> Result<Self::File, IndexError> {
        Ok(QueryObservedFile {
            inner: self.inner.open_root().await?,
            observer: self.observer.clone(),
        })
    }

    async fn open_block(&self, descriptor: &BlockDescriptor) -> Result<Self::File, IndexError> {
        Ok(QueryObservedFile {
            inner: self.inner.open_block(descriptor).await?,
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
    inner: ManifestIndexFile,
    observer: QueryReadObserver,
}

impl IndexFileRead for QueryObservedFile {
    type Slice = IndexSlice;

    async fn read_at(&self, offset: u64, max_length: usize) -> Result<Self::Slice, IndexError> {
        let slice = self.inner.read_at(offset, max_length).await?;
        self.observer
            .record_read_and_yield(slice.as_ref().len())
            .await;
        Ok(slice)
    }
}

struct QueryActiveGuard {
    kind: Option<anvil_index::IndexKind>,
    span: tracing::Span,
    observer: QueryReadObserver,
    started: std::time::Instant,
    finished: bool,
}

struct QueryWaitingGuard {
    kind: Option<anvil_index::IndexKind>,
    span: tracing::Span,
    started: std::time::Instant,
    waiting: bool,
}

impl QueryWaitingGuard {
    fn start(kind: Option<anvil_index::IndexKind>, span: &tracing::Span) -> Self {
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
                "local index query admission wait released"
            );
            tracing::info!(
                index.kind = ?self.kind,
                query.admission_outcome = "admitted",
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
            let waiting_seconds = self.started.elapsed().as_secs_f64();
            self.span.in_scope(|| {
                tracing::info!(
                    index.kind = ?self.kind,
                    counter.anvil_index_query_waiting = -1_i64,
                    "local index query admission wait released"
                );
                tracing::info!(
                    index.kind = ?self.kind,
                    query.admission_outcome = "cancelled",
                    monotonic_counter.anvil_index_query_admission_cancellations_total = 1_u64,
                    histogram.anvil_index_query_wait_duration_seconds = waiting_seconds,
                    "local index query admission was cancelled"
                );
            });
        }
    }
}

impl QueryActiveGuard {
    fn start(
        kind: Option<anvil_index::IndexKind>,
        span: &tracing::Span,
        observer: QueryReadObserver,
    ) -> Self {
        if let Some(kind) = kind {
            span.in_scope(|| {
                tracing::info!(
                    index.kind = ?kind,
                    counter.anvil_index_query_active = 1_i64,
                    monotonic_counter.anvil_index_query_runs_total = 1_u64,
                    "local index query admitted"
                );
            });
        }
        Self {
            kind,
            span: span.clone(),
            observer,
            started: std::time::Instant::now(),
            finished: false,
        }
    }

    fn finish(&mut self, result: &Result<ExecutedIndexQuery, Status>) {
        let returned = completed_returned_hits(result);
        self.emit_terminal(
            if result.is_err() {
                "failed"
            } else {
                "completed"
            },
            returned,
            result.is_err(),
            false,
        );
        self.finished = true;
    }

    fn emit_terminal(
        &self,
        outcome: &'static str,
        returned: Option<u64>,
        failed: bool,
        cancelled: bool,
    ) {
        let elapsed_seconds = self.started.elapsed().as_secs_f64();
        let snapshot = self.observer.snapshot();
        self.span.record("query.read_ops", snapshot.reads);
        self.span.record("query.read_bytes", snapshot.bytes);
        self.span
            .record("query.cooperative_yields", snapshot.cooperative_yields);
        if let Some(returned) = returned {
            self.span.record("query.returned_hits", returned);
        }
        self.span.record("query.elapsed_seconds", elapsed_seconds);
        self.span.record("query.outcome", outcome);
        self.span
            .record("otel.status_code", if failed { "error" } else { "ok" });
        if let Some(kind) = self.kind {
            let observed_quanta = snapshot
                .cooperative_yields
                .saturating_add(u64::from(snapshot.partial_quantum_bytes != 0));
            let bytes_per_quantum = snapshot.bytes as f64 / observed_quanta.max(1) as f64;
            self.span.in_scope(|| {
                // A dropped or failed query did not return a response page. Do
                // not turn that unknown count into a misleading zero sample.
                if let Some(returned) = returned {
                    tracing::info!(
                        index.kind = ?kind,
                        query.outcome = outcome,
                        monotonic_counter.anvil_index_query_read_ops_total = snapshot.reads,
                        monotonic_counter.anvil_index_query_read_bytes_total = snapshot.bytes,
                        monotonic_counter.anvil_index_query_cooperative_yields_total =
                            snapshot.cooperative_yields,
                        monotonic_counter.anvil_index_query_failures_total = u64::from(failed),
                        monotonic_counter.anvil_index_query_cancellations_total =
                            u64::from(cancelled),
                        histogram.anvil_index_query_duration_seconds = elapsed_seconds,
                        histogram.anvil_index_query_returned_hits = returned,
                        histogram.anvil_index_query_read_quantum_bytes = bytes_per_quantum,
                        "local index query reached a terminal outcome"
                    );
                } else {
                    tracing::info!(
                        index.kind = ?kind,
                        query.outcome = outcome,
                        monotonic_counter.anvil_index_query_read_ops_total = snapshot.reads,
                        monotonic_counter.anvil_index_query_read_bytes_total = snapshot.bytes,
                        monotonic_counter.anvil_index_query_cooperative_yields_total =
                            snapshot.cooperative_yields,
                        monotonic_counter.anvil_index_query_failures_total = u64::from(failed),
                        monotonic_counter.anvil_index_query_cancellations_total =
                            u64::from(cancelled),
                        histogram.anvil_index_query_duration_seconds = elapsed_seconds,
                        histogram.anvil_index_query_read_quantum_bytes = bytes_per_quantum,
                        "local index query reached a terminal outcome"
                    );
                }
            });
        }
    }
}

fn completed_returned_hits(result: &Result<ExecutedIndexQuery, Status>) -> Option<u64> {
    result
        .as_ref()
        .ok()
        .map(|executed| executed.hits.len() as u64)
}

impl Drop for QueryActiveGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.emit_terminal("cancelled", None, true, true);
        }
        if let Some(kind) = self.kind {
            self.span.in_scope(|| {
                tracing::info!(
                    index.kind = ?kind,
                    counter.anvil_index_query_active = -1_i64,
                    "local index query released"
                );
            });
        }
    }
}

#[derive(Clone)]
pub(crate) struct LocalGenerationQueryExecutor {
    reader: ClusterObjectReader,
    cache: IndexCache,
    events: Arc<IndexEventJournal>,
    cpu: IndexCpuPool,
    admission: Arc<tokio::sync::Semaphore>,
    work_quantum_bytes: u64,
}

impl LocalGenerationQueryExecutor {
    pub(crate) fn new(
        reader: ClusterObjectReader,
        cache: IndexCache,
        events: Arc<IndexEventJournal>,
        cpu: IndexCpuPool,
        max_concurrency: u32,
        work_quantum_bytes: u64,
    ) -> Self {
        debug_assert!(work_quantum_bytes > 0);
        Self {
            reader,
            cache,
            events,
            cpu,
            admission: Arc::new(tokio::sync::Semaphore::new(max_concurrency as usize)),
            work_quantum_bytes,
        }
    }

    async fn execute(&self, request: LocalIndexQueryRequest) -> Result<ExecutedIndexQuery, Status> {
        let index_id = request.definition.index_id;
        let tenant_id = request.tenant_id;
        let bucket_id = request.bucket_id;
        let kind = request
            .definition
            .specification
            .as_ref()
            .and_then(|specification| kind_for_specification(specification).ok());
        let span = tracing::info_span!(
            "anvil.index.query",
            index.id = index_id,
            tenant.id = tenant_id,
            bucket.id = bucket_id,
            index.kind = ?kind,
            query.work_quantum_bytes = self.work_quantum_bytes,
            query.admission_wait_seconds = tracing::field::Empty,
            query.read_ops = tracing::field::Empty,
            query.read_bytes = tracing::field::Empty,
            query.cooperative_yields = tracing::field::Empty,
            query.returned_hits = tracing::field::Empty,
            query.elapsed_seconds = tracing::field::Empty,
            query.outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        if let Some(kind) = kind {
            span.in_scope(|| {
                tracing::info!(
                    index.kind = ?kind,
                    gauge.anvil_index_query_work_quantum_bytes = self.work_quantum_bytes,
                    "local index query work quantum configured"
                );
            });
        }
        let waiting = QueryWaitingGuard::start(kind, &span);
        let permit = self
            .admission
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("index query admission is closed"))?;
        let admission_wait_seconds = waiting.admitted();
        span.record("query.admission_wait_seconds", admission_wait_seconds);
        let observer = QueryReadObserver::new(self.work_quantum_bytes);
        let mut active = QueryActiveGuard::start(kind, &span, observer.clone());
        let result = self
            .execute_inner(request, observer.clone())
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
    ) -> Result<ExecutedIndexQuery, Status> {
        let requested_generation = request.resume.as_ref().map(|resume| resume.generation);
        let loaded = self
            .load_generation(
                &request.storage_tenant,
                &request.definition.bucket,
                request.tenant_id,
                request.bucket_id,
                request.definition.index_id,
                requested_generation,
            )
            .await?;
        let indexed = loaded
            .as_ref()
            .map(|loaded| {
                loaded
                    .manifest
                    .barrier()
                    .map_err(|error| Status::data_loss(error.to_string()))
            })
            .transpose()?;
        let observed = match self
            .events
            .capture_index_bucket_barrier(request.tenant_id, request.bucket_id, indexed.as_ref())
            .await
        {
            Ok(observed) => Some(observed),
            Err(error) if query_history_requires_explicit_rebuild(&error) => {
                return Err(Status::failed_precondition(format!(
                    "index source history is unavailable; an authorized explicit rebuild is required: {error}"
                )));
            }
            Err(error) => return Err(Status::unavailable(error.to_string())),
        };
        let Some(loaded) = loaded else {
            return Ok(ExecutedIndexQuery {
                hits: Vec::new(),
                freshness: empty_freshness(
                    request.definition.index_id,
                    request.definition.version,
                    observed.as_ref(),
                ),
                next_position: None,
            });
        };

        if loaded.manifest.definition_version != request.definition.version {
            if requested_generation.is_some() {
                return Err(Status::failed_precondition(
                    "requested generation belongs to another index definition version",
                ));
            }
            return Ok(ExecutedIndexQuery {
                hits: Vec::new(),
                freshness: query_freshness(&loaded, observed.as_ref(), false)?,
                next_position: None,
            });
        }
        let specification = request
            .definition
            .specification
            .as_ref()
            .ok_or_else(|| Status::data_loss("index definition has no specification"))?;
        let expected_kind = kind_for_specification(specification).map_err(index_status)?;
        if expected_kind != loaded.manifest.kind {
            return Err(Status::data_loss(
                "index manifest kind differs from its definition",
            ));
        }
        let position = request
            .resume
            .as_ref()
            .map(|resume| {
                serde_json::from_slice::<IndexQueryPosition>(&resume.last_position)
                    .map_err(|_| Status::invalid_argument("index page position is malformed"))
            })
            .transpose()?
            .unwrap_or_default();
        let directories = loaded
            .runs
            .iter()
            .map(|run| {
                ManifestIndexDirectory::open(self.cache.clone(), run).map(|inner| {
                    QueryObservedDirectory::new(
                        inner,
                        observer.clone(),
                        self.cpu.clone(),
                        expected_kind,
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(index_status)?;
        let page = execute_query(
            &directories,
            specification,
            &request.query,
            request.limit,
            position,
        )
        .await
        .map_err(index_status)?;
        let hits = page
            .hits
            .into_iter()
            .map(|hit| IndexQueryHit {
                address: hit.object_path.map(|path| ObjectAddress {
                    tenant: request.storage_tenant.clone(),
                    bucket: request.definition.bucket.clone(),
                    path,
                }),
                object_version: hit.object_version,
                score: hit.score,
                fields_json: hit.fields_json,
            })
            .collect();
        let next_position = page
            .next
            .map(|position| {
                serde_json::to_vec(&position)
                    .map_err(|error| Status::internal(format!("encode index position: {error}")))
            })
            .transpose()?;
        Ok(ExecutedIndexQuery {
            hits,
            freshness: query_freshness(&loaded, observed.as_ref(), true)?,
            next_position,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_generation(
        &self,
        tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
        exact_generation: Option<u64>,
    ) -> Result<Option<LoadedGeneration>, Status> {
        let key = ObjectKey::new(tenant, bucket, current_path(index_id))
            .map_err(|error| Status::internal(error.to_string()))?;
        let Some(opened) = self
            .reader
            .open_stable(&key, tenant_id, bucket_id, None)
            .await?
        else {
            return Ok(None);
        };
        if opened.version.deleted {
            return Ok(None);
        }
        let pointer_bytes = read_payload(opened.payload, "current index pointer")?;
        let pointer = IndexCurrentPointer::decode(&pointer_bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if pointer.index_id != index_id {
            return Err(Status::data_loss(
                "current index pointer belongs to another index",
            ));
        }
        let mut published_at = pointer.published_at_unix_millis;
        let mut manifest = self
            .read_manifest_blob(index_id, &pointer.manifest_path, &pointer.manifest_blob)
            .await?;
        let requested = exact_generation.unwrap_or(pointer.generation);
        if requested > pointer.generation {
            return Err(Status::failed_precondition(
                "requested index generation was never published",
            ));
        }
        while manifest.generation > requested {
            let previous = manifest.previous.as_ref().ok_or_else(|| {
                Status::failed_precondition("requested index generation is no longer retained")
            })?;
            published_at = previous.published_at_unix_millis;
            manifest = self.read_manifest_reference(index_id, previous).await?;
        }
        if manifest.generation != requested {
            return Err(Status::failed_precondition(
                "requested index generation is no longer retained",
            ));
        }

        // Engine query order is newest first; manifest persistence is ascending
        // so sequence validation and deterministic CAS bytes stay simple.
        let runs = manifest.runs.iter().rev().cloned().collect();
        Ok(Some(LoadedGeneration {
            manifest,
            runs,
            published_at_unix_millis: published_at,
        }))
    }

    async fn read_manifest_reference(
        &self,
        index_id: u64,
        reference: &ManifestReference,
    ) -> Result<IndexGenerationManifest, Status> {
        let manifest = self
            .read_manifest_blob(index_id, &reference.path, &reference.blob)
            .await?;
        if manifest.generation != reference.generation
            || manifest.definition_version != reference.definition_version
        {
            return Err(Status::data_loss(
                "index manifest predecessor identity differs from its reference",
            ));
        }
        Ok(manifest)
    }

    async fn read_manifest_blob(
        &self,
        index_id: u64,
        path: &str,
        blob: &BlobRef,
    ) -> Result<IndexGenerationManifest, Status> {
        if path != manifest_path(index_id, blob.hash) {
            return Err(Status::data_loss("index manifest path is not canonical"));
        }
        let bytes = self.reader.read_blob_bytes(blob).await?;
        let manifest = IndexGenerationManifest::decode(&bytes)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if manifest.index_id != index_id {
            return Err(Status::data_loss("index manifest belongs to another index"));
        }
        Ok(manifest)
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
    /// Newest first, matching the engine's deterministic version tie-break.
    runs: Vec<ManifestRun>,
    published_at_unix_millis: u64,
}

fn read_payload(
    payload: Option<crate::cluster_object_read::ClusterReadPayload>,
    label: &str,
) -> Result<Vec<u8>, Status> {
    let Some(mut payload) = payload else {
        return Err(Status::data_loss(format!("{label} has no payload")));
    };
    let mut bytes = Vec::new();
    payload
        .read_to_end(&mut bytes)
        .map_err(|error| Status::internal(format!("read {label}: {error}")))?;
    Ok(bytes)
}

fn freshness(
    generation: &LoadedGeneration,
    observed: Option<&IndexBarrier>,
    initial_build_complete: bool,
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
        published_at: Some(publication_time(generation.published_at_unix_millis)?.into()),
        sources,
        initial_build_complete,
        rebuilding,
        authorization_revision: 0,
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
) -> Result<IndexFreshness, Status> {
    let mut value = freshness(generation, observed, initial_build_complete)?;
    if observed.is_none() {
        // The published generation remains complete and queryable, but a
        // A missing observed tail prevents a provable freshness bound until
        // an authorized explicit rebuild or checkpoint publication completes.
        value.rebuilding = true;
    }
    Ok(value)
}

fn query_history_requires_explicit_rebuild(error: &IndexEventError) -> bool {
    matches!(
        error,
        IndexEventError::CheckpointMismatch(_)
            | IndexEventError::SourceEpochChanged(_)
            | IndexEventError::SourceHistoryGap(_)
            | IndexEventError::IncompleteSources
    )
}

fn empty_freshness(
    index_id: u64,
    definition_version: u64,
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
        authorization_revision: 0,
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

fn index_status(error: anvil_index::IndexError) -> Status {
    match error {
        anvil_index::IndexError::InvalidQuery(_) => Status::invalid_argument(error.to_string()),
        anvil_index::IndexError::ResourceLimit { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        anvil_index::IndexError::InvalidDefinition(_) => {
            Status::failed_precondition(error.to_string())
        }
        anvil_index::IndexError::Io(_) => Status::unavailable(error.to_string()),
        anvil_index::IndexError::Encode(_) => Status::internal(error.to_string()),
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
    fn reloaded_checkpoint_only_generation_reports_zero_lag() {
        let source = anvil_store::SourceId {
            node_id: 1,
            source_epoch: [7; 32],
        };
        let barrier = IndexBarrier {
            fence: anvil_store::PlacementLogId { term: 3, index: 8 },
            atomic: super::super::events::AtomicProgramWatermark::new(None, None, 0),
            sources: std::collections::BTreeMap::from([(
                anvil_consensus::NodeId(1),
                super::super::events::IndexSourceCursor {
                    source,
                    next_offset: 41,
                },
            )]),
        };
        let manifest = IndexGenerationManifest::new(
            9,
            2,
            4,
            anvil_index::IndexKind::Path,
            &barrier,
            Vec::new(),
            None,
            0,
            0,
        )
        .unwrap();
        let reloaded = IndexGenerationManifest::decode(&manifest.encode().unwrap()).unwrap();
        let loaded = LoadedGeneration {
            manifest: reloaded,
            runs: Vec::new(),
            published_at_unix_millis: 1_000,
        };

        let result = freshness(&loaded, Some(&barrier), true).unwrap();
        assert!(!result.rebuilding);
        assert_eq!(result.sources.len(), 1);
        assert_eq!(result.sources[0].indexed_next_offset, 41);
        assert_eq!(result.sources[0].observed_tail, Some(40));
        assert_eq!(result.sources[0].lag_hint, 0);
    }

    #[test]
    fn retained_history_gap_keeps_generation_queryable_without_claiming_freshness() {
        let source = anvil_store::SourceId {
            node_id: 1,
            source_epoch: [7; 32],
        };
        let barrier = IndexBarrier {
            fence: anvil_store::PlacementLogId { term: 3, index: 8 },
            atomic: super::super::events::AtomicProgramWatermark::new(None, None, 0),
            sources: std::collections::BTreeMap::from([(
                anvil_consensus::NodeId(1),
                super::super::events::IndexSourceCursor {
                    source,
                    next_offset: 41,
                },
            )]),
        };
        let loaded = LoadedGeneration {
            manifest: IndexGenerationManifest::new(
                9,
                2,
                4,
                anvil_index::IndexKind::Path,
                &barrier,
                Vec::new(),
                None,
                0,
                0,
            )
            .unwrap(),
            runs: Vec::new(),
            published_at_unix_millis: 1_000,
        };

        let unknown = query_freshness(&loaded, None, true).unwrap();
        assert_eq!(unknown.generation, 2);
        assert!(unknown.initial_build_complete);
        assert!(unknown.rebuilding);
        assert_eq!(unknown.sources.len(), 1);
        assert_eq!(unknown.sources[0].indexed_next_offset, 41);
        assert_eq!(unknown.sources[0].observed_tail, None);

        let restored = query_freshness(&loaded, Some(&barrier), true).unwrap();
        assert!(!restored.rebuilding);
        assert_eq!(restored.sources[0].observed_tail, Some(40));
        assert_eq!(restored.sources[0].lag_hint, 0);
    }

    #[test]
    fn source_history_failures_require_an_explicit_rebuild() {
        assert!(query_history_requires_explicit_rebuild(
            &IndexEventError::IncompleteSources
        ));
        assert!(!query_history_requires_explicit_rebuild(
            &IndexEventError::ZeroPageByteLimit
        ));
    }

    #[test]
    fn query_failures_preserve_public_status_semantics() {
        assert_eq!(
            index_status(anvil_index::IndexError::InvalidQuery("bad query".into())).code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            index_status(anvil_index::IndexError::ResourceLimit {
                needed: 2,
                limit: 1,
            })
            .code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(
            index_status(anvil_index::IndexError::Integrity).code(),
            tonic::Code::DataLoss
        );
        assert_eq!(
            index_status(anvil_index::IndexError::Encode("failed".into())).code(),
            tonic::Code::Internal
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
        let snapshot = observer.snapshot();
        assert_eq!(snapshot.reads, 2);
        assert_eq!(snapshot.bytes, 14);
        assert_eq!(snapshot.cooperative_yields, 1);
    }

    #[test]
    fn returned_hit_telemetry_exists_only_for_a_completed_response_page() {
        let completed = Ok(ExecutedIndexQuery {
            hits: vec![IndexQueryHit::default(), IndexQueryHit::default()],
            freshness: empty_freshness(7, 1, None),
            next_position: None,
        });
        assert_eq!(completed_returned_hits(&completed), Some(2));

        let failed = Err(Status::deadline_exceeded("query timed out"));
        assert_eq!(completed_returned_hits(&failed), None);
    }
}
