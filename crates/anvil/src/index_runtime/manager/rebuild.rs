use std::collections::VecDeque;
use std::io;

use anvil_index::bulk::BulkBuildOptions;
use rayon::prelude::*;
use tracing::Instrument;

use super::*;
use crate::cluster_object_read::ClusterReadPayload;
use crate::index_runtime::budget::IndexMemoryPermit;
use crate::index_runtime::engine::EngineBulkBuilder;
use crate::index_runtime::publisher::IndexBlockStagingSink;

pub(super) struct RebuildWork {
    pub(super) current: Option<PublishedGeneration>,
    pub(super) _snapshot_slot: tokio::sync::OwnedSemaphorePermit,
    pub(super) _memory_permit: IndexMemoryPermit,
    pub(super) snapshot: ClusterIndexSourceSnapshot,
    pub(super) through: IndexBarrier,
    pub(super) candidate: CandidateGeneration,
    pub(super) builder: Option<EngineBulkBuilder<IndexBlockStagingSink, IndexCompactionExecutor>>,
    pub(super) plan: SegmentMemoryPlan,
    pub(super) source_quantum_bytes: u64,
    pub(super) progress: BuilderProgress,
}

pub(super) fn open_bulk_builder(
    job: &BuilderJob,
    dependencies: &IndexBuilderDependencies,
    plan: SegmentMemoryPlan,
) -> Result<EngineBulkBuilder<IndexBlockStagingSink, IndexCompactionExecutor>, Status> {
    let configured = usize::try_from(dependencies.config.external_sort_chunk_bytes(job.kind))
        .map_err(|_| Status::resource_exhausted("external-sort chunk exceeds platform"))?;
    let sort_chunk_bytes = plan.max_resident_bytes.min(configured).max(1);
    let configured_lanes = usize::try_from(dependencies.config.projection_max_lanes(job.kind))
        .map_err(|_| Status::resource_exhausted("bulk rewrite lane limit exceeds platform"))?;
    let rewrite_parallelism = CompactionParallelism::for_budget(
        configured_lanes,
        dependencies.cpu.workers(),
        plan.total_bytes as u64,
    )
    .map_err(index_status)?;
    let options = BulkBuildOptions::new(sort_chunk_bytes, rewrite_parallelism.max_lanes())
        .map_err(index_status)?;
    tracing::info!(
        index.kind = ?job.kind,
        builder.phase = "bulk_open",
        gauge.anvil_index_bulk_sort_chunk_bytes = sort_chunk_bytes as u64,
        gauge.anvil_index_bulk_rewrite_configured_lanes = configured_lanes as u64,
        gauge.anvil_index_bulk_rewrite_effective_lanes = rewrite_parallelism.max_lanes() as u64,
        gauge.anvil_index_bulk_workspace_bytes = plan.total_bytes as u64,
        "direct index bulk builder configured"
    );
    EngineBulkBuilder::new(
        &job.specification,
        dependencies.publisher.staging_sink(),
        IndexCompactionExecutor::new(dependencies.cpu.clone()),
        options,
    )
    .map_err(index_status)
}

pub(super) async fn advance_rebuild(
    job: &BuilderJob,
    mut work: RebuildWork,
    dependencies: &IndexBuilderDependencies,
) -> Result<
    (
        BuilderPhase,
        BuilderDisposition,
        Option<PublishedGeneration>,
    ),
    Status,
> {
    let mut quantum = SourceWorkQuantum::from_wire_limit(work.source_quantum_bytes);
    loop {
        let scan_started = Instant::now();
        let scan_span = tracing::info_span!(
            "anvil.index.bulk_scan",
            index.kind = ?job.kind,
            scan.frame_records = tracing::field::Empty,
            scan.frame_bytes = tracing::field::Empty,
            scan.elapsed_seconds = tracing::field::Empty,
            scan.outcome = tracing::field::Empty,
        );
        let frame = await_with_builder_heartbeats(
            &work.progress,
            work.snapshot.next_frame().instrument(scan_span.clone()),
        )
        .await;
        let scan_elapsed_seconds = scan_started.elapsed().as_secs_f64();
        let (scan_records, scan_bytes) = match frame.as_ref() {
            Ok(Some(frame)) => (frame.len() as u64, measure_snapshot_frame(frame)?),
            Ok(None) | Err(_) => (0, 0),
        };
        scan_span.record("scan.frame_records", scan_records);
        scan_span.record("scan.frame_bytes", scan_bytes);
        scan_span.record("scan.elapsed_seconds", scan_elapsed_seconds);
        scan_span.record(
            "scan.outcome",
            if frame.is_err() {
                "failed"
            } else {
                "completed"
            },
        );
        scan_span.in_scope(|| {
            tracing::info!(
                index.kind = ?job.kind,
                scan.outcome = if frame.is_err() { "failed" } else { "completed" },
                monotonic_counter.anvil_index_bulk_scan_reads_total = 1_u64,
                monotonic_counter.anvil_index_bulk_scan_failures_total =
                    u64::from(frame.is_err()),
                monotonic_counter.anvil_index_bulk_scan_records_total = scan_records,
                monotonic_counter.anvil_index_bulk_scan_bytes_total = scan_bytes,
                histogram.anvil_index_bulk_scan_frame_records = scan_records,
                histogram.anvil_index_bulk_scan_frame_bytes = scan_bytes,
                histogram.anvil_index_bulk_scan_read_duration_seconds = scan_elapsed_seconds,
                "index bulk source frame read"
            );
        });
        let frame = frame?;
        let Some(frame) = frame else {
            let builder = work
                .builder
                .take()
                .ok_or_else(|| Status::internal("snapshot bulk builder is missing"))?;
            let sort_progress = builder.external_sort_progress();
            let finish_started = Instant::now();
            let finish_span = tracing::info_span!(
                "anvil.index.bulk_finish",
                index.kind = ?job.kind,
                bulk.outcome = tracing::field::Empty,
                bulk.elapsed_seconds = tracing::field::Empty,
            );
            let finished = await_with_builder_heartbeats(
                &work.progress,
                builder.finish().instrument(finish_span.clone()),
            )
            .await;
            let finish_elapsed_seconds = finish_started.elapsed().as_secs_f64();
            let finish_failed = finished.is_err();
            let sort_snapshot = sort_progress
                .map(|progress| progress.snapshot())
                .unwrap_or_default();
            finish_span.record(
                "bulk.outcome",
                if finish_failed { "failed" } else { "completed" },
            );
            finish_span.record("bulk.elapsed_seconds", finish_elapsed_seconds);
            finish_span.in_scope(|| {
                tracing::info!(
                    index.kind = ?job.kind,
                    builder.phase = "sort_and_pack",
                    builder.outcome = if finish_failed { "failed" } else { "completed" },
                    monotonic_counter.anvil_index_bulk_finish_failures_total =
                        u64::from(finish_failed),
                    monotonic_counter.anvil_index_external_sort_chunks_total =
                        sort_snapshot.sort_chunks,
                    monotonic_counter.anvil_index_external_sort_merge_passes_total =
                        sort_snapshot.sort_merge_passes,
                    gauge.anvil_index_external_sort_peak_workspace_bytes =
                        sort_snapshot.sort_peak_workspace_bytes,
                    histogram.anvil_index_bulk_finish_duration_seconds = finish_elapsed_seconds,
                    "direct index bulk builder finished"
                );
            });
            let (sealed, sink) = finished.map_err(index_status)?;
            if let Some(sealed) = sealed {
                let sequence = work.candidate.allocate_sequence()?;
                let published = await_with_builder_heartbeats(
                    &work.progress,
                    dependencies.publisher.publish_run(
                        &job.definition.stored,
                        job.definition.tenant_id,
                        job.definition.bucket_id,
                        sequence,
                        sealed,
                        sink,
                    ),
                )
                .await?;
                work.candidate.runs.push(published.manifest);
                tracing::info!(
                    index.kind = ?job.kind,
                    monotonic_counter.anvil_index_bulk_base_runs_created_total = 1_u64,
                    "direct index snapshot base run published"
                );
            }
            let target = dependencies
                .journal
                .capture_barrier()
                .await
                .map_err(event_status)?;
            work.progress.complete();
            emit_source_lag(job.kind, &work.through, &target);
            return Ok((
                BuilderPhase::CatchUp(CatchUpWork {
                    current: work.current,
                    through: work.through,
                    target,
                    candidate: work.candidate,
                    changed: false,
                    must_publish: true,
                    maintenance: false,
                    progress: BuilderProgress::start(
                        job.telemetry_identity(),
                        BuilderProgressPhase::CatchUp,
                    ),
                }),
                BuilderDisposition::Ready,
                None,
            ));
        };

        let records = scan_records;
        let encoded_bytes = scan_bytes;
        await_with_builder_heartbeats(
            &work.progress,
            process_snapshot_frame(
                &job.definition,
                &job.specification,
                &work.through,
                frame,
                work.plan,
                work.builder
                    .as_mut()
                    .ok_or_else(|| Status::internal("snapshot bulk builder is missing"))?,
                &mut work.candidate,
                dependencies,
            ),
        )
        .await?;
        work.progress.advance(records, encoded_bytes);
        if quantum.advance_frame(encoded_bytes)? == SourceWorkBoundary::SealAndYield {
            let range_started = Instant::now();
            let range_result = await_with_builder_heartbeats(
                &work.progress,
                work.builder
                    .as_mut()
                    .ok_or_else(|| Status::internal("snapshot bulk builder is missing"))?
                    .finish_range(),
            )
            .await;
            tracing::info!(
                index.kind = ?job.kind,
                builder.phase = "range_finish",
                builder.outcome = if range_result.is_err() { "failed" } else { "completed" },
                monotonic_counter.anvil_index_bulk_ranges_finished_total =
                    u64::from(range_result.is_ok()),
                monotonic_counter.anvil_index_bulk_range_finish_failures_total =
                    u64::from(range_result.is_err()),
                histogram.anvil_index_bulk_range_finish_duration_seconds =
                    range_started.elapsed().as_secs_f64(),
                "direct index bulk source range finished"
            );
            range_result.map_err(index_status)?;
            return Ok((BuilderPhase::Rebuild(work), BuilderDisposition::Ready, None));
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_snapshot_frame(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    barrier: &IndexBarrier,
    frame: Vec<IndexSourceSnapshotHead>,
    plan: SegmentMemoryPlan,
    builder: &mut EngineBulkBuilder<IndexBlockStagingSink, IndexCompactionExecutor>,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let configured_lanes = usize::try_from(
        dependencies
            .config
            .projection_max_lanes(kind_for_specification(specification).map_err(index_status)?),
    )
    .map_err(|_| Status::resource_exhausted("projection lane limit exceeds platform"))?;
    let max_parallel = dependencies.cpu.workers().min(configured_lanes).max(1);
    let projection_budget = plan.max_source_projection_bytes as u64;
    let mut batch = ProjectionBatch::new(projection_budget, max_parallel);
    for head in frame {
        if head.tenant_id != definition.tenant_id
            || head.bucket_id != definition.bucket_id
            || head.head.version != head.version.id
            || head.head.deleted
            || head.version.deleted
        {
            return Err(Status::data_loss(
                "index snapshot returned an invalid current live head",
            ));
        }
        require_visible_head(&head.head, barrier)?;
        if !source_matches_definition(
            &definition.stored,
            &head.exact_path,
            head.version.content_type.as_deref(),
        ) {
            continue;
        }
        let source = IndexSourceMutation::Upsert(build_object(&head.exact_path, &head.version)?);
        let prepared = PreparedProjection::new(specification, source)?;
        if let Some(pending) = batch.try_push(prepared)? {
            let full = std::mem::replace(
                &mut batch,
                ProjectionBatch::new(projection_budget, max_parallel),
            );
            project_snapshot_batch(specification, plan, full, builder, candidate, dependencies)
                .await?;
            if batch.try_push(pending)?.is_some() {
                return Err(Status::internal(
                    "projection source was rejected by an empty batch after admission",
                ));
            }
        }
    }
    if !batch.is_empty() {
        project_snapshot_batch(specification, plan, batch, builder, candidate, dependencies)
            .await?;
    }
    Ok(())
}

async fn project_snapshot_batch(
    specification: &IndexSpecification,
    plan: SegmentMemoryPlan,
    batch: ProjectionBatch,
    builder: &mut EngineBulkBuilder<IndexBlockStagingSink, IndexCompactionExecutor>,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let kind = kind_for_specification(specification).map_err(index_status)?;
    let configured_lanes = dependencies.config.projection_max_lanes(kind);
    let source_count = batch.sources.len() as u64;
    let admitted_bytes = batch.resident_bytes;
    let effective_lanes = batch.effective_lanes();
    let lane_limit = batch.lane_limit()?;
    let payload_fetches = batch
        .sources
        .iter()
        .filter(|source| source.needs_payload)
        .count() as u64;
    let payload_bytes = batch
        .sources
        .iter()
        .filter_map(|source| match &source.source {
            IndexSourceMutation::Upsert(object) if source.needs_payload => {
                Some(object.content_length)
            }
            _ => None,
        })
        .fold(0_u64, u64::saturating_add);
    let started = Instant::now();
    let span = tracing::info_span!(
        "anvil.index.projection_wave",
        index.kind = ?kind,
        projection.sources = source_count,
        projection.configured_lanes = configured_lanes,
        projection.effective_lanes = effective_lanes as u64,
        projection.lane_budget_bytes = lane_limit as u64,
        projection.admitted_bytes = admitted_bytes,
        projection.payload_fetches = payload_fetches,
        projection.payload_bytes = payload_bytes,
        projection.accepted = tracing::field::Empty,
        projection.skipped = tracing::field::Empty,
        projection.rayon_queue_seconds = tracing::field::Empty,
        projection.cpu_seconds = tracing::field::Empty,
        projection.elapsed_seconds = tracing::field::Empty,
        projection.outcome = tracing::field::Empty,
    );
    span.in_scope(|| {
        tracing::info!(
            index.kind = ?kind,
            counter.anvil_index_projection_active_lanes = effective_lanes as i64,
            gauge.anvil_index_projection_configured_lanes = configured_lanes,
            gauge.anvil_index_projection_effective_lanes = effective_lanes as u64,
            gauge.anvil_index_projection_lane_budget_bytes = lane_limit as u64,
            gauge.anvil_index_projection_wave_budget_bytes =
                plan.max_source_projection_bytes as u64,
            monotonic_counter.anvil_index_projection_waves_total = 1_u64,
            monotonic_counter.anvil_index_projection_inputs_total = source_count,
            monotonic_counter.anvil_index_projection_admitted_bytes_total = admitted_bytes,
            monotonic_counter.anvil_index_projection_payload_fetches_total = payload_fetches,
            monotonic_counter.anvil_index_projection_payload_bytes_total = payload_bytes,
            "index projection wave started"
        );
    });
    let result = project_snapshot_batch_inner(
        specification,
        batch.sources,
        effective_lanes,
        lane_limit,
        builder,
        candidate,
        dependencies,
    )
    .instrument(span.clone())
    .await;
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let (accepted, skipped) = result
        .as_ref()
        .map_or((0, 0), |totals| (totals.accepted, totals.skipped));
    span.record("projection.accepted", accepted);
    span.record("projection.skipped", skipped);
    span.record("projection.elapsed_seconds", elapsed_seconds);
    span.record(
        "projection.rayon_queue_seconds",
        result.as_ref().map_or(0.0, |totals| totals.queue_seconds),
    );
    span.record(
        "projection.cpu_seconds",
        result.as_ref().map_or(0.0, |totals| totals.cpu_seconds),
    );
    span.record(
        "projection.outcome",
        if result.is_err() {
            "failed"
        } else {
            "completed"
        },
    );
    span.in_scope(|| {
        tracing::info!(
            index.kind = ?kind,
            counter.anvil_index_projection_active_lanes = -(effective_lanes as i64),
            "index projection wave released"
        );
        tracing::info!(
            index.kind = ?kind,
            projection.outcome = if result.is_err() { "failed" } else { "completed" },
            monotonic_counter.anvil_index_projection_failures_total =
                u64::from(result.is_err()),
            monotonic_counter.anvil_index_projection_accepted_total = accepted,
            monotonic_counter.anvil_index_projection_skipped_total = skipped,
            histogram.anvil_index_projection_wave_duration_seconds = elapsed_seconds,
            histogram.anvil_index_projection_rayon_queue_seconds =
                result.as_ref().map_or(0.0, |totals| totals.queue_seconds),
            histogram.anvil_index_projection_cpu_seconds =
                result.as_ref().map_or(0.0, |totals| totals.cpu_seconds),
            "index projection wave finished"
        );
    });
    result.map(|_| ())
}

#[derive(Clone, Copy, Default)]
struct ProjectionWaveTotals {
    accepted: u64,
    skipped: u64,
    queue_seconds: f64,
    cpu_seconds: f64,
}

struct PreparedProjection {
    source: IndexSourceMutation,
    projection_bytes: u64,
    resident_bytes: u64,
    needs_payload: bool,
}

impl PreparedProjection {
    fn new(
        specification: &IndexSpecification,
        source: IndexSourceMutation,
    ) -> Result<Self, Status> {
        let projection_bytes =
            projection_admission_bytes(specification, &source).map_err(index_status)?;
        let needs_payload =
            source_needs_payload(specification) && matches!(source, IndexSourceMutation::Upsert(_));
        let payload_bytes = match &source {
            IndexSourceMutation::Upsert(object) if needs_payload => object.content_length,
            _ => 0,
        };
        // Inline reads temporarily retain the verified source and destination
        // buffers. Large reads retain an anonymous spool plus one bounded I/O
        // frame. Both charges belong to the already-held per-kind permit.
        let payload_reserve = if payload_bytes == 0 {
            0
        } else if payload_bytes <= anvil_store::SMALL_BLOB_MAX_BYTES as u64 {
            payload_bytes
                .checked_mul(2)
                .ok_or_else(|| Status::resource_exhausted("inline payload reserve overflow"))?
        } else {
            crate::payload_read::PAYLOAD_READ_FRAME_BYTES as u64
        };
        let resident_bytes = projection_bytes
            .checked_add(payload_reserve)
            .ok_or_else(|| Status::resource_exhausted("projection batch reserve overflow"))?;
        Ok(Self {
            source,
            projection_bytes,
            resident_bytes,
            needs_payload,
        })
    }
}

struct ProjectionBatch {
    sources: Vec<PreparedProjection>,
    resident_bytes: u64,
    max_projection_bytes: u64,
    budget: u64,
    max_lanes: usize,
}

impl ProjectionBatch {
    fn new(budget: u64, max_lanes: usize) -> Self {
        Self {
            sources: Vec::new(),
            resident_bytes: 0,
            max_projection_bytes: 0,
            budget,
            max_lanes: max_lanes.max(1),
        }
    }

    fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    fn effective_lanes(&self) -> usize {
        self.sources.len().min(self.max_lanes).max(1)
    }

    fn lane_limit(&self) -> Result<usize, Status> {
        let (_, limit) = self
            .layout(
                self.sources.len(),
                self.resident_bytes,
                self.max_projection_bytes,
            )
            .ok_or_else(|| {
                Status::resource_exhausted("projection batch exceeds its byte budget")
            })?;
        usize::try_from(limit)
            .map_err(|_| Status::resource_exhausted("projection lane budget exceeds platform"))
    }

    fn try_push(
        &mut self,
        source: PreparedProjection,
    ) -> Result<Option<PreparedProjection>, Status> {
        let count = self.sources.len().saturating_add(1);
        let resident = self
            .resident_bytes
            .checked_add(source.resident_bytes)
            .ok_or_else(|| Status::resource_exhausted("projection batch reserve overflow"))?;
        let maximum = self.max_projection_bytes.max(source.projection_bytes);
        if self.layout(count, resident, maximum).is_none() {
            if self.sources.is_empty() {
                return Err(Status::resource_exhausted(format!(
                    "one index source projection cannot fit the {} byte batch budget",
                    self.budget
                )));
            }
            return Ok(Some(source));
        }
        self.resident_bytes = resident;
        self.max_projection_bytes = maximum;
        self.sources.push(source);
        Ok(None)
    }

    fn layout(&self, count: usize, resident: u64, maximum: u64) -> Option<(usize, u64)> {
        if count == 0 || resident >= self.budget {
            return None;
        }
        let lanes = count.min(self.max_lanes).max(1);
        // A lane can have one result buffered while it computes its next one.
        // When every lane owns one source only, no second result can coexist.
        let output_slots = if count <= lanes {
            count
        } else {
            lanes.checked_mul(2)?
        };
        let output_slots = u64::try_from(output_slots).ok()?;
        let lane_limit = self.budget.checked_sub(resident)? / output_slots;
        (lane_limit >= maximum && lane_limit > 0).then_some((lanes, lane_limit))
    }
}

struct FetchedProjection {
    source: IndexSourceMutation,
    payload: Option<ClusterReadPayload>,
}

type ProjectedSource = Result<(EngineMutation, IndexBuildDiagnostics), IndexError>;

async fn project_snapshot_batch_inner(
    specification: &IndexSpecification,
    sources: Vec<PreparedProjection>,
    effective_lanes: usize,
    lane_limit: usize,
    builder: &mut EngineBulkBuilder<IndexBlockStagingSink, IndexCompactionExecutor>,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<ProjectionWaveTotals, Status> {
    let fetched = fetch_projection_sources(sources, effective_lanes, dependencies).await?;
    let lanes = partition_projection_lanes(fetched, effective_lanes);
    let mut senders = Vec::with_capacity(lanes.len());
    let mut receivers = Vec::with_capacity(lanes.len());
    for _ in 0..lanes.len() {
        let (sender, receiver) = tokio::sync::mpsc::channel::<ProjectedSource>(1);
        senders.push(sender);
        receivers.push(receiver);
    }
    let cpu = dependencies.cpu.clone();
    let specification = specification.clone();
    let queued = Instant::now();
    let cpu_task = tokio::spawn(async move {
        cpu.install(move || {
            let cpu_started = Instant::now();
            let queue_seconds = cpu_started.saturating_duration_since(queued).as_secs_f64();
            let lane_cpu_seconds = lanes
                .into_par_iter()
                .zip(senders.into_par_iter())
                .map(|(lane, sender)| {
                    lane.into_iter().fold(0.0, |cpu_seconds, mut fetched| {
                        let started = Instant::now();
                        let reader = fetched
                            .payload
                            .as_mut()
                            .map(|payload| payload as &mut dyn std::io::Read);
                        let projected =
                            project_mutation(&specification, fetched.source, reader, lane_limit);
                        let cpu_seconds = cpu_seconds + started.elapsed().as_secs_f64();
                        let failed = projected.is_err();
                        if sender.blocking_send(projected).is_err() || failed {
                            return cpu_seconds;
                        }
                        cpu_seconds
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .sum();
            ProjectionExecution {
                queue_seconds,
                cpu_seconds: lane_cpu_seconds,
            }
        })
        .await
        .map_err(cpu_status)
    });

    let mut totals = ProjectionWaveTotals::default();
    let mut failure = None;
    'ordered: for receiver in &mut receivers {
        while let Some(projected) = receiver.recv().await {
            match projected {
                Ok((mutation, diagnostics)) => {
                    totals.accepted = totals.accepted.saturating_add(diagnostics.accepted_objects);
                    totals.skipped = totals.skipped.saturating_add(diagnostics.skipped_objects);
                    candidate.diagnostics.add(diagnostics);
                    if let Err(error) = builder.push(mutation).await {
                        failure = Some(index_status(error));
                        break 'ordered;
                    }
                }
                Err(error) => {
                    failure = Some(index_status(error));
                    break 'ordered;
                }
            }
        }
    }
    drop(receivers);
    let timing = cpu_task
        .await
        .map_err(|error| Status::internal(format!("projection batch task failed: {error}")))??;
    if let Some(error) = failure {
        return Err(error);
    }
    totals.queue_seconds = timing.queue_seconds;
    totals.cpu_seconds = timing.cpu_seconds;
    Ok(totals)
}

async fn fetch_projection_sources(
    sources: Vec<PreparedProjection>,
    max_concurrent: usize,
    dependencies: &IndexBuilderDependencies,
) -> Result<Vec<FetchedProjection>, Status> {
    let source_count = sources.len();
    let mut pending = sources.into_iter().enumerate().collect::<VecDeque<_>>();
    let mut fetched = std::iter::repeat_with(|| None)
        .take(source_count)
        .collect::<Vec<Option<FetchedProjection>>>();
    let mut tasks = tokio::task::JoinSet::new();
    fill_projection_fetches(
        &mut tasks,
        &mut pending,
        &mut fetched,
        max_concurrent,
        dependencies,
    )?;
    while let Some(joined) = tasks.join_next().await {
        let (position, value) = joined
            .map_err(|error| Status::internal(format!("payload fetch task failed: {error}")))??;
        let slot = fetched
            .get_mut(position)
            .ok_or_else(|| Status::internal("payload fetch returned an invalid position"))?;
        if slot.replace(value).is_some() {
            return Err(Status::internal(
                "payload fetch returned a duplicate position",
            ));
        }
        fill_projection_fetches(
            &mut tasks,
            &mut pending,
            &mut fetched,
            max_concurrent,
            dependencies,
        )?;
    }
    fetched
        .into_iter()
        .map(|source| source.ok_or_else(|| Status::internal("payload fetch omitted a source")))
        .collect()
}

fn fill_projection_fetches(
    tasks: &mut tokio::task::JoinSet<Result<(usize, FetchedProjection), Status>>,
    pending: &mut VecDeque<(usize, PreparedProjection)>,
    fetched: &mut [Option<FetchedProjection>],
    max_concurrent: usize,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    while tasks.len() < max_concurrent {
        let Some((position, prepared)) = pending.pop_front() else {
            break;
        };
        if !prepared.needs_payload {
            fetched[position] = Some(FetchedProjection {
                source: prepared.source,
                payload: None,
            });
            continue;
        }
        let dependencies = dependencies.clone();
        let span = tracing::Span::current();
        tasks.spawn(
            async move {
                let reference = match &prepared.source {
                    IndexSourceMutation::Upsert(object) => anvil_store::BlobRef {
                        hash: object.content_hash,
                        length: object.content_length,
                    },
                    IndexSourceMutation::Remove(_) => {
                        return Err(Status::internal(
                            "remove projection unexpectedly requested a payload",
                        ));
                    }
                };
                let payload = dependencies.reader.open_blob_payload(&reference).await?;
                Ok((
                    position,
                    FetchedProjection {
                        source: prepared.source,
                        payload: Some(payload),
                    },
                ))
            }
            .instrument(span),
        );
    }
    Ok(())
}

fn partition_projection_lanes<T>(values: Vec<T>, lane_count: usize) -> Vec<Vec<T>> {
    let lane_count = lane_count.min(values.len()).max(1);
    let base = values.len() / lane_count;
    let remainder = values.len() % lane_count;
    let mut values = values.into_iter();
    (0..lane_count)
        .map(|lane| {
            let length = base + usize::from(lane < remainder);
            values.by_ref().take(length).collect()
        })
        .collect()
}

pub(super) fn measure_snapshot_frame(frame: &[IndexSourceSnapshotHead]) -> Result<u64, Status> {
    let mut counter = ByteCounter(0);
    for head in frame {
        serde_json::to_writer(&mut counter, head)
            .map_err(|error| Status::internal(format!("measure index snapshot frame: {error}")))?;
    }
    Ok(counter.0)
}

struct ByteCounter(u64);

impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("index source byte count overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepared(index: usize, projection_bytes: u64, resident_bytes: u64) -> PreparedProjection {
        PreparedProjection {
            source: IndexSourceMutation::Remove(DocumentRef {
                path: format!("docs/{index}"),
                version: 1,
            }),
            projection_bytes,
            resident_bytes,
            needs_payload: false,
        }
    }

    #[test]
    fn projection_batch_is_bounded_by_bytes_not_lane_count() {
        let mut batch = ProjectionBatch::new(64, 4);
        for index in 0..12 {
            assert!(batch.try_push(prepared(index, 1, 1)).unwrap().is_none());
        }

        assert_eq!(batch.sources.len(), 12);
        assert_eq!(batch.effective_lanes(), 4);
        assert_eq!(batch.lane_limit().unwrap(), 6);

        let mut rejected = None;
        for index in 12..64 {
            if let Some(pending) = batch.try_push(prepared(index, 1, 1)).unwrap() {
                rejected = Some(pending);
                break;
            }
        }
        assert!(rejected.is_some());
        assert_eq!(batch.sources.len(), 56);
        assert_eq!(batch.lane_limit().unwrap(), 1);
    }

    #[test]
    fn projection_lane_partition_preserves_order_and_requested_parallelism() {
        let lanes = partition_projection_lanes((0_u64..11).collect(), 4);
        assert_eq!(lanes.iter().map(Vec::len).collect::<Vec<_>>(), [3, 3, 3, 2]);
        assert_eq!(
            lanes.into_iter().flatten().collect::<Vec<_>>(),
            (0_u64..11).collect::<Vec<_>>()
        );
    }
}
