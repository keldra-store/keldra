use std::collections::VecDeque;
use std::io;
use std::io::Read;
use std::sync::Arc;

use keldra_store::Head;
use tracing::Instrument;

use super::*;
use crate::cluster_object_read::ClusterReadPayload;
use crate::index_runtime::committed_view::IndexCommitManifest;
use crate::index_runtime::publisher::LoadedRebuildRoot;
use crate::index_runtime::rebuild_root::DurableRebuildRoot;

pub(super) struct RebuildWork {
    pub(super) current: Option<CommittedIndexView>,
    pub(super) snapshot: ClusterIndexSourceSnapshot,
    pub(super) through: IndexBarrier,
    pub(super) candidate: CandidateCommit,
    pub(super) maximum_frame_bytes: u64,
    pub(super) progress: BuilderProgress,
    pub(super) attempt_id: u64,
    pub(super) root_version: VersionId,
    pub(super) last_canonical_path: Option<String>,
    pub(super) scanned_records: u64,
    pub(super) scanned_bytes: u64,
    pub(super) resumed_from_durable_root: bool,
}

pub(super) async fn start_rebuild_work(
    job: &BuilderJob,
    current: Option<CommittedIndexView>,
    snapshot: ClusterIndexSourceSnapshot,
    through: IndexBarrier,
    maximum_frame_bytes: u64,
    progress: BuilderProgress,
    dependencies: &IndexBuilderDependencies,
) -> Result<RebuildWork, Status> {
    let attempt_id = dependencies
        .store
        .allocate_snowflake_id()
        .map_err(|error| Status::internal(format!("allocate rebuild attempt ID: {error}")))?;
    let candidate = CandidateCommit::rebuild();
    let manifest = IndexCommitManifest::new(
        job.definition.physical_index_id(),
        attempt_id,
        job.definition.physical_definition_version(),
        job.definition.schema.kind,
        job.definition.schema_fingerprint,
        &through,
        Vec::new(),
        None,
        manifest_physical_order(&job.definition.schema),
        Vec::new(),
        Vec::new(),
        0,
        0,
    )
    .map_err(|error| Status::internal(error.to_string()))?;
    let root = DurableRebuildRoot {
        index_id: job.definition.physical_index_id(),
        definition_version: job.definition.physical_definition_version(),
        attempt_id,
        baseline: through.clone(),
        last_canonical_path: None,
        baseline_complete: false,
        scanned_records: 0,
        scanned_bytes: 0,
        candidate: manifest,
    };
    let loaded = dependencies
        .publisher
        .publish_rebuild_root(
            &job.definition.physical_stored(),
            VersionId(job.definition.object_version),
            job.definition.tenant_id,
            job.definition.bucket_id,
            &root,
            None,
        )
        .await?;
    Ok(RebuildWork {
        current,
        snapshot,
        through,
        candidate,
        maximum_frame_bytes,
        progress,
        attempt_id,
        root_version: loaded.object_version,
        last_canonical_path: None,
        scanned_records: 0,
        scanned_bytes: 0,
        resumed_from_durable_root: false,
    })
}

pub(super) fn resume_rebuild_work(
    current: Option<CommittedIndexView>,
    snapshot: ClusterIndexSourceSnapshot,
    loaded: LoadedRebuildRoot,
    maximum_frame_bytes: u64,
    progress: BuilderProgress,
) -> Result<RebuildWork, Status> {
    let root = loaded.root;
    for checkpoint in snapshot.checkpoints() {
        let baseline = root.baseline.sources.get(&checkpoint.node).ok_or_else(|| {
            Status::failed_precondition("rebuild source set changed while resuming")
        })?;
        if checkpoint.source != baseline.source
            || checkpoint.captured_tail.saturating_add(1) < baseline.next_offset
        {
            return Err(Status::failed_precondition(
                "rebuild source epoch or retained baseline changed while resuming",
            ));
        }
    }
    if snapshot.checkpoints().len() != root.baseline.sources.len()
        || snapshot.placement_fence() != root.baseline.fence
    {
        return Err(Status::failed_precondition(
            "rebuild placement changed while resuming",
        ));
    }
    Ok(RebuildWork {
        current,
        snapshot,
        through: root.baseline,
        candidate: CandidateCommit::from_rebuild_manifest(&root.candidate),
        maximum_frame_bytes,
        progress,
        attempt_id: root.attempt_id,
        root_version: loaded.object_version,
        last_canonical_path: root.last_canonical_path,
        scanned_records: root.scanned_records,
        scanned_bytes: root.scanned_bytes,
        resumed_from_durable_root: true,
    })
}

pub(super) async fn resume_durable_rebuild(
    job: &BuilderJob,
    current: Option<CommittedIndexView>,
    maximum_frame_bytes: u64,
    dependencies: &IndexBuilderDependencies,
) -> Result<Option<RebuildWork>, Status> {
    let Some(loaded) = dependencies
        .publisher
        .load_rebuild_root(
            &job.definition.stored,
            job.definition.tenant_id,
            job.definition.bucket_id,
        )
        .await?
    else {
        return Ok(None);
    };
    if let Some(serving) = current.as_ref()
        && serving_completes_rebuild(
            &loaded.root,
            &serving.manifest,
            job.definition.physical_definition_version(),
        )
    {
        dependencies
            .publisher
            .delete_rebuild_root(
                &job.definition.stored,
                job.definition.tenant_id,
                job.definition.bucket_id,
                loaded.object_version,
            )
            .await?;
        return Ok(None);
    }
    if rebuild_root_requires_replacement(
        &loaded.root,
        job.definition.physical_definition_version(),
        job.definition.schema.kind,
        job.definition.schema_fingerprint,
    ) {
        dependencies
            .publisher
            .delete_rebuild_root(
                &job.definition.stored,
                job.definition.tenant_id,
                job.definition.bucket_id,
                loaded.object_version,
            )
            .await?;
        return Ok(None);
    }
    let snapshot = dependencies
        .scanner
        .begin_source_snapshot(
            job.definition.tenant_id,
            job.definition.bucket_id,
            job.definition.stored.path_prefix.clone(),
            loaded.root.last_canonical_path.clone(),
            maximum_frame_bytes,
        )
        .await?;
    let root_version = loaded.object_version;
    let progress = BuilderProgress::start(job.telemetry_identity(), BuilderProgressPhase::Rebuild);
    match resume_rebuild_work(current, snapshot, loaded, maximum_frame_bytes, progress) {
        Ok(work) => Ok(Some(work)),
        Err(error) if error.code() == tonic::Code::FailedPrecondition => {
            // A topology/source-epoch change invalidates only this non-serving
            // attempt. Exact-delete it so Inspect can establish a fresh
            // baseline instead of retrying an impossible resume forever.
            dependencies
                .publisher
                .delete_rebuild_root(
                    &job.definition.stored,
                    job.definition.tenant_id,
                    job.definition.bucket_id,
                    root_version,
                )
                .await?;
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn serving_completes_rebuild(
    root: &DurableRebuildRoot,
    serving: &IndexCommitManifest,
    definition_version: u64,
) -> bool {
    root.definition_version == definition_version
        && root.baseline_complete
        && root.candidate.segments == serving.segments
        && root.candidate.locator_roots == serving.locator_roots
}

fn rebuild_root_requires_replacement(
    root: &DurableRebuildRoot,
    definition_version: u64,
    kind: keldra_index::v4::IndexKind,
    schema_fingerprint: [u8; 32],
) -> bool {
    root.definition_version != definition_version
        || root.candidate.kind != kind
        || root.candidate.schema_fingerprint != schema_fingerprint
}

pub(super) async fn checkpoint_catch_up_root(
    job: &BuilderJob,
    work: &CatchUpWork,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let Some(loaded) = dependencies
        .publisher
        .load_rebuild_root(
            &job.definition.stored,
            job.definition.tenant_id,
            job.definition.bucket_id,
        )
        .await?
    else {
        return Ok(());
    };
    let encoded_bytes = work
        .candidate
        .segments
        .iter()
        .map(|segment| segment.encoded_bytes)
        .chain(
            work.candidate
                .locator_roots
                .iter()
                .map(|root| root.encoded_bytes),
        )
        .try_fold(0_u64, |total, bytes| total.checked_add(bytes))
        .ok_or_else(|| Status::resource_exhausted("rebuild artifact bytes overflow"))?;
    let logical_bytes = work
        .candidate
        .segments
        .iter()
        .map(|segment| segment.logical_bytes)
        .chain(
            work.candidate
                .locator_roots
                .iter()
                .map(|root| root.logical_bytes),
        )
        .try_fold(0_u64, |total, bytes| total.checked_add(bytes))
        .ok_or_else(|| Status::resource_exhausted("rebuild logical bytes overflow"))?;
    let candidate = IndexCommitManifest::new(
        job.definition.physical_index_id(),
        loaded.root.attempt_id,
        job.definition.physical_definition_version(),
        job.definition.schema.kind,
        job.definition.schema_fingerprint,
        &work.through,
        work.candidate.pending_atomic_batches.clone(),
        None,
        manifest_physical_order(&job.definition.schema),
        work.candidate.segments.clone(),
        work.candidate.locator_roots.clone(),
        encoded_bytes,
        logical_bytes,
    )
    .map_err(commit_view_status)?;
    let checkpoint = DurableRebuildRoot {
        candidate,
        baseline_complete: true,
        ..loaded.root.clone()
    };
    if checkpoint == loaded.root {
        return Ok(());
    }
    dependencies
        .publisher
        .publish_rebuild_root(
            &job.definition.physical_stored(),
            VersionId(job.definition.object_version),
            job.definition.tenant_id,
            job.definition.bucket_id,
            &checkpoint,
            Some(loaded.object_version),
        )
        .await?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RebuildTurnStart {
    RepayDebt,
    ConsumeSnapshot,
}

fn rebuild_turn_start(candidate: &CandidateCommit, limits: DebtLimits) -> RebuildTurnStart {
    if debt::select(&candidate.segments, limits).is_some()
        || debt::select_locator_roots(&candidate.locator_roots, limits).is_some()
    {
        RebuildTurnStart::RepayDebt
    } else {
        RebuildTurnStart::ConsumeSnapshot
    }
}

pub(super) async fn advance_rebuild(
    job: &BuilderJob,
    mut work: RebuildWork,
    dependencies: &IndexBuilderDependencies,
) -> Result<(BuilderPhase, BuilderDisposition, Option<CommittedIndexView>), Status> {
    let debt_limits = DebtLimits::new(
        dependencies.config.max_segments_per_tier(job.kind) as usize,
        dependencies.config.max_unmerged_bytes_per_tier(job.kind),
    );
    // A rebuild candidate is not yet visible, so repay its bounded immutable
    // segment and locator debt before consuming another source frame.
    if rebuild_turn_start(&work.candidate, debt_limits) == RebuildTurnStart::RepayDebt {
        if !compact_one_if_needed(job, &mut work.candidate, debt_limits, dependencies).await? {
            return Err(Status::internal(
                "rebuild compaction debt changed without manager mutation",
            ));
        }
        return Ok((BuilderPhase::Rebuild(work), BuilderDisposition::Ready, None));
    }
    let budget = dependencies.budgets.for_kind(job.kind);
    let permit = await_with_builder_heartbeats(
        &work.progress,
        budget.acquire_up_to(budget.limit(), budget.working_memory_limit()),
    )
    .await
    .map_err(budget_status)?;
    let granted_bytes = permit.bytes();
    let plan = work_plan_for_limit(
        granted_bytes,
        0,
        dependencies.config.segment_flush_bytes(job.kind),
    )?;
    let mut builder =
        NativeSegmentBuild::new(job, plan, SegmentPublicationLane::Maintenance, dependencies)?;
    let mut quantum = SourceWorkQuantum::for_rebuild_turn(granted_bytes, work.maximum_frame_bytes);
    loop {
        let scan_started = Instant::now();
        let scan_span = tracing::debug_span!(
            "keldra.index.bulk_scan",
            index.kind = ?job.kind,
            scan.frame_records = tracing::field::Empty,
            scan.frame_bytes = tracing::field::Empty,
            scan.frame_resident_bytes = tracing::field::Empty,
            scan.elapsed_seconds = tracing::field::Empty,
            scan.outcome = tracing::field::Empty,
        );
        let frame = await_with_builder_heartbeats(
            &work.progress,
            work.snapshot.next_frame().instrument(scan_span.clone()),
        )
        .await;
        let scan_elapsed_seconds = scan_started.elapsed().as_secs_f64();
        let (scan_records, frame_measure) = match frame.as_ref() {
            Ok(Some(frame)) => (
                frame.len() as u64,
                measure_snapshot_frame(frame, frame.capacity())?,
            ),
            Ok(None) | Err(_) => (0, SnapshotFrameMeasure::default()),
        };
        let scan_bytes = frame_measure.encoded_bytes;
        scan_span.record("scan.frame_records", scan_records);
        scan_span.record("scan.frame_bytes", scan_bytes);
        scan_span.record("scan.frame_resident_bytes", frame_measure.resident_bytes);
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
            tracing::debug!(
                index.kind = ?job.kind,
                scan.outcome = if frame.is_err() { "failed" } else { "completed" },
                monotonic_counter.keldra_index_bulk_scan_reads_total = 1_u64,
                monotonic_counter.keldra_index_bulk_scan_failures_total =
                    u64::from(frame.is_err()),
                monotonic_counter.keldra_index_bulk_scan_records_total = scan_records,
                monotonic_counter.keldra_index_bulk_scan_bytes_total = scan_bytes,
                histogram.keldra_index_bulk_scan_frame_records = scan_records,
                histogram.keldra_index_bulk_scan_frame_bytes = scan_bytes,
                histogram.keldra_index_bulk_scan_frame_resident_bytes =
                    frame_measure.resident_bytes,
                histogram.keldra_index_bulk_scan_read_duration_seconds = scan_elapsed_seconds,
                "index bulk source frame read"
            );
        });
        let frame = frame?;
        let Some(frame) = frame else {
            flush_builder(
                &job.definition,
                job.kind,
                &mut builder,
                &mut work.candidate,
                dependencies,
            )
            .await?;
            work.root_version = persist_rebuild_root(job, &work, true, dependencies).await?;
            drop(permit);
            let target = dependencies
                .journal
                .capture_index_bucket_barrier(
                    job.definition.tenant_id,
                    job.definition.bucket_id,
                    Some(&work.through),
                )
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
                    checkpoint_started: None,
                    maintenance: false,
                    progress: BuilderProgress::start(
                        job.telemetry_identity(),
                        BuilderProgressPhase::CatchUp,
                    ),
                    active: None,
                    publishing: None,
                    atomic_projection: None,
                }),
                BuilderDisposition::Ready,
                None,
            ));
        };

        let records = scan_records;
        let encoded_bytes = scan_bytes;
        let last_canonical_path = frame.last().map(|head| head.exact_path.clone());
        let frame_plan = work_plan_for_limit(
            budget.limit(),
            frame_measure.resident_bytes,
            dependencies.config.segment_flush_bytes(job.kind),
        )?;
        let source_payload_bytes = await_with_builder_heartbeats(
            &work.progress,
            process_snapshot_frame(
                &job.definition,
                &work.through,
                work.resumed_from_durable_root,
                frame,
                frame_plan,
                &mut builder,
                &mut work.candidate,
                dependencies,
            ),
        )
        .await?;
        work.progress.advance(records, encoded_bytes);
        work.scanned_records = work.scanned_records.saturating_add(records);
        work.scanned_bytes = work.scanned_bytes.saturating_add(encoded_bytes);
        if let Some(path) = last_canonical_path {
            work.last_canonical_path = Some(path);
        }
        tracing::debug!(
            index.kind = ?job.kind,
            monotonic_counter.keldra_index_source_payload_bytes_total = source_payload_bytes,
            histogram.keldra_index_source_frame_payload_bytes = source_payload_bytes,
            "index source snapshot payload charged to work quantum"
        );
        if quantum.advance_frame(encoded_bytes, source_payload_bytes)?
            == SourceWorkBoundary::SealAndYield
        {
            flush_builder(
                &job.definition,
                job.kind,
                &mut builder,
                &mut work.candidate,
                dependencies,
            )
            .await?;
            work.root_version = persist_rebuild_root(job, &work, false, dependencies).await?;
            drop(permit);
            return Ok((BuilderPhase::Rebuild(work), BuilderDisposition::Ready, None));
        }
    }
}

async fn persist_rebuild_root(
    job: &BuilderJob,
    work: &RebuildWork,
    baseline_complete: bool,
    dependencies: &IndexBuilderDependencies,
) -> Result<VersionId, Status> {
    let encoded_bytes = work
        .candidate
        .segments
        .iter()
        .map(|segment| segment.encoded_bytes)
        .chain(
            work.candidate
                .locator_roots
                .iter()
                .map(|root| root.encoded_bytes),
        )
        .try_fold(0_u64, |total, bytes| total.checked_add(bytes))
        .ok_or_else(|| Status::resource_exhausted("rebuild artifact bytes overflow"))?;
    let logical_bytes = work
        .candidate
        .segments
        .iter()
        .map(|segment| segment.logical_bytes)
        .chain(
            work.candidate
                .locator_roots
                .iter()
                .map(|root| root.logical_bytes),
        )
        .try_fold(0_u64, |total, bytes| total.checked_add(bytes))
        .ok_or_else(|| Status::resource_exhausted("rebuild logical bytes overflow"))?;
    let candidate = IndexCommitManifest::new(
        job.definition.physical_index_id(),
        work.attempt_id,
        job.definition.physical_definition_version(),
        job.definition.schema.kind,
        job.definition.schema_fingerprint,
        &work.through,
        work.candidate.pending_atomic_batches.clone(),
        None,
        manifest_physical_order(&job.definition.schema),
        work.candidate.segments.clone(),
        work.candidate.locator_roots.clone(),
        encoded_bytes,
        logical_bytes,
    )
    .map_err(|error| Status::internal(error.to_string()))?;
    let root = DurableRebuildRoot {
        index_id: job.definition.physical_index_id(),
        definition_version: job.definition.physical_definition_version(),
        attempt_id: work.attempt_id,
        baseline: work.through.clone(),
        last_canonical_path: work.last_canonical_path.clone(),
        baseline_complete,
        scanned_records: work.scanned_records,
        scanned_bytes: work.scanned_bytes,
        candidate,
    };
    dependencies
        .publisher
        .publish_rebuild_root(
            &job.definition.physical_stored(),
            VersionId(job.definition.object_version),
            job.definition.tenant_id,
            job.definition.bucket_id,
            &root,
            Some(work.root_version),
        )
        .await
        .map(|loaded| loaded.object_version)
}

#[allow(clippy::too_many_arguments)]
async fn process_snapshot_frame(
    definition: &CatalogDefinition,
    barrier: &IndexBarrier,
    resumed_from_durable_root: bool,
    frame: Vec<IndexSourceSnapshotHead>,
    plan: SegmentMemoryPlan,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
) -> Result<u64, Status> {
    let configured_lanes = usize::try_from(
        dependencies
            .config
            .projection_max_lanes(runtime_kind(definition.schema.kind)),
    )
    .map_err(|_| Status::resource_exhausted("projection lane limit exceeds platform"))?;
    let max_parallel = dependencies.cpu.workers().min(configured_lanes).max(1);
    let projection_budget = plan.max_source_projection_bytes as u64;
    let mut batch = ProjectionBatch::new(projection_budget, max_parallel);
    let mut source_payload_bytes = 0_u64;
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
        if let Err(error) = require_visible_head(&head.head, barrier) {
            if skip_post_baseline_head(&error, resumed_from_durable_root) {
                // A reopened scan is newer than the durable baseline. Its
                // post-baseline state is reconstructed from the retained
                // journal suffix, never admitted early into the baseline.
                continue;
            }
            return Err(error);
        }
        let Some(source) = rebuild_source(definition, barrier, head, dependencies).await? else {
            continue;
        };
        source_payload_bytes = source_payload_bytes
            .checked_add(source_payload_bytes_for(&definition.schema, &source))
            .ok_or_else(|| Status::resource_exhausted("index source payload bytes overflow"))?;
        let prepared = PreparedProjection::new(&definition.schema, source)?;
        if let Some(pending) = batch.try_push(prepared)? {
            let full = std::mem::replace(
                &mut batch,
                ProjectionBatch::new(projection_budget, max_parallel),
            );
            project_snapshot_batch(
                definition,
                barrier,
                plan,
                full,
                builder,
                candidate,
                dependencies,
            )
            .await?;
            if batch.try_push(pending)?.is_some() {
                return Err(Status::internal(
                    "projection source was rejected by an empty batch after admission",
                ));
            }
        }
    }
    if !batch.is_empty() {
        project_snapshot_batch(
            definition,
            barrier,
            plan,
            batch,
            builder,
            candidate,
            dependencies,
        )
        .await?;
    }
    Ok(source_payload_bytes)
}

async fn rebuild_source(
    definition: &CatalogDefinition,
    barrier: &IndexBarrier,
    head: IndexSourceSnapshotHead,
    dependencies: &IndexBuilderDependencies,
) -> Result<Option<IndexSourceMutation>, Status> {
    if !head.version.protected_link_descriptor {
        return source_matches_definition(
            &definition.stored,
            &head.exact_path,
            head.version.content_type.as_deref(),
        )
        .then(|| build_object(&head.exact_path, &head.version))
        .transpose()
        .map(|value| value.map(IndexSourceMutation::Upsert));
    }
    if !crate::index_service::path_matches_prefix(&head.exact_path, &definition.stored.path_prefix)
    {
        return Ok(None);
    }
    let alias = ObjectKey::new(
        &definition.stored.tenant,
        &definition.stored.bucket,
        &head.exact_path,
    )
    .map_err(|error| Status::data_loss(error.to_string()))?;
    let opened = dependencies
        .reader
        .open(&alias, Some(head.version.id))
        .await?
        .ok_or_else(|| Status::unavailable("index rebuild link descriptor changed"))?;
    if !opened.version.protected_link_descriptor
        || opened.version.content_type.as_deref() != Some(keldra_store::OBJECT_LINK_CONTENT_TYPE)
    {
        return Err(Status::data_loss(
            "index rebuild protected link descriptor metadata changed",
        ));
    }
    let mut payload = opened
        .payload
        .ok_or_else(|| Status::data_loss("index rebuild link descriptor has no payload"))?
        .into_spool();
    let mut bytes = Vec::new();
    payload
        .read_to_end(&mut bytes)
        .map_err(|error| Status::internal(format!("read index rebuild link: {error}")))?;
    let descriptor = keldra_store::ObjectLinkDescriptor::decode(&bytes)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    let target = ObjectKey::new(alias.tenant(), alias.bucket(), descriptor.target_path())
        .map_err(|error| Status::data_loss(error.to_string()))?;
    let current = dependencies
        .reader
        .current_head_snapshot_stable(&target, definition.tenant_id, definition.bucket_id)
        .await?
        .filter(|current| !current.head.deleted && !current.version.deleted)
        .ok_or_else(|| Status::unavailable("index rebuild link target changed"))?;
    require_visible_head(&current.head, barrier)?;
    if current.version.protected_link_descriptor {
        return Err(Status::data_loss("index rebuild encountered a link chain"));
    }
    let registry = current
        .alias_registry
        .as_ref()
        .ok_or_else(|| Status::unavailable("index rebuild link target sidecar changed"))?;
    registry
        .validate(target.path())
        .map_err(|error| Status::data_loss(error.to_string()))?;
    if !barrier.atomic.permits(registry.program_commit_cursor)
        || registry
            .aliases
            .binary_search_by(|path| path.as_str().cmp(alias.path()))
            .is_err()
    {
        return Err(Status::unavailable(
            "index rebuild link sidecar is not visible at the captured barrier",
        ));
    }
    if !source_matches_definition(
        &definition.stored,
        alias.path(),
        current.version.content_type.as_deref(),
    ) {
        return Ok(None);
    }
    Ok(Some(IndexSourceMutation::Upsert(build_object(
        alias.path(),
        &current.version,
    )?)))
}

fn skip_post_baseline_head(error: &Status, resumed_from_durable_root: bool) -> bool {
    resumed_from_durable_root
        && matches!(
            error.code(),
            tonic::Code::Aborted | tonic::Code::Unavailable
        )
}

fn require_visible_head(head: &Head, barrier: &IndexBarrier) -> Result<(), Status> {
    let Some(stamp) = head.mutation_stamp else {
        return Ok(());
    };
    if !barrier.atomic.permits(stamp.program_commit_cursor) {
        return Err(Status::unavailable(
            "index source belongs to an unfinalized atomic program",
        ));
    }
    let stamp_fence = (
        stamp.active_placement_log_id.term,
        stamp.active_placement_log_id.index,
    );
    let barrier_fence = (barrier.fence.term, barrier.fence.index);
    if stamp_fence > barrier_fence {
        return Err(Status::aborted(
            "index source advanced beyond its captured placement fence",
        ));
    }
    if stamp_fence == barrier_fence {
        let node = NodeId(u64::from(stamp.source_id.node_id));
        let Some(cursor) = barrier.sources.get(&node) else {
            return Err(Status::aborted(
                "index source mutation is absent from the captured source vector",
            ));
        };
        if cursor.source != stamp.source_id || stamp.source_journal_position >= cursor.next_offset {
            return Err(Status::aborted(
                "index source mutation advanced beyond its captured journal target",
            ));
        }
    }
    Ok(())
}

async fn project_snapshot_batch(
    definition: &CatalogDefinition,
    barrier: &IndexBarrier,
    plan: SegmentMemoryPlan,
    batch: ProjectionBatch,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let kind = runtime_kind(definition.schema.kind);
    let configured_lanes = dependencies.config.projection_max_lanes(kind);
    let source_count = batch.sources.len() as u64;
    let admitted_bytes = batch.resident_bytes;
    let effective_lanes = batch.effective_lanes();
    let lane_limit = batch.lane_limit()?;
    // Shared Typed JSON fetches happen only on mapper misses and are emitted at
    // that exact boundary. Other kinds still fetch every prepared payload in
    // this wave, so their existing aggregate remains exact.
    let shared_mapper = kind == IndexKind::TypedJson;
    let payload_fetches = (!shared_mapper)
        .then(|| {
            batch
                .sources
                .iter()
                .filter(|source| source.needs_payload)
                .count() as u64
        })
        .unwrap_or(0);
    let payload_bytes = (!shared_mapper)
        .then(|| {
            batch
                .sources
                .iter()
                .filter_map(|source| match &source.source {
                    IndexSourceMutation::Upsert(object) if source.needs_payload => {
                        Some(object.content_length)
                    }
                    _ => None,
                })
                .fold(0_u64, u64::saturating_add)
        })
        .unwrap_or(0);
    let started = Instant::now();
    let span = tracing::debug_span!(
        "keldra.index.projection_wave",
        index.kind = ?kind,
        projection.sources = source_count,
        projection.configured_lanes = configured_lanes,
        projection.effective_lanes = effective_lanes as u64,
        projection.lane_budget_bytes = lane_limit as u64,
        projection.admitted_bytes = admitted_bytes,
        projection.payload_fetches = payload_fetches,
        projection.payload_bytes = payload_bytes,
        projection.shared_mapper = shared_mapper,
        projection.accepted = tracing::field::Empty,
        projection.skipped = tracing::field::Empty,
        projection.rayon_queue_seconds = tracing::field::Empty,
        projection.cpu_seconds = tracing::field::Empty,
        projection.elapsed_seconds = tracing::field::Empty,
        projection.outcome = tracing::field::Empty,
    );
    span.in_scope(|| {
        tracing::debug!(
            index.kind = ?kind,
            counter.keldra_index_projection_active_lanes = effective_lanes as i64,
            gauge.keldra_index_projection_configured_lanes = configured_lanes,
            gauge.keldra_index_projection_effective_lanes = effective_lanes as u64,
            gauge.keldra_index_projection_lane_budget_bytes = lane_limit as u64,
            gauge.keldra_index_projection_wave_budget_bytes =
                plan.max_source_projection_bytes as u64,
            monotonic_counter.keldra_index_projection_waves_total = 1_u64,
            monotonic_counter.keldra_index_projection_inputs_total = source_count,
            monotonic_counter.keldra_index_projection_admitted_bytes_total = admitted_bytes,
            monotonic_counter.keldra_index_projection_payload_fetches_total = payload_fetches,
            monotonic_counter.keldra_index_projection_payload_bytes_total = payload_bytes,
            "index projection wave started"
        );
    });
    let result = project_snapshot_batch_inner(
        definition,
        barrier,
        &definition.schema,
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
        tracing::debug!(
            index.kind = ?kind,
            counter.keldra_index_projection_active_lanes = -(effective_lanes as i64),
            "index projection wave released"
        );
        tracing::debug!(
            index.kind = ?kind,
            projection.outcome = if result.is_err() { "failed" } else { "completed" },
            monotonic_counter.keldra_index_projection_failures_total =
                u64::from(result.is_err()),
            monotonic_counter.keldra_index_projection_accepted_total = accepted,
            monotonic_counter.keldra_index_projection_skipped_total = skipped,
            histogram.keldra_index_projection_wave_duration_seconds = elapsed_seconds,
            histogram.keldra_index_projection_rayon_queue_seconds =
                result.as_ref().map_or(0.0, |totals| totals.queue_seconds),
            histogram.keldra_index_projection_cpu_seconds =
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

pub(super) struct PreparedProjection {
    pub(super) source: IndexSourceMutation,
    pub(super) projection_bytes: u64,
    pub(super) resident_bytes: u64,
    pub(super) needs_payload: bool,
}

impl PreparedProjection {
    pub(super) fn new(schema: &Schema, source: IndexSourceMutation) -> Result<Self, Status> {
        let projection_bytes = projection_admission_bytes(schema, &source).map_err(index_status)?;
        let needs_payload =
            source_needs_payload(schema) && matches!(source, IndexSourceMutation::Upsert(_));
        let payload_bytes = match &source {
            IndexSourceMutation::Upsert(object) if needs_payload => object.content_length,
            _ => 0,
        };
        // Inline reads temporarily retain the verified source and destination
        // buffers. Large reads retain an anonymous spool plus one bounded I/O
        // frame. Both charges belong to the already-held per-kind permit.
        let payload_reserve = if payload_bytes == 0 {
            0
        } else if payload_bytes <= keldra_store::SMALL_BLOB_MAX_BYTES as u64 {
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

pub(super) struct ProjectionBatch {
    pub(super) sources: Vec<PreparedProjection>,
    pub(super) resident_bytes: u64,
    max_projection_bytes: u64,
    budget: u64,
    max_lanes: usize,
}

impl ProjectionBatch {
    pub(super) fn new(budget: u64, max_lanes: usize) -> Self {
        Self {
            sources: Vec::new(),
            resident_bytes: 0,
            max_projection_bytes: 0,
            budget,
            max_lanes: max_lanes.max(1),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    pub(super) fn effective_lanes(&self) -> usize {
        self.sources.len().min(self.max_lanes).max(1)
    }

    pub(super) fn lane_limit(&self) -> Result<usize, Status> {
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

    pub(super) fn try_push(
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

pub(super) struct FetchedProjection {
    pub(super) source: IndexSourceMutation,
    pub(super) payload: Option<ClusterReadPayload>,
}

type ProjectedSource = Result<(MergeMutation, IndexBuildDiagnostics), IndexError>;

pub(super) async fn run_projection_lanes<T, O, F>(
    cpu: IndexCpuPool,
    lanes: Vec<Vec<T>>,
    senders: Vec<tokio::sync::mpsc::Sender<Result<O, IndexError>>>,
    work: F,
) -> Result<ProjectionExecution, Status>
where
    T: Send + 'static,
    O: Send + 'static,
    F: Fn(T) -> Result<O, IndexError> + Send + Sync + 'static,
{
    if lanes.len() != senders.len() {
        return Err(Status::internal(
            "projection lanes and result channels do not match",
        ));
    }
    let work = Arc::new(work);
    let mut tasks = tokio::task::JoinSet::new();
    for (lane, sender) in lanes.into_iter().zip(senders) {
        let cpu = cpu.clone();
        let work = Arc::clone(&work);
        tasks.spawn(async move {
            let mut execution = ProjectionExecution::default();
            for value in lane {
                let queued = Instant::now();
                let work = Arc::clone(&work);
                let (projected, queue_seconds, cpu_seconds) = cpu
                    .submit(move || {
                        let started = Instant::now();
                        let queue_seconds = started.saturating_duration_since(queued).as_secs_f64();
                        let projected = work(value);
                        let cpu_seconds = started.elapsed().as_secs_f64();
                        (projected, queue_seconds, cpu_seconds)
                    })
                    .await
                    .map_err(cpu_status)?;
                execution.queue_seconds += queue_seconds;
                execution.cpu_seconds += cpu_seconds;
                let failed = projected.is_err();
                // This wait is deliberately outside the Rayon job. A full
                // consumer channel cannot retain a worker required by a
                // builder spill or another nested CPU operation.
                if sender.send(projected).await.is_err() || failed {
                    break;
                }
            }
            Ok::<_, Status>(execution)
        });
    }

    let mut execution = ProjectionExecution::default();
    while let Some(result) = tasks.join_next().await {
        let lane = result
            .map_err(|error| Status::internal(format!("projection lane task failed: {error}")))??;
        execution.queue_seconds += lane.queue_seconds;
        execution.cpu_seconds += lane.cpu_seconds;
    }
    Ok(execution)
}

async fn project_snapshot_batch_inner(
    definition: &CatalogDefinition,
    barrier: &IndexBarrier,
    schema: &Schema,
    sources: Vec<PreparedProjection>,
    effective_lanes: usize,
    lane_limit: usize,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
) -> Result<ProjectionWaveTotals, Status> {
    if schema.kind == IndexKind::TypedJson {
        let family_plan = dependencies
            .projection_mapper
            .family_plan(definition.projection_family_identity())?
            .ok_or_else(|| Status::failed_precondition("projection family is not registered"))?;
        let sources = sources
            .into_iter()
            .map(|prepared| prepared.source)
            .collect();
        let projected = dependencies
            .projection_family_writer
            .advance_rebuild_visible_frame(
                &family_plan,
                sources,
                super::super::projection_family_writer::projection_barrier(barrier)?,
                lane_limit,
                DerivedArtifactAdmission::PublicationProgress,
            )
            .await?;
        let mut totals = ProjectionWaveTotals::default();
        totals.accepted = projected.diagnostics.accepted_objects;
        totals.skipped = projected.diagnostics.skipped_objects;
        candidate.diagnostics.add(projected.diagnostics);
        for mutation in projected.cache_mutations {
            let MergeMutation::Upsert(source) = mutation else {
                return Err(Status::data_loss(
                    "baseline projection emitted a stable cache tombstone",
                ));
            };
            push_or_flush(
                definition,
                runtime_kind(definition.schema.kind),
                builder,
                source,
                candidate,
                dependencies,
                true,
            )
            .await?;
        }
        return Ok(totals);
    }
    let fetched = fetch_projection_sources(sources, effective_lanes, dependencies).await?;
    let source_count = fetched.len();
    let lanes = partition_projection_lanes(fetched, effective_lanes);
    let mut senders = Vec::with_capacity(lanes.len());
    let mut receivers = Vec::with_capacity(lanes.len());
    for _ in 0..lanes.len() {
        let (sender, receiver) = tokio::sync::mpsc::channel::<ProjectedSource>(1);
        senders.push(sender);
        receivers.push(receiver);
    }
    let cpu = dependencies.cpu.clone();
    let projection_schema = schema.clone();
    let cpu_task = AbortOnDropTask::new(tokio::spawn(run_projection_lanes(
        cpu,
        lanes,
        senders,
        move |mut fetched: FetchedProjection| {
            let reader = fetched
                .payload
                .as_mut()
                .map(|payload| payload as &mut dyn std::io::Read);
            project_mutation(&projection_schema, fetched.source, reader, lane_limit)
        },
    )));

    let mut totals = ProjectionWaveTotals::default();
    let mut failure = None;
    for position in 0..source_count {
        let projected = match receive_ordered_lane_item(&mut receivers, position).await {
            Some(projected) => projected,
            None => {
                failure = Some(Status::internal("projection lane omitted a source"));
                break;
            }
        };
        match projected {
            Ok((mutation, diagnostics)) => {
                totals.accepted = totals.accepted.saturating_add(diagnostics.accepted_objects);
                totals.skipped = totals.skipped.saturating_add(diagnostics.skipped_objects);
                candidate.diagnostics.add(diagnostics);
                if let MergeMutation::Upsert(source) = mutation
                    && let Err(error) = push_or_flush(
                        definition,
                        runtime_kind(definition.schema.kind),
                        builder,
                        source,
                        candidate,
                        dependencies,
                        true,
                    )
                    .await
                {
                    failure = Some(error);
                    break;
                }
            }
            Err(error) => {
                failure = Some(index_status(error));
                break;
            }
        }
    }
    drop(receivers);
    let timing = cpu_task
        .join()
        .await
        .map_err(|error| Status::internal(format!("projection batch task failed: {error}")))??;
    if let Some(error) = failure {
        return Err(error);
    }
    totals.queue_seconds = timing.queue_seconds;
    totals.cpu_seconds = timing.cpu_seconds;
    Ok(totals)
}

pub(super) async fn receive_ordered_lane_item<T>(
    receivers: &mut [tokio::sync::mpsc::Receiver<T>],
    position: usize,
) -> Option<T> {
    let lane_count = receivers.len();
    if lane_count == 0 {
        return None;
    }
    let lane = position % lane_count;
    receivers[lane].recv().await
}

pub(super) async fn run_shared_projection_lanes(
    definition: CatalogDefinition,
    lanes: Vec<Vec<PreparedProjection>>,
    senders: Vec<tokio::sync::mpsc::Sender<Result<(MergeMutation, IndexBuildDiagnostics), Status>>>,
    lane_limit: usize,
    mapper: SharedProjectionMapper,
) -> Result<(), Status> {
    let mut tasks = tokio::task::JoinSet::new();
    for (lane, sender) in lanes.into_iter().zip(senders) {
        let mapper = mapper.clone();
        let definition = definition.clone();
        tasks.spawn(async move {
            for prepared in lane {
                let projected = mapper
                    .project(&definition, prepared.source, lane_limit)
                    .await;
                let failed = projected.is_err();
                if sender.send(projected).await.is_err() || failed {
                    break;
                }
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.map_err(|error| {
            Status::internal(format!("shared projection lane task failed: {error}"))
        })?;
    }
    Ok(())
}

pub(super) async fn fetch_projection_sources(
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
                    IndexSourceMutation::Upsert(object) => keldra_store::BlobRef {
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

pub(super) fn partition_projection_lanes<T>(values: Vec<T>, lane_count: usize) -> Vec<Vec<T>> {
    let lane_count = lane_count.min(values.len()).max(1);
    let mut lanes = (0..lane_count).map(|_| Vec::new()).collect::<Vec<_>>();
    for (position, value) in values.into_iter().enumerate() {
        lanes[position % lane_count].push(value);
    }
    lanes
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct SnapshotFrameMeasure {
    pub(super) encoded_bytes: u64,
    pub(super) resident_bytes: u64,
}

pub(super) fn measure_snapshot_frame(
    frame: &[IndexSourceSnapshotHead],
    frame_capacity: usize,
) -> Result<SnapshotFrameMeasure, Status> {
    if frame_capacity < frame.len() {
        return Err(Status::internal(
            "index snapshot frame capacity is below its length",
        ));
    }
    let mut counter = ByteCounter(0);
    let mut resident_bytes = std::mem::size_of::<Vec<IndexSourceSnapshotHead>>()
        .checked_add(
            frame_capacity
                .checked_mul(std::mem::size_of::<IndexSourceSnapshotHead>())
                .ok_or_else(|| Status::resource_exhausted("index snapshot resident overflow"))?,
        )
        .ok_or_else(|| Status::resource_exhausted("index snapshot resident overflow"))?;
    for head in frame {
        serde_json::to_writer(&mut counter, head)
            .map_err(|error| Status::internal(format!("measure index snapshot frame: {error}")))?;
        resident_bytes = resident_bytes
            .checked_add(head.exact_path.capacity())
            .and_then(|bytes| {
                bytes.checked_add(
                    head.version
                        .content_type
                        .as_ref()
                        .map_or(0, String::capacity),
                )
            })
            .and_then(|bytes| {
                head.alias_registry
                    .as_ref()
                    .map_or(Some(bytes), |registry| {
                        bytes
                            .checked_add(
                                registry
                                    .aliases
                                    .capacity()
                                    .checked_mul(std::mem::size_of::<String>())?,
                            )
                            .and_then(|bytes| {
                                registry.aliases.iter().try_fold(bytes, |bytes, alias| {
                                    bytes.checked_add(alias.capacity())
                                })
                            })
                    })
            })
            .ok_or_else(|| Status::resource_exhausted("index snapshot resident overflow"))?;
    }
    Ok(SnapshotFrameMeasure {
        encoded_bytes: counter.0,
        resident_bytes: u64::try_from(resident_bytes)
            .map_err(|_| Status::resource_exhausted("index snapshot resident exceeds u64"))?,
    })
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
    use keldra_store::{PlacementLogId, SourceId};

    use crate::index_runtime::events::{AtomicProgramWatermark, IndexSourceCursor};

    use super::*;

    #[test]
    fn stale_rebuild_root_does_not_match_replaced_definition() {
        let barrier = IndexBarrier {
            fence: PlacementLogId { term: 1, index: 2 },
            atomic: AtomicProgramWatermark::new(None, None, 0),
            sources: BTreeMap::from([(
                NodeId(1),
                IndexSourceCursor {
                    source: SourceId {
                        node_id: 1,
                        source_epoch: [3; 32],
                    },
                    next_offset: 1,
                },
            )]),
        };
        let candidate = IndexCommitManifest::new(
            7,
            9,
            4,
            keldra_index::v4::IndexKind::Path,
            [5; 32],
            &barrier,
            Vec::new(),
            None,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
            0,
        )
        .unwrap();
        let mut root = DurableRebuildRoot {
            index_id: 7,
            definition_version: 4,
            attempt_id: 9,
            baseline: barrier,
            last_canonical_path: None,
            baseline_complete: false,
            scanned_records: 0,
            scanned_bytes: 0,
            candidate,
        };
        assert!(!rebuild_root_requires_replacement(
            &root,
            4,
            keldra_index::v4::IndexKind::Path,
            [5; 32]
        ));
        assert!(rebuild_root_requires_replacement(
            &root,
            5,
            keldra_index::v4::IndexKind::Path,
            [5; 32]
        ));
        assert!(rebuild_root_requires_replacement(
            &root,
            4,
            keldra_index::v4::IndexKind::FullText,
            [5; 32]
        ));
        assert!(rebuild_root_requires_replacement(
            &root,
            4,
            keldra_index::v4::IndexKind::Path,
            [6; 32]
        ));
        let serving = root.candidate.clone();
        assert!(!serving_completes_rebuild(&root, &serving, 4));
        root.baseline_complete = true;
        assert!(serving_completes_rebuild(&root, &serving, 4));
        assert!(!serving_completes_rebuild(&root, &serving, 5));
    }

    #[test]
    fn resumed_scan_defers_post_baseline_heads_to_journal_replay() {
        assert!(skip_post_baseline_head(
            &Status::aborted("newer source position"),
            true
        ));
        assert!(skip_post_baseline_head(
            &Status::unavailable("atomic program not finalized at baseline"),
            true
        ));
        assert!(!skip_post_baseline_head(
            &Status::aborted("newer source position"),
            false
        ));
        assert!(!skip_post_baseline_head(
            &Status::data_loss("corrupt head"),
            true
        ));
    }

    fn prepared(index: usize, projection_bytes: u64, resident_bytes: u64) -> PreparedProjection {
        PreparedProjection {
            source: IndexSourceMutation::Remove(ObjectIdentity {
                path: format!("docs/{index}"),
                version: 1,
            }),
            projection_bytes,
            resident_bytes,
            needs_payload: false,
        }
    }

    fn locator_root(sequence: u64) -> LocatorRoot {
        let hash = [1; 32];
        LocatorRoot {
            sequence,
            identity: SegmentIdentity::new(1, 1, [1; 32], sequence).unwrap(),
            artifact: keldra_index::v4::ArtifactDescriptor::new(
                1,
                0,
                0,
                keldra_index::v4::COMPONENT_HEADER_BYTES as u64,
                0,
                keldra_index::v4::ComponentKind::ROUTING_NODE,
                1,
                [1; 32],
            )
            .unwrap(),
            pack_ownership: LocatorPackOwnership::Standalone(vec![
                keldra_index::v4::ArtifactPackReference::new(
                    1,
                    keldra_index::v4::artifact_path(1, hash),
                    1,
                    hash,
                    keldra_index::v4::COMPONENT_HEADER_BYTES as u64,
                )
                .unwrap(),
            ]),
            encoded_bytes: 1,
            logical_bytes: 1,
        }
    }

    #[test]
    fn rebuild_turn_repays_candidate_debt_before_consuming_another_snapshot_frame() {
        let clean = CandidateCommit::rebuild();
        assert_eq!(
            rebuild_turn_start(&clean, DebtLimits::new(1, u64::MAX)),
            RebuildTurnStart::ConsumeSnapshot
        );

        let mut indebted = CandidateCommit::rebuild();
        indebted.locator_roots = vec![locator_root(1), locator_root(2)];
        assert_eq!(
            rebuild_turn_start(&indebted, DebtLimits::new(1, u64::MAX)),
            RebuildTurnStart::RepayDebt
        );
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
            (0..11)
                .map(|position| lanes[position % lanes.len()][position / lanes.len()])
                .collect::<Vec<_>>(),
            (0_u64..11).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn projection_backpressure_releases_workers_needed_by_nested_builder_work() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let cpu = IndexCpuPool::new(2).unwrap();
        let lanes = partition_projection_lanes((0_u64..4).collect(), 2);
        let mut receivers = Vec::new();
        let mut senders = Vec::new();
        for _ in 0..lanes.len() {
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            senders.push(sender);
            receivers.push(receiver);
        }
        let projected = Arc::new(AtomicUsize::new(0));
        let projected_by_work = Arc::clone(&projected);
        let task = tokio::spawn(run_projection_lanes(
            cpu.clone(),
            lanes,
            senders,
            move |value| {
                projected_by_work.fetch_add(1, Ordering::Release);
                Ok::<_, IndexError>(value)
            },
        ));

        // Each lane has filled its one-result channel and computed another
        // result. Those second sends now exert backpressure. In the old
        // scheduler both Rayon workers blocked here in blocking_send.
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while projected.load(Ordering::Acquire) != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all projection work should finish before results are drained");

        let nested = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            cpu.install(|| "external-sort spill"),
        )
        .await
        .expect("result backpressure must not starve nested CPU work")
        .unwrap();
        assert_eq!(nested, "external-sort spill");

        let mut delivered = Vec::new();
        for position in 0..4 {
            delivered.push(
                receive_ordered_lane_item(&mut receivers, position)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(delivered, (0_u64..4).collect::<Vec<_>>());
        task.await.unwrap().unwrap();
    }
}
