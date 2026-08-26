//! Bounded exact-version journal catch-up.

use keldra_store::MAX_OBJECT_RECORD_EXPORT_RECORDS;

use crate::index_runtime::committed_view::MAX_PENDING_ATOMIC_BATCHES;
use crate::index_runtime::events::{
    IndexJournalChange, IndexSourceCursor, index_journal_change_encoded_len,
};

use super::rebuild::{
    FetchedProjection, PreparedProjection, ProjectionBatch, fetch_projection_sources,
    partition_projection_lanes, receive_ordered_lane_item, run_projection_lanes,
};
use super::*;

pub(super) struct JournalPageWork {
    pub(super) changed: bool,
    pub(super) source_payload_bytes: u64,
    pub(super) processed_records: u64,
    pub(super) processed_encoded_bytes: u64,
    pub(super) through: IndexBarrier,
    pub(super) first_changed_at: Option<Instant>,
    pub(super) atomic_pending: bool,
}

pub(super) struct AtomicProjectionWork {
    cursor: u64,
    bundle_hash: keldra_store::PreparedBundleHash,
    paths: Vec<(String, u64)>,
    next_path: usize,
    staged: CandidateCommit,
    builder: NativeSegmentBuild,
    plan: SegmentMemoryPlan,
    source_payload_bytes: u64,
    phase: AtomicProjectionPhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AtomicProjectionPhase {
    Project,
    Flush,
    Compact,
    Done,
}

const ORDINARY_MUTATION_MICROBATCH: usize = 64;

pub(super) fn locator_slots_reserved(
    candidate: &CandidateCommit,
    builder: &NativeSegmentBuild,
) -> usize {
    locator_slots_reserved_counts(
        candidate.locator_roots.len(),
        builder.frozen.is_some(),
        !builder.is_empty(),
    )
}

fn locator_slots_reserved_counts(committed: usize, frozen: bool, active: bool) -> usize {
    committed
        .saturating_add(usize::from(frozen))
        .saturating_add(usize::from(active))
}

pub(super) fn locator_publication_required(
    candidate: &CandidateCommit,
    builder: &NativeSegmentBuild,
) -> bool {
    debt::locator_headroom_requires_compaction(locator_slots_reserved(candidate, builder))
}

pub(super) fn published_candidate_requires_locator_maintenance(
    work: &CatchUpWork,
    published: &CommittedIndexView,
) -> Result<bool, Status> {
    Ok(
        debt::locator_headroom_requires_compaction(published.manifest.locator_roots.len())
            && work.through == published.manifest.barrier().map_err(commit_view_status)?
            && work.candidate.segments == published.manifest.segments
            && work.candidate.locator_roots == published.manifest.locator_roots
            && work.candidate.pending_atomic_batches == published.manifest.pending_atomic_batches
            && work
                .active
                .as_ref()
                .is_none_or(|active| active.builder.is_empty() && active.builder.frozen.is_none()),
    )
}

pub(super) async fn stop_at_locator_headroom(
    job: &BuilderJob,
    mut work: CatchUpWork,
    mut active: ActiveIncrementalBuffer,
    admission: DerivedArtifactAdmission,
    dependencies: &IndexBuilderDependencies,
) -> Result<(BuilderPhase, BuilderDisposition, Option<CommittedIndexView>), Status> {
    if work.publishing.is_none() {
        flush_builder(
            &job.definition,
            job.kind,
            &mut active.builder,
            &mut work.candidate,
            dependencies,
        )
        .await?;
        work.must_publish = true;
        enqueue_candidate_publication(job, &mut work, admission, dependencies).await?;
        // The sealed candidate owns no active builder state. Release its
        // working memory while commit admission is pending; a later turn
        // reacquires an exact permit only after publication completes.
        drop(active);
    } else {
        work.active = Some(active);
    }
    Ok((
        BuilderPhase::CatchUp(work),
        BuilderDisposition::Retry(Duration::from_millis(10)),
        None,
    ))
}

pub(super) const fn should_compact_before_catch_up(
    maintenance: bool,
    segment_count: usize,
    locator_root_count: usize,
) -> bool {
    maintenance
        || segment_count >= MAX_SEGMENTS_PER_COMMIT.saturating_sub(1)
        || debt::locator_headroom_requires_compaction(locator_root_count)
}

pub(super) fn record_source_page_progress(work: &mut CatchUpWork, through: &IndexBarrier) {
    // Persist even zero-effect routed pages so restart cannot forget a proven
    // checkpoint and freshness does not wait on already-inspected source work.
    if work.through != *through {
        work.must_publish = true;
        work.checkpoint_started.get_or_insert_with(Instant::now);
    }
    work.through = through.clone();
    work.candidate
        .prune_finalized_atomic_batches(work.through.atomic.finalized_through());
}

pub(super) fn earliest_publication_start(
    mutation_started: Option<Instant>,
    checkpoint_started: Option<Instant>,
) -> Option<Instant> {
    match (mutation_started, checkpoint_started) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(started), None) | (None, Some(started)) => Some(started),
        (None, None) => None,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn process_journal_page(
    definition: &CatalogDefinition,
    kind: IndexKind,
    from: &IndexBarrier,
    page: &IndexJournalPage,
    plan: SegmentMemoryPlan,
    atomic_plan: SegmentMemoryPlan,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateCommit,
    atomic_projection: &mut Option<AtomicProjectionWork>,
    dependencies: &IndexBuilderDependencies,
    mut deadline: Option<Instant>,
    maximum_age: Duration,
) -> Result<JournalPageWork, Status> {
    let mut changed = false;
    let mut first_changed_at = None;
    let mut source_payload_bytes = 0_u64;
    let mut position = 0usize;
    let mut processed_encoded_bytes = 0_u64;
    while position < page.changes.len() {
        let start = position;
        let atomic = matches!(
            page.changes[position].change,
            LocalChange::AtomicBatchPublished(_)
        );
        let end = if atomic {
            position + 1
        } else {
            let available_roots = MAX_LOCATOR_ROOTS_PER_COMMIT
                .saturating_sub(1)
                .saturating_sub(locator_slots_reserved(candidate, builder))
                .max(1);
            page.changes[position..]
                .iter()
                .position(|entry| matches!(entry.change, LocalChange::AtomicBatchPublished(_)))
                .map_or(page.changes.len(), |relative| position + relative)
                .min(position.saturating_add(ORDINARY_MUTATION_MICROBATCH.min(available_roots)))
        };
        if let LocalChange::AtomicBatchPublished(batch) = &page.changes[position].change {
            if !atomic_batch_already_materialized(from, candidate, batch)? {
                if candidate.pending_atomic_batches.len() >= MAX_PENDING_ATOMIC_BATCHES {
                    return Err(Status::resource_exhausted(
                        "pending atomic batch identity bound reached",
                    ));
                }
                let has_paths = batch.mutations.iter().any(|mutation| {
                    mutation.tenant_id == definition.tenant_id
                        && mutation.bucket_id == definition.bucket_id
                        && path_matches_prefix(&mutation.exact_path, &definition.stored.path_prefix)
                        && !contains_reserved_segment(&mutation.exact_path)
                });
                if !has_paths {
                    candidate.record_atomic_batch(batch.cursor, batch.bundle_hash)?;
                    position = end;
                    processed_encoded_bytes = processed_encoded_bytes
                        .checked_add(processed_journal_encoded_bytes(&page.changes[start..end])?)
                        .ok_or_else(|| {
                            Status::resource_exhausted("processed journal bytes overflow")
                        })?;
                    continue;
                }
                if atomic_projection.is_none() {
                    let overhead = atomic_staging_overhead_bytes(definition, batch, candidate)?;
                    let unit_plan = charge_atomic_staging_overhead(atomic_plan, overhead)?;
                    let paths = atomic_source_paths(
                        definition.tenant_id,
                        definition.bucket_id,
                        &definition.stored.path_prefix,
                        batch,
                    );
                    *atomic_projection = Some(
                        start_atomic_projection(
                            definition,
                            kind,
                            unit_plan,
                            paths,
                            batch,
                            builder,
                            candidate,
                            dependencies,
                        )
                        .await?,
                    );
                }
                let projection = atomic_projection.as_mut().expect("atomic work installed");
                if projection.cursor != batch.cursor || projection.bundle_hash != batch.bundle_hash
                {
                    return Err(Status::data_loss(
                        "pending atomic projection differs from replayed journal event",
                    ));
                }
                let changed_at = Instant::now();
                first_changed_at.get_or_insert(changed_at);
                deadline.get_or_insert(changed_at + maximum_age);
                let advance =
                    advance_atomic_projection(definition, kind, projection, dependencies).await;
                if let Err(error) = advance {
                    if transient_atomic_projection_error(&error) {
                        tracing::debug!(%error, "retrying retained atomic projection substep");
                        return Ok(JournalPageWork {
                            changed,
                            source_payload_bytes,
                            processed_records: position as u64,
                            processed_encoded_bytes,
                            through: barrier_after_changes(from, &page.changes[..position])?,
                            first_changed_at,
                            atomic_pending: true,
                        });
                    }
                    return Err(error);
                }
                if projection.phase != AtomicProjectionPhase::Done {
                    return Ok(JournalPageWork {
                        changed,
                        source_payload_bytes,
                        processed_records: position as u64,
                        processed_encoded_bytes,
                        through: barrier_after_changes(from, &page.changes[..position])?,
                        first_changed_at,
                        atomic_pending: true,
                    });
                }
                let completed = atomic_projection
                    .take()
                    .expect("completed atomic work installed");
                source_payload_bytes = source_payload_bytes
                    .checked_add(completed.source_payload_bytes)
                    .ok_or_else(|| Status::resource_exhausted("index source payload overflow"))?;
                *candidate = completed.staged;
                changed = true;
            }
        } else {
            let paths = ordinary_journal_source_paths(
                definition.tenant_id,
                definition.bucket_id,
                &definition.stored.path_prefix,
                &page.changes[position..end],
            );
            if !paths.is_empty() {
                let changed_at = Instant::now();
                first_changed_at.get_or_insert(changed_at);
                deadline.get_or_insert(changed_at + maximum_age);
            }
            changed |= !paths.is_empty();
            for paths in paths.chunks(MAX_OBJECT_RECORD_EXPORT_RECORDS as usize) {
                let sources = load_exact_sources(definition, paths, dependencies).await?;
                source_payload_bytes =
                    add_source_payload_bytes(source_payload_bytes, &definition.schema, &sources)?;
                project_sources(
                    definition,
                    kind,
                    plan,
                    sources,
                    builder,
                    candidate,
                    dependencies,
                    true,
                )
                .await?;
            }
        }
        position = end;
        processed_encoded_bytes = processed_encoded_bytes
            .checked_add(processed_journal_encoded_bytes(&page.changes[start..end])?)
            .ok_or_else(|| Status::resource_exhausted("processed journal bytes overflow"))?;
        if locator_publication_required(candidate, builder)
            || deadline.is_some_and(|deadline| Instant::now() >= deadline)
        {
            break;
        }
    }
    let complete = position == page.changes.len();
    let through = if complete {
        page.through.clone()
    } else {
        barrier_after_changes(from, &page.changes[..position])?
    };
    Ok(JournalPageWork {
        changed,
        source_payload_bytes,
        processed_records: position as u64,
        processed_encoded_bytes,
        through,
        first_changed_at,
        atomic_pending: false,
    })
}

/// Projects an atomic journal event into isolated candidate state. Immutable
/// artifacts written by an unsuccessful attempt are intentionally unattached;
/// the orphan scrub reclaims them after its safety horizon.
#[allow(clippy::too_many_arguments)]
async fn start_atomic_projection(
    definition: &CatalogDefinition,
    kind: IndexKind,
    plan: SegmentMemoryPlan,
    paths: Vec<(String, u64)>,
    batch: &keldra_store::AtomicBatchPublished,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
) -> Result<AtomicProjectionWork, Status> {
    // Ordinary work preceding the atomic unit is complete before we establish
    // the base graph. Nothing from the atomic event can attach to that graph
    // until its complete exact output is known to fit.
    flush_builder(definition, kind, builder, candidate, dependencies).await?;
    let base_segments = candidate.segments.len();
    let staged = candidate.clone();
    // Atomic ingress admits at most one commit's segment-producing source
    // count. The staging writer may cross the current manifest ceiling so
    // exact effects can be measured, but remains finitely bounded by that
    // authoritative unit limit.
    let staging_segment_limit = base_segments
        .checked_add(MAX_SEGMENTS_PER_COMMIT)
        .ok_or_else(|| Status::resource_exhausted("atomic staging segment bound overflow"))?;
    let staged_builder = NativeSegmentBuild::open_with_segment_limit(
        definition,
        plan,
        builder.publication_lane,
        staging_segment_limit,
        dependencies,
    )?;
    Ok(AtomicProjectionWork {
        cursor: batch.cursor,
        bundle_hash: batch.bundle_hash,
        paths,
        next_path: 0,
        staged,
        builder: staged_builder,
        plan,
        source_payload_bytes: 0,
        phase: AtomicProjectionPhase::Project,
    })
}

async fn advance_atomic_projection(
    definition: &CatalogDefinition,
    kind: IndexKind,
    work: &mut AtomicProjectionWork,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    match work.phase {
        AtomicProjectionPhase::Project => {
            let end = work
                .next_path
                .saturating_add(MAX_OBJECT_RECORD_EXPORT_RECORDS as usize)
                .min(work.paths.len());
            if work.next_path == end {
                work.phase = AtomicProjectionPhase::Flush;
                return Ok(());
            }
            let sources =
                load_exact_sources(definition, &work.paths[work.next_path..end], dependencies)
                    .await?;
            let payload = add_source_payload_bytes(0, &definition.schema, &sources)?;
            project_sources(
                definition,
                kind,
                work.plan,
                sources,
                &mut work.builder,
                &mut work.staged,
                dependencies,
                false,
            )
            .await?;
            work.source_payload_bytes = work
                .source_payload_bytes
                .checked_add(payload)
                .ok_or_else(|| Status::resource_exhausted("atomic source payload overflow"))?;
            work.next_path = end;
            if end == work.paths.len() {
                work.phase = AtomicProjectionPhase::Flush;
            }
        }
        AtomicProjectionPhase::Flush => {
            flush_builder(
                definition,
                kind,
                &mut work.builder,
                &mut work.staged,
                dependencies,
            )
            .await?;
            work.staged
                .record_atomic_batch(work.cursor, work.bundle_hash)?;
            work.phase = AtomicProjectionPhase::Compact;
        }
        AtomicProjectionPhase::Compact => {
            if atomic_manifest_fits(&work.staged) {
                work.phase = AtomicProjectionPhase::Done;
            } else if !compact_atomic_staged_once(
                definition,
                kind,
                work.plan,
                &mut work.staged,
                dependencies,
            )
            .await?
            {
                return Err(atomic_manifest_capacity_error(
                    work.staged.segments.len(),
                    work.staged.locator_roots.len(),
                ));
            }
        }
        AtomicProjectionPhase::Done => {}
    }
    Ok(())
}

fn transient_atomic_projection_error(error: &Status) -> bool {
    matches!(
        error.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
    )
}

fn atomic_staging_overhead_bytes(
    definition: &CatalogDefinition,
    batch: &keldra_store::AtomicBatchPublished,
    candidate: &CandidateCommit,
) -> Result<usize, Status> {
    let mut relevant_count = 0usize;
    let mut path_dynamic_bytes = 0usize;
    let mut key_chunk_count = 0usize;
    let mut key_chunk_dynamic_bytes = 0usize;
    let mut maximum_key_bytes = 0usize;
    for mutation in &batch.mutations {
        if mutation.tenant_id != definition.tenant_id
            || mutation.bucket_id != definition.bucket_id
            || !path_matches_prefix(&mutation.exact_path, &definition.stored.path_prefix)
            || contains_reserved_segment(&mutation.exact_path)
        {
            continue;
        }
        relevant_count = relevant_count
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("atomic path count overflow"))?;
        path_dynamic_bytes = path_dynamic_bytes
            .checked_add(mutation.exact_path.len())
            .ok_or_else(|| Status::resource_exhausted("atomic path size overflow"))?;
        key_chunk_count += 1;
        key_chunk_dynamic_bytes = key_chunk_dynamic_bytes
            .checked_add(definition.stored.tenant.len())
            .and_then(|bytes| bytes.checked_add(definition.stored.bucket.len()))
            .and_then(|bytes| bytes.checked_add(mutation.exact_path.len()))
            .ok_or_else(|| Status::resource_exhausted("atomic source-key size overflow"))?;
        if key_chunk_count == MAX_OBJECT_RECORD_EXPORT_RECORDS as usize {
            maximum_key_bytes = maximum_key_bytes.max(key_chunk_resident_bytes(
                key_chunk_count,
                key_chunk_dynamic_bytes,
            )?);
            key_chunk_count = 0;
            key_chunk_dynamic_bytes = 0;
        }
    }
    maximum_key_bytes = maximum_key_bytes.max(key_chunk_resident_bytes(
        key_chunk_count,
        key_chunk_dynamic_bytes,
    )?);
    let path_bytes = std::mem::size_of::<Vec<(String, u64)>>()
        .checked_add(
            relevant_count
                .checked_mul(std::mem::size_of::<(String, u64)>())
                .ok_or_else(|| Status::resource_exhausted("atomic path size overflow"))?,
        )
        .and_then(|bytes| bytes.checked_add(path_dynamic_bytes))
        .ok_or_else(|| Status::resource_exhausted("atomic path size overflow"))?;
    candidate
        .clone_resident_bytes()?
        .checked_add(path_bytes)
        .and_then(|bytes| bytes.checked_add(maximum_key_bytes))
        .ok_or_else(|| Status::resource_exhausted("atomic staging size overflow"))
}

fn key_chunk_resident_bytes(count: usize, dynamic_bytes: usize) -> Result<usize, Status> {
    std::mem::size_of::<Vec<ObjectKey>>()
        .checked_add(
            count
                .checked_mul(std::mem::size_of::<ObjectKey>())
                .ok_or_else(|| Status::resource_exhausted("atomic source-key size overflow"))?,
        )
        .and_then(|bytes| bytes.checked_add(dynamic_bytes))
        .ok_or_else(|| Status::resource_exhausted("atomic source-key size overflow"))
}

fn charge_atomic_staging_overhead(
    plan: SegmentMemoryPlan,
    overhead: usize,
) -> Result<SegmentMemoryPlan, Status> {
    let total_bytes = plan.total_bytes.checked_sub(overhead).ok_or_else(|| {
        Status::resource_exhausted("atomic staging metadata exhausts its global memory permit")
    })?;
    let max_source_projection_bytes = plan
        .max_source_projection_bytes
        .checked_sub(overhead)
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| {
            Status::resource_exhausted(
                "atomic staging metadata leaves no bounded projection workspace",
            )
        })?;
    if total_bytes < MIN_INDEX_KIND_MEMORY_BYTES {
        return Err(Status::resource_exhausted(
            "atomic staging metadata leaves no minimum builder workspace",
        ));
    }
    Ok(SegmentMemoryPlan {
        total_bytes,
        max_resident_bytes: plan.max_resident_bytes,
        max_source_projection_bytes,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AtomicManifestEffects {
    output_segments: usize,
    output_roots: usize,
    total_segments: usize,
    total_roots: usize,
}

impl AtomicManifestEffects {
    fn between(
        base_segments: usize,
        base_roots: usize,
        total_segments: usize,
        total_roots: usize,
    ) -> Result<Self, Status> {
        Ok(Self {
            output_segments: total_segments.checked_sub(base_segments).ok_or_else(|| {
                Status::internal("atomic projection removed base commit segments")
            })?,
            output_roots: total_roots
                .checked_sub(base_roots)
                .ok_or_else(|| Status::internal("atomic projection removed base locator roots"))?,
            total_segments,
            total_roots,
        })
    }

    const fn fits(self) -> bool {
        self.total_segments <= MAX_SEGMENTS_PER_COMMIT
            && self.total_roots <= MAX_LOCATOR_ROOTS_PER_COMMIT
    }
}

fn atomic_manifest_fits(candidate: &CandidateCommit) -> bool {
    candidate.segments.len() <= MAX_SEGMENTS_PER_COMMIT
        && candidate.locator_roots.len() <= MAX_LOCATOR_ROOTS_PER_COMMIT
}

fn atomic_manifest_capacity_error(segments: usize, roots: usize) -> Status {
    Status::resource_exhausted(format!(
        "one atomic batch cannot be compacted into a legal format-v4 commit: {segments} segments, {roots} locator roots"
    ))
}

async fn compact_atomic_staged_once(
    definition: &CatalogDefinition,
    kind: IndexKind,
    plan: SegmentMemoryPlan,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
) -> Result<bool, Status> {
    let segment_overflow = candidate.segments.len() > MAX_SEGMENTS_PER_COMMIT;
    if segment_overflow {
        let Some(mut selection) = debt::select(&candidate.segments, DebtLimits::new(1, u64::MAX))
        else {
            return Ok(false);
        };
        // Reclaim exactly one segment slot, then measure the replayed atomic
        // output again. Wider maintenance merges stay off the journal path.
        selection.input_segments = 2;
        let _publication_slot = dependencies.publication_slots.acquire_maintenance().await?;
        compact_tier(
            definition,
            kind,
            selection,
            plan.total_bytes as u64,
            compaction_admission(),
            candidate,
            dependencies,
        )
        .await?;
        return Ok(true);
    }
    let Some(mut selection) =
        debt::select_locator_roots(&candidate.locator_roots, DebtLimits::new(1, u64::MAX))
    else {
        return Ok(false);
    };
    // The oldest two roots are the minimum legal prefix compaction.
    selection.input_roots = 2;
    let _publication_slot = dependencies.publication_slots.acquire_maintenance().await?;
    locator_debt::compact_oldest_prefix(
        definition,
        kind,
        selection,
        compaction_admission(),
        candidate,
        dependencies,
    )
    .await?;
    Ok(true)
}

fn atomic_batch_already_materialized(
    from: &IndexBarrier,
    candidate: &CandidateCommit,
    batch: &keldra_store::AtomicBatchPublished,
) -> Result<bool, Status> {
    // Consult the pending identity first so a conflicting hash remains
    // corruption even after the finalized watermark covers this cursor.
    if candidate.contains_atomic_batch(batch.cursor, batch.bundle_hash)? {
        return Ok(true);
    }
    Ok(from
        .atomic
        .finalized_through()
        .is_some_and(|finalized| batch.cursor <= finalized))
}

fn processed_journal_encoded_bytes(changes: &[IndexJournalChange]) -> Result<u64, Status> {
    changes.iter().try_fold(0_u64, |total, change| {
        total
            .checked_add(index_journal_change_encoded_len(change).map_err(event_status)?)
            .ok_or_else(|| Status::resource_exhausted("processed journal bytes overflow"))
    })
}

fn add_source_payload_bytes(
    initial: u64,
    schema: &Schema,
    sources: &[IndexSourceMutation],
) -> Result<u64, Status> {
    sources.iter().try_fold(initial, |total, source| {
        total
            .checked_add(source_payload_bytes_for(schema, source))
            .ok_or_else(|| Status::resource_exhausted("index source payload bytes overflow"))
    })
}

fn barrier_after_changes(
    from: &IndexBarrier,
    entries: &[IndexJournalChange],
) -> Result<IndexBarrier, Status> {
    let mut through = from.clone();
    for entry in entries {
        let cursor = through
            .sources
            .get_mut(&entry.node)
            .ok_or_else(|| Status::data_loss("journal page names an unknown source node"))?;
        cursor.next_offset = entry
            .change
            .offset()
            .checked_add(1)
            .ok_or_else(|| Status::data_loss("journal change offset overflow"))?;
    }
    Ok(through)
}

fn ordinary_journal_source_paths(
    tenant_id: u64,
    bucket_id: u64,
    path_prefix: &str,
    changes: &[IndexJournalChange],
) -> Vec<(String, u64)> {
    let mut paths = BTreeMap::<String, u64>::new();
    for entry in changes {
        let (change_tenant_id, change_bucket_id, path, version) = match &entry.change {
            LocalChange::ObjectHead(change) if change.program_commit_cursor.is_none() => (
                change.tenant_id,
                change.bucket_id,
                &change.exact_path,
                change.path_version.0,
            ),
            LocalChange::RetainedVersionDeleted(change)
                if change.resulting_head_version.is_some() =>
            {
                (
                    change.tenant_id,
                    change.bucket_id,
                    &change.exact_path,
                    change.resulting_head_version.unwrap().0,
                )
            }
            _ => continue,
        };
        if change_tenant_id == tenant_id
            && change_bucket_id == bucket_id
            && path_matches_prefix(path, path_prefix)
            && !contains_reserved_segment(path)
        {
            // A source journal, not the numeric VersionId, is the ordering
            // authority. Repeated mutations to one path coalesce to the last
            // record in this exact processed interval.
            paths.insert(path.clone(), version);
        }
    }
    paths.into_iter().collect()
}

fn atomic_source_paths(
    tenant_id: u64,
    bucket_id: u64,
    path_prefix: &str,
    batch: &keldra_store::AtomicBatchPublished,
) -> Vec<(String, u64)> {
    // Atomic mutation descriptors are canonically sorted at authoritative
    // ingress. Preserve that order and coalesce adjacent duplicates without a
    // second tree allocation.
    let relevant_count = batch
        .mutations
        .iter()
        .filter(|mutation| {
            mutation.tenant_id == tenant_id
                && mutation.bucket_id == bucket_id
                && path_matches_prefix(&mutation.exact_path, path_prefix)
                && !contains_reserved_segment(&mutation.exact_path)
        })
        .count();
    let mut paths = Vec::<(String, u64)>::with_capacity(relevant_count);
    for mutation in &batch.mutations {
        if mutation.tenant_id == tenant_id
            && mutation.bucket_id == bucket_id
            && path_matches_prefix(&mutation.exact_path, path_prefix)
            && !contains_reserved_segment(&mutation.exact_path)
        {
            if let Some((last_path, selected)) = paths.last_mut()
                && last_path == &mutation.exact_path
            {
                *selected = (*selected).max(mutation.path_version.0);
            } else {
                paths.push((mutation.exact_path.clone(), mutation.path_version.0));
            }
        }
    }
    paths
}

#[cfg(test)]
pub(super) fn journal_source_paths(
    tenant_id: u64,
    bucket_id: u64,
    path_prefix: &str,
    page: &IndexJournalPage,
) -> BTreeMap<String, u64> {
    let mut paths = BTreeMap::<String, u64>::new();
    for entry in &page.changes {
        let (change_tenant_id, change_bucket_id, path, version) = match &entry.change {
            LocalChange::ObjectHead(change) if change.program_commit_cursor.is_none() => (
                change.tenant_id,
                change.bucket_id,
                &change.exact_path,
                change.path_version.0,
            ),
            LocalChange::RetainedVersionDeleted(change)
                if change.resulting_head_version.is_some() =>
            {
                (
                    change.tenant_id,
                    change.bucket_id,
                    &change.exact_path,
                    change.resulting_head_version.unwrap().0,
                )
            }
            LocalChange::AtomicBatchPublished(batch) => {
                for mutation in &batch.mutations {
                    if mutation.tenant_id == tenant_id
                        && mutation.bucket_id == bucket_id
                        && path_matches_prefix(&mutation.exact_path, path_prefix)
                        && !contains_reserved_segment(&mutation.exact_path)
                    {
                        paths
                            .entry(mutation.exact_path.clone())
                            .and_modify(|selected| {
                                *selected = (*selected).max(mutation.path_version.0)
                            })
                            .or_insert(mutation.path_version.0);
                    }
                }
                continue;
            }
            LocalChange::AggregateChanged(_) | LocalChange::ContentLifecycleChanged(_) => continue,
            _ => continue,
        };
        if change_tenant_id == tenant_id
            && change_bucket_id == bucket_id
            && path_matches_prefix(path, path_prefix)
            && !contains_reserved_segment(path)
        {
            paths.insert(path.clone(), version);
        }
    }
    paths
}

/// Concrete heap-resident bytes retained by one decoded journal page while
/// its ordered source mutations are projected. Fixed-size fields live in the
/// vector/map node charges; every variable-capacity field is added explicitly.
pub(super) fn journal_page_resident_bytes(page: &IndexJournalPage) -> Result<u64, Status> {
    let mut bytes = std::mem::size_of::<IndexJournalPage>()
        .checked_add(
            page.changes
                .capacity()
                .checked_mul(std::mem::size_of::<IndexJournalChange>())
                .ok_or_else(|| Status::resource_exhausted("journal page resident overflow"))?,
        )
        .and_then(|bytes| {
            bytes.checked_add(page.through.sources.len().checked_mul(
                std::mem::size_of::<(NodeId, IndexSourceCursor)>()
                    + 3 * std::mem::size_of::<usize>(),
            )?)
        })
        .ok_or_else(|| Status::resource_exhausted("journal page resident overflow"))?;
    for entry in &page.changes {
        let dynamic = match &entry.change {
            LocalChange::ObjectHead(change) => change
                .exact_path
                .capacity()
                .checked_add(
                    change
                        .reference_deltas
                        .capacity()
                        .checked_mul(std::mem::size_of::<keldra_store::ReferenceDelta>())
                        .ok_or_else(|| {
                            Status::resource_exhausted("journal page resident overflow")
                        })?,
                )
                .and_then(|bytes| {
                    bytes.checked_add(
                        change
                            .definition_transition
                            .as_ref()
                            .map_or(0, |transition| transition.path.capacity()),
                    )
                }),
            LocalChange::RetainedVersionDeleted(change) => {
                change.exact_path.capacity().checked_add(
                    change
                        .reference_deltas
                        .capacity()
                        .checked_mul(std::mem::size_of::<keldra_store::ReferenceDelta>())
                        .ok_or_else(|| {
                            Status::resource_exhausted("journal page resident overflow")
                        })?,
                )
            }
            LocalChange::AggregateChanged(change) => Some(change.aggregate_key.capacity()),
            LocalChange::ContentLifecycleChanged(change) => {
                change.blob_identity.capacity().checked_add(
                    change
                        .reference_deltas
                        .capacity()
                        .checked_mul(std::mem::size_of::<keldra_store::ReferenceDelta>())
                        .ok_or_else(|| {
                            Status::resource_exhausted("journal page resident overflow")
                        })?,
                )
            }
            LocalChange::AtomicBatchPublished(change) => {
                let routes = change
                    .affected_routes
                    .capacity()
                    .checked_mul(std::mem::size_of::<keldra_store::AtomicBatchRoute>());
                let mutations = change
                    .mutations
                    .capacity()
                    .checked_mul(std::mem::size_of::<keldra_store::AtomicBatchMutation>());
                routes
                    .zip(mutations)
                    .and_then(|(routes, mutations)| routes.checked_add(mutations))
                    .and_then(|fixed| {
                        change.mutations.iter().try_fold(fixed, |bytes, mutation| {
                            bytes.checked_add(mutation.exact_path.capacity())
                        })
                    })
            }
            _ => Some(0),
        }
        .ok_or_else(|| Status::resource_exhausted("journal page resident overflow"))?;
        bytes = bytes
            .checked_add(dynamic)
            .ok_or_else(|| Status::resource_exhausted("journal page resident overflow"))?;
    }
    u64::try_from(bytes)
        .map_err(|_| Status::resource_exhausted("journal page resident exceeds u64"))
}

async fn load_exact_sources(
    definition: &CatalogDefinition,
    paths: &[(String, u64)],
    dependencies: &IndexBuilderDependencies,
) -> Result<Vec<IndexSourceMutation>, Status> {
    let keys = paths
        .iter()
        .map(|(path, _)| {
            ObjectKey::new(&definition.stored.tenant, &definition.stored.bucket, path)
                .map_err(|error| Status::internal(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let versions = paths
        .iter()
        .map(|(_, version)| VersionId(*version))
        .collect::<Vec<_>>();
    let selected = dependencies
        .reader
        .exact_versions_stable(&keys, &versions, definition.tenant_id, definition.bucket_id)
        .await?;
    paths
        .iter()
        .zip(selected)
        .map(|((path, expected_version), version)| {
            exact_source(definition, path, *expected_version, version)
        })
        .collect()
}

pub(super) fn exact_source(
    definition: &CatalogDefinition,
    path: &str,
    expected_version: u64,
    version: Option<keldra_store::Version>,
) -> Result<IndexSourceMutation, Status> {
    let version = version.ok_or_else(|| {
        Status::data_loss(format!(
            "source journal names missing immutable version {expected_version} for {path}"
        ))
    })?;
    if version.id.0 != expected_version {
        return Err(Status::data_loss(
            "exact source read returned another version identity",
        ));
    }
    if version.deleted
        || !source_matches_definition(&definition.stored, path, version.content_type.as_deref())
    {
        return Ok(IndexSourceMutation::Remove(ObjectIdentity {
            path: path.to_owned(),
            version: expected_version,
        }));
    }
    Ok(IndexSourceMutation::Upsert(build_object(path, &version)?))
}

#[allow(clippy::too_many_arguments)]
async fn project_sources(
    definition: &CatalogDefinition,
    kind: IndexKind,
    plan: SegmentMemoryPlan,
    sources: Vec<IndexSourceMutation>,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
    soft_flush_allowed: bool,
) -> Result<(), Status> {
    let configured_lanes = usize::try_from(dependencies.config.projection_max_lanes(kind))
        .map_err(|_| Status::resource_exhausted("projection lane limit exceeds platform"))?;
    let max_lanes = configured_lanes.min(dependencies.cpu.workers()).max(1);
    let projection_budget = plan.max_source_projection_bytes as u64;
    let mut batch = ProjectionBatch::new(projection_budget, max_lanes);
    for source in sources {
        let prepared = PreparedProjection::new(&definition.schema, source)?;
        if let Some(pending) = batch.try_push(prepared)? {
            let full = std::mem::replace(
                &mut batch,
                ProjectionBatch::new(projection_budget, max_lanes),
            );
            project_catch_up_batch(
                definition,
                kind,
                plan,
                full,
                builder,
                candidate,
                dependencies,
                soft_flush_allowed,
            )
            .await?;
            if batch.try_push(pending)?.is_some() {
                return Err(Status::internal(
                    "catch-up projection source was rejected by an empty batch after admission",
                ));
            }
        }
    }
    if !batch.is_empty() {
        project_catch_up_batch(
            definition,
            kind,
            plan,
            batch,
            builder,
            candidate,
            dependencies,
            soft_flush_allowed,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn project_catch_up_batch(
    definition: &CatalogDefinition,
    kind: IndexKind,
    plan: SegmentMemoryPlan,
    batch: ProjectionBatch,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
    soft_flush_allowed: bool,
) -> Result<(), Status> {
    let effective_lanes = batch.effective_lanes();
    let lane_limit = batch.lane_limit()?;
    let fetched = fetch_projection_sources(batch.sources, effective_lanes, dependencies).await?;
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
    let projection_schema = definition.schema.clone();
    let cpu_task = tokio::spawn(run_projection_lanes(
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
    ));

    let mut failure = None;
    let mut mutations = Vec::with_capacity(source_count);
    for position in 0..source_count {
        let projected = match receive_ordered_lane_item(&mut receivers, position).await {
            Some(projected) => projected,
            None => {
                failure = Some(Status::internal(
                    "catch-up projection lane omitted a source",
                ));
                break;
            }
        };
        match projected {
            Ok((mutation, diagnostics)) => {
                candidate.diagnostics.add(diagnostics);
                mutations.push(mutation);
            }
            Err(error) => {
                failure = Some(index_status(error));
                break;
            }
        }
    }
    drop(receivers);
    cpu_task
        .await
        .map_err(|error| Status::internal(format!("catch-up projection task failed: {error}")))??;
    if let Some(error) = failure {
        return Err(error);
    }
    apply_incremental_mutations(
        definition,
        kind,
        plan,
        builder,
        mutations,
        candidate,
        dependencies,
        soft_flush_allowed,
    )
    .await
}

async fn apply_incremental_mutations(
    definition: &CatalogDefinition,
    kind: IndexKind,
    plan: SegmentMemoryPlan,
    builder: &mut NativeSegmentBuild,
    mutations: Vec<MergeMutation>,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
    soft_flush_allowed: bool,
) -> Result<(), Status> {
    if mutations.windows(2).any(|pair| {
        mutation_identity(&pair[0]).path.as_str() >= mutation_identity(&pair[1]).path.as_str()
    }) {
        return Err(Status::data_loss(
            "catch-up projection did not preserve its sorted unique source order",
        ));
    }
    let mut pending = mutations;

    // A detached segment may encode and flush while later disjoint sources are
    // projected into the replacement active buffer. Join only when this batch
    // touches an identity owned by that frozen buffer; otherwise the prior
    // committed locator roots are sufficient and journal intake keeps moving.
    let conflicts_with_frozen = builder.frozen.as_ref().is_some_and(|frozen| {
        pending.iter().any(|mutation| {
            frozen
                .source_paths
                .contains_key(&mutation_identity(mutation).path)
        })
    });
    if conflicts_with_frozen {
        finish_frozen_segment(kind, builder, candidate).await?;
    }

    let conflicts_with_open_segment = pending.iter().any(|mutation| {
        let identity = mutation_identity(mutation);
        builder
            .writer
            .source_version(&identity.path)
            .is_some_and(|version| {
                !matches!(mutation, MergeMutation::Upsert(_)) || version != identity.version
            })
    });
    if conflicts_with_open_segment {
        flush_builder(definition, kind, builder, candidate, dependencies).await?;
    }
    pending.retain(|mutation| {
        let identity = mutation_identity(mutation);
        !matches!(mutation, MergeMutation::Upsert(_))
            || builder.writer.source_version(&identity.path) != Some(identity.version)
    });
    if pending.is_empty() {
        return Ok(());
    }

    let directory = ManifestArtifactDirectory::new(
        dependencies.cache.clone(),
        dependencies.reader.clone(),
        definition.stored.tenant.clone(),
        definition.stored.bucket.clone(),
        definition.tenant_id,
        definition.bucket_id,
        definition.stored.index_id,
    )
    .map_err(index_status)?;
    let roots = candidate.locator_stream_roots()?;
    let mutation_bytes = mutation_batch_resident_bytes(&pending, pending.capacity())?;
    let mut previous_by_ordinal = if roots.is_empty() {
        Vec::new()
    } else {
        // `pending` remains the sole owner of each path. The locator result is
        // ordinal-aligned, so neither request keys nor matched keys are cloned.
        let paths = pending
            .iter()
            .map(|mutation| mutation_identity(mutation).path.as_str())
            .collect::<Vec<_>>();
        let path_reference_bytes =
            borrowed_path_references_resident_bytes(&paths, paths.capacity())?;
        let result_budget = plan
            .max_source_projection_bytes
            .checked_sub(mutation_bytes)
            .and_then(|bytes| bytes.checked_sub(path_reference_bytes))
            .ok_or_else(|| {
                Status::resource_exhausted(
                    "catch-up mutations leave no bounded path-locator workspace",
                )
            })?;
        locate_path_values(&directory, &roots, &paths, result_budget)
            .await
            .map_err(index_status)?
    };
    let mut invalidations = BTreeMap::<u64, Vec<DocIdRange>>::new();
    let mut accepted = Vec::with_capacity(pending.len());
    for (ordinal, mutation) in pending.into_iter().enumerate() {
        let identity = mutation_identity(&mutation);
        let previous = previous_by_ordinal.get_mut(ordinal).and_then(Option::take);
        let Some(previous) = previous else {
            accepted.push(mutation);
            continue;
        };
        if previous.version() > identity.version {
            continue;
        }
        if previous.version() == identity.version {
            let idempotent = matches!(
                (&previous, &mutation),
                (LocatorValue::Live { .. }, MergeMutation::Upsert(_))
                    | (LocatorValue::Deleted { .. }, MergeMutation::Delete(_))
            );
            if idempotent {
                continue;
            }
            return Err(Status::data_loss(
                "format-v4 locator disagrees with a source mutation at the same version",
            ));
        }
        if let LocatorValue::Live { ranges, .. } = previous {
            for range in ranges {
                invalidations
                    .entry(range.segment_id)
                    .or_default()
                    .push(range);
            }
        }
        accepted.push(mutation);
    }

    if !invalidations.is_empty() {
        let routing_codec = definition
            .schema
            .codec_version(keldra_index::v4::ComponentKind::ROUTING_NODE)
            .map_err(index_status)?;
        let mut sink = dependencies.publisher.component_sink(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
            DerivedArtifactAdmission::PublicationProgress,
        );
        for (segment_id, mut ranges) in invalidations {
            let position = candidate
                .segments
                .iter()
                .position(|segment| segment.identity.segment_id == segment_id)
                .ok_or_else(|| {
                    Status::data_loss("format-v4 locator names a missing commit segment")
                })?;
            normalize_invalidation_ranges(segment_id, &mut ranges)?;
            let replacement = rewrite_segment_live_mask(
                &directory,
                &mut sink,
                &candidate.segments[position],
                routing_codec,
                &ranges,
            )
            .await
            .map_err(index_status)?;
            candidate.segments[position] = replacement;
        }
    }

    let mut tombstones = Vec::new();
    for mutation in accepted {
        match mutation {
            MergeMutation::Upsert(source) => {
                push_or_flush(
                    definition,
                    kind,
                    builder,
                    source,
                    candidate,
                    dependencies,
                    soft_flush_allowed,
                )
                .await?;
            }
            MergeMutation::Delete(identity) => tombstones.push(LocatorEntry {
                path: identity.path,
                value: LocatorValue::Deleted {
                    tombstone_version: identity.version,
                },
            }),
        }
    }
    if !tombstones.is_empty() {
        let identity = SegmentIdentity::new(
            definition.stored.index_id,
            definition.object_version,
            definition.schema_fingerprint,
            dependencies
                .store
                .allocate_snowflake_id()
                .map_err(|error| Status::internal(format!("allocate locator ID: {error}")))?,
        )
        .map_err(index_status)?;
        let mut sink = dependencies.publisher.component_sink(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
            DerivedArtifactAdmission::PublicationProgress,
        );
        sink.begin_segment(identity, &[]).map_err(index_status)?;
        let published = publish_locator_delta(
            &mut sink,
            identity,
            definition
                .schema
                .codec_version(keldra_index::v4::ComponentKind::PATH_LOCATOR)
                .map_err(index_status)?,
            definition
                .schema
                .codec_version(keldra_index::v4::ComponentKind::ROUTING_NODE)
                .map_err(index_status)?,
            tombstones,
        )
        .await
        .map_err(index_status)?;
        let packs = sink
            .finalize_segment(identity)
            .await
            .map_err(index_status)?;
        let sequence = candidate.allocate_sequence()?;
        candidate.locator_roots.push(LocatorRoot {
            sequence,
            identity,
            artifact: published.root,
            pack_ownership: LocatorPackOwnership::Standalone(packs),
            encoded_bytes: published.encoded_bytes,
            logical_bytes: published.logical_bytes,
        });
        candidate.locator_roots.sort_by_key(|root| root.sequence);
    }
    Ok(())
}

fn normalize_invalidation_ranges(
    segment_id: u64,
    ranges: &mut Vec<DocIdRange>,
) -> Result<(), Status> {
    ranges.sort_by_key(|range| range.first_doc_id.get());
    let mut write = 0usize;
    for read in 0..ranges.len() {
        let current = ranges[read];
        if current.segment_id != segment_id || current.count == 0 {
            return Err(Status::data_loss(
                "path locator returned an invalid live DocId range",
            ));
        }
        let current_end = current
            .first_doc_id
            .get()
            .checked_add(current.count)
            .ok_or_else(|| Status::data_loss("locator DocId range overflow"))?;
        if write != 0 {
            let previous = &mut ranges[write - 1];
            let previous_end = previous
                .first_doc_id
                .get()
                .checked_add(previous.count)
                .ok_or_else(|| Status::data_loss("locator DocId range overflow"))?;
            if current.first_doc_id.get() < previous_end {
                return Err(Status::data_loss(
                    "path locator returned overlapping live DocId ranges",
                ));
            }
            if current.first_doc_id.get() == previous_end {
                previous.count = current_end
                    .checked_sub(previous.first_doc_id.get())
                    .ok_or_else(|| Status::data_loss("locator DocId range underflow"))?;
                continue;
            }
        }
        ranges[write] = current;
        write += 1;
    }
    ranges.truncate(write);
    Ok(())
}

fn mutation_batch_resident_bytes(
    mutations: &[MergeMutation],
    capacity: usize,
) -> Result<usize, Status> {
    let mut bytes = std::mem::size_of::<Vec<MergeMutation>>()
        .checked_add(
            capacity
                .checked_mul(std::mem::size_of::<MergeMutation>())
                .ok_or_else(|| Status::resource_exhausted("catch-up mutation reserve overflow"))?,
        )
        .ok_or_else(|| Status::resource_exhausted("catch-up mutation reserve overflow"))?;
    for mutation in mutations {
        let dynamic = match mutation {
            MergeMutation::Upsert(source) => source
                .resident_bytes()
                .map_err(index_status)?
                .checked_sub(std::mem::size_of::<NativeProjectedSource>())
                .ok_or_else(|| {
                    Status::internal("projected source resident measure omitted its fixed value")
                })?,
            MergeMutation::Delete(identity) => identity.path.capacity(),
        };
        bytes = bytes
            .checked_add(dynamic)
            .ok_or_else(|| Status::resource_exhausted("catch-up mutation reserve overflow"))?;
    }
    Ok(bytes)
}

fn borrowed_path_references_resident_bytes(
    paths: &[&str],
    capacity: usize,
) -> Result<usize, Status> {
    if capacity < paths.len() {
        return Err(Status::internal(
            "borrowed path reference capacity is smaller than its length",
        ));
    }
    std::mem::size_of::<Vec<&str>>()
        .checked_add(
            capacity
                .checked_mul(std::mem::size_of::<&str>())
                .ok_or_else(|| {
                    Status::resource_exhausted("borrowed path reference reserve overflow")
                })?,
        )
        .ok_or_else(|| Status::resource_exhausted("borrowed path reference reserve overflow"))
}

fn mutation_identity(mutation: &MergeMutation) -> &ObjectIdentity {
    match mutation {
        MergeMutation::Upsert(source) => &source.source_identity,
        MergeMutation::Delete(identity) => identity,
    }
}

type ProjectedSource = Result<(MergeMutation, IndexBuildDiagnostics), IndexError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn prepared(index: usize, projection_bytes: u64, resident_bytes: u64) -> PreparedProjection {
        PreparedProjection {
            source: IndexSourceMutation::Remove(ObjectIdentity {
                path: format!("objects/{index}"),
                version: 1,
            }),
            projection_bytes,
            resident_bytes,
            needs_payload: false,
        }
    }

    #[test]
    fn catch_up_page_batch_keeps_all_sources_beyond_the_lane_count() {
        const BUDGET: u64 = 32 * 1024 * 1024;
        let mut batch = ProjectionBatch::new(BUDGET, 4);
        for index in 0..MAX_OBJECT_RECORD_EXPORT_RECORDS as usize {
            assert!(
                batch
                    .try_push(prepared(index, 1_024, 4_096))
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(batch.sources.len(), 1_000);
        assert_eq!(batch.effective_lanes(), 4);
        let two_bounded_output_slots_per_lane = 2 * batch.effective_lanes() as u64;
        assert!(
            batch.resident_bytes
                + two_bounded_output_slots_per_lane * batch.lane_limit().unwrap() as u64
                <= BUDGET
        );
    }

    #[test]
    fn catch_up_page_batch_rejects_sources_before_exceeding_its_shared_bytes() {
        let mut batch = ProjectionBatch::new(40, 4);
        assert!(batch.try_push(prepared(0, 10, 10)).unwrap().is_none());
        assert!(batch.try_push(prepared(1, 10, 10)).unwrap().is_none());
        assert!(batch.try_push(prepared(2, 10, 10)).unwrap().is_some());
        assert_eq!(batch.sources.len(), 2);
        assert_eq!(batch.resident_bytes, 20);
        assert_eq!(batch.lane_limit().unwrap(), 10);
    }

    #[test]
    fn invalidation_ranges_merge_adjacency_without_expanding_doc_ids() {
        let mut ranges = vec![
            DocIdRange {
                segment_id: 7,
                first_doc_id: keldra_index::v4::DocId::new(8),
                count: 2,
            },
            DocIdRange {
                segment_id: 7,
                first_doc_id: keldra_index::v4::DocId::new(2),
                count: 3,
            },
            DocIdRange {
                segment_id: 7,
                first_doc_id: keldra_index::v4::DocId::new(5),
                count: 3,
            },
        ];
        normalize_invalidation_ranges(7, &mut ranges).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].first_doc_id.get(), 2);
        assert_eq!(ranges[0].count, 8);
    }

    #[test]
    fn invalidation_ranges_reject_overlap_as_locator_corruption() {
        let mut ranges = vec![
            DocIdRange {
                segment_id: 7,
                first_doc_id: keldra_index::v4::DocId::new(2),
                count: 4,
            },
            DocIdRange {
                segment_id: 7,
                first_doc_id: keldra_index::v4::DocId::new(5),
                count: 2,
            },
        ];
        assert!(normalize_invalidation_ranges(7, &mut ranges).is_err());
    }

    #[test]
    fn journal_page_resident_measure_charges_decoded_path_capacity() {
        let mut path = String::with_capacity(4 * 1024);
        path.push_str("objects/a");
        let page = IndexJournalPage {
            changes: vec![IndexJournalChange {
                node: NodeId(1),
                change: LocalChange::ObjectHead(keldra_store::ObjectHeadChange {
                    offset: 1,
                    tenant_id: 2,
                    bucket_id: 3,
                    exact_path: path,
                    path_version: VersionId(4),
                    kind: keldra_store::ObjectHeadChangeKind::Put,
                    program_commit_cursor: None,
                    reference_deltas: Vec::new(),
                    accounting_transition: None,
                    definition_transition: None,
                }),
            }],
            through: IndexBarrier {
                fence: keldra_store::PlacementLogId { term: 1, index: 1 },
                atomic: crate::index_runtime::events::AtomicProgramWatermark::new(None, None, 0),
                sources: BTreeMap::new(),
            },
            encoded_bytes: 1,
        };
        assert!(journal_page_resident_bytes(&page).unwrap() >= 4 * 1024);
    }

    #[test]
    fn microbatch_checkpoint_advances_only_the_processed_source_offset() {
        let mut from = IndexBarrier {
            fence: keldra_store::PlacementLogId { term: 1, index: 1 },
            atomic: crate::index_runtime::events::AtomicProgramWatermark::new(
                Some(50),
                Some(50),
                0,
            ),
            sources: BTreeMap::from([
                (
                    NodeId(1),
                    IndexSourceCursor {
                        source: keldra_store::SourceId {
                            node_id: 1,
                            source_epoch: [1; 32],
                        },
                        next_offset: 3,
                    },
                ),
                (
                    NodeId(2),
                    IndexSourceCursor {
                        source: keldra_store::SourceId {
                            node_id: 2,
                            source_epoch: [2; 32],
                        },
                        next_offset: 11,
                    },
                ),
            ]),
        };
        let entry = IndexJournalChange {
            node: NodeId(1),
            change: LocalChange::ObjectHead(keldra_store::ObjectHeadChange {
                offset: 7,
                tenant_id: 2,
                bucket_id: 3,
                exact_path: "objects/a".into(),
                path_version: VersionId(4),
                kind: keldra_store::ObjectHeadChangeKind::Put,
                program_commit_cursor: None,
                reference_deltas: Vec::new(),
                accounting_transition: None,
                definition_transition: None,
            }),
        };

        let second_entry = IndexJournalChange {
            node: NodeId(2),
            change: LocalChange::ObjectHead(keldra_store::ObjectHeadChange {
                offset: 14,
                tenant_id: 2,
                bucket_id: 3,
                exact_path: "objects/b".into(),
                path_version: VersionId(5),
                kind: keldra_store::ObjectHeadChangeKind::Put,
                program_commit_cursor: None,
                reference_deltas: Vec::new(),
                accounting_transition: None,
                definition_transition: None,
            }),
        };

        let through = barrier_after_changes(&from, &[entry, second_entry]).unwrap();
        assert_eq!(through.sources[&NodeId(1)].next_offset, 8);
        assert_eq!(through.sources[&NodeId(2)].next_offset, 15);
        assert_eq!(through.atomic, from.atomic);
        from.sources.get_mut(&NodeId(1)).unwrap().next_offset = 8;
        from.sources.get_mut(&NodeId(2)).unwrap().next_offset = 15;
        assert_eq!(through, from);
    }

    #[test]
    fn partial_microbatch_charges_only_processed_journal_bytes() {
        let change = |offset, path: &str| IndexJournalChange {
            node: NodeId(1),
            change: LocalChange::ObjectHead(keldra_store::ObjectHeadChange {
                offset,
                tenant_id: 2,
                bucket_id: 3,
                exact_path: path.into(),
                path_version: VersionId(offset),
                kind: keldra_store::ObjectHeadChangeKind::Put,
                program_commit_cursor: None,
                reference_deltas: Vec::new(),
                accounting_transition: None,
                definition_transition: None,
            }),
        };
        let changes = vec![
            change(1, "objects/a"),
            change(2, "objects/a-much-longer-path"),
        ];
        let first = processed_journal_encoded_bytes(&changes[..1]).unwrap();
        let complete = processed_journal_encoded_bytes(&changes).unwrap();
        assert_eq!(
            first,
            index_journal_change_encoded_len(&changes[0]).unwrap()
        );
        assert!(first < complete);
    }

    #[test]
    fn sustained_catch_up_reserves_active_and_frozen_locator_slots_before_the_bound() {
        let boundary = MAX_LOCATOR_ROOTS_PER_COMMIT - 1;
        for committed in 0..boundary {
            assert_eq!(
                locator_slots_reserved_counts(committed, false, false),
                committed
            );
        }
        assert!(!debt::locator_headroom_requires_compaction(
            locator_slots_reserved_counts(boundary - 2, false, true)
        ));
        assert!(debt::locator_headroom_requires_compaction(
            locator_slots_reserved_counts(boundary - 2, true, true)
        ));
        assert!(debt::locator_headroom_requires_compaction(
            locator_slots_reserved_counts(boundary - 1, true, false)
        ));
    }

    #[test]
    fn atomic_multi_roll_exactly_fits_near_manifest_caps() {
        let effects = AtomicManifestEffects::between(
            MAX_SEGMENTS_PER_COMMIT - 3,
            MAX_LOCATOR_ROOTS_PER_COMMIT - 3,
            MAX_SEGMENTS_PER_COMMIT,
            MAX_LOCATOR_ROOTS_PER_COMMIT,
        )
        .unwrap();
        assert_eq!(effects.output_segments, 3);
        assert_eq!(effects.output_roots, 3);
        assert!(effects.fits());
    }

    #[test]
    fn atomic_exact_effects_force_base_compaction_before_retry() {
        let effects = AtomicManifestEffects::between(
            MAX_SEGMENTS_PER_COMMIT - 1,
            MAX_LOCATOR_ROOTS_PER_COMMIT - 2,
            MAX_SEGMENTS_PER_COMMIT + 2,
            MAX_LOCATOR_ROOTS_PER_COMMIT + 1,
        )
        .unwrap();
        assert_eq!(effects.output_segments, 3);
        assert_eq!(effects.output_roots, 3);
        assert!(!effects.fits());
    }

    #[test]
    fn irreducible_atomic_output_fails_definitively() {
        let error = atomic_manifest_capacity_error(
            MAX_SEGMENTS_PER_COMMIT + 1,
            MAX_LOCATOR_ROOTS_PER_COMMIT,
        );
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(error.message().contains("cannot be compacted"));
    }

    #[test]
    fn atomic_staging_metadata_is_subtracted_from_the_global_permit() {
        let staging_bytes = 1024 * 1024;
        let plan = SegmentMemoryPlan::new(MIN_INDEX_KIND_MEMORY_BYTES + staging_bytes).unwrap();
        let charged = charge_atomic_staging_overhead(plan, staging_bytes).unwrap();
        assert_eq!(charged.total_bytes, MIN_INDEX_KIND_MEMORY_BYTES);
        assert_eq!(
            charged.max_source_projection_bytes,
            plan.max_source_projection_bytes - staging_bytes
        );
        assert!(charge_atomic_staging_overhead(plan, plan.total_bytes).is_err());
    }

    #[test]
    fn atomic_projection_retries_only_transient_substeps() {
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Cancelled,
            tonic::Code::Unknown,
        ] {
            assert!(transient_atomic_projection_error(&Status::new(
                code, "retry"
            )));
        }
        assert!(!transient_atomic_projection_error(&Status::data_loss(
            "permanent"
        )));
    }

    #[test]
    fn atomic_paths_are_projected_only_from_the_complete_batch_event() {
        let page = IndexJournalPage {
            changes: vec![
                IndexJournalChange {
                    node: NodeId(1),
                    change: LocalChange::ObjectHead(keldra_store::ObjectHeadChange {
                        offset: 1,
                        tenant_id: 2,
                        bucket_id: 3,
                        exact_path: "objects/partial".into(),
                        path_version: VersionId(4),
                        kind: keldra_store::ObjectHeadChangeKind::Put,
                        program_commit_cursor: Some(70),
                        reference_deltas: Vec::new(),
                        accounting_transition: None,
                        definition_transition: None,
                    }),
                },
                IndexJournalChange {
                    node: NodeId(1),
                    change: LocalChange::AtomicBatchPublished(keldra_store::AtomicBatchPublished {
                        offset: 2,
                        cursor: 70,
                        bundle_hash: keldra_store::PreparedBundleHash([7; 32]),
                        affected_routes: vec![keldra_store::AtomicBatchRoute {
                            tenant_id: 2,
                            bucket_id: 3,
                        }],
                        mutations: vec![keldra_store::AtomicBatchMutation {
                            source_id: keldra_store::SourceId {
                                node_id: 1,
                                source_epoch: [1; 32],
                            },
                            source_journal_position: 1,
                            tenant_id: 2,
                            bucket_id: 3,
                            exact_path: "objects/complete".into(),
                            path_version: VersionId(5),
                            deleted: false,
                        }],
                    }),
                },
            ],
            through: IndexBarrier {
                fence: keldra_store::PlacementLogId { term: 1, index: 1 },
                atomic: crate::index_runtime::events::AtomicProgramWatermark::new(
                    Some(70),
                    Some(70),
                    0,
                ),
                sources: BTreeMap::new(),
            },
            encoded_bytes: 1,
        };
        assert_eq!(
            journal_source_paths(2, 3, "objects/", &page),
            BTreeMap::from([("objects/complete".into(), 5)])
        );
    }

    #[test]
    fn deleting_a_non_current_retained_version_is_not_a_head_mutation() {
        let changes = vec![IndexJournalChange {
            node: NodeId(1),
            change: LocalChange::RetainedVersionDeleted(
                keldra_store::RetainedVersionDeletedChange {
                    offset: 8,
                    tenant_id: 2,
                    bucket_id: 3,
                    exact_path: "objects/current".into(),
                    deleted_version: VersionId(4),
                    resulting_head_version: None,
                    reference_deltas: Vec::new(),
                    accounting_transition: None,
                },
            ),
        }];
        assert!(ordinary_journal_source_paths(2, 3, "objects/", &changes).is_empty());
    }

    #[test]
    fn ordinary_coalescing_uses_journal_order_not_version_magnitude() {
        let change = |offset, version| IndexJournalChange {
            node: NodeId(1),
            change: LocalChange::ObjectHead(keldra_store::ObjectHeadChange {
                offset,
                tenant_id: 2,
                bucket_id: 3,
                exact_path: "objects/current".into(),
                path_version: VersionId(version),
                kind: keldra_store::ObjectHeadChangeKind::Put,
                program_commit_cursor: None,
                reference_deltas: Vec::new(),
                accounting_transition: None,
                definition_transition: None,
            }),
        };
        let paths =
            ordinary_journal_source_paths(2, 3, "objects/", &[change(8, 100), change(9, 90)]);
        assert_eq!(paths, vec![("objects/current".into(), 90)]);
    }

    #[test]
    fn finalized_atomic_replay_is_suppressed_but_conflicting_pending_hash_fails() {
        let from = IndexBarrier {
            fence: keldra_store::PlacementLogId { term: 1, index: 1 },
            atomic: crate::index_runtime::events::AtomicProgramWatermark::new(
                Some(70),
                Some(70),
                0,
            ),
            sources: BTreeMap::new(),
        };
        let batch = keldra_store::AtomicBatchPublished {
            offset: 2,
            cursor: 70,
            bundle_hash: keldra_store::PreparedBundleHash([7; 32]),
            affected_routes: vec![keldra_store::AtomicBatchRoute {
                tenant_id: 2,
                bucket_id: 3,
            }],
            mutations: vec![keldra_store::AtomicBatchMutation {
                source_id: keldra_store::SourceId {
                    node_id: 1,
                    source_epoch: [1; 32],
                },
                source_journal_position: 1,
                tenant_id: 2,
                bucket_id: 3,
                exact_path: "objects/complete".into(),
                path_version: VersionId(5),
                deleted: false,
            }],
        };
        let candidate = CandidateCommit::rebuild();
        assert!(atomic_batch_already_materialized(&from, &candidate, &batch).unwrap());

        let mut conflicting = CandidateCommit::rebuild();
        conflicting
            .record_atomic_batch(70, keldra_store::PreparedBundleHash([8; 32]))
            .unwrap();
        assert!(atomic_batch_already_materialized(&from, &conflicting, &batch).is_err());
    }

    #[test]
    fn catch_up_locator_workspace_charges_mutations_and_borrowed_references_exactly() {
        let mut path = String::with_capacity(4 * 1024);
        path.push_str("objects/a");
        let mut mutations = Vec::with_capacity(7);
        mutations.push(MergeMutation::Delete(ObjectIdentity { path, version: 1 }));
        let mutation_bytes = mutation_batch_resident_bytes(&mutations, mutations.capacity())
            .expect("mutation resident measure");
        assert_eq!(
            mutation_bytes,
            std::mem::size_of::<Vec<MergeMutation>>()
                + mutations.capacity() * std::mem::size_of::<MergeMutation>()
                + 4 * 1024
        );

        let mut references = Vec::with_capacity(5);
        references.push("objects/a");
        references.push("objects/b");
        assert_eq!(
            borrowed_path_references_resident_bytes(&references, references.capacity()).unwrap(),
            std::mem::size_of::<Vec<&str>>() + references.capacity() * std::mem::size_of::<&str>()
        );
    }

    #[tokio::test]
    async fn catch_up_lanes_progress_concurrently_and_apply_in_source_order() {
        let lanes = partition_projection_lanes((0_u64..6).collect(), 3);
        let (progress_sender, mut progress_receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut receivers = Vec::new();
        let mut tasks = Vec::new();
        for lane in lanes {
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            receivers.push(receiver);
            let progress_sender = progress_sender.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                for value in lane {
                    sender.blocking_send(value).unwrap();
                    progress_sender.send(value).unwrap();
                }
            }));
        }
        drop(progress_sender);

        let mut first_round = Vec::new();
        for _ in 0..3 {
            first_round.push(progress_receiver.recv().await.unwrap());
        }
        first_round.sort_unstable();
        assert_eq!(first_round, [0, 1, 2]);

        let mut delivered = Vec::new();
        for position in 0..6 {
            delivered.push(
                receive_ordered_lane_item(&mut receivers, position)
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(delivered, (0_u64..6).collect::<Vec<_>>());
        for task in tasks {
            task.await.unwrap();
        }
    }
}
