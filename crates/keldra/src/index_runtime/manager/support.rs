//! Shared admission, placement, telemetry, and error conversion helpers.

use super::*;

pub(super) fn abort_replaced_worker(
    change: &CatalogChange,
    scheduler: &BuilderScheduler,
    running: &mut HashMap<CatalogIdentity, (tokio::task::Id, tokio::task::AbortHandle)>,
    inflight: &mut HashMap<tokio::task::Id, WorkMetadata>,
) {
    let logical_identity = change.identity();
    let running_identity = scheduler
        .logical_entries
        .get(&logical_identity)
        .and_then(|physical| scheduler.physical_entries.get(physical))
        .copied();
    let replaces = match change {
        CatalogChange::Upsert(definition) => running_identity.is_some_and(|identity| {
            identity == logical_identity
                && scheduler.entries.get(&identity).is_some_and(|entry| {
                    entry.definition.object_version != definition.object_version
                        || entry.definition.stored != definition.stored
                })
        }),
        CatalogChange::Delete { .. } | CatalogChange::Remove(_) => {
            running_identity == Some(logical_identity)
        }
    };
    if !replaces {
        return;
    }
    if let Some((task_id, handle)) = running.remove(&logical_identity) {
        inflight.remove(&task_id);
        handle.abort();
    }
}

pub(super) fn remove_running_task(
    running: &mut HashMap<CatalogIdentity, (tokio::task::Id, tokio::task::AbortHandle)>,
    identity: CatalogIdentity,
    task_id: tokio::task::Id,
) {
    if running
        .get(&identity)
        .is_some_and(|(running_id, _)| *running_id == task_id)
    {
        running.remove(&identity);
    }
}

#[derive(Clone, Copy)]
pub(super) struct WorkMetadata {
    pub(super) identity: CatalogIdentity,
    pub(super) definition_version: u64,
    pub(super) schema_fingerprint: [u8; 32],
    pub(super) kind: IndexKind,
    pub(super) held_snapshot: bool,
    pub(super) inspecting: bool,
}

impl WorkMetadata {
    pub(super) fn from_job(job: &BuilderJob) -> Self {
        Self {
            identity: job.definition.identity(),
            definition_version: job.definition.object_version,
            schema_fingerprint: job.definition.schema_fingerprint,
            kind: job.kind,
            held_snapshot: job.holds_snapshot(),
            inspecting: matches!(job.phase, BuilderPhase::Inspect),
        }
    }
}

pub(super) fn builder_lease_is_current(
    definition: &CatalogDefinition,
    local_node: NodeId,
    decisions: &DecisionRaft,
    store: &Store,
) -> Result<bool, Status> {
    let assignment = store
        .definition_assignment(
            DefinitionKind::Index,
            definition.tenant_id,
            definition.bucket_id,
            definition.stored.index_id,
        )
        .map_err(|error| Status::internal(format!("read active index assignment: {error}")))?;
    let Some(assignment) = assignment else {
        return Ok(false);
    };
    let placement = current_placement(decisions)?;
    let identity = IndexIdentity::projection_partition(definition.tenant_id, definition.bucket_id)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    let owners = IndexPlacement::derive(identity, &placement)
        .map_err(|error| Status::unavailable(error.to_string()))?;
    Ok(assignment.kind == DefinitionKind::Index
        && assignment.object_version == VersionId(definition.object_version)
        && assignment.definition_path == definition_path(&definition.stored.name)?
        && assignment.observed_fence == placement.fence()
        && assignment.rank == 0
        && owners.builder() == local_node)
}

pub(super) fn take_ready(
    queue: &mut VecDeque<CatalogIdentity>,
    entries: &mut BTreeMap<CatalogIdentity, ScheduledBuilder>,
    active: bool,
) -> Option<CatalogIdentity> {
    while let Some(identity) = queue.pop_front() {
        let Some(entry) = entries.get(&identity) else {
            continue;
        };
        if entry.queued
            && entry
                .job
                .as_ref()
                .is_some_and(|job| job.is_active() == active)
        {
            return Some(identity);
        }
    }
    None
}

pub(super) fn apply_catalog_change(
    scheduler: &mut BuilderScheduler,
    change: CatalogChange,
    local_node: NodeId,
    decisions: &DecisionRaft,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let identity = change.identity();
    let projection_upsert = match &change {
        CatalogChange::Upsert(definition) => Some(definition.clone()),
        CatalogChange::Delete { .. } | CatalogChange::Remove(_) => None,
    };
    let physical = match &change {
        CatalogChange::Upsert(definition) => Some(definition.physical_identity()),
        CatalogChange::Delete { .. } | CatalogChange::Remove(_) => {
            scheduler.logical_entries.get(&identity).copied()
        }
    };
    let applied = scheduler.apply_change(change, local_node, decisions, &dependencies.retention);
    let registered = if let Some(definition) = projection_upsert
        && scheduler.logical_entries.contains_key(&identity)
    {
        dependencies.projection_mapper.upsert(&definition)
    } else {
        dependencies.projection_mapper.remove(identity)
    };
    applied.and(registered)?;
    if let Some(physical) = physical
        && let Some(representative) = scheduler.physical_entries.get(&physical).copied()
        && let Some(entry) = scheduler.entries.get(&representative)
        && entry.definition.schema.kind == IndexKind::TypedJson
    {
        let family = entry.definition.projection_family_identity();
        let schema = dependencies
            .projection_mapper
            .family_schema(family)?
            .ok_or_else(|| Status::internal("active projection family is not registered"))?;
        scheduler.refresh_physical_schema(physical, schema)?;
    }
    Ok(())
}

pub(super) fn source_wire_limit(limit: u64) -> u64 {
    let fixed = FIXED_INDEX_SEAL_WORKSPACE_BYTES as u64;
    let remaining = limit.saturating_sub(fixed);
    let builder_reserve = remaining / 2;
    // A source page is decoded and retained while its payload projections are
    // prepared. Reserving only the builder half lets a maximum-sized page
    // consume every remaining byte and leaves no legal projection workspace.
    // Split the other half between source input and projection; the runtime
    // also halves this bound and retries the unadvanced page when concrete
    // decoded residency is more expensive than its wire representation.
    let projection_reserve = remaining.saturating_sub(builder_reserve) / 2;
    let safe = remaining
        .saturating_sub(builder_reserve)
        .saturating_sub(projection_reserve)
        .saturating_sub(256);
    MAX_SOURCE_WIRE_BYTES.min(safe.max(64 * 1024))
}

pub(super) fn reduced_source_wire_limit(current: u64) -> Option<u64> {
    const MINIMUM_SOURCE_WIRE_BYTES: u64 = 64 * 1024;
    (current > MINIMUM_SOURCE_WIRE_BYTES).then(|| (current / 2).max(MINIMUM_SOURCE_WIRE_BYTES))
}

/// A native TypedJson cache is disposable and may lag its canonical family if
/// a process dies after installing v5 current but before publishing the cache
/// manifest. Rebuild that cache from a fresh source snapshot instead of
/// replaying an already-covered journal unit and silently omitting its material
/// mutations.
pub(super) async fn projection_cache_is_coherent(
    definition: &CatalogDefinition,
    current: &CommittedIndexView,
    dependencies: &IndexBuilderDependencies,
) -> Result<bool, Status> {
    if definition.schema.kind != IndexKind::TypedJson {
        return Ok(true);
    }
    let family = definition.projection_family_identity();
    let Some(projection) = dependencies
        .publisher
        .load_projection_generation(
            &definition.stored.tenant,
            &definition.stored.bucket,
            family.tenant_id,
            family.bucket_id,
            family.family_id,
        )
        .await?
    else {
        return Ok(false);
    };
    let cache = current.manifest.barrier().map_err(commit_view_status)?;
    let cache = super::super::projection_family_writer::projection_barrier(&cache)?;
    Ok(projection.generation.barrier == cache)
}

pub(super) fn emit_projection_cache_rebuild(definition: &CatalogDefinition) {
    tracing::warn!(
        index.id = definition.stored.index_id,
        "rebuilding disposable native cache whose canonical projection barrier differs"
    );
}

pub(super) fn work_plan_for_limit(
    limit: u64,
    source_resident_bytes: u64,
    segment_flush_bytes: u64,
) -> Result<SegmentMemoryPlan, Status> {
    let total = usize::try_from(limit)
        .map_err(|_| Status::resource_exhausted("index construction budget exceeds platform"))?;
    let source_resident = usize::try_from(source_resident_bytes)
        .map_err(|_| Status::resource_exhausted("index source frame exceeds platform"))?;
    let available = total.checked_sub(source_resident).ok_or_else(|| {
        Status::resource_exhausted("resident index source frame exhausts its kind budget")
    })?;
    let configured = SegmentMemoryPlan::new(total).map_err(index_status)?;
    let segment_flush_bytes = usize::try_from(segment_flush_bytes)
        .map_err(|_| Status::resource_exhausted("index segment flush target exceeds platform"))?;
    let max_resident_bytes = configured.max_resident_bytes.min(segment_flush_bytes);
    let max_source_projection_bytes = available
        .checked_sub(FIXED_INDEX_SEAL_WORKSPACE_BYTES)
        .and_then(|bytes| bytes.checked_sub(max_resident_bytes))
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| {
            Status::resource_exhausted("index source frame leaves no bounded projection workspace")
        })?;
    Ok(SegmentMemoryPlan {
        total_bytes: total,
        max_resident_bytes,
        max_source_projection_bytes,
    })
}

pub(super) fn emit_source_lag(kind: IndexKind, from: &IndexBarrier, target: &IndexBarrier) {
    let lag = from.sources.iter().fold(0_u64, |total, (node, cursor)| {
        total.saturating_add(target.sources.get(node).map_or(0, |latest| {
            latest.next_offset.saturating_sub(cursor.next_offset)
        }))
    });
    tracing::debug!(
        index.kind = ?kind,
        gauge.keldra_index_source_lag = lag,
        gauge.keldra_index_publication_fresh = u64::from(lag == 0),
        "index source lag observed"
    );
}

pub(super) fn emit_publication_age(kind: IndexKind, current: Option<&CommittedIndexView>) {
    let Some(current) = current else {
        tracing::debug!(
            index.kind = ?kind,
            gauge.keldra_index_publication_present = 0_u64,
            gauge.keldra_index_publication_age_seconds = 0_f64,
            gauge.keldra_index_publication_fresh = 0_u64,
            "index has no published committed view"
        );
        return;
    };
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u64, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        });
    let age_seconds = now_millis.saturating_sub(current.pointer.current.published_at_unix_millis)
        as f64
        / 1_000.0;
    tracing::debug!(
        index.kind = ?kind,
        gauge.keldra_index_publication_present = 1_u64,
        gauge.keldra_index_publication_age_seconds = age_seconds,
        "index publication age observed"
    );
}

pub(super) fn current_placement(decisions: &DecisionRaft) -> Result<ClusterPlacement, Status> {
    let state = decisions
        .state()
        .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
    ClusterPlacement::from_applied(&state).map_err(|error| Status::unavailable(error.to_string()))
}

pub(super) fn budget_status(error: IndexBudgetError) -> Status {
    Status::resource_exhausted(error.to_string())
}

pub(super) fn cpu_status(error: IndexCpuPoolError) -> Status {
    Status::internal(error.to_string())
}

pub(super) fn index_status(error: IndexError) -> Status {
    match error {
        IndexError::ResourceLimit { .. } => Status::resource_exhausted(error.to_string()),
        IndexError::Io(_) => Status::unavailable(error.to_string()),
        _ => Status::data_loss(error.to_string()),
    }
}

pub(super) fn event_status(error: IndexEventError) -> Status {
    match error {
        IndexEventError::Placement(_)
        | IndexEventError::AtomicProgramInProgress
        | IndexEventError::Source { .. }
        | IndexEventError::Task(_) => Status::unavailable(error.to_string()),
        IndexEventError::BarrierChanged => Status::aborted(error.to_string()),
        IndexEventError::CheckpointMismatch(_)
        | IndexEventError::SourceEpochChanged(_)
        | IndexEventError::SourceHistoryGap(_)
        | IndexEventError::IncompleteSources => Status::failed_precondition(error.to_string()),
        IndexEventError::PageBytesExceeded { .. } => Status::resource_exhausted(error.to_string()),
        IndexEventError::ZeroPageByteLimit => Status::invalid_argument(error.to_string()),
        IndexEventError::InvalidSourceStatus(_)
        | IndexEventError::InvalidAtomicBatch(_)
        | IndexEventError::NonContiguousSource(_)
        | IndexEventError::OffsetOverflow(_)
        | IndexEventError::PageLengthOverflow
        | IndexEventError::PageLengthMismatch { .. }
        | IndexEventError::Encode(_) => Status::data_loss(error.to_string()),
    }
}

pub(super) fn commit_view_status(error: super::super::committed_view::CommitViewError) -> Status {
    Status::data_loss(error.to_string())
}
