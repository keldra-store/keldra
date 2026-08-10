//! Weighted-HRW assignment and bounded v2 index generation construction.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::time::Duration;

use anvil_api::v1::IndexSpecification;
use anvil_consensus::{DecisionRaft, NodeId};
use anvil_index::{
    DocumentRef, FIXED_INDEX_SEAL_WORKSPACE_BYTES, IndexError, IndexKind, SegmentMemoryPlan,
};
use anvil_store::{Head, LocalChange, ObjectKey, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::IndexSourceSnapshotHead;
use crate::cluster_placement::ClusterPlacement;
use crate::index_service::{StoredIndexDefinition, path_matches_prefix};

use super::budget::{IndexBudgetError, IndexMemoryBudget, IndexMemoryBudgets};
use super::cache::IndexCache;
use super::catalog::{CatalogDefinition, IndexCatalog};
use super::cpu::{IndexCpuPool, IndexCpuPoolError};
use super::directory::ManifestIndexDirectory;
use super::engine::{
    EngineMutation, EngineSegmentBuilder, EngineSegmentPush, IndexBuildDiagnostics,
    IndexBuildObject, IndexSourceMutation, kind_for_specification, merge_runs, project_mutation,
    projection_admission_bytes,
};
use super::events::{IndexBarrier, IndexEventError, IndexEventJournal, IndexJournalPage};
use super::generation::{MAX_RUNS_PER_LEVEL, ManifestRun};
use super::placement::{IndexIdentity, IndexPlacement};
use super::publisher::{IndexGenerationPublisher, PublishedGeneration};
use super::retention::IndexGenerationRetention;
use super::scanner::ClusterIndexScanner;

const ASSIGNMENT_INTERVAL: Duration = Duration::from_secs(2);
const BUILDER_IDLE_INTERVAL: Duration = Duration::from_millis(100);
const BUILDER_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const RETENTION_RETRY_INTERVAL: Duration = Duration::from_secs(30);
const MAX_SOURCE_WIRE_BYTES: u64 = 4 * 1024 * 1024;
const DECODED_SOURCE_MULTIPLIER: u64 = 4;

pub(crate) struct IndexBuilderManagerTask {
    task: tokio::task::JoinHandle<()>,
}

impl IndexBuilderManagerTask {
    pub(crate) fn start(
        local_node: NodeId,
        decisions: DecisionRaft,
        catalog: IndexCatalog,
        dependencies: IndexBuilderDependencies,
    ) -> Self {
        let task = tokio::spawn(async move {
            let mut builders = BTreeMap::<u64, RunningBuilder>::new();
            let mut interval = tokio::time::interval(ASSIGNMENT_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let definitions = match catalog.all() {
                    Ok(definitions) => definitions,
                    Err(error) => {
                        tracing::warn!(%error, "index builder catalog is unavailable");
                        continue;
                    }
                };
                let placement = match current_placement(&decisions) {
                    Ok(placement) => placement,
                    Err(error) => {
                        tracing::warn!(%error, "index builder placement is unavailable");
                        continue;
                    }
                };
                let mut desired = BTreeMap::new();
                for definition in definitions {
                    let identity = match IndexIdentity::new(
                        definition.tenant_id,
                        definition.bucket_id,
                        definition.stored.index_id,
                    ) {
                        Ok(identity) => identity,
                        Err(error) => {
                            tracing::warn!(%error, "invalid stable index identity in catalog");
                            continue;
                        }
                    };
                    let assignment = match IndexPlacement::derive(identity, &placement) {
                        Ok(assignment) => assignment,
                        Err(error) => {
                            tracing::warn!(%error, "cannot derive index builder assignment");
                            continue;
                        }
                    };
                    if assignment.builder() == local_node {
                        desired.insert(definition.stored.index_id, definition);
                    }
                }

                let desired_ids = desired.keys().copied().collect::<BTreeSet<_>>();
                builders.retain(|index_id, running| {
                    let keep = desired_ids.contains(index_id)
                        && desired.get(index_id).is_some_and(|definition| {
                            definition.object_version == running.definition_version
                        })
                        && !running.task.is_finished();
                    if !keep {
                        running.task.abort();
                    }
                    keep
                });
                for (index_id, definition) in desired {
                    if builders.contains_key(&index_id) {
                        continue;
                    }
                    let definition_version = definition.object_version;
                    let dependencies = dependencies.clone();
                    let task = tokio::spawn(async move {
                        run_builder(definition, dependencies).await;
                    });
                    builders.insert(
                        index_id,
                        RunningBuilder {
                            definition_version,
                            task,
                        },
                    );
                }
            }
        });
        Self { task }
    }
}

impl Drop for IndexBuilderManagerTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct RunningBuilder {
    definition_version: u64,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
pub(crate) struct IndexBuilderDependencies {
    pub(crate) catalog: IndexCatalog,
    pub(crate) journal: std::sync::Arc<IndexEventJournal>,
    pub(crate) scanner: ClusterIndexScanner,
    pub(crate) reader: ClusterObjectReader,
    pub(crate) publisher: IndexGenerationPublisher,
    pub(crate) retention: IndexGenerationRetention,
    pub(crate) cache: IndexCache,
    pub(crate) budgets: IndexMemoryBudgets,
    pub(crate) cpu: IndexCpuPool,
}

async fn run_builder(definition: CatalogDefinition, dependencies: IndexBuilderDependencies) {
    let mut observed = None;
    let mut retention_retry_deadline = next_retention_retry(tokio::time::Instant::now());
    loop {
        let specification = match definition.stored.specification() {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(index.id = definition.stored.index_id, %error, "index definition is invalid");
                return;
            }
        };
        let kind = match kind_for_specification(&specification) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(index.id = definition.stored.index_id, %error, "index kind is invalid");
                return;
            }
        };
        match build_once(
            &definition,
            &specification,
            kind,
            observed.as_ref(),
            &dependencies,
        )
        .await
        {
            Ok(BuildProgress::Idle(progress)) => {
                observed = Some(progress);
                retry_retention_if_due(
                    &definition,
                    kind,
                    &dependencies,
                    &mut retention_retry_deadline,
                )
                .await;
                tokio::time::sleep(BUILDER_IDLE_INTERVAL).await;
            }
            Ok(BuildProgress::Published) => {
                observed = None;
                retention_retry_deadline = next_retention_retry(tokio::time::Instant::now());
                tokio::task::yield_now().await;
            }
            Err(error) => {
                tracing::warn!(
                    index.id = definition.stored.index_id,
                    index.kind = ?kind,
                    %error,
                    "index build attempt failed; prior generation remains current"
                );
                retry_retention_if_due(
                    &definition,
                    kind,
                    &dependencies,
                    &mut retention_retry_deadline,
                )
                .await;
                tokio::time::sleep(BUILDER_RETRY_INTERVAL).await;
            }
        }
    }
}

fn next_retention_retry(now: tokio::time::Instant) -> tokio::time::Instant {
    now + RETENTION_RETRY_INTERVAL
}

fn retention_retry_due(now: tokio::time::Instant, next_retry: tokio::time::Instant) -> bool {
    now >= next_retry
}

async fn retry_retention_if_due(
    definition: &CatalogDefinition,
    kind: IndexKind,
    dependencies: &IndexBuilderDependencies,
    next_retry: &mut tokio::time::Instant,
) {
    if !retention_retry_due(tokio::time::Instant::now(), *next_retry) {
        return;
    }
    retry_retention(definition, kind, dependencies).await;
    *next_retry = next_retention_retry(tokio::time::Instant::now());
}

async fn retry_retention(
    definition: &CatalogDefinition,
    kind: IndexKind,
    dependencies: &IndexBuilderDependencies,
) {
    let current = dependencies
        .publisher
        .load_current(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
        )
        .await;
    match current {
        Ok(Some(current)) => {
            collect_obsolete_generation_artifacts(
                definition,
                kind,
                &current,
                dependencies,
                "periodic",
            )
            .await;
        }
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(
                index.id = definition.stored.index_id,
                index.kind = ?kind,
                %error,
                "periodic obsolete index cleanup reload deferred"
            );
        }
    }
}

// This progress is deliberately task-local: a restart may safely rescan ignored
// reserved-artifact journal entries from the published generation barrier.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedGenerationProgress {
    current_object_version: VersionId,
    barrier: IndexBarrier,
}

enum BuildProgress {
    Idle(ObservedGenerationProgress),
    Published,
}

async fn build_once(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    kind: IndexKind,
    observed: Option<&ObservedGenerationProgress>,
    dependencies: &IndexBuilderDependencies,
) -> Result<BuildProgress, Status> {
    let current = dependencies
        .publisher
        .load_current(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
        )
        .await?;
    if let Some(current) = current.as_ref()
        && current.manifest.definition_version == definition.object_version
        && current.manifest.kind == kind
    {
        let target = dependencies
            .journal
            .capture_barrier()
            .await
            .map_err(event_status)?;
        let published = current.manifest.barrier().map_err(generation_status)?;
        let from = incremental_start(current.current_object_version, &published, observed).clone();
        if barriers_can_advance(&from, &target) {
            if from == target {
                return Ok(BuildProgress::Idle(ObservedGenerationProgress {
                    current_object_version: current.current_object_version,
                    barrier: target,
                }));
            }
            let advanced = advance_generation(
                definition,
                specification,
                kind,
                current,
                from,
                target,
                dependencies,
            )
            .await;
            if advanced
                .as_ref()
                .is_err_and(|status| status.code() == tonic::Code::FailedPrecondition)
            {
                return rebuild_generation(
                    definition,
                    specification,
                    kind,
                    Some(current),
                    dependencies,
                )
                .await;
            }
            return advanced;
        }
    }
    rebuild_generation(
        definition,
        specification,
        kind,
        current.as_ref(),
        dependencies,
    )
    .await
}

fn incremental_start<'a>(
    current_object_version: VersionId,
    published: &'a IndexBarrier,
    observed: Option<&'a ObservedGenerationProgress>,
) -> &'a IndexBarrier {
    observed
        .filter(|progress| {
            progress.current_object_version == current_object_version
                && barriers_can_advance(published, &progress.barrier)
        })
        .map_or(published, |progress| &progress.barrier)
}

fn barriers_can_advance(from: &IndexBarrier, target: &IndexBarrier) -> bool {
    from.fence == target.fence
        && from.sources.len() == target.sources.len()
        && from.sources.iter().all(|(node, cursor)| {
            target.sources.get(node).is_some_and(|latest| {
                latest.source == cursor.source && latest.next_offset >= cursor.next_offset
            })
        })
}

async fn rebuild_generation(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    kind: IndexKind,
    current: Option<&PublishedGeneration>,
    dependencies: &IndexBuilderDependencies,
) -> Result<BuildProgress, Status> {
    tracing::info!(
        index.kind = ?kind,
        monotonic_counter.anvil_index_rebuilds_total = 1_u64,
        "index snapshot rebuild started"
    );
    let budget = dependencies.budgets.for_kind(kind);
    let _snapshot_slot = budget
        .acquire_snapshot_slot()
        .await
        .map_err(budget_status)?;
    let max_frame_bytes = source_wire_limit(budget.limit());
    let mut snapshot = dependencies
        .scanner
        .begin_source_snapshot(
            definition.tenant_id,
            definition.bucket_id,
            definition.stored.path_prefix.clone(),
            max_frame_bytes,
        )
        .await?;
    let tails = snapshot
        .checkpoints()
        .iter()
        .map(|checkpoint| (checkpoint.node, checkpoint.source, checkpoint.captured_tail))
        .collect::<Vec<_>>();
    let mut through = dependencies
        .journal
        .barrier_from_snapshot_tails(snapshot.placement_fence(), &tails)
        .map_err(event_status)?;
    let mut candidate = CandidateGeneration::rebuild();

    loop {
        let permit = budget
            .acquire(budget.limit())
            .await
            .map_err(budget_status)?;
        let Some(frame) = snapshot.next_frame().await? else {
            drop(permit);
            break;
        };
        let encoded_bytes = measure_snapshot_frame(&frame)?;
        let plan = work_plan(budget, encoded_bytes)?;
        process_snapshot_frame(
            definition,
            specification,
            kind,
            &through,
            frame,
            plan,
            &mut candidate,
            dependencies,
        )
        .await?;
        drop(permit);
        compact_until_bounded(
            definition,
            specification,
            kind,
            &mut candidate,
            budget,
            dependencies,
        )
        .await?;
        tokio::task::yield_now().await;
    }

    let target = dependencies
        .journal
        .capture_barrier()
        .await
        .map_err(event_status)?;
    emit_source_lag(kind, &through, &target);
    let _ = catch_up(
        definition,
        specification,
        kind,
        &mut through,
        &target,
        &mut candidate,
        budget,
        dependencies,
    )
    .await?;
    publish_candidate(definition, kind, through, candidate, current, dependencies).await
}

async fn advance_generation(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    kind: IndexKind,
    current: &PublishedGeneration,
    mut through: IndexBarrier,
    target: IndexBarrier,
    dependencies: &IndexBuilderDependencies,
) -> Result<BuildProgress, Status> {
    let budget = dependencies.budgets.for_kind(kind);
    let mut candidate = CandidateGeneration::incremental(current);
    emit_source_lag(kind, &through, &target);
    let changed = catch_up(
        definition,
        specification,
        kind,
        &mut through,
        &target,
        &mut candidate,
        budget,
        dependencies,
    )
    .await?;
    if !changed {
        return Ok(BuildProgress::Idle(ObservedGenerationProgress {
            current_object_version: current.current_object_version,
            barrier: through,
        }));
    }
    publish_candidate(
        definition,
        kind,
        through,
        candidate,
        Some(current),
        dependencies,
    )
    .await
}

async fn catch_up(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    kind: IndexKind,
    through: &mut IndexBarrier,
    target: &IndexBarrier,
    candidate: &mut CandidateGeneration,
    budget: &IndexMemoryBudget,
    dependencies: &IndexBuilderDependencies,
) -> Result<bool, Status> {
    let page_bytes = source_wire_limit(budget.limit());
    let mut changed = false;
    loop {
        let permit = budget
            .acquire(budget.limit())
            .await
            .map_err(budget_status)?;
        let page = dependencies
            .journal
            .next_page(through, target, page_bytes)
            .await
            .map_err(event_status)?;
        let Some(page) = page else {
            drop(permit);
            break;
        };
        let plan = work_plan(budget, page.encoded_bytes)?;
        changed |= process_journal_page(
            definition,
            specification,
            kind,
            target,
            &page,
            plan,
            candidate,
            dependencies,
        )
        .await?;
        *through = page.through;
        drop(permit);
        compact_until_bounded(
            definition,
            specification,
            kind,
            candidate,
            budget,
            dependencies,
        )
        .await?;
        tokio::task::yield_now().await;
    }
    if through != target {
        return Err(Status::unavailable(
            "index catch-up did not reach its complete source barrier",
        ));
    }
    Ok(changed)
}

struct CandidateGeneration {
    runs: Vec<ManifestRun>,
    next_sequence: u64,
    diagnostics: IndexBuildDiagnostics,
}

impl CandidateGeneration {
    fn rebuild() -> Self {
        Self {
            runs: Vec::new(),
            next_sequence: 1,
            diagnostics: IndexBuildDiagnostics::default(),
        }
    }

    fn incremental(current: &PublishedGeneration) -> Self {
        let next_sequence = current
            .manifest
            .runs
            .last()
            .map_or(1, |run| run.sequence.saturating_add(1));
        Self {
            runs: current.manifest.runs.clone(),
            next_sequence,
            diagnostics: IndexBuildDiagnostics {
                accepted_objects: current.manifest.accepted_objects,
                skipped_objects: current.manifest.skipped_objects,
            },
        }
    }

    fn allocate_sequence(&mut self) -> Result<u64, Status> {
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("index run sequence exhausted"))?;
        Ok(sequence)
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_snapshot_frame(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    kind: IndexKind,
    barrier: &IndexBarrier,
    frame: Vec<IndexSourceSnapshotHead>,
    plan: SegmentMemoryPlan,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let mut builder = EngineSegmentBuilder::new(specification, plan).map_err(index_status)?;
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
        let object = build_object(&head.exact_path, &head.version)?;
        let source = IndexSourceMutation::Upsert(object);
        let (mutation, diagnostics) = project_source(
            specification,
            source,
            plan.max_source_projection_bytes,
            dependencies,
        )
        .await?;
        candidate.diagnostics.add(diagnostics);
        push_or_flush(
            definition,
            specification,
            kind,
            plan,
            &mut builder,
            mutation,
            candidate,
            dependencies,
        )
        .await?;
    }
    flush_builder(definition, kind, builder, candidate, dependencies).await
}

#[allow(clippy::too_many_arguments)]
async fn process_journal_page(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    kind: IndexKind,
    target: &IndexBarrier,
    page: &IndexJournalPage,
    plan: SegmentMemoryPlan,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<bool, Status> {
    let paths = journal_source_paths(
        definition.tenant_id,
        definition.bucket_id,
        &definition.stored.path_prefix,
        page,
    );
    let changed = !paths.is_empty();

    let mut builder = EngineSegmentBuilder::new(specification, plan).map_err(index_status)?;
    for (path, fallback_version) in paths {
        let source = load_target_source(
            definition,
            specification,
            &path,
            fallback_version,
            target,
            dependencies,
        )
        .await?;
        let (mutation, diagnostics) = project_source(
            specification,
            source,
            plan.max_source_projection_bytes,
            dependencies,
        )
        .await?;
        candidate.diagnostics.add(diagnostics);
        push_or_flush(
            definition,
            specification,
            kind,
            plan,
            &mut builder,
            mutation,
            candidate,
            dependencies,
        )
        .await?;
    }
    flush_builder(definition, kind, builder, candidate, dependencies).await?;
    Ok(changed)
}

fn journal_source_paths(
    tenant_id: u64,
    bucket_id: u64,
    path_prefix: &str,
    page: &IndexJournalPage,
) -> BTreeMap<String, u64> {
    let mut paths = BTreeMap::<String, u64>::new();
    for entry in &page.changes {
        let (change_tenant_id, change_bucket_id, path, version) = match &entry.change {
            LocalChange::ObjectHead(change) => (
                change.tenant_id,
                change.bucket_id,
                &change.exact_path,
                change.path_version.0,
            ),
            LocalChange::RetainedVersionDeleted(change) => (
                change.tenant_id,
                change.bucket_id,
                &change.exact_path,
                change
                    .resulting_head_version
                    .unwrap_or(change.deleted_version)
                    .0,
            ),
            LocalChange::AggregateChanged(_) | LocalChange::ContentLifecycleChanged(_) => continue,
            _ => continue,
        };
        if change_tenant_id == tenant_id
            && change_bucket_id == bucket_id
            && path_matches_prefix(path, path_prefix)
            && !contains_reserved_segment(path)
        {
            paths
                .entry(path.clone())
                .and_modify(|selected| *selected = (*selected).max(version))
                .or_insert(version);
        }
    }
    paths
}

async fn load_target_source(
    definition: &CatalogDefinition,
    _specification: &IndexSpecification,
    path: &str,
    fallback_version: u64,
    target: &IndexBarrier,
    dependencies: &IndexBuilderDependencies,
) -> Result<IndexSourceMutation, Status> {
    let key = ObjectKey::new(&definition.stored.tenant, &definition.stored.bucket, path)
        .map_err(|error| Status::internal(error.to_string()))?;
    let Some(snapshot) = dependencies
        .reader
        .current_snapshot_stable(&key, definition.tenant_id, definition.bucket_id)
        .await?
    else {
        return Ok(IndexSourceMutation::Remove(DocumentRef {
            path: path.to_owned(),
            version: fallback_version,
        }));
    };
    if snapshot.exact_path != path {
        return Err(Status::data_loss(
            "index current-head reread returned another exact path",
        ));
    }
    require_visible_head(&snapshot.head, target)?;
    let version = snapshot
        .versions
        .iter()
        .find(|version| version.id == snapshot.head.version)
        .ok_or_else(|| Status::data_loss("index current head has no matching version"))?;
    if version.deleted
        || !source_matches_definition(&definition.stored, path, version.content_type.as_deref())
    {
        return Ok(IndexSourceMutation::Remove(DocumentRef {
            path: path.to_owned(),
            version: version.id.0,
        }));
    }
    Ok(IndexSourceMutation::Upsert(build_object(path, version)?))
}

async fn project_source(
    specification: &IndexSpecification,
    source: IndexSourceMutation,
    max_projection_bytes: usize,
    dependencies: &IndexBuilderDependencies,
) -> Result<(EngineMutation, IndexBuildDiagnostics), Status> {
    let admission = projection_admission_bytes(specification, &source).map_err(index_status)?;
    if admission > max_projection_bytes as u64 {
        return Err(Status::resource_exhausted(format!(
            "one index source projection needs {admission} bytes but is capped at {max_projection_bytes}"
        )));
    }
    let payload = match &source {
        IndexSourceMutation::Upsert(object) if source_needs_payload(specification) => {
            let reference = anvil_store::BlobRef {
                hash: object.content_hash,
                length: object.content_length,
            };
            Some(dependencies.reader.open_blob_payload(&reference).await?)
        }
        _ => None,
    };
    let specification = specification.clone();
    dependencies
        .cpu
        .install(move || {
            let mut payload = payload;
            let reader = payload
                .as_mut()
                .map(|payload| payload as &mut dyn std::io::Read);
            project_mutation(&specification, source, reader, max_projection_bytes)
        })
        .await
        .map_err(cpu_status)?
        .map_err(index_status)
}

#[allow(clippy::too_many_arguments)]
async fn push_or_flush(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    kind: IndexKind,
    plan: SegmentMemoryPlan,
    builder: &mut EngineSegmentBuilder,
    mutation: EngineMutation,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    match builder.try_push(mutation).map_err(index_status)? {
        EngineSegmentPush::Accepted => Ok(()),
        EngineSegmentPush::Full(pending) => {
            let replacement =
                EngineSegmentBuilder::new(specification, plan).map_err(index_status)?;
            let full = std::mem::replace(builder, replacement);
            flush_builder(definition, kind, full, candidate, dependencies).await?;
            match builder.try_push(pending).map_err(index_status)? {
                EngineSegmentPush::Accepted => Ok(()),
                EngineSegmentPush::Full(_) => Err(Status::resource_exhausted(
                    "one index mutation cannot fit an empty bounded builder",
                )),
            }
        }
    }
}

async fn flush_builder(
    definition: &CatalogDefinition,
    kind: IndexKind,
    builder: EngineSegmentBuilder,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    if builder.is_empty() {
        return Ok(());
    }
    let resident = builder.resident_bytes() as u64;
    let workspace = builder.seal_workspace_bytes().map_err(index_status)? as u64;
    let mut sink = dependencies.publisher.staging_sink();
    let Some(sealed) = builder.seal(&mut sink).await.map_err(index_status)? else {
        return Ok(());
    };
    let sequence = candidate.allocate_sequence()?;
    let published = dependencies
        .publisher
        .publish_run(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
            sequence,
            sealed,
        )
        .await?;
    candidate.runs.push(published.manifest);
    candidate.runs.sort_by_key(|run| run.sequence);
    tracing::info!(
        index.kind = ?kind,
        gauge.anvil_index_construction_used_bytes = resident,
        gauge.anvil_index_construction_peak_bytes = workspace,
        monotonic_counter.anvil_index_flushes_total = 1_u64,
        monotonic_counter.anvil_index_runs_created_total = 1_u64,
        "index L0 run flushed"
    );
    Ok(())
}

async fn compact_until_bounded(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    kind: IndexKind,
    candidate: &mut CandidateGeneration,
    budget: &IndexMemoryBudget,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    while let Some(level) = overfull_level(&candidate.runs) {
        let _permit = budget
            .acquire(budget.limit())
            .await
            .map_err(budget_status)?;
        if let Err(error) = compact_level(
            definition,
            specification,
            kind,
            level,
            candidate,
            dependencies,
        )
        .await
        {
            tracing::info!(
                index.kind = ?kind,
                monotonic_counter.anvil_index_compaction_failures_total = 1_u64,
                "index compaction failed"
            );
            return Err(error);
        }
        tokio::task::yield_now().await;
    }
    Ok(())
}

fn overfull_level(runs: &[ManifestRun]) -> Option<u8> {
    let mut counts = BTreeMap::<u8, usize>::new();
    for run in runs {
        let count = counts.entry(run.level).or_default();
        *count += 1;
        if *count > MAX_RUNS_PER_LEVEL {
            return Some(run.level);
        }
    }
    None
}

async fn compact_level(
    definition: &CatalogDefinition,
    specification: &IndexSpecification,
    kind: IndexKind,
    level: u8,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let output_level = level
        .checked_add(1)
        .ok_or_else(|| Status::resource_exhausted("index compaction level exhausted"))?;
    let mut selected = candidate
        .runs
        .iter()
        .filter(|run| run.level == level)
        .take(MAX_RUNS_PER_LEVEL)
        .cloned()
        .collect::<Vec<_>>();
    if selected.len() != MAX_RUNS_PER_LEVEL {
        return Err(Status::internal("overfull index level has too few inputs"));
    }
    // Engines resolve equal versions by input order, newest first.
    selected.sort_by_key(|run| std::cmp::Reverse(run.sequence));
    let replacement_sequence = compaction_replacement_sequence(&selected)?;
    let directories = selected
        .iter()
        .map(|run| ManifestIndexDirectory::open(dependencies.cache.clone(), run))
        .collect::<Result<Vec<_>, _>>()
        .map_err(index_status)?;
    let mut sink = dependencies.publisher.staging_sink();
    let sealed = merge_runs(specification, &directories, output_level, &mut sink)
        .await
        .map_err(index_status)?;
    let published = dependencies
        .publisher
        .publish_run(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
            replacement_sequence,
            sealed,
        )
        .await?;
    let selected_sequences = selected
        .iter()
        .map(|run| run.sequence)
        .collect::<BTreeSet<_>>();
    candidate
        .runs
        .retain(|run| !selected_sequences.contains(&run.sequence));
    candidate.runs.push(published.manifest);
    candidate.runs.sort_by_key(|run| run.sequence);
    tracing::info!(
        index.kind = ?kind,
        monotonic_counter.anvil_index_compactions_total = 1_u64,
        histogram.anvil_index_compaction_input_runs = selected.len() as u64,
        histogram.anvil_index_compaction_output_runs = 1_u64,
        "index runs compacted"
    );
    Ok(())
}

fn compaction_replacement_sequence(inputs: &[ManifestRun]) -> Result<u64, Status> {
    inputs
        .iter()
        .map(|run| run.sequence)
        .max()
        .ok_or_else(|| Status::internal("index compaction has no sequence"))
}

async fn publish_candidate(
    definition: &CatalogDefinition,
    kind: IndexKind,
    barrier: IndexBarrier,
    candidate: CandidateGeneration,
    current: Option<&PublishedGeneration>,
    dependencies: &IndexBuilderDependencies,
) -> Result<BuildProgress, Status> {
    if !dependencies.catalog.is_current(
        definition.tenant_id,
        definition.bucket_id,
        &definition.stored.name,
        definition.stored.index_id,
        definition.object_version,
    )? {
        return Err(Status::aborted(
            "index definition changed before generation publication",
        ));
    }
    dependencies
        .journal
        .validate_publication_barrier(&barrier)
        .await
        .map_err(event_status)?;
    let result = dependencies
        .publisher
        .publish_manifest(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
            definition.object_version,
            kind,
            barrier,
            candidate.runs,
            candidate.diagnostics,
            current,
        )
        .await;
    let published = match result {
        Ok(value) => value,
        Err(error) => {
            tracing::info!(
                index.kind = ?kind,
                monotonic_counter.anvil_index_publication_cas_failures_total = 1_u64,
                "index generation publication CAS failed"
            );
            return Err(error);
        }
    };
    tracing::info!(
        index.kind = ?kind,
        gauge.anvil_index_generation = published.pointer.generation,
        monotonic_counter.anvil_index_publication_cas_total = 1_u64,
        "index generation published"
    );
    collect_obsolete_generation_artifacts(
        definition,
        kind,
        &published,
        dependencies,
        "publication",
    )
    .await;
    Ok(BuildProgress::Published)
}

async fn collect_obsolete_generation_artifacts(
    definition: &CatalogDefinition,
    kind: IndexKind,
    current: &PublishedGeneration,
    dependencies: &IndexBuilderDependencies,
    trigger: &'static str,
) {
    match dependencies
        .retention
        .collect(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
            current,
        )
        .await
    {
        Ok(deleted) if deleted > 0 && trigger == "periodic" => {
            tracing::info!(
                index.id = definition.stored.index_id,
                index.kind = ?kind,
                index.cleanup.trigger = trigger,
                monotonic_counter.anvil_index_retention_artifacts_deleted_total = deleted,
                "idle obsolete index cleanup completed"
            );
        }
        Ok(deleted) if deleted > 0 => {
            tracing::info!(
                index.id = definition.stored.index_id,
                index.kind = ?kind,
                index.cleanup.trigger = trigger,
                monotonic_counter.anvil_index_retention_artifacts_deleted_total = deleted,
                "obsolete index cleanup completed"
            );
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(
                index.id = definition.stored.index_id,
                index.kind = ?kind,
                index.cleanup.trigger = trigger,
                %error,
                "obsolete index cleanup deferred"
            );
        }
    }
}

fn source_matches_definition(
    definition: &StoredIndexDefinition,
    path: &str,
    content_type: Option<&str>,
) -> bool {
    path_matches_prefix(path, &definition.path_prefix)
        && !contains_reserved_segment(path)
        && definition
            .content_type
            .as_deref()
            .is_none_or(|expected| content_type == Some(expected))
}

fn contains_reserved_segment(path: &str) -> bool {
    path.split('/').any(|segment| segment == "_anvil")
}

fn build_object(path: &str, version: &anvil_store::Version) -> Result<IndexBuildObject, Status> {
    let blob = version
        .blob
        .as_ref()
        .ok_or_else(|| Status::data_loss("live index source version has no blob"))?;
    Ok(IndexBuildObject {
        path: path.to_owned(),
        version: version.id.0,
        content_type: version.content_type.clone(),
        content_hash: blob.hash,
        content_length: blob.length,
        committed_at_unix_millis: version.committed_at_unix_millis,
    })
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
        return Err(Status::unavailable(
            "index source advanced beyond its captured placement fence",
        ));
    }
    if stamp_fence == barrier_fence {
        let node = NodeId(u64::from(stamp.source_id.node_id));
        let Some(cursor) = barrier.sources.get(&node) else {
            return Err(Status::unavailable(
                "index source mutation is absent from the captured source vector",
            ));
        };
        if cursor.source != stamp.source_id || stamp.source_journal_position >= cursor.next_offset {
            return Err(Status::unavailable(
                "index source mutation advanced beyond its captured journal target",
            ));
        }
    }
    Ok(())
}

fn source_needs_payload(specification: &IndexSpecification) -> bool {
    !matches!(
        specification.specification,
        Some(anvil_api::v1::index_specification::Specification::Path(_))
            | Some(anvil_api::v1::index_specification::Specification::MetadataFilter(_))
    )
}

fn source_wire_limit(limit: u64) -> u64 {
    let fixed = FIXED_INDEX_SEAL_WORKSPACE_BYTES as u64;
    let safe = limit.saturating_sub(fixed).saturating_sub(256) / DECODED_SOURCE_MULTIPLIER;
    MAX_SOURCE_WIRE_BYTES.min(safe.max(64 * 1024))
}

fn work_plan(
    budget: &IndexMemoryBudget,
    encoded_source_bytes: u64,
) -> Result<SegmentMemoryPlan, Status> {
    let total = usize::try_from(budget.limit())
        .map_err(|_| Status::resource_exhausted("index construction budget exceeds platform"))?;
    let encoded = usize::try_from(encoded_source_bytes)
        .map_err(|_| Status::resource_exhausted("index source frame exceeds platform"))?;
    let reserve = encoded
        .checked_mul(DECODED_SOURCE_MULTIPLIER as usize)
        .ok_or_else(|| Status::resource_exhausted("decoded index source reserve overflow"))?;
    let available = total.checked_sub(reserve).ok_or_else(|| {
        Status::resource_exhausted("decoded index source frame exhausts its kind budget")
    })?;
    if available <= FIXED_INDEX_SEAL_WORKSPACE_BYTES + 256 {
        return Err(Status::resource_exhausted(
            "index source frame leaves no bounded builder workspace",
        ));
    }
    let configured = budget.memory_plan();
    let max_resident_bytes = configured
        .max_resident_bytes
        .min(available - FIXED_INDEX_SEAL_WORKSPACE_BYTES);
    let max_source_projection_bytes = available - max_resident_bytes;
    Ok(SegmentMemoryPlan {
        total_bytes: available,
        max_resident_bytes,
        max_source_projection_bytes,
    })
}

fn measure_snapshot_frame(frame: &[IndexSourceSnapshotHead]) -> Result<u64, Status> {
    let mut counter = ByteCounter(0);
    serde_json::to_writer(&mut counter, frame)
        .map_err(|error| Status::internal(format!("measure index snapshot frame: {error}")))?;
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

fn emit_source_lag(kind: IndexKind, from: &IndexBarrier, target: &IndexBarrier) {
    let lag = from.sources.iter().fold(0_u64, |total, (node, cursor)| {
        total.saturating_add(target.sources.get(node).map_or(0, |latest| {
            latest.next_offset.saturating_sub(cursor.next_offset)
        }))
    });
    tracing::info!(
        index.kind = ?kind,
        gauge.anvil_index_source_lag = lag,
        "index source lag observed"
    );
}

fn current_placement(decisions: &DecisionRaft) -> Result<ClusterPlacement, Status> {
    let state = decisions
        .state()
        .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
    ClusterPlacement::from_applied(&state).map_err(|error| Status::unavailable(error.to_string()))
}

fn budget_status(error: IndexBudgetError) -> Status {
    Status::resource_exhausted(error.to_string())
}

fn cpu_status(error: IndexCpuPoolError) -> Status {
    Status::internal(error.to_string())
}

fn index_status(error: IndexError) -> Status {
    match error {
        IndexError::ResourceLimit { .. } => Status::resource_exhausted(error.to_string()),
        IndexError::Io(_) => Status::unavailable(error.to_string()),
        _ => Status::data_loss(error.to_string()),
    }
}

fn event_status(error: IndexEventError) -> Status {
    match error {
        IndexEventError::PageBytesExceeded { .. } => Status::resource_exhausted(error.to_string()),
        IndexEventError::CheckpointMismatch(_)
        | IndexEventError::SourceEpochChanged(_)
        | IndexEventError::IncompleteSources => Status::failed_precondition(error.to_string()),
        _ => Status::unavailable(error.to_string()),
    }
}

fn generation_status(error: super::generation::GenerationError) -> Status {
    Status::data_loss(error.to_string())
}

#[cfg(test)]
mod tests {
    use anvil_store::{
        ObjectHeadChange, ObjectHeadChangeKind, PlacementLogId, SourceId, VersionId,
    };

    use super::*;
    use crate::index_runtime::events::{
        AtomicProgramWatermark, IndexJournalChange, IndexSourceCursor,
    };

    fn run(sequence: u64, level: u8) -> ManifestRun {
        ManifestRun {
            sequence,
            level,
            root_path: format!("_anvil/indexes/v2/9/runs/{:064x}/root", sequence),
            root_blob: anvil_store::BlobRef {
                hash: [sequence as u8; 32],
                length: 10,
            },
            root_object_version: anvil_store::VersionId(sequence),
            mutation_count: 1,
            live_document_count: 1,
            minimum_version: 1,
            maximum_version: 1,
            authoritative_bytes: 10,
        }
    }

    fn barrier(next_offset: u64) -> IndexBarrier {
        IndexBarrier {
            fence: PlacementLogId { term: 3, index: 7 },
            atomic: AtomicProgramWatermark::new(None, None, 0),
            sources: [(
                NodeId(1),
                IndexSourceCursor {
                    source: SourceId {
                        node_id: 1,
                        source_epoch: [1; 32],
                    },
                    next_offset,
                },
            )]
            .into_iter()
            .collect(),
        }
    }

    fn journal_change(
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        offset: u64,
    ) -> IndexJournalChange {
        IndexJournalChange {
            node: NodeId(1),
            change: LocalChange::ObjectHead(ObjectHeadChange {
                offset,
                tenant_id,
                bucket_id,
                exact_path: path.to_owned(),
                path_version: VersionId(offset),
                kind: ObjectHeadChangeKind::Put,
                reference_deltas: Vec::new(),
                accounting_transition: None,
            }),
        }
    }

    fn journal_page(changes: Vec<IndexJournalChange>, next_offset: u64) -> IndexJournalPage {
        IndexJournalPage {
            changes,
            through: barrier(next_offset),
            encoded_bytes: 1,
        }
    }

    #[test]
    fn compaction_replacement_uses_newest_input_sequence() {
        let inputs = (1..=4).map(|sequence| run(sequence, 0)).collect::<Vec<_>>();
        let replacement = compaction_replacement_sequence(&inputs).unwrap();
        assert_eq!(replacement, 4);
        let newer_uncompacted = run(5, 0);
        assert!(replacement < newer_uncompacted.sequence);
    }

    #[test]
    fn reserved_segment_matching_is_not_a_string_prefix_guess() {
        assert!(contains_reserved_segment("a/_anvil/meta.json"));
        assert!(!contains_reserved_segment("a/_anvilish/meta.json"));
    }

    #[test]
    fn reserved_artifact_pages_have_no_generation_source_changes() {
        let page = journal_page(
            vec![
                journal_change(1, 2, "_anvil/indexes/v2/9/current", 11),
                journal_change(
                    1,
                    2,
                    "_anvil/indexes/v2/9/manifests/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    12,
                ),
            ],
            13,
        );

        assert!(journal_source_paths(1, 2, "", &page).is_empty());
    }

    #[test]
    fn observed_artifact_progress_is_reused_for_the_next_real_mutation() {
        let published = barrier(10);
        let observed = ObservedGenerationProgress {
            current_object_version: VersionId(7),
            barrier: barrier(13),
        };
        let next_target = barrier(14);

        let start = incremental_start(VersionId(7), &published, Some(&observed));
        assert_eq!(start, &observed.barrier);
        assert!(barriers_can_advance(start, &next_target));
        assert_eq!(start.sources[&NodeId(1)].next_offset, 13);
        assert_eq!(next_target.sources[&NodeId(1)].next_offset, 14);

        let page = journal_page(vec![journal_change(1, 2, "records/real.json", 13)], 14);
        assert_eq!(
            journal_source_paths(1, 2, "records/", &page),
            BTreeMap::from([("records/real.json".to_owned(), 13)])
        );
    }

    #[test]
    fn observed_progress_does_not_cross_a_current_pointer_change() {
        let published = barrier(20);
        let observed = ObservedGenerationProgress {
            current_object_version: VersionId(7),
            barrier: barrier(24),
        };

        assert_eq!(
            incremental_start(VersionId(8), &published, Some(&observed)),
            &published
        );
    }

    #[test]
    fn retention_retry_rearms_at_the_bounded_interval() {
        let started = tokio::time::Instant::now();
        let first_retry = next_retention_retry(started);
        assert_eq!(
            first_retry.saturating_duration_since(started),
            RETENTION_RETRY_INTERVAL
        );
        assert!(!retention_retry_due(started, first_retry));
        assert!(retention_retry_due(first_retry, first_retry));

        let second_retry = next_retention_retry(first_retry);
        assert_eq!(
            second_retry.saturating_duration_since(first_retry),
            RETENTION_RETRY_INTERVAL
        );
    }

    #[test]
    fn first_overfull_level_is_selected_deterministically() {
        let mut runs = (1..=5).map(|sequence| run(sequence, 0)).collect::<Vec<_>>();
        runs.extend((6..=10).map(|sequence| run(sequence, 1)));
        assert_eq!(overfull_level(&runs), Some(0));
    }

    #[test]
    fn lost_incremental_history_requests_a_snapshot_rebuild() {
        for error in [
            IndexEventError::CheckpointMismatch(NodeId(1)),
            IndexEventError::SourceEpochChanged(NodeId(1)),
            IndexEventError::IncompleteSources,
        ] {
            assert_eq!(event_status(error).code(), tonic::Code::FailedPrecondition);
        }
    }
}
