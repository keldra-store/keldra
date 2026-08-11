//! Weighted-HRW assignment and bounded v2 index generation construction.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::io;
use std::time::Duration;

use anvil_api::v1::IndexSpecification;
use anvil_consensus::{DecisionRaft, NodeId};
use anvil_index::{
    DocumentRef, FIXED_INDEX_SEAL_WORKSPACE_BYTES, IndexError, IndexKind, SegmentMemoryPlan,
};
use anvil_store::{DefinitionKind, Head, LocalChange, ObjectKey, Store, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::IndexSourceSnapshotHead;
use crate::cluster_placement::ClusterPlacement;
use crate::index_service::{StoredIndexDefinition, definition_path, path_matches_prefix};

use super::budget::{IndexBudgetError, IndexMemoryBudget, IndexMemoryBudgets};
use super::cache::IndexCache;
use super::catalog::{CatalogChange, CatalogDefinition, CatalogIdentity, IndexCatalog};
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
use super::retention::{IndexGenerationRetention, IndexRetentionTask};
use super::scanner::{ClusterIndexScanner, ClusterIndexSourceSnapshot};

#[path = "manager/recovery.rs"]
mod recovery;
use recovery::{BuilderFailurePhase, recover_builder_failure};
#[cfg(test)]
use recovery::{BuilderFailureRecovery, failure_recovery};

const BUILDER_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const MAX_SOURCE_WIRE_BYTES: u64 = 4 * 1024 * 1024;
const DECODED_SOURCE_MULTIPLIER: u64 = 4;
const INDEX_KIND_COUNT: usize = 8;
const BUILDER_CATALOG_PAGE: usize = 256;
const MAX_OPEN_REBUILDS_PER_KIND: usize = 3;
const MAX_ACTIVE_BUILDERS: usize = 64;

pub(crate) struct IndexBuilderManagerTask {
    task: tokio::task::JoinHandle<()>,
    _retention: IndexRetentionTask,
}

impl IndexBuilderManagerTask {
    pub(crate) fn start(
        local_node: NodeId,
        decisions: DecisionRaft,
        catalog: IndexCatalog,
        dependencies: IndexBuilderDependencies,
    ) -> Self {
        // Subscribe before draining the bounded handoff so a concurrent
        // assignment mutation cannot fall between pending work and its wakeup.
        let changes = catalog.subscribe();
        let retention = dependencies.retention.start_scheduler();
        let task = tokio::spawn(run_manager(
            local_node,
            decisions,
            catalog,
            changes,
            dependencies,
        ));
        Self {
            task,
            _retention: retention,
        }
    }
}

impl Drop for IndexBuilderManagerTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
pub(crate) struct IndexBuilderDependencies {
    pub(crate) store: Store,
    pub(crate) journal: std::sync::Arc<IndexEventJournal>,
    pub(crate) scanner: ClusterIndexScanner,
    pub(crate) reader: ClusterObjectReader,
    pub(crate) publisher: IndexGenerationPublisher,
    pub(crate) retention: IndexGenerationRetention,
    pub(crate) cache: IndexCache,
    pub(crate) budgets: IndexMemoryBudgets,
    pub(crate) cpu: IndexCpuPool,
}

async fn run_manager(
    local_node: NodeId,
    decisions: DecisionRaft,
    catalog: IndexCatalog,
    mut changes: tokio::sync::broadcast::Receiver<CatalogIdentity>,
    dependencies: IndexBuilderDependencies,
) {
    let mut scheduler = BuilderScheduler::default();
    let mut workers = tokio::task::JoinSet::new();
    let mut inflight = HashMap::new();

    loop {
        let mut available = scheduler.remaining_capacity();
        match catalog.take_page(BUILDER_CATALOG_PAGE, |identity| {
            if scheduler.entries.contains_key(&identity) {
                true
            } else if available > 0 {
                available -= 1;
                true
            } else {
                false
            }
        }) {
            Ok(page) => {
                for change in page {
                    if let Err(error) = scheduler.apply_change(
                        change,
                        local_node,
                        &decisions,
                        &dependencies.retention,
                    ) {
                        tracing::debug!(%error, "bounded index builder admission deferred to assignment rediscovery");
                    }
                }
            }
            Err(error) => tracing::warn!(%error, "assigned index handoff drain will retry"),
        }
        scheduler.promote_due(tokio::time::Instant::now());
        while let Some(work) = scheduler.pop_runnable() {
            let metadata = WorkMetadata::from_job(&work);
            match builder_lease_is_current(
                &work.definition,
                local_node,
                &decisions,
                &dependencies.store,
            ) {
                Ok(true) => {}
                Ok(false) => {
                    scheduler.release(metadata, &dependencies.retention);
                    continue;
                }
                Err(error) => {
                    tracing::warn!(index.id = metadata.identity.index_id, %error, "active index assignment point-read will retry");
                    scheduler.release(metadata, &dependencies.retention);
                    continue;
                }
            }
            let task_dependencies = dependencies.clone();
            let handle = workers.spawn(async move {
                let step = advance_builder(work, &task_dependencies).await;
                (metadata, step)
            });
            inflight.insert(handle.id(), metadata);
        }

        let next_wake = scheduler
            .next_due()
            .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(60 * 60));

        tokio::select! {
            received = changes.recv() => match received {
                Ok(identity) => {
                    if let Ok(Some(change)) = catalog.take(identity, scheduler.can_admit(identity))
                        && let Err(error) = scheduler.apply_change(
                            change,
                            local_node,
                            &decisions,
                            &dependencies.retention,
                        )
                    {
                        tracing::debug!(%error, "bounded index builder admission deferred to assignment rediscovery");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            },
            completed = workers.join_next_with_id(), if !workers.is_empty() => {
                match completed {
                    Some(Ok((task_id, (metadata, step)))) => {
                        inflight.remove(&task_id);
                        scheduler.complete(metadata, step, &dependencies.retention);
                    }
                    Some(Err(error)) => {
                        let metadata = inflight.remove(&error.id());
                        tracing::warn!(%error, "bounded index builder work task failed");
                        if let Some(metadata) = metadata {
                            scheduler.lost(metadata, &dependencies.retention);
                        }
                    }
                    None => {}
                }
            },
            _ = tokio::time::sleep_until(next_wake) => {}
        }
    }
}

#[derive(Clone, Copy)]
struct WorkMetadata {
    identity: CatalogIdentity,
    definition_version: u64,
    kind: IndexKind,
    held_snapshot: bool,
}

impl WorkMetadata {
    fn from_job(job: &BuilderJob) -> Self {
        Self {
            identity: job.definition.identity(),
            definition_version: job.definition.object_version,
            kind: job.kind,
            held_snapshot: job.holds_snapshot(),
        }
    }
}

struct BuilderScheduler {
    entries: BTreeMap<CatalogIdentity, ScheduledBuilder>,
    ready_active: [VecDeque<CatalogIdentity>; INDEX_KIND_COUNT],
    ready_inspect: [VecDeque<CatalogIdentity>; INDEX_KIND_COUNT],
    delayed: BTreeMap<CatalogIdentity, (tokio::time::Instant, u64)>,
    running_kinds: [bool; INDEX_KIND_COUNT],
    open_rebuilds: [usize; INDEX_KIND_COUNT],
    prefer_active: [bool; INDEX_KIND_COUNT],
    next_kind: usize,
}

impl Default for BuilderScheduler {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            ready_active: std::array::from_fn(|_| VecDeque::new()),
            ready_inspect: std::array::from_fn(|_| VecDeque::new()),
            delayed: BTreeMap::new(),
            running_kinds: [false; INDEX_KIND_COUNT],
            open_rebuilds: [0; INDEX_KIND_COUNT],
            prefer_active: [false; INDEX_KIND_COUNT],
            next_kind: 0,
        }
    }
}

struct ScheduledBuilder {
    definition: CatalogDefinition,
    job: Option<BuilderJob>,
    queued: bool,
}

impl BuilderScheduler {
    fn remaining_capacity(&self) -> usize {
        MAX_ACTIVE_BUILDERS.saturating_sub(self.entries.len())
    }

    fn can_admit(&self, identity: CatalogIdentity) -> bool {
        self.entries.contains_key(&identity) || self.remaining_capacity() > 0
    }

    fn apply_change(
        &mut self,
        change: CatalogChange,
        local_node: NodeId,
        decisions: &DecisionRaft,
        retention: &IndexGenerationRetention,
    ) -> Result<(), Status> {
        match change {
            CatalogChange::Upsert(definition) => {
                self.upsert(definition, local_node, decisions, retention)
            }
            CatalogChange::Remove(identity) => {
                self.remove(identity, retention);
                Ok(())
            }
        }
    }

    fn upsert(
        &mut self,
        definition: CatalogDefinition,
        local_node: NodeId,
        decisions: &DecisionRaft,
        retention: &IndexGenerationRetention,
    ) -> Result<(), Status> {
        let identity = definition.identity();
        if is_local_builder(&definition, local_node, &current_placement(decisions)?)? {
            self.insert(definition, retention)
        } else {
            self.remove(identity, retention);
            Ok(())
        }
    }

    fn insert(
        &mut self,
        definition: CatalogDefinition,
        retention: &IndexGenerationRetention,
    ) -> Result<(), Status> {
        let identity = definition.identity();
        if self.entries.get(&identity).is_some_and(|entry| {
            entry.definition.object_version == definition.object_version
                && entry.definition.stored == definition.stored
        }) {
            return Ok(());
        }
        if !self.entries.contains_key(&identity) && self.entries.len() >= MAX_ACTIVE_BUILDERS {
            return Err(Status::resource_exhausted(
                "node-wide active index builder lease limit reached",
            ));
        }
        let job = BuilderJob::new(definition.clone())?;
        if let Some(previous) = self.entries.remove(&identity) {
            self.release_queued_snapshot(&previous);
            retention.unschedule(identity.tenant_id, identity.bucket_id, identity.index_id)?;
        }
        self.entries.insert(
            identity,
            ScheduledBuilder {
                definition,
                job: Some(job),
                queued: false,
            },
        );
        self.enqueue(identity);
        Ok(())
    }

    fn remove(&mut self, identity: CatalogIdentity, retention: &IndexGenerationRetention) {
        let removed = self.evict_builder(identity);
        if removed
            && let Err(error) =
                retention.unschedule(identity.tenant_id, identity.bucket_id, identity.index_id)
        {
            tracing::warn!(index.id = identity.index_id, %error, "index retention unschedule failed");
        }
    }

    fn evict_builder(&mut self, identity: CatalogIdentity) -> bool {
        self.delayed.remove(&identity);
        for queue in self
            .ready_active
            .iter_mut()
            .chain(self.ready_inspect.iter_mut())
        {
            queue.retain(|queued| *queued != identity);
        }
        let removed = self.entries.remove(&identity);
        if let Some(entry) = removed.as_ref() {
            self.release_queued_snapshot(entry);
        }
        removed.is_some()
    }

    fn enqueue(&mut self, identity: CatalogIdentity) {
        let Some(entry) = self.entries.get_mut(&identity) else {
            return;
        };
        if !entry.queued && entry.job.is_some() {
            entry.queued = true;
            let job = entry.job.as_ref().expect("queued builder has work");
            let queue = if job.is_active() {
                &mut self.ready_active[kind_slot(job.kind)]
            } else {
                &mut self.ready_inspect[kind_slot(job.kind)]
            };
            queue.push_back(identity);
        }
    }

    fn promote_due(&mut self, now: tokio::time::Instant) {
        let due = self
            .delayed
            .iter()
            .filter_map(|(identity, (due, version))| (*due <= now).then_some((*identity, *version)))
            .collect::<Vec<_>>();
        for (identity, version) in due {
            self.delayed.remove(&identity);
            if self
                .entries
                .get(&identity)
                .is_some_and(|entry| entry.definition.object_version == version)
            {
                self.enqueue(identity);
            }
        }
    }

    fn next_due(&self) -> Option<tokio::time::Instant> {
        self.delayed.values().map(|(due, _)| *due).min()
    }

    fn pop_runnable(&mut self) -> Option<BuilderJob> {
        for offset in 0..INDEX_KIND_COUNT {
            let slot = (self.next_kind + offset) % INDEX_KIND_COUNT;
            if self.running_kinds[slot] {
                continue;
            }
            let can_inspect = self.open_rebuilds[slot] < MAX_OPEN_REBUILDS_PER_KIND;
            let active_first = self.prefer_active[slot] || !can_inspect;
            let identity = if active_first {
                take_ready(&mut self.ready_active[slot], &mut self.entries, true).or_else(|| {
                    can_inspect
                        .then(|| {
                            take_ready(&mut self.ready_inspect[slot], &mut self.entries, false)
                        })
                        .flatten()
                })
            } else {
                take_ready(&mut self.ready_inspect[slot], &mut self.entries, false)
                    .or_else(|| take_ready(&mut self.ready_active[slot], &mut self.entries, true))
            };
            let Some(identity) = identity else {
                continue;
            };
            let entry = self.entries.get_mut(&identity).expect("ready entry exists");
            entry.queued = false;
            self.running_kinds[slot] = true;
            self.prefer_active[slot] = !active_first;
            self.next_kind = (slot + 1) % INDEX_KIND_COUNT;
            return entry.job.take();
        }
        None
    }

    fn complete(
        &mut self,
        metadata: WorkMetadata,
        step: BuilderStep,
        retention: &IndexGenerationRetention,
    ) {
        self.complete_with(metadata, step, |definition, identity, current| {
            match retention.schedule(
                definition,
                identity.tenant_id,
                identity.bucket_id,
                current,
            ) {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!(index.id = identity.index_id, %error, "index retention lease admission will retry");
                    false
                }
            }
        });
    }

    fn complete_with(
        &mut self,
        metadata: WorkMetadata,
        step: BuilderStep,
        mut schedule_retention: impl FnMut(
            &StoredIndexDefinition,
            CatalogIdentity,
            &PublishedGeneration,
        ) -> bool,
    ) {
        let slot = kind_slot(metadata.kind);
        self.running_kinds[slot] = false;
        if metadata.held_snapshot {
            self.open_rebuilds[slot] = self.open_rebuilds[slot].saturating_sub(1);
        }
        let Some(entry) = self.entries.get_mut(&metadata.identity) else {
            return;
        };
        if entry.definition.object_version != metadata.definition_version {
            return;
        }
        if step.job.holds_snapshot() {
            self.open_rebuilds[slot] = self.open_rebuilds[slot].saturating_add(1);
        }
        let retention_admitted = step.retention_current.as_ref().is_none_or(|current| {
            schedule_retention(&entry.definition.stored, metadata.identity, current)
        });
        entry.job = Some(step.job);
        if !retention_admitted {
            // Retention has its own bounded scheduler. Durable assignment
            // rediscovery retries admission without pinning a builder lease.
            self.evict_builder(metadata.identity);
            return;
        }
        match step.disposition {
            BuilderDisposition::Ready => self.enqueue(metadata.identity),
            BuilderDisposition::Retry(_delay) => {
                // The durable assignment is the retry queue. Yield this scarce
                // process-local lease so one failing definition cannot block a
                // later healthy definition from being discovered.
                self.evict_builder(metadata.identity);
            }
            BuilderDisposition::Idle => {
                // The published generation and durable assignment are the
                // resume point. Idle definitions consume no scheduler state;
                // the fair assignment walk will lease them again.
                self.evict_builder(metadata.identity);
            }
            BuilderDisposition::Failed => {
                tracing::error!(
                    index.id = metadata.identity.index_id,
                    definition.version = metadata.definition_version,
                    "index definition failed closed for this lease; assignment rediscovery will retry it"
                );
                self.evict_builder(metadata.identity);
            }
        }
    }

    fn lost(&mut self, metadata: WorkMetadata, retention: &IndexGenerationRetention) {
        self.release(metadata, retention);
    }

    fn release(&mut self, metadata: WorkMetadata, retention: &IndexGenerationRetention) {
        let slot = kind_slot(metadata.kind);
        self.running_kinds[slot] = false;
        if metadata.held_snapshot {
            self.open_rebuilds[slot] = self.open_rebuilds[slot].saturating_sub(1);
        }
        self.remove(metadata.identity, retention);
    }

    fn release_queued_snapshot(&mut self, entry: &ScheduledBuilder) {
        let Some(job) = entry.job.as_ref() else {
            return;
        };
        if job.holds_snapshot() {
            let slot = kind_slot(job.kind);
            self.open_rebuilds[slot] = self.open_rebuilds[slot].saturating_sub(1);
        }
    }
}

fn builder_lease_is_current(
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
    let identity = IndexIdentity::new(
        definition.tenant_id,
        definition.bucket_id,
        definition.stored.index_id,
    )
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

fn take_ready(
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

// This progress is deliberately task-local: a restart may safely rescan ignored
// reserved-artifact journal entries from the published generation barrier.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedGenerationProgress {
    current_object_version: VersionId,
    barrier: IndexBarrier,
}

struct BuilderJob {
    definition: CatalogDefinition,
    specification: IndexSpecification,
    kind: IndexKind,
    observed: Option<ObservedGenerationProgress>,
    force_snapshot_rebuild: bool,
    phase: BuilderPhase,
}

impl BuilderJob {
    fn new(definition: CatalogDefinition) -> Result<Self, Status> {
        let specification = definition.stored.specification()?;
        let kind = kind_for_specification(&specification).map_err(index_status)?;
        Ok(Self {
            definition,
            specification,
            kind,
            observed: None,
            force_snapshot_rebuild: false,
            phase: BuilderPhase::Inspect,
        })
    }

    fn is_active(&self) -> bool {
        !matches!(self.phase, BuilderPhase::Inspect)
    }

    fn holds_snapshot(&self) -> bool {
        matches!(self.phase, BuilderPhase::Rebuild(_))
    }
}

enum BuilderPhase {
    Inspect,
    Rebuild(RebuildWork),
    CatchUp(CatchUpWork),
    Publish(PublishWork),
}

struct RebuildWork {
    current: Option<PublishedGeneration>,
    _snapshot_slot: tokio::sync::OwnedSemaphorePermit,
    snapshot: ClusterIndexSourceSnapshot,
    through: IndexBarrier,
    candidate: CandidateGeneration,
}

#[derive(Clone)]
struct CatchUpWork {
    current: Option<PublishedGeneration>,
    through: IndexBarrier,
    target: IndexBarrier,
    candidate: CandidateGeneration,
    changed: bool,
    must_publish: bool,
}

#[derive(Clone)]
struct PublishWork {
    current: Option<PublishedGeneration>,
    barrier: IndexBarrier,
    candidate: CandidateGeneration,
}

struct BuilderStep {
    job: BuilderJob,
    disposition: BuilderDisposition,
    retention_current: Option<PublishedGeneration>,
}

enum BuilderDisposition {
    Ready,
    Idle,
    Retry(Duration),
    Failed,
}

async fn advance_builder(
    mut job: BuilderJob,
    dependencies: &IndexBuilderDependencies,
) -> BuilderStep {
    let phase = std::mem::replace(&mut job.phase, BuilderPhase::Inspect);
    let (failure_phase, retry_phase, result) = match phase {
        BuilderPhase::Inspect => (
            BuilderFailurePhase::Inspect,
            Some(BuilderPhase::Inspect),
            inspect_builder(&mut job, dependencies).await,
        ),
        BuilderPhase::Rebuild(work) => (
            BuilderFailurePhase::Rebuild,
            None,
            advance_rebuild(&job, work, dependencies).await,
        ),
        BuilderPhase::CatchUp(work) => {
            let retry = work.clone();
            (
                BuilderFailurePhase::CatchUp,
                Some(BuilderPhase::CatchUp(retry)),
                advance_catch_up(&mut job, work, dependencies).await,
            )
        }
        BuilderPhase::Publish(work) => {
            let retry = work.clone();
            (
                BuilderFailurePhase::Publish,
                Some(BuilderPhase::Publish(retry)),
                publish_builder(&mut job, work, dependencies).await,
            )
        }
    };
    match result {
        Ok((next, disposition, retention_current)) => {
            job.phase = next;
            BuilderStep {
                job,
                disposition,
                retention_current,
            }
        }
        Err(error) => recover_builder_failure(job, failure_phase, retry_phase, error),
    }
}

async fn inspect_builder(
    job: &mut BuilderJob,
    dependencies: &IndexBuilderDependencies,
) -> Result<
    (
        BuilderPhase,
        BuilderDisposition,
        Option<PublishedGeneration>,
    ),
    Status,
> {
    let definition = &job.definition;
    let current = dependencies
        .publisher
        .load_current(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
        )
        .await?;
    if !job.force_snapshot_rebuild
        && let Some(current) = current.as_ref()
        && current.manifest.definition_version == definition.object_version
        && current.manifest.kind == job.kind
    {
        let target = dependencies
            .journal
            .capture_barrier()
            .await
            .map_err(event_status)?;
        let published = current.manifest.barrier().map_err(generation_status)?;
        let from = incremental_start(
            current.current_object_version,
            &published,
            job.observed.as_ref(),
        )
        .clone();
        let retention_current = Some(current.clone());
        if barriers_can_advance(&from, &target) {
            if from == target {
                job.observed = Some(ObservedGenerationProgress {
                    current_object_version: current.current_object_version,
                    barrier: target,
                });
                return Ok((
                    BuilderPhase::Inspect,
                    BuilderDisposition::Idle,
                    retention_current,
                ));
            }
            let candidate = CandidateGeneration::incremental(current);
            emit_source_lag(job.kind, &from, &target);
            return Ok((
                BuilderPhase::CatchUp(CatchUpWork {
                    current: Some(current.clone()),
                    through: from,
                    target,
                    candidate,
                    changed: false,
                    must_publish: false,
                }),
                BuilderDisposition::Ready,
                retention_current,
            ));
        }
    }

    tracing::info!(
        index.id = job.definition.stored.index_id,
        index.kind = ?job.kind,
        tenant_id = job.definition.tenant_id,
        bucket_id = job.definition.bucket_id,
        monotonic_counter.anvil_index_rebuilds_total = 1_u64,
        "index snapshot rebuild started"
    );
    job.observed = None;
    let budget = dependencies.budgets.for_kind(job.kind);
    let snapshot_slot = budget
        .acquire_snapshot_slot()
        .await
        .map_err(budget_status)?;
    let max_frame_bytes = source_wire_limit(budget.limit());
    let (expected_fence, expected_atomic) = dependencies
        .journal
        .snapshot_authority()
        .map_err(event_status)?;
    let snapshot = dependencies
        .scanner
        .begin_source_snapshot(
            definition.tenant_id,
            definition.bucket_id,
            definition.stored.path_prefix.clone(),
            max_frame_bytes,
        )
        .await?;
    if snapshot.placement_fence() != expected_fence {
        return Err(Status::unavailable(
            "cluster placement changed while opening index source snapshots",
        ));
    }
    let tails = snapshot
        .checkpoints()
        .iter()
        .map(|checkpoint| (checkpoint.node, checkpoint.source, checkpoint.captured_tail))
        .collect::<Vec<_>>();
    let through = dependencies
        .journal
        .barrier_from_snapshot_tails(snapshot.placement_fence(), expected_atomic, &tails)
        .map_err(event_status)?;
    job.force_snapshot_rebuild = false;
    Ok((
        BuilderPhase::Rebuild(RebuildWork {
            current,
            _snapshot_slot: snapshot_slot,
            snapshot,
            through,
            candidate: CandidateGeneration::rebuild(),
        }),
        BuilderDisposition::Ready,
        None,
    ))
}

async fn advance_rebuild(
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
    if compact_one_if_needed(job, &mut work.candidate, dependencies).await? {
        return Ok((BuilderPhase::Rebuild(work), BuilderDisposition::Ready, None));
    }
    let budget = dependencies.budgets.for_kind(job.kind);
    let permit = budget
        .acquire(budget.limit())
        .await
        .map_err(budget_status)?;
    let frame = work.snapshot.next_frame().await?;
    match frame {
        Some(frame) => {
            let encoded_bytes = measure_snapshot_frame(&frame)?;
            let plan = work_plan(budget, encoded_bytes)?;
            process_snapshot_frame(
                &job.definition,
                &job.specification,
                job.kind,
                &work.through,
                frame,
                plan,
                &mut work.candidate,
                dependencies,
            )
            .await?;
            drop(permit);
            Ok((BuilderPhase::Rebuild(work), BuilderDisposition::Ready, None))
        }
        None => {
            drop(permit);
            let target = dependencies
                .journal
                .capture_barrier()
                .await
                .map_err(event_status)?;
            emit_source_lag(job.kind, &work.through, &target);
            Ok((
                BuilderPhase::CatchUp(CatchUpWork {
                    current: work.current,
                    through: work.through,
                    target,
                    candidate: work.candidate,
                    changed: false,
                    must_publish: true,
                }),
                BuilderDisposition::Ready,
                None,
            ))
        }
    }
}

async fn advance_catch_up(
    job: &mut BuilderJob,
    mut work: CatchUpWork,
    dependencies: &IndexBuilderDependencies,
) -> Result<
    (
        BuilderPhase,
        BuilderDisposition,
        Option<PublishedGeneration>,
    ),
    Status,
> {
    if compact_one_if_needed(job, &mut work.candidate, dependencies).await? {
        return Ok((BuilderPhase::CatchUp(work), BuilderDisposition::Ready, None));
    }
    let budget = dependencies.budgets.for_kind(job.kind);
    let permit = budget
        .acquire(budget.limit())
        .await
        .map_err(budget_status)?;
    let page = dependencies
        .journal
        .next_page(
            job.definition.tenant_id,
            job.definition.bucket_id,
            &work.through,
            &work.target,
            source_wire_limit(budget.limit()),
        )
        .await
        .map_err(event_status)?;
    match page {
        Some(page) => {
            let plan = work_plan(budget, page.encoded_bytes)?;
            work.changed |= process_journal_page(
                &job.definition,
                &job.specification,
                job.kind,
                &work.target,
                &page,
                plan,
                &mut work.candidate,
                dependencies,
            )
            .await?;
            work.through = page.through;
            drop(permit);
            Ok((BuilderPhase::CatchUp(work), BuilderDisposition::Ready, None))
        }
        None => {
            drop(permit);
            if work.through != work.target {
                return Err(Status::unavailable(
                    "index catch-up did not reach its complete source barrier",
                ));
            }
            if work.must_publish || work.changed {
                return Ok((
                    BuilderPhase::Publish(PublishWork {
                        current: work.current,
                        barrier: work.through,
                        candidate: work.candidate,
                    }),
                    BuilderDisposition::Ready,
                    None,
                ));
            }
            let current = work.current.ok_or_else(|| {
                Status::internal("incremental builder lost its current generation")
            })?;
            job.observed = Some(ObservedGenerationProgress {
                current_object_version: current.current_object_version,
                barrier: work.through,
            });
            Ok((
                BuilderPhase::Inspect,
                BuilderDisposition::Idle,
                Some(current),
            ))
        }
    }
}

async fn publish_builder(
    job: &mut BuilderJob,
    work: PublishWork,
    dependencies: &IndexBuilderDependencies,
) -> Result<
    (
        BuilderPhase,
        BuilderDisposition,
        Option<PublishedGeneration>,
    ),
    Status,
> {
    let published = publish_candidate(
        &job.definition,
        job.kind,
        work.barrier,
        work.candidate,
        work.current.as_ref(),
        dependencies,
    )
    .await?;
    let (next, disposition) = complete_publication(job);
    Ok((next, disposition, Some(published)))
}

fn complete_publication(job: &mut BuilderJob) -> (BuilderPhase, BuilderDisposition) {
    job.observed = None;
    (BuilderPhase::Inspect, BuilderDisposition::Idle)
}

async fn compact_one_if_needed(
    job: &BuilderJob,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<bool, Status> {
    let Some(level) = overfull_level(&candidate.runs) else {
        return Ok(false);
    };
    let budget = dependencies.budgets.for_kind(job.kind);
    let _permit = budget
        .acquire(budget.limit())
        .await
        .map_err(budget_status)?;
    compact_level(
        &job.definition,
        &job.specification,
        job.kind,
        level,
        candidate,
        dependencies,
    )
    .await?;
    Ok(true)
}

fn is_local_builder(
    definition: &CatalogDefinition,
    local_node: NodeId,
    placement: &ClusterPlacement,
) -> Result<bool, Status> {
    let identity = IndexIdentity::new(
        definition.tenant_id,
        definition.bucket_id,
        definition.stored.index_id,
    )
    .map_err(|error| Status::data_loss(error.to_string()))?;
    Ok(IndexPlacement::derive(identity, placement)
        .map_err(|error| Status::unavailable(error.to_string()))?
        .builder()
        == local_node)
}

fn kind_slot(kind: IndexKind) -> usize {
    kind as u8 as usize - 1
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

#[derive(Clone)]
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
        .current_head_snapshot_stable(&key, definition.tenant_id, definition.bucket_id)
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
    let version = &snapshot.version;
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
) -> Result<PublishedGeneration, Status> {
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
                index.id = definition.stored.index_id,
                index.kind = ?kind,
                monotonic_counter.anvil_index_publication_cas_failures_total = 1_u64,
                "index generation publication CAS failed"
            );
            return Err(error);
        }
    };
    tracing::info!(
        index.id = definition.stored.index_id,
        index.kind = ?kind,
        gauge.anvil_index_generation = published.pointer.generation,
        monotonic_counter.anvil_index_publication_cas_total = 1_u64,
        "index generation published"
    );
    Ok(published)
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
        IndexEventError::Placement(_)
        | IndexEventError::AtomicProgramInProgress
        | IndexEventError::Source { .. }
        | IndexEventError::Task(_) => Status::unavailable(error.to_string()),
        IndexEventError::BarrierChanged
        | IndexEventError::CheckpointMismatch(_)
        | IndexEventError::SourceEpochChanged(_)
        | IndexEventError::SourceHistoryGap(_)
        | IndexEventError::IncompleteSources => Status::failed_precondition(error.to_string()),
        IndexEventError::PageBytesExceeded { .. } => Status::resource_exhausted(error.to_string()),
        IndexEventError::ZeroPageByteLimit => Status::invalid_argument(error.to_string()),
        IndexEventError::InvalidSourceStatus(_)
        | IndexEventError::NonContiguousSource(_)
        | IndexEventError::OffsetOverflow(_)
        | IndexEventError::PageLengthOverflow
        | IndexEventError::PageLengthMismatch { .. }
        | IndexEventError::Encode(_) => Status::data_loss(error.to_string()),
    }
}

fn generation_status(error: super::generation::GenerationError) -> Status {
    Status::data_loss(error.to_string())
}

#[cfg(test)]
#[path = "manager/tests.rs"]
mod tests;
