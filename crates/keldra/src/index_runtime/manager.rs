//! Weighted-HRW assignment and bounded format-v4 index commit construction.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use keldra_consensus::{DecisionRaft, NodeId};
use keldra_index::v4::build::{
    BuildLimits, BuiltSegment, ComponentBatchSink, MergeMutation, NativeSegmentWriter,
    ProjectedSource as NativeProjectedSource, SourcePush,
};
use keldra_index::v4::{
    DocIdRange, LocatorEntry, LocatorStreamRoot, LocatorValue, ObjectIdentity, Schema,
    SegmentDescriptor, SegmentIdentity, locate_path_values, publish_locator_delta,
    rewrite_segment_live_mask,
};
use keldra_index::{
    FIXED_INDEX_SEAL_WORKSPACE_BYTES, IndexError, IndexKind, MIN_INDEX_KIND_MEMORY_BYTES,
    SegmentMemoryPlan,
};
use keldra_store::{DefinitionKind, LocalChange, ObjectKey, Store, VersionId};
use tonic::Status;
use tracing::Instrument;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::IndexSourceSnapshotHead;
use crate::cluster_placement::ClusterPlacement;
use crate::derived_consumer::{
    DerivedBarrierEvidence, DerivedDefinitionIdentity, DerivedProgressReporter,
};
use crate::index_config::IndexRuntimeConfig;
use crate::index_service::{StoredIndexDefinition, definition_path, path_matches_prefix};

use super::budget::{IndexBudgetError, IndexMemoryBudgets, IndexMemoryPermit};
use super::cache::IndexCache;
use super::catalog::{CatalogChange, CatalogDefinition, CatalogIdentity, IndexCatalog};
use super::committed_view::{
    LocatorPackOwnership, LocatorRoot, MAX_LOCATOR_ROOTS_PER_COMMIT, MAX_SEGMENTS_PER_COMMIT,
};
use super::cpu::{IndexCpuPool, IndexCpuPoolError};
use super::directory::ManifestArtifactDirectory;
use super::events::{IndexBarrier, IndexEventError, IndexEventJournal, IndexJournalPage};
use super::placement::{IndexIdentity, IndexPlacement};
use super::publication::DerivedArtifactAdmission;
use super::publisher::{CommittedIndexView, IndexCommitPublisher};
use super::retention::{IndexCommitRetention, IndexRetentionTask};
use super::scanner::{ClusterIndexScanner, ClusterIndexSourceSnapshot};
use super::source::{IndexBuildDiagnostics, IndexBuildObject, IndexSourceMutation};
use super::telemetry::{
    BuilderProgress, BuilderProgressPhase, IndexTelemetryIdentity, await_with_builder_heartbeats,
    emit_compaction_debt,
};
use super::v4_projection::{project_mutation, projection_admission_bytes};

#[path = "manager/catch_up.rs"]
mod catch_up;
use catch_up::process_journal_page;
#[path = "manager/candidate.rs"]
mod candidate;
use candidate::{CandidateCommit, manifest_physical_order, runtime_kind};
#[path = "manager/debt.rs"]
mod debt;
use debt::{DebtLimits, DebtSelection};
#[path = "manager/locator_debt.rs"]
mod locator_debt;
#[path = "manager/observability.rs"]
mod observability;
#[path = "manager/publication.rs"]
mod publication;
use publication::{AbortOnDropTask, start_candidate_publication};
pub(crate) use publication::{IndexMaintenanceWorkSlots, IndexPublicationSlots};
#[path = "manager/publication_cohort.rs"]
pub(crate) mod publication_cohort;
pub(crate) use publication_cohort::PublicationCohortClass;
#[path = "manager/recovery.rs"]
mod recovery;
use recovery::{BuilderFailurePhase, recover_builder_failure};
#[cfg(test)]
use recovery::{BuilderFailureRecovery, failure_recovery};
#[path = "manager/support.rs"]
mod support;
use support::*;
#[path = "manager/quantum.rs"]
mod quantum;
use quantum::{SourceWorkBoundary, SourceWorkQuantum};
#[path = "manager/rebuild.rs"]
mod rebuild;
use rebuild::{RebuildWork, advance_rebuild, resume_durable_rebuild, start_rebuild_work};
#[path = "manager/v4_merge.rs"]
mod v4_merge;

const BUILDER_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const MAX_SOURCE_WIRE_BYTES: u64 = 16 * 1024 * 1024;
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
    pub(crate) publisher: IndexCommitPublisher,
    pub(crate) retention: IndexCommitRetention,
    pub(crate) cache: IndexCache,
    pub(crate) budgets: IndexMemoryBudgets,
    pub(crate) cpu: IndexCpuPool,
    pub(crate) config: IndexRuntimeConfig,
    pub(crate) derived_progress: DerivedProgressReporter,
    pub(crate) maintenance_work_slots: IndexMaintenanceWorkSlots,
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
    let mut running = HashMap::new();

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
                    abort_replaced_worker(&change, &scheduler, &mut running, &mut inflight);
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
                let step = advance_builder(work, task_dependencies).await;
                (metadata, step)
            });
            let task_id = handle.id();
            running.insert(metadata.identity, (task_id, handle));
            inflight.insert(task_id, metadata);
        }

        let next_wake = scheduler
            .next_due()
            .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(60 * 60));

        tokio::select! {
            received = changes.recv() => match received {
                Ok(identity) => {
                    if let Ok(Some(change)) = catalog.take(identity, scheduler.can_admit(identity))
                    {
                        abort_replaced_worker(&change, &scheduler, &mut running, &mut inflight);
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
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            },
            completed = workers.join_next_with_id(), if !workers.is_empty() => {
                match completed {
                    Some(Ok((task_id, (metadata, step)))) => {
                        inflight.remove(&task_id);
                        remove_running_task(&mut running, metadata.identity, task_id);
                        scheduler.complete(metadata, step, &dependencies.retention);
                    }
                    Some(Err(error)) => {
                        let metadata = inflight.remove(&error.id());
                        tracing::debug!(%error, "bounded index builder work task will retry");
                        if let Some(metadata) = metadata {
                            remove_running_task(&mut running, metadata.identity, error.id());
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

fn abort_replaced_worker(
    change: &CatalogChange,
    scheduler: &BuilderScheduler,
    running: &mut HashMap<CatalogIdentity, (tokio::task::Id, tokio::task::AbortHandle)>,
    inflight: &mut HashMap<tokio::task::Id, WorkMetadata>,
) {
    let identity = change.identity();
    let replaces = match change {
        CatalogChange::Upsert(definition) => {
            scheduler.entries.get(&identity).is_some_and(|entry| {
                entry.definition.object_version != definition.object_version
                    || entry.definition.stored != definition.stored
            })
        }
        CatalogChange::Delete { .. } | CatalogChange::Remove(_) => true,
    };
    if !replaces {
        return;
    }
    if let Some((task_id, handle)) = running.remove(&identity) {
        inflight.remove(&task_id);
        handle.abort();
    }
}

fn remove_running_task(
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
struct WorkMetadata {
    identity: CatalogIdentity,
    definition_version: u64,
    kind: IndexKind,
    held_snapshot: bool,
    inspecting: bool,
}

impl WorkMetadata {
    fn from_job(job: &BuilderJob) -> Self {
        Self {
            identity: job.definition.identity(),
            definition_version: job.definition.object_version,
            kind: job.kind,
            held_snapshot: job.holds_snapshot(),
            inspecting: matches!(job.phase, BuilderPhase::Inspect),
        }
    }
}

struct BuilderScheduler {
    entries: BTreeMap<CatalogIdentity, ScheduledBuilder>,
    ready_active: [VecDeque<CatalogIdentity>; INDEX_KIND_COUNT],
    ready_inspect: [VecDeque<CatalogIdentity>; INDEX_KIND_COUNT],
    delayed: BTreeMap<CatalogIdentity, (tokio::time::Instant, u64)>,
    running_inspects: [usize; INDEX_KIND_COUNT],
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
            running_inspects: [0; INDEX_KIND_COUNT],
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
    wake_pending: bool,
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
        retention: &IndexCommitRetention,
    ) -> Result<(), Status> {
        match change {
            CatalogChange::Upsert(definition) => {
                self.upsert(definition, local_node, decisions, retention)
            }
            CatalogChange::Delete { identity, .. } => {
                // Definition delivery already committed the durable scoped
                // cleanup schedule. Stop active construction without erasing
                // that restart-safe handoff.
                self.evict_builder(identity);
                Ok(())
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
        retention: &IndexCommitRetention,
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
        retention: &IndexCommitRetention,
    ) -> Result<(), Status> {
        let identity = definition.identity();
        if self.record_same_definition_wake(&definition) {
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
                wake_pending: false,
            },
        );
        self.enqueue(identity);
        Ok(())
    }

    fn record_same_definition_wake(&mut self, definition: &CatalogDefinition) -> bool {
        let identity = definition.identity();
        let Some(entry) = self.entries.get_mut(&identity) else {
            return false;
        };
        if entry.definition.object_version != definition.object_version
            || entry.definition.stored != definition.stored
        {
            return false;
        }
        entry.wake_pending = true;
        true
    }

    fn remove(&mut self, identity: CatalogIdentity, retention: &IndexCommitRetention) {
        self.evict_builder(identity);
        if let Err(error) =
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
            let can_inspect = self.open_rebuilds[slot].saturating_add(self.running_inspects[slot])
                < MAX_OPEN_REBUILDS_PER_KIND;
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
            if !entry.job.as_ref().is_some_and(BuilderJob::is_active) {
                self.running_inspects[slot] = self.running_inspects[slot].saturating_add(1);
            }
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
        retention: &IndexCommitRetention,
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
            &CommittedIndexView,
        ) -> bool,
    ) {
        let slot = kind_slot(metadata.kind);
        if metadata.inspecting {
            self.running_inspects[slot] = self.running_inspects[slot].saturating_sub(1);
        }
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
            BuilderDisposition::Retry(delay) => {
                self.delayed.insert(
                    metadata.identity,
                    (
                        tokio::time::Instant::now() + delay,
                        metadata.definition_version,
                    ),
                );
            }
            BuilderDisposition::Idle => {
                if entry.wake_pending {
                    entry.wake_pending = false;
                    self.enqueue(metadata.identity);
                } else {
                    self.evict_builder(metadata.identity);
                }
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

    fn lost(&mut self, metadata: WorkMetadata, retention: &IndexCommitRetention) {
        self.release(metadata, retention);
    }

    fn release(&mut self, metadata: WorkMetadata, retention: &IndexCommitRetention) {
        let slot = kind_slot(metadata.kind);
        if metadata.inspecting {
            self.running_inspects[slot] = self.running_inspects[slot].saturating_sub(1);
        }
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

struct BuilderJob {
    definition: CatalogDefinition,
    kind: IndexKind,
    phase: BuilderPhase,
}

impl BuilderJob {
    fn new(definition: CatalogDefinition) -> Result<Self, Status> {
        let kind = runtime_kind(definition.schema.kind);
        Ok(Self {
            definition,
            kind,
            phase: BuilderPhase::Inspect,
        })
    }

    fn is_active(&self) -> bool {
        !matches!(self.phase, BuilderPhase::Inspect)
    }

    fn holds_snapshot(&self) -> bool {
        matches!(self.phase, BuilderPhase::Rebuild(_))
    }

    fn telemetry_identity(&self) -> IndexTelemetryIdentity {
        IndexTelemetryIdentity {
            index_id: self.definition.stored.index_id,
            tenant_id: self.definition.tenant_id,
            bucket_id: self.definition.bucket_id,
            kind: self.kind,
        }
    }
}

enum BuilderPhase {
    Inspect,
    Rebuild(RebuildWork),
    CatchUp(CatchUpWork),
}

struct CatchUpWork {
    current: Option<CommittedIndexView>,
    through: IndexBarrier,
    target: IndexBarrier,
    candidate: CandidateCommit,
    changed: bool,
    must_publish: bool,
    checkpoint_started: Option<Instant>,
    maintenance: bool,
    progress: BuilderProgress,
    active: Option<ActiveIncrementalBuffer>,
    publishing: Option<InFlightPublication>,
    atomic_projection: Option<catch_up::AtomicProjectionWork>,
}

struct InFlightPublication {
    current: Option<CommittedIndexView>,
    barrier: IndexBarrier,
    candidate: CandidateCommit,
    admission: DerivedArtifactAdmission,
    task: AbortOnDropTask<Result<CommittedIndexView, Status>>,
}

struct ActiveIncrementalBuffer {
    builder: NativeSegmentBuild,
    permit: IndexMemoryPermit,
    quantum: SourceWorkQuantum,
    started: Option<Instant>,
    operations: u64,
}

struct BuilderStep {
    job: BuilderJob,
    disposition: BuilderDisposition,
    retention_current: Option<CommittedIndexView>,
}

enum BuilderDisposition {
    Ready,
    Idle,
    Retry(Duration),
    Failed,
}

async fn advance_builder(
    mut job: BuilderJob,
    dependencies: IndexBuilderDependencies,
) -> BuilderStep {
    let phase = std::mem::replace(&mut job.phase, BuilderPhase::Inspect);
    let (failure_phase, retry_phase, result) = match phase {
        BuilderPhase::Inspect => (
            BuilderFailurePhase::Inspect,
            Some(BuilderPhase::Inspect),
            inspect_builder(&mut job, &dependencies).await,
        ),
        BuilderPhase::Rebuild(work) => (
            BuilderFailurePhase::Rebuild,
            None,
            advance_rebuild(&job, work, &dependencies).await,
        ),
        BuilderPhase::CatchUp(work) => (
            BuilderFailurePhase::CatchUp,
            None,
            advance_catch_up(&mut job, work, &dependencies).await,
        ),
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
    job: &BuilderJob,
    dependencies: &IndexBuilderDependencies,
) -> Result<(BuilderPhase, BuilderDisposition, Option<CommittedIndexView>), Status> {
    let telemetry_identity = job.telemetry_identity();
    let definition = &job.definition;
    let current = dependencies
        .publisher
        .load_current(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
        )
        .await?;
    let budget = dependencies.budgets.for_kind(job.kind);
    let max_frame_bytes =
        source_wire_limit(budget.limit()).min(dependencies.config.source_quantum_bytes(job.kind));
    if let Some(work) =
        resume_durable_rebuild(job, current.clone(), max_frame_bytes, dependencies).await?
    {
        return Ok((BuilderPhase::Rebuild(work), BuilderDisposition::Ready, None));
    }
    emit_publication_age(job.kind, current.as_ref());
    if let Some(current) = current.as_ref()
        && current.manifest.definition_version == definition.object_version
        && current.manifest.kind == definition.schema.kind
    {
        let published = current.manifest.barrier().map_err(commit_view_status)?;
        let target = dependencies
            .journal
            .capture_index_bucket_barrier(
                definition.tenant_id,
                definition.bucket_id,
                Some(&published),
            )
            .await
            .map_err(event_status)?;
        dependencies
            .derived_progress
            .report(
                derived_identity(definition),
                DerivedBarrierEvidence::Published(published.clone()),
            )
            .await;
        let from = published.clone();
        let retention_current = Some(current.clone());
        emit_source_lag(job.kind, &from, &target);
        if from == target {
            let maintenance_limits = DebtLimits::maintenance();
            if debt::select(&current.manifest.segments, maintenance_limits).is_some()
                || debt::select_locator_roots(&current.manifest.locator_roots, maintenance_limits)
                    .is_some()
            {
                return Ok((
                    BuilderPhase::CatchUp(CatchUpWork {
                        current: Some(current.clone()),
                        through: target.clone(),
                        target,
                        candidate: CandidateCommit::incremental(current),
                        changed: false,
                        must_publish: true,
                        checkpoint_started: None,
                        maintenance: true,
                        progress: BuilderProgress::start(
                            telemetry_identity,
                            BuilderProgressPhase::CatchUp,
                        ),
                        active: None,
                        publishing: None,
                        atomic_projection: None,
                    }),
                    BuilderDisposition::Ready,
                    retention_current,
                ));
            }
            return Ok((
                BuilderPhase::Inspect,
                BuilderDisposition::Idle,
                retention_current,
            ));
        }
        let candidate = CandidateCommit::incremental(current);
        return Ok((
            BuilderPhase::CatchUp(CatchUpWork {
                current: Some(current.clone()),
                through: from,
                target,
                candidate,
                changed: false,
                must_publish: false,
                checkpoint_started: None,
                maintenance: false,
                progress: BuilderProgress::start(telemetry_identity, BuilderProgressPhase::CatchUp),
                active: None,
                publishing: None,
                atomic_projection: None,
            }),
            BuilderDisposition::Ready,
            retention_current,
        ));
    }

    tracing::info!(
        index.id = job.definition.stored.index_id,
        index.kind = ?job.kind,
        tenant_id = job.definition.tenant_id,
        bucket_id = job.definition.bucket_id,
        "index snapshot rebuild started"
    );
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
            None,
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
    let work = start_rebuild_work(
        job,
        current,
        snapshot,
        through,
        max_frame_bytes,
        BuilderProgress::start(telemetry_identity, BuilderProgressPhase::Rebuild),
        dependencies,
    )
    .await?;
    Ok((BuilderPhase::Rebuild(work), BuilderDisposition::Ready, None))
}

async fn advance_catch_up(
    job: &BuilderJob,
    mut work: CatchUpWork,
    dependencies: &IndexBuilderDependencies,
) -> Result<(BuilderPhase, BuilderDisposition, Option<CommittedIndexView>), Status> {
    let debt_limits = if work.maintenance {
        DebtLimits::maintenance()
    } else {
        DebtLimits::new(
            dependencies.config.max_segments_per_tier(job.kind) as usize,
            dependencies.config.max_unmerged_bytes_per_tier(job.kind),
        )
    };
    let admission = publication_admission(work.maintenance);
    if work
        .publishing
        .as_ref()
        .is_some_and(|publication| publication.task.is_finished())
    {
        let publication = work
            .publishing
            .take()
            .expect("finished publication remains installed");
        let InFlightPublication {
            current,
            barrier,
            candidate,
            admission,
            task,
        } = publication;
        match task.join().await {
            Ok(Ok(published)) => {
                if catch_up::published_candidate_requires_locator_maintenance(&work, &published)? {
                    work.progress.complete();
                    return Ok((
                        BuilderPhase::Inspect,
                        BuilderDisposition::Ready,
                        Some(published),
                    ));
                }
                work.current = Some(published.clone());
                return Ok((
                    BuilderPhase::CatchUp(work),
                    BuilderDisposition::Ready,
                    Some(published),
                ));
            }
            result => {
                let error = match result {
                    Ok(Err(error)) => error,
                    Err(error) => Status::internal(format!(
                        "asynchronous index publication task failed: {error}"
                    )),
                    Ok(Ok(_)) => unreachable!("successful publication handled above"),
                };
                if matches!(
                    error.code(),
                    tonic::Code::Aborted | tonic::Code::FailedPrecondition
                ) {
                    tracing::debug!(
                        index.kind = ?job.kind,
                        %error,
                        "discarding publication candidate whose current, source, or definition authority changed"
                    );
                    work.progress.complete();
                    return Ok((BuilderPhase::Inspect, BuilderDisposition::Ready, None));
                }
                tracing::debug!(
                    index.kind = ?job.kind,
                    %error,
                    "retrying exact failed index publication without discarding later intake"
                );
                let task = start_candidate_publication(
                    job.definition.clone(),
                    job.kind,
                    barrier.clone(),
                    candidate.clone(),
                    current.clone(),
                    admission,
                    dependencies.clone(),
                );
                work.publishing = Some(InFlightPublication {
                    current,
                    barrier,
                    candidate,
                    admission,
                    task,
                });
                return Ok((
                    BuilderPhase::CatchUp(work),
                    BuilderDisposition::Retry(BUILDER_RETRY_INTERVAL),
                    None,
                ));
            }
        }
    }
    if work.publishing.is_some()
        && work.atomic_projection.is_none()
        && work
            .active
            .as_ref()
            .is_some_and(|active| active.builder.is_empty() && active.builder.frozen.is_none())
    {
        work.active = None;
    }
    if work.publishing.is_some() && work.active.is_none() {
        return Ok((
            BuilderPhase::CatchUp(work),
            BuilderDisposition::Retry(Duration::from_millis(10)),
            None,
        ));
    }
    if work.active.is_none()
        && catch_up::should_compact_before_catch_up(
            work.maintenance,
            work.candidate.segments.len(),
            work.candidate.locator_roots.len(),
        )
        && compact_one_if_needed(job, &mut work.candidate, debt_limits, dependencies).await?
    {
        return Ok((BuilderPhase::CatchUp(work), BuilderDisposition::Ready, None));
    }
    let mut active = match work.active.take() {
        Some(active) => active,
        None => {
            let budget = dependencies.budgets.for_kind(job.kind);
            let permit = await_with_builder_heartbeats(
                &work.progress,
                budget.acquire_up_to(MIN_INDEX_KIND_MEMORY_BYTES as u64, budget.limit()),
            )
            .await
            .map_err(budget_status)?;
            let granted_bytes = permit.bytes();
            let plan = work_plan_for_limit(
                granted_bytes,
                0,
                dependencies.config.segment_flush_bytes(job.kind),
            )?;
            ActiveIncrementalBuffer {
                builder: NativeSegmentBuild::new(
                    job,
                    plan,
                    SegmentPublicationLane::Incremental,
                    dependencies,
                )?,
                permit,
                quantum: SourceWorkQuantum::from_budget_limit(granted_bytes),
                started: None,
                operations: 0,
            }
        }
    };
    loop {
        // A staged atomic unit was based on this exact candidate graph. Finish
        // it before publication or maintenance can replace that base; the
        // staged graph remains invisible until its Done transition.
        if work.atomic_projection.is_none()
            && catch_up::locator_publication_required(&work.candidate, &active.builder)
        {
            return catch_up::stop_at_locator_headroom(job, work, active, admission, dependencies)
                .await;
        }
        let Some(page_limit) = active.quantum.remaining() else {
            if !active.builder.is_empty() {
                freeze_builder(
                    &job.definition,
                    job.kind,
                    &mut active.builder,
                    &mut work.candidate,
                    dependencies,
                )
                .await?;
            }
            active.quantum = SourceWorkQuantum::from_budget_limit(active.permit.bytes());
            work.active = Some(active);
            return Ok((BuilderPhase::CatchUp(work), BuilderDisposition::Ready, None));
        };
        let page = await_with_builder_heartbeats(
            &work.progress,
            dependencies.journal.next_page(
                job.definition.tenant_id,
                job.definition.bucket_id,
                &work.through,
                &work.target,
                page_limit,
            ),
        )
        .await;
        let page = match page {
            Ok(page) => page,
            Err(IndexEventError::PageBytesExceeded { bytes, .. })
                if active.quantum.defer_page_to_next_quantum(bytes) =>
            {
                if !active.builder.is_empty() {
                    freeze_builder(
                        &job.definition,
                        job.kind,
                        &mut active.builder,
                        &mut work.candidate,
                        dependencies,
                    )
                    .await?;
                }
                active.quantum = SourceWorkQuantum::from_budget_limit(active.permit.bytes());
                work.active = Some(active);
                return Ok((BuilderPhase::CatchUp(work), BuilderDisposition::Ready, None));
            }
            Err(error) => return Err(event_status(error)),
        };
        let Some(page) = page else {
            if work.through != work.target {
                return Err(Status::unavailable(
                    "index catch-up did not reach its complete source barrier",
                ));
            }
            if !work.maintenance && (work.changed || work.must_publish) {
                let publication_started =
                    catch_up::earliest_publication_start(active.started, work.checkpoint_started);
                let remaining = publication_started
                    .map(|started| started + dependencies.config.segment_flush_max_age())
                    .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
                    .filter(|remaining| !remaining.is_zero());
                if let Some(remaining) = remaining {
                    work.active = Some(active);
                    return Ok((
                        BuilderPhase::CatchUp(work),
                        BuilderDisposition::Retry(remaining),
                        None,
                    ));
                }
            }
            if work.publishing.is_some() {
                work.active = Some(active);
                return Ok((
                    BuilderPhase::CatchUp(work),
                    BuilderDisposition::Retry(Duration::from_millis(10)),
                    None,
                ));
            }
            flush_builder(
                &job.definition,
                job.kind,
                &mut active.builder,
                &mut work.candidate,
                dependencies,
            )
            .await?;
            // Incremental publication owns the journal-retention critical
            // path. Segment and locator debt is repaid only after this exact
            // checkpoint is committed; a merge must never delay source
            // checkpoint publication.
            if work.must_publish || work.changed {
                enqueue_candidate_publication(job, &mut work, admission, dependencies).await?;
                drop(active);
                return Ok((
                    BuilderPhase::CatchUp(work),
                    BuilderDisposition::Retry(Duration::from_millis(10)),
                    None,
                ));
            }
            let current = work.current.ok_or_else(|| {
                Status::internal("incremental builder lost its current committed view")
            })?;
            work.progress.complete();
            return Ok((
                BuilderPhase::Inspect,
                BuilderDisposition::Idle,
                Some(current),
            ));
        };

        let page_resident_bytes = catch_up::journal_page_resident_bytes(&page)?;
        let admitted_resident_bytes = page_resident_bytes
            .checked_add(active.builder.frozen_resident_charge())
            .ok_or_else(|| Status::resource_exhausted("index pipeline resident bytes overflow"))?;
        let page_plan = work_plan_for_limit(
            active.permit.bytes(),
            admitted_resident_bytes,
            dependencies.config.segment_flush_bytes(job.kind),
        );
        let page_plan = match page_plan {
            Ok(plan) => plan,
            Err(_) if active.builder.frozen.is_some() => {
                // The one frozen slot has consumed this permit's remaining
                // projection capacity. Join it and retry the exact page rather
                // than allocating outside the global budget.
                finish_frozen_segment(job.kind, &mut active.builder, &mut work.candidate).await?;
                work_plan_for_limit(
                    active.permit.bytes(),
                    page_resident_bytes,
                    dependencies.config.segment_flush_bytes(job.kind),
                )?
            }
            Err(error) => return Err(error),
        };
        let atomic_plan = if page
            .changes
            .iter()
            .any(|entry| matches!(entry.change, LocalChange::AtomicBatchPublished(_)))
        {
            let atomic_resident_bytes = page_resident_bytes
                .checked_add(active.builder.frozen_resident_charge())
                .ok_or_else(|| Status::resource_exhausted("atomic unit resident bytes overflow"))?;
            match work_plan_for_limit(
                active.permit.bytes(),
                atomic_resident_bytes,
                active.permit.bytes(),
            ) {
                Ok(plan) => plan,
                Err(_) if active.builder.frozen.is_some() => {
                    finish_frozen_segment(job.kind, &mut active.builder, &mut work.candidate)
                        .await?;
                    work_plan_for_limit(
                        active.permit.bytes(),
                        page_resident_bytes,
                        active.permit.bytes(),
                    )?
                }
                Err(error) => return Err(error),
            }
        } else {
            page_plan
        };
        tracing::debug!(
            index.kind = ?job.kind,
            gauge.keldra_index_journal_page_resident_bytes = page_resident_bytes,
            histogram.keldra_index_journal_page_resident_bytes = page_resident_bytes,
            "index journal page admitted by concrete resident size"
        );
        let page_work = await_with_builder_heartbeats(
            &work.progress,
            process_journal_page(
                &job.definition,
                job.kind,
                &work.through,
                &page,
                page_plan,
                atomic_plan,
                &mut active.builder,
                &mut work.candidate,
                &mut work.atomic_projection,
                dependencies,
                active
                    .started
                    .map(|started| started + dependencies.config.segment_flush_max_age()),
                dependencies.config.segment_flush_max_age(),
            ),
        )
        .await?;
        if page_work.changed {
            active
                .started
                .get_or_insert(page_work.first_changed_at.unwrap_or_else(Instant::now));
        }
        work.changed |= page_work.changed;
        active.operations = active
            .operations
            .saturating_add(page_work.processed_records);
        catch_up::record_source_page_progress(&mut work, &page_work.through);
        work.progress.advance(
            page_work.processed_records,
            page_work.processed_encoded_bytes,
        );
        tracing::debug!(
            index.kind = ?job.kind,
            monotonic_counter.keldra_index_source_payload_bytes_total =
                page_work.source_payload_bytes,
            histogram.keldra_index_source_page_payload_bytes = page_work.source_payload_bytes,
            "index source page payload charged to work quantum"
        );
        if page_work.atomic_pending {
            work.active = Some(active);
            return Ok((
                BuilderPhase::CatchUp(work),
                BuilderDisposition::Retry(Duration::from_millis(10)),
                None,
            ));
        }
        let quantum_boundary = active.quantum.advance_page(
            page_work.processed_encoded_bytes,
            page_work.source_payload_bytes,
        )? == SourceWorkBoundary::SealAndYield;
        let age_boundary = active.started.is_some_and(|started| {
            started.elapsed() >= dependencies.config.segment_flush_max_age()
        });
        let operation_boundary =
            active.operations >= dependencies.config.segment_flush_max_operations(job.kind);
        if age_boundary {
            if work.publishing.is_some() {
                work.active = Some(active);
                return Ok((
                    BuilderPhase::CatchUp(work),
                    BuilderDisposition::Retry(Duration::from_millis(10)),
                    None,
                ));
            }
            flush_builder(
                &job.definition,
                job.kind,
                &mut active.builder,
                &mut work.candidate,
                dependencies,
            )
            .await?;
            enqueue_candidate_publication(job, &mut work, admission, dependencies).await?;
            drop(active);
            return Ok((BuilderPhase::CatchUp(work), BuilderDisposition::Ready, None));
        }
        if quantum_boundary || operation_boundary {
            // Size/operation/fairness boundaries detach the active buffer but
            // do not force a commit. Journal intake continues through the
            // replacement buffer while the single frozen slot seals. Only its
            // explicit queue saturation can make the next detach wait.
            if !active.builder.is_empty() {
                freeze_builder(
                    &job.definition,
                    job.kind,
                    &mut active.builder,
                    &mut work.candidate,
                    dependencies,
                )
                .await?;
            }
            active.quantum = SourceWorkQuantum::from_budget_limit(active.permit.bytes());
            active.operations = 0;
            work.active = Some(active);
            return Ok((BuilderPhase::CatchUp(work), BuilderDisposition::Ready, None));
        }
    }
}

async fn enqueue_candidate_publication(
    job: &BuilderJob,
    work: &mut CatchUpWork,
    admission: DerivedArtifactAdmission,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    debug_assert!(work.publishing.is_none());
    rebuild::checkpoint_catch_up_root(job, work, dependencies).await?;
    let current = work.current.clone();
    let barrier = work.through.clone();
    let candidate = work.candidate.clone();
    let task = start_candidate_publication(
        job.definition.clone(),
        job.kind,
        barrier.clone(),
        candidate.clone(),
        current.clone(),
        admission,
        dependencies.clone(),
    );
    work.publishing = Some(InFlightPublication {
        current,
        barrier,
        candidate,
        admission,
        task,
    });
    work.changed = false;
    work.must_publish = false;
    work.checkpoint_started = None;
    Ok(())
}

async fn compact_one_if_needed(
    job: &BuilderJob,
    candidate: &mut CandidateCommit,
    limits: DebtLimits,
    dependencies: &IndexBuilderDependencies,
) -> Result<bool, Status> {
    emit_compaction_debt(
        job.kind,
        &candidate.segments,
        limits.maximum_segments,
        limits.maximum_bytes,
        "observed",
    );
    let Some(selection) = debt::select_before_locator_limit(
        &candidate.segments,
        candidate.locator_roots.len(),
        limits,
    ) else {
        let Some(locator_selection) = debt::select_locator_roots(&candidate.locator_roots, limits)
        else {
            return Ok(false);
        };
        tracing::info!(
            index.kind = ?job.kind,
            compaction.trigger = "locator_debt",
            compaction.input_roots = locator_selection.input_roots,
            gauge.keldra_index_locator_roots = candidate.locator_roots.len() as u64,
            monotonic_counter.keldra_index_locator_compaction_admission_stops_total = 1_u64,
            "index source work yielded to bounded locator compaction debt"
        );
        let budget = dependencies.budgets.for_kind(job.kind);
        let (_publication_slot, _permit) = acquire_maintenance_memory(
            &dependencies.maintenance_work_slots,
            budget,
            budget.limit(),
            budget.limit(),
        )
        .await?;
        locator_debt::compact_oldest_prefix(
            &job.definition,
            job.kind,
            locator_selection,
            compaction_admission(),
            candidate,
            dependencies,
        )
        .await?;
        return Ok(true);
    };
    tracing::info!(
        index.kind = ?job.kind,
        compaction.trigger = "debt",
        monotonic_counter.keldra_index_compaction_admission_stops_total = 1_u64,
        "index source work yielded to bounded compaction debt"
    );
    let budget = dependencies.budgets.for_kind(job.kind);
    let (_publication_slot, permit) = acquire_maintenance_memory(
        &dependencies.maintenance_work_slots,
        budget,
        budget.limit(),
        budget.working_memory_limit(),
    )
    .await?;
    compact_tier(
        &job.definition,
        job.kind,
        selection,
        permit.bytes(),
        compaction_admission(),
        candidate,
        dependencies,
    )
    .await?;
    emit_compaction_debt(
        job.kind,
        &candidate.segments,
        limits.maximum_segments,
        limits.maximum_bytes,
        "repaid",
    );
    Ok(true)
}

async fn acquire_maintenance_memory(
    slots: &IndexMaintenanceWorkSlots,
    budget: &super::budget::IndexMemoryBudget,
    minimum: u64,
    preferred: u64,
) -> Result<(tokio::sync::OwnedSemaphorePermit, IndexMemoryPermit), Status> {
    // Queue for the scarce maintenance lane before leasing construction
    // memory. Waiting maintenance must not pin bytes that incremental builders
    // can use while it is not runnable.
    let slot = slots.acquire().await?;
    let permit = budget
        .acquire_up_to(minimum, preferred)
        .await
        .map_err(budget_status)?;
    Ok((slot, permit))
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

#[derive(Clone, Copy, Debug, Default)]
struct ProjectionExecution {
    queue_seconds: f64,
    cpu_seconds: f64,
}

struct NativeSegmentBuild {
    writer: NativeSegmentWriter,
    plan: SegmentMemoryPlan,
    maximum_operations: u64,
    started: Option<Instant>,
    source_paths: BTreeMap<String, u64>,
    frozen: Option<FrozenSegmentTask>,
    publication_lane: SegmentPublicationLane,
    maximum_segments: usize,
}

#[derive(Clone, Copy)]
enum SegmentPublicationLane {
    Incremental,
    Maintenance,
}

impl SegmentPublicationLane {
    const fn cohort_class(self) -> PublicationCohortClass {
        match self {
            Self::Incremental => PublicationCohortClass::Incremental,
            Self::Maintenance => PublicationCohortClass::Maintenance,
        }
    }
}

struct FrozenSegmentTask {
    task: AbortOnDropTask<Result<FrozenSegment, Status>>,
    source_paths: BTreeMap<String, u64>,
    resident_charge: u64,
}

struct FrozenSegment {
    built: BuiltSegment,
    resident_bytes: u64,
    seal_workspace_bytes: u64,
}

impl NativeSegmentBuild {
    fn new(
        job: &BuilderJob,
        plan: SegmentMemoryPlan,
        publication_lane: SegmentPublicationLane,
        dependencies: &IndexBuilderDependencies,
    ) -> Result<Self, Status> {
        Self::open(&job.definition, plan, publication_lane, dependencies)
    }

    fn open(
        definition: &CatalogDefinition,
        plan: SegmentMemoryPlan,
        publication_lane: SegmentPublicationLane,
        dependencies: &IndexBuilderDependencies,
    ) -> Result<Self, Status> {
        Self::open_with_segment_limit(
            definition,
            plan,
            publication_lane,
            MAX_SEGMENTS_PER_COMMIT,
            dependencies,
        )
    }

    fn open_with_segment_limit(
        definition: &CatalogDefinition,
        plan: SegmentMemoryPlan,
        publication_lane: SegmentPublicationLane,
        maximum_segments: usize,
        dependencies: &IndexBuilderDependencies,
    ) -> Result<Self, Status> {
        let segment_id = dependencies
            .store
            .allocate_snowflake_id()
            .map_err(|error| Status::internal(format!("allocate index segment ID: {error}")))?;
        let identity = SegmentIdentity::new(
            definition.stored.index_id,
            definition.object_version,
            definition.schema_fingerprint,
            segment_id,
        )
        .map_err(index_status)?;
        let limits = BuildLimits::with_resident_limits(
            plan.total_bytes,
            plan.max_resident_bytes,
            FIXED_INDEX_SEAL_WORKSPACE_BYTES,
        )
        .map_err(index_status)?;
        let writer = NativeSegmentWriter::new(identity, definition.schema.clone(), limits)
            .map_err(index_status)?;
        Ok(Self {
            writer,
            plan,
            maximum_operations: dependencies
                .config
                .segment_flush_max_operations(runtime_kind(definition.schema.kind)),
            started: None,
            source_paths: BTreeMap::new(),
            frozen: None,
            publication_lane,
            maximum_segments,
        })
    }

    fn is_empty(&self) -> bool {
        self.writer.source_count() == 0
    }

    fn frozen_resident_charge(&self) -> u64 {
        self.frozen
            .as_ref()
            .map_or(0, |frozen| frozen.resident_charge)
    }
}

async fn push_or_flush(
    definition: &CatalogDefinition,
    kind: IndexKind,
    builder: &mut NativeSegmentBuild,
    source: NativeProjectedSource,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
    soft_flush_allowed: bool,
) -> Result<(), Status> {
    let age_boundary = soft_flush_allowed
        && builder.started.is_some_and(|started| {
            started.elapsed() >= dependencies.config.segment_flush_max_age()
        });
    if soft_flush_allowed
        && (builder.writer.source_count() as u64 >= builder.maximum_operations || age_boundary)
    {
        freeze_builder(definition, kind, builder, candidate, dependencies).await?;
    }
    if let Some(version) = builder.writer.source_version(&source.source_identity.path) {
        if version == source.source_identity.version {
            return Ok(());
        }
        flush_builder(definition, kind, builder, candidate, dependencies).await?;
    }
    let path = source.source_identity.path.clone();
    let version = source.source_identity.version;
    match builder.writer.push_source(source).map_err(index_status)? {
        SourcePush::Accepted => {
            builder.started.get_or_insert_with(Instant::now);
            builder.source_paths.insert(path, version);
            observability::emit_active_buffer(
                definition.stored.index_id,
                kind,
                builder.writer.buffered_source_bytes() as u64,
                builder.writer.source_count() as u64,
                builder
                    .started
                    .map_or(Duration::ZERO, |started| started.elapsed()),
            );
            Ok(())
        }
        SourcePush::Full(pending) => {
            freeze_builder(definition, kind, builder, candidate, dependencies).await?;
            match builder.writer.push_source(pending).map_err(index_status)? {
                SourcePush::Accepted => {
                    builder.started.get_or_insert_with(Instant::now);
                    builder.source_paths.insert(path, version);
                    observability::emit_active_buffer(
                        definition.stored.index_id,
                        kind,
                        builder.writer.buffered_source_bytes() as u64,
                        builder.writer.source_count() as u64,
                        builder
                            .started
                            .map_or(Duration::ZERO, |started| started.elapsed()),
                    );
                    Ok(())
                }
                SourcePush::Full(_) => Err(Status::resource_exhausted(
                    "one projected source cannot fit an empty format-v4 segment",
                )),
            }
        }
    }
}

async fn flush_builder(
    definition: &CatalogDefinition,
    kind: IndexKind,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    if !builder.is_empty() {
        freeze_builder(definition, kind, builder, candidate, dependencies).await?;
    }
    finish_frozen_segment(kind, builder, candidate).await
}

async fn freeze_builder(
    definition: &CatalogDefinition,
    kind: IndexKind,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    if builder.is_empty() {
        return Ok(());
    }
    let flush_reason = if builder
        .started
        .is_some_and(|started| started.elapsed() >= dependencies.config.segment_flush_max_age())
    {
        "maximum_age"
    } else if builder.writer.source_count() as u64 >= builder.maximum_operations {
        "maximum_operations"
    } else if builder.writer.buffered_source_bytes() >= builder.plan.max_resident_bytes {
        "maximum_bytes"
    } else {
        "explicit_boundary"
    };
    // The queue contains exactly one detached buffer. Saturation pauses only
    // this definition; it cannot allocate an unbounded sequence of frozen
    // builders or consume another definition's memory budget.
    finish_frozen_segment(kind, builder, candidate).await?;
    if candidate.segments.len() >= builder.maximum_segments {
        return Err(Status::resource_exhausted(
            "format-v4 commit reached its immutable segment bound",
        ));
    }
    let publication_lane = builder.publication_lane;
    let replacement =
        NativeSegmentBuild::open(definition, builder.plan, publication_lane, dependencies)?;
    let full = std::mem::replace(builder, replacement);
    let resident = full.writer.buffered_source_bytes() as u64;
    let seal_workspace = full
        .plan
        .seal_workspace_bytes(full.writer.buffered_source_bytes())
        .map_err(index_status)?;
    let mut sink = dependencies.publisher.component_sink(
        &definition.stored,
        definition.tenant_id,
        definition.bucket_id,
        DerivedArtifactAdmission::PublicationProgress,
        publication_lane.cohort_class(),
    );
    builder.frozen = Some(FrozenSegmentTask {
        source_paths: full.source_paths,
        // The active permit owns the complete bounded pipeline. Every later
        // source-page plan subtracts this detached seal footprint before it
        // admits projection work, so active and frozen state cannot each spend
        // the same reserved bytes.
        resident_charge: seal_workspace as u64,
        task: AbortOnDropTask::new(tokio::spawn(async move {
            let built = full.writer.seal(&mut sink).await.map_err(index_status)?;
            Ok(FrozenSegment {
                built,
                resident_bytes: resident,
                seal_workspace_bytes: seal_workspace as u64,
            })
        })),
    });
    observability::emit_active_buffer(definition.stored.index_id, kind, 0, 0, Duration::ZERO);
    observability::emit_frozen_buffer(
        definition.stored.index_id,
        kind,
        1,
        seal_workspace as u64,
        flush_reason,
    );
    Ok(())
}

async fn finish_frozen_segment(
    kind: IndexKind,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateCommit,
) -> Result<(), Status> {
    let Some(frozen_task) = builder.frozen.take() else {
        return Ok(());
    };
    let frozen =
        frozen_task.task.join().await.map_err(|error| {
            Status::internal(format!("index segment flush task failed: {error}"))
        })??;
    let built = frozen.built;
    observability::emit_frozen_buffer(built.descriptor.identity.index_id, kind, 0, 0, "completed");
    let sequence = candidate.allocate_sequence()?;
    let descriptor_identity = built.descriptor.identity;
    candidate.segments.push(built.descriptor);
    candidate
        .segments
        .sort_by_key(|segment| segment.identity.segment_id);
    candidate.locator_roots.push(LocatorRoot {
        sequence,
        identity: descriptor_identity,
        artifact: built.locator.root,
        pack_ownership: LocatorPackOwnership::Segment,
        encoded_bytes: built.locator.encoded_bytes,
        logical_bytes: built.locator.logical_bytes,
    });
    candidate.locator_roots.sort_by_key(|root| root.sequence);
    tracing::debug!(
        index.kind = ?kind,
        gauge.keldra_index_construction_resident_bytes = frozen.resident_bytes,
        gauge.keldra_index_construction_workspace_bytes = frozen.seal_workspace_bytes,
        monotonic_counter.keldra_index_flushes_total = 1_u64,
        monotonic_counter.keldra_index_segments_created_total = 1_u64,
        "format-v4 index segment flushed"
    );
    Ok(())
}

async fn compact_tier(
    definition: &CatalogDefinition,
    kind: IndexKind,
    selection: DebtSelection,
    leased_bytes: u64,
    admission: DerivedArtifactAdmission,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    v4_merge::compact_selected_segments(
        definition,
        kind,
        selection,
        leased_bytes,
        admission,
        candidate,
        dependencies,
    )
    .await
}

const fn publication_admission(maintenance: bool) -> DerivedArtifactAdmission {
    if maintenance {
        DerivedArtifactAdmission::Bounded
    } else {
        DerivedArtifactAdmission::PublicationProgress
    }
}

const fn compaction_admission() -> DerivedArtifactAdmission {
    DerivedArtifactAdmission::Bounded
}

fn derived_identity(definition: &CatalogDefinition) -> DerivedDefinitionIdentity {
    DerivedDefinitionIdentity {
        kind: DefinitionKind::Index,
        tenant_id: definition.tenant_id,
        bucket_id: definition.bucket_id,
        definition_id: definition.stored.index_id,
        object_version: VersionId(definition.object_version),
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
    path.split('/').any(|segment| segment == "_keldra")
}

fn build_object(path: &str, version: &keldra_store::Version) -> Result<IndexBuildObject, Status> {
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

fn source_needs_payload(schema: &Schema) -> bool {
    !matches!(
        schema.kind,
        keldra_index::v4::IndexKind::Path | keldra_index::v4::IndexKind::MetadataFilter
    )
}

fn source_payload_bytes_for(schema: &Schema, source: &IndexSourceMutation) -> u64 {
    match source {
        IndexSourceMutation::Upsert(object) if source_needs_payload(schema) => {
            object.content_length
        }
        IndexSourceMutation::Upsert(_) | IndexSourceMutation::Remove(_) => 0,
    }
}

#[cfg(test)]
#[path = "manager/tests.rs"]
mod tests;
