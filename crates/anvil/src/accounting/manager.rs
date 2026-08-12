//! One node-wide, bounded accounting builder scheduler.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{DefinitionKind, ObjectKey, Store, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_placement::ClusterPlacement;
use crate::derived_consumer::{
    DerivedBarrierEvidence, DerivedDefinitionIdentity, DerivedProgressReporter,
};
use crate::index_runtime::events::{
    IndexBarrier, IndexEventError, IndexEventJournal, IndexJournalPage, MAX_INDEX_EVENT_PAGE_BYTES,
};
use crate::index_runtime::placement::{IndexIdentity, IndexPlacement};
use crate::index_runtime::scanner::{ClusterIndexScanner, ClusterRetainedSourceSnapshot};

use super::{
    AccountingBaselineAccumulator, AccountingCatalog, AccountingCatalogChange, AccountingIdentity,
    AccountingObjectSnapshot, AccountingPublisher, LoadedAccountingDefinition,
    StoredAccountingDefinition, StoredAccountingRollup, StoredTrafficCheckpoint,
    StoredTrafficSource, current_path, definition_path, includes_path, outbound_source_path,
};

const RETRY_INTERVAL: Duration = Duration::from_secs(1);
const BASELINE_FRAME_BYTES: u64 = 512 * 1024;
const CATALOG_HANDOFF_PAGE: usize = 256;
const MAX_ACTIVE_ACCOUNTING_WORKERS: usize = 64;

pub(crate) struct AccountingManagerTask {
    task: tokio::task::JoinHandle<()>,
}

impl AccountingManagerTask {
    pub(crate) fn start(
        local_node: NodeId,
        decisions: DecisionRaft,
        catalog: AccountingCatalog,
        dependencies: AccountingBuilderDependencies,
    ) -> Self {
        // Subscribe before spawning so handoff mutations cannot fall into a
        // task-start race. The bounded pending queue covers definitions
        // offered before this subscription was created.
        let changes = catalog.subscribe();
        let task = tokio::spawn(run_scheduler(
            local_node,
            decisions,
            catalog,
            dependencies,
            changes,
        ));
        Self { task }
    }
}

impl Drop for AccountingManagerTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
pub(crate) struct AccountingBuilderDependencies {
    pub(crate) store: Store,
    pub(crate) journal: Arc<IndexEventJournal>,
    pub(crate) scanner: ClusterIndexScanner,
    pub(crate) reader: ClusterObjectReader,
    pub(crate) publisher: AccountingPublisher,
    pub(crate) derived_progress: DerivedProgressReporter,
}

async fn run_scheduler(
    local_node: NodeId,
    decisions: DecisionRaft,
    catalog: AccountingCatalog,
    dependencies: AccountingBuilderDependencies,
    mut changes: tokio::sync::broadcast::Receiver<AccountingIdentity>,
) {
    let mut scheduler = AccountingScheduler::default();
    loop {
        let mut available = scheduler.remaining_capacity();
        match catalog.take_page(CATALOG_HANDOFF_PAGE, |identity| {
            if scheduler.workers.contains_key(&identity) {
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
                    if !scheduler.apply_change(change) {
                        tracing::debug!(
                            "bounded accounting worker admission deferred to assignment rediscovery"
                        );
                    }
                }
            }
            Err(error) => tracing::warn!(%error, "accounting assignment handoff drain will retry"),
        }
        scheduler.promote_due();
        if let Some(identity) = scheduler.pop_ready() {
            run_identity_quantum(
                identity,
                local_node,
                &decisions,
                &dependencies,
                &mut scheduler,
            )
            .await;
            continue;
        }

        let sleep_until = next_scheduler_wake(&scheduler);
        tokio::select! {
            received = changes.recv() => {
                match received {
                    Ok(identity) => {
                        match catalog.take(identity, scheduler.can_admit(identity)) {
                            Ok(Some(change)) => { scheduler.apply_change(change); }
                            Ok(None) => {}
                            Err(error) => tracing::warn!(%error, "accounting assignment handoff read will retry"),
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
            _ = tokio::time::sleep_until(sleep_until) => {}
        }
    }
}

fn next_scheduler_wake(scheduler: &AccountingScheduler) -> tokio::time::Instant {
    tokio::time::Instant::from_std(next_scheduler_wake_std(scheduler, Instant::now()))
}

fn next_scheduler_wake_std(scheduler: &AccountingScheduler, now: Instant) -> Instant {
    scheduler
        .next_due_std()
        .unwrap_or(now + Duration::from_secs(60))
}

#[derive(Default)]
struct AccountingScheduler {
    workers: BTreeMap<AccountingIdentity, AccountingWorker>,
    ready: VecDeque<AccountingIdentity>,
    queued: BTreeSet<AccountingIdentity>,
    due: BTreeMap<AccountingIdentity, Instant>,
    baseline_owner: Option<AccountingIdentity>,
    baseline_waiters: VecDeque<AccountingIdentity>,
    waiting_for_baseline: BTreeSet<AccountingIdentity>,
}

impl AccountingScheduler {
    fn remaining_capacity(&self) -> usize {
        MAX_ACTIVE_ACCOUNTING_WORKERS.saturating_sub(self.workers.len())
    }

    fn can_admit(&self, identity: AccountingIdentity) -> bool {
        self.workers.contains_key(&identity) || self.remaining_capacity() > 0
    }

    /// Returns false only when a new lease cannot enter the bounded active set.
    fn apply_change(&mut self, change: AccountingCatalogChange) -> bool {
        match change {
            AccountingCatalogChange::Remove(identity) => {
                self.remove_worker(identity);
                true
            }
            AccountingCatalogChange::Upsert(definition) => {
                let identity = (
                    definition.tenant_id,
                    definition.bucket_id,
                    definition.stored.accounting_id,
                );
                let replace = self.workers.get(&identity).is_none_or(|worker| {
                    worker.definition.version != definition.version
                        || worker.definition.stored != definition.stored
                });
                if !replace {
                    self.schedule_now(identity);
                    return true;
                }
                if !self.workers.contains_key(&identity)
                    && self.workers.len() >= MAX_ACTIVE_ACCOUNTING_WORKERS
                {
                    return false;
                }
                self.remove_worker(identity);
                self.workers.insert(
                    identity,
                    AccountingWorker {
                        definition,
                        phase: WorkerPhase::Recover,
                    },
                );
                self.schedule_now(identity);
                true
            }
        }
    }

    fn schedule_now(&mut self, identity: AccountingIdentity) {
        self.due.remove(&identity);
        if self.waiting_for_baseline.contains(&identity) {
            return;
        }
        if self.queued.insert(identity) {
            self.ready.push_back(identity);
        }
    }

    fn delay(&mut self, identity: AccountingIdentity, duration: Duration) {
        let due = Instant::now() + duration;
        self.due.insert(identity, due);
    }

    fn wait_for_baseline(&mut self, identity: AccountingIdentity) {
        if self.waiting_for_baseline.insert(identity) {
            self.baseline_waiters.push_back(identity);
        }
    }

    fn wake_baseline_waiter(&mut self) {
        if self.baseline_owner.is_some() {
            return;
        }
        while let Some(identity) = self.baseline_waiters.pop_front() {
            if self.waiting_for_baseline.remove(&identity) && self.workers.contains_key(&identity) {
                self.schedule_now(identity);
                break;
            }
        }
    }

    fn promote_due(&mut self) {
        let now = Instant::now();
        let ready = self
            .due
            .iter()
            .filter_map(|(identity, due)| (*due <= now).then_some(*identity))
            .collect::<Vec<_>>();
        for identity in ready {
            self.due.remove(&identity);
            self.schedule_now(identity);
        }
    }

    fn pop_ready(&mut self) -> Option<AccountingIdentity> {
        while let Some(identity) = self.ready.pop_front() {
            if self.queued.remove(&identity) {
                return Some(identity);
            }
        }
        None
    }

    fn next_due_std(&self) -> Option<Instant> {
        self.due.values().min().copied()
    }

    fn retry(&mut self, identity: AccountingIdentity) {
        // Durable assignment rediscovery is the retry queue. Do not let one
        // failing definition retain one of the bounded process-local leases.
        self.remove_worker(identity);
    }

    fn remove_worker(&mut self, identity: AccountingIdentity) {
        let released = self.baseline_owner == Some(identity);
        self.workers.remove(&identity);
        self.queued.remove(&identity);
        self.ready.retain(|queued| *queued != identity);
        self.due.remove(&identity);
        self.waiting_for_baseline.remove(&identity);
        self.baseline_waiters.retain(|waiting| *waiting != identity);
        if released {
            self.baseline_owner = None;
            self.wake_baseline_waiter();
        }
    }
}

struct AccountingWorker {
    definition: LoadedAccountingDefinition,
    phase: WorkerPhase,
}

enum WorkerPhase {
    Recover,
    AwaitBaseline,
    Baseline {
        stream: ClusterRetainedSourceSnapshot,
        through: IndexBarrier,
        accumulator: AccountingBaselineAccumulator,
    },
    Ready {
        snapshot: AccountingObjectSnapshot,
        through: IndexBarrier,
        target: Option<IndexBarrier>,
        publish_empty: bool,
    },
}

impl WorkerPhase {
    fn holds_baseline(&self) -> bool {
        matches!(self, Self::Baseline { .. })
    }
}

enum QuantumSchedule {
    Now,
    Idle,
    Retry(Status),
    WaitForBaseline,
}

async fn run_identity_quantum(
    identity: AccountingIdentity,
    local_node: NodeId,
    decisions: &DecisionRaft,
    dependencies: &AccountingBuilderDependencies,
    scheduler: &mut AccountingScheduler,
) {
    let Some(definition) = scheduler
        .workers
        .get(&identity)
        .map(|worker| worker.definition.clone())
    else {
        return;
    };
    match accounting_lease_is_current(&definition, local_node, decisions, &dependencies.store) {
        Ok(true) => {}
        Ok(false) => {
            scheduler.remove_worker(identity);
            return;
        }
        Err(error) => {
            tracing::warn!(accounting.id = identity.2, %error, "active accounting assignment point-read will retry");
            scheduler.retry(identity);
            return;
        }
    }
    let mut worker = scheduler
        .workers
        .remove(&identity)
        .expect("worker was installed");
    let held_before = worker.phase.holds_baseline();
    let baseline_available =
        scheduler.baseline_owner.is_none() || scheduler.baseline_owner == Some(identity);
    let phase = std::mem::replace(&mut worker.phase, WorkerPhase::Recover);
    let (phase, schedule) =
        run_quantum(&worker.definition, dependencies, phase, baseline_available).await;
    let held_after = phase.holds_baseline();
    worker.phase = phase;
    if !held_before && held_after {
        scheduler.baseline_owner = Some(identity);
    } else if held_before && !held_after && scheduler.baseline_owner == Some(identity) {
        scheduler.baseline_owner = None;
    }
    scheduler.workers.insert(identity, worker);
    if held_before && !held_after {
        scheduler.wake_baseline_waiter();
    }
    match schedule {
        QuantumSchedule::Now => scheduler.schedule_now(identity),
        QuantumSchedule::Idle => scheduler.remove_worker(identity),
        QuantumSchedule::WaitForBaseline => scheduler.wait_for_baseline(identity),
        QuantumSchedule::Retry(error) => {
            tracing::warn!(accounting.id = identity.2, %error, "accounting work quantum will retry");
            scheduler.retry(identity);
        }
    }
}

fn accounting_lease_is_current(
    definition: &LoadedAccountingDefinition,
    local_node: NodeId,
    decisions: &DecisionRaft,
    store: &Store,
) -> Result<bool, Status> {
    let record = store
        .definition_assignment(
            DefinitionKind::Accounting,
            definition.tenant_id,
            definition.bucket_id,
            definition.stored.accounting_id,
        )
        .map_err(|error| Status::internal(format!("read active accounting assignment: {error}")))?;
    let Some(record) = record else {
        return Ok(false);
    };
    let placement = current_placement(decisions)?;
    let owners = assignment(definition, &placement)?;
    Ok(record.kind == DefinitionKind::Accounting
        && record.object_version == definition.version
        && record.definition_path == definition_path(definition.stored.accounting_id)?
        && record.observed_fence == placement.fence()
        && record.rank == 0
        && owners.builder() == local_node)
}

async fn run_quantum(
    definition: &LoadedAccountingDefinition,
    dependencies: &AccountingBuilderDependencies,
    phase: WorkerPhase,
    baseline_available: bool,
) -> (WorkerPhase, QuantumSchedule) {
    match phase {
        WorkerPhase::Recover => match recover_rollup(definition, dependencies).await {
            Ok(Some((snapshot, through, target))) => (
                WorkerPhase::Ready {
                    snapshot,
                    through,
                    target: Some(target),
                    publish_empty: false,
                },
                QuantumSchedule::Now,
            ),
            Ok(None) => (WorkerPhase::AwaitBaseline, QuantumSchedule::Now),
            Err(error) => (WorkerPhase::Recover, QuantumSchedule::Retry(error)),
        },
        WorkerPhase::AwaitBaseline if !baseline_available => {
            (WorkerPhase::AwaitBaseline, QuantumSchedule::WaitForBaseline)
        }
        WorkerPhase::AwaitBaseline => match open_baseline(definition, dependencies).await {
            Ok((stream, through)) => (
                WorkerPhase::Baseline {
                    stream,
                    through,
                    accumulator: AccountingBaselineAccumulator::default(),
                },
                QuantumSchedule::Now,
            ),
            Err(error) => (WorkerPhase::AwaitBaseline, QuantumSchedule::Retry(error)),
        },
        WorkerPhase::Baseline {
            mut stream,
            through,
            mut accumulator,
        } => match stream.next_frame().await {
            Ok(Some(frame)) => match accumulator.apply_frame(definition, &frame) {
                Ok(()) => {
                    tracing::debug!(
                        accounting.id = definition.stored.accounting_id,
                        monotonic_counter.anvil_accounting_snapshot_records_total =
                            frame.len() as u64,
                        "accounting retained snapshot frame reduced"
                    );
                    (
                        WorkerPhase::Baseline {
                            stream,
                            through,
                            accumulator,
                        },
                        QuantumSchedule::Now,
                    )
                }
                Err(error) => (WorkerPhase::AwaitBaseline, QuantumSchedule::Retry(error)),
            },
            Ok(None) => (
                WorkerPhase::Ready {
                    snapshot: accumulator.finish(),
                    through,
                    target: None,
                    publish_empty: true,
                },
                QuantumSchedule::Now,
            ),
            Err(error) => (WorkerPhase::AwaitBaseline, QuantumSchedule::Retry(error)),
        },
        WorkerPhase::Ready {
            mut snapshot,
            mut through,
            mut target,
            mut publish_empty,
        } => {
            if target.is_none() {
                match dependencies.journal.capture_barrier().await {
                    Ok(captured) if resumable_barrier(&through, &captured) => {
                        target = Some(captured);
                    }
                    Ok(_) => {
                        return (
                            WorkerPhase::AwaitBaseline,
                            QuantumSchedule::Retry(Status::unavailable(
                                "accounting source identity changed; scoped baseline required",
                            )),
                        );
                    }
                    Err(error) => {
                        return (
                            WorkerPhase::Ready {
                                snapshot,
                                through,
                                target,
                                publish_empty,
                            },
                            QuantumSchedule::Retry(event_status(error)),
                        );
                    }
                }
            }
            let captured = target.clone().expect("target was captured");
            if through == captured {
                if publish_empty {
                    if let Err(error) =
                        publish_snapshot(definition, dependencies, &snapshot, &through).await
                    {
                        return (WorkerPhase::Recover, QuantumSchedule::Retry(error));
                    }
                    publish_empty = false;
                }
                return (
                    WorkerPhase::Ready {
                        snapshot,
                        through,
                        target: None,
                        publish_empty,
                    },
                    QuantumSchedule::Idle,
                );
            }
            match dependencies
                .journal
                .next_page(
                    definition.tenant_id,
                    definition.bucket_id,
                    &through,
                    &captured,
                    MAX_INDEX_EVENT_PAGE_BYTES,
                )
                .await
            {
                Ok(Some(page)) => {
                    if let Err(error) = apply_page(definition, &mut snapshot, &page) {
                        return (
                            WorkerPhase::AwaitBaseline,
                            QuantumSchedule::Retry(Status::unavailable(error.to_string())),
                        );
                    }
                    through = page.through;
                    if let Err(error) =
                        publish_snapshot(definition, dependencies, &snapshot, &through).await
                    {
                        return (WorkerPhase::Recover, QuantumSchedule::Retry(error));
                    }
                    let completed = captured == through;
                    (
                        WorkerPhase::Ready {
                            snapshot,
                            through,
                            target: (!completed).then_some(captured),
                            publish_empty: false,
                        },
                        if completed {
                            QuantumSchedule::Idle
                        } else {
                            QuantumSchedule::Now
                        },
                    )
                }
                Ok(None) => (
                    WorkerPhase::AwaitBaseline,
                    QuantumSchedule::Retry(Status::unavailable(
                        "accounting journal stopped before its captured barrier",
                    )),
                ),
                Err(error) if event_requires_baseline(&error) => (
                    WorkerPhase::AwaitBaseline,
                    QuantumSchedule::Retry(event_status(error)),
                ),
                Err(error) => (
                    WorkerPhase::Ready {
                        snapshot,
                        through,
                        target,
                        publish_empty,
                    },
                    QuantumSchedule::Retry(event_status(error)),
                ),
            }
        }
    }
}

async fn recover_rollup(
    definition: &LoadedAccountingDefinition,
    dependencies: &AccountingBuilderDependencies,
) -> Result<Option<(AccountingObjectSnapshot, IndexBarrier, IndexBarrier)>, Status> {
    let Some((_, rollup)) = read_rollup(definition, &dependencies.reader).await? else {
        return Ok(None);
    };
    let target = dependencies
        .journal
        .capture_barrier()
        .await
        .map_err(event_status)?;
    let resumed = resume_rollup(definition, &rollup, &target)?;
    if let Some((_, through)) = resumed.as_ref() {
        dependencies
            .derived_progress
            .report(
                derived_identity(definition),
                DerivedBarrierEvidence::Published(through.clone()),
            )
            .await;
    }
    Ok(resumed.map(|(snapshot, through)| (snapshot, through, target)))
}

async fn open_baseline(
    definition: &LoadedAccountingDefinition,
    dependencies: &AccountingBuilderDependencies,
) -> Result<(ClusterRetainedSourceSnapshot, IndexBarrier), Status> {
    let (expected_fence, expected_atomic) = dependencies
        .journal
        .snapshot_authority()
        .map_err(event_status)?;
    let stream = dependencies
        .scanner
        .begin_retained_source_snapshot(
            definition.tenant_id,
            definition.bucket_id,
            definition.stored.path_prefix.clone(),
            BASELINE_FRAME_BYTES,
        )
        .await?;
    if stream.placement_fence() != expected_fence {
        return Err(Status::unavailable(
            "cluster placement changed while opening accounting snapshots",
        ));
    }
    let tails = stream
        .checkpoints()
        .iter()
        .map(|checkpoint| (checkpoint.node, checkpoint.source, checkpoint.captured_tail))
        .collect::<Vec<_>>();
    let through = dependencies
        .journal
        .barrier_from_snapshot_tails(stream.placement_fence(), expected_atomic, &tails)
        .map_err(event_status)?;
    dependencies
        .derived_progress
        .report(
            derived_identity(definition),
            DerivedBarrierEvidence::ScopedSnapshot(through.clone()),
        )
        .await;
    Ok((stream, through))
}

fn resume_rollup(
    definition: &LoadedAccountingDefinition,
    rollup: &StoredAccountingRollup,
    current: &IndexBarrier,
) -> Result<Option<(AccountingObjectSnapshot, IndexBarrier)>, Status> {
    if !rollup.complete || rollup.definition_version != definition.version.0 {
        return Ok(None);
    }
    let through = rollup.barrier()?;
    Ok(resumable_barrier(&through, current)
        .then(|| (AccountingObjectSnapshot::from_rollup(rollup), through)))
}

fn resumable_barrier(previous: &IndexBarrier, current: &IndexBarrier) -> bool {
    previous.fence == current.fence
        && previous.atomic.finalized_through() <= current.atomic.finalized_through()
        && previous.sources.len() == current.sources.len()
        && previous.sources.iter().all(|(node, source)| {
            current.sources.get(node).is_some_and(|candidate| {
                candidate.source == source.source && candidate.next_offset >= source.next_offset
            })
        })
}

fn event_requires_baseline(error: &IndexEventError) -> bool {
    matches!(
        error,
        IndexEventError::IncompleteSources
            | IndexEventError::InvalidSourceStatus(_)
            | IndexEventError::CheckpointMismatch(_)
            | IndexEventError::SourceHistoryGap(_)
            | IndexEventError::SourceEpochChanged(_)
            | IndexEventError::NonContiguousSource(_)
    )
}

fn event_status(error: IndexEventError) -> Status {
    Status::unavailable(error.to_string())
}

fn apply_page(
    definition: &LoadedAccountingDefinition,
    snapshot: &mut AccountingObjectSnapshot,
    page: &IndexJournalPage,
) -> Result<(), super::snapshot::AccountingAdvanceError> {
    snapshot.apply(definition, page)?;
    Ok(())
}

async fn publish_snapshot(
    definition: &LoadedAccountingDefinition,
    dependencies: &AccountingBuilderDependencies,
    snapshot: &AccountingObjectSnapshot,
    barrier: &IndexBarrier,
) -> Result<(), Status> {
    let existing = read_rollup(definition, &dependencies.reader).await?;
    let expected_version = existing.as_ref().map(|(version, _)| *version);
    let previous = existing
        .as_ref()
        .map(|(_, rollup)| rollup)
        .filter(|rollup| rollup.definition_version == definition.version.0);
    let (inbound, outbound, traffic_sources) =
        merge_traffic(definition, barrier, previous, &dependencies.reader).await?;
    let rollup = StoredAccountingRollup::new(
        definition.stored.accounting_id,
        definition.version.0,
        snapshot.logical_stored_bytes(),
        snapshot.object_count(),
        inbound,
        outbound,
        true,
        barrier,
        traffic_sources,
    )?;
    let command_id = rollup_command_id(&rollup)?;
    dependencies
        .publisher
        .publish_rollup(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
            &rollup,
            definition.version,
            expected_version,
            command_id,
        )
        .await?;
    dependencies
        .derived_progress
        .report(
            derived_identity(definition),
            DerivedBarrierEvidence::Published(barrier.clone()),
        )
        .await;
    Ok(())
}

fn derived_identity(definition: &LoadedAccountingDefinition) -> DerivedDefinitionIdentity {
    DerivedDefinitionIdentity {
        kind: DefinitionKind::Accounting,
        tenant_id: definition.tenant_id,
        bucket_id: definition.bucket_id,
        definition_id: definition.stored.accounting_id,
        object_version: definition.version,
    }
}

async fn merge_traffic(
    definition: &LoadedAccountingDefinition,
    barrier: &IndexBarrier,
    previous: Option<&StoredAccountingRollup>,
    reader: &ClusterObjectReader,
) -> Result<(u64, u64, Vec<StoredTrafficCheckpoint>), Status> {
    let mut inbound = previous.map_or(0, |value| value.accepted_inbound_bytes);
    let mut outbound = previous.map_or(0, |value| value.served_outbound_bytes);
    let mut checkpoints = previous
        .map(|value| {
            value
                .traffic_sources
                .iter()
                .map(|source| (source.node_id, source.clone()))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    for node in barrier.sources.keys() {
        let Some(source) = read_traffic_source(definition, *node, reader).await? else {
            continue;
        };
        if source.definition_version != definition.version.0 {
            continue;
        }
        let old = checkpoints.get(&node.0);
        let old_inbound = old.map_or(0, |value| value.accepted_inbound_bytes);
        let old_outbound = old.map_or(0, |value| value.served_outbound_bytes);
        inbound = inbound
            .checked_add(source.accepted_inbound_bytes.saturating_sub(old_inbound))
            .ok_or_else(|| Status::resource_exhausted("accounting inbound total overflow"))?;
        outbound = outbound
            .checked_add(source.served_outbound_bytes.saturating_sub(old_outbound))
            .ok_or_else(|| Status::resource_exhausted("accounting outbound total overflow"))?;
        checkpoints.insert(
            node.0,
            StoredTrafficCheckpoint {
                node_id: node.0,
                accepted_inbound_bytes: source.accepted_inbound_bytes,
                served_outbound_bytes: source.served_outbound_bytes,
            },
        );
    }
    Ok((inbound, outbound, checkpoints.into_values().collect()))
}

pub(crate) async fn read_rollup(
    definition: &LoadedAccountingDefinition,
    reader: &ClusterObjectReader,
) -> Result<Option<(VersionId, StoredAccountingRollup)>, Status> {
    let path = current_path(definition.stored.accounting_id)?;
    let Some((version, bytes)) = read_object(definition, &path, reader).await? else {
        return Ok(None);
    };
    Ok(Some((version, StoredAccountingRollup::decode(&bytes)?)))
}

pub(crate) async fn read_traffic_source(
    definition: &LoadedAccountingDefinition,
    node: NodeId,
    reader: &ClusterObjectReader,
) -> Result<Option<StoredTrafficSource>, Status> {
    Ok(read_traffic_source_versioned(definition, node, reader)
        .await?
        .map(|(_, source)| source))
}

pub(crate) async fn read_traffic_source_versioned(
    definition: &LoadedAccountingDefinition,
    node: NodeId,
    reader: &ClusterObjectReader,
) -> Result<Option<(VersionId, StoredTrafficSource)>, Status> {
    let path = outbound_source_path(definition.stored.accounting_id, node.0)?;
    let Some((version, bytes)) = read_object(definition, &path, reader).await? else {
        return Ok(None);
    };
    let source = StoredTrafficSource::decode(&bytes)?;
    if source.accounting_id != definition.stored.accounting_id || source.node_id != node.0 {
        return Err(Status::data_loss(
            "accounting traffic source identity does not match its path",
        ));
    }
    Ok(Some((version, source)))
}

async fn read_object(
    definition: &LoadedAccountingDefinition,
    path: &str,
    reader: &ClusterObjectReader,
) -> Result<Option<(VersionId, Vec<u8>)>, Status> {
    let key = ObjectKey::new(
        &definition.stored.storage_tenant,
        &definition.stored.bucket,
        path,
    )
    .map_err(|error| Status::data_loss(error.to_string()))?;
    let Some(opened) = reader
        .open_stable(&key, definition.tenant_id, definition.bucket_id, None)
        .await?
    else {
        return Ok(None);
    };
    if opened.version.deleted {
        return Ok(None);
    }
    let mut payload = opened
        .payload
        .ok_or_else(|| Status::data_loss("live accounting object has no readable payload"))?;
    let mut bytes = Vec::new();
    payload
        .read_to_end(&mut bytes)
        .map_err(|error| Status::internal(format!("read accounting object: {error}")))?;
    Ok(Some((opened.version.id, bytes)))
}

fn rollup_command_id(rollup: &StoredAccountingRollup) -> Result<String, Status> {
    let encoded = rollup.encode()?;
    let hash = blake3::hash(&encoded);
    Ok(format!(
        "accounting-rollup-{}-{}",
        rollup.accounting_id,
        hex::encode(&hash.as_bytes()[..16])
    ))
}

fn assignment(
    definition: &LoadedAccountingDefinition,
    placement: &ClusterPlacement,
) -> Result<IndexPlacement, Status> {
    let identity = IndexIdentity::new(
        definition.tenant_id,
        definition.bucket_id,
        definition.stored.accounting_id,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))?;
    IndexPlacement::derive(identity, placement)
        .map_err(|error| Status::unavailable(error.to_string()))
}

fn current_placement(decisions: &DecisionRaft) -> Result<ClusterPlacement, Status> {
    let state = decisions
        .state()
        .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
    ClusterPlacement::from_applied(&state).map_err(|error| Status::unavailable(error.to_string()))
}

#[cfg(test)]
mod tests {
    use anvil_store::{PlacementLogId, SourceId};

    use super::*;
    use crate::index_runtime::events::{AtomicProgramWatermark, IndexSourceCursor};

    fn definition_at(path_prefix: &str) -> LoadedAccountingDefinition {
        LoadedAccountingDefinition {
            tenant_id: 11,
            bucket_id: 12,
            version: VersionId(7),
            stored: StoredAccountingDefinition::create(
                "tenant".into(),
                "bucket".into(),
                path_prefix.into(),
                11,
                12,
            )
            .unwrap(),
        }
    }

    fn definition() -> LoadedAccountingDefinition {
        definition_at("docs")
    }

    fn barrier(next_offset: u64) -> IndexBarrier {
        IndexBarrier {
            fence: PlacementLogId { term: 2, index: 3 },
            atomic: AtomicProgramWatermark::new(Some(5), Some(5), 0),
            sources: BTreeMap::from([(
                NodeId(4),
                IndexSourceCursor {
                    source: SourceId {
                        node_id: 4,
                        source_epoch: [6; 32],
                    },
                    next_offset,
                },
            )]),
        }
    }

    #[test]
    fn compatible_complete_rollup_resumes_without_a_baseline() {
        let definition = definition();
        let rollup = StoredAccountingRollup::new(
            definition.stored.accounting_id,
            definition.version.0,
            90,
            3,
            4,
            5,
            true,
            &barrier(9),
            Vec::new(),
        )
        .unwrap();
        let (snapshot, through) = resume_rollup(&definition, &rollup, &barrier(12))
            .unwrap()
            .expect("compatible rollup must resume");
        assert_eq!(snapshot.logical_stored_bytes(), 90);
        assert_eq!(snapshot.object_count(), 3);
        assert_eq!(through.sources[&NodeId(4)].next_offset, 9);
    }

    #[test]
    fn scheduler_holds_only_one_cold_baseline_owner() {
        let mut scheduler = AccountingScheduler::default();
        scheduler.baseline_owner = Some((1, 2, 3));
        scheduler.workers.insert(
            (1, 2, 4),
            AccountingWorker {
                definition: definition(),
                phase: WorkerPhase::AwaitBaseline,
            },
        );
        scheduler.wait_for_baseline((1, 2, 4));
        scheduler.wake_baseline_waiter();
        assert!(scheduler.pop_ready().is_none());
        scheduler.baseline_owner = None;
        scheduler.wake_baseline_waiter();
        assert_eq!(scheduler.pop_ready(), Some((1, 2, 4)));
    }

    #[test]
    fn transient_failures_retain_state_but_gaps_require_a_scoped_baseline() {
        assert!(!event_requires_baseline(&IndexEventError::Source {
            node: NodeId(4),
            message: "peer unavailable".into(),
        }));
        assert!(!event_requires_baseline(
            &IndexEventError::AtomicProgramInProgress
        ));
        assert!(event_requires_baseline(
            &IndexEventError::SourceEpochChanged(NodeId(4))
        ));
        assert!(event_requires_baseline(
            &IndexEventError::CheckpointMismatch(NodeId(4))
        ));
        assert!(event_requires_baseline(&IndexEventError::SourceHistoryGap(
            NodeId(4)
        )));
    }

    #[test]
    fn delayed_active_lease_wakes_at_its_retry_deadline() {
        let now = Instant::now();
        let mut scheduler = AccountingScheduler::default();
        scheduler.delay((1, 2, 3), RETRY_INTERVAL);
        let wake = next_scheduler_wake_std(&scheduler, now);
        assert!(wake >= now && wake <= Instant::now() + RETRY_INTERVAL);
    }

    #[test]
    fn transient_failure_yields_a_lease_to_a_later_definition() {
        let mut scheduler = AccountingScheduler::default();
        for ordinal in 0..MAX_ACTIVE_ACCOUNTING_WORKERS {
            assert!(
                scheduler.apply_change(AccountingCatalogChange::Upsert(definition_at(&format!(
                    "docs/{ordinal}"
                ))))
            );
        }
        let later = definition_at("docs/later");
        let later_identity = (later.tenant_id, later.bucket_id, later.stored.accounting_id);
        assert!(!scheduler.apply_change(AccountingCatalogChange::Upsert(later.clone())));

        let failing = *scheduler.workers.keys().next().unwrap();
        scheduler.baseline_owner = Some(failing);
        scheduler.retry(failing);

        assert!(!scheduler.workers.contains_key(&failing));
        assert!(scheduler.baseline_owner.is_none());
        assert!(scheduler.apply_change(AccountingCatalogChange::Upsert(later)));
        assert!(scheduler.workers.contains_key(&later_identity));
    }
}
