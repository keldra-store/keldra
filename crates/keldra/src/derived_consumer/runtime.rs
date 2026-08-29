//! Bounded source demultiplexing for aggregate derived-consumer retention.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use keldra_consensus::{DecisionRaft, NodeId};
use keldra_store::{
    DefinitionAssignment, DefinitionAssignmentCursor, DefinitionAssignmentMutation,
    DefinitionConsumerKind, DefinitionKind, DerivedConsumerKind, LocalChange,
    MAX_DEFINITION_STATE_SCAN_RECORDS, PlacementLogId, SourceId, Store, WatchJournalStatus,
};
use tonic::Status;

use crate::accounting::{AccountingCatalog, LoadedAccountingDefinition, read_rollup};
use crate::cluster_object_read::ClusterObjectReader;
use crate::index_runtime::catalog::IndexCatalog;
use crate::index_runtime::coordination::{current_placement, load_index_assignment};
use crate::index_runtime::events::{
    IndexBarrier, IndexEventJournal, MAX_INDEX_EVENT_PAGE_BYTES, RoutedSourceEffect,
};
use crate::index_runtime::publisher::IndexCommitPublisher;

use super::{
    DerivedBarrierEvidence, DerivedCheckpointPublisher, DerivedDefinitionIdentity,
    SparseDerivedInventory, SparseDerivedTracker, assigned::AssignedBucketInventory,
};

const RETRY_INTERVAL: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const PROGRESS_CHANNEL_CAPACITY: usize = 1_024;

#[derive(Clone)]
pub(crate) struct DerivedProgressReporter {
    kind: DerivedConsumerKind,
    sender: tokio::sync::mpsc::Sender<ProgressMessage>,
}

struct ProgressMessage {
    identity: DerivedDefinitionIdentity,
    evidence: DerivedBarrierEvidence,
}

impl DerivedProgressReporter {
    fn channel(kind: DerivedConsumerKind) -> (Self, tokio::sync::mpsc::Receiver<ProgressMessage>) {
        let (sender, receiver) = tokio::sync::mpsc::channel(PROGRESS_CHANNEL_CAPACITY);
        (Self { kind, sender }, receiver)
    }

    pub(crate) async fn report(
        &self,
        identity: DerivedDefinitionIdentity,
        evidence: DerivedBarrierEvidence,
    ) {
        if identity.kind != definition_kind(self.kind) {
            tracing::error!("derived progress reporter received another consumer kind");
            return;
        }
        if self
            .sender
            .send(ProgressMessage { identity, evidence })
            .await
            .is_err()
        {
            tracing::warn!(
                consumer.kind = ?self.kind,
                "derived progress reporter is unavailable"
            );
        }
    }
}

#[derive(Clone)]
pub(crate) enum DerivedEvidenceResolver {
    Index {
        local_node: NodeId,
        decisions: DecisionRaft,
        reader: ClusterObjectReader,
        publisher: IndexCommitPublisher,
        catalog: IndexCatalog,
    },
    Accounting {
        local_node: NodeId,
        decisions: DecisionRaft,
        reader: ClusterObjectReader,
        catalog: AccountingCatalog,
    },
}

impl DerivedEvidenceResolver {
    pub(crate) fn index(
        local_node: NodeId,
        decisions: DecisionRaft,
        reader: ClusterObjectReader,
        publisher: IndexCommitPublisher,
        catalog: IndexCatalog,
    ) -> Self {
        Self::Index {
            local_node,
            decisions,
            reader,
            publisher,
            catalog,
        }
    }

    pub(crate) fn accounting(
        local_node: NodeId,
        decisions: DecisionRaft,
        reader: ClusterObjectReader,
        catalog: AccountingCatalog,
    ) -> Self {
        Self::Accounting {
            local_node,
            decisions,
            reader,
            catalog,
        }
    }

    async fn affected(
        &self,
        assignment: &DefinitionAssignment,
        effects: &BTreeMap<SourceId, RoutedSourceEffect>,
    ) -> Result<Option<DerivedBarrierEvidence>, Status> {
        match self {
            Self::Index {
                local_node,
                decisions,
                reader,
                publisher,
                catalog,
            } => {
                let Some(definition) =
                    load_index_assignment(*local_node, decisions, reader, assignment).await?
                else {
                    return Ok(None);
                };
                let current = publisher
                    .load_current(
                        &definition.stored,
                        definition.tenant_id,
                        definition.bucket_id,
                    )
                    .await?;
                let evidence = current
                    .filter(|current| {
                        current.manifest.definition_version == definition.object_version
                    })
                    .map(|current| {
                        current
                            .manifest
                            .barrier()
                            .map(DerivedBarrierEvidence::Published)
                            .map_err(|error| Status::data_loss(error.to_string()))
                    })
                    .transpose()?;
                if !evidence_covers_effects(evidence.as_ref(), effects) {
                    catalog.upsert_wait(definition).await?;
                }
                Ok(evidence)
            }
            Self::Accounting {
                local_node,
                decisions,
                reader,
                catalog,
            } => {
                let Some(definition) = crate::accounting::runtime::load_assignment(
                    *local_node,
                    decisions,
                    reader,
                    assignment,
                )
                .await?
                else {
                    return Ok(None);
                };
                let evidence = accounting_evidence(&definition, reader).await?;
                if !evidence_covers_effects(evidence.as_ref(), effects) {
                    catalog.upsert(definition)?;
                }
                Ok(evidence)
            }
        }
    }
}

async fn accounting_evidence(
    definition: &LoadedAccountingDefinition,
    reader: &ClusterObjectReader,
) -> Result<Option<DerivedBarrierEvidence>, Status> {
    let Some((_, rollup)) = read_rollup(definition, reader).await? else {
        return Ok(None);
    };
    if !rollup.complete || rollup.definition_version != definition.version.0 {
        return Ok(None);
    }
    Ok(Some(DerivedBarrierEvidence::Published(rollup.barrier()?)))
}

pub(crate) struct DerivedConsumerRuntimeTask {
    task: tokio::task::JoinHandle<()>,
}

impl DerivedConsumerRuntimeTask {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        kind: DerivedConsumerKind,
        local_node: NodeId,
        decisions: DecisionRaft,
        store: Store,
        journal: Arc<IndexEventJournal>,
        publisher: DerivedCheckpointPublisher,
        resolver: DerivedEvidenceResolver,
    ) -> (DerivedProgressReporter, Self) {
        let (reporter, receiver) = DerivedProgressReporter::channel(kind);
        let assignment_changes = store.subscribe_definition_assignment_changes();
        let task = tokio::spawn(run(
            kind,
            local_node,
            decisions,
            store,
            journal,
            publisher,
            resolver,
            receiver,
            assignment_changes,
        ));
        (reporter, Self { task })
    }
}

impl Drop for DerivedConsumerRuntimeTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct RuntimeState {
    tracker: SparseDerivedTracker,
    demux: IndexBarrier,
    assignments: AssignedBucketInventory,
}

#[allow(clippy::too_many_arguments)]
async fn run(
    kind: DerivedConsumerKind,
    local_node: NodeId,
    decisions: DecisionRaft,
    store: Store,
    journal: Arc<IndexEventJournal>,
    publisher: DerivedCheckpointPublisher,
    resolver: DerivedEvidenceResolver,
    mut progress: tokio::sync::mpsc::Receiver<ProgressMessage>,
    mut assignment_changes: tokio::sync::broadcast::Receiver<Vec<DefinitionAssignmentMutation>>,
) {
    loop {
        assignment_changes = assignment_changes.resubscribe();
        let initialized = initialize(
            kind,
            local_node,
            &decisions,
            &store,
            &journal,
            &publisher,
            &resolver,
            &mut assignment_changes,
        )
        .await;
        let mut state = match initialized {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!(consumer.kind = ?kind, %error, "derived consumer inventory will retry");
                tokio::time::sleep(RETRY_INTERVAL).await;
                continue;
            }
        };
        let mut interval = tokio::time::interval(POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let result = tokio::select! {
                message = progress.recv() => match message {
                    Some(message) => apply_progress(&publisher, &mut state.tracker, message).await,
                    None => return,
                },
                received = assignment_changes.recv() => match received {
                    Ok(mutations) => apply_assignment_changes(
                        &mut state,
                        mutations,
                    ),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => Err(
                        Status::unavailable(format!(
                            "derived assignment notifications lagged by {skipped} batches",
                        )),
                    ),
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                },
                _ = interval.tick() => advance_once(
                    kind,
                    &decisions,
                    &store,
                    &journal,
                    &publisher,
                    &resolver,
                    &mut assignment_changes,
                    &mut state,
                ).await,
            };
            if let Err(error) = result {
                tracing::warn!(consumer.kind = ?kind, %error, "derived consumer progress will rebuild its disposable inventory");
                break;
            }
        }
    }
}

async fn initialize(
    kind: DerivedConsumerKind,
    local_node: NodeId,
    decisions: &DecisionRaft,
    store: &Store,
    journal: &IndexEventJournal,
    publisher: &DerivedCheckpointPublisher,
    resolver: &DerivedEvidenceResolver,
    assignment_changes: &mut tokio::sync::broadcast::Receiver<Vec<DefinitionAssignmentMutation>>,
) -> Result<RuntimeState, Status> {
    let target = journal.capture_barrier().await.map_err(event_status)?;
    wait_for_assignment_delivery(kind, decisions, store, &target).await?;
    let statuses = publisher.source_statuses().await?;
    let sources = inventory_sources(kind, publisher, &target, statuses)?;
    let from = baseline_barrier(&target, &sources)?;
    let mut inventory = SparseDerivedInventory::begin(
        kind,
        local_node,
        target.fence,
        sources
            .iter()
            .map(|(status, checkpoint)| (*status, checkpoint.clone())),
    )
    .map_err(tracker_status)?;
    let mut assignments = AssignedBucketInventory::new(definition_kind(kind), target.fence);
    scan_inventory(
        kind,
        store,
        journal,
        resolver,
        &from,
        &target,
        &mut inventory,
        &mut assignments,
    )
    .await?;
    // The checkpoint read shares the store's assignment-state lock with page
    // application, so every notification through `target` has been sent when
    // this returns. Drain them before this disposable inventory is trusted.
    wait_for_assignment_delivery(kind, decisions, store, &target).await?;
    let mut tracker = inventory.finish();
    drain_assignment_changes(&mut assignments, &mut tracker, assignment_changes)?;
    publisher.publish_tracker(&mut tracker).await?;
    Ok(RuntimeState {
        tracker,
        demux: target,
        assignments,
    })
}

fn inventory_sources(
    kind: DerivedConsumerKind,
    publisher: &DerivedCheckpointPublisher,
    target: &IndexBarrier,
    statuses: Vec<WatchJournalStatus>,
) -> Result<
    Vec<(
        WatchJournalStatus,
        Option<keldra_store::DefinitionCheckpoint>,
    )>,
    Status,
> {
    let mut result = Vec::with_capacity(statuses.len());
    for mut status in statuses {
        let node = NodeId(u64::from(status.source_id.node_id));
        let cursor = target
            .sources
            .get(&node)
            .ok_or_else(|| Status::data_loss("source status is absent from captured barrier"))?;
        if cursor.source != status.source_id
            || cursor.next_offset > status.settled_through.saturating_add(1)
        {
            return Err(Status::out_of_range(
                "captured source barrier and status disagree",
            ));
        }
        // Only the captured barrier was inventoried. A source may have
        // accepted later writes while that bounded scan ran, but they cannot
        // enter this aggregate proof until the next demultiplexing cycle.
        status.settled_through = cursor.next_offset.saturating_sub(1);
        result.push((
            status,
            publisher.local_checkpoint(kind, status.source_id.node_id)?,
        ));
    }
    if result.len() != target.sources.len() {
        return Err(Status::data_loss("captured source set is incomplete"));
    }
    Ok(result)
}

fn baseline_barrier(
    target: &IndexBarrier,
    sources: &[(
        WatchJournalStatus,
        Option<keldra_store::DefinitionCheckpoint>,
    )],
) -> Result<IndexBarrier, Status> {
    let mut baseline = target.clone();
    for (status, checkpoint) in sources {
        let node = NodeId(u64::from(status.source_id.node_id));
        let floor_next = status
            .retention_floor
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("source floor exhausted"))?;
        let next = checkpoint
            .as_ref()
            .filter(|checkpoint| {
                checkpoint.source_id == status.source_id
                    && checkpoint.observed_fence == target.fence
            })
            .map_or(floor_next, |checkpoint| checkpoint.next_offset);
        if next < floor_next || next > target.sources[&node].next_offset {
            return Err(Status::out_of_range(
                "aggregate checkpoint is outside retained source history",
            ));
        }
        baseline.sources.get_mut(&node).unwrap().next_offset = next;
    }
    Ok(baseline)
}

async fn scan_inventory(
    kind: DerivedConsumerKind,
    store: &Store,
    journal: &IndexEventJournal,
    resolver: &DerivedEvidenceResolver,
    from: &IndexBarrier,
    target: &IndexBarrier,
    inventory: &mut SparseDerivedInventory,
    assignments: &mut AssignedBucketInventory,
) -> Result<(), Status> {
    let mut cursor: Option<DefinitionAssignmentCursor> = None;
    let mut bucket = None;
    let mut effects = BTreeMap::new();
    loop {
        let page = scan_assignments(store, kind, cursor.as_ref()).await?;
        for assignment in page.assignments {
            if assignment.rank != 0 || assignment.observed_fence != target.fence {
                continue;
            }
            assignments.insert_scanned(assignment.clone());
            let current_bucket = (assignment.tenant_id, assignment.bucket_id);
            if bucket != Some(current_bucket) {
                effects = routed_effects(
                    kind,
                    journal,
                    current_bucket.0,
                    current_bucket.1,
                    from,
                    target,
                )
                .await?;
                bucket = Some(current_bucket);
            }
            if effects.is_empty() {
                continue;
            }
            let evidence = resolver.affected(&assignment, &effects).await?;
            for (&source, effect) in &effects {
                inventory
                    .record_affected(
                        &assignment,
                        source,
                        effect.next_offset,
                        (kind == DerivedConsumerKind::Index)
                            .then_some(effect.required_atomic_cursor)
                            .flatten(),
                        (kind == DerivedConsumerKind::Index)
                            .then_some(effect.atomic_hold_next)
                            .flatten(),
                        evidence.as_ref(),
                    )
                    .map_err(tracker_status)?;
            }
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
}

async fn apply_progress(
    publisher: &DerivedCheckpointPublisher,
    tracker: &mut SparseDerivedTracker,
    message: ProgressMessage,
) -> Result<(), Status> {
    tracker
        .observe_proof_identity(message.identity, &message.evidence)
        .map_err(tracker_status)?;
    publisher.publish_tracker(tracker).await
}

fn apply_assignment_changes(
    state: &mut RuntimeState,
    mutations: Vec<DefinitionAssignmentMutation>,
) -> Result<(), Status> {
    apply_assignment_mutations(&mut state.assignments, &mut state.tracker, mutations)
}

async fn advance_once(
    kind: DerivedConsumerKind,
    decisions: &DecisionRaft,
    store: &Store,
    journal: &IndexEventJournal,
    publisher: &DerivedCheckpointPublisher,
    resolver: &DerivedEvidenceResolver,
    assignment_changes: &mut tokio::sync::broadcast::Receiver<Vec<DefinitionAssignmentMutation>>,
    state: &mut RuntimeState,
) -> Result<(), Status> {
    let target = journal.capture_barrier().await.map_err(event_status)?;
    require_compatible(&state.demux, &target)?;
    if state.demux == target {
        return Ok(());
    }
    wait_for_assignment_delivery(kind, decisions, store, &target).await?;
    drain_assignment_changes(
        &mut state.assignments,
        &mut state.tracker,
        assignment_changes,
    )?;
    // Routed effects are bounded by the target captured above. Move the
    // disposable tracker's settled view to that same target before asking it
    // to validate those effects; validating first compares new offsets with
    // the previous polling turn and spuriously rejects healthy live traffic.
    let statuses = publisher.source_statuses().await?;
    for mut status in statuses {
        let node = NodeId(u64::from(status.source_id.node_id));
        let cursor = target
            .sources
            .get(&node)
            .ok_or_else(|| Status::out_of_range("source incarnation changed"))?;
        if cursor.source != status.source_id {
            return Err(Status::out_of_range("source incarnation changed"));
        }
        status.settled_through = cursor.next_offset.saturating_sub(1);
        status.retained_entries = status.tail.saturating_sub(status.retention_floor);
        state
            .tracker
            .update_source_status(status)
            .map_err(tracker_status)?;
    }
    scan_steady(
        kind,
        journal,
        resolver,
        &state.demux,
        &target,
        &state.assignments,
        &mut state.tracker,
    )
    .await?;
    require_fence(decisions, target.fence)?;
    publisher.publish_tracker(&mut state.tracker).await?;
    state.demux = target;
    Ok(())
}

async fn scan_steady(
    kind: DerivedConsumerKind,
    journal: &IndexEventJournal,
    resolver: &DerivedEvidenceResolver,
    from: &IndexBarrier,
    target: &IndexBarrier,
    assignments: &AssignedBucketInventory,
    tracker: &mut SparseDerivedTracker,
) -> Result<(), Status> {
    let bucket_count = assignments.bucket_count();
    if bucket_count == 0 {
        return Ok(());
    }
    let (interval_records, changed_sources) = interval_shape(from, target)?;
    if demux_strategy(bucket_count, interval_records, changed_sources) == DemuxStrategy::Raw {
        return scan_raw_interval(kind, journal, resolver, from, target, assignments, tracker)
            .await;
    }
    for ((tenant_id, bucket_id), definitions) in assignments.buckets() {
        let effects = routed_effects(kind, journal, tenant_id, bucket_id, from, target).await?;
        if effects.is_empty() {
            continue;
        }
        apply_bucket_effects(definitions, &effects, resolver, tracker).await?;
    }
    Ok(())
}

async fn scan_raw_interval(
    kind: DerivedConsumerKind,
    journal: &IndexEventJournal,
    resolver: &DerivedEvidenceResolver,
    from: &IndexBarrier,
    target: &IndexBarrier,
    assignments: &AssignedBucketInventory,
    tracker: &mut SparseDerivedTracker,
) -> Result<(), Status> {
    let mut through = from.clone();
    while let Some(page) = journal
        .next_raw_page(&through, target, MAX_INDEX_EVENT_PAGE_BYTES)
        .await
        .map_err(event_status)?
    {
        let mut effects = BTreeMap::<(u64, u64), BTreeMap<SourceId, RoutedSourceEffect>>::new();
        for change in &page.changes {
            let event_source = page.through.sources[&change.node].source;
            let source_effects =
                crate::index_runtime::events::source_change_effects(event_source, &change.change)
                    .map_err(event_status)?;
            for bucket in change_buckets(kind, &change.change) {
                if assignments.definitions(bucket.0, bucket.1).is_none() {
                    continue;
                }
                let bucket_effects = effects.entry(bucket).or_default();
                for effect in &source_effects {
                    let source = effect.source;
                    let next = effect.next_offset;
                    bucket_effects
                        .entry(source)
                        .and_modify(|current| {
                            current.next_offset = current.next_offset.max(next);
                            if kind == DerivedConsumerKind::Index {
                                current.required_atomic_cursor = current
                                    .required_atomic_cursor
                                    .max(effect.required_atomic_cursor);
                                current.atomic_hold_next = current
                                    .atomic_hold_next
                                    .into_iter()
                                    .chain(effect.atomic_hold_next)
                                    .min();
                            }
                        })
                        .or_insert(RoutedSourceEffect {
                            next_offset: next,
                            required_atomic_cursor: (kind == DerivedConsumerKind::Index)
                                .then_some(effect.required_atomic_cursor)
                                .flatten(),
                            atomic_hold_next: (kind == DerivedConsumerKind::Index)
                                .then_some(effect.atomic_hold_next)
                                .flatten(),
                        });
                }
            }
        }
        for ((tenant_id, bucket_id), effects) in effects {
            let definitions = assignments
                .definitions(tenant_id, bucket_id)
                .expect("raw effects were filtered by the assignment inventory");
            apply_bucket_effects(definitions, &effects, resolver, tracker).await?;
        }
        through = page.through;
    }
    Ok(())
}

async fn routed_effects(
    kind: DerivedConsumerKind,
    journal: &IndexEventJournal,
    tenant_id: u64,
    bucket_id: u64,
    from: &IndexBarrier,
    target: &IndexBarrier,
) -> Result<BTreeMap<SourceId, RoutedSourceEffect>, Status> {
    let mut effects = match kind {
        DerivedConsumerKind::Index => {
            journal
                .routed_index_effects(tenant_id, bucket_id, from, target)
                .await
        }
        DerivedConsumerKind::Accounting => {
            journal
                .routed_accounting_effects(tenant_id, bucket_id, from, target)
                .await
        }
    }
    .map_err(event_status)?;
    if kind == DerivedConsumerKind::Accounting {
        for effect in effects.values_mut() {
            effect.required_atomic_cursor = None;
            effect.atomic_hold_next = None;
        }
    }
    Ok(effects)
}

async fn apply_bucket_effects(
    definitions: &BTreeMap<u64, DefinitionAssignment>,
    effects: &BTreeMap<SourceId, RoutedSourceEffect>,
    resolver: &DerivedEvidenceResolver,
    tracker: &mut SparseDerivedTracker,
) -> Result<(), Status> {
    for assignment in definitions.values() {
        let evidence = resolver.affected(assignment, effects).await?;
        for (&source, effect) in effects {
            tracker
                .observe_routed_effect(
                    assignment,
                    source,
                    effect.next_offset,
                    effect.required_atomic_cursor,
                    effect.atomic_hold_next,
                    evidence.as_ref(),
                )
                .map_err(tracker_status)?;
        }
    }
    Ok(())
}

fn evidence_covers_effects(
    evidence: Option<&DerivedBarrierEvidence>,
    effects: &BTreeMap<SourceId, RoutedSourceEffect>,
) -> bool {
    let Some(evidence) = evidence else {
        return false;
    };
    let barrier = match evidence {
        DerivedBarrierEvidence::Published(barrier) => barrier,
    };
    effects.iter().all(|(source, effect)| {
        let node = NodeId(u64::from(source.node_id));
        let source_covered = barrier.sources.get(&node).is_some_and(|cursor| {
            cursor.source == *source && cursor.next_offset >= effect.next_offset
        });
        source_covered
            && effect.required_atomic_cursor.is_none_or(|required| {
                barrier
                    .atomic
                    .finalized_through()
                    .is_some_and(|through| through >= required)
            })
    })
}

fn interval_shape(from: &IndexBarrier, target: &IndexBarrier) -> Result<(u64, u64), Status> {
    require_compatible(from, target)?;
    let mut records = 0_u64;
    let mut changed_sources = 0_u64;
    for (node, start) in &from.sources {
        let end = target.sources[node].next_offset;
        let count = end
            .checked_sub(start.next_offset)
            .ok_or_else(|| Status::out_of_range("derived source cursor regressed"))?;
        records = records.saturating_add(count);
        changed_sources += u64::from(count != 0);
    }
    Ok((records, changed_sources))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DemuxStrategy {
    Empty,
    Routed,
    Raw,
}

fn demux_strategy(
    bucket_count: usize,
    interval_records: u64,
    changed_sources: u64,
) -> DemuxStrategy {
    if bucket_count == 0 {
        return DemuxStrategy::Empty;
    }
    let route_probes = u64::try_from(bucket_count)
        .unwrap_or(u64::MAX)
        .saturating_mul(changed_sources);
    if interval_records < route_probes {
        DemuxStrategy::Raw
    } else {
        DemuxStrategy::Routed
    }
}

fn change_buckets(kind: DerivedConsumerKind, change: &LocalChange) -> Vec<(u64, u64)> {
    let included = match kind {
        DerivedConsumerKind::Index => crate::index_runtime::events::is_index_source_change(change),
        DerivedConsumerKind::Accounting => crate::accounting::is_accounting_source_change(change),
    };
    if !included {
        return Vec::new();
    }
    match change {
        LocalChange::ObjectHead(change) => vec![(change.tenant_id, change.bucket_id)],
        LocalChange::RetainedVersionDeleted(change) => {
            vec![(change.tenant_id, change.bucket_id)]
        }
        LocalChange::AtomicBatchPublished(change) => change
            .affected_routes
            .iter()
            .map(|route| (route.tenant_id, route.bucket_id))
            .collect(),
        LocalChange::ContentLifecycleChanged(change) => change
            .accounting_transition
            .as_ref()
            .map(|transition| vec![(transition.tenant_id, transition.bucket_id)])
            .unwrap_or_default(),
        LocalChange::AggregateChanged(_) => Vec::new(),
        _ => Vec::new(),
    }
}

fn drain_assignment_changes(
    assignments: &mut AssignedBucketInventory,
    tracker: &mut SparseDerivedTracker,
    receiver: &mut tokio::sync::broadcast::Receiver<Vec<DefinitionAssignmentMutation>>,
) -> Result<(), Status> {
    loop {
        match receiver.try_recv() {
            Ok(mutations) => apply_assignment_mutations(assignments, tracker, mutations)?,
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => return Ok(()),
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                return Err(Status::unavailable(
                    "derived assignment notifications closed",
                ));
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(skipped)) => {
                return Err(Status::unavailable(format!(
                    "derived assignment notifications lagged by {skipped} batches",
                )));
            }
        }
    }
}

fn apply_assignment_mutations(
    assignments: &mut AssignedBucketInventory,
    tracker: &mut SparseDerivedTracker,
    mutations: Vec<DefinitionAssignmentMutation>,
) -> Result<(), Status> {
    for mutation in mutations {
        mutation
            .validate()
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if let Some(removed) = assignments.apply(mutation) {
            tracker.remove_identity(removed).map_err(tracker_status)?;
        }
    }
    Ok(())
}

async fn wait_for_assignment_delivery(
    kind: DerivedConsumerKind,
    decisions: &DecisionRaft,
    store: &Store,
    target: &IndexBarrier,
) -> Result<(), Status> {
    loop {
        // A checkpoint for a captured fence can never arrive after membership
        // has moved on. Return to the outer runtime so it can capture and
        // inventory the new fence rather than sleeping forever on stale work.
        require_fence(decisions, target.fence)?;
        let mut complete = true;
        for cursor in target.sources.values() {
            let next = assignment_delivery_next(kind, store, cursor.source, target.fence).await?;
            complete &= next >= cursor.next_offset;
        }
        if complete {
            require_fence(decisions, target.fence)?;
            return Ok(());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn assignment_delivery_next(
    kind: DerivedConsumerKind,
    store: &Store,
    source: SourceId,
    fence: PlacementLogId,
) -> Result<u64, Status> {
    let store = store.clone();
    let checkpoint = tokio::task::spawn_blocking(move || {
        store.definition_checkpoint(assignment_kind(kind), source.node_id)
    })
    .await
    .map_err(join_status)?
    .map_err(internal_status)?;
    Ok(checkpoint
        .filter(|checkpoint| checkpoint.source_id == source && checkpoint.observed_fence == fence)
        .map_or(0, |checkpoint| checkpoint.next_offset))
}

async fn scan_assignments(
    store: &Store,
    kind: DerivedConsumerKind,
    cursor: Option<&DefinitionAssignmentCursor>,
) -> Result<keldra_store::DefinitionAssignmentPage, Status> {
    let store = store.clone();
    let cursor = cursor.cloned();
    tokio::task::spawn_blocking(move || {
        store.scan_definition_assignments_by_kind(
            definition_kind(kind),
            cursor.as_ref(),
            MAX_DEFINITION_STATE_SCAN_RECORDS,
        )
    })
    .await
    .map_err(join_status)?
    .map_err(internal_status)
}

fn require_compatible(from: &IndexBarrier, target: &IndexBarrier) -> Result<(), Status> {
    if from.fence != target.fence
        || from.sources.len() != target.sources.len()
        || from.sources.iter().any(|(node, cursor)| {
            target.sources.get(node).is_none_or(|target| {
                cursor.source != target.source || cursor.next_offset > target.next_offset
            })
        })
    {
        return Err(Status::unavailable("derived source vector changed"));
    }
    Ok(())
}

fn require_fence(decisions: &DecisionRaft, expected: PlacementLogId) -> Result<(), Status> {
    if current_placement(decisions)?.fence() == expected {
        Ok(())
    } else {
        Err(Status::unavailable("derived membership fence changed"))
    }
}

const fn definition_kind(kind: DerivedConsumerKind) -> DefinitionKind {
    match kind {
        DerivedConsumerKind::Index => DefinitionKind::Index,
        DerivedConsumerKind::Accounting => DefinitionKind::Accounting,
    }
}

const fn assignment_kind(kind: DerivedConsumerKind) -> DefinitionConsumerKind {
    match kind {
        DerivedConsumerKind::Index => DefinitionConsumerKind::IndexAssignments,
        DerivedConsumerKind::Accounting => DefinitionConsumerKind::AccountingAssignments,
    }
}

fn event_status(error: impl std::fmt::Display) -> Status {
    Status::unavailable(error.to_string())
}

fn tracker_status(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(error.to_string())
}

fn join_status(error: tokio::task::JoinError) -> Status {
    Status::internal(format!("derived consumer task failed: {error}"))
}

fn internal_status(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_demux_has_explicit_empty_routed_and_raw_paths() {
        assert_eq!(demux_strategy(0, 1, 1), DemuxStrategy::Empty);
        assert_eq!(demux_strategy(1, 1, 1), DemuxStrategy::Routed);
        assert_eq!(demux_strategy(1_000_000, 1, 1), DemuxStrategy::Raw);
    }

    #[test]
    fn interval_shape_counts_each_source_offset_exactly_once() {
        let fence = PlacementLogId { term: 3, index: 7 };
        let atomic = crate::index_runtime::events::AtomicProgramWatermark::new(None, None, 0);
        let source = |node: u16| SourceId {
            node_id: node,
            source_epoch: [node as u8; 32],
        };
        let from = IndexBarrier {
            fence,
            atomic,
            sources: BTreeMap::from([
                (
                    NodeId(1),
                    crate::index_runtime::events::IndexSourceCursor {
                        source: source(1),
                        next_offset: 10,
                    },
                ),
                (
                    NodeId(2),
                    crate::index_runtime::events::IndexSourceCursor {
                        source: source(2),
                        next_offset: 20,
                    },
                ),
            ]),
        };
        let target = IndexBarrier {
            sources: BTreeMap::from([
                (
                    NodeId(1),
                    crate::index_runtime::events::IndexSourceCursor {
                        source: source(1),
                        next_offset: 12,
                    },
                ),
                (
                    NodeId(2),
                    crate::index_runtime::events::IndexSourceCursor {
                        source: source(2),
                        next_offset: 23,
                    },
                ),
            ]),
            ..from.clone()
        };

        assert_eq!(interval_shape(&from, &target).unwrap(), (5, 2));
        assert_eq!(demux_strategy(2, 5, 2), DemuxStrategy::Routed);
        assert_eq!(demux_strategy(3, 5, 2), DemuxStrategy::Raw);
    }

    #[test]
    fn only_published_source_complete_evidence_suppresses_a_builder_wake() {
        let source = SourceId {
            node_id: 1,
            source_epoch: [4; 32],
        };
        let effects = BTreeMap::from([(
            source,
            RoutedSourceEffect {
                next_offset: 12,
                required_atomic_cursor: None,
                atomic_hold_next: None,
            },
        )]);
        let evidence = DerivedBarrierEvidence::Published(IndexBarrier {
            fence: PlacementLogId { term: 3, index: 7 },
            atomic: crate::index_runtime::events::AtomicProgramWatermark::new(None, None, 0),
            sources: BTreeMap::from([(
                NodeId(1),
                crate::index_runtime::events::IndexSourceCursor {
                    source,
                    next_offset: 12,
                },
            )]),
        });

        assert!(evidence_covers_effects(Some(&evidence), &effects));
        assert!(!evidence_covers_effects(None, &effects));
        assert!(!evidence_covers_effects(
            Some(&evidence),
            &BTreeMap::from([(
                source,
                RoutedSourceEffect {
                    next_offset: 13,
                    required_atomic_cursor: None,
                    atomic_hold_next: None,
                },
            )])
        ));
    }

    #[test]
    fn raw_demux_uses_each_projection_source_predicate() {
        let change = |path: &str| {
            LocalChange::ObjectHead(keldra_store::ObjectHeadChange {
                offset: 8,
                tenant_id: 1,
                bucket_id: 2,
                exact_path: path.into(),
                canonical_path: None,
                path_version: keldra_store::VersionId(9),
                kind: keldra_store::ObjectHeadChangeKind::Put,
                program_commit_cursor: None,
                reference_deltas: Vec::new(),
                definition_transition: None,
                accounting_transition: None,
            })
        };

        assert_eq!(
            change_buckets(
                DerivedConsumerKind::Index,
                &change("objects/_keldra/index-pack")
            ),
            Vec::<(u64, u64)>::new()
        );
        assert_eq!(
            change_buckets(
                DerivedConsumerKind::Accounting,
                &change("_keldra/accounting/7/current")
            ),
            Vec::<(u64, u64)>::new()
        );
        assert_eq!(
            change_buckets(
                DerivedConsumerKind::Accounting,
                &change("_keldra/accounting/7/sources/3")
            ),
            vec![(1, 2)]
        );
        assert_eq!(
            change_buckets(
                DerivedConsumerKind::Accounting,
                &change("customer/ordinary.json")
            ),
            vec![(1, 2)]
        );
    }
}
