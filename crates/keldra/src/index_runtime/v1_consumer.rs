//! Sole ordered producer for format-v1 physical projection partitions.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use keldra_consensus::{DecisionRaft, NodeId};
use keldra_index::v1::{
    IndexingMemoryCredits, IndexingMemoryPermit, IndexingMemoryStage, IndexingProgressReservation,
    MemoryAdmission, PartitionProjectionAccumulator, PreparedProjectionBatchReservation,
    PreparedProjectionRow, PreparedQueryMutationBatch, ProjectionBatchAdmission,
    ProjectionPackCredits, ProjectionPartitionIdentity, QueryBlockCredits,
    bind_realtime_source_position,
};
use keldra_store::{ObjectHeadChange, ObjectHeadChangeKind, SourceId, Store, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_placement::ClusterPlacement;
use crate::index_config::IndexRuntimeConfig;

use super::catalog::{IndexCatalog, PhysicalCatalogRecipe};
use super::events::{IndexBarrier, IndexEventJournal, MAX_INDEX_EVENT_PAGE_BYTES};
use super::hot_ingress::HotProjectionIngress;
use super::source::{IndexBuildObject, IndexSourceMutation};
use super::v1_backfill::open_partition_baseline;
use super::v1_extractor::{
    SelectedV1Source, V1PreparationSlot, V1ProjectionExtractor, matching_recipes,
};
use super::v1_journal_dispatch::{V1OrderedSourceDispatcher, V1SourceDispatch};
use super::v1_mutation_window::{
    MAX_ADVANCE_SLICE_BYTES, bounded_advance_operations, coalesce_latest_by_source_path,
    journal_read_ahead_pages, publication_chunks,
};
use super::v1_parallel::{partition_lane_parallelism, run_bounded_ordered};
use super::v1_producer_state::{
    PartitionEvidence, PartitionEvidenceMap, ProducerStage, contain_integrity_failure,
};
use super::v1_publication::{
    LoadedV1ProjectionGeneration, PendingV1Publication, V1PostCasVerification,
    V1ProjectionPublisher, V1PublicationPredecessor, finish_required_post_cas_verification,
};
use super::v1_source_batch::{ExactMutationRequest, load_exact_mutations};

#[path = "v1_consumer_atomic_progress.rs"]
mod atomic_progress;
#[path = "v1_consumer_compaction.rs"]
mod compaction;
#[path = "v1_consumer_look_ahead.rs"]
mod look_ahead;
#[path = "v1_consumer_mutation_buffer.rs"]
mod mutation_buffer;
#[path = "v1_consumer_outcomes.rs"]
mod outcome_handling;
#[path = "v1_consumer_prepare.rs"]
mod prepare;
#[path = "v1_realtime_consumer.rs"]
mod realtime;
#[path = "v1_consumer_sealing.rs"]
mod sealing;
#[path = "v1_consumer_skipped_progress.rs"]
mod skipped_progress;
use compaction::{
    BackgroundCompaction, ensure_background_compaction, publish_finished_background_compaction,
    should_harvest_background_compaction, take_finished_background_compaction,
};
use look_ahead::{LookAheadContext, LookAheadTask, finish_look_ahead, start_look_ahead};
#[cfg(test)]
use mutation_buffer::queue_mutation_window;
use mutation_buffer::{mutation_window_needed, prepare_dispatches, prepare_page, queue_mutations};
#[cfg(test)]
use prepare::{
    acquire_preparation_construction, preparation_chunk_size, preparation_refill_size,
    selected_mutation_resident_bytes,
};
use prepare::{apply_rows, prepare_lane};
use sealing::{reserve_sealing_progress, spine_preload_bound};
#[cfg(test)]
pub(super) use skipped_progress::skipped_interval_has_external_change;
use skipped_progress::{acknowledge_external_skipped_positions, owned_source_scan_start};

const RETRY: Duration = Duration::from_millis(250);
const SAFETY_RECONCILE: Duration = Duration::from_secs(30);
const STALL_AFTER: Duration = Duration::from_secs(30);
const JOURNAL_PAGE_RESIDENT_MULTIPLIER: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReconcileOutcome {
    Stable,
    RetryPartition,
}

pub(crate) struct V1IndexProducerTask {
    task: tokio::task::JoinHandle<()>,
    _realtime: realtime::RealtimeLaneTask,
}

impl Drop for V1IndexProducerTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone, Copy)]
pub(super) struct Limits {
    bytes: usize,
    flush_bytes: usize,
    projection_batch_bytes: usize,
    flush_age: Duration,
    flush_operations: u64,
    lsm_runs: u64,
    lsm_bytes: u64,
    parallelism: usize,
    worker_bytes: usize,
}

struct Writer {
    recipe: Arc<PhysicalCatalogRecipe>,
    source: SourceId,
    partition: ProjectionPartitionIdentity,
    current: Option<LoadedV1ProjectionGeneration>,
    catalog_rebuild_current_version: Option<VersionId>,
    dispatcher: Option<V1OrderedSourceDispatcher>,
    look_ahead: Option<LookAheadTask>,
    look_ahead_context: Option<LookAheadContext>,
    look_ahead_permitted: bool,
    look_ahead_progress: Option<IndexingProgressReservation>,
    look_ahead_prepared: BTreeMap<
        (u64, u32),
        (
            (Mutation, u64, Arc<IndexingMemoryPermit>),
            V1PreparationSlot,
        ),
    >,
    scanned: IndexBarrier,
    accumulator: PartitionProjectionAccumulator,
    baseline: Option<super::v1_backfill::V1PartitionBaseline>,
    query: PreparedQueryMutationBatch,
    query_credits: QueryBlockCredits,
    /// Per-document preparation credits retained until the merged query input
    /// has been consumed into immutable artifacts.
    query_input_credits: Vec<QueryBlockCredits>,
    sealing_progress: Option<IndexingProgressReservation>,
    since: Option<Instant>,
    source_bytes: u64,
    pending_prepared_rows: u64,
    pending_prepared_bytes: u64,
    pending_projected_rows: u64,
    pending_projected_encoded_bytes: u64,
    through_atomic: u64,
    atomic_replay_target: Option<(IndexBarrier, IndexingMemoryPermit)>,
    pending_mutations: BTreeMap<String, Mutation>,
    pending_mutation_bytes: usize,
    pending_operations: u64,
    pending_next: u64,
    skipped_proof_next: u64,
    pending_skipped_ack: bool,
    pending_mutation_capacity: usize,
    pending_mutation_permit: IndexingMemoryPermit,
    background_compaction: Option<BackgroundCompaction>,
    post_cas_verification: Option<V1PostCasVerification>,
    pending_publication: Option<PendingV1Publication>,
    halted_on_integrity_failure: bool,
    stage: ProducerStage,
}

#[derive(Clone, Debug)]
pub(super) struct Mutation {
    offset: u64,
    ordinal: u32,
    tenant_id: u64,
    bucket_id: u64,
    path: String,
    canonical_path: Option<String>,
    version: u64,
    deleted: bool,
    /// Finalized atomic-batch cursor. Every source-local mutation carrying the
    /// same cursor is one indivisible publication unit even when its source
    /// journal positions differ.
    atomic_group: Option<u64>,
    /// The first journal transition for this path after durable Current proves
    /// that no live predecessor existed. This evidence survives newest-wins
    /// coalescing across the complete unpublished window.
    predecessor_absent_at_window_start: bool,
}

struct SelectedMutation {
    mutation: Mutation,
    selected: SelectedV1Source,
    source_bytes: u64,
    _input: keldra_index::v1::IndexingMemoryPermit,
}

impl V1IndexProducerTask {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        local_node: NodeId,
        decisions: DecisionRaft,
        catalog: IndexCatalog,
        journal: Arc<IndexEventJournal>,
        store: Store,
        scanner: super::scanner::ClusterIndexScanner,
        reader: ClusterObjectReader,
        cpu: super::cpu::IndexCpuPool,
        hot: HotProjectionIngress,
        publisher: V1ProjectionPublisher,
        config: IndexRuntimeConfig,
        credits: IndexingMemoryCredits,
    ) -> Result<Self, Status> {
        let limits = limits(config)?;
        let mut catalog_changes = catalog.subscribe();
        let mut publication_changes = publisher.subscribe();
        let mut journal_changes = hot.subscribe();
        let compaction_cpu = cpu.clone();
        let realtime_cpu = cpu.clone();
        let extractor = V1ProjectionExtractor::new(reader.clone(), cpu, hot, limits.worker_bytes);
        let realtime = realtime::RealtimeLaneTask::start(
            local_node,
            decisions.clone(),
            catalog.clone(),
            store,
            reader.clone(),
            extractor.clone(),
            publisher.clone(),
            credits.clone(),
            limits,
            realtime_cpu,
        );
        let lag_observation_epoch = Arc::new(AtomicU64::new(0));
        let lag_observation_active = Arc::new(AtomicBool::new(false));
        let compaction_ready = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn(async move {
            let mut writers = BTreeMap::new();
            let mut evidence = PartitionEvidenceMap::new();
            loop {
                let result = reconcile(
                    local_node,
                    &decisions,
                    &catalog,
                    &journal,
                    &scanner,
                    &reader,
                    &extractor,
                    &publisher,
                    &compaction_cpu,
                    &credits,
                    limits,
                    &lag_observation_epoch,
                    &lag_observation_active,
                    &compaction_ready,
                    &mut writers,
                    &mut evidence,
                )
                .await;
                match result {
                    Err(error) => {
                        writers.clear();
                        tracing::warn!(%error, "v1 producer will replay from Current");
                        tokio::time::sleep(RETRY).await;
                    }
                    Ok(ReconcileOutcome::RetryPartition) => {
                        // Retry only the failed partition from durable Current.
                        tokio::time::sleep(RETRY).await;
                    }
                    Ok(ReconcileOutcome::Stable) => {
                        let deadline = next_reconcile_delay(&writers, limits);
                        tokio::select! {
                            _ = wait_for_physical_catalog_change(&mut catalog_changes) => {}
                            _ = journal_changes.recv() => {}
                            _ = publication_changes.recv() => {}
                            () = compaction_ready.notified() => {}
                            () = tokio::time::sleep(deadline) => {}
                        }
                    }
                }
            }
        });
        Ok(Self {
            task,
            _realtime: realtime,
        })
    }
}

async fn wait_for_physical_catalog_change(
    changes: &mut tokio::sync::broadcast::Receiver<super::catalog::CatalogNotice>,
) {
    loop {
        match changes.recv().await {
            Ok(notice) if notice.physical_changed => return,
            Ok(_) => {}
            Err(_) => return,
        }
    }
}

fn next_reconcile_delay(
    writers: &BTreeMap<ProjectionPartitionIdentity, Writer>,
    limits: Limits,
) -> Duration {
    writers
        .values()
        .filter(|writer| !writer.halted_on_integrity_failure)
        .filter_map(|writer| writer.since)
        .map(|since| limits.flush_age.saturating_sub(since.elapsed()))
        .min()
        .unwrap_or(SAFETY_RECONCILE)
        .min(SAFETY_RECONCILE)
}

fn limits(config: IndexRuntimeConfig) -> Result<Limits, Status> {
    // Every retained stage uses the same accounted pipeline credits; unused
    // catalog/hot capacity is available to journal preparation and sealing.
    let configured = usize::try_from(config.pipeline_memory_bytes())
        .map_err(|_| Status::invalid_argument("v1 pipeline memory exceeds this platform"))?;
    let bytes = configured.max(1);
    let flush_bytes = usize::try_from(config.flush_bytes())
        .map_err(|_| Status::invalid_argument("v1 flush bytes exceed this platform"))?
        .min(MAX_ADVANCE_SLICE_BYTES)
        .min(bytes.saturating_div(4).max(1));
    // Projection output may legitimately exceed its bounded journal input.
    // Reserve a bounded producer share rather than treating flush as an output cap.
    let projection_batch_bytes = bytes.saturating_div(4).max(flush_bytes);
    let parallelism = usize::try_from(config.indexing_cores())
        .map_err(|_| Status::invalid_argument("v1 indexing cores exceed this platform"))?;
    if parallelism == 0 {
        return Err(Status::invalid_argument(
            "v1 indexing cores must be positive",
        ));
    }
    Ok(Limits {
        bytes,
        flush_bytes,
        projection_batch_bytes,
        flush_age: config.flush_max_age(),
        // The charged mutation window remains the memory authority. A hard
        // operation ceiling additionally bounds visibility latency under a
        // sustained backlog; atomic source units remain indivisible.
        flush_operations: bounded_advance_operations(config.flush_max_operations()),
        lsm_runs: u64::from(config.lsm_max_runs_per_level()),
        lsm_bytes: config.lsm_max_unmerged_bytes_per_level(),
        parallelism,
        worker_bytes: flush_bytes.saturating_div(parallelism).max(1),
    })
}

#[allow(clippy::too_many_arguments)]
async fn reconcile(
    local_node: NodeId,
    decisions: &DecisionRaft,
    catalog: &IndexCatalog,
    journal: &Arc<IndexEventJournal>,
    scanner: &super::scanner::ClusterIndexScanner,
    reader: &ClusterObjectReader,
    extractor: &V1ProjectionExtractor,
    publisher: &V1ProjectionPublisher,
    compaction_cpu: &super::cpu::IndexCpuPool,
    credits: &IndexingMemoryCredits,
    limits: Limits,
    lag_observation_epoch: &Arc<AtomicU64>,
    lag_observation_active: &Arc<AtomicBool>,
    compaction_ready: &Arc<tokio::sync::Notify>,
    writers: &mut BTreeMap<ProjectionPartitionIdentity, Writer>,
    evidence: &mut PartitionEvidenceMap,
) -> Result<ReconcileOutcome, Status> {
    let placement = current_placement(decisions)?;
    let target = journal.capture_barrier().await.map_err(event_status)?;
    if placement.fence() != target.fence {
        return Err(Status::unavailable("v1 placement changed"));
    }
    let catalog_snapshot = catalog.physical_snapshot()?;
    let mut assigned = BTreeSet::new();
    // First capture the complete stable assignment set. `IndexCatalog` already
    // interns recipes by physical family, so logical aliases never repeat one
    // partition's source work.
    for recipe in catalog_snapshot.recipes.iter() {
        let Some((directory, _)) = publisher
            .load_family_directory(
                &recipe.storage_tenant,
                &recipe.bucket,
                recipe.family.tenant_id,
                recipe.family.bucket_id,
                recipe.family.family_id,
            )
            .await?
        else {
            continue;
        };
        for entry in directory.entries {
            let partition = entry.partition;
            if partition.producer_node != local_node.0
                || partition.placement_term != target.fence.term
                || partition.placement_index != target.fence.index
            {
                continue;
            }
            assigned.insert(partition);
            let generation_changed = writers.get(&partition).is_some_and(|writer| {
                writer.recipe.physical_generation != recipe.physical_generation
            }) || evidence
                .get(&partition)
                .is_some_and(|state| state.physical_generation != recipe.physical_generation);
            if generation_changed {
                // A physical generation is a clean rebuild boundary.
                writers.remove(&partition);
                evidence.remove(&partition);
            }
            if !writers.contains_key(&partition)
                && !evidence.get(&partition).is_some_and(|state| state.halted)
            {
                let writer = open_writer(
                    recipe.clone(),
                    partition,
                    &target,
                    journal,
                    publisher,
                    credits,
                    limits,
                )
                .await?;
                let published_next = writer
                    .current
                    .as_ref()
                    .map_or(0, |current| current.current.next_offset);
                match evidence.entry(partition) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(PartitionEvidence::opened(
                            writer.source,
                            writer.recipe.family.family_id,
                            writer.recipe.physical_generation,
                            published_next,
                        ));
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().reopened(
                            writer.source,
                            writer.recipe.family.family_id,
                            writer.recipe.physical_generation,
                            published_next,
                        );
                    }
                }
                writers.insert(partition, writer);
            }
        }
    }
    writers.retain(|partition, _| assigned.contains(partition));
    evidence.retain(|partition, _| assigned.contains(partition));
    let lag_requests = writers
        .iter()
        .map(|(partition, writer)| {
            (
                *partition,
                writer.recipe.family.tenant_id,
                writer.recipe.family.bucket_id,
                writer.source,
                writer
                    .current
                    .as_ref()
                    .map_or(0, |current| current.current.next_offset),
            )
        })
        .collect::<Vec<_>>();
    spawn_lag_observation(
        Arc::clone(journal),
        publisher.clone(),
        target.clone(),
        lag_requests,
        limits.parallelism,
        Arc::clone(lag_observation_epoch),
        Arc::clone(lag_observation_active),
    );
    let lag_targets = writers
        .iter()
        .map(|(partition, writer)| {
            let published_next = writer
                .current
                .as_ref()
                .map_or(0, |current| current.current.next_offset);
            (
                *partition,
                publisher
                    .observed_source_next(*partition)
                    .unwrap_or(published_next),
            )
        })
        .collect::<BTreeMap<_, _>>();
    record_lag_telemetry(&lag_targets, writers, evidence);
    let physical_catalog_identity = catalog_snapshot.identity;
    let runnable = assigned
        .into_iter()
        .filter(|partition| runnable_partition(writers, partition))
        .collect::<Vec<_>>();
    let active_writers = runnable.len().min(limits.parallelism.max(1));
    // Each task owns one Writer, preserving its generation/CAS order.
    let work = runnable
        .into_iter()
        .enumerate()
        .map(|(ordinal, partition)| {
            let writer = writers
                .remove(&partition)
                .expect("assigned v1 writer was opened");
            let mut writer_limits = limits;
            writer_limits.parallelism =
                partition_lane_parallelism(limits.parallelism, active_writers, ordinal);
            (partition, (writer, writer_limits))
        })
        .collect();
    let outcomes = run_partition_advances(work, active_writers, {
        let target = target.clone();
        let journal = Arc::clone(journal);
        let scanner = scanner.clone();
        let reader = reader.clone();
        let extractor = extractor.clone();
        let publisher = publisher.clone();
        let compaction_cpu = compaction_cpu.clone();
        let credits = credits.clone();
        let compaction_ready = Arc::clone(compaction_ready);
        move |(mut writer, writer_limits)| {
            let target = target.clone();
            let journal = Arc::clone(&journal);
            let scanner = scanner.clone();
            let reader = reader.clone();
            let extractor = extractor.clone();
            let publisher = publisher.clone();
            let compaction_cpu = compaction_cpu.clone();
            let credits = credits.clone();
            let compaction_ready = Arc::clone(&compaction_ready);
            async move {
                let _advance = super::v1_telemetry::global().begin_in_flight_advance(STALL_AFTER);
                let before = writer_dispatch_progress(&writer);
                let result = advance(
                    &mut writer,
                    &target,
                    physical_catalog_identity,
                    &journal,
                    &scanner,
                    &reader,
                    &extractor,
                    &publisher,
                    &compaction_cpu,
                    &credits,
                    &compaction_ready,
                    writer_limits,
                )
                .await;
                (writer, writer_limits, before, result)
            }
        }
    })
    .await?;
    let mut partition_failed = false;
    for (partition, (writer, result)) in outcomes {
        partition_failed |= outcome_handling::record_partition_outcome(
            partition, writer, result, writers, evidence,
        );
    }
    record_lag_telemetry(&lag_targets, writers, evidence);
    Ok(if partition_failed {
        ReconcileOutcome::RetryPartition
    } else {
        ReconcileOutcome::Stable
    })
}

type PartitionAdvanceOutcome = (Writer, Limits, u64, Result<(), Status>);

async fn run_partition_advances<F, Fut>(
    work: Vec<(ProjectionPartitionIdentity, (Writer, Limits))>,
    maximum_parallelism: usize,
    operation: F,
) -> Result<Vec<(ProjectionPartitionIdentity, (Writer, Result<(), Status>))>, Status>
where
    F: Fn((Writer, Limits)) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = PartitionAdvanceOutcome> + Send + 'static,
{
    let started = Instant::now();
    let mut pending = VecDeque::from(work);
    let mut active = tokio::task::JoinSet::new();
    let mut output = Vec::new();
    let maximum_parallelism = maximum_parallelism.max(1);
    let mut rescheduled = 0_u64;
    let mut completed_advances = 0_u64;
    let mut peak_in_flight = 0_usize;

    loop {
        while active.len() < maximum_parallelism {
            let Some((partition, input)) = pending.pop_front() else {
                break;
            };
            let operation = operation.clone();
            active.spawn(async move { (partition, operation(input).await) });
            peak_in_flight = peak_in_flight.max(active.len());
        }
        let Some(completed) = active.join_next().await else {
            break;
        };
        let (partition, (writer, limits, before, result)) = completed.map_err(|error| {
            Status::internal(format!("v1 partition advance task failed: {error}"))
        })?;
        completed_advances = completed_advances.saturating_add(1);
        if result.is_ok()
            && should_reschedule_after_advance(
                writer.stage,
                before,
                writer_dispatch_progress(&writer),
            )
        {
            // Requeue at the back so a partition never monopolizes a bounded
            // worker slot while peers are waiting for their first page.
            rescheduled = rescheduled.saturating_add(1);
            pending.push_back((partition, (writer, limits)));
        } else {
            output.push((partition, (writer, result)));
        }
    }
    output.sort_unstable_by_key(|(partition, _)| *partition);
    tracing::debug!(
        histogram.keldra_index_v1_scheduler_duration_seconds = started.elapsed().as_secs_f64(),
        counter.keldra_index_v1_partition_advances = completed_advances,
        counter.keldra_index_v1_partition_reschedules = rescheduled,
        gauge.keldra_index_v1_partition_advances_peak_in_flight = peak_in_flight,
        "v1 producer scheduler completed a bounded target"
    );
    Ok(output)
}

fn should_reschedule_after_advance(stage: ProducerStage, before: u64, after: u64) -> bool {
    after > before && matches!(stage, ProducerStage::Backfill | ProducerStage::JournalScan)
}

type LagObservationRequest = (ProjectionPartitionIdentity, u64, u64, SourceId, u64);

fn spawn_lag_observation(
    journal: Arc<IndexEventJournal>,
    publisher: V1ProjectionPublisher,
    target: IndexBarrier,
    requests: Vec<LagObservationRequest>,
    maximum_parallelism: usize,
    epoch: Arc<AtomicU64>,
    active: Arc<AtomicBool>,
) {
    if active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    let observation_epoch = epoch.fetch_add(1, Ordering::AcqRel).saturating_add(1);
    tokio::spawn(async move {
        let started = Instant::now();
        let requested_partitions = requests.len();
        struct ActiveGuard(Arc<AtomicBool>);
        impl Drop for ActiveGuard {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _active = ActiveGuard(active);
        let work = requests
            .into_iter()
            .map(
                |(partition, tenant_id, bucket_id, source, published_next)| {
                    (partition, (tenant_id, bucket_id, source, published_next))
                },
            )
            .collect();
        let observed = run_bounded_ordered(work, maximum_parallelism, {
            let journal = Arc::clone(&journal);
            move |(tenant_id, bucket_id, source, published_next)| {
                let journal = Arc::clone(&journal);
                let target = target.clone();
                async move {
                    journal
                        .routed_index_source_next(
                            tenant_id,
                            bucket_id,
                            source,
                            routed_lag_start(published_next),
                            &target,
                        )
                        .await
                        .map_err(event_status)
                }
            }
        })
        .await;
        match observed {
            Ok(observed) if lag_observation_is_current(&epoch, observation_epoch) => {
                let successful_partitions =
                    observed.iter().filter(|(_, result)| result.is_ok()).count();
                let observations = observed
                    .into_iter()
                    .filter_map(|(partition, result)| result.ok().map(|next| (partition, next)))
                    .collect();
                publisher.replace_observed_source_next(&observations);
                tracing::debug!(
                    histogram.keldra_index_v1_lag_scan_duration_seconds =
                        started.elapsed().as_secs_f64(),
                    counter.keldra_index_v1_lag_scan_partitions = requested_partitions,
                    counter.keldra_index_v1_lag_scan_successes = successful_partitions,
                    counter.keldra_index_v1_lag_scan_failures =
                        requested_partitions.saturating_sub(successful_partitions),
                    "v1 routed lag observation completed off the producer critical path"
                );
            }
            Ok(_) => {
                tracing::debug!(
                    histogram.keldra_index_v1_lag_scan_duration_seconds =
                        started.elapsed().as_secs_f64(),
                    counter.keldra_index_v1_lag_scan_stale_results = requested_partitions,
                    "v1 routed lag observation discarded a stale result"
                );
            }
            Err(error) => {
                tracing::debug!(%error, "v1 routed lag observation task failed");
            }
        }
    });
}

fn lag_observation_is_current(epoch: &AtomicU64, observation_epoch: u64) -> bool {
    epoch.load(Ordering::Acquire) == observation_epoch
}

fn runnable_partition(
    writers: &BTreeMap<ProjectionPartitionIdentity, Writer>,
    partition: &ProjectionPartitionIdentity,
) -> bool {
    writers
        .get(partition)
        .is_some_and(|writer| writer_state_is_runnable(writer.halted_on_integrity_failure))
}

fn writer_state_is_runnable(halted_on_integrity_failure: bool) -> bool {
    !halted_on_integrity_failure
}

fn halts_partition(error: &Status) -> bool {
    error.code() == tonic::Code::DataLoss
}

fn record_lag_telemetry(
    lag_targets: &BTreeMap<ProjectionPartitionIdentity, u64>,
    writers: &BTreeMap<ProjectionPartitionIdentity, Writer>,
    evidence: &mut PartitionEvidenceMap,
) {
    let now = Instant::now();
    let mut local_next = u64::MAX;
    let mut local_tail = 0_u64;
    let mut lag_entries = 0_u64;
    for (partition, writer) in writers {
        let published_next = writer
            .current
            .as_ref()
            .map_or(0, |current| current.current.next_offset);
        evidence
            .entry(*partition)
            .or_insert_with(|| {
                PartitionEvidence::opened(
                    writer.source,
                    writer.recipe.family.family_id,
                    writer.recipe.physical_generation,
                    published_next,
                )
            })
            .observe_progress(published_next, writer_processed_next(writer), writer.stage);
    }
    let mut oldest_age = 0_u64;
    let mut oldest_no_progress_age = 0_u64;
    let mut stalled_partitions = 0_u64;
    let mut halted_partitions = 0_u64;
    let mut retrying_partitions = 0_u64;
    for (partition, state) in evidence.iter_mut() {
        let Some(&indexable_next) = lag_targets.get(partition) else {
            continue;
        };
        let writer = writers.get(partition);
        let scanned_next = writer
            .and_then(|writer| {
                writer
                    .scanned
                    .sources
                    .get(&NodeId(u64::from(writer.source.node_id)))
                    .filter(|cursor| cursor.source == writer.source)
                    .map(|cursor| cursor.next_offset)
            })
            .unwrap_or(state.published_next);
        let processed_next = writer.map_or(state.processed_next, writer_processed_next);
        let published_next = state.published_next;
        let stage = state.stage;
        state.observe_progress(published_next, processed_next, stage);
        let has_unpublished_projection_work = writer.is_some_and(|writer| {
            writer.pending_prepared_rows > 0
                || writer.pending_skipped_ack
                || !writer.pending_mutations.is_empty()
        });
        let processing_is_behind = state.processed_next < indexable_next;
        let lag = state.observe_lag(
            indexable_next,
            processing_is_behind,
            has_unpublished_projection_work,
            STALL_AFTER,
            now,
        );
        tracing::debug!(
            target: "keldra::index_runtime::v1_consumer_state",
            ?partition,
            family_id = ?state.family_id,
            producer_stage = state.stage.label(),
            published_next_offset = state.published_next,
            processed_next_offset = state.processed_next,
            scanned_next_offset = scanned_next,
            pending_next_offset = writer.map_or(state.published_next, |writer| writer.pending_next),
            accumulator_next_offset = writer.map_or(state.published_next, |writer| writer.accumulator.next_offset()),
            observed_next_offset = indexable_next,
            pending_mutations = writer.map_or(0, |writer| writer.pending_mutations.len()),
            pending_mutation_bytes = writer.map_or(0, |writer| writer.pending_mutation_bytes),
            pending_operations = writer.map_or(0, |writer| writer.pending_operations),
            pending_prepared_rows = writer.map_or(0, |writer| writer.pending_prepared_rows),
            pending_age_milliseconds = writer
                .and_then(|writer| writer.since)
                .map_or(0_u64, |since| since.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
            last_progress_age_milliseconds = lag.no_progress_milliseconds,
            identical_retries = state.identical_retries,
            last_error = state.last_error_message.as_deref().unwrap_or(""),
            halted = state.halted,
            "v1 projection partition state"
        );
        local_next = local_next.min(state.published_next);
        local_tail = local_tail.max(indexable_next.saturating_sub(1));
        lag_entries = lag_entries.saturating_add(lag.entries);
        if lag.entries > 0 {
            let lag_started = state
                .lag_started_at
                .expect("lagging v1 partition has a start instant");
            oldest_age = oldest_age.max(
                now.saturating_duration_since(lag_started)
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
            );
            if state.halted || processing_is_behind || has_unpublished_projection_work {
                oldest_no_progress_age = oldest_no_progress_age.max(lag.no_progress_milliseconds);
            }
            if lag.stalled {
                stalled_partitions = stalled_partitions.saturating_add(1);
            }
        }
        halted_partitions = halted_partitions.saturating_add(u64::from(state.halted));
        retrying_partitions =
            retrying_partitions.saturating_add(u64::from(state.stage == ProducerStage::Backoff));
    }
    let telemetry = super::v1_telemetry::global();
    super::v1_telemetry::V1PipelineTelemetry::set(
        &telemetry.local_next_offset,
        if local_next == u64::MAX {
            0
        } else {
            local_next
        },
    );
    super::v1_telemetry::V1PipelineTelemetry::set(&telemetry.local_tail, local_tail);
    super::v1_telemetry::V1PipelineTelemetry::set(&telemetry.lag_entries, lag_entries);
    super::v1_telemetry::V1PipelineTelemetry::set(&telemetry.lag_oldest_age_millis, oldest_age);
    super::v1_telemetry::V1PipelineTelemetry::set(
        &telemetry.oldest_no_progress_age_millis,
        oldest_no_progress_age,
    );
    super::v1_telemetry::V1PipelineTelemetry::set(
        &telemetry.stalled_partitions,
        stalled_partitions,
    );
    super::v1_telemetry::V1PipelineTelemetry::set(
        &telemetry.retrying_partitions,
        retrying_partitions,
    );
    super::v1_telemetry::V1PipelineTelemetry::set(&telemetry.halted_partitions, halted_partitions);
}

fn writer_dispatch_progress(writer: &Writer) -> u64 {
    writer
        .scanned
        .sources
        .values()
        .fold(writer_processed_next(writer), |progress, cursor| {
            progress.saturating_add(cursor.next_offset)
        })
}

fn writer_processed_next(writer: &Writer) -> u64 {
    let scanned_next = writer
        .scanned
        .sources
        .get(&NodeId(u64::from(writer.source.node_id)))
        .filter(|cursor| cursor.source == writer.source)
        .map_or(0, |cursor| cursor.next_offset);
    scanned_next
        .max(writer.pending_next)
        .max(writer.accumulator.next_offset())
}

fn routed_lag_start(published_next: u64) -> u64 {
    // Journal offset zero is the fresh-partition sentinel. The first real
    // source record is offset one, represented by next-offset one before it is
    // consumed. Keep the sentinel as one unit of initial-build work while
    // still routing every real source position.
    published_next.max(1)
}

async fn open_writer(
    recipe: Arc<PhysicalCatalogRecipe>,
    partition: ProjectionPartitionIdentity,
    target: &IndexBarrier,
    journal: &IndexEventJournal,
    publisher: &V1ProjectionPublisher,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<Writer, Status> {
    let source = SourceId {
        node_id: u16::try_from(partition.source_node)
            .map_err(|_| Status::data_loss("v1 source node exceeds SourceId"))?,
        source_epoch: partition.source_epoch,
    };
    let loaded_current = publisher
        .load_current(
            &recipe.storage_tenant,
            &recipe.bucket,
            recipe.family.tenant_id,
            recipe.family.bucket_id,
            partition,
        )
        .await?;
    let (current, catalog_rebuild_current_version) =
        current_for_catalog(loaded_current, recipe.physical_generation);
    let durable_start = current
        .as_ref()
        .map_or(1, |loaded| loaded.current.next_offset);
    let node = NodeId(u64::from(source.node_id));
    let cursor = target
        .sources
        .get(&node)
        .filter(|cursor| cursor.source == source)
        .ok_or_else(|| Status::unavailable("assigned v1 source is absent"))?;
    if durable_start > cursor.next_offset {
        return Err(Status::data_loss("v1 Current is ahead of source"));
    }
    let control_bytes = limits.bytes.saturating_div(16).max(512);
    let control = credits
        .acquire(IndexingMemoryStage::ReplayInput, 1)
        .map_err(|_| Status::resource_exhausted("v1 control memory unavailable"))?;
    let dispatcher = V1OrderedSourceDispatcher::new(
        target.fence,
        BTreeSet::from([source]),
        control,
        control_bytes,
    );
    let replay_metadata_bytes = target
        .sources
        .len()
        .checked_mul(512 + std::mem::size_of::<super::events::IndexSourceCursor>())
        .and_then(|sources| sources.checked_add(std::mem::size_of::<IndexBarrier>()))
        .and_then(|barrier| barrier.checked_mul(4))
        .ok_or_else(|| Status::resource_exhausted("v1 replay metadata memory overflow"))?;
    let replay_metadata = credits
        .acquire(IndexingMemoryStage::ReplayInput, replay_metadata_bytes)
        .map_err(|_| Status::resource_exhausted("v1 replay metadata memory unavailable"))?;
    let retained_start = journal
        .retained_replay_start(target)
        .await
        .map_err(event_status)?;
    let scanned = owned_source_scan_start(
        target,
        &retained_start,
        source,
        durable_start,
        current.is_some(),
    )?;
    drop(retained_start);
    drop(replay_metadata);
    // Offset zero is the journal sentinel. A fresh empty partition publishes
    // the query-ready no-op range [0, 1), giving activation a real Current
    // without claiming any retained source mutation.
    let accumulator_start = if current.is_some() { durable_start } else { 0 };
    let accumulator = PartitionProjectionAccumulator::new(
        source_scope(source),
        partition,
        accumulator_start,
        limits.projection_batch_bytes,
        credits.clone(),
    )
    .map_err(index_status)?;
    let query_credits = empty_query_credits(credits, limits)?;
    let through_atomic = current.as_ref().map_or_else(
        || target.atomic.finalized_through().unwrap_or(0),
        |loaded| loaded.current.through_atomic_position,
    );
    let pending_mutation_capacity = limits.flush_bytes.max(1);
    let pending_mutation_permit = credits
        .acquire(IndexingMemoryStage::ReplayInput, 1)
        .map_err(|_| Status::resource_exhausted("v1 mutation-window memory unavailable"))?;
    Ok(Writer {
        recipe,
        source,
        partition,
        current,
        catalog_rebuild_current_version,
        dispatcher: Some(dispatcher),
        look_ahead: None,
        look_ahead_context: None,
        look_ahead_permitted: false,
        look_ahead_progress: None,
        look_ahead_prepared: BTreeMap::new(),
        scanned,
        accumulator,
        baseline: None,
        query: PreparedQueryMutationBatch::default(),
        query_credits,
        query_input_credits: Vec::new(),
        sealing_progress: None,
        since: None,
        source_bytes: 0,
        pending_prepared_rows: 0,
        pending_prepared_bytes: 0,
        pending_projected_rows: 0,
        pending_projected_encoded_bytes: 0,
        through_atomic,
        atomic_replay_target: None,
        pending_mutations: BTreeMap::new(),
        pending_mutation_bytes: 0,
        pending_operations: 0,
        pending_next: accumulator_start,
        skipped_proof_next: durable_start,
        pending_skipped_ack: false,
        pending_mutation_capacity,
        pending_mutation_permit,
        background_compaction: None,
        post_cas_verification: None,
        pending_publication: None,
        halted_on_integrity_failure: false,
        stage: ProducerStage::Opening,
    })
}

fn current_for_catalog(
    current: Option<LoadedV1ProjectionGeneration>,
    physical_catalog_generation: [u8; 32],
) -> (Option<LoadedV1ProjectionGeneration>, Option<VersionId>) {
    match current {
        Some(current)
            if current.current.physical_catalog_generation == physical_catalog_generation =>
        {
            (Some(current), None)
        }
        Some(current) => (None, Some(current.current_object_version)),
        None => (None, None),
    }
}

pub(super) fn empty_query_credits(
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<QueryBlockCredits, Status> {
    let permit = credits
        .acquire(IndexingMemoryStage::OrderingCatalog, 1)
        .map_err(|_| Status::resource_exhausted("v1 query memory unavailable"))?;
    QueryBlockCredits::from_growable_pipeline_permit(permit, limits.bytes.saturating_div(4).max(1))
        .map_err(index_status)
}

async fn backfill(
    writer: &mut Writer,
    physical_catalog_identity: [u8; 32],
    scanner: &super::scanner::ClusterIndexScanner,
    reader: &ClusterObjectReader,
    extractor: &V1ProjectionExtractor,
    publisher: &V1ProjectionPublisher,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<(), Status> {
    writer.stage = ProducerStage::Backfill;
    if writer.baseline.is_none() {
        let frame_bytes = u64::try_from(limits.flush_bytes).unwrap_or(u64::MAX);
        writer.baseline = Some(
            open_partition_baseline(
                scanner,
                &writer.recipe,
                writer.source,
                writer.partition,
                frame_bytes,
            )
            .await?,
        );
    }
    let mut baseline = writer
        .baseline
        .take()
        .expect("v1 backfill cursor was initialized");
    let captured_next = baseline.captured_next_offset();
    let batch_items = usize::try_from(limits.flush_operations)
        .unwrap_or(usize::MAX)
        .min(4_096)
        .max(1);
    let selected_bytes = limits.worker_bytes;
    let batch = baseline
        .next_selected_batch(
            extractor,
            physical_catalog_identity,
            credits,
            selected_bytes,
            batch_items,
        )
        .await?;
    if !batch.is_empty() {
        let mut rows = Vec::with_capacity(batch.len());
        let mut prepared_rows = Vec::with_capacity(batch.len());
        let next = batch
            .last()
            .and_then(|item| item.baseline_offset.checked_add(1))
            .ok_or_else(|| Status::data_loss("v1 baseline offset overflow"))?;
        for item in batch {
            let (path, version) = match &item.selected.source {
                IndexSourceMutation::Upsert(object) => (object.path.clone(), object.version),
                IndexSourceMutation::Remove { .. } => {
                    return Err(Status::data_loss("v1 current baseline contains a delete"));
                }
            };
            let prepared = V1ProjectionExtractor::prepare(
                source_scope(writer.source),
                &item.selected,
                &writer.recipe,
                &mut writer.query_credits,
            )?;
            prepared_rows.push((
                path,
                version,
                item.baseline_offset,
                item.source_bytes,
                prepared,
            ));
            let _source_journal_offset = item.source_journal_offset;
        }
        reserve_sealing_progress(
            writer,
            prepared_rows
                .iter()
                .map(|(_, _, _, _, prepared)| &prepared.query),
            prepared_rows
                .iter()
                .map(|(path, _, _, _, prepared)| (path.as_str(), prepared.current.as_slice())),
            credits,
            publisher.query_block_limits(),
        )?;
        for (path, version, baseline_offset, source_bytes, prepared) in prepared_rows {
            merge_query(&mut writer.query, prepared.query)?;
            writer.source_bytes = writer.source_bytes.saturating_add(source_bytes);
            // Reading the exact journal position is intentional lineage
            // validation even though the accumulator uses dense baseline
            // offsets for this one non-journal initial build.
            rows.push(PreparedProjectionRow {
                source_offset: baseline_offset,
                mutation_ordinal: 0,
                source_path: path,
                source_version: version,
                projected_states: prepared.current,
            });
        }
        apply_rows(writer, next, rows, credits, limits)?;
        writer.baseline = Some(baseline);
        return Ok(());
    }
    apply_rows(writer, captured_next, Vec::new(), credits, limits)?;
    let node = NodeId(u64::from(writer.source.node_id));
    writer
        .scanned
        .sources
        .get_mut(&node)
        .ok_or_else(|| Status::data_loss("v1 baseline source cursor is absent"))?
        .next_offset = captured_next;
    writer.pending_next = captured_next;
    // Dense baseline offsets become a journal checkpoint only at this final
    // cut. Its first journal window can now prepare during initial staging.
    writer.look_ahead_permitted = true;
    flush(
        writer,
        physical_catalog_identity,
        reader,
        extractor,
        publisher,
        credits,
        limits,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn advance(
    writer: &mut Writer,
    target: &IndexBarrier,
    physical_catalog_identity: [u8; 32],
    journal: &Arc<IndexEventJournal>,
    scanner: &super::scanner::ClusterIndexScanner,
    reader: &ClusterObjectReader,
    extractor: &V1ProjectionExtractor,
    publisher: &V1ProjectionPublisher,
    compaction_cpu: &super::cpu::IndexCpuPool,
    credits: &IndexingMemoryCredits,
    compaction_ready: &Arc<tokio::sync::Notify>,
    limits: Limits,
) -> Result<(), Status> {
    atomic_progress::capture_replay_target(writer, target, credits)?;
    let captured_target = writer
        .atomic_replay_target
        .as_ref()
        .map(|(target, _)| target.clone());
    let target = captured_target.as_ref().unwrap_or(target);
    writer.look_ahead_context = Some(LookAheadContext {
        journal: journal.clone(),
        target: target.clone(),
    });
    // Observe a completed readback even when the partition has no next page.
    // An in-flight verifier is deliberately left alone so journal read-ahead
    // and source extraction can overlap it; the next Current CAS gates on it.
    if writer
        .post_cas_verification
        .as_ref()
        .is_some_and(V1PostCasVerification::is_finished)
    {
        finish_required_post_cas_verification(&mut writer.post_cas_verification).await?;
    }
    if writer.pending_publication.is_some() {
        writer.stage = ProducerStage::Publishing;
        if writer.atomic_replay_target.is_some() {
            journal
                .validate_publication_barrier(target)
                .await
                .map_err(event_status)?;
        }
        finish_pending_publication(writer, publisher).await?;
        return Ok(());
    }
    // Finish the captured baseline before entering journal replay.
    if writer.current.is_none() {
        writer.stage = ProducerStage::Backfill;
        backfill(
            writer,
            physical_catalog_identity,
            scanner,
            reader,
            extractor,
            publisher,
            credits,
            limits,
        )
        .await?;
        return Ok(());
    }
    ensure_background_compaction(
        writer,
        publisher,
        compaction_cpu,
        credits,
        compaction_ready,
        limits,
    )?;
    if !writer.pending_skipped_ack
        && should_harvest_background_compaction(
            writer.current.is_some(),
            writer.pending_prepared_rows,
            writer.pending_mutations.is_empty(),
            writer
                .background_compaction
                .as_ref()
                .is_some_and(BackgroundCompaction::is_finished),
        )
        && publish_finished_background_compaction(writer, publisher).await?
    {
        return Ok(());
    }
    writer.stage = ProducerStage::JournalScan;
    // Leave charged mutation-window room for the decoded journal page.
    let max_page = u64::try_from(limits.flush_bytes.saturating_div(4).max(1))
        .unwrap_or(u64::MAX)
        .min(MAX_INDEX_EVENT_PAGE_BYTES)
        .max(1);
    let page_memory_bytes = usize::try_from(max_page)
        .unwrap_or(usize::MAX)
        .saturating_mul(JOURNAL_PAGE_RESIDENT_MULTIPLIER)
        .max(1);
    // Advance across several journal pages before yielding the partition. The
    // mutation window owns coalesced records and their memory permit, while a
    // transient page permit bounds each decode. This removes the former
    // reconcile/publication barrier after every page without multiplying the
    // configured memory ceiling.
    let read_ahead_pages = journal_read_ahead_pages(limits.parallelism);
    for _ in 0..read_ahead_pages {
        let ready = finish_look_ahead(writer, target).await?;
        writer.look_ahead_permitted = false;
        let mut page_present = false;
        // Charge the encoded page, decoded changes, dispatcher output, and
        // mutation clones before reading any of them. This permit is transient;
        // only the coalesced mutation window remains charged after this advance.
        {
            let _page_memory = if ready.is_none() {
                Some(
                    credits
                        .acquire(IndexingMemoryStage::ReplayInput, page_memory_bytes)
                        .map_err(|_| {
                            Status::resource_exhausted("v1 journal-page memory unavailable")
                        })?,
                )
            } else {
                None
            };
            let page_started = Instant::now();
            let (page, prepared_dispatches, _look_ahead_memory) = if let Some(ready) = ready {
                (Some(ready.page), Some(ready.dispatches), Some(ready.memory))
            } else {
                (
                    journal
                        .next_page(
                            writer.recipe.family.tenant_id,
                            writer.recipe.family.bucket_id,
                            &writer.scanned,
                            target,
                            max_page,
                        )
                        .await
                        .map_err(event_status)?,
                    None,
                    None,
                )
            };
            tracing::debug!(
                histogram.keldra_index_v1_journal_page_duration_seconds =
                    page_started.elapsed().as_secs_f64(),
                counter.keldra_index_v1_journal_page_changes =
                    page.as_ref().map_or(0, |page| page.changes.len()),
                gauge.keldra_index_v1_journal_page_memory_bytes = page_memory_bytes,
                "v1 producer read one bounded journal page"
            );
            if let Some(page) = page {
                page_present = true;
                let mut dispatches = prepared_dispatches.unwrap_or_default();
                // A prefetched page was already observed by its owned dispatcher.
                if _look_ahead_memory.is_none() {
                    for change in &page.changes {
                        let event_source = page.through.sources[&change.node].source;
                        dispatches.extend(
                            writer
                                .dispatcher
                                .as_mut()
                                .expect("look-ahead dispatcher restored")
                                .observe(event_source, &change.change)?,
                        );
                    }
                }
                writer.scanned = page.through;
                let node = NodeId(u64::from(writer.source.node_id));
                let proposed = writer.scanned.sources[&node].next_offset;
                let safe_next = writer
                    .dispatcher
                    .as_ref()
                    .expect("look-ahead dispatcher restored")
                    .checkpoint_limit(writer.source, proposed);
                if safe_next > writer.pending_next {
                    let (page_atomic, mutations) = prepare_page(writer, dispatches, safe_next)?;
                    if !writer.pending_mutations.is_empty()
                        && mutation_window_needed(
                            &writer.pending_mutations,
                            writer.pending_mutation_bytes,
                            &mutations,
                        )? > writer.pending_mutation_capacity
                    {
                        flush(
                            writer,
                            physical_catalog_identity,
                            reader,
                            extractor,
                            publisher,
                            credits,
                            limits,
                        )
                        .await?;
                    }
                    let chunks = publication_chunks(
                        mutations,
                        safe_next,
                        limits.flush_operations,
                        writer.through_atomic,
                        page_atomic,
                        |mutation| mutation.offset,
                        |mutation| mutation.atomic_group,
                    )?;
                    let chunk_count = chunks.len();
                    for (index, chunk) in chunks.into_iter().enumerate() {
                        let operations = u64::try_from(chunk.mutations.len()).unwrap_or(u64::MAX);
                        if !writer.pending_mutations.is_empty()
                            && writer.pending_operations.saturating_add(operations)
                                > limits.flush_operations
                        {
                            flush(
                                writer,
                                physical_catalog_identity,
                                reader,
                                extractor,
                                publisher,
                                credits,
                                limits,
                            )
                            .await?;
                        }
                        writer.through_atomic = chunk.through_atomic;
                        queue_mutations(writer, chunk.mutations, chunk.next)?;
                        if index + 1 != chunk_count || should_flush(writer, limits) {
                            writer.look_ahead_permitted = index + 1 == chunk_count;
                            flush(
                                writer,
                                physical_catalog_identity,
                                reader,
                                extractor,
                                publisher,
                                credits,
                                limits,
                            )
                            .await?;
                        }
                    }
                }
            }
        }
        if !page_present {
            break;
        }
    }
    acknowledge_external_skipped_positions(writer, target, journal, credits, limits).await?;
    let atomic_ack = atomic_progress::acknowledge_complete_atomic_cut(
        writer, target, journal, publisher, credits, limits,
    )
    .await?;
    if atomic_ack && writer.pending_publication.is_some() {
        finish_pending_publication(writer, publisher).await?;
    } else if atomic_ack {
        flush(
            writer,
            physical_catalog_identity,
            reader,
            extractor,
            publisher,
            credits,
            limits,
        )
        .await?;
    }
    if writer
        .since
        .is_some_and(|since| since.elapsed() >= limits.flush_age)
    {
        writer.look_ahead_permitted = true;
        flush(
            writer,
            physical_catalog_identity,
            reader,
            extractor,
            publisher,
            credits,
            limits,
        )
        .await?;
    }
    let target_next = target
        .sources
        .get(&NodeId(u64::from(writer.source.node_id)))
        .filter(|cursor| cursor.source == writer.source)
        .map_or(0, |cursor| cursor.next_offset);
    writer.stage = if writer_processed_next(writer) >= target_next
        && writer.scanned.sources == target.sources
    {
        ProducerStage::CaughtUp
    } else {
        ProducerStage::JournalScan
    };
    if writer.scanned.sources == target.sources
        && writer.current.as_ref().is_some_and(|current| {
            current.current.through_atomic_position
                >= target.atomic.finalized_through().unwrap_or(0)
        })
    {
        writer.atomic_replay_target = None;
    }
    Ok(())
}

fn should_flush(writer: &Writer, limits: Limits) -> bool {
    !writer.pending_mutations.is_empty()
        && (writer.pending_mutation_bytes >= limits.flush_bytes
            || writer.pending_operations >= limits.flush_operations
            || writer
                .since
                .is_some_and(|since| since.elapsed() >= limits.flush_age))
}

fn has_publication_work(has_current: bool, prepared_source_rows: u64) -> bool {
    !has_current || prepared_source_rows != 0
}

async fn flush(
    writer: &mut Writer,
    physical_catalog_identity: [u8; 32],
    reader: &ClusterObjectReader,
    extractor: &V1ProjectionExtractor,
    publisher: &V1ProjectionPublisher,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<(), Status> {
    let telemetry = super::v1_telemetry::global();
    super::v1_telemetry::V1PipelineTelemetry::add(&telemetry.flush_calls, 1);
    let _flush_timer =
        super::v1_telemetry::V1PipelineTelemetry::start_phase(&telemetry.flush_nanos);
    if writer.pending_publication.is_some() {
        return finish_pending_publication(writer, publisher).await;
    }
    if !writer.pending_mutations.is_empty() {
        writer.stage = ProducerStage::Preparing;
        let next = writer.pending_next;
        let mut mutations = writer
            .pending_mutations
            .values()
            .cloned()
            .collect::<Vec<_>>();
        mutations.sort_by_key(|mutation| (mutation.offset, mutation.ordinal));
        prepare_lane(
            writer,
            physical_catalog_identity,
            mutations,
            next,
            reader,
            extractor,
            publisher,
            credits,
            limits,
        )
        .await?;
    }
    if !writer.pending_skipped_ack
        && !has_publication_work(writer.current.is_some(), writer.pending_prepared_rows)
    {
        clear_pending_mutations(writer)?;
        writer.since = None;
        return Ok(());
    }
    let start = publication_start(writer.current.as_ref());
    let next = writer.accumulator.next_offset();
    if next <= start {
        return Ok(());
    }
    super::v1_telemetry::V1PipelineTelemetry::add(&telemetry.publication_flushes, 1);
    let projected_bytes = u64::try_from(writer.accumulator.buffered_bytes())
        .map_err(|_| Status::resource_exhausted("v1 projected bytes exceed telemetry"))?;
    writer.pending_projected_encoded_bytes = projected_bytes;
    writer.stage = ProducerStage::Sealing;
    if writer.sealing_progress.is_none() {
        reserve_sealing_progress(
            writer,
            std::iter::empty(),
            std::iter::empty(),
            credits,
            publisher.query_block_limits(),
        )?;
    }
    let seal_timer = super::v1_telemetry::V1PipelineTelemetry::start_phase(&telemetry.seal_nanos);
    let sealed = writer.accumulator.seal_and_reset().map_err(index_status)?;
    let (sealed, source_permit) = sealed.into_parts();
    let packed = sealed.deltas.iter().try_fold(0usize, |sum, delta| {
        sum.checked_add(delta.bytes.len())
            .ok_or_else(|| Status::resource_exhausted("v1 pack size overflow"))
    })?;
    let pack_permit = credits
        .acquire(IndexingMemoryStage::SealScratch, packed.max(1))
        .map_err(|_| Status::resource_exhausted("v1 pack memory unavailable"))?;
    let preload_bytes = spine_preload_bound(writer.current.as_ref())?;
    let _preload = credits
        .acquire(IndexingMemoryStage::SealScratch, preload_bytes)
        .map_err(|_| Status::resource_exhausted("v1 spine preload memory unavailable"))?;
    drop(seal_timer);
    let compaction = take_finished_background_compaction(writer, publisher).await?;
    let query = std::mem::take(&mut writer.query);
    let placeholder = empty_query_credits(credits, limits)?;
    let mut query_credits = std::mem::replace(&mut writer.query_credits, placeholder);
    query_credits.enter_sealing().map_err(index_status)?;
    start_look_ahead(
        writer,
        next,
        physical_catalog_identity,
        reader,
        extractor,
        credits,
        limits,
        publisher.query_block_limits(),
    )?;
    let generation_build_timer =
        super::v1_telemetry::V1PipelineTelemetry::start_phase(&telemetry.generation_build_nanos);
    let prepared = if let Some((base, _)) = &compaction {
        writer.stage = ProducerStage::Publishing;
        publisher
            .prepare_atomic_generation_after_compaction(
                &writer.recipe.storage_tenant,
                &writer.recipe.bucket,
                writer.recipe.family.tenant_id,
                writer.recipe.family.bucket_id,
                writer.partition,
                writer.recipe.physical_generation,
                writer
                    .current
                    .as_ref()
                    .expect("compaction requires Current"),
                base,
                start,
                next,
                writer.through_atomic,
                sealed.deltas,
                query,
                query_credits,
                ProjectionPackCredits::from_pipeline_permit(pack_permit),
                preload_bytes,
            )
            .await?
    } else {
        writer.stage = ProducerStage::Publishing;
        publisher
            .prepare_atomic_generation(
                &writer.recipe.storage_tenant,
                &writer.recipe.bucket,
                writer.recipe.family.tenant_id,
                writer.recipe.family.bucket_id,
                writer.partition,
                writer.recipe.physical_generation,
                writer.current.as_ref(),
                start,
                next,
                writer.through_atomic,
                sealed.deltas,
                query,
                query_credits,
                ProjectionPackCredits::from_pipeline_permit(pack_permit),
                preload_bytes,
            )
            .await?
    };
    drop(generation_build_timer);
    // The merged query input has now been encoded into the charged immutable
    // artifacts owned by `prepared`; release its independent lane credits.
    writer.query_input_credits.clear();
    drop(source_permit);
    let rows = next
        .checked_sub(start)
        .ok_or_else(|| Status::data_loss("v1 cut regressed"))?;
    // Original compaction artifacts are already durable. Retain the proposal
    // so any small path-copy pages created while rebasing are included in the
    // successor's ordinary immutable publication and remain memory-charged.
    let compaction_artifacts = compaction.map(|(_, artifacts)| artifacts);
    let predecessor = if let Some(version) = writer.catalog_rebuild_current_version {
        V1PublicationPredecessor::CatalogRebuild(version)
    } else if let Some(current) = writer.current.as_ref() {
        V1PublicationPredecessor::Current(current)
    } else {
        V1PublicationPredecessor::Initial
    };
    writer.pending_publication = Some(publisher.prepare_atomic_publication(
        writer.partition,
        predecessor,
        prepared,
        compaction_artifacts,
        rows,
        writer.source_bytes,
    )?);
    finish_pending_publication(writer, publisher).await
}

async fn finish_pending_publication(
    writer: &mut Writer,
    publisher: &V1ProjectionPublisher,
) -> Result<(), Status> {
    let telemetry = super::v1_telemetry::global();
    let mut pending = writer
        .pending_publication
        .take()
        .expect("v1 pending publication exists");
    let result = publisher
        .publish_atomic_generation(
            &writer.recipe.storage_tenant,
            &writer.recipe.bucket,
            writer.recipe.family.tenant_id,
            writer.recipe.family.bucket_id,
            writer.partition,
            &mut pending,
            &mut writer.post_cas_verification,
        )
        .await;
    let (published, verification) = match result {
        Ok(published) => published,
        Err(error) => {
            writer.pending_publication = Some(pending);
            return Err(error);
        }
    };
    writer.catalog_rebuild_current_version = None;
    writer.current = Some(published);
    if let Some(look_ahead) = &mut writer.look_ahead {
        look_ahead.note_publication_finished();
    }
    writer.post_cas_verification = Some(verification);
    super::v1_telemetry::V1PipelineTelemetry::add(
        &telemetry.prepared_rows,
        writer.pending_prepared_rows,
    );
    super::v1_telemetry::V1PipelineTelemetry::add(
        &telemetry.prepared_bytes,
        writer.pending_prepared_bytes,
    );
    super::v1_telemetry::V1PipelineTelemetry::add(
        &telemetry.projected_rows,
        writer.pending_projected_rows,
    );
    super::v1_telemetry::V1PipelineTelemetry::add(
        &telemetry.projected_bytes,
        writer.pending_projected_encoded_bytes,
    );
    writer.since = None;
    writer.source_bytes = 0;
    writer.pending_prepared_rows = 0;
    writer.pending_skipped_ack = false;
    writer.skipped_proof_next = writer.skipped_proof_next.max(
        writer
            .current
            .as_ref()
            .expect("published Current exists")
            .current
            .next_offset,
    );
    writer.pending_prepared_bytes = 0;
    writer.pending_projected_rows = 0;
    writer.pending_projected_encoded_bytes = 0;
    writer.sealing_progress = None;
    clear_pending_mutations(writer)?;
    Ok(())
}

fn clear_pending_mutations(writer: &mut Writer) -> Result<(), Status> {
    writer.pending_mutations.clear();
    writer.pending_mutation_bytes = 0;
    writer.pending_operations = 0;
    writer
        .pending_mutation_permit
        .shrink_to(1)
        .map_err(index_status)
}

fn dispatch_mutations(dispatch: V1SourceDispatch) -> Result<(u64, Vec<Mutation>), Status> {
    match dispatch {
        V1SourceDispatch::OrdinaryHead { head, .. } => Ok((0, vec![head_mutation(head, 0)])),
        V1SourceDispatch::FinalizedAtomic(group) => {
            if group.mutations.is_empty() {
                return Err(Status::data_loss("v1 atomic group is empty"));
            }
            let mutations = group
                .mutations
                .into_iter()
                .enumerate()
                .map(|(ordinal, value)| {
                    let mutation = value.mutation;
                    Ok(Mutation {
                        offset: mutation.source_journal_position,
                        ordinal: u32::try_from(ordinal).map_err(|_| {
                            Status::resource_exhausted("v1 atomic group is too large")
                        })?,
                        tenant_id: mutation.tenant_id,
                        bucket_id: mutation.bucket_id,
                        path: mutation.exact_path,
                        canonical_path: mutation.canonical_path,
                        version: mutation.path_version.0,
                        deleted: mutation.deleted,
                        atomic_group: Some(group.cursor),
                        // Atomic summaries carry no predecessor accounting evidence.
                        predecessor_absent_at_window_start: false,
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?;
            Ok((group.cursor, mutations))
        }
    }
}

pub(super) fn head_mutation(head: ObjectHeadChange, ordinal: u32) -> Mutation {
    let predecessor_absent_at_window_start = head.canonical_path.is_none()
        && head
            .accounting_transition
            .is_some_and(|transition| transition.previous_live_length.is_none());
    Mutation {
        offset: head.offset,
        ordinal,
        tenant_id: head.tenant_id,
        bucket_id: head.bucket_id,
        path: head.exact_path,
        canonical_path: head.canonical_path,
        version: head.path_version.0,
        deleted: matches!(head.kind, ObjectHeadChangeKind::Delete),
        atomic_group: None,
        predecessor_absent_at_window_start,
    }
}

pub(super) fn merge_query(
    target: &mut PreparedQueryMutationBatch,
    mut source: PreparedQueryMutationBatch,
) -> Result<(), Status> {
    // Preparations carry complete replacements and need no preceding Current
    // state. Coalesce them in source order so each document contributes only
    // its newest gate and complete field material to this unpublished run.
    if let Some(incoming) = source.membership.take() {
        match &mut target.membership {
            Some(current) if current.recipe == incoming.recipe => {
                merge_query_gates(&mut current.gates, incoming.gates);
            }
            None => {
                let mut incoming = incoming;
                let gates = std::mem::take(&mut incoming.gates);
                merge_query_gates(&mut incoming.gates, gates);
                target.membership = Some(incoming);
            }
            Some(_) => return Err(Status::data_loss("v1 membership recipe conflict")),
        }
    }
    for field in source.fields {
        let key = (field.recipe, field.delta.presence.document);
        match target.fields.binary_search_by_key(&key, |current| {
            (current.recipe, current.delta.presence.document)
        }) {
            Ok(index) => target.fields[index] = field,
            Err(index) => target.fields.insert(index, field),
        }
    }
    Ok(())
}

fn merge_query_gates(
    target: &mut Vec<keldra_index::v1::QueryDocumentGate>,
    source: Vec<keldra_index::v1::QueryDocumentGate>,
) {
    for gate in source {
        match target.binary_search_by_key(&gate.document, |current| current.document) {
            Ok(index) => target[index] = gate,
            Err(index) => target.insert(index, gate),
        }
    }
}

pub(crate) fn source_scope(source: SourceId) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keldra/v1/source-scope/v1\0");
    hasher.update(&source.node_id.to_be_bytes());
    hasher.update(&source.source_epoch);
    *hasher.finalize().as_bytes()
}

fn index_status(error: keldra_index::IndexError) -> Status {
    match error {
        keldra_index::IndexError::ResourceLimit { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        _ => Status::data_loss(error.to_string()),
    }
}

fn event_status(error: super::events::IndexEventError) -> Status {
    Status::unavailable(error.to_string())
}

fn publication_start(current: Option<&LoadedV1ProjectionGeneration>) -> u64 {
    current.map_or(0, |value| value.current.next_offset)
}

fn coalesce_units(
    mut through_atomic: u64,
    units: Vec<(u64, Vec<Mutation>)>,
) -> Result<(u64, Vec<Mutation>), Status> {
    let mut mutations = Vec::new();
    let mut first_transitions = BTreeMap::new();
    for (atomic, unit) in units {
        let unique = unit
            .iter()
            .map(|mutation| mutation.path.as_str())
            .collect::<BTreeSet<_>>();
        if unique.len() != unit.len() {
            return Err(Status::data_loss(
                "one atomic mutation unit repeats an exact source path",
            ));
        }
        for mutation in &unit {
            let position = (mutation.offset, mutation.ordinal);
            first_transitions
                .entry(mutation.path.clone())
                .and_modify(|(first_position, predecessor_absent)| {
                    if position < *first_position {
                        *first_position = position;
                        *predecessor_absent = mutation.predecessor_absent_at_window_start;
                    }
                })
                .or_insert((position, mutation.predecessor_absent_at_window_start));
        }
        through_atomic = through_atomic.max(atomic);
        mutations.extend(unit);
    }
    let mut mutations = coalesce_latest_by_source_path(mutations, |mutation| {
        (mutation.path.clone(), mutation.offset, mutation.ordinal)
    })?;
    for mutation in &mut mutations {
        mutation.predecessor_absent_at_window_start = first_transitions[&mutation.path].1;
    }
    Ok((through_atomic, mutations))
}

fn current_placement(decisions: &DecisionRaft) -> Result<ClusterPlacement, Status> {
    let state = decisions
        .state()
        .map_err(|_| Status::unavailable("membership unavailable"))?;
    ClusterPlacement::from_applied(&state).map_err(|error| Status::unavailable(error.to_string()))
}

#[cfg(test)]
#[path = "v1_consumer_tests.rs"]
mod tests;
