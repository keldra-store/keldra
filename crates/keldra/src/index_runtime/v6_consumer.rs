//! Sole ordered producer for format-v6 physical projection partitions.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use keldra_consensus::{DecisionRaft, NodeId};
use keldra_index::v6::{
    IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryPermit, IndexingMemoryStage,
    PartitionProjectionAccumulator, PreparedProjectionBatchReservation, PreparedProjectionRow,
    PreparedQueryMutationBatch, ProjectionBatchAdmission, ProjectionPackCredits,
    ProjectionPartitionIdentity, QueryBlockCredits,
};
use keldra_store::{ObjectHeadChange, ObjectHeadChangeKind, ObjectKey, SourceId, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_placement::ClusterPlacement;
use crate::index_config::IndexRuntimeConfig;

use super::catalog::{IndexCatalog, PhysicalCatalogRecipe};
use super::events::{IndexBarrier, IndexEventJournal, MAX_INDEX_EVENT_PAGE_BYTES};
use super::hot_ingress::HotProjectionIngress;
use super::source::{IndexBuildObject, IndexSourceMutation};
use super::v6_backfill::open_partition_baseline;
use super::v6_extractor::{SelectedV6Source, V6ProjectionExtractor, matching_recipes};
use super::v6_journal_dispatch::{V6OrderedSourceDispatcher, V6SourceDispatch};
use super::v6_mutation_window::coalesce_latest_by_source_path;
use super::v6_publication::{LoadedV6ProjectionGeneration, V6ProjectionPublisher};

const POLL: Duration = Duration::from_millis(25);
const RETRY: Duration = Duration::from_millis(250);

pub(crate) struct V6IndexProducerTask {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for V6IndexProducerTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone, Copy)]
struct Limits {
    bytes: usize,
    flush_bytes: usize,
    flush_age: Duration,
    flush_operations: u64,
    lsm_runs: u64,
    lsm_bytes: u64,
    parallelism: usize,
}

struct Writer {
    recipe: PhysicalCatalogRecipe,
    source: SourceId,
    partition: ProjectionPartitionIdentity,
    current: Option<LoadedV6ProjectionGeneration>,
    dispatcher: V6OrderedSourceDispatcher,
    scanned: IndexBarrier,
    accumulator: PartitionProjectionAccumulator,
    query: PreparedQueryMutationBatch,
    query_credits: QueryBlockCredits,
    since: Option<Instant>,
    source_bytes: u64,
    pending_prepared_rows: u64,
    pending_prepared_bytes: u64,
    pending_projected_rows: u64,
    through_atomic: u64,
    pending_mutations: BTreeMap<String, Mutation>,
    pending_mutation_bytes: usize,
    pending_operations: u64,
    pending_next: u64,
    pending_mutation_capacity: usize,
    _pending_mutation_permit: IndexingMemoryPermit,
}

#[derive(Clone)]
struct Mutation {
    offset: u64,
    ordinal: u32,
    tenant_id: u64,
    bucket_id: u64,
    path: String,
    version: u64,
    deleted: bool,
}

struct SelectedMutation {
    mutation: Mutation,
    selected: SelectedV6Source,
    previous: Vec<keldra_index::v6::ProjectedDocumentState>,
    source_bytes: u64,
    _input: keldra_index::v6::IndexingMemoryPermit,
}

impl V6IndexProducerTask {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        local_node: NodeId,
        decisions: DecisionRaft,
        catalog: IndexCatalog,
        journal: Arc<IndexEventJournal>,
        scanner: super::scanner::ClusterIndexScanner,
        reader: ClusterObjectReader,
        cpu: super::cpu::IndexCpuPool,
        hot: HotProjectionIngress,
        publisher: V6ProjectionPublisher,
        config: IndexRuntimeConfig,
    ) -> Result<Self, Status> {
        let limits = limits(config)?;
        let stage_limits = IndexingMemoryLimits {
            hot_payload_bytes: limits.bytes,
            worker_scratch_bytes: limits.bytes,
            prepared_rows_bytes: limits.bytes,
            replay_input_bytes: limits.bytes,
            projection_accumulator_bytes: limits.bytes,
            seal_scratch_bytes: limits.bytes,
            ordering_catalog_bytes: limits.bytes,
        };
        let credits =
            IndexingMemoryCredits::new(limits.bytes, stage_limits).map_err(index_status)?;
        let extractor = V6ProjectionExtractor::new(
            reader.clone(),
            cpu,
            hot,
            limits.flush_bytes.saturating_div(limits.parallelism).max(1),
        );
        let task = tokio::spawn(async move {
            let mut writers = BTreeMap::new();
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
                    &credits,
                    limits,
                    &mut writers,
                )
                .await;
                if let Err(error) = result {
                    writers.clear();
                    tracing::warn!(%error, "v6 producer will replay from Current");
                    tokio::time::sleep(RETRY).await;
                } else {
                    tokio::time::sleep(POLL).await;
                }
            }
        });
        Ok(Self { task })
    }
}

fn limits(config: IndexRuntimeConfig) -> Result<Limits, Status> {
    // Catalog and hot ingress own one quarter each. The producer is limited to
    // the remaining half, so their independent allocators cannot exceed the
    // one configured pipeline ceiling.
    let configured = usize::try_from(config.pipeline_memory_bytes())
        .map_err(|_| Status::invalid_argument("v6 pipeline memory exceeds this platform"))?;
    let bytes = configured.saturating_div(2).max(1);
    let flush_bytes = usize::try_from(config.flush_bytes())
        .map_err(|_| Status::invalid_argument("v6 flush bytes exceed this platform"))?
        .min(bytes.saturating_div(4).max(1));
    Ok(Limits {
        bytes,
        flush_bytes,
        flush_age: config.flush_max_age(),
        flush_operations: config.flush_max_operations(),
        lsm_runs: u64::from(config.lsm_max_runs_per_level()),
        lsm_bytes: config.lsm_max_unmerged_bytes_per_level(),
        parallelism: usize::try_from(config.indexing_cores())
            .map_err(|_| Status::invalid_argument("v6 indexing cores exceed this platform"))?,
    })
}

#[allow(clippy::too_many_arguments)]
async fn reconcile(
    local_node: NodeId,
    decisions: &DecisionRaft,
    catalog: &IndexCatalog,
    journal: &IndexEventJournal,
    scanner: &super::scanner::ClusterIndexScanner,
    reader: &ClusterObjectReader,
    extractor: &V6ProjectionExtractor,
    publisher: &V6ProjectionPublisher,
    credits: &IndexingMemoryCredits,
    limits: Limits,
    writers: &mut BTreeMap<ProjectionPartitionIdentity, Writer>,
) -> Result<(), Status> {
    let placement = current_placement(decisions)?;
    let target = journal.capture_barrier().await.map_err(event_status)?;
    if placement.fence() != target.fence {
        return Err(Status::unavailable("v6 placement changed"));
    }
    let (_, _, _, recipes, _) = catalog.snapshot()?;
    let mut assigned = BTreeSet::new();
    // `IndexCatalog` already interns these by physical family. Logical aliases
    // therefore never repeat this loop's source work.
    for recipe in recipes {
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
            if !writers.contains_key(&partition) {
                writers.insert(
                    partition,
                    open_writer(
                        recipe.clone(),
                        partition,
                        &target,
                        publisher,
                        credits,
                        limits,
                    )
                    .await?,
                );
            }
            let writer = writers.get_mut(&partition).expect("writer was inserted");
            if writer.recipe.physical_generation != recipe.physical_generation {
                return Err(Status::unavailable("v6 physical catalog changed"));
            }
            advance(
                writer, &target, journal, scanner, reader, extractor, publisher, credits, limits,
            )
            .await?;
        }
    }
    writers.retain(|partition, _| assigned.contains(partition));
    Ok(())
}

async fn open_writer(
    recipe: PhysicalCatalogRecipe,
    partition: ProjectionPartitionIdentity,
    target: &IndexBarrier,
    publisher: &V6ProjectionPublisher,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<Writer, Status> {
    let source = SourceId {
        node_id: u16::try_from(partition.source_node)
            .map_err(|_| Status::data_loss("v6 source node exceeds SourceId"))?,
        source_epoch: partition.source_epoch,
    };
    let current = publisher
        .load_current(
            &recipe.storage_tenant,
            &recipe.bucket,
            recipe.family.tenant_id,
            recipe.family.bucket_id,
            partition,
        )
        .await?;
    let durable_start = current
        .as_ref()
        .map_or(1, |loaded| loaded.current.next_offset);
    let node = NodeId(u64::from(source.node_id));
    let cursor = target
        .sources
        .get(&node)
        .filter(|cursor| cursor.source == source)
        .ok_or_else(|| Status::unavailable("assigned v6 source is absent"))?;
    if durable_start > cursor.next_offset {
        return Err(Status::data_loss("v6 Current is ahead of source"));
    }
    let control_bytes = limits.bytes.saturating_div(16).max(512);
    let control = credits
        .acquire(IndexingMemoryStage::ReplayInput, control_bytes)
        .map_err(|_| Status::resource_exhausted("v6 control memory unavailable"))?;
    let dispatcher = V6OrderedSourceDispatcher::new(
        target.fence,
        BTreeSet::from([source]),
        control,
        control_bytes,
    );
    let mut scanned = target.clone();
    for cursor in scanned.sources.values_mut() {
        cursor.next_offset = cursor.next_offset.min(durable_start);
    }
    // Offset zero is the journal sentinel. A fresh empty partition publishes
    // the query-ready no-op range [0, 1), giving activation a real Current
    // without claiming any retained source mutation.
    let accumulator_start = if current.is_some() { durable_start } else { 0 };
    let accumulator = PartitionProjectionAccumulator::new(
        source_scope(source),
        partition,
        accumulator_start,
        limits.flush_bytes,
        credits.clone(),
    )
    .map_err(index_status)?;
    let query_permit = credits
        .acquire(
            IndexingMemoryStage::OrderingCatalog,
            limits.bytes.saturating_div(4).max(1),
        )
        .map_err(|_| Status::resource_exhausted("v6 query memory unavailable"))?;
    let through_atomic = current.as_ref().map_or_else(
        || target.atomic.finalized_through().unwrap_or(0),
        |loaded| loaded.current.through_atomic_position,
    );
    let pending_mutation_capacity = limits.flush_bytes.max(1);
    let pending_mutation_permit = credits
        .acquire(IndexingMemoryStage::ReplayInput, pending_mutation_capacity)
        .map_err(|_| Status::resource_exhausted("v6 mutation-window memory unavailable"))?;
    Ok(Writer {
        recipe,
        source,
        partition,
        current,
        dispatcher,
        scanned,
        accumulator,
        query: PreparedQueryMutationBatch::default(),
        query_credits: QueryBlockCredits::from_pipeline_permit(query_permit),
        since: None,
        source_bytes: 0,
        pending_prepared_rows: 0,
        pending_prepared_bytes: 0,
        pending_projected_rows: 0,
        through_atomic,
        pending_mutations: BTreeMap::new(),
        pending_mutation_bytes: 0,
        pending_operations: 0,
        pending_next: accumulator_start,
        pending_mutation_capacity,
        _pending_mutation_permit: pending_mutation_permit,
    })
}

async fn backfill(
    writer: &mut Writer,
    scanner: &super::scanner::ClusterIndexScanner,
    reader: &ClusterObjectReader,
    extractor: &V6ProjectionExtractor,
    publisher: &V6ProjectionPublisher,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<(), Status> {
    let frame_bytes = u64::try_from(limits.flush_bytes).unwrap_or(u64::MAX);
    let mut baseline = open_partition_baseline(
        scanner,
        &writer.recipe,
        writer.source,
        writer.partition,
        frame_bytes,
    )
    .await?;
    let captured_next = baseline.captured_next_offset();
    let batch_items = usize::try_from(limits.flush_operations)
        .unwrap_or(usize::MAX)
        .min(4_096)
        .max(1);
    let selected_bytes = limits.flush_bytes.saturating_div(limits.parallelism).max(1);
    loop {
        let batch = baseline
            .next_selected_batch(extractor, credits, selected_bytes, batch_items)
            .await?;
        if batch.is_empty() {
            break;
        }
        let mut rows = Vec::with_capacity(batch.len());
        let mut previous = BTreeMap::new();
        let next = batch
            .last()
            .and_then(|item| item.baseline_offset.checked_add(1))
            .ok_or_else(|| Status::data_loss("v6 baseline offset overflow"))?;
        for item in batch {
            let (path, version) = match &item.selected.source {
                IndexSourceMutation::Upsert(object) => (object.path.clone(), object.version),
                IndexSourceMutation::Remove(_) => {
                    return Err(Status::data_loss("v6 current baseline contains a delete"));
                }
            };
            let prepared = V6ProjectionExtractor::prepare(
                source_scope(writer.source),
                &item.selected,
                &writer.recipe,
                Vec::new(),
                &mut writer.query_credits,
            )?;
            merge_query(&mut writer.query, prepared.query)?;
            previous.insert(path.clone(), Vec::new());
            writer.source_bytes = writer.source_bytes.saturating_add(item.source_bytes);
            // Reading the exact journal position is intentional lineage
            // validation even though the accumulator uses dense baseline
            // offsets for this one non-journal initial build.
            let _source_journal_offset = item.source_journal_offset;
            rows.push(PreparedProjectionRow {
                source_offset: item.baseline_offset,
                mutation_ordinal: 0,
                source_path: path,
                source_version: version,
                projected_states: prepared.current,
            });
        }
        apply_rows(writer, next, rows, previous, credits, limits)?;
    }
    apply_rows(
        writer,
        captured_next,
        Vec::new(),
        BTreeMap::new(),
        credits,
        limits,
    )?;
    let node = NodeId(u64::from(writer.source.node_id));
    writer
        .scanned
        .sources
        .get_mut(&node)
        .ok_or_else(|| Status::data_loss("v6 baseline source cursor is absent"))?
        .next_offset = captured_next;
    writer.pending_next = captured_next;
    flush(writer, reader, extractor, publisher, credits, limits).await
}

#[allow(clippy::too_many_arguments)]
async fn advance(
    writer: &mut Writer,
    target: &IndexBarrier,
    journal: &IndexEventJournal,
    scanner: &super::scanner::ClusterIndexScanner,
    reader: &ClusterObjectReader,
    extractor: &V6ProjectionExtractor,
    publisher: &V6ProjectionPublisher,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<(), Status> {
    if writer.current.is_none() && writer.accumulator.next_offset() == 0 {
        backfill(
            writer, scanner, reader, extractor, publisher, credits, limits,
        )
        .await?;
        return Ok(());
    }
    // A mutation-window entry owns two path strings plus its map node. Keep
    // journal pages below one quarter of the charged window so one fresh page
    // can always be represented conservatively before the threshold flush.
    let max_page = u64::try_from(limits.flush_bytes.saturating_div(4).max(1))
        .unwrap_or(u64::MAX)
        .min(MAX_INDEX_EVENT_PAGE_BYTES)
        .max(1);
    while let Some(page) = journal
        .next_page(
            writer.recipe.family.tenant_id,
            writer.recipe.family.bucket_id,
            &writer.scanned,
            target,
            max_page,
        )
        .await
        .map_err(event_status)?
    {
        let mut dispatches = Vec::new();
        for change in &page.changes {
            let event_source = page.through.sources[&change.node].source;
            dispatches.extend(writer.dispatcher.observe(event_source, &change.change)?);
        }
        writer.scanned = page.through;
        let node = NodeId(u64::from(writer.source.node_id));
        let proposed = writer.scanned.sources[&node].next_offset;
        let safe_next = writer.dispatcher.checkpoint_limit(writer.source, proposed);
        if safe_next <= writer.pending_next {
            continue;
        }
        let (page_atomic, mutations) = prepare_page(writer, dispatches, safe_next)?;
        if !writer.pending_mutations.is_empty()
            && mutation_window_needed(
                &writer.pending_mutations,
                writer.pending_mutation_bytes,
                &mutations,
            )? > writer.pending_mutation_capacity
        {
            flush(writer, reader, extractor, publisher, credits, limits).await?;
        }
        writer.through_atomic = writer.through_atomic.max(page_atomic);
        queue_mutations(writer, mutations, safe_next)?;
        if should_flush(writer, limits) {
            flush(writer, reader, extractor, publisher, credits, limits).await?;
        }
    }
    if writer
        .since
        .is_some_and(|since| since.elapsed() >= limits.flush_age)
    {
        flush(writer, reader, extractor, publisher, credits, limits).await?;
    }
    Ok(())
}

fn prepare_page(
    writer: &Writer,
    dispatches: Vec<V6SourceDispatch>,
    safe_next: u64,
) -> Result<(u64, Vec<Mutation>), Status> {
    let first = writer.pending_next;
    if safe_next <= first {
        return Ok((0, Vec::new()));
    }
    let mut units = Vec::new();
    for dispatch in dispatches {
        let (atomic, mut group) = dispatch_mutations(dispatch)?;
        group.retain(|mutation| mutation.offset >= first && mutation.offset < safe_next);
        if !group.is_empty() {
            units.push((atomic, group));
        }
    }
    coalesce_units(0, units)
}

fn queue_mutations(
    writer: &mut Writer,
    mutations: Vec<Mutation>,
    safe_next: u64,
) -> Result<(), Status> {
    let arms_age = !mutations.is_empty();
    queue_mutation_window(
        &mut writer.pending_mutations,
        &mut writer.pending_mutation_bytes,
        &mut writer.pending_operations,
        &mut writer.pending_next,
        writer.pending_mutation_capacity,
        mutations,
        safe_next,
    )?;
    if arms_age {
        writer.since.get_or_insert_with(Instant::now);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn queue_mutation_window(
    latest: &mut BTreeMap<String, Mutation>,
    resident_bytes: &mut usize,
    observed_operations: &mut u64,
    next_offset: &mut u64,
    capacity: usize,
    mutations: Vec<Mutation>,
    safe_next: u64,
) -> Result<(), Status> {
    let needed = mutation_window_needed(latest, *resident_bytes, &mutations)?;
    if needed > capacity {
        return Err(Status::resource_exhausted(format!(
            "v6 mutation window requires {needed} bytes but admits {}",
            capacity
        )));
    }
    let operations = u64::try_from(mutations.len())
        .map_err(|_| Status::resource_exhausted("v6 mutation-window operations overflow"))?;
    for mutation in mutations {
        let replace = latest.get(&mutation.path).is_none_or(|previous| {
            (mutation.offset, mutation.ordinal) > (previous.offset, previous.ordinal)
        });
        if replace {
            latest.insert(mutation.path.clone(), mutation);
        }
    }
    *resident_bytes = needed;
    *observed_operations = observed_operations.saturating_add(operations);
    *next_offset = safe_next;
    Ok(())
}

fn mutation_window_needed(
    latest: &BTreeMap<String, Mutation>,
    resident_bytes: usize,
    mutations: &[Mutation],
) -> Result<usize, Status> {
    mutations
        .iter()
        .try_fold(resident_bytes, |mut needed, mutation| {
            if let Some(previous) = latest.get(&mutation.path) {
                if (mutation.offset, mutation.ordinal) <= (previous.offset, previous.ordinal) {
                    return Ok(needed);
                }
                needed = needed.saturating_sub(mutation_window_bytes(previous));
            }
            needed
                .checked_add(mutation_window_bytes(mutation))
                .ok_or_else(|| Status::resource_exhausted("v6 mutation-window size overflow"))
        })
}

fn mutation_window_bytes(mutation: &Mutation) -> usize {
    std::mem::size_of::<Mutation>()
        .saturating_add(mutation.path.capacity().saturating_mul(2))
        .saturating_add(std::mem::size_of::<usize>().saturating_mul(4))
}

#[allow(clippy::too_many_arguments)]
async fn prepare_lane(
    writer: &mut Writer,
    mutations: Vec<Mutation>,
    safe_next: u64,
    reader: &ClusterObjectReader,
    extractor: &V6ProjectionExtractor,
    publisher: &V6ProjectionPublisher,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<(), Status> {
    let current = writer.current.clone();
    let recipe = writer.recipe.clone();
    let scope = source_scope(writer.source);
    let input_bytes = limits.flush_bytes.saturating_div(limits.parallelism).max(1);
    let mut selected = Vec::new();
    for chunk in mutations.chunks(limits.parallelism) {
        let mut jobs = tokio::task::JoinSet::new();
        for mutation in chunk.iter().cloned() {
            let reader = reader.clone();
            let extractor = extractor.clone();
            let publisher = publisher.clone();
            let recipe = recipe.clone();
            let current = current.clone();
            let credits = credits.clone();
            jobs.spawn(async move {
                let input = credits
                    .acquire(IndexingMemoryStage::ReplayInput, input_bytes)
                    .map_err(|_| {
                        Status::resource_exhausted("v6 replay input memory unavailable")
                    })?;
                select_mutation(
                    reader, extractor, publisher, recipe, current, scope, mutation, input,
                )
                .await
            });
        }
        while let Some(joined) = jobs.join_next().await {
            let result = joined
                .map_err(|error| Status::internal(format!("v6 selection task failed: {error}")))?;
            if let Some(value) = result? {
                selected.push(value);
            }
        }
    }
    selected.sort_by_key(|value| (value.mutation.offset, value.mutation.ordinal));
    let mut rows = Vec::with_capacity(selected.len());
    let mut previous = BTreeMap::new();
    for value in selected {
        let mutation = value.mutation;
        let prepared = V6ProjectionExtractor::prepare(
            scope,
            &value.selected,
            &writer.recipe,
            value.previous.clone(),
            &mut writer.query_credits,
        )?;
        merge_query(&mut writer.query, prepared.query)?;
        previous.insert(mutation.path.clone(), value.previous);
        writer.source_bytes = writer.source_bytes.saturating_add(value.source_bytes);
        rows.push(PreparedProjectionRow {
            source_offset: mutation.offset,
            mutation_ordinal: mutation.ordinal,
            source_path: mutation.path,
            source_version: mutation.version,
            projected_states: prepared.current,
        });
    }
    apply_rows(writer, safe_next, rows, previous, credits, limits)
}

async fn select_mutation(
    reader: ClusterObjectReader,
    extractor: V6ProjectionExtractor,
    publisher: V6ProjectionPublisher,
    recipe: PhysicalCatalogRecipe,
    current: Option<LoadedV6ProjectionGeneration>,
    scope: [u8; 32],
    mutation: Mutation,
    mut input: keldra_index::v6::IndexingMemoryPermit,
) -> Result<Option<SelectedMutation>, Status> {
    let source = load_exact_mutation(
        &reader,
        &recipe,
        &mutation.path,
        mutation.version,
        mutation.deleted,
    )
    .await?;
    let source_bytes = match &source {
        IndexSourceMutation::Upsert(object) => object.content_length,
        IndexSourceMutation::Remove(_) => 0,
    };
    let content_type = match &source {
        IndexSourceMutation::Upsert(object) => object.content_type.as_deref(),
        IndexSourceMutation::Remove(_) => None,
    };
    let matched = matching_recipes(
        std::slice::from_ref(&recipe),
        mutation.tenant_id,
        mutation.bucket_id,
        &mutation.path,
        content_type,
    );
    if matched.is_empty() {
        return Ok(None);
    }
    let selected = extractor
        .select(mutation.tenant_id, mutation.bucket_id, source, &matched)
        .await?;
    let previous = match current {
        Some(current) => {
            publisher
                .load_source_states(
                    &recipe.storage_tenant,
                    &recipe.bucket,
                    mutation.tenant_id,
                    mutation.bucket_id,
                    &current.generation,
                    scope,
                    &mutation.path,
                )
                .await?
        }
        None => Vec::new(),
    };
    let retained_bytes = selected_mutation_resident_bytes(&mutation, &selected, &previous)?;
    if retained_bytes > input.bytes() {
        return Err(Status::resource_exhausted(format!(
            "v6 replay selection requires {retained_bytes} bytes but its construction bound is {}",
            input.bytes()
        )));
    }
    input
        .shrink_to(retained_bytes.max(1))
        .map_err(index_status)?;
    Ok(Some(SelectedMutation {
        mutation,
        selected,
        previous,
        source_bytes,
        _input: input,
    }))
}

fn selected_mutation_resident_bytes(
    mutation: &Mutation,
    selected: &SelectedV6Source,
    previous: &[keldra_index::v6::ProjectedDocumentState],
) -> Result<usize, Status> {
    let mut bytes = std::mem::size_of::<SelectedMutation>()
        .checked_add(mutation.path.capacity())
        .ok_or_else(|| Status::resource_exhausted("v6 replay selection size overflow"))?;
    bytes = bytes
        .checked_add(
            match &selected.source {
                IndexSourceMutation::Upsert(object) => std::mem::size_of::<IndexBuildObject>()
                    .checked_add(object.path.capacity())
                    .and_then(|value| {
                        value.checked_add(
                            object
                                .content_type
                                .as_ref()
                                .map_or(0, |content_type| content_type.capacity()),
                        )
                    }),
                IndexSourceMutation::Remove(identity) => {
                    std::mem::size_of_val(identity).checked_add(identity.path.capacity())
                }
            }
            .ok_or_else(|| Status::resource_exhausted("v6 replay selection size overflow"))?,
        )
        .ok_or_else(|| Status::resource_exhausted("v6 replay selection size overflow"))?;
    if let Some(projection) = &selected.selected {
        bytes = bytes
            .checked_add(projection.resident_bytes().map_err(index_status)?)
            .ok_or_else(|| Status::resource_exhausted("v6 replay selection size overflow"))?;
    }
    bytes = bytes
        .checked_add(
            previous
                .len()
                .checked_mul(std::mem::size_of::<keldra_index::v6::ProjectedDocumentState>())
                .ok_or_else(|| Status::resource_exhausted("v6 replay selection size overflow"))?,
        )
        .ok_or_else(|| Status::resource_exhausted("v6 replay selection size overflow"))?;
    for state in previous {
        bytes = bytes
            .checked_add(state.resident_bytes().map_err(index_status)?)
            .ok_or_else(|| Status::resource_exhausted("v6 replay selection size overflow"))?;
    }
    Ok(bytes)
}

fn apply_rows(
    writer: &mut Writer,
    next: u64,
    mut rows: Vec<PreparedProjectionRow>,
    previous: BTreeMap<String, Vec<keldra_index::v6::ProjectedDocumentState>>,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<(), Status> {
    let first = writer.accumulator.next_offset();
    if next <= first {
        return Ok(());
    }
    rows.sort_by_key(|row| (row.source_offset, row.mutation_ordinal));
    let reservation = PreparedProjectionBatchReservation::reserve(credits, limits.flush_bytes)
        .map_err(|_| Status::resource_exhausted("v6 prepared-row memory unavailable"))?;
    let batch = reservation
        .finish(source_scope(writer.source), first, next, rows)
        .map_err(index_status)?;
    let prepared_bytes = u64::try_from(batch.resident_bytes())
        .map_err(|_| Status::resource_exhausted("v6 prepared-row bytes exceed telemetry"))?;
    match writer
        .accumulator
        .apply_batch(batch, previous)
        .map_err(index_status)?
    {
        ProjectionBatchAdmission::Applied {
            source_rows,
            coalesced_rows,
            ..
        } => {
            let source_rows = u64::try_from(source_rows)
                .map_err(|_| Status::resource_exhausted("v6 prepared rows exceed telemetry"))?;
            writer.pending_prepared_rows = writer.pending_prepared_rows.saturating_add(source_rows);
            writer.pending_prepared_bytes = writer.pending_prepared_bytes.saturating_add(
                super::v6_telemetry::V6PipelineTelemetry::indexed_prepared_bytes(
                    source_rows,
                    prepared_bytes,
                ),
            );
            writer.pending_projected_rows = writer.pending_projected_rows.saturating_add(
                u64::try_from(coalesced_rows).map_err(|_| {
                    Status::resource_exhausted("v6 projected rows exceed telemetry")
                })?,
            );
            // Cursor-only batches are expected for reserved projection and
            // catalog objects. They advance the in-memory contiguous cut, but
            // must not arm publication: an empty Current creates another
            // reserved source event and otherwise feeds itself forever. The
            // preceding Current remains the conservative durable retention
            // proof. A later matching mutation publishes one range spanning
            // every skipped control event; restart safely replays the retained
            // gap from that preceding Current.
            if source_rows != 0 {
                writer.since.get_or_insert_with(Instant::now);
            }
            Ok(())
        }
        ProjectionBatchAdmission::ReplayRequired { .. } => {
            Err(Status::resource_exhausted("v6 accumulator requires replay"))
        }
    }
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
    reader: &ClusterObjectReader,
    extractor: &V6ProjectionExtractor,
    publisher: &V6ProjectionPublisher,
    credits: &IndexingMemoryCredits,
    limits: Limits,
) -> Result<(), Status> {
    if !writer.pending_mutations.is_empty() {
        let next = writer.pending_next;
        let mut mutations = writer
            .pending_mutations
            .values()
            .cloned()
            .collect::<Vec<_>>();
        mutations.sort_by_key(|mutation| (mutation.offset, mutation.ordinal));
        prepare_lane(
            writer, mutations, next, reader, extractor, publisher, credits, limits,
        )
        .await?;
    }
    if !has_publication_work(writer.current.is_some(), writer.pending_prepared_rows) {
        writer.pending_mutations.clear();
        writer.pending_mutation_bytes = 0;
        writer.pending_operations = 0;
        writer.since = None;
        return Ok(());
    }
    let start = publication_start(writer.current.as_ref());
    let next = writer.accumulator.next_offset();
    if next <= start {
        return Ok(());
    }
    let projected_bytes = u64::try_from(writer.accumulator.buffered_bytes())
        .map_err(|_| Status::resource_exhausted("v6 projected bytes exceed telemetry"))?;
    let needs_compaction = writer.current.as_ref().is_some_and(|current| {
        current.generation.query_stream_root.run_count >= limits.lsm_runs
            || current.generation.roots.iter().any(|root| {
                root.segment_count >= limits.lsm_runs || root.encoded_bytes >= limits.lsm_bytes
            })
    });
    let sealed = writer.accumulator.seal_and_reset().map_err(index_status)?;
    let (sealed, source_permit) = sealed.into_parts();
    let packed = sealed.deltas.iter().try_fold(0usize, |sum, delta| {
        sum.checked_add(delta.bytes.len())
            .ok_or_else(|| Status::resource_exhausted("v6 pack size overflow"))
    })?;
    let pack_permit = credits
        .acquire(IndexingMemoryStage::SealScratch, packed.max(1))
        .map_err(|_| Status::resource_exhausted("v6 pack memory unavailable"))?;
    let preload_bytes = limits.bytes.saturating_div(8).max(1);
    let _preload = credits
        .acquire(IndexingMemoryStage::ReplayInput, preload_bytes)
        .map_err(|_| Status::resource_exhausted("v6 spine preload memory unavailable"))?;
    let compaction = if needs_compaction {
        let component_permit = credits
            .acquire(
                IndexingMemoryStage::SealScratch,
                limits.bytes.saturating_div(8).max(1),
            )
            .map_err(|_| {
                Status::resource_exhausted("v6 component compaction memory unavailable")
            })?;
        let query_permit = credits
            .acquire(
                IndexingMemoryStage::OrderingCatalog,
                limits.bytes.saturating_div(8).max(1),
            )
            .map_err(|_| Status::resource_exhausted("v6 query compaction memory unavailable"))?;
        Some(
            publisher
                .prepare_compaction(
                    &writer.recipe.storage_tenant,
                    &writer.recipe.bucket,
                    writer.recipe.family.tenant_id,
                    writer.recipe.family.bucket_id,
                    writer
                        .current
                        .as_ref()
                        .expect("compaction requires Current"),
                    usize::try_from(limits.lsm_runs).map_err(|_| {
                        Status::invalid_argument("v6 LSM run bound exceeds this platform")
                    })?,
                    usize::try_from(limits.lsm_bytes).map_err(|_| {
                        Status::invalid_argument("v6 LSM byte bound exceeds this platform")
                    })?,
                    preload_bytes,
                    ProjectionPackCredits::from_pipeline_permit(component_permit),
                    QueryBlockCredits::from_pipeline_permit(query_permit),
                )
                .await?
                .into_parts(),
        )
    } else {
        None
    };
    let query = std::mem::take(&mut writer.query);
    let placeholder = credits
        .acquire(IndexingMemoryStage::OrderingCatalog, 1)
        .map_err(|_| Status::resource_exhausted("v6 query transfer memory unavailable"))?;
    let query_credits = std::mem::replace(
        &mut writer.query_credits,
        QueryBlockCredits::from_pipeline_permit(placeholder),
    );
    let prepared = if let Some((base, _)) = &compaction {
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
    drop(source_permit);
    let rows = next
        .checked_sub(start)
        .ok_or_else(|| Status::data_loss("v6 cut regressed"))?;
    if let Some((_, artifacts)) = compaction {
        publisher
            .publish_compaction_artifacts(
                &writer.recipe.storage_tenant,
                &writer.recipe.bucket,
                writer.recipe.family.tenant_id,
                writer.recipe.family.bucket_id,
                writer.partition,
                artifacts,
            )
            .await?;
    }
    let published = publisher
        .publish_atomic_generation(
            &writer.recipe.storage_tenant,
            &writer.recipe.bucket,
            writer.recipe.family.tenant_id,
            writer.recipe.family.bucket_id,
            writer.partition,
            writer.current.as_ref(),
            prepared,
            rows,
            writer.source_bytes,
        )
        .await?;
    let next_query_permit = credits
        .acquire(
            IndexingMemoryStage::OrderingCatalog,
            limits.bytes.saturating_div(4).max(1),
        )
        .map_err(|_| Status::resource_exhausted("v6 next query memory unavailable"))?;
    writer.query_credits = QueryBlockCredits::from_pipeline_permit(next_query_permit);
    writer.current = Some(published);
    let telemetry = super::v6_telemetry::global();
    super::v6_telemetry::V6PipelineTelemetry::add(
        &telemetry.prepared_rows,
        writer.pending_prepared_rows,
    );
    super::v6_telemetry::V6PipelineTelemetry::add(
        &telemetry.prepared_bytes,
        writer.pending_prepared_bytes,
    );
    super::v6_telemetry::V6PipelineTelemetry::add(
        &telemetry.projected_rows,
        writer.pending_projected_rows,
    );
    super::v6_telemetry::V6PipelineTelemetry::add(&telemetry.projected_bytes, projected_bytes);
    writer.since = None;
    writer.source_bytes = 0;
    writer.pending_prepared_rows = 0;
    writer.pending_prepared_bytes = 0;
    writer.pending_projected_rows = 0;
    writer.pending_mutations.clear();
    writer.pending_mutation_bytes = 0;
    writer.pending_operations = 0;
    Ok(())
}

fn dispatch_mutations(dispatch: V6SourceDispatch) -> Result<(u64, Vec<Mutation>), Status> {
    match dispatch {
        V6SourceDispatch::OrdinaryHead { head, .. } => Ok((0, vec![head_mutation(head, 0)])),
        V6SourceDispatch::FinalizedAtomic(group) => {
            if group.mutations.is_empty() {
                return Err(Status::data_loss("v6 atomic group is empty"));
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
                            Status::resource_exhausted("v6 atomic group is too large")
                        })?,
                        tenant_id: mutation.tenant_id,
                        bucket_id: mutation.bucket_id,
                        path: mutation.exact_path,
                        version: mutation.path_version.0,
                        deleted: mutation.deleted,
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?;
            Ok((group.cursor, mutations))
        }
    }
}

fn head_mutation(head: ObjectHeadChange, ordinal: u32) -> Mutation {
    Mutation {
        offset: head.offset,
        ordinal,
        tenant_id: head.tenant_id,
        bucket_id: head.bucket_id,
        path: head.exact_path,
        version: head.path_version.0,
        deleted: matches!(head.kind, ObjectHeadChangeKind::Delete),
    }
}

async fn load_exact_mutation(
    reader: &ClusterObjectReader,
    recipe: &PhysicalCatalogRecipe,
    path: &str,
    version: u64,
    deleted: bool,
) -> Result<IndexSourceMutation, Status> {
    let identity = keldra_index::v6::ObjectIdentity {
        path: path.into(),
        version,
    };
    if deleted {
        return Ok(IndexSourceMutation::Remove(identity));
    }
    let key = ObjectKey::new(&recipe.storage_tenant, &recipe.bucket, path)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    let selected = reader
        .exact_versions_stable(
            &[key],
            &[VersionId(version)],
            recipe.family.tenant_id,
            recipe.family.bucket_id,
        )
        .await?
        .pop()
        .flatten()
        .ok_or_else(|| Status::failed_precondition("v6 exact source version is absent"))?;
    if selected.deleted || selected.id.0 != version {
        return Err(Status::data_loss("v6 exact source version mismatch"));
    }
    let blob = selected
        .blob
        .ok_or_else(|| Status::data_loss("v6 source blob is absent"))?;
    Ok(IndexSourceMutation::Upsert(IndexBuildObject {
        path: path.into(),
        version,
        content_type: selected.content_type,
        content_hash: blob.hash,
        content_length: blob.length,
        committed_at_unix_millis: selected.committed_at_unix_millis,
    }))
}

fn merge_query(
    target: &mut PreparedQueryMutationBatch,
    mut source: PreparedQueryMutationBatch,
) -> Result<(), Status> {
    if let Some(incoming) = source.membership.take() {
        match &mut target.membership {
            Some(current) if current.recipe == incoming.recipe => {
                current.gates.extend(incoming.gates)
            }
            None => target.membership = Some(incoming),
            Some(_) => return Err(Status::data_loss("v6 membership recipe conflict")),
        }
    }
    target.fields.extend(source.fields);
    target.fields.sort_by_key(|field| field.recipe);
    Ok(())
}

pub(crate) fn source_scope(source: SourceId) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keldra/v6/source-scope/v1\0");
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

fn publication_start(current: Option<&LoadedV6ProjectionGeneration>) -> u64 {
    current.map_or(0, |value| value.current.next_offset)
}

fn coalesce_units(
    mut through_atomic: u64,
    units: Vec<(u64, Vec<Mutation>)>,
) -> Result<(u64, Vec<Mutation>), Status> {
    let mut mutations = Vec::new();
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
        through_atomic = through_atomic.max(atomic);
        mutations.extend(unit);
    }
    let mutations = coalesce_latest_by_source_path(mutations, |mutation| {
        (mutation.path.clone(), mutation.offset, mutation.ordinal)
    })?;
    Ok((through_atomic, mutations))
}

fn current_placement(decisions: &DecisionRaft) -> Result<ClusterPlacement, Status> {
    let state = decisions
        .state()
        .map_err(|_| Status::unavailable("membership unavailable"))?;
    ClusterPlacement::from_applied(&state).map_err(|error| Status::unavailable(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query_update(
        document: keldra_index::v6::StableDocumentKey,
        path: &str,
        version: u64,
    ) -> PreparedQueryMutationBatch {
        PreparedQueryMutationBatch {
            membership: Some(keldra_index::v6::PreparedQueryMembershipDelta {
                recipe: keldra_index::v6::RecipeIdentity::new([3; 32]).unwrap(),
                gates: vec![keldra_index::v6::QueryDocumentGate {
                    document,
                    material_source_version: version,
                    current_source_version: version,
                    live: true,
                    source_path: Some(path.into()),
                    result_path: Some(path.into()),
                    result_version: version,
                }],
            }),
            fields: Vec::new(),
        }
    }

    fn mutation(path: &str, offset: u64) -> Mutation {
        Mutation {
            offset,
            ordinal: 0,
            tenant_id: 1,
            bucket_id: 2,
            path: path.into(),
            version: offset,
            deleted: false,
        }
    }

    #[test]
    fn fresh_partition_publication_starts_at_the_zero_sentinel() {
        assert_eq!(publication_start(None), 0);
    }

    #[test]
    fn reserved_only_progress_does_not_request_an_empty_current_publication() {
        // A fresh partition needs its one baseline Current for activation,
        // even when the baseline contains no matching objects.
        assert!(has_publication_work(false, 0));

        // Once Current exists, filtered control events advance only the replay
        // cursor. They cannot generate another Current/source event alone.
        assert!(!has_publication_work(true, 0));

        // The first later matching mutation makes the complete contiguous
        // range, including the skipped control positions, publishable.
        assert!(has_publication_work(true, 1));
    }

    #[test]
    fn repeated_hot_paths_coalesce_without_losing_the_safe_atomic_cut() {
        let units = (0..10_000_u64)
            .map(|offset| {
                (
                    offset + 10,
                    vec![mutation(
                        &format!("objects/{:03}", offset % 256),
                        offset + 1,
                    )],
                )
            })
            .collect();
        let (through_atomic, output) = coalesce_units(7, units).unwrap();
        assert_eq!(through_atomic, 10_009);
        assert_eq!(output.len(), 256);
        assert!(
            output
                .windows(2)
                .all(|pair| pair[0].offset < pair[1].offset)
        );
        assert!(output.iter().all(|mutation| mutation.offset > 9_744));
    }

    #[test]
    fn duplicate_path_inside_one_atomic_unit_still_fails_closed() {
        let error = coalesce_units(
            0,
            vec![(9, vec![mutation("objects/a", 3), mutation("objects/a", 3)])],
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn publication_window_is_newest_wins_across_page_boundaries() {
        let mut latest = BTreeMap::new();
        let mut bytes = 0;
        let mut operations = 0;
        let mut next = 1;
        let mut changed = mutation("objects/a", 2);
        changed.version = 20;
        queue_mutation_window(
            &mut latest,
            &mut bytes,
            &mut operations,
            &mut next,
            usize::MAX,
            vec![changed],
            3,
        )
        .unwrap();
        let mut restored = mutation("objects/a", 3);
        restored.version = 10;
        queue_mutation_window(
            &mut latest,
            &mut bytes,
            &mut operations,
            &mut next,
            usize::MAX,
            vec![restored],
            4,
        )
        .unwrap();

        assert_eq!(latest.len(), 1);
        assert_eq!(latest["objects/a"].version, 10);
        assert_eq!(next, 4);
        assert_eq!(operations, 2);
    }

    #[test]
    fn create_then_delete_remains_an_empty_latest_mutation_and_advances() {
        let mut latest = BTreeMap::new();
        let mut bytes = 0;
        let mut operations = 0;
        let mut next = 0;
        queue_mutation_window(
            &mut latest,
            &mut bytes,
            &mut operations,
            &mut next,
            usize::MAX,
            vec![mutation("objects/a", 1)],
            2,
        )
        .unwrap();
        let mut deleted = mutation("objects/a", 2);
        deleted.deleted = true;
        queue_mutation_window(
            &mut latest,
            &mut bytes,
            &mut operations,
            &mut next,
            usize::MAX,
            vec![deleted],
            3,
        )
        .unwrap();

        assert!(latest["objects/a"].deleted);
        assert_eq!(latest.len(), 1);
        assert_eq!(next, 3);
    }

    #[test]
    fn ten_thousand_hot_updates_prepare_only_256_query_documents() {
        let scope = [9; 32];
        let mut latest = BTreeMap::new();
        let mut bytes = 0;
        let mut operations = 0;
        let mut next = 0;
        for offset in 1..=10_000_u64 {
            queue_mutation_window(
                &mut latest,
                &mut bytes,
                &mut operations,
                &mut next,
                usize::MAX,
                vec![mutation(&format!("objects/{:03}", offset % 256), offset)],
                offset + 1,
            )
            .unwrap();
        }
        assert_eq!(latest.len(), 256);
        assert_eq!(operations, 10_000);
        assert_eq!(next, 10_001);

        let mut pending = PreparedQueryMutationBatch::default();
        let mut preparations = 0;
        for mutation in latest.into_values() {
            let document =
                keldra_index::v6::StableDocumentKey::derive(scope, &mutation.path, 0).unwrap();
            merge_query(
                &mut pending,
                query_update(document, &mutation.path, mutation.version),
            )
            .unwrap();
            preparations += 1;
        }

        let membership = pending.membership.as_ref().unwrap();
        assert_eq!(membership.gates.len(), 256);
        assert_eq!(preparations, 256);
        assert!(membership.gates.iter().all(|gate| {
            let path = gate.source_path.as_deref().unwrap();
            let suffix = path.rsplit('/').next().unwrap().parse::<u64>().unwrap();
            gate.current_source_version == 10_000 - ((10_000 - suffix) % 256)
        }));

        let bytes = 4 * 1024 * 1024;
        let memory = IndexingMemoryCredits::new(
            bytes,
            IndexingMemoryLimits {
                hot_payload_bytes: bytes,
                worker_scratch_bytes: bytes,
                prepared_rows_bytes: bytes,
                replay_input_bytes: bytes,
                projection_accumulator_bytes: bytes,
                seal_scratch_bytes: bytes,
                ordering_catalog_bytes: bytes,
            },
        )
        .unwrap();
        let credits = QueryBlockCredits::from_pipeline_permit(
            memory
                .acquire(IndexingMemoryStage::OrderingCatalog, bytes)
                .unwrap(),
        );
        let artifacts = keldra_index::v6::prepare_projection_query_run(
            ProjectionPartitionIdentity::new([1; 32], 2, [3; 32], 2, 4, 5).unwrap(),
            [6; 32],
            1,
            1,
            10_001,
            10_000,
            pending,
            keldra_index::v6::QueryBlockLimits::default_for_memory(),
            credits,
        )
        .unwrap();
        assert!(!artifacts.artifacts().blocks.is_empty());
    }

    #[test]
    fn replay_selection_holds_measured_metadata_not_construction_headroom() {
        let mutation = mutation("objects/a", 7);
        let selected = SelectedV6Source {
            source: IndexSourceMutation::Remove(keldra_index::v6::ObjectIdentity {
                path: "objects/a".into(),
                version: 7,
            }),
            selected: None,
        };

        let retained = selected_mutation_resident_bytes(&mutation, &selected, &[]).unwrap();

        assert!(retained < 1024);
        assert!(retained >= std::mem::size_of::<SelectedMutation>());
    }
}
