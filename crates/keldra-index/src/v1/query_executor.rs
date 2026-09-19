use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;

use crate::IndexError;
use crate::typed_json::{
    AggregateRequest, AggregateResult, FacetRequest, FacetResult, FieldId, FieldSchema, OrderField,
    Predicate, RangeBound, ScalarValue, analyze_typed_json_text, encode_scalar_sort_key,
};

use super::{
    ArtifactPackReference, DecodedQueryBlock, LogicalProjectionBinding,
    MAX_QUERY_DOCUMENT_PATH_BYTES, ProjectionPartitionIdentity, ProjectionQueryRunDescriptor,
    ProjectionQueryStreamRoot, QueryBlockCredits, QueryBlockDescriptor, QueryBlockKind,
    QueryBlockLimits, QueryDocumentGate, QueryRecipeCatalogProof, QueryRunChild, QueryRunPage,
    QueryRunReference, QueryTermEntry, RecipeIdentity, StableDocumentKey, decode_document_gate,
    decode_positions, decode_projection_query_run, decode_query_run_page, decode_term_entry,
};

#[path = "query_executor_admission.rs"]
mod admission;
pub use admission::{
    AuthorizedQueryCandidate, MAX_QUERY_CANDIDATE_ADMISSION_BATCH, PinnedPartitionQueryRoot,
    PinnedRealtimeOverlayRun, QueryAdmissionCandidate, QueryAdmissionContext, QueryArtifactKind,
    QueryArtifactLoad, QueryCandidateAdmission, QueryCommonCut, QueryRootCutProof,
};
#[path = "query_executor_authorization.rs"]
mod authorization;
use admission::{match_all_live_documents, resident_gate_bytes, resident_selected_candidate_bytes};
use authorization::{authorize_selected_candidates, resident_authorized_candidate_bytes};
#[path = "query_executor_budget.rs"]
mod budget;
use budget::Budget;
#[path = "query_executor_candidates.rs"]
mod candidates;
use candidates::{
    AlignedGates, candidate_is_current, resident_gate_dynamic_bytes, select_handoff_candidate,
};
#[path = "query_executor_dense.rs"]
mod dense;
use dense::{
    DenseCandidateSet, DenseQueryPoint, DenseQueryPosting, QueryMaterialCandidate,
    SegmentCandidateIdentity, candidate_set_bytes, document_key_set_bytes,
    intersect_term_candidates,
};
#[path = "query_executor_general.rs"]
mod general;
use general::execute_general_query;
#[path = "query_executor_natural.rs"]
mod natural;
#[path = "query_executor_parallel_blocks.rs"]
mod parallel_blocks;
use super::query_parallel::{
    QueryPartitionExecutor, QueryPartitionJob, SerialQueryPartitionExecutor,
    execute_typed_json_query_with_executor,
};
use natural::execute_bounded_natural_equal;
use parallel_blocks::{
    PostingBlockWork, decode_gate_block, decode_position_block, decode_posting_block,
    decode_range_block, load_decoded_block, point_descriptor_overlaps, posting_entry_bytes,
    resident_point_entry_bytes,
};
#[path = "query_executor_snapshot.rs"]
mod snapshot;
use snapshot::{
    PartitionManifest, PartitionView, load_exact_pre_admitted, load_partition_manifest,
    load_realtime_overlay_descriptor,
};
#[path = "query_executor_validate.rs"]
mod validate;
pub use snapshot::{
    QuerySnapshotIdentity, ValidatedQuerySnapshot, query_snapshot_identity,
    query_snapshot_identity_with_overlays,
};
use validate::validate_request;
#[path = "query_executor_values.rs"]
mod values;
use values::{
    BoundedCandidateCollector, PartitionValueColumns, QueryCandidate, QueryValueReducers,
    count_predicate_nodes, leaf_field, page_summary, predicate_requires_universe,
    requested_value_recipes, resident_scalar_bytes, resource, validate_leaf_capability,
};
pub use values::{ExplicitQuerySearchAfter, QueryPublicValueEncoder, ScalarSortKeyValueEncoder};

/// Maximum number of physical partition roots admitted by one logical query.
pub const MAX_QUERY_PARTITIONS: usize = 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryExecutionLimits {
    pub maximum_partitions: usize,
    pub maximum_page_loads: usize,
    pub maximum_run_loads: usize,
    pub maximum_block_loads: usize,
    pub maximum_page_bytes: usize,
    pub maximum_loaded_bytes: usize,
    pub maximum_heap_bytes: usize,
    pub maximum_boolean_nodes: usize,
    pub maximum_expanded_terms: usize,
    pub maximum_candidates: usize,
    pub maximum_results: usize,
    pub maximum_order_fields: usize,
    pub maximum_facets: usize,
    pub maximum_aggregates: usize,
}

impl QueryExecutionLimits {
    pub const fn default_for_memory() -> Self {
        Self {
            maximum_partitions: MAX_QUERY_PARTITIONS,
            maximum_page_loads: 8_192,
            maximum_run_loads: 4_096,
            maximum_block_loads: 4_096,
            maximum_page_bytes: 64 * 1024,
            maximum_loaded_bytes: 256 * 1024 * 1024,
            maximum_heap_bytes: 256 * 1024 * 1024,
            maximum_boolean_nodes: 256,
            maximum_expanded_terms: 4_096,
            maximum_candidates: 1_000_000,
            maximum_results: 10_000,
            maximum_order_fields: 64,
            maximum_facets: 64,
            maximum_aggregates: 64,
        }
    }

    pub fn validate(self) -> Result<Self, IndexError> {
        if self.maximum_partitions == 0
            || self.maximum_partitions > MAX_QUERY_PARTITIONS
            || self.maximum_page_loads == 0
            || self.maximum_run_loads == 0
            || self.maximum_block_loads == 0
            || self.maximum_page_bytes == 0
            || self.maximum_loaded_bytes == 0
            || self.maximum_heap_bytes == 0
            || self.maximum_boolean_nodes == 0
            || self.maximum_expanded_terms == 0
            || self.maximum_candidates == 0
            || self.maximum_results == 0
            || self.maximum_order_fields == 0
            || self.maximum_facets == 0
            || self.maximum_aggregates == 0
        {
            return Err(IndexError::InvalidDefinition(
                "v1 query execution limits are invalid".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryFieldBinding {
    pub field: FieldSchema,
    pub recipe: RecipeIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedJsonQueryRequest {
    pub logical: LogicalProjectionBinding,
    pub fields: Vec<QueryFieldBinding>,
    pub catalog_lineage: Vec<[u8; 32]>,
    pub recipe_catalog_proofs: Vec<QueryRecipeCatalogProof>,
    /// Absence is the public match-all contract over live membership gates.
    pub predicate: Option<Predicate>,
    pub order: Vec<OrderField>,
    pub facets: Vec<FacetRequest>,
    pub aggregates: Vec<AggregateRequest>,
    /// Exclusive stable-document cursor for the natural document order.
    /// This is only valid when no explicit order, facets, or aggregates are
    /// requested, because those operations require the complete candidate set.
    pub resume_after_document: Option<StableDocumentKey>,
    pub result_limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TypedJsonQueryResult {
    pub through_atomic_position: u64,
    pub candidates: Vec<AuthorizedQueryCandidate>,
    pub facets: Vec<FacetResult>,
    pub aggregates: Vec<AggregateResult>,
    pub loads: QueryLoadEvidence,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueryLoadEvidence {
    pub pages: usize,
    pub runs: usize,
    pub blocks: usize,
    pub bytes: usize,
}

/// Loads immutable artifacts by their content identity.
///
/// Implementations must verify bytes when they cross an untrusted boundary.
/// Callers may then decode the returned trusted-local bytes without hashing the
/// complete artifact again on every query. The reference-counted return value
/// lets implementations reuse immutable cached storage without copying it.
pub trait QueryArtifactLoader: Send {
    fn load_query_artifact(
        &mut self,
        request: QueryArtifactLoad,
    ) -> impl std::future::Future<Output = Result<Bytes, IndexError>> + Send;

    /// Returns an already validated immutable run descriptor when the loader
    /// keeps a decoded view beside its byte cache. Implementations may omit
    /// this optimization; query correctness never depends on it.
    fn cached_projection_query_run(
        &self,
        _request: QueryArtifactLoad,
    ) -> Result<Option<Arc<ProjectionQueryRunDescriptor>>, IndexError> {
        Ok(None)
    }

    /// Publishes a decoded immutable run descriptor for reuse by later
    /// queries. The default is deliberately a no-op for uncached loaders.
    fn cache_projection_query_run(
        &mut self,
        _request: QueryArtifactLoad,
        _descriptor: Arc<ProjectionQueryRunDescriptor>,
    ) {
    }

    /// Returns a structurally validated lookup view for an immutable block.
    /// Generation is part of the key so a publication cut cannot accidentally
    /// reuse a view admitted for another generation.
    fn cached_query_block(
        &self,
        _generation: [u8; 32],
        _request: QueryArtifactLoad,
    ) -> Result<Option<Arc<DecodedQueryBlock>>, IndexError> {
        Ok(None)
    }

    fn cache_query_block(
        &mut self,
        _generation: [u8; 32],
        _request: QueryArtifactLoad,
        _block: Arc<DecodedQueryBlock>,
    ) {
    }

    /// Join or lead population of one decoded immutable run. Leadership is an
    /// execution-future-local lease: dropping the query future drops the lease
    /// and wakes waiters even when the loader itself remains alive.
    fn coordinate_query_run_population(
        &mut self,
        _request: QueryArtifactLoad,
    ) -> impl std::future::Future<Output = Result<QueryPopulation, IndexError>> + Send {
        async { Ok(QueryPopulation::Lead(Box::new(()))) }
    }

    /// Equivalent single-flight coordination for a generation-bound decoded
    /// query block.
    fn coordinate_query_block_population(
        &mut self,
        _generation: [u8; 32],
        _request: QueryArtifactLoad,
    ) -> impl std::future::Future<Output = Result<QueryPopulation, IndexError>> + Send {
        async { Ok(QueryPopulation::Lead(Box::new(()))) }
    }

    /// Fork a query-local handle for independent partition work.
    ///
    /// A returned handle must address the same immutable artifact authority and
    /// share any authoritative validation/admission state with this handle.
    /// Disposable decoded caches may be shared as an optimization but cannot
    /// change correctness. The default keeps existing loaders serial.
    fn try_fork_query_loader(&self) -> Result<Option<Self>, IndexError>
    where
        Self: Sized,
    {
        Ok(None)
    }
}

/// Cancellation-safe ownership of one decoded-artifact population attempt.
///
/// Implementations release leadership and wake waiters from `Drop`.
pub trait QueryPopulationGuard: Send {}

impl<T: Send> QueryPopulationGuard for T {}

/// Result of joining the single-flight population for one decoded artifact.
pub enum QueryPopulation {
    /// Another attempt completed; the caller must retry its cache lookup.
    Completed,
    /// This attempt owns population while the returned lease remains alive.
    Lead(Box<dyn QueryPopulationGuard>),
}

pub(super) fn matching_run_blocks(
    run: &ProjectionQueryRunDescriptor,
    kind: QueryBlockKind,
    recipe: RecipeIdentity,
) -> &[QueryBlockDescriptor] {
    // Descriptor validation guarantees canonical
    // (kind, recipe, minimum, maximum, hash) order, so each kind/recipe group
    // is already contiguous and needs no per-query materialized directory.
    let start = run
        .blocks
        .partition_point(|block| (block.kind, block.recipe) < (kind, recipe));
    let end = start
        + run.blocks[start..].partition_point(|block| (block.kind, block.recipe) == (kind, recipe));
    &run.blocks[start..end]
}

pub async fn execute_typed_json_query<
    L: QueryArtifactLoader + 'static,
    A: QueryCandidateAdmission,
>(
    loader: &mut L,
    admission: &mut A,
    common_cut: QueryCommonCut,
    pins: &[PinnedPartitionQueryRoot],
    validated_snapshot: Option<Arc<ValidatedQuerySnapshot>>,
    request: &TypedJsonQueryRequest,
    execution_limits: QueryExecutionLimits,
    block_limits: QueryBlockLimits,
    block_credits: &mut QueryBlockCredits,
) -> Result<(TypedJsonQueryResult, Arc<ValidatedQuerySnapshot>), IndexError> {
    execute_typed_json_query_with_executor(
        loader,
        admission,
        common_cut,
        pins,
        validated_snapshot,
        request,
        execution_limits,
        block_limits,
        block_credits,
        &SerialQueryPartitionExecutor,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_typed_json_query_with_cursor<
    L: QueryArtifactLoader + 'static,
    A: QueryCandidateAdmission,
    E: QueryPublicValueEncoder,
>(
    loader: &mut L,
    admission: &mut A,
    common_cut: QueryCommonCut,
    pins: &[PinnedPartitionQueryRoot],
    validated_snapshot: Option<Arc<ValidatedQuerySnapshot>>,
    request: &TypedJsonQueryRequest,
    explicit_search_after: Option<&ExplicitQuerySearchAfter>,
    public_value_encoder: &E,
    execution_limits: QueryExecutionLimits,
    block_limits: QueryBlockLimits,
    block_credits: &mut QueryBlockCredits,
) -> Result<
    (
        TypedJsonQueryResult,
        Arc<ValidatedQuerySnapshot>,
        Option<ExplicitQuerySearchAfter>,
    ),
    IndexError,
> {
    execute_typed_json_query_with_cursor_and_executor(
        loader,
        admission,
        common_cut,
        pins,
        validated_snapshot,
        request,
        explicit_search_after,
        public_value_encoder,
        execution_limits,
        block_limits,
        block_credits,
        &SerialQueryPartitionExecutor,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_typed_json_query_with_cursor_and_executor<
    L: QueryArtifactLoader + 'static,
    A: QueryCandidateAdmission,
    E: QueryPublicValueEncoder,
    X: QueryPartitionExecutor,
>(
    loader: &mut L,
    admission: &mut A,
    common_cut: QueryCommonCut,
    pins: &[PinnedPartitionQueryRoot],
    validated_snapshot: Option<Arc<ValidatedQuerySnapshot>>,
    request: &TypedJsonQueryRequest,
    explicit_search_after: Option<&ExplicitQuerySearchAfter>,
    public_value_encoder: &E,
    execution_limits: QueryExecutionLimits,
    block_limits: QueryBlockLimits,
    block_credits: &mut QueryBlockCredits,
    partition_executor: &X,
) -> Result<
    (
        TypedJsonQueryResult,
        Arc<ValidatedQuerySnapshot>,
        Option<ExplicitQuerySearchAfter>,
    ),
    IndexError,
> {
    execute_typed_json_query_with_realtime_overlays_and_executor(
        loader,
        admission,
        common_cut,
        pins,
        &[],
        &[],
        validated_snapshot,
        request,
        explicit_search_after,
        public_value_encoder,
        execution_limits,
        block_limits,
        block_credits,
        partition_executor,
    )
    .await
}

/// Execute over one complete-prefix base vector plus canonical sparse
/// real-time runs. Sparse evidence participates in snapshot identity and is
/// merged before candidate authorization, ordering, facets, and aggregates.
#[allow(clippy::too_many_arguments)]
pub async fn execute_typed_json_query_with_realtime_overlays_and_executor<
    L: QueryArtifactLoader + 'static,
    A: QueryCandidateAdmission,
    E: QueryPublicValueEncoder,
    X: QueryPartitionExecutor,
>(
    loader: &mut L,
    admission: &mut A,
    common_cut: QueryCommonCut,
    pins: &[PinnedPartitionQueryRoot],
    realtime_overlay_generations: &[(ProjectionPartitionIdentity, [u8; 32])],
    realtime_overlays: &[PinnedRealtimeOverlayRun],
    validated_snapshot: Option<Arc<ValidatedQuerySnapshot>>,
    request: &TypedJsonQueryRequest,
    explicit_search_after: Option<&ExplicitQuerySearchAfter>,
    public_value_encoder: &E,
    execution_limits: QueryExecutionLimits,
    block_limits: QueryBlockLimits,
    block_credits: &mut QueryBlockCredits,
    partition_executor: &X,
) -> Result<
    (
        TypedJsonQueryResult,
        Arc<ValidatedQuerySnapshot>,
        Option<ExplicitQuerySearchAfter>,
    ),
    IndexError,
> {
    let execution_limits = execution_limits.validate()?;
    let block_limits = block_limits.validate()?;
    let snapshot_identity = validated_snapshot
        .as_ref()
        .map(|snapshot| snapshot.identity())
        .map_or_else(
            || {
                snapshot::query_snapshot_identity_with_overlays(
                    common_cut,
                    pins,
                    realtime_overlay_generations,
                    realtime_overlays,
                )
            },
            Result::<_, IndexError>::Ok,
        )?;
    if let Some(snapshot) = validated_snapshot.as_ref() {
        snapshot.validate_for(
            snapshot_identity,
            common_cut,
            pins,
            realtime_overlays,
            request,
        )?;
    }
    let contracts = validate_request(
        common_cut,
        pins,
        request,
        execution_limits,
        validated_snapshot.is_none(),
    )?;
    validate_realtime_overlays(pins, realtime_overlays)?;
    if explicit_search_after.is_some() && request.resume_after_document.is_some() {
        return Err(IndexError::InvalidQuery(
            "natural and explicitly ordered continuations cannot be combined".into(),
        ));
    }
    let mut budget = Budget::new(execution_limits);
    let mut snapshot_resident_bytes = 0usize;
    let mut snapshot_index_bytes = 0usize;
    let snapshot = if let Some(snapshot) = validated_snapshot {
        snapshot
    } else {
        let mut manifests = Vec::with_capacity(pins.len());
        for pin in pins.iter().copied() {
            let (mut manifest, resident_bytes, index_bytes) = load_partition_manifest(
                loader,
                partition_executor,
                PartitionView { pin },
                &request.catalog_lineage,
                &request.recipe_catalog_proofs,
                block_limits,
                block_credits,
                &mut budget,
            )
            .await?;
            snapshot_resident_bytes = snapshot_resident_bytes
                .checked_add(resident_bytes)
                .ok_or(IndexError::OffsetOverflow)?;
            snapshot_index_bytes = snapshot_index_bytes
                .checked_add(index_bytes)
                .ok_or(IndexError::OffsetOverflow)?;
            let mut overlay_runs = Vec::new();
            for overlay in realtime_overlays
                .iter()
                .rev()
                .filter(|overlay| overlay.partition == pin.partition)
            {
                let (run, overlay_resident) = load_realtime_overlay_descriptor(
                    loader,
                    partition_executor,
                    &PartitionView { pin },
                    &request.catalog_lineage,
                    &request.recipe_catalog_proofs,
                    overlay.run,
                    block_limits,
                    block_credits,
                    &mut budget,
                    true,
                )
                .await?;
                let overlay_index = std::mem::size_of::<Arc<ProjectionQueryRunDescriptor>>();
                budget.reserve_heap(block_credits, overlay_index)?;
                snapshot_resident_bytes = snapshot_resident_bytes
                    .checked_add(overlay_resident)
                    .ok_or(IndexError::OffsetOverflow)?;
                snapshot_index_bytes = snapshot_index_bytes
                    .checked_add(overlay_index)
                    .ok_or(IndexError::OffsetOverflow)?;
                overlay_runs.push(run);
            }
            overlay_runs.append(&mut manifest.runs);
            manifest.runs = overlay_runs;
            manifests.push(manifest);
        }
        Arc::new(ValidatedQuerySnapshot {
            memory_lease: super::SegmentMemoryLease::default(),
            identity: snapshot_identity,
            common_cut,
            pins: pins.to_vec(),
            realtime_overlays: realtime_overlays.to_vec(),
            logical: request.logical.clone(),
            catalog_lineage: request.catalog_lineage.clone(),
            recipe_catalog_proofs: request.recipe_catalog_proofs.clone(),
            manifests,
        })
    };
    let manifests = snapshot.manifests.as_slice();

    if let Some(candidates) = execute_bounded_natural_equal(
        loader,
        admission,
        partition_executor,
        common_cut,
        &manifests,
        request,
        &contracts,
        block_limits,
        block_credits,
        &mut budget,
    )
    .await?
    {
        block_credits.release(snapshot_resident_bytes)?;
        budget.release_heap(block_credits, snapshot_index_bytes)?;
        return Ok((
            TypedJsonQueryResult {
                through_atomic_position: common_cut.through_atomic_position,
                candidates,
                facets: Vec::new(),
                aggregates: Vec::new(),
                loads: budget.evidence(),
            },
            snapshot,
            None,
        ));
    }

    let general = execute_general_query(
        loader,
        admission,
        partition_executor,
        common_cut,
        manifests,
        request,
        &contracts,
        explicit_search_after,
        public_value_encoder,
        block_limits,
        block_credits,
        &mut budget,
    )
    .await?;
    let candidates = general.candidates;
    let facets = general.facets;
    let aggregates = general.aggregates;
    let next_search_after = general.next;
    block_credits.release(snapshot_resident_bytes)?;
    budget.release_heap(block_credits, snapshot_index_bytes)?;
    Ok((
        TypedJsonQueryResult {
            through_atomic_position: common_cut.through_atomic_position,
            candidates,
            facets,
            aggregates,
            loads: budget.evidence(),
        },
        snapshot,
        next_search_after,
    ))
}

fn validate_realtime_overlays(
    pins: &[PinnedPartitionQueryRoot],
    overlays: &[PinnedRealtimeOverlayRun],
) -> Result<(), IndexError> {
    if overlays.windows(2).any(|pair| {
        (
            pair[0].partition,
            pair[0].source_position,
            pair[0].atomic_position,
            pair[0].run.hash,
        ) >= (
            pair[1].partition,
            pair[1].source_position,
            pair[1].atomic_position,
            pair[1].run.hash,
        )
    }) {
        return Err(IndexError::InvalidQuery(
            "real-time overlay vector is not canonical".into(),
        ));
    }
    for overlay in overlays {
        let base = pins
            .binary_search_by_key(&overlay.partition, |pin| pin.partition)
            .ok()
            .map(|index| pins[index])
            .ok_or_else(|| {
                IndexError::InvalidQuery("real-time overlay has no base partition".into())
            })?;
        overlay.validate_against(base)?;
    }
    Ok(())
}

fn realtime_gate_is_absorbed(gate: &QueryDocumentGate, base_next_offset: u64) -> bool {
    gate.selective_source_position
        .is_some_and(|position| position < base_next_offset)
}

async fn load_latest_gates<L: QueryArtifactLoader + 'static, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    manifest: &PartitionManifest,
    recipe: RecipeIdentity,
    kind: QueryBlockKind,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<BTreeMap<StableDocumentKey, QueryDocumentGate>, IndexError> {
    let blocks = manifest
        .matching_blocks(kind, recipe)
        .enumerate()
        .map(|(ordinal, (run, descriptor))| {
            Ok((
                ordinal,
                manifest
                    .runs
                    .get(run)
                    .ok_or(IndexError::Integrity)?
                    .physical_catalog_generation,
                descriptor.clone(),
            ))
        })
        .collect::<Result<Vec<_>, IndexError>>()?;
    let mut gates = BTreeMap::new();
    let mut decoded = Vec::with_capacity(blocks.len());
    let can_parallel = blocks.len() > 1 && executor.maximum_parallelism() > 1;
    if can_parallel {
        let mut jobs = Vec::with_capacity(blocks.len());
        for (ordinal, generation, descriptor) in &blocks {
            let Some(mut task_loader) = loader.try_fork_query_loader()? else {
                jobs.clear();
                break;
            };
            let (mut task_budget, mut task_credits) = budget.try_fork_query(credits)?;
            let descriptor = (*descriptor).clone();
            let ordinal = *ordinal;
            let generation = *generation;
            let task_executor = executor.clone();
            jobs.push((
                ordinal,
                Box::pin(async move {
                    decode_gate_block(
                        &mut task_loader,
                        &task_executor,
                        generation,
                        &descriptor,
                        kind,
                        block_limits,
                        &mut task_credits,
                        &mut task_budget,
                    )
                    .await
                }) as QueryPartitionJob<_>,
            ));
        }
        if jobs.len() == blocks.len() {
            decoded = executor.execute_ordered(jobs).await?;
        }
    }
    if decoded.is_empty() && !blocks.is_empty() {
        for (ordinal, generation, descriptor) in &blocks {
            decoded.push((
                *ordinal,
                decode_gate_block(
                    loader,
                    executor,
                    *generation,
                    descriptor,
                    kind,
                    block_limits,
                    credits,
                    budget,
                )
                .await?,
            ));
        }
    }
    if decoded.len() != blocks.len()
        || decoded
            .iter()
            .enumerate()
            .any(|(expected, (actual, _))| expected != *actual)
    {
        return Err(IndexError::Integrity);
    }
    for (_, block_gates) in decoded {
        for gate in block_gates {
            let bytes = resident_gate_bytes(&gate)?;
            if realtime_gate_is_absorbed(&gate, manifest.view.pin.root.next_offset) {
                budget.release_heap(credits, bytes)?;
                continue;
            }
            if gates.contains_key(&gate.document) {
                budget.release_heap(credits, bytes)?;
            } else {
                gates.insert(gate.document, gate);
            }
        }
    }
    budget.candidates(gates.len())?;
    Ok(gates)
}

#[allow(clippy::too_many_arguments)]
async fn load_latest_gates_for_keys<L: QueryArtifactLoader, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    manifest: &PartitionManifest,
    recipe: RecipeIdentity,
    kind: QueryBlockKind,
    keys: &BTreeSet<StableDocumentKey>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<AlignedGates, IndexError> {
    if keys.is_empty() {
        return Ok(AlignedGates::default());
    }
    let key_index_bytes = keys
        .len()
        .checked_mul(std::mem::size_of::<StableDocumentKey>())
        .ok_or(IndexError::OffsetOverflow)?;
    let slot_bytes = keys
        .len()
        .checked_mul(std::mem::size_of::<Option<QueryDocumentGate>>())
        .ok_or(IndexError::OffsetOverflow)?;
    budget.reserve_heap(
        credits,
        key_index_bytes
            .checked_add(slot_bytes)
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let mut ordered_keys = Vec::with_capacity(keys.len());
    ordered_keys.extend(keys.iter().copied());
    let mut gates = std::iter::repeat_with(|| None)
        .take(ordered_keys.len())
        .collect::<Vec<_>>();
    let mut resident_bytes = slot_bytes;
    let mut found = 0usize;
    for (run_index, descriptor) in manifest.matching_blocks(kind, recipe) {
        let minimum = StableDocumentKey::from_bytes(
            descriptor
                .minimum_key
                .as_slice()
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        )?;
        let maximum = StableDocumentKey::from_bytes(
            descriptor
                .maximum_key
                .as_slice()
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        )?;
        let first = ordered_keys.partition_point(|key| *key < minimum);
        let end = ordered_keys.partition_point(|key| *key <= maximum);
        if first == end {
            continue;
        }
        let generation = manifest
            .runs
            .get(run_index)
            .ok_or(IndexError::Integrity)?
            .physical_catalog_generation;
        let block = load_decoded_block(
            loader,
            executor,
            generation,
            descriptor,
            block_limits,
            credits,
            budget,
        )
        .await?;
        let result = (|| {
            let first_candidate_bytes = ordered_keys[first].bytes();
            let mut records = block.records_from(&first_candidate_bytes).peekable();
            let mut candidate = first;
            while let Some(record) = records.peek().copied() {
                if candidate == end {
                    break;
                }
                if gates[candidate].is_some() {
                    candidate += 1;
                    continue;
                }
                let key = ordered_keys[candidate];
                match record.key.cmp(key.bytes().as_slice()) {
                    Ordering::Less => {
                        records.next();
                    }
                    Ordering::Greater => {
                        candidate += 1;
                    }
                    Ordering::Equal => {
                        let gate = decode_document_gate(record)?;
                        if gate.document != key
                            || (kind == QueryBlockKind::Gate) != gate.source_path.is_some()
                        {
                            return Err(IndexError::Integrity);
                        }
                        if realtime_gate_is_absorbed(&gate, manifest.view.pin.root.next_offset) {
                            records.next();
                            candidate += 1;
                            continue;
                        }
                        let dynamic_bytes = resident_gate_dynamic_bytes(&gate)?;
                        budget.reserve_heap(credits, dynamic_bytes)?;
                        resident_bytes = resident_bytes
                            .checked_add(dynamic_bytes)
                            .ok_or(IndexError::OffsetOverflow)?;
                        gates[candidate] = Some(gate);
                        found += 1;
                        records.next();
                        candidate += 1;
                    }
                }
            }
            Ok(())
        })();
        let release = credits.release_loaded_block(block.encoded_bytes());
        result?;
        release?;
        if found == ordered_keys.len() {
            break;
        }
    }
    budget.release_heap(credits, key_index_bytes)?;
    Ok(AlignedGates {
        gates,
        resident_bytes,
    })
}

#[path = "query_executor_predicate.rs"]
mod predicate;
use predicate::evaluate_predicate;

#[allow(clippy::too_many_arguments)]
async fn seek_terms<L: QueryArtifactLoader + 'static, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    manifest: &PartitionManifest,
    recipe: RecipeIdentity,
    exact: &[ScalarValue],
    prefix: Option<&str>,
    minimum_document_exclusive: Option<StableDocumentKey>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<BTreeMap<StableDocumentKey, QueryMaterialCandidate>, IndexError> {
    let newest = seek_term_postings(
        loader,
        executor,
        manifest,
        recipe,
        exact,
        prefix,
        minimum_document_exclusive,
        block_limits,
        credits,
        budget,
    )
    .await?;
    let input_bytes = term_posting_map_bytes(&newest)?;
    let input_count = newest.values().try_fold(0usize, |count, entries| {
        count
            .checked_add(entries.len())
            .ok_or(IndexError::OffsetOverflow)
    })?;
    budget.reserve_heap(credits, candidate_set_bytes(input_count)?)?;
    let mut output = BTreeMap::<StableDocumentKey, QueryMaterialCandidate>::new();
    for postings in newest.into_values() {
        for (key, (_, posting)) in postings {
            if posting.live {
                output
                    .entry(key)
                    .and_modify(|candidate| {
                        if posting.material_source_version > candidate.material_source_version {
                            *candidate = QueryMaterialCandidate::from(posting.clone());
                        }
                    })
                    .or_insert_with(|| QueryMaterialCandidate::from(posting));
            }
        }
        budget.candidates(output.len())?;
    }
    budget.release_heap(
        credits,
        candidate_set_bytes(input_count)?
            .checked_sub(candidate_set_bytes(output.len())?)
            .ok_or(IndexError::Integrity)?,
    )?;
    budget.release_heap(credits, input_bytes)?;
    Ok(output)
}

type TermPostingMap =
    BTreeMap<ScalarValue, BTreeMap<StableDocumentKey, (usize, DenseQueryPosting)>>;

fn term_posting_map_bytes(postings: &TermPostingMap) -> Result<usize, IndexError> {
    postings.iter().try_fold(0usize, |bytes, (term, entries)| {
        entries
            .len()
            .checked_mul(posting_entry_bytes())
            .and_then(|entry_bytes| entry_bytes.checked_add(4096))
            .and_then(|entry_bytes| entry_bytes.checked_add(resident_scalar_bytes(term)))
            .and_then(|entry_bytes| bytes.checked_add(entry_bytes))
            .ok_or(IndexError::OffsetOverflow)
    })
}

#[allow(clippy::too_many_arguments)]
async fn seek_term_postings<L: QueryArtifactLoader + 'static, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    manifest: &PartitionManifest,
    recipe: RecipeIdentity,
    exact: &[ScalarValue],
    prefix: Option<&str>,
    minimum_document_exclusive: Option<StableDocumentKey>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<TermPostingMap, IndexError> {
    let mut newest = TermPostingMap::new();
    let mut work = Vec::new();
    let mut selected_term_bytes = 0usize;
    let dictionaries = manifest
        .matching_blocks(QueryBlockKind::TermDictionary, recipe)
        .enumerate()
        .map(|(ordinal, (run, descriptor))| {
            Ok((
                ordinal,
                run,
                manifest
                    .runs
                    .get(run)
                    .ok_or(IndexError::Integrity)?
                    .physical_catalog_generation,
                descriptor.clone(),
            ))
        })
        .collect::<Result<Vec<_>, IndexError>>()?;
    let mut selected_terms = Vec::with_capacity(dictionaries.len());
    if dictionaries.len() > 1 && executor.maximum_parallelism() > 1 {
        let parallel_exact = Arc::<[ScalarValue]>::from(exact);
        let parallel_prefix = prefix.map(Arc::<str>::from);
        let parallel_input_bytes = parallel_exact
            .iter()
            .try_fold(
                parallel_exact
                    .len()
                    .checked_mul(std::mem::size_of::<ScalarValue>())
                    .ok_or(IndexError::OffsetOverflow)?,
                |bytes, value| {
                    bytes
                        .checked_add(resident_scalar_bytes(value))
                        .ok_or(IndexError::OffsetOverflow)
                },
            )?
            .checked_add(parallel_prefix.as_ref().map_or(0, |value| value.len()))
            .ok_or(IndexError::OffsetOverflow)?;
        let parallel_input_charge = budget.reserve_heap_scoped(credits, parallel_input_bytes)?;
        let mut jobs = Vec::with_capacity(dictionaries.len());
        for (ordinal, _, generation, descriptor) in &dictionaries {
            let Some(mut task_loader) = loader.try_fork_query_loader()? else {
                jobs.clear();
                break;
            };
            let exact = parallel_exact.clone();
            let prefix = parallel_prefix.clone();
            let descriptor = (*descriptor).clone();
            let generation = *generation;
            let ordinal = *ordinal;
            let (mut task_budget, mut task_credits) = budget.try_fork_query(credits)?;
            let task_executor = executor.clone();
            jobs.push((
                ordinal,
                Box::pin(async move {
                    load_selected_terms(
                        &mut task_loader,
                        &task_executor,
                        generation,
                        &descriptor,
                        exact.as_ref(),
                        prefix.as_deref(),
                        block_limits,
                        &mut task_credits,
                        &mut task_budget,
                    )
                    .await
                }) as QueryPartitionJob<_>,
            ));
        }
        if jobs.len() == dictionaries.len() {
            selected_terms = executor.execute_ordered(jobs).await?;
        }
        drop(parallel_exact);
        drop(parallel_prefix);
        parallel_input_charge.release()?;
    }
    if selected_terms.is_empty() && !dictionaries.is_empty() {
        for (ordinal, _, generation, descriptor) in &dictionaries {
            selected_terms.push((
                *ordinal,
                load_selected_terms(
                    loader,
                    executor,
                    *generation,
                    descriptor,
                    exact,
                    prefix,
                    block_limits,
                    credits,
                    budget,
                )
                .await?,
            ));
        }
    }
    if selected_terms.len() != dictionaries.len()
        || selected_terms
            .iter()
            .enumerate()
            .any(|(expected, (actual, _))| dictionaries[expected].0 != *actual)
    {
        return Err(IndexError::Integrity);
    }
    for (ordinal, (entries, entry_bytes)) in selected_terms {
        let (_, run, _, _) = dictionaries.get(ordinal).ok_or(IndexError::Integrity)?;
        let run = *run;
        selected_term_bytes = selected_term_bytes
            .checked_add(entry_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        for entry in entries {
            if !newest.contains_key(&entry.term) {
                budget.reserve_heap(
                    credits,
                    4096usize
                        .checked_add(resident_scalar_bytes(&entry.term))
                        .ok_or(IndexError::OffsetOverflow)?,
                )?;
                newest.insert(entry.term.clone(), BTreeMap::new());
            }
            for shard in entry.posting_shards {
                if minimum_document_exclusive
                    .is_some_and(|minimum| shard.maximum_document <= minimum)
                {
                    continue;
                }
                let posting_descriptor = manifest.find_run_block(
                    run,
                    shard.posting_block_hash,
                    QueryBlockKind::Posting,
                    recipe,
                )?;
                if posting_descriptor.records != shard.posting_records
                    || posting_descriptor.minimum_key != shard.minimum_document.bytes()
                    || posting_descriptor.maximum_key != shard.maximum_document.bytes()
                {
                    return Err(IndexError::Integrity);
                }
                work.push(PostingBlockWork {
                    term: entry.term.clone(),
                    run,
                    generation: manifest
                        .runs
                        .get(run)
                        .ok_or(IndexError::Integrity)?
                        .physical_catalog_generation,
                    descriptor: posting_descriptor.clone(),
                    minimum_document: shard.minimum_document,
                    maximum_document: shard.maximum_document,
                });
            }
            if newest.len() > budget.limits().maximum_expanded_terms {
                return resource(newest.len(), budget.limits().maximum_expanded_terms);
            }
        }
    }
    let mut decoded = Vec::with_capacity(work.len());
    if work.len() > 1 && executor.maximum_parallelism() > 1 {
        let mut jobs = Vec::with_capacity(work.len());
        for (ordinal, item) in work.iter().enumerate() {
            let Some(mut task_loader) = loader.try_fork_query_loader()? else {
                jobs.clear();
                break;
            };
            let descriptor = item.descriptor.clone();
            let generation = item.generation;
            let minimum = item.minimum_document;
            let maximum = item.maximum_document;
            let (mut task_budget, mut task_credits) = budget.try_fork_query(credits)?;
            let task_executor = executor.clone();
            jobs.push((
                ordinal,
                Box::pin(async move {
                    decode_posting_block(
                        &mut task_loader,
                        &task_executor,
                        generation,
                        &descriptor,
                        minimum,
                        maximum,
                        minimum_document_exclusive,
                        block_limits,
                        &mut task_credits,
                        &mut task_budget,
                    )
                    .await
                }) as QueryPartitionJob<_>,
            ));
        }
        if jobs.len() == work.len() {
            decoded = executor.execute_ordered(jobs).await?;
        }
    }
    if decoded.is_empty() && !work.is_empty() {
        for (ordinal, item) in work.iter().enumerate() {
            decoded.push((
                ordinal,
                decode_posting_block(
                    loader,
                    executor,
                    item.generation,
                    &item.descriptor,
                    item.minimum_document,
                    item.maximum_document,
                    minimum_document_exclusive,
                    block_limits,
                    credits,
                    budget,
                )
                .await?,
            ));
        }
    }
    if decoded.len() != work.len()
        || decoded
            .iter()
            .enumerate()
            .any(|(expected, (actual, _))| expected != *actual)
    {
        return Err(IndexError::Integrity);
    }
    for (ordinal, postings) in decoded {
        let item = work.get(ordinal).ok_or(IndexError::Integrity)?;
        let term = newest.get_mut(&item.term).ok_or(IndexError::Integrity)?;
        for posting in postings {
            let document = posting.document()?;
            if term.contains_key(&document) {
                budget.release_heap(credits, posting_entry_bytes())?;
            } else {
                term.insert(document, (item.run, posting));
            }
        }
    }
    budget.release_heap(credits, selected_term_bytes)?;
    Ok(newest)
}

#[allow(clippy::too_many_arguments)]
async fn seek_range<L: QueryArtifactLoader + 'static, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    manifest: &PartitionManifest,
    recipe: RecipeIdentity,
    lower: Option<&RangeBound>,
    upper: Option<&RangeBound>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<BTreeMap<StableDocumentKey, QueryMaterialCandidate>, IndexError> {
    let mut newest =
        BTreeMap::<(ScalarValue, StableDocumentKey), (bool, QueryMaterialCandidate)>::new();
    let lower_key = lower
        .map(|bound| encode_scalar_sort_key(&bound.value))
        .transpose()?;
    let upper_key = upper
        .map(|bound| {
            let mut key = encode_scalar_sort_key(&bound.value)?;
            key.extend_from_slice(&[0xff; 32]);
            Ok::<_, IndexError>(key)
        })
        .transpose()?;
    let blocks = manifest
        .matching_blocks(QueryBlockKind::Point, recipe)
        .enumerate()
        .filter(|(_, (_, descriptor))| {
            point_descriptor_overlaps(descriptor, lower_key.as_deref(), upper_key.as_deref())
        })
        .map(|(ordinal, (run, descriptor))| {
            Ok((
                ordinal,
                manifest
                    .runs
                    .get(run)
                    .ok_or(IndexError::Integrity)?
                    .physical_catalog_generation,
                descriptor.clone(),
            ))
        })
        .collect::<Result<Vec<_>, IndexError>>()?;
    let mut decoded = Vec::with_capacity(blocks.len());
    if blocks.len() > 1 && executor.maximum_parallelism() > 1 {
        let mut jobs = Vec::with_capacity(blocks.len());
        for (ordinal, generation, descriptor) in &blocks {
            let Some(mut task_loader) = loader.try_fork_query_loader()? else {
                jobs.clear();
                break;
            };
            let (mut task_budget, mut task_credits) = budget.try_fork_query(credits)?;
            let lower = lower.cloned();
            let upper = upper.cloned();
            let descriptor = (*descriptor).clone();
            let generation = *generation;
            let ordinal = *ordinal;
            let task_executor = executor.clone();
            jobs.push((
                ordinal,
                Box::pin(async move {
                    decode_range_block(
                        &mut task_loader,
                        &task_executor,
                        generation,
                        &descriptor,
                        lower.as_ref(),
                        upper.as_ref(),
                        block_limits,
                        &mut task_credits,
                        &mut task_budget,
                    )
                    .await
                }) as QueryPartitionJob<_>,
            ));
        }
        if jobs.len() == blocks.len() {
            decoded = executor.execute_ordered(jobs).await?;
        }
    }
    if decoded.is_empty() && !blocks.is_empty() {
        for (ordinal, generation, descriptor) in &blocks {
            decoded.push((
                *ordinal,
                decode_range_block(
                    loader,
                    executor,
                    *generation,
                    descriptor,
                    lower,
                    upper,
                    block_limits,
                    credits,
                    budget,
                )
                .await?,
            ));
        }
    }
    if decoded.len() != blocks.len()
        || decoded
            .iter()
            .enumerate()
            .any(|(expected, (actual, _))| blocks[expected].0 != *actual)
    {
        return Err(IndexError::Integrity);
    }
    for (_, points) in decoded {
        for point in points {
            let document = point.documents.document(point.point.document)?;
            let key = (point.point.value, document);
            if newest.contains_key(&key) {
                budget.release_heap(credits, resident_point_entry_bytes(&key.0))?;
            } else {
                if newest.is_empty() {
                    budget.reserve_heap(credits, 2048)?;
                }
                newest.insert(
                    key,
                    (
                        point.point.live,
                        QueryMaterialCandidate {
                            material_source_version: point.point.material_source_version,
                            segment: Some(SegmentCandidateIdentity {
                                documents: point.documents,
                                local_id: point.point.document,
                            }),
                        },
                    ),
                );
            }
        }
    }
    let input_count = newest.len();
    let input_bytes = newest.keys().try_fold(
        if input_count == 0 { 0usize } else { 2048usize },
        |bytes, (value, _)| {
            bytes
                .checked_add(resident_point_entry_bytes(value))
                .ok_or(IndexError::OffsetOverflow)
        },
    )?;
    budget.reserve_heap(credits, candidate_set_bytes(input_count)?)?;
    let mut output = BTreeMap::<StableDocumentKey, QueryMaterialCandidate>::new();
    for ((_, key), (live, candidate)) in newest {
        if live {
            output
                .entry(key)
                .and_modify(|current| {
                    if candidate.material_source_version > current.material_source_version {
                        *current = candidate.clone();
                    }
                })
                .or_insert(candidate);
        }
    }
    budget.candidates(output.len())?;
    budget.release_heap(
        credits,
        candidate_set_bytes(input_count)?
            .checked_sub(candidate_set_bytes(output.len())?)
            .ok_or(IndexError::Integrity)?,
    )?;
    budget.release_heap(credits, input_bytes)?;
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
async fn verify_phrase<L: QueryArtifactLoader + 'static, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    manifest: &PartitionManifest,
    recipe: RecipeIdentity,
    terms: &[ScalarValue],
    postings: &TermPostingMap,
    candidates: &mut BTreeSet<StableDocumentKey>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<(), IndexError> {
    let mut positions = BTreeMap::<(ScalarValue, StableDocumentKey), Vec<u32>>::new();
    let mut resident_positions = 0usize;
    let mut work = Vec::new();
    for term in terms {
        let mut by_block = BTreeMap::<(usize, [u8; 32]), Vec<StableDocumentKey>>::new();
        for (document, (run, posting)) in postings.get(term).into_iter().flatten() {
            if posting.live && candidates.contains(document) {
                if let Some(hash) = posting.position_block_hash {
                    by_block.entry((*run, hash)).or_default().push(*document);
                }
            }
        }
        for ((run, hash), documents) in by_block {
            let descriptor =
                manifest.find_run_block(run, hash, QueryBlockKind::Position, recipe)?;
            work.push((
                term.clone(),
                manifest
                    .runs
                    .get(run)
                    .ok_or(IndexError::Integrity)?
                    .physical_catalog_generation,
                descriptor.clone(),
                documents,
            ));
        }
    }
    let mut decoded = Vec::with_capacity(work.len());
    if work.len() > 1 && executor.maximum_parallelism() > 1 {
        let mut jobs = Vec::with_capacity(work.len());
        for (ordinal, (term, generation, descriptor, documents)) in work.iter().enumerate() {
            let Some(mut task_loader) = loader.try_fork_query_loader()? else {
                jobs.clear();
                break;
            };
            let term = term.clone();
            let generation = *generation;
            let descriptor = (*descriptor).clone();
            let documents = documents.clone();
            let (mut task_budget, mut task_credits) = budget.try_fork_query(credits)?;
            let task_executor = executor.clone();
            jobs.push((
                ordinal,
                Box::pin(async move {
                    decode_position_block(
                        &mut task_loader,
                        &task_executor,
                        generation,
                        &descriptor,
                        &term,
                        &documents,
                        block_limits,
                        &mut task_credits,
                        &mut task_budget,
                    )
                    .await
                }) as QueryPartitionJob<_>,
            ));
        }
        if jobs.len() == work.len() {
            decoded = executor.execute_ordered(jobs).await?;
        }
    }
    if decoded.is_empty() && !work.is_empty() {
        for (ordinal, (term, generation, descriptor, documents)) in work.iter().enumerate() {
            decoded.push((
                ordinal,
                decode_position_block(
                    loader,
                    executor,
                    *generation,
                    descriptor,
                    term,
                    documents,
                    block_limits,
                    credits,
                    budget,
                )
                .await?,
            ));
        }
    }
    if decoded.len() != work.len()
        || decoded
            .iter()
            .enumerate()
            .any(|(expected, (actual, _))| expected != *actual)
    {
        return Err(IndexError::Integrity);
    }
    for (_, block_positions) in decoded {
        for (key, values, resident) in block_positions {
            if positions.insert(key, values).is_some() {
                return Err(IndexError::Integrity);
            }
            resident_positions = resident_positions
                .checked_add(resident)
                .ok_or(IndexError::OffsetOverflow)?;
        }
    }
    candidates.retain(|key| {
        let Some(first) = positions.get(&(terms[0].clone(), *key)) else {
            return false;
        };
        first.iter().any(|start| {
            terms.iter().enumerate().all(|(offset, term)| {
                positions.get(&(term.clone(), *key)).is_some_and(|values| {
                    values
                        .binary_search(&start.saturating_add(offset as u32))
                        .is_ok()
                })
            })
        })
    });
    budget.release_heap(credits, resident_positions)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn load_selected_terms<L: QueryArtifactLoader, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    generation: [u8; 32],
    descriptor: &QueryBlockDescriptor,
    exact: &[ScalarValue],
    prefix: Option<&str>,
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<(Vec<QueryTermEntry>, usize), IndexError> {
    let exact_keys = exact
        .iter()
        .map(encode_scalar_sort_key)
        .collect::<Result<Vec<_>, _>>()?;
    let prefix_start = prefix
        .map(|prefix| encode_scalar_sort_key(&ScalarValue::String(prefix.into())))
        .transpose()?;
    let prefix_end = prefix_start.as_ref().map(|start| {
        let mut end = start.clone();
        debug_assert_eq!(end.pop(), Some(0));
        debug_assert_eq!(end.pop(), Some(0));
        end.push(0xff);
        end
    });
    let relevant = exact_keys.iter().any(|key| {
        descriptor.minimum_key.as_slice() <= key.as_slice()
            && descriptor.maximum_key.as_slice() >= key.as_slice()
    }) || prefix_start.as_ref().zip(prefix_end.as_ref()).is_some_and(
        |(start, end)| {
            descriptor.maximum_key.as_slice() >= start.as_slice()
                && descriptor.minimum_key.as_slice() <= end.as_slice()
        },
    );
    if !relevant {
        return Ok((Vec::new(), 0));
    }
    let block = load_decoded_block(
        loader, executor, generation, descriptor, limits, credits, budget,
    )
    .await?;
    let exact = exact.to_vec();
    let prefix = prefix.map(str::to_owned);
    let records = descriptor.records as usize;
    let mut cpu_budget = budget.clone();
    let mut cpu_credits = credits.try_fork_query()?;
    executor
        .run_cpu(Box::new(move || {
            // Decoded terms own scalar strings and posting-shard vectors. Keep
            // the encoded size as a conservative bound for those allocations.
            let resident_bytes = records
                .checked_mul(std::mem::size_of::<QueryTermEntry>())
                .and_then(|bytes| bytes.checked_add(block.encoded_bytes()))
                .ok_or(IndexError::OffsetOverflow)?;
            cpu_budget.reserve_heap(&mut cpu_credits, resident_bytes)?;
            let mut output = Vec::new();
            for (term, key) in exact.iter().zip(&exact_keys) {
                if let Some(record) = block.records_from(key).next() {
                    let entry = decode_term_entry(record, limits)?;
                    if entry.term == *term {
                        output.push(entry);
                    }
                }
            }
            if let (Some(prefix), Some(start)) = (prefix.as_deref(), prefix_start) {
                for record in block.records_from(&start) {
                    let entry = decode_term_entry(record, limits)?;
                    match &entry.term {
                        ScalarValue::String(term) if term.starts_with(prefix) => output.push(entry),
                        _ => break,
                    }
                    if output.len() > cpu_budget.limits().maximum_expanded_terms {
                        return resource(output.len(), cpu_budget.limits().maximum_expanded_terms);
                    }
                }
            }
            output.sort_unstable_by(|left, right| left.term.cmp(&right.term));
            output.dedup_by(|left, right| left.term == right.term);
            cpu_credits.release_loaded_block(block.encoded_bytes())?;
            Ok((output, resident_bytes))
        }))
        .await
}

#[allow(clippy::too_many_arguments)]
async fn load_candidate_doc_values<L: QueryArtifactLoader, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    manifest: &PartitionManifest,
    recipe: RecipeIdentity,
    candidates: &[QueryCandidate],
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<(Vec<Option<Option<Vec<ScalarValue>>>>, usize), IndexError> {
    let slot_bytes = candidates
        .len()
        .checked_mul(std::mem::size_of::<Option<Option<Vec<ScalarValue>>>>())
        .ok_or(IndexError::OffsetOverflow)?;
    budget.reserve_heap(credits, slot_bytes)?;
    let mut resident_bytes = slot_bytes;
    let mut output = vec![None; candidates.len()];
    for (run, descriptor) in manifest.matching_blocks(QueryBlockKind::DocValue, recipe) {
        let minimum = StableDocumentKey::from_bytes(
            descriptor
                .minimum_key
                .as_slice()
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        )?;
        let maximum_key = StableDocumentKey::from_bytes(
            descriptor
                .maximum_key
                .as_slice()
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        )?;
        let candidate_start = candidates.partition_point(|candidate| candidate.document < minimum);
        let candidate_end =
            candidates.partition_point(|candidate| candidate.document <= maximum_key);
        if candidate_start == candidate_end {
            continue;
        }
        let block = load_decoded_block(
            loader,
            executor,
            manifest
                .runs
                .get(run)
                .ok_or(IndexError::Integrity)?
                .physical_catalog_generation,
            descriptor,
            block_limits,
            credits,
            budget,
        )
        .await?;
        let cpu_candidates = candidates[candidate_start..candidate_end].to_vec();
        let candidate_bytes = cpu_candidates
            .len()
            .checked_mul(std::mem::size_of::<QueryCandidate>())
            .ok_or(IndexError::OffsetOverflow)?;
        let candidate_charge = budget.reserve_heap_scoped(credits, candidate_bytes)?;
        let mut cpu_budget = budget.clone();
        let mut cpu_credits = credits.try_fork_query()?;
        let decoded = executor
            .run_cpu(Box::new(move || {
                let mut decoded = Vec::new();
                for (relative, candidate) in cpu_candidates.iter().enumerate() {
                    if let Ok(local) = block.documents().id(candidate.document)
                        && let Some(value) = block.doc_value(local, block_limits)?
                    {
                        if value.document == candidate.document
                            && value.material_source_version == candidate.material_source_version
                        {
                            let resident = value
                                .value
                                .as_ref()
                                .map(|values| {
                                    values.iter().try_fold(0usize, |bytes, value| {
                                        bytes
                                            .checked_add(resident_scalar_bytes(value))
                                            .ok_or(IndexError::OffsetOverflow)
                                    })
                                })
                                .transpose()?
                                .unwrap_or(0);
                            cpu_budget.reserve_heap(&mut cpu_credits, resident)?;
                            decoded.push((relative, value.value, resident));
                        }
                    }
                }
                cpu_credits.release_loaded_block(block.encoded_bytes())?;
                Ok(decoded)
            }))
            .await?;
        candidate_charge.release()?;
        for (relative, value, resident) in decoded {
            let output_index = candidate_start + relative;
            if output[output_index].is_some() {
                budget.release_heap(credits, resident)?;
            } else {
                resident_bytes = resident_bytes
                    .checked_add(resident)
                    .ok_or(IndexError::OffsetOverflow)?;
                output[output_index] = Some(value);
            }
        }
    }
    Ok((output, resident_bytes))
}

#[cfg(test)]
#[path = "query_executor_tests.rs"]
mod tests;
