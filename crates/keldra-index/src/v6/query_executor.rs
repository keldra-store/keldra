use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use crate::IndexError;
use crate::typed_json::{
    AggregateRequest, AggregateResult, Cardinality, FacetBucket, FacetRequest, FacetResult,
    FieldCapabilities, FieldId, FieldSchema, OrderDirection, OrderField, Predicate, RangeBound,
    ScalarValue, analyze_typed_json_text, encode_scalar_sort_key,
};

use super::{
    LogicalProjectionBinding, MAX_QUERY_DOCUMENT_PATH_BYTES, ProjectionPartitionIdentity,
    ProjectionQueryRunDescriptor, ProjectionQueryStreamRoot, QueryBlockCredits, QueryBlockCursor,
    QueryBlockDescriptor, QueryBlockKind, QueryBlockLimits, QueryDocumentGate, QueryPosting,
    QueryRecipeCatalogProof, QueryRunChild, QueryRunPage, QueryRunReference, QueryTermEntry,
    RecipeIdentity, StableDocumentKey, decode_doc_value, decode_document_gate, decode_point,
    decode_positions, decode_posting, decode_projection_query_run, decode_query_run_page,
    decode_term_entry,
};

#[path = "query_executor_admission.rs"]
mod admission;
use admission::match_all_live_documents;
pub use admission::{
    AuthorizedQueryCandidate, PinnedPartitionQueryRoot, QueryAdmissionCandidate,
    QueryAdmissionContext, QueryArtifactKind, QueryArtifactLoad, QueryCandidateAdmission,
    QueryCommonCut, QueryRootCutProof,
};
#[path = "query_executor_values.rs"]
mod values;
use values::{
    leaf_field, reduce_streaming, resident_scalar_bytes, resource, validate_leaf_capability,
    verify_hash,
};

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
            maximum_partitions: 1_024,
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
                "v6 query execution limits are invalid".into(),
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
    pub recipe_catalog_proofs: Vec<QueryRecipeCatalogProof>,
    /// Absence is the public match-all contract over live membership gates.
    pub predicate: Option<Predicate>,
    pub order: Vec<OrderField>,
    pub facets: Vec<FacetRequest>,
    pub aggregates: Vec<AggregateRequest>,
    pub result_limit: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueryCandidate {
    pub partition: ProjectionPartitionIdentity,
    pub document: StableDocumentKey,
    pub material_source_version: u64,
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

pub trait QueryArtifactLoader: Send {
    fn query_artifact_size(
        &mut self,
        kind: QueryArtifactKind,
        hash: [u8; 32],
    ) -> impl std::future::Future<Output = Result<usize, IndexError>> + Send;
    fn load_query_artifact(
        &mut self,
        request: QueryArtifactLoad,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, IndexError>> + Send;
}

struct Budget {
    limits: QueryExecutionLimits,
    evidence: QueryLoadEvidence,
    heap_bytes: usize,
}

impl Budget {
    fn load(&mut self, kind: QueryArtifactKind, bytes: usize) -> Result<(), IndexError> {
        let counter = match kind {
            QueryArtifactKind::Page => &mut self.evidence.pages,
            QueryArtifactKind::Run => &mut self.evidence.runs,
            QueryArtifactKind::Block => &mut self.evidence.blocks,
        };
        let limit = match kind {
            QueryArtifactKind::Page => self.limits.maximum_page_loads,
            QueryArtifactKind::Run => self.limits.maximum_run_loads,
            QueryArtifactKind::Block => self.limits.maximum_block_loads,
        };
        *counter = counter.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        if *counter > limit {
            return resource(*counter, limit);
        }
        self.evidence.bytes = self
            .evidence
            .bytes
            .checked_add(bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        if self.evidence.bytes > self.limits.maximum_loaded_bytes {
            return resource(self.evidence.bytes, self.limits.maximum_loaded_bytes);
        }
        Ok(())
    }

    fn candidates(&self, count: usize) -> Result<(), IndexError> {
        if count > self.limits.maximum_candidates {
            resource(count, self.limits.maximum_candidates)
        } else {
            Ok(())
        }
    }

    fn reserve_heap(
        &mut self,
        credits: &mut QueryBlockCredits,
        bytes: usize,
    ) -> Result<(), IndexError> {
        let next = self
            .heap_bytes
            .checked_add(bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        if next > self.limits.maximum_heap_bytes {
            return resource(next, self.limits.maximum_heap_bytes);
        }
        credits.reserve(bytes)?;
        self.heap_bytes = next;
        Ok(())
    }

    fn release_heap(
        &mut self,
        credits: &mut QueryBlockCredits,
        bytes: usize,
    ) -> Result<(), IndexError> {
        self.heap_bytes = self
            .heap_bytes
            .checked_sub(bytes)
            .ok_or(IndexError::Integrity)?;
        credits.release(bytes)
    }
}

struct PartitionView<'a> {
    pin: PinnedPartitionQueryRoot,
    recipe_catalog_proofs: &'a [QueryRecipeCatalogProof],
}

struct QueryRunStream {
    stack: Vec<QueryRunChild>,
    pending: Vec<QueryRunReference>,
    emitted: u64,
    expected_runs: u64,
    previous_sequence: Option<u64>,
}

async fn load_pre_admitted<L: QueryArtifactLoader>(
    loader: &mut L,
    kind: QueryArtifactKind,
    hash: [u8; 32],
    maximum_bytes: usize,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Vec<u8>, IndexError> {
    let encoded_bytes = loader.query_artifact_size(kind, hash).await?;
    if encoded_bytes == 0 || encoded_bytes > maximum_bytes {
        return resource(encoded_bytes, maximum_bytes);
    }
    budget.load(kind, encoded_bytes)?;
    credits.reserve(encoded_bytes)?;
    let request = QueryArtifactLoad {
        kind,
        hash,
        encoded_bytes,
    };
    let loaded = loader.load_query_artifact(request).await;
    let bytes = match loaded {
        Ok(bytes) => bytes,
        Err(error) => {
            credits.release(encoded_bytes)?;
            return Err(error);
        }
    };
    if bytes.len() != encoded_bytes || verify_hash(hash, &bytes).is_err() {
        credits.release(encoded_bytes)?;
        return Err(IndexError::Integrity);
    }
    Ok(bytes)
}

impl QueryRunStream {
    fn new(root: ProjectionQueryStreamRoot) -> Self {
        let mut stack = Vec::new();
        if root.run_count != 0 {
            stack.push(QueryRunChild {
                hash: root.stream_root_hash,
                run_count: root.run_count,
                first_sequence: root.first_sequence,
                last_sequence: root.last_sequence,
                source_start_offset: root.source_start_offset,
                next_offset: root.next_offset,
                through_atomic_position: root.through_atomic_position,
            });
        }
        Self {
            stack,
            pending: Vec::new(),
            emitted: 0,
            expected_runs: root.run_count,
            previous_sequence: None,
        }
    }

    async fn next<L: QueryArtifactLoader>(
        &mut self,
        loader: &mut L,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<Option<QueryRunReference>, IndexError> {
        loop {
            if let Some(reference) = self.pending.pop() {
                if self
                    .previous_sequence
                    .is_some_and(|sequence| sequence <= reference.sequence)
                {
                    return Err(IndexError::Integrity);
                }
                self.previous_sequence = Some(reference.sequence);
                self.emitted = self
                    .emitted
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                if self.emitted > self.expected_runs {
                    return Err(IndexError::Integrity);
                }
                return Ok(Some(reference));
            }
            let Some(expected) = self.stack.pop() else {
                if self.emitted != self.expected_runs {
                    return Err(IndexError::Integrity);
                }
                return Ok(None);
            };
            let maximum = budget.limits.maximum_page_bytes;
            let bytes = load_pre_admitted(
                loader,
                QueryArtifactKind::Page,
                expected.hash,
                maximum,
                credits,
                budget,
            )
            .await?;
            budget.reserve_heap(credits, bytes.len())?;
            let page = decode_query_run_page(&bytes)?;
            credits.release(bytes.len())?;
            if page_summary(expected.hash, &page)? != expected {
                return Err(IndexError::Integrity);
            }
            match page {
                QueryRunPage::Leaf(runs) => self.pending.extend(runs),
                QueryRunPage::Branch(children) => self.stack.extend(children),
            }
        }
    }
}

pub async fn execute_typed_json_query<L: QueryArtifactLoader, A: QueryCandidateAdmission>(
    loader: &mut L,
    admission: &mut A,
    common_cut: QueryCommonCut,
    pins: &[PinnedPartitionQueryRoot],
    request: &TypedJsonQueryRequest,
    execution_limits: QueryExecutionLimits,
    block_limits: QueryBlockLimits,
    block_credits: &mut QueryBlockCredits,
) -> Result<TypedJsonQueryResult, IndexError> {
    let execution_limits = execution_limits.validate()?;
    let block_limits = block_limits.validate()?;
    let contracts = validate_request(common_cut, pins, request, execution_limits)?;
    let mut budget = Budget {
        limits: execution_limits,
        evidence: QueryLoadEvidence::default(),
        heap_bytes: 0,
    };
    budget.reserve_heap(
        block_credits,
        pins.len()
            .checked_mul(std::mem::size_of::<PartitionView>())
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let views = pins
        .iter()
        .copied()
        .map(|pin| PartitionView {
            pin,
            recipe_catalog_proofs: &request.recipe_catalog_proofs,
        })
        .collect::<Vec<_>>();

    let mut selected = BTreeMap::<StableDocumentKey, QueryAdmissionCandidate>::new();
    for view in &views {
        let mut gates = load_latest_gates(
            loader,
            view,
            request.logical.membership,
            QueryBlockKind::Gate,
            block_limits,
            block_credits,
            &mut budget,
        )
        .await?;
        let keys = if let Some(predicate) = request.predicate.as_ref() {
            evaluate_predicate(
                loader,
                view,
                &gates,
                &contracts,
                predicate,
                block_limits,
                block_credits,
                &mut budget,
            )
            .await?
        } else {
            match_all_live_documents(&gates)
        };
        let key_bytes = request
            .predicate
            .as_ref()
            .map(|_| {
                keys.len()
                    .checked_mul(std::mem::size_of::<StableDocumentKey>())
                    .ok_or(IndexError::OffsetOverflow)
            })
            .transpose()?;
        for document in keys {
            let gate = gates.remove(&document).ok_or(IndexError::Integrity)?;
            let gate_bytes = resident_gate_bytes(&gate)?;
            if gate.live {
                let candidate = QueryAdmissionCandidate {
                    partition: view.pin.partition,
                    handoff_lineage_id: view.pin.handoff_lineage_id,
                    covered_through_source_position: view.pin.covered_through_source_position()?,
                    document,
                    material_source_version: gate.material_source_version,
                    current_source_version: gate.current_source_version,
                    source_path: gate.source_path.ok_or(IndexError::Integrity)?,
                    result_path: gate.result_path.ok_or(IndexError::Integrity)?,
                    result_version: gate.result_version,
                };
                select_handoff_candidate(&mut selected, candidate, block_credits, &mut budget)?;
                budget.candidates(selected.len())?;
            }
            budget.release_heap(block_credits, gate_bytes)?;
        }
        if let Some(bytes) = key_bytes {
            budget.release_heap(block_credits, bytes)?;
        }
        let remaining_gate_bytes = gates.values().try_fold(0usize, |total, gate| {
            total
                .checked_add(resident_gate_bytes(gate)?)
                .ok_or(IndexError::OffsetOverflow)
        })?;
        drop(gates);
        budget.release_heap(block_credits, remaining_gate_bytes)?;
    }

    let mut authorized = BTreeMap::new();
    let mut candidates = Vec::new();
    for candidate in selected.into_values() {
        let selected_bytes = resident_selected_candidate_bytes(&candidate)?;
        let context = QueryAdmissionContext {
            logical_index_id: request.logical.logical_index_id,
            logical_definition_version: request.logical.logical_definition_version,
            common_cut,
            candidate: candidate.clone(),
        };
        if let Some(admitted) = admission.admit_exact_current_authorized(context).await? {
            admitted.validate_for(&candidate)?;
            budget.reserve_heap(
                block_credits,
                std::mem::size_of::<AuthorizedQueryCandidate>()
                    .checked_add(std::mem::size_of::<QueryCandidate>())
                    .and_then(|bytes| {
                        bytes.checked_add(std::mem::size_of::<(
                            ProjectionPartitionIdentity,
                            StableDocumentKey,
                        )>())
                    })
                    .and_then(|bytes| bytes.checked_add(admitted.result_path.len()))
                    .and_then(|bytes| bytes.checked_add(candidate.source_path.len()))
                    .and_then(|bytes| bytes.checked_add(candidate.result_path.len()))
                    .ok_or(IndexError::OffsetOverflow)?,
            )?;
            let key = (candidate.partition, candidate.document);
            authorized.insert(key, admitted);
            candidates.push(QueryCandidate {
                partition: candidate.partition,
                document: candidate.document,
                material_source_version: candidate.material_source_version,
            });
        }
        budget.release_heap(block_credits, selected_bytes)?;
    }

    let needed_values = requested_value_recipes(request, &contracts)?;
    let mut values = BTreeMap::new();
    for view in &views {
        let partition_count = candidates
            .iter()
            .filter(|candidate| candidate.partition == view.pin.partition)
            .count();
        budget.reserve_heap(
            block_credits,
            partition_count
                .checked_mul(std::mem::size_of::<StableDocumentKey>())
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let partition_candidates = candidates
            .iter()
            .filter_map(|candidate| {
                (candidate.partition == view.pin.partition).then_some(candidate.document)
            })
            .collect::<BTreeSet<_>>();
        for recipe in &needed_values {
            let loaded = load_candidate_doc_values(
                loader,
                view,
                *recipe,
                &partition_candidates,
                block_limits,
                block_credits,
                &mut budget,
            )
            .await?;
            for (key, value) in loaded {
                budget.reserve_heap(
                    block_credits,
                    std::mem::size_of::<(
                        (
                            ProjectionPartitionIdentity,
                            StableDocumentKey,
                            RecipeIdentity,
                        ),
                        Option<Vec<ScalarValue>>,
                    )>(),
                )?;
                values.insert((view.pin.partition, key, *recipe), value);
            }
        }
    }

    order_candidates(&mut candidates, &request.order, &contracts, &values)?;
    let facets = facet_candidates(
        &candidates,
        &request.facets,
        &contracts,
        &values,
        block_credits,
        &mut budget,
    )?;
    let aggregates = aggregate_candidates(
        &candidates,
        &request.aggregates,
        &contracts,
        &values,
        block_credits,
        &mut budget,
    )?;
    candidates.truncate(request.result_limit);
    let candidates = candidates
        .into_iter()
        .map(|candidate| {
            authorized
                .remove(&(candidate.partition, candidate.document))
                .ok_or(IndexError::Integrity)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(TypedJsonQueryResult {
        through_atomic_position: common_cut.through_atomic_position,
        candidates,
        facets,
        aggregates,
        loads: budget.evidence,
    })
}

fn validate_request(
    common_cut: QueryCommonCut,
    pins: &[PinnedPartitionQueryRoot],
    request: &TypedJsonQueryRequest,
    limits: QueryExecutionLimits,
) -> Result<BTreeMap<FieldId, QueryFieldBinding>, IndexError> {
    if pins.is_empty() || pins.len() > limits.maximum_partitions {
        return resource(pins.len(), limits.maximum_partitions);
    }
    if let Some(predicate) = request.predicate.as_ref() {
        predicate.validate()?;
    }
    if request.result_limit == 0 || request.result_limit > limits.maximum_results {
        return resource(request.result_limit, limits.maximum_results);
    }
    if request.order.len() > limits.maximum_order_fields {
        return resource(request.order.len(), limits.maximum_order_fields);
    }
    if request.facets.len() > limits.maximum_facets {
        return resource(request.facets.len(), limits.maximum_facets);
    }
    if request.aggregates.len() > limits.maximum_aggregates {
        return resource(request.aggregates.len(), limits.maximum_aggregates);
    }
    let mut nodes = 0usize;
    if let Some(predicate) = request.predicate.as_ref() {
        count_predicate_nodes(predicate, &mut nodes)?;
    }
    if nodes > limits.maximum_boolean_nodes {
        return resource(nodes, limits.maximum_boolean_nodes);
    }
    if request.logical.logical_index_id == 0 || request.logical.logical_definition_version == 0 {
        return Err(IndexError::InvalidQuery(
            "logical query binding identity is zero".into(),
        ));
    }
    let mut partitions = BTreeSet::new();
    for pin in pins {
        pin.validate_at(common_cut)?;
        if pin.partition.family_id != request.logical.family_id
            || pin.physical_catalog_generation != request.logical.physical_catalog_generation
            || pin.physical_catalog_generation == [0; 32]
            || !partitions.insert(pin.partition)
        {
            return Err(IndexError::InvalidQuery(
                "v6 query roots are not one unique common-cut vector".into(),
            ));
        }
    }
    if request.recipe_catalog_proofs.is_empty()
        || request
            .recipe_catalog_proofs
            .windows(2)
            .any(|pair| pair[0].recipe >= pair[1].recipe)
    {
        return Err(IndexError::InvalidQuery(
            "v6 query recipe catalog proofs are absent or non-canonical".into(),
        ));
    }
    for proof in &request.recipe_catalog_proofs {
        proof.validate(request.logical.physical_catalog_generation)?;
    }
    let logical = request
        .logical
        .fields
        .iter()
        .map(|field| (field.public_field_id, field.recipe))
        .collect::<BTreeMap<_, _>>();
    if logical.len() != request.logical.fields.len() {
        return Err(IndexError::InvalidQuery(
            "logical query binding has duplicate public fields".into(),
        ));
    }
    let mut contracts = BTreeMap::new();
    for binding in &request.fields {
        binding.field.validate()?;
        if logical.get(&binding.field.id.get()) != Some(&binding.recipe)
            || contracts
                .insert(binding.field.id, binding.clone())
                .is_some()
        {
            return Err(IndexError::InvalidQuery(
                "v6 query field contract disagrees with its logical binding".into(),
            ));
        }
    }
    for recipe in std::iter::once(request.logical.membership)
        .chain(contracts.values().map(|binding| binding.recipe))
    {
        if recipe_proof(request, recipe).is_none() {
            return Err(IndexError::InvalidQuery(
                "v6 query recipe lacks a catalog-lineage proof".into(),
            ));
        }
    }
    Ok(contracts)
}

fn recipe_proof(
    request: &TypedJsonQueryRequest,
    recipe: RecipeIdentity,
) -> Option<&QueryRecipeCatalogProof> {
    request
        .recipe_catalog_proofs
        .binary_search_by_key(&recipe, |proof| proof.recipe)
        .ok()
        .map(|index| &request.recipe_catalog_proofs[index])
}

fn count_predicate_nodes(predicate: &Predicate, count: &mut usize) -> Result<(), IndexError> {
    *count = count.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                count_predicate_nodes(child, count)?;
            }
        }
        Predicate::Not(child) => count_predicate_nodes(child, count)?,
        _ => {}
    }
    Ok(())
}

async fn load_next_descriptor<L: QueryArtifactLoader>(
    loader: &mut L,
    view: &PartitionView<'_>,
    stream: &mut QueryRunStream,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Option<(ProjectionQueryRunDescriptor, usize)>, IndexError> {
    let Some(reference) = stream.next(loader, credits, budget).await? else {
        return Ok(None);
    };
    let bytes = load_pre_admitted(
        loader,
        QueryArtifactKind::Run,
        reference.hash,
        block_limits.maximum_run_descriptor_bytes,
        credits,
        budget,
    )
    .await?;
    let descriptor = decode_projection_query_run(&bytes, block_limits, credits)?;
    credits.release(bytes.len())?;
    if descriptor.partition != view.pin.partition
        || descriptor.sequence != reference.sequence
        || descriptor.source_start_offset != reference.source_start_offset
        || descriptor.next_offset != reference.next_offset
        || descriptor.through_atomic_position != reference.through_atomic_position
        || descriptor.through_atomic_position > view.pin.root.through_atomic_position
    {
        return Err(IndexError::Integrity);
    }
    for block in &descriptor.blocks {
        let proof = view
            .recipe_catalog_proofs
            .binary_search_by_key(&block.recipe, |proof| proof.recipe)
            .ok()
            .map(|index| &view.recipe_catalog_proofs[index])
            .ok_or(IndexError::Integrity)?;
        if !proof.accepts(descriptor.physical_catalog_generation) {
            return Err(IndexError::Integrity);
        }
    }
    Ok(Some((descriptor, bytes.len())))
}

fn page_summary(hash: [u8; 32], page: &QueryRunPage) -> Result<QueryRunChild, IndexError> {
    match page {
        QueryRunPage::Leaf(runs) => {
            let first = runs.first().ok_or(IndexError::Integrity)?;
            let last = runs.last().ok_or(IndexError::Integrity)?;
            Ok(QueryRunChild {
                hash,
                run_count: runs.len() as u64,
                first_sequence: first.sequence,
                last_sequence: last.sequence,
                source_start_offset: first.source_start_offset,
                next_offset: last.next_offset,
                through_atomic_position: last.through_atomic_position,
            })
        }
        QueryRunPage::Branch(children) => {
            let first = children.first().ok_or(IndexError::Integrity)?;
            let last = children.last().ok_or(IndexError::Integrity)?;
            Ok(QueryRunChild {
                hash,
                run_count: children.iter().try_fold(0u64, |sum, child| {
                    sum.checked_add(child.run_count)
                        .ok_or(IndexError::OffsetOverflow)
                })?,
                first_sequence: first.first_sequence,
                last_sequence: last.last_sequence,
                source_start_offset: first.source_start_offset,
                next_offset: last.next_offset,
                through_atomic_position: last.through_atomic_position,
            })
        }
    }
}

async fn load_latest_gates<L: QueryArtifactLoader>(
    loader: &mut L,
    view: &PartitionView<'_>,
    recipe: RecipeIdentity,
    kind: QueryBlockKind,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<BTreeMap<StableDocumentKey, QueryDocumentGate>, IndexError> {
    let mut gates = BTreeMap::new();
    let mut stream = QueryRunStream::new(view.pin.root);
    while let Some((run, run_bytes)) =
        load_next_descriptor(loader, view, &mut stream, block_limits, credits, budget).await?
    {
        for descriptor in run
            .blocks
            .iter()
            .filter(|block| block.kind == kind && block.recipe == recipe)
        {
            let (records, record_bytes) =
                load_block(loader, descriptor, block_limits, credits, budget).await?;
            for record in records {
                let gate = decode_document_gate(record.as_ref())?;
                if (kind == QueryBlockKind::Gate) != gate.source_path.is_some() {
                    return Err(IndexError::Integrity);
                }
                if !gates.contains_key(&gate.document) {
                    budget.reserve_heap(credits, resident_gate_bytes(&gate)?)?;
                    gates.insert(gate.document, gate);
                }
            }
            budget.release_heap(credits, record_bytes)?;
        }
        credits.release(run_bytes)?;
    }
    budget.candidates(gates.len())?;
    Ok(gates)
}

#[allow(clippy::too_many_arguments)]
fn evaluate_predicate<'a, L: QueryArtifactLoader + 'a>(
    loader: &'a mut L,
    view: &'a PartitionView<'_>,
    universe: &'a BTreeMap<StableDocumentKey, QueryDocumentGate>,
    contracts: &'a BTreeMap<FieldId, QueryFieldBinding>,
    predicate: &'a Predicate,
    block_limits: QueryBlockLimits,
    credits: &'a mut QueryBlockCredits,
    budget: &'a mut Budget,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<BTreeSet<StableDocumentKey>, IndexError>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let result = match predicate {
            Predicate::And(children) => {
                let mut children = children.iter();
                let first = children.next().ok_or_else(|| {
                    IndexError::InvalidQuery("Boolean predicate requires a child".into())
                })?;
                let mut output = evaluate_predicate(
                    loader,
                    view,
                    universe,
                    contracts,
                    first,
                    block_limits,
                    credits,
                    budget,
                )
                .await?;
                for child in children {
                    let next = evaluate_predicate(
                        loader,
                        view,
                        universe,
                        contracts,
                        child,
                        block_limits,
                        credits,
                        budget,
                    )
                    .await?;
                    output.retain(|key| next.contains(key));
                }
                output
            }
            Predicate::Or(children) => {
                let mut output = BTreeSet::new();
                for child in children {
                    let next = evaluate_predicate(
                        loader,
                        view,
                        universe,
                        contracts,
                        child,
                        block_limits,
                        credits,
                        budget,
                    )
                    .await?;
                    budget.reserve_heap(
                        credits,
                        next.len()
                            .checked_mul(std::mem::size_of::<StableDocumentKey>())
                            .ok_or(IndexError::OffsetOverflow)?,
                    )?;
                    output.extend(next);
                    budget.candidates(output.len())?;
                }
                output
            }
            Predicate::Not(child) => {
                let excluded = evaluate_predicate(
                    loader,
                    view,
                    universe,
                    contracts,
                    child,
                    block_limits,
                    credits,
                    budget,
                )
                .await?;
                budget.reserve_heap(
                    credits,
                    universe
                        .len()
                        .checked_mul(std::mem::size_of::<StableDocumentKey>())
                        .ok_or(IndexError::OffsetOverflow)?,
                )?;
                universe
                    .iter()
                    .filter_map(|(key, gate)| {
                        (gate.live && !excluded.contains(key)).then_some(*key)
                    })
                    .collect()
            }
            leaf => {
                evaluate_leaf(
                    loader,
                    view,
                    universe,
                    contracts,
                    leaf,
                    block_limits,
                    credits,
                    budget,
                )
                .await?
            }
        };
        budget.candidates(result.len())?;
        Ok(result)
    })
}

#[allow(clippy::too_many_arguments)]
async fn evaluate_leaf<L: QueryArtifactLoader>(
    loader: &mut L,
    view: &PartitionView<'_>,
    universe: &BTreeMap<StableDocumentKey, QueryDocumentGate>,
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
    predicate: &Predicate,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<BTreeSet<StableDocumentKey>, IndexError> {
    let field_id =
        leaf_field(predicate).ok_or_else(|| IndexError::InvalidQuery("expected leaf".into()))?;
    let binding = contracts
        .get(&field_id)
        .ok_or_else(|| IndexError::InvalidQuery("query field is not bound".into()))?;
    validate_leaf_capability(&binding.field, predicate)?;
    if matches!(predicate, Predicate::Exists { .. }) {
        let presence = load_latest_gates(
            loader,
            view,
            binding.recipe,
            QueryBlockKind::Presence,
            block_limits,
            credits,
            budget,
        )
        .await?;
        budget.reserve_heap(
            credits,
            presence
                .len()
                .checked_mul(std::mem::size_of::<StableDocumentKey>())
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        return Ok(presence
            .into_iter()
            .filter_map(|(key, gate)| {
                (gate.live && universe.get(&key).is_some_and(|membership| membership.live))
                    .then_some(key)
            })
            .collect());
    }
    let mut candidates = match predicate {
        Predicate::Equal { value, .. } => {
            seek_terms(
                loader,
                view,
                binding.recipe,
                std::slice::from_ref(value),
                None,
                block_limits,
                credits,
                budget,
            )
            .await?
        }
        Predicate::In { values, .. } => {
            seek_terms(
                loader,
                view,
                binding.recipe,
                values,
                None,
                block_limits,
                credits,
                budget,
            )
            .await?
        }
        Predicate::Prefix { prefix, .. } => {
            seek_terms(
                loader,
                view,
                binding.recipe,
                &[],
                Some(prefix),
                block_limits,
                credits,
                budget,
            )
            .await?
        }
        Predicate::FullText { text, .. } | Predicate::Phrase { text, .. } => {
            budget.reserve_heap(credits, text.len())?;
            let terms = analyze_typed_json_text(text)
                .into_iter()
                .map(ScalarValue::String)
                .collect::<Vec<_>>();
            if terms.is_empty() {
                return Ok(BTreeSet::new());
            }
            let mut intersection = None::<BTreeMap<_, _>>;
            for term in &terms {
                let found = seek_terms(
                    loader,
                    view,
                    binding.recipe,
                    std::slice::from_ref(term),
                    None,
                    block_limits,
                    credits,
                    budget,
                )
                .await?;
                match &mut intersection {
                    None => intersection = Some(found),
                    Some(output) => output.retain(|key, _| found.contains_key(key)),
                }
            }
            let mut found = intersection.unwrap_or_default();
            if matches!(predicate, Predicate::Phrase { .. }) {
                budget.reserve_heap(
                    credits,
                    found
                        .len()
                        .checked_mul(std::mem::size_of::<StableDocumentKey>())
                        .ok_or(IndexError::OffsetOverflow)?,
                )?;
                let mut phrase_candidates = found.keys().copied().collect();
                verify_phrase(
                    loader,
                    view,
                    binding.recipe,
                    &terms,
                    &mut phrase_candidates,
                    block_limits,
                    credits,
                    budget,
                )
                .await?;
                found.retain(|key, _| phrase_candidates.contains(key));
            }
            found
        }
        Predicate::Range { lower, upper, .. } => {
            seek_range(
                loader,
                view,
                binding.recipe,
                lower.as_ref(),
                upper.as_ref(),
                block_limits,
                credits,
                budget,
            )
            .await?
        }
        _ => return Err(IndexError::InvalidQuery("expected Typed JSON leaf".into())),
    };
    let presence = load_latest_gates(
        loader,
        view,
        binding.recipe,
        QueryBlockKind::Presence,
        block_limits,
        credits,
        budget,
    )
    .await?;
    candidates.retain(|key, material_source_version| {
        candidate_is_current(
            universe.get(key),
            presence.get(key),
            *material_source_version,
        )
    });
    budget.reserve_heap(
        credits,
        candidates
            .len()
            .checked_mul(std::mem::size_of::<StableDocumentKey>())
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    Ok(candidates.into_keys().collect())
}

fn candidate_is_current(
    membership: Option<&QueryDocumentGate>,
    presence: Option<&QueryDocumentGate>,
    candidate_material_version: u64,
) -> bool {
    membership.is_some_and(|gate| gate.live)
        && presence.is_some_and(|gate| {
            gate.live && candidate_material_version <= gate.material_source_version
        })
}

fn select_handoff_candidate(
    selected: &mut BTreeMap<StableDocumentKey, QueryAdmissionCandidate>,
    incoming: QueryAdmissionCandidate,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<(), IndexError> {
    use std::collections::btree_map::Entry;
    match selected.entry(incoming.document) {
        Entry::Vacant(entry) => {
            budget.reserve_heap(credits, resident_selected_candidate_bytes(&incoming)?)?;
            entry.insert(incoming);
        }
        Entry::Occupied(mut entry) => {
            let current = entry.get().clone();
            if current.handoff_lineage_id != incoming.handoff_lineage_id {
                return Err(IndexError::Integrity);
            }
            match incoming
                .covered_through_source_position
                .cmp(&current.covered_through_source_position)
            {
                Ordering::Greater => {
                    replace_selected_candidate_charge(credits, budget, &current, &incoming)?;
                    entry.insert(incoming);
                }
                Ordering::Equal
                    if incoming.material_source_version != current.material_source_version
                        || incoming.current_source_version != current.current_source_version
                        || incoming.source_path != current.source_path
                        || incoming.result_path != current.result_path
                        || incoming.result_version != current.result_version =>
                {
                    return Err(IndexError::Integrity);
                }
                Ordering::Equal if incoming.partition < current.partition => {
                    replace_selected_candidate_charge(credits, budget, &current, &incoming)?;
                    entry.insert(incoming);
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn resident_gate_bytes(gate: &QueryDocumentGate) -> Result<usize, IndexError> {
    std::mem::size_of::<StableDocumentKey>()
        .checked_add(std::mem::size_of::<QueryDocumentGate>())
        .and_then(|bytes| bytes.checked_add(gate.source_path.as_ref().map_or(0, String::len)))
        .and_then(|bytes| bytes.checked_add(gate.result_path.as_ref().map_or(0, String::len)))
        .ok_or(IndexError::OffsetOverflow)
}

fn resident_selected_candidate_bytes(
    candidate: &QueryAdmissionCandidate,
) -> Result<usize, IndexError> {
    std::mem::size_of::<StableDocumentKey>()
        .checked_add(std::mem::size_of::<QueryAdmissionCandidate>())
        .and_then(|bytes| bytes.checked_add(candidate.source_path.len()))
        .and_then(|bytes| bytes.checked_add(candidate.result_path.len()))
        .ok_or(IndexError::OffsetOverflow)
}

fn replace_selected_candidate_charge(
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
    current: &QueryAdmissionCandidate,
    incoming: &QueryAdmissionCandidate,
) -> Result<(), IndexError> {
    let current = resident_selected_candidate_bytes(current)?;
    let incoming = resident_selected_candidate_bytes(incoming)?;
    if incoming > current {
        budget.reserve_heap(credits, incoming - current)
    } else {
        budget.release_heap(credits, current - incoming)
    }
}

#[allow(clippy::too_many_arguments)]
async fn seek_terms<L: QueryArtifactLoader>(
    loader: &mut L,
    view: &PartitionView<'_>,
    recipe: RecipeIdentity,
    exact: &[ScalarValue],
    prefix: Option<&str>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<BTreeMap<StableDocumentKey, u64>, IndexError> {
    let mut newest = BTreeMap::<ScalarValue, BTreeMap<StableDocumentKey, QueryPosting>>::new();
    let mut stream = QueryRunStream::new(view.pin.root);
    while let Some((run, run_bytes)) =
        load_next_descriptor(loader, view, &mut stream, block_limits, credits, budget).await?
    {
        for descriptor in run
            .blocks
            .iter()
            .filter(|block| block.kind == QueryBlockKind::TermDictionary && block.recipe == recipe)
        {
            let (entries, entry_bytes) = load_selected_terms(
                loader,
                descriptor,
                exact,
                prefix,
                block_limits,
                credits,
                budget,
            )
            .await?;
            for entry in entries {
                let term = newest.entry(entry.term).or_default();
                for shard in entry.posting_shards {
                    let posting_descriptor = find_block(
                        &run,
                        shard.posting_block_hash,
                        QueryBlockKind::Posting,
                        recipe,
                    )?;
                    let (postings, posting_bytes) =
                        load_block(loader, posting_descriptor, block_limits, credits, budget)
                            .await?;
                    if postings.len() != shard.posting_records as usize {
                        return Err(IndexError::Integrity);
                    }
                    for record in postings {
                        let posting = decode_posting(record.as_ref())?;
                        if posting.document < shard.minimum_document
                            || posting.document > shard.maximum_document
                        {
                            return Err(IndexError::Integrity);
                        }
                        if !term.contains_key(&posting.document) {
                            budget.reserve_heap(
                                credits,
                                std::mem::size_of::<StableDocumentKey>()
                                    + std::mem::size_of::<QueryPosting>(),
                            )?;
                            term.insert(posting.document, posting);
                        }
                    }
                    budget.release_heap(credits, posting_bytes)?;
                }
                if newest.len() > budget.limits.maximum_expanded_terms {
                    return resource(newest.len(), budget.limits.maximum_expanded_terms);
                }
            }
            budget.release_heap(credits, entry_bytes)?;
        }
        credits.release(run_bytes)?;
    }
    let mut output = BTreeMap::<StableDocumentKey, u64>::new();
    for postings in newest.into_values() {
        for (key, posting) in postings {
            if posting.live {
                output
                    .entry(key)
                    .and_modify(|version| {
                        *version = (*version).max(posting.material_source_version)
                    })
                    .or_insert_with(|| posting.material_source_version);
            }
        }
        budget.candidates(output.len())?;
    }
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
async fn seek_range<L: QueryArtifactLoader>(
    loader: &mut L,
    view: &PartitionView<'_>,
    recipe: RecipeIdentity,
    lower: Option<&RangeBound>,
    upper: Option<&RangeBound>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<BTreeMap<StableDocumentKey, u64>, IndexError> {
    let mut newest = BTreeMap::<(ScalarValue, StableDocumentKey), (bool, u64)>::new();
    let mut stream = QueryRunStream::new(view.pin.root);
    while let Some((run, run_bytes)) =
        load_next_descriptor(loader, view, &mut stream, block_limits, credits, budget).await?
    {
        for descriptor in run
            .blocks
            .iter()
            .filter(|block| block.kind == QueryBlockKind::Point && block.recipe == recipe)
        {
            if !point_descriptor_overlaps(descriptor, lower, upper)? {
                continue;
            }
            let (records, record_bytes) =
                load_block(loader, descriptor, block_limits, credits, budget).await?;
            for record in records {
                let point = decode_point(record.as_ref())?;
                if in_range(&point.value, lower, upper) {
                    let key = (point.value, point.document);
                    if !newest.contains_key(&key) {
                        budget.reserve_heap(
                            credits,
                            std::mem::size_of::<(ScalarValue, StableDocumentKey)>()
                                + std::mem::size_of::<(bool, u64)>()
                                + resident_scalar_bytes(&key.0),
                        )?;
                        newest.insert(key, (point.live, point.material_source_version));
                    }
                }
            }
            budget.release_heap(credits, record_bytes)?;
        }
        credits.release(run_bytes)?;
    }
    let mut output = BTreeMap::<StableDocumentKey, u64>::new();
    for ((_, key), (live, version)) in newest {
        if live {
            output
                .entry(key)
                .and_modify(|current| *current = (*current).max(version))
                .or_insert(version);
        }
    }
    budget.candidates(output.len())?;
    Ok(output)
}

fn point_descriptor_overlaps(
    descriptor: &QueryBlockDescriptor,
    lower: Option<&RangeBound>,
    upper: Option<&RangeBound>,
) -> Result<bool, IndexError> {
    let lower = lower
        .map(|bound| encode_scalar_sort_key(&bound.value))
        .transpose()?;
    let upper = upper
        .map(|bound| {
            let mut key = encode_scalar_sort_key(&bound.value)?;
            key.extend_from_slice(&[0xff; 32]);
            Ok::<_, IndexError>(key)
        })
        .transpose()?;
    Ok(lower
        .as_ref()
        .is_none_or(|key| descriptor.maximum_key.as_slice() >= key.as_slice())
        && upper
            .as_ref()
            .is_none_or(|key| descriptor.minimum_key.as_slice() <= key.as_slice()))
}

fn in_range(value: &ScalarValue, lower: Option<&RangeBound>, upper: Option<&RangeBound>) -> bool {
    lower.is_none_or(|bound| value > &bound.value || bound.inclusive && value == &bound.value)
        && upper
            .is_none_or(|bound| value < &bound.value || bound.inclusive && value == &bound.value)
}

#[allow(clippy::too_many_arguments)]
async fn verify_phrase<L: QueryArtifactLoader>(
    loader: &mut L,
    view: &PartitionView<'_>,
    recipe: RecipeIdentity,
    terms: &[ScalarValue],
    candidates: &mut BTreeSet<StableDocumentKey>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<(), IndexError> {
    let mut positions = BTreeMap::<(ScalarValue, StableDocumentKey), Vec<u32>>::new();
    for term in terms {
        let mut decided = BTreeSet::new();
        let mut stream = QueryRunStream::new(view.pin.root);
        while let Some((run, run_bytes)) =
            load_next_descriptor(loader, view, &mut stream, block_limits, credits, budget).await?
        {
            for dictionary in run.blocks.iter().filter(|block| {
                block.kind == QueryBlockKind::TermDictionary && block.recipe == recipe
            }) {
                let (entries, entry_bytes) = load_selected_terms(
                    loader,
                    dictionary,
                    std::slice::from_ref(term),
                    None,
                    block_limits,
                    credits,
                    budget,
                )
                .await?;
                for entry in entries {
                    for shard in entry.posting_shards {
                        let posting_descriptor = find_block(
                            &run,
                            shard.posting_block_hash,
                            QueryBlockKind::Posting,
                            recipe,
                        )?;
                        let (postings, posting_bytes) =
                            load_block(loader, posting_descriptor, block_limits, credits, budget)
                                .await?;
                        if postings.len() != shard.posting_records as usize {
                            return Err(IndexError::Integrity);
                        }
                        for record in postings {
                            let posting = decode_posting(record.as_ref())?;
                            if !decided.insert(posting.document)
                                || !posting.live
                                || !candidates.contains(&posting.document)
                            {
                                continue;
                            }
                            let Some(hash) = posting.position_block_hash else {
                                continue;
                            };
                            let descriptor =
                                find_block(&run, hash, QueryBlockKind::Position, recipe)?;
                            let (records, position_bytes) =
                                load_block(loader, descriptor, block_limits, credits, budget)
                                    .await?;
                            if let Some(record) = records.iter().find(|record| {
                                record.key.as_slice() == posting.document.bytes().as_slice()
                            }) {
                                let key = (term.clone(), posting.document);
                                if let std::collections::btree_map::Entry::Vacant(entry) =
                                    positions.entry(key)
                                {
                                    let decoded =
                                        decode_positions(record.as_ref(), block_limits)?.positions;
                                    budget.reserve_heap(
                                        credits,
                                        std::mem::size_of::<(ScalarValue, StableDocumentKey)>()
                                            .checked_add(std::mem::size_of::<Vec<u32>>())
                                            .and_then(|bytes| {
                                                bytes.checked_add(resident_scalar_bytes(term))
                                            })
                                            .and_then(|bytes| {
                                                decoded
                                                    .len()
                                                    .checked_mul(std::mem::size_of::<u32>())
                                                    .and_then(|positions| {
                                                        bytes.checked_add(positions)
                                                    })
                                            })
                                            .ok_or(IndexError::OffsetOverflow)?,
                                    )?;
                                    entry.insert(decoded);
                                }
                            }
                            budget.release_heap(credits, position_bytes)?;
                        }
                        budget.release_heap(credits, posting_bytes)?;
                    }
                    budget.release_heap(credits, entry_bytes)?;
                }
            }
            credits.release(run_bytes)?;
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
    Ok(())
}

fn find_block(
    run: &ProjectionQueryRunDescriptor,
    hash: [u8; 32],
    kind: QueryBlockKind,
    recipe: RecipeIdentity,
) -> Result<&QueryBlockDescriptor, IndexError> {
    run.blocks
        .iter()
        .find(|block| block.hash == hash && block.kind == kind && block.recipe == recipe)
        .ok_or(IndexError::Integrity)
}

struct OwnedRecord {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl OwnedRecord {
    fn as_ref(&self) -> super::QueryBlockRecordRef<'_> {
        super::QueryBlockRecordRef {
            key: &self.key,
            value: &self.value,
        }
    }
}

async fn load_block<L: QueryArtifactLoader>(
    loader: &mut L,
    descriptor: &QueryBlockDescriptor,
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<(Vec<OwnedRecord>, usize), IndexError> {
    let maximum = usize::try_from(descriptor.encoded_bytes).map_err(|_| IndexError::Integrity)?;
    let bytes = load_pre_admitted(
        loader,
        QueryArtifactKind::Block,
        descriptor.hash,
        maximum,
        credits,
        budget,
    )
    .await?;
    if bytes.len() != maximum {
        credits.release(bytes.len())?;
        return Err(IndexError::Integrity);
    }
    credits.release(bytes.len())?;
    let mut cursor = QueryBlockCursor::new(descriptor, &bytes, limits, credits)?;
    let resident_bytes = (descriptor.records as usize)
        .checked_mul(std::mem::size_of::<OwnedRecord>())
        .and_then(|bytes| bytes.checked_add(maximum))
        .ok_or(IndexError::OffsetOverflow)?;
    budget.reserve_heap(credits, resident_bytes)?;
    let mut output = Vec::with_capacity(descriptor.records as usize);
    while let Some(record) = cursor.next()? {
        output.push(OwnedRecord {
            key: record.key.to_vec(),
            value: record.value.to_vec(),
        });
    }
    drop(cursor);
    credits.release_loaded_block(bytes.len())?;
    Ok((output, resident_bytes))
}

#[allow(clippy::too_many_arguments)]
async fn load_selected_terms<L: QueryArtifactLoader>(
    loader: &mut L,
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
    let maximum = usize::try_from(descriptor.encoded_bytes).map_err(|_| IndexError::Integrity)?;
    let bytes = load_pre_admitted(
        loader,
        QueryArtifactKind::Block,
        descriptor.hash,
        maximum,
        credits,
        budget,
    )
    .await?;
    if bytes.len() != maximum {
        credits.release(bytes.len())?;
        return Err(IndexError::Integrity);
    }
    credits.release(bytes.len())?;
    let mut cursor = QueryBlockCursor::new(descriptor, &bytes, limits, credits)?;
    let resident_bytes = (descriptor.records as usize)
        .checked_mul(std::mem::size_of::<QueryTermEntry>())
        .and_then(|bytes| bytes.checked_add(maximum))
        .ok_or(IndexError::OffsetOverflow)?;
    budget.reserve_heap(credits, resident_bytes)?;
    let mut output = Vec::new();
    for (term, key) in exact.iter().zip(&exact_keys) {
        if let Some(record) = cursor.seek_to(key)? {
            let entry = decode_term_entry(record, limits)?;
            if entry.term == *term {
                output.push(entry);
            }
        }
    }
    if let (Some(prefix), Some(start)) = (prefix, prefix_start) {
        let mut next = cursor.seek_to(&start)?;
        while let Some(record) = next {
            let entry = decode_term_entry(record, limits)?;
            match &entry.term {
                ScalarValue::String(term) if term.starts_with(prefix) => output.push(entry),
                _ => break,
            }
            if output.len() > budget.limits.maximum_expanded_terms {
                return resource(output.len(), budget.limits.maximum_expanded_terms);
            }
            next = cursor.next()?;
        }
    }
    output.sort_unstable_by(|left, right| left.term.cmp(&right.term));
    output.dedup_by(|left, right| left.term == right.term);
    drop(cursor);
    credits.release_loaded_block(bytes.len())?;
    Ok((output, resident_bytes))
}

#[allow(clippy::too_many_arguments)]
async fn load_candidate_doc_values<L: QueryArtifactLoader>(
    loader: &mut L,
    view: &PartitionView<'_>,
    recipe: RecipeIdentity,
    candidates: &BTreeSet<StableDocumentKey>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<BTreeMap<StableDocumentKey, Option<Vec<ScalarValue>>>, IndexError> {
    let mut output = BTreeMap::new();
    let mut stream = QueryRunStream::new(view.pin.root);
    while let Some((run, run_bytes)) =
        load_next_descriptor(loader, view, &mut stream, block_limits, credits, budget).await?
    {
        for descriptor in run
            .blocks
            .iter()
            .filter(|block| block.kind == QueryBlockKind::DocValue && block.recipe == recipe)
        {
            if !candidates.iter().any(|key| {
                key.bytes().as_slice() >= descriptor.minimum_key.as_slice()
                    && key.bytes().as_slice() <= descriptor.maximum_key.as_slice()
            }) {
                continue;
            }
            let maximum =
                usize::try_from(descriptor.encoded_bytes).map_err(|_| IndexError::Integrity)?;
            let bytes = load_pre_admitted(
                loader,
                QueryArtifactKind::Block,
                descriptor.hash,
                maximum,
                credits,
                budget,
            )
            .await?;
            if bytes.len() != maximum {
                credits.release(bytes.len())?;
                return Err(IndexError::Integrity);
            }
            credits.release(bytes.len())?;
            let mut cursor = QueryBlockCursor::new(descriptor, &bytes, block_limits, credits)?;
            budget.reserve_heap(
                credits,
                candidates
                    .len()
                    .checked_mul(std::mem::size_of::<Vec<ScalarValue>>())
                    .and_then(|bytes| bytes.checked_add(maximum))
                    .ok_or(IndexError::OffsetOverflow)?,
            )?;
            for candidate in candidates {
                if output.contains_key(candidate)
                    || candidate.bytes().as_slice() < descriptor.minimum_key.as_slice()
                    || candidate.bytes().as_slice() > descriptor.maximum_key.as_slice()
                {
                    continue;
                }
                if let Some(record) = cursor.seek_to(&candidate.bytes())? {
                    let value = decode_doc_value(record, block_limits)?;
                    if value.document == *candidate {
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
                        budget.reserve_heap(credits, resident)?;
                        output.insert(*candidate, value.value);
                    }
                }
            }
            drop(cursor);
            credits.release_loaded_block(bytes.len())?;
            budget.release_heap(
                credits,
                candidates
                    .len()
                    .checked_mul(std::mem::size_of::<Vec<ScalarValue>>())
                    .and_then(|bytes| bytes.checked_add(maximum))
                    .ok_or(IndexError::OffsetOverflow)?,
            )?;
        }
        credits.release(run_bytes)?;
    }
    Ok(output)
}

fn requested_value_recipes(
    request: &TypedJsonQueryRequest,
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
) -> Result<BTreeSet<RecipeIdentity>, IndexError> {
    request
        .order
        .iter()
        .map(|field| field.field_id)
        .chain(request.facets.iter().map(|field| field.field_id))
        .chain(request.aggregates.iter().map(|field| field.field_id))
        .map(|id| {
            contracts
                .get(&id)
                .map(|binding| binding.recipe)
                .ok_or_else(|| IndexError::InvalidQuery("query field is not bound".into()))
        })
        .collect()
}

fn order_candidates(
    candidates: &mut [QueryCandidate],
    order: &[OrderField],
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
    values: &BTreeMap<
        (
            ProjectionPartitionIdentity,
            StableDocumentKey,
            RecipeIdentity,
        ),
        Option<Vec<ScalarValue>>,
    >,
) -> Result<(), IndexError> {
    for field in order {
        let contract = contracts
            .get(&field.field_id)
            .ok_or_else(|| IndexError::InvalidQuery("order field is not bound".into()))?;
        if contract.field.cardinality != Cardinality::Single
            || !contract
                .field
                .capabilities
                .contains(FieldCapabilities::ORDER)
        {
            return Err(IndexError::InvalidQuery("field cannot order".into()));
        }
    }
    if order.iter().any(|field| {
        let recipe = contracts[&field.field_id].recipe;
        candidates.iter().any(|candidate| {
            values
                .get(&(candidate.partition, candidate.document, recipe))
                .and_then(Option::as_ref)
                .is_some_and(|values| values.len() != 1)
        })
    }) {
        return Err(IndexError::Integrity);
    }
    candidates.sort_by(|left, right| {
        for field in order {
            let recipe = contracts[&field.field_id].recipe;
            let left_value = values.get(&(left.partition, left.document, recipe));
            let right_value = values.get(&(right.partition, right.document, recipe));
            let comparison = compare_order_value(left_value, right_value);
            let comparison = match field.direction {
                OrderDirection::Ascending => comparison,
                OrderDirection::Descending => comparison.reverse(),
            };
            if comparison != Ordering::Equal {
                return comparison;
            }
        }
        // Handoff may move the selected copy of one stable document from a
        // retiring partition to its successor while a continuation retains
        // the same logical cut. Physical placement must therefore never enter
        // the public result order. Deduplication above guarantees document
        // keys are unique here, so the stable key is a complete tie-breaker.
        left.document.cmp(&right.document)
    });
    Ok(())
}

fn compare_order_value(
    left: Option<&Option<Vec<ScalarValue>>>,
    right: Option<&Option<Vec<ScalarValue>>>,
) -> Ordering {
    fn one(value: Option<&Option<Vec<ScalarValue>>>) -> (u8, Option<&ScalarValue>) {
        match value {
            None | Some(None) => (0, None),
            Some(Some(values)) => (1, values.first()),
        }
    }
    one(left).cmp(&one(right))
}

fn facet_candidates(
    candidates: &[QueryCandidate],
    requests: &[FacetRequest],
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
    values: &BTreeMap<
        (
            ProjectionPartitionIdentity,
            StableDocumentKey,
            RecipeIdentity,
        ),
        Option<Vec<ScalarValue>>,
    >,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Vec<FacetResult>, IndexError> {
    budget.reserve_heap(
        credits,
        requests
            .len()
            .checked_mul(std::mem::size_of::<FacetResult>())
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let mut results = Vec::with_capacity(requests.len());
    for request in requests {
        let binding = contracts
            .get(&request.field_id)
            .ok_or_else(|| IndexError::InvalidQuery("facet field is not bound".into()))?;
        if !binding
            .field
            .capabilities
            .contains(FieldCapabilities::FACET)
            || request.limit == 0
        {
            return Err(IndexError::InvalidQuery(
                "field cannot facet or facet limit is zero".into(),
            ));
        }
        let mut counts = BTreeMap::<ScalarValue, u64>::new();
        for candidate in candidates {
            if let Some(Some(document_values)) =
                values.get(&(candidate.partition, candidate.document, binding.recipe))
            {
                let mut previous = None;
                for value in document_values {
                    if previous == Some(value) {
                        continue;
                    }
                    previous = Some(value);
                    if !counts.contains_key(value) {
                        budget.reserve_heap(
                            credits,
                            resident_scalar_bytes(value)
                                .checked_add(std::mem::size_of::<u64>())
                                .ok_or(IndexError::OffsetOverflow)?,
                        )?;
                    }
                    *counts.entry(value.clone()).or_default() += 1;
                }
            }
        }
        budget.reserve_heap(
            credits,
            counts
                .len()
                .checked_mul(std::mem::size_of::<FacetBucket>())
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut buckets = counts
            .into_iter()
            .map(|(value, count)| FacetBucket { value, count })
            .collect::<Vec<_>>();
        buckets.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.value.cmp(&right.value))
        });
        buckets.truncate(request.limit as usize);
        results.push(FacetResult {
            field_id: request.field_id,
            buckets,
        });
    }
    Ok(results)
}

fn aggregate_candidates(
    candidates: &[QueryCandidate],
    requests: &[AggregateRequest],
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
    values: &BTreeMap<
        (
            ProjectionPartitionIdentity,
            StableDocumentKey,
            RecipeIdentity,
        ),
        Option<Vec<ScalarValue>>,
    >,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Vec<AggregateResult>, IndexError> {
    budget.reserve_heap(
        credits,
        requests
            .len()
            .checked_mul(std::mem::size_of::<AggregateResult>())
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let mut results = Vec::with_capacity(requests.len());
    for request in requests {
        let binding = contracts
            .get(&request.field_id)
            .ok_or_else(|| IndexError::InvalidQuery("aggregate field is not bound".into()))?;
        if !binding
            .field
            .capabilities
            .contains(FieldCapabilities::AGGREGATE)
        {
            return Err(IndexError::InvalidQuery("field cannot aggregate".into()));
        }
        let selected = candidates
            .iter()
            .flat_map(|candidate| {
                values
                    .get(&(candidate.partition, candidate.document, binding.recipe))
                    .and_then(Option::as_ref)
            })
            .flatten()
            .filter(|value| !matches!(value, ScalarValue::Null));
        let (value, contributing_count) = reduce_streaming(request.operation, selected)?;
        if let Some(value) = &value {
            budget.reserve_heap(credits, resident_scalar_bytes(value))?;
        }
        results.push(AggregateResult {
            field_id: request.field_id,
            operation: request.operation,
            value,
            contributing_count,
        });
    }
    Ok(results)
}

#[cfg(test)]
#[path = "query_executor_tests.rs"]
mod tests;
