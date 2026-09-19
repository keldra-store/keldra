use std::collections::BTreeMap;
use std::sync::Arc;

use crate::IndexError;

use super::super::query_parallel::QueryPartitionJob;
use super::*;

struct ManifestGroup {
    key: ProjectionPartitionIdentity,
    manifests: Vec<PartitionManifest>,
}

pub(super) struct GeneralQueryOutput {
    pub candidates: Vec<AuthorizedQueryCandidate>,
    pub facets: Vec<FacetResult>,
    pub aggregates: Vec<AggregateResult>,
    pub next: Option<ExplicitQuerySearchAfter>,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_general_query<
    L: QueryArtifactLoader + 'static,
    A: QueryCandidateAdmission,
    E: QueryPublicValueEncoder,
    X: QueryPartitionExecutor,
>(
    loader: &mut L,
    admission: &mut A,
    partition_executor: &X,
    common_cut: QueryCommonCut,
    manifests: &[PartitionManifest],
    request: &TypedJsonQueryRequest,
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
    explicit_search_after: Option<&ExplicitQuerySearchAfter>,
    public_value_encoder: &E,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<GeneralQueryOutput, IndexError> {
    let needed_values = requested_value_recipes(request, contracts)?
        .into_iter()
        .collect::<Vec<_>>();
    let mut reducers = QueryValueReducers::new(
        &request.facets,
        &request.aggregates,
        contracts,
        credits,
        budget,
    )?;
    let mut collector = BoundedCandidateCollector::new(
        request.result_limit,
        &request.order,
        contracts,
        explicit_search_after,
        credits,
        budget,
    )?;
    let mut grouped = BTreeMap::<[u8; 32], Vec<PartitionManifest>>::new();
    for manifest in manifests {
        grouped
            .entry(manifest.view.pin.handoff_lineage_id)
            .or_default()
            .push(clone_manifest(manifest));
    }
    let mut groups = grouped
        .into_iter()
        .map(|(_, manifests)| ManifestGroup {
            key: manifests[0].view.pin.partition,
            manifests,
        })
        .collect::<Vec<_>>();
    groups.sort_unstable_by_key(|group| group.key);
    let width = partition_executor.maximum_parallelism().max(1);
    for group_batch in groups.chunks(width) {
        if let [group] = group_batch {
            let selected = scan_group(
                loader,
                partition_executor,
                &group.manifests,
                request,
                contracts,
                block_limits,
                credits,
                budget,
            )
            .await?;
            process_selected(
                loader,
                admission,
                partition_executor,
                common_cut,
                manifests,
                request,
                contracts,
                &needed_values,
                selected,
                public_value_encoder,
                block_limits,
                credits,
                budget,
                &mut reducers,
                &mut collector,
            )
            .await?;
            continue;
        }
        let mut jobs = Vec::with_capacity(group_batch.len());
        let mut can_fork = true;
        for group in group_batch {
            let Some(mut task_loader) = loader.try_fork_query_loader()? else {
                can_fork = false;
                break;
            };
            let manifests = group
                .manifests
                .iter()
                .map(clone_manifest)
                .collect::<Vec<_>>();
            let request = request.clone();
            let contracts = contracts.clone();
            let (mut task_budget, mut task_credits) = budget.try_fork_query(credits)?;
            jobs.push((
                group.key,
                Box::pin(async move {
                    scan_group(
                        &mut task_loader,
                        &SerialQueryPartitionExecutor,
                        &manifests,
                        &request,
                        &contracts,
                        block_limits,
                        &mut task_credits,
                        &mut task_budget,
                    )
                    .await
                }) as QueryPartitionJob<_>,
            ));
        }
        let selected_groups = if can_fork {
            partition_executor.execute_ordered(jobs).await?
        } else {
            let mut selected = Vec::with_capacity(group_batch.len());
            for group in group_batch {
                selected.push((
                    group.key,
                    scan_group(
                        loader,
                        &SerialQueryPartitionExecutor,
                        &group.manifests,
                        request,
                        contracts,
                        block_limits,
                        credits,
                        budget,
                    )
                    .await?,
                ));
            }
            selected
        };
        for (_, selected) in selected_groups {
            process_selected(
                loader,
                admission,
                partition_executor,
                common_cut,
                manifests,
                request,
                contracts,
                &needed_values,
                selected,
                public_value_encoder,
                block_limits,
                credits,
                budget,
                &mut reducers,
                &mut collector,
            )
            .await?;
        }
    }
    let (facets, aggregates) = reducers.finish(credits, budget)?;
    let (candidates, next) = collector.finish(credits, budget)?;
    Ok(GeneralQueryOutput {
        candidates,
        facets,
        aggregates,
        next,
    })
}

fn clone_manifest(manifest: &PartitionManifest) -> PartitionManifest {
    PartitionManifest {
        view: manifest.view,
        runs: manifest.runs.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn scan_group<L: QueryArtifactLoader + 'static, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    manifests: &[PartitionManifest],
    request: &TypedJsonQueryRequest,
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<BTreeMap<StableDocumentKey, QueryAdmissionCandidate>, IndexError> {
    let mut selected = BTreeMap::new();
    for manifest in manifests {
        scan_manifest(
            loader,
            executor,
            manifest,
            request,
            contracts,
            block_limits,
            credits,
            budget,
            &mut selected,
        )
        .await?;
    }
    Ok(selected)
}

#[allow(clippy::too_many_arguments)]
async fn scan_manifest<L: QueryArtifactLoader + 'static, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    manifest: &PartitionManifest,
    request: &TypedJsonQueryRequest,
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
    selected: &mut BTreeMap<StableDocumentKey, QueryAdmissionCandidate>,
) -> Result<(), IndexError> {
    let view = &manifest.view;
    let needs_universe = request
        .predicate
        .as_ref()
        .is_none_or(predicate_requires_universe);
    let mut gates = if needs_universe {
        load_latest_gates(
            loader,
            executor,
            manifest,
            request.logical.membership,
            QueryBlockKind::Gate,
            block_limits,
            credits,
            budget,
        )
        .await?
    } else {
        BTreeMap::new()
    };
    let mut keys = if let Some(predicate) = request.predicate.as_ref() {
        evaluate_predicate(
            loader,
            executor,
            manifest,
            &gates,
            contracts,
            request.logical.membership,
            predicate,
            request.resume_after_document,
            block_limits,
            credits,
            budget,
        )
        .await?
        .into_document_keys(credits, budget)?
    } else {
        budget.reserve_heap(credits, document_key_set_bytes(gates.len())?)?;
        let keys = match_all_live_documents(&gates);
        budget.release_heap(
            credits,
            document_key_set_bytes(gates.len())?
                .checked_sub(document_key_set_bytes(keys.len())?)
                .ok_or(IndexError::Integrity)?,
        )?;
        keys
    };
    let key_bytes = document_key_set_bytes(keys.len())?;
    if let Some(resume) = request.resume_after_document {
        keys.retain(|key| *key > resume);
    }
    if !needs_universe {
        let aligned = load_latest_gates_for_keys(
            loader,
            executor,
            manifest,
            request.logical.membership,
            QueryBlockKind::Gate,
            &keys,
            block_limits,
            credits,
            budget,
        )
        .await?;
        let (aligned_gates, aligned_bytes) = aligned.into_parts();
        if aligned_gates.len() != keys.len() {
            return Err(IndexError::Integrity);
        }
        for (document, gate) in keys.into_iter().zip(aligned_gates) {
            add_live_gate(
                view,
                document,
                gate.ok_or(IndexError::Integrity)?,
                selected,
                credits,
                budget,
            )?;
        }
        budget.release_heap(credits, aligned_bytes)?;
    } else {
        for document in keys {
            let gate = gates.remove(&document).ok_or(IndexError::Integrity)?;
            let gate_bytes = resident_gate_bytes(&gate)?;
            add_live_gate(view, document, gate, selected, credits, budget)?;
            budget.release_heap(credits, gate_bytes)?;
        }
        let remaining = gates.values().try_fold(0usize, |bytes, gate| {
            bytes
                .checked_add(resident_gate_bytes(gate)?)
                .ok_or(IndexError::OffsetOverflow)
        })?;
        budget.release_heap(credits, remaining)?;
    }
    drop(gates);
    budget.release_heap(credits, key_bytes)?;
    Ok(())
}

fn add_live_gate(
    view: &PartitionView,
    document: StableDocumentKey,
    gate: QueryDocumentGate,
    selected: &mut BTreeMap<StableDocumentKey, QueryAdmissionCandidate>,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<(), IndexError> {
    if !gate.live {
        return Ok(());
    }
    select_handoff_candidate(
        selected,
        QueryAdmissionCandidate {
            partition: view.pin.partition,
            handoff_lineage_id: view.pin.handoff_lineage_id,
            covered_through_source_position: gate
                .selective_source_position
                .unwrap_or(view.pin.covered_through_source_position()?),
            document,
            material_source_version: gate.material_source_version,
            current_source_version: gate.current_source_version,
            source_path: gate.source_path.ok_or(IndexError::Integrity)?,
            canonical_source_path: gate.canonical_source_path,
            result_path: gate.result_path.ok_or(IndexError::Integrity)?,
            result_version: gate.result_version,
        },
        credits,
        budget,
    )?;
    budget.candidates(selected.len())
}

#[allow(clippy::too_many_arguments)]
async fn process_selected<
    L: QueryArtifactLoader + 'static,
    A: QueryCandidateAdmission,
    E: QueryPublicValueEncoder,
    X: QueryPartitionExecutor,
>(
    loader: &mut L,
    admission: &mut A,
    executor: &X,
    common_cut: QueryCommonCut,
    manifests: &[PartitionManifest],
    request: &TypedJsonQueryRequest,
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
    needed_values: &[RecipeIdentity],
    selected: BTreeMap<StableDocumentKey, QueryAdmissionCandidate>,
    encoder: &E,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
    reducers: &mut QueryValueReducers,
    collector: &mut BoundedCandidateCollector,
) -> Result<(), IndexError> {
    let ordered_bytes = selected
        .len()
        .checked_mul(std::mem::size_of::<QueryAdmissionCandidate>())
        .ok_or(IndexError::OffsetOverflow)?;
    budget.reserve_heap(credits, ordered_bytes)?;
    let mut ordered = selected.into_values().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|candidate| (candidate.partition, candidate.document));
    let mut ordered = ordered.into_iter();
    loop {
        let mut batch = BTreeMap::new();
        for _ in 0..MAX_QUERY_CANDIDATE_ADMISSION_BATCH {
            let Some(candidate) = ordered.next() else {
                break;
            };
            batch.insert(candidate.document, candidate);
        }
        if batch.is_empty() {
            break;
        }
        let (mut authorized, mut candidates) = authorize_selected_candidates(
            admission,
            batch,
            request.logical.logical_index_id,
            request.logical.logical_definition_version,
            common_cut,
            credits,
            budget,
            None,
        )
        .await?;
        candidates.sort_unstable_by_key(|candidate| (candidate.partition, candidate.document));
        let mut start = 0;
        while start < candidates.len() {
            let partition = candidates[start].partition;
            let end =
                start + candidates[start..].partition_point(|value| value.partition == partition);
            let manifest = manifests
                .binary_search_by_key(&partition, |manifest| manifest.view.pin.partition)
                .ok()
                .and_then(|index| manifests.get(index))
                .ok_or(IndexError::Integrity)?;
            process_partition_batch(
                loader,
                executor,
                manifest,
                &candidates[start..end],
                &mut authorized,
                contracts,
                needed_values,
                encoder,
                block_limits,
                credits,
                budget,
                reducers,
                collector,
            )
            .await?;
            start = end;
        }
        if !authorized.is_empty() {
            return Err(IndexError::Integrity);
        }
    }
    budget.release_heap(credits, ordered_bytes)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_partition_batch<
    L: QueryArtifactLoader + 'static,
    E: QueryPublicValueEncoder,
    X: QueryPartitionExecutor,
>(
    loader: &mut L,
    executor: &X,
    manifest: &PartitionManifest,
    candidates: &[QueryCandidate],
    authorized: &mut BTreeMap<
        (ProjectionPartitionIdentity, StableDocumentKey),
        AuthorizedQueryCandidate,
    >,
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
    needed_values: &[RecipeIdentity],
    encoder: &E,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
    reducers: &mut QueryValueReducers,
    collector: &mut BoundedCandidateCollector,
) -> Result<(), IndexError> {
    let metadata = needed_values
        .len()
        .checked_mul(
            std::mem::size_of::<RecipeIdentity>()
                + std::mem::size_of::<Vec<Option<Option<Vec<ScalarValue>>>>>(),
        )
        .ok_or(IndexError::OffsetOverflow)?;
    budget.reserve_heap(credits, metadata)?;
    let mut columns = PartitionValueColumns::new(needed_values.to_vec(), metadata);
    let mut loaded_columns = Vec::with_capacity(needed_values.len());
    if needed_values.len() > 1 && executor.maximum_parallelism() > 1 {
        let candidate_copy_bytes = candidates
            .len()
            .checked_mul(std::mem::size_of::<QueryCandidate>())
            .ok_or(IndexError::OffsetOverflow)?;
        let candidate_copy_charge = budget.reserve_heap_scoped(credits, candidate_copy_bytes)?;
        let parallel_candidates = Arc::<[QueryCandidate]>::from(candidates);
        let mut jobs = Vec::with_capacity(needed_values.len());
        for (ordinal, recipe) in needed_values.iter().copied().enumerate() {
            let Some(mut task_loader) = loader.try_fork_query_loader()? else {
                jobs.clear();
                break;
            };
            let manifest = clone_manifest(manifest);
            let candidates = parallel_candidates.clone();
            let (mut task_budget, mut task_credits) = budget.try_fork_query(credits)?;
            let task_executor = executor.clone();
            jobs.push((
                ordinal,
                Box::pin(async move {
                    load_candidate_doc_values(
                        &mut task_loader,
                        &task_executor,
                        &manifest,
                        recipe,
                        candidates.as_ref(),
                        block_limits,
                        &mut task_credits,
                        &mut task_budget,
                    )
                    .await
                }) as QueryPartitionJob<_>,
            ));
        }
        if jobs.len() == needed_values.len() {
            loaded_columns = executor.execute_ordered(jobs).await?;
        }
        drop(parallel_candidates);
        candidate_copy_charge.release()?;
    }
    if loaded_columns.is_empty() && !needed_values.is_empty() {
        for (ordinal, recipe) in needed_values.iter().copied().enumerate() {
            loaded_columns.push((
                ordinal,
                load_candidate_doc_values(
                    loader,
                    executor,
                    manifest,
                    recipe,
                    candidates,
                    block_limits,
                    credits,
                    budget,
                )
                .await?,
            ));
        }
    }
    if loaded_columns.len() != needed_values.len()
        || loaded_columns
            .iter()
            .enumerate()
            .any(|(expected, (actual, _))| expected != *actual)
    {
        return Err(IndexError::Integrity);
    }
    for (_, (loaded, bytes)) in loaded_columns {
        columns.push(loaded, bytes)?;
    }
    for (row, candidate) in candidates.iter().copied().enumerate() {
        let admitted = authorized
            .remove(&(candidate.partition, candidate.document))
            .ok_or(IndexError::Integrity)?;
        reducers.observe(row, &columns, contracts, encoder, credits, budget)?;
        collector.observe(candidate, admitted, row, &columns, credits, budget)?;
    }
    budget.release_heap(credits, columns.resident_bytes())
}
