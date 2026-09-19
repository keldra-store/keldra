use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};

use crate::IndexError;
use crate::typed_json::{Predicate, ScalarValue};

use super::super::query_parallel::QueryPartitionJob;
use super::DenseQueryPosting;
use super::admission::resident_selected_candidate_bytes;
use super::{
    AuthorizedQueryCandidate, Budget, PartitionManifest, QueryAdmissionCandidate,
    QueryArtifactLoader, QueryBlockCredits, QueryBlockDescriptor, QueryBlockKind, QueryBlockLimits,
    QueryCandidateAdmission, QueryCommonCut, QueryPartitionExecutor, StableDocumentKey,
    TypedJsonQueryRequest, authorize_selected_candidates, candidate_is_current,
    load_latest_gates_for_keys, load_selected_terms, select_handoff_candidate,
};

const POSTING_SOURCE_CHUNK: usize = 32;

/// Natural-order equality pages are the common exact-query path. Keep their
/// work proportional to the requested page instead of rebuilding the complete
/// suffix of the posting list for every continuation token.
#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_bounded_natural_equal<
    L: QueryArtifactLoader + 'static,
    A: QueryCandidateAdmission,
    X: QueryPartitionExecutor,
>(
    loader: &mut L,
    admission: &mut A,
    partition_executor: &X,
    common_cut: QueryCommonCut,
    manifests: &[PartitionManifest],
    request: &TypedJsonQueryRequest,
    contracts: &BTreeMap<crate::typed_json::FieldId, super::QueryFieldBinding>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Option<Vec<AuthorizedQueryCandidate>>, IndexError> {
    if !request.order.is_empty() || !request.facets.is_empty() || !request.aggregates.is_empty() {
        return Ok(None);
    }
    let Some(Predicate::Equal {
        field_id, value, ..
    }) = request.predicate.as_ref()
    else {
        return Ok(None);
    };
    let binding = contracts
        .get(field_id)
        .ok_or_else(|| IndexError::InvalidQuery("query field is not bound".into()))?;
    super::validate_leaf_capability(&binding.field, request.predicate.as_ref().unwrap())?;

    let scan_limit = request
        .result_limit
        .max(super::MAX_QUERY_CANDIDATE_ADMISSION_BATCH);
    let mut resume = request.resume_after_document;
    let mut output = Vec::with_capacity(request.result_limit);

    while output.len() < request.result_limit {
        let partition_pages = scan_equal_partition_pages(
            loader,
            partition_executor,
            manifests,
            request.logical.membership,
            binding.recipe,
            value,
            resume,
            scan_limit,
            block_limits,
            credits,
            budget,
        )
        .await?;
        let mut safe_through = None;
        let mut more = false;
        for page in &partition_pages {
            if page.truncated {
                more = true;
                let through = page.scan_through.ok_or(IndexError::Integrity)?;
                safe_through = Some(
                    safe_through.map_or(through, |current: StableDocumentKey| current.min(through)),
                );
            }
        }

        let mut selected = BTreeMap::new();
        for page in partition_pages {
            for candidate in page.candidates {
                budget.release_heap(credits, resident_selected_candidate_bytes(&candidate)?)?;
                if safe_through.is_none_or(|through| candidate.document <= through) {
                    select_handoff_candidate(&mut selected, candidate, credits, budget)?;
                }
            }
        }

        let remaining = request.result_limit.saturating_sub(output.len());
        let (mut authorized, candidates) = authorize_selected_candidates(
            admission,
            selected,
            request.logical.logical_index_id,
            request.logical.logical_definition_version,
            common_cut,
            credits,
            budget,
            Some(remaining),
        )
        .await?;
        for candidate in candidates {
            output.push(
                authorized
                    .remove(&(candidate.partition, candidate.document))
                    .ok_or(IndexError::Integrity)?,
            );
        }
        if output.len() == request.result_limit || !more {
            break;
        }
        let next = safe_through.ok_or(IndexError::Integrity)?;
        if resume.is_some_and(|previous| next <= previous) {
            return Err(IndexError::Integrity);
        }
        resume = Some(next);
    }

    Ok(Some(output))
}

struct PartitionCandidatePage {
    candidates: Vec<QueryAdmissionCandidate>,
    scan_through: Option<StableDocumentKey>,
    truncated: bool,
}

#[allow(clippy::too_many_arguments)]
async fn scan_equal_partition_pages<L: QueryArtifactLoader + 'static, X: QueryPartitionExecutor>(
    loader: &mut L,
    partition_executor: &X,
    manifests: &[PartitionManifest],
    membership_recipe: super::RecipeIdentity,
    field_recipe: super::RecipeIdentity,
    value: &ScalarValue,
    resume: Option<StableDocumentKey>,
    limit: usize,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Vec<PartitionCandidatePage>, IndexError> {
    if manifests.len() <= 1 || partition_executor.maximum_parallelism() <= 1 {
        let mut pages = Vec::with_capacity(manifests.len());
        for manifest in manifests {
            pages.push(
                scan_equal_partition_page(
                    loader,
                    partition_executor,
                    manifest,
                    membership_recipe,
                    field_recipe,
                    value,
                    resume,
                    limit,
                    block_limits,
                    credits,
                    budget,
                )
                .await?,
            );
        }
        return Ok(pages);
    }

    // Fork every loader before starting work. A loader without a shared
    // query-local authority falls back to the exact serial path rather than
    // manufacturing a second cache or storage authority.
    let mut loaders = Vec::with_capacity(manifests.len());
    for _ in manifests {
        let Some(fork) = loader.try_fork_query_loader()? else {
            let mut pages = Vec::with_capacity(manifests.len());
            for manifest in manifests {
                pages.push(
                    scan_equal_partition_page(
                        loader,
                        &super::super::SerialQueryPartitionExecutor,
                        manifest,
                        membership_recipe,
                        field_recipe,
                        value,
                        resume,
                        limit,
                        block_limits,
                        credits,
                        budget,
                    )
                    .await?,
                );
            }
            return Ok(pages);
        };
        loaders.push(fork);
    }

    let mut jobs =
        Vec::<(usize, QueryPartitionJob<PartitionCandidatePage>)>::with_capacity(manifests.len());
    for (ordinal, (mut loader, manifest)) in loaders.into_iter().zip(manifests).enumerate() {
        let manifest = PartitionManifest {
            view: manifest.view,
            runs: manifest.runs.clone(),
        };
        let value = value.clone();
        let (mut task_budget, mut task_credits) = budget.try_fork_query(credits)?;
        jobs.push((
            ordinal,
            Box::pin(async move {
                scan_equal_partition_page(
                    &mut loader,
                    &super::super::SerialQueryPartitionExecutor,
                    &manifest,
                    membership_recipe,
                    field_recipe,
                    &value,
                    resume,
                    limit,
                    block_limits,
                    &mut task_credits,
                    &mut task_budget,
                )
                .await
            }),
        ));
    }

    let mut keyed_pages = partition_executor.execute_ordered(jobs).await?;
    keyed_pages.sort_unstable_by_key(|(ordinal, _)| *ordinal);
    if keyed_pages.len() != manifests.len()
        || keyed_pages
            .iter()
            .enumerate()
            .any(|(expected, (actual, _))| expected != *actual)
    {
        return Err(IndexError::Integrity);
    }
    Ok(keyed_pages.into_iter().map(|(_, page)| page).collect())
}

#[allow(clippy::too_many_arguments)]
async fn scan_equal_partition_page<L: QueryArtifactLoader + 'static, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    manifest: &PartitionManifest,
    membership_recipe: super::RecipeIdentity,
    field_recipe: super::RecipeIdentity,
    value: &ScalarValue,
    resume: Option<StableDocumentKey>,
    limit: usize,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<PartitionCandidatePage, IndexError> {
    let (postings, truncated) = load_bounded_equal_postings(
        loader,
        executor,
        manifest,
        field_recipe,
        value,
        resume,
        limit,
        block_limits,
        credits,
        budget,
    )
    .await?;
    let scan_through = postings.keys().next_back().copied();
    let live = postings
        .iter()
        .filter_map(|(document, (_, posting))| posting.live.then_some(*document))
        .collect::<BTreeSet<_>>();
    budget.reserve_heap(
        credits,
        live.len()
            .checked_mul(std::mem::size_of::<StableDocumentKey>())
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let presence = load_latest_gates_for_keys(
        loader,
        executor,
        manifest,
        field_recipe,
        QueryBlockKind::Presence,
        &live,
        block_limits,
        credits,
        budget,
    )
    .await?;
    let membership = load_latest_gates_for_keys(
        loader,
        executor,
        manifest,
        membership_recipe,
        QueryBlockKind::Gate,
        &live,
        block_limits,
        credits,
        budget,
    )
    .await?;
    let (presence, presence_bytes) = presence.into_parts();
    let (membership, membership_bytes) = membership.into_parts();
    if presence.len() != live.len() || membership.len() != live.len() {
        return Err(IndexError::Integrity);
    }

    let mut candidates = Vec::with_capacity(live.len());
    for (ordinal, document) in live.iter().copied().enumerate() {
        let (_, posting) = postings.get(&document).ok_or(IndexError::Integrity)?;
        let membership = membership[ordinal].as_ref();
        if candidate_is_current(
            membership,
            presence[ordinal].as_ref(),
            posting.material_source_version,
        ) {
            let gate = membership.ok_or(IndexError::Integrity)?;
            let candidate = QueryAdmissionCandidate {
                partition: manifest.view.pin.partition,
                handoff_lineage_id: manifest.view.pin.handoff_lineage_id,
                covered_through_source_position: gate
                    .selective_source_position
                    .unwrap_or(manifest.view.pin.covered_through_source_position()?),
                document,
                material_source_version: posting.material_source_version,
                current_source_version: gate.current_source_version,
                source_path: gate.source_path.clone().ok_or(IndexError::Integrity)?,
                canonical_source_path: gate.canonical_source_path.clone(),
                result_path: gate.result_path.clone().ok_or(IndexError::Integrity)?,
                result_version: gate.result_version,
            };
            budget.reserve_heap(credits, resident_selected_candidate_bytes(&candidate)?)?;
            candidates.push(candidate);
        }
    }

    budget.release_heap(credits, presence_bytes)?;
    budget.release_heap(credits, membership_bytes)?;
    budget.release_heap(
        credits,
        live.len()
            .saturating_mul(std::mem::size_of::<StableDocumentKey>()),
    )?;
    budget.release_heap(
        credits,
        postings.len().saturating_mul(posting_entry_bytes()),
    )?;
    Ok(PartitionCandidatePage {
        candidates,
        scan_through,
        truncated,
    })
}

#[allow(clippy::too_many_arguments)]
async fn load_bounded_equal_postings<
    L: QueryArtifactLoader + 'static,
    X: QueryPartitionExecutor,
>(
    loader: &mut L,
    executor: &X,
    manifest: &PartitionManifest,
    recipe: super::RecipeIdentity,
    value: &ScalarValue,
    resume: Option<StableDocumentKey>,
    limit: usize,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<
    (
        BTreeMap<StableDocumentKey, (usize, DenseQueryPosting)>,
        bool,
    ),
    IndexError,
> {
    let mut sources = Vec::new();
    for (run, descriptor) in manifest.matching_blocks(QueryBlockKind::TermDictionary, recipe) {
        let (entries, entry_bytes) = load_selected_terms(
            loader,
            executor,
            manifest
                .runs
                .get(run)
                .ok_or(IndexError::Integrity)?
                .physical_catalog_generation,
            descriptor,
            std::slice::from_ref(value),
            None,
            block_limits,
            credits,
            budget,
        )
        .await?;
        for entry in entries {
            if entry.term != *value {
                return Err(IndexError::Integrity);
            }
            for shard in entry.posting_shards {
                if resume.is_some_and(|minimum| shard.maximum_document <= minimum) {
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
                sources.push(PostingSource {
                    run,
                    generation: manifest
                        .runs
                        .get(run)
                        .ok_or(IndexError::Integrity)?
                        .physical_catalog_generation,
                    descriptor: posting_descriptor,
                    minimum_document: shard.minimum_document,
                    maximum_document: shard.maximum_document,
                    resume,
                    buffered: VecDeque::new(),
                    exhausted: false,
                });
            }
        }
        budget.release_heap(credits, entry_bytes)?;
    }

    let source_bytes = sources
        .capacity()
        .checked_mul(std::mem::size_of::<PostingSource<'_>>())
        .ok_or(IndexError::OffsetOverflow)?;
    budget.reserve_heap(credits, source_bytes)?;
    let mut heap = BinaryHeap::new();
    let initial_sources = (0..sources.len()).collect();
    refill_posting_sources(
        loader,
        executor,
        &mut sources,
        initial_sources,
        block_limits,
        credits,
        budget,
    )
    .await?;
    for (source_index, source) in sources.iter().enumerate() {
        if let Some(posting) = source.buffered.front() {
            heap.push(Reverse((posting.document()?, source_index)));
        }
    }

    let mut newest = BTreeMap::new();
    while newest.len() < limit {
        let Some(Reverse((document, source_index))) = heap.pop() else {
            break;
        };
        let mut same_document_sources = vec![source_index];
        while heap
            .peek()
            .is_some_and(|Reverse((next, _))| *next == document)
        {
            let Reverse((_, source_index)) = heap.pop().ok_or(IndexError::Integrity)?;
            same_document_sources.push(source_index);
        }

        let mut selected = None::<(usize, DenseQueryPosting)>;
        let mut refill = Vec::new();
        for source_index in same_document_sources.iter().copied() {
            let source = sources.get_mut(source_index).ok_or(IndexError::Integrity)?;
            let posting = source.buffered.pop_front().ok_or(IndexError::Integrity)?;
            if posting.document()? != document {
                return Err(IndexError::Integrity);
            }
            budget.release_heap(credits, posting_buffer_bytes())?;
            if selected
                .as_ref()
                .is_none_or(|(selected_run, _)| source.run < *selected_run)
            {
                selected = Some((source.run, posting));
            }
            if source.buffered.is_empty() {
                refill.push(source_index);
            }
        }
        refill_posting_sources(
            loader,
            executor,
            &mut sources,
            refill,
            block_limits,
            credits,
            budget,
        )
        .await?;
        for source_index in same_document_sources {
            if let Some(next) = sources[source_index].buffered.front() {
                heap.push(Reverse((next.document()?, source_index)));
            }
        }
        let selected = selected.ok_or(IndexError::Integrity)?;
        budget.reserve_heap(credits, posting_entry_bytes())?;
        newest.insert(document, selected);
    }
    let truncated = !heap.is_empty();
    for source in sources {
        budget.release_heap(
            credits,
            source
                .buffered
                .len()
                .checked_mul(posting_buffer_bytes())
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
    }
    budget.release_heap(credits, source_bytes)?;
    budget.candidates(newest.len())?;
    Ok((newest, truncated))
}

struct PostingChunk {
    buffered: VecDeque<DenseQueryPosting>,
    resume: Option<StableDocumentKey>,
    exhausted: bool,
}

#[allow(clippy::too_many_arguments)]
async fn refill_posting_sources<L: QueryArtifactLoader + 'static, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    sources: &mut [PostingSource<'_>],
    indexes: Vec<usize>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<(), IndexError> {
    let indexes = indexes
        .into_iter()
        .filter(|index| {
            sources
                .get(*index)
                .is_some_and(|source| !source.exhausted && source.buffered.is_empty())
        })
        .collect::<Vec<_>>();
    let mut chunks = Vec::with_capacity(indexes.len());
    if indexes.len() > 1 && executor.maximum_parallelism() > 1 {
        let mut jobs = Vec::with_capacity(indexes.len());
        for source_index in indexes.iter().copied() {
            let source = sources.get(source_index).ok_or(IndexError::Integrity)?;
            let Some(mut task_loader) = loader.try_fork_query_loader()? else {
                jobs.clear();
                break;
            };
            let descriptor = (*source.descriptor).clone();
            let generation = source.generation;
            let minimum = source.minimum_document;
            let maximum = source.maximum_document;
            let resume = source.resume;
            let (mut task_budget, mut task_credits) = budget.try_fork_query(credits)?;
            let task_executor = executor.clone();
            jobs.push((
                source_index,
                Box::pin(async move {
                    load_posting_chunk(
                        &mut task_loader,
                        &task_executor,
                        generation,
                        &descriptor,
                        minimum,
                        maximum,
                        resume,
                        block_limits,
                        &mut task_credits,
                        &mut task_budget,
                    )
                    .await
                }) as QueryPartitionJob<_>,
            ));
        }
        if jobs.len() == indexes.len() {
            chunks = executor.execute_ordered(jobs).await?;
        }
    }
    if chunks.is_empty() && !indexes.is_empty() {
        for source_index in indexes.iter().copied() {
            let source = sources.get(source_index).ok_or(IndexError::Integrity)?;
            chunks.push((
                source_index,
                load_posting_chunk(
                    loader,
                    executor,
                    source.generation,
                    source.descriptor,
                    source.minimum_document,
                    source.maximum_document,
                    source.resume,
                    block_limits,
                    credits,
                    budget,
                )
                .await?,
            ));
        }
    }
    if chunks.len() != indexes.len()
        || chunks
            .iter()
            .zip(&indexes)
            .any(|((actual, _), expected)| actual != expected)
    {
        return Err(IndexError::Integrity);
    }
    for (source_index, chunk) in chunks {
        let source = sources.get_mut(source_index).ok_or(IndexError::Integrity)?;
        source.buffered = chunk.buffered;
        source.resume = chunk.resume;
        source.exhausted = chunk.exhausted;
    }
    Ok(())
}

struct PostingSource<'a> {
    run: usize,
    generation: [u8; 32],
    descriptor: &'a QueryBlockDescriptor,
    minimum_document: StableDocumentKey,
    maximum_document: StableDocumentKey,
    resume: Option<StableDocumentKey>,
    buffered: VecDeque<DenseQueryPosting>,
    exhausted: bool,
}

#[allow(clippy::too_many_arguments)]
async fn load_posting_chunk<L: QueryArtifactLoader, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    generation: [u8; 32],
    descriptor: &QueryBlockDescriptor,
    minimum_document: StableDocumentKey,
    maximum_document: StableDocumentKey,
    mut resume: Option<StableDocumentKey>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<PostingChunk, IndexError> {
    let encoded_bytes =
        usize::try_from(descriptor.encoded_bytes).map_err(|_| IndexError::Integrity)?;
    let block = super::load_decoded_block(
        loader,
        executor,
        generation,
        descriptor,
        block_limits,
        credits,
        budget,
    )
    .await?;
    let mut cpu_budget = budget.clone();
    let mut cpu_credits = credits.try_fork_query()?;
    executor
        .run_cpu(Box::new(move || {
            let lower = block.documents().lower_bound(minimum_document);
            let upper = block.documents().upper_bound(maximum_document);
            let minimum = resume.map_or(lower, |resume| {
                block.documents().upper_bound(resume).max(lower)
            });
            let mut buffered = VecDeque::new();
            let mut exhausted = true;
            let mut last_document = None;
            let mut cursor = block.posting_cursor()?;
            let mut next = cursor.advance(minimum);
            while let Some(dense) = next? {
                if dense.document < lower || dense.document >= upper {
                    return Err(IndexError::Integrity);
                }
                last_document = Some(dense.document);
                cpu_budget.reserve_heap(&mut cpu_credits, posting_buffer_bytes())?;
                buffered.push_back(DenseQueryPosting {
                    documents: block.documents().clone(),
                    posting: dense,
                });
                if buffered.len() == POSTING_SOURCE_CHUNK {
                    exhausted = false;
                    break;
                }
                next = cursor.next();
            }
            if let Some(last) = last_document {
                resume = Some(block.documents().document(last)?);
            }
            cpu_credits.release_loaded_block(encoded_bytes)?;
            Ok(PostingChunk {
                buffered,
                resume,
                exhausted,
            })
        }))
        .await
}

const fn posting_entry_bytes() -> usize {
    std::mem::size_of::<StableDocumentKey>() + std::mem::size_of::<DenseQueryPosting>()
}

const fn posting_buffer_bytes() -> usize {
    std::mem::size_of::<DenseQueryPosting>()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    use bytes::Bytes;

    use super::*;
    use crate::typed_json::ScalarValue;
    use crate::v1::{
        ArtifactPackReference, ArtifactPackTable, ProjectionPartitionIdentity,
        ProjectionQueryRunDescriptor, ProjectionQueryStreamRoot, QueryArtifactLoad,
        QueryBlockRecord, QueryExecutionLimits, QueryMemoryPermit, QueryPosting, QueryPostingShard,
        QueryRootCutProof, QueryTermEntry, RecipeIdentity, encode_posting, encode_query_block,
        encode_term_entry,
    };

    struct Permit(usize);

    impl QueryMemoryPermit for Permit {
        fn admitted_bytes(&self) -> usize {
            self.0
        }
    }

    struct Loader {
        artifacts: BTreeMap<[u8; 32], Bytes>,
        decoded: BTreeMap<[u8; 32], Arc<super::super::DecodedQueryBlock>>,
        loads: BTreeMap<[u8; 32], usize>,
    }

    impl QueryArtifactLoader for Loader {
        fn load_query_artifact(
            &mut self,
            request: QueryArtifactLoad,
        ) -> impl std::future::Future<Output = Result<Bytes, IndexError>> + Send {
            *self.loads.entry(request.hash).or_default() += 1;
            let value = self.artifacts.get(&request.hash).cloned();
            async move { value.ok_or(IndexError::Integrity) }
        }

        fn cached_query_block(
            &self,
            _generation: [u8; 32],
            request: QueryArtifactLoad,
        ) -> Result<Option<Arc<super::super::DecodedQueryBlock>>, IndexError> {
            Ok(self.decoded.get(&request.hash).cloned())
        }

        fn cache_query_block(
            &mut self,
            _generation: [u8; 32],
            request: QueryArtifactLoad,
            block: Arc<super::super::DecodedQueryBlock>,
        ) {
            self.decoded.insert(request.hash, block);
        }
    }

    #[derive(Clone)]
    struct ForkLoader {
        artifacts: Arc<BTreeMap<[u8; 32], Bytes>>,
    }

    impl QueryArtifactLoader for ForkLoader {
        fn load_query_artifact(
            &mut self,
            request: QueryArtifactLoad,
        ) -> impl std::future::Future<Output = Result<Bytes, IndexError>> + Send {
            let value = self.artifacts.get(&request.hash).cloned();
            async move { value.ok_or(IndexError::Integrity) }
        }

        fn try_fork_query_loader(&self) -> Result<Option<Self>, IndexError> {
            Ok(Some(self.clone()))
        }
    }

    #[derive(Clone)]
    struct RecordingExecutor(Arc<AtomicUsize>);

    impl QueryPartitionExecutor for RecordingExecutor {
        fn maximum_parallelism(&self) -> usize {
            4
        }

        async fn execute_ordered<K, O>(
            &self,
            jobs: Vec<(K, QueryPartitionJob<O>)>,
        ) -> Result<Vec<(K, O)>, IndexError>
        where
            K: Copy + Ord + Send + 'static,
            O: Send + 'static,
        {
            self.0.fetch_add(jobs.len(), Ordering::AcqRel);
            let mut outcomes = Vec::with_capacity(jobs.len());
            for (key, job) in jobs.into_iter().rev() {
                outcomes.push((key, job.await));
            }
            super::super::super::query_parallel::resolve_query_partition_results(outcomes)
        }

        async fn run_cpu<O>(
            &self,
            job: super::super::super::query_parallel::QueryCpuJob<O>,
        ) -> Result<O, IndexError>
        where
            O: Send + 'static,
        {
            job()
        }
    }

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("in-memory future unexpectedly yielded"),
        }
    }

    #[test]
    fn bounded_equal_postings_advance_without_rebuilding_the_suffix() {
        let block_limits = QueryBlockLimits::default_for_memory();
        let recipe = RecipeIdentity::new([3; 32]).unwrap();
        let value = ScalarValue::String("mutable".into());
        let documents = (1u8..=5)
            .map(|value| StableDocumentKey::from_bytes([value; 32]).unwrap())
            .collect::<Vec<_>>();
        let posting_records = documents
            .iter()
            .map(|document| {
                encode_posting(QueryPosting {
                    document: *document,
                    material_source_version: 1,
                    live: true,
                    position_block_hash: None,
                    positions: 0,
                })
                .unwrap()
            })
            .collect::<Vec<QueryBlockRecord>>();
        let mut encoding_credits =
            QueryBlockCredits::from_query_permit(Box::new(Permit(1024 * 1024))).unwrap();
        let posting = encode_query_block(
            QueryBlockKind::Posting,
            recipe,
            &posting_records,
            block_limits,
            &mut encoding_credits,
        )
        .unwrap();
        let term_record = encode_term_entry(&QueryTermEntry {
            term: value.clone(),
            posting_shards: vec![QueryPostingShard {
                posting_block_hash: posting.descriptor.hash,
                posting_records: posting.descriptor.records,
                minimum_document: documents[0],
                maximum_document: documents[4],
            }],
        })
        .unwrap();
        let dictionary = encode_query_block(
            QueryBlockKind::TermDictionary,
            recipe,
            &[term_record],
            block_limits,
            &mut encoding_credits,
        )
        .unwrap();
        let partition = ProjectionPartitionIdentity::new([1; 32], 1, [2; 32], 1, 1, 1).unwrap();
        let packed =
            crate::v1::pack_query_blocks(vec![dictionary.clone(), posting.clone()]).unwrap();
        let pack_table = ArtifactPackTable::new(
            packed
                .packs
                .iter()
                .map(|pack| ArtifactPackReference {
                    ordinal: pack.ordinal,
                    canonical_path: format!(
                        "_keldra/index-projections/v1/test/artifacts/packs/{}",
                        pack.ordinal
                    )
                    .into(),
                    object_version: u64::from(pack.ordinal) + 1,
                    hash: pack.hash,
                    length: pack.bytes.len() as u64,
                })
                .collect(),
        )
        .unwrap();
        let (_, mut blocks) = packed.bind(pack_table).unwrap();
        blocks.sort_by(|left, right| {
            (
                left.kind,
                left.recipe,
                &left.minimum_key,
                &left.maximum_key,
                left.hash,
            )
                .cmp(&(
                    right.kind,
                    right.recipe,
                    &right.minimum_key,
                    &right.maximum_key,
                    right.hash,
                ))
        });
        let run = Arc::new(ProjectionQueryRunDescriptor {
            memory_lease: Default::default(),
            partition,
            physical_catalog_generation: [4; 32],
            sequence: 1,
            source_start_offset: 1,
            next_offset: 6,
            through_atomic_position: 1,
            pack_table: blocks[0].pack_table.clone(),
            blocks,
        });
        run.validate(block_limits).unwrap();
        let manifest = PartitionManifest {
            view: super::super::PartitionView {
                pin: crate::v1::PinnedPartitionQueryRoot {
                    partition,
                    physical_catalog_generation: [4; 32],
                    root: ProjectionQueryStreamRoot {
                        stream_root_hash: [5; 32],
                        stream_root_encoded_bytes: 1,
                        run_count: 1,
                        first_sequence: 1,
                        last_sequence: 1,
                        source_start_offset: 1,
                        next_offset: 6,
                        through_atomic_position: 1,
                    },
                    cut_proof: QueryRootCutProof {
                        common_cut: QueryCommonCut {
                            through_atomic_position: 1,
                        },
                        selected_stream_root_hash: [5; 32],
                        next_newer_through_atomic_position: None,
                    },
                    handoff_lineage_id: [6; 32],
                },
            },
            runs: vec![run],
        };
        let mut loader = Loader {
            artifacts: [
                (dictionary.descriptor.hash, Bytes::from(dictionary.bytes)),
                (posting.descriptor.hash, Bytes::from(posting.bytes)),
            ]
            .into(),
            decoded: BTreeMap::new(),
            loads: BTreeMap::new(),
        };

        let mut resume = None;
        for (expected, expected_truncated) in [
            (&documents[0..2], true),
            (&documents[2..4], true),
            (&documents[4..5], false),
        ] {
            let mut credits =
                QueryBlockCredits::from_query_permit(Box::new(Permit(1024 * 1024))).unwrap();
            let mut budget = Budget::new(super::super::QueryExecutionLimits::default_for_memory());
            let (page, truncated) = ready(load_bounded_equal_postings(
                &mut loader,
                &super::super::SerialQueryPartitionExecutor,
                &manifest,
                recipe,
                &value,
                resume,
                2,
                block_limits,
                &mut credits,
                &mut budget,
            ))
            .unwrap();
            assert_eq!(page.keys().copied().collect::<Vec<_>>(), expected);
            assert_eq!(truncated, expected_truncated);
            resume = page.keys().next_back().copied();
        }
        assert_eq!(loader.loads[&posting.descriptor.hash], 1);
        assert_eq!(loader.loads[&dictionary.descriptor.hash], 1);
    }

    #[test]
    fn independent_natural_posting_sources_refill_through_the_bounded_executor() {
        let block_limits = QueryBlockLimits::default_for_memory();
        let recipe = RecipeIdentity::new([13; 32]).unwrap();
        let documents = [
            StableDocumentKey::from_bytes([1; 32]).unwrap(),
            StableDocumentKey::from_bytes([2; 32]).unwrap(),
        ];
        let mut encoding_credits =
            QueryBlockCredits::from_query_permit(Box::new(Permit(1024 * 1024))).unwrap();
        let blocks = documents.map(|document| {
            encode_query_block(
                QueryBlockKind::Posting,
                recipe,
                &[encode_posting(QueryPosting {
                    document,
                    material_source_version: 1,
                    live: true,
                    position_block_hash: None,
                    positions: 0,
                })
                .unwrap()],
                block_limits,
                &mut encoding_credits,
            )
            .unwrap()
        });
        let descriptors = blocks.each_ref().map(|encoded| {
            let pack_table = Arc::new(
                ArtifactPackTable::new(vec![ArtifactPackReference {
                    ordinal: 0,
                    canonical_path: "_keldra/index-projections/v1/test/packs/0".into(),
                    object_version: 1,
                    hash: encoded.descriptor.hash,
                    length: encoded.descriptor.encoded_bytes,
                }])
                .unwrap(),
            );
            super::super::QueryBlockDescriptor {
                kind: encoded.descriptor.kind,
                recipe: encoded.descriptor.recipe,
                minimum_key: encoded.descriptor.minimum_key.clone(),
                maximum_key: encoded.descriptor.maximum_key.clone(),
                hash: encoded.descriptor.hash,
                encoded_bytes: encoded.descriptor.encoded_bytes,
                records: encoded.descriptor.records,
                documents: encoded.descriptor.documents.clone(),
                locator: crate::v1::ArtifactPackLocator {
                    ordinal: 0,
                    offset: 0,
                    encoded_bytes: encoded.descriptor.encoded_bytes,
                    logical_bytes: encoded.descriptor.encoded_bytes,
                    checksum: encoded.descriptor.hash,
                },
                pack_table,
            }
        });
        let mut loader = ForkLoader {
            artifacts: Arc::new(
                [
                    (
                        blocks[0].descriptor.hash,
                        Bytes::from(blocks[0].bytes.clone()),
                    ),
                    (
                        blocks[1].descriptor.hash,
                        Bytes::from(blocks[1].bytes.clone()),
                    ),
                ]
                .into(),
            ),
        };
        let mut sources = descriptors
            .iter()
            .enumerate()
            .map(|(index, descriptor)| PostingSource {
                run: index,
                generation: [4; 32],
                descriptor,
                minimum_document: documents[index],
                maximum_document: documents[index],
                resume: None,
                buffered: VecDeque::new(),
                exhausted: false,
            })
            .collect::<Vec<_>>();
        let submitted = Arc::new(AtomicUsize::new(0));
        let executor = RecordingExecutor(submitted.clone());
        let mut query_credits =
            QueryBlockCredits::from_query_permit(Box::new(Permit(1024 * 1024))).unwrap();
        let mut query_budget = Budget::new(QueryExecutionLimits::default_for_memory());

        ready(refill_posting_sources(
            &mut loader,
            &executor,
            &mut sources,
            vec![0, 1],
            block_limits,
            &mut query_credits,
            &mut query_budget,
        ))
        .unwrap();

        assert_eq!(submitted.load(Ordering::Acquire), 2);
        assert_eq!(
            sources[0].buffered.front().unwrap().document().unwrap(),
            documents[0]
        );
        assert_eq!(
            sources[1].buffered.front().unwrap().document().unwrap(),
            documents[1]
        );
    }
}
