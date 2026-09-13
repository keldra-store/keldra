use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};

use crate::IndexError;
use crate::typed_json::{Predicate, ScalarValue};

use super::admission::resident_selected_candidate_bytes;
use super::{
    AuthorizedQueryCandidate, Budget, PartitionManifest, QueryAdmissionCandidate,
    QueryArtifactLoader, QueryBlockCredits, QueryBlockDescriptor, QueryBlockKind, QueryBlockLimits,
    QueryCandidateAdmission, QueryCommonCut, QueryPosting, StableDocumentKey,
    TypedJsonQueryRequest, authorize_selected_candidates, candidate_is_current, decode_posting,
    load_latest_gates_for_keys, load_selected_terms, select_handoff_candidate,
};

const POSTING_SOURCE_CHUNK: usize = 32;

/// Natural-order equality pages are the common exact-query path. Keep their
/// work proportional to the requested page instead of rebuilding the complete
/// suffix of the posting list for every continuation token.
#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_bounded_natural_equal<
    L: QueryArtifactLoader,
    A: QueryCandidateAdmission,
>(
    loader: &mut L,
    admission: &mut A,
    common_cut: QueryCommonCut,
    manifests: &[PartitionManifest<'_>],
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
        let mut partition_pages = Vec::with_capacity(manifests.len());
        let mut safe_through = None;
        let mut more = false;
        for manifest in manifests {
            let page = scan_equal_partition_page(
                loader,
                manifest,
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
            if page.truncated {
                more = true;
                let through = page.scan_through.ok_or(IndexError::Integrity)?;
                safe_through = Some(
                    safe_through.map_or(through, |current: StableDocumentKey| current.min(through)),
                );
            }
            partition_pages.push(page);
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
async fn scan_equal_partition_page<L: QueryArtifactLoader>(
    loader: &mut L,
    manifest: &PartitionManifest<'_>,
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
                covered_through_source_position: manifest
                    .view
                    .pin
                    .covered_through_source_position()?,
                document,
                material_source_version: posting.material_source_version,
                current_source_version: gate.current_source_version,
                source_path: gate.source_path.clone().ok_or(IndexError::Integrity)?,
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
async fn load_bounded_equal_postings<L: QueryArtifactLoader>(
    loader: &mut L,
    manifest: &PartitionManifest<'_>,
    recipe: super::RecipeIdentity,
    value: &ScalarValue,
    resume: Option<StableDocumentKey>,
    limit: usize,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<(BTreeMap<StableDocumentKey, (usize, QueryPosting)>, bool), IndexError> {
    let mut sources = Vec::new();
    for (run, descriptor) in manifest.matching_blocks(QueryBlockKind::TermDictionary, recipe) {
        let (entries, entry_bytes) = load_selected_terms(
            loader,
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
    for (source_index, source) in sources.iter_mut().enumerate() {
        refill_posting_source(loader, source, block_limits, credits, budget).await?;
        if let Some(posting) = source.buffered.front() {
            heap.push(Reverse((posting.document, source_index)));
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

        let mut selected = None::<(usize, QueryPosting)>;
        for source_index in same_document_sources {
            let source = sources.get_mut(source_index).ok_or(IndexError::Integrity)?;
            let posting = source.buffered.pop_front().ok_or(IndexError::Integrity)?;
            if posting.document != document {
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
                refill_posting_source(loader, source, block_limits, credits, budget).await?;
            }
            if let Some(next) = source.buffered.front() {
                heap.push(Reverse((next.document, source_index)));
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

struct PostingSource<'a> {
    run: usize,
    generation: [u8; 32],
    descriptor: &'a QueryBlockDescriptor,
    minimum_document: StableDocumentKey,
    maximum_document: StableDocumentKey,
    resume: Option<StableDocumentKey>,
    buffered: VecDeque<QueryPosting>,
    exhausted: bool,
}

#[allow(clippy::too_many_arguments)]
async fn refill_posting_source<L: QueryArtifactLoader>(
    loader: &mut L,
    source: &mut PostingSource<'_>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<(), IndexError> {
    if source.exhausted || !source.buffered.is_empty() {
        return Ok(());
    }
    let encoded_bytes =
        usize::try_from(source.descriptor.encoded_bytes).map_err(|_| IndexError::Integrity)?;
    let block = super::load_decoded_block(
        loader,
        source.generation,
        source.descriptor,
        block_limits,
        credits,
        budget,
    )
    .await?;
    let minimum = source
        .resume
        .filter(|minimum| source.minimum_document <= *minimum)
        .unwrap_or(source.minimum_document);
    let mut exhausted = true;
    for record in block.records_from(&minimum.bytes()) {
        let posting = decode_posting(record)?;
        if posting.document < source.minimum_document || posting.document > source.maximum_document
        {
            return Err(IndexError::Integrity);
        }
        if source
            .resume
            .is_none_or(|minimum| posting.document > minimum)
        {
            source.resume = Some(posting.document);
            budget.reserve_heap(credits, posting_buffer_bytes())?;
            source.buffered.push_back(posting);
            if source.buffered.len() == POSTING_SOURCE_CHUNK {
                exhausted = false;
                break;
            }
        }
    }
    source.exhausted = exhausted;
    credits.release_loaded_block(encoded_bytes)?;
    Ok(())
}

const fn posting_entry_bytes() -> usize {
    std::mem::size_of::<StableDocumentKey>() + std::mem::size_of::<QueryPosting>()
}

const fn posting_buffer_bytes() -> usize {
    std::mem::size_of::<QueryPosting>()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    use bytes::Bytes;

    use super::*;
    use crate::typed_json::ScalarValue;
    use crate::v1::{
        ProjectionPartitionIdentity, ProjectionQueryRunDescriptor, ProjectionQueryStreamRoot,
        QueryArtifactLoad, QueryBlockRecord, QueryMemoryPermit, QueryPostingShard,
        QueryRecipeCatalogProof, QueryRootCutProof, QueryTermEntry, RecipeIdentity, encode_posting,
        encode_query_block, encode_term_entry,
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
        let mut blocks = vec![dictionary.descriptor.clone(), posting.descriptor.clone()];
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
            partition,
            physical_catalog_generation: [4; 32],
            sequence: 1,
            source_start_offset: 1,
            next_offset: 6,
            through_atomic_position: 1,
            blocks,
        });
        run.validate(block_limits).unwrap();
        let catalog_ordinals = BTreeMap::new();
        let recipe_catalog_proofs = Vec::<QueryRecipeCatalogProof>::new();
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
                catalog_ordinals: &catalog_ordinals,
                recipe_catalog_proofs: &recipe_catalog_proofs,
            },
            runs: vec![run],
            resident_bytes: 0,
            index_bytes: 0,
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
            let mut budget = Budget {
                limits: super::super::QueryExecutionLimits::default_for_memory(),
                evidence: super::super::QueryLoadEvidence::default(),
                heap_bytes: 0,
            };
            let (page, truncated) = ready(load_bounded_equal_postings(
                &mut loader,
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
        assert_eq!(loader.loads[&dictionary.descriptor.hash], 3);
    }
}
