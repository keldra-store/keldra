use super::*;

pub(super) struct PostingBlockWork {
    pub(super) term: ScalarValue,
    pub(super) run: usize,
    pub(super) generation: [u8; 32],
    pub(super) descriptor: QueryBlockDescriptor,
    pub(super) minimum_document: StableDocumentKey,
    pub(super) maximum_document: StableDocumentKey,
}

pub(super) async fn load_decoded_block<L: QueryArtifactLoader, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    generation: [u8; 32],
    descriptor: &QueryBlockDescriptor,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Arc<DecodedQueryBlock>, IndexError> {
    let encoded_bytes =
        usize::try_from(descriptor.encoded_bytes).map_err(|_| IndexError::Integrity)?;
    let request = QueryArtifactLoad::packed(descriptor)?;
    loop {
        if let Some(block) = loader.cached_query_block(generation, request.clone())? {
            if !block.matches_descriptor(descriptor) {
                return Err(IndexError::Integrity);
            }
            budget.load(QueryArtifactKind::Block, encoded_bytes)?;
            credits.reserve_loaded_block(encoded_bytes, block_limits.maximum_loaded_blocks)?;
            return Ok(block);
        }
        let _leadership = match loader
            .coordinate_query_block_population(generation, request.clone())
            .await?
        {
            QueryPopulation::Completed => continue,
            QueryPopulation::Lead(leadership) => leadership,
        };
        let populated = async {
            let bytes = load_exact_pre_admitted(loader, request.clone(), credits, budget).await?;
            credits.release(bytes.len())?;
            let descriptor_for_decode = descriptor.clone();
            let mut decode_credits = credits.try_fork_query()?;
            let block = executor
                .run_cpu(Box::new(move || {
                    Ok(Arc::new(DecodedQueryBlock::from_verified_content(
                        &descriptor_for_decode,
                        bytes,
                        block_limits,
                        &mut decode_credits,
                    )?))
                }))
                .await?;
            loader.cache_query_block(generation, request.clone(), block.clone());
            credits.reserve_loaded_block(encoded_bytes, block_limits.maximum_loaded_blocks)?;
            Ok(block)
        }
        .await;
        return populated;
    }
}

pub(super) async fn decode_gate_block<L: QueryArtifactLoader, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    generation: [u8; 32],
    descriptor: &QueryBlockDescriptor,
    kind: QueryBlockKind,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Vec<QueryDocumentGate>, IndexError> {
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
    let records = descriptor.records as usize;
    let mut cpu_budget = budget.clone();
    let mut cpu_credits = credits.try_fork_query()?;
    executor
        .run_cpu(Box::new(move || {
            let mut gates = Vec::with_capacity(records);
            for record in block.records() {
                let gate = decode_document_gate(record)?;
                if (kind == QueryBlockKind::Gate) != gate.source_path.is_some() {
                    return Err(IndexError::Integrity);
                }
                cpu_budget.reserve_heap(&mut cpu_credits, resident_gate_bytes(&gate)?)?;
                gates.push(gate);
            }
            cpu_credits.release_loaded_block(block.encoded_bytes())?;
            Ok(gates)
        }))
        .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn decode_posting_block<L: QueryArtifactLoader, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    generation: [u8; 32],
    descriptor: &QueryBlockDescriptor,
    minimum_document: StableDocumentKey,
    maximum_document: StableDocumentKey,
    minimum_document_exclusive: Option<StableDocumentKey>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Vec<QueryPosting>, IndexError> {
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
    let mut cpu_budget = budget.clone();
    let mut cpu_credits = credits.try_fork_query()?;
    executor
        .run_cpu(Box::new(move || {
            let minimum = minimum_document_exclusive
                .filter(|minimum| minimum_document <= *minimum)
                .map_or(minimum_document.bytes(), |minimum| minimum.bytes());
            let mut postings = Vec::new();
            for record in block.records_from(&minimum) {
                let posting = decode_posting(record)?;
                if posting.document < minimum_document || posting.document > maximum_document {
                    return Err(IndexError::Integrity);
                }
                if minimum_document_exclusive.is_none_or(|minimum| posting.document > minimum) {
                    cpu_budget.reserve_heap(&mut cpu_credits, posting_entry_bytes())?;
                    postings.push(posting);
                }
            }
            cpu_credits.release_loaded_block(block.encoded_bytes())?;
            Ok(postings)
        }))
        .await
}

pub(super) const fn posting_entry_bytes() -> usize {
    std::mem::size_of::<StableDocumentKey>() + std::mem::size_of::<QueryPosting>()
}

pub(super) fn resident_point_entry_bytes(value: &ScalarValue) -> usize {
    std::mem::size_of::<(ScalarValue, StableDocumentKey)>()
        .saturating_add(std::mem::size_of::<(bool, u64)>())
        .saturating_add(resident_scalar_bytes(value))
}

pub(super) fn point_descriptor_overlaps(
    descriptor: &QueryBlockDescriptor,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
) -> bool {
    lower.is_none_or(|key| descriptor.maximum_key.as_slice() >= key)
        && upper.is_none_or(|key| descriptor.minimum_key.as_slice() <= key)
}

fn in_range(value: &ScalarValue, lower: Option<&RangeBound>, upper: Option<&RangeBound>) -> bool {
    lower.is_none_or(|bound| value > &bound.value || bound.inclusive && value == &bound.value)
        && upper
            .is_none_or(|bound| value < &bound.value || bound.inclusive && value == &bound.value)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn decode_range_block<L: QueryArtifactLoader, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    generation: [u8; 32],
    descriptor: &QueryBlockDescriptor,
    lower: Option<&RangeBound>,
    upper: Option<&RangeBound>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Vec<QueryPoint>, IndexError> {
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
    let lower = lower.cloned();
    let upper = upper.cloned();
    let mut cpu_budget = budget.clone();
    let mut cpu_credits = credits.try_fork_query()?;
    executor
        .run_cpu(Box::new(move || {
            let mut points = Vec::new();
            for record in block.records() {
                let point = decode_point(record)?;
                if in_range(&point.value, lower.as_ref(), upper.as_ref()) {
                    cpu_budget
                        .reserve_heap(&mut cpu_credits, resident_point_entry_bytes(&point.value))?;
                    points.push(point);
                }
            }
            cpu_credits.release_loaded_block(block.encoded_bytes())?;
            Ok(points)
        }))
        .await
}

pub(super) type DecodedPositionEntry = ((ScalarValue, StableDocumentKey), Vec<u32>, usize);

#[allow(clippy::too_many_arguments)]
pub(super) async fn decode_position_block<L: QueryArtifactLoader, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    generation: [u8; 32],
    descriptor: &QueryBlockDescriptor,
    term: &ScalarValue,
    documents: &[StableDocumentKey],
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Vec<DecodedPositionEntry>, IndexError> {
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
    let term = term.clone();
    let documents = documents.to_vec();
    let mut cpu_budget = budget.clone();
    let mut cpu_credits = credits.try_fork_query()?;
    executor
        .run_cpu(Box::new(move || {
            let mut output = Vec::with_capacity(documents.len());
            for document in documents {
                if let Some(record) = block.records_from(&document.bytes()).next() {
                    let decoded = decode_positions(record, block_limits)?;
                    if decoded.document != document {
                        return Err(IndexError::Integrity);
                    }
                    let resident = std::mem::size_of::<(ScalarValue, StableDocumentKey)>()
                        .checked_add(std::mem::size_of::<Vec<u32>>())
                        .and_then(|bytes| bytes.checked_add(resident_scalar_bytes(&term)))
                        .and_then(|bytes| {
                            decoded
                                .positions
                                .len()
                                .checked_mul(std::mem::size_of::<u32>())
                                .and_then(|positions| bytes.checked_add(positions))
                        })
                        .ok_or(IndexError::OffsetOverflow)?;
                    cpu_budget.reserve_heap(&mut cpu_credits, resident)?;
                    output.push(((term.clone(), document), decoded.positions, resident));
                }
            }
            cpu_credits.release_loaded_block(block.encoded_bytes())?;
            Ok(output)
        }))
        .await
}
