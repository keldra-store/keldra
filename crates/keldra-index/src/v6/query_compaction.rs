//! Production compaction for encoded v6 query mini-runs.
//!
//! The selected whole-run window is verified before merge state is allocated.
//! Newest records win by their canonical semantic key; tombstones remain so
//! data in older, unselected levels cannot be resurrected.

use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;

use crate::IndexError;
use crate::typed_json::ScalarValue;

use super::{
    ChargedProjectionQueryRunArtifacts, PreparedQueryFieldDelta, PreparedQueryMembershipDelta,
    PreparedQueryMutationBatch, PreparedQueryRecipeDelta, PreparedQueryRunSplice,
    ProjectionPartitionIdentity, ProjectionQueryRunArtifacts, ProjectionQueryRunDescriptor,
    ProjectionQueryStreamRoot, QueryBlockCredits, QueryBlockCursor, QueryBlockDescriptor,
    QueryBlockKind, QueryBlockLimits, QueryDocValue, QueryDocumentGate, QueryPoint, QueryPosting,
    QueryRunCompactionPlan, QueryRunReference, RecipeIdentity, StableDocumentKey, decode_doc_value,
    decode_document_gate, decode_point, decode_positions, decode_posting,
    decode_projection_query_run, decode_term_entry, prepare_projection_query_run,
    splice_compacted_query_runs,
};

#[derive(Debug)]
pub struct ChargedQueryRunCompaction {
    artifacts: ProjectionQueryRunArtifacts,
    reference: QueryRunReference,
    splice: PreparedQueryRunSplice,
    _credits: QueryBlockCredits,
}

impl ChargedQueryRunCompaction {
    pub const fn artifacts(&self) -> &ProjectionQueryRunArtifacts {
        &self.artifacts
    }

    pub const fn reference(&self) -> QueryRunReference {
        self.reference
    }

    pub const fn splice(&self) -> &PreparedQueryRunSplice {
        &self.splice
    }

    pub fn into_parts(
        self,
    ) -> (
        ProjectionQueryRunArtifacts,
        QueryRunReference,
        PreparedQueryRunSplice,
    ) {
        (self.artifacts, self.reference, self.splice)
    }
}

/// Merge one selected same-level window and return both immutable run
/// artifacts and the exact path-copy replacement for the pinned stream root.
#[allow(clippy::too_many_arguments)]
pub fn compact_encoded_query_runs(
    previous: ProjectionQueryStreamRoot,
    plan: &QueryRunCompactionPlan,
    partition: ProjectionPartitionIdentity,
    physical_catalog_generation: [u8; 32],
    limits: QueryBlockLimits,
    mut credits: QueryBlockCredits,
    mut load_run: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    mut load_block: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    mut load_page: impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
) -> Result<ChargedQueryRunCompaction, IndexError> {
    let limits = limits.validate()?;
    partition.validate()?;
    if physical_catalog_generation == [0; 32] {
        return invalid("v6 query compaction catalog is zero");
    }
    let inputs = plan.inputs_newest_first();
    if inputs.is_empty() {
        return invalid("v6 query compaction has no inputs");
    }

    // Two passes are intentional: reject every fixed identity/cut mismatch
    // before descriptor vectors or semantic merge maps are allocated.
    for reference in inputs {
        let bytes = load_run(reference.hash)?;
        validate_projection_query_run_fixed(
            &bytes,
            reference.hash,
            partition,
            physical_catalog_generation,
            reference.sequence,
            reference.source_start_offset,
            reference.next_offset,
            reference.through_atomic_position,
            limits,
        )?;
    }
    let mut runs = Vec::with_capacity(inputs.len());
    for reference in inputs {
        let bytes = load_run(reference.hash)?;
        let descriptor = decode_projection_query_run(&bytes, limits, &mut credits)?;
        validate_descriptor_reference(
            &descriptor,
            *reference,
            partition,
            physical_catalog_generation,
        )?;
        runs.push(descriptor);
    }

    let mut membership = BTreeMap::<(RecipeIdentity, StableDocumentKey), QueryDocumentGate>::new();
    let mut presence = BTreeMap::<(RecipeIdentity, StableDocumentKey), QueryDocumentGate>::new();
    let mut doc_values = BTreeMap::<(RecipeIdentity, StableDocumentKey), QueryDocValue>::new();
    let mut points =
        BTreeMap::<(RecipeIdentity, ScalarValue, StableDocumentKey), QueryPoint>::new();
    let mut terms = BTreeMap::<
        (RecipeIdentity, ScalarValue, StableDocumentKey),
        super::PreparedQueryTermDelta,
    >::new();

    for run in &runs {
        for descriptor in &run.blocks {
            match descriptor.kind {
                QueryBlockKind::Gate => visit_block(
                    descriptor,
                    limits,
                    &mut credits,
                    &mut load_block,
                    &mut |record, credits| {
                        let value = decode_document_gate(record)?;
                        if value.source_path.is_none() {
                            return Err(IndexError::Integrity);
                        }
                        let heap = value.source_path.as_ref().map_or(0, String::len)
                            + value.result_path.as_ref().map_or(0, String::len);
                        insert_newest(
                            &mut membership,
                            (descriptor.recipe, value.document),
                            value,
                            heap,
                            credits,
                        )
                    },
                )?,
                QueryBlockKind::Presence => visit_block(
                    descriptor,
                    limits,
                    &mut credits,
                    &mut load_block,
                    &mut |record, credits| {
                        let value = decode_document_gate(record)?;
                        if value.source_path.is_some()
                            || value.result_path.is_some()
                            || value.result_version != 0
                        {
                            return Err(IndexError::Integrity);
                        }
                        insert_newest(
                            &mut presence,
                            (descriptor.recipe, value.document),
                            value,
                            0,
                            credits,
                        )
                    },
                )?,
                QueryBlockKind::DocValue => visit_block(
                    descriptor,
                    limits,
                    &mut credits,
                    &mut load_block,
                    &mut |record, credits| {
                        let value = decode_doc_value(record, limits)?;
                        let heap = value
                            .value
                            .as_ref()
                            .map(|values| values.iter().map(scalar_heap).sum())
                            .unwrap_or(0);
                        insert_newest(
                            &mut doc_values,
                            (descriptor.recipe, value.document),
                            value,
                            heap,
                            credits,
                        )
                    },
                )?,
                QueryBlockKind::Point => visit_block(
                    descriptor,
                    limits,
                    &mut credits,
                    &mut load_block,
                    &mut |record, credits| {
                        let value = decode_point(record)?;
                        let heap = scalar_heap(&value.value);
                        insert_newest(
                            &mut points,
                            (descriptor.recipe, value.value.clone(), value.document),
                            value,
                            heap,
                            credits,
                        )
                    },
                )?,
                _ => {}
            }
        }
        merge_run_terms(run, limits, &mut credits, &mut load_block, &mut terms)?;
    }

    let batch = build_batch(
        membership,
        presence,
        doc_values,
        points,
        terms,
        &mut credits,
    )?;
    let newest = *inputs.first().ok_or(IndexError::Integrity)?;
    let charged: ChargedProjectionQueryRunArtifacts = prepare_projection_query_run(
        partition,
        physical_catalog_generation,
        newest.sequence,
        plan.source_start_offset(),
        plan.next_offset(),
        plan.through_atomic_position(),
        batch,
        limits,
        credits,
    )?;
    let (artifacts, credits) = charged.into_parts();
    let reference = QueryRunReference {
        hash: artifacts.run.hash,
        sequence: newest.sequence,
        level: plan.output_level(),
        source_start_offset: plan.source_start_offset(),
        next_offset: plan.next_offset(),
        through_atomic_position: plan.through_atomic_position(),
    };
    let splice = splice_compacted_query_runs(previous, plan, reference, &mut load_page)?;
    Ok(ChargedQueryRunCompaction {
        artifacts,
        reference,
        splice,
        _credits: credits,
    })
}

fn validate_descriptor_reference(
    run: &ProjectionQueryRunDescriptor,
    reference: QueryRunReference,
    partition: ProjectionPartitionIdentity,
    catalog: [u8; 32],
) -> Result<(), IndexError> {
    if run.partition != partition
        || run.physical_catalog_generation != catalog
        || run.sequence != reference.sequence
        || run.source_start_offset != reference.source_start_offset
        || run.next_offset != reference.next_offset
        || run.through_atomic_position != reference.through_atomic_position
    {
        return Err(IndexError::Integrity);
    }
    Ok(())
}

fn validate_projection_query_run_fixed(
    bytes: &[u8],
    hash: [u8; 32],
    partition: ProjectionPartitionIdentity,
    catalog: [u8; 32],
    sequence: u64,
    source_start_offset: u64,
    next_offset: u64,
    atomic: u64,
    limits: QueryBlockLimits,
) -> Result<(), IndexError> {
    if bytes.len() > limits.maximum_run_descriptor_bytes
        || bytes.len() < 206
        || *blake3::hash(bytes).as_bytes() != hash
    {
        return Err(IndexError::Integrity);
    }
    let payload = &bytes[..bytes.len() - 32];
    if blake3::hash(payload).as_bytes() != &bytes[bytes.len() - 32..]
        || &payload[..8] != b"K6QRUN01"
        || u16::from_be_bytes(
            payload[8..10]
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        ) != 1
        || payload[10..42] != partition.family_id
        || read_u64(payload, 42)? != partition.source_node
        || payload[50..82] != partition.source_epoch
        || read_u64(payload, 82)? != partition.producer_node
        || read_u64(payload, 90)? != partition.placement_term
        || read_u64(payload, 98)? != partition.placement_index
        || payload[106..138] != catalog
        || read_u64(payload, 138)? != sequence
        || read_u64(payload, 146)? != source_start_offset
        || read_u64(payload, 154)? != next_offset
        || read_u64(payload, 162)? != atomic
    {
        return Err(IndexError::Integrity);
    }
    let count = u32::from_be_bytes(
        payload[170..174]
            .try_into()
            .map_err(|_| IndexError::Integrity)?,
    ) as usize;
    if count > limits.maximum_loaded_blocks.saturating_mul(4096) {
        return Err(IndexError::InvalidFormat("v6 query run block count"));
    }
    Ok(())
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, IndexError> {
    Ok(u64::from_be_bytes(
        bytes
            .get(offset..offset + 8)
            .ok_or(IndexError::Integrity)?
            .try_into()
            .map_err(|_| IndexError::Integrity)?,
    ))
}

fn insert_newest<K: Ord, V>(
    map: &mut BTreeMap<K, V>,
    key: K,
    value: V,
    heap_bytes: usize,
    credits: &mut QueryBlockCredits,
) -> Result<(), IndexError> {
    if let std::collections::btree_map::Entry::Vacant(entry) = map.entry(key) {
        credits.reserve(
            size_of::<K>()
                .saturating_add(size_of::<V>())
                .saturating_add(heap_bytes)
                .saturating_add(64),
        )?;
        entry.insert(value);
    }
    Ok(())
}

fn visit_block(
    descriptor: &QueryBlockDescriptor,
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    load: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    visit: &mut impl FnMut(
        super::QueryBlockRecordRef<'_>,
        &mut QueryBlockCredits,
    ) -> Result<(), IndexError>,
) -> Result<(), IndexError> {
    let bytes = load(descriptor.hash)?;
    let mut cursor = QueryBlockCursor::new(descriptor, &bytes, limits, credits)?;
    let result = (|| {
        while let Some(record) = cursor.next()? {
            visit(record, credits)?;
        }
        Ok(())
    })();
    drop(cursor);
    credits.release_loaded_block(bytes.len())?;
    result
}

fn merge_run_terms(
    run: &ProjectionQueryRunDescriptor,
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    load: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    output: &mut BTreeMap<
        (RecipeIdentity, ScalarValue, StableDocumentKey),
        super::PreparedQueryTermDelta,
    >,
) -> Result<(), IndexError> {
    for dictionary in run
        .blocks
        .iter()
        .filter(|block| block.kind == QueryBlockKind::TermDictionary)
    {
        let mut entries = Vec::new();
        visit_block(dictionary, limits, credits, load, &mut |record, credits| {
            let entry = decode_term_entry(record, limits)?;
            credits.reserve(
                size_of::<super::QueryTermEntry>()
                    .saturating_add(
                        entry
                            .posting_shards
                            .len()
                            .saturating_mul(size_of::<super::QueryPostingShard>()),
                    )
                    .saturating_add(scalar_heap(&entry.term)),
            )?;
            entries.push(entry);
            Ok(())
        })?;
        for entry in entries {
            for shard in entry.posting_shards {
                let posting_descriptor = find_block(
                    run,
                    shard.posting_block_hash,
                    QueryBlockKind::Posting,
                    dictionary.recipe,
                )?;
                if posting_descriptor.records != shard.posting_records
                    || posting_descriptor.minimum_key != shard.minimum_document.bytes()
                    || posting_descriptor.maximum_key != shard.maximum_document.bytes()
                {
                    return Err(IndexError::Integrity);
                }
                let mut winners = Vec::<QueryPosting>::new();
                visit_block(
                    posting_descriptor,
                    limits,
                    credits,
                    load,
                    &mut |record, credits| {
                        let posting = decode_posting(record)?;
                        let key = (dictionary.recipe, entry.term.clone(), posting.document);
                        if !output.contains_key(&key) {
                            credits.reserve(
                                size_of::<QueryPosting>()
                                    .saturating_add(scalar_heap(&entry.term))
                                    .saturating_add(128),
                            )?;
                            winners.push(posting);
                        }
                        Ok(())
                    },
                )?;
                let needed_hashes = winners
                    .iter()
                    .filter_map(|posting| posting.position_block_hash)
                    .collect::<BTreeSet<_>>();
                let mut positions = BTreeMap::<([u8; 32], StableDocumentKey), Vec<u32>>::new();
                for hash in needed_hashes {
                    let descriptor =
                        find_block(run, hash, QueryBlockKind::Position, dictionary.recipe)?;
                    visit_block(descriptor, limits, credits, load, &mut |record, credits| {
                        let value = decode_positions(record, limits)?;
                        credits.reserve(
                            value
                                .positions
                                .len()
                                .saturating_mul(size_of::<u32>())
                                .saturating_add(96),
                        )?;
                        positions.insert((hash, value.document), value.positions);
                        Ok(())
                    })?;
                }
                for posting in winners {
                    let values = match posting.position_block_hash {
                        Some(hash) => positions
                            .remove(&(hash, posting.document))
                            .filter(|values| values.len() == posting.positions as usize)
                            .ok_or(IndexError::Integrity)?,
                        None => Vec::new(),
                    };
                    let key = (dictionary.recipe, entry.term.clone(), posting.document);
                    output.entry(key).or_insert(super::PreparedQueryTermDelta {
                        term: entry.term.clone(),
                        document: posting.document,
                        material_source_version: posting.material_source_version,
                        live: posting.live,
                        positions: values,
                    });
                }
            }
        }
    }
    Ok(())
}

fn find_block(
    run: &ProjectionQueryRunDescriptor,
    hash: [u8; 32],
    kind: QueryBlockKind,
    recipe: RecipeIdentity,
) -> Result<&QueryBlockDescriptor, IndexError> {
    let mut found = run
        .blocks
        .iter()
        .filter(|block| block.hash == hash && block.kind == kind && block.recipe == recipe);
    let block = found.next().ok_or(IndexError::Integrity)?;
    if found.next().is_some() {
        return Err(IndexError::Integrity);
    }
    Ok(block)
}

fn build_batch(
    membership: BTreeMap<(RecipeIdentity, StableDocumentKey), QueryDocumentGate>,
    presence: BTreeMap<(RecipeIdentity, StableDocumentKey), QueryDocumentGate>,
    mut doc_values: BTreeMap<(RecipeIdentity, StableDocumentKey), QueryDocValue>,
    points: BTreeMap<(RecipeIdentity, ScalarValue, StableDocumentKey), QueryPoint>,
    terms: BTreeMap<
        (RecipeIdentity, ScalarValue, StableDocumentKey),
        super::PreparedQueryTermDelta,
    >,
    credits: &mut QueryBlockCredits,
) -> Result<PreparedQueryMutationBatch, IndexError> {
    let mut membership_by_recipe = BTreeMap::<RecipeIdentity, Vec<QueryDocumentGate>>::new();
    for ((recipe, _), gate) in membership {
        credits.reserve(size_of::<QueryDocumentGate>().saturating_add(64))?;
        membership_by_recipe.entry(recipe).or_default().push(gate);
    }
    if membership_by_recipe.len() > 1 {
        return invalid("v6 query compaction found multiple membership recipes");
    }
    let membership = membership_by_recipe
        .into_iter()
        .next()
        .map(|(recipe, gates)| PreparedQueryMembershipDelta { recipe, gates });

    let mut term_by_document =
        BTreeMap::<(RecipeIdentity, StableDocumentKey), Vec<super::PreparedQueryTermDelta>>::new();
    for ((recipe, _, document), term) in terms {
        credits.reserve(size_of::<super::PreparedQueryTermDelta>().saturating_add(64))?;
        term_by_document
            .entry((recipe, document))
            .or_default()
            .push(term);
    }
    let mut point_by_document =
        BTreeMap::<(RecipeIdentity, StableDocumentKey), Vec<QueryPoint>>::new();
    for ((recipe, _, document), point) in points {
        credits.reserve(size_of::<QueryPoint>().saturating_add(64))?;
        point_by_document
            .entry((recipe, document))
            .or_default()
            .push(point);
    }
    let mut fields = Vec::with_capacity(presence.len());
    credits.reserve(
        presence
            .len()
            .saturating_mul(size_of::<PreparedQueryRecipeDelta>()),
    )?;
    for ((recipe, document), gate) in presence {
        fields.push(PreparedQueryRecipeDelta {
            recipe,
            delta: PreparedQueryFieldDelta {
                presence: gate,
                doc_value: doc_values.remove(&(recipe, document)),
                terms: term_by_document
                    .remove(&(recipe, document))
                    .unwrap_or_default(),
                points: point_by_document
                    .remove(&(recipe, document))
                    .unwrap_or_default(),
            },
        });
    }
    if !doc_values.is_empty() || !term_by_document.is_empty() || !point_by_document.is_empty() {
        return Err(IndexError::Integrity);
    }
    Ok(PreparedQueryMutationBatch { membership, fields })
}

fn scalar_heap(value: &ScalarValue) -> usize {
    match value {
        ScalarValue::String(value) => value.len(),
        _ => 0,
    }
}

fn invalid<T>(message: &str) -> Result<T, IndexError> {
    Err(IndexError::InvalidDefinition(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v6::{
        IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage, PreparedQueryTermDelta,
        QueryRunCompactionLimits, append_query_run_path_copy, select_query_run_compaction,
    };

    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 2, [3; 32], 4, 5, 6).unwrap()
    }

    fn pool(bytes: usize) -> IndexingMemoryCredits {
        IndexingMemoryCredits::new(
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
        .unwrap()
    }

    fn credits(pool: &IndexingMemoryCredits, bytes: usize) -> QueryBlockCredits {
        QueryBlockCredits::from_pipeline_permit(
            pool.acquire(IndexingMemoryStage::OrderingCatalog, bytes)
                .unwrap(),
        )
    }

    fn document(byte: u8) -> StableDocumentKey {
        StableDocumentKey::from_bytes([byte; 32]).unwrap()
    }

    fn batch(version: u64, reordered: bool) -> PreparedQueryMutationBatch {
        let document = document(7);
        let recipe = RecipeIdentity::new([9; 32]).unwrap();
        let (alpha, beta) = if reordered { (1, 0) } else { (0, 1) };
        PreparedQueryMutationBatch {
            membership: Some(PreparedQueryMembershipDelta {
                recipe: RecipeIdentity::new([8; 32]).unwrap(),
                gates: vec![QueryDocumentGate {
                    document,
                    material_source_version: version,
                    current_source_version: version,
                    live: true,
                    source_path: Some("objects/document.json".into()),
                    result_path: Some("objects/document.json".into()),
                    result_version: version,
                }],
            }),
            fields: vec![PreparedQueryRecipeDelta {
                recipe,
                delta: PreparedQueryFieldDelta {
                    presence: QueryDocumentGate {
                        document,
                        material_source_version: version,
                        current_source_version: version,
                        live: true,
                        source_path: None,
                        result_path: None,
                        result_version: 0,
                    },
                    doc_value: Some(QueryDocValue {
                        document,
                        material_source_version: version,
                        value: Some(vec![
                            ScalarValue::Signed(version as i64),
                            ScalarValue::Signed(version as i64),
                        ]),
                    }),
                    terms: vec![
                        PreparedQueryTermDelta {
                            term: ScalarValue::String("alpha".into()),
                            document,
                            material_source_version: version,
                            live: true,
                            positions: vec![alpha],
                        },
                        PreparedQueryTermDelta {
                            term: ScalarValue::String("beta".into()),
                            document,
                            material_source_version: version,
                            live: true,
                            positions: vec![beta],
                        },
                    ],
                    points: vec![QueryPoint {
                        value: ScalarValue::Signed(version as i64),
                        document,
                        material_source_version: version,
                        live: true,
                    }],
                },
            }],
        }
    }

    type Store = BTreeMap<[u8; 32], Vec<u8>>;

    fn fixture() -> (
        ProjectionQueryStreamRoot,
        QueryRunCompactionPlan,
        Store,
        Store,
        Store,
    ) {
        let limits = QueryBlockLimits::default_for_memory();
        let mut runs = Store::new();
        let mut blocks = Store::new();
        let mut pages = Store::new();
        let mut root = None;
        for (sequence, reordered) in [(1, false), (2, true)] {
            let memory = pool(32 * 1024 * 1024);
            let charged = prepare_projection_query_run(
                partition(),
                [4; 32],
                sequence,
                sequence - 1,
                sequence,
                sequence,
                batch(sequence, reordered),
                limits,
                credits(&memory, 32 * 1024 * 1024),
            )
            .unwrap();
            let (artifacts, _) = charged.into_parts();
            let reference = QueryRunReference {
                hash: artifacts.run.hash,
                sequence,
                level: 0,
                source_start_offset: sequence - 1,
                next_offset: sequence,
                through_atomic_position: sequence,
            };
            runs.insert(artifacts.run.hash, artifacts.run.bytes);
            for block in artifacts.blocks {
                blocks.insert(block.descriptor.hash, block.bytes);
            }
            let append =
                append_query_run_path_copy(root, partition(), [4; 32], reference, |hash| {
                    pages.get(&hash).cloned().ok_or(IndexError::Integrity)
                })
                .unwrap();
            root = Some(append.root);
            for page in append.pages {
                pages.insert(page.hash, page.bytes);
            }
        }
        let root = root.unwrap();
        let plan = select_query_run_compaction(
            root,
            |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
            QueryRunCompactionLimits {
                level_trigger: 2,
                maximum_input_runs: 2,
            },
        )
        .unwrap()
        .unwrap();
        (root, plan, runs, blocks, pages)
    }

    #[test]
    fn production_compaction_builds_every_query_kind_and_exact_splice() {
        let (root, plan, runs, blocks, pages) = fixture();
        let memory = pool(128 * 1024 * 1024);
        let compacted = compact_encoded_query_runs(
            root,
            &plan,
            partition(),
            [4; 32],
            QueryBlockLimits::default_for_memory(),
            credits(&memory, 128 * 1024 * 1024),
            |hash| runs.get(&hash).cloned().ok_or(IndexError::Integrity),
            |hash| blocks.get(&hash).cloned().ok_or(IndexError::Integrity),
            |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
        )
        .unwrap();
        assert_eq!(compacted.splice().root.run_count, 1);
        assert_eq!(compacted.reference().level, 1);
        let kinds = compacted
            .artifacts()
            .blocks
            .iter()
            .map(|block| block.descriptor.kind)
            .collect::<BTreeSet<_>>();
        for kind in [
            QueryBlockKind::Gate,
            QueryBlockKind::Presence,
            QueryBlockKind::DocValue,
            QueryBlockKind::Point,
            QueryBlockKind::TermDictionary,
            QueryBlockKind::Posting,
            QueryBlockKind::Position,
        ] {
            assert!(kinds.contains(&kind), "missing {kind:?}");
        }
    }

    #[test]
    fn fixed_identity_mismatch_refuses_before_loading_any_block() {
        let (root, plan, runs, _blocks, pages) = fixture();
        let memory = pool(8 * 1024 * 1024);
        let block_loads = std::cell::Cell::new(0usize);
        assert!(
            compact_encoded_query_runs(
                root,
                &plan,
                partition(),
                [5; 32],
                QueryBlockLimits::default_for_memory(),
                credits(&memory, 8 * 1024 * 1024),
                |hash| runs.get(&hash).cloned().ok_or(IndexError::Integrity),
                |_| {
                    block_loads.set(block_loads.get() + 1);
                    Err(IndexError::Integrity)
                },
                |hash| pages.get(&hash).cloned().ok_or(IndexError::Integrity),
            )
            .is_err()
        );
        assert_eq!(block_loads.get(), 0);
        assert_eq!(memory.used_bytes(), 0);
    }
}
