//! Memory-charged preparation of typed query deltas into immutable mini-runs.
//!
//! Inputs are consumed under the same pipeline permit that owns every encoded
//! block and the run descriptor. No source JSON or opaque component state is
//! consulted here.

use std::collections::BTreeMap;
use std::mem::size_of;

use crate::IndexError;
use crate::typed_json::ScalarValue;

use super::{
    EncodedProjectionQueryRun, EncodedQueryBlock, PreparedQueryFieldDelta,
    ProjectionPartitionIdentity, ProjectionQueryRunDescriptor, QueryBlockCredits, QueryBlockKind,
    QueryBlockLimits, QueryBlockRecord, QueryDocumentGate, QueryPositions, QueryPosting,
    QueryPostingShard, QueryTermEntry, RecipeIdentity, StableDocumentKey, encode_doc_value,
    encode_document_gate, encode_point, encode_positions, encode_posting,
    encode_projection_query_run, encode_query_block, encode_term_entry,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedQueryRecipeDelta {
    pub recipe: RecipeIdentity,
    pub delta: PreparedQueryFieldDelta,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedQueryMembershipDelta {
    pub recipe: RecipeIdentity,
    pub gates: Vec<QueryDocumentGate>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PreparedQueryMutationBatch {
    pub membership: Option<PreparedQueryMembershipDelta>,
    pub fields: Vec<PreparedQueryRecipeDelta>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionQueryRunArtifacts {
    pub blocks: Vec<EncodedQueryBlock>,
    pub run: EncodedProjectionQueryRun,
}

/// Holds the real pipeline permit for as long as prepared immutable bytes are
/// resident. Dropping this object releases the complete conservative charge.
#[derive(Debug)]
pub struct ChargedProjectionQueryRunArtifacts {
    artifacts: ProjectionQueryRunArtifacts,
    _credits: QueryBlockCredits,
}

impl ChargedProjectionQueryRunArtifacts {
    pub const fn artifacts(&self) -> &ProjectionQueryRunArtifacts {
        &self.artifacts
    }

    pub(crate) fn into_parts(self) -> (ProjectionQueryRunArtifacts, QueryBlockCredits) {
        (self.artifacts, self._credits)
    }
}

/// Convert already typed, sparse old/new field deltas into one query-ready
/// mini-run. Empty batches deliberately produce an empty-block run descriptor:
/// it is the durable no-op marker that advances an exact source/atomic cut.
#[allow(clippy::too_many_arguments)]
pub fn prepare_projection_query_run(
    partition: ProjectionPartitionIdentity,
    physical_catalog_generation: [u8; 32],
    sequence: u64,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
    batch: PreparedQueryMutationBatch,
    limits: QueryBlockLimits,
    mut credits: QueryBlockCredits,
) -> Result<ChargedProjectionQueryRunArtifacts, IndexError> {
    let limits = limits.validate()?;
    validate_run_identity(
        partition,
        physical_catalog_generation,
        sequence,
        source_start_offset,
        next_offset,
    )?;
    let mut grouped = GroupedRecords::default();
    if let Some(membership) = batch.membership {
        for gate in membership.gates {
            let path_bytes = gate
                .source_path
                .as_ref()
                .ok_or_else(|| {
                    IndexError::InvalidDefinition("membership gate has no source path".into())
                })?
                .len();
            let result_bytes = gate
                .result_path
                .as_ref()
                .ok_or_else(|| {
                    IndexError::InvalidDefinition("membership gate has no result path".into())
                })?
                .len();
            charge_fixed_record(
                &mut credits,
                32,
                33usize
                    .checked_add(path_bytes)
                    .and_then(|bytes| bytes.checked_add(result_bytes))
                    .ok_or(IndexError::OffsetOverflow)?,
            )?;
            insert_unique(
                grouped
                    .ordinary
                    .entry((QueryBlockKind::Gate, membership.recipe))
                    .or_default(),
                encode_document_gate(gate)?,
            )?;
        }
    }
    for field in batch.fields {
        let recipe = field.recipe;
        if field.delta.presence.source_path.is_some()
            || field.delta.presence.result_path.is_some()
            || field.delta.presence.result_version != 0
        {
            return Err(IndexError::InvalidDefinition(
                "field-presence gate contains a source path".into(),
            ));
        }
        charge_fixed_record(&mut credits, 32, 33)?;
        insert_unique(
            grouped
                .ordinary
                .entry((QueryBlockKind::Presence, recipe))
                .or_default(),
            encode_document_gate(field.delta.presence)?,
        )?;
        if let Some(value) = field.delta.doc_value {
            let value_bytes =
                value
                    .value
                    .as_ref()
                    .into_iter()
                    .flatten()
                    .try_fold(13usize, |bytes, value| {
                        bytes
                            .checked_add(scalar_bound(Some(value)))
                            .and_then(|bytes| bytes.checked_add(4))
                            .ok_or(IndexError::OffsetOverflow)
                    })?;
            charge_fixed_record(&mut credits, 32, value_bytes)?;
            insert_unique(
                grouped
                    .ordinary
                    .entry((QueryBlockKind::DocValue, recipe))
                    .or_default(),
                encode_doc_value(&value, limits)?,
            )?;
        }
        for point in field.delta.points {
            charge_fixed_record(&mut credits, scalar_bound(Some(&point.value)) + 32, 9)?;
            insert_unique(
                grouped
                    .ordinary
                    .entry((QueryBlockKind::Point, recipe))
                    .or_default(),
                encode_point(&point)?,
            )?;
        }
        for term in field.delta.terms {
            let bytes = scalar_bound(Some(&term.term))
                .checked_add(term.positions.len().saturating_mul(4))
                .and_then(|bytes| bytes.checked_add(192))
                .ok_or(IndexError::OffsetOverflow)?;
            credits.reserve(bytes)?;
            let document = term.document;
            let entry = grouped
                .terms
                .entry((recipe, term.term.clone()))
                .or_default();
            merge_term_delta(entry, document, term)?;
        }
    }

    let mut blocks = Vec::new();
    let mut dictionaries = BTreeMap::<RecipeIdentity, Vec<QueryBlockRecord>>::new();
    for ((recipe, term), documents) in grouped.terms {
        let posting_shards = encode_term_shards(
            recipe,
            documents.into_values(),
            limits,
            &mut credits,
            &mut blocks,
        )?;
        charge_fixed_record(
            &mut credits,
            scalar_bound(Some(&term)),
            4usize
                .checked_add(posting_shards.len().saturating_mul(100))
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        dictionaries
            .entry(recipe)
            .or_default()
            .push(encode_term_entry(&QueryTermEntry {
                term,
                posting_shards,
            })?);
    }
    for (recipe, records) in dictionaries {
        encode_split_blocks(
            QueryBlockKind::TermDictionary,
            recipe,
            records,
            limits,
            &mut credits,
            &mut blocks,
        )?;
    }
    for ((kind, recipe), records) in grouped.ordinary {
        encode_split_blocks(
            kind,
            recipe,
            records.into_values(),
            limits,
            &mut credits,
            &mut blocks,
        )?;
    }
    blocks.sort_unstable_by(|left, right| descriptor_order(left).cmp(&descriptor_order(right)));
    let descriptor_bytes = blocks.iter().try_fold(0usize, |bytes, block| {
        bytes
            .checked_add(size_of::<super::QueryBlockDescriptor>())
            .and_then(|bytes| bytes.checked_add(block.descriptor.minimum_key.len()))
            .and_then(|bytes| bytes.checked_add(block.descriptor.maximum_key.len()))
            .ok_or(IndexError::OffsetOverflow)
    })?;
    credits.reserve(descriptor_bytes)?;
    let descriptor = ProjectionQueryRunDescriptor {
        partition,
        physical_catalog_generation,
        sequence,
        source_start_offset,
        next_offset,
        through_atomic_position,
        blocks: blocks
            .iter()
            .map(|block| block.descriptor.clone())
            .collect(),
    };
    let run = encode_projection_query_run(&descriptor, limits, &mut credits)?;
    Ok(ChargedProjectionQueryRunArtifacts {
        artifacts: ProjectionQueryRunArtifacts { blocks, run },
        _credits: credits,
    })
}

fn validate_run_identity(
    partition: ProjectionPartitionIdentity,
    catalog: [u8; 32],
    sequence: u64,
    source_start_offset: u64,
    next_offset: u64,
) -> Result<(), IndexError> {
    partition.validate()?;
    if catalog == [0; 32] || sequence == 0 || source_start_offset >= next_offset {
        return Err(IndexError::InvalidDefinition(
            "v6 query run identity is invalid".into(),
        ));
    }
    Ok(())
}

fn merge_term_delta(
    documents: &mut BTreeMap<StableDocumentKey, super::PreparedQueryTermDelta>,
    document: StableDocumentKey,
    delta: super::PreparedQueryTermDelta,
) -> Result<(), IndexError> {
    let Some(previous) = documents.get(&document) else {
        documents.insert(document, delta);
        return Ok(());
    };
    if previous.material_source_version != delta.material_source_version
        || previous.live && delta.live && previous.positions != delta.positions
    {
        return Err(IndexError::InvalidDefinition(
            "v6 query preparation contains conflicting term/document deltas".into(),
        ));
    }
    if delta.live || !previous.live {
        documents.insert(document, delta);
    }
    Ok(())
}

fn encode_term_shards(
    recipe: RecipeIdentity,
    documents: impl Iterator<Item = super::PreparedQueryTermDelta>,
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    blocks: &mut Vec<EncodedQueryBlock>,
) -> Result<Vec<QueryPostingShard>, IndexError> {
    let mut shards = Vec::new();
    let mut pending = Vec::new();
    let mut posting_bytes = 87usize;
    let mut position_bytes = 87usize;
    for delta in documents {
        let posting_value = if delta.live && !delta.positions.is_empty() {
            46
        } else {
            10
        };
        let next_posting = block_record_bytes(posting_bytes, pending.len(), 32, posting_value)?;
        let next_position = if delta.live && !delta.positions.is_empty() {
            block_record_bytes(
                position_bytes,
                pending
                    .iter()
                    .filter(|item: &&super::PreparedQueryTermDelta| {
                        item.live && !item.positions.is_empty()
                    })
                    .count(),
                32,
                4usize
                    .checked_add(delta.positions.len().saturating_mul(4))
                    .ok_or(IndexError::OffsetOverflow)?,
            )?
        } else {
            position_bytes
        };
        if !pending.is_empty()
            && (pending.len() == limits.maximum_records
                || next_posting > limits.maximum_block_bytes
                || next_position > limits.maximum_block_bytes)
        {
            shards.push(encode_term_shard(
                recipe, &pending, limits, credits, blocks,
            )?);
            pending.clear();
            posting_bytes = 87;
            position_bytes = 87;
        }
        let position_count = pending
            .iter()
            .filter(|item: &&super::PreparedQueryTermDelta| item.live && !item.positions.is_empty())
            .count();
        posting_bytes = block_record_bytes(posting_bytes, pending.len(), 32, posting_value)?;
        if delta.live && !delta.positions.is_empty() {
            position_bytes = block_record_bytes(
                position_bytes,
                position_count,
                32,
                4usize
                    .checked_add(delta.positions.len().saturating_mul(4))
                    .ok_or(IndexError::OffsetOverflow)?,
            )?;
        }
        if posting_bytes > limits.maximum_block_bytes || position_bytes > limits.maximum_block_bytes
        {
            return Err(IndexError::ResourceLimit {
                needed: posting_bytes.max(position_bytes),
                limit: limits.maximum_block_bytes,
            });
        }
        pending.push(delta);
    }
    if !pending.is_empty() {
        shards.push(encode_term_shard(
            recipe, &pending, limits, credits, blocks,
        )?);
    }
    Ok(shards)
}

fn block_record_bytes(
    current: usize,
    record_index: usize,
    key: usize,
    value: usize,
) -> Result<usize, IndexError> {
    current
        .checked_add(8)
        .and_then(|bytes| bytes.checked_add(key))
        .and_then(|bytes| bytes.checked_add(value))
        .and_then(|bytes| bytes.checked_add(usize::from(record_index % 64 == 0) * 4))
        .ok_or(IndexError::OffsetOverflow)
}

fn encode_term_shard(
    recipe: RecipeIdentity,
    deltas: &[super::PreparedQueryTermDelta],
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    blocks: &mut Vec<EncodedQueryBlock>,
) -> Result<QueryPostingShard, IndexError> {
    let mut positions = Vec::new();
    for delta in deltas
        .iter()
        .filter(|delta| delta.live && !delta.positions.is_empty())
    {
        charge_fixed_record(credits, 32, 4 + delta.positions.len().saturating_mul(4))?;
        positions.push(encode_positions(&QueryPositions {
            document: delta.document,
            positions: delta.positions.clone(),
        })?);
    }
    let position_hash = if positions.is_empty() {
        None
    } else {
        let block = encode_one_block(QueryBlockKind::Position, recipe, positions, limits, credits)?;
        let hash = block.descriptor.hash;
        push_block(blocks, block, credits)?;
        Some(hash)
    };
    let mut postings = Vec::with_capacity(deltas.len());
    credits.reserve(
        deltas
            .len()
            .checked_mul(size_of::<QueryBlockRecord>())
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    for delta in deltas {
        charge_fixed_record(credits, 32, 46)?;
        let position_block_hash = if delta.positions.is_empty() {
            None
        } else {
            Some(position_hash.ok_or(IndexError::Integrity)?)
        };
        postings.push(encode_posting(QueryPosting {
            document: delta.document,
            material_source_version: delta.material_source_version,
            live: delta.live,
            position_block_hash,
            positions: u32::try_from(delta.positions.len())
                .map_err(|_| IndexError::OffsetOverflow)?,
        })?);
    }
    let posting = encode_one_block(QueryBlockKind::Posting, recipe, postings, limits, credits)?;
    let shard = QueryPostingShard {
        posting_block_hash: posting.descriptor.hash,
        posting_records: posting.descriptor.records,
        minimum_document: StableDocumentKey::from_bytes(
            posting
                .descriptor
                .minimum_key
                .as_slice()
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        )?,
        maximum_document: StableDocumentKey::from_bytes(
            posting
                .descriptor
                .maximum_key
                .as_slice()
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        )?,
    };
    push_block(blocks, posting, credits)?;
    Ok(shard)
}

#[derive(Default)]
struct GroupedRecords {
    ordinary: BTreeMap<(QueryBlockKind, RecipeIdentity), BTreeMap<Vec<u8>, QueryBlockRecord>>,
    terms: BTreeMap<
        (RecipeIdentity, ScalarValue),
        BTreeMap<StableDocumentKey, super::PreparedQueryTermDelta>,
    >,
}

fn insert_unique(
    records: &mut BTreeMap<Vec<u8>, QueryBlockRecord>,
    record: QueryBlockRecord,
) -> Result<(), IndexError> {
    if records.insert(record.key.clone(), record).is_some() {
        return Err(IndexError::InvalidDefinition(
            "v6 query preparation contains duplicate record keys".into(),
        ));
    }
    Ok(())
}

fn encode_split_blocks(
    kind: QueryBlockKind,
    recipe: RecipeIdentity,
    records: impl IntoIterator<Item = QueryBlockRecord>,
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    output: &mut Vec<EncodedQueryBlock>,
) -> Result<(), IndexError> {
    let mut records = records.into_iter().collect::<Vec<_>>();
    records.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    reject_duplicate_records(&records)?;
    let mut pending = Vec::new();
    let mut encoded_bytes = 87usize;
    for record in records {
        let extra = 8usize
            .checked_add(record.key.len())
            .and_then(|bytes| bytes.checked_add(record.value.len()))
            .and_then(|bytes| bytes.checked_add(usize::from(pending.len() % 64 == 0) * 4))
            .ok_or(IndexError::OffsetOverflow)?;
        if !pending.is_empty()
            && (pending.len() == limits.maximum_records
                || encoded_bytes.saturating_add(extra) > limits.maximum_block_bytes)
        {
            let block = encode_query_block(kind, recipe, &pending, limits, credits)?;
            push_block(output, block, credits)?;
            pending.clear();
            encoded_bytes = 87;
        }
        if encoded_bytes.saturating_add(extra) > limits.maximum_block_bytes {
            return Err(IndexError::ResourceLimit {
                needed: encoded_bytes.saturating_add(extra),
                limit: limits.maximum_block_bytes,
            });
        }
        encoded_bytes = encoded_bytes
            .checked_add(extra)
            .ok_or(IndexError::OffsetOverflow)?;
        pending.push(record);
    }
    if !pending.is_empty() {
        let block = encode_query_block(kind, recipe, &pending, limits, credits)?;
        push_block(output, block, credits)?;
    }
    Ok(())
}

fn encode_one_block(
    kind: QueryBlockKind,
    recipe: RecipeIdentity,
    mut records: Vec<QueryBlockRecord>,
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
) -> Result<EncodedQueryBlock, IndexError> {
    records.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    reject_duplicate_records(&records)?;
    encode_query_block(kind, recipe, &records, limits, credits)
}

fn reject_duplicate_records(records: &[QueryBlockRecord]) -> Result<(), IndexError> {
    if records.windows(2).any(|pair| pair[0].key >= pair[1].key) {
        return Err(IndexError::InvalidDefinition(
            "v6 query preparation records are not unique canonical order".into(),
        ));
    }
    Ok(())
}

fn push_block(
    output: &mut Vec<EncodedQueryBlock>,
    block: EncodedQueryBlock,
    credits: &mut QueryBlockCredits,
) -> Result<(), IndexError> {
    credits.reserve(size_of::<EncodedQueryBlock>())?;
    output.push(block);
    Ok(())
}

fn descriptor_order(
    block: &EncodedQueryBlock,
) -> (QueryBlockKind, RecipeIdentity, &[u8], &[u8], [u8; 32]) {
    (
        block.descriptor.kind,
        block.descriptor.recipe,
        &block.descriptor.minimum_key,
        &block.descriptor.maximum_key,
        block.descriptor.hash,
    )
}

fn charge_fixed_record(
    credits: &mut QueryBlockCredits,
    key_bytes: usize,
    value_bytes: usize,
) -> Result<(), IndexError> {
    credits.reserve(
        size_of::<QueryBlockRecord>()
            .checked_add(key_bytes.saturating_mul(2))
            .and_then(|bytes| bytes.checked_add(value_bytes))
            .ok_or(IndexError::OffsetOverflow)?,
    )
}

fn scalar_bound(value: Option<&ScalarValue>) -> usize {
    match value {
        None => 1,
        Some(ScalarValue::String(value)) => value.len().saturating_add(8),
        Some(_) => 16,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typed_json::{
        Analyzer, Cardinality, Collation, FieldCapabilities, FieldId, FieldSchema, FieldType,
        TypedJsonFieldState,
    };
    use crate::v6::{
        IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage,
        decode_projection_query_run, prepare_typed_json_field_delta,
    };

    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 2, [3; 32], 2, 4, 5).unwrap()
    }

    fn memory(bytes: usize) -> IndexingMemoryCredits {
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

    fn credits(memory: &IndexingMemoryCredits, bytes: usize) -> QueryBlockCredits {
        QueryBlockCredits::from_pipeline_permit(
            memory
                .acquire(IndexingMemoryStage::OrderingCatalog, bytes)
                .unwrap(),
        )
    }

    fn keyword() -> FieldSchema {
        FieldSchema {
            id: FieldId::new(1),
            name: "status".into(),
            source_selector: "/status".into(),
            field_type: FieldType::Keyword,
            cardinality: Cardinality::Single,
            allow_missing: true,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::EXACT
                .union(FieldCapabilities::RANGE)
                .union(FieldCapabilities::ORDER)
                .union(FieldCapabilities::FACET),
            analyzer: None,
            date_format: None,
        }
    }

    #[test]
    fn typed_delta_becomes_query_ready_blocks_and_one_exact_run() {
        let pool = memory(8 * 1024 * 1024);
        let mut reservation = credits(&pool, 8 * 1024 * 1024);
        let field = keyword();
        let previous = TypedJsonFieldState::from_selected(
            &field,
            Some(vec![ScalarValue::String("queued".into())]),
        )
        .unwrap();
        let current = TypedJsonFieldState::from_selected(
            &field,
            Some(vec![ScalarValue::String("running".into())]),
        )
        .unwrap();
        let document = StableDocumentKey::from_bytes([9; 32]).unwrap();
        let delta = prepare_typed_json_field_delta(
            &field,
            document,
            7,
            Some(&previous),
            Some(&current),
            &mut reservation,
        )
        .unwrap();
        let recipe = RecipeIdentity::new([7; 32]).unwrap();
        let prepared = prepare_projection_query_run(
            partition(),
            [8; 32],
            3,
            20,
            24,
            11,
            PreparedQueryMutationBatch {
                membership: Some(PreparedQueryMembershipDelta {
                    recipe: RecipeIdentity::new([6; 32]).unwrap(),
                    gates: vec![QueryDocumentGate {
                        document,
                        material_source_version: 7,
                        current_source_version: 7,
                        live: true,
                        source_path: Some("objects/document.json".into()),
                        result_path: Some("objects/document.json".into()),
                        result_version: 7,
                    }],
                }),
                fields: vec![PreparedQueryRecipeDelta { recipe, delta }],
            },
            QueryBlockLimits::default_for_memory(),
            reservation,
        )
        .unwrap();
        let artifacts = prepared.artifacts();
        let kinds = artifacts
            .blocks
            .iter()
            .map(|block| block.descriptor.kind)
            .collect::<std::collections::BTreeSet<_>>();
        assert!(kinds.contains(&QueryBlockKind::Gate));
        assert!(kinds.contains(&QueryBlockKind::Presence));
        assert!(kinds.contains(&QueryBlockKind::TermDictionary));
        assert!(kinds.contains(&QueryBlockKind::Posting));
        assert!(kinds.contains(&QueryBlockKind::Point));
        assert!(kinds.contains(&QueryBlockKind::DocValue));
        let verification_memory = memory(8 * 1024 * 1024);
        let mut verification = credits(&verification_memory, 8 * 1024 * 1024);
        let descriptor = decode_projection_query_run(
            &artifacts.run.bytes,
            QueryBlockLimits::default_for_memory(),
            &mut verification,
        )
        .unwrap();
        assert_eq!(descriptor.sequence, 3);
        assert_eq!(descriptor.source_start_offset, 20);
        assert_eq!(descriptor.next_offset, 24);
        assert_eq!(descriptor.through_atomic_position, 11);
        assert_eq!(descriptor.blocks.len(), artifacts.blocks.len());
    }

    #[test]
    fn refusal_releases_the_real_pipeline_permit() {
        let memory = memory(128);
        let reservation = credits(&memory, 128);
        assert!(matches!(
            prepare_projection_query_run(
                partition(),
                [8; 32],
                1,
                1,
                2,
                0,
                PreparedQueryMutationBatch::default(),
                QueryBlockLimits::default_for_memory(),
                reservation,
            ),
            Err(IndexError::ResourceLimit { .. })
        ));
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn popular_term_is_sharded_beyond_one_block_record_limit() {
        let count = 32_769usize;
        let pool = memory(128 * 1024 * 1024);
        let reservation = credits(&pool, 128 * 1024 * 1024);
        let recipe = RecipeIdentity::new([7; 32]).unwrap();
        let fields = (1..=count)
            .map(|number| {
                let mut bytes = [0; 32];
                bytes[24..].copy_from_slice(&(number as u64).to_be_bytes());
                let document = StableDocumentKey::from_bytes(bytes).unwrap();
                PreparedQueryRecipeDelta {
                    recipe,
                    delta: PreparedQueryFieldDelta {
                        presence: QueryDocumentGate {
                            document,
                            material_source_version: 1,
                            current_source_version: 1,
                            live: true,
                            source_path: None,
                            result_path: None,
                            result_version: 0,
                        },
                        doc_value: None,
                        terms: vec![super::super::PreparedQueryTermDelta {
                            term: ScalarValue::String("popular".into()),
                            document,
                            material_source_version: 1,
                            live: true,
                            positions: vec![0],
                        }],
                        points: Vec::new(),
                    },
                }
            })
            .collect();
        let prepared = prepare_projection_query_run(
            partition(),
            [8; 32],
            1,
            0,
            1,
            1,
            PreparedQueryMutationBatch {
                membership: None,
                fields,
            },
            QueryBlockLimits::default_for_memory(),
            reservation,
        )
        .unwrap();
        let dictionary = prepared
            .artifacts()
            .blocks
            .iter()
            .find(|block| block.descriptor.kind == QueryBlockKind::TermDictionary)
            .unwrap();
        let verify_pool = memory(2 * 1024 * 1024);
        let mut verify = credits(&verify_pool, 2 * 1024 * 1024);
        let mut cursor = super::super::QueryBlockCursor::new(
            &dictionary.descriptor,
            &dictionary.bytes,
            QueryBlockLimits::default_for_memory(),
            &mut verify,
        )
        .unwrap();
        let entry = super::super::decode_term_entry(
            cursor.next().unwrap().unwrap(),
            QueryBlockLimits::default_for_memory(),
        )
        .unwrap();
        assert!(entry.posting_shards.len() > 1);
        assert_eq!(
            entry
                .posting_shards
                .iter()
                .map(|shard| shard.posting_records as usize)
                .sum::<usize>(),
            count
        );
        let posting_blocks = prepared
            .artifacts()
            .blocks
            .iter()
            .filter(|block| block.descriptor.kind == QueryBlockKind::Posting)
            .count();
        let position_blocks = prepared
            .artifacts()
            .blocks
            .iter()
            .filter(|block| block.descriptor.kind == QueryBlockKind::Position)
            .count();
        assert!(posting_blocks > 1);
        assert_eq!(position_blocks, posting_blocks);
    }

    #[test]
    fn position_reorder_canonicalizes_to_one_live_term_document_delta() {
        let field = FieldSchema {
            id: FieldId::new(2),
            name: "body".into(),
            source_selector: "/body".into(),
            field_type: FieldType::Text,
            cardinality: Cardinality::Single,
            allow_missing: true,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::FULL_TEXT,
            analyzer: Some(Analyzer::UnicodeAlphanumericLowercase),
            date_format: None,
        };
        let previous = TypedJsonFieldState::from_selected(
            &field,
            Some(vec![ScalarValue::String("one two".into())]),
        )
        .unwrap();
        let current = TypedJsonFieldState::from_selected(
            &field,
            Some(vec![ScalarValue::String("two one".into())]),
        )
        .unwrap();
        let pool = memory(8 * 1024 * 1024);
        let mut reservation = credits(&pool, 8 * 1024 * 1024);
        let document = StableDocumentKey::from_bytes([4; 32]).unwrap();
        let delta = prepare_typed_json_field_delta(
            &field,
            document,
            2,
            Some(&previous),
            Some(&current),
            &mut reservation,
        )
        .unwrap();
        assert_eq!(delta.terms.len(), 4, "old tombstones plus live positions");
        let prepared = prepare_projection_query_run(
            partition(),
            [8; 32],
            1,
            0,
            1,
            1,
            PreparedQueryMutationBatch {
                membership: None,
                fields: vec![PreparedQueryRecipeDelta {
                    recipe: RecipeIdentity::new([7; 32]).unwrap(),
                    delta,
                }],
            },
            QueryBlockLimits::default_for_memory(),
            reservation,
        )
        .unwrap();
        let postings = prepared
            .artifacts()
            .blocks
            .iter()
            .filter(|block| block.descriptor.kind == QueryBlockKind::Posting)
            .map(|block| block.descriptor.records as usize)
            .sum::<usize>();
        assert_eq!(postings, 2);
    }
}
