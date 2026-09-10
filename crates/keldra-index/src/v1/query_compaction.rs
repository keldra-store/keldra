//! Production compaction for encoded v1 query mini-runs.
//!
//! The selected whole-run window is verified before merge state is allocated.
//! Newest records win by their canonical semantic key; tombstones remain so
//! data in older, unselected levels cannot be resurrected.

use std::collections::BTreeSet;
use std::mem::size_of;

use crate::IndexError;
use crate::typed_json::{ScalarValue, decode_scalar_sort_key};

use super::query_prepare::{encode_term_shard, finish_projection_query_run, push_block};
use super::{
    EncodedQueryBlock, PreparedQueryRunSplice, PreparedQueryTermDelta, ProjectionPartitionIdentity,
    ProjectionQueryRunArtifacts, ProjectionQueryRunDescriptor, ProjectionQueryStreamRoot,
    QueryBlockCredits, QueryBlockCursor, QueryBlockDescriptor, QueryBlockKind, QueryBlockLimits,
    QueryBlockRecord, QueryPostingShard, QueryRunCompactionPlan, QueryRunReference, QueryTermEntry,
    RecipeIdentity, decode_doc_value, decode_document_gate, decode_point, decode_positions,
    decode_posting, decode_projection_query_run, decode_term_entry, encode_query_block,
    encode_term_entry, splice_compacted_query_runs,
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
        return invalid("v1 query compaction catalog is zero");
    }
    let inputs = plan.inputs_newest_first();
    if inputs.is_empty() {
        return invalid("v1 query compaction has no inputs");
    }
    if inputs.len() > limits.maximum_loaded_blocks {
        return Err(IndexError::ResourceLimit {
            needed: inputs.len(),
            limit: limits.maximum_loaded_blocks,
        });
    }

    let mut runs = Vec::with_capacity(inputs.len());
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
        let descriptor = decode_projection_query_run(&bytes, limits, &mut credits)?;
        validate_descriptor_reference(
            &descriptor,
            *reference,
            partition,
            physical_catalog_generation,
        )?;
        runs.push(descriptor);
    }

    let mut blocks = Vec::new();
    merge_ordinary_runs(&runs, limits, &mut credits, &mut load_block, &mut blocks)?;
    merge_term_runs(&runs, limits, &mut credits, &mut load_block, &mut blocks)?;
    let newest = *inputs.first().ok_or(IndexError::Integrity)?;
    let charged = finish_projection_query_run(
        partition,
        physical_catalog_generation,
        newest.sequence,
        plan.source_start_offset(),
        plan.next_offset(),
        plan.through_atomic_position(),
        blocks,
        limits,
        credits,
    )?;
    let (artifacts, credits) = charged.into_parts();
    let reference = QueryRunReference {
        hash: artifacts.run.hash,
        encoded_bytes: u64::try_from(artifacts.run.bytes.len())
            .map_err(|_| IndexError::OffsetOverflow)?,
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
        || bytes.len() < 174
        || *crate::profiled_blake3_hash!(bytes).as_bytes() != hash
    {
        return Err(IndexError::Integrity);
    }
    let payload = bytes;
    if &payload[..8] != b"K1QRUN01"
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
        return Err(IndexError::InvalidFormat("v1 query run block count"));
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

struct OwnedRecord {
    record: QueryBlockRecord,
    charge: usize,
}

struct RecordLane {
    run: usize,
    next_block: usize,
    kind: QueryBlockKind,
    recipe: RecipeIdentity,
    records: Vec<Option<OwnedRecord>>,
    next_record: usize,
}

impl RecordLane {
    fn head(&self) -> Option<&OwnedRecord> {
        self.records.get(self.next_record).and_then(Option::as_ref)
    }

    fn take_head(&mut self) -> Option<OwnedRecord> {
        let record = self.records.get_mut(self.next_record)?.take()?;
        self.next_record += 1;
        Some(record)
    }

    fn refill(
        &mut self,
        runs: &[ProjectionQueryRunDescriptor],
        limits: QueryBlockLimits,
        credits: &mut QueryBlockCredits,
        load: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    ) -> Result<(), IndexError> {
        if self.head().is_some() {
            return Ok(());
        }
        self.records.clear();
        self.next_record = 0;
        let run = runs.get(self.run).ok_or(IndexError::Integrity)?;
        let Some((block, descriptor)) = run
            .blocks
            .iter()
            .enumerate()
            .skip(self.next_block)
            .find(|(_, block)| block.kind == self.kind && block.recipe == self.recipe)
        else {
            return Ok(());
        };
        self.next_block = block + 1;
        let bytes = load(descriptor.hash)?;
        let mut cursor = QueryBlockCursor::new(descriptor, &bytes, limits, credits)?;
        while let Some(record) = cursor.next()? {
            validate_ordinary_record(self.kind, record, limits)?;
            let charge = size_of::<QueryBlockRecord>()
                .saturating_add(record.key.len())
                .saturating_add(record.value.len());
            credits.reserve(charge)?;
            self.records.push(Some(OwnedRecord {
                record: QueryBlockRecord {
                    key: record.key.to_vec(),
                    value: record.value.to_vec(),
                },
                charge,
            }));
        }
        drop(cursor);
        credits.release_loaded_block(bytes.len())?;
        Ok(())
    }
}

struct StreamingBlockWriter {
    kind: QueryBlockKind,
    recipe: RecipeIdentity,
    records: Vec<QueryBlockRecord>,
    charge: usize,
    encoded_bytes: usize,
}

impl StreamingBlockWriter {
    fn new(kind: QueryBlockKind, recipe: RecipeIdentity) -> Self {
        Self {
            kind,
            recipe,
            records: Vec::new(),
            charge: 0,
            encoded_bytes: 87,
        }
    }

    fn push(
        &mut self,
        owned: OwnedRecord,
        limits: QueryBlockLimits,
        credits: &mut QueryBlockCredits,
        output: &mut Vec<EncodedQueryBlock>,
    ) -> Result<(), IndexError> {
        let extra = 8usize
            .saturating_add(owned.record.key.len())
            .saturating_add(owned.record.value.len())
            .saturating_add(usize::from(self.records.len() % 64 == 0) * 4);
        if !self.records.is_empty()
            && (self.records.len() == limits.maximum_records
                || self.encoded_bytes.saturating_add(extra) > limits.maximum_block_bytes)
        {
            self.flush(limits, credits, output)?;
        }
        if self.encoded_bytes.saturating_add(extra) > limits.maximum_block_bytes {
            return Err(IndexError::ResourceLimit {
                needed: self.encoded_bytes.saturating_add(extra),
                limit: limits.maximum_block_bytes,
            });
        }
        self.encoded_bytes = self.encoded_bytes.saturating_add(extra);
        self.charge = self.charge.saturating_add(owned.charge);
        self.records.push(owned.record);
        Ok(())
    }

    fn flush(
        &mut self,
        limits: QueryBlockLimits,
        credits: &mut QueryBlockCredits,
        output: &mut Vec<EncodedQueryBlock>,
    ) -> Result<(), IndexError> {
        if self.records.is_empty() {
            return Ok(());
        }
        let block = encode_query_block(self.kind, self.recipe, &self.records, limits, credits)?;
        push_block(output, block, credits)?;
        self.records.clear();
        credits.release(std::mem::take(&mut self.charge))?;
        self.encoded_bytes = 87;
        Ok(())
    }
}

fn validate_ordinary_record(
    kind: QueryBlockKind,
    record: super::QueryBlockRecordRef<'_>,
    limits: QueryBlockLimits,
) -> Result<(), IndexError> {
    match kind {
        QueryBlockKind::Gate => {
            let gate = decode_document_gate(record)?;
            if gate.source_path.is_none() || gate.result_path.is_none() {
                return Err(IndexError::Integrity);
            }
        }
        QueryBlockKind::Presence => {
            let gate = decode_document_gate(record)?;
            if gate.source_path.is_some() || gate.result_path.is_some() || gate.result_version != 0
            {
                return Err(IndexError::Integrity);
            }
        }
        QueryBlockKind::DocValue => {
            decode_doc_value(record, limits)?;
        }
        QueryBlockKind::Point => {
            decode_point(record)?;
        }
        // Dictionary values are decoded exactly once when their term becomes
        // the active merge key; retaining the verified record bytes here
        // avoids allocating every posting-shard vector twice.
        QueryBlockKind::TermDictionary => {}
        _ => return Err(IndexError::Integrity),
    }
    Ok(())
}

fn merge_ordinary_runs(
    runs: &[ProjectionQueryRunDescriptor],
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    load: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    output: &mut Vec<EncodedQueryBlock>,
) -> Result<(), IndexError> {
    let groups = runs
        .iter()
        .flat_map(|run| run.blocks.iter())
        .filter(|block| {
            matches!(
                block.kind,
                QueryBlockKind::Gate
                    | QueryBlockKind::Presence
                    | QueryBlockKind::DocValue
                    | QueryBlockKind::Point
            )
        })
        .map(|block| (block.kind, block.recipe))
        .collect::<BTreeSet<_>>();
    for (kind, recipe) in groups {
        let mut lanes = runs
            .iter()
            .enumerate()
            .filter_map(|(run_index, run)| {
                run.blocks
                    .iter()
                    .any(|block| block.kind == kind && block.recipe == recipe)
                    .then_some(RecordLane {
                        run: run_index,
                        next_block: 0,
                        kind,
                        recipe,
                        records: Vec::new(),
                        next_record: 0,
                    })
            })
            .collect::<Vec<_>>();
        for lane in &mut lanes {
            lane.refill(runs, limits, credits, load)?;
        }
        let mut writer = StreamingBlockWriter::new(kind, recipe);
        loop {
            let Some(minimum) = lanes
                .iter()
                .filter_map(RecordLane::head)
                .map(|record| record.record.key.as_slice())
                .min()
                .map(ToOwned::to_owned)
            else {
                break;
            };
            credits.reserve(minimum.len())?;
            let winner = lanes
                .iter()
                .enumerate()
                .filter(|(_, lane)| lane.head().is_some_and(|head| head.record.key == minimum))
                .min_by_key(|(_, lane)| lane.run)
                .map(|(index, _)| index)
                .ok_or(IndexError::Integrity)?;
            let winner = lanes[winner].take_head().ok_or(IndexError::Integrity)?;
            for lane in &mut lanes {
                if lane.head().is_some_and(|head| head.record.key == minimum) {
                    let discarded = lane.take_head().ok_or(IndexError::Integrity)?;
                    credits.release(discarded.charge)?;
                }
                lane.refill(runs, limits, credits, load)?;
            }
            writer.push(winner, limits, credits, output)?;
            credits.release(minimum.len())?;
        }
        writer.flush(limits, credits, output)?;
    }
    Ok(())
}

struct TermSource {
    run: usize,
    recipe: RecipeIdentity,
    shards: Vec<QueryPostingShard>,
    charge: usize,
}

struct OwnedTermDelta {
    delta: PreparedQueryTermDelta,
    charge: usize,
}

struct PostingLane {
    source: TermSource,
    next_shard: usize,
    records: Vec<Option<OwnedTermDelta>>,
    next_record: usize,
}

impl PostingLane {
    fn head(&self) -> Option<&OwnedTermDelta> {
        self.records.get(self.next_record).and_then(Option::as_ref)
    }

    fn take_head(&mut self) -> Option<OwnedTermDelta> {
        let record = self.records.get_mut(self.next_record)?.take()?;
        self.next_record += 1;
        Some(record)
    }

    fn refill(
        &mut self,
        runs: &[ProjectionQueryRunDescriptor],
        term: &ScalarValue,
        limits: QueryBlockLimits,
        credits: &mut QueryBlockCredits,
        load: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    ) -> Result<(), IndexError> {
        if self.head().is_some() {
            return Ok(());
        }
        self.records.clear();
        self.next_record = 0;
        let Some(shard) = self.source.shards.get(self.next_shard) else {
            return Ok(());
        };
        self.next_shard += 1;
        let run = runs.get(self.source.run).ok_or(IndexError::Integrity)?;
        let posting_descriptor = find_block_linear(
            run,
            shard.posting_block_hash,
            QueryBlockKind::Posting,
            self.source.recipe,
        )?;
        if posting_descriptor.records != shard.posting_records
            || posting_descriptor.minimum_key != shard.minimum_document.bytes()
            || posting_descriptor.maximum_key != shard.maximum_document.bytes()
        {
            return Err(IndexError::Integrity);
        }
        let mut postings = Vec::new();
        visit_block(
            posting_descriptor,
            limits,
            credits,
            load,
            &mut |record, credits| {
                let posting = decode_posting(record)?;
                let charge = size_of::<PreparedQueryTermDelta>();
                credits.reserve(charge)?;
                postings.push((posting, charge));
                Ok(())
            },
        )?;
        let position_hash = postings
            .iter()
            .filter_map(|(posting, _)| posting.position_block_hash)
            .next();
        if postings
            .iter()
            .filter_map(|(posting, _)| posting.position_block_hash)
            .any(|hash| Some(hash) != position_hash)
        {
            return Err(IndexError::Integrity);
        }
        let mut positions = Vec::new();
        if let Some(hash) = position_hash {
            let descriptor =
                find_block_linear(run, hash, QueryBlockKind::Position, self.source.recipe)?;
            visit_block(descriptor, limits, credits, load, &mut |record, credits| {
                let value = decode_positions(record, limits)?;
                let charge = size_of::<super::QueryPositions>()
                    .saturating_add(value.positions.len().saturating_mul(size_of::<u32>()));
                credits.reserve(charge)?;
                positions.push((value, charge));
                Ok(())
            })?;
        }
        let mut position_index = 0;
        for (posting, posting_charge) in postings {
            let (values, position_charge) = if posting.position_block_hash.is_some() {
                let (position, charge) = positions
                    .get_mut(position_index)
                    .ok_or(IndexError::Integrity)?;
                if position.document != posting.document
                    || position.positions.len() != posting.positions as usize
                {
                    return Err(IndexError::Integrity);
                }
                position_index += 1;
                (std::mem::take(&mut position.positions), *charge)
            } else {
                (Vec::new(), 0)
            };
            let term_charge = scalar_heap_bytes(term);
            credits.reserve(term_charge)?;
            self.records.push(Some(OwnedTermDelta {
                delta: PreparedQueryTermDelta {
                    term: term.clone(),
                    document: posting.document,
                    material_source_version: posting.material_source_version,
                    live: posting.live,
                    positions: values,
                },
                charge: posting_charge
                    .saturating_add(position_charge)
                    .saturating_add(term_charge),
            }));
        }
        if position_index != positions.len() {
            return Err(IndexError::Integrity);
        }
        Ok(())
    }
}

struct TermShardWriter {
    recipe: RecipeIdentity,
    pending: Vec<PreparedQueryTermDelta>,
    charge: usize,
    posting_bytes: usize,
    position_bytes: usize,
    position_records: usize,
    shards: Vec<QueryPostingShard>,
}

impl TermShardWriter {
    fn new(recipe: RecipeIdentity) -> Self {
        Self {
            recipe,
            pending: Vec::new(),
            charge: 0,
            posting_bytes: 87,
            position_bytes: 87,
            position_records: 0,
            shards: Vec::new(),
        }
    }

    fn push(
        &mut self,
        owned: OwnedTermDelta,
        limits: QueryBlockLimits,
        credits: &mut QueryBlockCredits,
        blocks: &mut Vec<EncodedQueryBlock>,
    ) -> Result<(), IndexError> {
        let posting_value = if owned.delta.live && !owned.delta.positions.is_empty() {
            46
        } else {
            10
        };
        let next_posting = record_bytes(self.posting_bytes, self.pending.len(), 32, posting_value)?;
        let next_position = if owned.delta.live && !owned.delta.positions.is_empty() {
            record_bytes(
                self.position_bytes,
                self.position_records,
                32,
                4usize.saturating_add(owned.delta.positions.len().saturating_mul(4)),
            )?
        } else {
            self.position_bytes
        };
        if !self.pending.is_empty()
            && (self.pending.len() == limits.maximum_records
                || next_posting > limits.maximum_block_bytes
                || next_position > limits.maximum_block_bytes)
        {
            self.flush(limits, credits, blocks)?;
        }
        self.posting_bytes =
            record_bytes(self.posting_bytes, self.pending.len(), 32, posting_value)?;
        if owned.delta.live && !owned.delta.positions.is_empty() {
            self.position_bytes = record_bytes(
                self.position_bytes,
                self.position_records,
                32,
                4usize.saturating_add(owned.delta.positions.len().saturating_mul(4)),
            )?;
            self.position_records += 1;
        }
        self.charge = self.charge.saturating_add(owned.charge);
        self.pending.push(owned.delta);
        Ok(())
    }

    fn flush(
        &mut self,
        limits: QueryBlockLimits,
        credits: &mut QueryBlockCredits,
        blocks: &mut Vec<EncodedQueryBlock>,
    ) -> Result<(), IndexError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.shards.push(encode_term_shard(
            self.recipe,
            &self.pending,
            limits,
            credits,
            blocks,
        )?);
        self.pending.clear();
        credits.release(std::mem::take(&mut self.charge))?;
        self.posting_bytes = 87;
        self.position_bytes = 87;
        self.position_records = 0;
        Ok(())
    }
}

fn record_bytes(
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

fn merge_term_runs(
    runs: &[ProjectionQueryRunDescriptor],
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    load: &mut impl FnMut([u8; 32]) -> Result<Vec<u8>, IndexError>,
    output: &mut Vec<EncodedQueryBlock>,
) -> Result<(), IndexError> {
    let recipes = runs
        .iter()
        .flat_map(|run| run.blocks.iter())
        .filter(|block| block.kind == QueryBlockKind::TermDictionary)
        .map(|block| block.recipe)
        .collect::<BTreeSet<_>>();
    for recipe in recipes {
        let mut dictionaries = runs
            .iter()
            .enumerate()
            .filter_map(|(run_index, run)| {
                run.blocks
                    .iter()
                    .any(|block| {
                        block.kind == QueryBlockKind::TermDictionary && block.recipe == recipe
                    })
                    .then_some(RecordLane {
                        run: run_index,
                        next_block: 0,
                        kind: QueryBlockKind::TermDictionary,
                        recipe,
                        records: Vec::new(),
                        next_record: 0,
                    })
            })
            .collect::<Vec<_>>();
        for lane in &mut dictionaries {
            lane.refill(runs, limits, credits, load)?;
        }
        let mut dictionary_writer =
            StreamingBlockWriter::new(QueryBlockKind::TermDictionary, recipe);
        loop {
            let Some(term_key) = dictionaries
                .iter()
                .filter_map(RecordLane::head)
                .map(|record| record.record.key.as_slice())
                .min()
                .map(ToOwned::to_owned)
            else {
                break;
            };
            credits.reserve(term_key.len())?;
            let mut sources = Vec::new();
            for lane in &mut dictionaries {
                if lane.head().is_some_and(|head| head.record.key == term_key) {
                    let owned = lane.take_head().ok_or(IndexError::Integrity)?;
                    let entry = decode_term_entry(
                        super::QueryBlockRecordRef {
                            key: &owned.record.key,
                            value: &owned.record.value,
                        },
                        limits,
                    )?;
                    let source_charge = size_of::<TermSource>().saturating_add(
                        entry
                            .posting_shards
                            .len()
                            .saturating_mul(size_of::<QueryPostingShard>()),
                    );
                    credits.reserve(source_charge)?;
                    credits.release(owned.charge)?;
                    sources.push(TermSource {
                        run: lane.run,
                        recipe,
                        shards: entry.posting_shards,
                        charge: source_charge,
                    });
                }
                lane.refill(runs, limits, credits, load)?;
            }
            let term = decode_scalar_sort_key(&term_key)?.0;
            let mut postings = sources
                .into_iter()
                .map(|source| PostingLane {
                    source,
                    next_shard: 0,
                    records: Vec::new(),
                    next_record: 0,
                })
                .collect::<Vec<_>>();
            for lane in &mut postings {
                lane.refill(runs, &term, limits, credits, load)?;
            }
            let mut shard_writer = TermShardWriter::new(recipe);
            loop {
                let Some(document) = postings
                    .iter()
                    .filter_map(PostingLane::head)
                    .map(|record| record.delta.document)
                    .min()
                else {
                    break;
                };
                let winner = postings
                    .iter()
                    .enumerate()
                    .filter(|(_, lane)| {
                        lane.head()
                            .is_some_and(|head| head.delta.document == document)
                    })
                    .min_by_key(|(_, lane)| lane.source.run)
                    .map(|(index, _)| index)
                    .ok_or(IndexError::Integrity)?;
                let winner = postings[winner].take_head().ok_or(IndexError::Integrity)?;
                for lane in &mut postings {
                    if lane
                        .head()
                        .is_some_and(|head| head.delta.document == document)
                    {
                        let discarded = lane.take_head().ok_or(IndexError::Integrity)?;
                        credits.release(discarded.charge)?;
                    }
                    lane.refill(runs, &term, limits, credits, load)?;
                }
                shard_writer.push(winner, limits, credits, output)?;
            }
            shard_writer.flush(limits, credits, output)?;
            let source_charge = postings
                .iter()
                .map(|lane| lane.source.charge)
                .sum::<usize>();
            drop(postings);
            credits.release(source_charge)?;
            let record = encode_term_entry(&QueryTermEntry {
                term,
                posting_shards: shard_writer.shards,
            })?;
            let charge = size_of::<QueryBlockRecord>()
                .saturating_add(record.key.len())
                .saturating_add(record.value.len());
            credits.reserve(charge)?;
            dictionary_writer.push(OwnedRecord { record, charge }, limits, credits, output)?;
            credits.release(term_key.len())?;
        }
        dictionary_writer.flush(limits, credits, output)?;
    }
    Ok(())
}

fn scalar_heap_bytes(value: &ScalarValue) -> usize {
    match value {
        ScalarValue::String(value) => value.len(),
        _ => 0,
    }
}

fn find_block_linear(
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

fn invalid<T>(message: &str) -> Result<T, IndexError> {
    Err(IndexError::InvalidDefinition(message.into()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::v1::{
        ChargedProjectionQueryRunArtifacts, IndexingMemoryCredits, IndexingMemoryLimits,
        IndexingMemoryStage, PreparedQueryFieldDelta, PreparedQueryMembershipDelta,
        PreparedQueryMutationBatch, PreparedQueryRecipeDelta, PreparedQueryTermDelta,
        QueryDocValue, QueryDocumentGate, QueryPoint, QueryRunCompactionLimits, StableDocumentKey,
        append_query_run_path_copy, prepare_projection_query_run, select_query_run_compaction,
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
                            ScalarValue::String(format!("value-{version}")),
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
                    points: vec![
                        QueryPoint {
                            value: ScalarValue::Signed(version as i64),
                            document,
                            material_source_version: version,
                            live: true,
                        },
                        QueryPoint {
                            value: ScalarValue::String(format!("value-{version}")),
                            document,
                            material_source_version: version,
                            live: true,
                        },
                    ],
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
                encoded_bytes: u64::try_from(artifacts.run.bytes.len()).unwrap(),
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
        let gate = compacted
            .artifacts()
            .blocks
            .iter()
            .find(|block| block.descriptor.kind == QueryBlockKind::Gate)
            .unwrap();
        let verification_memory = pool(2 * 1024 * 1024);
        let mut verification = credits(&verification_memory, 2 * 1024 * 1024);
        let mut cursor = QueryBlockCursor::new(
            &gate.descriptor,
            &gate.bytes,
            QueryBlockLimits::default_for_memory(),
            &mut verification,
        )
        .unwrap();
        let winner = decode_document_gate(cursor.next().unwrap().unwrap()).unwrap();
        assert_eq!(winner.material_source_version, 2);
        assert_eq!(winner.current_source_version, 2);
        assert!(cursor.next().unwrap().is_none());
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
