//! Bounded, integrity-checked query blocks for v6 projection mini-runs.

use std::collections::{BTreeMap, BTreeSet};

use crate::IndexError;
use crate::typed_json::{
    Cardinality, FieldCapabilities, FieldSchema, ScalarValue, TypedJsonFieldState,
    analyze_typed_json_text, decode_scalar_sort_key, encode_scalar_sort_key,
    encode_typed_json_field_state,
};

use super::{
    ProjectionPartitionIdentity, QueryBlockCredits, QueryDocumentGate, RecipeIdentity,
    StableDocumentKey, decode_document_gate,
};

#[cfg(test)]
use super::encode_document_gate;

const BLOCK_MAGIC: &[u8; 8] = b"K6QBLK01";
const BLOCK_FORMAT: u16 = 2;
const RUN_MAGIC: &[u8; 8] = b"K6QRUN01";
const RUN_FORMAT: u16 = 1;
pub const DEFAULT_QUERY_BLOCK_BYTES: usize = 512 * 1024;
const QUERY_BLOCK_RESTART_INTERVAL: usize = 64;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum QueryBlockKind {
    TermDictionary = 1,
    Posting = 2,
    Position = 3,
    Point = 4,
    DocValue = 5,
    Gate = 6,
    /// Per-field stable-key presence deltas. The enclosing recipe is the
    /// field identity, so no reserved scalar term can collide with user data.
    Presence = 7,
}

impl QueryBlockKind {
    fn decode(value: u8) -> Result<Self, IndexError> {
        match value {
            1 => Ok(Self::TermDictionary),
            2 => Ok(Self::Posting),
            3 => Ok(Self::Position),
            4 => Ok(Self::Point),
            5 => Ok(Self::DocValue),
            6 => Ok(Self::Gate),
            7 => Ok(Self::Presence),
            _ => Err(IndexError::InvalidFormat("v6 query block kind")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryBlockLimits {
    pub maximum_block_bytes: usize,
    pub maximum_records: usize,
    pub maximum_key_bytes: usize,
    pub maximum_value_bytes: usize,
    pub maximum_loaded_blocks: usize,
    /// Maximum bytes accepted for a run descriptor, including integrity.
    pub maximum_run_descriptor_bytes: usize,
}

impl QueryBlockLimits {
    pub const fn default_for_memory() -> Self {
        Self {
            maximum_block_bytes: DEFAULT_QUERY_BLOCK_BYTES,
            maximum_records: 32_768,
            maximum_key_bytes: 32_768,
            maximum_value_bytes: DEFAULT_QUERY_BLOCK_BYTES,
            maximum_loaded_blocks: 16,
            maximum_run_descriptor_bytes: 8 * 1024 * 1024,
        }
    }

    pub fn validate(self) -> Result<Self, IndexError> {
        if self.maximum_block_bytes < 64
            || self.maximum_records == 0
            || self.maximum_key_bytes == 0
            || self.maximum_value_bytes == 0
            || self.maximum_loaded_blocks == 0
            || self.maximum_run_descriptor_bytes < 256
            || self.maximum_key_bytes > self.maximum_block_bytes
            || self.maximum_value_bytes > self.maximum_block_bytes
        {
            return Err(IndexError::InvalidDefinition(
                "v6 query block limits are invalid".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBlockRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryPostingShard {
    pub posting_block_hash: [u8; 32],
    pub posting_records: u32,
    pub minimum_document: StableDocumentKey,
    pub maximum_document: StableDocumentKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTermEntry {
    pub term: ScalarValue,
    pub posting_shards: Vec<QueryPostingShard>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryPosting {
    pub document: StableDocumentKey,
    pub material_source_version: u64,
    pub live: bool,
    pub position_block_hash: Option<[u8; 32]>,
    pub positions: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPoint {
    pub value: ScalarValue,
    pub document: StableDocumentKey,
    pub material_source_version: u64,
    pub live: bool,
}

/// Stable-key doc values used only after Boolean candidate selection for
/// order, facets, and aggregates. Values are in canonical sorted order and
/// retain repetitions for aggregate semantics;
/// `None` is an explicit field tombstone while `Some(Vec::new())` is a present
/// field with no non-null values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryDocValue {
    pub document: StableDocumentKey,
    pub material_source_version: u64,
    pub value: Option<Vec<ScalarValue>>,
}

/// Position list for one document in one term-specific position block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPositions {
    pub document: StableDocumentKey,
    pub positions: Vec<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedQueryTermDelta {
    pub term: ScalarValue,
    pub document: StableDocumentKey,
    pub material_source_version: u64,
    pub live: bool,
    pub positions: Vec<u32>,
}

/// Storage-neutral query material for one field update. The caller supplies
/// the field recipe when grouping these records into immutable block kinds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedQueryFieldDelta {
    pub presence: QueryDocumentGate,
    pub doc_value: Option<QueryDocValue>,
    pub terms: Vec<PreparedQueryTermDelta>,
    pub points: Vec<QueryPoint>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryBlockDescriptor {
    pub kind: QueryBlockKind,
    pub recipe: RecipeIdentity,
    pub minimum_key: Vec<u8>,
    pub maximum_key: Vec<u8>,
    pub hash: [u8; 32],
    pub encoded_bytes: u64,
    pub records: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedQueryBlock {
    pub descriptor: QueryBlockDescriptor,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryBlockRecordRef<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

/// Borrowing cursor over one verified bounded block. It never allocates after
/// construction and callers can stop as soon as the requested key range ends.
pub struct QueryBlockCursor<'a> {
    descriptor: &'a QueryBlockDescriptor,
    bytes: &'a [u8],
    offset: usize,
    remaining: u32,
    record_index: u32,
    records_start: usize,
    restart_offsets_start: usize,
    restart_interval: u32,
    restart_count: u32,
    previous: Option<&'a [u8]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionQueryRunDescriptor {
    pub partition: ProjectionPartitionIdentity,
    pub physical_catalog_generation: [u8; 32],
    pub sequence: u64,
    pub source_start_offset: u64,
    pub next_offset: u64,
    pub through_atomic_position: u64,
    pub blocks: Vec<QueryBlockDescriptor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedProjectionQueryRun {
    pub hash: [u8; 32],
    pub bytes: Vec<u8>,
}

/// Prepare sparse, query-ready field deltas while selected Typed JSON values
/// are resident. Old terms/points become tombstones, new values become live
/// deltas, and field presence is independent of document membership. JSON
/// pointer extraction intentionally remains outside this storage core.
pub fn prepare_typed_json_field_delta(
    field: &FieldSchema,
    document: StableDocumentKey,
    material_source_version: u64,
    previous: Option<&TypedJsonFieldState>,
    current: Option<&TypedJsonFieldState>,
    credits: &mut QueryBlockCredits,
) -> Result<PreparedQueryFieldDelta, IndexError> {
    field.validate()?;
    if material_source_version == 0 {
        return Err(IndexError::InvalidDefinition(
            "v6 query field delta version is zero".into(),
        ));
    }
    let previous = normalized_state(field, previous)?;
    let current = normalized_state(field, current)?;
    let estimated = estimate_query_delta_bytes(previous, current)?;
    credits.reserve(estimated)?;

    let old_values = state_values(previous);
    let new_values = state_values(current);
    let mut terms = Vec::new();
    if field.capabilities.contains(FieldCapabilities::FULL_TEXT) {
        let old_terms = analyzed_terms(&old_values)?;
        let new_terms = analyzed_terms(&new_values)?;
        for (term, positions) in &old_terms {
            if new_terms.get(term) != Some(positions) {
                terms.push(PreparedQueryTermDelta {
                    term: ScalarValue::String(term.clone()),
                    document,
                    material_source_version,
                    live: false,
                    positions: Vec::new(),
                });
            }
        }
        for (term, positions) in &new_terms {
            if old_terms.get(term) != Some(positions) {
                terms.push(PreparedQueryTermDelta {
                    term: ScalarValue::String(term.clone()),
                    document,
                    material_source_version,
                    live: true,
                    positions: positions.clone(),
                });
            }
        }
    } else if field.capabilities.contains(FieldCapabilities::EXACT)
        || field.capabilities.contains(FieldCapabilities::PREFIX)
    {
        for value in old_values.difference(&new_values) {
            terms.push(PreparedQueryTermDelta {
                term: value.clone(),
                document,
                material_source_version,
                live: false,
                positions: Vec::new(),
            });
        }
        for value in new_values.difference(&old_values) {
            terms.push(PreparedQueryTermDelta {
                term: value.clone(),
                document,
                material_source_version,
                live: true,
                positions: Vec::new(),
            });
        }
    }

    let mut points = Vec::new();
    if field.capabilities.contains(FieldCapabilities::RANGE) {
        for value in old_values.difference(&new_values) {
            points.push(QueryPoint {
                value: value.clone(),
                document,
                material_source_version,
                live: false,
            });
        }
        for value in new_values.difference(&old_values) {
            points.push(QueryPoint {
                value: value.clone(),
                document,
                material_source_version,
                live: true,
            });
        }
    }

    let old_doc_value = doc_values(field, previous)?;
    let new_doc_value = doc_values(field, current)?;
    let doc_value = (old_doc_value != new_doc_value).then_some(QueryDocValue {
        document,
        material_source_version,
        value: new_doc_value,
    });
    Ok(PreparedQueryFieldDelta {
        presence: QueryDocumentGate {
            document,
            material_source_version,
            current_source_version: material_source_version,
            live: current.is_some(),
            source_path: None,
            result_path: None,
            result_version: 0,
        },
        doc_value,
        terms,
        points,
    })
}

fn normalized_state<'a>(
    field: &FieldSchema,
    state: Option<&'a TypedJsonFieldState>,
) -> Result<Option<&'a TypedJsonFieldState>, IndexError> {
    let Some(state) = state.filter(|state| state.present) else {
        return Ok(None);
    };
    // The canonical codec rejects malformed/multi-value/non-field-compatible
    // input before any query records are admitted.
    encode_typed_json_field_state(field, state)?;
    Ok(Some(state))
}

fn state_values(state: Option<&TypedJsonFieldState>) -> BTreeSet<ScalarValue> {
    let mut values: BTreeSet<ScalarValue> = state
        .map(|state| state.values.iter().cloned().collect())
        .unwrap_or_default();
    if state.is_some_and(|state| state.null) {
        values.insert(ScalarValue::Null);
    }
    values
}

fn analyzed_terms(
    values: &BTreeSet<ScalarValue>,
) -> Result<BTreeMap<String, Vec<u32>>, IndexError> {
    let mut terms = BTreeMap::new();
    let mut position = 0u32;
    for value in values {
        let ScalarValue::String(value) = value else {
            return Err(IndexError::InvalidDefinition(
                "text field selected a non-string scalar".into(),
            ));
        };
        for term in analyze_typed_json_text(value) {
            terms.entry(term).or_insert_with(Vec::new).push(position);
            position = position.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        }
    }
    Ok(terms)
}

fn doc_values(
    field: &FieldSchema,
    state: Option<&TypedJsonFieldState>,
) -> Result<Option<Vec<ScalarValue>>, IndexError> {
    if !field.capabilities.contains(FieldCapabilities::ORDER)
        && !field.capabilities.contains(FieldCapabilities::FACET)
        && !field.capabilities.contains(FieldCapabilities::AGGREGATE)
    {
        return Ok(None);
    }
    if field.capabilities.contains(FieldCapabilities::ORDER)
        && field.cardinality != Cardinality::Single
    {
        return Err(IndexError::InvalidDefinition(
            "ORDER doc values require a single-valued field".into(),
        ));
    }
    Ok(state.map(|state| {
        let mut values = state.values.clone();
        if state.null {
            values.push(ScalarValue::Null);
        }
        values.sort_unstable();
        values
    }))
}

fn estimate_query_delta_bytes(
    previous: Option<&TypedJsonFieldState>,
    current: Option<&TypedJsonFieldState>,
) -> Result<usize, IndexError> {
    [previous, current]
        .into_iter()
        .flatten()
        .flat_map(|state| state.values.iter())
        .try_fold(512usize, |total, value| {
            let scalar = match value {
                ScalarValue::String(value) => value.len(),
                _ => 8,
            };
            total
                .checked_add(scalar.checked_mul(8).ok_or(IndexError::OffsetOverflow)?)
                .ok_or(IndexError::OffsetOverflow)
        })
}

pub fn encode_term_entry(entry: &QueryTermEntry) -> Result<QueryBlockRecord, IndexError> {
    let total_records = entry.posting_shards.iter().try_fold(0u64, |total, shard| {
        total.checked_add(u64::from(shard.posting_records))
    });
    if entry.posting_shards.is_empty()
        || entry
            .posting_shards
            .windows(2)
            .any(|pair| pair[0].maximum_document.bytes() >= pair[1].minimum_document.bytes())
        || entry.posting_shards.iter().any(|shard| {
            shard.posting_block_hash == [0; 32]
                || shard.posting_records == 0
                || shard.minimum_document.bytes() > shard.maximum_document.bytes()
        })
        || entry
            .posting_shards
            .iter()
            .map(|shard| shard.posting_block_hash)
            .collect::<BTreeSet<_>>()
            .len()
            != entry.posting_shards.len()
        || total_records.is_none()
    {
        return Err(IndexError::InvalidDefinition(
            "v6 term dictionary entry is invalid".into(),
        ));
    }
    let mut value = Vec::with_capacity(4 + entry.posting_shards.len() * 100);
    put_u32(&mut value, entry.posting_shards.len())?;
    for shard in &entry.posting_shards {
        value.extend_from_slice(&shard.posting_block_hash);
        value.extend_from_slice(&shard.posting_records.to_be_bytes());
        value.extend_from_slice(&shard.minimum_document.bytes());
        value.extend_from_slice(&shard.maximum_document.bytes());
    }
    Ok(QueryBlockRecord {
        key: encode_scalar_sort_key(&entry.term)?,
        value,
    })
}

pub fn decode_term_entry(
    record: QueryBlockRecordRef<'_>,
    limits: QueryBlockLimits,
) -> Result<QueryTermEntry, IndexError> {
    let limits = limits.validate()?;
    let (term, used) = decode_scalar_sort_key(record.key)?;
    if used != record.key.len()
        || record.value.len() < 104
        || record.value.len() > limits.maximum_value_bytes
    {
        return Err(IndexError::InvalidFormat("v6 term dictionary record"));
    }
    let count = read_u32(record.value, 0)? as usize;
    if count == 0
        || count > limits.maximum_records
        || record.value.len() != 4usize.saturating_add(count.saturating_mul(100))
    {
        return Err(IndexError::InvalidFormat("v6 term dictionary record"));
    }
    let mut posting_shards = Vec::with_capacity(count);
    for bytes in record.value[4..].chunks_exact(100) {
        posting_shards.push(QueryPostingShard {
            posting_block_hash: bytes[..32].try_into().map_err(|_| IndexError::Integrity)?,
            posting_records: u32::from_be_bytes(
                bytes[32..36]
                    .try_into()
                    .map_err(|_| IndexError::Integrity)?,
            ),
            minimum_document: StableDocumentKey::from_bytes(
                bytes[36..68]
                    .try_into()
                    .map_err(|_| IndexError::Integrity)?,
            )?,
            maximum_document: StableDocumentKey::from_bytes(
                bytes[68..100]
                    .try_into()
                    .map_err(|_| IndexError::Integrity)?,
            )?,
        });
    }
    let entry = QueryTermEntry {
        term,
        posting_shards,
    };
    encode_term_entry(&entry)
        .map_err(|_| IndexError::InvalidFormat("v6 term dictionary record"))?;
    Ok(entry)
}

pub fn encode_posting(posting: QueryPosting) -> Result<QueryBlockRecord, IndexError> {
    if posting.material_source_version == 0
        || posting.positions > 0 && posting.position_block_hash.is_none()
        || posting.positions == 0 && posting.position_block_hash.is_some()
        || !posting.live && (posting.positions != 0 || posting.position_block_hash.is_some())
    {
        return Err(IndexError::InvalidDefinition(
            "v6 posting is invalid".into(),
        ));
    }
    let mut value = Vec::with_capacity(46);
    value.extend_from_slice(&posting.material_source_version.to_be_bytes());
    value.push(u8::from(posting.live));
    match posting.position_block_hash {
        Some(hash) if hash != [0; 32] => {
            value.push(1);
            value.extend_from_slice(&hash);
            value.extend_from_slice(&posting.positions.to_be_bytes());
        }
        Some(_) => {
            return Err(IndexError::InvalidDefinition(
                "v6 posting is invalid".into(),
            ));
        }
        None => value.push(0),
    }
    Ok(QueryBlockRecord {
        key: posting.document.bytes().to_vec(),
        value,
    })
}

pub fn decode_posting(record: QueryBlockRecordRef<'_>) -> Result<QueryPosting, IndexError> {
    let document = stable_key(record.key)?;
    let material_source_version = read_u64(record.value, 0)?;
    if material_source_version == 0 {
        return Err(IndexError::InvalidFormat("v6 posting version"));
    }
    let live = match record.value.get(8) {
        Some(0) => false,
        Some(1) => true,
        _ => return Err(IndexError::InvalidFormat("v6 posting liveness")),
    };
    match record.value.get(9) {
        Some(0) if record.value.len() == 10 => Ok(QueryPosting {
            document,
            material_source_version,
            live,
            position_block_hash: None,
            positions: 0,
        }),
        Some(1) if live && record.value.len() == 46 => {
            let hash = record.value[10..42]
                .try_into()
                .map_err(|_| IndexError::Integrity)?;
            let positions = u32::from_be_bytes(
                record.value[42..]
                    .try_into()
                    .map_err(|_| IndexError::Integrity)?,
            );
            if hash == [0; 32] || positions == 0 {
                return Err(IndexError::InvalidFormat("v6 posting positions"));
            }
            Ok(QueryPosting {
                document,
                material_source_version,
                live,
                position_block_hash: Some(hash),
                positions,
            })
        }
        _ => Err(IndexError::InvalidFormat("v6 posting record")),
    }
}

pub fn encode_point(point: &QueryPoint) -> Result<QueryBlockRecord, IndexError> {
    if point.material_source_version == 0 {
        return Err(IndexError::InvalidDefinition(
            "v6 point version is zero".into(),
        ));
    }
    let mut key = encode_scalar_sort_key(&point.value)?;
    key.extend_from_slice(&point.document.bytes());
    let mut value = point.material_source_version.to_be_bytes().to_vec();
    value.push(u8::from(point.live));
    Ok(QueryBlockRecord { key, value })
}

pub fn decode_point(record: QueryBlockRecordRef<'_>) -> Result<QueryPoint, IndexError> {
    let (value, used) = decode_scalar_sort_key(record.key)?;
    let document = stable_key(
        record
            .key
            .get(used..)
            .ok_or(IndexError::InvalidFormat("v6 point key"))?,
    )?;
    if record.value.len() != 9 {
        return Err(IndexError::InvalidFormat("v6 point record"));
    }
    let material_source_version = read_u64(record.value, 0)?;
    let live = match record.value[8] {
        0 => false,
        1 => true,
        _ => return Err(IndexError::InvalidFormat("v6 point liveness")),
    };
    if material_source_version == 0 {
        return Err(IndexError::InvalidFormat("v6 point version"));
    }
    Ok(QueryPoint {
        value,
        document,
        material_source_version,
        live,
    })
}

pub fn encode_positions(positions: &QueryPositions) -> Result<QueryBlockRecord, IndexError> {
    if positions.positions.is_empty()
        || positions
            .positions
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err(IndexError::InvalidDefinition(
            "v6 positions are not canonical".into(),
        ));
    }
    let mut value = Vec::with_capacity(4 + positions.positions.len() * 4);
    value.extend_from_slice(
        &u32::try_from(positions.positions.len())
            .map_err(|_| IndexError::OffsetOverflow)?
            .to_be_bytes(),
    );
    for position in &positions.positions {
        value.extend_from_slice(&position.to_be_bytes());
    }
    Ok(QueryBlockRecord {
        key: positions.document.bytes().to_vec(),
        value,
    })
}

pub fn decode_positions(
    record: QueryBlockRecordRef<'_>,
    limits: QueryBlockLimits,
) -> Result<QueryPositions, IndexError> {
    let limits = limits.validate()?;
    let count =
        usize::try_from(read_u32(record.value, 0)?).map_err(|_| IndexError::OffsetOverflow)?;
    if count == 0 || count > limits.maximum_records || record.value.len() != 4 + count * 4 {
        return Err(IndexError::InvalidFormat("v6 positions record"));
    }
    let mut positions = Vec::with_capacity(count);
    for chunk in record.value[4..].chunks_exact(4) {
        positions.push(u32::from_be_bytes(
            chunk.try_into().map_err(|_| IndexError::Integrity)?,
        ));
    }
    if positions.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(IndexError::UnsortedRecords);
    }
    Ok(QueryPositions {
        document: stable_key(record.key)?,
        positions,
    })
}

pub fn encode_query_block(
    kind: QueryBlockKind,
    recipe: RecipeIdentity,
    records: &[QueryBlockRecord],
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
) -> Result<EncodedQueryBlock, IndexError> {
    let limits = limits.validate()?;
    if records.is_empty() || records.len() > limits.maximum_records {
        return Err(IndexError::ResourceLimit {
            needed: records.len(),
            limit: limits.maximum_records,
        });
    }
    let restart_count = records.len().div_ceil(QUERY_BLOCK_RESTART_INTERVAL);
    let mut required: usize = 8 + 2 + 1 + 32 + 4 + 4 + 4;
    required = required
        .checked_add(
            restart_count
                .checked_mul(4)
                .ok_or(IndexError::OffsetOverflow)?,
        )
        .ok_or(IndexError::OffsetOverflow)?;
    let mut previous = None::<&[u8]>;
    for record in records {
        if record.key.is_empty()
            || record.key.len() > limits.maximum_key_bytes
            || record.value.len() > limits.maximum_value_bytes
            || previous.is_some_and(|previous| previous >= record.key.as_slice())
        {
            return Err(IndexError::InvalidDefinition(
                "v6 query block records are not bounded canonical order".into(),
            ));
        }
        required = required
            .checked_add(8)
            .and_then(|bytes| bytes.checked_add(record.key.len()))
            .and_then(|bytes| bytes.checked_add(record.value.len()))
            .ok_or(IndexError::OffsetOverflow)?;
        previous = Some(&record.key);
    }
    required = required.checked_add(32).ok_or(IndexError::OffsetOverflow)?;
    if required > limits.maximum_block_bytes {
        return Err(IndexError::ResourceLimit {
            needed: required,
            limit: limits.maximum_block_bytes,
        });
    }
    credits.reserve(required)?;
    let mut bytes = Vec::with_capacity(required);
    bytes.extend_from_slice(BLOCK_MAGIC);
    bytes.extend_from_slice(&BLOCK_FORMAT.to_be_bytes());
    bytes.push(kind as u8);
    bytes.extend_from_slice(&recipe.bytes());
    put_u32(&mut bytes, records.len())?;
    put_u32(&mut bytes, QUERY_BLOCK_RESTART_INTERVAL)?;
    put_u32(&mut bytes, restart_count)?;
    let restart_offsets_start = bytes.len();
    bytes.resize(
        restart_offsets_start
            .checked_add(
                restart_count
                    .checked_mul(4)
                    .ok_or(IndexError::OffsetOverflow)?,
            )
            .ok_or(IndexError::OffsetOverflow)?,
        0,
    );
    let records_start = bytes.len();
    for (index, record) in records.iter().enumerate() {
        if index % QUERY_BLOCK_RESTART_INTERVAL == 0 {
            let offset = u32::try_from(bytes.len() - records_start)
                .map_err(|_| IndexError::OffsetOverflow)?;
            let target = restart_offsets_start
                .checked_add((index / QUERY_BLOCK_RESTART_INTERVAL) * 4)
                .ok_or(IndexError::OffsetOverflow)?;
            bytes[target..target + 4].copy_from_slice(&offset.to_be_bytes());
        }
        put_bytes(&mut bytes, &record.key)?;
        put_bytes(&mut bytes, &record.value)?;
    }
    let integrity = *blake3::hash(&bytes).as_bytes();
    bytes.extend_from_slice(&integrity);
    if bytes.len() != required {
        return Err(IndexError::Integrity);
    }
    let descriptor = QueryBlockDescriptor {
        kind,
        recipe,
        minimum_key: records.first().expect("nonempty").key.clone(),
        maximum_key: records.last().expect("nonempty").key.clone(),
        hash: *blake3::hash(&bytes).as_bytes(),
        encoded_bytes: bytes.len() as u64,
        records: u32::try_from(records.len()).map_err(|_| IndexError::OffsetOverflow)?,
    };
    Ok(EncodedQueryBlock { descriptor, bytes })
}

impl<'a> QueryBlockCursor<'a> {
    pub fn new(
        descriptor: &'a QueryBlockDescriptor,
        bytes: &'a [u8],
        limits: QueryBlockLimits,
        credits: &mut QueryBlockCredits,
    ) -> Result<Self, IndexError> {
        let limits = limits.validate()?;
        if bytes.len() > limits.maximum_block_bytes
            || bytes.len() as u64 != descriptor.encoded_bytes
            || *blake3::hash(bytes).as_bytes() != descriptor.hash
        {
            return Err(IndexError::Integrity);
        }
        credits.reserve_loaded_block(bytes.len(), limits.maximum_loaded_blocks)?;
        let split = bytes
            .len()
            .checked_sub(32)
            .ok_or(IndexError::UnexpectedEof {
                expected: 32,
                actual: bytes.len() as u64,
            })?;
        let (payload, integrity) = bytes.split_at(split);
        if blake3::hash(payload).as_bytes() != integrity {
            return Err(IndexError::Integrity);
        }
        let mut input = BlockInput::new(payload);
        input.expect(BLOCK_MAGIC)?;
        if input.u16()? != BLOCK_FORMAT
            || QueryBlockKind::decode(input.byte()?)? != descriptor.kind
            || input.array_32()? != descriptor.recipe.bytes()
        {
            return Err(IndexError::InvalidFormat("v6 query block header"));
        }
        let records = input.u32()?;
        if records == 0
            || records != descriptor.records
            || records as usize > limits.maximum_records
        {
            return Err(IndexError::InvalidFormat("v6 query block record count"));
        }
        let restart_interval = input.u32()?;
        let restart_count = input.u32()?;
        if restart_interval == 0 || restart_interval as usize > limits.maximum_records {
            return Err(IndexError::InvalidFormat("v6 query block restart table"));
        }
        let expected_restarts = records.div_ceil(restart_interval);
        if restart_count != expected_restarts {
            return Err(IndexError::InvalidFormat("v6 query block restart table"));
        }
        let restart_offsets_start = input.offset;
        let restart_bytes = usize::try_from(restart_count)
            .map_err(|_| IndexError::OffsetOverflow)?
            .checked_mul(4)
            .ok_or(IndexError::OffsetOverflow)?;
        input.take(restart_bytes)?;
        let records_start = input.offset;
        let mut prior = None;
        for restart in 0..restart_count {
            let offset = restart_offset(payload, restart_offsets_start, restart)?;
            if (restart == 0 && offset != 0)
                || prior.is_some_and(|prior| prior >= offset)
                || offset as usize >= payload.len().saturating_sub(records_start)
            {
                return Err(IndexError::InvalidFormat("v6 query block restart offsets"));
            }
            prior = Some(offset);
        }
        Ok(Self {
            descriptor,
            bytes: payload,
            offset: input.offset,
            remaining: records,
            record_index: 0,
            records_start,
            restart_offsets_start,
            restart_interval,
            restart_count,
            previous: None,
        })
    }

    pub fn next(&mut self) -> Result<Option<QueryBlockRecordRef<'a>>, IndexError> {
        if self.remaining == 0 {
            if self.offset != self.bytes.len() {
                return Err(IndexError::InvalidFormat("v6 query block trailing bytes"));
            }
            return Ok(None);
        }
        let mut input = BlockInput {
            bytes: self.bytes,
            offset: self.offset,
        };
        let key = input.bytes()?;
        let value = input.bytes()?;
        if key.is_empty() || self.previous.is_some_and(|previous| previous >= key) {
            return Err(IndexError::UnsortedRecords);
        }
        self.previous = Some(key);
        self.offset = input.offset;
        self.remaining -= 1;
        self.record_index = self
            .record_index
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        if self.remaining == 0 && self.offset != self.bytes.len() {
            return Err(IndexError::InvalidFormat("v6 query block trailing bytes"));
        }
        Ok(Some(QueryBlockRecordRef { key, value }))
    }

    pub fn seek_to(&mut self, key: &[u8]) -> Result<Option<QueryBlockRecordRef<'a>>, IndexError> {
        let mut lower = 0u32;
        let mut upper = self.restart_count;
        while lower < upper {
            let midpoint = lower + (upper - lower) / 2;
            let offset = restart_offset(self.bytes, self.restart_offsets_start, midpoint)?;
            let record = record_at(self.bytes, self.records_start, offset as usize)?;
            if record.key <= key {
                lower = midpoint + 1;
            } else {
                upper = midpoint;
            }
        }
        let restart = lower.saturating_sub(1);
        let offset = restart_offset(self.bytes, self.restart_offsets_start, restart)?;
        self.offset = self
            .records_start
            .checked_add(offset as usize)
            .ok_or(IndexError::OffsetOverflow)?;
        self.record_index = restart
            .checked_mul(self.restart_interval)
            .ok_or(IndexError::OffsetOverflow)?;
        self.remaining = self
            .descriptor
            .records
            .checked_sub(self.record_index)
            .ok_or(IndexError::Integrity)?;
        self.previous = None;
        while let Some(record) = self.next()? {
            if record.key >= key {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    pub const fn descriptor(&self) -> &QueryBlockDescriptor {
        self.descriptor
    }
}

impl ProjectionQueryRunDescriptor {
    pub fn validate(&self, limits: QueryBlockLimits) -> Result<(), IndexError> {
        limits.validate()?;
        self.partition.validate()?;
        if self.physical_catalog_generation == [0; 32]
            || self.sequence == 0
            || self.source_start_offset >= self.next_offset
            || self.blocks.len() > limits.maximum_loaded_blocks.saturating_mul(4096)
        {
            return Err(IndexError::InvalidDefinition(
                "v6 query run descriptor is invalid".into(),
            ));
        }
        let mut total_descriptor_key_bytes = 0usize;
        let mut previous = None::<(&QueryBlockKind, &RecipeIdentity, &[u8], &[u8], &[u8; 32])>;
        for block in &self.blocks {
            if block.hash == [0; 32]
                || block.encoded_bytes == 0
                || usize::try_from(block.encoded_bytes)
                    .map_or(true, |bytes| bytes > limits.maximum_block_bytes)
                || block.records == 0
                || block.records as usize > limits.maximum_records
                || block.minimum_key.is_empty()
                || block.minimum_key > block.maximum_key
                || block.minimum_key.len() > limits.maximum_key_bytes
                || block.maximum_key.len() > limits.maximum_key_bytes
            {
                return Err(IndexError::InvalidDefinition(
                    "v6 query block descriptor is invalid".into(),
                ));
            }
            total_descriptor_key_bytes = total_descriptor_key_bytes
                .checked_add(block.minimum_key.len())
                .and_then(|bytes| bytes.checked_add(block.maximum_key.len()))
                .ok_or(IndexError::OffsetOverflow)?;
            let current = (
                &block.kind,
                &block.recipe,
                block.minimum_key.as_slice(),
                block.maximum_key.as_slice(),
                &block.hash,
            );
            if previous.is_some_and(|previous| previous >= current) {
                return Err(IndexError::InvalidDefinition(
                    "v6 query block descriptors are not canonical order".into(),
                ));
            }
            previous = Some(current);
        }
        if total_descriptor_key_bytes > limits.maximum_run_descriptor_bytes {
            return Err(IndexError::ResourceLimit {
                needed: total_descriptor_key_bytes,
                limit: limits.maximum_run_descriptor_bytes,
            });
        }
        Ok(())
    }

    pub fn matching_blocks<'a>(
        &'a self,
        kind: QueryBlockKind,
        recipe: RecipeIdentity,
        lower: &[u8],
        upper: &[u8],
    ) -> impl Iterator<Item = &'a QueryBlockDescriptor> {
        self.blocks.iter().filter(move |block| {
            block.kind == kind
                && block.recipe == recipe
                && block.maximum_key.as_slice() >= lower
                && block.minimum_key.as_slice() <= upper
        })
    }
}

pub fn encode_projection_query_run(
    descriptor: &ProjectionQueryRunDescriptor,
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
) -> Result<EncodedProjectionQueryRun, IndexError> {
    descriptor.validate(limits)?;
    let mut required: usize = 8 + 2 + 96 + 32 + 8 * 4 + 4;
    for block in &descriptor.blocks {
        required = required
            .checked_add(
                1 + 32 + 8 + 4 + 4 + block.minimum_key.len() + 4 + block.maximum_key.len() + 32,
            )
            .ok_or(IndexError::OffsetOverflow)?;
    }
    required = required.checked_add(32).ok_or(IndexError::OffsetOverflow)?;
    if required > limits.maximum_run_descriptor_bytes {
        return Err(IndexError::ResourceLimit {
            needed: required,
            limit: limits.maximum_run_descriptor_bytes,
        });
    }
    credits.reserve(required)?;
    let mut bytes = Vec::with_capacity(required);
    bytes.extend_from_slice(RUN_MAGIC);
    bytes.extend_from_slice(&RUN_FORMAT.to_be_bytes());
    put_partition(&mut bytes, descriptor.partition);
    bytes.extend_from_slice(&descriptor.physical_catalog_generation);
    put_u64(&mut bytes, descriptor.sequence);
    put_u64(&mut bytes, descriptor.source_start_offset);
    put_u64(&mut bytes, descriptor.next_offset);
    put_u64(&mut bytes, descriptor.through_atomic_position);
    put_u32(&mut bytes, descriptor.blocks.len())?;
    for block in &descriptor.blocks {
        bytes.push(block.kind as u8);
        bytes.extend_from_slice(&block.recipe.bytes());
        put_u64(&mut bytes, block.encoded_bytes);
        put_u32(&mut bytes, block.records as usize)?;
        put_bytes(&mut bytes, &block.minimum_key)?;
        put_bytes(&mut bytes, &block.maximum_key)?;
        bytes.extend_from_slice(&block.hash);
    }
    let integrity = *blake3::hash(&bytes).as_bytes();
    bytes.extend_from_slice(&integrity);
    if bytes.len() != required {
        return Err(IndexError::Integrity);
    }
    Ok(EncodedProjectionQueryRun {
        hash: *blake3::hash(&bytes).as_bytes(),
        bytes,
    })
}

pub fn decode_projection_query_run(
    bytes: &[u8],
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
) -> Result<ProjectionQueryRunDescriptor, IndexError> {
    let limits = limits.validate()?;
    if bytes.len() > limits.maximum_run_descriptor_bytes {
        return Err(IndexError::ResourceLimit {
            needed: bytes.len(),
            limit: limits.maximum_run_descriptor_bytes,
        });
    }
    let split = bytes
        .len()
        .checked_sub(32)
        .ok_or(IndexError::UnexpectedEof {
            expected: 32,
            actual: bytes.len() as u64,
        })?;
    let (payload, integrity) = bytes.split_at(split);
    if blake3::hash(payload).as_bytes() != integrity {
        return Err(IndexError::Integrity);
    }
    let mut input = BlockInput::new(payload);
    input.expect(RUN_MAGIC)?;
    if input.u16()? != RUN_FORMAT {
        return Err(IndexError::InvalidFormat("v6 query run format"));
    }
    let partition = read_partition(&mut input)?;
    let physical_catalog_generation = input.array_32()?;
    let sequence = input.u64()?;
    let source_start_offset = input.u64()?;
    let next_offset = input.u64()?;
    let through_atomic_position = input.u64()?;
    let count = input.u32()? as usize;
    if count > limits.maximum_loaded_blocks.saturating_mul(4096) {
        return Err(IndexError::InvalidFormat("v6 query run block count"));
    }
    const MINIMUM_DESCRIPTOR_BLOCK_BYTES: usize = 1 + 32 + 8 + 4 + 4 + 4 + 32;
    if count > input.remaining() / MINIMUM_DESCRIPTOR_BLOCK_BYTES {
        return Err(IndexError::UnexpectedEof {
            expected: count
                .checked_mul(MINIMUM_DESCRIPTOR_BLOCK_BYTES)
                .and_then(|size| size.checked_add(input.offset))
                .ok_or(IndexError::OffsetOverflow)? as u64,
            actual: input.bytes.len() as u64,
        });
    }
    credits.reserve(bytes.len())?;
    let mut blocks = Vec::with_capacity(count);
    for _ in 0..count {
        let kind = QueryBlockKind::decode(input.byte()?)?;
        let recipe = RecipeIdentity::new(input.array_32()?)?;
        let encoded_bytes = input.u64()?;
        let records = input.u32()?;
        let minimum_key = input.bytes()?.to_vec();
        let maximum_key = input.bytes()?.to_vec();
        let hash = input.array_32()?;
        blocks.push(QueryBlockDescriptor {
            kind,
            recipe,
            minimum_key,
            maximum_key,
            hash,
            encoded_bytes,
            records,
        });
    }
    input.finish()?;
    let descriptor = ProjectionQueryRunDescriptor {
        partition,
        physical_catalog_generation,
        sequence,
        source_start_offset,
        next_offset,
        through_atomic_position,
        blocks,
    };
    descriptor.validate(limits)?;
    Ok(descriptor)
}

pub fn merge_query_block_records(
    kind: QueryBlockKind,
    recipe: RecipeIdentity,
    inputs: &mut [QueryBlockCursor<'_>],
    limits: QueryBlockLimits,
    output: &mut impl FnMut(QueryBlockRecordRef<'_>) -> Result<(), IndexError>,
) -> Result<(), IndexError> {
    let limits = limits.validate()?;
    if inputs.is_empty() || inputs.len() > limits.maximum_loaded_blocks {
        return Err(IndexError::ResourceLimit {
            needed: inputs.len(),
            limit: limits.maximum_loaded_blocks,
        });
    }
    if inputs
        .iter()
        .any(|input| input.descriptor.kind != kind || input.descriptor.recipe != recipe)
    {
        return Err(IndexError::InvalidDefinition(
            "v6 query block merge lanes disagree".into(),
        ));
    }
    let mut current = inputs
        .iter_mut()
        .map(QueryBlockCursor::next)
        .collect::<Result<Vec<_>, _>>()?;
    loop {
        let Some((winner, key)) = current
            .iter()
            .enumerate()
            .filter_map(|(index, record)| record.map(|record| (index, record.key)))
            .min_by(|left, right| left.1.cmp(right.1).then_with(|| left.0.cmp(&right.0)))
        else {
            return Ok(());
        };
        output(current[winner].expect("winner exists"))?;
        for (index, record) in current.iter_mut().enumerate() {
            if record.as_ref().is_some_and(|record| record.key == key) {
                *record = inputs[index].next()?;
            }
        }
    }
}

/// Visit only live documents from newest-first posting lanes for one selected
/// term. Tombstones still participate in merge precedence but are not emitted
/// as candidates, so an old-term removal or delete cannot resurrect a stale
/// posting from an older run.
pub fn visit_live_postings(
    recipe: RecipeIdentity,
    inputs: &mut [QueryBlockCursor<'_>],
    limits: QueryBlockLimits,
    visit: &mut impl FnMut(QueryPosting) -> Result<(), IndexError>,
) -> Result<(), IndexError> {
    merge_query_block_records(
        QueryBlockKind::Posting,
        recipe,
        inputs,
        limits,
        &mut |record| {
            let posting = decode_posting(record)?;
            if posting.live {
                visit(posting)?;
            }
            Ok(())
        },
    )
}

/// Visit the newest live stable keys in either the membership universe (`Gate`)
/// or one field's query-ready presence stream (`Presence`). This is the only
/// universe used for Boolean NOT/Exists; it never consults source objects or
/// opaque field state.
pub fn visit_live_gates(
    kind: QueryBlockKind,
    recipe: RecipeIdentity,
    inputs: &mut [QueryBlockCursor<'_>],
    limits: QueryBlockLimits,
    visit: &mut impl FnMut(QueryDocumentGate) -> Result<(), IndexError>,
) -> Result<(), IndexError> {
    if !matches!(kind, QueryBlockKind::Gate | QueryBlockKind::Presence) {
        return Err(IndexError::InvalidDefinition(
            "v6 live gate query requires gate or presence blocks".into(),
        ));
    }
    merge_query_block_records(kind, recipe, inputs, limits, &mut |record| {
        let gate = decode_document_gate(record)?;
        if gate.live {
            visit(gate)?;
        }
        Ok(())
    })
}

/// Seek one exact term dictionary block without scanning its postings.
pub fn seek_exact_term(
    cursor: &mut QueryBlockCursor<'_>,
    term: &ScalarValue,
) -> Result<Option<QueryTermEntry>, IndexError> {
    let key = encode_scalar_sort_key(term)?;
    let Some(record) = cursor.seek_to(&key)? else {
        return Ok(None);
    };
    let entry = decode_term_entry(record, QueryBlockLimits::default_for_memory())?;
    Ok((entry.term == *term).then_some(entry))
}

/// Iterate lexically contiguous keyword/text terms beginning with `prefix`.
pub fn visit_prefix_terms(
    cursor: &mut QueryBlockCursor<'_>,
    prefix: &str,
    visit: &mut impl FnMut(QueryTermEntry) -> Result<(), IndexError>,
) -> Result<(), IndexError> {
    let start = encode_scalar_sort_key(&ScalarValue::String(prefix.into()))?;
    let mut next = cursor.seek_to(&start)?;
    while let Some(record) = next {
        let entry = decode_term_entry(record, QueryBlockLimits::default_for_memory())?;
        let ScalarValue::String(term) = &entry.term else {
            return Ok(());
        };
        if !term.starts_with(prefix) {
            return Ok(());
        }
        visit(entry)?;
        next = cursor.next()?;
    }
    Ok(())
}

/// Stream live range candidates from selected point lanes. The caller chooses
/// descriptor ranges first; only matching immutable point blocks are loaded.
pub fn visit_live_range_points(
    recipe: RecipeIdentity,
    inputs: &mut [QueryBlockCursor<'_>],
    limits: QueryBlockLimits,
    lower: Option<&ScalarValue>,
    upper: Option<&ScalarValue>,
    visit: &mut impl FnMut(QueryPoint) -> Result<(), IndexError>,
) -> Result<(), IndexError> {
    merge_query_block_records(
        QueryBlockKind::Point,
        recipe,
        inputs,
        limits,
        &mut |record| {
            let point = decode_point(record)?;
            if point.live
                && lower.is_none_or(|lower| point.value >= lower.clone())
                && upper.is_none_or(|upper| point.value <= upper.clone())
            {
                visit(point)?;
            }
            Ok(())
        },
    )
}

fn put_partition(out: &mut Vec<u8>, value: ProjectionPartitionIdentity) {
    out.extend_from_slice(&value.family_id);
    put_u64(out, value.source_node);
    out.extend_from_slice(&value.source_epoch);
    put_u64(out, value.producer_node);
    put_u64(out, value.placement_term);
    put_u64(out, value.placement_index);
}
fn stable_key(bytes: &[u8]) -> Result<StableDocumentKey, IndexError> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| IndexError::InvalidFormat("v6 stable document key"))?;
    StableDocumentKey::from_bytes(bytes)
}
fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, IndexError> {
    let end = offset.checked_add(4).ok_or(IndexError::OffsetOverflow)?;
    Ok(u32::from_be_bytes(
        bytes
            .get(offset..end)
            .ok_or(IndexError::UnexpectedEof {
                expected: end as u64,
                actual: bytes.len() as u64,
            })?
            .try_into()
            .map_err(|_| IndexError::Integrity)?,
    ))
}
fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, IndexError> {
    let end = offset.checked_add(8).ok_or(IndexError::OffsetOverflow)?;
    Ok(u64::from_be_bytes(
        bytes
            .get(offset..end)
            .ok_or(IndexError::UnexpectedEof {
                expected: end as u64,
                actual: bytes.len() as u64,
            })?
            .try_into()
            .map_err(|_| IndexError::Integrity)?,
    ))
}
fn read_partition(input: &mut BlockInput<'_>) -> Result<ProjectionPartitionIdentity, IndexError> {
    ProjectionPartitionIdentity::new(
        input.array_32()?,
        input.u64()?,
        input.array_32()?,
        input.u64()?,
        input.u64()?,
        input.u64()?,
    )
}
fn restart_offset(
    bytes: &[u8],
    restart_offsets_start: usize,
    restart: u32,
) -> Result<u32, IndexError> {
    let offset = restart_offsets_start
        .checked_add(
            usize::try_from(restart)
                .map_err(|_| IndexError::OffsetOverflow)?
                .checked_mul(4)
                .ok_or(IndexError::OffsetOverflow)?,
        )
        .ok_or(IndexError::OffsetOverflow)?;
    read_u32(bytes, offset)
}
fn record_at<'a>(
    bytes: &'a [u8],
    records_start: usize,
    offset: usize,
) -> Result<QueryBlockRecordRef<'a>, IndexError> {
    let offset = records_start
        .checked_add(offset)
        .ok_or(IndexError::OffsetOverflow)?;
    let mut input = BlockInput { bytes, offset };
    let key = input.bytes()?;
    let value = input.bytes()?;
    Ok(QueryBlockRecordRef { key, value })
}
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_u32(out: &mut Vec<u8>, value: usize) -> Result<(), IndexError> {
    out.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| IndexError::OffsetOverflow)?
            .to_be_bytes(),
    );
    Ok(())
}
fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), IndexError> {
    put_u32(out, value.len())?;
    out.extend_from_slice(value);
    Ok(())
}
struct BlockInput<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> BlockInput<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], IndexError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(IndexError::UnexpectedEof {
                expected: end as u64,
                actual: self.bytes.len() as u64,
            })?;
        self.offset = end;
        Ok(value)
    }
    fn expect(&mut self, expected: &[u8]) -> Result<(), IndexError> {
        if self.take(expected.len())? != expected {
            return Err(IndexError::InvalidFormat("v6 query block magic"));
        }
        Ok(())
    }
    fn byte(&mut self) -> Result<u8, IndexError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, IndexError> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        ))
    }
    fn u32(&mut self) -> Result<u32, IndexError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        ))
    }
    fn u64(&mut self) -> Result<u64, IndexError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| IndexError::Integrity)?,
        ))
    }
    fn array_32(&mut self) -> Result<[u8; 32], IndexError> {
        self.take(32)?.try_into().map_err(|_| IndexError::Integrity)
    }
    fn bytes(&mut self) -> Result<&'a [u8], IndexError> {
        let length = usize::try_from(self.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        self.take(length)
    }
    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }
    fn finish(self) -> Result<(), IndexError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(IndexError::InvalidFormat("v6 query block trailing bytes"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typed_json::{Collation, FieldId, FieldType};
    use crate::v6::{
        IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage, QueryMemoryPermit,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestQueryPermit {
        bytes: usize,
        drops: Arc<AtomicUsize>,
    }

    impl QueryMemoryPermit for TestQueryPermit {
        fn admitted_bytes(&self) -> usize {
            self.bytes
        }
    }

    impl Drop for TestQueryPermit {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn query_credits_retain_and_release_the_runtime_admission() {
        let drops = Arc::new(AtomicUsize::new(0));
        let credits = QueryBlockCredits::from_query_permit(Box::new(TestQueryPermit {
            bytes: 4096,
            drops: drops.clone(),
        }))
        .unwrap();
        assert_eq!(credits.remaining(), 4096);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        drop(credits);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    fn credits(bytes: usize) -> QueryBlockCredits {
        let limits = IndexingMemoryLimits {
            hot_payload_bytes: bytes,
            worker_scratch_bytes: bytes,
            prepared_rows_bytes: bytes,
            replay_input_bytes: bytes,
            projection_accumulator_bytes: bytes,
            seal_scratch_bytes: bytes,
            ordering_catalog_bytes: bytes,
        };
        let memory = IndexingMemoryCredits::new(bytes, limits).unwrap();
        let permit = memory
            .acquire(IndexingMemoryStage::OrderingCatalog, bytes)
            .unwrap();
        QueryBlockCredits::from_pipeline_permit(permit)
    }

    fn recipe() -> RecipeIdentity {
        RecipeIdentity::new([7; 32]).unwrap()
    }
    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 2, [3; 32], 2, 4, 5).unwrap()
    }
    fn records() -> Vec<QueryBlockRecord> {
        vec![
            QueryBlockRecord {
                key: b"alpha".to_vec(),
                value: b"one".to_vec(),
            },
            QueryBlockRecord {
                key: b"beta".to_vec(),
                value: b"two".to_vec(),
            },
        ]
    }

    fn key(byte: u8) -> StableDocumentKey {
        StableDocumentKey::from_bytes([byte; 32]).unwrap()
    }

    fn record_ref(record: &QueryBlockRecord) -> QueryBlockRecordRef<'_> {
        QueryBlockRecordRef {
            key: &record.key,
            value: &record.value,
        }
    }

    #[test]
    fn typed_query_records_are_canonical_and_round_trip() {
        let term = QueryTermEntry {
            term: ScalarValue::String("rust".into()),
            posting_shards: vec![QueryPostingShard {
                posting_block_hash: [1; 32],
                posting_records: 2,
                minimum_document: key(1),
                maximum_document: key(2),
            }],
        };
        let term_record = encode_term_entry(&term).unwrap();
        assert_eq!(
            decode_term_entry(
                record_ref(&term_record),
                QueryBlockLimits::default_for_memory()
            )
            .unwrap(),
            term
        );

        let posting = QueryPosting {
            document: key(2),
            material_source_version: 3,
            live: true,
            position_block_hash: Some([4; 32]),
            positions: 2,
        };
        let posting_record = encode_posting(posting).unwrap();
        assert_eq!(
            decode_posting(record_ref(&posting_record)).unwrap(),
            posting
        );

        let point = QueryPoint {
            value: ScalarValue::String("rust\0index".into()),
            document: key(3),
            material_source_version: 4,
            live: true,
        };
        let point_record = encode_point(&point).unwrap();
        assert_eq!(decode_point(record_ref(&point_record)).unwrap(), point);

        let gate = QueryDocumentGate {
            document: key(5),
            material_source_version: 6,
            current_source_version: 8,
            live: false,
            source_path: Some("objects/5.json".into()),
            result_path: Some("results/5.json".into()),
            result_version: 8,
        };
        let gate_record = encode_document_gate(gate.clone()).unwrap();
        assert_eq!(
            decode_document_gate(record_ref(&gate_record)).unwrap(),
            gate
        );

        let positions = QueryPositions {
            document: key(6),
            positions: vec![0, 3, 8],
        };
        let position_record = encode_positions(&positions).unwrap();
        assert_eq!(
            decode_positions(
                record_ref(&position_record),
                QueryBlockLimits::default_for_memory()
            )
            .unwrap(),
            positions
        );
    }

    #[test]
    fn cursor_is_lazy_integrity_checked_and_seekable() {
        let limits = QueryBlockLimits::default_for_memory();
        let mut credits = credits(DEFAULT_QUERY_BLOCK_BYTES);
        let encoded = encode_query_block(
            QueryBlockKind::TermDictionary,
            recipe(),
            &records(),
            limits,
            &mut credits,
        )
        .unwrap();
        let mut cursor =
            QueryBlockCursor::new(&encoded.descriptor, &encoded.bytes, limits, &mut credits)
                .unwrap();
        assert_eq!(cursor.seek_to(b"beta").unwrap().unwrap().value, b"two");
        assert!(cursor.next().unwrap().is_none());
        let mut corrupt = encoded.bytes.clone();
        corrupt[0] ^= 1;
        assert!(
            QueryBlockCursor::new(&encoded.descriptor, &corrupt, limits, &mut credits).is_err()
        );
    }

    #[test]
    fn restart_table_limits_exact_seek_to_one_block_tail() {
        let limits = QueryBlockLimits::default_for_memory();
        let records = (0u16..192)
            .map(|value| QueryBlockRecord {
                key: format!("term-{value:03}").into_bytes(),
                value: value.to_be_bytes().to_vec(),
            })
            .collect::<Vec<_>>();
        let mut credits = credits(DEFAULT_QUERY_BLOCK_BYTES);
        let encoded = encode_query_block(
            QueryBlockKind::TermDictionary,
            recipe(),
            &records,
            limits,
            &mut credits,
        )
        .unwrap();
        let mut cursor =
            QueryBlockCursor::new(&encoded.descriptor, &encoded.bytes, limits, &mut credits)
                .unwrap();
        assert_eq!(
            cursor.seek_to(b"term-151").unwrap().unwrap().key,
            b"term-151"
        );
        assert!(cursor.record_index < 192);
        assert!(cursor.record_index > 128);
    }

    #[test]
    fn encoded_posting_merge_keeps_old_term_tombstones_and_suppresses_delete() {
        let limits = QueryBlockLimits::default_for_memory();
        let document = key(9);
        // Build with a shared reservation so both immutable source blocks and
        // their caller-loaded cursors are charged to one pipeline admission.
        let mut reservation = credits(DEFAULT_QUERY_BLOCK_BYTES);
        let old_live = encode_query_block(
            QueryBlockKind::Posting,
            recipe(),
            &[encode_posting(QueryPosting {
                document,
                material_source_version: 1,
                live: true,
                position_block_hash: None,
                positions: 0,
            })
            .unwrap()],
            limits,
            &mut reservation,
        )
        .unwrap();
        let old_term_removal = encode_query_block(
            QueryBlockKind::Posting,
            recipe(),
            &[encode_posting(QueryPosting {
                document,
                material_source_version: 2,
                live: false,
                position_block_hash: None,
                positions: 0,
            })
            .unwrap()],
            limits,
            &mut reservation,
        )
        .unwrap();
        let mut inputs = [
            QueryBlockCursor::new(
                &old_term_removal.descriptor,
                &old_term_removal.bytes,
                limits,
                &mut reservation,
            )
            .unwrap(),
            QueryBlockCursor::new(
                &old_live.descriptor,
                &old_live.bytes,
                limits,
                &mut reservation,
            )
            .unwrap(),
        ];
        let mut candidates = Vec::new();
        visit_live_postings(recipe(), &mut inputs, limits, &mut |posting| {
            candidates.push(posting.document);
            Ok(())
        })
        .unwrap();
        assert!(candidates.is_empty());

        let new_term = encode_query_block(
            QueryBlockKind::Posting,
            recipe(),
            &[encode_posting(QueryPosting {
                document,
                material_source_version: 2,
                live: true,
                position_block_hash: None,
                positions: 0,
            })
            .unwrap()],
            limits,
            &mut reservation,
        )
        .unwrap();
        let mut new_inputs = [QueryBlockCursor::new(
            &new_term.descriptor,
            &new_term.bytes,
            limits,
            &mut reservation,
        )
        .unwrap()];
        visit_live_postings(recipe(), &mut new_inputs, limits, &mut |posting| {
            candidates.push(posting.document);
            Ok(())
        })
        .unwrap();
        assert_eq!(candidates, vec![document]);
    }

    #[test]
    fn credit_refusal_happens_before_output_allocation() {
        let limits = QueryBlockLimits::default_for_memory();
        let mut credits = credits(1);
        assert!(matches!(
            encode_query_block(
                QueryBlockKind::Posting,
                recipe(),
                &records(),
                limits,
                &mut credits
            ),
            Err(IndexError::ResourceLimit { .. })
        ));
        assert_eq!(credits.remaining(), 1);
    }

    #[test]
    fn multi_numeric_facet_and_aggregate_preserve_all_values_and_tombstone() {
        let field = FieldSchema {
            id: FieldId::new(0),
            name: "scores".into(),
            source_selector: "/scores".into(),
            field_type: FieldType::UnsignedInteger,
            cardinality: Cardinality::Multi,
            allow_missing: true,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::FACET.union(FieldCapabilities::AGGREGATE),
            analyzer: None,
            date_format: None,
        };
        let state = TypedJsonFieldState::from_selected(
            &field,
            Some(vec![
                ScalarValue::Unsigned(9),
                ScalarValue::Unsigned(2),
                ScalarValue::Unsigned(2),
            ]),
        )
        .unwrap();
        let mut memory = credits(4096);
        let created =
            prepare_typed_json_field_delta(&field, key(7), 1, None, Some(&state), &mut memory)
                .unwrap();
        assert_eq!(
            created.doc_value.unwrap().value,
            Some(vec![
                ScalarValue::Unsigned(2),
                ScalarValue::Unsigned(2),
                ScalarValue::Unsigned(9),
            ])
        );
        let mut memory = credits(4096);
        let deleted =
            prepare_typed_json_field_delta(&field, key(7), 2, Some(&state), None, &mut memory)
                .unwrap();
        assert_eq!(deleted.doc_value.unwrap().value, None);
    }

    #[test]
    fn descriptor_is_exact_family_partition_catalog_and_cut_binding() {
        let limits = QueryBlockLimits::default_for_memory();
        let mut credits = credits(DEFAULT_QUERY_BLOCK_BYTES);
        let block = encode_query_block(
            QueryBlockKind::Point,
            recipe(),
            &records(),
            limits,
            &mut credits,
        )
        .unwrap();
        let descriptor = ProjectionQueryRunDescriptor {
            partition: partition(),
            physical_catalog_generation: [8; 32],
            sequence: 1,
            source_start_offset: 9,
            next_offset: 10,
            through_atomic_position: 11,
            blocks: vec![block.descriptor],
        };
        let encoded = encode_projection_query_run(&descriptor, limits, &mut credits).unwrap();
        assert_eq!(
            decode_projection_query_run(&encoded.bytes, limits, &mut credits).unwrap(),
            descriptor
        );
    }
}
