//! Bounded, content-addressed query blocks for v1 projection mini-runs.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use bytes::Bytes;

use crate::IndexError;
use crate::typed_json::{
    Cardinality, FieldCapabilities, FieldSchema, ScalarValue, TypedJsonFieldState,
    analyze_typed_json_text, decode_scalar_sort_key, encode_scalar_sort_key,
    validate_canonical_field_state,
};

use super::{
    ArtifactPackLocator, ArtifactPackTable, ProjectionPartitionIdentity, QueryBlockCredits,
    QueryDocumentGate, RecipeIdentity, StableDocumentKey, decode_document_gate,
};

#[path = "segment_documents.rs"]
mod segment_documents;
pub use segment_documents::{
    SegmentDocumentId, SegmentDocumentTable, SegmentLiveDocuments, SegmentMemoryLease,
};
#[path = "segment_postings.rs"]
mod segment_postings;
pub use segment_postings::{DenseSegmentPoint, DenseSegmentPosting, SegmentPostingCursor};

#[cfg(test)]
use super::encode_document_gate;

const BLOCK_MAGIC: &[u8; 8] = b"K1QBLK01";
const BLOCK_FORMAT: u16 = 1;
const RUN_MAGIC: &[u8; 8] = b"K1QRUN01";
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
            _ => Err(IndexError::InvalidFormat("v1 query block kind")),
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
    /// Maximum bytes accepted for a run descriptor.
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
            // A full 65,536-operation publication can legitimately describe
            // more than 8 MiB of query blocks. Keep the descriptor bounded to
            // one eighth of the default 512 MiB query-memory budget instead
            // of rejecting a producer-built run that the query runtime can
            // load within its bounded lease.
            maximum_run_descriptor_bytes: 64 * 1024 * 1024,
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
                "v1 query block limits are invalid".into(),
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
    pub locator: ArtifactPackLocator,
    pub pack_table: std::sync::Arc<ArtifactPackTable>,
    pub documents: std::sync::Arc<SegmentDocumentTable>,
}

impl QueryBlockDescriptor {
    pub fn pack_reference(&self) -> Result<&super::ArtifactPackReference, IndexError> {
        self.locator.resolve(&self.pack_table)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedQueryBlock {
    pub descriptor: LogicalQueryBlockDescriptor,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalQueryBlockDescriptor {
    pub kind: QueryBlockKind,
    pub recipe: RecipeIdentity,
    pub minimum_key: Vec<u8>,
    pub maximum_key: Vec<u8>,
    pub hash: [u8; 32],
    pub encoded_bytes: u64,
    pub records: u32,
    pub documents: std::sync::Arc<SegmentDocumentTable>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryBlockRecordRef<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    /// Resolved identity for a compact point key (which exposes scalar bytes).
    pub document: Option<StableDocumentKey>,
}

impl QueryBlockRecordRef<'_> {
    pub fn canonical_key(&self) -> Vec<u8> {
        canonical_record_key(*self)
    }

    fn compare_key(&self, key: &[u8]) -> std::cmp::Ordering {
        if let Some(document) = self.document {
            self.key
                .iter()
                .chain(document.bytes().iter())
                .cmp(key.iter())
        } else {
            self.key.cmp(key)
        }
    }
}

#[derive(Clone, Debug)]
struct DecodedQueryBlockRecord {
    key: DecodedRecordKey,
    value: Range<usize>,
}

#[derive(Clone, Debug)]
enum DecodedRecordKey {
    Bytes(Range<usize>),
    Document(u32),
    Point(Range<usize>, u32),
}

/// Disposable, bounded lookup view over an immutable encoded query block.
///
/// The encoded bytes remain authoritative. This view records only byte ranges,
/// so repeated queries do not reparse record lengths or copy keys and values.
pub struct SegmentReader {
    bytes: Bytes,
    records: Vec<DecodedQueryBlockRecord>,
    kind: QueryBlockKind,
    recipe: RecipeIdentity,
    hash: [u8; 32],
    record_count: u32,
    documents: std::sync::Arc<SegmentDocumentTable>,
    memory_lease: std::sync::OnceLock<std::sync::Arc<dyn Send + Sync + std::fmt::Debug>>,
}

pub type DecodedQueryBlock = SegmentReader;

impl DecodedQueryBlock {
    pub(crate) fn from_verified_content(
        descriptor: &QueryBlockDescriptor,
        bytes: Bytes,
        limits: QueryBlockLimits,
        credits: &mut QueryBlockCredits,
    ) -> Result<Self, IndexError> {
        let index_bytes = (descriptor.records as usize)
            .checked_mul(std::mem::size_of::<DecodedQueryBlockRecord>())
            .ok_or(IndexError::OffsetOverflow)?;
        credits.reserve(index_bytes)?;
        let mut records = Vec::with_capacity(descriptor.records as usize);
        let base = bytes.as_ptr() as usize;
        let mut loaded = false;
        let parsed = (|| {
            let mut cursor = QueryBlockCursor::from_verified_content(
                descriptor,
                bytes.as_ref(),
                limits,
                credits,
            )?;
            loaded = true;
            while let Some(record) = cursor.next()? {
                let key = if descriptor.kind == QueryBlockKind::TermDictionary {
                    let start = (record.key.as_ptr() as usize)
                        .checked_sub(base)
                        .ok_or(IndexError::Integrity)?;
                    DecodedRecordKey::Bytes(start..start + record.key.len())
                } else if descriptor.kind == QueryBlockKind::Point {
                    let start = (record.key.as_ptr() as usize)
                        .checked_sub(base)
                        .ok_or(IndexError::Integrity)?;
                    DecodedRecordKey::Point(
                        start..start + record.key.len(),
                        descriptor
                            .documents
                            .id(record.document.ok_or(IndexError::Integrity)?)?
                            .0,
                    )
                } else {
                    DecodedRecordKey::Document(descriptor.documents.id(stable_key(record.key)?)?.0)
                };
                let value_start = (record.value.as_ptr() as usize)
                    .checked_sub(base)
                    .ok_or(IndexError::Integrity)?;
                records.push(DecodedQueryBlockRecord {
                    key,
                    value: value_start..value_start + record.value.len(),
                });
            }
            Ok(())
        })();
        // The cache owns the resulting allocation; query credits cover only
        // construction and validation of that bounded disposable view.
        let release_loaded = if loaded {
            credits.release_loaded_block(bytes.len())
        } else {
            Ok(())
        };
        let release_index = credits.release(index_bytes);
        parsed?;
        release_loaded?;
        release_index?;
        Ok(Self {
            bytes,
            records,
            kind: descriptor.kind,
            recipe: descriptor.recipe,
            hash: descriptor.hash,
            record_count: descriptor.records,
            documents: descriptor.documents.clone(),
            memory_lease: std::sync::OnceLock::new(),
        })
    }

    pub fn encoded_bytes(&self) -> usize {
        self.bytes.len()
    }

    pub fn attach_memory_lease(
        &self,
        lease: std::sync::Arc<dyn Send + Sync + std::fmt::Debug>,
    ) -> bool {
        self.memory_lease.set(lease).is_ok()
    }
    pub fn has_memory_lease(&self) -> bool {
        self.memory_lease.get().is_some()
    }

    pub fn documents(&self) -> &std::sync::Arc<SegmentDocumentTable> {
        &self.documents
    }

    pub fn live_documents_for_candidates(
        &self,
        candidates: impl IntoIterator<Item = SegmentDocumentId>,
        mut current: impl FnMut(StableDocumentKey, u64) -> bool,
    ) -> Result<SegmentLiveDocuments, IndexError> {
        let mut live = SegmentLiveDocuments::none_live(self.documents.clone());
        for id in candidates {
            let document = self.documents.document(id)?;
            let version = self.documents.material_version(id)?;
            if version == 0 {
                return Err(IndexError::IntegrityViolation(
                    "cannot bind liveness to an unversioned segment document".into(),
                ));
            }
            if current(document, version) {
                live.set_live(id)?;
            }
        }
        Ok(live)
    }

    pub fn resident_index_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(
            self.records
                .capacity()
                .saturating_mul(std::mem::size_of::<DecodedQueryBlockRecord>()),
        )
    }

    pub fn matches_descriptor(&self, descriptor: &QueryBlockDescriptor) -> bool {
        self.kind == descriptor.kind
            && self.recipe == descriptor.recipe
            && self.hash == descriptor.hash
            && self.record_count == descriptor.records
            && self.bytes.len() as u64 == descriptor.encoded_bytes
            && self.documents.identity() == descriptor.documents.identity()
    }

    pub fn matches_content(&self, hash: [u8; 32], encoded_bytes: usize) -> bool {
        self.hash == hash && self.bytes.len() == encoded_bytes
    }

    pub fn records(&self) -> impl Iterator<Item = QueryBlockRecordRef<'_>> {
        self.records_from_index(0)
    }

    pub fn records_from(
        &self,
        minimum_key: &[u8],
    ) -> impl Iterator<Item = QueryBlockRecordRef<'_>> {
        let first = self
            .records
            .partition_point(|record| self.record_ref(record).compare_key(minimum_key).is_lt());
        self.records_from_index(first)
    }

    fn records_from_index(&self, first: usize) -> impl Iterator<Item = QueryBlockRecordRef<'_>> {
        self.records[first..]
            .iter()
            .map(|record| self.record_ref(record))
    }

    fn record_ref(&self, record: &DecodedQueryBlockRecord) -> QueryBlockRecordRef<'_> {
        let (key, document) = match &record.key {
            DecodedRecordKey::Bytes(range) => (&self.bytes[range.clone()], None),
            DecodedRecordKey::Document(id) => (
                self.documents
                    .key(*id)
                    .expect("validated local document ID"),
                None,
            ),
            DecodedRecordKey::Point(range, id) => (
                &self.bytes[range.clone()],
                Some(
                    self.documents
                        .document(SegmentDocumentId(*id))
                        .expect("validated local document ID"),
                ),
            ),
        };
        QueryBlockRecordRef {
            key,
            value: &self.bytes[record.value.clone()],
            document,
        }
    }
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

#[derive(Debug, Eq, PartialEq)]
pub struct ProjectionQueryRunDescriptor {
    pub partition: ProjectionPartitionIdentity,
    pub physical_catalog_generation: [u8; 32],
    pub sequence: u64,
    pub source_start_offset: u64,
    pub next_offset: u64,
    pub through_atomic_position: u64,
    pub pack_table: std::sync::Arc<ArtifactPackTable>,
    pub blocks: Vec<QueryBlockDescriptor>,
    pub memory_lease: SegmentMemoryLease,
}

impl Clone for ProjectionQueryRunDescriptor {
    fn clone(&self) -> Self {
        Self {
            partition: self.partition,
            physical_catalog_generation: self.physical_catalog_generation,
            sequence: self.sequence,
            source_start_offset: self.source_start_offset,
            next_offset: self.next_offset,
            through_atomic_position: self.through_atomic_position,
            pack_table: self.pack_table.clone(),
            blocks: self.blocks.clone(),
            memory_lease: SegmentMemoryLease::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedProjectionQueryRun {
    pub hash: [u8; 32],
    pub bytes: Vec<u8>,
}

/// One deliberately empty position between adjacent values of a multi-valued
/// text field. Array order is the canonical token-stream order and duplicate
/// values are significant, matching Lucene's ordered repeated-field model.
pub(super) const TEXT_VALUE_POSITION_GAP: u32 = 1;

/// Prepare a complete field replacement from the new selected values only.
/// Previous immutable postings are retired by document liveness, never by
/// reconstructing old fields or subtracting individual terms.
pub fn prepare_typed_json_field_delta(
    field: &FieldSchema,
    document: StableDocumentKey,
    material_source_version: u64,
    current: Option<&TypedJsonFieldState>,
    credits: &mut QueryBlockCredits,
) -> Result<PreparedQueryFieldDelta, IndexError> {
    field.validate()?;
    if material_source_version == 0 {
        return Err(IndexError::InvalidDefinition(
            "v1 query field version is zero".into(),
        ));
    }
    let current = normalized_state(field, current)?;
    credits.reserve(estimate_query_delta_bytes(current)?)?;
    let values = state_values(current);
    let mut terms = Vec::new();
    if field.capabilities.contains(FieldCapabilities::FULL_TEXT) {
        for (term, positions) in analyzed_terms(current)? {
            terms.push(PreparedQueryTermDelta {
                term: ScalarValue::String(term),
                document,
                material_source_version,
                live: true,
                positions,
            });
        }
    } else if field.capabilities.contains(FieldCapabilities::EXACT)
        || field.capabilities.contains(FieldCapabilities::PREFIX)
    {
        terms.extend(values.iter().cloned().map(|term| PreparedQueryTermDelta {
            term,
            document,
            material_source_version,
            live: true,
            positions: Vec::new(),
        }));
    }
    let points = if field.capabilities.contains(FieldCapabilities::RANGE) {
        values
            .into_iter()
            .map(|value| QueryPoint {
                value,
                document,
                material_source_version,
                live: true,
            })
            .collect()
    } else {
        Vec::new()
    };
    let has_values = field.capabilities.contains(FieldCapabilities::ORDER)
        || field.capabilities.contains(FieldCapabilities::FACET)
        || field.capabilities.contains(FieldCapabilities::AGGREGATE);
    let doc_value = if has_values {
        Some(QueryDocValue {
            document,
            material_source_version,
            value: doc_values(field, current)?,
        })
    } else {
        None
    };
    Ok(PreparedQueryFieldDelta {
        presence: QueryDocumentGate {
            document,
            material_source_version,
            current_source_version: material_source_version,
            live: current.is_some(),
            source_path: None,
            canonical_source_path: None,
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
    validate_canonical_field_state(field, state)?;
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

pub(super) fn analyzed_terms(
    state: Option<&TypedJsonFieldState>,
) -> Result<BTreeMap<String, Vec<u32>>, IndexError> {
    let mut terms = BTreeMap::new();
    let mut position = 0u32;
    let values = state.map_or(&[][..], |state| state.values.as_slice());
    for (value_index, value) in values.iter().enumerate() {
        let ScalarValue::String(value) = value else {
            return Err(IndexError::InvalidDefinition(
                "text field selected a non-string scalar".into(),
            ));
        };
        for term in analyze_typed_json_text(value) {
            terms.entry(term).or_insert_with(Vec::new).push(position);
            position = position.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        }
        if value_index + 1 < values.len() {
            position = position
                .checked_add(TEXT_VALUE_POSITION_GAP)
                .ok_or(IndexError::OffsetOverflow)?;
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

fn estimate_query_delta_bytes(current: Option<&TypedJsonFieldState>) -> Result<usize, IndexError> {
    current
        .into_iter()
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
            "v1 term dictionary entry is invalid".into(),
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
        return Err(IndexError::InvalidFormat("v1 term dictionary record"));
    }
    let count = read_u32(record.value, 0)? as usize;
    if count == 0
        || count > limits.maximum_records
        || record.value.len() != 4usize.saturating_add(count.saturating_mul(100))
    {
        return Err(IndexError::InvalidFormat("v1 term dictionary record"));
    }
    let mut posting_shards = Vec::with_capacity(count);
    let mut hashes = BTreeSet::new();
    let mut previous_maximum = None;
    for bytes in record.value[4..].chunks_exact(100) {
        let shard = QueryPostingShard {
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
        };
        if shard.posting_block_hash == [0; 32]
            || shard.posting_records == 0
            || shard.minimum_document > shard.maximum_document
            || previous_maximum.is_some_and(|maximum| maximum >= shard.minimum_document)
            || !hashes.insert(shard.posting_block_hash)
        {
            return Err(IndexError::InvalidFormat("v1 term dictionary record"));
        }
        previous_maximum = Some(shard.maximum_document);
        posting_shards.push(shard);
    }
    Ok(QueryTermEntry {
        term,
        posting_shards,
    })
}

pub fn encode_posting(posting: QueryPosting) -> Result<QueryBlockRecord, IndexError> {
    if posting.material_source_version == 0
        || posting.positions > 0 && posting.position_block_hash.is_none()
        || posting.positions == 0 && posting.position_block_hash.is_some()
        || !posting.live && (posting.positions != 0 || posting.position_block_hash.is_some())
    {
        return Err(IndexError::InvalidDefinition(
            "v1 posting is invalid".into(),
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
                "v1 posting is invalid".into(),
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
    let dense = decode_dense_posting_value(SegmentDocumentId(0), record.value)?;
    Ok(QueryPosting {
        document,
        material_source_version: dense.material_source_version,
        live: dense.live,
        position_block_hash: dense.position_block_hash,
        positions: dense.positions,
    })
}

fn decode_dense_posting_value(
    document: SegmentDocumentId,
    value: &[u8],
) -> Result<DenseSegmentPosting, IndexError> {
    let material_source_version = read_u64(value, 0)?;
    if material_source_version == 0 {
        return Err(IndexError::InvalidFormat("v1 posting version"));
    }
    let live = match value.get(8) {
        Some(0) => false,
        Some(1) => true,
        _ => return Err(IndexError::InvalidFormat("v1 posting liveness")),
    };
    match value.get(9) {
        Some(0) if value.len() == 10 => Ok(DenseSegmentPosting {
            document,
            material_source_version,
            live,
            position_block_hash: None,
            positions: 0,
        }),
        Some(1) if live && value.len() == 46 => {
            let hash = value[10..42]
                .try_into()
                .map_err(|_| IndexError::Integrity)?;
            let positions =
                u32::from_be_bytes(value[42..].try_into().map_err(|_| IndexError::Integrity)?);
            if hash == [0; 32] || positions == 0 {
                return Err(IndexError::InvalidFormat("v1 posting positions"));
            }
            Ok(DenseSegmentPosting {
                document,
                material_source_version,
                live,
                position_block_hash: Some(hash),
                positions,
            })
        }
        _ => Err(IndexError::InvalidFormat("v1 posting record")),
    }
}

pub fn encode_point(point: &QueryPoint) -> Result<QueryBlockRecord, IndexError> {
    if point.material_source_version == 0 {
        return Err(IndexError::InvalidDefinition(
            "v1 point version is zero".into(),
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
    let document = if let Some(document) = record.document {
        if used != record.key.len() {
            return Err(IndexError::InvalidFormat("v1 point key"));
        }
        document
    } else {
        stable_key(
            record
                .key
                .get(used..)
                .ok_or(IndexError::InvalidFormat("v1 point key"))?,
        )?
    };
    if record.value.len() != 9 {
        return Err(IndexError::InvalidFormat("v1 point record"));
    }
    let material_source_version = read_u64(record.value, 0)?;
    let live = match record.value[8] {
        0 => false,
        1 => true,
        _ => return Err(IndexError::InvalidFormat("v1 point liveness")),
    };
    if material_source_version == 0 {
        return Err(IndexError::InvalidFormat("v1 point version"));
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
            "v1 positions are not canonical".into(),
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
        return Err(IndexError::InvalidFormat("v1 positions record"));
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
    let documents = std::sync::Arc::new(document_table_for_records(kind, records)?);
    encode_query_block_with_documents(kind, recipe, records, documents, limits, credits)
}

pub(super) fn document_table_for_records(
    kind: QueryBlockKind,
    records: &[QueryBlockRecord],
) -> Result<SegmentDocumentTable, IndexError> {
    if kind == QueryBlockKind::TermDictionary {
        return SegmentDocumentTable::new([]);
    }
    SegmentDocumentTable::new_with_versions(
        records
            .iter()
            .map(|record| {
                Ok((
                    document_key_for_record(kind, &record.key)?,
                    if kind == QueryBlockKind::Position {
                        0
                    } else {
                        read_u64(&record.value, 0)?
                    },
                ))
            })
            .collect::<Result<Vec<_>, IndexError>>()?,
    )
}

pub(super) fn document_key_for_record(
    kind: QueryBlockKind,
    key: &[u8],
) -> Result<StableDocumentKey, IndexError> {
    let key = if kind == QueryBlockKind::Point {
        let (_, used) = decode_scalar_sort_key(key)?;
        key.get(used..).ok_or(IndexError::Integrity)?
    } else {
        key
    };
    stable_key(key)
}

pub(super) fn encode_query_block_with_documents(
    kind: QueryBlockKind,
    recipe: RecipeIdentity,
    records: &[QueryBlockRecord],
    documents: std::sync::Arc<SegmentDocumentTable>,
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
    let mut required: usize = 8 + 2 + 1 + 32 + 32 + 4 + 4 + 4;
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
                "v1 query block records are not bounded canonical order".into(),
            ));
        }
        required = required
            .checked_add(8)
            .and_then(|bytes| {
                bytes.checked_add(if kind == QueryBlockKind::TermDictionary {
                    record.key.len()
                } else {
                    record.key.len().checked_sub(28)?
                })
            })
            .and_then(|bytes| bytes.checked_add(record.value.len()))
            .ok_or(IndexError::OffsetOverflow)?;
        previous = Some(&record.key);
    }
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
    bytes.extend_from_slice(&documents.identity());
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
        if kind == QueryBlockKind::TermDictionary {
            put_bytes(&mut bytes, &record.key)?;
        } else {
            let split = record
                .key
                .len()
                .checked_sub(32)
                .ok_or(IndexError::Integrity)?;
            if kind != QueryBlockKind::Point && split != 0 {
                return Err(IndexError::Integrity);
            }
            let id = documents.id(stable_key(&record.key[split..])?)?;
            if kind != QueryBlockKind::Position
                && documents.material_version(id)? != read_u64(&record.value, 0)?
            {
                return Err(IndexError::IntegrityViolation(
                    "segment document material version does not match field row".into(),
                ));
            }
            put_u32(&mut bytes, split + 4)?;
            bytes.extend_from_slice(&record.key[..split]);
            bytes.extend_from_slice(&id.0.to_be_bytes());
        }
        put_bytes(&mut bytes, &record.value)?;
    }
    if bytes.len() != required {
        return Err(IndexError::Integrity);
    }
    let descriptor = LogicalQueryBlockDescriptor {
        kind,
        recipe,
        minimum_key: records.first().expect("nonempty").key.clone(),
        maximum_key: records.last().expect("nonempty").key.clone(),
        hash: *crate::profiled_blake3_hash!(&bytes).as_bytes(),
        encoded_bytes: bytes.len() as u64,
        records: u32::try_from(records.len()).map_err(|_| IndexError::OffsetOverflow)?,
        documents,
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
        if *crate::profiled_blake3_hash!(bytes).as_bytes() != descriptor.hash {
            return Err(IndexError::Integrity);
        }
        Self::from_verified_content(descriptor, bytes, limits, credits)
    }

    /// Open bytes whose complete content hash was already verified by the
    /// artifact-loading boundary. Structural and descriptor checks still run.
    pub(crate) fn from_verified_content(
        descriptor: &'a QueryBlockDescriptor,
        bytes: &'a [u8],
        limits: QueryBlockLimits,
        credits: &mut QueryBlockCredits,
    ) -> Result<Self, IndexError> {
        let limits = limits.validate()?;
        if bytes.len() > limits.maximum_block_bytes
            || bytes.len() as u64 != descriptor.encoded_bytes
        {
            return Err(IndexError::Integrity);
        }
        let payload = bytes;
        let mut input = BlockInput::new(payload);
        input.expect(BLOCK_MAGIC)?;
        if input.u16()? != BLOCK_FORMAT
            || QueryBlockKind::decode(input.byte()?)? != descriptor.kind
            || input.array_32()? != descriptor.recipe.bytes()
            || input.array_32()? != descriptor.documents.identity()
        {
            return Err(IndexError::InvalidFormat("v1 query block header"));
        }
        let records = input.u32()?;
        if records == 0
            || records != descriptor.records
            || records as usize > limits.maximum_records
        {
            return Err(IndexError::InvalidFormat("v1 query block record count"));
        }
        let restart_interval = input.u32()?;
        let restart_count = input.u32()?;
        if restart_interval == 0 || restart_interval as usize > limits.maximum_records {
            return Err(IndexError::InvalidFormat("v1 query block restart table"));
        }
        let expected_restarts = records.div_ceil(restart_interval);
        if restart_count != expected_restarts {
            return Err(IndexError::InvalidFormat("v1 query block restart table"));
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
                return Err(IndexError::InvalidFormat("v1 query block restart offsets"));
            }
            prior = Some(offset);
        }
        credits.reserve_loaded_block(bytes.len(), limits.maximum_loaded_blocks)?;
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
                return Err(IndexError::InvalidFormat("v1 query block trailing bytes"));
            }
            return Ok(None);
        }
        let mut input = BlockInput {
            bytes: self.bytes,
            offset: self.offset,
        };
        let raw_key = input.bytes()?;
        let value = input.bytes()?;
        if raw_key.is_empty() || self.previous.is_some_and(|previous| previous >= raw_key) {
            return Err(IndexError::UnsortedRecords);
        }
        self.previous = Some(raw_key);
        self.offset = input.offset;
        self.remaining -= 1;
        self.record_index = self
            .record_index
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        if self.remaining == 0 && self.offset != self.bytes.len() {
            return Err(IndexError::InvalidFormat("v1 query block trailing bytes"));
        }
        Ok(Some(self.resolve_record(raw_key, value)?))
    }

    pub fn seek_to(&mut self, key: &[u8]) -> Result<Option<QueryBlockRecordRef<'a>>, IndexError> {
        // Repeated seeks in query execution are naturally monotonic: candidate
        // document keys arrive in stable order. Keep walking from the current
        // record in that case instead of binary-searching the restart table and
        // decoding the same restart interval again for every key.
        if self.previous.is_some_and(|previous| {
            self.compare_key(previous, key)
                .is_ok_and(|order| order.is_lt())
        }) {
            while let Some(record) = self.next()? {
                if record.compare_key(key).is_ge() {
                    return Ok(Some(record));
                }
            }
            return Ok(None);
        }
        let mut lower = 0u32;
        let mut upper = self.restart_count;
        while lower < upper {
            let midpoint = lower + (upper - lower) / 2;
            let offset = restart_offset(self.bytes, self.restart_offsets_start, midpoint)?;
            let record = record_at(self.bytes, self.records_start, offset as usize)?;
            if self.compare_key(record.key, key)?.is_le() {
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
            if record.compare_key(key).is_ge() {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    pub const fn descriptor(&self) -> &QueryBlockDescriptor {
        self.descriptor
    }

    fn resolve_record(
        &self,
        raw_key: &'a [u8],
        value: &'a [u8],
    ) -> Result<QueryBlockRecordRef<'a>, IndexError> {
        if self.descriptor.kind == QueryBlockKind::TermDictionary {
            return Ok(QueryBlockRecordRef {
                key: raw_key,
                value,
                document: None,
            });
        }
        let split = raw_key.len().checked_sub(4).ok_or(IndexError::Integrity)?;
        let id = read_u32(raw_key, split)?;
        let key = self.descriptor.documents.key(id)?;
        if !value.is_empty()
            && self.descriptor.kind != QueryBlockKind::Position
            && self
                .descriptor
                .documents
                .material_version(SegmentDocumentId(id))?
                != read_u64(value, 0)?
        {
            return Err(IndexError::Integrity);
        }
        if self.descriptor.kind == QueryBlockKind::Point {
            let scalar = &raw_key[..split];
            let (_, used) = decode_scalar_sort_key(scalar)?;
            if used != scalar.len() {
                return Err(IndexError::Integrity);
            }
            Ok(QueryBlockRecordRef {
                key: scalar,
                value,
                document: Some(stable_key(key)?),
            })
        } else {
            if split != 0 {
                return Err(IndexError::Integrity);
            }
            Ok(QueryBlockRecordRef {
                key,
                value,
                document: None,
            })
        }
    }

    fn compare_key(&self, raw_key: &'a [u8], key: &[u8]) -> Result<std::cmp::Ordering, IndexError> {
        let record = self.resolve_record(raw_key, &[])?;
        Ok(record.compare_key(key))
    }
}

fn canonical_record_key(record: QueryBlockRecordRef<'_>) -> Vec<u8> {
    let mut key = record.key.to_vec();
    if let Some(document) = record.document {
        key.extend_from_slice(&document.bytes());
    }
    key
}

#[path = "query_run_codec.rs"]
mod query_run_codec;
pub use query_run_codec::{decode_projection_query_run, encode_projection_query_run};

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
            "v1 query block merge lanes disagree".into(),
        ));
    }
    let mut current = inputs
        .iter_mut()
        .map(QueryBlockCursor::next)
        .collect::<Result<Vec<_>, _>>()?;
    let mut pending = BTreeMap::<Vec<u8>, Vec<usize>>::new();
    for (index, record) in current.iter().enumerate() {
        if let Some(record) = record {
            pending
                .entry(canonical_record_key(*record))
                .or_default()
                .push(index);
        }
    }
    while let Some((_key, lanes)) = pending.pop_first() {
        let winner = *lanes.first().ok_or(IndexError::Integrity)?;
        output(current[winner].expect("winner exists"))?;
        for index in lanes {
            current[index] = inputs[index].next()?;
            if let Some(record) = current[index] {
                pending
                    .entry(canonical_record_key(record))
                    .or_default()
                    .push(index);
            }
        }
    }
    Ok(())
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
            "v1 live gate query requires gate or presence blocks".into(),
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
        .map_err(|_| IndexError::InvalidFormat("v1 stable document key"))?;
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
    Ok(QueryBlockRecordRef {
        key,
        value,
        document: None,
    })
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
            return Err(IndexError::InvalidFormat("v1 query block magic"));
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
            Err(IndexError::InvalidFormat("v1 query block trailing bytes"))
        }
    }
}

#[cfg(test)]
#[path = "query_blocks_tests.rs"]
mod tests;
