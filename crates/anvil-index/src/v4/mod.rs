//! Native format-v4 immutable segment foundations.
//!
//! Durable bytes use explicit little-endian Anvil codecs and never Rust,
//! `sux`, protobuf, or JSON memory layouts.

mod analyzer;
mod artifact;
pub mod build;
mod codec;
mod columns;
mod executor;
mod identity;
mod io;
mod keys;
mod liveness;
mod locator;
mod model;
mod mutation;
mod points;
mod postings;
mod query;
mod reader;
mod routing;
mod scan;
mod schema;
mod segment_reader;
mod statistics;
mod terms;
mod text;
mod vectors;

#[cfg(test)]
mod decoder_mutation_tests;

pub use analyzer::analyze_unicode_alphanumeric_lowercase;
pub use artifact::{
    ArtifactDescriptor, ArtifactPackReference, ArtifactReference, GeneratedComponent,
    SegmentComponent, SegmentDescriptor, artifact_path, current_path, definition_path,
    manifest_path,
};
pub use codec::{
    COMPONENT_HEADER_BYTES, ComponentHeader, DecodedComponent, decode_component, encode_component,
};
pub use columns::{DOC_VALUES_COMPONENT_CODEC_VERSION, DocValueBlock, DocValueCell, ScalarValue};
pub use executor::{
    MAXIMUM_CANDIDATE_GATE_BATCH, NativeQueryExecutionError, NativeQueryExecutor, NativeQueryLimits,
};
pub use identity::{DocumentIdentity, IdentityBlock, ObjectIdentity};
pub use io::{ArtifactDirectoryRead, LoadedComponent, read_artifact_component};
pub use keys::{
    FIELD_PRESENCE_TERM, TERM_TYPE_BOOLEAN, TERM_TYPE_FIELD_PRESENCE, TERM_TYPE_HASHED_KEYWORD,
    TERM_TYPE_NULL, TERM_TYPE_NUMBER, TERM_TYPE_SIGNED, TERM_TYPE_STRING, TERM_TYPE_TEXT,
    TERM_TYPE_UNSIGNED, canonical_term_key, encode_physical_order_key, scalar_term, text_term,
};
pub use liveness::{LIVE_MASK_BLOCK_DOCS, LiveMask, LiveMaskBlock};
pub use locator::{
    DocIdRange, LocatorEntry, LocatorValue, PathLocatorBlock, merge_locator_entries,
};
pub use model::{
    ComponentKind, DocId, INDEX_ARTIFACT_PACK_BYTES, INDEX_COMPONENT_BYTES, INDEX_DECODE_BYTES,
    INDEX_FORMAT_VERSION, INDEX_GENERATION_SEGMENTS, INDEX_ROUTING_FANOUT, INDEX_ROUTING_HEIGHT,
    INDEX_ROUTING_KEY_BYTES, INDEX_TERM_BYTES, SegmentIdentity, component_ordinal_key,
    decode_component_ordinal_key,
};
pub use mutation::{
    LOCATOR_COMPACTION_FAN_IN, LocatorStreamRoot, compact_locator_roots, locate_path,
    locate_path_values, locate_paths, publish_locator_delta, rewrite_segment_live_mask,
};
pub(crate) use points::POINT_BLOCK_ENTRIES;
pub use points::{
    POINTS_COMPONENT_CODEC_VERSION, PointBlock, PointEntry, PointValue, point_entry_key,
    point_presence_key, point_scalar_key, point_value_key, point_value_range,
};
pub use postings::{
    POSTING_SKIP_INTERVAL, PostingBlock, PostingCodec, PostingCursor, PostingImpact, PostingList,
};
pub use query::{
    AggregateOperation, AggregateRequest, AggregateResult, CandidateGate, CandidateGateEvidence,
    CandidateReference, FacetBucket, FacetRequest, FacetResult, NativeQuery, NativeQueryCursor,
    NativeQueryHit, NativeQueryPage, NativeQueryRequest,
};
pub use reader::{ComponentStream, StreamLeaf, StreamTotals};
pub use routing::{RoutingEntry, RoutingNode};
pub use scan::{
    AuthorizationScope, CandidateIdentity, GenerationSelection, Predicate, PredicateId,
    PredicatePushdown, RangeBound, ScanBatch, ScanCapabilities, ScanColumn, ScanRequest,
    SortCursor, SortValue,
};
pub use schema::{
    Analyzer, Cardinality, Collation, ComponentVersion, FieldCapabilities, FieldComponents,
    FieldId, FieldSchema, FieldType, IndexKind, IndexSemantics, OrderDirection, OrderField, Schema,
    VectorMetric, VectorNormalization,
};
pub use segment_reader::SegmentComponentReader;
pub use statistics::{
    NativeQueryExecutionTier, NativeQueryStatistics, NativeQueryStatisticsRecorder,
};
pub(crate) use terms::TERM_DICTIONARY_TARGET_BYTES;
pub use terms::{PostingReference, TermDictionary, TermEntry};
pub use text::{
    ComponentStatistics, FieldStatistics, NormBlock, PhysicalOrderBounds, PositionEntry,
    PositionsBlock, SegmentStatistics,
};
pub use vectors::VectorBlock;
