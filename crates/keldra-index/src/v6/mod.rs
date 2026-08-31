//! Format-v6 partition-owned, memory-first projection primitives.
//!
//! Format v6 separates the exact current document head from independently
//! reusable membership and field-recipe state. These types are deliberately
//! storage-neutral; publication and generation fencing remain runtime duties.

mod buffer;
mod codec;
mod directory;
mod generation;
mod pack;
mod paths;
mod pipeline;
mod projected_state;
mod publication;
mod query_blocks;
mod query_compaction;
mod query_credits;
mod query_doc_values;
mod query_executor;
mod query_gate;
mod query_prepare;
#[cfg(test)]
mod query_run;
mod query_stream;
mod read;
mod stream;
mod typed_document;

pub use directory::{
    CatalogBaseline, ProjectionCatalogActivation, ProjectionFamilyPartitionDirectory,
    ProjectionPartitionDirectoryEntry, ProjectionPartitionLifecycle,
    decode_projection_catalog_activation, decode_projection_family_directory,
    encode_projection_catalog_activation, encode_projection_family_directory,
};
pub use generation::{
    ComponentIdentity, ComponentRoot, LogicalFieldBinding, LogicalProjectionBinding,
    MAX_QUERY_RECIPE_CATALOG_PROOFS, MAX_RETAINED_CATALOG_GENERATIONS_PER_RECIPE,
    ProjectionCatalogTransition, ProjectionCurrent, ProjectionGeneration,
    ProjectionGenerationReference, ProjectionPartitionIdentity, ProjectionQueryStreamRoot,
    QueryRecipeCatalogProof,
};
pub use pack::{
    ChargedProjectionDeltaPacks, PackedComponentDelta, ProjectionPackCredits,
    SealedProjectionDeltaPack, pack_component_deltas,
};
pub use paths::{
    ProjectionArtifactKind, ProjectionArtifactPath, ProjectionCatalogPath,
    ProjectionCatalogPathKind, parse_projection_artifact_path, parse_projection_catalog_path,
    projection_artifact_routing_id, projection_catalog_activation_path,
    projection_catalog_routing_id, projection_component_page_path, projection_current_path,
    projection_family_directory_path, projection_generation_path, projection_pack_path,
    projection_query_run_pack_path, projection_query_run_stream_page_path, projection_routing_id,
    projection_stream_page_path,
};
pub use pipeline::{
    ChargedPreparedProjectionBatch, ChargedSealedPartitionProjection, IndexingMemoryCredits,
    IndexingMemoryLimits, IndexingMemoryPermit, IndexingMemoryStage, MemoryAdmission,
    PartitionProjectionAccumulator, PartitionProjectionCheckpoint, PreparedProjectionBatch,
    PreparedProjectionBatchError, PreparedProjectionBatchReservation, PreparedProjectionRow,
    ProjectionBatchAdmission, SealedPartitionProjection,
};
pub use projected_state::{
    CanonicalRecipeState, DocumentHead, ObjectIdentity, ProjectedDocumentDelta,
    ProjectedDocumentState, RecipeDelta, RecipeIdentity, StableDocumentKey,
    inherit_projection_preserving_versions,
};
pub use publication::{
    PreparedAtomicProjectionGeneration, PreparedProjectionGeneration,
    prepare_atomic_projection_catalog_transition, prepare_atomic_projection_generation,
    prepare_projection_generation,
};
pub use query_blocks::{
    DEFAULT_QUERY_BLOCK_BYTES, EncodedProjectionQueryRun, EncodedQueryBlock,
    PreparedQueryFieldDelta, PreparedQueryTermDelta, ProjectionQueryRunDescriptor,
    QueryBlockCursor, QueryBlockDescriptor, QueryBlockKind, QueryBlockLimits, QueryBlockRecord,
    QueryBlockRecordRef, QueryDocValue, QueryPoint, QueryPositions, QueryPosting,
    QueryPostingShard, QueryTermEntry, decode_point, decode_positions, decode_posting,
    decode_projection_query_run, decode_term_entry, encode_point, encode_positions, encode_posting,
    encode_projection_query_run, encode_query_block, encode_term_entry, merge_query_block_records,
    prepare_typed_json_field_delta, seek_exact_term, visit_live_gates, visit_live_postings,
    visit_live_range_points, visit_prefix_terms,
};
pub use query_compaction::{ChargedQueryRunCompaction, compact_encoded_query_runs};
pub use query_credits::{QueryBlockCredits, QueryMemoryPermit};
pub use query_doc_values::{decode_doc_value, encode_doc_value};
pub use query_executor::{
    AuthorizedQueryCandidate, MAX_QUERY_CANDIDATE_ADMISSION_BATCH, PinnedPartitionQueryRoot,
    QueryAdmissionCandidate, QueryAdmissionContext, QueryArtifactKind, QueryArtifactLoad,
    QueryArtifactLoader, QueryCandidateAdmission, QueryCommonCut, QueryExecutionLimits,
    QueryFieldBinding, QueryLoadEvidence, QueryRootCutProof, TypedJsonQueryRequest,
    TypedJsonQueryResult, execute_typed_json_query,
};
pub use query_gate::{
    MAX_QUERY_DOCUMENT_PATH_BYTES, QueryDocumentGate, decode_document_gate, encode_document_gate,
};
pub use query_prepare::{
    ChargedProjectionQueryRunArtifacts, PreparedQueryMembershipDelta, PreparedQueryMutationBatch,
    PreparedQueryRecipeDelta, ProjectionQueryRunArtifacts, prepare_projection_query_run,
};
pub use query_stream::{
    EncodedQueryRunPage, PreparedQueryRunAppend, PreparedQueryRunSplice, QUERY_RUN_PAGE_FANOUT,
    QueryRunChild, QueryRunCompactionLimits, QueryRunCompactionPlan, QueryRunPage,
    QueryRunReference, append_query_run_path_copy, decode_query_run_page, encode_query_run_page,
    find_query_run_by_hash, select_query_run_compaction, splice_compacted_query_runs,
    visit_query_runs_newest,
};
pub use read::decode_component_records_in_pack;
pub use stream::{
    COMPONENT_STREAM_DIRECTORY_FANOUT, ComponentCompactionLimits, ComponentCompactionPlan,
    ComponentRecordLookup, ComponentSegmentDescriptor, ComponentStreamAppend,
    ComponentStreamDirectory, ComponentStreamReverseCursor, ComponentStreamReverseStep,
    ComponentStreamRoot, EncodedComponentStreamPage, TombstoneCompactionPolicy,
    append_component_delta, append_component_stream, build_component_stream,
    compact_component_runs, component_stream_child_hashes, decode_component_stream,
    lookup_component_record_in_pack, resolve_component_record, select_component_compaction,
    splice_compacted_component_runs,
};
pub use typed_document::{
    PreparedTypedJsonDocument, TypedJsonDocumentInput, TypedJsonSelectedField,
    prepare_typed_json_document,
};

pub const INDEX_FORMAT_VERSION: u16 = 6;
pub use buffer::{
    ComponentDeltaRecord, DecodedComponentDelta, ProjectionMutationBuffer, SealedComponentDelta,
    decode_component_delta, decode_component_delta_segment, decode_document_head,
    decode_source_records, encode_document_head,
};
pub use codec::{
    COMPONENT_DIRECTORY_FANOUT, ComponentDirectory, EncodedComponentDirectoryPage,
    EncodedProjectionGeneration, MAX_INHERITED_PROJECTION_PARTITIONS, MAX_LOGICAL_BINDING_FIELDS,
    ProjectionGenerationHeader, build_component_directory, component_directory_child_hashes,
    decode_component_directory, decode_current_projection_generation_header,
    decode_logical_projection_binding, decode_projection_current, decode_projection_generation,
    decode_projection_generation_header, empty_component_directory_hash,
    encode_logical_projection_binding, encode_projection_current, encode_projection_generation,
    resolve_component_root,
};
