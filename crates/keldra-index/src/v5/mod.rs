//! Format-v5 shared-projection primitives.
//!
//! Format v5 separates the exact current document head from independently
//! reusable membership and field-recipe state. These types are deliberately
//! storage-neutral; publication and generation fencing remain runtime duties.

mod buffer;
mod codec;
mod generation;
mod pack;
mod paths;
mod projected_state;
mod projector;
mod publication;
mod stream;

pub use generation::{
    ComponentIdentity, ComponentRoot, LogicalFieldBinding, LogicalProjectionBinding,
    ProjectionBarrier, ProjectionCurrent, ProjectionGeneration,
};
pub use pack::{PackedComponentDelta, SealedProjectionDeltaPack, pack_component_deltas};
pub use paths::{
    ProjectionArtifactKind, ProjectionArtifactPath, parse_projection_artifact_path,
    projection_component_page_path, projection_current_path, projection_generation_path,
    projection_pack_path, projection_routing_id, projection_stream_page_path,
};
pub use projected_state::{
    CanonicalRecipeState, DocumentHead, ProjectedDocumentDelta, ProjectedDocumentState,
    RecipeDelta, RecipeIdentity, StableDocumentKey, inherit_projection_preserving_versions,
};
pub use projector::{
    decode_canonical_field_state, merge_projected_document_states, projected_document_states,
    query_cache_mutations,
};
pub use publication::{PreparedProjectionGeneration, prepare_projection_generation};
pub use stream::{
    COMPONENT_STREAM_DIRECTORY_FANOUT, ComponentRecordLookup, ComponentSegmentDescriptor,
    ComponentStreamAppend, ComponentStreamDirectory, ComponentStreamRoot,
    EncodedComponentStreamPage, append_component_delta, append_component_stream,
    build_component_stream, compact_component_stream, component_stream_child_hashes,
    decode_component_stream, lookup_component_record_in_pack, resolve_component_record,
};

pub const INDEX_FORMAT_VERSION: u16 = 5;
pub use buffer::{
    ComponentDeltaRecord, DecodedComponentDelta, ProjectionMutationBuffer, SealedComponentDelta,
    decode_component_delta, decode_component_delta_segment, decode_document_head,
    decode_source_records, encode_document_head,
};
pub use codec::{
    COMPONENT_DIRECTORY_FANOUT, ComponentDirectory, EncodedComponentDirectoryPage,
    EncodedProjectionGeneration, MAX_LOGICAL_BINDING_FIELDS, MAX_PROJECTION_SOURCES,
    ProjectionGenerationHeader, build_component_directory, component_directory_child_hashes,
    decode_component_directory, decode_logical_projection_binding, decode_projected_document_state,
    decode_projection_current, decode_projection_generation, decode_projection_generation_header,
    empty_component_directory_hash, encode_logical_projection_binding,
    encode_projected_document_state, encode_projection_current, encode_projection_generation,
    resolve_component_root,
};
