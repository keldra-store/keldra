//! Format-v5 shared-projection primitives.
//!
//! Format v5 separates the exact current document head from independently
//! reusable membership and field-recipe state. These types are deliberately
//! storage-neutral; publication and generation fencing remain runtime duties.

mod buffer;
mod codec;
mod generation;
mod pack;
mod projected_state;
mod projector;
mod stream;

pub use generation::{
    ComponentIdentity, ComponentRoot, LogicalFieldBinding, LogicalProjectionBinding,
    ProjectionBarrier, ProjectionGeneration,
};
pub use pack::{PackedComponentDelta, SealedProjectionDeltaPack, pack_component_deltas};
pub use projected_state::{
    CanonicalRecipeState, DocumentHead, ProjectedDocumentDelta, ProjectedDocumentState,
    RecipeDelta, RecipeIdentity, StableDocumentKey,
};
pub use projector::projected_document_states;
pub use stream::{
    COMPONENT_STREAM_DIRECTORY_FANOUT, ComponentSegmentDescriptor, ComponentStreamDirectory,
    EncodedComponentStreamPage, append_component_delta, build_component_stream,
    compact_component_stream, decode_component_stream, resolve_component_record,
};

pub const INDEX_FORMAT_VERSION: u16 = 5;
pub use buffer::{
    ComponentDeltaRecord, DecodedComponentDelta, ProjectionMutationBuffer, SealedComponentDelta,
    decode_component_delta, decode_component_delta_segment,
};
pub use codec::{
    COMPONENT_DIRECTORY_FANOUT, ComponentDirectory, EncodedComponentDirectoryPage,
    EncodedProjectionGeneration, MAX_LOGICAL_BINDING_FIELDS, MAX_PROJECTION_SOURCES,
    build_component_directory, decode_component_directory, decode_logical_projection_binding,
    decode_projected_document_state, decode_projection_generation,
    encode_logical_projection_binding, encode_projected_document_state,
    encode_projection_generation,
};
