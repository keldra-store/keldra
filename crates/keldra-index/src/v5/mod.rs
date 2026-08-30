//! Format-v5 shared-projection primitives.
//!
//! Format v5 separates the exact current document head from independently
//! reusable membership and field-recipe state. These types are deliberately
//! storage-neutral; publication and generation fencing remain runtime duties.

mod generation;
mod projected_state;
mod projector;

pub use generation::{
    ComponentIdentity, ComponentRoot, LogicalFieldBinding, LogicalProjectionBinding,
    ProjectionBarrier, ProjectionGeneration,
};
pub use projected_state::{
    CanonicalRecipeState, DocumentHead, ProjectedDocumentDelta, ProjectedDocumentState,
    RecipeDelta, RecipeIdentity, StableDocumentKey,
};
pub use projector::projected_document_states;

pub const INDEX_FORMAT_VERSION: u16 = 5;
