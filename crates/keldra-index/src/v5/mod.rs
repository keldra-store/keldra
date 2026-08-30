//! Format-v5 shared-projection primitives.
//!
//! Format v5 separates the exact current document head from independently
//! reusable membership and field-recipe state. These types are deliberately
//! storage-neutral; publication and generation fencing remain runtime duties.

mod projected_state;

pub use projected_state::{
    CanonicalRecipeState, DocumentHead, ProjectedDocumentDelta, ProjectedDocumentState,
    RecipeDelta, RecipeIdentity, StableDocumentKey,
};

pub const INDEX_FORMAT_VERSION: u16 = 5;
