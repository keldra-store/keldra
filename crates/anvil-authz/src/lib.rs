//! A small, storage-neutral Zanzibar-style authorization kernel.
//!
//! Each [`Authorization`] value evaluates one realm's already-authoritative
//! schema and active tuple set. Realm isolation is structural: namespaces,
//! objects, and usersets are local to that evaluator and never contain an
//! encoded realm prefix.
//!
//! Permissions are bounded unions of inherited relations and tuple-to-userset
//! rewrites. There are deliberately no caveats, exclusion/intersection rules,
//! implicit path-prefix inheritance, clocks, network calls, or hidden I/O.

#![forbid(unsafe_code)]

mod evaluator;
mod model;
mod schema;

pub use evaluator::Authorization;
pub use model::{
    ANONYMOUS_SUBJECT_ID, AllowedSubject, AuthorizationCheck, AuthorizationError,
    AuthorizationLimits, DEFAULT_REALM_ID, ExactPath, MAX_NAMESPACE_BYTES, MAX_PATH_BYTES,
    NamespaceDefinition, ObjectId, ObjectRef, PUBLIC_SUBJECT_ID, PUBLIC_SUBJECT_NAMESPACE, RealmId,
    RelationDefinition, RelationKind, RewriteRule, SYSTEM_REALM_ID, Schema, Tuple, TupleSubject,
    UsersetRef,
};

pub type Result<T> = std::result::Result<T, AuthorizationError>;

#[cfg(test)]
mod tests;
