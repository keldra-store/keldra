//! Bounded deterministic programs for atomic changes to a small object set.
//!
//! This crate is intentionally not a transaction manager. A definition is
//! written as an ordinary immutable object below `_keldra/programs/`; the
//! nominated executor loads its pinned address and hash for invocation. It
//! locks the finite expanded path set, evaluates a small JSON/opaque-value
//! language, then returns one bounded bundle for durable preparation and atomic
//! publication.
//!
//! The language cannot perform networking, read a clock, generate randomness,
//! loop, discover paths from payloads, or execute arbitrary client code. Durable
//! command receipts make a repeated command deterministic.

mod engine;
mod error;
mod json;
mod locks;
mod model;
mod template;

pub use engine::{AtomicProgramEngine, ExecutionLease, StateReader};
pub use error::EngineError;
pub use locks::{LocalLockGuard, LocalLockManager};
pub use model::*;
pub use template::{PathTemplate, StringTemplate};

#[cfg(test)]
mod tests;
