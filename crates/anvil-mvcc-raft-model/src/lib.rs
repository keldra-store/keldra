//! Executable model of the cluster-local MVCC-under-Raft protocol.
//!
//! The model deliberately contains logical keys, immutable transaction bundles,
//! Raft certification order, durability evidence, application watermarks, and
//! repair/GC state. It contains no roots, physical CoreMeta generations, owner
//! fences, or product row bodies.
//!
//! `bundle_holders` intentionally abstracts both the immutable transaction
//! bundle and the durable byte/shard representation required by its durability
//! level. This model checks acknowledgement and reconstruction thresholds;
//! framing and erasure arithmetic belong to lower-level tests.

mod model;

pub use model::*;

#[cfg(test)]
mod tests;
