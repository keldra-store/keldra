//! Shared distributed-index runtime primitives.
//!
//! Index formats remain independent of cluster placement, source-journal
//! collection, and local byte materialisation. This module owns those common
//! mechanics; it never interprets index contents.

pub(crate) mod cache;
pub(crate) mod catalog;
pub(crate) mod directory;
pub(crate) mod discovery;
pub(crate) mod distributed_query;
pub(crate) mod engine;
pub(crate) mod events;
pub(crate) mod generation;
pub(crate) mod local_query;
pub(crate) mod manager;
pub(crate) mod placement;
pub(crate) mod publication;
pub(crate) mod publisher;
pub(crate) mod query;
pub(crate) mod retention;
pub(crate) mod runtime;
pub(crate) mod scanner;
pub(crate) mod snapshot;
