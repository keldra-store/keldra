//! Shared distributed-index runtime primitives.
//!
//! Index formats remain independent of cluster placement, source-journal
//! collection, and local byte materialisation. This module owns those common
//! mechanics; it never interprets index contents.

pub(crate) mod budget;
pub(crate) mod cache;
pub(crate) mod catalog;
pub(crate) mod committed_view;
pub(crate) mod coordination;
pub(crate) mod cpu;
pub(crate) mod date;
pub(crate) mod directory;
pub(crate) mod distributed_query;
pub(crate) mod events;
pub(crate) mod json_projection;
pub(crate) mod local_query;
pub(crate) mod manager;
pub(crate) mod placement;
pub(crate) mod publication;
pub(crate) mod publisher;
pub(crate) mod query_budget;
pub(crate) mod query_response;
pub(crate) mod rebuild_root;
pub(crate) mod retention;
pub(crate) mod runtime;
pub(crate) mod scanner;
pub(crate) mod source;
pub(crate) mod telemetry;
pub(crate) mod v4_projection;
pub(crate) mod v4_query;
pub(crate) mod v4_schema;
pub(crate) mod working_memory;
