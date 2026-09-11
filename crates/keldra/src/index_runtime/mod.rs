//! Shared distributed-index runtime primitives.
//!
//! Index formats remain independent of cluster placement, source-journal
//! collection, and local byte materialisation. This module owns those common
//! mechanics; it never interprets index contents.

pub(crate) mod catalog;
pub(crate) mod coordination;
pub(crate) mod cpu;
pub(crate) mod date;
pub(crate) mod distributed_query;
pub(crate) mod events;
pub(crate) mod hot_ingress;
pub(crate) mod json_projection;
pub(crate) mod placement;
pub(crate) mod publication;
pub(crate) mod query_budget;
pub(crate) mod runtime;
pub(crate) mod scanner;
pub(crate) mod source;
pub(crate) mod typed_json_schema;
pub(crate) mod v1_atomic_dispatch;
mod v1_artifact_cache;
pub(crate) mod v1_backfill;
pub(crate) mod v1_catalog_lifecycle;
pub(crate) mod v1_compaction;
pub(crate) mod v1_consumer;
pub(crate) mod v1_extractor;
pub(crate) mod v1_journal_dispatch;
pub(crate) mod v1_mutation_window;
pub(crate) mod v1_publication;
pub(crate) mod v1_query_compile;
pub(crate) mod v1_query_runtime;
pub(crate) mod v1_removal_quiescence;
pub(crate) mod v1_retention;
pub(crate) mod v1_telemetry;
pub(crate) mod working_memory;
