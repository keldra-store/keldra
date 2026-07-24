#![recursion_limit = "256"]

#[path = "distributed_tests/common.rs"]
mod common;
#[path = "distributed_tests/faults.rs"]
mod faults;
#[path = "distributed_tests/metadata.rs"]
mod metadata;
#[path = "distributed_tests/objects.rs"]
mod objects;
#[path = "distributed_tests/publication_recovery.rs"]
mod publication_recovery;
