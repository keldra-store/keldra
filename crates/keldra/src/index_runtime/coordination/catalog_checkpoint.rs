//! Durable all-source progress for the v6 catalog feed.

use keldra_store::{DefinitionCheckpoint, DefinitionConsumerKind, Store};
use tonic::Status;

use super::super::events::IndexBarrier;
use super::{internal_status, join_status};

/// Persist only after the caller has completed its baseline inventory and
/// contiguous replay through this exact barrier.
pub(super) async fn persist(
    store: &Store,
    barrier: &IndexBarrier,
    replayed_rows: u64,
    replayed_bytes: u64,
) -> Result<(), Status> {
    for source in barrier.sources.values().copied() {
        let checkpoint = DefinitionCheckpoint {
            consumer_kind: DefinitionConsumerKind::V6IndexCatalog,
            source_id: source.source,
            next_offset: source.next_offset,
            observed_fence: barrier.fence,
        };
        let store = store.clone();
        tokio::task::spawn_blocking(move || {
            store.apply_definition_assignment_page(&[], &checkpoint)
        })
        .await
        .map_err(join_status)?
        .map_err(internal_status)?;
    }
    super::super::v6_telemetry::global().record_catalog_checkpoint(replayed_rows, replayed_bytes);
    Ok(())
}
