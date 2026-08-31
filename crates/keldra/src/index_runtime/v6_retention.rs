//! Aggregate journal-retention proof from baseline-ready format-v6 roots.
//!
//! The source journal is the sole indexing checkpoint.  A logical definition
//! never owns a cursor: this task advances one conservative checkpoint per
//! source only after every physical family has a durable baseline-ready
//! activation and live partition currents through that cursor. Directory and
//! activation objects are authoritative; the in-process catalog merely tells
//! us which physical recipes currently require proof.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use keldra_consensus::NodeId;
use keldra_index::v6::ProjectionFamilyPartitionDirectory;
use keldra_store::{
    DefinitionConsumerKind, DerivedConsumerCheckpoint, DerivedConsumerKind, SourceId, Store,
};
use tonic::Status;

use crate::derived_consumer::DerivedCheckpointPublisher;

use super::catalog::IndexCatalog;
use super::catalog::PhysicalCatalogRecipe;
use super::events::{IndexBarrier, IndexEventJournal, IndexSourceCursor};
use super::v6_publication::V6ProjectionPublisher;

const RETENTION_POLL_INTERVAL: Duration = Duration::from_millis(100);
const RETENTION_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Owns the disposable local task that emits format-v6 journal-retention
/// proofs.  Dropping the runtime stops the task; no local state is durable.
pub(crate) struct V6IndexRetentionTask {
    task: tokio::task::JoinHandle<()>,
}

impl V6IndexRetentionTask {
    pub(crate) fn start(
        local_node: NodeId,
        store: Store,
        catalog: IndexCatalog,
        journal: Arc<IndexEventJournal>,
        checkpoints: DerivedCheckpointPublisher,
        projections: V6ProjectionPublisher,
    ) -> Self {
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(RETENTION_POLL_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(error) = advance_once(
                    local_node,
                    &store,
                    &catalog,
                    &journal,
                    &checkpoints,
                    &projections,
                )
                .await
                {
                    tracing::warn!(%error, "v6 index retention proof will retry");
                    tokio::time::sleep(RETENTION_RETRY_INTERVAL).await;
                }
            }
        });
        Self { task }
    }
}

impl Drop for V6IndexRetentionTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn advance_once(
    local_node: NodeId,
    store: &Store,
    catalog: &IndexCatalog,
    journal: &IndexEventJournal,
    checkpoints: &DerivedCheckpointPublisher,
    projections: &V6ProjectionPublisher,
) -> Result<(), Status> {
    let barrier = journal
        .capture_barrier()
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let (_, _, _, recipes, _) = catalog.snapshot()?;
    let recipes = recipes
        .into_iter()
        .map(|recipe| (recipe.family, recipe))
        .collect::<BTreeMap<_, _>>();
    let recipes = recipes.into_values().collect::<Vec<_>>();
    for cursor in barrier.sources.values().copied() {
        let Some(catalog_next) = catalog_checkpoint_limit(store, cursor, barrier.fence).await?
        else {
            return Ok(());
        };
        let catalog_cursor = IndexSourceCursor {
            next_offset: catalog_next,
            ..cursor
        };
        let next_offset = if recipes.is_empty() {
            // The durable catalog checkpoint proves the all-source baseline
            // inventory and replay cut even when that inventory is empty.
            catalog_next
        } else if let Some(next_offset) =
            family_coverage(catalog_cursor, &recipes, projections).await?
        {
            next_offset
        } else {
            // Publishing no proof is conservative.  It is required while a
            // new physical family is still backfilling or awaiting activation.
            return Ok(());
        };
        let consumer_node_id = u16::try_from(local_node.0).map_err(|_| {
            Status::data_loss("local node cannot be represented by a derived checkpoint")
        })?;
        checkpoints
            .publish(DerivedConsumerCheckpoint {
                consumer_kind: DerivedConsumerKind::Index,
                source_id: cursor.source,
                consumer_node_id,
                next_offset,
                observed_fence: barrier.fence,
            })
            .await?;
        // Retention publication is a proof, not a source-row processing
        // event. The source consumer increments row and byte counters from
        // its exact journal page evidence before publishing this proof.
    }
    Ok(())
}

async fn catalog_checkpoint_limit(
    store: &Store,
    cursor: IndexSourceCursor,
    fence: keldra_store::PlacementLogId,
) -> Result<Option<u64>, Status> {
    let store = store.clone();
    let checkpoint = tokio::task::spawn_blocking(move || {
        store.definition_checkpoint(
            DefinitionConsumerKind::V6IndexCatalog,
            cursor.source.node_id,
        )
    })
    .await
    .map_err(|error| Status::internal(format!("v6 catalog checkpoint task failed: {error}")))?
    .map_err(|error| Status::unavailable(error.to_string()))?;
    let Some(checkpoint) = checkpoint else {
        return Ok(None);
    };
    if checkpoint.consumer_kind != DefinitionConsumerKind::V6IndexCatalog
        || checkpoint.source_id != cursor.source
        || checkpoint.observed_fence != fence
        || checkpoint.next_offset > cursor.next_offset
    {
        return Err(Status::data_loss(
            "v6 catalog checkpoint does not prove the captured source barrier",
        ));
    }
    Ok(Some(checkpoint.next_offset))
}

/// Return the first uncovered source position across all active families.
/// `None` means a family is not query-visible yet, so retention must stay put.
async fn family_coverage(
    cursor: IndexSourceCursor,
    recipes: &[PhysicalCatalogRecipe],
    projections: &V6ProjectionPublisher,
) -> Result<Option<u64>, Status> {
    let mut covered_through = cursor.next_offset;
    for recipe in recipes {
        let Some((activation, _)) = projections
            .load_activation(
                &recipe.storage_tenant,
                &recipe.bucket,
                recipe.family.tenant_id,
                recipe.family.bucket_id,
                recipe.family.family_id,
                recipe.physical_generation,
            )
            .await?
        else {
            return Ok(None);
        };
        activation.validate().map_err(index_status)?;
        if activation.physical_catalog_generation != recipe.physical_generation {
            return Err(Status::data_loss(
                "v6 activation does not prove the live physical catalog generation",
            ));
        }
        let Some((directory, _)) = projections
            .load_family_directory(
                &recipe.storage_tenant,
                &recipe.bucket,
                recipe.family.tenant_id,
                recipe.family.bucket_id,
                recipe.family.family_id,
            )
            .await?
        else {
            return Ok(None);
        };
        let Some(family_next) =
            live_directory_coverage_for_source(cursor, recipe, &directory, projections).await?
        else {
            return Ok(None);
        };
        covered_through = covered_through.min(family_next);
    }
    Ok(Some(covered_through))
}

async fn live_directory_coverage_for_source(
    cursor: IndexSourceCursor,
    recipe: &PhysicalCatalogRecipe,
    directory: &ProjectionFamilyPartitionDirectory,
    projections: &V6ProjectionPublisher,
) -> Result<Option<u64>, Status> {
    directory.validate().map_err(index_status)?;
    let source_node = u64::from(cursor.source.node_id);
    let source_entries = directory
        .entries
        .iter()
        .filter(|entry| {
            entry.partition.source_node == source_node
                && entry.partition.source_epoch == cursor.source.source_epoch
        })
        .collect::<Vec<_>>();
    if source_entries.is_empty() {
        return Err(Status::data_loss(
            "v6 family directory has no partition for an ACTIVE source incarnation",
        ));
    }
    let mut next = u64::MAX;
    for entry in source_entries {
        let Some(current) = projections
            .load_current(
                &recipe.storage_tenant,
                &recipe.bucket,
                recipe.family.tenant_id,
                recipe.family.bucket_id,
                entry.partition,
            )
            .await?
        else {
            return Ok(None);
        };
        if current.current.physical_catalog_generation != recipe.physical_generation {
            return Ok(None);
        }
        for predecessor in &entry.covered_predecessors {
            if current
                .generation
                .inherited_partitions
                .binary_search(predecessor)
                .is_err()
            {
                return Err(Status::data_loss(
                    "v6 successor directory coverage is absent from its current generation",
                ));
            }
        }
        next = next.min(current.current.next_offset);
    }
    if next == 0 || next > cursor.next_offset {
        return Err(Status::data_loss(
            "v6 live partition coverage is outside the captured source barrier",
        ));
    }
    Ok(Some(next))
}

fn index_status(error: keldra_index::IndexError) -> Status {
    Status::data_loss(format!("invalid v6 retention evidence: {error}"))
}
