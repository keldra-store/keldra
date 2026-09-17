//! Aggregate journal-retention proof from baseline-ready format-v1 roots.
//!
//! The source journal is the sole indexing checkpoint.  A logical definition
//! never owns a cursor: this task advances one conservative checkpoint per
//! source only after every physical family has a durable baseline-ready
//! activation and live partition currents through that cursor. Directory and
//! activation objects are authoritative; the in-process catalog merely tells
//! us which physical recipes currently require proof.

use std::sync::Arc;
use std::time::Duration;

use keldra_consensus::NodeId;
use keldra_index::v1::{
    IndexingMemoryCredits, IndexingMemoryStage, ProjectionFamilyPartitionDirectory,
};
use keldra_store::{DerivedConsumerCheckpoint, DerivedConsumerKind};
use tonic::Status;

use crate::derived_consumer::DerivedCheckpointPublisher;

use super::catalog::IndexCatalog;
use super::catalog::PhysicalCatalogRecipe;
use super::events::{IndexEventJournal, IndexSourceCursor};
use super::v1_publication::V1ProjectionPublisher;

#[path = "v1_retention_atomic.rs"]
mod atomic;

#[cfg(test)]
#[path = "v1_retention_readiness_tests.rs"]
mod readiness_tests;

const RETENTION_SAFETY_INTERVAL: Duration = Duration::from_secs(30);
const RETENTION_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Owns the disposable local task that emits format-v1 journal-retention
/// proofs.  Dropping the runtime stops the task; no local state is durable.
pub(crate) struct V1IndexRetentionTask {
    task: tokio::task::JoinHandle<()>,
}

impl V1IndexRetentionTask {
    pub(crate) fn start(
        local_node: NodeId,
        catalog: IndexCatalog,
        journal: Arc<IndexEventJournal>,
        checkpoints: DerivedCheckpointPublisher,
        projections: V1ProjectionPublisher,
        credits: IndexingMemoryCredits,
    ) -> Self {
        let task = tokio::spawn(async move {
            let mut catalog_changes = catalog.subscribe();
            let mut projection_changes = projections.subscribe();
            let mut interval = tokio::time::interval(RETENTION_SAFETY_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last_reconciled = None;
            loop {
                let forced = tokio::select! {
                    _ = interval.tick() => false,
                    _ = catalog_changes.recv() => true,
                    _ = projection_changes.recv() => true,
                };
                if let Err(error) = advance_once(
                    local_node,
                    &catalog,
                    &journal,
                    &checkpoints,
                    &projections,
                    &credits,
                    &mut last_reconciled,
                    forced,
                )
                .await
                {
                    tracing::warn!(%error, "v1 index retention proof will retry");
                    tokio::time::sleep(RETENTION_RETRY_INTERVAL).await;
                }
            }
        });
        Self { task }
    }
}

impl Drop for V1IndexRetentionTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn advance_once(
    local_node: NodeId,
    catalog: &IndexCatalog,
    journal: &IndexEventJournal,
    checkpoints: &DerivedCheckpointPublisher,
    projections: &V1ProjectionPublisher,
    credits: &IndexingMemoryCredits,
    last_reconciled: &mut Option<(
        Arc<super::catalog::CompletedCatalogReplay>,
        super::events::IndexBarrier,
    )>,
    forced: bool,
) -> Result<(), Status> {
    let barrier = journal
        .capture_barrier()
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let Some(catalog_snapshot) = catalog.retention_snapshot()? else {
        // Old durable catalog checkpoints do not prove that this process has
        // reloaded its ordinary definitions into the transient inventory.
        return Ok(());
    };
    if !forced
        && last_reconciled.as_ref().is_some_and(|(catalog, target)| {
            catalog.generation == catalog_snapshot.generation
                && catalog.barrier == catalog_snapshot.barrier
                && target == &barrier
        })
    {
        return Ok(());
    }
    let input = (catalog_snapshot.clone(), barrier.clone());
    let recipes = catalog_snapshot.physical.recipes.as_ref();
    for cursor in barrier.sources.values().copied() {
        let Some(catalog_next) =
            catalog_checkpoint_limit(&catalog_snapshot, cursor, barrier.fence)?
        else {
            return Ok(());
        };
        let catalog_cursor = IndexSourceCursor {
            next_offset: catalog_next,
            ..cursor
        };
        let next_offset = if recipes.is_empty() {
            // This process completed the baseline and all-source replay at
            // the cut paired with this exact empty inventory snapshot.
            catalog_next
        } else if let Some(next_offset) =
            family_coverage(catalog_cursor, recipes, projections).await?
        {
            next_offset
        } else {
            // Publishing no proof is conservative.  It is required while a
            // new physical family is still backfilling or awaiting activation.
            return Ok(());
        };
        let Some(next_offset) = atomic::cap_before_uncovered_finalizer(
            journal,
            &barrier,
            cursor,
            next_offset,
            recipes,
            projections,
            credits,
        )
        .await?
        else {
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
    *last_reconciled = Some(input);
    Ok(())
}

fn catalog_checkpoint_limit(
    snapshot: &super::catalog::CompletedCatalogReplay,
    cursor: IndexSourceCursor,
    fence: keldra_store::PlacementLogId,
) -> Result<Option<u64>, Status> {
    if snapshot.barrier.fence != fence {
        return Ok(None);
    }
    let Some(checkpoint) = snapshot
        .barrier
        .sources
        .get(&NodeId(u64::from(cursor.source.node_id)))
        .filter(|checkpoint| checkpoint.source == cursor.source)
    else {
        return Ok(None);
    };
    Ok(Some(checkpoint.next_offset.min(cursor.next_offset)))
}

/// Return the first uncovered source position across all active families.
/// `None` means a family is not query-visible yet, so retention must stay put.
async fn family_coverage(
    cursor: IndexSourceCursor,
    recipes: &[Arc<PhysicalCatalogRecipe>],
    projections: &V1ProjectionPublisher,
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
                "v1 activation does not prove the live physical catalog generation",
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
        // A disposable scan cannot justify pruning beyond restart authority.
        // The producer durably publishes safe external skipped cuts in Current
        // before retention may cover them; artifact-only suffixes stay retained.
        covered_through = covered_through.min(family_next);
    }
    Ok(Some(covered_through))
}

async fn live_directory_coverage_for_source(
    cursor: IndexSourceCursor,
    recipe: &PhysicalCatalogRecipe,
    directory: &ProjectionFamilyPartitionDirectory,
    projections: &V1ProjectionPublisher,
) -> Result<Option<u64>, Status> {
    Ok(
        live_directory_cut_for_source(cursor, recipe, directory, projections)
            .await?
            .map(|cut| cut.0),
    )
}

async fn live_directory_cut_for_source(
    cursor: IndexSourceCursor,
    recipe: &PhysicalCatalogRecipe,
    directory: &ProjectionFamilyPartitionDirectory,
    projections: &V1ProjectionPublisher,
) -> Result<Option<(u64, u64)>, Status> {
    directory.validate().map_err(index_status)?;
    let source_node = u64::from(cursor.source.node_id);
    let source_entries = directory.entries.iter().filter(|entry| {
        entry.partition.source_node == source_node
            && entry.partition.source_epoch == cursor.source.source_epoch
    });
    if source_entries.clone().next().is_none() {
        return Err(Status::data_loss(
            "v1 family directory has no partition for an ACTIVE source incarnation",
        ));
    }
    let mut next = u64::MAX;
    let mut atomic = u64::MAX;
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
                    "v1 successor directory coverage is absent from its current generation",
                ));
            }
        }
        next = next.min(current.current.next_offset);
        atomic = atomic.min(current.current.through_atomic_position);
    }
    if next == 0 || next > cursor.next_offset {
        return Err(Status::data_loss(
            "v1 live partition coverage is outside the captured source barrier",
        ));
    }
    Ok(Some((next, atomic)))
}

fn index_status(error: keldra_index::IndexError) -> Status {
    Status::data_loss(format!("invalid v1 retention evidence: {error}"))
}
