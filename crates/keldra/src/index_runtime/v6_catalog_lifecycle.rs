//! Family directory and activation publication for the v6 physical catalog.
//!
//! Source owners publish only their partition `current` roots.  This task is
//! the separate low-cardinality family authority: it makes the live partition
//! set durable and activates a catalog generation only after every required
//! root is present.  It never advances an indexing cursor.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use keldra_index::v6::{
    CatalogBaseline, ProjectionCatalogActivation, ProjectionFamilyPartitionDirectory,
    ProjectionPartitionDirectoryEntry, ProjectionPartitionIdentity, ProjectionPartitionLifecycle,
    QueryRecipeCatalogProof, RecipeIdentity, projection_catalog_routing_id,
};
use tonic::Status;

use super::catalog::{IndexCatalog, PhysicalCatalogRecipe};
use super::events::{IndexBarrier, IndexEventJournal};
use super::publication::IndexArtifactRouter;
use super::v6_publication::V6ProjectionPublisher;

const LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const LIFECYCLE_RETRY_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) struct V6CatalogLifecycleTask {
    task: tokio::task::JoinHandle<()>,
}

impl V6CatalogLifecycleTask {
    pub(crate) fn start(
        catalog: IndexCatalog,
        journal: Arc<IndexEventJournal>,
        artifacts: IndexArtifactRouter,
        projections: V6ProjectionPublisher,
    ) -> Self {
        let baselines = Arc::new(Mutex::new(BTreeMap::new()));
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(LIFECYCLE_POLL_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(error) =
                    reconcile_once(&catalog, &journal, &artifacts, &projections, &baselines).await
                {
                    tracing::warn!(%error, "v6 projection catalog lifecycle will retry");
                    tokio::time::sleep(LIFECYCLE_RETRY_INTERVAL).await;
                }
            }
        });
        Self { task }
    }
}

impl Drop for V6CatalogLifecycleTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn reconcile_once(
    catalog: &IndexCatalog,
    journal: &IndexEventJournal,
    artifacts: &IndexArtifactRouter,
    projections: &V6ProjectionPublisher,
    baselines: &Arc<Mutex<BTreeMap<([u8; 32], [u8; 32]), IndexBarrier>>>,
) -> Result<(), Status> {
    let live_barrier = journal
        .capture_barrier()
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let (_, definition_catalog_hash, _, recipes, _) = catalog.snapshot()?;
    // One physical family can serve many logical definitions. Reconcile its
    // directory and activation exactly once per poll.
    let recipes = recipes
        .into_iter()
        .map(|recipe| (recipe.family, recipe))
        .collect::<BTreeMap<_, _>>();
    for recipe in recipes.into_values() {
        let baseline = pinned_baseline(&recipe, journal, baselines).await?;
        reconcile_family(
            &live_barrier,
            &baseline,
            definition_catalog_hash,
            &recipe,
            artifacts,
            projections,
        )
        .await?;
    }
    Ok(())
}

async fn reconcile_family(
    live_barrier: &IndexBarrier,
    baseline: &IndexBarrier,
    definition_catalog_hash: [u8; 32],
    recipe: &PhysicalCatalogRecipe,
    artifacts: &IndexArtifactRouter,
    projections: &V6ProjectionPublisher,
) -> Result<(), Status> {
    let routing = projection_catalog_routing_id(recipe.family.family_id, recipe.family.family_id)
        .map_err(index_status)?;
    if !artifacts.is_local_catalog_authority(
        recipe.family.tenant_id,
        recipe.family.bucket_id,
        routing,
    )? {
        return Ok(());
    }
    let expected = partitions_for_barrier(recipe, live_barrier, artifacts)?;
    let loaded = projections
        .load_family_directory(
            &recipe.storage_tenant,
            &recipe.bucket,
            recipe.family.tenant_id,
            recipe.family.bucket_id,
            recipe.family.family_id,
        )
        .await?;
    let (directory, directory_version) = match loaded {
        Some((current, version)) => {
            let next = extend_directory(current.clone(), expected)?;
            let changed = next != current;
            (next, Some((version, changed)))
        }
        None => (
            ProjectionFamilyPartitionDirectory {
                family_id: recipe.family.family_id,
                revision: 1,
                entries: expected,
            },
            None,
        ),
    };
    let (directory, directory_version) = if let Some((version, changed)) = directory_version {
        if changed {
            let published_version = projections
                .publish_family_directory(
                    &recipe.storage_tenant,
                    &recipe.bucket,
                    recipe.family.tenant_id,
                    recipe.family.bucket_id,
                    &directory,
                    Some(version),
                )
                .await?;
            super::v6_telemetry::V6PipelineTelemetry::add(
                &super::v6_telemetry::global().catalog_directory_publications,
                1,
            );
            (directory, published_version)
        } else {
            (directory, version)
        }
    } else {
        let version = projections
            .publish_family_directory(
                &recipe.storage_tenant,
                &recipe.bucket,
                recipe.family.tenant_id,
                recipe.family.bucket_id,
                &directory,
                None,
            )
            .await?;
        super::v6_telemetry::V6PipelineTelemetry::add(
            &super::v6_telemetry::global().catalog_directory_publications,
            1,
        );
        (directory, version)
    };
    let (directory, _) =
        complete_covered_handoffs(recipe, directory, directory_version, projections).await?;
    activate_if_complete(
        baseline,
        definition_catalog_hash,
        recipe,
        &directory,
        projections,
    )
    .await
}

async fn pinned_baseline(
    recipe: &PhysicalCatalogRecipe,
    journal: &IndexEventJournal,
    baselines: &Arc<Mutex<BTreeMap<([u8; 32], [u8; 32]), IndexBarrier>>>,
) -> Result<IndexBarrier, Status> {
    let key = (recipe.family.family_id, recipe.physical_generation);
    if let Some(existing) = baselines
        .lock()
        .map_err(|_| Status::internal("v6 catalog baseline registry is poisoned"))?
        .get(&key)
        .cloned()
    {
        return Ok(existing);
    }
    let captured = journal
        .capture_barrier()
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let mut baselines = baselines
        .lock()
        .map_err(|_| Status::internal("v6 catalog baseline registry is poisoned"))?;
    Ok(baselines.entry(key).or_insert(captured).clone())
}

fn partitions_for_barrier(
    recipe: &PhysicalCatalogRecipe,
    barrier: &IndexBarrier,
    artifacts: &IndexArtifactRouter,
) -> Result<Vec<ProjectionPartitionDirectoryEntry>, Status> {
    barrier
        .sources
        .iter()
        .map(|(node, cursor)| {
            let (producer, fence) = artifacts.source_projection_producer(
                recipe.family.tenant_id,
                recipe.family.bucket_id,
                cursor.source,
            )?;
            if fence != barrier.fence {
                return Err(Status::unavailable(
                    "v6 source producer assignment changed during captured barrier",
                ));
            }
            Ok(ProjectionPartitionDirectoryEntry {
                partition: ProjectionPartitionIdentity::new(
                    recipe.family.family_id,
                    node.0,
                    cursor.source.source_epoch,
                    producer.0,
                    barrier.fence.term,
                    barrier.fence.index,
                )
                .map_err(index_status)?,
                lifecycle: ProjectionPartitionLifecycle::Active,
                covered_predecessors: Vec::new(),
            })
        })
        .collect()
}

fn extend_directory(
    mut directory: ProjectionFamilyPartitionDirectory,
    expected: Vec<ProjectionPartitionDirectoryEntry>,
) -> Result<ProjectionFamilyPartitionDirectory, Status> {
    let mut changed = false;
    for entry in expected {
        if directory
            .entries
            .iter()
            .any(|current| current.partition == entry.partition)
        {
            continue;
        }
        let predecessor = directory.entries.iter().position(|current| {
            current.partition.source_node == entry.partition.source_node
                && current.partition.source_epoch == entry.partition.source_epoch
        });
        if let Some(predecessor) = predecessor {
            match directory.entries[predecessor].lifecycle {
                ProjectionPartitionLifecycle::Active => {
                    // A producer or fence change is a successor handoff even
                    // if the selected producer node remains identical.
                    directory.entries[predecessor].lifecycle =
                        ProjectionPartitionLifecycle::Retiring {
                            successor: entry.partition,
                        };
                    directory.entries.push(entry);
                    changed = true;
                }
                ProjectionPartitionLifecycle::Retiring { successor }
                    if successor == entry.partition => {}
                ProjectionPartitionLifecycle::Retiring { .. } => {
                    return Err(Status::failed_precondition(
                        "v6 source incarnation has an unfinished different handoff",
                    ));
                }
            }
        } else {
            directory.entries.push(entry);
            changed = true;
        }
    }
    if changed {
        directory.entries.sort_by_key(|entry| entry.partition);
        directory.revision = directory
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("v6 family directory revision overflow"))?;
    }
    directory.validate().map_err(index_status)?;
    Ok(directory)
}

async fn complete_covered_handoffs(
    recipe: &PhysicalCatalogRecipe,
    mut directory: ProjectionFamilyPartitionDirectory,
    mut version: keldra_store::VersionId,
    projections: &V6ProjectionPublisher,
) -> Result<(ProjectionFamilyPartitionDirectory, keldra_store::VersionId), Status> {
    loop {
        let Some((retiring, successor)) =
            directory
                .entries
                .iter()
                .find_map(|entry| match entry.lifecycle {
                    ProjectionPartitionLifecycle::Retiring { successor } => {
                        Some((entry.partition, successor))
                    }
                    ProjectionPartitionLifecycle::Active => None,
                })
        else {
            return Ok((directory, version));
        };
        let Some(predecessor) = projections
            .load_current(
                &recipe.storage_tenant,
                &recipe.bucket,
                recipe.family.tenant_id,
                recipe.family.bucket_id,
                retiring,
            )
            .await?
        else {
            return Ok((directory, version));
        };
        let Some(replacement) = projections
            .load_current(
                &recipe.storage_tenant,
                &recipe.bucket,
                recipe.family.tenant_id,
                recipe.family.bucket_id,
                successor,
            )
            .await?
        else {
            return Ok((directory, version));
        };
        let predecessor = predecessor
            .generation
            .reference(predecessor.current.generation_hash)
            .map_err(index_status)?;
        if replacement
            .generation
            .inherited_partitions
            .binary_search(&predecessor)
            .is_err()
        {
            return Ok((directory, version));
        }
        let successor_entry = directory
            .entries
            .iter_mut()
            .find(|entry| entry.partition == successor)
            .ok_or_else(|| {
                Status::data_loss("v6 retiring partition has no directory successor entry")
            })?;
        if successor_entry
            .covered_predecessors
            .binary_search(&predecessor)
            .is_err()
        {
            successor_entry.covered_predecessors.push(predecessor);
            successor_entry.covered_predecessors.sort();
        }
        directory = directory
            .complete_handoff(&[predecessor])
            .map_err(index_status)?;
        version = projections
            .publish_family_directory(
                &recipe.storage_tenant,
                &recipe.bucket,
                recipe.family.tenant_id,
                recipe.family.bucket_id,
                &directory,
                Some(version),
            )
            .await?;
        super::v6_telemetry::V6PipelineTelemetry::add(
            &super::v6_telemetry::global().catalog_directory_publications,
            1,
        );
    }
}

async fn activate_if_complete(
    barrier: &IndexBarrier,
    definition_catalog_hash: [u8; 32],
    recipe: &PhysicalCatalogRecipe,
    directory: &ProjectionFamilyPartitionDirectory,
    projections: &V6ProjectionPublisher,
) -> Result<(), Status> {
    let baseline = activation_baseline(barrier, directory)?;
    let required_partitions = directory
        .entries
        .iter()
        .map(|entry| entry.partition)
        .collect::<Vec<_>>();
    let current = projections
        .load_activation(
            &recipe.storage_tenant,
            &recipe.bucket,
            recipe.family.tenant_id,
            recipe.family.bucket_id,
            recipe.family.family_id,
            recipe.physical_generation,
        )
        .await?;
    if current.is_some() {
        // Activation is the durable baseline record for a physical catalog
        // generation.  It intentionally survives directory handoffs and
        // process restarts: live currents govern retention and progress, not
        // a replacement activation assembled from a later barrier.
        return Ok(());
    }
    let required_atomic_position = barrier.atomic.finalized_through().unwrap_or(0);
    let mut coverage = Vec::with_capacity(directory.entries.len());
    for entry in &directory.entries {
        let source_baseline = baseline
            .iter()
            .find(|source| {
                source.source_node == entry.partition.source_node
                    && source.source_epoch == entry.partition.source_epoch
            })
            .ok_or_else(|| {
                Status::failed_precondition(
                    "v6 family directory partition is absent from the captured source barrier",
                )
            })?;
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
            return Ok(());
        };
        if current.current.physical_catalog_generation != recipe.physical_generation {
            return Ok(());
        }
        if !current_covers_cut(&current.current, source_baseline, required_atomic_position) {
            // A directory is not query-ready merely because it has a first
            // root. Every required partition must cover this common captured
            // journal and atomic cut.
            return Ok(());
        }
        coverage.push(
            current
                .generation
                .reference(current.current.generation_hash)
                .map_err(index_status)?,
        );
    }
    coverage.sort();
    let mut recipe_catalog_proofs = recipe
        .fields
        .keys()
        .copied()
        .chain(std::iter::once(recipe.membership_recipe))
        .map(|identity| {
            Ok(QueryRecipeCatalogProof {
                recipe: RecipeIdentity::new(identity).map_err(index_status)?,
                accepted_catalog_generations: vec![recipe.physical_generation],
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    recipe_catalog_proofs.sort();
    recipe_catalog_proofs.dedup_by_key(|proof| proof.recipe);
    let activation = ProjectionCatalogActivation {
        family_id: recipe.family.family_id,
        physical_catalog_generation: recipe.physical_generation,
        definition_catalog_hash,
        recipe_catalog_hash: recipe.physical_generation,
        baseline,
        required_partitions,
        activated_coverage: coverage,
        recipe_catalog_proofs,
    };
    activation.validate().map_err(index_status)?;
    projections
        .publish_activation(
            &recipe.storage_tenant,
            &recipe.bucket,
            recipe.family.tenant_id,
            recipe.family.bucket_id,
            &activation,
            current.map(|(_, version)| version),
        )
        .await?;
    super::v6_telemetry::V6PipelineTelemetry::add(
        &super::v6_telemetry::global().catalog_activations,
        1,
    );
    Ok(())
}

fn current_covers_cut(
    current: &keldra_index::v6::ProjectionCurrent,
    baseline: &CatalogBaseline,
    required_atomic_position: u64,
) -> bool {
    current.next_offset >= baseline.next_offset
        && current.through_atomic_position >= required_atomic_position
}

fn activation_baseline(
    barrier: &IndexBarrier,
    directory: &ProjectionFamilyPartitionDirectory,
) -> Result<Vec<CatalogBaseline>, Status> {
    let mut baseline = Vec::with_capacity(directory.entries.len());
    for entry in &directory.entries {
        let cursor = barrier
            .sources
            .values()
            .find(|cursor| {
                u64::from(cursor.source.node_id) == entry.partition.source_node
                    && cursor.source.source_epoch == entry.partition.source_epoch
            })
            .ok_or_else(|| {
                Status::failed_precondition(
                    "v6 family directory contains a partition outside the captured barrier",
                )
            })?;
        baseline.push(CatalogBaseline {
            source_node: entry.partition.source_node,
            source_epoch: entry.partition.source_epoch,
            next_offset: cursor.next_offset,
        });
    }
    baseline.sort();
    baseline.dedup_by_key(|source| (source.source_node, source.source_epoch));
    if baseline.is_empty() {
        return Err(Status::failed_precondition(
            "v6 family directory has no active partition baseline",
        ));
    }
    Ok(baseline)
}

fn index_status(error: keldra_index::IndexError) -> Status {
    Status::data_loss(format!(
        "invalid v6 projection catalog lifecycle evidence: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use keldra_consensus::NodeId;
    use keldra_index::v6::{
        ProjectionCurrent, ProjectionFamilyPartitionDirectory, ProjectionPartitionDirectoryEntry,
        ProjectionPartitionIdentity, ProjectionPartitionLifecycle,
    };
    use keldra_store::{PlacementLogId, SourceId};

    use super::*;

    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([3; 32], 7, [4; 32], 7, 2, 5).unwrap()
    }

    #[test]
    fn activation_baseline_is_the_captured_source_tail_not_a_synthetic_one() {
        let partition = partition();
        let directory = ProjectionFamilyPartitionDirectory {
            family_id: [3; 32],
            revision: 1,
            entries: vec![ProjectionPartitionDirectoryEntry {
                partition,
                lifecycle: ProjectionPartitionLifecycle::Active,
                covered_predecessors: Vec::new(),
            }],
        };
        let barrier = IndexBarrier {
            fence: PlacementLogId { term: 2, index: 5 },
            atomic: super::super::events::AtomicProgramWatermark::new(Some(11), Some(11), 0),
            sources: [(
                NodeId(7),
                super::super::events::IndexSourceCursor {
                    source: SourceId {
                        node_id: 7,
                        source_epoch: [4; 32],
                    },
                    next_offset: 42,
                },
            )]
            .into_iter()
            .collect(),
        };
        assert_eq!(
            activation_baseline(&barrier, &directory).unwrap(),
            vec![CatalogBaseline {
                source_node: 7,
                source_epoch: [4; 32],
                next_offset: 42,
            }]
        );
    }

    #[test]
    fn current_before_the_captured_baseline_cannot_activate() {
        let partition = partition();
        let current = ProjectionCurrent {
            partition,
            physical_catalog_generation: [8; 32],
            generation_hash: [9; 32],
            generation_revision: 1,
            next_offset: 41,
            through_atomic_position: 11,
        };
        let baseline = CatalogBaseline {
            source_node: 7,
            source_epoch: [4; 32],
            next_offset: 42,
        };
        assert!(!current_covers_cut(&current, &baseline, 11));
        assert!(!current_covers_cut(&current, &baseline, 12));
    }

    #[test]
    fn fence_refresh_creates_a_successor_even_when_the_producer_is_unchanged() {
        let predecessor = partition();
        let successor = ProjectionPartitionIdentity::new([3; 32], 7, [4; 32], 7, 2, 6).unwrap();
        let directory = ProjectionFamilyPartitionDirectory {
            family_id: [3; 32],
            revision: 1,
            entries: vec![ProjectionPartitionDirectoryEntry {
                partition: predecessor,
                lifecycle: ProjectionPartitionLifecycle::Active,
                covered_predecessors: Vec::new(),
            }],
        };
        let next = extend_directory(
            directory,
            vec![ProjectionPartitionDirectoryEntry {
                partition: successor,
                lifecycle: ProjectionPartitionLifecycle::Active,
                covered_predecessors: Vec::new(),
            }],
        )
        .unwrap();
        assert_eq!(next.revision, 2);
        assert!(next.entries.iter().any(|entry| {
            entry.partition == predecessor
                && entry.lifecycle == ProjectionPartitionLifecycle::Retiring { successor }
        }));
        assert!(next.entries.iter().any(|entry| {
            entry.partition == successor && entry.lifecycle == ProjectionPartitionLifecycle::Active
        }));
    }
}
