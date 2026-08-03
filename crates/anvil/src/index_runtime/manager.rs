//! Assignment and lifecycle of local weighted-HRW index builders.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anvil_consensus::{DecisionRaft, NodeId};

use crate::cluster_object_read::ClusterObjectReader;

use super::catalog::{CatalogDefinition, IndexCatalog};
use super::events::{IndexEventCatchUp, IndexEventRouter};
use super::placement::{IndexIdentity, IndexPlacement};
use super::publisher::IndexGenerationPublisher;
use super::retention::IndexGenerationRetention;
use super::scanner::ClusterIndexScanner;
use super::snapshot::IndexObjectSnapshot;
use crate::cluster_placement::ClusterPlacement;

const ASSIGNMENT_INTERVAL: Duration = Duration::from_secs(2);
const BUILDER_IDLE_INTERVAL: Duration = Duration::from_millis(100);
const BUILDER_RETRY_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) struct IndexBuilderManagerTask {
    task: tokio::task::JoinHandle<()>,
}

impl IndexBuilderManagerTask {
    pub(crate) fn start(
        local_node: NodeId,
        decisions: DecisionRaft,
        catalog: IndexCatalog,
        dependencies: IndexBuilderDependencies,
    ) -> Self {
        let task = tokio::spawn(async move {
            let mut builders = BTreeMap::<u64, RunningBuilder>::new();
            let mut interval = tokio::time::interval(ASSIGNMENT_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let definitions = match catalog.all() {
                    Ok(definitions) => definitions,
                    Err(error) => {
                        tracing::warn!(%error, "index builder catalog is unavailable");
                        continue;
                    }
                };
                let placement = match current_placement(&decisions) {
                    Ok(placement) => placement,
                    Err(error) => {
                        tracing::warn!(%error, "index builder placement is unavailable");
                        continue;
                    }
                };
                let mut desired = BTreeMap::new();
                for definition in definitions {
                    let identity = match IndexIdentity::new(
                        definition.tenant_id,
                        definition.bucket_id,
                        definition.stored.index_id,
                    ) {
                        Ok(identity) => identity,
                        Err(error) => {
                            tracing::warn!(%error, "invalid stable index identity in catalog");
                            continue;
                        }
                    };
                    let assignment = match IndexPlacement::derive(identity, &placement) {
                        Ok(assignment) => assignment,
                        Err(error) => {
                            tracing::warn!(%error, "cannot derive index builder assignment");
                            continue;
                        }
                    };
                    if assignment.builder() == local_node {
                        desired.insert(definition.stored.index_id, definition);
                    }
                }

                let desired_ids = desired.keys().copied().collect::<BTreeSet<_>>();
                builders.retain(|index_id, running| {
                    let keep = desired_ids.contains(index_id)
                        && desired.get(index_id).is_some_and(|value| {
                            value.object_version == running.definition_version
                        })
                        && !running.task.is_finished();
                    if !keep {
                        running.task.abort();
                    }
                    keep
                });
                for (index_id, definition) in desired {
                    if builders.contains_key(&index_id) {
                        continue;
                    }
                    let definition_version = definition.object_version;
                    let dependencies = dependencies.clone();
                    let task = tokio::spawn(async move {
                        run_builder(definition, dependencies).await;
                    });
                    builders.insert(
                        index_id,
                        RunningBuilder {
                            definition_version,
                            task,
                        },
                    );
                }
            }
        });
        Self { task }
    }
}

impl Drop for IndexBuilderManagerTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct RunningBuilder {
    definition_version: u64,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
pub(crate) struct IndexBuilderDependencies {
    pub(crate) router: IndexEventRouter,
    pub(crate) scanner: ClusterIndexScanner,
    pub(crate) reader: ClusterObjectReader,
    pub(crate) publisher: IndexGenerationPublisher,
    pub(crate) retention: IndexGenerationRetention,
}

async fn run_builder(definition: CatalogDefinition, dependencies: IndexBuilderDependencies) {
    loop {
        match build_from_scan(&definition, &dependencies).await {
            Ok(BuilderState::Running {
                mut snapshot,
                mut through,
            }) => loop {
                match dependencies.router.changes_after(&through).await {
                    IndexEventCatchUp::Available {
                        batches,
                        through: latest,
                    } => {
                        let mut dirty = false;
                        let mut refresh_failed = false;
                        for batch in batches {
                            match snapshot
                                .apply(
                                    &definition.stored,
                                    definition.tenant_id,
                                    definition.bucket_id,
                                    &batch,
                                    &dependencies.reader,
                                )
                                .await
                            {
                                Ok(changed) => dirty |= changed,
                                Err(error) => {
                                    tracing::warn!(
                                        index.id = definition.stored.index_id,
                                        %error,
                                        "incremental index head refresh failed; rescanning"
                                    );
                                    refresh_failed = true;
                                    break;
                                }
                            }
                        }
                        if refresh_failed {
                            break;
                        }
                        through = latest;
                        if dirty
                            && let Err(error) = publish_snapshot(
                                &definition,
                                &dependencies,
                                &snapshot,
                                through.clone(),
                            )
                            .await
                        {
                            tracing::warn!(
                                index.id = definition.stored.index_id,
                                %error,
                                "index generation publication failed; rescanning before retry"
                            );
                            break;
                        }
                        tokio::time::sleep(BUILDER_IDLE_INTERVAL).await;
                    }
                    IndexEventCatchUp::RescanRequired { reason, .. } => {
                        tracing::info!(
                            index.id = definition.stored.index_id,
                            ?reason,
                            "index event history requires a current-head rescan"
                        );
                        break;
                    }
                }
            },
            Err(error) => {
                tracing::warn!(
                    index.id = definition.stored.index_id,
                    %error,
                    "index initial build failed; retrying"
                );
            }
        }
        tokio::time::sleep(BUILDER_RETRY_INTERVAL).await;
    }
}

enum BuilderState {
    Running {
        snapshot: IndexObjectSnapshot,
        through: super::events::IndexBarrier,
    },
}

async fn build_from_scan(
    definition: &CatalogDefinition,
    dependencies: &IndexBuilderDependencies,
) -> Result<BuilderState, tonic::Status> {
    let mut through = dependencies.router.current_barrier().await;
    let mut snapshot = IndexObjectSnapshot::initial(
        &definition.stored,
        definition.tenant_id,
        definition.bucket_id,
        &through,
        &dependencies.scanner,
        &dependencies.reader,
    )
    .await?;
    match dependencies.router.changes_after(&through).await {
        IndexEventCatchUp::Available {
            batches,
            through: latest,
        } => {
            for batch in batches {
                snapshot
                    .apply(
                        &definition.stored,
                        definition.tenant_id,
                        definition.bucket_id,
                        &batch,
                        &dependencies.reader,
                    )
                    .await?;
            }
            through = latest;
        }
        IndexEventCatchUp::RescanRequired { .. } => {
            return Err(tonic::Status::unavailable(
                "index event history changed during initial scan",
            ));
        }
    }
    publish_snapshot(definition, dependencies, &snapshot, through.clone()).await?;
    Ok(BuilderState::Running { snapshot, through })
}

async fn publish_snapshot(
    definition: &CatalogDefinition,
    dependencies: &IndexBuilderDependencies,
    snapshot: &IndexObjectSnapshot,
    barrier: super::events::IndexBarrier,
) -> Result<(), tonic::Status> {
    let published = dependencies
        .publisher
        .build_and_publish(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
            definition.object_version,
            barrier,
            snapshot.values(),
        )
        .await?;
    if let Err(error) = dependencies
        .retention
        .collect(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
            published.pointer.generation,
        )
        .await
    {
        tracing::warn!(
            index.id = definition.stored.index_id,
            %error,
            "obsolete index generation cleanup will retry after a later publication"
        );
    }
    Ok(())
}

fn current_placement(decisions: &DecisionRaft) -> Result<ClusterPlacement, tonic::Status> {
    let state = decisions
        .state()
        .map_err(|_| tonic::Status::unavailable("applied cluster membership is unavailable"))?;
    ClusterPlacement::from_applied(&state)
        .map_err(|error| tonic::Status::unavailable(error.to_string()))
}
