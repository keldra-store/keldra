//! Construction and lifetime of assigned accounting workers.

use std::sync::Arc;
use std::time::Duration;

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{
    DefinitionAssignment, DefinitionAssignmentCursor, DefinitionAssignmentMutation, DefinitionKind,
    MAX_DEFINITION_STATE_SCAN_RECORDS, PlacementLogId, Store,
};
use anyhow::Result;
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_placement::ClusterPlacement;
use crate::index_runtime::coordination::load_assigned_definition_object;
use crate::index_runtime::events::IndexEventJournal;
use crate::index_runtime::placement::{IndexIdentity, IndexPlacement};
use crate::index_runtime::publication::IndexArtifactRouter;
use crate::index_runtime::scanner::ClusterIndexScanner;

use super::flusher::AccountingTrafficTask;
use super::manager::{AccountingBuilderDependencies, AccountingManagerTask};
use super::{
    AccountingCatalog, AccountingPublisher, AccountingServiceImpl, AccountingTraffic,
    AccountingTrafficConfig, LoadedAccountingDefinition, StoredAccountingDefinition,
    definition_path,
};

const ASSIGNMENT_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const ASSIGNMENT_REVISIT_INTERVAL: Duration = Duration::from_secs(30);

pub(crate) struct RunningAccountingRuntime {
    pub(crate) traffic: AccountingTraffic,
    pub(crate) publisher: AccountingPublisher,
    assignments: tokio::task::JoinHandle<()>,
    manager: AccountingManagerTask,
    traffic_task: Option<AccountingTrafficTask>,
}

impl RunningAccountingRuntime {
    pub(crate) fn start_traffic(
        &mut self,
        local_node: NodeId,
        decisions: DecisionRaft,
        peers: ClusterPeerTransport,
        service: AccountingServiceImpl,
    ) {
        self.traffic_task = Some(AccountingTrafficTask::start(
            local_node,
            decisions,
            peers,
            self.traffic.clone(),
            service,
        ));
    }
}

impl Drop for RunningAccountingRuntime {
    fn drop(&mut self) {
        self.assignments.abort();
        let _ = &self.manager;
        let _ = &self.traffic_task;
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn start(
    local_node: NodeId,
    decisions: DecisionRaft,
    store: Store,
    reader: ClusterObjectReader,
    scanner: ClusterIndexScanner,
    event_journal: Arc<IndexEventJournal>,
    artifacts: IndexArtifactRouter,
    traffic_config: AccountingTrafficConfig,
) -> Result<RunningAccountingRuntime> {
    let catalog = AccountingCatalog::default();
    // Subscribe before the bounded initial assigned scan so a concurrent
    // assignment cannot fall into a discovery gap.
    let changes = store.subscribe_definition_assignment_changes();
    let assignments = tokio::spawn(run_accounting_assignments(
        local_node,
        decisions.clone(),
        store.clone(),
        reader.clone(),
        catalog.clone(),
        changes,
    ));
    let source = store.local_watch_status()?.source_id;
    let traffic = AccountingTraffic::new(source, traffic_config);
    let publisher = AccountingPublisher::new(store.clone(), artifacts);
    let manager = AccountingManagerTask::start(
        local_node,
        decisions,
        catalog.clone(),
        AccountingBuilderDependencies {
            store,
            journal: event_journal,
            scanner,
            reader,
            publisher: publisher.clone(),
        },
    );
    Ok(RunningAccountingRuntime {
        traffic,
        publisher,
        assignments,
        manager,
        traffic_task: None,
    })
}

async fn run_accounting_assignments(
    local_node: NodeId,
    decisions: DecisionRaft,
    store: Store,
    reader: ClusterObjectReader,
    catalog: AccountingCatalog,
    mut changes: tokio::sync::broadcast::Receiver<Vec<DefinitionAssignmentMutation>>,
) {
    let mut cursor: Option<DefinitionAssignmentCursor> = None;
    let mut scan_at = tokio::time::Instant::now();
    loop {
        tokio::select! {
            received = changes.recv() => match received {
                Ok(mutations) => {
                    for mutation in mutations {
                        if mutation.kind() != DefinitionKind::Accounting {
                            continue;
                        }
                        let identity = mutation_identity(&mutation);
                        if let Err(error) = refresh_assignment(
                            local_node,
                            &decisions,
                            &store,
                            &reader,
                            &catalog,
                            identity,
                        ).await {
                            tracing::warn!(accounting.id = identity.2, %error, "assigned accounting refresh will retry");
                            if cursor.is_none() {
                                scan_at = tokio::time::Instant::now() + ASSIGNMENT_RETRY_INTERVAL;
                            }
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    cursor = None;
                    scan_at = tokio::time::Instant::now();
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            },
            _ = tokio::time::sleep_until(scan_at) => {
                match scan_assignment_page(
                    local_node,
                    &decisions,
                    &store,
                    &reader,
                    &catalog,
                    cursor.as_ref(),
                ).await {
                    Ok(Some(next)) => {
                        cursor = Some(next);
                        scan_at = tokio::time::Instant::now();
                        tokio::task::yield_now().await;
                    }
                    Ok(None) => {
                        cursor = None;
                        scan_at = tokio::time::Instant::now() + ASSIGNMENT_REVISIT_INTERVAL;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "paged assigned accounting inventory will retry");
                        scan_at = tokio::time::Instant::now() + ASSIGNMENT_RETRY_INTERVAL;
                    }
                }
            }
        }
    }
}

async fn scan_assignment_page(
    local_node: NodeId,
    decisions: &DecisionRaft,
    store: &Store,
    reader: &ClusterObjectReader,
    catalog: &AccountingCatalog,
    cursor: Option<&DefinitionAssignmentCursor>,
) -> Result<Option<DefinitionAssignmentCursor>, Status> {
    let page = {
        let store = store.clone();
        let cursor = cursor.cloned();
        tokio::task::spawn_blocking(move || {
            store.scan_definition_assignments_by_kind(
                DefinitionKind::Accounting,
                cursor.as_ref(),
                MAX_DEFINITION_STATE_SCAN_RECORDS,
            )
        })
        .await
        .map_err(join_status)?
        .map_err(internal_status)?
    };
    for assignment in page.assignments {
        if assignment.rank != 0 {
            continue;
        }
        match load_assignment(local_node, decisions, reader, &assignment).await {
            Ok(Some(definition)) => catalog.upsert(definition)?,
            Ok(None) => remove_stale_assignment(store, &assignment).await?,
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                ) =>
            {
                return Err(error);
            }
            Err(error) => tracing::warn!(
                accounting.id = assignment.definition_id,
                %error,
                "invalid assigned accounting definition was isolated"
            ),
        }
    }
    Ok(page.next_cursor)
}

async fn refresh_assignment(
    local_node: NodeId,
    decisions: &DecisionRaft,
    store: &Store,
    reader: &ClusterObjectReader,
    catalog: &AccountingCatalog,
    identity: (u64, u64, u64),
) -> Result<(), Status> {
    catalog.remove(identity)?;
    let assignment = {
        let store = store.clone();
        tokio::task::spawn_blocking(move || {
            store.definition_assignment(
                DefinitionKind::Accounting,
                identity.0,
                identity.1,
                identity.2,
            )
        })
        .await
        .map_err(join_status)?
        .map_err(internal_status)?
    };
    let Some(assignment) = assignment else {
        return Ok(());
    };
    if assignment.rank != 0 {
        return Ok(());
    }
    match load_assignment(local_node, decisions, reader, &assignment).await? {
        Some(definition) => catalog.upsert(definition)?,
        None => remove_stale_assignment(store, &assignment).await?,
    }
    Ok(())
}

async fn remove_stale_assignment(
    store: &Store,
    assignment: &DefinitionAssignment,
) -> Result<(), Status> {
    let assignment = assignment.clone();
    let store = store.clone();
    tokio::task::spawn_blocking(move || store.remove_definition_assignment_if_matches(&assignment))
        .await
        .map_err(join_status)?
        .map(|_| ())
        .map_err(internal_status)
}

pub(crate) async fn load_assignment(
    local_node: NodeId,
    decisions: &DecisionRaft,
    reader: &ClusterObjectReader,
    assignment: &DefinitionAssignment,
) -> Result<Option<LoadedAccountingDefinition>, Status> {
    assignment
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    if assignment.kind != DefinitionKind::Accounting {
        return Ok(None);
    }
    let placement = current_placement(decisions)?;
    let identity = IndexIdentity::new(
        assignment.tenant_id,
        assignment.bucket_id,
        assignment.definition_id,
    )
    .map_err(|error| Status::data_loss(error.to_string()))?;
    let owners = IndexPlacement::derive(identity, &placement)
        .map_err(|error| Status::unavailable(error.to_string()))?;
    if owners.rank_of(local_node) != Some(assignment.rank)
        || assignment.observed_fence != placement.fence()
    {
        return Ok(None);
    }
    let Some(opened) = load_assigned_definition_object(reader, assignment).await? else {
        return Ok(None);
    };
    let stored = StoredAccountingDefinition::decode(&opened.bytes)?;
    if stored.accounting_id != assignment.definition_id
        || definition_path(stored.accounting_id)? != assignment.definition_path
    {
        return Err(Status::data_loss(
            "assigned accounting identity disagrees with its ordinary definition",
        ));
    }
    if current_placement(decisions)?.fence() != placement.fence() {
        return Err(Status::unavailable(
            "accounting placement changed while loading an assignment",
        ));
    }
    Ok(Some(LoadedAccountingDefinition {
        tenant_id: assignment.tenant_id,
        bucket_id: assignment.bucket_id,
        version: opened.object_version,
        stored,
    }))
}

fn mutation_identity(mutation: &DefinitionAssignmentMutation) -> (u64, u64, u64) {
    match mutation {
        DefinitionAssignmentMutation::Upsert(value) => {
            (value.tenant_id, value.bucket_id, value.definition_id)
        }
        DefinitionAssignmentMutation::Remove {
            tenant_id,
            bucket_id,
            definition_id,
            ..
        } => (*tenant_id, *bucket_id, *definition_id),
    }
}

fn current_placement(decisions: &DecisionRaft) -> Result<ClusterPlacement, Status> {
    let state = decisions
        .state()
        .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
    ClusterPlacement::from_applied(&state).map_err(|error| Status::unavailable(error.to_string()))
}

fn join_status(error: tokio::task::JoinError) -> Status {
    Status::internal(format!("assigned accounting worker failed: {error}"))
}

fn internal_status(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use anvil_store::{DefinitionAssignment, VersionId};

    use super::*;

    #[tokio::test]
    async fn definitive_failed_revalidation_removes_stale_accounting_assignment() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(anvil_store::StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let fence = PlacementLogId { term: 7, index: 8 };
        let assignment = DefinitionAssignment {
            kind: DefinitionKind::Accounting,
            tenant_id: 2,
            bucket_id: 3,
            definition_id: 4,
            definition_path: "_anvil/accounting/definitions/4".into(),
            object_version: VersionId(5),
            observed_fence: fence,
            rank: 0,
        };
        store
            .apply_definition_assignment_mutations(&[DefinitionAssignmentMutation::Upsert(
                assignment.clone(),
            )])
            .unwrap();
        assert!(
            store
                .remove_definition_assignment_if_matches(&assignment)
                .unwrap()
        );
        assert!(
            store
                .definition_assignment(DefinitionKind::Accounting, 2, 3, 4)
                .unwrap()
                .is_none()
        );
    }
}
