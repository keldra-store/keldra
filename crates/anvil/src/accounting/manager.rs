//! Weighted-HRW accounting worker assignment and scalar rollup publication.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{ObjectKey, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_placement::ClusterPlacement;
use crate::index_runtime::events::{
    IndexBarrier, IndexEventJournal, IndexJournalPage, MAX_INDEX_EVENT_PAGE_BYTES,
};
use crate::index_runtime::placement::{IndexIdentity, IndexPlacement};
use crate::index_runtime::scanner::ClusterIndexScanner;

use super::{
    AccountingCatalog, AccountingObjectSnapshot, AccountingPublisher, LoadedAccountingDefinition,
    StoredAccountingRollup, StoredTrafficCheckpoint, StoredTrafficSource, current_path,
    includes_path, outbound_source_path,
};

const ASSIGNMENT_INTERVAL: Duration = Duration::from_secs(2);
const IDLE_INTERVAL: Duration = Duration::from_millis(100);
const RETRY_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) struct AccountingManagerTask {
    task: tokio::task::JoinHandle<()>,
}

impl AccountingManagerTask {
    pub(crate) fn start(
        local_node: NodeId,
        decisions: DecisionRaft,
        catalog: AccountingCatalog,
        dependencies: AccountingBuilderDependencies,
    ) -> Self {
        let task = tokio::spawn(async move {
            let mut workers = BTreeMap::<u64, RunningWorker>::new();
            let mut interval = tokio::time::interval(ASSIGNMENT_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let definitions = match catalog.all() {
                    Ok(value) => value,
                    Err(error) => {
                        tracing::warn!(%error, "accounting definition catalog is unavailable");
                        continue;
                    }
                };
                let placement = match current_placement(&decisions) {
                    Ok(value) => value,
                    Err(error) => {
                        tracing::warn!(%error, "accounting worker placement is unavailable");
                        continue;
                    }
                };
                let mut desired = BTreeMap::new();
                for definition in definitions {
                    match assignment(&definition, &placement) {
                        Ok(value) if value.builder() == local_node => {
                            desired.insert(definition.stored.accounting_id, definition);
                        }
                        Ok(_) => {}
                        Err(error) => {
                            tracing::warn!(%error, "accounting definition has invalid placement identity");
                        }
                    }
                }

                let desired_ids = desired.keys().copied().collect::<BTreeSet<_>>();
                workers.retain(|accounting_id, running| {
                    let keep = desired_ids.contains(accounting_id)
                        && desired.get(accounting_id).is_some_and(|definition| {
                            definition.version == running.definition_version
                        })
                        && !running.task.is_finished();
                    if !keep {
                        running.task.abort();
                    }
                    keep
                });
                for (accounting_id, definition) in desired {
                    if workers.contains_key(&accounting_id) {
                        continue;
                    }
                    let definition_version = definition.version;
                    let dependencies = dependencies.clone();
                    let task = tokio::spawn(async move {
                        run_worker(definition, dependencies).await;
                    });
                    workers.insert(
                        accounting_id,
                        RunningWorker {
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

impl Drop for AccountingManagerTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct RunningWorker {
    definition_version: VersionId,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
pub(crate) struct AccountingBuilderDependencies {
    pub(crate) journal: Arc<IndexEventJournal>,
    pub(crate) scanner: ClusterIndexScanner,
    pub(crate) reader: ClusterObjectReader,
    pub(crate) publisher: AccountingPublisher,
}

async fn run_worker(
    definition: LoadedAccountingDefinition,
    dependencies: AccountingBuilderDependencies,
) {
    loop {
        match build_from_scan(&definition, &dependencies).await {
            Ok((mut snapshot, mut through)) => loop {
                let target = match dependencies.journal.capture_barrier().await {
                    Ok(target) => target,
                    Err(error) => {
                        tracing::info!(
                            accounting.id = definition.stored.accounting_id,
                            %error,
                            "accounting cannot capture a complete journal barrier"
                        );
                        break;
                    }
                };
                let mut dirty = false;
                let mut failed = false;
                while through != target {
                    let page = match dependencies
                        .journal
                        .next_page(&through, &target, MAX_INDEX_EVENT_PAGE_BYTES)
                        .await
                    {
                        Ok(Some(page)) => page,
                        Ok(None) => break,
                        Err(error) => {
                            tracing::info!(
                                accounting.id = definition.stored.accounting_id,
                                %error,
                                "accounting journal evidence requires a current-head rebase"
                            );
                            failed = true;
                            break;
                        }
                    };
                    match apply_page(&definition, &mut snapshot, &page) {
                        Ok(changed) => dirty |= changed,
                        Err(error) => {
                            tracing::info!(
                                accounting.id = definition.stored.accounting_id,
                                %error,
                                "accounting transition evidence requires a current-head rebase"
                            );
                            failed = true;
                            break;
                        }
                    }
                    through = page.through;
                }
                if failed {
                    break;
                }
                if through != target {
                    tracing::info!(
                        accounting.id = definition.stored.accounting_id,
                        "accounting journal stopped before its captured barrier"
                    );
                    break;
                }
                if dirty
                    && let Err(error) =
                        publish_snapshot(&definition, &dependencies, snapshot, &through).await
                {
                    tracing::warn!(
                        accounting.id = definition.stored.accounting_id,
                        %error,
                        "accounting rollup publication failed; rebasing before retry"
                    );
                    break;
                }
                tokio::time::sleep(IDLE_INTERVAL).await;
            },
            Err(error) => {
                tracing::warn!(
                    accounting.id = definition.stored.accounting_id,
                    %error,
                    "accounting baseline failed; retrying"
                );
            }
        }
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
}

async fn build_from_scan(
    definition: &LoadedAccountingDefinition,
    dependencies: &AccountingBuilderDependencies,
) -> Result<(AccountingObjectSnapshot, IndexBarrier), Status> {
    let mut through = dependencies
        .journal
        .capture_barrier()
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let mut snapshot = AccountingObjectSnapshot::initial(definition, &dependencies.scanner).await?;
    let target = dependencies
        .journal
        .capture_barrier()
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    while through != target {
        let page = dependencies
            .journal
            .next_page(&through, &target, MAX_INDEX_EVENT_PAGE_BYTES)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?
            .ok_or_else(|| {
                Status::unavailable("accounting journal stopped before its captured barrier")
            })?;
        apply_page(definition, &mut snapshot, &page).map_err(|error| {
            Status::unavailable(format!(
                "accounting baseline transition evidence is unavailable: {error}"
            ))
        })?;
        through = page.through;
    }
    publish_snapshot(definition, dependencies, snapshot, &through).await?;
    Ok((snapshot, through))
}

fn apply_page(
    definition: &LoadedAccountingDefinition,
    snapshot: &mut AccountingObjectSnapshot,
    page: &IndexJournalPage,
) -> Result<bool, super::snapshot::AccountingAdvanceError> {
    let traffic_changed = page
        .changes
        .iter()
        .any(|change| traffic_source_change(definition, &change.change));
    Ok(snapshot.apply(definition, page)? || traffic_changed)
}

async fn publish_snapshot(
    definition: &LoadedAccountingDefinition,
    dependencies: &AccountingBuilderDependencies,
    snapshot: AccountingObjectSnapshot,
    barrier: &IndexBarrier,
) -> Result<(), Status> {
    let existing = read_rollup(definition, &dependencies.reader).await?;
    let expected_version = existing.as_ref().map(|(version, _)| *version);
    let previous = existing
        .as_ref()
        .map(|(_, rollup)| rollup)
        .filter(|rollup| rollup.definition_version == definition.version.0);
    let (inbound, outbound, traffic_sources) =
        merge_traffic(definition, barrier, previous, &dependencies.reader).await?;
    let rollup = StoredAccountingRollup::new(
        definition.stored.accounting_id,
        definition.version.0,
        snapshot.logical_stored_bytes(),
        snapshot.object_count(),
        inbound,
        outbound,
        true,
        barrier,
        traffic_sources,
    )?;
    let command_id = rollup_command_id(&rollup)?;
    dependencies
        .publisher
        .publish_rollup(
            &definition.stored,
            definition.tenant_id,
            definition.bucket_id,
            &rollup,
            expected_version,
            command_id,
        )
        .await?;
    Ok(())
}

async fn merge_traffic(
    definition: &LoadedAccountingDefinition,
    barrier: &IndexBarrier,
    previous: Option<&StoredAccountingRollup>,
    reader: &ClusterObjectReader,
) -> Result<(u64, u64, Vec<StoredTrafficCheckpoint>), Status> {
    let mut inbound = previous.map_or(0, |value| value.accepted_inbound_bytes);
    let mut outbound = previous.map_or(0, |value| value.served_outbound_bytes);
    let mut checkpoints = previous
        .map(|value| {
            value
                .traffic_sources
                .iter()
                .map(|source| (source.node_id, source.clone()))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    for node in barrier.sources.keys() {
        let Some(source) = read_traffic_source(definition, *node, reader).await? else {
            continue;
        };
        if source.definition_version != definition.version.0 {
            continue;
        }
        let old = checkpoints.get(&node.0);
        let old_inbound = old.map_or(0, |value| value.accepted_inbound_bytes);
        let old_outbound = old.map_or(0, |value| value.served_outbound_bytes);
        inbound = inbound
            .checked_add(source.accepted_inbound_bytes.saturating_sub(old_inbound))
            .ok_or_else(|| Status::resource_exhausted("accounting inbound total overflow"))?;
        outbound = outbound
            .checked_add(source.served_outbound_bytes.saturating_sub(old_outbound))
            .ok_or_else(|| Status::resource_exhausted("accounting outbound total overflow"))?;
        checkpoints.insert(
            node.0,
            StoredTrafficCheckpoint {
                node_id: node.0,
                accepted_inbound_bytes: source.accepted_inbound_bytes,
                served_outbound_bytes: source.served_outbound_bytes,
            },
        );
    }
    Ok((inbound, outbound, checkpoints.into_values().collect()))
}

pub(crate) async fn read_rollup(
    definition: &LoadedAccountingDefinition,
    reader: &ClusterObjectReader,
) -> Result<Option<(VersionId, StoredAccountingRollup)>, Status> {
    let path = current_path(definition.stored.accounting_id)?;
    let Some((version, bytes)) = read_object(definition, &path, reader).await? else {
        return Ok(None);
    };
    Ok(Some((version, StoredAccountingRollup::decode(&bytes)?)))
}

pub(crate) async fn read_traffic_source(
    definition: &LoadedAccountingDefinition,
    node: NodeId,
    reader: &ClusterObjectReader,
) -> Result<Option<StoredTrafficSource>, Status> {
    let path = outbound_source_path(definition.stored.accounting_id, node.0)?;
    let Some((_, bytes)) = read_object(definition, &path, reader).await? else {
        return Ok(None);
    };
    let source = StoredTrafficSource::decode(&bytes)?;
    if source.accounting_id != definition.stored.accounting_id || source.node_id != node.0 {
        return Err(Status::data_loss(
            "accounting traffic source identity does not match its path",
        ));
    }
    Ok(Some(source))
}

async fn read_object(
    definition: &LoadedAccountingDefinition,
    path: &str,
    reader: &ClusterObjectReader,
) -> Result<Option<(VersionId, Vec<u8>)>, Status> {
    let key = ObjectKey::new(
        &definition.stored.storage_tenant,
        &definition.stored.bucket,
        path,
    )
    .map_err(|error| Status::data_loss(error.to_string()))?;
    let Some(opened) = reader
        .open_stable(&key, definition.tenant_id, definition.bucket_id, None)
        .await?
    else {
        return Ok(None);
    };
    if opened.version.deleted {
        return Ok(None);
    }
    let mut payload = opened
        .payload
        .ok_or_else(|| Status::data_loss("live accounting object has no readable payload"))?;
    let mut bytes = Vec::new();
    payload
        .read_to_end(&mut bytes)
        .map_err(|error| Status::internal(format!("read accounting object: {error}")))?;
    Ok(Some((opened.version.id, bytes)))
}

fn traffic_source_change(
    definition: &LoadedAccountingDefinition,
    change: &anvil_store::LocalChange,
) -> bool {
    let anvil_store::LocalChange::ObjectHead(head) = change else {
        return false;
    };
    head.tenant_id == definition.tenant_id
        && head.bucket_id == definition.bucket_id
        && head.exact_path.starts_with(&format!(
            "_anvil/accounting/{}/sources/",
            definition.stored.accounting_id
        ))
        && !includes_path(&definition.stored.path_prefix, &head.exact_path)
}

fn rollup_command_id(rollup: &StoredAccountingRollup) -> Result<String, Status> {
    let encoded = rollup.encode()?;
    let hash = blake3::hash(&encoded);
    Ok(format!(
        "accounting-rollup-{}-{}",
        rollup.accounting_id,
        hex::encode(&hash.as_bytes()[..16])
    ))
}

fn assignment(
    definition: &LoadedAccountingDefinition,
    placement: &ClusterPlacement,
) -> Result<IndexPlacement, Status> {
    let identity = IndexIdentity::new(
        definition.tenant_id,
        definition.bucket_id,
        definition.stored.accounting_id,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))?;
    IndexPlacement::derive(identity, placement)
        .map_err(|error| Status::unavailable(error.to_string()))
}

fn current_placement(decisions: &DecisionRaft) -> Result<ClusterPlacement, Status> {
    let state = decisions
        .state()
        .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
    ClusterPlacement::from_applied(&state).map_err(|error| Status::unavailable(error.to_string()))
}
