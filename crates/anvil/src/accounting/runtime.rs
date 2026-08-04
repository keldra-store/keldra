//! Construction and lifetime of the accounting catalog and workers.

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::Store;
use anyhow::{Context, Result};

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::ClusterPeerTransport;
use crate::index_runtime::events::IndexEventRouter;
use crate::index_runtime::publication::IndexArtifactRouter;
use crate::index_runtime::scanner::ClusterIndexScanner;

use super::flusher::AccountingTrafficTask;
use super::manager::{AccountingBuilderDependencies, AccountingManagerTask};
use super::{
    AccountingCatalog, AccountingDiscovery, AccountingPublisher, AccountingServiceImpl,
    AccountingTraffic,
};

pub(crate) struct RunningAccountingRuntime {
    pub(crate) catalog: AccountingCatalog,
    pub(crate) traffic: AccountingTraffic,
    pub(crate) publisher: AccountingPublisher,
    discovery: tokio::task::JoinHandle<()>,
    manager: AccountingManagerTask,
    traffic_task: Option<AccountingTrafficTask>,
}

impl RunningAccountingRuntime {
    pub(crate) fn start_traffic(
        &mut self,
        local_node: NodeId,
        decisions: DecisionRaft,
        store: Store,
        peers: ClusterPeerTransport,
        service: AccountingServiceImpl,
    ) {
        self.traffic_task = Some(AccountingTrafficTask::start(
            local_node,
            decisions,
            store,
            peers,
            self.catalog.clone(),
            self.traffic.clone(),
            service,
        ));
    }
}

impl Drop for RunningAccountingRuntime {
    fn drop(&mut self) {
        self.discovery.abort();
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
    event_router: IndexEventRouter,
    artifacts: IndexArtifactRouter,
) -> Result<RunningAccountingRuntime> {
    let catalog = AccountingCatalog::default();
    let discovery = AccountingDiscovery::new(scanner.clone(), reader.clone(), catalog.clone());
    discovery
        .refresh()
        .await
        .context("perform initial accounting definition discovery")?;
    let discovery_task = discovery.spawn();
    let traffic = AccountingTraffic::new(catalog.clone());
    let publisher = AccountingPublisher::new(store, artifacts);
    let manager = AccountingManagerTask::start(
        local_node,
        decisions,
        catalog.clone(),
        AccountingBuilderDependencies {
            router: event_router,
            scanner,
            reader,
            publisher: publisher.clone(),
        },
    );
    Ok(RunningAccountingRuntime {
        catalog,
        traffic,
        publisher,
        discovery: discovery_task,
        manager,
        traffic_task: None,
    })
}
