//! Construction of the format-v6 index producer and query runtime.

use std::sync::Arc;

use anyhow::{Context, Result};
use keldra_consensus::{DecisionRaft, NodeId};

use crate::bucket_governance::BucketGovernance;
use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{ClusterPeerTransport, LocalIndexQueryExecutor};
use crate::data_peer::DataPeerTransport;
use crate::derived_consumer::DerivedCheckpointPublisher;
use crate::distributed_list::DistributedObjectLister;
use crate::index_config::IndexRuntimeConfig;
use crate::index_service::{
    DistributedIndexDefinitionLister, IndexDefinitionLister, IndexQueryExecutor,
};
use crate::object_distribution::ObjectDistribution;
use crate::startup_scan_evidence::StartupScanEvidence;
use keldra_store::Store;

use super::catalog::IndexCatalog;
use super::coordination::DefinitionCoordinationTask;
use super::cpu::IndexCpuPool;
use super::distributed_query::DistributedIndexQueryExecutor;
use super::events::{ClusterIndexEventSources, DecisionIndexEventAuthority, IndexEventJournal};
use super::hot_ingress::HotProjectionIngress;
use super::publication::{IndexArtifactCoordinator, IndexArtifactRouter};
use super::query_budget::IndexQueryMemoryBudget;
use super::scanner::ClusterIndexScanner;
use super::v6_catalog_lifecycle::V6CatalogLifecycleTask;
use super::v6_consumer::V6IndexProducerTask;
use super::v6_publication::V6ProjectionPublisher;
use super::v6_query_runtime::V6LocalIndexQueryExecutor;
use super::v6_retention::V6IndexRetentionTask;
use super::working_memory::{IndexWorkingMemory, WorkingMemoryAccount, WorkingMemoryPermit};

pub(crate) struct RunningIndexRuntime {
    pub(crate) definitions: Arc<dyn IndexDefinitionLister>,
    pub(crate) queries: Arc<dyn IndexQueryExecutor>,
    pub(crate) local_queries: Arc<dyn LocalIndexQueryExecutor>,
    pub(crate) event_journal: Arc<IndexEventJournal>,
    pub(crate) scanner: ClusterIndexScanner,
    pub(crate) artifact_router: IndexArtifactRouter,
    _definition_coordination: DefinitionCoordinationTask,
    _pipeline_memory: WorkingMemoryPermit,
    _producer: V6IndexProducerTask,
    _v6_catalog_lifecycle: V6CatalogLifecycleTask,
    _v6_retention: V6IndexRetentionTask,
    _v6_telemetry_summary: tokio::task::JoinHandle<()>,
    _catalog_router_sync: tokio::task::JoinHandle<()>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn start(
    local_node: NodeId,
    decisions: DecisionRaft,
    store: Store,
    data_peers: DataPeerTransport,
    cluster_peers: ClusterPeerTransport,
    objects: ObjectDistribution,
    governance: BucketGovernance,
    reader: ClusterObjectReader,
    object_lister: DistributedObjectLister,
    config: IndexRuntimeConfig,
    derived_checkpoints: DerivedCheckpointPublisher,
    startup_scan_evidence: StartupScanEvidence,
) -> Result<RunningIndexRuntime> {
    let v6_telemetry_summary = super::v6_telemetry::start_summary_task();
    tracing::info!("index runtime starts from sparse assigned-definition state");
    let scanner = ClusterIndexScanner::new(
        decisions.clone(),
        cluster_peers.clone(),
        startup_scan_evidence.clone(),
    );
    let journal = Arc::new(IndexEventJournal::new(
        Arc::new(DecisionIndexEventAuthority::new(decisions.clone())),
        Arc::new(ClusterIndexEventSources::new(
            local_node,
            store.clone(),
            data_peers.clone(),
        )),
    ));
    let pipeline_memory = config.pipeline_memory_bytes();
    let catalog = IndexCatalog::with_memory_bytes(pipeline_memory / 4)
        .map_err(anyhow::Error::msg)
        .context("reserve bounded TypedJson ordering-catalog memory")?;
    let definition_coordination = DefinitionCoordinationTask::start(
        local_node,
        decisions.clone(),
        store.clone(),
        data_peers,
        cluster_peers.clone(),
        reader.clone(),
        catalog.clone(),
        journal.clone(),
    );

    let cpu = IndexCpuPool::new(config.indexing_cores())
        .context("initialize the fixed index Rayon pool")?;
    let coordinator = IndexArtifactCoordinator::new(
        store.clone(),
        objects.clone(),
        governance,
        cluster_peers.clone(),
    );
    let artifact_router = IndexArtifactRouter::new(
        local_node,
        coordinator,
        objects.clone(),
        cluster_peers.clone(),
    );
    let v6_publisher =
        V6ProjectionPublisher::new(store.clone(), reader.clone(), artifact_router.clone());
    let v6_catalog_lifecycle = V6CatalogLifecycleTask::start(
        catalog.clone(),
        journal.clone(),
        artifact_router.clone(),
        v6_publisher.clone(),
    );
    let v6_retention = V6IndexRetentionTask::start(
        local_node,
        store.clone(),
        catalog.clone(),
        journal.clone(),
        derived_checkpoints.clone(),
        v6_publisher.clone(),
    );
    let working_memory = IndexWorkingMemory::from_config(config)
        .context("validate aggregate index working-memory budget")?;
    let pipeline_memory_permit = working_memory
        .acquire_up_to(
            WorkingMemoryAccount::IndexingPipeline,
            pipeline_memory,
            pipeline_memory,
        )
        .await
        .context("reserve format-v6 indexing pipeline memory")?;
    let hot_ingress = HotProjectionIngress::new(pipeline_memory / 4)
        .map_err(anyhow::Error::msg)
        .context("initialize bounded TypedJson hot ingress")?;
    hot_ingress
        .install_cpu(cpu.clone())
        .map_err(anyhow::Error::msg)
        .context("install hot indexing CPU pool")?;
    let catalog_router_sync = start_catalog_router_sync(catalog.clone(), hot_ingress.clone());
    objects
        .install_hot_indexing(hot_ingress.clone())
        .map_err(anyhow::Error::msg)
        .context("install TypedJson hot ingress on object mutation coordinators")?;
    let query_budget = IndexQueryMemoryBudget::from_shared(working_memory.clone());
    let local_queries: Arc<dyn LocalIndexQueryExecutor> = Arc::new(V6LocalIndexQueryExecutor::new(
        decisions.clone(),
        reader.clone(),
        catalog.clone(),
        v6_publisher.clone(),
        query_budget,
    ));
    let queries: Arc<dyn IndexQueryExecutor> = Arc::new(DistributedIndexQueryExecutor::new(
        local_node,
        decisions.clone(),
        cluster_peers.clone(),
        local_queries.clone(),
    ));

    let producer = V6IndexProducerTask::start(
        local_node,
        decisions.clone(),
        catalog.clone(),
        journal.clone(),
        scanner.clone(),
        reader.clone(),
        cpu,
        hot_ingress,
        v6_publisher,
        config,
    )
    .map_err(anyhow::Error::msg)
    .context("start format-v6 index producer")?;

    Ok(RunningIndexRuntime {
        definitions: Arc::new(DistributedIndexDefinitionLister::new(object_lister)),
        queries,
        local_queries,
        event_journal: journal,
        scanner,
        artifact_router,
        _definition_coordination: definition_coordination,
        _pipeline_memory: pipeline_memory_permit,
        _producer: producer,
        _v6_catalog_lifecycle: v6_catalog_lifecycle,
        _v6_retention: v6_retention,
        _v6_telemetry_summary: v6_telemetry_summary,
        _catalog_router_sync: catalog_router_sync,
    })
}

fn start_catalog_router_sync(
    catalog: IndexCatalog,
    ingress: HotProjectionIngress,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut changes = catalog.subscribe();
        let refresh = || match catalog.hot_router_snapshot() {
            Ok((generation, routes)) => ingress.replace_compiled_catalog(generation, routes),
            Err(error) => tracing::error!(%error, "active physical catalog router refresh failed"),
        };
        refresh();
        loop {
            match changes.recv().await {
                Ok(notice) if notice.physical_changed => refresh(),
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => refresh(),
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    })
}
