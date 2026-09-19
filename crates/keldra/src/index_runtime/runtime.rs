//! Construction of the format-v1 index producer and query runtime.

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
use super::v1_artifact_cache::ImmutableArtifactCache;
use super::v1_catalog_lifecycle::V1CatalogLifecycleTask;
use super::v1_consumer::V1IndexProducerTask;
use super::v1_publication::V1ProjectionPublisher;
use super::v1_query_runtime::V1LocalIndexQueryExecutor;
use super::v1_retention::V1IndexRetentionTask;
use super::working_memory::{IndexWorkingMemory, SharedIndexingMemoryBackend};
use keldra_index::v1::{IndexingMemoryCredits, IndexingMemoryLimits};

pub(crate) struct RunningIndexRuntime {
    pub(crate) definitions: Arc<dyn IndexDefinitionLister>,
    pub(crate) queries: Arc<dyn IndexQueryExecutor>,
    pub(crate) local_queries: Arc<dyn LocalIndexQueryExecutor>,
    pub(crate) event_journal: Arc<IndexEventJournal>,
    pub(crate) scanner: ClusterIndexScanner,
    pub(crate) artifact_router: IndexArtifactRouter,
    _definition_coordination: DefinitionCoordinationTask,
    _producer: V1IndexProducerTask,
    _v1_catalog_lifecycle: V1CatalogLifecycleTask,
    _v1_retention: V1IndexRetentionTask,
    _v1_telemetry_summary: tokio::task::JoinHandle<()>,
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
    let v1_telemetry_summary = super::v1_telemetry::start_summary_task();
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
    let working_memory = IndexWorkingMemory::from_config(config)
        .context("validate aggregate index working-memory budget")?;
    let pipeline_bytes = usize::try_from(pipeline_memory)
        .context("index pipeline memory exceeds platform capacity")?;
    let stage_limits = IndexingMemoryLimits {
        hot_payload_bytes: pipeline_bytes,
        worker_scratch_bytes: pipeline_bytes,
        prepared_rows_bytes: pipeline_bytes,
        replay_input_bytes: pipeline_bytes,
        projection_accumulator_bytes: pipeline_bytes,
        seal_scratch_bytes: pipeline_bytes,
        ordering_catalog_bytes: pipeline_bytes,
    };
    let shared_credits = IndexingMemoryCredits::new_with_backend(
        pipeline_bytes,
        stage_limits,
        Arc::new(SharedIndexingMemoryBackend(working_memory.clone())),
    )
    .map_err(anyhow::Error::msg)
    .context("initialize actual-lifetime shared indexing credits")?;
    let catalog = IndexCatalog::with_credits(shared_credits.clone(), pipeline_bytes)
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
    let query_scheduler = cpu.query_scheduler_reserving_producer();
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
    let v1_publisher = V1ProjectionPublisher::new(
        store.clone(),
        reader.clone(),
        artifact_router.clone(),
        ImmutableArtifactCache::with_working_memory(working_memory.clone()),
        keldra_index::v1::QueryBlockLimits::for_query_memory(config.query_memory_bytes()),
    );
    let v1_catalog_lifecycle = V1CatalogLifecycleTask::start(
        catalog.clone(),
        journal.clone(),
        artifact_router.clone(),
        v1_publisher.clone(),
    );
    let v1_retention = V1IndexRetentionTask::start(
        local_node,
        catalog.clone(),
        journal.clone(),
        derived_checkpoints.clone(),
        v1_publisher.clone(),
        shared_credits.clone(),
    );
    let hot_ingress = HotProjectionIngress::with_credits(shared_credits.clone(), pipeline_memory)
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
    let local_queries: Arc<dyn LocalIndexQueryExecutor> = Arc::new(V1LocalIndexQueryExecutor::new(
        decisions.clone(),
        catalog.clone(),
        v1_publisher.clone(),
        query_budget,
        query_scheduler,
    ));
    let queries: Arc<dyn IndexQueryExecutor> = Arc::new(DistributedIndexQueryExecutor::new(
        local_node,
        decisions.clone(),
        cluster_peers.clone(),
        local_queries.clone(),
    ));

    let producer = V1IndexProducerTask::start(
        local_node,
        decisions.clone(),
        catalog.clone(),
        journal.clone(),
        store.clone(),
        scanner.clone(),
        reader.clone(),
        cpu,
        hot_ingress,
        v1_publisher,
        config,
        shared_credits,
    )
    .map_err(anyhow::Error::msg)
    .context("start format-v1 index producer")?;

    Ok(RunningIndexRuntime {
        definitions: Arc::new(DistributedIndexDefinitionLister::new(object_lister)),
        queries,
        local_queries,
        event_journal: journal,
        scanner,
        artifact_router,
        _definition_coordination: definition_coordination,
        _producer: producer,
        _v1_catalog_lifecycle: v1_catalog_lifecycle,
        _v1_retention: v1_retention,
        _v1_telemetry_summary: v1_telemetry_summary,
        _catalog_router_sync: catalog_router_sync,
    })
}

fn start_catalog_router_sync(
    catalog: IndexCatalog,
    ingress: HotProjectionIngress,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut changes = catalog.subscribe();
        let refresh = || match catalog.physical_snapshot() {
            Ok(snapshot) => {
                if !ingress.replace_compiled_catalog(snapshot) {
                    tracing::warn!(
                        "hot index router exceeded its bounded memory; journal replay remains active"
                    );
                }
            }
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
