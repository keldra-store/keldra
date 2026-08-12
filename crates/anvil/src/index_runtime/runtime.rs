//! Construction of the shared index cache, source journals, builders and queries.

use std::path::Path;
use std::sync::Arc;

use anvil_consensus::{DecisionRaft, NodeId};
use anyhow::{Context, Result};

use crate::bucket_governance::BucketGovernance;
use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{ClusterPeerTransport, LocalIndexQueryExecutor};
use crate::data_peer::DataPeerTransport;
use crate::derived_consumer::{
    DerivedCheckpointPublisher, DerivedConsumerRuntimeTask, DerivedEvidenceResolver,
};
use crate::distributed_list::DistributedObjectLister;
use crate::index_config::IndexRuntimeConfig;
use crate::index_service::{
    DistributedIndexDefinitionLister, IndexDefinitionLister, IndexQueryExecutor,
};
use crate::object_distribution::ObjectDistribution;
use crate::startup_scan_evidence::StartupScanEvidence;
use anvil_store::Store;

use super::budget::IndexMemoryBudgets;
use super::cache::{IndexCache, IndexCacheConfig};
use super::catalog::IndexCatalog;
use super::coordination::DefinitionCoordinationTask;
use super::cpu::IndexCpuPool;
use super::distributed_query::DistributedIndexQueryExecutor;
use super::events::{ClusterIndexEventSources, DecisionIndexEventAuthority, IndexEventJournal};
use super::local_query::{ClusterIndexSegmentFetcher, LocalGenerationQueryExecutor};
use super::manager::{IndexBuilderDependencies, IndexBuilderManagerTask};
use super::publication::{IndexArtifactCoordinator, IndexArtifactRouter};
use super::publisher::IndexGenerationPublisher;
use super::retention::IndexGenerationRetention;
use super::scanner::ClusterIndexScanner;

pub(crate) struct RunningIndexRuntime {
    pub(crate) definitions: Arc<dyn IndexDefinitionLister>,
    pub(crate) queries: Arc<dyn IndexQueryExecutor>,
    pub(crate) local_queries: Arc<dyn LocalIndexQueryExecutor>,
    pub(crate) event_journal: Arc<IndexEventJournal>,
    pub(crate) scanner: ClusterIndexScanner,
    pub(crate) artifact_router: IndexArtifactRouter,
    _definition_coordination: DefinitionCoordinationTask,
    _derived_retention: DerivedConsumerRuntimeTask,
    _builders: IndexBuilderManagerTask,
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
    data_directory: &Path,
    config: IndexRuntimeConfig,
    derived_checkpoints: DerivedCheckpointPublisher,
    startup_scan_evidence: StartupScanEvidence,
) -> Result<RunningIndexRuntime> {
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
    let catalog = IndexCatalog::default();
    let definition_coordination = DefinitionCoordinationTask::start(
        local_node,
        decisions.clone(),
        store.clone(),
        data_peers,
        cluster_peers.clone(),
        reader.clone(),
        catalog.clone(),
    );

    let memory_bytes = index_memory_budget(config.memory_percent())?;
    let cache = IndexCache::new_with_startup_scan_evidence(
        data_directory.join("index-cache"),
        IndexCacheConfig::new(config.disk_cache_bytes(), memory_bytes)
            .context("validate index cache budgets")?,
        Arc::new(ClusterIndexSegmentFetcher::new(reader.clone())),
        startup_scan_evidence,
    )
    .context("initialize disposable index cache")?;
    let cpu = IndexCpuPool::new(config.rayon_workers())
        .context("initialize the fixed index Rayon pool")?;
    let local_queries: Arc<dyn LocalIndexQueryExecutor> =
        Arc::new(LocalGenerationQueryExecutor::new(
            local_node,
            decisions.clone(),
            reader.clone(),
            cache.clone(),
            journal.clone(),
            catalog.clone(),
            cpu.clone(),
            config.query_max_concurrency(),
            config.query_work_quantum_bytes(),
        ));
    let queries: Arc<dyn IndexQueryExecutor> = Arc::new(DistributedIndexQueryExecutor::new(
        local_node,
        decisions.clone(),
        cluster_peers.clone(),
        local_queries.clone(),
    ));

    let coordinator = IndexArtifactCoordinator::new(
        store.clone(),
        objects.clone(),
        governance,
        cluster_peers.clone(),
    );
    let artifact_router = IndexArtifactRouter::new(local_node, coordinator, objects, cluster_peers);
    let publisher = IndexGenerationPublisher::new(
        store.clone(),
        reader.clone(),
        artifact_router.clone(),
        data_directory.join("index-scratch"),
    )
    .context("initialize disposable index construction scratch")?;
    let (derived_progress, derived_retention) = DerivedConsumerRuntimeTask::start(
        anvil_store::DerivedConsumerKind::Index,
        local_node,
        decisions.clone(),
        store.clone(),
        journal.clone(),
        derived_checkpoints,
        DerivedEvidenceResolver::index(
            local_node,
            decisions.clone(),
            reader.clone(),
            publisher.clone(),
            catalog.clone(),
        ),
    );
    let generation_retention = IndexGenerationRetention::new(
        scanner.clone(),
        reader.clone(),
        artifact_router.clone(),
        config,
    );
    let budgets = IndexMemoryBudgets::from_config(config)
        .context("validate per-kind index construction budgets")?;
    let builders = IndexBuilderManagerTask::start(
        local_node,
        decisions,
        catalog.clone(),
        IndexBuilderDependencies {
            store,
            journal: journal.clone(),
            scanner: scanner.clone(),
            reader,
            publisher,
            retention: generation_retention,
            cache,
            budgets,
            cpu,
            config,
            derived_progress,
        },
    );

    Ok(RunningIndexRuntime {
        definitions: Arc::new(DistributedIndexDefinitionLister::new(object_lister)),
        queries,
        local_queries,
        event_journal: journal,
        scanner,
        artifact_router,
        _definition_coordination: definition_coordination,
        _derived_retention: derived_retention,
        _builders: builders,
    })
}

fn index_memory_budget(percent: u8) -> Result<u64> {
    let total = cgroup_memory_limit()
        .or_else(host_memory_bytes)
        .context("determine physical memory for index materialization budget")?;
    let bytes = (u128::from(total) * u128::from(percent) / 100)
        .try_into()
        .context("index materialization budget exceeds u64")?;
    anyhow::ensure!(
        bytes > 0,
        "index materialization budget resolved to zero bytes"
    );
    Ok(bytes)
}

fn cgroup_memory_limit() -> Option<u64> {
    let value = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let parsed = value.trim().parse::<u64>().ok()?;
    (parsed > 0).then_some(parsed)
}

fn host_memory_bytes() -> Option<u64> {
    let contents = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kilobytes = contents
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?
        .split_ascii_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    kilobytes.checked_mul(1_024)
}

#[cfg(test)]
mod tests {
    #[test]
    fn configured_percentage_is_applied_without_float_rounding() {
        let total = 1_000_u64;
        let percent = 17_u8;
        let bytes: u64 = (u128::from(total) * u128::from(percent) / 100)
            .try_into()
            .unwrap();
        assert_eq!(bytes, 170);
    }
}
