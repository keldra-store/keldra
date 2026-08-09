//! Construction of the shared index cache, source journals, builders and queries.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anvil_consensus::{DecisionRaft, NodeId};
use anyhow::{Context, Result};

use crate::bucket_governance::BucketGovernance;
use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::{ClusterPeerTransport, LocalIndexQueryExecutor};
use crate::data_peer::DataPeerTransport;
use crate::index_config::IndexRuntimeConfig;
use crate::index_service::{IndexDefinitionLister, IndexQueryExecutor};
use crate::object_distribution::ObjectDistribution;
use anvil_store::Store;

use super::budget::IndexMemoryBudgets;
use super::cache::{IndexCache, IndexCacheConfig};
use super::catalog::IndexCatalog;
use super::cpu::IndexCpuPool;
use super::discovery::{IndexDefinitionDiscovery, capture_then_refresh};
use super::distributed_query::DistributedIndexQueryExecutor;
use super::events::{ClusterIndexEventSources, DecisionIndexEventAuthority, IndexEventJournal};
use super::local_query::{ClusterIndexSegmentFetcher, LocalGenerationQueryExecutor};
use super::manager::{IndexBuilderDependencies, IndexBuilderManagerTask};
use super::publication::{IndexArtifactCoordinator, IndexArtifactRouter};
use super::publisher::IndexGenerationPublisher;
use super::retention::IndexGenerationRetention;
use super::scanner::ClusterIndexScanner;

const JOURNAL_START_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const JOURNAL_START_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct RunningIndexRuntime {
    pub(crate) definitions: Arc<dyn IndexDefinitionLister>,
    pub(crate) queries: Arc<dyn IndexQueryExecutor>,
    pub(crate) local_queries: Arc<dyn LocalIndexQueryExecutor>,
    pub(crate) event_journal: Arc<IndexEventJournal>,
    pub(crate) scanner: ClusterIndexScanner,
    pub(crate) artifact_router: IndexArtifactRouter,
    _definition_discovery: tokio::task::JoinHandle<()>,
    _builders: IndexBuilderManagerTask,
}

impl Drop for RunningIndexRuntime {
    fn drop(&mut self) {
        self._definition_discovery.abort();
    }
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
    data_directory: &Path,
    config: IndexRuntimeConfig,
) -> Result<RunningIndexRuntime> {
    let scanner = ClusterIndexScanner::new(decisions.clone(), cluster_peers.clone());
    let journal = Arc::new(IndexEventJournal::new(
        Arc::new(DecisionIndexEventAuthority::new(decisions.clone())),
        Arc::new(ClusterIndexEventSources::new(
            local_node,
            store.clone(),
            data_peers,
        )),
    ));
    let catalog = IndexCatalog::default();
    let discovery = IndexDefinitionDiscovery::new(scanner.clone(), reader.clone(), catalog.clone());
    let initial_definition_barrier = tokio::time::timeout(
        JOURNAL_START_TIMEOUT,
        capture_initial_definitions(&discovery, &journal),
    )
    .await
    .context("index journals did not reach a clear initial definition barrier")??;
    let definition_discovery = discovery.spawn(journal.clone(), initial_definition_barrier);

    let memory_bytes = index_memory_budget(config.memory_percent())?;
    let cache = IndexCache::new(
        data_directory.join("index-cache"),
        IndexCacheConfig::new(config.disk_cache_bytes(), memory_bytes)
            .context("validate index cache budgets")?,
        Arc::new(ClusterIndexSegmentFetcher::new(reader.clone())),
    )
    .context("initialize disposable index cache")?;
    let local_queries: Arc<dyn LocalIndexQueryExecutor> = Arc::new(
        LocalGenerationQueryExecutor::new(reader.clone(), cache.clone(), journal.clone()),
    );
    let queries: Arc<dyn IndexQueryExecutor> = Arc::new(DistributedIndexQueryExecutor::new(
        local_node,
        decisions.clone(),
        cluster_peers.clone(),
        local_queries.clone(),
    ));

    let coordinator = IndexArtifactCoordinator::new(objects.clone(), governance);
    let artifact_router = IndexArtifactRouter::new(local_node, coordinator, objects, cluster_peers);
    let publisher = IndexGenerationPublisher::new(store, reader.clone(), artifact_router.clone());
    let generation_retention = IndexGenerationRetention::new(
        scanner.clone(),
        reader.clone(),
        artifact_router.clone(),
        config,
    );
    let budgets = IndexMemoryBudgets::new(config.builder_memory_bytes_per_kind())
        .context("validate per-kind index construction budgets")?;
    let cpu = IndexCpuPool::new(config.rayon_workers())
        .context("initialize the fixed index Rayon pool")?;
    let builders = IndexBuilderManagerTask::start(
        local_node,
        decisions,
        catalog.clone(),
        IndexBuilderDependencies {
            catalog: catalog.clone(),
            journal: journal.clone(),
            scanner: scanner.clone(),
            reader,
            publisher,
            retention: generation_retention,
            cache,
            budgets,
            cpu,
        },
    );

    Ok(RunningIndexRuntime {
        definitions: Arc::new(catalog),
        queries,
        local_queries,
        event_journal: journal,
        scanner,
        artifact_router,
        _definition_discovery: definition_discovery,
        _builders: builders,
    })
}

async fn capture_initial_definitions(
    discovery: &IndexDefinitionDiscovery,
    journal: &IndexEventJournal,
) -> Result<super::events::IndexBarrier, tonic::Status> {
    loop {
        match capture_then_refresh(discovery, journal).await {
            Ok(barrier) => return Ok(barrier),
            Err(error) if error.code() == tonic::Code::Unavailable => {
                tokio::time::sleep(JOURNAL_START_RETRY_INTERVAL).await;
            }
            Err(error) => return Err(error),
        }
    }
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
