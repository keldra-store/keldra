//! Construction of the shared index cache, source router, builders and queries.

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

use super::cache::{IndexCache, IndexCacheConfig};
use super::catalog::IndexCatalog;
use super::discovery::IndexDefinitionDiscovery;
use super::distributed_query::DistributedIndexQueryExecutor;
use super::events::{
    ClusterIndexEventSources, DecisionIndexEventAuthority, IndexEventJournal, IndexEventRouter,
    IndexEventRouterRetention, IndexEventRouterTask,
};
use super::local_query::{ClusterIndexSegmentFetcher, LocalGenerationQueryExecutor};
use super::manager::{IndexBuilderDependencies, IndexBuilderManagerTask};
use super::publication::{IndexArtifactCoordinator, IndexArtifactRouter};
use super::publisher::IndexGenerationPublisher;
use super::retention::IndexGenerationRetention;
use super::scanner::ClusterIndexScanner;

const EVENT_ROUTER_MAX_BATCHES: usize = 1_024;
const EVENT_ROUTER_MAX_CHANGES: usize = 1_000_000;
const EVENT_ROUTER_POLL_INTERVAL: Duration = Duration::from_millis(100);
const EVENT_ROUTER_START_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct RunningIndexRuntime {
    pub(crate) definitions: Arc<dyn IndexDefinitionLister>,
    pub(crate) queries: Arc<dyn IndexQueryExecutor>,
    pub(crate) local_queries: Arc<dyn LocalIndexQueryExecutor>,
    _definition_discovery: tokio::task::JoinHandle<()>,
    _event_router: IndexEventRouterTask,
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
    let catalog = IndexCatalog::default();
    let discovery = IndexDefinitionDiscovery::new(scanner.clone(), reader.clone(), catalog.clone());
    discovery
        .refresh()
        .await
        .context("perform initial index definition discovery")?;
    let definition_discovery = discovery.spawn();

    let journal = Arc::new(IndexEventJournal::new(
        Arc::new(DecisionIndexEventAuthority::new(decisions.clone())),
        Arc::new(ClusterIndexEventSources::new(
            local_node,
            store.clone(),
            data_peers,
        )),
    ));
    let retention =
        IndexEventRouterRetention::new(EVENT_ROUTER_MAX_BATCHES, EVENT_ROUTER_MAX_CHANGES)
            .context("validate index event-router retention")?;
    let (event_router, event_router_task) = tokio::time::timeout(
        EVENT_ROUTER_START_TIMEOUT,
        start_event_router(journal, retention),
    )
    .await
    .context("index event router did not reach a clear cluster barrier")??;

    let memory_bytes = index_memory_budget(config.memory_percent())?;
    let cache = IndexCache::new(
        data_directory.join("index-cache"),
        IndexCacheConfig::new(config.disk_cache_bytes(), memory_bytes)
            .context("validate index cache budgets")?,
        Arc::new(ClusterIndexSegmentFetcher::new(reader.clone())),
    )
    .context("initialize disposable index cache")?;
    let local_queries: Arc<dyn LocalIndexQueryExecutor> = Arc::new(
        LocalGenerationQueryExecutor::new(reader.clone(), cache, event_router.clone()),
    );
    let queries: Arc<dyn IndexQueryExecutor> = Arc::new(DistributedIndexQueryExecutor::new(
        local_node,
        decisions.clone(),
        cluster_peers.clone(),
        local_queries.clone(),
    ));

    let coordinator = IndexArtifactCoordinator::new(objects.clone(), governance);
    let artifact_router = IndexArtifactRouter::new(local_node, coordinator, objects, cluster_peers);
    let publisher = IndexGenerationPublisher::new(
        store,
        reader.clone(),
        scanner.clone(),
        artifact_router.clone(),
    );
    let generation_retention =
        IndexGenerationRetention::new(scanner.clone(), artifact_router, config);
    let builders = IndexBuilderManagerTask::start(
        local_node,
        decisions,
        catalog.clone(),
        IndexBuilderDependencies {
            router: event_router,
            scanner,
            reader,
            publisher,
            retention: generation_retention,
        },
    );

    Ok(RunningIndexRuntime {
        definitions: Arc::new(catalog),
        queries,
        local_queries,
        _definition_discovery: definition_discovery,
        _event_router: event_router_task,
        _builders: builders,
    })
}

async fn start_event_router(
    journal: Arc<IndexEventJournal>,
    retention: IndexEventRouterRetention,
) -> Result<(IndexEventRouter, IndexEventRouterTask), super::events::IndexEventRouterError> {
    loop {
        match IndexEventRouter::start(journal.clone(), retention, EVENT_ROUTER_POLL_INTERVAL).await
        {
            Ok(runtime) => return Ok(runtime),
            Err(super::events::IndexEventRouterError::Journal(
                super::events::IndexEventError::AtomicProgramInProgress
                | super::events::IndexEventError::BarrierChanged,
            )) => tokio::time::sleep(EVENT_ROUTER_POLL_INTERVAL).await,
            Err(error) => return Err(error),
        }
    }
}

fn index_memory_budget(percent: u8) -> Result<u64> {
    let total = cgroup_memory_limit()
        .or_else(host_memory_bytes)
        .context("determine physical memory for index cache budget")?;
    let bytes = (u128::from(total) * u128::from(percent) / 100)
        .try_into()
        .context("index memory cache budget exceeds u64")?;
    anyhow::ensure!(
        bytes > 0,
        "index memory cache budget resolved to zero bytes"
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
