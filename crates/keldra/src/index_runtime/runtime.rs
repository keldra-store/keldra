//! Construction of the shared index cache, source journals, builders and queries.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use keldra_consensus::{DecisionRaft, NodeId};

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
use crate::logical_name_resolution::LogicalNameResolver;
use crate::object_distribution::ObjectDistribution;
use crate::startup_scan_evidence::StartupScanEvidence;
use keldra_store::Store;

use super::budget::IndexMemoryBudgets;
use super::cache::{IndexCache, IndexCacheConfig};
use super::catalog::IndexCatalog;
use super::coordination::DefinitionCoordinationTask;
use super::cpu::IndexCpuPool;
use super::distributed_query::DistributedIndexQueryExecutor;
use super::events::{ClusterIndexEventSources, DecisionIndexEventAuthority, IndexEventJournal};
use super::local_query::{ClusterIndexSegmentFetcher, LocalRevisionQueryExecutor};
use super::manager::{IndexBuilderDependencies, IndexBuilderManagerTask, IndexPublicationSlots};
use super::publication::{IndexArtifactCoordinator, IndexArtifactRouter};
use super::publisher::IndexCommitPublisher;
use super::query_budget::IndexQueryMemoryBudget;
use super::retention::IndexCommitRetention;
use super::scanner::ClusterIndexScanner;
use super::working_memory::IndexWorkingMemory;

pub(crate) struct RunningIndexRuntime {
    pub(crate) definitions: Arc<dyn IndexDefinitionLister>,
    pub(crate) queries: Arc<dyn IndexQueryExecutor>,
    pub(crate) local_queries: Arc<dyn LocalIndexQueryExecutor>,
    pub(crate) event_journal: Arc<IndexEventJournal>,
    pub(crate) scanner: ClusterIndexScanner,
    pub(crate) artifact_router: IndexArtifactRouter,
    cache: IndexCache,
    _definition_coordination: DefinitionCoordinationTask,
    _derived_retention: DerivedConsumerRuntimeTask,
    _builders: IndexBuilderManagerTask,
}

impl RunningIndexRuntime {
    /// Start disposable cache reconciliation only after the public listener is
    /// accepting requests. Runtime construction performs no cache inventory.
    pub(crate) fn start_cache_reconciler(&self) -> Result<()> {
        anyhow::ensure!(
            self.cache.start_reconciler(),
            "index cache reconciler was started more than once"
        );
        Ok(())
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
    object_lister: DistributedObjectLister,
    names: LogicalNameResolver,
    cache_directory: &Path,
    scratch_directory: &Path,
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
    let cache = IndexCache::new_with_directories_and_startup_scan_evidence(
        cache_directory,
        scratch_directory,
        IndexCacheConfig::new(config.disk_cache_bytes(), memory_bytes)
            .context("validate index cache budgets")?,
        Arc::new(ClusterIndexSegmentFetcher::new(reader.clone())),
        startup_scan_evidence,
    )
    .context("initialize disposable index cache")?;
    let cpu = IndexCpuPool::new(config.rayon_workers())
        .context("initialize the fixed index Rayon pool")?;
    let coordinator = IndexArtifactCoordinator::new(
        store.clone(),
        objects.clone(),
        governance,
        cluster_peers.clone(),
    );
    let artifact_router =
        IndexArtifactRouter::new(local_node, coordinator, objects, cluster_peers.clone());
    let publisher = IndexCommitPublisher::new(
        store.clone(),
        reader.clone(),
        artifact_router.clone(),
        config,
    );
    let working_memory = IndexWorkingMemory::from_config(config)
        .context("validate aggregate index working-memory budget")?;
    let query_budget = IndexQueryMemoryBudget::from_shared(working_memory.clone());
    let local_queries: Arc<dyn LocalIndexQueryExecutor> =
        Arc::new(LocalRevisionQueryExecutor::new(
            reader.clone(),
            cache.clone(),
            publisher.clone(),
            cpu.clone(),
            query_budget,
            config.query_max_concurrency(),
            config.query_work_quantum_bytes(),
        ));
    let queries: Arc<dyn IndexQueryExecutor> = Arc::new(DistributedIndexQueryExecutor::new(
        local_node,
        decisions.clone(),
        cluster_peers.clone(),
        local_queries.clone(),
    ));

    let (derived_progress, derived_retention) = DerivedConsumerRuntimeTask::start(
        keldra_store::DerivedConsumerKind::Index,
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
    let commit_retention = IndexCommitRetention::new(
        store.clone(),
        scanner.clone(),
        reader.clone(),
        artifact_router.clone(),
        publisher.clone(),
        cache.merge_scratch(),
        names,
        config,
    );
    let budgets = IndexMemoryBudgets::from_config(config, working_memory)
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
            retention: commit_retention,
            cache: cache.clone(),
            budgets,
            cpu,
            config,
            derived_progress,
            publication_slots: IndexPublicationSlots::default(),
        },
    );

    Ok(RunningIndexRuntime {
        definitions: Arc::new(DistributedIndexDefinitionLister::new(object_lister)),
        queries,
        local_queries,
        event_journal: journal,
        scanner,
        artifact_router,
        cache,
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

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "macos")]
fn host_memory_bytes() -> Option<u64> {
    unsafe extern "C" {
        fn sysctlbyname(
            name: *const std::ffi::c_char,
            old_value: *mut std::ffi::c_void,
            old_length: *mut usize,
            new_value: *mut std::ffi::c_void,
            new_length: usize,
        ) -> std::ffi::c_int;
    }

    let mut bytes = 0_u64;
    let mut length = std::mem::size_of::<u64>();
    // SAFETY: the NUL-terminated name is static, `bytes` and `length` point to
    // writable storage of the documented sizes, and this read-only query does
    // not retain either pointer.
    let result = unsafe {
        sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&mut bytes as *mut u64).cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    (result == 0 && length == std::mem::size_of::<u64>() && bytes > 0).then_some(bytes)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn host_memory_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_percentage_is_applied_without_float_rounding() {
        let total = 1_000_u64;
        let percent = 17_u8;
        let bytes: u64 = (u128::from(total) * u128::from(percent) / 100)
            .try_into()
            .unwrap();
        assert_eq!(bytes, 170);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn host_memory_is_available_on_supported_servers() {
        assert!(host_memory_bytes().is_some_and(|bytes| bytes > 0));
    }
}
