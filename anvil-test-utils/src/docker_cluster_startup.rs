use super::*;

/// Create a Docker-backed cluster with a unique compose project and host ports.
/// Use this only for tests that intentionally stop/restart nodes, need a unique
/// region, or otherwise cannot share the long-lived Docker cluster safely.
pub async fn isolated_docker_test_cluster(label: &str, region: &str) -> DockerTestCluster {
    isolated_docker_test_cluster_inner(label, region, None).await
}

/// Create an isolated cluster while withholding one peer's production topology
/// admission. Tests can create committed history before admitting that peer.
pub async fn isolated_docker_test_cluster_with_deferred_peer(
    label: &str,
    region: &str,
    deferred_ordinal: u8,
) -> DockerTestCluster {
    isolated_docker_test_cluster_inner(label, region, Some(deferred_ordinal)).await
}

async fn isolated_docker_test_cluster_inner(
    label: &str,
    region: &str,
    deferred_ordinal: Option<u8>,
) -> DockerTestCluster {
    let cluster_permit = acquire_test_cluster_permit().await;
    let label = compact_resource_label(label, 24);
    let region = if region.trim().is_empty() {
        docker_test_region()
    } else {
        region.to_string()
    };
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated Docker Anvil test-cluster runtime");
        runtime.block_on(async move {
            DockerTestCluster::start_isolated(&label, &region, deferred_ordinal, cluster_permit)
                .await
        })
    })
    .await
    .expect("isolated Docker Anvil test cluster initialization panicked")
}
