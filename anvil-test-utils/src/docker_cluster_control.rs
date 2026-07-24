use super::*;

const DOCKER_EQUAL_PEER_COUNT: u8 = 6;
const DOCKER_METADATA_REPLICA_COUNT: u8 = 3;
const BLOCK_SHARD_ROOT: &str = "/var/lib/anvil/corestore/blocks/local-cache";
const BLOCK_SHARD_ERASURE_SET_ID: &str = "local-erasure-set";
const DEFERRED_PEER_ADMISSION_TIMEOUT: Duration = Duration::from_secs(240);
const DOCKER_NETWORK_TRANSITION_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DockerPeer {
    pub ordinal: u8,
    pub node_id: String,
    pub cell_id: String,
    pub grpc_addr: String,
    pub admin_addr: String,
}

impl DockerPeer {
    pub fn service_name(&self) -> String {
        docker_node_service(self.ordinal)
    }
}

impl DockerTestCluster {
    pub fn equal_peers(&self) -> Vec<DockerPeer> {
        assert_eq!(
            self.grpc_addrs.len(),
            DOCKER_EQUAL_PEER_COUNT as usize,
            "distributed Docker acceptance requires six equal peers"
        );
        assert_eq!(
            self.admin_addrs.len(),
            DOCKER_EQUAL_PEER_COUNT as usize,
            "distributed Docker acceptance requires six equal admin endpoints"
        );
        (1..=DOCKER_EQUAL_PEER_COUNT)
            .map(|ordinal| self.equal_peer(ordinal))
            .collect()
    }

    pub fn equal_peer(&self, ordinal: u8) -> DockerPeer {
        assert!(
            (1..=DOCKER_EQUAL_PEER_COUNT).contains(&ordinal),
            "Docker equal-peer ordinal must be between 1 and 6"
        );
        let index = usize::from(ordinal - 1);
        DockerPeer {
            ordinal,
            node_id: format!("anvil-test-node-{ordinal}"),
            cell_id: format!("test-cell-{ordinal}"),
            grpc_addr: self.grpc_addrs[index].clone(),
            admin_addr: self.admin_addrs[index].clone(),
        }
    }

    /// The default metadata-r3-q2 profile selects the first three active
    /// candidates after the production region/cell/node ordering is applied.
    pub fn selected_metadata_replicas(&self) -> Vec<DockerPeer> {
        self.equal_peers()
            .into_iter()
            .take(DOCKER_METADATA_REPLICA_COUNT as usize)
            .collect()
    }

    pub async fn restart_node_same_volume(&self, ordinal: u8) {
        self.stop_node(ordinal).await;
        self.start_node(ordinal).await;
    }

    pub async fn admit_deferred_peer(&self, ordinal: u8) {
        assert!(
            (1..=DOCKER_EQUAL_PEER_COUNT).contains(&ordinal),
            "Docker equal-peer ordinal must be between 1 and 6"
        );
        let topology = self
            .deferred_topologies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&ordinal)
            .unwrap_or_else(|| panic!("Docker peer {ordinal} has no deferred topology admission"));
        let peer_index = usize::from(ordinal - 1);
        let mut client =
            docker_topology::connect_docker_mesh_control(&self.admin_addrs[peer_index]).await;
        let mut request = tonic::Request::new(topology);
        add_docker_admin_bearer(&mut request, &self.admin_token);
        let response = client
            .bootstrap_mesh_topology(request)
            .await
            .expect("deferred Docker peer BootstrapMeshTopology")
            .into_inner();
        assert!(
            !response.already_initialised,
            "deferred Docker peer unexpectedly had an existing mesh topology"
        );
        let addr = &self.grpc_addrs[peer_index];
        assert!(
            wait_for_http_ready(addr, DEFERRED_PEER_ADMISSION_TIMEOUT).await,
            "deferred Docker peer did not become ready after topology admission: {addr}"
        );
    }

    /// Removes only final block shards while preserving the peer's metadata,
    /// identity, and volume. Restarting must cause production repair to restore
    /// any shard that is still required by a published manifest.
    pub async fn erase_node_block_shards_and_restart(&self, ordinal: u8) {
        self.stop_node(ordinal).await;
        let project_name = self.project_name.clone();
        let compose_env = self.compose_env.clone();
        tokio::task::spawn_blocking(move || {
            docker_clear_node_block_shards(&project_name, &compose_env, ordinal);
        })
        .await
        .expect("clear Docker equal-peer block shards panicked");
        self.start_node(ordinal).await;
    }

    /// Removes exact final shard paths from an already stopped peer. The
    /// caller controls restart ordering so recovery tests cannot accidentally
    /// introduce an extra stop/start race or erase unrelated shards.
    pub async fn erase_stopped_node_block_shards(&self, ordinal: u8, paths: &BTreeSet<String>) {
        assert!(
            !paths.is_empty(),
            "Docker block shard deletion requires at least one exact path"
        );
        let node_root = self.node_block_shard_root(ordinal);
        for path in paths {
            assert!(
                path.strip_prefix(&node_root)
                    .is_some_and(|suffix| suffix.starts_with('/') && !suffix.contains("/../")),
                "Docker block shard path is outside peer {ordinal}'s logical-node root: {path}"
            );
        }
        let project_name = self.project_name.clone();
        let compose_env = self.compose_env.clone();
        let paths = paths.iter().cloned().collect::<Vec<_>>();
        tokio::task::spawn_blocking(move || {
            docker_remove_node_block_shards(&project_name, &compose_env, ordinal, &paths);
        })
        .await
        .expect("remove exact Docker equal-peer block shards panicked");
    }

    pub async fn node_block_shard_count(&self, ordinal: u8) -> u64 {
        let output = self
            .exec_node_output(
                ordinal,
                &[
                    "sh",
                    "-c",
                    &format!("find {BLOCK_SHARD_ROOT} -type f -name '*.anb' 2>/dev/null | wc -l"),
                ],
            )
            .await;
        assert!(
            output.status.success(),
            "count Docker peer {ordinal} block shards: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("Docker shard count is utf-8")
            .trim()
            .parse::<u64>()
            .expect("Docker shard count is numeric")
    }

    /// Lists final shards stored for this peer's distributed logical node.
    /// Synthetic bootstrap `local-node-*` cache shards are outside this path.
    pub async fn node_block_shard_paths(&self, ordinal: u8) -> BTreeSet<String> {
        let node_root = self.node_block_shard_root(ordinal);
        let output = self
            .exec_node_output(
                ordinal,
                &[
                    "sh",
                    "-c",
                    &format!(
                        "if [ -d '{node_root}' ]; then find '{node_root}' -type f -name 'shard-*.anb' -print; fi"
                    ),
                ],
            )
            .await;
        assert!(
            output.status.success(),
            "list Docker peer {ordinal} block shard paths: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("Docker shard paths are utf-8")
            .lines()
            .map(str::to_string)
            .collect()
    }

    /// Reads one exact final shard for format-level distributed assertions.
    /// The path must come from `node_block_shard_paths`; arbitrary container
    /// paths are deliberately rejected.
    pub async fn node_block_shard_file(&self, ordinal: u8, path: &str) -> Vec<u8> {
        let node_root = self.node_block_shard_root(ordinal);
        assert!(
            path.strip_prefix(&node_root)
                .is_some_and(|suffix| suffix.starts_with('/') && !suffix.contains("/../")),
            "Docker block shard path is outside peer {ordinal}'s logical-node root: {path}"
        );
        let output = self.exec_node_output(ordinal, &["cat", "--", path]).await;
        assert!(
            output.status.success(),
            "read Docker peer {ordinal} block shard {path}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }

    /// Waits for every expected identity; unrelated shards cannot substitute
    /// for a missing path.
    pub async fn wait_for_node_block_shard_paths(
        &self,
        ordinal: u8,
        expected: &BTreeSet<String>,
        timeout: Duration,
    ) {
        assert!(
            !expected.is_empty(),
            "Docker block shard path wait requires at least one expected path"
        );
        let deadline = Instant::now() + timeout;
        let observed = loop {
            let observed = self.node_block_shard_paths(ordinal).await;
            if expected.is_subset(&observed) {
                return;
            }
            if Instant::now() >= deadline {
                break observed;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        let missing = expected
            .difference(&observed)
            .cloned()
            .collect::<BTreeSet<_>>();
        panic!(
            "Docker peer {ordinal} did not recover the exact expected block shard paths before timeout; missing={missing:?}; expected={expected:?}; observed={observed:?}"
        );
    }

    fn node_block_shard_root(&self, ordinal: u8) -> String {
        let node_id = self.equal_peer(ordinal).node_id;
        format!("{BLOCK_SHARD_ROOT}/{BLOCK_SHARD_ERASURE_SET_ID}/{node_id}")
    }

    /// Splits all six peers with kernel routing rules inside their Docker
    /// network namespaces. The peer network and DNS remain stable, while each
    /// node's independent driver network keeps its published API reachable.
    pub async fn partition_equal_peers(
        &self,
        first: &[u8],
        second: &[u8],
    ) -> DockerNetworkPartition {
        validate_partition_groups(first, second);
        let state = DockerNetworkPartitionState::new(self, first, second);
        let apply = state.clone();
        tokio::task::spawn_blocking(move || apply_partition(&apply))
            .await
            .expect("apply Docker equal-peer network partition panicked");
        for addr in &self.grpc_addrs {
            assert!(
                wait_for_http_reachable(addr, DOCKER_NETWORK_TRANSITION_TIMEOUT).await,
                "Docker equal-peer API did not remain reachable after partition: {addr}"
            );
        }
        DockerNetworkPartition { state: Some(state) }
    }
}

#[derive(Debug)]
pub struct DockerNetworkPartition {
    state: Option<DockerNetworkPartitionState>,
}

impl DockerNetworkPartition {
    pub async fn heal(mut self) {
        let state = self
            .state
            .clone()
            .expect("Docker network partition was already healed");
        let grpc_addrs = state.grpc_addrs.clone();
        tokio::task::spawn_blocking(move || heal_partition(&state))
            .await
            .expect("heal Docker equal-peer network partition panicked");
        for addr in &grpc_addrs {
            assert!(
                wait_for_http_reachable(addr, DOCKER_NETWORK_TRANSITION_TIMEOUT).await,
                "Docker equal-peer API did not become reachable after healing partition: {addr}"
            );
        }
        self.state = None;
    }
}

impl Drop for DockerNetworkPartition {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        let _ = std::panic::catch_unwind(|| heal_partition(&state));
    }
}

#[derive(Debug, Clone)]
struct DockerNetworkPartitionState {
    blocked_routes: Vec<(String, Vec<String>)>,
    grpc_addrs: Vec<String>,
}

impl DockerNetworkPartitionState {
    fn new(cluster: &DockerTestCluster, first: &[u8], second: &[u8]) -> Self {
        let peer_network = docker_compose_network_name(&cluster.project_name);
        let peers = (1..=DOCKER_EQUAL_PEER_COUNT)
            .map(|ordinal| {
                let container = docker_container_id(&cluster.project_name, ordinal);
                let address = docker_network_container_ipv4(&peer_network, &container);
                (ordinal, (container, address))
            })
            .collect::<BTreeMap<_, _>>();
        let mut blocked_routes = Vec::with_capacity(DOCKER_EQUAL_PEER_COUNT as usize);
        for (sources, targets) in [(first, second), (second, first)] {
            let target_addresses = targets
                .iter()
                .map(|ordinal| peers[ordinal].1.clone())
                .collect::<Vec<_>>();
            for source in sources {
                blocked_routes.push((peers[source].0.clone(), target_addresses.clone()));
            }
        }
        Self {
            blocked_routes,
            grpc_addrs: cluster.grpc_addrs.clone(),
        }
    }
}

fn validate_partition_groups(first: &[u8], second: &[u8]) {
    assert!(!first.is_empty(), "first Docker network partition is empty");
    assert!(
        !second.is_empty(),
        "second Docker network partition is empty"
    );
    let actual = first.iter().chain(second).copied().collect::<BTreeSet<_>>();
    let expected = (1..=DOCKER_EQUAL_PEER_COUNT).collect::<BTreeSet<_>>();
    assert_eq!(
        first.len() + second.len(),
        expected.len(),
        "Docker network partitions contain duplicate peers"
    );
    assert_eq!(
        actual, expected,
        "Docker network partitions must contain every equal peer exactly once"
    );
}

fn apply_partition(state: &DockerNetworkPartitionState) {
    for (container, peer_addresses) in &state.blocked_routes {
        docker_set_unreachable_peer_routes(container, peer_addresses, true);
    }
}

fn heal_partition(state: &DockerNetworkPartitionState) {
    for (container, peer_addresses) in &state.blocked_routes {
        docker_set_unreachable_peer_routes(container, peer_addresses, false);
    }
}
