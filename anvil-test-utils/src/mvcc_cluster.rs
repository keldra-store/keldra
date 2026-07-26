//! Docker-free real OpenRaft and gRPC replication cluster fixture.

use std::{sync::Arc, time::Duration};

use anvil_core::config::Config;
use anvil_core::{
    AppState,
    anvil_api::{
        consensus_transport_server::ConsensusTransportServer,
        replication_service_server::ReplicationServiceServer,
    },
    mvcc_node_runtime::CommitOutcome,
    mvcc_transaction::{DurabilityLevel, LogicalKey, ReadConsistency},
    personaldb_signing::PersonalDbProtocolKeyring,
};
use anvil_mvcc_consensus::Consensus as _;
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Three independent RocksDB nodes using real OpenRaft and replication RPCs.
///
/// Tests can compose the public API server over [`state`](Self::state), while
/// restart and partition controls retain the same disk and network identities.
pub struct RealMvccCluster {
    _directories: Vec<TempDir>,
    configs: Vec<Config>,
    endpoints: Vec<String>,
    states: Vec<Option<Arc<AppState>>>,
    transports: Vec<Option<JoinHandle<()>>>,
}

impl RealMvccCluster {
    pub async fn start() -> anyhow::Result<Self> {
        let cluster_id = format!("e2e-{}", uuid::Uuid::new_v4().simple());
        let directories = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let mut listeners = Vec::new();
        for _ in 0..3 {
            listeners.push(TcpListener::bind("127.0.0.1:0").await?);
        }
        let endpoints = listeners
            .iter()
            .map(|listener| format!("http://{}", listener.local_addr().unwrap()))
            .collect::<Vec<_>>();
        let peers_json = serde_json::to_string(
            &endpoints
                .iter()
                .enumerate()
                .map(|(index, endpoint)| {
                    serde_json::json!({
                        "cluster_id": cluster_id,
                        "raft_node_id": index + 1,
                        "node_id": format!("{cluster_id}-node-{}", index + 1),
                        "incarnation": 1,
                        "endpoint": endpoint,
                        "failure_domain": format!("zone-{}", index + 1),
                        "voter": true,
                    })
                })
                .collect::<Vec<_>>(),
        )?;
        let configs = directories
            .iter()
            .enumerate()
            .map(|(index, directory)| Config {
                jwt_secret: "e2e-secret".into(),
                anvil_secret_encryption_key:
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                node_id: format!("{cluster_id}-node-{}", index + 1),
                region: "e2e-region".into(),
                storage_path: directory
                    .path()
                    .join("storage")
                    .to_string_lossy()
                    .into_owned(),
                mvcc_cluster_id: cluster_id.clone(),
                mvcc_raft_node_id: index as u64 + 1,
                mvcc_node_incarnation: 1,
                mvcc_failure_domain: format!("zone-{}", index + 1),
                mvcc_peers_json: peers_json.clone(),
                mvcc_bootstrap_membership: index == 0,
                mvcc_bundle_quorum_holders: 2,
                mvcc_tolerated_failure_domains: 1,
                mvcc_rpc_timeout_ms: 1_000,
                allow_test_only_insecure_mvcc_transport: true,
                bootstrap_node_ids: (1..=3)
                    .map(|node| format!("{cluster_id}-node-{node}"))
                    .collect(),
                ..Config::default()
            })
            .collect::<Vec<_>>();
        let mut listeners = listeners.into_iter().map(Some).collect::<Vec<_>>();
        let mut states = vec![None, None, None];
        let mut transports = vec![None, None, None];
        // Followers must be accepting consensus RPCs before the bootstrap
        // voter installs the three-node membership and initial control state.
        // Constructing node zero first deadlocks its quorum proposals against
        // peers whose AppState/transport has not yet been started.
        for index in [1_usize, 2, 0] {
            let state = Arc::new(
                AppState::new(
                    configs[index].clone(),
                    PersonalDbProtocolKeyring::disabled(),
                )
                .await?,
            );
            transports[index] = Some(spawn_transport(
                listeners[index].take().expect("listener started once"),
                &state,
            ));
            states[index] = Some(state);
        }
        let cluster = Self {
            _directories: directories,
            configs,
            endpoints,
            states,
            transports,
        };
        cluster.wait_for_any_leader(&[0, 1, 2]).await?;
        Ok(cluster)
    }

    pub fn state(&self, node: usize) -> &Arc<AppState> {
        self.states[node]
            .as_ref()
            .expect("requested MVCC cluster node is running")
    }

    pub fn endpoint(&self, node: usize) -> &str {
        &self.endpoints[node]
    }

    /// Simulates a bidirectional network partition for this node.
    pub fn partition(&mut self, node: usize) {
        if let Some(transport) = self.transports[node].take() {
            transport.abort();
        }
    }

    /// Reopens the node from the same RocksDB directory and network identity.
    pub async fn restart_node(&mut self, node: usize) -> anyhow::Result<()> {
        if let Some(transport) = self.transports[node].take() {
            transport.abort();
            let _ = transport.await;
        }
        let previous = self.states[node]
            .take()
            .expect("restarted MVCC cluster node is running");
        previous.mvcc.shutdown().await;
        previous.mvcc.consensus.shutdown().await?;
        drop(previous);
        let state = Arc::new(
            AppState::new(
                self.configs[node].clone(),
                PersonalDbProtocolKeyring::disabled(),
            )
            .await?,
        );
        let listener =
            TcpListener::bind(self.endpoints[node].trim_start_matches("http://")).await?;
        self.transports[node] = Some(spawn_transport(listener, &state));
        self.states[node] = Some(state);
        Ok(())
    }

    pub async fn wait_for_any_leader(&self, nodes: &[usize]) -> anyhow::Result<usize> {
        Ok(tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                for node in nodes {
                    if self
                        .state(*node)
                        .mvcc
                        .consensus
                        .linearized_read_barrier()
                        .await
                        .is_ok()
                    {
                        return *node;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await?)
    }

    pub async fn commit(
        &self,
        node: usize,
        id: &str,
        key: LogicalKey,
        value: Vec<u8>,
    ) -> anyhow::Result<CommitOutcome> {
        let state = self.state(node);
        let principal = "e2e-principal";
        let handle = state
            .mvcc
            .open_transactions
            .begin(
                state.mvcc.runtime.as_ref(),
                state.mvcc.cluster_id().to_string(),
                principal,
                id,
                Duration::from_secs(30),
                DurabilityLevel::Quorum,
                ReadConsistency::Linearized,
                1,
            )
            .await?;
        state.mvcc.open_transactions.put(
            &handle.transaction_id,
            state.mvcc.cluster_id(),
            key,
            value,
            2,
        )?;
        Ok(state
            .mvcc
            .open_transactions
            .commit(
                state.mvcc.runtime.as_ref(),
                &handle.transaction_id,
                principal,
                3,
            )
            .await?)
    }
}

fn spawn_transport(listener: TcpListener, state: &Arc<AppState>) -> JoinHandle<()> {
    let consensus = state.mvcc.consensus_service.clone();
    let replication = state.mvcc.replication_service.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(ConsensusTransportServer::new(consensus))
            .add_service(ReplicationServiceServer::new(replication))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("real MVCC fixture transport");
    })
}

impl Drop for RealMvccCluster {
    fn drop(&mut self) {
        for transport in self.transports.iter_mut().flatten() {
            transport.abort();
        }
    }
}
