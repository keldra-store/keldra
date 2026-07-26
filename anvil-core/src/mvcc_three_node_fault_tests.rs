use std::{sync::Arc, time::Duration};

use anvil_mvcc_consensus::Consensus as _;
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::{
    AppState, Config,
    anvil_api::{
        consensus_transport_server::ConsensusTransportServer,
        replication_service_server::ReplicationServiceServer,
    },
    mvcc_transaction::{DurabilityLevel, LogicalKey, ReadConsistency},
    personaldb_signing,
};

struct ThreeNodeFixture {
    _directories: Vec<TempDir>,
    states: Vec<Arc<AppState>>,
    servers: Vec<Option<JoinHandle<()>>>,
}

impl ThreeNodeFixture {
    async fn start() -> Self {
        let directories = (0..3)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();
        let listeners = [
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
            TcpListener::bind("127.0.0.1:0").await.unwrap(),
        ];
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
                        "cluster_id": "fault-e2e",
                        "raft_node_id": index + 1,
                        "node_id": format!("node-{}", index + 1),
                        "incarnation": 1,
                        "endpoint": endpoint,
                        "failure_domain": format!("zone-{}", index + 1),
                        "voter": true,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let mut states = Vec::new();
        for (index, directory) in directories.iter().enumerate() {
            let config = Config {
                jwt_secret: "fault-secret".into(),
                anvil_secret_encryption_key:
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                public_api_addr: "127.0.0.1:0".into(),
                api_listen_addr: "127.0.0.1:0".into(),
                region: "fault".into(),
                node_id: format!("node-{}", index + 1),
                bootstrap_system_admin_subject_kind: "app".into(),
                bootstrap_system_admin_subject_id: "admin-principal".into(),
                allow_test_only_embedding_provider: true,
                bootstrap_node_ids: vec!["node-1".into(), "node-2".into(), "node-3".into()],
                storage_path: directory
                    .path()
                    .join("storage")
                    .to_string_lossy()
                    .into_owned(),
                mvcc_cluster_id: "fault-e2e".into(),
                mvcc_raft_node_id: index as u64 + 1,
                mvcc_node_incarnation: 1,
                mvcc_failure_domain: format!("zone-{}", index + 1),
                mvcc_peers_json: peers_json.clone(),
                mvcc_bootstrap_membership: index == 0,
                mvcc_bundle_quorum_holders: 2,
                mvcc_tolerated_failure_domains: 1,
                mvcc_rpc_timeout_ms: 1_000,
                allow_test_only_insecure_mvcc_transport: true,
                ..Config::default()
            };
            states.push(Arc::new(
                AppState::new(
                    config,
                    personaldb_signing::PersonalDbProtocolKeyring::disabled(),
                )
                .await
                .unwrap(),
            ));
        }
        let mut servers = Vec::new();
        for (listener, state) in listeners.into_iter().zip(&states) {
            let consensus = state.mvcc.consensus_service.clone();
            let replication = state.mvcc.replication_service.clone();
            servers.push(Some(tokio::spawn(async move {
                Server::builder()
                    .add_service(ConsensusTransportServer::new(consensus))
                    .add_service(ReplicationServiceServer::new(replication))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            })));
        }
        let fixture = Self {
            _directories: directories,
            states,
            servers,
        };
        fixture.wait_for_leader(0).await;
        fixture
    }

    async fn wait_for_leader(&self, node: usize) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if self.states[node]
                    .mvcc
                    .consensus
                    .linearized_read_barrier()
                    .await
                    .is_ok()
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cluster elects requested leader");
    }

    async fn wait_for_any_leader(&self, nodes: &[usize]) -> usize {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                for node in nodes {
                    if self.states[*node]
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
        .await
        .expect("surviving majority elects a leader")
    }

    fn stop_transport(&mut self, node: usize) {
        self.servers[node].take().unwrap().abort();
    }

    async fn write(&self, node: usize, id: &str, key: LogicalKey) {
        let principal = "fault-principal";
        let handle = self.states[node]
            .mvcc
            .open_transactions
            .begin(
                self.states[node].mvcc.runtime.as_ref(),
                "fault-e2e",
                principal,
                id,
                Duration::from_secs(30),
                DurabilityLevel::Quorum,
                ReadConsistency::Linearized,
                1,
            )
            .await
            .unwrap();
        self.states[node]
            .mvcc
            .open_transactions
            .put(
                &handle.transaction_id,
                "fault-e2e",
                key,
                id.as_bytes().to_vec(),
                2,
            )
            .unwrap();
        self.states[node]
            .mvcc
            .open_transactions
            .commit(
                self.states[node].mvcc.runtime.as_ref(),
                &handle.transaction_id,
                principal,
                3,
            )
            .await
            .unwrap();
    }
}

impl Drop for ThreeNodeFixture {
    fn drop(&mut self) {
        for server in self.servers.iter_mut().flatten() {
            server.abort();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn quorum_commit_survives_one_minority_transport_loss() {
    let mut cluster = ThreeNodeFixture::start().await;
    cluster.stop_transport(2);
    cluster
        .write(
            0,
            "minority-loss",
            LogicalKey {
                table_id: 1,
                application_key: b"minority".to_vec(),
            },
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn majority_elects_and_commits_after_leader_transport_loss() {
    let mut cluster = ThreeNodeFixture::start().await;
    cluster.states[0].mvcc.consensus.shutdown().await.unwrap();
    cluster.stop_transport(0);
    let leader = cluster.wait_for_any_leader(&[1, 2]).await;
    cluster
        .write(
            leader,
            "leader-change",
            LogicalKey {
                table_id: 1,
                application_key: b"new-leader".to_vec(),
            },
        )
        .await;
}
