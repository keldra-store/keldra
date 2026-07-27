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
    public_endpoints: Vec<String>,
    states: Vec<Option<Arc<AppState>>>,
    transports: Vec<Option<JoinHandle<()>>>,
    public_transports: Vec<Option<JoinHandle<()>>>,
}

pub struct PublicActor {
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub principal: String,
    pub token: String,
}

impl RealMvccCluster {
    pub async fn start() -> anyhow::Result<Self> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("warn,anvil_core=debug")
            .try_init();
        let cluster_id = format!("e2e-{}", uuid::Uuid::new_v4().simple());
        let directories = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let mut listeners = Vec::new();
        for _ in 0..3 {
            listeners.push(TcpListener::bind("127.0.0.1:0").await?);
        }
        let mut public_listeners = Vec::new();
        for _ in 0..3 {
            public_listeners.push(TcpListener::bind("127.0.0.1:0").await?);
        }
        let endpoints = listeners
            .iter()
            .map(|listener| format!("http://{}", listener.local_addr().unwrap()))
            .collect::<Vec<_>>();
        let public_endpoints = public_listeners
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
                bootstrap_system_admin_subject_kind: "app".into(),
                bootstrap_system_admin_subject_id: "e2e-admin".into(),
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
        let mut public_listeners = public_listeners.into_iter().map(Some).collect::<Vec<_>>();
        let mut states = vec![None, None, None];
        let mut transports = vec![None, None, None];
        let mut public_transports = vec![None, None, None];
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
            public_transports[index] = Some(spawn_public_api(
                public_listeners[index]
                    .take()
                    .expect("public listener started once"),
                &state,
            ));
            states[index] = Some(state);
        }
        let cluster = Self {
            _directories: directories,
            configs,
            endpoints,
            public_endpoints,
            states,
            transports,
            public_transports,
        };
        cluster.wait_for_any_leader(&[0, 1, 2]).await?;
        cluster.wait_for_system_realm().await?;
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

    pub fn public_endpoint(&self, node: usize) -> &str {
        &self.public_endpoints[node]
    }

    pub fn node_index(&self, node_id: &str) -> Option<usize> {
        self.configs
            .iter()
            .position(|config| config.node_id == node_id)
    }

    pub fn replication_transfer_path(
        &self,
        node: usize,
        transfer_id: uuid::Uuid,
    ) -> std::path::PathBuf {
        std::path::Path::new(&self.configs[node].storage_path)
            .join("mvcc")
            .join(&self.configs[node].mvcc_cluster_id)
            .join("replication-inbox")
            .join(format!("{transfer_id}.complete"))
    }

    pub fn remove_replication_transfer(
        &self,
        node: usize,
        transfer_id: uuid::Uuid,
    ) -> anyhow::Result<()> {
        std::fs::remove_file(self.replication_transfer_path(node, transfer_id))?;
        Ok(())
    }

    /// Simulates a bidirectional network partition for this node.
    pub fn partition(&mut self, node: usize) {
        if let Some(transport) = self.transports[node].take() {
            transport.abort();
        }
        if let Some(transport) = self.public_transports[node].take() {
            transport.abort();
        }
    }

    /// Reopens the node from the same RocksDB directory and network identity.
    pub async fn restart_node(&mut self, node: usize) -> anyhow::Result<()> {
        if let Some(transport) = self.transports[node].take() {
            transport.abort();
            let _ = transport.await;
        }
        if let Some(transport) = self.public_transports[node].take() {
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
        let public_listener =
            TcpListener::bind(self.public_endpoints[node].trim_start_matches("http://")).await?;
        self.public_transports[node] = Some(spawn_public_api(public_listener, &state));
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

    pub async fn bootstrap_public_actor(
        &self,
        node: usize,
        bucket_name: &str,
    ) -> anyhow::Result<PublicActor> {
        let state = self.state(node);
        state.persistence.create_region("e2e-region").await?;
        let tenant = state
            .persistence
            .create_tenant("e2e-tenant", "e2e-tenant-key")
            .await?;
        let encrypted_secret = state.secret_keyring.encrypt(b"e2e-app-secret")?;
        let app = state
            .persistence
            .create_app(
                tenant.id,
                "e2e-app",
                "e2e-app",
                &encrypted_secret,
                None,
                None,
            )
            .await?;
        anvil_core::access_control::grant_storage_tenant_owner(
            &state.persistence,
            tenant.id,
            &app.id.to_string(),
            "e2e-fixture",
            "grant fixture actor storage ownership",
        )
        .await?;
        let bucket = state
            .persistence
            .create_bucket(tenant.id, bucket_name, "e2e-region")
            .await?;
        let token = state
            .jwt_manager
            .mint_token(app.id.to_string(), tenant.id)?;
        Ok(PublicActor {
            tenant_id: tenant.id,
            bucket_id: bucket.id,
            bucket_name: bucket.name,
            principal: app.id.to_string(),
            token,
        })
    }

    pub async fn wait_for_applied_version(&self, node: usize, version: u64) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if self.state(node).mvcc.runtime.applied_version()? >= version {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await?
    }

    async fn wait_for_system_realm(&self) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if (0..3).all(|node| {
                    self.state(node)
                        .system_realm_is_bootstrapped()
                        .unwrap_or(false)
                }) {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("system realm did not become visible on every cluster node"))?
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

fn spawn_public_api(listener: TcpListener, state: &Arc<AppState>) -> JoinHandle<()> {
    let app_state = state.as_ref().clone();
    let auth_state = app_state.clone();
    let interceptor = anvil_core::services::AuthInterceptorFn::new(move |request| {
        anvil_core::middleware::auth_interceptor(request, &auth_state)
    });
    let routes = anvil_core::services::create_grpc_router(app_state, interceptor);
    tokio::spawn(async move {
        Server::builder()
            .add_routes(routes)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("real MVCC fixture public API");
    })
}

impl Drop for RealMvccCluster {
    fn drop(&mut self) {
        for transport in self.transports.iter_mut().flatten() {
            transport.abort();
        }
        for transport in self.public_transports.iter_mut().flatten() {
            transport.abort();
        }
    }
}
