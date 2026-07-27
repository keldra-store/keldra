//! Docker-free real OpenRaft and gRPC replication cluster fixture.

use std::{sync::Arc, time::Duration};

use anvil_core::config::Config;
use anvil_core::{
    AppState,
    anvil_api::{
        consensus_transport_server::ConsensusTransportServer,
        replication_service_server::ReplicationServiceServer,
    },
    mesh_lifecycle::{
        BootstrapMeshLifecycleProjection, CreateRegionDescriptor, LifecycleState, NodeCapability,
        RegisterCellDescriptor, RegisterNodeDescriptor, install_bootstrap_lifecycle_projection,
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
                public_api_addr: public_endpoints[index].clone(),
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
                mvcc_prepared_bundle_gc_grace_ms: 1,
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
        cluster.bootstrap_active_topology().await?;
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

    pub fn replication_transfer_metadata_path(
        &self,
        node: usize,
        transfer_id: uuid::Uuid,
    ) -> std::path::PathBuf {
        std::path::Path::new(&self.configs[node].storage_path)
            .join("mvcc")
            .join(&self.configs[node].mvcc_cluster_id)
            .join("replication-inbox")
            .join(format!("{transfer_id}.meta"))
    }

    pub fn remove_replication_transfer(
        &self,
        node: usize,
        transfer_id: uuid::Uuid,
    ) -> anyhow::Result<()> {
        std::fs::remove_file(self.replication_transfer_path(node, transfer_id))?;
        Ok(())
    }

    pub fn corrupt_replication_transfer(
        &self,
        node: usize,
        transfer_id: uuid::Uuid,
    ) -> anyhow::Result<()> {
        use std::io::{Read, Seek, SeekFrom, Write};

        let path = self.replication_transfer_path(node, transfer_id);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        let mut first = [0_u8; 1];
        file.read_exact(&mut first)?;
        first[0] ^= 0xff;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&first)?;
        file.sync_all()?;
        Ok(())
    }

    /// Blocks both inbound and outbound consensus/replication links while
    /// leaving the node and its public API alive.
    pub fn partition(&self, node: usize) {
        let cluster_id = &self.configs[node].mvcc_cluster_id;
        anvil_core::cluster_transport_fault::partition_node(
            cluster_id,
            format!("raft:{}", self.configs[node].mvcc_raft_node_id),
        );
        anvil_core::cluster_transport_fault::partition_node(
            cluster_id,
            self.configs[node].node_id.clone(),
        );
    }

    pub fn heal(&self, node: usize) {
        let cluster_id = &self.configs[node].mvcc_cluster_id;
        anvil_core::cluster_transport_fault::heal_node(
            cluster_id,
            &format!("raft:{}", self.configs[node].mvcc_raft_node_id),
        );
        anvil_core::cluster_transport_fault::heal_node(cluster_id, &self.configs[node].node_id);
    }

    /// Blocks only replication traffic for a node, leaving its Raft transport
    /// connected so tests can observe a committed decision ahead of its local
    /// bundle/application watermark.
    pub fn partition_replication(&self, node: usize) {
        anvil_core::cluster_transport_fault::partition_node(
            &self.configs[node].mvcc_cluster_id,
            self.configs[node].node_id.clone(),
        );
    }

    pub fn heal_replication(&self, node: usize) {
        anvil_core::cluster_transport_fault::heal_node(
            &self.configs[node].mvcc_cluster_id,
            &self.configs[node].node_id,
        );
    }

    /// Reopens the node from the same RocksDB directory and network identity.
    pub async fn restart_node(&mut self, node: usize) -> anyhow::Result<()> {
        self.heal(node);
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
        // These persistence futures are intentionally boxed. In debug builds
        // several of them retain large MVCC mutation plans; embedding every
        // future directly in this fixture future made the generated poll frame
        // large enough to overflow libtest's default thread stack.
        Box::pin(state.persistence.create_region("e2e-region")).await?;
        let tenant = Box::pin(
            state
                .persistence
                .create_tenant("e2e-tenant", "e2e-tenant-key"),
        )
        .await?;
        let encrypted_secret = state.secret_keyring.encrypt(b"e2e-app-secret")?;
        let app = Box::pin(state.persistence.create_app(
            tenant.id,
            "e2e-app",
            "e2e-app",
            &encrypted_secret,
            None,
            None,
        ))
        .await?;
        Box::pin(anvil_core::access_control::grant_storage_tenant_owner(
            &state.persistence,
            tenant.id,
            &app.id.to_string(),
            "e2e-fixture",
            "grant fixture actor storage ownership",
        ))
        .await?;
        let bucket = Box::pin(state.persistence.create_bucket(
            tenant.id,
            bucket_name,
            "e2e-region",
        ))
        .await?;
        Box::pin(anvil_core::access_control::grant_bucket_defaults(
            &state.persistence,
            &bucket,
            &app.id.to_string(),
            "e2e-fixture",
            "grant fixture actor bucket ownership",
        ))
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

    pub async fn wait_for_readable_version(&self, node: usize, version: u64) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if self
                    .state(node)
                    .mvcc
                    .runtime
                    .local_store()
                    .readable_version()?
                    >= version
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await?
    }

    pub async fn wait_for_gc_watermark(&self, node: usize, version: u64) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if self.state(node).mvcc.runtime.local_store().gc_watermark()? >= version {
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

    async fn bootstrap_active_topology(&self) -> anyhow::Result<()> {
        let coordinator = self.wait_for_any_leader(&[0, 1, 2]).await?;
        let state = self.state(coordinator);
        let region_id = self.configs[coordinator].region.clone();
        let cell_id = self.configs[coordinator].cell_id.clone();
        let mesh_id = self.configs[coordinator].mesh_id.clone();

        let node_keys = self
            .states
            .iter()
            .flatten()
            .map(|state| {
                (
                    state.config.node_id.clone(),
                    state.core_store.local_receipt_signing_public_key(),
                )
            })
            .collect::<Vec<_>>();
        let region_input = CreateRegionDescriptor {
            mesh_id: mesh_id.clone(),
            region: region_id.clone(),
            public_base_url: self.public_endpoints[coordinator].clone(),
            virtual_host_suffix: format!("{region_id}.test.invalid"),
            placement_weight: 100,
            default_cell: Some(cell_id.clone()),
        };
        let cell_input = RegisterCellDescriptor {
            mesh_id: mesh_id.clone(),
            region: region_id.clone(),
            cell_id: cell_id.clone(),
            placement_weight: 100,
            failure_domain: cell_id.clone(),
        };
        let node_inputs = self
            .configs
            .iter()
            .enumerate()
            .map(|(index, config)| RegisterNodeDescriptor {
                mesh_id: mesh_id.clone(),
                node_id: config.node_id.clone(),
                region: region_id.clone(),
                cell_id: cell_id.clone(),
                receipt_signing_public_key: node_keys[index].1.clone(),
                public_api_addr: config.public_api_addr.clone(),
                capabilities: vec![
                    NodeCapability::Object,
                    NodeCapability::Index,
                    NodeCapability::PersonalDb,
                    NodeCapability::Metadata,
                    NodeCapability::Gateway,
                    NodeCapability::Admin,
                ],
                capacity_json: "{}".to_string(),
            })
            .collect::<Vec<_>>();
        let physical_projection = BootstrapMeshLifecycleProjection {
            regions: vec![region_input.clone()],
            cells: vec![cell_input.clone()],
            nodes: node_inputs.clone(),
        };
        for target in self.states.iter().flatten() {
            for (node_id, key) in &node_keys {
                target
                    .core_store
                    .register_node_receipt_signing_public_key(node_id, key)?;
            }
            install_bootstrap_lifecycle_projection(
                &target.storage,
                &target.core_store,
                physical_projection.clone(),
            )?;
        }

        let mut region = state
            .persistence
            .create_region_descriptor(region_input)
            .await?;
        let mut cell = state
            .persistence
            .register_cell_descriptor(cell_input)
            .await?;
        cell = state
            .persistence
            .transition_cell_descriptor(
                &region_id,
                &cell_id,
                cell.generation,
                LifecycleState::Active,
            )
            .await?;

        region = state
            .persistence
            .transition_region_descriptor(&region_id, region.generation, LifecycleState::Active)
            .await?;

        for input in node_inputs {
            let mut node = state.persistence.register_node_descriptor(input).await?;
            node = state
                .persistence
                .transition_node_descriptor(
                    &node.node_id,
                    node.generation,
                    LifecycleState::Active,
                    None,
                )
                .await?;
            debug_assert_eq!(node.state, LifecycleState::Active);
        }
        debug_assert_eq!(region.state, LifecycleState::Active);
        debug_assert_eq!(cell.state, LifecycleState::Active);

        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let mut converged = true;
                for node in 0..self.states.len() {
                    let persistence = &self.state(node).persistence;
                    let regions = persistence.list_region_descriptors().await?;
                    let cells = persistence.list_cell_descriptors(Some(&region_id)).await?;
                    let nodes = persistence
                        .list_node_descriptors(Some(&region_id), Some(&cell_id))
                        .await?;
                    converged &= regions.iter().any(|region| {
                        region.region == region_id && region.state == LifecycleState::Active
                    });
                    converged &= cells.iter().any(|cell| {
                        cell.cell_id == cell_id && cell.state == LifecycleState::Active
                    });
                    converged &= self.configs.iter().all(|config| {
                        nodes.iter().any(|node| {
                            node.node_id == config.node_id && node.state == LifecycleState::Active
                        })
                    });
                }
                if converged {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!("mesh topology did not become visible on every cluster node")
        })?
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
    let app = anvil_core::services::create_axum_router(routes);
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .expect("real MVCC fixture public API");
    })
}

impl Drop for RealMvccCluster {
    fn drop(&mut self) {
        for node in 0..self.configs.len() {
            self.heal(node);
        }
        for transport in self.transports.iter_mut().flatten() {
            transport.abort();
        }
        for transport in self.public_transports.iter_mut().flatten() {
            transport.abort();
        }
    }
}
