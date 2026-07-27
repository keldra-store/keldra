//! Process-backed three-node MVCC cluster fixture.
//!
//! Unlike [`crate::mvcc_cluster::RealMvccCluster`], every node here is an
//! `anvil-server` OS child with its own RocksDB directory. This is intentionally
//! reserved for the small number of crash/restart acceptance tests which need
//! the kernel to tear down every coordinator task and socket at once.

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anvil::anvil_api::{
    AdminRequestContext, BeginTransactionRequest, BeginTransactionResponse,
    CommitTransactionRequest, MvccDurability, MvccReadConsistency,
    ReplaceClusterNodeIncarnationRequest, WriteResponse,
    admin_service_client::AdminServiceClient,
    transaction_service_client::TransactionServiceClient,
};
use anvil_core::{auth::JwtManager, system_realm::SYSTEM_STORAGE_TENANT_ID};
use anyhow::{Context, bail};
use tempfile::TempDir;
use tokio::process::{Child, Command};
use tonic::Request;

const JWT_SECRET: &str = "process-mvcc-fixture-secret";
const ENCRYPTION_KEY: &str =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ADMIN_PRINCIPAL: &str = "process-mvcc-admin";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct ProcessNode {
    api_addr: SocketAddr,
    admin_addr: SocketAddr,
    storage_path: PathBuf,
    incarnation: u64,
    child: Option<Child>,
}

/// Three `anvil-server` children with stable addresses and persistent,
/// independent storage directories.
#[derive(Debug)]
pub struct ProcessMvccCluster {
    _directory: TempDir,
    binary: PathBuf,
    cluster_id: String,
    peers_json: String,
    admin_token: String,
    nodes: Vec<ProcessNode>,
}

impl ProcessMvccCluster {
    pub async fn start(binary: impl AsRef<Path>) -> anyhow::Result<Self> {
        let directory = tempfile::tempdir().context("create process MVCC cluster directory")?;
        let cluster_id = format!("process-e2e-{}", uuid::Uuid::new_v4().simple());
        let mut reserved_addrs = reserve_loopback_addresses(6)?;
        let admin_addrs = reserved_addrs.split_off(3);
        let api_addrs = reserved_addrs;
        let peers_json = serde_json::to_string(
            &api_addrs
                .iter()
                .enumerate()
                .map(|(index, address)| {
                    serde_json::json!({
                        "cluster_id": cluster_id,
                        "raft_node_id": index + 1,
                        "node_id": format!("{cluster_id}-node-{}", index + 1),
                        "incarnation": 1,
                        "endpoint": format!("http://{address}"),
                        "failure_domain": format!("zone-{}", index + 1),
                        "voter": true,
                    })
                })
                .collect::<Vec<_>>(),
        )?;
        let nodes = api_addrs
            .into_iter()
            .zip(admin_addrs)
            .enumerate()
            .map(|(index, (api_addr, admin_addr))| ProcessNode {
                api_addr,
                admin_addr,
                storage_path: directory.path().join(format!("node-{}", index + 1)),
                incarnation: 1,
                child: None,
            })
            .collect();
        let admin_token = JwtManager::new(JWT_SECRET.to_string())
            .mint_token(ADMIN_PRINCIPAL.to_string(), SYSTEM_STORAGE_TENANT_ID)?;
        let mut cluster = Self {
            _directory: directory,
            binary: binary.as_ref().to_path_buf(),
            cluster_id,
            peers_json,
            admin_token,
            nodes,
        };

        // Followers must finish constructing their RPC services before the
        // bootstrap voter attempts to install the initial membership.
        for node in [1_usize, 2] {
            cluster.spawn_node(node).await?;
            cluster.wait_for_admin(node).await?;
        }
        cluster.spawn_node(0).await?;
        cluster.wait_for_admin(0).await?;
        cluster.wait_for_leader(&[0, 1, 2]).await?;
        Ok(cluster)
    }

    pub fn public_endpoint(&self, node: usize) -> String {
        format!("http://{}", self.nodes[node].api_addr)
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Find the current leader using the public linearized transaction API.
    pub async fn wait_for_leader(&self, candidates: &[usize]) -> anyhow::Result<usize> {
        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            for &node in candidates {
                if self
                    .begin_transaction(node, MvccReadConsistency::Linearized)
                    .await
                    .is_ok()
                {
                    return Ok(node);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("no process MVCC leader became available");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn begin_transaction(
        &self,
        node: usize,
        consistency: MvccReadConsistency,
    ) -> anyhow::Result<BeginTransactionResponse> {
        let mut client =
            TransactionServiceClient::connect(self.public_endpoint(node)).await?;
        Ok(client
            .begin_transaction(authorized(
                BeginTransactionRequest {
                    idempotency_key: uuid::Uuid::new_v4().to_string(),
                    ttl_ms: 30_000,
                    read_consistency: consistency as i32,
                    cluster_id: self.cluster_id.clone(),
                    durability: MvccDurability::Quorum as i32,
                },
                &self.admin_token,
            ))
            .await?
            .into_inner())
    }

    pub async fn commit_transaction(
        &self,
        endpoint: String,
        transaction_id: String,
    ) -> anyhow::Result<WriteResponse> {
        let mut client = TransactionServiceClient::connect(endpoint).await?;
        Ok(client
            .commit_transaction(authorized(
                CommitTransactionRequest {
                    transaction_id,
                    cluster_id: self.cluster_id.clone(),
                },
                &self.admin_token,
            ))
            .await?
            .into_inner())
    }

    /// Send SIGKILL to a node, retaining its directory and address for restart.
    pub async fn sigkill(&mut self, node: usize) -> anyhow::Result<()> {
        let mut child = self.nodes[node]
            .child
            .take()
            .context("process MVCC node is not running")?;
        child.start_kill().context("SIGKILL process MVCC node")?;
        child.wait().await.context("reap killed process MVCC node")?;
        Ok(())
    }

    pub async fn restart(&mut self, node: usize) -> anyhow::Result<()> {
        if self.nodes[node].child.is_some() {
            bail!("cannot restart a running process MVCC node");
        }
        self.spawn_node(node).await?;
        self.wait_for_admin(node).await
    }

    /// Start a clean replacement process with the same logical and Raft node
    /// IDs, endpoint and failure domain but a strictly newer incarnation.
    pub async fn spawn_replacement(
        &mut self,
        node: usize,
        incarnation: u64,
    ) -> anyhow::Result<()> {
        if self.nodes[node].child.is_some() {
            bail!("replacement requires the old process to be stopped");
        }
        if incarnation <= self.nodes[node].incarnation {
            bail!("replacement incarnation must advance");
        }
        self.nodes[node].incarnation = incarnation;
        self.nodes[node].storage_path = self
            ._directory
            .path()
            .join(format!("node-{}-incarnation-{incarnation}", node + 1));
        let mut peers: Vec<serde_json::Value> = serde_json::from_str(&self.peers_json)?;
        peers[node]["incarnation"] = serde_json::json!(incarnation);
        self.peers_json = serde_json::to_string(&peers)?;
        self.spawn_node(node).await?;
        self.wait_for_admin(node).await
    }

    /// Apply the authenticated replacement operation to one coordinator.
    /// The leader call installs control; subsequent survivor calls update each
    /// coordinator's local replication route after observing that decision.
    pub async fn apply_replacement(
        &self,
        coordinator: usize,
        replaced_node: usize,
        install_control: bool,
    ) -> anyhow::Result<()> {
        let mut client = AdminServiceClient::connect(format!(
            "http://{}",
            self.nodes[coordinator].admin_addr
        ))
        .await?;
        client
            .replace_cluster_node_incarnation(authorized(
                ReplaceClusterNodeIncarnationRequest {
                    context: Some(AdminRequestContext {
                        request_id: uuid::Uuid::new_v4().to_string(),
                        idempotency_key: uuid::Uuid::new_v4().to_string(),
                        audit_reason: "process MVCC incarnation replacement acceptance".into(),
                        expected_generation: self.nodes[replaced_node]
                            .incarnation
                            .saturating_sub(1),
                    }),
                    cluster_id: self.cluster_id.clone(),
                    raft_node_id: replaced_node as u64 + 1,
                    node_id: format!("{}-node-{}", self.cluster_id, replaced_node + 1),
                    incarnation: self.nodes[replaced_node].incarnation,
                    failure_domain: format!("zone-{}", replaced_node + 1),
                    endpoint: self.public_endpoint(replaced_node),
                    install_control,
                },
                &self.admin_token,
            ))
            .await?;
        Ok(())
    }

    async fn spawn_node(&mut self, node: usize) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.nodes[node].storage_path)?;
        let child = Command::new(&self.binary)
            .env("JWT_SECRET", JWT_SECRET)
            .env("ANVIL_SECRET_ENCRYPTION_KEY", ENCRYPTION_KEY)
            .env("PUBLIC_API_ADDR", self.public_endpoint(node))
            .env("API_LISTEN_ADDR", self.nodes[node].api_addr.to_string())
            .env("ADMIN_LISTEN_ADDR", self.nodes[node].admin_addr.to_string())
            .env("REGION", "process-e2e-region")
            .env("NODE_ID", format!("{}-node-{}", self.cluster_id, node + 1))
            .env("MVCC_RAFT_NODE_ID", (node + 1).to_string())
            .env(
                "MVCC_NODE_INCARNATION",
                self.nodes[node].incarnation.to_string(),
            )
            .env("MVCC_FAILURE_DOMAIN", format!("zone-{}", node + 1))
            .env("MVCC_PEERS_JSON", &self.peers_json)
            .env("MVCC_BOOTSTRAP_MEMBERSHIP", (node == 0).to_string())
            .env("MVCC_RAFT_GROUP_ID", "1")
            .env("MVCC_CLUSTER_ID", &self.cluster_id)
            .env("MVCC_BUNDLE_QUORUM_HOLDERS", "2")
            .env("MVCC_TOLERATED_FAILURE_DOMAINS", "1")
            .env("MVCC_RPC_TIMEOUT_MS", "1000")
            .env("MVCC_NODE_CONNECTION_TOKEN", "process-e2e-node-token")
            .env("STORAGE_PATH", &self.nodes[node].storage_path)
            .env("BOOTSTRAP_SYSTEM_ADMIN_SUBJECT_KIND", "app")
            .env("BOOTSTRAP_SYSTEM_ADMIN_SUBJECT_ID", ADMIN_PRINCIPAL)
            .env(
                "BOOTSTRAP_NODE_IDS",
                (1..=3)
                    .map(|id| format!("{}-node-{id}", self.cluster_id))
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .env("ANVIL_TEST_ALLOW_INSECURE_MVCC_TRANSPORT", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawn {}", self.binary.display()))?;
        self.nodes[node].child = Some(child);
        Ok(())
    }

    async fn wait_for_admin(&mut self, node: usize) -> anyhow::Result<()> {
        let endpoint = format!("http://{}", self.nodes[node].admin_addr);
        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            if self.nodes[node]
                .child
                .as_mut()
                .is_some_and(|child| matches!(child.try_wait(), Ok(Some(_))))
            {
                bail!("process MVCC node {node} exited during startup");
            }
            if let Ok(mut client) = AdminServiceClient::connect(endpoint.clone()).await {
                let request = authorized(
                    anvil::anvil_api::GetLocalNodeDescriptorRequest {},
                    &self.admin_token,
                );
                if client.get_local_node_descriptor(request).await.is_ok() {
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("process MVCC node {node} did not become ready");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for ProcessMvccCluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(child) = &mut node.child {
                let _ = child.start_kill();
            }
        }
    }
}

fn reserve_loopback_addresses(count: usize) -> anyhow::Result<Vec<SocketAddr>> {
    let mut listeners = Vec::with_capacity(count);
    for _ in 0..count {
        listeners.push(StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))?);
    }
    Ok(listeners
        .iter()
        .map(|listener| listener.local_addr())
        .collect::<Result<Vec<_>, _>>()?)
}

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}")
            .parse()
            .expect("fixture token is valid gRPC metadata"),
    );
    request
}
