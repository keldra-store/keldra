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
    BootstrapMeshTopologyRequest, CheckPermissionRequest, CommitTransactionRequest,
    CreateBucketRequest, CreateIndexRequest, CreatePersonalDbGroupRequest, GetGitBlobByPathRequest,
    GetLocalNodeDescriptorRequest, GetObjectRequest, GetPersonalDbGroupRequest,
    GetTransactionRequest, GitBlobLocation, GitPackMetadata, HeadObjectRequest,
    IndexDefinitionRecord, IndexKind, ListIndexesRequest, ListRoutingRecordsRequest,
    MutationBatchOperation, MutationBatchPutObject, MutationBatchRequest, MutationBatchResponse,
    MvccDurability, MvccReadConsistency, NativeMutationContext, NodeCapability,
    PersonalDbGroupResponse, PersonalDbVoterAck, PutCellRequest, PutGitPackRequest,
    PutGitPackResponse, PutNodeRequest, PutRegionRequest, QueryIndexRequest, QueryIndexResponse,
    ReadAuthzTuplesRequest, ReadConsistency, ReplaceClusterNodeIncarnationRequest,
    RoutingRecordFamily, SubmitPersonalDbChangesetRequest, SubmitPersonalDbChangesetResponse,
    TransactionStatus, WatchGitSourceRequest, WatchGitSourceResponse, WriteOptions, WriteResponse,
    admin_service_client::AdminServiceClient, auth_service_client::AuthServiceClient,
    bucket_service_client::BucketServiceClient, git_source_service_client::GitSourceServiceClient,
    index_service_client::IndexServiceClient,
    mesh_control_service_client::MeshControlServiceClient, mutation_batch_operation,
    object_service_client::ObjectServiceClient,
    personal_db_service_client::PersonalDbServiceClient, put_git_pack_request,
    transaction_service_client::TransactionServiceClient, write_options,
};
use anvil_core::{
    auth::JwtManager, services::consensus_transport::TonicConsensusRpcFactory,
    system_realm::SYSTEM_STORAGE_TENANT_ID,
};
use anvil_mvcc_consensus::{
    ConsensusNode, ConsensusRpc, ConsensusRpcFactory as _, ConsensusRpcKind, NodeId,
};
use anyhow::{Context, bail};
use tempfile::TempDir;
use tokio::process::{Child, Command};
use tonic::Request;

const JWT_SECRET: &str = "process-mvcc-fixture-secret";
const ENCRYPTION_KEY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ADMIN_PRINCIPAL: &str = "process-mvcc-admin";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct ProcessNode {
    api_addr: SocketAddr,
    admin_addr: SocketAddr,
    storage_path: PathBuf,
    incarnation: u64,
    hard_crash_control_path: PathBuf,
    child: Option<Child>,
}

#[derive(Debug)]
struct ObsoleteNode {
    storage_path: PathBuf,
    peers_json: String,
    incarnation: u64,
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
    obsolete_nodes: Vec<Option<ObsoleteNode>>,
    obsolete_children: Vec<Child>,
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
                hard_crash_control_path: directory
                    .path()
                    .join(format!("node-{}-hard-crash", index + 1)),
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
            obsolete_nodes: (0..3).map(|_| None).collect(),
            obsolete_children: Vec::new(),
        };

        // Followers must finish constructing their RPC services before the
        // bootstrap voter attempts to install the initial membership.
        for node in [1_usize, 2] {
            cluster.spawn_node(node).await?;
            cluster.wait_for_admin_transport(node).await?;
        }
        cluster.spawn_node(0).await?;
        cluster.wait_for_admin_transport(0).await?;
        let coordinator = cluster.wait_for_leader(&[0, 1, 2]).await?;
        for node in 0..3 {
            cluster.wait_for_admin(node).await?;
        }
        cluster.bootstrap_cluster_topology(coordinator).await?;
        Ok(cluster)
    }

    pub fn public_endpoint(&self, node: usize) -> String {
        format!("http://{}", self.nodes[node].api_addr)
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Find the current leader through OpenRaft's leader-local linearized read
    /// barrier.
    ///
    /// The public transaction API deliberately forwards linearized reads from
    /// followers, so a successful `BeginTransaction` only proves that a leader
    /// is reachable. Sending the existing internal forward-read RPC directly
    /// to each candidate invokes `linearized_read_barrier_locally` on that
    /// process and succeeds only on the current leader.
    pub async fn wait_for_leader(&self, candidates: &[usize]) -> anyhow::Result<usize> {
        let source = candidates
            .iter()
            .copied()
            .find(|&node| {
                self.nodes
                    .get(node)
                    .is_some_and(|candidate| candidate.child.is_some())
            })
            .context("leader wait requires at least one live candidate")?;
        let factory = TonicConsensusRpcFactory::new(
            self.cluster_id.clone(),
            NodeId(source as u64 + 1),
            self.nodes[source].incarnation,
            "process-e2e-node-token",
            Duration::from_secs(1),
        );
        let probe_payload = bincode::serde::encode_to_vec((), bincode::config::standard())
            .context("encode process MVCC leader probe")?;
        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            for &node in candidates {
                let Some(candidate) = self.nodes.get(node) else {
                    bail!("leader candidate index {node} is out of bounds");
                };
                if candidate.child.is_none() {
                    continue;
                }
                let mut client = factory.client(
                    NodeId(node as u64 + 1),
                    &ConsensusNode {
                        address: self.public_endpoint(node),
                    },
                );
                if client
                    .request(ConsensusRpc {
                        schema_version: 1,
                        kind: ConsensusRpcKind::ForwardLinearizedRead,
                        payload: probe_payload.clone(),
                    })
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
        self.begin_transaction_with_durability(node, consistency, MvccDurability::Quorum)
            .await
    }

    pub async fn begin_transaction_with_durability(
        &self,
        node: usize,
        consistency: MvccReadConsistency,
        durability: MvccDurability,
    ) -> anyhow::Result<BeginTransactionResponse> {
        self.begin_transaction_at_with_durability(
            self.public_endpoint(node),
            consistency,
            durability,
        )
        .await
    }

    pub async fn begin_transaction_at(
        &self,
        endpoint: String,
        consistency: MvccReadConsistency,
    ) -> anyhow::Result<BeginTransactionResponse> {
        self.begin_transaction_at_with_durability(endpoint, consistency, MvccDurability::Quorum)
            .await
    }

    async fn begin_transaction_at_with_durability(
        &self,
        endpoint: String,
        consistency: MvccReadConsistency,
        durability: MvccDurability,
    ) -> anyhow::Result<BeginTransactionResponse> {
        let mut client = TransactionServiceClient::connect(endpoint).await?;
        Ok(client
            .begin_transaction(authorized(
                BeginTransactionRequest {
                    idempotency_key: uuid::Uuid::new_v4().to_string(),
                    ttl_ms: 30_000,
                    read_consistency: consistency as i32,
                    cluster_id: self.cluster_id.clone(),
                    durability: durability as i32,
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

    pub async fn get_transaction(
        &self,
        node: usize,
        transaction_id: &str,
    ) -> anyhow::Result<TransactionStatus> {
        let mut client = TransactionServiceClient::connect(self.public_endpoint(node)).await?;
        Ok(client
            .get_transaction(authorized(
                GetTransactionRequest {
                    transaction_id: transaction_id.to_string(),
                    cluster_id: self.cluster_id.clone(),
                },
                &self.admin_token,
            ))
            .await?
            .into_inner())
    }

    pub async fn create_bucket(&self, node: usize, bucket_name: &str) -> anyhow::Result<i64> {
        let mut client = BucketServiceClient::connect(self.public_endpoint(node)).await?;
        Ok(client
            .create_bucket(authorized(
                CreateBucketRequest {
                    bucket_name: bucket_name.to_string(),
                    region: "process-e2e-region".to_string(),
                    options: None,
                },
                &self.admin_token,
            ))
            .await?
            .into_inner()
            .bucket_id)
    }

    pub async fn stage_bucket_create(
        &self,
        node: usize,
        bucket_name: &str,
        transaction_id: &str,
    ) -> anyhow::Result<i64> {
        let mut client = BucketServiceClient::connect(self.public_endpoint(node)).await?;
        Ok(client
            .create_bucket(authorized(
                CreateBucketRequest {
                    bucket_name: bucket_name.to_string(),
                    region: "process-e2e-region".to_string(),
                    options: Some(WriteOptions {
                        idempotency_key: format!("process-bucket-{bucket_name}"),
                        consistency: 0,
                        wait_for_finalization: false,
                        preconditions: Vec::new(),
                        boundary_values: Vec::new(),
                        execution: Some(write_options::Execution::TransactionId(
                            transaction_id.to_string(),
                        )),
                    }),
                },
                &self.admin_token,
            ))
            .await?
            .into_inner()
            .bucket_id)
    }

    pub async fn bucket_locator_record_count(
        &self,
        node: usize,
        bucket_name: &str,
    ) -> anyhow::Result<usize> {
        let mut client =
            AdminServiceClient::connect(format!("http://{}", self.nodes[node].admin_addr)).await?;
        Ok(client
            .list_routing_records(authorized(
                ListRoutingRecordsRequest {
                    family: RoutingRecordFamily::BucketLocator as i32,
                    page: None,
                },
                &self.admin_token,
            ))
            .await?
            .into_inner()
            .records
            .into_iter()
            .filter(|record| record.record_key.ends_with(&format!("/{bucket_name}")))
            .count())
    }

    pub async fn create_personaldb_group(
        &self,
        node: usize,
        database_id: &str,
        schema_sql: &str,
    ) -> anyhow::Result<PersonalDbGroupResponse> {
        let mut client = PersonalDbServiceClient::connect(self.public_endpoint(node)).await?;
        let genesis_hash = hex::encode(anvil::formats::hash32(
            format!("genesis:{database_id}").as_bytes(),
        ));
        Ok(client
            .create_personal_db_group(authorized(
                CreatePersonalDbGroupRequest {
                    database_id: database_id.to_string(),
                    schema_hash: hex::encode(anvil::formats::hash32(schema_sql.as_bytes())),
                    genesis_hash,
                    schema_sql: schema_sql.to_string(),
                    options: None,
                },
                &self.admin_token,
            ))
            .await?
            .into_inner())
    }

    pub async fn stage_git_pack(
        &self,
        node: usize,
        repository_id: &str,
        bucket_name: &str,
        pack: Vec<u8>,
        transaction_id: &str,
    ) -> anyhow::Result<PutGitPackResponse> {
        let mut client = GitSourceServiceClient::connect(self.public_endpoint(node)).await?;
        let request = authorized(
            tokio_stream::iter(vec![
                PutGitPackRequest {
                    data: Some(put_git_pack_request::Data::Metadata(GitPackMetadata {
                        repository_id: repository_id.to_string(),
                        bucket_name: bucket_name.to_string(),
                        options: Some(WriteOptions {
                            idempotency_key: format!("process-git-{repository_id}"),
                            consistency: 0,
                            wait_for_finalization: false,
                            preconditions: Vec::new(),
                            boundary_values: Vec::new(),
                            execution: Some(write_options::Execution::TransactionId(
                                transaction_id.to_string(),
                            )),
                        }),
                    })),
                },
                PutGitPackRequest {
                    data: Some(put_git_pack_request::Data::Chunk(pack)),
                },
            ]),
            &self.admin_token,
        );
        Ok(client.put_git_pack(request).await?.into_inner())
    }

    pub async fn get_git_blob_by_path(
        &self,
        node: usize,
        repository_id: &str,
        commit_id: &str,
        tree_path: &str,
    ) -> anyhow::Result<GitBlobLocation> {
        let mut client = GitSourceServiceClient::connect(self.public_endpoint(node)).await?;
        client
            .get_git_blob_by_path(authorized(
                GetGitBlobByPathRequest {
                    repository_id: repository_id.to_string(),
                    commit_id: commit_id.to_string(),
                    tree_path: tree_path.to_string(),
                },
                &self.admin_token,
            ))
            .await?
            .into_inner()
            .location
            .context("GitSource blob path is missing")
    }

    pub async fn git_source_watch_events(
        &self,
        node: usize,
        repository_id: &str,
        collection_window: Duration,
    ) -> anyhow::Result<Vec<WatchGitSourceResponse>> {
        use futures_util::StreamExt;

        let mut client = GitSourceServiceClient::connect(self.public_endpoint(node)).await?;
        let mut stream = client
            .watch_git_source(authorized(
                WatchGitSourceRequest {
                    repository_id: repository_id.to_string(),
                    after_cursor_low: 0,
                    after_cursor_high: 0,
                },
                &self.admin_token,
            ))
            .await?
            .into_inner();
        let mut events = Vec::new();
        let deadline = tokio::time::Instant::now() + collection_window;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(event))) => events.push(event),
                Ok(Some(Err(error))) => return Err(error.into()),
                Ok(None) | Err(_) => break,
            }
        }
        Ok(events)
    }

    pub async fn stage_personaldb_submit(
        &self,
        node: usize,
        database_id: &str,
        genesis_hash: &str,
        changeset_bytes: Vec<u8>,
        transaction_id: &str,
    ) -> anyhow::Result<SubmitPersonalDbChangesetResponse> {
        let mut client = PersonalDbServiceClient::connect(self.public_endpoint(node)).await?;
        Ok(client
            .submit_personal_db_changeset(authorized(
                SubmitPersonalDbChangesetRequest {
                    tenant_id: SYSTEM_STORAGE_TENANT_ID,
                    database_id: database_id.to_string(),
                    principal: ADMIN_PRINCIPAL.to_string(),
                    session_token: self.admin_token.clone(),
                    request_id: format!("process-submit-{database_id}"),
                    idempotency_key: format!("process-submit-{database_id}"),
                    base_log_index: 0,
                    base_log_hash: genesis_hash.to_string(),
                    client_log_epoch: 1,
                    membership_epoch: 1,
                    policy_epoch: 1,
                    leader_replica_id: ADMIN_PRINCIPAL.to_string(),
                    voter_acks: vec![PersonalDbVoterAck {
                        replica_id: ADMIN_PRINCIPAL.to_string(),
                        log_index: 1,
                        log_hash: hex::encode(anvil::formats::hash32(&changeset_bytes)),
                        signature: "process-fixture".to_string(),
                    }],
                    changeset_payload_hash: hex::encode(anvil::formats::hash32(&changeset_bytes)),
                    changeset_bytes,
                    client_debug_metadata_json: String::new(),
                    options: Some(WriteOptions {
                        idempotency_key: format!("process-submit-options-{database_id}"),
                        consistency: 0,
                        wait_for_finalization: false,
                        preconditions: Vec::new(),
                        boundary_values: Vec::new(),
                        execution: Some(write_options::Execution::TransactionId(
                            transaction_id.to_string(),
                        )),
                    }),
                },
                &self.admin_token,
            ))
            .await?
            .into_inner())
    }

    pub async fn get_personaldb_group(
        &self,
        node: usize,
        database_id: &str,
    ) -> anyhow::Result<PersonalDbGroupResponse> {
        let mut client = PersonalDbServiceClient::connect(self.public_endpoint(node)).await?;
        Ok(client
            .get_personal_db_group(authorized(
                GetPersonalDbGroupRequest {
                    tenant_id: SYSTEM_STORAGE_TENANT_ID,
                    database_id: database_id.to_string(),
                },
                &self.admin_token,
            ))
            .await?
            .into_inner())
    }

    pub async fn personaldb_row_owner_tuple_count(
        &self,
        node: usize,
        database_id: &str,
        resource_type: &str,
        resource_id: &str,
    ) -> anyhow::Result<usize> {
        let mut client = AuthServiceClient::connect(self.public_endpoint(node)).await?;
        let namespace = anvil_core::authz_scope::encode_realm_namespace(
            anvil_core::authz_scope::DEFAULT_AUTHZ_REALM_ID,
            "personaldb_row",
        );
        let object_id = format!(
            "tenant-{SYSTEM_STORAGE_TENANT_ID}/{database_id}/{resource_type}/{resource_id}"
        );
        let mut count = 0;
        for relation in [
            "personaldb:insert",
            "personaldb:update",
            "personaldb:delete",
        ] {
            count += client
                .read_authz_tuples(authorized(
                    ReadAuthzTuplesRequest {
                        namespace: namespace.clone(),
                        object_id: object_id.clone(),
                        relation: relation.to_string(),
                        subject_kind: anvil_core::access_control::APP_SUBJECT_KIND.to_string(),
                        subject_id: ADMIN_PRINCIPAL.to_string(),
                        caveat_hash: String::new(),
                        consistency: "latest".to_string(),
                        zookie: String::new(),
                        page_size: 10,
                        page_token: String::new(),
                        scope: None,
                    },
                    &self.admin_token,
                ))
                .await?
                .into_inner()
                .tuples
                .len();
        }
        Ok(count)
    }

    pub async fn stage_path_index(
        &self,
        node: usize,
        bucket_name: &str,
        index_name: &str,
        transaction_id: &str,
    ) -> anyhow::Result<IndexDefinitionRecord> {
        let mut client = IndexServiceClient::connect(self.public_endpoint(node)).await?;
        Ok(client
            .create_index(authorized(
                CreateIndexRequest {
                    bucket_name: bucket_name.to_string(),
                    name: index_name.to_string(),
                    kind: IndexKind::Path as i32,
                    selector_json: serde_json::json!({"prefix": ""}).to_string(),
                    extractor_json: "{}".to_string(),
                    authorization_mode: "inherit_object".to_string(),
                    build_policy_json: "{}".to_string(),
                    options: Some(WriteOptions {
                        idempotency_key: uuid::Uuid::new_v4().to_string(),
                        consistency: 0,
                        wait_for_finalization: false,
                        preconditions: Vec::new(),
                        boundary_values: Vec::new(),
                        execution: Some(write_options::Execution::TransactionId(
                            transaction_id.to_string(),
                        )),
                    }),
                },
                &self.admin_token,
            ))
            .await?
            .into_inner()
            .index
            .context("create index response omitted index")?)
    }

    pub async fn list_indexes(
        &self,
        node: usize,
        bucket_name: &str,
    ) -> anyhow::Result<Vec<IndexDefinitionRecord>> {
        let mut client = IndexServiceClient::connect(self.public_endpoint(node)).await?;
        Ok(client
            .list_indexes(authorized(
                ListIndexesRequest {
                    bucket_name: bucket_name.to_string(),
                    include_disabled: true,
                    page: None,
                },
                &self.admin_token,
            ))
            .await?
            .into_inner()
            .indexes)
    }

    pub async fn query_path_index(
        &self,
        node: usize,
        bucket_name: &str,
        index_name: &str,
    ) -> anyhow::Result<QueryIndexResponse> {
        let mut client = IndexServiceClient::connect(self.public_endpoint(node)).await?;
        Ok(client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket_name.to_string(),
                    index_name: index_name.to_string(),
                    query_text: String::new(),
                    query_vector: Vec::new(),
                    limit: 10,
                    phrase: false,
                    path_prefix: String::new(),
                    metadata_filters_json: String::new(),
                    typed_predicates_json: String::new(),
                    typed_order_json: String::new(),
                    page_token: String::new(),
                    require_caught_up_to_watch_cursor: String::new(),
                    lag_timeout_ms: 1_000,
                    boundary_predicates_json: String::new(),
                },
                &self.admin_token,
            ))
            .await?
            .into_inner())
    }

    pub async fn index_creator_is_owner(
        &self,
        node: usize,
        bucket_id: i64,
        index_name: &str,
    ) -> anyhow::Result<bool> {
        let mut client = AuthServiceClient::connect(self.public_endpoint(node)).await?;
        Ok(client
            .check_permission(authorized(
                CheckPermissionRequest {
                    namespace: anvil_core::access_control::system_realm_namespace(
                        anvil_core::system_realm::SYSTEM_INDEX_NAMESPACE,
                    ),
                    object_id: format!("{bucket_id}/{index_name}"),
                    relation: "owner".to_string(),
                    subject_kind: anvil_core::access_control::APP_SUBJECT_KIND.to_string(),
                    subject_id: ADMIN_PRINCIPAL.to_string(),
                    caveat_hash: String::new(),
                    consistency: "latest".to_string(),
                    zookie: String::new(),
                    scope: None,
                },
                &self.admin_token,
            ))
            .await?
            .into_inner()
            .allowed)
    }

    pub async fn index_creator_owner_tuple_count(
        &self,
        node: usize,
        bucket_id: i64,
        index_name: &str,
    ) -> anyhow::Result<usize> {
        let mut client = AuthServiceClient::connect(self.public_endpoint(node)).await?;
        Ok(client
            .read_authz_tuples(authorized(
                ReadAuthzTuplesRequest {
                    namespace: anvil_core::access_control::system_realm_namespace(
                        anvil_core::system_realm::SYSTEM_INDEX_NAMESPACE,
                    ),
                    object_id: format!("{bucket_id}/{index_name}"),
                    relation: "owner".to_string(),
                    subject_kind: anvil_core::access_control::APP_SUBJECT_KIND.to_string(),
                    subject_id: ADMIN_PRINCIPAL.to_string(),
                    caveat_hash: String::new(),
                    consistency: "latest".to_string(),
                    zookie: String::new(),
                    page_size: 100,
                    page_token: String::new(),
                    scope: None,
                },
                &self.admin_token,
            ))
            .await?
            .into_inner()
            .tuples
            .len())
    }

    async fn bootstrap_cluster_topology(&self, coordinator: usize) -> anyhow::Result<()> {
        let mut descriptors = Vec::new();
        for node in 0..self.nodes.len() {
            let mut admin =
                AdminServiceClient::connect(format!("http://{}", self.nodes[node].admin_addr))
                    .await?;
            descriptors.push(
                admin
                    .get_local_node_descriptor(authorized(
                        GetLocalNodeDescriptorRequest {},
                        &self.admin_token,
                    ))
                    .await?
                    .into_inner()
                    .node
                    .context("local node descriptor response omitted node")?,
            );
        }
        let cells = descriptors
            .iter()
            .map(|node| PutCellRequest {
                region_id: node.region.clone(),
                cell_id: node.cell_id.clone(),
                failure_domain: node.cell_id.clone(),
                state: "active".to_string(),
                options: None,
            })
            .collect();
        let nodes = descriptors
            .iter()
            .map(|node| {
                Ok(PutNodeRequest {
                    node_id: node.node_id.clone(),
                    region_id: node.region.clone(),
                    cell_id: node.cell_id.clone(),
                    advertise_addr: node.public_api_addr.clone(),
                    state: "active".to_string(),
                    capacity_json: "{}".to_string(),
                    options: None,
                    receipt_signing_public_key: node.receipt_signing_public_key.clone(),
                    capabilities: node
                        .capabilities
                        .iter()
                        .copied()
                        .map(bootstrap_capability_name)
                        .collect::<anyhow::Result<Vec<_>>>()?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let topology = BootstrapMeshTopologyRequest {
            regions: vec![PutRegionRequest {
                region_id: "process-e2e-region".to_string(),
                endpoint: self.public_endpoint(coordinator),
                state: "active".to_string(),
                options: None,
            }],
            cells,
            nodes,
            canonical_coremeta_rows: Vec::new(),
        };
        let mut seed = MeshControlServiceClient::connect(format!(
            "http://{}",
            self.nodes[coordinator].admin_addr
        ))
        .await?;
        let seeded = seed
            .bootstrap_mesh_topology(authorized(topology.clone(), &self.admin_token))
            .await?
            .into_inner();
        if seeded.canonical_coremeta_rows.is_empty() {
            bail!("process topology bootstrap omitted its canonical CoreMeta snapshot");
        }
        for node in 0..self.nodes.len() {
            if node == coordinator {
                continue;
            }
            let mut join = topology.clone();
            join.canonical_coremeta_rows = seeded.canonical_coremeta_rows.clone();
            let mut mesh = MeshControlServiceClient::connect(format!(
                "http://{}",
                self.nodes[node].admin_addr
            ))
            .await?;
            mesh.bootstrap_mesh_topology(authorized(join, &self.admin_token))
                .await?;
        }
        Ok(())
    }

    pub async fn stage_object_puts(
        &self,
        node: usize,
        bucket_name: &str,
        bucket_id: i64,
        transaction_id: &str,
        objects: &[(&str, &[u8])],
    ) -> anyhow::Result<MutationBatchResponse> {
        let mut client = ObjectServiceClient::connect(self.public_endpoint(node)).await?;
        Ok(client
            .mutation_batch(authorized(
                MutationBatchRequest {
                    bucket_name: bucket_name.to_string(),
                    mutation_context: Some(NativeMutationContext {
                        tenant_id: SYSTEM_STORAGE_TENANT_ID,
                        bucket_id,
                        principal: ADMIN_PRINCIPAL.to_string(),
                        request_id: uuid::Uuid::new_v4().to_string(),
                        precondition: "none".to_string(),
                        authz_zookie_optional: String::new(),
                        idempotency_key: uuid::Uuid::new_v4().to_string(),
                        transaction_id: Some(transaction_id.to_string()),
                        write_visibility: None,
                    }),
                    precondition: None,
                    operations: objects
                        .iter()
                        .map(|(key, payload)| MutationBatchOperation {
                            op: Some(mutation_batch_operation::Op::PutObject(
                                MutationBatchPutObject {
                                    object_key: (*key).to_string(),
                                    payload: payload.to_vec(),
                                    content_type: Some("application/octet-stream".to_string()),
                                    user_metadata_json: "{}".to_string(),
                                    storage_class: None,
                                },
                            )),
                        })
                        .collect(),
                },
                &self.admin_token,
            ))
            .await?
            .into_inner())
    }

    pub async fn object_exists(
        &self,
        node: usize,
        bucket_name: &str,
        object_key: &str,
    ) -> anyhow::Result<bool> {
        let mut client = ObjectServiceClient::connect(self.public_endpoint(node)).await?;
        match client
            .head_object(authorized(
                HeadObjectRequest {
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                    version_id: None,
                    ..Default::default()
                },
                &self.admin_token,
            ))
            .await
        {
            Ok(_) => Ok(true),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(false),
            Err(status) => Err(status.into()),
        }
    }

    pub async fn read_object(
        &self,
        node: usize,
        bucket_name: &str,
        object_key: &str,
    ) -> anyhow::Result<Vec<u8>> {
        let mut client = ObjectServiceClient::connect(self.public_endpoint(node)).await?;
        let mut stream = client
            .get_object(authorized(
                GetObjectRequest {
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                    version_id: None,
                    range: None,
                    consistency: Some(ReadConsistency {
                        mode: Some(anvil::anvil_api::read_consistency::Mode::Latest(true)),
                    }),
                },
                &self.admin_token,
            ))
            .await?
            .into_inner();
        let mut bytes = Vec::new();
        while let Some(frame) = stream.message().await? {
            if let Some(anvil::anvil_api::get_object_response::Data::Chunk(chunk)) = frame.data {
                bytes.extend(chunk);
            }
        }
        Ok(bytes)
    }

    /// Send SIGKILL to a node, retaining its directory and address for restart.
    pub async fn sigkill(&mut self, node: usize) -> anyhow::Result<()> {
        let mut child = self.nodes[node]
            .child
            .take()
            .context("process MVCC node is not running")?;
        child.start_kill().context("SIGKILL process MVCC node")?;
        child
            .wait()
            .await
            .context("reap killed process MVCC node")?;
        Ok(())
    }

    /// Arm a one-shot hard crash in an already-running debug child. The hook
    /// consumes this file before aborting, so same-disk restart can recover.
    pub fn arm_hard_crash(&self, node: usize, fault_point: &str) -> anyhow::Result<()> {
        const PROCESS_SAFE_POINTS: &[&str] = &[
            "PreparedBundleWrite",
            "ShardWrite",
            "MvccBatchWrite",
            "RaftLogWrite",
            "IndexFinalizationBeforeExecute",
            "IndexFinalizationAfterExecute",
            "PersonalDbPostCommitBeforeEffects",
            "PersonalDbPostCommitAfterEffects",
            "GitSourcePostCommitBeforeEffects",
            "GitSourcePostCommitAfterEffects",
            "BucketLocatorFinalizationBeforeEffects",
            "BucketLocatorFinalizationAfterEffects",
        ];
        if !PROCESS_SAFE_POINTS.contains(&fault_point) {
            bail!("fault point is not enabled for process-backed hard crashes");
        }
        std::fs::write(&self.nodes[node].hard_crash_control_path, fault_point)
            .context("arm process MVCC hard crash")
    }

    /// Arm the same worker fault on every live node.
    ///
    /// Background work is placed by compact-Raft partition assignment, not by
    /// request coordination. Process acceptance tests which exercise a worker
    /// crash therefore cannot assume that the transaction coordinator will
    /// execute the committed job.
    pub fn arm_hard_crash_on_all(&self, fault_point: &str) -> anyhow::Result<()> {
        for node in 0..self.nodes.len() {
            self.arm_hard_crash(node, fault_point)?;
        }
        Ok(())
    }

    pub async fn wait_for_hard_crash(
        &mut self,
        node: usize,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let result = tokio::time::timeout(timeout, async {
            loop {
                let child = self.nodes[node]
                    .child
                    .as_mut()
                    .context("process MVCC node is not running")?;
                if let Some(status) = child.try_wait()? {
                    self.nodes[node].child = None;
                    return self.verify_expected_hard_crash(node, status);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        // A timeout or unexpected exit must not leave a delayed crash armed
        // against later recovery work.
        let cleanup = self.disarm_hard_crash(node);
        let outcome = result.context("timed out waiting for process MVCC hard crash")?;
        outcome?;
        cleanup
    }

    /// Wait for the compact-Raft-assigned worker to consume a fault armed on
    /// multiple candidates, returning the process which actually executed it.
    pub async fn wait_for_any_hard_crash(
        &mut self,
        candidates: &[usize],
        timeout: Duration,
    ) -> anyhow::Result<usize> {
        if candidates.is_empty() {
            bail!("hard-crash wait requires at least one candidate node");
        }
        let result = tokio::time::timeout(timeout, async {
            loop {
                let mut exited = Vec::new();
                for &node in candidates {
                    let Some(candidate) = self.nodes.get_mut(node) else {
                        bail!("hard-crash candidate index {node} is out of bounds");
                    };
                    if let Some(child) = candidate.child.as_mut() {
                        if let Some(status) = child.try_wait()? {
                            exited.push((node, status));
                        }
                    }
                }
                if !exited.is_empty() {
                    for &(node, _) in &exited {
                        self.nodes[node].child = None;
                    }
                    if exited.len() != 1 {
                        bail!(
                            "expected one assigned process MVCC worker to hard crash, but nodes {:?} exited",
                            exited.iter().map(|(node, _)| *node).collect::<Vec<_>>()
                        );
                    }
                    let (node, status) = exited.pop().expect("one exit was observed");
                    self.verify_expected_hard_crash(node, status)?;
                    return Ok::<_, anyhow::Error>(node);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        // Disarm every losing candidate immediately after the assigned worker
        // aborts. Also disarm on timeout/error so a later unrelated task cannot
        // consume a stale control file.
        let mut cleanup = Ok(());
        for &node in candidates {
            if let Err(error) = self.disarm_hard_crash(node) {
                cleanup = Err(error);
                break;
            }
        }
        let outcome = result
            .context("timed out waiting for an assigned process MVCC worker to hard crash")?;
        let node = outcome?;
        cleanup?;
        Ok(node)
    }

    fn verify_expected_hard_crash(
        &self,
        node: usize,
        status: std::process::ExitStatus,
    ) -> anyhow::Result<()> {
        if status.success() {
            bail!("process MVCC node {node} exited successfully instead of aborting");
        }
        if self.nodes[node].hard_crash_control_path.exists() {
            bail!("process MVCC node {node} exited without consuming its armed hard-crash control");
        }
        Ok(())
    }

    fn disarm_hard_crash(&self, node: usize) -> anyhow::Result<()> {
        match std::fs::remove_file(&self.nodes[node].hard_crash_control_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).context("disarm process MVCC hard crash"),
        }
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
    pub async fn spawn_replacement(&mut self, node: usize, incarnation: u64) -> anyhow::Result<()> {
        if self.nodes[node].child.is_some() {
            bail!("replacement requires the old process to be stopped");
        }
        if incarnation <= self.nodes[node].incarnation {
            bail!("replacement incarnation must advance");
        }
        self.obsolete_nodes[node] = Some(ObsoleteNode {
            storage_path: self.nodes[node].storage_path.clone(),
            peers_json: self.peers_json.clone(),
            incarnation: self.nodes[node].incarnation,
        });
        self.nodes[node].incarnation = incarnation;
        self.nodes[node].storage_path = self
            ._directory
            .path()
            .join(format!("node-{}-incarnation-{incarnation}", node + 1));
        let mut peers: Vec<serde_json::Value> = serde_json::from_str(&self.peers_json)?;
        peers[node]["incarnation"] = serde_json::json!(incarnation);
        self.peers_json = serde_json::to_string(&peers)?;
        self.spawn_node(node).await?;
        // This is a clean disk whose newer incarnation has not yet been
        // installed by ReplaceClusterNodeIncarnation. Requiring an
        // authenticated admin response here creates a cycle: the replacement
        // cannot apply the system realm until the surviving quorum admits it,
        // while the caller cannot submit that admission until this method
        // returns. Transport readiness is the correct pre-admission boundary.
        self.wait_for_admin_transport(node).await
    }

    /// Relaunch the retired disk on separate listeners. Its authenticated
    /// streams still present the obsolete incarnation and must be rejected by
    /// the surviving nodes' applied control fences.
    pub async fn spawn_obsolete_incarnation(&mut self, node: usize) -> anyhow::Result<String> {
        let obsolete = self.obsolete_nodes[node]
            .as_ref()
            .context("node has no retired incarnation")?;
        let addresses = reserve_loopback_addresses(2)?;
        let api_addr = addresses[0];
        let admin_addr = addresses[1];
        let child = Command::new(&self.binary)
            .env("JWT_SECRET", JWT_SECRET)
            .env("ANVIL_SECRET_ENCRYPTION_KEY", ENCRYPTION_KEY)
            .env("PUBLIC_API_ADDR", format!("http://{api_addr}"))
            .env("API_LISTEN_ADDR", api_addr.to_string())
            .env("ADMIN_LISTEN_ADDR", admin_addr.to_string())
            .env("REGION", "process-e2e-region")
            .env("CELL_ID", format!("zone-{}", node + 1))
            .env("NODE_ID", format!("{}-node-{}", self.cluster_id, node + 1))
            .env("MVCC_RAFT_NODE_ID", (node + 1).to_string())
            .env("MVCC_NODE_INCARNATION", obsolete.incarnation.to_string())
            .env("MVCC_FAILURE_DOMAIN", format!("zone-{}", node + 1))
            .env("MVCC_PEERS_JSON", &obsolete.peers_json)
            .env("MVCC_BOOTSTRAP_MEMBERSHIP", "false")
            .env("MVCC_RAFT_GROUP_ID", "1")
            .env("MVCC_CLUSTER_ID", &self.cluster_id)
            .env("MVCC_BUNDLE_QUORUM_HOLDERS", "2")
            .env("MVCC_TOLERATED_FAILURE_DOMAINS", "1")
            .env("MVCC_RPC_TIMEOUT_MS", "1000")
            .env("MVCC_NODE_CONNECTION_TOKEN", "process-e2e-node-token")
            .env("STORAGE_PATH", &obsolete.storage_path)
            .env("BOOTSTRAP_SYSTEM_ADMIN_SUBJECT_KIND", "app")
            .env("BOOTSTRAP_SYSTEM_ADMIN_SUBJECT_ID", ADMIN_PRINCIPAL)
            .env("ANVIL_TEST_ENABLE_PROCESS_HARD_CRASH", "1")
            .env("ANVIL_TEST_ALLOW_INSECURE_MVCC_TRANSPORT", "1")
            .env(
                "ANVIL_MVCC_HARD_CRASH_CONTROL_FILE",
                &self.nodes[node].hard_crash_control_path,
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        self.obsolete_children.push(child);
        Ok(format!("http://{api_addr}"))
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
        let mut client =
            AdminServiceClient::connect(format!("http://{}", self.nodes[coordinator].admin_addr))
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
            .env("CELL_ID", format!("zone-{}", node + 1))
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
            .env("ANVIL_TEST_ENABLE_PROCESS_HARD_CRASH", "1")
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

    async fn wait_for_admin_transport(&mut self, node: usize) -> anyhow::Result<()> {
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
                // The system realm is committed through this cluster's Raft
                // group, so an authenticated admin authorization check cannot
                // succeed before the bootstrap voter is running. An
                // unauthenticated response proves that the admin gRPC router
                // and its interceptor are serving without weakening
                // production authorization.
                match client
                    .get_local_node_descriptor(Request::new(GetLocalNodeDescriptorRequest {}))
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(status) if status.code() == tonic::Code::Unauthenticated => return Ok(()),
                    Err(_) => {}
                }
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("process MVCC node {node} admin transport did not become ready");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn bootstrap_capability_name(value: i32) -> anyhow::Result<String> {
    let Ok(capability) = NodeCapability::try_from(value) else {
        bail!("local node descriptor advertised unknown capability {value}");
    };
    let name = match capability {
        NodeCapability::Object => "object",
        NodeCapability::Index => "index",
        NodeCapability::Personaldb => "personaldb",
        NodeCapability::Metadata => "metadata",
        NodeCapability::Gateway => "gateway",
        NodeCapability::Admin => "admin",
        NodeCapability::Unspecified => {
            bail!("local node descriptor advertised an unspecified capability")
        }
    };
    Ok(name.to_string())
}

impl Drop for ProcessMvccCluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(child) = &mut node.child {
                let _ = child.start_kill();
            }
        }
        for child in &mut self.obsolete_children {
            let _ = child.start_kill();
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
