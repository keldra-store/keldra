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
    mvcc_fault_injection::{self, DeterministicFaults, FaultPoint},
    mvcc_transaction::{
        CertificationAbort, CertificationResult, DurabilityLevel, LogicalKey, ReadConsistency,
    },
    personaldb_signing,
};

struct ThreeNodeFixture {
    _directories: Vec<TempDir>,
    configs: Vec<Config>,
    endpoints: Vec<String>,
    states: Vec<Arc<AppState>>,
    servers: Vec<Option<JoinHandle<()>>>,
}

impl ThreeNodeFixture {
    async fn start() -> Self {
        let directories = (0..3)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();
        let mut listeners = vec![
            Some(TcpListener::bind("127.0.0.1:0").await.unwrap()),
            Some(TcpListener::bind("127.0.0.1:0").await.unwrap()),
            Some(TcpListener::bind("127.0.0.1:0").await.unwrap()),
        ];
        let endpoints = listeners
            .iter()
            .map(|listener| {
                format!(
                    "http://{}",
                    listener.as_ref().unwrap().local_addr().unwrap()
                )
            })
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
        let mut configs = Vec::new();
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
            configs.push(config);
        }

        // Followers must be listening before the bootstrap member can form a
        // majority. Constructing node 1 first deadlocks fixture startup:
        // AppState waits to install the initial Raft control state while nodes
        // 2 and 3 have not yet been created and cannot answer vote requests.
        let mut states = vec![None, None, None];
        let mut servers = (0..3).map(|_| None).collect::<Vec<_>>();
        for index in [1_usize, 2, 0] {
            let state = Arc::new(
                AppState::new(
                    configs[index].clone(),
                    personaldb_signing::PersonalDbProtocolKeyring::disabled(),
                )
                .await
                .unwrap(),
            );
            let consensus = state.mvcc.consensus_service.clone();
            let replication = state.mvcc.replication_service.clone();
            let listener = listeners[index].take().unwrap();
            servers[index] = Some(tokio::spawn(async move {
                Server::builder()
                    .add_service(ConsensusTransportServer::new(consensus))
                    .add_service(ReplicationServiceServer::new(replication))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            }));
            states[index] = Some(state);
        }
        let states = states.into_iter().map(Option::unwrap).collect::<Vec<_>>();
        let fixture = Self {
            _directories: directories,
            configs,
            endpoints,
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

    async fn restart_node(&mut self, node: usize) {
        self.stop_transport(node);
        self.states[node].mvcc.shutdown().await;
        self.states[node].mvcc.consensus.shutdown().await.unwrap();
        let state = Arc::new(
            AppState::new(
                self.configs[node].clone(),
                personaldb_signing::PersonalDbProtocolKeyring::disabled(),
            )
            .await
            .unwrap(),
        );
        let listener = TcpListener::bind(self.endpoints[node].trim_start_matches("http://"))
            .await
            .unwrap();
        let consensus = state.mvcc.consensus_service.clone();
        let replication = state.mvcc.replication_service.clone();
        self.servers[node] = Some(tokio::spawn(async move {
            Server::builder()
                .add_service(ConsensusTransportServer::new(consensus))
                .add_service(ReplicationServiceServer::new(replication))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        }));
        self.states[node] = state;
    }

    fn inject_transport_loss(&mut self, node: usize, point: FaultPoint) {
        mvcc_fault_injection::install(DeterministicFaults::default().fail_at(point, 1));
        let injected = mvcc_fault_injection::hit(point).unwrap_err();
        mvcc_fault_injection::clear();
        assert_eq!(injected.point, point);
        self.stop_transport(node);
    }

    async fn write(&self, node: usize, id: &str, key: LogicalKey) {
        self.write_outcome(node, id, key).await.unwrap();
    }

    async fn write_outcome(
        &self,
        node: usize,
        id: &str,
        key: LogicalKey,
    ) -> anyhow::Result<crate::mvcc_node_runtime::CommitOutcome> {
        let transaction_id = self.stage_write(node, id, key).await;
        self.states[node]
            .mvcc
            .open_transactions
            .commit(
                self.states[node].mvcc.runtime.as_ref(),
                &transaction_id,
                "fault-principal",
                3,
            )
            .await
    }

    async fn stage_write(&self, node: usize, id: &str, key: LogicalKey) -> String {
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
        handle.transaction_id
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
async fn non_owner_producer_enqueues_but_worker_mutations_remain_assignment_fenced() {
    let mut cluster = ThreeNodeFixture::start().await;
    let task_queue_partition =
        crate::mvcc_worker_authority::work_partition_id("task-queue", "global").unwrap();
    let _ = cluster.states[0]
        .mvcc
        .reconcile_work_assignment("task-queue", "global")
        .await
        .unwrap();
    cluster.states[0]
        .mvcc
        .consensus
        .linearized_read_barrier()
        .await
        .unwrap();
    let assigned_owner = cluster.states[0]
        .mvcc
        .consensus
        .applied_control_snapshot()
        .unwrap()
        .partitions
        .into_iter()
        .find(|(partition_id, _)| *partition_id == task_queue_partition)
        .expect("task queue assignment is installed")
        .1
        .owner;
    let assigned_owner_index = cluster
        .states
        .iter()
        .position(|state| {
            crate::mvcc_bootstrap::consensus_control_node_id(&state.config.node_id)
                == assigned_owner.node_id
                && state.config.mvcc_node_incarnation == assigned_owner.incarnation
        })
        .expect("task queue assignment names a fixture node");

    // Node zero is the initial leader. If rendezvous placement also selected it
    // as the task worker, elect one of the surviving voters as coordinator.
    // The stopped node remains installed, so the exact task-queue assignment
    // continues to name it and cannot accidentally move to the producer.
    let producer = if assigned_owner_index == 0 {
        cluster.stop_transport(0);
        cluster.states[0].mvcc.consensus.shutdown().await.unwrap();
        cluster.wait_for_any_leader(&[1, 2]).await
    } else {
        0
    };
    assert_ne!(producer, assigned_owner_index);
    cluster.states[producer]
        .mvcc
        .consensus
        .linearized_read_barrier()
        .await
        .unwrap();
    let producer_snapshot = cluster.states[producer]
        .mvcc
        .consensus
        .applied_control_snapshot()
        .unwrap();
    let current_owner = producer_snapshot
        .partitions
        .iter()
        .find(|(partition_id, _)| *partition_id == task_queue_partition)
        .expect("producer observes the task queue assignment")
        .1
        .owner;
    assert_eq!(current_owner, assigned_owner);
    assert_ne!(
        current_owner.node_id,
        crate::mvcc_bootstrap::consensus_control_node_id(&cluster.states[producer].config.node_id)
    );

    let legacy_partition_id = hex::encode(crate::task_journal::task_queue_partition_id());
    let legacy_owner_before = crate::partition_fence::read_partition_owner_mvcc(
        &cluster.states[producer].mvcc,
        "task_queue",
        &legacy_partition_id,
        cluster.states[producer]
            .persistence
            .partition_owner_signing_key(),
    )
    .unwrap();
    let marker = format!("non-owner-producer-{producer}");
    cluster.states[producer]
        .persistence
        .enqueue_task(
            crate::tasks::TaskType::DeleteBucket,
            serde_json::json!({
                "bucket_id": 7,
                "regression_marker": marker,
            }),
            100,
        )
        .await
        .expect("ordinary producer enqueue does not require the task worker assignment");
    let legacy_owner_after = crate::partition_fence::read_partition_owner_mvcc(
        &cluster.states[producer].mvcc,
        "task_queue",
        &legacy_partition_id,
        cluster.states[producer]
            .persistence
            .partition_owner_signing_key(),
    )
    .unwrap();
    assert_eq!(
        legacy_owner_after, legacy_owner_before,
        "producer enqueue must not acquire or transfer legacy task-queue partition ownership"
    );
    let task = cluster.states[producer]
        .persistence
        .list_tasks_page(None, 1_000)
        .await
        .unwrap()
        .tasks
        .into_iter()
        .find(|task| task.payload["regression_marker"].as_str() == Some(marker.as_str()))
        .expect("non-owner producer task is atomically visible");
    assert_eq!(task.status, crate::tasks::TaskStatus::Pending);

    let claim_error = cluster.states[producer]
        .persistence
        .claim_pending_tasks(1)
        .await
        .expect_err("non-owner must not claim task-queue work");
    assert!(
        format!("{claim_error:#}").contains("local node does not own the task queue assignment"),
        "unexpected claim failure: {claim_error:#}"
    );

    let update_error = cluster.states[producer]
        .persistence
        .update_task_status(task.id, crate::tasks::TaskStatus::Completed)
        .await
        .expect_err("non-owner must not publish task status");
    assert!(
        format!("{update_error:#}").contains("local node does not own the task queue assignment"),
        "unexpected status failure: {update_error:#}"
    );
    let failure_error = cluster.states[producer]
        .persistence
        .fail_task(task.id, "must remain fenced")
        .await
        .expect_err("non-owner must not publish task failure");
    assert!(
        format!("{failure_error:#}").contains("local node does not own the task queue assignment"),
        "unexpected failure transition error: {failure_error:#}"
    );
    let legacy_owner_after_rejected_worker_mutations =
        crate::partition_fence::read_partition_owner_mvcc(
            &cluster.states[producer].mvcc,
            "task_queue",
            &legacy_partition_id,
            cluster.states[producer]
                .persistence
                .partition_owner_signing_key(),
        )
        .unwrap();
    assert_eq!(
        legacy_owner_after_rejected_worker_mutations, legacy_owner_before,
        "rejected non-owner worker mutations must not acquire or transfer legacy task-queue partition ownership"
    );
    let unchanged = cluster.states[producer]
        .persistence
        .list_tasks_page(None, 1_000)
        .await
        .unwrap()
        .tasks
        .into_iter()
        .find(|candidate| candidate.id == task.id)
        .expect("rejected worker update retains the producer task");
    assert_eq!(unchanged.status, crate::tasks::TaskStatus::Pending);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn quorum_commit_survives_one_minority_transport_loss() {
    let mut cluster = ThreeNodeFixture::start().await;
    cluster.inject_transport_loss(2, FaultPoint::MinorityNodeLoss);
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
    cluster.inject_transport_loss(0, FaultPoint::LeaderChange);
    cluster.states[0].mvcc.consensus.shutdown().await.unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn committed_value_survives_leader_loss_after_proposal() {
    let mut cluster = ThreeNodeFixture::start().await;
    let key = LogicalKey {
        table_id: 1,
        application_key: b"leader-loss-after-commit".to_vec(),
    };
    cluster
        .write(0, "leader-loss-after-commit", key.clone())
        .await;

    cluster.inject_transport_loss(0, FaultPoint::LeaderChange);
    cluster.states[0].mvcc.consensus.shutdown().await.unwrap();
    let leader = cluster.wait_for_any_leader(&[1, 2]).await;
    cluster.states[leader]
        .mvcc
        .consensus
        .linearized_read_barrier()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if cluster.states[leader].mvcc.read_latest_value(&key).unwrap()
                == Some(b"leader-loss-after-commit".to_vec())
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("new leader applies the value committed before leader loss");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn blind_writes_do_not_conflict_but_observed_writes_do() {
    let cluster = ThreeNodeFixture::start().await;
    let principal = "fault-principal";
    let key = LogicalKey {
        table_id: 1,
        application_key: b"blind-write-semantics".to_vec(),
    };
    let range_observer = cluster.states[0]
        .mvcc
        .open_transactions
        .begin(
            cluster.states[0].mvcc.runtime.as_ref(),
            "fault-e2e",
            principal,
            "blind-range-observer",
            Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            1,
        )
        .await
        .unwrap();
    cluster.states[0]
        .mvcc
        .open_transactions
        .observe_range(
            &range_observer.transaction_id,
            "fault-e2e",
            1,
            Some(b"blind-write-".to_vec()),
            Some(b"blind-write-z".to_vec()),
            None,
            2,
        )
        .unwrap();
    cluster.states[0]
        .mvcc
        .open_transactions
        .put(
            &range_observer.transaction_id,
            "fault-e2e",
            LogicalKey {
                table_id: 2,
                application_key: b"force-certification".to_vec(),
            },
            b"value".to_vec(),
            2,
        )
        .unwrap();

    let first = cluster.states[0]
        .mvcc
        .open_transactions
        .begin(
            cluster.states[0].mvcc.runtime.as_ref(),
            "fault-e2e",
            principal,
            "blind-first",
            Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            1,
        )
        .await
        .unwrap();
    let second = cluster.states[0]
        .mvcc
        .open_transactions
        .begin(
            cluster.states[0].mvcc.runtime.as_ref(),
            "fault-e2e",
            principal,
            "blind-second",
            Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            1,
        )
        .await
        .unwrap();
    for (handle, value) in [
        (&first, b"first".as_slice()),
        (&second, b"second".as_slice()),
    ] {
        cluster.states[0]
            .mvcc
            .open_transactions
            .put(
                &handle.transaction_id,
                "fault-e2e",
                key.clone(),
                value.to_vec(),
                2,
            )
            .unwrap();
    }
    for handle in [&first, &second] {
        assert!(matches!(
            cluster.states[0]
                .mvcc
                .open_transactions
                .commit(
                    cluster.states[0].mvcc.runtime.as_ref(),
                    &handle.transaction_id,
                    principal,
                    3,
                )
                .await
                .unwrap()
                .certification,
            CertificationResult::Committed { .. }
        ));
    }
    assert_eq!(
        cluster.states[0].mvcc.read_latest_value(&key).unwrap(),
        Some(b"second".to_vec())
    );
    assert!(matches!(
        cluster.states[0]
            .mvcc
            .open_transactions
            .commit(
                cluster.states[0].mvcc.runtime.as_ref(),
                &range_observer.transaction_id,
                principal,
                4,
            )
            .await
            .unwrap()
            .certification,
        CertificationResult::Aborted {
            reason: CertificationAbort::RangeConflict { .. }
        }
    ));

    let observed_key = LogicalKey {
        table_id: 1,
        application_key: b"observed-write-semantics".to_vec(),
    };
    let observed_first = cluster.states[0]
        .mvcc
        .open_transactions
        .begin(
            cluster.states[0].mvcc.runtime.as_ref(),
            "fault-e2e",
            principal,
            "observed-first",
            Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            4,
        )
        .await
        .unwrap();
    let observed_second = cluster.states[0]
        .mvcc
        .open_transactions
        .begin(
            cluster.states[0].mvcc.runtime.as_ref(),
            "fault-e2e",
            principal,
            "observed-second",
            Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            4,
        )
        .await
        .unwrap();
    for (handle, value) in [
        (&observed_first, b"first".as_slice()),
        (&observed_second, b"second".as_slice()),
    ] {
        cluster.states[0]
            .mvcc
            .open_transactions
            .observe_point(
                &handle.transaction_id,
                "fault-e2e",
                observed_key.clone(),
                None,
                5,
            )
            .unwrap();
        cluster.states[0]
            .mvcc
            .open_transactions
            .put(
                &handle.transaction_id,
                "fault-e2e",
                observed_key.clone(),
                value.to_vec(),
                5,
            )
            .unwrap();
    }
    assert!(matches!(
        cluster.states[0]
            .mvcc
            .open_transactions
            .commit(
                cluster.states[0].mvcc.runtime.as_ref(),
                &observed_first.transaction_id,
                principal,
                6,
            )
            .await
            .unwrap()
            .certification,
        CertificationResult::Committed { .. }
    ));
    assert!(matches!(
        cluster.states[0]
            .mvcc
            .open_transactions
            .commit(
                cluster.states[0].mvcc.runtime.as_ref(),
                &observed_second.transaction_id,
                principal,
                6,
            )
            .await
            .unwrap()
            .certification,
        CertificationResult::Aborted {
            reason: CertificationAbort::PointConflict { .. }
        }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn coordinator_failure_before_proposal_is_retryable_without_a_commit() {
    let mut cluster = ThreeNodeFixture::start().await;
    let key = LogicalKey {
        table_id: 1,
        application_key: b"before-proposal".to_vec(),
    };
    let transaction_id = cluster.stage_write(0, "before-proposal", key.clone()).await;
    let version_before = cluster.states[0].mvcc.consensus.observed_commit_version();
    let first = mvcc_fault_injection::scoped(
        DeterministicFaults::default().fail_at(FaultPoint::BeforeProposal, 1),
        cluster.states[0].mvcc.open_transactions.commit(
            cluster.states[0].mvcc.runtime.as_ref(),
            &transaction_id,
            "fault-principal",
            3,
        ),
    )
    .await;
    assert!(first.unwrap_err().to_string().contains("BeforeProposal"));
    assert_eq!(
        cluster.states[0].mvcc.read_latest_value(&key).unwrap(),
        None
    );
    assert_eq!(
        cluster.states[0].mvcc.consensus.observed_commit_version(),
        version_before,
        "failure before proposal must not create a commit"
    );
    assert_eq!(
        cluster.states[0]
            .mvcc
            .open_transactions
            .status(&transaction_id, "fault-principal", 3)
            .unwrap()
            .state,
        "open",
        "a definite failure before proposal must return the draft to Open"
    );

    cluster.restart_node(0).await;
    cluster.wait_for_any_leader(&[0, 1, 2]).await;
    let recovered = cluster.states[0]
        .mvcc
        .open_transactions
        .commit(
            cluster.states[0].mvcc.runtime.as_ref(),
            &transaction_id,
            "fault-principal",
            4,
        )
        .await
        .unwrap();
    let CertificationResult::Committed { commit_version } = recovered.certification else {
        panic!("retry after pre-proposal crash did not commit");
    };
    for node in 0..3 {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if cluster.states[node].mvcc.read_latest_value(&key).unwrap()
                    == Some(b"before-proposal".to_vec())
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(cluster.states[node].mvcc.runtime.applied_version().unwrap() >= commit_version);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn quorum_rollback_after_definite_failure_allows_fresh_absent_cas() {
    let mut cluster = ThreeNodeFixture::start().await;
    let principal = "fault-principal";
    let key = LogicalKey {
        table_id: 1,
        application_key: b"quorum-fresh-after-rollback".to_vec(),
    };
    let failed_transaction_id = cluster
        .stage_write(0, "quorum-failed-before-proposal", key.clone())
        .await;
    cluster.states[0]
        .mvcc
        .open_transactions
        .observe_point(&failed_transaction_id, "fault-e2e", key.clone(), None, 2)
        .unwrap();

    let failure = mvcc_fault_injection::scoped(
        DeterministicFaults::default().fail_at(FaultPoint::BeforeProposal, 1),
        cluster.states[0].mvcc.open_transactions.commit(
            cluster.states[0].mvcc.runtime.as_ref(),
            &failed_transaction_id,
            principal,
            3,
        ),
    )
    .await
    .expect_err("the injected definite pre-proposal failure must be returned");
    assert!(failure.to_string().contains("BeforeProposal"));
    assert_eq!(
        cluster.states[0]
            .mvcc
            .open_transactions
            .status(&failed_transaction_id, principal, 3)
            .unwrap()
            .state,
        "open"
    );
    cluster.states[0]
        .mvcc
        .open_transactions
        .rollback(&failed_transaction_id, principal, 4)
        .unwrap();
    assert_eq!(
        cluster.states[0].mvcc.read_latest_value(&key).unwrap(),
        None,
        "rollback must leave no readable or predicate-visible value"
    );

    let fresh_transaction_id = cluster
        .stage_write(0, "quorum-fresh-after-rollback", key.clone())
        .await;
    assert_ne!(fresh_transaction_id, failed_transaction_id);
    cluster.states[0]
        .mvcc
        .open_transactions
        .observe_point(&fresh_transaction_id, "fault-e2e", key.clone(), None, 5)
        .unwrap();
    let committed = cluster.states[0]
        .mvcc
        .open_transactions
        .commit(
            cluster.states[0].mvcc.runtime.as_ref(),
            &fresh_transaction_id,
            principal,
            6,
        )
        .await
        .unwrap();
    let CertificationResult::Committed { commit_version } = committed.certification else {
        panic!("fresh absent-CAS transaction did not commit");
    };

    for node in 0..3 {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if cluster.states[node].mvcc.read_latest_value(&key).unwrap()
                    == Some(b"quorum-fresh-after-rollback".to_vec())
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fresh quorum commit becomes readable on every node");
        assert!(cluster.states[node].mvcc.runtime.applied_version().unwrap() >= commit_version);
    }

    cluster.restart_node(0).await;
    cluster.wait_for_any_leader(&[0, 1, 2]).await;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if cluster.states[0].mvcc.read_latest_value(&key).unwrap()
                == Some(b"quorum-fresh-after-rollback".to_vec())
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fresh quorum commit remains readable after node restart and Raft replay");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn coordinator_failure_after_proposal_recovers_the_stable_commit() {
    let mut cluster = ThreeNodeFixture::start().await;
    let key = LogicalKey {
        table_id: 1,
        application_key: b"after-proposal".to_vec(),
    };
    let transaction_id = cluster.stage_write(0, "after-proposal", key.clone()).await;
    let first = mvcc_fault_injection::scoped(
        DeterministicFaults::default().fail_at(FaultPoint::AfterProposal, 1),
        cluster.states[0].mvcc.open_transactions.commit(
            cluster.states[0].mvcc.runtime.as_ref(),
            &transaction_id,
            "fault-principal",
            3,
        ),
    )
    .await;
    assert!(first.unwrap_err().to_string().contains("AfterProposal"));
    assert_eq!(
        cluster.states[0]
            .mvcc
            .open_transactions
            .status(&transaction_id, "fault-principal", 3)
            .unwrap()
            .state,
        "committing"
    );
    let committed_before_response = cluster.states[0]
        .mvcc
        .consensus
        .linearized_transaction_outcome(crate::mvcc_consensus_adapter::consensus_transaction_id(
            cluster.states[0].mvcc.cluster_id(),
            &transaction_id,
        ))
        .await
        .unwrap()
        .expect("failed response still has a linearized terminal transaction outcome");
    let committed_before_response = match committed_before_response.result {
        anvil_mvcc_consensus::CertificationResult::Committed { commit_version, .. } => {
            commit_version.0
        }
        anvil_mvcc_consensus::CertificationResult::Aborted { reason, .. } => {
            panic!("post-proposal crash recorded an abort: {reason:?}")
        }
    };

    cluster.restart_node(0).await;
    cluster.wait_for_any_leader(&[0, 1, 2]).await;
    let recovered = cluster.states[0]
        .mvcc
        .open_transactions
        .commit(
            cluster.states[0].mvcc.runtime.as_ref(),
            &transaction_id,
            "fault-principal",
            4,
        )
        .await
        .unwrap();
    let commit_version = match &recovered.certification {
        CertificationResult::Committed { commit_version } => *commit_version,
        CertificationResult::Aborted { reason } => {
            panic!("indeterminate proposal resolved as abort: {reason:?}")
        }
    };
    assert_eq!(commit_version, committed_before_response);
    let committed_bundle = cluster.states[0]
        .mvcc
        .consensus
        .applied_decisions_after(anvil_mvcc_consensus::CommitVersion(
            commit_version.saturating_sub(1),
        ))
        .unwrap()
        .into_iter()
        .find(|decision| decision.position.0 == commit_version)
        .and_then(|decision| decision.committed_bundle)
        .expect("stable commit has one committed bundle decision");
    let replay = cluster.states[0]
        .mvcc
        .open_transactions
        .commit(
            cluster.states[0].mvcc.runtime.as_ref(),
            &transaction_id,
            "fault-principal",
            5,
        )
        .await
        .unwrap();
    assert_eq!(replay.certification, recovered.certification);
    assert_eq!(
        cluster.states[0]
            .mvcc
            .consensus
            .applied_decisions_after(anvil_mvcc_consensus::CommitVersion(commit_version))
            .unwrap()
            .into_iter()
            .filter(|decision| {
                decision
                    .committed_bundle
                    .as_ref()
                    .is_some_and(|bundle| bundle.bundle_hash == committed_bundle.bundle_hash)
            })
            .count(),
        0,
        "resolved retry must not create a second committed bundle decision"
    );
    assert_eq!(
        cluster.states[0].mvcc.read_latest_value(&key).unwrap(),
        Some(b"after-proposal".to_vec())
    );
    for node in 0..3 {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if cluster.states[node].mvcc.read_latest_value(&key).unwrap()
                    == Some(b"after-proposal".to_vec())
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
