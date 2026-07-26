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
    let cluster = ThreeNodeFixture::start().await;
    let key = LogicalKey {
        table_id: 1,
        application_key: b"before-proposal".to_vec(),
    };
    mvcc_fault_injection::install(
        DeterministicFaults::default().fail_at(FaultPoint::BeforeProposal, 1),
    );
    let transaction_id = cluster.stage_write(0, "before-proposal", key.clone()).await;
    let first = cluster.states[0]
        .mvcc
        .open_transactions
        .commit(
            cluster.states[0].mvcc.runtime.as_ref(),
            &transaction_id,
            "fault-principal",
            3,
        )
        .await;
    mvcc_fault_injection::clear();
    assert!(first.unwrap_err().to_string().contains("BeforeProposal"));
    assert_eq!(
        cluster.states[0].mvcc.read_latest_value(&key).unwrap(),
        None
    );

    assert!(matches!(
        cluster.states[0]
            .mvcc
            .open_transactions
            .commit(
                cluster.states[0].mvcc.runtime.as_ref(),
                &transaction_id,
                "fault-principal",
                4,
            )
            .await
            .unwrap()
            .certification,
        CertificationResult::Committed { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn coordinator_failure_after_proposal_recovers_the_stable_commit() {
    let cluster = ThreeNodeFixture::start().await;
    let key = LogicalKey {
        table_id: 1,
        application_key: b"after-proposal".to_vec(),
    };
    mvcc_fault_injection::install(
        DeterministicFaults::default().fail_at(FaultPoint::AfterProposal, 1),
    );
    let transaction_id = cluster.stage_write(0, "after-proposal", key.clone()).await;
    let first = cluster.states[0]
        .mvcc
        .open_transactions
        .commit(
            cluster.states[0].mvcc.runtime.as_ref(),
            &transaction_id,
            "fault-principal",
            3,
        )
        .await;
    mvcc_fault_injection::clear();
    assert!(first.unwrap_err().to_string().contains("AfterProposal"));

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
    assert!(matches!(
        recovered.certification,
        CertificationResult::Committed { .. }
    ));
    assert_eq!(
        cluster.states[0].mvcc.read_latest_value(&key).unwrap(),
        Some(b"after-proposal".to_vec())
    );
}
