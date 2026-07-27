//! OS child-process crash/restart acceptance at durable MVCC boundaries.

use std::{path::Path, process::Command, sync::Arc};

use anvil_mvcc_consensus::RocksRaftStore;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Status, metadata::MetadataMap};

use crate::{
    anvil_api::replication_service_server::ReplicationServiceServer,
    bundle_replication::{
        AppendOnlyPreparedBundleStore, BundleTarget, BundleTargetStream,
    },
    mvcc_store::LocalMvccStore,
    mvcc_transaction::{
        BundleIdentity, HierarchicalRangeStampScheme, LogicalKey, NodeIncarnation,
        PreparedBundleStore, TransactionBundleBuilder,
    },
    replication::{AckStatus, AuthenticatedPeer},
    replication_client::{
        ReplicationPeer, ReplicationStreamOptions, TonicReplicationStreamManager,
    },
    services::replication::{
        ReplicationConnectionAuthorizer, ReplicationServiceImpl,
    },
    shard_store::{ShardKind, ShardRecord, ShardSegment},
};

const CHILD_TEST: &str =
    "mvcc_process_crash_acceptance::mvcc_os_crash_child";
fn child_path() -> std::path::PathBuf {
    std::env::var_os("ANVIL_MVCC_CRASH_DIRECTORY")
        .map(Into::into)
        .expect("crash child directory")
}

fn run_child(scenario: &str, crash_at: &str, directory: &Path) {
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD_TEST, "--nocapture"])
        .env("ANVIL_MVCC_CRASH_SCENARIO", scenario)
        .env("ANVIL_MVCC_HARD_CRASH_AT", crash_at)
        .env("ANVIL_MVCC_CRASH_DIRECTORY", directory)
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "child must terminate at the requested hard-crash boundary"
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(6),
            "child must die from process::abort at the failpoint, not from an assertion or panic"
        );
    }
}

fn unused_loopback_endpoint() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    endpoint
}

fn run_tonic_child(
    scenario: &str,
    crash_at: &str,
    directory: &Path,
    endpoint: &str,
) {
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD_TEST, "--nocapture"])
        .env("ANVIL_MVCC_CRASH_SCENARIO", scenario)
        .env("ANVIL_MVCC_HARD_CRASH_AT", crash_at)
        .env("ANVIL_MVCC_CRASH_DIRECTORY", directory)
        .env("ANVIL_MVCC_CRASH_ENDPOINT", endpoint)
        .status()
        .unwrap();
    assert!(!status.success());
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(6));
    }
}

#[derive(Clone)]
struct CrashReplicationAuthorizer;

#[async_trait]
impl ReplicationConnectionAuthorizer for CrashReplicationAuthorizer {
    async fn authorize(
        &self,
        _metadata: &MetadataMap,
        open: &crate::anvil_api::ReplicationSessionOpen,
    ) -> Result<AuthenticatedPeer, Status> {
        AuthenticatedPeer::new_bound(
            open.node_id.clone(),
            open.node_incarnation,
            "crash-client",
        )
        .map_err(|error| Status::permission_denied(error.to_string()))
    }
}

fn crash_bundle() -> (BundleIdentity, Vec<u8>, BundleTarget) {
    let bytes = b"durable-before-complete-ack".to_vec();
    (
        bundle_identity(&bytes),
        bytes,
        BundleTarget {
            cluster_id: "cluster".into(),
            node: NodeIncarnation {
                node_id: "server-node".into(),
                incarnation: 1,
            },
            failure_domain: "zone-a".into(),
            voter: true,
        },
    )
}

async fn replication_manager(endpoint: String) -> TonicReplicationStreamManager {
    TonicReplicationStreamManager::new(
        "cluster",
        NodeIncarnation {
            node_id: "crash-client".into(),
            incarnation: 1,
        },
        "node-token",
        [ReplicationPeer {
            cluster_id: "cluster".into(),
            node: NodeIncarnation {
                node_id: "server-node".into(),
                incarnation: 1,
            },
            endpoint,
        }],
        ReplicationStreamOptions {
            allow_insecure_transport_for_tests: true,
            frame_bytes: 4 * 1024,
            ..ReplicationStreamOptions::default()
        },
    )
    .unwrap()
}

async fn serve_and_send(directory: &Path, endpoint: &str) -> crate::replication::ReplicationAck {
    let listener = tokio::net::TcpListener::bind(
        endpoint.trim_start_matches("http://"),
    )
    .await
    .unwrap();
    let service =
        ReplicationServiceImpl::open(CrashReplicationAuthorizer, directory).unwrap();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ReplicationServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let (identity, bytes, target) = crash_bundle();
    let manager = replication_manager(endpoint.to_string()).await;
    let ack = manager.send_bundle(&target, &identity, &bytes).await.unwrap();
    server.abort();
    ack
}

fn bundle_identity(bytes: &[u8]) -> BundleIdentity {
    let mut hash = Sha256::new();
    hash.update(b"anvil.mvcc.transaction-bundle.v1");
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    BundleIdentity {
        hash: format!("sha256:{}", hex::encode(hash.finalize())),
        length: bytes.len() as u64,
    }
}

fn shard_record() -> ShardRecord {
    ShardRecord {
        transaction_id: uuid::Uuid::from_u128(1),
        object_identity: uuid::Uuid::from_u128(2),
        encoding_generation: 1,
        prepared_at_unix_ms: 1,
        stripe_ordinal: 0,
        shard_ordinal: 0,
        shard_kind: ShardKind::Data,
        payload: b"hard-crash-shard".to_vec(),
    }
}

#[test]
fn mvcc_os_crash_child() {
    let Ok(scenario) = std::env::var("ANVIL_MVCC_CRASH_SCENARIO") else {
        return;
    };
    let path = child_path();
    match scenario.as_str() {
        "prepared_bundle" => {
            let bytes = b"hard-crash-prepared-bundle";
            let identity = bundle_identity(bytes);
            let store = AppendOnlyPreparedBundleStore::open(
                path,
                "cluster",
                NodeIncarnation {
                    node_id: "node-a".into(),
                    incarnation: 1,
                },
                "zone-a",
            )
            .unwrap();
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(store.persist(&identity, bytes))
                .unwrap();
        }
        "shard" => {
            ShardSegment::open(path, 1)
                .unwrap()
                .append(&shard_record())
                .unwrap();
        }
        "raft_wal" => {
            let store = RocksRaftStore::open(path, 1)
                .unwrap()
                .with_log_write_fault_hook(Arc::new(|| {
                    crate::mvcc_fault_injection::hit(
                        crate::mvcc_fault_injection::FaultPoint::RaftLogWrite,
                    )
                    .map_err(|error| error.to_string())
                }));
            store.append_logs(&[(0, b"must-not-commit".to_vec())]).unwrap();
        }
        "mvcc_batch" => {
            let key = LogicalKey {
                table_id: 9,
                application_key: b"must-not-be-visible".to_vec(),
            };
            let mut builder = TransactionBundleBuilder::new(
                "cluster",
                "hard-crash-batch",
                0,
                "principal",
                HierarchicalRangeStampScheme::new(),
            );
            builder.put(key, b"value".to_vec());
            LocalMvccStore::open(path)
                .unwrap()
                .apply_certified_bundle(1, &builder.build().unwrap())
                .unwrap();
        }
        "complete_ack_tonic_before" | "complete_ack_tonic_after" => {
            let endpoint =
                std::env::var("ANVIL_MVCC_CRASH_ENDPOINT").unwrap();
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(serve_and_send(&path, &endpoint));
        }
        other => panic!("unknown crash scenario {other}"),
    }
    panic!("child operation returned without reaching hard-crash boundary");
}

#[test]
fn prepared_bundle_crash_reopens_without_false_durability() {
    let directory = tempfile::tempdir().unwrap();
    run_child(
        "prepared_bundle",
        "PreparedBundleWrite",
        directory.path(),
    );
    let bytes = b"hard-crash-prepared-bundle";
    let identity = bundle_identity(bytes);
    let reopened = AppendOnlyPreparedBundleStore::open(
        directory.path(),
        "cluster",
        NodeIncarnation {
            node_id: "node-a".into(),
            incarnation: 1,
        },
        "zone-a",
    )
    .unwrap();
    assert!(reopened.read(&identity).unwrap().is_none());
}

#[test]
fn shard_sync_crash_reopens_without_acknowledgeable_record() {
    let directory = tempfile::tempdir().unwrap();
    run_child("shard", "ShardWrite", directory.path());
    let reopened = ShardSegment::open(directory.path(), 1).unwrap();
    assert_eq!(reopened.path().metadata().unwrap().len(), 0);
}

#[test]
fn raft_wal_crash_reopens_without_unflushed_entry() {
    let directory = tempfile::tempdir().unwrap();
    run_child("raft_wal", "RaftLogWrite", directory.path());
    let reopened = RocksRaftStore::open(directory.path(), 1).unwrap();
    assert_eq!(reopened.last_log_index().unwrap(), None);
    assert!(reopened.get_log(0).unwrap().is_none());
}

#[test]
fn mvcc_batch_crash_reopens_without_partial_visibility() {
    let directory = tempfile::tempdir().unwrap();
    run_child("mvcc_batch", "MvccBatchWrite", directory.path());
    let reopened = LocalMvccStore::open(directory.path()).unwrap();
    let key = LogicalKey {
        table_id: 9,
        application_key: b"must-not-be-visible".to_vec(),
    };
    assert_eq!(reopened.applied_version().unwrap(), 0);
    assert!(reopened.read_latest(&key).unwrap().is_none());
}

async fn assert_tonic_ack_crash_resumes(crash_at: &str, scenario: &str) {
    let directory = tempfile::tempdir().unwrap();
    let endpoint = unused_loopback_endpoint();
    run_tonic_child(scenario, crash_at, directory.path(), &endpoint);
    let ack = serve_and_send(directory.path(), &endpoint).await;
    assert_eq!(ack.status, AckStatus::Complete);
    assert_eq!(
        ack.persisted_through,
        b"durable-before-complete-ack".len() as u64
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tonic_crash_before_complete_ack_restarts_and_resumes_idempotently() {
    assert_tonic_ack_crash_resumes(
        "BeforeCompleteAck",
        "complete_ack_tonic_before",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tonic_crash_after_complete_ack_restarts_and_resumes_idempotently() {
    assert_tonic_ack_crash_resumes(
        "AfterCompleteAck",
        "complete_ack_tonic_after",
    )
    .await;
}
