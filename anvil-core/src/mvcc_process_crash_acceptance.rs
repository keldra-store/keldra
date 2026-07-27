//! OS child-process crash/restart acceptance at durable MVCC boundaries.

use std::{path::Path, process::Command, sync::Arc};

use anvil_mvcc_consensus::RocksRaftStore;
use sha2::{Digest, Sha256};

use crate::{
    bundle_replication::AppendOnlyPreparedBundleStore,
    mvcc_store::LocalMvccStore,
    mvcc_transaction::{
        BundleIdentity, HierarchicalRangeStampScheme, LogicalKey, NodeIncarnation,
        PreparedBundleStore, TransactionBundleBuilder,
    },
    replication::{
        ConnectionSession, ReplicationFrame, TransferKind, TransferReceiver,
    },
    shard_store::{ShardKind, ShardRecord, ShardSegment},
};

const CHILD_TEST: &str =
    "mvcc_process_crash_acceptance::mvcc_os_crash_child";
const TRANSFER_ID: uuid::Uuid =
    uuid::Uuid::from_u128(0x89abcdef_01234567_89abcdef_01234567);

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
        "complete_ack" => {
            let payload = b"durable-before-complete-ack";
            let final_hash = *blake3::hash(payload).as_bytes();
            let peer = crate::replication::AuthenticatedPeer::new("node-a", 1).unwrap();
            let mut session = ConnectionSession::establish("cluster", peer).unwrap();
            let frame = ReplicationFrame {
                session_id: session.id(),
                cluster_id: "cluster".into(),
                sequence: 1,
                partition: "crash/ack".into(),
                transfer_id: TRANSFER_ID,
                kind: TransferKind::ObjectShard,
                offset: 0,
                payload: payload.to_vec(),
                payload_checksum: ReplicationFrame::checksum(payload),
                total_length: payload.len() as u64,
                final_hash,
                finish: true,
            };
            TransferReceiver::open(path)
                .unwrap()
                .receive(&mut session, &frame)
                .unwrap();
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

#[test]
fn complete_ack_crash_reopens_durable_transfer_without_false_response() {
    let directory = tempfile::tempdir().unwrap();
    run_child("complete_ack", "BeforeCompleteAck", directory.path());
    let reopened = TransferReceiver::open(directory.path()).unwrap();
    let watermark = reopened
        .watermark(TRANSFER_ID)
        .unwrap()
        .expect("sync completed before the child died");
    assert!(watermark.complete);
    assert_eq!(
        watermark.persisted_through,
        b"durable-before-complete-ack".len() as u64
    );
}
