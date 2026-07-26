//! Crash-boundary acceptance tests for MVCC-under-Raft durability.
//!
//! Dropping every open handle and constructing a fresh store is the strongest
//! hard-restart equivalent available to these storage-level tests. The final
//! ACK case explicitly models a crash between durable append and ACK emission;
//! it does not claim to kill an operating-system process.

use std::sync::Arc;

use anvil_mvcc_consensus::RocksRaftStore;
use sha2::{Digest, Sha256};

use crate::{
    bundle_replication::AppendOnlyPreparedBundleStore,
    mvcc_fault_injection::{self, DeterministicFaults, FaultPoint},
    mvcc_store::LocalMvccStore,
    mvcc_transaction::{
        BundleIdentity, HierarchicalRangeStampScheme, LogicalKey, NodeIncarnation,
        PreparedBundleStore, TransactionBundleBuilder,
    },
    shard_store::{ShardKind, ShardRecord, ShardSegment},
};

struct ClearFault;

impl Drop for ClearFault {
    fn drop(&mut self) {
        mvcc_fault_injection::clear();
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

fn shard(payload: &[u8]) -> ShardRecord {
    ShardRecord {
        transaction_id: uuid::Uuid::from_u128(1),
        object_identity: uuid::Uuid::from_u128(2),
        encoding_generation: 1,
        prepared_at_unix_ms: 1,
        stripe_ordinal: 0,
        shard_ordinal: 0,
        shard_kind: ShardKind::Data,
        payload: payload.to_vec(),
    }
}

#[tokio::test]
async fn prepared_bundle_sync_failure_reopens_without_false_durability() {
    let _clear = ClearFault;
    let directory = tempfile::tempdir().unwrap();
    let bytes = b"prepared bundle";
    let identity = bundle_identity(bytes);
    {
        let store = AppendOnlyPreparedBundleStore::open(
            directory.path(),
            "cluster",
            NodeIncarnation {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            "zone-a",
        )
        .unwrap();
        mvcc_fault_injection::install(
            DeterministicFaults::default().fail_at(FaultPoint::PreparedBundleWrite, 1),
        );
        assert!(store.persist(&identity, bytes).await.is_err());
    }
    mvcc_fault_injection::clear();
    let restarted = AppendOnlyPreparedBundleStore::open(
        directory.path(),
        "cluster",
        NodeIncarnation {
            node_id: "node-a".into(),
            incarnation: 1,
        },
        "zone-a",
    )
    .unwrap();
    assert!(restarted.read(&identity).unwrap().is_none());
    restarted.persist(&identity, bytes).await.unwrap();
    assert_eq!(
        restarted.read(&identity).unwrap().as_deref(),
        Some(bytes.as_slice())
    );
}

#[test]
fn shard_sync_failure_reopens_without_acknowledgeable_record() {
    let _clear = ClearFault;
    let directory = tempfile::tempdir().unwrap();
    {
        let mut segment = ShardSegment::open(directory.path(), 1).unwrap();
        mvcc_fault_injection::install(
            DeterministicFaults::default().fail_at(FaultPoint::ShardWrite, 1),
        );
        assert!(segment.append(&shard(b"shard")).is_err());
    }
    mvcc_fault_injection::clear();
    let restarted = ShardSegment::open(directory.path(), 1).unwrap();
    assert_eq!(restarted.path().metadata().unwrap().len(), 0);
}

#[test]
fn raft_wal_append_failure_reopens_at_previous_log_tip() {
    let directory = tempfile::tempdir().unwrap();
    let store = RocksRaftStore::open(directory.path(), 7).unwrap();
    store.append_logs(&[(0, b"committed".to_vec())]).unwrap();
    let failing = store.with_log_write_fault_hook(Arc::new(|| Err("crash at WAL append".into())));
    assert!(
        failing
            .append_logs(&[(1, b"uncommitted".to_vec())])
            .is_err()
    );
    drop(failing);

    let restarted = RocksRaftStore::open(directory.path(), 7).unwrap();
    assert_eq!(restarted.last_log_index().unwrap(), Some(0));
    assert_eq!(
        restarted.get_log(0).unwrap().as_deref(),
        Some(b"committed".as_slice())
    );
    assert!(restarted.get_log(1).unwrap().is_none());
}

#[test]
fn mvcc_batch_failure_reopens_with_no_partial_rows_or_watermark() {
    let _clear = ClearFault;
    let directory = tempfile::tempdir().unwrap();
    let first = LogicalKey {
        table_id: 7,
        application_key: b"first".to_vec(),
    };
    let second = LogicalKey {
        table_id: 8,
        application_key: b"second".to_vec(),
    };
    let mut builder = TransactionBundleBuilder::new(
        "cluster",
        "atomic-crash",
        0,
        "principal",
        HierarchicalRangeStampScheme::new(),
    );
    builder.put(first.clone(), b"a".to_vec());
    builder.put(second.clone(), b"b".to_vec());
    let bundle = builder.build().unwrap();
    {
        let store = LocalMvccStore::open(directory.path()).unwrap();
        mvcc_fault_injection::install(
            DeterministicFaults::default().fail_at(FaultPoint::MvccBatchWrite, 1),
        );
        assert!(store.apply_certified_bundle(1, &bundle).is_err());
    }
    mvcc_fault_injection::clear();
    let restarted = LocalMvccStore::open(directory.path()).unwrap();
    assert_eq!(restarted.applied_version().unwrap(), 0);
    assert!(restarted.read_latest(&first).unwrap().is_none());
    assert!(restarted.read_latest(&second).unwrap().is_none());
}

#[test]
fn crash_equivalent_after_shard_sync_before_ack_reopens_durable_bytes() {
    let _clear = ClearFault;
    let directory = tempfile::tempdir().unwrap();
    let location = {
        let mut segment = ShardSegment::open(directory.path(), 1).unwrap();
        let location = segment.append(&shard(b"durable-before-ack")).unwrap();
        mvcc_fault_injection::install(
            DeterministicFaults::default().fail_at(FaultPoint::BeforeCompleteAck, 1),
        );
        assert!(mvcc_fault_injection::hit(FaultPoint::BeforeCompleteAck).is_err());
        location
    };
    mvcc_fault_injection::clear();
    let mut restarted = ShardSegment::open(directory.path(), 1).unwrap();
    assert_eq!(
        restarted.read(&location).unwrap(),
        b"durable-before-ack",
        "a missing ACK must not imply the already-synced shard was lost"
    );
}
