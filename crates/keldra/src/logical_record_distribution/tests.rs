use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use keldra_consensus::{ClusterId, NodeId};
use keldra_store::{
    BucketPolicy, LogicalRecordMutationContext, LogicalRecordValue, PlacementLogId, StoreOptions,
    VersionId, WatchRetention,
};
use tonic::Code;

use super::*;
use crate::placement::PlacementNode;

#[derive(Default)]
struct StoreTransport {
    stores: BTreeMap<NodeId, Store>,
    failed_applies: Mutex<BTreeSet<NodeId>>,
    failed_repairs: Mutex<BTreeSet<NodeId>>,
    block_reads: AtomicBool,
    reads_started: AtomicUsize,
    reads_changed: tokio::sync::Notify,
    release_reads: tokio::sync::Notify,
}

impl StoreTransport {
    fn fail_apply(&self, node: NodeId) {
        self.failed_applies.lock().unwrap().insert(node);
    }

    fn fail_repair(&self, node: NodeId) {
        self.failed_repairs.lock().unwrap().insert(node);
    }

    async fn wait_for_reads(&self, expected: usize) {
        loop {
            let changed = self.reads_changed.notified();
            if self.reads_started.load(Ordering::SeqCst) >= expected {
                return;
            }
            changed.await;
        }
    }

    fn release_reads(&self) {
        self.block_reads.store(false, Ordering::SeqCst);
        self.release_reads.notify_waiters();
    }
}

#[tonic::async_trait]
impl LogicalRecordReplicaTransport for StoreTransport {
    async fn read_candidate(
        &self,
        target: NodeId,
        _address: &str,
        id: &LogicalRecordId,
    ) -> Result<Option<LogicalRecordCandidate>, Status> {
        if self.block_reads.load(Ordering::SeqCst) {
            self.reads_started.fetch_add(1, Ordering::SeqCst);
            self.reads_changed.notify_waiters();
            self.release_reads.notified().await;
        }
        self.stores[&target]
            .logical_record_candidate(id)
            .map_err(logical_record_status)
    }

    async fn repair_candidate(
        &self,
        target: NodeId,
        _address: &str,
        id: &LogicalRecordId,
        candidate: Option<&LogicalRecordCandidate>,
    ) -> Result<LogicalRecordSnapshotApplied, Status> {
        if self.failed_repairs.lock().unwrap().contains(&target) {
            return Err(Status::unavailable("injected repair failure"));
        }
        self.stores[&target]
            .repair_quorum_reconciled_logical_record(id, candidate)
            .map_err(logical_record_status)
    }

    async fn apply_mutation(
        &self,
        target: NodeId,
        _address: &str,
        mutation: &LogicalRecordMutation,
    ) -> Result<LogicalRecordApplied, Status> {
        if self.failed_applies.lock().unwrap().contains(&target) {
            return Err(Status::unavailable("injected replica failure"));
        }
        self.stores[&target]
            .apply_logical_record_mutation_replica(mutation)
            .map_err(logical_record_status)
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    stores: BTreeMap<NodeId, Store>,
    transport: Arc<StoreTransport>,
    route: LogicalRecordRoute,
    core: LogicalRecordDistributionCore,
}

async fn fixture(active_count: usize) -> Fixture {
    fixture_with_retention(active_count, None).await
}

async fn fixture_with_retention(
    active_count: usize,
    watch_retention: Option<WatchRetention>,
) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let mut stores = BTreeMap::new();
    let mut nodes = Vec::with_capacity(active_count);
    for id in 1..=active_count as u64 {
        let node = NodeId(id);
        let options = StoreOptions::new(root.path().join(format!("node-{id}")), id as u16);
        stores.insert(
            node,
            Store::open(match watch_retention {
                Some(retention) => options.with_watch_retention(retention),
                None => options,
            })
            .await
            .unwrap(),
        );
        nodes.push(PlacementNode::new(
            node,
            NonZeroU32::new(1_000_000).unwrap(),
        ));
    }
    let (kind, key) = placement_key(&policy_value("initial").id()).unwrap();
    let group = MutableRecordReplicaGroup::select(kind, ClusterId([9; 16]), &key, &nodes).unwrap();
    let endpoints = group
        .replicas()
        .iter()
        .map(|node_id| ReplicaEndpoint {
            node_id: *node_id,
            address: format!("node-{}", node_id.0),
        })
        .collect();
    let route = LogicalRecordRoute {
        group,
        endpoints,
        active_placement_log_id: PlacementLogId { term: 3, index: 7 },
        serving_fence_term: 5,
    };
    let local_node = route.group.coordinator();
    let transport = Arc::new(StoreTransport {
        stores: stores.clone(),
        ..Default::default()
    });
    let core = LogicalRecordDistributionCore {
        local_node,
        store: stores[&local_node].clone(),
        peers: transport.clone(),
        coordinator_serial: Arc::new(tokio::sync::Mutex::new(())),
        mutation_admission: crate::mutation_admission::MutationAdmission::new(),
    };
    Fixture {
        _root: root,
        stores,
        transport,
        route,
        core,
    }
}

fn policy_value(prefix: &str) -> LogicalRecordValue {
    LogicalRecordValue::BucketPolicy {
        tenant_id: 7,
        bucket_id: 11,
        policy: BucketPolicy {
            immutable_prefixes: vec![prefix.to_owned()],
            program_only_prefixes: Vec::new(),
        },
    }
}

fn context(route: &LogicalRecordRoute, version: u64) -> LogicalRecordMutationContext {
    LogicalRecordMutationContext {
        record_version: VersionId(version),
        active_placement_log_id: route.active_placement_log_id,
        serving_fence_term: route.serving_fence_term,
    }
}

fn install(store: &Store, value: LogicalRecordValue, context: LogicalRecordMutationContext) {
    let mutation = store
        .construct_logical_record_mutation(value, context)
        .unwrap();
    store
        .apply_logical_record_mutation_replica(&mutation)
        .unwrap();
}

#[tokio::test]
async fn one_two_and_three_node_groups_apply_their_fixed_quorums() {
    for active_count in [1, 2, 3] {
        let fixture = fixture(active_count).await;
        if active_count == 3 {
            fixture
                .transport
                .fail_apply(fixture.route.group.replicas()[2]);
        }
        let applied = fixture
            .core
            .coordinate(&fixture.route, policy_value("committed"), || Ok(()))
            .await
            .unwrap();
        assert_ne!(applied.record_version, VersionId(0));
        let durable = fixture
            .route
            .group
            .replicas()
            .iter()
            .filter(|node| {
                fixture
                    .stores
                    .get(*node)
                    .unwrap()
                    .logical_record_candidate(&policy_value("committed").id())
                    .unwrap()
                    .is_some()
            })
            .count();
        assert_eq!(durable, active_count.min(2));
    }

    let fixture = fixture(2).await;
    fixture
        .transport
        .fail_apply(fixture.route.group.replicas()[1]);
    assert_eq!(
        fixture
            .core
            .coordinate(&fixture.route, policy_value("not-quorate"), || Ok(()),)
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
}

#[tokio::test]
async fn exact_journal_capacity_blocks_without_mutation_then_wakes_and_retries() {
    let fixture =
        fixture_with_retention(1, Some(WatchRetention::new(1, 1024 * 1024).unwrap())).await;
    let first_value = policy_value("first");
    let first = fixture
        .core
        .coordinate(&fixture.route, first_value.clone(), || Ok(()))
        .await
        .unwrap();
    let coordinator = fixture.route.group.coordinator();
    let store = fixture.stores[&coordinator].clone();
    assert_eq!(store.local_watch_status().unwrap().tail, 1);

    let core = fixture.core.clone();
    let route = fixture.route.clone();
    let mut waiting = tokio::spawn(async move {
        core.coordinate(&route, policy_value("second"), || Ok(()))
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut waiting)
            .await
            .is_err(),
        "the second mutation must wait at the exact journal bound"
    );
    assert_eq!(store.local_watch_status().unwrap().tail, 1);
    let current = store
        .logical_record_candidate(&first_value.id())
        .unwrap()
        .unwrap();
    let LogicalRecordCandidate::Versioned(current) = current else {
        panic!("the first logical record is not versioned")
    };
    assert_eq!(current.record_version, first.record_version);
    assert_eq!(current.typed_value, first_value);

    let serial = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.core.coordinator_serial.lock(),
    )
    .await
    .expect("capacity waiting must release the coordinator serialization lock");
    drop(serial);

    store
        .advance_source_journal_reference_safe_through(1)
        .await
        .unwrap();
    let second = tokio::time::timeout(std::time::Duration::from_secs(2), waiting)
        .await
        .expect("the capacity notification must wake the coordinator")
        .unwrap()
        .unwrap();
    assert_ne!(second.record_version, first.record_version);
    let status = store.local_watch_status().unwrap();
    assert_eq!(status.tail, 2);
    assert_eq!(status.retention_floor, 1);
    assert_eq!(status.retained_entries, 1);
    let current = store
        .logical_record_candidate(&first_value.id())
        .unwrap()
        .unwrap();
    let LogicalRecordCandidate::Versioned(current) = current else {
        panic!("the retried logical record is not versioned")
    };
    assert_eq!(current.record_version, second.record_version);
    assert_eq!(current.typed_value, policy_value("second"));
}

#[tokio::test]
async fn quorum_proven_successor_discards_a_higher_version_minority_sibling() {
    let fixture = fixture(3).await;
    let replicas = fixture.route.group.replicas();
    let winner_value = policy_value("winner");
    let winner = fixture.stores[&replicas[0]]
        .construct_logical_record_mutation(winner_value.clone(), context(&fixture.route, 100))
        .unwrap();
    fixture.stores[&replicas[0]]
        .apply_logical_record_mutation_replica(&winner)
        .unwrap();
    fixture.stores[&replicas[1]]
        .apply_logical_record_mutation_replica(&winner)
        .unwrap();
    install(
        &fixture.stores[&replicas[2]],
        policy_value("higher-minority"),
        context(&fixture.route, 200),
    );

    let selected = fixture
        .core
        .reconcile(&fixture.route, &winner_value.id())
        .await
        .unwrap();
    assert_eq!(
        selected,
        Some(LogicalRecordCandidate::Versioned(winner.clone()))
    );
    for replica in replicas {
        assert_eq!(
            fixture.stores[replica]
                .logical_record_candidate(&winner_value.id())
                .unwrap(),
            selected
        );
    }
}

#[tokio::test]
async fn quorum_proven_current_state_repairs_a_multi_version_stale_replica() {
    let fixture = fixture(3).await;
    let replicas = fixture.route.group.replicas();
    let id = policy_value("first").id();
    let first = fixture.stores[&replicas[0]]
        .construct_logical_record_mutation(policy_value("first"), context(&fixture.route, 100))
        .unwrap();
    for replica in replicas {
        fixture.stores[replica]
            .apply_logical_record_mutation_replica(&first)
            .unwrap();
    }
    let second = fixture.stores[&replicas[0]]
        .construct_logical_record_mutation(policy_value("second"), context(&fixture.route, 200))
        .unwrap();
    for replica in &replicas[..2] {
        fixture.stores[replica]
            .apply_logical_record_mutation_replica(&second)
            .unwrap();
    }
    let third = fixture.stores[&replicas[0]]
        .construct_logical_record_mutation(policy_value("third"), context(&fixture.route, 300))
        .unwrap();
    for replica in &replicas[..2] {
        fixture.stores[replica]
            .apply_logical_record_mutation_replica(&third)
            .unwrap();
    }

    let selected = fixture.core.reconcile(&fixture.route, &id).await.unwrap();
    assert_eq!(
        selected,
        Some(LogicalRecordCandidate::Versioned(third.clone()))
    );
    for replica in replicas {
        assert_eq!(
            fixture.stores[replica]
                .logical_record_candidate(&id)
                .unwrap(),
            selected
        );
    }
}

#[tokio::test]
async fn valid_successor_of_quorum_absence_is_completed() {
    let fixture = fixture(3).await;
    let value = policy_value("complete-me");
    let candidate = fixture.stores[&fixture.route.group.coordinator()]
        .construct_logical_record_mutation(value.clone(), context(&fixture.route, 200))
        .unwrap();
    fixture.stores[&fixture.route.group.coordinator()]
        .apply_logical_record_mutation_replica(&candidate)
        .unwrap();

    assert_eq!(
        fixture
            .core
            .reconcile(&fixture.route, &value.id())
            .await
            .unwrap(),
        Some(LogicalRecordCandidate::Versioned(candidate.clone()))
    );
    for replica in fixture.route.group.replicas() {
        assert_eq!(
            fixture.stores[replica]
                .logical_record_candidate(&value.id())
                .unwrap(),
            Some(LogicalRecordCandidate::Versioned(candidate.clone()))
        );
    }
}

#[tokio::test]
async fn two_node_split_selects_and_repairs_the_direct_successor() {
    let fixture = fixture(2).await;
    let replicas = fixture.route.group.replicas();
    let first_value = policy_value("first");
    let first = fixture.stores[&replicas[0]]
        .construct_logical_record_mutation(first_value.clone(), context(&fixture.route, 100))
        .unwrap();
    for replica in replicas {
        fixture.stores[replica]
            .apply_logical_record_mutation_replica(&first)
            .unwrap();
    }
    let successor = fixture.stores[&replicas[1]]
        .construct_logical_record_mutation(policy_value("successor"), context(&fixture.route, 200))
        .unwrap();
    fixture.stores[&replicas[1]]
        .apply_logical_record_mutation_replica(&successor)
        .unwrap();

    let selected = fixture
        .core
        .reconcile(&fixture.route, &first_value.id())
        .await
        .unwrap();
    assert_eq!(
        selected,
        Some(LogicalRecordCandidate::Versioned(successor.clone()))
    );
    for replica in replicas {
        assert_eq!(
            fixture.stores[replica]
                .logical_record_candidate(&first_value.id())
                .unwrap(),
            selected
        );
    }
}

#[tokio::test]
async fn two_node_recovery_waits_for_both_replicas() {
    let fixture = fixture(2).await;
    let replicas = fixture.route.group.replicas();
    let first_value = policy_value("first");
    let first = fixture.stores[&replicas[0]]
        .construct_logical_record_mutation(first_value.clone(), context(&fixture.route, 100))
        .unwrap();
    for replica in replicas {
        fixture.stores[replica]
            .apply_logical_record_mutation_replica(&first)
            .unwrap();
    }
    let successor = fixture.stores[&replicas[0]]
        .construct_logical_record_mutation(policy_value("successor"), context(&fixture.route, 200))
        .unwrap();
    fixture.stores[&replicas[0]]
        .apply_logical_record_mutation_replica(&successor)
        .unwrap();
    fixture.transport.fail_repair(replicas[1]);

    assert_eq!(
        fixture
            .core
            .reconcile(&fixture.route, &first_value.id())
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
}

#[tokio::test]
async fn two_node_split_with_missing_predecessor_evidence_fails_closed() {
    let fixture = fixture(2).await;
    let replicas = fixture.route.group.replicas();
    let id = policy_value("left").id();
    install(
        &fixture.stores[&replicas[0]],
        policy_value("left"),
        context(&fixture.route, 100),
    );
    let middle = fixture.stores[&replicas[1]]
        .construct_logical_record_mutation(policy_value("middle"), context(&fixture.route, 150))
        .unwrap();
    fixture.stores[&replicas[1]]
        .apply_logical_record_mutation_replica(&middle)
        .unwrap();
    let right = fixture.stores[&replicas[1]]
        .construct_logical_record_mutation(policy_value("right"), context(&fixture.route, 200))
        .unwrap();
    fixture.stores[&replicas[1]]
        .apply_logical_record_mutation_replica(&right)
        .unwrap();

    assert_eq!(
        fixture
            .core
            .reconcile(&fixture.route, &id)
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
}

#[tokio::test]
async fn two_node_contradictory_siblings_fail_closed() {
    let fixture = fixture(2).await;
    let replicas = fixture.route.group.replicas();
    let id = policy_value("left").id();
    install(
        &fixture.stores[&replicas[0]],
        policy_value("left"),
        context(&fixture.route, 100),
    );
    install(
        &fixture.stores[&replicas[1]],
        policy_value("right"),
        context(&fixture.route, 200),
    );

    assert_eq!(
        fixture
            .core
            .reconcile(&fixture.route, &id)
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
}

#[tokio::test]
async fn prewrite_repair_replaces_local_minority_before_constructing_successor() {
    let fixture = fixture(3).await;
    let replicas = fixture.route.group.replicas();
    let winner_value = policy_value("winner");
    let winner = fixture.stores[&replicas[1]]
        .construct_logical_record_mutation(winner_value.clone(), context(&fixture.route, 100))
        .unwrap();
    fixture.stores[&replicas[1]]
        .apply_logical_record_mutation_replica(&winner)
        .unwrap();
    fixture.stores[&replicas[2]]
        .apply_logical_record_mutation_replica(&winner)
        .unwrap();
    install(
        &fixture.stores[&replicas[0]],
        policy_value("local-minority"),
        context(&fixture.route, 200),
    );

    fixture
        .core
        .coordinate(&fixture.route, policy_value("successor"), || Ok(()))
        .await
        .unwrap();
    let expected = fixture.stores[&replicas[0]]
        .logical_record_candidate(&winner_value.id())
        .unwrap();
    let Some(LogicalRecordCandidate::Versioned(expected)) = expected else {
        panic!("successor mutation is missing")
    };
    assert_eq!(
        expected.predecessor,
        keldra_store::LogicalRecordPredecessor::VersionId(VersionId(100))
    );
    for replica in replicas {
        assert_eq!(
            fixture.stores[replica]
                .logical_record_candidate(&winner_value.id())
                .unwrap(),
            Some(LogicalRecordCandidate::Versioned(expected.clone()))
        );
    }
}

#[tokio::test]
async fn retry_after_lost_response_returns_the_exact_committed_version() {
    let fixture = fixture(3).await;
    fixture
        .transport
        .fail_apply(fixture.route.group.replicas()[2]);
    let value = LogicalRecordValue::TenantNameClaim {
        storage_tenant: keldra_store::StorageTenantId::parse("same-command").unwrap(),
        tenant_id: 99,
    };
    let first = fixture
        .core
        .coordinate(&fixture.route, value.clone(), || Ok(()))
        .await
        .unwrap();
    assert!(!first.replayed);

    let retry = fixture
        .core
        .coordinate(&fixture.route, value.clone(), || Ok(()))
        .await
        .unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.record_version, first.record_version);
    for replica in fixture.route.group.replicas() {
        let candidate = fixture.stores[replica]
            .logical_record_candidate(&value.id())
            .unwrap()
            .unwrap();
        let LogicalRecordCandidate::Versioned(mutation) = candidate else {
            panic!("committed retry candidate is not versioned")
        };
        assert_eq!(mutation.record_version, first.record_version);
    }
}

#[tokio::test]
async fn one_process_mutex_serializes_complete_reconcile_mutate_sequences() {
    let fixture = fixture(3).await;
    fixture.transport.block_reads.store(true, Ordering::SeqCst);
    let first_core = fixture.core.clone();
    let first_route = fixture.route.clone();
    let first = tokio::spawn(async move {
        first_core
            .coordinate(&first_route, policy_value("first"), || Ok(()))
            .await
    });
    fixture.transport.wait_for_reads(2).await;

    let second_core = fixture.core.clone();
    let second_route = fixture.route.clone();
    let second = tokio::spawn(async move {
        second_core
            .coordinate(&second_route, policy_value("second"), || Ok(()))
            .await
    });
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert_eq!(fixture.transport.reads_started.load(Ordering::SeqCst), 2);
    fixture.transport.release_reads();

    let first = first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    let candidate = fixture.stores[&fixture.route.group.coordinator()]
        .logical_record_candidate(&policy_value("second").id())
        .unwrap()
        .unwrap();
    let LogicalRecordCandidate::Versioned(mutation) = candidate else {
        panic!("serialized successor is missing")
    };
    assert_eq!(
        mutation.predecessor,
        keldra_store::LogicalRecordPredecessor::VersionId(first.record_version)
    );
}

#[tokio::test]
async fn coordinator_rechecks_the_exact_fence_before_mutation_and_return() {
    let fixture = fixture(1).await;
    let checks = AtomicUsize::new(0);
    fixture
        .core
        .coordinate(&fixture.route, policy_value("fenced"), || {
            checks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(checks.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn every_selected_replica_may_coordinate_a_quorum_read() {
    let fixture = fixture(3).await;
    for replica in fixture.route.group.replicas() {
        require_local_read_replica(&fixture.route, *replica).unwrap();
    }
    assert_eq!(
        require_local_read_replica(&fixture.route, NodeId(999))
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
}

#[test]
fn selected_read_targets_prefer_local_without_losing_ranked_fallbacks() {
    let target = |node_id| LogicalRecordReadTarget {
        node_id: NodeId(node_id),
        address: format!("node-{node_id}"),
        placement_fence: PlacementLogId { term: 3, index: 7 },
    };
    assert_eq!(
        order_read_targets_local_first(vec![target(1), target(2), target(3)], NodeId(2)),
        vec![target(2), target(1), target(3)]
    );
    assert_eq!(
        order_read_targets_local_first(vec![target(1), target(2), target(3)], NodeId(9)),
        vec![target(1), target(2), target(3)]
    );
}

#[test]
fn placement_keys_use_only_the_fixed_rfc_encodings() {
    let (_, tenant) = placement_key(&LogicalRecordId::TenantRecord { tenant_id: 7 }).unwrap();
    assert_eq!(tenant, 7_u64.to_be_bytes());
    let (_, bucket) = placement_key(&LogicalRecordId::BucketOptions {
        tenant_id: 7,
        bucket_id: 11,
    })
    .unwrap();
    assert_eq!(bucket, [7_u64.to_be_bytes(), 11_u64.to_be_bytes()].concat());
    let (kind, bucket_name) = placement_key(&LogicalRecordId::BucketNameClaim {
        tenant_id: 7,
        bucket: "mutable-name".into(),
    })
    .unwrap();
    assert_eq!(kind, PlacementKind::TenantOrBucketRecord);
    assert_eq!(
        bucket_name,
        [7_u64.to_be_bytes().as_slice(), b"mutable-name"].concat()
    );
    assert_eq!(
        placement_key(&LogicalRecordId::TenantSchema {
            storage_tenant: keldra_store::StorageTenantId::parse("tenant").unwrap(),
            schema_ref: keldra_store::SchemaRef {
                schema_id: keldra_store::SchemaId::parse("default").unwrap(),
                schema_revision: 1,
                schema_digest: keldra_store::SchemaDigest([0; 32]),
            },
        })
        .unwrap_err()
        .code(),
        Code::Unimplemented
    );
}
