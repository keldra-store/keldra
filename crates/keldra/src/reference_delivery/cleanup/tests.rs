use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use keldra_store::{
    Durability, LogicalRecordMutationContext, LogicalRecordValue, ObjectKey, ObjectMutationContext,
    PublishRequest, PutMode, ReferenceProof, SourceId, StorageTenantId, StoreOptions, VersionId,
};

use super::*;

#[derive(Clone)]
struct ScriptedPlacement {
    views: Arc<Mutex<VecDeque<ReferenceProofCleanupView>>>,
}

impl ScriptedPlacement {
    fn new(views: impl IntoIterator<Item = ReferenceProofCleanupView>) -> Self {
        let views = views.into_iter().collect::<VecDeque<_>>();
        assert!(!views.is_empty());
        Self {
            views: Arc::new(Mutex::new(views)),
        }
    }
}

impl ReferenceProofCleanupPlacement for ScriptedPlacement {
    fn current(&self) -> Result<ReferenceProofCleanupView, String> {
        let mut views = self.views.lock().expect("placement script lock");
        if views.len() > 1 {
            Ok(views.pop_front().expect("script is nonempty"))
        } else {
            Ok(views.front().expect("script is nonempty").clone())
        }
    }
}

#[derive(Default)]
struct TestStatuses {
    responses: Mutex<BTreeMap<NodeId, Result<WatchJournalStatus, String>>>,
    calls: AtomicUsize,
}

impl TestStatuses {
    fn set(&self, node: NodeId, status: Result<WatchJournalStatus, String>) {
        self.responses
            .lock()
            .expect("source status lock")
            .insert(node, status);
    }
}

#[tonic::async_trait]
impl ReferenceProofSourceStatuses for TestStatuses {
    async fn status(&self, node: NodeId, _address: &str) -> Result<WatchJournalStatus, String> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.responses
            .lock()
            .expect("source status lock")
            .get(&node)
            .cloned()
            .unwrap_or_else(|| Err("source is unavailable".into()))
    }
}

fn view(
    nodes: &[u64],
    fence: u64,
    transition_in_progress: bool,
    reference_reconstruction_safe: bool,
) -> ReferenceProofCleanupView {
    ReferenceProofCleanupView {
        placement_fence: PlacementLogId {
            term: 1,
            index: fence,
        },
        active_sources: nodes
            .iter()
            .map(|node| ActiveReferenceSource {
                node: NodeId(*node),
                address: format!("node-{node}:7443"),
            })
            .collect(),
        transition_in_progress,
        reference_reconstruction_safe,
    }
}

fn status(source_id: SourceId, tail: u64, retention_floor: u64) -> WatchJournalStatus {
    WatchJournalStatus {
        source_id,
        tail,
        settled_through: tail,
        retention_floor,
        retained_entries: tail - retention_floor,
        retained_bytes: 0,
    }
}

async fn proof(store: &Store, path: &str, command: &str) -> ReferenceProof {
    ensure_test_bucket_identity(store);
    let blob = store.stage_blob(command.as_bytes()).await.unwrap();
    let mutation = store
        .coordinate_distributed_publish(
            PublishRequest {
                key: ObjectKey::new("tenant", "bucket", path).unwrap(),
                blob,
                content_type: None,
                mode: PutMode::PutIfAbsent,
                command_id: Some(command.into()),
                durability: Durability::Local,
            },
            ObjectMutationContext {
                active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                serving_fence_term: 1,
            },
        )
        .await
        .unwrap()
        .mutation
        .unwrap();
    store
        .read_reference_proof(
            mutation.stamp.source_id,
            mutation.stamp.source_journal_position,
        )
        .unwrap()
        .unwrap()
}

fn ensure_test_bucket_identity(store: &Store) {
    if store.resolve_bucket_ids("tenant", "bucket").is_ok() {
        return;
    }
    let placement = PlacementLogId { term: 1, index: 1 };
    for (record_version, typed_value) in [
        (
            100,
            LogicalRecordValue::TenantNameClaim {
                storage_tenant: StorageTenantId::parse("tenant").unwrap(),
                tenant_id: 7,
            },
        ),
        (
            101,
            LogicalRecordValue::BucketNameClaim {
                tenant_id: 7,
                bucket: "bucket".into(),
                bucket_id: 11,
            },
        ),
    ] {
        let mutation = store
            .construct_logical_record_mutation(
                typed_value,
                LogicalRecordMutationContext {
                    record_version: VersionId(record_version),
                    active_placement_log_id: placement,
                    serving_fence_term: 1,
                },
            )
            .unwrap();
        store
            .apply_logical_record_mutation_replica(&mutation)
            .unwrap();
    }
}

#[tokio::test]
async fn stable_captured_floors_prune_only_the_inclusive_source_prefixes() {
    let temporary = tempfile::tempdir().unwrap();
    let first_source = Store::open(StoreOptions::new(temporary.path().join("source-1"), 1))
        .await
        .unwrap();
    let second_source = Store::open(StoreOptions::new(temporary.path().join("source-2"), 2))
        .await
        .unwrap();
    let replica = Store::open(StoreOptions::new(temporary.path().join("replica"), 3))
        .await
        .unwrap();
    let first = proof(&first_source, "first/1", "first-1").await;
    let above_floor = proof(&first_source, "first/2", "first-2").await;
    let second = proof(&second_source, "second/1", "second-1").await;
    for candidate in [&first, &above_floor, &second] {
        replica
            .install_quorum_reconciled_reference_proof(candidate)
            .await
            .unwrap();
    }

    let statuses = Arc::new(TestStatuses::default());
    statuses.set(
        NodeId(1),
        Ok(status(
            first.source_id,
            above_floor.offset(),
            first.offset(),
        )),
    );
    statuses.set(
        NodeId(2),
        Ok(status(second.source_id, second.offset(), second.offset())),
    );
    let cleanup = ReferenceProofCleanup::new(
        replica.clone(),
        Arc::new(ScriptedPlacement::new([view(&[1, 2], 7, false, true)])),
        statuses,
    );

    let run = cleanup.run_once().await.unwrap();
    assert_eq!(run.deleted_records, 2);
    assert!(run.complete);
    assert_eq!(run.pause, None);
    assert!(
        replica
            .read_reference_proof(first.source_id, first.offset())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        replica
            .read_reference_proof(above_floor.source_id, above_floor.offset())
            .unwrap(),
        Some(above_floor)
    );
    assert!(
        replica
            .read_reference_proof(second.source_id, second.offset())
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn a_missing_source_pauses_before_any_local_proof_is_deleted() {
    let temporary = tempfile::tempdir().unwrap();
    let source = Store::open(StoreOptions::new(temporary.path().join("source"), 1))
        .await
        .unwrap();
    let replica = Store::open(StoreOptions::new(temporary.path().join("replica"), 3))
        .await
        .unwrap();
    let candidate = proof(&source, "missing", "missing").await;
    replica
        .install_quorum_reconciled_reference_proof(&candidate)
        .await
        .unwrap();
    let statuses = Arc::new(TestStatuses::default());
    statuses.set(
        NodeId(1),
        Ok(status(
            candidate.source_id,
            candidate.offset(),
            candidate.offset(),
        )),
    );
    let cleanup = ReferenceProofCleanup::new(
        replica.clone(),
        Arc::new(ScriptedPlacement::new([view(&[1, 2], 7, false, true)])),
        statuses,
    );

    let run = cleanup.run_once().await.unwrap();
    assert_eq!(
        run.pause,
        Some(ReferenceProofCleanupPause::MissingSource(NodeId(2)))
    );
    assert_eq!(run.deleted_records, 0);
    assert_eq!(
        replica
            .read_reference_proof(candidate.source_id, candidate.offset())
            .unwrap(),
        Some(candidate)
    );
}

#[tokio::test]
async fn transition_and_reconstruction_safety_pause_without_status_reads() {
    for (unsafe_view, expected) in [
        (
            view(&[1], 7, true, true),
            ReferenceProofCleanupPause::MembershipTransition,
        ),
        (
            view(&[1], 7, false, false),
            ReferenceProofCleanupPause::ReferenceReconstruction,
        ),
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 3))
            .await
            .unwrap();
        let statuses = Arc::new(TestStatuses::default());
        let cleanup = ReferenceProofCleanup::new(
            store,
            Arc::new(ScriptedPlacement::new([unsafe_view])),
            statuses.clone(),
        );
        let run = cleanup.run_once().await.unwrap();
        assert_eq!(run.pause, Some(expected));
        assert_eq!(statuses.calls.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn placement_is_rechecked_before_each_source_prune() {
    let temporary = tempfile::tempdir().unwrap();
    let first_source = Store::open(StoreOptions::new(temporary.path().join("source-1"), 1))
        .await
        .unwrap();
    let second_source = Store::open(StoreOptions::new(temporary.path().join("source-2"), 2))
        .await
        .unwrap();
    let replica = Store::open(StoreOptions::new(temporary.path().join("replica"), 3))
        .await
        .unwrap();
    let first = proof(&first_source, "first", "first").await;
    let second = proof(&second_source, "second", "second").await;
    for candidate in [&first, &second] {
        replica
            .install_quorum_reconciled_reference_proof(candidate)
            .await
            .unwrap();
    }
    let statuses = Arc::new(TestStatuses::default());
    statuses.set(
        NodeId(1),
        Ok(status(first.source_id, first.offset(), first.offset())),
    );
    statuses.set(
        NodeId(2),
        Ok(status(second.source_id, second.offset(), second.offset())),
    );
    let stable = view(&[1, 2], 7, false, true);
    let changed = view(&[1, 2], 8, false, true);
    let cleanup = ReferenceProofCleanup::new(
        replica.clone(),
        Arc::new(ScriptedPlacement::new([stable.clone(), stable, changed])),
        statuses,
    );

    let run = cleanup.run_once().await.unwrap();
    assert_eq!(
        run.pause,
        Some(ReferenceProofCleanupPause::PlacementChanged)
    );
    assert_eq!(run.deleted_records, 1);
    assert!(
        replica
            .read_reference_proof(first.source_id, first.offset())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        replica
            .read_reference_proof(second.source_id, second.offset())
            .unwrap(),
        Some(second)
    );
}

#[tokio::test]
async fn a_new_worker_resumes_bounded_cleanup_without_a_cursor() {
    let temporary = tempfile::tempdir().unwrap();
    let source = Store::open(StoreOptions::new(temporary.path().join("source"), 1))
        .await
        .unwrap();
    let replica = Store::open(StoreOptions::new(temporary.path().join("replica"), 3))
        .await
        .unwrap();
    let first = proof(&source, "page/1", "page-1").await;
    let second = proof(&source, "page/2", "page-2").await;
    for candidate in [&first, &second] {
        replica
            .install_quorum_reconciled_reference_proof(candidate)
            .await
            .unwrap();
    }
    let statuses = Arc::new(TestStatuses::default());
    statuses.set(
        NodeId(1),
        Ok(status(first.source_id, second.offset(), second.offset())),
    );
    let placement = Arc::new(ScriptedPlacement::new([view(&[1], 7, false, true)]));

    let first_worker =
        ReferenceProofCleanup::new(replica.clone(), placement.clone(), statuses.clone())
            .with_limits(1, MAX_REFERENCE_PROOF_PRUNE_BYTES);
    let first_run = first_worker.run_once().await.unwrap();
    assert_eq!(first_run.deleted_records, 1);
    assert!(!first_run.complete);
    drop(first_worker);

    let restarted_worker = ReferenceProofCleanup::new(replica, placement, statuses)
        .with_limits(1, MAX_REFERENCE_PROOF_PRUNE_BYTES);
    let second_run = restarted_worker.run_once().await.unwrap();
    assert_eq!(second_run.deleted_records, 1);
    assert!(second_run.complete);
}
