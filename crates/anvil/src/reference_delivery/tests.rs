use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, RwLock};

use anvil_store::{
    BlobReferenceState, Durability, LogicalRecordMutationContext, LogicalRecordValue, ObjectKey,
    ObjectMutationContext, PlacementLogId, PublishRequest, PutMode, PutRequest, ReferenceDelta,
    StorageTenantId, StoreOptions, VersionId, WatchRetention,
};
use tempfile::TempDir;

use super::*;

fn placement(node_ids: &[u64], fence: u64) -> ReferencePlacement {
    let nodes = node_ids
        .iter()
        .map(|node| {
            PlacementNode::new(
                NodeId(*node),
                NonZeroU32::new(1_000_000).expect("test weight is positive"),
            )
        })
        .collect::<Vec<_>>();
    let addresses = node_ids
        .iter()
        .map(|node| (NodeId(*node), format!("node-{node}:7443")))
        .collect();
    ReferencePlacement {
        cluster_id: ClusterId(*b"ref-delivery-v1!"),
        fence: PlacementLogId {
            term: 1,
            index: fence,
        },
        nodes,
        addresses,
    }
}

#[derive(Clone)]
struct TestPlacement(Arc<RwLock<ReferencePlacement>>);

impl TestPlacement {
    fn new(placement: ReferencePlacement) -> Self {
        Self(Arc::new(RwLock::new(placement)))
    }

    fn replace(&self, placement: ReferencePlacement) {
        *self.0.write().expect("test placement lock") = placement;
    }
}

impl ReferencePlacementAuthority for TestPlacement {
    fn current(&self) -> Result<ReferencePlacement, String> {
        Ok(self.0.read().expect("test placement lock").clone())
    }
}

#[derive(Default)]
struct TestCommits {
    dispositions: Mutex<BTreeMap<u64, Result<ReferenceCommitDisposition, String>>>,
}

impl TestCommits {
    fn set(&self, offset: u64, disposition: Result<ReferenceCommitDisposition, String>) {
        self.dispositions
            .lock()
            .expect("test commit lock")
            .insert(offset, disposition);
    }
}

#[tonic::async_trait]
impl ReferenceCommitAuthority for TestCommits {
    async fn classify(
        &self,
        _source: SourceId,
        change: &LocalChange,
    ) -> Result<ReferenceCommitDisposition, String> {
        self.dispositions
            .lock()
            .expect("test commit lock")
            .get(&change.offset())
            .cloned()
            .unwrap_or(Ok(ReferenceCommitDisposition::CommittedOrAncestor))
    }
}

#[derive(Default)]
struct TestProofPeers {
    responses: Mutex<BTreeMap<NodeId, Result<Option<ReferenceProof>, String>>>,
    reads: Mutex<Vec<(NodeId, String, ReferenceProofRead)>>,
    placement_change: Mutex<Option<(TestPlacement, ReferencePlacement)>>,
}

impl TestProofPeers {
    fn respond(&self, node: NodeId, response: Result<Option<ReferenceProof>, String>) {
        self.responses
            .lock()
            .expect("test proof response lock")
            .insert(node, response);
    }

    fn change_placement_on_read(&self, authority: TestPlacement, replacement: ReferencePlacement) {
        *self
            .placement_change
            .lock()
            .expect("test placement-change lock") = Some((authority, replacement));
    }
}

#[tonic::async_trait]
impl ReferenceProofPeers for TestProofPeers {
    async fn read_reference_proof(
        &self,
        node: NodeId,
        address: &str,
        request: ReferenceProofRead,
    ) -> Result<Option<ReferenceProof>, String> {
        self.reads
            .lock()
            .expect("test proof-read lock")
            .push((node, address.to_owned(), request));
        if let Some((authority, replacement)) = self
            .placement_change
            .lock()
            .expect("test placement-change lock")
            .take()
        {
            authority.replace(replacement);
        }
        self.responses
            .lock()
            .expect("test proof response lock")
            .get(&node)
            .cloned()
            .unwrap_or_else(|| Err("test peer has no configured response".into()))
    }
}

#[derive(Clone)]
struct TestDestinations {
    stores: Arc<BTreeMap<NodeId, Store>>,
    fail_once: Arc<Mutex<BTreeSet<NodeId>>>,
    batches: Arc<Mutex<Vec<(NodeId, ReferenceDeltaBatch)>>>,
    order: Arc<Mutex<Vec<String>>>,
}

impl TestDestinations {
    fn new(stores: Arc<BTreeMap<NodeId, Store>>, order: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            stores,
            fail_once: Arc::new(Mutex::new(BTreeSet::new())),
            batches: Arc::new(Mutex::new(Vec::new())),
            order,
        }
    }

    fn fail_next(&self, node: NodeId) {
        self.fail_once
            .lock()
            .expect("test failure lock")
            .insert(node);
    }
}

#[tonic::async_trait]
impl ReferenceDestinations for TestDestinations {
    async fn cursor(&self, node: NodeId, _address: &str, source: SourceId) -> Result<u64, String> {
        self.stores[&node]
            .reference_delta_cursor(source)
            .map_err(|error| error.to_string())
    }

    async fn apply(
        &self,
        node: NodeId,
        _address: &str,
        batch: ReferenceDeltaBatch,
    ) -> Result<ReferenceDeltaApplied, String> {
        self.batches
            .lock()
            .expect("test batch lock")
            .push((node, batch.clone()));
        self.order
            .lock()
            .expect("test order lock")
            .push(format!("apply:{}", node.0));
        if self
            .fail_once
            .lock()
            .expect("test failure lock")
            .remove(&node)
        {
            return Err("injected one-shot failure".into());
        }
        self.stores[&node]
            .apply_reference_deltas(batch)
            .await
            .map_err(|error| error.to_string())
    }
}

struct TestPayloads {
    source: Store,
    stores: Arc<BTreeMap<NodeId, Store>>,
    profile: ErasureProfile,
    calls: Mutex<Vec<BlobRef>>,
    order: Arc<Mutex<Vec<String>>>,
}

impl TestPayloads {
    fn new(
        source: Store,
        stores: Arc<BTreeMap<NodeId, Store>>,
        order: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        Self {
            source,
            stores,
            profile: ErasureProfile::default(),
            calls: Mutex::new(Vec::new()),
            order,
        }
    }
}

#[tonic::async_trait]
impl PositiveReferencePreparation for TestPayloads {
    async fn prepare(&self, placement: &ReferencePlacement, blob: &BlobRef) -> Result<(), String> {
        self.order
            .lock()
            .expect("test order lock")
            .push("prepare".into());
        self.calls
            .lock()
            .expect("test payload lock")
            .push(blob.clone());
        let bytes = self
            .source
            .read_small_copy(blob)
            .map_err(|error| error.to_string())?;
        let PayloadPlacement::Small(selected) = select_payload_placement(
            placement.cluster_id(),
            blob,
            self.profile,
            placement.placement_nodes(),
        ) else {
            return Err("test payload preparation only supports small content".into());
        };
        for owner in selected.owners() {
            self.stores[owner]
                .seal_small_copy(blob, &bytes)
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

struct TestStores {
    _directories: Vec<TempDir>,
    stores: Arc<BTreeMap<NodeId, Store>>,
}

impl TestStores {
    async fn open(node_ids: &[u64]) -> Self {
        let directories = node_ids
            .iter()
            .map(|_| tempfile::tempdir().expect("test directory"))
            .collect::<Vec<_>>();
        let stores = open_paths(node_ids, &directories, None).await;
        Self {
            _directories: directories,
            stores: Arc::new(stores),
        }
    }
}

async fn open_paths(
    node_ids: &[u64],
    directories: &[TempDir],
    source_retention: Option<WatchRetention>,
) -> BTreeMap<NodeId, Store> {
    let mut stores = BTreeMap::new();
    for (node, directory) in node_ids.iter().zip(directories) {
        let mut options = StoreOptions::new(directory.path(), *node as u16);
        if *node == node_ids[0]
            && let Some(retention) = source_retention
        {
            options = options.with_watch_retention(retention);
        }
        let store = Store::open(options).await.expect("open test store");
        install_test_identity(&store);
        stores.insert(NodeId(*node), store);
    }
    stores
}

fn install_test_identity(store: &Store) {
    if store.resolve_bucket_ids("tenant", "bucket").is_ok() {
        return;
    }
    let tenant = StorageTenantId::parse("tenant").expect("test tenant name");
    for (record_version, typed_value) in [
        (
            101,
            LogicalRecordValue::TenantNameClaim {
                storage_tenant: tenant,
                tenant_id: 1,
            },
        ),
        (
            102,
            LogicalRecordValue::BucketNameClaim {
                tenant_id: 1,
                bucket: "bucket".into(),
                bucket_id: 1,
            },
        ),
    ] {
        let mutation = store
            .construct_logical_record_mutation(
                typed_value,
                LogicalRecordMutationContext {
                    record_version: VersionId(record_version),
                    active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                    serving_fence_term: 1,
                },
            )
            .expect("construct test identity");
        store
            .commit_logical_record_mutation(&mutation)
            .expect("commit test identity");
    }
}

async fn publish(source: &Store, path: &str, bytes: &[u8], command: &str) -> BlobRef {
    let blob = source.stage_blob(bytes).await.expect("stage test payload");
    source
        .coordinate_distributed_publish(
            PublishRequest {
                key: ObjectKey::new("tenant", "bucket", path).expect("test object key"),
                blob: blob.clone(),
                content_type: None,
                mode: PutMode::Put,
                command_id: Some(command.into()),
                durability: Durability::Local,
            },
            ObjectMutationContext {
                active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                serving_fence_term: 1,
            },
        )
        .await
        .expect("coordinate test publish");
    blob
}

struct ProofFixture {
    _stores: TestStores,
    source: Store,
    source_id: SourceId,
    change: LocalChange,
    proof: ReferenceProof,
}

impl ProofFixture {
    async fn open() -> Self {
        let stores = TestStores::open(&[1]).await;
        let source = stores.stores[&NodeId(1)].clone();
        publish(&source, "proof", b"proof payload", "proof-command").await;
        let source_id = source.local_watch_status().unwrap().source_id;
        let change = source
            .scan_local_changes(0, 8)
            .unwrap()
            .into_iter()
            .find(|change| matches!(change, LocalChange::ObjectHead(_)))
            .expect("source object-head change");
        let proof = source
            .read_reference_proof(source_id, change.offset())
            .unwrap()
            .expect("source proof");
        Self {
            _stores: stores,
            source,
            source_id,
            change,
            proof,
        }
    }

    fn authority(
        &self,
        placement: TestPlacement,
        peers: Arc<TestProofPeers>,
    ) -> QuorumReferenceCommitAuthority {
        QuorumReferenceCommitAuthority::new(self.source.clone(), Arc::new(placement), peers)
    }
}

fn sibling_proof(proof: &ReferenceProof) -> ReferenceProof {
    let mut encoded = serde_json::to_value(proof).expect("serialize proof");
    let first = encoded["mutation_fingerprint"][0]
        .as_u64()
        .expect("fingerprint byte");
    encoded["mutation_fingerprint"][0] = serde_json::Value::from((first as u8) ^ 0xff);
    serde_json::from_value(encoded).expect("deserialize sibling proof")
}

fn delivery(
    source: Store,
    placement: TestPlacement,
    commits: Arc<TestCommits>,
    destinations: Arc<TestDestinations>,
    payloads: Arc<TestPayloads>,
) -> ReferenceDelivery {
    ReferenceDelivery::new(
        source,
        Arc::new(placement),
        commits,
        destinations,
        payloads,
        ErasureProfile::default(),
    )
}

fn lifecycle(store: &Store, blob: &BlobRef) -> Option<BlobReferenceState> {
    store
        .blob_reference_state(blob)
        .expect("read test lifecycle")
}

#[test]
fn routing_uses_complete_small_owners_and_distinct_large_shard_owners() {
    let placement = placement(&[1, 2, 3, 4], 1);
    let small = BlobRef {
        hash: [1; 32],
        length: 12,
    };
    let large = BlobRef {
        hash: [2; 32],
        length: anvil_store::SMALL_BLOB_MAX_BYTES as u64 + 1,
    };
    let routed = route_effects(
        &placement,
        ErasureProfile::default(),
        &[
            ReferenceDelta {
                blob: small.clone(),
                change: 1,
            },
            ReferenceDelta {
                blob: large.clone(),
                change: 1,
            },
        ],
    );

    let effects = routed.values().flatten().collect::<Vec<_>>();
    assert_eq!(
        effects
            .iter()
            .filter(|delta| matches!(
                delta.artifact,
                DestinationReferenceArtifact::CompleteBlob(ref blob) if *blob == small
            ))
            .count(),
        2
    );
    let shards = effects
        .iter()
        .filter_map(|delta| match &delta.artifact {
            DestinationReferenceArtifact::Shard(identity) if identity.blob() == &large => {
                Some(identity.ordinal())
            }
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(shards, BTreeSet::from([0, 1, 2]));
    assert!(!effects.iter().any(|delta| matches!(
        delta.artifact,
        DestinationReferenceArtifact::CompleteBlob(ref blob) if *blob == large
    )));
}

#[tokio::test]
async fn every_active_destination_advances_and_positive_bytes_arrive_first() {
    let stores = TestStores::open(&[1, 2, 3]).await;
    let source = stores.stores[&NodeId(1)].clone();
    let blob = publish(&source, "one", b"first payload", "first").await;
    let view = placement(&[1, 2, 3], 1);
    let expected = route_effects(
        &view,
        ErasureProfile::default(),
        &[ReferenceDelta {
            blob: blob.clone(),
            change: 1,
        }],
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let destinations = Arc::new(TestDestinations::new(stores.stores.clone(), order.clone()));
    let payloads = Arc::new(TestPayloads::new(
        source.clone(),
        stores.stores.clone(),
        order.clone(),
    ));
    let expected_tail = source.local_watch_status().unwrap().tail;
    assert_eq!(expected_tail, 2);
    let progress = delivery(
        source.clone(),
        TestPlacement::new(view),
        Arc::new(TestCommits::default()),
        destinations.clone(),
        payloads,
    )
    .deliver_once()
    .await
    .unwrap();

    assert_eq!(
        (progress.safe_through, progress.tail),
        (expected_tail, expected_tail)
    );
    for node in [NodeId(1), NodeId(2), NodeId(3)] {
        assert_eq!(
            stores.stores[&node]
                .reference_delta_cursor(progress.source_id)
                .unwrap(),
            expected_tail
        );
    }
    let batches = destinations.batches.lock().unwrap();
    assert_eq!(batches.len(), 3);
    assert!(
        batches
            .iter()
            .any(|(node, batch)| { !expected.contains_key(node) && batch.deltas.is_empty() })
    );
    let order = order.lock().unwrap();
    assert_eq!(order.first().map(String::as_str), Some("prepare"));
    assert!(order[1..].iter().all(|entry| entry.starts_with("apply:")));
}

#[tokio::test]
async fn a_destination_gap_stops_before_any_delivery() {
    let directories = (0..3)
        .map(|_| tempfile::tempdir().unwrap())
        .collect::<Vec<_>>();
    let retention = WatchRetention::new(2, 1024 * 1024).unwrap();
    let stores = Arc::new(open_paths(&[1, 2, 3], &directories, Some(retention)).await);
    let source = stores[&NodeId(1)].clone();
    publish(&source, "one", b"one", "one").await;
    let first_tail = source.local_watch_status().unwrap().tail;
    assert_eq!(first_tail, 2);
    source
        .advance_source_journal_safe_through(first_tail)
        .await
        .unwrap();
    publish(&source, "two", b"two", "two").await;
    assert_eq!(
        source.local_watch_status().unwrap().retention_floor,
        first_tail
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let destinations = Arc::new(TestDestinations::new(stores.clone(), order.clone()));
    let payloads = Arc::new(TestPayloads::new(source.clone(), stores, order));
    let error = delivery(
        source,
        TestPlacement::new(placement(&[1, 2, 3], 1)),
        Arc::new(TestCommits::default()),
        destinations,
        payloads,
    )
    .deliver_once()
    .await
    .unwrap_err();
    assert!(
        matches!(
            &error,
            ReferenceDeliveryError::JournalGap {
                cursor,
                floor,
                ..
            } if cursor < floor && *floor == first_tail
        ),
        "unexpected delivery error: {error:?}"
    );
}

#[tokio::test]
async fn a_failed_destination_retries_without_double_applying_other_nodes() {
    let stores = TestStores::open(&[1, 2, 3]).await;
    let source = stores.stores[&NodeId(1)].clone();
    let blob = publish(&source, "one", b"retry payload", "retry").await;
    let order = Arc::new(Mutex::new(Vec::new()));
    let destinations = Arc::new(TestDestinations::new(stores.stores.clone(), order.clone()));
    destinations.fail_next(NodeId(2));
    let payloads = Arc::new(TestPayloads::new(
        source.clone(),
        stores.stores.clone(),
        order,
    ));
    let runner = delivery(
        source,
        TestPlacement::new(placement(&[1, 2, 3], 1)),
        Arc::new(TestCommits::default()),
        destinations,
        payloads,
    );
    assert!(matches!(
        runner.deliver_once().await,
        Err(ReferenceDeliveryError::Destination {
            node: NodeId(2),
            ..
        })
    ));
    runner.deliver_once().await.unwrap();

    let routed = route_effects(
        &placement(&[1, 2, 3], 1),
        ErasureProfile::default(),
        &[ReferenceDelta {
            blob: blob.clone(),
            change: 1,
        }],
    );
    for owner in routed.keys() {
        assert_eq!(
            lifecycle(&stores.stores[owner], &blob).unwrap().ref_count,
            1
        );
    }
}

#[tokio::test]
async fn restart_reconstructs_progress_from_durable_destination_cursors() {
    let directories = (0..3)
        .map(|_| tempfile::tempdir().unwrap())
        .collect::<Vec<_>>();
    let source_id;
    let expected_tail;
    {
        let stores = Arc::new(open_paths(&[1, 2, 3], &directories, None).await);
        let source = stores[&NodeId(1)].clone();
        publish(&source, "one", b"first", "first").await;
        publish(&source, "two", b"second", "second").await;
        let status = source.local_watch_status().unwrap();
        source_id = status.source_id;
        expected_tail = status.tail;
        assert_eq!(expected_tail, 4);
        let order = Arc::new(Mutex::new(Vec::new()));
        let destinations = Arc::new(TestDestinations::new(stores.clone(), order.clone()));
        let payloads = Arc::new(TestPayloads::new(source.clone(), stores, order));
        delivery(
            source,
            TestPlacement::new(placement(&[1, 2, 3], 1)),
            Arc::new(TestCommits::default()),
            destinations,
            payloads,
        )
        .with_page_size(1)
        .deliver_once()
        .await
        .unwrap();
    }

    let stores = Arc::new(open_paths(&[1, 2, 3], &directories, None).await);
    let source = stores[&NodeId(1)].clone();
    assert_eq!(source.local_watch_status().unwrap().source_id, source_id);
    let order = Arc::new(Mutex::new(Vec::new()));
    let destinations = Arc::new(TestDestinations::new(stores.clone(), order.clone()));
    let payloads = Arc::new(TestPayloads::new(source.clone(), stores.clone(), order));
    let progress = delivery(
        source,
        TestPlacement::new(placement(&[1, 2, 3], 1)),
        Arc::new(TestCommits::default()),
        destinations,
        payloads,
    )
    .deliver_once()
    .await
    .unwrap();
    assert_eq!(progress.safe_through, expected_tail);
    for store in stores.values() {
        assert_eq!(
            store.reference_delta_cursor(source_id).unwrap(),
            expected_tail
        );
    }
}

#[tokio::test]
async fn an_unacknowledged_minority_negative_has_no_reference_effect() {
    let stores = TestStores::open(&[1, 2, 3]).await;
    let source = stores.stores[&NodeId(1)].clone();
    let old = publish(&source, "same", b"committed", "first").await;
    let replacement = publish(&source, "same", b"minority", "second").await;
    let replacement_offset = source.local_watch_status().unwrap().tail;
    let commits = Arc::new(TestCommits::default());
    commits.set(
        replacement_offset,
        Ok(ReferenceCommitDisposition::DiscardedMinority),
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let destinations = Arc::new(TestDestinations::new(stores.stores.clone(), order.clone()));
    let payloads = Arc::new(TestPayloads::new(
        source.clone(),
        stores.stores.clone(),
        order,
    ));
    let calls = payloads.clone();
    delivery(
        source,
        TestPlacement::new(placement(&[1, 2, 3], 1)),
        commits,
        destinations,
        payloads,
    )
    .deliver_once()
    .await
    .unwrap();

    assert_eq!(*calls.calls.lock().unwrap(), [old.clone()]);
    let old_routes = route_effects(
        &placement(&[1, 2, 3], 1),
        ErasureProfile::default(),
        &[ReferenceDelta {
            blob: old.clone(),
            change: 1,
        }],
    );
    for owner in old_routes.keys() {
        assert_eq!(lifecycle(&stores.stores[owner], &old).unwrap().ref_count, 1);
    }
    let replacement_routes = route_effects(
        &placement(&[1, 2, 3], 1),
        ErasureProfile::default(),
        &[ReferenceDelta {
            blob: replacement.clone(),
            change: 1,
        }],
    );
    for owner in replacement_routes.keys() {
        let state = lifecycle(&stores.stores[owner], &replacement);
        assert!(state.is_none_or(|state| state.flags & anvil_store::AWAITING_PUBLISH != 0));
    }
}

#[tokio::test]
async fn missing_lineage_never_advances_past_the_unproven_event() {
    let stores = TestStores::open(&[1, 2, 3]).await;
    let source = stores.stores[&NodeId(1)].clone();
    publish(&source, "one", b"one", "one").await;
    publish(&source, "two", b"two", "two").await;
    let blocked_offset = source.local_watch_status().unwrap().tail;
    let commits = Arc::new(TestCommits::default());
    commits.set(
        blocked_offset,
        Err("retained descriptors do not carry ancestry".into()),
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let destinations = Arc::new(TestDestinations::new(stores.stores.clone(), order.clone()));
    let payloads = Arc::new(TestPayloads::new(
        source.clone(),
        stores.stores.clone(),
        order,
    ));
    let error = delivery(
        source.clone(),
        TestPlacement::new(placement(&[1, 2, 3], 1)),
        commits,
        destinations,
        payloads,
    )
    .deliver_once()
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        ReferenceDeliveryError::CommitProof { offset, .. } if offset == blocked_offset
    ));
    let source_id = source.local_watch_status().unwrap().source_id;
    for store in stores.stores.values() {
        assert_eq!(
            store.reference_delta_cursor(source_id).unwrap(),
            blocked_offset - 1
        );
    }
}

#[tokio::test]
async fn compaction_uses_only_every_current_active_destination() {
    let directories = (0..4)
        .map(|_| tempfile::tempdir().unwrap())
        .collect::<Vec<_>>();
    let retention = WatchRetention::new(8, 1024 * 1024).unwrap();
    let stores = Arc::new(open_paths(&[1, 2, 3, 4], &directories, Some(retention)).await);
    let source = stores[&NodeId(1)].clone();
    publish(&source, "one", b"one", "one").await;
    publish(&source, "two", b"two", "two").await;
    let current = TestPlacement::new(placement(&[1, 2, 3, 4], 1));
    let order = Arc::new(Mutex::new(Vec::new()));
    let destinations = Arc::new(TestDestinations::new(stores.clone(), order.clone()));
    destinations.fail_next(NodeId(4));
    let payloads = Arc::new(TestPayloads::new(source.clone(), stores, order));
    let runner = delivery(
        source.clone(),
        current.clone(),
        Arc::new(TestCommits::default()),
        destinations,
        payloads,
    );
    assert!(runner.deliver_once().await.is_err());

    current.replace(placement(&[1, 2, 3], 2));
    let caught_up = runner.deliver_once().await.unwrap();
    let before_third = source.local_watch_status().unwrap();
    assert_eq!(caught_up.safe_through, before_third.tail);
    publish(&source, "three", b"three", "three").await;
    let status = source.local_watch_status().unwrap();
    assert_eq!(status.tail, before_third.tail + 2);
    assert_eq!(status.retained_entries, 8);
    assert_eq!(status.retention_floor, status.tail - 8);
    assert!(status.retention_floor > 0);
}

#[tokio::test]
async fn one_of_one_exact_proof_is_committed() {
    let fixture = ProofFixture::open().await;
    let peers = Arc::new(TestProofPeers::default());
    let result = fixture
        .authority(TestPlacement::new(placement(&[1], 1)), peers.clone())
        .classify(fixture.source_id, &fixture.change)
        .await
        .unwrap();

    assert_eq!(result, ReferenceCommitDisposition::CommittedOrAncestor);
    assert!(peers.reads.lock().unwrap().is_empty());
}

#[tokio::test]
async fn one_node_proofless_object_event_is_already_applied_locally() {
    let stores = TestStores::open(&[1]).await;
    let source = stores.stores[&NodeId(1)].clone();
    source
        .put(PutRequest {
            key: ObjectKey::new("tenant", "bucket", "legacy-inline").unwrap(),
            bytes: b"legacy inline reference".to_vec(),
            content_type: None,
            mode: PutMode::PutIfAbsent,
            command_id: Some("legacy-inline".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    let source_id = source.local_watch_status().unwrap().source_id;
    let change = source
        .scan_local_changes(0, 16)
        .unwrap()
        .into_iter()
        .find(|change| {
            matches!(
                change,
                LocalChange::ObjectHead(change) if change.exact_path == "legacy-inline"
            )
        })
        .unwrap();
    assert!(
        source
            .read_reference_proof(source_id, change.offset())
            .unwrap()
            .is_none()
    );

    let result = QuorumReferenceCommitAuthority::new(
        source.clone(),
        Arc::new(TestPlacement::new(placement(&[1], 1))),
        Arc::new(TestProofPeers::default()),
    )
    .classify(source_id, &change)
    .await
    .unwrap();

    assert_eq!(result, ReferenceCommitDisposition::AlreadyAppliedLocally);

    let distributed_error = QuorumReferenceCommitAuthority::new(
        source,
        Arc::new(TestPlacement::new(placement(&[1, 2], 2))),
        Arc::new(TestProofPeers::default()),
    )
    .classify(source_id, &change)
    .await
    .unwrap_err();
    assert!(distributed_error.contains("reference proof is missing"));
}

#[tokio::test]
async fn two_of_two_requires_the_remote_exact_proof() {
    let fixture = ProofFixture::open().await;
    let peers = Arc::new(TestProofPeers::default());
    peers.respond(NodeId(2), Ok(Some(fixture.proof.clone())));
    let result = fixture
        .authority(TestPlacement::new(placement(&[1, 2], 7)), peers.clone())
        .classify(fixture.source_id, &fixture.change)
        .await
        .unwrap();

    assert_eq!(result, ReferenceCommitDisposition::CommittedOrAncestor);
    let reads = peers.reads.lock().unwrap();
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[0].0, NodeId(2));
    assert_eq!(reads[0].2.placement_fence.index, 7);
    let LocalChange::ObjectHead(change) = &fixture.change else {
        panic!("fixture must publish an object head");
    };
    assert_eq!(
        (
            reads[0].2.tenant_id,
            reads[0].2.bucket_id,
            reads[0].2.exact_path.as_str(),
        ),
        (
            change.tenant_id,
            change.bucket_id,
            change.exact_path.as_str()
        )
    );
}

#[tokio::test]
async fn two_of_three_exact_proofs_win_over_one_absence() {
    let fixture = ProofFixture::open().await;
    let peers = Arc::new(TestProofPeers::default());
    peers.respond(NodeId(2), Ok(Some(fixture.proof.clone())));
    peers.respond(NodeId(3), Ok(None));
    let result = fixture
        .authority(TestPlacement::new(placement(&[1, 2, 3], 1)), peers)
        .classify(fixture.source_id, &fixture.change)
        .await
        .unwrap();

    assert_eq!(result, ReferenceCommitDisposition::CommittedOrAncestor);
}

#[tokio::test]
async fn two_of_three_exact_absences_discard_a_minority_proof() {
    let fixture = ProofFixture::open().await;
    let peers = Arc::new(TestProofPeers::default());
    peers.respond(NodeId(2), Ok(None));
    peers.respond(NodeId(3), Ok(None));
    let result = fixture
        .authority(TestPlacement::new(placement(&[1, 2, 3], 1)), peers)
        .classify(fixture.source_id, &fixture.change)
        .await
        .unwrap();

    assert_eq!(result, ReferenceCommitDisposition::DiscardedMinority);
}

#[tokio::test]
async fn present_absent_split_without_a_quorum_fails_closed() {
    let fixture = ProofFixture::open().await;
    let peers = Arc::new(TestProofPeers::default());
    peers.respond(NodeId(2), Ok(None));
    let error = fixture
        .authority(TestPlacement::new(placement(&[1, 2], 1)), peers)
        .classify(fixture.source_id, &fixture.change)
        .await
        .unwrap_err();

    assert!(error.contains("unresolved between 1 exact and 1 absent"));
}

#[tokio::test]
async fn sibling_proof_at_the_same_source_position_fails_closed() {
    let fixture = ProofFixture::open().await;
    let peers = Arc::new(TestProofPeers::default());
    peers.respond(NodeId(2), Ok(Some(sibling_proof(&fixture.proof))));
    peers.respond(NodeId(3), Ok(Some(fixture.proof.clone())));
    let error = fixture
        .authority(TestPlacement::new(placement(&[1, 2, 3], 1)), peers)
        .classify(fixture.source_id, &fixture.change)
        .await
        .unwrap_err();

    assert!(error.contains("conflicting proof"));
}

#[tokio::test]
async fn unavailable_replicas_cannot_be_inferred_from_the_local_proof() {
    let fixture = ProofFixture::open().await;
    let peers = Arc::new(TestProofPeers::default());
    peers.respond(NodeId(2), Err("offline".into()));
    peers.respond(NodeId(3), Err("timed out".into()));
    let error = fixture
        .authority(TestPlacement::new(placement(&[1, 2, 3], 1)), peers)
        .classify(fixture.source_id, &fixture.change)
        .await
        .unwrap_err();

    assert!(error.contains("reached 1 of 2 required replicas"));
}

#[tokio::test]
async fn missing_source_proof_fails_before_peer_observations() {
    let fixture = ProofFixture::open().await;
    assert!(
        fixture
            .source
            .delete_reference_proof_if_matches(&fixture.proof)
            .await
            .unwrap()
    );
    let peers = Arc::new(TestProofPeers::default());
    peers.respond(NodeId(2), Ok(Some(fixture.proof.clone())));
    let error = fixture
        .authority(TestPlacement::new(placement(&[1, 2], 1)), peers.clone())
        .classify(fixture.source_id, &fixture.change)
        .await
        .unwrap_err();

    assert!(error.contains("source reference proof is missing"));
    assert!(peers.reads.lock().unwrap().is_empty());
}

#[tokio::test]
async fn placement_fence_change_invalidates_an_otherwise_exact_quorum() {
    let fixture = ProofFixture::open().await;
    let current = TestPlacement::new(placement(&[1, 2], 1));
    let peers = Arc::new(TestProofPeers::default());
    peers.respond(NodeId(2), Ok(Some(fixture.proof.clone())));
    peers.change_placement_on_read(current.clone(), placement(&[1, 2], 2));
    let error = fixture
        .authority(current, peers)
        .classify(fixture.source_id, &fixture.change)
        .await
        .unwrap_err();

    assert!(error.contains("placement changed"));
}
