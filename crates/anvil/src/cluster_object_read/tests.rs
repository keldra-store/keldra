use std::collections::BTreeMap;
use std::io::{Cursor, Read, Write};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use anvil_consensus::{ClusterId, NodeId};
use anvil_store::{
    BlobRef, ErasureCodec, Head, ObjectPathSnapshot, ShardIdentity, Version, VersionId,
};
use tonic::Code;

use super::*;
use crate::payload_placement::{PayloadPlacement, select_payload_placement};
use crate::placement::PlacementNode;

#[derive(Clone)]
struct TestTopology {
    cluster_id: ClusterId,
    nodes: Vec<PlacementNode>,
    addresses: BTreeMap<NodeId, String>,
}

impl TestTopology {
    fn three_nodes() -> Self {
        let nodes = (1..=3)
            .map(|node| PlacementNode::new(NodeId(node), NonZeroU32::new(1_000_000).unwrap()))
            .collect::<Vec<_>>();
        let addresses = nodes
            .iter()
            .map(|node| (node.node_id(), format!("node-{}:50052", node.node_id().0)))
            .collect();
        Self {
            cluster_id: ClusterId([7; 16]),
            nodes,
            addresses,
        }
    }

    fn placement(&self, index: u64) -> TestPlacement {
        TestPlacement {
            topology: self.clone(),
            fence: PlacementLogId { term: 3, index },
        }
    }
}

struct TestPlacement {
    topology: TestTopology,
    fence: PlacementLogId,
}

impl PayloadReadPlacementView for TestPlacement {
    fn cluster_id(&self) -> ClusterId {
        self.topology.cluster_id
    }

    fn fence(&self) -> PlacementLogId {
        self.fence
    }

    fn placement_nodes(&self) -> &[PlacementNode] {
        &self.topology.nodes
    }

    fn address(&self, node: NodeId) -> Option<&str> {
        self.topology.addresses.get(&node).map(String::as_str)
    }
}

struct FakeMetadata {
    topology: TestTopology,
    fence_index: Arc<AtomicU64>,
    snapshot: Option<ObjectPathSnapshot>,
    current_snapshot: Option<CurrentObjectSnapshot>,
    full_snapshot_reads: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl ObjectReadMetadata for FakeMetadata {
    async fn reconciled_snapshot(
        &self,
        _key: &ObjectKey,
    ) -> Result<Option<ObjectPathSnapshot>, Status> {
        self.full_snapshot_reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.snapshot.clone())
    }

    async fn reconciled_current_snapshot_stable(
        &self,
        _key: &ObjectKey,
        _tenant_id: u64,
        _bucket_id: u64,
    ) -> Result<Option<CurrentObjectSnapshot>, Status> {
        Ok(self.current_snapshot.clone())
    }

    fn current_placement(&self) -> Result<Arc<dyn PayloadReadPlacementView>, Status> {
        Ok(Arc::new(
            self.topology
                .placement(self.fence_index.load(Ordering::SeqCst)),
        ))
    }

    fn require_current_fence(&self, expected: PlacementLogId) -> Result<(), Status> {
        if expected.index != self.fence_index.load(Ordering::SeqCst) {
            return Err(Status::unavailable("test placement changed"));
        }
        Ok(())
    }
}

type SmallKey = (NodeId, [u8; 32], u64);
type ShardKey = (NodeId, [u8; 32], u64, u16);

#[derive(Default)]
struct FakePayloadTransport {
    small: Mutex<BTreeMap<SmallKey, Vec<u8>>>,
    shards: Mutex<BTreeMap<ShardKey, Vec<u8>>>,
    change_fence_after_fetch: AtomicBool,
    fence_changed: AtomicBool,
    fence_index: Option<Arc<AtomicU64>>,
}

impl FakePayloadTransport {
    fn with_fence_change(fence_index: Arc<AtomicU64>) -> Self {
        Self {
            change_fence_after_fetch: AtomicBool::new(true),
            fence_index: Some(fence_index),
            ..Self::default()
        }
    }

    fn insert_small(&self, node: NodeId, reference: &BlobRef, bytes: &[u8]) {
        self.small
            .lock()
            .unwrap()
            .insert((node, reference.hash, reference.length), bytes.to_vec());
    }

    fn insert_shard(&self, node: NodeId, identity: &ShardIdentity, bytes: Vec<u8>) {
        self.shards.lock().unwrap().insert(
            (
                node,
                identity.blob().hash,
                identity.blob().length,
                identity.ordinal(),
            ),
            bytes,
        );
    }

    fn after_fetch(&self) {
        if self.change_fence_after_fetch.load(Ordering::SeqCst)
            && !self.fence_changed.swap(true, Ordering::SeqCst)
        {
            self.fence_index
                .as_ref()
                .unwrap()
                .fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[tonic::async_trait]
impl PayloadReadTransport for FakePayloadTransport {
    async fn get_small(
        &self,
        _fence: PlacementLogId,
        target: NodeId,
        _address: &str,
        reference: &BlobRef,
        destination: &mut (dyn Write + Send),
    ) -> Result<(), crate::payload_read::PayloadReadTransportError> {
        let bytes = self
            .small
            .lock()
            .unwrap()
            .get(&(target, reference.hash, reference.length))
            .cloned()
            .ok_or(crate::payload_read::PayloadReadTransportError::NotFound)?;
        destination.write_all(&bytes).map_err(|error| {
            crate::payload_read::PayloadReadTransportError::Destination(error.to_string())
        })?;
        self.after_fetch();
        Ok(())
    }

    async fn put_small(
        &self,
        _fence: PlacementLogId,
        target: NodeId,
        _address: &str,
        reference: &BlobRef,
        bytes: &[u8],
    ) -> Result<(), crate::payload_read::PayloadReadTransportError> {
        self.insert_small(target, reference, bytes);
        Ok(())
    }

    async fn get_shard(
        &self,
        _fence: PlacementLogId,
        target: NodeId,
        _address: &str,
        identity: &ShardIdentity,
        destination: &mut (dyn Write + Send),
    ) -> Result<(), crate::payload_read::PayloadReadTransportError> {
        let bytes = self
            .shards
            .lock()
            .unwrap()
            .get(&(
                target,
                identity.blob().hash,
                identity.blob().length,
                identity.ordinal(),
            ))
            .cloned()
            .ok_or(crate::payload_read::PayloadReadTransportError::NotFound)?;
        destination.write_all(&bytes).map_err(|error| {
            crate::payload_read::PayloadReadTransportError::Destination(error.to_string())
        })?;
        self.after_fetch();
        Ok(())
    }

    async fn put_shard(
        &self,
        _fence: PlacementLogId,
        target: NodeId,
        _address: &str,
        identity: &ShardIdentity,
        mut source: Box<dyn Read + Send>,
    ) -> Result<(), crate::payload_read::PayloadReadTransportError> {
        let mut bytes = Vec::new();
        source.read_to_end(&mut bytes).map_err(|error| {
            crate::payload_read::PayloadReadTransportError::Destination(error.to_string())
        })?;
        self.insert_shard(target, identity, bytes);
        Ok(())
    }
}

struct MemorySpools;

impl PayloadReadSpoolFactory for MemorySpools {
    fn create(&self) -> io::Result<Box<dyn PayloadReadSpool>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }
}

fn key() -> ObjectKey {
    ObjectKey::new("tenant", "bucket", "docs/a").unwrap()
}

fn definition_key() -> ObjectKey {
    ObjectKey::new(
        "system",
        "definitions",
        "_anvil/indexes/v2/definitions/example",
    )
    .unwrap()
}

fn blob(bytes: &[u8]) -> BlobRef {
    BlobRef {
        hash: *blake3::hash(bytes).as_bytes(),
        length: bytes.len() as u64,
    }
}

fn live_version(id: u64, reference: BlobRef) -> Version {
    Version {
        id: VersionId(id),
        blob: Some(reference),
        content_type: Some("application/octet-stream".into()),
        deleted: false,
        committed_at_unix_millis: id,
    }
}

fn tombstone(id: u64) -> Version {
    Version {
        id: VersionId(id),
        blob: None,
        content_type: None,
        deleted: true,
        committed_at_unix_millis: id,
    }
}

fn snapshot(head: &Version, versions: Vec<Version>) -> ObjectPathSnapshot {
    snapshot_at(key().path(), head, versions)
}

fn snapshot_at(exact_path: &str, head: &Version, versions: Vec<Version>) -> ObjectPathSnapshot {
    ObjectPathSnapshot {
        tenant_id: 11,
        bucket_id: 12,
        exact_path: exact_path.to_owned(),
        head: Head {
            version: head.id,
            deleted: head.deleted,
            mutation_stamp: None,
        },
        versions,
        definition_locator: None,
    }
}

fn reader(
    snapshot: ObjectPathSnapshot,
    topology: TestTopology,
    fence_index: Arc<AtomicU64>,
    transport: Arc<FakePayloadTransport>,
) -> ClusterObjectReader {
    reader_with_full_snapshot_counter(snapshot, topology, fence_index, transport).0
}

fn reader_with_full_snapshot_counter(
    snapshot: ObjectPathSnapshot,
    topology: TestTopology,
    fence_index: Arc<AtomicU64>,
    transport: Arc<FakePayloadTransport>,
) -> (ClusterObjectReader, Arc<AtomicU64>) {
    let version = snapshot
        .versions
        .iter()
        .find(|version| version.id == snapshot.head.version)
        .unwrap()
        .clone();
    let current_snapshot = CurrentObjectSnapshot {
        tenant_id: snapshot.tenant_id,
        bucket_id: snapshot.bucket_id,
        exact_path: snapshot.exact_path.clone(),
        head: snapshot.head.clone(),
        version,
    };
    let full_snapshot_reads = Arc::new(AtomicU64::new(0));
    ClusterObjectReader::with_components(
        Arc::new(FakeMetadata {
            topology,
            fence_index,
            snapshot: Some(snapshot),
            current_snapshot: Some(current_snapshot),
            full_snapshot_reads: full_snapshot_reads.clone(),
        }),
        ErasureProfile::default(),
        transport,
        Arc::new(MemorySpools),
    )
    .map(|reader| (reader, full_snapshot_reads))
    .unwrap()
}

#[tokio::test]
async fn current_only_definition_open_never_requests_retained_history() {
    let topology = TestTopology::three_nodes();
    let fence_index = Arc::new(AtomicU64::new(8));
    let bytes = b"bounded definition";
    let reference = blob(bytes);
    let current = live_version(10_001, reference.clone());
    let mut versions = (1..=10_000)
        .map(|version| tombstone(version))
        .collect::<Vec<_>>();
    versions.push(current.clone());
    let transport = Arc::new(FakePayloadTransport::default());
    let placement = topology.placement(8);
    let PayloadPlacement::Small(owners) = select_payload_placement(
        placement.cluster_id(),
        &reference,
        ErasureProfile::default(),
        placement.placement_nodes(),
    ) else {
        panic!("test payload must use complete-copy placement");
    };
    for owner in owners.owners() {
        transport.insert_small(*owner, &reference, bytes);
    }
    let (reader, full_snapshot_reads) = reader_with_full_snapshot_counter(
        snapshot_at(definition_key().path(), &current, versions),
        topology,
        fence_index,
        transport,
    );

    let head = reader
        .current_head_snapshot_stable(&definition_key(), 11, 12)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.version, current);
    let mut opened = reader
        .open_current_stable(&definition_key(), 11, 12)
        .await
        .unwrap()
        .unwrap();
    let mut recovered = Vec::new();
    opened
        .payload
        .as_mut()
        .unwrap()
        .read_to_end(&mut recovered)
        .unwrap();
    assert_eq!(recovered, bytes);
    assert_eq!(opened.version, current);
    assert_eq!(full_snapshot_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn current_tombstone_and_exact_live_version_are_selected() {
    let topology = TestTopology::three_nodes();
    let fence_index = Arc::new(AtomicU64::new(9));
    let bytes = b"retained bytes";
    let reference = blob(bytes);
    let version = live_version(1, reference.clone());
    let deleted = tombstone(2);
    let transport = Arc::new(FakePayloadTransport::default());
    let placement = topology.placement(9);
    let PayloadPlacement::Small(owners) = select_payload_placement(
        placement.cluster_id(),
        &reference,
        ErasureProfile::default(),
        placement.placement_nodes(),
    ) else {
        panic!("test payload must use complete-copy placement");
    };
    for owner in owners.owners() {
        transport.insert_small(*owner, &reference, bytes);
    }
    let reader = reader(
        snapshot(&deleted, vec![version, deleted.clone()]),
        topology,
        fence_index,
        transport,
    );

    assert_eq!(reader.head(&key()).await.unwrap(), Some(deleted.clone()));
    let current = reader.open(&key(), None).await.unwrap().unwrap();
    assert_eq!(current.version, deleted);
    assert!(current.payload.is_none());

    let mut exact = reader
        .open(&key(), Some(VersionId(1)))
        .await
        .unwrap()
        .unwrap();
    let mut recovered = Vec::new();
    exact
        .payload
        .as_mut()
        .unwrap()
        .read_to_end(&mut recovered)
        .unwrap();
    assert_eq!(recovered, bytes);
    assert!(
        reader
            .open(&key(), Some(VersionId(99)))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn placement_change_after_payload_fetch_fails_closed() {
    let topology = TestTopology::three_nodes();
    let fence_index = Arc::new(AtomicU64::new(20));
    let bytes = b"bytes that must not escape";
    let reference = blob(bytes);
    let version = live_version(3, reference.clone());
    let transport = Arc::new(FakePayloadTransport::with_fence_change(fence_index.clone()));
    let placement = topology.placement(20);
    let PayloadPlacement::Small(owners) = select_payload_placement(
        placement.cluster_id(),
        &reference,
        ErasureProfile::default(),
        placement.placement_nodes(),
    ) else {
        panic!("test payload must use complete-copy placement");
    };
    for owner in owners.owners() {
        transport.insert_small(*owner, &reference, bytes);
    }
    let reader = reader(
        snapshot(&version, vec![version.clone()]),
        topology,
        fence_index,
        transport,
    );

    let error = reader.open(&key(), None).await.err().unwrap();
    assert_eq!(error.code(), Code::Unavailable);
}

#[tokio::test]
async fn large_payload_recovers_from_two_of_three_shards() {
    let topology = TestTopology::three_nodes();
    let fence_index = Arc::new(AtomicU64::new(30));
    let bytes = (0..96 * 1024)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let reference = blob(&bytes);
    let version = live_version(4, reference.clone());
    let profile = ErasureProfile::default();
    let codec = ErasureCodec::new(profile).unwrap();
    let mut encoded = vec![Vec::new(); usize::from(profile.total_shards())];
    codec
        .encode(Cursor::new(&bytes), &reference, &mut encoded)
        .unwrap();

    let placement = topology.placement(30);
    let PayloadPlacement::Large(shards) = select_payload_placement(
        placement.cluster_id(),
        &reference,
        profile,
        placement.placement_nodes(),
    ) else {
        panic!("test payload must use shard placement");
    };
    let transport = Arc::new(FakePayloadTransport::default());
    // Omit ordinal zero: reconstruction must use one data shard and parity.
    for shard in shards.shards().iter().filter(|shard| shard.ordinal() != 0) {
        let identity = ShardIdentity::new(reference.clone(), shard.ordinal());
        transport.insert_shard(
            shard.owner(),
            &identity,
            encoded[usize::from(shard.ordinal())].clone(),
        );
    }
    let reader = reader(
        snapshot(&version, vec![version.clone()]),
        topology,
        fence_index,
        transport,
    );

    let mut opened = reader.open(&key(), None).await.unwrap().unwrap();
    let mut recovered = Vec::new();
    opened
        .payload
        .as_mut()
        .unwrap()
        .read_to_end(&mut recovered)
        .unwrap();
    assert_eq!(recovered, bytes);
}
