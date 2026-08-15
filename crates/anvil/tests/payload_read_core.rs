#[path = "../src/payload_placement.rs"]
mod payload_placement;
#[path = "../src/payload_read.rs"]
mod payload_read;
#[path = "../src/placement.rs"]
mod placement;

use std::collections::BTreeMap;
use std::io::{self, Cursor, Read, Write};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};

use anvil_consensus::{ClusterId, NodeId};
use anvil_store::{
    BlobRef, ErasureCodec, ErasureProfile, PlacementLogId, SMALL_BLOB_MAX_BYTES, ShardIdentity,
};

use payload_placement::{PayloadPlacement, select_payload_placement};
use payload_read::{
    AnonymousPayloadReadSpools, DistributedPayloadReader, PAYLOAD_READ_FRAME_BYTES,
    PayloadReadError, PayloadReadPlacementView, PayloadReadSpool, PayloadReadSpoolFactory,
    PayloadReadTransport, PayloadReadTransportError,
};
use placement::PlacementNode;

const FENCE: PlacementLogId = PlacementLogId { term: 7, index: 91 };

#[derive(Clone)]
struct TestPlacement {
    cluster_id: ClusterId,
    nodes: Vec<PlacementNode>,
    addresses: BTreeMap<NodeId, String>,
}

impl TestPlacement {
    fn new(node_count: u64) -> Self {
        let nodes = (1..=node_count)
            .map(|id| {
                PlacementNode::new(
                    NodeId(id),
                    NonZeroU32::new(1_000_000).expect("fixed weight is non-zero"),
                )
            })
            .collect::<Vec<_>>();
        let addresses = nodes
            .iter()
            .map(|node| (node.node_id(), format!("node-{}:7443", node.node_id().0)))
            .collect();
        Self {
            cluster_id: ClusterId([3; 16]),
            nodes,
            addresses,
        }
    }
}

impl PayloadReadPlacementView for TestPlacement {
    fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    fn fence(&self) -> PlacementLogId {
        FENCE
    }

    fn placement_nodes(&self) -> &[PlacementNode] {
        &self.nodes
    }

    fn address(&self, node: NodeId) -> Option<&str> {
        self.addresses.get(&node).map(String::as_str)
    }
}

#[derive(Clone, Debug)]
enum Artifact {
    Bytes(Vec<u8>),
    Missing,
    Unavailable,
    OversizedFrame(Vec<u8>),
}

#[derive(Default)]
struct FakeState {
    small: BTreeMap<NodeId, Artifact>,
    shards: BTreeMap<(NodeId, u16), Artifact>,
    small_puts: Vec<NodeId>,
    shard_puts: Vec<(NodeId, u16)>,
    gets: Vec<NodeId>,
}

#[derive(Clone, Default)]
struct FakeTransport {
    state: Arc<Mutex<FakeState>>,
}

impl FakeTransport {
    fn set_small(&self, node: NodeId, artifact: Artifact) {
        self.state.lock().unwrap().small.insert(node, artifact);
    }

    fn set_shard(&self, node: NodeId, ordinal: u16, artifact: Artifact) {
        self.state
            .lock()
            .unwrap()
            .shards
            .insert((node, ordinal), artifact);
    }

    fn shard(&self, node: NodeId, ordinal: u16) -> Option<Vec<u8>> {
        match self.state.lock().unwrap().shards.get(&(node, ordinal)) {
            Some(Artifact::Bytes(bytes)) => Some(bytes.clone()),
            _ => None,
        }
    }
}

#[tonic::async_trait]
impl PayloadReadTransport for FakeTransport {
    async fn get_small(
        &self,
        fence: PlacementLogId,
        target: NodeId,
        _address: &str,
        _reference: &BlobRef,
        destination: &mut (dyn Write + Send),
    ) -> Result<(), PayloadReadTransportError> {
        assert_eq!(fence, FENCE);
        let artifact = {
            let mut state = self.state.lock().unwrap();
            state.gets.push(target);
            state
                .small
                .get(&target)
                .cloned()
                .unwrap_or(Artifact::Missing)
        };
        stream_artifact(artifact, destination)
    }

    async fn put_small(
        &self,
        fence: PlacementLogId,
        target: NodeId,
        _address: &str,
        reference: &BlobRef,
        bytes: &[u8],
    ) -> Result<(), PayloadReadTransportError> {
        assert_eq!(fence, FENCE);
        assert_eq!(bytes.len() as u64, reference.length);
        assert_eq!(blake3::hash(bytes).as_bytes(), &reference.hash);
        let mut state = self.state.lock().unwrap();
        state.small.insert(target, Artifact::Bytes(bytes.to_vec()));
        state.small_puts.push(target);
        Ok(())
    }

    async fn get_shard(
        &self,
        fence: PlacementLogId,
        target: NodeId,
        _address: &str,
        identity: &ShardIdentity,
        destination: &mut (dyn Write + Send),
    ) -> Result<(), PayloadReadTransportError> {
        assert_eq!(fence, FENCE);
        let artifact = {
            let mut state = self.state.lock().unwrap();
            state.gets.push(target);
            state
                .shards
                .get(&(target, identity.ordinal()))
                .cloned()
                .unwrap_or(Artifact::Missing)
        };
        stream_artifact(artifact, destination)
    }

    async fn put_shard(
        &self,
        fence: PlacementLogId,
        target: NodeId,
        _address: &str,
        identity: &ShardIdentity,
        mut source: Box<dyn Read + Send>,
    ) -> Result<(), PayloadReadTransportError> {
        assert_eq!(fence, FENCE);
        let mut bytes = Vec::new();
        source
            .read_to_end(&mut bytes)
            .map_err(|error| PayloadReadTransportError::Destination(error.to_string()))?;
        let mut state = self.state.lock().unwrap();
        state
            .shards
            .insert((target, identity.ordinal()), Artifact::Bytes(bytes));
        state.shard_puts.push((target, identity.ordinal()));
        Ok(())
    }
}

fn stream_artifact(
    artifact: Artifact,
    destination: &mut (dyn Write + Send),
) -> Result<(), PayloadReadTransportError> {
    match artifact {
        Artifact::Bytes(bytes) => {
            for frame in bytes.chunks(7 * 1024) {
                destination
                    .write_all(frame)
                    .map_err(|error| PayloadReadTransportError::Destination(error.to_string()))?;
            }
            Ok(())
        }
        Artifact::Missing => Err(PayloadReadTransportError::NotFound),
        Artifact::Unavailable => Err(PayloadReadTransportError::Unavailable("test outage".into())),
        Artifact::OversizedFrame(bytes) => destination
            .write_all(&bytes)
            .map_err(|error| PayloadReadTransportError::Destination(error.to_string())),
    }
}

#[derive(Clone, Default)]
struct MemorySpools;

impl PayloadReadSpoolFactory for MemorySpools {
    fn create(&self) -> io::Result<Box<dyn PayloadReadSpool>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }
}

#[derive(Clone, Default)]
struct CapturedOutput {
    state: Arc<Mutex<CapturedOutputState>>,
}

#[derive(Default)]
struct CapturedOutputState {
    bytes: Vec<u8>,
    maximum_write: usize,
}

impl CapturedOutput {
    fn bytes(&self) -> Vec<u8> {
        self.state.lock().unwrap().bytes.clone()
    }

    fn maximum_write(&self) -> usize {
        self.state.lock().unwrap().maximum_write
    }
}

impl Write for CapturedOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut state = self.state.lock().unwrap();
        state.maximum_write = state.maximum_write.max(bytes.len());
        state.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn reference(bytes: &[u8]) -> BlobRef {
    BlobRef {
        hash: *blake3::hash(bytes).as_bytes(),
        length: bytes.len() as u64,
    }
}

fn encode(profile: ErasureProfile, bytes: &[u8]) -> (BlobRef, Vec<Vec<u8>>) {
    let reference = reference(bytes);
    let codec = ErasureCodec::new(profile).unwrap();
    let mut shards = vec![Vec::new(); usize::from(profile.total_shards())];
    codec
        .encode(Cursor::new(bytes), &reference, &mut shards)
        .unwrap();
    (reference, shards)
}

fn small_owners(
    placement: &TestPlacement,
    profile: ErasureProfile,
    reference: &BlobRef,
) -> Vec<NodeId> {
    match select_payload_placement(
        placement.cluster_id(),
        reference,
        profile,
        placement.placement_nodes(),
    ) {
        PayloadPlacement::Small(owners) => owners.owners().to_vec(),
        PayloadPlacement::LargeComplete(_) => panic!("expected small placement"),
        PayloadPlacement::Large(_) => panic!("expected small placement"),
    }
}

fn shard_owners(
    placement: &TestPlacement,
    profile: ErasureProfile,
    reference: &BlobRef,
) -> Vec<(NodeId, u16)> {
    match select_payload_placement(
        placement.cluster_id(),
        reference,
        profile,
        placement.placement_nodes(),
    ) {
        PayloadPlacement::Large(owners) => owners
            .shards()
            .iter()
            .map(|owner| (owner.owner(), owner.ordinal()))
            .collect(),
        PayloadPlacement::LargeComplete(_) => panic!("expected erasure-coded placement"),
        PayloadPlacement::Small(_) => panic!("expected large placement"),
    }
}

fn reader(profile: ErasureProfile, transport: FakeTransport) -> DistributedPayloadReader {
    DistributedPayloadReader::new(profile, Arc::new(transport), Arc::new(MemorySpools)).unwrap()
}

fn large_bytes(length: usize) -> Vec<u8> {
    assert!(length > SMALL_BLOB_MAX_BYTES);
    (0..length)
        .map(|index| ((index * 37 + index / 251) % 256) as u8)
        .collect()
}

#[tokio::test]
async fn small_read_uses_a_verified_owner_and_repairs_a_missing_copy() {
    let profile = ErasureProfile::default();
    let placement = TestPlacement::new(3);
    let bytes = b"one bounded small value".to_vec();
    let reference = reference(&bytes);
    let owners = small_owners(&placement, profile, &reference);
    let transport = FakeTransport::default();
    transport.set_small(owners[0], Artifact::Bytes(bytes.clone()));
    transport.set_small(owners[1], Artifact::Missing);
    let output = CapturedOutput::default();

    let report = reader(profile, transport.clone())
        .read(&placement, &reference, output.clone())
        .await
        .unwrap();

    assert_eq!(output.bytes(), bytes);
    assert_eq!(report.sources.healthy, 1);
    assert_eq!(report.sources.missing, 2);
    assert_eq!(report.repairs_attempted, 1);
    assert_eq!(report.repairs_completed, 1);
    assert_eq!(transport.state.lock().unwrap().small_puts, [owners[1]]);
}

#[tokio::test]
async fn small_hash_mismatch_never_reaches_the_output() {
    let profile = ErasureProfile::default();
    let placement = TestPlacement::new(3);
    let expected = b"expected small value".to_vec();
    let reference = reference(&expected);
    let owners = small_owners(&placement, profile, &reference);
    let transport = FakeTransport::default();
    for owner in owners {
        transport.set_small(owner, Artifact::Bytes(b"different small value".to_vec()));
    }
    let output = CapturedOutput::default();

    let result = reader(profile, transport)
        .read(&placement, &reference, output.clone())
        .await;

    assert!(matches!(result, Err(PayloadReadError::Unavailable { .. })));
    assert!(output.bytes().is_empty());
}

#[tokio::test]
async fn healthy_large_read_uses_local_and_remote_owners_with_bounded_output() {
    let profile = ErasureProfile::default();
    let placement = TestPlacement::new(3);
    let bytes = large_bytes(310_123);
    let (reference, shards) = encode(profile, &bytes);
    let owners = shard_owners(&placement, profile, &reference);
    let transport = FakeTransport::default();
    for (node, ordinal) in &owners {
        transport.set_shard(
            *node,
            *ordinal,
            Artifact::Bytes(shards[*ordinal as usize].clone()),
        );
    }
    let output = CapturedOutput::default();

    let report = reader(profile, transport.clone())
        .read(&placement, &reference, output.clone())
        .await
        .unwrap();

    assert_eq!(output.bytes(), bytes);
    assert!(output.maximum_write() <= PAYLOAD_READ_FRAME_BYTES);
    assert_eq!(report.sources.healthy, 3);
    assert_eq!(report.repairs_attempted, 0);
    let gets = &transport.state.lock().unwrap().gets;
    assert!(gets.contains(&owners[0].0));
    assert!(gets.iter().any(|node| *node != owners[0].0));
}

#[tokio::test]
async fn missing_large_shard_is_reconstructed_and_repaired() {
    let profile = ErasureProfile::default();
    let placement = TestPlacement::new(3);
    let bytes = large_bytes(180_007);
    let (reference, shards) = encode(profile, &bytes);
    let owners = shard_owners(&placement, profile, &reference);
    let transport = FakeTransport::default();
    for (node, ordinal) in &owners[..2] {
        transport.set_shard(
            *node,
            *ordinal,
            Artifact::Bytes(shards[*ordinal as usize].clone()),
        );
    }
    transport.set_shard(owners[2].0, owners[2].1, Artifact::Missing);
    let output = CapturedOutput::default();

    let report = reader(profile, transport.clone())
        .read(&placement, &reference, output.clone())
        .await
        .unwrap();

    assert_eq!(output.bytes(), bytes);
    assert_eq!(report.sources.missing, 1);
    assert_eq!(report.repairs_completed, 1);
    let repaired = transport.shard(owners[2].0, owners[2].1).unwrap();
    ErasureCodec::new(profile)
        .unwrap()
        .validate_shard(&reference, owners[2].1, Cursor::new(repaired))
        .unwrap();
}

#[tokio::test]
async fn corrupt_large_shard_is_replaced_after_valid_reconstruction() {
    let profile = ErasureProfile::default();
    let placement = TestPlacement::new(3);
    let bytes = large_bytes(170_021);
    let (reference, mut shards) = encode(profile, &bytes);
    let owners = shard_owners(&placement, profile, &reference);
    shards[2][64] ^= 0x80;
    let transport = FakeTransport::default();
    for (node, ordinal) in &owners {
        transport.set_shard(
            *node,
            *ordinal,
            Artifact::Bytes(shards[*ordinal as usize].clone()),
        );
    }
    let output = CapturedOutput::default();

    let report = reader(profile, transport.clone())
        .read(&placement, &reference, output.clone())
        .await
        .unwrap();

    assert_eq!(output.bytes(), bytes);
    assert_eq!(report.sources.corrupt, 1);
    assert_eq!(report.repairs_completed, 1);
    let repaired = transport.shard(owners[2].0, owners[2].1).unwrap();
    ErasureCodec::new(profile)
        .unwrap()
        .validate_shard(&reference, owners[2].1, Cursor::new(repaired))
        .unwrap();
}

#[tokio::test]
async fn sparse_current_owners_need_only_k_and_repair_only_known_absence() {
    let profile = ErasureProfile::new(3, 2, 8 * 1024).unwrap();
    let placement = TestPlacement::new(7);
    let bytes = large_bytes(420_009);
    let (reference, shards) = encode(profile, &bytes);
    let owners = shard_owners(&placement, profile, &reference);
    let transport = FakeTransport::default();
    for (node, ordinal) in &owners[..3] {
        transport.set_shard(
            *node,
            *ordinal,
            Artifact::Bytes(shards[*ordinal as usize].clone()),
        );
    }
    transport.set_shard(owners[3].0, owners[3].1, Artifact::Missing);
    transport.set_shard(owners[4].0, owners[4].1, Artifact::Unavailable);
    let output = CapturedOutput::default();

    let report = reader(profile, transport.clone())
        .read(&placement, &reference, output.clone())
        .await
        .unwrap();

    assert_eq!(output.bytes(), bytes);
    assert_eq!(report.sources.healthy, 3);
    assert_eq!(report.sources.missing, 1);
    assert_eq!(report.sources.unavailable, 1);
    assert_eq!(report.repairs_attempted, 1);
    assert_eq!(report.repairs_completed, 1);
    assert_eq!(transport.state.lock().unwrap().shard_puts.len(), 1);
}

#[tokio::test]
async fn fewer_than_k_valid_shards_fails_before_output() {
    let profile = ErasureProfile::default();
    let placement = TestPlacement::new(3);
    let bytes = large_bytes(100_001);
    let (reference, shards) = encode(profile, &bytes);
    let owners = shard_owners(&placement, profile, &reference);
    let transport = FakeTransport::default();
    transport.set_shard(
        owners[0].0,
        owners[0].1,
        Artifact::Bytes(shards[owners[0].1 as usize].clone()),
    );
    transport.set_shard(owners[1].0, owners[1].1, Artifact::Missing);
    transport.set_shard(owners[2].0, owners[2].1, Artifact::Unavailable);
    let output = CapturedOutput::default();

    let result = reader(profile, transport)
        .read(&placement, &reference, output.clone())
        .await;

    assert!(matches!(result, Err(PayloadReadError::Unavailable { .. })));
    assert!(output.bytes().is_empty());
}

#[tokio::test]
async fn oversized_owner_frame_is_corrupt_and_repaired_without_large_memory_growth() {
    let profile = ErasureProfile::default();
    let placement = TestPlacement::new(3);
    let bytes = large_bytes(140_003);
    let (reference, shards) = encode(profile, &bytes);
    let owners = shard_owners(&placement, profile, &reference);
    let transport = FakeTransport::default();
    for (node, ordinal) in &owners[..2] {
        transport.set_shard(
            *node,
            *ordinal,
            Artifact::Bytes(shards[*ordinal as usize].clone()),
        );
    }
    transport.set_shard(
        owners[2].0,
        owners[2].1,
        Artifact::OversizedFrame(vec![0; PAYLOAD_READ_FRAME_BYTES + 1]),
    );
    let output = CapturedOutput::default();

    let report = reader(profile, transport)
        .read(&placement, &reference, output.clone())
        .await
        .unwrap();

    assert_eq!(output.bytes(), bytes);
    assert_eq!(report.sources.corrupt, 1);
    assert_eq!(report.repairs_completed, 1);
}

#[test]
#[cfg(target_os = "linux")]
fn production_spools_have_no_directory_entry() {
    let temporary = tempfile::tempdir().unwrap();
    let directory = temporary.path().join("missing/payload-read");
    assert!(!directory.exists());
    let factory = AnonymousPayloadReadSpools::new(&directory).unwrap();
    assert!(directory.is_dir());
    let before = std::fs::read_dir(&directory).unwrap().count();
    let mut spool = factory.create().unwrap();
    spool
        .write_all(&vec![7; 2 * PAYLOAD_READ_FRAME_BYTES])
        .unwrap();
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), before);
    drop(spool);
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), before);
}
