use std::time::Duration;

use anvil_index::v4::{
    ArtifactDescriptor, ComponentKind, RoutingEntry, RoutingNode, SegmentIdentity, artifact_path,
    encode_component, manifest_path,
};
use anvil_store::{BlobRef, VersionId};

use super::*;

fn artifact(index_id: u64, seed: u8, kind: ComponentKind) -> ArtifactDescriptor {
    ArtifactDescriptor::new(
        index_id,
        artifact_path(index_id, [seed; 32]),
        u64::from(seed) + 1,
        [seed; 32],
        4096,
        0,
        120,
        0,
        kind,
        1,
        [seed.wrapping_add(1); 32],
    )
    .unwrap()
}

fn reference(generation: u64, published_at: u64, seed: u8) -> ManifestReference {
    ManifestReference {
        generation,
        definition_version: 1,
        schema_fingerprint: [9; 32],
        path: manifest_path(9, [seed; 32]),
        blob: BlobRef {
            hash: [seed; 32],
            length: 120,
        },
        object_version: VersionId(generation + 10),
        published_at_unix_millis: published_at,
    }
}

fn pointer() -> IndexCurrentPointer {
    IndexCurrentPointer::new(
        9,
        reference(3, 300, 3),
        vec![reference(2, 200, 2), reference(1, 100, 1)],
    )
    .unwrap()
}

#[test]
fn exact_byte_contributions_trim_only_an_oldest_suffix() {
    let pointer = pointer();
    let mut contributions = [0_u64; RETENTION_GENERATION_SLOTS];
    contributions[0] = 100;
    contributions[1] = 20;
    contributions[2] = 50;
    assert_eq!(
        select_byte_retained(&pointer, &contributions, 119)
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        select_byte_retained(&pointer, &contributions, 120)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        select_byte_retained(&pointer, &contributions, 170)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn idle_age_revisit_uses_the_earliest_retained_expiry() {
    let config = IndexRuntimeConfig::default();
    let age = config.max_generation_age_hours() * 60 * 60 * 1_000;
    assert_eq!(next_age_due(&pointer(), config), Some(100 + age + 1));
    assert_eq!(minimum_due(Some(9), Some(4)), Some(4));
    assert_eq!(minimum_due(None, Some(4)), Some(4));
}

#[test]
fn routing_scratch_round_trip_preserves_the_exact_graph_edge() {
    let identity = SegmentIdentity::new(9, 3, [4; 32], 5).unwrap();
    let routing = RoutingArtifact {
        rank: 2,
        artifact: artifact(9, 7, ComponentKind::ROUTING_NODE),
        role: ComponentKind::POSTINGS,
        expected_identity: ExpectedComponentIdentity::exact(identity),
        expected_height: Some(3),
    };
    let decoded = decode_routing(&encode_routing(&routing)).unwrap();
    assert_eq!(decoded.rank, 2);
    assert_eq!(decoded.artifact, routing.artifact);
    assert_eq!(decoded.role, routing.role);
    assert_eq!(decoded.expected_identity, routing.expected_identity);
    assert_eq!(decoded.expected_height, Some(3));
}

#[test]
fn routing_envelope_identity_is_read_from_the_portable_v4_header() {
    let identity = SegmentIdentity::new(9, 3, [4; 32], 5).unwrap();
    let node = RoutingNode::new(
        9,
        1,
        vec![RoutingEntry {
            minimum_key: b"a".to_vec(),
            maximum_key: b"z".to_vec(),
            element_count: 1,
            child: artifact(9, 6, ComponentKind::POSTINGS),
        }],
    )
    .unwrap();
    let payload = node.encode_payload().unwrap();
    let component = encode_component(
        identity,
        ComponentKind::ROUTING_NODE,
        1,
        0,
        payload.len() as u64,
        payload,
    )
    .unwrap();
    assert_eq!(component_identity(component.bytes()).unwrap(), identity);
}

#[test]
fn routing_reader_returns_only_its_descriptor_range_from_a_shared_pack() {
    let prefix = vec![1_u8; 7];
    let component = vec![2_u8; anvil_index::v4::COMPONENT_HEADER_BYTES];
    let suffix = vec![3_u8; 11];
    let mut object = prefix.clone();
    object.extend_from_slice(&component);
    object.extend_from_slice(&suffix);
    let hash = *blake3::hash(&object).as_bytes();
    let descriptor = ArtifactDescriptor::new(
        9,
        artifact_path(9, hash),
        1,
        hash,
        object.len() as u64,
        prefix.len() as u64,
        component.len() as u64,
        0,
        ComponentKind::ROUTING_NODE,
        1,
        [4; 32],
    )
    .unwrap();

    let mut input = std::io::Cursor::new(object);
    assert_eq!(
        read_routing_component(&mut input, &descriptor).unwrap(),
        component
    );
    assert_eq!(
        input.position(),
        descriptor.offset + descriptor.encoded_length
    );
}

#[test]
fn routing_reader_rejects_a_truncated_descriptor_range() {
    let hash = [5; 32];
    let descriptor = ArtifactDescriptor::new(
        9,
        artifact_path(9, hash),
        1,
        hash,
        anvil_index::v4::COMPONENT_HEADER_BYTES as u64,
        0,
        anvil_index::v4::COMPONENT_HEADER_BYTES as u64,
        0,
        ComponentKind::ROUTING_NODE,
        1,
        [6; 32],
    )
    .unwrap();
    let mut input = std::io::Cursor::new(vec![0_u8; 119]);

    let error = read_routing_component(&mut input, &descriptor).unwrap_err();
    assert_eq!(error.code(), tonic::Code::DataLoss);
}

#[test]
fn retention_budget_and_schedule_reject_unbounded_ticks() {
    assert!(
        IndexRetentionBudget::new(1, MAX_RETENTION_RECORD_BYTES - 1, Duration::from_secs(1))
            .is_err()
    );
    assert!(IndexRetentionSchedule::new(Duration::ZERO, Duration::from_secs(1)).is_err());
    assert!(IndexRetentionSchedule::new(Duration::from_secs(1), Duration::ZERO).is_err());
}
