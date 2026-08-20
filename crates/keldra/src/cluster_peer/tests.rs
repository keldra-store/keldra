use tonic::metadata::{MetadataMap, MetadataValue};

use super::admission::{validate_context, validate_context_with_timeout_limit};
use super::routing::test_bearer;
use super::{
    CLUSTER_PEER_SCHEMA_VERSION, MAX_INDEX_SOURCE_SNAPSHOT_TIME, decode_json, encode_json, wire,
};

#[test]
fn routed_bearer_is_metadata_only_and_exact() {
    let mut metadata = MetadataMap::new();
    metadata.insert(
        "authorization",
        MetadataValue::from_static("Bearer signed.jwt"),
    );
    assert_eq!(test_bearer(&metadata).unwrap().as_ref(), "signed.jwt");

    metadata.append("authorization", MetadataValue::from_static("Bearer second"));
    assert_eq!(
        test_bearer(&metadata).unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
}

#[test]
fn admission_separates_replica_calls_from_one_hop_routes() {
    let mut context = wire::PeerContext {
        schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        cluster_id: vec![7; 16],
        source_node_id: 3,
        placement_term: 4,
        placement_index: 9,
        hop_count: 0,
        remaining_deadline_millis: 30_000,
    };
    assert!(validate_context(&context, 0).is_ok());
    assert!(validate_context(&context, 1).is_err());

    context.hop_count = 1;
    assert!(validate_context(&context, 1).is_ok());
    context.hop_count = 2;
    assert_eq!(
        validate_context(&context, 1).unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn admission_rejects_unversioned_identity_and_unbounded_deadline() {
    let mut context = wire::PeerContext {
        schema_version: 0,
        cluster_id: vec![7; 16],
        source_node_id: 3,
        placement_term: 4,
        placement_index: 9,
        hop_count: 0,
        remaining_deadline_millis: 30_000,
    };
    assert_eq!(
        validate_context(&context, 0).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    context.schema_version = CLUSTER_PEER_SCHEMA_VERSION;
    context.remaining_deadline_millis = 30_001;
    assert_eq!(
        validate_context(&context, 0).unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn snapshot_scan_has_a_bounded_internal_deadline_beyond_public_requests() {
    let context = wire::PeerContext {
        schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        cluster_id: vec![7; 16],
        source_node_id: 3,
        placement_term: 4,
        placement_index: 9,
        hop_count: 0,
        remaining_deadline_millis: 60_000,
    };
    assert!(validate_context(&context, 0).is_err());
    assert!(
        validate_context_with_timeout_limit(&context, 0, MAX_INDEX_SOURCE_SNAPSHOT_TIME,).is_ok()
    );
}

#[test]
fn typed_codec_round_trips_a_logical_identity_and_rejects_empty_input() {
    let id = keldra_store::LogicalRecordId::BucketOptions {
        tenant_id: 17,
        bucket_id: 23,
    };
    let encoded = encode_json(&id).unwrap();
    assert_eq!(
        decode_json::<keldra_store::LogicalRecordId>(&encoded).unwrap(),
        id
    );
    assert_eq!(
        decode_json::<keldra_store::LogicalRecordId>(&[])
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}
