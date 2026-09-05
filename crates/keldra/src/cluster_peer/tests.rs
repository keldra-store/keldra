use prost::Message;
use tonic::metadata::{MetadataMap, MetadataValue};

use super::admission::{validate_context, validate_context_with_timeout_limit};
use super::routing::test_bearer;
use super::{
    CLUSTER_PEER_SCHEMA_VERSION, MAX_CLUSTER_BULK_OPERATION_TIME, MAX_INDEX_SOURCE_SNAPSHOT_TIME,
    decode_json, encode_json, wire,
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
fn bulk_route_accepts_the_remaining_bulk_budget_beyond_ordinary_peer_time() {
    let context = wire::PeerContext {
        schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        cluster_id: vec![7; 16],
        source_node_id: 3,
        placement_term: 4,
        placement_index: 9,
        hop_count: 1,
        remaining_deadline_millis: 600_000,
    };
    assert!(validate_context(&context, 1).is_err());
    assert!(
        validate_context_with_timeout_limit(&context, 1, MAX_CLUSTER_BULK_OPERATION_TIME,).is_ok()
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

#[test]
fn clone_route_wire_preserves_peer_fence_and_public_request() {
    let request = wire::RouteCloneObjectRequest {
        peer: Some(wire::PeerContext {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            cluster_id: vec![7; 16],
            source_node_id: 3,
            placement_term: 4,
            placement_index: 9,
            hop_count: 1,
            remaining_deadline_millis: 500,
        }),
        request: Some(keldra_api::v1::CloneObjectRequest {
            source: Some(keldra_api::v1::ObjectAddress {
                tenant: "tenant".into(),
                bucket: "bucket".into(),
                path: "source".into(),
            }),
            source_version: 17,
            destination: Some(keldra_api::v1::ObjectAddress {
                tenant: "tenant".into(),
                bucket: "bucket".into(),
                path: "destination".into(),
            }),
            command_id: "clone".into(),
            durability: keldra_api::v1::Durability::Local as i32,
            operation: Some(keldra_api::v1::clone_object_request::Operation::Put(
                keldra_api::v1::PutOperation {},
            )),
        }),
    };

    let decoded = wire::RouteCloneObjectRequest::decode(request.encode_to_vec().as_slice())
        .expect("clone route must decode");
    assert_eq!(decoded, request);
    assert_eq!(decoded.peer.unwrap().placement_index, 9);
}

#[test]
fn link_routes_preserve_peer_fence_and_public_requests() {
    let peer = wire::PeerContext {
        schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        cluster_id: vec![7; 16],
        source_node_id: 3,
        placement_term: 4,
        placement_index: 9,
        hop_count: 1,
        remaining_deadline_millis: 500,
    };
    let link = keldra_api::v1::ObjectAddress {
        tenant: "tenant".into(),
        bucket: "bucket".into(),
        path: "link".into(),
    };
    let link_request = wire::RouteLinkObjectRequest {
        peer: Some(peer.clone()),
        request: Some(keldra_api::v1::LinkObjectRequest {
            link: Some(link.clone()),
            target: Some(keldra_api::v1::ObjectAddress {
                path: "target".into(),
                ..link.clone()
            }),
            command_id: "link".into(),
            durability: keldra_api::v1::Durability::Local as i32,
        }),
    };
    assert_eq!(
        wire::RouteLinkObjectRequest::decode(link_request.encode_to_vec().as_slice()).unwrap(),
        link_request
    );

    let unlink_request = wire::RouteUnlinkObjectRequest {
        peer: Some(peer),
        request: Some(keldra_api::v1::UnlinkObjectRequest {
            link: Some(link),
            command_id: "unlink".into(),
            durability: keldra_api::v1::Durability::Local as i32,
        }),
    };
    assert_eq!(
        wire::RouteUnlinkObjectRequest::decode(unlink_request.encode_to_vec().as_slice()).unwrap(),
        unlink_request
    );
}

#[test]
fn built_in_replay_batch_wire_preserves_indices_and_per_item_failures() {
    let request = wire::RouteBuiltInReplayBatchRequest {
        peer: Some(wire::PeerContext {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            cluster_id: vec![7; 16],
            source_node_id: 3,
            placement_term: 4,
            placement_index: 9,
            hop_count: 0,
            remaining_deadline_millis: 500,
        }),
        executor_nomination_log_index: 12,
        lookups: vec![wire::BuiltInReplayLookup {
            original_index: 41,
            authority_kind: 3,
            contract_version: 1,
            invocation_id: vec![1; 32],
            input_fingerprint: vec![2; 32],
        }],
    };
    assert_eq!(
        wire::RouteBuiltInReplayBatchRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
        request
    );
    let response = wire::RouteBuiltInReplayBatchResponse {
        schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        outcomes: vec![wire::BuiltInReplayOutcome {
            original_index: 41,
            result_json: Vec::new(),
            error_code: tonic::Code::AlreadyExists as i32,
            error_message: "IDEMPOTENCY_INPUT_MISMATCH".into(),
        }],
    };
    assert_eq!(
        wire::RouteBuiltInReplayBatchResponse::decode(response.encode_to_vec().as_slice()).unwrap(),
        response
    );
}
