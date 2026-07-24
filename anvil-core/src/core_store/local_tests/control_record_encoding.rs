use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use super::local_stream_control::control_record_proto::{
    decode_core_fence_record, decode_object_manifest_record, encode_core_fence_record,
    encode_object_manifest_record,
};
use super::*;

#[test]
fn core_fence_records_are_protobuf_not_json_or_cbor() {
    let record = CoreFenceRecord {
        schema: CORE_FENCE_SCHEMA.to_string(),
        fence_name: "fence-a".to_string(),
        owner_principal: "principal-a".to_string(),
        fence_token: 7,
        expires_at_ms: 123_456,
        updated_at: "2026-07-08T00:00:00+00:00".to_string(),
    };

    let bytes = encode_core_fence_record(&record).unwrap();

    assert_control_record_not_json_or_cbor("core fence", &bytes);
    assert_eq!(decode_core_fence_record(&bytes).unwrap(), record);
}

#[test]
fn object_manifest_records_are_protobuf_not_json_or_cbor() {
    let hash = "a".repeat(64);
    let mut object_ref =
        CoreObjectRef::test_unlocated(format!("sha256:{hash}"), 42, encode_manifest_ref(&hash));
    object_ref.encoding.encryption = "aes_gcm_siv".to_string();
    let logical_file_id = canonical_logical_file_id(
        WriterFamily::Stream,
        9,
        "stream/tenant-a/events/block-3",
        b"stream-block-3",
    );
    let manifest = CoreObjectManifest {
        schema: CORE_OBJECT_MANIFEST_SCHEMA.to_string(),
        mesh_id: "local-mesh".to_string(),
        region_id: "local".to_string(),
        object_hash: object_ref.hash.clone(),
        logical_size: object_ref.logical_size,
        logical_file_id: logical_file_id.clone(),
        logical_offset: 16_384,
        writer_family: WriterFamily::Stream.as_str().to_string(),
        encryption_algorithm: "aes_gcm_siv".to_string(),
        boundary_values: vec![CoreBoundaryValue {
            schema_generation: 1,
            name: "customer_tenant".to_string(),
            value_type: "uuid".to_string(),
            value: "tenant-a".to_string(),
            categories: vec!["query_prune".to_string()],
            source_kind: "user_metadata_json_pointer".to_string(),
            required: true,
            max_values_per_block: 1,
            placement_affinity: "none".to_string(),
            compaction_scope: "none".to_string(),
            shared_ranges_allowed: false,
            shared_record_kinds: Vec::new(),
        }],
        encoding: object_ref.encoding.clone(),
        placements: object_ref.placements.clone(),
        created_at: "2026-07-08T00:00:00+00:00".to_string(),
        mutation_id: "mutation-a".to_string(),
    };

    let bytes = encode_object_manifest_record(&manifest).unwrap();
    let decoded = decode_object_manifest_record(&bytes).unwrap();

    assert_control_record_not_json_or_cbor("object manifest", &bytes);
    assert_eq!(bytes, encode_object_manifest_record(&manifest).unwrap());
    assert_eq!(decoded.logical_file_id, logical_file_id);
    assert_eq!(decoded.logical_offset, 16_384);
    assert_eq!(decoded.writer_family, WriterFamily::Stream.as_str());
    assert_eq!(decoded.encryption_algorithm, "aes_gcm_siv");
    assert_eq!(decoded, manifest);
}

#[test]
fn core_object_ref_targets_are_protobuf_not_json_or_cbor() {
    let hash = "b".repeat(64);
    let object_ref =
        CoreObjectRef::test_unlocated(format!("sha256:{hash}"), 99, encode_manifest_ref(&hash));

    let target = encode_core_object_ref_target(&object_ref).unwrap();
    let encoded = target.strip_prefix("core-object-ref:").unwrap();
    let bytes = URL_SAFE_NO_PAD.decode(encoded).unwrap();

    assert_control_record_not_json_or_cbor("core object ref target", &bytes);
    assert_eq!(decode_core_object_ref_target(&target).unwrap(), object_ref);
}

#[test]
fn stream_event_hash_input_is_protobuf_not_json_or_cbor() {
    let record = StreamRecord {
        schema: "anvil.core.watch_event.v1".to_string(),
        stream_id: "stream-a".to_string(),
        partition_id: "partition-a".to_string(),
        sequence: 1,
        cursor: "stream-a:00000000000000000001".to_string(),
        previous_event_hash: ZERO_HASH.to_string(),
        event_hash: String::new(),
        record_kind: "test".to_string(),
        payload_hash: format!("sha256:{}", sha256_hex(b"payload")),
        payload: b"payload".to_vec(),
        content_type: None,
        user_metadata_json: "{}".to_string(),
        authenticated_principal: "tenant/1/principal/test-writer".to_string(),
        transaction_id: None,
        idempotency_key_hash: Some("sha256:key-a".to_string()),
        created_at: "2026-07-08T00:00:00+00:00".to_string(),
    };

    let bytes = event_hash_input(&record).unwrap();

    assert_control_record_not_json_or_cbor("stream event hash input", &bytes);
    assert_eq!(bytes, event_hash_input(&record).unwrap());
}
