use super::*;
use tempfile::tempdir;

fn sample_header(sequence: u64) -> Vec<u8> {
    sample_header_at(sequence, 0)
}

fn sample_header_at(sequence: u64, byte_offset: u64) -> Vec<u8> {
    encode_control_mutation_header(ControlMutationHeaderInput {
        schema: "anvil.mesh.control_mutation.v1",
        mesh_id: "mesh_01",
        stream_family: "bucket_locator",
        partition: "0a7f",
        sequence: ControlStreamSequence::new(sequence).unwrap(),
        record_key: "tenant_acme/releases",
        operation: "upsert",
        expected_generation: sequence.checked_add(17),
        new_generation: sequence.saturating_add(18),
        writer_node_id: "node_01J0",
        writer_fence: 44,
        idempotency_key: Some("req-123"),
        record_digest: &ControlRecordDigest::blake3(b"record"),
        created_at: "2026-07-02T00:00:00Z",
        byte_offset,
    })
}

fn sample_payload() -> Vec<u8> {
    br#"{"tenant_id":"tenant_acme","bucket":"releases"}"#.to_vec()
}

fn bucket_locator_operator_json(home_region: &str) -> Vec<u8> {
    let descriptor = crate::mesh_directory::BucketLocatorDescriptor::active(
        crate::mesh_directory::MeshId::new("mesh_01").unwrap(),
        crate::mesh_directory::TenantId::new("tenant_acme").unwrap(),
        crate::mesh_directory::BucketName::canonicalize("releases").unwrap(),
        crate::mesh_directory::BucketId::new("bucket_01HY").unwrap(),
        crate::mesh_directory::RegionName::new(home_region).unwrap(),
        crate::mesh_directory::CellId::new("cell_a").unwrap(),
        "regional-primary",
        "objects/tenant_acme/releases/",
        "2026-07-02T00:00:00Z",
    )
    .unwrap();
    serde_json::to_vec(&descriptor).unwrap()
}

fn bucket_locator_payload_proto(home_region: &str) -> Vec<u8> {
    let operator_json = bucket_locator_operator_json(home_region);
    crate::mesh_directory::encode_control_payload_from_operator_json(
        crate::mesh_directory::RoutingRecordFamily::BucketLocator,
        &operator_json,
    )
    .unwrap()
}

fn sample_header_for_payload(sequence: u64, payload: &[u8]) -> Vec<u8> {
    encode_control_mutation_header(ControlMutationHeaderInput {
        schema: "anvil.mesh.control_mutation.v1",
        mesh_id: "mesh_01",
        stream_family: "bucket_locator",
        partition: "0a7f",
        sequence: ControlStreamSequence::new(sequence).unwrap(),
        record_key: "tenant_acme/releases",
        operation: "upsert",
        expected_generation: None,
        new_generation: 1,
        writer_node_id: "node_01J0",
        writer_fence: 44,
        idempotency_key: Some("req-123"),
        record_digest: &ControlRecordDigest::blake3(payload),
        created_at: "2026-07-02T00:00:00Z",
        byte_offset: 0,
    })
}

fn sample_header_for_record(
    cursor: &ControlStreamAppendCursor,
    record_key: &str,
    operation: &str,
    generation: u64,
    payload: &[u8],
) -> Vec<u8> {
    encode_control_mutation_header(ControlMutationHeaderInput {
        schema: "anvil.mesh.control_mutation.v1",
        mesh_id: "mesh_01",
        stream_family: "bucket_locator",
        partition: "0a7f",
        sequence: cursor.sequence,
        record_key,
        operation,
        expected_generation: generation.checked_sub(1),
        new_generation: generation,
        writer_node_id: "node_01J0",
        writer_fence: 44,
        idempotency_key: None,
        record_digest: &ControlRecordDigest::blake3(payload),
        created_at: "2026-07-02T00:00:00Z",
        byte_offset: cursor.byte_offset,
    })
}

#[test]
fn crc32_matches_standard_check_value() {
    assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
}

#[test]
fn frame_round_trips_with_big_endian_header_and_metadata() {
    let frame = ControlStreamFrame::new(sample_header(1844), sample_payload());
    let encoded = frame.encode().unwrap();

    assert_eq!(&encoded[0..8], CONTROL_STREAM_MAGIC);
    assert_eq!(
        u16::from_be_bytes(encoded[8..10].try_into().unwrap()),
        CONTROL_STREAM_VERSION
    );
    assert_eq!(
        u32::from_be_bytes(encoded[10..14].try_into().unwrap()),
        frame.header_proto.len() as u32
    );
    assert_eq!(
        u64::from_be_bytes(encoded[14..22].try_into().unwrap()),
        frame.payload_proto.len() as u64
    );
    assert_eq!(
        u32::from_be_bytes(encoded[22..26].try_into().unwrap()),
        crc32(&frame.header_proto)
    );
    assert_eq!(
        u32::from_be_bytes(encoded[26..30].try_into().unwrap()),
        crc32(&frame.payload_proto)
    );

    let (decoded, used) = ControlStreamFrame::decode(&encoded).unwrap();
    assert_eq!(used, encoded.len());
    assert_eq!(decoded, frame);
    let metadata = decoded.metadata().unwrap();
    assert_eq!(metadata.sequence.get(), 1844);
    assert!(metadata.record_digest.as_str().starts_with("blake3:"));
}

#[test]
fn frame_decode_validates_header_and_payload_crc32() {
    let frame = ControlStreamFrame::new(sample_header(1), sample_payload());
    let encoded = frame.encode().unwrap();

    let mut bad_header = encoded.clone();
    bad_header[CONTROL_STREAM_FIXED_HEADER_LEN] ^= 1;
    assert!(matches!(
        ControlStreamFrame::decode(&bad_header).unwrap_err(),
        ControlStreamFrameError::HeaderCrc32Mismatch { .. }
    ));

    let mut bad_payload = encoded;
    let payload_offset = CONTROL_STREAM_FIXED_HEADER_LEN + frame.header_proto.len();
    bad_payload[payload_offset] ^= 1;
    assert!(matches!(
        ControlStreamFrame::decode(&bad_payload).unwrap_err(),
        ControlStreamFrameError::PayloadCrc32Mismatch { .. }
    ));
}

#[test]
fn log_decode_ignores_partial_final_frame() {
    let first = ControlStreamFrame::new(sample_header(1), sample_payload())
        .encode()
        .unwrap();
    let second = ControlStreamFrame::new(sample_header(2), sample_payload())
        .encode()
        .unwrap();
    let mut log = Vec::new();
    log.extend_from_slice(&first);
    log.extend_from_slice(&second[..CONTROL_STREAM_FIXED_HEADER_LEN + 5]);

    let decoded = decode_control_stream_log(&log).unwrap();
    assert_eq!(decoded.records.len(), 1);
    assert_eq!(decoded.records[0].metadata.sequence.get(), 1);
    assert_eq!(decoded.complete_len, first.len() as u64);
    assert_eq!(
        decoded.partial_final_frame,
        Some(PartialFinalFrame {
            offset: first.len() as u64,
            expected_len: second.len(),
            actual_len: CONTROL_STREAM_FIXED_HEADER_LEN + 5,
        })
    );
}

#[test]
fn metadata_requires_sequence_and_digest() {
    let mut missing_digest = ControlFrameHeaderProto::decode(&sample_header(1)[..]).unwrap();
    missing_digest.record_digest.clear();
    let missing_digest = missing_digest.encode_to_vec();
    assert_eq!(
        metadata_from_header_proto(&missing_digest).unwrap_err(),
        ControlStreamFrameError::MissingHeaderField {
            field: "record_digest"
        }
    );

    let mut zero_sequence = ControlFrameHeaderProto::decode(&sample_header(1)[..]).unwrap();
    zero_sequence.sequence = 0;
    let zero_sequence = zero_sequence.encode_to_vec();
    assert_eq!(
        metadata_from_header_proto(&zero_sequence).unwrap_err(),
        ControlStreamFrameError::InvalidSequence
    );

    let mut bad_digest = ControlFrameHeaderProto::decode(&sample_header(1)[..]).unwrap();
    bad_digest.record_digest = "sha256:abc".to_string();
    let bad_digest = bad_digest.encode_to_vec();
    assert_eq!(
        metadata_from_header_proto(&bad_digest).unwrap_err(),
        ControlStreamFrameError::InvalidDigest
    );
}

#[tokio::test]
async fn append_and_read_control_stream_pages() {
    let dir = tempdir().unwrap();
    let storage = Storage::new_at(dir.path()).await.unwrap();
    let first = ControlStreamFrame::new(sample_header(1), sample_payload());

    let first_append =
        append_control_stream_frame(&storage, "bucket_locator", "0a7f", &first, None)
            .await
            .unwrap();
    let first_len = first.encode().unwrap().len();
    let second = ControlStreamFrame::new(sample_header_at(2, first_len as u64), sample_payload());
    let second_append =
        append_control_stream_frame(&storage, "bucket_locator", "0a7f", &second, None)
            .await
            .unwrap();

    assert_eq!(first_append.offset, 0);
    assert_eq!(first_append.encoded_len, first_len);
    assert_eq!(second_append.offset, first_len as u64);
    assert_eq!(second_append.position.sequence.get(), 2);

    let log = read_control_stream_page(&storage, "bucket_locator", "0a7f", 0, 8)
        .await
        .unwrap();
    assert_eq!(log.records.len(), 2);
    assert!(!log.has_more);
    assert_eq!(log.records[1].metadata.sequence.get(), 2);
}

#[tokio::test]
async fn control_stream_pages_are_bounded_and_latest_lookup_is_key_scoped() {
    let dir = tempdir().unwrap();
    let storage = Storage::new_at(dir.path()).await.unwrap();
    let projected_payload = bucket_locator_payload_proto("eu-west-1");

    for (record_key, generation, payload) in [
        ("tenant_acme/releases", 1, projected_payload.as_slice()),
        ("unrelated/one", 1, &b"unrelated-one"[..]),
        ("unrelated/two", 1, &b"unrelated-two"[..]),
    ] {
        let cursor = control_stream_append_cursor(&storage, "bucket_locator", "0a7f")
            .await
            .unwrap();
        let frame = ControlStreamFrame::new(
            sample_header_for_record(&cursor, record_key, "upsert", generation, payload),
            payload.to_vec(),
        );
        append_control_stream_frame(&storage, "bucket_locator", "0a7f", &frame, None)
            .await
            .unwrap();
    }

    let first = read_control_stream_page(&storage, "bucket_locator", "0a7f", 0, 2)
        .await
        .unwrap();
    assert_eq!(first.records.len(), 2);
    assert!(first.has_more);
    let second =
        read_control_stream_page(&storage, "bucket_locator", "0a7f", first.next_sequence, 2)
            .await
            .unwrap();
    assert_eq!(second.records.len(), 1);
    assert!(!second.has_more);

    let current_first =
        list_current_control_stream_records_page(&storage, "bucket_locator", "0a7f", None, 2)
            .await
            .unwrap();
    assert_eq!(current_first.records.len(), 2);
    let current_second = list_current_control_stream_records_page(
        &storage,
        "bucket_locator",
        "0a7f",
        current_first.next_stream_id.as_deref(),
        2,
    )
    .await
    .unwrap();
    assert_eq!(current_second.records.len(), 1);
    assert!(current_second.next_stream_id.is_none());

    let latest = latest_projected_record_from_control_stream(
        &storage,
        "bucket_locator",
        "0a7f",
        "tenant_acme/releases",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(latest.generation, 1);
    assert_eq!(
        latest.payload_json,
        bucket_locator_operator_json("eu-west-1")
    );

    let cursor = control_stream_append_cursor(&storage, "bucket_locator", "0a7f")
        .await
        .unwrap();
    let delete = ControlStreamFrame::new(
        sample_header_for_record(
            &cursor,
            "tenant_acme/releases",
            "delete",
            2,
            &projected_payload,
        ),
        projected_payload,
    );
    append_control_stream_frame(&storage, "bucket_locator", "0a7f", &delete, None)
        .await
        .unwrap();
    let latest = latest_projected_record_from_control_stream(
        &storage,
        "bucket_locator",
        "0a7f",
        "tenant_acme/releases",
    )
    .await
    .unwrap()
    .unwrap();
    assert!(latest.deleted);
    assert_eq!(latest.generation, 2);
    let mut current =
        list_current_control_stream_records_page(&storage, "bucket_locator", "0a7f", None, 8)
            .await
            .unwrap()
            .records;
    current.sort_by(|left, right| left.record_key.cmp(&right.record_key));
    assert_eq!(current.len(), 3);
    let deleted = current
        .iter()
        .find(|record| record.record_key == "tenant_acme/releases")
        .unwrap();
    assert!(deleted.deleted);
    assert_eq!(deleted.generation, 2);
    assert!(
        read_control_stream_page(&storage, "bucket_locator", "0a7f", 0, 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("page size")
    );
    assert!(
        list_current_control_stream_records_page(&storage, "bucket_locator", "0a7f", None, 0,)
            .await
            .unwrap_err()
            .to_string()
            .contains("page size")
    );
}

#[tokio::test]
async fn append_rejects_log_with_partial_final_frame() {
    let partial = ControlStreamFrame::new(sample_header(1), sample_payload())
        .encode()
        .unwrap();
    let err = decode_control_stream_log(&partial[..partial.len() - 1]).unwrap();
    assert!(
        err.partial_final_frame.is_some(),
        "partial frame must remain a byte-format validation concern"
    );
}

#[tokio::test]
async fn control_checkpoint_round_trips_and_rejects_path_body_scope_mismatch() {
    let dir = tempdir().unwrap();
    let storage = Storage::new_at(dir.path()).await.unwrap();
    let digest = ControlRecordDigest::blake3(b"checkpointed-record");
    let checkpoint = ControlCheckpointRecord::new(
        "mesh-a",
        "eu-west-1",
        "bucket_locator",
        "0a7f",
        ControlStreamSequence::new(7).unwrap(),
        digest.clone(),
        "2026-07-02T00:00:00Z",
    );

    write_control_checkpoint(&storage, &checkpoint)
        .await
        .unwrap();
    assert_eq!(
        read_control_checkpoint(&storage, "eu-west-1", "bucket_locator", "0a7f")
            .await
            .unwrap(),
        Some(checkpoint)
    );

    let mismatched_body = ControlCheckpointRecord::new(
        "mesh-a",
        "us-east-1",
        "tenant_name",
        "ffff",
        ControlStreamSequence::new(1).unwrap(),
        digest,
        "2026-07-02T00:00:00Z",
    );
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let row_key = control_checkpoint_row_key("eu-west-2", "bucket_locator", "0a7f").unwrap();
    let current = store
        .read_coremeta_row(CF_MESH, TABLE_MESH_PARTITION_ROW, &row_key)
        .unwrap();
    let payload = encode_control_checkpoint_proto(&mismatched_body).unwrap();
    let partition_id =
        control_checkpoint_partition_id("eu-west-2", "bucket_locator", "0a7f").unwrap();
    let root_publications = control_checkpoint_root_publications(
        partition_id.clone(),
        control_checkpoint_root_anchor_key(
            &mismatched_body.region,
            &mismatched_body.stream_family,
            &mismatched_body.partition,
        ),
    );
    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: "mismatched-checkpoint-test".to_string(),
            scope_partition: partition_id.clone(),
            committed_by_principal: "mesh-control-checkpoint-test".to_string(),
            root_publications,
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: CF_MESH.to_string(),
                table_id: TABLE_MESH_PARTITION_ROW,
                tuple_key: row_key.clone(),
                expected_payload_hash: current
                    .as_ref()
                    .map(|payload| core_meta_payload_digest(TABLE_MESH_PARTITION_ROW, payload)),
                require_absent: current.is_none(),
                require_present: current.is_some(),
            }],
            operations: vec![CoreMutationOperation::CoreMetaPut {
                partition_id,
                cf: CF_MESH.to_string(),
                table_id: TABLE_MESH_PARTITION_ROW,
                tuple_key: row_key,
                payload,
            }],
        })
        .await
        .unwrap();

    let err = read_control_checkpoint(&storage, "eu-west-2", "bucket_locator", "0a7f")
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("control checkpoint path does not match checkpoint body"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn control_checkpoint_rejects_unsafe_path_scopes() {
    let dir = tempdir().unwrap();
    let storage = Storage::new_at(dir.path()).await.unwrap();
    let checkpoint = ControlCheckpointRecord::new(
        "mesh-a",
        "../escape",
        "bucket_locator",
        "0a7f",
        ControlStreamSequence::new(1).unwrap(),
        ControlRecordDigest::blake3(b"checkpointed-record"),
        "2026-07-02T00:00:00Z",
    );

    let err = write_control_checkpoint(&storage, &checkpoint)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("control checkpoint region is not a safe path component"),
        "unexpected error: {err}"
    );

    let err = read_control_checkpoint(&storage, "eu-west-1", "bucket_locator", "0A7F")
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("control stream partition must be four lowercase hex characters"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn control_checkpoint_is_monotonic_idempotent_and_digest_scoped() {
    let dir = tempdir().unwrap();
    let storage = Storage::new_at(dir.path()).await.unwrap();
    let first_digest = ControlRecordDigest::blake3(b"first");
    let first = ControlCheckpointRecord::new(
        "mesh-a",
        "eu-west-1",
        "bucket_locator",
        "0a7f",
        ControlStreamSequence::new(4).unwrap(),
        first_digest.clone(),
        "2026-07-02T00:00:00Z",
    );
    write_control_checkpoint(&storage, &first).await.unwrap();
    write_control_checkpoint(&storage, &first).await.unwrap();

    let same_sequence_different_digest = ControlCheckpointRecord::new(
        "mesh-a",
        "eu-west-1",
        "bucket_locator",
        "0a7f",
        ControlStreamSequence::new(4).unwrap(),
        ControlRecordDigest::blake3(b"diverged"),
        "2026-07-02T00:01:00Z",
    );
    let err = write_control_checkpoint(&storage, &same_sequence_different_digest)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("ControlStreamDivergence"),
        "unexpected error: {err}"
    );

    let backwards = ControlCheckpointRecord::new(
        "mesh-a",
        "eu-west-1",
        "bucket_locator",
        "0a7f",
        ControlStreamSequence::new(3).unwrap(),
        first_digest,
        "2026-07-02T00:02:00Z",
    );
    let err = write_control_checkpoint(&storage, &backwards)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("control checkpoint cannot move backwards"),
        "unexpected error: {err}"
    );

    let advanced = ControlCheckpointRecord::new(
        "mesh-a",
        "eu-west-1",
        "bucket_locator",
        "0a7f",
        ControlStreamSequence::new(5).unwrap(),
        ControlRecordDigest::blake3(b"advanced"),
        "2026-07-02T00:03:00Z",
    );
    write_control_checkpoint(&storage, &advanced).await.unwrap();
    assert_eq!(
        read_control_checkpoint(&storage, "eu-west-1", "bucket_locator", "0a7f")
            .await
            .unwrap(),
        Some(advanced)
    );
}

#[tokio::test]
async fn projection_diagnostic_detects_stream_projection_payload_mismatch() {
    let dir = tempdir().unwrap();
    let storage = Storage::new_at(dir.path()).await.unwrap();
    let stream_payload_proto = bucket_locator_payload_proto("eu-west-1");
    let stream_payload_json = bucket_locator_operator_json("eu-west-1");
    let projection_payload_json = bucket_locator_operator_json("us-east-1");
    let frame = ControlStreamFrame::new(
        sample_header_for_payload(1, &stream_payload_proto),
        stream_payload_proto,
    );
    append_control_stream_frame(&storage, "bucket_locator", "0a7f", &frame, None)
        .await
        .unwrap();

    let clean = diagnose_control_stream_projection(
        &storage,
        "bucket_locator",
        "0a7f",
        &[ControlProjectionRecord::new(
            "tenant_acme/releases",
            1,
            stream_payload_json,
        )],
    )
    .await
    .unwrap();
    assert!(clean.is_empty());

    let diagnostics = diagnose_control_stream_projection(
        &storage,
        "bucket_locator",
        "0a7f",
        &[ControlProjectionRecord::new(
            "tenant_acme/releases",
            1,
            projection_payload_json,
        )],
    )
    .await
    .unwrap();
    assert!(diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "mesh_control_projection_payload_mismatch"
            && diagnostic.record_key == "tenant_acme/releases"
            && diagnostic.repair_safe
            && diagnostic.proposed_action == "repair_routing_record_from_control_stream"
    }));
}
