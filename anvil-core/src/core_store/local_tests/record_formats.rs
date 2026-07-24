use super::local_stream_control::control_record_proto::{
    decode_object_manifest_record, decode_stream_head_record, decode_stream_record_index_row,
};
use super::*;
use crate::core_store::TABLE_TASK_LEASE_ROW;

#[test]
fn coremeta_lease_preconditions_round_trip_with_their_deadline() {
    let lease = CoreMutationPrecondition::CoreMetaLease {
        cf: CF_LEASES_FENCES.to_string(),
        table_id: TABLE_TASK_LEASE_ROW,
        tuple_key: b"task-lease:tenant-0:task-42".to_vec(),
        expected_payload_hash: format!("sha256:{}", "ab".repeat(32)),
        expires_at_unix_nanos: 1_900_000_000_000_000_000,
    };
    let batch = CoreMutationBatch {
        transaction_id: "txn-coremeta-lease-codec".to_string(),
        scope_partition: "task-queue".to_string(),
        committed_by_principal: "node:test".to_string(),
        root_publications: Vec::new(),
        preconditions: vec![lease.clone()],
        operations: Vec::new(),
    };

    let encoded = encode_core_mutation_batch(&batch).unwrap();
    let decoded = decode_core_mutation_batch(&encoded).unwrap();
    assert_eq!(decoded, batch);

    let original_hash = core_mutation_preconditions_hash(&[lease.clone()]).unwrap();
    let mut changed_deadline = lease;
    let CoreMutationPrecondition::CoreMetaLease {
        expires_at_unix_nanos,
        ..
    } = &mut changed_deadline
    else {
        unreachable!("test constructs a CoreMeta lease precondition");
    };
    *expires_at_unix_nanos += 1;
    assert_ne!(
        core_mutation_preconditions_hash(&[changed_deadline]).unwrap(),
        original_hash
    );
}

#[tokio::test]
async fn core_store_internal_control_records_written_by_store_are_binary_not_json_or_cbor() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    let object_ref = store
        .put_blob(PutBlob {
            logical_name: "tenant:t/bucket:b/object:format-proof".to_string(),
            bytes: b"representative object payload".repeat(8),
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "format-proof-object".to_string(),
        })
        .await
        .unwrap();
    let object_manifest_bytes = store
        .meta
        .get(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &object_manifest_meta_key(&object_ref),
        )
        .unwrap()
        .expect("object manifest row");
    assert_control_record_not_json_or_cbor("object manifest", &object_manifest_bytes);
    assert_eq!(
        decode_object_manifest_record(&object_manifest_bytes)
            .unwrap()
            .object_hash,
        object_ref.hash
    );

    let fence = store
        .acquire_fence(AcquireFence {
            fence_name: "tenant:t/bucket:b/object:format-proof".to_string(),
            authenticated_principal: "principal:writer".to_string(),
            ttl_ms: 60_000,
        })
        .await
        .unwrap();
    let fence_record_bytes = store
        .meta
        .get(
            CF_LEASES_FENCES,
            TABLE_CORE_FENCE_ROW,
            &super::local_stream_control::core_fence_row_key(
                "tenant:t/bucket:b/object:format-proof",
            )
            .unwrap(),
        )
        .unwrap()
        .expect("fence CoreMeta row");
    assert_control_record_not_json_or_cbor("fence CoreMeta row", &fence_record_bytes);
    assert_eq!(
        super::local_stream_control::read_core_fence_current_row(
            &store,
            "tenant:t/bucket:b/object:format-proof",
        )
        .unwrap()
        .expect("fence CoreMeta row")
        .fence_token,
        fence.fence_token
    );

    let stream_id = "tenant:t/bucket:b/format-proof-stream".to_string();
    let appended = store
        .append_stream(AppendStreamRecord {
            stream_id: stream_id.clone(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "format.created".to_string(),
            payload: br#"{"format":"stream-index"}"#.to_vec(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("format-proof-stream-1".to_string()),
        })
        .await
        .unwrap();
    let stream_head_bytes = store
        .meta
        .get(
            CF_STREAM_HEADS,
            TABLE_STREAM_HEAD_ROW,
            &stream_head_key(&stream_id),
        )
        .unwrap()
        .expect("stream head row");
    assert_control_record_not_json_or_cbor("stream head", &stream_head_bytes);
    assert_eq!(
        decode_stream_head_record(&stream_head_bytes)
            .unwrap()
            .last_sequence,
        appended.sequence
    );
    let stream_index_bytes = store
        .meta
        .get(
            CF_STREAM_RECORDS,
            TABLE_STREAM_RECORD_INDEX_ROW,
            &stream_record_key(&stream_id, appended.sequence),
        )
        .unwrap()
        .expect("stream index row");
    assert_control_record_not_json_or_cbor("stream record index", &stream_index_bytes);
    assert_eq!(
        decode_stream_record_index_row(&stream_index_bytes)
            .unwrap()
            .sequence,
        appended.sequence
    );

    let sealed = store
        .seal_stream_segment(SealStreamSegment {
            stream_id: stream_id.clone(),
            partition_id: "tenant:t/bucket:b".to_string(),
            through_sequence: Some(appended.sequence),
            segment_kind: "format-proof".to_string(),
            mutation_id: "format-proof-segment".to_string(),
        })
        .await
        .unwrap();
    let segment_bytes = store
        .get_blob(GetBlob {
            object_ref: sealed.object_ref.clone(),
        })
        .await
        .unwrap();
    assert_control_record_not_json_or_cbor("stream segment", &segment_bytes);
    assert!(segment_bytes.starts_with(CORE_STREAM_SEGMENT_MAGIC));
    assert_eq!(store.read_stream_segment(&sealed).await.unwrap().len(), 1);

    let target = encode_core_object_ref_target(&object_ref).unwrap();

    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: "txn-format-proof".to_string(),
            scope_partition: "tenant:t/bucket:b".to_string(),
            committed_by_principal: "principal:writer".to_string(),
            root_publications: vec![
                CoreMutationRootPublication::new(
                    "tenant:t/bucket:b",
                    WriterFamily::CoreControl.as_str(),
                )
                .coordinator(),
            ],
            preconditions: Vec::new(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id: "tenant:t/bucket:b".to_string(),
                stream_id: "object_metadata:t:b:format-proof".to_string(),
                record_kind: "object.put".to_string(),
                payload: br#"{"object":"format-proof"}"#.to_vec(),
                idempotency_key: Some("format-proof-txn-stream".to_string()),
            }],
        })
        .await
        .unwrap();
    let transaction_record_bytes = store
        .meta
        .get(
            CF_TRANSACTIONS,
            TABLE_EXPLICIT_TRANSACTION_ROW,
            &core_meta_tuple_key(&[
                CoreMetaTuplePart::Utf8("transaction"),
                CoreMetaTuplePart::Utf8("txn-format-proof"),
                CoreMetaTuplePart::Utf8("header"),
            ])
            .unwrap(),
        )
        .unwrap()
        .expect("transaction header row");
    assert_control_record_not_json_or_cbor("transaction header row", &transaction_record_bytes);
    assert_eq!(
        store
            .read_transaction("txn-format-proof")
            .await
            .unwrap()
            .expect("transaction")
            .transaction_id,
        "txn-format-proof"
    );

    let pending = store
        .admit_core_mutation(
            "stream.append",
            "stream",
            test_stream_append_target(
                "tenant:t/bucket:b/format-proof-pending",
                "tenant:t/bucket:b",
                "format.pending",
            ),
            "format-proof-pending".to_string(),
            Some("format-proof-pending-key".to_string()),
            CorePendingMutationPayload::Inline(br#"{"pending":true}"#),
            Vec::new(),
        )
        .await
        .unwrap();
    let pending_bytes = store
        .meta
        .get(
            CF_TRANSACTIONS,
            TABLE_PENDING_MUTATION_ROW,
            &admission_record_key(&pending.target.admission_shard().hash, pending.sequence),
        )
        .unwrap()
        .expect("pending mutation row");
    assert_control_record_not_json_or_cbor("pending mutation", &pending_bytes);
    assert_eq!(
        decode_stored_pending_mutation_row(&pending_bytes)
            .unwrap()
            .0
            .mutation_id,
        "format-proof-pending"
    );

    let latest_anchor = store
        .read_latest_root_anchor(core_transaction_root_anchor_key())
        .await
        .unwrap()
        .expect("latest root anchor");
    let root_anchor_bytes = store
        .meta
        .get(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_cache_key(core_transaction_root_anchor_key()),
        )
        .unwrap()
        .expect("root anchor cache row");
    assert_control_record_not_json_or_cbor("root anchor", &root_anchor_bytes);
    assert_eq!(
        decode_root_cache_row(&root_anchor_bytes)
            .unwrap()
            .root_generation,
        latest_anchor.root_generation
    );

    let generation_row = store
        .meta
        .get(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_anchor_generation_key(
                &latest_anchor.root_key_hash,
                latest_anchor.root_generation,
            ),
        )
        .unwrap()
        .expect("root anchor generation row");
    assert_control_record_not_json_or_cbor("root anchor generation row", &generation_row);
    assert_eq!(
        decode_root_cache_row(&generation_row)
            .unwrap()
            .root_generation,
        latest_anchor.root_generation
    );
}

#[tokio::test]
async fn authenticated_append_principal_is_durable_and_idempotency_bound() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let stream_id = "tenant:t/bucket:b/authenticated-principal".to_string();
    let append = AppendStreamRecord {
        stream_id: stream_id.clone(),
        partition_id: "tenant:t/bucket:b".to_string(),
        record_kind: "producer.event".to_string(),
        payload: b"payload".to_vec(),
        content_type: Some("application/octet-stream".to_string()),
        user_metadata_json: r#"{"authenticated_principal":"tenant/1/principal/spoofed"}"#
            .to_string(),
        fence: None,
        transaction_id: None,
        idempotency_key: Some("authenticated-principal-1".to_string()),
    };

    store
        .append_stream_authenticated(append.clone(), "tenant/1/principal/producer-a")
        .await
        .unwrap();
    let records = store
        .read_stream(ReadStream {
            stream_id: stream_id.clone(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].authenticated_principal,
        "tenant/1/principal/producer-a"
    );
    assert!(records[0].user_metadata_json.contains("spoofed"));

    let error = store
        .append_stream_authenticated(append, "tenant/1/principal/producer-b")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("authenticated principal mismatch")
    );
}

#[tokio::test]
async fn large_stream_payloads_use_locator_rows_not_rocksdb_inline_payloads() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let stream_id = "tenant:t/bucket:b/large-stream-payload".to_string();
    let payload = vec![0x5a; CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES * 8];

    let appended = store
        .append_stream(AppendStreamRecord {
            stream_id: stream_id.clone(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "large.payload".to_string(),
            payload: payload.clone(),
            content_type: Some("application/octet-stream".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("large-stream-payload-1".to_string()),
        })
        .await
        .unwrap();

    let stream_index_bytes = store
        .meta
        .get(
            CF_STREAM_RECORDS,
            TABLE_STREAM_RECORD_INDEX_ROW,
            &stream_record_key(&stream_id, appended.sequence),
        )
        .unwrap()
        .expect("stream index row");
    assert!(
        stream_index_bytes.len() <= CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES,
        "stream metadata row must stay bounded; got {} bytes",
        stream_index_bytes.len()
    );
    let row = decode_stream_record_index_row(&stream_index_bytes).unwrap();
    assert_eq!(row.payload_len, payload.len() as u64);
    assert!(
        row.inline_payload.is_none(),
        "large payload must not be inlined"
    );
    assert!(
        row.payload_locator.is_some(),
        "large payload must be represented by a byte-pipeline locator"
    );

    let records = store
        .read_stream(ReadStream {
            stream_id,
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, payload);
    assert_eq!(
        records[0].payload_hash,
        format!("sha256:{}", sha256_hex(&payload))
    );
}

#[tokio::test]
async fn stream_reads_apply_after_sequence_and_limit_without_replaying_the_stream() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let stream_id = "tenant:t/bucket:b/bounded-stream-read".to_string();

    for sequence in 1..=3 {
        store
            .append_stream(AppendStreamRecord {
                stream_id: stream_id.clone(),
                partition_id: "tenant:t/bucket:b".to_string(),
                record_kind: "bounded.record".to_string(),
                payload: format!("record-{sequence}").into_bytes(),
                content_type: Some("text/plain".to_string()),
                user_metadata_json: "{}".to_string(),
                fence: None,
                transaction_id: None,
                idempotency_key: Some(format!("bounded-stream-read-{sequence}")),
            })
            .await
            .unwrap();
    }

    let records = store
        .read_stream(ReadStream {
            stream_id,
            after_sequence: 1,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].sequence, 2);
    assert_eq!(records[0].payload, b"record-2");
}
