use super::local_stream_control::control_record_proto::encode_object_manifest_record;
use super::*;
use crate::core_store::meta::{CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW};

fn mutation_root_publication(
    root_anchor_key: impl Into<String>,
    writer_family: WriterFamily,
    transaction_coordinator: bool,
) -> CoreMutationRootPublication {
    CoreMutationRootPublication {
        root_anchor_key: root_anchor_key.into(),
        writer_families: vec![writer_family.as_str().to_string()],
        transaction_coordinator,
    }
}

#[tokio::test]
async fn core_store_pending_mutation_records_never_inline_large_payloads_before_finalisation() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let bytes = vec![b'x'; CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES + 1];
    store
        .admit_core_mutation(
            "object.put",
            "object_blob",
            test_object_put_target("tenant:t/bucket:b/object:large"),
            "large-payload-admission".to_string(),
            None,
            CorePendingMutationPayload::Landed(&bytes),
            Vec::new(),
        )
        .await
        .unwrap();

    let pending_mutation_records = read_test_pending_mutation_records(&store).await;
    assert_eq!(pending_mutation_records.len(), 1);
    assert!(
        pending_mutation_records[0].1.is_empty(),
        "large payloads must never be embedded in RocksDB admission payloads"
    );
    let landed = pending_mutation_records[0].0.landed_bytes.first().unwrap();
    assert_eq!(landed.length, bytes.len() as u64);
    assert!(
        storage
            .resolve_relative_storage_path(&landed.relative_path)
            .unwrap()
            .exists(),
        "large payload bytes must land outside the pending mutation and be referenced by hash/length"
    );
    store
        .verify_landed_bytes_ref_row(
            &pending_mutation_records[0].0.target.admission_shard().hash,
            &landed.landing_id,
            "large-payload-admission",
            &landed.sha256,
            landed.length,
            &[],
        )
        .unwrap();
    assert!(
        !storage
            .core_store_landed_bytes_path()
            .join("sha256")
            .join(landed.sha256.trim_start_matches("sha256:")[0..2].to_string())
            .join(format!(
                "{}.meta",
                landed.sha256.trim_start_matches("sha256:")
            ))
            .exists(),
        "landed bytes metadata must live in RocksDB/CoreMeta, not sidecar files"
    );
}

#[tokio::test]
async fn core_store_landed_bytes_are_reclaimed_after_finalised_shard_refs_leave_recovery_state() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let bytes = vec![b'd'; CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES + 8];

    store
        .admit_core_mutation(
            "object.put",
            "object_blob",
            test_object_put_target("tenant:t/bucket:b/object:dedup-a"),
            "dedup-admission-a".to_string(),
            None,
            CorePendingMutationPayload::Landed(&bytes),
            Vec::new(),
        )
        .await
        .unwrap();
    store
        .admit_core_mutation(
            "object.put",
            "object_blob",
            test_object_put_target("tenant:t/bucket:b/object:dedup-b"),
            "dedup-admission-b".to_string(),
            None,
            CorePendingMutationPayload::Landed(&bytes),
            Vec::new(),
        )
        .await
        .unwrap();

    let records = read_test_pending_mutation_records(&store).await;
    assert_eq!(records.len(), 2);
    let first_landed = records[0].0.landed_bytes[0].clone();
    let landed_path = storage
        .resolve_relative_storage_path(&first_landed.relative_path)
        .unwrap();

    store
        .mark_pending_mutation_finalised_unlocked(&records[0].0, "committed")
        .await
        .unwrap();
    assert!(landed_path.exists());

    store
        .mark_pending_mutation_finalised_unlocked(&records[1].0, "committed")
        .await
        .unwrap();
    assert!(
        landed_path.exists(),
        "foreground finalisation must not scan other admission shards before reclaiming shared content"
    );
    store.unregister_process_instance_for_tests();
    drop(store);

    let recovered = CoreStore::new(storage).await.unwrap();
    assert!(!landed_path.exists());
    recovered.unregister_process_instance_for_tests();
}

#[tokio::test]
async fn corestore_rocksdb_records_never_inline_large_payloads() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let stream_id = "tenant:t/bucket:b/stream:large-payload".to_string();
    let payload = vec![0x5a; CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES * 8];

    store
        .append_stream(AppendStreamRecord {
            stream_id: stream_id.clone(),
            partition_id: "partition:large-payload".to_string(),
            record_kind: "object.put".to_string(),
            payload: payload.clone(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("large-stream-payload".to_string()),
        })
        .await
        .unwrap();

    let rows = store
        .meta
        .scan_prefix_page(
            CF_STREAM_RECORDS,
            TABLE_STREAM_RECORD_INDEX_ROW,
            &stream_record_prefix(&stream_id),
            None,
            2,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].payload.len() <= CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES,
        "stream record metadata row must stay bounded and payload-free"
    );
    let row = super::local_stream_control::control_record_proto::decode_stream_record_index_row(
        &rows[0].payload,
    )
    .unwrap();
    assert_eq!(row.payload_len, payload.len() as u64);
    assert_eq!(row.payload_hash, format!("sha256:{}", sha256_hex(&payload)));
    assert_eq!(row.inline_payload, None);
    assert!(
        row.payload_locator
            .as_ref()
            .is_some_and(|locator| locator.manifest_length > 0)
    );
    assert!(
        serde_json::from_slice::<serde_json::Value>(&rows[0].payload).is_err(),
        "stream record metadata row must use protobuf, not JSON"
    );

    let read = store
        .read_stream(ReadStream {
            stream_id,
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].payload, payload);

    let object_payload = vec![0xa5; CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES * 8];
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: "tenant:t/bucket:b/object:large-payload".to_string(),
            bytes: object_payload.clone(),
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "large-object-payload".to_string(),
        })
        .await
        .unwrap();
    assert!(object_ref.manifest_ref.starts_with("core-manifest-sha256:"));
    assert_eq!(object_ref.logical_size, object_payload.len() as u64);
    assert!(
        !object_ref.placements.is_empty(),
        "large object bytes must produce CoreStore shard placement metadata"
    );
    for placement in &object_ref.placements {
        assert!(
            store
                .shard_path(
                    &placement.node_id,
                    &object_ref.encoding.block_id,
                    placement.shard_index,
                )
                .exists(),
            "large object shard must exist for placement {placement:?}"
        );
    }
    assert_eq!(
        store
            .get_blob(GetBlob {
                object_ref: object_ref.clone()
            })
            .await
            .unwrap(),
        object_payload
    );
    let object_rows = store
        .meta
        .scan_prefix_page(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            b"",
            None,
            CORE_META_MAX_SCAN_PAGE_ROWS,
        )
        .unwrap();
    assert!(!object_rows.is_empty());
    assert!(
        object_rows
            .iter()
            .all(|row| row.payload.len() <= CORE_META_MAX_VALUE_BYTES),
        "object metadata rows must remain bounded manifests/locators"
    );
    let inline_rows = store
        .meta
        .scan_prefix_page(
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            b"",
            None,
            CORE_META_MAX_SCAN_PAGE_ROWS,
        )
        .unwrap();
    assert!(
        inline_rows.iter().all(|row| row.payload != object_payload),
        "large object bytes must not be copied into inline RocksDB payload rows"
    );
}

#[tokio::test]
async fn core_store_landed_bytes_existing_file_must_match_hash() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let bytes = b"expected landed bytes".to_vec();
    let hash = sha256_hex(&bytes);
    let final_path = store.landed_bytes_path(&hash);
    fs::create_dir_all(final_path.parent().unwrap())
        .await
        .unwrap();
    fs::write(&final_path, vec![0x55; bytes.len()])
        .await
        .unwrap();

    assert!(
        store
            .admit_core_mutation(
                "stream.append",
                "stream",
                test_stream_append_target(
                    "tenant:t/bucket:b/corrupt-landed",
                    "tenant:t/bucket:b",
                    "event.created",
                ),
                "corrupt-existing-landed".to_string(),
                None,
                CorePendingMutationPayload::Landed(&bytes),
                Vec::new(),
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("existing hash mismatch"),
        "admission must verify existing landed bytes by hash before referencing them from pending mutation"
    );
}

#[tokio::test]
async fn core_store_pending_mutation_records_include_boundary_values() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    store
        .admit_core_mutation(
            "object.put",
            "object_blob",
            test_object_put_target("tenant:t/bucket:b/object:bounded"),
            "bounded-payload-admission".to_string(),
            None,
            CorePendingMutationPayload::Landed(b"bounded"),
            vec![CoreBoundaryValue {
                schema_generation: 2,
                name: "customer_tenant".to_string(),
                value_type: "uuid".to_string(),
                value: "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a".to_string(),
                categories: vec!["security_realm".to_string()],
                source_kind: "user_metadata_json_pointer".to_string(),
                required: true,
                max_values_per_block: 1,
                placement_affinity: "none".to_string(),
                compaction_scope: "none".to_string(),
                shared_ranges_allowed: false,
                shared_record_kinds: Vec::new(),
            }],
        )
        .await
        .unwrap();

    let pending_mutation_records = read_test_pending_mutation_records(&store).await;
    assert_eq!(pending_mutation_records.len(), 1);
    assert_eq!(pending_mutation_records[0].0.boundary_values.len(), 1);
    assert_eq!(
        pending_mutation_records[0].0.boundary_values[0].name,
        "customer_tenant"
    );
    assert_eq!(
        pending_mutation_records[0].0.boundary_values[0].value,
        "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a"
    );
    let landed = pending_mutation_records[0].0.landed_bytes.first().unwrap();
    let stored = store
        .meta
        .get(
            CF_MATERIALISATION,
            TABLE_LANDED_BYTE_REF_ROW,
            &landed_byte_ref_key(
                &pending_mutation_records[0].0.target.admission_shard().hash,
                &landed.landing_id,
            ),
        )
        .unwrap()
        .expect("landed byte metadata row");
    let stored = decode_landed_byte_ref_row(&stored).unwrap();
    assert_eq!(stored.boundary_values[0].name, "customer_tenant");
}

#[tokio::test]
async fn core_store_pending_mutation_header_uses_deterministic_protobuf() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let record = store
        .admit_core_mutation(
            "stream.append",
            "stream",
            test_stream_append_target(
                "tenant:t/bucket:b/canonical-pending_mutation",
                "tenant:t/bucket:b",
                "event.created",
            ),
            "protobuf-pending_mutation-row".to_string(),
            Some("canonical-idempotency".to_string()),
            CorePendingMutationPayload::Inline(br#"{"ok":true}"#),
            Vec::new(),
        )
        .await
        .unwrap();

    let row = store
        .meta
        .get(
            CF_TRANSACTIONS,
            TABLE_PENDING_MUTATION_ROW,
            &admission_record_key(&record.target.admission_shard().hash, 1),
        )
        .unwrap()
        .expect("pending mutation row");
    assert!(decode_stored_pending_mutation_row(&row).is_ok());
    assert!(serde_json::from_slice::<serde_json::Value>(&row).is_err());
}

#[tokio::test]
async fn core_store_admission_records_are_not_file_backed() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    store
        .admit_core_mutation(
            "stream.append",
            "stream",
            test_stream_append_target(
                "tenant:t/bucket:b/partial-pending_mutation",
                "tenant:t/bucket:b",
                "event.created",
            ),
            "partial-tail-pending_mutation".to_string(),
            None,
            CorePendingMutationPayload::Inline(br#"{"ok":true}"#),
            Vec::new(),
        )
        .await
        .unwrap();

    let records = store
        .read_pending_mutation_records_with_payload()
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert!(
        !store.admission_root().join("pending_mutation").exists(),
        "admission state must live in CoreStore metadata, not a sidecar pending mutation file"
    );
}

#[tokio::test]
async fn core_store_pending_mutation_admission_writes_signed_local_evidence() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let record = store
        .admit_core_mutation(
            "stream.append",
            "stream",
            test_stream_append_target(
                "tenant:t/bucket:b/certified-pending_mutation",
                "tenant:t/bucket:b",
                "event.created",
            ),
            "certified-pending_mutation-admission".to_string(),
            Some("certified-idempotency".to_string()),
            CorePendingMutationPayload::Landed(br#"{"ok":true}"#),
            Vec::new(),
        )
        .await
        .unwrap();

    let pending_mutation_hash_input = encode_pending_mutation_hash_input(&record, &[]).unwrap();
    let evidence = store
        .verify_local_admission_evidence(&record, &pending_mutation_hash_input)
        .unwrap();
    assert_eq!(
        evidence.local_receipt.local_metadata_fsync_sequence,
        record.sequence
    );
    assert_eq!(evidence.local_receipt.landed_byte_hashes.len(), 1);
    assert_eq!(evidence.local_receipt.descriptor_hashes.len(), 1);
    assert!(
        evidence
            .local_receipt
            .signed_payload_hash
            .starts_with("sha256:")
    );
    assert!(evidence.signed_payload_hash.starts_with("sha256:"));
    assert!(!evidence.local_receipt.source_signature.is_empty());
    assert!(!evidence.source_signature.is_empty());

    let evidence_bytes = store
        .meta
        .get(
            CF_TRANSACTIONS,
            TABLE_LOCAL_ADMISSION_EVIDENCE_ROW,
            &admission_evidence_key(&record.target.admission_shard().hash, record.sequence),
        )
        .unwrap()
        .expect("pending mutation evidence row");
    assert!(decode_local_admission_evidence(&evidence_bytes).is_ok());
    assert!(serde_json::from_slice::<serde_json::Value>(&evidence_bytes).is_err());
}

#[tokio::test]
async fn core_store_recovery_rejects_pending_mutation_without_local_evidence() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let record = test_pending_mutation_record(
        "uncertified-pending_mutation-record",
        unix_timestamp_nanos(),
        1,
    );
    let shard = record.target.admission_shard();
    write_test_pending_mutation_records(&store, vec![record]).await;
    store
        .meta
        .delete(
            CF_TRANSACTIONS,
            TABLE_LOCAL_ADMISSION_EVIDENCE_ROW,
            &admission_evidence_key(&shard.hash, 1),
        )
        .unwrap();
    store.unregister_process_instance_for_tests();
    drop(store);

    assert!(
        CoreStore::new(storage)
            .await
            .unwrap_err()
            .to_string()
            .contains("local admission evidence"),
        "recovery must not replay a pending mutation that lacks committed admission evidence"
    );
}

#[tokio::test]
async fn core_store_object_manifest_includes_boundary_values() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let boundary_value = CoreBoundaryValue {
        schema_generation: 2,
        name: "customer_tenant".to_string(),
        value_type: "uuid".to_string(),
        value: "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a".to_string(),
        categories: vec!["security_realm".to_string()],
        source_kind: "user_metadata_json_pointer".to_string(),
        required: true,
        max_values_per_block: 1,
        placement_affinity: "none".to_string(),
        compaction_scope: "none".to_string(),
        shared_ranges_allowed: false,
        shared_record_kinds: Vec::new(),
    };
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: "tenant:t/bucket:b/object:bounded".to_string(),
            bytes: vec![0x62; 80 * 1024],
            boundary_values: vec![boundary_value.clone()],
            region_id: "local".to_string(),
            mutation_id: "bounded-manifest".to_string(),
        })
        .await
        .unwrap();

    let manifest = store.read_object_manifest(&object_ref).await.unwrap();
    assert_eq!(manifest.boundary_values, vec![boundary_value]);
    for placement in &manifest.placements {
        assert_ne!(placement.written_at_unix_nanos, 0);
        assert!(placement.signed_payload_hash.starts_with("sha256:"));
        assert_eq!(placement.signature_algorithm, "ed25519");
        assert!(!placement.receipt_signature.is_empty());
    }

    let mut bad_manifest = manifest.clone();
    bad_manifest.placements[0].receipt_signature[0] ^= 0x01;
    store
        .meta
        .put(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &object_manifest_meta_key(&object_ref),
            &encode_object_manifest_record(&bad_manifest).unwrap(),
        )
        .unwrap();
    assert!(
        store
            .get_blob(GetBlob {
                object_ref: object_ref.clone()
            })
            .await
            .unwrap_err()
            .to_string()
            .contains("signature verification failed")
    );
}

#[tokio::test]
async fn core_store_recovers_unfinalised_put_blob_pending_mutation_on_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let bytes = b"recover object from pending_mutation".to_vec();
    let logical_name = "tenant:t/bucket:b/object:recovered";
    let payload_hash = format!("sha256:{}", sha256_hex(&bytes));
    store
        .admit_core_mutation(
            "object.put",
            "object_blob",
            CorePendingMutationTarget::ObjectPut {
                logical_name: logical_name.to_string(),
                region_id: "local".to_string(),
                erasure_profile_id: LOCAL_ERASURE_PROFILE_ID.to_string(),
                encryption: "none".to_string(),
                block_plain_hash: payload_hash.clone(),
                object_hash: payload_hash.clone(),
                object_logical_size: bytes.len() as u64,
                compression: none_compression_descriptor(&bytes),
                writer_generation: 0_u64,
                block_ordinal: 0_u64,
                logical_offset: 0,
            },
            "recover-object-from-pending_mutation".to_string(),
            None,
            CorePendingMutationPayload::Landed(&bytes),
            Vec::new(),
        )
        .await
        .unwrap();
    store.unregister_process_instance_for_tests();
    drop(store);

    let recovered = CoreStore::new(storage).await.unwrap();
    let object_ref =
        test_object_ref_for_payload(&recovered, logical_name, &bytes, LOCAL_EC_4_2_PROFILE);
    assert_eq!(
        recovered
            .get_blob(GetBlob {
                object_ref: object_ref.clone()
            })
            .await
            .unwrap(),
        bytes
    );
    assert!(
        read_test_pending_mutation_records(&recovered)
            .await
            .is_empty(),
        "startup recovery must checkpoint recovered object pending mutation rows"
    );
    assert_eq!(recovered.admission_landed_bytes().await.unwrap(), 0);
}

#[tokio::test]
async fn core_store_recovers_unfinalised_stream_append_pending_mutation_on_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let payload = br#"{"event":"recover"}"#.to_vec();
    let idempotency_key = "recover-stream-idempotency";
    store
        .admit_core_mutation(
            "stream.append",
            "stream",
            test_stream_append_target(
                "tenant:t/bucket:b/recovered-stream",
                "tenant:t/bucket:b",
                "event.recovered",
            ),
            "recover-stream-from-pending_mutation".to_string(),
            Some(idempotency_key.to_string()),
            CorePendingMutationPayload::Inline(&payload),
            Vec::new(),
        )
        .await
        .unwrap();
    store.unregister_process_instance_for_tests();
    drop(store);

    let recovered = CoreStore::new(storage).await.unwrap();
    let records = recovered
        .read_stream(ReadStream {
            stream_id: "tenant:t/bucket:b/recovered-stream".to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record_kind, "event.recovered");
    assert_eq!(records[0].payload, payload);
    let expected_idempotency_hash = format!("sha256:{}", sha256_hex(idempotency_key.as_bytes()));
    assert_eq!(
        records[0].idempotency_key_hash.as_deref(),
        Some(expected_idempotency_hash.as_str())
    );
    assert!(
        read_test_pending_mutation_records(&recovered)
            .await
            .is_empty(),
        "startup recovery must checkpoint recovered stream pending mutation rows"
    );
}

#[tokio::test]
async fn core_store_recovers_unfinalised_mutation_batch_pending_mutation_on_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let batch = CoreMutationBatch {
        transaction_id: "recover-mutation-batch".to_string(),
        scope_partition: "tenant:t/bucket:b".to_string(),
        committed_by_principal: "principal:recovery".to_string(),
        root_publications: vec![
            mutation_root_publication(
                "stream/object_metadata:t:b:batch-recovered",
                WriterFamily::Stream,
                false,
            ),
            mutation_root_publication("tenant:t/bucket:b", WriterFamily::CoreControl, true),
        ],
        preconditions: Vec::new(),
        operations: vec![CoreMutationOperation::StreamAppend {
            partition_id: "tenant:t/bucket:b".to_string(),
            stream_id: "object_metadata:t:b:batch-recovered".to_string(),
            record_kind: "object.put".to_string(),
            payload: br#"{"object":"batch-recovered"}"#.to_vec(),
            idempotency_key: Some("batch-recovered-event".to_string()),
        }],
    };
    store
        .admit_core_mutation(
            "mutation.batch",
            "core_control",
            CorePendingMutationTarget::MutationBatch {
                transaction_id: batch.transaction_id.clone(),
                scope_partition: batch.scope_partition.clone(),
                operation_count: batch.operations.len() as u64,
            },
            batch.transaction_id.clone(),
            Some(batch.transaction_id.clone()),
            CorePendingMutationPayload::Inline(&encode_core_mutation_batch(&batch).unwrap()),
            Vec::new(),
        )
        .await
        .unwrap();
    store.unregister_process_instance_for_tests();
    drop(store);

    let recovered = CoreStore::new(storage).await.unwrap();
    let transaction = recovered
        .read_transaction("recover-mutation-batch")
        .await
        .unwrap()
        .expect("recovered transaction");
    assert_eq!(
        transaction.state,
        CoreTransactionState::Committed,
        "recovery finalisation error: {:?}",
        transaction.finalisation_error
    );
    let records = recovered
        .read_stream(ReadStream {
            stream_id: "object_metadata:t:b:batch-recovered".to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record_kind, "object.put");
    let stream_anchor = recovered
        .read_latest_root_anchor("stream/object_metadata:t:b:batch-recovered")
        .await
        .unwrap()
        .expect("recovered canonical stream root");
    assert_eq!(stream_anchor.root_generation, 1);
    assert_eq!(
        stream_anchor.mutation_last.as_deref(),
        Some("recover-mutation-batch")
    );
    assert!(
        read_test_pending_mutation_records(&recovered)
            .await
            .is_empty(),
        "startup recovery must checkpoint recovered mutation batch pending mutation rows"
    );
}

#[tokio::test]
async fn core_store_recovery_finalises_admitted_delete_after_winning_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let scope_partition = "tenant:t/bucket:b/task-queue";
    let row_root = "tenant:t/bucket:b/task-row";
    let row_key = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("stale-delete")]).unwrap();
    let seed_transaction = "seed-stale-delete";
    let row_payload = encode_core_meta_inline_payload_row(
        b"stale-delete",
        core_meta_committed_row_common(
            scope_partition,
            core_meta_root_key_hash(row_root),
            1,
            seed_transaction,
            1,
        ),
    )
    .unwrap();
    store
        .commit_coremeta_root_groups(
            seed_transaction,
            &[CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: &row_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&row_payload),
            }],
            &[CoreMetaRootPublication::new(
                row_root,
                WriterFamily::CoreControl,
            )],
        )
        .await
        .unwrap();

    let stale_transaction = "recover-superseded-rooted-delete";
    let stale_batch = CoreMutationBatch {
        transaction_id: stale_transaction.to_string(),
        scope_partition: scope_partition.to_string(),
        committed_by_principal: "principal:recovery".to_string(),
        root_publications: vec![
            mutation_root_publication(row_root, WriterFamily::CoreControl, false),
            mutation_root_publication(scope_partition, WriterFamily::CoreControl, true),
        ],
        preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
            cf: CF_INLINE_PAYLOADS.to_string(),
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key: row_key.clone(),
            expected_payload_hash: Some(core_meta_payload_digest(
                TABLE_INLINE_PAYLOAD_ROW,
                &row_payload,
            )),
            require_absent: false,
            require_present: true,
        }],
        operations: vec![CoreMutationOperation::CoreMetaDelete {
            partition_id: scope_partition.to_string(),
            cf: CF_INLINE_PAYLOADS.to_string(),
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key: row_key.clone(),
        }],
    };
    store
        .validate_mutation_root_publications_unlocked(&stale_batch, false)
        .unwrap();
    store
        .admit_core_mutation(
            "mutation.batch",
            WriterFamily::CoreControl.as_str(),
            CorePendingMutationTarget::MutationBatch {
                transaction_id: stale_transaction.to_string(),
                scope_partition: scope_partition.to_string(),
                operation_count: 1,
            },
            stale_transaction.to_string(),
            Some(stale_transaction.to_string()),
            CorePendingMutationPayload::Inline(&encode_core_mutation_batch(&stale_batch).unwrap()),
            Vec::new(),
        )
        .await
        .unwrap();

    let winning_receipt = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: "winning-rooted-delete".to_string(),
            scope_partition: scope_partition.to_string(),
            committed_by_principal: "principal:winner".to_string(),
            root_publications: stale_batch.root_publications.clone(),
            preconditions: stale_batch.preconditions.clone(),
            operations: stale_batch.operations.clone(),
        })
        .await
        .unwrap();
    assert_eq!(winning_receipt.state, CoreTransactionState::Committed);
    assert!(
        store
            .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &row_key)
            .unwrap()
            .is_none()
    );

    store.unregister_process_instance_for_tests();
    drop(store);
    let recovered = CoreStore::new(storage).await.unwrap();
    let transaction = recovered
        .read_transaction(stale_transaction)
        .await
        .unwrap()
        .expect("recovered stale delete transaction");
    assert_eq!(transaction.state, CoreTransactionState::FinalisationFailed);
    assert!(transaction.visible_updates.is_empty());
    assert!(transaction.finalisation_error.is_some());
    let winning_anchor = recovered
        .read_latest_root_anchor(scope_partition)
        .await
        .unwrap()
        .expect("winning root remains visible");
    assert_eq!(
        winning_anchor.mutation_last.as_deref(),
        Some("winning-rooted-delete")
    );
    assert!(
        read_test_pending_mutation_records(&recovered)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn core_store_recovery_finalises_materialised_stream_without_operation_idempotency() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let transaction_id = "recover-materialised-mixed-batch";
    let scope_partition = "tenant:t/bucket:b";
    let stream_id = "object_metadata:t:b:mixed-recovered";
    let row_key = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("mixed-recovered")]).unwrap();
    let row_payload = encode_core_meta_inline_payload_row(
        b"mixed-recovered",
        core_meta_committed_row_common(
            scope_partition,
            core_meta_root_key_hash("tenant:t/bucket:b/mixed-recovered"),
            1,
            transaction_id,
            1,
        ),
    )
    .unwrap();
    let batch = CoreMutationBatch {
        transaction_id: transaction_id.to_string(),
        scope_partition: scope_partition.to_string(),
        committed_by_principal: "principal:recovery".to_string(),
        root_publications: vec![
            mutation_root_publication(format!("stream/{stream_id}"), WriterFamily::Stream, false),
            mutation_root_publication(
                "tenant:t/bucket:b/mixed-recovered",
                WriterFamily::CoreControl,
                false,
            ),
            mutation_root_publication(scope_partition, WriterFamily::CoreControl, true),
        ],
        preconditions: vec![
            CoreMutationPrecondition::StreamHead {
                stream_id: stream_id.to_string(),
                expected_last_sequence: 0,
                expected_last_event_hash: ZERO_HASH.to_string(),
            },
            CoreMutationPrecondition::CoreMetaRow {
                cf: CF_INLINE_PAYLOADS.to_string(),
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: row_key.clone(),
                expected_payload_hash: None,
                require_absent: true,
                require_present: false,
            },
        ],
        operations: vec![
            CoreMutationOperation::StreamAppend {
                partition_id: scope_partition.to_string(),
                stream_id: stream_id.to_string(),
                record_kind: "object.put".to_string(),
                payload: br#"{"object":"mixed-recovered"}"#.to_vec(),
                idempotency_key: None,
            },
            CoreMutationOperation::CoreMetaPut {
                partition_id: scope_partition.to_string(),
                cf: CF_INLINE_PAYLOADS.to_string(),
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: row_key.clone(),
                payload: row_payload.clone(),
            },
        ],
    };
    store
        .admit_core_mutation(
            "mutation.batch",
            "core_control",
            CorePendingMutationTarget::MutationBatch {
                transaction_id: batch.transaction_id.clone(),
                scope_partition: batch.scope_partition.clone(),
                operation_count: batch.operations.len() as u64,
            },
            batch.transaction_id.clone(),
            Some(batch.transaction_id.clone()),
            CorePendingMutationPayload::Inline(&encode_core_mutation_batch(&batch).unwrap()),
            Vec::new(),
        )
        .await
        .unwrap();
    let CoreMutationOperation::StreamAppend {
        partition_id,
        stream_id,
        record_kind,
        payload,
        idempotency_key,
    } = &batch.operations[0]
    else {
        unreachable!();
    };
    store
        .append_stream_unlocked_for_principal(
            AppendStreamRecord {
                stream_id: stream_id.clone(),
                partition_id: partition_id.clone(),
                record_kind: record_kind.clone(),
                payload: payload.clone(),
                content_type: None,
                user_metadata_json: "{}".to_string(),
                fence: None,
                transaction_id: Some(batch.transaction_id.clone()),
                idempotency_key: idempotency_key.clone(),
            },
            batch.committed_by_principal.clone(),
        )
        .await
        .unwrap();
    store
        .commit_coremeta_root_groups(
            transaction_id,
            &[CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: &row_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&row_payload),
            }],
            &[CoreMetaRootPublication::new(
                "tenant:t/bucket:b/mixed-recovered",
                WriterFamily::CoreControl,
            )],
        )
        .await
        .unwrap();
    store.unregister_process_instance_for_tests();
    drop(store);

    let recovered = CoreStore::new(storage).await.unwrap();
    let transaction = recovered
        .read_transaction(transaction_id)
        .await
        .unwrap()
        .expect("recovered mixed transaction");
    assert_eq!(
        transaction.state,
        CoreTransactionState::Committed,
        "recovery finalisation error: {:?}",
        transaction.finalisation_error
    );
    assert_eq!(transaction.visible_updates.len(), 2);
    assert_eq!(
        recovered
            .meta
            .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &row_key)
            .unwrap(),
        Some(row_payload)
    );
    assert_eq!(
        recovered
            .read_stream(ReadStream {
                stream_id: stream_id.to_string(),
                after_sequence: 0,
                limit: 10,
            })
            .await
            .unwrap()
            .len(),
        1,
        "startup recovery must not duplicate the materialised stream append"
    );
    assert!(
        read_test_pending_mutation_records(&recovered)
            .await
            .is_empty(),
        "startup recovery must checkpoint the materialised mixed mutation"
    );
}

#[tokio::test]
async fn core_store_recovery_completes_partially_materialised_mutation_batch() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let transaction_id = "recover-partially-materialised-batch";
    let scope_partition = "tenant:t/bucket:b";
    let stream_id = "object_metadata:t:b:partially-materialised";
    let row_key = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("partial-row")]).unwrap();
    let row_payload = encode_core_meta_inline_payload_row(
        b"partial-row",
        core_meta_committed_row_common(
            scope_partition,
            core_meta_root_key_hash("tenant:t/bucket:b/partial-row"),
            1,
            transaction_id,
            1,
        ),
    )
    .unwrap();
    let batch = CoreMutationBatch {
        transaction_id: transaction_id.to_string(),
        scope_partition: scope_partition.to_string(),
        committed_by_principal: "principal:recovery".to_string(),
        root_publications: vec![
            mutation_root_publication(format!("stream/{stream_id}"), WriterFamily::Stream, false),
            mutation_root_publication(
                "tenant:t/bucket:b/partial-row",
                WriterFamily::CoreControl,
                false,
            ),
            mutation_root_publication(scope_partition, WriterFamily::CoreControl, true),
        ],
        preconditions: vec![
            CoreMutationPrecondition::StreamHead {
                stream_id: stream_id.to_string(),
                expected_last_sequence: 0,
                expected_last_event_hash: ZERO_HASH.to_string(),
            },
            CoreMutationPrecondition::CoreMetaRow {
                cf: CF_INLINE_PAYLOADS.to_string(),
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: row_key.clone(),
                expected_payload_hash: None,
                require_absent: true,
                require_present: false,
            },
        ],
        operations: vec![
            CoreMutationOperation::StreamAppend {
                partition_id: scope_partition.to_string(),
                stream_id: stream_id.to_string(),
                record_kind: "object.put".to_string(),
                payload: br#"{"object":"partial"}"#.to_vec(),
                idempotency_key: Some("partial-stream-event".to_string()),
            },
            CoreMutationOperation::CoreMetaPut {
                partition_id: scope_partition.to_string(),
                cf: CF_INLINE_PAYLOADS.to_string(),
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: row_key.clone(),
                payload: row_payload.clone(),
            },
        ],
    };
    store
        .admit_core_mutation(
            "mutation.batch",
            "core_control",
            CorePendingMutationTarget::MutationBatch {
                transaction_id: batch.transaction_id.clone(),
                scope_partition: batch.scope_partition.clone(),
                operation_count: batch.operations.len() as u64,
            },
            batch.transaction_id.clone(),
            Some(batch.transaction_id.clone()),
            CorePendingMutationPayload::Inline(&encode_core_mutation_batch(&batch).unwrap()),
            Vec::new(),
        )
        .await
        .unwrap();
    store
        .commit_coremeta_root_groups(
            transaction_id,
            &[CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: &row_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&row_payload),
            }],
            &[CoreMetaRootPublication::new(
                "tenant:t/bucket:b/partial-row",
                WriterFamily::CoreControl,
            )],
        )
        .await
        .unwrap();
    store.unregister_process_instance_for_tests();
    drop(store);

    let recovered = CoreStore::new(storage).await.unwrap();
    let transaction = recovered
        .read_transaction(transaction_id)
        .await
        .unwrap()
        .expect("recovered partial transaction");
    assert_eq!(transaction.state, CoreTransactionState::Committed);
    assert_eq!(transaction.visible_updates.len(), 2);
    let records = recovered
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, br#"{"object":"partial"}"#);
    assert_eq!(
        recovered
            .meta
            .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &row_key)
            .unwrap(),
        Some(row_payload)
    );
    assert!(
        read_test_pending_mutation_records(&recovered)
            .await
            .is_empty(),
        "startup recovery must checkpoint the partially materialised mutation"
    );
}

#[tokio::test]
async fn core_store_recovery_finalises_unapplied_batch_after_precondition_conflict() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let transaction_id = "recover-conflicted-unapplied-batch";
    let scope_partition = "tenant:t/bucket:b";
    let stream_id = "object_metadata:t:b:conflicted-unapplied";
    let row_key = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("conflicted-row")]).unwrap();
    let intended_payload = encode_core_meta_inline_payload_row(
        b"intended",
        core_meta_committed_row_common(
            scope_partition,
            core_meta_root_key_hash("tenant:t/bucket:b/conflicted-row"),
            1,
            transaction_id,
            1,
        ),
    )
    .unwrap();
    let conflicting_payload = encode_core_meta_inline_payload_row(
        b"existing",
        core_meta_committed_row_common(
            scope_partition,
            core_meta_root_key_hash("tenant:t/bucket:b/conflicted-row"),
            1,
            "other-transaction",
            2,
        ),
    )
    .unwrap();
    let batch = CoreMutationBatch {
        transaction_id: transaction_id.to_string(),
        scope_partition: scope_partition.to_string(),
        committed_by_principal: "principal:recovery".to_string(),
        root_publications: vec![
            mutation_root_publication(format!("stream/{stream_id}"), WriterFamily::Stream, false),
            mutation_root_publication(
                "tenant:t/bucket:b/conflicted-row",
                WriterFamily::CoreControl,
                false,
            ),
            mutation_root_publication(scope_partition, WriterFamily::CoreControl, true),
        ],
        preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
            cf: CF_INLINE_PAYLOADS.to_string(),
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key: row_key.clone(),
            expected_payload_hash: None,
            require_absent: true,
            require_present: false,
        }],
        operations: vec![
            CoreMutationOperation::StreamAppend {
                partition_id: scope_partition.to_string(),
                stream_id: stream_id.to_string(),
                record_kind: "object.put".to_string(),
                payload: br#"{"object":"must-not-append"}"#.to_vec(),
                idempotency_key: Some("conflicted-stream-event".to_string()),
            },
            CoreMutationOperation::CoreMetaPut {
                partition_id: scope_partition.to_string(),
                cf: CF_INLINE_PAYLOADS.to_string(),
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: row_key.clone(),
                payload: intended_payload,
            },
        ],
    };
    store
        .admit_core_mutation(
            "mutation.batch",
            "core_control",
            CorePendingMutationTarget::MutationBatch {
                transaction_id: batch.transaction_id.clone(),
                scope_partition: batch.scope_partition.clone(),
                operation_count: batch.operations.len() as u64,
            },
            batch.transaction_id.clone(),
            Some(batch.transaction_id.clone()),
            CorePendingMutationPayload::Inline(&encode_core_mutation_batch(&batch).unwrap()),
            Vec::new(),
        )
        .await
        .unwrap();
    store
        .commit_coremeta_root_groups(
            "other-transaction",
            &[CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: &row_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&conflicting_payload),
            }],
            &[CoreMetaRootPublication::new(
                "tenant:t/bucket:b/conflicted-row",
                WriterFamily::CoreControl,
            )],
        )
        .await
        .unwrap();
    store.unregister_process_instance_for_tests();
    drop(store);

    let recovered = CoreStore::new(storage).await.unwrap();
    let transaction = recovered
        .read_transaction(transaction_id)
        .await
        .unwrap()
        .expect("failed recovery transaction");
    assert_eq!(transaction.state, CoreTransactionState::FinalisationFailed);
    assert!(transaction.visible_updates.is_empty());
    assert!(transaction.finalisation_error.is_some());
    assert!(
        recovered
            .read_stream(ReadStream {
                stream_id: stream_id.to_string(),
                after_sequence: 0,
                limit: 10,
            })
            .await
            .unwrap()
            .is_empty(),
        "a conflicted unapplied batch must not append its stream operation"
    );
    assert_eq!(
        recovered
            .meta
            .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &row_key)
            .unwrap(),
        Some(conflicting_payload)
    );
    assert!(
        read_test_pending_mutation_records(&recovered)
            .await
            .is_empty(),
        "failed recovery must checkpoint the stale pending mutation"
    );
}

#[tokio::test]
async fn core_store_recovery_finalises_a_materialised_batch_after_its_stream_advances() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let stream_id = "object_metadata:t:b:materialised-before-finalisation";
    let batch = CoreMutationBatch {
        transaction_id: "recover-materialised-mutation-batch".to_string(),
        scope_partition: "tenant:t/bucket:b".to_string(),
        committed_by_principal: "principal:recovery".to_string(),
        root_publications: vec![
            mutation_root_publication(format!("stream/{stream_id}"), WriterFamily::Stream, false),
            mutation_root_publication("tenant:t/bucket:b", WriterFamily::CoreControl, true),
        ],
        preconditions: vec![CoreMutationPrecondition::StreamHead {
            stream_id: stream_id.to_string(),
            expected_last_sequence: 0,
            expected_last_event_hash: ZERO_HASH.to_string(),
        }],
        operations: vec![
            CoreMutationOperation::StreamAppend {
                partition_id: "tenant:t/bucket:b".to_string(),
                stream_id: stream_id.to_string(),
                record_kind: "object.put".to_string(),
                payload: br#"{"object":"first"}"#.to_vec(),
                idempotency_key: Some("materialised-first".to_string()),
            },
            CoreMutationOperation::StreamAppend {
                partition_id: "tenant:t/bucket:b".to_string(),
                stream_id: stream_id.to_string(),
                record_kind: "directory.entry".to_string(),
                payload: br#"{"directory":"first"}"#.to_vec(),
                idempotency_key: Some("materialised-directory".to_string()),
            },
        ],
    };
    store
        .admit_core_mutation(
            "mutation.batch",
            "core_control",
            CorePendingMutationTarget::MutationBatch {
                transaction_id: batch.transaction_id.clone(),
                scope_partition: batch.scope_partition.clone(),
                operation_count: batch.operations.len() as u64,
            },
            batch.transaction_id.clone(),
            Some(batch.transaction_id.clone()),
            CorePendingMutationPayload::Inline(&encode_core_mutation_batch(&batch).unwrap()),
            Vec::new(),
        )
        .await
        .unwrap();
    for operation in &batch.operations {
        let CoreMutationOperation::StreamAppend {
            partition_id,
            stream_id,
            record_kind,
            payload,
            idempotency_key,
        } = operation
        else {
            unreachable!();
        };
        store
            .append_stream_unlocked_for_principal(
                AppendStreamRecord {
                    stream_id: stream_id.clone(),
                    partition_id: partition_id.clone(),
                    record_kind: record_kind.clone(),
                    payload: payload.clone(),
                    content_type: None,
                    user_metadata_json: "{}".to_string(),
                    fence: None,
                    transaction_id: Some(batch.transaction_id.clone()),
                    idempotency_key: idempotency_key.clone(),
                },
                batch.committed_by_principal.clone(),
            )
            .await
            .unwrap();
    }
    store
        .append_stream(AppendStreamRecord {
            stream_id: stream_id.to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "object.put".to_string(),
            payload: br#"{"object":"later"}"#.to_vec(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("later-object".to_string()),
        })
        .await
        .unwrap();
    store.unregister_process_instance_for_tests();
    drop(store);

    let recovered = CoreStore::new(storage).await.unwrap();
    let transaction = recovered
        .read_transaction(&batch.transaction_id)
        .await
        .unwrap()
        .expect("recovered materialised transaction");
    assert_eq!(transaction.state, CoreTransactionState::Committed);
    assert_eq!(transaction.visible_updates.len(), 2);
    let records = recovered
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        records.len(),
        3,
        "recovery must not duplicate stream writes"
    );
    assert_eq!(records[0].record_kind, "object.put");
    assert_eq!(records[1].record_kind, "directory.entry");
    assert_eq!(records[2].payload, br#"{"object":"later"}"#);
    assert!(
        read_test_pending_mutation_records(&recovered)
            .await
            .is_empty(),
        "startup recovery must finalise the already-materialised mutation"
    );
}

#[tokio::test]
async fn core_store_admission_rejects_when_pending_mutation_hard_limit_would_be_exceeded() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let capacity_record = test_pending_mutation_record("capacity-row", 1, 1);
    let capacity_shard_hash = capacity_record.target.admission_shard().hash;
    store
        .meta
        .put(
            CF_TRANSACTIONS,
            TABLE_PENDING_MUTATION_ROW,
            &admission_record_key(&capacity_shard_hash, 1),
            &encode_stored_pending_mutation_row(&capacity_record, b"capacity-payload").unwrap(),
        )
        .unwrap();
    store
        .meta
        .put(
            CF_MATERIALISATION,
            TABLE_MATERIALISATION_CURSOR_ROW,
            &admission_sequence_key(&capacity_shard_hash),
            &encode_admission_sequence_cursor_row(&capacity_shard_hash, 1).unwrap(),
        )
        .unwrap();
    store.install_admission_point_state_for_tests().unwrap();
    let current_pending_bytes = store.pending_mutation_bytes().await.unwrap();

    let err = store
        .enforce_admission_capacity_with_limits(
            &capacity_shard_hash,
            16,
            0,
            CoreAdmissionCapacityLimits {
                pending_mutation_soft_limit_rows: 1_000_000,
                pending_mutation_hard_limit_rows: 2_000_000,
                pending_mutation_soft_limit_bytes: current_pending_bytes.saturating_add(128),
                pending_mutation_hard_limit_bytes: current_pending_bytes.saturating_add(15),
                pending_mutation_soft_lag_seconds: 60,
                pending_mutation_hard_lag_seconds: 300,
                landed_bytes_soft_limit_bytes: 1024,
                landed_bytes_hard_limit_bytes: 2048,
            },
        )
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains(AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str())
    );
}

#[tokio::test]
async fn core_store_admission_rejects_when_landed_hard_limit_would_be_exceeded() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let existing_landed = vec![0_u8; 64];
    let existing = store
        .admit_core_mutation(
            "stream.append",
            "stream",
            test_stream_append_target(
                "tenant:t/bucket:b/landed-capacity",
                "tenant:t/bucket:b",
                "event.created",
            ),
            "landed-capacity-existing".to_string(),
            None,
            CorePendingMutationPayload::Landed(&existing_landed),
            Vec::new(),
        )
        .await
        .unwrap();
    let admission_shard_hash = existing.target.admission_shard().hash;

    let err = store
        .enforce_admission_capacity_with_limits(
            &admission_shard_hash,
            0,
            64,
            CoreAdmissionCapacityLimits {
                pending_mutation_soft_limit_rows: 1_000_000,
                pending_mutation_hard_limit_rows: 2_000_000,
                pending_mutation_soft_limit_bytes: 1024,
                pending_mutation_hard_limit_bytes: 2048,
                pending_mutation_soft_lag_seconds: 60,
                pending_mutation_hard_lag_seconds: 300,
                landed_bytes_soft_limit_bytes: 96,
                landed_bytes_hard_limit_bytes: 100,
            },
        )
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains(AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str())
    );
}

#[tokio::test]
async fn core_store_admission_rejects_when_pending_mutation_materialisation_lag_is_too_old() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let old_record = test_pending_mutation_record(
        "old-lag-mutation",
        unix_timestamp_nanos().saturating_sub(301_000_000_000),
        1,
    );
    let admission_shard_hash = old_record.target.admission_shard().hash;
    write_test_pending_mutation_records(&store, vec![old_record]).await;

    let err = store
        .enforce_admission_capacity_with_limits(
            &admission_shard_hash,
            0,
            0,
            CoreAdmissionCapacityLimits {
                pending_mutation_soft_limit_rows: 1_000_000,
                pending_mutation_hard_limit_rows: 2_000_000,
                pending_mutation_soft_limit_bytes: 1024 * 1024,
                pending_mutation_hard_limit_bytes: 2 * 1024 * 1024,
                pending_mutation_soft_lag_seconds: 60,
                pending_mutation_hard_lag_seconds: 300,
                landed_bytes_soft_limit_bytes: 1024 * 1024,
                landed_bytes_hard_limit_bytes: 2 * 1024 * 1024,
            },
        )
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains(AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str())
    );
}

#[tokio::test]
async fn core_store_admission_lag_ignores_finalised_pending_mutation_records() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let record = test_pending_mutation_record(
        "old-finalised-mutation",
        unix_timestamp_nanos().saturating_sub(301_000_000_000),
        1,
    );
    write_test_pending_mutation_records(&store, vec![record.clone()]).await;
    store
        .mark_pending_mutation_finalised_unlocked(&record, "committed")
        .await
        .unwrap();

    store
        .enforce_admission_capacity_with_limits(
            &record.target.admission_shard().hash,
            0,
            0,
            CoreAdmissionCapacityLimits {
                pending_mutation_soft_limit_rows: 1_000_000,
                pending_mutation_hard_limit_rows: 2_000_000,
                pending_mutation_soft_limit_bytes: 1024 * 1024,
                pending_mutation_hard_limit_bytes: 2 * 1024 * 1024,
                pending_mutation_soft_lag_seconds: 60,
                pending_mutation_hard_lag_seconds: 300,
                landed_bytes_soft_limit_bytes: 1024 * 1024,
                landed_bytes_hard_limit_bytes: 2 * 1024 * 1024,
            },
        )
        .await
        .unwrap();
    assert!(
        read_test_pending_mutation_records(&store).await.is_empty(),
        "a fully finalised admission prefix must be checkpointed out of RocksDB metadata"
    );
    assert_eq!(
        store
            .next_core_mutation_sequence(&record.target)
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn core_store_pending_mutation_finalisation_is_idempotent_for_same_record() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let record = test_pending_mutation_record("same-finalisation", unix_timestamp_nanos(), 1);
    write_test_pending_mutation_records(&store, vec![record.clone()]).await;

    store
        .mark_pending_mutation_finalised_unlocked(&record, "committed")
        .await
        .unwrap();
    store
        .mark_pending_mutation_finalised_unlocked(&record, "committed")
        .await
        .unwrap();

    let finalisations = store
        .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
        .await
        .unwrap()
        .into_iter()
        .filter(|record| record.record_kind == CORE_PENDING_MUTATION_FINALISATION_RECORD_KIND)
        .collect::<Vec<_>>();
    assert_eq!(finalisations.len(), 1);
    assert!(
        finalisations[0].transaction_id.is_none(),
        "a finalisation event must not reuse the source mutation publication id"
    );

    let mut conflicting = record.clone();
    conflicting.mutation_id = "different-finalisation".to_string();
    assert!(
        store
            .mark_pending_mutation_finalised_unlocked(&conflicting, "committed")
            .await
            .is_err(),
        "same pending mutation node/epoch/sequence with a different mutation must fail closed"
    );
}

#[tokio::test]
async fn pending_mutation_finalisation_recovers_a_published_event_without_a_local_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let record = test_pending_mutation_record("lost-finalisation-marker", 41, 1);
    write_test_pending_mutation_records(&store, vec![record.clone()]).await;
    let published = CorePendingMutationFinalisationRecord {
        schema: CORE_PENDING_MUTATION_FINALISATION_SCHEMA.to_string(),
        node_id: record.node_id.clone(),
        mutation_epoch: record.mutation_epoch,
        mutation_sequence: record.sequence,
        mutation_id: record.mutation_id.clone(),
        operation_family: record.operation_family.clone(),
        writer_family: record.writer_family.clone(),
        target: record.target.clone(),
        boundary_values: record.boundary_values.clone(),
        landed_bytes: record.landed_bytes.clone(),
        state: "committed".to_string(),
        result: None,
        finalised_at_unix_nanos: 0,
    };
    let published = store
        .publish_pending_mutation_finalisation_transaction_record(&published)
        .await
        .unwrap();
    assert_ne!(published.finalised_at_unix_nanos, 0);
    assert!(
        store
            .read_pending_mutation_finalisation_record(&CorePendingMutationKey::from(&record))
            .unwrap()
            .is_none(),
        "the test must model a committed stream event with a lost local marker"
    );
    assert_eq!(
        store
            .read_pending_mutation_records_with_payload()
            .await
            .unwrap()
            .len(),
        1,
        "canonical publication must not remove source admission state before local finalisation"
    );

    store
        .mark_pending_mutation_finalised_unlocked(&record, "committed")
        .await
        .unwrap();

    let recovered = store
        .read_pending_mutation_finalisation_record(&CorePendingMutationKey::from(&record))
        .unwrap()
        .expect("local finalisation marker");
    assert_eq!(
        recovered.finalised_at_unix_nanos,
        published.finalised_at_unix_nanos
    );
    let finalisations = store
        .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
        .await
        .unwrap()
        .into_iter()
        .filter(|record| record.record_kind == CORE_PENDING_MUTATION_FINALISATION_RECORD_KIND)
        .count();
    assert_eq!(finalisations, 1);
    assert!(
        store
            .read_pending_mutation_records_with_payload()
            .await
            .unwrap()
            .is_empty(),
        "local finalisation must remove source admission state after canonical publication"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pending_mutation_finalisations_publish_one_contiguous_root_stream() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = Arc::new(CoreStore::new(storage).await.unwrap());
    let mut pending = Vec::new();
    for index in 0..8 {
        pending.push(
            store
                .admit_core_mutation(
                    "mutation.batch",
                    "core_control",
                    test_mutation_target(),
                    format!("concurrent-finalisation-{index}"),
                    Some(format!("concurrent-finalisation-key-{index}")),
                    CorePendingMutationPayload::Inline(b"concurrent-finalisation"),
                    Vec::new(),
                )
                .await
                .unwrap(),
        );
    }

    let barrier = Arc::new(tokio::sync::Barrier::new(pending.len()));
    let tasks = pending.into_iter().map(|record| {
        let store = store.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .mark_pending_mutation_finalised_unlocked(&record, "committed")
                .await
        })
    });
    for result in futures_util::future::join_all(tasks).await {
        result.unwrap().unwrap();
    }

    let records = store
        .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
        .await
        .unwrap();
    assert_eq!(records.len(), 8);
    assert_eq!(
        records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    assert_eq!(
        store
            .read_latest_root_anchor(core_transaction_root_anchor_key())
            .await
            .unwrap()
            .expect("concurrent finalisation root anchor")
            .root_generation,
        8
    );
}

#[tokio::test]
async fn core_store_pending_mutation_checkpoint_preserves_high_watermark_when_prefix_is_unfinalised()
 {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let first = test_pending_mutation_record("unfinalised-prefix", unix_timestamp_nanos(), 1);
    let second = test_pending_mutation_record("finalised-after-gap", unix_timestamp_nanos(), 2);
    write_test_pending_mutation_records(&store, vec![first, second.clone()]).await;
    store
        .mark_pending_mutation_finalised_unlocked(&second, "committed")
        .await
        .unwrap();

    let pending_mutation_records = read_test_pending_mutation_records(&store).await;
    assert_eq!(
        pending_mutation_records.len(),
        1,
        "checkpointing may remove independently finalised pending mutation rows once the high watermark is persisted"
    );
    assert_eq!(
        store
            .next_core_mutation_sequence(&second.target)
            .await
            .unwrap(),
        3,
        "pending mutation sequence allocation must not reuse a finalised sequence that remains after an unfinalised prefix"
    );
}
