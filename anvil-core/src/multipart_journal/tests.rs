use super::*;
use tempfile::tempdir;

const KEY: &[u8] = b"multipart journal partition owner key";

async fn create_multipart_upload(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
) -> Result<MultipartUploadMutation> {
    create_multipart_upload_inner(storage, tenant_id, bucket_id, key, 0, None, None).await
}

async fn upsert_multipart_part(
    storage: &Storage,
    upload_row_id: i64,
    part_number: i32,
    object_ref: CoreObjectRef,
    size: i64,
    etag: &str,
) -> Result<MultipartUploadPartMutation> {
    upsert_multipart_part_inner(
        storage,
        upload_row_id,
        part_number,
        object_ref,
        size,
        etag,
        None,
        None,
    )
    .await
}

async fn complete_multipart_upload(
    storage: &Storage,
    upload_row_id: i64,
) -> Result<MultipartCompletionMutation> {
    complete_multipart_upload_inner(storage, upload_row_id, None, None).await
}

async fn read_events_from_store(
    core_store: &CoreStore,
    stream_id: &str,
) -> Result<Vec<MultipartEventProto>> {
    let records = core_store
        .read_stream(crate::core_store::ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 100,
        })
        .await?;
    let mut events = Vec::new();
    for record in records {
        if record.record_kind == "multipart_metadata" {
            events.push(decode_multipart_event(&record.payload)?);
        }
    }
    Ok(events)
}

pub(crate) async fn read_multipart_frame_fences(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(core_store
        .read_stream(crate::core_store::ReadStream {
            stream_id: multipart_metadata_stream_id(tenant_id, bucket_id),
            after_sequence: 0,
            limit: 100,
        })
        .await?
        .into_iter()
        .filter(|record| record.record_kind == "multipart_metadata")
        .map(|record| decode_multipart_event_fence(&record.payload))
        .collect::<Result<Vec<_>>>()?)
}

#[test]
fn multipart_upload_indexes_share_logical_revision_without_reserving_publication_generation() {
    let upload = test_upload(7, 1, 2, "shared-generation");
    let row = MultipartUploadCurrentRow {
        upload,
        logical_revision: 9,
    };
    let mut update = MultipartCurrentRowUpdate {
        upload_row: Some(row),
        ..Default::default()
    };
    stage_active_count_update(&mut update, None, 1, 2, MultipartMutationKind::CreateUpload)
        .unwrap();
    let operations = multipart_current_row_operations(&update, "partition").unwrap();
    let payloads = operations
        .iter()
        .filter_map(|operation| match operation {
            CoreMutationOperation::CoreMetaPut {
                table_id, payload, ..
            } if *table_id == TABLE_MULTIPART_UPLOAD_CURRENT_ROW => Some(payload),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(payloads.len(), 4);
    let root_key_hash = core_meta_root_key_hash(&multipart_current_root_key(1, 2));
    assert!(payloads.iter().all(|payload| {
        core_meta_row_common_from_payload(payload)
            .map(|common| {
                common.root_key_hash == root_key_hash
                    && common.root_generation == MULTIPART_CURRENT_ROW_CANDIDATE_GENERATION
            })
            .unwrap_or(false)
    }));
    assert!(payloads[..3].iter().all(|payload| {
        MultipartUploadCurrentRowProto::decode(payload.as_slice())
            .map(|row| row.logical_revision == 9)
            .unwrap_or(false)
    }));
    assert_ne!(MULTIPART_CURRENT_ROW_CANDIDATE_GENERATION, 9);
    assert!(payloads[..3].windows(2).all(|pair| pair[0] == pair[1]));
}

#[tokio::test]
async fn multipart_current_rows_drive_upload_parts_and_state() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let upload = create_multipart_upload(&storage, 1, 2, "obj")
        .await
        .unwrap()
        .upload;
    upsert_multipart_part(
        &storage,
        upload.id,
        1,
        payload_ref("hash-a", 10),
        10,
        "etag-a",
    )
    .await
    .unwrap();
    upsert_multipart_part(
        &storage,
        upload.id,
        1,
        payload_ref("hash-b", 11),
        11,
        "etag-b",
    )
    .await
    .unwrap();
    assert_eq!(
        list_multipart_parts(&storage, upload.id).await.unwrap()[0].etag,
        "etag-b"
    );
    assert!(
        get_active_multipart_upload(&storage, 1, 2, "obj", upload.upload_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        complete_multipart_upload(&storage, upload.id)
            .await
            .unwrap()
            .completed
    );
    assert!(
        !complete_multipart_upload(&storage, upload.id)
            .await
            .unwrap()
            .completed
    );
    assert!(
        get_active_multipart_upload(&storage, 1, 2, "obj", upload.upload_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn multipart_current_rows_work_without_history_stream_records() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let upload = test_upload(7, 1, 2, "obj-no-history");
    let part = test_part(upload.id, 2, "hash-no-history", 12, "etag-no-history");

    write_current_rows_for_test(&storage, &upload, std::slice::from_ref(&part))
        .await
        .unwrap();

    let core_store = CoreStore::new(storage.clone()).await.unwrap();
    assert!(
        read_events_from_store(&core_store, &multipart_metadata_stream_id(1, 2))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        get_active_multipart_upload(&storage, 1, 2, "obj-no-history", upload.upload_id)
            .await
            .unwrap()
            .is_some()
    );
    let parts = list_multipart_parts(&storage, upload.id).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].etag, "etag-no-history");
}

#[tokio::test]
async fn multipart_part_pages_seek_one_upload_and_bound_materialization() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let upload = test_upload(7, 1, 2, "paged-parts");
    let parts = [
        test_part(upload.id, 1, "part-1", 11, "etag-1"),
        test_part(upload.id, 2, "part-2", 12, "etag-2"),
        test_part(upload.id, 3, "part-3", 13, "etag-3"),
    ];
    write_current_rows_for_test(&storage, &upload, &parts)
        .await
        .unwrap();

    // This same-table row has a mismatched physical key and would fail if the
    // page materialized another upload's part rows.
    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    let unrelated_part = MultipartPartCurrentRow {
        tenant_id: 1,
        bucket_id: 2,
        part: test_part(98, 1, "unrelated", 1, "unrelated"),
        logical_revision: 1,
    };
    let mut malformed = MultipartPartCurrentRowProto::decode(
        encode_part_current_row(&unrelated_part).unwrap().as_slice(),
    )
    .unwrap();
    malformed.part = None;
    let mut malformed_payload = Vec::new();
    malformed.encode(&mut malformed_payload).unwrap();
    meta.put(
        CF_OBJECT_HEADS,
        TABLE_MULTIPART_PART_CURRENT_ROW,
        &multipart_part_row_key(1, 2, 98, 1).unwrap(),
        &malformed_payload,
    )
    .unwrap();

    let first = list_multipart_parts_page(&storage, upload.id, 0, 1)
        .await
        .unwrap();
    assert_eq!(first.parts.len(), 1);
    assert_eq!(first.parts[0].part_number, 1);
    assert_eq!(first.next_part_number_marker, Some(1));
    let second = list_multipart_parts_page(
        &storage,
        upload.id,
        first.next_part_number_marker.unwrap(),
        1,
    )
    .await
    .unwrap();
    assert_eq!(second.parts.len(), 1);
    assert_eq!(second.parts[0].part_number, 2);
    assert_eq!(
        list_multipart_parts(&storage, upload.id)
            .await
            .unwrap()
            .len(),
        3
    );
    assert!(
        list_multipart_parts_page(&storage, upload.id, 0, 1001)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn active_upload_pages_seek_bucket_key_and_use_point_heads() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let first_upload = test_upload(11, 1, 2, "docs/a");
    let second_upload = test_upload(12, 1, 2, "docs/b");
    write_current_rows_for_test(&storage, &first_upload, &[])
        .await
        .unwrap();
    write_current_rows_for_test(&storage, &second_upload, &[])
        .await
        .unwrap();

    let mut terminal = test_upload(10, 1, 2, "docs/0");
    terminal.completed_at = Some(Utc::now());
    write_stale_active_index_for_test(&storage, &terminal)
        .await
        .unwrap();
    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    // A malformed same-table row outside this bucket proves the source prefix
    // does not visit or materialize unrelated rows.
    let unrelated_id = uuid::Uuid::new_v4();
    let unrelated_upload = MultipartUploadCurrentRow {
        upload: MultipartUpload {
            id: 99,
            tenant_id: 1,
            bucket_id: 99,
            key: "broken".to_string(),
            upload_id: unrelated_id,
            created_at: Utc::now(),
            completed_at: None,
            aborted_at: None,
        },
        logical_revision: 1,
    };
    let mut malformed = MultipartUploadCurrentRowProto::decode(
        encode_upload_current_row(&unrelated_upload)
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    malformed.upload = None;
    let mut malformed_payload = Vec::new();
    malformed.encode(&mut malformed_payload).unwrap();
    meta.put(
        CF_OBJECT_HEADS,
        TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
        &multipart_active_upload_key(99, "broken", unrelated_id).unwrap(),
        &malformed_payload,
    )
    .unwrap();

    let tombstone = list_active_multipart_uploads(&storage, 2, "docs/", "", None, 1)
        .await
        .unwrap();
    assert!(tombstone.uploads.is_empty());
    assert!(tombstone.is_truncated);
    assert_eq!(tombstone.next_key_marker.as_deref(), Some("docs/0"));
    let first = list_active_multipart_uploads(
        &storage,
        2,
        "docs/",
        tombstone.next_key_marker.as_deref().unwrap(),
        tombstone.next_upload_id_marker,
        1,
    )
    .await
    .unwrap();
    assert_eq!(first.uploads.len(), 1);
    assert_eq!(first.uploads[0].key, "docs/a");
    assert!(first.is_truncated);
    let second = list_active_multipart_uploads(
        &storage,
        2,
        "docs/",
        first.next_key_marker.as_deref().unwrap(),
        first.next_upload_id_marker,
        1,
    )
    .await
    .unwrap();
    assert_eq!(second.uploads.len(), 1);
    assert_eq!(second.uploads[0].key, "docs/b");
    assert!(!second.is_truncated);
    assert!(has_active_multipart_upload(&storage, 2).await.unwrap());
    assert_eq!(
        find_multipart_upload_partition(&storage, first_upload.id)
            .await
            .unwrap(),
        Some((1, 2))
    );
    assert!(
        get_active_multipart_upload(&storage, 1, 2, &first_upload.key, first_upload.upload_id,)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn multipart_current_rows_ignore_tampered_history_stream() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let upload = create_multipart_upload(&storage, 1, 2, "obj")
        .await
        .unwrap()
        .upload;
    upsert_multipart_part(
        &storage,
        upload.id,
        1,
        payload_ref("hash-a", 10),
        10,
        "etag-a",
    )
    .await
    .unwrap();

    let core_store = CoreStore::new(storage.clone()).await.unwrap();
    core_store
        .corrupt_stream_record_payload_for_test(&multipart_metadata_stream_id(1, 2), 1)
        .unwrap();
    assert!(
        read_events_from_store(&core_store, &multipart_metadata_stream_id(1, 2))
            .await
            .is_err()
    );

    assert!(
        get_active_multipart_upload(&storage, 1, 2, "obj", upload.upload_id)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        list_multipart_parts(&storage, upload.id).await.unwrap()[0].etag,
        "etag-a"
    );
    assert!(has_active_multipart_upload(&storage, 2).await.unwrap());
}

#[tokio::test]
async fn multipart_history_alone_does_not_create_current_state() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let upload = test_upload(1, 1, 2, "audit-only");
    append_history_only_for_test(
        &storage,
        upload.tenant_id,
        upload.bucket_id,
        MultipartMutationKind::CreateUpload,
        Some(upload.clone()),
        None,
    )
    .await
    .unwrap();

    assert!(
        get_active_multipart_upload(&storage, 1, 2, "audit-only", upload.upload_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        list_active_multipart_uploads(&storage, 2, "", "", None, 1000)
            .await
            .unwrap()
            .uploads
            .is_empty()
    );
    assert!(
        find_multipart_upload_partition(&storage, upload.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
pub(crate) async fn multipart_journal_with_permit_writes_fenced_payloads() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, 1, 2, "node-a").await;
    let permit = owner.write_permit().unwrap();

    let upload = create_multipart_upload_with_permit(&storage, 1, 2, "obj", &permit, KEY)
        .await
        .unwrap();
    upsert_multipart_part_with_permit(
        &storage,
        upload.upload.id,
        1,
        payload_ref("hash-a", 10),
        10,
        "etag-a",
        &permit,
        KEY,
    )
    .await
    .unwrap();
    complete_multipart_upload_with_permit(&storage, upload.upload.id, &permit, KEY)
        .await
        .unwrap();

    let fences = read_multipart_frame_fences(&storage, 1, 2).await.unwrap();
    assert_eq!(fences.len(), 3);
    assert!(fences.iter().all(|fence| *fence == permit.fence_token));
}

#[tokio::test]
pub(crate) async fn multipart_journal_with_permit_rejects_stale_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, 1, 2, "node-a").await;
    let stale_permit = owner.write_permit().unwrap();
    let upload = create_multipart_upload_with_permit(&storage, 1, 2, "obj", &stale_permit, KEY)
        .await
        .unwrap();
    let newer = ready_owner(&storage, 1, 2, "node-b").await;
    assert!(newer.fence_token > stale_permit.fence_token);

    let err = upsert_multipart_part_with_permit(
        &storage,
        upload.upload.id,
        1,
        payload_ref("hash-a", 10),
        10,
        "etag-a",
        &stale_permit,
        KEY,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("write permit owner is not current")
    );
}

#[tokio::test]
pub(crate) async fn multipart_journal_batch_rejects_stale_partition_precondition() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, 1, 2, "node-a").await;
    let stale_permit = owner.write_permit().unwrap();
    let stale_precondition = partition_write_precondition(&storage, &stale_permit, KEY)
        .await
        .unwrap();
    let newer = ready_owner(&storage, 1, 2, "node-b").await;
    assert!(newer.fence_token > stale_permit.fence_token);

    let err = create_multipart_upload_inner(
        &storage,
        1,
        2,
        "obj",
        stale_permit.fence_token,
        Some(stale_precondition),
        None,
    )
    .await
    .unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("generation mismatch")
            || message.contains("target mismatch")
            || message.contains("precondition failed"),
        "unexpected stale precondition error: {message}"
    );

    create_multipart_upload_with_permit(&storage, 1, 2, "obj", &newer.write_permit().unwrap(), KEY)
        .await
        .unwrap();
}

async fn ready_owner(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    owner_node_id: &str,
) -> crate::partition_fence::PartitionOwnerState {
    let family = "multipart_metadata".to_string();
    let id = hex::encode(multipart_metadata_partition_id(tenant_id, bucket_id));
    crate::partition_fence::ready_partition_owner_for_test(
        storage,
        family,
        id,
        owner_node_id,
        0,
        hex::encode([0; 32]),
        hex::encode([1; 32]),
        KEY,
    )
    .await
}

async fn write_current_rows_for_test(
    storage: &Storage,
    upload: &MultipartUpload,
    parts: &[MultipartUploadPart],
) -> Result<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let partition_id = hex::encode(multipart_metadata_partition_id(
        upload.tenant_id,
        upload.bucket_id,
    ));
    let update = multipart_current_row_update(
        &store,
        upload.tenant_id,
        upload.bucket_id,
        MultipartMutationKind::CreateUpload,
        Some(upload),
        None,
    )?;
    let operations = multipart_current_row_operations(&update, &partition_id)?;
    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("multipart-current-test:{}", uuid::Uuid::new_v4()),
            scope_partition: partition_id.clone(),
            committed_by_principal: multipart_metadata_partition_principal(
                upload.tenant_id,
                upload.bucket_id,
            ),
            root_publications: multipart_root_publications(
                multipart_current_root_key(upload.tenant_id, upload.bucket_id),
                partition_id.clone(),
            ),
            preconditions: update.preconditions,
            operations,
        })
        .await?;
    for part in parts {
        let update = multipart_current_row_update(
            &store,
            upload.tenant_id,
            upload.bucket_id,
            MultipartMutationKind::UpsertPart,
            None,
            Some(part),
        )?;
        let operations = multipart_current_row_operations(&update, &partition_id)?;
        store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: format!("multipart-current-test:{}", uuid::Uuid::new_v4()),
                scope_partition: partition_id.clone(),
                committed_by_principal: multipart_metadata_partition_principal(
                    upload.tenant_id,
                    upload.bucket_id,
                ),
                root_publications: multipart_root_publications(
                    multipart_current_root_key(upload.tenant_id, upload.bucket_id),
                    partition_id.clone(),
                ),
                preconditions: update.preconditions,
                operations,
            })
            .await?;
    }
    Ok(())
}

async fn write_stale_active_index_for_test(
    storage: &Storage,
    upload: &MultipartUpload,
) -> Result<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let transaction_id = format!("terminal-active-index:{}", uuid::Uuid::new_v4());
    let row = MultipartUploadCurrentRow {
        upload: upload.clone(),
        logical_revision: 1,
    };
    let payload = encode_upload_current_row(&row)?;
    let tuple_key = multipart_active_upload_key(upload.bucket_id, &upload.key, upload.upload_id)?;
    let op = CoreMetaBatchOp {
        cf: CF_OBJECT_HEADS,
        table_id: TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    store
        .commit_coremeta_root_groups(
            &transaction_id,
            &[op],
            &[CoreMetaRootPublication::new(
                multipart_current_root_key(upload.tenant_id, upload.bucket_id),
                WriterFamily::ObjectBlob,
            )],
        )
        .await?;
    Ok(())
}

async fn append_history_only_for_test(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    event: MultipartMutationKind,
    upload: Option<MultipartUpload>,
    part: Option<MultipartUploadPart>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = multipart_metadata_stream_id(tenant_id, bucket_id);
    let mutation_id = uuid::Uuid::new_v4();
    let payload = encode_multipart_event(event, upload.as_ref(), part.as_ref(), 0, mutation_id)?;
    let partition_id = hex::encode(multipart_metadata_partition_id(tenant_id, bucket_id));
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("multipart-history-only-test:{mutation_id}"),
            scope_partition: partition_id.clone(),
            committed_by_principal: multipart_metadata_partition_principal(tenant_id, bucket_id),
            root_publications: vec![
                CoreMutationRootPublication::new(
                    partition_id.clone(),
                    WriterFamily::CoreControl.as_str(),
                )
                .coordinator(),
            ],
            preconditions: Vec::new(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id,
                record_kind: "multipart_metadata".to_string(),
                payload,
                idempotency_key: Some(format!("multipart-history-only-test:{mutation_id}")),
            }],
        })
        .await?;
    Ok(())
}

fn test_upload(id: i64, tenant_id: i64, bucket_id: i64, key: &str) -> MultipartUpload {
    MultipartUpload {
        id,
        tenant_id,
        bucket_id,
        key: key.to_string(),
        upload_id: uuid::Uuid::new_v4(),
        created_at: Utc::now(),
        completed_at: None,
        aborted_at: None,
    }
}

fn test_part(
    upload_id: i64,
    part_number: i32,
    hash_label: &str,
    size: i64,
    etag: &str,
) -> MultipartUploadPart {
    MultipartUploadPart {
        id: i64::from(part_number),
        upload_id,
        part_number,
        content_hash: payload_ref(hash_label, size as u64).hash,
        object_ref: payload_ref(hash_label, size as u64),
        size,
        etag: etag.to_string(),
        created_at: Utc::now(),
    }
}

fn payload_ref(label: &str, logical_size: u64) -> CoreObjectRef {
    CoreObjectRef::test_unlocated(
        format!(
            "sha256:{}",
            hex::encode(blake3::hash(label.as_bytes()).as_bytes())
        ),
        logical_size,
        format!("manifest:{label}"),
    )
}
