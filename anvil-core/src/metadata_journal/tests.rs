use super::*;
use crate::core_store::{CoreStore, PutBlob};
use crate::writer_segment_catalog::read_writer_segment_catalog_record;
use chrono::Utc;
use tempfile::tempdir;

const PARTITION_OWNER_KEY: &[u8] = b"object metadata partition owner signing key";

fn sample_bucket() -> Bucket {
    Bucket {
        id: 7,
        tenant_id: 3,
        name: "journal-bucket".to_string(),
        region: "test-region".to_string(),
        created_at: Utc::now(),
        is_public_read: false,
    }
}

fn sample_object(id: i64, key: &str, delete_marker: bool) -> Object {
    Object {
        id,
        tenant_id: 3,
        bucket_id: 7,
        key: key.to_string(),
        kind: object_links::ObjectEntryKind::Blob,
        content_hash: format!("hash-{id}"),
        size: 42,
        etag: format!("etag-{id}"),
        content_type: Some("text/plain".to_string()),
        version_id: uuid::Uuid::new_v4(),
        mutation_id: uuid::Uuid::new_v4(),
        index_policy_snapshot: "snapshot".to_string(),
        user_metadata_hash: "metadata-hash".to_string(),
        authz_revision: 11,
        record_hash: format!("record-{id}"),
        created_at: Utc::now(),
        deleted_at: delete_marker.then(Utc::now),
        storage_class: None,
        user_meta: None,
        shard_map: None,
        checksum: None,
        link: None,
    }
}

async fn ready_object_metadata_permit(
    storage: &Storage,
    bucket: &Bucket,
    owner_node_id: &str,
) -> PartitionWritePermit {
    crate::partition_fence::ready_partition_owner_for_test(
        storage,
        "object_metadata".to_string(),
        hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id)),
        owner_node_id,
        0,
        hex::encode([0; 32]),
        hex::encode([1; 32]),
        PARTITION_OWNER_KEY,
    )
    .await
    .write_permit()
    .unwrap()
}

#[tokio::test]
async fn append_object_mutation_writes_one_authoritative_metadata_record() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let first = sample_object(1, "docs/a.txt", false);
    let second = sample_object(2, "docs/b.txt", true);

    append_object_mutation(&storage, &bucket, &first, ObjectJournalMutation::Put)
        .await
        .unwrap();
    append_object_mutation(
        &storage,
        &bucket,
        &second,
        ObjectJournalMutation::DeleteMarker,
    )
    .await
    .unwrap();

    let records = read_all_metadata_journal_records(&storage, &bucket)
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0].record_kind,
        ObjectMetadataRecordKind::ObjectVersion
    );
    assert_eq!(
        records[1].record_kind,
        ObjectMetadataRecordKind::DeleteMarker
    );
    assert!(records.iter().all(|record| !record.payload.is_empty()));

    let current = read_current_objects(&storage, &bucket, b"unused without manifest")
        .await
        .unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].key, first.key);
    assert_eq!(current[0].content_hash, first.content_hash);

    let current_through_directory_frame =
        read_current_objects_through_sequence(&storage, &bucket, b"unused without manifest", 2)
            .await
            .unwrap();
    assert_eq!(current_through_directory_frame.len(), 1);
    assert_eq!(
        current_through_directory_frame[0].content_hash,
        first.content_hash
    );
}

#[tokio::test]
async fn object_metadata_journal_reads_bounded_cursor_pages() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    for (id, key) in [(1, "docs/a.txt"), (2, "docs/b.txt")] {
        append_object_mutation(
            &storage,
            &bucket,
            &sample_object(id, key, false),
            ObjectJournalMutation::Put,
        )
        .await
        .unwrap();
    }

    let core_store = CoreStore::new(storage.clone()).await.unwrap();
    let stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
    let first = read_metadata_journal_records_page(&core_store, &stream_id, 0, 1)
        .await
        .unwrap();
    assert_eq!(first.records.len(), 1);
    assert_eq!(first.next_sequence, 1);
    assert!(first.has_more);
    let second =
        read_metadata_journal_records_page(&core_store, &stream_id, first.next_sequence, 1)
            .await
            .unwrap();
    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].partition_sequence, 2);
    assert!(!second.has_more);
    assert!(
        read_metadata_journal_records_page(&core_store, &stream_id, 0, 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("page size")
    );
}

#[tokio::test]
async fn object_metadata_source_checkpoint_uses_stream_metadata() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let first = sample_object(1, "docs/a.txt", false);
    let second = sample_object(2, "docs/b.txt", false);

    append_object_mutation(&storage, &bucket, &first, ObjectJournalMutation::Put)
        .await
        .unwrap();
    let first_cursor = object_metadata_source_cursor(&storage, &bucket, PARTITION_OWNER_KEY)
        .await
        .unwrap();
    assert_eq!(first_cursor, 1);
    let first_hash = object_metadata_source_checkpoint_hash(
        &storage,
        &bucket,
        PARTITION_OWNER_KEY,
        first_cursor,
    )
    .await
    .unwrap();

    append_object_mutation(&storage, &bucket, &second, ObjectJournalMutation::Put)
        .await
        .unwrap();
    let second_cursor = object_metadata_source_cursor(&storage, &bucket, PARTITION_OWNER_KEY)
        .await
        .unwrap();
    assert_eq!(second_cursor, 2);
    let second_hash = object_metadata_source_checkpoint_hash(
        &storage,
        &bucket,
        PARTITION_OWNER_KEY,
        second_cursor,
    )
    .await
    .unwrap();
    assert_ne!(first_hash, second_hash);
    assert_eq!(
        object_metadata_source_checkpoint_hash(
            &storage,
            &bucket,
            PARTITION_OWNER_KEY,
            first_cursor,
        )
        .await
        .unwrap(),
        first_hash
    );

    let stats = active_object_journal_stats(&storage, &bucket, PARTITION_OWNER_KEY)
        .await
        .unwrap();
    assert_eq!(stats.last_sequence, 2);
    assert_eq!(stats.uncompacted_frame_count, 2);
    assert!(stats.uncompacted_encoded_bytes > 0);
}

#[tokio::test]
async fn object_metadata_write_permit_sets_frame_and_manifest_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let permit = ready_object_metadata_permit(&storage, &bucket, "node-a").await;
    let object = sample_object(1, "docs/fenced.txt", false);

    append_object_mutation_with_permit(
        &storage,
        &bucket,
        &object,
        ObjectJournalMutation::Put,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    let records = read_all_metadata_journal_records(&storage, &bucket)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert!(
        records
            .iter()
            .all(|record| record.body.fence_token == permit.fence_token)
    );

    let manifest_key = b"manifest signing key";
    let sealed = seal_object_journal_segments_with_permit(
        &storage,
        &bucket,
        manifest_key,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    assert_eq!(sealed.generation, 1);
    let manifest = read_latest_partition_manifest(&storage, &bucket, manifest_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.fence_token, permit.fence_token);
}

#[tokio::test]
async fn object_metadata_write_rejects_stale_partition_permit() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let stale_permit = ready_object_metadata_permit(&storage, &bucket, "node-a").await;
    let fresh_permit = ready_object_metadata_permit(&storage, &bucket, "node-b").await;
    assert!(fresh_permit.fence_token > stale_permit.fence_token);

    let rejected = append_object_mutation_with_permit(
        &storage,
        &bucket,
        &sample_object(1, "docs/stale.txt", false),
        ObjectJournalMutation::Put,
        &stale_permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap_err();
    assert!(rejected.to_string().contains("PartitionNotOwned"));

    append_object_mutation_with_permit(
        &storage,
        &bucket,
        &sample_object(2, "docs/fresh.txt", false),
        ObjectJournalMutation::Put,
        &fresh_permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn object_metadata_corestore_batch_rejects_stale_partition_precondition() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let stale_permit = ready_object_metadata_permit(&storage, &bucket, "node-a").await;
    let stale_precondition = crate::partition_fence::partition_write_precondition(
        &storage,
        &stale_permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    let fresh_permit = ready_object_metadata_permit(&storage, &bucket, "node-b").await;
    assert!(fresh_permit.fence_token > stale_permit.fence_token);

    let rejected = append_object_mutation_inner(
        &storage,
        &bucket,
        &sample_object(1, "docs/stale-precondition.txt", false),
        ObjectJournalMutation::Put,
        stale_permit.fence_token,
        Some(stale_precondition),
        None,
        None,
    )
    .await
    .unwrap_err();
    assert!(
        rejected.to_string().contains("target mismatch")
            || rejected.to_string().contains("generation mismatch")
            || rejected.to_string().contains("precondition failed"),
        "unexpected error: {rejected:?}"
    );

    append_object_mutation_with_permit(
        &storage,
        &bucket,
        &sample_object(2, "docs/fresh-precondition.txt", false),
        ObjectJournalMutation::Put,
        &fresh_permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn object_metadata_mutation_updates_current_object_coremeta_projection() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let permit = ready_object_metadata_permit(&storage, &bucket, "node-a").await;
    let key = "docs/current-row.txt";
    let first = sample_object(1, key, false);

    append_object_mutation_with_permit(
        &storage,
        &bucket,
        &first,
        ObjectJournalMutation::Put,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    assert_eq!(
        read_current_object(&storage, &bucket, PARTITION_OWNER_KEY, key)
            .await
            .unwrap()
            .unwrap()
            .version_id,
        first.version_id
    );

    let second = sample_object(2, key, true);
    append_object_mutation_with_permit(
        &storage,
        &bucket,
        &second,
        ObjectJournalMutation::DeleteMarker,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    assert!(
        read_current_object(&storage, &bucket, PARTITION_OWNER_KEY, key)
            .await
            .unwrap()
            .is_none(),
        "current reads hide delete-marker heads; exact version reads still expose the marker"
    );
    assert_eq!(
        read_object_version(
            &storage,
            &bucket,
            PARTITION_OWNER_KEY,
            key,
            second.version_id
        )
        .await
        .unwrap()
        .unwrap()
        .version_id,
        second.version_id
    );
}

#[tokio::test]
async fn read_current_object_uses_coremeta_row() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let permit = ready_object_metadata_permit(&storage, &bucket, "node-a").await;
    let key = "docs/ref-driven-read.txt";
    let first = sample_object(1, key, false);
    let second = sample_object(2, key, false);

    append_object_mutation_with_permit(
        &storage,
        &bucket,
        &first,
        ObjectJournalMutation::Put,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    append_object_mutation_with_permit(
        &storage,
        &bucket,
        &second,
        ObjectJournalMutation::Put,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    assert_eq!(
        read_current_object(&storage, &bucket, PARTITION_OWNER_KEY, key)
            .await
            .unwrap()
            .unwrap()
            .content_hash,
        second.content_hash
    );
    assert_eq!(
        read_current_object(&storage, &bucket, PARTITION_OWNER_KEY, key)
            .await
            .unwrap()
            .unwrap()
            .content_hash,
        second.content_hash
    );
}

#[tokio::test]
async fn read_current_object_returns_none_for_current_delete_marker_ref() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let permit = ready_object_metadata_permit(&storage, &bucket, "node-a").await;
    let key = "docs/deleted-current.txt";

    append_object_mutation_with_permit(
        &storage,
        &bucket,
        &sample_object(1, key, false),
        ObjectJournalMutation::Put,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    append_object_mutation_with_permit(
        &storage,
        &bucket,
        &sample_object(2, key, true),
        ObjectJournalMutation::DeleteMarker,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();

    assert!(
        read_current_object(&storage, &bucket, PARTITION_OWNER_KEY, key)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn read_object_version_returns_exact_version_and_delete_marker() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let object = sample_object(1, "docs/a.txt", false);
    let delete_marker = sample_object(2, "docs/a.txt", true);

    append_object_mutation(&storage, &bucket, &object, ObjectJournalMutation::Put)
        .await
        .unwrap();
    append_object_mutation(
        &storage,
        &bucket,
        &delete_marker,
        ObjectJournalMutation::DeleteMarker,
    )
    .await
    .unwrap();

    let read = read_object_version(
        &storage,
        &bucket,
        b"unused without manifest",
        &object.key,
        object.version_id,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(read.version_id, object.version_id);
    assert!(read.deleted_at.is_none());

    let read_marker = read_object_version(
        &storage,
        &bucket,
        b"unused without manifest",
        &delete_marker.key,
        delete_marker.version_id,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(read_marker.version_id, delete_marker.version_id);
    assert!(read_marker.deleted_at.is_some());
}

#[tokio::test]
async fn read_object_version_hides_explicitly_deleted_version_after_seal() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let object = sample_object(1, "docs/a.txt", false);

    append_object_mutation(&storage, &bucket, &object, ObjectJournalMutation::Put)
        .await
        .unwrap();
    let signing_key = b"manifest signing key";
    seal_object_journal_segments(&storage, &bucket, signing_key)
        .await
        .unwrap();

    let before_delete = read_object_version(
        &storage,
        &bucket,
        signing_key,
        &object.key,
        object.version_id,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(before_delete.version_id, object.version_id);

    let mut deletion = object.clone();
    deletion.id = 2;
    deletion.mutation_id = uuid::Uuid::new_v4();
    deletion.deleted_at = Some(Utc::now());
    append_object_mutation(
        &storage,
        &bucket,
        &deletion,
        ObjectJournalMutation::DeleteVersion,
    )
    .await
    .unwrap();

    let after_delete = read_object_version(
        &storage,
        &bucket,
        signing_key,
        &object.key,
        object.version_id,
    )
    .await
    .unwrap();
    assert!(after_delete.is_none());
}

#[tokio::test]
async fn seal_object_journal_segments_writes_metadata_and_directory_segments() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let first = sample_object(1, "docs/a.txt", false);
    let second = sample_object(2, "docs/a.txt", false);
    let third = sample_object(3, "docs/b.txt", false);

    append_object_mutation(&storage, &bucket, &first, ObjectJournalMutation::Put)
        .await
        .unwrap();
    append_object_mutation(&storage, &bucket, &second, ObjectJournalMutation::Put)
        .await
        .unwrap();
    append_object_mutation(&storage, &bucket, &third, ObjectJournalMutation::Put)
        .await
        .unwrap();

    let signing_key = b"manifest signing key";
    let sealed = seal_object_journal_segments(&storage, &bucket, signing_key)
        .await
        .unwrap();
    assert_eq!(sealed.generation, 3);
    assert_eq!(sealed.metadata_record_count, 3);
    assert_eq!(sealed.directory_record_count, 2);
    assert_eq!(
        sealed.manifest_ref,
        metadata_manifest_ref_name(&bucket).unwrap()
    );

    let manifest = read_latest_partition_manifest(&storage, &bucket, signing_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.generation, sealed.generation);
    assert_eq!(
        manifest.manifest_hash.as_deref(),
        Some(sealed.manifest_hash.as_str())
    );
    assert_eq!(manifest.sealed_journals.len(), 1);
    assert_eq!(manifest.segments.len(), 2);
    assert_eq!(manifest.segments[0].family, "metadata_segment");
    assert_eq!(manifest.segments[1].family, "directory_segment");
    assert!(manifest.active_journal.is_none());

    let mut tampered_manifest = manifest.clone();
    tampered_manifest.generation += 1;
    assert!(verify_partition_manifest(&tampered_manifest, signing_key).is_err());

    let recovered = recover_object_metadata_partition(&storage, &bucket, signing_key)
        .await
        .unwrap();
    assert_eq!(recovered.manifest.generation, sealed.generation);
    assert_eq!(recovered.metadata_records.len(), 3);
    assert_eq!(recovered.directory_records.len(), 2);
    assert!(
        read_core_ref_uri_payload(&storage, "../escape.anseg")
            .await
            .is_err()
    );
    let current = read_current_objects(&storage, &bucket, signing_key)
        .await
        .unwrap();
    assert_eq!(current.len(), 2);
    assert_eq!(current[0].key, second.key);
    assert_eq!(current[0].version_id, second.version_id);
    let listed = list_current_objects(&storage, &bucket, signing_key, "docs/", "", 10, "/")
        .await
        .unwrap();
    assert_eq!(listed.objects.len(), 2);
    assert!(listed.common_prefixes.is_empty());
    let versions = read_object_versions(&storage, &bucket, signing_key, "docs/", "", None, 10)
        .await
        .unwrap();
    assert_eq!(versions.versions.len(), 3);
    assert_eq!(versions.versions[0].object.version_id, second.version_id);
    assert!(versions.versions[0].is_latest);
    assert_eq!(versions.versions[1].object.version_id, first.version_id);
    assert!(!versions.versions[1].is_latest);
    let next_versions = read_object_versions(
        &storage,
        &bucket,
        signing_key,
        "docs/",
        "docs/a.txt",
        Some(second.version_id),
        10,
    )
    .await
    .unwrap();
    assert_eq!(
        next_versions.versions[0].object.version_id,
        first.version_id
    );

    let metadata_bytes = read_core_ref_uri_payload(&storage, &sealed.metadata_ref)
        .await
        .unwrap();
    let metadata_records =
        decode_segment_file(&metadata_bytes, FileFamily::MetadataSegment).unwrap();
    assert_eq!(metadata_records.len(), 3);
    assert!(
        metadata_records
            .windows(2)
            .all(|pair| pair[0].key <= pair[1].key)
    );

    let directory_bytes = read_core_ref_uri_payload(&storage, &sealed.directory_ref)
        .await
        .unwrap();
    let directory_records =
        decode_segment_file(&directory_bytes, FileFamily::DirectorySegment).unwrap();
    assert_eq!(directory_records.len(), 2);
    let latest_a = decode_directory_entry_body(&directory_records[0].value).unwrap();
    assert_eq!(latest_a.version_id, second.version_id.to_string());

    let mut corrupted_metadata = read_core_ref_uri_payload(&storage, &sealed.metadata_ref)
        .await
        .unwrap();
    let body_byte = corrupted_metadata.len() - crate::formats::SEGMENT_HASH_LEN - 1;
    corrupted_metadata[body_byte] ^= 1;
    let ref_name = sealed
        .metadata_ref
        .strip_prefix(MANIFEST_SEGMENT_REF_PREFIX)
        .unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: ref_name.to_string(),
            bytes: corrupted_metadata,
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "corrupt-metadata-segment-test".to_string(),
        })
        .await
        .unwrap();
    let mut catalog = read_writer_segment_catalog_record(
        &storage,
        OBJECT_METADATA_SEGMENT_CATALOG_FAMILY,
        &object_metadata_segment_scope(ref_name).unwrap(),
        object_metadata_segment_generation(ref_name).unwrap(),
        ref_name,
    )
    .await
    .unwrap()
    .expect("metadata segment catalog row exists");
    catalog.core_object_ref_target = encode_core_object_ref_target(&object_ref).unwrap();
    crate::writer_segment_catalog::test_overwrite_writer_segment_catalog_record(&storage, &catalog)
        .unwrap();
    assert!(
        recover_object_metadata_partition(&storage, &bucket, signing_key)
            .await
            .is_err()
    );
    assert_eq!(
        read_current_objects(&storage, &bucket, signing_key)
            .await
            .unwrap()
            .len(),
        2,
        "live current-object reads are served from CoreMeta rows, not corrupted compacted segments"
    );
    let directory_listing =
        list_current_objects(&storage, &bucket, signing_key, "docs/", "", 10, "/")
            .await
            .unwrap();
    assert_eq!(
        directory_listing
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/a.txt", "docs/b.txt"]
    );
    assert_eq!(directory_listing.objects[0].version_id, second.version_id);
}

#[tokio::test]
async fn prefix_list_uses_directory_segment_plus_active_directory_journal() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let first = sample_object(1, "docs/a.txt", false);
    let second = sample_object(2, "docs/b.txt", false);
    let nested = sample_object(3, "docs/nested/c.txt", false);

    append_object_mutation(&storage, &bucket, &first, ObjectJournalMutation::Put)
        .await
        .unwrap();
    append_object_mutation(&storage, &bucket, &second, ObjectJournalMutation::Put)
        .await
        .unwrap();
    append_object_mutation(&storage, &bucket, &nested, ObjectJournalMutation::Put)
        .await
        .unwrap();

    let signing_key = b"manifest signing key";
    seal_object_journal_segments(&storage, &bucket, signing_key)
        .await
        .unwrap();

    let replacement = sample_object(4, "docs/a.txt", false);
    let delete_nested = sample_object(5, "docs/nested/c.txt", true);
    append_object_mutation(&storage, &bucket, &replacement, ObjectJournalMutation::Put)
        .await
        .unwrap();
    append_object_mutation(
        &storage,
        &bucket,
        &delete_nested,
        ObjectJournalMutation::DeleteMarker,
    )
    .await
    .unwrap();

    let listing = list_current_objects(&storage, &bucket, signing_key, "docs/", "", 10, "/")
        .await
        .unwrap();
    assert_eq!(
        listing
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/a.txt", "docs/b.txt"]
    );
    assert_eq!(listing.objects[0].version_id, replacement.version_id);
    assert_eq!(listing.objects[0].content_hash, replacement.content_hash);
    assert!(listing.common_prefixes.is_empty());

    let nested_listing =
        list_current_objects(&storage, &bucket, signing_key, "docs/nested/", "", 10, "/")
            .await
            .unwrap();
    assert!(nested_listing.objects.is_empty());
}

#[tokio::test]
async fn object_metadata_stream_rejects_corrupted_appended_frame() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let object = sample_object(1, "docs/a.txt", false);
    append_object_mutation(&storage, &bucket, &object, ObjectJournalMutation::Put)
        .await
        .unwrap();

    let stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
    CoreStore::new(storage.clone())
        .await
        .unwrap()
        .corrupt_stream_record_payload_for_test(&stream_id, 1)
        .unwrap();
    assert!(
        read_all_metadata_journal_records(&storage, &bucket)
            .await
            .is_err()
    );
}
