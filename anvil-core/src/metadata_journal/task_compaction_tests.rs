use super::*;
use crate::task_execution_guard::TaskExecutionGuard;
use crate::task_lease::{
    TaskLease, TaskLeaseAcquire, TaskLeaseOwner, acquire_task_lease, renew_task_lease,
};
use crate::writer_segment_catalog::{
    latest_writer_segment_catalog_record, read_writer_segment_catalog_record,
};
use chrono::Utc;
use tempfile::{TempDir, tempdir};

const PARTITION_OWNER_KEY: &[u8] = b"task compaction partition owner key";
const TASK_LEASE_KEY: &[u8] = b"task compaction lease signing key";
const TASK_LEASE_TTL_NANOS: i64 = 60_000_000_000;

#[tokio::test]
async fn retry_staging_is_byte_identical() {
    let (_temp, storage, bucket, _partition_precondition, first) = staged_compaction().await;
    let replay = stage_object_journal_segments(
        &storage,
        &bucket,
        b"task compaction manifest key",
        first.partition_manifest.manifest.fence_token,
    )
    .await
    .unwrap();

    assert_eq!(first.segments.len(), replay.segments.len());
    for (first_segment, replay_segment) in first.segments.iter().zip(&replay.segments) {
        assert_eq!(first_segment.catalog_record, replay_segment.catalog_record);
        assert_eq!(first_segment.file_hash, replay_segment.file_hash);
        assert_eq!(first_segment.ref_name, replay_segment.ref_name);
    }
    assert_eq!(
        first.partition_manifest.manifest,
        replay.partition_manifest.manifest
    );
    assert_eq!(
        first.partition_manifest.manifest_payload,
        replay.partition_manifest.manifest_payload
    );
    assert_eq!(
        first.partition_manifest.transaction_id,
        replay.partition_manifest.transaction_id
    );
}

#[tokio::test]
async fn stale_task_precondition_publishes_no_segment_catalog_or_head() {
    let (_temp, storage, bucket, partition_precondition, staged) = staged_compaction().await;
    let (guard, lease) = task_guard(&storage, &bucket, TASK_LEASE_TTL_NANOS).await;
    let publication_permit = guard.publication_permit().await.unwrap();

    renew_task_lease(
        &storage,
        &lease,
        now_nanos(),
        TASK_LEASE_TTL_NANOS,
        TASK_LEASE_KEY,
    )
    .await
    .unwrap();

    let segment = &staged.segments[0];
    let publication_storage = &storage;
    publication_permit
        .publish_with(|task_precondition| async move {
            let preconditions = [partition_precondition, task_precondition];
            publish_segment_catalog(publication_storage, segment, &preconditions).await
        })
        .await
        .unwrap_err();

    let record = &segment.catalog_record;
    assert!(
        read_writer_segment_catalog_record(
            &storage,
            &record.family,
            &record.scope,
            record.generation,
            &record.segment_ref,
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        latest_writer_segment_catalog_record(&storage, &record.family, &record.scope)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn expired_task_precondition_publishes_no_partition_manifest_pointer() {
    let (_temp, storage, bucket, partition_precondition, staged) = staged_compaction().await;
    let (guard, _) = task_guard(&storage, &bucket, TASK_LEASE_TTL_NANOS).await;
    let publication_permit = guard.publication_permit().await.unwrap();
    let publication_storage = &storage;
    let publication_bucket = &bucket;
    let partition_manifest = &staged.partition_manifest;
    publication_permit
        .publish_with(|mut task_precondition| async move {
            let CoreMutationPrecondition::CoreMetaLease {
                expires_at_unix_nanos,
                ..
            } = &mut task_precondition
            else {
                panic!("task guard must produce a temporal CoreMeta lease precondition");
            };
            *expires_at_unix_nanos = 1;
            let preconditions = [partition_precondition, task_precondition];
            publish_partition_manifest(
                publication_storage,
                publication_bucket,
                partition_manifest,
                &preconditions,
            )
            .await
        })
        .await
        .unwrap_err();

    for segment in &staged.segments {
        let record = &segment.catalog_record;
        assert!(
            read_writer_segment_catalog_record(
                &storage,
                &record.family,
                &record.scope,
                record.generation,
                &record.segment_ref,
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            latest_writer_segment_catalog_record(&storage, &record.family, &record.scope)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(
        read_object_metadata_partition_manifest_row(&storage, &bucket)
            .await
            .unwrap()
            .is_none()
    );
}

async fn staged_compaction() -> (
    TempDir,
    Storage,
    Bucket,
    CoreMutationPrecondition,
    StagedObjectMetadataCompaction,
) {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = sample_bucket();
    let permit = crate::partition_fence::ready_partition_owner_for_test(
        &storage,
        "object_metadata".to_string(),
        hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id)),
        "task-compaction-node",
        0,
        hex::encode([0; 32]),
        hex::encode([1; 32]),
        PARTITION_OWNER_KEY,
    )
    .await
    .write_permit()
    .unwrap();
    append_object_mutation_with_permit(
        &storage,
        &bucket,
        &sample_object(),
        ObjectJournalMutation::Put,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    let partition_precondition =
        partition_write_precondition(&storage, &permit, PARTITION_OWNER_KEY)
            .await
            .unwrap();
    let staged = stage_object_journal_segments(
        &storage,
        &bucket,
        b"task compaction manifest key",
        permit.fence_token,
    )
    .await
    .unwrap();
    (temp, storage, bucket, partition_precondition, staged)
}

async fn task_guard(
    storage: &Storage,
    bucket: &Bucket,
    ttl_nanos: i64,
) -> (TaskExecutionGuard, TaskLease) {
    let now_nanos = now_nanos();
    let lease = acquire_task_lease(
        storage,
        TaskLeaseAcquire {
            task_id: format!("object-metadata-compaction-{}", uuid::Uuid::new_v4()),
            task_kind: "OBJECT_METADATA_COMPACTION".to_string(),
            partition_family: "object_metadata".to_string(),
            partition_id: hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id)),
            owner: TaskLeaseOwner::node_instance(
                "task-compaction-node",
                format!("test-{}", uuid::Uuid::new_v4()),
            ),
            source_cursor: 1,
            now_nanos,
            ttl_nanos,
        },
        TASK_LEASE_KEY,
    )
    .await
    .unwrap();
    let guard =
        TaskExecutionGuard::new(storage.clone(), TASK_LEASE_KEY.to_vec(), lease.clone()).unwrap();
    (guard, lease)
}

fn sample_bucket() -> Bucket {
    Bucket {
        id: 41,
        tenant_id: 17,
        name: "task-compaction".to_string(),
        region: "test-region".to_string(),
        created_at: Utc::now(),
        is_public_read: false,
    }
}

fn sample_object() -> Object {
    Object {
        id: 1,
        tenant_id: 17,
        bucket_id: 41,
        key: "objects/task-fenced.txt".to_string(),
        kind: object_links::ObjectEntryKind::Blob,
        content_hash: "content-hash".to_string(),
        size: 17,
        etag: "etag".to_string(),
        content_type: Some("text/plain".to_string()),
        version_id: uuid::Uuid::new_v4(),
        mutation_id: uuid::Uuid::new_v4(),
        index_policy_snapshot: "snapshot".to_string(),
        user_metadata_hash: "metadata-hash".to_string(),
        authz_revision: 1,
        record_hash: "record-hash".to_string(),
        created_at: Utc::now(),
        deleted_at: None,
        storage_class: None,
        user_meta: None,
        shard_map: None,
        checksum: None,
        link: None,
    }
}

fn now_nanos() -> i64 {
    Utc::now()
        .timestamp_nanos_opt()
        .expect("test timestamp must fit nanoseconds")
}
