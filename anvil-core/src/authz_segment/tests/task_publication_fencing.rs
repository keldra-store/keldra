use super::super::*;
use crate::{
    core_store::CoreMutationPrecondition,
    storage::Storage,
    task_execution_guard::TaskExecutionGuard,
    task_lease::{TaskLease, TaskLeaseAcquire, TaskLeaseOwner, acquire_task_lease},
    writer_segment_catalog::WriterSegmentCatalogRecord,
};
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::sleep;

const TASK_LEASE_KEY: &[u8] = b"authz segment task lease test key";
const LONG_TASK_LEASE_TTL_NANOS: i64 = 60_000_000_000;
const EXPIRING_TASK_LEASE_TTL_NANOS: i64 = 30_000_000_000;

#[test]
fn authz_segment_timestamp_fallback_is_revision_deterministic() {
    let first = deterministic_authz_segment_timestamp(42, &[]).unwrap();
    let second = deterministic_authz_segment_timestamp(42, &[]).unwrap();

    assert_eq!(first, second);
    assert_eq!(unix_nanos_from_rfc3339(&first), 42);
}

#[tokio::test]
async fn repeated_authz_segment_staging_is_byte_identical() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let mut source_record = super::record(1, "add");
    source_record.written_at =
        chrono::DateTime::<chrono::Utc>::from_timestamp(1_720_000_000, 123_456_789).unwrap();

    let first =
        stage_authz_tuple_checkpoint_segment(&storage, 7, &[source_record.clone()], None, 1, 1, 3)
            .await
            .unwrap();
    let second = stage_authz_tuple_checkpoint_segment(&storage, 7, &[source_record], None, 1, 1, 3)
        .await
        .unwrap();

    assert_eq!(first.segment_ref, second.segment_ref);
    assert_eq!(first.catalog_record, second.catalog_record);
    assert_eq!(
        first.catalog_record.created_at_unix_nanos,
        1_720_000_000_123_456_789
    );
}

#[tokio::test]
async fn stale_task_lease_cannot_advance_authz_segment_catalog_or_head() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let staged = staged_segment(1);
    let (stale_guard, stale_lease) =
        acquire_guard(&storage, "authz-segment-stale", LONG_TASK_LEASE_TTL_NANOS).await;
    let stale_permit = stale_guard.publication_permit().await.unwrap();
    let fresh_lease = reacquire(
        &storage,
        &stale_lease,
        now_nanos(),
        LONG_TASK_LEASE_TTL_NANOS,
    )
    .await;

    stale_permit
        .publish_with(|precondition| async {
            publish_staged_authz_tuple_segment(&storage, staged.clone(), &[precondition]).await
        })
        .await
        .unwrap_err();
    assert!(
        latest_authz_tuple_segment_record(&storage, 7)
            .await
            .unwrap()
            .is_none()
    );

    let fresh_guard =
        TaskExecutionGuard::new(storage.clone(), TASK_LEASE_KEY.to_vec(), fresh_lease).unwrap();
    fresh_guard
        .publication_permit()
        .await
        .unwrap()
        .publish_with(|precondition| async {
            publish_staged_authz_tuple_segment(&storage, staged, &[precondition]).await
        })
        .await
        .unwrap();
    assert_eq!(
        latest_authz_tuple_segment_record(&storage, 7)
            .await
            .unwrap()
            .unwrap()
            .generation,
        1
    );
}

#[tokio::test]
async fn expired_task_lease_cannot_advance_authz_segment_catalog_or_head() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let staged = staged_segment(1);
    let (expired_guard, expired_lease) = acquire_guard(
        &storage,
        "authz-segment-expired",
        EXPIRING_TASK_LEASE_TTL_NANOS,
    )
    .await;
    let expired_permit = expired_guard.publication_permit().await.unwrap();
    sleep_until_lease_expires(&expired_lease).await;

    expired_permit
        .publish_with(|precondition| async {
            publish_staged_authz_tuple_segment(&storage, staged.clone(), &[precondition]).await
        })
        .await
        .unwrap_err();
    assert!(
        latest_authz_tuple_segment_record(&storage, 7)
            .await
            .unwrap()
            .is_none()
    );

    let fresh_lease = reacquire(
        &storage,
        &expired_lease,
        now_nanos(),
        LONG_TASK_LEASE_TTL_NANOS,
    )
    .await;
    let fresh_guard =
        TaskExecutionGuard::new(storage.clone(), TASK_LEASE_KEY.to_vec(), fresh_lease).unwrap();
    fresh_guard
        .publication_permit()
        .await
        .unwrap()
        .publish_with(|precondition| async {
            publish_staged_authz_tuple_segment(&storage, staged, &[precondition]).await
        })
        .await
        .unwrap();
    assert_eq!(
        latest_authz_tuple_segment_record(&storage, 7)
            .await
            .unwrap()
            .unwrap()
            .generation,
        1
    );
}

#[tokio::test]
async fn authz_segment_publication_preserves_all_caller_fences() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let staged = staged_segment(1);
    let (guard, _) = acquire_guard(
        &storage,
        "authz-segment-source-fence",
        LONG_TASK_LEASE_TTL_NANOS,
    )
    .await;
    let missing_source_fence = CoreMutationPrecondition::CoreMetaRow {
        cf: crate::core_store::CF_LEASES_FENCES.to_string(),
        table_id: crate::core_store::TABLE_PARTITION_OWNER_ROW,
        tuple_key: b"missing-authz-source-owner".to_vec(),
        expected_payload_hash: None,
        require_absent: false,
        require_present: true,
    };

    guard
        .publication_permit()
        .await
        .unwrap()
        .publish_with(|task_precondition| async {
            publish_staged_authz_tuple_segment(
                &storage,
                staged,
                &[missing_source_fence, task_precondition],
            )
            .await
        })
        .await
        .unwrap_err();
    assert!(
        latest_authz_tuple_segment_record(&storage, 7)
            .await
            .unwrap()
            .is_none()
    );
}

fn staged_segment(generation: u64) -> StagedAuthzTupleSegment {
    let segment_ref = authz_tuple_segment_ref_name(7, generation).unwrap();
    StagedAuthzTupleSegment {
        segment_ref: segment_ref.clone(),
        catalog_record: WriterSegmentCatalogRecord {
            family: AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY.to_string(),
            scope: authz_tuple_segment_scope(7).unwrap(),
            segment_ref,
            core_object_ref_target: format!("core-object-ref:authz-test-{generation}"),
            segment_hash: hex::encode([generation as u8; 32]),
            segment_length: 1,
            generation,
            source_cursor: generation,
            created_at_unix_nanos: generation,
        },
    }
}

async fn acquire_guard(
    storage: &Storage,
    task_id: &str,
    ttl_nanos: i64,
) -> (TaskExecutionGuard, TaskLease) {
    let lease = acquire_task_lease(
        storage,
        TaskLeaseAcquire {
            task_id: task_id.to_string(),
            task_kind: "authz_materialization".to_string(),
            partition_family: "authz_materialization".to_string(),
            partition_id: hex::encode([6; 32]),
            owner: TaskLeaseOwner::node_instance("node-a", "worker-a"),
            source_cursor: 1,
            now_nanos: now_nanos(),
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

async fn reacquire(
    storage: &Storage,
    previous: &TaskLease,
    now_nanos: i64,
    ttl_nanos: i64,
) -> TaskLease {
    acquire_task_lease(
        storage,
        TaskLeaseAcquire {
            task_id: previous.task_id.clone(),
            task_kind: previous.task_kind.clone(),
            partition_family: previous.partition_family.clone(),
            partition_id: previous.partition_id.clone(),
            owner: previous.owner.clone(),
            source_cursor: previous.source_cursor,
            now_nanos,
            ttl_nanos,
        },
        TASK_LEASE_KEY,
    )
    .await
    .unwrap()
}

fn now_nanos() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap()
}

async fn sleep_until_lease_expires(lease: &TaskLease) {
    let remaining = lease.expires_at_nanos.saturating_sub(now_nanos());
    let wait_nanos = remaining.max(0).saturating_add(200_000_000);
    sleep(Duration::from_nanos(u64::try_from(wait_nanos).unwrap())).await;
}
