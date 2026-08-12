use crate::{Durability, ObjectKey, PutMode, PutRequest, StoreOptions};

use super::*;

fn put(path: &str, bytes: &[u8], command: &str) -> PutRequest {
    PutRequest {
        key: ObjectKey::new("tenant", "bucket", path).unwrap(),
        bytes: bytes.to_vec(),
        content_type: Some("application/octet-stream".into()),
        mode: PutMode::Put,
        command_id: Some(command.into()),
        durability: Durability::Local,
    }
}

#[tokio::test]
async fn one_path_with_many_versions_streams_in_bounded_path_version_order() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    store
        .enable_bucket_versioning("tenant", "bucket")
        .await
        .unwrap();
    let first = store.put(put("docs/a", b"one", "put-one")).await.unwrap();
    let second = store.put(put("docs/a", b"two", "put-two")).await.unwrap();
    store
        .put(put("docs-old/adjacent", b"skip", "put-adjacent"))
        .await
        .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let captured_status = store.local_watch_status().unwrap();
    let mut scan = store
        .start_retained_object_snapshot_scan(tenant_id, bucket_id, "docs", 1, 1024 * 1024, |_| true)
        .await
        .unwrap();
    assert_eq!(scan.source(), captured_status.source_id);
    assert_eq!(scan.captured_tail(), captured_status.tail);

    // The held snapshot remains on the first two versions while later writes
    // continue through the ordinary path.
    store
        .put(put("docs/a", b"three", "put-three"))
        .await
        .unwrap();

    let mut records = Vec::new();
    while let Some(frame) = scan.next_frame().await.unwrap() {
        assert_eq!(frame.records.len(), 1);
        assert_eq!(frame.through.exact_path, frame.records[0].exact_path);
        assert_eq!(frame.through.version, frame.records[0].version.id);
        records.extend(frame.records);
    }
    assert_eq!(records.len(), 2);
    assert_eq!(
        records
            .iter()
            .map(|record| record.version.id)
            .collect::<Vec<_>>(),
        [first.version, second.version]
    );
    assert!(records.iter().all(|record| record.exact_path == "docs/a"));
    assert!(
        records
            .iter()
            .all(|record| record.current_head.version == second.version)
    );
}

#[tokio::test]
async fn unversioned_overwrite_exposes_only_the_current_retained_descriptor() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    store.put(put("docs/a", b"one", "put-one")).await.unwrap();
    let current = store.put(put("docs/a", b"two", "put-two")).await.unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let mut scan = store
        .start_retained_object_snapshot_scan(
            tenant_id,
            bucket_id,
            "docs/a",
            10,
            1024 * 1024,
            |_| true,
        )
        .await
        .unwrap();
    let frame = scan.next_frame().await.unwrap().unwrap();
    assert_eq!(frame.records.len(), 1);
    assert_eq!(frame.records[0].version.id, current.version);
    assert_eq!(frame.records[0].current_head.version, current.version);
    assert!(scan.next_frame().await.unwrap().is_none());
}

#[tokio::test]
async fn retained_frames_reject_one_record_larger_than_the_byte_budget() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    store.put(put("docs/a", b"one", "put-one")).await.unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let mut scan = store
        .start_retained_object_snapshot_scan(tenant_id, bucket_id, "docs/a", 1, 1, |_| true)
        .await
        .unwrap();
    assert!(matches!(
        scan.next_frame().await,
        Err(ObjectSnapshotError::ExportRecordTooLarge { .. })
    ));
}

#[tokio::test]
async fn owner_filter_runs_before_frame_byte_accounting() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    store.put(put("docs/a", b"one", "put-one")).await.unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let mut scan = store
        .start_retained_object_snapshot_scan(tenant_id, bucket_id, "docs", 1, 1, |_| false)
        .await
        .unwrap();
    assert!(scan.next_frame().await.unwrap().is_none());
}

#[tokio::test]
async fn retained_prefix_page_seeks_past_many_unrelated_versions() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    store
        .enable_bucket_versioning("tenant", "bucket")
        .await
        .unwrap();
    for item in 0..256 {
        store
            .put(put(
                &format!("000-unrelated/{item:04}"),
                b"unrelated",
                &format!("put-unrelated-{item}"),
            ))
            .await
            .unwrap();
    }
    let path = "_anvil/indexes/v3/9/current";
    let first = store
        .put(put(path, b"one", "put-target-one"))
        .await
        .unwrap();
    let second = store
        .put(put(path, b"two", "put-target-two"))
        .await
        .unwrap();
    store
        .put(put(
            "_anvil/indexes/v3/90/current",
            b"adjacent",
            "put-adjacent-index",
        ))
        .await
        .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();

    let first_page = store
        .export_retained_objects_by_prefix(
            tenant_id,
            bucket_id,
            "_anvil/indexes/v3/9/",
            None,
            1,
            1024 * 1024,
        )
        .unwrap();
    assert_eq!(first_page.records.len(), 1);
    assert_eq!(first_page.records[0].exact_path, path);
    assert_eq!(first_page.records[0].version.id, first.version);
    let cursor = first_page.next_cursor.unwrap();

    let second_page = store
        .export_retained_objects_by_prefix(
            tenant_id,
            bucket_id,
            "_anvil/indexes/v3/9/",
            Some(&cursor),
            1,
            1024 * 1024,
        )
        .unwrap();
    assert_eq!(second_page.records.len(), 1);
    assert_eq!(second_page.records[0].exact_path, path);
    assert_eq!(second_page.records[0].version.id, second.version);
    assert!(second_page.next_cursor.is_none());

    let token = cursor.as_token().to_owned();
    let decoded = RetainedObjectCursor::from_token(token).unwrap();
    assert!(matches!(
        store.export_retained_objects_by_prefix(
            tenant_id,
            bucket_id,
            "_anvil/indexes/v3/8/",
            Some(&decoded),
            1,
            1024 * 1024,
        ),
        Err(ObjectSnapshotError::InvalidCursor)
    ));
}
