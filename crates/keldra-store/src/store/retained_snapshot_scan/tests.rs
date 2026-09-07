use crate::{
    Durability, OBJECT_ALIAS_REGISTRY_FORMAT, ObjectAliasRegistry, ObjectKey, PutMode, PutRequest,
    StoreOptions,
};

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
    assert!(records.iter().all(|record| record.user_retained));
}

#[tokio::test]
async fn logical_scan_joins_alias_registry_from_the_held_snapshot() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    store
        .enable_bucket_versioning("tenant", "bucket")
        .await
        .unwrap();
    store
        .put(put("targets/item", &[1; 10], "put-target-one"))
        .await
        .unwrap();
    store
        .put(put("targets/item", &[2; 20], "put-target-two"))
        .await
        .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let identity = BucketIdentity {
        tenant_id: TenantId(tenant_id),
        bucket_id: BucketId(bucket_id),
    };
    let registry = ObjectAliasRegistry {
        format: OBJECT_ALIAS_REGISTRY_FORMAT,
        revision: 1,
        aliases: vec!["docs/alias".into()],
        program_commit_cursor: Some(1),
    };
    let alias_key = identity.head_key("targets/item");
    store
        .db
        .put_cf(
            store.cf(CF_OBJECT_ALIAS_REGISTRIES).unwrap(),
            &alias_key,
            registry.canonical_bytes().unwrap(),
        )
        .unwrap();

    let mut alias_only = store
        .start_logical_retained_object_snapshot_scan(
            tenant_id,
            bucket_id,
            "docs",
            10,
            1024 * 1024,
            |record| {
                !record.version.protected_link_descriptor
                    && (record.user_retained || record.version.id == record.current_head.version)
            },
            |path| path_is_within_prefix(path, "docs"),
        )
        .await
        .unwrap();
    // The registry is part of the already-held snapshot, not a later join.
    store
        .db
        .delete_cf(store.cf(CF_OBJECT_ALIAS_REGISTRIES).unwrap(), &alias_key)
        .unwrap();
    let alias_records = alias_only.next_frame().await.unwrap().unwrap().records;
    assert!(alias_only.next_frame().await.unwrap().is_none());
    assert_eq!(alias_records.len(), 2);
    assert!(
        alias_records
            .iter()
            .all(|record| record.exact_path == "targets/item" && record.scope_name_count == 1)
    );
    assert_eq!(
        alias_records
            .iter()
            .map(|record| record.version.blob.as_ref().unwrap().length)
            .sum::<u64>(),
        30
    );

    store
        .db
        .put_cf(
            store.cf(CF_OBJECT_ALIAS_REGISTRIES).unwrap(),
            &alias_key,
            registry.canonical_bytes().unwrap(),
        )
        .unwrap();
    let mut whole_bucket = store
        .start_logical_retained_object_snapshot_scan(
            tenant_id,
            bucket_id,
            "",
            10,
            1024 * 1024,
            |record| {
                !record.version.protected_link_descriptor
                    && (record.user_retained || record.version.id == record.current_head.version)
            },
            |_| true,
        )
        .await
        .unwrap();
    let whole_records = whole_bucket.next_frame().await.unwrap().unwrap().records;
    assert!(whole_bucket.next_frame().await.unwrap().is_none());
    assert_eq!(whole_records.len(), 2);
    assert!(
        whole_records
            .iter()
            .all(|record| record.scope_name_count == 2)
    );
}

#[tokio::test]
async fn unversioned_overwrite_keeps_checkpoint_retained_descriptors_reachable() {
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
    assert_eq!(frame.records.len(), 2);
    assert_eq!(frame.records[1].version.id, current.version);
    assert!(
        frame
            .records
            .iter()
            .all(|record| record.current_head.version == current.version)
    );
    assert!(scan.next_frame().await.unwrap().is_none());
    assert!(frame.records.iter().all(|record| !record.user_retained));
}

#[tokio::test]
async fn logical_filter_skips_oversized_transient_history_before_frame_accounting() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let mut first_request = put("docs/a", b"one", "put-one");
    first_request.content_type = Some("x".repeat(crate::MAX_CONTENT_TYPE_BYTES));
    let first = store.put(first_request).await.unwrap();
    let second = store.put(put("docs/a", b"two", "put-two")).await.unwrap();
    store
        .enable_bucket_versioning("tenant", "bucket")
        .await
        .unwrap();
    let third = store
        .put(put("docs/a", b"three", "put-three"))
        .await
        .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let mut all = store
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
    let records = all.next_frame().await.unwrap().unwrap().records;
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].version.id, first.version);
    assert_eq!(records[1].version.id, second.version);
    assert_eq!(records[2].version.id, third.version);
    assert!(!records[0].user_retained);
    assert!(records[1..].iter().all(|record| record.user_retained));
    let selected_max_bytes = records[1..]
        .iter()
        .map(|record| serde_json::to_vec(record).unwrap().len() as u64)
        .max()
        .unwrap();
    assert!(serde_json::to_vec(&records[0]).unwrap().len() as u64 > selected_max_bytes);

    let mut logical = store
        .start_retained_object_snapshot_scan(
            tenant_id,
            bucket_id,
            "docs/a",
            1,
            selected_max_bytes,
            |record| record.user_retained || record.version.id == record.current_head.version,
        )
        .await
        .unwrap();
    let first_frame = logical.next_frame().await.unwrap().unwrap();
    assert_eq!(first_frame.records[0].version.id, second.version);
    let second_frame = logical.next_frame().await.unwrap().unwrap();
    assert_eq!(second_frame.records[0].version.id, third.version);
    assert!(logical.next_frame().await.unwrap().is_none());
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
    let path = "_keldra/index-projections/v1/0000000000000000000000000000000000000000000000000000000000000001/partitions/7/0202020202020202020202020202020202020202020202020202020202020202/3/4/current";
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
            "_keldra/index-projections/v1/0000000000000000000000000000000000000000000000000000000000000001/partitions/7/0202020202020202020202020202020202020202020202020202020202020202/3/5/current",
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
            "_keldra/index-projections/v1/0000000000000000000000000000000000000000000000000000000000000001/partitions/7/0202020202020202020202020202020202020202020202020202020202020202/3/4/",
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
            "_keldra/index-projections/v1/0000000000000000000000000000000000000000000000000000000000000001/partitions/7/0202020202020202020202020202020202020202020202020202020202020202/3/4/",
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
            "_keldra/index-projections/v1/0000000000000000000000000000000000000000000000000000000000000001/partitions/7/0202020202020202020202020202020202020202020202020202020202020202/3/3/",
            Some(&decoded),
            1,
            1024 * 1024,
        ),
        Err(ObjectSnapshotError::InvalidCursor)
    ));
}
