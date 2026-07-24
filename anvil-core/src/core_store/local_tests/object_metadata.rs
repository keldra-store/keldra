use super::*;
use crate::object_links;
use crate::persistence::{Bucket, Object};

fn test_bucket() -> Bucket {
    Bucket {
        id: 23,
        tenant_id: 17,
        name: "object-metadata-pages".to_string(),
        region: "test-region".to_string(),
        created_at: chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
        is_public_read: false,
    }
}

fn test_object(id: i64, key: &str, delete_marker: bool) -> Object {
    let created_at =
        chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000 + id, 0).unwrap();
    Object {
        id,
        tenant_id: 17,
        bucket_id: 23,
        key: key.to_string(),
        kind: object_links::ObjectEntryKind::Blob,
        content_hash: format!("hash-{id}"),
        size: id,
        etag: format!("etag-{id}"),
        content_type: Some("application/octet-stream".to_string()),
        version_id: uuid::Uuid::from_u128(id as u128),
        mutation_id: uuid::Uuid::from_u128(10_000 + id as u128),
        index_policy_snapshot: "test-policy".to_string(),
        user_metadata_hash: format!("metadata-{id}"),
        authz_revision: 1,
        record_hash: format!("record-{id}"),
        created_at,
        deleted_at: delete_marker.then_some(created_at),
        storage_class: None,
        user_meta: None,
        shard_map: None,
        checksum: None,
        link: None,
    }
}

#[tokio::test]
async fn object_metadata_points_pages_and_delete_replacement_share_generations() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let bucket = test_bucket();
    let first = test_object(1, "docs/report.txt", false);
    let marker = test_object(2, "docs/report.txt", true);
    let latest = test_object(3, "docs/report.txt", false);

    for object in [&first, &marker, &latest] {
        store.put_object_metadata(&bucket, object).await.unwrap();
    }

    assert_eq!(
        store
            .read_current_object_metadata(&bucket, &latest.key)
            .await
            .unwrap()
            .unwrap()
            .version_id,
        latest.version_id
    );
    assert_eq!(
        store
            .read_object_version_metadata_by_id(&bucket, marker.version_id)
            .await
            .unwrap()
            .unwrap()
            .version_id,
        marker.version_id
    );
    assert_eq!(
        store
            .read_current_object_metadata_at_generation(&bucket, &latest.key, 1)
            .await
            .unwrap()
            .unwrap()
            .version_id,
        first.version_id
    );
    assert!(
        store
            .read_current_object_metadata_at_generation(&bucket, &latest.key, 2)
            .await
            .unwrap()
            .is_none()
    );

    let mut deletion = latest.clone();
    deletion.id = 4;
    deletion.mutation_id = uuid::Uuid::from_u128(10_004);
    deletion.deleted_at = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_004, 0);
    store
        .delete_object_version_metadata(&bucket, &deletion)
        .await
        .unwrap();

    assert!(
        store
            .read_current_object_metadata(&bucket, &latest.key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .read_object_version_metadata_by_id(&bucket, latest.version_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .read_object_version_metadata_at_generation(&bucket, &latest.key, latest.version_id, 4,)
            .await
            .unwrap()
            .is_none()
    );

    let current_page = store
        .list_current_object_metadata_page(&bucket, "docs/", "", None, None, 10)
        .await
        .unwrap();
    assert!(current_page.objects.is_empty());
    assert_eq!(current_page.source_generation, 4);

    let versions = store
        .list_object_versions_metadata_page(&bucket, "docs/", "", None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(
        versions
            .versions
            .iter()
            .map(|version| version.object.version_id)
            .collect::<Vec<_>>(),
        vec![marker.version_id, first.version_id]
    );
    assert!(versions.versions[0].is_latest);
    assert!(versions.versions[0].is_delete_marker);
    assert!(
        versions.versions[1..]
            .iter()
            .all(|version| !version.is_latest)
    );

    assert_eq!(store.next_object_metadata_id(&bucket).await.unwrap(), 5);
}

#[tokio::test]
async fn stale_object_ids_still_publish_one_new_root_generation() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let bucket = test_bucket();
    let first = test_object(1, "alpha", false);
    let mut stale = test_object(1, "beta", false);
    stale.version_id = uuid::Uuid::from_u128(101);
    stale.mutation_id = uuid::Uuid::from_u128(10_101);

    store.put_object_metadata(&bucket, &first).await.unwrap();
    store.put_object_metadata(&bucket, &stale).await.unwrap();

    let latest = store
        .list_current_object_metadata_page(&bucket, "", "", None, None, 10)
        .await
        .unwrap();
    assert_eq!(latest.source_generation, 2);
    assert_eq!(
        latest
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );
    let historical = store
        .list_current_object_metadata_page(&bucket, "", "", Some(1), None, 10)
        .await
        .unwrap();
    assert_eq!(historical.objects.len(), 1);
    assert_eq!(historical.objects[0].key, "alpha");
    assert_eq!(store.next_object_metadata_id(&bucket).await.unwrap(), 2);
}

#[tokio::test]
async fn ordered_object_pages_seek_and_bind_cursors_to_their_source() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let bucket = test_bucket();
    for object in [
        test_object(1, "alpha", false),
        test_object(2, "alpha/one", false),
        test_object(3, "alpha/two", false),
        test_object(4, "beta", false),
    ] {
        store.put_object_metadata(&bucket, &object).await.unwrap();
    }
    assert!(
        store
            .list_current_object_metadata_page(&bucket, "", "", None, None, 0)
            .await
            .is_err()
    );
    assert!(
        store
            .list_object_versions_metadata_page(&bucket, "", "", None, None, None, 4_096)
            .await
            .is_err()
    );

    let first = store
        .list_current_object_metadata_page(&bucket, "alpha", "", None, None, 2)
        .await
        .unwrap();
    assert_eq!(
        first
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "alpha/one"]
    );
    assert_eq!(first.candidates_visited, 2);
    let second = store
        .list_current_object_metadata_page(
            &bucket,
            "alpha",
            "",
            None,
            first.next_cursor.as_ref(),
            2,
        )
        .await
        .unwrap();
    assert_eq!(
        second
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha/two"]
    );
    assert!(second.next_cursor.is_none());

    let after_exact = store
        .list_current_object_metadata_page(&bucket, "", "alpha", None, None, 1)
        .await
        .unwrap();
    assert_eq!(after_exact.objects[0].key, "alpha/one");
    let after_directory = after_exact
        .next_cursor
        .as_ref()
        .unwrap()
        .after_current_prefix(&bucket, "alpha/")
        .unwrap();
    let after_directory_page = store
        .list_current_object_metadata_page(&bucket, "", "alpha", None, Some(&after_directory), 1)
        .await
        .unwrap();
    assert_eq!(after_directory_page.objects[0].key, "beta");

    let latest_first = store
        .list_current_object_metadata_page(&bucket, "alpha", "", None, None, 1)
        .await
        .unwrap();
    let scope_error = store
        .list_current_object_metadata_page(
            &bucket,
            "alpha/",
            "",
            None,
            latest_first.next_cursor.as_ref(),
            1,
        )
        .await
        .unwrap_err();
    assert!(
        scope_error
            .to_string()
            .contains("ObjectMetadataPageCursorSourceMismatch")
    );
    let historical_first = store
        .list_current_object_metadata_page(&bucket, "alpha", "", Some(4), None, 1)
        .await
        .unwrap();
    store
        .put_object_metadata(&bucket, &test_object(5, "zeta", false))
        .await
        .unwrap();

    let latest_error = store
        .list_current_object_metadata_page(
            &bucket,
            "alpha",
            "",
            None,
            latest_first.next_cursor.as_ref(),
            1,
        )
        .await
        .unwrap_err();
    assert!(
        latest_error
            .to_string()
            .contains("ObjectMetadataPageCursorSourceMismatch")
    );
    let historical_second = store
        .list_current_object_metadata_page(
            &bucket,
            "alpha",
            "",
            Some(4),
            historical_first.next_cursor.as_ref(),
            1,
        )
        .await
        .unwrap();
    assert_eq!(historical_second.objects[0].key, "alpha/one");
    assert_eq!(historical_second.source_generation, 4);
}

#[tokio::test]
async fn ordered_version_pages_preserve_markers_and_sort_order() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let bucket = test_bucket();
    let first = test_object(1, "docs/a.txt", false);
    let mut second = test_object(1, "docs/a.txt", false);
    second.version_id = uuid::Uuid::from_u128(102);
    second.mutation_id = uuid::Uuid::from_u128(10_102);
    let other = test_object(2, "docs/b.txt", false);
    for object in [&first, &second, &other] {
        store.put_object_metadata(&bucket, object).await.unwrap();
    }

    let page = store
        .list_object_versions_metadata_page(&bucket, "docs/", "", None, None, None, 2)
        .await
        .unwrap();
    assert_eq!(
        page.versions
            .iter()
            .map(|version| version.object.version_id)
            .collect::<Vec<_>>(),
        vec![second.version_id, first.version_id]
    );
    assert!(page.versions[0].is_latest);
    assert!(!page.versions[1].is_latest);

    let continued = store
        .list_object_versions_metadata_page(
            &bucket,
            "docs/",
            "",
            None,
            None,
            page.next_cursor.as_ref(),
            2,
        )
        .await
        .unwrap();
    assert_eq!(continued.versions[0].object.version_id, other.version_id);
    assert!(continued.versions[0].is_latest);

    let after_marker = store
        .list_object_versions_metadata_page(
            &bucket,
            "docs/",
            &second.key,
            Some(second.version_id),
            None,
            None,
            10,
        )
        .await
        .unwrap();
    assert_eq!(
        after_marker
            .versions
            .iter()
            .map(|version| version.object.version_id)
            .collect::<Vec<_>>(),
        vec![first.version_id, other.version_id]
    );

    let missing_marker = store
        .list_object_versions_metadata_page(
            &bucket,
            "docs/",
            "before-prefix",
            Some(uuid::Uuid::from_u128(999)),
            None,
            None,
            10,
        )
        .await
        .unwrap();
    assert!(missing_marker.versions.is_empty());
    assert!(missing_marker.next_cursor.is_none());
}
