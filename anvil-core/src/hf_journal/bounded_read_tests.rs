use super::*;
use tempfile::tempdir;

async fn ingestion(storage: &Storage, tenant_id: i64, bucket: &str, prefix: &str) -> i64 {
    create_ingestion(
        storage,
        1,
        tenant_id,
        2,
        "owner/repo",
        None,
        bucket,
        "region",
        Some(prefix),
        &[],
        &[],
    )
    .await
    .unwrap()
}

async fn stored_item(storage: &Storage, ingestion_id: i64, path: &str, size: i64) -> i64 {
    let item_id = add_item(storage, ingestion_id, path, None, None)
        .await
        .unwrap();
    update_item_success(storage, item_id, size, &format!("etag-{size}"))
        .await
        .unwrap();
    item_id
}

#[tokio::test]
async fn stored_ingestion_item_pages_are_bounded_and_cursor_exact() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let ingestion_id = ingestion(&storage, 11, "bucket", "target").await;

    for index in 0..7 {
        stored_item(&storage, ingestion_id, &format!("item-{index:03}"), index).await;
    }

    let first = list_stored_ingestion_item_page(&storage, ingestion_id, None, 3)
        .await
        .unwrap();
    assert_eq!(first.items.len(), 3);
    assert_eq!(first.items[0].path, "item-000");
    assert_eq!(first.items[2].path, "item-002");

    let second =
        list_stored_ingestion_item_page(&storage, ingestion_id, first.next_cursor.as_deref(), 3)
            .await
            .unwrap();
    assert_eq!(second.items.len(), 3);
    assert_eq!(second.items[0].path, "item-003");
    assert_eq!(second.items[2].path, "item-005");

    let third =
        list_stored_ingestion_item_page(&storage, ingestion_id, second.next_cursor.as_deref(), 3)
            .await
            .unwrap();
    assert_eq!(third.items.len(), 1);
    assert_eq!(third.items[0].path, "item-006");
    assert!(third.next_cursor.is_none());

    let other_ingestion = ingestion(&storage, 11, "bucket", "other").await;
    let error =
        list_stored_ingestion_item_page(&storage, other_ingestion, first.next_cursor.as_deref(), 3)
            .await
            .unwrap_err();
    assert!(error.to_string().contains("outside the requested prefix"));
}

#[tokio::test]
async fn target_pages_ignore_unrelated_cardinality_and_preserve_item_order() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();

    let unrelated = ingestion(&storage, 99, "noise", "prefix").await;
    for index in 0..48 {
        stored_item(&storage, unrelated, &format!("noise-{index:03}"), index).await;
    }

    let first_ingestion = ingestion(&storage, 11, "bucket", "target").await;
    stored_item(&storage, first_ingestion, "shared", 1).await;
    stored_item(&storage, first_ingestion, "first-only", 2).await;
    let second_ingestion = ingestion(&storage, 11, "bucket", "target").await;
    stored_item(&storage, second_ingestion, "shared", 3).await;

    let first = list_stored_target_item_page(&storage, 11, "bucket", "target", None, 2)
        .await
        .unwrap();
    assert_eq!(first.items.len(), 2);
    assert_eq!(first.items[0].path, "shared");
    assert_eq!(first.items[0].size, Some(1));
    assert_eq!(first.items[1].path, "first-only");

    let second = list_stored_target_item_page(
        &storage,
        11,
        "bucket",
        "target",
        first.next_cursor.as_deref(),
        2,
    )
    .await
    .unwrap();
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].path, "shared");
    assert_eq!(second.items[0].size, Some(3));
    assert!(second.next_cursor.is_none());
}

#[tokio::test]
async fn ingestion_status_is_a_cardinality_independent_point_projection() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let ingestion_id = ingestion(&storage, 11, "bucket", "target").await;

    let mut item_ids = Vec::new();
    for index in 0..64 {
        item_ids.push(
            add_item(
                &storage,
                ingestion_id,
                &format!("item-{index:03}"),
                None,
                None,
            )
            .await
            .unwrap(),
        );
    }

    update_item_state_inner(
        &storage,
        item_ids[0],
        crate::tasks::HFIngestionItemState::Downloading,
        None,
        HfWriteGuard::default(),
    )
    .await
    .unwrap();
    update_item_success(&storage, item_ids[1], 10, "stored")
        .await
        .unwrap();
    update_item_state_inner(
        &storage,
        item_ids[2],
        crate::tasks::HFIngestionItemState::Failed,
        Some("failed"),
        HfWriteGuard::default(),
    )
    .await
    .unwrap();
    update_item_state_inner(
        &storage,
        item_ids[3],
        crate::tasks::HFIngestionItemState::Skipped,
        None,
        HfWriteGuard::default(),
    )
    .await
    .unwrap();

    let status = get_ingestion_status(&storage, ingestion_id).await.unwrap();
    assert_eq!(status.queued, 60);
    assert_eq!(status.downloading, 1);
    assert_eq!(status.stored, 1);
    assert_eq!(status.failed, 1);

    update_item_success(&storage, item_ids[0], 11, "stored-later")
        .await
        .unwrap();
    update_item_success(&storage, item_ids[0], 11, "stored-again")
        .await
        .unwrap();
    let status = get_ingestion_status(&storage, ingestion_id).await.unwrap();
    assert_eq!(status.queued, 60);
    assert_eq!(status.downloading, 0);
    assert_eq!(status.stored, 2);
    assert_eq!(status.failed, 1);

    let page = list_stored_ingestion_item_page(&storage, ingestion_id, None, 1)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert!(page.next_cursor.is_some());
}
