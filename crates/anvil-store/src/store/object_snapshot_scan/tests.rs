use crate::{
    BatchOperation, DeleteRequest, Durability, ObjectKey, ObjectMutationContext, PlacementLogId,
    Precondition, PutMode, PutRequest, StoreOptions,
};

use super::*;

fn put(path: &str, value: &[u8], command_id: &str) -> PutRequest {
    PutRequest {
        key: ObjectKey::new("tenant", "bucket", path).unwrap(),
        bytes: value.to_vec(),
        content_type: Some("application/octet-stream".into()),
        mode: PutMode::Put,
        command_id: Some(command_id.into()),
        durability: Durability::Local,
    }
}

fn delete(path: &str, command_id: &str) -> DeleteRequest {
    DeleteRequest {
        key: ObjectKey::new("tenant", "bucket", path).unwrap(),
        precondition: Precondition::Any,
        command_id: Some(command_id.into()),
        durability: Durability::Local,
    }
}

fn context(index: u64) -> ObjectMutationContext {
    ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 7, index },
        serving_fence_term: 7,
    }
}

async fn put_at(store: &Store, path: &str, value: &[u8], command: &str, index: u64) {
    store
        .coordinate_object_mutation(
            BatchOperation::Put(put(path, value, command)),
            context(index),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn stable_prefix_pages_are_sorted_bounded_and_scope_the_cursor() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    put_at(&store, "docs/c", b"c", "put-c", 1).await;
    put_at(&store, "other/a", b"other", "put-other", 2).await;
    put_at(&store, "docs/a", b"a", "put-a", 3).await;
    put_at(&store, "docs/b", b"b", "put-b", 4).await;
    put_at(&store, "docs2/a", b"adjacent", "put-adjacent", 5).await;
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();

    let mut cursor = None;
    let mut paths = Vec::new();
    loop {
        let page = store
            .export_current_heads_by_prefix(
                tenant_id,
                bucket_id,
                "docs",
                cursor.as_ref(),
                1,
                1024 * 1024,
            )
            .unwrap();
        paths.extend(page.heads.into_iter().map(|head| head.exact_path));
        let Some(next) = page.next_cursor else {
            break;
        };
        assert!(crate::ObjectRecordCursor::from_token(next.as_token()).is_ok());
        cursor = Some(next);
    }
    assert_eq!(paths, ["docs/a", "docs/b", "docs/c"]);

    let all = store
        .export_all_current_heads(None, 10, 1024 * 1024)
        .unwrap();
    assert_eq!(
        all.heads
            .iter()
            .map(|head| head.exact_path.as_str())
            .collect::<Vec<_>>(),
        ["docs/a", "docs/b", "docs/c", "docs2/a", "other/a"]
    );
    assert!(all.next_cursor.is_none());

    assert_eq!(
        store
            .export_current_heads_by_prefix(
                tenant_id,
                bucket_id,
                "other/",
                cursor.as_ref(),
                1,
                1024 * 1024,
            )
            .unwrap_err(),
        ObjectSnapshotError::InvalidCursor
    );
}

#[tokio::test]
async fn one_snapshot_binds_heads_and_tail_across_concurrent_put_delete_and_epoch_change() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    put_at(&store, "docs/a", b"a-one", "put-a", 1).await;
    put_at(&store, "docs/b", b"b-one", "put-b", 2).await;
    put_at(&store, "docs/c", b"c-one", "put-c", 3).await;
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let before = store.local_watch_status().unwrap();
    let expected_a = store
        .export_current_heads_by_prefix(tenant_id, bucket_id, "docs/a", None, 1, 1024 * 1024)
        .unwrap()
        .heads
        .pop()
        .unwrap()
        .version;

    // The credit-driven cursor remains bound to its original RocksDB snapshot
    // while later commits proceed; it decodes no frame until `next_frame`.
    let mut scan = store
        .start_current_head_snapshot_scan(tenant_id, bucket_id, "docs/", 1, 1024 * 1024, |_| true)
        .await
        .unwrap();
    assert_eq!(scan.source(), before.source_id);
    assert_eq!(scan.captured_tail(), before.tail);

    put_at(&store, "docs/a", b"a-two-new", "put-a-two", 4).await;
    store
        .coordinate_object_mutation(
            BatchOperation::Delete(delete("docs/b", "delete-b")),
            context(5),
        )
        .await
        .unwrap();
    put_at(&store, "docs/d", b"d-one", "put-d", 6).await;

    let replacement_epoch = [9_u8; 32];
    store
        .db
        .put_cf(
            store.cf(CF_METADATA).unwrap(),
            LOCAL_INVALIDATION_EPOCH_KEY,
            replacement_epoch,
        )
        .unwrap();

    let mut captured = Vec::new();
    while let Some(frame) = scan.next_frame().await.unwrap() {
        captured.extend(frame.heads);
    }
    assert_eq!(
        captured
            .iter()
            .map(|head| head.exact_path.as_str())
            .collect::<Vec<_>>(),
        ["docs/a", "docs/b", "docs/c"]
    );
    assert_eq!(
        captured
            .iter()
            .find(|head| head.exact_path == "docs/a")
            .unwrap()
            .version,
        expected_a
    );
    assert_eq!(scan.source(), before.source_id);
    assert_eq!(scan.captured_tail(), before.tail);
    assert!(store.local_watch_status().unwrap().tail > before.tail);
}

#[tokio::test]
async fn snapshot_filter_runs_before_bounded_frames_are_emitted() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    put_at(&store, "docs/a", b"a", "put-a", 1).await;
    put_at(&store, "docs/b", b"b", "put-b", 2).await;
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let mut scan = store
        .start_current_head_snapshot_scan(tenant_id, bucket_id, "docs/", 1, 1024 * 1024, |head| {
            head.exact_path.ends_with("/b")
        })
        .await
        .unwrap();
    let frame = scan.next_frame().await.unwrap().unwrap();
    assert_eq!(frame.heads.len(), 1);
    assert_eq!(frame.heads[0].exact_path, "docs/b");
    assert!(scan.next_frame().await.unwrap().is_none());
}

#[tokio::test]
async fn dropping_a_broken_stream_stops_the_snapshot_worker() {
    struct DropNotice(std::sync::mpsc::Sender<()>);

    impl Drop for DropNotice {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    for index in 0..8 {
        put_at(
            &store,
            &format!("docs/{index}"),
            b"value",
            &format!("put-{index}"),
            index + 1,
        )
        .await;
    }
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let (notice_sender, notice_receiver) = std::sync::mpsc::channel();
    let notice = DropNotice(notice_sender);
    let scan = store
        .start_current_head_snapshot_scan(
            tenant_id,
            bucket_id,
            "docs/",
            1,
            1024 * 1024,
            move |_| {
                let _keep_alive = &notice;
                true
            },
        )
        .await
        .unwrap();
    drop(scan);
    notice_receiver
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("dropping the receiver must release the scan closure and snapshot");
}
