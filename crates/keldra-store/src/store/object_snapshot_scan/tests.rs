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
    let tail = store.local_watch_status().unwrap().tail;
    store
        .advance_source_journal_reference_safe_through(tail)
        .await
        .unwrap();
    store
        .advance_source_journal_settled_through(tail)
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
async fn exact_current_snapshot_never_decodes_retained_history() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    assert!(
        store
            .enable_bucket_versioning("tenant", "bucket")
            .await
            .unwrap()
    );
    for version in 1..=256 {
        put_at(
            &store,
            "deep/history",
            format!("value-{version}").as_bytes(),
            &format!("put-{version}"),
            version,
        )
        .await;
    }
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let complete = store
        .export_object_path_record(tenant_id, bucket_id, "deep/history")
        .unwrap()
        .unwrap();
    assert_eq!(complete.versions.len(), 256);
    let expected = complete.versions.last().unwrap().clone();

    // A malformed historical descriptor makes the complete-record export
    // fail. The exact-current read still succeeds because it performs only a
    // head lookup followed by the one descriptor named by that head.
    let identity = stable_identity(tenant_id, bucket_id);
    let head_key = identity.head_key("deep/history");
    store
        .db
        .put_cf(
            store.cf(CF_VERSIONS).unwrap(),
            exact_version_key(&head_key, complete.versions[0].id),
            b"not-json",
        )
        .unwrap();
    assert!(
        store
            .export_object_path_record(tenant_id, bucket_id, "deep/history")
            .is_err()
    );
    let current = store
        .export_current_object_snapshot(tenant_id, bucket_id, "deep/history")
        .unwrap()
        .unwrap();
    assert_eq!(current.exact_path, "deep/history");
    assert_eq!(current.version, expected);
    assert_eq!(current.head.version, expected.id);
}

#[tokio::test]
async fn exact_current_snapshot_batch_preserves_current_overwritten_deleted_and_request_order() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    put_at(&store, "docs/current", b"current", "put-current", 1).await;
    put_at(&store, "docs/overwritten", b"old", "put-old", 2).await;
    put_at(&store, "docs/deleted", b"deleted", "put-deleted", 3).await;
    put_at(&store, "docs/overwritten", b"new", "put-new", 4).await;
    store
        .coordinate_object_mutation(
            BatchOperation::Delete(delete("docs/deleted", "delete-current")),
            context(5),
        )
        .await
        .unwrap();

    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let paths = vec![
        "docs/deleted".to_owned(),
        "docs/current".to_owned(),
        "docs/missing".to_owned(),
        "docs/overwritten".to_owned(),
        "docs/current".to_owned(),
    ];
    let snapshots = store
        .export_current_object_snapshots(tenant_id, bucket_id, &paths)
        .unwrap();

    assert_eq!(snapshots.len(), paths.len());
    let deleted = snapshots[0].as_ref().unwrap();
    assert_eq!(deleted.exact_path, "docs/deleted");
    assert!(deleted.head.deleted);
    assert!(deleted.version.deleted);
    assert!(deleted.version.blob.is_none());

    let current = snapshots[1].as_ref().unwrap();
    assert_eq!(current.exact_path, "docs/current");
    assert_eq!(
        current.version.blob.as_ref().unwrap().hash,
        *blake3::hash(b"current").as_bytes()
    );
    assert!(snapshots[2].is_none());

    let overwritten = snapshots[3].as_ref().unwrap();
    assert_eq!(overwritten.exact_path, "docs/overwritten");
    assert_eq!(
        overwritten.version.blob.as_ref().unwrap().hash,
        *blake3::hash(b"new").as_bytes()
    );
    assert_eq!(snapshots[4], snapshots[1]);
}

#[tokio::test]
async fn ordinary_definition_guard_blocks_only_its_exact_path() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    put_at(&store, "definitions/one", b"v1", "one-v1", 1).await;
    put_at(&store, "definitions/two", b"v1", "two-v1", 2).await;

    let guarded_key = ObjectKey::new("tenant", "bucket", "definitions/one").unwrap();
    let guarded_store = store.clone();
    let (entered_sender, entered_receiver) = tokio::sync::oneshot::channel();
    let (release_sender, release_receiver) = tokio::sync::oneshot::channel();
    let guard = tokio::spawn(async move {
        guarded_store
            .with_ordinary_object_path_lock(&guarded_key, || async move {
                entered_sender.send(()).unwrap();
                release_receiver.await.unwrap();
            })
            .await;
    });
    entered_receiver.await.unwrap();

    let same_store = store.clone();
    let mut same_path = tokio::spawn(async move {
        put_at(&same_store, "definitions/one", b"v2", "one-v2", 3).await;
    });
    let other_store = store.clone();
    let other_path = tokio::spawn(async move {
        put_at(&other_store, "definitions/two", b"v2", "two-v2", 4).await;
    });
    let delete_store = store.clone();
    let mut same_delete = tokio::spawn(async move {
        delete_store
            .coordinate_object_mutation(
                BatchOperation::Delete(delete("definitions/one", "one-delete")),
                context(5),
            )
            .await
            .unwrap();
    });

    tokio::time::timeout(std::time::Duration::from_secs(1), other_path)
        .await
        .expect("an unrelated definition must not share the guard")
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut same_path)
            .await
            .is_err(),
        "the ordinary update must use the exact same path lock as the guard"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut same_delete)
            .await
            .is_err(),
        "the ordinary delete must use the exact same path lock as the guard"
    );
    release_sender.send(()).unwrap();
    guard.await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), same_path)
        .await
        .expect("the same-path update must resume after guard release")
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), same_delete)
        .await
        .expect("the same-path delete must resume after guard release")
        .unwrap();
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
        .start_current_head_snapshot_scan(
            tenant_id,
            bucket_id,
            "docs/",
            None,
            1,
            1024 * 1024,
            |_| true,
        )
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
    assert_eq!(scan.heads_visited(), 3);
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
async fn snapshot_waits_for_the_proof_backed_tail_without_holding_the_commit_lock() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    store
        .coordinate_object_mutation(
            BatchOperation::Put(put("docs/pending", b"pending", "pending")),
            context(1),
        )
        .await
        .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let status = store.local_watch_status().unwrap();
    assert!(status.settled_through < status.tail);

    let scanning = store.clone();
    let mut scan = tokio::spawn(async move {
        scanning
            .start_current_head_snapshot_scan(
                tenant_id,
                bucket_id,
                "docs/",
                None,
                1,
                1024 * 1024,
                |_| true,
            )
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut scan)
            .await
            .is_err()
    );

    store
        .advance_source_journal_settled_through(status.tail)
        .await
        .unwrap();
    let mut scan = tokio::time::timeout(std::time::Duration::from_secs(1), scan)
        .await
        .expect("snapshot resumes after settlement")
        .unwrap()
        .unwrap();
    assert_eq!(scan.captured_tail(), status.tail);
    assert_eq!(scan.next_frame().await.unwrap().unwrap().heads.len(), 1);
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
        .start_current_head_snapshot_scan(
            tenant_id,
            bucket_id,
            "docs/",
            None,
            1,
            1024 * 1024,
            |head| head.exact_path.ends_with("/b"),
        )
        .await
        .unwrap();
    let frame = scan.next_frame().await.unwrap().unwrap();
    assert_eq!(frame.heads.len(), 1);
    assert_eq!(frame.heads[0].exact_path, "docs/b");
    assert!(scan.next_frame().await.unwrap().is_none());
    assert_eq!(scan.heads_visited(), 2);
}

#[tokio::test]
async fn current_head_snapshot_resumes_exclusively_after_canonical_path() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    for (ordinal, path) in ["docs/a", "docs/b", "docs/c"].into_iter().enumerate() {
        put_at(
            &store,
            path,
            path.as_bytes(),
            &format!("put-{path}"),
            ordinal as u64 + 1,
        )
        .await;
    }
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let mut scan = store
        .start_current_head_snapshot_scan(
            tenant_id,
            bucket_id,
            "docs/",
            Some("docs/b"),
            10,
            1024 * 1024,
            |_| true,
        )
        .await
        .unwrap();
    let frame = scan.next_frame().await.unwrap().unwrap();
    assert_eq!(
        frame
            .heads
            .iter()
            .map(|head| head.exact_path.as_str())
            .collect::<Vec<_>>(),
        ["docs/c"]
    );
    assert!(scan.next_frame().await.unwrap().is_none());
}

#[tokio::test]
async fn enlarged_internal_snapshot_limit_remains_strictly_byte_bounded() {
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
    let max_bytes = (0..8)
        .map(|index| {
            let head = store
                .export_current_object_snapshot(tenant_id, bucket_id, &format!("docs/{index}"))
                .unwrap()
                .unwrap();
            encoded_record_bytes(&head).unwrap()
        })
        .max()
        .unwrap();
    let mut scan = store
        .start_current_head_snapshot_scan(
            tenant_id,
            bucket_id,
            "docs/",
            None,
            crate::MAX_CURRENT_HEAD_SNAPSHOT_RECORDS,
            max_bytes,
            |_| true,
        )
        .await
        .unwrap();
    let mut count = 0;
    while let Some(frame) = scan.next_frame().await.unwrap() {
        let frame_bytes = frame.heads.iter().try_fold(0_u64, |total, head| {
            total.checked_add(encoded_record_bytes(head).unwrap())
        });
        assert!(frame_bytes.unwrap() <= max_bytes);
        count += frame.heads.len();
    }
    assert_eq!(count, 8);
}

#[test]
fn internal_snapshot_record_cap_is_distinct_from_public_export_pages() {
    let above_public = crate::MAX_OBJECT_RECORD_EXPORT_RECORDS + 1;
    assert!(validate_scan_request(1, 2, "docs/", above_public, 1024).is_err());
    assert!(validate_snapshot_scan_request(1, 2, "docs/", above_public, 1024).is_ok());
    assert!(
        validate_snapshot_scan_request(
            1,
            2,
            "docs/",
            crate::MAX_CURRENT_HEAD_SNAPSHOT_RECORDS + 1,
            1024,
        )
        .is_err()
    );
    assert!(
        validate_snapshot_scan_request(
            1,
            2,
            "docs/",
            1,
            crate::store::object_snapshot_scan::MAX_CURRENT_HEAD_SNAPSHOT_BYTES + 1,
        )
        .is_err()
    );
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
            None,
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
