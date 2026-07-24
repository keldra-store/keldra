use super::*;

async fn append_test_event(store: &CoreStore, stream_id: &str, ordinal: u64) {
    store
        .append_stream(AppendStreamRecord {
            stream_id: stream_id.to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "test.event".to_string(),
            payload: ordinal.to_be_bytes().to_vec(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!("{stream_id}:{ordinal}")),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn stream_catalog_and_watch_pages_seek_to_the_requested_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    for stream_id in ["audit:alpha", "audit:beta", "other:unrelated"] {
        append_test_event(&store, stream_id, 1).await;
        append_test_event(&store, stream_id, 2).await;
    }

    let first_ids = store.list_stream_ids_page("audit:", None, 1).await.unwrap();
    assert_eq!(first_ids, vec!["audit:alpha"]);
    let second_ids = store
        .list_stream_ids_page("audit:", first_ids.last().map(String::as_str), 1)
        .await
        .unwrap();
    assert_eq!(second_ids, vec!["audit:beta"]);
    assert!(
        store
            .list_stream_ids_page("audit:", second_ids.last().map(String::as_str), 1)
            .await
            .unwrap()
            .is_empty()
    );

    let first = store
        .watch(WatchRequest {
            stream_prefix: "audit:".to_string(),
            after_cursor: None,
            limit: 2,
        })
        .await
        .unwrap();
    assert_eq!(first.events.len(), 2);
    assert!(
        first
            .events
            .iter()
            .all(|event| event.stream_id == "audit:alpha")
    );
    assert!(first.has_more);

    let second = store
        .watch(WatchRequest {
            stream_prefix: "audit:".to_string(),
            after_cursor: first.next_cursor,
            limit: 2,
        })
        .await
        .unwrap();
    assert_eq!(second.events.len(), 2);
    assert!(
        second
            .events
            .iter()
            .all(|event| event.stream_id == "audit:beta")
    );

    let end = store
        .watch(WatchRequest {
            stream_prefix: "audit:".to_string(),
            after_cursor: second.next_cursor,
            limit: 2,
        })
        .await
        .unwrap();
    assert!(end.events.is_empty());
    assert!(!end.has_more);
}

#[tokio::test]
async fn stream_pages_reject_cursors_outside_the_requested_prefix() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    append_test_event(&store, "audit:alpha", 1).await;

    assert!(
        store
            .list_stream_ids_page("audit:", Some("other:stream"), 10)
            .await
            .is_err()
    );
    assert!(
        store
            .watch(WatchRequest {
                stream_prefix: "audit:".to_string(),
                after_cursor: Some("other:stream:00000000000000000001".to_string()),
                limit: 10,
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn exact_stream_pages_advance_by_durable_source_sequence() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    for ordinal in 1..=3 {
        append_test_event(&store, "audit:exact", ordinal).await;
    }
    append_test_event(&store, "audit:exact-suffix", 1).await;

    let first = store
        .read_stream_page(ReadStream {
            stream_id: "audit:exact".to_string(),
            after_sequence: 0,
            limit: 2,
        })
        .await
        .unwrap();
    assert_eq!(
        first
            .records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(first.next_sequence, 2);
    assert!(first.has_more);

    let second = store
        .read_stream_page(ReadStream {
            stream_id: "audit:exact".to_string(),
            after_sequence: first.next_sequence,
            limit: 2,
        })
        .await
        .unwrap();
    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].sequence, 3);
    assert_eq!(second.next_sequence, 3);
    assert!(!second.has_more);
}

#[tokio::test]
async fn durable_stream_append_wakes_scope_subscribers() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let observer_storage = Storage::new_at(tmp.path()).await.unwrap();
    let mut notifications = observer_storage.subscribe_stream("audit:wake");
    let store = CoreStore::new(storage).await.unwrap();

    append_test_event(&store, "audit:wake", 1).await;

    tokio::time::timeout(std::time::Duration::from_secs(1), notifications.recv())
        .await
        .expect("stream append should wake subscriber")
        .expect("stream notifier should remain open");
}

#[tokio::test]
async fn watch_does_not_advance_past_a_transaction_that_can_still_commit() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let root = "tenant:t/bucket:watch-transaction";
    let principal = "principal:watch-transaction";
    let transaction = store
        .begin_explicit_transaction(CoreBeginTransaction {
            idempotency_key: "watch-transaction".to_string(),
            root_anchor_key: root.to_string(),
            root_key_hash: CoreStore::root_key_hash_for_anchor(root),
            scope_partition: root.to_string(),
            ttl_ms: 60_000,
            purpose: "verify watch transaction visibility".to_string(),
            principal: principal.to_string(),
            preconditions_hash: ZERO_HASH.to_string(),
        })
        .await
        .unwrap();
    store
        .stage_explicit_transaction_batch(CoreMutationBatch {
            transaction_id: transaction.transaction_id.clone(),
            scope_partition: root.to_string(),
            committed_by_principal: principal.to_string(),
            root_publications: vec![
                CoreMutationRootPublication::new(root, WriterFamily::CoreControl.as_str())
                    .coordinator(),
            ],
            preconditions: Vec::new(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id: root.to_string(),
                stream_id: "watch:pending".to_string(),
                record_kind: "test.pending".to_string(),
                payload: b"pending".to_vec(),
                idempotency_key: Some("watch-pending-event".to_string()),
            }],
        })
        .await
        .unwrap();

    let pending = store
        .watch(WatchRequest {
            stream_prefix: "watch:".to_string(),
            after_cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert!(pending.events.is_empty());
    assert!(pending.next_cursor.is_none());
    assert!(pending.has_more);

    let mut notifications = store.storage.subscribe_stream("watch:pending");
    store
        .commit_explicit_transaction(&transaction.transaction_id, principal)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), notifications.recv())
        .await
        .expect("transaction commit should wake stream subscriber")
        .expect("stream notifier should remain open");
    let committed = store
        .watch(WatchRequest {
            stream_prefix: "watch:".to_string(),
            after_cursor: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(committed.events.len(), 1);
    assert_eq!(committed.events[0].stream_id, "watch:pending");
}
