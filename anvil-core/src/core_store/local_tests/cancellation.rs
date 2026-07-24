use super::*;
use std::time::Duration;

#[tokio::test]
async fn named_advisory_locks_ignore_unlocked_files_left_by_crashed_processes() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let stale_lock = storage
        .core_store_staging_path()
        .join("locks")
        .join("stream")
        .join("stale.lock");
    std::fs::create_dir_all(stale_lock.parent().unwrap()).unwrap();
    std::fs::write(&stale_lock, []).unwrap();

    let store = CoreStore::new(storage).await.unwrap();

    assert!(stale_lock.exists());
    let stale_guard = store.acquire_named_lock("stream", "stale").await.unwrap();
    drop(stale_guard);
    let live_lock = store.acquire_named_lock("stream", "live").await.unwrap();
    drop(live_lock);
}

#[tokio::test]
async fn named_lock_contenders_are_serialized_without_deleting_the_lock_file() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let first = store
        .acquire_named_lock("stream", "contended")
        .await
        .unwrap();
    let contender_store = CoreStore::new(storage).await.unwrap();
    let contender = tokio::spawn(async move {
        contender_store
            .acquire_named_lock("stream", "contended")
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!contender.is_finished());
    drop(first);
    let second = tokio::time::timeout(Duration::from_secs(2), contender)
        .await
        .expect("the contender should acquire after release")
        .expect("the contender task should not panic")
        .expect("the contender lock should succeed");
    let path = second.path.clone();
    drop(second);
    assert!(path.exists());
}

#[tokio::test]
async fn explicit_transaction_lifecycle_does_not_take_the_startup_recovery_lock() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let root = "tenant:t/bucket:explicit-lock-order";
    let principal = "principal:explicit-lock-order";
    let startup_guard = store.startup_recovery_lock.lock().await;
    let transaction_store = store.clone();
    let lifecycle = tokio::spawn(async move {
        let transaction = transaction_store
            .begin_explicit_transaction(CoreBeginTransaction {
                idempotency_key: "explicit-lock-order".to_string(),
                root_anchor_key: root.to_string(),
                root_key_hash: CoreStore::root_key_hash_for_anchor(root),
                scope_partition: root.to_string(),
                ttl_ms: 60_000,
                purpose: "verify scoped explicit transaction locks".to_string(),
                principal: principal.to_string(),
                preconditions_hash: ZERO_HASH.to_string(),
            })
            .await?;
        transaction_store
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
                    stream_id: "explicit-lock-order-stream".to_string(),
                    record_kind: "test.explicit-lock-order".to_string(),
                    payload: b"lock-order".to_vec(),
                    idempotency_key: Some("explicit-lock-order-event".to_string()),
                }],
            })
            .await?;
        transaction_store
            .commit_explicit_transaction(&transaction.transaction_id, principal)
            .await
    });

    let committed = tokio::time::timeout(Duration::from_secs(10), lifecycle)
        .await
        .expect("an explicit transaction must not wait for the startup recovery lock")
        .expect("explicit transaction task should not panic")
        .expect("explicit transaction should commit");
    assert_eq!(committed.state, CoreTransactionState::Committed);
    drop(startup_guard);
}

#[tokio::test]
async fn independent_stream_append_does_not_take_the_startup_recovery_lock() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let startup_guard = store.startup_recovery_lock.lock().await;
    let append_store = store.clone();
    let append = tokio::spawn(async move {
        append_store
            .append_stream(AppendStreamRecord {
                stream_id: "independent-stream".to_string(),
                partition_id: "independent-partition".to_string(),
                record_kind: "test.append".to_string(),
                payload: b"independent".to_vec(),
                content_type: None,
                user_metadata_json: "{}".to_string(),
                fence: None,
                transaction_id: None,
                idempotency_key: Some("independent-stream-append".to_string()),
            })
            .await
    });

    let receipt = tokio::time::timeout(Duration::from_secs(5), append)
        .await
        .expect("an independent stream append must not wait for the startup recovery lock")
        .expect("append task should not panic")
        .expect("append should succeed");
    assert_eq!(receipt.sequence, 1);
    drop(startup_guard);
}

#[tokio::test]
async fn admitted_mutation_finalisation_survives_caller_cancellation() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let transaction_id = "cancelled-caller-mutation";
    let stream_id = "object_metadata:t:b:cancelled-caller";
    let blocker = store
        .acquire_named_lock("stream", CORE_TRANSACTION_STREAM_ID)
        .await
        .unwrap();
    let commit_store = store.clone();
    let commit = tokio::spawn(async move {
        commit_store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: transaction_id.to_string(),
                scope_partition: "tenant:t/bucket:b".to_string(),
                committed_by_principal: "principal:cancellation-test".to_string(),
                root_publications: vec![
                    CoreMutationRootPublication::new(
                        "tenant:t/bucket:b",
                        WriterFamily::CoreControl.as_str(),
                    )
                    .coordinator(),
                ],
                preconditions: Vec::new(),
                operations: vec![CoreMutationOperation::StreamAppend {
                    partition_id: "tenant:t/bucket:b".to_string(),
                    stream_id: stream_id.to_string(),
                    record_kind: "object.put".to_string(),
                    payload: br#"{"object":"cancelled-caller"}"#.to_vec(),
                    idempotency_key: Some("cancelled-caller-event".to_string()),
                }],
            })
            .await
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !read_test_pending_mutation_records(&store).await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("mutation should reach its durable admission boundary");

    commit.abort();
    let _ = commit.await;
    drop(blocker);

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let transaction = store.read_transaction(transaction_id).await.unwrap();
            if transaction
                .as_ref()
                .is_some_and(|transaction| transaction.state == CoreTransactionState::Committed)
                && read_test_pending_mutation_records(&store).await.is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("detached finalisation should commit and checkpoint the admitted mutation");

    let records = store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
}
