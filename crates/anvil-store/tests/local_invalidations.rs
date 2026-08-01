use anvil_store::{
    BatchOperation, DeleteRequest, InvalidationStateHint, ObjectKey, Precondition, PutRequest,
    Store, StoreOptions,
};

fn key(path: &str) -> ObjectKey {
    ObjectKey::new("tenant", "bucket", path).unwrap()
}

fn put(path: &str, bytes: &[u8], command_id: &str) -> PutRequest {
    PutRequest {
        key: key(path),
        bytes: bytes.to_vec(),
        content_type: Some("application/octet-stream".into()),
        precondition: Precondition::Absent,
        command_id: Some(command_id.into()),
        durability_class: "test-default".into(),
    }
}

#[tokio::test]
async fn put_delete_and_replay_keep_one_durable_invalidation_per_head_change() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    assert_eq!(store.local_invalidation_offset().unwrap(), 0);

    let put_request = put("record", b"value", "put-record");
    let created = store.put(put_request.clone()).await.unwrap();
    let delete_request = DeleteRequest {
        key: key("record"),
        precondition: Precondition::Version(created.version),
        command_id: Some("delete-record".into()),
        durability_class: "test-default".into(),
    };
    let deleted = store.delete(delete_request.clone()).await.unwrap();

    assert_eq!(store.local_invalidation_offset().unwrap(), 2);
    let invalidations = store.scan_local_invalidations(0, 10).unwrap();
    assert_eq!(invalidations.len(), 2);
    assert_eq!(invalidations[0].offset, 1);
    assert_eq!(invalidations[0].key, key("record"));
    assert_eq!(invalidations[0].minimum_path_version, created.version);
    assert_eq!(invalidations[0].state_hint, InvalidationStateHint::Present);
    assert_eq!(invalidations[1].offset, 2);
    assert_eq!(invalidations[1].minimum_path_version, deleted.version);
    assert_eq!(invalidations[1].state_hint, InvalidationStateHint::Deleted);
    assert_eq!(
        store.read_local_invalidation(2).unwrap(),
        Some(invalidations[1].clone())
    );
    assert!(store.read_local_invalidation(3).unwrap().is_none());

    drop(store);
    let reopened = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    assert_eq!(reopened.local_invalidation_offset().unwrap(), 2);
    assert!(reopened.put(put_request).await.unwrap().replayed);
    assert!(reopened.delete(delete_request).await.unwrap().replayed);
    assert_eq!(reopened.local_invalidation_offset().unwrap(), 2);
    assert_eq!(reopened.scan_local_invalidations(0, 10).unwrap().len(), 2);
}

#[tokio::test]
async fn bulk_appends_only_successful_head_changes_and_bounds_scans() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let first = put("a", b"first", "first");
    let conflicting = put("a", b"conflict", "conflict");
    let second = put("b", b"second", "second");

    let outcomes = store
        .bulk_write(vec![
            BatchOperation::Put(first.clone()),
            BatchOperation::Put(conflicting),
            BatchOperation::Put(second.clone()),
        ])
        .await;
    assert!(outcomes[0].result.is_ok());
    assert!(outcomes[1].result.is_err());
    assert!(outcomes[2].result.is_ok());
    assert_eq!(store.local_invalidation_offset().unwrap(), 2);

    let first_page = store.scan_local_invalidations(0, 1).unwrap();
    assert_eq!(first_page.len(), 1);
    assert_eq!(first_page[0].offset, 1);
    assert_eq!(first_page[0].key, key("a"));
    let second_page = store.scan_local_invalidations(1, 10).unwrap();
    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0].offset, 2);
    assert_eq!(second_page[0].key, key("b"));

    let replay = store
        .bulk_write(vec![
            BatchOperation::Put(first),
            BatchOperation::Put(second),
        ])
        .await;
    assert!(replay.iter().all(|outcome| {
        outcome
            .result
            .as_ref()
            .is_ok_and(|receipt| receipt.replayed)
    }));
    assert_eq!(store.local_invalidation_offset().unwrap(), 2);
    assert_eq!(store.scan_local_invalidations(0, 10).unwrap().len(), 2);
}
