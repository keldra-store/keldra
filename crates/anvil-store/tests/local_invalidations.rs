use anvil_authz::ObjectRef;
use anvil_store::{
    AuthzRevision, BatchOperation, CreateBucketRequest, DeleteRequest, Durability,
    ObjectHeadChangeKind, ObjectKey, ObjectVersioning, Precondition, ProvisionTenantRequest,
    PutMode, PutRequest, StorageTenantId, Store, StoreOptions, SystemBootstrapRequest, WatchError,
    WatchRetention, WatchScope, WatchStart,
};

const SECRET: &str = "test-secret-0123456789abcdef0123456789abcdef";

fn seed_bucket_identity(store: &Store) {
    store
        .bootstrap_system(SystemBootstrapRequest {
            app_id: "bootstrap-app".into(),
            client_id: "bootstrap-client".into(),
            client_secret: SECRET.into(),
        })
        .unwrap();
    let tenant = StorageTenantId::parse("tenant").unwrap();
    let bootstrap = ObjectRef::opaque("app", "bootstrap-app").unwrap();
    let owner = ObjectRef::opaque("app", "owner-app").unwrap();
    store
        .provision_tenant(ProvisionTenantRequest {
            storage_tenant: tenant.clone(),
            owner_app_id: "owner-app".into(),
            owner_client_id: "owner-client".into(),
            owner_client_secret: SECRET.into(),
            principal: bootstrap,
            expected_authorization_revision: AuthzRevision(3),
            expected_binding_generation: 1,
        })
        .unwrap();
    store
        .create_bucket(CreateBucketRequest {
            storage_tenant: tenant,
            bucket: "bucket".into(),
            owner: owner.clone(),
            principal: owner,
            expected_authorization_revision: AuthzRevision(4),
            expected_binding_generation: 1,
            versioning: ObjectVersioning::Unversioned,
        })
        .unwrap();
}

fn key(path: &str) -> ObjectKey {
    ObjectKey::new("tenant", "bucket", path).unwrap()
}

fn put(path: &str, bytes: &[u8], command_id: &str) -> PutRequest {
    PutRequest {
        key: key(path),
        bytes: bytes.to_vec(),
        content_type: Some("application/octet-stream".into()),
        mode: PutMode::PutIfAbsent,
        command_id: Some(command_id.into()),
        durability: Durability::Local,
    }
}

#[tokio::test]
async fn put_delete_and_replay_keep_one_durable_invalidation_per_head_change() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    seed_bucket_identity(&store);
    assert_eq!(store.local_invalidation_offset().unwrap(), 0);

    let put_request = put("record", b"value", "put-record");
    let created = store.put(put_request.clone()).await.unwrap();
    let delete_request = DeleteRequest {
        key: key("record"),
        precondition: Precondition::Version(created.version),
        command_id: Some("delete-record".into()),
        durability: Durability::Local,
    };
    let deleted = store.delete(delete_request.clone()).await.unwrap();

    assert_eq!(store.local_invalidation_offset().unwrap(), 2);
    let changes = store.scan_local_changes(0, 10).unwrap();
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].offset(), 1);
    assert_eq!(changes[1].offset(), 2);
    let invalidations = store
        .scan_local_changes(0, 10)
        .unwrap()
        .into_iter()
        .filter_map(|change| change.into_object_head())
        .collect::<Vec<_>>();
    assert_eq!(invalidations.len(), 2);
    assert_eq!(invalidations[0].offset, 1);
    assert_eq!(invalidations[0].exact_path, "record");
    assert_eq!(invalidations[0].path_version, created.version);
    assert_eq!(invalidations[0].kind, ObjectHeadChangeKind::Put);
    assert_eq!(invalidations[1].offset, 2);
    assert_eq!(invalidations[1].path_version, deleted.version);
    assert_eq!(invalidations[1].kind, ObjectHeadChangeKind::Delete);
    assert_ne!(invalidations[0].tenant_id, 0);
    assert_ne!(invalidations[0].bucket_id, 0);
    assert_eq!(invalidations[0].tenant_id, invalidations[1].tenant_id);
    assert_eq!(invalidations[0].bucket_id, invalidations[1].bucket_id);
    assert_eq!(
        store
            .read_local_change(2)
            .unwrap()
            .and_then(|change| change.into_object_head()),
        Some(invalidations[1].clone())
    );
    assert!(store.read_local_change(3).unwrap().is_none());

    drop(store);
    let reopened = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    assert_eq!(reopened.local_invalidation_offset().unwrap(), 2);
    assert!(reopened.put(put_request).await.unwrap().replayed);
    assert!(reopened.delete(delete_request).await.unwrap().replayed);
    assert_eq!(reopened.local_invalidation_offset().unwrap(), 2);
    assert_eq!(reopened.scan_local_changes(0, 10).unwrap().len(), 2);
}

#[tokio::test]
async fn bulk_appends_only_successful_head_changes_and_bounds_scans() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    seed_bucket_identity(&store);
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

    let first_page = store
        .scan_local_changes(0, 1)
        .unwrap()
        .into_iter()
        .filter_map(|change| change.into_object_head())
        .collect::<Vec<_>>();
    assert_eq!(first_page.len(), 1);
    assert_eq!(first_page[0].offset, 1);
    assert_eq!(first_page[0].exact_path, "a");
    let second_page = store
        .scan_local_changes(1, 10)
        .unwrap()
        .into_iter()
        .filter_map(|change| change.into_object_head())
        .collect::<Vec<_>>();
    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0].offset, 2);
    assert_eq!(second_page[0].exact_path, "b");

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
    assert_eq!(store.scan_local_changes(0, 10).unwrap().len(), 2);
}

#[tokio::test]
async fn watch_starts_resume_and_checkpoint_across_unrelated_paths_and_restart() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1)
            .with_watch_retention(WatchRetention::new(10, 1024 * 1024).unwrap()),
    )
    .await
    .unwrap();
    seed_bucket_identity(&store);
    let scope = WatchScope::new("tenant", "bucket", "matching").unwrap();
    let initial_epoch = store.local_watch_status().unwrap().source_epoch;
    let now = store.start_watch(&scope, WatchStart::Now).unwrap();
    assert_eq!(now.offset(), 0);
    let initial_token = store.watch_checkpoint(&scope, now).unwrap();

    store
        .put(put("unrelated/one", b"one", "unrelated"))
        .await
        .unwrap();
    store
        .put(put("matching/one", b"two", "matching"))
        .await
        .unwrap();

    let page = store.scan_watch_page(&scope, now, 10).await.unwrap();
    assert_eq!(page.checkpoint.offset(), 2);
    assert_eq!(page.invalidations.len(), 1);
    assert_eq!(page.invalidations[0].key, key("matching/one"));
    // Until the consumer durably stores the checkpoint, replaying the same
    // cursor deliberately redelivers the same at-least-once invalidation.
    assert_eq!(
        store
            .scan_watch_page(&scope, now, 10)
            .await
            .unwrap()
            .invalidations,
        page.invalidations
    );
    let checkpoint = store.watch_checkpoint(&scope, page.checkpoint).unwrap();
    assert_eq!(
        store
            .start_watch(&scope, WatchStart::Resume(checkpoint.clone()))
            .unwrap(),
        page.checkpoint
    );

    drop(store);
    let reopened = Store::open(
        StoreOptions::new(temporary.path(), 1)
            .with_watch_retention(WatchRetention::new(10, 1024 * 1024).unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened.local_watch_status().unwrap().source_epoch,
        initial_epoch
    );
    assert_eq!(
        reopened
            .start_watch(&scope, WatchStart::Resume(checkpoint.clone()))
            .unwrap()
            .offset(),
        2
    );
    assert_eq!(
        reopened
            .start_watch(&scope, WatchStart::Resume(initial_token))
            .unwrap()
            .offset(),
        0
    );

    drop(reopened);
    let changed_window = Store::open(
        StoreOptions::new(temporary.path(), 1)
            .with_watch_retention(WatchRetention::new(11, 1024 * 1024).unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(
        changed_window
            .start_watch(&scope, WatchStart::Resume(checkpoint))
            .unwrap_err(),
        WatchError::ResumeExpired
    );
}

#[tokio::test]
async fn public_watch_hides_reserved_segments_and_advances_its_checkpoint() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    seed_bucket_identity(&store);
    let scope = WatchScope::new("tenant", "bucket", "").unwrap();
    let cursor = store.start_watch(&scope, WatchStart::Now).unwrap();

    for (path, command) in [
        ("_anvil", "reserved-root"),
        ("a/_anvil/meta.json", "reserved-descendant"),
        ("_anvilish", "ordinary-path"),
    ] {
        store
            .put(put(path, path.as_bytes(), command))
            .await
            .unwrap();
    }

    let page = store.scan_watch_page(&scope, cursor, 10).await.unwrap();
    assert_eq!(page.checkpoint.offset(), 3);
    assert_eq!(page.invalidations.len(), 1);
    assert_eq!(page.invalidations[0].offset, 3);
    assert_eq!(page.invalidations[0].key, key("_anvilish"));
}

#[tokio::test]
async fn entry_retention_prunes_in_the_head_batch_and_expires_stale_tokens() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1)
            .with_watch_retention(WatchRetention::new(2, 1024 * 1024).unwrap()),
    )
    .await
    .unwrap();
    seed_bucket_identity(&store);
    let scope = WatchScope::new("tenant", "bucket", "").unwrap();
    let stale = store
        .watch_checkpoint(&scope, store.start_watch(&scope, WatchStart::Now).unwrap())
        .unwrap();
    for (path, command) in [("a", "a"), ("b", "b"), ("c", "c")] {
        store
            .put(put(path, path.as_bytes(), command))
            .await
            .unwrap();
    }

    let status = store.local_watch_status().unwrap();
    assert_eq!(status.tail, 3);
    assert_eq!(status.retention_floor, 1);
    assert_eq!(status.retained_entries, 2);
    assert!(store.read_local_change(1).unwrap().is_none());
    assert_eq!(
        store
            .start_watch(&scope, WatchStart::Resume(stale))
            .unwrap_err(),
        WatchError::ResumeExpired
    );
    let beginning = store
        .start_watch(&scope, WatchStart::RetainedBeginning)
        .unwrap();
    assert_eq!(beginning.offset(), 1);
    let page = store.scan_watch_page(&scope, beginning, 10).await.unwrap();
    assert_eq!(
        page.invalidations
            .iter()
            .map(|entry| entry.offset)
            .collect::<Vec<_>>(),
        [2, 3]
    );
}

#[tokio::test]
async fn byte_retention_is_hard_even_when_one_record_exceeds_the_bound() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1)
            .with_watch_retention(WatchRetention::new(100, 1).unwrap()),
    )
    .await
    .unwrap();
    seed_bucket_identity(&store);
    let scope = WatchScope::new("tenant", "bucket", "").unwrap();
    let before = store.start_watch(&scope, WatchStart::Now).unwrap();
    store.put(put("record", b"value", "record")).await.unwrap();

    let status = store.local_watch_status().unwrap();
    assert_eq!(status.tail, 1);
    assert_eq!(status.retention_floor, 1);
    assert_eq!(status.retained_entries, 0);
    assert_eq!(status.retained_bytes, 0);
    assert_eq!(
        store.scan_watch_page(&scope, before, 10).await.unwrap_err(),
        WatchError::ResumeExpired
    );
}

#[tokio::test]
async fn wait_observes_a_new_durable_invalidation_without_a_lost_wakeup() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    seed_bucket_identity(&store);
    let scope = WatchScope::new("tenant", "bucket", "").unwrap();
    let cursor = store.start_watch(&scope, WatchStart::Now).unwrap();
    let waiting = {
        let store = store.clone();
        tokio::spawn(async move { store.wait_for_watch_change(cursor).await })
    };
    store.put(put("wake", b"up", "wake")).await.unwrap();
    waiting.await.unwrap().unwrap();
}
