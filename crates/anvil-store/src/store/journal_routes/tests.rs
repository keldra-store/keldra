use crate::{
    BatchOperation, BucketPolicy, DefinitionMutationIntent, DeleteRequest, Durability,
    INDEX_DEFINITION_PREFIX, ObjectKey, ObjectMutationGovernance, ObjectVersioning, Precondition,
    PutMode, PutRequest, StoreOptions, WatchRetention,
};

use super::*;

fn put(tenant: &str, bucket: &str, path: &str, command: &str) -> PutRequest {
    PutRequest {
        key: ObjectKey::new(tenant, bucket, path).unwrap(),
        bytes: command.as_bytes().to_vec(),
        content_type: Some("application/octet-stream".into()),
        mode: PutMode::Put,
        command_id: Some(command.into()),
        durability: Durability::Local,
    }
}

async fn store() -> (tempfile::TempDir, Store) {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    (temporary, store)
}

#[tokio::test]
async fn routed_pages_are_target_bounded_advance_empty_intervals_and_measure_peer_bytes() {
    let (_temporary, store) = store().await;
    store
        .put(put("tenant", "bucket", "a", "put-a"))
        .await
        .unwrap();
    store
        .put(put("tenant", "other", "unrelated", "put-unrelated"))
        .await
        .unwrap();
    store
        .put(put("tenant", "bucket", "b", "put-b"))
        .await
        .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let (other_tenant_id, other_bucket_id) = store.resolve_bucket_ids("tenant", "other").unwrap();
    assert_eq!(tenant_id, other_tenant_id);
    let status = store.local_watch_status().unwrap();
    assert_eq!(status.tail, 3);
    let expected_logical_bytes = store
        .scan_local_changes(0, 10)
        .unwrap()
        .iter()
        .map(|change| {
            let encoded = crate::watch::encode_local_change(change).unwrap();
            crate::watch::invalidation_record_bytes(encoded.len())
                + journal_route_logical_bytes(change)
        })
        .sum::<u64>();
    assert_eq!(status.retained_bytes, expected_logical_bytes);

    // The matching route at offset three is immediately after this captured
    // target and must not leak into the page or its byte accounting.
    let page = store
        .scan_routed_local_changes(
            JournalRoute::Bucket {
                tenant_id,
                bucket_id,
            },
            status.source_id,
            0,
            2,
            10,
            u64::MAX,
        )
        .unwrap();
    assert_eq!(
        page.changes
            .iter()
            .map(LocalChange::offset)
            .collect::<Vec<_>>(),
        [1]
    );
    assert_eq!(page.through_offset, 2);
    let peer_bytes = serde_json::to_vec(&page.changes[0]).unwrap().len() as u64;
    assert_eq!(page.encoded_bytes, peer_bytes);

    let empty_between_matches = store
        .scan_routed_local_changes(
            JournalRoute::Bucket {
                tenant_id,
                bucket_id,
            },
            status.source_id,
            1,
            2,
            10,
            1024,
        )
        .unwrap();
    assert!(empty_between_matches.changes.is_empty());
    assert_eq!(empty_between_matches.through_offset, 2);

    let empty = store
        .scan_routed_local_changes(
            JournalRoute::Bucket {
                tenant_id,
                bucket_id: other_bucket_id,
            },
            status.source_id,
            2,
            2,
            10,
            1024,
        )
        .unwrap();
    assert!(empty.changes.is_empty());
    assert_eq!(empty.through_offset, 2);
    assert_eq!(empty.encoded_bytes, 0);

    let no_matches = store
        .scan_routed_local_changes(
            JournalRoute::Bucket {
                tenant_id,
                bucket_id: u64::MAX,
            },
            status.source_id,
            0,
            status.tail,
            10,
            1024,
        )
        .unwrap();
    assert!(no_matches.changes.is_empty());
    assert_eq!(no_matches.through_offset, status.tail);

    let oversize = store
        .scan_routed_local_changes(
            JournalRoute::Bucket {
                tenant_id,
                bucket_id,
            },
            status.source_id,
            0,
            status.tail,
            10,
            peer_bytes - 1,
        )
        .unwrap();
    assert!(oversize.changes.is_empty());
    assert_eq!(oversize.through_offset, 0);
    assert_eq!(
        oversize.oversize,
        Some(OversizeLocalChange {
            offset: 1,
            encoded_bytes: peer_bytes,
        })
    );
}

#[tokio::test]
async fn definition_mutations_commit_locator_and_both_sparse_routes() {
    let (_temporary, store) = store().await;
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let governance = ObjectMutationGovernance {
        tenant_id,
        bucket_id,
        versioning: ObjectVersioning::Unversioned,
        policy: BucketPolicy::default(),
    };
    let path = format!("{INDEX_DEFINITION_PREFIX}search");
    let intent = DefinitionMutationIntent::new(DefinitionKind::Index, 41).unwrap();
    let created = store
        .mutate_definition_with_governance(
            BatchOperation::Put(PutRequest {
                mode: PutMode::PutIfAbsent,
                ..put("tenant", "bucket", &path, "create-definition")
            }),
            governance.clone(),
            intent,
        )
        .await
        .unwrap();
    let status = store.local_watch_status().unwrap();
    assert_eq!(status.tail, 1);
    let locator = store
        .definition_locator(DefinitionKind::Index, tenant_id, bucket_id, &path)
        .unwrap()
        .unwrap();
    assert_eq!(locator.definition_id, 41);
    assert_eq!(locator.object_version, created.version);

    for route in [
        JournalRoute::Definition(DefinitionKind::Index),
        JournalRoute::Bucket {
            tenant_id,
            bucket_id,
        },
    ] {
        let page = store
            .scan_routed_local_changes(route, status.source_id, 0, status.tail, 10, u64::MAX)
            .unwrap();
        assert_eq!(page.changes.len(), 1);
        assert_eq!(page.through_offset, 1);
        let LocalChange::ObjectHead(change) = &page.changes[0] else {
            panic!("definition route returned a non-head change")
        };
        assert_eq!(
            change.definition_transition.as_ref().unwrap().definition_id,
            41
        );
    }

    let deleted = store
        .mutate_definition_with_governance(
            BatchOperation::Delete(DeleteRequest {
                key: ObjectKey::new("tenant", "bucket", &path).unwrap(),
                precondition: Precondition::Version(created.version),
                command_id: Some("delete-definition".into()),
                durability: Durability::Local,
            }),
            governance,
            intent,
        )
        .await
        .unwrap();
    assert!(deleted.deleted);
    let deleted_locator = store
        .definition_locator(DefinitionKind::Index, tenant_id, bucket_id, &path)
        .unwrap()
        .unwrap();
    assert_eq!(deleted_locator.definition_id, 41);
    assert_eq!(deleted_locator.object_version, deleted.version);
    assert_eq!(
        deleted_locator.operation,
        crate::DefinitionOperation::Delete
    );
    let status = store.local_watch_status().unwrap();
    let page = store
        .scan_routed_local_changes(
            JournalRoute::Definition(DefinitionKind::Index),
            status.source_id,
            1,
            2,
            10,
            u64::MAX,
        )
        .unwrap();
    let LocalChange::ObjectHead(change) = &page.changes[0] else {
        panic!("definition route returned a non-head change")
    };
    assert_eq!(
        change.definition_transition.as_ref().unwrap().operation,
        crate::DefinitionOperation::Delete
    );
}

#[tokio::test]
async fn accounting_definition_mutations_commit_typed_locator_and_routes() {
    let (_temporary, store) = store().await;
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let governance = ObjectMutationGovernance {
        tenant_id,
        bucket_id,
        versioning: ObjectVersioning::Unversioned,
        policy: BucketPolicy::default(),
    };
    let path = "_anvil/accounting/definitions/52";
    let intent = DefinitionMutationIntent::new(DefinitionKind::Accounting, 52).unwrap();
    let created = store
        .mutate_definition_with_governance(
            BatchOperation::Put(PutRequest {
                mode: PutMode::PutIfAbsent,
                ..put("tenant", "bucket", path, "create-accounting-definition")
            }),
            governance.clone(),
            intent,
        )
        .await
        .unwrap();

    let locator = store
        .definition_locator(DefinitionKind::Accounting, tenant_id, bucket_id, path)
        .unwrap()
        .unwrap();
    assert_eq!(locator.definition_id, 52);
    assert_eq!(locator.object_version, created.version);
    let status = store.local_watch_status().unwrap();
    let page = store
        .scan_routed_local_changes(
            JournalRoute::Definition(DefinitionKind::Accounting),
            status.source_id,
            0,
            status.tail,
            10,
            u64::MAX,
        )
        .unwrap();
    assert_eq!(page.changes.len(), 1);
    let LocalChange::ObjectHead(change) = &page.changes[0] else {
        panic!("accounting definition route returned a non-head change")
    };
    assert_eq!(
        change.definition_transition.as_ref().unwrap().kind,
        DefinitionKind::Accounting
    );

    let deleted = store
        .mutate_definition_with_governance(
            BatchOperation::Delete(DeleteRequest {
                key: ObjectKey::new("tenant", "bucket", path).unwrap(),
                precondition: Precondition::Version(created.version),
                command_id: Some("delete-accounting-definition".into()),
                durability: Durability::Local,
            }),
            governance,
            intent,
        )
        .await
        .unwrap();
    let deleted_locator = store
        .definition_locator(DefinitionKind::Accounting, tenant_id, bucket_id, path)
        .unwrap()
        .unwrap();
    assert_eq!(deleted_locator.definition_id, 52);
    assert_eq!(deleted_locator.object_version, deleted.version);
    assert_eq!(
        deleted_locator.operation,
        crate::DefinitionOperation::Delete
    );
}

#[tokio::test]
async fn primary_journal_retention_prunes_its_routes_in_the_same_batch() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1)
            .with_watch_retention(WatchRetention::new(1, 1024 * 1024).unwrap()),
    )
    .await
    .unwrap();
    store
        .put(put("tenant", "bucket", "a", "put-a"))
        .await
        .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let first_status = store.local_watch_status().unwrap();
    let first_route = route_key(
        JournalRoute::Bucket {
            tenant_id,
            bucket_id,
        },
        first_status.source_id.source_epoch,
        1,
    )
    .unwrap();
    assert!(
        store
            .db
            .get_cf(store.cf(CF_JOURNAL_ROUTES).unwrap(), &first_route)
            .unwrap()
            .is_some()
    );

    store
        .advance_source_journal_reference_safe_through(1)
        .await
        .unwrap();
    store
        .put(put("tenant", "bucket", "b", "put-b"))
        .await
        .unwrap();
    let status = store.local_watch_status().unwrap();
    assert_eq!(status.retention_floor, 1);
    assert!(
        store
            .db
            .get_cf(
                store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
                invalidation_key(1)
            )
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .db
            .get_cf(store.cf(CF_JOURNAL_ROUTES).unwrap(), first_route)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .db
            .get_cf(
                store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
                invalidation_key(2)
            )
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        store.scan_routed_local_changes(
            JournalRoute::Bucket {
                tenant_id,
                bucket_id,
            },
            status.source_id,
            0,
            status.tail,
            1,
            1024,
        ),
        Err(RoutedJournalError::CursorExpired {
            cursor: 0,
            retention_floor: 1,
        })
    ));
}

#[tokio::test]
async fn routed_cursor_and_missing_primary_fail_distinctly() {
    let (_temporary, store) = store().await;
    store
        .put(put("tenant", "bucket", "a", "put-a"))
        .await
        .unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let status = store.local_watch_status().unwrap();
    let route = JournalRoute::Bucket {
        tenant_id,
        bucket_id,
    };
    assert!(matches!(
        store.scan_routed_local_changes(route, status.source_id, 1, 0, 1, 1024),
        Err(RoutedJournalError::TargetBeforeCursor { .. })
    ));
    assert!(matches!(
        store.scan_routed_local_changes(route, status.source_id, 0, 2, 1, 1024),
        Err(RoutedJournalError::TargetFuture { .. })
    ));
    assert!(matches!(
        store.scan_routed_local_changes(route, status.source_id, 2, 2, 1, 1024),
        Err(RoutedJournalError::CursorFuture { .. })
    ));
    let mut wrong_source = status.source_id;
    wrong_source.source_epoch[0] ^= 1;
    assert_eq!(
        store
            .scan_routed_local_changes(route, wrong_source, 0, 1, 1, 1024)
            .unwrap_err(),
        RoutedJournalError::SourceEpochMismatch
    );

    store
        .db
        .delete_cf(
            store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
            invalidation_key(1),
        )
        .unwrap();
    assert_eq!(
        store
            .scan_routed_local_changes(route, status.source_id, 0, 1, 1, 1024)
            .unwrap_err(),
        RoutedJournalError::MissingPrimary { offset: 1 }
    );
}
