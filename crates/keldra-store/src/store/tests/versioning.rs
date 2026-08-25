use super::*;

#[test]
fn program_definition_paths_are_direct_versioned_children() {
    assert!(is_program_definition_path("_keldra/programs/import_osv@1"));
    assert!(!is_program_definition_path("_keldra/programs/import_osv"));
    assert!(!is_program_definition_path("_keldra/programs/@1"));
    assert!(!is_program_definition_path("_keldra/programs/import_osv@"));
    assert!(!is_program_definition_path(
        "_keldra/programs/nested/import_osv@1"
    ));
    assert!(!is_program_definition_path(
        "_keldra/programs/import_osv@1@copy"
    ));
}

#[tokio::test]
async fn unversioned_put_retains_replay_descriptor_and_exact_cas_moves_the_head() {
    let (_temporary, store) = store().await;
    let first = store
        .put(put("a", b"one", Precondition::Absent, "one"))
        .await
        .unwrap();
    let second = store
        .put(put(
            "a",
            b"two",
            Precondition::Version(first.version),
            "two",
        ))
        .await
        .unwrap();
    assert!(second.version > first.version);
    assert_eq!(store.get(&key("a")).await.unwrap().unwrap().bytes, b"two");
    assert!(
        store
            .version_metadata(&key("a"), first.version)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store
            .get_version(&key("a"), first.version)
            .await
            .unwrap_err(),
        MutationError::ObjectVersioningNotEnabled
    );
}

#[tokio::test]
async fn enabled_versioning_retains_descriptors_and_payload_references() {
    let (_temporary, store) = store().await;
    assert!(
        store
            .enable_bucket_versioning("tenant", "bucket")
            .await
            .unwrap()
    );
    assert!(
        !store
            .enable_bucket_versioning("tenant", "bucket")
            .await
            .unwrap()
    );
    let first = store
        .put(put("a", b"same", Precondition::Absent, "first"))
        .await
        .unwrap();
    let second = store
        .put(put(
            "a",
            b"same",
            Precondition::Version(first.version),
            "second",
        ))
        .await
        .unwrap();

    assert_eq!(
        store
            .get_version(&key("a"), first.version)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"same"
    );
    assert_eq!(
        store
            .list_object_versions(&key("a"), None, MAX_LIST_OBJECT_VERSIONS)
            .unwrap()
            .into_iter()
            .map(|version| version.id)
            .collect::<Vec<_>>(),
        vec![first.version, second.version]
    );
    let reference = blob_reference_for_bytes(b"same");
    assert_eq!(
        store
            .blob_reference_state(&reference)
            .unwrap()
            .unwrap()
            .ref_count,
        2
    );
}

#[tokio::test]
async fn get_version_rejects_a_descriptor_id_that_disagrees_with_its_key() {
    let (_temporary, store) = store().await;
    store
        .enable_bucket_versioning("tenant", "bucket")
        .await
        .unwrap();
    let created = store
        .put(put(
            "corrupt-version-id",
            b"value",
            Precondition::Absent,
            "corrupt-version-id-create",
        ))
        .await
        .unwrap();
    let object_key = key("corrupt-version-id");
    let identity = store
        .resolve_bucket_identity(object_key.tenant(), object_key.bucket())
        .unwrap();
    let descriptor_key = version_key(identity, &object_key, created.version);
    let mut stored = store
        .stored_version_by_key(&descriptor_key)
        .unwrap()
        .unwrap();
    stored.version.id = VersionId(u64::MAX);
    store
        .db
        .put_cf(
            store.cf(CF_VERSIONS).unwrap(),
            descriptor_key,
            serde_json::to_vec(&stored).unwrap(),
        )
        .unwrap();

    assert!(matches!(
        store.get_version(&object_key, created.version).await,
        Err(MutationError::Storage(message)) if message.contains("disagrees with its key")
    ));
}

#[tokio::test]
async fn batch_get_rejects_a_descriptor_that_disagrees_with_its_current_head() {
    let (_temporary, store) = store().await;
    let created = store
        .put(put(
            "corrupt-current-head",
            b"value",
            Precondition::Absent,
            "corrupt-current-head-create",
        ))
        .await
        .unwrap();
    let object_key = key("corrupt-current-head");
    let identity = store
        .resolve_bucket_identity(object_key.tenant(), object_key.bucket())
        .unwrap();
    let descriptor_key = version_key(identity, &object_key, created.version);
    let mut stored = store
        .stored_version_by_key(&descriptor_key)
        .unwrap()
        .unwrap();
    stored.version.blob = None;
    stored.version.deleted = true;
    store
        .db
        .put_cf(
            store.cf(CF_VERSIONS).unwrap(),
            descriptor_key,
            serde_json::to_vec(&stored).unwrap(),
        )
        .unwrap();

    let results = store.batch_get(&[(object_key, None)]).await;
    assert!(matches!(
        &results[0],
        Err(MutationError::Storage(message)) if message.contains("disagrees with its head")
    ));
}

#[tokio::test]
async fn enabling_versioning_retains_the_existing_current_value_and_survives_reopen() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let first = store
        .put(put("a", b"first", Precondition::Absent, "first"))
        .await
        .unwrap();
    assert!(
        store
            .enable_bucket_versioning("tenant", "bucket")
            .await
            .unwrap()
    );
    let second = store
        .put(put(
            "a",
            b"second",
            Precondition::Version(first.version),
            "second",
        ))
        .await
        .unwrap();
    drop(store);

    let reopened = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    assert_eq!(
        reopened.bucket_versioning("tenant", "bucket").unwrap(),
        ObjectVersioning::Enabled
    );
    assert_eq!(
        reopened
            .get_version(&key("a"), first.version)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"first"
    );
    assert_eq!(
        reopened
            .get_version(&key("a"), second.version)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"second"
    );
}

#[tokio::test]
async fn unversioned_reference_ownership_remains_pinned_until_checkpoint() {
    let (_temporary, store) = store().await;
    let same_reference = blob_reference_for_bytes(b"same");
    let other_reference = blob_reference_for_bytes(b"other");
    let first = store
        .put(put("a", b"same", Precondition::Absent, "first"))
        .await
        .unwrap();
    let second = store
        .put(put(
            "a",
            b"same",
            Precondition::Version(first.version),
            "same-again",
        ))
        .await
        .unwrap();
    assert_eq!(
        store
            .blob_reference_state(&same_reference)
            .unwrap()
            .unwrap()
            .ref_count,
        2
    );
    assert!(
        store
            .version_metadata(&key("a"), first.version)
            .unwrap()
            .is_some()
    );

    let third = store
        .put(put(
            "a",
            b"other",
            Precondition::Version(second.version),
            "replace",
        ))
        .await
        .unwrap();
    assert_eq!(
        store
            .blob_reference_state(&same_reference)
            .unwrap()
            .unwrap()
            .ref_count,
        2
    );
    assert_eq!(
        store
            .blob_reference_state(&other_reference)
            .unwrap()
            .unwrap()
            .ref_count,
        1
    );

    let deleted = store
        .delete(DeleteRequest {
            key: key("a"),
            precondition: Precondition::Version(third.version),
            command_id: Some("delete".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .blob_reference_state(&other_reference)
            .unwrap()
            .unwrap()
            .ref_count,
        1
    );
    assert!(
        store
            .version_metadata(&key("a"), third.version)
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .version_metadata(&key("a"), deleted.version)
            .unwrap()
            .unwrap()
            .deleted
    );
}

#[tokio::test]
async fn retained_version_deletion_never_reveals_an_older_value() {
    let (_temporary, store) = store().await;
    store
        .enable_bucket_versioning("tenant", "bucket")
        .await
        .unwrap();
    let first = store
        .put(put("a", b"first", Precondition::Absent, "first"))
        .await
        .unwrap();
    let second = store
        .put(put(
            "a",
            b"second",
            Precondition::Version(first.version),
            "second",
        ))
        .await
        .unwrap();
    let third = store
        .put(put(
            "a",
            b"third",
            Precondition::Version(second.version),
            "third",
        ))
        .await
        .unwrap();

    assert_eq!(
        store
            .delete_retained_version(&key("a"), first.version)
            .await
            .unwrap(),
        DeleteRetainedVersionOutcome::DeletedNonCurrent
    );
    assert_eq!(
        store.head(&key("a")).unwrap().unwrap().version,
        third.version
    );
    assert_eq!(
        store
            .list_object_versions(&key("a"), Some(first.version), 1)
            .unwrap()
            .into_iter()
            .map(|version| version.id)
            .collect::<Vec<_>>(),
        vec![second.version]
    );

    let tombstone = match store
        .delete_retained_version(&key("a"), third.version)
        .await
        .unwrap()
    {
        DeleteRetainedVersionOutcome::ReplacedCurrentWithTombstone { version } => version,
        other => panic!("unexpected retained-version deletion outcome: {other:?}"),
    };
    assert!(tombstone > third.version);
    assert_eq!(store.head(&key("a")).unwrap().unwrap().version, tombstone);
    assert!(store.head(&key("a")).unwrap().unwrap().deleted);
    assert!(
        store
            .version_metadata(&key("a"), third.version)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .list_object_versions(&key("a"), None, MAX_LIST_OBJECT_VERSIONS)
            .unwrap()
            .into_iter()
            .map(|version| version.id)
            .collect::<Vec<_>>(),
        vec![second.version, tombstone]
    );
    let retained_deletions = store
        .scan_local_changes(0, 20)
        .unwrap()
        .into_iter()
        .filter_map(|change| match change {
            LocalChange::RetainedVersionDeleted(change) => Some(change),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(retained_deletions.len(), 2);
    assert_eq!(retained_deletions[0].deleted_version, first.version);
    assert_eq!(retained_deletions[0].resulting_head_version, None);
    assert_eq!(
        retained_deletions[0].reference_deltas,
        [ReferenceDelta {
            blob: blob_reference_for_bytes(b"first"),
            change: -1,
        }]
    );
    assert_eq!(retained_deletions[1].deleted_version, third.version);
    assert_eq!(
        retained_deletions[1].resulting_head_version,
        Some(tombstone)
    );
    assert_eq!(
        retained_deletions[1].reference_deltas,
        [ReferenceDelta {
            blob: blob_reference_for_bytes(b"third"),
            change: -1,
        }]
    );
    assert_eq!(
        store
            .delete_retained_version(&key("a"), tombstone)
            .await
            .unwrap_err(),
        MutationError::CurrentTombstoneCannotBeDeleted
    );
    assert_eq!(
        store
            .delete_retained_version(&key("a"), VersionId(u64::MAX))
            .await
            .unwrap(),
        DeleteRetainedVersionOutcome::NotFound
    );
}
