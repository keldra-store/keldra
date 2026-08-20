use super::*;

#[tokio::test]
async fn batch_get_preserves_tombstone_version_and_never_existed() {
    let (_temporary, store) = store().await;
    let created = store
        .put(put(
            "deleted",
            b"value",
            Precondition::Absent,
            "create-deleted",
        ))
        .await
        .unwrap();
    let deleted = store
        .delete(DeleteRequest {
            key: key("deleted"),
            precondition: Precondition::Version(created.version),
            command_id: Some("delete-current".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    let results = store
        .batch_get(&[(key("deleted"), None), (key("never"), None)])
        .await;
    let tombstone = results[0].as_ref().unwrap().as_ref().unwrap();
    assert!(tombstone.version.deleted);
    assert_eq!(tombstone.version.id, deleted.version);
    assert!(results[1].as_ref().unwrap().is_none());
}

#[tokio::test]
async fn batch_get_selection_releases_the_fence_before_blob_reads_and_keeps_selected_bytes() {
    let (_temporary, store) = store().await;
    store
        .enable_bucket_versioning("tenant", "bucket")
        .await
        .unwrap();
    let old = store
        .put(put("moving", b"old", Precondition::Absent, "moving-old"))
        .await
        .unwrap();
    let old_version = old.version;
    let large_payload = vec![9_u8; SMALL_BLOB_MAX_BYTES + 1];
    let large = store
        .put(put("large", &large_payload, Precondition::Absent, "large"))
        .await
        .unwrap();
    let created = store
        .put(put(
            "deleted",
            b"value",
            Precondition::Absent,
            "deleted-create",
        ))
        .await
        .unwrap();
    let deleted = store
        .delete(DeleteRequest {
            key: key("deleted"),
            precondition: Precondition::Version(created.version),
            command_id: Some("deleted-delete".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();

    let selection = store
        .select_batch_get(&[
            (key("moving"), None),
            (key("large"), Some(large.version)),
            (key("deleted"), None),
            (key("never"), None),
            (key("moving"), Some(VersionId(u64::MAX))),
        ])
        .await;
    assert_eq!(
        selection.declared_present_payload_bytes(),
        (b"old".len() + large_payload.len()) as u64
    );

    let moving_store = store.clone();
    let movement = tokio::spawn(async move {
        moving_store
            .put(put(
                "moving",
                b"new head",
                Precondition::Version(old_version),
                "moving-new",
            ))
            .await
            .unwrap()
    });
    let current = tokio::time::timeout(std::time::Duration::from_secs(1), movement)
        .await
        .expect("holding an immutable batch selection must not fence an unrelated commit")
        .unwrap();
    let results = store.read_batch_get_selection(selection).await;

    let selected_old = results[0].as_ref().unwrap().as_ref().unwrap();
    assert_eq!(selected_old.version.id, old_version);
    assert_eq!(selected_old.bytes, b"old");
    assert_eq!(
        results[1].as_ref().unwrap().as_ref().unwrap().bytes,
        large_payload
    );
    let selected_tombstone = results[2].as_ref().unwrap().as_ref().unwrap();
    assert_eq!(selected_tombstone.version.id, deleted.version);
    assert!(selected_tombstone.version.deleted);
    assert!(results[3].as_ref().unwrap().is_none());
    assert!(results[4].as_ref().unwrap().is_none());
    assert_eq!(
        store.head(&key("moving")).unwrap().unwrap().version,
        current.version
    );
}

#[tokio::test]
async fn unversioned_retirement_does_not_block_or_break_selected_blob_reads() {
    let (_temporary, store) = store().await;
    let original_bytes = vec![0x41; SMALL_BLOB_MAX_BYTES + 1];
    let original = store
        .put(put(
            "retired-after-selection",
            &original_bytes,
            Precondition::Absent,
            "retired-original",
        ))
        .await
        .unwrap();
    let original_metadata = store
        .version_metadata(&key("retired-after-selection"), original.version)
        .unwrap()
        .unwrap();
    let original_blob = original_metadata.blob.unwrap();
    let selection = store
        .select_batch_get(&[(key("retired-after-selection"), None)])
        .await;

    let replacement = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        store.put(put(
            "retired-after-selection",
            b"replacement",
            Precondition::Version(original.version),
            "retired-replacement",
        )),
    )
    .await
    .expect("an outstanding immutable selection must not fence replacement")
    .unwrap();
    assert!(replacement.version > original.version);
    assert!(
        store
            .version_metadata(&key("retired-after-selection"), original.version)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .blob_reference_state(&original_blob)
            .unwrap()
            .unwrap()
            .ref_count,
        0
    );

    let selected = store.read_batch_get_selection(selection).await;
    assert_eq!(
        selected[0].as_ref().unwrap().as_ref().unwrap().bytes,
        original_bytes
    );
}

#[tokio::test]
async fn reserved_program_definitions_require_put_immutable_then_replay_same_content() {
    let (_temporary, store) = store().await;
    let path = "_keldra/programs/import_osv@1";
    store
        .set_bucket_policy(
            "tenant",
            "bucket",
            BucketPolicy {
                program_only_prefixes: vec!["_keldra".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .put(put(path, b"definition", Precondition::Any, "unsafe-define"))
            .await
            .unwrap_err(),
        MutationError::Immutable
    );
    assert!(store.head(&key(path)).unwrap().is_none());

    let first = store
        .put(immutable_put(path, b"definition", "define"))
        .await
        .unwrap();

    let replay = store
        .put(immutable_put(path, b"definition", "define-again"))
        .await
        .unwrap();
    assert_eq!(replay.version, first.version);
    assert_eq!(
        store
            .put(put(
                path,
                b"different",
                Precondition::Version(first.version),
                "replace-definition",
            ))
            .await
            .unwrap_err(),
        MutationError::Immutable
    );
    assert_eq!(
        store
            .delete(DeleteRequest {
                key: key(path),
                precondition: Precondition::Version(first.version),
                command_id: Some("delete-definition".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap_err(),
        MutationError::Immutable
    );

    let published_path = "_keldra/programs/published@1";
    let blob = store.stage_blob(b"published-definition").await.unwrap();
    assert_eq!(
        store
            .publish(PublishRequest {
                key: key(published_path),
                blob: blob.clone(),
                content_type: Some("application/json".into()),
                mode: PutMode::Put,
                command_id: Some("unsafe-publish".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap_err(),
        MutationError::Immutable
    );
    assert!(store.head(&key(published_path)).unwrap().is_none());

    let published = store
        .publish(PublishRequest {
            key: key(published_path),
            blob: blob.clone(),
            content_type: Some("application/json".into()),
            mode: PutMode::PutImmutable,
            command_id: Some("publish".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    let replay = store
        .publish(PublishRequest {
            key: key(published_path),
            blob,
            content_type: Some("application/json".into()),
            mode: PutMode::PutImmutable,
            command_id: Some("publish-again".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    assert_eq!(replay.version, published.version);
}

#[tokio::test]
async fn program_only_policy_reports_concurrency_violation_for_every_direct_write_kind() {
    let (_temporary, store) = store().await;
    store
        .set_bucket_policy(
            "tenant",
            "bucket",
            BucketPolicy {
                program_only_prefixes: vec!["managed".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .put(put(
                "managed/a",
                b"value",
                Precondition::Absent,
                "managed-put"
            ))
            .await
            .unwrap_err(),
        MutationError::ProgramConcurrencyViolation
    );
    let blob = store.stage_blob(b"value").await.unwrap();
    assert_eq!(
        store
            .publish(PublishRequest {
                key: key("managed/a"),
                blob,
                content_type: None,
                mode: PutMode::PutIfAbsent,
                command_id: Some("managed-publish".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap_err(),
        MutationError::ProgramConcurrencyViolation
    );
    assert_eq!(
        store
            .delete(DeleteRequest {
                key: key("managed/a"),
                precondition: Precondition::Any,
                command_id: Some("managed-delete".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap_err(),
        MutationError::ProgramConcurrencyViolation
    );
    assert!(
        store
            .put(put("managed-other", b"ok", Precondition::Absent, "outside"))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn list_objects_seeks_the_stable_id_head_prefix_and_returns_lexical_live_pages() {
    let (_temporary, store) = store().await;
    for (index, path) in [
        "z",
        "aa",
        "b",
        "foo",
        "foo/bar",
        "foobar",
        "foo/deleted",
        "_keldra",
        "a/_keldra/meta.json",
        "_keldraish",
    ]
    .into_iter()
    .enumerate()
    {
        store
            .put(put(
                path,
                path.as_bytes(),
                Precondition::Absent,
                &format!("list-{index}"),
            ))
            .await
            .unwrap();
    }
    store
        .delete(DeleteRequest {
            key: key("foo/deleted"),
            precondition: Precondition::Any,
            command_id: Some("list-delete".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    store
        .put(PutRequest {
            key: ObjectKey::new("tenant", "other-bucket", "foo/hidden").unwrap(),
            bytes: b"other bucket".to_vec(),
            content_type: None,
            mode: PutMode::PutIfAbsent,
            command_id: Some("list-other-bucket".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();

    let identity = store.resolve_bucket_identity("tenant", "bucket").unwrap();
    assert_eq!(
        store.head_storage_key(&key("foo")).unwrap(),
        [identity.encode().to_vec(), b"foo".to_vec()].concat()
    );
    assert!(
        store
            .head_storage_key(&key("foo"))
            .unwrap()
            .windows(b"tenant".len())
            .all(|window| window != b"tenant")
    );
    assert!(
        store
            .head_storage_key(&key("foo"))
            .unwrap()
            .windows(b"bucket".len())
            .all(|window| window != b"bucket")
    );

    let all = store
        .list_objects("tenant", "bucket", "", None, MAX_LIST_OBJECTS)
        .unwrap();
    assert_eq!(
        all.paths,
        ["_keldraish", "aa", "b", "foo", "foo/bar", "foobar", "z"]
            .map(str::to_owned)
            .to_vec()
    );
    assert!(!all.has_more);

    // Prefix matching is literal, not path-segment aware: `foo` includes
    // both `foo/bar` and `foobar`. The tombstone is not a listed object.
    let first = store
        .list_objects("tenant", "bucket", "foo", None, 2)
        .unwrap();
    assert_eq!(first.paths, ["foo", "foo/bar"].map(str::to_owned).to_vec());
    assert!(first.has_more);
    let second = store
        .list_objects(
            "tenant",
            "bucket",
            "foo",
            first.paths.last().map(String::as_str),
            2,
        )
        .unwrap();
    assert_eq!(second.paths, vec!["foobar".to_owned()]);
    assert!(!second.has_more);
}

#[tokio::test]
async fn retained_version_keys_use_one_nul_terminator_after_the_raw_path() {
    let (_temporary, store) = store().await;
    let logical = key("a");
    let identity = store
        .resolve_bucket_identity(logical.tenant(), logical.bucket())
        .unwrap();
    let version = VersionId(0x0102_0304_0506_0708);
    assert_eq!(
        version_key(identity, &logical, version),
        [
            identity.encode().to_vec(),
            b"a".to_vec(),
            vec![0],
            version.0.to_be_bytes().to_vec(),
        ]
        .concat()
    );
    assert!(
        !identity
            .head_key("a/b")
            .starts_with(&version_prefix(identity, &logical))
    );
}

#[tokio::test]
async fn list_objects_pages_are_read_committed_not_a_cross_page_snapshot() {
    let (_temporary, store) = store().await;
    store
        .put(put("a", b"a", Precondition::Absent, "page-a"))
        .await
        .unwrap();
    store
        .put(put("c", b"c", Precondition::Absent, "page-c"))
        .await
        .unwrap();

    let first = store.list_objects("tenant", "bucket", "", None, 1).unwrap();
    assert_eq!(first.paths, vec!["a".to_owned()]);
    assert!(first.has_more);

    // A later page observes commits made after the first page. `b` appears
    // and the newly deleted `c` disappears.
    store
        .put(put("b", b"b", Precondition::Absent, "page-b"))
        .await
        .unwrap();
    store
        .delete(DeleteRequest {
            key: key("c"),
            precondition: Precondition::Any,
            command_id: Some("page-delete-c".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    let second = store
        .list_objects("tenant", "bucket", "", Some("a"), 10)
        .unwrap();
    assert_eq!(second.paths, vec!["b".to_owned()]);
    assert!(!second.has_more);
}

#[tokio::test]
async fn local_cluster_listing_uses_stable_ids_and_excludes_non_owned_heads() {
    let (_temporary, store) = store().await;
    for (index, path) in [
        "deep/prefix/a",
        "deep/prefix/b",
        "deep/prefix/deleted",
        "deep/prefix/_keldra/meta.json",
        "deep/sibling",
    ]
    .into_iter()
    .enumerate()
    {
        store
            .put(put(
                path,
                path.as_bytes(),
                Precondition::Absent,
                &format!("cluster-list-{index}"),
            ))
            .await
            .unwrap();
    }
    store
        .delete(DeleteRequest {
            key: key("deep/prefix/deleted"),
            precondition: Precondition::Any,
            command_id: Some("cluster-list-delete".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();

    let identity = store.resolve_bucket_identity("tenant", "bucket").unwrap();
    let mut considered = Vec::new();
    let page = store
        .list_local_owned_objects(
            identity.tenant_id.0,
            identity.bucket_id.0,
            "deep/prefix/",
            None,
            10,
            |tenant_id, bucket_id, path| {
                assert_eq!(tenant_id, identity.tenant_id.0);
                assert_eq!(bucket_id, identity.bucket_id.0);
                considered.push(path.to_owned());
                path != "deep/prefix/b"
            },
        )
        .unwrap();

    assert_eq!(
        considered,
        ["deep/prefix/a", "deep/prefix/b"].map(str::to_owned)
    );
    assert_eq!(page.paths, vec!["deep/prefix/a".to_owned()]);
    assert!(!page.has_more);
}

#[tokio::test]
async fn internal_index_definition_listing_is_format_four_and_bucket_scoped() {
    let (_temporary, store) = store().await;
    let definition = format!("{INDEX_DEFINITION_PREFIX}search");
    store
        .put(put(
            &definition,
            b"definition",
            Precondition::Absent,
            "format-four",
        ))
        .await
        .unwrap();
    store
        .put(put(
            "ordinary/object",
            b"not a definition",
            Precondition::Absent,
            "ordinary-object",
        ))
        .await
        .unwrap();
    store
        .put(PutRequest {
            key: ObjectKey::new(
                "tenant",
                "another-bucket",
                format!("{INDEX_DEFINITION_PREFIX}other"),
            )
            .unwrap(),
            bytes: b"other definition".to_vec(),
            content_type: Some("application/json".into()),
            mode: PutMode::PutIfAbsent,
            command_id: Some("other-bucket-definition".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();

    let identity = store.resolve_bucket_identity("tenant", "bucket").unwrap();
    let mut considered = Vec::new();

    let page = store
        .list_local_owned_index_definitions(
            identity.tenant_id.0,
            identity.bucket_id.0,
            INDEX_DEFINITION_PREFIX,
            None,
            10,
            |tenant_id, bucket_id, path| {
                considered.push((tenant_id, bucket_id, path.to_owned()));
                true
            },
        )
        .unwrap();
    assert_eq!(page.paths, vec![definition.clone()]);
    // The ownership callback is invoked for every head considered by the
    // bounded iterator. Neither the ordinary head in this bucket nor the
    // definition in another bucket entered the scan range.
    assert_eq!(
        considered,
        vec![(identity.tenant_id.0, identity.bucket_id.0, definition)]
    );

    assert!(matches!(
        store.list_local_owned_index_definitions(
            identity.tenant_id.0,
            identity.bucket_id.0,
            "_keldra/indexes/v2/definitions/",
            None,
            10,
            |_, _, _| true,
        ),
        Err(MutationError::InvalidObjectMutation(_))
    ));
    assert!(matches!(
        store.list_local_owned_index_definitions(
            identity.tenant_id.0,
            identity.bucket_id.0,
            INDEX_DEFINITION_PREFIX,
            Some("_keldra/indexes/v2/definitions/search"),
            10,
            |_, _, _| true,
        ),
        Err(MutationError::InvalidObjectMutation(_))
    ));
}

#[tokio::test]
async fn local_cluster_listing_has_no_total_result_cap_across_pages() {
    let (_temporary, store) = store().await;
    let identity = store.resolve_bucket_identity("tenant", "bucket").unwrap();
    let mut batch = WriteBatch::default();
    for index in 0..(MAX_LIST_OBJECTS + 5) {
        batch.put_cf(
            store.cf(CF_HEADS).unwrap(),
            identity.head_key(&format!("many/{index:04}")),
            serde_json::to_vec(&Head {
                version: VersionId(index as u64 + 1),
                deleted: false,
                mutation_stamp: None,
            })
            .unwrap(),
        );
    }
    store.db.write(batch).unwrap();

    let first = store
        .list_local_owned_objects(
            identity.tenant_id.0,
            identity.bucket_id.0,
            "many/",
            None,
            MAX_LIST_OBJECTS,
            |_, _, _| true,
        )
        .unwrap();
    assert_eq!(first.paths.len(), MAX_LIST_OBJECTS);
    assert!(first.has_more);

    let second = store
        .list_local_owned_objects(
            identity.tenant_id.0,
            identity.bucket_id.0,
            "many/",
            first.paths.last().map(String::as_str),
            MAX_LIST_OBJECTS,
            |_, _, _| true,
        )
        .unwrap();
    assert_eq!(second.paths.len(), 5);
    assert_eq!(second.paths.first().map(String::as_str), Some("many/1000"));
    assert_eq!(second.paths.last().map(String::as_str), Some("many/1004"));
    assert!(!second.has_more);
}

#[tokio::test]
async fn reopen_seeds_version_clock_above_persisted_high_watermark() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let first = store
        .put(put("first", b"one", Precondition::Absent, "first-version"))
        .await
        .unwrap();
    let forced = VersionId(first.version.0 + (1 << 22));
    store
        .db
        .put_cf(
            store.cf(CF_METADATA).unwrap(),
            VERSION_HIGH_WATERMARK_KEY,
            serde_json::to_vec(&forced).unwrap(),
        )
        .unwrap();
    drop(store);

    let reopened = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let next = reopened
        .put(put(
            "second",
            b"two",
            Precondition::Absent,
            "second-version",
        ))
        .await
        .unwrap();
    assert!(next.version > forced);
}
