use super::*;

#[tokio::test]
async fn conditional_authz_batch_receipt_replays_and_conflicts() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let base_revision = bind_default_document_schema(&storage, 42).await;
    let permit = ready_authz_permit(&storage, 42, "node-a").await;
    let schema_binding_key = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("schema_binding"),
        CoreMetaTuplePart::I64(42),
        CoreMetaTuplePart::Utf8("default"),
    ])
    .unwrap();
    let options = crate::persistence::AuthzTupleBatchWriteOptions {
        authz_realm_id: "default".to_string(),
        operation_id: Some("provision-access-1".to_string()),
        expected_revision: Some(base_revision),
        schema_binding_precondition: Some(crate::persistence::AuthzSchemaBindingPrecondition {
            tuple_key: schema_binding_key.clone(),
            expected_payload_hash: None,
        }),
    };
    let writes = || {
        vec![
            AuthzTupleWrite {
                tenant_id: 42,
                namespace: "realm__default__document",
                object_id: "alpha",
                relation: "viewer",
                subject_kind: "user",
                subject_id: "alice",
                caveat_hash: "",
                operation: "add",
                written_by: "app:writer",
                reason: "provision",
            },
            AuthzTupleWrite {
                tenant_id: 42,
                namespace: "realm__default__document",
                object_id: "beta",
                relation: "viewer",
                subject_kind: "user",
                subject_id: "alice",
                caveat_hash: "",
                operation: "add",
                written_by: "app:writer",
                reason: "provision",
            },
        ]
    };

    let first = write_authz_tuple_batch_conditionally_with_permit(
        &storage,
        writes(),
        &options,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    assert!(!first.replayed);
    assert_eq!(first.records.len(), 2);
    assert!(
        first
            .records
            .iter()
            .all(|record| record.revision == base_revision + 1)
    );
    assert_eq!(
        CoreMetaStore::open(storage.core_store_meta_path())
            .unwrap()
            .scan_prefix_page(CF_AUTHZ, TABLE_AUTHZ_IDEMPOTENCY_RECEIPT_ROW, &[], None, 2,)
            .unwrap()
            .len(),
        1,
        "the receipt must be committed with the tuple journal batch"
    );

    let second_options = crate::persistence::AuthzTupleBatchWriteOptions {
        operation_id: Some("provision-access-2".to_string()),
        expected_revision: Some(base_revision + 1),
        ..options.clone()
    };
    let second = write_authz_tuple_batch_conditionally_with_permit(
        &storage,
        vec![AuthzTupleWrite {
            tenant_id: 42,
            namespace: "realm__default__document",
            object_id: "gamma",
            relation: "viewer",
            subject_kind: "user",
            subject_id: "bob",
            caveat_hash: "",
            operation: "add",
            written_by: "app:writer",
            reason: "provision",
        }],
        &second_options,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    assert_eq!(second.records[0].revision, base_revision + 2);
    let current_head = crate::authz_head::read(&storage, 42).await.unwrap().head;
    assert_eq!(current_head.committed_revision, (base_revision + 2) as u64);
    assert_eq!(current_head.schema_revision, base_revision as u64);
    assert_eq!(current_head.tuple_revision, (base_revision + 2) as u64);

    let replay = write_authz_tuple_batch_conditionally_with_permit(
        &storage,
        writes(),
        &options,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.records.len(), first.records.len());
    assert_eq!(
        replay
            .records
            .iter()
            .map(|record| record.record_hash.as_str())
            .collect::<Vec<_>>(),
        first
            .records
            .iter()
            .map(|record| record.record_hash.as_str())
            .collect::<Vec<_>>()
    );
    assert!(
        replay
            .records
            .iter()
            .all(|record| record.revision == base_revision + 1)
    );
    assert_eq!(
        latest_authz_revision(&storage, 42).await.unwrap(),
        base_revision + 2
    );

    let changed = write_authz_tuple_batch_conditionally_with_permit(
        &storage,
        vec![AuthzTupleWrite {
            object_id: "changed",
            ..writes()[0]
        }],
        &options,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap_err();
    assert!(changed.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<crate::persistence::AuthzTupleBatchWriteError>(),
            Some(crate::persistence::AuthzTupleBatchWriteError::OperationConflict)
        )
    }));

    let stale_options = crate::persistence::AuthzTupleBatchWriteOptions {
        operation_id: Some("stale-operation".to_string()),
        expected_revision: Some(base_revision + 1),
        ..options
    };
    let stale = write_authz_tuple_batch_conditionally_with_permit(
        &storage,
        writes(),
        &stale_options,
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap_err();
    assert!(stale.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<crate::persistence::AuthzTupleBatchWriteError>(),
            Some(
                crate::persistence::AuthzTupleBatchWriteError::RevisionConflict {
                    expected,
                    actual,
                }
            ) if *expected == base_revision + 1 && *actual == base_revision + 2
        )
    }));
}
