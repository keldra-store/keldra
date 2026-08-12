use crate::{
    BatchOperation, BucketPolicy, DefinitionKind, DefinitionMutationIntent, Durability, ObjectKey,
    ObjectMutationContext, ObjectMutationGovernance, ObjectVersioning, PlacementLogId, PutMode,
    PutRequest, StoreOptions,
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

fn context(index: u64) -> ObjectMutationContext {
    ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 3, index },
        serving_fence_term: 3,
    }
}

async fn populated_store(path: &std::path::Path) -> Store {
    let store = Store::open(StoreOptions::new(path, 1)).await.unwrap();
    store
        .enable_bucket_versioning("tenant", "bucket")
        .await
        .unwrap();
    for (index, (path, value, command)) in [
        ("a", b"one".as_slice(), "a-one"),
        ("a", b"two".as_slice(), "a-two"),
        ("b", b"three".as_slice(), "b-three"),
    ]
    .into_iter()
    .enumerate()
    {
        store
            .coordinate_object_mutation(
                BatchOperation::Put(put(path, value, command)),
                context(index as u64 + 1),
            )
            .await
            .unwrap();
    }
    store
}

fn export_all(store: &Store, page_size: u32) -> Vec<ObjectRecordExport> {
    let mut cursor = None;
    let mut records = Vec::new();
    loop {
        let page = store
            .export_object_records(cursor.as_ref(), page_size, MAX_OBJECT_RECORD_EXPORT_BYTES)
            .unwrap();
        assert!(!page.records.is_empty() || page.next_cursor.is_none());
        records.extend(page.records);
        let Some(next) = page.next_cursor else {
            return records;
        };
        cursor = Some(next);
    }
}

fn path_record(records: &[ObjectRecordExport], path: &str) -> ObjectPathSnapshot {
    records
        .iter()
        .find_map(|record| match record {
            ObjectRecordExport::ExactPath(record) if record.exact_path == path => {
                Some(record.clone())
            }
            _ => None,
        })
        .unwrap()
}

#[tokio::test]
async fn bounded_pages_install_replay_and_survive_restart_without_payload_bytes() {
    let temporary = tempfile::tempdir().unwrap();
    let source_path = temporary.path().join("source");
    let target_path = temporary.path().join("target");
    let source = populated_store(&source_path).await;

    let records = export_all(&source, 1);
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, ObjectRecordExport::ExactPath(_)))
            .count(),
        2
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, ObjectRecordExport::Receipt(_)))
            .count(),
        3
    );
    let a = path_record(&records, "a");
    assert_eq!(a.versions.len(), 2);
    assert_eq!(
        source
            .export_object_path_record(a.tenant_id, a.bucket_id, "a")
            .unwrap(),
        Some(a.clone())
    );

    let target = Store::open(StoreOptions::new(&target_path, 2))
        .await
        .unwrap();
    for record in &records {
        let applied = target
            .install_quorum_reconciled_object_record(record)
            .await
            .unwrap();
        assert!(!applied.replayed);
        assert!(applied.retained);
    }
    assert_eq!(target.local_watch_status().unwrap().tail, 0);
    assert_eq!(export_all(&target, 1), records);
    for record in &records {
        assert!(
            target
                .install_quorum_reconciled_object_record(record)
                .await
                .unwrap()
                .replayed
        );
    }

    drop(target);
    let target = Store::open(StoreOptions::new(&target_path, 2))
        .await
        .unwrap();
    assert_eq!(export_all(&target, 2), records);
    assert_eq!(
        target
            .export_object_path_record(a.tenant_id, a.bucket_id, "a")
            .unwrap(),
        Some(a)
    );
}

#[tokio::test]
async fn install_rejects_conflicts_and_tampered_records() {
    let temporary = tempfile::tempdir().unwrap();
    let source = populated_store(&temporary.path().join("source")).await;
    let target = Store::open(StoreOptions::new(temporary.path().join("target"), 2))
        .await
        .unwrap();
    let records = export_all(&source, MAX_OBJECT_RECORD_EXPORT_RECORDS);
    let initial = ObjectRecordExport::ExactPath(path_record(&records, "a"));
    target
        .install_quorum_reconciled_object_record(&initial)
        .await
        .unwrap();

    source
        .coordinate_object_mutation(
            BatchOperation::Put(put("a", b"newer", "a-newer")),
            context(20),
        )
        .await
        .unwrap();
    let current = source
        .export_object_path_record(initial.tenant_id(), initial.bucket_id(), "a")
        .unwrap()
        .unwrap();
    assert_eq!(
        target
            .install_quorum_reconciled_object_record(&ObjectRecordExport::ExactPath(current))
            .await
            .unwrap_err(),
        ObjectSnapshotError::SnapshotConflict
    );

    let mut malformed = path_record(&records, "a");
    malformed.versions.reverse();
    assert!(matches!(
        target
            .install_quorum_reconciled_object_record(&ObjectRecordExport::ExactPath(malformed))
            .await,
        Err(ObjectSnapshotError::InvalidRecord(_))
    ));

    let mut receipt = records
        .iter()
        .find_map(|record| match record {
            ObjectRecordExport::Receipt(receipt) => Some(receipt.clone()),
            _ => None,
        })
        .unwrap();
    receipt.input_fingerprint[0] ^= 1;
    assert!(matches!(
        target
            .install_quorum_reconciled_object_record(&ObjectRecordExport::Receipt(receipt))
            .await,
        Err(ObjectSnapshotError::InvalidRecord(_))
    ));
}

#[tokio::test]
async fn export_validates_cursors_limits_and_record_size() {
    let temporary = tempfile::tempdir().unwrap();
    let source = populated_store(temporary.path()).await;
    assert_eq!(
        ObjectRecordCursor::from_token("not-a-cursor").unwrap_err(),
        ObjectSnapshotError::InvalidCursor
    );
    assert!(matches!(
        source.export_object_records(None, 0, MAX_OBJECT_RECORD_EXPORT_BYTES),
        Err(ObjectSnapshotError::InvalidExportLimit(_))
    ));
    assert!(matches!(
        source.export_object_records(None, 1, 1),
        Err(ObjectSnapshotError::ExportRecordTooLarge { .. })
    ));
    assert!(matches!(
        source.export_object_path_record(0, 1, "a"),
        Err(ObjectSnapshotError::InvalidRecord(_))
    ));
}

#[tokio::test]
async fn definition_locator_survives_replica_replay_handoff_and_read_repair() {
    let temporary = tempfile::tempdir().unwrap();
    let source = Store::open(StoreOptions::new(temporary.path().join("source"), 1))
        .await
        .unwrap();
    let (tenant_id, bucket_id) = source.resolve_bucket_ids("tenant", "bucket").unwrap();
    let governance = ObjectMutationGovernance {
        tenant_id,
        bucket_id,
        versioning: ObjectVersioning::Unversioned,
        policy: BucketPolicy::default(),
    };
    let path = "_anvil/indexes/v3/definitions/search";
    let intent = DefinitionMutationIntent::new(DefinitionKind::Index, 41).unwrap();
    let coordinated = source
        .coordinate_definition_object_mutation_with_governance(
            BatchOperation::Put(PutRequest {
                mode: PutMode::PutIfAbsent,
                ..put(path, b"definition", "create-definition")
            }),
            governance,
            context(1),
            intent,
        )
        .await
        .unwrap();
    let mutation = coordinated.mutation.as_ref().unwrap();
    let expected = source
        .export_object_path_record(tenant_id, bucket_id, path)
        .unwrap()
        .unwrap();
    assert_eq!(
        expected.definition_locator.as_ref().unwrap().definition_id,
        41
    );

    let replica = Store::open(StoreOptions::new(temporary.path().join("replica"), 2))
        .await
        .unwrap();
    assert!(
        !replica
            .apply_object_mutation_replica(mutation)
            .await
            .unwrap()
            .replayed
    );
    assert!(
        replica
            .apply_object_mutation_replica(mutation)
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(
        replica
            .definition_locator(DefinitionKind::Index, tenant_id, bucket_id, path)
            .unwrap(),
        expected.definition_locator.clone()
    );

    let handoff = Store::open(StoreOptions::new(temporary.path().join("handoff"), 3))
        .await
        .unwrap();
    handoff
        .install_quorum_reconciled_object_record(&ObjectRecordExport::ExactPath(expected.clone()))
        .await
        .unwrap();
    assert_eq!(
        handoff
            .definition_locator(DefinitionKind::Index, tenant_id, bucket_id, path)
            .unwrap(),
        expected.definition_locator.clone()
    );

    let repaired = Store::open(StoreOptions::new(temporary.path().join("repair"), 4))
        .await
        .unwrap();
    repaired
        .repair_object_path_snapshot(tenant_id, bucket_id, path, None, Some(&expected))
        .await
        .unwrap();
    assert_eq!(
        repaired
            .definition_locator(DefinitionKind::Index, tenant_id, bucket_id, path)
            .unwrap(),
        expected.definition_locator.clone()
    );
}
