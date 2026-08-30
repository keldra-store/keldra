use rocksdb::WriteBatch;

use super::*;
use crate::definition_state::DefinitionDeletion;
use crate::{DefinitionMutationIntent, DefinitionOperation, INDEX_DEFINITION_PREFIX};

async fn store() -> (tempfile::TempDir, Store) {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(crate::StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    (temporary, store)
}

fn fence(index: u64) -> PlacementLogId {
    PlacementLogId { term: 3, index }
}

#[test]
fn keys_are_versioned_fixed_width_and_big_endian() {
    let locator = locator_key(
        DefinitionKind::Index,
        0x0102_0304_0506_0708,
        0x1112_1314_1516_1718,
        "indexes/by-path",
    )
    .unwrap();
    assert_eq!(
        locator,
        [
            vec![STORAGE_KEY_FORMAT_VERSION, b'L', 1],
            0x0102_0304_0506_0708_u64.to_be_bytes().to_vec(),
            0x1112_1314_1516_1718_u64.to_be_bytes().to_vec(),
            b"indexes/by-path".to_vec(),
        ]
        .concat()
    );
    assert_eq!(
        assignment_key(DefinitionKind::Accounting, 7, 9, 11).unwrap(),
        [
            vec![STORAGE_KEY_FORMAT_VERSION, b'A', 2],
            7_u64.to_be_bytes().to_vec(),
            9_u64.to_be_bytes().to_vec(),
            11_u64.to_be_bytes().to_vec(),
        ]
        .concat()
        .as_slice()
    );
    assert_eq!(
        checkpoint_key(DefinitionConsumerKind::IndexAssignments, 0x1234).unwrap(),
        [STORAGE_KEY_FORMAT_VERSION, b'C', 1, 0x12, 0x34]
    );
    assert_eq!(
        checkpoint_key(DefinitionConsumerKind::IndexDelivery, 0x1234).unwrap(),
        [STORAGE_KEY_FORMAT_VERSION, b'C', 3, 0x12, 0x34]
    );
    assert_eq!(
        checkpoint_key(DefinitionConsumerKind::AccountingDelivery, 0x1234).unwrap(),
        [STORAGE_KEY_FORMAT_VERSION, b'C', 4, 0x12, 0x34]
    );
    assert_eq!(
        reconciliation_key().as_slice(),
        [STORAGE_KEY_FORMAT_VERSION, b'R']
    );
}

#[tokio::test]
async fn membership_reconciliation_fence_is_durable_and_monotonic_across_restart() {
    let temporary = tempfile::tempdir().unwrap();
    let options = crate::StoreOptions::new(temporary.path(), 1);
    {
        let store = Store::open(options.clone()).await.unwrap();
        assert_eq!(store.definition_reconciliation_fence().unwrap(), None);
        store.complete_definition_reconciliation(fence(17)).unwrap();
        store.complete_definition_reconciliation(fence(17)).unwrap();
        store.complete_definition_reconciliation(fence(18)).unwrap();
        assert_eq!(
            store
                .complete_definition_reconciliation(fence(17))
                .unwrap_err(),
            DefinitionStateError::ReconciliationFenceRegression
        );
    }

    let reopened = Store::open(options).await.unwrap();
    assert_eq!(
        reopened.definition_reconciliation_fence().unwrap(),
        Some(fence(18))
    );
}

#[tokio::test]
async fn membership_reconciliation_fence_rejects_malformed_storage() {
    let (_temporary, store) = store().await;
    let mut malformed = encode_reconciliation_fence(fence(17));
    malformed[0] = VALUE_FORMAT + 1;
    store
        .db
        .put_cf(
            store.definition_state_cf().unwrap(),
            reconciliation_key(),
            malformed,
        )
        .unwrap();
    assert!(matches!(
        store.definition_reconciliation_fence(),
        Err(DefinitionStateError::Malformed(_))
    ));
}

#[test]
fn value_codecs_reject_unknown_versions_and_identity_mismatches() {
    let locator = DefinitionLocator {
        kind: DefinitionKind::Index,
        tenant_id: 7,
        bucket_id: 9,
        definition_id: 11,
        path: "_keldra/indexes/by-path".into(),
        object_version: VersionId(13),
        operation: DefinitionOperation::Upsert,
    };
    let key = locator_key(locator.kind, 7, 9, &locator.path).unwrap();
    let encoded = encode_locator(&locator);
    assert_eq!(decode_locator(&key, &encoded).unwrap(), locator);
    let mut unsupported = encoded;
    unsupported[0] += 1;
    assert!(matches!(
        decode_locator(&key, &unsupported),
        Err(DefinitionStateError::Malformed(_))
    ));
    assert!(matches!(
        decode_locator(&key, &[1; 17]),
        Err(DefinitionStateError::Malformed(_))
    ));

    let assignment = DefinitionAssignment {
        kind: DefinitionKind::Index,
        tenant_id: 7,
        bucket_id: 9,
        definition_id: 11,
        definition_path: "_keldra/indexes/by-path".into(),
        object_version: VersionId(13),
        observed_fence: fence(17),
        rank: 2,
    };
    let key = assignment_key(DefinitionKind::Index, 7, 9, 11).unwrap();
    assert_eq!(
        decode_assignment(&key, &encode_assignment(&assignment).unwrap()).unwrap(),
        assignment
    );

    let checkpoint = DefinitionCheckpoint {
        consumer_kind: DefinitionConsumerKind::IndexDelivery,
        source_id: SourceId {
            node_id: 4,
            source_epoch: [8; 32],
        },
        next_offset: 0,
        observed_fence: fence(17),
    };
    let key = checkpoint_key(checkpoint.consumer_kind, checkpoint.source_id.node_id).unwrap();
    assert_eq!(
        decode_checkpoint(&key, &encode_checkpoint(&checkpoint)).unwrap(),
        checkpoint
    );
}

#[tokio::test]
async fn locator_transition_retains_one_current_state_tombstone_and_recreation_replaces_it() {
    let (_temporary, store) = store().await;
    let intent = DefinitionMutationIntent::new(DefinitionKind::Index, 11).unwrap();
    let transition = DefinitionTransition {
        kind: intent.kind,
        tenant_id: 7,
        bucket_id: 9,
        definition_id: intent.definition_id,
        path: "_keldra/indexes/by-path".into(),
        object_version: VersionId(13),
        operation: DefinitionOperation::Upsert,
    };
    let mut batch = WriteBatch::default();
    store
        .stage_definition_transition(&mut batch, &transition)
        .unwrap();
    store.db.write(batch).unwrap();
    assert_eq!(
        store
            .definition_locator(DefinitionKind::Index, 7, 9, &transition.path)
            .unwrap(),
        Some(transition.locator())
    );

    let mut deletion = transition;
    deletion.object_version = VersionId(14);
    deletion.operation = DefinitionOperation::Delete;
    let mut batch = WriteBatch::default();
    store
        .stage_definition_transition(&mut batch, &deletion)
        .unwrap();
    store.db.write(batch).unwrap();
    assert_eq!(
        store
            .definition_locator(DefinitionKind::Index, 7, 9, &deletion.path)
            .unwrap(),
        Some(deletion.locator())
    );

    let tombstones = store
        .scan_definition_locators_by_bucket(DefinitionKind::Index, 7, 9, None, 10)
        .unwrap();
    assert_eq!(tombstones.locators, vec![deletion.locator()]);

    let mut recreated = deletion;
    recreated.definition_id = 12;
    recreated.object_version = VersionId(15);
    recreated.operation = DefinitionOperation::Upsert;
    let mut batch = WriteBatch::default();
    store
        .stage_definition_transition(&mut batch, &recreated)
        .unwrap();
    store.db.write(batch).unwrap();
    let current = store
        .scan_definition_locators_by_bucket(DefinitionKind::Index, 7, 9, None, 10)
        .unwrap();
    assert_eq!(current.locators, vec![recreated.locator()]);
    assert!(current.next_cursor.is_none());
}

#[tokio::test]
async fn assignment_page_and_checkpoint_commit_and_page_together() {
    let (_temporary, store) = store().await;
    let source = SourceId {
        node_id: 4,
        source_epoch: [8; 32],
    };
    let checkpoint = DefinitionCheckpoint {
        consumer_kind: DefinitionConsumerKind::IndexAssignments,
        source_id: source,
        next_offset: 21,
        observed_fence: fence(19),
    };
    let assignments = (1..=3)
        .map(|definition_id| {
            DefinitionAssignmentMutation::Upsert(DefinitionAssignment {
                kind: DefinitionKind::Index,
                tenant_id: 7,
                bucket_id: 9,
                definition_id,
                definition_path: format!("_keldra/indexes/{definition_id}"),
                object_version: VersionId(30 + definition_id),
                observed_fence: checkpoint.observed_fence,
                rank: (definition_id - 1) as u8,
            })
        })
        .collect::<Vec<_>>();
    store
        .apply_definition_assignment_page(&assignments, &checkpoint)
        .unwrap();
    assert_eq!(
        store
            .definition_checkpoint(checkpoint.consumer_kind, source.node_id)
            .unwrap(),
        Some(checkpoint)
    );

    let first = store.scan_definition_assignments(None, 2).unwrap();
    assert_eq!(first.assignments.len(), 2);
    let second = store
        .scan_definition_assignments(first.next_cursor.as_ref(), 2)
        .unwrap();
    assert_eq!(second.assignments.len(), 1);
    assert!(second.next_cursor.is_none());

    let regression = DefinitionCheckpoint {
        next_offset: 20,
        ..checkpoint
    };
    assert_eq!(
        store
            .apply_definition_assignment_page(&[], &regression)
            .unwrap_err(),
        DefinitionStateError::CheckpointRegression
    );
}

#[tokio::test]
async fn definition_delete_is_delivered_distinctly_and_removes_the_assignment() {
    let (_temporary, store) = store().await;
    let mut changes = store.subscribe_definition_assignment_changes();
    let assignment = DefinitionAssignment {
        kind: DefinitionKind::Index,
        tenant_id: 7,
        bucket_id: 9,
        definition_id: 11,
        definition_path: "_keldra/indices/v4/definitions/example".into(),
        object_version: VersionId(13),
        observed_fence: fence(17),
        rank: 0,
    };
    store
        .apply_definition_assignment_mutations(&[DefinitionAssignmentMutation::Upsert(
            assignment.clone(),
        )])
        .unwrap();
    assert!(matches!(
        changes.recv().await.unwrap().as_slice(),
        [DefinitionAssignmentMutation::Upsert(_)]
    ));

    let deletion = DefinitionDeletion {
        kind: assignment.kind,
        tenant_id: assignment.tenant_id,
        bucket_id: assignment.bucket_id,
        definition_id: assignment.definition_id,
        definition_path: assignment.definition_path,
        object_version: VersionId(14),
        observed_fence: fence(18),
        rank: 0,
    };
    let checkpoint = DefinitionCheckpoint {
        consumer_kind: DefinitionConsumerKind::IndexAssignments,
        source_id: SourceId {
            node_id: 4,
            source_epoch: [8; 32],
        },
        next_offset: 21,
        observed_fence: deletion.observed_fence,
    };
    store
        .apply_definition_assignment_page(
            &[DefinitionAssignmentMutation::Delete(deletion.clone())],
            &checkpoint,
        )
        .unwrap();
    assert_eq!(
        changes.recv().await.unwrap(),
        vec![DefinitionAssignmentMutation::Delete(deletion)]
    );
    assert!(
        store
            .definition_assignment(DefinitionKind::Index, 7, 9, 11)
            .unwrap()
            .is_none()
    );
    let due = store
        .oldest_deleted_definition_cleanup()
        .unwrap()
        .expect("delete delivery must atomically retain cleanup evidence");
    assert_eq!(due.tenant_id, 7);
    assert_eq!(due.bucket_id, 9);
    assert_eq!(due.index_id, 11);
    assert_eq!(due.definition_object_version, VersionId(14));
    assert_eq!(
        due.definition_path,
        "_keldra/indices/v4/definitions/example"
    );
    assert!(due.due_at_unix_millis > 0);
    assert_eq!(
        store
            .definition_checkpoint(checkpoint.consumer_kind, checkpoint.source_id.node_id)
            .unwrap(),
        Some(checkpoint)
    );
}

#[tokio::test]
async fn conditional_assignment_removal_deletes_only_the_exact_observed_value() {
    let (_temporary, store) = store().await;
    let observed = DefinitionAssignment {
        kind: DefinitionKind::Index,
        tenant_id: 7,
        bucket_id: 9,
        definition_id: 11,
        definition_path: format!("{INDEX_DEFINITION_PREFIX}by-path"),
        object_version: VersionId(13),
        observed_fence: fence(17),
        rank: 0,
    };
    store
        .apply_definition_assignment_mutations(&[DefinitionAssignmentMutation::Upsert(
            observed.clone(),
        )])
        .unwrap();

    assert!(
        store
            .remove_definition_assignment_if_matches(&observed)
            .unwrap()
    );
    assert!(
        store
            .definition_assignment(DefinitionKind::Index, 7, 9, 11)
            .unwrap()
            .is_none()
    );
    assert!(
        !store
            .remove_definition_assignment_if_matches(&observed)
            .unwrap()
    );
}

#[tokio::test]
async fn delayed_conditional_removal_preserves_a_concurrent_placement_repair() {
    let (_temporary, store) = store().await;
    let observed = DefinitionAssignment {
        kind: DefinitionKind::Accounting,
        tenant_id: 7,
        bucket_id: 9,
        definition_id: 11,
        definition_path: "_keldra/accounting/definitions/11".into(),
        object_version: VersionId(13),
        observed_fence: fence(17),
        rank: 0,
    };
    store
        .apply_definition_assignment_mutations(&[DefinitionAssignmentMutation::Upsert(
            observed.clone(),
        )])
        .unwrap();

    // Membership reconciliation installs the same authoritative object
    // version under the new fence before delayed cleanup resumes.
    let repaired = DefinitionAssignment {
        observed_fence: fence(18),
        rank: 1,
        ..observed.clone()
    };
    store
        .apply_definition_assignment_mutations(&[DefinitionAssignmentMutation::Upsert(
            repaired.clone(),
        )])
        .unwrap();

    assert!(
        !store
            .remove_definition_assignment_if_matches(&observed)
            .unwrap()
    );
    assert_eq!(
        store
            .definition_assignment(DefinitionKind::Accounting, 7, 9, 11)
            .unwrap(),
        Some(repaired)
    );
}

#[tokio::test]
async fn bucket_locator_and_kind_assignment_scans_never_cross_their_prefix() {
    let (_temporary, store) = store().await;
    let mut batch = WriteBatch::default();
    for (kind, tenant_id, bucket_id, definition_id, path, version) in [
        (DefinitionKind::Index, 7, 9, 11, "indexes/a", 21),
        (DefinitionKind::Index, 7, 9, 12, "indexes/b", 22),
        (DefinitionKind::Index, 7, 10, 13, "indexes/other", 23),
        (DefinitionKind::Accounting, 7, 9, 14, "accounting/a", 24),
    ] {
        store
            .stage_definition_transition(
                &mut batch,
                &DefinitionTransition {
                    kind,
                    tenant_id,
                    bucket_id,
                    definition_id,
                    path: path.into(),
                    object_version: VersionId(version),
                    operation: DefinitionOperation::Upsert,
                },
            )
            .unwrap();
    }
    store.db.write(batch).unwrap();

    let first = store
        .scan_definition_locators_by_bucket(DefinitionKind::Index, 7, 9, None, 1)
        .unwrap();
    assert_eq!(first.locators.len(), 1);
    assert_eq!(first.locators[0].path, "indexes/a");
    let second = store
        .scan_definition_locators_by_bucket(
            DefinitionKind::Index,
            7,
            9,
            first.next_cursor.as_ref(),
            1,
        )
        .unwrap();
    assert_eq!(second.locators.len(), 1);
    assert_eq!(second.locators[0].path, "indexes/b");
    assert!(second.next_cursor.is_none());
    assert_eq!(
        store
            .scan_definition_locators_by_bucket(
                DefinitionKind::Index,
                7,
                10,
                first.next_cursor.as_ref(),
                1,
            )
            .unwrap_err(),
        DefinitionStateError::InvalidCursor
    );

    let assignments = [
        DefinitionAssignmentMutation::Upsert(DefinitionAssignment {
            kind: DefinitionKind::Index,
            tenant_id: 7,
            bucket_id: 9,
            definition_id: 11,
            definition_path: "indexes/a".into(),
            object_version: VersionId(21),
            observed_fence: fence(30),
            rank: 0,
        }),
        DefinitionAssignmentMutation::Upsert(DefinitionAssignment {
            kind: DefinitionKind::Accounting,
            tenant_id: 7,
            bucket_id: 9,
            definition_id: 14,
            definition_path: "accounting/a".into(),
            object_version: VersionId(24),
            observed_fence: fence(30),
            rank: 1,
        }),
    ];
    store
        .apply_definition_assignment_mutations(&assignments)
        .unwrap();
    let indexes = store
        .scan_definition_assignments_by_kind(DefinitionKind::Index, None, 10)
        .unwrap();
    assert_eq!(indexes.assignments.len(), 1);
    assert_eq!(indexes.assignments[0].kind, DefinitionKind::Index);
}

#[tokio::test]
async fn locator_scan_skips_corruption_with_a_bounded_raw_continuation() {
    let (_temporary, store) = store().await;
    let mut batch = WriteBatch::default();
    for (definition_id, path) in [(11, "a"), (13, "c")] {
        store
            .stage_definition_transition(
                &mut batch,
                &DefinitionTransition {
                    kind: DefinitionKind::Index,
                    tenant_id: 7,
                    bucket_id: 9,
                    definition_id,
                    path: path.into(),
                    object_version: VersionId(definition_id + 10),
                    operation: DefinitionOperation::Upsert,
                },
            )
            .unwrap();
    }
    store.db.write(batch).unwrap();

    let mut corrupt_key = locator_bucket_prefix(DefinitionKind::Index, 7, 9);
    corrupt_key.extend_from_slice(&[b'b', 0xff]);
    let placeholder = DefinitionLocator {
        kind: DefinitionKind::Index,
        tenant_id: 7,
        bucket_id: 9,
        definition_id: 12,
        path: "b".into(),
        object_version: VersionId(22),
        operation: DefinitionOperation::Upsert,
    };
    store
        .db
        .put_cf(
            store.definition_state_cf().unwrap(),
            &corrupt_key,
            encode_locator(&placeholder),
        )
        .unwrap();

    let first = store
        .scan_definition_locators_by_bucket(DefinitionKind::Index, 7, 9, None, 2)
        .unwrap();
    assert_eq!(
        first
            .locators
            .iter()
            .map(|locator| locator.path.as_str())
            .collect::<Vec<_>>(),
        ["a"]
    );
    let encoded_cursor = first.next_cursor.unwrap().as_bytes().to_vec();
    assert_eq!(encoded_cursor, corrupt_key);
    let cursor = DefinitionLocatorCursor::from_bytes(encoded_cursor).unwrap();

    let second = store
        .scan_definition_locators_by_bucket(DefinitionKind::Index, 7, 9, Some(&cursor), 2)
        .unwrap();
    assert_eq!(
        second
            .locators
            .iter()
            .map(|locator| locator.path.as_str())
            .collect::<Vec<_>>(),
        ["c"]
    );
    assert!(second.next_cursor.is_none());
}

#[tokio::test]
async fn assignment_scan_skips_corruption_with_a_bounded_raw_continuation() {
    let (_temporary, store) = store().await;
    let mutations = [1_u64, 3]
        .into_iter()
        .map(|definition_id| {
            DefinitionAssignmentMutation::Upsert(DefinitionAssignment {
                kind: DefinitionKind::Index,
                tenant_id: 7,
                bucket_id: 9,
                definition_id,
                definition_path: format!("indexes/{definition_id}"),
                object_version: VersionId(20 + definition_id),
                observed_fence: fence(30),
                rank: 0,
            })
        })
        .collect::<Vec<_>>();
    store
        .apply_definition_assignment_mutations(&mutations)
        .unwrap();

    let mut corrupt_key = assignment_key(DefinitionKind::Index, 7, 9, 2)
        .unwrap()
        .to_vec();
    corrupt_key.push(0);
    store
        .db
        .put_cf(
            store.definition_state_cf().unwrap(),
            &corrupt_key,
            [VALUE_FORMAT],
        )
        .unwrap();

    let first = store
        .scan_definition_assignments_by_kind(DefinitionKind::Index, None, 2)
        .unwrap();
    assert_eq!(
        first
            .assignments
            .iter()
            .map(|assignment| assignment.definition_id)
            .collect::<Vec<_>>(),
        [1]
    );
    let encoded_cursor = first.next_cursor.unwrap().as_bytes().to_vec();
    assert_eq!(encoded_cursor, corrupt_key);
    let cursor = DefinitionAssignmentCursor::from_bytes(encoded_cursor).unwrap();

    let second = store
        .scan_definition_assignments_by_kind(DefinitionKind::Index, Some(&cursor), 2)
        .unwrap();
    assert_eq!(
        second
            .assignments
            .iter()
            .map(|assignment| assignment.definition_id)
            .collect::<Vec<_>>(),
        [3]
    );
    assert!(second.next_cursor.is_none());
}

#[tokio::test]
async fn direct_assignment_transfer_is_idempotent_stale_safe_and_notifies_changes() {
    let (_temporary, store) = store().await;
    let mut changes = store.subscribe_definition_assignment_changes();
    let assignment = DefinitionAssignment {
        kind: DefinitionKind::Index,
        tenant_id: 7,
        bucket_id: 9,
        definition_id: 11,
        definition_path: "indexes/a".into(),
        object_version: VersionId(21),
        observed_fence: fence(30),
        rank: 0,
    };
    let upsert = DefinitionAssignmentMutation::Upsert(assignment.clone());
    store
        .apply_definition_assignment_mutations(std::slice::from_ref(&upsert))
        .unwrap();
    assert_eq!(changes.recv().await.unwrap(), vec![upsert.clone()]);

    store
        .apply_definition_assignment_mutations(std::slice::from_ref(&upsert))
        .unwrap();
    assert!(matches!(
        changes.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    let stale = DefinitionAssignmentMutation::Upsert(DefinitionAssignment {
        object_version: VersionId(20),
        observed_fence: fence(31),
        rank: 2,
        ..assignment.clone()
    });
    store
        .apply_definition_assignment_mutations(&[stale])
        .unwrap();
    assert!(matches!(
        changes.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    assert_eq!(
        store
            .definition_assignment(DefinitionKind::Index, 7, 9, 11)
            .unwrap(),
        Some(assignment.clone())
    );

    store
        .apply_definition_assignment_mutations(&[DefinitionAssignmentMutation::Remove {
            kind: DefinitionKind::Index,
            tenant_id: 7,
            bucket_id: 9,
            definition_id: 11,
            object_version: VersionId(22),
            observed_fence: fence(31),
        }])
        .unwrap();
    assert!(matches!(
        changes.recv().await.unwrap().as_slice(),
        [DefinitionAssignmentMutation::Remove {
            definition_id: 11,
            ..
        }]
    ));
    assert!(
        store
            .definition_assignment(DefinitionKind::Index, 7, 9, 11)
            .unwrap()
            .is_none()
    );

    let newer = DefinitionAssignmentMutation::Upsert(DefinitionAssignment {
        object_version: VersionId(23),
        observed_fence: fence(32),
        ..assignment
    });
    let newer_remove = DefinitionAssignmentMutation::Remove {
        kind: DefinitionKind::Index,
        tenant_id: 7,
        bucket_id: 9,
        definition_id: 11,
        object_version: VersionId(24),
        observed_fence: fence(32),
    };
    store
        .apply_definition_assignment_mutations(&[newer.clone(), newer_remove.clone()])
        .unwrap();
    assert_eq!(changes.recv().await.unwrap(), vec![newer, newer_remove]);
    assert!(
        store
            .definition_assignment(DefinitionKind::Index, 7, 9, 11)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn exact_assignment_snapshot_receiver_starts_after_the_visited_state() {
    let (_temporary, store) = store().await;
    let first = DefinitionAssignment {
        kind: DefinitionKind::Index,
        tenant_id: 7,
        bucket_id: 9,
        definition_id: 11,
        definition_path: "indexes/a".into(),
        object_version: VersionId(21),
        observed_fence: fence(30),
        rank: 0,
    };
    store
        .apply_definition_assignment_mutations(&[DefinitionAssignmentMutation::Upsert(
            first.clone(),
        )])
        .unwrap();

    let mut visited = Vec::new();
    let mut changes = store
        .visit_definition_assignment_snapshot(DefinitionKind::Index, |assignment| {
            visited.push(assignment);
        })
        .unwrap();
    assert_eq!(visited, [first.clone()]);
    assert!(matches!(
        changes.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    let second = DefinitionAssignment {
        definition_id: 12,
        definition_path: "indexes/b".into(),
        ..first
    };
    let mutation = DefinitionAssignmentMutation::Upsert(second);
    store
        .apply_definition_assignment_mutations(std::slice::from_ref(&mutation))
        .unwrap();
    assert_eq!(changes.recv().await.unwrap(), [mutation]);
}
