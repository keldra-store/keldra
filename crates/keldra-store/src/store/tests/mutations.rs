use super::*;
use crate::{ObjectMutation, ObjectMutationContext, PlacementLogId, ReplicaObjectMutationApplied};

#[tokio::test]
async fn clone_publishes_an_independent_destination_and_binds_replay_to_source() {
    let (_temporary, store) = store().await;
    let source_key = key("clone-source");
    let destination_key = key("clone-destination");
    let source = store
        .put(put(
            "clone-source",
            b"shared bytes",
            Precondition::Absent,
            "source-command",
        ))
        .await
        .unwrap();
    let source_version = store
        .version_metadata(&source_key, source.version)
        .unwrap()
        .unwrap();
    let blob = source_version.blob.clone().unwrap();
    let request = CloneRequest {
        source: source_key.clone(),
        source_version: source.version,
        destination: destination_key.clone(),
        blob: blob.clone(),
        content_type: source_version.content_type.clone(),
        mode: PutMode::PutIfAbsent,
        command_id: Some("clone-command".into()),
        durability: Durability::Local,
    };

    let cloned = store.clone_object(request.clone()).await.unwrap();
    assert!(!cloned.replayed);
    assert_eq!(
        store
            .blob_reference_state(&blob)
            .unwrap()
            .unwrap()
            .ref_count,
        2
    );
    let replay = store.clone_object(request.clone()).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.version, cloned.version);

    let mut conflicting = request;
    conflicting.source = key("another-source");
    assert_eq!(
        store.clone_object(conflicting).await.unwrap_err(),
        MutationError::IdempotencyConflict
    );

    store
        .delete(DeleteRequest {
            key: source_key,
            precondition: Precondition::Version(source.version),
            command_id: Some("delete-source".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    assert_eq!(
        store.get(&destination_key).await.unwrap().unwrap().bytes,
        b"shared bytes"
    );
    store
        .put(put(
            "clone-destination",
            b"destination changed independently",
            Precondition::Version(cloned.version),
            "replace-destination",
        ))
        .await
        .unwrap();
    assert_eq!(
        store.get(&destination_key).await.unwrap().unwrap().bytes,
        b"destination changed independently"
    );
}

#[tokio::test]
async fn clone_destination_cas_is_an_ordinary_put_precondition() {
    let (_temporary, store) = store().await;
    let source = store
        .put(put(
            "clone-cas-source",
            b"source",
            Precondition::Absent,
            "source",
        ))
        .await
        .unwrap();
    let destination = store
        .put(put(
            "clone-cas-destination",
            b"old",
            Precondition::Absent,
            "old",
        ))
        .await
        .unwrap();
    let source_key = key("clone-cas-source");
    let version = store
        .version_metadata(&source_key, source.version)
        .unwrap()
        .unwrap();
    let clone = |expected, command: &str| CloneRequest {
        source: source_key.clone(),
        source_version: source.version,
        destination: key("clone-cas-destination"),
        blob: version.blob.clone().unwrap(),
        content_type: version.content_type.clone(),
        mode: PutMode::PutIfVersion(expected),
        command_id: Some(command.into()),
        durability: Durability::Local,
    };
    assert!(matches!(
        store
            .clone_object(clone(VersionId(destination.version.0 + 1), "wrong"))
            .await,
        Err(MutationError::PreconditionFailed { .. })
    ));
    let applied = store
        .clone_object(clone(destination.version, "right"))
        .await
        .unwrap();
    assert!(applied.version > destination.version);
}

#[tokio::test]
async fn clone_revalidates_the_exact_source_under_the_shared_path_commit_fence() {
    let (_temporary, store) = store().await;
    let source_key = key("clone-retired-source");
    let source = store
        .put(put(
            source_key.path(),
            b"source",
            Precondition::Absent,
            "source",
        ))
        .await
        .unwrap();
    let selected = store
        .version_metadata(&source_key, source.version)
        .unwrap()
        .unwrap();
    let blob = selected.blob.unwrap();
    let request = CloneRequest {
        source: source_key.clone(),
        source_version: source.version,
        destination: key("clone-after-retirement"),
        blob: blob.clone(),
        content_type: selected.content_type,
        mode: PutMode::PutIfAbsent,
        command_id: Some("stale-clone".into()),
        durability: Durability::Local,
    };

    store
        .delete(DeleteRequest {
            key: source_key,
            precondition: Precondition::Version(source.version),
            command_id: Some("retire-source".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();

    assert!(matches!(
        store.clone_object(request).await,
        Err(MutationError::InvalidObjectMutation(message))
            if message.contains("source exact version")
    ));
    assert!(
        store
            .head(&key("clone-after-retirement"))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .blob_reference_state(&blob)
            .unwrap()
            .unwrap()
            .ref_count,
        1,
        "a rejected stale clone must not publish another reference"
    );
}

#[tokio::test]
async fn concurrent_source_delete_and_clone_have_one_safe_linearization() {
    let (_temporary, store) = store().await;
    for ordinal in 0..16 {
        let source_key = key(&format!("clone-race-source-{ordinal}"));
        let destination_key = key(&format!("clone-race-destination-{ordinal}"));
        // Blob reference counts are content-addressed across every object in
        // the Store. Keep each race iteration on an independent BlobRef so
        // this assertion measures only that iteration's source and clone.
        let payload = format!("race payload {ordinal}");
        let source = store
            .put(put(
                source_key.path(),
                payload.as_bytes(),
                Precondition::Absent,
                &format!("race-source-{ordinal}"),
            ))
            .await
            .unwrap();
        let selected = store
            .version_metadata(&source_key, source.version)
            .unwrap()
            .unwrap();
        let blob = selected.blob.unwrap();
        let clone_request = CloneRequest {
            source: source_key.clone(),
            source_version: source.version,
            destination: destination_key.clone(),
            blob: blob.clone(),
            content_type: selected.content_type,
            mode: PutMode::PutIfAbsent,
            command_id: Some(format!("race-clone-{ordinal}")),
            durability: Durability::Local,
        };
        let delete_request = DeleteRequest {
            key: source_key,
            precondition: Precondition::Version(source.version),
            command_id: Some(format!("race-delete-{ordinal}")),
            durability: Durability::Local,
        };
        let clone_store = store.clone();
        let delete_store = store.clone();
        let (cloned, deleted) = tokio::join!(
            clone_store.clone_object(clone_request),
            delete_store.delete(delete_request),
        );
        deleted.unwrap();

        let clone_committed = cloned.is_ok();
        if let Err(error) = cloned {
            assert!(matches!(error, MutationError::InvalidObjectMutation(_)));
        }
        assert_eq!(
            store.head(&destination_key).unwrap().is_some(),
            clone_committed
        );
        assert_eq!(
            store
                .blob_reference_state(&blob)
                .unwrap()
                .unwrap()
                .ref_count,
            1 + u64::from(clone_committed),
            "a committed clone must own its reference before source retirement can win"
        );
    }
}

#[tokio::test]
async fn generic_distributed_mutation_cannot_bypass_clone_atomic_authority() {
    let (_temporary, store) = store().await;
    let source_key = key("distributed-clone-source");
    let source = store
        .put(put(
            source_key.path(),
            b"source",
            Precondition::Absent,
            "distributed-source",
        ))
        .await
        .unwrap();
    let selected = store
        .version_metadata(&source_key, source.version)
        .unwrap()
        .unwrap();
    let error = store
        .coordinate_object_mutation(
            BatchOperation::Clone(CloneRequest {
                source: source_key,
                source_version: source.version,
                destination: key("distributed-clone-destination"),
                blob: selected.blob.unwrap(),
                content_type: selected.content_type,
                mode: PutMode::PutIfAbsent,
                command_id: Some("distributed-clone".into()),
                durability: Durability::Replicated,
            }),
            distributed_context(29),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MutationError::InvalidObjectMutation(message)
            if message.contains("exact retained-version atomic precondition")
    ));
}

#[tokio::test]
async fn idempotency_is_checked_before_the_precondition() {
    let (_temporary, store) = store().await;
    let request = put("a", b"one", Precondition::Absent, "same-command");
    let first = store.put(request.clone()).await.unwrap();
    let replay = store.put(request).await.unwrap();
    assert!(!first.replayed);
    assert!(replay.replayed);
    assert_eq!(replay.version, first.version);
    assert_eq!(replay.fingerprint, first.fingerprint);
    let conflict = store
        .put(put("a", b"different", Precondition::Absent, "same-command"))
        .await
        .unwrap_err();
    assert_eq!(conflict, MutationError::IdempotencyConflict);
}

#[tokio::test]
async fn unexpired_receipts_backpressure_new_commands_but_never_their_replay() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1).with_mutation_receipt_retention(
            MutationReceiptRetention::new(60, 1, 1024 * 1024).unwrap(),
        ),
    )
    .await
    .unwrap();
    let request = put("first", b"one", Precondition::Absent, "first-command");
    let applied = store.put(request.clone()).await.unwrap();
    assert!(applied.replay_guarantee_expires_at_unix_millis > now_unix_millis().unwrap());
    let replay = store.put(request).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(
        replay.replay_guarantee_expires_at_unix_millis,
        applied.replay_guarantee_expires_at_unix_millis
    );
    assert_eq!(
        store
            .put(put(
                "second",
                b"two",
                Precondition::Absent,
                "second-command",
            ))
            .await
            .unwrap_err(),
        MutationError::ReceiptCapacity
    );
    assert!(store.head(&key("second")).unwrap().is_none());
}

#[tokio::test]
async fn production_bulk_waits_for_receipt_capacity_without_losing_the_write() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1).with_mutation_receipt_retention(
            MutationReceiptRetention::new(1, 1, 1024 * 1024).unwrap(),
        ),
    )
    .await
    .unwrap();

    let waiting = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .bulk_write_with_backpressure(vec![
                    BatchOperation::Put(put(
                        "first",
                        b"one",
                        Precondition::Absent,
                        "first-command",
                    )),
                    BatchOperation::Put(put(
                        "second",
                        b"two",
                        Precondition::Absent,
                        "second-command",
                    )),
                ])
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while store.head(&key("first")).unwrap().is_none() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the independent first bulk item should commit before capacity clears");
    assert!(!waiting.is_finished());
    assert!(store.head(&key("first")).unwrap().is_some());
    assert!(store.head(&key("second")).unwrap().is_none());

    let outcomes = tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
        .await
        .expect("receipt expiry should release the blocked writer")
        .unwrap();
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));
    assert!(store.head(&key("second")).unwrap().is_some());
}

#[tokio::test]
async fn an_individually_oversized_receipt_fails_without_waiting_or_mutating() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1)
            .with_mutation_receipt_retention(MutationReceiptRetention::new(60, 10, 1).unwrap()),
    )
    .await
    .unwrap();
    let outcomes = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        store.bulk_write_with_backpressure(vec![BatchOperation::Put(put(
            "oversized",
            b"value",
            Precondition::Absent,
            "oversized-command",
        ))]),
    )
    .await
    .expect("an individually oversized receipt must not wait for capacity");
    assert!(matches!(
        &outcomes[0].result,
        Err(MutationError::ReceiptTooLarge { maximum: 1, .. })
    ));
    assert!(store.head(&key("oversized")).unwrap().is_none());
    assert_eq!(store.local_watch_status().unwrap().tail, 0);
}

#[tokio::test]
async fn expired_receipts_are_pruned_and_the_command_id_can_be_new_again() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1).with_mutation_receipt_retention(
            MutationReceiptRetention::new(1, 1, 1024 * 1024).unwrap(),
        ),
    )
    .await
    .unwrap();
    let first = store
        .put(put("path", b"value", Precondition::Any, "command"))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let second = store
        .put(put("path", b"value", Precondition::Any, "command"))
        .await
        .unwrap();
    assert!(!second.replayed);
    assert!(second.version > first.version);
    assert_eq!(store.mutation_receipt_status().unwrap().entries, 1);
}

#[tokio::test]
async fn capacity_maintenance_prunes_expired_receipts_in_bounded_passes() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1).with_mutation_receipt_retention(
            MutationReceiptRetention::new(1, 2_000, 16 * 1024 * 1024).unwrap(),
        ),
    )
    .await
    .unwrap();
    let outcomes = store
        .bulk_write(
            (0..1_025)
                .map(|index| {
                    BatchOperation::Put(put(
                        &format!("receipt-{index}"),
                        b"value",
                        Precondition::Absent,
                        &format!("command-{index}"),
                    ))
                })
                .collect(),
        )
        .await;
    assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;

    assert!(store.prune_expired_receipts_for_capacity().await.unwrap());
    assert_eq!(store.mutation_receipt_status().unwrap().entries, 1);
    assert!(store.prune_expired_receipts_for_capacity().await.unwrap());
    assert_eq!(store.mutation_receipt_status().unwrap().entries, 0);
    assert!(!store.prune_expired_receipts_for_capacity().await.unwrap());
}

#[tokio::test]
async fn replicated_durability_is_rejected_before_any_head_change() {
    let (_temporary, store) = store().await;

    let mut replicated_put = put("put", b"value", Precondition::Absent, "put-command");
    replicated_put.durability = Durability::Replicated;
    assert_eq!(
        store.put(replicated_put).await.unwrap_err(),
        MutationError::DurabilityUnavailable
    );
    assert!(store.head(&key("put")).unwrap().is_none());

    let blob = store.stage_blob(b"published").await.unwrap();
    let replicated_publish = PublishRequest {
        key: key("publish"),
        blob,
        content_type: Some("application/octet-stream".into()),
        mode: PutMode::PutIfAbsent,
        command_id: Some("publish-command".into()),
        durability: Durability::Replicated,
    };
    assert_eq!(
        store.publish(replicated_publish).await.unwrap_err(),
        MutationError::DurabilityUnavailable
    );
    assert!(store.head(&key("publish")).unwrap().is_none());

    let created = store
        .put(put(
            "delete",
            b"value",
            Precondition::Absent,
            "create-delete-target",
        ))
        .await
        .unwrap();
    let replicated_delete = DeleteRequest {
        key: key("delete"),
        precondition: Precondition::Version(created.version),
        command_id: Some("delete-command".into()),
        durability: Durability::Replicated,
    };
    assert_eq!(
        store.delete(replicated_delete).await.unwrap_err(),
        MutationError::DurabilityUnavailable
    );
    assert_eq!(
        store.head(&key("delete")).unwrap().unwrap().version,
        created.version
    );
}

#[tokio::test]
async fn internal_publish_and_inline_put_share_one_canonical_fingerprint() {
    let (_temporary, store) = store().await;
    let bytes = b"same logical object";
    let blob = store.stage_blob(bytes).await.unwrap();
    let published = store
        .publish(PublishRequest {
            key: key("streamed"),
            blob: blob.clone(),
            content_type: Some("application/octet-stream".into()),
            mode: PutMode::PutIfAbsent,
            command_id: Some("streamed-command".into()),
            durability: Durability::Local,
        })
        .await;
    let published = published.unwrap();
    let replay = store
        .put(put(
            "streamed",
            bytes,
            Precondition::Absent,
            "streamed-command",
        ))
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.version, published.version);
    assert_eq!(replay.fingerprint, published.fingerprint);

    let inline = store
        .put(put("bulk", bytes, Precondition::Absent, "bulk-command"))
        .await
        .unwrap();
    let replay = store
        .publish(PublishRequest {
            key: key("bulk"),
            blob,
            content_type: Some("application/octet-stream".into()),
            mode: PutMode::PutIfAbsent,
            command_id: Some("bulk-command".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.version, inline.version);
    assert_eq!(replay.fingerprint, inline.fingerprint);
}

#[tokio::test]
async fn create_once_policy_applies_to_every_write_surface() {
    let (_temporary, store) = store().await;
    store
        .set_bucket_policy(
            "tenant",
            "bucket",
            BucketPolicy {
                immutable_prefixes: vec!["ledger".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .put(put(
                "ledger/entry-1",
                b"entry",
                Precondition::Absent,
                "ordinary-entry",
            ))
            .await
            .unwrap_err(),
        MutationError::Immutable
    );
    assert!(store.head(&key("ledger/entry-1")).unwrap().is_none());
    let first = store
        .put(immutable_put("ledger/entry-1", b"entry", "entry"))
        .await
        .unwrap();
    let identical = store
        .put(immutable_put(
            "ledger/entry-1",
            b"entry",
            "same-entry-new-command",
        ))
        .await
        .unwrap();
    assert_eq!(identical.version, first.version);
    assert_eq!(
        store
            .put(put(
                "ledger/entry-1",
                b"replacement",
                Precondition::Version(first.version),
                "replace",
            ))
            .await
            .unwrap_err(),
        MutationError::Immutable
    );
    assert_eq!(
        store
            .put(immutable_put("mutable/entry", b"entry", "wrong-policy"))
            .await
            .unwrap_err(),
        MutationError::ImmutablePolicyRequired
    );
    assert_eq!(
        store
            .delete(DeleteRequest {
                key: key("ledger/entry-1"),
                precondition: Precondition::Version(first.version),
                command_id: Some("delete".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap_err(),
        MutationError::Immutable
    );
}

#[tokio::test]
async fn an_exact_tombstone_version_can_be_used_to_recreate() {
    let (_temporary, store) = store().await;
    let first = store
        .put(put("a", b"one", Precondition::Absent, "create"))
        .await
        .unwrap();
    let deleted = store
        .delete(DeleteRequest {
            key: key("a"),
            precondition: Precondition::Version(first.version),
            command_id: Some("delete".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    let recreated = store
        .put(put(
            "a",
            b"two",
            Precondition::Version(deleted.version),
            "recreate",
        ))
        .await
        .unwrap();
    assert!(recreated.version > deleted.version);
}

#[tokio::test]
async fn bulk_returns_per_item_results_and_persists_successes_once() {
    let (_temporary, store) = store().await;
    let outcomes = store
        .bulk_write(vec![
            BatchOperation::Put(put("a", b"a", Precondition::Absent, "a")),
            BatchOperation::Put(put("a", b"bad", Precondition::Absent, "bad")),
            BatchOperation::Put(put("b", b"b", Precondition::Absent, "b")),
        ])
        .await;
    assert!(outcomes[0].result.is_ok());
    assert!(matches!(
        outcomes[1].result,
        Err(MutationError::PreconditionFailed { .. })
    ));
    assert!(outcomes[2].result.is_ok());
    assert_eq!(store.get(&key("a")).await.unwrap().unwrap().bytes, b"a");
    assert_eq!(store.get(&key("b")).await.unwrap().unwrap().bytes, b"b");
    let rejected = blob_reference_for_bytes(b"bad");
    assert!(
        store
            .db
            .get_cf(
                store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(),
                complete_artifact_key(&rejected),
            )
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn pathological_bulk_of_hundreds_of_values_above_64k_uses_integrated_storage() {
    const OPERATIONS: usize = 807;
    const PAYLOAD_BYTES: usize = 63_016_190;
    let (temporary, store) = store().await;
    let base = PAYLOAD_BYTES / OPERATIONS;
    let remainder = PAYLOAD_BYTES % OPERATIONS;
    let mut references = Vec::with_capacity(OPERATIONS);
    let mut operations = Vec::with_capacity(OPERATIONS);

    for ordinal in 0..OPERATIONS {
        let length = base + usize::from(ordinal < remainder);
        assert!(length > SMALL_BLOB_MAX_BYTES);
        let mut bytes = vec![0x5a; length];
        bytes[..8].copy_from_slice(&(ordinal as u64).to_be_bytes());
        references.push(blob_reference_for_bytes(&bytes));
        operations.push(BatchOperation::Put(put(
            &format!("osv/{ordinal}"),
            &bytes,
            Precondition::Absent,
            &format!("osv-{ordinal}"),
        )));
    }

    let outcomes = store.bulk_write(operations).await;
    assert_eq!(outcomes.len(), OPERATIONS);
    assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));

    let payload_cf = store.cf(CF_PAYLOAD_ARTIFACTS).unwrap();
    store.db.flush_cf(payload_cf).unwrap();
    for reference in &references {
        assert!(
            store
                .db
                .get_cf(payload_cf, complete_artifact_key(reference))
                .unwrap()
                .is_some()
        );
        assert!(store.read_complete_manifest(reference).unwrap().is_some());
        let encoded_hash = hex::encode(reference.hash);
        assert!(
            !temporary
                .path()
                .join("blobs")
                .join(&encoded_hash[..2])
                .join(encoded_hash)
                .exists()
        );
    }

    assert_eq!(
        store
            .db
            .iterator_cf(payload_cf, IteratorMode::Start)
            .count(),
        OPERATIONS
    );
    let physical_payload_files = std::fs::read_dir(temporary.path().join("blobs"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
        .count();
    assert!(
        (1..=4).contains(&physical_payload_files),
        "807 logical payloads unexpectedly produced {physical_payload_files} physical payload files"
    );
}

#[tokio::test]
async fn bulk_repeated_path_cas_is_evaluated_in_input_order() {
    let (_temporary, store) = store().await;
    let outcomes = store
        .bulk_write(vec![
            BatchOperation::Put(put("same", b"first", Precondition::Absent, "first")),
            BatchOperation::Put(put("same", b"rejected", Precondition::Absent, "second")),
            BatchOperation::Put(put("same", b"last", Precondition::Any, "third")),
        ])
        .await;

    let first = outcomes[0].result.as_ref().unwrap();
    assert!(matches!(
        outcomes[1].result,
        Err(MutationError::PreconditionFailed {
            current: Some(current)
        }) if current == first.version
    ));
    let last = outcomes[2].result.as_ref().unwrap();
    assert!(last.version > first.version);
    assert_eq!(
        store.get(&key("same")).await.unwrap().unwrap().bytes,
        b"last"
    );
}

#[tokio::test]
async fn bulk_repeated_command_replays_the_pending_receipt() {
    let (_temporary, store) = store().await;
    let operation =
        BatchOperation::Put(put("same", b"value", Precondition::Absent, "same-command"));
    let before = store.local_watch_status().unwrap().tail;
    let outcomes = store.bulk_write(vec![operation.clone(), operation]).await;

    let first = outcomes[0].result.as_ref().unwrap();
    let replay = outcomes[1].result.as_ref().unwrap();
    assert!(!first.replayed);
    assert!(replay.replayed);
    assert_eq!(replay.version, first.version);
    assert_eq!(store.local_watch_status().unwrap().tail, before + 1);
}

#[tokio::test]
async fn bulk_wal_contains_one_high_watermark_and_replay_adds_no_write() {
    let (_temporary, store) = store().await;
    store.resolve_bucket_identity("tenant", "bucket").unwrap();
    let operations = vec![
        BatchOperation::Put(put("a", b"a", Precondition::Absent, "a")),
        BatchOperation::Put(put("b", b"b", Precondition::Absent, "b")),
        BatchOperation::Put(put("c", b"c", Precondition::Absent, "c")),
    ];
    let before = store.db.latest_sequence_number();
    let outcomes = store.bulk_write(operations.clone()).await;
    assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));

    let updates = store
        .db
        .get_updates_since(before)
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(updates.len(), 1);
    let mut counter = WalOperationCounter::default();
    updates[0].1.iterate_cf(&mut counter);
    // Three small raw values, blob lifecycle records, versions, heads, receipts,
    // receipt-expiry indexes, invalidations and bucket journal routes, plus one
    // version watermark, five watch counters, the locally-applied reference
    // cursor and two receipt counters. All metadata moves in this one physical
    // batch rather than once per mutation.
    assert_eq!(counter.puts, 36);
    assert_eq!(counter.high_watermark_puts, 1);
    assert_eq!(counter.invalidation_metadata_puts, 5);
    assert_eq!(counter.receipt_metadata_puts, 2);
    assert_eq!(counter.deletes, 0);
    assert_eq!(counter.merges, 0);

    let expected_high_watermark = outcomes
        .iter()
        .map(|outcome| outcome.result.as_ref().unwrap().version)
        .max()
        .unwrap();
    assert_eq!(
        store
            .read_json::<VersionId>(CF_METADATA, VERSION_HIGH_WATERMARK_KEY)
            .unwrap(),
        Some(expected_high_watermark)
    );

    let sequence_after_first_write = store.db.latest_sequence_number();
    let replay = store.bulk_write(operations).await;
    assert!(replay.iter().all(|outcome| {
        outcome
            .result
            .as_ref()
            .is_ok_and(|receipt| receipt.replayed)
    }));
    assert_eq!(
        store.db.latest_sequence_number(),
        sequence_after_first_write
    );
}

#[tokio::test]
async fn prepared_put_keeps_inline_bytes_in_memory_and_reuses_installed_bytes() {
    let (_temporary, store) = store().await;
    let identity = store.resolve_bucket_identity("tenant", "bucket").unwrap();
    let first_bytes = b"small payload".to_vec();
    let first = store
        .prepare(
            BatchOperation::Put(put("first", &first_bytes, Precondition::Absent, "first")),
            identity,
            false,
        )
        .await
        .unwrap();
    match first {
        PreparedOperation::Put {
            request,
            payload: PreparedPayload::Inline { reference, bytes },
            ..
        } => {
            assert!(request.bytes.is_empty());
            assert_eq!(reference.length, first_bytes.len() as u64);
            assert_eq!(bytes, first_bytes);
            assert!(!store.contains_blob(&reference).await.unwrap());
        }
        _ => panic!("small put was not retained in memory"),
    }

    let blob_bytes = vec![9_u8; PAYLOAD_ARTIFACT_CHUNK_BYTES + 1];
    let sequence_before_prepare = store.db.latest_sequence_number();
    let blob = store
        .prepare(
            BatchOperation::Put(put("blob", &blob_bytes, Precondition::Absent, "blob")),
            identity,
            false,
        )
        .await
        .unwrap();
    match blob {
        PreparedOperation::Put {
            request,
            payload: PreparedPayload::Installed(reference),
            ..
        } => {
            assert!(request.bytes.is_empty());
            assert_eq!(reference.length, blob_bytes.len() as u64);
            assert_eq!(store.read_blob_bytes(&reference).await.unwrap(), blob_bytes);
            let state = store.blob_reference_state(&reference).unwrap().unwrap();
            assert_eq!((state.ref_count, state.flags), (1, AWAITING_PUBLISH));
            assert!(store.db.latest_sequence_number() > sequence_before_prepare);
        }
        _ => panic!("large put was not durably materialized"),
    }
}

fn distributed_context(index: u64) -> ObjectMutationContext {
    ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 7, index },
        serving_fence_term: 7,
    }
}

async fn two_stores(watch_entries: u64) -> (TempDir, Store, Store) {
    let temporary = tempfile::tempdir().unwrap();
    let watch_retention = WatchRetention::new(watch_entries, 1024 * 1024).unwrap();
    let coordinator = Store::open(
        StoreOptions::new(temporary.path().join("coordinator"), 1)
            .with_watch_retention(watch_retention),
    )
    .await
    .unwrap();
    let replica = Store::open(
        StoreOptions::new(temporary.path().join("replica"), 2)
            .with_watch_retention(watch_retention),
    )
    .await
    .unwrap();
    (temporary, coordinator, replica)
}

fn mutation_identity(mutation: &ObjectMutation) -> BucketIdentity {
    BucketIdentity {
        tenant_id: TenantId(mutation.tenant_id),
        bucket_id: BucketId(mutation.bucket_id),
    }
}

fn mutation_version_key(mutation: &ObjectMutation) -> Vec<u8> {
    let identity = mutation_identity(mutation);
    let mut encoded = identity.head_key(&mutation.exact_path);
    encoded.push(0);
    encoded.extend_from_slice(&mutation.version.id.0.to_be_bytes());
    encoded
}

fn assert_same_mutation_metadata(coordinator: &Store, replica: &Store, mutation: &ObjectMutation) {
    let identity = mutation_identity(mutation);
    let head_key = identity.head_key(&mutation.exact_path);
    let version_key = mutation_version_key(mutation);
    let receipt_key = receipt_key(identity, &mutation.command_id);
    assert_eq!(
        coordinator.read_json::<Head>(CF_HEADS, &head_key).unwrap(),
        replica.read_json::<Head>(CF_HEADS, &head_key).unwrap()
    );
    assert_eq!(
        coordinator
            .read_json::<StoredVersion>(CF_VERSIONS, &version_key)
            .unwrap(),
        replica
            .read_json::<StoredVersion>(CF_VERSIONS, &version_key)
            .unwrap()
    );
    assert_eq!(
        coordinator.read_stored_receipt(&receipt_key).unwrap(),
        replica.read_stored_receipt(&receipt_key).unwrap()
    );
}

#[tokio::test]
async fn distributed_metadata_coordination_retains_only_awaiting_source_content() {
    let (_temporary, coordinator, replica) = two_stores(16).await;
    let cases = [
        ("small-source", b"small source".to_vec()),
        ("large-source", vec![0x5a; SMALL_BLOB_MAX_BYTES + 1]),
    ];
    for (index, (path, bytes)) in cases.into_iter().enumerate() {
        let coordinated = coordinator
            .coordinate_object_mutation(
                BatchOperation::Put(put(
                    path,
                    &bytes,
                    Precondition::Absent,
                    &format!("source-{index}"),
                )),
                distributed_context(40),
            )
            .await
            .unwrap();
        let mutation = coordinated.mutation.unwrap();
        assert_eq!(mutation.format, crate::LEGACY_OBJECT_MUTATION_FORMAT);
        assert!(mutation.alias_snapshot.is_none());
        let legacy_wire = serde_json::to_vec(&mutation).unwrap();
        let legacy_wire_text = String::from_utf8_lossy(&legacy_wire);
        assert!(!legacy_wire_text.contains("alias_snapshot"));
        assert!(!legacy_wire_text.contains("protected_link_descriptor"));
        let decoded_legacy: ObjectMutation = serde_json::from_slice(&legacy_wire).unwrap();
        decoded_legacy.validate().unwrap();
        assert_eq!(decoded_legacy, mutation);
        let reference = mutation.version.blob.as_ref().unwrap();
        assert_eq!(
            mutation.reference_deltas,
            vec![ReferenceDelta {
                blob: reference.clone(),
                change: 1,
            }]
        );
        let source_state = coordinator
            .blob_reference_state(reference)
            .unwrap()
            .unwrap();
        assert_eq!(source_state.ref_count, 1);
        assert_eq!(source_state.flags, AWAITING_PUBLISH);
        assert_eq!(coordinator.read_blob_bytes(reference).await.unwrap(), bytes);

        replica
            .apply_object_mutation_replica(&mutation)
            .await
            .unwrap();
        assert!(replica.blob_reference_state(reference).unwrap().is_none());
        assert!(!replica.contains_blob(reference).await.unwrap());
    }
}

#[tokio::test]
async fn verified_distributed_publish_does_not_require_payload_on_path_coordinator() {
    let (_temporary, coordinator, replica) = two_stores(16).await;
    let reference = BlobRef {
        hash: *blake3::hash(b"source lives on another active node").as_bytes(),
        length: b"source lives on another active node".len() as u64,
    };
    let request = PublishRequest {
        key: key("remote-source"),
        blob: reference.clone(),
        content_type: Some("application/octet-stream".into()),
        mode: PutMode::PutIfAbsent,
        command_id: Some("remote-source-command".into()),
        durability: Durability::Replicated,
    };

    assert_eq!(
        coordinator
            .coordinate_object_mutation(
                BatchOperation::Publish(request.clone()),
                distributed_context(51),
            )
            .await
            .unwrap_err(),
        MutationError::BlobNotFound
    );

    let coordinated = coordinator
        .coordinate_distributed_publish(request, distributed_context(51))
        .await
        .unwrap();
    let mutation = coordinated.mutation.unwrap();
    assert_eq!(mutation.version.blob, Some(reference.clone()));
    assert_eq!(
        mutation.reference_deltas,
        [ReferenceDelta {
            blob: reference.clone(),
            change: 1,
        }]
    );
    assert!(
        coordinator
            .blob_reference_state(&reference)
            .unwrap()
            .is_none()
    );
    assert!(!coordinator.contains_blob(&reference).await.unwrap());

    replica
        .apply_object_mutation_replica(&mutation)
        .await
        .unwrap();
    assert_same_mutation_metadata(&coordinator, &replica, &mutation);
}

#[tokio::test]
async fn typed_mutation_replicates_exactly_and_retries_after_head_and_journal_move() {
    let (_temporary, coordinator, replica) = two_stores(4).await;
    let first_request = put(
        "replicated",
        b"first",
        Precondition::Absent,
        "first-command",
    );
    let first = coordinator
        .coordinate_object_mutation(
            BatchOperation::Put(first_request.clone()),
            distributed_context(11),
        )
        .await
        .unwrap();
    let first_mutation = first.mutation.clone().unwrap();
    assert_eq!(first_mutation.stamp.source_journal_position, 2);
    assert!(!first.receipt.replayed);
    assert_eq!(
        replica
            .apply_object_mutation_replica(&first_mutation)
            .await
            .unwrap(),
        ReplicaObjectMutationApplied {
            version: first_mutation.version.id,
            replayed: false,
        }
    );
    assert_same_mutation_metadata(&coordinator, &replica, &first_mutation);
    assert_eq!(coordinator.local_watch_status().unwrap().tail, 2);
    assert_eq!(replica.local_watch_status().unwrap().tail, 0);
    assert!(
        replica
            .blob_reference_state(first_mutation.version.blob.as_ref().unwrap())
            .unwrap()
            .is_none()
    );

    let sequence_after_first_apply = replica.db.latest_sequence_number();
    let retry = replica
        .apply_object_mutation_replica(&first_mutation)
        .await
        .unwrap();
    assert!(retry.replayed);
    assert_eq!(
        replica.db.latest_sequence_number(),
        sequence_after_first_apply
    );

    assert!(
        coordinator
            .settle_source_journal_position_if_contiguous(
                first_mutation.stamp.source_id,
                first_mutation.stamp.source_journal_position,
            )
            .await
            .unwrap()
    );
    coordinator
        .advance_source_journal_reference_safe_through(2)
        .await
        .unwrap();
    // Match the production distributed path: seal the payload once, then
    // coordinate its compact publish descriptor.
    let second_put = put(
        "replicated",
        b"second",
        Precondition::Version(first_mutation.version.id),
        "second-command",
    );
    let second_request = PublishRequest {
        key: second_put.key,
        blob: coordinator.stage_blob(&second_put.bytes).await.unwrap(),
        content_type: second_put.content_type,
        mode: second_put.mode,
        command_id: second_put.command_id,
        durability: second_put.durability,
    };
    let second = coordinator
        .coordinate_distributed_publish(second_request, distributed_context(11))
        .await
        .unwrap();
    let second_mutation = second.mutation.clone().unwrap();
    assert_eq!(second_mutation.stamp.source_journal_position, 4);
    replica
        .apply_object_mutation_replica(&second_mutation)
        .await
        .unwrap();
    assert!(
        coordinator
            .settle_source_journal_position_if_contiguous(
                second_mutation.stamp.source_id,
                second_mutation.stamp.source_journal_position,
            )
            .await
            .unwrap()
    );
    assert_same_mutation_metadata(&coordinator, &replica, &second_mutation);
    // Force two bounded proof-backed passes: the payload lifecycle record and
    // then the first object-head record. This deterministically moves the
    // journal and retires the superseded unversioned descriptor without an
    // unbounded retry loop in the test.
    coordinator.wait_for_mutation_capacity().await;
    coordinator.wait_for_mutation_capacity().await;
    assert!(
        coordinator
            .read_local_change(first_mutation.stamp.source_journal_position)
            .unwrap()
            .is_none()
    );
    assert!(
        coordinator
            .read_json::<StoredVersion>(CF_VERSIONS, &mutation_version_key(&first_mutation))
            .unwrap()
            .is_none()
    );
    coordinator
        .advance_source_journal_reference_safe_through(
            second_mutation.stamp.source_journal_position,
        )
        .await
        .unwrap();

    let recovered = coordinator
        .coordinate_object_mutation(BatchOperation::Put(first_request), distributed_context(11))
        .await
        .unwrap();
    assert!(recovered.receipt.replayed);
    assert_eq!(recovered.mutation, Some(first_mutation.clone()));
    let replica_sequence = replica.db.latest_sequence_number();
    assert!(
        replica
            .apply_object_mutation_replica(&first_mutation)
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(replica.db.latest_sequence_number(), replica_sequence);
}

#[tokio::test]
async fn minority_rollback_retry_reapplies_the_same_typed_mutation_to_quorum() {
    let (_temporary, coordinator, replica) = two_stores(16).await;
    let request = put(
        "minority-retry",
        b"value",
        Precondition::Absent,
        "minority-command",
    );
    let first = coordinator
        .coordinate_object_mutation(
            BatchOperation::Put(request.clone()),
            distributed_context(19),
        )
        .await
        .unwrap();
    let mutation = first.mutation.unwrap();

    let observed = coordinator
        .export_object_path_record(mutation.tenant_id, mutation.bucket_id, &mutation.exact_path)
        .unwrap();
    coordinator
        .repair_object_path_snapshot(
            mutation.tenant_id,
            mutation.bucket_id,
            &mutation.exact_path,
            observed.as_ref(),
            None,
        )
        .await
        .unwrap();
    assert!(
        coordinator
            .export_object_path_record(
                mutation.tenant_id,
                mutation.bucket_id,
                &mutation.exact_path,
            )
            .unwrap()
            .is_none()
    );

    let retry = coordinator
        .coordinate_object_mutation(BatchOperation::Put(request), distributed_context(19))
        .await
        .unwrap();
    assert!(retry.receipt.replayed);
    assert_eq!(retry.mutation.as_ref(), Some(&mutation));
    assert!(
        !coordinator
            .apply_object_mutation_replica(&mutation)
            .await
            .unwrap()
            .replayed
    );
    assert!(
        !replica
            .apply_object_mutation_replica(&mutation)
            .await
            .unwrap()
            .replayed
    );

    let committed = coordinator
        .export_object_path_record(mutation.tenant_id, mutation.bucket_id, &mutation.exact_path)
        .unwrap();
    assert_eq!(
        replica
            .export_object_path_record(
                mutation.tenant_id,
                mutation.bucket_id,
                &mutation.exact_path,
            )
            .unwrap(),
        committed
    );
}

#[tokio::test]
async fn receipt_replay_fails_closed_when_current_lineage_is_more_than_one_step_ahead() {
    let (_temporary, coordinator, replica) = two_stores(16).await;
    let first = coordinator
        .coordinate_object_mutation(
            BatchOperation::Put(put("bounded-lineage", b"one", Precondition::Absent, "one")),
            distributed_context(20),
        )
        .await
        .unwrap()
        .mutation
        .unwrap();
    replica.apply_object_mutation_replica(&first).await.unwrap();
    let second = coordinator
        .coordinate_object_mutation(
            BatchOperation::Put(put(
                "bounded-lineage",
                b"two",
                Precondition::Version(first.version.id),
                "two",
            )),
            distributed_context(20),
        )
        .await
        .unwrap()
        .mutation
        .unwrap();
    replica
        .apply_object_mutation_replica(&second)
        .await
        .unwrap();
    let third = coordinator
        .coordinate_object_mutation(
            BatchOperation::Put(put(
                "bounded-lineage",
                b"three",
                Precondition::Version(second.version.id),
                "three",
            )),
            distributed_context(20),
        )
        .await
        .unwrap()
        .mutation
        .unwrap();
    replica.apply_object_mutation_replica(&third).await.unwrap();

    assert_eq!(
        replica
            .apply_object_mutation_replica(&first)
            .await
            .unwrap_err(),
        MutationError::ObjectMutationLineageGap {
            current: Some(third.version.id),
            predecessor: first.stamp.predecessor_version,
        }
    );
}

#[tokio::test]
async fn replica_rejects_lineage_gaps_and_contradictory_siblings() {
    let (_temporary, coordinator, replica) = two_stores(16).await;
    let first = coordinator
        .coordinate_object_mutation(
            BatchOperation::Put(put("lineage", b"one", Precondition::Absent, "one")),
            distributed_context(21),
        )
        .await
        .unwrap()
        .mutation
        .unwrap();
    let second = coordinator
        .coordinate_object_mutation(
            BatchOperation::Put(put(
                "lineage",
                b"two",
                Precondition::Version(first.version.id),
                "two",
            )),
            distributed_context(21),
        )
        .await
        .unwrap()
        .mutation
        .unwrap();

    assert_eq!(
        replica
            .apply_object_mutation_replica(&second)
            .await
            .unwrap_err(),
        MutationError::ObjectMutationLineageGap {
            current: None,
            predecessor: Some(first.version.id),
        }
    );
    replica.apply_object_mutation_replica(&first).await.unwrap();
    replica
        .apply_object_mutation_replica(&second)
        .await
        .unwrap();

    let mut sibling = second.clone();
    sibling.command_id = "sibling".into();
    sibling.version.id = VersionId(second.version.id.0.checked_add(1).unwrap());
    sibling.version.committed_at_unix_millis = sibling
        .version
        .committed_at_unix_millis
        .checked_add(1)
        .unwrap();
    sibling.stamp.source_journal_position += 1;
    sibling.set_computed_fingerprint();
    sibling.validate().unwrap();
    assert_eq!(
        replica
            .apply_object_mutation_replica(&sibling)
            .await
            .unwrap_err(),
        MutationError::ObjectMutationSibling {
            predecessor: Some(first.version.id),
        }
    );
}

#[tokio::test]
async fn first_typed_mutation_accepts_an_unstamped_050_baseline() {
    let (_temporary, coordinator, replica) = two_stores(16).await;
    let baseline = coordinator
        .put(put(
            "upgrade",
            b"baseline",
            Precondition::Absent,
            "legacy-command",
        ))
        .await
        .unwrap();
    let logical_key = key("upgrade");
    let identity = coordinator
        .resolve_bucket_identity(logical_key.tenant(), logical_key.bucket())
        .unwrap();
    let baseline_head = coordinator.head(&logical_key).unwrap().unwrap();
    assert_eq!(baseline_head.mutation_stamp, None);
    let baseline_version = coordinator
        .version_metadata(&logical_key, baseline.version)
        .unwrap()
        .unwrap();
    let baseline_reference = baseline_version.blob.clone().unwrap();
    let baseline_lifecycle = coordinator
        .blob_reference_state(&baseline_reference)
        .unwrap()
        .unwrap();
    assert_eq!(baseline_lifecycle.flags, 0);
    let mut seed = WriteBatch::default();
    seed.put_cf(
        replica.cf(CF_HEADS).unwrap(),
        identity.head_key(logical_key.path()),
        serde_json::to_vec(&baseline_head).unwrap(),
    );
    seed.put_cf(
        replica.cf(CF_VERSIONS).unwrap(),
        version_key(identity, &logical_key, baseline.version),
        serde_json::to_vec(&StoredVersion::new(
            baseline_version,
            StoredVersionRetention::JournalPending,
        ))
        .unwrap(),
    );
    replica.db.write(seed).unwrap();

    let typed = coordinator
        .coordinate_object_mutation(
            BatchOperation::Put(put(
                "upgrade",
                b"distributed",
                Precondition::Version(baseline.version),
                "typed-command",
            )),
            distributed_context(31),
        )
        .await
        .unwrap()
        .mutation
        .unwrap();
    assert_eq!(typed.stamp.predecessor_version, Some(baseline.version));
    assert_eq!(
        coordinator
            .blob_reference_state(&baseline_reference)
            .unwrap()
            .unwrap(),
        baseline_lifecycle
    );
    let new_reference = typed.version.blob.as_ref().unwrap();
    let new_lifecycle = coordinator
        .blob_reference_state(new_reference)
        .unwrap()
        .unwrap();
    assert_eq!(new_lifecycle.ref_count, 1);
    assert_eq!(new_lifecycle.flags, AWAITING_PUBLISH);
    replica.apply_object_mutation_replica(&typed).await.unwrap();
    assert!(
        replica
            .blob_reference_state(new_reference)
            .unwrap()
            .is_none()
    );
    assert_same_mutation_metadata(&coordinator, &replica, &typed);
    assert_eq!(
        replica
            .read_json::<Head>(CF_HEADS, &identity.head_key(logical_key.path()))
            .unwrap()
            .unwrap()
            .mutation_stamp,
        Some(typed.stamp)
    );
}

#[tokio::test]
async fn bulk_publishes_identical_large_payloads_after_durable_reservations() {
    let (temporary, store) = store().await;
    store.resolve_bucket_identity("tenant", "bucket").unwrap();
    let bytes = vec![0x5a; SMALL_BLOB_MAX_BYTES + 1];
    let reference = blob_reference_for_bytes(&bytes);
    let operations = vec![
        BatchOperation::Put(put("first", &bytes, Precondition::Absent, "first-large")),
        BatchOperation::Put(put("second", &bytes, Precondition::Absent, "second-large")),
    ];
    let before = store.db.latest_sequence_number();

    let outcomes = store.bulk_write(operations).await;

    assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));
    let updates = store
        .db
        .get_updates_since(before)
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    // Both modest payloads and their lifecycle records join the object
    // publications, final reference count, journal entries and source cursor
    // in one WAL batch.
    assert_eq!(updates.len(), 1);
    let state = store.blob_reference_state(&reference).unwrap().unwrap();
    assert_eq!(state.ref_count, 2);
    assert_eq!(state.flags, 0);
    let journal = store.local_watch_status().unwrap();
    assert_eq!(
        store.reference_delta_cursor(journal.source_id).unwrap(),
        journal.tail
    );

    drop(store);
    let reopened = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    for path in ["first", "second"] {
        assert_eq!(
            reopened.get(&key(path)).await.unwrap().unwrap().bytes,
            bytes
        );
    }
}

#[tokio::test]
async fn locally_applied_reference_effect_and_source_cursor_share_one_batch() {
    let (_temporary, store) = store().await;
    store.resolve_bucket_identity("tenant", "bucket").unwrap();
    let bytes = vec![0x35; SMALL_BLOB_MAX_BYTES + 1];
    let reference = blob_reference_for_bytes(&bytes);
    let before = store.db.latest_sequence_number();

    store
        .put(put("cursor", &bytes, Precondition::Absent, "cursor-write"))
        .await
        .unwrap();

    let batches = store
        .db
        .get_updates_since(before)
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    // The modest payload, object publication, final count, journal entry and
    // source cursor share one WAL batch.
    assert_eq!(batches.len(), 1);
    assert_eq!(
        store
            .blob_reference_state(&reference)
            .unwrap()
            .unwrap()
            .ref_count,
        1
    );
    let journal = store.local_watch_status().unwrap();
    assert_eq!(
        store.reference_delta_cursor(journal.source_id).unwrap(),
        journal.tail
    );
}

#[tokio::test]
async fn rejected_modest_payload_put_leaves_no_artifact_or_lifecycle() {
    let temporary = tempfile::tempdir().unwrap();
    let store =
        Store::open(StoreOptions::new(temporary.path(), 1).with_awaiting_publish_ttl_seconds(1))
            .await
            .unwrap();
    store
        .put(put("occupied", b"current", Precondition::Absent, "create"))
        .await
        .unwrap();
    let bytes = vec![0x6b; SMALL_BLOB_MAX_BYTES + 1];
    let reference = blob_reference_for_bytes(&bytes);

    let rejected = store
        .put(put(
            "occupied",
            &bytes,
            Precondition::Absent,
            "rejected-large",
        ))
        .await;

    assert!(matches!(
        rejected,
        Err(MutationError::PreconditionFailed { .. })
    ));
    assert!(store.blob_reference_state(&reference).unwrap().is_none());
    assert!(store.read_complete_manifest(&reference).unwrap().is_none());
    assert!(!store.contains_blob(&reference).await.unwrap());
}

#[tokio::test]
async fn bulk_prefetches_distinct_bucket_policies_without_point_lookups() {
    let (_temporary, store) = store().await;
    let put_in = |bucket: &str, path: &str, command: &str| {
        BatchOperation::Put(PutRequest {
            key: ObjectKey::new("tenant", bucket, path).unwrap(),
            bytes: path.as_bytes().to_vec(),
            content_type: None,
            mode: PutMode::PutIfAbsent,
            command_id: Some(command.into()),
            durability: Durability::Local,
        })
    };

    let outcomes = store
        .bulk_write(vec![
            put_in("first", "a", "first-a"),
            put_in("first", "b", "first-b"),
            put_in("second", "a", "second-a"),
            put_in("first", "c", "first-c"),
            put_in("second", "b", "second-b"),
        ])
        .await;

    assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));
    assert_eq!(
        store
            .policy_lookup_count
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn bulk_routes_payloads_to_the_deterministic_small_or_large_plane() {
    let (_temporary, store) = store().await;
    let small = b"small".to_vec();
    let large = vec![9u8; SMALL_BLOB_MAX_BYTES + 1];
    let outcomes = store
        .bulk_write(vec![
            BatchOperation::Put(put("small", &small, Precondition::Absent, "small")),
            BatchOperation::Put(put("large", &large, Precondition::Absent, "large")),
        ])
        .await;
    assert!(outcomes.iter().all(|outcome| outcome.result.is_ok()));
    let small_version = store
        .version_metadata(&key("small"), outcomes[0].result.as_ref().unwrap().version)
        .unwrap()
        .unwrap();
    let large_version = store
        .version_metadata(&key("large"), outcomes[1].result.as_ref().unwrap().version)
        .unwrap()
        .unwrap();
    let small_reference = small_version.blob.as_ref().unwrap();
    assert_eq!(small_reference, &blob_reference_for_bytes(&small));
    assert!(large_version.blob.is_some());
    assert_eq!(
        store
            .db
            .get_cf(
                store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(),
                complete_artifact_key(small_reference),
            )
            .unwrap()
            .unwrap()
            .as_slice(),
        small.as_slice()
    );
    assert!(store.contains_blob(small_reference).await.unwrap());
    assert!(
        store
            .db
            .get_cf(
                store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(),
                complete_artifact_key(large_version.blob.as_ref().unwrap()),
            )
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .contains_blob(large_version.blob.as_ref().unwrap())
            .await
            .unwrap()
    );
    assert_eq!(
        store.get(&key("small")).await.unwrap().unwrap().bytes,
        small
    );
    assert_eq!(
        store.get(&key("large")).await.unwrap().unwrap().bytes,
        large
    );
}
