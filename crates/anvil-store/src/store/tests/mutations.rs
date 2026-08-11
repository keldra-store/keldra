use super::*;
use crate::{ObjectMutation, ObjectMutationContext, PlacementLogId, ReplicaObjectMutationApplied};

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
                store.cf(CF_SMALL_BLOBS).unwrap(),
                blob_reference_key(&rejected),
            )
            .unwrap()
            .is_none()
    );
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
    assert_eq!(counter.puts, 33);
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
async fn prepared_put_keeps_small_bytes_in_memory_and_materializes_large_bytes() {
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
            payload: PreparedPayload::Small { reference, bytes },
            ..
        } => {
            assert!(request.bytes.is_empty());
            assert_eq!(reference.length, first_bytes.len() as u64);
            assert_eq!(bytes, first_bytes);
            assert!(!store.contains_blob(&reference).await.unwrap());
        }
        _ => panic!("small put was not retained in memory"),
    }

    let blob_bytes = vec![9_u8; SMALL_BLOB_MAX_BYTES + 1];
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
            payload: PreparedPayload::Large(reference),
            ..
        } => {
            assert!(request.bytes.is_empty());
            assert_eq!(reference.length, blob_bytes.len() as u64);
            assert_eq!(store.blobs.get(&reference).await.unwrap(), blob_bytes);
            assert!(store.blob_reference_state(&reference).unwrap().is_none());
            assert_eq!(store.db.latest_sequence_number(), sequence_before_prepare);
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
            .read_json::<Version>(CF_VERSIONS, &version_key)
            .unwrap(),
        replica
            .read_json::<Version>(CF_VERSIONS, &version_key)
            .unwrap()
    );
    assert_eq!(
        coordinator
            .read_json::<StoredReceipt>(CF_RECEIPTS, &receipt_key)
            .unwrap(),
        replica
            .read_json::<StoredReceipt>(CF_RECEIPTS, &receipt_key)
            .unwrap()
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
    let (_temporary, coordinator, replica) = two_stores(2).await;
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
    let second = coordinator
        .coordinate_object_mutation(
            BatchOperation::Put(put(
                "replicated",
                b"second",
                Precondition::Version(first_mutation.version.id),
                "second-command",
            )),
            distributed_context(11),
        )
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
    assert!(
        coordinator
            .read_local_change(first_mutation.stamp.source_journal_position)
            .unwrap()
            .is_none()
    );
    assert!(
        coordinator
            .read_json::<Version>(CF_VERSIONS, &mutation_version_key(&first_mutation))
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
    let baseline_reference = baseline_version.blob.as_ref().unwrap();
    let baseline_lifecycle = coordinator
        .blob_reference_state(baseline_reference)
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
        serde_json::to_vec(&baseline_version).unwrap(),
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
            .blob_reference_state(baseline_reference)
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
async fn bulk_publishes_identical_large_payloads_in_one_rocksdb_batch() {
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
    // The complete payload file is sealed before the one metadata commit;
    // object publication, count, journal entry and cursor are one WAL batch.
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
async fn rejected_large_inline_put_leaves_only_an_age_gated_orphan() {
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
    assert!(store.blobs.contains(&reference).await.unwrap());
    let modified = blob_file_path(&store, &reference)
        .metadata()
        .unwrap()
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert_eq!(
        store.collect_blob_garbage_at(modified + 999).await.unwrap(),
        0
    );
    assert!(store.blobs.contains(&reference).await.unwrap());
    assert_eq!(
        store
            .collect_blob_garbage_at(modified + 1_000)
            .await
            .unwrap(),
        1
    );
    assert!(!store.blobs.contains(&reference).await.unwrap());
}

#[tokio::test]
async fn bulk_loads_each_distinct_bucket_policy_once() {
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
        2
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
                store.cf(CF_SMALL_BLOBS).unwrap(),
                blob_reference_key(small_reference),
            )
            .unwrap()
            .unwrap()
            .as_slice(),
        small.as_slice()
    );
    assert!(!store.blobs.contains(small_reference).await.unwrap());
    assert!(
        store
            .db
            .get_cf(
                store.cf(CF_SMALL_BLOBS).unwrap(),
                blob_reference_key(large_version.blob.as_ref().unwrap()),
            )
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .blobs
            .contains(large_version.blob.as_ref().unwrap())
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
