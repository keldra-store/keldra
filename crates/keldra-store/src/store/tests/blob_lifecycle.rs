use super::super::blob_references::blob_gc_due_key;
use super::*;
use crate::{BlobGcBudget, BlobGcCursor};

fn deliver_retirement_effects(store: &Store, blob: &BlobRef, count: usize) {
    let mut batch = WriteBatch::default();
    let mut pending = PendingBlobReferences::new();
    let now = now_unix_millis().unwrap();
    for _ in 0..count {
        let (key, state) = store
            .prepare_blob_reference_retirement(blob, &pending, now)
            .unwrap();
        store
            .stage_blob_reference_update(&mut batch, &mut pending, key, state)
            .unwrap();
    }
    store.db.write(batch).unwrap();
}

#[test]
fn blob_reference_state_is_exactly_twenty_five_bytes() {
    let state = BlobReferenceState {
        ref_count: 1,
        flags: AWAITING_PUBLISH,
        created_at: 11,
        updated_at: 13,
    };
    let encoded = encode_blob_reference_state(state);
    assert_eq!(encoded.len(), 25);
    assert_eq!(decode_blob_reference_state(&encoded).unwrap(), state);

    let mut unknown_flag = encoded;
    unknown_flag[8] = 1 << 7;
    assert!(matches!(
        decode_blob_reference_state(&unknown_flag),
        Err(MutationError::Storage(message)) if message.contains("unknown flags")
    ));
    assert!(decode_blob_reference_state(&encoded[..24]).is_err());

    let mut invalid_reservation = encoded;
    invalid_reservation[..8].copy_from_slice(&2_u64.to_be_bytes());
    assert!(matches!(
        decode_blob_reference_state(&invalid_reservation),
        Err(MutationError::Storage(message))
            if message.contains("exactly one reservation")
    ));
}

#[test]
fn blob_gc_due_keys_sort_by_update_time_before_artifact_identity() {
    let earlier_state = BlobReferenceState {
        ref_count: 0,
        flags: 0,
        created_at: 1,
        updated_at: 10,
    };
    let later_state = BlobReferenceState {
        updated_at: 11,
        ..earlier_state
    };
    let lexically_late = BlobRef {
        hash: [0xff; 32],
        length: 1,
    };
    let lexically_early = BlobRef {
        hash: [0; 32],
        length: 1,
    };

    let earlier = blob_gc_due_key(&blob_reference_key(&lexically_late), earlier_state)
        .unwrap()
        .unwrap();
    let later = blob_gc_due_key(&blob_reference_key(&lexically_early), later_state)
        .unwrap()
        .unwrap();

    assert!(earlier < later);
}

#[tokio::test]
async fn sealing_creates_one_reservation_and_reuse_only_refreshes_it() {
    let (_temporary, store) = store().await;
    let blob = store.stage_blob(b"sealed once").await.unwrap();
    let first = store.blob_reference_state(&blob).unwrap().unwrap();
    assert_eq!(first.ref_count, 1);
    assert_eq!(first.flags, AWAITING_PUBLISH);
    assert_eq!(first.created_at, first.updated_at);
    let first_due = blob_gc_due_key(&blob_reference_key(&blob), first)
        .unwrap()
        .unwrap();
    assert!(
        store
            .db
            .get_cf(store.cf(CF_BLOB_GC_DUE).unwrap(), &first_due)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store
            .db
            .get_cf(
                store.cf(CF_BLOB_REFERENCES).unwrap(),
                blob_reference_key(&blob),
            )
            .unwrap()
            .unwrap()
            .len(),
        25
    );

    store
        .reserve_sealed_blob(&blob, first.updated_at + 10)
        .unwrap();
    let refreshed = store.blob_reference_state(&blob).unwrap().unwrap();
    assert_eq!(refreshed.ref_count, 1);
    assert_eq!(refreshed.flags, AWAITING_PUBLISH);
    assert_eq!(refreshed.created_at, first.created_at);
    assert_eq!(refreshed.updated_at, first.updated_at + 10);
    let refreshed_due = blob_gc_due_key(&blob_reference_key(&blob), refreshed)
        .unwrap()
        .unwrap();
    assert!(
        store
            .db
            .get_cf(store.cf(CF_BLOB_GC_DUE).unwrap(), first_due)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .db
            .get_cf(store.cf(CF_BLOB_GC_DUE).unwrap(), refreshed_due)
            .unwrap()
            .is_some()
    );
    let changes = store.scan_local_changes(0, 10).unwrap();
    assert_eq!(changes.len(), 2);
    for (change, expected_revision) in changes.iter().zip([first.updated_at, refreshed.updated_at])
    {
        let LocalChange::ContentLifecycleChanged(change) = change else {
            panic!("sealed lifecycle update must be journaled")
        };
        assert_eq!(change.blob_identity, blob_reference_key(&blob));
        assert_eq!(change.revision, expected_revision);
        assert!(change.reference_deltas.is_empty());
    }
}

#[tokio::test]
async fn streamed_seal_retains_memory_without_a_filesystem_spool_while_waiting_for_commit() {
    let (temporary, store) = store().await;
    let bytes = vec![0x5a; SMALL_BLOB_MAX_BYTES + 1];
    let expected = blob_reference_for_bytes(&bytes);
    let mut upload = store.begin_blob_upload().await.unwrap();
    upload.write(&bytes).await.unwrap();

    let commit_guard = store.commit_lock.lock().await;
    let sealing_store = store.clone();
    let sealing = tokio::spawn(async move { sealing_store.seal_blob_upload(upload).await });
    tokio::task::yield_now().await;
    assert!(!store.contains_blob(&expected).await.unwrap());
    assert!(!sealing.is_finished());
    assert!(!temporary.path().join("blobs/.upload-spool").exists());

    drop(commit_guard);
    assert_eq!(sealing.await.unwrap().unwrap(), expected);
    assert!(!temporary.path().join("blobs/.upload-spool").exists());
    let state = store.blob_reference_state(&expected).unwrap().unwrap();
    assert_eq!(state.ref_count, 1);
    assert_eq!(state.flags, AWAITING_PUBLISH);
    assert!(state.created_at <= state.updated_at);
}

#[tokio::test]
async fn concurrent_seal_refresh_prevents_a_selected_blob_from_being_collected() {
    let temporary = tempfile::tempdir().unwrap();
    let store =
        Store::open(StoreOptions::new(temporary.path(), 1).with_awaiting_publish_ttl_seconds(1))
            .await
            .unwrap();
    let bytes = vec![0x5a; SMALL_BLOB_MAX_BYTES + 1];
    let blob = store.stage_blob(&bytes).await.unwrap();
    let published = store
        .publish(publish("stale", blob.clone(), "publish-stale"))
        .await
        .unwrap();
    store
        .delete(DeleteRequest {
            key: key("stale"),
            precondition: Precondition::Version(published.version),
            command_id: Some("delete-stale".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    // Derived-source retention now delays the physical -1 until its durable
    // release event is delivered to this payload owner.
    deliver_retirement_effects(&store, &blob, 1);
    let retired = store.blob_reference_state(&blob).unwrap().unwrap();
    assert_eq!(retired.ref_count, 0);

    let mut upload = store.begin_blob_upload().await.unwrap();
    upload.write(&bytes).await.unwrap();
    // GC may discover this exact zero-reference state without holding the
    // commit fence. A concurrent seal that refreshes the lifecycle record must
    // win the exact reread and keep the bytes reachable.
    let selected = retired;
    let sealing_store = store.clone();
    let sealing = tokio::spawn(async move { sealing_store.seal_blob_upload(upload).await });
    assert_eq!(sealing.await.unwrap().unwrap(), blob);
    assert!(
        !store
            .remove_blob_gc_reference_if_unchanged(
                &blob_reference_key(&blob),
                selected,
                selected.updated_at + store.awaiting_publish_ttl_millis,
            )
            .await
            .unwrap()
    );
    let refreshed = store.blob_reference_state(&blob).unwrap().unwrap();
    assert_eq!(refreshed.ref_count, 1);
    assert_eq!(refreshed.flags, AWAITING_PUBLISH);
    assert!(store.contains_blob(&blob).await.unwrap());
}

#[tokio::test]
async fn small_blob_boundary_and_streamed_seal_use_only_rocksdb() {
    let (_temporary, store) = store().await;
    let boundary_bytes = vec![7_u8; SMALL_BLOB_MAX_BYTES];
    let boundary = store.stage_blob(&boundary_bytes).await.unwrap();
    assert_eq!(boundary.length, SMALL_BLOB_MAX_BYTES as u64);
    assert_eq!(
        store.read_blob_bytes(&boundary).await.unwrap(),
        boundary_bytes
    );
    assert!(store.contains_blob(&boundary).await.unwrap());
    let mut reader = store.open_blob(&boundary).await.unwrap();
    let mut read_back = Vec::new();
    let mut chunk = [0_u8; 4_096];
    loop {
        let read = reader.read(&mut chunk).await.unwrap();
        if read == 0 {
            break;
        }
        read_back.extend_from_slice(&chunk[..read]);
    }
    assert_eq!(read_back, boundary_bytes);

    let mut upload = store.begin_blob_upload().await.unwrap();
    upload.write(b"streamed small payload").await.unwrap();
    let streamed = store.seal_blob_upload(upload).await.unwrap();
    assert_eq!(
        store.read_blob_bytes(&streamed).await.unwrap(),
        b"streamed small payload"
    );
    assert!(store.read_complete_manifest(&streamed).unwrap().is_some());

    let large_bytes = vec![9_u8; SMALL_BLOB_MAX_BYTES + 1];
    let large = store.stage_blob(&large_bytes).await.unwrap();
    assert!(store.contains_blob(&large).await.unwrap());
    assert!(
        store
            .db
            .get_cf(
                store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(),
                complete_artifact_key(&large),
            )
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn publication_consumes_then_increments_without_counting_replays() {
    let (_temporary, store) = store().await;
    let blob = store.stage_blob(b"shared payload").await.unwrap();
    let first_request = publish("first", blob.clone(), "first-command");
    let outcomes = store
        .bulk_write(vec![
            BatchOperation::Publish(first_request.clone()),
            BatchOperation::Publish(publish("second", blob.clone(), "second-command")),
        ])
        .await;
    assert!(outcomes.iter().all(|outcome| {
        outcome
            .result
            .as_ref()
            .is_ok_and(|receipt| !receipt.replayed)
    }));
    let published_state = store.blob_reference_state(&blob).unwrap().unwrap();
    assert_eq!(published_state.ref_count, 2);
    assert_eq!(published_state.flags, 0);
    assert!(
        store
            .db
            .iterator_cf(store.cf(CF_BLOB_GC_DUE).unwrap(), IteratorMode::Start)
            .next()
            .is_none()
    );

    let replay = store.publish(first_request).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(
        store.blob_reference_state(&blob).unwrap().unwrap(),
        published_state
    );
}

#[tokio::test]
async fn retirement_reaches_zero_but_gc_waits_for_the_inactivity_ttl() {
    let (_temporary, store) = store().await;
    let blob = store.stage_blob(b"retired payload").await.unwrap();
    store
        .publish(publish("first", blob.clone(), "first-command"))
        .await
        .unwrap();
    store
        .publish(publish("second", blob.clone(), "second-command"))
        .await
        .unwrap();

    let mut pending = PendingBlobReferences::new();
    let mut batch = WriteBatch::default();
    for now in [100_u64, 101] {
        let (key, state) = store
            .prepare_blob_reference_retirement(&blob, &pending, now)
            .unwrap();
        store
            .stage_blob_reference_update(&mut batch, &mut pending, key, state)
            .unwrap();
    }
    store.db.write(batch).unwrap();
    let retired = store.blob_reference_state(&blob).unwrap().unwrap();
    assert_eq!(retired.ref_count, 0);
    assert_eq!(
        store
            .collect_blob_garbage_at(retired.updated_at + store.awaiting_publish_ttl_millis - 1,)
            .await
            .unwrap(),
        0
    );
    assert!(store.read_complete_manifest(&blob).unwrap().is_some());
    assert_eq!(
        store
            .collect_blob_garbage_at(retired.updated_at + store.awaiting_publish_ttl_millis,)
            .await
            .unwrap(),
        1
    );
    assert!(store.blob_reference_state(&blob).unwrap().is_none());
    assert!(store.read_complete_manifest(&blob).unwrap().is_none());
}

#[tokio::test]
async fn deleting_source_and_clone_releases_both_shared_references_before_gc() {
    let (_temporary, store) = store().await;
    let source_key = key("clone-gc-source");
    let source = store
        .put(put(
            source_key.path(),
            b"shared clone payload",
            Precondition::Absent,
            "clone-gc-source",
        ))
        .await
        .unwrap();
    let source_version = store
        .version_metadata(&source_key, source.version)
        .unwrap()
        .unwrap();
    let blob = source_version.blob.unwrap();
    let destination_key = key("clone-gc-destination");
    let cloned = store
        .clone_object(CloneRequest {
            source: source_key.clone(),
            source_version: source.version,
            destination: destination_key.clone(),
            blob: blob.clone(),
            content_type: source_version.content_type,
            mode: PutMode::PutIfAbsent,
            command_id: Some("clone-gc".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .blob_reference_state(&blob)
            .unwrap()
            .unwrap()
            .ref_count,
        2
    );

    for (key, version, command) in [
        (source_key, source.version, "delete-clone-gc-source"),
        (
            destination_key,
            cloned.version,
            "delete-clone-gc-destination",
        ),
    ] {
        store
            .delete(DeleteRequest {
                key,
                precondition: Precondition::Version(version),
                command_id: Some(command.into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();
    }
    deliver_retirement_effects(&store, &blob, 2);
    let retired = store.blob_reference_state(&blob).unwrap().unwrap();
    assert_eq!(retired.ref_count, 0);
    assert_eq!(
        store
            .collect_blob_garbage_at(retired.updated_at + store.awaiting_publish_ttl_millis)
            .await
            .unwrap(),
        1
    );
    assert!(!store.contains_blob(&blob).await.unwrap());
}

#[tokio::test]
async fn due_order_applies_the_ttl_configured_after_restart() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("store");
    let reference;
    let updated_at;
    {
        let store =
            Store::open(StoreOptions::new(&root, 1).with_awaiting_publish_ttl_seconds(60 * 60))
                .await
                .unwrap();
        reference = store
            .stage_blob(&vec![0x6c; SMALL_BLOB_MAX_BYTES + 1])
            .await
            .unwrap();
        updated_at = store
            .blob_reference_state(&reference)
            .unwrap()
            .unwrap()
            .updated_at;
    }

    let reopened = Store::open(StoreOptions::new(&root, 1).with_awaiting_publish_ttl_seconds(1))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .collect_blob_garbage_at(updated_at + 999)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        reopened
            .collect_blob_garbage_at(updated_at + 1_000)
            .await
            .unwrap(),
        1
    );
    assert!(!reopened.contains_blob(&reference).await.unwrap());
}

#[tokio::test]
async fn blob_gc_cursor_spreads_collection_across_hard_record_budgets() {
    let temporary = tempfile::tempdir().unwrap();
    let store =
        Store::open(StoreOptions::new(temporary.path(), 1).with_awaiting_publish_ttl_seconds(1))
            .await
            .unwrap();
    let first = store.stage_blob(b"bounded garbage one").await.unwrap();
    let second = store.stage_blob(b"bounded garbage two").await.unwrap();
    let now = [first, second]
        .iter()
        .map(|reference| {
            store
                .blob_reference_state(reference)
                .unwrap()
                .unwrap()
                .updated_at
        })
        .max()
        .unwrap()
        + store.awaiting_publish_ttl_millis;
    let budget = BlobGcBudget::new(1, 1_024, std::time::Duration::from_secs(1)).unwrap();
    let mut cursor = BlobGcCursor::default();

    let first_tick = store
        .collect_blob_garbage_tick_at(&mut cursor, budget, now)
        .await
        .unwrap();
    assert_eq!(first_tick.inspected_records, 1);
    assert!(first_tick.inspected_bytes <= budget.max_bytes);
    assert_eq!(first_tick.removed, 1);
    assert!(!first_tick.cycle_complete);

    let second_tick = store
        .collect_blob_garbage_tick_at(&mut cursor, budget, now)
        .await
        .unwrap();
    assert_eq!(second_tick.inspected_records, 1);
    assert!(second_tick.inspected_bytes <= budget.max_bytes);
    assert_eq!(second_tick.removed, 1);
    assert!(!second_tick.cycle_complete);
}

#[tokio::test]
async fn gc_uses_durable_due_order_without_a_filesystem_inventory() {
    let temporary = tempfile::tempdir().unwrap();
    let store =
        Store::open(StoreOptions::new(temporary.path(), 1).with_awaiting_publish_ttl_seconds(1))
            .await
            .unwrap();
    let awaiting = store.stage_blob(b"awaiting").await.unwrap();
    let state = store.blob_reference_state(&awaiting).unwrap().unwrap();
    assert_eq!(
        store
            .collect_blob_garbage_at(state.updated_at + 999)
            .await
            .unwrap(),
        0
    );
    assert!(store.contains_blob(&awaiting).await.unwrap());
    assert_eq!(
        store
            .collect_blob_garbage_at(state.updated_at + 1_000)
            .await
            .unwrap(),
        1
    );
    assert!(store.blob_reference_state(&awaiting).unwrap().is_none());
    assert!(!store.contains_blob(&awaiting).await.unwrap());
}

#[tokio::test]
async fn identical_seals_share_one_reservation_and_zero_count_content_can_be_reused() {
    let temporary = tempfile::tempdir().unwrap();
    let store =
        Store::open(StoreOptions::new(temporary.path(), 1).with_awaiting_publish_ttl_seconds(1))
            .await
            .unwrap();
    let first = store.stage_blob(b"shared bytes").await.unwrap();
    let initial = store.blob_reference_state(&first).unwrap().unwrap();
    let second = store.stage_blob(b"shared bytes").await.unwrap();
    assert_eq!(second, first);
    let resealed = store.blob_reference_state(&first).unwrap().unwrap();
    assert_eq!(resealed.ref_count, 1);
    assert_eq!(resealed.flags, AWAITING_PUBLISH);
    assert_eq!(resealed.created_at, initial.created_at);
    assert!(resealed.updated_at >= initial.updated_at);

    let first_receipt = store
        .publish(publish("first", first.clone(), "publish-first"))
        .await
        .unwrap();
    let second_receipt = store
        .publish(publish("second", first.clone(), "publish-second"))
        .await
        .unwrap();
    assert_eq!(
        store
            .blob_reference_state(&first)
            .unwrap()
            .unwrap()
            .ref_count,
        2
    );
    for (path, version, command) in [
        ("first", first_receipt.version, "delete-first"),
        ("second", second_receipt.version, "delete-second"),
    ] {
        store
            .delete(DeleteRequest {
                key: key(path),
                precondition: Precondition::Version(version),
                command_id: Some(command.into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();
    }
    deliver_retirement_effects(&store, &first, 2);
    let retired = store.blob_reference_state(&first).unwrap().unwrap();
    assert_eq!(retired.ref_count, 0);
    assert_eq!(
        store
            .collect_blob_garbage_at(retired.updated_at + 999)
            .await
            .unwrap(),
        0
    );

    let reused = store.stage_blob(b"shared bytes").await.unwrap();
    assert_eq!(reused, first);
    let reserved_again = store.blob_reference_state(&reused).unwrap().unwrap();
    assert_eq!(reserved_again.ref_count, 1);
    assert_eq!(reserved_again.flags, AWAITING_PUBLISH);
    assert_eq!(reserved_again.created_at, initial.created_at);
    store
        .publish(publish("third", reused.clone(), "publish-third"))
        .await
        .unwrap();
    let published_again = store.blob_reference_state(&reused).unwrap().unwrap();
    assert_eq!(published_again.ref_count, 1);
    assert_eq!(published_again.flags, 0);
}
