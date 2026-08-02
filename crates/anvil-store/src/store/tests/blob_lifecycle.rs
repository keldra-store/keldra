use super::*;

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

#[tokio::test]
async fn sealing_creates_one_reservation_and_reuse_only_refreshes_it() {
    let (_temporary, store) = store().await;
    let blob = store.stage_blob(b"sealed once").await.unwrap();
    let first = store.blob_reference_state(&blob).unwrap().unwrap();
    assert_eq!(first.ref_count, 1);
    assert_eq!(first.flags, AWAITING_PUBLISH);
    assert_eq!(first.created_at, first.updated_at);
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
}

#[tokio::test]
async fn streamed_seal_finishes_byte_plane_io_before_waiting_for_commit_fence() {
    let (_temporary, store) = store().await;
    let bytes = vec![0x5a; SMALL_BLOB_MAX_BYTES + 1];
    let expected = blob_reference_for_bytes(&bytes);
    let mut upload = store.begin_blob_upload().await.unwrap();
    upload.write(&bytes).await.unwrap();

    let commit_guard = store.commit_lock.lock().await;
    let sealing_store = store.clone();
    let sealing = tokio::spawn(async move { sealing_store.seal_blob_upload(upload).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if store.blobs.contains(&expected).await.unwrap() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("physical upload must finish before seal waits for the commit fence");
    assert!(!sealing.is_finished());

    drop(commit_guard);
    assert_eq!(sealing.await.unwrap().unwrap(), expected);
    let state = store.blob_reference_state(&expected).unwrap().unwrap();
    assert_eq!(state.ref_count, 1);
    assert_eq!(state.flags, AWAITING_PUBLISH);
    assert!(state.created_at <= state.updated_at);
}

#[tokio::test]
async fn streamed_seal_fails_cleanly_when_gc_wins_before_reservation() {
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
    let retired = store.blob_reference_state(&blob).unwrap().unwrap();
    assert_eq!(retired.ref_count, 0);

    let mut upload = store.begin_blob_upload().await.unwrap();
    upload.write(&bytes).await.unwrap();
    let commit_guard = store.commit_lock.lock().await;
    let sealing_store = store.clone();
    let sealing = tokio::spawn(async move { sealing_store.seal_blob_upload(upload).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if std::fs::read_dir(store.blobs.root().join(".staging"))
                .unwrap()
                .next()
                .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("physical deduplication must finish before seal waits for the commit fence");

    assert_eq!(
        store
            .collect_blob_garbage_at(retired.updated_at + store.awaiting_publish_ttl_millis)
            .unwrap(),
        1
    );
    drop(commit_guard);

    assert_eq!(
        sealing.await.unwrap().unwrap_err(),
        MutationError::BlobNotFound
    );
    assert!(store.blob_reference_state(&blob).unwrap().is_none());
    assert!(!store.contains_blob(&blob).await.unwrap());
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
    assert!(!store.blobs.contains(&boundary).await.unwrap());
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
    assert!(!blob_file_path(&store, &streamed).exists());

    let large_bytes = vec![9_u8; SMALL_BLOB_MAX_BYTES + 1];
    let large = store.stage_blob(&large_bytes).await.unwrap();
    assert!(store.blobs.contains(&large).await.unwrap());
    assert!(
        store
            .db
            .get_cf(
                store.cf(CF_SMALL_BLOBS).unwrap(),
                blob_reference_key(&large)
            )
            .unwrap()
            .is_none()
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
            .unwrap(),
        0
    );
    assert!(store.contains_blob(&blob).await.unwrap());
    assert_eq!(
        store
            .collect_blob_garbage_at(retired.updated_at + store.awaiting_publish_ttl_millis,)
            .unwrap(),
        1
    );
    assert!(store.blob_reference_state(&blob).unwrap().is_none());
    assert!(!store.contains_blob(&blob).await.unwrap());
}

#[tokio::test]
async fn gc_uses_awaiting_inactivity_and_removes_untracked_crash_files() {
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
            .unwrap(),
        0
    );
    assert!(store.contains_blob(&awaiting).await.unwrap());
    assert_eq!(
        store
            .collect_blob_garbage_at(state.updated_at + 1_000)
            .unwrap(),
        1
    );
    assert!(store.blob_reference_state(&awaiting).unwrap().is_none());
    assert!(!store.contains_blob(&awaiting).await.unwrap());

    let orphan = store.blobs.put(b"crash orphan").await.unwrap();
    assert!(store.blob_reference_state(&orphan).unwrap().is_none());
    let encoded_orphan_hash = hex::encode(orphan.hash);
    let orphan_path = store
        .blobs
        .root()
        .join(&encoded_orphan_hash[..2])
        .join(encoded_orphan_hash);
    let orphan_modified = orphan_path
        .metadata()
        .unwrap()
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert_eq!(
        store
            .collect_blob_garbage_at(orphan_modified + 999)
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .collect_blob_garbage_at(orphan_modified + 1_000)
            .unwrap(),
        1
    );
    assert!(!store.blobs.contains(&orphan).await.unwrap());

    let transition = store.stage_blob(b"sealed small transition").await.unwrap();
    store
        .publish(publish(
            "sealed-small-transition",
            transition.clone(),
            "sealed-small-transition",
        ))
        .await
        .unwrap();
    assert_eq!(
        store.blobs.put(b"sealed small transition").await.unwrap(),
        transition
    );
    let transition_path = blob_file_path(&store, &transition);
    let transition_modified = transition_path
        .metadata()
        .unwrap()
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert_eq!(
        store
            .collect_blob_garbage_at(transition_modified + 1_000)
            .unwrap(),
        1
    );
    assert!(!transition_path.exists());
    assert!(store.contains_blob(&transition).await.unwrap());

    let staged = store.blobs.root().join(".staging").join("crash-orphan.tmp");
    std::fs::write(&staged, b"abandoned staging bytes").unwrap();
    let modified = staged
        .metadata()
        .unwrap()
        .modified()
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert_eq!(
        store
            .collect_blob_garbage_at(modified + store.awaiting_publish_ttl_millis)
            .unwrap(),
        1
    );
    assert!(!staged.exists());
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
    let retired = store.blob_reference_state(&first).unwrap().unwrap();
    assert_eq!(retired.ref_count, 0);
    assert_eq!(
        store
            .collect_blob_garbage_at(retired.updated_at + 999)
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
