use super::*;

fn stream_target(stream_id: &str) -> CorePendingMutationTarget {
    test_stream_append_target(stream_id, "tenant:test/bucket:admission", "event.created")
}

#[test]
fn admission_shard_identity_frames_variable_length_target_parts() {
    let left = test_stream_append_target("b/c", "a", "event.created").admission_shard();
    let right = test_stream_append_target("c", "a/b", "event.created").admission_shard();

    assert_ne!(left.key, right.key);
    assert_ne!(left.hash, right.hash);
}

async fn admit_inline(
    store: &CoreStore,
    target: CorePendingMutationTarget,
    mutation_id: &str,
    idempotency_key: Option<&str>,
    payload: &[u8],
) -> CorePendingMutationRecord {
    store
        .admit_core_mutation(
            "stream.append",
            "stream",
            target,
            mutation_id.to_string(),
            idempotency_key.map(str::to_string),
            CorePendingMutationPayload::Inline(payload),
            Vec::new(),
        )
        .await
        .unwrap()
}

async fn admit_landed(
    store: &CoreStore,
    target: CorePendingMutationTarget,
    mutation_id: &str,
    idempotency_key: &str,
    payload: &[u8],
) -> CorePendingMutationRecord {
    store
        .admit_core_mutation(
            "stream.append",
            "stream",
            target,
            mutation_id.to_string(),
            Some(idempotency_key.to_string()),
            CorePendingMutationPayload::Landed(payload),
            Vec::new(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn same_admission_shard_allocates_a_contiguous_sequence() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let target = stream_target("tenant:test/bucket:admission/shared-stream");
    let shard = target.admission_shard();

    let first = admit_inline(
        &store,
        target.clone(),
        "same-shard-first",
        Some("same-shard-first"),
        b"first",
    )
    .await;
    let second = admit_inline(
        &store,
        target.clone(),
        "same-shard-second",
        Some("same-shard-second"),
        b"second",
    )
    .await;

    assert_eq!((first.sequence, second.sequence), (1, 2));
    let state = store.admission_point_state_for_tests(&shard.hash).unwrap();
    assert_eq!(state.0, 2);
    assert_eq!(state.1, 2);
    assert_eq!(state.4, Some(1));
    assert_eq!(store.next_core_mutation_sequence(&target).await.unwrap(), 3);

    store
        .mark_pending_mutation_finalised_unlocked(&first, "committed")
        .await
        .unwrap();
    let state = store.admission_point_state_for_tests(&shard.hash).unwrap();
    assert_eq!(state.1, 1);
    assert_eq!(state.4, Some(2));

    store
        .mark_pending_mutation_finalised_unlocked(&second, "committed")
        .await
        .unwrap();
    assert_eq!(
        store.admission_point_state_for_tests(&shard.hash).unwrap(),
        (2, 0, 0, 0, None, None)
    );
}

#[tokio::test]
async fn mutation_and_idempotency_retries_reuse_one_pending_row() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let target = stream_target("tenant:test/bucket:admission/idempotent-stream");
    let shard = target.admission_shard();
    let payload = vec![0x5a; CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES + 1];

    let first = admit_landed(
        &store,
        target.clone(),
        "idempotency-original",
        "stable-request",
        &payload,
    )
    .await;
    let mutation_retry = admit_landed(
        &store,
        target.clone(),
        "idempotency-original",
        "stable-request",
        &payload,
    )
    .await;
    let idempotency_retry = admit_landed(
        &store,
        target,
        "idempotency-new-mutation-id",
        "stable-request",
        &payload,
    )
    .await;

    assert_eq!(mutation_retry.mutation_id, first.mutation_id);
    assert_eq!(idempotency_retry.mutation_id, first.mutation_id);
    assert_eq!(idempotency_retry.sequence, first.sequence);
    let state = store.admission_point_state_for_tests(&shard.hash).unwrap();
    assert_eq!(state.1, 1);
    assert_eq!(
        store
            .landed_byte_reference_count_for_tests(&shard.hash, &first.landed_bytes[0].sha256,)
            .unwrap(),
        Some(1)
    );
}

#[tokio::test]
async fn admission_publication_exposes_one_complete_recovery_row_set() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let target = stream_target("tenant:test/bucket:admission/crash-boundary");
    let shard = target.admission_shard();
    let payload = vec![0x7c; CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES + 1];
    let record = admit_landed(
        &store,
        target,
        "atomic-admission",
        "atomic-admission-key",
        &payload,
    )
    .await;

    assert!(
        store
            .meta
            .get(
                CF_TRANSACTIONS,
                TABLE_PENDING_MUTATION_ROW,
                &admission_record_key(&shard.hash, record.sequence),
            )
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .read_admission_mutation_head(&shard.hash, &record.mutation_id)
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .read_admission_idempotency_head(
                &shard.hash,
                record.idempotency_key_hash.as_deref().unwrap(),
            )
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .meta
            .get(
                CF_TRANSACTIONS,
                TABLE_LOCAL_ADMISSION_EVIDENCE_ROW,
                &admission_evidence_key(&shard.hash, record.sequence),
            )
            .unwrap()
            .is_some()
    );
    store
        .verify_landed_bytes_ref_row(
            &shard.hash,
            &record.landed_bytes[0].landing_id,
            &record.mutation_id,
            &record.landed_bytes[0].sha256,
            record.landed_bytes[0].length,
            &record.boundary_values,
        )
        .unwrap();
    let pending_hash_input = encode_pending_mutation_hash_input(&record, &[]).unwrap();
    store
        .verify_local_admission_evidence(&record, &pending_hash_input)
        .unwrap();
    assert_eq!(
        store.validate_admission_recovery_state_for_tests().unwrap(),
        1
    );
}

#[tokio::test]
async fn recovery_snapshot_remains_consistent_across_concurrent_admission() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let target = stream_target("tenant:test/bucket:admission/recovery-admit-race");
    let shard = target.admission_shard();
    admit_inline(
        &store,
        target.clone(),
        "recovery-admit-first",
        Some("recovery-admit-first"),
        b"first",
    )
    .await;

    let snapshot = store.meta.read_snapshot();
    admit_inline(
        &store,
        target,
        "recovery-admit-second",
        Some("recovery-admit-second"),
        b"second",
    )
    .await;

    store
        .validate_admission_recovery_snapshot(&snapshot)
        .unwrap();
    assert_eq!(
        store
            .admission_point_state_for_tests(&shard.hash)
            .unwrap()
            .1,
        2
    );
    store.validate_admission_recovery_state_for_tests().unwrap();
}

#[tokio::test]
async fn recovery_snapshot_remains_consistent_across_concurrent_finalisation() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let target = stream_target("tenant:test/bucket:admission/recovery-finalise-race");
    let first = admit_inline(
        &store,
        target.clone(),
        "recovery-finalise-first",
        Some("recovery-finalise-first"),
        b"first",
    )
    .await;
    admit_inline(
        &store,
        target.clone(),
        "recovery-finalise-second",
        Some("recovery-finalise-second"),
        b"second",
    )
    .await;

    let snapshot = store.meta.read_snapshot();
    store
        .mark_pending_mutation_finalised_unlocked(&first, "committed")
        .await
        .unwrap();

    store
        .validate_admission_recovery_snapshot(&snapshot)
        .unwrap();
    assert_eq!(
        store
            .admission_point_state_for_tests(&target.admission_shard().hash)
            .unwrap()
            .1,
        1
    );
    store.validate_admission_recovery_state_for_tests().unwrap();
}

#[tokio::test]
async fn stale_recovery_snapshot_does_not_quarantine_newly_referenced_landed_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let payload = vec![0xa5; CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES + 1];
    let record = admit_landed(
        &store,
        stream_target("tenant:test/bucket:admission/recovery-landed-race"),
        "recovery-landed-race",
        "recovery-landed-race",
        &payload,
    )
    .await;

    store
        .reconcile_landed_bytes_after_rocksdb_recovery(&BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(
        store
            .read_landed_bytes(&record.landed_bytes[0])
            .await
            .unwrap(),
        payload
    );
}

#[tokio::test]
async fn crash_recovery_rejects_a_torn_admission_projection() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let target = stream_target("tenant:test/bucket:admission/torn-projection");
    let shard = target.admission_shard();
    let record = admit_inline(
        &store,
        target,
        "torn-admission",
        Some("torn-admission-key"),
        b"durable",
    )
    .await;

    store
        .meta
        .delete(
            CF_MATERIALISATION,
            TABLE_MATERIALISATION_CURSOR_ROW,
            &admission_mutation_head_key(&shard.hash, &record.mutation_id),
        )
        .unwrap();

    let error = store
        .validate_admission_recovery_state_for_tests()
        .unwrap_err();
    assert!(error.to_string().contains("missing its mutation head"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_shard_admissions_allocate_each_sequence_once() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(
        CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
            .await
            .unwrap(),
    );
    let target = stream_target("tenant:test/bucket:admission/concurrent-stream");
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = Vec::new();
    for index in 0..8 {
        let store = store.clone();
        let target = target.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let mutation_id = format!("same-shard-concurrent-{index}");
            let idempotency_key = format!("same-shard-concurrent-key-{index}");
            let payload = format!("payload-{index}");
            admit_inline(
                &store,
                target,
                &mutation_id,
                Some(&idempotency_key),
                payload.as_bytes(),
            )
            .await
            .sequence
        }));
    }

    let mut sequences = futures_util::future::join_all(tasks)
        .await
        .into_iter()
        .map(|result| result.unwrap())
        .collect::<Vec<_>>();
    sequences.sort_unstable();
    assert_eq!(sequences, (1..=8).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_admission_shards_progress_while_one_shard_is_locked() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(
        CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
            .await
            .unwrap(),
    );
    let blocked_target = stream_target("tenant:test/bucket:admission/blocked-stream");
    let free_target = stream_target("tenant:test/bucket:admission/free-stream");
    let blocked_shard = blocked_target.admission_shard();
    assert_ne!(blocked_shard.hash, free_target.admission_shard().hash);

    let guard = store
        .acquire_named_lock("admission-shard", &blocked_shard.hash)
        .await
        .unwrap();
    let blocked_store = store.clone();
    let blocked = tokio::spawn(async move {
        admit_inline(
            &blocked_store,
            blocked_target,
            "blocked-shard-mutation",
            Some("blocked-shard-key"),
            b"blocked",
        )
        .await
    });
    tokio::task::yield_now().await;

    let free = tokio::time::timeout(
        Duration::from_secs(2),
        admit_inline(
            &store,
            free_target,
            "free-shard-mutation",
            Some("free-shard-key"),
            b"free",
        ),
    )
    .await
    .expect("an independent shard must not wait for another shard's lock");
    assert_eq!(free.sequence, 1);
    assert!(!blocked.is_finished());

    drop(guard);
    let blocked = blocked.await.unwrap();
    assert_eq!(blocked.sequence, 1);
}
