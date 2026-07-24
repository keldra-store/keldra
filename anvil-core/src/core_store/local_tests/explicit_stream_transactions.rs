use super::*;

fn explicit_stream_root_publication(root_anchor_key: &str) -> CoreMutationRootPublication {
    CoreMutationRootPublication {
        root_anchor_key: root_anchor_key.to_string(),
        writer_families: vec![
            WriterFamily::CoreControl.as_str().to_string(),
            WriterFamily::Stream.as_str().to_string(),
        ],
        transaction_coordinator: true,
    }
}

async fn begin_stream_transaction(
    store: &CoreStore,
    root_anchor_key: &str,
    principal: &str,
    idempotency_key: &str,
) -> CoreTransaction {
    store
        .begin_explicit_transaction(CoreBeginTransaction {
            idempotency_key: idempotency_key.to_string(),
            root_anchor_key: root_anchor_key.to_string(),
            root_key_hash: CoreStore::root_key_hash_for_anchor(root_anchor_key),
            scope_partition: root_anchor_key.to_string(),
            ttl_ms: 60_000,
            purpose: "verify explicit stream transaction architecture".to_string(),
            principal: principal.to_string(),
            preconditions_hash: ZERO_HASH.to_string(),
        })
        .await
        .unwrap()
}

fn stream_stage_batch(
    transaction: &CoreTransaction,
    stream_id: &str,
    payloads: &[&[u8]],
    expected_head: Option<(u64, String)>,
) -> CoreMutationBatch {
    let preconditions = expected_head
        .map(|(expected_last_sequence, expected_last_event_hash)| {
            vec![CoreMutationPrecondition::StreamHead {
                stream_id: stream_id.to_string(),
                expected_last_sequence,
                expected_last_event_hash,
            }]
        })
        .unwrap_or_default();
    let operations = payloads
        .iter()
        .enumerate()
        .map(|(ordinal, payload)| CoreMutationOperation::StreamAppend {
            partition_id: transaction.scope_partition.clone(),
            stream_id: stream_id.to_string(),
            record_kind: "test.explicit-stream".to_string(),
            payload: payload.to_vec(),
            idempotency_key: Some(format!(
                "{}:{stream_id}:{ordinal}",
                transaction.transaction_id
            )),
        })
        .collect();

    CoreMutationBatch {
        transaction_id: transaction.transaction_id.clone(),
        scope_partition: transaction.scope_partition.clone(),
        committed_by_principal: transaction.committed_by_principal.clone(),
        root_publications: vec![explicit_stream_root_publication(
            &transaction.root_anchor_key,
        )],
        preconditions,
        operations,
    }
}

fn assert_no_physical_stream_rows(store: &CoreStore, stream_id: &str) {
    assert!(
        store
            .meta
            .get(
                CF_STREAM_HEADS,
                TABLE_STREAM_HEAD_ROW,
                &stream_head_key(stream_id),
            )
            .unwrap()
            .is_none(),
        "an uncommitted transaction must not write a physical stream head"
    );
    assert!(
        store
            .meta
            .scan_prefix_page(
                CF_STREAM_RECORDS,
                TABLE_STREAM_RECORD_INDEX_ROW,
                &stream_record_prefix(stream_id),
                None,
                CORE_META_MAX_SCAN_PAGE_ROWS,
            )
            .unwrap()
            .is_empty(),
        "an uncommitted transaction must not write physical stream records"
    );
}

fn transaction_row_commons(store: &CoreStore, transaction_id: &str) -> Vec<CoreMetaRowCommonProto> {
    store
        .meta
        .scan_prefix_page(
            CF_TRANSACTIONS,
            TABLE_EXPLICIT_TRANSACTION_ROW,
            b"",
            None,
            CORE_META_MAX_SCAN_PAGE_ROWS,
        )
        .unwrap()
        .into_iter()
        .filter_map(|row| {
            let common = core_meta_row_common_from_payload(&row.payload).unwrap();
            (common.transaction_id == transaction_id).then_some(common)
        })
        .collect()
}

fn stream_row_commons(store: &CoreStore, stream_id: &str) -> Vec<CoreMetaRowCommonProto> {
    let mut commons = Vec::new();
    let head = store
        .meta
        .get(
            CF_STREAM_HEADS,
            TABLE_STREAM_HEAD_ROW,
            &stream_head_key(stream_id),
        )
        .unwrap()
        .expect("committed stream must have a head row");
    commons.push(core_meta_row_common_from_payload(&head).unwrap());
    commons.extend(
        store
            .meta
            .scan_prefix_page(
                CF_STREAM_RECORDS,
                TABLE_STREAM_RECORD_INDEX_ROW,
                &stream_record_prefix(stream_id),
                None,
                CORE_META_MAX_SCAN_PAGE_ROWS,
            )
            .unwrap()
            .into_iter()
            .map(|row| core_meta_row_common_from_payload(&row.payload).unwrap()),
    );
    commons
}

async fn append_committed_record(
    store: &CoreStore,
    partition_id: &str,
    stream_id: &str,
    ordinal: u64,
) {
    store
        .append_stream(AppendStreamRecord {
            stream_id: stream_id.to_string(),
            partition_id: partition_id.to_string(),
            record_kind: "test.committed-competitor".to_string(),
            payload: ordinal.to_be_bytes().to_vec(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!("{stream_id}:committed:{ordinal}")),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn explicit_stream_staging_writes_no_stream_state_and_advances_no_root() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let root = "tenant:t/bucket:explicit-stage";
    let root_hash = CoreStore::root_key_hash_for_anchor(root);
    let stream_id = "explicit:stage-only";
    let transaction =
        begin_stream_transaction(&store, root, "principal:explicit-stage", "explicit-stage").await;

    let staged = store
        .stage_explicit_transaction_batch(stream_stage_batch(
            &transaction,
            stream_id,
            &[b"first", b"second"],
            Some((0, ZERO_HASH.to_string())),
        ))
        .await
        .unwrap();

    assert_eq!(staged.state, CoreTransactionState::Open);
    assert_eq!(staged.visible_updates.len(), 2);
    assert_no_physical_stream_rows(&store, stream_id);
    assert!(
        store
            .read_stream(ReadStream {
                stream_id: stream_id.to_string(),
                after_sequence: 0,
                limit: 10,
            })
            .await
            .unwrap()
            .is_empty()
    );
    assert!(store.read_latest_root_anchor(root).await.unwrap().is_none());
    assert_eq!(count_root_cache_generations(&store, &root_hash), 0);

    let retained = store
        .read_transaction(&transaction.transaction_id)
        .await
        .unwrap()
        .expect("staged transaction intent must be retained");
    assert_eq!(retained.state, CoreTransactionState::Open);
    assert_eq!(retained.visible_updates.len(), 2);
    assert_eq!(retained.committed_root_generation, None);
}

#[tokio::test]
async fn rolled_back_explicit_stream_transaction_remains_invisible() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let root = "tenant:t/bucket:explicit-rollback";
    let root_hash = CoreStore::root_key_hash_for_anchor(root);
    let stream_id = "explicit:rolled-back";
    let principal = "principal:explicit-rollback";
    let transaction = begin_stream_transaction(&store, root, principal, "explicit-rollback").await;
    store
        .stage_explicit_transaction_batch(stream_stage_batch(
            &transaction,
            stream_id,
            &[b"must-never-be-visible"],
            Some((0, ZERO_HASH.to_string())),
        ))
        .await
        .unwrap();

    let rolled_back = store
        .rollback_explicit_transaction(
            &transaction.transaction_id,
            principal,
            "architecture test rollback",
        )
        .await
        .unwrap();

    assert_eq!(rolled_back.state, CoreTransactionState::RolledBack);
    assert_eq!(rolled_back.committed_root_generation, None);
    assert_no_physical_stream_rows(&store, stream_id);
    assert!(
        store
            .read_stream(ReadStream {
                stream_id: stream_id.to_string(),
                after_sequence: 0,
                limit: 10,
            })
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .watch(WatchRequest {
                stream_prefix: "explicit:".to_string(),
                after_cursor: None,
                limit: 10,
            })
            .await
            .unwrap()
            .events
            .is_empty()
    );
    assert!(store.read_latest_root_anchor(root).await.unwrap().is_none());
    assert_eq!(count_root_cache_generations(&store, &root_hash), 0);
    assert!(
        transaction_row_commons(&store, &transaction.transaction_id)
            .iter()
            .all(|common| {
                common.visibility_state_enum() == CoreMetaVisibilityState::RolledBack
                    && common.root_key_hash.is_empty()
                    && common.root_generation == 0
            })
    );
}

#[tokio::test]
async fn explicit_stream_commit_publishes_records_and_transaction_rows_in_one_generation() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let root = "tenant:t/bucket:explicit-commit";
    let root_hash = CoreStore::root_key_hash_for_anchor(root);
    let stream_id = "explicit:committed";
    let principal = "principal:explicit-commit";
    let transaction = begin_stream_transaction(&store, root, principal, "explicit-commit").await;
    store
        .stage_explicit_transaction_batch(stream_stage_batch(
            &transaction,
            stream_id,
            &[b"first", b"second"],
            Some((0, ZERO_HASH.to_string())),
        ))
        .await
        .unwrap();
    let roots_before_commit = count_root_cache_generations(&store, &root_hash);

    let committed = store
        .commit_explicit_transaction(&transaction.transaction_id, principal)
        .await
        .unwrap();
    let generation = committed
        .committed_root_generation
        .expect("committed transaction must identify its root generation");

    assert_eq!(committed.state, CoreTransactionState::Committed);
    assert_eq!(roots_before_commit, 0);
    assert_eq!(count_root_cache_generations(&store, &root_hash), 1);
    assert_eq!(generation, 1);
    assert!(
        store
            .root_generation_is_published(&root_hash, generation, &transaction.transaction_id,)
            .unwrap()
    );
    let records = store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload.as_slice(), b"first");
    assert_eq!(records[1].payload.as_slice(), b"second");
    assert_eq!(records[0].sequence, 1);
    assert_eq!(records[1].sequence, 2);

    let stream_commons = stream_row_commons(&store, stream_id);
    assert_eq!(stream_commons.len(), 3, "one head and two record rows");
    let transaction_commons = transaction_row_commons(&store, &transaction.transaction_id);
    assert_eq!(
        transaction_commons.len(),
        4,
        "one header, two staged updates, and one precondition row"
    );
    for common in stream_commons.iter().chain(&transaction_commons) {
        assert_eq!(
            common.visibility_state_enum(),
            CoreMetaVisibilityState::Committed
        );
        assert_eq!(common.root_key_hash, root_hash);
        assert_eq!(common.root_generation, generation);
        assert_eq!(common.transaction_id, transaction.transaction_id);
    }
}

#[tokio::test]
async fn explicit_stream_sequence_is_independent_of_transaction_root_generation() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let root = "tenant:t/bucket:sequence-independent-root";
    let stream_id = "explicit:sequence-independent";
    for ordinal in 1..=3 {
        append_committed_record(&store, root, stream_id, ordinal).await;
    }
    let existing = store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    let head = existing.last().unwrap();
    let principal = "principal:sequence-independent";
    let transaction =
        begin_stream_transaction(&store, root, principal, "sequence-independent").await;
    store
        .stage_explicit_transaction_batch(stream_stage_batch(
            &transaction,
            stream_id,
            &[b"fourth"],
            Some((head.sequence, head.event_hash.clone())),
        ))
        .await
        .unwrap();

    let committed = store
        .commit_explicit_transaction(&transaction.transaction_id, principal)
        .await
        .unwrap();
    let generation = committed.committed_root_generation.unwrap();
    let committed_sequence = committed
        .visible_updates
        .iter()
        .find_map(|update| match update {
            CoreTransactionUpdate::StreamAppend {
                visible_sequence, ..
            } => Some(*visible_sequence),
            _ => None,
        })
        .unwrap();

    assert_eq!(generation, 1);
    assert_eq!(committed_sequence, 4);
    assert_ne!(generation, committed_sequence);
    let records = store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(records.last().unwrap().payload.as_slice(), b"fourth");
    let committed_row = store
        .meta
        .get(
            CF_STREAM_RECORDS,
            TABLE_STREAM_RECORD_INDEX_ROW,
            &stream_record_key(stream_id, committed_sequence),
        )
        .unwrap()
        .unwrap();
    let common = core_meta_row_common_from_payload(&committed_row).unwrap();
    assert_eq!(common.root_generation, generation);
}

#[tokio::test]
async fn competing_committed_stream_append_causes_deterministic_stream_head_conflict() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let root = "tenant:t/bucket:explicit-conflict";
    let root_hash = CoreStore::root_key_hash_for_anchor(root);
    let stream_id = "explicit:conflict";
    let principal = "principal:explicit-conflict";
    let transaction = begin_stream_transaction(&store, root, principal, "explicit-conflict").await;
    store
        .stage_explicit_transaction_batch(stream_stage_batch(
            &transaction,
            stream_id,
            &[b"staged"],
            Some((0, ZERO_HASH.to_string())),
        ))
        .await
        .unwrap();
    append_committed_record(&store, root, stream_id, 1).await;

    let first_error = store
        .commit_explicit_transaction(&transaction.transaction_id, principal)
        .await
        .unwrap_err();
    let second_error = store
        .commit_explicit_transaction(&transaction.transaction_id, principal)
        .await
        .unwrap_err();

    assert!(is_stream_head_mismatch(&first_error));
    assert_eq!(first_error.to_string(), second_error.to_string());
    let retained = store
        .read_transaction(&transaction.transaction_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.state, CoreTransactionState::Open);
    assert_eq!(retained.committed_root_generation, None);
    assert!(store.read_latest_root_anchor(root).await.unwrap().is_none());
    assert_eq!(count_root_cache_generations(&store, &root_hash), 0);
    let records = store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_ne!(records[0].payload.as_slice(), b"staged");
}
