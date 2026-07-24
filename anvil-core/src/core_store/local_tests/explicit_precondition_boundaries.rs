use super::*;

const PRINCIPAL: &str = "principal:explicit-precondition-boundary";

async fn begin_transaction(store: &CoreStore, test_name: &str) -> CoreTransaction {
    let root_anchor_key = format!("tenant:test/bucket:explicit-precondition/{test_name}");
    store
        .begin_explicit_transaction(CoreBeginTransaction {
            idempotency_key: format!("explicit-precondition-{test_name}"),
            root_key_hash: CoreStore::root_key_hash_for_anchor(&root_anchor_key),
            scope_partition: root_anchor_key.clone(),
            root_anchor_key,
            ttl_ms: 60_000,
            purpose: "verify explicit precondition stage boundaries".to_string(),
            principal: PRINCIPAL.to_string(),
            preconditions_hash: ZERO_HASH.to_string(),
        })
        .await
        .unwrap()
}

fn tuple_key(test_name: &str) -> Vec<u8> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("explicit-precondition-boundary"),
        CoreMetaTuplePart::Utf8(test_name),
    ])
    .unwrap()
}

fn committed_payload(value: &[u8]) -> Vec<u8> {
    encode_core_meta_inline_payload_row(
        value,
        core_meta_committed_row_common(PRINCIPAL, "", 0, "", 1),
    )
    .unwrap()
}

fn staged_payload(transaction: &CoreTransaction, value: &[u8]) -> Vec<u8> {
    encode_core_meta_inline_payload_row(
        value,
        core_meta_committed_row_common(
            PRINCIPAL,
            transaction.root_key_hash.clone(),
            0,
            transaction.transaction_id.clone(),
            transaction.created_at_unix_nanos,
        ),
    )
    .unwrap()
}

fn install_committed_payload(store: &CoreStore, key: &[u8], value: &[u8]) -> Vec<u8> {
    let payload = committed_payload(value);
    store
        .meta
        .put(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, key, &payload)
        .unwrap();
    payload
}

#[tokio::test]
async fn require_absent_guard_is_revalidated_before_its_own_put() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let transaction = begin_transaction(&store, "absent-put").await;
    let key = tuple_key("absent-put");

    store
        .stage_coremeta_put_in_transaction(
            &transaction.transaction_id,
            PRINCIPAL,
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            key.clone(),
            staged_payload(&transaction, b"created"),
            None,
            true,
            false,
        )
        .await
        .unwrap();

    let committed = store
        .commit_explicit_transaction(&transaction.transaction_id, PRINCIPAL)
        .await
        .unwrap();
    assert_eq!(committed.state, CoreTransactionState::Committed);
    assert!(
        store
            .meta
            .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &key)
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn require_present_guard_is_revalidated_before_its_own_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let transaction = begin_transaction(&store, "present-delete").await;
    let key = tuple_key("present-delete");
    install_committed_payload(&store, &key, b"existing");

    store
        .stage_coremeta_delete_in_transaction(
            &transaction.transaction_id,
            PRINCIPAL,
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            key.clone(),
            None,
            true,
        )
        .await
        .unwrap();

    let committed = store
        .commit_explicit_transaction(&transaction.transaction_id, PRINCIPAL)
        .await
        .unwrap();
    assert_eq!(committed.state, CoreTransactionState::Committed);
    assert!(
        store
            .meta
            .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &key)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn expected_hash_guard_is_revalidated_before_its_own_replacement() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let transaction = begin_transaction(&store, "hash-replacement").await;
    let key = tuple_key("hash-replacement");
    let existing = install_committed_payload(&store, &key, b"existing");
    let expected_hash = core_meta_payload_digest(TABLE_INLINE_PAYLOAD_ROW, &existing);

    store
        .stage_coremeta_put_in_transaction(
            &transaction.transaction_id,
            PRINCIPAL,
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            key,
            staged_payload(&transaction, b"replacement"),
            Some(expected_hash),
            false,
            true,
        )
        .await
        .unwrap();

    let committed = store
        .commit_explicit_transaction(&transaction.transaction_id, PRINCIPAL)
        .await
        .unwrap();
    assert_eq!(committed.state, CoreTransactionState::Committed);
}

#[tokio::test]
async fn later_stage_guard_observes_only_preceding_staged_updates() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let transaction = begin_transaction(&store, "multi-stage").await;
    let key = tuple_key("multi-stage");
    let first_payload = staged_payload(&transaction, b"first");

    store
        .stage_coremeta_put_in_transaction(
            &transaction.transaction_id,
            PRINCIPAL,
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            key.clone(),
            first_payload.clone(),
            None,
            true,
            false,
        )
        .await
        .unwrap();
    store
        .stage_coremeta_put_in_transaction(
            &transaction.transaction_id,
            PRINCIPAL,
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            key,
            staged_payload(&transaction, b"second"),
            Some(core_meta_payload_digest(
                TABLE_INLINE_PAYLOAD_ROW,
                &first_payload,
            )),
            false,
            true,
        )
        .await
        .unwrap();

    let preconditions = store
        .read_transaction_preconditions_unlocked(&transaction.transaction_id)
        .await
        .unwrap();
    assert_eq!(
        preconditions
            .iter()
            .map(|precondition| precondition.visible_update_boundary)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );

    let committed = store
        .commit_explicit_transaction(&transaction.transaction_id, PRINCIPAL)
        .await
        .unwrap();
    assert_eq!(committed.state, CoreTransactionState::Committed);
}

#[tokio::test]
async fn stream_head_guard_is_revalidated_before_its_own_append() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let transaction = begin_transaction(&store, "stream-head").await;
    let stream_id = "explicit-precondition-boundary:stream-head";

    store
        .stage_explicit_transaction_batch(CoreMutationBatch {
            transaction_id: transaction.transaction_id.clone(),
            scope_partition: transaction.scope_partition.clone(),
            committed_by_principal: PRINCIPAL.to_string(),
            root_publications: vec![CoreMutationRootPublication {
                root_anchor_key: transaction.root_anchor_key.clone(),
                writer_families: vec![
                    WriterFamily::CoreControl.as_str().to_string(),
                    WriterFamily::Stream.as_str().to_string(),
                ],
                transaction_coordinator: true,
            }],
            preconditions: vec![CoreMutationPrecondition::StreamHead {
                stream_id: stream_id.to_string(),
                expected_last_sequence: 0,
                expected_last_event_hash: ZERO_HASH.to_string(),
            }],
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id: transaction.scope_partition.clone(),
                stream_id: stream_id.to_string(),
                record_kind: "test.explicit-precondition-boundary".to_string(),
                payload: b"first".to_vec(),
                idempotency_key: Some("explicit-precondition-boundary-stream".to_string()),
            }],
        })
        .await
        .unwrap();

    let committed = store
        .commit_explicit_transaction(&transaction.transaction_id, PRINCIPAL)
        .await
        .unwrap();
    assert_eq!(committed.state, CoreTransactionState::Committed);
}
