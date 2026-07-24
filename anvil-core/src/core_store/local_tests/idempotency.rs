use super::super::local_root_publication_test_control::fail_publication_once;
use super::*;

fn implicit_put_batch(transaction_id: &str, value: &[u8]) -> CoreMutationBatch {
    let root_anchor_key = "test/implicit-idempotency";
    let tuple_key = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("logical-row")]).unwrap();
    let payload = encode_core_meta_inline_payload_row(
        value,
        core_meta_committed_row_common(
            "test/idempotency",
            core_meta_root_key_hash(root_anchor_key),
            0,
            "",
            1,
        ),
    )
    .unwrap();
    CoreMutationBatch {
        transaction_id: transaction_id.to_string(),
        scope_partition: root_anchor_key.to_string(),
        committed_by_principal: "principal:idempotency-test".to_string(),
        root_publications: vec![
            CoreMutationRootPublication::new(root_anchor_key, WriterFamily::CoreControl.as_str())
                .coordinator(),
        ],
        preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
            cf: CF_INLINE_PAYLOADS.to_string(),
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key: tuple_key.clone(),
            expected_payload_hash: None,
            require_absent: true,
            require_present: false,
        }],
        operations: vec![CoreMutationOperation::CoreMetaPut {
            partition_id: root_anchor_key.to_string(),
            cf: CF_INLINE_PAYLOADS.to_string(),
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key,
            payload,
        }],
    }
}

#[tokio::test]
async fn implicit_mutation_exact_retry_returns_the_committed_receipt() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let batch = implicit_put_batch("implicit-exact-retry", b"first");

    let first = store.commit_mutation_batch(batch.clone()).await.unwrap();
    let replay = store.commit_mutation_batch(batch).await.unwrap();

    assert_eq!(replay, first);
    assert_eq!(replay.state, CoreTransactionState::Committed);
}

#[tokio::test]
async fn implicit_mutation_rejects_transaction_id_reuse_for_a_different_plan() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let transaction_id = "implicit-conflicting-retry";

    store
        .commit_mutation_batch(implicit_put_batch(transaction_id, b"first"))
        .await
        .unwrap();
    let error = store
        .commit_mutation_batch(implicit_put_batch(transaction_id, b"different"))
        .await
        .unwrap_err();

    assert!(
        format!("{error:#}").contains("idempotency conflict"),
        "unexpected replay error: {error:#}"
    );
}

#[tokio::test]
async fn implicit_mutation_recovery_reuses_the_durable_publication_plan() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let transaction_id = "implicit-publication-recovery";
    fail_publication_once(transaction_id);

    let receipt = store
        .commit_mutation_batch(implicit_put_batch(transaction_id, b"durable"))
        .await
        .unwrap();

    assert_eq!(receipt.state, CoreTransactionState::Committed);
    assert!(
        store
            .read_root_publication_intent(transaction_id)
            .unwrap()
            .is_none(),
        "the recovered publication intent must be cleared after commit"
    );
    assert_eq!(
        store
            .read_transaction(transaction_id)
            .await
            .unwrap()
            .expect("committed transaction")
            .state,
        CoreTransactionState::Committed
    );
}
