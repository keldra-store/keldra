use super::super::local_root_publication_test_control::fail_publication_once;
use super::*;

const IMPLICIT_IDEMPOTENCY_ROOT: &str = "test/implicit-idempotency";

fn implicit_put_batch(transaction_id: &str, value: &[u8]) -> CoreMutationBatch {
    implicit_put_batch_at(
        transaction_id,
        IMPLICIT_IDEMPOTENCY_ROOT,
        "logical-row",
        value,
    )
}

fn implicit_put_batch_at(
    transaction_id: &str,
    root_anchor_key: &str,
    logical_row: &str,
    value: &[u8],
) -> CoreMutationBatch {
    let tuple_key = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(logical_row)]).unwrap();
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

fn implicit_delete_batch_at(
    transaction_id: &str,
    root_anchor_key: &str,
    logical_row: &str,
    expected_payload_hash: String,
) -> CoreMutationBatch {
    let tuple_key = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(logical_row)]).unwrap();
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
            expected_payload_hash: Some(expected_payload_hash),
            require_absent: false,
            require_present: true,
        }],
        operations: vec![CoreMutationOperation::CoreMetaDelete {
            partition_id: root_anchor_key.to_string(),
            cf: CF_INLINE_PAYLOADS.to_string(),
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key,
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
    assert!(replay.is_committed());
}

#[tokio::test]
async fn implicit_mutation_exact_retry_survives_a_later_same_root_generation() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let first_batch = implicit_put_batch_at(
        "implicit-retry-before-root-advance",
        IMPLICIT_IDEMPOTENCY_ROOT,
        "first-row",
        b"first",
    );

    let first = store
        .commit_mutation_batch(first_batch.clone())
        .await
        .unwrap();
    store
        .commit_mutation_batch(implicit_put_batch_at(
            "implicit-root-advancer",
            IMPLICIT_IDEMPOTENCY_ROOT,
            "second-row",
            b"second",
        ))
        .await
        .unwrap();
    let advanced = store
        .read_internal_root_anchor(IMPLICIT_IDEMPOTENCY_ROOT, 2)
        .await
        .unwrap();

    let replay = store.commit_mutation_batch(first_batch).await.unwrap();
    let after_replay = store
        .read_internal_root_anchor(IMPLICIT_IDEMPOTENCY_ROOT, advanced.generation)
        .await
        .unwrap();

    assert_eq!(replay, first);
    assert_eq!(after_replay.generation, advanced.generation);
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
async fn implicit_mutation_rejects_transaction_id_reuse_across_scope_and_plan() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let transaction_id = "implicit-conflicting-scope-retry";

    store
        .commit_mutation_batch(implicit_put_batch_at(
            transaction_id,
            "test/idempotency-scope-a",
            "scope-a-row",
            b"first",
        ))
        .await
        .unwrap();
    let error = store
        .commit_mutation_batch(implicit_put_batch_at(
            transaction_id,
            "test/idempotency-scope-b",
            "scope-b-row",
            b"different",
        ))
        .await
        .unwrap_err();

    assert!(
        format!("{error:#}").contains("idempotency conflict"),
        "unexpected cross-scope replay error: {error:#}"
    );
}

#[tokio::test]
async fn implicit_delete_retry_survives_row_absence_and_later_recreation() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let logical_row = "delete-retry-row";
    let tuple_key = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(logical_row)]).unwrap();

    store
        .commit_mutation_batch(implicit_put_batch_at(
            "implicit-delete-seed",
            IMPLICIT_IDEMPOTENCY_ROOT,
            logical_row,
            b"original",
        ))
        .await
        .unwrap();
    let original_payload = store
        .meta
        .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &tuple_key)
        .unwrap()
        .expect("seeded row");
    let delete_batch = implicit_delete_batch_at(
        "implicit-delete-retry",
        IMPLICIT_IDEMPOTENCY_ROOT,
        logical_row,
        core_meta_payload_digest(TABLE_INLINE_PAYLOAD_ROW, &original_payload),
    );

    let deleted = store
        .commit_mutation_batch(delete_batch.clone())
        .await
        .unwrap();
    assert!(
        store
            .meta
            .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &tuple_key,)
            .unwrap()
            .is_none()
    );
    let absent_replay = store
        .commit_mutation_batch(delete_batch.clone())
        .await
        .unwrap();
    assert_eq!(absent_replay, deleted);

    store
        .commit_mutation_batch(implicit_put_batch_at(
            "implicit-delete-recreation",
            IMPLICIT_IDEMPOTENCY_ROOT,
            logical_row,
            b"recreated",
        ))
        .await
        .unwrap();
    let recreated_payload = store
        .meta
        .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &tuple_key)
        .unwrap()
        .expect("recreated row");
    let advanced = store
        .read_internal_root_anchor(IMPLICIT_IDEMPOTENCY_ROOT, 3)
        .await
        .unwrap();

    let recreated_replay = store.commit_mutation_batch(delete_batch).await.unwrap();
    let after_replay = store
        .read_internal_root_anchor(IMPLICIT_IDEMPOTENCY_ROOT, advanced.generation)
        .await
        .unwrap();

    assert_eq!(recreated_replay, deleted);
    assert_eq!(
        store
            .meta
            .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &tuple_key,)
            .unwrap(),
        Some(recreated_payload)
    );
    assert_eq!(after_replay.generation, advanced.generation);
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

    assert!(receipt.is_committed());
    assert!(
        store
            .read_root_publication_intent(transaction_id)
            .unwrap()
            .is_none(),
        "the recovered publication intent must be cleared after commit"
    );
}
