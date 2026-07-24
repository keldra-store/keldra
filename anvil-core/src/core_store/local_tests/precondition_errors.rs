use super::*;
use crate::core_store::TABLE_CONTROL_CURRENT_ROW;

fn coremeta_lease_row_payload(value: &[u8]) -> Vec<u8> {
    encode_core_meta_inline_payload_row(
        value,
        core_meta_committed_row_common("test/coremeta-lease", "", 0, "", 1),
    )
    .unwrap()
}

fn coremeta_lease_tuple_key(test_name: &str) -> Vec<u8> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("coremeta-lease-precondition"),
        CoreMetaTuplePart::Utf8(test_name),
    ])
    .unwrap()
}

fn install_coremeta_lease_row(store: &CoreStore, tuple_key: &[u8], value: &[u8]) -> Vec<u8> {
    let payload = coremeta_lease_row_payload(value);
    store
        .meta
        .put(
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            tuple_key,
            &payload,
        )
        .unwrap();
    payload
}

fn coremeta_lease_precondition(
    tuple_key: Vec<u8>,
    payload: &[u8],
    expires_at_unix_nanos: u64,
) -> CoreMutationPrecondition {
    CoreMutationPrecondition::CoreMetaLease {
        cf: CF_INLINE_PAYLOADS.to_string(),
        table_id: TABLE_INLINE_PAYLOAD_ROW,
        tuple_key,
        expected_payload_hash: core_meta_payload_digest(TABLE_INLINE_PAYLOAD_ROW, payload),
        expires_at_unix_nanos,
    }
}

async fn stage_coremeta_lease_transaction(
    store: &CoreStore,
    test_name: &str,
    preconditions: Vec<CoreMutationPrecondition>,
) -> CoreTransaction {
    let root_anchor_key = format!("test/coremeta-lease/{test_name}");
    let principal = "principal:coremeta-lease-test";
    let transaction = store
        .begin_explicit_transaction(CoreBeginTransaction {
            idempotency_key: format!("coremeta-lease-{test_name}"),
            root_anchor_key: root_anchor_key.clone(),
            root_key_hash: CoreStore::root_key_hash_for_anchor(&root_anchor_key),
            scope_partition: root_anchor_key.clone(),
            ttl_ms: 60_000,
            purpose: "verify CoreMeta lease commit preconditions".to_string(),
            principal: principal.to_string(),
            preconditions_hash: ZERO_HASH.to_string(),
        })
        .await
        .unwrap();
    store
        .stage_explicit_transaction_batch(CoreMutationBatch {
            transaction_id: transaction.transaction_id.clone(),
            scope_partition: root_anchor_key.clone(),
            committed_by_principal: principal.to_string(),
            root_publications: vec![CoreMutationRootPublication {
                root_anchor_key: root_anchor_key.clone(),
                writer_families: vec![
                    WriterFamily::CoreControl.as_str().to_string(),
                    WriterFamily::Stream.as_str().to_string(),
                ],
                transaction_coordinator: true,
            }],
            preconditions,
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id: root_anchor_key,
                stream_id: format!("coremeta-lease-events-{test_name}"),
                record_kind: "test.coremeta-lease".to_string(),
                payload: b"guarded mutation".to_vec(),
                idempotency_key: Some(format!("coremeta-lease-event-{test_name}")),
            }],
        })
        .await
        .unwrap();
    transaction
}

fn assert_coremeta_lease_rejected(error: &anyhow::Error, expected_reason: &str) {
    let failure = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<CoreStoreCommitError>())
        .expect("CoreMeta lease rejection must preserve its structured commit error");
    let CoreStoreCommitError::CoreMetaRowPreconditionFailed { reason, .. } = failure else {
        panic!("unexpected CoreMeta lease error: {error:#}");
    };
    assert!(
        reason.contains(expected_reason),
        "expected rejection reason containing {expected_reason:?}, got {reason:?}"
    );
}

#[test]
fn retryable_conflict_classification_preserves_error_kind() {
    let stream: anyhow::Error = CoreStoreCommitError::StreamHeadMismatch {
        stream_id: "events".to_string(),
        expected_last_sequence: 1,
        expected_last_event_hash: "sha256:expected".to_string(),
        actual_sequence: 2,
        actual_event_hash: "sha256:actual".to_string(),
    }
    .into();
    assert!(is_stream_head_mismatch(&stream));
    assert!(is_retryable_mutation_conflict(&stream));

    let row: anyhow::Error = CoreStoreCommitError::CoreMetaRowPreconditionFailed {
        cf: CF_MESH.to_string(),
        table_id: TABLE_CONTROL_CURRENT_ROW,
        tuple_key_hex: "01".to_string(),
        reason: "row must be absent".to_string(),
    }
    .into();
    assert!(!is_stream_head_mismatch(&row));
    assert!(is_retryable_mutation_conflict(&row));

    let root: anyhow::Error = CoreStoreCommitError::RootChangedBeforeDurableStaging {
        root_key_hash: "sha256:root".to_string(),
        expected_generation: 3,
        expected_hash: "sha256:expected".to_string(),
        actual_generation: 4,
        actual_hash: "sha256:actual".to_string(),
    }
    .into();
    assert!(!is_stream_head_mismatch(&root));
    assert!(is_retryable_mutation_conflict(&root));

    assert!(!is_retryable_mutation_conflict(&anyhow::anyhow!(
        "corrupt row"
    )));
}

#[test]
fn availability_classification_preserves_retryable_failures() {
    let topology: anyhow::Error = CoreStoreAvailabilityError::MeshTopologyUnavailable {
        node_id: "node-b".to_string(),
    }
    .into();
    assert!(is_core_store_unavailable(&topology));

    let quorum: anyhow::Error = CoreStoreAvailabilityError::QuorumUnavailable {
        operation: "prepare",
        required: 3,
        received: 2,
        details: "one peer is joining".to_string(),
    }
    .into();
    assert!(is_core_store_unavailable(&quorum));

    let wrapped = quorum.context("publish CoreMeta root");
    assert!(is_core_store_unavailable(&wrapped));

    let peer: anyhow::Error = CoreStoreAvailabilityError::PeerUnavailable {
        operation: "put shard".to_string(),
        endpoint: "http://node-b:50051".to_string(),
        details: "connection refused".to_string(),
    }
    .into();
    assert!(is_core_store_unavailable(&peer));

    let shards: anyhow::Error = CoreStoreAvailabilityError::ShardQuorumUnavailable {
        operation: "object_write",
        required: 6,
        received: 4,
        details: "node-e and node-f are unavailable".to_string(),
    }
    .into();
    assert!(is_core_store_unavailable(&shards));

    assert!(!is_core_store_unavailable(&anyhow::anyhow!(
        "invalid commit certificate"
    )));
}

#[tokio::test]
async fn coremeta_lease_accepts_exact_row_hash_before_expiry() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let tuple_key = coremeta_lease_tuple_key("exact-before-expiry");
    let payload = install_coremeta_lease_row(&store, &tuple_key, b"lease-v1");
    let transaction = stage_coremeta_lease_transaction(
        &store,
        "exact-before-expiry",
        vec![coremeta_lease_precondition(tuple_key, &payload, u64::MAX)],
    )
    .await;

    let committed = store
        .commit_explicit_transaction(
            &transaction.transaction_id,
            &transaction.committed_by_principal,
        )
        .await
        .unwrap();

    assert_eq!(committed.state, CoreTransactionState::Committed);
}

#[tokio::test]
async fn coremeta_lease_rejects_expired_deadline_at_commit() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let tuple_key = coremeta_lease_tuple_key("expired-at-commit");
    let payload = install_coremeta_lease_row(&store, &tuple_key, b"lease-v1");
    let transaction =
        stage_coremeta_lease_transaction(&store, "expired-at-commit", Vec::new()).await;
    let transaction = store
        .read_transaction(&transaction.transaction_id)
        .await
        .unwrap()
        .expect("staged CoreMeta lease transaction");
    // Inject the durable guard after staging so finalisation sees a deterministically
    // expired lease without a wall-clock sleep or a race-prone short deadline.
    store
        .write_pending_transaction_with_staged_rows_unlocked(
            &transaction,
            &[coremeta_lease_precondition(tuple_key, &payload, 1)],
        )
        .await
        .unwrap();

    let error = store
        .commit_explicit_transaction(
            &transaction.transaction_id,
            &transaction.committed_by_principal,
        )
        .await
        .unwrap_err();

    assert_coremeta_lease_rejected(&error, "lease expired");
}

#[tokio::test]
async fn coremeta_lease_rejects_changed_row_hash() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let tuple_key = coremeta_lease_tuple_key("changed-row-hash");
    let expected_payload = install_coremeta_lease_row(&store, &tuple_key, b"lease-v1");
    let transaction = stage_coremeta_lease_transaction(
        &store,
        "changed-row-hash",
        vec![coremeta_lease_precondition(
            tuple_key.clone(),
            &expected_payload,
            u64::MAX,
        )],
    )
    .await;
    install_coremeta_lease_row(&store, &tuple_key, b"lease-v2");

    let error = store
        .commit_explicit_transaction(
            &transaction.transaction_id,
            &transaction.committed_by_principal,
        )
        .await
        .unwrap_err();

    assert_coremeta_lease_rejected(&error, "payload hash mismatch");
}

#[tokio::test]
async fn coremeta_lease_rejects_absent_row() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let tuple_key = coremeta_lease_tuple_key("absent-row");
    let expected_payload = install_coremeta_lease_row(&store, &tuple_key, b"lease-v1");
    let transaction = stage_coremeta_lease_transaction(
        &store,
        "absent-row",
        vec![coremeta_lease_precondition(
            tuple_key.clone(),
            &expected_payload,
            u64::MAX,
        )],
    )
    .await;
    store
        .meta
        .delete(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &tuple_key)
        .unwrap();

    let error = store
        .commit_explicit_transaction(
            &transaction.transaction_id,
            &transaction.committed_by_principal,
        )
        .await
        .unwrap_err();

    assert_coremeta_lease_rejected(&error, "row must be present");
}
