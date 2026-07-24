use super::super::local_root_publication_test_control::{
    pause_after_root_register_quorum, pause_publication,
};
use super::*;
use crate::task_lease::{
    TaskLeaseAcquire, TaskLeaseOwner, acquire_task_lease, read_task_lease, renew_task_lease,
    task_lease_fenced_precondition,
};
use std::time::{SystemTime, UNIX_EPOCH};

const PRINCIPAL: &str = "principal:final-linearization";
const LEASE_SIGNING_KEY: &[u8] = b"final-linearization-test-signing-key";

async fn begin_transaction(store: &CoreStore, name: &str, ttl_ms: u64) -> CoreTransaction {
    let root_anchor_key = format!("tenant:test/bucket:final-linearization/{name}");
    store
        .begin_explicit_transaction(CoreBeginTransaction {
            idempotency_key: format!("final-linearization-{name}"),
            root_key_hash: CoreStore::root_key_hash_for_anchor(&root_anchor_key),
            scope_partition: root_anchor_key.clone(),
            root_anchor_key,
            ttl_ms,
            purpose: "verify final-linearization guards".to_string(),
            principal: PRINCIPAL.to_string(),
            preconditions_hash: ZERO_HASH.to_string(),
        })
        .await
        .unwrap()
}

fn tuple_key(name: &str) -> Vec<u8> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("final-linearization"),
        CoreMetaTuplePart::Utf8(name),
    ])
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

fn published_payload(
    root_anchor_key: &str,
    root_generation: u64,
    transaction_id: &str,
    value: &[u8],
) -> Vec<u8> {
    encode_core_meta_inline_payload_row(
        value,
        core_meta_committed_row_common(
            PRINCIPAL,
            root_key_hash(root_anchor_key),
            root_generation,
            transaction_id,
            root_generation,
        ),
    )
    .unwrap()
}

fn root_publication(transaction: &CoreTransaction) -> CoreMutationRootPublication {
    CoreMutationRootPublication::new(
        transaction.root_anchor_key.clone(),
        WriterFamily::CoreControl.as_str(),
    )
    .coordinator()
}

async fn stage_guarded_put(
    store: &CoreStore,
    transaction: &CoreTransaction,
    key: Vec<u8>,
    payload: Vec<u8>,
    preconditions: Vec<CoreMutationPrecondition>,
) {
    store
        .stage_explicit_transaction_batch(CoreMutationBatch {
            transaction_id: transaction.transaction_id.clone(),
            scope_partition: transaction.scope_partition.clone(),
            committed_by_principal: PRINCIPAL.to_string(),
            root_publications: vec![root_publication(transaction)],
            preconditions,
            operations: vec![CoreMutationOperation::CoreMetaPut {
                partition_id: transaction.scope_partition.clone(),
                cf: CF_INLINE_PAYLOADS.to_string(),
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: key,
                payload,
            }],
        })
        .await
        .unwrap();
}

async fn wait_for_pause(
    pause: &super::super::local_root_publication_test_control::PublicationPause,
    commit: &mut tokio::task::JoinHandle<Result<CoreTransaction>>,
) {
    tokio::select! {
        _ = pause.wait_until_reached() => {}
        result = commit => panic!("publication finished before the final-linearization pause: {result:?}"),
        _ = tokio::time::sleep(Duration::from_secs(10)) => {
            panic!("publication did not reach the final-linearization pause")
        }
    }
}

fn now_unix_nanos_i64() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap()
}

fn assert_terminal_error(error: &anyhow::Error) {
    assert!(
        format!("{error:#}").contains("CoreMetaPublicationTerminal"),
        "unexpected publication error: {error:#}"
    );
}

#[tokio::test]
async fn explicit_stage_boundaries_survive_final_linearization() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(
        CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
            .await
            .unwrap(),
    );
    let transaction = begin_transaction(&store, "stage-boundaries", 60_000).await;
    let key = tuple_key("stage-boundaries");
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
            key.clone(),
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

    let pause = pause_publication(&transaction.transaction_id);
    let commit_store = Arc::clone(&store);
    let transaction_id = transaction.transaction_id.clone();
    let mut commit = tokio::spawn(async move {
        commit_store
            .commit_explicit_transaction(&transaction_id, PRINCIPAL)
            .await
    });
    wait_for_pause(&pause, &mut commit).await;

    let intent = store
        .read_root_publication_intent(&transaction.transaction_id)
        .unwrap()
        .expect("durable publication intent");
    let guard = intent.guard.as_ref().expect("publication guard summary");
    assert_eq!(
        guard.transaction_expires_at_unix_nanos,
        transaction.expires_at_unix_nanos
    );
    assert_eq!(guard.visible_update_count, 2);
    assert_eq!(guard.precondition_count, 2);

    pause.release();
    let committed = commit.await.unwrap().unwrap();
    assert_eq!(committed.state, CoreTransactionState::Committed);
    let expected = match committed.visible_updates.last().unwrap() {
        CoreTransactionUpdate::CoreMetaPut { payload, .. } => payload,
        update => panic!("unexpected final update: {update:?}"),
    };
    assert_eq!(
        store
            .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &key)
            .unwrap()
            .as_deref(),
        Some(expected.as_slice())
    );
}

#[tokio::test]
async fn row_change_after_prepare_is_terminal_and_recovery_cannot_bypass() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(
        CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
            .await
            .unwrap(),
    );
    let transaction = begin_transaction(&store, "row-race", 60_000).await;
    let key = tuple_key("row-race");
    store
        .stage_coremeta_put_in_transaction(
            &transaction.transaction_id,
            PRINCIPAL,
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            key.clone(),
            staged_payload(&transaction, b"stale-writer"),
            None,
            true,
            false,
        )
        .await
        .unwrap();

    let pause = pause_publication(&transaction.transaction_id);
    let commit_store = Arc::clone(&store);
    let transaction_id = transaction.transaction_id.clone();
    let mut commit = tokio::spawn(async move {
        commit_store
            .commit_explicit_transaction(&transaction_id, PRINCIPAL)
            .await
    });
    wait_for_pause(&pause, &mut commit).await;

    let competing_root = "tenant:test/bucket:final-linearization/row-race-competitor";
    let competing_transaction = "final-linearization-row-race-competitor";
    let competing_payload = published_payload(competing_root, 1, competing_transaction, b"winner");
    store
        .commit_coremeta_root_groups(
            competing_transaction,
            &[CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: &key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&competing_payload),
            }],
            &[CoreMetaRootPublication::new(
                competing_root,
                WriterFamily::CoreControl,
            )],
        )
        .await
        .unwrap();

    pause.release();
    let error = commit.await.unwrap().unwrap_err();
    assert_terminal_error(&error);
    assert_eq!(
        store
            .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &key)
            .unwrap(),
        Some(competing_payload)
    );

    let terminal = store
        .read_root_publication_intent(&transaction.transaction_id)
        .unwrap()
        .expect("terminal intent remains durable");
    assert!(terminal.ensure_pending().is_err());
    let recovery_error = store
        .resume_root_publication_intent(terminal.clone())
        .await
        .unwrap_err();
    assert_terminal_error(&recovery_error);
    store.recover_root_publication_intents().await.unwrap();
    assert!(
        store
            .read_root_publication_intent(&transaction.transaction_id)
            .unwrap()
            .expect("terminal intent remains after recovery scan")
            .ensure_pending()
            .is_err()
    );
}

#[tokio::test]
async fn stream_head_change_after_prepare_publishes_no_staged_stream_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(
        CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
            .await
            .unwrap(),
    );
    let transaction = begin_transaction(&store, "stream-race", 60_000).await;
    let stream_id = "final-linearization:stream-race";
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
                record_kind: "test.final-linearization.staged".to_string(),
                payload: b"must-not-publish".to_vec(),
                idempotency_key: Some("final-linearization-staged".to_string()),
            }],
        })
        .await
        .unwrap();

    let pause = pause_publication(&transaction.transaction_id);
    let commit_store = Arc::clone(&store);
    let transaction_id = transaction.transaction_id.clone();
    let mut commit = tokio::spawn(async move {
        commit_store
            .commit_explicit_transaction(&transaction_id, PRINCIPAL)
            .await
    });
    wait_for_pause(&pause, &mut commit).await;

    store
        .append_stream(AppendStreamRecord {
            stream_id: stream_id.to_string(),
            partition_id: "tenant:test/bucket:final-linearization/stream-competitor".to_string(),
            record_kind: "test.final-linearization.competitor".to_string(),
            payload: b"winner".to_vec(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("final-linearization-competitor".to_string()),
        })
        .await
        .unwrap();

    pause.release();
    let error = commit.await.unwrap().unwrap_err();
    assert_terminal_error(&error);
    let records = store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, b"winner");
}

#[tokio::test]
async fn task_lease_renewal_after_prepare_invalidates_exact_lease_guard() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = Arc::new(CoreStore::new(storage.clone()).await.unwrap());
    let owner = TaskLeaseOwner::node_instance("node-final-linearization", "worker-a");
    let now = now_unix_nanos_i64();
    let lease = acquire_task_lease(
        &storage,
        TaskLeaseAcquire {
            task_id: "final-linearization-task".to_string(),
            task_kind: "test.final-linearization".to_string(),
            partition_family: "test".to_string(),
            partition_id: "a".repeat(64),
            owner,
            source_cursor: 1,
            now_nanos: now,
            ttl_nanos: 60_000_000_000,
        },
        LEASE_SIGNING_KEY,
    )
    .await
    .unwrap();
    assert_eq!(
        read_task_lease(
            &storage,
            lease.owner.tenant_id,
            &lease.task_id,
            LEASE_SIGNING_KEY
        )
        .await
        .unwrap(),
        Some(lease.clone())
    );
    let lease_precondition =
        task_lease_fenced_precondition(&storage, &lease, now, LEASE_SIGNING_KEY)
            .await
            .unwrap();
    let transaction = begin_transaction(&store, "lease-renewal", 60_000).await;
    let key = tuple_key("lease-renewal");
    stage_guarded_put(
        &store,
        &transaction,
        key.clone(),
        staged_payload(&transaction, b"must-not-publish"),
        vec![lease_precondition],
    )
    .await;
    assert_eq!(
        read_task_lease(
            &storage,
            lease.owner.tenant_id,
            &lease.task_id,
            LEASE_SIGNING_KEY
        )
        .await
        .unwrap(),
        Some(lease.clone())
    );

    let pause = pause_publication(&transaction.transaction_id);
    let commit_store = Arc::clone(&store);
    let transaction_id = transaction.transaction_id.clone();
    let mut commit = tokio::spawn(async move {
        commit_store
            .commit_explicit_transaction(&transaction_id, PRINCIPAL)
            .await
    });
    wait_for_pause(&pause, &mut commit).await;
    assert_eq!(
        read_task_lease(
            &storage,
            lease.owner.tenant_id,
            &lease.task_id,
            LEASE_SIGNING_KEY
        )
        .await
        .unwrap(),
        Some(lease.clone())
    );

    let renewed_at = now_unix_nanos_i64();
    let renewed = renew_task_lease(
        &storage,
        &lease,
        renewed_at,
        60_000_000_000,
        LEASE_SIGNING_KEY,
    )
    .await
    .unwrap();
    assert_ne!(renewed.lease_hash, lease.lease_hash);

    pause.release();
    let error = commit.await.unwrap().unwrap_err();
    assert_terminal_error(&error);
    assert!(
        store
            .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &key)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn transaction_expiry_at_final_apply_publishes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(
        CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
            .await
            .unwrap(),
    );
    let transaction = begin_transaction(&store, "deadline", 500).await;
    let key = tuple_key("deadline");
    store
        .stage_coremeta_put_in_transaction(
            &transaction.transaction_id,
            PRINCIPAL,
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            key.clone(),
            staged_payload(&transaction, b"must-not-publish"),
            None,
            true,
            false,
        )
        .await
        .unwrap();

    let pause = pause_publication(&transaction.transaction_id);
    let commit_store = Arc::clone(&store);
    let transaction_id = transaction.transaction_id.clone();
    let mut commit = tokio::spawn(async move {
        commit_store
            .commit_explicit_transaction(&transaction_id, PRINCIPAL)
            .await
    });
    wait_for_pause(&pause, &mut commit).await;

    let now = current_unix_nanos_u64().unwrap();
    let remaining = transaction.expires_at_unix_nanos.saturating_sub(now);
    tokio::time::sleep(Duration::from_nanos(remaining.saturating_add(1_000_000))).await;
    pause.release();

    let error = commit.await.unwrap().unwrap_err();
    assert_terminal_error(&error);
    assert!(
        store
            .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &key)
            .unwrap()
            .is_none()
    );
    let terminal = store
        .read_root_publication_intent(&transaction.transaction_id)
        .unwrap()
        .expect("expired publication intent remains durable");
    assert!(terminal.ensure_pending().is_err());
}

#[tokio::test]
async fn restart_materializes_root_register_quorum_after_transaction_deadline() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = Arc::new(CoreStore::new(storage.clone()).await.unwrap());
    let transaction = begin_transaction(&store, "committed-before-restart", 1_000).await;
    let key = tuple_key("committed-before-restart");
    store
        .stage_coremeta_put_in_transaction(
            &transaction.transaction_id,
            PRINCIPAL,
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            key.clone(),
            staged_payload(&transaction, b"committed-before-restart"),
            None,
            true,
            false,
        )
        .await
        .unwrap();

    let pause = pause_after_root_register_quorum(&transaction.transaction_id);
    let commit_store = Arc::clone(&store);
    let transaction_id = transaction.transaction_id.clone();
    let mut commit = tokio::spawn(async move {
        commit_store
            .commit_explicit_transaction(&transaction_id, PRINCIPAL)
            .await
    });
    wait_for_pause(&pause, &mut commit).await;
    assert!(
        store
            .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &key)
            .unwrap()
            .is_none(),
        "root-register prepare quorum must not expose rows before local materialization"
    );

    commit.abort();
    assert!(commit.await.unwrap_err().is_cancelled());
    pause.release();
    let now = current_unix_nanos_u64().unwrap();
    let remaining = transaction.expires_at_unix_nanos.saturating_sub(now);
    tokio::time::sleep(Duration::from_nanos(remaining.saturating_add(1_000_000))).await;

    store.unregister_process_instance_for_tests();
    drop(store);
    let reopened = CoreStore::new(storage).await.unwrap();
    assert!(
        reopened
            .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &key)
            .unwrap()
            .is_some(),
        "a root-register quorum is already committed and must materialize after restart"
    );
    assert_eq!(
        reopened
            .read_transaction_unlocked(&transaction.transaction_id)
            .await
            .unwrap()
            .expect("committed transaction")
            .state,
        CoreTransactionState::Committed
    );
    assert!(
        reopened
            .read_root_publication_intent(&transaction.transaction_id)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn implicit_delete_accepts_a_row_older_than_the_root_head() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let root_anchor_key = "tenant:test/bucket:final-linearization/lagging-delete";
    let first_transaction = "lagging-delete-generation-1";
    let second_transaction = "lagging-delete-generation-2";
    let delete_transaction = "lagging-delete-generation-3";
    let old_key = tuple_key("lagging-delete-old-row");
    let current_key = tuple_key("lagging-delete-current-row");
    let old_payload = published_payload(root_anchor_key, 1, first_transaction, b"old");
    let first_current_payload =
        published_payload(root_anchor_key, 1, first_transaction, b"current-1");
    let second_current_payload =
        published_payload(root_anchor_key, 2, second_transaction, b"current-2");
    let coremeta_publication =
        CoreMetaRootPublication::new(root_anchor_key, WriterFamily::CoreControl).coordinator();
    let mutation_publication =
        CoreMutationRootPublication::new(root_anchor_key, WriterFamily::CoreControl.as_str())
            .coordinator();

    store
        .commit_coremeta_root_groups(
            first_transaction,
            &[
                CoreMetaBatchOp {
                    cf: CF_INLINE_PAYLOADS,
                    table_id: TABLE_INLINE_PAYLOAD_ROW,
                    tuple_key: &old_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&old_payload),
                },
                CoreMetaBatchOp {
                    cf: CF_INLINE_PAYLOADS,
                    table_id: TABLE_INLINE_PAYLOAD_ROW,
                    tuple_key: &current_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&first_current_payload),
                },
            ],
            std::slice::from_ref(&coremeta_publication),
        )
        .await
        .unwrap();
    store
        .commit_coremeta_root_groups(
            second_transaction,
            &[CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: &current_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&second_current_payload),
            }],
            std::slice::from_ref(&coremeta_publication),
        )
        .await
        .unwrap();

    let receipt = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: delete_transaction.to_string(),
            scope_partition: root_anchor_key.to_string(),
            committed_by_principal: PRINCIPAL.to_string(),
            root_publications: vec![mutation_publication],
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: CF_INLINE_PAYLOADS.to_string(),
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: old_key.clone(),
                expected_payload_hash: Some(core_meta_payload_digest(
                    TABLE_INLINE_PAYLOAD_ROW,
                    &old_payload,
                )),
                require_absent: false,
                require_present: true,
            }],
            operations: vec![CoreMutationOperation::CoreMetaDelete {
                partition_id: root_anchor_key.to_string(),
                cf: CF_INLINE_PAYLOADS.to_string(),
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: old_key.clone(),
            }],
        })
        .await
        .unwrap();

    assert_eq!(receipt.state, CoreTransactionState::Committed);
    assert!(receipt.finalisation_error.is_none());
    assert!(
        store
            .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &old_key)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &current_key,)
            .unwrap()
            .as_deref(),
        Some(second_current_payload.as_slice())
    );
    assert_eq!(
        store
            .read_latest_root_anchor(root_anchor_key)
            .await
            .unwrap()
            .expect("root anchor")
            .root_generation,
        3
    );
}
