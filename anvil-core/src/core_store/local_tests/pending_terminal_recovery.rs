use super::super::local_root_publication_recovery::RootPublicationIntentState;
use super::*;

#[tokio::test]
async fn terminal_publication_finalises_its_admission_without_republishing() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let transaction_id = "terminal-admitted-mutation";
    let root_anchor_key = "tenant:t/bucket:b/terminal-admission";
    let tuple_key = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("terminal-admission"),
        CoreMetaTuplePart::Utf8("row"),
    ])
    .unwrap();
    let payload = encode_core_meta_inline_payload_row(
        b"must-not-publish",
        core_meta_committed_row_common(
            root_anchor_key,
            root_key_hash(root_anchor_key),
            1,
            transaction_id,
            1,
        ),
    )
    .unwrap();
    let batch = CoreMutationBatch {
        transaction_id: transaction_id.to_string(),
        scope_partition: root_anchor_key.to_string(),
        committed_by_principal: "principal:recovery".to_string(),
        root_publications: vec![CoreMutationRootPublication {
            root_anchor_key: root_anchor_key.to_string(),
            writer_families: vec![WriterFamily::CoreControl.as_str().to_string()],
            transaction_coordinator: true,
        }],
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
            tuple_key: tuple_key.clone(),
            payload: payload.clone(),
        }],
    };
    store
        .validate_mutation_root_publications_unlocked(&batch, false)
        .unwrap();
    store
        .admit_core_mutation(
            "mutation.batch",
            WriterFamily::CoreControl.as_str(),
            CorePendingMutationTarget::MutationBatch {
                transaction_id: transaction_id.to_string(),
                scope_partition: root_anchor_key.to_string(),
                operation_count: 1,
            },
            transaction_id.to_string(),
            Some(transaction_id.to_string()),
            CorePendingMutationPayload::Inline(&encode_core_mutation_batch(&batch).unwrap()),
            Vec::new(),
        )
        .await
        .unwrap();

    let descriptor =
        CoreMetaRootPublication::new(root_anchor_key, WriterFamily::CoreControl).coordinator();
    let created_at_unix_nanos = unix_timestamp_nanos();
    let prepared = store
        .prepare_root_publication(transaction_id, &descriptor, 1, None, created_at_unix_nanos)
        .await
        .unwrap();
    let plan_rows = store
        .meta
        .encode_batch_ops(&[CoreMetaBatchOp {
            cf: CF_INLINE_PAYLOADS,
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key: &tuple_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&payload),
        }])
        .unwrap();
    let plan_hash =
        root_publication_plan_hash(transaction_id, &[(descriptor, plan_rows.clone())], &[])
            .unwrap();
    let mut staged_rows = plan_rows;
    staged_rows.push(prepared.transaction_manifest_row.clone());
    let mut intent = build_root_publication_intent(
        transaction_id,
        plan_hash,
        store.node_identity.node_id.clone(),
        created_at_unix_nanos,
        vec![RootPublicationIntentRoot {
            ordinal: 0,
            publication: prepared,
            expected_root_generation: 0,
            rows: staged_rows,
            certificate_hash: None,
        }],
        Vec::new(),
    )
    .unwrap();
    intent.state = RootPublicationIntentState::Terminal;
    intent.terminal_reason = Some("PublicationSupersededByCommittedRoot".to_string());
    store.persist_root_publication_intent(&intent).unwrap();

    store.unregister_process_instance_for_tests();
    drop(store);
    let recovered = CoreStore::new(storage).await.unwrap();
    let transaction = recovered
        .read_transaction(transaction_id)
        .await
        .unwrap()
        .expect("terminal admitted mutation transaction");
    assert_eq!(transaction.state, CoreTransactionState::FinalisationFailed);
    assert!(
        transaction
            .finalisation_error
            .as_deref()
            .is_some_and(|error| error.contains("PublicationSupersededByCommittedRoot"))
    );
    assert!(
        recovered
            .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &tuple_key)
            .unwrap()
            .is_none()
    );
    assert!(
        read_test_pending_mutation_records(&recovered)
            .await
            .is_empty()
    );
    assert!(
        recovered
            .read_root_publication_intent(transaction_id)
            .unwrap()
            .expect("terminal publication evidence remains durable")
            .ensure_pending()
            .is_err()
    );
    recovered.unregister_process_instance_for_tests();
}
