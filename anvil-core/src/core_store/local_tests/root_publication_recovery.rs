use super::super::local_root_publication_recovery::{
    RootPublicationIntentState, publication_transaction_id, root_publication_evidence,
};
use super::super::local_root_publication_test_control::pause_publication;
use super::*;
use crate::core_store::{
    CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, validate_commit_certificate_with_verifier,
};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct PublicationValueProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    value: String,
}

fn publication_payload(
    root_anchor_key: &str,
    root_generation: u64,
    transaction_id: &str,
    value: &str,
) -> Vec<u8> {
    encode_deterministic_proto(&PublicationValueProto {
        common: Some(core_meta_committed_row_common(
            "publication-recovery-test",
            root_key_hash(root_anchor_key),
            root_generation,
            transaction_id,
            root_generation,
        )),
        value: value.to_string(),
    })
}

fn local_publication_payload(transaction_id: &str, value: &str) -> Vec<u8> {
    encode_deterministic_proto(&PublicationValueProto {
        common: Some(core_meta_committed_row_common(
            "publication-recovery-test",
            "",
            0,
            transaction_id,
            2,
        )),
        value: value.to_string(),
    })
}

fn publication_key(name: &str) -> Vec<u8> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("publication-recovery-test"),
        CoreMetaTuplePart::Utf8(name),
    ])
    .unwrap()
}

async fn publish_initial_value(
    store: &CoreStore,
    root_anchor_key: &str,
    transaction_id: &str,
    key: &[u8],
    payload: &[u8],
) {
    store
        .commit_coremeta_root_groups(
            transaction_id,
            &[CoreMetaBatchOp {
                cf: CF_OBSERVABILITY,
                table_id: TABLE_DIAGNOSTIC_ROW,
                tuple_key: key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(payload),
            }],
            &[CoreMetaRootPublication::new(
                root_anchor_key,
                WriterFamily::CoreControl,
            )],
        )
        .await
        .unwrap();
}

async fn stage_single_put_intent(
    store: &CoreStore,
    root_anchor_key: &str,
    transaction_id: &str,
    publisher_node_id: &str,
    key: &[u8],
    payload: &[u8],
) -> RootPublicationIntent {
    let descriptor = CoreMetaRootPublication::new(root_anchor_key, WriterFamily::CoreControl);
    let created_at_unix_nanos = unix_timestamp_nanos();
    let prepared = store
        .prepare_root_publication(transaction_id, &descriptor, 2, None, created_at_unix_nanos)
        .await
        .unwrap();
    let plan_rows = store
        .meta
        .encode_batch_ops(&[CoreMetaBatchOp {
            cf: CF_OBSERVABILITY,
            table_id: TABLE_DIAGNOSTIC_ROW,
            tuple_key: key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(payload),
        }])
        .unwrap();
    let mut staged_rows = plan_rows.clone();
    staged_rows.push(prepared.transaction_manifest_row.clone());
    let plan_hash =
        root_publication_plan_hash(transaction_id, &[(descriptor, plan_rows)], &[]).unwrap();
    build_root_publication_intent(
        transaction_id,
        plan_hash,
        publisher_node_id.to_string(),
        created_at_unix_nanos,
        vec![RootPublicationIntentRoot {
            ordinal: 0,
            publication: prepared,
            expected_root_generation: 1,
            rows: staged_rows,
            certificate_hash: None,
        }],
        Vec::new(),
    )
    .unwrap()
}

fn intent_snapshot(store: &CoreStore, transaction_id: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let prefix = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("root-publication-intent"),
        CoreMetaTuplePart::Utf8(transaction_id),
    ])
    .unwrap();
    store
        .meta
        .scan_prefix_page(
            CF_TRANSACTIONS,
            TABLE_ROOT_PUBLICATION_INTENT_ROW,
            &prefix,
            None,
            CORE_META_MAX_SCAN_PAGE_ROWS,
        )
        .unwrap()
        .into_iter()
        .map(|record| (record.key, record.payload))
        .collect()
}

fn publication_row_hashes(rows: &[CoreMetaEncodedOwnedRow]) -> Vec<String> {
    let mut hashes = rows
        .iter()
        .map(|row| {
            core_meta_encoded_row_hash_with_delete(
                &row.cf,
                &row.core_meta_key,
                &row.value_envelope,
                row.delete_marker,
            )
        })
        .collect::<Vec<_>>();
    hashes.sort();
    hashes.dedup();
    hashes
}

#[tokio::test]
async fn persisted_publication_summary_rejects_a_changed_commit_plan() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let root = "publication/summary-validation";
    let transaction_id = "publication-summary-validation-v2";
    let key = publication_key("summary-validation");
    let old = publication_payload(root, 1, "publication-summary-validation-v1", "old");
    publish_initial_value(
        &store,
        root,
        "publication-summary-validation-v1",
        &key,
        &old,
    )
    .await;
    let new = publication_payload(root, 2, transaction_id, "new");
    let intent = stage_single_put_intent(
        &store,
        root,
        transaction_id,
        &store.node_identity.node_id,
        &key,
        &new,
    )
    .await;
    store.persist_root_publication_intent(&intent).unwrap();

    assert!(
        store
            .validate_persisted_root_publication_intent_summary(&intent)
            .unwrap()
    );
    let mut changed = intent;
    changed.plan_hash = format!("sha256:{}", "0".repeat(64));
    let error = store
        .validate_persisted_root_publication_intent_summary(&changed)
        .unwrap_err();
    assert!(error.to_string().contains("intent header changed"));
}

#[tokio::test]
async fn stale_recovery_intent_terminalizes_after_another_transaction_wins_its_generation() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let root = "publication/stale-recovery-generation";
    let key = publication_key("stale-recovery-generation");
    let initial_transaction_id = "publication-stale-recovery-v1";
    let stale_transaction_id = "publication-stale-recovery-stale-v2";
    let winner_transaction_id = "publication-stale-recovery-winner-v2";
    let initial = publication_payload(root, 1, initial_transaction_id, "initial");
    publish_initial_value(&store, root, initial_transaction_id, &key, &initial).await;

    let stale_payload = publication_payload(root, 2, stale_transaction_id, "stale");
    let stale_intent = stage_single_put_intent(
        &store,
        root,
        stale_transaction_id,
        &store.node_identity.node_id,
        &key,
        &stale_payload,
    )
    .await;
    store
        .persist_root_publication_intent(&stale_intent)
        .unwrap();

    let winner_payload = publication_payload(root, 2, winner_transaction_id, "winner");
    publish_initial_value(&store, root, winner_transaction_id, &key, &winner_payload).await;

    store
        .resume_root_publication_intent_for_recovery(stale_intent)
        .await
        .unwrap();
    let terminal = store
        .read_root_publication_intent(stale_transaction_id)
        .unwrap()
        .expect("stale publication intent remains as durable terminal evidence");
    assert_eq!(terminal.state, RootPublicationIntentState::Terminal);
    assert!(
        terminal
            .terminal_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("PublicationRootChanged"))
    );
    assert_eq!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &key)
            .unwrap(),
        Some(winner_payload)
    );
    let head = store
        .read_latest_root_anchor(root)
        .await
        .unwrap()
        .expect("winner root head");
    assert_eq!(head.root_generation, 2);
    assert_eq!(
        publication_transaction_id(&head).unwrap(),
        winner_transaction_id
    );

    store
        .resume_root_publication_intent_for_recovery(terminal)
        .await
        .unwrap();
}

fn signed_prepare_receipt(
    store: &CoreStore,
    replica_node_id: &str,
    pending_hash: &str,
    root_hash: &str,
    transaction_id: &str,
) -> CoreMetaPrepareReceipt {
    let mut receipt = CoreMetaPrepareReceipt {
        replica_node_id: replica_node_id.to_string(),
        write_sequence: 1,
        pending_batch_hash: pending_hash.to_string(),
        root_key_hash: root_hash.to_string(),
        expected_root_generation: 1,
        post_root_generation: 2,
        transaction_id: transaction_id.to_string(),
        signed_payload_hash: String::new(),
        signature: Vec::new(),
    };
    receipt.signed_payload_hash = prepare_receipt_payload_hash(&receipt).unwrap();
    receipt.signature = store
        .sign_internal_core_receipt(&receipt.signed_payload_hash)
        .unwrap();
    receipt
}

fn signed_persist_receipt(
    store: &CoreStore,
    replica_node_id: &str,
    certificate_hash: &str,
    committed_hash: &str,
    root_hash: &str,
    transaction_id: &str,
) -> CoreMetaCertificatePersistReceipt {
    let mut receipt = CoreMetaCertificatePersistReceipt {
        replica_node_id: replica_node_id.to_string(),
        write_sequence: 1,
        certificate_hash: certificate_hash.to_string(),
        committed_batch_hash: committed_hash.to_string(),
        root_key_hash: root_hash.to_string(),
        post_root_generation: 2,
        transaction_id: transaction_id.to_string(),
        signed_payload_hash: String::new(),
        signature: Vec::new(),
    };
    receipt.signed_payload_hash = certificate_persist_receipt_payload_hash(&receipt).unwrap();
    receipt.signature = store
        .sign_internal_core_receipt(&receipt.signed_payload_hash)
        .unwrap();
    receipt
}

fn copy_coremeta_snapshot(source: &CoreStore, target: &CoreStore) {
    let mut after = None;
    loop {
        let page = source
            .export_coremeta_snapshot_rows_page(after.as_ref(), CORE_META_MAX_SCAN_PAGE_ROWS)
            .unwrap();
        let rows = page
            .rows
            .iter()
            .map(|row| CoreMetaEncodedRow {
                cf: &row.cf,
                core_meta_key: &row.core_meta_key,
                value_envelope: &row.value_envelope,
                delete_marker: row.delete_marker,
            })
            .collect::<Vec<_>>();
        target.write_coremeta_encoded_rows(&rows).unwrap();
        let Some(next) = page.next_cursor else {
            break;
        };
        after = Some(next);
    }
}

#[tokio::test]
async fn group_publication_keeps_old_put_and_delete_values_and_all_root_caches_visible() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(
        CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
            .await
            .unwrap(),
    );
    let coordinator_root = "publication/atomic/coordinator";
    let participant_root = "publication/atomic/participant";
    let coordinator_key = publication_key("atomic-put");
    let participant_key = publication_key("atomic-delete");
    let coordinator_old = publication_payload(
        coordinator_root,
        1,
        "publication-atomic-coordinator-v1",
        "coordinator-old",
    );
    let participant_old = publication_payload(
        participant_root,
        1,
        "publication-atomic-participant-v1",
        "participant-old",
    );
    publish_initial_value(
        &store,
        coordinator_root,
        "publication-atomic-coordinator-v1",
        &coordinator_key,
        &coordinator_old,
    )
    .await;
    publish_initial_value(
        &store,
        participant_root,
        "publication-atomic-participant-v1",
        &participant_key,
        &participant_old,
    )
    .await;

    let transaction_id = "publication-atomic-group-v2";
    let coordinator_new =
        publication_payload(coordinator_root, 2, transaction_id, "coordinator-new");
    let participant_delete_common = core_meta_committed_row_common(
        "publication-recovery-test",
        root_key_hash(participant_root),
        2,
        transaction_id,
        2,
    );
    let pause = pause_publication(transaction_id);
    let commit_store = Arc::clone(&store);
    let coordinator_key_for_commit = coordinator_key.clone();
    let participant_key_for_commit = participant_key.clone();
    let commit = tokio::spawn(async move {
        commit_store
            .commit_coremeta_root_groups(
                transaction_id,
                &[
                    CoreMetaBatchOp {
                        cf: CF_OBSERVABILITY,
                        table_id: TABLE_DIAGNOSTIC_ROW,
                        tuple_key: &coordinator_key_for_commit,
                        common: None,
                        kind: CoreMetaBatchOpKind::Put(&coordinator_new),
                    },
                    CoreMetaBatchOp {
                        cf: CF_OBSERVABILITY,
                        table_id: TABLE_DIAGNOSTIC_ROW,
                        tuple_key: &participant_key_for_commit,
                        common: Some(participant_delete_common),
                        kind: CoreMetaBatchOpKind::Delete,
                    },
                ],
                &[
                    CoreMetaRootPublication::new(coordinator_root, WriterFamily::CoreControl)
                        .coordinator(),
                    CoreMetaRootPublication::new(participant_root, WriterFamily::CoreControl),
                ],
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), pause.wait_until_reached())
        .await
        .expect("publication did not reach the coordinator pause");

    let staged = store
        .read_root_publication_intent(transaction_id)
        .unwrap()
        .expect("durable publication intent");
    assert!(staged.all_outcomes_recorded());
    assert_eq!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &coordinator_key)
            .unwrap(),
        Some(coordinator_old)
    );
    assert_eq!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &participant_key)
            .unwrap(),
        Some(participant_old)
    );
    assert_eq!(
        store
            .read_internal_root_anchor_by_hash(&root_key_hash(coordinator_root), 0)
            .await
            .unwrap()
            .generation,
        1
    );
    assert_eq!(
        store
            .read_internal_root_anchor_by_hash(&root_key_hash(participant_root), 0)
            .await
            .unwrap()
            .generation,
        1
    );
    for root in [coordinator_root, participant_root] {
        assert!(
            store
                .catch_up_coremeta_generation_history(
                    &root_key_hash(root),
                    None,
                    2,
                    CORE_META_MAX_SCAN_PAGE_ROWS,
                    1024 * 1024,
                )
                .is_err(),
            "generation history became visible before group publication for {root}"
        );
    }

    pause.release();
    commit.await.unwrap().unwrap();
    assert_eq!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &coordinator_key)
            .unwrap(),
        Some(publication_payload(
            coordinator_root,
            2,
            transaction_id,
            "coordinator-new",
        ))
    );
    assert!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &participant_key)
            .unwrap()
            .is_none()
    );
    for root in [coordinator_root, participant_root] {
        assert_eq!(
            store
                .read_internal_root_anchor_by_hash(&root_key_hash(root), 0)
                .await
                .unwrap()
                .generation,
            2
        );
        let history = store
            .catch_up_coremeta_generation_history(
                &root_key_hash(root),
                None,
                2,
                CORE_META_MAX_SCAN_PAGE_ROWS,
                1024 * 1024,
            )
            .unwrap();
        assert_eq!(history.last().unwrap().final_generation, 2);
        assert!(history.last().unwrap().history_complete);
    }
}

#[tokio::test]
async fn exact_retry_reuses_persisted_generation_after_root_has_advanced() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(
        CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
            .await
            .unwrap(),
    );
    let root = "publication/exact-retry-generation";
    let key = publication_key("exact-retry-generation");
    let initial_transaction_id = "publication-exact-retry-generation-v1";
    let transaction_id = "publication-exact-retry-generation-v2";
    let initial = publication_payload(root, 1, initial_transaction_id, "old");
    publish_initial_value(&store, root, initial_transaction_id, &key, &initial).await;

    let replacement = publication_payload(root, 2, transaction_id, "new");
    let pause = pause_publication(transaction_id);
    let commit_store = Arc::clone(&store);
    let commit_key = key.clone();
    let commit_replacement = replacement.clone();
    let commit = tokio::spawn(async move {
        commit_store
            .commit_coremeta_root_groups(
                transaction_id,
                &[CoreMetaBatchOp {
                    cf: CF_OBSERVABILITY,
                    table_id: TABLE_DIAGNOSTIC_ROW,
                    tuple_key: &commit_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&commit_replacement),
                }],
                &[CoreMetaRootPublication::new(
                    root,
                    WriterFamily::CoreControl,
                )],
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), pause.wait_until_reached())
        .await
        .expect("publication did not reach the coordinator pause");

    let durable_intent = store
        .read_root_publication_intent(transaction_id)
        .unwrap()
        .expect("durable publication intent");
    assert!(durable_intent.all_outcomes_recorded());
    assert_eq!(durable_intent.roots[0].publication.post_root_generation, 2);

    pause.release();
    let first = commit.await.unwrap().unwrap();
    assert_eq!(first[0].post_root_generation, 2);
    assert_eq!(
        store
            .read_internal_root_anchor_by_hash(&root_key_hash(root), 0)
            .await
            .unwrap()
            .generation,
        2
    );
    assert!(
        store
            .read_root_publication_intent(transaction_id)
            .unwrap()
            .is_none()
    );

    // A recovery worker may have read the recorded intent before the
    // foreground publisher completed, then acquire the root lock after the
    // foreground path has cleared it. Durable completion makes that replay a
    // successful no-op rather than a recovery-readiness failure.
    store
        .resume_root_publication_intent_for_recovery(durable_intent.clone())
        .await
        .unwrap();

    // Model a lost cleanup acknowledgement: the durable plan is visible to a
    // late exact retry even though the root already reached that generation.
    store
        .persist_root_publication_intent(&durable_intent)
        .unwrap();
    let replay = store
        .commit_coremeta_root_groups(
            transaction_id,
            &[CoreMetaBatchOp {
                cf: CF_OBSERVABILITY,
                table_id: TABLE_DIAGNOSTIC_ROW,
                tuple_key: &key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&replacement),
            }],
            &[CoreMetaRootPublication::new(
                root,
                WriterFamily::CoreControl,
            )],
        )
        .await
        .unwrap();

    assert_eq!(replay.len(), first.len());
    for (replayed, original) in replay.iter().zip(&first) {
        assert_eq!(replayed.root_key_hash, original.root_key_hash);
        assert_eq!(replayed.post_root_generation, original.post_root_generation);
        assert_eq!(replayed.certificate_hash, original.certificate_hash);
        assert_eq!(replayed.committed_batch_hash, original.committed_batch_hash);
    }
    assert_eq!(
        store
            .read_internal_root_anchor_by_hash(&root_key_hash(root), 0)
            .await
            .unwrap()
            .generation,
        2
    );
    assert_eq!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &key)
            .unwrap(),
        Some(replacement)
    );
    assert!(
        store
            .read_root_publication_intent(transaction_id)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn restart_resumes_only_publisher_owned_intents_with_byte_identical_retries() {
    for publisher_owned in [true, false] {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let root = if publisher_owned {
            "publication/restart/owned"
        } else {
            "publication/restart/foreign"
        };
        let transaction_id = if publisher_owned {
            "publication-restart-owned-v2"
        } else {
            "publication-restart-foreign-v2"
        };
        let key = publication_key(transaction_id);
        let old = publication_payload(root, 1, &format!("{transaction_id}-v1"), "old");
        publish_initial_value(&store, root, &format!("{transaction_id}-v1"), &key, &old).await;
        let publisher = if publisher_owned {
            store.node_identity.node_id.as_str()
        } else {
            "foreign-publication-owner"
        };
        let new = publication_payload(root, 2, transaction_id, "new");
        let intent =
            stage_single_put_intent(&store, root, transaction_id, publisher, &key, &new).await;
        store.persist_root_publication_intent(&intent).unwrap();
        let first = intent_snapshot(&store, transaction_id);
        store.persist_root_publication_intent(&intent).unwrap();
        assert_eq!(first, intent_snapshot(&store, transaction_id));
        assert_eq!(
            store
                .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &key)
                .unwrap(),
            Some(old.clone())
        );
        store.unregister_process_instance_for_tests();
        drop(store);

        let restarted = CoreStore::new(storage).await.unwrap();
        if publisher_owned {
            assert_eq!(
                restarted
                    .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &key)
                    .unwrap(),
                Some(new)
            );
            assert!(
                restarted
                    .read_root_publication_intent(transaction_id)
                    .unwrap()
                    .is_none()
            );
        } else {
            assert_eq!(
                restarted
                    .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &key)
                    .unwrap(),
                Some(old)
            );
            assert!(
                restarted
                    .read_root_publication_intent(transaction_id)
                    .unwrap()
                    .is_some()
            );
        }
        restarted.unregister_process_instance_for_tests();
    }
}

#[tokio::test]
async fn replica_certificate_persistence_writes_only_evidence_and_keeps_candidate_staged() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let root = "publication/replica/evidence-only";
    let transaction_id = "publication-replica-evidence-v2";
    let key = publication_key("replica-evidence");
    let old = publication_payload(root, 1, "publication-replica-evidence-v1", "old");
    publish_initial_value(&store, root, "publication-replica-evidence-v1", &key, &old).await;
    let new = publication_payload(root, 2, transaction_id, "new");
    let intent = stage_single_put_intent(
        &store,
        root,
        transaction_id,
        &store.node_identity.node_id,
        &key,
        &new,
    )
    .await;
    store.persist_root_publication_intent(&intent).unwrap();
    let rows_by_root = intent
        .roots
        .iter()
        .map(|root| {
            (
                root.publication.descriptor.root_key_hash(),
                root.rows.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let root_hash = root_key_hash(root);
    let row_hashes = publication_row_hashes(&intent.roots[0].rows);
    let pending_hash = pending_batch_hash(&CoreMetaPendingBatchInput {
        root_key_hash: root_hash.clone(),
        expected_root_generation: 1,
        post_root_generation: 2,
        transaction_id: transaction_id.to_string(),
        row_hashes: row_hashes.clone(),
    })
    .unwrap();
    let profile = store.default_coremeta_quorum_profile().unwrap();
    let certificate = build_commit_certificate(
        &profile,
        root_hash.clone(),
        1,
        2,
        transaction_id.to_string(),
        pending_hash.clone(),
        vec![
            signed_prepare_receipt(
                &store,
                "local-node-0",
                &pending_hash,
                &root_hash,
                transaction_id,
            ),
            signed_prepare_receipt(
                &store,
                "local-node-1",
                &pending_hash,
                &root_hash,
                transaction_id,
            ),
        ],
    )
    .unwrap();
    validate_commit_certificate_with_verifier(
        &profile,
        &certificate,
        |node_id, signed_payload_hash, signature| {
            store.verify_internal_core_receipt_signature(node_id, signed_payload_hash, signature)
        },
    )
    .unwrap();
    let committed_batch_hash = committed_batch_hash(&CoreMetaCommittedBatchInput {
        root_key_hash: root_hash.clone(),
        expected_root_generation: 1,
        post_root_generation: 2,
        transaction_id: transaction_id.to_string(),
        pending_batch_hash: pending_hash,
        committed_row_hashes: row_hashes,
    })
    .unwrap();
    let persist_receipt = signed_persist_receipt(
        &store,
        "local-node-0",
        &certificate.certificate_hash,
        &committed_batch_hash,
        &root_hash,
        transaction_id,
    );
    let certificate_hash = certificate.certificate_hash.clone();
    assert!(certificate_hash.starts_with("blake3:"));
    assert!(committed_batch_hash.starts_with("blake3:"));
    let mut wrong_algorithm = certificate.clone();
    wrong_algorithm.certificate_hash = format!("sha256:{}", "11".repeat(32));
    assert!(
        validate_commit_certificate_with_verifier(
            &profile,
            &wrong_algorithm,
            |node_id, signed_payload_hash, signature| {
                store.verify_internal_core_receipt_signature(
                    node_id,
                    signed_payload_hash,
                    signature,
                )
            },
        )
        .is_err()
    );
    let evidence = store
        .coremeta_commit_evidence_encoded_row_at(
            &root_hash,
            2,
            transaction_id,
            &certificate_hash,
            &committed_batch_hash,
            encode_deterministic_proto(&core_commit_certificate_to_api(&certificate)),
            vec![certificate_persist_receipt_payload_hash(&persist_receipt).unwrap()],
            vec![encode_deterministic_proto(&core_persist_receipt_to_api(
                &persist_receipt,
            ))],
            intent.created_at_unix_nanos,
        )
        .unwrap();
    store
        .persist_replica_publication_certificate_evidence(
            transaction_id,
            &rows_by_root,
            &[evidence],
        )
        .unwrap();

    assert_eq!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &key)
            .unwrap(),
        Some(old)
    );
    assert_eq!(
        store
            .read_internal_root_anchor_by_hash(&root_key_hash(root), 0)
            .await
            .unwrap()
            .generation,
        1
    );
    assert!(
        store
            .read_root_publication_intent(transaction_id)
            .unwrap()
            .expect("staged replica intent")
            .no_outcomes_recorded()
    );
    assert!(
        store
            .read_coremeta_commit_evidence(&certificate_hash)
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn remote_replica_completion_and_retry_publish_the_group_atomically() {
    let owner_tmp = tempfile::tempdir().unwrap();
    let replica_tmp = tempfile::tempdir().unwrap();
    let owner = Arc::new(
        CoreStore::new(Storage::new_at(owner_tmp.path()).await.unwrap())
            .await
            .unwrap(),
    );
    let coordinator_root = "publication/remote/coordinator";
    let participant_root = "publication/remote/participant";
    let coordinator_key = publication_key("remote-put");
    let participant_key = publication_key("remote-delete");
    let local_key = publication_key("remote-local-put");
    let coordinator_old = publication_payload(
        coordinator_root,
        1,
        "publication-remote-coordinator-v1",
        "coordinator-old",
    );
    let participant_old = publication_payload(
        participant_root,
        1,
        "publication-remote-participant-v1",
        "participant-old",
    );
    publish_initial_value(
        &owner,
        coordinator_root,
        "publication-remote-coordinator-v1",
        &coordinator_key,
        &coordinator_old,
    )
    .await;
    publish_initial_value(
        &owner,
        participant_root,
        "publication-remote-participant-v1",
        &participant_key,
        &participant_old,
    )
    .await;

    let replica_storage = Storage::new_at(replica_tmp.path()).await.unwrap();
    let replica_seed = CoreStore::new(replica_storage.clone()).await.unwrap();
    copy_coremeta_snapshot(&owner, &replica_seed);
    replica_seed.unregister_process_instance_for_tests();
    drop(replica_seed);
    let replica = CoreStore::new(replica_storage).await.unwrap();

    let transaction_id = "publication-remote-group-v2";
    let coordinator_new =
        publication_payload(coordinator_root, 2, transaction_id, "coordinator-new");
    let local_new = local_publication_payload(transaction_id, "local-new");
    let participant_delete_common = core_meta_committed_row_common(
        "publication-recovery-test",
        root_key_hash(participant_root),
        2,
        transaction_id,
        2,
    );
    let pause = pause_publication(transaction_id);
    let commit_owner = Arc::clone(&owner);
    let coordinator_key_for_commit = coordinator_key.clone();
    let participant_key_for_commit = participant_key.clone();
    let local_key_for_commit = local_key.clone();
    let commit = tokio::spawn(async move {
        commit_owner
            .commit_coremeta_root_groups(
                transaction_id,
                &[
                    CoreMetaBatchOp {
                        cf: CF_OBSERVABILITY,
                        table_id: TABLE_DIAGNOSTIC_ROW,
                        tuple_key: &coordinator_key_for_commit,
                        common: None,
                        kind: CoreMetaBatchOpKind::Put(&coordinator_new),
                    },
                    CoreMetaBatchOp {
                        cf: CF_OBSERVABILITY,
                        table_id: TABLE_DIAGNOSTIC_ROW,
                        tuple_key: &participant_key_for_commit,
                        common: Some(participant_delete_common),
                        kind: CoreMetaBatchOpKind::Delete,
                    },
                    CoreMetaBatchOp {
                        cf: CF_OBSERVABILITY,
                        table_id: TABLE_DIAGNOSTIC_ROW,
                        tuple_key: &local_key_for_commit,
                        common: None,
                        kind: CoreMetaBatchOpKind::Put(&local_new),
                    },
                ],
                &[
                    CoreMetaRootPublication::new(coordinator_root, WriterFamily::CoreControl)
                        .coordinator(),
                    CoreMetaRootPublication::new(participant_root, WriterFamily::CoreControl),
                ],
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), pause.wait_until_reached())
        .await
        .expect("owner publication did not reach the coordinator pause");

    let committed_intent = owner
        .read_root_publication_intent(transaction_id)
        .unwrap()
        .expect("owner publication intent");
    let outcomes = owner.root_publication_outcomes(&committed_intent).unwrap();
    assert!(outcomes.iter().all(|outcome| {
        outcome.certificate_hash.starts_with("blake3:")
            && outcome.committed_batch_hash.starts_with("blake3:")
    }));

    let mut staged_intent = committed_intent.clone();
    for root in &mut staged_intent.roots {
        root.certificate_hash = None;
    }
    let rows_by_root = staged_intent
        .roots
        .iter()
        .map(|root| {
            (
                root.publication.descriptor.root_key_hash(),
                root.rows.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let encoded_intent = owner
        .encode_replica_root_publication_intent(&staged_intent)
        .unwrap();
    replica
        .stage_replica_root_publication_intent(&encoded_intent, &rows_by_root)
        .unwrap();
    let replica_intent = replica
        .read_root_publication_intent(transaction_id)
        .unwrap()
        .expect("replica publication intent");
    assert_eq!(replica_intent.plan_hash, staged_intent.plan_hash);
    assert_eq!(
        publication_row_hashes(&replica_intent.local_rows),
        publication_row_hashes(&staged_intent.local_rows)
    );

    let evidence_rows = outcomes
        .iter()
        .map(|outcome| {
            let receipt = outcome
                .certificate_persist_receipts
                .first()
                .expect("quorum outcome persist receipt");
            replica.coremeta_commit_evidence_encoded_row_at(
                &outcome.root_key_hash,
                outcome.post_root_generation,
                transaction_id,
                &outcome.certificate_hash,
                &outcome.committed_batch_hash,
                outcome.certificate_bytes.clone(),
                vec![certificate_persist_receipt_payload_hash(receipt).unwrap()],
                vec![encode_deterministic_proto(&core_persist_receipt_to_api(
                    receipt,
                ))],
                committed_intent.created_at_unix_nanos,
            )
        })
        .collect::<Result<Vec<_>>>()
        .unwrap();
    replica
        .persist_replica_publication_certificate_evidence(
            transaction_id,
            &rows_by_root,
            &evidence_rows,
        )
        .unwrap();
    assert_eq!(
        replica
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &coordinator_key)
            .unwrap(),
        Some(coordinator_old)
    );
    assert_eq!(
        replica
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &participant_key)
            .unwrap(),
        Some(participant_old)
    );
    assert!(
        replica
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &local_key)
            .unwrap()
            .is_none()
    );
    assert!(
        replica
            .read_root_publication_intent(transaction_id)
            .unwrap()
            .expect("remote staged intent")
            .no_outcomes_recorded()
    );

    let anchors = owner
        .publication_anchors(&committed_intent, &outcomes)
        .unwrap();
    let participant_evidence = root_publication_evidence(&anchors, &outcomes).unwrap();
    let participant_anchor_records = replica
        .install_root_publication_commit_evidence(
            &owner.node_identity.node_id,
            transaction_id,
            &participant_evidence,
        )
        .await
        .unwrap();
    let recorded_intent = replica
        .read_root_publication_intent(transaction_id)
        .unwrap()
        .expect("replica publication intent after evidence installation");
    assert!(recorded_intent.all_outcomes_recorded());
    assert!(
        replica
            .validate_persisted_root_publication_intent_summary(&recorded_intent)
            .unwrap()
    );
    assert!(
        replica
            .validate_persisted_root_publication_intent_summary(&replica_intent)
            .is_err(),
        "the pre-install intent snapshot must not validate after outcomes are recorded"
    );
    replica
        .persist_replica_publication_certificate_evidence(
            transaction_id,
            &rows_by_root,
            &evidence_rows,
        )
        .unwrap();
    let (coordinator_hash, _) = committed_intent.coordinator_scope().unwrap().unwrap();
    let coordinator = anchors
        .iter()
        .find(|anchor| anchor.root_key_hash == coordinator_hash)
        .unwrap();
    let coordinator_bytes = encode_root_anchor_record(coordinator).unwrap();
    let expected_root_hash = (coordinator.previous_root_hash != ZERO_HASH)
        .then_some(coordinator.previous_root_hash.as_str())
        .unwrap_or_default();
    replica
        .compare_and_swap_internal_root_anchor(
            &coordinator.root_key_hash,
            coordinator.root_generation - 1,
            expected_root_hash,
            &coordinator_bytes,
            &participant_anchor_records,
        )
        .await
        .unwrap();

    assert_eq!(
        replica
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &coordinator_key)
            .unwrap(),
        Some(publication_payload(
            coordinator_root,
            2,
            transaction_id,
            "coordinator-new",
        ))
    );
    assert!(
        replica
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &participant_key)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        replica
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &local_key)
            .unwrap(),
        Some(local_publication_payload(transaction_id, "local-new"))
    );
    for root in [coordinator_root, participant_root] {
        assert_eq!(
            replica
                .read_internal_root_anchor_by_hash(&root_key_hash(root), 0)
                .await
                .unwrap()
                .generation,
            2
        );
    }
    assert!(
        replica
            .read_root_publication_intent(transaction_id)
            .unwrap()
            .is_none()
    );

    let retry_records = replica
        .install_root_publication_commit_evidence(
            &owner.node_identity.node_id,
            transaction_id,
            &participant_evidence,
        )
        .await
        .unwrap();
    let retry = replica
        .compare_and_swap_internal_root_anchor(
            &coordinator.root_key_hash,
            coordinator.root_generation - 1,
            expected_root_hash,
            &coordinator_bytes,
            &retry_records,
        )
        .await
        .unwrap();
    assert_eq!(retry.generation, 2);

    pause.release();
    commit.await.unwrap().unwrap();
    replica.unregister_process_instance_for_tests();
}

#[tokio::test]
async fn bounded_range_scan_filters_unpublished_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let visible_key = publication_key("range-a-visible");
    let hidden_key = publication_key("range-b-hidden");
    let visible = encode_deterministic_proto(&PublicationValueProto {
        common: Some(core_meta_committed_row_common("local", "", 0, "", 1)),
        value: "visible".to_string(),
    });
    let hidden = publication_payload(
        "publication/range/unpublished",
        1,
        "publication-range-unpublished",
        "hidden",
    );
    store
        .meta
        .write_batch(&[
            CoreMetaBatchOp {
                cf: CF_OBSERVABILITY,
                table_id: TABLE_DIAGNOSTIC_ROW,
                tuple_key: &visible_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&visible),
            },
            CoreMetaBatchOp {
                cf: CF_OBSERVABILITY,
                table_id: TABLE_DIAGNOSTIC_ROW,
                tuple_key: &hidden_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&hidden),
            },
        ])
        .unwrap();

    let rows = store
        .scan_coremeta_range_inclusive(
            CF_OBSERVABILITY,
            TABLE_DIAGNOSTIC_ROW,
            &visible_key,
            &hidden_key,
            10,
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].payload, visible);
}
