use super::super::local_coremeta_history::{
    CoreMetaGenerationHistoryInput, CoreMetaGenerationInstallOutcome,
    TABLE_COREMETA_GENERATION_ENVELOPE_CHUNK_ROW, catch_up_frame_hash, prepare_generation_history,
};
use super::super::local_root_publication_recovery::root_publication_evidence;
use super::super::local_root_publication_test_control::pause_publication;
use super::*;
use crate::anvil_api::{CoreMetaBatchFrame, CoreMetaHistoryCursor};
use crate::core_store::{
    CF_OBSERVABILITY, CORE_META_MAX_VALUE_BYTES, TABLE_DIAGNOSTIC_ROW,
    local_signed_certificate_persist_receipt, local_signed_prepare_receipt,
};

const PAGE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, PartialEq, prost::Message)]
struct HistoryTestRow {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(bytes, tag = "3")]
    body: Vec<u8>,
}

async fn history_test_store() -> (tempfile::TempDir, CoreStore) {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    (tmp, store)
}

fn history_root(label: &str) -> String {
    format!("sha256:{}", sha256_hex(label.as_bytes()))
}

fn history_tuple_key(label: &str) -> Vec<u8> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(label)]).unwrap()
}

fn try_history_put_row(
    store: &CoreStore,
    root_key_hash: &str,
    generation: u64,
    transaction_id: &str,
    key: &str,
    body: Vec<u8>,
) -> Result<CoreMetaEncodedOwnedRow> {
    let common = core_meta_committed_row_common(
        "system/coremeta-history-test",
        root_key_hash,
        generation,
        transaction_id,
        1_000_000 + generation,
    );
    let payload = HistoryTestRow {
        common: Some(common.clone()),
        schema: "anvil.test.coremeta_history.v1".to_string(),
        body,
    }
    .encode_to_vec();
    let tuple_key = history_tuple_key(key);
    let rows = store.meta.encode_batch_ops(&[CoreMetaBatchOp {
        cf: CF_OBSERVABILITY,
        table_id: TABLE_DIAGNOSTIC_ROW,
        tuple_key: &tuple_key,
        common: Some(common),
        kind: CoreMetaBatchOpKind::Put(&payload),
    }])?;
    rows.into_iter()
        .next()
        .ok_or_else(|| anyhow!("test CoreMeta put did not encode a row"))
}

fn history_put_row(
    store: &CoreStore,
    root_key_hash: &str,
    generation: u64,
    transaction_id: &str,
    key: &str,
    body: &[u8],
) -> CoreMetaEncodedOwnedRow {
    try_history_put_row(
        store,
        root_key_hash,
        generation,
        transaction_id,
        key,
        body.to_vec(),
    )
    .unwrap()
}

fn history_delete_row(
    store: &CoreStore,
    root_key_hash: &str,
    generation: u64,
    transaction_id: &str,
    key: &str,
) -> CoreMetaEncodedOwnedRow {
    let common = core_meta_committed_row_common(
        "system/coremeta-history-test",
        root_key_hash,
        generation,
        transaction_id,
        1_000_000 + generation,
    );
    let tuple_key = history_tuple_key(key);
    store
        .meta
        .encode_batch_ops(&[CoreMetaBatchOp {
            cf: CF_OBSERVABILITY,
            table_id: TABLE_DIAGNOSTIC_ROW,
            tuple_key: &tuple_key,
            common: Some(common),
            kind: CoreMetaBatchOpKind::Delete,
        }])
        .unwrap()
        .into_iter()
        .next()
        .expect("test CoreMeta delete row")
}

fn history_input(
    store: &CoreStore,
    root_key_hash: &str,
    generation: u64,
    transaction_id: &str,
    rows: Vec<CoreMetaEncodedOwnedRow>,
) -> CoreMetaGenerationHistoryInput {
    let mut ordered = rows.clone();
    ordered.sort_by(|left, right| {
        left.cf
            .cmp(&right.cf)
            .then_with(|| left.core_meta_key.cmp(&right.core_meta_key))
            .then_with(|| left.delete_marker.cmp(&right.delete_marker))
            .then_with(|| left.value_envelope.cmp(&right.value_envelope))
    });
    let row_hashes = ordered
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
    let expected_generation = generation - 1;
    let pending_batch_hash = pending_batch_hash(&CoreMetaPendingBatchInput {
        root_key_hash: root_key_hash.to_string(),
        expected_root_generation: expected_generation,
        post_root_generation: generation,
        transaction_id: transaction_id.to_string(),
        row_hashes: row_hashes.clone(),
    })
    .unwrap();
    let profile = store.default_coremeta_quorum_profile().unwrap();
    let node_ids = super::super::local_tx_helpers::local_control_node_ids();
    let prepare_receipts = node_ids
        .iter()
        .take(profile.prepare_quorum)
        .enumerate()
        .map(|(index, node_id)| {
            let mut receipt = local_signed_prepare_receipt(
                node_id.clone(),
                index as u64 + 1,
                pending_batch_hash.clone(),
                root_key_hash.to_string(),
                expected_generation,
                generation,
                transaction_id.to_string(),
            )
            .unwrap();
            receipt.signature = store
                .sign_internal_core_receipt(&receipt.signed_payload_hash)
                .unwrap();
            receipt
        })
        .collect::<Vec<_>>();
    let certificate = build_commit_certificate(
        &profile,
        root_key_hash.to_string(),
        expected_generation,
        generation,
        transaction_id.to_string(),
        pending_batch_hash.clone(),
        prepare_receipts,
    )
    .unwrap();
    let committed_batch_hash = committed_batch_hash(&CoreMetaCommittedBatchInput {
        root_key_hash: root_key_hash.to_string(),
        expected_root_generation: expected_generation,
        post_root_generation: generation,
        transaction_id: transaction_id.to_string(),
        pending_batch_hash: pending_batch_hash.clone(),
        committed_row_hashes: row_hashes,
    })
    .unwrap();
    let persist_receipts = node_ids
        .iter()
        .take(profile.certificate_persist_quorum)
        .enumerate()
        .map(|(index, node_id)| {
            let mut receipt = local_signed_certificate_persist_receipt(
                node_id.clone(),
                index as u64 + 1,
                certificate.certificate_hash.clone(),
                committed_batch_hash.clone(),
                root_key_hash.to_string(),
                generation,
                transaction_id.to_string(),
            )
            .unwrap();
            receipt.signature = store
                .sign_internal_core_receipt(&receipt.signed_payload_hash)
                .unwrap();
            receipt
        })
        .collect::<Vec<_>>();
    let certificate_persist_receipt_hashes = persist_receipts
        .iter()
        .map(|receipt| certificate_persist_receipt_payload_hash(receipt).unwrap())
        .collect();
    let certificate_persist_receipt_bytes = persist_receipts
        .iter()
        .map(|receipt| {
            encode_deterministic_proto(
                &super::super::local_coremeta_quorum::core_persist_receipt_to_api(receipt),
            )
        })
        .collect();
    let certificate_bytes = encode_deterministic_proto(
        &super::super::local_coremeta_quorum::core_commit_certificate_to_api(&certificate),
    );

    CoreMetaGenerationHistoryInput {
        root_key_hash: root_key_hash.to_string(),
        generation,
        transaction_id: transaction_id.to_string(),
        pending_batch_hash,
        committed_batch_hash,
        certificate_hash: certificate.certificate_hash,
        certificate_bytes,
        certificate_persist_receipt_hashes,
        certificate_persist_receipt_bytes,
        coordinator_root_key_hash: None,
        coordinator_root_generation: None,
        publication_bundle: format!("test-publication-bundle:{transaction_id}").into_bytes(),
        mutations: rows,
        created_at_unix_nanos: 1_000_000 + generation,
    }
}

fn history_frame(
    prepared: &super::super::local_coremeta_history::PreparedGenerationHistory,
    mutations: Vec<crate::anvil_api::CoreMetaGenerationMutation>,
    history_complete: bool,
) -> CoreMetaBatchFrame {
    let last = mutations.last().expect("non-empty history frame").ordinal;
    let generation_complete = last + 1 == prepared.descriptor.mutation_count;
    let mut frame = CoreMetaBatchFrame {
        descriptor: Some(prepared.descriptor.clone()),
        encoded_bytes: prepared.descriptor.encoded_len() as u64
            + mutations
                .iter()
                .map(|mutation| mutation.encoded_len() as u64)
                .sum::<u64>(),
        mutations,
        next_cursor: Some(CoreMetaHistoryCursor {
            generation: prepared.descriptor.generation,
            ordinal: last,
        }),
        generation_complete,
        history_complete,
        final_generation: prepared.descriptor.generation,
        retention_floor_generation: prepared.descriptor.generation,
        frame_hash: String::new(),
    };
    frame.frame_hash = catch_up_frame_hash(&frame);
    frame
}

fn canonical_row_exists(store: &CoreStore, row: &CoreMetaEncodedOwnedRow) -> bool {
    let table_id = core_meta_record_table_id(&row.core_meta_key).unwrap();
    let tuple_key = core_meta_record_tuple_key(&row.core_meta_key).unwrap();
    store
        .meta
        .get_named(&row.cf, table_id, tuple_key)
        .unwrap()
        .is_some()
}

fn install_portable_source_bootstrap(source: &CoreStore, replica: &CoreStore) {
    let rows = source
        .export_portable_coremeta_bootstrap_rows(4096)
        .unwrap();
    replica
        .install_portable_coremeta_bootstrap_rows(&rows)
        .unwrap();
}

struct CatchUpPublicationFixture {
    transaction_id: String,
    publisher_node_id: String,
    intent: RootPublicationIntent,
    anchors: Vec<CoreRootAnchorRecord>,
    participant_evidence: Vec<crate::anvil_api::CoreMetaRootPublicationEvidence>,
    frames: BTreeMap<String, CoreMetaBatchFrame>,
    observable_rows: Vec<CoreMetaEncodedOwnedRow>,
}

async fn prepare_catch_up_publication_fixture(
    source: Arc<CoreStore>,
    replica: &CoreStore,
    label: &str,
    root_count: usize,
) -> CatchUpPublicationFixture {
    assert!(root_count > 0);
    let transaction_id = format!("coremeta-history-catch-up-{label}");
    let root_anchor_keys = (0..root_count)
        .map(|index| format!("coremeta/history/catch-up/{label}/root-{index}"))
        .collect::<Vec<_>>();
    let rows = root_anchor_keys
        .iter()
        .enumerate()
        .map(|(index, root_anchor_key)| {
            let key = history_tuple_key(&format!("{label}-row-{index}"));
            let payload = HistoryTestRow {
                common: Some(core_meta_committed_row_common(
                    "system/coremeta-history-test",
                    root_key_hash(root_anchor_key),
                    1,
                    &transaction_id,
                    1_000_000,
                )),
                schema: "anvil.test.coremeta_history.v1".to_string(),
                body: format!("{label}-value-{index}").into_bytes(),
            }
            .encode_to_vec();
            (key, payload)
        })
        .collect::<Vec<_>>();
    let publications = root_anchor_keys
        .iter()
        .enumerate()
        .map(|(index, root_anchor_key)| {
            let publication =
                CoreMetaRootPublication::new(root_anchor_key, WriterFamily::CoreControl);
            if index == 0 {
                publication.coordinator()
            } else {
                publication
            }
        })
        .collect::<Vec<_>>();

    let pause = pause_publication(&transaction_id);
    let commit_source = Arc::clone(&source);
    let commit_transaction_id = transaction_id.clone();
    let mut commit = tokio::spawn(async move {
        let operations = rows
            .iter()
            .map(|(key, payload)| CoreMetaBatchOp {
                cf: CF_OBSERVABILITY,
                table_id: TABLE_DIAGNOSTIC_ROW,
                tuple_key: key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(payload),
            })
            .collect::<Vec<_>>();
        commit_source
            .commit_coremeta_root_groups(&commit_transaction_id, &operations, &publications)
            .await
    });
    tokio::select! {
        _ = pause.wait_until_reached() => {}
        result = &mut commit => {
            panic!("source publication finished before its coordinator pause: {result:?}");
        }
        _ = tokio::time::sleep(Duration::from_secs(60)) => {
            panic!("source publication did not reach its coordinator pause");
        }
    }

    let intent = source
        .read_root_publication_intent(&transaction_id)
        .unwrap()
        .expect("source publication intent");
    let outcomes = source.root_publication_outcomes(&intent).unwrap();
    let anchors = source.publication_anchors(&intent, &outcomes).unwrap();
    let participant_evidence = root_publication_evidence(&anchors, &outcomes).unwrap();
    let mut replica_intent = intent.clone();
    for root in &mut replica_intent.roots {
        root.certificate_hash = None;
    }
    let rows_by_root = replica_intent
        .roots
        .iter()
        .map(|root| {
            (
                root.publication.descriptor.root_key_hash(),
                root.rows.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let encoded_intent = source
        .encode_replica_root_publication_intent(&replica_intent)
        .unwrap();
    replica
        .stage_replica_root_publication_intent(&encoded_intent, &rows_by_root)
        .unwrap();

    pause.release();
    commit.await.unwrap().unwrap();

    let frames = rows_by_root
        .keys()
        .map(|root_key_hash| {
            let frames = source
                .catch_up_coremeta_generation_history(
                    root_key_hash,
                    None,
                    1,
                    CORE_META_MAX_SCAN_PAGE_ROWS,
                    PAGE_BYTES,
                )
                .unwrap();
            assert_eq!(frames.len(), 1);
            (root_key_hash.clone(), frames.into_iter().next().unwrap())
        })
        .collect::<BTreeMap<_, _>>();
    let observable_rows = intent
        .roots
        .iter()
        .flat_map(|root| root.rows.iter())
        .filter(|row| row.cf == CF_OBSERVABILITY)
        .cloned()
        .collect();

    CatchUpPublicationFixture {
        transaction_id,
        publisher_node_id: source.node_identity.node_id.clone(),
        intent,
        anchors,
        participant_evidence,
        frames,
        observable_rows,
    }
}

async fn publish_catch_up_fixture(replica: &CoreStore, fixture: &CatchUpPublicationFixture) {
    let participant_anchor_records = replica
        .install_root_publication_commit_evidence(
            &fixture.publisher_node_id,
            &fixture.transaction_id,
            &fixture.participant_evidence,
        )
        .await
        .unwrap();
    let coordinator_root_key_hash = fixture
        .intent
        .coordinator_scope()
        .unwrap()
        .map(|(root_key_hash, _)| root_key_hash)
        .unwrap_or_else(|| fixture.anchors[0].root_key_hash.clone());
    let coordinator = fixture
        .anchors
        .iter()
        .find(|anchor| anchor.root_key_hash == coordinator_root_key_hash)
        .unwrap();
    let expected_root_hash = (coordinator.previous_root_hash != ZERO_HASH)
        .then_some(coordinator.previous_root_hash.as_str())
        .unwrap_or_default();
    replica
        .compare_and_swap_internal_root_anchor(
            &coordinator.root_key_hash,
            coordinator.root_generation - 1,
            expected_root_hash,
            &encode_root_anchor_record(coordinator).unwrap(),
            &participant_anchor_records,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn coremeta_history_catch_up_resumes_split_generation_without_skipping() {
    let (_tmp, store) = history_test_store().await;
    let root = history_root("split-generation");
    let transaction = "tx-split-generation";
    let rows = (0..3)
        .map(|index| {
            history_put_row(
                &store,
                &root,
                1,
                transaction,
                &format!("row-{index}"),
                format!("value-{index}").as_bytes(),
            )
        })
        .collect();
    store
        .install_coremeta_generation_history_for_test(history_input(
            &store,
            &root,
            1,
            transaction,
            rows,
        ))
        .unwrap();

    let first = store
        .catch_up_coremeta_generation_history(&root, None, 1, 2, PAGE_BYTES)
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0]
            .mutations
            .iter()
            .map(|mutation| mutation.ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert!(!first[0].generation_complete);
    assert!(!first[0].history_complete);

    let second = store
        .catch_up_coremeta_generation_history(
            &root,
            first[0].next_cursor.as_ref(),
            1,
            2,
            PAGE_BYTES,
        )
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].mutations[0].ordinal, 2);
    assert!(second[0].generation_complete);
    assert!(second[0].history_complete);
}

#[tokio::test]
async fn recovery_does_not_treat_unpublished_history_as_a_visible_generation() {
    let (_tmp, store) = history_test_store().await;
    let root = history_root("unpublished-recovery-history");
    let transaction = "tx-unpublished-recovery-history";
    let row = history_put_row(&store, &root, 1, transaction, "row", b"staged");
    store
        .install_coremeta_generation_history_for_test(history_input(
            &store,
            &root,
            1,
            transaction,
            vec![row],
        ))
        .unwrap();

    assert_eq!(
        store.generation_history_bounds(&root).unwrap(),
        Some((1, 1))
    );
    assert_eq!(
        store.coremeta_recovery_published_generation(&root).unwrap(),
        0,
        "immutable catch-up history remains invisible until root publication"
    );
    assert_eq!(
        store.coremeta_recovery_cursor(&root).unwrap(),
        None,
        "recovery must resume after the published root, not after unpublished history"
    );
}

#[tokio::test]
async fn coremeta_history_preserves_overwrites_and_deletes_across_generations() {
    let (_tmp, store) = history_test_store().await;
    let root = history_root("overwrite-delete");
    let first_transaction = "tx-overwrite-delete-1";
    let first_a = history_put_row(&store, &root, 1, first_transaction, "row-a", b"old-value");
    let first_b = history_put_row(&store, &root, 1, first_transaction, "row-b", b"delete-me");
    store
        .install_coremeta_generation_history_for_test(history_input(
            &store,
            &root,
            1,
            first_transaction,
            vec![first_a.clone(), first_b],
        ))
        .unwrap();

    let second_transaction = "tx-overwrite-delete-2";
    let second_a = history_put_row(&store, &root, 2, second_transaction, "row-a", b"new-value");
    let second_b = history_delete_row(&store, &root, 2, second_transaction, "row-b");
    store
        .install_coremeta_generation_history_for_test(history_input(
            &store,
            &root,
            2,
            second_transaction,
            vec![second_a.clone(), second_b],
        ))
        .unwrap();

    let frames = store
        .catch_up_coremeta_generation_history(&root, None, 2, 16, PAGE_BYTES)
        .unwrap();
    assert_eq!(frames.len(), 2);
    let first_rows = frames[0]
        .mutations
        .iter()
        .filter_map(|mutation| mutation.mutation.as_ref())
        .collect::<Vec<_>>();
    let second_rows = frames[1]
        .mutations
        .iter()
        .filter_map(|mutation| mutation.mutation.as_ref())
        .collect::<Vec<_>>();
    assert!(first_rows.iter().any(|row| {
        row.core_meta_key == first_a.core_meta_key
            && row.value_envelope == first_a.value_envelope
            && !row.delete_marker
    }));
    assert!(second_rows.iter().any(|row| {
        row.core_meta_key == second_a.core_meta_key
            && row.value_envelope == second_a.value_envelope
            && !row.delete_marker
    }));
    assert!(second_rows.iter().any(|row| row.delete_marker));
}

#[tokio::test]
async fn coremeta_inventory_is_descriptor_only_and_immutable() {
    let (_tmp, store) = history_test_store().await;
    let root = history_root("immutable-inventory");
    let transaction = "tx-immutable-inventory-1";
    let row = history_put_row(&store, &root, 1, transaction, "row", b"first");
    store
        .install_coremeta_generation_history_for_test(history_input(
            &store,
            &root,
            1,
            transaction,
            vec![row],
        ))
        .unwrap();
    let before = store
        .coremeta_generation_inventory(&root, None, 1, 16, PAGE_BYTES)
        .unwrap();
    assert_eq!(before.descriptors.len(), 1);

    let conflict = history_put_row(&store, &root, 1, transaction, "row", b"changed");
    let error = store
        .install_coremeta_generation_history_for_test(history_input(
            &store,
            &root,
            1,
            transaction,
            vec![conflict],
        ))
        .unwrap_err();
    assert!(error.to_string().contains("immutable"));

    let second_transaction = "tx-immutable-inventory-2";
    let second = history_put_row(&store, &root, 2, second_transaction, "row-2", b"second");
    store
        .install_coremeta_generation_history_for_test(history_input(
            &store,
            &root,
            2,
            second_transaction,
            vec![second],
        ))
        .unwrap();
    let captured = store
        .coremeta_generation_inventory(&root, None, 1, 16, PAGE_BYTES)
        .unwrap();
    assert_eq!(captured.descriptors, before.descriptors);
    assert!(captured.inventory_complete);
    assert_eq!(captured.final_generation, 1);
}

#[tokio::test]
async fn coremeta_history_rejects_unbounded_page_requests() {
    let (_tmp, store) = history_test_store().await;
    let root = history_root("bounded-pages");

    assert!(
        store
            .catch_up_coremeta_generation_history(&root, None, 0, 0, 128 * 1024)
            .is_err()
    );
    assert!(
        store
            .catch_up_coremeta_generation_history(&root, None, 0, 4_097, 128 * 1024)
            .is_err()
    );
    assert!(
        store
            .catch_up_coremeta_generation_history(&root, None, 0, 1, 128 * 1024 - 1)
            .is_err()
    );
    assert!(
        store
            .coremeta_generation_inventory(&root, None, 0, 4_097, PAGE_BYTES)
            .is_err()
    );
    assert!(
        store
            .coremeta_generation_inventory(&root, None, 0, 1, PAGE_BYTES + 1)
            .is_err()
    );
}

#[tokio::test]
async fn coremeta_history_chunks_the_largest_canonical_value_envelope() {
    let (_tmp, store) = history_test_store().await;
    let root = history_root("largest-envelope");
    let transaction = "tx-largest-envelope";
    let mut low = 1usize;
    let mut high = CORE_META_MAX_VALUE_BYTES;
    let mut largest = None;
    while low <= high {
        let midpoint = low + (high - low) / 2;
        match try_history_put_row(
            &store,
            &root,
            1,
            transaction,
            "large-row",
            vec![0x5a; midpoint],
        ) {
            Ok(row) => {
                largest = Some(row);
                low = midpoint + 1;
            }
            Err(_) => high = midpoint - 1,
        }
    }
    let largest = largest.expect("at least one canonical value envelope");
    assert!(largest.value_envelope.len() > 48 * 1024);
    assert!(largest.value_envelope.len() <= CORE_META_MAX_VALUE_BYTES);

    let history_rows = store
        .prepare_coremeta_generation_history_rows(history_input(
            &store,
            &root,
            1,
            transaction,
            vec![largest],
        ))
        .unwrap();
    assert!(
        history_rows
            .iter()
            .all(|row| row.value_envelope.len() <= CORE_META_MAX_VALUE_BYTES)
    );
    let chunk_count = history_rows
        .iter()
        .filter(|row| {
            core_meta_record_table_id(&row.core_meta_key).unwrap()
                == TABLE_COREMETA_GENERATION_ENVELOPE_CHUNK_ROW
        })
        .count();
    assert!(chunk_count >= 3);
}

#[tokio::test]
async fn coremeta_install_keeps_partial_generation_invisible_until_verified() {
    let (_tmp, store) = history_test_store().await;
    let root = history_root("staged-install");
    let transaction = "tx-staged-install";
    let rows = (0..3)
        .map(|index| {
            history_put_row(
                &store,
                &root,
                1,
                transaction,
                &format!("row-{index}"),
                format!("value-{index}").as_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let prepared =
        prepare_generation_history(history_input(&store, &root, 1, transaction, rows.clone()))
            .unwrap();
    let first = history_frame(&prepared, prepared.mutations[..2].to_vec(), false);
    assert_eq!(
        store
            .install_coremeta_generation_frame(&first)
            .await
            .unwrap(),
        CoreMetaGenerationInstallOutcome::StagedPartial {
            root_key_hash: root.clone(),
            generation: 1,
        }
    );
    assert!(rows.iter().all(|row| !canonical_row_exists(&store, row)));
    let inventory = store
        .coremeta_generation_inventory(&root, None, 0, 16, PAGE_BYTES)
        .unwrap();
    assert!(inventory.descriptors.is_empty());

    let second = history_frame(&prepared, prepared.mutations[2..].to_vec(), true);
    assert_eq!(
        store
            .install_coremeta_generation_frame(&second)
            .await
            .unwrap(),
        CoreMetaGenerationInstallOutcome::StagedComplete {
            root_key_hash: root.clone(),
            generation: 1,
            coordinator_root_key_hash: None,
            coordinator_root_generation: None,
        }
    );
    assert!(rows.iter().all(|row| !canonical_row_exists(&store, row)));
    assert!(
        store
            .coremeta_generation_inventory(&root, None, 0, 16, PAGE_BYTES)
            .unwrap()
            .descriptors
            .is_empty()
    );
}

#[tokio::test]
async fn committed_certificate_replaces_a_conflicting_staged_generation() {
    let (_tmp, store) = history_test_store().await;
    let root = history_root("committed-stage-replacement");
    let losing = prepare_generation_history(history_input(
        &store,
        &root,
        1,
        "tx-losing-stage",
        vec![history_put_row(
            &store,
            &root,
            1,
            "tx-losing-stage",
            "row",
            b"losing",
        )],
    ))
    .unwrap();
    let winner = prepare_generation_history(history_input(
        &store,
        &root,
        1,
        "tx-committed-winner",
        vec![history_put_row(
            &store,
            &root,
            1,
            "tx-committed-winner",
            "row",
            b"winner",
        )],
    ))
    .unwrap();

    store
        .install_coremeta_generation_frame(&history_frame(&losing, losing.mutations.clone(), true))
        .await
        .unwrap();
    assert_eq!(
        store
            .read_complete_coremeta_generation_for_recovery(&root, 1)
            .unwrap()
            .unwrap()
            .descriptor
            .certificate_hash,
        losing.descriptor.certificate_hash
    );

    let installed = store
        .install_committed_coremeta_generation_frame(
            &history_frame(&winner, winner.mutations.clone(), true),
            &winner.descriptor.certificate_hash,
            Some(&winner.descriptor.publication_bundle),
        )
        .await
        .unwrap();
    assert!(matches!(
        installed,
        CoreMetaGenerationInstallOutcome::StagedComplete { .. }
    ));
    assert_eq!(
        store
            .read_complete_coremeta_generation_for_recovery(&root, 1)
            .unwrap()
            .unwrap()
            .descriptor
            .certificate_hash,
        winner.descriptor.certificate_hash
    );
}

#[tokio::test]
async fn coremeta_install_verifies_generations_larger_than_one_scan_page() {
    let (_tmp, store) = history_test_store().await;
    let root = history_root("multi-page-install");
    let transaction = "tx-multi-page-install";
    let rows = (0..4_097)
        .map(|index| {
            history_put_row(
                &store,
                &root,
                1,
                transaction,
                &format!("row-{index:05}"),
                b"value",
            )
        })
        .collect::<Vec<_>>();
    let prepared =
        prepare_generation_history(history_input(&store, &root, 1, transaction, rows)).unwrap();

    let first = history_frame(&prepared, prepared.mutations[..4_096].to_vec(), false);
    assert!(matches!(
        store
            .install_coremeta_generation_frame(&first)
            .await
            .unwrap(),
        CoreMetaGenerationInstallOutcome::StagedPartial { .. }
    ));
    let second = history_frame(&prepared, prepared.mutations[4_096..].to_vec(), true);
    assert!(matches!(
        store
            .install_coremeta_generation_frame(&second)
            .await
            .unwrap(),
        CoreMetaGenerationInstallOutcome::StagedComplete { .. }
    ));
    let inventory = store
        .coremeta_generation_inventory(&root, None, 0, 1, PAGE_BYTES)
        .unwrap();
    assert!(inventory.descriptors.is_empty());
}

#[tokio::test]
async fn coremeta_install_rejects_tampered_frame_descriptor_certificate_and_evidence() {
    let (_tmp, store) = history_test_store().await;
    let root = history_root("tampered-install");
    let transaction = "tx-tampered-install";
    let rows = vec![history_put_row(
        &store,
        &root,
        1,
        transaction,
        "row",
        b"value",
    )];
    let prepared =
        prepare_generation_history(history_input(&store, &root, 1, transaction, rows.clone()))
            .unwrap();
    let valid = history_frame(&prepared, prepared.mutations.clone(), true);

    let mut tampered_frame = valid.clone();
    tampered_frame.frame_hash = format!("sha256:{}", "00".repeat(32));
    assert!(
        store
            .install_coremeta_generation_frame(&tampered_frame)
            .await
            .unwrap_err()
            .to_string()
            .contains("frame hash")
    );

    let mut tampered_descriptor = valid.clone();
    tampered_descriptor
        .descriptor
        .as_mut()
        .unwrap()
        .generation_hash = format!("sha256:{}", "11".repeat(32));
    tampered_descriptor.frame_hash = catch_up_frame_hash(&tampered_descriptor);
    assert!(
        store
            .install_coremeta_generation_frame(&tampered_descriptor)
            .await
            .unwrap_err()
            .to_string()
            .contains("generation hash")
    );

    let mut tampered_certificate = valid.clone();
    tampered_certificate
        .descriptor
        .as_mut()
        .unwrap()
        .commit_certificate[0] ^= 0x01;
    tampered_certificate.frame_hash = catch_up_frame_hash(&tampered_certificate);
    assert!(
        store
            .install_coremeta_generation_frame(&tampered_certificate)
            .await
            .is_err()
    );

    let mut tampered_evidence = valid;
    tampered_evidence
        .descriptor
        .as_mut()
        .unwrap()
        .certificate_persist_evidence[0]
        .evidence[0] ^= 0x01;
    tampered_evidence.frame_hash = catch_up_frame_hash(&tampered_evidence);
    assert!(
        store
            .install_coremeta_generation_frame(&tampered_evidence)
            .await
            .is_err()
    );

    assert!(rows.iter().all(|row| !canonical_row_exists(&store, row)));
    assert!(
        store
            .coremeta_generation_inventory(&root, None, 0, 16, PAGE_BYTES)
            .unwrap()
            .descriptors
            .is_empty()
    );
}

#[tokio::test]
async fn coordinated_catch_up_requires_the_full_group_and_root_cas_publishes_atomically() {
    let source_tmp = tempfile::tempdir().unwrap();
    let replica_tmp = tempfile::tempdir().unwrap();
    let source = Arc::new(
        CoreStore::new(Storage::new_at(source_tmp.path()).await.unwrap())
            .await
            .unwrap(),
    );
    let replica = CoreStore::new(Storage::new_at(replica_tmp.path()).await.unwrap())
        .await
        .unwrap();
    install_portable_source_bootstrap(&source, &replica);
    let fixture =
        prepare_catch_up_publication_fixture(Arc::clone(&source), &replica, "coordinated", 2).await;
    let scopes = fixture
        .frames
        .values()
        .map(|frame| {
            let descriptor = frame.descriptor.as_ref().unwrap();
            (descriptor.root_key_hash.clone(), descriptor.generation)
        })
        .collect::<Vec<_>>();

    let first_frame = fixture.frames.values().next().unwrap();
    assert!(matches!(
        replica
            .install_coremeta_generation_frame(first_frame)
            .await
            .unwrap(),
        CoreMetaGenerationInstallOutcome::StagedComplete {
            coordinator_root_key_hash: Some(_),
            coordinator_root_generation: Some(1),
            ..
        }
    ));
    let partial_error = replica
        .validate_staged_coremeta_generation_group_for_publication(&scopes[..1])
        .await
        .unwrap_err();
    assert!(
        partial_error
            .to_string()
            .contains("complete participant group")
    );

    for frame in fixture.frames.values().skip(1) {
        assert!(matches!(
            replica
                .install_coremeta_generation_frame(frame)
                .await
                .unwrap(),
            CoreMetaGenerationInstallOutcome::StagedComplete { .. }
        ));
    }
    let staged = replica
        .validate_staged_coremeta_generation_group_for_publication(&scopes)
        .await
        .unwrap();
    assert!(staged.iter().all(|outcome| matches!(
        outcome,
        CoreMetaGenerationInstallOutcome::StagedComplete { .. }
    )));
    assert!(
        fixture
            .observable_rows
            .iter()
            .all(|row| !canonical_row_exists(&replica, row))
    );
    for row in &fixture.observable_rows {
        let tuple_key = core_meta_record_tuple_key(&row.core_meta_key).unwrap();
        assert!(
            replica
                .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, tuple_key)
                .unwrap()
                .is_none()
        );
    }

    publish_catch_up_fixture(&replica, &fixture).await;

    assert!(
        fixture
            .observable_rows
            .iter()
            .all(|row| canonical_row_exists(&replica, row))
    );
    for row in &fixture.observable_rows {
        let tuple_key = core_meta_record_tuple_key(&row.core_meta_key).unwrap();
        assert!(
            replica
                .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, tuple_key)
                .unwrap()
                .is_some()
        );
    }
    let published = replica
        .validate_staged_coremeta_generation_group_for_publication(&scopes)
        .await
        .unwrap();
    assert!(
        published
            .iter()
            .all(|outcome| matches!(outcome, CoreMetaGenerationInstallOutcome::Published { .. }))
    );
    for frame in fixture.frames.values() {
        assert!(matches!(
            replica
                .install_coremeta_generation_frame(frame)
                .await
                .unwrap(),
            CoreMetaGenerationInstallOutcome::Published { .. }
        ));
    }
    source.unregister_process_instance_for_tests();
    replica.unregister_process_instance_for_tests();
}

#[tokio::test]
async fn single_root_coordinator_catch_up_publishes_its_complete_generation() {
    let source_tmp = tempfile::tempdir().unwrap();
    let replica_tmp = tempfile::tempdir().unwrap();
    let source = Arc::new(
        CoreStore::new(Storage::new_at(source_tmp.path()).await.unwrap())
            .await
            .unwrap(),
    );
    let replica = CoreStore::new(Storage::new_at(replica_tmp.path()).await.unwrap())
        .await
        .unwrap();
    install_portable_source_bootstrap(&source, &replica);
    let fixture =
        prepare_catch_up_publication_fixture(Arc::clone(&source), &replica, "single", 1).await;
    let frame = fixture.frames.values().next().unwrap();
    let descriptor = frame.descriptor.as_ref().unwrap();
    assert_eq!(
        descriptor.coordinator_root_key_hash.as_deref(),
        Some(descriptor.root_key_hash.as_str())
    );
    assert_eq!(
        descriptor.coordinator_root_generation,
        Some(descriptor.generation)
    );

    assert!(matches!(
        replica
            .install_coremeta_generation_frame(frame)
            .await
            .unwrap(),
        CoreMetaGenerationInstallOutcome::StagedComplete {
            coordinator_root_key_hash: Some(_),
            coordinator_root_generation: Some(1),
            ..
        }
    ));
    let scopes = vec![(descriptor.root_key_hash.clone(), descriptor.generation)];
    let staged = replica
        .validate_staged_coremeta_generation_group_for_publication(&scopes)
        .await
        .unwrap();
    assert!(matches!(
        staged.as_slice(),
        [CoreMetaGenerationInstallOutcome::StagedComplete { .. }]
    ));

    publish_catch_up_fixture(&replica, &fixture).await;
    assert!(
        fixture
            .observable_rows
            .iter()
            .all(|row| canonical_row_exists(&replica, row))
    );
    assert!(matches!(
        replica
            .validate_staged_coremeta_generation_group_for_publication(&scopes)
            .await
            .unwrap()
            .as_slice(),
        [CoreMetaGenerationInstallOutcome::Published { .. }]
    ));
    source.unregister_process_instance_for_tests();
    replica.unregister_process_instance_for_tests();
}
