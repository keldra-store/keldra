use super::*;
use crate::core_store::{CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct PortableBootstrapTestRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    value: String,
}

async fn store_with_identity(path: &std::path::Path, node_id: &str) -> CoreStore {
    let storage = Storage::new_at(path).await.unwrap();
    CoreStore::new_with_pipeline_keyring_and_identity(
        storage,
        CorePipelineKeyring::new("bootstrap-test", [0x5a; 32]).unwrap(),
        CoreStoreNodeIdentity {
            mesh_id: "bootstrap-test-mesh".to_string(),
            node_id: node_id.to_string(),
            region_id: "bootstrap-test-region".to_string(),
            cell_id: format!("{node_id}-cell"),
            public_api_addr: format!("http://{node_id}:50051"),
            internal_bearer_token: None,
        },
        CoreStoreStartupRecovery::Immediate,
    )
    .await
    .unwrap()
}

fn bootstrap_test_payload(root_anchor_key: &str, transaction_id: &str, value: &str) -> Vec<u8> {
    bootstrap_test_payload_at_generation(root_anchor_key, 1, transaction_id, value)
}

fn bootstrap_test_payload_at_generation(
    root_anchor_key: &str,
    root_generation: u64,
    transaction_id: &str,
    value: &str,
) -> Vec<u8> {
    encode_deterministic_proto(&PortableBootstrapTestRowProto {
        common: Some(core_meta_committed_row_common(
            "portable-bootstrap-test",
            root_key_hash(root_anchor_key),
            root_generation,
            transaction_id,
            root_generation,
        )),
        value: value.to_string(),
    })
}

async fn publish_test_root(
    store: &CoreStore,
    root_anchor_key: &str,
    transaction_id: &str,
    tuple_value: &str,
) {
    let tuple_key = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("portable-bootstrap"),
        CoreMetaTuplePart::Utf8(tuple_value),
    ])
    .unwrap();
    let payload = bootstrap_test_payload(root_anchor_key, transaction_id, tuple_value);
    store
        .commit_coremeta_root_groups(
            transaction_id,
            &[CoreMetaBatchOp {
                cf: CF_OBSERVABILITY,
                table_id: TABLE_DIAGNOSTIC_ROW,
                tuple_key: &tuple_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&payload),
            }],
            &[CoreMetaRootPublication::new(
                root_anchor_key,
                WriterFamily::CoreControl,
            )],
        )
        .await
        .unwrap();
}

fn export_portable_rows(store: &CoreStore) -> Vec<CoreMetaEncodedOwnedRow> {
    store.export_portable_coremeta_bootstrap_rows(4096).unwrap()
}

async fn persist_unfinished_publication_intent(
    store: &CoreStore,
    root_anchor_key: &str,
    transaction_id: &str,
) {
    let descriptor = CoreMetaRootPublication::new(root_anchor_key, WriterFamily::CoreControl);
    let created_at_unix_nanos = unix_timestamp_nanos();
    let prepared = store
        .prepare_root_publication(transaction_id, &descriptor, 2, None, created_at_unix_nanos)
        .await
        .unwrap();
    let tuple_key = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("portable-bootstrap"),
        CoreMetaTuplePart::Utf8("unfinished"),
    ])
    .unwrap();
    let payload = bootstrap_test_payload_at_generation(
        root_anchor_key,
        2,
        transaction_id,
        "local-node-locator-must-not-split-an-intent",
    );
    let plan_rows = store
        .meta
        .encode_batch_ops(&[CoreMetaBatchOp {
            cf: CF_OBSERVABILITY,
            table_id: TABLE_DIAGNOSTIC_ROW,
            tuple_key: &tuple_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&payload),
        }])
        .unwrap();
    let mut staged_rows = plan_rows.clone();
    staged_rows.push(prepared.transaction_manifest_row.clone());
    let plan_hash =
        root_publication_plan_hash(transaction_id, &[(descriptor, plan_rows)], &[]).unwrap();
    let intent = build_root_publication_intent(
        transaction_id,
        plan_hash,
        store.node_identity.node_id.clone(),
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
    .unwrap();
    store.persist_root_publication_intent(&intent).unwrap();
}

fn table_id(row: &CoreMetaEncodedOwnedRow) -> Option<u16> {
    (row.core_meta_key.len() >= 3)
        .then(|| u16::from_be_bytes([row.core_meta_key[1], row.core_meta_key[2]]))
}

#[tokio::test]
async fn portable_bootstrap_preserves_root_verification_evidence() {
    let source_dir = tempfile::tempdir().unwrap();
    let source = store_with_identity(source_dir.path(), "bootstrap-source").await;
    let root_anchor_key = "portable-bootstrap/root";
    publish_test_root(
        &source,
        root_anchor_key,
        "portable-bootstrap-transaction",
        "source-value",
    )
    .await;

    let bootstrap_receipt_key = node_receipt_signing_public_key_key("local-control-node-1");
    assert!(
        source
            .read_coremeta_row(
                CF_MESH,
                TABLE_NODE_SIGNING_KEYPAIR_ROW,
                &bootstrap_receipt_key,
            )
            .unwrap()
            .is_some(),
        "bootstrap receipt keys must be visible without depending on the roots they verify"
    );

    let rows = export_portable_rows(&source);
    assert!(rows.iter().any(|row| {
        row.cf == CF_TRANSACTIONS && table_id(row) == Some(TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW)
    }));

    let target_dir = tempfile::tempdir().unwrap();
    let target = store_with_identity(target_dir.path(), "bootstrap-target").await;
    let target_only_root = "portable-bootstrap/target-only";
    publish_test_root(
        &target,
        target_only_root,
        "portable-bootstrap-target-only",
        "target-only-value",
    )
    .await;
    target
        .install_portable_coremeta_bootstrap_rows(&rows)
        .unwrap();

    assert!(
        target
            .read_latest_root_anchor(target_only_root)
            .await
            .unwrap()
            .is_none(),
        "canonical bootstrap must replace independently signed joining-node roots"
    );

    let imported = target
        .read_latest_root_anchor(root_anchor_key)
        .await
        .unwrap()
        .expect("portable bootstrap root remains verifiable on the joining node");
    assert_eq!(imported.root_generation, 1);
    assert!(
        target
            .validate_root_anchor_coremeta_commit_evidence(&imported)
            .unwrap()
            .is_some(),
        "joining node must cryptographically verify the imported root evidence"
    );

    let source_bootstrap_key =
        load_node_receipt_signing_public_key(&source.meta, "local-control-node-1")
            .unwrap()
            .unwrap()
            .to_bytes()
            .to_vec();
    assert_ne!(
        source_bootstrap_key,
        target.local_receipt_signing_public_key(),
        "the restart regression requires distinct source and joining-node identities"
    );

    target.unregister_process_instance_for_tests();
    drop(target);
    let restarted = store_with_identity(target_dir.path(), "bootstrap-target").await;
    let restarted_imported = restarted
        .read_latest_root_anchor(root_anchor_key)
        .await
        .unwrap()
        .expect("portable bootstrap root remains visible after joining-node restart");
    assert!(
        restarted
            .validate_root_anchor_coremeta_commit_evidence(&restarted_imported)
            .unwrap()
            .is_some(),
        "restart must retain the synthetic signer needed by historical evidence"
    );
    assert_eq!(
        load_node_receipt_signing_public_key(&restarted.meta, "local-control-node-1",)
            .unwrap()
            .unwrap()
            .to_bytes()
            .to_vec(),
        source_bootstrap_key,
        "startup must not replace portable bootstrap receipt identities"
    );
}

#[tokio::test]
async fn portable_bootstrap_excludes_unfinished_publication_intents_atomically() {
    let source_dir = tempfile::tempdir().unwrap();
    let source = store_with_identity(source_dir.path(), "bootstrap-source").await;
    let root_anchor_key = "portable-bootstrap/unfinished-root";
    publish_test_root(
        &source,
        root_anchor_key,
        "portable-bootstrap-finished-transaction",
        "finished",
    )
    .await;
    let unfinished_transaction = "portable-bootstrap-unfinished-transaction";
    persist_unfinished_publication_intent(&source, root_anchor_key, unfinished_transaction).await;
    assert!(
        source
            .read_root_publication_intent(unfinished_transaction)
            .unwrap()
            .is_some(),
        "the source must contain the unfinished intent exercised by this regression"
    );

    let rows = export_portable_rows(&source);
    assert!(
        rows.iter().all(|row| {
            row.cf != CF_TRANSACTIONS || table_id(row) != Some(TABLE_ROOT_PUBLICATION_INTENT_ROW)
        }),
        "portable bootstrap must exclude every row of transient publication intents"
    );
}

#[tokio::test]
async fn portable_bootstrap_keeps_committed_rows_that_name_source_nodes() {
    let source_dir = tempfile::tempdir().unwrap();
    let source = store_with_identity(source_dir.path(), "bootstrap-source").await;
    let tuple_value = "committed-local-node-reference";
    publish_test_root(
        &source,
        "portable-bootstrap/source-node-reference",
        "portable-bootstrap-source-node-reference",
        tuple_value,
    )
    .await;
    let tuple_key = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("portable-bootstrap"),
        CoreMetaTuplePart::Utf8(tuple_value),
    ])
    .unwrap();

    let rows = export_portable_rows(&source);

    assert!(rows.iter().any(|row| {
        row.cf == CF_OBSERVABILITY
            && table_id(row) == Some(TABLE_DIAGNOSTIC_ROW)
            && core_meta_record_tuple_key(&row.core_meta_key).unwrap() == tuple_key
    }));
}
