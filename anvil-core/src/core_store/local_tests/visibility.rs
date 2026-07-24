use super::*;
use crate::core_store::{CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct VisibilityTestRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    value: String,
}

fn visibility_row_payload(
    root_key_hash: impl Into<String>,
    root_generation: u64,
    transaction_id: impl Into<String>,
    value: impl Into<String>,
) -> Vec<u8> {
    encode_deterministic_proto(&VisibilityTestRowProto {
        common: Some(CoreMetaRowCommonProto {
            realm_id: "visibility-test".to_string(),
            root_key_hash: root_key_hash.into(),
            root_generation,
            transaction_id: transaction_id.into(),
            visibility_state: CoreMetaVisibilityState::Committed as i32,
            created_at_unix_nanos: 1,
            payload_schema_version: 1,
        }),
        value: value.into(),
    })
}

fn visibility_row_key(value: &str) -> Vec<u8> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("coremeta-visibility-test"),
        CoreMetaTuplePart::Utf8(value),
    ])
    .unwrap()
}

fn write_unpublished_row(
    store: &CoreStore,
    tuple_key: &[u8],
    payload: &[u8],
) -> anyhow::Result<()> {
    store.meta.write_batch(&[CoreMetaBatchOp {
        cf: CF_OBSERVABILITY,
        table_id: TABLE_DIAGNOSTIC_ROW,
        tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(payload),
    }])
}

async fn publish_row(
    store: &CoreStore,
    root_anchor_key: &str,
    transaction_id: &str,
    tuple_key: &[u8],
    payload: &[u8],
) {
    let ops = [CoreMetaBatchOp {
        cf: CF_OBSERVABILITY,
        table_id: TABLE_DIAGNOSTIC_ROW,
        tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(payload),
    }];
    let publications = [CoreMetaRootPublication::new(
        root_anchor_key,
        WriterFamily::CoreControl,
    )];
    store
        .commit_coremeta_root_groups(transaction_id, &ops, &publications)
        .await
        .unwrap();
}

fn explicit_transaction(
    transaction_id: &str,
    root_anchor_key: &str,
    state: CoreTransactionState,
    committed_root_generation: Option<u64>,
) -> CoreTransaction {
    CoreTransaction {
        schema: CORE_TRANSACTION_SCHEMA.to_string(),
        transaction_id: transaction_id.to_string(),
        scope_partition: root_anchor_key.to_string(),
        state,
        preconditions_hash: format!("sha256:{}", sha256_hex(b"visibility-preconditions")),
        operations_hash: format!("sha256:{}", sha256_hex(b"visibility-operations")),
        writer_families: vec![WriterFamily::CoreControl.as_str().to_string()],
        visible_updates: Vec::new(),
        finalisation_error: None,
        committed_at: String::new(),
        committed_by_principal: "visibility-test".to_string(),
        created_at_unix_nanos: 1,
        expires_at_unix_nanos: u64::MAX,
        root_anchor_key: root_anchor_key.to_string(),
        root_key_hash: root_key_hash(root_anchor_key),
        committed_root_generation,
        purpose: "visibility-test".to_string(),
        failure_evidence: None,
        outcome: match state {
            CoreTransactionState::Open => "open",
            CoreTransactionState::Prepared => "prepared",
            CoreTransactionState::Committed => "committed",
            CoreTransactionState::FinalisationFailed => "finalisation_failed",
            CoreTransactionState::Aborted => "aborted",
            CoreTransactionState::RolledBack => "rolled_back",
            CoreTransactionState::Expired => "expired",
            CoreTransactionState::Failed => "failed",
        }
        .to_string(),
    }
}

fn write_transaction_header(store: &CoreStore, transaction: &CoreTransaction) {
    let owned = [store
        .transaction_header_as_coremeta_op_unlocked(transaction)
        .unwrap()];
    let ops = borrow_owned_coremeta_batch_ops(&owned);
    store.meta.write_batch(&ops).unwrap();
}

#[tokio::test]
async fn unanchored_rooted_rows_are_invisible_but_local_rows_remain_visible() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();

    let root_anchor_key = "visibility/unanchored";
    let rooted_key = visibility_row_key("unanchored-rooted");
    let rooted_payload = visibility_row_payload(
        root_key_hash(root_anchor_key),
        1,
        "visibility-unanchored",
        "rooted",
    );
    write_unpublished_row(&store, &rooted_key, &rooted_payload).unwrap();

    assert!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &rooted_key)
            .unwrap()
            .is_none()
    );

    let local_key = visibility_row_key("local");
    let local_payload = visibility_row_payload("", 0, "", "local");
    write_unpublished_row(&store, &local_key, &local_payload).unwrap();
    assert_eq!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &local_key)
            .unwrap(),
        Some(local_payload)
    );
}

#[tokio::test]
async fn visible_prefix_pages_advance_across_unpublished_physical_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let prefix =
        core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("coremeta-visibility-test")]).unwrap();
    let hidden_root = root_key_hash("visibility/paging/hidden");
    let rows = [
        (
            visibility_row_key("page-a-hidden"),
            visibility_row_payload(
                hidden_root.clone(),
                1,
                "visibility-page-hidden-a",
                "hidden-a",
            ),
        ),
        (
            visibility_row_key("page-b-visible"),
            visibility_row_payload("", 0, "", "visible-b"),
        ),
        (
            visibility_row_key("page-c-hidden"),
            visibility_row_payload(hidden_root, 1, "visibility-page-hidden-c", "hidden-c"),
        ),
        (
            visibility_row_key("page-d-visible"),
            visibility_row_payload("", 0, "", "visible-d"),
        ),
    ];
    let operations = rows
        .iter()
        .map(|(tuple_key, payload)| CoreMetaBatchOp {
            cf: CF_OBSERVABILITY,
            table_id: TABLE_DIAGNOSTIC_ROW,
            tuple_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(payload),
        })
        .collect::<Vec<_>>();
    store.meta.write_batch(&operations).unwrap();

    let page = store
        .scan_coremeta_prefix_page(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &prefix, None, 2)
        .unwrap();
    assert_eq!(
        page.iter()
            .map(|row| core_meta_record_tuple_key(&row.key).unwrap())
            .collect::<Vec<_>>(),
        vec![rows[1].0.as_slice(), rows[3].0.as_slice()]
    );

    let continuation = store
        .scan_coremeta_prefix_page(
            CF_OBSERVABILITY,
            TABLE_DIAGNOSTIC_ROW,
            &prefix,
            Some(&rows[1].0),
            1,
        )
        .unwrap();
    assert_eq!(continuation.len(), 1);
    assert_eq!(
        core_meta_record_tuple_key(&continuation[0].key).unwrap(),
        rows[3].0.as_slice()
    );
}

#[tokio::test]
async fn rooted_rows_require_their_exact_published_generation_and_manifest_identity() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();

    let root_anchor_key = "visibility/exact-generation";
    let root_hash = root_key_hash(root_anchor_key);
    let generation_one_key = visibility_row_key("generation-one");
    let generation_one_payload = visibility_row_payload(
        root_hash.clone(),
        1,
        "visibility-generation-one",
        "generation-one",
    );
    publish_row(
        &store,
        root_anchor_key,
        "visibility-generation-one",
        &generation_one_key,
        &generation_one_payload,
    )
    .await;
    assert_eq!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &generation_one_key,)
            .unwrap(),
        Some(generation_one_payload)
    );

    let generation_two_key = visibility_row_key("generation-two");
    let generation_two_payload = visibility_row_payload(
        root_hash.clone(),
        2,
        "visibility-generation-two",
        "generation-two",
    );
    write_unpublished_row(&store, &generation_two_key, &generation_two_payload).unwrap();
    assert!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &generation_two_key,)
            .unwrap()
            .is_none()
    );

    publish_row(
        &store,
        root_anchor_key,
        "visibility-generation-two",
        &generation_two_key,
        &generation_two_payload,
    )
    .await;
    assert_eq!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &generation_two_key,)
            .unwrap(),
        Some(generation_two_payload)
    );

    let wrong_identity_key = visibility_row_key("wrong-manifest-identity");
    let wrong_identity_payload = visibility_row_payload(
        root_hash,
        1,
        "visibility-not-in-generation-one-manifest",
        "wrong-identity",
    );
    write_unpublished_row(&store, &wrong_identity_key, &wrong_identity_payload).unwrap();
    assert!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &wrong_identity_key,)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn explicit_transaction_rows_wait_for_a_committed_published_coordinator() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();

    let transaction_id = "visibility-explicit-transaction";
    let data_root = "visibility/explicit-data";
    let data_key = visibility_row_key("explicit-data");
    let data_payload =
        visibility_row_payload(root_key_hash(data_root), 1, transaction_id, "explicit-data");
    publish_row(&store, data_root, transaction_id, &data_key, &data_payload).await;
    assert_eq!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &data_key)
            .unwrap(),
        Some(data_payload.clone())
    );

    let coordinator_root = "visibility/explicit-coordinator";
    let open = explicit_transaction(
        transaction_id,
        coordinator_root,
        CoreTransactionState::Open,
        None,
    );
    write_transaction_header(&store, &open);
    assert!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &data_key)
            .unwrap()
            .is_none()
    );

    let committed = explicit_transaction(
        transaction_id,
        coordinator_root,
        CoreTransactionState::Committed,
        Some(1),
    );
    write_transaction_header(&store, &committed);
    assert!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &data_key)
            .unwrap()
            .is_none()
    );

    let owned = [store
        .transaction_header_as_coremeta_op_unlocked(&committed)
        .unwrap()];
    let ops = borrow_owned_coremeta_batch_ops(&owned);
    let publications =
        [CoreMetaRootPublication::new(coordinator_root, WriterFamily::CoreControl).coordinator()];
    store
        .commit_coremeta_root_groups(transaction_id, &ops, &publications)
        .await
        .unwrap();

    assert_eq!(
        store
            .read_coremeta_row(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &data_key)
            .unwrap(),
        Some(data_payload)
    );
}
