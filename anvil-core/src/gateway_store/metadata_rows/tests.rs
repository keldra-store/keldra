use super::*;
use tempfile::tempdir;

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestRecord(String);

impl GatewayRecordCodec for TestRecord {
    fn encode_record(&self) -> Result<Vec<u8>> {
        Ok(self.0.as_bytes().to_vec())
    }

    fn decode_record(bytes: &[u8]) -> Result<Self> {
        Ok(Self(std::str::from_utf8(bytes)?.to_string()))
    }

    fn clear_record_hash(&mut self) {}
}

#[tokio::test]
async fn gateway_metadata_pages_are_bounded_and_kind_scoped() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();

    for row_key in ["a", "b", "c"] {
        put_record_row(
            &storage,
            "target",
            row_key,
            &TestRecord(row_key.into()),
            true,
            None,
        )
        .await
        .unwrap();
    }

    // This payload cannot decode as TestRecord. A table-wide scan would fail.
    let unrelated = GatewayMetadataRowProto {
        common: Some(core_meta_committed_row_common(
            gateway_payload_realm_id(&[0xff]),
            gateway_metadata_root_key_hash("other", "broken"),
            GATEWAY_METADATA_CANDIDATE_GENERATION,
            GATEWAY_METADATA_CANDIDATE_TRANSACTION_ID,
            1,
        )),
        schema: GATEWAY_METADATA_ROW_SCHEMA.to_string(),
        row_kind: "other".to_string(),
        row_key: "broken".to_string(),
        generation: 1,
        record_payload: vec![0xff],
        record_payload_hash: format!("sha256:{}", sha256_hex(&[0xff])),
        updated_at: now_rfc3339(),
    };
    let unrelated_key = gateway_metadata_tuple_key("other", "broken").unwrap();
    let unrelated_payload = encode_deterministic_proto(&unrelated);
    let unrelated_op = CoreMetaBatchOp {
        cf: CF_REGISTRY,
        table_id: TABLE_GATEWAY_METADATA_ROW,
        tuple_key: &unrelated_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&unrelated_payload),
    };
    CoreStore::new(storage.clone())
        .await
        .unwrap()
        .commit_coremeta_root_groups(
            "gateway-test-unrelated",
            &[unrelated_op],
            &[CoreMetaRootPublication::new(
                gateway_metadata_root_anchor_key("other", "broken"),
                crate::formats::writer::WriterFamily::Registry,
            )],
        )
        .await
        .unwrap();

    let first = list_record_rows::<TestRecord>(&storage, "target", None, 2)
        .await
        .unwrap();
    assert_eq!(
        first
            .records
            .iter()
            .map(|record| record.row_key.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    let second =
        list_record_rows::<TestRecord>(&storage, "target", first.next_tuple_key.as_deref(), 2)
            .await
            .unwrap();
    assert_eq!(
        second
            .records
            .iter()
            .map(|record| record.row_key.as_str())
            .collect::<Vec<_>>(),
        vec!["c"]
    );
    assert!(second.next_tuple_key.is_none());
    assert!(
        list_record_rows::<TestRecord>(&storage, "target", None, 0)
            .await
            .is_err()
    );
    assert!(
        list_record_rows::<TestRecord>(&storage, "target", None, GATEWAY_METADATA_PAGE_MAX + 1,)
            .await
            .is_err()
    );
}

#[test]
fn gateway_logical_generation_is_independent_of_publication_generation() {
    let logical_generation = 41;
    let payload = encode_gateway_metadata_row(
        "target",
        "logical",
        logical_generation,
        &TestRecord("value".to_string()),
    )
    .unwrap();
    let mut proto = GatewayMetadataRowProto::decode(payload.as_slice()).unwrap();
    let common = proto.common.as_mut().unwrap();
    assert_eq!(
        common.root_generation,
        GATEWAY_METADATA_CANDIDATE_GENERATION
    );
    assert_ne!(common.root_generation, logical_generation);

    common.root_generation = 7;
    common.transaction_id = "gateway-publication-7".to_string();
    let rebound = encode_deterministic_proto(&proto);
    let decoded = decode_gateway_metadata_row::<TestRecord>("target", "logical", &rebound).unwrap();
    assert_eq!(decoded.generation, logical_generation);
    assert_eq!(decoded.record, TestRecord("value".to_string()));
}
