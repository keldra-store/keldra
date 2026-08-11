#[test]
fn metadata_builder_rejects_a_source_larger_than_its_budget() {
    let mut builder =
        MetadataSegmentBuilder::new(definition(), SegmentBuildOptions::new(256).unwrap()).unwrap();
    let fields = selected(vec![ScalarValue::String("x".repeat(1024))], 1.0);
    assert!(matches!(
        builder.try_push(IndexMutation::Upsert(MetadataDocument {
            document: DocumentRef {
                path: "/oversized".into(),
                version: 1,
            },
            fields,
        })),
        Err(IndexError::ResourceLimit { .. })
    ));
}

#[test]
fn corrupt_typed_row_count_is_rejected_before_allocation() {
    assert_eq!(
        decode_typed_rows(&u32::MAX.to_le_bytes(), ComponentCodec::FixedRows).unwrap_err(),
        IndexError::InvalidFormat("index component element count")
    );
}

#[tokio::test]
async fn typed_writer_splits_many_short_fields_at_the_decoded_resident_cap() {
    let payload = TypedPayload {
        fields: (0..64)
            .map(|index| (format!("f{index:03}"), vec![ScalarValue::Null]))
            .collect(),
    };
    let row_encoded_bytes = payload.encoded_bytes().saturating_add(16);
    let row_decoded_resident_bytes =
        std::mem::size_of::<TypedRow>().saturating_add(payload.decoded_resident_bytes());
    let row_count = (MAX_INDEX_DECODED_BLOCK_BYTES
        .saturating_sub(ELIAS_FANO_DECODED_FIXED_BYTES)
        / row_decoded_resident_bytes.saturating_add(ELIAS_FANO_DECODED_BYTES_PER_VALUE))
    .saturating_add(1);
    assert!(
        row_count.saturating_mul(row_encoded_bytes) < DEFAULT_COMPONENT_BLOCK_BYTES,
        "the decoded-resident limit, not the encoded target, must force this split"
    );

    let mut sink = MemoryBlockSink::default();
    let mut writer =
        TypedComponentWriter::new(IndexKind::TypedJson, 1, DEFAULT_COMPONENT_BLOCK_BYTES);
    for ordinal in 0..row_count {
        writer
            .push(
                TypedRow {
                    ordinal: ordinal as u64,
                    payload: payload.clone(),
                },
                &mut sink,
            )
            .await
            .unwrap();
        assert!(
            writer
                .decoded_resident_bytes
                .saturating_add(writer.ordinal_decode_resident_bytes(writer.rows.len()))
                <= MAX_INDEX_DECODED_BLOCK_BYTES
        );
    }
    assert!(sink.len() > 0, "the resident cap must flush one leaf");

    let tree = writer.finish(&mut sink).await.unwrap();
    let directory = sink.directory();
    let mut cursor = LeafCursor::new(&directory, tree.root);
    let mut decoded_rows = 0usize;
    let mut leaves = 0usize;
    while let Some(descriptor) = cursor.next().await.unwrap() {
        let rows = read_typed_block(&directory, &descriptor).await.unwrap();
        let resident_bytes = rows
            .iter()
            .fold(0usize, |bytes, row| {
                bytes
                    .saturating_add(std::mem::size_of::<TypedRow>())
                    .saturating_add(row.payload.decoded_resident_bytes())
            })
            .saturating_add(ELIAS_FANO_DECODED_FIXED_BYTES)
            .saturating_add(
                rows.len()
                    .saturating_mul(ELIAS_FANO_DECODED_BYTES_PER_VALUE),
            );
        assert!(resident_bytes <= MAX_INDEX_DECODED_BLOCK_BYTES);
        decoded_rows = decoded_rows.saturating_add(rows.len());
        leaves = leaves.saturating_add(1);
    }
    assert_eq!(decoded_rows, row_count);
    assert_eq!(leaves, 2);
}

#[test]
fn typed_row_too_large_for_one_block_fails_before_admission() {
    let values = (0..1_100)
        .map(|index| ScalarValue::String(format!("{index:04}{}", "x".repeat(3_996))))
        .collect();
    let fields = BTreeMap::from([("status".into(), values)]);
    assert!(matches!(
        preflight_typed_row(&fields),
        Err(IndexError::ResourceLimit { .. })
    ));
}
