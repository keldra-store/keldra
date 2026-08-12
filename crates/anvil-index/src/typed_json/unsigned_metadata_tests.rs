#[test]
fn unsigned_scalar_codec_and_sort_keys_preserve_full_u64_range() {
    let values = [
        ScalarValue::Unsigned(1_u64 << 53),
        ScalarValue::Unsigned((1_u64 << 53) + 1),
        ScalarValue::Unsigned(u64::MAX),
    ];
    let keys = values
        .iter()
        .map(sortable_scalar_key)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    for value in values {
        let mut encoder = Encoder::default();
        encode_scalar(&mut encoder, &value).unwrap();
        let encoded = encoder.finish();
        let mut decoder = Decoder::new(&encoded);
        assert_eq!(decode_scalar(&mut decoder).unwrap(), value);
        decoder.finish().unwrap();
    }
}

#[tokio::test]
async fn metadata_equality_distinguishes_adjacent_u64_values_above_two_to_53() {
    let definition = TypedJsonDefinition {
        fields: vec![TypedField {
            name: "version".into(),
            json_pointer: "/version".into(),
        }],
    };
    let first = (1_u64 << 53) + 1;
    let second = first + 1;
    let mut builder = MetadataSegmentBuilder::new(
        definition.clone(),
        SegmentBuildOptions::for_level(64 * 1024, 1).unwrap(),
    )
    .unwrap();
    for (path, version) in [("/a", first), ("/b", second)] {
        assert!(matches!(
            builder
                .try_push(IndexMutation::Upsert(MetadataDocument {
                    document: DocumentRef {
                        path: path.into(),
                        version,
                    },
                    fields: BTreeMap::from([(
                        "version".into(),
                        vec![ScalarValue::Unsigned(version)],
                    )]),
                }))
                .unwrap(),
            SegmentPush::Accepted
        ));
    }
    let mut sink = MemoryBlockSink::default();
    let run = builder.seal(&mut sink).await.unwrap().unwrap();
    let hits = MetadataFilterEngine::query(
        &[directory(&sink, run)],
        &definition,
        &TypedQuery {
            predicates: vec![TypedPredicate::Equal {
                field: "version".into(),
                value: ScalarValue::Unsigned(second),
            }],
            order: Vec::new(),
            limit: 10,
        },
    )
    .await
    .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].document.path, "/b");
}
