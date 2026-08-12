fn qualification_fields(index: u64) -> SelectedScalarFields {
    BTreeMap::from([
        ("active".into(), vec![ScalarValue::Boolean(index % 2 == 0)]),
        (
            "ecosystem".into(),
            vec![ScalarValue::String(
                ["cargo", "npm", "pypi", "maven", "go", "nuget"]
                    [index as usize % 6]
                    .into(),
            )],
        ),
        (
            "modified_day".into(),
            vec![ScalarValue::Number((18_000 + index % 2_365) as f64)],
        ),
        (
            "package".into(),
            vec![ScalarValue::String(format!("package-{index:08x}"))],
        ),
        (
            "partition".into(),
            vec![ScalarValue::Number((index % 1_024) as f64)],
        ),
        (
            "published_day".into(),
            vec![ScalarValue::Number((18_000 + index % 2_000) as f64)],
        ),
        ("record_id".into(), vec![ScalarValue::Number(index as f64)]),
        (
            "score".into(),
            vec![ScalarValue::Number((index % 10_001) as f64 / 100.0)],
        ),
        ("sequence".into(), vec![ScalarValue::Number(index as f64)]),
        (
            "severity".into(),
            vec![ScalarValue::String(
                ["low", "medium", "high", "critical"][index as usize % 4].into(),
            )],
        ),
        (
            "source".into(),
            vec![ScalarValue::String(
                ["feed-a", "feed-b", "feed-c", "feed-d"][index as usize % 4].into(),
            )],
        ),
        (
            "withdrawn".into(),
            vec![ScalarValue::Boolean(index % 97 == 0)],
        ),
    ])
}

#[tokio::test]
async fn production_shaped_l0_seal_splits_every_component_below_the_block_limit() {
    let definition = TypedJsonDefinition {
        fields: [
            "record_id",
            "ecosystem",
            "package",
            "severity",
            "active",
            "withdrawn",
            "score",
            "published_day",
            "modified_day",
            "sequence",
            "source",
            "partition",
        ]
        .into_iter()
        .map(|name| TypedField {
            name: name.into(),
            json_pointer: format!("/{name}"),
        })
        .collect(),
    };
    let mut builder = TypedJsonSegmentBuilder::new(
        definition,
        SegmentBuildOptions::new(64 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    for index in 0..4_002 {
        let mutation = IndexMutation::Upsert(TypedJsonDocument {
            document: DocumentRef {
                path: format!("records/{index:012}.json"),
                version: index + 1,
            },
            fields: qualification_fields(index),
        });
        assert!(matches!(
            builder.try_push(mutation).unwrap(),
            SegmentPush::Accepted
        ));
    }
    let mut sink = MemoryBlockSink::default();
    builder.seal(&mut sink).await.unwrap().unwrap();
}
