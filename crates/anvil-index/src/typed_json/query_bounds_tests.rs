#[tokio::test]
async fn query_rejects_a_page_whose_retained_projections_exceed_one_decode_budget() {
    let values = (0..100)
        .map(|index| ScalarValue::String(format!("{index:03}{}", "x".repeat(3_900))))
        .collect::<Vec<_>>();
    let mutations = (0..12)
        .map(|index| {
            upsert_fields(
                &format!("/large/{index:02}"),
                1,
                selected(values.clone(), index as f64),
            )
        })
        .collect::<Vec<_>>();
    let mut builder = TypedJsonSegmentBuilder::new(
        definition(),
        SegmentBuildOptions::new(16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    for mutation in mutations {
        assert!(matches!(
            builder.try_push(mutation).unwrap(),
            SegmentPush::Accepted
        ));
    }
    let mut sink = MemoryBlockSink::default();
    let run = builder.seal(&mut sink).await.unwrap().unwrap();
    let result = TypedJsonEngine::query(
        &[directory(&sink, run)],
        &definition(),
        &TypedQuery {
            limit: 12,
            ..exists_query()
        },
    )
    .await;
    assert!(matches!(result, Err(IndexError::ResourceLimit { .. })));
}
