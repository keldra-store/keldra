#[tokio::test]
async fn hybrid_rejects_pathological_term_fanout_before_opening_postings() {
    let definition = definition();
    let (sink, run) = build(
        &definition,
        [upsert("/one", 1, "one", &[1.0, 0.0])],
        0,
        256,
        64 * 1024,
    )
    .await;
    let text = (0..=crate::full_text::MAX_QUERY_TERM_CURSORS)
        .map(|index| format!("term{index}"))
        .collect::<Vec<_>>()
        .join(" ");
    let fields = Vec::new();
    let result = HybridEngine::query(
        &[directory(&sink, run)],
        &definition,
        HybridQuery {
            text: &text,
            vector: &[],
            fields: &fields,
            phrase: false,
            limit: 1,
        },
    )
    .await;
    assert!(matches!(result, Err(IndexError::ResourceLimit { .. })));
}
