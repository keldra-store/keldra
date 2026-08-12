async fn query_paths(runs: &[MemoryDirectory], predicate: TypedPredicate) -> Vec<String> {
    TypedJsonEngine::query(
        runs,
        &definition(),
        &TypedQuery {
            predicates: vec![predicate],
            order: Vec::new(),
            limit: 100,
        },
    )
    .await
    .unwrap()
    .into_iter()
    .map(|hit| hit.document.path)
    .collect()
}

#[tokio::test]
async fn compressed_postings_preserve_types_sets_ranges_prefixes_and_arrays() {
    let entries = [
        ("/null", ScalarValue::Null, 1.0),
        ("/false", ScalarValue::Boolean(false), 2.0),
        ("/true", ScalarValue::Boolean(true), 3.0),
        ("/number", ScalarValue::Number(1.0), 4.0),
        ("/string", ScalarValue::String("1".into()), 5.0),
    ]
    .into_iter()
    .map(|(path, status, amount)| upsert_fields(path, 1, selected(vec![status], amount)))
    .chain([upsert_fields(
        "/array",
        1,
        selected(
            vec![
                ScalarValue::String("alpha".into()),
                ScalarValue::String("beta".into()),
                ScalarValue::String("beta".into()),
            ],
            6.0,
        ),
    )]);
    let (sink, run) = build_run(entries, 1, 128).await;
    let runs = [directory(&sink, run)];

    assert_eq!(
        query_paths(
            &runs,
            TypedPredicate::Equal {
                field: "status".into(),
                value: ScalarValue::Number(1.0),
            },
        )
        .await,
        ["/number"]
    );
    assert_eq!(
        query_paths(
            &runs,
            TypedPredicate::Equal {
                field: "status".into(),
                value: ScalarValue::String("1".into()),
            },
        )
        .await,
        ["/string"]
    );
    assert_eq!(
        query_paths(
            &runs,
            TypedPredicate::In {
                field: "status".into(),
                values: vec![ScalarValue::Null, ScalarValue::Boolean(true)],
            },
        )
        .await,
        ["/null", "/true"]
    );
    assert_eq!(
        query_paths(
            &runs,
            TypedPredicate::Prefix {
                field: "status".into(),
                prefix: "bet".into(),
            },
        )
        .await,
        ["/array"]
    );
    assert_eq!(
        query_paths(
            &runs,
            TypedPredicate::LessThan {
                field: "amount".into(),
                value: ScalarValue::Number(3.0),
            },
        )
        .await,
        ["/false", "/null"]
    );
    assert_eq!(
        query_paths(
            &runs,
            TypedPredicate::LessThanOrEqual {
                field: "amount".into(),
                value: ScalarValue::Number(3.0),
            },
        )
        .await,
        ["/false", "/null", "/true"]
    );
    assert_eq!(
        query_paths(
            &runs,
            TypedPredicate::GreaterThan {
                field: "amount".into(),
                value: ScalarValue::Number(5.0),
            },
        )
        .await,
        ["/array"]
    );
    assert_eq!(
        query_paths(
            &runs,
            TypedPredicate::GreaterThanOrEqual {
                field: "amount".into(),
                value: ScalarValue::Number(5.0),
            },
        )
        .await,
        ["/array", "/string"]
    );
    assert_eq!(
        query_paths(
            &runs,
            TypedPredicate::Exists {
                field: "status".into(),
            },
        )
        .await
        .len(),
        6
    );
}
