use super::*;

const ACTIVE: &[&str] = &[
    "docs/active-c.json",
    "docs/active-b.json",
    "docs/active-a.json",
];

pub(super) fn keyword_field(
    name: &str,
    json_pointer: &str,
    capabilities: &[IndexFieldCapability],
) -> IndexField {
    IndexField {
        name: name.into(),
        json_pointer: json_pointer.into(),
        cardinality: IndexFieldCardinality::Single as i32,
        capabilities: capabilities.iter().map(|value| *value as i32).collect(),
        field_type: Some(IndexFieldType::Keyword(KeywordIndexField {})),
    }
}

pub(super) fn keyword_multi_field(
    name: &str,
    json_pointer: &str,
    capabilities: &[IndexFieldCapability],
) -> IndexField {
    let mut field = keyword_field(name, json_pointer, capabilities);
    field.cardinality = IndexFieldCardinality::Multi as i32;
    field
}

pub(super) fn signed_integer_field(
    name: &str,
    json_pointer: &str,
    capabilities: &[IndexFieldCapability],
) -> IndexField {
    IndexField {
        name: name.into(),
        json_pointer: json_pointer.into(),
        cardinality: IndexFieldCardinality::Single as i32,
        capabilities: capabilities.iter().map(|value| *value as i32).collect(),
        field_type: Some(IndexFieldType::SignedInteger(SignedIntegerIndexField {})),
    }
}

pub(super) fn signed_integer_multi_field(
    name: &str,
    json_pointer: &str,
    capabilities: &[IndexFieldCapability],
) -> IndexField {
    let mut field = signed_integer_field(name, json_pointer, capabilities);
    field.cardinality = IndexFieldCardinality::Multi as i32;
    field
}

pub(super) fn unsigned_integer_field(
    name: &str,
    json_pointer: &str,
    capabilities: &[IndexFieldCapability],
) -> IndexField {
    IndexField {
        name: name.into(),
        json_pointer: json_pointer.into(),
        cardinality: IndexFieldCardinality::Single as i32,
        capabilities: capabilities.iter().map(|value| *value as i32).collect(),
        field_type: Some(IndexFieldType::UnsignedInteger(
            UnsignedIntegerIndexField {},
        )),
    }
}

pub(super) fn float_field(
    name: &str,
    json_pointer: &str,
    capabilities: &[IndexFieldCapability],
) -> IndexField {
    IndexField {
        name: name.into(),
        json_pointer: json_pointer.into(),
        cardinality: IndexFieldCardinality::Single as i32,
        capabilities: capabilities.iter().map(|value| *value as i32).collect(),
        field_type: Some(IndexFieldType::Float(FloatIndexField {})),
    }
}

pub(super) fn boolean_field(
    name: &str,
    json_pointer: &str,
    capabilities: &[IndexFieldCapability],
) -> IndexField {
    IndexField {
        name: name.into(),
        json_pointer: json_pointer.into(),
        cardinality: IndexFieldCardinality::Single as i32,
        capabilities: capabilities.iter().map(|value| *value as i32).collect(),
        field_type: Some(IndexFieldType::Boolean(BooleanIndexField {})),
    }
}

pub(super) fn text_field(name: &str, json_pointer: &str) -> IndexField {
    IndexField {
        name: name.into(),
        json_pointer: json_pointer.into(),
        cardinality: IndexFieldCardinality::Single as i32,
        capabilities: vec![IndexFieldCapability::FullText as i32],
        field_type: Some(IndexFieldType::Text(TextIndexField {
            analyzer: TextAnalyzer::UnicodeAlphanumericLowercase as i32,
        })),
    }
}

pub(super) fn typed_json_order() -> Vec<IndexOrder> {
    vec![
        IndexOrder {
            field: "modified_at".into(),
            direction: IndexOrderDirection::Descending as i32,
        },
        IndexOrder {
            field: "source_record_id".into(),
            direction: IndexOrderDirection::Ascending as i32,
        },
    ]
}

pub(super) async fn qualify(
    clients: &mut [IndexClient],
    tenant: &str,
    case: &EngineCase,
    commit_revision: &IndexFreshness,
) -> TestResult<()> {
    let checks = [
        (
            "exact keyword",
            predicates([predicate(
                "status",
                IndexPredicateOperator::Equal,
                &["\"active\""],
            )]),
            ACTIVE,
        ),
        (
            "keyword membership",
            predicates([predicate(
                "source_record_id",
                IndexPredicateOperator::In,
                &["\"a\"", "\"z\""],
            )]),
            &ACTIVE[..2],
        ),
        (
            "keyword prefix",
            predicates([predicate(
                "status",
                IndexPredicateOperator::Prefix,
                &["\"act\""],
            )]),
            ACTIVE,
        ),
        (
            "keyword range",
            predicates([predicate(
                "source_record_id",
                IndexPredicateOperator::LessThan,
                &["\"x\""],
            )]),
            &["docs/active-c.json", "docs/active-a.json"],
        ),
        (
            "signed exact point",
            predicates([predicate(
                "modified_at",
                IndexPredicateOperator::Equal,
                &["100"],
            )]),
            &["docs/active-a.json"],
        ),
        (
            "signed range point",
            predicates([predicate(
                "modified_at",
                IndexPredicateOperator::GreaterThanOrEqual,
                &["200"],
            )]),
            &[
                "docs/inactive.json",
                "docs/active-c.json",
                "docs/active-b.json",
            ],
        ),
        (
            "unsigned exact point",
            predicates([predicate("sequence", IndexPredicateOperator::Equal, &["2"])]),
            &["docs/active-b.json"],
        ),
        (
            "float range point",
            predicates([predicate(
                "score",
                IndexPredicateOperator::GreaterThanOrEqual,
                &["2.5"],
            )]),
            &[
                "docs/inactive.json",
                "docs/active-c.json",
                "docs/active-b.json",
            ],
        ),
        (
            "boolean exact term",
            predicates([predicate(
                "enabled",
                IndexPredicateOperator::Equal,
                &["false"],
            )]),
            &["docs/inactive.json"],
        ),
        (
            "multi-valued keyword exact",
            predicates([predicate(
                "labels",
                IndexPredicateOperator::Equal,
                &["\"stable\""],
            )]),
            ACTIVE,
        ),
        (
            "doc-value-only existence",
            predicates([predicate(
                "measurements",
                IndexPredicateOperator::Exists,
                &[],
            )]),
            &[
                "docs/inactive.json",
                "docs/active-c.json",
                "docs/active-b.json",
                "docs/active-a.json",
            ],
        ),
        (
            "fielded full text",
            predicates([predicate(
                "summary",
                IndexPredicateOperator::FullText,
                &["\"durable\""],
            )]),
            ACTIVE,
        ),
        (
            "fielded phrase",
            predicates([predicate(
                "summary",
                IndexPredicateOperator::Phrase,
                &["\"durable journal\""],
            )]),
            ACTIVE,
        ),
    ];

    for (label, query, expected) in checks {
        let responses = execute(clients, case, query).await?;
        verify_pages(
            &responses,
            tenant,
            case.bucket,
            commit_revision,
            expected,
            label,
        )?;
    }

    let mut computations = predicates([predicate(
        "status",
        IndexPredicateOperator::Equal,
        &["\"active\""],
    )]);
    computations.facets = vec![
        IndexFacetRequest {
            field: "status".into(),
            limit: 10,
        },
        IndexFacetRequest {
            field: "source_record_id".into(),
            limit: 10,
        },
        IndexFacetRequest {
            field: "modified_at".into(),
            limit: 10,
        },
        IndexFacetRequest {
            field: "sequence".into(),
            limit: 10,
        },
        IndexFacetRequest {
            field: "score".into(),
            limit: 10,
        },
        IndexFacetRequest {
            field: "enabled".into(),
            limit: 10,
        },
        IndexFacetRequest {
            field: "labels".into(),
            limit: 10,
        },
        IndexFacetRequest {
            field: "measurements".into(),
            limit: 10,
        },
    ];
    computations.aggregates = ["modified_at", "sequence", "score", "measurements"]
        .into_iter()
        .flat_map(|field| {
            [
                IndexAggregateOperation::Count,
                IndexAggregateOperation::Minimum,
                IndexAggregateOperation::Maximum,
                IndexAggregateOperation::Sum,
                IndexAggregateOperation::Average,
            ]
            .into_iter()
            .map(move |operation| IndexAggregateRequest {
                field: field.into(),
                operation: operation as i32,
            })
        })
        .collect();
    let responses = execute(clients, case, computations).await?;
    verify_pages(
        &responses,
        tenant,
        case.bucket,
        commit_revision,
        ACTIVE,
        "facets and aggregates",
    )?;
    for response in &responses {
        verify_computations(response)?;
    }

    println!(
        "{} exercised all Typed JSON field types and declared query capabilities through every public endpoint",
        case.name
    );
    Ok(())
}

fn predicate(
    field: &str,
    operator: IndexPredicateOperator,
    values_json: &[&str],
) -> IndexPredicate {
    IndexPredicate {
        field: field.into(),
        operator: operator as i32,
        values_json: values_json
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect(),
    }
}

fn predicates<const N: usize>(values: [IndexPredicate; N]) -> TypedJsonIndexQuery {
    TypedJsonIndexQuery {
        predicates: values.into(),
        order: typed_json_order(),
        facets: Vec::new(),
        aggregates: Vec::new(),
    }
}

async fn execute(
    clients: &mut [IndexClient],
    case: &EngineCase,
    query: TypedJsonIndexQuery,
) -> TestResult<Vec<QueryIndexResponse>> {
    let request = QueryIndexRequest {
        bucket: case.bucket.into(),
        index_name: case.name.into(),
        query: Some(super::query(QueryValue::TypedJson(query))),
        limit: 100,
        page_token: Vec::new(),
        tenant: String::new(),
        required_freshness: None,
    };
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        let mut responses = Vec::with_capacity(clients.len());
        for client in clients.iter_mut() {
            match client.query_index(request.clone()).await {
                Ok(response) => responses.push(response.into_inner()),
                Err(status) if retryable_transport(&status) && Instant::now() < deadline => {
                    responses.clear();
                    break;
                }
                Err(status) => return Err(status.into()),
            }
        }
        if responses.len() == clients.len() && routed_responses_agree(&responses) {
            return Ok(responses);
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "{} capability query did not agree across endpoints",
                case.name
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

fn verify_pages(
    responses: &[QueryIndexResponse],
    tenant: &str,
    bucket: &str,
    commit_revision: &IndexFreshness,
    expected: &[&str],
    label: &str,
) -> TestResult<()> {
    for response in responses {
        if !response.next_page_token.is_empty()
            || !stable_freshness_agrees(response.freshness.as_ref(), Some(commit_revision))
        {
            return Err(invalid(format!(
                "{label} did not remain on the complete qualification commit_revision"
            )));
        }
        let paths = response
            .hits
            .iter()
            .map(|hit| {
                let address = hit
                    .address
                    .as_ref()
                    .ok_or_else(|| invalid(format!("{label} returned a hit without identity")))?;
                if address.tenant != tenant
                    || address.bucket != bucket
                    || hit.object_version == 0
                    || hit.score.is_some()
                {
                    return Err(invalid(format!(
                        "{label} returned an invalid identity-only Typed JSON hit"
                    )));
                }
                Ok(address.path.as_str())
            })
            .collect::<TestResult<Vec<_>>>()?;
        if paths != expected {
            return Err(invalid(format!(
                "{label} returned {paths:?}, expected {expected:?}"
            )));
        }
    }
    Ok(())
}

fn verify_computations(response: &QueryIndexResponse) -> TestResult<()> {
    if response.facet_results.len() != 8 || response.aggregate_results.len() != 20 {
        return Err(invalid("Typed JSON computation result count changed"));
    }
    let expected_facets: &[(&str, &[(&[u8], u64)])] = &[
        ("status", &[(b"\"active\"", 3)]),
        (
            "source_record_id",
            &[(b"\"a\"", 1), (b"\"b\"", 1), (b"\"z\"", 1)],
        ),
        ("modified_at", &[(b"200", 2), (b"100", 1)]),
        ("sequence", &[(b"1", 1), (b"2", 1), (b"3", 1)]),
        ("score", &[(b"1.5", 1), (b"2.5", 1), (b"3.5", 1)]),
        ("enabled", &[(b"true", 3)]),
        (
            "labels",
            &[
                (b"\"stable\"", 3),
                (b"\"alpha\"", 1),
                (b"\"beta\"", 1),
                (b"\"gamma\"", 1),
            ],
        ),
        (
            "measurements",
            &[(b"1", 1), (b"2", 1), (b"3", 1), (b"4", 1)],
        ),
    ];
    for (result, (field, buckets)) in response.facet_results.iter().zip(expected_facets) {
        if result.field != *field
            || result.buckets.len() != buckets.len()
            || result
                .buckets
                .iter()
                .zip(*buckets)
                .any(|(actual, (value, count))| {
                    actual.value_json.as_slice() != *value || actual.count != *count
                })
        {
            return Err(invalid(format!("facet result for {field} changed")));
        }
    }

    let expected = [
        ("modified_at", IndexAggregateOperation::Count, 3.0, 3),
        ("modified_at", IndexAggregateOperation::Minimum, 100.0, 3),
        ("modified_at", IndexAggregateOperation::Maximum, 200.0, 3),
        ("modified_at", IndexAggregateOperation::Sum, 500.0, 3),
        (
            "modified_at",
            IndexAggregateOperation::Average,
            500.0 / 3.0,
            3,
        ),
        ("sequence", IndexAggregateOperation::Count, 3.0, 3),
        ("sequence", IndexAggregateOperation::Minimum, 1.0, 3),
        ("sequence", IndexAggregateOperation::Maximum, 3.0, 3),
        ("sequence", IndexAggregateOperation::Sum, 6.0, 3),
        ("sequence", IndexAggregateOperation::Average, 2.0, 3),
        ("score", IndexAggregateOperation::Count, 3.0, 3),
        ("score", IndexAggregateOperation::Minimum, 1.5, 3),
        ("score", IndexAggregateOperation::Maximum, 3.5, 3),
        ("score", IndexAggregateOperation::Sum, 7.5, 3),
        ("score", IndexAggregateOperation::Average, 2.5, 3),
        ("measurements", IndexAggregateOperation::Count, 5.0, 5),
        ("measurements", IndexAggregateOperation::Minimum, 1.0, 5),
        ("measurements", IndexAggregateOperation::Maximum, 4.0, 5),
        ("measurements", IndexAggregateOperation::Sum, 11.0, 5),
        ("measurements", IndexAggregateOperation::Average, 2.2, 5),
    ];
    for (result, (field, operation, expected_value, expected_count)) in
        response.aggregate_results.iter().zip(expected)
    {
        let value = result
            .value_json
            .as_deref()
            .ok_or_else(|| invalid("numeric aggregate omitted its result"))?;
        let value: f64 = serde_json::from_slice(value)?;
        if result.field != field
            || result.operation != operation as i32
            || result.contributing_count != expected_count
            || (value - expected_value).abs() > 1e-12
        {
            return Err(invalid("numeric aggregate result changed"));
        }
    }
    Ok(())
}
