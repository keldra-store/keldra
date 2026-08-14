use super::*;
use crate::v4::{
    FIELD_PRESENCE_TERM, NativeQueryStatisticsRecorder, Predicate, TERM_TYPE_FIELD_PRESENCE,
};

fn presence_term(field_id: u32) -> ProjectedTerm {
    ProjectedTerm {
        field_id: FieldId::new(field_id),
        term_type: TERM_TYPE_FIELD_PRESENCE,
        term: FIELD_PRESENCE_TERM.to_vec(),
        frequency: 1,
        positions: Vec::new(),
    }
}

async fn build_segment(
    sink: &mut ExactMemorySink,
    schema: &Schema,
    index_id: u64,
    segment_id: u64,
    sources: Vec<ProjectedSource>,
) -> super::super::super::super::SegmentDescriptor {
    let identity =
        SegmentIdentity::new(index_id, 1, schema.fingerprint().unwrap(), segment_id).unwrap();
    let mut writer = NativeSegmentWriter::new(
        identity,
        schema.clone(),
        BuildLimits::new(64 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    for source in sources {
        assert_eq!(writer.push_source(source).unwrap(), SourcePush::Accepted);
    }
    writer.seal(sink).await.unwrap().descriptor
}

fn text_source(path: &str, token: &str, frequency: u32, length: u32) -> ProjectedSource {
    let (term_type, term) = text_term(token).unwrap();
    one_record_source(
        path,
        None,
        vec![ProjectedTerm {
            field_id: FieldId::new(0),
            term_type,
            term,
            frequency,
            positions: (0..frequency).collect(),
        }],
        Vec::new(),
        Vec::new(),
        vec![(FieldId::new(0), length)],
    )
}

#[tokio::test]
async fn bm25_ranking_uses_statistics_from_nonmatching_segments() {
    let text_components = FieldComponents::TERMS
        .union(FieldComponents::POSITIONS)
        .union(FieldComponents::NORMS)
        .union(FieldComponents::STORED);
    let mut schema = Schema {
        kind: IndexKind::FullText,
        path_prefix: String::new(),
        content_type_scope: None,
        fields: vec![field(0, "body", text_components)],
        semantics: IndexSemantics::FullText {
            analyzer: Analyzer::UnicodeAlphanumericLowercase,
            bm25_k1: 1.2,
            bm25_b: 0.75,
        },
        physical_order: Vec::new(),
        component_versions: Vec::new(),
    };
    schema.component_versions = versions(&schema.fields);

    let mut sink = ExactMemorySink::new();
    let segments = vec![
        build_segment(
            &mut sink,
            &schema,
            31,
            1,
            vec![text_source("candidate/high-tf", "hello", 3, 100)],
        )
        .await,
        build_segment(
            &mut sink,
            &schema,
            31,
            2,
            vec![text_source("candidate/short", "hello", 1, 1)],
        )
        .await,
        // This segment has no query posting, but its documents and field
        // lengths still belong to the pinned generation's BM25 corpus.
        build_segment(
            &mut sink,
            &schema,
            31,
            3,
            vec![text_source("non-match/long", "other", 1_000, 1_000)],
        )
        .await,
    ];
    let directory = MemoryArtifacts::from_sink(&sink);
    let gate = TestGate {
        revision: 1,
        denied: BTreeSet::new(),
        batches: Mutex::new(Vec::new()),
    };
    let page = NativeQueryExecutor::new(&directory, &gate, NativeQueryLimits::default())
        .unwrap()
        .execute(&NativeQueryRequest {
            schema,
            segments,
            query: NativeQuery::FullText {
                text: "hello".into(),
                phrase: false,
            },
            after: None,
            limit: 2,
            authorization_revision: 1,
        })
        .await
        .unwrap();

    assert_eq!(
        page.hits
            .iter()
            .map(|hit| hit.result.path.as_str())
            .collect::<Vec<_>>(),
        ["candidate/high-tf", "candidate/short"]
    );
    assert!(page.hits[0].score.unwrap() > page.hits[1].score.unwrap());
}

#[tokio::test]
async fn full_text_block_max_skips_only_noncompetitive_ranges() {
    let text_components = FieldComponents::TERMS
        .union(FieldComponents::POSITIONS)
        .union(FieldComponents::NORMS)
        .union(FieldComponents::STORED);
    let mut schema = Schema {
        kind: IndexKind::FullText,
        path_prefix: String::new(),
        content_type_scope: None,
        fields: vec![field(0, "body", text_components)],
        semantics: IndexSemantics::FullText {
            analyzer: Analyzer::UnicodeAlphanumericLowercase,
            bm25_k1: 1.2,
            bm25_b: 0.75,
        },
        physical_order: Vec::new(),
        component_versions: Vec::new(),
    };
    schema.component_versions = versions(&schema.fields);
    let mut sink = ExactMemorySink::new();
    let winner = build_segment(
        &mut sink,
        &schema,
        39,
        1,
        vec![text_source("winner", "hello", 100, 1)],
    )
    .await;
    let ordinary = (0..200)
        .map(|ordinal| text_source(&format!("ordinary/{ordinal:03}"), "hello", 1, 1))
        .collect();
    let ordinary = build_segment(&mut sink, &schema, 39, 2, ordinary).await;
    let directory = MemoryArtifacts::from_sink(&sink);
    let gate = TestGate {
        revision: 1,
        denied: BTreeSet::new(),
        batches: Mutex::new(Vec::new()),
    };
    let executor =
        NativeQueryExecutor::new(&directory, &gate, NativeQueryLimits::default()).unwrap();
    let query = NativeQuery::FullText {
        text: "hello".into(),
        phrase: false,
    };
    let optimized = executor
        .execute(&NativeQueryRequest {
            schema: schema.clone(),
            segments: vec![winner.clone(), ordinary.clone()],
            query: query.clone(),
            after: None,
            limit: 1,
            authorization_revision: 1,
        })
        .await
        .unwrap();
    let exhaustive = executor
        .execute(&NativeQueryRequest {
            schema,
            segments: vec![winner, ordinary],
            query,
            after: None,
            limit: 201,
            authorization_revision: 1,
        })
        .await
        .unwrap();

    assert_eq!(optimized.hits[0], exhaustive.hits[0]);
    assert_eq!(optimized.hits[0].result.path, "winner");
    assert!(optimized.statistics.posting_advance_calls > 0);
    assert!(optimized.statistics.cursor_seeks > 0);
    assert!(optimized.statistics.cursor_skipped_doc_ids >= 200);
    assert_eq!(optimized.statistics.candidate_doc_ids, 1);
    assert_eq!(exhaustive.statistics.cursor_seeks, 0);
    assert_eq!(exhaustive.statistics.candidate_doc_ids, 201);
}

#[tokio::test]
async fn equal_score_identity_tie_is_not_impact_skipped() {
    let text_components = FieldComponents::TERMS
        .union(FieldComponents::POSITIONS)
        .union(FieldComponents::NORMS)
        .union(FieldComponents::STORED);
    let mut schema = Schema {
        kind: IndexKind::FullText,
        path_prefix: String::new(),
        content_type_scope: None,
        fields: vec![field(0, "body", text_components)],
        semantics: IndexSemantics::FullText {
            analyzer: Analyzer::UnicodeAlphanumericLowercase,
            bm25_k1: 1.2,
            bm25_b: 0.75,
        },
        physical_order: Vec::new(),
        component_versions: Vec::new(),
    };
    schema.component_versions = versions(&schema.fields);
    let mut sink = ExactMemorySink::new();
    let segments = vec![
        build_segment(
            &mut sink,
            &schema,
            40,
            1,
            vec![text_source("z", "hello", 1, 1)],
        )
        .await,
        build_segment(
            &mut sink,
            &schema,
            40,
            2,
            vec![text_source("a", "hello", 1, 1)],
        )
        .await,
    ];
    let directory = MemoryArtifacts::from_sink(&sink);
    let gate = TestGate {
        revision: 1,
        denied: BTreeSet::new(),
        batches: Mutex::new(Vec::new()),
    };
    let page = NativeQueryExecutor::new(&directory, &gate, NativeQueryLimits::default())
        .unwrap()
        .execute(&NativeQueryRequest {
            schema,
            segments,
            query: NativeQuery::FullText {
                text: "hello".into(),
                phrase: false,
            },
            after: None,
            limit: 1,
            authorization_revision: 1,
        })
        .await
        .unwrap();

    assert_eq!(page.hits[0].result.path, "a");
    assert!(page.statistics.posting_advance_calls > 0);
    assert_eq!(page.statistics.cursor_seeks, 0);
}

async fn assert_l2_euclidean_query_is_normalized(
    index_id: u64,
    mut schema: Schema,
    source: ProjectedSource,
    query: NativeQuery,
) {
    schema.component_versions = versions(&schema.fields);
    let mut sink = ExactMemorySink::new();
    let segment = build_segment(&mut sink, &schema, index_id, 1, vec![source]).await;
    let directory = MemoryArtifacts::from_sink(&sink);
    let gate = TestGate {
        revision: 1,
        denied: BTreeSet::new(),
        batches: Mutex::new(Vec::new()),
    };
    let page = NativeQueryExecutor::new(&directory, &gate, NativeQueryLimits::default())
        .unwrap()
        .execute(&NativeQueryRequest {
            schema,
            segments: vec![segment],
            query,
            after: None,
            limit: 1,
            authorization_revision: 1,
        })
        .await
        .unwrap();
    assert_eq!(page.hits.len(), 1);
    assert!(page.hits[0].score.unwrap().abs() <= f32::EPSILON);
}

#[tokio::test]
async fn l2_normalization_is_symmetric_for_vector_and_hybrid_queries() {
    let vector_field = field(
        0,
        "embedding",
        FieldComponents::VECTOR.union(FieldComponents::STORED),
    );
    assert_l2_euclidean_query_is_normalized(
        32,
        Schema {
            kind: IndexKind::Vector,
            path_prefix: String::new(),
            content_type_scope: None,
            fields: vec![vector_field],
            semantics: IndexSemantics::Vector {
                dimensions: 2,
                metric: VectorMetric::Euclidean,
                normalization: VectorNormalization::L2,
            },
            physical_order: Vec::new(),
            component_versions: Vec::new(),
        },
        one_record_source(
            "vectors/unit",
            None,
            Vec::new(),
            Vec::new(),
            vec![ProjectedVector {
                field_id: FieldId::new(0),
                values: vec![0.6, 0.8],
            }],
            Vec::new(),
        ),
        NativeQuery::Vector {
            values: vec![3.0, 4.0],
        },
    )
    .await;

    let text_components = FieldComponents::TERMS
        .union(FieldComponents::POSITIONS)
        .union(FieldComponents::NORMS)
        .union(FieldComponents::STORED);
    assert_l2_euclidean_query_is_normalized(
        33,
        Schema {
            kind: IndexKind::Hybrid,
            path_prefix: String::new(),
            content_type_scope: None,
            fields: vec![
                field(0, "body", text_components),
                field(1, "embedding", FieldComponents::VECTOR),
            ],
            semantics: IndexSemantics::Hybrid {
                analyzer: Analyzer::UnicodeAlphanumericLowercase,
                bm25_k1: 1.2,
                bm25_b: 0.75,
                dimensions: 2,
                metric: VectorMetric::Euclidean,
                normalization: VectorNormalization::L2,
                lexical_weight: 1.0,
                vector_weight: 1.0,
            },
            physical_order: Vec::new(),
            component_versions: Vec::new(),
        },
        one_record_source(
            "hybrid/unit",
            None,
            Vec::new(),
            Vec::new(),
            vec![ProjectedVector {
                field_id: FieldId::new(1),
                values: vec![0.6, 0.8],
            }],
            Vec::new(),
        ),
        NativeQuery::Hybrid {
            text: String::new(),
            vector: vec![3.0, 4.0],
        },
    )
    .await;
}

fn repeated_record(
    terms: Vec<ProjectedTerm>,
    order_field: FieldId,
    order_value: &str,
    ordinal: u32,
) -> ProjectedRecord {
    ProjectedRecord {
        result_identity: Some(ObjectIdentity {
            path: "shared/result".into(),
            version: 7,
        }),
        order_key: Vec::new(),
        terms,
        columns: vec![ProjectedColumn {
            field_id: order_field,
            multi_valued: false,
            cell: FastColumnCell::value(ScalarValue::String(order_value.into())),
        }],
        stored_fields: Some(format!(r#"{{"ordinal":{ordinal}}}"#).into_bytes()),
        vectors: Vec::new(),
        field_lengths: Vec::new(),
    }
}

async fn assert_multi_record_pagination(
    index_id: u64,
    mut schema: Schema,
    source: ProjectedSource,
    query: NativeQuery,
) {
    schema.component_versions = versions(&schema.fields);
    let mut sink = ExactMemorySink::new();
    let segment = build_segment(&mut sink, &schema, index_id, 1, vec![source]).await;
    let directory = MemoryArtifacts::from_sink(&sink);
    let gate = TestGate {
        revision: 1,
        denied: BTreeSet::new(),
        batches: Mutex::new(Vec::new()),
    };
    let executor =
        NativeQueryExecutor::new(&directory, &gate, NativeQueryLimits::default()).unwrap();
    let mut request = NativeQueryRequest {
        schema,
        segments: vec![segment],
        query,
        after: None,
        limit: 1,
        authorization_revision: 1,
    };
    let first = executor.execute(&request).await.unwrap();
    assert_eq!(first.hits.len(), 1);
    assert_eq!(first.hits[0].cursor.source_record, 0);
    request.after = first.next;
    let second = executor.execute(&request).await.unwrap();
    assert_eq!(second.hits.len(), 1);
    assert_eq!(second.hits[0].cursor.source_record, 1);
    assert_eq!(first.hits[0].result, second.hits[0].result);
    assert_eq!(first.hits[0].source, second.hits[0].source);
    request.after = second.next;
    assert!(executor.execute(&request).await.unwrap().hits.is_empty());
}

#[tokio::test]
async fn git_and_tensor_pagination_uses_stable_source_record_tie_breaks() {
    let git_terms = || {
        vec![
            scalar_projected_term(0, "repo"),
            scalar_projected_term(1, "commit"),
            scalar_projected_term(2, "src/lib.rs"),
        ]
    };
    assert_multi_record_pagination(
        34,
        Schema {
            kind: IndexKind::GitSource,
            path_prefix: String::new(),
            content_type_scope: None,
            fields: vec![
                field(0, "repository", FieldComponents::TERMS),
                field(1, "commit", FieldComponents::TERMS),
                field(
                    2,
                    "tree_path",
                    FieldComponents::TERMS
                        .union(FieldComponents::FAST_COLUMN)
                        .union(FieldComponents::STORED),
                ),
            ],
            semantics: IndexSemantics::GitSource {
                repository_scope: "repo".into(),
            },
            physical_order: Vec::new(),
            component_versions: Vec::new(),
        },
        ProjectedSource {
            source_identity: ObjectIdentity {
                path: "git/manifest".into(),
                version: 3,
            },
            records: vec![
                repeated_record(git_terms(), FieldId::new(2), "src/lib.rs", 0),
                repeated_record(git_terms(), FieldId::new(2), "src/lib.rs", 1),
            ],
        },
        NativeQuery::GitSource {
            repository_id: "repo".into(),
            commit_id: "commit".into(),
            tree_path: "src/lib.rs".into(),
            prefix: false,
        },
    )
    .await;

    let tensor_terms = || {
        vec![
            scalar_projected_term(0, "model"),
            scalar_projected_term(1, "weights"),
        ]
    };
    assert_multi_record_pagination(
        35,
        Schema {
            kind: IndexKind::Tensor,
            path_prefix: String::new(),
            content_type_scope: None,
            fields: vec![
                field(0, "model", FieldComponents::TERMS),
                field(
                    1,
                    "tensor",
                    FieldComponents::TERMS
                        .union(FieldComponents::FAST_COLUMN)
                        .union(FieldComponents::STORED),
                ),
            ],
            semantics: IndexSemantics::Tensor {
                model_scope: "model".into(),
            },
            physical_order: Vec::new(),
            component_versions: Vec::new(),
        },
        ProjectedSource {
            source_identity: ObjectIdentity {
                path: "tensor/manifest".into(),
                version: 5,
            },
            records: vec![
                repeated_record(tensor_terms(), FieldId::new(1), "weights", 0),
                repeated_record(tensor_terms(), FieldId::new(1), "weights", 1),
            ],
        },
        NativeQuery::Tensor {
            model_id: "model".into(),
            tensor_name: "weights".into(),
        },
    )
    .await;
}

fn equal(id: u32, field_id: u32, value: &str) -> Predicate {
    Predicate::Equal {
        id: PredicateId::new(id),
        field_id: FieldId::new(field_id),
        value: ScalarValue::String(value.into()),
    }
}

#[tokio::test]
async fn conjunction_uses_the_rarest_exact_posting_as_its_lead_cursor() {
    let schema = schema();
    let mut sink = ExactMemorySink::new();
    let segment = build_segment(
        &mut sink,
        &schema,
        36,
        1,
        vec![
            source("common/10", "common", 10),
            source("common/9", "common", 9),
            source("common/8", "common", 8),
            source("common/7", "common", 7),
            source("common/6", "common", 6),
            source("rare", "rare", 0),
        ],
    )
    .await;
    let directory = MemoryArtifacts::from_sink(&sink);
    let gate = TestGate {
        revision: 1,
        denied: BTreeSet::new(),
        batches: Mutex::new(Vec::new()),
    };
    let page = NativeQueryExecutor::new(&directory, &gate, NativeQueryLimits::default())
        .unwrap()
        .execute(&NativeQueryRequest {
            schema: schema.clone(),
            segments: vec![segment],
            query: NativeQuery::Filter {
                predicate: Some(Predicate::And(vec![
                    equal(1, 2, "advisory"),
                    equal(2, 0, "rare"),
                ])),
                order: schema.physical_order,
            },
            after: None,
            limit: 10,
            authorization_revision: 1,
        })
        .await
        .unwrap();

    assert_eq!(
        page.hits
            .iter()
            .map(|hit| hit.result.path.as_str())
            .collect::<Vec<_>>(),
        ["rare"]
    );
    assert_eq!(page.statistics.planner_conjunctions, 1);
    assert_eq!(page.statistics.planner_reordered_conjunctions, 1);
    assert_eq!(page.statistics.planner_costed_children, 2);
    assert_eq!(page.statistics.planner_child_cost_total, 7);
    assert_eq!(page.statistics.planner_lead_cost_min, 1);
    assert_eq!(page.statistics.planner_lead_cost_max, 1);
    assert!(page.statistics.conjunction_advances > 0);
    assert!(page.statistics.posting_blocks_sought > 0);
    assert!(page.statistics.posting_bytes_read > 0);
    assert_eq!(page.statistics.two_phase_verifications, 1);
    assert!(page.statistics.live_mask_blocks_decoded > 0);
    assert_eq!(page.statistics.candidate_gate_batches, 1);
}

#[tokio::test]
async fn boolean_union_heap_deduplicates_and_obeys_the_expansion_limit() {
    let page = execute_many(
        37,
        schema(),
        vec![
            source("active/3", "active", 3),
            source("inactive/2", "inactive", 2),
            source("active/1", "active", 1),
        ],
        NativeQuery::Filter {
            predicate: Some(Predicate::Or(vec![
                equal(1, 0, "active"),
                equal(2, 2, "advisory"),
            ])),
            order: Vec::new(),
        },
        3,
    )
    .await;
    assert!(page.statistics.union_heap_pushes > 0);
    assert!(page.statistics.union_heap_pops > 0);
    assert_eq!(page.statistics.candidate_gate_batches, 1);

    let schema = schema();
    let mut sink = ExactMemorySink::new();
    let segment = build_segment(&mut sink, &schema, 38, 1, vec![source("one", "active", 1)]).await;
    let directory = MemoryArtifacts::from_sink(&sink);
    let gate = TestGate {
        revision: 1,
        denied: BTreeSet::new(),
        batches: Mutex::new(Vec::new()),
    };
    let executor = NativeQueryExecutor::new(
        &directory,
        &gate,
        NativeQueryLimits {
            maximum_expanded_terms: 2,
            ..NativeQueryLimits::default()
        },
    )
    .unwrap();
    let error = executor
        .execute(&NativeQueryRequest {
            schema,
            segments: vec![segment],
            query: NativeQuery::Filter {
                predicate: Some(Predicate::Or(vec![
                    equal(1, 0, "active"),
                    equal(2, 0, "inactive"),
                    equal(3, 0, "unknown"),
                ])),
                order: Vec::new(),
            },
            after: None,
            limit: 1,
            authorization_revision: 1,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        NativeQueryExecutionError::Index(IndexError::ResourceLimit {
            needed: 3,
            limit: 2
        })
    ));
}

#[tokio::test]
async fn exists_uses_the_exact_presence_posting_for_null_and_empty_arrays() {
    let mut present_field = field(
        0,
        "optional",
        FieldComponents::TERMS
            .union(FieldComponents::FAST_COLUMN)
            .union(FieldComponents::STORED),
    );
    present_field.cardinality = Cardinality::Multi;
    present_field.allow_missing = true;
    present_field.allow_null = true;
    let mut schema = Schema {
        kind: IndexKind::TypedJson,
        path_prefix: String::new(),
        content_type_scope: Some("application/json".into()),
        fields: vec![present_field],
        semantics: IndexSemantics::TypedJson,
        physical_order: Vec::new(),
        component_versions: Vec::new(),
    };
    schema.component_versions = versions(&schema.fields);

    let missing = one_record_source(
        "records/missing",
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let explicit_null = one_record_source(
        "records/null",
        None,
        vec![presence_term(0)],
        vec![ProjectedColumn {
            field_id: FieldId::new(0),
            multi_valued: true,
            cell: FastColumnCell {
                present: true,
                null: true,
                values: Vec::new(),
            },
        }],
        Vec::new(),
        Vec::new(),
    );
    let empty_array = one_record_source(
        "records/empty",
        None,
        vec![presence_term(0)],
        vec![ProjectedColumn {
            field_id: FieldId::new(0),
            multi_valued: true,
            cell: FastColumnCell {
                present: true,
                null: false,
                values: Vec::new(),
            },
        }],
        Vec::new(),
        Vec::new(),
    );
    let mut sink = ExactMemorySink::new();
    let segment = build_segment(
        &mut sink,
        &schema,
        41,
        1,
        vec![missing, explicit_null, empty_array],
    )
    .await;
    let directory = MemoryArtifacts::from_sink(&sink);
    let predicate = Predicate::Exists {
        id: PredicateId::new(1),
        field_id: FieldId::new(0),
    };
    let query = NativeQuery::Filter {
        predicate: Some(predicate),
        order: Vec::new(),
    };
    let planner_statistics = NativeQueryStatisticsRecorder::new();
    let plan = crate::v4::executor::plan::plan_segment(
        &directory,
        &segment,
        &schema,
        &query,
        64,
        &planner_statistics,
    )
    .await
    .unwrap();
    assert!(matches!(
        plan.cursor,
        crate::v4::executor::posting::DocCursor::Posting(_)
    ));

    let gate = TestGate {
        revision: 1,
        denied: BTreeSet::new(),
        batches: Mutex::new(Vec::new()),
    };
    let page = NativeQueryExecutor::new(&directory, &gate, NativeQueryLimits::default())
        .unwrap()
        .execute(&NativeQueryRequest {
            schema,
            segments: vec![segment],
            query,
            after: None,
            limit: 3,
            authorization_revision: 1,
        })
        .await
        .unwrap();
    assert_eq!(
        page.hits
            .iter()
            .map(|hit| hit.result.path.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["records/empty", "records/null"])
    );
    assert_eq!(page.statistics.candidate_doc_ids, 2);
    assert!(page.statistics.term_seeks > 0);
    assert!(page.statistics.posting_blocks_decoded > 0);
}
