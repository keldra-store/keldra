use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::IndexError;
use crate::IndexFileRead;
use crate::v4::build::{
    BuildLimits, ExactMemorySink, NativeSegmentWriter, ProjectedRecord, ProjectedSource,
    ProjectedTerm, ProjectedVector, PublishedObject, SourcePush,
};
use crate::v4::{
    Analyzer, ArtifactDescriptor, ArtifactDirectoryRead, CandidateGate, CandidateGateEvidence,
    CandidateReference, Cardinality, Collation, ComponentKind, ComponentVersion, FieldCapabilities,
    FieldComponents, FieldId, FieldSchema, FieldType, IndexKind, IndexSemantics, NativeQuery,
    NativeQueryExecutor, NativeQueryLimits, NativeQueryRequest, ObjectIdentity, Schema,
    SegmentDescriptor, SegmentIdentity, VectorMetric, VectorNormalization, scalar_term, text_term,
};

#[derive(Clone)]
struct RankedMemoryFile(Arc<[u8]>);

impl IndexFileRead for RankedMemoryFile {
    type Slice = Arc<[u8]>;

    async fn read_at(&self, offset: u64, maximum: usize) -> Result<Self::Slice, IndexError> {
        let start = usize::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?;
        if start >= self.0.len() {
            return Ok(Arc::from([]));
        }
        Ok(Arc::from(
            &self.0[start..start.saturating_add(maximum).min(self.0.len())],
        ))
    }
}

struct RankedMemoryArtifacts(BTreeMap<String, PublishedObject>);

impl ArtifactDirectoryRead for RankedMemoryArtifacts {
    type File = RankedMemoryFile;

    async fn open_artifact(
        &self,
        descriptor: &ArtifactDescriptor,
    ) -> Result<Self::File, IndexError> {
        let object = self
            .0
            .get(&descriptor.path)
            .ok_or_else(|| IndexError::FileNotFound(descriptor.path.clone()))?;
        if object.object_version != descriptor.object_version {
            return Err(IndexError::Integrity);
        }
        let start = usize::try_from(descriptor.offset).map_err(|_| IndexError::OffsetOverflow)?;
        let length =
            usize::try_from(descriptor.encoded_length).map_err(|_| IndexError::OffsetOverflow)?;
        let end = start
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(RankedMemoryFile(Arc::from(
            object.bytes.get(start..end).ok_or(IndexError::Integrity)?,
        )))
    }
}

struct RankedAllowAll;

impl CandidateGate for RankedAllowAll {
    type Error = IndexError;

    async fn evaluate(
        &self,
        candidates: &[CandidateReference],
    ) -> Result<CandidateGateEvidence, Self::Error> {
        Ok(CandidateGateEvidence {
            visible: vec![true; candidates.len()],
            authorization_revision: 7,
            denied: 0,
            stale: 0,
        })
    }
}

fn ranked_field(
    id: u32,
    name: &str,
    field_type: FieldType,
    capabilities: FieldCapabilities,
    analyzer: Option<Analyzer>,
) -> FieldSchema {
    let mut field = FieldSchema {
        id: FieldId::new(id),
        name: name.into(),
        source_selector: format!("/{name}"),
        field_type,
        cardinality: if field_type == FieldType::Vector {
            Cardinality::Multi
        } else {
            Cardinality::Single
        },
        allow_missing: true,
        allow_null: false,
        collation: Collation::BinaryUtf8,
        capabilities,
        analyzer,
        components: FieldComponents::TERMS,
    };
    field.components = field.compiled_components().unwrap();
    field
}

fn ranked_version(component_kind: ComponentKind) -> ComponentVersion {
    ComponentVersion {
        component_kind,
        codec_version: u16::from(component_kind == ComponentKind::IDENTITY_TABLE) + 1,
    }
}

fn ranked_versions(fields: &[FieldSchema]) -> Vec<ComponentVersion> {
    let mut components = BTreeSet::from([
        ComponentKind::ROUTING_NODE,
        ComponentKind::IDENTITY_TABLE,
        ComponentKind::LIVE_MASK,
        ComponentKind::PATH_LOCATOR,
        ComponentKind::SCORING_STATISTICS,
    ]);
    for field in fields {
        if field.components.contains(FieldComponents::TERMS) {
            components.insert(ComponentKind::TERM_DICTIONARY);
            components.insert(ComponentKind::POSTINGS);
        }
        if field.components.contains(FieldComponents::POINTS) {
            components.insert(ComponentKind::POINTS);
        }
        if field.components.contains(FieldComponents::DOC_VALUES) {
            components.insert(ComponentKind::DOC_VALUES);
        }
        if field.components.contains(FieldComponents::POSITIONS) {
            components.insert(ComponentKind::POSITIONS);
        }
        if field.components.contains(FieldComponents::NORMS) {
            components.insert(ComponentKind::NORMS);
        }
        if field.components.contains(FieldComponents::VECTOR) {
            components.insert(ComponentKind::VECTORS);
        }
    }
    components.into_iter().map(ranked_version).collect()
}

fn complete_schema(mut schema: Schema) -> Schema {
    schema.component_versions = ranked_versions(&schema.fields);
    schema.validate().unwrap();
    schema
}

async fn ranked_segment(
    sink: &mut ExactMemorySink,
    schema: &Schema,
    index_id: u64,
    segment_id: u64,
    sources: Vec<ProjectedSource>,
) -> SegmentDescriptor {
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

fn record_source(
    source_path: &str,
    result_identity: Option<ObjectIdentity>,
    terms: Vec<ProjectedTerm>,
    vectors: Vec<ProjectedVector>,
    field_lengths: Vec<(FieldId, u32)>,
) -> ProjectedSource {
    ProjectedSource {
        source_identity: ObjectIdentity {
            path: source_path.into(),
            version: 1,
        },
        records: vec![ProjectedRecord {
            result_identity,
            order_key: Vec::new(),
            terms,
            points: Vec::new(),
            doc_values: Vec::new(),
            vectors,
            field_lengths,
        }],
    }
}

fn text_source(path: &str, token: &str, frequency: u32, length: u32) -> ProjectedSource {
    let (term_type, term) = text_term(token).unwrap();
    record_source(
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
        vec![(FieldId::new(0), length)],
    )
}

fn ranked_request(
    schema: Schema,
    segments: Vec<SegmentDescriptor>,
    query: NativeQuery,
    limit: u32,
) -> NativeQueryRequest {
    NativeQueryRequest {
        schema,
        segments,
        query,
        after: None,
        limit,
        facets: Vec::new(),
        aggregates: Vec::new(),
        authorization_revision: 7,
    }
}

fn full_text_schema() -> Schema {
    complete_schema(Schema {
        kind: IndexKind::FullText,
        path_prefix: String::new(),
        content_type_scope: None,
        fields: vec![ranked_field(
            0,
            "body",
            FieldType::Text,
            FieldCapabilities::FULL_TEXT,
            Some(Analyzer::UnicodeAlphanumericLowercase),
        )],
        semantics: IndexSemantics::FullText {
            analyzer: Analyzer::UnicodeAlphanumericLowercase,
            bm25_k1: 1.2,
            bm25_b: 0.75,
        },
        physical_order: Vec::new(),
        component_versions: Vec::new(),
    })
}

#[tokio::test]
async fn bm25_ranking_uses_statistics_from_nonmatching_segments() {
    let schema = full_text_schema();
    let mut sink = ExactMemorySink::new();
    let segments = vec![
        ranked_segment(
            &mut sink,
            &schema,
            31,
            1,
            vec![text_source("candidate/high-tf", "hello", 3, 100)],
        )
        .await,
        ranked_segment(
            &mut sink,
            &schema,
            31,
            2,
            vec![text_source("candidate/short", "hello", 1, 1)],
        )
        .await,
        ranked_segment(
            &mut sink,
            &schema,
            31,
            3,
            vec![text_source("non-match/long", "other", 1_000, 1_000)],
        )
        .await,
    ];
    let directory = RankedMemoryArtifacts(sink.objects().clone());
    let page = NativeQueryExecutor::new(&directory, &RankedAllowAll, NativeQueryLimits::default())
        .unwrap()
        .execute(&ranked_request(
            schema,
            segments,
            NativeQuery::FullText {
                text: "hello".into(),
                phrase: false,
            },
            2,
        ))
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
    let schema = full_text_schema();
    let mut sink = ExactMemorySink::new();
    let winner = ranked_segment(
        &mut sink,
        &schema,
        39,
        1,
        vec![text_source("winner", "hello", 100, 1)],
    )
    .await;
    let ordinary = ranked_segment(
        &mut sink,
        &schema,
        39,
        2,
        (0..200)
            .map(|ordinal| text_source(&format!("ordinary/{ordinal:03}"), "hello", 1, 1))
            .collect(),
    )
    .await;
    let directory = RankedMemoryArtifacts(sink.objects().clone());
    let executor =
        NativeQueryExecutor::new(&directory, &RankedAllowAll, NativeQueryLimits::default())
            .unwrap();
    let query = NativeQuery::FullText {
        text: "hello".into(),
        phrase: false,
    };
    let optimized = executor
        .execute(&ranked_request(
            schema.clone(),
            vec![winner.clone(), ordinary.clone()],
            query.clone(),
            1,
        ))
        .await
        .unwrap();
    let exhaustive = executor
        .execute(&ranked_request(schema, vec![winner, ordinary], query, 201))
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
    let schema = full_text_schema();
    let mut sink = ExactMemorySink::new();
    let segments = vec![
        ranked_segment(
            &mut sink,
            &schema,
            40,
            1,
            vec![text_source("z", "hello", 1, 1)],
        )
        .await,
        ranked_segment(
            &mut sink,
            &schema,
            40,
            2,
            vec![text_source("a", "hello", 1, 1)],
        )
        .await,
    ];
    let directory = RankedMemoryArtifacts(sink.objects().clone());
    let page = NativeQueryExecutor::new(&directory, &RankedAllowAll, NativeQueryLimits::default())
        .unwrap()
        .execute(&ranked_request(
            schema,
            segments,
            NativeQuery::FullText {
                text: "hello".into(),
                phrase: false,
            },
            1,
        ))
        .await
        .unwrap();

    assert_eq!(page.hits[0].result.path, "a");
    assert!(page.statistics.posting_advance_calls > 0);
    assert_eq!(page.statistics.cursor_seeks, 0);
}

async fn assert_l2_euclidean_query_is_normalized(
    index_id: u64,
    schema: Schema,
    source: ProjectedSource,
    query: NativeQuery,
) {
    let mut sink = ExactMemorySink::new();
    let segment = ranked_segment(&mut sink, &schema, index_id, 1, vec![source]).await;
    let directory = RankedMemoryArtifacts(sink.objects().clone());
    let page = NativeQueryExecutor::new(&directory, &RankedAllowAll, NativeQueryLimits::default())
        .unwrap()
        .execute(&ranked_request(schema, vec![segment], query, 1))
        .await
        .unwrap();
    assert_eq!(page.hits.len(), 1);
    assert!(page.hits[0].score.unwrap().abs() <= f32::EPSILON);
}

#[tokio::test]
async fn l2_normalization_is_symmetric_for_vector_and_hybrid_queries() {
    let vector_schema = complete_schema(Schema {
        kind: IndexKind::Vector,
        path_prefix: String::new(),
        content_type_scope: None,
        fields: vec![ranked_field(
            0,
            "embedding",
            FieldType::Vector,
            FieldCapabilities::empty(),
            None,
        )],
        semantics: IndexSemantics::Vector {
            dimensions: 2,
            metric: VectorMetric::Euclidean,
            normalization: VectorNormalization::L2,
        },
        physical_order: Vec::new(),
        component_versions: Vec::new(),
    });
    assert_l2_euclidean_query_is_normalized(
        32,
        vector_schema,
        record_source(
            "vectors/unit",
            None,
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

    let hybrid_schema = complete_schema(Schema {
        kind: IndexKind::Hybrid,
        path_prefix: String::new(),
        content_type_scope: None,
        fields: vec![
            ranked_field(
                0,
                "body",
                FieldType::Text,
                FieldCapabilities::FULL_TEXT,
                Some(Analyzer::UnicodeAlphanumericLowercase),
            ),
            ranked_field(
                1,
                "embedding",
                FieldType::Vector,
                FieldCapabilities::empty(),
                None,
            ),
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
    });
    assert_l2_euclidean_query_is_normalized(
        33,
        hybrid_schema,
        record_source(
            "hybrid/unit",
            None,
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

fn scalar_projected_term(field_id: u32, value: &str) -> ProjectedTerm {
    let (term_type, term) = scalar_term(&crate::v4::ScalarValue::String(value.into())).unwrap();
    ProjectedTerm {
        field_id: FieldId::new(field_id),
        term_type,
        term,
        frequency: 1,
        positions: Vec::new(),
    }
}

fn repeated_record(terms: Vec<ProjectedTerm>) -> ProjectedRecord {
    ProjectedRecord {
        result_identity: Some(ObjectIdentity {
            path: "shared/result".into(),
            version: 7,
        }),
        order_key: Vec::new(),
        terms,
        points: Vec::new(),
        doc_values: Vec::new(),
        vectors: Vec::new(),
        field_lengths: Vec::new(),
    }
}

async fn assert_multi_record_pagination(
    index_id: u64,
    schema: Schema,
    source: ProjectedSource,
    query: NativeQuery,
) {
    let mut sink = ExactMemorySink::new();
    let segment = ranked_segment(&mut sink, &schema, index_id, 1, vec![source]).await;
    let directory = RankedMemoryArtifacts(sink.objects().clone());
    let executor =
        NativeQueryExecutor::new(&directory, &RankedAllowAll, NativeQueryLimits::default())
            .unwrap();
    let mut request = ranked_request(schema, vec![segment], query, 1);
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
    let git_schema = complete_schema(Schema {
        kind: IndexKind::GitSource,
        path_prefix: String::new(),
        content_type_scope: None,
        fields: vec![
            ranked_field(
                0,
                "repository",
                FieldType::Keyword,
                FieldCapabilities::EXACT,
                None,
            ),
            ranked_field(
                1,
                "commit",
                FieldType::Keyword,
                FieldCapabilities::EXACT,
                None,
            ),
            ranked_field(
                2,
                "tree_path",
                FieldType::Keyword,
                FieldCapabilities::EXACT.union(FieldCapabilities::PREFIX),
                None,
            ),
        ],
        semantics: IndexSemantics::GitSource {
            repository_scope: "repo".into(),
        },
        physical_order: Vec::new(),
        component_versions: Vec::new(),
    });
    let git_terms = || {
        vec![
            scalar_projected_term(0, "repo"),
            scalar_projected_term(1, "commit"),
            scalar_projected_term(2, "src/lib.rs"),
        ]
    };
    assert_multi_record_pagination(
        34,
        git_schema,
        ProjectedSource {
            source_identity: ObjectIdentity {
                path: "git/manifest".into(),
                version: 3,
            },
            records: vec![repeated_record(git_terms()), repeated_record(git_terms())],
        },
        NativeQuery::GitSource {
            repository_id: "repo".into(),
            commit_id: "commit".into(),
            tree_path: "src/lib.rs".into(),
            prefix: false,
        },
    )
    .await;

    let tensor_schema = complete_schema(Schema {
        kind: IndexKind::Tensor,
        path_prefix: String::new(),
        content_type_scope: None,
        fields: vec![
            ranked_field(
                0,
                "model",
                FieldType::Keyword,
                FieldCapabilities::EXACT,
                None,
            ),
            ranked_field(
                1,
                "tensor",
                FieldType::Keyword,
                FieldCapabilities::EXACT,
                None,
            ),
        ],
        semantics: IndexSemantics::Tensor {
            model_scope: "model".into(),
        },
        physical_order: Vec::new(),
        component_versions: Vec::new(),
    });
    let tensor_terms = || {
        vec![
            scalar_projected_term(0, "model"),
            scalar_projected_term(1, "weights"),
        ]
    };
    assert_multi_record_pagination(
        35,
        tensor_schema,
        ProjectedSource {
            source_identity: ObjectIdentity {
                path: "tensor/manifest".into(),
                version: 5,
            },
            records: vec![
                repeated_record(tensor_terms()),
                repeated_record(tensor_terms()),
            ],
        },
        NativeQuery::Tensor {
            model_id: "model".into(),
            tensor_name: "weights".into(),
        },
    )
    .await;
}
