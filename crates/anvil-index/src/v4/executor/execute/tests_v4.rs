use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use super::*;
use crate::IndexFileRead;
use crate::v4::build::{
    BuildLimits, ExactMemorySink, NativeSegmentWriter, ProjectedDocValue, ProjectedPoint,
    ProjectedRecord, ProjectedSource, PublishedObject, SourcePush,
};
use crate::v4::{
    AggregateOperation, AggregateRequest, ArtifactPackReference, Cardinality, Collation,
    ComponentKind, ComponentVersion, DocValueCell, FIELD_PRESENCE_TERM, FacetRequest,
    FieldCapabilities, FieldComponents, FieldSchema, FieldType, IndexKind, IndexSemantics,
    NativeQuery, NativeQueryRequest, ObjectIdentity, OrderDirection, OrderField, Predicate,
    PredicateId, RangeBound, ScalarValue, Schema, SegmentDescriptor, SegmentIdentity, SortValue,
    TERM_TYPE_FIELD_PRESENCE, encode_physical_order_key, scalar_term,
};

#[derive(Clone)]
struct MemoryFile(Arc<[u8]>);

impl IndexFileRead for MemoryFile {
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

struct MemoryArtifacts(BTreeMap<String, PublishedObject>);

impl ArtifactDirectoryRead for MemoryArtifacts {
    type File = MemoryFile;

    async fn open_artifact(&self, pack: &ArtifactPackReference) -> Result<Self::File, IndexError> {
        let object = self
            .0
            .get(&pack.path)
            .ok_or_else(|| IndexError::FileNotFound(pack.path.clone()))?;
        if object.object_version != pack.object_version
            || *blake3::hash(&object.bytes).as_bytes() != pack.object_content_hash
            || object.bytes.len() as u64 != pack.object_length
        {
            return Err(IndexError::Integrity);
        }
        Ok(MemoryFile(Arc::from(object.bytes.as_slice())))
    }
}

struct ParallelMemoryArtifacts {
    inner: MemoryArtifacts,
    parallelism: usize,
}

impl ArtifactDirectoryRead for ParallelMemoryArtifacts {
    type File = MemoryFile;

    fn query_parallelism(&self) -> usize {
        self.parallelism
    }

    async fn open_artifact(&self, pack: &ArtifactPackReference) -> Result<Self::File, IndexError> {
        self.inner.open_artifact(pack).await
    }
}

struct AllowAll;

impl CandidateGate for AllowAll {
    type Error = IndexError;

    async fn evaluate(
        &self,
        candidates: &[CandidateReference],
    ) -> Result<super::super::super::CandidateGateEvidence, Self::Error> {
        Ok(super::super::super::CandidateGateEvidence {
            visible: vec![true; candidates.len()],
            authorization_revision: 7,
            denied: 0,
            stale: 0,
        })
    }
}

#[derive(Default)]
struct ConcurrentGate {
    active: AtomicUsize,
    maximum: AtomicUsize,
}

impl CandidateGate for ConcurrentGate {
    type Error = IndexError;

    async fn evaluate(
        &self,
        candidates: &[CandidateReference],
    ) -> Result<super::super::super::CandidateGateEvidence, Self::Error> {
        let active = self.active.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        self.maximum.fetch_max(active, AtomicOrdering::SeqCst);
        tokio::task::yield_now().await;
        self.active.fetch_sub(1, AtomicOrdering::SeqCst);
        Ok(super::super::super::CandidateGateEvidence {
            visible: vec![true; candidates.len()],
            authorization_revision: 7,
            denied: 0,
            stale: 0,
        })
    }
}

struct SelectiveGate {
    denied: BTreeSet<String>,
    stale: BTreeSet<String>,
    revision: u64,
}

impl CandidateGate for SelectiveGate {
    type Error = IndexError;

    async fn evaluate(
        &self,
        candidates: &[CandidateReference],
    ) -> Result<super::super::super::CandidateGateEvidence, Self::Error> {
        let visible = candidates
            .iter()
            .map(|candidate| {
                !self.denied.contains(&candidate.result.path)
                    && !self.stale.contains(&candidate.result.path)
            })
            .collect::<Vec<_>>();
        Ok(super::super::super::CandidateGateEvidence {
            visible,
            authorization_revision: self.revision,
            denied: candidates
                .iter()
                .filter(|candidate| self.denied.contains(&candidate.result.path))
                .count() as u64,
            stale: candidates
                .iter()
                .filter(|candidate| self.stale.contains(&candidate.result.path))
                .count() as u64,
        })
    }
}

fn version(component_kind: ComponentKind) -> ComponentVersion {
    ComponentVersion {
        component_kind,
        codec_version: u16::from(component_kind == ComponentKind::IDENTITY_TABLE) + 1,
    }
}

fn schema() -> Schema {
    let mut state = FieldSchema {
        id: FieldId::new(0),
        name: "state".into(),
        source_selector: "/state".into(),
        field_type: FieldType::Keyword,
        cardinality: Cardinality::Single,
        allow_missing: false,
        allow_null: false,
        collation: Collation::BinaryUtf8,
        capabilities: FieldCapabilities::EXACT
            .union(FieldCapabilities::PREFIX)
            .union(FieldCapabilities::RANGE)
            .union(FieldCapabilities::ORDER)
            .union(FieldCapabilities::FACET),
        analyzer: None,
        components: FieldComponents::TERMS.union(FieldComponents::DOC_VALUES),
    };
    state.components = state.compiled_components().unwrap();
    let mut priority = FieldSchema {
        id: FieldId::new(1),
        name: "priority".into(),
        source_selector: "/priority".into(),
        field_type: FieldType::SignedInteger,
        cardinality: Cardinality::Single,
        allow_missing: false,
        allow_null: false,
        collation: Collation::BinaryUtf8,
        capabilities: FieldCapabilities::EXACT
            .union(FieldCapabilities::RANGE)
            .union(FieldCapabilities::ORDER)
            .union(FieldCapabilities::FACET)
            .union(FieldCapabilities::AGGREGATE),
        analyzer: None,
        components: FieldComponents::POINTS.union(FieldComponents::DOC_VALUES),
    };
    priority.components = priority.compiled_components().unwrap();
    let mut components = BTreeSet::from([
        ComponentKind::ROUTING_NODE,
        ComponentKind::IDENTITY_TABLE,
        ComponentKind::LIVE_MASK,
        ComponentKind::PATH_LOCATOR,
        ComponentKind::SCORING_STATISTICS,
    ]);
    for field in [&state, &priority] {
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
    }
    Schema {
        kind: IndexKind::TypedJson,
        path_prefix: String::new(),
        content_type_scope: Some("application/json".into()),
        fields: vec![state, priority],
        semantics: IndexSemantics::TypedJson,
        physical_order: vec![
            OrderField {
                field_id: FieldId::new(1),
                direction: OrderDirection::Descending,
            },
            OrderField {
                field_id: FieldId::new(0),
                direction: OrderDirection::Ascending,
            },
        ],
        component_versions: components.into_iter().map(version).collect(),
    }
}

fn source(path: &str, state: &str, priority: i64) -> ProjectedSource {
    let state_value = ScalarValue::String(state.into());
    let priority_value = ScalarValue::Signed(priority);
    let (state_type, state_term) = scalar_term(&state_value).unwrap();
    ProjectedSource {
        source_identity: ObjectIdentity {
            path: path.into(),
            version: 1,
        },
        records: vec![ProjectedRecord {
            result_identity: None,
            order_key: encode_physical_order_key(&[
                (
                    SortValue::Value(priority_value.clone()),
                    OrderDirection::Descending,
                ),
                (
                    SortValue::Value(state_value.clone()),
                    OrderDirection::Ascending,
                ),
            ])
            .unwrap(),
            terms: vec![
                crate::v4::build::ProjectedTerm {
                    field_id: FieldId::new(0),
                    term_type: state_type,
                    term: state_term,
                    frequency: 1,
                    positions: Vec::new(),
                },
                crate::v4::build::ProjectedTerm {
                    field_id: FieldId::new(0),
                    term_type: TERM_TYPE_FIELD_PRESENCE,
                    term: FIELD_PRESENCE_TERM.to_vec(),
                    frequency: 1,
                    positions: Vec::new(),
                },
            ],
            points: vec![ProjectedPoint {
                field_id: FieldId::new(1),
                present: true,
                null: false,
                values: vec![priority_value.clone()],
            }],
            doc_values: vec![
                ProjectedDocValue {
                    field_id: FieldId::new(0),
                    multi_valued: false,
                    cell: DocValueCell::value(state_value),
                },
                ProjectedDocValue {
                    field_id: FieldId::new(1),
                    multi_valued: false,
                    cell: DocValueCell::value(priority_value),
                },
            ],
            vectors: Vec::new(),
            field_lengths: Vec::new(),
        }],
    }
}

fn keyword_schema(kind: IndexKind, semantics: IndexSemantics) -> Schema {
    let mut field = FieldSchema {
        id: FieldId::new(0),
        name: "value".into(),
        source_selector: "/value".into(),
        field_type: FieldType::Keyword,
        cardinality: Cardinality::Single,
        allow_missing: false,
        allow_null: false,
        collation: Collation::BinaryUtf8,
        capabilities: FieldCapabilities::EXACT.union(FieldCapabilities::PREFIX),
        analyzer: None,
        components: FieldComponents::TERMS,
    };
    field.components = field.compiled_components().unwrap();
    Schema {
        kind,
        path_prefix: String::new(),
        content_type_scope: None,
        fields: vec![field],
        semantics,
        physical_order: Vec::new(),
        component_versions: [
            ComponentKind::ROUTING_NODE,
            ComponentKind::IDENTITY_TABLE,
            ComponentKind::LIVE_MASK,
            ComponentKind::PATH_LOCATOR,
            ComponentKind::TERM_DICTIONARY,
            ComponentKind::POSTINGS,
            ComponentKind::SCORING_STATISTICS,
        ]
        .into_iter()
        .map(version)
        .collect(),
    }
}

fn keyword_source(path: &str, value: &str) -> ProjectedSource {
    let (term_type, term) = scalar_term(&ScalarValue::String(value.into())).unwrap();
    ProjectedSource {
        source_identity: ObjectIdentity {
            path: path.into(),
            version: 1,
        },
        records: vec![ProjectedRecord {
            result_identity: None,
            order_key: Vec::new(),
            terms: vec![
                crate::v4::build::ProjectedTerm {
                    field_id: FieldId::new(0),
                    term_type,
                    term,
                    frequency: 1,
                    positions: Vec::new(),
                },
                crate::v4::build::ProjectedTerm {
                    field_id: FieldId::new(0),
                    term_type: TERM_TYPE_FIELD_PRESENCE,
                    term: FIELD_PRESENCE_TERM.to_vec(),
                    frequency: 1,
                    positions: Vec::new(),
                },
            ],
            points: Vec::new(),
            doc_values: Vec::new(),
            vectors: Vec::new(),
            field_lengths: Vec::new(),
        }],
    }
}

async fn fixture() -> (Schema, SegmentDescriptor, MemoryArtifacts) {
    let schema = schema();
    let identity = SegmentIdentity::new(9, 1, schema.fingerprint().unwrap(), 1).unwrap();
    let mut writer = NativeSegmentWriter::new(
        identity,
        schema.clone(),
        BuildLimits::new(64 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    for source in [
        source("a", "active", 1),
        source("b", "inactive", 5),
        source("c", "active", 3),
    ] {
        assert_eq!(writer.push_source(source).unwrap(), SourcePush::Accepted);
    }
    let mut sink = ExactMemorySink::new();
    let built = writer.seal(&mut sink).await.unwrap();
    (
        schema,
        built.descriptor,
        MemoryArtifacts(sink.objects().clone()),
    )
}

async fn build_fixture(
    schema: Schema,
    index_id: u64,
    sources: Vec<ProjectedSource>,
) -> (Schema, SegmentDescriptor, MemoryArtifacts) {
    build_segment_fixture(schema, index_id, 1, sources).await
}

async fn build_segment_fixture(
    schema: Schema,
    index_id: u64,
    segment_id: u64,
    sources: Vec<ProjectedSource>,
) -> (Schema, SegmentDescriptor, MemoryArtifacts) {
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
    let mut sink = ExactMemorySink::new();
    let segment = writer.seal(&mut sink).await.unwrap().descriptor;
    (schema, segment, MemoryArtifacts(sink.objects().clone()))
}

fn request(schema: &Schema, segment: &SegmentDescriptor, query: NativeQuery) -> NativeQueryRequest {
    NativeQueryRequest {
        schema: schema.clone(),
        segments: vec![segment.clone()],
        query,
        after: None,
        limit: 100,
        facets: Vec::new(),
        aggregates: Vec::new(),
        authorization_revision: 7,
    }
}

fn paths(page: &NativeQueryPage) -> BTreeSet<&str> {
    page.hits
        .iter()
        .map(|hit| hit.result.path.as_str())
        .collect()
}

#[tokio::test]
async fn exact_prefix_range_and_exists_use_declared_native_components() {
    let (schema, segment, directory) = fixture().await;
    let executor =
        NativeQueryExecutor::new(&directory, &AllowAll, NativeQueryLimits::default()).unwrap();

    let exact = executor
        .execute(&request(
            &schema,
            &segment,
            NativeQuery::Filter {
                predicate: Some(Predicate::Equal {
                    id: PredicateId::new(1),
                    field_id: FieldId::new(0),
                    value: ScalarValue::String("active".into()),
                }),
                order: Vec::new(),
            },
        ))
        .await
        .unwrap();
    assert_eq!(paths(&exact), BTreeSet::from(["a", "c"]));

    let prefix = executor
        .execute(&request(
            &schema,
            &segment,
            NativeQuery::Filter {
                predicate: Some(Predicate::Prefix {
                    id: PredicateId::new(2),
                    field_id: FieldId::new(0),
                    prefix: "act".into(),
                }),
                order: Vec::new(),
            },
        ))
        .await
        .unwrap();
    assert_eq!(paths(&prefix), BTreeSet::from(["a", "c"]));

    let range = executor
        .execute(&request(
            &schema,
            &segment,
            NativeQuery::Filter {
                predicate: Some(Predicate::Range {
                    id: PredicateId::new(3),
                    field_id: FieldId::new(1),
                    lower: Some(RangeBound {
                        value: ScalarValue::Signed(2),
                        inclusive: true,
                    }),
                    upper: Some(RangeBound {
                        value: ScalarValue::Signed(5),
                        inclusive: true,
                    }),
                }),
                order: Vec::new(),
            },
        ))
        .await
        .unwrap();
    assert_eq!(paths(&range), BTreeSet::from(["b", "c"]));

    let exists = executor
        .execute(&request(
            &schema,
            &segment,
            NativeQuery::Filter {
                predicate: Some(Predicate::Exists {
                    id: PredicateId::new(4),
                    field_id: FieldId::new(1),
                }),
                order: Vec::new(),
            },
        ))
        .await
        .unwrap();
    assert_eq!(paths(&exists), BTreeSet::from(["a", "b", "c"]));
}

#[tokio::test]
async fn point_range_traverses_each_routed_leaf_once_for_many_matches() {
    let schema = schema();
    let sources = (0..1_024)
        .map(|index| source(&format!("objects/{index:04}"), "active", 1))
        .collect();
    let (schema, segment, directory) = build_fixture(schema, 91, sources).await;
    let executor =
        NativeQueryExecutor::new(&directory, &AllowAll, NativeQueryLimits::default()).unwrap();
    let mut query = request(
        &schema,
        &segment,
        NativeQuery::Filter {
            predicate: Some(Predicate::Range {
                id: PredicateId::new(1),
                field_id: FieldId::new(1),
                lower: Some(RangeBound {
                    value: ScalarValue::Signed(1),
                    inclusive: true,
                }),
                upper: Some(RangeBound {
                    value: ScalarValue::Signed(1),
                    inclusive: true,
                }),
            }),
            order: Vec::new(),
        },
    );
    query.limit = 100;
    let statistics = NativeQueryStatisticsRecorder::new();

    let page = executor
        .execute_observed(&query, statistics.clone())
        .await
        .unwrap();

    assert_eq!(page.hits.len(), 100);
    let snapshot = statistics.snapshot();
    assert_eq!(snapshot.candidate_doc_ids, 1_024);
    assert!(snapshot.point_blocks_decoded <= 3, "{snapshot:?}");
}

#[tokio::test]
async fn term_prefix_traverses_the_dictionary_once_for_many_matches() {
    let schema = keyword_schema(IndexKind::MetadataFilter, IndexSemantics::MetadataFilter);
    let sources = (0..1_024)
        .map(|index| keyword_source(&format!("objects/{index:04}"), &format!("value/{index:04}")))
        .collect();
    let (schema, segment, directory) = build_fixture(schema, 92, sources).await;
    let executor =
        NativeQueryExecutor::new(&directory, &AllowAll, NativeQueryLimits::default()).unwrap();
    let mut query = request(
        &schema,
        &segment,
        NativeQuery::Filter {
            predicate: Some(Predicate::Prefix {
                id: PredicateId::new(1),
                field_id: FieldId::new(0),
                prefix: "value/".into(),
            }),
            order: Vec::new(),
        },
    );
    query.limit = 100;
    let statistics = NativeQueryStatisticsRecorder::new();

    let page = executor
        .execute_observed(&query, statistics.clone())
        .await
        .unwrap();

    assert_eq!(page.hits.len(), 100);
    let snapshot = statistics.snapshot();
    assert_eq!(snapshot.candidate_doc_ids, 1_024);
    assert_eq!(snapshot.term_seeks, 1, "{snapshot:?}");
}

#[tokio::test]
async fn independent_segments_rank_concurrently_and_merge_exact_top_k() {
    let mut schema = schema();
    schema.physical_order.clear();
    let clear_order = |mut source: ProjectedSource| {
        source.records[0].order_key.clear();
        source
    };
    let (schema, first, first_directory) = build_segment_fixture(
        schema,
        93,
        1,
        vec![
            clear_order(source("a", "active", 1)),
            clear_order(source("b", "active", 4)),
        ],
    )
    .await;
    let (_, second, second_directory) = build_segment_fixture(
        schema.clone(),
        93,
        2,
        vec![
            clear_order(source("c", "active", 2)),
            clear_order(source("d", "active", 5)),
        ],
    )
    .await;
    let mut objects = first_directory.0;
    objects.extend(second_directory.0);
    let directory = ParallelMemoryArtifacts {
        inner: MemoryArtifacts(objects),
        parallelism: 2,
    };
    let gate = ConcurrentGate::default();
    let executor =
        NativeQueryExecutor::new(&directory, &gate, NativeQueryLimits::default()).unwrap();
    let mut query = request(
        &schema,
        &first,
        NativeQuery::Filter {
            predicate: None,
            order: vec![OrderField {
                field_id: FieldId::new(1),
                direction: OrderDirection::Descending,
            }],
        },
    );
    query.segments.push(second);
    query.limit = 2;

    let page = executor.execute(&query).await.unwrap();

    assert_eq!(
        page.hits
            .iter()
            .map(|hit| hit.result.path.as_str())
            .collect::<Vec<_>>(),
        ["d", "b"]
    );
    assert_eq!(gate.maximum.load(AtomicOrdering::SeqCst), 2);
}

#[tokio::test]
async fn boolean_and_or_in_and_not_match_exact_sets() {
    let (schema, segment, directory) = fixture().await;
    let executor =
        NativeQueryExecutor::new(&directory, &AllowAll, NativeQueryLimits::default()).unwrap();
    let equal_state = |id, value: &str| Predicate::Equal {
        id: PredicateId::new(id),
        field_id: FieldId::new(0),
        value: ScalarValue::String(value.into()),
    };
    let equal_priority = |id, value| Predicate::Equal {
        id: PredicateId::new(id),
        field_id: FieldId::new(1),
        value: ScalarValue::Signed(value),
    };
    let query = |predicate| {
        request(
            &schema,
            &segment,
            NativeQuery::Filter {
                predicate: Some(predicate),
                order: Vec::new(),
            },
        )
    };

    let and = executor
        .execute(&query(Predicate::And(vec![
            equal_state(1, "active"),
            Predicate::Range {
                id: PredicateId::new(2),
                field_id: FieldId::new(1),
                lower: Some(RangeBound {
                    value: ScalarValue::Signed(3),
                    inclusive: true,
                }),
                upper: None,
            },
        ])))
        .await
        .unwrap();
    assert_eq!(paths(&and), BTreeSet::from(["c"]));

    let or = executor
        .execute(&query(Predicate::Or(vec![
            equal_state(3, "inactive"),
            equal_priority(4, 1),
        ])))
        .await
        .unwrap();
    assert_eq!(paths(&or), BTreeSet::from(["a", "b"]));

    let in_page = executor
        .execute(&query(Predicate::In {
            id: PredicateId::new(5),
            field_id: FieldId::new(0),
            values: vec![
                ScalarValue::String("active".into()),
                ScalarValue::String("inactive".into()),
            ],
        }))
        .await
        .unwrap();
    assert_eq!(paths(&in_page), BTreeSet::from(["a", "b", "c"]));

    let not = executor
        .execute(&query(Predicate::Not(Box::new(equal_state(6, "active")))))
        .await
        .unwrap();
    assert_eq!(paths(&not), BTreeSet::from(["b"]));
}

#[tokio::test]
async fn physical_order_and_search_after_are_stable() {
    let (schema, segment, directory) = fixture().await;
    let executor =
        NativeQueryExecutor::new(&directory, &AllowAll, NativeQueryLimits::default()).unwrap();
    let mut first = request(
        &schema,
        &segment,
        NativeQuery::Filter {
            predicate: None,
            order: schema.physical_order.clone(),
        },
    );
    first.limit = 2;
    let page = executor.execute(&first).await.unwrap();
    assert_eq!(
        page.hits
            .iter()
            .map(|hit| hit.result.path.as_str())
            .collect::<Vec<_>>(),
        ["b", "c"]
    );
    let mut second = first;
    second.after = page.next;
    let page = executor.execute(&second).await.unwrap();
    assert_eq!(
        page.hits
            .iter()
            .map(|hit| hit.result.path.as_str())
            .collect::<Vec<_>>(),
        ["a"]
    );
}

#[tokio::test]
async fn physical_order_memory_is_bounded_across_many_segments() {
    let (schema, segment, directory) = fixture().await;
    let executor =
        NativeQueryExecutor::new(&directory, &AllowAll, NativeQueryLimits::default()).unwrap();
    let one = request(
        &schema,
        &segment,
        NativeQuery::Filter {
            predicate: Some(Predicate::Equal {
                id: PredicateId::new(1),
                field_id: FieldId::new(0),
                value: ScalarValue::String("active".into()),
            }),
            order: schema.physical_order.clone(),
        },
    );
    let mut many = one.clone();
    many.segments = (1..=64)
        .map(|segment_id| {
            let mut descriptor = segment.clone();
            descriptor.identity.segment_id = segment_id;
            descriptor
        })
        .collect();

    let one_bytes = executor.working_memory_bytes(&one).unwrap();
    let many_bytes = executor.working_memory_bytes(&many).unwrap();

    assert!(many_bytes > one_bytes);
    assert!(many_bytes - one_bytes < crate::v4::INDEX_DECODE_BYTES);
}

#[tokio::test]
async fn arbitrary_top_k_agrees_with_physical_order_across_pages() {
    let (physical_schema, physical_segment, physical_directory) = fixture().await;
    let mut top_k_schema = physical_schema.clone();
    top_k_schema.physical_order.clear();
    let unordered = [
        source("a", "active", 1),
        source("b", "inactive", 5),
        source("c", "active", 3),
    ]
    .into_iter()
    .map(|mut source| {
        source.records[0].order_key.clear();
        source
    })
    .collect();
    let (_, top_k_segment, top_k_directory) =
        build_fixture(top_k_schema.clone(), 10, unordered).await;
    let order = physical_schema.physical_order.clone();
    let mut physical_request = request(
        &physical_schema,
        &physical_segment,
        NativeQuery::Filter {
            predicate: None,
            order: order.clone(),
        },
    );
    let mut top_k_request = request(
        &top_k_schema,
        &top_k_segment,
        NativeQuery::Filter {
            predicate: None,
            order,
        },
    );
    physical_request.limit = 2;
    top_k_request.limit = 2;
    let physical_executor =
        NativeQueryExecutor::new(&physical_directory, &AllowAll, NativeQueryLimits::default())
            .unwrap();
    let top_k_executor =
        NativeQueryExecutor::new(&top_k_directory, &AllowAll, NativeQueryLimits::default())
            .unwrap();
    let physical_first = physical_executor.execute(&physical_request).await.unwrap();
    let top_k_first = top_k_executor.execute(&top_k_request).await.unwrap();
    assert_eq!(
        physical_first
            .hits
            .iter()
            .map(|hit| hit.result.path.as_str())
            .collect::<Vec<_>>(),
        top_k_first
            .hits
            .iter()
            .map(|hit| hit.result.path.as_str())
            .collect::<Vec<_>>()
    );
    physical_request.after = physical_first.next;
    top_k_request.after = top_k_first.next;
    let physical_second = physical_executor.execute(&physical_request).await.unwrap();
    let top_k_second = top_k_executor.execute(&top_k_request).await.unwrap();
    assert_eq!(paths(&physical_second), paths(&top_k_second));
    assert_eq!(paths(&physical_second), BTreeSet::from(["a"]));
}

#[tokio::test]
async fn authorization_and_exact_current_rejections_refill_physical_results() {
    let (schema, segment, directory) = fixture().await;
    let gate = SelectiveGate {
        denied: BTreeSet::from(["b".to_owned()]),
        stale: BTreeSet::from(["c".to_owned()]),
        revision: 7,
    };
    let executor =
        NativeQueryExecutor::new(&directory, &gate, NativeQueryLimits::default()).unwrap();
    let mut query = request(
        &schema,
        &segment,
        NativeQuery::Filter {
            predicate: None,
            order: schema.physical_order.clone(),
        },
    );
    query.limit = 2;
    let page = executor.execute(&query).await.unwrap();
    assert_eq!(paths(&page), BTreeSet::from(["a"]));
    assert_eq!(page.statistics.candidate_gate_denied, 1);
    assert_eq!(page.statistics.candidate_gate_stale, 1);
    assert_eq!(page.statistics.candidate_gate_refills, 1);

    let wrong_revision = SelectiveGate {
        denied: BTreeSet::new(),
        stale: BTreeSet::new(),
        revision: 8,
    };
    let error = NativeQueryExecutor::new(&directory, &wrong_revision, NativeQueryLimits::default())
        .unwrap()
        .execute(&query)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        NativeQueryExecutionError::Index(IndexError::InvalidQuery(_))
    ));
}

#[tokio::test]
async fn facets_and_aggregates_are_exact_and_observed() {
    let (schema, segment, directory) = fixture().await;
    let executor =
        NativeQueryExecutor::new(&directory, &AllowAll, NativeQueryLimits::default()).unwrap();
    let mut request = request(
        &schema,
        &segment,
        NativeQuery::Filter {
            predicate: None,
            order: Vec::new(),
        },
    );
    request.facets.push(FacetRequest {
        field_id: FieldId::new(0),
        limit: 10,
    });
    request.aggregates.extend([
        AggregateRequest {
            field_id: FieldId::new(1),
            operation: AggregateOperation::Count,
        },
        AggregateRequest {
            field_id: FieldId::new(1),
            operation: AggregateOperation::Minimum,
        },
        AggregateRequest {
            field_id: FieldId::new(1),
            operation: AggregateOperation::Maximum,
        },
        AggregateRequest {
            field_id: FieldId::new(1),
            operation: AggregateOperation::Sum,
        },
        AggregateRequest {
            field_id: FieldId::new(1),
            operation: AggregateOperation::Average,
        },
    ]);
    let page = executor.execute(&request).await.unwrap();
    assert_eq!(
        page.facet_results[0].buckets[0].value,
        ScalarValue::String("active".into())
    );
    assert_eq!(page.facet_results[0].buckets[0].count, 2);
    assert_eq!(
        page.aggregate_results[0].value,
        Some(ScalarValue::Unsigned(3))
    );
    assert_eq!(
        page.aggregate_results[1].value,
        Some(ScalarValue::Signed(1))
    );
    assert_eq!(
        page.aggregate_results[2].value,
        Some(ScalarValue::Signed(5))
    );
    assert_eq!(
        page.aggregate_results[3].value,
        Some(ScalarValue::Signed(9))
    );
    assert_eq!(
        page.aggregate_results[4].value,
        Some(ScalarValue::number(3.0).unwrap())
    );
    assert_eq!(page.statistics.facet_documents_processed, 3);
    assert_eq!(page.statistics.aggregate_documents_processed, 15);
}

#[tokio::test]
async fn query_admission_checks_computations_and_empty_generation_cursors() {
    let (schema, segment, _) = fixture().await;
    let mut non_filter = request(
        &schema,
        &segment,
        NativeQuery::Path {
            prefix: String::new(),
            start_after: None,
        },
    );
    non_filter.facets.push(FacetRequest {
        field_id: FieldId::new(0),
        limit: 1,
    });
    assert!(non_filter.validate().is_err());

    let mut empty = request(
        &schema,
        &segment,
        NativeQuery::Filter {
            predicate: None,
            order: schema.physical_order.clone(),
        },
    );
    empty.segments.clear();
    empty.after = Some(crate::v4::NativeQueryCursor {
        sort_values: Vec::new(),
        result: ObjectIdentity {
            path: "a".into(),
            version: 1,
        },
        source: ObjectIdentity {
            path: "a".into(),
            version: 1,
        },
        source_record: 0,
    });
    assert!(empty.validate().is_err());
}

#[tokio::test]
async fn path_and_metadata_filter_complete_the_all_kind_native_matrix() {
    let path_schema = keyword_schema(IndexKind::Path, IndexSemantics::Path);
    let (path_schema, path_segment, path_directory) = build_fixture(
        path_schema,
        20,
        vec![
            keyword_source("docs/a", "docs/a"),
            keyword_source("docs/b", "docs/b"),
            keyword_source("other/c", "other/c"),
        ],
    )
    .await;
    let path_executor =
        NativeQueryExecutor::new(&path_directory, &AllowAll, NativeQueryLimits::default()).unwrap();
    let page = path_executor
        .execute(&request(
            &path_schema,
            &path_segment,
            NativeQuery::Path {
                prefix: "docs/".into(),
                start_after: Some("docs/a".into()),
            },
        ))
        .await
        .unwrap();
    assert_eq!(paths(&page), BTreeSet::from(["docs/b"]));

    let metadata_schema = keyword_schema(IndexKind::MetadataFilter, IndexSemantics::MetadataFilter);
    let (metadata_schema, metadata_segment, metadata_directory) = build_fixture(
        metadata_schema,
        21,
        vec![
            keyword_source("objects/json", "application/json"),
            keyword_source("objects/text", "text/plain"),
        ],
    )
    .await;
    let metadata_executor =
        NativeQueryExecutor::new(&metadata_directory, &AllowAll, NativeQueryLimits::default())
            .unwrap();
    let page = metadata_executor
        .execute(&request(
            &metadata_schema,
            &metadata_segment,
            NativeQuery::Filter {
                predicate: Some(Predicate::Equal {
                    id: PredicateId::new(1),
                    field_id: FieldId::new(0),
                    value: ScalarValue::String("application/json".into()),
                }),
                order: Vec::new(),
            },
        ))
        .await
        .unwrap();
    assert_eq!(paths(&page), BTreeSet::from(["objects/json"]));
}

#[tokio::test]
async fn executor_fails_closed_on_generation_corruption_and_resource_limits() {
    let (schema, segment, directory) = fixture().await;
    let mut wrong_generation = segment.clone();
    wrong_generation.identity.definition_version += 1;
    wrong_generation.identity.segment_id += 1;
    let mut invalid = request(
        &schema,
        &segment,
        NativeQuery::Filter {
            predicate: None,
            order: Vec::new(),
        },
    );
    invalid.segments.push(wrong_generation);
    assert!(invalid.validate().is_err());

    let limited = NativeQueryExecutor::new(
        &directory,
        &AllowAll,
        NativeQueryLimits {
            maximum_result_limit: 1,
            ..NativeQueryLimits::default()
        },
    )
    .unwrap();
    let mut oversized = request(
        &schema,
        &segment,
        NativeQuery::Filter {
            predicate: None,
            order: Vec::new(),
        },
    );
    oversized.limit = 2;
    assert!(matches!(
        limited.execute(&oversized).await,
        Err(NativeQueryExecutionError::Index(
            IndexError::ResourceLimit { .. }
        ))
    ));

    let mut corrupt_directory = directory;
    let identity = segment
        .components
        .iter()
        .find(|component| component.role == ComponentKind::IDENTITY_TABLE)
        .unwrap()
        .artifact
        .clone();
    let pack = identity
        .pack(segment.identity.index_id, &segment.packs)
        .unwrap();
    let object = corrupt_directory.0.get_mut(&pack.path).unwrap();
    let byte = usize::try_from(identity.offset).unwrap() + 16;
    object.bytes[byte] ^= 1;
    let corrupt =
        NativeQueryExecutor::new(&corrupt_directory, &AllowAll, NativeQueryLimits::default())
            .unwrap()
            .execute(&request(
                &schema,
                &segment,
                NativeQuery::Filter {
                    predicate: None,
                    order: Vec::new(),
                },
            ))
            .await;
    assert!(matches!(
        corrupt,
        Err(NativeQueryExecutionError::Index(IndexError::Integrity))
            | Err(NativeQueryExecutionError::Index(IndexError::InvalidFormat(
                _
            )))
    ));
}
