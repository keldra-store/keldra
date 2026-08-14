use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::*;
use crate::IndexFileRead;
use crate::v4::build::{
    BuildLimits, ExactMemorySink, NativeSegmentWriter, ProjectedDocValue, ProjectedPoint,
    ProjectedRecord, ProjectedSource, PublishedObject, SourcePush,
};
use crate::v4::{
    AggregateOperation, AggregateRequest, ArtifactDescriptor, Cardinality, Collation,
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
        Ok(Arc::from(&self.0[start..start.saturating_add(maximum).min(self.0.len())]))
    }
}

struct MemoryArtifacts(BTreeMap<String, PublishedObject>);

impl ArtifactDirectoryRead for MemoryArtifacts {
    type File = MemoryFile;

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
        let end = start.checked_add(length).ok_or(IndexError::OffsetOverflow)?;
        Ok(MemoryFile(Arc::from(
            object.bytes.get(start..end).ok_or(IndexError::Integrity)?,
        )))
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
    page.hits.iter().map(|hit| hit.result.path.as_str()).collect()
}

#[tokio::test]
async fn exact_prefix_range_and_exists_use_declared_native_components() {
    let (schema, segment, directory) = fixture().await;
    let executor = NativeQueryExecutor::new(&directory, &AllowAll, NativeQueryLimits::default())
        .unwrap();

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
async fn physical_order_and_search_after_are_stable() {
    let (schema, segment, directory) = fixture().await;
    let executor = NativeQueryExecutor::new(&directory, &AllowAll, NativeQueryLimits::default())
        .unwrap();
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
    assert_eq!(page.hits.iter().map(|hit| hit.result.path.as_str()).collect::<Vec<_>>(), ["b", "c"]);
    let mut second = first;
    second.after = page.next;
    let page = executor.execute(&second).await.unwrap();
    assert_eq!(page.hits.iter().map(|hit| hit.result.path.as_str()).collect::<Vec<_>>(), ["a"]);
}

#[tokio::test]
async fn facets_and_aggregates_are_exact_and_observed() {
    let (schema, segment, directory) = fixture().await;
    let executor = NativeQueryExecutor::new(&directory, &AllowAll, NativeQueryLimits::default())
        .unwrap();
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
    assert_eq!(page.facet_results[0].buckets[0].value, ScalarValue::String("active".into()));
    assert_eq!(page.facet_results[0].buckets[0].count, 2);
    assert_eq!(page.aggregate_results[0].value, Some(ScalarValue::Unsigned(3)));
    assert_eq!(page.aggregate_results[1].value, Some(ScalarValue::Signed(1)));
    assert_eq!(page.aggregate_results[2].value, Some(ScalarValue::Signed(5)));
    assert_eq!(page.aggregate_results[3].value, Some(ScalarValue::Signed(9)));
    assert_eq!(page.aggregate_results[4].value, Some(ScalarValue::number(3.0).unwrap()));
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
