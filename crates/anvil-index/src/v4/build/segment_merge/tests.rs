use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::compaction::{
    CompactionExecutor, CompactionParallelism, CompactionProgress, CompactionTaskFuture,
    CompactionTaskHandle,
};
use crate::{IndexError, IndexFileRead};

use super::super::super::{
    AggregateOperation, AggregateRequest, Analyzer, ArtifactDirectoryRead, ArtifactPackReference,
    CandidateGate, CandidateGateEvidence, CandidateReference, Cardinality, Collation,
    ComponentKind, ComponentStatistics, ComponentVersion, DocId, DocIdRange, DocValueCell,
    FacetRequest, FieldCapabilities, FieldComponents, FieldId, FieldSchema, FieldStatistics,
    FieldType, IndexKind, IndexSemantics, NativeQuery, NativeQueryExecutor, NativeQueryLimits,
    NativeQueryRequest, ObjectIdentity, OrderDirection, OrderField, Predicate, PredicateId,
    RangeBound, ScalarValue, Schema, SegmentComponent, SegmentComponentReader, SegmentDescriptor,
    SegmentIdentity, SortValue, VectorMetric, VectorNormalization, encode_physical_order_key,
    rewrite_segment_live_mask,
};
use super::super::{
    BuildLimits, ComponentBatchSink, ExactMemorySink, MergeScratchFile, MergeScratchSpace,
    NativeSegmentWriter, ProjectedDocValue, ProjectedPoint, ProjectedRecord, ProjectedSource,
    ProjectedTerm, ProjectedVector, SourcePush,
};
use super::{SegmentStream, merge_schema_workspace_bytes, merge_segments};

#[derive(Clone, Default)]
struct SharedSink {
    state: Arc<Mutex<SinkState>>,
}

#[derive(Default)]
struct SinkState(ExactMemorySink);

fn poll_ready<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(std::task::Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("exact memory sink future unexpectedly pending"),
    }
}

impl ComponentBatchSink for SharedSink {
    fn begin_segment(
        &mut self,
        identity: SegmentIdentity,
        base_packs: &[ArtifactPackReference],
    ) -> Result<(), IndexError> {
        self.state
            .lock()
            .map_err(|_| IndexError::Io("test sink lock poisoned".into()))?
            .0
            .begin_segment(identity, base_packs)
    }

    fn stage_component(
        &mut self,
        component: super::super::super::GeneratedComponent,
    ) -> impl Future<Output = Result<super::super::super::ArtifactDescriptor, IndexError>> + Send
    {
        let result = (|| {
            let mut state = self
                .state
                .lock()
                .map_err(|_| IndexError::Io("test sink lock poisoned".into()))?;
            poll_ready(state.0.stage_component(component))
        })();
        std::future::ready(result)
    }

    fn finalize_segment(
        &mut self,
        identity: SegmentIdentity,
    ) -> impl Future<Output = Result<Vec<ArtifactPackReference>, IndexError>> + Send {
        let result = (|| {
            let mut state = self
                .state
                .lock()
                .map_err(|_| IndexError::Io("test sink lock poisoned".into()))?;
            poll_ready(state.0.finalize_segment(identity))
        })();
        std::future::ready(result)
    }
}

#[derive(Clone)]
struct SharedDirectory(SharedSink);

struct MemoryFile(Arc<[u8]>);

impl IndexFileRead for MemoryFile {
    type Slice = Arc<[u8]>;

    async fn read_at(&self, offset: u64, maximum: usize) -> Result<Self::Slice, IndexError> {
        let start = usize::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?;
        let end = start.saturating_add(maximum).min(self.0.len());
        Ok(self.0.get(start..end).unwrap_or_default().to_vec().into())
    }
}

impl ArtifactDirectoryRead for SharedDirectory {
    type File = MemoryFile;

    fn open_artifact(
        &self,
        pack: &ArtifactPackReference,
    ) -> impl Future<Output = Result<Self::File, IndexError>> + Send {
        let result = (|| {
            let state = self
                .0
                .state
                .lock()
                .map_err(|_| IndexError::Io("test sink lock poisoned".into()))?;
            let object = state
                .0
                .objects()
                .get(&pack.path)
                .ok_or_else(|| IndexError::FileNotFound(pack.path.clone()))?;
            if object.object_version != pack.object_version
                || *blake3::hash(&object.bytes).as_bytes() != pack.object_content_hash
                || object.bytes.len() as u64 != pack.object_length
            {
                return Err(IndexError::Integrity);
            }
            Ok(MemoryFile(object.bytes.clone().into()))
        })();
        std::future::ready(result)
    }
}

#[derive(Clone, Default)]
struct MemoryScratch {
    files: Arc<Mutex<Vec<MemoryScratchFile>>>,
}

#[derive(Clone, Default)]
struct MemoryScratchFile(Arc<Mutex<Vec<u8>>>);

impl MergeScratchSpace for MemoryScratch {
    type File = MemoryScratchFile;

    fn create_file(&self) -> impl Future<Output = Result<Self::File, IndexError>> + Send {
        let file = MemoryScratchFile::default();
        let result = self
            .files
            .lock()
            .map_err(|_| IndexError::Io("test scratch lock poisoned".into()))
            .map(|mut files| {
                files.push(file.clone());
                file
            });
        std::future::ready(result)
    }
}

impl MergeScratchFile for MemoryScratchFile {
    fn resize_zeroed(&self, length: u64) -> impl Future<Output = Result<(), IndexError>> + Send {
        let result = usize::try_from(length)
            .map_err(|_| IndexError::OffsetOverflow)
            .and_then(|length| {
                self.0
                    .lock()
                    .map_err(|_| IndexError::Io("test scratch lock poisoned".into()))?
                    .resize(length, 0);
                Ok(())
            });
        std::future::ready(result)
    }

    fn write_all_at(
        &self,
        offset: u64,
        bytes: Vec<u8>,
    ) -> impl Future<Output = Result<(), IndexError>> + Send {
        let result = (|| {
            let start = usize::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?;
            let end = start
                .checked_add(bytes.len())
                .ok_or(IndexError::OffsetOverflow)?;
            let mut target = self
                .0
                .lock()
                .map_err(|_| IndexError::Io("test scratch lock poisoned".into()))?;
            target
                .get_mut(start..end)
                .ok_or(IndexError::InvalidFormat("test scratch write range"))?
                .copy_from_slice(&bytes);
            Ok(())
        })();
        std::future::ready(result)
    }

    fn append(&self, bytes: Vec<u8>) -> impl Future<Output = Result<u64, IndexError>> + Send {
        let result = (|| {
            let mut target = self
                .0
                .lock()
                .map_err(|_| IndexError::Io("test scratch lock poisoned".into()))?;
            let offset = target.len() as u64;
            target.extend_from_slice(&bytes);
            Ok(offset)
        })();
        std::future::ready(result)
    }

    fn read_exact_at(
        &self,
        offset: u64,
        length: usize,
    ) -> impl Future<Output = Result<Vec<u8>, IndexError>> + Send {
        let result = (|| {
            let start = usize::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?;
            let end = start
                .checked_add(length)
                .ok_or(IndexError::OffsetOverflow)?;
            Ok(self
                .0
                .lock()
                .map_err(|_| IndexError::Io("test scratch lock poisoned".into()))?
                .get(start..end)
                .ok_or(IndexError::InvalidFormat("test scratch read range"))?
                .to_vec())
        })();
        std::future::ready(result)
    }

    fn len(&self) -> impl Future<Output = Result<u64, IndexError>> + Send {
        let result = self
            .0
            .lock()
            .map_err(|_| IndexError::Io("test scratch lock poisoned".into()))
            .map(|bytes| bytes.len() as u64);
        std::future::ready(result)
    }
}

#[derive(Clone)]
struct TokioExecutor;

struct TokioTask(tokio::task::JoinHandle<Result<(), IndexError>>);

impl Future for TokioTask {
    type Output = Result<(), IndexError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(error)) => Poll::Ready(Err(IndexError::Io(error.to_string()))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl CompactionTaskHandle for TokioTask {
    fn abort(&self) {
        self.0.abort();
    }
}

impl CompactionExecutor for TokioExecutor {
    type Task = TokioTask;

    fn spawn_io(&self, task: CompactionTaskFuture) -> Self::Task {
        TokioTask(tokio::spawn(task))
    }

    async fn run_cpu<T, F>(&self, work: F) -> Result<T, IndexError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, IndexError> + Send + 'static,
    {
        work()
    }
}

fn versions(kinds: &[ComponentKind]) -> Vec<ComponentVersion> {
    let mut kinds = kinds.to_vec();
    kinds.sort();
    kinds.dedup();
    kinds
        .into_iter()
        .map(|component_kind| ComponentVersion {
            component_kind,
            codec_version: if component_kind == ComponentKind::IDENTITY_TABLE {
                2
            } else {
                1
            },
        })
        .collect()
}

fn hybrid_schema() -> Schema {
    Schema {
        kind: IndexKind::Hybrid,
        path_prefix: "objects/".into(),
        content_type_scope: Some("application/json".into()),
        fields: vec![
            FieldSchema {
                id: FieldId::new(0),
                name: "body".into(),
                source_selector: "/body".into(),
                field_type: FieldType::Text,
                cardinality: Cardinality::Single,
                allow_missing: true,
                allow_null: false,
                collation: Collation::BinaryUtf8,
                capabilities: FieldCapabilities::FULL_TEXT,
                analyzer: Some(Analyzer::UnicodeAlphanumericLowercase),
                components: FieldComponents::TERMS
                    .union(FieldComponents::POSITIONS)
                    .union(FieldComponents::NORMS),
            },
            FieldSchema {
                id: FieldId::new(1),
                name: "vector".into(),
                source_selector: "/vector".into(),
                field_type: FieldType::Vector,
                cardinality: Cardinality::Single,
                allow_missing: true,
                allow_null: false,
                collation: Collation::BinaryUtf8,
                capabilities: FieldCapabilities::empty(),
                analyzer: None,
                components: FieldComponents::VECTOR,
            },
        ],
        semantics: IndexSemantics::Hybrid {
            analyzer: Analyzer::UnicodeAlphanumericLowercase,
            bm25_k1: 1.2,
            bm25_b: 0.75,
            dimensions: 2,
            metric: VectorMetric::Cosine,
            normalization: VectorNormalization::L2,
            lexical_weight: 0.5,
            vector_weight: 0.5,
        },
        physical_order: Vec::new(),
        component_versions: versions(&[
            ComponentKind::ROUTING_NODE,
            ComponentKind::IDENTITY_TABLE,
            ComponentKind::LIVE_MASK,
            ComponentKind::PATH_LOCATOR,
            ComponentKind::TERM_DICTIONARY,
            ComponentKind::POSTINGS,
            ComponentKind::POSITIONS,
            ComponentKind::NORMS,
            ComponentKind::VECTORS,
            ComponentKind::SCORING_STATISTICS,
        ]),
    }
}

fn hybrid_source(path: &str, value: &str) -> ProjectedSource {
    ProjectedSource {
        source_identity: ObjectIdentity {
            path: path.into(),
            version: 1,
        },
        records: vec![ProjectedRecord {
            result_identity: Some(ObjectIdentity {
                path: format!("results/{path}"),
                version: 3,
            }),
            order_key: Vec::new(),
            terms: vec![ProjectedTerm {
                field_id: FieldId::new(0),
                term_type: super::super::super::TERM_TYPE_TEXT,
                term: value.as_bytes().to_vec(),
                frequency: 2,
                positions: vec![1, 4],
            }],
            points: Vec::new(),
            doc_values: Vec::new(),
            vectors: vec![ProjectedVector {
                field_id: FieldId::new(1),
                values: vec![0.25, 0.75],
            }],
            field_lengths: vec![(FieldId::new(0), 2)],
        }],
    }
}

async fn build_segment(
    sink: &mut SharedSink,
    schema: &Schema,
    segment_id: u64,
    sources: Vec<ProjectedSource>,
    limits: usize,
) -> super::super::BuiltSegment {
    let identity = SegmentIdentity::new(9, 2, schema.fingerprint().unwrap(), segment_id).unwrap();
    let mut writer =
        NativeSegmentWriter::new(identity, schema.clone(), BuildLimits::new(limits).unwrap())
            .unwrap();
    for source in sources {
        assert_eq!(writer.push_source(source).unwrap(), SourcePush::Accepted);
    }
    writer.seal(sink).await.unwrap()
}

#[tokio::test]
async fn streaming_merge_preserves_all_native_components() {
    let schema = hybrid_schema();
    let mut sink = SharedSink::default();
    let mut repeated = hybrid_source("objects/b", "beta");
    repeated.records.push(repeated.records[0].clone());
    let left = build_segment(&mut sink, &schema, 10, vec![repeated], 64 * 1024 * 1024).await;
    let right = build_segment(
        &mut sink,
        &schema,
        11,
        vec![hybrid_source("objects/a", "alpha")],
        64 * 1024 * 1024,
    )
    .await;
    let directory = SharedDirectory(sink.clone());
    let progress = CompactionProgress::default();
    let merged = merge_segments(
        &directory,
        &schema,
        &[left.descriptor, right.descriptor],
        SegmentIdentity::new(9, 2, schema.fingerprint().unwrap(), 12).unwrap(),
        BuildLimits::new(64 * 1024 * 1024).unwrap(),
        &mut sink,
        &MemoryScratch::default(),
        TokioExecutor,
        CompactionParallelism::new(4, 64 * 1024 * 1024).unwrap(),
        progress.clone(),
    )
    .await
    .unwrap();
    assert_eq!(merged.descriptor.document_count, 3);
    assert_eq!(merged.source_count, 2);
    for kind in [
        ComponentKind::IDENTITY_TABLE,
        ComponentKind::LIVE_MASK,
        ComponentKind::TERM_DICTIONARY,
        ComponentKind::POSTINGS,
        ComponentKind::POSITIONS,
        ComponentKind::NORMS,
        ComponentKind::VECTORS,
        ComponentKind::SCORING_STATISTICS,
    ] {
        assert!(
            merged
                .descriptor
                .components
                .iter()
                .any(|component| component.role == kind),
            "missing component {}",
            kind.get()
        );
    }
    let reader = SegmentComponentReader::new(&directory, &merged.descriptor).unwrap();
    let identities = reader.identity_blocks(None, None).await.unwrap();
    assert_eq!(identities[0].entries()[0].source.path, "objects/a");
    assert_eq!(identities[0].entries()[1].source.path, "objects/b");
    assert_eq!(identities[0].entries()[2].source.path, "objects/b");
    assert_eq!(identities[0].entries()[0].source_record, 0);
    assert_eq!(identities[0].entries()[1].source_record, 0);
    assert_eq!(identities[0].entries()[2].source_record, 1);
    let dictionary = reader
        .term_dictionaries(FieldId::new(0), None, None)
        .await
        .unwrap();
    assert_eq!(
        dictionary
            .iter()
            .map(|block| block.entries().len())
            .sum::<usize>(),
        2
    );
    let mut posting_documents = 0usize;
    let mut document_frequencies = Vec::new();
    let mut total_term_frequencies = Vec::new();
    for entry in dictionary.iter().flat_map(|block| block.entries()) {
        let postings = reader
            .posting_blocks(
                FieldId::new(0),
                entry.postings.first_component_ordinal,
                entry.postings.component_count,
            )
            .await
            .unwrap();
        posting_documents += postings
            .iter()
            .map(|block| block.doc_ids().len())
            .sum::<usize>();
        assert!(postings.iter().all(|block| {
            block.impact()
                == Some(super::super::super::PostingImpact {
                    maximum_frequency: 2,
                    minimum_field_length: 2,
                })
        }));
        document_frequencies.push(entry.postings.document_frequency);
        total_term_frequencies.push(entry.postings.total_term_frequency);
    }
    document_frequencies.sort_unstable();
    total_term_frequencies.sort_unstable();
    assert_eq!(document_frequencies, [1, 2]);
    assert_eq!(total_term_frequencies, [2, 4]);
    assert_eq!(posting_documents, 3);
    let statistics = reader.statistics().await.unwrap();
    assert_eq!(statistics.source_count, 2);
    assert_eq!(statistics.document_count, 3);
    assert_eq!(statistics.unique_terms, 2);
    assert_eq!(statistics.fields[0].present_documents, 3);
    assert_eq!(statistics.fields[0].total_term_frequency, 6);
    assert_eq!(statistics.fields[0].total_field_length, 6);
    assert_eq!(statistics.fields[0].minimum_field_length, Some(2));
    assert_eq!(statistics.fields[0].maximum_field_length, Some(2));
    assert_eq!(statistics.fields[1].vector_count, 3);
    assert_eq!(statistics.fields[1].vector_dimensions, Some(2));
    assert_eq!(statistics.fields[0].string_values, 0);
    assert_eq!(statistics.fields[0].multi_valued_documents, 0);
    assert!(statistics.physical_order_bounds.is_none());
    assert_eq!(statistics.components.len(), 3);
    assert!(statistics.components.iter().all(|component| {
        component.leaf_count > 0
            && component.component_count >= component.leaf_count
            && component.decoded_bytes_upper_bound
                == component.component_count * super::super::super::INDEX_DECODE_BYTES as u64
    }));
    let progress = progress.snapshot();
    assert!(progress.ranges_total >= 4);
    assert!(progress.ranges_completed >= 4);
    assert!(progress.effective_lanes >= 2);
}

struct AllowAllCandidates;

impl CandidateGate for AllowAllCandidates {
    type Error = IndexError;

    fn evaluate(
        &self,
        candidates: &[CandidateReference],
    ) -> impl Future<Output = Result<CandidateGateEvidence, Self::Error>> + Send {
        std::future::ready(Ok(CandidateGateEvidence {
            visible: vec![true; candidates.len()],
            authorization_revision: 1,
            denied: 0,
            stale: 0,
        }))
    }
}

fn numeric_point_schema() -> Schema {
    Schema {
        kind: IndexKind::TypedJson,
        path_prefix: "objects/".into(),
        content_type_scope: Some("application/json".into()),
        fields: vec![FieldSchema {
            id: FieldId::new(0),
            name: "priority".into(),
            source_selector: "/priority".into(),
            field_type: FieldType::SignedInteger,
            cardinality: Cardinality::Single,
            allow_missing: true,
            allow_null: true,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::EXACT.union(FieldCapabilities::RANGE),
            analyzer: None,
            components: FieldComponents::POINTS,
        }],
        semantics: IndexSemantics::TypedJson,
        physical_order: Vec::new(),
        component_versions: versions(&[
            ComponentKind::ROUTING_NODE,
            ComponentKind::IDENTITY_TABLE,
            ComponentKind::LIVE_MASK,
            ComponentKind::PATH_LOCATOR,
            ComponentKind::POINTS,
            ComponentKind::SCORING_STATISTICS,
        ]),
    }
}

fn numeric_point_source(path: &str, present: bool, value: Option<i64>) -> ProjectedSource {
    ProjectedSource {
        source_identity: ObjectIdentity {
            path: path.into(),
            version: 1,
        },
        records: vec![ProjectedRecord {
            result_identity: None,
            order_key: Vec::new(),
            terms: Vec::new(),
            points: present
                .then(|| ProjectedPoint {
                    field_id: FieldId::new(0),
                    present: true,
                    null: value.is_none(),
                    values: value.into_iter().map(ScalarValue::Signed).collect(),
                })
                .into_iter()
                .collect(),
            doc_values: Vec::new(),
            vectors: Vec::new(),
            field_lengths: Vec::new(),
        }],
    }
}

async fn query_paths(
    directory: &SharedDirectory,
    schema: &Schema,
    segments: Vec<SegmentDescriptor>,
    predicate: Predicate,
) -> BTreeSet<String> {
    let gate = AllowAllCandidates;
    let executor =
        NativeQueryExecutor::new(directory, &gate, NativeQueryLimits::default()).unwrap();
    let request = NativeQueryRequest {
        schema: schema.clone(),
        segments,
        query: NativeQuery::Filter {
            predicate: Some(predicate),
            order: Vec::new(),
        },
        after: None,
        limit: 100,
        facets: Vec::new(),
        aggregates: Vec::new(),
        authorization_revision: 1,
    };
    let page = executor.execute(&request).await.unwrap();
    page.hits.into_iter().map(|hit| hit.result.path).collect()
}

#[tokio::test]
async fn numeric_point_queries_are_equivalent_before_and_after_merge() {
    let schema = numeric_point_schema();
    let mut sink = SharedSink::default();
    let left = build_segment(
        &mut sink,
        &schema,
        30,
        vec![
            numeric_point_source("objects/five", true, Some(5)),
            numeric_point_source("objects/null", true, None),
        ],
        64 * 1024 * 1024,
    )
    .await;
    let right = build_segment(
        &mut sink,
        &schema,
        31,
        vec![
            numeric_point_source("objects/ten", true, Some(10)),
            numeric_point_source("objects/twenty", true, Some(20)),
            numeric_point_source("objects/missing", false, None),
        ],
        64 * 1024 * 1024,
    )
    .await;
    let inputs = vec![left.descriptor.clone(), right.descriptor.clone()];
    let directory = SharedDirectory(sink.clone());
    let merged = merge_segments(
        &directory,
        &schema,
        &inputs,
        SegmentIdentity::new(9, 2, schema.fingerprint().unwrap(), 32).unwrap(),
        BuildLimits::new(64 * 1024 * 1024).unwrap(),
        &mut sink,
        &MemoryScratch::default(),
        TokioExecutor,
        CompactionParallelism::new(2, 64 * 1024 * 1024).unwrap(),
        CompactionProgress::default(),
    )
    .await
    .unwrap();
    let directory = SharedDirectory(sink);
    let cases = [
        (
            Predicate::Equal {
                id: PredicateId::new(1),
                field_id: FieldId::new(0),
                value: ScalarValue::Null,
            },
            BTreeSet::from(["objects/null".to_owned()]),
        ),
        (
            Predicate::Equal {
                id: PredicateId::new(1),
                field_id: FieldId::new(0),
                value: ScalarValue::Signed(10),
            },
            BTreeSet::from(["objects/ten".to_owned()]),
        ),
        (
            Predicate::Range {
                id: PredicateId::new(1),
                field_id: FieldId::new(0),
                lower: Some(RangeBound {
                    value: ScalarValue::Signed(6),
                    inclusive: true,
                }),
                upper: Some(RangeBound {
                    value: ScalarValue::Signed(20),
                    inclusive: false,
                }),
            },
            BTreeSet::from(["objects/ten".to_owned()]),
        ),
        (
            Predicate::Exists {
                id: PredicateId::new(1),
                field_id: FieldId::new(0),
            },
            BTreeSet::from([
                "objects/five".to_owned(),
                "objects/null".to_owned(),
                "objects/ten".to_owned(),
                "objects/twenty".to_owned(),
            ]),
        ),
    ];
    for (predicate, expected) in cases {
        let before = query_paths(&directory, &schema, inputs.clone(), predicate.clone()).await;
        let after = query_paths(
            &directory,
            &schema,
            vec![merged.descriptor.clone()],
            predicate,
        )
        .await;
        assert_eq!(before, expected);
        assert_eq!(after, before);
    }
}

fn terms_only_keyword_schema() -> Schema {
    Schema {
        kind: IndexKind::TypedJson,
        path_prefix: "objects/".into(),
        content_type_scope: Some("application/json".into()),
        fields: vec![FieldSchema {
            id: FieldId::new(0),
            name: "state".into(),
            source_selector: "/state".into(),
            field_type: FieldType::Keyword,
            cardinality: Cardinality::Multi,
            allow_missing: true,
            allow_null: true,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::EXACT,
            analyzer: None,
            components: FieldComponents::TERMS,
        }],
        semantics: IndexSemantics::TypedJson,
        physical_order: Vec::new(),
        component_versions: versions(&[
            ComponentKind::ROUTING_NODE,
            ComponentKind::IDENTITY_TABLE,
            ComponentKind::LIVE_MASK,
            ComponentKind::PATH_LOCATOR,
            ComponentKind::TERM_DICTIONARY,
            ComponentKind::POSTINGS,
            ComponentKind::SCORING_STATISTICS,
        ]),
    }
}

fn terms_only_keyword_source(path: &str, value: ScalarValue, frequency: u32) -> ProjectedSource {
    let (term_type, term) = super::super::super::scalar_term(&value).unwrap();
    let mut terms = vec![
        ProjectedTerm {
            field_id: FieldId::new(0),
            term_type,
            term,
            frequency,
            positions: Vec::new(),
        },
        ProjectedTerm {
            field_id: FieldId::new(0),
            term_type: super::super::super::TERM_TYPE_FIELD_PRESENCE,
            term: super::super::super::FIELD_PRESENCE_TERM.to_vec(),
            frequency: 1,
            positions: Vec::new(),
        },
    ];
    terms.sort_by(|left, right| {
        (left.term_type, left.term.as_slice()).cmp(&(right.term_type, right.term.as_slice()))
    });
    ProjectedSource {
        source_identity: ObjectIdentity {
            path: path.into(),
            version: 1,
        },
        records: vec![ProjectedRecord {
            result_identity: None,
            order_key: Vec::new(),
            terms,
            points: Vec::new(),
            doc_values: Vec::new(),
            vectors: Vec::new(),
            field_lengths: Vec::new(),
        }],
    }
}

#[tokio::test]
async fn terms_only_statistics_survive_merge_without_presence_double_counting() {
    let schema = terms_only_keyword_schema();
    let mut sink = SharedSink::default();
    let left = build_segment(
        &mut sink,
        &schema,
        40,
        vec![terms_only_keyword_source(
            "objects/null",
            ScalarValue::Null,
            1,
        )],
        64 * 1024 * 1024,
    )
    .await;
    let right = build_segment(
        &mut sink,
        &schema,
        41,
        vec![terms_only_keyword_source(
            "objects/active",
            ScalarValue::String("active".into()),
            2,
        )],
        64 * 1024 * 1024,
    )
    .await;
    let inputs = vec![left.descriptor, right.descriptor];
    let directory = SharedDirectory(sink.clone());
    let merged = merge_segments(
        &directory,
        &schema,
        &inputs,
        SegmentIdentity::new(9, 2, schema.fingerprint().unwrap(), 42).unwrap(),
        BuildLimits::new(64 * 1024 * 1024).unwrap(),
        &mut sink,
        &MemoryScratch::default(),
        TokioExecutor,
        CompactionParallelism::new(2, 64 * 1024 * 1024).unwrap(),
        CompactionProgress::default(),
    )
    .await
    .unwrap();
    let directory = SharedDirectory(sink);
    assert_eq!(
        query_paths(
            &directory,
            &schema,
            vec![merged.descriptor.clone()],
            Predicate::Equal {
                id: PredicateId::new(1),
                field_id: FieldId::new(0),
                value: ScalarValue::Null,
            },
        )
        .await,
        BTreeSet::from(["objects/null".to_owned()]),
    );
    let statistics = SegmentComponentReader::new(&directory, &merged.descriptor)
        .unwrap()
        .statistics()
        .await
        .unwrap();
    let field = &statistics.fields[0];
    assert_eq!(field.present_documents, 2);
    assert_eq!(field.null_documents, 1);
    assert_eq!(field.value_count, 2);
    assert_eq!(field.string_values, 2);
    assert_eq!(field.unique_terms, 2);
    assert_eq!(field.total_term_frequency, 3);
}

fn numeric_computation_schema() -> Schema {
    Schema {
        kind: IndexKind::TypedJson,
        path_prefix: "objects/".into(),
        content_type_scope: Some("application/json".into()),
        fields: vec![FieldSchema {
            id: FieldId::new(0),
            name: "score".into(),
            source_selector: "/score".into(),
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
        }],
        semantics: IndexSemantics::TypedJson,
        physical_order: vec![OrderField {
            field_id: FieldId::new(0),
            direction: OrderDirection::Descending,
        }],
        component_versions: versions(&[
            ComponentKind::ROUTING_NODE,
            ComponentKind::IDENTITY_TABLE,
            ComponentKind::LIVE_MASK,
            ComponentKind::PATH_LOCATOR,
            ComponentKind::POINTS,
            ComponentKind::DOC_VALUES,
            ComponentKind::SCORING_STATISTICS,
        ]),
    }
}

fn numeric_computation_source(path: &str, score: i64) -> ProjectedSource {
    let value = ScalarValue::Signed(score);
    ProjectedSource {
        source_identity: ObjectIdentity {
            path: path.into(),
            version: 1,
        },
        records: vec![ProjectedRecord {
            result_identity: None,
            order_key: encode_physical_order_key(&[(
                SortValue::Value(value.clone()),
                OrderDirection::Descending,
            )])
            .unwrap(),
            terms: Vec::new(),
            points: vec![ProjectedPoint {
                field_id: FieldId::new(0),
                present: true,
                null: false,
                values: vec![value.clone()],
            }],
            doc_values: vec![ProjectedDocValue {
                field_id: FieldId::new(0),
                multi_valued: false,
                cell: DocValueCell::value(value),
            }],
            vectors: Vec::new(),
            field_lengths: Vec::new(),
        }],
    }
}

async fn computation_page(
    directory: &SharedDirectory,
    schema: &Schema,
    segments: Vec<SegmentDescriptor>,
) -> super::super::super::NativeQueryPage {
    let gate = AllowAllCandidates;
    NativeQueryExecutor::new(directory, &gate, NativeQueryLimits::default())
        .unwrap()
        .execute(&NativeQueryRequest {
            schema: schema.clone(),
            segments,
            query: NativeQuery::Filter {
                predicate: Some(Predicate::Range {
                    id: PredicateId::new(1),
                    field_id: FieldId::new(0),
                    lower: Some(RangeBound {
                        value: ScalarValue::Signed(0),
                        inclusive: true,
                    }),
                    upper: None,
                }),
                order: schema.physical_order.clone(),
            },
            after: None,
            limit: 100,
            facets: vec![FacetRequest {
                field_id: FieldId::new(0),
                limit: 10,
            }],
            aggregates: vec![
                AggregateRequest {
                    field_id: FieldId::new(0),
                    operation: AggregateOperation::Count,
                },
                AggregateRequest {
                    field_id: FieldId::new(0),
                    operation: AggregateOperation::Sum,
                },
            ],
            authorization_revision: 1,
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn exact_doc_values_facets_and_aggregates_are_merge_equivalent() {
    let schema = numeric_computation_schema();
    let mut sink = SharedSink::default();
    let left = build_segment(
        &mut sink,
        &schema,
        40,
        vec![
            numeric_computation_source("objects/one", 1),
            numeric_computation_source("objects/two", 2),
        ],
        64 * 1024 * 1024,
    )
    .await;
    let right = build_segment(
        &mut sink,
        &schema,
        41,
        vec![
            numeric_computation_source("objects/three", 3),
            numeric_computation_source("objects/two-again", 2),
        ],
        64 * 1024 * 1024,
    )
    .await;
    let left_directory = SharedDirectory(sink.clone());
    let left = rewrite_segment_live_mask(
        &left_directory,
        &mut sink,
        &left.descriptor,
        1,
        &[DocIdRange {
            segment_id: left.descriptor.identity.segment_id,
            first_doc_id: DocId::new(0),
            count: 1,
        }],
    )
    .await
    .unwrap();
    let inputs = vec![left, right.descriptor];
    let directory = SharedDirectory(sink.clone());
    let merged = merge_segments(
        &directory,
        &schema,
        &inputs,
        SegmentIdentity::new(9, 2, schema.fingerprint().unwrap(), 42).unwrap(),
        BuildLimits::new(64 * 1024 * 1024).unwrap(),
        &mut sink,
        &MemoryScratch::default(),
        TokioExecutor,
        CompactionParallelism::new(2, 64 * 1024 * 1024).unwrap(),
        CompactionProgress::default(),
    )
    .await
    .unwrap();
    let directory = SharedDirectory(sink);
    let before = computation_page(&directory, &schema, inputs).await;
    let after = computation_page(&directory, &schema, vec![merged.descriptor]).await;
    assert_eq!(
        before
            .hits
            .iter()
            .map(|hit| hit.result.path.as_str())
            .collect::<Vec<_>>(),
        after
            .hits
            .iter()
            .map(|hit| hit.result.path.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(before.facet_results, after.facet_results);
    assert_eq!(before.aggregate_results, after.aggregate_results);
    assert_eq!(before.hits.len(), 3);
    assert!(
        before
            .hits
            .iter()
            .all(|hit| hit.result.path != "objects/two")
    );
}

fn doc_value_schema() -> Schema {
    Schema {
        kind: IndexKind::TypedJson,
        path_prefix: "objects/".into(),
        content_type_scope: Some("application/json".into()),
        fields: vec![FieldSchema {
            id: FieldId::new(0),
            name: "sortable".into(),
            source_selector: "/sortable".into(),
            field_type: FieldType::Keyword,
            cardinality: Cardinality::Single,
            allow_missing: true,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::ORDER,
            analyzer: None,
            components: FieldComponents::DOC_VALUES,
        }],
        semantics: IndexSemantics::TypedJson,
        physical_order: Vec::new(),
        component_versions: versions(&[
            ComponentKind::ROUTING_NODE,
            ComponentKind::IDENTITY_TABLE,
            ComponentKind::LIVE_MASK,
            ComponentKind::PATH_LOCATOR,
            ComponentKind::DOC_VALUES,
            ComponentKind::SCORING_STATISTICS,
        ]),
    }
}

#[test]
fn merge_plan_charges_schema_retained_statistics_and_descriptor_vectors() {
    let schema = doc_value_schema();
    let shape = schema.segment_shape().unwrap();
    let streams = shape.component_count * std::mem::size_of::<SegmentStream>();
    let fields_and_statistics = shape.field_count * std::mem::size_of::<FieldStatistics>()
        + shape.component_statistics_count * std::mem::size_of::<ComponentStatistics>();
    let descriptors = shape.component_count * std::mem::size_of::<SegmentComponent>();
    assert_eq!(
        merge_schema_workspace_bytes(&schema).unwrap(),
        streams + fields_and_statistics.max(descriptors)
    );
}

#[tokio::test]
async fn merged_output_may_exceed_the_complete_resident_budget() {
    let schema = doc_value_schema();
    let payload = "x".repeat(24 * 1024);
    let sources = (0..1_600)
        .map(|ordinal| ProjectedSource {
            source_identity: ObjectIdentity {
                path: format!("objects/{ordinal:04}"),
                version: 1,
            },
            records: vec![ProjectedRecord {
                result_identity: None,
                order_key: Vec::new(),
                terms: Vec::new(),
                points: Vec::new(),
                doc_values: vec![ProjectedDocValue {
                    field_id: FieldId::new(0),
                    multi_valued: false,
                    cell: DocValueCell::value(ScalarValue::String(format!(
                        "{ordinal:04}-{payload}"
                    ))),
                }],
                vectors: Vec::new(),
                field_lengths: Vec::new(),
            }],
        })
        .collect();
    let mut sink = SharedSink::default();
    let input = build_segment(&mut sink, &schema, 20, sources, 512 * 1024 * 1024).await;
    let directory = SharedDirectory(sink.clone());
    let budget = 32 * 1024 * 1024;
    let merged = merge_segments(
        &directory,
        &schema,
        &[input.descriptor],
        SegmentIdentity::new(9, 2, schema.fingerprint().unwrap(), 21).unwrap(),
        BuildLimits::new(budget).unwrap(),
        &mut sink,
        &MemoryScratch::default(),
        TokioExecutor,
        CompactionParallelism::serial(),
        CompactionProgress::default(),
    )
    .await
    .unwrap();
    assert!(merged.descriptor.logical_bytes > budget as u64);
    assert_eq!(merged.descriptor.document_count, 1_600);
    let reader = SegmentComponentReader::new(&directory, &merged.descriptor).unwrap();
    let blocks = reader
        .doc_value_blocks(FieldId::new(0), None, None)
        .await
        .unwrap();
    assert_eq!(
        blocks
            .iter()
            .map(|block| block.cells().len())
            .sum::<usize>(),
        1_600
    );
}
