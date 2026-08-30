use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use keldra_api::v1::index_field::FieldType as ApiFieldType;
use keldra_api::v1::index_specification::Specification;
use keldra_api::v1::{
    GitSourceIndexSpec, IndexField, IndexFieldCapability, IndexFieldCardinality,
    IndexSpecification, KeywordIndexField, TensorIndexSpec, TypedJsonIndexSpec,
};
use keldra_index::v4::build::{
    BuildLimits, ExactMemorySink, NativeSegmentWriter, PublishedObject, SourcePush,
};
use keldra_index::v4::{
    ArtifactDirectoryRead, ArtifactPackReference, CandidateGate, CandidateGateEvidence,
    CandidateReference, NativeQuery, NativeQueryExecutor, NativeQueryLimits, NativeQueryRequest,
    Predicate, PredicateId, ScalarValue, Schema, SegmentIdentity,
};
use keldra_index::{FIXED_INDEX_SEAL_WORKSPACE_BYTES, IndexError, IndexFileRead};

use super::*;
use crate::index_runtime::v4_schema::compile_schema;

const TEST_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const AUTHORIZATION_REVISION: u64 = 7;

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
            || object.bytes.len() as u64 != pack.object_length
            || *blake3::hash(&object.bytes).as_bytes() != pack.object_content_hash
        {
            return Err(IndexError::Integrity);
        }
        Ok(MemoryFile(Arc::from(object.bytes.as_slice())))
    }
}

struct AllowAll;

impl CandidateGate for AllowAll {
    type Error = IndexError;

    async fn evaluate(
        &self,
        candidates: &[CandidateReference],
    ) -> Result<CandidateGateEvidence, Self::Error> {
        Ok(CandidateGateEvidence {
            visible: vec![true; candidates.len()],
            authorization_revision: AUTHORIZATION_REVISION,
            denied: 0,
            stale: 0,
        })
    }
}

fn schema(specification: Specification) -> Schema {
    compile_schema(
        "records",
        Some("application/json"),
        &IndexSpecification {
            specification: Some(specification),
        },
    )
    .unwrap()
}

fn project(schema: &Schema, body: &[u8]) -> ProjectedSource {
    let source = IndexBuildObject {
        path: "records/source.json".into(),
        version: 7,
        content_type: Some("application/json".into()),
        content_hash: [0xab; 32],
        content_length: body.len() as u64,
        committed_at_unix_millis: 19,
    };
    let mut input = Cursor::new(body);
    let (mutation, _) = project_mutation(
        schema,
        IndexSourceMutation::Upsert(source),
        Some(&mut input),
        TEST_MEMORY_BYTES,
    )
    .unwrap();
    let MergeMutation::Upsert(source) = mutation else {
        panic!("expected projected source")
    };
    source
}

async fn execute(schema: Schema, source: ProjectedSource, query: NativeQuery) -> Vec<String> {
    execute_with_query_schema(schema.clone(), schema, source, query).await
}

async fn execute_with_query_schema(
    build_schema: Schema,
    query_schema: Schema,
    source: ProjectedSource,
    query: NativeQuery,
) -> Vec<String> {
    let identity = SegmentIdentity::new(71, 1, build_schema.fingerprint().unwrap(), 91).unwrap();
    let limits = BuildLimits::with_resident_limits(
        TEST_MEMORY_BYTES,
        TEST_MEMORY_BYTES - FIXED_INDEX_SEAL_WORKSPACE_BYTES,
        FIXED_INDEX_SEAL_WORKSPACE_BYTES,
    )
    .unwrap();
    let mut writer = NativeSegmentWriter::new(identity, build_schema, limits).unwrap();
    assert_eq!(writer.push_source(source).unwrap(), SourcePush::Accepted);
    let mut sink = ExactMemorySink::new();
    let segment = writer.seal(&mut sink).await.unwrap().descriptor;
    let directory = MemoryArtifacts(sink.objects().clone());
    let page = NativeQueryExecutor::new(&directory, &AllowAll, NativeQueryLimits::default())
        .unwrap()
        .execute(&NativeQueryRequest {
            schema: query_schema,
            segments: vec![segment],
            query,
            after: None,
            limit: 10,
            facets: Vec::new(),
            aggregates: Vec::new(),
            authorization_revision: AUTHORIZATION_REVISION,
        })
        .await
        .unwrap();
    page.hits.into_iter().map(|hit| hit.result.path).collect()
}

fn keyword_field(name: &str, pointer: &str) -> IndexField {
    IndexField {
        name: name.into(),
        json_pointer: pointer.into(),
        cardinality: IndexFieldCardinality::Single as i32,
        capabilities: vec![IndexFieldCapability::Exact as i32],
        field_type: Some(ApiFieldType::Keyword(KeywordIndexField {})),
    }
}

#[tokio::test]
async fn renamed_and_reordered_logical_fields_query_one_shared_segment() {
    let first = schema(Specification::TypedJson(TypedJsonIndexSpec {
        fields: vec![
            keyword_field("state", "/state"),
            keyword_field("ecosystem", "/ecosystem"),
        ],
        physical_order: Vec::new(),
    }));
    let second = schema(Specification::TypedJson(TypedJsonIndexSpec {
        fields: vec![
            keyword_field("package_ecosystem", "/ecosystem"),
            keyword_field("advisory_state", "/state"),
        ],
        physical_order: Vec::new(),
    }));
    assert_eq!(first.fingerprint().unwrap(), second.fingerprint().unwrap());
    let state = second
        .fields
        .iter()
        .find(|field| field.name == "advisory_state")
        .unwrap()
        .id;
    let source = project(&first, br#"{"state":"active","ecosystem":"cargo"}"#);

    assert_eq!(
        execute_with_query_schema(
            first,
            second,
            source,
            NativeQuery::Filter {
                predicate: Some(Predicate::Equal {
                    id: PredicateId::new(0),
                    field_id: state,
                    value: ScalarValue::String("active".into()),
                }),
                order: Vec::new(),
            },
        )
        .await,
        ["records/source.json"]
    );
}

#[tokio::test]
async fn real_git_schema_projects_object_id_and_executes_a_nonempty_query() {
    let schema = schema(Specification::GitSource(GitSourceIndexSpec {
        repository_id: "repo".into(),
    }));
    let source = project(
        &schema,
        br#"{"repository_id":"repo","commit_id":"abc","tree_path":"src/lib.rs","object_id":"def","pack_path":"packs/one.pack","pack_version":4,"offset":12,"length":90}"#,
    );
    assert_eq!(source.records[0].terms.len(), 8);
    assert_eq!(source.records[0].doc_values.len(), 1);
    assert_eq!(
        execute(
            schema,
            source,
            NativeQuery::GitSource {
                repository_id: "repo".into(),
                commit_id: "abc".into(),
                tree_path: "src/".into(),
                prefix: true,
            },
        )
        .await,
        ["packs/one.pack"]
    );
}

#[tokio::test]
async fn real_tensor_schema_executes_a_nonempty_query() {
    let schema = schema(Specification::Tensor(TensorIndexSpec {
        model_id: "model".into(),
    }));
    let source = project(
        &schema,
        br#"{"model_id":"model","tensor_name":"layer.weight","source_path":"weights/model.bin","source_version":9,"offset":16,"length":128,"dtype":"f32","shape":[4,8]}"#,
    );
    assert_eq!(source.records[0].doc_values.len(), 1);
    assert_eq!(
        execute(
            schema,
            source,
            NativeQuery::Tensor {
                model_id: "model".into(),
                tensor_name: "layer.weight".into(),
            },
        )
        .await,
        ["weights/model.bin"]
    );
}
