#![recursion_limit = "256"]

use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::repair_service_client::RepairServiceClient;
use anvil::anvil_api::{
    self, AppendStreamRecordRequest, CreateAppendStreamRequest, CreateBucketRequest,
    CreateIndexRequest, DisableIndexRequest, DropIndexRequest, GetAccessTokenRequest, IndexKind,
    ListIndexDiagnosticsRequest, ListIndexesRequest, ListRepairFindingsRequest,
    NativeMutationContext, ObjectMetadata, PutObjectRequest, QueryIndexRequest, QueryIndexResponse,
    QuerySpecRequest, RepairIndexRequest, UpdateIndexRequest, WatchIndexDefinitionRequest,
    WatchIndexPartitionRequest, WriteAuthzTupleRequest, WriteVisibilityOptions,
};
use anvil::authz_scope::{DEFAULT_AUTHZ_REALM_ID, encode_realm_namespace};
use anvil::authz_userset_index::{DEFAULT_DERIVED_USERSET_INDEX_ID, read_derived_userset_index};
use anvil::core_store::{AuthzScopeRef as CoreAuthzScopeRef, SourceId, SourceKind};
use anvil::formats::full_text::{FullTextDocument, build_full_text_postings};
use anvil::formats::vector::{
    VECTOR_INDEX_SCHEMA, VectorMetric, VectorModality, VectorPayload, VectorRecord,
};
use anvil::full_text_segment::{
    FullTextDocumentTableRow, FullTextSegmentWrite, encode_full_text_document_table,
    write_full_text_segment,
};
use anvil::partition_fence::{
    AcquireOwnership, MAX_OWNERSHIP_LEASE_MS, OwnershipPrincipal, OwnershipResource,
    OwnershipResourceKind, acquire_ownership,
};
use anvil::search_query::{FullTextSegmentQuery, query_full_text_segment};
use anvil::typed_field_segment::source_id_binary;
use anvil::vector_segment::{VectorSegmentEntry, VectorSegmentWrite, write_vector_segment};
use anvil_test_utils::*;
use futures_util::StreamExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use tonic::Request;

const INDEX_EVENTUAL_CONSISTENCY_TIMEOUT: Duration = Duration::from_secs(60);

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

async fn get_app_token(grpc_addr: &str, client_id: &str, client_secret: &str) -> String {
    let mut auth_client = AuthServiceClient::connect(grpc_addr.to_string())
        .await
        .unwrap();
    auth_client
        .get_access_token(GetAccessTokenRequest {
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
        })
        .await
        .unwrap()
        .into_inner()
        .access_token
}

fn native_mutation_context(bucket_id: i64, tag: &str) -> NativeMutationContext {
    let nonce = uuid::Uuid::new_v4();
    NativeMutationContext {
        tenant_id: 1,
        bucket_id,
        principal: "2".to_string(),
        request_id: format!("{tag}-{nonce}-request"),
        precondition: "none".to_string(),
        authz_zookie_optional: String::new(),
        idempotency_key: format!("{tag}-{nonce}-idempotency"),
        transaction_id: None,
        saga_operation: None,
        saga_compensation_operation: None,
        write_visibility: Some(strict_write_visibility()),
    }
}

fn strict_write_visibility() -> WriteVisibilityOptions {
    WriteVisibilityOptions {
        indexes: 1,
        watches: 1,
        authz_materialization: 1,
        boundary_extraction: 1,
        index_policy_snapshot: 1,
        authz_revision: 1,
    }
}

fn test_full_text_document_table(rows: &[(u64, u16, &str, uuid::Uuid)]) -> Vec<u8> {
    let rows = rows
        .iter()
        .map(
            |(document_id, field_id, object_key, version_id)| FullTextDocumentTableRow {
                document_id: *document_id,
                field_id: *field_id,
                object_key: (*object_key).to_string(),
                version_id: *version_id,
            },
        )
        .collect::<Vec<_>>();
    encode_full_text_document_table(&rows).expect("test full-text document table should encode")
}

fn rfc_vector_policy(
    extractor_kind: &str,
    provider: &str,
    model: impl Into<String>,
    dimension: u16,
    modality: &str,
    metric: &str,
) -> serde_json::Value {
    serde_json::json!({
        "schema": VECTOR_INDEX_SCHEMA,
        "source": {"kind": "object_current"},
        "extractor": {"kind": extractor_kind},
        "embedding": {
            "provider": provider,
            "model": model.into(),
            "dimension": dimension,
            "modality": modality,
            "normalisation": "unit_l2",
            "chunking": {"strategy": "whole_object"}
        },
        "ann": {
            "algorithm": "hnsw",
            "metric": metric
        }
    })
}

fn index_build_payload_i64(task: &anvil::persistence::TaskRecord, field: &str) -> Option<i64> {
    task.payload.get(field)?.as_i64()
}

fn scoped_index_build_task(
    task: &anvil::persistence::TaskRecord,
    tenant_id: i64,
    bucket_id: i64,
    index_ids: &[i64],
) -> bool {
    task.task_type == anvil::tasks::TaskType::IndexBuild
        && index_build_payload_i64(task, "tenant_id") == Some(tenant_id)
        && index_build_payload_i64(task, "bucket_id") == Some(bucket_id)
        && index_build_payload_i64(task, "index_id").is_some_and(|id| index_ids.contains(&id))
}

async fn wait_for_index_builds_for_indexes(
    cluster: &TestCluster,
    timeout: Duration,
    tenant_id: i64,
    bucket_id: i64,
    index_ids: &[i64],
) -> Vec<anvil::persistence::TaskRecord> {
    assert!(
        !index_ids.is_empty(),
        "test must wait for at least one specific index"
    );
    let wait_start = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempts = 0_u64;
    let mut matching_tasks = Vec::new();
    while tokio::time::Instant::now() < deadline {
        attempts += 1;
        matching_tasks = cluster.states[0]
            .persistence
            .list_tasks_page(None, 1_000)
            .await
            .unwrap()
            .tasks
            .into_iter()
            .filter(|task| scoped_index_build_task(task, tenant_id, bucket_id, index_ids))
            .collect();
        let completed_index_ids = matching_tasks
            .iter()
            .filter_map(|task| {
                (task.status == anvil::tasks::TaskStatus::Completed)
                    .then(|| index_build_payload_i64(task, "index_id"))
                    .flatten()
            })
            .collect::<BTreeSet<_>>();
        if index_ids.iter().all(|id| completed_index_ids.contains(id)) {
            emit_test_timing(
                format!(
                    "wait_for_index_builds_for_indexes bucket={bucket_id} indexes={} attempts={attempts}",
                    index_ids.len()
                ),
                wait_start.elapsed(),
            );
            return matching_tasks;
        }
        assert!(
            !matching_tasks
                .iter()
                .any(|task| task.status == anvil::tasks::TaskStatus::Failed),
            "scoped index build task failed for bucket_id={bucket_id} index_ids={index_ids:?}; tasks={matching_tasks:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    emit_test_timing(
        format!(
            "wait_for_index_builds_for_indexes timeout bucket={bucket_id} indexes={} attempts={attempts}",
            index_ids.len()
        ),
        wait_start.elapsed(),
    );
    panic!(
        "timed out waiting for scoped index build tasks bucket_id={bucket_id} index_ids={index_ids:?}; tasks={matching_tasks:?}"
    );
}

async fn persist_index_object(
    cluster: &TestCluster,
    bucket_name: &str,
    key: &str,
    user_meta: Option<serde_json::Value>,
) {
    let payload = format!("payload for {key}").into_bytes();
    put_index_object_bytes(
        cluster,
        1,
        bucket_name,
        key,
        Some("text/plain"),
        user_meta,
        &payload,
    )
    .await;
}

async fn put_index_object_bytes(
    cluster: &TestCluster,
    tenant_id: i64,
    bucket_name: &str,
    key: &str,
    content_type: Option<&str>,
    user_meta: Option<serde_json::Value>,
    body: &[u8],
) -> anvil::persistence::Object {
    let claims = if cluster.token.is_empty() {
        let test_app = cluster.states[0]
            .persistence
            .get_app_by_client_id("test-app")
            .await
            .expect("test app lookup should succeed")
            .expect("test-app is seeded for index tests");
        let token = cluster.states[0]
            .jwt_manager
            .mint_token(test_app.id.to_string(), test_app.tenant_id)
            .expect("test app token should mint");
        cluster.states[0]
            .jwt_manager
            .verify_token(&token)
            .expect("minted test cluster token should decode")
    } else {
        cluster.states[0]
            .jwt_manager
            .verify_token(&cluster.token)
            .expect("test cluster token should decode")
    };
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(tenant_id, bucket_name)
        .await
        .expect("test bucket lookup should succeed")
        .expect("test bucket should exist before object write");
    let has_bucket_write = anvil::access_control::system_realm_relationship_allows(
        &cluster.states[0].storage,
        &claims,
        anvil::system_realm::SYSTEM_BUCKET_NAMESPACE,
        &anvil::access_control::bucket_object_id(&bucket),
        "put_object",
        None,
    )
    .await
    .expect("test bucket authorisation lookup should succeed");
    if !has_bucket_write {
        anvil::access_control::grant_bucket_defaults(
            &cluster.states[0].persistence,
            &bucket,
            &claims.sub,
            "index-test",
            "grant direct test-created bucket defaults",
        )
        .await
        .expect("direct test-created bucket should receive normal default authz tuples");
    }
    cluster.states[0]
        .object_manager
        .put_object(
            &claims,
            bucket_name,
            key,
            futures_util::stream::iter(vec![Ok(body.to_vec())]),
            anvil::object_manager::ObjectWriteOptions {
                content_type: content_type.map(ToOwned::to_owned),
                user_metadata: user_meta,
                visibility: anvil::object_manager::ObjectWriteVisibility::strict(),
                ..Default::default()
            },
        )
        .await
        .expect("persist index test object through final CoreStore object path")
}

async fn put_json_object(
    object_client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_id: i64,
    bucket_name: &str,
    key: &str,
    value: serde_json::Value,
    tag: &str,
) {
    let metadata = PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Metadata(
            ObjectMetadata {
                bucket_name: bucket_name.to_string(),
                object_key: key.to_string(),
                mutation_context: Some(native_mutation_context(bucket_id, tag)),
                content_type: Some("application/json".to_string()),
                user_metadata_json: String::new(),
                storage_class: None,
            },
        )),
    };
    let chunk = PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Chunk(
            serde_json::to_vec(&value).unwrap(),
        )),
    };
    object_client
        .put_object(authorized(tokio_stream::iter(vec![metadata, chunk]), token))
        .await
        .unwrap();
}

async fn query_index_until_hits(
    index_client: &mut IndexServiceClient<tonic::transport::Channel>,
    token: &str,
    request: QueryIndexRequest,
    expected_hits: usize,
    timeout: Duration,
) -> QueryIndexResponse {
    let wait_start = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempts = 0_u64;
    let mut last = None;
    let mut last_failed_precondition = None;
    while tokio::time::Instant::now() < deadline {
        attempts += 1;
        let response = match index_client
            .query_index(authorized(request.clone(), token))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                last_failed_precondition = Some(status.message().to_string());
                tokio::time::sleep(Duration::from_millis(150)).await;
                continue;
            }
            Err(status) => panic!("query failed while waiting for index: {status:?}"),
        };
        if response.hits.len() == expected_hits && response.is_caught_up {
            emit_test_timing(
                format!("query_index_until_hits expected={expected_hits} attempts={attempts}"),
                wait_start.elapsed(),
            );
            return response;
        }
        last = Some(response);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    emit_test_timing(
        format!("query_index_until_hits timeout expected={expected_hits} attempts={attempts}"),
        wait_start.elapsed(),
    );
    last.unwrap_or_else(|| {
        panic!(
            "query did not execute before timeout; last_failed_precondition={last_failed_precondition:?}"
        )
    })
}

async fn query_spec_until_hits(
    index_client: &mut IndexServiceClient<tonic::transport::Channel>,
    token: &str,
    request: QuerySpecRequest,
    expected_hits: usize,
    timeout: Duration,
) -> anvil_api::QuerySpecResponse {
    let wait_start = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempts = 0_u64;
    let mut last = None;
    while tokio::time::Instant::now() < deadline {
        attempts += 1;
        let response = match index_client
            .query_spec(authorized(request.clone(), token))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                tokio::time::sleep(Duration::from_millis(150)).await;
                continue;
            }
            Err(status) => panic!("query spec failed while waiting for indexes: {status:?}"),
        };
        let Some(result) = response.result.as_ref() else {
            panic!("query spec response omitted result");
        };
        if result.hits.len() == expected_hits && result.is_caught_up {
            emit_test_timing(
                format!("query_spec_until_hits expected={expected_hits} attempts={attempts}"),
                wait_start.elapsed(),
            );
            return response;
        }
        last = Some(response);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    emit_test_timing(
        format!("query_spec_until_hits timeout expected={expected_hits} attempts={attempts}"),
        wait_start.elapsed(),
    );
    last.unwrap_or_else(|| panic!("query spec did not execute before timeout"))
}

async fn write_authz_tuple(
    auth_client: &mut AuthServiceClient<tonic::transport::Channel>,
    token: &str,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
) {
    auth_client
        .write_authz_tuple(authorized(
            WriteAuthzTupleRequest {
                namespace: namespace.to_string(),
                object_id: object_id.to_string(),
                relation: relation.to_string(),
                subject_kind: subject_kind.to_string(),
                subject_id: subject_id.to_string(),
                caveat_hash: String::new(),
                operation: "add".to_string(),
                reason: "query spec authz regression".to_string(),
                scope: None,
            },
            token,
        ))
        .await
        .unwrap();
}

async fn grant_bucket_index_query_for_principal(
    cluster: &TestCluster,
    bucket_name: &str,
    principal_id: &str,
) {
    anvil::access_control::write_delegated_action_tuple(
        &cluster.states[0].storage,
        &cluster.states[0].persistence,
        1,
        principal_id,
        anvil::permissions::AnvilAction::IndexRead,
        bucket_name,
        "add",
        "index-test",
        "grant test principal bucket index query access",
    )
    .await
    .expect("test principal should receive index query access");
}

async fn grant_tenant_object_reader_for_principal(
    cluster: &TestCluster,
    bucket_name: &str,
    object_key: &str,
    principal_id: &str,
) {
    cluster.states[0]
        .persistence
        .write_authz_tuple(
            1,
            &encode_realm_namespace(DEFAULT_AUTHZ_REALM_ID, "object"),
            &format!("{bucket_name}/{object_key}"),
            "reader",
            anvil::access_control::APP_SUBJECT_KIND,
            principal_id,
            "",
            "add",
            "index-test",
            "grant test principal object reader access",
        )
        .await
        .expect("test principal should receive object reader access");
}

async fn wait_for_derived_authz_entry(
    cluster: &TestCluster,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        let maybe_index = read_derived_userset_index(
            &cluster.states[0].storage,
            1,
            DEFAULT_DERIVED_USERSET_INDEX_ID,
        )
        .await
        .unwrap();
        if maybe_index.is_some_and(|index| {
            index.entries.iter().any(|entry| {
                entry.namespace == namespace
                    && entry.object_id == object_id
                    && entry.relation == relation
                    && entry.subject_kind == subject_kind
                    && entry.subject_id == subject_id
                    && entry.caveat_hash.is_empty()
            })
        }) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    panic!("derived authz userset entry was not indexed before timeout");
}

#[path = "index_tests/build_repair.rs"]
mod build_repair;
#[path = "index_tests/list_indexes.rs"]
mod list_indexes;
#[path = "index_tests/query_spec.rs"]
mod query_spec;
#[path = "index_tests/typed_lifecycle.rs"]
mod typed_lifecycle;
#[path = "index_tests/validation_diagnostics.rs"]
mod validation_diagnostics;
#[path = "index_tests/vector_hybrid.rs"]
mod vector_hybrid;

async fn wait_for_vector_hit(
    index_client: &mut IndexServiceClient<tonic::transport::Channel>,
    bucket_name: &str,
    index_name: &str,
    object_key: &str,
    query_vector: Vec<f32>,
    token: &str,
) -> anvil::anvil_api::QueryIndexResponse {
    let wait_start = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut attempts = 0_u64;
    let mut last = None;
    let mut last_failed_precondition = None;

    while tokio::time::Instant::now() < deadline {
        attempts += 1;
        let response = match index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket_name.to_string(),
                    index_name: index_name.to_string(),
                    query_text: String::new(),
                    query_vector: query_vector.clone(),
                    limit: 10,
                    phrase: false,
                    path_prefix: String::new(),
                    metadata_filters_json: String::new(),
                    boundary_predicates_json: String::new(),
                    typed_predicates_json: String::new(),
                    typed_order_json: String::new(),
                    page_token: String::new(),
                    require_caught_up_to_watch_cursor: String::new(),
                    lag_timeout_ms: 0,
                },
                token,
            ))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                last_failed_precondition = Some(status.message().to_string());
                tokio::time::sleep(Duration::from_millis(150)).await;
                continue;
            }
            Err(status) => panic!("vector query failed while waiting for index: {status:?}"),
        };
        if response.is_caught_up && response.hits.iter().any(|hit| hit.object_key == object_key) {
            emit_test_timing(
                format!("wait_for_vector_hit index={index_name} attempts={attempts}"),
                wait_start.elapsed(),
            );
            return response;
        }
        last = Some(response);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    panic!(
        "vector index `{index_name}` did not return `{object_key}` before timeout; last_failed_precondition={last_failed_precondition:?}; last_response={last:?}"
    );
}

async fn wait_for_index_diagnostic(
    index_client: &mut IndexServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_name: &str,
    index_name: &str,
    severity: &str,
    object_key: &str,
    code: &str,
    timeout: Duration,
) -> Vec<anvil::anvil_api::IndexDiagnosticRecord> {
    let wait_start = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempts = 0_u64;
    let mut last = Vec::new();

    while tokio::time::Instant::now() < deadline {
        attempts += 1;
        last = index_client
            .list_index_diagnostics(authorized(
                ListIndexDiagnosticsRequest {
                    bucket_name: bucket_name.to_string(),
                    index_name: index_name.to_string(),
                    severity: severity.to_string(),
                    page: Some(anvil::anvil_api::PageRequest {
                        page_size: 10,
                        page_token: String::new(),
                    }),
                },
                token,
            ))
            .await
            .unwrap()
            .into_inner()
            .diagnostics;
        if last
            .iter()
            .any(|diagnostic| diagnostic.object_key == object_key && diagnostic.code == code)
        {
            emit_test_timing(
                format!("wait_for_index_diagnostic index={index_name} attempts={attempts}"),
                wait_start.elapsed(),
            );
            return last;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    panic!(
        "index `{index_name}` did not write diagnostic {code} for {object_key} before timeout; diagnostics={last:?}"
    );
}

fn test_object_authz_label_hash(
    bucket: &anvil::persistence::Bucket,
    object: &anvil::persistence::Object,
) -> [u8; 32] {
    anvil::formats::hash32(
        format!(
            "tenant:{}:bucket:{}:object:{}:authz:{}",
            bucket.tenant_id, bucket.id, object.key, object.authz_revision
        )
        .as_bytes(),
    )
}

fn vector_entry_with_authz_label(
    bucket: &anvil::persistence::Bucket,
    object: &anvil::persistence::Object,
    vector_id: u64,
    values: Vec<f32>,
    authz_label_hash: [u8; 32],
) -> VectorSegmentEntry {
    let mut entry = vector_entry(bucket, object, vector_id, values);
    entry.record.authz_label_hash = authz_label_hash;
    entry
}

fn vector_entry(
    bucket: &anvil::persistence::Bucket,
    object: &anvil::persistence::Object,
    vector_id: u64,
    values: Vec<f32>,
) -> VectorSegmentEntry {
    let source_id = SourceId {
        schema: "anvil.query.source_id.v1".to_string(),
        mesh_id: "default".to_string(),
        anvil_storage_tenant_id: bucket.tenant_id.to_string(),
        authz_scope: CoreAuthzScopeRef {
            anvil_storage_tenant_id: bucket.tenant_id.to_string(),
            authz_realm_id: format!("tenant:{}", bucket.tenant_id),
        },
        kind: SourceKind::ObjectCurrent,
        resource_namespace: "anvil_object".to_string(),
        resource_id: format!("{}/{}/{}", bucket.tenant_id, bucket.name, object.key),
        generation: object.id.max(0) as u64,
        tombstone: object.deleted_at.is_some(),
        variant: BTreeMap::from([
            ("bucket_id".to_string(), bucket.id.to_string()),
            ("version_id".to_string(), object.version_id.to_string()),
        ]),
    };

    VectorSegmentEntry {
        source_id_binary: source_id_binary(&source_id).expect("test source id should encode"),
        source_generation: vector_id,
        labels: Vec::new(),
        record: VectorRecord {
            vector_id,
            object_version_id: *object.version_id.as_bytes(),
            chunk_id: vector_id as u32,
            modality: VectorModality::Text as u8,
            metric: VectorMetric::Cosine as u8,
            dimension: 2,
            vector_payload_offset: 0,
            source_start: vector_id * 10,
            source_len: 10,
            authz_label_hash: [1; 32],
            metadata_filter_bits: 0,
        },
        payload: VectorPayload {
            dimension: 2,
            values,
        },
    }
}
