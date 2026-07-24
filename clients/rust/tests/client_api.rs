use anvil_storage::{
    AnvilClient, BearerInterceptor, BeginTransaction, CommitTransaction, GetTransaction,
    MutationBatch, RollbackTransaction, bearer_metadata, proto, write_options_with_transaction,
};
use tonic::Request;
use tonic::service::Interceptor;
use tonic::transport::Endpoint;

#[test]
fn bearer_metadata_uses_authorization_header_value() {
    let value = bearer_metadata("token-123").expect("bearer metadata should parse");
    assert_eq!(value.to_str().unwrap(), "Bearer token-123");
    assert!(value.is_sensitive());
}

#[test]
fn bearer_interceptor_inserts_authorization_metadata() {
    let mut interceptor = BearerInterceptor::new("token-123").expect("interceptor should build");
    let request = interceptor
        .call(Request::new(()))
        .expect("request should pass");
    assert_eq!(
        request
            .metadata()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap(),
        "Bearer token-123"
    );
}

#[test]
fn bearer_debug_output_redacts_token() {
    let interceptor = BearerInterceptor::new("token-123").expect("interceptor should build");
    let rendered = format!("{interceptor:?}");
    assert!(rendered.contains("<redacted>"));
    assert!(!rendered.contains("token-123"));
    assert!(!rendered.contains("Bearer token"));
}

#[test]
fn generated_proto_exports_core_service_types() {
    let request = proto::CreateBucketRequest {
        bucket_name: "documents".to_string(),
        region: "eu-west-1".to_string(),
        options: None,
    };
    assert_eq!(request.bucket_name, "documents");

    let scope = proto::AuthzScope {
        anvil_storage_tenant_id: "storage".to_string(),
        authz_realm_id: "realm".to_string(),
    };
    let schema_ref = proto::AuthzSchemaRef {
        schema_id: "default".to_string(),
        schema_revision: 1,
        schema_digest: "digest".to_string(),
    };
    let bind = proto::BindAuthzSchemaRequest {
        scope: Some(scope),
        schema_ref: Some(schema_ref),
        expected_binding_generation: Some(1),
        reason: "test".to_string(),
    };
    assert_eq!(bind.expected_binding_generation, Some(1));

    let member = proto::AuthzRelationSchema {
        relation: "viewer".to_string(),
        rules: Vec::new(),
        member_kind: proto::AuthzSchemaMemberKind::DirectRelation as i32,
        allowed_subjects: vec![proto::AuthzAllowedSubject {
            selector_kind: proto::AuthzSubjectSelectorKind::AnyCanonicalId as i32,
            subject_kind: "user".to_string(),
            subject_id: String::new(),
        }],
    };
    assert_eq!(member.allowed_subjects.len(), 1);

    let batch = proto::WriteAuthzTuplesRequest {
        mutations: vec![proto::AuthzTupleMutation {
            namespace: "document".to_string(),
            object_id: "doc-1".to_string(),
            relation: "viewer".to_string(),
            subject_kind: "user".to_string(),
            subject_id: "alice".to_string(),
            caveat_hash: String::new(),
            operation: "add".to_string(),
            reason: "grant access".to_string(),
            scope: None,
        }],
        scope: None,
        operation_id: Some("grant-doc-1".to_string()),
        expected_revision: Some(41),
    };
    assert_eq!(batch.operation_id.as_deref(), Some("grant-doc-1"));
    assert_eq!(batch.expected_revision, Some(41));

    let start = proto::StartSagaRequest {
        idempotency_key: "saga-idem".to_string(),
        realm_id: "realm".to_string(),
        draft_ttl_ms: 30_000,
        purpose: "client-test".to_string(),
        execution_policy: None,
    };
    assert_eq!(start.purpose, "client-test");

    let append_record = proto::AppendStreamRecordInfo {
        authenticated_principal: "tenant/7/principal/app-1".to_string(),
        ..Default::default()
    };
    assert_eq!(
        append_record.authenticated_principal,
        "tenant/7/principal/app-1"
    );
}

#[test]
fn transaction_helpers_construct_proto_requests() {
    let scope = proto::TransactionScope {
        root_anchor_key: "tenant/root".to_string(),
        root_key_hash: "hash-123".to_string(),
    };
    let precondition = proto::WritePrecondition {
        object_versions: vec![proto::ObjectVersionPrecondition {
            bucket_name: "documents".to_string(),
            object_key: "invoice.json".to_string(),
            expected_version_id: Some("v1".to_string()),
            must_not_exist: false,
        }],
        lease_fence: None,
    };
    let boundary = proto::BoundaryValue {
        name: "tenant".to_string(),
        value: "acme".to_string(),
    };

    let begin = proto::BeginTransactionRequest::from(
        BeginTransaction::new("begin-idem", scope.clone(), 30_000, "client-test")
            .with_preconditions([precondition.clone()])
            .with_boundary_values([boundary.clone()]),
    );
    assert_eq!(begin.idempotency_key, "begin-idem");
    assert_eq!(begin.scope, Some(scope));
    assert_eq!(begin.preconditions, vec![precondition.clone()]);
    assert_eq!(begin.boundary_values, vec![boundary]);
    assert_eq!(begin.ttl_ms, 30_000);
    assert_eq!(begin.purpose, "client-test");

    let commit = proto::CommitTransactionRequest::from(
        CommitTransaction::new("tx-123", proto::ConsistencyMode::Finalised)
            .wait_for_finalization(true)
            .with_final_preconditions([precondition]),
    );
    assert_eq!(commit.transaction_id, "tx-123");
    assert_eq!(commit.consistency, proto::ConsistencyMode::Finalised as i32);
    assert!(commit.wait_for_finalization);
    assert_eq!(commit.final_preconditions.len(), 1);

    let rollback =
        proto::RollbackTransactionRequest::from(RollbackTransaction::new("tx-123", "aborted"));
    assert_eq!(rollback.transaction_id, "tx-123");
    assert_eq!(rollback.reason, "aborted");

    let get = proto::GetTransactionRequest::from(GetTransaction::new("tx-123"));
    assert_eq!(get.transaction_id, "tx-123");
}

#[test]
fn mutation_batch_helper_wraps_typed_operations() {
    let context = proto::NativeMutationContext {
        tenant_id: 7,
        bucket_id: 11,
        principal: "app/client".to_string(),
        request_id: "request-1".to_string(),
        precondition: "none".to_string(),
        authz_zookie_optional: String::new(),
        idempotency_key: "batch-idem".to_string(),
        transaction_id: Some("tx-123".to_string()),
        saga_operation: None,
        saga_compensation_operation: None,
        write_visibility: None,
    };
    let precondition = proto::WritePrecondition {
        object_versions: Vec::new(),
        lease_fence: Some(proto::LeaseFencePrecondition {
            task_id: "task-1".to_string(),
            fence_token: 42,
        }),
    };
    let put = proto::MutationBatchPutObject {
        object_key: "payload.json".to_string(),
        payload: br#"{"ready":true}"#.to_vec(),
        content_type: Some("application/json".to_string()),
        user_metadata_json: "{}".to_string(),
        storage_class: None,
    };
    let delete = proto::MutationBatchDeleteObject {
        object_key: "old.json".to_string(),
        version_id: None,
    };

    let request = proto::MutationBatchRequest::from(
        MutationBatch::new("documents", context.clone())
            .with_precondition(precondition.clone())
            .push_operation(MutationBatch::put_object(put.clone()))
            .push_operation(MutationBatch::delete_object(delete.clone())),
    );
    assert_eq!(request.bucket_name, "documents");
    assert_eq!(request.mutation_context, Some(context));
    assert_eq!(request.precondition, Some(precondition));
    assert_eq!(request.operations.len(), 2);

    match request.operations[0].op.as_ref() {
        Some(proto::mutation_batch_operation::Op::PutObject(actual)) => {
            assert_eq!(actual, &put);
        }
        _ => panic!("expected put-object batch operation"),
    }
    match request.operations[1].op.as_ref() {
        Some(proto::mutation_batch_operation::Op::DeleteObject(actual)) => {
            assert_eq!(actual, &delete);
        }
        _ => panic!("expected delete-object batch operation"),
    }
}

#[test]
fn write_options_helper_uses_execution_oneof() {
    let options = write_options_with_transaction(proto::WriteOptions::default(), "tx-123");

    match options.execution.as_ref() {
        Some(proto::write_options::Execution::TransactionId(transaction_id)) => {
            assert_eq!(transaction_id, "tx-123");
        }
        other => panic!("expected transaction execution context, got {other:?}"),
    }
}

#[test]
fn bounded_collection_page_types_are_public() {
    let request = proto::ListBucketsRequest {
        page: Some(proto::PageRequest {
            page_size: 64,
            page_token: "next".into(),
        }),
    };
    let response = proto::ListBucketsResponse {
        buckets: Vec::new(),
        page: Some(proto::PageResponse {
            next_page_token: "after".into(),
        }),
    };

    assert_eq!(request.page.unwrap().page_size, 64);
    assert_eq!(response.page.unwrap().next_page_token, "after");
}

#[test]
fn packaged_proto_omits_internal_node_service() {
    let packaged_proto = include_str!("../proto/anvil.proto");
    assert!(packaged_proto.contains("optional string operation_id = 3;"));
    assert!(packaged_proto.contains("optional uint64 expected_revision = 4;"));
    assert!(!packaged_proto.contains("InternalAnvilService"));
    assert!(!packaged_proto.contains("InternalProxyService"));
    assert!(!packaged_proto.contains("PutShardRequest"));
    assert!(!packaged_proto.contains("CommitShardRequest"));
    assert!(!packaged_proto.contains("GetShardRequest"));
    assert!(!packaged_proto.contains("DeleteShardRequest"));
    assert!(!packaged_proto.contains("InternalRequestHeader"));
}

#[tokio::test]
async fn client_constructs_all_public_service_clients_from_channel() {
    let channel = Endpoint::from_static("http://127.0.0.1:50051").connect_lazy();
    let client = AnvilClient::from_channel_with_bearer(channel, "token-123").unwrap();
    let rendered = format!("{client:?}");
    assert!(rendered.contains("<redacted>"));
    assert!(!rendered.contains("token-123"));

    let _auth = client.auth();
    let _buckets = client.buckets();
    let _objects = client.objects();
    let _indexes = client.indexes();
    let _git = client.git_sources();
    let _personaldb = client.personaldb();
    let _repair = client.repair();
    let _keys = client.hugging_face_keys();
    let _hf = client.hf_ingestion();
    let _models = client.models();
    let _transactions = client.transactions();
    let _sagas = client.sagas();
}
