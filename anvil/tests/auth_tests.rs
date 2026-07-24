#![recursion_limit = "256"]

use anvil::anvil_api::audit_service_client::AuditServiceClient;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::coordination_service_client::CoordinationServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::repair_service_client::RepairServiceClient;
use anvil::anvil_api::{
    AcquireTaskLeaseRequest, ApplyAuthzSchemaRequest, AuthzAllowedSubject, AuthzNamespaceSchema,
    AuthzRelationRule, AuthzRelationSchema, AuthzSchemaMemberKind, AuthzScope,
    AuthzSubjectSelectorKind, AuthzTupleMutation, BindAuthzSchemaRequest, CheckPermissionRequest,
    CheckPermissionsRequest, CheckpointTaskLeaseRequest, CommitTaskLeaseRequest,
    CreateApplicationCredentialRequest, CreateBucketRequest, CreateHostAliasRequest,
    CreateObjectLinkRequest, CreateTenantRequest, DeleteApplicationCredentialRequest,
    DeleteHostAliasRequest, DeleteObjectLinkRequest, ForceReleaseTaskLeaseRequest,
    GetAccessTokenRequest, GetAuthzSchemaBindingRequest, GetAuthzSchemaRequest, GetObjectRequest,
    GrantAccessRequest, ListAccessGrantsRequest, ListApplicationsRequest, ListAuditEventsRequest,
    ListAuthzObjectsRequest, ListAuthzSubjectsRequest, ListBucketsRequest, ListHostAliasesRequest,
    ListIndexDiagnosticsRequest, ListObjectLinksRequest, ListObjectVersionsRequest,
    ListObjectsRequest, ListRepairFindingsRequest, NativeMutationContext, ObjectMetadata,
    PageRequest, PutAuthzSchemaRequest, PutObjectRequest, ReadAuthzTuplesRequest,
    ReadHostAliasRequest, ReadObjectLinkRequest, ReadTaskLeaseRequest,
    RepairAuthzDerivedIndexRequest, RepairIndexRequest, RevokeAccessRequest,
    RotateApplicationCredentialSecretRequest, SetPublicAccessRequest, UpdateObjectLinkRequest,
    VerifyHostAliasRequest, WatchAuthzDerivedLagRequest, WatchAuthzNamespaceRequest,
    WatchAuthzTupleLogRequest, WriteAuthzTupleRequest, WriteAuthzTuplesRequest,
};
use anvil::authz_derived_lag_watch::{
    AuthzDerivedLagWatchPayload, append_authz_derived_lag_watch_record,
};
use anvil::authz_namespace_watch::{
    AuthzNamespaceWatchPayload, append_authz_namespace_watch_record,
};
use anvil::authz_userset_index::DEFAULT_DERIVED_USERSET_INDEX_ID;
use anvil::core_store::{
    CF_AUTHZ, CoreMetaStore, CoreMetaTuplePart, TABLE_AUTHZ_TUPLE_PAGE_ROW, core_meta_tuple_key,
};
use futures_util::StreamExt;
use std::time::Duration;
use tonic::Request;

use anvil_test_utils::*;

fn authz_any_subject(subject_kind: &str) -> AuthzAllowedSubject {
    AuthzAllowedSubject {
        selector_kind: AuthzSubjectSelectorKind::AnyCanonicalId as i32,
        subject_kind: subject_kind.to_string(),
        subject_id: String::new(),
    }
}

fn authz_direct_relation(name: &str, subject_kinds: &[&str]) -> AuthzRelationSchema {
    AuthzRelationSchema {
        relation: name.to_string(),
        rules: Vec::new(),
        member_kind: AuthzSchemaMemberKind::DirectRelation as i32,
        allowed_subjects: subject_kinds
            .iter()
            .map(|kind| authz_any_subject(kind))
            .collect(),
    }
}

fn authz_permission(name: &str, rules: Vec<AuthzRelationRule>) -> AuthzRelationSchema {
    AuthzRelationSchema {
        relation: name.to_string(),
        rules,
        member_kind: AuthzSchemaMemberKind::Permission as i32,
        allowed_subjects: Vec::new(),
    }
}

#[tokio::test]
async fn grpc_error_responses_include_server_request_id() {
    let cluster = shared_docker_test_cluster().await;
    let grpc_addr = cluster.grpc_addr_for_test("grpc-error-request-id");

    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();
    let err = bucket_client
        .list_buckets(Request::new(ListBucketsRequest { page: None }))
        .await
        .expect_err("unauthenticated request must fail");

    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    let request_id = err
        .metadata()
        .get("x-anvil-request-id")
        .expect("gRPC error metadata must include x-anvil-request-id")
        .to_str()
        .expect("request id must be ASCII metadata");
    assert_eq!(request_id.len(), 32);
    assert!(request_id.bytes().all(|byte| byte.is_ascii_hexdigit()));
}

// Helper function to create an app through the network admin API.
async fn create_app(cluster: &TestCluster, app_name: &str) -> (String, String) {
    let (_, client_id, client_secret) = create_app_with_id(cluster, app_name).await;
    (client_id, client_secret)
}

async fn create_app_with_id(cluster: &TestCluster, app_name: &str) -> (String, String, String) {
    cluster
        .create_application_with_id("default", app_name)
        .await
}

async fn grant_policy(cluster: &TestCluster, app_name: &str, action: &str, resource: &str) {
    cluster
        .grant_application_policy("default", app_name, action, resource)
        .await;
}

async fn grant_docker_actor_policy(
    cluster: &DockerTestCluster,
    actor: &DockerTestStorageActor,
    action: &str,
    resource: &str,
) {
    cluster
        .grant_application_policy(actor.tenant_id, &actor.app_name, action, resource)
        .await;
}

async fn grant_docker_authz_realm(
    cluster: &DockerTestCluster,
    actor: &DockerTestStorageActor,
    realm_id: &str,
) {
    // Docker actors are provisioned with the complete default-realm grant set.
    // Avoid replaying those six durable mutations in every authz test.
    if realm_id == "default" {
        return;
    }
    let policies = [
        "authz:tuple_write",
        "authz:tuple_read",
        "authz:check",
        "authz:watch",
        "authz:schema_read",
        "authz:schema_write",
    ]
    .into_iter()
    .map(|action| (action.to_string(), realm_id.to_string()))
    .collect::<Vec<_>>();
    cluster
        .grant_application_policies(actor.tenant_id, &actor.app_name, &policies)
        .await;
}

fn test_default_authz_schema() -> Vec<AuthzNamespaceSchema> {
    vec![
        AuthzNamespaceSchema {
            namespace: "document".to_string(),
            relations: vec![
                authz_direct_relation("viewer", &["user", "folder", "userset"]),
                authz_direct_relation("editor", &["user"]),
            ],
            schema_json: r#"{"namespace":"document"}"#.to_string(),
            schema_hash: String::new(),
            schema_version: 0,
            authz_revision: 0,
            applied_at: String::new(),
        },
        AuthzNamespaceSchema {
            namespace: "folder".to_string(),
            relations: vec![authz_direct_relation("viewer", &["user", "userset"])],
            schema_json: r#"{"namespace":"folder"}"#.to_string(),
            schema_hash: String::new(),
            schema_version: 0,
            authz_revision: 0,
            applied_at: String::new(),
        },
        AuthzNamespaceSchema {
            namespace: "group".to_string(),
            relations: vec![authz_direct_relation("member", &["user"])],
            schema_json: r#"{"namespace":"group"}"#.to_string(),
            schema_hash: String::new(),
            schema_version: 0,
            authz_revision: 0,
            applied_at: String::new(),
        },
    ]
}

async fn prepare_docker_default_authz_realm(
    cluster: &DockerTestCluster,
    actor: &DockerTestStorageActor,
) -> u64 {
    grant_docker_authz_realm(cluster, actor, "default").await;
    bind_default_authz_schema(&actor.grpc_addr, actor.tenant_id, &actor.token).await
}

async fn bind_default_authz_schema(grpc_addr: &str, tenant_id: i64, token: &str) -> u64 {
    let mut auth_client = AuthServiceClient::connect(grpc_addr.to_string())
        .await
        .unwrap();
    let mut put = Request::new(PutAuthzSchemaRequest {
        anvil_storage_tenant_id: tenant_id.to_string(),
        schema_id: "default".to_string(),
        namespaces: test_default_authz_schema(),
        reason: "install Docker test authorization schema".to_string(),
    });
    add_bearer(&mut put, token);
    let schema_ref = auth_client
        .put_authz_schema(put)
        .await
        .unwrap()
        .into_inner()
        .schema_ref
        .expect("put default Docker test schema returns a schema reference");

    let mut bind = Request::new(BindAuthzSchemaRequest {
        scope: Some(AuthzScope {
            anvil_storage_tenant_id: tenant_id.to_string(),
            authz_realm_id: "default".to_string(),
        }),
        schema_ref: Some(schema_ref),
        expected_binding_generation: None,
        reason: "bind Docker test authorization schema".to_string(),
    });
    add_bearer(&mut bind, token);
    auth_client
        .bind_authz_schema(bind)
        .await
        .unwrap()
        .into_inner()
        .authz_revision
}

async fn get_token(grpc_addr: &str, client_id: &str, client_secret: &str) -> String {
    try_get_token(grpc_addr, client_id, client_secret)
        .await
        .unwrap()
}

async fn try_get_token(
    grpc_addr: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<String, tonic::Status> {
    let mut auth_client = AuthServiceClient::connect(grpc_addr.to_string())
        .await
        .unwrap();
    auth_client
        .get_access_token(GetAccessTokenRequest {
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
        })
        .await
        .map(|r| r.into_inner().access_token)
}

async fn default_test_app_id(cluster: &TestCluster) -> String {
    cluster.states[0]
        .persistence
        .get_app_by_client_id("test-app")
        .await
        .unwrap()
        .expect("test-app exists")
        .id
        .to_string()
}

async fn create_tenant(cluster: &TestCluster, tenant_name: &str) -> String {
    let mut admin = anvil::anvil_api::admin_service_client::AdminServiceClient::connect(
        cluster.admin_addrs[0].clone(),
    )
    .await
    .unwrap();
    let mut request = Request::new(CreateTenantRequest {
        context: Some(anvil::anvil_api::AdminRequestContext {
            request_id: format!("create-tenant-{tenant_name}-{}", uuid::Uuid::new_v4()),
            idempotency_key: uuid::Uuid::new_v4().to_string(),
            audit_reason: format!("test create tenant {tenant_name}"),
            expected_generation: 0,
        }),
        name: tenant_name.to_string(),
        home_region: "test-region-1".to_string(),
    });
    add_bearer(&mut request, &cluster.admin_token());
    admin
        .create_tenant(request)
        .await
        .unwrap()
        .into_inner()
        .tenant
        .expect("created tenant")
        .tenant_id
}

fn add_bearer<T>(request: &mut Request<T>, token: &str) {
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
}

fn native_mutation_context(
    tenant_id: i64,
    bucket_id: i64,
    principal: &str,
    tag: &str,
) -> NativeMutationContext {
    let nonce = uuid::Uuid::new_v4();
    NativeMutationContext {
        tenant_id,
        bucket_id,
        principal: principal.to_string(),
        request_id: format!("{tag}-{nonce}-request"),
        precondition: "none".to_string(),
        authz_zookie_optional: String::new(),
        idempotency_key: format!("{tag}-{nonce}-idempotency"),
        transaction_id: None,
        saga_operation: None,
        saga_compensation_operation: None,
        write_visibility: None,
    }
}

fn public_mutation_context(
    tag: &str,
    expected_generation: u64,
) -> anvil::anvil_api::PublicMutationContext {
    anvil::anvil_api::PublicMutationContext {
        request_id: format!("{tag}-{}", uuid::Uuid::new_v4()),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        expected_generation,
        transaction_id: None,
        saga_operation: None,
        saga_compensation_operation: None,
    }
}

async fn put_test_object(
    object_client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    tenant_id: i64,
    bucket_id: i64,
    principal_id: &str,
    bucket_name: &str,
    object_key: &str,
    payload: &[u8],
) {
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                    mutation_context: Some(native_mutation_context(
                        tenant_id,
                        bucket_id,
                        principal_id,
                        "put-test-object",
                    )),
                    content_type: None,
                    user_metadata_json: String::new(),
                    storage_class: None,
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                payload.to_vec(),
            )),
        },
    ];
    let mut request = Request::new(tokio_stream::iter(chunks));
    add_bearer(&mut request, token);
    object_client.put_object(request).await.unwrap();
}

fn write_authz_tuple_request(
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    operation: &str,
) -> WriteAuthzTupleRequest {
    WriteAuthzTupleRequest {
        namespace: namespace.to_string(),
        object_id: object_id.to_string(),
        relation: relation.to_string(),
        subject_kind: subject_kind.to_string(),
        subject_id: subject_id.to_string(),
        caveat_hash: String::new(),
        operation: operation.to_string(),
        reason: "test".to_string(),
        scope: None,
    }
}

fn authz_mutation(
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    operation: &str,
) -> AuthzTupleMutation {
    AuthzTupleMutation {
        namespace: namespace.to_string(),
        object_id: object_id.to_string(),
        relation: relation.to_string(),
        subject_kind: subject_kind.to_string(),
        subject_id: subject_id.to_string(),
        caveat_hash: String::new(),
        operation: operation.to_string(),
        reason: "test".to_string(),
        scope: None,
    }
}

fn check_permission_request(
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    consistency: &str,
    zookie: &str,
) -> CheckPermissionRequest {
    CheckPermissionRequest {
        namespace: namespace.to_string(),
        object_id: object_id.to_string(),
        relation: relation.to_string(),
        subject_kind: subject_kind.to_string(),
        subject_id: subject_id.to_string(),
        caveat_hash: String::new(),
        consistency: consistency.to_string(),
        zookie: zookie.to_string(),
        scope: None,
    }
}

#[path = "auth_tests/access_and_tuple.rs"]
mod access_and_tuple;
#[path = "auth_tests/leases_and_object_authz.rs"]
mod leases_and_object_authz;
#[path = "auth_tests/links_apps_and_tenant_scope.rs"]
mod links_apps_and_tenant_scope;
#[path = "auth_tests/object_lists_and_schemas.rs"]
mod object_lists_and_schemas;
#[path = "auth_tests/public_access_and_secret_reset.rs"]
mod public_access_and_secret_reset;
#[path = "auth_tests/stream_authorisation.rs"]
mod stream_authorisation;
