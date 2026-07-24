#![recursion_limit = "512"]

use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    CreateHostAliasRequest, CreateIndexRequest, CreateObjectLinkRequest, GrantAccessRequest,
    IndexKind, ObjectLinkResolution, PublicMutationContext, QueryIndexRequest,
    SetPublicAccessRequest, VerifyHostAliasRequest,
};
use anvil::formats::vector::VECTOR_INDEX_SCHEMA;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier,
    VersioningConfiguration,
};
use rand::random;
use std::env::temp_dir;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tonic::Request;

use anvil_test_utils::*;

fn tenant_routed_public_url(http_base: &str, tenant: &str, bucket: &str, key: &str) -> String {
    format!("{http_base}/{tenant}/{bucket}/{key}")
}

fn docker_actor_tenant_route(actor: &DockerTestStorageActor) -> &str {
    actor
        .tenant_name
        .as_deref()
        .expect("Docker test actor should retain its routeable tenant name")
}

fn assert_reserved_namespace_error(error: impl std::fmt::Debug) {
    let rendered = format!("{error:?}");
    assert!(
        rendered.contains("UnauthorizedReservedNamespace"),
        "expected UnauthorizedReservedNamespace error, got {rendered}"
    );
}

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

fn public_mutation_context(tag: &str, expected_generation: u64) -> PublicMutationContext {
    PublicMutationContext {
        request_id: format!("{tag}-{}", uuid::Uuid::new_v4()),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        expected_generation,
        transaction_id: None,
        saga_operation: None,
        saga_compensation_operation: None,
    }
}

fn rfc_vector_policy(
    extractor_kind: &str,
    provider: &str,
    model: &str,
    dimension: u16,
) -> serde_json::Value {
    serde_json::json!({
        "schema": VECTOR_INDEX_SCHEMA,
        "source": {"kind": "object_current"},
        "extractor": {"kind": extractor_kind},
        "embedding": {
            "provider": provider,
            "model": model,
            "dimension": dimension,
            "modality": "text",
            "normalisation": "unit_l2",
            "chunking": {"strategy": "whole_object"}
        },
        "ann": {
            "algorithm": "hnsw",
            "metric": "cosine"
        }
    })
}

const LARGE_OBJECT_RANGE_SPLIT_BYTES: usize = 1024 * 1024;

fn run_large_s3_gateway_test(future: Pin<Box<dyn Future<Output = ()> + Send>>) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("build S3 gateway test runtime");
    runtime.block_on(async move {
        tokio::spawn(future)
            .await
            .expect("S3 gateway test task should not panic");
    });
}

async fn create_app(cluster: &TestCluster, app_name: &str) -> (String, String) {
    let (_, client_id, client_secret) = create_app_with_id(cluster, app_name).await;
    (client_id, client_secret)
}

async fn create_app_with_id(cluster: &TestCluster, app_name: &str) -> (String, String, String) {
    cluster
        .create_application_with_id("default", app_name)
        .await
}

async fn create_docker_app(cluster: &DockerTestCluster, label: &str) -> DockerTestStorageActor {
    create_docker_storage_test_actor(cluster, label).await
}

fn s3_client_for_docker_app(cluster: &DockerTestCluster, actor: &DockerTestStorageActor) -> Client {
    cluster.s3_client(actor)
}

async fn set_bucket_public_for_docker_app(actor: &DockerTestStorageActor, bucket: &str) {
    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut public_req = tonic::Request::new(SetPublicAccessRequest {
        bucket: bucket.to_string(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", actor.token).parse().unwrap(),
    );
    auth_client.set_public_access(public_req).await.unwrap();
}

async fn grant_storage_tenant_owner_for_test(cluster: &TestCluster, app_name: &str) {
    let app = cluster.states[0]
        .persistence
        .get_app_by_tenant_name(1, app_name)
        .await
        .unwrap()
        .expect("test app should exist before granting storage tenant owner");
    anvil::access_control::grant_storage_tenant_owner(
        &cluster.states[0].persistence,
        1,
        &app.id.to_string(),
        "test",
        "grant S3 test app storage tenant ownership",
    )
    .await
    .unwrap();
}

fn s3_client(http_base: &str, client_id: &str, client_secret: &str) -> Client {
    let credentials = aws_sdk_s3::config::Credentials::new(
        client_id,
        client_secret,
        None, // session token
        None, // expiry
        "static",
    );

    let config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(aws_sdk_s3::config::Region::new("test-region"))
        .endpoint_url(http_base)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

#[path = "s3_gateway_tests/bucket_listing.rs"]
mod bucket_listing;
#[path = "s3_gateway_tests/public_private_large_object.rs"]
mod public_private_large_object;
#[path = "s3_gateway_tests/routing_public_alias.rs"]
mod routing_public_alias;
#[path = "s3_gateway_tests/streaming_upload.rs"]
mod streaming_upload;
#[path = "s3_gateway_tests/writes_indexes_compaction.rs"]
mod writes_indexes_compaction;
