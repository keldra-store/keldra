#![recursion_limit = "256"]

use anvil_storage::AnvilClient;
use anvil_storage::proto::get_object_response;
use anvil_storage::proto::put_object_request;
use anvil_storage::proto::{
    CreateBucketRequest, GetObjectRequest, ListBucketsRequest, NativeMutationContext,
    ObjectMetadata, PutObjectRequest,
};
use anvil_test_utils::{
    create_docker_storage_test_actor, shared_docker_test_cluster, unique_test_name,
};

fn native_mutation_context(
    tenant_id: i64,
    principal: &str,
    bucket_id: i64,
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

#[tokio::test]
async fn rust_client_calls_live_native_api() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "rust-client").await;

    let client = AnvilClient::connect_with_bearer(actor.grpc_addr.clone(), &actor.token)
        .await
        .expect("rust client should connect to live test node");

    let bucket_name = unique_test_name("rust-client");
    let bucket = client
        .buckets()
        .create_bucket(CreateBucketRequest {
            bucket_name: bucket_name.clone(),
            region: "test-region-1".to_string(),

            options: None,
        })
        .await
        .expect("rust client should create a bucket")
        .into_inner();

    let listed = client
        .buckets()
        .list_buckets(ListBucketsRequest { page: None })
        .await
        .expect("rust client should list buckets")
        .into_inner();
    assert!(
        listed
            .buckets
            .iter()
            .any(|bucket| bucket.name == bucket_name)
    );

    let object_key = "notes/welcome.txt".to_string();
    let payload = b"hello from the Rust client".to_vec();
    let upload = vec![
        PutObjectRequest {
            data: Some(put_object_request::Data::Metadata(ObjectMetadata {
                bucket_name: bucket_name.clone(),
                object_key: object_key.clone(),
                mutation_context: Some(native_mutation_context(
                    actor.tenant_id,
                    &actor.app_id,
                    bucket.bucket_id,
                    "put-object",
                )),
                content_type: None,
                user_metadata_json: String::new(),
                storage_class: None,
            })),
        },
        PutObjectRequest {
            data: Some(put_object_request::Data::Chunk(payload.clone())),
        },
    ];

    let stored = client
        .objects()
        .put_object(tokio_stream::iter(upload))
        .await
        .expect("rust client should stream an object upload")
        .into_inner();
    assert!(!stored.version_id.is_empty());

    let mut download = client
        .objects()
        .get_object(GetObjectRequest {
            bucket_name,
            object_key,
            version_id: Some(stored.version_id),
            range: None,

            ..Default::default()
        })
        .await
        .expect("rust client should stream an object download")
        .into_inner();

    let mut downloaded = Vec::new();
    while let Some(part) = download
        .message()
        .await
        .expect("download stream item should decode")
    {
        if let Some(get_object_response::Data::Chunk(bytes)) = part.data {
            downloaded.extend(bytes);
        }
    }

    assert_eq!(downloaded, payload);
}
