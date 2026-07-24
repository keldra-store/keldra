#![recursion_limit = "256"]

use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    AbortMultipartRequest, CreateBucketRequest, DeleteBucketRequest, GetBucketPolicyRequest,
    InitiateMultipartRequest, ListBucketsRequest, NativeMutationContext, ObjectMetadata,
    PageRequest, PutBucketPolicyRequest, PutObjectRequest, WatchBucketMetadataRequest,
};
use anvil::tasks::TaskStatus;
use futures_util::StreamExt;
use std::time::{Duration, Instant};
use tonic::Request;

use anvil_test_utils::*;

type BucketTestActor = DockerTestStorageActor;

async fn create_bucket_test_actor(cluster: &DockerTestCluster, label: &str) -> BucketTestActor {
    create_docker_storage_test_actor(cluster, label).await
}

fn native_mutation_context(
    actor: &BucketTestActor,
    bucket_id: i64,
    tag: &str,
) -> NativeMutationContext {
    let nonce = uuid::Uuid::new_v4();
    NativeMutationContext {
        tenant_id: actor.tenant_id,
        bucket_id,
        principal: actor.app_id.clone(),
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

fn authenticated<T>(request: Request<T>, token: &str) -> Request<T> {
    authenticated_request(request, token)
}

async fn create_bucket(
    client: &mut BucketServiceClient<tonic::transport::Channel>,
    actor: &BucketTestActor,
    bucket_name: &str,
) -> Result<i64, tonic::Status> {
    client
        .create_bucket(authenticated(
            Request::new(CreateBucketRequest {
                bucket_name: bucket_name.to_string(),
                region: actor.region.clone(),
                options: None,
            }),
            &actor.token,
        ))
        .await
        .map(|response| response.into_inner().bucket_id)
}

async fn list_contains_bucket(
    client: &mut BucketServiceClient<tonic::transport::Channel>,
    actor: &BucketTestActor,
    bucket_name: &str,
) -> bool {
    let list_res = client
        .list_buckets(authenticated(
            Request::new(ListBucketsRequest { page: None }),
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();

    list_res
        .buckets
        .iter()
        .any(|bucket| bucket.name == bucket_name)
}

async fn wait_until_bucket_name_can_be_recreated(
    client: &mut BucketServiceClient<tonic::transport::Channel>,
    actor: &BucketTestActor,
    bucket_name: &str,
) -> i64 {
    let start = Instant::now();
    loop {
        match create_bucket(client, actor, bucket_name).await {
            Ok(bucket_id) => return bucket_id,
            Err(status) if status.code() == tonic::Code::AlreadyExists => {
                assert!(
                    start.elapsed() < Duration::from_secs(30),
                    "bucket name {bucket_name} was not reclaimed before timeout"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(status) => panic!("unexpected recreate failure for {bucket_name}: {status:?}"),
        }
    }
}

#[tokio::test]
async fn concurrent_bucket_creates_allocate_unique_ids() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_bucket_test_actor(&cluster, "concurrent-bucket-ids").await;

    let creates = (0..4).map(|ordinal| {
        let actor = actor.clone();
        async move {
            let mut client = BucketServiceClient::connect(actor.grpc_addr.clone())
                .await
                .unwrap();
            let name = unique_test_name(&format!("concurrent-bucket-{ordinal}"));
            let id = create_bucket(&mut client, &actor, &name).await.unwrap();
            (name, id)
        }
    });
    let created = futures_util::future::join_all(creates).await;
    let ids = created
        .iter()
        .map(|(_, id)| *id)
        .collect::<std::collections::HashSet<_>>();

    assert_eq!(
        ids.len(),
        created.len(),
        "bucket IDs must be globally unique"
    );

    let mut client = BucketServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let listed = client
        .list_buckets(authenticated(
            Request::new(ListBucketsRequest { page: None }),
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();
    for (name, id) in created {
        let bucket = listed
            .buckets
            .iter()
            .find(|bucket| bucket.name == name)
            .expect("concurrently created bucket must be listable");
        assert_eq!(bucket.bucket_id, id);
    }
}

#[tokio::test]
async fn test_task_claim_marks_tasks_running_before_execution() {
    let cluster = isolated_test_cluster(
        "asserts exact task claim state in a fresh persistence queue",
        &["test-region-1"],
    )
    .await;
    let persistence = &cluster.states[0].persistence;

    persistence
        .enqueue_task(
            anvil::tasks::TaskType::DeleteBucket,
            serde_json::json!({ "bucket_id": 123_i64 }),
            100,
        )
        .await
        .unwrap();

    let claimed = persistence.claim_pending_tasks(10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let task_id = claimed[0].id;

    let claimed_again = persistence.claim_pending_tasks(10).await.unwrap();
    assert!(
        claimed_again.is_empty(),
        "running tasks must not be claimed again"
    );

    let tasks = persistence
        .list_tasks_page(None, 1_000)
        .await
        .unwrap()
        .tasks;
    let task = tasks.iter().find(|task| task.id == task_id).unwrap();
    assert_eq!(task.status, TaskStatus::Running);
}

#[tokio::test]
async fn test_delete_bucket_soft_deletes_and_reclaims_name() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_bucket_test_actor(&cluster, "delete-bucket").await;

    let grpc_addr = actor.grpc_addr.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("delete-bucket");
    create_bucket(&mut bucket_client, &actor, &bucket_name)
        .await
        .unwrap();

    // 1. Verify it exists
    assert!(list_contains_bucket(&mut bucket_client, &actor, &bucket_name).await);

    // 2. Delete the bucket
    bucket_client
        .delete_bucket(authenticated(
            Request::new(DeleteBucketRequest {
                bucket_name: bucket_name.clone(),
                options: None,
            }),
            &actor.token,
        ))
        .await
        .unwrap();

    // 3. Verify it is gone from listings (soft deleted)
    assert!(
        !list_contains_bucket(&mut bucket_client, &actor, &bucket_name).await,
        "soft-deleted bucket should not be listed"
    );

    // 4. Verify the distributed/public effect: the background deletion path
    // eventually reclaims the name and a client can create the bucket again.
    wait_until_bucket_name_can_be_recreated(&mut bucket_client, &actor, &bucket_name).await;
}

#[tokio::test]
async fn test_delete_bucket_rejects_retained_objects() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_bucket_test_actor(&cluster, "delete-retained").await;

    let grpc_addr = actor.grpc_addr.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("delete-nonempty");
    let object_key = unique_test_name("retained-object");
    let bucket_id = create_bucket(&mut bucket_client, &actor, &bucket_name)
        .await
        .unwrap();

    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key,
                    mutation_context: Some(native_mutation_context(
                        &actor,
                        bucket_id,
                        "object-metadata",
                    )),
                    content_type: None,
                    user_metadata_json: String::new(),
                    storage_class: None,
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"retained".to_vec(),
            )),
        },
    ];
    object_client
        .put_object(authenticated(
            Request::new(tokio_stream::iter(chunks)),
            &actor.token,
        ))
        .await
        .unwrap();

    let err = bucket_client
        .delete_bucket(authenticated(
            Request::new(DeleteBucketRequest {
                bucket_name: bucket_name.clone(),
                options: None,
            }),
            &actor.token,
        ))
        .await
        .expect_err("retained object versions should keep bucket non-empty");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("Bucket not empty"));
}

#[tokio::test]
async fn test_delete_bucket_rejects_active_multipart_uploads() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_bucket_test_actor(&cluster, "delete-mpu").await;

    let grpc_addr = actor.grpc_addr.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("delete-mpu");
    let object_key = unique_test_name("multipart-object");
    let bucket_id = create_bucket(&mut bucket_client, &actor, &bucket_name)
        .await
        .unwrap();

    let upload_id = object_client
        .initiate_multipart_upload(authenticated(
            Request::new(InitiateMultipartRequest {
                bucket_name: bucket_name.clone(),
                object_key: object_key.clone(),
                mutation_context: Some(native_mutation_context(
                    &actor,
                    bucket_id,
                    "initiate-multipart",
                )),
            }),
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .upload_id;

    let err = bucket_client
        .delete_bucket(authenticated(
            Request::new(DeleteBucketRequest {
                bucket_name: bucket_name.clone(),
                options: None,
            }),
            &actor.token,
        ))
        .await
        .expect_err("active multipart upload should keep bucket non-empty");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("Bucket not empty"));

    object_client
        .abort_multipart_upload(authenticated(
            Request::new(AbortMultipartRequest {
                bucket_name: bucket_name.clone(),
                object_key,
                upload_id,
                mutation_context: Some(native_mutation_context(
                    &actor,
                    bucket_id,
                    "abort-multipart",
                )),
            }),
            &actor.token,
        ))
        .await
        .unwrap();

    bucket_client
        .delete_bucket(authenticated(
            Request::new(DeleteBucketRequest {
                bucket_name,
                options: None,
            }),
            &actor.token,
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_list_buckets() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_bucket_test_actor(&cluster, "list-buckets").await;

    let grpc_addr = actor.grpc_addr.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name1 = unique_test_name("list-bucket-a");
    let bucket_name2 = unique_test_name("list-bucket-b");

    create_bucket(&mut bucket_client, &actor, &bucket_name1)
        .await
        .unwrap();
    create_bucket(&mut bucket_client, &actor, &bucket_name2)
        .await
        .unwrap();

    let list_res = bucket_client
        .list_buckets(authenticated(
            Request::new(ListBucketsRequest { page: None }),
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert!(list_res.buckets.iter().any(|b| b.name == bucket_name1));
    assert!(list_res.buckets.iter().any(|b| b.name == bucket_name2));

    let first_page = bucket_client
        .list_buckets(authenticated(
            Request::new(ListBucketsRequest {
                page: Some(PageRequest {
                    page_size: 1,
                    page_token: String::new(),
                }),
            }),
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first_page.buckets.len(), 1);
    let next_page_token = first_page
        .page
        .expect("first page metadata")
        .next_page_token;
    assert!(!next_page_token.is_empty());

    let second_page = bucket_client
        .list_buckets(authenticated(
            Request::new(ListBucketsRequest {
                page: Some(PageRequest {
                    page_size: 1,
                    page_token: next_page_token,
                }),
            }),
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second_page.buckets.len(), 1);
    assert_ne!(first_page.buckets[0].name, second_page.buckets[0].name);
    assert!(
        second_page
            .page
            .expect("second page metadata")
            .next_page_token
            .is_empty()
    );
}

#[tokio::test]
async fn test_get_bucket_policy_reflects_public_read_flag() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_bucket_test_actor(&cluster, "bucket-policy").await;

    let grpc_addr = actor.grpc_addr.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("policy-bucket");
    create_bucket(&mut bucket_client, &actor, &bucket_name)
        .await
        .unwrap();

    let policy = bucket_client
        .get_bucket_policy(authenticated(
            Request::new(GetBucketPolicyRequest {
                bucket_name: bucket_name.clone(),
            }),
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .policy_json;
    let policy: serde_json::Value = serde_json::from_str(&policy).unwrap();
    assert_eq!(policy["is_public_read"], false);

    bucket_client
        .put_bucket_policy(authenticated(
            Request::new(PutBucketPolicyRequest {
                bucket_name: bucket_name.clone(),
                policy_json: serde_json::json!({"is_public_read": true}).to_string(),
                options: None,
            }),
            &actor.token,
        ))
        .await
        .unwrap();

    let policy = bucket_client
        .get_bucket_policy(authenticated(
            Request::new(GetBucketPolicyRequest { bucket_name }),
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .policy_json;
    let policy: serde_json::Value = serde_json::from_str(&policy).unwrap();
    assert_eq!(policy["is_public_read"], true);
}

#[tokio::test]
async fn test_watch_bucket_metadata_streams_snapshot_events() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_bucket_test_actor(&cluster, "bucket-watch").await;

    let grpc_addr = actor.grpc_addr.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("watch-bucket");
    create_bucket(&mut bucket_client, &actor, &bucket_name)
        .await
        .unwrap();

    bucket_client
        .put_bucket_policy(authenticated(
            Request::new(PutBucketPolicyRequest {
                bucket_name: bucket_name.clone(),
                policy_json: serde_json::json!({"is_public_read": true}).to_string(),
                options: None,
            }),
            &actor.token,
        ))
        .await
        .unwrap();

    bucket_client
        .delete_bucket(authenticated(
            Request::new(DeleteBucketRequest {
                bucket_name: bucket_name.clone(),
                options: None,
            }),
            &actor.token,
        ))
        .await
        .unwrap();

    let mut watch = bucket_client
        .watch_bucket_metadata(authenticated(
            Request::new(WatchBucketMetadataRequest {
                bucket_name: bucket_name.clone(),
                after_cursor: 0,
            }),
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();

    let mut events = Vec::new();
    for _ in 0..3 {
        events.push(watch.next().await.unwrap().unwrap());
    }

    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["create", "policy_update", "delete"]
    );
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].cursor < pair[1].cursor)
    );
    assert_eq!(events[0].bucket.as_ref().unwrap().name, bucket_name);
    assert!(events[1].bucket.as_ref().unwrap().is_public_read);
    assert!(events[2].bucket.as_ref().unwrap().deleted);
    for event in &events {
        let envelope = event.envelope.as_ref().expect("bucket metadata envelope");
        assert_eq!(envelope.watch_stream_id, "bucket_metadata");
        assert_eq!(envelope.partition_family, "bucket_metadata");
        assert_eq!(envelope.cursor_low, event.cursor);
        assert_eq!(envelope.record_kind, "bucket_metadata");
        assert_eq!(envelope.object_ref, bucket_name);
        assert!(!envelope.mutation_id.is_empty());
        assert!(!envelope.payload_hash.is_empty());
    }
}
