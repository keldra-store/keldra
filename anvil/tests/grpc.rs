#![recursion_limit = "256"]

use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    self, CreateBucketRequest, GetObjectRequest, ListObjectsRequest, NativeMutationContext,
    ObjectMetadata, PutObjectRequest,
};
use futures_util::StreamExt;
use std::time::Duration;
use tonic::Code;

use anvil_test_utils::*;

async fn count_docker_corestore_shard_files(cluster: &DockerTestCluster) -> usize {
    let mut count = 0_usize;
    for node in 1..=6 {
        let output = cluster
            .exec_node_output(
                node,
                &[
                    "sh",
                    "-lc",
                    "find /var/lib/anvil/corestore/blocks/local-cache -name 'shard-*.anb' 2>/dev/null | wc -l",
                ],
            )
            .await;
        assert!(
            output.status.success(),
            "failed to count Docker shard files on node {node}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        count += String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<usize>()
            .unwrap_or(0);
    }
    count
}

async fn count_docker_corestore_object_manifest_sidecars(cluster: &DockerTestCluster) -> usize {
    let mut count = 0_usize;
    for node in 1..=6 {
        let output = cluster
            .exec_node_output(
                node,
                &[
                    "sh",
                    "-lc",
                    "find /var/lib/anvil/corestore/meta/replicas -type d -name manifests 2>/dev/null | wc -l",
                ],
            )
            .await;
        assert!(
            output.status.success(),
            "failed to count Docker manifest sidecars on node {node}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        count += String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<usize>()
            .unwrap_or(0);
    }
    count
}

fn native_mutation_context(
    actor: &DockerTestStorageActor,
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

#[tokio::test]
async fn test_distributed_put_and_get() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "grpc-distributed").await;
    let token = actor.token.clone();
    let client_addr = actor.grpc_addr.clone();

    let mut bucket_client = BucketServiceClient::connect(client_addr.clone())
        .await
        .unwrap();
    let bucket_name = unique_test_name("dist-bucket");
    let mut create_bucket_req = tonic::Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),
        options: None,
    });
    create_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let mut object_client = ObjectServiceClient::connect(client_addr).await.unwrap();
    let object_key = "my-distributed-object".to_string();
    let data = (0..1024 * 128).map(|i| (i % 256) as u8).collect::<Vec<_>>();

    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "object-metadata",
        )),
        content_type: None,
        user_metadata_json: String::new(),
        storage_class: None,
    };
    let mut chunks = vec![PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Metadata(metadata)),
    }];
    for chunk in data.chunks(1024 * 64) {
        chunks.push(PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Chunk(chunk.to_vec())),
        });
    }

    let request_stream = tokio_stream::iter(chunks);
    let mut put_object_req = tonic::Request::new(request_stream);
    put_object_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );

    let response = object_client
        .put_object(put_object_req)
        .await
        .unwrap()
        .into_inner();
    let object_hash = response.etag;

    let get_request = GetObjectRequest {
        bucket_name,
        object_key,
        version_id: Some(response.version_id),
        range: None,
        ..Default::default()
    };
    let mut get_object_req = tonic::Request::new(get_request);
    get_object_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut response_stream = object_client
        .get_object(get_object_req)
        .await
        .unwrap()
        .into_inner();

    let mut downloaded_data = Vec::new();
    if let Some(Ok(first_chunk)) = response_stream.next().await {
        if let Some(anvil_api::get_object_response::Data::Metadata(_)) = first_chunk.data {
            while let Some(Ok(chunk)) = response_stream.next().await {
                if let Some(anvil_api::get_object_response::Data::Chunk(bytes)) = chunk.data {
                    downloaded_data.extend_from_slice(&bytes);
                }
            }
        }
    }

    assert_eq!(downloaded_data, data);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let shards_found = count_docker_corestore_shard_files(&cluster).await;
    let manifest_sidecars = count_docker_corestore_object_manifest_sidecars(&cluster).await;
    assert!(
        shards_found >= 6,
        "expected CoreStore erasure shards for {object_hash} in Docker cluster"
    );
    assert_eq!(
        manifest_sidecars, 0,
        "CoreStore object manifests must be reconstructed from shard placement, not final sidecar JSON"
    );
}

fn legacy_native_mutation_context(bucket_id: i64, tag: &str) -> NativeMutationContext {
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
        write_visibility: None,
    }
}

// Internal-only until the Docker harness has a one-node topology profile.
#[tokio::test]
async fn test_single_node_put() {
    // Keep isolated: this covers the one-node tiny-object/inline write path;
    // the shared default cluster is intentionally a six-node topology.
    let mut cluster = isolated_test_cluster(
        "verifies native object writes on a single-node topology",
        &["test-region-1"],
    )
    .await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;

    let token = cluster.token.clone();
    let client_addr = cluster.grpc_addrs[0].clone();

    let mut bucket_client = BucketServiceClient::connect(client_addr.clone())
        .await
        .unwrap();
    let bucket_name = unique_test_name("single-bucket");
    let mut create_bucket_req = tonic::Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    create_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let mut object_client = ObjectServiceClient::connect(client_addr).await.unwrap();
    let object_key = "single-node-object".to_string();
    let data = b"hello world".to_vec();

    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(legacy_native_mutation_context(bucket_id, "object-metadata")),
        content_type: None,
        user_metadata_json: String::new(),
        storage_class: None,
    };
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Metadata(metadata)),
        },
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Chunk(data.clone())),
        },
    ];

    let request_stream = tokio_stream::iter(chunks);

    let mut put_object_req = tonic::Request::new(request_stream);
    put_object_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );

    let result = object_client.put_object(put_object_req).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_multi_region_list_and_isolation() {
    // Keep isolated: this test intentionally compares two independent regional
    // clusters and asserts the west cluster has no east-cluster bucket state.
    let mut cluster_east = isolated_test_cluster(
        "compares two independent one-node regional clusters",
        &["us-east-1"],
    )
    .await;
    cluster_east
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;

    let mut cluster_west = isolated_test_cluster(
        "compares two independent one-node regional clusters",
        &["eu-west-1"],
    )
    .await;
    cluster_west
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;

    let token = cluster_east.token.clone();
    let east_client_addr = cluster_east.grpc_addrs[0].clone();
    let west_client_addr = cluster_west.grpc_addrs[0].clone();

    let mut bucket_client_east = BucketServiceClient::connect(east_client_addr.clone())
        .await
        .unwrap();
    let mut object_client_east = ObjectServiceClient::connect(east_client_addr)
        .await
        .unwrap();
    let mut object_client_west = ObjectServiceClient::connect(west_client_addr)
        .await
        .unwrap();

    let bucket_name = unique_test_name("regional-bucket");
    let mut create_bucket_req = tonic::Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "us-east-1".to_string(),

        options: None,
    });
    create_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client_east
        .create_bucket(create_bucket_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let object_key = "regional-object".to_string();
    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(legacy_native_mutation_context(bucket_id, "object-metadata")),
        content_type: None,
        user_metadata_json: String::new(),
        storage_class: None,
    };
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Metadata(metadata)),
        },
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Chunk(
                b"regional data".to_vec(),
            )),
        },
    ];
    let mut put_object_req = tonic::Request::new(tokio_stream::iter(chunks));
    put_object_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client_east.put_object(put_object_req).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let list_req_east = ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        ..Default::default()
    };
    let mut list_req_east_auth = tonic::Request::new(list_req_east);
    list_req_east_auth.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_resp_east = object_client_east
        .list_objects(list_req_east_auth)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list_resp_east.objects.len(), 1);
    assert_eq!(list_resp_east.objects[0].key, object_key);

    let list_req_west = ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        ..Default::default()
    };
    let mut list_req_west_auth = tonic::Request::new(list_req_west);
    list_req_west_auth.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_resp_west = object_client_west.list_objects(list_req_west_auth).await;

    assert!(list_resp_west.is_err());
    assert_eq!(list_resp_west.unwrap_err().code(), Code::NotFound);
}
