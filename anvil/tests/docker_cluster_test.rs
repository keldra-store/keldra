#![recursion_limit = "256"]

use std::time::Duration;

use reqwest::header::HOST;

use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::{CreateBucketRequest, SetPublicAccessRequest};
use anvil_test_utils::{
    authenticated_request, create_docker_storage_test_actor, emit_test_timing,
    shared_docker_test_cluster,
};

#[tokio::test]
async fn docker_cluster_end_to_end() {
    let start = std::time::Instant::now();
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "docker-e2e").await;
    emit_test_timing("docker_cluster_e2e shared_cluster_ready", start.elapsed());

    let mut bucket_client = BucketServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let suffix = uuid::Uuid::new_v4().to_string();
    let private_bucket = format!("e2e-private-{suffix}");
    let public_bucket = format!("e2e-public-{suffix}");

    bucket_client
        .create_bucket(authenticated_request(
            tonic::Request::new(CreateBucketRequest {
                bucket_name: private_bucket.clone(),
                region: actor.region.clone(),
                options: None,
            }),
            &actor.token,
        ))
        .await
        .expect("create private bucket");

    bucket_client
        .create_bucket(authenticated_request(
            tonic::Request::new(CreateBucketRequest {
                bucket_name: public_bucket.clone(),
                region: actor.region.clone(),
                options: None,
            }),
            &actor.token,
        ))
        .await
        .expect("create public bucket");

    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    auth_client
        .set_public_access(authenticated_request(
            tonic::Request::new(SetPublicAccessRequest {
            context: None,
                bucket: public_bucket.clone(),
                allow_public_read: true,
            }),
            &actor.token,
        ))
        .await
        .expect("set public access");

    // Public-access projection is watch-derived. Wait for it by exercising the
    // same external S3/public HTTP path that users rely on.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let s3 = cluster.s3_client(&actor);
    let private_key = "private.txt";
    let public_key = "public.txt";
    let private_content = b"docker private";
    let public_content = b"docker public";

    s3.put_object()
        .bucket(&private_bucket)
        .key(private_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(
            private_content.to_vec(),
        ))
        .send()
        .await
        .unwrap();
    s3.put_object()
        .bucket(&public_bucket)
        .key(public_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(
            public_content.to_vec(),
        ))
        .send()
        .await
        .unwrap();

    let resp = s3
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .send()
        .await
        .unwrap();
    let data = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), private_content);

    let tenant_name = actor
        .tenant_name
        .as_deref()
        .expect("docker storage actor has a routed tenant name");
    let public_url = format!(
        "{}/{}/{}/{}",
        actor.grpc_addr.trim_end_matches('/'),
        tenant_name,
        public_bucket,
        public_key
    );
    let client = reqwest::Client::new();
    let public_resp = client
        .get(&public_url)
        .header(HOST, &cluster.public_region_host)
        .send()
        .await
        .unwrap();
    assert_eq!(public_resp.status(), 200);
    let public_data = public_resp.bytes().await.unwrap();
    assert_eq!(public_data.as_ref(), public_content);

    let private_url = format!(
        "{}/{}/{}/{}",
        actor.grpc_addr.trim_end_matches('/'),
        tenant_name,
        private_bucket,
        private_key
    );
    let private_resp = client
        .get(&private_url)
        .header(HOST, &cluster.public_region_host)
        .send()
        .await
        .unwrap();
    assert!(private_resp.status() == 403 || private_resp.status() == 404);

    let public_list_url = format!(
        "{}/{}/{}?list-type=2",
        actor.grpc_addr.trim_end_matches('/'),
        tenant_name,
        public_bucket
    );
    let public_list_resp = client
        .get(&public_list_url)
        .header(HOST, &cluster.public_region_host)
        .send()
        .await
        .unwrap();
    assert_eq!(public_list_resp.status(), 200);
    let public_list_body = public_list_resp.text().await.unwrap();
    assert!(public_list_body.contains("<ListBucketResult"));

    let private_list_url = format!(
        "{}/{}/{}?list-type=2",
        actor.grpc_addr.trim_end_matches('/'),
        tenant_name,
        private_bucket
    );
    let private_list_resp = client
        .get(&private_list_url)
        .header(HOST, &cluster.public_region_host)
        .send()
        .await
        .unwrap();
    assert!(private_list_resp.status() == 401 || private_list_resp.status() == 403);

    let list = s3
        .list_objects_v2()
        .bucket(&private_bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(list.key_count(), Some(1));
}
