use super::*;

#[tokio::test]
async fn list_buckets_uses_bounded_continuation_pages() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_app(&cluster, "s3-list-buckets-page").await;
    let client = s3_client_for_docker_app(&cluster, &actor);
    let suffix = uuid::Uuid::new_v4();
    let bucket_names = [format!("s3-list-a-{suffix}"), format!("s3-list-b-{suffix}")];

    for bucket_name in &bucket_names {
        client
            .create_bucket()
            .bucket(bucket_name)
            .send()
            .await
            .expect("create bucket for paged S3 listing");
    }

    let first = client
        .list_buckets()
        .max_buckets(1)
        .send()
        .await
        .expect("list first S3 bucket page");
    assert_eq!(first.buckets().len(), 1);
    let continuation = first
        .continuation_token()
        .expect("first S3 bucket page continuation")
        .to_string();

    let second = client
        .list_buckets()
        .max_buckets(1)
        .continuation_token(continuation)
        .send()
        .await
        .expect("list second S3 bucket page");
    assert_eq!(second.buckets().len(), 1);
    assert_ne!(first.buckets()[0].name(), second.buckets()[0].name());
    assert!(second.continuation_token().is_none());

    for bucket_name in &bucket_names {
        client
            .delete_bucket()
            .bucket(bucket_name)
            .send()
            .await
            .expect("delete bucket from paged S3 listing");
    }
}
