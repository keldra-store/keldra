use super::*;

fn list_request(
    bucket_name: &str,
    include_disabled: bool,
    page_size: u32,
    page_token: String,
) -> ListIndexesRequest {
    ListIndexesRequest {
        bucket_name: bucket_name.to_string(),
        include_disabled,
        page: Some(anvil_api::PageRequest {
            page_size,
            page_token,
        }),
    }
}

async fn create_full_text_index(
    client: &mut IndexServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_name: &str,
    name: &str,
) {
    client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.to_string(),
                name: name.to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"source": "object_body_utf8"}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),
                options: None,
            },
            token,
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn list_indexes_uses_revision_bound_ordered_source_pages() {
    let cluster = shared_default_test_cluster().await;
    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();
    let bucket_name = unique_test_name("list-indexes-page");
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    for name in ["charlie", "alpha", "bravo"] {
        create_full_text_index(&mut index_client, &token, &bucket_name, name).await;
    }

    let first = index_client
        .list_indexes(authorized(
            list_request(&bucket_name, true, 2, String::new()),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        first
            .indexes
            .iter()
            .map(|index| index.name.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "bravo"]
    );
    let continuation = first
        .page
        .expect("first index page must carry page metadata")
        .next_page_token;
    assert!(!continuation.is_empty());

    let second = index_client
        .list_indexes(authorized(
            list_request(&bucket_name, true, 2, continuation),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        second
            .indexes
            .iter()
            .map(|index| index.name.as_str())
            .collect::<Vec<_>>(),
        ["charlie"]
    );

    let stale_first = index_client
        .list_indexes(authorized(
            list_request(&bucket_name, true, 1, String::new()),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let stale_token = stale_first
        .page
        .expect("index page must carry page metadata")
        .next_page_token;
    index_client
        .disable_index(authorized(
            DisableIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "alpha".to_string(),
                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    let stale = index_client
        .list_indexes(authorized(
            list_request(&bucket_name, true, 1, stale_token),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), tonic::Code::InvalidArgument);

    index_client
        .drop_index(authorized(
            DropIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "bravo".to_string(),
                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    let after_drop = index_client
        .list_indexes(authorized(
            list_request(&bucket_name, true, 10, String::new()),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        after_drop
            .indexes
            .iter()
            .map(|index| index.name.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "charlie"]
    );
    assert!(!after_drop.indexes[0].enabled);

    let enabled_after_drop = index_client
        .list_indexes(authorized(
            list_request(&bucket_name, false, 10, String::new()),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        enabled_after_drop
            .indexes
            .iter()
            .map(|index| index.name.as_str())
            .collect::<Vec<_>>(),
        ["charlie"]
    );
}
