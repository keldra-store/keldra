use super::*;

#[tokio::test]
async fn test_index_definition_rejects_invalid_policy_shape() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("index-validation-bucket");
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

    let unspecified_kind = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "unspecified-kind".to_string(),
                kind: IndexKind::Unspecified as i32,
                selector_json: "{}".to_string(),
                extractor_json: "{}".to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: "{}".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(unspecified_kind.code(), tonic::Code::InvalidArgument);
    assert_eq!(unspecified_kind.message(), "index kind is required");

    let invalid_kind = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "invalid-kind".to_string(),
                kind: 999,
                selector_json: "{}".to_string(),
                extractor_json: "{}".to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: "{}".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(invalid_kind.code(), tonic::Code::InvalidArgument);
    assert_eq!(invalid_kind.message(), "Invalid index kind");

    let invalid_full_text_policy = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "invalid-full-text-policy".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: "{}".to_string(),
                extractor_json: "{}".to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({"max_token_chars": 129}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(
        invalid_full_text_policy.code(),
        tonic::Code::InvalidArgument
    );

    let mut valid_vector_policy = rfc_vector_policy(
        "object_body_utf8",
        "test_only",
        "text-embedding-v1",
        768,
        "text",
        "cosine",
    );
    valid_vector_policy["embedding"]["chunking"] = serde_json::json!({
        "strategy": "token_window",
        "max_tokens": 512,
        "overlap_tokens": 64
    });
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "valid-vector".to_string(),
                kind: IndexKind::Vector as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: valid_vector_policy.to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let invalid_vector_policy = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "invalid-vector".to_string(),
                kind: IndexKind::Vector as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: rfc_vector_policy(
                    "object_body_utf8",
                    "test_only",
                    "text-embedding-v1",
                    0,
                    "text",
                    "cosine",
                )
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(invalid_vector_policy.code(), tonic::Code::InvalidArgument);

    let invalid_vector_update = index_client
        .update_index(authorized(
            UpdateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "valid-vector".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/v2/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: rfc_vector_policy(
                    "object_body_utf8",
                    "test_only",
                    "",
                    768,
                    "text",
                    "cosine",
                )
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(invalid_vector_update.code(), tonic::Code::InvalidArgument);

    let invalid_json = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name,
                name: "invalid-json".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: "{".to_string(),
                extractor_json: "{}".to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: "{}".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(invalid_json.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_list_index_diagnostics_filters_by_index_and_severity() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("index-diagnostics-bucket");
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

    let created = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "body-text".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"selector": "object_body_utf8"}).to_string(),
                extractor_json: serde_json::json!({"encoding": "utf8"}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({"require_index_success": false}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("created index");

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(claims.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("bucket exists");
    cluster.states[0]
        .persistence
        .create_index_diagnostic(
            claims.tenant_id,
            bucket.id,
            &bucket.name,
            Some(created.index_id as i64),
            "body-text",
            "docs/bad.txt",
            None,
            "warning",
            "ExtractionFailed",
            "object body was not valid UTF-8",
            serde_json::json!({"selector": "object_body_utf8"}),
        )
        .await
        .unwrap();
    cluster.states[0]
        .persistence
        .create_index_diagnostic(
            claims.tenant_id,
            bucket.id,
            &bucket.name,
            Some(created.index_id as i64),
            "body-text",
            "docs/too-large.txt",
            None,
            "error",
            "PayloadTooLarge",
            "payload exceeded extraction limit",
            serde_json::json!({"limit_bytes": 1048576}),
        )
        .await
        .unwrap();

    let warnings = index_client
        .list_index_diagnostics(authorized(
            ListIndexDiagnosticsRequest {
                bucket_name: bucket_name.clone(),
                index_name: "body-text".to_string(),
                severity: "warning".to_string(),
                page: Some(anvil::anvil_api::PageRequest {
                    page_size: 100,
                    page_token: String::new(),
                }),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .diagnostics;

    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].object_key, "docs/bad.txt");
    assert_eq!(warnings[0].code, "ExtractionFailed");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&warnings[0].details_json).unwrap()["selector"],
        "object_body_utf8"
    );

    let first = index_client
        .list_index_diagnostics(authorized(
            ListIndexDiagnosticsRequest {
                bucket_name: bucket_name.clone(),
                index_name: String::new(),
                severity: String::new(),
                page: Some(anvil::anvil_api::PageRequest {
                    page_size: 1,
                    page_token: String::new(),
                }),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.diagnostics.len(), 1);
    let next_page_token = first.page.unwrap().next_page_token;
    assert!(!next_page_token.is_empty());

    let second = index_client
        .list_index_diagnostics(authorized(
            ListIndexDiagnosticsRequest {
                bucket_name,
                index_name: String::new(),
                severity: String::new(),
                page: Some(anvil::anvil_api::PageRequest {
                    page_size: 1,
                    page_token: next_page_token,
                }),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second.diagnostics.len(), 1);
    assert!(second.page.unwrap().next_page_token.is_empty());

    let mut severities = [
        first.diagnostics[0].severity.as_str(),
        second.diagnostics[0].severity.as_str(),
    ];
    severities.sort_unstable();
    assert_eq!(severities, ["error", "warning"]);
}
