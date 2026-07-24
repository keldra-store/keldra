use super::*;

#[tokio::test]
async fn test_typed_json_index_queries_canonical_object_body_with_range_order_and_page_token() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("typed-json-index-bucket");
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "queue/item-a.json",
        serde_json::json!({"state": {
            "queue_name": "outbound",
            "state": "pending",
            "available_at": "2026-07-03T10:00:00Z",
            "priority": 20,
            "item_id": "item-a"
        }}),
        "typed-a",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "queue/item-b.json",
        serde_json::json!({"state": {
            "queue_name": "outbound",
            "state": "failed",
            "available_at": "2026-07-03T10:00:00Z",
            "priority": 50,
            "item_id": "item-b"
        }}),
        "typed-b",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "queue/item-c.json",
        serde_json::json!({"state": {
            "queue_name": "outbound",
            "state": "pending",
            "available_at": "2026-07-04T10:00:00Z",
            "priority": 100,
            "item_id": "item-c"
        }}),
        "typed-c",
    )
    .await;

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "due-work".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({"prefix": "queue/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [
                        {"name": "queue_name", "extractor": "/state/queue_name", "required": true},
                        {"name": "state", "extractor": "/state/state", "required": true},
                        {"name": "available_at", "extractor": "/state/available_at", "required": true},
                        {"name": "priority", "extractor": "/state/priority", "required": true},
                        {"name": "item_id", "extractor": "/state/item_id", "required": true}
                    ],
                    "default_order": [
                        {"field": "available_at", "direction": "asc"},
                        {"field": "priority", "direction": "desc"},
                        {"field": "item_id", "direction": "asc"}
                    ]
                })
                .to_string(),

                options: None,},
            &token,
        ))
        .await
        .unwrap();

    let first_page = query_index_until_hits(
        &mut index_client,
        &token,
        QueryIndexRequest {
            bucket_name: bucket_name.clone(),
            index_name: "due-work".to_string(),
            query_text: String::new(),
            query_vector: vec![],
            limit: 1,
            phrase: false,
            path_prefix: "queue/".to_string(),
            metadata_filters_json: String::new(),
            boundary_predicates_json: String::new(),
            typed_predicates_json: serde_json::json!([
                {"field": "queue_name", "op": "eq", "value": "outbound"},
                {"field": "state", "op": "in", "values": ["pending", "failed"]},
                {"field": "available_at", "op": "lte", "value": "2026-07-03T12:00:00Z"}
            ])
            .to_string(),
            typed_order_json: String::new(),
            page_token: String::new(),
            require_caught_up_to_watch_cursor: String::new(),
            lag_timeout_ms: 0,
        },
        1,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
    )
    .await;
    assert_eq!(first_page.hits.len(), 1);
    assert_eq!(first_page.hits[0].object_key, "queue/item-b.json");
    assert!(!first_page.next_page_token.is_empty());
    assert!(first_page.is_caught_up);

    let second_page = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "due-work".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: "queue/".to_string(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: serde_json::json!([
                    {"field": "queue_name", "op": "eq", "value": "outbound"},
                    {"field": "state", "op": "in", "values": ["pending", "failed"]},
                    {"field": "available_at", "op": "lte", "value": "2026-07-03T12:00:00Z"}
                ])
                .to_string(),
                typed_order_json: String::new(),
                page_token: first_page.next_page_token.clone(),
                require_caught_up_to_watch_cursor: first_page.source_watch_cursor_high.to_string(),
                lag_timeout_ms: 1000,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second_page.hits.len(), 1);
    assert_eq!(second_page.hits[0].object_key, "queue/item-a.json");

    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "queue/"},
                {"field": "queue_name", "op": "eq", "value": "outbound"},
                {"field": "state", "op": "in", "value": ["pending", "failed"]},
                {"field": "available_at", "op": "lte", "value": "2026-07-03T12:00:00Z"},
                {"can": {"relation": "read"}}
            ]
        },
        "order_by": [
            {"field": "available_at", "direction": "asc"},
            {"field": "priority", "direction": "desc"},
            {"field": "item_id", "direction": "asc"}
        ],
        "limit": 1,
        "consistency": {
            "min_source_cursor": first_page.source_watch_cursor_high.to_string(),
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();
    let spec_first = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: query_spec.clone(),
                page_token: String::new(),
                lag_timeout_ms: 1000,
                accept_degraded: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let spec_first_result = spec_first.result.expect("query spec result");
    assert_eq!(spec_first_result.hits.len(), 1);
    assert_eq!(spec_first_result.hits[0].object_key, "queue/item-b.json");
    assert!(!spec_first_result.next_page_token.is_empty());
    assert_eq!(spec_first.canonical_query_hash.len(), 64);
    let plan: serde_json::Value = serde_json::from_str(&spec_first.plan_json).unwrap();
    assert_eq!(plan["schema"], "anvil.query.plan.v1");
    assert_eq!(plan["selected_index"]["name"], "due-work");
    assert_eq!(plan["selected_index"]["kind"], "typed_json");
    assert_eq!(plan["authz_relation"], "read");
    assert_eq!(plan["degraded"], false);

    let spec_second = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: query_spec,
                page_token: spec_first_result.next_page_token,
                lag_timeout_ms: 1000,
                accept_degraded: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("query spec second result");
    assert_eq!(spec_second.hits.len(), 1);
    assert_eq!(spec_second.hits[0].object_key, "queue/item-a.json");

    let missing_can_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "anvil_storage_tenant_id": "1",
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "queue/"},
                {"field": "state", "op": "in", "value": ["pending", "failed"]}
            ]
        },
        "limit": 10
    })
    .to_string();
    let missing_can = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: missing_can_spec,
                page_token: String::new(),
                lag_timeout_ms: 0,
                accept_degraded: false,
            },
            &token,
        ))
        .await;
    assert_eq!(
        missing_can.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "protected QuerySpec must fail closed without an explicit can predicate"
    );

    let unbounded_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "anvil_storage_tenant_id": "1",
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"can": {"relation": "read"}}
            ]
        },
        "limit": 10
    })
    .to_string();
    let unbounded = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: unbounded_spec,
                page_token: String::new(),
                lag_timeout_ms: 0,
                accept_degraded: false,
            },
            &token,
        ))
        .await;
    assert_eq!(
        unbounded.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "QuerySpec without a bounded primitive predicate must not scan the bucket"
    );

    let mismatched_predicate = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "due-work".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: "queue/".to_string(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: serde_json::json!([
                    {"field": "queue_name", "op": "eq", "value": "outbound"},
                    {"field": "state", "op": "eq", "value": "pending"}
                ])
                .to_string(),
                typed_order_json: String::new(),
                page_token: first_page.next_page_token.clone(),
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await;
    assert_eq!(
        mismatched_predicate.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );

    let mut tampered_token = first_page.next_page_token.clone();
    tampered_token.push('A');
    let tampered = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name,
                index_name: "due-work".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: "queue/".to_string(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: serde_json::json!([
                    {"field": "queue_name", "op": "eq", "value": "outbound"},
                    {"field": "state", "op": "in", "values": ["pending", "failed"]},
                    {"field": "available_at", "op": "lte", "value": "2026-07-03T12:00:00Z"}
                ])
                .to_string(),
                typed_order_json: String::new(),
                page_token: tampered_token,
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await;
    assert_eq!(tampered.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_query_index_results_are_filtered_by_zanzibar_object_relationships() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("query-authz");
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "queue/visible.json",
        serde_json::json!({"state": {"queue_name": "outbound", "state": "pending", "available_at": "2026-07-03T10:00:00Z", "priority": 20, "item_id": "visible"}}),
        "query-authz-visible",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "queue/hidden.json",
        serde_json::json!({"state": {"queue_name": "outbound", "state": "pending", "available_at": "2026-07-03T10:00:00Z", "priority": 50, "item_id": "hidden"}}),
        "query-authz-hidden",
    )
    .await;

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "due-work".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({"prefix": "queue/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [
                        {"name": "queue_name", "extractor": "/state/queue_name", "required": true},
                        {"name": "state", "extractor": "/state/state", "required": true},
                        {"name": "available_at", "extractor": "/state/available_at", "required": true},
                        {"name": "priority", "extractor": "/state/priority", "required": true},
                        {"name": "item_id", "extractor": "/state/item_id", "required": true}
                    ],
                    "default_order": [
                        {"field": "available_at", "direction": "asc"},
                        {"field": "priority", "direction": "desc"},
                        {"field": "item_id", "direction": "asc"}
                    ]
                })
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let owner_query = QueryIndexRequest {
        bucket_name: bucket_name.clone(),
        index_name: "due-work".to_string(),
        query_text: String::new(),
        query_vector: vec![],
        limit: 10,
        phrase: false,
        path_prefix: "queue/".to_string(),
        metadata_filters_json: String::new(),
        boundary_predicates_json: String::new(),
        typed_predicates_json: serde_json::json!([
            {"field": "queue_name", "op": "eq", "value": "outbound"},
            {"field": "state", "op": "eq", "value": "pending"}
        ])
        .to_string(),
        typed_order_json: String::new(),
        page_token: String::new(),
        require_caught_up_to_watch_cursor: String::new(),
        lag_timeout_ms: 0,
    };
    query_index_until_hits(
        &mut index_client,
        &token,
        owner_query.clone(),
        2,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
    )
    .await;

    let reader_app_name = unique_test_name("query-reader-app");
    let (_reader_app_id, reader_client_id, reader_client_secret) = cluster
        .create_application_with_id("default", &reader_app_name)
        .await;
    cluster
        .grant_application_policy(
            "default",
            &reader_app_name,
            "index:read",
            &format!("{bucket_name}/due-work"),
        )
        .await;
    cluster
        .grant_application_policy(
            "default",
            &reader_app_name,
            "object:read",
            &format!("{bucket_name}/queue/visible.json"),
        )
        .await;
    let reader_token = get_app_token(&grpc_addr, &reader_client_id, &reader_client_secret).await;

    let reader_first = query_index_until_hits(
        &mut index_client,
        &reader_token,
        owner_query.clone(),
        1,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
    )
    .await;
    assert_eq!(
        reader_first
            .hits
            .iter()
            .map(|hit| hit.object_key.as_str())
            .collect::<Vec<_>>(),
        vec!["queue/visible.json"]
    );

    cluster
        .grant_application_policy(
            "default",
            &reader_app_name,
            "object:read",
            &format!("{bucket_name}/queue/hidden.json"),
        )
        .await;
    let reader_second = query_index_until_hits(
        &mut index_client,
        &reader_token,
        owner_query,
        2,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
    )
    .await;
    assert_eq!(
        reader_second
            .hits
            .iter()
            .map(|hit| hit.object_key.as_str())
            .collect::<Vec<_>>(),
        vec!["queue/hidden.json", "queue/visible.json"]
    );
}

#[tokio::test]
async fn test_query_spec_intersects_full_text_with_typed_filter_without_bucket_scan() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("query-spec-composite");
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "case-text".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "cases/"}).to_string(),
                extractor_json: serde_json::json!({
                    "source": "json_pointer",
                    "pointer": "/summary"
                })
                .to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "case-state".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({"prefix": "cases/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [
                        {"name": "state", "extractor": "/state", "required": true},
                        {"name": "priority", "extractor": "/priority", "required": true},
                        {"name": "case_id", "extractor": "/case_id", "required": true}
                    ],
                    "default_order": [
                        {"field": "priority", "direction": "desc"},
                        {"field": "case_id", "direction": "asc"}
                    ]
                })
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "cases/a.json",
        serde_json::json!({
            "summary": "payment failure requires operator follow up",
            "state": "pending",
            "priority": 30,
            "case_id": "case-a"
        }),
        "composite-a",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "cases/b.json",
        serde_json::json!({
            "summary": "payment failure already resolved",
            "state": "pending",
            "priority": 90,
            "case_id": "case-b"
        }),
        "composite-b",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "cases/c.json",
        serde_json::json!({
            "summary": "profile update needs review",
            "state": "pending",
            "priority": 100,
            "case_id": "case-c"
        }),
        "composite-c",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "cases/d.json",
        serde_json::json!({
            "summary": "payment failure needs later audit",
            "state": "closed",
            "priority": 10,
            "case_id": "case-d"
        }),
        "composite-d",
    )
    .await;

    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "cases/"},
                {"full_text": {"query": "payment failure"}},
                {"field": "state", "op": "eq", "value": "pending"},
                {"can": {"relation": "read"}}
            ]
        },
        "order_by": [
            {"field": "priority", "direction": "desc"},
            {"field": "case_id", "direction": "asc"}
        ],
        "limit": 1,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();

    let mut warmup_spec: serde_json::Value = serde_json::from_str(&query_spec).unwrap();
    warmup_spec["limit"] = serde_json::json!(10);
    query_spec_until_hits(
        &mut index_client,
        &token,
        QuerySpecRequest {
            query_spec_json: warmup_spec.to_string(),
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        2,
        Duration::from_secs(90),
    )
    .await;

    let response = query_spec_until_hits(
        &mut index_client,
        &token,
        QuerySpecRequest {
            query_spec_json: query_spec.clone(),
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        1,
        Duration::from_secs(90),
    )
    .await;
    let result = response.result.expect("query spec result");
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].object_key, "cases/b.json");
    assert!(!result.next_page_token.is_empty());

    let plan: serde_json::Value = serde_json::from_str(&response.plan_json).unwrap();
    assert_eq!(plan["planner"], "primitive-index-intersection");
    assert_eq!(plan["selected_index"]["name"], "case-text");
    assert_eq!(plan["selected_index"]["kind"], "full_text");
    assert_eq!(plan["filter_index"]["name"], "case-state");
    assert_eq!(plan["filter_index"]["kind"], "typed_json");

    let scoring: serde_json::Value = serde_json::from_str(&result.scoring_recipe_json).unwrap();
    assert_eq!(scoring["kind"], "query_spec_composite");
    assert_eq!(scoring["planner"], "primitive-index-intersection");
    assert_eq!(scoring["primary_index"], "case-text");
    assert_eq!(scoring["typed_filter_index"], "case-state");

    let second_page = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: query_spec.clone(),
                page_token: result.next_page_token.clone(),
                lag_timeout_ms: 1000,
                accept_degraded: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("second query spec page");
    assert_eq!(second_page.hits.len(), 1);
    assert_eq!(second_page.hits[0].object_key, "cases/a.json");

    let tampered_predicate = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "cases/"},
                {"full_text": {"query": "payment failure"}},
                {"field": "state", "op": "eq", "value": "closed"},
                {"can": {"relation": "read"}}
            ]
        },
        "order_by": [
            {"field": "priority", "direction": "desc"},
            {"field": "case_id", "direction": "asc"}
        ],
        "limit": 1,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();
    let tampered = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: tampered_predicate,
                page_token: result.next_page_token,
                lag_timeout_ms: 1000,
                accept_degraded: false,
            },
            &token,
        ))
        .await;
    assert_eq!(tampered.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_query_spec_intersection_filters_inherit_object_hits_by_read_scope() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("qsa");
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "auth-text".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "auth/"}).to_string(),
                extractor_json: serde_json::json!({
                    "source": "json_pointer",
                    "pointer": "/summary"
                })
                .to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "auth-state".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({"prefix": "auth/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [
                        {"name": "state", "extractor": "/state", "required": true},
                        {"name": "priority", "extractor": "/priority", "required": true}
                    ],
                    "default_order": [
                        {"field": "priority", "direction": "desc"}
                    ]
                })
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "auth/allowed.json",
        serde_json::json!({
            "summary": "payment failure requires review",
            "state": "pending",
            "priority": 10
        }),
        "auth-composite-allowed",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "auth/denied.json",
        serde_json::json!({
            "summary": "payment failure high priority",
            "state": "pending",
            "priority": 100
        }),
        "auth-composite-denied",
    )
    .await;

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let scope_reader = unique_test_name("query-spec-scope-reader");
    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token(scope_reader.clone(), claims.tenant_id)
        .unwrap();
    grant_bucket_index_query_for_principal(&cluster, &bucket_name, &scope_reader).await;
    grant_tenant_object_reader_for_principal(
        &cluster,
        &bucket_name,
        "auth/allowed.json",
        &scope_reader,
    )
    .await;

    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "auth/"},
                {"full_text": {"query": "payment failure"}},
                {"field": "state", "op": "eq", "value": "pending"},
                {"can": {"relation": "read"}}
            ]
        },
        "order_by": [
            {"field": "priority", "direction": "desc"}
        ],
        "limit": 10,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();
    let response = query_spec_until_hits(
        &mut index_client,
        &limited_token,
        QuerySpecRequest {
            query_spec_json: query_spec,
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        1,
        Duration::from_secs(30),
    )
    .await;
    let result = response.result.expect("query spec result");
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].object_key, "auth/allowed.json");

    let plan: serde_json::Value = serde_json::from_str(&response.plan_json).unwrap();
    assert_eq!(plan["planner"], "primitive-index-intersection");
    assert_eq!(
        plan["selected_index"]["authorization_mode"],
        "inherit_object"
    );
    assert_eq!(plan["filter_index"]["authorization_mode"], "inherit_object");
    assert_eq!(plan["authz_relation"], "read");
}

#[tokio::test]
async fn test_query_spec_path_filter_intersects_authz_before_results() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("qsp");
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "by-path".to_string(),
                kind: IndexKind::Path as i32,
                selector_json: serde_json::json!({}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "docs/allowed.json",
        serde_json::json!({"title": "allowed"}),
        "path-authz-allowed",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "docs/denied.json",
        serde_json::json!({"title": "denied"}),
        "path-authz-denied",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "other/allowed.json",
        serde_json::json!({"title": "outside path"}),
        "path-authz-outside",
    )
    .await;

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let path_reader = unique_test_name("query-spec-path-reader");
    let prefix_reader = unique_test_name("query-spec-path-prefix-reader");
    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token(path_reader.clone(), claims.tenant_id)
        .unwrap();
    let prefix_token = cluster.states[0]
        .jwt_manager
        .mint_token(prefix_reader.clone(), claims.tenant_id)
        .unwrap();
    grant_bucket_index_query_for_principal(&cluster, &bucket_name, &path_reader).await;
    grant_tenant_object_reader_for_principal(
        &cluster,
        &bucket_name,
        "docs/allowed.json",
        &path_reader,
    )
    .await;
    grant_bucket_index_query_for_principal(&cluster, &bucket_name, &prefix_reader).await;
    grant_tenant_object_reader_for_principal(
        &cluster,
        &bucket_name,
        "docs/allowed.json",
        &prefix_reader,
    )
    .await;
    grant_tenant_object_reader_for_principal(
        &cluster,
        &bucket_name,
        "docs/denied.json",
        &prefix_reader,
    )
    .await;

    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "docs/"},
                {"can": {"relation": "read"}}
            ]
        },
        "limit": 10,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();
    let response = query_spec_until_hits(
        &mut index_client,
        &limited_token,
        QuerySpecRequest {
            query_spec_json: query_spec.clone(),
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        1,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
    )
    .await;
    let result = response.result.expect("query spec result");
    assert_eq!(
        result
            .hits
            .iter()
            .map(|hit| hit.object_key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/allowed.json"]
    );
    let plan: serde_json::Value = serde_json::from_str(&response.plan_json).unwrap();
    assert_eq!(plan["selected_index"]["kind"], "path");
    assert_eq!(plan["authz_relation"], "read");
    assert_eq!(plan["degraded"], false);

    let prefix_response = query_spec_until_hits(
        &mut index_client,
        &prefix_token,
        QuerySpecRequest {
            query_spec_json: query_spec,
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        2,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
    )
    .await
    .result
    .expect("query spec prefix result");
    assert_eq!(
        prefix_response
            .hits
            .iter()
            .map(|hit| hit.object_key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/allowed.json", "docs/denied.json"]
    );
}

#[tokio::test]
async fn test_query_spec_inherit_object_filter_uses_derived_userset_grants() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut auth_client = AuthServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("qsu");
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "by-path".to_string(),
                kind: IndexKind::Path as i32,
                selector_json: serde_json::json!({}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "docs/group-allowed.json",
        serde_json::json!({"title": "group allowed"}),
        "userset-authz-allowed",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "docs/group-denied.json",
        serde_json::json!({"title": "group denied"}),
        "userset-authz-denied",
    )
    .await;

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let reader_subject = unique_test_name("query-spec-userset-reader");
    let group_name = unique_test_name("query-spec-group");
    write_authz_tuple(
        &mut auth_client,
        &token,
        "group",
        &group_name,
        "member",
        "app",
        &reader_subject,
    )
    .await;
    write_authz_tuple(
        &mut auth_client,
        &token,
        "object",
        &format!("{bucket_name}/docs/group-allowed.json"),
        "reader",
        "userset",
        &format!("group/{group_name}#member"),
    )
    .await;

    wait_for_derived_authz_entry(
        &cluster,
        &encode_realm_namespace(DEFAULT_AUTHZ_REALM_ID, "object"),
        &format!("{bucket_name}/docs/group-allowed.json"),
        "reader",
        "app",
        &reader_subject,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
    )
    .await;

    let userset_token = cluster.states[0]
        .jwt_manager
        .mint_token(reader_subject.clone(), claims.tenant_id)
        .unwrap();
    grant_bucket_index_query_for_principal(&cluster, &bucket_name, &reader_subject).await;
    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "docs/"},
                {"can": {"relation": "read"}}
            ]
        },
        "limit": 10,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();

    let response = query_spec_until_hits(
        &mut index_client,
        &userset_token,
        QuerySpecRequest {
            query_spec_json: query_spec,
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        1,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
    )
    .await;
    let result = response.result.expect("query spec result");
    assert_eq!(
        result
            .hits
            .iter()
            .map(|hit| hit.object_key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/group-allowed.json"]
    );
    let plan: serde_json::Value = serde_json::from_str(&response.plan_json).unwrap();
    assert_eq!(plan["selected_index"]["kind"], "path");
    assert_eq!(plan["authz_relation"], "read");
    assert_eq!(plan["degraded"], false);
}

#[tokio::test]
async fn test_query_spec_intersects_vector_with_typed_filter_without_bucket_scan() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("qsv");
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "case-vector".to_string(),
                kind: IndexKind::Vector as i32,
                selector_json: serde_json::json!({"prefix": "vectors/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: rfc_vector_policy(
                    "object_body_json_vector",
                    "caller_supplied",
                    "test-explicit-vector",
                    2,
                    "text",
                    "cosine",
                )
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "vector-state".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({"prefix": "vectors/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [
                        {"name": "state", "extractor": "/state", "required": true},
                        {"name": "case_id", "extractor": "/case_id", "required": true}
                    ],
                    "default_order": [
                        {"field": "case_id", "direction": "asc"}
                    ]
                })
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "vectors/a.json",
        serde_json::json!({
            "vector": [1.0, 0.0],
            "source_start": 0,
            "source_len": 4,
            "state": "pending",
            "case_id": "case-a"
        }),
        "vector-composite-a",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "vectors/b.json",
        serde_json::json!({
            "vector": [1.0, 0.0],
            "source_start": 0,
            "source_len": 4,
            "state": "closed",
            "case_id": "case-b"
        }),
        "vector-composite-b",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "vectors/c.json",
        serde_json::json!({
            "vector": [0.0, 1.0],
            "source_start": 0,
            "source_len": 4,
            "state": "pending",
            "case_id": "case-c"
        }),
        "vector-composite-c",
    )
    .await;

    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "vectors/"},
                {"vector": {"near": [1.0, 0.0]}},
                {"field": "state", "op": "eq", "value": "pending"},
                {"can": {"relation": "read"}}
            ]
        },
        "limit": 10,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();
    let response = query_spec_until_hits(
        &mut index_client,
        &token,
        QuerySpecRequest {
            query_spec_json: query_spec,
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        2,
        Duration::from_secs(90),
    )
    .await;
    let result = response.result.expect("query spec result");
    let hit_keys = result
        .hits
        .iter()
        .map(|hit| hit.object_key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(hit_keys, vec!["vectors/a.json", "vectors/c.json"]);

    let plan: serde_json::Value = serde_json::from_str(&response.plan_json).unwrap();
    assert_eq!(plan["planner"], "primitive-index-intersection");
    assert_eq!(plan["selected_index"]["name"], "case-vector");
    assert_eq!(plan["selected_index"]["kind"], "vector");
    assert_eq!(plan["filter_index"]["name"], "vector-state");
    assert_eq!(plan["filter_index"]["kind"], "typed_json");
}

#[tokio::test]
async fn test_query_spec_intersects_hybrid_with_typed_filter_without_bucket_scan() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("qsh");
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let mut vector_policy = rfc_vector_policy(
        "object_body_json_vector",
        "caller_supplied",
        "test-explicit-vector",
        2,
        "text",
        "cosine",
    );
    vector_policy["extractor"]["json_pointer"] = serde_json::json!("/embedding");
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "case-hybrid".to_string(),
                kind: IndexKind::Hybrid as i32,
                selector_json: serde_json::json!({"prefix": "hybrid/"}).to_string(),
                extractor_json: serde_json::json!({
                    "text": {
                        "source": "json_pointer",
                        "json_pointer": "/summary"
                    }
                })
                .to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "full_text": {"positions": true},
                    "vector": vector_policy
                })
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "hybrid-state".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({"prefix": "hybrid/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [
                        {"name": "state", "extractor": "/state", "required": true},
                        {"name": "priority", "extractor": "/priority", "required": true},
                        {"name": "case_id", "extractor": "/case_id", "required": true}
                    ],
                    "default_order": [
                        {"field": "priority", "direction": "desc"},
                        {"field": "case_id", "direction": "asc"}
                    ]
                })
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "hybrid/a.json",
        serde_json::json!({
            "summary": "lease dashboard escalation",
            "embedding": [0.0, 1.0],
            "source_start": 0,
            "source_len": 12,
            "state": "pending",
            "priority": 20,
            "case_id": "case-a"
        }),
        "hybrid-composite-a",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "hybrid/b.json",
        serde_json::json!({
            "summary": "lease dashboard archived",
            "embedding": [0.0, 1.0],
            "source_start": 0,
            "source_len": 12,
            "state": "closed",
            "priority": 90,
            "case_id": "case-b"
        }),
        "hybrid-composite-b",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "hybrid/c.json",
        serde_json::json!({
            "summary": "lease dashboard planning",
            "embedding": [0.0, 1.0],
            "source_start": 0,
            "source_len": 12,
            "state": "pending",
            "priority": 70,
            "case_id": "case-c"
        }),
        "hybrid-composite-c",
    )
    .await;

    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "hybrid/"},
                {"full_text": {"query": "lease dashboard"}},
                {"vector": {"near": [0.0, 1.0]}},
                {"field": "state", "op": "eq", "value": "pending"},
                {"can": {"relation": "read"}}
            ]
        },
        "order_by": [
            {"field": "priority", "direction": "desc"},
            {"field": "case_id", "direction": "asc"}
        ],
        "limit": 1,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();

    let mut warmup_spec: serde_json::Value = serde_json::from_str(&query_spec).unwrap();
    warmup_spec["limit"] = serde_json::json!(10);
    query_spec_until_hits(
        &mut index_client,
        &token,
        QuerySpecRequest {
            query_spec_json: warmup_spec.to_string(),
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        2,
        Duration::from_secs(90),
    )
    .await;

    let response = query_spec_until_hits(
        &mut index_client,
        &token,
        QuerySpecRequest {
            query_spec_json: query_spec.clone(),
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        1,
        Duration::from_secs(90),
    )
    .await;
    let result = response.result.expect("query spec result");
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].object_key, "hybrid/c.json");
    assert!(!result.next_page_token.is_empty());

    let plan: serde_json::Value = serde_json::from_str(&response.plan_json).unwrap();
    assert_eq!(plan["planner"], "primitive-index-intersection");
    assert_eq!(plan["selected_index"]["name"], "case-hybrid");
    assert_eq!(plan["selected_index"]["kind"], "hybrid");
    assert_eq!(plan["filter_index"]["name"], "hybrid-state");
    assert_eq!(plan["filter_index"]["kind"], "typed_json");

    let scoring: serde_json::Value = serde_json::from_str(&result.scoring_recipe_json).unwrap();
    assert_eq!(scoring["kind"], "query_spec_composite");
    assert_eq!(scoring["primary_index"], "case-hybrid");
    assert_eq!(scoring["typed_filter_index"], "hybrid-state");
    assert_eq!(scoring["primary_scoring"]["kind"], "hybrid");

    let second_page = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: query_spec,
                page_token: result.next_page_token,
                lag_timeout_ms: 1000,
                accept_degraded: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("second query spec page");
    assert_eq!(second_page.hits.len(), 1);
    assert_eq!(second_page.hits[0].object_key, "hybrid/a.json");
}
