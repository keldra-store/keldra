use super::*;
use anvil::typed_field_segment::{
    TypedFieldSegmentRow, TypedFieldSegmentWrite, write_typed_field_segment,
};
use std::collections::BTreeMap;

#[tokio::test]
async fn test_typed_json_index_queries_append_record_payloads() {
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

    let bucket_name = unique_test_name("typed-json-append-index-bucket");
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

    let stream = object_client
        .create_append_stream(authorized(
            CreateAppendStreamRequest {
                bucket_name: bucket_name.clone(),
                stream_key: "audit/tenant-a".to_string(),
                mutation_context: Some(native_mutation_context(bucket_id, "append-stream")),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    for (idx, payload) in [
        serde_json::json!({"event": {"kind": "attempt", "severity": "warn", "attempt": 2}}),
        serde_json::json!({"event": {"kind": "attempt", "severity": "info", "attempt": 1}}),
        serde_json::json!({"event": {"kind": "repair", "severity": "warn", "attempt": 3}}),
    ]
    .into_iter()
    .enumerate()
    {
        object_client
            .append_stream_record(authorized(
                AppendStreamRecordRequest {
                    bucket_name: bucket_name.clone(),
                    stream_key: "audit/tenant-a".to_string(),
                    stream_id: stream.stream_id.clone(),
                    payload: serde_json::to_vec(&payload).unwrap(),
                    mutation_context: Some(native_mutation_context(
                        bucket_id,
                        &format!("append-record-{idx}"),
                    )),
                    content_type: Some("application/json".to_string()),
                    user_metadata_json: serde_json::json!({"source": "test"}).to_string(),
                    precondition: None,
                },
                &token,
            ))
            .await
            .unwrap();
    }

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "append-events".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({"prefix": "audit/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "append_record",
                    "fields": [
                        {"name": "stream", "extractor": "append_stream_key", "required": true},
                        {"name": "sequence", "extractor": "append_record_sequence", "required": true},
                        {"name": "kind", "extractor": "append_payload_json_pointer:/event/kind", "required": true},
                        {"name": "severity", "extractor": "append_payload_json_pointer:/event/severity", "required": true},
                        {"name": "attempt", "extractor": "append_payload_json_pointer:/event/attempt", "required": true},
                        {"name": "source", "extractor": "append_user_metadata_json_pointer:/source", "required": true}
                    ],
                    "default_order": [
                        {"field": "attempt", "direction": "asc"},
                        {"field": "sequence", "direction": "asc"}
                    ]
                })
                .to_string(),

                options: None,},
            &token,
        ))
        .await
        .unwrap();

    let response = query_index_until_hits(
        &mut index_client,
        &token,
        QueryIndexRequest {
            bucket_name,
            index_name: "append-events".to_string(),
            query_text: String::new(),
            query_vector: vec![],
            limit: 10,
            phrase: false,
            path_prefix: "audit/".to_string(),
            metadata_filters_json: String::new(),
            boundary_predicates_json: String::new(),
            typed_predicates_json: serde_json::json!([
                {"field": "kind", "op": "eq", "value": "attempt"},
                {"field": "severity", "op": "in", "values": ["info", "warn"]},
                {"field": "source", "op": "eq", "value": "test"}
            ])
            .to_string(),
            typed_order_json: String::new(),
            page_token: String::new(),
            require_caught_up_to_watch_cursor: String::new(),
            lag_timeout_ms: 0,
        },
        2,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
    )
    .await;

    assert_eq!(response.hits.len(), 2);
    assert_eq!(response.hits[0].object_key, "audit/tenant-a");
    let first_values: serde_json::Value =
        serde_json::from_str(&response.hits[0].metadata_json).unwrap();
    assert_eq!(first_values["typed_values"]["attempt"], 1);
    let second_values: serde_json::Value =
        serde_json::from_str(&response.hits[1].metadata_json).unwrap();
    assert_eq!(second_values["typed_values"]["attempt"], 2);
}

#[tokio::test]
async fn test_typed_json_index_omits_reserved_internal_candidates() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("typed-reserved");
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

    let created = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "typed-visibility".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [
                        {"name": "kind", "extractor": "/kind", "required": true},
                        {"name": "sort", "extractor": "/sort", "required": true}
                    ],
                    "default_order": [
                        {"field": "sort", "direction": "asc"}
                    ]
                })
                .to_string(),

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
    let index_storage_id = anvil::index_journal::index_storage_id(
        claims.tenant_id,
        bucket_id,
        created.index_id as i64,
    );
    let field_names = vec!["kind".to_string(), "sort".to_string()];
    let visible_values = BTreeMap::from([
        ("kind".to_string(), serde_json::json!("case")),
        ("sort".to_string(), serde_json::json!("a")),
    ]);
    let reserved_values = BTreeMap::from([
        ("kind".to_string(), serde_json::json!("case")),
        ("sort".to_string(), serde_json::json!("b")),
    ]);
    let rows = vec![
        TypedFieldSegmentRow {
            object_key: "visible/query-candidate.json".to_string(),
            object_version_id: uuid::Uuid::new_v4().to_string(),
            source_identity: "visible/query-candidate.json".to_string(),
            values: visible_values,
            encoded_values: BTreeMap::new(),
            source_id_binary: Vec::new(),
            value_flags: 0,
            authz_label_hash: hex::encode([1_u8; 32]),
            authz_revision: 0,
        },
        TypedFieldSegmentRow {
            object_key: "_anvil/authz/query-candidate.json".to_string(),
            object_version_id: uuid::Uuid::new_v4().to_string(),
            source_identity: "_anvil/authz/query-candidate.json".to_string(),
            values: reserved_values,
            encoded_values: BTreeMap::new(),
            source_id_binary: Vec::new(),
            value_flags: 0,
            authz_label_hash: hex::encode([2_u8; 32]),
            authz_revision: 0,
        },
    ];
    let definition_hash = hex::encode(blake3::hash(b"typed-reserved-query").as_bytes());
    write_typed_field_segment(
        &cluster.states[0].storage,
        TypedFieldSegmentWrite {
            index_id: &index_storage_id,
            generation: 99,
            source_kind: "object_current",
            source_cursor: 1,
            authz_revision: 0,
            boundary_values: &[],
            definition_hash: &definition_hash,
            field_names: &field_names,
            rows: &rows,
        },
    )
    .await
    .unwrap();

    let response = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name,
                index_name: "typed-visibility".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: String::new(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: serde_json::json!([
                    {"field": "kind", "op": "eq", "value": "case"}
                ])
                .to_string(),
                typed_order_json: String::new(),
                page_token: String::new(),
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response
            .hits
            .iter()
            .map(|hit| hit.object_key.as_str())
            .collect::<Vec<_>>(),
        vec!["visible/query-candidate.json"]
    );
}

#[tokio::test]
async fn test_index_definition_lifecycle() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("index-definition-bucket");
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
                name: "docs-full-text".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"fields": [{"path": "body", "source": "utf8"}]})
                    .to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({"positions": true, "language": "simple"})
                    .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("created index");

    assert_eq!(created.bucket_name, bucket_name);
    assert_eq!(created.name, "docs-full-text");
    assert_eq!(created.kind, IndexKind::FullText as i32);
    assert_eq!(created.authorization_mode, "inherit_object");
    assert!(created.enabled);
    assert_eq!(created.version, 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&created.selector_json).unwrap()["prefix"],
        "docs/"
    );

    let listed = index_client
        .list_indexes(authorized(
            ListIndexesRequest {
                bucket_name: bucket_name.clone(),
                include_disabled: false,
                page: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .indexes;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "docs-full-text");

    let updated = index_client
        .update_index(authorized(
            UpdateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "docs-full-text".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/v2/"}).to_string(),
                extractor_json:
                    serde_json::json!({"fields": [{"path": "summary", "source": "utf8"}]})
                        .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": false, "language": "simple"})
                    .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("updated index");
    assert_eq!(updated.authorization_mode, "index_only");
    assert_eq!(updated.version, 2);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&updated.selector_json).unwrap()["prefix"],
        "docs/v2/"
    );

    let disabled = index_client
        .disable_index(authorized(
            DisableIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "docs-full-text".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("disabled index");
    assert!(!disabled.enabled);
    assert_eq!(disabled.version, 3);

    let active_only = index_client
        .list_indexes(authorized(
            ListIndexesRequest {
                bucket_name: bucket_name.clone(),
                include_disabled: false,
                page: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .indexes;
    assert!(active_only.is_empty());

    let with_disabled = index_client
        .list_indexes(authorized(
            ListIndexesRequest {
                bucket_name: bucket_name.clone(),
                include_disabled: true,
                page: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .indexes;
    assert_eq!(with_disabled.len(), 1);
    assert!(!with_disabled[0].enabled);

    index_client
        .drop_index(authorized(
            DropIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "docs-full-text".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let after_drop = index_client
        .list_indexes(authorized(
            ListIndexesRequest {
                bucket_name: bucket_name.clone(),
                include_disabled: true,
                page: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .indexes;
    assert!(after_drop.is_empty());

    let mut watch = index_client
        .watch_index_definition(authorized(
            WatchIndexDefinitionRequest {
                bucket_name,
                after_cursor: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let mut events = Vec::new();
    for _ in 0..4 {
        events.push(watch.next().await.unwrap().unwrap());
    }
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["create", "update", "disable", "drop"]
    );
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].cursor < pair[1].cursor)
    );
    for event in &events {
        let envelope = event.envelope.as_ref().expect("index definition envelope");
        assert_eq!(envelope.watch_stream_id, "index_definition");
        assert_eq!(envelope.partition_family, "index_definition");
        assert_eq!(envelope.cursor_low, event.cursor);
        assert_eq!(envelope.record_kind, "index_definition");
        assert!(!envelope.payload_hash.is_empty());
    }
    assert_eq!(events[3].index.as_ref().unwrap().name, "docs-full-text");
}

#[tokio::test]
async fn test_query_path_and_metadata_filter_indexes_from_object_metadata() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("metadata-backed-index");
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

    let path_index = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "by-path".to_string(),
                kind: IndexKind::Path as i32,
                selector_json: serde_json::json!({}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("created path index");
    let metadata_index = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "by-meta".to_string(),
                kind: IndexKind::MetadataFilter as i32,
                selector_json: serde_json::json!({}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("created metadata index");

    let bucket_id = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .expect("bucket metadata should exist")
        .id;
    persist_index_object(
        &cluster,
        &bucket_name,
        "docs/alpha.txt",
        Some(serde_json::json!({"tenant": "alpha", "nested": {"state": "open"}})),
    )
    .await;
    persist_index_object(
        &cluster,
        &bucket_name,
        "docs/beta.txt",
        Some(serde_json::json!({"tenant": "beta", "nested": {"state": "open"}})),
    )
    .await;
    persist_index_object(
        &cluster,
        &bucket_name,
        "images/logo.txt",
        Some(serde_json::json!({"tenant": "alpha", "nested": {"state": "open"}})),
    )
    .await;
    wait_for_index_builds_for_indexes(
        &cluster,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
        1,
        bucket_id,
        &[path_index.index_id as i64, metadata_index.index_id as i64],
    )
    .await;

    let path_response = query_index_until_hits(
        &mut index_client,
        &token,
        QueryIndexRequest {
            bucket_name: bucket_name.clone(),
            index_name: "by-path".to_string(),
            query_text: String::new(),
            query_vector: vec![],
            limit: 10,
            phrase: false,
            path_prefix: "docs/".to_string(),
            metadata_filters_json: String::new(),
            boundary_predicates_json: String::new(),
            typed_predicates_json: String::new(),
            typed_order_json: String::new(),
            page_token: String::new(),
            require_caught_up_to_watch_cursor: String::new(),
            lag_timeout_ms: 0,
        },
        2,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
    )
    .await;

    assert_eq!(path_response.index_kind, IndexKind::Path as i32);
    assert_eq!(path_response.hits.len(), 2);
    assert_eq!(path_response.hits[0].kind, IndexKind::Path as i32);
    assert_eq!(path_response.hits[0].object_key, "docs/alpha.txt");
    assert_eq!(path_response.hits[1].object_key, "docs/beta.txt");
    let path_recipe: serde_json::Value =
        serde_json::from_str(&path_response.scoring_recipe_json).unwrap();
    assert_eq!(path_recipe["source"], "corestore_typed_field_segment");

    let paged_path_first = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "by-path".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 1,
                phrase: false,
                path_prefix: "docs/".to_string(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: String::new(),
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(paged_path_first.hits.len(), 1);
    assert_eq!(paged_path_first.hits[0].object_key, "docs/alpha.txt");
    assert!(!paged_path_first.next_page_token.is_empty());

    let paged_path_second = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "by-path".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: "docs/".to_string(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: paged_path_first.next_page_token.clone(),
                require_caught_up_to_watch_cursor: paged_path_first
                    .source_watch_cursor_high
                    .to_string(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(paged_path_second.hits.len(), 1);
    assert_eq!(paged_path_second.hits[0].object_key, "docs/beta.txt");

    let changed_predicate = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "by-path".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: "images/".to_string(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: paged_path_first.next_page_token,
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await;
    assert_eq!(
        changed_predicate.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );

    let metadata_response = query_index_until_hits(
        &mut index_client,
        &token,
        QueryIndexRequest {
            bucket_name,
            index_name: "by-meta".to_string(),
            query_text: String::new(),
            query_vector: vec![],
            limit: 10,
            phrase: false,
            path_prefix: "docs/".to_string(),
            metadata_filters_json: serde_json::json!({
                "tenant": "alpha",
                "/nested/state": "open"
            })
            .to_string(),
            boundary_predicates_json: String::new(),
            typed_predicates_json: String::new(),
            typed_order_json: String::new(),
            page_token: String::new(),
            require_caught_up_to_watch_cursor: String::new(),
            lag_timeout_ms: 0,
        },
        1,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
    )
    .await;

    assert_eq!(
        metadata_response.index_kind,
        IndexKind::MetadataFilter as i32
    );
    assert_eq!(metadata_response.hits.len(), 1);
    assert_eq!(
        metadata_response.hits[0].kind,
        IndexKind::MetadataFilter as i32
    );
    assert_eq!(metadata_response.hits[0].score, 1.0);
    assert_eq!(metadata_response.hits[0].object_key, "docs/alpha.txt");
    let hit_metadata: serde_json::Value =
        serde_json::from_str(&metadata_response.hits[0].metadata_json).unwrap();
    assert_eq!(hit_metadata["user_metadata"]["tenant"], "alpha");
    assert_eq!(hit_metadata["user_metadata"]["nested"]["state"], "open");
}

#[tokio::test]
async fn test_live_metadata_query_uses_planner_authz_candidates_and_scoped_page_tokens() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("planner-authz-metadata");
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
                name: "by-metadata".to_string(),
                kind: IndexKind::MetadataFilter as i32,
                selector_json: serde_json::json!({}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("created metadata index");
    let bucket_id = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .expect("bucket metadata should exist")
        .id;

    persist_index_object(
        &cluster,
        &bucket_name,
        "tenant-a/allowed-1.json",
        Some(serde_json::json!({"tenant": "a", "kind": "invoice"})),
    )
    .await;
    persist_index_object(
        &cluster,
        &bucket_name,
        "tenant-a/allowed-2.json",
        Some(serde_json::json!({"tenant": "a", "kind": "invoice"})),
    )
    .await;
    persist_index_object(
        &cluster,
        &bucket_name,
        "tenant-a/denied.json",
        Some(serde_json::json!({"tenant": "a", "kind": "invoice"})),
    )
    .await;
    persist_index_object(
        &cluster,
        &bucket_name,
        "tenant-b/allowed.json",
        Some(serde_json::json!({"tenant": "b", "kind": "invoice"})),
    )
    .await;
    wait_for_index_builds_for_indexes(
        &cluster,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
        1,
        bucket_id,
        &[created.index_id as i64],
    )
    .await;

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let metadata_reader = unique_test_name("planner-metadata-reader");
    let no_object_reader = unique_test_name("planner-no-object-reader");
    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token(metadata_reader.clone(), claims.tenant_id)
        .unwrap();
    let no_object_token = cluster.states[0]
        .jwt_manager
        .mint_token(no_object_reader.clone(), claims.tenant_id)
        .unwrap();

    grant_bucket_index_query_for_principal(&cluster, &bucket_name, &metadata_reader).await;
    grant_bucket_index_query_for_principal(&cluster, &bucket_name, &no_object_reader).await;
    grant_tenant_object_reader_for_principal(
        &cluster,
        &bucket_name,
        "tenant-a/allowed-1.json",
        &metadata_reader,
    )
    .await;
    grant_tenant_object_reader_for_principal(
        &cluster,
        &bucket_name,
        "tenant-a/allowed-2.json",
        &metadata_reader,
    )
    .await;
    grant_tenant_object_reader_for_principal(
        &cluster,
        &bucket_name,
        "tenant-b/allowed.json",
        &metadata_reader,
    )
    .await;

    let first_page = query_index_until_hits(
        &mut index_client,
        &limited_token,
        QueryIndexRequest {
            bucket_name: bucket_name.clone(),
            index_name: "by-metadata".to_string(),
            query_text: String::new(),
            query_vector: vec![],
            limit: 1,
            phrase: false,
            path_prefix: "tenant-a/".to_string(),
            metadata_filters_json: serde_json::json!({
                "tenant": "a",
                "kind": "invoice"
            })
            .to_string(),
            boundary_predicates_json: String::new(),
            typed_predicates_json: String::new(),
            typed_order_json: String::new(),
            page_token: String::new(),
            require_caught_up_to_watch_cursor: String::new(),
            lag_timeout_ms: 0,
        },
        1,
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(first_page.hits[0].object_key, "tenant-a/allowed-1.json");
    assert!(!first_page.next_page_token.is_empty());

    let second_page = query_index_until_hits(
        &mut index_client,
        &limited_token,
        QueryIndexRequest {
            bucket_name: bucket_name.clone(),
            index_name: "by-metadata".to_string(),
            query_text: String::new(),
            query_vector: vec![],
            limit: 10,
            phrase: false,
            path_prefix: "tenant-a/".to_string(),
            metadata_filters_json: serde_json::json!({
                "tenant": "a",
                "kind": "invoice"
            })
            .to_string(),
            boundary_predicates_json: String::new(),
            typed_predicates_json: String::new(),
            typed_order_json: String::new(),
            page_token: first_page.next_page_token.clone(),
            require_caught_up_to_watch_cursor: first_page.source_watch_cursor_high.to_string(),
            lag_timeout_ms: 0,
        },
        1,
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(second_page.hits[0].object_key, "tenant-a/allowed-2.json");

    let no_object_results = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "by-metadata".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: "tenant-a/".to_string(),
                metadata_filters_json: serde_json::json!({
                    "tenant": "a",
                    "kind": "invoice"
                })
                .to_string(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: String::new(),
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &no_object_token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(no_object_results.hits.is_empty());

    let wrong_scope = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "by-metadata".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: "tenant-b/".to_string(),
                metadata_filters_json: serde_json::json!({
                    "tenant": "b",
                    "kind": "invoice"
                })
                .to_string(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: first_page.next_page_token,
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &limited_token,
        ))
        .await;
    assert_eq!(
        wrong_scope.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );

    let stale_cursor = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name,
                index_name: "by-metadata".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: "tenant-a/".to_string(),
                metadata_filters_json: serde_json::json!({
                    "tenant": "a",
                    "kind": "invoice"
                })
                .to_string(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: String::new(),
                require_caught_up_to_watch_cursor: u64::MAX.to_string(),
                lag_timeout_ms: 1,
            },
            &limited_token,
        ))
        .await;
    assert_eq!(
        stale_cursor.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
}
