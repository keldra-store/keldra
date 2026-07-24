use super::*;
use prost::Message;
use sha2::{Digest, Sha256};

#[derive(Clone, PartialEq, Message)]
struct TestFullTextDocumentTableProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(message, repeated, tag = "2")]
    rows: Vec<TestFullTextDocumentTableRowProto>,
}

#[derive(Clone, PartialEq, Message)]
struct TestFullTextDocumentTableRowProto {
    #[prost(uint64, tag = "1")]
    document_id: u64,
    #[prost(uint32, tag = "2")]
    field_id: u32,
    #[prost(string, tag = "3")]
    object_key: String,
    #[prost(string, tag = "4")]
    version_id: String,
}

fn derived_index_proof_head_tuple_key(index_id: &str) -> Vec<u8> {
    anvil::core_store::core_meta_tuple_key(&[
        anvil::core_store::CoreMetaTuplePart::Utf8("derived-index-proof"),
        anvil::core_store::CoreMetaTuplePart::Utf8(index_id),
        anvil::core_store::CoreMetaTuplePart::Utf8("head"),
    ])
    .unwrap()
}

fn index_segment_tuple_keys(
    record: &anvil::index_coremeta::IndexSegmentCoreMetaRecord,
) -> Vec<Vec<u8>> {
    use anvil::core_store::{CoreMetaTuplePart, core_meta_tuple_key};

    let segment_ref_hash = format!(
        "sha256:{}",
        hex::encode(Sha256::digest(record.segment_ref.as_bytes()))
    );
    [
        vec![
            CoreMetaTuplePart::Utf8("index_segment"),
            CoreMetaTuplePart::Utf8(&record.index_id),
            CoreMetaTuplePart::Utf8(&record.index_kind),
            CoreMetaTuplePart::U64(record.generation),
            CoreMetaTuplePart::Utf8(&record.segment_hash),
        ],
        vec![
            CoreMetaTuplePart::Utf8("index_segment_latest"),
            CoreMetaTuplePart::Utf8(&record.index_id),
        ],
        vec![
            CoreMetaTuplePart::Utf8("index_segment_family_latest"),
            CoreMetaTuplePart::Utf8(&record.index_id),
            CoreMetaTuplePart::Utf8(&record.writer_family),
        ],
        vec![
            CoreMetaTuplePart::Utf8("index_segment_generation"),
            CoreMetaTuplePart::Utf8(&record.index_id),
            CoreMetaTuplePart::Utf8(&record.writer_family),
            CoreMetaTuplePart::U64(record.generation),
        ],
        vec![
            CoreMetaTuplePart::Utf8("index_segment_ref"),
            CoreMetaTuplePart::Utf8(&record.index_id),
            CoreMetaTuplePart::Hash(&segment_ref_hash),
        ],
    ]
    .into_iter()
    .map(|parts| core_meta_tuple_key(&parts).unwrap())
    .collect()
}

fn delete_index_segment_coremeta_row(
    storage: &anvil::storage::Storage,
    record: &anvil::index_coremeta::IndexSegmentCoreMetaRecord,
) {
    let store = anvil::core_store::CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    for tuple_key in index_segment_tuple_keys(record) {
        store
            .delete(
                anvil::core_store::CF_INDEX_ROWS,
                anvil::core_store::TABLE_INDEX_ROW,
                &tuple_key,
            )
            .unwrap();
    }
}

async fn collect_index_segments_for_test(
    storage: &anvil::storage::Storage,
    index_id: &str,
) -> Vec<anvil::index_coremeta::IndexSegmentCoreMetaRecord> {
    let mut cursor: Option<Vec<u8>> = None;
    let mut records = Vec::new();
    loop {
        let page = anvil::index_coremeta::page_index_segment_coremeta_records(
            storage,
            index_id,
            cursor.as_deref(),
            256,
        )
        .await
        .unwrap();
        records.extend(page.records);
        let Some(next_cursor) = page.next_tuple_key else {
            break;
        };
        assert!(
            match cursor {
                None => true,
                Some(current) => current.as_slice() < next_cursor.as_slice(),
            },
            "index segment page cursor must advance"
        );
        cursor = Some(next_cursor);
    }
    records
}

#[allow(clippy::too_many_arguments)]
async fn create_index_definition_for_test(
    persistence: &anvil::persistence::Persistence,
    bucket: &anvil::persistence::Bucket,
    name: &str,
    kind: &str,
    selector: serde_json::Value,
    extractor: serde_json::Value,
    authorization_mode: &str,
    build_policy: serde_json::Value,
) -> anvil::persistence::IndexDefinition {
    let mutation = anvil::persistence::IndexDefinitionMutation::Create {
        name: name.to_string(),
        kind: kind.to_string(),
        selector,
        extractor,
        authorization_mode: authorization_mode.to_string(),
        build_policy,
    };
    let outcome = persistence
        .apply_index_definition_mutation(bucket, &mutation, None, None)
        .await
        .unwrap();
    let anvil::persistence::IndexDefinitionMutationOutcome::Published { index, .. } = outcome
    else {
        panic!("test index definition create should publish");
    };
    index
}

#[tokio::test]
async fn test_full_text_index_builds_from_object_write_task() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("index-build-task");
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
                name: "body".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"source": "object_body_utf8"}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    let created = created.into_inner().index.expect("created index");

    let bucket_id = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .expect("bucket metadata should exist")
        .id;
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: "docs/alpha.txt".to_string(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
                    storage_class: None,
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"alpha beta automatic index build".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    object_client.put_object(put_req).await.unwrap();

    let response = query_index_until_hits(
        &mut index_client,
        &token,
        QueryIndexRequest {
            bucket_name: bucket_name.clone(),
            index_name: "body".to_string(),
            query_text: "automatic index".to_string(),
            query_vector: vec![],
            limit: 10,
            phrase: false,
            path_prefix: String::new(),
            metadata_filters_json: String::new(),
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
    assert_eq!(response.index_kind, IndexKind::FullText as i32);
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "docs/alpha.txt");

    let mut watch = index_client
        .watch_index_partition(authorized(
            WatchIndexPartitionRequest {
                bucket_name: bucket_name.clone(),
                index_name: "body".to_string(),
                partition_id: String::new(),
                after_cursor_low: 0,
                after_cursor_high: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let watch_event = tokio::time::timeout(Duration::from_secs(30), watch.next())
        .await
        .expect("index partition watch should yield a built segment event")
        .expect("index partition watch stream should stay open")
        .expect("index partition watch event should be successful");
    assert_eq!(watch_event.bucket_name, bucket_name);
    assert_eq!(watch_event.index_name, "body");
    assert_eq!(watch_event.event_type, "segment_built");
    assert_eq!(watch_event.index_kind, IndexKind::FullText as i32);
    assert_eq!(watch_event.generation, response.index_generation);
    assert!(!watch_event.index_storage_id.is_empty());
    assert!(!watch_event.partition_id.is_empty());
    assert!(!watch_event.source_manifest_hash.is_empty());
    assert!(!watch_event.proof_hash.is_empty());
    assert!(!watch_event.segment_hashes.is_empty());
    let envelope = watch_event
        .envelope
        .as_ref()
        .expect("index partition envelope");
    assert_eq!(envelope.watch_stream_id, "index_partition");
    assert_eq!(envelope.partition_family, "index_partition");
    assert_eq!(envelope.cursor_low, watch_event.cursor_low);
    assert_eq!(envelope.cursor_high, watch_event.cursor_high);
    assert_eq!(envelope.index_generation, watch_event.generation);
    assert_eq!(envelope.record_kind, "index_partition");
    assert!(!envelope.payload_hash.is_empty());

    let tasks = wait_for_index_builds_for_indexes(
        &cluster,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
        1,
        bucket_id,
        &[created.index_id as i64],
    )
    .await;
    assert!(tasks.iter().any(|task| {
        task.task_type == anvil::tasks::TaskType::IndexBuild
            && task.status == anvil::tasks::TaskStatus::Completed
    }));
    assert!(!tasks.iter().any(|task| {
        task.task_type == anvil::tasks::TaskType::IndexBuild
            && task.status == anvil::tasks::TaskStatus::Failed
    }));
}

#[tokio::test]
async fn test_full_text_index_build_extracts_json_pointer_from_object_write_task() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("json-pointer-index-build");
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
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "summary".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({
                    "source": "json_pointer",
                    "pointer": "/summary"
                })
                .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let bucket_id = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .expect("bucket metadata should exist")
        .id;
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: "docs/report.json".to_string(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
                    storage_class: None,
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                br#"{"summary":"quarterly tenant retention analysis","body":"ignored"}"#.to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    object_client.put_object(put_req).await.unwrap();

    let response = query_index_until_hits(
        &mut index_client,
        &token,
        QueryIndexRequest {
            bucket_name: bucket_name.clone(),
            index_name: "summary".to_string(),
            query_text: "tenant retention".to_string(),
            query_vector: vec![],
            limit: 10,
            phrase: false,
            path_prefix: String::new(),
            metadata_filters_json: String::new(),
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
    assert_eq!(response.index_kind, IndexKind::FullText as i32);
    assert_eq!(response.hits[0].object_key, "docs/report.json");
    assert!(response.hits[0].score > 0.0);
}

#[tokio::test]
async fn test_full_text_index_build_uses_source_cursor_snapshot() {
    let cluster = isolated_test_cluster(
        "source cursor snapshot test inspects pending index tasks with background workers disabled",
        &["test-region-1"],
    )
    .await;
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = unique_test_name("cursor-snapshot");
    let bucket = persistence
        .create_bucket(tenant_id, &bucket_name, "test-region-1")
        .await
        .unwrap();
    let index = create_index_definition_for_test(
        persistence,
        &bucket,
        "body",
        "full_text",
        serde_json::json!({"prefix": "docs/"}),
        serde_json::json!({"source": "object_body_utf8"}),
        "index_only",
        serde_json::json!({"positions": true}),
    )
    .await;

    put_index_object_bytes(
        &cluster,
        tenant_id,
        &bucket.name,
        "docs/alpha.txt",
        Some("text/plain"),
        None,
        b"alpha cursor visible",
    )
    .await;
    let source_cursor = persistence
        .list_tasks_page(None, 1_000)
        .await
        .unwrap()
        .tasks
        .into_iter()
        .find(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.payload["tenant_id"] == serde_json::json!(tenant_id)
                && task.payload["bucket_id"] == serde_json::json!(bucket.id)
                && task.payload["index_id"] == serde_json::json!(index.id)
        })
        .and_then(|task| task.payload["source_cursor"].as_u64())
        .expect("first index build task records source cursor");

    put_index_object_bytes(
        &cluster,
        tenant_id,
        &bucket.name,
        "docs/future.txt",
        Some("text/plain"),
        None,
        b"future object must wait",
    )
    .await;
    let index_tasks = persistence
        .list_tasks_page(None, 1_000)
        .await
        .unwrap()
        .tasks
        .into_iter()
        .filter(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.payload["tenant_id"] == serde_json::json!(tenant_id)
                && task.payload["bucket_id"] == serde_json::json!(bucket.id)
                && task.payload["index_id"] == serde_json::json!(index.id)
        })
        .collect::<Vec<_>>();
    let pending_tasks = index_tasks
        .iter()
        .filter(|task| task.status == anvil::tasks::TaskStatus::Pending)
        .collect::<Vec<_>>();
    assert_eq!(
        pending_tasks.len(),
        1,
        "index build tasks for the same index should expose one pending build before execution"
    );
    assert!(
        pending_tasks[0].payload["source_cursor"].as_u64().unwrap() > source_cursor,
        "pending index build should advance to the latest source cursor"
    );
    assert!(
        index_tasks
            .iter()
            .any(|task| task.status == anvil::tasks::TaskStatus::Completed
                && task.payload["source_cursor"] == serde_json::json!(source_cursor)),
        "superseded index build should remain in the journal as a completed task"
    );

    persistence
        .rebuild_index_direct(
            tenant_id,
            bucket.id,
            index.id,
            index.version,
            u128::from(source_cursor),
        )
        .await
        .unwrap()
        .expect("index build succeeds");

    let index_storage_id = anvil::index_journal::index_storage_id(tenant_id, bucket.id, index.id);
    let segment = anvil::full_text_segment::read_latest_full_text_segment(
        &cluster.states[0].storage,
        &index_storage_id,
    )
    .await
    .unwrap()
    .expect("full text segment exists");
    assert_eq!(segment.header.source_cursor, source_cursor);
    let signing_key = hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap();
    let proof = anvil::derived_index_proof::read_latest_derived_index_proof(
        &cluster.states[0].storage,
        &index_storage_id,
        &signing_key,
    )
    .await
    .unwrap()
    .expect("derived index proof exists");
    assert_eq!(proof.source_cursor, u128::from(source_cursor));
    assert_eq!(proof.generation, segment.header.generation);
    assert_eq!(proof.segment_hashes.len(), 1);
    let checkpoint = anvil::watch_checkpoint::read_watch_checkpoint(
        &cluster.states[0].storage,
        "object_metadata",
        &index_storage_id,
        &signing_key,
    )
    .await
    .unwrap()
    .expect("index watch checkpoint exists");
    assert_eq!(checkpoint.cursor, u128::from(source_cursor));
    assert_eq!(checkpoint.source_manifest_hash, proof.source_manifest_hash);

    let definition = anvil::formats::full_text::FullTextIndexDefinition::from_json(
        &serde_json::json!({"positions": true}),
    )
    .unwrap();
    let alpha_hits = query_full_text_segment(
        &segment,
        FullTextSegmentQuery {
            query: "alpha",
            tokenizer: &definition.tokenizer,
            positions_enabled: definition.positions_enabled,
            phrase: false,
            bm25: anvil::formats::full_text::Bm25Config::default(),
            authorized_labels: None,
            limit: 10,
        },
    )
    .unwrap();
    let future_hits = query_full_text_segment(
        &segment,
        FullTextSegmentQuery {
            query: "future",
            tokenizer: &definition.tokenizer,
            positions_enabled: definition.positions_enabled,
            phrase: false,
            bm25: anvil::formats::full_text::Bm25Config::default(),
            authorized_labels: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(!alpha_hits.is_empty());
    assert!(future_hits.is_empty());

    let document_table = TestFullTextDocumentTableProto::decode(segment.document_table.as_slice())
        .expect("full text document table is deterministic protobuf");
    assert_eq!(
        document_table.schema,
        "anvil.index.full_text.document_table.v1"
    );
    let indexed_objects = document_table
        .rows
        .iter()
        .map(|row| row.object_key.as_str())
        .collect::<Vec<_>>();
    assert!(indexed_objects.contains(&"docs/alpha.txt"));
    assert!(!indexed_objects.contains(&"docs/future.txt"));
}

#[tokio::test]
async fn test_index_build_requires_current_rfc_ownership_fence() {
    let cluster = isolated_test_cluster(
        "ownership fence test manually claims index tasks with background workers disabled",
        &["test-region-1"],
    )
    .await;
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = unique_test_name("index-fence");
    let bucket = persistence
        .create_bucket(tenant_id, &bucket_name, "test-region-1")
        .await
        .unwrap();
    let index = create_index_definition_for_test(
        persistence,
        &bucket,
        "body",
        "full_text",
        serde_json::json!({"prefix": "docs/"}),
        serde_json::json!({"source": "object_body_utf8"}),
        "index_only",
        serde_json::json!({"positions": true}),
    )
    .await;
    put_index_object_bytes(
        &cluster,
        tenant_id,
        &bucket.name,
        "docs/alpha.txt",
        Some("text/plain"),
        None,
        b"alpha fence visible",
    )
    .await;
    let source_cursor = persistence
        .list_tasks_page(None, 1_000)
        .await
        .unwrap()
        .tasks
        .into_iter()
        .find(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.payload["tenant_id"] == serde_json::json!(tenant_id)
                && task.payload["bucket_id"] == serde_json::json!(bucket.id)
                && task.payload["index_id"] == serde_json::json!(index.id)
        })
        .and_then(|task| task.payload["source_cursor"].as_u64())
        .expect("index build task records source cursor");
    let index_storage_id = anvil::index_journal::index_storage_id(tenant_id, bucket.id, index.id);
    let resource = OwnershipResource {
        resource_kind: OwnershipResourceKind::IndexPartition,
        resource_id: format!(
            "tenant/{tenant_id}/bucket/{}/index_build/{index_storage_id}",
            bucket.id
        ),
    };
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap();

    acquire_ownership(
        &cluster.states[0].storage,
        AcquireOwnership {
            request_id: "other-node-index-owner".to_string(),
            idempotency_key: "other-node-index-owner".to_string(),
            resource,
            owner: OwnershipPrincipal {
                tenant_id: 0,
                principal_kind: "node".to_string(),
                principal_id: "other-node".to_string(),
                actor_instance_id: "other-node".to_string(),
                display_name: "other-node".to_string(),
                region: "test-region-1".to_string(),
                cell: "default".to_string(),
            },
            now_nanos,
            ttl_nanos: i64::try_from(MAX_OWNERSHIP_LEASE_MS)
                .unwrap()
                .saturating_mul(1_000_000),
        },
        &hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap(),
    )
    .await
    .unwrap();

    let err = persistence
        .rebuild_index_direct(
            tenant_id,
            bucket.id,
            index.id,
            index.version,
            u128::from(source_cursor),
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("OwnershipHeld"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_index_enqueue_rebuilds_when_checkpoint_exists_but_proof_is_missing() {
    let cluster = isolated_test_cluster(
        "missing proof rebuild test mutates index task checkpoints with background workers disabled",
        &["test-region-1"],
    )
    .await;
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = unique_test_name("index-missing-proof");
    let bucket = persistence
        .create_bucket(tenant_id, &bucket_name, "test-region-1")
        .await
        .unwrap();
    let index = create_index_definition_for_test(
        persistence,
        &bucket,
        "body",
        "full_text",
        serde_json::json!({"prefix": "docs/"}),
        serde_json::json!({"source": "object_body_utf8"}),
        "index_only",
        serde_json::json!({"positions": true}),
    )
    .await;
    put_index_object_bytes(
        &cluster,
        tenant_id,
        &bucket.name,
        "docs/alpha.txt",
        Some("text/plain"),
        None,
        b"alpha proves missing proof rebuild",
    )
    .await;

    let initial_task = persistence
        .list_tasks_page(None, 1_000)
        .await
        .unwrap()
        .tasks
        .into_iter()
        .find(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.payload["tenant_id"] == serde_json::json!(tenant_id)
                && task.payload["bucket_id"] == serde_json::json!(bucket.id)
                && task.payload["index_id"] == serde_json::json!(index.id)
        })
        .expect("initial index build task should exist");
    let source_cursor = initial_task
        .payload
        .get("source_cursor")
        .and_then(serde_json::Value::as_u64)
        .expect("initial index build task records source cursor");

    persistence
        .rebuild_index_direct(
            tenant_id,
            bucket.id,
            index.id,
            index.version,
            u128::from(source_cursor),
        )
        .await
        .unwrap()
        .expect("index build succeeds");
    persistence
        .update_task_status(initial_task.id, anvil::tasks::TaskStatus::Completed)
        .await
        .unwrap();

    let index_storage_id = anvil::index_journal::index_storage_id(tenant_id, bucket.id, index.id);
    let signing_key = hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap();
    let checkpoint = anvil::watch_checkpoint::read_watch_checkpoint(
        &cluster.states[0].storage,
        "object_metadata",
        &index_storage_id,
        &signing_key,
    )
    .await
    .unwrap()
    .expect("index build should checkpoint object metadata cursor");
    assert_eq!(checkpoint.cursor, u128::from(source_cursor));

    anvil::core_store::CoreMetaStore::open(cluster.states[0].storage.core_store_meta_path())
        .unwrap()
        .delete(
            anvil::core_store::CF_INDEX_ROWS,
            anvil::core_store::TABLE_DERIVED_INDEX_PROOF_ROW,
            &derived_index_proof_head_tuple_key(&index_storage_id),
        )
        .expect("remove proof head to simulate lost derived proof");

    assert!(
        persistence
            .enqueue_index_build_for_index(&bucket, &index)
            .await
            .unwrap(),
        "missing proof must schedule a rebuild even when checkpoint cursor is current"
    );
    let rebuild_task = persistence
        .list_tasks_page(None, 1_000)
        .await
        .unwrap()
        .tasks
        .into_iter()
        .find(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.status == anvil::tasks::TaskStatus::Pending
                && task.payload["tenant_id"] == serde_json::json!(tenant_id)
                && task.payload["bucket_id"] == serde_json::json!(bucket.id)
                && task.payload["index_id"] == serde_json::json!(index.id)
                && task.payload["source_cursor"] == serde_json::json!(source_cursor)
        })
        .expect("missing proof rebuild task should be pending");
    assert_eq!(
        rebuild_task.payload["catch_up_plan"]["RebuildFromManifest"]["reason"],
        serde_json::json!("MissingProof")
    );
    assert_eq!(
        rebuild_task.payload["catch_up_plan"]["RebuildFromManifest"]["resume_after_cursor"],
        serde_json::json!(0)
    );
    assert!(
        rebuild_task
            .payload
            .get("source_manifest_hash")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value.len() == 64),
        "rebuild task should record the source checkpoint hash"
    );
}

#[tokio::test]
async fn test_repair_rebuilds_missing_full_text_segment_from_base_journal() {
    let cluster = shared_default_test_cluster().await;
    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = unique_test_name("index-repair");
    let bucket = persistence
        .create_bucket(tenant_id, &bucket_name, "test-region-1")
        .await
        .unwrap();
    put_index_object_bytes(
        &cluster,
        tenant_id,
        &bucket.name,
        "docs/repair.txt",
        Some("text/plain"),
        None,
        b"repair rebuilds derived full text segment",
    )
    .await;
    persistence
        .compact_object_metadata(bucket.id)
        .await
        .unwrap()
        .expect("object metadata compaction writes manifest segments");
    let index = create_index_definition_for_test(
        persistence,
        &bucket,
        "body",
        "full_text",
        serde_json::json!({"prefix": "docs/"}),
        serde_json::json!({"source": "object_body_utf8"}),
        "index_only",
        serde_json::json!({"positions": true}),
    )
    .await;
    let signing_key = hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap();
    let stats = anvil::metadata_journal::active_object_journal_stats(
        &cluster.states[0].storage,
        &bucket,
        &signing_key,
    )
    .await
    .unwrap();
    let source_cursor = anvil::index_repair::source_cursor_from_stats(stats);
    assert!(
        source_cursor > 0,
        "compacted source manifest must expose a repairable source cursor"
    );

    persistence
        .rebuild_index_direct(tenant_id, bucket.id, index.id, index.version, source_cursor)
        .await
        .unwrap()
        .expect("initial index build succeeds");
    let index_storage_id = anvil::index_journal::index_storage_id(tenant_id, bucket.id, index.id);
    let signing_key = hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap();
    let proof = anvil::derived_index_proof::read_latest_derived_index_proof(
        &cluster.states[0].storage,
        &index_storage_id,
        &signing_key,
    )
    .await
    .unwrap()
    .expect("proof exists before deleting segment");
    assert!(!proof.segment_hashes.is_empty());
    for segment in collect_index_segments_for_test(&cluster.states[0].storage, &index_storage_id)
        .await
        .into_iter()
        .filter(|record| record.index_kind == "full_text")
    {
        delete_index_segment_coremeta_row(&cluster.states[0].storage, &segment);
    }
    assert!(
        anvil::full_text_segment::read_latest_full_text_segment(
            &cluster.states[0].storage,
            &index_storage_id
        )
        .await
        .unwrap()
        .is_none(),
        "segment CoreMeta row deletion must remove the queryable derived index"
    );

    let mut repair_client = RepairServiceClient::connect(grpc_addr).await.unwrap();
    let report = repair_client
        .repair_index(authorized(
            RepairIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "body".to_string(),
                rebuild: true,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(report.status, "rebuilt_derived_index");
    assert_eq!(report.reason, "DerivedIndexSegmentMissing");
    assert_eq!(report.index_storage_id, index_storage_id);
    assert_eq!(
        report.source_cursor_low,
        u64::try_from(source_cursor).unwrap()
    );
    assert_eq!(report.source_cursor_high, 0);
    assert!(report.finding.is_some());
    assert!(report.build.is_some());

    let repaired = anvil::full_text_segment::read_latest_full_text_segment(
        &cluster.states[0].storage,
        &index_storage_id,
    )
    .await
    .unwrap()
    .expect("repair rebuilds segment");
    let definition = anvil::formats::full_text::FullTextIndexDefinition::from_json(
        &serde_json::json!({"positions": true}),
    )
    .unwrap();
    let hits = query_full_text_segment(
        &repaired,
        FullTextSegmentQuery {
            query: "repair rebuilds",
            tokenizer: &definition.tokenizer,
            positions_enabled: definition.positions_enabled,
            phrase: false,
            bm25: anvil::formats::full_text::Bm25Config::default(),
            authorized_labels: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(
        !hits.is_empty(),
        "rebuilt derived index must be queryable from base metadata and payload journals"
    );

    let findings = repair_client
        .list_repair_findings(authorized(
            ListRepairFindingsRequest {
                scope_kind: "bucket".to_string(),
                scope_id: format!("tenant-{tenant_id}-bucket-{}", bucket.id),
                page: Some(anvil::anvil_api::PageRequest {
                    page_size: 10,
                    page_token: String::new(),
                }),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .findings;
    assert!(findings.iter().any(|finding| {
        finding.code == "DerivedIndexSegmentMissing" && finding.status == "RebuiltDerivedIndex"
    }));
}

#[tokio::test]
async fn test_repair_rebuilds_missing_vector_segment_from_base_journal() {
    let cluster = shared_default_test_cluster().await;
    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = unique_test_name("vector-index-repair");
    let bucket = persistence
        .create_bucket(tenant_id, &bucket_name, "test-region-1")
        .await
        .unwrap();
    let vector_payload = br#"{"vector":[1.0,0.0],"source_start":4,"source_len":12}"#;
    put_index_object_bytes(
        &cluster,
        tenant_id,
        &bucket.name,
        "vectors/repair.json",
        Some("application/json"),
        None,
        vector_payload,
    )
    .await;
    persistence
        .compact_object_metadata(bucket.id)
        .await
        .unwrap()
        .expect("object metadata compaction writes manifest segments");
    let index = create_index_definition_for_test(
        persistence,
        &bucket,
        "embedding",
        "vector",
        serde_json::json!({"prefix": "vectors/"}),
        serde_json::json!({}),
        "index_only",
        rfc_vector_policy(
            "object_body_json_vector",
            "caller_supplied",
            "test-explicit-vector",
            2,
            "text",
            "cosine",
        ),
    )
    .await;
    let signing_key = hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap();
    let stats = anvil::metadata_journal::active_object_journal_stats(
        &cluster.states[0].storage,
        &bucket,
        &signing_key,
    )
    .await
    .unwrap();
    let source_cursor = anvil::index_repair::source_cursor_from_stats(stats);
    assert!(
        source_cursor > 0,
        "compacted source manifest must expose a repairable source cursor"
    );

    persistence
        .rebuild_index_direct(tenant_id, bucket.id, index.id, index.version, source_cursor)
        .await
        .unwrap()
        .expect("initial vector index build succeeds");
    let index_storage_id = anvil::index_journal::index_storage_id(tenant_id, bucket.id, index.id);
    let proof = anvil::derived_index_proof::read_latest_derived_index_proof(
        &cluster.states[0].storage,
        &index_storage_id,
        &signing_key,
    )
    .await
    .unwrap()
    .expect("proof exists before deleting segment");
    assert!(!proof.segment_hashes.is_empty());
    for segment in collect_index_segments_for_test(&cluster.states[0].storage, &index_storage_id)
        .await
        .into_iter()
        .filter(|record| record.index_kind == "vector")
    {
        delete_index_segment_coremeta_row(&cluster.states[0].storage, &segment);
    }
    assert!(
        anvil::vector_segment::read_latest_vector_segment(
            &cluster.states[0].storage,
            &index_storage_id
        )
        .await
        .unwrap()
        .is_none(),
        "segment CoreMeta row deletion must remove the queryable vector derived index"
    );

    let mut repair_client = RepairServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let report = repair_client
        .repair_index(authorized(
            RepairIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "embedding".to_string(),
                rebuild: true,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(report.status, "rebuilt_derived_index");
    assert_eq!(report.reason, "DerivedIndexSegmentMissing");
    assert_eq!(report.index_storage_id, index_storage_id);
    assert_eq!(
        report.source_cursor_low,
        u64::try_from(source_cursor).unwrap()
    );
    assert_eq!(report.source_cursor_high, 0);
    assert!(report.finding.is_some());
    assert!(report.build.is_some());

    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();
    let response = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "embedding".to_string(),
                query_text: String::new(),
                query_vector: vec![1.0, 0.0],
                limit: 10,
                phrase: false,
                path_prefix: String::new(),
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
    assert_eq!(response.index_kind, IndexKind::Vector as i32);
    assert!(response.index_generation >= 1);
    assert!(
        response
            .hits
            .iter()
            .any(|hit| hit.object_key == "vectors/repair.json"),
        "rebuilt vector index must be queryable from base metadata and payload journals"
    );
}

#[tokio::test]
async fn test_index_build_followup_waits_for_running_build_and_catches_up_after_restart() {
    let cluster = isolated_test_cluster(
        "follow-up handoff test claims pending tasks and reopens persistence to simulate restart",
        &["test-region-1"],
    )
    .await;
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = unique_test_name("index-handoff");
    let bucket = persistence
        .create_bucket(tenant_id, &bucket_name, "test-region-1")
        .await
        .unwrap();
    let index = create_index_definition_for_test(
        persistence,
        &bucket,
        "body",
        "full_text",
        serde_json::json!({"prefix": "docs/"}),
        serde_json::json!({"source": "object_body_utf8"}),
        "index_only",
        serde_json::json!({"positions": true}),
    )
    .await;

    put_index_object_bytes(
        &cluster,
        tenant_id,
        &bucket.name,
        "docs/alpha.txt",
        Some("text/plain"),
        None,
        b"alpha handoff first",
    )
    .await;
    let running = persistence.claim_pending_tasks(10).await.unwrap();
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].task_type, anvil::tasks::TaskType::IndexBuild);
    let first_cursor = running[0].payload["source_cursor"].as_u64().unwrap();

    put_index_object_bytes(
        &cluster,
        tenant_id,
        &bucket.name,
        "docs/bravo.txt",
        Some("text/plain"),
        None,
        b"bravo handoff followup",
    )
    .await;
    assert!(
        persistence
            .claim_pending_tasks(10)
            .await
            .unwrap()
            .is_empty(),
        "follow-up for a running index build must wait for the active build"
    );

    let restarted = anvil::persistence::Persistence::new(&cluster.states[0].config).unwrap();
    restarted
        .rebuild_index_direct(
            tenant_id,
            bucket.id,
            index.id,
            index.version,
            u128::from(first_cursor),
        )
        .await
        .unwrap()
        .expect("first build succeeds after restart");
    restarted
        .update_task_status(running[0].id, anvil::tasks::TaskStatus::Completed)
        .await
        .unwrap();

    let followup = restarted.claim_pending_tasks(10).await.unwrap();
    assert_eq!(followup.len(), 1);
    let followup_cursor = followup[0].payload["source_cursor"].as_u64().unwrap();
    assert!(followup_cursor > first_cursor);
    restarted
        .rebuild_index_direct(
            tenant_id,
            bucket.id,
            index.id,
            index.version,
            u128::from(followup_cursor),
        )
        .await
        .unwrap()
        .expect("follow-up build succeeds");
    restarted
        .update_task_status(followup[0].id, anvil::tasks::TaskStatus::Completed)
        .await
        .unwrap();

    let index_storage_id = anvil::index_journal::index_storage_id(tenant_id, bucket.id, index.id);
    let segment = anvil::full_text_segment::read_latest_full_text_segment(
        &cluster.states[0].storage,
        &index_storage_id,
    )
    .await
    .unwrap()
    .expect("full text segment exists");
    assert_eq!(segment.header.source_cursor, followup_cursor);
    let definition = anvil::formats::full_text::FullTextIndexDefinition::from_json(
        &serde_json::json!({"positions": true}),
    )
    .unwrap();
    for term in ["alpha", "bravo"] {
        let hits = query_full_text_segment(
            &segment,
            FullTextSegmentQuery {
                query: term,
                tokenizer: &definition.tokenizer,
                positions_enabled: definition.positions_enabled,
                phrase: false,
                bm25: anvil::formats::full_text::Bm25Config::default(),
                authorized_labels: None,
                limit: 10,
            },
        )
        .unwrap();
        assert!(!hits.is_empty(), "{term} should be present after catch-up");
    }
}
